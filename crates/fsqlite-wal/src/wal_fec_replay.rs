//! Reconstruct replayable SQLite WAL bytes from verified WAL-FEC groups.
//!
//! This is the missing byte-reconstruction stage after RaptorQ decoding, not
//! an in-place repair or a claim that the public Connection open path invokes
//! recovery. Inputs are immutable snapshots; no filesystem entry is opened,
//! changed, truncated, or published here. Native callers must capture those
//! snapshots under the existing namespace/recovery fences, execute decoding on
//! their caller-owned blocking pool, and settle durable writes before publishing
//! a WAL index. A returned prefix is never permission to discard a live WAL.

use std::borrow::Cow;

use fsqlite_error::{FrankenError, Result};
use fsqlite_types::PageNumber;

use super::{
    WalFecDecodeProof, WalFecGroupMeta, WalFecRecoveryFallbackReason,
    WalFecRecoveryGroupRecord, WalFecRecoveryOutcome, WalFrameCandidate,
    read_length_prefixed, recover_wal_fec_group_record_with_decoder,
    scan_offset_after_optional_pragma_header, wal_fec_raptorq_decode,
};
use crate::checksum::{
    SqliteWalChecksum, WAL_FRAME_HEADER_SIZE, WAL_HEADER_SIZE, WalChecksumTransform,
    WalFrameHeader, WalHeader, validate_wal_header_checksum,
};

/// Admission limits, checked before image copies or decoder construction.
/// These bound input sizes and source/repair counts, not total process RSS.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalFecReplayLimits {
    pub max_wal_bytes: usize,
    pub max_sidecar_bytes: usize,
    pub max_sidecar_groups: usize,
    pub max_source_pages: usize,
    pub max_repair_symbols: usize,
}

impl Default for WalFecReplayLimits {
    fn default() -> Self {
        Self {
            max_wal_bytes: 64 * 1024 * 1024,
            max_sidecar_bytes: 32 * 1024 * 1024,
            max_sidecar_groups: 4096,
            max_source_pages: 256,
            max_repair_symbols: 255,
        }
    }
}

/// Why a complete recovery could not be established.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalFecReplayStopReason {
    PartialFrame,
    MissingGroup,
    UnusableSidecar,
    AmbiguousGroup,
    ResourceLimit,
    InvalidGroupBoundary,
    PrefixMismatch,
    TerminalAnchorMismatch,
    Decode(WalFecRecoveryFallbackReason),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalFecReplayStop {
    /// One-based physical WAL frame at which replay stopped.
    pub frame_no: u32,
    pub reason: WalFecReplayStopReason,
}

/// A verified committed prefix plus explicit evidence about the omitted tail.
///
/// Successful decoding is NOT durability. `complete_image` refuses an unresolved
/// failure; `replayable_prefix` is deliberately named for callers implementing
/// an explicit SQLite-compatible fallback policy. Neither method writes files.
#[derive(Debug)]
pub struct WalFecReplayResult<'a> {
    image: Cow<'a, [u8]>,
    header: WalHeader,
    committed_frames: u32,
    db_size_pages: Option<u32>,
    discarded_tail_bytes: usize,
    repaired_frame_nos: Vec<u32>,
    decode_proofs: Vec<WalFecDecodeProof>,
    stop: Option<WalFecReplayStop>,
}

impl<'a> WalFecReplayResult<'a> {
    /// Validated header of the input generation.
    #[must_use]
    pub const fn header(&self) -> &WalHeader {
        &self.header
    }

    #[must_use]
    pub const fn committed_frames(&self) -> u32 {
        self.committed_frames
    }

    #[must_use]
    pub const fn db_size_pages(&self) -> Option<u32> {
        self.db_size_pages
    }

    #[must_use]
    pub const fn discarded_tail_bytes(&self) -> usize {
        self.discarded_tail_bytes
    }

    /// Includes repaired frame headers, even when no payload needed decoding.
    #[must_use]
    pub fn repaired_frame_nos(&self) -> &[u32] {
        &self.repaired_frame_nos
    }

    /// Decoder evidence is separate from acceptance of the reconstructed WAL.
    /// A successful decode can still fail prefix or terminal-anchor validation.
    #[must_use]
    pub fn decode_proofs(&self) -> &[WalFecDecodeProof] {
        &self.decode_proofs
    }

    #[must_use]
    pub const fn stop(&self) -> Option<WalFecReplayStop> {
        self.stop
    }

    #[must_use]
    pub fn replayable_prefix(&self) -> &[u8] {
        &self.image
    }

    /// Return the image only when every complete input frame was validated.
    /// A valid, non-committed suffix is omitted according to SQLite replay rules.
    pub fn complete_image(self) -> Result<Cow<'a, [u8]>> {
        if let Some(stop) = self.stop {
            return Err(corrupt(format!(
                "WAL-FEC recovery stopped at frame {}: {:?}",
                stop.frame_no, stop.reason
            )));
        }
        Ok(self.image)
    }
}

/// Decode damaged groups and reconstruct their real WAL frame headers/checksums.
///
/// The healthy path borrows the input and never parses the sidecar or constructs
/// a decoder. A damaged group must start immediately after the last validated
/// commit. Independently hashed payloads are decoded by the existing production
/// RaptorQ implementation; the reconstructed end-frame header must exactly match
/// the original end-frame header. This terminal anchor prevents stale metadata
/// from blessing a different generation or inventing a commit boundary. It also
/// deliberately refuses damage to that final header; such recovery requires a
/// stronger durable anchor than the current sidecar provides.
/// These are accidental-corruption checks, not cryptographic authentication.
///
/// Later frames are checked against their ORIGINAL checksum fields. Repairing
/// one group never recomputes checksums over an unrelated damaged later group.
/// Input bytes remain unchanged on every success, error, and fallback path.
pub fn recover_wal_fec_image<'a>(
    wal_bytes: &'a [u8],
    sidecar_bytes: &[u8],
    limits: WalFecReplayLimits,
) -> Result<WalFecReplayResult<'a>> {
    if wal_bytes.len() > limits.max_wal_bytes {
        return Err(corrupt("WAL image exceeds recovery input limit"));
    }
    let header = WalHeader::from_bytes(wal_bytes)?;
    if !validate_wal_header_checksum(wal_bytes, header.big_endian_checksum())? {
        return Err(corrupt("WAL header checksum mismatch before FEC recovery"));
    }
    let page_size = usize::try_from(header.page_size)
        .map_err(|_| corrupt("WAL page size exceeds address space"))?;
    let frame_size = WAL_FRAME_HEADER_SIZE
        .checked_add(page_size)
        .ok_or_else(|| corrupt("WAL frame size overflow"))?;
    let physical_frames = (wal_bytes.len() - WAL_HEADER_SIZE) / frame_size;
    let frame_count = u32::try_from(physical_frames)
        .map_err(|_| corrupt("WAL frame count exceeds SQLite's domain"))?;
    let mut image = Cow::Borrowed(wal_bytes);
    let mut frame_index = 0_u32;
    let mut running = header.checksum;
    let mut committed_frames = 0_u32;
    let mut committed_checksum = running;
    let mut db_size_pages = None;
    let mut repaired_frame_nos = Vec::new();
    let mut decode_proofs = Vec::new();
    let mut stop = None;

    while frame_index < frame_count {
        let offset = frame_offset(frame_index, frame_size)?;
        let frame = &image[offset..offset + frame_size];
        let frame_header = WalFrameHeader::from_bytes(frame)?;
        let expected = WalChecksumTransform::for_wal_frame(
            frame, page_size, header.big_endian_checksum(),
        )?.apply(running);
        if PageNumber::new(frame_header.page_number).is_some()
            && (frame_header.db_size == 0 || PageNumber::new(frame_header.db_size).is_some())
            && frame_header.salts == header.salts
            && frame_header.checksum == expected
        {
            running = expected;
            frame_index += 1;
            if frame_header.is_commit() {
                committed_frames = frame_index;
                committed_checksum = running;
                db_size_pages = Some(frame_header.db_size);
            }
            continue;
        }

        let damaged_frame_no = frame_index + 1;
        let group = match find_recovery_group(sidecar_bytes, &header, damaged_frame_no, limits) {
            Ok(group) => group,
            Err(reason) => {
                stop = Some(WalFecReplayStop { frame_no: damaged_frame_no, reason });
                break;
            }
        };
        let meta = &group.meta;
        let source_pages = usize::try_from(meta.k_source)
            .map_err(|_| corrupt("FEC source count exceeds address space"))?;
        if source_pages > limits.max_source_pages
            || usize::try_from(meta.r_repair).unwrap_or(usize::MAX) > limits.max_repair_symbols
        {
            stop = Some(WalFecReplayStop {
                frame_no: damaged_frame_no, reason: WalFecReplayStopReason::ResourceLimit,
            });
            break;
        }
        if meta.page_size != header.page_size
            || meta.start_frame_no.checked_sub(1) != Some(committed_frames)
            || meta.end_frame_no > frame_count
            || PageNumber::new(meta.db_size_pages).is_none()
            || meta.page_numbers.iter().any(|page| PageNumber::new(*page).is_none())
        {
            stop = Some(WalFecReplayStop {
                frame_no: damaged_frame_no, reason: WalFecReplayStopReason::InvalidGroupBoundary,
            });
            break;
        }

        // Include independently verified sources after the chain break. Their
        // cumulative WAL checksum failures do not make their payloads erasures.
        let mut candidates = Vec::with_capacity(source_pages);
        for number in meta.start_frame_no..=meta.end_frame_no {
            let offset = frame_offset(number - 1, frame_size)?;
            candidates.push(WalFrameCandidate {
                frame_no: number,
                page_data: image[offset + WAL_FRAME_HEADER_SIZE..offset + frame_size].to_vec(),
            });
        }
        let mut decode = wal_fec_raptorq_decode;
        let outcome = recover_wal_fec_group_record_with_decoder(
            &group, damaged_frame_no, &candidates, &mut decode,
        )?;
        let recovered = match outcome {
            WalFecRecoveryOutcome::Recovered(recovered) => recovered,
            WalFecRecoveryOutcome::TruncateBeforeGroup { decode_proof, .. } => {
                let reason = decode_proof.fallback_reason
                    .unwrap_or(WalFecRecoveryFallbackReason::DecodeFailed);
                decode_proofs.push(decode_proof);
                stop = Some(WalFecReplayStop {
                    frame_no: damaged_frame_no, reason: WalFecReplayStopReason::Decode(reason),
                });
                break;
            }
        };
        let mut rebuilt = Vec::with_capacity(source_pages * frame_size);
        let mut rebuilt_checksum = committed_checksum;
        for (index, page) in recovered.recovered_pages.iter().enumerate() {
            let is_last = index + 1 == source_pages;
            let start = rebuilt.len();
            rebuilt.extend_from_slice(&WalFrameHeader {
                page_number: meta.page_numbers[index],
                db_size: if is_last { meta.db_size_pages } else { 0 },
                salts: header.salts,
                checksum: SqliteWalChecksum::default(),
            }.to_bytes());
            rebuilt.extend_from_slice(page);
            rebuilt_checksum = WalChecksumTransform::for_wal_frame(
                &rebuilt[start..], page_size, header.big_endian_checksum(),
            )?.apply(rebuilt_checksum);
            rebuilt[start + 16..start + 20].copy_from_slice(&rebuilt_checksum.s1.to_be_bytes());
            rebuilt[start + 20..start + 24].copy_from_slice(&rebuilt_checksum.s2.to_be_bytes());
        }
        let group_start = frame_offset(committed_frames, frame_size)?;
        let group_end = frame_offset(meta.end_frame_no, frame_size)?;
        let verified_len = frame_offset(frame_index, frame_size)? - group_start;
        let terminal_offset = rebuilt.len() - frame_size;
        let original_terminal = group_end - frame_size;
        let refusal = if rebuilt[..verified_len] != image[group_start..group_start + verified_len] {
            Some(WalFecReplayStopReason::PrefixMismatch)
        } else if rebuilt[terminal_offset..terminal_offset + WAL_FRAME_HEADER_SIZE]
            != image[original_terminal..original_terminal + WAL_FRAME_HEADER_SIZE]
        {
            Some(WalFecReplayStopReason::TerminalAnchorMismatch)
        } else {
            None
        };
        decode_proofs.push(recovered.decode_proof);
        if let Some(reason) = refusal {
            stop = Some(WalFecReplayStop { frame_no: damaged_frame_no, reason });
            break;
        }

        // Publish only into the owned plan, after BOTH anchors match. A failed
        // attempt never modifies even an earlier validated frame in the plan.
        for (index, frame) in rebuilt.chunks_exact(frame_size).enumerate() {
            let offset = group_start + index * frame_size;
            if image[offset..offset + frame_size] != *frame {
                repaired_frame_nos.push(meta.start_frame_no + u32::try_from(index)
                    .map_err(|_| corrupt("repaired frame index overflow"))?);
            }
        }
        image.to_mut()[group_start..group_end].copy_from_slice(&rebuilt);
        frame_index = meta.end_frame_no;
        committed_frames = frame_index;
        running = rebuilt_checksum;
        committed_checksum = running;
        db_size_pages = Some(meta.db_size_pages);
    }

    if stop.is_none() && !(wal_bytes.len() - WAL_HEADER_SIZE).is_multiple_of(frame_size) {
        stop = Some(WalFecReplayStop {
            frame_no: frame_count.checked_add(1)
                .ok_or_else(|| corrupt("partial WAL frame number overflow"))?,
            reason: WalFecReplayStopReason::PartialFrame,
        });
    }
    let prefix_len = frame_offset(committed_frames, frame_size)?;
    image = match image {
        Cow::Borrowed(bytes) => Cow::Borrowed(&bytes[..prefix_len]),
        Cow::Owned(mut bytes) => { bytes.truncate(prefix_len); Cow::Owned(bytes) }
    };
    Ok(WalFecReplayResult {
        image, header, committed_frames, db_size_pages,
        discarded_tail_bytes: wal_bytes.len() - prefix_len,
        repaired_frame_nos, decode_proofs, stop,
    })
}

/// Scan borrowed sidecar bytes without allocating symbol payloads for unrelated
/// groups. Invalid repair records are erasures, not inputs to the decoder.
fn find_recovery_group(
    bytes: &[u8],
    header: &WalHeader,
    frame_no: u32,
    limits: WalFecReplayLimits,
) -> std::result::Result<WalFecRecoveryGroupRecord, WalFecReplayStopReason> {
    use WalFecReplayStopReason as Stop;
    if bytes.len() > limits.max_sidecar_bytes {
        return Err(Stop::ResourceLimit);
    }
    let mut cursor = scan_offset_after_optional_pragma_header(bytes)
        .map_err(|_| Stop::UnusableSidecar)?;
    let mut found = None;
    let mut groups_seen = 0_usize;
    while cursor < bytes.len() {
        if groups_seen >= limits.max_sidecar_groups {
            return Err(Stop::ResourceLimit);
        }
        groups_seen += 1;
        let meta_bytes = read_length_prefixed(bytes, &mut cursor)
            .map_err(|_| Stop::UnusableSidecar)?
            .ok_or(Stop::UnusableSidecar)?;
        let meta = WalFecGroupMeta::from_record_bytes(meta_bytes)
            .map_err(|_| Stop::UnusableSidecar)?;
        let matches = meta.verify_salt_binding(header.salts).is_ok()
            && meta.start_frame_no <= frame_no && frame_no <= meta.end_frame_no;
        if matches && found.is_some() {
            // Do not choose an arbitrary winner among overlapping authorities.
            return Err(Stop::AmbiguousGroup);
        }
        if matches && (usize::try_from(meta.k_source).unwrap_or(usize::MAX) > limits.max_source_pages
            || usize::try_from(meta.r_repair).unwrap_or(usize::MAX) > limits.max_repair_symbols)
        {
            return Err(Stop::ResourceLimit);
        }
        // Every iteration consumes at least a length prefix. A forged count
        // cannot spin past the admitted input size.
        if u64::from(meta.r_repair) > u64::try_from((bytes.len() - cursor) / 4).unwrap_or(0) {
            return Err(Stop::UnusableSidecar);
        }
        let mut repair_symbols = Vec::new();
        let mut corruption_observations = 0_u32;
        for _ in 0..meta.r_repair {
            let payload = read_length_prefixed(bytes, &mut cursor)
                .map_err(|_| Stop::UnusableSidecar)?
                .ok_or(Stop::UnusableSidecar)?;
            if matches {
                match fsqlite_types::SymbolRecord::from_bytes(payload) {
                    Ok(symbol) => repair_symbols.push(symbol),
                    Err(_) => corruption_observations += 1,
                }
            }
        }
        if matches {
            found = Some(WalFecRecoveryGroupRecord { meta, repair_symbols, corruption_observations });
        }
    }
    found.ok_or(Stop::MissingGroup)
}

fn frame_offset(frame_index: u32, frame_size: usize) -> Result<usize> {
    usize::try_from(frame_index).ok()
        .and_then(|index| index.checked_mul(frame_size))
        .and_then(|offset| offset.checked_add(WAL_HEADER_SIZE))
        .ok_or_else(|| corrupt("WAL recovery byte offset overflow"))
}

fn corrupt(detail: impl Into<String>) -> FrankenError {
    FrankenError::WalCorrupt { detail: detail.into() }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checksum::{WAL_FORMAT_VERSION, WAL_MAGIC_BE, WAL_MAGIC_LE, WalSalts};
    use crate::wal_fec::{
        WalFecGroupMetaInit, WalFecGroupRecord, build_source_page_hashes,
        encode_wal_fec_group, generate_wal_fec_repair_symbols,
    };
    use fsqlite_types::{ObjectId, Oti};

    const PAGE_SIZE_U32: u32 = 512;
    const PAGE_SIZE: usize = 512;
    const FRAME_SIZE: usize = WAL_FRAME_HEADER_SIZE + PAGE_SIZE;

    fn header(magic: u32) -> WalHeader {
        WalHeader {
            magic, format_version: WAL_FORMAT_VERSION, page_size: PAGE_SIZE_U32,
            checkpoint_seq: 17, salts: WalSalts { salt1: 12345, salt2: 67890 },
            checksum: SqliteWalChecksum::default(),
        }
    }

    fn append_group(wal: &mut Vec<u8>, count: u32, repairs: u32, tag: u8) -> Vec<u8> {
        let header = WalHeader::from_bytes(wal).unwrap();
        let start_frame_no = u32::try_from((wal.len() - WAL_HEADER_SIZE) / FRAME_SIZE).unwrap() + 1;
        let pages: Vec<Vec<u8>> = (0..count).map(|index| {
            let mut page = vec![tag; PAGE_SIZE];
            page[..4].copy_from_slice(&index.to_be_bytes());
            page
        }).collect();
        let meta = WalFecGroupMeta::from_init(WalFecGroupMetaInit {
            wal_salt1: header.salts.salt1, wal_salt2: header.salts.salt2,
            start_frame_no, end_frame_no: start_frame_no + count - 1,
            db_size_pages: 100, page_size: PAGE_SIZE_U32, k_source: count, r_repair: repairs,
            oti: Oti { f: u64::from(count) * u64::from(PAGE_SIZE_U32), al: 1, t: PAGE_SIZE_U32, z: 1, n: 1 },
            object_id: ObjectId::derive_from_canonical_bytes(&[tag]),
            page_numbers: (1..=count).collect(), source_page_xxh3_128: build_source_page_hashes(&pages),
        }).unwrap();
        let mut checksum = if start_frame_no == 1 { header.checksum } else {
            WalFrameHeader::from_bytes(&wal[wal.len() - FRAME_SIZE..]).unwrap().checksum
        };
        for (index, page) in pages.iter().enumerate() {
            let start = wal.len();
            wal.extend_from_slice(&WalFrameHeader {
                page_number: meta.page_numbers[index],
                db_size: if index + 1 == pages.len() { meta.db_size_pages } else { 0 },
                salts: header.salts, checksum: SqliteWalChecksum::default(),
            }.to_bytes());
            wal.extend_from_slice(page);
            checksum = WalChecksumTransform::for_wal_frame(
                &wal[start..], PAGE_SIZE, header.big_endian_checksum(),
            ).unwrap().apply(checksum);
            wal[start + 16..start + 20].copy_from_slice(&checksum.s1.to_be_bytes());
            wal[start + 20..start + 24].copy_from_slice(&checksum.s2.to_be_bytes());
        }
        let symbols = generate_wal_fec_repair_symbols(&meta, &pages).unwrap();
        encode_wal_fec_group(&WalFecGroupRecord::new(meta, symbols).unwrap()).unwrap()
    }

    fn corrupt_payload(wal: &mut [u8], frame: u32) {
        let offset = frame_offset(frame - 1, FRAME_SIZE).unwrap();
        wal[offset + WAL_FRAME_HEADER_SIZE + 73] ^= 0x5a;
    }

    #[test]
    fn intact_wal_borrows_and_does_not_parse_sidecar() {
        let mut wal = header(WAL_MAGIC_LE).to_bytes().unwrap().to_vec();
        append_group(&mut wal, 3, 4, 1);
        let result = recover_wal_fec_image(&wal, b"invalid sidecar", WalFecReplayLimits::default()).unwrap();
        assert_eq!(result.committed_frames, 3);
        assert!(result.decode_proofs.is_empty());
        assert!(matches!(result.complete_image().unwrap(), Cow::Borrowed(_)));
    }

    #[test]
    fn real_raptorq_repair_reconstructs_identical_wal_and_later_commit() {
        for magic in [WAL_MAGIC_LE, WAL_MAGIC_BE] {
            let mut wal = header(magic).to_bytes().unwrap().to_vec();
            let sidecar = append_group(&mut wal, 5, 8, 7);
            append_group(&mut wal, 2, 4, 8); // Later healthy commit has no sidecar entry.
            let expected = wal.clone();
            corrupt_payload(&mut wal, 2);
            let damaged = wal.clone();
            let result = recover_wal_fec_image(&wal, &sidecar, WalFecReplayLimits::default()).unwrap();
            assert_eq!(result.committed_frames, 7);
            assert_eq!(result.repaired_frame_nos, vec![2]);
            assert!(result.decode_proofs[0].decode_attempted);
            assert_eq!(result.complete_image().unwrap().as_ref(), expected);
            assert_eq!(wal, damaged, "recovery must not mutate its input");
        }
    }

    #[test]
    fn independent_damaged_groups_are_repaired_without_blessing_the_tail() {
        let mut wal = header(WAL_MAGIC_LE).to_bytes().unwrap().to_vec();
        let mut sidecar = append_group(&mut wal, 3, 8, 1);
        sidecar.extend(append_group(&mut wal, 4, 8, 2));
        let expected = wal.clone();
        corrupt_payload(&mut wal, 1);
        corrupt_payload(&mut wal, 6);
        let result = recover_wal_fec_image(&wal, &sidecar, WalFecReplayLimits::default()).unwrap();
        assert_eq!(result.decode_proofs.len(), 2);
        assert_eq!(result.repaired_frame_nos, vec![1, 6]);
        assert_eq!(result.complete_image().unwrap().as_ref(), expected);
    }

    #[test]
    fn insufficient_symbols_retains_only_the_previous_complete_transaction() {
        let mut wal = header(WAL_MAGIC_LE).to_bytes().unwrap().to_vec();
        append_group(&mut wal, 2, 2, 1);
        let expected_len = wal.len();
        let sidecar = append_group(&mut wal, 5, 2, 2);
        for frame in [4, 5, 6] { corrupt_payload(&mut wal, frame); }
        let result = recover_wal_fec_image(&wal, &sidecar, WalFecReplayLimits::default()).unwrap();
        assert_eq!(result.committed_frames, 2, "frame 3 is valid but not committed");
        assert_eq!(result.replayable_prefix(), &wal[..expected_len]);
        assert_eq!(result.stop.unwrap().reason,
            WalFecReplayStopReason::Decode(WalFecRecoveryFallbackReason::InsufficientSymbols));
        assert!(result.complete_image().is_err(), "fallback is not complete recovery");
    }

    #[test]
    fn corrupt_later_group_is_not_hidden_by_repairing_an_earlier_one() {
        let mut wal = header(WAL_MAGIC_LE).to_bytes().unwrap().to_vec();
        let sidecar = append_group(&mut wal, 3, 8, 1);
        let expected_prefix = wal.clone();
        append_group(&mut wal, 3, 8, 2);
        corrupt_payload(&mut wal, 2);
        corrupt_payload(&mut wal, 5);
        let result = recover_wal_fec_image(&wal, &sidecar, WalFecReplayLimits::default()).unwrap();
        assert_eq!(result.committed_frames, 3);
        assert_eq!(result.replayable_prefix(), expected_prefix);
        assert_eq!(result.stop.unwrap().reason, WalFecReplayStopReason::MissingGroup);
        assert!(result.complete_image().is_err());
    }

    #[test]
    fn damaged_terminal_anchor_is_not_replaced_with_an_invented_commit() {
        let mut wal = header(WAL_MAGIC_LE).to_bytes().unwrap().to_vec();
        let sidecar = append_group(&mut wal, 3, 8, 1);
        let last = frame_offset(2, FRAME_SIZE).unwrap();
        wal[last + 16] ^= 1;
        let result = recover_wal_fec_image(&wal, &sidecar, WalFecReplayLimits::default()).unwrap();
        assert_eq!(result.committed_frames, 0);
        assert_eq!(result.stop.unwrap().reason, WalFecReplayStopReason::TerminalAnchorMismatch);
        assert!(result.repaired_frame_nos.is_empty());
        assert!(result.complete_image().is_err());
    }

    #[test]
    fn same_salts_with_a_different_checkpoint_sequence_fail_the_terminal_anchor() {
        let mut wal = header(WAL_MAGIC_LE).to_bytes().unwrap().to_vec();
        let sidecar = append_group(&mut wal, 3, 8, 1);
        let mut replacement = header(WAL_MAGIC_LE);
        replacement.checkpoint_seq += 1;
        wal[..WAL_HEADER_SIZE].copy_from_slice(&replacement.to_bytes().unwrap());
        let result = recover_wal_fec_image(&wal, &sidecar, WalFecReplayLimits::default()).unwrap();
        assert_eq!(result.stop.unwrap().reason, WalFecReplayStopReason::TerminalAnchorMismatch);
        assert!(result.complete_image().is_err());
    }

    #[test]
    fn overlapping_sidecar_authorities_are_refused() {
        let mut wal = header(WAL_MAGIC_LE).to_bytes().unwrap().to_vec();
        let first = append_group(&mut wal, 3, 8, 1);
        let mut sidecar = first.clone();
        sidecar.extend(first);
        corrupt_payload(&mut wal, 1);
        let result = recover_wal_fec_image(&wal, &sidecar, WalFecReplayLimits::default()).unwrap();
        assert_eq!(result.stop.unwrap().reason, WalFecReplayStopReason::AmbiguousGroup);
        assert!(result.decode_proofs.is_empty());
    }

    #[test]
    fn source_admission_limit_precedes_decoder_construction() {
        let mut wal = header(WAL_MAGIC_LE).to_bytes().unwrap().to_vec();
        let sidecar = append_group(&mut wal, 3, 8, 1);
        corrupt_payload(&mut wal, 1);
        let limits = WalFecReplayLimits { max_source_pages: 2, ..WalFecReplayLimits::default() };
        let result = recover_wal_fec_image(&wal, &sidecar, limits).unwrap();
        assert_eq!(result.stop.unwrap().reason, WalFecReplayStopReason::ResourceLimit);
        assert!(result.decode_proofs.is_empty());
    }

    #[test]
    fn image_admission_and_invalid_headers_fail_without_repair() {
        let mut wal = header(WAL_MAGIC_LE).to_bytes().unwrap().to_vec();
        let limits = WalFecReplayLimits { max_wal_bytes: 31, ..WalFecReplayLimits::default() };
        assert!(recover_wal_fec_image(&wal, &[], limits).is_err());
        wal[24] ^= 1;
        assert!(recover_wal_fec_image(&wal, &[], WalFecReplayLimits::default()).is_err());
        assert!(recover_wal_fec_image(&wal[..20], &[], WalFecReplayLimits::default()).is_err());
    }

    #[test]
    fn nonterminal_header_damage_is_rebuilt_without_decoding_intact_payloads() {
        let mut wal = header(WAL_MAGIC_LE).to_bytes().unwrap().to_vec();
        let sidecar = append_group(&mut wal, 3, 8, 1);
        let expected = wal.clone();
        let offset = frame_offset(1, FRAME_SIZE).unwrap();
        wal[offset + 8] ^= 1; // Salts are not covered by SQLite's frame checksum.
        let result = recover_wal_fec_image(&wal, &sidecar, WalFecReplayLimits::default()).unwrap();
        assert!(!result.decode_proofs[0].decode_attempted);
        assert_eq!(result.repaired_frame_nos, vec![2]);
        assert_eq!(result.complete_image().unwrap().as_ref(), expected);
    }

    #[test]
    fn metadata_cannot_change_an_already_validated_prefix() {
        let mut wal = header(WAL_MAGIC_LE).to_bytes().unwrap().to_vec();
        let sidecar = append_group(&mut wal, 3, 8, 1);
        let mut records = crate::wal_fec::scan_wal_fec_bytes(
            std::path::Path::new("snapshot-only"), &sidecar,
        ).unwrap().groups;
        records[0].meta.page_numbers[0] += 10;
        records[0].meta.checksum = records[0].meta.compute_checksum();
        let tampered = encode_wal_fec_group(&records[0]).unwrap();
        corrupt_payload(&mut wal, 2);
        let result = recover_wal_fec_image(&wal, &tampered, WalFecReplayLimits::default()).unwrap();
        assert_eq!(result.stop.unwrap().reason, WalFecReplayStopReason::PrefixMismatch);
        assert!(result.repaired_frame_nos.is_empty());
    }

    #[test]
    fn metadata_cannot_change_the_original_database_size_commit_marker() {
        let mut wal = header(WAL_MAGIC_LE).to_bytes().unwrap().to_vec();
        let sidecar = append_group(&mut wal, 3, 8, 1);
        let mut records = crate::wal_fec::scan_wal_fec_bytes(
            std::path::Path::new("snapshot-only"), &sidecar,
        ).unwrap().groups;
        records[0].meta.db_size_pages += 1;
        records[0].meta.checksum = records[0].meta.compute_checksum();
        let tampered = encode_wal_fec_group(&records[0]).unwrap();
        corrupt_payload(&mut wal, 1);
        let result = recover_wal_fec_image(&wal, &tampered, WalFecReplayLimits::default()).unwrap();
        assert_eq!(result.stop.unwrap().reason, WalFecReplayStopReason::TerminalAnchorMismatch);
        assert!(result.complete_image().is_err());
    }

    #[test]
    fn corrupt_repair_records_are_erased_before_the_real_decoder() {
        let mut wal = header(WAL_MAGIC_LE).to_bytes().unwrap().to_vec();
        let mut sidecar = append_group(&mut wal, 5, 8, 1);
        let expected = wal.clone();
        let meta_len = usize::try_from(u32::from_le_bytes(sidecar[..4].try_into().unwrap())).unwrap();
        let symbol_prefix = 4 + meta_len;
        let symbol_len = usize::try_from(u32::from_le_bytes(
            sidecar[symbol_prefix..symbol_prefix + 4].try_into().unwrap(),
        )).unwrap();
        sidecar[symbol_prefix + 4 + symbol_len - 1] ^= 1;
        corrupt_payload(&mut wal, 2);
        let result = recover_wal_fec_image(&wal, &sidecar, WalFecReplayLimits::default()).unwrap();
        assert!(result.decode_proofs[0].corruption_observations > 0);
        assert!(result.decode_proofs[0].decode_attempted);
        assert_eq!(result.complete_image().unwrap().as_ref(), expected);
    }

    #[test]
    fn partial_tail_is_reported_not_certified_as_complete() {
        let mut wal = header(WAL_MAGIC_LE).to_bytes().unwrap().to_vec();
        append_group(&mut wal, 2, 2, 1);
        let expected = wal.clone();
        wal.extend_from_slice(&[0; 17]);
        let result = recover_wal_fec_image(&wal, &[], WalFecReplayLimits::default()).unwrap();
        assert_eq!(result.replayable_prefix(), expected);
        assert_eq!(result.discarded_tail_bytes, 17);
        assert_eq!(result.stop.unwrap().reason, WalFecReplayStopReason::PartialFrame);
        assert!(result.complete_image().is_err());
    }
}
