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
use std::collections::BTreeMap;

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
use crate::parallel_wal::{
    PARALLEL_WAL_MAX_DURABLE_CERTIFICATE_RECORD_SIZE, ParallelWalDurableCertificateRecord,
    ParallelWalFramePayloadDigestBuilder,
};
use crate::wal::WalGenerationIdentity;

/// Admission limits, checked before image copies or decoder construction.
/// These bound input sizes and source/repair counts, not total process RSS.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalFecReplayLimits {
    pub max_wal_bytes: usize,
    pub max_sidecar_bytes: usize,
    pub max_sidecar_groups: usize,
    pub max_source_pages: usize,
    pub max_repair_symbols: usize,
    pub max_certificate_bytes: usize,
    pub max_certificate_records: usize,
}

impl Default for WalFecReplayLimits {
    fn default() -> Self {
        Self {
            max_wal_bytes: 64 * 1024 * 1024,
            max_sidecar_bytes: 32 * 1024 * 1024,
            max_sidecar_groups: 4096,
            max_source_pages: 256,
            max_repair_symbols: 255,
            max_certificate_bytes: 32 * 1024 * 1024,
            max_certificate_records: 4096,
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

/// Independently stored certificate that accepted otherwise unanchored repairs.
/// This records byte validation, not a new durability or publication event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalFecReplayCertificateAnchor {
    pub start_frame_no: u32,
    pub end_frame_no: u32,
    pub certificate_epoch: u64,
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
    certificate_anchors: Vec<WalFecReplayCertificateAnchor>,
    stop: Option<WalFecReplayStop>,
}

/// The last accepted prefix before a chain of repairs lost its original
/// terminal checksum. A later ORIGINAL checksum must bind the whole chain
/// before these tentative commits can enter the returned replay prefix.
#[derive(Debug, Clone, Copy)]
struct PendingReplayAnchor {
    first_damaged_frame_no: u32,
    committed_frames: u32,
    db_size_pages: Option<u32>,
    repaired_frame_count: usize,
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

    /// Accepted repairs only, including headers with intact payloads. Tentative
    /// repairs discarded for lack of an original checksum anchor are excluded.
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

    /// Certificates whose full ordered payload digest accepted a repair chain.
    #[must_use]
    pub fn certificate_anchors(&self) -> &[WalFecReplayCertificateAnchor] {
        &self.certificate_anchors
    }

    #[must_use]
    pub const fn stop(&self) -> Option<WalFecReplayStop> {
        self.stop
    }

    #[must_use]
    pub fn replayable_prefix(&self) -> &[u8] {
        &self.image
    }

    /// Materialize a standalone database from a coherent main-file snapshot
    /// and the completely verified WAL. No source bytes are changed.
    ///
    /// This deliberately refuses a recovery fallback: a main file can already
    /// contain checkpointed pages newer than that fallback's committed prefix.
    /// Combining them would manufacture a mixed transaction snapshot.
    /// Callers must capture main and WAL under the same recovery fence, or
    /// supply an externally frozen pair from the same database generation.
    ///
    /// Later page versions win. Commit-time shrink boundaries retire both
    /// earlier WAL versions and base-file pages, so subsequent growth cannot
    /// resurrect stale bytes. Every page beyond the retained base must have a
    /// surviving WAL image; unexplained holes are refused rather than zeroed.
    /// The result preserves page-one metadata and reserved bytes, including
    /// WAL journal mode. It needs no old WAL or SHM file to be read.
    ///
    /// `max_database_bytes` bounds both the base input and output allocation,
    /// not total process memory. This checks image provenance and header
    /// consistency, not B-tree integrity of untouched main-file pages.
    pub fn database_image(&self, database: &[u8], max_database_bytes: usize) -> Result<Vec<u8>> {
        if let Some(stop) = self.stop {
            return Err(corrupt(format!(
                "cannot materialize a database from incomplete WAL recovery at frame {}: {:?}",
                stop.frame_no, stop.reason
            )));
        }
        if database.len() > max_database_bytes {
            return Err(corrupt("main database exceeds recovery input limit"));
        }
        let page_size = usize::try_from(self.header.page_size)
            .map_err(|_| corrupt("database page size exceeds address space"))?;
        if !database.len().is_multiple_of(page_size) {
            return Err(corrupt("main database has a partial page"));
        }
        let physical_base_pages = u32::try_from(database.len() / page_size)
            .map_err(|_| corrupt("main database page count exceeds SQLite's domain"))?;
        let mut retained_base_pages = physical_base_pages;
        if !database.is_empty() {
            validate_database_header(database, self.header.page_size)?;
            if let Some(declared) = authoritative_database_size(database) {
                retained_base_pages = retained_base_pages.min(declared);
            }
        }

        let frame_size = WAL_FRAME_HEADER_SIZE + page_size;
        let mut latest_pages = BTreeMap::<u32, &[u8]>::new();
        for frame in self.image[WAL_HEADER_SIZE..].chunks_exact(frame_size) {
            let frame_header = WalFrameHeader::from_bytes(frame)?;
            latest_pages.insert(frame_header.page_number, &frame[WAL_FRAME_HEADER_SIZE..]);
            if frame_header.is_commit() {
                retained_base_pages = retained_base_pages.min(frame_header.db_size);
                while latest_pages.last_key_value()
                    .is_some_and(|(page, _)| *page > frame_header.db_size)
                {
                    latest_pages.pop_last();
                }
            }
        }
        let final_pages = match self.db_size_pages {
            Some(pages) => pages,
            None if !database.is_empty() => {
                authoritative_database_size(database).unwrap_or(physical_base_pages)
            }
            None => return Err(corrupt("no committed database image is available")),
        };
        if PageNumber::new(final_pages).is_none() {
            return Err(corrupt("invalid recovered database page count"));
        }
        let output_len = usize::try_from(final_pages).ok()
            .and_then(|pages| pages.checked_mul(page_size))
            .filter(|size| *size <= max_database_bytes)
            .ok_or_else(|| corrupt("recovered database exceeds output limit"))?;
        retained_base_pages = retained_base_pages.min(final_pages);
        for page in retained_base_pages + 1..=final_pages {
            if !latest_pages.contains_key(&page) {
                return Err(corrupt(format!(
                    "recovered database page {page} has no surviving source image"
                )));
            }
        }

        // Validate the final page-one version before allocating the output.
        let page_one = latest_pages.get(&1).copied()
            .or_else(|| database.get(..page_size))
            .ok_or_else(|| corrupt("recovered database has no page one"))?;
        validate_database_header(page_one, self.header.page_size)?;
        if authoritative_database_size(page_one).is_some_and(|size| size != final_pages) {
            return Err(corrupt("recovered page-one size disagrees with the final WAL commit"));
        }

        let mut output = Vec::new();
        output.try_reserve_exact(output_len).map_err(|_| FrankenError::OutOfMemory)?;
        output.resize(output_len, 0);
        let base_len = usize::try_from(retained_base_pages)
            .map_err(|_| corrupt("base page count exceeds address space"))? * page_size;
        output[..base_len].copy_from_slice(&database[..base_len]);
        for (page, payload) in latest_pages {
            let offset = usize::try_from(page - 1)
                .map_err(|_| corrupt("page number exceeds address space"))? * page_size;
            output[offset..offset + page_size].copy_from_slice(payload);
        }
        Ok(output)
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

fn validate_database_header(bytes: &[u8], expected_page_size: u32) -> Result<()> {
    if bytes.len() < 100 || !bytes.starts_with(b"SQLite format 3\0") {
        return Err(corrupt("invalid main database header during recovery"));
    }
    let encoded = u16::from_be_bytes([bytes[16], bytes[17]]);
    let page_size = if encoded == 1 { 65_536 } else { u32::from(encoded) };
    if page_size != expected_page_size {
        return Err(corrupt("main database and WAL page sizes differ"));
    }
    if !matches!(bytes[18], 1 | 2) || !matches!(bytes[19], 1 | 2)
        || bytes[21..24] != [64, 32, 32]
        || page_size.saturating_sub(u32::from(bytes[20])) < 480
    {
        return Err(corrupt("unsupported main database header format during recovery"));
    }
    Ok(())
}

fn authoritative_database_size(bytes: &[u8]) -> Option<u32> {
    let size = u32::from_be_bytes(bytes[28..32].try_into().expect("validated database header"));
    (size != 0 && bytes[24..28] == bytes[92..96]).then_some(size)
}

/// Decode damaged groups and reconstruct their real WAL frame headers/checksums.
///
/// The healthy path borrows the input and never parses the sidecar or constructs
/// a decoder. A damaged group must start immediately after the last validated
/// commit. Independently hashed payloads are decoded by the existing production
/// RaptorQ implementation; the reconstructed rolling checksum must match the
/// ORIGINAL end-frame checksum. That checksum binds the reconstructed page
/// number, commit size, payload and preceding chain, so other end-frame header
/// fields can be repaired too. Salts are independently bound to the validated
/// WAL header by the FEC metadata; SQLite excludes them from frame checksums.
/// If that checksum is damaged too, reconstruction remains tentative until a
/// later original frame or repaired group's original end checksum validates
/// the entire preceding chain. If no such anchor survives, all tentative
/// commits are excluded, even when every payload was successfully decoded.
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
    recover_wal_fec_image_with_certificates(wal_bytes, sidecar_bytes, &[], [0; 16], limits)
}

/// Recover using optional durable certificates when original checksum anchors
/// have been lost, including in the final committed transaction.
///
/// `database_file_id` MUST come from bytes 76..92 of the coherently captured
/// main-file header, never from a decoded WAL page or the certificate itself.
/// Zero/legacy identities cannot authorize this stronger recovery. The caller
/// must capture all inputs from the same source under its recovery fences.
///
/// A certificate must bind the exact WAL generation and database identity,
/// cover EVERY tentative group since the last accepted commit, and match the
/// independently reconstructed ordered frame digest and final database size.
/// It cannot authorize missing physical frames, supply missing FEC payloads,
/// invent an uncommitted transaction, or overwrite a verified prefix.
///
/// Certificates are optional evidence: malformed, legacy, oversized or torn
/// streams provide no authority, but do not impede original-checksum recovery.
/// Conflicting eligible records provide no authority either. The healthy WAL
/// path does not parse certificates. Checksums/digests protect against accidental
/// corruption; these records are not signatures authenticating hostile inputs.
pub fn recover_wal_fec_image_with_certificates<'a>(
    wal_bytes: &'a [u8],
    sidecar_bytes: &[u8],
    certificate_bytes: &[u8],
    database_file_id: [u8; 16],
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
    let mut certificate_anchors = Vec::new();
    let mut certificates = None;
    let mut stop = None;
    let mut pending_anchor: Option<PendingReplayAnchor> = None;

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
            // This frame has not been reconstructed. Its original checksum
            // validates every tentative preceding group, even if this frame
            // belongs to a still-uncommitted successor transaction.
            pending_anchor = None;
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
        let original_terminal = group_end - frame_size;
        let original_checksum = WalFrameHeader::from_bytes(
            &wal_bytes[original_terminal..original_terminal + WAL_FRAME_HEADER_SIZE],
        )?.checksum;
        decode_proofs.push(recovered.decode_proof);
        if rebuilt[..verified_len] != image[group_start..group_start + verified_len] {
            stop = Some(WalFecReplayStop {
                frame_no: damaged_frame_no, reason: WalFecReplayStopReason::PrefixMismatch,
            });
            break;
        }
        if rebuilt_checksum == original_checksum {
            pending_anchor = None;
        } else if pending_anchor.is_none() {
            pending_anchor = Some(PendingReplayAnchor {
                first_damaged_frame_no: damaged_frame_no,
                committed_frames,
                db_size_pages,
                repaired_frame_count: repaired_frame_nos.len(),
            });
        }

        // Tentative bytes stay in this private plan. Missing groups, decoder
        // failures, admission refusals and end-of-input all discard them unless
        // an independently stored later checksum closes the pending anchor.
        for (index, frame) in rebuilt.chunks_exact(frame_size).enumerate() {
            let offset = group_start + index * frame_size;
            if image[offset..offset + frame_size] != *frame {
                repaired_frame_nos.push(meta.start_frame_no + u32::try_from(index)
                    .map_err(|_| corrupt("repaired frame index overflow"))?);
            }
        }
        image.to_mut()[group_start..group_end].copy_from_slice(&rebuilt);
        if let Some(pending) = pending_anchor {
            let records = certificates.get_or_insert_with(|| {
                read_replay_certificates(
                    certificate_bytes, database_file_id, &header, frame_count, limits,
                )
            });
            if let Some(anchor) = match_replay_certificate(
                records, &image, pending.committed_frames + 1, meta.end_frame_no, frame_size,
            ) {
                certificate_anchors.push(anchor);
                pending_anchor = None;
            }
        }
        frame_index = meta.end_frame_no;
        committed_frames = frame_index;
        running = rebuilt_checksum;
        committed_checksum = running;
        db_size_pages = Some(meta.db_size_pages);
    }

    if let Some(pending) = pending_anchor {
        committed_frames = pending.committed_frames;
        db_size_pages = pending.db_size_pages;
        repaired_frame_nos.truncate(pending.repaired_frame_count);
        // Preserve a more specific failure (e.g. insufficient symbols). Decoder
        // proofs are retained as evidence, not claims of accepted WAL repair.
        if stop.is_none() {
            stop = Some(WalFecReplayStop {
                frame_no: pending.first_damaged_frame_no,
                reason: WalFecReplayStopReason::TerminalAnchorMismatch,
            });
        }
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
        repaired_frame_nos, decode_proofs, certificate_anchors, stop,
    })
}

/// Parse at most the admitted number of bounded records, once per replay.
/// Any malformed envelope invalidates the optional stream; an unrelated bad
/// certificate can never turn checksum-verified WAL bytes into a failure.
fn read_replay_certificates(
    bytes: &[u8],
    database_file_id: [u8; 16],
    header: &WalHeader,
    physical_frames: u32,
    limits: WalFecReplayLimits,
) -> Vec<ParallelWalDurableCertificateRecord> {
    if database_file_id == [0; 16] || bytes.len() > limits.max_certificate_bytes {
        return Vec::new();
    }
    let parse = || -> Option<Vec<ParallelWalDurableCertificateRecord>> {
        let mut records = Vec::new();
        let mut remaining = bytes;
        let mut seen = 0;
        while !remaining.is_empty() {
            if seen >= limits.max_certificate_records {
                return None;
            }
            seen += 1;
            let encoded_len = u32::from_le_bytes(remaining.get(10..14)?.try_into().ok()?);
            let len = usize::try_from(encoded_len).ok()?;
            if !(ParallelWalDurableCertificateRecord::MIN_ENCODED_SIZE
                ..=PARALLEL_WAL_MAX_DURABLE_CERTIFICATE_RECORD_SIZE).contains(&len)
            {
                return None;
            }
            let record = ParallelWalDurableCertificateRecord::from_bytes(remaining.get(..len)?)
                .ok()?;
            remaining = remaining.get(len..)?;
            if record.db_file_id == database_file_id
                && record.wal_generation == WalGenerationIdentity::from_header(header)
                && record.wal_frame_end <= u64::from(physical_frames)
            {
                records.push(record);
            }
        }
        Some(records)
    };
    parse().unwrap_or_default()
}

/// Unlike a rolling WAL checksum, a certificate digest binds ONLY its own
/// interval. A later certificate starting after `first_unanchored` must never
/// bless an earlier tentative transaction, even if its own digest matches.
fn match_replay_certificate(
    records: &[ParallelWalDurableCertificateRecord],
    image: &[u8],
    first_unanchored: u32,
    end_frame_no: u32,
    frame_size: usize,
) -> Option<WalFecReplayCertificateAnchor> {
    let mut candidates = records.iter().filter(|record| {
        record.wal_frame_start <= u64::from(first_unanchored)
            && record.wal_frame_end == u64::from(end_frame_no)
    });
    let record = candidates.next()?;
    // An exact retry is harmless. Do not select an arbitrary authority among
    // contradictory, independently checksummed records for the same endpoint.
    if candidates.any(|other| other != record) {
        return None;
    }
    let start_frame_no = u32::try_from(record.wal_frame_start).ok()?;
    let start = frame_offset(start_frame_no.checked_sub(1)?, frame_size).ok()?;
    let end = frame_offset(end_frame_no, frame_size).ok()?;
    if start_frame_no > 1 {
        let previous = image.get(start.checked_sub(frame_size)?..start)?;
        if !WalFrameHeader::from_bytes(previous).ok()?.is_commit() {
            return None;
        }
    }
    let mut digest = ParallelWalFramePayloadDigestBuilder::new();
    let mut terminal = None;
    for frame in image.get(start..end)?.chunks_exact(frame_size) {
        let header = WalFrameHeader::from_bytes(frame).ok()?;
        let page = PageNumber::new(header.page_number)?;
        digest.update(page, header.db_size, &frame[WAL_FRAME_HEADER_SIZE..]);
        terminal = Some(header);
    }
    let terminal = terminal?;
    let generation = WalGenerationIdentity::from_header(&WalHeader::from_bytes(image).ok()?);
    if !terminal.is_commit()
        || terminal.db_size != record.certificate.db_size_pages
        || !record.authorizes_wal_boundary(
            generation, u64::from(end_frame_no), u64::from(end_frame_no), digest.finalize(),
        )
    {
        return None;
    }
    Some(WalFecReplayCertificateAnchor {
        start_frame_no,
        end_frame_no,
        certificate_epoch: record.certificate.certificate_epoch,
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
    fn terminal_header_fields_are_restored_using_the_original_checksum() {
        for magic in [WAL_MAGIC_LE, WAL_MAGIC_BE] {
            let mut original = header(magic).to_bytes().unwrap().to_vec();
            let sidecar = append_group(&mut original, 3, 8, 9);
            let terminal = frame_offset(2, FRAME_SIZE).unwrap();
            // Page number, commit size and both salts are repairable. The
            // original checksum words, not corrupted header fields, anchor it.
            for field_byte in 0..16 {
                let mut damaged = original.clone();
                damaged[terminal + field_byte] ^= 0x80;
                let saved = damaged.clone();
                let result = recover_wal_fec_image(
                    &damaged, &sidecar, WalFecReplayLimits::default(),
                ).unwrap();
                assert_eq!(result.committed_frames(), 3);
                assert_eq!(result.repaired_frame_nos(), &[3]);
                assert!(!result.decode_proofs()[0].decode_attempted);
                assert_eq!(result.complete_image().unwrap().as_ref(), original);
                assert_eq!(damaged, saved);
            }
        }
    }

    fn corrupt_checksum(wal: &mut [u8], frame_no: u32) {
        let offset = frame_offset(frame_no - 1, FRAME_SIZE).unwrap();
        wal[offset + 16] ^= 1;
    }

    #[test]
    fn damaged_terminal_checksum_uses_a_later_original_frame() {
        for magic in [WAL_MAGIC_LE, WAL_MAGIC_BE] {
            let mut wal = header(magic).to_bytes().unwrap().to_vec();
            let sidecar = append_group(&mut wal, 3, 8, 21);
            append_group(&mut wal, 2, 8, 22); // No FEC is needed for this successor.
            let expected = wal.clone();
            corrupt_checksum(&mut wal, 3);
            let damaged = wal.clone();
            let result = recover_wal_fec_image(
                &wal, &sidecar, WalFecReplayLimits::default(),
            ).unwrap();
            assert_eq!(result.committed_frames(), 5);
            assert_eq!(result.repaired_frame_nos(), &[3]);
            assert!(!result.decode_proofs()[0].decode_attempted);
            assert_eq!(result.complete_image().unwrap().as_ref(), expected);
            assert_eq!(wal, damaged);
        }
    }

    #[test]
    fn consecutive_damaged_anchors_wait_for_an_original_later_checksum() {
        for magic in [WAL_MAGIC_LE, WAL_MAGIC_BE] {
            let mut wal = header(magic).to_bytes().unwrap().to_vec();
            let mut sidecar = append_group(&mut wal, 3, 8, 23);
            sidecar.extend(append_group(&mut wal, 4, 8, 24));
            sidecar.extend(append_group(&mut wal, 2, 8, 25));
            let expected = wal.clone();
            let terminal = frame_offset(2, FRAME_SIZE).unwrap();
            wal[terminal..terminal + WAL_FRAME_HEADER_SIZE].fill(0);
            // Neither successor starts with a valid original frame, so all
            // three groups must wait for the third group's original terminal.
            corrupt_payload(&mut wal, 4);
            corrupt_checksum(&mut wal, 7);
            corrupt_payload(&mut wal, 8);
            let result = recover_wal_fec_image(
                &wal, &sidecar, WalFecReplayLimits::default(),
            ).unwrap();
            assert_eq!(result.committed_frames(), 9);
            assert_eq!(result.repaired_frame_nos(), &[3, 4, 7, 8]);
            assert_eq!(result.decode_proofs().len(), 3);
            assert!(result.decode_proofs()[1].decode_attempted);
            assert!(result.decode_proofs()[2].decode_attempted);
            assert_eq!(result.complete_image().unwrap().as_ref(), expected);
        }
    }

    #[test]
    fn unanchored_chain_never_enters_the_returned_prefix_or_database() {
        let mut wal = header(WAL_MAGIC_LE).to_bytes().unwrap().to_vec();
        append_group(&mut wal, 2, 8, 26);
        let accepted = wal.clone();
        let mut sidecar = append_group(&mut wal, 3, 8, 27);
        sidecar.extend(append_group(&mut wal, 3, 8, 28));
        corrupt_checksum(&mut wal, 5);
        corrupt_payload(&mut wal, 6);
        corrupt_checksum(&mut wal, 8);
        let result = recover_wal_fec_image(
            &wal, &sidecar, WalFecReplayLimits::default(),
        ).unwrap();
        assert_eq!(result.committed_frames(), 2);
        assert_eq!(result.db_size_pages(), Some(100));
        assert_eq!(result.replayable_prefix(), accepted);
        assert_eq!(result.discarded_tail_bytes(), 6 * FRAME_SIZE);
        assert!(result.repaired_frame_nos().is_empty());
        assert_eq!(result.decode_proofs().len(), 2);
        assert!(result.database_image(&[0; PAGE_SIZE], 100 * PAGE_SIZE).is_err());
        assert!(result.complete_image().is_err());
    }

    #[test]
    fn unanchored_growth_does_not_change_the_reported_database_size() {
        let mut wal = header(WAL_MAGIC_LE).to_bytes().unwrap().to_vec();
        append_database_frames(&mut wal, &[(1, database_page_one(PAGE_SIZE_U32, 1))], 1);
        let accepted = wal.clone();
        let sidecar = append_group(&mut wal, 3, 8, 42); // Tentative size is 100.
        corrupt_checksum(&mut wal, 4);
        let result = recover_wal_fec_image(
            &wal, &sidecar, WalFecReplayLimits::default(),
        ).unwrap();
        assert_eq!(result.committed_frames(), 1);
        assert_eq!(result.db_size_pages(), Some(1));
        assert_eq!(result.replayable_prefix(), accepted);
        assert!(result.complete_image().is_err());
    }

    #[test]
    fn an_accepted_earlier_repair_survives_later_anchor_failure() {
        let mut wal = header(WAL_MAGIC_BE).to_bytes().unwrap().to_vec();
        let mut sidecar = append_group(&mut wal, 2, 8, 29);
        let accepted = wal.clone();
        sidecar.extend(append_group(&mut wal, 3, 8, 30));
        sidecar.extend(append_group(&mut wal, 3, 8, 31));
        corrupt_payload(&mut wal, 1);
        corrupt_checksum(&mut wal, 5);
        corrupt_payload(&mut wal, 6);
        corrupt_checksum(&mut wal, 8);
        let result = recover_wal_fec_image(
            &wal, &sidecar, WalFecReplayLimits::default(),
        ).unwrap();
        assert_eq!(result.committed_frames(), 2);
        assert_eq!(result.repaired_frame_nos(), &[1]);
        assert_eq!(result.replayable_prefix(), accepted);
        assert_eq!(result.decode_proofs().len(), 3);
        assert!(result.complete_image().is_err());
    }

    #[test]
    fn uncommitted_successor_anchors_without_becoming_a_commit() {
        let mut wal = header(WAL_MAGIC_LE).to_bytes().unwrap().to_vec();
        let sidecar = append_group(&mut wal, 3, 8, 32);
        let accepted = wal.clone();
        append_database_frames(&mut wal, &[(9, vec![6; PAGE_SIZE])], 0);
        corrupt_checksum(&mut wal, 3);
        let result = recover_wal_fec_image(
            &wal, &sidecar, WalFecReplayLimits::default(),
        ).unwrap();
        assert_eq!(result.committed_frames(), 3);
        assert_eq!(result.repaired_frame_nos(), &[3]);
        assert_eq!(result.discarded_tail_bytes(), FRAME_SIZE);
        assert_eq!(result.complete_image().unwrap().as_ref(), accepted);
    }

    #[test]
    fn missing_successor_group_discards_the_unanchored_predecessor() {
        let mut wal = header(WAL_MAGIC_LE).to_bytes().unwrap().to_vec();
        append_group(&mut wal, 2, 8, 33);
        let accepted = wal.clone();
        let sidecar = append_group(&mut wal, 3, 8, 34);
        append_group(&mut wal, 2, 8, 35);
        corrupt_checksum(&mut wal, 5);
        corrupt_payload(&mut wal, 6);
        let result = recover_wal_fec_image(
            &wal, &sidecar, WalFecReplayLimits::default(),
        ).unwrap();
        assert_eq!(result.stop().unwrap().reason, WalFecReplayStopReason::MissingGroup);
        assert_eq!(result.replayable_prefix(), accepted);
        assert!(result.repaired_frame_nos().is_empty());
        assert!(result.complete_image().is_err());
    }

    #[test]
    fn later_anchors_cannot_validate_a_substituted_generation_or_commit_size() {
        for change_generation in [false, true] {
            let mut wal = header(WAL_MAGIC_LE).to_bytes().unwrap().to_vec();
            let mut sidecar = append_group(&mut wal, 3, 8, 36);
            sidecar.extend(append_group(&mut wal, 2, 8, 37));
            if change_generation {
                let mut replacement = header(WAL_MAGIC_LE);
                replacement.checkpoint_seq += 1;
                wal[..WAL_HEADER_SIZE].copy_from_slice(&replacement.to_bytes().unwrap());
            } else {
                let mut records = crate::wal_fec::scan_wal_fec_bytes(
                    std::path::Path::new("snapshot-only"), &sidecar,
                ).unwrap().groups;
                records[0].meta.db_size_pages += 1;
                records[0].meta.checksum = records[0].meta.compute_checksum();
                sidecar = encode_wal_fec_group(&records[0]).unwrap();
                sidecar.extend(encode_wal_fec_group(&records[1]).unwrap());
            }
            corrupt_payload(&mut wal, 1);
            let result = recover_wal_fec_image(
                &wal, &sidecar, WalFecReplayLimits::default(),
            ).unwrap();
            assert_eq!(result.committed_frames(), 0);
            assert!(result.repaired_frame_nos().is_empty());
            assert!(result.complete_image().is_err());
        }
    }

    #[test]
    fn successor_resource_refusal_discards_tentative_commits() {
        let mut wal = header(WAL_MAGIC_LE).to_bytes().unwrap().to_vec();
        append_group(&mut wal, 2, 8, 38);
        let accepted = wal.clone();
        let mut sidecar = append_group(&mut wal, 3, 8, 39);
        sidecar.extend(append_group(&mut wal, 4, 8, 40));
        corrupt_checksum(&mut wal, 5);
        corrupt_payload(&mut wal, 6);
        let limits = WalFecReplayLimits { max_source_pages: 3, ..WalFecReplayLimits::default() };
        let result = recover_wal_fec_image(&wal, &sidecar, limits).unwrap();
        assert_eq!(result.stop().unwrap().reason, WalFecReplayStopReason::ResourceLimit);
        assert_eq!(result.decode_proofs().len(), 1);
        assert!(result.repaired_frame_nos().is_empty());
        assert_eq!(result.replayable_prefix(), accepted);
    }

    #[test]
    fn a_partial_successor_is_not_an_original_checksum_anchor() {
        let mut wal = header(WAL_MAGIC_LE).to_bytes().unwrap().to_vec();
        let sidecar = append_group(&mut wal, 3, 8, 41);
        corrupt_checksum(&mut wal, 3);
        wal.extend_from_slice(&[0; 17]);
        let result = recover_wal_fec_image(
            &wal, &sidecar, WalFecReplayLimits::default(),
        ).unwrap();
        assert_eq!(result.committed_frames(), 0);
        assert_eq!(result.stop().unwrap().reason, WalFecReplayStopReason::TerminalAnchorMismatch);
        assert!(result.repaired_frame_nos().is_empty());
        assert!(result.complete_image().is_err());
    }

    #[test]
    fn erased_commit_marker_and_corrupted_payload_recover_together() {
        for magic in [WAL_MAGIC_LE, WAL_MAGIC_BE] {
            let mut wal = header(magic).to_bytes().unwrap().to_vec();
            let sidecar = append_group(&mut wal, 5, 8, 11);
            let expected = wal.clone();
            let terminal = frame_offset(4, FRAME_SIZE).unwrap();
            wal[terminal + 4..terminal + 8].fill(0);
            corrupt_payload(&mut wal, 2);
            corrupt_payload(&mut wal, 5);
            let result = recover_wal_fec_image(
                &wal, &sidecar, WalFecReplayLimits::default(),
            ).unwrap();
            assert_eq!(result.repaired_frame_nos(), &[2, 5]);
            assert_eq!(result.db_size_pages(), Some(100));
            assert!(result.decode_proofs()[0].decode_attempted);
            assert_eq!(result.complete_image().unwrap().as_ref(), expected);
        }
    }

    #[test]
    fn sidecar_cannot_substitute_a_different_terminal_page_number() {
        let mut wal = header(WAL_MAGIC_LE).to_bytes().unwrap().to_vec();
        let sidecar = append_group(&mut wal, 3, 8, 12);
        let mut records = crate::wal_fec::scan_wal_fec_bytes(
            std::path::Path::new("snapshot-only"), &sidecar,
        ).unwrap().groups;
        records[0].meta.page_numbers[2] += 1;
        records[0].meta.checksum = records[0].meta.compute_checksum();
        let substituted = encode_wal_fec_group(&records[0]).unwrap();
        corrupt_payload(&mut wal, 1);
        let result = recover_wal_fec_image(
            &wal, &substituted, WalFecReplayLimits::default(),
        ).unwrap();
        assert_eq!(result.stop().unwrap().reason, WalFecReplayStopReason::TerminalAnchorMismatch);
        assert!(result.repaired_frame_nos().is_empty());
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

    fn database_page_one(page_size: u32, pages: u32) -> Vec<u8> {
        let mut page = vec![0; usize::try_from(page_size).unwrap()];
        page[..16].copy_from_slice(b"SQLite format 3\0");
        let encoded = if page_size == 65_536 { 1 } else { u16::try_from(page_size).unwrap() };
        page[16..18].copy_from_slice(&encoded.to_be_bytes());
        page[18..20].copy_from_slice(&[2, 2]);
        page[21..24].copy_from_slice(&[64, 32, 32]);
        page[24..28].copy_from_slice(&7_u32.to_be_bytes());
        page[28..32].copy_from_slice(&pages.to_be_bytes());
        page[44..48].copy_from_slice(&4_u32.to_be_bytes());
        page[56..60].copy_from_slice(&1_u32.to_be_bytes());
        page[60..64].copy_from_slice(&123_u32.to_be_bytes());
        page[68..72].copy_from_slice(&456_u32.to_be_bytes());
        page[92..96].copy_from_slice(&7_u32.to_be_bytes());
        page[100] = 13; // Empty sqlite_schema leaf, for header/provenance tests.
        let cell_start = if page_size == 65_536 { 0 } else { encoded };
        page[105..107].copy_from_slice(&cell_start.to_be_bytes());
        page
    }

    fn append_database_frames(wal: &mut Vec<u8>, pages: &[(u32, Vec<u8>)], commit_size: u32) {
        let header = WalHeader::from_bytes(wal).unwrap();
        let page_size = usize::try_from(header.page_size).unwrap();
        let frame_size = WAL_FRAME_HEADER_SIZE + page_size;
        let mut running = if wal.len() == WAL_HEADER_SIZE {
            header.checksum
        } else {
            WalFrameHeader::from_bytes(&wal[wal.len() - frame_size..]).unwrap().checksum
        };
        for (index, (number, page)) in pages.iter().enumerate() {
            assert_eq!(page.len(), page_size);
            let start = wal.len();
            wal.extend_from_slice(&WalFrameHeader {
                page_number: *number,
                db_size: if index + 1 == pages.len() { commit_size } else { 0 },
                salts: header.salts,
                checksum: SqliteWalChecksum::default(),
            }.to_bytes());
            wal.extend_from_slice(page);
            running = WalChecksumTransform::for_wal_frame(
                &wal[start..], page_size, header.big_endian_checksum(),
            ).unwrap().apply(running);
            wal[start + 16..start + 20].copy_from_slice(&running.s1.to_be_bytes());
            wal[start + 20..start + 24].copy_from_slice(&running.s2.to_be_bytes());
        }
    }

    #[test]
    fn database_image_uses_latest_committed_pages_and_preserves_metadata() {
        let mut database = database_page_one(PAGE_SIZE_U32, 2);
        database.extend_from_slice(&[1; PAGE_SIZE]);
        let original = database.clone();
        let mut wal = header(WAL_MAGIC_LE).to_bytes().unwrap().to_vec();
        append_database_frames(&mut wal, &[(2, vec![2; PAGE_SIZE])], 2);
        append_database_frames(&mut wal, &[(2, vec![3; PAGE_SIZE])], 2);
        // A valid but uncommitted suffix must not affect the exported database.
        append_database_frames(&mut wal, &[(2, vec![4; PAGE_SIZE])], 0);
        let result = recover_wal_fec_image(&wal, &[], WalFecReplayLimits::default()).unwrap();
        let image = result.database_image(&database, 8 * PAGE_SIZE).unwrap();
        assert_eq!(&image[..PAGE_SIZE], &database[..PAGE_SIZE]);
        assert_eq!(&image[PAGE_SIZE..], &[3; PAGE_SIZE]);
        assert_eq!(database, original);
        assert_eq!(result.committed_frames(), 2);
    }

    #[test]
    fn database_image_can_rebuild_an_empty_base_with_complete_wal_coverage() {
        let page_one = database_page_one(PAGE_SIZE_U32, 2);
        let mut wal = header(WAL_MAGIC_LE).to_bytes().unwrap().to_vec();
        append_database_frames(&mut wal, &[(1, page_one.clone()), (2, vec![7; PAGE_SIZE])], 2);
        let result = recover_wal_fec_image(&wal, &[], WalFecReplayLimits::default()).unwrap();
        let image = result.database_image(&[], 2 * PAGE_SIZE).unwrap();
        assert_eq!(&image[..PAGE_SIZE], page_one);
        assert_eq!(&image[PAGE_SIZE..], &[7; PAGE_SIZE]);
    }

    #[test]
    fn database_image_refuses_missing_growth_pages() {
        let database = database_page_one(PAGE_SIZE_U32, 1);
        let mut wal = header(WAL_MAGIC_LE).to_bytes().unwrap().to_vec();
        append_database_frames(&mut wal, &[
            (1, database_page_one(PAGE_SIZE_U32, 3)), (3, vec![9; PAGE_SIZE]),
        ], 3);
        let result = recover_wal_fec_image(&wal, &[], WalFecReplayLimits::default()).unwrap();
        assert!(result.database_image(&database, 3 * PAGE_SIZE).unwrap_err()
            .to_string().contains("page 2 has no surviving source"));
    }

    #[test]
    fn database_image_applies_commit_shrink() {
        let mut database = database_page_one(PAGE_SIZE_U32, 3);
        database.extend_from_slice(&[2; PAGE_SIZE]);
        database.extend_from_slice(&[3; PAGE_SIZE]);
        let mut wal = header(WAL_MAGIC_LE).to_bytes().unwrap().to_vec();
        append_database_frames(&mut wal, &[(1, database_page_one(PAGE_SIZE_U32, 2))], 2);
        let result = recover_wal_fec_image(&wal, &[], WalFecReplayLimits::default()).unwrap();
        let image = result.database_image(&database, 4 * PAGE_SIZE).unwrap();
        assert_eq!(image.len(), 2 * PAGE_SIZE);
        assert_eq!(&image[PAGE_SIZE..], &[2; PAGE_SIZE]);
    }

    #[test]
    fn database_image_cannot_resurrect_pages_across_shrink_and_regrowth() {
        let mut database = database_page_one(PAGE_SIZE_U32, 3);
        database.extend_from_slice(&[2; 2 * PAGE_SIZE]);
        let mut wal = header(WAL_MAGIC_LE).to_bytes().unwrap().to_vec();
        append_database_frames(&mut wal, &[(3, vec![9; PAGE_SIZE])], 3);
        append_database_frames(&mut wal, &[(1, database_page_one(PAGE_SIZE_U32, 1))], 1);
        append_database_frames(&mut wal, &[
            (1, database_page_one(PAGE_SIZE_U32, 3)), (2, vec![6; PAGE_SIZE]),
        ], 3);
        let result = recover_wal_fec_image(&wal, &[], WalFecReplayLimits::default()).unwrap();
        assert!(result.database_image(&database, 4 * PAGE_SIZE).unwrap_err()
            .to_string().contains("page 3 has no surviving source"));
        // A new post-shrink image supplies the missing provenance.
        append_database_frames(&mut wal, &[(3, vec![8; PAGE_SIZE])], 3);
        let result = recover_wal_fec_image(&wal, &[], WalFecReplayLimits::default()).unwrap();
        let image = result.database_image(&database, 4 * PAGE_SIZE).unwrap();
        assert_eq!(&image[PAGE_SIZE..2 * PAGE_SIZE], &[6; PAGE_SIZE]);
        assert_eq!(&image[2 * PAGE_SIZE..], &[8; PAGE_SIZE]);
    }

    #[test]
    fn database_image_refuses_a_fallback_even_with_a_full_main_file() {
        let database = database_page_one(PAGE_SIZE_U32, 1);
        let mut wal = header(WAL_MAGIC_LE).to_bytes().unwrap().to_vec();
        append_database_frames(&mut wal, &[(1, database.clone())], 1);
        corrupt_payload(&mut wal, 1);
        let result = recover_wal_fec_image(&wal, &[], WalFecReplayLimits::default()).unwrap();
        assert!(result.database_image(&database, 4 * PAGE_SIZE).unwrap_err()
            .to_string().contains("incomplete WAL recovery"));
    }

    #[test]
    fn database_image_checks_input_output_and_header_boundaries() {
        let database = database_page_one(PAGE_SIZE_U32, 1);
        let mut wal = header(WAL_MAGIC_LE).to_bytes().unwrap().to_vec();
        append_database_frames(&mut wal, &[(1, database.clone())], 1);
        let result = recover_wal_fec_image(&wal, &[], WalFecReplayLimits::default()).unwrap();
        assert!(result.database_image(&database, PAGE_SIZE - 1).is_err());
        assert!(result.database_image(&database[..PAGE_SIZE - 1], 4 * PAGE_SIZE).is_err());
        assert!(result.database_image(&database_page_one(1024, 1), 4 * PAGE_SIZE).is_err());
        let mut malformed = database.clone();
        malformed[21] = 63;
        assert!(result.database_image(&malformed, 4 * PAGE_SIZE).is_err());
        append_database_frames(&mut wal, &[(1, database_page_one(PAGE_SIZE_U32, 3))], 2);
        let result = recover_wal_fec_image(&wal, &[], WalFecReplayLimits::default()).unwrap();
        assert!(result.database_image(&database, PAGE_SIZE).unwrap_err()
            .to_string().contains("output limit"));
    }

    #[test]
    fn database_image_refuses_authoritative_page_one_size_disagreement() {
        let mut database = database_page_one(PAGE_SIZE_U32, 2);
        database.extend_from_slice(&[2; PAGE_SIZE]);
        let mut wal = header(WAL_MAGIC_LE).to_bytes().unwrap().to_vec();
        append_database_frames(&mut wal, &[(1, database_page_one(PAGE_SIZE_U32, 3))], 2);
        let result = recover_wal_fec_image(&wal, &[], WalFecReplayLimits::default()).unwrap();
        assert!(result.database_image(&database, 4 * PAGE_SIZE).unwrap_err()
            .to_string().contains("page-one size disagrees"));
    }

    #[test]
    fn database_image_respects_legacy_size_validity_and_64k_pages() {
        for page_size in [PAGE_SIZE_U32, 65_536] {
            let size = usize::try_from(page_size).unwrap();
            let mut database = database_page_one(page_size, 99);
            database[92..96].copy_from_slice(&6_u32.to_be_bytes()); // Size is stale.
            let mut wal_header = header(WAL_MAGIC_BE);
            wal_header.page_size = page_size;
            let wal = wal_header.to_bytes().unwrap();
            let result = recover_wal_fec_image(&wal, &[], WalFecReplayLimits::default()).unwrap();
            assert_eq!(result.database_image(&database, size).unwrap(), database);
            assert!(result.database_image(&[], size).is_err());
        }
    }

    #[test]
    fn database_image_trims_stale_physical_tail_without_a_wal_commit() {
        let mut database = database_page_one(PAGE_SIZE_U32, 1);
        database.extend_from_slice(&[0xaa; PAGE_SIZE]);
        let wal = header(WAL_MAGIC_LE).to_bytes().unwrap();
        let result = recover_wal_fec_image(&wal, &[], WalFecReplayLimits::default()).unwrap();
        assert_eq!(result.database_image(&database, 2 * PAGE_SIZE).unwrap(), &database[..PAGE_SIZE]);
    }

    const CERTIFICATE_DATABASE_ID: [u8; 16] = [0x6d; 16];

    fn certificate_for(
        wal: &[u8], start_frame: u32, end_frame: u32,
    ) -> ParallelWalDurableCertificateRecord {
        use crate::parallel_wal::{
            PARALLEL_WAL_COMMIT_CERTIFICATE_VERSION, ParallelWalCommitCertificate,
            ParallelWalOrderedResidue,
        };
        use fsqlite_types::CommitSeq;

        let mut digest = ParallelWalFramePayloadDigestBuilder::new();
        let mut commits = 0_u64;
        let mut pages = std::collections::BTreeSet::new();
        let mut db_size = 0;
        for number in start_frame..=end_frame {
            let offset = frame_offset(number - 1, FRAME_SIZE).unwrap();
            let frame = &wal[offset..offset + FRAME_SIZE];
            let header = WalFrameHeader::from_bytes(frame).unwrap();
            pages.insert(header.page_number);
            commits += u64::from(header.is_commit());
            db_size = header.db_size;
            digest.update(PageNumber::new(header.page_number).unwrap(), db_size,
                &frame[WAL_FRAME_HEADER_SIZE..]);
        }
        let mut certificate = ParallelWalCommitCertificate {
            format_version: PARALLEL_WAL_COMMIT_CERTIFICATE_VERSION,
            residue: ParallelWalOrderedResidue::CommitCertificateThenPublish,
            certificate_epoch: 7,
            commit_seq_lo: CommitSeq::new(1),
            commit_seq_hi: CommitSeq::new(commits),
            durable_segment_epoch: 7,
            lane_count: 1,
            lane_record_counts: vec![end_frame - start_frame + 1],
            db_size_pages: db_size,
            page_set_size: u32::try_from(pages.len()).unwrap(),
            wal_frame_payload_digest: digest.finalize(),
            certificate_crc32c: 0,
            fallback_active: false,
        };
        certificate.certificate_crc32c = certificate.computed_crc32c();
        ParallelWalDurableCertificateRecord::new(
            WalGenerationIdentity::from_header(&WalHeader::from_bytes(wal).unwrap()),
            u64::from(start_frame), u64::from(end_frame), CERTIFICATE_DATABASE_ID, certificate,
        ).unwrap()
    }

    fn certificate_recovery<'a>(
        wal: &'a [u8], sidecar: &[u8], certificates: &[u8],
    ) -> WalFecReplayResult<'a> {
        recover_wal_fec_image_with_certificates(
            wal, sidecar, certificates, CERTIFICATE_DATABASE_ID, WalFecReplayLimits::default(),
        ).unwrap()
    }

    #[test]
    fn certificate_restores_final_commit_without_a_surviving_checksum() {
        for magic in [WAL_MAGIC_LE, WAL_MAGIC_BE] {
            let mut wal = header(magic).to_bytes().unwrap().to_vec();
            let sidecar = append_group(&mut wal, 5, 8, 50);
            let certificate = certificate_for(&wal, 1, 5).to_bytes();
            let expected = wal.clone();
            let terminal = frame_offset(4, FRAME_SIZE).unwrap();
            wal[terminal..terminal + WAL_FRAME_HEADER_SIZE].fill(0);
            corrupt_payload(&mut wal, 2);
            corrupt_payload(&mut wal, 5);
            let damaged = wal.clone();
            let result = certificate_recovery(&wal, &sidecar, &certificate);
            assert_eq!(result.committed_frames(), 5);
            assert_eq!(result.repaired_frame_nos(), &[2, 5]);
            assert_eq!(result.certificate_anchors(), &[WalFecReplayCertificateAnchor {
                start_frame_no: 1, end_frame_no: 5, certificate_epoch: 7,
            }]);
            assert_eq!(result.complete_image().unwrap().as_ref(), expected);
            assert_eq!(wal, damaged);
            assert!(recover_wal_fec_image(&wal, &sidecar, WalFecReplayLimits::default())
                .unwrap().complete_image().is_err());
        }
    }

    #[test]
    fn certificate_must_cover_the_entire_unanchored_chain() {
        let mut wal = header(WAL_MAGIC_LE).to_bytes().unwrap().to_vec();
        let mut sidecar = append_group(&mut wal, 3, 8, 51);
        sidecar.extend(append_group(&mut wal, 4, 8, 52));
        let full = certificate_for(&wal, 1, 7).to_bytes();
        let narrow = certificate_for(&wal, 4, 7).to_bytes();
        let expected = wal.clone();
        corrupt_checksum(&mut wal, 3);
        corrupt_payload(&mut wal, 4);
        corrupt_checksum(&mut wal, 7);
        let rejected = certificate_recovery(&wal, &sidecar, &narrow);
        assert_eq!(rejected.committed_frames(), 0);
        assert!(rejected.certificate_anchors().is_empty());
        assert!(rejected.complete_image().is_err());
        let recovered = certificate_recovery(&wal, &sidecar, &full);
        assert_eq!(recovered.repaired_frame_nos(), &[3, 4, 7]);
        assert_eq!(recovered.complete_image().unwrap().as_ref(), expected);
    }

    #[test]
    fn certificate_interval_must_start_at_a_transaction_boundary() {
        let mut wal = header(WAL_MAGIC_LE).to_bytes().unwrap().to_vec();
        append_group(&mut wal, 3, 8, 53);
        let prefix = wal.clone();
        let sidecar = append_group(&mut wal, 3, 8, 54);
        let valid = certificate_for(&wal, 4, 6).to_bytes();
        let mid_transaction = certificate_for(&wal, 2, 6).to_bytes();
        let expected = wal.clone();
        corrupt_checksum(&mut wal, 6);
        let rejected = certificate_recovery(&wal, &sidecar, &mid_transaction);
        assert_eq!(rejected.replayable_prefix(), prefix);
        assert!(rejected.complete_image().is_err());
        assert_eq!(certificate_recovery(&wal, &sidecar, &valid)
            .complete_image().unwrap().as_ref(), expected);
    }

    #[test]
    fn certificates_require_nonzero_matching_main_file_identity() {
        let mut wal = header(WAL_MAGIC_LE).to_bytes().unwrap().to_vec();
        let sidecar = append_group(&mut wal, 3, 8, 55);
        let record = certificate_for(&wal, 1, 3);
        corrupt_checksum(&mut wal, 3);
        for identity in [[0; 16], [0x7a; 16]] {
            let result = recover_wal_fec_image_with_certificates(
                &wal, &sidecar, &record.to_bytes(), identity, WalFecReplayLimits::default(),
            ).unwrap();
            assert!(result.certificate_anchors().is_empty());
            assert!(result.complete_image().is_err());
        }
        let mut legacy = record;
        legacy.db_file_id = [0; 16];
        assert!(certificate_recovery(&wal, &sidecar, &legacy.to_bytes())
            .complete_image().is_err());
    }

    #[test]
    fn valid_envelope_cannot_substitute_generation_payload_or_database_size() {
        let mut wal = header(WAL_MAGIC_LE).to_bytes().unwrap().to_vec();
        let sidecar = append_group(&mut wal, 3, 8, 56);
        let original = certificate_for(&wal, 1, 3);
        corrupt_checksum(&mut wal, 3);
        for change in 0..6 {
            let mut record = original.clone();
            match change {
                0 => record.wal_generation.checkpoint_seq += 1,
                1 => record.wal_generation.salts.salt1 ^= 1,
                2 => record.certificate.wal_frame_payload_digest[0] ^= 1,
                3 => record.certificate.db_size_pages += 1,
                4 => record.wal_frame_end += 1,
                _ => record.db_file_id[0] ^= 1,
            }
            record.certificate.certificate_crc32c = record.certificate.computed_crc32c();
            let encoded = record.to_bytes();
            assert!(ParallelWalDurableCertificateRecord::from_bytes(&encoded).is_ok());
            assert!(certificate_recovery(&wal, &sidecar, &encoded).complete_image().is_err());
        }
    }

    #[test]
    fn certificate_retries_are_deduplicated_but_conflicting_authorities_are_refused() {
        let mut wal = header(WAL_MAGIC_LE).to_bytes().unwrap().to_vec();
        let sidecar = append_group(&mut wal, 3, 8, 57);
        let mut record = certificate_for(&wal, 1, 3);
        let encoded = record.to_bytes();
        let expected = wal.clone();
        corrupt_checksum(&mut wal, 3);
        let mut duplicate = encoded.clone();
        duplicate.extend_from_slice(&encoded);
        assert_eq!(certificate_recovery(&wal, &sidecar, &duplicate)
            .complete_image().unwrap().as_ref(), expected);
        record.certificate.wal_frame_payload_digest[0] ^= 1;
        record.certificate.certificate_crc32c = record.certificate.computed_crc32c();
        for reversed in [false, true] {
            let mut conflicting = if reversed { record.to_bytes() } else { encoded.clone() };
            conflicting.extend(if reversed { encoded.clone() } else { record.to_bytes() });
            assert!(certificate_recovery(&wal, &sidecar, &conflicting).complete_image().is_err());
        }
    }

    #[test]
    fn malformed_or_over_budget_certificates_never_authorize_recovery() {
        let mut wal = header(WAL_MAGIC_LE).to_bytes().unwrap().to_vec();
        let sidecar = append_group(&mut wal, 3, 8, 58);
        let encoded = certificate_for(&wal, 1, 3).to_bytes();
        corrupt_checksum(&mut wal, 3);
        for len in [0, 1, 13, encoded.len() - 1] {
            assert!(certificate_recovery(&wal, &sidecar, &encoded[..len])
                .complete_image().is_err());
        }
        for index in 0..encoded.len() {
            let mut corrupted = encoded.clone();
            corrupted[index] ^= 1;
            assert!(certificate_recovery(&wal, &sidecar, &corrupted).complete_image().is_err());
        }
        for limits in [
            WalFecReplayLimits { max_certificate_bytes: encoded.len() - 1, ..WalFecReplayLimits::default() },
            WalFecReplayLimits { max_certificate_records: 0, ..WalFecReplayLimits::default() },
        ] {
            assert!(recover_wal_fec_image_with_certificates(
                &wal, &sidecar, &encoded, CERTIFICATE_DATABASE_ID, limits,
            ).unwrap().complete_image().is_err());
        }
        let mut torn_tail = encoded;
        torn_tail.push(0);
        assert!(certificate_recovery(&wal, &sidecar, &torn_tail).complete_image().is_err());
    }

    #[test]
    fn invalid_certificates_do_not_poison_original_checksum_recovery() {
        let mut wal = header(WAL_MAGIC_LE).to_bytes().unwrap().to_vec();
        let sidecar = append_group(&mut wal, 3, 8, 59);
        append_group(&mut wal, 2, 8, 60);
        let expected = wal.clone();
        corrupt_checksum(&mut wal, 3);
        let limits = WalFecReplayLimits { max_certificate_bytes: 0, ..WalFecReplayLimits::default() };
        let result = recover_wal_fec_image_with_certificates(
            &wal, &sidecar, b"invalid optional certificate", CERTIFICATE_DATABASE_ID, limits,
        ).unwrap();
        assert!(result.certificate_anchors().is_empty());
        assert_eq!(result.complete_image().unwrap().as_ref(), expected);
    }

    #[test]
    fn certificate_never_supplies_missing_fec_or_missing_physical_frames() {
        let mut wal = header(WAL_MAGIC_LE).to_bytes().unwrap().to_vec();
        let sidecar = append_group(&mut wal, 3, 8, 61);
        let encoded = certificate_for(&wal, 1, 3).to_bytes();
        corrupt_checksum(&mut wal, 3);
        assert!(certificate_recovery(&wal, &[], &encoded).complete_image().is_err());
        let truncated = &wal[..wal.len() - 1];
        let result = certificate_recovery(truncated, &sidecar, &encoded);
        assert!(result.certificate_anchors().is_empty());
        assert!(result.complete_image().is_err());
        // A certificate written before an append is not proof that the
        // absent commit frame ever reached the WAL, even with old FEC nearby.
        let missing_commit = &wal[..wal.len() - FRAME_SIZE];
        let result = certificate_recovery(missing_commit, &sidecar, &encoded);
        assert_eq!(result.committed_frames(), 0);
        assert!(result.certificate_anchors().is_empty());
        assert_eq!(result.replayable_prefix().len(), WAL_HEADER_SIZE);
    }

    #[test]
    fn accepted_certificate_survives_later_unanchored_failure_without_certifying_it() {
        let mut wal = header(WAL_MAGIC_LE).to_bytes().unwrap().to_vec();
        let mut sidecar = append_group(&mut wal, 3, 8, 62);
        let accepted = wal.clone();
        let encoded = certificate_for(&wal, 1, 3).to_bytes();
        sidecar.extend(append_group(&mut wal, 3, 8, 63));
        corrupt_checksum(&mut wal, 3);
        corrupt_payload(&mut wal, 4);
        corrupt_checksum(&mut wal, 6);
        let result = certificate_recovery(&wal, &sidecar, &encoded);
        assert_eq!(result.committed_frames(), 3);
        assert_eq!(result.repaired_frame_nos(), &[3]);
        assert_eq!(result.certificate_anchors().len(), 1);
        assert_eq!(result.replayable_prefix(), accepted);
        assert!(result.complete_image().is_err());
    }
}
