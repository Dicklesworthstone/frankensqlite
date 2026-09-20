//! Snapshot membership and page coverage fixed before packet arrival.
//!
//! Obtain the expected manifest ID from a trusted control plane. A digest
//! supplied beside an untrusted manifest is not an authentication mechanism.
//! This module validates transfer contents, not atomic source capture or
//! publication of the received database.

pub mod spool;
pub use spool::{SnapshotCheckpoint, SnapshotSpool, SnapshotSpoolState};

use std::collections::{HashMap, HashSet};
use std::fmt;

use fsqlite_error::{FrankenError, Result};
use fsqlite_types::cx::Cx;

use super::{
    BlockDecoder, DecodedBlock, SnapshotPacketResult, SnapshotSender,
    parse_decoded_snapshot_block,
};
use crate::replication_sender::{
    CHANGESET_HEADER_SIZE, ChangesetHeader, ChangesetId, MAX_REPAIR_WORK_BYTES,
    ReplicationPacket, ReplicationWireVersion, compute_changeset_id,
    derive_seed_from_changeset_id, repair_parameters, symbol_schedule_end, validate_codec_esi,
};

const MAGIC: &[u8; 8] = b"FSMAN\0\0\x01";
const HEADER_BYTES: usize = 16;
const BLOCK_BYTES: usize = 74;
const MAX_BLOCKS: usize = 256;
const ID_DOMAIN: &str = "fsqlite:snapshot-manifest:v1";
const BLOCK_DOMAIN: &str = "fsqlite:snapshot-block:v1";

fn corrupt(detail: &str) -> FrankenError {
    FrankenError::DatabaseCorrupt { detail: detail.to_owned() }
}

/// Immutable description of one independently decodable changeset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotBlockManifest {
    changeset_id: ChangesetId,
    first_page: u32,
    page_count: u32,
    k_source: u32,
    r_repair: u32,
    symbol_size: u16,
    encoded_len: u64,
    digest: [u8; 32],
}

impl SnapshotBlockManifest {
    pub const fn changeset_id(&self) -> ChangesetId { self.changeset_id }
    pub const fn first_page(&self) -> u32 { self.first_page }
    pub const fn page_count(&self) -> u32 { self.page_count }
}

/// A canonical, bounded description of every page in one snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotManifest {
    page_size: u32,
    blocks: Vec<SnapshotBlockManifest>,
}

impl SnapshotManifest {
    pub const fn page_size(&self) -> u32 { self.page_size }
    pub fn blocks(&self) -> &[SnapshotBlockManifest] { &self.blocks }

    pub fn id(&self) -> [u8; 32] {
        blake3::derive_key(ID_DOMAIN, &self.to_bytes())
    }

    /// Canonical fixed-width encoding; private fields preserve validation.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(HEADER_BYTES + BLOCK_BYTES * self.blocks.len());
        out.extend_from_slice(MAGIC);
        out.extend_from_slice(&self.page_size.to_le_bytes());
        out.extend_from_slice(&(self.blocks.len() as u32).to_le_bytes());
        for block in &self.blocks {
            out.extend_from_slice(block.changeset_id.as_bytes());
            out.extend_from_slice(&block.first_page.to_le_bytes());
            out.extend_from_slice(&block.page_count.to_le_bytes());
            out.extend_from_slice(&block.k_source.to_le_bytes());
            out.extend_from_slice(&block.r_repair.to_le_bytes());
            out.extend_from_slice(&block.symbol_size.to_le_bytes());
            out.extend_from_slice(&block.encoded_len.to_le_bytes());
            out.extend_from_slice(&block.digest);
        }
        out
    }

    /// Reject dimensions and trailing bytes before allocating block state.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < HEADER_BYTES || &bytes[..8] != MAGIC {
            return Err(corrupt("invalid snapshot manifest header"));
        }
        let page_size = u32::from_le_bytes(bytes[8..12].try_into().expect("header width"));
        let count = u32::from_le_bytes(bytes[12..16].try_into().expect("header width")) as usize;
        if count == 0 || count > MAX_BLOCKS || bytes.len() != HEADER_BYTES + count * BLOCK_BYTES {
            return Err(corrupt("invalid snapshot manifest block count or length"));
        }
        let mut blocks = Vec::with_capacity(count);
        for bytes in bytes[HEADER_BYTES..].chunks_exact(BLOCK_BYTES) {
            blocks.push(SnapshotBlockManifest {
                changeset_id: ChangesetId::from_bytes(bytes[..16].try_into().expect("block width")),
                first_page: u32::from_le_bytes(bytes[16..20].try_into().expect("block width")),
                page_count: u32::from_le_bytes(bytes[20..24].try_into().expect("block width")),
                k_source: u32::from_le_bytes(bytes[24..28].try_into().expect("block width")),
                r_repair: u32::from_le_bytes(bytes[28..32].try_into().expect("block width")),
                symbol_size: u16::from_le_bytes(bytes[32..34].try_into().expect("block width")),
                encoded_len: u64::from_le_bytes(bytes[34..42].try_into().expect("block width")),
                digest: bytes[42..74].try_into().expect("block width"),
            });
        }
        let manifest = Self { page_size, blocks };
        manifest.validate()?;
        Ok(manifest)
    }

    fn validate(&self) -> Result<()> {
        if self.page_size == 0 || self.blocks.is_empty() || self.blocks.len() > MAX_BLOCKS {
            return Err(corrupt("invalid snapshot dimensions"));
        }
        let mut next_page = 1_u64;
        let mut identities = HashSet::new();
        for block in &self.blocks {
            if block.page_count == 0 || u64::from(block.first_page) != next_page
                || !identities.insert(block.changeset_id)
            {
                return Err(corrupt("snapshot has a gap, overlap, empty block or repeated object"));
            }
            next_page += u64::from(block.page_count);
            if next_page > u64::from(u32::MAX) + 1 {
                return Err(FrankenError::TooBig);
            }
            ReplicationPacket::validate_symbol_size(usize::from(block.symbol_size))?;
            let expected = u64::from(block.page_count)
                .checked_mul(u64::from(self.page_size) + 12)
                .and_then(|n| n.checked_add(CHANGESET_HEADER_SIZE as u64))
                .ok_or(FrankenError::TooBig)?;
            if expected != block.encoded_len
                || expected.div_ceil(u64::from(block.symbol_size)) != u64::from(block.k_source)
                || block.k_source == 0
            {
                return Err(corrupt("snapshot block length disagrees with coding dimensions"));
            }
            let end = block.k_source.checked_add(block.r_repair).ok_or(FrankenError::TooBig)?;
            validate_codec_esi(end - 1)?;
            let k = block.k_source as usize;
            repair_parameters(k, usize::from(block.symbol_size), k, MAX_REPAIR_WORK_BYTES)?;
        }
        Ok(())
    }
}

impl SnapshotSender {
    /// Bind the manifest to encoded bytes, not mutable public partition hints.
    pub fn manifest(&self) -> Result<SnapshotManifest> {
        let mut blocks = Vec::with_capacity(self.block_changesets.len());
        let mut page_size = None;
        for bytes in &self.block_changesets {
            let header_bytes = bytes.get(..CHANGESET_HEADER_SIZE)
                .ok_or_else(|| corrupt("snapshot changeset has no header"))?;
            let header = ChangesetHeader::from_bytes(header_bytes.try_into().expect("header width"))?;
            if page_size.is_some_and(|size| size != header.page_size) {
                return Err(corrupt("snapshot blocks disagree on page size"));
            }
            page_size = Some(header.page_size);
            let id = compute_changeset_id(bytes);
            let pages = parse_decoded_snapshot_block(bytes, header.page_size, id)?;
            let first = pages.first().ok_or_else(|| corrupt("empty snapshot block"))?.page_number;
            for (offset, page) in pages.iter().enumerate() {
                if u64::from(page.page_number) != u64::from(first) + offset as u64 {
                    return Err(corrupt("snapshot block pages are not contiguous"));
                }
            }
            let k = u32::try_from(bytes.len().div_ceil(usize::from(self.config.symbol_size)))
                .map_err(|_| FrankenError::TooBig)?;
            blocks.push(SnapshotBlockManifest {
                changeset_id: id,
                first_page: first,
                page_count: header.n_pages,
                k_source: k,
                r_repair: symbol_schedule_end(k, self.config.max_isi_multiplier) - k,
                symbol_size: self.config.symbol_size,
                encoded_len: bytes.len() as u64,
                digest: blake3::derive_key(BLOCK_DOMAIN, bytes),
            });
        }
        let manifest = SnapshotManifest { page_size: page_size.unwrap_or(0), blocks };
        manifest.validate()?;
        Ok(manifest)
    }
}

/// Authenticated receiver with fixed block identities and retained-payload limits.
///
/// The limit covers saved symbol payloads and undrained decoded page payloads,
/// not metadata, decoder scratch or total RSS. The existing codec admission
/// limit separately bounds its conservative work estimate. Interleaving too
/// many blocks can return TooBig; drain output and retry the rejected packet,
/// or restart with a larger budget if no complete output can yet be drained.
/// Completion is transfer completion, not durable database publication.
pub struct ManifestSnapshotReceiver {
    manifest: SnapshotManifest,
    manifest_id: [u8; 32],
    auth_key: [u8; 32],
    index: HashMap<ChangesetId, usize>,
    decoders: Vec<BlockDecoder>,
    ready: Vec<DecodedBlock>,
    decoded: usize,
    retained: usize,
    max_payload_bytes: usize,
}

impl fmt::Debug for ManifestSnapshotReceiver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ManifestSnapshotReceiver")
            .field("manifest_id", &self.manifest_id)
            .field("decoded", &self.decoded)
            .field("retained_payload_bytes", &self.retained)
            .finish_non_exhaustive()
    }
}

impl ManifestSnapshotReceiver {
    pub fn new(
        manifest: SnapshotManifest, expected_id: [u8; 32], auth_key: [u8; 32],
        max_payload_bytes: usize,
    ) -> Result<Self> {
        manifest.validate()?;
        if manifest.id() != expected_id {
            return Err(corrupt("snapshot manifest does not match trusted identity"));
        }
        if max_payload_bytes == 0 { return Err(FrankenError::TooBig); }
        let mut index = HashMap::new();
        let mut decoders = Vec::with_capacity(manifest.blocks.len());
        for (slot, block) in manifest.blocks.iter().enumerate() {
            index.insert(block.changeset_id, slot);
            let mut decoder = BlockDecoder::new();
            decoder.initialize(block.changeset_id, block.k_source, u32::from(block.symbol_size));
            decoders.push(decoder);
        }
        Ok(Self {
            manifest, manifest_id: expected_id, auth_key, index, decoders,
            ready: Vec::new(), decoded: 0, retained: 0, max_payload_bytes,
        })
    }

    pub fn manifest(&self) -> &SnapshotManifest { &self.manifest }
    pub const fn manifest_id(&self) -> [u8; 32] { self.manifest_id }
    pub const fn blocks_decoded(&self) -> usize { self.decoded }
    pub fn is_complete(&self) -> bool { self.decoded == self.decoders.len() }
    pub const fn retained_payload_bytes(&self) -> usize { self.retained }

    pub fn take_decoded_blocks(&mut self) -> Vec<DecodedBlock> {
        let blocks = std::mem::take(&mut self.ready);
        for block in &blocks {
            self.retained -= block.pages.iter().map(|page| page.page_data.len()).sum::<usize>();
        }
        blocks
    }

    // Non-mutating admission is also used before persisting a spool record.
    fn admission(&self, packet: &ReplicationPacket) -> Result<Option<SnapshotPacketResult>> {
        let Some(&slot) = self.index.get(&packet.changeset_id) else {
            return Ok(Some(SnapshotPacketResult::Rejected));
        };
        let block = &self.manifest.blocks[slot];
        if packet.symbol_data.len() != usize::from(block.symbol_size)
            || packet.wire_version != ReplicationWireVersion::FramedV2
            || !packet.verify_integrity(Some(&self.auth_key))
        {
            return Ok(Some(SnapshotPacketResult::Rejected));
        }
        if packet.sbn != 0 || packet.k_source != block.k_source
            || packet.r_repair != block.r_repair || packet.symbol_size_t != block.symbol_size
            || packet.seed != derive_seed_from_changeset_id(&block.changeset_id)
            || packet.esi >= block.k_source + block.r_repair
        {
            return Err(corrupt("authenticated packet disagrees with snapshot manifest"));
        }
        let decoder = &self.decoders[slot];
        if decoder.decoded {
            return Ok(Some(SnapshotPacketResult::BlockAlreadyDecoded));
        }
        if let Some(previous) = decoder.symbols.get(&packet.esi) {
            if previous != &packet.symbol_data {
                return Err(corrupt("conflicting authenticated snapshot symbol"));
            }
            return Ok(Some(SnapshotPacketResult::Duplicate));
        }
        if self.retained.checked_add(packet.symbol_data.len())
            .is_none_or(|bytes| bytes > self.max_payload_bytes)
        {
            return Err(FrankenError::TooBig);
        }
        Ok(None)
    }

    pub fn process_packet(&mut self, cx: &Cx, packet: &ReplicationPacket) -> Result<SnapshotPacketResult> {
        cx.checkpoint().map_err(|_| FrankenError::Abort)?;
        if let Some(result) = self.admission(packet)? { return Ok(result); }
        let slot = self.index[&packet.changeset_id];
        let decoder = &mut self.decoders[slot];
        decoder.add_symbol(packet.esi, packet.symbol_data.clone());
        self.retained += packet.symbol_data.len();
        let decoded = (|| {
            if !decoder.ready_to_decode() { return Ok(None); }
            let Some(padded) = decoder.try_decode(cx)? else { return Ok(None); };
            let block = &self.manifest.blocks[slot];
            let length = usize::try_from(block.encoded_len).map_err(|_| FrankenError::TooBig)?;
            let bytes = padded.get(..length).ok_or_else(|| corrupt("short decoded snapshot block"))?;
            if blake3::derive_key(BLOCK_DOMAIN, bytes) != block.digest {
                return Err(corrupt("snapshot block full digest mismatch"));
            }
            let pages = parse_decoded_snapshot_block(bytes, self.manifest.page_size, block.changeset_id)?;
            if pages.len() != block.page_count as usize || pages.iter().enumerate().any(|(i, page)| {
                u64::from(page.page_number) != u64::from(block.first_page) + i as u64
            }) {
                return Err(corrupt("decoded snapshot page coverage mismatch"));
            }
            Ok(Some(pages))
        })();
        let pages = match decoded {
            Ok(Some(pages)) => pages,
            Ok(None) => return Ok(SnapshotPacketResult::Accepted),
            Err(error) => {
                decoder.symbols.remove(&packet.esi);
                decoder.received_isis.remove(&packet.esi);
                self.retained -= packet.symbol_data.len();
                return Err(error);
            }
        };
        self.retained -= decoder.symbols.values().map(Vec::len).sum::<usize>();
        self.retained += pages.iter().map(|page| page.page_data.len()).sum::<usize>();
        decoder.symbols.clear();
        decoder.received_isis.clear();
        decoder.decoded = true;
        self.decoded += 1;
        self.ready.push(DecodedBlock { block_index: slot as u32, pages });
        Ok(SnapshotPacketResult::BlockDecoded(slot as u32))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replication_sender::{PageEntry, SenderConfig};
    use crate::snapshot_shipping::{ResumeState, SnapshotReceiver, SnapshotReceiverState};

    const KEY: [u8; 32] = [0xAB; 32];

    fn sender(first: u32, count: u32, multiplier: u32) -> SnapshotSender {
        let mut pages: Vec<_> = (first..first + count)
            .map(|page| PageEntry::new(page, vec![page as u8; 256])).collect();
        SnapshotSender::prepare(256, &mut pages, SenderConfig {
            symbol_size: 256, max_isi_multiplier: multiplier,
        }).unwrap()
    }

    fn packets(sender: &mut SnapshotSender) -> Vec<ReplicationPacket> {
        let cx = Cx::new();
        let mut out = Vec::new();
        while let Some(mut packet) = sender.next_packet(&cx).unwrap() {
            packet.attach_auth_tag(&KEY);
            out.push(packet);
        }
        out
    }

    fn receiver(manifest: SnapshotManifest, limit: usize) -> ManifestSnapshotReceiver {
        let id = manifest.id();
        ManifestSnapshotReceiver::new(manifest, id, KEY, limit).unwrap()
    }

    fn two_blocks() -> (SnapshotManifest, Vec<ReplicationPacket>, Vec<ReplicationPacket>) {
        let mut a = sender(1, 1, 1);
        // Build the second encoded block with page 2; production prepare can
        // produce the same descriptors when a large input crosses its bound.
        let mut pages = [PageEntry::new(2, vec![2; 256])];
        let mut b = SnapshotSender::prepare(256, &mut pages, SenderConfig {
            symbol_size: 256, max_isi_multiplier: 1,
        }).unwrap();
        let mut combined = sender(1, 1, 1);
        combined.block_changesets.push(b.block_changesets[0].clone());
        let manifest = combined.manifest().unwrap();
        (manifest, packets(&mut a), packets(&mut b))
    }

    #[test]
    fn manifest_roundtrip_is_canonical_and_trusted_id_is_required() {
        let manifest = sender(1, 3, 1).manifest().unwrap();
        let bytes = manifest.to_bytes();
        assert_eq!(bytes.len(), HEADER_BYTES + BLOCK_BYTES);
        assert_eq!(SnapshotManifest::from_bytes(&bytes).unwrap(), manifest);
        assert!(ManifestSnapshotReceiver::new(manifest, [0; 32], KEY, 4096).is_err());
    }

    #[test]
    fn manifest_parser_rejects_truncation_trailing_data_and_huge_counts() {
        let bytes = sender(1, 1, 1).manifest().unwrap().to_bytes();
        for length in 0..bytes.len() {
            assert!(SnapshotManifest::from_bytes(&bytes[..length]).is_err());
        }
        let mut bad = bytes.clone();
        bad.push(0);
        assert!(SnapshotManifest::from_bytes(&bad).is_err());
        let mut bad = bytes;
        bad[12..16].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(SnapshotManifest::from_bytes(&bad).is_err());
    }

    #[test]
    fn manifest_rejects_gaps_overlaps_and_inconsistent_dimensions() {
        let (manifest, _, _) = two_blocks();
        for first in [1, 3, u32::MAX] {
            let mut bad = manifest.clone();
            bad.blocks[1].first_page = first;
            assert!(SnapshotManifest::from_bytes(&bad.to_bytes()).is_err());
        }
        let mut bad = manifest;
        bad.blocks[0].k_source += 1;
        assert!(SnapshotManifest::from_bytes(&bad.to_bytes()).is_err());
    }

    #[test]
    fn manifest_uses_encoded_bytes_not_public_sender_hints() {
        let mut source = sender(1, 1, 1);
        let expected = source.manifest().unwrap();
        source.page_size = 1;
        source.source_blocks.clear();
        assert_eq!(source.manifest().unwrap(), expected);
    }

    #[test]
    fn reverse_arrival_keeps_manifest_slots_and_draining_keeps_completion() {
        let (manifest, a, b) = two_blocks();
        let mut receiver = receiver(manifest, 4096);
        for (expected_slot, packets) in [(1, b), (0, a)] {
            for packet in packets { receiver.process_packet(&Cx::new(), &packet).unwrap(); }
            let blocks = receiver.take_decoded_blocks();
            assert_eq!(blocks.len(), 1);
            assert_eq!(blocks[0].block_index, expected_slot);
            assert_eq!(blocks[0].pages[0].page_number, expected_slot + 1);
        }
        assert_eq!(receiver.blocks_decoded(), 2);
        assert!(receiver.is_complete());
        assert_eq!(receiver.retained_payload_bytes(), 0);
    }

    #[test]
    fn foreign_snapshot_and_wrong_authentication_do_not_change_state() {
        let mut source = sender(1, 1, 1);
        let mut receiver = receiver(source.manifest().unwrap(), 4096);
        let mut packet = packets(&mut source).remove(0);
        packet.auth_tag = None;
        assert_eq!(receiver.process_packet(&Cx::new(), &packet).unwrap(), SnapshotPacketResult::Rejected);
        packet.attach_auth_tag(&[7; 32]);
        assert_eq!(receiver.process_packet(&Cx::new(), &packet).unwrap(), SnapshotPacketResult::Rejected);
        let mut foreign = sender(1, 2, 1);
        for packet in packets(&mut foreign) {
            assert_eq!(receiver.process_packet(&Cx::new(), &packet).unwrap(), SnapshotPacketResult::Rejected);
        }
        assert_eq!(receiver.retained_payload_bytes(), 0);
        assert_eq!(receiver.blocks_decoded(), 0);
    }

    #[test]
    fn conflicting_authenticated_duplicate_is_rejected_without_replacing_bytes() {
        let mut source = sender(1, 1, 1);
        let mut receiver = receiver(source.manifest().unwrap(), 4096);
        let packets = packets(&mut source);
        receiver.process_packet(&Cx::new(), &packets[0]).unwrap();
        let mut bad = packets[0].clone();
        bad.symbol_data[0] ^= 1;
        bad.payload_xxh3 = ReplicationPacket::compute_payload_xxh3(&bad.symbol_data);
        bad.attach_auth_tag(&KEY);
        assert!(receiver.process_packet(&Cx::new(), &bad).is_err());
        assert_eq!(receiver.retained_payload_bytes(), 256);
        for packet in packets { receiver.process_packet(&Cx::new(), &packet).unwrap(); }
        assert!(receiver.is_complete());
    }

    #[test]
    fn decoded_payload_remains_budgeted_until_consumer_drains_it() {
        let (manifest, a, b) = two_blocks();
        let mut receiver = receiver(manifest, 512);
        for packet in a { receiver.process_packet(&Cx::new(), &packet).unwrap(); }
        receiver.process_packet(&Cx::new(), &b[0]).unwrap();
        assert!(matches!(receiver.process_packet(&Cx::new(), &b[1]), Err(FrankenError::TooBig)));
        assert_eq!(receiver.retained_payload_bytes(), 512);
        assert_eq!(receiver.take_decoded_blocks().len(), 1);
        receiver.process_packet(&Cx::new(), &b[1]).unwrap();
        assert!(receiver.is_complete());
    }

    #[test]
    fn full_digest_mismatch_never_releases_decoded_pages() {
        let mut source = sender(1, 1, 1);
        let mut manifest = source.manifest().unwrap();
        manifest.blocks[0].digest[0] ^= 1;
        let mut receiver = receiver(manifest, 4096);
        let packets = packets(&mut source);
        receiver.process_packet(&Cx::new(), &packets[0]).unwrap();
        assert!(receiver.process_packet(&Cx::new(), &packets[1]).is_err());
        assert_eq!(receiver.blocks_decoded(), 0);
        assert!(receiver.take_decoded_blocks().is_empty());
    }

    #[test]
    #[cfg(not(target_arch = "wasm32"))]
    fn authenticated_manifest_receiver_repairs_permanently_missing_source_zero() {
        let mut source = sender(1, 3, 8);
        let mut receiver = receiver(source.manifest().unwrap(), 64 * 1024);
        let mut repairs = 0;
        for packet in packets(&mut source) {
            if packet.esi == 0 { continue; }
            if !packet.is_source_symbol() { repairs += 1; }
            receiver.process_packet(&Cx::new(), &packet).unwrap();
            if receiver.is_complete() { break; }
        }
        assert!(receiver.is_complete());
        assert!(repairs > 0);
        let blocks = receiver.take_decoded_blocks();
        assert_eq!(blocks[0].pages.len(), 3);
        for page in &blocks[0].pages { assert_eq!(page.page_data, vec![page.page_number as u8; 256]); }
    }

    #[test]
    fn metadata_only_resume_cannot_claim_payload_completion() {
        let mut resume = ResumeState::new(1);
        resume.blocks[0].decoded = true;
        let receiver = SnapshotReceiver::from_resume(resume, 256);
        assert_eq!(receiver.state(), SnapshotReceiverState::Waiting);
        assert_eq!(receiver.blocks_decoded(), 0);
        assert!(!receiver.resume_state().all_decoded());
    }

    #[test]
    fn debug_does_not_expose_transport_key() {
        let source = sender(1, 1, 1);
        let receiver = receiver(source.manifest().unwrap(), 4096);
        let debug = format!("{receiver:?}");
        assert!(!debug.contains("auth_key"));
        assert!(!debug.contains(&format!("{:?}", KEY)));
    }
}
