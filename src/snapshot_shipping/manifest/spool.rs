//! Persistent packet journal for a manifest-bound snapshot transfer.
//!
//! This is a private SINGLE-OWNER file, not a database WAL. The caller owns
//! namespace exclusion, file-creation/directory durability, and quiescence of
//! old I/O before reopening (including after dropping an in-flight future).
//! A checkpoint attests to saved packet bytes, not applied database pages.
//! Replayed output must be applied idempotently; atomic database publication
//! and source-snapshot capture remain the caller's responsibility.

use std::fmt;

use fsqlite_error::{FrankenError, Result};
use fsqlite_types::{cx::Cx, flags::SyncFlags};
use fsqlite_vfs::traits::VfsFile;

use super::{ManifestSnapshotReceiver, corrupt};
use crate::replication_sender::{MAX_UDP_PAYLOAD, REPLICATION_HEADER_SIZE, ReplicationPacket};
use crate::snapshot_shipping::{DecodedBlock, SnapshotPacketResult};

const MAGIC: &[u8; 8] = b"FSSPOOL1";
const HEADER_BYTES: usize = 40;
const RECORD_HEADER: usize = 12;
const HASH_BYTES: usize = 32;
const CHAIN_DOMAIN: &str = "fsqlite:snapshot-spool-chain:v1";

/// Exact prefix established by a successful FULL sync.
///
/// Persist this receipt in trusted caller state. An attacker-supplied receipt
/// does not establish which prefix was acknowledged before a restart.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SnapshotCheckpoint {
    pub manifest_id: [u8; 32],
    pub end_offset: u64,
    pub record_count: u64,
    pub chain_hash: [u8; 32],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotSpoolState {
    /// Call replay_next until None; drain decoded output between calls.
    Replaying,
    /// All existing records are verified; appends and checkpoints are allowed.
    Ready,
    /// Incomplete, unacknowledged suffix retained; only a fresh-file fork can append.
    TornTail,
    /// Failed/abandoned I/O or corrupt content: no further writes or acknowledgments.
    Poisoned,
}

pub struct SnapshotSpool<F: VfsFile> {
    file: F,
    receiver: ManifestSnapshotReceiver,
    state: SnapshotSpoolState,
    end: u64,
    records: u64,
    chain: [u8; 32],
    observed_end: u64,
    max_file_bytes: u64,
    required: Option<SnapshotCheckpoint>,
}

impl<F: VfsFile> fmt::Debug for SnapshotSpool<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SnapshotSpool")
            .field("state", &self.state)
            .field("verified_end", &self.end)
            .field("records", &self.records)
            .field("receiver", &self.receiver)
            .finish_non_exhaustive()
    }
}

fn header(id: [u8; 32]) -> [u8; HEADER_BYTES] {
    let mut bytes = [0; HEADER_BYTES];
    bytes[..8].copy_from_slice(MAGIC);
    bytes[8..].copy_from_slice(&id);
    bytes
}

fn chain_hash(previous: &[u8; 32], bytes: &[u8]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new_derive_key(CHAIN_DOMAIN);
    hasher.update(previous);
    hasher.update(bytes);
    *hasher.finalize().as_bytes()
}

async fn read_exact<F: VfsFile>(file: &F, cx: &Cx, bytes: &mut [u8], offset: u64) -> Result<()> {
    if file.read(cx, bytes, offset).await? != bytes.len() {
        return Err(corrupt("snapshot spool short read"));
    }
    Ok(())
}

fn require_fresh(receiver: &ManifestSnapshotReceiver) -> Result<()> {
    if receiver.decoded != 0 || receiver.retained != 0 {
        return Err(corrupt("snapshot spool requires a fresh bound receiver"));
    }
    Ok(())
}

impl<F: VfsFile> SnapshotSpool<F> {
    /// Initialize an exclusively owned EMPTY file. Call checkpoint for durability.
    /// The file is never truncated or replaced, including on failure.
    pub async fn create(
        cx: &Cx, file: F, receiver: ManifestSnapshotReceiver, max_file_bytes: u64,
    ) -> Result<Self> {
        cx.checkpoint().map_err(|_| FrankenError::Abort)?;
        require_fresh(&receiver)?;
        if max_file_bytes < HEADER_BYTES as u64 { return Err(FrankenError::TooBig); }
        if file.file_size(cx)? != 0 { return Err(corrupt("snapshot spool destination is not empty")); }
        let bytes = header(receiver.manifest_id());
        file.write(cx, &bytes, 0).await?;
        Ok(Self {
            file, receiver, state: SnapshotSpoolState::Ready,
            end: HEADER_BYTES as u64, records: 0,
            chain: chain_hash(&[0; 32], &bytes), observed_end: HEADER_BYTES as u64,
            max_file_bytes, required: None,
        })
    }

    /// Open without mutating the source. The supplied receiver carries the
    /// independently trusted manifest and packet key. An optional prior receipt
    /// must be verified before replay can finish or any append can start.
    pub async fn open(
        cx: &Cx, file: F, receiver: ManifestSnapshotReceiver, max_file_bytes: u64,
        required: Option<SnapshotCheckpoint>,
    ) -> Result<Self> {
        cx.checkpoint().map_err(|_| FrankenError::Abort)?;
        require_fresh(&receiver)?;
        let observed_end = file.file_size(cx)?;
        if observed_end > max_file_bytes { return Err(FrankenError::TooBig); }
        if observed_end < HEADER_BYTES as u64 { return Err(corrupt("snapshot spool header was lost")); }
        let expected = header(receiver.manifest_id());
        let mut bytes = [0; HEADER_BYTES];
        read_exact(&file, cx, &mut bytes, 0).await?;
        if bytes != expected { return Err(corrupt("snapshot spool belongs to another manifest")); }
        if required.is_some_and(|receipt| {
            receipt.manifest_id != receiver.manifest_id()
                || receipt.end_offset < HEADER_BYTES as u64 || receipt.end_offset > observed_end
        }) {
            return Err(corrupt("required snapshot checkpoint is missing or mismatched"));
        }
        let mut spool = Self {
            file, receiver, state: SnapshotSpoolState::Replaying,
            end: HEADER_BYTES as u64, records: 0, chain: chain_hash(&[0; 32], &bytes),
            observed_end, max_file_bytes, required,
        };
        spool.verify_required_boundary()?;
        Ok(spool)
    }

    pub const fn state(&self) -> SnapshotSpoolState { self.state }
    pub fn receiver(&self) -> &ManifestSnapshotReceiver { &self.receiver }
    pub const fn record_count(&self) -> u64 { self.records }
    pub const fn verified_end_offset(&self) -> u64 { self.end }

    pub fn take_decoded_blocks(&mut self) -> Vec<DecodedBlock> {
        self.receiver.take_decoded_blocks()
    }

    /// Recover the owned handle. Quiesce any abandoned backend I/O before reuse.
    pub fn into_file(self) -> F { self.file }

    fn prefix(&self) -> SnapshotCheckpoint {
        SnapshotCheckpoint {
            manifest_id: self.receiver.manifest_id(), end_offset: self.end,
            record_count: self.records, chain_hash: self.chain,
        }
    }

    fn verify_required_boundary(&mut self) -> Result<()> {
        if let Some(required) = self.required {
            if self.end > required.end_offset { return Err(corrupt("checkpoint is not a record boundary")); }
            if self.end == required.end_offset {
                if self.prefix() != required { return Err(corrupt("snapshot checkpoint prefix mismatch")); }
                self.required = None;
            }
        }
        Ok(())
    }

    fn finish_replay(&mut self, torn: bool) -> Result<Option<SnapshotPacketResult>> {
        if self.required.is_some() { return Err(corrupt("required snapshot checkpoint was not recovered")); }
        self.state = if torn { SnapshotSpoolState::TornTail } else { SnapshotSpoolState::Ready };
        Ok(None)
    }

    /// Replay at most one bounded record. None means replay has terminated;
    /// inspect state to distinguish a clean tail from a preserved torn suffix.
    /// TooBig/Abort leave the record unconsumed so the caller can drain/retry.
    pub async fn replay_next(&mut self, cx: &Cx) -> Result<Option<SnapshotPacketResult>> {
        match self.state {
            SnapshotSpoolState::Ready | SnapshotSpoolState::TornTail => return Ok(None),
            SnapshotSpoolState::Poisoned => return Err(FrankenError::BusyRecovery),
            SnapshotSpoolState::Replaying => {}
        }
        let result = self.replay_one(cx).await;
        if matches!(&result, Err(error) if !matches!(error, FrankenError::TooBig | FrankenError::Abort)) {
            self.state = SnapshotSpoolState::Poisoned;
        }
        result
    }

    async fn replay_one(&mut self, cx: &Cx) -> Result<Option<SnapshotPacketResult>> {
        cx.checkpoint().map_err(|_| FrankenError::Abort)?;
        if self.file.file_size(cx)? != self.observed_end {
            return Err(corrupt("snapshot spool changed during replay"));
        }
        let remaining = self.observed_end - self.end;
        if remaining == 0 { return self.finish_replay(false); }
        if remaining < RECORD_HEADER as u64 { return self.finish_replay(true); }
        let mut prefix = [0; RECORD_HEADER];
        read_exact(&self.file, cx, &mut prefix, self.end).await?;
        let sequence = u64::from_le_bytes(prefix[..8].try_into().expect("record width"));
        let length = u32::from_le_bytes(prefix[8..].try_into().expect("record width")) as usize;
        if sequence != self.records.checked_add(1).ok_or(FrankenError::TooBig)?
            || length <= REPLICATION_HEADER_SIZE || length > MAX_UDP_PAYLOAD
        {
            return Err(corrupt("invalid snapshot spool record sequence or length"));
        }
        let record_len = RECORD_HEADER + length + HASH_BYTES;
        if remaining < record_len as u64 { return self.finish_replay(true); }
        let next_end = self.end.checked_add(record_len as u64).ok_or(FrankenError::TooBig)?;
        if self.required.is_some_and(|receipt| receipt.end_offset < next_end) {
            return Err(corrupt("required checkpoint falls inside a spool record"));
        }
        let mut record = vec![0; record_len];
        record[..RECORD_HEADER].copy_from_slice(&prefix);
        read_exact(&self.file, cx, &mut record[RECORD_HEADER..], self.end + RECORD_HEADER as u64).await?;
        let data_end = RECORD_HEADER + length;
        let next_hash = chain_hash(&self.chain, &record[..data_end]);
        if record[data_end..] != next_hash { return Err(corrupt("snapshot spool hash chain mismatch")); }
        let packet = ReplicationPacket::from_bytes(&record[RECORD_HEADER..data_end])?;
        // Only newly admitted records are written, so duplicates in a spool
        // indicate an inconsistent journal even though network duplicates are OK.
        if self.receiver.admission(&packet)?.is_some() {
            return Err(corrupt("snapshot spool contains an unadmitted packet"));
        }
        let result = self.receiver.process_packet(cx, &packet)?;
        self.end = next_end;
        self.chain = next_hash;
        self.records = sequence;
        self.verify_required_boundary()?;
        Ok(Some(result))
    }

    /// Save a newly admitted authenticated packet, then update decoder state.
    /// Success here is NOT a durable acknowledgment; call checkpoint for that.
    pub async fn append(&mut self, cx: &Cx, packet: &ReplicationPacket) -> Result<SnapshotPacketResult> {
        cx.checkpoint().map_err(|_| FrankenError::Abort)?;
        if self.state != SnapshotSpoolState::Ready { return Err(FrankenError::BusyRecovery); }
        if let Some(result) = self.receiver.admission(packet)? { return Ok(result); }
        let wire = packet.to_bytes()?;
        if wire.len() > MAX_UDP_PAYLOAD { return Err(FrankenError::TooBig); }
        let sequence = self.records.checked_add(1).ok_or(FrankenError::TooBig)?;
        let mut record = Vec::with_capacity(RECORD_HEADER + wire.len() + HASH_BYTES);
        record.extend_from_slice(&sequence.to_le_bytes());
        record.extend_from_slice(&(wire.len() as u32).to_le_bytes());
        record.extend_from_slice(&wire);
        let next_hash = chain_hash(&self.chain, &record);
        record.extend_from_slice(&next_hash);
        let next_end = self.end.checked_add(record.len() as u64).ok_or(FrankenError::TooBig)?;
        if next_end > self.max_file_bytes { return Err(FrankenError::TooBig); }
        if self.file.file_size(cx)? != self.end {
            self.state = SnapshotSpoolState::Poisoned;
            return Err(corrupt("snapshot spool was changed by another owner"));
        }
        // Arm before suspension. Dropping the future cannot permit an append
        // or checkpoint over an in-doubt write. Recovery needs a new owner
        // only after the caller has established old-I/O quiescence.
        self.state = SnapshotSpoolState::Poisoned;
        self.file.write(cx, &record, self.end).await?;
        let result = self.receiver.process_packet(cx, packet)?;
        self.end = next_end;
        self.observed_end = next_end;
        self.records = sequence;
        self.chain = next_hash;
        self.state = SnapshotSpoolState::Ready;
        Ok(result)
    }

    /// Sync the exact verified prefix before returning its receipt. This does
    /// not sync the parent directory or acknowledge application of any page.
    pub fn checkpoint(&mut self, cx: &Cx) -> Result<SnapshotCheckpoint> {
        cx.checkpoint().map_err(|_| FrankenError::Abort)?;
        if self.state != SnapshotSpoolState::Ready { return Err(FrankenError::BusyRecovery); }
        self.state = SnapshotSpoolState::Poisoned;
        if self.file.file_size(cx)? != self.end { return Err(corrupt("snapshot spool size changed before sync")); }
        self.file.sync(cx, SyncFlags::FULL)?;
        self.state = SnapshotSpoolState::Ready;
        Ok(self.prefix())
    }

    /// Preserve the old torn file and copy its verified prefix into a new,
    /// exclusively owned EMPTY file. The returned owner must finish replay
    /// against the exact copied-prefix receipt before it can append.
    pub async fn fork_verified_prefix<G: VfsFile>(&self, cx: &Cx, destination: G) -> Result<SnapshotSpool<G>> {
        cx.checkpoint().map_err(|_| FrankenError::Abort)?;
        if self.state != SnapshotSpoolState::TornTail { return Err(FrankenError::BusyRecovery); }
        if destination.file_size(cx)? != 0 { return Err(corrupt("snapshot fork destination is not empty")); }
        if self.file.file_size(cx)? != self.observed_end { return Err(corrupt("snapshot fork source changed")); }
        let mut destination = destination;
        let mut offset = 0_u64;
        let mut buffer = vec![0; 64 * 1024];
        while offset < self.end {
            let count = usize::try_from((self.end - offset).min(buffer.len() as u64)).expect("bounded copy chunk");
            read_exact(&self.file, cx, &mut buffer[..count], offset).await?;
            destination.write(cx, &buffer[..count], offset).await?;
            offset += count as u64;
        }
        destination.sync(cx, SyncFlags::FULL)?;
        let receiver = ManifestSnapshotReceiver::new(
            self.receiver.manifest.clone(), self.receiver.manifest_id,
            self.receiver.auth_key, self.receiver.max_payload_bytes,
        )?;
        SnapshotSpool::open(cx, destination, receiver, self.max_file_bytes, Some(self.prefix())).await
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use fsqlite_types::flags::VfsOpenFlags;
    use fsqlite_vfs::{MemoryVfs, MemoryVfsConfig, traits::Vfs};
    use std::path::Path;
    use crate::replication_sender::{PageEntry, SenderConfig};
    use crate::snapshot_shipping::{SnapshotManifest, SnapshotSender};

    const KEY: [u8; 32] = [0x5A; 32];
    const CAP: u64 = 1024 * 1024;

    fn run<F: std::future::Future<Output = ()>>(future: F) {
        asupersync::runtime::RuntimeBuilder::current_thread()
            .blocking_threads(1, 1).build().unwrap().block_on(future);
    }

    fn cx() -> Cx {
        let cx = Cx::new();
        cx.set_native_cx(asupersync::Cx::current().expect("test runtime context"));
        cx
    }

    fn fixture() -> (SnapshotManifest, Vec<ReplicationPacket>) {
        let mut pages = [PageEntry::new(1, vec![0x31; 256]), PageEntry::new(2, vec![0x32; 256])];
        let mut sender = SnapshotSender::prepare(256, &mut pages, SenderConfig {
            symbol_size: 256, max_isi_multiplier: 1,
        }).unwrap();
        let manifest = sender.manifest().unwrap();
        let mut packets = Vec::new();
        while let Some(mut packet) = sender.next_packet(&Cx::new()).unwrap() {
            packet.attach_auth_tag(&KEY);
            packets.push(packet);
        }
        (manifest, packets)
    }

    fn receiver(manifest: &SnapshotManifest) -> ManifestSnapshotReceiver {
        ManifestSnapshotReceiver::new(manifest.clone(), manifest.id(), KEY, 4096).unwrap()
    }

    fn file<V: Vfs>(vfs: &V, cx: &Cx, name: &Path) -> V::File {
        vfs.open(cx, Some(name), VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE).unwrap().0
    }

    async fn replay<F: VfsFile>(spool: &mut SnapshotSpool<F>, cx: &Cx) {
        while spool.replay_next(cx).await.unwrap().is_some() {}
    }

    #[test]
    fn partial_symbols_survive_reopen_without_retransmission() {
        run(async {
            let cx = cx(); let vfs = MemoryVfs::new(); let (manifest, packets) = fixture();
            let path = Path::new("partial");
            let mut spool = SnapshotSpool::create(&cx, file(&vfs, &cx, path), receiver(&manifest), CAP).await.unwrap();
            spool.append(&cx, &packets[0]).await.unwrap();
            let receipt = spool.checkpoint(&cx).unwrap();
            drop(spool.into_file());
            let mut spool = SnapshotSpool::open(&cx, file(&vfs, &cx, path), receiver(&manifest), CAP, Some(receipt)).await.unwrap();
            replay(&mut spool, &cx).await;
            assert_eq!(spool.receiver().retained_payload_bytes(), 256);
            for packet in &packets[1..] { spool.append(&cx, packet).await.unwrap(); }
            assert!(spool.receiver().is_complete());
            assert_eq!(spool.take_decoded_blocks()[0].pages.len(), 2);
        });
    }

    #[test]
    fn drained_pages_are_reconstructed_by_replay_and_counts_stay_complete() {
        run(async {
            let cx = cx(); let vfs = MemoryVfs::new(); let (manifest, packets) = fixture();
            let path = Path::new("drained");
            let mut spool = SnapshotSpool::create(&cx, file(&vfs, &cx, path), receiver(&manifest), CAP).await.unwrap();
            for packet in &packets { spool.append(&cx, packet).await.unwrap(); }
            let expected = spool.take_decoded_blocks().remove(0).pages;
            assert!(spool.receiver().is_complete());
            let receipt = spool.checkpoint(&cx).unwrap();
            drop(spool.into_file());
            let mut spool = SnapshotSpool::open(&cx, file(&vfs, &cx, path), receiver(&manifest), CAP, Some(receipt)).await.unwrap();
            replay(&mut spool, &cx).await;
            assert_eq!(spool.take_decoded_blocks().remove(0).pages, expected);
            assert_eq!(spool.receiver().blocks_decoded(), 1);
        });
    }

    #[test]
    fn required_checkpoint_detects_lost_bytes_and_wrong_prefix_hash() {
        run(async {
            let cx = cx(); let vfs = MemoryVfs::new(); let (manifest, packets) = fixture();
            let path = Path::new("required");
            let mut spool = SnapshotSpool::create(&cx, file(&vfs, &cx, path), receiver(&manifest), CAP).await.unwrap();
            spool.append(&cx, &packets[0]).await.unwrap();
            let receipt = spool.checkpoint(&cx).unwrap();
            let mut owned = spool.into_file();
            let mut wrong = receipt; wrong.chain_hash[0] ^= 1;
            let mut reopened = SnapshotSpool::open(&cx, file(&vfs, &cx, path), receiver(&manifest), CAP, Some(wrong)).await.unwrap();
            assert!(reopened.replay_next(&cx).await.is_err());
            assert_eq!(reopened.state(), SnapshotSpoolState::Poisoned);
            drop(reopened.into_file());
            // Fault injection only: truncate this sacrificial memory file.
            owned.truncate(&cx, receipt.end_offset - 1).unwrap();
            drop(owned);
            assert!(SnapshotSpool::open(&cx, file(&vfs, &cx, path), receiver(&manifest), CAP, Some(receipt)).await.is_err());
        });
    }

    #[test]
    fn torn_unacknowledged_tail_is_preserved_and_forked_without_overwrite() {
        run(async {
            let cx = cx(); let vfs = MemoryVfs::new(); let (manifest, packets) = fixture();
            let path = Path::new("torn");
            let mut spool = SnapshotSpool::create(&cx, file(&vfs, &cx, path), receiver(&manifest), CAP).await.unwrap();
            spool.append(&cx, &packets[0]).await.unwrap();
            let receipt = spool.checkpoint(&cx).unwrap();
            let owned = spool.into_file();
            owned.write(&cx, &[1, 2, 3], receipt.end_offset).await.unwrap();
            let mut before = vec![0; owned.file_size(&cx).unwrap() as usize];
            read_exact(&owned, &cx, &mut before, 0).await.unwrap();
            drop(owned);
            let mut spool = SnapshotSpool::open(&cx, file(&vfs, &cx, path), receiver(&manifest), CAP, Some(receipt)).await.unwrap();
            replay(&mut spool, &cx).await;
            assert_eq!(spool.state(), SnapshotSpoolState::TornTail);
            assert!(spool.append(&cx, &packets[1]).await.is_err());
            let mut fork = spool.fork_verified_prefix(&cx, file(&vfs, &cx, Path::new("fork"))).await.unwrap();
            replay(&mut fork, &cx).await;
            assert_eq!(fork.checkpoint(&cx).unwrap(), receipt);
            for packet in &packets[1..] { fork.append(&cx, packet).await.unwrap(); }
            assert!(fork.receiver().is_complete());
            let mut after = vec![0; before.len()];
            read_exact(&spool.file, &cx, &mut after, 0).await.unwrap();
            assert_eq!(before, after);
        });
    }

    #[test]
    fn complete_record_corruption_is_not_mistaken_for_a_torn_suffix() {
        run(async {
            let cx = cx(); let vfs = MemoryVfs::new(); let (manifest, packets) = fixture();
            let path = Path::new("corrupt");
            let mut spool = SnapshotSpool::create(&cx, file(&vfs, &cx, path), receiver(&manifest), CAP).await.unwrap();
            spool.append(&cx, &packets[0]).await.unwrap();
            let owned = spool.into_file();
            let offset = (HEADER_BYTES + RECORD_HEADER + REPLICATION_HEADER_SIZE) as u64;
            let mut byte = [0]; read_exact(&owned, &cx, &mut byte, offset).await.unwrap();
            byte[0] ^= 1; owned.write(&cx, &byte, offset).await.unwrap();
            drop(owned);
            let mut spool = SnapshotSpool::open(&cx, file(&vfs, &cx, path), receiver(&manifest), CAP, None).await.unwrap();
            assert!(spool.replay_next(&cx).await.is_err());
            assert_eq!(spool.state(), SnapshotSpoolState::Poisoned);
            assert!(spool.checkpoint(&cx).is_err());
        });
    }

    #[test]
    fn unauthenticated_and_duplicate_packets_do_not_extend_spool() {
        run(async {
            let cx = cx(); let vfs = MemoryVfs::new(); let (manifest, packets) = fixture();
            let mut spool = SnapshotSpool::create(&cx, file(&vfs, &cx, Path::new("admit")), receiver(&manifest), CAP).await.unwrap();
            let mut bad = packets[0].clone(); bad.auth_tag = None;
            assert_eq!(spool.append(&cx, &bad).await.unwrap(), SnapshotPacketResult::Rejected);
            assert_eq!(spool.record_count(), 0);
            spool.append(&cx, &packets[0]).await.unwrap();
            let receipt = spool.checkpoint(&cx).unwrap();
            assert_eq!(spool.append(&cx, &packets[0]).await.unwrap(), SnapshotPacketResult::Duplicate);
            assert_eq!(spool.checkpoint(&cx).unwrap(), receipt);
        });
    }

    #[test]
    fn disk_limit_refuses_cleanly_but_real_write_failure_fences_the_owner() {
        run(async {
            let cx = cx(); let (manifest, packets) = fixture();
            let vfs = MemoryVfs::new();
            let mut spool = SnapshotSpool::create(&cx, file(&vfs, &cx, Path::new("disk-limit")), receiver(&manifest), HEADER_BYTES as u64).await.unwrap();
            assert!(matches!(spool.append(&cx, &packets[0]).await, Err(FrankenError::TooBig)));
            assert_eq!(spool.state(), SnapshotSpoolState::Ready);
            assert_eq!(spool.checkpoint(&cx).unwrap().record_count, 0);
            let limited = MemoryVfs::new_with_config(MemoryVfsConfig {
                initial_reserve_bytes: 0, growth_chunk_bytes: 64, max_bytes: Some(256),
            });
            let mut spool = SnapshotSpool::create(&cx, file(&limited, &cx, Path::new("oom")), receiver(&manifest), CAP).await.unwrap();
            assert!(matches!(spool.append(&cx, &packets[0]).await, Err(FrankenError::OutOfMemory)));
            assert_eq!(spool.state(), SnapshotSpoolState::Poisoned);
            assert!(spool.checkpoint(&cx).is_err());
            assert!(spool.append(&cx, &packets[0]).await.is_err());
        });
    }

    #[test]
    fn receipt_must_end_at_a_record_boundary_and_debug_redacts_key() {
        run(async {
            let cx = cx(); let vfs = MemoryVfs::new(); let (manifest, packets) = fixture();
            let path = Path::new("boundary");
            let mut spool = SnapshotSpool::create(&cx, file(&vfs, &cx, path), receiver(&manifest), CAP).await.unwrap();
            spool.append(&cx, &packets[0]).await.unwrap();
            let mut receipt = spool.checkpoint(&cx).unwrap();
            assert!(!format!("{spool:?}").contains(&format!("{KEY:?}")));
            drop(spool.into_file());
            receipt.end_offset -= 1;
            let mut spool = SnapshotSpool::open(&cx, file(&vfs, &cx, path), receiver(&manifest), CAP, Some(receipt)).await.unwrap();
            assert!(spool.replay_next(&cx).await.is_err());
        });
    }

    #[test]
    #[cfg(all(feature = "native", unix))]
    fn native_file_full_close_reopen_reconstructs_saved_payloads() {
        run(async {
            let cx = cx(); let vfs = fsqlite_vfs::UnixVfs::new();
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("snapshot.spool");
            let (manifest, packets) = fixture();
            let mut spool = SnapshotSpool::create(&cx, file(&vfs, &cx, &path), receiver(&manifest), CAP).await.unwrap();
            for packet in &packets { spool.append(&cx, packet).await.unwrap(); }
            let receipt = spool.checkpoint(&cx).unwrap();
            drop(spool.into_file());
            let mut spool = SnapshotSpool::open(&cx, file(&vfs, &cx, &path), receiver(&manifest), CAP, Some(receipt)).await.unwrap();
            replay(&mut spool, &cx).await;
            assert!(spool.receiver().is_complete());
            let blocks = spool.take_decoded_blocks();
            assert_eq!(blocks[0].pages[0].page_data, vec![0x31; 256]);
            assert_eq!(blocks[0].pages[1].page_data, vec![0x32; 256]);
        });
    }
}
