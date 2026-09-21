//! Bounded, read-only symbol streaming from an already frozen SQLite image.
//!
//! This scanner is NOT a snapshot-isolation mechanism. Supply a private,
//! immutable database image without a WAL overlay, not a live database file.
//! Preparation reads it once to bind every block and the complete image hash.
//! Streaming reloads one block at a time and verifies its original identity
//! before emitting any symbol. No file is created, modified, or removed here.

use std::fmt;

use fsqlite_error::{FrankenError, Result};
use fsqlite_types::cx::Cx;
use fsqlite_vfs::traits::VfsFile;

use super::{BLOCK_DOMAIN, MAX_BLOCKS, SnapshotBlockManifest, SnapshotManifest, corrupt};
use crate::replication_sender::{
    CHANGESET_HEADER_SIZE, CHANGESET_MAGIC, CHANGESET_VERSION, ChangesetHeader,
    RepairEncoder, ReplicationPacket, ReplicationPacketV2Header, SenderConfig,
    compute_changeset_id, derive_seed_from_changeset_id, max_pages_per_repair_block,
    symbol_schedule_end,
};

/// Explicit disk-image and encoded-block admission limits.
///
/// The block limit excludes codec scratch and the returned packet; the codec's existing work
/// admission applies separately. Neither limit is a total RSS guarantee.
#[derive(Debug, Clone, Copy)]
pub struct SnapshotSourceLimits {
    pub max_image_bytes: u64,
    pub max_block_bytes: usize,
}

impl SnapshotSourceLimits {
    pub(crate) fn validate(self) -> Result<()> {
        if self.max_image_bytes < 512 || self.max_block_bytes < CHANGESET_HEADER_SIZE + 512 + 12 {
            return Err(FrankenError::TooBig);
        }
        Ok(())
    }
}

/// Owns a frozen image descriptor, at most 256 manifest entries, one encoded
/// block, and the existing bounded repair encoder.
///
/// It never retains all pages.
/// Outgoing packets are unsigned, like SnapshotSender's packets; authenticate
/// them with ReplicationPacket::attach_auth_tag before remote transmission.
pub struct SnapshotFileSender<F: VfsFile> {
    file: F,
    manifest: SnapshotManifest,
    byte_len: u64,
    image_blake3: [u8; 32],
    block: usize,
    esi: u32,
    encoded: Vec<u8>,
    repair: RepairEncoder,
    failed: bool,
}

impl<F: VfsFile> fmt::Debug for SnapshotFileSender<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SnapshotFileSender")
            .field("byte_len", &self.byte_len)
            .field("block_count", &self.manifest.blocks.len())
            .field("block", &self.block)
            .field("esi", &self.esi)
            .field("encoded_payload_bytes", &self.encoded.len())
            .field("failed", &self.failed)
            .finish_non_exhaustive()
    }
}

fn checkpoint(cx: &Cx) -> Result<()> {
    cx.checkpoint().map_err(|_| FrankenError::Abort)
}

async fn read_exact<F: VfsFile>(file: &F, cx: &Cx, bytes: &mut [u8], offset: u64) -> Result<()> {
    checkpoint(cx)?;
    if file.read(cx, bytes, offset).await? != bytes.len() {
        return Err(corrupt("frozen snapshot image read was short"));
    }
    Ok(())
}

fn geometry(header: &[u8; 100], byte_len: u64) -> Result<(u32, u32)> {
    if &header[..16] != b"SQLite format 3\0" {
        return Err(corrupt("snapshot source is not a SQLite database image"));
    }
    let encoded = u16::from_be_bytes([header[16], header[17]]);
    let page_size = if encoded == 1 { 65_536 } else { u32::from(encoded) };
    if !(512..=65_536).contains(&page_size) || !page_size.is_power_of_two()
        || byte_len == 0 || !byte_len.is_multiple_of(u64::from(page_size))
        || !matches!(header[18], 1 | 2) || !matches!(header[19], 1 | 2)
        || header[21..24] != [64, 32, 32]
    {
        return Err(corrupt("snapshot source has invalid SQLite geometry"));
    }
    let page_count = u32::try_from(byte_len / u64::from(page_size)).map_err(|_| FrankenError::TooBig)?;
    if page_count == u32::MAX { return Err(FrankenError::TooBig); }
    let declared = u32::from_be_bytes(header[28..32].try_into().expect("header width"));
    if declared != 0 && header[24..28] == header[92..96] && declared != page_count {
        return Err(corrupt("snapshot source header count disagrees with its file size"));
    }
    Ok((page_size, page_count))
}

/// Encode directly into one bounded buffer. No Vec<PageEntry> or second copy
/// of the block's pages is needed. This is the canonical changeset encoding.
async fn read_block<F: VfsFile>(
    file: &F, cx: &Cx, page_size: u32, first_page: u32, page_count: u32,
) -> Result<Vec<u8>> {
    let stride = (page_size as usize).checked_add(12).ok_or(FrankenError::TooBig)?;
    let len = (page_count as usize).checked_mul(stride)
        .and_then(|n| n.checked_add(CHANGESET_HEADER_SIZE)).ok_or(FrankenError::TooBig)?;
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(len).map_err(|_| FrankenError::OutOfMemory)?;
    bytes.resize(len, 0);
    bytes[..CHANGESET_HEADER_SIZE].copy_from_slice(&ChangesetHeader {
        magic: CHANGESET_MAGIC, version: CHANGESET_VERSION,
        page_size, n_pages: page_count, total_len: len as u64,
    }.to_bytes());
    for (index, entry) in bytes[CHANGESET_HEADER_SIZE..].chunks_exact_mut(stride).enumerate() {
        let number = first_page.checked_add(index as u32).ok_or(FrankenError::TooBig)?;
        entry[..4].copy_from_slice(&number.to_le_bytes());
        read_exact(file, cx, &mut entry[12..], (u64::from(number) - 1) * u64::from(page_size)).await?;
        let checksum = xxhash_rust::xxh3::xxh3_64(&entry[12..]);
        entry[4..12].copy_from_slice(&checksum.to_le_bytes());
    }
    Ok(bytes)
}

impl<F: VfsFile> SnapshotFileSender<F> {
    /// Bind a private, immutable, standalone image. Merely passing a read-only
    /// handle does not freeze other writers; the caller must supply isolation.
    /// Limits and block count are checked before block-sized allocations.
    pub async fn open(
        cx: &Cx, file: F, config: SenderConfig, limits: SnapshotSourceLimits,
    ) -> Result<Self> {
        checkpoint(cx)?;
        config.validate()?;
        limits.validate()?;
        let byte_len = file.file_size(cx)?;
        if byte_len > limits.max_image_bytes { return Err(FrankenError::TooBig); }
        if byte_len < 100 { return Err(corrupt("snapshot source header is missing")); }
        let mut header = [0; 100];
        read_exact(&file, cx, &mut header, 0).await?;
        let (page_size, page_count) = geometry(&header, byte_len)?;
        let payload_pages = (limits.max_block_bytes - CHANGESET_HEADER_SIZE) / (page_size as usize + 12);
        let block_pages = payload_pages.min(max_pages_per_repair_block(page_size, config.symbol_size)?);
        if block_pages == 0 { return Err(FrankenError::TooBig); }
        let block_count = (page_count as usize).div_ceil(block_pages);
        if block_count > MAX_BLOCKS { return Err(FrankenError::TooBig); }
        let mut blocks = Vec::with_capacity(block_count);
        let mut image_hash = blake3::Hasher::new();
        let mut first = 1_u32;
        while first <= page_count {
            let count = ((page_count - first + 1) as usize).min(block_pages) as u32;
            let bytes = read_block(&file, cx, page_size, first, count).await?;
            if first == 1 && bytes[CHANGESET_HEADER_SIZE + 12..CHANGESET_HEADER_SIZE + 112] != header {
                return Err(corrupt("snapshot source header changed during preparation"));
            }
            for entry in bytes[CHANGESET_HEADER_SIZE..].chunks_exact(page_size as usize + 12) {
                image_hash.update(&entry[12..]);
            }
            let k = u32::try_from(bytes.len().div_ceil(usize::from(config.symbol_size)))
                .map_err(|_| FrankenError::TooBig)?;
            blocks.push(SnapshotBlockManifest {
                changeset_id: compute_changeset_id(&bytes), first_page: first, page_count: count,
                k_source: k, r_repair: symbol_schedule_end(k, config.max_isi_multiplier) - k,
                symbol_size: config.symbol_size, encoded_len: bytes.len() as u64,
                digest: blake3::derive_key(BLOCK_DOMAIN, &bytes),
            });
            first += count;
        }
        let mut final_header = [0; 100];
        read_exact(&file, cx, &mut final_header, 0).await?;
        if file.file_size(cx)? != byte_len || final_header != header {
            return Err(corrupt("snapshot source changed during preparation"));
        }
        let manifest = SnapshotManifest { page_size, blocks };
        manifest.validate()?;
        Ok(Self {
            file, manifest, byte_len, image_blake3: *image_hash.finalize().as_bytes(),
            block: 0, esi: 0, encoded: Vec::new(), repair: RepairEncoder::default(), failed: false,
        })
    }

    pub fn manifest(&self) -> &SnapshotManifest { &self.manifest }
    pub const fn byte_len(&self) -> u64 { self.byte_len }
    pub const fn image_blake3(&self) -> [u8; 32] { self.image_blake3 }
    pub fn encoded_payload_bytes(&self) -> usize { self.encoded.len() }
    pub fn is_exhausted(&self) -> bool { !self.failed && self.block == self.manifest.blocks.len() }
    pub fn into_file(self) -> F { self.file }

    /// Replay the same symbol schedule and manifest. Every reloaded block is
    /// checked again. A source-integrity/I/O failure requires a new sender;
    /// restart cannot silently adopt changed source bytes under the old ID.
    pub fn restart(&mut self) -> Result<()> {
        if self.failed { return Err(FrankenError::BusyRecovery); }
        self.block = 0;
        self.esi = 0;
        self.encoded = Vec::new();
        self.repair = RepairEncoder::default();
        Ok(())
    }

    /// Emit one verified source/repair symbol. Cancellation never consumes a
    /// symbol: all awaited reads use a local block buffer and ESI advances only
    /// after successful encoding. Dropping a read future leaves no partial block
    /// installed. Packet delivery/acknowledgment remains the caller's protocol.
    pub async fn next_packet(&mut self, cx: &Cx) -> Result<Option<ReplicationPacket>> {
        if self.failed { return Err(FrankenError::BusyRecovery); }
        let result = self.next_verified(cx).await;
        if matches!(&result, Err(error) if !matches!(error, FrankenError::Abort)) {
            self.failed = true;
            self.encoded = Vec::new();
            self.repair = RepairEncoder::default();
        }
        result
    }

    async fn next_verified(&mut self, cx: &Cx) -> Result<Option<ReplicationPacket>> {
        checkpoint(cx)?;
        if self.block == self.manifest.blocks.len() { return Ok(None); }
        if self.file.file_size(cx)? != self.byte_len {
            return Err(corrupt("frozen snapshot image size changed"));
        }
        let previous = &self.manifest.blocks[self.block];
        if self.esi == previous.k_source + previous.r_repair {
            self.block += 1;
            self.esi = 0;
            self.encoded = Vec::new();
            self.repair = RepairEncoder::default();
            if self.block == self.manifest.blocks.len() { return Ok(None); }
        }
        let block = &self.manifest.blocks[self.block];
        if self.encoded.is_empty() {
            let bytes = read_block(&self.file, cx, self.manifest.page_size, block.first_page, block.page_count).await?;
            if bytes.len() as u64 != block.encoded_len
                || blake3::derive_key(BLOCK_DOMAIN, &bytes) != block.digest
                || compute_changeset_id(&bytes) != block.changeset_id
                || self.file.file_size(cx)? != self.byte_len
            {
                return Err(corrupt("frozen snapshot block changed since manifest creation"));
            }
            self.encoded = bytes;
        }
        let t = usize::from(block.symbol_size);
        let data = if self.esi < block.k_source {
            let start = self.esi as usize * t;
            let end = (start + t).min(self.encoded.len());
            let mut symbol = vec![0; t];
            symbol[..end - start].copy_from_slice(&self.encoded[start..end]);
            symbol
        } else {
            self.repair.symbol(cx, &self.encoded, block.k_source, block.symbol_size, self.esi)?
        };
        let packet = ReplicationPacket::new_v2(ReplicationPacketV2Header {
            changeset_id: block.changeset_id, sbn: 0, esi: self.esi,
            k_source: block.k_source, r_repair: block.r_repair, symbol_size_t: block.symbol_size,
            seed: derive_seed_from_changeset_id(&block.changeset_id),
        }, data);
        self.esi += 1;
        Ok(Some(packet))
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use std::path::Path;
    use fsqlite_types::flags::VfsOpenFlags;
    use fsqlite_vfs::{MemoryVfs, memory::MemoryFile, traits::Vfs};
    use crate::replication_sender::PageEntry;
    use crate::snapshot_shipping::{ManifestSnapshotReceiver, SnapshotSender};

    fn run<F: std::future::Future<Output = ()>>(future: F) {
        asupersync::runtime::RuntimeBuilder::current_thread().blocking_threads(1, 1)
            .build().unwrap().block_on(future);
    }

    fn cx() -> Cx {
        let cx = Cx::new();
        cx.set_native_cx(asupersync::Cx::current().expect("runtime context"));
        cx
    }

    fn image(page_size: usize, count: u32) -> Vec<u8> {
        let mut bytes: Vec<_> = (0..page_size * count as usize).map(|n| (n % 251) as u8).collect();
        bytes[..100].fill(0);
        bytes[..16].copy_from_slice(b"SQLite format 3\0");
        let encoded = if page_size == 65_536 { 1 } else { page_size as u16 };
        bytes[16..18].copy_from_slice(&encoded.to_be_bytes());
        bytes[18] = 2; bytes[19] = 2;
        bytes[21..24].copy_from_slice(&[64, 32, 32]);
        bytes[28..32].copy_from_slice(&count.to_be_bytes());
        bytes
    }

    async fn fixture(cx: &Cx, bytes: &[u8]) -> (MemoryVfs, MemoryFile) {
        let vfs = MemoryVfs::new();
        let file = vfs.open(cx, Some(Path::new("frozen")), VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE).unwrap().0;
        file.write(cx, bytes, 0).await.unwrap();
        (vfs, file)
    }

    fn limits(block: usize) -> SnapshotSourceLimits {
        SnapshotSourceLimits { max_image_bytes: 16 * 1024 * 1024, max_block_bytes: block }
    }

    fn config(multiplier: u32) -> SenderConfig {
        SenderConfig { symbol_size: 512, max_isi_multiplier: multiplier }
    }

    #[test]
    fn file_sender_matches_existing_encoder_and_restarts_exactly() {
        run(async {
            let cx = cx();
            let bytes = image(512, 4);
            let (_vfs, file) = fixture(&cx, &bytes).await;
            let mut file_sender = SnapshotFileSender::open(&cx, file, config(4), limits(8192)).await.unwrap();
            let mut pages: Vec<_> = bytes.as_chunks::<512>().0.iter().enumerate()
                .map(|(i, page)| PageEntry::new(i as u32 + 1, page.to_vec())).collect();
            let mut original = SnapshotSender::prepare(512, &mut pages, config(4)).unwrap();
            assert_eq!(file_sender.manifest(), &original.manifest().unwrap());
            assert_eq!(file_sender.image_blake3(), *blake3::hash(&bytes).as_bytes());
            for _ in 0..2 {
                while let Some(expected) = original.next_packet(&cx).unwrap() {
                    assert_eq!(file_sender.next_packet(&cx).await.unwrap(), Some(expected));
                }
                assert!(file_sender.next_packet(&cx).await.unwrap().is_none());
                assert!(file_sender.is_exhausted());
                assert_eq!(file_sender.encoded_payload_bytes(), 0);
                file_sender.restart().unwrap();
                original.restart();
            }
        });
    }

    #[test]
    fn bounded_blocks_reconstruct_all_pages_without_whole_image_retention() {
        run(async {
            let cx = cx(); let bytes = image(512, 7);
            let (_vfs, file) = fixture(&cx, &bytes).await;
            let cap = CHANGESET_HEADER_SIZE + 2 * (512 + 12);
            let mut sender = SnapshotFileSender::open(&cx, file, config(1), limits(cap)).await.unwrap();
            let manifest = sender.manifest().clone();
            assert_eq!(manifest.blocks().len(), 4);
            assert_eq!(sender.encoded_payload_bytes(), 0);
            let mut receiver = ManifestSnapshotReceiver::new(manifest.clone(), manifest.id(), [9; 32], 8192).unwrap();
            let mut rebuilt = Vec::new();
            while let Some(mut packet) = sender.next_packet(&cx).await.unwrap() {
                assert!(sender.encoded_payload_bytes() <= cap);
                packet.attach_auth_tag(&[9; 32]);
                receiver.process_packet(&cx, &packet).unwrap();
                for block in receiver.take_decoded_blocks() {
                    for page in block.pages { rebuilt.extend_from_slice(&page.page_data); }
                }
            }
            assert!(receiver.is_complete());
            assert_eq!(rebuilt, bytes);
        });
    }

    #[test]
    fn source_mutation_is_terminal_and_cannot_rebind_the_manifest() {
        run(async {
            let cx = cx(); let bytes = image(512, 3);
            let (vfs, file) = fixture(&cx, &bytes).await;
            let mut sender = SnapshotFileSender::open(&cx, file, config(1), limits(4096)).await.unwrap();
            let manifest = sender.manifest().clone();
            let peer = vfs.open(&cx, Some(Path::new("frozen")), VfsOpenFlags::READWRITE).unwrap().0;
            peer.write(&cx, &[bytes[600] ^ 1], 600).await.unwrap();
            assert!(sender.next_packet(&cx).await.is_err());
            assert!(sender.restart().is_err());
            assert!(matches!(sender.next_packet(&cx).await, Err(FrankenError::BusyRecovery)));
            assert_eq!(sender.manifest(), &manifest);
        });
    }

    #[test]
    fn cancellation_does_not_consume_the_next_symbol() {
        run(async {
            let cx = cx(); let (_vfs, file) = fixture(&cx, &image(512, 1)).await;
            let mut sender = SnapshotFileSender::open(&cx, file, config(1), limits(2048)).await.unwrap();
            let cancelled = Cx::new(); cancelled.cancel();
            assert!(matches!(sender.next_packet(&cancelled).await, Err(FrankenError::Abort)));
            assert_eq!(sender.next_packet(&cx).await.unwrap().unwrap().esi, 0);
        });
    }

    #[test]
    fn limits_and_invalid_geometry_refuse_before_streaming() {
        run(async {
            let cx = cx();
            for (bytes, budget) in [
                (image(512, 2), SnapshotSourceLimits { max_image_bytes: 512, max_block_bytes: 4096 }),
                (image(512, 1), limits(545)),
                (image(512, 257), limits(546)),
                (vec![0; 1024], limits(4096)),
                ({ let mut b = image(512, 2); b.push(0); b }, limits(4096)),
                ({ let mut b = image(512, 2); b[28..32].copy_from_slice(&1_u32.to_be_bytes()); b }, limits(4096)),
            ] {
                let (_vfs, file) = fixture(&cx, &bytes).await;
                assert!(SnapshotFileSender::open(&cx, file, config(1), budget).await.is_err());
            }
        });
    }

    #[test]
    fn largest_sqlite_page_and_non_authoritative_legacy_count_are_supported() {
        run(async {
            let cx = cx();
            for size in [512, 65_536] {
                let mut bytes = image(size, 1);
                bytes[24] = 1;
                bytes[28..32].copy_from_slice(&17_u32.to_be_bytes());
                let (_vfs, file) = fixture(&cx, &bytes).await;
                let source = SnapshotFileSender::open(&cx, file, config(1), limits(128 * 1024)).await.unwrap();
                assert_eq!(source.manifest().page_size(), size as u32);
                assert_eq!(source.manifest().blocks()[0].page_count(), 1);
                assert_eq!(source.byte_len(), bytes.len() as u64);
            }
        });
    }
}

#[cfg(all(feature = "native", not(target_arch = "wasm32"), any(unix, windows)))]
pub use capture::CapturedSnapshotSender;

#[cfg(all(feature = "native", not(target_arch = "wasm32"), any(unix, windows)))]
mod capture {
    use std::path::{Path, PathBuf};

    use super::{SnapshotFileSender, SnapshotSourceLimits, checkpoint, corrupt};
    use crate::connection::{BackupReport, Connection};
    use crate::replication_sender::SenderConfig;
    use fsqlite_error::{FrankenError, Result};
    use fsqlite_types::{cx::Cx, flags::VfsOpenFlags};
    use fsqlite_vfs::{FileIdentity, host_fs, traits::{Vfs, VfsFile}};

    #[cfg(unix)]
    use fsqlite_vfs::{UnixFile as NativeFile, UnixVfs as NativeVfs};
    #[cfg(windows)]
    use fsqlite_vfs::{WindowsFile as NativeFile, WindowsVfs as NativeVfs};

    /// Native read-only transfer source returned by Connection capture.
    pub type CapturedSnapshotSender = SnapshotFileSender<NativeFile>;

    fn require_standalone(vfs: &NativeVfs, cx: &Cx, path: &Path) -> Result<()> {
        for suffix in ["-wal", "-shm", "-journal"] {
            let mut sidecar = path.as_os_str().to_os_string();
            sidecar.push(suffix);
            let sidecar = PathBuf::from(sidecar);
            if vfs.path_entry_exists(cx, &sidecar)? {
                return Err(FrankenError::CannotOpen { path: sidecar });
            }
        }
        Ok(())
    }

    impl Connection {
        /// Capture committed SQL state through the engine's verified backup
        /// path, then prepare a bounded file-backed transfer source.
        ///
        /// The destination must be absent inside a caller-controlled private
        /// namespace, kept exclusively owned and immutable for the sender's
        /// lifetime. Existing files and journal/WAL/SHM entries are refused.
        /// The live source may keep its WAL; backup_exact_to, NOT a raw main-file
        /// copy or forced checkpoint here, owns the coherent source snapshot.
        /// An active transaction on this connection is refused by that API.
        ///
        /// SnapshotSourceLimits govern admission/encoding of the resulting
        /// frozen image, not allocation or disk writes inside backup_exact_to.
        /// Backup retains its ConnectionEnv cancellation/resource policy; cx
        /// is checked before/after it and governs the subsequent VFS reads.
        /// A rejected/cancelled attempt may leave a backup file. It is preserved,
        /// never deleted or reused by this method. Directory durability and
        /// cleanup remain caller obligations, as for the backup API itself.
        /// Quiesce abandoned backup I/O before reusing its namespace; retries
        /// should use a fresh private destination rather than replacing a file.
        ///
        /// The original BackupReport is returned unchanged as provenance. The
        /// sender's raw image hash is a separate digest, not a reinterpretation
        /// of the backup API's logical hash. Obtain the manifest ID locally here
        /// and convey it to receivers over a trusted control plane.
        pub async fn capture_snapshot_transfer(
            &self,
            cx: &Cx,
            destination: &Path,
            config: SenderConfig,
            limits: SnapshotSourceLimits,
        ) -> Result<(BackupReport, CapturedSnapshotSender)> {
            checkpoint(cx)?;
            config.validate()?;
            limits.validate()?;
            let vfs = NativeVfs::new();
            // Resolve once so a process-wide cwd change across awaits cannot
            // redirect the backup, identity probe and stream to different files.
            let destination = vfs.full_pathname(cx, destination)?;
            if vfs.path_entry_exists(cx, &destination)? {
                return Err(FrankenError::CannotOpen { path: destination });
            }
            require_standalone(&vfs, cx, &destination)?;
            let report = self.backup_exact_to(&destination).await?;
            checkpoint(cx)?;
            if report.byte_len > limits.max_image_bytes { return Err(FrankenError::TooBig); }
            require_standalone(&vfs, cx, &destination)?;

            // Reject final symlinks/reparse points and verify the native VFS
            // opened the same file as this no-follow descriptor. This is not a
            // substitute for caller-owned namespace exclusion during backup.
            let probe = host_fs::open_existing_regular_file_no_follow(&destination)?;
            let expected = FileIdentity::from_file(&probe)?.ok_or(FrankenError::BusyRecovery)?;
            let (file, _) = vfs.open(cx, Some(&destination), VfsOpenFlags::READONLY)?;
            if file.file_identity()? != Some(expected) { return Err(FrankenError::BusyRecovery); }
            drop(probe); // The private backup has no database lock owners.
            let sender = SnapshotFileSender::open(cx, file, config, limits).await?;
            let pages: u64 = sender.manifest().blocks().iter()
                .map(|block| u64::from(block.page_count())).sum();
            if sender.byte_len() != report.byte_len || sender.manifest().page_size() != report.page_size
                || pages != u64::from(report.page_count)
            {
                return Err(corrupt("frozen transfer image disagrees with verified backup geometry"));
            }
            Ok((report, sender))
        }
    }
}
