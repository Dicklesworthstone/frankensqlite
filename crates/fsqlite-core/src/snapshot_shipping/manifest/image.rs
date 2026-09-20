//! Assemble a manifest-bound snapshot into a fresh SQLite database image.
//!
//! The destination must be an exclusively owned, empty staging file in a new
//! namespace with no journal/WAL sidecars. Never pass an open database's file.
//! The caller owns namespace exclusion, directory durability, and atomic
//! publication. This module never renames, truncates, deletes, or replaces a
//! file. Quiesce abandoned backend I/O before recovering its handle.
//!
//! DecodedBlock is publicly mutable, so each block is authenticated again
//! against the trusted manifest before any write. Page one is withheld until
//! every other page has passed readback verification and the first FULL sync.
//! A second FULL sync follows page-one installation. Only then is a receipt
//! returned. Durability is the supplied VFS's contract (MemoryVfs is volatile),
//! and byte equality is not a substitute for a B-tree integrity check.

use std::fmt;

use fsqlite_error::{FrankenError, Result};
use fsqlite_types::{cx::Cx, flags::SyncFlags};
use fsqlite_vfs::traits::VfsFile;

use super::{BLOCK_DOMAIN, SnapshotBlockManifest, SnapshotManifest, corrupt};
use crate::replication_sender::{
    CHANGESET_DOMAIN, CHANGESET_MAGIC, CHANGESET_VERSION, ChangesetHeader,
};
use crate::snapshot_shipping::DecodedBlock;

/// Completion of exact image readback and both VFS FULL-sync boundaries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SnapshotImageReceipt {
    pub manifest_id: [u8; 32],
    pub page_size: u32,
    pub page_count: u32,
    pub byte_len: u64,
    /// Unkeyed BLAKE3 of the exact database bytes in page-number order.
    pub image_blake3: [u8; 32],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotImageState {
    /// Validated blocks can be applied in any order.
    Writing,
    /// Readback verification and both syncs have completed.
    Verified,
    /// A write, verification or sync was failed/abandoned; no further writes.
    Poisoned,
}

/// Bounded-state writer: one page-one copy, one readback page, and at most
/// 256 block flags. Input DecodedBlocks remain owned by the caller.
pub struct SnapshotImageWriter<F: VfsFile> {
    file: F,
    manifest: SnapshotManifest,
    manifest_id: [u8; 32],
    page_count: u32,
    byte_len: u64,
    applied: Vec<bool>,
    completed: usize,
    page_one: Option<Vec<u8>>,
    high_water: u64,
    state: SnapshotImageState,
    receipt: Option<SnapshotImageReceipt>,
}

impl<F: VfsFile> fmt::Debug for SnapshotImageWriter<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SnapshotImageWriter")
            .field("manifest_id", &self.manifest_id)
            .field("state", &self.state)
            .field("blocks_applied", &self.completed)
            .field("byte_len", &self.byte_len)
            .finish_non_exhaustive()
    }
}

/// Hash the canonical changeset incrementally, without allocating a second
/// block-sized buffer. This is the same encoding used by SnapshotSender.
struct BlockHashes {
    content: blake3::Hasher,
    identity: blake3::Hasher,
}

impl BlockHashes {
    fn new(manifest: &SnapshotManifest, block: &SnapshotBlockManifest) -> Self {
        let header = ChangesetHeader {
            magic: CHANGESET_MAGIC,
            version: CHANGESET_VERSION,
            page_size: manifest.page_size,
            n_pages: block.page_count,
            total_len: block.encoded_len,
        }
        .to_bytes();
        let mut content = blake3::Hasher::new_derive_key(BLOCK_DOMAIN);
        content.update(&header);
        let mut identity = blake3::Hasher::new();
        identity.update(CHANGESET_DOMAIN.as_bytes());
        identity.update(&header);
        Self { content, identity }
    }

    fn page(&mut self, number: u32, bytes: &[u8]) {
        let number = number.to_le_bytes();
        let checksum = xxhash_rust::xxh3::xxh3_64(bytes).to_le_bytes();
        for hasher in [&mut self.content, &mut self.identity] {
            hasher.update(&number);
            hasher.update(&checksum);
            hasher.update(bytes);
        }
    }

    fn verify(&self, block: &SnapshotBlockManifest) -> Result<()> {
        if self.content.finalize().as_bytes() != &block.digest
            || &self.identity.finalize().as_bytes()[..16] != block.changeset_id.as_bytes()
        {
            return Err(corrupt("snapshot image block does not match its manifest"));
        }
        Ok(())
    }
}

fn checkpoint(cx: &Cx) -> Result<()> {
    cx.checkpoint().map_err(|_| FrankenError::Abort)
}

// SQLite file format section 1.3: page-size 1 encodes 65536; the in-header
// page count is authoritative only when nonzero and change_counter matches
// version_valid_for. Preserve legacy headers rather than rewriting the image.
fn validate_header(bytes: &[u8], page_size: u32, page_count: u32) -> Result<()> {
    if bytes.len() != page_size as usize || bytes.len() < 100
        || &bytes[..16] != b"SQLite format 3\0"
    {
        return Err(corrupt("snapshot image has no SQLite database header"));
    }
    let encoded = u16::from_be_bytes([bytes[16], bytes[17]]);
    let header_page_size = if encoded == 1 { 65_536 } else { u32::from(encoded) };
    if header_page_size != page_size
        || !matches!(bytes[18], 1 | 2) || !matches!(bytes[19], 1 | 2)
        || bytes[21..24] != [64, 32, 32]
    {
        return Err(corrupt("snapshot image header geometry or format mismatch"));
    }
    let declared = u32::from_be_bytes(bytes[28..32].try_into().expect("header width"));
    if declared != 0 && bytes[24..28] == bytes[92..96] && declared != page_count {
        return Err(corrupt("snapshot image header page count disagrees with manifest"));
    }
    Ok(())
}

async fn read_page<F: VfsFile>(file: &F, cx: &Cx, bytes: &mut [u8], offset: u64) -> Result<()> {
    if file.read(cx, bytes, offset).await? != bytes.len() {
        return Err(corrupt("snapshot image readback was short"));
    }
    Ok(())
}

impl<F: VfsFile> SnapshotImageWriter<F> {
    /// Admit a fresh staging file without mutating it. Obtain expected_id from
    /// trusted control-plane state, not from an untrusted manifest's sender.
    pub fn create(
        cx: &Cx,
        file: F,
        manifest: SnapshotManifest,
        expected_id: [u8; 32],
        max_file_bytes: u64,
    ) -> Result<Self> {
        checkpoint(cx)?;
        manifest.validate()?;
        if manifest.id() != expected_id {
            return Err(corrupt("snapshot image manifest identity mismatch"));
        }
        if !(512..=65_536).contains(&manifest.page_size) || !manifest.page_size.is_power_of_two() {
            return Err(corrupt("snapshot image requires a SQLite page size"));
        }
        let count: u64 = manifest.blocks.iter().map(|block| u64::from(block.page_count)).sum();
        let page_count = u32::try_from(count).map_err(|_| FrankenError::TooBig)?;
        let byte_len = count.checked_mul(u64::from(manifest.page_size)).ok_or(FrankenError::TooBig)?;
        if byte_len > max_file_bytes || page_count == u32::MAX {
            return Err(FrankenError::TooBig);
        }
        if file.file_size(cx)? != 0 {
            return Err(corrupt("snapshot image destination is not empty"));
        }
        let applied = vec![false; manifest.blocks.len()];
        Ok(Self {
            file, manifest, manifest_id: expected_id, page_count, byte_len, applied,
            completed: 0, page_one: None, high_water: 0,
            state: SnapshotImageState::Writing, receipt: None,
        })
    }

    pub const fn state(&self) -> SnapshotImageState { self.state }
    pub const fn manifest_id(&self) -> [u8; 32] { self.manifest_id }
    pub const fn blocks_applied(&self) -> usize { self.completed }

    /// Recover the owned handle, including after a failure. This does not
    /// certify completion; wait for any abandoned backend I/O before reuse.
    pub fn into_file(self) -> F { self.file }

    /// Validate a whole block BEFORE its first write. Return true for a newly
    /// applied block, false for an identical validated duplicate. A rejected
    /// input changes neither the file nor progress. I/O failure/drop poisons
    /// the writer, and cannot release a completion receipt.
    pub async fn apply_block(&mut self, cx: &Cx, decoded: &DecodedBlock) -> Result<bool> {
        if self.state != SnapshotImageState::Writing { return Err(FrankenError::BusyRecovery); }
        checkpoint(cx)?;
        let slot = decoded.block_index as usize;
        let block = self.manifest.blocks.get(slot)
            .ok_or_else(|| corrupt("snapshot image block index out of range"))?;
        if decoded.pages.len() != block.page_count as usize {
            return Err(corrupt("snapshot image block page count mismatch"));
        }
        let mut hashes = BlockHashes::new(&self.manifest, block);
        for (index, page) in decoded.pages.iter().enumerate() {
            checkpoint(cx)?;
            if u64::from(page.page_number) != u64::from(block.first_page) + index as u64
                || page.page_data.len() != self.manifest.page_size as usize
            {
                return Err(corrupt("snapshot image block has missing, repeated or mis-sized pages"));
            }
            if page.page_number == 1 {
                validate_header(&page.page_data, self.manifest.page_size, self.page_count)?;
            }
            hashes.page(page.page_number, &page.page_data);
        }
        hashes.verify(block)?;
        if self.applied[slot] { return Ok(false); }
        if self.file.file_size(cx)? != self.high_water {
            self.state = SnapshotImageState::Poisoned;
            return Err(corrupt("snapshot image staging file changed outside its owner"));
        }
        // Arm before the first await: the backend write can outlive a dropped
        // future. Do not permit retries through this owner after that boundary.
        self.state = SnapshotImageState::Poisoned;
        for page in &decoded.pages {
            checkpoint(cx)?;
            if page.page_number == 1 { continue; }
            let offset = (u64::from(page.page_number) - 1) * u64::from(self.manifest.page_size);
            self.file.write(cx, &page.page_data, offset).await?;
            self.high_water = self.high_water.max(offset + u64::from(self.manifest.page_size));
        }
        if let Some(first) = decoded.pages.first().filter(|page| page.page_number == 1) {
            self.page_one = Some(first.page_data.clone());
        }
        self.applied[slot] = true;
        self.completed += 1;
        self.state = SnapshotImageState::Writing;
        Ok(true)
    }

    /// Verify every stored block with one page of readback scratch, then sync
    /// payload, install page one, verify it, and sync again. Missing blocks
    /// return Busy without poisoning; failures after verification starts are
    /// terminal for this writer. Repeated successful calls return one receipt.
    pub async fn finish(&mut self, cx: &Cx) -> Result<SnapshotImageReceipt> {
        checkpoint(cx)?;
        if let Some(receipt) = self.receipt { return Ok(receipt); }
        if self.state != SnapshotImageState::Writing { return Err(FrankenError::BusyRecovery); }
        if self.completed != self.applied.len() { return Err(FrankenError::Busy); }
        self.state = SnapshotImageState::Poisoned;
        let expected_staged_len = if self.page_count == 1 { 0 } else { self.byte_len };
        if self.file.file_size(cx)? != expected_staged_len {
            return Err(corrupt("snapshot image staging length mismatch"));
        }
        let first = self.page_one.as_ref().ok_or_else(|| corrupt("snapshot image missing page one"))?;
        let mut scratch = vec![0; self.manifest.page_size as usize];
        let mut image_hash = blake3::Hasher::new();
        for block in &self.manifest.blocks {
            let mut hashes = BlockHashes::new(&self.manifest, block);
            for index in 0..block.page_count {
                checkpoint(cx)?;
                let number = block.first_page + index;
                let data = if number == 1 {
                    first.as_slice()
                } else {
                    let offset = (u64::from(number) - 1) * u64::from(self.manifest.page_size);
                    read_page(&self.file, cx, &mut scratch, offset).await?;
                    scratch.as_slice()
                };
                hashes.page(number, data);
                image_hash.update(data);
            }
            hashes.verify(block)?;
        }
        // Do not allow storage reordering to install a valid SQLite header
        // before the rest of the verified image has crossed its sync boundary.
        self.file.sync(cx, SyncFlags::FULL)?;
        checkpoint(cx)?;
        self.file.write(cx, first, 0).await?;
        read_page(&self.file, cx, &mut scratch, 0).await?;
        if scratch.as_slice() != first.as_slice() || self.file.file_size(cx)? != self.byte_len {
            return Err(corrupt("snapshot image page-one readback mismatch"));
        }
        self.file.sync(cx, SyncFlags::FULL)?;
        let receipt = SnapshotImageReceipt {
            manifest_id: self.manifest_id, page_size: self.manifest.page_size,
            page_count: self.page_count, byte_len: self.byte_len,
            image_blake3: *image_hash.finalize().as_bytes(),
        };
        self.receipt = Some(receipt);
        self.state = SnapshotImageState::Verified;
        Ok(receipt)
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use std::path::Path;

    use asupersync::runtime::RuntimeBuilder;
    use fsqlite_types::flags::VfsOpenFlags;
    use fsqlite_vfs::{MemoryVfs, MemoryVfsConfig, traits::Vfs};

    use super::*;
    use crate::replication_sender::{PageEntry, compute_changeset_id, encode_changeset};
    use crate::snapshot_shipping::DecodedBlockPage;

    fn run<F: std::future::Future>(future: F) -> F::Output {
        RuntimeBuilder::current_thread().build().unwrap().block_on(future)
    }

    fn file<V: Vfs>(vfs: &V, cx: &Cx, name: &str) -> V::File {
        vfs.open(cx, Some(Path::new(name)), VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE).unwrap().0
    }

    fn fixture(page_size: u32, count: u32, per_block: usize) -> (SnapshotManifest, Vec<DecodedBlock>, Vec<u8>) {
        let mut pages: Vec<_> = (1..=count).map(|number| {
            PageEntry::new(number, vec![number as u8; page_size as usize])
        }).collect();
        let first = &mut pages[0].page_bytes;
        first[..100].fill(0);
        first[..16].copy_from_slice(b"SQLite format 3\0");
        let encoded = if page_size == 65_536 { 1 } else { page_size as u16 };
        first[16..18].copy_from_slice(&encoded.to_be_bytes());
        first[18..24].copy_from_slice(&[1, 1, 0, 64, 32, 32]);
        first[24..28].copy_from_slice(&7_u32.to_be_bytes());
        first[28..32].copy_from_slice(&count.to_be_bytes());
        first[92..96].copy_from_slice(&7_u32.to_be_bytes());
        pages[0].page_xxh3 = xxhash_rust::xxh3::xxh3_64(&pages[0].page_bytes);
        let image = pages.iter().flat_map(|page| page.page_bytes.iter().copied()).collect();
        let mut manifest = SnapshotManifest { page_size, blocks: Vec::new() };
        let mut decoded = Vec::new();
        for (slot, chunk) in pages.chunks_mut(per_block).enumerate() {
            let bytes = encode_changeset(page_size, chunk).unwrap();
            manifest.blocks.push(SnapshotBlockManifest {
                changeset_id: compute_changeset_id(&bytes), first_page: chunk[0].page_number,
                page_count: chunk.len() as u32, k_source: bytes.len().div_ceil(1024) as u32,
                r_repair: 0, symbol_size: 1024, encoded_len: bytes.len() as u64,
                digest: blake3::derive_key(BLOCK_DOMAIN, &bytes),
            });
            decoded.push(DecodedBlock {
                block_index: slot as u32,
                pages: chunk.iter().map(|page| DecodedBlockPage {
                    page_number: page.page_number, page_data: page.page_bytes.clone(),
                }).collect(),
            });
        }
        manifest.validate().unwrap();
        (manifest, decoded, image)
    }

    #[test]
    fn reverse_blocks_produce_exact_image_and_withhold_header_until_finish() {
        run(async {
            let cx = Cx::new(); let vfs = MemoryVfs::new();
            let (manifest, blocks, expected) = fixture(512, 7, 2);
            let mut writer = SnapshotImageWriter::create(&cx, file(&vfs, &cx, "reverse"), manifest.clone(), manifest.id(), 4096).unwrap();
            let observer = file(&vfs, &cx, "reverse");
            for block in blocks.iter().rev() {
                assert!(writer.apply_block(&cx, block).await.unwrap());
                assert!(!writer.apply_block(&cx, block).await.unwrap());
                let mut magic = [0; 16];
                observer.read(&cx, &mut magic, 0).await.unwrap();
                assert_eq!(magic, [0; 16]);
            }
            let receipt = writer.finish(&cx).await.unwrap();
            assert_eq!(writer.state(), SnapshotImageState::Verified);
            assert_eq!(writer.finish(&cx).await.unwrap(), receipt);
            assert_eq!(receipt.image_blake3, *blake3::hash(&expected).as_bytes());
            let mut readback = vec![0; expected.len()];
            assert_eq!(observer.read(&cx, &mut readback, 0).await.unwrap(), expected.len());
            assert_eq!(readback, expected);
        });
    }

    #[test]
    fn tampered_payload_coverage_and_duplicate_are_rejected_before_writes() {
        run(async {
            let cx = Cx::new(); let vfs = MemoryVfs::new();
            let (manifest, blocks, _) = fixture(512, 3, 3);
            let mut writer = SnapshotImageWriter::create(&cx, file(&vfs, &cx, "bad"), manifest.clone(), manifest.id(), 4096).unwrap();
            for kind in 0..5 {
                let mut bad = blocks[0].clone();
                match kind {
                    0 => bad.pages[1].page_data[0] ^= 1,
                    1 => bad.pages[1].page_number = 1,
                    2 => { bad.pages.pop(); }
                    3 => { bad.pages[2].page_data.pop(); }
                    _ => bad.block_index = u32::MAX,
                }
                assert!(writer.apply_block(&cx, &bad).await.is_err());
                assert_eq!(writer.blocks_applied(), 0);
                assert_eq!(writer.file.file_size(&cx).unwrap(), 0);
            }
            writer.apply_block(&cx, &blocks[0]).await.unwrap();
            let mut bad_duplicate = blocks[0].clone();
            bad_duplicate.pages[2].page_data[9] ^= 1;
            assert!(writer.apply_block(&cx, &bad_duplicate).await.is_err());
            assert_eq!(writer.blocks_applied(), 1);
            writer.finish(&cx).await.unwrap();
        });
    }

    #[test]
    fn missing_blocks_cannot_finish_but_can_be_supplied_later() {
        run(async {
            let cx = Cx::new(); let vfs = MemoryVfs::new();
            let (manifest, blocks, _) = fixture(512, 2, 1);
            let mut writer = SnapshotImageWriter::create(&cx, file(&vfs, &cx, "missing"), manifest.clone(), manifest.id(), 4096).unwrap();
            writer.apply_block(&cx, &blocks[0]).await.unwrap();
            assert!(matches!(writer.finish(&cx).await, Err(FrankenError::Busy)));
            assert_eq!(writer.state(), SnapshotImageState::Writing);
            assert_eq!(writer.file.file_size(&cx).unwrap(), 0);
            writer.apply_block(&cx, &blocks[1]).await.unwrap();
            writer.finish(&cx).await.unwrap();
        });
    }

    #[test]
    fn construction_refuses_existing_data_wrong_manifest_and_oversized_image() {
        run(async {
            let cx = Cx::new(); let vfs = MemoryVfs::new();
            let (manifest, _, _) = fixture(512, 1, 1);
            let existing = file(&vfs, &cx, "occupied");
            existing.write(&cx, b"owned elsewhere", 0).await.unwrap();
            assert!(SnapshotImageWriter::create(&cx, file(&vfs, &cx, "occupied"), manifest.clone(), manifest.id(), 4096).is_err());
            assert!(SnapshotImageWriter::create(&cx, file(&vfs, &cx, "id"), manifest.clone(), [0; 32], 4096).is_err());
            assert!(matches!(SnapshotImageWriter::create(&cx, file(&vfs, &cx, "cap"), manifest.clone(), manifest.id(), 511), Err(FrankenError::TooBig)));
            let mut bytes = [0; 15];
            existing.read(&cx, &mut bytes, 0).await.unwrap();
            assert_eq!(&bytes, b"owned elsewhere");
        });
    }

    #[test]
    fn readback_detects_same_length_corruption_before_header_install() {
        run(async {
            let cx = Cx::new(); let vfs = MemoryVfs::new();
            let (manifest, blocks, _) = fixture(512, 3, 2);
            let mut writer = SnapshotImageWriter::create(&cx, file(&vfs, &cx, "corrupt"), manifest.clone(), manifest.id(), 4096).unwrap();
            for block in &blocks { writer.apply_block(&cx, block).await.unwrap(); }
            let observer = file(&vfs, &cx, "corrupt");
            observer.write(&cx, &[99], 700).await.unwrap();
            assert!(writer.finish(&cx).await.is_err());
            assert_eq!(writer.state(), SnapshotImageState::Poisoned);
            assert!(writer.finish(&cx).await.is_err());
            let mut magic = [0; 16];
            observer.read(&cx, &mut magic, 0).await.unwrap();
            assert_eq!(magic, [0; 16]);
        });
    }

    #[test]
    fn allocation_failure_poisoning_prevents_false_completion() {
        run(async {
            let cx = Cx::new();
            let vfs = MemoryVfs::new_with_config(MemoryVfsConfig {
                initial_reserve_bytes: 0, growth_chunk_bytes: 1, max_bytes: Some(512),
            });
            let (manifest, blocks, _) = fixture(512, 2, 2);
            let mut writer = SnapshotImageWriter::create(&cx, file(&vfs, &cx, "oom"), manifest.clone(), manifest.id(), 4096).unwrap();
            assert!(matches!(writer.apply_block(&cx, &blocks[0]).await, Err(FrankenError::OutOfMemory)));
            assert_eq!(writer.state(), SnapshotImageState::Poisoned);
            assert!(writer.finish(&cx).await.is_err());
        });
    }

    #[test]
    fn single_page_and_65536_page_images_use_exact_sqlite_geometry() {
        run(async {
            let cx = Cx::new();
            for size in [512, 4096, 65_536] {
                let vfs = MemoryVfs::new();
                let (manifest, blocks, expected) = fixture(size, 1, 1);
                let mut writer = SnapshotImageWriter::create(&cx, file(&vfs, &cx, "one"), manifest.clone(), manifest.id(), u64::from(size)).unwrap();
                writer.apply_block(&cx, &blocks[0]).await.unwrap();
                assert_eq!(writer.file.file_size(&cx).unwrap(), 0);
                let receipt = writer.finish(&cx).await.unwrap();
                assert_eq!(receipt.byte_len, u64::from(size));
                assert_eq!(receipt.image_blake3, *blake3::hash(&expected).as_bytes());
            }
        });
    }

    #[test]
    fn header_count_uses_sqlite_version_valid_for_semantics() {
        let (_, blocks, _) = fixture(512, 3, 3);
        let mut header = blocks[0].pages[0].page_data.clone();
        header[28..32].copy_from_slice(&99_u32.to_be_bytes());
        assert!(validate_header(&header, 512, 3).is_err());
        header[92..96].copy_from_slice(&6_u32.to_be_bytes());
        assert!(validate_header(&header, 512, 3).is_ok());
        header[28..32].copy_from_slice(&0_u32.to_be_bytes());
        header[92..96].copy_from_slice(&7_u32.to_be_bytes());
        assert!(validate_header(&header, 512, 3).is_ok());
        assert!(validate_header(&header, 1024, 3).is_err());
    }
}
