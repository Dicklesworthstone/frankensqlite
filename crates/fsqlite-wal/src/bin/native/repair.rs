//! Explicit WAL-only repair under the existing native recovery owner.
//!
//! No async suspension exists in the mutation phase. The blocking task owns
//! both source descriptors and all recovery locks until writes, sync, readback
//! and index publication (or rollback) settle. Dropping its awaiter cannot
//! release those locks while physical writes are still running.

use std::future::Future;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};

use asupersync::runtime::spawn_blocking;
use fsqlite_error::{FrankenError, Result};
use fsqlite_types::cx::Cx;
use fsqlite_types::flags::VfsOpenFlags;
use fsqlite_vfs::{FileIdentity, ShmRegion, Vfs, VfsFile, host_fs};
use crate::wal_fec::replay::recover_wal_fec_image_with_certificates;
use crate::wal_index::{
    WAL_INDEX_VERSION, WAL_SHM_SEGMENT_BYTES, WalIndexFrameLocation, WalIndexHdr,
    append_native_wal_index_entry, invalidate_shared_wal_index_header,
    publish_shared_wal_index_header, read_shared_wal_index_header,
    replace_shared_wal_index_region, reset_shared_wal_index_recovery_marks,
    validate_shared_wal_index_wal_binding,
};
use crate::{WAL_FRAME_HEADER_SIZE, WAL_HEADER_SIZE, WalFrameHeader};

use super::{
    CapturedSource, ExportReport, IO_CHUNK, NativeFile, NativeVfs, Options, Snapshot, SourceFile,
    capture_held, checkpoint, companion, refuse_destination_artifacts, verify_image,
};

/// Keep the original main-file descriptor alive across the identity-bound open.
/// Recovery locks are already restored, so the opener may acquire its own
/// claims. This MUST be a managed VFS descriptor: closing an independent raw
/// main descriptor would release the new connection's process-scoped locks.
struct RepairHandoff {
    source: PathBuf,
    identity: FileIdentity,
    main: SourceFile<NativeFile>,
    report: ExportReport,
}

impl RepairHandoff {
    // Connection futures and their results are intentionally allowed to be !Send.
    #[allow(clippy::future_not_send)]
    async fn open<T, F, Fut>(self, cx: &Cx, opener: F) -> (Result<T>, ExportReport)
    where
        F: FnOnce(PathBuf, FileIdentity) -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let Self { source, identity, main, report } = self;
        // Cancellation after successful repair is an OPEN failure, not evidence
        // that repair rolled back. Preserve the receipt in this case too.
        let opened = match checkpoint(cx) {
            Ok(()) => opener(source, identity).await,
            Err(error) => Err(error),
        };
        // Also runs through SourceFile::drop when this future is abandoned or
        // the opener unwinds. Native VFS cleanup retains retryable obligations.
        drop(main);
        (opened, report)
    }
}

impl Options {
    /// Repair this existing Unix WAL, then open the same physical database.
    ///
    /// `opener` is invoked at most once, only after complete recovery, the new
    /// original-WAL backup, durable writes, index publication and restoration
    /// of the recovery fence succeed. It receives the canonical source path
    /// and its captured physical identity. It MUST use an existing-only,
    /// expected-identity constructor, not an unchecked pathname open.
    /// The managed identity descriptor stays alive until its future completes
    /// or is dropped; no raw main-file descriptor is closed under the opener.
    /// The callback and returned future need not be Send.
    ///
    /// Outer `Err` means repair or its handoff could not be certified; source
    /// writes may already have occurred, so retain the backup. Outer `Ok`
    /// always includes the successful repair receipt, even when the inner
    /// open fails or cancellation is observed before the callback starts.
    /// A failed open does not undo a successfully repaired WAL. Dropping this
    /// future does not undo repair either, and may leave an unobserved receipt.
    ///
    /// This explicit administrative operation uses the caller's runtime. It
    /// does not change ordinary Connection open or writer-concurrency defaults.
    #[allow(clippy::future_not_send)]
    pub async fn repair_and_open<T, F, Fut>(
        &self,
        cx: &Cx,
        opener: F,
    ) -> Result<(Result<T>, ExportReport)>
    where
        F: FnOnce(PathBuf, FileIdentity) -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        super::preflight(cx, self)?;
        let handoff = run_for_open(&NativeVfs::new(), cx, self).await?;
        Ok(handoff.open(cx, opener).await)
    }
}

/// All offsets are into the original physical WAL. The header, inode, file
/// length, unchanged frames and uncommitted suffix are never replaced.
struct RepairPlan {
    target: Vec<u8>,
    changed: Vec<Range<usize>>,
    index_regions: Vec<Vec<u8>>,
    index_header: WalIndexHdr,
    pages: usize,
    certificate_anchors: usize,
}

fn corruption(detail: impl Into<String>) -> FrankenError {
    FrankenError::WalCorrupt { detail: detail.into() }
}

impl RepairPlan {
    fn build(snapshot: &Snapshot, options: &Options) -> Result<Self> {
        let mut database_file_id = [0; 16];
        database_file_id.copy_from_slice(snapshot.database.get(76..92)
            .ok_or_else(|| corruption("missing captured database identity"))?);
        let replay = recover_wal_fec_image_with_certificates(
            &snapshot.wal, &snapshot.sidecar, &snapshot.certificates,
            database_file_id, options.replay,
        )?;
        // This additionally refuses mixed snapshots, missing page provenance
        // and incompatible main/WAL headers before any backup or source write.
        let database = replay.database_image(&snapshot.database, options.max_database_bytes)?;
        let page_size = usize::try_from(replay.header().page_size)
            .map_err(|_| FrankenError::TooBig)?;
        let pages = database.len() / page_size;
        drop(database);
        let frame_size = page_size.checked_add(WAL_FRAME_HEADER_SIZE)
            .ok_or(FrankenError::TooBig)?;
        let prefix = replay.replayable_prefix();
        if prefix.len() > snapshot.wal.len()
            || prefix.get(..WAL_HEADER_SIZE) != snapshot.wal.get(..WAL_HEADER_SIZE)
        {
            return Err(corruption("repair cannot replace a WAL generation"));
        }
        let mut target = Vec::new();
        target.try_reserve_exact(snapshot.wal.len()).map_err(|_| FrankenError::OutOfMemory)?;
        target.extend_from_slice(&snapshot.wal);
        target[..prefix.len()].copy_from_slice(prefix);
        let frame_count = replay.committed_frames();
        let mut changed = Vec::new();
        changed.try_reserve(usize::try_from(frame_count).map_err(|_| FrankenError::TooBig)?)
            .map_err(|_| FrankenError::OutOfMemory)?;
        for offset in (WAL_HEADER_SIZE..prefix.len()).step_by(frame_size) {
            let range = offset..offset + frame_size;
            if snapshot.wal[range.clone()] != target[range.clone()] {
                changed.push(range);
            }
        }

        let last_region = if frame_count == 0 { 0 } else {
            WalIndexFrameLocation::new(frame_count)?.region
        };
        let mut index_regions = Vec::new();
        for _ in 0..=last_region {
            let mut region = Vec::new();
            region.try_reserve_exact(WAL_SHM_SEGMENT_BYTES).map_err(|_| FrankenError::OutOfMemory)?;
            region.resize(WAL_SHM_SEGMENT_BYTES, 0);
            index_regions.push(region);
        }
        let mut terminal = None;
        for (index, frame) in prefix[WAL_HEADER_SIZE..].chunks_exact(frame_size).enumerate() {
            let number = u32::try_from(index).ok().and_then(|index| index.checked_add(1))
                .ok_or(FrankenError::TooBig)?;
            let marker = WalFrameHeader::from_bytes(frame)?;
            let region = usize::try_from(WalIndexFrameLocation::new(number)?.region)
                .map_err(|_| FrankenError::TooBig)?;
            append_native_wal_index_entry(&mut index_regions[region], number, marker.page_number)?;
            terminal = Some((number, marker));
        }
        let wal_header = replay.header();
        let mut index_header = WalIndexHdr {
            i_version: WAL_INDEX_VERSION, unused: 0, i_change: 0, is_init: 1,
            big_end_cksum: u8::from(wal_header.big_endian_checksum()),
            sz_page: if page_size == 65_536 { 1 } else {
                u16::try_from(page_size).map_err(|_| FrankenError::TooBig)?
            },
            mx_frame: frame_count,
            n_page: terminal.map_or(0, |(_, marker)| marker.db_size),
            a_frame_cksum: terminal.map_or([0, 0], |(_, marker)| {
                [marker.checksum.s1, marker.checksum.s2]
            }),
            a_salt: [wal_header.salts.salt1, wal_header.salts.salt2],
            a_cksum: [0, 0],
        };
        index_header.update_checksum()?;
        validate_shared_wal_index_wal_binding(&index_header, wal_header, terminal)?;
        Ok(Self {
            target, changed, index_regions, index_header, pages,
            certificate_anchors: replay.certificate_anchors().len(),
        })
    }
}

/// Protect even an error or panic during index replacement/publication. A
/// partial derived index must never remain advertised as a valid snapshot.
struct IndexPublication {
    zero: ShmRegion,
    complete: bool,
}

impl Drop for IndexPublication {
    fn drop(&mut self) {
        if !self.complete
            && let Err(error) = invalidate_shared_wal_index_header(&self.zero)
        {
            eprintln!("failed to invalidate incomplete recovery index: {error}");
        }
    }
}

fn write_ranges(file: &mut (impl Write + Seek), image: &[u8], ranges: &[Range<usize>]) -> io::Result<()> {
    for range in ranges {
        file.seek(SeekFrom::Start(u64::try_from(range.start)
            .map_err(|_| io::Error::other("WAL repair offset overflow"))?))?;
        file.write_all(&image[range.clone()])?;
    }
    Ok(())
}

/// Settle mutation, or restore and verify the exact original damaged bytes.
/// The caller masks cancellation and keeps the recovery owner alive throughout.
fn settle_writes<W: Read + Write + Seek>(
    file: &mut W,
    cx: &Cx,
    original: &[u8],
    plan: &RepairPlan,
    mut sync: impl FnMut(&mut W) -> io::Result<()>,
) -> Result<blake3::Hash> {
    let write = (|| {
        write_ranges(file, &plan.target, &plan.changed)?;
        sync(file)?;
        verify_image(file, cx, &plan.target)
    })();
    match write {
        Ok(digest) => Ok(digest),
        Err(error) => {
            let restore = (|| {
                write_ranges(file, original, &plan.changed)?;
                sync(file)?;
                verify_image(file, cx, original)
            })();
            match restore {
                Ok(_) => Err(corruption(format!(
                    "WAL repair failed; exact original WAL restored and synced: {error}"
                ))),
                Err(restore_error) => Err(corruption(format!(
                    "WAL repair outcome is indeterminate; retain the original backup and do not use the database: repair={error}; restore={restore_error}"
                ))),
            }
        }
    }
}

fn recheck_names(vfs: &NativeVfs, cx: &Cx, source: &Path, captured: &CapturedSource) -> Result<()> {
    let wal_path = companion(source, "-wal");
    for (path, identity, flags) in [
        (source, captured.main_identity, VfsOpenFlags::READWRITE | VfsOpenFlags::MAIN_DB),
        (wal_path.as_path(), captured.wal_identity, VfsOpenFlags::READONLY | VfsOpenFlags::WAL),
    ] {
        let (probe, _) = vfs.open_with_expected_identity(cx, path, flags, identity)?;
        SourceFile::new(probe, cx).finish()?;
    }
    Ok(())
}

fn backup_original(vfs: &NativeVfs, cx: &Cx, path: &Path, original: &[u8]) -> Result<()> {
    checkpoint(cx)?;
    refuse_destination_artifacts(vfs, cx, path)?;
    let mut backup = host_fs::reserve_new_file(path)?;
    let result = (|| {
        refuse_destination_artifacts(vfs, cx, path)?;
        for chunk in original.chunks(IO_CHUNK) {
            checkpoint(cx)?;
            backup.write_all(chunk)?;
        }
        backup.sync_all()?;
        verify_image(&mut backup, cx, original)?;
        vfs.sync_parent_directory(cx, path)
    })();
    if result.is_err() {
        eprintln!("Incomplete backup retained at {}; source WAL has not been modified", path.display());
    }
    result
}

fn repair_captured(cx: &Cx, options: &Options, mut captured: CapturedSource) -> Result<RepairHandoff> {
    checkpoint(cx)?;
    let vfs = NativeVfs::new();
    let mut plan = RepairPlan::build(&captured.snapshot, options)?;
    let wal_path = companion(&options.source, "-wal");
    recheck_names(&vfs, cx, &options.source, &captured)?;
    // Main is never independently opened through std::fs. This descriptor is
    // for the separate, identity-checked single-link WAL only, and is dropped
    // before either VFS source descriptor releases its recovery obligations.
    let mut wal = host_fs::open_wal_for_guarded_repair(&wal_path, captured.wal_identity)?;
    verify_image(&mut wal, cx, &captured.snapshot.wal)?;
    let region_size = u32::try_from(WAL_SHM_SEGMENT_BYTES).map_err(|_| FrankenError::TooBig)?;
    let mut regions = Vec::new();
    for number in 0..plan.index_regions.len() {
        regions.push(captured.main.file.shm_map(
            cx, u32::try_from(number).map_err(|_| FrankenError::TooBig)?, region_size, true,
        )?);
    }
    if let Ok(Some(previous)) = read_shared_wal_index_header(&regions[0]) {
        plan.index_header.i_change = previous.i_change.wrapping_add(1);
        plan.index_header.update_checksum()?;
    }
    backup_original(&vfs, cx, &options.destination, &captured.snapshot.wal)?;
    recheck_names(&vfs, cx, &options.source, &captured)?;
    verify_image(&mut wal, cx, &captured.snapshot.wal)?;
    checkpoint(cx)?;

    // Once source mutation can begin, cancellation must not interrupt its
    // settlement. All work below is synchronous on this owning blocking task.
    let _mask = cx.masked();
    invalidate_shared_wal_index_header(&regions[0])?;
    let mut publication = IndexPublication { zero: regions[0].share(), complete: false };
    let result = (|| {
        let digest = settle_writes(&mut wal, cx, &captured.snapshot.wal, &plan, |file| file.sync_all())?;
        for (number, (region, bytes)) in regions.iter().zip(&plan.index_regions).enumerate() {
            replace_shared_wal_index_region(
                region, u32::try_from(number).map_err(|_| FrankenError::TooBig)?, bytes,
            )?;
        }
        reset_shared_wal_index_recovery_marks(&regions[0], plan.index_header.mx_frame)?;
        captured.main.file.shm_barrier();
        publish_shared_wal_index_header(&regions[0], &plan.index_header)?;
        if read_shared_wal_index_header(&regions[0])? != Some(plan.index_header) {
            return Err(corruption("repaired WAL index failed publication readback"));
        }
        publication.complete = true;
        Ok::<_, FrankenError>(digest)
    })();
    drop(publication);
    drop(regions);
    drop(wal);
    if result.is_err() {
        eprintln!("WAL repair is NOT certified; original backup retained at {}", options.destination.display());
    }
    let digest = result?;
    // Stop blocking native readers/writers before invoking an SQL constructor,
    // but retain the same main descriptor as a live, non-reusable identity.
    // A failed restoration retains its marker and NEVER authorizes the opener.
    captured.wal.finish()?;
    {
        let _cleanup_mask = captured.main.cleanup_cx.masked();
        captured.main.file.restore_external_maintenance_attempt(&captured.main.cleanup_cx)?;
        captured.main.maintenance = false;
    }
    let report = ExportReport {
        destination: options.destination.clone(), pages: plan.pages,
        wal_frames: plan.index_header.mx_frame, repaired_frames: plan.changed.len(),
        certificate_anchors: plan.certificate_anchors, digest, repaired_in_place: true,
    };
    Ok(RepairHandoff {
        source: options.source.clone(), identity: captured.main_identity,
        main: captured.main, report,
    })
}

pub(super) async fn run(vfs: &NativeVfs, cx: &Cx, options: &Options) -> Result<ExportReport> {
    let mut handoff = run_for_open(vfs, cx, options).await?;
    handoff.main.finish()?;
    Ok(handoff.report)
}

async fn run_for_open(vfs: &NativeVfs, cx: &Cx, options: &Options) -> Result<RepairHandoff> {
    let source = vfs.full_pathname(cx, &options.source)?;
    let destination = vfs.full_pathname(cx, &options.destination)?;
    // A backup must not become any companion of the source, even one that is
    // currently absent. This prevents an optional FEC/certificate path from
    // accidentally acquiring the original WAL bytes as its contents.
    if ["", "-wal", "-shm", "-journal", "-wal-fec", "-wal-fec.lock", "-wal-cert", ".fsqlite-shm"]
        .iter().any(|suffix| destination == companion(&source, suffix))
        || vfs.path_entry_exists(cx, &destination)?
    {
        return Err(FrankenError::CannotOpen { path: destination });
    }
    refuse_destination_artifacts(vfs, cx, &destination)?;
    let options = Options { source, destination, ..*options };
    let captured = capture_held(vfs, cx, &options).await?;
    let worker_cx = cx.create_child_for_spawn();
    // Ownership is moved, not borrowed. A cancelled/dropped awaiter cannot
    // run SourceFile::drop while this closure still has pending physical work.
    spawn_blocking(move || repair_captured(&worker_cx, &options, captured)).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;
    use std::io::Cursor;
    use std::rc::Rc;
    use std::task::{Context, Poll, Waker};

    fn with_runtime<F: Future>(future: F) -> F::Output {
        asupersync::runtime::RuntimeBuilder::current_thread()
            .blocking_threads(1, 2).build().unwrap().block_on(future)
    }

    fn attached_context() -> Cx {
        let cx = Cx::new();
        cx.set_native_cx(asupersync::Cx::current().unwrap());
        cx
    }

    /// Three versions of an empty sqlite_schema page in one FEC-covered
    /// transaction. Damage is in the first payload, not its terminal anchor.
    fn handoff_fixture() -> (Options, Vec<u8>, Vec<u8>) {
        use crate::checksum::{SqliteWalChecksum, WalChecksumTransform, WalHeader, WalSalts};
        use crate::wal_fec::{
            WalFecGroupMeta, WalFecGroupMetaInit, WalFecGroupRecord, append_wal_fec_group,
            build_source_page_hashes, generate_wal_fec_repair_symbols,
        };
        use fsqlite_types::{ObjectId, Oti};

        let directory = tempfile::tempdir().unwrap().keep();
        let options = Options::new(directory.join("source.db"), directory.join("original.wal"));
        let mut page = vec![0_u8; 512];
        page[..16].copy_from_slice(b"SQLite format 3\0");
        page[16..18].copy_from_slice(&512_u16.to_be_bytes());
        page[18..20].copy_from_slice(&[2, 2]);
        page[21..24].copy_from_slice(&[64, 32, 32]);
        page[24..28].copy_from_slice(&7_u32.to_be_bytes());
        page[28..32].copy_from_slice(&1_u32.to_be_bytes());
        page[44..48].copy_from_slice(&4_u32.to_be_bytes());
        page[56..60].copy_from_slice(&1_u32.to_be_bytes());
        page[92..96].copy_from_slice(&7_u32.to_be_bytes());
        page[100] = 13;
        page[105..107].copy_from_slice(&512_u16.to_be_bytes());
        host_fs::write(&options.source, &page).unwrap();
        let pages: Vec<_> = (1_u32..=3).map(|version| {
            let mut current = page.clone();
            current[60..64].copy_from_slice(&version.to_be_bytes());
            current
        }).collect();
        let header = WalHeader {
            magic: crate::WAL_MAGIC_LE, format_version: crate::WAL_FORMAT_VERSION,
            page_size: 512, checkpoint_seq: 1,
            salts: WalSalts { salt1: 123, salt2: 456 }, checksum: SqliteWalChecksum::default(),
        };
        let mut wal = header.to_bytes().unwrap().to_vec();
        let mut running = WalHeader::from_bytes(&wal).unwrap().checksum;
        for (index, page) in pages.iter().enumerate() {
            let start = wal.len();
            wal.extend_from_slice(&WalFrameHeader {
                page_number: 1, db_size: u32::from(index == 2), salts: header.salts,
                checksum: SqliteWalChecksum::default(),
            }.to_bytes());
            wal.extend_from_slice(page);
            running = WalChecksumTransform::for_wal_frame(&wal[start..], 512, false)
                .unwrap().apply(running);
            wal[start + 16..start + 20].copy_from_slice(&running.s1.to_be_bytes());
            wal[start + 20..start + 24].copy_from_slice(&running.s2.to_be_bytes());
        }
        let meta = WalFecGroupMeta::from_init(WalFecGroupMetaInit {
            wal_salt1: 123, wal_salt2: 456, start_frame_no: 1, end_frame_no: 3,
            db_size_pages: 1, page_size: 512, k_source: 3, r_repair: 8,
            oti: Oti { f: 1536, al: 1, t: 512, z: 1, n: 1 },
            object_id: ObjectId::derive_from_canonical_bytes(b"repair-open-handoff"),
            page_numbers: vec![1; 3], source_page_xxh3_128: build_source_page_hashes(&pages),
        }).unwrap();
        let symbols = generate_wal_fec_repair_symbols(&meta, &pages).unwrap();
        append_wal_fec_group(&companion(&options.source, "-wal-fec"),
            &WalFecGroupRecord::new(meta, symbols).unwrap()).unwrap();
        let repaired = wal.clone();
        wal[WAL_HEADER_SIZE + WAL_FRAME_HEADER_SIZE + 60] ^= 0xff;
        host_fs::write(&companion(&options.source, "-wal"), &wal).unwrap();
        (options, wal, repaired)
    }

    fn assert_recovery_available(source: &Path, cx: &Cx) {
        let (file, _) = NativeVfs::new().open(cx, Some(source),
            VfsOpenFlags::READWRITE | VfsOpenFlags::MAIN_DB).unwrap();
        let mut owner = SourceFile::new(file, cx);
        owner.acquire_recovery(cx).unwrap();
        owner.finish().unwrap();
    }

    #[test]
    fn handoff_opens_once_with_live_identity_after_repair_fences_are_restored() {
        with_runtime(async {
            let (options, original, repaired) = handoff_fixture();
            let cx = attached_context();
            let calls = Rc::new(Cell::new(0));
            let calls_in_opener = Rc::clone(&calls);
            let opener_cx = &cx;
            let (opened, report) = options.repair_and_open(&cx, |path, identity| async move {
                calls_in_opener.set(calls_in_opener.get() + 1);
                let (file, _) = NativeVfs::new().open_with_expected_identity(
                    opener_cx, &path, VfsOpenFlags::READWRITE | VfsOpenFlags::MAIN_DB, identity,
                )?;
                let mut owner = SourceFile::new(file, opener_cx);
                assert_eq!(owner.file.file_identity()?, Some(identity));
                // This would conflict if the old recovery fence remained held.
                owner.acquire_recovery(opener_cx)?;
                owner.finish()?;
                Ok(calls_in_opener) // Explicitly non-Send result and future.
            }).await.unwrap();
            assert!(Rc::ptr_eq(&opened.unwrap(), &calls));
            assert_eq!(calls.get(), 1);
            assert_eq!(report.wal_frames, 3);
            assert_eq!(report.repaired_frames, 1);
            assert!(report.repaired_in_place);
            assert_eq!(report.digest, blake3::hash(&repaired));
            assert_eq!(host_fs::read(&options.destination).unwrap(), original);
            assert_eq!(host_fs::read(&companion(&options.source, "-wal")).unwrap(), repaired);
        });
    }

    #[test]
    fn handoff_preserves_repair_receipt_when_opener_fails() {
        with_runtime(async {
            let (options, original, repaired) = handoff_fixture();
            let cx = attached_context();
            let (opened, report) = options.repair_and_open(&cx, |_, _| async {
                Err::<(), _>(FrankenError::NoSuchTable { name: "opener sentinel".to_owned() })
            }).await.unwrap();
            assert!(matches!(opened, Err(FrankenError::NoSuchTable { name }) if name == "opener sentinel"));
            assert_eq!(report.digest, blake3::hash(&repaired));
            assert_eq!(host_fs::read(&options.destination).unwrap(), original);
            assert_eq!(host_fs::read(&companion(&options.source, "-wal")).unwrap(), repaired);
            assert_recovery_available(&options.source, &cx);
        });
    }

    #[test]
    fn handoff_does_not_unlock_the_openers_native_claims() {
        const CHILD_PATH: &str = "FSQLITE_REPAIR_OPEN_LOCK_CHILD";
        const TEST: &str = "native_recovery::repair::tests::handoff_does_not_unlock_the_openers_native_claims";
        if let Some(path) = std::env::var_os(CHILD_PATH) {
            with_runtime(async {
                let cx = attached_context();
                let (file, _) = NativeVfs::new().open(&cx, Some(Path::new(&path)),
                    VfsOpenFlags::READWRITE | VfsOpenFlags::MAIN_DB).unwrap();
                let mut peer = SourceFile::new(file, &cx);
                assert!(matches!(peer.acquire_recovery(&cx), Err(FrankenError::Busy)));
                peer.finish().unwrap();
            });
            return;
        }
        with_runtime(async {
            let (options, _, _) = handoff_fixture();
            let cx = attached_context();
            let opener_cx = &cx;
            let (opened, _) = options.repair_and_open(&cx, |path, identity| async move {
                let (file, _) = NativeVfs::new().open_with_expected_identity(
                    opener_cx, &path, VfsOpenFlags::READWRITE | VfsOpenFlags::MAIN_DB, identity,
                )?;
                let mut new_owner = SourceFile::new(file, opener_cx);
                new_owner.acquire_recovery(opener_cx)?;
                Ok(new_owner)
            }).await.unwrap();
            let mut owner = opened.unwrap();
            // The old handoff descriptor has now been cleaned up. A raw
            // close-any-fd bug would silently lose this owner's POSIX claims;
            // a new process checks the actual kernel locks, not our ledger.
            let mut child = std::process::Command::new(std::env::current_exe().unwrap())
                .args([TEST, "--exact", "--nocapture"])
                .env(CHILD_PATH, &options.source).spawn().unwrap();
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
            loop {
                if let Some(status) = child.try_wait().unwrap() {
                    assert!(status.success(), "foreign process must observe the opener's locks");
                    break;
                }
                if std::time::Instant::now() >= deadline {
                    let _ = child.kill();
                    let _ = child.wait();
                    panic!("foreign lock witness did not terminate");
                }
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
            owner.finish().unwrap();
            assert_recovery_available(&options.source, &cx);
        });
    }

    #[test]
    fn handoff_refusal_never_invokes_opener_or_overwrites_a_backup() {
        with_runtime(async {
            for existing_backup in [false, true] {
                let (options, original, _) = handoff_fixture();
                let cx = attached_context();
                if existing_backup {
                    host_fs::write(&options.destination, b"existing backup").unwrap();
                } else {
                    host_fs::write(&companion(&options.source, "-wal"), b"invalid WAL").unwrap();
                }
                let calls = Cell::new(0);
                let result = options.repair_and_open(&cx, |_, _| {
                    calls.set(calls.get() + 1);
                    std::future::ready(Ok(()))
                }).await;
                assert!(result.is_err());
                assert_eq!(calls.get(), 0);
                if existing_backup {
                    assert_eq!(host_fs::read(&options.destination).unwrap(), b"existing backup");
                    assert_eq!(host_fs::read(&companion(&options.source, "-wal")).unwrap(), original);
                } else {
                    assert!(!NativeVfs::new().path_entry_exists(&cx, &options.destination).unwrap());
                }
            }
        });
    }

    #[test]
    fn handoff_contention_never_invokes_opener_and_can_be_retried() {
        with_runtime(async {
            let (options, original, _) = handoff_fixture();
            let cx = attached_context();
            let vfs = NativeVfs::new();
            let (file, _) = vfs.open(&cx, Some(&options.source),
                VfsOpenFlags::READWRITE | VfsOpenFlags::MAIN_DB).unwrap();
            let mut owner = SourceFile::new(file, &cx);
            owner.acquire_recovery(&cx).unwrap();
            let calls = Cell::new(0);
            let result = options.repair_and_open(&cx, |_, _| {
                calls.set(calls.get() + 1);
                std::future::ready(Ok(()))
            }).await;
            assert!(result.is_err());
            assert_eq!(calls.get(), 0);
            assert!(!vfs.path_entry_exists(&cx, &options.destination).unwrap());
            assert_eq!(host_fs::read(&companion(&options.source, "-wal")).unwrap(), original);
            owner.finish().unwrap();
            options.repair_and_open(&cx, |_, _| std::future::ready(Ok(())))
                .await.unwrap().0.unwrap();
        });
    }

    #[test]
    fn handoff_cancellation_after_repair_retains_receipt_without_starting_open() {
        with_runtime(async {
            let (options, original, repaired) = handoff_fixture();
            let cx = attached_context();
            let handoff = run_for_open(&NativeVfs::new(), &cx, &options).await.unwrap();
            cx.cancel();
            let calls = Cell::new(0);
            let (opened, report) = handoff.open(&cx, |_, _| {
                calls.set(calls.get() + 1);
                std::future::ready(Ok(()))
            }).await;
            assert!(matches!(opened, Err(FrankenError::Interrupt)));
            assert_eq!(calls.get(), 0);
            assert_eq!(report.digest, blake3::hash(&repaired));
            assert_eq!(host_fs::read(&options.destination).unwrap(), original);
        });
    }

    #[test]
    fn handoff_drop_during_open_releases_managed_identity_guard() {
        with_runtime(async {
            let (options, original, repaired) = handoff_fixture();
            let cx = attached_context();
            let handoff = run_for_open(&NativeVfs::new(), &cx, &options).await.unwrap();
            let calls = Cell::new(0);
            let mut future = Box::pin(handoff.open(&cx, |_, _| {
                calls.set(calls.get() + 1);
                std::future::pending::<Result<()>>()
            }));
            assert!(matches!(future.as_mut().poll(&mut Context::from_waker(Waker::noop())), Poll::Pending));
            drop(future);
            assert_eq!(calls.get(), 1);
            assert_recovery_available(&options.source, &cx);
            assert_eq!(host_fs::read(&options.destination).unwrap(), original);
            assert_eq!(host_fs::read(&companion(&options.source, "-wal")).unwrap(), repaired);
        });
    }

    #[test]
    fn handoff_unpolled_future_and_preflight_failure_have_no_effects() {
        with_runtime(async {
            let (options, original, _) = handoff_fixture();
            let cx = attached_context();
            let calls = Cell::new(0);
            drop(options.repair_and_open(&cx, |_, _| {
                calls.set(1);
                std::future::ready(Ok(()))
            }));
            let detached = Cx::new();
            assert!(options.repair_and_open(&detached, |_, _| {
                calls.set(1);
                std::future::ready(Ok(()))
            }).await.is_err());
            assert_eq!(calls.get(), 0);
            assert!(!NativeVfs::new().path_entry_exists(&cx, &options.destination).unwrap());
            assert_eq!(host_fs::read(&companion(&options.source, "-wal")).unwrap(), original);
        });
    }

    fn byte_plan(original: &[u8]) -> RepairPlan {
        let mut target = original.to_vec();
        target[40..60].fill(0x77);
        target[100..130].fill(0x66);
        RepairPlan {
            target, changed: vec![40..60, 100..130], index_regions: Vec::new(),
            index_header: WalIndexHdr {
                i_version: WAL_INDEX_VERSION, unused: 0, i_change: 0, is_init: 1,
                big_end_cksum: 0, sz_page: 512, mx_frame: 0, n_page: 0,
                a_frame_cksum: [0; 2], a_salt: [0; 2], a_cksum: [0; 2],
            },
            pages: 1, certificate_anchors: 0,
        }
    }

    #[test]
    fn physical_settlement_preserves_header_length_and_untouched_bytes() {
        let original = vec![0x11; 256];
        let plan = byte_plan(&original);
        let mut file = Cursor::new(original.clone());
        let digest = settle_writes(&mut file, &Cx::new(), &original, &plan, |_| Ok(())).unwrap();
        assert_eq!(file.get_ref(), &plan.target);
        assert_eq!(digest, blake3::hash(&plan.target));
        assert_eq!(&file.get_ref()[..WAL_HEADER_SIZE], &original[..WAL_HEADER_SIZE]);
        assert_eq!(file.get_ref().len(), original.len());
    }

    #[test]
    fn failed_sync_restores_the_original_and_never_returns_a_receipt() {
        let original = vec![0x11; 256];
        let plan = byte_plan(&original);
        let mut file = Cursor::new(original.clone());
        let mut calls = 0;
        let error = settle_writes(&mut file, &Cx::new(), &original, &plan, |_| {
            calls += 1;
            if calls == 1 { Err(io::Error::other("injected sync failure")) } else { Ok(()) }
        }).unwrap_err();
        assert_eq!(calls, 2);
        assert_eq!(file.into_inner(), original);
        assert!(error.to_string().contains("original WAL restored and synced"));
    }

    #[test]
    fn failed_rollback_sync_is_reported_as_indeterminate() {
        let original = vec![0x11; 256];
        let plan = byte_plan(&original);
        let mut file = Cursor::new(original.clone());
        let error = settle_writes(&mut file, &Cx::new(), &original, &plan, |_| {
            Err(io::Error::other("persistent sync failure"))
        }).unwrap_err();
        assert!(error.to_string().contains("indeterminate"));
    }

    #[test]
    fn bad_readback_restores_the_original_before_returning_error() {
        let original = vec![0x11; 256];
        let plan = byte_plan(&original);
        let mut file = Cursor::new(original.clone());
        let mut calls = 0;
        let error = settle_writes(&mut file, &Cx::new(), &original, &plan, |file| {
            calls += 1;
            if calls == 1 { file.get_mut()[45] ^= 1; }
            Ok(())
        }).unwrap_err();
        assert_eq!(file.into_inner(), original);
        assert!(error.to_string().contains("original WAL restored"));
    }

    struct TornWrite {
        file: Cursor<Vec<u8>>,
        bytes_until_failure: usize,
        failed: bool,
    }

    impl Read for TornWrite {
        fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
            self.file.read(bytes)
        }
    }

    impl Seek for TornWrite {
        fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
            self.file.seek(position)
        }
    }

    impl Write for TornWrite {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            if !self.failed {
                if self.bytes_until_failure == 0 {
                    self.failed = true;
                    return Err(io::Error::other("injected torn physical write"));
                }
                let len = bytes.len().min(self.bytes_until_failure);
                self.bytes_until_failure -= len;
                return self.file.write(&bytes[..len]);
            }
            self.file.write(bytes)
        }

        fn flush(&mut self) -> io::Result<()> { Ok(()) }
    }

    #[test]
    fn every_partial_write_boundary_restores_the_exact_original() {
        let original = vec![0x11; 256];
        let plan = byte_plan(&original);
        for prefix_bytes in 0..50 {
            let mut file = TornWrite {
                file: Cursor::new(original.clone()), bytes_until_failure: prefix_bytes, failed: false,
            };
            let error = settle_writes(&mut file, &Cx::new(), &original, &plan, |_| Ok(()))
                .unwrap_err();
            assert!(file.failed);
            assert_eq!(file.file.into_inner(), original);
            assert!(error.to_string().contains("original WAL restored and synced"));
        }
    }

    #[test]
    fn cancellation_after_mutation_does_not_interrupt_masked_settlement() {
        let original = vec![0x11; 256];
        let plan = byte_plan(&original);
        let mut file = Cursor::new(original.clone());
        let cx = Cx::new();
        let _mask = cx.masked();
        settle_writes(&mut file, &cx, &original, &plan, |_| {
            cx.cancel();
            Ok(())
        }).unwrap();
        assert_eq!(file.into_inner(), plan.target);
    }
}
