//! Checkpoint crash-safety regressions using the real WAL and MemoryVfs I/O.

use fsqlite_error::FrankenError;
use fsqlite_types::PageNumber;
use fsqlite_types::cx::Cx;
use fsqlite_types::flags::{SyncFlags, VfsOpenFlags};
use fsqlite_vfs::traits::Vfs;
use fsqlite_vfs::{MemoryVfs, SyncKind, VfsFile};

use super::{CheckpointTarget, CheckpointTargetFuture, execute_checkpoint};
use crate::checkpoint::{CheckpointMode, CheckpointState};
use crate::checksum::{WalHeader, WalSalts};
use crate::fault_hooks::{self, FaultHookArm, FaultInjectionSessionLock};
use crate::test_support::FutureResultTestExt as _;
use crate::wal::WalFile;

type TestFile = <MemoryVfs as Vfs>::File;

const PAGE_SIZE: u32 = 4096;
const MODES: [CheckpointMode; 4] = [
    CheckpointMode::Passive,
    CheckpointMode::Full,
    CheckpointMode::Restart,
    CheckpointMode::Truncate,
];

fn page(fill: u8) -> Vec<u8> {
    vec![fill; usize::try_from(PAGE_SIZE).expect("page size fits usize")]
}

fn open_wal(vfs: &MemoryVfs, cx: &Cx) -> TestFile {
    let flags = VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE | VfsOpenFlags::WAL;
    vfs.open(cx, Some(std::path::Path::new("durability.db-wal")), flags)
        .expect("open WAL")
        .0
}

fn populated_wal(vfs: &MemoryVfs, cx: &Cx) -> WalFile<TestFile> {
    let mut wal = WalFile::create(
        cx,
        open_wal(vfs, cx),
        PAGE_SIZE,
        0,
        WalSalts {
            salt1: 0x1234_5678,
            salt2: 0x9ABC_DEF0,
        },
    )
    .expect("create WAL");
    wal.append_frame(cx, 1, &page(0x11), 0)
        .expect("append first transaction page");
    wal.append_frame(cx, 2, &page(0x22), 2)
        .expect("append commit page");
    assert_eq!(wal.last_fsynced_frame_count(), 0);
    wal
}

fn state() -> CheckpointState {
    CheckpointState {
        total_frames: 2,
        backfilled_frames: 0,
        oldest_reader_frame: None,
    }
}

/// Real VFS-backed database target, with counters at externally visible edges.
/// The write-error switch exercises failure at the first database mutation.
struct Target {
    file: TestFile,
    written: Vec<PageNumber>,
    write_attempts: usize,
    truncations: Vec<u32>,
    syncs: usize,
    published: Vec<u32>,
    reset_gates: usize,
    reset_releases: usize,
    fail_write: bool,
}

impl Target {
    fn new(vfs: &MemoryVfs, cx: &Cx) -> Self {
        let flags = VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE;
        let (file, _) = vfs
            .open(cx, Some(std::path::Path::new("durability.db")), flags)
            .expect("open database");
        Self {
            file,
            written: Vec::new(),
            write_attempts: 0,
            truncations: Vec::new(),
            syncs: 0,
            published: Vec::new(),
            reset_gates: 0,
            reset_releases: 0,
            fail_write: false,
        }
    }

    fn assert_no_calls(&self) {
        assert_eq!(self.write_attempts, 0);
        assert!(self.written.is_empty());
        assert!(self.truncations.is_empty());
        assert_eq!(self.syncs, 0);
        assert!(self.published.is_empty());
        assert_eq!(self.reset_gates, 0);
        assert_eq!(self.reset_releases, 0);
    }

    fn assert_untouched(&self, cx: &Cx) {
        self.assert_no_calls();
        assert_eq!(self.file.file_size(cx).expect("database size"), 0);
    }

    fn assert_page(&self, cx: &Cx, number: u32, fill: u8) {
        let mut bytes = page(0);
        let offset = u64::from(number - 1) * u64::from(PAGE_SIZE);
        let read = self
            .file
            .read(cx, &mut bytes, offset)
            .expect("read database page");
        assert_eq!(read, bytes.len());
        assert_eq!(bytes, page(fill));
    }
}

impl CheckpointTarget for Target {
    fn write_page<'a>(
        &'a mut self,
        cx: &'a Cx,
        page_no: PageNumber,
        data: &'a [u8],
    ) -> CheckpointTargetFuture<'a, ()> {
        Box::pin(async move {
            self.write_attempts += 1;
            if self.fail_write {
                return Err(FrankenError::Io(std::io::Error::other(
                    "injected database write failure",
                )));
            }
            let offset = u64::from(page_no.get() - 1) * u64::from(PAGE_SIZE);
            self.file.write(cx, data, offset).await?;
            self.written.push(page_no);
            Ok(())
        })
    }

    fn truncate_db<'a>(&'a mut self, cx: &'a Cx, pages: u32) -> CheckpointTargetFuture<'a, ()> {
        Box::pin(async move {
            self.file
                .truncate(cx, u64::from(pages) * u64::from(PAGE_SIZE))?;
            self.truncations.push(pages);
            Ok(())
        })
    }

    fn sync_db<'a>(&'a mut self, cx: &'a Cx) -> CheckpointTargetFuture<'a, ()> {
        Box::pin(async move {
            self.file.sync(cx, SyncFlags::FULL)?;
            self.syncs += 1;
            Ok(())
        })
    }

    fn read_page_if_supported<'a>(
        &'a mut self,
        cx: &'a Cx,
        page_no: PageNumber,
        buf: &'a mut [u8],
    ) -> CheckpointTargetFuture<'a, Option<usize>> {
        Box::pin(async move {
            let offset = u64::from(page_no.get() - 1) * u64::from(PAGE_SIZE);
            self.file.read(cx, buf, offset).await.map(Some)
        })
    }

    fn publish_backfill<'a>(
        &'a mut self,
        _cx: &'a Cx,
        _header: &'a WalHeader,
        frames: u32,
    ) -> CheckpointTargetFuture<'a, ()> {
        Box::pin(async move {
            self.published.push(frames);
            Ok(())
        })
    }

    fn acquire_wal_reset_gate<'a>(&'a mut self, _cx: &'a Cx) -> CheckpointTargetFuture<'a, bool> {
        Box::pin(async move {
            self.reset_gates += 1;
            Ok(true)
        })
    }

    fn release_wal_reset_gate<'a>(&'a mut self, _cx: &'a Cx) -> CheckpointTargetFuture<'a, ()> {
        Box::pin(async move {
            self.reset_releases += 1;
            Ok(())
        })
    }
}

#[test]
fn wal_sync_failure_prevents_checkpoint_mutation_in_every_mode() {
    let lock = FaultInjectionSessionLock::new();
    let _session = lock.lock().expect("own fault hooks");
    for mode in MODES {
        let cx = Cx::default();
        let vfs = MemoryVfs::new();
        let mut wal = populated_wal(&vfs, &cx);
        let before_header = *wal.header();
        let before_size = wal.file().file_size(&cx).expect("WAL size");
        let mut target = Target::new(&vfs, &cx);
        fault_hooks::arm_sync_failure(FaultHookArm::new(
            "checkpoint-durability",
            format!("wal-sync-{mode:?}"),
            "wal-before-database",
        ));

        let error = execute_checkpoint(&cx, &mut wal, mode, state(), &mut target)
            .expect_err("failed WAL sync must stop checkpoint");
        assert!(matches!(error, FrankenError::Io(_)));
        target.assert_untouched(&cx);
        assert_eq!(wal.header(), &before_header);
        assert_eq!(wal.frame_count(), 2);
        assert_eq!(wal.last_fsynced_frame_count(), 0);
        assert_eq!(wal.file().file_size(&cx).expect("WAL size"), before_size);
        assert_eq!(fault_hooks::take_records().len(), 1);
    }
}

#[test]
fn failed_database_write_is_reached_only_after_wal_durability() {
    let lock = FaultInjectionSessionLock::new();
    let _session = lock.lock().expect("own fault hooks");
    for mode in MODES {
        let cx = Cx::default();
        let vfs = MemoryVfs::new();
        let mut wal = populated_wal(&vfs, &cx);
        let before_header = *wal.header();
        let mut target = Target::new(&vfs, &cx);
        target.fail_write = true;

        let error = execute_checkpoint(&cx, &mut wal, mode, state(), &mut target)
            .expect_err("injected database write failure");
        assert!(matches!(error, FrankenError::Io(_)));
        assert_eq!(target.write_attempts, 1);
        assert_eq!(wal.last_fsynced_frame_count(), 2);
        assert_eq!(wal.frame_count(), 2);
        assert_eq!(wal.header(), &before_header);
        assert!(target.written.is_empty());
        assert!(target.truncations.is_empty());
        assert_eq!(target.syncs, 0);
        assert!(target.published.is_empty());
        assert_eq!(target.reset_gates, 0);
        assert_eq!(target.file.file_size(&cx).expect("database size"), 0);
    }
}

#[test]
fn checkpoint_sync_retry_requires_rewriting_failed_frames() {
    let lock = FaultInjectionSessionLock::new();
    let _session = lock.lock().expect("own fault hooks");
    for mode in MODES {
        let cx = Cx::default();
        let vfs = MemoryVfs::new();
        let mut wal = populated_wal(&vfs, &cx);
        let mut target = Target::new(&vfs, &cx);
        fault_hooks::arm_sync_failure(FaultHookArm::new(
            "checkpoint-durability",
            format!("retry-{mode:?}"),
            "failed-sync-rewrite",
        ));
        execute_checkpoint(&cx, &mut wal, mode, state(), &mut target)
            .expect_err("first sync fails");
        target.assert_untouched(&cx);
        assert_eq!(fault_hooks::take_records().len(), 1);

        // The one-shot hook is gone. A successful-looking second fsync must
        // not bless frames whose first writeback failed (fsyncgate).
        execute_checkpoint(&cx, &mut wal, mode, state(), &mut target)
            .expect_err("retry must preserve the WAL's failed-sync fence");
        target.assert_untouched(&cx);
        assert!(fault_hooks::take_records().is_empty());

        wal.rewrite_frames_after_failed_sync(&cx)
            .expect("re-dirty the failed WAL interval");
        let result = execute_checkpoint(&cx, &mut wal, mode, state(), &mut target)
            .expect("rewritten WAL can checkpoint");
        assert_eq!(result.frames_backfilled, 2);
        assert_eq!(target.published, vec![2]);
        target.assert_page(&cx, 1, 0x11);
        target.assert_page(&cx, 2, 0x22);
        let resets = matches!(mode, CheckpointMode::Restart | CheckpointMode::Truncate);
        assert_eq!(result.wal_was_reset, resets);
        assert_eq!(target.reset_gates, usize::from(resets));
        assert_eq!(target.reset_releases, usize::from(resets));
        if !resets {
            assert_eq!(wal.last_fsynced_frame_count(), 2);
        }
    }
}

#[test]
fn reader_blocked_checkpoint_does_not_issue_a_wal_sync() {
    let lock = FaultInjectionSessionLock::new();
    let _session = lock.lock().expect("own fault hooks");
    for mode in MODES {
        let cx = Cx::default();
        let vfs = MemoryVfs::new();
        let mut wal = populated_wal(&vfs, &cx);
        let mut target = Target::new(&vfs, &cx);
        fault_hooks::arm_sync_failure(FaultHookArm::new(
            "checkpoint-durability",
            format!("blocked-{mode:?}"),
            "no-unnecessary-sync",
        ));
        let state = CheckpointState {
            oldest_reader_frame: Some(0),
            ..state()
        };
        let result = execute_checkpoint(&cx, &mut wal, mode, state, &mut target)
            .expect("reader-blocked checkpoint has no database writes");
        assert_eq!(result.frames_backfilled, 0);
        assert!(!result.wal_was_reset);
        assert_eq!(target.write_attempts, 0);
        assert_eq!(target.syncs, 0);
        assert!(target.truncations.is_empty());
        assert_eq!(target.published, vec![0]);
        assert_eq!(target.reset_gates, 0);
        assert_eq!(wal.last_fsynced_frame_count(), 0);
        assert!(fault_hooks::take_records().is_empty());
        wal.durable_sync(&cx, SyncKind::FullDurable)
            .expect_err("checkpoint must not have consumed the sync fault");
        assert_eq!(fault_hooks::take_records().len(), 1);
    }
}

#[test]
fn partial_checkpoint_syncs_the_wal_before_copying_its_reader_safe_prefix() {
    let lock = FaultInjectionSessionLock::new();
    let _session = lock.lock().expect("own fault hooks");
    for mode in MODES {
        let cx = Cx::default();
        let vfs = MemoryVfs::new();
        let mut wal = populated_wal(&vfs, &cx);
        wal.append_frame(&cx, 3, &page(0x33), 3)
            .expect("append later transaction");
        let mut target = Target::new(&vfs, &cx);
        let state = CheckpointState {
            total_frames: 3,
            backfilled_frames: 0,
            oldest_reader_frame: Some(2),
        };
        let result = execute_checkpoint(&cx, &mut wal, mode, state, &mut target)
            .expect("partial checkpoint");
        assert_eq!(result.frames_backfilled, 2);
        assert!(!result.wal_was_reset);
        assert_eq!(wal.frame_count(), 3);
        assert_eq!(wal.last_fsynced_frame_count(), 3);
        assert_eq!(target.published, vec![2]);
        assert_eq!(target.written.len(), 2);
        assert!(target.truncations.is_empty());
        assert_eq!(target.reset_gates, 0);
        target.assert_page(&cx, 1, 0x11);
        target.assert_page(&cx, 2, 0x22);
        assert_eq!(
            target.file.file_size(&cx).expect("database size"),
            2 * u64::from(PAGE_SIZE),
        );
    }
}

#[test]
fn corrupt_wal_source_is_rejected_before_the_durability_barrier() {
    let lock = FaultInjectionSessionLock::new();
    let _session = lock.lock().expect("own fault hooks");
    for mode in MODES {
        let cx = Cx::default();
        let vfs = MemoryVfs::new();
        let mut wal = populated_wal(&vfs, &cx);
        // Change an actual payload byte without updating its rolling checksum.
        let peer = open_wal(&vfs, &cx);
        let offset = wal.frame_offset(0)
            + u64::try_from(crate::checksum::WAL_FRAME_HEADER_SIZE).expect("header size fits u64")
            + 100;
        peer.write(&cx, &[0xFF], offset).expect("corrupt source");
        let mut target = Target::new(&vfs, &cx);
        fault_hooks::arm_sync_failure(FaultHookArm::new(
            "checkpoint-durability",
            format!("corrupt-{mode:?}"),
            "validate-before-sync",
        ));

        let error = execute_checkpoint(&cx, &mut wal, mode, state(), &mut target)
            .expect_err("corrupt source must be rejected");
        assert!(matches!(error, FrankenError::WalCorrupt { .. }));
        target.assert_untouched(&cx);
        assert_eq!(wal.frame_count(), 2);
        assert_eq!(wal.last_fsynced_frame_count(), 0);
        assert!(fault_hooks::take_records().is_empty());
        wal.durable_sync(&cx, SyncKind::FullDurable)
            .expect_err("invalid source must not have consumed the sync fault");
        assert_eq!(fault_hooks::take_records().len(), 1);
    }
}

fn file_bytes(file: &TestFile, cx: &Cx) -> Vec<u8> {
    let len = usize::try_from(file.file_size(cx).expect("file size")).expect("size fits usize");
    let mut bytes = vec![0; len];
    let read = file.read(cx, &mut bytes, 0).expect("read file image");
    assert_eq!(read, len);
    bytes
}

/// Establish a genuine completed checkpoint, then reopen the same database
/// target with fresh observation counters. No synthetic backfill receipt is used.
fn completed_checkpoint(vfs: &MemoryVfs, cx: &Cx) -> (WalFile<TestFile>, Target) {
    let mut wal = populated_wal(vfs, cx);
    let mut target = Target::new(vfs, cx);
    let result = execute_checkpoint(cx, &mut wal, CheckpointMode::Passive, state(), &mut target)
        .expect("initial complete checkpoint");
    assert_eq!(result.frames_backfilled, 2);
    assert!(!result.wal_was_reset);
    assert_eq!(target.published, vec![2]);
    drop(target);
    (wal, Target::new(vfs, cx))
}

#[test]
fn no_copy_checkpoint_rejects_a_replaced_wal_generation_in_every_mode() {
    let lock = FaultInjectionSessionLock::new();
    let _session = lock.lock().expect("own fault hooks");
    for mode in MODES {
        let cx = Cx::default();
        let vfs = MemoryVfs::new();
        let (mut wal, mut target) = completed_checkpoint(&vfs, &cx);
        let old_header = *wal.header();
        let database_before = file_bytes(&target.file, &cx);

        // A second real handle replaces the physical WAL while the first
        // retains its old two-frame view. Equal counts must not authorize
        // publishing an old receipt or erasing this newer generation.
        let mut peer = WalFile::create(
            &cx,
            open_wal(&vfs, &cx),
            PAGE_SIZE,
            17,
            WalSalts {
                salt1: 0xAAAA_5555,
                salt2: 0x5555_AAAA,
            },
        )
        .expect("replace physical WAL generation");
        peer.append_frame(&cx, 1, &page(0xA1), 0)
            .expect("peer first page");
        peer.append_frame(&cx, 2, &page(0xA2), 2)
            .expect("peer commit");
        peer.durable_sync(&cx, SyncKind::FullDurable)
            .expect("make peer transaction durable");
        let wal_before = file_bytes(peer.file(), &cx);
        assert_eq!(wal.frame_count(), peer.frame_count());

        let state = CheckpointState {
            backfilled_frames: 2,
            ..state()
        };
        let error = execute_checkpoint(&cx, &mut wal, mode, state, &mut target)
            .expect_err("no-copy checkpoint must validate its physical generation");
        assert!(matches!(error, FrankenError::WalCorrupt { .. }));
        target.assert_no_calls();
        assert_eq!(wal.header(), &old_header);
        assert_eq!(wal.frame_count(), 2);
        assert_eq!(file_bytes(peer.file(), &cx), wal_before);
        assert_eq!(file_bytes(&target.file, &cx), database_before);
    }
}

#[test]
fn no_copy_checkpoint_rejects_a_corrupt_or_short_physical_header() {
    let lock = FaultInjectionSessionLock::new();
    let _session = lock.lock().expect("own fault hooks");
    for mode in MODES {
        // None corrupts a checksum byte. The other cases truncate the header.
        for short_len in [None, Some(0_u64), Some(8), Some(31)] {
            let cx = Cx::default();
            let vfs = MemoryVfs::new();
            let (mut wal, mut target) = completed_checkpoint(&vfs, &cx);
            let before_header = *wal.header();
            let database_before = file_bytes(&target.file, &cx);
            let mut peer = open_wal(&vfs, &cx);
            if let Some(len) = short_len {
                peer.truncate(&cx, len).expect("truncate WAL header");
            } else {
                let old = file_bytes(&peer, &cx)[24];
                peer.write(&cx, &[old ^ 1], 24)
                    .expect("damage header checksum");
            }
            let wal_before = file_bytes(&peer, &cx);
            let state = CheckpointState {
                backfilled_frames: 2,
                ..state()
            };

            let error = execute_checkpoint(&cx, &mut wal, mode, state, &mut target)
                .expect_err("invalid physical header must block a no-copy checkpoint");
            assert!(matches!(error, FrankenError::WalCorrupt { .. }));
            target.assert_no_calls();
            assert_eq!(wal.header(), &before_header);
            assert_eq!(wal.frame_count(), 2);
            assert_eq!(file_bytes(&peer, &cx), wal_before);
            assert_eq!(file_bytes(&target.file, &cx), database_before);
        }
    }
}

#[test]
fn reader_blocked_checkpoint_validates_its_generation_before_publication() {
    let lock = FaultInjectionSessionLock::new();
    let _session = lock.lock().expect("own fault hooks");
    for mode in MODES {
        let cx = Cx::default();
        let vfs = MemoryVfs::new();
        let mut wal = populated_wal(&vfs, &cx);
        let mut peer = WalFile::create(
            &cx,
            open_wal(&vfs, &cx),
            PAGE_SIZE,
            9,
            WalSalts {
                salt1: 0x1111_2222,
                salt2: 0x3333_4444,
            },
        )
        .expect("replace WAL generation");
        peer.append_frame(&cx, 1, &page(0xAA), 1)
            .expect("peer commit");
        let wal_before = file_bytes(peer.file(), &cx);
        let mut target = Target::new(&vfs, &cx);
        let state = CheckpointState {
            oldest_reader_frame: Some(0),
            ..state()
        };

        let error = execute_checkpoint(&cx, &mut wal, mode, state, &mut target)
            .expect_err("reader-blocked path must not publish a stale-generation prefix");
        assert!(matches!(error, FrankenError::WalCorrupt { .. }));
        target.assert_untouched(&cx);
        assert_eq!(file_bytes(peer.file(), &cx), wal_before);
    }
}

#[test]
fn no_copy_checkpoint_accepts_its_unchanged_generation() {
    let lock = FaultInjectionSessionLock::new();
    let _session = lock.lock().expect("own fault hooks");
    for mode in MODES {
        let cx = Cx::default();
        let vfs = MemoryVfs::new();
        let (mut wal, mut target) = completed_checkpoint(&vfs, &cx);
        let database_before = file_bytes(&target.file, &cx);
        let state = CheckpointState {
            backfilled_frames: 2,
            ..state()
        };
        let result = execute_checkpoint(&cx, &mut wal, mode, state, &mut target)
            .expect("a valid already-backfilled generation remains usable");
        assert_eq!(result.frames_backfilled, 0);
        assert_eq!(target.write_attempts, 0);
        assert!(target.truncations.is_empty());
        assert_eq!(target.published, vec![2]);
        assert_eq!(file_bytes(&target.file, &cx), database_before);
        let resets = matches!(mode, CheckpointMode::Restart | CheckpointMode::Truncate);
        assert_eq!(result.wal_was_reset, resets);
        assert_eq!(target.reset_gates, usize::from(resets));
        assert_eq!(target.reset_releases, usize::from(resets));
    }
}
