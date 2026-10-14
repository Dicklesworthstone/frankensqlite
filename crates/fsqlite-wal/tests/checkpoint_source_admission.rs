//! Exercise the public checkpoint entry point, including plans with no new
//! backfill. These are deterministic independent-handle races, not a claim
//! that checkpoint source checks replace the caller's coordination locks.
#![cfg(not(target_arch = "wasm32"))]

use std::collections::BTreeMap;
use std::future::Future;
use std::path::Path;

use asupersync::runtime::RuntimeBuilder;
use fsqlite_error::FrankenError;
use fsqlite_types::PageNumber;
use fsqlite_types::cx::Cx;
use fsqlite_types::flags::VfsOpenFlags;
use fsqlite_vfs::{MemoryVfs, Vfs, VfsFile};
use fsqlite_wal::checkpoint_executor::CheckpointTargetFuture;
use fsqlite_wal::{
    CheckpointMode, CheckpointPostAction, CheckpointState, CheckpointTarget,
    WAL_HEADER_SIZE, WalFile, WalHeader, WalSalts, execute_checkpoint,
};

type TestFile = <MemoryVfs as Vfs>::File;
type TestWal = WalFile<TestFile>;
const PAGE_SIZE: u32 = 512;
const MODES: [CheckpointMode; 4] = [
    CheckpointMode::Passive, CheckpointMode::Full,
    CheckpointMode::Restart, CheckpointMode::Truncate,
];

fn run(future: impl Future<Output = ()>) {
    RuntimeBuilder::current_thread().blocking_threads(1, 2).build()
        .expect("test runtime").block_on(future);
}

fn open_file(vfs: &MemoryVfs, cx: &Cx) -> TestFile {
    let flags = VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE | VfsOpenFlags::WAL;
    vfs.open(cx, Some(Path::new("admission.db-wal")), flags).expect("open WAL").0
}

async fn create(vfs: &MemoryVfs, cx: &Cx) -> TestWal {
    WalFile::create(cx, open_file(vfs, cx), PAGE_SIZE, 0,
        WalSalts { salt1: 123, salt2: 456 }).await.expect("create WAL")
}

async fn append(wal: &mut TestWal, cx: &Cx, seed: u8, committed: bool) {
    let page = vec![seed; usize::try_from(PAGE_SIZE).expect("page size")];
    wal.append_frame(cx, 1, &page, u32::from(committed)).await.expect("append");
}

async fn bytes(cx: &Cx, file: &TestFile) -> Vec<u8> {
    let len = usize::try_from(file.file_size(cx).expect("size")).expect("usize size");
    let mut result = vec![0; len];
    assert_eq!(file.read(cx, &mut result, 0).await.expect("read"), len);
    result
}

fn state(total_frames: u32, backfilled_frames: u32) -> CheckpointState {
    CheckpointState { total_frames, backfilled_frames, oldest_reader_frame: None }
}

#[derive(Default)]
struct Target {
    calls: usize,
    pages: BTreeMap<u32, Vec<u8>>,
}

impl CheckpointTarget for Target {
    fn write_page<'a>(&'a mut self, _: &'a Cx, page: PageNumber, data: &'a [u8])
        -> CheckpointTargetFuture<'a, ()>
    {
        Box::pin(async move {
            self.calls += 1;
            self.pages.insert(page.get(), data.to_vec());
            Ok(())
        })
    }

    fn truncate_db<'a>(&'a mut self, _: &'a Cx, count: u32) -> CheckpointTargetFuture<'a, ()> {
        Box::pin(async move {
            self.calls += 1;
            self.pages.retain(|page, _| *page <= count);
            Ok(())
        })
    }

    fn sync_db<'a>(&'a mut self, _: &'a Cx) -> CheckpointTargetFuture<'a, ()> {
        Box::pin(async move { self.calls += 1; Ok(()) })
    }

    fn read_page_if_supported<'a>(&'a mut self, _: &'a Cx, page: PageNumber, buf: &'a mut [u8])
        -> CheckpointTargetFuture<'a, Option<usize>>
    {
        Box::pin(async move {
            self.calls += 1;
            let data = self.pages.get(&page.get()).expect("written page");
            buf.copy_from_slice(data);
            Ok(Some(data.len()))
        })
    }

    fn acquire_wal_reset_gate<'a>(&'a mut self, _: &'a Cx) -> CheckpointTargetFuture<'a, bool> {
        Box::pin(async move { self.calls += 1; Ok(true) })
    }

    fn publish_backfill<'a>(&'a mut self, _: &'a Cx, _: &'a WalHeader, _: u32)
        -> CheckpointTargetFuture<'a, ()>
    {
        Box::pin(async move { self.calls += 1; Ok(()) })
    }
}

async fn refused(cx: &Cx, wal: &mut TestWal, mode: CheckpointMode, snapshot: CheckpointState) {
    let original = bytes(cx, wal.file()).await;
    let count = wal.frame_count();
    let checksum = wal.running_checksum();
    let generation = wal.generation_identity();
    let mut target = Target::default();
    let error = execute_checkpoint(cx, wal, mode, snapshot, &mut target)
        .await.expect_err("stale or corrupt snapshot must be rejected");
    assert!(matches!(error,
        FrankenError::CheckpointFailed { .. } | FrankenError::WalCorrupt { .. }
        | FrankenError::BusyRecovery), "{error:?}");
    assert_eq!(target.calls, 0, "admission must precede EVERY target callback");
    assert_eq!(bytes(cx, wal.file()).await, original, "no WAL mutation");
    assert_eq!(wal.frame_count(), count, "no implicit refresh");
    assert_eq!(wal.running_checksum(), checksum);
    assert_eq!(wal.generation_identity(), generation);
}

#[test]
fn cross_handle_commit_growth_is_rejected_even_with_zero_backfill() {
    run(async {
        for mode in MODES {
            for (initial, backfilled) in [(0, 0), (1, 0), (1, 1)] {
                let cx = Cx::default();
                let vfs = MemoryVfs::new();
                let mut writer = create(&vfs, &cx).await;
                if initial > 0 { append(&mut writer, &cx, 1, true).await; }
                let mut stale = WalFile::open(&cx, open_file(&vfs, &cx)).await.expect("reader");
                // Both counters still agree. Only the physical file grows.
                append(&mut writer, &cx, 2, false).await;
                append(&mut writer, &cx, 3, true).await;
                refused(&cx, &mut stale, mode, state(initial, backfilled)).await;

                stale.refresh(&cx).await.expect("explicit refresh");
                let total = u32::try_from(stale.frame_count()).expect("frame count");
                let mut target = Target::default();
                execute_checkpoint(&cx, &mut stale, mode, state(total, 0), &mut target)
                    .await.expect("freshly planned checkpoint");
                assert_eq!(target.pages.get(&1).expect("latest page"),
                    &vec![3; usize::try_from(PAGE_SIZE).expect("page size")]);
            }
        }
    });
}

#[test]
fn zero_backfill_rejects_replaced_generation_at_same_file_length() {
    run(async {
        for mode in MODES {
            let cx = Cx::default();
            let vfs = MemoryVfs::new();
            let mut writer = create(&vfs, &cx).await;
            append(&mut writer, &cx, 1, true).await;
            let mut stale = WalFile::open(&cx, open_file(&vfs, &cx)).await.expect("reader");
            writer.reset(&cx, 1, WalSalts { salt1: 789, salt2: 321 }, false)
                .await.expect("reset");
            append(&mut writer, &cx, 2, true).await;
            refused(&cx, &mut stale, mode, state(1, 1)).await;
        }
    });
}

#[test]
fn zero_backfill_rejects_torn_header_short_file_and_corrupt_terminal_payload() {
    run(async {
        for mode in MODES {
            for damage in 0..3 {
                let cx = Cx::default();
                let vfs = MemoryVfs::new();
                let mut wal = create(&vfs, &cx).await;
                append(&mut wal, &cx, 1, true).await;
                let mut file = open_file(&vfs, &cx);
                let original = bytes(&cx, &file).await;
                if damage == 1 {
                    file.truncate(&cx, 32).expect("truncate accepted frame");
                } else {
                    let offset = if damage == 0 { 24 } else { original.len() - 1 };
                    file.write(&cx, &[original[offset] ^ 1], u64::try_from(offset).expect("offset"))
                        .await.expect("damage");
                }
                refused(&cx, &mut wal, mode, state(1, 1)).await;
            }
        }
    });
}

#[test]
fn recovery_residue_is_not_mistaken_for_another_commit() {
    run(async {
        // Valid uncommitted, partial, and checksum-invalid suffixes all leave
        // recovery at the original committed prefix, not the physical length.
        for residue in 0..3 {
            let cx = Cx::default();
            let vfs = MemoryVfs::new();
            let mut writer = create(&vfs, &cx).await;
            append(&mut writer, &cx, 1, true).await;
            append(&mut writer, &cx, 2, residue == 2).await;
            let mut file = open_file(&vfs, &cx);
            let original = bytes(&cx, &file).await;
            if residue == 1 {
                file.truncate(&cx, u64::try_from(original.len() - 1).expect("length"))
                    .expect("partial frame");
            } else if residue == 2 {
                let offset = original.len() - 1;
                file.write(&cx, &[original[offset] ^ 1], u64::try_from(offset).expect("offset"))
                    .await.expect("invalid checksum");
            }
            let mut recovered = WalFile::open(&cx, open_file(&vfs, &cx)).await.expect("recover");
            assert_eq!(recovered.frame_count(), 1);
            let mut target = Target::default();
            execute_checkpoint(&cx, &mut recovered, CheckpointMode::Truncate,
                state(1, 0), &mut target).await.expect("checkpoint recovered prefix");
            assert_eq!(target.pages.get(&1).expect("committed page"),
                &vec![1; usize::try_from(PAGE_SIZE).expect("page size")]);
            assert_eq!(recovered.file().file_size(&cx).expect("size"),
                u64::try_from(WAL_HEADER_SIZE).expect("header size"));
        }
    });
}

#[test]
fn truncate_accepts_old_generation_residue_behind_an_empty_header() {
    run(async {
        let cx = Cx::default();
        let vfs = MemoryVfs::new();
        let mut wal = create(&vfs, &cx).await;
        append(&mut wal, &cx, 1, true).await;
        wal.reset(&cx, 1, WalSalts { salt1: 789, salt2: 321 }, false).await.expect("reset");
        assert_eq!(wal.frame_count(), 0);
        assert!(wal.file().file_size(&cx).expect("size") > 32);
        let mut target = Target::default();
        let result = execute_checkpoint(&cx, &mut wal, CheckpointMode::Truncate,
            state(0, 0), &mut target).await.expect("truncate old bytes");
        assert!(result.wal_was_reset);
        assert_eq!(wal.file().file_size(&cx).expect("size"), 32);
    });
}

#[test]
fn already_empty_headerless_wal_is_a_true_noop_in_every_mode() {
    run(async {
        for mode in MODES {
            let cx = Cx::default();
            let vfs = MemoryVfs::new();
            let mut wal = WalFile::open_headerless(open_file(&vfs, &cx), PAGE_SIZE);
            let generation = wal.generation_identity();
            let mut target = Target::default();
            let result = execute_checkpoint(&cx, &mut wal, mode, state(0, 0), &mut target)
                .await.expect("empty no-op");
            assert_eq!(target.calls, 0);
            assert_eq!(result.frames_backfilled, 0);
            assert_eq!(result.plan.post_action, CheckpointPostAction::None);
            assert!(!result.wal_was_reset);
            assert!(!result.reset_deferred_by_readers());
            assert!(wal.is_headerless());
            assert_eq!(wal.generation_identity(), generation);
            assert_eq!(wal.file().file_size(&cx).expect("size"), 0);
        }
    });
}

#[test]
fn headerless_observation_cannot_authorize_a_later_generation() {
    run(async {
        for mode in MODES {
            let cx = Cx::default();
            let vfs = MemoryVfs::new();
            let mut stale = WalFile::open_headerless(open_file(&vfs, &cx), PAGE_SIZE);
            let mut writer = create(&vfs, &cx).await;
            append(&mut writer, &cx, 1, true).await;
            refused(&cx, &mut stale, mode, state(0, 0)).await;
        }
    });
}
