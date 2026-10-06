#![allow(clippy::future_not_send)] // These engine futures run on the caller's current-thread runtime.
//! Existing sealed pager/B-tree consumers over native storage. These tests do
//! not select native SQL or certify physical power-loss behavior.
use std::future::Future;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};

use asupersync::runtime::RuntimeBuilder;
use fsqlite_btree::{BtCursor, BtreeCursorOps, SeekResult, TransactionPageIo};
use fsqlite_error::{FrankenError, Result};
use fsqlite_harness::fault_vfs::{FaultInjectingVfs, FaultSpec};
use fsqlite_pager::native::NativePager;
use fsqlite_pager::{PagerCommitState, TransactionHandle, TransactionMode};
use fsqlite_types::cx::Cx;
use fsqlite_types::flags::{SyncFlags, VfsOpenFlags};
use fsqlite_types::{
    CommitSeq, LockLevel, ObjectId, Oti, PageNumber, SymbolRecord, SymbolRecordFlags,
    reconstruct_systematic_happy_path,
};
use fsqlite_vfs::{
    FileIdentity, MemoryVfs, ShmRegion, Vfs, VfsFile, VfsWriteCompletion, VfsWriteCompletionState,
};
use fsqlite_wal::native_commit::durable::NativeObjectCodec;
use fsqlite_wal::native_durability::{NativeDurabilityLimits, NativeDurabilityLog};
use fsqlite_wal::native_pages::{NativePageLimits, NativePageStore};

struct TestCodec;
impl NativeObjectCodec for TestCodec {
    fn encode(&self, _: &Cx, bytes: &[u8]) -> Result<Vec<SymbolRecord>> {
        let size = u32::try_from(bytes.len()).map_err(|_| FrankenError::TooBig)?;
        Ok(vec![SymbolRecord::new(
            ObjectId::derive_from_canonical_bytes(bytes),
            Oti {
                f: u64::from(size),
                al: 1,
                t: size,
                z: 1,
                n: 1,
            },
            0,
            bytes.to_vec(),
            SymbolRecordFlags::SYSTEMATIC_RUN_START,
        )])
    }
    fn decode(&self, _: &Cx, id: ObjectId, records: &[SymbolRecord]) -> Result<Vec<u8>> {
        let bytes = reconstruct_systematic_happy_path(records)
            .map_err(|error| FrankenError::Internal(error.to_string()))?;
        if ObjectId::derive_from_canonical_bytes(&bytes) != id {
            return Err(FrankenError::Abort);
        }
        Ok(bytes)
    }
}
fn run(future: impl Future<Output = ()>) {
    RuntimeBuilder::current_thread()
        .blocking_threads(1, 2)
        .build()
        .unwrap()
        .block_on(future);
}
fn page(number: u32) -> PageNumber {
    PageNumber::new(number).unwrap()
}
fn open<V: Vfs>(vfs: &V, cx: &Cx, path: &str) -> V::File {
    vfs.open(
        cx,
        Some(Path::new(path)),
        VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE | VfsOpenFlags::WAL,
    )
    .unwrap()
    .0
}
fn pager<V: Vfs>(vfs: &V, cx: &Cx) -> NativePager<V::File, V::File, TestCodec> {
    let log = NativeDurabilityLog::create(
        cx,
        open(vfs, cx, "objects"),
        open(vfs, cx, "markers"),
        NativeDurabilityLimits::default(),
    )
    .unwrap();
    NativePager::new(
        NativePageStore::new(log, TestCodec, 512, NativePageLimits::default()).unwrap(),
    )
    .unwrap()
}
async fn recovered<V: Vfs>(vfs: &V, cx: &Cx) -> (NativePager<V::File, V::File, TestCodec>, usize) {
    let (store, report) = NativePageStore::recover(
        cx,
        open(vfs, cx, "objects"),
        open(vfs, cx, "markers"),
        TestCodec,
        512,
        NativeDurabilityLimits::default(),
        NativePageLimits::default(),
    )
    .await
    .unwrap();
    (NativePager::new(store).unwrap(), report.markers.len())
}
async fn root<T: TransactionHandle>(txn: &mut T, cx: &Cx, table: bool) -> PageNumber {
    let root = txn.allocate_page(cx).await.unwrap();
    let size = usize::try_from(txn.page_size().get()).unwrap();
    let mut bytes = vec![0; size];
    bytes[0] = if table { 0x0D } else { 0x0A };
    bytes[5..7].copy_from_slice(&u16::try_from(size).unwrap().to_be_bytes());
    txn.write_page(cx, root, &bytes).await.unwrap();
    root
}
async fn rows<T: TransactionHandle>(txn: &mut T, cx: &Cx, root: PageNumber) -> Vec<(i64, Vec<u8>)> {
    let size = txn.page_size().get();
    let mut cursor = BtCursor::new(TransactionPageIo::new(txn), root, size, true);
    let mut rows = Vec::new();
    if cursor.first(cx).await.unwrap() {
        loop {
            rows.push((
                cursor.rowid(cx).await.unwrap(),
                cursor.payload(cx).await.unwrap(),
            ));
            if !cursor.next(cx).await.unwrap() {
                break;
            }
        }
    }
    rows
}

#[test]
fn ordinary_transaction_page_io_splits_overflows_deletes_and_reopens_natively() {
    run(async {
        let cx = Cx::new();
        let vfs = MemoryVfs::new();
        let owner = pager(&vfs, &cx);
        let mut txn = owner.begin(&cx, TransactionMode::Concurrent).unwrap();
        let root = root(&mut txn, &cx, true).await;
        let overflow = vec![0xFE; 4097];
        {
            let mut cursor = BtCursor::new(TransactionPageIo::new(&mut txn), root, 512, true);
            for row in 0_u8..120 {
                cursor
                    .table_insert(&cx, i64::from(row), &vec![row; 80])
                    .await
                    .unwrap();
            }
            cursor.table_insert(&cx, 200, &overflow).await.unwrap();
        }
        assert_eq!(txn.get_page(&cx, root).await.unwrap().as_bytes()[0], 0x05);
        assert!(txn.pending_commit_pages().unwrap().len() > 2);
        let expected = rows(&mut txn, &cx, root).await;
        assert_eq!(expected.len(), 121);
        assert_eq!(expected.last().unwrap(), &(200, overflow));
        txn.commit_at(&cx, 100).await.unwrap();
        owner.close(&cx).unwrap();
        let (owner, count) = recovered(&vfs, &cx).await;
        assert_eq!(count, 1);
        let mut old = owner.begin(&cx, TransactionMode::ReadOnly).unwrap();
        let mut delete = owner.begin(&cx, TransactionMode::Concurrent).unwrap();
        assert_eq!(rows(&mut delete, &cx, root).await, expected);
        {
            let mut cursor = BtCursor::new(TransactionPageIo::new(&mut delete), root, 512, true);
            for row in (0_i64..120).step_by(3).chain(std::iter::once(200)) {
                assert_eq!(
                    cursor.table_move_to(&cx, row).await.unwrap(),
                    SeekResult::Found
                );
                cursor.delete(&cx).await.unwrap();
            }
        }
        delete.commit_at(&cx, 101).await.unwrap();
        assert_eq!(rows(&mut old, &cx, root).await, expected);
        old.rollback(&cx).await.unwrap();
        owner.close(&cx).unwrap();
        let (owner, count) = recovered(&vfs, &cx).await;
        assert_eq!(count, 2);
        let mut fresh = owner.begin(&cx, TransactionMode::ReadOnly).unwrap();
        let expected: Vec<_> = expected
            .into_iter()
            .filter(|(row, _)| *row != 200 && row % 3 != 0)
            .collect();
        assert_eq!(rows(&mut fresh, &cx, root).await, expected);
        fresh.rollback(&cx).await.unwrap();
        owner.close(&cx).unwrap();
    });
}

#[test]
fn sealed_savepoint_restores_table_and_index_after_real_unique_failure() {
    run(async {
        let cx = Cx::new();
        let vfs = MemoryVfs::new();
        let owner = pager(&vfs, &cx);
        let mut txn = owner.begin(&cx, TransactionMode::Concurrent).unwrap();
        let table = root(&mut txn, &cx, true).await;
        let index = root(&mut txn, &cx, false).await;
        {
            let mut cursor = BtCursor::new(TransactionPageIo::new(&mut txn), table, 512, true);
            cursor.table_insert(&cx, 1, b"keep").await.unwrap();
        }
        {
            let mut cursor = BtCursor::new(TransactionPageIo::new(&mut txn), index, 512, false);
            cursor
                .index_insert_unique(&cx, &[3, 1, 1, 7, 1], 1, "unique_key")
                .await
                .unwrap();
        }
        txn.savepoint(&cx, "statement").unwrap();
        let before = txn.pending_commit_pages().unwrap();
        {
            let mut cursor = BtCursor::new(TransactionPageIo::new(&mut txn), table, 512, true);
            cursor
                .table_insert(&cx, 2, b"must disappear")
                .await
                .unwrap();
        }
        {
            let mut cursor = BtCursor::new(TransactionPageIo::new(&mut txn), index, 512, false);
            assert!(matches!(
                cursor
                    .index_insert_unique(&cx, &[3, 1, 1, 7, 2], 1, "unique_key")
                    .await,
                Err(FrankenError::UniqueViolation { .. })
            ));
        }
        txn.rollback_to_savepoint(&cx, "statement").unwrap();
        txn.release_savepoint(&cx, "statement").unwrap();
        assert_eq!(txn.pending_commit_pages().unwrap(), before);
        assert_eq!(
            rows(&mut txn, &cx, table).await,
            vec![(1, b"keep".to_vec())]
        );
        txn.commit_at(&cx, 100).await.unwrap();
        owner.close(&cx).unwrap();
        let (owner, _) = recovered(&vfs, &cx).await;
        let mut txn = owner.begin(&cx, TransactionMode::ReadOnly).unwrap();
        assert_eq!(
            rows(&mut txn, &cx, table).await,
            vec![(1, b"keep".to_vec())]
        );
        txn.rollback(&cx).await.unwrap();
        owner.close(&cx).unwrap();
    });
}

#[test]
fn real_threads_prepare_disjoint_writes_before_either_is_published() {
    let vfs = MemoryVfs::new();
    let cx = Cx::new();
    let owner = pager(&vfs, &cx);
    // Bind both snapshots before starting either thread. Beginning is a short
    // exclusive admission; this test measures overlapping private preparation,
    // not admission retry scheduling or parallel physical publication.
    let mut a_txn = owner.begin(&cx, TransactionMode::Concurrent).unwrap();
    let mut b_txn = owner.begin(&cx, TransactionMode::Concurrent).unwrap();
    let prepared = Arc::new(std::sync::Barrier::new(3));
    let (first_done, first_completed) = std::sync::mpsc::channel();
    let a_barrier = Arc::clone(&prepared);
    let a = std::thread::spawn(move || {
        run(async {
            let cx = Cx::new();
            let staged = a_txn.write_page(&cx, page(2), &[2; 512]).await;
            a_barrier.wait();
            staged.unwrap();
            a_txn.commit_at(&cx, 100).await.unwrap();
            first_done.send(()).unwrap();
        })
    });
    let b_barrier = Arc::clone(&prepared);
    let b = std::thread::spawn(move || {
        run(async {
            let cx = Cx::new();
            let staged = b_txn.write_page(&cx, page(3), &[3; 512]).await;
            b_barrier.wait();
            staged.unwrap();
            first_completed.recv().unwrap();
            assert_eq!(
                b_txn.published_visible_commit_seq_hint(),
                Some(CommitSeq::ZERO)
            );
            b_txn.commit_at(&cx, 101).await.unwrap();
        })
    });
    prepared.wait();
    a.join().unwrap();
    b.join().unwrap();
    run(async {
        let mut view = owner.begin(&cx, TransactionMode::ReadOnly).unwrap();
        assert_eq!(
            view.get_page(&cx, page(2)).await.unwrap().as_bytes(),
            &[2; 512]
        );
        assert_eq!(
            view.get_page(&cx, page(3)).await.unwrap().as_bytes(),
            &[3; 512]
        );
        view.rollback(&cx).await.unwrap();
        owner.close(&cx).unwrap();
    });
}

#[test]
fn failed_syncs_remain_in_doubt_and_recovery_never_invents_an_old_reply() {
    run(async {
        for (path, ordinal, expected) in [("objects", 1, 0), ("markers", 2, 1)] {
            let cx = Cx::new();
            let vfs = FaultInjectingVfs::new(MemoryVfs::new());
            let owner = pager(&vfs, &cx);
            let mut txn = owner.begin(&cx, TransactionMode::Concurrent).unwrap();
            txn.write_page(&cx, page(2), &[9; 512]).await.unwrap();
            vfs.inject_fault(FaultSpec::power_cut(path).after_nth_sync(ordinal).build());
            assert!(txn.commit_at(&cx, 100).await.is_err());
            assert!(vfs.is_powered_off());
            assert_eq!(txn.pager_commit_state(), PagerCommitState::InDoubt);
            assert!(txn.acknowledgement().is_none());
            assert!(matches!(
                txn.settle_commit(&cx).await,
                Err(FrankenError::BusyRecovery)
            ));
            vfs.power_on();
            assert!(txn.rollback(&cx).await.is_err());
            assert!(txn.commit_at(&cx, 101).await.is_err());
            assert_eq!(txn.pager_commit_state(), PagerCommitState::InDoubt);
            assert!(owner.needs_recovery().unwrap());
            assert!(owner.begin(&cx, TransactionMode::Concurrent).is_err());
            drop(txn);
            owner.close(&cx).unwrap();
            let (fresh, count) = recovered(&vfs, &cx).await;
            assert_eq!(count, expected);
            let mut view = fresh.begin(&cx, TransactionMode::ReadOnly).unwrap();
            assert!(view.acknowledgement().is_none());
            if expected == 1 {
                assert_eq!(
                    view.get_page(&cx, page(2)).await.unwrap().as_bytes(),
                    &[9; 512]
                );
            } else {
                assert!(view.get_page(&cx, page(2)).await.is_err());
            }
            view.rollback(&cx).await.unwrap();
            fresh.close(&cx).unwrap();
        }
    });
}

// Deterministic source-completion pause: write the bytes, retain the source's
// completion token externally, and suspend before reporting completion. This
// tests ownership, not hardware persistence or a fabricated background worker.
type MemoryFile = <MemoryVfs as Vfs>::File;
struct PausedFile {
    inner: MemoryFile,
    pause: bool,
    source: Arc<Mutex<Option<VfsWriteCompletion>>>,
}
impl VfsFile for PausedFile {
    fn close(&mut self, cx: &Cx) -> Result<()> {
        self.inner.close(cx)
    }
    fn file_identity(&self) -> Result<Option<FileIdentity>> {
        self.inner.file_identity()
    }
    fn refresh_file_identity(&self) -> Result<Option<FileIdentity>> {
        self.inner.refresh_file_identity()
    }
    async fn read<'a>(&'a self, cx: &'a Cx, buf: &'a mut [u8], offset: u64) -> Result<usize> {
        self.inner.read(cx, buf, offset).await
    }
    async fn write<'a>(&'a self, cx: &'a Cx, buf: &'a [u8], offset: u64) -> Result<()> {
        self.inner.write(cx, buf, offset).await
    }
    async fn write_tracked<'a>(
        &'a self,
        cx: &'a Cx,
        buf: &'a [u8],
        offset: u64,
        completion: VfsWriteCompletion,
    ) -> Result<()> {
        if !self.pause {
            return self.inner.write_tracked(cx, buf, offset, completion).await;
        }
        self.inner.write(cx, buf, offset).await?;
        *self.source.lock().unwrap() = Some(completion);
        std::future::pending::<Result<()>>().await
    }
    fn truncate(&mut self, cx: &Cx, size: u64) -> Result<()> {
        self.inner.truncate(cx, size)
    }
    fn sync(&mut self, cx: &Cx, flags: SyncFlags) -> Result<()> {
        self.inner.sync(cx, flags)
    }
    fn file_size(&self, cx: &Cx) -> Result<u64> {
        self.inner.file_size(cx)
    }
    fn lock(&mut self, cx: &Cx, level: LockLevel) -> Result<()> {
        self.inner.lock(cx, level)
    }
    fn unlock(&mut self, cx: &Cx, level: LockLevel) -> Result<()> {
        self.inner.unlock(cx, level)
    }
    fn lock_external_wal_append(&mut self, cx: &Cx) -> Result<()> {
        self.inner.lock_external_wal_append(cx)
    }
    fn owns_external_wal_append_write(&self, cx: &Cx) -> Result<bool> {
        self.inner.owns_external_wal_append_write(cx)
    }
    fn restore_external_wal_append_attempt(&mut self, cx: &Cx) -> Result<()> {
        self.inner.restore_external_wal_append_attempt(cx)
    }
    fn lock_external_shared_snapshot(&mut self, cx: &Cx) -> Result<()> {
        self.inner.lock_external_shared_snapshot(cx)
    }
    fn restore_external_shared_snapshot_attempt(&mut self, cx: &Cx) -> Result<()> {
        self.inner.restore_external_shared_snapshot_attempt(cx)
    }
    fn lock_external_maintenance(&mut self, cx: &Cx, wal: bool) -> Result<()> {
        self.inner.lock_external_maintenance(cx, wal)
    }
    fn lock_external_wal_recovery(&mut self, cx: &Cx) -> Result<()> {
        self.inner.lock_external_wal_recovery(cx)
    }
    fn restore_external_maintenance_attempt(&mut self, cx: &Cx) -> Result<()> {
        self.inner.restore_external_maintenance_attempt(cx)
    }
    fn check_reserved_lock(&self, cx: &Cx) -> Result<bool> {
        self.inner.check_reserved_lock(cx)
    }
    fn shm_map(&mut self, cx: &Cx, region: u32, size: u32, extend: bool) -> Result<ShmRegion> {
        self.inner.shm_map(cx, region, size, extend)
    }
    fn shm_lock(&mut self, cx: &Cx, offset: u32, n: u32, flags: u32) -> Result<()> {
        self.inner.shm_lock(cx, offset, n, flags)
    }
    fn shm_barrier(&self) {
        self.inner.shm_barrier();
    }
    fn shm_unmap(&mut self, cx: &Cx, delete: bool) -> Result<()> {
        self.inner.shm_unmap(cx, delete)
    }
}
struct NoopWake;
impl Wake for NoopWake {
    fn wake(self: Arc<Self>) {}
}

#[test]
fn dropped_commit_retains_source_completion_and_refuses_premature_close() {
    run(async {
        let cx = Cx::new();
        let vfs = MemoryVfs::new();
        let source = Arc::new(Mutex::new(None));
        let log = NativeDurabilityLog::create(
            &cx,
            PausedFile {
                inner: open(&vfs, &cx, "objects"),
                pause: false,
                source: Arc::clone(&source),
            },
            PausedFile {
                inner: open(&vfs, &cx, "markers"),
                pause: true,
                source: Arc::clone(&source),
            },
            NativeDurabilityLimits::default(),
        )
        .unwrap();
        let owner = NativePager::new(
            NativePageStore::new(log, TestCodec, 512, NativePageLimits::default()).unwrap(),
        )
        .unwrap();
        let mut txn = owner.begin(&cx, TransactionMode::Concurrent).unwrap();
        txn.write_page(&cx, page(2), &[6; 512]).await.unwrap();
        drop(txn.commit_at(&cx, 99)); // An unpolled future has performed no I/O.
        assert_eq!(txn.pager_commit_state(), PagerCommitState::NotCommitted);
        {
            let mut commit = Box::pin(txn.commit_at(&cx, 100));
            let waker = Waker::from(Arc::new(NoopWake));
            let mut context = Context::from_waker(&waker);
            assert!(matches!(commit.as_mut().poll(&mut context), Poll::Pending));
            assert!(
                source.lock().unwrap().is_some(),
                "must abandon after the source wrote marker bytes"
            );
            drop(commit);
        }
        assert_eq!(txn.pager_commit_state(), PagerCommitState::InDoubt);
        assert_eq!(owner.committed_tip().unwrap(), CommitSeq::ZERO);
        let completion = owner.outstanding_write().unwrap().unwrap();
        assert_eq!(completion.state(), VfsWriteCompletionState::Pending);
        let mut markers = open(&vfs, &cx, "markers");
        assert_eq!(
            markers.file_size(&cx).unwrap(),
            u64::try_from(fsqlite_types::COMMIT_MARKER_RECORD_V1_SIZE).unwrap()
        );
        markers.close(&cx).unwrap();
        assert!(txn.settle_commit(&cx).await.is_err());
        assert!(txn.rollback(&cx).await.is_err());
        drop(txn);
        assert!(matches!(owner.close(&cx), Err(FrankenError::BusyRecovery)));
        source.lock().unwrap().take().unwrap().complete_success();
        assert_eq!(completion.state(), VfsWriteCompletionState::Success);
        owner.close(&cx).unwrap();
        let (fresh, count) = recovered(&vfs, &cx).await;
        assert_eq!(count, 1);
        let mut view = fresh.begin(&cx, TransactionMode::ReadOnly).unwrap();
        assert_eq!(
            view.get_page(&cx, page(2)).await.unwrap().as_bytes(),
            &[6; 512]
        );
        view.rollback(&cx).await.unwrap();
        fresh.close(&cx).unwrap();
    });
}

#[cfg(unix)]
#[test]
fn unix_sealed_pager_reopens_authenticated_native_btree_pages() {
    use fsqlite_wal::native_commit::durable::codec::RaptorQNativeCodec;
    run(async {
        let cx = Cx::new();
        cx.set_native_cx(asupersync::Cx::current().unwrap());
        let directory = tempfile::tempdir().unwrap();
        let objects = directory.path().join("objects");
        let markers = directory.path().join("markers");
        let vfs = fsqlite_vfs::unix::UnixVfs::new();
        let flags = VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE | VfsOpenFlags::WAL;
        let log = NativeDurabilityLog::create(
            &cx,
            vfs.open(&cx, Some(&objects), flags).unwrap().0,
            vfs.open(&cx, Some(&markers), flags).unwrap().0,
            NativeDurabilityLimits::default(),
        )
        .unwrap();
        let store = NativePageStore::new(
            log,
            RaptorQNativeCodec::new(Some([7; 32])),
            512,
            NativePageLimits::default(),
        )
        .unwrap();
        let owner = NativePager::new(store).unwrap();
        let mut txn = owner.begin(&cx, TransactionMode::Concurrent).unwrap();
        let root = root(&mut txn, &cx, true).await;
        {
            let mut cursor = BtCursor::new(TransactionPageIo::new(&mut txn), root, 512, true);
            cursor
                .table_insert(&cx, 7, &vec![0xAD; 4097])
                .await
                .unwrap();
        }
        txn.commit_at(&cx, 100).await.unwrap();
        owner.close(&cx).unwrap();
        let flags = VfsOpenFlags::READWRITE | VfsOpenFlags::WAL;
        let (store, report) = NativePageStore::recover(
            &cx,
            vfs.open(&cx, Some(&objects), flags).unwrap().0,
            vfs.open(&cx, Some(&markers), flags).unwrap().0,
            RaptorQNativeCodec::new(Some([7; 32])),
            512,
            NativeDurabilityLimits::default(),
            NativePageLimits::default(),
        )
        .await
        .unwrap();
        assert_eq!(report.markers.len(), 1);
        let owner = NativePager::new(store).unwrap();
        let mut txn = owner.begin(&cx, TransactionMode::ReadOnly).unwrap();
        assert_eq!(rows(&mut txn, &cx, root).await, vec![(7, vec![0xAD; 4097])]);
        assert!(matches!(
            txn.write_page(&cx, root, &[0; 512]).await,
            Err(FrankenError::ReadOnly)
        ));
        txn.rollback(&cx).await.unwrap();
        owner.close(&cx).unwrap();
    });
}
