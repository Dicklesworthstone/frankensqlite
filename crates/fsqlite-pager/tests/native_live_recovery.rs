#![cfg(all(feature = "native", not(target_arch = "wasm32")))]
#![allow(clippy::future_not_send)] // Native publication runs on the caller's current-thread runtime.
//! Live pager failure -> same-handle recovery -> new transactions.
//! MemoryVfs and controlled source completions are not power-loss certification.

use std::future::{Future, pending, poll_fn, ready};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::Poll;

use asupersync::runtime::RuntimeBuilder;
use fsqlite_error::{FrankenError, Result};
use fsqlite_pager::native::NativePager;
use fsqlite_pager::{PagerCommitState, TransactionHandle, TransactionMode};
use fsqlite_types::cx::Cx;
use fsqlite_types::flags::{SyncFlags, VfsOpenFlags};
use fsqlite_types::{
    CommitSeq, LockLevel, ObjectId, Oti, PageNumber, SymbolRecord, SymbolRecordFlags,
    reconstruct_systematic_happy_path,
};
use fsqlite_vfs::{
    FileIdentity, MemoryVfs, ShmRegion, SyncKind, Vfs, VfsFile, VfsWriteCompletion,
    VfsWriteCompletionState,
};
use fsqlite_wal::native_commit::durable::NativeObjectCodec;
use fsqlite_wal::native_durability::{NativeDurabilityLimits, NativeDurabilityLog};
use fsqlite_wal::native_pages::{NativePageLimits, NativePageRecovery, NativePageStore};

// The codec isolates ownership/ordering. Real RaptorQ is covered separately;
// this source-only fixture is not evidence of erasure repair or authentication.
struct TestCodec;
impl NativeObjectCodec for TestCodec {
    fn encode(&self, _: &Cx, bytes: &[u8]) -> Result<Vec<SymbolRecord>> {
        let size = u32::try_from(bytes.len()).map_err(|_| FrankenError::TooBig)?;
        Ok(vec![SymbolRecord::new(
            ObjectId::derive_from_canonical_bytes(bytes),
            Oti { f: u64::from(size), al: 1, t: size, z: 1, n: 1 },
            0, bytes.to_vec(), SymbolRecordFlags::SYSTEMATIC_RUN_START,
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

#[derive(Default)]
struct Audit {
    reads: AtomicUsize,
    writes: AtomicUsize,
    syncs: AtomicUsize,
    closes: AtomicUsize,
    fail_sync_at: AtomicUsize,
    fail_close: AtomicBool,
    hold_read: AtomicBool,
    hold_write: AtomicBool,
    pending_write: Mutex<Option<VfsWriteCompletion>>,
}

impl Audit {
    fn counts(&self) -> [usize; 4] {
        [&self.reads, &self.writes, &self.syncs, &self.closes]
            .map(|count| count.load(Ordering::SeqCst))
    }

    fn sync(&self) -> Result<()> {
        let ordinal = self.syncs.fetch_add(1, Ordering::SeqCst) + 1;
        if self.fail_sync_at.load(Ordering::SeqCst) == ordinal {
            self.fail_sync_at.store(0, Ordering::SeqCst);
            return Err(std::io::Error::other("injected live native sync failure").into());
        }
        Ok(())
    }
}

type File = <MemoryVfs as Vfs>::File;

// The original MemoryVfs handle is never replaced by a synthetic byte image.
struct AuditedFile {
    inner: File,
    audit: Arc<Audit>,
}

impl VfsFile for AuditedFile {
    fn close(&mut self, cx: &Cx) -> Result<()> {
        self.audit.closes.fetch_add(1, Ordering::SeqCst);
        if self.audit.fail_close.swap(false, Ordering::SeqCst) {
            return Err(std::io::Error::other("injected live close failure").into());
        }
        self.inner.close(cx)
    }
    fn file_identity(&self) -> Result<Option<FileIdentity>> { self.inner.file_identity() }
    fn refresh_file_identity(&self) -> Result<Option<FileIdentity>> {
        self.inner.refresh_file_identity()
    }
    async fn read<'a>(&'a self, cx: &'a Cx, bytes: &'a mut [u8], offset: u64) -> Result<usize> {
        self.audit.reads.fetch_add(1, Ordering::SeqCst);
        if self.audit.hold_read.swap(false, Ordering::SeqCst) {
            return pending().await;
        }
        self.inner.read(cx, bytes, offset).await
    }
    async fn write<'a>(&'a self, cx: &'a Cx, bytes: &'a [u8], offset: u64) -> Result<()> {
        self.audit.writes.fetch_add(1, Ordering::SeqCst);
        self.inner.write(cx, bytes, offset).await
    }
    async fn write_tracked<'a>(
        &'a self, cx: &'a Cx, bytes: &'a [u8], offset: u64, completion: VfsWriteCompletion,
    ) -> Result<()> {
        self.audit.writes.fetch_add(1, Ordering::SeqCst);
        if self.audit.hold_write.swap(false, Ordering::SeqCst) {
            // Retain source completion outside the borrowed future. The test
            // explicitly settles it after dropping the wait; no worker is spawned.
            *self.audit.pending_write.lock().unwrap() = Some(completion.clone());
            if let Err(error) = self.inner.write(cx, &bytes[..bytes.len().min(60)], offset).await {
                completion.complete_error();
                return Err(error);
            }
            return pending().await;
        }
        self.inner.write_tracked(cx, bytes, offset, completion).await
    }
    fn truncate(&mut self, cx: &Cx, size: u64) -> Result<()> {
        self.audit.writes.fetch_add(1, Ordering::SeqCst);
        self.inner.truncate(cx, size)
    }
    fn sync(&mut self, cx: &Cx, flags: SyncFlags) -> Result<()> {
        self.audit.sync()?;
        self.inner.sync(cx, flags)
    }
    fn durable_sync(&mut self, cx: &Cx, kind: SyncKind) -> Result<()> {
        self.audit.sync()?;
        self.inner.durable_sync(cx, kind)
    }
    fn file_size(&self, cx: &Cx) -> Result<u64> { self.inner.file_size(cx) }
    fn lock(&mut self, cx: &Cx, level: LockLevel) -> Result<()> { self.inner.lock(cx, level) }
    fn unlock(&mut self, cx: &Cx, level: LockLevel) -> Result<()> { self.inner.unlock(cx, level) }
    fn lock_external_wal_append(&mut self, cx: &Cx) -> Result<()> { self.inner.lock_external_wal_append(cx) }
    fn owns_external_wal_append_write(&self, cx: &Cx) -> Result<bool> {
        self.inner.owns_external_wal_append_write(cx)
    }
    fn restore_external_wal_append_attempt(&mut self, cx: &Cx) -> Result<()> {
        self.inner.restore_external_wal_append_attempt(cx)
    }
    fn lock_external_shared_snapshot(&mut self, cx: &Cx) -> Result<()> { self.inner.lock_external_shared_snapshot(cx) }
    fn restore_external_shared_snapshot_attempt(&mut self, cx: &Cx) -> Result<()> {
        self.inner.restore_external_shared_snapshot_attempt(cx)
    }
    fn lock_external_maintenance(&mut self, cx: &Cx, wal: bool) -> Result<()> {
        self.inner.lock_external_maintenance(cx, wal)
    }
    fn lock_external_wal_recovery(&mut self, cx: &Cx) -> Result<()> { self.inner.lock_external_wal_recovery(cx) }
    fn restore_external_maintenance_attempt(&mut self, cx: &Cx) -> Result<()> {
        self.inner.restore_external_maintenance_attempt(cx)
    }
    fn check_reserved_lock(&self, cx: &Cx) -> Result<bool> { self.inner.check_reserved_lock(cx) }
    fn shm_map(&mut self, cx: &Cx, region: u32, size: u32, extend: bool) -> Result<ShmRegion> {
        self.inner.shm_map(cx, region, size, extend)
    }
    fn shm_lock(&mut self, cx: &Cx, offset: u32, n: u32, flags: u32) -> Result<()> {
        self.inner.shm_lock(cx, offset, n, flags)
    }
    fn shm_barrier(&self) { self.inner.shm_barrier(); }
    fn shm_unmap(&mut self, cx: &Cx, delete: bool) -> Result<()> { self.inner.shm_unmap(cx, delete) }
}

type Pager = NativePager<AuditedFile, AuditedFile, TestCodec>;

fn run(future: impl Future<Output = ()>) {
    RuntimeBuilder::current_thread().blocking_threads(1, 2).build().unwrap().block_on(future);
}
fn page(number: u32) -> PageNumber { PageNumber::new(number).unwrap() }
fn file(vfs: &MemoryVfs, cx: &Cx, path: &str) -> File {
    vfs.open(cx, Some(Path::new(path)),
        VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE | VfsOpenFlags::WAL).unwrap().0
}
fn pager(vfs: &MemoryVfs, cx: &Cx, audit: &Arc<Audit>) -> Pager {
    let stream = |name| AuditedFile { inner: file(vfs, cx, name), audit: Arc::clone(audit) };
    let log = NativeDurabilityLog::create(cx, stream("objects"), stream("markers"),
        NativeDurabilityLimits::default()).unwrap();
    NativePager::new(NativePageStore::new(log, TestCodec, 512, NativePageLimits::default()).unwrap()).unwrap()
}
fn refused(owner: Pager) -> Pager {
    match owner.try_into_recovery() {
        Err(owner) => owner,
        Ok(_) => panic!("live pager ownership was discarded"),
    }
}

#[test]
fn both_failed_group_barriers_recover_on_original_streams_and_resume_transactions() {
    run(async {
        for barrier in 1..=2 {
            let cx = Cx::new(); let vfs = MemoryVfs::new(); let audit = Arc::new(Audit::default());
            let mut owner = pager(&vfs, &cx, &audit);
            let mut seed = owner.begin(&cx, TransactionMode::Concurrent).unwrap();
            seed.write_page(&cx, page(2), &[0x11; 512]).await.unwrap();
            seed.commit_at(&cx, 100).await.unwrap();
            drop(seed);
            let mut a = owner.begin(&cx, TransactionMode::Concurrent).unwrap();
            let mut b = owner.begin(&cx, TransactionMode::Concurrent).unwrap();
            a.write_page(&cx, page(2), &[0x22; 512]).await.unwrap();
            b.write_page(&cx, page(3), &[0x33; 512]).await.unwrap();
            let fail_at = audit.syncs.load(Ordering::SeqCst) + barrier;
            audit.fail_sync_at.store(fail_at, Ordering::SeqCst);
            assert!(matches!(owner.commit_batch_at(&cx, &mut [&mut a, &mut b], 101).await,
                Err(FrankenError::Io(_))));
            assert_eq!(audit.syncs.load(Ordering::SeqCst), fail_at);
            assert_eq!(a.pager_commit_state(), PagerCommitState::InDoubt);
            assert_eq!(b.pager_commit_state(), PagerCommitState::InDoubt);
            assert!(a.acknowledgement().is_none());
            assert!(b.acknowledgement().is_none());
            owner = refused(owner);
            assert!(owner.needs_recovery().unwrap());
            drop(a); drop(b); // Abandonment is not a rollback verdict.
            let before = audit.counts();
            let (mut storage, codec) = owner.try_into_recovery()
                .unwrap_or_else(|_| panic!("failed quiescent pager must transfer"));
            assert_eq!(audit.counts(), before, "handoff must perform no I/O");
            storage.prepare(&cx, |id, records| ready(codec.decode(&cx, id, &records).map(|_| ())))
                .await.unwrap();
            let recovered = storage.finish().unwrap();
            let commits = if barrier == 1 { 1 } else { 3 };
            assert_eq!(recovered.report().markers.len(), commits);
            let syncs = audit.syncs.load(Ordering::SeqCst);
            let mut replay = NativePageRecovery::with_shared_codec(
                recovered, codec, 512, NativePageLimits::default());
            replay.prepare(&cx).await.unwrap();
            let (store, _) = replay.finish().unwrap();
            assert_eq!(audit.syncs.load(Ordering::SeqCst), syncs);
            assert_eq!(audit.closes.load(Ordering::SeqCst), 0);
            let owner = NativePager::new(store).unwrap();
            assert_eq!(owner.committed_tip().unwrap(), CommitSeq::new(u64::try_from(commits).unwrap()));
            let mut old = owner.begin(&cx, TransactionMode::ReadOnly).unwrap();
            let expected = if barrier == 1 { 0x11 } else { 0x22 };
            assert_eq!(old.get_page(&cx, page(2)).await.unwrap().as_bytes(), &[expected; 512]);
            if barrier == 1 {
                assert!(old.get_page(&cx, page(3)).await.is_err());
            } else {
                assert_eq!(old.get_page(&cx, page(3)).await.unwrap().as_bytes(), &[0x33; 512]);
            }
            let mut stale = owner.begin(&cx, TransactionMode::Concurrent).unwrap();
            stale.get_page(&cx, page(2)).await.unwrap();
            stale.write_page(&cx, page(4), &[4; 512]).await.unwrap();
            let mut writer = owner.begin(&cx, TransactionMode::Concurrent).unwrap();
            writer.write_page(&cx, page(2), &[0x44; 512]).await.unwrap();
            writer.commit_at(&cx, 1000).await.unwrap();
            assert_eq!(old.get_page(&cx, page(2)).await.unwrap().as_bytes(), &[expected; 512]);
            assert!(matches!(stale.commit_at(&cx, 1001).await, Err(FrankenError::BusySnapshot { .. })));
            stale.rollback(&cx).await.unwrap();
            old.rollback(&cx).await.unwrap();
            owner.close(&cx).unwrap();
        }
    });
}

#[test]
fn transferred_pending_source_write_blocks_recovery_io_and_preserves_its_torn_tail() {
    run(async {
        let cx = Cx::new(); let vfs = MemoryVfs::new(); let audit = Arc::new(Audit::default());
        let owner = pager(&vfs, &cx, &audit);
        let mut seed = owner.begin(&cx, TransactionMode::Concurrent).unwrap();
        seed.write_page(&cx, page(2), &[2; 512]).await.unwrap();
        seed.commit_at(&cx, 100).await.unwrap();
        drop(seed);
        let mut txn = owner.begin(&cx, TransactionMode::Concurrent).unwrap();
        txn.write_page(&cx, page(5), &[5; 512]).await.unwrap();
        audit.hold_write.store(true, Ordering::SeqCst);
        {
            let mut commit = std::pin::pin!(txn.commit_at(&cx, 101));
            poll_fn(|task_cx| {
                assert!(commit.as_mut().poll(task_cx).is_pending());
                Poll::Ready(())
            }).await;
        }
        let source = audit.pending_write.lock().unwrap().as_ref().unwrap().clone();
        assert_eq!(source.state(), VfsWriteCompletionState::Pending);
        assert_eq!(txn.pager_commit_state(), PagerCommitState::InDoubt);
        drop(txn);
        let before = audit.counts();
        let (mut storage, codec) = owner.try_into_recovery()
            .unwrap_or_else(|_| panic!("pending source ownership must be transferable"));
        assert_eq!(audit.counts(), before);
        assert_eq!(storage.outstanding_write().unwrap().state(), VfsWriteCompletionState::Pending);
        assert!(matches!(storage.prepare(&cx, |id, records| {
            ready(codec.decode(&cx, id, &records).map(|_| ()))
        }).await, Err(FrankenError::BusyRecovery)));
        assert!(matches!(storage.close(&cx), Err(FrankenError::BusyRecovery)));
        assert_eq!(audit.counts(), before, "no I/O beneath an outstanding write");
        audit.pending_write.lock().unwrap().take().unwrap().complete_error();
        assert_ne!(source.state(), VfsWriteCompletionState::Pending);
        assert_ne!(storage.outstanding_write().unwrap().state(), VfsWriteCompletionState::Pending);
        storage.prepare(&cx, |id, records| ready(codec.decode(&cx, id, &records).map(|_| ())))
            .await.unwrap();
        let recovered = storage.finish().unwrap();
        assert_eq!(recovered.report().markers.len(), 1);
        assert_eq!(recovered.report().symbol_tail_bytes, 60);
        let mut replay = NativePageRecovery::with_shared_codec(
            recovered, codec, 512, NativePageLimits::default());
        replay.prepare(&cx).await.unwrap();
        let (store, _) = replay.finish().unwrap();
        let owner = NativePager::new(store).unwrap();
        assert!(owner.needs_recovery().unwrap());
        assert!(matches!(owner.begin(&cx, TransactionMode::Concurrent), Err(FrankenError::BusyRecovery)));
        owner.close(&cx).unwrap();
    });
}

#[test]
fn abandoned_validation_queue_transfers_without_misclassifying_it_as_healthy_work() {
    run(async {
        let cx = Cx::new(); let vfs = MemoryVfs::new(); let audit = Arc::new(Audit::default());
        let owner = pager(&vfs, &cx, &audit);
        let mut seed = owner.begin(&cx, TransactionMode::Concurrent).unwrap();
        seed.write_page(&cx, page(2), &[2; 512]).await.unwrap();
        seed.commit_at(&cx, 100).await.unwrap();
        drop(seed);
        let mut txn = owner.begin(&cx, TransactionMode::Concurrent).unwrap();
        txn.write_page(&cx, page(9), &[9; 512]).await.unwrap();
        // Capsule append completes; the driver's pre-publication read suspends.
        // Only the page layer knows the admitted queue was then abandoned.
        audit.hold_read.store(true, Ordering::SeqCst);
        {
            let mut commit = std::pin::pin!(txn.commit_at(&cx, 101));
            poll_fn(|task_cx| {
                assert!(commit.as_mut().poll(task_cx).is_pending());
                Poll::Ready(())
            }).await;
        }
        assert!(!audit.hold_read.load(Ordering::SeqCst));
        assert_eq!(txn.pager_commit_state(), PagerCommitState::InDoubt);
        assert!(owner.outstanding_write().unwrap().is_none());
        assert!(owner.needs_recovery().unwrap());
        drop(txn);
        let before = audit.counts();
        let (mut storage, codec) = owner.try_into_recovery()
            .unwrap_or_else(|_| panic!("page abandonment must propagate to the coordinator"));
        assert_eq!(audit.counts(), before);
        storage.prepare(&cx, |id, records| ready(codec.decode(&cx, id, &records).map(|_| ())))
            .await.unwrap();
        let mut replay = NativePageRecovery::with_shared_codec(
            storage.finish().unwrap(), codec, 512, NativePageLimits::default());
        replay.prepare(&cx).await.unwrap();
        let (store, report) = replay.finish().unwrap();
        assert_eq!(report.markers.len(), 1);
        assert!(!report.append_blocked());
        let owner = NativePager::new(store).unwrap();
        let mut next = owner.begin(&cx, TransactionMode::Concurrent).unwrap();
        assert!(next.get_page(&cx, page(9)).await.is_err());
        next.write_page(&cx, page(9), &[8; 512]).await.unwrap();
        next.commit_at(&cx, 101).await.unwrap();
        assert_eq!(next.acknowledgement().unwrap().commit_seq, CommitSeq::new(2));
        owner.close(&cx).unwrap();
    });
}

#[test]
fn pager_handoff_cannot_recover_away_a_previously_acknowledged_commit() {
    run(async {
        let cx = Cx::new(); let vfs = MemoryVfs::new(); let audit = Arc::new(Audit::default());
        let owner = pager(&vfs, &cx, &audit);
        let mut txn = owner.begin(&cx, TransactionMode::Concurrent).unwrap();
        txn.write_page(&cx, page(2), &[2; 512]).await.unwrap();
        txn.commit_at(&cx, 100).await.unwrap();
        drop(txn);
        let mut markers = file(&vfs, &cx, "markers");
        markers.truncate(&cx, 0).unwrap(); // Sacrificial fixture: lose the accepted prefix.
        markers.close(&cx).unwrap();
        let (mut storage, codec) = owner.try_into_recovery()
            .unwrap_or_else(|_| panic!("quiescent owner must transfer its remembered fence"));
        let before = audit.counts();
        for _ in 0..2 {
            assert!(matches!(storage.prepare(&cx, |id, records| {
                ready(codec.decode(&cx, id, &records).map(|_| ()))
            }).await, Err(FrankenError::WalCorrupt { .. })));
        }
        assert_eq!(audit.writes.load(Ordering::SeqCst), before[1]);
        assert_eq!(audit.syncs.load(Ordering::SeqCst), before[2]);
        assert_eq!(audit.closes.load(Ordering::SeqCst), before[3]);
        storage.close(&cx).unwrap();
    });
}

#[test]
fn failed_close_remains_retryable_but_cannot_be_bypassed_by_recovery_transfer() {
    run(async {
        let cx = Cx::new(); let vfs = MemoryVfs::new(); let audit = Arc::new(Audit::default());
        let owner = pager(&vfs, &cx, &audit);
        audit.fail_close.store(true, Ordering::SeqCst);
        assert!(owner.close(&cx).is_err());
        let before = audit.counts();
        let owner = refused(owner);
        assert_eq!(audit.counts(), before);
        assert!(owner.begin(&cx, TransactionMode::Concurrent).is_err());
        owner.close(&cx).unwrap();
        assert_eq!(audit.closes.load(Ordering::SeqCst), 3);
    });
}
