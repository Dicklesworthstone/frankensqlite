//! Automatic native commit service. Memory/fault-VFS checks are not physical
//! crash certificates. These nonignored tests require an explicit Cargo run.
use std::future::{Future, poll_fn};
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};

use asupersync::Cx as NativeCx;
use asupersync::runtime::RuntimeBuilder;
use fsqlite_btree::{BtCursor, BtreeCursorOps, TransactionPageIo};
use fsqlite_error::{FrankenError, Result};
use fsqlite_harness::fault_vfs::{FaultInjectingVfs, FaultSpec};
use fsqlite_pager::native::NativePager;
use fsqlite_pager::native_service::NativeCommitService;
use fsqlite_pager::{PagerCommitState, TransactionHandle, TransactionMode};
use fsqlite_types::cx::Cx;
use fsqlite_types::flags::{SyncFlags, VfsOpenFlags};
use fsqlite_types::{CommitSeq, LockLevel, ObjectId, Oti, PageNumber, SymbolRecord, SymbolRecordFlags, reconstruct_systematic_happy_path};
use fsqlite_vfs::{FileIdentity, MemoryVfs, ShmRegion, Vfs, VfsFile, VfsWriteCompletion, VfsWriteCompletionState};
use fsqlite_wal::native_commit::durable::NativeObjectCodec;
use fsqlite_wal::native_durability::{NativeDurabilityLimits, NativeDurabilityLog};
use fsqlite_wal::native_pages::{NativePageLimits, NativePageStore};

struct TestCodec;
impl NativeObjectCodec for TestCodec {
    fn encode(&self, _: &Cx, bytes: &[u8]) -> Result<Vec<SymbolRecord>> {
        let t = u32::try_from(bytes.len()).map_err(|_| FrankenError::TooBig)?;
        Ok(vec![SymbolRecord::new(
            ObjectId::derive_from_canonical_bytes(bytes),
            Oti { f: u64::from(t), al: 1, t, z: 1, n: 1 }, 0, bytes.to_vec(),
            SymbolRecordFlags::SYSTEMATIC_RUN_START,
        )])
    }
    fn decode(&self, _: &Cx, id: ObjectId, records: &[SymbolRecord]) -> Result<Vec<u8>> {
        let bytes = reconstruct_systematic_happy_path(records)
            .map_err(|error| FrankenError::WalCorrupt { detail: error.to_string() })?;
        if ObjectId::derive_from_canonical_bytes(&bytes) != id {
            return Err(FrankenError::Abort);
        }
        Ok(bytes)
    }
}
fn run(future: impl Future<Output = ()>) {
    RuntimeBuilder::current_thread().blocking_threads(1, 2).build().unwrap().block_on(future);
}
fn contexts() -> (Cx, NativeCx) {
    let native = NativeCx::current().expect("caller runtime");
    let cx = Cx::new();
    cx.set_native_cx(native.clone());
    (cx, native)
}
fn page(n: u32) -> PageNumber { PageNumber::new(n).unwrap() }
fn file<V: Vfs>(vfs: &V, cx: &Cx, name: &str) -> V::File {
    vfs.open(cx, Some(Path::new(name)),
        VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE | VfsOpenFlags::WAL).unwrap().0
}
fn pager<V: Vfs>(vfs: &V, cx: &Cx) -> NativePager<V::File, V::File, TestCodec> {
    let log = NativeDurabilityLog::create(cx, file(vfs, cx, "objects"),
        file(vfs, cx, "markers"), NativeDurabilityLimits::default()).unwrap();
    NativePager::new(NativePageStore::new(log, TestCodec, 512, NativePageLimits::default()).unwrap()).unwrap()
}
async fn pending_once<F: Future>(mut future: Pin<&mut F>) {
    poll_fn(|cx| {
        assert!(future.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    }).await;
}

#[test]
fn independent_producers_share_syncs_and_receive_their_original_handles() {
    run(async {
        let (cx, native) = contexts();
        let vfs = FaultInjectingVfs::new(MemoryVfs::new());
        let owner = pager(&vfs, &cx);
        let (sender, worker) = NativeCommitService::new(&owner, 16, 16).unwrap();
        let mut tickets = Vec::new();
        let mut old = owner.begin(&cx, TransactionMode::ReadOnly).unwrap();
        for n in 2_u8..=12 {
            let mut transaction = owner.begin(&cx, TransactionMode::Concurrent).unwrap();
            transaction.write_page(&cx, page(u32::from(n)), &[n; 512]).await.unwrap();
            let producer = sender.clone();
            let permit = producer.reserve(&native).await.unwrap();
            tickets.push((n, permit.send(transaction, 100).unwrap()));
        }
        let readonly = owner.begin(&cx, TransactionMode::ReadOnly).unwrap();
        let mut read_ticket = sender.reserve(&native).await.unwrap().send(readonly, 100).unwrap();
        sender.request_shutdown();
        assert_eq!(vfs.sync_count(), 0);
        worker.run(&cx).await.unwrap();
        assert_eq!(vfs.sync_count(), 2, "eleven independent writers share one barrier pair");
        assert_eq!(owner.committed_tip().unwrap(), CommitSeq::new(11));
        for (n, mut ticket) in tickets {
            let completion = ticket.wait(&native).await.unwrap();
            completion.result.unwrap();
            assert_eq!(completion.transaction.pager_commit_state(), PagerCommitState::Committed);
            assert_eq!(completion.transaction.mode(), TransactionMode::Concurrent);
            assert_eq!(completion.transaction.published_visible_commit_seq_hint(), Some(CommitSeq::ZERO));
            assert_eq!(completion.transaction.acknowledgement().unwrap().commit_seq.get(), u64::from(n - 1));
        }
        let completed = read_ticket.wait(&native).await.unwrap();
        completed.result.unwrap();
        assert!(completed.transaction.acknowledgement().is_none());
        assert!(old.get_page(&cx, page(2)).await.is_err());
        old.rollback(&cx).await.unwrap();
        let mut fresh = owner.begin(&cx, TransactionMode::ReadOnly).unwrap();
        for n in 2_u8..=12 {
            assert_eq!(fresh.get_page(&cx, page(u32::from(n))).await.unwrap().as_bytes(), &[n; 512]);
        }
        fresh.rollback(&cx).await.unwrap();
        owner.close(&cx).unwrap();
    });
}

#[test]
fn reservation_backpressure_and_shutdown_return_unaccepted_private_work() {
    run(async {
        let (cx, native) = contexts();
        let vfs = FaultInjectingVfs::new(MemoryVfs::new());
        let owner = pager(&vfs, &cx);
        let (sender, worker) = NativeCommitService::new(&owner, 1, 1).unwrap();
        let slot = sender.try_reserve(&native).unwrap();
        let mut waiting = Box::pin(sender.reserve(&native));
        pending_once(waiting.as_mut()).await;
        drop(waiting); // No transaction was moved into this wait.
        assert!(matches!(sender.try_reserve(&native), Err(FrankenError::Busy)));
        drop(slot);
        let slot = sender.try_reserve(&native).unwrap();
        let mut private = owner.begin(&cx, TransactionMode::Deferred).unwrap();
        private.write_page(&cx, page(2), &[7; 512]).await.unwrap();
        sender.request_shutdown();
        let rejected = match slot.send(private, 100) {
            Ok(_) => panic!("shutdown must reject a previously reserved unsent slot"),
            Err(rejected) => rejected,
        };
        let mut private = rejected.transaction;
        assert_eq!(private.pager_commit_state(), PagerCommitState::NotCommitted);
        assert_eq!(private.get_page(&cx, page(2)).await.unwrap().as_bytes(), &[7; 512]);
        assert_eq!(vfs.sync_count(), 0);
        worker.run(&cx).await.unwrap();
        private.rollback(&cx).await.unwrap();
        owner.close(&cx).unwrap();
    });
}

#[test]
fn dropping_a_wait_or_ticket_does_not_cancel_accepted_intent() {
    run(async {
        let (cx, native) = contexts();
        let vfs = FaultInjectingVfs::new(MemoryVfs::new());
        let owner = pager(&vfs, &cx);
        let (sender, worker) = NativeCommitService::new(&owner, 4, 4).unwrap();
        let mut tickets = Vec::new();
        for n in 2..=3 {
            let mut transaction = owner.begin(&cx, TransactionMode::Concurrent).unwrap();
            transaction.write_page(&cx, page(n), &[9; 512]).await.unwrap();
            tickets.push(sender.reserve(&native).await.unwrap().send(transaction, 100).unwrap());
        }
        let mut retained = tickets.pop().unwrap();
        let mut waiting = Box::pin(retained.wait(&native));
        pending_once(waiting.as_mut()).await;
        drop(waiting);
        drop(tickets); // The other client abandons its reply entirely.
        drop(sender); // Last producer closes: accepted intents still drain.
        worker.run(&cx).await.unwrap();
        retained.wait(&native).await.unwrap().result.unwrap();
        assert_eq!(owner.committed_tip().unwrap(), CommitSeq::new(2));
        assert_eq!(vfs.sync_count(), 2);
        let mut view = owner.begin(&cx, TransactionMode::ReadOnly).unwrap();
        assert_eq!(view.get_page(&cx, page(2)).await.unwrap().as_bytes(), &[9; 512]);
        view.rollback(&cx).await.unwrap();
        owner.close(&cx).unwrap();
    });
}

#[test]
fn dropping_worker_before_io_returns_all_original_private_handles() {
    run(async {
        for polled in [false, true] {
            let (cx, native) = contexts();
            let vfs = FaultInjectingVfs::new(MemoryVfs::new());
            let owner = pager(&vfs, &cx);
            let (sender, worker) = NativeCommitService::new(&owner, 4, 4).unwrap();
            let mut tickets = Vec::new();
            for n in 2..=4 {
                let mut transaction = owner.begin(&cx, TransactionMode::Concurrent).unwrap();
                transaction.savepoint(&cx, "before").unwrap();
                transaction.write_page(&cx, page(n), &[8; 512]).await.unwrap();
                tickets.push((n, sender.reserve(&native).await.unwrap().send(transaction, 100).unwrap()));
            }
            let mut running = Box::pin(worker.run(&cx));
            if polled { pending_once(running.as_mut()).await; } // First request is now worker-owned.
            drop(running);
            assert_eq!(vfs.sync_count(), 0);
            assert!(sender.try_reserve(&native).is_err());
            for (n, mut ticket) in tickets {
                let completion = ticket.wait(&native).await.unwrap();
                assert!(completion.result.is_err());
                let mut transaction = completion.transaction;
                assert_eq!(transaction.pager_commit_state(), PagerCommitState::NotCommitted);
                assert_eq!(transaction.get_page(&cx, page(n)).await.unwrap().as_bytes(), &[8; 512]);
                transaction.rollback_to_savepoint(&cx, "before").unwrap();
                assert!(!transaction.has_pending_writes());
                transaction.rollback(&cx).await.unwrap();
            }
            owner.close(&cx).unwrap();
        }
    });
}

#[test]
fn one_stale_writer_does_not_prevent_independent_queued_commits() {
    run(async {
        let (cx, native) = contexts();
        let vfs = FaultInjectingVfs::new(MemoryVfs::new());
        let owner = pager(&vfs, &cx);
        let (sender, worker) = NativeCommitService::new(&owner, 4, 4).unwrap();
        let mut a = owner.begin(&cx, TransactionMode::Concurrent).unwrap();
        let mut stale = owner.begin(&cx, TransactionMode::Concurrent).unwrap();
        let mut independent = owner.begin(&cx, TransactionMode::Concurrent).unwrap();
        a.write_page(&cx, page(2), &[1; 512]).await.unwrap();
        assert!(stale.get_page(&cx, page(2)).await.is_err());
        stale.write_page(&cx, page(3), &[2; 512]).await.unwrap();
        independent.write_page(&cx, page(4), &[3; 512]).await.unwrap();
        let mut ta = sender.reserve(&native).await.unwrap().send(a, 100).unwrap();
        let mut ts = sender.reserve(&native).await.unwrap().send(stale, 101).unwrap();
        let mut ti = sender.reserve(&native).await.unwrap().send(independent, 102).unwrap();
        sender.request_shutdown();
        worker.run(&cx).await.unwrap();
        ta.wait(&native).await.unwrap().result.unwrap();
        let rejected = ts.wait(&native).await.unwrap();
        assert!(matches!(rejected.result.unwrap_err().as_ref(), FrankenError::BusySnapshot { .. }));
        let mut stale = rejected.transaction;
        assert_eq!(stale.pager_commit_state(), PagerCommitState::NotCommitted);
        assert_eq!(stale.pending_commit_pages().unwrap(), vec![page(3)]);
        stale.rollback(&cx).await.unwrap();
        let independent = ti.wait(&native).await.unwrap();
        independent.result.unwrap();
        assert_eq!(independent.transaction.acknowledgement().unwrap().commit_seq, CommitSeq::new(2));
        assert_eq!(vfs.sync_count(), 4, "only the two revalidated valid writers publish");
        owner.close(&cx).unwrap();
    });
}

#[test]
fn foreign_handles_never_commit_via_individual_fallback() {
    run(async {
        let (cx, native) = contexts();
        let vfs = MemoryVfs::new();
        let other_vfs = MemoryVfs::new();
        let owner = pager(&vfs, &cx);
        let other = pager(&other_vfs, &cx);
        let (sender, worker) = NativeCommitService::new(&owner, 4, 4).unwrap();
        let mut foreign = other.begin(&cx, TransactionMode::Concurrent).unwrap();
        let mut valid = owner.begin(&cx, TransactionMode::Concurrent).unwrap();
        foreign.write_page(&cx, page(2), &[1; 512]).await.unwrap();
        valid.write_page(&cx, page(2), &[2; 512]).await.unwrap();
        let mut bad = sender.reserve(&native).await.unwrap().send(foreign, 100).unwrap();
        let mut good = sender.reserve(&native).await.unwrap().send(valid, 100).unwrap();
        sender.request_shutdown();
        worker.run(&cx).await.unwrap();
        let bad = bad.wait(&native).await.unwrap();
        assert!(matches!(bad.result.unwrap_err().as_ref(), FrankenError::Abort));
        assert_eq!(other.committed_tip().unwrap(), CommitSeq::ZERO);
        let mut foreign = bad.transaction;
        foreign.rollback(&cx).await.unwrap();
        good.wait(&native).await.unwrap().result.unwrap();
        assert_eq!(owner.committed_tip().unwrap(), CommitSeq::new(1));
        owner.close(&cx).unwrap();
        other.close(&cx).unwrap();
    });
}

#[test]
fn cancelled_worker_returns_queued_work_without_starting_storage() {
    run(async {
        let (cx, native) = contexts();
        let vfs = FaultInjectingVfs::new(MemoryVfs::new());
        let owner = pager(&vfs, &cx);
        let (sender, worker) = NativeCommitService::new(&owner, 1, 1).unwrap();
        let mut transaction = owner.begin(&cx, TransactionMode::Concurrent).unwrap();
        transaction.write_page(&cx, page(2), &[4; 512]).await.unwrap();
        let mut ticket = sender.reserve(&native).await.unwrap().send(transaction, 100).unwrap();
        let cancelled = Cx::new();
        cancelled.cancel();
        assert!(matches!(worker.run(&cancelled).await, Err(FrankenError::Interrupt)));
        let completed = ticket.wait(&native).await.unwrap();
        assert!(matches!(completed.result.unwrap_err().as_ref(), FrankenError::Interrupt));
        let mut transaction = completed.transaction;
        assert_eq!(transaction.pager_commit_state(), PagerCommitState::NotCommitted);
        transaction.rollback(&cx).await.unwrap();
        assert_eq!(vfs.sync_count(), 0);
        owner.close(&cx).unwrap();
    });
}

#[test]
fn uncertain_sync_stops_batching_without_retrying_or_losing_queued_handles() {
    run(async {
        for (path, ordinal, recovered_commits) in [("objects", 1, 0), ("markers", 2, 2)] {
            let (cx, native) = contexts();
            let vfs = FaultInjectingVfs::new(MemoryVfs::new());
            let owner = pager(&vfs, &cx);
            let (sender, worker) = NativeCommitService::new(&owner, 4, 2).unwrap();
            let mut tickets = Vec::new();
            for n in 2..=4 {
                let mut transaction = owner.begin(&cx, TransactionMode::Concurrent).unwrap();
                transaction.write_page(&cx, page(n), &[7; 512]).await.unwrap();
                tickets.push(sender.reserve(&native).await.unwrap().send(transaction, 100).unwrap());
            }
            vfs.inject_fault(FaultSpec::power_cut(path).after_nth_sync(ordinal).build());
            assert!(matches!(worker.run(&cx).await, Err(FrankenError::BusyRecovery)));
            assert_eq!(vfs.sync_count(), ordinal, "an uncertain group must never be split/retried");
            assert!(sender.try_reserve(&native).is_err());
            assert!(owner.needs_recovery().unwrap());
            let mut completions = Vec::new();
            for mut ticket in tickets {
                completions.push(ticket.wait(&native).await.unwrap());
            }
            for completion in &completions[..2] {
                assert!(completion.result.is_err());
                assert_eq!(completion.transaction.pager_commit_state(), PagerCommitState::InDoubt);
                assert!(completion.transaction.has_pending_writes());
            }
            assert_eq!(completions[2].transaction.pager_commit_state(), PagerCommitState::NotCommitted);
            assert!(completions[2].result.is_err());
            assert!(matches!(owner.close(&cx), Err(FrankenError::Busy)));
            vfs.power_on();
            for completion in &mut completions[..2] {
                assert!(completion.transaction.settle_commit(&cx).await.is_err());
                assert!(completion.transaction.rollback(&cx).await.is_err());
            }
            completions[2].transaction.rollback(&cx).await.unwrap();
            drop(completions);
            owner.close(&cx).unwrap();
            let (mut reopened, report) = NativePageStore::recover(&cx,
                file(&vfs, &cx, "objects"), file(&vfs, &cx, "markers"), TestCodec, 512,
                NativeDurabilityLimits::default(), NativePageLimits::default(),
            ).await.unwrap();
            assert_eq!(report.markers.len(), recovered_commits);
            assert_eq!(reopened.committed_tip().get(), u64::try_from(recovered_commits).unwrap());
            reopened.close(&cx).unwrap();
        }
    });
}

#[test]
fn completed_handle_cannot_be_submitted_as_a_new_commit() {
    run(async {
        let (cx, native) = contexts();
        let vfs = FaultInjectingVfs::new(MemoryVfs::new());
        let owner = pager(&vfs, &cx);
        assert!(NativeCommitService::new(&owner, 0, 1).is_err());
        assert!(NativeCommitService::new(&owner, 1, 0).is_err());
        let (sender, worker) = NativeCommitService::new(&owner, 1, 1).unwrap();
        let mut transaction = owner.begin(&cx, TransactionMode::Concurrent).unwrap();
        transaction.write_page(&cx, page(2), &[6; 512]).await.unwrap();
        transaction.commit_at(&cx, 100).await.unwrap();
        let error = match sender.reserve(&native).await.unwrap().send(transaction, 200) {
            Ok(_) => panic!("terminal handle must not be readmitted"),
            Err(error) => error,
        };
        assert!(matches!(error.error, FrankenError::Abort));
        assert_eq!(error.transaction.acknowledgement().unwrap().commit_time_unix_ns, 100);
        sender.request_shutdown();
        worker.run(&cx).await.unwrap();
        assert_eq!(vfs.sync_count(), 2);
        assert_eq!(owner.committed_tip().unwrap(), CommitSeq::new(1));
        owner.close(&cx).unwrap();
    });
}

#[derive(Default)]
struct WakeCounter(AtomicUsize);
impl Wake for WakeCounter {
    fn wake(self: Arc<Self>) { self.0.fetch_add(1, Ordering::SeqCst); }
    fn wake_by_ref(self: &Arc<Self>) { self.0.fetch_add(1, Ordering::SeqCst); }
}

#[test]
fn idle_service_is_woken_by_local_cancellation_and_graceful_shutdown() {
    run(async {
        for cancel in [true, false] {
            let (cx, native) = contexts();
            let vfs = FaultInjectingVfs::new(MemoryVfs::new());
            let owner = pager(&vfs, &cx);
            let (sender, worker) = NativeCommitService::new(&owner, 1, 1).unwrap();
            // Intentionally do NOT attach native: cancelling this local node
            // must wake the worker without cancelling the unrelated runtime.
            let local = Cx::new();
            let wakes = Arc::new(WakeCounter::default());
            let waker = Waker::from(Arc::clone(&wakes));
            let mut task_cx = Context::from_waker(&waker);
            let mut running = Box::pin(worker.run(&local));
            assert!(running.as_mut().poll(&mut task_cx).is_pending());
            let before = wakes.0.load(Ordering::SeqCst);
            if cancel { local.cancel(); } else { sender.request_shutdown(); }
            assert!(wakes.0.load(Ordering::SeqCst) > before,
                "the idle future needs a real wake, not an unsolicited manual repoll");
            if cancel {
                assert!(matches!(running.as_mut().poll(&mut task_cx),
                    Poll::Ready(Err(FrankenError::Interrupt))));
            } else {
                assert!(matches!(running.as_mut().poll(&mut task_cx), Poll::Ready(Ok(()))));
            }
            drop(running);
            assert!(sender.try_reserve(&native).is_err());
            assert_eq!(vfs.sync_count(), 0);
            assert!(!owner.needs_recovery().unwrap());
            owner.close(&cx).unwrap();
        }
    });
}

#[test]
fn bounded_producers_and_worker_progress_when_driven_concurrently() {
    run(async {
        let (cx, native) = contexts();
        let vfs = FaultInjectingVfs::new(MemoryVfs::new());
        let owner = pager(&vfs, &cx);
        let (sender, worker) = NativeCommitService::new(&owner, 1, 4).unwrap();
        let mut prepared = Vec::new();
        for n in 2_u8..=9 {
            let mut transaction = owner.begin(&cx, TransactionMode::Concurrent).unwrap();
            transaction.write_page(&cx, page(u32::from(n)), &[n; 512]).await.unwrap();
            prepared.push(transaction);
        }
        let mut producer = Box::pin(async {
            let mut tickets = Vec::new();
            for transaction in prepared {
                // Eight intents cannot all enter a one-slot mailbox before
                // the worker runs. No sender blocks the executor's thread.
                let permit = sender.reserve(&native).await.unwrap();
                tickets.push(permit.send(transaction, 100).unwrap());
            }
            sender.request_shutdown();
            let mut sequences = Vec::new();
            for mut ticket in tickets {
                let completion = ticket.wait(&native).await.unwrap();
                completion.result.unwrap();
                sequences.push(completion.transaction.acknowledgement().unwrap().commit_seq.get());
            }
            sequences
        });
        pending_once(producer.as_mut()).await; // First intent queued; second is backpressured.
        let mut running = Box::pin(worker.run(&cx));
        let mut produced = None;
        let mut finished = false;
        poll_fn(|task_cx| {
            if produced.is_none() {
                if let Poll::Ready(sequences) = producer.as_mut().poll(task_cx) {
                    produced = Some(sequences);
                }
            }
            if !finished {
                if let Poll::Ready(result) = running.as_mut().poll(task_cx) {
                    result.unwrap();
                    finished = true;
                }
            }
            if produced.is_some() && finished { Poll::Ready(()) } else { Poll::Pending }
        }).await;
        drop(producer);
        drop(running);
        assert_eq!(produced.unwrap(), (1_u64..=8).collect::<Vec<_>>());
        assert_eq!(owner.committed_tip().unwrap(), CommitSeq::new(8));
        // Do not bake the coalescing scheduler's exact group sizes into a
        // throughput claim. Each nonempty publication still has two syncs.
        assert_eq!(vfs.sync_count() % 2, 0);
        assert!((2..=16).contains(&vfs.sync_count()));
        let mut view = owner.begin(&cx, TransactionMode::ReadOnly).unwrap();
        for n in 2_u8..=9 {
            assert_eq!(view.get_page(&cx, page(u32::from(n))).await.unwrap().as_bytes(), &[n; 512]);
        }
        view.rollback(&cx).await.unwrap();
        owner.close(&cx).unwrap();
    });
}

// Deterministic source-completion pause, not a fabricated background task:
// write actual marker bytes, retain the source's completion externally, and
// suspend before its success can be reported to the publisher.
struct PausedMarkerFile {
    inner: <MemoryVfs as Vfs>::File,
    source: Arc<Mutex<Option<VfsWriteCompletion>>>,
}
impl VfsFile for PausedMarkerFile {
    fn close(&mut self, cx: &Cx) -> Result<()> { self.inner.close(cx) }
    fn file_identity(&self) -> Result<Option<FileIdentity>> { self.inner.file_identity() }
    fn refresh_file_identity(&self) -> Result<Option<FileIdentity>> { self.inner.refresh_file_identity() }
    async fn read<'a>(&'a self, cx: &'a Cx, buf: &'a mut [u8], offset: u64) -> Result<usize> {
        self.inner.read(cx, buf, offset).await
    }
    async fn write<'a>(&'a self, cx: &'a Cx, buf: &'a [u8], offset: u64) -> Result<()> {
        self.inner.write(cx, buf, offset).await
    }
    async fn write_tracked<'a>(&'a self, cx: &'a Cx, buf: &'a [u8], offset: u64,
        completion: VfsWriteCompletion) -> Result<()> {
        self.inner.write(cx, buf, offset).await?;
        *self.source.lock().unwrap() = Some(completion);
        std::future::pending::<Result<()>>().await
    }
    fn truncate(&mut self, cx: &Cx, size: u64) -> Result<()> { self.inner.truncate(cx, size) }
    fn sync(&mut self, cx: &Cx, flags: SyncFlags) -> Result<()> { self.inner.sync(cx, flags) }
    fn file_size(&self, cx: &Cx) -> Result<u64> { self.inner.file_size(cx) }
    fn lock(&mut self, cx: &Cx, level: LockLevel) -> Result<()> { self.inner.lock(cx, level) }
    fn unlock(&mut self, cx: &Cx, level: LockLevel) -> Result<()> { self.inner.unlock(cx, level) }
    fn lock_external_wal_append(&mut self, cx: &Cx) -> Result<()> { self.inner.lock_external_wal_append(cx) }
    fn owns_external_wal_append_write(&self, cx: &Cx) -> Result<bool> { self.inner.owns_external_wal_append_write(cx) }
    fn restore_external_wal_append_attempt(&mut self, cx: &Cx) -> Result<()> { self.inner.restore_external_wal_append_attempt(cx) }
    fn lock_external_shared_snapshot(&mut self, cx: &Cx) -> Result<()> { self.inner.lock_external_shared_snapshot(cx) }
    fn restore_external_shared_snapshot_attempt(&mut self, cx: &Cx) -> Result<()> { self.inner.restore_external_shared_snapshot_attempt(cx) }
    fn lock_external_maintenance(&mut self, cx: &Cx, wal: bool) -> Result<()> { self.inner.lock_external_maintenance(cx, wal) }
    fn lock_external_wal_recovery(&mut self, cx: &Cx) -> Result<()> { self.inner.lock_external_wal_recovery(cx) }
    fn restore_external_maintenance_attempt(&mut self, cx: &Cx) -> Result<()> { self.inner.restore_external_maintenance_attempt(cx) }
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

#[test]
fn abandoned_worker_keeps_marker_source_completion_and_each_transaction_verdict() {
    run(async {
        let (cx, native) = contexts();
        let vfs = MemoryVfs::new();
        let source = Arc::new(Mutex::new(None));
        let log = NativeDurabilityLog::create(&cx, file(&vfs, &cx, "objects"),
            PausedMarkerFile { inner: file(&vfs, &cx, "markers"), source: Arc::clone(&source) },
            NativeDurabilityLimits::default(),
        ).unwrap();
        let owner = NativePager::new(NativePageStore::new(log, TestCodec, 512,
            NativePageLimits::default()).unwrap()).unwrap();
        let (sender, worker) = NativeCommitService::new(&owner, 4, 2).unwrap();
        let mut tickets = Vec::new();
        for n in 2_u8..=4 {
            let mut transaction = owner.begin(&cx, TransactionMode::Concurrent).unwrap();
            transaction.write_page(&cx, page(u32::from(n)), &[n; 512]).await.unwrap();
            tickets.push(sender.reserve(&native).await.unwrap().send(transaction, 100).unwrap());
        }
        let mut running = Box::pin(worker.run(&cx));
        pending_once(running.as_mut()).await; // Coalescing yield, still before publication.
        assert!(source.lock().unwrap().is_none());
        pending_once(running.as_mut()).await; // Memory I/O reaches the marker source pause.
        assert!(source.lock().unwrap().is_some(), "the drop must occur AFTER actual marker writes");
        drop(running);
        let completion = owner.outstanding_write().unwrap().unwrap();
        assert_eq!(completion.state(), VfsWriteCompletionState::Pending);
        assert_eq!(owner.committed_tip().unwrap(), CommitSeq::ZERO);
        let mut markers = file(&vfs, &cx, "markers");
        assert_eq!(markers.file_size(&cx).unwrap(),
            2 * u64::try_from(fsqlite_types::COMMIT_MARKER_RECORD_V1_SIZE).unwrap());
        markers.close(&cx).unwrap();
        let mut returned = Vec::new();
        for mut ticket in tickets { returned.push(ticket.wait(&native).await.unwrap()); }
        for result in &mut returned[..2] {
            assert!(matches!(result.result.as_ref().unwrap_err().as_ref(), FrankenError::BusyRecovery));
            assert_eq!(result.transaction.pager_commit_state(), PagerCommitState::InDoubt);
            assert!(result.transaction.rollback(&cx).await.is_err());
            assert!(result.transaction.settle_commit(&cx).await.is_err());
        }
        assert_eq!(returned[2].transaction.pager_commit_state(), PagerCommitState::NotCommitted);
        returned[2].transaction.rollback(&cx).await.unwrap();
        assert!(matches!(owner.close(&cx), Err(FrankenError::Busy)));
        drop(returned);
        assert!(matches!(owner.close(&cx), Err(FrankenError::BusyRecovery)));
        source.lock().unwrap().take().unwrap().complete_success();
        assert_eq!(completion.state(), VfsWriteCompletionState::Success);
        owner.close(&cx).unwrap();
        let (store, report) = NativePageStore::recover(&cx, file(&vfs, &cx, "objects"),
            file(&vfs, &cx, "markers"), TestCodec, 512,
            NativeDurabilityLimits::default(), NativePageLimits::default(),
        ).await.unwrap();
        assert_eq!(report.markers.len(), 2);
        let recovered = NativePager::new(store).unwrap();
        let mut view = recovered.begin(&cx, TransactionMode::ReadOnly).unwrap();
        for n in 2_u8..=3 {
            assert_eq!(view.get_page(&cx, page(u32::from(n))).await.unwrap().as_bytes(), &[n; 512]);
        }
        assert!(view.get_page(&cx, page(4)).await.is_err());
        view.rollback(&cx).await.unwrap();
        recovered.close(&cx).unwrap();
    });
}

async fn table_root<T: TransactionHandle>(transaction: &mut T, cx: &Cx) -> PageNumber {
    let root = transaction.allocate_page(cx).await.unwrap();
    let mut bytes = vec![0; transaction.page_size().as_usize()];
    bytes[0] = 0x0D;
    bytes[5..7].copy_from_slice(&512_u16.to_be_bytes());
    transaction.write_page(cx, root, &bytes).await.unwrap();
    root
}
async fn table_rows<T: TransactionHandle>(transaction: &mut T, cx: &Cx, root: PageNumber)
    -> Vec<(i64, Vec<u8>)> {
    let mut cursor = BtCursor::new(TransactionPageIo::new(transaction), root, 512, true);
    let mut rows = Vec::new();
    if cursor.first(cx).await.unwrap() {
        loop {
            rows.push((cursor.rowid(cx).await.unwrap(), cursor.payload(cx).await.unwrap()));
            if !cursor.next(cx).await.unwrap() { break; }
        }
    }
    rows
}

#[cfg(unix)]
#[test]
fn automatic_service_commits_real_btree_splits_and_authenticated_file_recovery() {
    use fsqlite_wal::native_commit::durable::codec::RaptorQNativeCodec;
    run(async {
        let (cx, native) = contexts();
        let directory = tempfile::tempdir().unwrap();
        let objects = directory.path().join("objects");
        let markers = directory.path().join("markers");
        let vfs = fsqlite_vfs::unix::UnixVfs::new();
        let flags = VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE | VfsOpenFlags::WAL;
        let log = NativeDurabilityLog::create(&cx,
            vfs.open(&cx, Some(&objects), flags).unwrap().0,
            vfs.open(&cx, Some(&markers), flags).unwrap().0,
            NativeDurabilityLimits::default(),
        ).unwrap();
        let owner = NativePager::new(NativePageStore::new(log,
            RaptorQNativeCodec::new(Some([7; 32])), 512, NativePageLimits::default(),
        ).unwrap()).unwrap();
        let mut left = owner.begin(&cx, TransactionMode::Concurrent).unwrap();
        let mut right = owner.begin(&cx, TransactionMode::Concurrent).unwrap();
        let mut old = owner.begin(&cx, TransactionMode::ReadOnly).unwrap();
        let left_root = table_root(&mut left, &cx).await;
        let right_root = table_root(&mut right, &cx).await;
        let mut expected = Vec::new();
        for (root, transaction, seed) in [(left_root, &mut left, 17_u8), (right_root, &mut right, 29)] {
            {
                let mut cursor = BtCursor::new(TransactionPageIo::new(&mut *transaction), root, 512, true);
                for row in 0_i64..120 {
                    cursor.table_insert(&cx, row, &vec![seed; 80]).await.unwrap();
                }
                cursor.table_insert(&cx, 999, &vec![seed; 4097]).await.unwrap();
            }
            assert_eq!(transaction.get_page(&cx, root).await.unwrap().as_bytes()[0], 0x05,
                "must exercise an interior B-tree, not a single leaf");
            assert!(transaction.pending_commit_pages().unwrap().len() > 2);
            let rows = table_rows(transaction, &cx, root).await;
            assert_eq!(rows.len(), 121);
            assert_eq!(rows.last().unwrap(), &(999, vec![seed; 4097]));
            expected.push((root, rows));
        }
        let (sender, worker) = NativeCommitService::new(&owner, 4, 4).unwrap();
        let mut a = sender.reserve(&native).await.unwrap().send(left, 100).unwrap();
        let mut b = sender.clone().reserve(&native).await.unwrap().send(right, 100).unwrap();
        sender.request_shutdown();
        worker.run(&cx).await.unwrap();
        let first = a.wait(&native).await.unwrap();
        first.result.unwrap();
        assert_eq!(first.transaction.acknowledgement().unwrap().commit_seq, CommitSeq::new(1));
        let second = b.wait(&native).await.unwrap();
        second.result.unwrap();
        assert_eq!(second.transaction.acknowledgement().unwrap().commit_seq, CommitSeq::new(2));
        assert!(old.get_page(&cx, left_root).await.is_err());
        assert!(old.get_page(&cx, right_root).await.is_err());
        old.rollback(&cx).await.unwrap();
        owner.close(&cx).unwrap();
        let flags = VfsOpenFlags::READWRITE | VfsOpenFlags::WAL;
        let (store, report) = NativePageStore::recover(&cx,
            vfs.open(&cx, Some(&objects), flags).unwrap().0,
            vfs.open(&cx, Some(&markers), flags).unwrap().0,
            RaptorQNativeCodec::new(Some([7; 32])), 512,
            NativeDurabilityLimits::default(), NativePageLimits::default(),
        ).await.unwrap();
        assert_eq!(report.markers.len(), 2);
        let recovered = NativePager::new(store).unwrap();
        let mut reader = recovered.begin(&cx, TransactionMode::ReadOnly).unwrap();
        for (root, expected) in expected {
            assert_eq!(table_rows(&mut reader, &cx, root).await, expected);
        }
        reader.rollback(&cx).await.unwrap();
        recovered.close(&cx).unwrap();
    });
}
