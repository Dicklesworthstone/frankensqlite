//! Automatic native commit service. Memory/fault-VFS checks are not physical
//! crash certificates. These nonignored tests require an explicit Cargo run.
use std::future::{Future, poll_fn};
use std::path::Path;
use std::pin::Pin;
use std::task::Poll;

use asupersync::Cx as NativeCx;
use asupersync::runtime::RuntimeBuilder;
use fsqlite_error::{FrankenError, Result};
use fsqlite_harness::fault_vfs::{FaultInjectingVfs, FaultSpec};
use fsqlite_pager::native::NativePager;
use fsqlite_pager::native_service::NativeCommitService;
use fsqlite_pager::{PagerCommitState, TransactionHandle, TransactionMode};
use fsqlite_types::cx::Cx;
use fsqlite_types::flags::VfsOpenFlags;
use fsqlite_types::{CommitSeq, ObjectId, Oti, PageNumber, SymbolRecord, SymbolRecordFlags, reconstruct_systematic_happy_path};
use fsqlite_vfs::{MemoryVfs, Vfs, VfsFile};
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
