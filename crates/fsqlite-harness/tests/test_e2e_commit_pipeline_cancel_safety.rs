#[path = "../src/commit_pipeline.rs"]
mod commit_pipeline;

use std::collections::{BTreeSet, HashSet};
use std::future::Future;
use std::sync::{Arc, mpsc};
use std::task::{Context, Poll, Waker};
use std::thread;

use asupersync::channel::mpsc::SendError;
use asupersync::cx::Cx;
use asupersync::types::{Budget, RegionId, TaskId};
use asupersync::util::ArenaIndex;

use commit_pipeline::{CommitPipeline, CommitRequest};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CancelPoint {
    None,
    DuringReserve,
    BetweenReserveAndSend,
    AfterSend,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WorkerOutcome {
    Committed,
    CancelledBeforeSend,
    CancelledAfterSend,
}

fn test_cx() -> Cx {
    Cx::new(
        RegionId::from_arena(ArenaIndex::new(0, 0)),
        TaskId::from_arena(ArenaIndex::new(0, 0)),
        Budget::INFINITE,
    )
}

fn cancelled_cx() -> Cx {
    let cx = test_cx();
    cx.set_cancel_requested(true);
    cx
}

fn block_on<F: Future>(future: F) -> F::Output {
    struct NoopWaker;

    impl std::task::Wake for NoopWaker {
        fn wake(self: Arc<Self>) {}
    }

    let waker = Waker::from(Arc::new(NoopWaker));
    let mut context = Context::from_waker(&waker);
    let mut pinned = Box::pin(future);

    loop {
        match pinned.as_mut().poll(&mut context) {
            Poll::Ready(value) => return value,
            Poll::Pending => thread::yield_now(),
        }
    }
}

fn deterministic_cancel_plan(total_writers: usize, cancelled_writers: usize) -> Vec<CancelPoint> {
    let mut chosen = BTreeSet::new();
    let mut seed = 0x0A11_CE55_u64;

    while chosen.len() < cancelled_writers {
        seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
        let index =
            usize::try_from(seed % u64::try_from(total_writers).expect("total_writers fits"))
                .expect("index conversion");
        let _ = chosen.insert(index);
    }

    let mut plan = vec![CancelPoint::None; total_writers];
    for (offset, index) in chosen.into_iter().enumerate() {
        plan[index] = match offset % 3 {
            0 => CancelPoint::DuringReserve,
            1 => CancelPoint::BetweenReserveAndSend,
            _ => CancelPoint::AfterSend,
        };
    }

    plan
}

#[test]
#[allow(clippy::too_many_lines)]
fn test_e2e_commit_pipeline_cancel_safety() {
    let total_writers = 50_usize;
    let cancelled_writers = 20_usize;
    let cancel_plan = deterministic_cancel_plan(total_writers, cancelled_writers);

    let cancelled_before_send_count = cancel_plan
        .iter()
        .filter(|point| {
            matches!(
                point,
                CancelPoint::DuringReserve | CancelPoint::BetweenReserveAndSend
            )
        })
        .count();
    let cancelled_after_send_count = cancel_plan
        .iter()
        .filter(|point| matches!(point, CancelPoint::AfterSend))
        .count();

    let expected_messages = total_writers.saturating_sub(cancelled_before_send_count);

    let (pipeline, mut receiver) = CommitPipeline::with_default_capacity();

    let (ready_tx, ready_rx) = mpsc::channel::<()>();
    let (done_tx, done_rx) = mpsc::channel::<()>();

    let coordinator = thread::spawn(move || {
        let cx = test_cx();
        let mut seen_txn_ids = Vec::with_capacity(expected_messages);
        for _ in 0..expected_messages {
            let request = block_on(receiver.recv(&cx)).expect("coordinator should not hang");
            seen_txn_ids.push(request.txn_id);
        }
        ready_tx
            .send(())
            .expect("coordinator should signal drained messages");
        done_rx
            .recv()
            .expect("coordinator should wait for post-drain checks");
        // Keep the commit receiver alive until after the post-storm recovery check.
        // Without this, NLL may drop `receiver` early (after its last use in the
        // drain loop), causing `reserve()` to observe Disconnected().
        drop(receiver);
        seen_txn_ids
    });

    let mut worker_threads = Vec::with_capacity(total_writers);
    for (writer_id, &cancel_point) in cancel_plan.iter().enumerate().take(total_writers) {
        let sender = pipeline.sender().clone();

        worker_threads.push(thread::spawn(move || {
            let cx = test_cx();
            match cancel_point {
                CancelPoint::None => {
                    let permit = block_on(sender.reserve(&cx)).expect("reserve should succeed");
                    permit.send(CommitRequest::new(
                        u64::try_from(writer_id).expect("writer_id fits"),
                        u64::try_from(writer_id).expect("writer_id fits"),
                        vec![1, 2, 3],
                    ));
                    WorkerOutcome::Committed
                }
                CancelPoint::DuringReserve => {
                    let cancelled = cancelled_cx();
                    let result = block_on(sender.reserve(&cancelled));
                    assert!(matches!(result, Err(SendError::Cancelled(()))));
                    WorkerOutcome::CancelledBeforeSend
                }
                CancelPoint::BetweenReserveAndSend => {
                    let permit = block_on(sender.reserve(&cx)).expect("reserve should succeed");
                    drop(permit);
                    WorkerOutcome::CancelledBeforeSend
                }
                CancelPoint::AfterSend => {
                    let permit = block_on(sender.reserve(&cx)).expect("reserve should succeed");
                    permit.send(CommitRequest::new(
                        u64::try_from(writer_id).expect("writer_id fits"),
                        u64::try_from(writer_id).expect("writer_id fits"),
                        vec![9, 9, 9],
                    ));
                    cx.set_cancel_requested(true);
                    WorkerOutcome::CancelledAfterSend
                }
            }
        }));
    }

    let mut committed_count = 0_usize;
    let mut cancelled_before_send_observed = 0_usize;
    let mut cancelled_after_send_observed = 0_usize;

    for worker in worker_threads {
        match worker.join().expect("writer thread should complete") {
            WorkerOutcome::Committed => committed_count = committed_count.saturating_add(1),
            WorkerOutcome::CancelledBeforeSend => {
                cancelled_before_send_observed = cancelled_before_send_observed.saturating_add(1);
            }
            WorkerOutcome::CancelledAfterSend => {
                cancelled_after_send_observed = cancelled_after_send_observed.saturating_add(1);
            }
        }
    }

    ready_rx
        .recv()
        .expect("coordinator should drain expected messages");

    // Capacity recovery / no ghost permits: we can reserve/drop exactly C permits after the storm.
    let recovery_cx = test_cx();
    for _ in 0..pipeline.capacity() {
        let permit = block_on(pipeline.sender().reserve(&recovery_cx))
            .expect("slot should be fully recovered after cancellations");
        drop(permit);
    }

    done_tx
        .send(())
        .expect("coordinator should be unblocked after recovery checks");

    let received_txn_ids = coordinator
        .join()
        .expect("coordinator thread should complete");

    assert_eq!(cancelled_before_send_observed, cancelled_before_send_count);
    assert_eq!(cancelled_after_send_observed, cancelled_after_send_count);
    assert_eq!(received_txn_ids.len(), expected_messages);
    assert_eq!(
        committed_count.saturating_add(cancelled_after_send_observed),
        expected_messages,
        "all non-cancelled and after-send-cancelled commits must be visible"
    );

    let unique_ids: HashSet<u64> = received_txn_ids.iter().copied().collect();
    assert_eq!(unique_ids.len(), received_txn_ids.len());

    // Receiver is dropped after coordinator join; reserve() would observe Disconnected().
}
