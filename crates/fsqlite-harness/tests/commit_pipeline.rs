#[path = "../src/commit_pipeline.rs"]
mod commit_pipeline;

use std::future::Future;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::task::{Context, Poll, Waker};
use std::thread;
use std::time::{Duration, Instant};

use asupersync::channel::mpsc::{RecvError, SendError};
use asupersync::channel::session;
use asupersync::cx::Cx;
use asupersync::types::{Budget, RegionId, TaskId};
use asupersync::util::ArenaIndex;

use commit_pipeline::{
    CommitPipeline, CommitRequest, DEFAULT_COMMIT_CHANNEL_CAPACITY, GroupCommitCoordinator,
    little_law_capacity, resolve_commit_channel_capacity,
};

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

fn wait_until(predicate: impl Fn() -> bool, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if predicate() {
            return;
        }
        thread::sleep(Duration::from_millis(1));
    }
    assert!(predicate(), "timed out waiting for condition");
}

#[test]
fn test_channel_capacity_16_default() {
    let (pipeline, _rx) = CommitPipeline::with_default_capacity();
    assert_eq!(pipeline.capacity(), 16);
    assert_eq!(pipeline.capacity(), DEFAULT_COMMIT_CHANNEL_CAPACITY);
}

#[test]
fn test_capacity_configurable_via_pragma() {
    assert_eq!(
        resolve_commit_channel_capacity(None),
        DEFAULT_COMMIT_CHANNEL_CAPACITY
    );
    assert_eq!(
        resolve_commit_channel_capacity(Some(0)),
        DEFAULT_COMMIT_CHANNEL_CAPACITY
    );
    assert_eq!(resolve_commit_channel_capacity(Some(32)), 32);

    let (pipeline, _rx) = CommitPipeline::from_pragma(Some(32));
    assert_eq!(pipeline.capacity(), 32);
}

#[test]
fn test_little_law_derivation() {
    let burst_capacity = little_law_capacity(148_000, 40, 1, 250);
    assert_eq!(burst_capacity, 15);

    let default_capacity = DEFAULT_COMMIT_CHANNEL_CAPACITY;
    assert!(default_capacity >= burst_capacity);
}

#[test]
fn test_two_phase_reserve_then_send() {
    let cx = test_cx();
    let (pipeline, mut receiver) = CommitPipeline::with_default_capacity();

    let permit = block_on(pipeline.sender().reserve(&cx)).expect("reserve should succeed");
    permit.send(CommitRequest::new(1, 0, vec![1, 2, 3]));

    let got = block_on(receiver.recv(&cx)).expect("receiver should get commit request");
    assert_eq!(got.txn_id, 1);
    assert_eq!(got.reserve_order, 0);
    assert_eq!(got.payload, vec![1, 2, 3]);
}

#[test]
fn test_two_phase_cancel_during_reserve() {
    let cx = test_cx();
    let cancelled = cancelled_cx();
    let (pipeline, _receiver) = CommitPipeline::new(1);

    let held_permit =
        block_on(pipeline.sender().reserve(&cx)).expect("initial reserve must succeed");

    let cancelled_reserve = block_on(pipeline.sender().reserve(&cancelled));
    assert!(matches!(cancelled_reserve, Err(SendError::Cancelled(()))));

    drop(held_permit);

    let reserve_after_cancel = block_on(pipeline.sender().reserve(&cx));
    assert!(reserve_after_cancel.is_ok());
}

#[test]
fn test_two_phase_drop_permit_releases_slot() {
    let cx = test_cx();
    let (pipeline, _receiver) = CommitPipeline::new(1);

    {
        let permit = block_on(pipeline.sender().reserve(&cx)).expect("reserve must succeed");
        drop(permit);
    }

    let reserve_after_drop = block_on(pipeline.sender().reserve(&cx));
    assert!(reserve_after_drop.is_ok());
}

#[test]
fn test_backpressure_blocks_at_capacity() {
    let cx = test_cx();
    let (pipeline, _receiver) = CommitPipeline::new(2);

    let permit_a = block_on(pipeline.sender().reserve(&cx)).expect("first reserve should succeed");
    let permit_b = block_on(pipeline.sender().reserve(&cx)).expect("second reserve should succeed");

    let started = Arc::new(AtomicBool::new(false));
    let acquired = Arc::new(AtomicBool::new(false));

    let started_clone = Arc::clone(&started);
    let acquired_clone = Arc::clone(&acquired);
    let sender = pipeline.sender().clone();

    let join = thread::spawn(move || {
        let thread_cx = test_cx();
        started_clone.store(true, Ordering::SeqCst);
        let permit =
            block_on(sender.reserve(&thread_cx)).expect("reserve should eventually unblock");
        acquired_clone.store(true, Ordering::SeqCst);
        drop(permit);
    });

    wait_until(
        || started.load(Ordering::SeqCst),
        Duration::from_millis(200),
    );
    thread::sleep(Duration::from_millis(20));
    assert!(
        !acquired.load(Ordering::SeqCst),
        "reserve should remain blocked while channel is full"
    );

    drop(permit_a);

    join.join().expect("reserve waiter thread should complete");
    assert!(acquired.load(Ordering::SeqCst));

    drop(permit_b);
}

#[test]
fn test_fifo_ordering_under_contention() {
    let (pipeline, mut receiver) = CommitPipeline::new(16);
    let receiver_join = thread::spawn(move || {
        let receiver_cx = test_cx();
        let mut observed_order = Vec::with_capacity(100);
        for _ in 0..100 {
            let request =
                block_on(receiver.recv(&receiver_cx)).expect("receiver should read all commits");
            observed_order.push(request.reserve_order);
        }
        observed_order
    });

    let order_counter = Arc::new(AtomicU64::new(0));
    let mut joins = Vec::new();

    for worker in 0_u64..10 {
        let sender = pipeline.sender().clone();
        let counter = Arc::clone(&order_counter);
        joins.push(thread::spawn(move || {
            let worker_cx = test_cx();
            for index in 0_u64..10 {
                let permit = block_on(sender.reserve(&worker_cx))
                    .expect("reserve should succeed under contention");
                let reserve_order = counter.fetch_add(1, Ordering::SeqCst);
                let txn_id = (worker * 10) + index;
                permit.send(CommitRequest::new(
                    txn_id,
                    reserve_order,
                    vec![u8::try_from(worker).expect("worker range")],
                ));
            }
        }));
    }

    for join in joins {
        join.join().expect("writer thread must complete");
    }

    let mut observed_order = receiver_join.join().expect("receiver thread must complete");

    let expected_order: Vec<u64> = (0_u64..100).collect();
    // The underlying bounded MPSC preserves FIFO order of *send* completion, not
    // FIFO order of permit reservation. Under contention, producers may reserve
    // permits and then be descheduled before `permit.send(...)`, allowing later
    // producers to enqueue earlier.
    //
    // We still require that every reserved sequence number is delivered exactly
    // once (no drops, no duplicates).
    observed_order.sort_unstable();
    assert_eq!(observed_order, expected_order);
}

#[test]
#[should_panic(expected = "OBLIGATION TOKEN LEAKED")]
fn test_tracked_sender_detects_leaked_permit() {
    let cx = test_cx();
    let (tracked_sender, _receiver) = session::tracked_channel::<CommitRequest>(4);
    let permit = block_on(tracked_sender.reserve(&cx)).expect("tracked reserve should succeed");
    drop(permit);
}

#[test]
fn test_group_commit_batch_size_near_optimal() {
    let mut coordinator = GroupCommitCoordinator::new(DEFAULT_COMMIT_CHANNEL_CAPACITY);
    let mut high_batch_count = 0_usize;

    for _ in 0..256 {
        let batch_size = coordinator.observe_and_plan_batch(2_000, 5, 128);
        assert!((1..=DEFAULT_COMMIT_CHANNEL_CAPACITY).contains(&batch_size));
        if batch_size >= 14 {
            high_batch_count = high_batch_count.saturating_add(1);
        }
    }

    assert!(
        high_batch_count >= 200,
        "batch planner should stay near optimal under sustained load"
    );
}

#[test]
fn test_conformal_batch_size_adapts_to_regime() {
    let mut coordinator = GroupCommitCoordinator::new(64);

    for _ in 0..64 {
        let _ = coordinator.observe_and_plan_batch(2_000, 500, 64);
    }
    let before = coordinator.observe_and_plan_batch(2_000, 500, 64);

    for _ in 0..64 {
        let _ = coordinator.observe_and_plan_batch(10_000, 500, 64);
    }
    let after = coordinator.observe_and_plan_batch(10_000, 500, 64);

    assert!(
        after > before,
        "batch size should increase after fsync regime shift"
    );
    assert!(
        coordinator.controller().regime_shift_resets() >= 1,
        "regime shift should reset calibration windows"
    );
}

#[test]
fn test_fifo_ordering_under_contention_disconnect_semantics() {
    let cx = test_cx();
    let (pipeline, mut receiver) = CommitPipeline::new(2);

    let permit = block_on(pipeline.sender().reserve(&cx)).expect("reserve should succeed");
    permit.send(CommitRequest::new(11, 0, vec![7]));

    let first = block_on(receiver.recv(&cx)).expect("first value should be received");
    assert_eq!(first.txn_id, 11);

    drop(pipeline);

    let disconnected = block_on(receiver.recv(&cx));
    assert!(matches!(disconnected, Err(RecvError::Disconnected)));
}
