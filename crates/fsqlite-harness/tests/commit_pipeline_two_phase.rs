use std::sync::mpsc as std_mpsc;
use std::thread;
use std::time::Duration;

use fsqlite_core::commit_repair::{
    CommitRequest, TrackedSender, conformal_batch_size, little_law_capacity, optimal_batch_size,
    two_phase_commit_channel,
};

fn req(txn_id: u64) -> CommitRequest {
    CommitRequest::new(
        txn_id,
        vec![u32::try_from(txn_id % 53).expect("txn id modulo fits in u32")],
        vec![u8::try_from(txn_id & 0xFF).expect("masked to u8")],
    )
}

#[test]
fn test_reserve_then_send_succeeds() {
    let (sender, receiver) = two_phase_commit_channel(4);
    let permit = sender.reserve();
    let seq = permit.reservation_seq();
    permit.send(req(seq));

    let received = receiver
        .try_recv_for(Duration::from_millis(100))
        .expect("request should be received");
    assert_eq!(received.txn_id, seq);
}

#[test]
fn test_reserve_then_abort_releases_slot() {
    let (sender, _receiver) = two_phase_commit_channel(1);
    let permit = sender.reserve();
    permit.abort();

    let retry = sender.try_reserve_for(Duration::from_millis(100));
    assert!(retry.is_some(), "aborted permit should free slot");
}

#[test]
fn test_reserve_blocks_at_capacity() {
    let (sender, _receiver) = two_phase_commit_channel(2);
    let _a = sender.reserve();
    let _b = sender.reserve();

    let sender_clone = sender.clone();
    let (tx, rx) = std_mpsc::channel();
    thread::spawn(move || {
        let permit = sender_clone.try_reserve_for(Duration::from_millis(20));
        tx.send(permit.is_none())
            .expect("channel send in reserve timeout test");
    });

    let blocked = rx
        .recv_timeout(Duration::from_secs(1))
        .expect("must receive reserve timeout result");
    assert!(blocked, "reserve should block/timeout when channel is full");
}

#[test]
fn test_cancel_during_reserve_no_leak() {
    let (sender, _receiver) = two_phase_commit_channel(1);
    let held = sender.reserve();
    let timed_out = sender.try_reserve_for(Duration::from_millis(5));
    assert!(timed_out.is_none());
    assert_eq!(sender.occupancy(), 1, "no ghost slot should be consumed");
    drop(held);
    assert_eq!(sender.occupancy(), 0);
}

#[test]
fn test_cancel_between_reserve_and_send() {
    let (sender, _receiver) = two_phase_commit_channel(1);
    let permit = sender.reserve();
    drop(permit);
    let retry = sender.try_reserve_for(Duration::from_millis(100));
    assert!(
        retry.is_some(),
        "drop between reserve/send must release slot"
    );
}

#[test]
fn test_fifo_ordering() {
    let (sender, receiver) = two_phase_commit_channel(4);
    for _ in 0..3 {
        let permit = sender.reserve();
        let seq = permit.reservation_seq();
        permit.send(req(seq));
    }

    let first = receiver
        .try_recv_for(Duration::from_millis(100))
        .expect("first receive");
    let second = receiver
        .try_recv_for(Duration::from_millis(100))
        .expect("second receive");
    let third = receiver
        .try_recv_for(Duration::from_millis(100))
        .expect("third receive");

    assert_eq!(first.txn_id, 1);
    assert_eq!(second.txn_id, 2);
    assert_eq!(third.txn_id, 3);
}

#[test]
fn test_tracked_sender_detects_leaked_permit() {
    let (sender, _receiver) = two_phase_commit_channel(2);
    let tracked = TrackedSender::new(sender);
    {
        let _permit = tracked.reserve();
    }
    assert_eq!(tracked.leaked_permit_count(), 1);
}

#[test]
fn test_tracked_sender_normal_send_no_violation() {
    let (sender, receiver) = two_phase_commit_channel(2);
    let tracked = TrackedSender::new(sender);

    let permit = tracked.reserve();
    permit.send(req(11));

    let _ = receiver
        .try_recv_for(Duration::from_millis(100))
        .expect("request should be consumed");
    assert_eq!(tracked.leaked_permit_count(), 0);
}

#[test]
fn test_tracked_sender_explicit_abort_no_violation() {
    let (sender, _receiver) = two_phase_commit_channel(2);
    let tracked = TrackedSender::new(sender);

    let permit = tracked.reserve();
    permit.abort();

    assert_eq!(tracked.leaked_permit_count(), 0);
}

#[test]
fn test_coordinator_drains_batch() {
    let (sender, receiver) = two_phase_commit_channel(8);
    for txn_id in 0_u64..8 {
        let permit = sender.reserve();
        permit.send(req(txn_id));
    }

    let mut drained = 0_usize;
    for _ in 0..8 {
        if receiver.try_recv_for(Duration::from_millis(100)).is_some() {
            drained += 1;
        }
    }
    assert_eq!(drained, 8);
}

#[test]
fn test_group_commit_batch_size_conformal() {
    let fsync_samples = vec![Duration::from_millis(2); 32];
    let validate_samples = vec![Duration::from_micros(5); 32];
    let n = conformal_batch_size(&fsync_samples, &validate_samples, 64);
    assert!(n >= 1);
    assert!(n <= 64);

    let n_opt = optimal_batch_size(Duration::from_millis(2), Duration::from_micros(5), 64);
    assert!(n_opt >= 1);
    assert!(n_opt <= 64);

    let c = little_law_capacity(37_000.0, Duration::from_micros(40), 4.0, 2.5);
    assert_eq!(c, 15);
}
