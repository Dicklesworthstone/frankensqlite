use std::sync::mpsc as std_mpsc;
use std::thread;
use std::time::Duration;

use fsqlite_core::commit_repair::{
    CommitRequest, TrackedSender, conformal_batch_size, little_law_capacity, optimal_batch_size,
    two_phase_commit_channel,
};
use fsqlite_types::cx::Cx;

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
    let permit = sender.reserve(&Cx::new()).expect("reserve should succeed");
    let seq = permit.reservation_seq();
    permit.send(req(seq));

    let got = receiver
        .try_recv_for(Duration::from_millis(100))
        .expect("request should be received");
    assert_eq!(got.txn_id, seq);
}

#[test]
fn test_reserve_then_abort_releases_slot() {
    let (sender, _receiver) = two_phase_commit_channel(1);
    let permit = sender.reserve(&Cx::new()).expect("reserve should succeed");
    permit.abort();

    let retry = sender.try_reserve_for(Duration::from_millis(100));
    assert!(retry.is_some(), "aborted permit should free slot");
}

#[test]
fn test_reserve_blocks_at_capacity() {
    let (sender, _receiver) = two_phase_commit_channel(2);
    let sender_a = sender.clone();
    let sender_b = sender.clone();
    let _a = sender_a
        .reserve(&Cx::new())
        .expect("reserve should succeed");
    let _b = sender_b
        .reserve(&Cx::new())
        .expect("reserve should succeed");

    let (tx, rx) = std_mpsc::channel();
    let sender_worker = sender.clone();
    thread::spawn(move || {
        let permit = sender_worker.try_reserve_for(Duration::from_millis(20));
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
    let held = sender.reserve(&Cx::new()).expect("reserve should succeed");
    let timed_out = sender.try_reserve_for(Duration::from_millis(5));
    assert!(timed_out.is_none());
    assert_eq!(sender.occupancy(), 1, "no ghost slot should be consumed");
    drop(held);
    assert_eq!(sender.occupancy(), 0);
}

#[test]
fn test_cancel_between_reserve_and_send() {
    let (sender, _receiver) = two_phase_commit_channel(1);
    let permit = sender.reserve(&Cx::new()).expect("reserve should succeed");
    drop(permit);
    let retry = sender.try_reserve_for(Duration::from_millis(100));
    assert!(
        retry.is_some(),
        "drop between reserve/send must release slot"
    );
}

#[test]
fn test_out_of_order_send_completion_still_delivers_by_reservation_sequence() {
    let (sender, receiver) = two_phase_commit_channel(4);
    let permit1 = sender.reserve(&Cx::new()).expect("reserve should succeed");
    let permit2 = sender.reserve(&Cx::new()).expect("reserve should succeed");
    let permit3 = sender.reserve(&Cx::new()).expect("reserve should succeed");

    let seq1 = permit1.reservation_seq();
    let seq2 = permit2.reservation_seq();
    let seq3 = permit3.reservation_seq();
    assert_eq!((seq1, seq2, seq3), (1, 2, 3));

    permit2.send(req(seq2));
    permit3.send(req(seq3));
    assert_eq!(
        receiver.try_recv_for(Duration::from_millis(20)),
        None,
        "later sends must not bypass an earlier unresolved reservation"
    );

    permit1.send(req(seq1));
    assert_eq!(
        receiver.try_recv_for(Duration::from_millis(100)),
        Some(req(seq1))
    );
    assert_eq!(
        receiver.try_recv_for(Duration::from_millis(100)),
        Some(req(seq2))
    );
    assert_eq!(
        receiver.try_recv_for(Duration::from_millis(100)),
        Some(req(seq3))
    );
}

#[test]
fn test_fifo_ordering() {
    let (sender, receiver) = two_phase_commit_channel(4);
    for _ in 0..3 {
        let permit = sender.reserve(&Cx::new()).expect("reserve should succeed");
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
        let _permit = tracked
            .reserve(&Cx::new())
            .expect("tracked reserve should succeed");
    }
    assert_eq!(tracked.leaked_permit_count(), 1);
}

#[test]
fn test_tracked_sender_normal_send_no_violation() {
    let (sender, receiver) = two_phase_commit_channel(2);
    let tracked = TrackedSender::new(sender);

    let permit = tracked
        .reserve(&Cx::new())
        .expect("tracked reserve should succeed");
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

    let permit = tracked
        .reserve(&Cx::new())
        .expect("tracked reserve should succeed");
    permit.abort();

    assert_eq!(tracked.leaked_permit_count(), 0);
}

#[test]
fn test_coordinator_drains_batch() {
    let (sender, receiver) = two_phase_commit_channel(8);
    for txn_id in 0_u64..8 {
        let permit = sender.reserve(&Cx::new()).expect("reserve should succeed");
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
