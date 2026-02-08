use std::thread;
use std::time::Duration;

use fsqlite_core::commit_repair::{CommitRequest, two_phase_commit_channel};

fn req(txn_id: u64) -> CommitRequest {
    CommitRequest::new(
        txn_id,
        vec![u32::try_from(txn_id % 4093).expect("txn id modulo fits in u32")],
        vec![u8::try_from(txn_id & 0xFF).expect("masked to u8")],
    )
}

#[test]
fn test_e2e_commit_pipeline_cancel_safety() {
    let (sender, receiver) = two_phase_commit_channel(16);

    // Force "cancel during reserve" deterministically by temporarily saturating capacity.
    let mut blockers = Vec::new();
    for _ in 0..16 {
        blockers.push(sender.reserve());
    }

    let mut reserve_cancel_workers = Vec::new();
    for _ in 0..10 {
        let sender_clone = sender.clone();
        reserve_cancel_workers.push(thread::spawn(move || {
            sender_clone
                .try_reserve_for(Duration::from_millis(5))
                .is_none()
        }));
    }

    for worker in reserve_cancel_workers {
        assert!(
            worker.join().expect("reserve-cancel worker join"),
            "reserve should time out while channel is saturated"
        );
    }

    drop(blockers);

    // Remaining 40 writers:
    // - 10 cancel between reserve/send
    // - 5 cancel after send (message still committed)
    // - 25 normal commit
    let mut writers = Vec::new();
    for writer_id in 0_u64..40 {
        let sender_clone = sender.clone();
        writers.push(thread::spawn(move || {
            let permit = sender_clone.reserve();
            let seq = permit.reservation_seq();

            if writer_id < 10 {
                drop(permit); // cancel between reserve and send
                return (false, false);
            }

            permit.send(req(seq));
            if writer_id < 15 {
                return (true, true); // sent, then task cancelled
            }

            (true, false) // sent normally
        }));
    }

    let coordinator = thread::spawn(move || {
        let mut received = Vec::new();
        while received.len() < 30 {
            let request = receiver
                .try_recv_for(Duration::from_secs(1))
                .expect("coordinator should not hang while messages remain");
            received.push(request.txn_id);
        }
        received
    });

    let mut sent_count = 0_usize;
    let mut cancelled_after_send_count = 0_usize;
    for writer in writers {
        let (sent, cancelled_after_send) = writer.join().expect("writer join");
        if sent {
            sent_count += 1;
        }
        if cancelled_after_send {
            cancelled_after_send_count += 1;
        }
    }

    let received = coordinator.join().expect("coordinator join");
    assert_eq!(cancelled_after_send_count, 5);
    assert_eq!(sent_count, 30, "30 messages should be sent total");
    assert_eq!(received.len(), 30, "all sent commits must be received");

    // No ghost entries: occupancy must return to zero.
    assert_eq!(
        sender.occupancy(),
        0,
        "pipeline leaked reserved/message slots"
    );

    // Capacity fully recovered: reserve full capacity again.
    let mut final_permits = Vec::new();
    for _ in 0..16 {
        final_permits.push(
            sender
                .try_reserve_for(Duration::from_millis(50))
                .expect("channel capacity should be fully recovered"),
        );
    }
    assert_eq!(sender.occupancy(), 16);
    drop(final_permits);
    assert_eq!(sender.occupancy(), 0);
}
