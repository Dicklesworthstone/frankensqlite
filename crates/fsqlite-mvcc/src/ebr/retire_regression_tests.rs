//! Queue-local regressions: no global metrics reset and no ignored test bodies.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, mpsc};
use std::time::Duration;

use super::{
    EbrRetireQueue, MAX_EBR_RECLAIM_SLOTS_PER_CYCLE, RetiredBatch, VersionIdx, epoch_batch_position,
};

#[test]
fn ebr_batch_caller_iterator_can_reenter_queue_without_deadlock() {
    let queue = Arc::new(EbrRetireQueue::new());
    let worker_queue = Arc::clone(&queue);
    let (finished, completion) = mpsc::channel();
    let worker = std::thread::spawn(move || {
        worker_queue.retire_batch(
            (0..64_u32).map(|slot| {
                // This takes the same mutex as retire_batch. Calling user
                // iteration with that mutex held deadlocks before slot 0.
                let _ = worker_queue.pending_count();
                VersionIdx::new(0, slot, 0)
            }),
            7,
        );
        finished.send(()).expect("report completed retirement");
    });
    // A regression fails rather than hanging the entire test process. On
    // failure the blocked JoinHandle is detached by unwinding, not joined.
    completion
        .recv_timeout(Duration::from_secs(10))
        .expect("retire_batch ran its iterator under the pending-queue mutex");
    worker.join().expect("retiring thread did not panic");
    assert_eq!(queue.pending_count(), 64);
    assert_eq!(queue.total_retired(), 64);
    assert!(queue.drain_if_safe(100, Some(7)).is_empty());
    assert_eq!(queue.drain_if_safe(8, None).len(), 64);
}

#[test]
fn ebr_batch_lookup_handles_wrapped_deque_and_extreme_epochs() {
    let mut batches = VecDeque::with_capacity(8);
    for epoch in 0..8 {
        batches.push_back(RetiredBatch {
            retire_epoch: epoch * 2,
            indices: vec![VersionIdx::new(0, epoch as u32, 0)],
        });
    }
    for _ in 0..5 {
        batches.pop_front();
    }
    for epoch in 8..13 {
        batches.push_back(RetiredBatch {
            retire_epoch: epoch * 2,
            indices: vec![VersionIdx::new(0, epoch as u32, 0)],
        });
    }
    // Binary search must operate on logical deque order, not just one of the
    // ring's physical slices. Compare against a simple linear oracle.
    for epoch in (0..30).chain([u64::MAX]) {
        let expected = batches
            .iter()
            .position(|batch| batch.retire_epoch == epoch)
            .map_or_else(
                || {
                    Err(batches
                        .iter()
                        .take_while(|batch| batch.retire_epoch < epoch)
                        .count())
                },
                Ok,
            );
        assert_eq!(epoch_batch_position(&batches, epoch), expected);
    }
    assert_eq!(epoch_batch_position(&VecDeque::new(), u64::MAX), Err(0));
}

#[test]
fn ebr_batch_delayed_producer_merges_once_and_preserves_epoch_order() {
    let queue = EbrRetireQueue::new();
    for epoch in (1..=64_u64).rev() {
        queue.retire(VersionIdx::new(0, epoch as u32, 0), epoch);
    }
    queue.retire_batch((100..1_100).map(|slot| VersionIdx::new(0, slot, 0)), 3);
    queue.retire_batch(std::iter::empty(), 2);
    let pending = queue.pending.lock();
    assert_eq!(pending.len(), 64);
    assert_eq!(pending[2].retire_epoch, 3);
    assert_eq!(pending[2].indices.len(), 1_001);
    assert!(
        pending
            .iter()
            .zip(pending.iter().skip(1))
            .all(|(a, b)| a.retire_epoch < b.retire_epoch)
    );
    drop(pending);
    assert_eq!(queue.total_retired(), 1_064);
    assert_eq!(queue.pending_count(), 1_064);
    // Only epochs 1, 2 and 3 are safe: the oldest queued epoch remains first.
    assert_eq!(queue.drain_if_safe(100, Some(4)).len(), 1_003);
    assert_eq!(queue.pending_count(), 61);
}

#[test]
fn ebr_batch_large_retirement_obeys_cycle_budget_without_losing_slots() {
    let queue = EbrRetireQueue::new();
    let count = MAX_EBR_RECLAIM_SLOTS_PER_CYCLE * 3 + 17;
    queue.retire_batch((0..count).map(|slot| VersionIdx::new(0, slot as u32, 0)), 9);
    assert!(queue.drain_if_safe(20, Some(9)).is_empty());
    let mut all = HashSet::new();
    while queue.pending_count() != 0 {
        let drained = queue.drain_if_safe(10, None);
        assert!(!drained.is_empty());
        assert!(drained.len() <= MAX_EBR_RECLAIM_SLOTS_PER_CYCLE);
        for index in drained {
            assert!(all.insert(index), "slot was recycled twice");
        }
    }
    assert_eq!(all.len(), count);
    assert_eq!(queue.total_retired(), count as u64);
    assert_eq!(queue.total_recycled(), count as u64);
    assert_eq!(queue.reclaim_cycle_receipt().collection_cycles_total, 4);
}

#[test]
fn ebr_batch_max_epoch_does_not_wrap_reclamation_boundary() {
    let queue = EbrRetireQueue::new();
    let older = VersionIdx::new(0, 1, 0);
    let newest = VersionIdx::new(0, 2, 0);
    queue.retire_batch([newest], u64::MAX);
    queue.retire_batch([older], u64::MAX - 1);
    assert_eq!(queue.drain_if_safe(u64::MAX, None), vec![older]);
    assert!(queue.drain_if_safe(u64::MAX, None).is_empty());
    assert_eq!(queue.force_drain(), vec![newest]);
}

#[test]
fn ebr_batch_randomized_retire_and_reclaim_match_slot_oracle() {
    let queue = EbrRetireQueue::new();
    let mut oracle: HashMap<VersionIdx, u64> = HashMap::new();
    let mut random = 0x4d56_4343_4542_5201_u64;
    let mut next_slot = 0_u32;
    let mut recycled = 0_u64;
    for _ in 0..5_000 {
        random = random
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
        let epoch = (random >> 24) % 128;
        if random & 3 != 0 {
            let count = ((random >> 40) % 19) as usize;
            let mut batch = Vec::with_capacity(count);
            for _ in 0..count {
                let index = VersionIdx::new(0, next_slot, 0);
                next_slot += 1;
                assert!(oracle.insert(index, epoch).is_none());
                batch.push(index);
            }
            queue.retire_batch(batch, epoch);
        } else {
            let current = epoch;
            let pinned = (random & 8 != 0).then_some((random >> 48) % 128);
            let safe = pinned.map_or(current, |pin| pin.min(current));
            let eligible = oracle.values().filter(|epoch| **epoch < safe).count();
            let drained = queue.drain_if_safe(current, pinned);
            assert_eq!(drained.len(), eligible.min(MAX_EBR_RECLAIM_SLOTS_PER_CYCLE));
            for index in drained {
                let retired_epoch = oracle.remove(&index).expect("slot belongs to the queue");
                assert!(retired_epoch < safe, "live-reader boundary was crossed");
                recycled += 1;
            }
        }
        assert_eq!(queue.pending_count(), oracle.len());
        assert_eq!(queue.total_retired(), u64::from(next_slot));
        assert_eq!(queue.total_recycled(), recycled);
    }
    for index in queue.force_drain() {
        assert!(oracle.remove(&index).is_some());
    }
    assert!(oracle.is_empty());
    assert_eq!(queue.total_retired(), queue.total_recycled());
}
