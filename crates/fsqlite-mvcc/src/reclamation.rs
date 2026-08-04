//! Epoch-advancement helpers for MVCC version-chain reclamation.
//!
//! The MVCC store already separates pruning from slot recycling: GC removes
//! obsolete versions from the arena with `take_for_retirement()`, then queues
//! the freed arena indices in [`crate::ebr::EbrRetireQueue`]. This module
//! closes the loop by bundling epoch advancement with queue draining so
//! call-sites can perform a complete EBR maintenance pass without open-coding
//! the same sequence repeatedly.

use std::sync::Arc;

use crate::core_types::{VersionArena, VersionIdx};
use crate::ebr::{EbrRetireQueue, VersionGuardRegistry};

/// Result of one EBR reclamation maintenance pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EbrReclamationPass {
    /// Global epoch observed after any requested advancement.
    pub observed_epoch: u64,
    /// Minimum epoch still pinned by an active reader/transaction, if any.
    pub min_pinned_epoch: Option<u64>,
    /// Number of arena slots recycled back to the free list.
    pub recycled_slots: usize,
}

fn drain_reclaimable_slots(
    registry: &Arc<VersionGuardRegistry>,
    retire_queue: &EbrRetireQueue,
    arena: &mut VersionArena,
    target_epoch: u64,
) -> EbrReclamationPass {
    let observed_epoch = registry.advance_epoch_to(target_epoch);
    let min_pinned_epoch = registry.min_pinned_epoch();
    let drained = retire_queue.drain_if_safe(observed_epoch, min_pinned_epoch);
    let recycled_slots = drained.len();
    if recycled_slots > 0 {
        arena.recycle_slots(drained);
    }

    EbrReclamationPass {
        observed_epoch,
        min_pinned_epoch,
        recycled_slots,
    }
}

/// Advance the global EBR epoch once and recycle any reclaimable arena slots.
#[must_use]
pub fn advance_epoch_and_reclaim(
    registry: &Arc<VersionGuardRegistry>,
    retire_queue: &EbrRetireQueue,
    arena: &mut VersionArena,
) -> EbrReclamationPass {
    let observed_epoch = registry.advance_epoch();
    drain_reclaimable_slots(registry, retire_queue, arena, observed_epoch)
}

/// Attempt to recycle retired arena slots at a caller-observed epoch.
#[must_use]
pub fn reclaim_at_epoch(
    registry: &Arc<VersionGuardRegistry>,
    retire_queue: &EbrRetireQueue,
    arena: &mut VersionArena,
    target_epoch: u64,
) -> EbrReclamationPass {
    drain_reclaimable_slots(registry, retire_queue, arena, target_epoch)
}

/// Queue newly retired arena slots, then advance far enough to reclaim them
/// immediately when no active reader still pins the retire epoch.
#[must_use]
pub fn retire_and_reclaim(
    registry: &Arc<VersionGuardRegistry>,
    retire_queue: &EbrRetireQueue,
    arena: &mut VersionArena,
    retired_indices: impl IntoIterator<Item = VersionIdx>,
    retire_epoch: u64,
) -> EbrReclamationPass {
    let retired_indices: Vec<_> = retired_indices.into_iter().collect();
    if retired_indices.is_empty() {
        return reclaim_at_epoch(registry, retire_queue, arena, retire_epoch);
    }

    retire_queue.retire_batch(retired_indices, retire_epoch);
    drain_reclaimable_slots(
        registry,
        retire_queue,
        arena,
        retire_epoch.saturating_add(1),
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use fsqlite_types::{
        CommitSeq, PageData, PageNumber, PageSize, PageVersion, TxnEpoch, TxnId, TxnToken,
    };

    use super::{advance_epoch_and_reclaim, retire_and_reclaim};
    use crate::core_types::{VersionArena, VersionIdx};
    use crate::ebr::{
        EbrRetireQueue, MAX_EBR_RECLAIM_SLOTS_PER_CYCLE, VersionGuard, VersionGuardRegistry,
    };

    fn make_version(pgno: u32, seq: u64) -> PageVersion {
        PageVersion {
            pgno: PageNumber::new(pgno).expect("page number is valid"),
            commit_seq: CommitSeq::new(seq),
            created_by: TxnToken::new(TxnId::new(1).expect("txn id is valid"), TxnEpoch::new(0)),
            data: PageData::zeroed(PageSize::DEFAULT),
            prev: None,
        }
    }

    fn retire_one_slot(arena: &mut VersionArena) -> VersionIdx {
        let idx = arena.alloc(make_version(1, 1));
        let _retired = arena.take_for_retirement(idx);
        idx
    }

    #[test]
    fn retire_and_reclaim_recycles_without_pinned_readers() {
        let registry = Arc::new(VersionGuardRegistry::default());
        let retire_queue = EbrRetireQueue::new();
        let mut arena = VersionArena::new();

        let retired_idx = retire_one_slot(&mut arena);
        let pass = retire_and_reclaim(&registry, &retire_queue, &mut arena, [retired_idx], 0);

        assert_eq!(pass.observed_epoch, 1);
        assert_eq!(pass.min_pinned_epoch, None);
        assert_eq!(pass.recycled_slots, 1);
        assert_eq!(arena.free_count(), 1);
        assert_eq!(retire_queue.pending_count(), 0);
    }

    #[test]
    fn retire_and_reclaim_waits_for_pinned_epoch_to_advance() {
        let registry = Arc::new(VersionGuardRegistry::default());
        let retire_queue = EbrRetireQueue::new();
        let mut arena = VersionArena::new();

        let reader_guard = VersionGuard::pin(Arc::clone(&registry));
        let retired_idx = retire_one_slot(&mut arena);
        let blocked = retire_and_reclaim(&registry, &retire_queue, &mut arena, [retired_idx], 0);

        assert_eq!(blocked.recycled_slots, 0);
        assert_eq!(blocked.min_pinned_epoch, Some(0));
        assert_eq!(retire_queue.pending_count(), 1);

        drop(reader_guard);

        let reclaimed = advance_epoch_and_reclaim(&registry, &retire_queue, &mut arena);
        assert_eq!(reclaimed.recycled_slots, 1);
        assert_eq!(retire_queue.pending_count(), 0);
        assert_eq!(arena.free_count(), 1);
    }

    #[test]
    fn advance_epoch_and_reclaim_bounds_backlog_and_eventually_recycles_it() {
        let registry = Arc::new(VersionGuardRegistry::default());
        let retire_queue = EbrRetireQueue::new();
        let mut arena = VersionArena::new();
        let backlog = MAX_EBR_RECLAIM_SLOTS_PER_CYCLE * 2 + 1;

        let retired = (0..backlog)
            .map(|slot| {
                let index = arena.alloc(make_version(
                    1,
                    u64::try_from(slot + 1).expect("keeper sequence fits u64"),
                ));
                let _version = arena.take_for_retirement(index);
                index
            })
            .collect::<Vec<_>>();
        retire_queue.retire_batch(retired, registry.current_epoch());

        let expected_cycles = [
            MAX_EBR_RECLAIM_SLOTS_PER_CYCLE,
            MAX_EBR_RECLAIM_SLOTS_PER_CYCLE,
            1,
        ];
        let mut recycled_total = 0_usize;
        for expected_slots in expected_cycles {
            let pass = advance_epoch_and_reclaim(&registry, &retire_queue, &mut arena);
            assert_eq!(pass.recycled_slots, expected_slots);
            assert!(
                pass.recycled_slots <= MAX_EBR_RECLAIM_SLOTS_PER_CYCLE,
                "one production maintenance pass must respect the declared reclaim bound"
            );
            recycled_total += pass.recycled_slots;
            assert_eq!(arena.free_count(), recycled_total);
        }

        assert_eq!(retire_queue.pending_count(), 0);
        assert_eq!(recycled_total, backlog);
    }
}
