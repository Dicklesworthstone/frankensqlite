//! GC coordination: scheduling, todo queue, and incremental pruning (§5.6.5).
//!
//! This module implements:
//! - [`GcTodo`]: Per-process touched-page queue with dedup (§5.6.5.1).
//! - [`GcScheduler`]: Frequency derivation from version chain pressure.
//! - [`gc_tick`]: Incremental pruning driver with work budgets.
//! - [`prune_page_chain`]: Single-page chain severing and free-list return.

use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use std::time::Instant;

use fsqlite_types::{CommitSeq, PageNumber, PageNumberBuildHasher, VersionPointer};

use crate::core_types::{VersionArena, VersionIdx};
use crate::ebr::{VersionGuard, VersionGuardRegistry};
use crate::invariants::ChainHeadTable;

/// Convert a `VersionPointer` stored in `PageVersion.prev` to a `VersionIdx`.
#[inline]
#[allow(clippy::cast_possible_truncation)]
fn ptr_to_idx(ptr: VersionPointer) -> VersionIdx {
    let raw = ptr.get();
    let offset = (raw & 0xFFF) as u32;
    let chunk = ((raw >> 12) & 0xF_FFFF) as u32;
    let generation = (raw >> 32) as u32;
    VersionIdx::new(chunk, offset, generation)
}

// ---------------------------------------------------------------------------
// Work budget constants (normative, §5.6.5.1)
// ---------------------------------------------------------------------------

/// Maximum pages to prune per `gc_tick` invocation.
pub const GC_PAGES_BUDGET: u32 = 64;

/// Maximum version slots to free per `gc_tick` invocation.
pub const GC_VERSIONS_BUDGET: u32 = 4096;

// ---------------------------------------------------------------------------
// GC scheduling constants (normative, §5.6.5)
// ---------------------------------------------------------------------------

/// Maximum GC frequency in Hz (never more than once per 10ms).
pub const GC_F_MAX_HZ: f64 = 100.0;

/// Minimum GC frequency in Hz (at least once per second).
pub const GC_F_MIN_HZ: f64 = 1.0;

/// Target mean version chain length (from Theorem 5: R*D+1 for R=100, D=0.07s).
pub const GC_TARGET_CHAIN_LENGTH: f64 = 8.0;

// ---------------------------------------------------------------------------
// GcScheduler
// ---------------------------------------------------------------------------

/// Derives the GC invocation frequency from observed version chain pressure.
///
/// Uses the normative formula from §5.6.5:
/// ```text
/// f_gc = min(f_max, max(f_min, pressure / target))
/// ```
///
/// Time is tracked as milliseconds (u64) for compatibility with the Cx
/// capability context's deterministic clock, avoiding ambient time authority.
#[derive(Debug, Clone)]
pub struct GcScheduler {
    f_max_hz: f64,
    f_min_hz: f64,
    target_chain_length: f64,
    /// Milliseconds since an arbitrary epoch when the last tick occurred.
    last_tick_millis: Option<u64>,
}

impl GcScheduler {
    /// Create a scheduler with the normative constants.
    #[must_use]
    pub fn new() -> Self {
        Self {
            f_max_hz: GC_F_MAX_HZ,
            f_min_hz: GC_F_MIN_HZ,
            target_chain_length: GC_TARGET_CHAIN_LENGTH,
            last_tick_millis: None,
        }
    }

    /// Compute the target GC frequency given observed mean chain length.
    ///
    /// Returns Hz (invocations per second).
    #[must_use]
    pub fn compute_frequency(&self, version_chain_pressure: f64) -> f64 {
        let raw = version_chain_pressure / self.target_chain_length;
        raw.max(self.f_min_hz).min(self.f_max_hz)
    }

    /// Compute the minimum interval between GC ticks for the given pressure.
    ///
    /// Returns interval in milliseconds.
    #[must_use]
    pub fn compute_interval_millis(&self, version_chain_pressure: f64) -> u64 {
        let hz = self.compute_frequency(version_chain_pressure);
        #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
        let millis = (1000.0 / hz) as u64;
        millis.max(1) // at least 1ms
    }

    /// Returns `true` if enough time has elapsed since the last tick for the
    /// given pressure level, and updates the last-tick timestamp.
    ///
    /// `now_millis` should be the current time in milliseconds (e.g., from
    /// `Cx::unix_millis()` or similar deterministic source).
    pub fn should_tick(&mut self, version_chain_pressure: f64, now_millis: u64) -> bool {
        let interval = self.compute_interval_millis(version_chain_pressure);
        match self.last_tick_millis {
            None => {
                self.last_tick_millis = Some(now_millis);
                true
            }
            Some(last) => {
                if now_millis.saturating_sub(last) >= interval {
                    self.last_tick_millis = Some(now_millis);
                    true
                } else {
                    false
                }
            }
        }
    }

    /// Record that a tick occurred at `now_millis` without the should-tick check.
    pub fn record_tick(&mut self, now_millis: u64) {
        self.last_tick_millis = Some(now_millis);
    }
}

impl Default for GcScheduler {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// GcTodo
// ---------------------------------------------------------------------------

/// Per-process touched-page queue for incremental GC (§5.6.5.1).
///
/// Pages that have been published or materialized are enqueued here.
/// `gc_tick` pops from this queue and prunes only those pages' version chains,
/// avoiding the forbidden "scan everything" stop-the-world approach.
#[derive(Debug)]
pub struct GcTodo {
    queue: VecDeque<PageNumber>,
    in_queue: HashSet<PageNumber, PageNumberBuildHasher>,
}

impl GcTodo {
    /// Create an empty todo queue.
    #[must_use]
    pub fn new() -> Self {
        Self {
            queue: VecDeque::new(),
            in_queue: HashSet::with_hasher(PageNumberBuildHasher::default()),
        }
    }

    /// Enqueue a page for future GC pruning.
    ///
    /// Duplicate enqueues are suppressed: a page already in the queue is not
    /// added again until it is popped by `gc_tick`.
    pub fn enqueue(&mut self, pgno: PageNumber) {
        if self.in_queue.insert(pgno) {
            self.queue.push_back(pgno);
        }
    }

    /// Pop the next page to prune, if any.
    pub fn pop(&mut self) -> Option<PageNumber> {
        let pgno = self.queue.pop_front()?;
        self.in_queue.remove(&pgno);
        Some(pgno)
    }

    /// Number of pages awaiting GC.
    #[must_use]
    pub fn len(&self) -> usize {
        self.queue.len()
    }

    /// Whether the queue is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }
}

impl Default for GcTodo {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// prune_page_chain
// ---------------------------------------------------------------------------

/// Result of a single `prune_page_chain` call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PruneResult {
    /// Number of version slots freed back to the arena.
    pub freed: u32,
    /// Whether the chain head was removed (page fully pruned).
    pub head_removed: bool,
    /// `(PageNumber, CommitSeq)` keys of pruned versions, for ARC cache eviction.
    ///
    /// When ARC is integrated (§6.5-6.7), the caller MUST remove these keys from
    /// ARC indexes and ghost lists to prevent memory leaks.
    pub pruned_keys: Vec<(PageNumber, CommitSeq)>,
    /// `VersionIdx` of pruned versions, for visibility_ranges cleanup.
    pub pruned_indices: Vec<VersionIdx>,
}

/// Prune the version chain for a single page, freeing versions older than the
/// GC horizon (§5.6.5.1 `prune_page_chain` normative pseudocode).
///
/// Version chains are ordered by descending `commit_seq` (INV-3). We walk from
/// the head, find the first committed version <= horizon, sever its `prev`
/// link, and free everything below.
///
/// This is pure in-memory work. It MUST NOT perform any file I/O (§5.6.5.1
/// I/O boundary normative rule).
///
/// # Arguments
///
/// * `pgno` — the page whose chain to prune.
/// * `horizon` — the current GC horizon (`shm.gc_horizon`).
/// * `arena` — mutable reference to the version arena (caller holds write lock).
/// * `chain_heads` — mutable reference to the chain head map.
#[must_use]
pub fn prune_page_chain(
    pgno: PageNumber,
    horizon: CommitSeq,
    arena: &mut VersionArena,
    chain_heads: &ChainHeadTable,
) -> PruneResult {
    let guard_registry = Arc::new(VersionGuardRegistry::default());
    prune_page_chain_with_registry(pgno, horizon, arena, chain_heads, &guard_registry)
}

/// Variant of [`prune_page_chain`] that reuses a shared EBR guard registry.
#[must_use]
pub fn prune_page_chain_with_registry(
    pgno: PageNumber,
    horizon: CommitSeq,
    arena: &mut VersionArena,
    chain_heads: &ChainHeadTable,
    guard_registry: &Arc<VersionGuardRegistry>,
) -> PruneResult {
    let Some(head_idx) = chain_heads.get_head(pgno) else {
        return PruneResult {
            freed: 0,
            head_removed: false,
            pruned_keys: Vec::new(),
            pruned_indices: Vec::new(),
        };
    };

    // Walk from head until we find a version with commit_seq <= horizon.
    // All versions above the horizon must be retained (visible to active txns).
    let mut cur_idx = Some(head_idx);

    while let Some(idx) = cur_idx {
        let Some(version) = arena.get(idx) else {
            // Broken chain — stop.
            cur_idx = None;
            break;
        };
        if version.commit_seq <= horizon {
            // Found the first version at or below the horizon.
            // This version itself is the last one we keep (it's the most recent
            // version that a snapshot at `horizon` would see). Everything below
            // it (via `prev`) is reclaimable by Theorem 4.
            break;
        }
        cur_idx = version.prev.map(ptr_to_idx);
    }

    let Some(sever_at) = cur_idx else {
        // Entire chain is above the horizon — nothing to prune.
        return PruneResult {
            freed: 0,
            head_removed: false,
            pruned_keys: Vec::new(),
            pruned_indices: Vec::new(),
        };
    };

    // `sever_at` is the first version with commit_seq <= horizon.
    // Read its prev pointer (the tail to free), then sever.
    let tail_idx = arena
        .get(sever_at)
        .expect("sever_at version must exist")
        .prev
        .map(ptr_to_idx);

    // Sever the chain: set prev = None on the sever point.
    if let Some(version) = arena.get_mut(sever_at) {
        version.prev = None;
    }

    // Free everything from tail_idx onward, collecting pruned keys for ARC.
    let retire_guard = VersionGuard::pin(Arc::clone(guard_registry));
    let mut freed = 0_u32;
    let mut pruned_keys = Vec::new();
    let mut pruned_indices = Vec::new();
    let mut current = tail_idx;
    while let Some(idx) = current {
        let retired = arena.take(idx);
        let next = retired.prev.map(ptr_to_idx);
        pruned_keys.push((pgno, retired.commit_seq));
        pruned_indices.push(idx);
        retire_guard.defer_retire(retired);
        freed += 1;
        current = next;
    }

    if freed > 0 {
        tracing::debug!(
            pgno = pgno.get(),
            freed,
            "prune_page_chain: freed old versions"
        );
    }

    PruneResult {
        freed,
        head_removed: false,
        pruned_keys,
        pruned_indices,
    }
}

// ---------------------------------------------------------------------------
// gc_tick
// ---------------------------------------------------------------------------

/// Result of a single `gc_tick` pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GcTickResult {
    /// Number of pages whose chains were pruned.
    pub pages_pruned: u32,
    /// Total version slots freed across all pruned chains.
    pub versions_freed: u32,
    /// Whether the tick was cut short by the versions budget.
    pub versions_budget_exhausted: bool,
    /// Whether the tick was cut short by the pages budget.
    pub pages_budget_exhausted: bool,
    /// Pages remaining in the GcTodo queue after this tick.
    pub queue_remaining: usize,
    /// Aggregated `(PageNumber, CommitSeq)` keys of pruned versions for ARC eviction.
    ///
    /// The caller MUST pass these to the ARC cache (when available) to remove
    /// stale entries from indexes and ghost lists (§6.7 normative rule).
    pub pruned_keys: Vec<(PageNumber, CommitSeq)>,
    /// `VersionIdx` of pruned versions for visibility range cleanup.
    pub pruned_indices: Vec<VersionIdx>,
}

/// Run one incremental GC pass: pop pages from the todo queue and prune their
/// version chains, subject to work budgets (§5.6.5.1 `gc_tick` pseudocode).
///
/// The caller must provide write-locked `arena` and `chain_heads`.
///
/// # Arguments
///
/// * `todo` — the per-process GC todo queue.
/// * `horizon` — the current GC horizon.
/// * `arena` — mutable reference to the version arena.
/// * `chain_heads` — mutable reference to the chain head map.
#[must_use]
pub fn gc_tick(
    todo: &mut GcTodo,
    horizon: CommitSeq,
    arena: &mut VersionArena,
    chain_heads: &ChainHeadTable,
) -> GcTickResult {
    let guard_registry = Arc::new(VersionGuardRegistry::default());
    gc_tick_with_registry(todo, horizon, arena, chain_heads, &guard_registry)
}

/// Variant of [`gc_tick`] that reuses a shared EBR guard registry.
#[must_use]
pub fn gc_tick_with_registry(
    todo: &mut GcTodo,
    horizon: CommitSeq,
    arena: &mut VersionArena,
    chain_heads: &ChainHeadTable,
    guard_registry: &Arc<VersionGuardRegistry>,
) -> GcTickResult {
    let start = Instant::now();
    let span = tracing::info_span!(
        target: "fsqlite_mvcc::gc",
        "ebr_reclaim",
        horizon = horizon.get(),
        queue_size = todo.len(),
    );
    let _guard = span.enter();

    let mut pages_budget = GC_PAGES_BUDGET;
    let mut versions_budget = GC_VERSIONS_BUDGET;
    let mut pages_pruned = 0_u32;
    let mut versions_freed = 0_u32;
    let mut all_pruned_keys = Vec::new();
    let mut all_pruned_indices = Vec::new();

    while pages_budget > 0 && versions_budget > 0 && !todo.is_empty() {
        let pgno = todo.pop().expect("queue is not empty");
        let result =
            prune_page_chain_with_registry(pgno, horizon, arena, chain_heads, guard_registry);
        versions_freed += result.freed;
        all_pruned_keys.extend(result.pruned_keys);
        all_pruned_indices.extend(result.pruned_indices);
        pages_pruned += 1;
        pages_budget -= 1;
        versions_budget = versions_budget.saturating_sub(result.freed);
    }

    let versions_budget_exhausted = versions_budget == 0 && !todo.is_empty();
    let pages_budget_exhausted = pages_budget == 0 && !todo.is_empty();
    #[allow(clippy::cast_possible_truncation)]
    let grace_period_us = start.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;

    if pages_pruned > 0 {
        tracing::info!(
            target: "fsqlite_mvcc::gc",
            pages_pruned,
            versions_freed,
            queue_remaining = todo.len(),
            grace_period_us,
            "ebr_reclaim: pruning batch complete"
        );
    }

    if !todo.is_empty() && (versions_budget_exhausted || pages_budget_exhausted) {
        tracing::warn!(
            target: "fsqlite_mvcc::gc",
            queue_remaining = todo.len(),
            versions_budget_exhausted,
            pages_budget_exhausted,
            "ebr_reclaim: budget exhausted with pages still queued"
        );
    }

    GcTickResult {
        pages_pruned,
        versions_freed,
        versions_budget_exhausted,
        pages_budget_exhausted,
        queue_remaining: todo.len(),
        pruned_keys: all_pruned_keys,
        pruned_indices: all_pruned_indices,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core_types::VersionArena;
    use crate::ebr::{GLOBAL_EBR_METRICS, VersionGuardRegistry};
    use crate::invariants::{ChainHeadTable, idx_to_version_pointer};
    use fsqlite_types::{
        CommitSeq, PageData, PageNumber, PageSize, PageVersion, TxnEpoch, TxnId, TxnToken,
    };
    use proptest::{prelude::*, test_runner::Config as ProptestConfig};
    use std::collections::HashSet;
    use std::sync::Arc;

    const BEAD_ZCDN: &str = "bd-zcdn";

    /// Helper: build a `PageVersion` with the given commit_seq and prev pointer.
    fn make_version(pgno: PageNumber, seq: u64, prev: Option<VersionIdx>) -> PageVersion {
        PageVersion {
            pgno,
            commit_seq: CommitSeq::new(seq),
            created_by: TxnToken::new(TxnId::new(1).unwrap(), TxnEpoch::new(0)),
            data: PageData::zeroed(PageSize::DEFAULT),
            prev: prev.map(idx_to_version_pointer),
        }
    }

    /// Helper: build a chain of N versions for `pgno` with ascending commit_seq
    /// values `[1, 2, ..., n]`, linked newest→oldest. Returns the head index
    /// and the list of all allocated indices (oldest first).
    fn build_chain(
        arena: &mut VersionArena,
        pgno: PageNumber,
        n: u32,
    ) -> (VersionIdx, Vec<VersionIdx>) {
        let mut indices = Vec::new();
        let mut prev: Option<VersionIdx> = None;
        for seq in 1..=n {
            let v = make_version(pgno, u64::from(seq), prev);
            let idx = arena.alloc(v);
            indices.push(idx);
            prev = Some(idx);
        }
        let head = *indices.last().expect("non-empty chain");
        (head, indices)
    }

    /// Helper: build a chain and install the head into a `ChainHeadTable`.
    fn build_chain_in_table(
        arena: &mut VersionArena,
        chain_heads: &ChainHeadTable,
        pgno: PageNumber,
        n: u32,
    ) -> (VersionIdx, Vec<VersionIdx>) {
        let (head, indices) = build_chain(arena, pgno, n);
        chain_heads.install_with_retry(pgno, head);
        (head, indices)
    }

    fn collect_chain_commit_seqs(
        pgno: PageNumber,
        arena: &VersionArena,
        chain_heads: &ChainHeadTable,
    ) -> Vec<u64> {
        let mut seqs = Vec::new();
        let mut cur = chain_heads.get_head(pgno);
        while let Some(idx) = cur {
            let v = arena
                .get(idx)
                .expect("chain head/index must reference a live version");
            seqs.push(v.commit_seq.get());
            cur = v.prev.map(ptr_to_idx);
        }
        seqs
    }

    // -----------------------------------------------------------------------
    // GcScheduler tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_gc_scheduler_frequency_at_target() {
        // bead_id=bd-zcdn: GC scheduling uses normative constants.
        let sched = GcScheduler::new();
        // At target chain length of 8, frequency = 8/8 = 1 Hz (the floor).
        let freq = sched.compute_frequency(8.0);
        assert!(
            (freq - 1.0).abs() < f64::EPSILON,
            "bead_id={BEAD_ZCDN} freq at target should be 1 Hz, got {freq}"
        );
    }

    #[test]
    fn test_gc_scheduler_frequency_clamps_to_f_min() {
        let sched = GcScheduler::new();
        // Below target: pressure=2 → 2/8 = 0.25, clamped to f_min=1 Hz.
        let freq = sched.compute_frequency(2.0);
        assert!(
            (freq - 1.0).abs() < f64::EPSILON,
            "bead_id={BEAD_ZCDN} freq below target should clamp to 1 Hz, got {freq}"
        );
    }

    #[test]
    fn test_gc_scheduler_frequency_clamps_to_f_max() {
        let sched = GcScheduler::new();
        // Very high pressure: 10_000/8 = 1250, clamped to f_max=100 Hz.
        let freq = sched.compute_frequency(10_000.0);
        assert!(
            (freq - 100.0).abs() < f64::EPSILON,
            "bead_id={BEAD_ZCDN} freq at extreme pressure should clamp to 100 Hz, got {freq}"
        );
    }

    #[test]
    fn test_gc_scheduler_frequency_proportional() {
        let sched = GcScheduler::new();
        // Moderate pressure: 40 → 40/8 = 5 Hz.
        let freq = sched.compute_frequency(40.0);
        assert!(
            (freq - 5.0).abs() < f64::EPSILON,
            "bead_id={BEAD_ZCDN} proportional freq should be 5 Hz, got {freq}"
        );
    }

    #[test]
    fn test_gc_scheduler_interval_from_frequency() {
        let sched = GcScheduler::new();
        let interval_ms = sched.compute_interval_millis(80.0); // 80/8 = 10 Hz → 100ms
        assert_eq!(
            interval_ms, 100,
            "bead_id={BEAD_ZCDN} interval at 10 Hz should be 100ms"
        );
    }

    #[test]
    fn test_gc_scheduler_should_tick_first_always_true() {
        let mut sched = GcScheduler::new();
        let now_millis: u64 = 1000; // arbitrary starting time
        assert!(
            sched.should_tick(1.0, now_millis),
            "bead_id={BEAD_ZCDN} first tick should always fire"
        );
    }

    #[test]
    fn test_gc_scheduler_should_tick_respects_interval() {
        let mut sched = GcScheduler::new();
        let t0: u64 = 1000; // arbitrary starting time in ms
        assert!(sched.should_tick(80.0, t0)); // 10 Hz → 100ms interval

        // 50ms later: too soon.
        let t1 = t0 + 50;
        assert!(
            !sched.should_tick(80.0, t1),
            "bead_id={BEAD_ZCDN} tick should not fire within interval"
        );

        // 100ms later: should fire.
        let t2 = t0 + 100;
        assert!(
            sched.should_tick(80.0, t2),
            "bead_id={BEAD_ZCDN} tick should fire after interval"
        );
    }

    // -----------------------------------------------------------------------
    // GcTodo tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_gc_todo_enqueue_dedup() {
        let mut todo = GcTodo::new();
        let pg1 = PageNumber::new(1).unwrap();
        let pg2 = PageNumber::new(2).unwrap();

        todo.enqueue(pg1);
        todo.enqueue(pg2);
        todo.enqueue(pg1); // duplicate — should be suppressed

        assert_eq!(
            todo.len(),
            2,
            "bead_id={BEAD_ZCDN} dedup should suppress duplicate enqueue"
        );

        assert_eq!(todo.pop(), Some(pg1));
        assert_eq!(todo.pop(), Some(pg2));
        assert_eq!(todo.pop(), None);
    }

    #[test]
    fn test_gc_todo_re_enqueue_after_pop() {
        let mut todo = GcTodo::new();
        let pg = PageNumber::new(5).unwrap();

        todo.enqueue(pg);
        assert_eq!(todo.pop(), Some(pg));

        // After pop, re-enqueue should succeed.
        todo.enqueue(pg);
        assert_eq!(
            todo.len(),
            1,
            "bead_id={BEAD_ZCDN} re-enqueue after pop should succeed"
        );
        assert_eq!(todo.pop(), Some(pg));
    }

    #[test]
    fn test_gc_todo_fifo_order() {
        let mut todo = GcTodo::new();
        let pages: Vec<_> = (1..=10).map(|i| PageNumber::new(i).unwrap()).collect();

        for &pg in &pages {
            todo.enqueue(pg);
        }

        for &expected in &pages {
            assert_eq!(
                todo.pop(),
                Some(expected),
                "bead_id={BEAD_ZCDN} queue should maintain FIFO order"
            );
        }
    }

    // -----------------------------------------------------------------------
    // prune_page_chain tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_prune_page_chain_frees_old_versions() {
        // bead_id=bd-zcdn: Incremental pruning reclaims obsolete page versions.
        let mut arena = VersionArena::new();
        let pgno = PageNumber::new(42).unwrap();

        // Build chain: seq 1 → 2 → 3 → 4 → 5 (head=5).
        let chain_heads = ChainHeadTable::new();
        let (_head, indices) = build_chain_in_table(&mut arena, &chain_heads, pgno, 5);

        // Horizon at seq 3 means: keep version 3 as the last safe version.
        // Versions 1 and 2 should be freed.
        let horizon = CommitSeq::new(3);
        let result = prune_page_chain(pgno, horizon, &mut arena, &chain_heads);

        assert_eq!(
            result.freed, 2,
            "bead_id={BEAD_ZCDN} should free versions 1 and 2"
        );

        // Verify freed slots are actually None in the arena.
        assert!(
            arena.get(indices[0]).is_none(),
            "bead_id={BEAD_ZCDN} version 1 should be freed"
        );
        assert!(
            arena.get(indices[1]).is_none(),
            "bead_id={BEAD_ZCDN} version 2 should be freed"
        );

        // Verify retained versions are still present.
        assert!(arena.get(indices[2]).is_some(), "version 3 retained");
        assert!(arena.get(indices[3]).is_some(), "version 4 retained");
        assert!(arena.get(indices[4]).is_some(), "version 5 retained");

        // Verify version 3 has prev = None (chain severed).
        let v3 = arena.get(indices[2]).unwrap();
        assert!(
            v3.prev.is_none(),
            "bead_id={BEAD_ZCDN} sever point prev should be None"
        );
    }

    #[test]
    fn test_prune_page_chain_uses_ebr_deferral() {
        let mut arena = VersionArena::new();
        let pgno = PageNumber::new(4242).unwrap();
        let chain_heads = ChainHeadTable::new();
        build_chain_in_table(&mut arena, &chain_heads, pgno, 5);
        let registry = Arc::new(VersionGuardRegistry::default());
        let before = GLOBAL_EBR_METRICS.snapshot();

        let result = prune_page_chain_with_registry(
            pgno,
            CommitSeq::new(3),
            &mut arena,
            &chain_heads,
            &registry,
        );

        let after = GLOBAL_EBR_METRICS.snapshot();
        assert_eq!(result.freed, 2, "versions 1 and 2 should be pruned");
        assert!(
            after.retirements_deferred_total >= before.retirements_deferred_total + 2,
            "pruned versions should be deferred via EBR"
        );
        assert!(
            after.guards_pinned_total > before.guards_pinned_total,
            "GC prune should pin an EBR guard while retiring versions"
        );
        assert!(
            after.guards_unpinned_total > before.guards_unpinned_total,
            "GC prune guard should unpin after retirement deferral"
        );
    }

    #[test]
    fn test_prune_page_chain_nothing_to_prune() {
        // All versions are above the horizon — nothing freed.
        let mut arena = VersionArena::new();
        let pgno = PageNumber::new(7).unwrap();

        let chain_heads = ChainHeadTable::new();
        build_chain_in_table(&mut arena, &chain_heads, pgno, 3);

        // Horizon at 0: everything is above it — no pruning.
        let horizon = CommitSeq::new(0);
        let result = prune_page_chain(pgno, horizon, &mut arena, &chain_heads);

        assert_eq!(
            result.freed, 0,
            "bead_id={BEAD_ZCDN} nothing to prune when all above horizon"
        );
    }

    #[test]
    fn test_prune_page_chain_nonexistent_page() {
        let mut arena = VersionArena::new();
        let pgno = PageNumber::new(99).unwrap();
        let chain_heads = ChainHeadTable::new();

        let result = prune_page_chain(pgno, CommitSeq::new(10), &mut arena, &chain_heads);
        assert_eq!(
            result.freed, 0,
            "bead_id={BEAD_ZCDN} nonexistent page should prune nothing"
        );
    }

    #[test]
    fn test_prune_page_chain_single_version_no_prune() {
        // Single version at horizon — nothing below it to prune.
        let mut arena = VersionArena::new();
        let pgno = PageNumber::new(1).unwrap();

        let v = make_version(pgno, 5, None);
        let idx = arena.alloc(v);

        let chain_heads = ChainHeadTable::new();
        chain_heads.install_with_retry(pgno, idx);

        let result = prune_page_chain(pgno, CommitSeq::new(5), &mut arena, &chain_heads);
        assert_eq!(
            result.freed, 0,
            "bead_id={BEAD_ZCDN} single version has nothing below to prune"
        );
        assert!(
            arena.get(idx).is_some(),
            "single version should be retained"
        );
    }

    #[test]
    fn test_prune_frees_arena_slots() {
        // bead_id=bd-zcdn: Freed versions return to the arena free list.
        let mut arena = VersionArena::new();
        let pgno = PageNumber::new(10).unwrap();

        let chain_heads = ChainHeadTable::new();
        build_chain_in_table(&mut arena, &chain_heads, pgno, 8);

        let free_before = arena.free_count();

        // Horizon at 5: versions 1-4 freed (4 slots).
        let result = prune_page_chain(pgno, CommitSeq::new(5), &mut arena, &chain_heads);
        assert_eq!(result.freed, 4);

        let free_after = arena.free_count();
        assert_eq!(
            free_after - free_before,
            4,
            "bead_id={BEAD_ZCDN} freed versions should be on the arena free list"
        );
    }

    #[test]
    fn test_prune_preserves_visible_versions() {
        // bead_id=bd-zcdn: No version visible to any active transaction is ever reclaimed.
        let mut arena = VersionArena::new();
        let pgno = PageNumber::new(20).unwrap();

        // Chain: seq 1, 2, 3, 4, 5, 6, 7, 8, 9, 10.
        let chain_heads = ChainHeadTable::new();
        let (head, indices) = build_chain_in_table(&mut arena, &chain_heads, pgno, 10);

        // Horizon at 6: a snapshot at commit_seq 6 would see version 6.
        // Versions 7-10 are above horizon (needed by newer snapshots).
        // Version 6 is the last safe version — kept. Versions 1-5 are freed.
        let result = prune_page_chain(pgno, CommitSeq::new(6), &mut arena, &chain_heads);
        assert_eq!(result.freed, 5, "versions 1-5 should be freed");

        // Verify: versions 6-10 are all still accessible.
        for (seq, idx) in indices.iter().enumerate().skip(5).take(5) {
            assert!(
                arena.get(*idx).is_some(),
                "bead_id={BEAD_ZCDN} version at seq {} must be retained (visible to active txn)",
                seq + 1
            );
        }

        // Verify: chain walk from head still works.
        let mut count = 0;
        let mut cur = Some(head);
        while let Some(idx) = cur {
            count += 1;
            let v = arena.get(idx).unwrap();
            cur = v.prev.map(ptr_to_idx);
        }
        assert_eq!(count, 5, "retained chain should have versions 6-10");
    }

    // -----------------------------------------------------------------------
    // gc_tick tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_gc_tick_incremental_pruning() {
        // bead_id=bd-zcdn: Incremental pruning with GcTodo queue.
        let mut arena = VersionArena::new();
        let chain_heads = ChainHeadTable::new();
        let mut todo = GcTodo::new();

        // Set up 3 pages, each with 5 versions.
        for i in 1..=3 {
            let pgno = PageNumber::new(i).unwrap();
            build_chain_in_table(&mut arena, &chain_heads, pgno, 5);
            todo.enqueue(pgno);
        }

        let horizon = CommitSeq::new(3); // keep version 3, free 1 & 2 per page.
        let result = gc_tick(&mut todo, horizon, &mut arena, &chain_heads);

        assert_eq!(result.pages_pruned, 3, "should prune all 3 pages");
        assert_eq!(
            result.versions_freed, 6,
            "should free 2 versions per page × 3 pages"
        );
        assert_eq!(result.queue_remaining, 0);
        assert!(!result.versions_budget_exhausted);
        assert!(!result.pages_budget_exhausted);
    }

    #[test]
    fn test_gc_tick_respects_pages_budget() {
        // bead_id=bd-zcdn: GC scheduling avoids starvation — budget enforcement.
        let mut arena = VersionArena::new();
        let chain_heads = ChainHeadTable::new();
        let mut todo = GcTodo::new();

        // Enqueue 100 pages (more than GC_PAGES_BUDGET=64).
        for i in 1..=100 {
            let pgno = PageNumber::new(i).unwrap();
            build_chain_in_table(&mut arena, &chain_heads, pgno, 3);
            todo.enqueue(pgno);
        }

        let result = gc_tick(&mut todo, CommitSeq::new(2), &mut arena, &chain_heads);

        assert_eq!(
            result.pages_pruned, GC_PAGES_BUDGET,
            "bead_id={BEAD_ZCDN} should stop at pages budget"
        );
        assert_eq!(
            result.queue_remaining, 36,
            "remaining pages should be 100 - 64 = 36"
        );
        assert!(result.pages_budget_exhausted);
    }

    #[test]
    fn test_gc_tick_respects_versions_budget() {
        // Create pages with very long chains to exhaust versions budget.
        let mut arena = VersionArena::new();
        let chain_heads = ChainHeadTable::new();
        let mut todo = GcTodo::new();

        // 10 pages, each with 1000 versions (seq 1..1000).
        // Horizon at 999 → sever at version 999, free versions 1..998 = 998 each.
        // versions_budget = 4096, pages_budget = 64.
        // Page 1: freed=998, budget=4096-998=3098
        // Page 2: freed=998, budget=3098-998=2100
        // Page 3: freed=998, budget=2100-998=1102
        // Page 4: freed=998, budget=1102-998=104
        // Page 5: freed=998, budget=104-998=0 (saturating)
        // Page 6: budget=0, loop exits.
        for i in 1..=10 {
            let pgno = PageNumber::new(i).unwrap();
            build_chain_in_table(&mut arena, &chain_heads, pgno, 1000);
            todo.enqueue(pgno);
        }

        let result = gc_tick(&mut todo, CommitSeq::new(999), &mut arena, &chain_heads);

        assert!(
            result.pages_pruned <= 10,
            "bead_id={BEAD_ZCDN} should stop before processing all pages"
        );
        assert!(
            result.versions_freed >= GC_VERSIONS_BUDGET,
            "should have freed at least the budget worth of versions (freed={}, budget={}, pages_pruned={}, queue_remaining={})",
            result.versions_freed,
            GC_VERSIONS_BUDGET,
            result.pages_pruned,
            result.queue_remaining
        );
        assert!(
            result.versions_budget_exhausted,
            "bead_id={BEAD_ZCDN} versions budget should be exhausted"
        );
    }

    #[test]
    fn test_gc_tick_empty_queue() {
        let mut arena = VersionArena::new();
        let chain_heads = ChainHeadTable::new();
        let mut todo = GcTodo::new();

        let result = gc_tick(&mut todo, CommitSeq::new(100), &mut arena, &chain_heads);

        assert_eq!(result.pages_pruned, 0);
        assert_eq!(result.versions_freed, 0);
        assert!(!result.versions_budget_exhausted);
        assert!(!result.pages_budget_exhausted);
    }

    #[test]
    fn test_gc_tick_no_io_during_prune() {
        // bead_id=bd-zcdn: prune_page_chain is pure in-memory.
        // This is a structural test: the function signature takes only
        // VersionArena and chain_heads — no File, no Pager, no I/O handle.
        // If it compiled, it cannot do I/O. This test documents that guarantee.
        let mut arena = VersionArena::new();
        let pgno = PageNumber::new(1).unwrap();
        let chain_heads = ChainHeadTable::new();
        build_chain_in_table(&mut arena, &chain_heads, pgno, 5);

        // This compiles and runs: proof that prune_page_chain is pure in-memory.
        let result = prune_page_chain(pgno, CommitSeq::new(3), &mut arena, &chain_heads);
        assert_eq!(
            result.freed, 2,
            "bead_id={BEAD_ZCDN} pure in-memory prune works correctly"
        );
    }

    // -----------------------------------------------------------------------
    // bd-3t3.10: incremental across calls + ARC eviction tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_gc_tick_incremental_across_calls() {
        // bead_id=bd-3t3.10: Enqueue 100 pages. First gc_tick processes 64
        // (pages budget). Second gc_tick processes remaining 36.
        let mut arena = VersionArena::new();
        let chain_heads = ChainHeadTable::new();
        let mut todo = GcTodo::new();

        for i in 1..=100 {
            let pgno = PageNumber::new(i).unwrap();
            build_chain_in_table(&mut arena, &chain_heads, pgno, 5);
            todo.enqueue(pgno);
        }
        assert_eq!(todo.len(), 100);

        // Tick 1: processes exactly GC_PAGES_BUDGET=64 pages.
        let horizon = CommitSeq::new(3); // free versions 1,2 per page
        let r1 = gc_tick(&mut todo, horizon, &mut arena, &chain_heads);
        assert_eq!(
            r1.pages_pruned, GC_PAGES_BUDGET,
            "bead_id=bd-3t3.10: first tick should process 64 pages"
        );
        assert_eq!(
            r1.queue_remaining, 36,
            "bead_id=bd-3t3.10: 36 pages should remain after first tick"
        );
        assert!(r1.pages_budget_exhausted);

        // Tick 2: processes the remaining 36 pages.
        let r2 = gc_tick(&mut todo, horizon, &mut arena, &chain_heads);
        assert_eq!(
            r2.pages_pruned, 36,
            "bead_id=bd-3t3.10: second tick should process remaining 36 pages"
        );
        assert_eq!(
            r2.queue_remaining, 0,
            "bead_id=bd-3t3.10: queue should be empty after second tick"
        );
        assert!(!r2.pages_budget_exhausted);
        assert!(!r2.versions_budget_exhausted);

        // Total freed: 2 versions × 100 pages = 200.
        assert_eq!(
            r1.versions_freed + r2.versions_freed,
            200,
            "bead_id=bd-3t3.10: total freed should be 2 per page × 100 pages"
        );
    }

    #[test]
    fn test_arc_eviction_on_prune() {
        // bead_id=bd-3t3.10: After pruning, verify pruned_keys contains the
        // correct (pgno, commit_seq) pairs for ARC cache eviction.
        // When ARC is integrated (§6.5-6.7), these keys MUST be removed from
        // ARC indexes and ghost lists.
        let mut arena = VersionArena::new();
        let pgno = PageNumber::new(50).unwrap();

        // Chain: seq 1, 2, 3, 4, 5 (head=5).
        let chain_heads = ChainHeadTable::new();
        build_chain_in_table(&mut arena, &chain_heads, pgno, 5);

        // Horizon at 3: keep versions 3,4,5. Free versions 1,2.
        let result = prune_page_chain(pgno, CommitSeq::new(3), &mut arena, &chain_heads);

        assert_eq!(result.freed, 2);
        assert_eq!(
            result.pruned_keys.len(),
            2,
            "bead_id=bd-3t3.10: pruned_keys should contain 2 entries"
        );

        // Verify the pruned keys are (pgno, seq=2) and (pgno, seq=1),
        // in the order they were freed (newest-first from the severed tail).
        let seqs: Vec<u64> = result.pruned_keys.iter().map(|(_, cs)| cs.get()).collect();
        assert!(
            seqs.contains(&1) && seqs.contains(&2),
            "bead_id=bd-3t3.10: pruned_keys must contain versions 1 and 2, got: {seqs:?}"
        );
        // All keys should be for this page.
        for (pn, _) in &result.pruned_keys {
            assert_eq!(
                *pn, pgno,
                "bead_id=bd-3t3.10: all pruned keys must be for the pruned page"
            );
        }
    }

    #[test]
    fn test_gc_tick_pruned_keys_aggregated() {
        // bead_id=bd-3t3.10: gc_tick aggregates pruned_keys from all pages.
        let mut arena = VersionArena::new();
        let chain_heads = ChainHeadTable::new();
        let mut todo = GcTodo::new();

        // 3 pages, each with 5 versions.
        for i in 1..=3 {
            let pgno = PageNumber::new(i).unwrap();
            build_chain_in_table(&mut arena, &chain_heads, pgno, 5);
            todo.enqueue(pgno);
        }

        // Horizon at 3: free versions 1,2 per page = 6 total pruned keys.
        let result = gc_tick(&mut todo, CommitSeq::new(3), &mut arena, &chain_heads);

        assert_eq!(result.versions_freed, 6);
        assert_eq!(
            result.pruned_keys.len(),
            6,
            "bead_id=bd-3t3.10: gc_tick should aggregate 6 pruned keys (2 per page × 3 pages)"
        );

        // Verify all 3 pages are represented.
        let page_nums: HashSet<u32> = result.pruned_keys.iter().map(|(pn, _)| pn.get()).collect();
        assert!(page_nums.contains(&1));
        assert!(page_nums.contains(&2));
        assert!(page_nums.contains(&3));
    }

    #[test]
    fn test_gc_horizon_monotonic_safety_invariant() {
        // bead_id=bd-zcdn: No version visible to any active transaction is reclaimed.
        // Simulate: active txn at begin_seq=5, chain with versions 1..10.
        // gc_horizon must not advance past 5 while that txn is alive.
        // After pruning at horizon=5, version 5 must still be present.
        let mut arena = VersionArena::new();
        let pgno = PageNumber::new(33).unwrap();
        let chain_heads = ChainHeadTable::new();
        let (_head, indices) = build_chain_in_table(&mut arena, &chain_heads, pgno, 10);

        // Active transaction started at begin_seq=5 → horizon cannot go past 5.
        let horizon = CommitSeq::new(5);
        let _ = prune_page_chain(pgno, horizon, &mut arena, &chain_heads);

        // The version at seq=5 (index 4) must still be accessible.
        let v5 = arena
            .get(indices[4])
            .expect("bead_id=bd-zcdn: version at horizon begin_seq must never be reclaimed");
        assert_eq!(v5.commit_seq, CommitSeq::new(5));

        // Versions 6-10 must also still be accessible.
        for (seq, idx) in indices.iter().enumerate().skip(5).take(5) {
            assert!(
                arena.get(*idx).is_some(),
                "bead_id={BEAD_ZCDN} version at seq {} must be retained",
                seq + 1
            );
        }
    }

    #[test]
    fn test_gc_memory_bounded() {
        // bd-bca.2: sustained history is bounded by active horizon after pruning.
        let mut arena = VersionArena::new();
        let chain_heads = ChainHeadTable::new();

        for page in 1_u32..=64 {
            let pgno = PageNumber::new(page).expect("page number in range");
            build_chain_in_table(&mut arena, &chain_heads, pgno, 1_000);
        }

        let mut todo = GcTodo::new();
        for page in 1_u32..=64 {
            todo.enqueue(PageNumber::new(page).expect("page number in range"));
        }

        // Keep only the newest 16 versions (active window) per page.
        let active_window = 16_u64;
        let horizon = CommitSeq::new(1_000 - active_window + 1);
        let mut total_freed = 0_u32;
        while !todo.is_empty() {
            let result = gc_tick(&mut todo, horizon, &mut arena, &chain_heads);
            total_freed = total_freed.saturating_add(result.versions_freed);
        }

        let expected_freed =
            u32::try_from(64_usize * usize::try_from(1_000_u64 - active_window).unwrap())
                .expect("expected freed versions fits u32");
        assert_eq!(
            total_freed, expected_freed,
            "GC should prune obsolete history and keep only active-window versions"
        );
    }

    #[test]
    fn test_gc_version_chain_length() {
        // bd-bca.2: chain length should be bounded by active transactions + 1 after prune.
        let mut arena = VersionArena::new();
        let pgno = PageNumber::new(777).unwrap();
        let chain_heads = ChainHeadTable::new();
        build_chain_in_table(&mut arena, &chain_heads, pgno, 40);

        let active_txns = 7_u64;
        let keep = active_txns + 1;
        let horizon = CommitSeq::new(40 - keep + 1);
        let result = prune_page_chain(pgno, horizon, &mut arena, &chain_heads);
        let retained_len =
            40usize.saturating_sub(usize::try_from(result.freed).expect("u32 fits usize"));
        assert!(
            retained_len <= usize::try_from(active_txns + 1).unwrap(),
            "retained chain length {} must be <= active_txns+1 ({})",
            retained_len,
            active_txns + 1
        );
    }

    // -----------------------------------------------------------------------
    // bd-2y306.5: property tests for EBR/GC invariants under varied schedules
    // -----------------------------------------------------------------------

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 10_000,
            .. ProptestConfig::default()
        })]

        #[test]
        fn prop_gc_prune_preserves_horizon_visibility_and_expected_freed(
            n in 1_u32..257,
            horizon in 0_u64..301,
        ) {
            let mut arena = VersionArena::new();
            let pgno = PageNumber::new(900).expect("fixed test pgno should be valid");
            let chain_heads = ChainHeadTable::new();
            build_chain_in_table(&mut arena, &chain_heads, pgno, n);

            let result = prune_page_chain(pgno, CommitSeq::new(horizon), &mut arena, &chain_heads);

            let n_u64 = u64::from(n);
            let expected_freed_u64 = if horizon == 0 {
                0
            } else {
                horizon.saturating_sub(1).min(n_u64.saturating_sub(1))
            };
            let expected_freed = u32::try_from(expected_freed_u64).expect("bounded by n<=256");
            prop_assert_eq!(result.freed, expected_freed);

            // Keep floor is the oldest commit_seq still required by visibility.
            let keep_floor = if horizon == 0 { 1 } else { horizon.min(n_u64) };
            let retained = collect_chain_commit_seqs(pgno, &arena, &chain_heads);
            let expected_retained_len = usize::try_from(n.saturating_sub(expected_freed))
                .expect("u32 fits usize");
            prop_assert_eq!(retained.len(), expected_retained_len);
            prop_assert!(!retained.is_empty());
            prop_assert_eq!(retained.iter().copied().min(), Some(keep_floor));
            for seq in &retained {
                prop_assert!(*seq >= keep_floor);
                prop_assert!(*seq <= n_u64);
            }

            for (_, seq) in &result.pruned_keys {
                prop_assert!(seq.get() < keep_floor);
            }
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 2_500,
            .. ProptestConfig::default()
        })]

        #[test]
        fn prop_gc_prune_monotonic_with_increasing_horizon(
            n in 2_u32..257,
            horizon_a in 0_u64..301,
            horizon_b in 0_u64..301,
        ) {
            let low = horizon_a.min(horizon_b);
            let high = horizon_a.max(horizon_b);

            let pgno_step = PageNumber::new(901).expect("fixed test pgno should be valid");
            let mut arena_step = VersionArena::new();
            let chain_heads_step = ChainHeadTable::new();
            build_chain_in_table(&mut arena_step, &chain_heads_step, pgno_step, n);

            let r_low = prune_page_chain(
                pgno_step,
                CommitSeq::new(low),
                &mut arena_step,
                &chain_heads_step,
            );
            let r_high = prune_page_chain(
                pgno_step,
                CommitSeq::new(high),
                &mut arena_step,
                &chain_heads_step,
            );
            let retained_step = collect_chain_commit_seqs(pgno_step, &arena_step, &chain_heads_step);

            let pgno_direct = PageNumber::new(902).expect("fixed test pgno should be valid");
            let mut arena_direct = VersionArena::new();
            let chain_heads_direct = ChainHeadTable::new();
            build_chain_in_table(&mut arena_direct, &chain_heads_direct, pgno_direct, n);

            let r_direct = prune_page_chain(
                pgno_direct,
                CommitSeq::new(high),
                &mut arena_direct,
                &chain_heads_direct,
            );
            let retained_direct =
                collect_chain_commit_seqs(pgno_direct, &arena_direct, &chain_heads_direct);

            prop_assert_eq!(retained_step, retained_direct);
            prop_assert_eq!(r_low.freed.saturating_add(r_high.freed), r_direct.freed);
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 2_500,
            .. ProptestConfig::default()
        })]

        #[test]
        fn prop_gc_chain_length_bounded_by_active_txns_plus_one(
            n in 1_u32..513,
            active_txns in 0_u32..65,
        ) {
            let mut arena = VersionArena::new();
            let pgno = PageNumber::new(903).expect("fixed test pgno should be valid");
            let chain_heads = ChainHeadTable::new();
            build_chain_in_table(&mut arena, &chain_heads, pgno, n);

            let keep_u64 = u64::from(active_txns) + 1;
            let n_u64 = u64::from(n);
            let horizon = if n_u64 > keep_u64 {
                CommitSeq::new(n_u64 - keep_u64 + 1)
            } else {
                CommitSeq::new(0)
            };

            let _ = prune_page_chain(pgno, horizon, &mut arena, &chain_heads);
            let retained = collect_chain_commit_seqs(pgno, &arena, &chain_heads);
            let keep_usize = usize::try_from(keep_u64).expect("bounded");
            prop_assert!(retained.len() <= keep_usize);
        }
    }
}
