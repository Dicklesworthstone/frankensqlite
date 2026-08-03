//! Flat Combining for Commit Sequencing (D3 — bd-3wop3.3).
//!
//! Replaces per-commit `fetch_add(1)` with batched `fetch_add(N)`, reducing
//! cache-line ping-pong from N round-trips to 1. Under 8-16 thread contention,
//! this converts the commit sequencer from a serialization bottleneck into a
//! scalable operation.
//!
//! ## Design (Hendler et al., SPAA 2010)
//!
//! When many threads want to allocate commit sequences:
//! 1. Each thread publishes its request to a per-thread slot
//! 2. One thread wins the combiner lock and becomes the "combiner"
//! 3. The combiner scans all pending slots, counts N requests
//! 4. Single `fetch_add(N)` to get a range `[base, base+N)`
//! 5. Assigns `base+i` to each pending request
//! 6. Non-combiners spin-wait on their slot (usually <1µs)
//!
//! ## Why This Matters
//!
//! At 8 threads doing 1000 commits/sec each:
//! - Before: 8000 `fetch_add(1)` = 8000 cache-line bounces = ~400µs
//! - After:  ~500 batched `fetch_add(N)` = ~500 cache-line bounces = ~25µs
//!
//! The combiner has all data in L1 cache — sequential execution is faster than
//! parallel contention.

use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
#[cfg(feature = "commit-combiner-test-support")]
use std::sync::{Condvar, Mutex as StdMutex, MutexGuard};
#[cfg(feature = "commit-combiner-test-support")]
use std::time::{Duration, Instant as StdInstant};

use smallvec::SmallVec;

use fsqlite_types::CommitSeq;
use fsqlite_types::sync_primitives::{Instant, Mutex};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Maximum threads that can participate in commit combining.
pub const MAX_COMMIT_THREADS: usize = 64;

/// Slot states.
const SLOT_EMPTY: u8 = 0;
const SLOT_PENDING: u8 = 1;
const SLOT_DONE: u8 = 2;

/// Maximum spin iterations before yielding.
const SPIN_BEFORE_YIELD: u32 = 1024;

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

/// Snapshot of commit combining metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub struct CommitCombineMetrics {
    pub batches_total: u64,
    pub ops_total: u64,
    pub batch_size_sum: u64,
    pub batch_size_max: u64,
    pub wait_ns_total: u64,
    pub wait_ns_max: u64,
}

/// Quiescent, instance-local receipt for the feature-gated combiner keeper.
///
/// Callers must join all registered allocation callers before taking this
/// receipt. It proves which public allocation route was used without reaching
/// into combiner slots or process-global counters.
#[cfg(feature = "commit-combiner-test-support")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommitCombineTestReceipt {
    pub next_seq: u64,
    pub metrics: CommitCombineMetrics,
    pub registered_allocations: u64,
    pub one_shot_allocations: u64,
}

#[cfg(feature = "commit-combiner-test-support")]
struct CommitCombineTestMetricRecorder {
    registered_allocations: AtomicU64,
    one_shot_allocations: AtomicU64,
}

#[cfg(feature = "commit-combiner-test-support")]
impl CommitCombineTestMetricRecorder {
    const fn new() -> Self {
        Self {
            registered_allocations: AtomicU64::new(0),
            one_shot_allocations: AtomicU64::new(0),
        }
    }
}

/// Snapshot of a completed deterministic staging phase.
#[cfg(feature = "commit-combiner-test-support")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommitCombineStagingReceipt {
    pub expected_callers: usize,
    pub staged_callers: usize,
}

#[cfg(feature = "commit-combiner-test-support")]
struct CommitCombineStagingState {
    staged_callers: usize,
    released: bool,
}

/// Test-only control that holds real registered callers after they publish a
/// pending allocation and before they attempt the normal combining path.
///
/// It is available only with `commit-combiner-test-support`. Production builds
/// do not contain the controller, its wait path, or its counters.
#[cfg(feature = "commit-combiner-test-support")]
pub struct CommitCombineStagingControl {
    expected_callers: usize,
    state: StdMutex<CommitCombineStagingState>,
    all_staged: Condvar,
    release_waiters: Condvar,
}

#[cfg(feature = "commit-combiner-test-support")]
impl CommitCombineStagingControl {
    /// Create one staging control for exactly `expected_callers` registered
    /// allocation calls.
    #[must_use]
    pub fn new(expected_callers: usize) -> Self {
        assert!(
            expected_callers > 0 && expected_callers <= MAX_COMMIT_THREADS,
            "staging control requires 1..=MAX_COMMIT_THREADS callers"
        );
        Self {
            expected_callers,
            state: StdMutex::new(CommitCombineStagingState {
                staged_callers: 0,
                released: false,
            }),
            all_staged: Condvar::new(),
            release_waiters: Condvar::new(),
        }
    }

    /// Wait until every expected caller has published a real pending request.
    #[must_use]
    pub fn wait_until_all_staged(&self, timeout: Duration) -> bool {
        let deadline = StdInstant::now() + timeout;
        let mut state = self.lock_state();
        while state.staged_callers < self.expected_callers {
            let Some(remaining) = deadline.checked_duration_since(StdInstant::now()) else {
                return false;
            };
            let (next_state, timeout_result) = self
                .all_staged
                .wait_timeout(state, remaining)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state = next_state;
            if timeout_result.timed_out() && state.staged_callers < self.expected_callers {
                return false;
            }
        }
        true
    }

    /// Release an entirely staged batch into the existing combiner path.
    #[must_use]
    pub fn release_when_all_staged(&self) -> CommitCombineStagingReceipt {
        let mut state = self.lock_state();
        assert_eq!(
            state.staged_callers, self.expected_callers,
            "cannot release a partial staged batch"
        );
        state.released = true;
        self.release_waiters.notify_all();
        CommitCombineStagingReceipt {
            expected_callers: self.expected_callers,
            staged_callers: state.staged_callers,
        }
    }

    /// Create an RAII fallback that releases waiters if a keeper assertion
    /// fails before the normal release point.
    #[must_use]
    pub fn release_guard(self: &Arc<Self>) -> CommitCombineStagingReleaseGuard {
        CommitCombineStagingReleaseGuard {
            control: Arc::clone(self),
        }
    }

    fn lock_state(&self) -> MutexGuard<'_, CommitCombineStagingState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn stage_registered_call(&self) {
        let mut state = self.lock_state();
        if state.released {
            return;
        }
        assert!(
            state.staged_callers < self.expected_callers,
            "staging control received more callers than configured"
        );
        state.staged_callers += 1;
        if state.staged_callers == self.expected_callers {
            self.all_staged.notify_all();
        }
        while !state.released {
            state = self
                .release_waiters
                .wait(state)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }

    fn release_for_drop(&self) {
        let mut state = self.lock_state();
        state.released = true;
        self.release_waiters.notify_all();
    }
}

/// RAII release for a [`CommitCombineStagingControl`] keeper.
#[cfg(feature = "commit-combiner-test-support")]
pub struct CommitCombineStagingReleaseGuard {
    control: Arc<CommitCombineStagingControl>,
}

#[cfg(feature = "commit-combiner-test-support")]
impl Drop for CommitCombineStagingReleaseGuard {
    fn drop(&mut self) {
        self.control.release_for_drop();
    }
}

/// Per-combiner metric recorder.
///
/// Keeping these counters beside their owning combiner makes a test or
/// diagnostic snapshot independent of unrelated database instances. The
/// recording work is unchanged from the former process-global counters.
struct CommitCombineMetricRecorder {
    batches_total: AtomicU64,
    ops_total: AtomicU64,
    batch_size_sum: AtomicU64,
    batch_size_max: AtomicU64,
    wait_ns_total: AtomicU64,
    wait_ns_max: AtomicU64,
}

impl CommitCombineMetricRecorder {
    const fn new() -> Self {
        Self {
            batches_total: AtomicU64::new(0),
            ops_total: AtomicU64::new(0),
            batch_size_sum: AtomicU64::new(0),
            batch_size_max: AtomicU64::new(0),
            wait_ns_total: AtomicU64::new(0),
            wait_ns_max: AtomicU64::new(0),
        }
    }

    fn snapshot(&self) -> CommitCombineMetrics {
        CommitCombineMetrics {
            batches_total: self.batches_total.load(Ordering::Relaxed),
            ops_total: self.ops_total.load(Ordering::Relaxed),
            batch_size_sum: self.batch_size_sum.load(Ordering::Relaxed),
            batch_size_max: self.batch_size_max.load(Ordering::Relaxed),
            wait_ns_total: self.wait_ns_total.load(Ordering::Relaxed),
            wait_ns_max: self.wait_ns_max.load(Ordering::Relaxed),
        }
    }

    fn record_wait(&self, elapsed_ns: u64) {
        self.wait_ns_total.fetch_add(elapsed_ns, Ordering::Relaxed);
        update_max(&self.wait_ns_max, elapsed_ns);
    }

    fn record_batch(&self, pending_count: u64) {
        self.batches_total.fetch_add(1, Ordering::Relaxed);
        self.ops_total.fetch_add(pending_count, Ordering::Relaxed);
        self.batch_size_sum
            .fetch_add(pending_count, Ordering::Relaxed);
        update_max(&self.batch_size_max, pending_count);
    }
}

fn update_max(metric: &AtomicU64, val: u64) {
    let mut prev = metric.load(Ordering::Relaxed);
    while val > prev {
        match metric.compare_exchange_weak(prev, val, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(actual) => prev = actual,
        }
    }
}

// ---------------------------------------------------------------------------
// CommitSlot
// ---------------------------------------------------------------------------

/// Cache-line aligned commit slot (64 bytes).
///
/// Uses atomic operations for both state and result to avoid `unsafe` code.
/// The state field encodes the slot state in the high bits and reserves
/// low bits for future extensions.
#[repr(align(64))]
struct CommitSlot {
    /// Slot state: EMPTY, PENDING, or DONE.
    state: AtomicU8,
    /// Padding to separate state from result (avoid false sharing).
    _pad1: [u8; 7],
    /// Result: the allocated CommitSeq (valid when state == DONE).
    result: AtomicU64,
    /// Padding to fill cache line.
    _pad2: [u8; 48],
}

impl CommitSlot {
    const fn new() -> Self {
        Self {
            state: AtomicU8::new(SLOT_EMPTY),
            _pad1: [0; 7],
            result: AtomicU64::new(0),
            _pad2: [0; 48],
        }
    }
}

// ---------------------------------------------------------------------------
// CommitSequenceCombiner
// ---------------------------------------------------------------------------

/// Flat combining commit sequence allocator.
///
/// Batches multiple `alloc_commit_seq` requests into a single `fetch_add(N)`,
/// reducing cache-line contention from O(N) round-trips to O(1).
pub struct CommitSequenceCombiner {
    /// The next commit sequence to allocate.
    next_commit_seq: AtomicU64,
    /// Per-thread slots for request/result exchange.
    slots: [CommitSlot; MAX_COMMIT_THREADS],
    /// Slot ownership: 0 = free, non-zero = occupied by thread.
    owners: [AtomicU64; MAX_COMMIT_THREADS],
    /// Combiner lock — only one thread processes a batch at a time.
    combiner_lock: Mutex<()>,
    /// Optional active-commits registry for batch-registering allocated sequences.
    /// When set, `combine_locked` batch-pushes all newly allocated sequences into
    /// this registry before signaling waiters, ensuring sequences are registered
    /// as active atomically with allocation (no gap for `finish_commit_seq`).
    active_registry: Option<Arc<Mutex<SmallVec<[u64; 16]>>>>,
    /// Instance-local observability for this combiner's batches and wait time.
    metrics: CommitCombineMetricRecorder,
    /// Deterministic staging exists only for test-support consumers.
    #[cfg(feature = "commit-combiner-test-support")]
    staging_control: Option<Arc<CommitCombineStagingControl>>,
    /// Test-only route counters make a keeper receipt self-contained.
    #[cfg(feature = "commit-combiner-test-support")]
    test_metrics: CommitCombineTestMetricRecorder,
}

impl CommitSequenceCombiner {
    /// Create a new combiner starting from the given initial commit sequence.
    pub fn new(initial_commit_seq: u64) -> Self {
        Self {
            next_commit_seq: AtomicU64::new(initial_commit_seq),
            slots: std::array::from_fn(|_| CommitSlot::new()),
            owners: std::array::from_fn(|_| AtomicU64::new(0)),
            combiner_lock: Mutex::new(()),
            active_registry: None,
            metrics: CommitCombineMetricRecorder::new(),
            #[cfg(feature = "commit-combiner-test-support")]
            staging_control: None,
            #[cfg(feature = "commit-combiner-test-support")]
            test_metrics: CommitCombineTestMetricRecorder::new(),
        }
    }

    /// Create a combiner with active-commits batch registration.
    ///
    /// When `registry` is provided, `combine_locked` batch-pushes all newly
    /// allocated sequences into it before signaling waiters. This ensures
    /// `finish_commit_seq` cannot advance `stable_commit_seq` past an
    /// allocated-but-unregistered sequence.
    pub fn new_with_registry(
        initial_commit_seq: u64,
        registry: Arc<Mutex<SmallVec<[u64; 16]>>>,
    ) -> Self {
        Self {
            next_commit_seq: AtomicU64::new(initial_commit_seq),
            slots: std::array::from_fn(|_| CommitSlot::new()),
            owners: std::array::from_fn(|_| AtomicU64::new(0)),
            combiner_lock: Mutex::new(()),
            active_registry: Some(registry),
            metrics: CommitCombineMetricRecorder::new(),
            #[cfg(feature = "commit-combiner-test-support")]
            staging_control: None,
            #[cfg(feature = "commit-combiner-test-support")]
            test_metrics: CommitCombineTestMetricRecorder::new(),
        }
    }

    /// Create a combiner whose registered callers can be staged by the
    /// feature-gated test controller before entering the existing combine path.
    #[cfg(feature = "commit-combiner-test-support")]
    #[must_use]
    pub fn new_with_staging_control(
        initial_commit_seq: u64,
        staging_control: Arc<CommitCombineStagingControl>,
    ) -> Self {
        let mut combiner = Self::new(initial_commit_seq);
        combiner.staging_control = Some(staging_control);
        combiner
    }

    /// Register a thread. Returns a handle with an assigned slot,
    /// or `None` if all slots are occupied.
    pub fn register(&self) -> Option<CommitCombineHandle<'_>> {
        let tid = thread_id_hash();
        for i in 0..MAX_COMMIT_THREADS {
            if self.owners[i]
                .compare_exchange(0, tid, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                return Some(CommitCombineHandle {
                    combiner: self,
                    slot: i,
                });
            }
        }
        None
    }

    /// Current next_commit_seq value (for diagnostics).
    ///
    /// Uses `Acquire` to pair with the `AcqRel` `fetch_add` in
    /// `combine_locked`, so a reader synchronizes-with the latest allocation
    /// rather than observing a stale epoch (bd-707lc). The one correctness
    /// consumer (`TxnManager::finish_commit_seq`) additionally reads this while
    /// holding the `active_commits` lock, which the combiner now also holds
    /// across the allocation; the `Acquire` here keeps unlocked diagnostic
    /// reads honest too.
    #[must_use]
    pub fn next_seq(&self) -> u64 {
        self.next_commit_seq.load(Ordering::Acquire)
    }

    /// Number of registered threads.
    #[must_use]
    pub fn active_threads(&self) -> usize {
        self.owners
            .iter()
            .filter(|o| o.load(Ordering::Relaxed) != 0)
            .count()
    }

    /// Snapshot metric counters for this combiner instance only.
    #[must_use]
    pub fn metrics(&self) -> CommitCombineMetrics {
        self.metrics.snapshot()
    }

    /// Return a quiescent, instance-local keeper receipt.
    ///
    /// Callers must join all allocation callers before sampling it.
    #[cfg(feature = "commit-combiner-test-support")]
    #[must_use]
    pub fn test_support_receipt(&self) -> CommitCombineTestReceipt {
        CommitCombineTestReceipt {
            next_seq: self.next_seq(),
            metrics: self.metrics(),
            registered_allocations: self.test_metrics.registered_allocations.load(Ordering::Acquire),
            one_shot_allocations: self.test_metrics.one_shot_allocations.load(Ordering::Acquire),
        }
    }

    /// Claim a temporary slot for one-shot allocation.
    fn claim_slot(&self, tid: u64) -> usize {
        loop {
            for i in 0..MAX_COMMIT_THREADS {
                if self.owners[i]
                    .compare_exchange(0, tid, Ordering::AcqRel, Ordering::Relaxed)
                    .is_ok()
                {
                    return i;
                }
            }
            // All 64 slots occupied; yield and retry. Under normal operation
            // (≤64 concurrent committers) this loop terminates on the first scan.
            std::thread::yield_now();
        }
    }

    /// One-shot commit sequence allocation via flat combining.
    ///
    /// Claims a temporary slot, submits a request, waits for the combiner
    /// to process it, and releases the slot. This avoids the need for
    /// pre-registered handles when the caller doesn't maintain per-thread state.
    ///
    /// If `active_registry` is set, the allocated sequence is batch-registered
    /// by the combiner thread before this method returns, ensuring no gap
    /// for `finish_commit_seq`.
    pub fn alloc_one_shot(&self) -> CommitSeq {
        let start = Instant::now();
        let tid = thread_id_hash();
        let slot = self.claim_slot(tid);

        // Publish request.
        self.slots[slot]
            .state
            .store(SLOT_PENDING, Ordering::Release);

        // Try to become the combiner.
        if let Some(_guard) = self.combiner_lock.try_lock() {
            self.combine_locked();
        }

        // Wait for result.
        let mut spins = 0u32;
        let seq = loop {
            let state = self.slots[slot].state.load(Ordering::Acquire);
            if state == SLOT_DONE {
                let raw = self.slots[slot].result.load(Ordering::Acquire);
                self.slots[slot].state.store(SLOT_EMPTY, Ordering::Release);
                break CommitSeq::new(raw);
            }

            spins += 1;
            if spins < SPIN_BEFORE_YIELD {
                std::hint::spin_loop();
            } else {
                if let Some(_guard) = self.combiner_lock.try_lock() {
                    self.combine_locked();
                } else {
                    std::thread::yield_now();
                }
                spins = 0;
            }
        };

        // Release slot ownership.
        self.owners[slot].store(0, Ordering::Release);

        #[allow(clippy::cast_possible_truncation)]
        let elapsed_ns = start.elapsed().as_nanos() as u64;
        self.metrics.record_wait(elapsed_ns);

        #[cfg(feature = "commit-combiner-test-support")]
        self.test_metrics
            .one_shot_allocations
            .fetch_add(1, Ordering::Relaxed);

        seq
    }

    /// Process all pending requests in a single batch.
    /// The caller MUST hold the `combiner_lock`.
    fn combine_locked(&self) {
        // Count pending requests.
        let mut pending_count = 0u64;
        let mut pending_slots = [false; MAX_COMMIT_THREADS];

        for (slot, is_pending) in self.slots.iter().zip(pending_slots.iter_mut()) {
            let state = slot.state.load(Ordering::Acquire);
            if state == SLOT_PENDING {
                *is_pending = true;
                pending_count += 1;
            }
        }

        if pending_count == 0 {
            return;
        }

        // Single batched fetch_add for all pending requests.
        //
        // bd-707lc: When an active-commits registry is present, the allocation
        // (`fetch_add` on `next_commit_seq`) and the registration of those
        // sequences into `active_commits` MUST be atomic with respect to
        // `TxnManager::finish_commit_seq`, which holds the same `active_commits`
        // lock and — when the active list drains to empty — advances
        // `stable_commit_seq` to `next_commit_seq - 1`. If the `fetch_add` ran
        // outside the registry lock (as it previously did), a concurrent
        // finisher could empty the active list and then observe a
        // `next_commit_seq` that already covers freshly-allocated but
        // not-yet-registered (in-flight) sequences, advancing the stable
        // watermark past uncommitted commits and making a partial commit
        // visible to new snapshots (INV-6 violation). Doing the `fetch_add`
        // under the lock closes that window: whenever a finisher sees the list
        // empty, every allocation reflected in `next_commit_seq` has already
        // been registered-then-finished, so `next_commit_seq - 1` is genuinely
        // stable. (No-registry path is unit-test only; production
        // `TxnManager::new` always supplies the registry.)
        let base_seq = if let Some(ref registry) = self.active_registry {
            let mut active = registry.lock();
            let base = self
                .next_commit_seq
                .fetch_add(pending_count, Ordering::AcqRel);
            for i in 0..pending_count {
                active.push(base + i);
            }
            base
        } else {
            self.next_commit_seq
                .fetch_add(pending_count, Ordering::AcqRel)
        };
        debug_assert!(
            base_seq < u64::MAX - pending_count,
            "CommitSeq allocation overflow"
        );

        // Assign sequences to each pending slot.
        let mut assigned = 0u64;
        for (slot, is_pending) in self.slots.iter().zip(pending_slots.iter()) {
            if *is_pending {
                let seq = base_seq + assigned;
                assigned += 1;

                // Store result first, then mark as DONE.
                // The slot owner reads only after observing state == DONE.
                slot.result.store(seq, Ordering::Release);
                slot.state.store(SLOT_DONE, Ordering::Release);
            }
        }

        debug_assert_eq!(assigned, pending_count);

        // Update metrics.
        self.metrics.record_batch(pending_count);

        tracing::debug!(
            target: "fsqlite.commit_combine",
            batch_size = pending_count,
            base_seq,
            "commit_combine_batch"
        );
    }
}

#[allow(clippy::missing_fields_in_debug)]
impl std::fmt::Debug for CommitSequenceCombiner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CommitSequenceCombiner")
            .field("next_seq", &self.next_seq())
            .field("active_threads", &self.active_threads())
            .finish_non_exhaustive()
    }
}

// ---------------------------------------------------------------------------
// CommitCombineHandle
// ---------------------------------------------------------------------------

/// Per-thread handle for commit sequence allocation.
/// Automatically unregisters on drop.
pub struct CommitCombineHandle<'a> {
    combiner: &'a CommitSequenceCombiner,
    slot: usize,
}

impl CommitCombineHandle<'_> {
    /// Allocate the next commit sequence using flat combining.
    ///
    /// Either this thread becomes the combiner and processes all pending
    /// requests, or it waits for the active combiner to process its request.
    pub fn alloc_commit_seq(&self) -> CommitSeq {
        let start = Instant::now();

        // Publish our request.
        self.combiner.slots[self.slot]
            .state
            .store(SLOT_PENDING, Ordering::Release);

        #[cfg(feature = "commit-combiner-test-support")]
        if let Some(staging_control) = &self.combiner.staging_control {
            staging_control.stage_registered_call();
        }

        // Try to become the combiner.
        if let Some(_guard) = self.combiner.combiner_lock.try_lock() {
            self.combiner.combine_locked();
        }

        // Wait for our result.
        let mut spins = 0u32;
        loop {
            let state = self.combiner.slots[self.slot].state.load(Ordering::Acquire);
            if state == SLOT_DONE {
                // Result ready — read and clear slot.
                // The combiner stored the result with Release before setting DONE.
                let seq = self.combiner.slots[self.slot]
                    .result
                    .load(Ordering::Acquire);
                self.combiner.slots[self.slot]
                    .state
                    .store(SLOT_EMPTY, Ordering::Release);

                #[allow(clippy::cast_possible_truncation)]
                let elapsed_ns = start.elapsed().as_nanos() as u64;
                self.combiner.metrics.record_wait(elapsed_ns);

                #[cfg(feature = "commit-combiner-test-support")]
                self.combiner
                    .test_metrics
                    .registered_allocations
                    .fetch_add(1, Ordering::Relaxed);

                return CommitSeq::new(seq);
            }

            // Still waiting. Spin or yield.
            spins += 1;
            if spins < SPIN_BEFORE_YIELD {
                std::hint::spin_loop();
            } else {
                // If the combiner is slow, try to take over.
                if let Some(_guard) = self.combiner.combiner_lock.try_lock() {
                    self.combiner.combine_locked();
                } else {
                    std::thread::yield_now();
                }
                spins = 0;
            }
        }
    }

    /// Slot index (for diagnostics).
    #[must_use]
    pub fn slot(&self) -> usize {
        self.slot
    }
}

impl Drop for CommitCombineHandle<'_> {
    fn drop(&mut self) {
        // Clear slot state and release ownership.
        self.combiner.slots[self.slot]
            .state
            .store(SLOT_EMPTY, Ordering::Release);
        self.combiner.owners[self.slot].store(0, Ordering::Release);
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Generate a unique non-zero thread ID hash.
fn thread_id_hash() -> u64 {
    let t = std::thread::current().id();
    let s = format!("{t:?}");
    let mut h = 1u64;
    for b in s.bytes() {
        h = h.wrapping_mul(31).wrapping_add(u64::from(b));
    }
    if h == 0 { 1 } else { h }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Barrier};
    use std::thread;

    #[test]
    fn test_combiner_single_thread() {
        let combiner = CommitSequenceCombiner::new(100);
        let handle = combiner.register().unwrap();

        let seq1 = handle.alloc_commit_seq();
        assert_eq!(seq1.get(), 100);

        let seq2 = handle.alloc_commit_seq();
        assert_eq!(seq2.get(), 101);

        let seq3 = handle.alloc_commit_seq();
        assert_eq!(seq3.get(), 102);

        drop(handle);
        assert_eq!(combiner.next_seq(), 103);
    }

    #[test]
    fn test_combiner_private_staged_batch_records_one_batch() {
        let combiner = CommitSequenceCombiner::new(100);

        // This is a private unit-level setup, not production-shaped staged
        // control. It avoids scheduler-dependent publication races while
        // checking how one already-staged batch is recorded.
        for slot in combiner.slots.iter().take(8) {
            slot.state.store(SLOT_PENDING, Ordering::Release);
        }
        let guard = combiner.combiner_lock.lock();
        combiner.combine_locked();
        drop(guard);

        let metrics = combiner.metrics();
        assert_eq!(metrics.ops_total, 8);
        assert_eq!(metrics.batches_total, 1);
        assert_eq!(metrics.batch_size_sum, 8);
        assert_eq!(metrics.batch_size_max, 8);
        assert_eq!(combiner.next_seq(), 108);

        // The private batch uses one sequence allocation for eight requests.
        // Public e2e release gates remain blocked on production-shaped staged
        // control and a receipt that external gates can consume.
        assert!(metrics.batches_total < metrics.ops_total);
    }

    #[test]
    fn test_combiner_metrics_are_instance_local() {
        let active = CommitSequenceCombiner::new(0);
        let untouched = CommitSequenceCombiner::new(0);

        active.alloc_one_shot();

        assert_eq!(active.metrics().ops_total, 1);
        assert_eq!(
            untouched.metrics(),
            CommitCombineMetrics {
                batches_total: 0,
                ops_total: 0,
                batch_size_sum: 0,
                batch_size_max: 0,
                wait_ns_total: 0,
                wait_ns_max: 0,
            }
        );
    }

    #[test]
    fn test_combiner_8t_all_commits_succeed() {
        let combiner = Arc::new(CommitSequenceCombiner::new(1000));
        let barrier = Arc::new(Barrier::new(8));
        let mut handles = Vec::new();

        for _ in 0..8 {
            let c = Arc::clone(&combiner);
            let b = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                let h = c.register().unwrap();
                b.wait(); // Synchronize start

                let mut seqs = Vec::new();
                for _ in 0..100 {
                    seqs.push(h.alloc_commit_seq().get());
                }
                drop(h);
                seqs
            }));
        }

        let mut all_seqs = Vec::new();
        for h in handles {
            all_seqs.extend(h.join().unwrap());
        }

        // All sequences should be unique.
        all_seqs.sort();
        let unique_count = all_seqs.len();
        all_seqs.dedup();
        assert_eq!(
            all_seqs.len(),
            unique_count,
            "all commit sequences must be unique"
        );

        // Total should be 8 threads * 100 commits = 800.
        assert_eq!(all_seqs.len(), 800);

        // Sequences should be in range [1000, 1800).
        assert!(all_seqs.iter().all(|&s| s >= 1000 && s < 1800));

        // The combiner should have advanced by 800.
        assert_eq!(combiner.next_seq(), 1800);
    }

    #[test]
    fn test_combiner_16t_throughput() {
        let combiner = Arc::new(CommitSequenceCombiner::new(0));
        let barrier = Arc::new(Barrier::new(16));
        let mut handles = Vec::new();

        let start = Instant::now();

        for _ in 0..16 {
            let c = Arc::clone(&combiner);
            let b = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                let h = c.register().unwrap();
                b.wait();

                for _ in 0..1000 {
                    h.alloc_commit_seq();
                }
                drop(h);
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        let elapsed = start.elapsed();

        // 16 threads * 1000 commits = 16000 total.
        assert_eq!(combiner.next_seq(), 16000);

        // Should complete reasonably fast (< 1 second for 16000 ops).
        assert!(
            elapsed.as_millis() < 1000,
            "16000 commits took {}ms, expected < 1000ms",
            elapsed.as_millis()
        );
    }

    #[test]
    fn test_combiner_cache_line_padding() {
        // Verify slot is cache-line aligned (64 bytes).
        assert_eq!(
            std::mem::align_of::<CommitSlot>(),
            64,
            "CommitSlot must be 64-byte aligned"
        );
        assert_eq!(
            std::mem::size_of::<CommitSlot>(),
            64,
            "CommitSlot must be exactly 64 bytes"
        );
    }

    #[test]
    fn test_combiner_batch_size_varies() {
        // Test that different batch sizes are handled correctly.
        let combiner = Arc::new(CommitSequenceCombiner::new(0));

        // Single commit.
        {
            let h = combiner.register().unwrap();
            h.alloc_commit_seq();
            drop(h);
        }
        assert_eq!(combiner.next_seq(), 1);

        // 4 concurrent commits.
        {
            let barrier = Arc::new(Barrier::new(4));
            let mut handles = Vec::new();
            for _ in 0..4 {
                let c = Arc::clone(&combiner);
                let b = Arc::clone(&barrier);
                handles.push(thread::spawn(move || {
                    let h = c.register().unwrap();
                    b.wait();
                    h.alloc_commit_seq();
                    drop(h);
                }));
            }
            for h in handles {
                h.join().unwrap();
            }
        }

        // Final state should have 5 total sequences allocated.
        assert_eq!(combiner.next_seq(), 5);
    }

    #[test]
    fn test_combiner_fairness() {
        // Verify no thread starves (all threads get commits within reasonable time).
        let combiner = Arc::new(CommitSequenceCombiner::new(0));
        let barrier = Arc::new(Barrier::new(8));
        let mut handles = Vec::new();

        for tid in 0..8u64 {
            let c = Arc::clone(&combiner);
            let b = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                let h = c.register().unwrap();
                b.wait();

                let start = Instant::now();
                let mut max_wait_ns = 0u64;

                for _ in 0..50 {
                    let op_start = Instant::now();
                    h.alloc_commit_seq();
                    #[allow(clippy::cast_possible_truncation)]
                    let wait = op_start.elapsed().as_nanos() as u64;
                    max_wait_ns = max_wait_ns.max(wait);
                }

                let total = start.elapsed();
                drop(h);
                (tid, max_wait_ns, total)
            }));
        }

        for h in handles {
            let (tid, max_wait_ns, total) = h.join().unwrap();
            // No single op should take more than 10ms (very generous).
            assert!(
                max_wait_ns < 10_000_000,
                "thread {tid} max wait {max_wait_ns}ns > 10ms"
            );
            // Total should complete in reasonable time.
            assert!(
                total.as_millis() < 500,
                "thread {tid} total time {}ms > 500ms",
                total.as_millis()
            );
        }
    }
}
