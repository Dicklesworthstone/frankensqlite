//! RCU/QSBR for lock-free metadata hot paths (§14.8).
//!
//! Userspace read-copy-update with quiescent-state-based reclamation for
//! metadata that is read on every query but updated rarely (schema cache,
//! configuration, statistics).  Readers pay zero overhead beyond a single
//! atomic load; writers serialize, publish new values, and wait for a grace
//! period before reusing old storage.
//!
//! ## QSBR Protocol
//!
//! Each participating thread registers a slot in the [`QsbrRegistry`].
//! Between read-side critical sections, threads call
//! [`QsbrHandle::quiescent`] to announce they are not holding references
//! to any RCU-protected data.  A grace period completes when every
//! registered thread has announced at least one quiescent state after
//! the writer advanced the global epoch.
//!
//! ## Read-side API
//!
//! ```text
//! handle.quiescent();                // announce "not reading"
//! let v = cell.read();               // zero-overhead atomic load
//! // ... use v ...
//! handle.quiescent();                // done reading
//! ```
//!
//! ## Writer-side API
//!
//! Writers that also hold a [`QsbrHandle`] must use
//! [`QsbrHandle::synchronize_as_writer`] (or pass the handle to
//! [`RcuPair::publish`] / [`RcuTriple::publish`]).  This auto-quiesces
//! the caller's slot at the new epoch so `synchronize` does not deadlock.
//!
//! ## Safety
//!
//! No `UnsafeCell` or `unsafe` blocks — all state uses `AtomicU64`.
//!
//! ## Tracing & Metrics
//!
//! - **Target**: `fsqlite.rcu`
//!   - `TRACE`: read-side entry/exit
//!   - `DEBUG`: grace period completions with `grace_period_us`, `objects_reclaimed`
//! - **Metrics**:
//!   - `fsqlite_rcu_grace_periods_total`
//!   - `fsqlite_rcu_grace_period_duration_ns_total`
//!   - `fsqlite_rcu_grace_period_duration_ns_max`
//!   - `fsqlite_rcu_reclaimed_total`

use fsqlite_types::{CommitSeq, sync_primitives::Instant};
use smallvec::SmallVec;
use std::sync::atomic::{AtomicU64, Ordering};

use fsqlite_types::sync_primitives::Mutex;
use serde::Serialize;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Maximum concurrent threads tracked by QSBR.
pub const MAX_RCU_THREADS: usize = 64;

/// Epoch value indicating an inactive (unregistered) slot.
const INACTIVE_EPOCH: u64 = 0;

/// Maximum spin iterations before yielding during grace period wait.
const SPIN_BEFORE_YIELD: u32 = 1024;

// ---------------------------------------------------------------------------
// Global metrics
// ---------------------------------------------------------------------------

static FSQLITE_RCU_GRACE_PERIODS_TOTAL: AtomicU64 = AtomicU64::new(0);
static FSQLITE_RCU_GRACE_PERIOD_DURATION_NS_TOTAL: AtomicU64 = AtomicU64::new(0);
static FSQLITE_RCU_GRACE_PERIOD_DURATION_NS_MAX: AtomicU64 = AtomicU64::new(0);
static FSQLITE_RCU_RECLAIMED_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Snapshot of RCU metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct RcuMetrics {
    pub fsqlite_rcu_grace_periods_total: u64,
    pub fsqlite_rcu_grace_period_duration_ns_total: u64,
    pub fsqlite_rcu_grace_period_duration_ns_max: u64,
    pub fsqlite_rcu_reclaimed_total: u64,
}

/// Read current RCU metrics.
#[must_use]
pub fn rcu_metrics() -> RcuMetrics {
    RcuMetrics {
        fsqlite_rcu_grace_periods_total: FSQLITE_RCU_GRACE_PERIODS_TOTAL.load(Ordering::Relaxed),
        fsqlite_rcu_grace_period_duration_ns_total: FSQLITE_RCU_GRACE_PERIOD_DURATION_NS_TOTAL
            .load(Ordering::Relaxed),
        fsqlite_rcu_grace_period_duration_ns_max: FSQLITE_RCU_GRACE_PERIOD_DURATION_NS_MAX
            .load(Ordering::Relaxed),
        fsqlite_rcu_reclaimed_total: FSQLITE_RCU_RECLAIMED_TOTAL.load(Ordering::Relaxed),
    }
}

/// Reset metrics (for tests).
pub fn reset_rcu_metrics() {
    FSQLITE_RCU_GRACE_PERIODS_TOTAL.store(0, Ordering::Relaxed);
    FSQLITE_RCU_GRACE_PERIOD_DURATION_NS_TOTAL.store(0, Ordering::Relaxed);
    FSQLITE_RCU_GRACE_PERIOD_DURATION_NS_MAX.store(0, Ordering::Relaxed);
    FSQLITE_RCU_RECLAIMED_TOTAL.store(0, Ordering::Relaxed);
}

/// Record reclaimed objects (for use after grace period).
pub fn record_rcu_reclaimed(count: u64) {
    FSQLITE_RCU_RECLAIMED_TOTAL.fetch_add(count, Ordering::Relaxed);
}

// ---------------------------------------------------------------------------
// Active transaction snapshot-table prototype
// ---------------------------------------------------------------------------

/// Representative capacity for the E3.3 active-transaction snapshot-table
/// prototype.
///
/// This intentionally matches the soft writer cap in
/// `begin_concurrent.rs::MAX_CONCURRENT_WRITERS` without coupling the RCU
/// helper module to the transaction-protocol module.
pub const MAX_ACTIVE_TXN_SNAPSHOT_ENTRIES: usize = 128;

/// Immutable entry published through [`RcuActiveTxnSnapshotTable`].
///
/// This models the active portion of
/// `begin_concurrent.rs::ConcurrentRegistry::{active_snapshot_highs,gc_horizon_counts}`:
/// readers bind one immutable view of `(session_id, snapshot_high)` pairs plus
/// the GC horizon, while per-session mutable leaves remain elsewhere.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActiveTxnSnapshotEntry {
    pub session_id: u64,
    pub snapshot_high: CommitSeq,
}

/// Owned point-in-time image returned by [`RcuActiveTxnSnapshotTable::snapshot`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActiveTxnSnapshotImage {
    pub generation: u64,
    pub gc_horizon: Option<CommitSeq>,
    pub entries: SmallVec<[ActiveTxnSnapshotEntry; 8]>,
}

impl ActiveTxnSnapshotImage {
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

struct ActiveTxnSnapshotSlot {
    session_ids: [AtomicU64; MAX_ACTIVE_TXN_SNAPSHOT_ENTRIES],
    snapshot_highs: [AtomicU64; MAX_ACTIVE_TXN_SNAPSHOT_ENTRIES],
    count: AtomicU64,
    generation: AtomicU64,
    gc_horizon: AtomicU64,
}

impl ActiveTxnSnapshotSlot {
    fn new() -> Self {
        Self {
            session_ids: std::array::from_fn(|_| AtomicU64::new(0)),
            snapshot_highs: std::array::from_fn(|_| AtomicU64::new(0)),
            count: AtomicU64::new(0),
            generation: AtomicU64::new(0),
            gc_horizon: AtomicU64::new(0),
        }
    }

    fn publish_from(
        &self,
        generation: u64,
        entries: &[ActiveTxnSnapshotEntry],
        gc_horizon: Option<CommitSeq>,
    ) {
        for (idx, entry) in entries.iter().enumerate() {
            self.session_ids[idx].store(entry.session_id, Ordering::Release);
            self.snapshot_highs[idx].store(entry.snapshot_high.get(), Ordering::Release);
        }
        self.gc_horizon
            .store(gc_horizon.map_or(0, CommitSeq::get), Ordering::Release);
        self.count.store(entries.len() as u64, Ordering::Release);
        self.generation.store(generation, Ordering::Release);
    }

    fn load_image(&self) -> ActiveTxnSnapshotImage {
        let generation = self.generation.load(Ordering::Acquire);
        let gc_horizon = match self.gc_horizon.load(Ordering::Acquire) {
            0 => None,
            value => Some(CommitSeq::new(value)),
        };
        let count = usize::try_from(self.count.load(Ordering::Acquire))
            .expect("active transaction snapshot count must fit in usize");
        let mut entries = SmallVec::<[ActiveTxnSnapshotEntry; 8]>::with_capacity(count);
        for idx in 0..count {
            entries.push(ActiveTxnSnapshotEntry {
                session_id: self.session_ids[idx].load(Ordering::Acquire),
                snapshot_high: CommitSeq::new(self.snapshot_highs[idx].load(Ordering::Acquire)),
            });
        }
        ActiveTxnSnapshotImage {
            generation,
            gc_horizon,
            entries,
        }
    }
}

/// Double-buffered RCU/QSBR publication prototype for the ActiveTxnRegistry
/// snapshot table selected in Track E3.
///
/// Writers publish a full immutable image into the inactive slot, flip the
/// active slot with a release store, then wait for a QSBR grace period before
/// the old slot becomes eligible for reuse. Readers bind the active slot with a
/// single atomic load and copy out one coherent image without contending with
/// the writer.
pub struct RcuActiveTxnSnapshotTable {
    slot0: ActiveTxnSnapshotSlot,
    slot1: ActiveTxnSnapshotSlot,
    active: AtomicU64,
    next_generation: AtomicU64,
    writer_lock: Mutex<()>,
}

impl RcuActiveTxnSnapshotTable {
    #[must_use]
    pub fn new() -> Self {
        Self {
            slot0: ActiveTxnSnapshotSlot::new(),
            slot1: ActiveTxnSnapshotSlot::new(),
            active: AtomicU64::new(0),
            next_generation: AtomicU64::new(1),
            writer_lock: Mutex::new(()),
        }
    }

    /// Bind the current immutable image.
    ///
    /// Callers are expected to use this within one QSBR read-side critical
    /// section, bounded by `QsbrHandle::quiescent()` calls.
    #[must_use]
    pub fn snapshot(&self) -> ActiveTxnSnapshotImage {
        if self.active.load(Ordering::Acquire) == 0 {
            self.slot0.load_image()
        } else {
            self.slot1.load_image()
        }
    }

    /// Publish a new immutable image and wait for a grace period before the
    /// previously active slot may be reused.
    ///
    /// # Panics
    ///
    /// Panics if `entries.len()` exceeds
    /// [`MAX_ACTIVE_TXN_SNAPSHOT_ENTRIES`].
    pub fn publish(
        &self,
        entries: &[ActiveTxnSnapshotEntry],
        gc_horizon: Option<CommitSeq>,
        handle: &QsbrHandle<'_>,
    ) {
        assert!(
            entries.len() <= MAX_ACTIVE_TXN_SNAPSHOT_ENTRIES,
            "active transaction snapshot image exceeded prototype capacity"
        );
        let _guard = self.writer_lock.lock();
        let current = self.active.load(Ordering::Acquire);
        let next_slot = 1 - current;
        let generation = self.next_generation.fetch_add(1, Ordering::AcqRel);
        if next_slot == 0 {
            self.slot0.publish_from(generation, entries, gc_horizon);
        } else {
            self.slot1.publish_from(generation, entries, gc_horizon);
        }
        self.active.store(next_slot, Ordering::Release);
        handle.synchronize_as_writer();
    }
}

impl Default for RcuActiveTxnSnapshotTable {
    fn default() -> Self {
        Self::new()
    }
}

#[allow(clippy::missing_fields_in_debug)]
impl std::fmt::Debug for RcuActiveTxnSnapshotTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RcuActiveTxnSnapshotTable")
            .field("active", &self.active.load(Ordering::Relaxed))
            .field(
                "next_generation",
                &self.next_generation.load(Ordering::Relaxed),
            )
            .finish_non_exhaustive()
    }
}

// ---------------------------------------------------------------------------
// QsbrRegistry
// ---------------------------------------------------------------------------

/// QSBR thread registry.  Tracks per-thread epochs and coordinates grace
/// period completion.
pub struct QsbrRegistry {
    /// Global epoch — advanced by writers during `synchronize`.
    global_epoch: AtomicU64,
    /// Per-thread epoch slots.  `INACTIVE_EPOCH` (0) means unregistered.
    slots: [AtomicU64; MAX_RCU_THREADS],
    /// Writer serialization.
    writer_lock: Mutex<()>,
}

impl QsbrRegistry {
    /// Create a new registry.  Global epoch starts at 1 (0 is reserved for
    /// inactive slots).
    pub fn new() -> Self {
        Self {
            global_epoch: AtomicU64::new(1),
            slots: std::array::from_fn(|_| AtomicU64::new(INACTIVE_EPOCH)),
            writer_lock: Mutex::new(()),
        }
    }

    /// Register a thread.  Returns a [`QsbrHandle`] with an assigned slot,
    /// or `None` if all slots are occupied.
    pub fn register(&self) -> Option<QsbrHandle<'_>> {
        let ge = self.global_epoch.load(Ordering::Acquire);
        for i in 0..MAX_RCU_THREADS {
            if self.slots[i]
                .compare_exchange(INACTIVE_EPOCH, ge, Ordering::SeqCst, Ordering::Relaxed)
                .is_ok()
            {
                return Some(QsbrHandle {
                    registry: self,
                    slot: i,
                });
            }
        }
        None
    }

    /// Current global epoch (for diagnostics).
    #[must_use]
    pub fn global_epoch(&self) -> u64 {
        self.global_epoch.load(Ordering::Relaxed)
    }

    /// Wait for a grace period: advance the global epoch and spin until
    /// all registered threads have announced a quiescent state at or after
    /// the new epoch.
    ///
    /// **Important**: If the calling thread holds a [`QsbrHandle`], use
    /// [`QsbrHandle::synchronize_as_writer`] instead to avoid deadlock.
    pub fn synchronize(&self) {
        self.synchronize_core(None);
    }

    /// Internal: synchronize with optional caller slot to auto-quiesce.
    pub(crate) fn synchronize_with_slot(&self, caller_slot: usize) {
        self.synchronize_core(Some(caller_slot));
    }

    #[allow(clippy::cast_possible_truncation)]
    fn synchronize_core(&self, caller_slot: Option<usize>) {
        let _guard = self.writer_lock.lock();
        let start = Instant::now();

        // Advance global epoch.
        let new_epoch = self.global_epoch.fetch_add(1, Ordering::SeqCst) + 1;

        // Auto-quiesce the caller's slot at the new epoch (the caller is by
        // definition quiescent — it's executing synchronize, not reading).
        if let Some(slot) = caller_slot {
            self.slots[slot].store(new_epoch, Ordering::SeqCst);
        }

        // Wait for every active slot to reach `new_epoch`.
        let mut spins = 0u32;
        loop {
            let mut all_caught_up = true;
            for slot in &self.slots {
                let te = slot.load(Ordering::Acquire);
                if te != INACTIVE_EPOCH && te < new_epoch {
                    all_caught_up = false;
                    break;
                }
            }
            if all_caught_up {
                break;
            }
            spins += 1;
            if spins < SPIN_BEFORE_YIELD {
                std::hint::spin_loop();
            } else {
                std::thread::yield_now();
            }
        }

        let elapsed = start.elapsed();
        #[allow(clippy::cast_possible_truncation)]
        let ns = elapsed.as_nanos() as u64;

        // Update metrics.
        FSQLITE_RCU_GRACE_PERIODS_TOTAL.fetch_add(1, Ordering::Relaxed);
        FSQLITE_RCU_GRACE_PERIOD_DURATION_NS_TOTAL.fetch_add(ns, Ordering::Relaxed);

        // CAS-update max.
        let mut prev_max = FSQLITE_RCU_GRACE_PERIOD_DURATION_NS_MAX.load(Ordering::Relaxed);
        while ns > prev_max {
            match FSQLITE_RCU_GRACE_PERIOD_DURATION_NS_MAX.compare_exchange_weak(
                prev_max,
                ns,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => prev_max = actual,
            }
        }

        tracing::debug!(
            target: "fsqlite.rcu",
            grace_period_us = elapsed.as_micros() as u64,
            epoch = new_epoch,
            "rcu_sync"
        );
    }

    /// Number of currently active (registered) slots.
    #[must_use]
    pub fn active_threads(&self) -> usize {
        self.slots
            .iter()
            .filter(|s| s.load(Ordering::Relaxed) != INACTIVE_EPOCH)
            .count()
    }
}

impl Default for QsbrRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[allow(clippy::missing_fields_in_debug)]
impl std::fmt::Debug for QsbrRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QsbrRegistry")
            .field("global_epoch", &self.global_epoch.load(Ordering::Relaxed))
            .field("active_threads", &self.active_threads())
            .finish_non_exhaustive()
    }
}

// ---------------------------------------------------------------------------
// QsbrHandle (per-thread)
// ---------------------------------------------------------------------------

/// Per-thread QSBR handle.  Automatically unregisters on drop.
pub struct QsbrHandle<'a> {
    registry: &'a QsbrRegistry,
    slot: usize,
}

impl QsbrHandle<'_> {
    /// Announce a quiescent state.  Call this between read-side critical
    /// sections (e.g., between queries or at transaction boundaries).
    #[inline]
    pub fn quiescent(&self) {
        let ge = self.registry.global_epoch.load(Ordering::Acquire);
        self.registry.slots[self.slot].store(ge, Ordering::SeqCst);
    }

    /// Wait for a grace period as a writer that also holds a reader handle.
    ///
    /// This auto-quiesces the caller's slot at the new epoch so it doesn't
    /// block itself, then waits for all other registered threads.
    pub fn synchronize_as_writer(&self) {
        self.registry.synchronize_with_slot(self.slot);
    }

    /// Slot index (for diagnostics).
    #[must_use]
    pub fn slot(&self) -> usize {
        self.slot
    }
}

impl Drop for QsbrHandle<'_> {
    fn drop(&mut self) {
        self.registry.slots[self.slot].store(INACTIVE_EPOCH, Ordering::Release);
    }
}

// ---------------------------------------------------------------------------
// RcuCell (single u64, zero-overhead reads)
// ---------------------------------------------------------------------------

/// RCU-protected single `u64` value.
///
/// Readers call [`read`](RcuCell::read) — a single atomic load with zero
/// additional overhead.  Writers call [`publish`](RcuCell::publish) to store
/// a new value; callers must coordinate grace periods externally via
/// [`QsbrHandle::synchronize_as_writer`] if reclamation is needed.
pub struct RcuCell {
    value: AtomicU64,
}

impl RcuCell {
    pub fn new(initial: u64) -> Self {
        Self {
            value: AtomicU64::new(initial),
        }
    }

    /// Zero-overhead read.
    #[allow(clippy::inline_always)]
    #[inline(always)]
    pub fn read(&self) -> u64 {
        self.value.load(Ordering::Acquire)
    }

    /// Publish a new value.  Becomes visible to subsequent readers.
    pub fn publish(&self, new_val: u64) {
        self.value.store(new_val, Ordering::Release);
    }
}

impl std::fmt::Debug for RcuCell {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RcuCell")
            .field("value", &self.value.load(Ordering::Relaxed))
            .finish()
    }
}

// ---------------------------------------------------------------------------
// RcuPair (two u64 values, consistent reads via double-buffering)
// ---------------------------------------------------------------------------

/// RCU-protected pair of `u64` values with consistent reads.
///
/// Uses double-buffered slots: readers load the active index and read from
/// that slot (two atomic loads, zero counters).  Writers update the inactive
/// slot, swap the active index, and wait for a grace period before reusing
/// the old slot.
pub struct RcuPair {
    slot0_a: AtomicU64,
    slot0_b: AtomicU64,
    slot1_a: AtomicU64,
    slot1_b: AtomicU64,
    /// Active slot index (0 or 1).
    active: AtomicU64,
    writer_lock: Mutex<()>,
}

impl RcuPair {
    pub fn new(a: u64, b: u64) -> Self {
        Self {
            slot0_a: AtomicU64::new(a),
            slot0_b: AtomicU64::new(b),
            slot1_a: AtomicU64::new(a),
            slot1_b: AtomicU64::new(b),
            active: AtomicU64::new(0),
            writer_lock: Mutex::new(()),
        }
    }

    /// Zero-overhead consistent read of the pair.
    ///
    /// The caller must be in an RCU read-side critical section (between
    /// two `quiescent()` calls).
    #[inline]
    pub fn read(&self) -> (u64, u64) {
        let slot = self.active.load(Ordering::Acquire);
        if slot == 0 {
            (
                self.slot0_a.load(Ordering::Acquire),
                self.slot0_b.load(Ordering::Acquire),
            )
        } else {
            (
                self.slot1_a.load(Ordering::Acquire),
                self.slot1_b.load(Ordering::Acquire),
            )
        }
    }

    /// Publish new values.  Uses the caller's QSBR handle to auto-quiesce
    /// and wait for a grace period before reusing the old slot.
    pub fn publish(&self, a: u64, b: u64, handle: &QsbrHandle<'_>) {
        let _guard = self.writer_lock.lock();
        let cur = self.active.load(Ordering::Acquire);

        // Write to inactive slot.
        if cur == 0 {
            self.slot1_a.store(a, Ordering::Release);
            self.slot1_b.store(b, Ordering::Release);
        } else {
            self.slot0_a.store(a, Ordering::Release);
            self.slot0_b.store(b, Ordering::Release);
        }

        // Swap active.
        self.active.store(1 - cur, Ordering::Release);

        // Grace period: wait for readers on old slot to finish.
        handle.synchronize_as_writer();

        // Update old slot so both slots are consistent (ready for next publish).
        if cur == 0 {
            self.slot0_a.store(a, Ordering::Release);
            self.slot0_b.store(b, Ordering::Release);
        } else {
            self.slot1_a.store(a, Ordering::Release);
            self.slot1_b.store(b, Ordering::Release);
        }
    }
}

#[allow(clippy::missing_fields_in_debug)]
impl std::fmt::Debug for RcuPair {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RcuPair")
            .field("active", &self.active.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

// ---------------------------------------------------------------------------
// RcuTriple (three u64 values)
// ---------------------------------------------------------------------------

/// RCU-protected triple of `u64` values with consistent reads.
pub struct RcuTriple {
    slot0_a: AtomicU64,
    slot0_b: AtomicU64,
    slot0_c: AtomicU64,
    slot1_a: AtomicU64,
    slot1_b: AtomicU64,
    slot1_c: AtomicU64,
    active: AtomicU64,
    writer_lock: Mutex<()>,
}

impl RcuTriple {
    pub fn new(a: u64, b: u64, c: u64) -> Self {
        Self {
            slot0_a: AtomicU64::new(a),
            slot0_b: AtomicU64::new(b),
            slot0_c: AtomicU64::new(c),
            slot1_a: AtomicU64::new(a),
            slot1_b: AtomicU64::new(b),
            slot1_c: AtomicU64::new(c),
            active: AtomicU64::new(0),
            writer_lock: Mutex::new(()),
        }
    }

    /// Zero-overhead consistent read of the triple.
    #[inline]
    pub fn read(&self) -> (u64, u64, u64) {
        let slot = self.active.load(Ordering::Acquire);
        if slot == 0 {
            (
                self.slot0_a.load(Ordering::Acquire),
                self.slot0_b.load(Ordering::Acquire),
                self.slot0_c.load(Ordering::Acquire),
            )
        } else {
            (
                self.slot1_a.load(Ordering::Acquire),
                self.slot1_b.load(Ordering::Acquire),
                self.slot1_c.load(Ordering::Acquire),
            )
        }
    }

    /// Publish new values with grace period synchronization.
    pub fn publish(&self, a: u64, b: u64, c: u64, handle: &QsbrHandle<'_>) {
        let _guard = self.writer_lock.lock();
        let cur = self.active.load(Ordering::Acquire);

        if cur == 0 {
            self.slot1_a.store(a, Ordering::Release);
            self.slot1_b.store(b, Ordering::Release);
            self.slot1_c.store(c, Ordering::Release);
        } else {
            self.slot0_a.store(a, Ordering::Release);
            self.slot0_b.store(b, Ordering::Release);
            self.slot0_c.store(c, Ordering::Release);
        }

        self.active.store(1 - cur, Ordering::Release);
        handle.synchronize_as_writer();

        if cur == 0 {
            self.slot0_a.store(a, Ordering::Release);
            self.slot0_b.store(b, Ordering::Release);
            self.slot0_c.store(c, Ordering::Release);
        } else {
            self.slot1_a.store(a, Ordering::Release);
            self.slot1_b.store(b, Ordering::Release);
            self.slot1_c.store(c, Ordering::Release);
        }
    }
}

#[allow(clippy::missing_fields_in_debug)]
impl std::fmt::Debug for RcuTriple {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RcuTriple")
            .field("active", &self.active.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicBool;
    use std::sync::{Arc, Barrier};
    use std::thread;
    use std::time::Duration;

    #[test]
    fn registry_register_unregister() {
        let reg = QsbrRegistry::new();
        assert_eq!(reg.active_threads(), 0);

        let h1 = reg.register().unwrap();
        assert_eq!(reg.active_threads(), 1);
        assert_eq!(h1.slot(), 0);

        let h2 = reg.register().unwrap();
        assert_eq!(reg.active_threads(), 2);

        drop(h1);
        assert_eq!(reg.active_threads(), 1);

        drop(h2);
        assert_eq!(reg.active_threads(), 0);
    }

    #[test]
    fn grace_period_single_thread() {
        let reg = QsbrRegistry::new();
        let h = reg.register().unwrap();

        // synchronize_as_writer auto-quiesces our slot so we don't deadlock.
        let start = Instant::now();
        h.synchronize_as_writer();
        let elapsed = start.elapsed();
        assert!(elapsed < Duration::from_millis(100));

        drop(h);
    }

    #[test]
    fn grace_period_waits_for_reader() {
        let reg = Arc::new(QsbrRegistry::new());

        // Reader thread registers and goes quiescent initially.
        let reg_r = Arc::clone(&reg);
        let ready = Arc::new(Barrier::new(2));
        let ready_r = Arc::clone(&ready);
        let do_quiescent = Arc::new(AtomicBool::new(false));
        let do_q_r = Arc::clone(&do_quiescent);

        let reader = thread::spawn(move || {
            let h = reg_r.register().unwrap();
            h.quiescent();
            ready_r.wait(); // signal main thread
            // Simulate being in a read critical section.
            while !do_q_r.load(Ordering::Acquire) {
                std::hint::spin_loop();
            }
            h.quiescent();
            // Keep handle alive a bit longer.
            thread::sleep(Duration::from_millis(10));
            drop(h);
        });

        ready.wait();
        // Reader is now registered and quiescent at epoch 1.
        // Synchronize will advance to epoch 2; reader hasn't seen it yet.
        // We signal the reader to quiesce so synchronize can complete.
        let writer = {
            let reg_w = Arc::clone(&reg);
            let do_q_w = Arc::clone(&do_quiescent);
            thread::spawn(move || {
                // Small delay to ensure reader is "in critical section"
                thread::sleep(Duration::from_millis(5));
                do_q_w.store(true, Ordering::Release);
                reg_w.synchronize();
            })
        };

        writer.join().unwrap();
        reader.join().unwrap();
    }

    #[test]
    fn rcu_cell_basic() {
        let cell = RcuCell::new(42);
        assert_eq!(cell.read(), 42);
        cell.publish(99);
        assert_eq!(cell.read(), 99);
    }

    #[test]
    fn rcu_pair_consistent_snapshot() {
        let reg = QsbrRegistry::new();
        let h = reg.register().unwrap();
        let pair = RcuPair::new(1, 2);
        assert_eq!(pair.read(), (1, 2));

        h.quiescent();
        pair.publish(10, 20, &h);
        h.quiescent();
        assert_eq!(pair.read(), (10, 20));

        drop(h);
    }

    #[test]
    fn rcu_triple_consistent_snapshot() {
        let reg = QsbrRegistry::new();
        let h = reg.register().unwrap();
        let triple = RcuTriple::new(1, 2, 3);
        assert_eq!(triple.read(), (1, 2, 3));

        h.quiescent();
        triple.publish(10, 20, 30, &h);
        h.quiescent();
        assert_eq!(triple.read(), (10, 20, 30));

        drop(h);
    }

    #[test]
    fn active_txn_snapshot_table_basic_publication() {
        let reg = QsbrRegistry::new();
        let h = reg.register().unwrap();
        let table = RcuActiveTxnSnapshotTable::new();

        let empty = table.snapshot();
        assert_eq!(empty.generation, 0);
        assert_eq!(empty.gc_horizon, None);
        assert!(empty.is_empty());

        h.quiescent();
        let entries = [
            ActiveTxnSnapshotEntry {
                session_id: 7,
                snapshot_high: CommitSeq::new(41),
            },
            ActiveTxnSnapshotEntry {
                session_id: 9,
                snapshot_high: CommitSeq::new(43),
            },
        ];
        table.publish(&entries, Some(CommitSeq::new(41)), &h);
        h.quiescent();

        let image = table.snapshot();
        assert_eq!(image.generation, 1);
        assert_eq!(image.gc_horizon, Some(CommitSeq::new(41)));
        assert_eq!(image.entries.as_slice(), &entries);

        drop(h);
    }

    #[test]
    #[allow(clippy::similar_names)]
    fn active_txn_snapshot_table_no_mixed_generations() {
        let reg = Arc::new(QsbrRegistry::new());
        let table = Arc::new(RcuActiveTxnSnapshotTable::new());
        let stop = Arc::new(AtomicBool::new(false));
        let barrier = Arc::new(Barrier::new(5)); // 1 writer + 4 readers

        let w_reg = Arc::clone(&reg);
        let w_table = Arc::clone(&table);
        let w_stop = Arc::clone(&stop);
        let w_barrier = Arc::clone(&barrier);
        let writer = thread::spawn(move || {
            let h = w_reg.register().unwrap();
            h.quiescent();
            w_barrier.wait();
            let mut generation = 0_u64;
            while !w_stop.load(Ordering::Relaxed) {
                generation += 1;
                let entries = [
                    ActiveTxnSnapshotEntry {
                        session_id: 1,
                        snapshot_high: CommitSeq::new(generation * 10 + 1),
                    },
                    ActiveTxnSnapshotEntry {
                        session_id: 2,
                        snapshot_high: CommitSeq::new(generation * 10 + 2),
                    },
                    ActiveTxnSnapshotEntry {
                        session_id: 3,
                        snapshot_high: CommitSeq::new(generation * 10 + 3),
                    },
                ];
                w_table.publish(&entries, Some(CommitSeq::new(generation * 10 + 1)), &h);
            }
            h.quiescent();
            drop(h);
            generation
        });

        let mut readers = Vec::new();
        for _ in 0..4 {
            let r_reg = Arc::clone(&reg);
            let r_table = Arc::clone(&table);
            let r_stop = Arc::clone(&stop);
            let r_barrier = Arc::clone(&barrier);
            readers.push(thread::spawn(move || {
                let h = r_reg.register().unwrap();
                h.quiescent();
                r_barrier.wait();
                let mut reads = 0_u64;
                while !r_stop.load(Ordering::Relaxed) {
                    let image = r_table.snapshot();
                    if !image.is_empty() {
                        assert_eq!(image.len(), 3);
                        assert_eq!(
                            image.gc_horizon,
                            Some(CommitSeq::new(image.generation * 10 + 1))
                        );
                        for (idx, entry) in image.entries.iter().enumerate() {
                            let expected_session_id = idx as u64 + 1;
                            assert_eq!(entry.session_id, expected_session_id);
                            assert_eq!(
                                entry.snapshot_high,
                                CommitSeq::new(image.generation * 10 + expected_session_id)
                            );
                        }
                    }
                    reads += 1;
                    if reads.is_multiple_of(200) {
                        h.quiescent();
                    }
                }
                h.quiescent();
                drop(h);
                reads
            }));
        }

        thread::sleep(Duration::from_millis(250));
        stop.store(true, Ordering::Release);

        let writes = writer.join().unwrap();
        let mut total_reads = 0_u64;
        for reader in readers {
            total_reads += reader.join().unwrap();
        }

        assert!(writes > 0);
        assert!(total_reads > 0);
    }

    #[test]
    fn active_txn_snapshot_table_publish_waits_for_reader_quiescent() {
        let reg = Arc::new(QsbrRegistry::new());
        let table = Arc::new(RcuActiveTxnSnapshotTable::new());
        let bootstrap = reg.register().unwrap();
        bootstrap.quiescent();
        table.publish(
            &[ActiveTxnSnapshotEntry {
                session_id: 1,
                snapshot_high: CommitSeq::new(11),
            }],
            Some(CommitSeq::new(11)),
            &bootstrap,
        );
        bootstrap.quiescent();
        drop(bootstrap);

        let reader_in_critical = Arc::new(AtomicBool::new(false));
        let allow_reader_quiesce = Arc::new(AtomicBool::new(false));
        let publish_finished = Arc::new(AtomicBool::new(false));

        let reader = {
            let reg = Arc::clone(&reg);
            let table = Arc::clone(&table);
            let reader_in_critical = Arc::clone(&reader_in_critical);
            let allow_reader_quiesce = Arc::clone(&allow_reader_quiesce);
            thread::spawn(move || {
                let h = reg.register().unwrap();
                h.quiescent();
                let image = table.snapshot();
                assert_eq!(image.generation, 1);
                reader_in_critical.store(true, Ordering::Release);
                while !allow_reader_quiesce.load(Ordering::Acquire) {
                    std::hint::spin_loop();
                }
                h.quiescent();
                drop(h);
            })
        };

        let writer = {
            let reg = Arc::clone(&reg);
            let table = Arc::clone(&table);
            let reader_in_critical = Arc::clone(&reader_in_critical);
            let publish_finished = Arc::clone(&publish_finished);
            thread::spawn(move || {
                let h = reg.register().unwrap();
                h.quiescent();
                while !reader_in_critical.load(Ordering::Acquire) {
                    std::hint::spin_loop();
                }
                table.publish(
                    &[ActiveTxnSnapshotEntry {
                        session_id: 2,
                        snapshot_high: CommitSeq::new(21),
                    }],
                    Some(CommitSeq::new(21)),
                    &h,
                );
                publish_finished.store(true, Ordering::Release);
                h.quiescent();
                drop(h);
            })
        };

        thread::sleep(Duration::from_millis(20));
        assert!(
            !publish_finished.load(Ordering::Acquire),
            "writer publish should wait for the reader grace period"
        );

        allow_reader_quiesce.store(true, Ordering::Release);
        writer.join().unwrap();
        reader.join().unwrap();

        assert!(publish_finished.load(Ordering::Acquire));
        let image = table.snapshot();
        assert_eq!(image.generation, 2);
        assert_eq!(image.entries[0].session_id, 2);
        assert_eq!(image.entries[0].snapshot_high, CommitSeq::new(21));
    }

    #[test]
    #[allow(clippy::similar_names)]
    fn rcu_pair_no_torn_reads() {
        let reg = Arc::new(QsbrRegistry::new());
        let pair = Arc::new(RcuPair::new(0, 0));
        let stop = Arc::new(AtomicBool::new(false));
        let barrier = Arc::new(Barrier::new(5)); // 1 writer + 4 readers

        // Writer
        let w_reg = Arc::clone(&reg);
        let w_pair = Arc::clone(&pair);
        let w_stop = Arc::clone(&stop);
        let w_barrier = Arc::clone(&barrier);
        let writer = thread::spawn(move || {
            let h = w_reg.register().unwrap();
            h.quiescent();
            w_barrier.wait();
            let mut val = 0u64;
            while !w_stop.load(Ordering::Relaxed) {
                val += 1;
                w_pair.publish(val, val, &h);
            }
            drop(h);
            val
        });

        // Readers
        let mut readers = Vec::new();
        for _ in 0..4 {
            let r_reg = Arc::clone(&reg);
            let r_pair = Arc::clone(&pair);
            let r_stop = Arc::clone(&stop);
            let r_barrier = Arc::clone(&barrier);
            readers.push(thread::spawn(move || {
                let h = r_reg.register().unwrap();
                h.quiescent();
                r_barrier.wait();
                let mut reads = 0u64;
                while !r_stop.load(Ordering::Relaxed) {
                    let (a, b) = r_pair.read();
                    assert_eq!(a, b, "TORN READ: a={a} b={b}");
                    reads += 1;
                    // Periodically announce quiescent state.
                    if reads.is_multiple_of(1000) {
                        h.quiescent();
                    }
                }
                h.quiescent();
                drop(h);
                reads
            }));
        }

        thread::sleep(Duration::from_millis(500));
        stop.store(true, Ordering::Release);

        let writes = writer.join().unwrap();
        let mut total_reads = 0u64;
        for r in readers {
            total_reads += r.join().unwrap();
        }

        assert!(writes > 0);
        assert!(total_reads > 0);
        println!("[rcu_pair] writes={writes} reads={total_reads} no torn reads");
    }

    #[test]
    fn metrics_track_grace_periods() {
        let before = rcu_metrics();
        let reg = QsbrRegistry::new();
        let h = reg.register().unwrap();

        h.synchronize_as_writer();
        h.synchronize_as_writer();
        h.synchronize_as_writer();

        let after = rcu_metrics();
        let grace_delta =
            after.fsqlite_rcu_grace_periods_total - before.fsqlite_rcu_grace_periods_total;
        let duration_delta = after.fsqlite_rcu_grace_period_duration_ns_total
            - before.fsqlite_rcu_grace_period_duration_ns_total;
        assert!(
            grace_delta >= 3,
            "expected at least 3 grace periods, got {grace_delta}"
        );
        assert!(
            duration_delta > 0,
            "expected duration delta > 0, got {duration_delta}"
        );

        drop(h);
    }

    #[test]
    fn debug_format() {
        let reg = QsbrRegistry::new();
        let dbg = format!("{reg:?}");
        assert!(dbg.contains("QsbrRegistry"));
        assert!(dbg.contains("global_epoch"));

        let cell = RcuCell::new(42);
        let dbg = format!("{cell:?}");
        assert!(dbg.contains("RcuCell"));
    }

    #[test]
    #[allow(clippy::similar_names)]
    fn stress_concurrent_rw() {
        let reg = Arc::new(QsbrRegistry::new());
        let pair = Arc::new(RcuPair::new(0, 0));
        let stop = Arc::new(AtomicBool::new(false));
        let barrier = Arc::new(Barrier::new(3)); // 1 writer + 2 readers

        // Writer
        let w_reg = Arc::clone(&reg);
        let w_pair = Arc::clone(&pair);
        let w_stop = Arc::clone(&stop);
        let w_barrier = Arc::clone(&barrier);
        let writer = thread::spawn(move || {
            let h = w_reg.register().unwrap();
            h.quiescent();
            w_barrier.wait();
            let mut val = 0u64;
            while !w_stop.load(Ordering::Relaxed) {
                val += 1;
                w_pair.publish(val, val, &h);
            }
            drop(h);
            val
        });

        // 2 readers
        let mut readers = Vec::new();
        for _ in 0..2 {
            let r_reg = Arc::clone(&reg);
            let r_pair = Arc::clone(&pair);
            let r_stop = Arc::clone(&stop);
            let r_barrier = Arc::clone(&barrier);
            readers.push(thread::spawn(move || {
                let h = r_reg.register().unwrap();
                h.quiescent();
                r_barrier.wait();
                let mut reads = 0u64;
                while !r_stop.load(Ordering::Relaxed) {
                    let (a, b) = r_pair.read();
                    assert_eq!(a, b, "TORN READ: a={a} b={b}");
                    reads += 1;
                    if reads.is_multiple_of(500) {
                        h.quiescent();
                    }
                }
                h.quiescent();
                drop(h);
                reads
            }));
        }

        thread::sleep(Duration::from_millis(300));
        stop.store(true, Ordering::Release);

        let writes = writer.join().unwrap();
        let mut total_reads = 0u64;
        for r in readers {
            total_reads += r.join().unwrap();
        }

        assert!(writes > 0);
        assert!(total_reads > 0);
        println!("[rcu_stress] writes={writes} reads={total_reads}");
    }
}
