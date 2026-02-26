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

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use parking_lot::Mutex;
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
                    if reads % 1000 == 0 {
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
                    if reads % 500 == 0 {
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
