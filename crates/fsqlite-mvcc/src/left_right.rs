//! Left-Right wait-free reads for metadata (§14.3).
//!
//! Two copies of the data are maintained; readers access one copy with wait-free
//! guarantees (at most one retry per concurrent swap), while the writer updates
//! the inactive copy, swaps the `active` pointer, waits for old-side readers to
//! drain, then updates the old copy.
//!
//! ## Protocol
//!
//! **Reader**:
//!   1. Load `active` index (0 or 1).
//!   2. Increment reader counter for that side.
//!   3. Re-check `active` — if it changed, decrement and retry (at most once).
//!   4. Read the value(s).
//!   5. Decrement reader counter.
//!
//! **Writer** (serialized via `fsqlite_types::sync_primitives::Mutex`):
//!   1. Update the *inactive* copy.
//!   2. Swap `active`.
//!   3. Spin-wait until reader counter on the *old* active side drains to zero.
//!   4. Update the *old* copy (now inactive) to match.
//!
//! ## Safety
//!
//! No `UnsafeCell` or `unsafe` blocks — all protected state uses `AtomicU64`.
//!
//! ## Tracing & Metrics
//!
//! - **Target**: `fsqlite.left_right`
//!   - `TRACE`: clean reads (zero retries)
//!   - `DEBUG`: reads that retried due to concurrent swap
//! - **Metrics** (global atomics, zero per-op overhead):
//!   - `fsqlite_leftright_reads_total`
//!   - `fsqlite_leftright_swaps_total`
//!   - `fsqlite_leftright_reader_retries_total`

use std::sync::atomic::{AtomicU64, Ordering};

use serde::Serialize;

// ---------------------------------------------------------------------------
// Global metrics (lock-free, Relaxed ordering)
// ---------------------------------------------------------------------------

static FSQLITE_LEFTRIGHT_READS_TOTAL: AtomicU64 = AtomicU64::new(0);
static FSQLITE_LEFTRIGHT_SWAPS_TOTAL: AtomicU64 = AtomicU64::new(0);
static FSQLITE_LEFTRIGHT_READER_RETRIES_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Snapshot of left-right metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct LeftRightMetrics {
    pub fsqlite_leftright_reads_total: u64,
    pub fsqlite_leftright_swaps_total: u64,
    pub fsqlite_leftright_reader_retries_total: u64,
}

/// Read current left-right metrics.
#[must_use]
pub fn leftright_metrics() -> LeftRightMetrics {
    LeftRightMetrics {
        fsqlite_leftright_reads_total: FSQLITE_LEFTRIGHT_READS_TOTAL.load(Ordering::Relaxed),
        fsqlite_leftright_swaps_total: FSQLITE_LEFTRIGHT_SWAPS_TOTAL.load(Ordering::Relaxed),
        fsqlite_leftright_reader_retries_total: FSQLITE_LEFTRIGHT_READER_RETRIES_TOTAL
            .load(Ordering::Relaxed),
    }
}

/// Reset metrics (for tests).
pub fn reset_leftright_metrics() {
    FSQLITE_LEFTRIGHT_READS_TOTAL.store(0, Ordering::Relaxed);
    FSQLITE_LEFTRIGHT_SWAPS_TOTAL.store(0, Ordering::Relaxed);
    FSQLITE_LEFTRIGHT_READER_RETRIES_TOTAL.store(0, Ordering::Relaxed);
}

// ---------------------------------------------------------------------------
// LeftRight (single u64 value)
// ---------------------------------------------------------------------------

/// A left-right primitive protecting a single `u64` metadata value.
///
/// Readers call [`read`](LeftRight::read) for a wait-free snapshot (at most
/// one retry per concurrent swap). Writers call [`write`](LeftRight::write) or
/// [`update`](LeftRight::update) which serialize via an internal mutex, update
/// both copies, and atomically swap the active pointer.
///
/// All data is stored in atomics — no `UnsafeCell`, no `unsafe`.
pub struct LeftRight {
    left: AtomicU64,
    right: AtomicU64,
    /// 0 = readers use left, 1 = readers use right
    active: AtomicU64,
    left_readers: AtomicU64,
    right_readers: AtomicU64,
    writer_lock: fsqlite_types::sync_primitives::Mutex<()>,
}

impl LeftRight {
    /// Create a new left-right with the given initial value (both copies set).
    pub fn new(initial: u64) -> Self {
        Self {
            left: AtomicU64::new(initial),
            right: AtomicU64::new(initial),
            active: AtomicU64::new(0),
            left_readers: AtomicU64::new(0),
            right_readers: AtomicU64::new(0),
            writer_lock: fsqlite_types::sync_primitives::Mutex::new(()),
        }
    }

    /// Wait-free read. Returns the current value.
    ///
    /// At most one retry if a concurrent writer swaps during the read.
    #[inline]
    pub fn read(&self, data_key: &str) -> u64 {
        let mut retries = 0u32;
        let value = loop {
            let side = self.active.load(Ordering::Acquire);
            let (readers, data) = if side == 0 {
                (&self.left_readers, &self.left)
            } else {
                (&self.right_readers, &self.right)
            };
            readers.fetch_add(1, Ordering::AcqRel);
            // Re-check: if active changed, we incremented the wrong counter.
            if self.active.load(Ordering::Acquire) == side {
                let v = data.load(Ordering::Acquire);
                readers.fetch_sub(1, Ordering::Release);
                break v;
            }
            // Side changed during enter; undo and retry on the new side.
            readers.fetch_sub(1, Ordering::Release);
            retries += 1;
        };
        FSQLITE_LEFTRIGHT_READS_TOTAL.fetch_add(1, Ordering::Relaxed);
        if retries > 0 {
            FSQLITE_LEFTRIGHT_READER_RETRIES_TOTAL.fetch_add(u64::from(retries), Ordering::Relaxed);
        }
        emit_read_trace(data_key, retries);
        value
    }

    /// Set a new value. Serialized with other writers.
    pub fn write(&self, new_val: u64) {
        let _guard = self.writer_lock.lock();
        self.write_inner(new_val);
    }

    /// Read-modify-write via a closure. Serialized with other writers.
    pub fn update<F: FnOnce(u64) -> u64>(&self, f: F) {
        let _guard = self.writer_lock.lock();
        let active = self.active.load(Ordering::Acquire);
        let current = if active == 0 {
            self.left.load(Ordering::Acquire)
        } else {
            self.right.load(Ordering::Acquire)
        };
        self.write_inner(f(current));
    }

    fn write_inner(&self, new_val: u64) {
        let active = self.active.load(Ordering::Acquire);

        // 1. Write to inactive copy.
        if active == 0 {
            self.right.store(new_val, Ordering::Release);
        } else {
            self.left.store(new_val, Ordering::Release);
        }

        // 2. Swap active side.
        let new_active = 1 - active;
        self.active.store(new_active, Ordering::Release);
        FSQLITE_LEFTRIGHT_SWAPS_TOTAL.fetch_add(1, Ordering::Relaxed);

        // 3. Wait for readers on the old active side to drain.
        let old_readers = if active == 0 {
            &self.left_readers
        } else {
            &self.right_readers
        };
        while old_readers.load(Ordering::Acquire) > 0 {
            std::hint::spin_loop();
        }

        // 4. Update old copy (now inactive) to match.
        if active == 0 {
            self.left.store(new_val, Ordering::Release);
        } else {
            self.right.store(new_val, Ordering::Release);
        }

        emit_swap_trace(1);
    }

    /// Returns which side is currently active (0 = left, 1 = right).
    #[must_use]
    pub fn active_side(&self) -> u64 {
        self.active.load(Ordering::Relaxed)
    }
}

#[allow(clippy::missing_fields_in_debug)]
impl std::fmt::Debug for LeftRight {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let active = self.active.load(Ordering::Relaxed);
        let lr = self.left_readers.load(Ordering::Relaxed);
        let rr = self.right_readers.load(Ordering::Relaxed);
        f.debug_struct("LeftRight")
            .field("active", &if active == 0 { "left" } else { "right" })
            .field("left_readers", &lr)
            .field("right_readers", &rr)
            .finish_non_exhaustive()
    }
}

// ---------------------------------------------------------------------------
// LeftRightPair (two u64 values, consistent snapshot)
// ---------------------------------------------------------------------------

/// A left-right primitive protecting a pair of `u64` values read atomically.
///
/// Useful for metadata that must be read consistently as a unit
/// (e.g., schema_epoch + commit_seq, or min/max version bounds).
pub struct LeftRightPair {
    left_a: AtomicU64,
    left_b: AtomicU64,
    right_a: AtomicU64,
    right_b: AtomicU64,
    active: AtomicU64,
    left_readers: AtomicU64,
    right_readers: AtomicU64,
    writer_lock: fsqlite_types::sync_primitives::Mutex<()>,
}

impl LeftRightPair {
    /// Create a new left-right pair with the given initial values.
    pub fn new(a: u64, b: u64) -> Self {
        Self {
            left_a: AtomicU64::new(a),
            left_b: AtomicU64::new(b),
            right_a: AtomicU64::new(a),
            right_b: AtomicU64::new(b),
            active: AtomicU64::new(0),
            left_readers: AtomicU64::new(0),
            right_readers: AtomicU64::new(0),
            writer_lock: fsqlite_types::sync_primitives::Mutex::new(()),
        }
    }

    /// Wait-free read of the consistent pair.
    #[inline]
    pub fn read(&self, data_key: &str) -> (u64, u64) {
        let mut retries = 0u32;
        let value = loop {
            let side = self.active.load(Ordering::Acquire);
            let (readers, da, db) = if side == 0 {
                (&self.left_readers, &self.left_a, &self.left_b)
            } else {
                (&self.right_readers, &self.right_a, &self.right_b)
            };
            readers.fetch_add(1, Ordering::AcqRel);
            if self.active.load(Ordering::Acquire) == side {
                let va = da.load(Ordering::Acquire);
                let vb = db.load(Ordering::Acquire);
                readers.fetch_sub(1, Ordering::Release);
                break (va, vb);
            }
            readers.fetch_sub(1, Ordering::Release);
            retries += 1;
        };
        FSQLITE_LEFTRIGHT_READS_TOTAL.fetch_add(1, Ordering::Relaxed);
        if retries > 0 {
            FSQLITE_LEFTRIGHT_READER_RETRIES_TOTAL.fetch_add(u64::from(retries), Ordering::Relaxed);
        }
        emit_read_trace(data_key, retries);
        value
    }

    /// Update both values. Serialized with other writers.
    pub fn write(&self, a: u64, b: u64) {
        let _guard = self.writer_lock.lock();
        let active = self.active.load(Ordering::Acquire);

        // Write to inactive side.
        if active == 0 {
            self.right_a.store(a, Ordering::Release);
            self.right_b.store(b, Ordering::Release);
        } else {
            self.left_a.store(a, Ordering::Release);
            self.left_b.store(b, Ordering::Release);
        }

        // Swap.
        self.active.store(1 - active, Ordering::Release);
        FSQLITE_LEFTRIGHT_SWAPS_TOTAL.fetch_add(1, Ordering::Relaxed);

        // Drain old readers.
        let old_readers = if active == 0 {
            &self.left_readers
        } else {
            &self.right_readers
        };
        while old_readers.load(Ordering::Acquire) > 0 {
            std::hint::spin_loop();
        }

        // Update old copy.
        if active == 0 {
            self.left_a.store(a, Ordering::Release);
            self.left_b.store(b, Ordering::Release);
        } else {
            self.right_a.store(a, Ordering::Release);
            self.right_b.store(b, Ordering::Release);
        }

        emit_swap_trace(1);
    }
}

#[allow(clippy::missing_fields_in_debug)]
impl std::fmt::Debug for LeftRightPair {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let active = self.active.load(Ordering::Relaxed);
        f.debug_struct("LeftRightPair")
            .field("active", &if active == 0 { "left" } else { "right" })
            .finish_non_exhaustive()
    }
}

// ---------------------------------------------------------------------------
// LeftRightTriple (three u64 values)
// ---------------------------------------------------------------------------

/// A left-right primitive protecting a triple of `u64` values.
///
/// Matches the `(commit_seq, schema_epoch, ecs_epoch)` pattern from the
/// shared-memory header.
pub struct LeftRightTriple {
    left_a: AtomicU64,
    left_b: AtomicU64,
    left_c: AtomicU64,
    right_a: AtomicU64,
    right_b: AtomicU64,
    right_c: AtomicU64,
    active: AtomicU64,
    left_readers: AtomicU64,
    right_readers: AtomicU64,
    writer_lock: fsqlite_types::sync_primitives::Mutex<()>,
}

impl LeftRightTriple {
    /// Create a new left-right triple.
    pub fn new(a: u64, b: u64, c: u64) -> Self {
        Self {
            left_a: AtomicU64::new(a),
            left_b: AtomicU64::new(b),
            left_c: AtomicU64::new(c),
            right_a: AtomicU64::new(a),
            right_b: AtomicU64::new(b),
            right_c: AtomicU64::new(c),
            active: AtomicU64::new(0),
            left_readers: AtomicU64::new(0),
            right_readers: AtomicU64::new(0),
            writer_lock: fsqlite_types::sync_primitives::Mutex::new(()),
        }
    }

    /// Wait-free read of the consistent triple.
    #[inline]
    pub fn read(&self, data_key: &str) -> (u64, u64, u64) {
        let mut retries = 0u32;
        let value = loop {
            let side = self.active.load(Ordering::Acquire);
            let (readers, da, db, dc) = if side == 0 {
                (&self.left_readers, &self.left_a, &self.left_b, &self.left_c)
            } else {
                (
                    &self.right_readers,
                    &self.right_a,
                    &self.right_b,
                    &self.right_c,
                )
            };
            readers.fetch_add(1, Ordering::AcqRel);
            if self.active.load(Ordering::Acquire) == side {
                let va = da.load(Ordering::Acquire);
                let vb = db.load(Ordering::Acquire);
                let vc = dc.load(Ordering::Acquire);
                readers.fetch_sub(1, Ordering::Release);
                break (va, vb, vc);
            }
            readers.fetch_sub(1, Ordering::Release);
            retries += 1;
        };
        FSQLITE_LEFTRIGHT_READS_TOTAL.fetch_add(1, Ordering::Relaxed);
        if retries > 0 {
            FSQLITE_LEFTRIGHT_READER_RETRIES_TOTAL.fetch_add(u64::from(retries), Ordering::Relaxed);
        }
        emit_read_trace(data_key, retries);
        value
    }

    /// Update all three values. Serialized with other writers.
    pub fn write(&self, a: u64, b: u64, c: u64) {
        let _guard = self.writer_lock.lock();
        let active = self.active.load(Ordering::Acquire);

        // Write to inactive side.
        if active == 0 {
            self.right_a.store(a, Ordering::Release);
            self.right_b.store(b, Ordering::Release);
            self.right_c.store(c, Ordering::Release);
        } else {
            self.left_a.store(a, Ordering::Release);
            self.left_b.store(b, Ordering::Release);
            self.left_c.store(c, Ordering::Release);
        }

        // Swap.
        self.active.store(1 - active, Ordering::Release);
        FSQLITE_LEFTRIGHT_SWAPS_TOTAL.fetch_add(1, Ordering::Relaxed);

        // Drain old readers.
        let old_readers = if active == 0 {
            &self.left_readers
        } else {
            &self.right_readers
        };
        while old_readers.load(Ordering::Acquire) > 0 {
            std::hint::spin_loop();
        }

        // Update old copy.
        if active == 0 {
            self.left_a.store(a, Ordering::Release);
            self.left_b.store(b, Ordering::Release);
            self.left_c.store(c, Ordering::Release);
        } else {
            self.right_a.store(a, Ordering::Release);
            self.right_b.store(b, Ordering::Release);
            self.right_c.store(c, Ordering::Release);
        }

        emit_swap_trace(1);
    }
}

#[allow(clippy::missing_fields_in_debug)]
impl std::fmt::Debug for LeftRightTriple {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let active = self.active.load(Ordering::Relaxed);
        f.debug_struct("LeftRightTriple")
            .field("active", &if active == 0 { "left" } else { "right" })
            .finish_non_exhaustive()
    }
}

// ---------------------------------------------------------------------------
// Tracing helpers
// ---------------------------------------------------------------------------

fn emit_read_trace(data_key: &str, retries: u32) {
    if retries > 0 {
        tracing::debug!(
            target: "fsqlite.left_right",
            data_key,
            retries,
            "left_right_read retried"
        );
    } else {
        tracing::trace!(
            target: "fsqlite.left_right",
            data_key,
            retries = 0u32,
            "left_right_read"
        );
    }
}

fn emit_swap_trace(swap_count: u32) {
    tracing::debug!(
        target: "fsqlite.left_right",
        swap_count,
        "left_right_swap"
    );
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
    fn basic_read_write() {
        let lr = LeftRight::new(42);
        assert_eq!(lr.read("test"), 42);
        lr.write(99);
        assert_eq!(lr.read("test"), 99);
    }

    #[test]
    fn update_closure() {
        let lr = LeftRight::new(10);
        lr.update(|v| v + 5);
        assert_eq!(lr.read("test"), 15);
        lr.update(|v| v * 2);
        assert_eq!(lr.read("test"), 30);
    }

    #[test]
    fn pair_consistent_snapshot() {
        let lr = LeftRightPair::new(1, 2);
        assert_eq!(lr.read("pair"), (1, 2));
        lr.write(10, 20);
        assert_eq!(lr.read("pair"), (10, 20));
    }

    #[test]
    fn triple_consistent_snapshot() {
        let lr = LeftRightTriple::new(1, 2, 3);
        assert_eq!(lr.read("triple"), (1, 2, 3));
        lr.write(10, 20, 30);
        assert_eq!(lr.read("triple"), (10, 20, 30));
    }

    /// Verify that concurrent readers never observe an inconsistent pair.
    #[test]
    fn no_torn_reads_pair() {
        let lr = Arc::new(LeftRightPair::new(0, 0));
        let stop = Arc::new(AtomicBool::new(false));
        let barrier = Arc::new(Barrier::new(5)); // 1 writer + 4 readers

        let writer_lr = Arc::clone(&lr);
        let writer_stop = Arc::clone(&stop);
        let writer_barrier = Arc::clone(&barrier);
        let writer = thread::spawn(move || {
            writer_barrier.wait();
            let mut val = 0u64;
            while !writer_stop.load(Ordering::Relaxed) {
                val += 1;
                writer_lr.write(val, val);
            }
            val
        });

        let mut readers = Vec::new();
        for _ in 0..4 {
            let rlr = Arc::clone(&lr);
            let rs = Arc::clone(&stop);
            let rb = Arc::clone(&barrier);
            readers.push(thread::spawn(move || {
                rb.wait();
                let mut reads = 0u64;
                while !rs.load(Ordering::Relaxed) {
                    let (a, b) = rlr.read("pair");
                    assert_eq!(a, b, "torn read: a={a}, b={b}");
                    reads += 1;
                }
                reads
            }));
        }

        thread::sleep(Duration::from_millis(500));
        stop.store(true, Ordering::Release);

        let writer_count = writer.join().unwrap();
        let mut total_reads = 0u64;
        for r in readers {
            total_reads += r.join().unwrap();
        }

        assert!(writer_count > 0, "writer must have written");
        assert!(total_reads > 0, "readers must have read");
        println!("[left_right_pair] writes={writer_count} reads={total_reads} no torn reads");
    }

    /// Verify that concurrent readers never observe an inconsistent triple.
    #[test]
    fn no_torn_reads_triple() {
        let lr = Arc::new(LeftRightTriple::new(0, 0, 0));
        let stop = Arc::new(AtomicBool::new(false));
        let barrier = Arc::new(Barrier::new(5));

        let writer_lr = Arc::clone(&lr);
        let writer_stop = Arc::clone(&stop);
        let writer_barrier = Arc::clone(&barrier);
        let writer = thread::spawn(move || {
            writer_barrier.wait();
            let mut val = 0u64;
            while !writer_stop.load(Ordering::Relaxed) {
                val += 1;
                writer_lr.write(val, val, val);
            }
            val
        });

        let mut readers = Vec::new();
        for _ in 0..4 {
            let rlr = Arc::clone(&lr);
            let rs = Arc::clone(&stop);
            let rb = Arc::clone(&barrier);
            readers.push(thread::spawn(move || {
                rb.wait();
                let mut reads = 0u64;
                while !rs.load(Ordering::Relaxed) {
                    let (a, b, c) = rlr.read("triple");
                    assert!(a == b && b == c, "torn read: a={a}, b={b}, c={c}");
                    reads += 1;
                }
                reads
            }));
        }

        thread::sleep(Duration::from_millis(500));
        stop.store(true, Ordering::Release);

        let writer_count = writer.join().unwrap();
        let mut total_reads = 0u64;
        for r in readers {
            total_reads += r.join().unwrap();
        }

        assert!(writer_count > 0);
        assert!(total_reads > 0);
        println!("[left_right_triple] writes={writer_count} reads={total_reads} no torn reads");
    }

    /// Verify multiple writers serialize correctly via the mutex.
    #[test]
    fn multiple_writers_serialize() {
        let lr = Arc::new(LeftRight::new(0));
        let barrier = Arc::new(Barrier::new(4));
        let mut handles = Vec::new();

        for _ in 0..4 {
            let l = Arc::clone(&lr);
            let b = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                b.wait();
                for _ in 0..1000 {
                    l.update(|v| v + 1);
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(lr.read("counter"), 4000);
    }

    #[test]
    fn metrics_increment() {
        // Delta-based: snapshot before, act, snapshot after.
        let before = leftright_metrics();
        let lr = LeftRight::new(1);
        lr.read("m1");
        lr.read("m2");
        lr.read("m3");

        let after = leftright_metrics();
        let delta = after.fsqlite_leftright_reads_total - before.fsqlite_leftright_reads_total;
        assert_eq!(delta, 3);
    }

    #[test]
    fn swap_count_increments() {
        // Delta-based: snapshot before, act, snapshot after.
        let before = leftright_metrics();
        let lr = LeftRight::new(0);
        lr.write(1);
        lr.write(2);
        lr.write(3);

        let after = leftright_metrics();
        let delta = after.fsqlite_leftright_swaps_total - before.fsqlite_leftright_swaps_total;
        assert!(delta >= 3, "expected at least 3 swaps, got {delta}");
    }

    /// Debug formatting works without deadlock.
    #[test]
    fn debug_format() {
        let lr = LeftRight::new(42);
        let dbg = format!("{lr:?}");
        assert!(dbg.contains("LeftRight"));
        assert!(
            dbg.contains("left") || dbg.contains("right"),
            "debug must show active side"
        );
    }

    /// Stress test: concurrent readers + writers, check no panics/deadlocks.
    #[test]
    fn stress_concurrent_rw() {
        let lr = Arc::new(LeftRight::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let barrier = Arc::new(Barrier::new(6)); // 2 writers + 4 readers
        let mut handles = Vec::new();

        // 2 writers
        for _ in 0..2 {
            let l = Arc::clone(&lr);
            let st = Arc::clone(&stop);
            let b = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                b.wait();
                let mut writes = 0u64;
                while !st.load(Ordering::Relaxed) {
                    l.update(|v| v.wrapping_add(1));
                    writes += 1;
                }
                writes
            }));
        }

        // 4 readers
        for _ in 0..4 {
            let l = Arc::clone(&lr);
            let st = Arc::clone(&stop);
            let b = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                b.wait();
                let mut reads = 0u64;
                while !st.load(Ordering::Relaxed) {
                    let _ = l.read("stress");
                    reads += 1;
                }
                reads
            }));
        }

        thread::sleep(Duration::from_millis(500));
        stop.store(true, Ordering::Release);

        let mut total_writes = 0u64;
        let mut total_reads = 0u64;
        for (i, h) in handles.into_iter().enumerate() {
            let count = h.join().unwrap();
            if i < 2 {
                total_writes += count;
            } else {
                total_reads += count;
            }
        }

        assert!(total_writes > 0);
        assert!(total_reads > 0);
        println!("[left_right_stress] writes={total_writes} reads={total_reads}");
    }
}
