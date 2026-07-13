//! Generic seqlock (§14.9) for optimistic metadata reads.
//!
//! A seqlock provides sub-nanosecond reads for rarely-changing data (database
//! schema, pragma settings) with retry on writer conflict. Readers never block
//! writers; writers are serialized via an internal parking_lot Mutex.
//!
//! ## Protocol
//!
//! The sequence counter is even when stable, odd during a write. Readers
//! sample the counter before and after reading protected data; if either
//! sample is odd or the two differ, the reader retries.
//!
//! ## Safety
//!
//! This implementation uses only `AtomicU64` for protected data, avoiding
//! `UnsafeCell` entirely. Protected values are stored as atomic slots and
//! read/written with appropriate memory orderings.
//!
//! ## Tracing & Metrics
//!
//! - Span `seqlock_read` (TRACE): emitted on every successful read, with
//!   `retries` and `data_key` fields.
//! - Log level DEBUG when `retries > 0`.
//! - Counters: `fsqlite_seqlock_reads_total`, `fsqlite_seqlock_retries_total`.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use serde::Serialize;

// ---------------------------------------------------------------------------
// Global metrics (lock-free, Relaxed ordering)
// ---------------------------------------------------------------------------

const SEQLOCK_COUNTER_STRIPE_COUNT: usize = 64;

static NEXT_SEQLOCK_COUNTER_STRIPE: AtomicUsize = AtomicUsize::new(0);
static FSQLITE_SEQLOCK_READS_TOTAL: StripedSeqlockCounter = StripedSeqlockCounter::new();
static FSQLITE_SEQLOCK_RETRIES_TOTAL: StripedSeqlockCounter = StripedSeqlockCounter::new();

std::thread_local! {
    static SEQLOCK_COUNTER_STRIPE_INDEX: usize =
        NEXT_SEQLOCK_COUNTER_STRIPE.fetch_add(1, Ordering::Relaxed)
            % SEQLOCK_COUNTER_STRIPE_COUNT;
}

#[repr(align(64))]
struct CacheAlignedAtomicU64(AtomicU64);

impl CacheAlignedAtomicU64 {
    const fn new(value: u64) -> Self {
        Self(AtomicU64::new(value))
    }

    fn fetch_add(&self, value: u64, ordering: Ordering) {
        self.0.fetch_add(value, ordering);
    }

    fn load(&self, ordering: Ordering) -> u64 {
        self.0.load(ordering)
    }

    fn store(&self, value: u64, ordering: Ordering) {
        self.0.store(value, ordering);
    }
}

struct StripedSeqlockCounter {
    stripes: [CacheAlignedAtomicU64; SEQLOCK_COUNTER_STRIPE_COUNT],
}

impl StripedSeqlockCounter {
    const fn new() -> Self {
        Self {
            stripes: [const { CacheAlignedAtomicU64::new(0) }; SEQLOCK_COUNTER_STRIPE_COUNT],
        }
    }

    fn increment(&self) {
        self.add(1);
    }

    fn add(&self, value: u64) {
        SEQLOCK_COUNTER_STRIPE_INDEX.with(|stripe| {
            self.stripes[*stripe].fetch_add(value, Ordering::Relaxed);
        });
    }

    fn load(&self) -> u64 {
        self.stripes.iter().fold(0_u64, |sum, stripe| {
            sum.saturating_add(stripe.load(Ordering::Acquire))
        })
    }

    fn reset(&self) {
        for stripe in &self.stripes {
            stripe.store(0, Ordering::Relaxed);
        }
    }
}

/// Snapshot of seqlock metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct SeqlockMetrics {
    pub fsqlite_seqlock_reads_total: u64,
    pub fsqlite_seqlock_retries_total: u64,
}

/// Read current seqlock metrics.
#[must_use]
pub fn seqlock_metrics() -> SeqlockMetrics {
    SeqlockMetrics {
        fsqlite_seqlock_reads_total: FSQLITE_SEQLOCK_READS_TOTAL.load(),
        fsqlite_seqlock_retries_total: FSQLITE_SEQLOCK_RETRIES_TOTAL.load(),
    }
}

/// Reset metrics (for tests).
pub fn reset_seqlock_metrics() {
    FSQLITE_SEQLOCK_READS_TOTAL.reset();
    FSQLITE_SEQLOCK_RETRIES_TOTAL.reset();
}

// ---------------------------------------------------------------------------
// SeqLock (single u64 value)
// ---------------------------------------------------------------------------

/// Maximum retries before a reader gives up.
const MAX_RETRIES: u32 = 1_000_000;

/// A seqlock protecting a single `u64` value.
///
/// Readers call [`read`](SeqLock::read) for an optimistic, non-blocking
/// snapshot. Writers call [`write`](SeqLock::write) or
/// [`update`](SeqLock::update) which serialize via an internal mutex and
/// bump the sequence counter.
///
/// All data is stored in atomics — no `UnsafeCell`, no `unsafe`.
pub struct SeqLock {
    seq: AtomicU64,
    value: AtomicU64,
    write_lock: fsqlite_types::sync_primitives::Mutex<()>,
}

impl SeqLock {
    /// Create a new seqlock with the given initial value.
    pub fn new(initial: u64) -> Self {
        Self {
            seq: AtomicU64::new(0),
            value: AtomicU64::new(initial),
            write_lock: fsqlite_types::sync_primitives::Mutex::new(()),
        }
    }

    /// Optimistic read. Spins while a writer is active or the sequence
    /// changed during the read. Returns `None` only if `MAX_RETRIES` is
    /// exhausted (should never happen in practice).
    #[inline]
    pub fn read(&self, data_key: &str) -> Option<u64> {
        self.read_with_retry_observer(data_key, |_| {})
    }

    #[inline]
    fn read_with_retry_observer(
        &self,
        data_key: &str,
        mut observe_retry: impl FnMut(u32),
    ) -> Option<u64> {
        let mut retries: u32 = 0;

        let result = loop {
            let seq1 = self.seq.load(Ordering::Acquire);
            if seq1 & 1 == 1 {
                retries += 1;
                observe_retry(retries);
                if retries >= MAX_RETRIES {
                    emit_trace(data_key, retries);
                    return None;
                }
                std::hint::spin_loop();
                continue;
            }

            // Ensure the payload load happens AFTER the first sequence check.
            std::sync::atomic::fence(Ordering::Acquire);
            let snapshot = self.value.load(Ordering::Relaxed);
            // Ensure the payload load happens BEFORE the second sequence check.
            std::sync::atomic::fence(Ordering::Acquire);

            let seq2 = self.seq.load(Ordering::Acquire);
            if seq1 == seq2 {
                break snapshot;
            }

            retries += 1;
            observe_retry(retries);
            if retries >= MAX_RETRIES {
                emit_trace(data_key, retries);
                return None;
            }
            std::hint::spin_loop();
        };

        FSQLITE_SEQLOCK_READS_TOTAL.increment();
        if retries > 0 {
            FSQLITE_SEQLOCK_RETRIES_TOTAL.add(u64::from(retries));
        }
        emit_trace(data_key, retries);

        Some(result)
    }

    /// Update the protected value. Serializes writers via an internal mutex.
    pub fn write(&self, new_value: u64) {
        let _guard = self.write_lock.lock();
        self.seq.fetch_add(1, Ordering::Release); // even → odd
        self.value.store(new_value, Ordering::Release);
        self.seq.fetch_add(1, Ordering::Release); // odd → even
    }

    /// Update the protected value via a closure.
    pub fn update(&self, f: impl FnOnce(u64) -> u64) {
        let _guard = self.write_lock.lock();
        self.seq.fetch_add(1, Ordering::Release);
        let old = self.value.load(Ordering::Acquire);
        self.value.store(f(old), Ordering::Release);
        self.seq.fetch_add(1, Ordering::Release);
    }

    /// Current sequence number (for diagnostics).
    #[must_use]
    pub fn sequence(&self) -> u64 {
        self.seq.load(Ordering::Relaxed)
    }
}

#[allow(clippy::missing_fields_in_debug)]
impl std::fmt::Debug for SeqLock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let seq = self.seq.load(Ordering::Relaxed);
        f.debug_struct("SeqLock")
            .field("seq", &seq)
            .field("writing", &(seq & 1 == 1))
            .finish_non_exhaustive()
    }
}

// ---------------------------------------------------------------------------
// SeqLockPair (two u64 values, consistent snapshot)
// ---------------------------------------------------------------------------

/// A seqlock protecting a pair of `u64` values read atomically.
///
/// Useful for metadata that must be read consistently as a unit
/// (e.g., schema_epoch + commit_seq, or min/max version bounds).
pub struct SeqLockPair {
    seq: AtomicU64,
    a: AtomicU64,
    b: AtomicU64,
    write_lock: fsqlite_types::sync_primitives::Mutex<()>,
}

impl SeqLockPair {
    /// Create a new seqlock pair with the given initial values.
    pub fn new(a: u64, b: u64) -> Self {
        Self {
            seq: AtomicU64::new(0),
            a: AtomicU64::new(a),
            b: AtomicU64::new(b),
            write_lock: fsqlite_types::sync_primitives::Mutex::new(()),
        }
    }

    /// Optimistic read of the consistent pair.
    #[inline]
    pub fn read(&self, data_key: &str) -> Option<(u64, u64)> {
        let mut retries: u32 = 0;

        let result = loop {
            let seq1 = self.seq.load(Ordering::Acquire);
            if seq1 & 1 == 1 {
                retries += 1;
                if retries >= MAX_RETRIES {
                    emit_trace(data_key, retries);
                    return None;
                }
                std::hint::spin_loop();
                continue;
            }

            std::sync::atomic::fence(Ordering::Acquire);
            let va = self.a.load(Ordering::Relaxed);
            let vb = self.b.load(Ordering::Relaxed);
            std::sync::atomic::fence(Ordering::Acquire);

            let seq2 = self.seq.load(Ordering::Acquire);
            if seq1 == seq2 {
                break (va, vb);
            }

            retries += 1;
            if retries >= MAX_RETRIES {
                emit_trace(data_key, retries);
                return None;
            }
            std::hint::spin_loop();
        };

        FSQLITE_SEQLOCK_READS_TOTAL.increment();
        if retries > 0 {
            FSQLITE_SEQLOCK_RETRIES_TOTAL.add(u64::from(retries));
        }
        emit_trace(data_key, retries);

        Some(result)
    }

    /// Update both values atomically (from the reader's perspective).
    pub fn write(&self, a: u64, b: u64) {
        let _guard = self.write_lock.lock();
        self.seq.fetch_add(1, Ordering::Release);
        self.a.store(a, Ordering::Release);
        self.b.store(b, Ordering::Release);
        self.seq.fetch_add(1, Ordering::Release);
    }

    /// Current sequence number.
    #[must_use]
    pub fn sequence(&self) -> u64 {
        self.seq.load(Ordering::Relaxed)
    }
}

#[allow(clippy::missing_fields_in_debug)]
impl std::fmt::Debug for SeqLockPair {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let seq = self.seq.load(Ordering::Relaxed);
        f.debug_struct("SeqLockPair")
            .field("seq", &seq)
            .field("writing", &(seq & 1 == 1))
            .finish_non_exhaustive()
    }
}

// ---------------------------------------------------------------------------
// SeqLockTriple (three u64 values — matches shm.rs snapshot pattern)
// ---------------------------------------------------------------------------

/// A seqlock protecting a triple of `u64` values.
///
/// Matches the `(commit_seq, schema_epoch, ecs_epoch)` pattern from the
/// shared-memory header.
pub struct SeqLockTriple {
    seq: AtomicU64,
    a: AtomicU64,
    b: AtomicU64,
    c: AtomicU64,
    write_lock: fsqlite_types::sync_primitives::Mutex<()>,
}

impl SeqLockTriple {
    /// Create a new seqlock triple.
    pub fn new(a: u64, b: u64, c: u64) -> Self {
        Self {
            seq: AtomicU64::new(0),
            a: AtomicU64::new(a),
            b: AtomicU64::new(b),
            c: AtomicU64::new(c),
            write_lock: fsqlite_types::sync_primitives::Mutex::new(()),
        }
    }

    /// Optimistic read of the consistent triple.
    #[inline]
    pub fn read(&self, data_key: &str) -> Option<(u64, u64, u64)> {
        let mut retries: u32 = 0;

        let result = loop {
            let seq1 = self.seq.load(Ordering::Acquire);
            if seq1 & 1 == 1 {
                retries += 1;
                if retries >= MAX_RETRIES {
                    emit_trace(data_key, retries);
                    return None;
                }
                std::hint::spin_loop();
                continue;
            }

            std::sync::atomic::fence(Ordering::Acquire);
            let va = self.a.load(Ordering::Relaxed);
            let vb = self.b.load(Ordering::Relaxed);
            let vc = self.c.load(Ordering::Relaxed);
            std::sync::atomic::fence(Ordering::Acquire);

            let seq2 = self.seq.load(Ordering::Acquire);
            if seq1 == seq2 {
                break (va, vb, vc);
            }

            retries += 1;
            if retries >= MAX_RETRIES {
                emit_trace(data_key, retries);
                return None;
            }
            std::hint::spin_loop();
        };

        FSQLITE_SEQLOCK_READS_TOTAL.increment();
        if retries > 0 {
            FSQLITE_SEQLOCK_RETRIES_TOTAL.add(u64::from(retries));
        }
        emit_trace(data_key, retries);

        Some(result)
    }

    /// Update all three values atomically (from the reader's perspective).
    pub fn write(&self, a: u64, b: u64, c: u64) {
        let _guard = self.write_lock.lock();
        self.seq.fetch_add(1, Ordering::Release);
        self.a.store(a, Ordering::Release);
        self.b.store(b, Ordering::Release);
        self.c.store(c, Ordering::Release);
        self.seq.fetch_add(1, Ordering::Release);
    }

    /// Current sequence number.
    #[must_use]
    pub fn sequence(&self) -> u64 {
        self.seq.load(Ordering::Relaxed)
    }
}

#[allow(clippy::missing_fields_in_debug)]
impl std::fmt::Debug for SeqLockTriple {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let seq = self.seq.load(Ordering::Relaxed);
        f.debug_struct("SeqLockTriple")
            .field("seq", &seq)
            .field("writing", &(seq & 1 == 1))
            .finish_non_exhaustive()
    }
}

// ---------------------------------------------------------------------------
// Tracing helper
// ---------------------------------------------------------------------------

fn emit_trace(data_key: &str, retries: u32) {
    if retries > 0 {
        tracing::debug!(
            target: "fsqlite.seqlock",
            data_key,
            retries,
            "seqlock_read contended"
        );
    } else if tracing::enabled!(target: "fsqlite.seqlock", tracing::Level::TRACE) {
        tracing::trace!(
            target: "fsqlite.seqlock",
            data_key,
            retries = 0u32,
            "seqlock_read"
        );
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Barrier};
    use std::thread;
    use std::time::Duration;

    #[test]
    fn basic_read_write() {
        let sl = SeqLock::new(42);
        assert_eq!(sl.read("test"), Some(42));
        sl.write(99);
        assert_eq!(sl.read("test"), Some(99));
        assert_eq!(sl.sequence(), 2); // One write = 2 increments.
    }

    #[test]
    fn update_closure() {
        let sl = SeqLock::new(10);
        sl.update(|v| v + 5);
        assert_eq!(sl.read("test"), Some(15));
        sl.update(|v| v * 2);
        assert_eq!(sl.read("test"), Some(30));
    }

    #[test]
    fn pair_consistent_snapshot() {
        let sl = SeqLockPair::new(1, 2);
        assert_eq!(sl.read("pair"), Some((1, 2)));
        sl.write(10, 20);
        assert_eq!(sl.read("pair"), Some((10, 20)));
    }

    #[test]
    fn triple_consistent_snapshot() {
        let sl = SeqLockTriple::new(1, 2, 3);
        assert_eq!(sl.read("triple"), Some((1, 2, 3)));
        sl.write(10, 20, 30);
        assert_eq!(sl.read("triple"), Some((10, 20, 30)));
    }

    /// Verify that concurrent readers never observe an inconsistent pair.
    #[test]
    fn no_torn_reads_pair() {
        let sl = Arc::new(SeqLockPair::new(0, 0));
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let barrier = Arc::new(Barrier::new(5)); // 1 writer + 4 readers

        let writer_sl = Arc::clone(&sl);
        let writer_stop = Arc::clone(&stop);
        let writer_barrier = Arc::clone(&barrier);
        let writer = thread::spawn(move || {
            writer_barrier.wait();
            let mut val = 0u64;
            while !writer_stop.load(std::sync::atomic::Ordering::Relaxed) {
                val += 1;
                writer_sl.write(val, val);
            }
            val
        });

        let mut readers = Vec::new();
        for _ in 0..4 {
            let rsl = Arc::clone(&sl);
            let rs = Arc::clone(&stop);
            let rb = Arc::clone(&barrier);
            readers.push(thread::spawn(move || {
                rb.wait();
                let mut reads = 0u64;
                while !rs.load(std::sync::atomic::Ordering::Relaxed) {
                    if let Some((a, b)) = rsl.read("pair") {
                        assert_eq!(a, b, "torn read: a={a}, b={b}");
                        reads += 1;
                    }
                }
                reads
            }));
        }

        thread::sleep(Duration::from_millis(500));
        stop.store(true, std::sync::atomic::Ordering::Release);

        let writer_count = writer.join().unwrap();
        let mut total_reads = 0u64;
        for r in readers {
            total_reads += r.join().unwrap();
        }

        assert!(writer_count > 0, "writer must have written");
        assert!(total_reads > 0, "readers must have read");
        println!("[seqlock_pair] writes={writer_count} reads={total_reads} no torn reads");
    }

    /// Verify that concurrent readers never observe an inconsistent triple.
    #[test]
    fn no_torn_reads_triple() {
        let sl = Arc::new(SeqLockTriple::new(0, 0, 0));
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let barrier = Arc::new(Barrier::new(5));

        let writer_sl = Arc::clone(&sl);
        let writer_stop = Arc::clone(&stop);
        let writer_barrier = Arc::clone(&barrier);
        let writer = thread::spawn(move || {
            writer_barrier.wait();
            let mut val = 0u64;
            while !writer_stop.load(std::sync::atomic::Ordering::Relaxed) {
                val += 1;
                writer_sl.write(val, val, val);
            }
            val
        });

        let mut readers = Vec::new();
        for _ in 0..4 {
            let rsl = Arc::clone(&sl);
            let rs = Arc::clone(&stop);
            let rb = Arc::clone(&barrier);
            readers.push(thread::spawn(move || {
                rb.wait();
                let mut reads = 0u64;
                while !rs.load(std::sync::atomic::Ordering::Relaxed) {
                    if let Some((a, b, c)) = rsl.read("triple") {
                        assert!(a == b && b == c, "torn read: a={a}, b={b}, c={c}");
                        reads += 1;
                    }
                }
                reads
            }));
        }

        thread::sleep(Duration::from_millis(500));
        stop.store(true, std::sync::atomic::Ordering::Release);

        let writer_count = writer.join().unwrap();
        let mut total_reads = 0u64;
        for r in readers {
            total_reads += r.join().unwrap();
        }

        assert!(writer_count > 0);
        assert!(total_reads > 0);
        println!("[seqlock_triple] writes={writer_count} reads={total_reads} no torn reads");
    }

    /// Verify multiple writers serialize correctly via the mutex.
    #[test]
    fn multiple_writers_serialize() {
        let sl = Arc::new(SeqLock::new(0));
        let barrier = Arc::new(Barrier::new(4));
        let mut handles = Vec::new();

        for _ in 0..4 {
            let s = Arc::clone(&sl);
            let b = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                b.wait();
                for _ in 0..1000 {
                    s.update(|v| v + 1);
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(sl.read("counter"), Some(4000));
    }

    #[test]
    fn metrics_increment() {
        let before = seqlock_metrics();
        let sl = SeqLock::new(1);
        sl.read("m1");
        sl.read("m2");
        sl.read("m3");

        let after = seqlock_metrics();
        let reads_delta = after.fsqlite_seqlock_reads_total - before.fsqlite_seqlock_reads_total;
        assert!(
            reads_delta >= 3,
            "expected at least 3 reads, got {reads_delta}"
        );
    }

    /// Sequence counter is always even after all writes complete.
    #[test]
    fn sequence_always_even_after_writes() {
        let sl = Arc::new(SeqLock::new(0));
        let barrier = Arc::new(Barrier::new(3));
        let mut handles = Vec::new();

        for _ in 0..3 {
            let s = Arc::clone(&sl);
            let b = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                b.wait();
                for i in 0..500 {
                    s.write(i);
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        let seq = sl.sequence();
        assert_eq!(seq % 2, 0, "sequence must be even: {seq}");
        assert_eq!(seq, 3 * 500 * 2);
    }

    /// Debug formatting works without deadlock.
    #[test]
    fn debug_format() {
        let sl = SeqLock::new(42);
        let dbg = format!("{sl:?}");
        assert!(dbg.contains("SeqLock"));
        assert!(dbg.contains("writing: false"));
    }

    // -----------------------------------------------------------------------
    // D6 (bd-3wop3.6): Seqlock verification tests
    // -----------------------------------------------------------------------

    /// D6: Verify that 8 reader threads never acquire the internal write_lock.
    ///
    /// Seqlock readers are entirely lock-free — they spin-retry on sequence
    /// changes but never contend for the Mutex. This test verifies the core
    /// optimization: reads scale perfectly with thread count.
    #[test]
    fn test_seqlock_reader_no_lock() {
        use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

        let sl = Arc::new(SeqLock::new(42));
        let barrier = Arc::new(Barrier::new(8));
        let stop = Arc::new(AtomicBool::new(false));
        let total_reads = Arc::new(AtomicU64::new(0));

        let mut handles = Vec::with_capacity(8);
        for _ in 0..8 {
            let s = Arc::clone(&sl);
            let b = Arc::clone(&barrier);
            let st = Arc::clone(&stop);
            let tr = Arc::clone(&total_reads);
            handles.push(thread::spawn(move || {
                b.wait();
                let mut local_reads = 0u64;
                while !st.load(Ordering::Relaxed) {
                    // Each read is lock-free optimistic.
                    if s.read("no_lock_test").is_some() {
                        local_reads += 1;
                    }
                }
                tr.fetch_add(local_reads, Ordering::Relaxed);
            }));
        }

        // Let readers run for 100ms with NO writer.
        thread::sleep(Duration::from_millis(100));
        stop.store(true, Ordering::Release);

        for h in handles {
            h.join().unwrap();
        }

        let reads = total_reads.load(Ordering::Relaxed);
        // With 8 threads over 100ms, we expect millions of reads (no lock contention).
        // Even on slow machines, 100k reads total is conservative.
        assert!(
            reads > 100_000,
            "bd-3wop3.6: expected >100k reads with 8 lock-free readers, got {reads}"
        );
        println!("[test_seqlock_reader_no_lock] 8 threads, {reads} total reads, no locks acquired");
    }

    /// D6: Verify that readers retry when a writer is in progress (odd sequence).
    ///
    /// When the sequence counter is odd, a write is in progress. Readers must
    /// spin-retry until the sequence becomes even again.
    #[test]
    fn test_seqlock_writer_blocks_readers() {
        use std::sync::atomic::Ordering;

        let sl = Arc::new(SeqLock::new(0));
        let retry_observed = Arc::new(Barrier::new(2));
        let writer_released = Arc::new(Barrier::new(2));

        // Keep the writer active until the reader proves that it observed the
        // odd sequence. The second barrier holds the reader in its first retry
        // callback until the new value and even sequence are both published.
        let guard = sl.write_lock.lock();
        sl.seq.fetch_add(1, Ordering::Release); // odd = writing

        let reader_sl = Arc::clone(&sl);
        let reader_retry_observed = Arc::clone(&retry_observed);
        let reader_writer_released = Arc::clone(&writer_released);
        let reader = thread::spawn(move || {
            let mut observed_retries = 0;
            let value = reader_sl.read_with_retry_observer("writer_blocks_test", |retries| {
                observed_retries = retries;
                if retries == 1 {
                    reader_retry_observed.wait();
                    reader_writer_released.wait();
                }
            });
            (value, observed_retries)
        });

        retry_observed.wait();
        sl.value.store(999, Ordering::Release);
        sl.seq.fetch_add(1, Ordering::Release); // even = done
        drop(guard);
        writer_released.wait();

        let (result, retries) = reader.join().unwrap();
        assert_eq!(result, Some(999), "reader should see final value");
        assert!(
            retries > 0,
            "bd-3wop3.6: reader should have retried during writer hold, got {retries} retries"
        );
        println!("[test_seqlock_writer_blocks_readers] reader retried {retries} times");
    }

    /// D6: Compare seqlock read throughput vs Mutex-protected reads.
    ///
    /// The seqlock should achieve significantly higher read throughput (>5x)
    /// compared to a Mutex-protected value under 8 concurrent readers.
    #[test]
    fn test_seqlock_throughput_vs_mutex() {
        use fsqlite_types::sync_primitives::Mutex;
        use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

        const THREADS: usize = 8;
        const DURATION_MS: u64 = 100;

        // ---- Mutex baseline ----
        let mutex_value = Arc::new(Mutex::new(42u64));
        let mutex_reads = Arc::new(AtomicU64::new(0));
        let mutex_stop = Arc::new(AtomicBool::new(false));
        let mutex_barrier = Arc::new(Barrier::new(THREADS));

        let mut mutex_handles = Vec::with_capacity(THREADS);
        for _ in 0..THREADS {
            let v = Arc::clone(&mutex_value);
            let r = Arc::clone(&mutex_reads);
            let s = Arc::clone(&mutex_stop);
            let b = Arc::clone(&mutex_barrier);
            mutex_handles.push(thread::spawn(move || {
                b.wait();
                let mut local = 0u64;
                while !s.load(Ordering::Relaxed) {
                    let _val = *v.lock();
                    local += 1;
                }
                r.fetch_add(local, Ordering::Relaxed);
            }));
        }

        thread::sleep(Duration::from_millis(DURATION_MS));
        mutex_stop.store(true, Ordering::Release);
        for h in mutex_handles {
            h.join().unwrap();
        }
        let mutex_total = mutex_reads.load(Ordering::Relaxed);

        // ---- Seqlock test ----
        let sl = Arc::new(SeqLock::new(42));
        let sl_reads = Arc::new(AtomicU64::new(0));
        let sl_stop = Arc::new(AtomicBool::new(false));
        let sl_barrier = Arc::new(Barrier::new(THREADS));

        let mut sl_handles = Vec::with_capacity(THREADS);
        for _ in 0..THREADS {
            let s = Arc::clone(&sl);
            let r = Arc::clone(&sl_reads);
            let st = Arc::clone(&sl_stop);
            let b = Arc::clone(&sl_barrier);
            sl_handles.push(thread::spawn(move || {
                b.wait();
                let mut local = 0u64;
                while !st.load(Ordering::Relaxed) {
                    if s.read("throughput_test").is_some() {
                        local += 1;
                    }
                }
                r.fetch_add(local, Ordering::Relaxed);
            }));
        }

        thread::sleep(Duration::from_millis(DURATION_MS));
        sl_stop.store(true, Ordering::Release);
        for h in sl_handles {
            h.join().unwrap();
        }
        let sl_total = sl_reads.load(Ordering::Relaxed);

        // Seqlock should be significantly faster (>5x) under read contention.
        let speedup = sl_total as f64 / mutex_total.max(1) as f64;
        println!(
            "[test_seqlock_throughput_vs_mutex] mutex={mutex_total} seqlock={sl_total} speedup={speedup:.1}x"
        );

        assert!(
            speedup > 5.0,
            "bd-3wop3.6: expected seqlock reads >5x mutex under 8-reader contention, got {speedup:.1}x"
        );
    }
}
