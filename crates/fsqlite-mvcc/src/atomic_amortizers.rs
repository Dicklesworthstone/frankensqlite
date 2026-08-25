//! Atomic amortizers: batch-reserve timestamps and TIDs to amortize expensive
//! atomic operations across many logical events.
//!
//! This module ships two complementary primitives that apply the same core
//! pattern — atomically reserve a *range* of ordered integers, then
//! hand them out from thread-local / call-local state — to two different
//! hot paths in MVCC:
//!
//! * [`ReadTsBatcher`] — Cicada-style read-timestamp batching. Each thread
//!   pre-reserves a contiguous batch of read timestamps from a shared
//!   [`AtomicU64`]. `next_read_ts()` hands out the next value from that
//!   thread-local batch, only reserving from the shared counter once every
//!   `batch_size` calls. This amortizes the cost of N read-ts reservations
//!   down to ~1 CAS + N local increments.
//!
//! * [`TidGapAllocator`] — Hekaton-style TID gap reservation. One atomic
//!   update of the shared counter reserves a contiguous gap of `gap_size` TIDs,
//!   returning a [`TidGap`] handle. The caller can then draw TIDs from the
//!   gap without any further synchronization. Two concurrent gap
//!   reservations are guaranteed non-overlapping by CAS semantics.
//!
//! # Guarantees
//!
//! * All issued values are **strictly monotonic** within a single batch/gap.
//! * Across the whole allocator, all issued values are **globally unique**
//!   (CAS serializes batch reservations).
//! * Values may be issued **out of strict numeric order across threads**
//!   when two threads hold disjoint batches simultaneously — this is by
//!   design (Cicada's observation: readers only need a snapshot-safe,
//!   unique read-ts, not a globally ordered one).
//!
//! # Non-goals / caveats
//!
//! * If a thread holding an unexhausted batch exits, the remaining values
//!   in that batch are **leaked** (skipped in the global sequence). This is
//!   fine for both use cases: read timestamps and TIDs are opaque monotone
//!   identifiers and gaps are safe.
//! * `batch_size == 0` / `gap_size == 0` is clamped to `1`.
//! * `u64::MAX` is an exhaustion sentinel and is never issued. Allocation
//!   returns `None` after the usable integer domain has been reserved.
//!
//! # No unsafe
//!
//! Implementation is pure safe Rust (AtomicU64 + thread_local + Cell).

use std::cell::Cell;
use std::sync::atomic::{AtomicU64, Ordering};

/// Process-local identity source for TLS-cached read-timestamp batches.
///
/// An address is not a sufficient allocator identity: after one allocator is
/// dropped, a new allocator can occupy the same storage and would otherwise
/// inherit the prior lifetime's unissued TLS values. Instance IDs make that
/// ABA impossible. Exhausting this counter would require constructing 2^64-1
/// batchers in one process, at which point failing fast is safer than aliasing
/// two allocator identities.
static NEXT_READ_TS_BATCHER_ID: AtomicU64 = AtomicU64::new(1);

fn next_read_ts_batcher_id() -> u64 {
    NEXT_READ_TS_BATCHER_ID
        .try_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            current.checked_add(1)
        })
        .expect("ReadTsBatcher instance-id space exhausted")
}

/// Atomically reserve a non-empty half-open range from `shared`.
///
/// `u64::MAX` is kept as an exhaustion sentinel, so a final reservation may
/// be shorter than `size`. The counter never wraps and every successful range
/// has `start < end`.
fn reserve_counter_range(shared: &AtomicU64, size: u64) -> Option<(u64, u64)> {
    debug_assert!(size > 0, "range reservation size must be nonzero");
    let mut start = shared.load(Ordering::Relaxed);
    loop {
        if start == u64::MAX {
            return None;
        }
        let end = start.saturating_add(size);
        match shared.compare_exchange_weak(start, end, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return Some((start, end)),
            Err(observed) => start = observed,
        }
    }
}

// ---------------------------------------------------------------------------
// Cicada read-ts batcher
// ---------------------------------------------------------------------------

/// Per-thread local range reserved from a [`ReadTsBatcher`].
///
/// `next` is the next value to hand out; once `next == end` the batch is
/// exhausted and the next call must CAS-reserve a fresh batch.
#[derive(Debug, Default, Clone, Copy)]
struct LocalBatch {
    next: u64,
    end: u64,
}

impl LocalBatch {
    const EMPTY: Self = Self { next: 0, end: 0 };

    #[inline]
    fn is_exhausted(&self) -> bool {
        self.next >= self.end
    }
}

// Each thread caches one LocalBatch per ReadTsBatcher *instance* it has seen.
// In practice FrankenSQLite wires a single global ReadTsBatcher, so we use a
// single thread-local slot keyed by the batcher's process-unique ID. If a
// thread ever interacts with a different batcher, we transparently re-reserve
// a batch from it; this is correct (never hands out stale values) but slightly
// less efficient for the pathological multi-batcher case.
thread_local! {
    static LOCAL_READ_TS: Cell<(u64, LocalBatch)> = const {
        Cell::new((0, LocalBatch::EMPTY))
    };
}

/// Cicada-style read-timestamp batcher.
///
/// Amortizes N calls to `next_read_ts()` into ~`N / batch_size` CAS
/// operations on the shared counter. Each thread holds its own contiguous
/// batch; within that batch, values are issued by a cheap local increment.
#[derive(Debug)]
pub struct ReadTsBatcher {
    instance_id: u64,
    shared: AtomicU64,
    batch_size: u64,
}

impl ReadTsBatcher {
    /// Create a new batcher starting at `start` with the given `batch_size`.
    ///
    /// `batch_size` is clamped to at least 1. `u64::MAX` is reserved as the
    /// exhaustion sentinel and is never returned by [`Self::next_read_ts`].
    #[must_use]
    pub fn new(start: u64, batch_size: u64) -> Self {
        let b = if batch_size == 0 { 1 } else { batch_size };
        Self {
            instance_id: next_read_ts_batcher_id(),
            shared: AtomicU64::new(start),
            batch_size: b,
        }
    }

    /// Create a new batcher starting at 1 with a default batch size of 64.
    #[must_use]
    pub fn with_default() -> Self {
        Self::new(1, 64)
    }

    /// Hand out the next unique read timestamp.
    ///
    /// Uses a thread-local batch when available; otherwise CAS-reserves a
    /// fresh batch of `batch_size` timestamps from the shared counter.
    /// Returns `None` once the usable integer domain is exhausted.
    pub fn next_read_ts(&self) -> Option<u64> {
        LOCAL_READ_TS.with(|slot| {
            let (key, mut batch) = slot.get();
            if key != self.instance_id || batch.is_exhausted() {
                batch = self.reserve_batch()?;
            }
            // Safe because reserve_batch() always returns a non-empty batch
            // (batch_size >= 1).
            debug_assert!(!batch.is_exhausted());
            let v = batch.next;
            batch.next += 1;
            slot.set((self.instance_id, batch));
            Some(v)
        })
    }

    /// Reserve a contiguous batch of `batch_size` timestamps via checked CAS.
    ///
    /// The uncontended path uses one atomic RMW for the whole range, rather
    /// than one atomic increment per timestamp. A CAS retry is needed only
    /// when another thread reserves a range concurrently.
    fn reserve_batch(&self) -> Option<LocalBatch> {
        let (start, end) = reserve_counter_range(&self.shared, self.batch_size)?;
        Some(LocalBatch { next: start, end })
    }

    /// Observe the current shared watermark (for tests / telemetry).
    ///
    /// Note: any outstanding per-thread batches are *above* this watermark
    /// only in the sense that they have already been reserved from it;
    /// the shared counter itself equals the next-batch-start.
    #[must_use]
    pub fn watermark(&self) -> u64 {
        self.shared.load(Ordering::Relaxed)
    }

    /// Configured batch size.
    #[must_use]
    pub const fn batch_size(&self) -> u64 {
        self.batch_size
    }
}

impl Default for ReadTsBatcher {
    fn default() -> Self {
        Self::with_default()
    }
}

// ---------------------------------------------------------------------------
// Hekaton TID gap allocator
// ---------------------------------------------------------------------------

/// A contiguous, exclusively-owned half-open range `[start, end)` of TIDs
/// reserved from a [`TidGapAllocator`].
///
/// The owner can hand out TIDs via [`TidGap::next_tid`] without any
/// synchronization: the gap is disjoint from every other gap reserved
/// from the same allocator.
#[derive(Debug)]
pub struct TidGap {
    start: u64,
    end: u64,
    next: Cell<u64>,
}

impl TidGap {
    /// First TID in the gap (inclusive).
    #[must_use]
    pub const fn start(&self) -> u64 {
        self.start
    }

    /// End of the gap (exclusive).
    #[must_use]
    pub const fn end(&self) -> u64 {
        self.end
    }

    /// Total size of the gap.
    #[must_use]
    pub const fn capacity(&self) -> u64 {
        self.end - self.start
    }

    /// Hand out the next unused TID in this gap.
    ///
    /// Returns `None` if the gap is exhausted; the caller should then ask
    /// its [`TidGapAllocator`] for a fresh gap.
    pub fn next_tid(&self) -> Option<u64> {
        let v = self.next.get();
        if v >= self.end {
            return None;
        }
        self.next.set(v + 1);
        Some(v)
    }

    /// Number of TIDs remaining in this gap.
    #[must_use]
    pub fn remaining(&self) -> u64 {
        self.end.saturating_sub(self.next.get())
    }
}

/// Hekaton-style TID gap allocator.
///
/// Each call to [`TidGapAllocator::reserve_gap`] reserves `gap_size`
/// contiguous TIDs via one checked atomic range update, returning a [`TidGap`]
/// the caller can draw from without further synchronization. Concurrent
/// reservations may retry the CAS but never reserve overlapping ranges.
#[derive(Debug)]
pub struct TidGapAllocator {
    shared: AtomicU64,
    gap_size: u64,
}

impl TidGapAllocator {
    /// Create a new allocator starting at `start` with the given `gap_size`.
    ///
    /// `gap_size` is clamped to at least 1.
    #[must_use]
    pub const fn new(start: u64, gap_size: u64) -> Self {
        let g = if gap_size == 0 { 1 } else { gap_size };
        Self {
            shared: AtomicU64::new(start),
            gap_size: g,
        }
    }

    /// Create a new allocator starting at 1 with a default gap size of 16.
    #[must_use]
    pub const fn with_default() -> Self {
        Self::new(1, 16)
    }

    /// Reserve a fresh contiguous gap of `gap_size` TIDs.
    ///
    /// Two calls from any threads are guaranteed to return non-overlapping
    /// ranges. Returns `None` once the usable integer domain is exhausted.
    #[must_use]
    pub fn reserve_gap(&self) -> Option<TidGap> {
        let (start, end) = reserve_counter_range(&self.shared, self.gap_size)?;
        Some(TidGap {
            start,
            end,
            next: Cell::new(start),
        })
    }

    /// Observe the current shared watermark (for tests / telemetry).
    #[must_use]
    pub fn watermark(&self) -> u64 {
        self.shared.load(Ordering::Relaxed)
    }

    /// Configured gap size.
    #[must_use]
    pub const fn gap_size(&self) -> u64 {
        self.gap_size
    }
}

impl Default for TidGapAllocator {
    fn default() -> Self {
        Self::with_default()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::sync::mpsc;
    use std::thread;

    // ------- Cicada -------

    /// Single-thread: 1000 next_read_ts calls are all unique and monotonic.
    #[test]
    fn cicada_single_thread_unique_monotonic() {
        let batcher = ReadTsBatcher::new(1, 16);
        let mut last: u64 = 0;
        let mut seen: HashSet<u64> = HashSet::with_capacity(1000);
        for _ in 0..1000 {
            let v = batcher.next_read_ts().expect("timestamp space available");
            assert!(
                v > last,
                "expected strictly monotonic, got {v} after {last}"
            );
            assert!(seen.insert(v), "duplicate read-ts value {v}");
            last = v;
        }
        assert_eq!(seen.len(), 1000);
        // All values should lie in [1, 1 + some multiple of batch_size].
        let max = *seen.iter().max().unwrap();
        assert!(max >= 1000, "max value {max} unexpectedly small");
    }

    /// Multi-thread: 4 threads × 250 calls → 1000 distinct values in aggregate.
    #[test]
    fn cicada_multi_thread_unique() {
        let batcher = Arc::new(ReadTsBatcher::new(1, 16));
        let (tx, rx) = mpsc::channel::<Vec<u64>>();
        let threads: Vec<_> = (0..4)
            .map(|_| {
                let b = Arc::clone(&batcher);
                let tx = tx.clone();
                thread::spawn(move || {
                    let mut local = Vec::with_capacity(250);
                    for _ in 0..250 {
                        local.push(b.next_read_ts().expect("timestamp space available"));
                    }
                    tx.send(local).unwrap();
                })
            })
            .collect();
        drop(tx);
        for t in threads {
            t.join().unwrap();
        }
        let mut all: HashSet<u64> = HashSet::with_capacity(1000);
        while let Ok(batch) = rx.recv() {
            assert_eq!(batch.len(), 250);
            // Within a single thread's trace, values are strictly monotonic.
            for pair in batch.windows(2) {
                assert!(
                    pair[0] < pair[1],
                    "non-monotonic within thread: {} then {}",
                    pair[0],
                    pair[1]
                );
            }
            for v in batch {
                assert!(all.insert(v), "duplicate read-ts across threads: {v}");
            }
        }
        assert_eq!(
            all.len(),
            1000,
            "expected 1000 unique values, got {}",
            all.len()
        );
    }

    /// batch_size == 0 is clamped to 1 and the allocator still works.
    #[test]
    fn cicada_zero_batch_clamped() {
        let batcher = ReadTsBatcher::new(100, 0);
        assert_eq!(batcher.batch_size(), 1);
        let a = batcher.next_read_ts().expect("timestamp space available");
        let b = batcher.next_read_ts().expect("timestamp space available");
        assert!(b > a);
    }

    /// Reserving the final partial batch must never wrap the shared watermark
    /// and reissue low timestamp values.
    #[test]
    fn cicada_id_space_exhaustion_does_not_wrap() {
        let batcher = ReadTsBatcher::new(u64::MAX - 1, 4);
        assert_eq!(batcher.next_read_ts(), Some(u64::MAX - 1));
        assert_eq!(batcher.watermark(), u64::MAX);
        assert_eq!(batcher.next_read_ts(), None);
        assert_eq!(batcher.next_read_ts(), None, "exhaustion is permanent");
    }

    /// The TLS cache must distinguish allocator lifetimes even when a new
    /// allocator is constructed in exactly the same storage address.
    #[test]
    fn cicada_reused_storage_does_not_inherit_prior_batch() {
        let mut storage = Some(ReadTsBatcher::new(10, 4));
        assert_eq!(storage.as_ref().unwrap().next_read_ts(), Some(10));

        storage = None;
        assert!(storage.is_none());
        storage = Some(ReadTsBatcher::new(1_000, 4));

        assert_eq!(
            storage.as_ref().unwrap().next_read_ts(),
            Some(1_000),
            "a new allocator reused the prior lifetime's cached batch"
        );
    }

    // ------- Hekaton -------

    /// 10 reserved gaps are pairwise non-overlapping.
    #[test]
    fn hekaton_ten_gaps_non_overlapping() {
        let alloc = TidGapAllocator::new(1, 8);
        let gaps: Vec<TidGap> = (0..10)
            .map(|_| alloc.reserve_gap().expect("TID space available"))
            .collect();
        // Sort by start and verify no overlaps.
        let mut ranges: Vec<(u64, u64)> = gaps.iter().map(|g| (g.start(), g.end())).collect();
        ranges.sort_by_key(|&(s, _)| s);
        for pair in ranges.windows(2) {
            let (_, e0) = pair[0];
            let (s1, _) = pair[1];
            assert!(e0 <= s1, "overlapping gaps: [_, {e0}) and [{s1}, _)");
        }
        // Each gap hands out exactly gap_size TIDs.
        for g in &gaps {
            assert_eq!(g.capacity(), 8);
            let mut tids = Vec::new();
            while let Some(t) = g.next_tid() {
                tids.push(t);
            }
            assert_eq!(tids.len(), 8);
            // Strictly monotonic within a gap.
            for w in tids.windows(2) {
                assert!(w[0] < w[1]);
            }
        }
        // After exhaustion, next_tid() is None.
        assert_eq!(gaps[0].next_tid(), None);
    }

    /// 4 threads × 25 gaps → 100 non-overlapping gaps in aggregate.
    #[test]
    fn hekaton_multi_thread_non_overlapping() {
        let alloc = Arc::new(TidGapAllocator::new(1, 8));
        let (tx, rx) = mpsc::channel::<Vec<(u64, u64)>>();
        let threads: Vec<_> = (0..4)
            .map(|_| {
                let a = Arc::clone(&alloc);
                let tx = tx.clone();
                thread::spawn(move || {
                    let mut local = Vec::with_capacity(25);
                    for _ in 0..25 {
                        let g = a.reserve_gap().expect("TID space available");
                        local.push((g.start(), g.end()));
                    }
                    tx.send(local).unwrap();
                })
            })
            .collect();
        drop(tx);
        for t in threads {
            t.join().unwrap();
        }
        let mut all: Vec<(u64, u64)> = Vec::with_capacity(100);
        while let Ok(batch) = rx.recv() {
            assert_eq!(batch.len(), 25);
            all.extend(batch);
        }
        assert_eq!(all.len(), 100);
        all.sort_by_key(|&(s, _)| s);
        for pair in all.windows(2) {
            let (s0, e0) = pair[0];
            let (s1, e1) = pair[1];
            assert!(
                e0 <= s1,
                "overlapping gaps in multi-thread trace: [{s0}, {e0}) and [{s1}, {e1})",
            );
            // And each gap is exactly gap_size wide.
            assert_eq!(e0 - s0, 8);
        }
        // Also check the full set of start values is unique.
        let starts: HashSet<u64> = all.iter().map(|&(s, _)| s).collect();
        assert_eq!(starts.len(), 100);
    }

    /// Calling next_tid() past end yields None without panicking or wrapping.
    #[test]
    fn hekaton_gap_exhaustion() {
        let alloc = TidGapAllocator::new(1000, 3);
        let g = alloc.reserve_gap().expect("TID space available");
        assert_eq!(g.start(), 1000);
        assert_eq!(g.end(), 1003);
        assert_eq!(g.next_tid(), Some(1000));
        assert_eq!(g.next_tid(), Some(1001));
        assert_eq!(g.next_tid(), Some(1002));
        assert_eq!(g.next_tid(), None);
        assert_eq!(g.next_tid(), None); // still None, idempotent
        assert_eq!(g.remaining(), 0);
    }

    /// gap_size == 0 is clamped to 1.
    #[test]
    fn hekaton_zero_gap_clamped() {
        let alloc = TidGapAllocator::new(5, 0);
        assert_eq!(alloc.gap_size(), 1);
        let g = alloc.reserve_gap().expect("TID space available");
        assert_eq!(g.capacity(), 1);
        assert_eq!(g.next_tid(), Some(5));
        assert_eq!(g.next_tid(), None);
    }

    /// Reserving the final partial gap must leave the allocator exhausted,
    /// rather than wrapping its shared watermark to a reusable low value.
    #[test]
    fn hekaton_id_space_exhaustion_does_not_wrap() {
        let alloc = TidGapAllocator::new(u64::MAX - 1, 4);
        let gap = alloc.reserve_gap().expect("final partial gap available");
        assert_eq!(gap.start(), u64::MAX - 1);
        assert_eq!(gap.end(), u64::MAX);
        assert_eq!(
            alloc.watermark(),
            u64::MAX,
            "gap reservation wrapped the shared watermark"
        );
        assert!(alloc.reserve_gap().is_none(), "exhaustion is permanent");
    }
}
