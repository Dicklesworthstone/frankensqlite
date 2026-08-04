//! MICA-style partitioned commit log primitive (AG-5C / IMPL-24).
//!
//! MT-bench shows fsqlite degrading from 4 to 8 concurrent writer threads due
//! to commit-log contention: every committer serializes on a single queue lock.
//! MICA (Lim et al., "MICA: A Holistic Approach to Fast In-Memory Key-Value
//! Storage", NSDI 2014) sidesteps this by *partitioning* per-core / per-thread
//! state so the hot path never touches a cross-core mutex.
//!
//! This module is a standalone MVP of that idea for the commit log. Each
//! logical "shard" owns its own short-lived `Mutex<Vec<CommitEntry>>` so that
//! the `append` hot path only contends with appenders that land on the same
//! shard (expected to be roughly the number of threads / `num_shards`). A
//! reader path — needed for audit, time-travel and recovery — merges all
//! shards into a single `commit_seq`-ordered stream via a BinaryHeap
//! (O(N log S)).
//!
//! # Status
//!
//! This is a primitive: it is **not wired into the real commit path**. It is
//! intended to be composed with [`crate::core_types::CommitLog`] and
//! [`crate::write_coordinator::WriteCoordinator`] in follow-up work. Keeping
//! the merge semantics isolated makes it easy to unit-test and to swap the
//! shard-selection strategy later (e.g. to a real CPU-id once we add a safe
//! wrapper around `sched_getcpu`).
//!
//! # Shard selection
//!
//! Shard selection is deterministic per thread: we hash
//! `std::thread::current().id().as_u64()` through a SplitMix64-style mix and
//! take `hash % num_shards`. This avoids the nightly-only `ThreadId::as_u64`
//! gotcha and never requires `unsafe`. A good hash mixer matters here because
//! [`std::thread::ThreadId`] values produced by a busy runtime are often
//! small consecutive integers, which would otherwise alias onto the same
//! shard under plain `id % num_shards`.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::hash::{Hash, Hasher};
use std::sync::Mutex;
use std::thread;

use xxhash_rust::xxh3::Xxh3;

/// A single commit record in the partitioned log.
///
/// Entries carry their own `commit_seq` so the reader can reconstruct a
/// globally ordered stream regardless of which shard accepted each append.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitEntry {
    /// Global commit sequence number. Ordering across the whole log is
    /// defined solely by this field.
    pub commit_seq: u64,
    /// Pages modified by this commit. Left opaque to the log: downstream
    /// consumers (GC, recovery, time-travel) interpret them.
    pub page_numbers: Vec<u32>,
    /// Wall-clock timestamp (nanoseconds since `UNIX_EPOCH`) captured at
    /// append time. Informational only; ordering never depends on it.
    pub timestamp_ns: u64,
}

/// One partition of the commit log.
///
/// The mutex is held only for the duration of a `Vec::push` (entries are
/// moved in, not cloned) so contention is bounded even under heavy same-shard
/// traffic. `shard_seq` is a per-shard monotonic counter exposed for
/// diagnostics; it is *not* used to derive the global `commit_seq`.
#[derive(Debug, Default)]
pub struct MicaShard {
    entries: Mutex<Vec<CommitEntry>>,
    shard_seq: std::sync::atomic::AtomicU64,
}

impl MicaShard {
    fn new() -> Self {
        Self::default()
    }

    /// Current per-shard append count. Useful for load-balance telemetry.
    #[must_use]
    pub fn shard_seq(&self) -> u64 {
        self.shard_seq.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Number of entries currently held in this shard.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.lock().expect("MicaShard mutex poisoned").len()
    }

    /// Whether this shard has any entries.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// A MICA-style partitioned commit log.
///
/// See the module docs for the sharding strategy and reader semantics.
#[derive(Debug)]
pub struct MicaCommitLog {
    shards: Vec<MicaShard>,
    num_shards: usize,
}

impl MicaCommitLog {
    /// Create a new log with `num_shards` partitions. `num_shards` is
    /// clamped to at least 1 so callers that pass `0` (e.g. because the
    /// host reports no usable CPUs) still get a functioning log.
    #[must_use]
    pub fn new(num_shards: usize) -> Self {
        let num_shards = num_shards.max(1);
        let mut shards = Vec::with_capacity(num_shards);
        for _ in 0..num_shards {
            shards.push(MicaShard::new());
        }
        Self { shards, num_shards }
    }

    /// Number of shards in this log.
    #[must_use]
    pub fn shard_count(&self) -> usize {
        self.num_shards
    }

    /// Append a new commit entry. The shard is selected by hashing the
    /// current thread id (see module docs). Mutex hold time is bounded to
    /// one `Vec::push`.
    pub fn append(&self, commit_seq: u64, page_numbers: Vec<u32>) {
        let idx = self.shard_for_current_thread();
        let entry = CommitEntry {
            commit_seq,
            page_numbers,
            timestamp_ns: now_ns(),
        };

        let shard = &self.shards[idx];
        {
            let mut guard = shard.entries.lock().expect("MicaShard mutex poisoned");
            guard.push(entry);
        }
        shard
            .shard_seq
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// Merge every shard into a single `commit_seq`-ordered `Vec`.
    ///
    /// Complexity is `O(N log S)` where `N` is the total entry count and
    /// `S` is the number of shards: we sort each shard locally then
    /// heap-merge the resulting sorted runs. Ties on `commit_seq` are
    /// broken by shard index, which keeps the output deterministic.
    ///
    /// This method is intended for cold / batch consumers (audit, recovery,
    /// time-travel). The hot append path does not call it.
    #[must_use]
    pub fn merge_ordered(&self) -> Vec<CommitEntry> {
        // Snapshot each shard. We sort each snapshot locally so the heap
        // merge below only needs to compare the current head of each shard.
        let mut shard_runs: Vec<Vec<CommitEntry>> = Vec::with_capacity(self.num_shards);
        let mut total = 0usize;
        for shard in &self.shards {
            let guard = shard.entries.lock().expect("MicaShard mutex poisoned");
            let mut run = guard.clone();
            drop(guard);
            run.sort_by_key(|entry| entry.commit_seq);
            total += run.len();
            shard_runs.push(run);
        }

        if total == 0 {
            return Vec::new();
        }

        // Heap of (commit_seq, shard_index, position-in-shard). We store
        // positions instead of popping from the fronts of the Vecs to keep
        // the merge allocation-free beyond the initial snapshot.
        let mut heap: BinaryHeap<HeapHead> = BinaryHeap::with_capacity(shard_runs.len());
        for (shard_idx, run) in shard_runs.iter().enumerate() {
            if let Some(entry) = run.first() {
                heap.push(HeapHead {
                    commit_seq: entry.commit_seq,
                    shard_idx,
                    pos: 0,
                });
            }
        }

        let mut out = Vec::with_capacity(total);
        while let Some(head) = heap.pop() {
            let HeapHead { shard_idx, pos, .. } = head;
            // Clone the chosen entry out of the snapshot run; the snapshot
            // is owned by this function so this clone is paid once per
            // output element.
            let run = &shard_runs[shard_idx];
            out.push(run[pos].clone());
            let next_pos = pos + 1;
            if next_pos < run.len() {
                heap.push(HeapHead {
                    commit_seq: run[next_pos].commit_seq,
                    shard_idx,
                    pos: next_pos,
                });
            }
        }
        out
    }

    /// Total entries currently held across every shard.
    #[must_use]
    pub fn total_len(&self) -> usize {
        self.shards.iter().map(MicaShard::len).sum()
    }

    /// Whether the log is empty across every shard.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.shards.iter().all(MicaShard::is_empty)
    }

    fn shard_for_current_thread(&self) -> usize {
        let tid = thread::current().id();
        let mut hasher = Xxh3::new();
        tid.hash(&mut hasher);
        let h = hasher.finish();
        // Additional SplitMix64 mix guards against `ThreadId`'s `Hash`
        // impl producing low-entropy output on platforms where IDs are
        // small consecutive integers.
        let mixed = splitmix64(h);
        (mixed as usize) % self.num_shards
    }
}

/// Heap node for the `merge_ordered` K-way merge. Ordering is reversed
/// (min-heap on `commit_seq`, tie-broken on `shard_idx`) so the standard
/// max-heap `BinaryHeap` yields the smallest element first.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
struct HeapHead {
    commit_seq: u64,
    shard_idx: usize,
    pos: usize,
}

impl Ord for HeapHead {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse so BinaryHeap (a max-heap) behaves as a min-heap.
        other
            .commit_seq
            .cmp(&self.commit_seq)
            .then_with(|| other.shard_idx.cmp(&self.shard_idx))
    }
}

impl PartialOrd for HeapHead {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// SplitMix64 finalizer — cheap and high-quality avalanche on u64 input.
#[inline]
fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

fn now_ns() -> u64 {
    use fsqlite_types::sync_primitives::SystemTime;
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0, |dur| {
            let secs = dur.as_secs();
            let nanos = u64::from(dur.subsec_nanos());
            secs.saturating_mul(1_000_000_000).saturating_add(nanos)
        })
}

#[cfg(test)]
mod tests {
    use super::{CommitEntry, MicaCommitLog};
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn single_thread_100_entries_merge_ordered() {
        let log = MicaCommitLog::new(4);
        for seq in 0..100 {
            log.append(seq, vec![seq as u32, (seq + 1) as u32]);
        }

        let merged = log.merge_ordered();
        assert_eq!(merged.len(), 100);
        for (i, entry) in merged.iter().enumerate() {
            assert_eq!(entry.commit_seq, i as u64);
            assert_eq!(entry.page_numbers, vec![i as u32, (i as u32) + 1]);
        }

        // Sanity: every entry strictly increasing.
        for pair in merged.windows(2) {
            assert!(pair[0].commit_seq < pair[1].commit_seq);
        }
    }

    #[test]
    fn multithreaded_4x100_entries_merge_ordered() {
        let log = Arc::new(MicaCommitLog::new(4));
        let threads: Vec<_> = (0..4)
            .map(|t| {
                let log = Arc::clone(&log);
                thread::spawn(move || {
                    // Each thread owns a disjoint commit_seq range so the
                    // merged order is well-defined.
                    let base = (t as u64) * 100;
                    for i in 0..100 {
                        log.append(base + i, vec![(base + i) as u32]);
                    }
                })
            })
            .collect();
        for h in threads {
            h.join().expect("worker thread panicked");
        }

        let merged: Vec<CommitEntry> = log.merge_ordered();
        assert_eq!(merged.len(), 400);
        for (i, entry) in merged.iter().enumerate() {
            assert_eq!(entry.commit_seq, i as u64, "out-of-order at index {i}");
        }
        // Monotonic non-decreasing (strict here because seqs are unique).
        for pair in merged.windows(2) {
            assert!(
                pair[0].commit_seq < pair[1].commit_seq,
                "merge produced non-monotone output"
            );
        }
    }

    #[test]
    fn empty_log_merge_ordered_is_empty() {
        let log = MicaCommitLog::new(8);
        assert_eq!(log.shard_count(), 8);
        assert!(log.is_empty());
        let merged = log.merge_ordered();
        assert!(merged.is_empty());
    }

    #[test]
    fn zero_shards_is_clamped_to_one() {
        // Defensive: hosts that report 0 usable CPUs must still yield a
        // working log rather than panic on a modulo-by-zero.
        let log = MicaCommitLog::new(0);
        assert_eq!(log.shard_count(), 1);
        log.append(42, vec![7]);
        let merged = log.merge_ordered();
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].commit_seq, 42);
        assert_eq!(merged[0].page_numbers, vec![7]);
    }

    #[test]
    fn ties_on_commit_seq_are_broken_deterministically() {
        // Two appends with the same commit_seq from the same thread (so
        // same shard) should preserve append order; across shards the
        // output must be stable on shard_idx.
        let log = MicaCommitLog::new(2);
        log.append(5, vec![1]);
        log.append(5, vec![2]);
        log.append(5, vec![3]);

        let merged = log.merge_ordered();
        assert_eq!(merged.len(), 3);
        for entry in &merged {
            assert_eq!(entry.commit_seq, 5);
        }

        // Running twice should yield the identical ordering.
        let merged2 = log.merge_ordered();
        assert_eq!(
            merged
                .iter()
                .map(|e| e.page_numbers.clone())
                .collect::<Vec<_>>(),
            merged2
                .iter()
                .map(|e| e.page_numbers.clone())
                .collect::<Vec<_>>(),
        );
    }
}
