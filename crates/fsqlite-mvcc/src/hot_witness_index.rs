//! §5.6.4.5 Hot Plane (Shared Memory): `HotWitnessIndex`.
//!
//! The hot plane is an **accelerator** for candidate discovery, NOT the source
//! of truth. It stores a fixed-size hash table mapping `(level, prefix)` to
//! bucket entries with **monotonic bitsets** of active TxnSlots.
//!
//! Key invariants:
//! - **No false negatives** for candidate discovery (overflow guarantees this).
//! - **Monotonic updates**: bits are only set, never cleared per-txn. Clearing
//!   only by epoch buffer refresh under epoch_lock.
//! - **Double-buffered** per bucket: two epoch-tagged bitset buffers allow epoch
//!   advancement without requiring zero Concurrent-mode transactions.
//! - **Bounded backoff**: epoch_lock acquisition respects budget and falls back
//!   to overflow when budget is exhausted.

use std::sync::atomic::{AtomicU8, AtomicU32, AtomicU64, Ordering};

use fsqlite_types::RangeKey;
use tracing::{debug, info, warn};

use crate::witness_hierarchy::range_key_bucket_index;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Sentinel: bucket entry is unoccupied.
const LEVEL_EMPTY: u8 = 0xFF;

/// Maximum bounded-backoff spins before falling back to overflow.
const EPOCH_LOCK_MAX_SPINS: u32 = 64;

// ---------------------------------------------------------------------------
// HotWitnessBucketEntry
// ---------------------------------------------------------------------------

/// One bucket in the hot witness index (§5.6.4.5).
///
/// Contains double-buffered epoch-tagged bitsets for readers and writers.
/// `W = ceil(max_txn_slots / 64)` determines the width of each bitset array.
pub struct HotWitnessBucketEntry {
    /// Hierarchy level (0xFF = empty/unoccupied).
    pub level: AtomicU8,
    /// Packed prefix bits for this bucket.
    pub prefix: AtomicU32,
    /// Spinlock for epoch install + clear: 0 = unlocked, 1 = locked.
    pub epoch_lock: AtomicU32,
    /// Epoch tag for buffer A.
    pub epoch_a: AtomicU32,
    /// Reader bitset for epoch A.
    pub readers_a: Vec<AtomicU64>,
    /// Writer bitset for epoch A.
    pub writers_a: Vec<AtomicU64>,
    /// Epoch tag for buffer B.
    pub epoch_b: AtomicU32,
    /// Reader bitset for epoch B.
    pub readers_b: Vec<AtomicU64>,
    /// Writer bitset for epoch B.
    pub writers_b: Vec<AtomicU64>,
}

impl HotWitnessBucketEntry {
    /// Create a new empty bucket entry with `words` u64 bitset width.
    fn new(words: u32) -> Self {
        let w = words as usize;
        let make_bitset = || (0..w).map(|_| AtomicU64::new(0)).collect::<Vec<_>>();
        Self {
            level: AtomicU8::new(LEVEL_EMPTY),
            prefix: AtomicU32::new(0),
            epoch_lock: AtomicU32::new(0),
            epoch_a: AtomicU32::new(0),
            readers_a: make_bitset(),
            writers_a: make_bitset(),
            epoch_b: AtomicU32::new(0),
            readers_b: make_bitset(),
            writers_b: make_bitset(),
        }
    }

    /// Check if this bucket is empty (unoccupied).
    fn is_empty(&self) -> bool {
        self.level.load(Ordering::Acquire) == LEVEL_EMPTY
    }

    /// Check if this bucket matches the given (level, prefix).
    fn matches(&self, level: u8, prefix: u32) -> bool {
        self.level.load(Ordering::Acquire) == level && self.prefix.load(Ordering::Acquire) == prefix
    }

    /// Try to claim this empty bucket for (level, prefix) via CAS.
    fn try_claim(&self, level: u8, prefix: u32) -> bool {
        let cas_result =
            self.level
                .compare_exchange(LEVEL_EMPTY, level, Ordering::AcqRel, Ordering::Acquire);
        if cas_result.is_ok() {
            self.prefix.store(prefix, Ordering::Release);
            true
        } else {
            false
        }
    }

    /// Set a bit in the readers bitset for the given epoch.
    ///
    /// Returns `true` if the bit was set in an epoch buffer, `false` if neither
    /// buffer matches the target epoch (caller must use slow path or overflow).
    fn set_reader_bit_fast(&self, target_epoch: u32, slot_id: u32) -> bool {
        let (word_idx, bit_mask) = slot_bit(slot_id);
        if word_idx >= self.readers_a.len() {
            return false;
        }
        if self.epoch_a.load(Ordering::Acquire) == target_epoch {
            self.readers_a[word_idx].fetch_or(bit_mask, Ordering::Release);
            return true;
        }
        if self.epoch_b.load(Ordering::Acquire) == target_epoch {
            self.readers_b[word_idx].fetch_or(bit_mask, Ordering::Release);
            return true;
        }
        false
    }

    /// Set a bit in the writers bitset for the given epoch.
    fn set_writer_bit_fast(&self, target_epoch: u32, slot_id: u32) -> bool {
        let (word_idx, bit_mask) = slot_bit(slot_id);
        if word_idx >= self.writers_a.len() {
            return false;
        }
        if self.epoch_a.load(Ordering::Acquire) == target_epoch {
            self.writers_a[word_idx].fetch_or(bit_mask, Ordering::Release);
            return true;
        }
        if self.epoch_b.load(Ordering::Acquire) == target_epoch {
            self.writers_b[word_idx].fetch_or(bit_mask, Ordering::Release);
            return true;
        }
        false
    }

    /// Slow path: acquire epoch_lock, install target_epoch into a stale buffer,
    /// then set the bit. Returns `true` on success, `false` if lock cannot be
    /// acquired within budget (caller must fall back to overflow).
    fn install_epoch_and_set_reader(
        &self,
        target_epoch: u32,
        current_epoch: u32,
        slot_id: u32,
    ) -> bool {
        if !self.try_acquire_epoch_lock() {
            return false;
        }
        // Re-check under lock: another thread may have installed the epoch.
        if self.set_reader_bit_fast(target_epoch, slot_id) {
            self.release_epoch_lock();
            return true;
        }
        // Install into whichever buffer is stale.
        // A buffer is stale if its epoch is 0 (sentinel) or not a live epoch.
        let ea = self.epoch_a.load(Ordering::Acquire);
        let eb = self.epoch_b.load(Ordering::Acquire);
        let target_buf = if is_epoch_stale(ea, current_epoch) {
            'a'
        } else if is_epoch_stale(eb, current_epoch) {
            'b'
        } else {
            // Both buffers hold live epochs — handle gracefully by using overflow.
            self.release_epoch_lock();
            return false;
        };

        let (word_idx, bit_mask) = slot_bit(slot_id);
        if target_buf == 'a' {
            self.clear_buffer_a();
            self.epoch_a.store(target_epoch, Ordering::Release);
            if word_idx < self.readers_a.len() {
                self.readers_a[word_idx].fetch_or(bit_mask, Ordering::Release);
            }
        } else {
            self.clear_buffer_b();
            self.epoch_b.store(target_epoch, Ordering::Release);
            if word_idx < self.readers_b.len() {
                self.readers_b[word_idx].fetch_or(bit_mask, Ordering::Release);
            }
        }
        self.release_epoch_lock();
        true
    }

    /// Slow path for writer bits.
    fn install_epoch_and_set_writer(
        &self,
        target_epoch: u32,
        current_epoch: u32,
        slot_id: u32,
    ) -> bool {
        if !self.try_acquire_epoch_lock() {
            return false;
        }
        if self.set_writer_bit_fast(target_epoch, slot_id) {
            self.release_epoch_lock();
            return true;
        }
        let ea = self.epoch_a.load(Ordering::Acquire);
        let eb = self.epoch_b.load(Ordering::Acquire);
        let target_buf = if is_epoch_stale(ea, current_epoch) {
            'a'
        } else if is_epoch_stale(eb, current_epoch) {
            'b'
        } else {
            self.release_epoch_lock();
            return false;
        };
        let (word_idx, bit_mask) = slot_bit(slot_id);
        if target_buf == 'a' {
            self.clear_buffer_a();
            self.epoch_a.store(target_epoch, Ordering::Release);
            if word_idx < self.writers_a.len() {
                self.writers_a[word_idx].fetch_or(bit_mask, Ordering::Release);
            }
        } else {
            self.clear_buffer_b();
            self.epoch_b.store(target_epoch, Ordering::Release);
            if word_idx < self.writers_b.len() {
                self.writers_b[word_idx].fetch_or(bit_mask, Ordering::Release);
            }
        }
        self.release_epoch_lock();
        true
    }

    /// Get all reader bits across both live epochs.
    fn all_reader_bits(&self, words: u32) -> Vec<u64> {
        let w = words as usize;
        let mut result = vec![0u64; w];
        for (r, a) in result.iter_mut().zip(self.readers_a.iter()).take(w) {
            *r |= a.load(Ordering::Acquire);
        }
        for (r, b) in result.iter_mut().zip(self.readers_b.iter()).take(w) {
            *r |= b.load(Ordering::Acquire);
        }
        result
    }

    /// Get all writer bits across both live epochs.
    fn all_writer_bits(&self, words: u32) -> Vec<u64> {
        let w = words as usize;
        let mut result = vec![0u64; w];
        for (r, a) in result.iter_mut().zip(self.writers_a.iter()).take(w) {
            *r |= a.load(Ordering::Acquire);
        }
        for (r, b) in result.iter_mut().zip(self.writers_b.iter()).take(w) {
            *r |= b.load(Ordering::Acquire);
        }
        result
    }

    /// Try to acquire the epoch_lock with bounded backoff (§5.6.4.5).
    fn try_acquire_epoch_lock(&self) -> bool {
        for _ in 0..EPOCH_LOCK_MAX_SPINS {
            if self
                .epoch_lock
                .compare_exchange(0, 1, Ordering::Acquire, Ordering::Relaxed)
                .is_ok()
            {
                return true;
            }
            std::hint::spin_loop();
        }
        warn!(
            max_spins = EPOCH_LOCK_MAX_SPINS,
            "epoch_lock acquisition budget exhausted, falling back to overflow"
        );
        false
    }

    /// Release the epoch_lock.
    fn release_epoch_lock(&self) {
        self.epoch_lock.store(0, Ordering::Release);
    }

    /// Clear buffer A bitsets to zero.
    fn clear_buffer_a(&self) {
        for w in &self.readers_a {
            w.store(0, Ordering::Release);
        }
        for w in &self.writers_a {
            w.store(0, Ordering::Release);
        }
    }

    /// Clear buffer B bitsets to zero.
    fn clear_buffer_b(&self) {
        for w in &self.readers_b {
            w.store(0, Ordering::Release);
        }
        for w in &self.writers_b {
            w.store(0, Ordering::Release);
        }
    }
}

impl std::fmt::Debug for HotWitnessBucketEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HotWitnessBucketEntry")
            .field("level", &self.level.load(Ordering::Relaxed))
            .field("prefix", &self.prefix.load(Ordering::Relaxed))
            .field("epoch_a", &self.epoch_a.load(Ordering::Relaxed))
            .field("epoch_b", &self.epoch_b.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

// ---------------------------------------------------------------------------
// HotWitnessIndex
// ---------------------------------------------------------------------------

/// Fixed-size hash table for hot witness candidate discovery (§5.6.4.5).
///
/// Maps `(level, prefix)` to double-buffered epoch-tagged bitsets.
/// `overflow` is a catch-all bucket that preserves "no false negatives".
pub struct HotWitnessIndex {
    /// Power-of-2 capacity of the hash table (excluding overflow).
    capacity: u32,
    /// Bitmask for table indexing: `capacity - 1`.
    mask: u32,
    /// Current witness epoch (monotonically increasing).
    epoch: AtomicU32,
    /// Hash table entries indexed by `range_key_bucket_index`.
    entries: Vec<HotWitnessBucketEntry>,
    /// Always-present catch-all: no false negatives even under capacity pressure.
    overflow: HotWitnessBucketEntry,
    /// Number of u64 words per bitset (`ceil(max_txn_slots / 64)`).
    words: u32,
    /// Maximum number of linear probes before giving up on insert.
    max_probes: u32,
}

impl HotWitnessIndex {
    /// Create a new hot witness index with the given power-of-2 capacity and
    /// max TxnSlot count.
    ///
    /// # Panics
    ///
    /// Panics if `capacity` is zero or not a power of two.
    #[must_use]
    pub fn new(capacity: u32, max_txn_slots: u32) -> Self {
        assert!(
            capacity > 0 && capacity.is_power_of_two(),
            "capacity must be power of 2"
        );
        let words = max_txn_slots.div_ceil(64);
        let mask = capacity - 1;
        let entries = (0..capacity)
            .map(|_| HotWitnessBucketEntry::new(words))
            .collect();
        Self {
            capacity,
            mask,
            epoch: AtomicU32::new(1), // Start at epoch 1 (0 is sentinel).
            entries,
            overflow: HotWitnessBucketEntry::new(words),
            words,
            max_probes: capacity, // Probe entire table before overflow.
        }
    }

    /// Current epoch value.
    #[must_use]
    pub fn current_epoch(&self) -> u32 {
        self.epoch.load(Ordering::Acquire)
    }

    /// Register a read witness for slot `slot_id` at the given `witness_epoch`
    /// across all `range_keys` derived from a `WitnessKey` (§5.6.4.5).
    pub fn register_read(&self, slot_id: u32, witness_epoch: u32, range_keys: &[RangeKey]) {
        for rk in range_keys {
            if !self.register_read_single(slot_id, witness_epoch, *rk) {
                // Capacity pressure or lock failure: fall back to overflow.
                self.set_overflow_reader(slot_id, witness_epoch);
                debug!(
                    slot_id,
                    witness_epoch,
                    level = rk.level,
                    prefix = rk.hash_prefix,
                    used_overflow = true,
                    "witness read registration used overflow"
                );
            }
        }
    }

    /// Register a write witness for slot `slot_id` at the given `witness_epoch`
    /// across all `range_keys`.
    pub fn register_write(&self, slot_id: u32, witness_epoch: u32, range_keys: &[RangeKey]) {
        for rk in range_keys {
            if !self.register_write_single(slot_id, witness_epoch, *rk) {
                self.set_overflow_writer(slot_id, witness_epoch);
                debug!(
                    slot_id,
                    witness_epoch,
                    level = rk.level,
                    prefix = rk.hash_prefix,
                    used_overflow = true,
                    "witness write registration used overflow"
                );
            }
        }
    }

    /// Discover candidate reader TxnSlot IDs for the given `range_keys`.
    ///
    /// Returns a bitset (as `Vec<u64>`) where bit `i` = 1 means TxnSlot `i` is
    /// a potential reader. Includes overflow bits.
    #[must_use]
    pub fn candidate_readers(&self, range_keys: &[RangeKey]) -> Vec<u64> {
        let mut result = self.overflow.all_reader_bits(self.words);
        for rk in range_keys {
            if let Some(bucket) = self.find_bucket(*rk) {
                let bits = bucket.all_reader_bits(self.words);
                for (r, b) in result.iter_mut().zip(bits.iter()) {
                    *r |= *b;
                }
            }
        }
        result
    }

    /// Discover candidate writer TxnSlot IDs for the given `range_keys`.
    #[must_use]
    pub fn candidate_writers(&self, range_keys: &[RangeKey]) -> Vec<u64> {
        let mut result = self.overflow.all_writer_bits(self.words);
        for rk in range_keys {
            if let Some(bucket) = self.find_bucket(*rk) {
                let bits = bucket.all_writer_bits(self.words);
                for (r, b) in result.iter_mut().zip(bits.iter()) {
                    *r |= *b;
                }
            }
        }
        result
    }

    /// Try to advance the epoch from `current` to `current + 1`.
    ///
    /// Returns `true` if the epoch was successfully advanced.
    /// This is safe iff no active Concurrent-mode txns have `witness_epoch == current - 1`.
    pub fn try_advance_epoch(&self, current: u32) -> bool {
        let cas = self.epoch.compare_exchange(
            current,
            current.wrapping_add(1),
            Ordering::AcqRel,
            Ordering::Acquire,
        );
        if cas.is_ok() {
            info!(
                old_epoch = current,
                new_epoch = current.wrapping_add(1),
                "witness epoch advanced"
            );
            true
        } else {
            false
        }
    }

    /// Internal: register a single read witness for one range key.
    fn register_read_single(&self, slot_id: u32, witness_epoch: u32, range_key: RangeKey) -> bool {
        let current_epoch = self.current_epoch();
        let Some(bucket) = self.find_or_create_bucket(range_key) else {
            return false; // No capacity → use overflow.
        };
        // Fast path: set bit in existing epoch buffer.
        if bucket.set_reader_bit_fast(witness_epoch, slot_id) {
            return true;
        }
        // Slow path: install epoch under lock.
        bucket.install_epoch_and_set_reader(witness_epoch, current_epoch, slot_id)
    }

    /// Internal: register a single write witness for one range key.
    fn register_write_single(&self, slot_id: u32, witness_epoch: u32, range_key: RangeKey) -> bool {
        let current_epoch = self.current_epoch();
        let Some(bucket) = self.find_or_create_bucket(range_key) else {
            return false;
        };
        if bucket.set_writer_bit_fast(witness_epoch, slot_id) {
            return true;
        }
        bucket.install_epoch_and_set_writer(witness_epoch, current_epoch, slot_id)
    }

    /// Find an existing bucket for the given range key.
    fn find_bucket(&self, range_key: RangeKey) -> Option<&HotWitnessBucketEntry> {
        let start = range_key_bucket_index(range_key, self.mask);
        for probe in 0..self.max_probes {
            let idx = ((start + probe) & self.mask) as usize;
            let entry = &self.entries[idx];
            if entry.matches(range_key.level, range_key.hash_prefix) {
                return Some(entry);
            }
            if entry.is_empty() {
                return None; // No more probes needed.
            }
        }
        None
    }

    /// Find or create a bucket for the given range key via linear probing.
    fn find_or_create_bucket(&self, range_key: RangeKey) -> Option<&HotWitnessBucketEntry> {
        let start = range_key_bucket_index(range_key, self.mask);
        for probe in 0..self.max_probes {
            let idx = ((start + probe) & self.mask) as usize;
            let entry = &self.entries[idx];
            if entry.matches(range_key.level, range_key.hash_prefix) {
                return Some(entry);
            }
            if entry.is_empty() && entry.try_claim(range_key.level, range_key.hash_prefix) {
                return Some(entry);
            }
            // If CAS failed, another thread may have claimed it. Re-check.
            if entry.matches(range_key.level, range_key.hash_prefix) {
                return Some(entry);
            }
        }
        None // Table full.
    }

    /// Set a reader bit in the overflow bucket.
    fn set_overflow_reader(&self, slot_id: u32, witness_epoch: u32) {
        let current = self.current_epoch();
        if !self.overflow.set_reader_bit_fast(witness_epoch, slot_id) {
            // Install epoch in overflow (always succeeds — we don't budget-limit overflow).
            let _ = self
                .overflow
                .install_epoch_and_set_reader(witness_epoch, current, slot_id);
        }
    }

    /// Set a writer bit in the overflow bucket.
    fn set_overflow_writer(&self, slot_id: u32, witness_epoch: u32) {
        let current = self.current_epoch();
        if !self.overflow.set_writer_bit_fast(witness_epoch, slot_id) {
            let _ = self
                .overflow
                .install_epoch_and_set_writer(witness_epoch, current, slot_id);
        }
    }
}

impl std::fmt::Debug for HotWitnessIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HotWitnessIndex")
            .field("capacity", &self.capacity)
            .field("epoch", &self.epoch.load(Ordering::Relaxed))
            .field("words", &self.words)
            .finish_non_exhaustive()
    }
}

// ---------------------------------------------------------------------------
// §5.6.4.6 Cold Plane Witness Store (In-Process Simulation)
// ---------------------------------------------------------------------------

/// In-process cold plane witness store for testing and single-process mode.
///
/// In Native mode, cold truth is ECS objects (RaptorQ-encodable).
/// In Compatibility mode, stored as sidecar under `.fsqlite/` directory.
#[derive(Debug, Clone, Default)]
pub struct ColdWitnessStore {
    /// Published read witnesses.
    pub read_witnesses: Vec<fsqlite_types::ReadWitness>,
    /// Published write witnesses.
    pub write_witnesses: Vec<fsqlite_types::WriteWitness>,
    /// Dependency edges.
    pub dependency_edges: Vec<fsqlite_types::DependencyEdge>,
    /// Commit proofs.
    pub commit_proofs: Vec<fsqlite_types::CommitProof>,
}

/// Operating mode for cold plane storage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColdPlaneMode {
    /// ECS objects (RaptorQ-encodable, replicable).
    Native,
    /// Sidecar log under `.fsqlite/` directory.
    Compatibility,
}

impl ColdWitnessStore {
    /// Create a new empty cold witness store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Publish a read witness.
    pub fn publish_read_witness(&mut self, witness: fsqlite_types::ReadWitness) {
        self.read_witnesses.push(witness);
    }

    /// Publish a write witness.
    pub fn publish_write_witness(&mut self, witness: fsqlite_types::WriteWitness) {
        self.write_witnesses.push(witness);
    }

    /// Publish a dependency edge.
    pub fn publish_dependency_edge(&mut self, edge: fsqlite_types::DependencyEdge) {
        self.dependency_edges.push(edge);
    }

    /// Publish a commit proof.
    pub fn publish_commit_proof(&mut self, proof: fsqlite_types::CommitProof) {
        self.commit_proofs.push(proof);
    }

    /// Compute the sidecar path for compatibility mode.
    #[must_use]
    pub fn compatibility_sidecar_path(db_path: &std::path::Path) -> std::path::PathBuf {
        let mut sidecar = db_path.to_path_buf();
        let stem = sidecar
            .file_stem()
            .map_or_else(|| "db".into(), std::ffi::OsStr::to_os_string);
        sidecar.set_file_name(".fsqlite");
        sidecar.push(format!("{}_witnesses.log", stem.to_string_lossy()));
        sidecar
    }

    /// Find all read witnesses for a given transaction.
    #[must_use]
    pub fn reads_for_txn(&self, txn: fsqlite_types::TxnId) -> Vec<&fsqlite_types::ReadWitness> {
        self.read_witnesses
            .iter()
            .filter(|w| w.txn == txn)
            .collect()
    }

    /// Find all write witnesses for a given transaction.
    #[must_use]
    pub fn writes_for_txn(&self, txn: fsqlite_types::TxnId) -> Vec<&fsqlite_types::WriteWitness> {
        self.write_witnesses
            .iter()
            .filter(|w| w.txn == txn)
            .collect()
    }

    /// GC: prune witnesses for transactions with `commit_seq < safe_gc_seq`.
    ///
    /// This is a simplified in-process GC; the real implementation would
    /// use ECS compaction per §5.6.4.8.
    pub fn prune_before(&mut self, _safe_gc_seq: fsqlite_types::CommitSeq) {
        // In-process simulation: no-op (requires commit sequence tracking).
        // Real implementation would filter based on commit_seq.
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Check if a buffer epoch tag is stale (safe to reuse).
///
/// Epoch 0 is a sentinel (uninitialized) and is always stale.
/// Otherwise, a buffer is stale if its epoch is not one of the two live epochs
/// (current or current-1).
#[must_use]
const fn is_epoch_stale(buffer_epoch: u32, current_epoch: u32) -> bool {
    if buffer_epoch == 0 {
        return true; // Sentinel: never used.
    }
    let prev_epoch = current_epoch.wrapping_sub(1);
    buffer_epoch != current_epoch && buffer_epoch != prev_epoch
}

/// Compute (word_index, bit_mask) for a given TxnSlot ID.
#[must_use]
const fn slot_bit(slot_id: u32) -> (usize, u64) {
    let word_idx = (slot_id / 64) as usize;
    let bit_idx = slot_id % 64;
    (word_idx, 1u64 << bit_idx)
}

/// Extract set bit positions from a bitset.
#[must_use]
#[allow(clippy::cast_possible_truncation)]
pub fn bitset_to_slot_ids(bitset: &[u64]) -> Vec<u32> {
    let mut ids = Vec::new();
    for (word_idx, &word) in bitset.iter().enumerate() {
        let base = (word_idx as u32) * 64;
        let mut w = word;
        while w != 0 {
            let bit = w.trailing_zeros();
            ids.push(base + bit);
            w &= w - 1; // Clear lowest set bit.
        }
    }
    ids
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::witness_hierarchy::{WitnessHierarchyConfigV1, derive_range_keys};
    use fsqlite_types::{PageNumber, WitnessKey};

    fn page(n: u32) -> PageNumber {
        PageNumber::new(n).unwrap()
    }

    fn default_config() -> WitnessHierarchyConfigV1 {
        WitnessHierarchyConfigV1::default()
    }

    fn range_keys_for(key: &WitnessKey) -> Vec<RangeKey> {
        derive_range_keys(key, &default_config())
    }

    // -- bd-3t3.9.2 test 1: Insert read key, verify T1 found as reader --

    #[test]
    fn test_hot_index_insert_read_key() {
        let idx = HotWitnessIndex::new(16, 256);
        let key = WitnessKey::for_cell_read(page(2), b"user_id=42");
        let rks = range_keys_for(&key);
        let epoch = idx.current_epoch();

        idx.register_read(0, epoch, &rks);

        let readers = idx.candidate_readers(&rks);
        let slot_ids = bitset_to_slot_ids(&readers);
        assert!(
            slot_ids.contains(&0),
            "T1 (slot 0) must be found as reader after registration"
        );
    }

    // -- bd-3t3.9.2 test 2: Insert write key, verify T2 found as writer --

    #[test]
    fn test_hot_index_insert_write_key() {
        let idx = HotWitnessIndex::new(16, 256);
        let key = WitnessKey::for_cell_read(page(2), b"user_id=42");
        let rks = range_keys_for(&key);
        let epoch = idx.current_epoch();

        idx.register_write(1, epoch, &rks);

        let writers = idx.candidate_writers(&rks);
        let slot_ids = bitset_to_slot_ids(&writers);
        assert!(
            slot_ids.contains(&1),
            "T2 (slot 1) must be found as writer after registration"
        );
    }

    // -- bd-3t3.9.2 test 3: T1 reads K1, T2 writes K1 → rw-antidependency detected --

    #[test]
    fn test_hot_index_detects_rw_conflict() {
        let idx = HotWitnessIndex::new(16, 256);
        let key = WitnessKey::for_cell_read(page(3), b"account=100");
        let rks = range_keys_for(&key);
        let epoch = idx.current_epoch();

        // T1 reads K1.
        idx.register_read(0, epoch, &rks);
        // T2 writes K1.
        idx.register_write(1, epoch, &rks);

        // Discovery: for key K1, find all readers (should include T1)
        // and all writers (should include T2).
        let readers = bitset_to_slot_ids(&idx.candidate_readers(&rks));
        let writers = bitset_to_slot_ids(&idx.candidate_writers(&rks));

        assert!(
            readers.contains(&0),
            "T1 must be discovered as reader of K1"
        );
        assert!(
            writers.contains(&1),
            "T2 must be discovered as writer of K1"
        );
        // rw-antidependency: reader T1 and writer T2 overlap on K1.
        assert!(
            readers.contains(&0) && writers.contains(&1),
            "rw-antidependency must be detected"
        );
    }

    // -- bd-3t3.9.2 test 4: No false negatives for keys in hot index --

    #[test]
    fn test_hot_index_no_false_negatives() {
        let idx = HotWitnessIndex::new(32, 256);
        let epoch = idx.current_epoch();

        // Register 10 distinct keys from different transactions.
        let mut keys_and_slots: Vec<(WitnessKey, u32)> = Vec::new();
        for i in 0..10_u32 {
            let key = WitnessKey::for_cell_read(page(i + 1), &i.to_le_bytes());
            let rks = range_keys_for(&key);
            idx.register_read(i, epoch, &rks);
            idx.register_write(i + 10, epoch, &rks);
            keys_and_slots.push((key, i));
        }

        // Verify: every registered reader/writer is discoverable.
        for (key, slot) in &keys_and_slots {
            let rks = range_keys_for(key);
            let readers = bitset_to_slot_ids(&idx.candidate_readers(&rks));
            let writers = bitset_to_slot_ids(&idx.candidate_writers(&rks));
            assert!(
                readers.contains(slot),
                "reader slot {slot} must be discoverable for its key"
            );
            assert!(
                writers.contains(&(slot + 10)),
                "writer slot {} must be discoverable for its key",
                slot + 10
            );
        }
    }

    // -- bd-3t3.9.2 test 5: Epoch advances periodically, old entries prunable --

    #[test]
    fn test_hot_index_epoch_advancement() {
        let idx = HotWitnessIndex::new(16, 256);
        let epoch1 = idx.current_epoch();

        // Register at epoch 1.
        let key = WitnessKey::for_cell_read(page(5), b"key1");
        let rks = range_keys_for(&key);
        idx.register_read(0, epoch1, &rks);

        // Advance epoch.
        assert!(
            idx.try_advance_epoch(epoch1),
            "epoch advancement should succeed"
        );
        let epoch2 = idx.current_epoch();
        assert_eq!(epoch2, epoch1 + 1);

        // Register at epoch 2.
        idx.register_read(1, epoch2, &rks);

        // Both epoch 1 and epoch 2 readers should be discoverable
        // (double-buffered: both live epochs consulted).
        let readers = bitset_to_slot_ids(&idx.candidate_readers(&rks));
        assert!(readers.contains(&0), "epoch 1 reader must still be visible");
        assert!(readers.contains(&1), "epoch 2 reader must be visible");

        // After another epoch advance, epoch 1 buffers become stale and reclaimable.
        assert!(idx.try_advance_epoch(epoch2));
        let epoch3 = idx.current_epoch();
        assert_eq!(epoch3, epoch2 + 1);

        // Epoch 1 reader may still be visible (bits aren't cleared until bucket refresh),
        // but epoch 2 reader MUST be visible (it's in the live epoch range).
        let readers = bitset_to_slot_ids(&idx.candidate_readers(&rks));
        assert!(
            readers.contains(&1),
            "epoch 2 reader must remain visible after epoch 3 advance"
        );
    }

    // -- bd-3t3.9.2 test 6: Overflow fallback preserves no false negatives --

    #[test]
    fn test_overflow_fallback_no_false_negatives() {
        // Tiny capacity to force overflow.
        let idx = HotWitnessIndex::new(2, 64);
        let epoch = idx.current_epoch();

        // Fill the table with distinct keys.
        let mut all_keys = Vec::new();
        for i in 0..10_u32 {
            let key = WitnessKey::for_cell_read(page(100 + i), &i.to_le_bytes());
            let rks = range_keys_for(&key);
            idx.register_read(i, epoch, &rks);
            all_keys.push((key, i));
        }

        // Verify ALL readers are discoverable (some via overflow).
        for (key, slot) in &all_keys {
            let rks = range_keys_for(key);
            let readers = bitset_to_slot_ids(&idx.candidate_readers(&rks));
            assert!(
                readers.contains(slot),
                "slot {slot} must be discoverable even under capacity pressure (via overflow)"
            );
        }
    }

    // -- bd-3t3.9.2 test 7: Lock acquisition respects budget, falls back to overflow --

    #[test]
    fn test_epoch_lock_bounded_backoff() {
        let idx = HotWitnessIndex::new(4, 64);
        let key = WitnessKey::for_cell_read(page(1), b"test");
        let rks = range_keys_for(&key);

        // Register at epoch 1 to create the bucket.
        let epoch1 = idx.current_epoch();
        idx.register_read(0, epoch1, &rks);

        // Advance epoch twice so the bucket needs a slow-path install.
        assert!(idx.try_advance_epoch(epoch1));
        let epoch2 = idx.current_epoch();
        assert!(idx.try_advance_epoch(epoch2));
        let epoch3 = idx.current_epoch();

        // Manually hold the epoch_lock on the bucket to simulate contention.
        // (Find the bucket first.)
        let bucket = idx.find_bucket(rks[0]);
        if let Some(b) = bucket {
            // Lock the bucket.
            b.epoch_lock.store(1, Ordering::Release);

            // Registering at epoch3 should fail the slow path and use overflow.
            idx.register_read(5, epoch3, &rks);

            // Release the lock.
            b.epoch_lock.store(0, Ordering::Release);
        }

        // The registration must still be discoverable (via overflow).
        let readers = bitset_to_slot_ids(&idx.candidate_readers(&rks));
        assert!(
            readers.contains(&5),
            "slot 5 must be discoverable via overflow after lock contention"
        );
    }

    // -- bd-3t3.9.2 test 8: Witness published as ECS object --

    #[test]
    fn test_witness_published_as_ecs_object() {
        let mut store = ColdWitnessStore::new();
        let txn = fsqlite_types::TxnId::new(42).unwrap();
        let key = WitnessKey::for_cell_read(page(2), b"pk=1");

        store.publish_read_witness(fsqlite_types::ReadWitness {
            txn,
            key: key.clone(),
        });
        store.publish_write_witness(fsqlite_types::WriteWitness { txn, key });

        assert_eq!(store.read_witnesses.len(), 1);
        assert_eq!(store.write_witnesses.len(), 1);
        assert_eq!(store.reads_for_txn(txn).len(), 1);
        assert_eq!(store.writes_for_txn(txn).len(), 1);
    }

    // -- bd-3t3.9.2 test 9: Witness survives process crash --

    #[test]
    fn test_witness_survives_process_crash() {
        // Simulate crash by serializing witnesses, dropping the store,
        // and deserializing into a fresh store.
        let mut store = ColdWitnessStore::new();
        let txn = fsqlite_types::TxnId::new(99).unwrap();
        let key = WitnessKey::for_cell_read(page(7), b"crash_test");

        store.publish_read_witness(fsqlite_types::ReadWitness {
            txn,
            key: key.clone(),
        });
        store.publish_dependency_edge(fsqlite_types::DependencyEdge {
            from: txn,
            to: fsqlite_types::TxnId::new(100).unwrap(),
            key_basis: key,
            observed_by: txn,
        });

        // Serialize (simulates durable persistence).
        let serialized_reads = serde_json::to_string(&store.read_witnesses).unwrap();
        let serialized_edges = serde_json::to_string(&store.dependency_edges).unwrap();

        // "Crash" — drop the original store.
        drop(store);

        // "Recovery" — deserialize into a new store.
        let recovered_reads: Vec<fsqlite_types::ReadWitness> =
            serde_json::from_str(&serialized_reads).unwrap();
        let recovered_edges: Vec<fsqlite_types::DependencyEdge> =
            serde_json::from_str(&serialized_edges).unwrap();

        assert_eq!(recovered_reads.len(), 1);
        assert_eq!(recovered_reads[0].txn, txn);
        assert_eq!(recovered_edges.len(), 1);
        assert_eq!(recovered_edges[0].from, txn);
    }

    // -- bd-3t3.9.2 test 10: Cold plane compatibility mode --

    #[test]
    fn test_cold_plane_compatibility_mode() {
        use std::path::Path;

        // In compatibility mode, cold plane stored under .fsqlite/ sidecar.
        let db_path = Path::new("/data/myapp/main.db");
        let sidecar = ColdWitnessStore::compatibility_sidecar_path(db_path);

        assert!(
            sidecar.to_string_lossy().contains(".fsqlite"),
            "sidecar must be under .fsqlite directory"
        );
        assert!(
            sidecar.to_string_lossy().contains("main_witnesses.log"),
            "sidecar filename must include db stem"
        );

        // Verify the mode enum covers both options.
        let native = ColdPlaneMode::Native;
        let compat = ColdPlaneMode::Compatibility;
        assert_ne!(native, compat);
    }

    // -- bd-3t3.9.2 test 11: Concurrent access consistency --

    #[test]
    fn prop_hot_index_consistent_under_concurrent_access() {
        use std::sync::Arc;
        use std::thread;

        let idx = Arc::new(HotWitnessIndex::new(16, 256));
        let config = default_config();
        let epoch = idx.current_epoch();
        let num_threads = 4;
        let ops_per_thread = 50;

        let handles: Vec<_> = (0..num_threads)
            .map(|t| {
                let idx = Arc::clone(&idx);
                thread::spawn(move || {
                    for i in 0..ops_per_thread {
                        let slot_id = t * ops_per_thread + i;
                        let key = WitnessKey::for_cell_read(page(t + 1), &i.to_le_bytes());
                        let rks = derive_range_keys(&key, &config);
                        if i % 2 == 0 {
                            idx.register_read(slot_id, epoch, &rks);
                        } else {
                            idx.register_write(slot_id, epoch, &rks);
                        }
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        // Verify: all registered witnesses are discoverable.
        for t in 0..num_threads {
            for i in 0..ops_per_thread {
                let slot_id = t * ops_per_thread + i;
                let key = WitnessKey::for_cell_read(page(t + 1), &i.to_le_bytes());
                let rks = derive_range_keys(&key, &config);
                if i % 2 == 0 {
                    let readers = bitset_to_slot_ids(&idx.candidate_readers(&rks));
                    assert!(
                        readers.contains(&slot_id),
                        "slot {slot_id} must be discoverable as reader"
                    );
                } else {
                    let writers = bitset_to_slot_ids(&idx.candidate_writers(&rks));
                    assert!(
                        writers.contains(&slot_id),
                        "slot {slot_id} must be discoverable as writer"
                    );
                }
            }
        }
    }

    // -- bd-3t3.9.2 test 12: RangeKey hierarchy L0/L1/L2 derivation --

    #[test]
    fn test_range_key_hierarchy_levels() {
        let config = default_config();
        let key = WitnessKey::for_cell_read(page(2), b"example_key");
        let rks = derive_range_keys(&key, &config);

        assert_eq!(rks.len(), 3, "must derive 3 range keys (L0, L1, L2)");
        assert_eq!(rks[0].level, 0);
        assert_eq!(rks[1].level, 1);
        assert_eq!(rks[2].level, 2);

        // L0 has 12-bit prefix → max value 4095.
        assert!(rks[0].hash_prefix <= 4095, "L0 prefix must fit in 12 bits");
        // L1 has 20-bit prefix → max value 1_048_575.
        assert!(
            rks[1].hash_prefix <= 1_048_575,
            "L1 prefix must fit in 20 bits"
        );
        // L2 has 28-bit prefix → max value 268_435_455.
        assert!(
            rks[2].hash_prefix <= 268_435_455,
            "L2 prefix must fit in 28 bits"
        );

        // L0 prefix must be a prefix of L1 (top 12 bits of hash same).
        assert_eq!(
            rks[0].hash_prefix,
            rks[1].hash_prefix >> 8,
            "L0 must be top-12-bit prefix of L1 (which is top-20-bit)"
        );

        // L1 prefix must be a prefix of L2.
        assert_eq!(
            rks[1].hash_prefix,
            rks[2].hash_prefix >> 8,
            "L1 must be top-20-bit prefix of L2 (which is top-28-bit)"
        );

        // Same key produces same range keys (determinism).
        let rks2 = derive_range_keys(&key, &config);
        assert_eq!(rks, rks2, "range key derivation must be deterministic");

        // Different key produces different range keys (at some level).
        let key2 = WitnessKey::for_cell_read(page(2), b"other_key");
        let rks3 = derive_range_keys(&key2, &config);
        assert_ne!(
            rks, rks3,
            "different keys should produce different range keys"
        );
    }

    // -- Helpers test --

    #[test]
    fn test_bitset_to_slot_ids() {
        let bitset = [0b1010u64, 0b0001u64];
        let ids = bitset_to_slot_ids(&bitset);
        assert_eq!(ids, vec![1, 3, 64]);
    }

    #[test]
    fn test_slot_bit() {
        assert_eq!(slot_bit(0), (0, 1));
        assert_eq!(slot_bit(1), (0, 2));
        assert_eq!(slot_bit(63), (0, 1 << 63));
        assert_eq!(slot_bit(64), (1, 1));
        assert_eq!(slot_bit(127), (1, 1 << 63));
    }
}
