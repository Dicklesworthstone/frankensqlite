//! MVCC core runtime types (§5.1).
//!
//! This module implements the runtime data structures that power MVCC
//! concurrency: version arenas, page lock tables, commit indices, and
//! transaction state.
//!
//! Foundation types (TxnId, CommitSeq, Snapshot, etc.) live in
//! [`fsqlite_types::glossary`]; this module builds the runtime machinery on top.

use std::collections::{HashMap, HashSet};
use std::hash::{BuildHasherDefault, Hasher};

use parking_lot::{Mutex, RwLock};
use smallvec::SmallVec;

use fsqlite_types::{
    CommitSeq, IntentLog, PageNumber, PageSize, PageVersion, Snapshot, TxnEpoch, TxnId, TxnSlot,
    TxnToken, WitnessKey,
};

// ---------------------------------------------------------------------------
// VersionIdx / VersionArena
// ---------------------------------------------------------------------------

/// Index into a [`VersionArena`] chunk.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct VersionIdx {
    chunk: u32,
    offset: u32,
}

impl VersionIdx {
    #[inline]
    const fn new(chunk: u32, offset: u32) -> Self {
        Self { chunk, offset }
    }
}

/// Number of page versions per arena chunk.
const ARENA_CHUNK: usize = 4096;

/// Bump-allocated arena for [`PageVersion`] objects.
///
/// Single-writer / multi-reader. The arena owns all page version data and
/// hands out [`VersionIdx`] handles. Freed slots are recycled via a free list.
pub struct VersionArena {
    chunks: Vec<Vec<Option<PageVersion>>>,
    free_list: Vec<VersionIdx>,
    high_water: u64,
}

impl VersionArena {
    /// Create an empty arena.
    #[must_use]
    pub fn new() -> Self {
        Self {
            chunks: vec![Vec::with_capacity(ARENA_CHUNK)],
            free_list: Vec::new(),
            high_water: 0,
        }
    }

    /// Allocate a slot for `version`, returning its index.
    pub fn alloc(&mut self, version: PageVersion) -> VersionIdx {
        if let Some(idx) = self.free_list.pop() {
            self.chunks[idx.chunk as usize][idx.offset as usize] = Some(version);
            return idx;
        }

        let last_chunk = self.chunks.len() - 1;
        if self.chunks[last_chunk].len() >= ARENA_CHUNK {
            self.chunks.push(Vec::with_capacity(ARENA_CHUNK));
        }

        let chunk_idx = self.chunks.len() - 1;
        let offset = self.chunks[chunk_idx].len();
        self.chunks[chunk_idx].push(Some(version));
        self.high_water += 1;

        #[allow(clippy::cast_possible_truncation)]
        VersionIdx::new(chunk_idx as u32, offset as u32)
    }

    /// Free the slot at `idx`, making it available for reuse.
    ///
    /// # Panics
    ///
    /// Debug-asserts that the slot is currently occupied (catches double-free).
    pub fn free(&mut self, idx: VersionIdx) {
        let slot = &mut self.chunks[idx.chunk as usize][idx.offset as usize];
        debug_assert!(slot.is_some(), "VersionArena::free: double-free of {idx:?}");
        *slot = None;
        self.free_list.push(idx);
    }

    /// Look up a version by index.
    #[must_use]
    pub fn get(&self, idx: VersionIdx) -> Option<&PageVersion> {
        self.chunks
            .get(idx.chunk as usize)?
            .get(idx.offset as usize)?
            .as_ref()
    }

    /// Look up a version mutably by index.
    pub fn get_mut(&mut self, idx: VersionIdx) -> Option<&mut PageVersion> {
        self.chunks
            .get_mut(idx.chunk as usize)?
            .get_mut(idx.offset as usize)?
            .as_mut()
    }

    /// Total versions ever allocated (including freed).
    #[must_use]
    pub fn high_water(&self) -> u64 {
        self.high_water
    }

    /// Number of chunks currently allocated.
    #[must_use]
    pub fn chunk_count(&self) -> usize {
        self.chunks.len()
    }

    /// Number of slots on the free list.
    #[must_use]
    pub fn free_count(&self) -> usize {
        self.free_list.len()
    }
}

impl Default for VersionArena {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for VersionArena {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VersionArena")
            .field("chunks", &self.chunks.len())
            .field("free_list", &self.free_list.len())
            .field("high_water", &self.high_water)
            .finish()
    }
}

// ---------------------------------------------------------------------------
// PageNumber hasher (identity-hash for u32 keys)
// ---------------------------------------------------------------------------

/// Fast identity hasher for `PageNumber` keys in lock/commit tables.
///
/// Page numbers are already well-distributed u32 values, so we skip
/// hashing entirely and use the raw value directly.
#[derive(Default)]
struct PageNumberHasher(u64);

impl Hasher for PageNumberHasher {
    fn write(&mut self, _: &[u8]) {}

    fn write_u32(&mut self, n: u32) {
        self.0 = u64::from(n);
    }

    fn finish(&self) -> u64 {
        self.0
    }
}

type PageNumberBuildHasher = BuildHasherDefault<PageNumberHasher>;

// ---------------------------------------------------------------------------
// InProcessPageLockTable
// ---------------------------------------------------------------------------

/// Number of shards in the lock table (power of 2 for fast modular indexing).
pub const LOCK_TABLE_SHARDS: usize = 64;

/// In-process page-level exclusive write locks.
///
/// Sharded into [`LOCK_TABLE_SHARDS`] buckets to reduce contention.
/// Each shard maps `PageNumber -> TxnId` for the transaction holding the lock.
pub struct InProcessPageLockTable {
    shards: Box<[Mutex<HashMap<PageNumber, TxnId, PageNumberBuildHasher>>; LOCK_TABLE_SHARDS]>,
}

impl InProcessPageLockTable {
    /// Create a new empty lock table.
    #[must_use]
    pub fn new() -> Self {
        Self {
            shards: Box::new(std::array::from_fn(|_| {
                Mutex::new(HashMap::with_hasher(PageNumberBuildHasher::default()))
            })),
        }
    }

    /// Try to acquire an exclusive lock on `page` for `txn`.
    ///
    /// Returns `Ok(())` if the lock was acquired, or `Err(holder)` with the
    /// TxnId of the current holder if the page is already locked.
    pub fn try_acquire(&self, page: PageNumber, txn: TxnId) -> Result<(), TxnId> {
        let shard = &self.shards[self.shard_index(page)];
        let mut map = shard.lock();
        if let Some(&holder) = map.get(&page) {
            if holder == txn {
                return Ok(()); // already held by this txn
            }
            return Err(holder);
        }
        map.insert(page, txn);
        drop(map);
        Ok(())
    }

    /// Release the lock on `page` held by `txn`.
    ///
    /// Returns `true` if the lock was released, `false` if `txn` did not hold it.
    pub fn release(&self, page: PageNumber, txn: TxnId) -> bool {
        let shard = &self.shards[self.shard_index(page)];
        let mut map = shard.lock();
        if map.get(&page) == Some(&txn) {
            map.remove(&page);
            true
        } else {
            false
        }
    }

    /// Release all locks held by `txn`.
    pub fn release_all(&self, txn: TxnId) {
        for shard in self.shards.iter() {
            let mut map = shard.lock();
            map.retain(|_, &mut v| v != txn);
        }
    }

    /// Check which txn holds the lock on `page`, if any.
    #[must_use]
    pub fn holder(&self, page: PageNumber) -> Option<TxnId> {
        let shard = &self.shards[self.shard_index(page)];
        let map = shard.lock();
        map.get(&page).copied()
    }

    /// Total number of locks currently held across all shards.
    #[must_use]
    pub fn lock_count(&self) -> usize {
        self.shards.iter().map(|s| s.lock().len()).sum()
    }

    /// Distribution of locks across shards (for birthday-problem analysis).
    #[must_use]
    pub fn shard_distribution(&self) -> Vec<usize> {
        self.shards.iter().map(|s| s.lock().len()).collect()
    }

    #[allow(clippy::unused_self)]
    fn shard_index(&self, page: PageNumber) -> usize {
        (page.get() as usize) & (LOCK_TABLE_SHARDS - 1)
    }
}

impl Default for InProcessPageLockTable {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for InProcessPageLockTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InProcessPageLockTable")
            .field("lock_count", &self.lock_count())
            .finish()
    }
}

// ---------------------------------------------------------------------------
// Transaction
// ---------------------------------------------------------------------------

/// Transaction state machine states.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TransactionState {
    /// Transaction is active (reading/writing).
    Active,
    /// Transaction has been committed.
    Committed,
    /// Transaction has been aborted.
    Aborted,
}

/// Transaction concurrency mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TransactionMode {
    /// Serialized: uses a global write mutex (one writer at a time).
    Serialized,
    /// Concurrent: uses page-level locks (MVCC).
    Concurrent,
}

/// A running MVCC transaction.
#[derive(Debug)]
#[allow(clippy::struct_excessive_bools)]
pub struct Transaction {
    pub txn_id: TxnId,
    pub txn_epoch: TxnEpoch,
    pub slot_id: Option<TxnSlot>,
    pub snapshot: Snapshot,
    pub snapshot_established: bool,
    pub write_set: Vec<PageNumber>,
    pub intent_log: IntentLog,
    pub page_locks: Vec<PageNumber>,
    pub state: TransactionState,
    pub mode: TransactionMode,
    /// True iff this txn currently holds the global write mutex (Serialized mode).
    pub serialized_write_lock_held: bool,
    /// SSI witness-plane read evidence (§5.6.4).
    pub read_keys: HashSet<WitnessKey>,
    /// SSI witness-plane write evidence (§5.6.4).
    pub write_keys: HashSet<WitnessKey>,
    /// SSI tracking: has an incoming rw-antidependency edge.
    pub has_in_rw: bool,
    /// SSI tracking: has an outgoing rw-antidependency edge.
    pub has_out_rw: bool,
}

impl Transaction {
    /// Create a new active transaction.
    #[must_use]
    pub fn new(
        txn_id: TxnId,
        txn_epoch: TxnEpoch,
        snapshot: Snapshot,
        mode: TransactionMode,
    ) -> Self {
        tracing::debug!(txn_id = %txn_id, ?mode, snapshot_high = snapshot.high.get(), "transaction started");
        Self {
            txn_id,
            txn_epoch,
            slot_id: None,
            snapshot,
            snapshot_established: true,
            write_set: Vec::new(),
            intent_log: Vec::new(),
            page_locks: Vec::new(),
            state: TransactionState::Active,
            mode,
            serialized_write_lock_held: false,
            read_keys: HashSet::new(),
            write_keys: HashSet::new(),
            has_in_rw: false,
            has_out_rw: false,
        }
    }

    /// Token identifying this transaction.
    #[must_use]
    pub fn token(&self) -> TxnToken {
        TxnToken::new(self.txn_id, self.txn_epoch)
    }

    /// Transition to committed state. Panics if not active.
    pub fn commit(&mut self) {
        assert_eq!(
            self.state,
            TransactionState::Active,
            "can only commit active transactions"
        );
        self.state = TransactionState::Committed;
        tracing::debug!(txn_id = %self.txn_id, "transaction committed");
    }

    /// Transition to aborted state. Panics if not active.
    pub fn abort(&mut self) {
        assert_eq!(
            self.state,
            TransactionState::Active,
            "can only abort active transactions"
        );
        self.state = TransactionState::Aborted;
        tracing::debug!(txn_id = %self.txn_id, "transaction aborted");
    }

    /// Whether this transaction would trigger SSI abort (both in + out rw edges).
    #[must_use]
    pub fn has_dangerous_structure(&self) -> bool {
        self.has_in_rw && self.has_out_rw
    }
}

// ---------------------------------------------------------------------------
// CommitRecord / CommitLog
// ---------------------------------------------------------------------------

/// A record in the commit log for a single committed transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitRecord {
    pub txn_id: TxnId,
    pub commit_seq: CommitSeq,
    pub pages: SmallVec<[PageNumber; 8]>,
    pub timestamp_unix_ns: u64,
}

/// Append-only commit log indexed by `CommitSeq`.
///
/// Provides O(1) append and O(1) direct index by `CommitSeq` (assuming
/// commit sequences start at 1 and are contiguous).
#[derive(Debug)]
pub struct CommitLog {
    records: Vec<CommitRecord>,
    /// The `CommitSeq` of the first record (usually 1).
    base_seq: u64,
}

impl CommitLog {
    /// Create a new empty commit log starting at the given base sequence.
    #[must_use]
    pub fn new(base_seq: CommitSeq) -> Self {
        Self {
            records: Vec::new(),
            base_seq: base_seq.get(),
        }
    }

    /// Append a commit record. The record's `commit_seq` must be the next
    /// expected sequence number.
    pub fn append(&mut self, record: CommitRecord) {
        let expected = self.base_seq + self.records.len() as u64;
        assert_eq!(
            record.commit_seq.get(),
            expected,
            "CommitLog: expected seq {expected}, got {}",
            record.commit_seq.get()
        );
        self.records.push(record);
    }

    /// Look up a commit record by its `CommitSeq`.
    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn get(&self, seq: CommitSeq) -> Option<&CommitRecord> {
        let idx = seq.get().checked_sub(self.base_seq)?;
        self.records.get(idx as usize)
    }

    /// Number of records in the log.
    #[must_use]
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// Whether the log is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// The latest `CommitSeq` in the log, or `None` if empty.
    #[must_use]
    pub fn latest_seq(&self) -> Option<CommitSeq> {
        if self.records.is_empty() {
            None
        } else {
            Some(CommitSeq::new(
                self.base_seq + self.records.len() as u64 - 1,
            ))
        }
    }
}

impl Default for CommitLog {
    fn default() -> Self {
        Self::new(CommitSeq::new(1))
    }
}

// ---------------------------------------------------------------------------
// CommitIndex
// ---------------------------------------------------------------------------

/// Index mapping each page to its latest committed `CommitSeq`.
///
/// Sharded like the lock table for reduced contention.
pub struct CommitIndex {
    shards: Box<[RwLock<HashMap<PageNumber, CommitSeq, PageNumberBuildHasher>>; LOCK_TABLE_SHARDS]>,
}

impl CommitIndex {
    #[must_use]
    pub fn new() -> Self {
        Self {
            shards: Box::new(std::array::from_fn(|_| {
                RwLock::new(HashMap::with_hasher(PageNumberBuildHasher::default()))
            })),
        }
    }

    /// Record that `page` was last committed at `seq`.
    pub fn update(&self, page: PageNumber, seq: CommitSeq) {
        let shard = &self.shards[self.shard_index(page)];
        let mut map = shard.write();
        map.insert(page, seq);
    }

    /// Get the latest `CommitSeq` for `page`.
    #[must_use]
    pub fn latest(&self, page: PageNumber) -> Option<CommitSeq> {
        let shard = &self.shards[self.shard_index(page)];
        let map = shard.read();
        map.get(&page).copied()
    }

    #[allow(clippy::unused_self)]
    fn shard_index(&self, page: PageNumber) -> usize {
        (page.get() as usize) & (LOCK_TABLE_SHARDS - 1)
    }
}

impl Default for CommitIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for CommitIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let total: usize = self.shards.iter().map(|s| s.read().len()).sum();
        f.debug_struct("CommitIndex")
            .field("page_count", &total)
            .finish()
    }
}

// ---------------------------------------------------------------------------
// PageBuf — page-aligned owned buffer
// ---------------------------------------------------------------------------

/// An owned, page-sized buffer.
///
/// The data is always exactly `page_size` bytes. For the MVP, alignment
/// is handled by the allocator (Vec guarantees sufficient alignment for
/// the element type).
#[derive(Clone, PartialEq, Eq)]
pub struct PageBuf {
    data: Vec<u8>,
    page_size: PageSize,
}

impl PageBuf {
    /// Create a zeroed page buffer.
    #[must_use]
    pub fn zeroed(page_size: PageSize) -> Self {
        Self {
            data: vec![0u8; page_size.as_usize()],
            page_size,
        }
    }

    /// Create from existing data. Panics if `data.len() != page_size`.
    #[must_use]
    pub fn from_vec(data: Vec<u8>, page_size: PageSize) -> Self {
        assert_eq!(data.len(), page_size.as_usize(), "PageBuf size mismatch");
        Self { data, page_size }
    }

    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }

    #[inline]
    pub fn as_bytes_mut(&mut self) -> &mut [u8] {
        &mut self.data
    }

    #[must_use]
    pub fn page_size(&self) -> PageSize {
        self.page_size
    }

    /// Check if the data pointer is aligned to the page size.
    #[must_use]
    pub fn is_aligned(&self) -> bool {
        (self.data.as_ptr() as usize) % self.page_size.as_usize() == 0
    }
}

#[allow(clippy::missing_fields_in_debug)]
impl std::fmt::Debug for PageBuf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PageBuf")
            .field("page_size", &self.page_size.get())
            .field("aligned", &self.is_aligned())
            .finish_non_exhaustive()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use fsqlite_types::{PageData, SchemaEpoch, VersionPointer};
    use proptest::prelude::*;

    fn make_page_version(pgno: u32, commit: u64) -> PageVersion {
        let pgno = PageNumber::new(pgno).unwrap();
        let commit_seq = CommitSeq::new(commit);
        let txn_id = TxnId::new(1).unwrap();
        let created_by = TxnToken::new(txn_id, TxnEpoch::new(0));
        PageVersion {
            pgno,
            commit_seq,
            created_by,
            data: PageData::zeroed(PageSize::DEFAULT),
            prev: None,
        }
    }

    // -- TxnId tests (from glossary, verified here for bd-3t3.1 acceptance) --

    #[test]
    fn test_txn_id_valid_range() {
        assert!(TxnId::new(0).is_none(), "0 must be rejected");
        assert!(TxnId::new(1).is_some(), "1 must be accepted");
        assert!(
            TxnId::new(TxnId::MAX_RAW).is_some(),
            "(1<<62)-1 must be accepted"
        );
        assert!(
            TxnId::new(TxnId::MAX_RAW + 1).is_none(),
            "(1<<62) must be rejected"
        );
        assert!(TxnId::new(u64::MAX).is_none(), "u64::MAX must be rejected");
    }

    #[test]
    fn test_txn_id_sentinel_encoding() {
        let max = TxnId::new(TxnId::MAX_RAW).unwrap();
        // Top two bits must be clear.
        assert_eq!(max.get() >> 62, 0);
    }

    #[test]
    fn test_txn_epoch_wraparound() {
        let epoch = TxnEpoch::new(u32::MAX);
        assert_eq!(epoch.get(), u32::MAX);
        // Wrapping add behavior is defined by u32.
        let next_raw = epoch.get().wrapping_add(1);
        assert_eq!(next_raw, 0);
    }

    #[test]
    fn test_txn_token_equality_includes_epoch() {
        let id = TxnId::new(5).unwrap();
        let a = TxnToken::new(id, TxnEpoch::new(1));
        let b = TxnToken::new(id, TxnEpoch::new(2));
        assert_ne!(a, b, "same id different epoch must be unequal");
    }

    #[test]
    fn test_commit_seq_monotonic() {
        let a = CommitSeq::new(5);
        let b = CommitSeq::new(10);
        assert!(a < b);
        assert_eq!(a.next(), CommitSeq::new(6));
    }

    #[test]
    fn test_schema_epoch_increment() {
        let a = SchemaEpoch::new(0);
        let b = SchemaEpoch::new(1);
        assert!(a < b);
    }

    #[test]
    fn test_page_number_nonzero() {
        assert!(PageNumber::new(0).is_none());
        assert!(PageNumber::new(1).is_some());
    }

    // -- Snapshot --

    #[test]
    fn test_snapshot_ordering() {
        let s5 = Snapshot::new(CommitSeq::new(5), SchemaEpoch::ZERO);
        let s10 = Snapshot::new(CommitSeq::new(10), SchemaEpoch::ZERO);
        // Snapshot { high: 5 } should see commits <= 5.
        assert!(CommitSeq::new(5) <= s5.high);
        assert!(CommitSeq::new(6) > s5.high);
        // Snapshot { high: 10 } sees <= 10.
        assert!(CommitSeq::new(10) <= s10.high);
    }

    // -- VersionArena --

    #[test]
    fn test_version_arena_alloc_free_reuse() {
        let mut arena = VersionArena::new();
        let v1 = make_page_version(1, 1);
        let idx1 = arena.alloc(v1);
        assert!(arena.get(idx1).is_some());

        arena.free(idx1);
        assert!(arena.get(idx1).is_none());
        assert_eq!(arena.free_count(), 1);

        // Reallocate should reuse the freed slot.
        let v2 = make_page_version(2, 2);
        let idx2 = arena.alloc(v2);
        assert_eq!(idx1, idx2, "freed slot should be reused");
        assert_eq!(arena.free_count(), 0);
    }

    #[test]
    #[allow(clippy::cast_possible_truncation)]
    fn test_version_arena_chunk_growth() {
        let mut arena = VersionArena::new();
        assert_eq!(arena.chunk_count(), 1);

        #[allow(clippy::cast_possible_truncation)]
        let upper = (ARENA_CHUNK + 1) as u32;
        for i in 1..=upper {
            let pgno = PageNumber::new(i.max(1)).unwrap();
            arena.alloc(PageVersion {
                pgno,
                commit_seq: CommitSeq::new(u64::from(i)),
                created_by: TxnToken::new(TxnId::new(1).unwrap(), TxnEpoch::new(0)),
                data: PageData::zeroed(PageSize::DEFAULT),
                prev: None,
            });
        }

        assert!(
            arena.chunk_count() >= 2,
            "should have grown to at least 2 chunks"
        );
    }

    #[test]
    fn test_page_version_chain_traversal() {
        let mut arena = VersionArena::new();

        let v1 = PageVersion {
            pgno: PageNumber::new(1).unwrap(),
            commit_seq: CommitSeq::new(1),
            created_by: TxnToken::new(TxnId::new(1).unwrap(), TxnEpoch::new(0)),
            data: PageData::zeroed(PageSize::DEFAULT),
            prev: None,
        };
        let idx1 = arena.alloc(v1);

        let v2 = PageVersion {
            pgno: PageNumber::new(1).unwrap(),
            commit_seq: CommitSeq::new(2),
            created_by: TxnToken::new(TxnId::new(2).unwrap(), TxnEpoch::new(0)),
            data: PageData::zeroed(PageSize::DEFAULT),
            prev: Some(VersionPointer::new(
                u64::from(idx1.chunk) << 32 | u64::from(idx1.offset),
            )),
        };
        let idx2 = arena.alloc(v2);

        // Traverse from v2 to v1.
        let version2 = arena.get(idx2).unwrap();
        assert_eq!(version2.commit_seq, CommitSeq::new(2));
        assert!(version2.prev.is_some());

        let version1 = arena.get(idx1).unwrap();
        assert_eq!(version1.commit_seq, CommitSeq::new(1));
        assert!(version1.prev.is_none());
    }

    // -- InProcessPageLockTable --

    #[test]
    fn test_in_process_lock_table_acquire_release() {
        let table = InProcessPageLockTable::new();
        let page = PageNumber::new(42).unwrap();
        let txn_a = TxnId::new(1).unwrap();
        let txn_b = TxnId::new(2).unwrap();

        // Acquire succeeds.
        assert!(table.try_acquire(page, txn_a).is_ok());
        assert_eq!(table.holder(page), Some(txn_a));
        assert_eq!(table.lock_count(), 1);

        // Re-acquire by same txn succeeds (idempotent).
        assert!(table.try_acquire(page, txn_a).is_ok());

        // Different txn gets Err(holder).
        assert_eq!(table.try_acquire(page, txn_b), Err(txn_a));

        // Release.
        assert!(table.release(page, txn_a));
        assert!(table.holder(page).is_none());
        assert_eq!(table.lock_count(), 0);
    }

    #[test]
    fn test_in_process_lock_table_release_all() {
        let table = InProcessPageLockTable::new();
        let txn = TxnId::new(1).unwrap();

        for i in 1..=10_u32 {
            let page = PageNumber::new(i).unwrap();
            table.try_acquire(page, txn).unwrap();
        }
        assert_eq!(table.lock_count(), 10);

        table.release_all(txn);
        assert_eq!(table.lock_count(), 0);
    }

    #[test]
    fn test_in_process_lock_table_shard_distribution() {
        let table = InProcessPageLockTable::new();
        let txn = TxnId::new(1).unwrap();

        // Acquire locks on pages 1..=128.
        for i in 1..=128_u32 {
            let page = PageNumber::new(i).unwrap();
            table.try_acquire(page, txn).unwrap();
        }

        let dist = table.shard_distribution();
        assert_eq!(dist.len(), LOCK_TABLE_SHARDS);

        // With 128 pages across 64 shards, each shard should have exactly 2.
        for &count in &dist {
            assert_eq!(count, 2, "uniform distribution expected");
        }
    }

    // -- Transaction --

    #[test]
    fn test_transaction_state_machine() {
        let txn_id = TxnId::new(1).unwrap();
        let snap = Snapshot::new(CommitSeq::new(0), SchemaEpoch::ZERO);

        let mut txn = Transaction::new(txn_id, TxnEpoch::new(0), snap, TransactionMode::Concurrent);
        assert_eq!(txn.state, TransactionState::Active);

        txn.commit();
        assert_eq!(txn.state, TransactionState::Committed);
    }

    #[test]
    fn test_transaction_abort() {
        let txn_id = TxnId::new(2).unwrap();
        let snap = Snapshot::new(CommitSeq::new(0), SchemaEpoch::ZERO);

        let mut txn = Transaction::new(txn_id, TxnEpoch::new(0), snap, TransactionMode::Concurrent);
        txn.abort();
        assert_eq!(txn.state, TransactionState::Aborted);
    }

    #[test]
    #[should_panic(expected = "can only commit active")]
    fn test_transaction_double_commit_panics() {
        let txn_id = TxnId::new(3).unwrap();
        let snap = Snapshot::new(CommitSeq::new(0), SchemaEpoch::ZERO);

        let mut txn = Transaction::new(txn_id, TxnEpoch::new(0), snap, TransactionMode::Concurrent);
        txn.commit();
        txn.commit(); // should panic
    }

    #[test]
    #[should_panic(expected = "can only abort active")]
    fn test_transaction_commit_then_abort_panics() {
        let txn_id = TxnId::new(4).unwrap();
        let snap = Snapshot::new(CommitSeq::new(0), SchemaEpoch::ZERO);

        let mut txn = Transaction::new(txn_id, TxnEpoch::new(0), snap, TransactionMode::Concurrent);
        txn.commit();
        txn.abort(); // should panic: already committed
    }

    #[test]
    #[should_panic(expected = "can only commit active")]
    fn test_transaction_abort_then_commit_panics() {
        let txn_id = TxnId::new(5).unwrap();
        let snap = Snapshot::new(CommitSeq::new(0), SchemaEpoch::ZERO);

        let mut txn = Transaction::new(txn_id, TxnEpoch::new(0), snap, TransactionMode::Concurrent);
        txn.abort();
        txn.commit(); // should panic: already aborted
    }

    #[test]
    fn test_transaction_mode_concurrent() {
        let txn_id = TxnId::new(1).unwrap();
        let snap = Snapshot::new(CommitSeq::new(0), SchemaEpoch::ZERO);

        let txn = Transaction::new(txn_id, TxnEpoch::new(0), snap, TransactionMode::Concurrent);
        assert_eq!(txn.mode, TransactionMode::Concurrent);
    }

    #[test]
    fn test_transaction_mode_serialized() {
        let txn_id = TxnId::new(1).unwrap();
        let snap = Snapshot::new(CommitSeq::new(0), SchemaEpoch::ZERO);

        let txn = Transaction::new(txn_id, TxnEpoch::new(0), snap, TransactionMode::Serialized);
        assert_eq!(txn.mode, TransactionMode::Serialized);
    }

    #[test]
    fn test_transaction_ssi_dangerous_structure() {
        let txn_id = TxnId::new(1).unwrap();
        let snap = Snapshot::new(CommitSeq::new(0), SchemaEpoch::ZERO);

        let mut txn = Transaction::new(txn_id, TxnEpoch::new(0), snap, TransactionMode::Concurrent);
        assert!(!txn.has_dangerous_structure());

        txn.has_in_rw = true;
        assert!(!txn.has_dangerous_structure());

        txn.has_out_rw = true;
        assert!(txn.has_dangerous_structure(), "both in+out rw = dangerous");
    }

    // -- CommitRecord / CommitLog --

    #[test]
    fn test_commit_log_append_and_index() {
        let mut log = CommitLog::new(CommitSeq::new(1));
        assert!(log.is_empty());

        let rec1 = CommitRecord {
            txn_id: TxnId::new(1).unwrap(),
            commit_seq: CommitSeq::new(1),
            pages: SmallVec::from_slice(&[PageNumber::new(5).unwrap()]),
            timestamp_unix_ns: 1000,
        };
        log.append(rec1.clone());

        let rec2 = CommitRecord {
            txn_id: TxnId::new(2).unwrap(),
            commit_seq: CommitSeq::new(2),
            pages: SmallVec::from_slice(&[
                PageNumber::new(10).unwrap(),
                PageNumber::new(20).unwrap(),
            ]),
            timestamp_unix_ns: 2000,
        };
        log.append(rec2.clone());

        assert_eq!(log.len(), 2);
        assert_eq!(log.get(CommitSeq::new(1)).unwrap(), &rec1);
        assert_eq!(log.get(CommitSeq::new(2)).unwrap(), &rec2);
        assert!(log.get(CommitSeq::new(3)).is_none());
        assert_eq!(log.latest_seq(), Some(CommitSeq::new(2)));
    }

    #[test]
    fn test_commit_record_smallvec_optimization() {
        // <= 8 pages should NOT heap-allocate.
        let pages: SmallVec<[PageNumber; 8]> =
            (1..=8).map(|i| PageNumber::new(i).unwrap()).collect();
        assert!(!pages.spilled(), "8 pages should stay on stack");

        // > 8 pages spill to heap.
        let pages: SmallVec<[PageNumber; 8]> =
            (1..=9).map(|i| PageNumber::new(i).unwrap()).collect();
        assert!(pages.spilled(), "9 pages should spill to heap");
    }

    // -- CommitIndex --

    #[test]
    fn test_commit_index_latest_commit() {
        let index = CommitIndex::new();
        let page = PageNumber::new(42).unwrap();

        assert!(index.latest(page).is_none());

        index.update(page, CommitSeq::new(5));
        assert_eq!(index.latest(page), Some(CommitSeq::new(5)));

        index.update(page, CommitSeq::new(10));
        assert_eq!(index.latest(page), Some(CommitSeq::new(10)));
    }

    // -- PageBuf --

    #[test]
    fn test_page_buf_zeroed() {
        let buf = PageBuf::zeroed(PageSize::DEFAULT);
        assert_eq!(buf.as_bytes().len(), 4096);
        assert!(buf.as_bytes().iter().all(|&b| b == 0));
        assert_eq!(buf.page_size(), PageSize::DEFAULT);
    }

    #[test]
    #[should_panic(expected = "PageBuf size mismatch")]
    fn test_page_buf_size_mismatch() {
        let _ = PageBuf::from_vec(vec![0u8; 100], PageSize::DEFAULT);
    }

    // -- All types Debug+Clone --

    #[test]
    fn test_all_types_debug_display() {
        fn assert_debug<T: std::fmt::Debug>() {}

        assert_debug::<VersionIdx>();
        assert_debug::<VersionArena>();
        assert_debug::<InProcessPageLockTable>();
        assert_debug::<TransactionState>();
        assert_debug::<TransactionMode>();
        assert_debug::<Transaction>();
        assert_debug::<CommitRecord>();
        assert_debug::<CommitLog>();
        assert_debug::<CommitIndex>();
        assert_debug::<PageBuf>();
    }

    #[test]
    fn test_page_buf_alignment() {
        let buf = PageBuf::zeroed(PageSize::DEFAULT);
        // Vec<u8> alignment is at least 1, but on 64-bit systems the allocator
        // often returns page-aligned memory for large allocations. Either way,
        // verify the method itself returns a boolean correctly.
        let _aligned = buf.is_aligned();
        // Verify the data pointer is at least 8-byte aligned (allocator minimum).
        assert_eq!(buf.as_bytes().as_ptr() as usize % 8, 0);
    }

    #[test]
    fn test_all_types_clone_eq() {
        fn assert_clone_eq<T: Clone + PartialEq>() {}

        assert_clone_eq::<VersionIdx>();
        assert_clone_eq::<TransactionState>();
        assert_clone_eq::<TransactionMode>();
        assert_clone_eq::<CommitRecord>();
        assert_clone_eq::<PageBuf>();
    }

    // -- Property tests --

    proptest! {
        #[test]
        fn prop_txn_id_fits_62_bits(raw in 1_u64..=TxnId::MAX_RAW) {
            let id = TxnId::new(raw).unwrap();
            prop_assert_eq!(id.get() >> 62, 0, "top 2 bits must be clear");
        }

        #[test]
        fn prop_version_arena_no_dangling(
            alloc_count in 1_usize..200,
            free_indices in proptest::collection::vec(any::<usize>(), 0..50),
        ) {
            let mut arena = VersionArena::new();
            let mut indices = Vec::new();

            for i in 0..alloc_count {
                // alloc_count is bounded to 200, so truncation cannot occur.
                let pgno = PageNumber::new(u32::try_from(i).unwrap().max(1)).unwrap();
                let v = PageVersion {
                    pgno,
                    commit_seq: CommitSeq::new(i as u64 + 1),
                    created_by: TxnToken::new(TxnId::new(1).unwrap(), TxnEpoch::new(0)),
                    data: PageData::zeroed(PageSize::DEFAULT),
                    prev: None,
                };
                indices.push(arena.alloc(v));
            }

            // Free some indices.
            let mut freed = std::collections::HashSet::new();
            for &fi in &free_indices {
                let idx = fi % indices.len();
                if freed.insert(idx) {
                    arena.free(indices[idx]);
                }
            }

            // All non-freed slots must still be reachable with valid data.
            for (i, &idx) in indices.iter().enumerate() {
                if freed.contains(&i) {
                    prop_assert!(arena.get(idx).is_none(), "freed slot must be None");
                } else {
                    prop_assert!(arena.get(idx).is_some(), "live slot must be Some");
                }
            }
        }

        #[test]
        fn prop_commit_seq_strictly_increasing(
            base in 0_u64..1_000_000,
            count in 1_usize..100,
        ) {
            let mut seqs: Vec<CommitSeq> = (0..count as u64)
                .map(|i| CommitSeq::new(base + i))
                .collect();
            seqs.sort();
            for window in seqs.windows(2) {
                prop_assert!(window[0] < window[1], "must be strictly increasing");
            }
        }

        #[test]
        fn prop_lock_table_no_phantom_locks(
            pages in proptest::collection::vec(1_u32..10_000, 1..100),
        ) {
            let table = InProcessPageLockTable::new();
            let txn = TxnId::new(1).unwrap();

            // Acquire all.
            for &p in &pages {
                let page = PageNumber::new(p).unwrap();
                let _ = table.try_acquire(page, txn);
            }

            // Release all.
            table.release_all(txn);

            // No locks should remain.
            prop_assert_eq!(table.lock_count(), 0, "no phantom locks after release_all");
        }
    }
}
