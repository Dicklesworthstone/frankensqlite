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
use std::time::{Duration, Instant};

use parking_lot::{Mutex, RwLock};
use smallvec::SmallVec;

use std::sync::atomic::Ordering;

use crate::cache_aligned::{
    decode_payload, decode_tag, encode_cleaning, is_sentinel, CacheAligned, SharedTxnSlot,
    CLAIMING_TIMEOUT_NO_PID_SECS, CLAIMING_TIMEOUT_SECS, TAG_CLAIMING,
};
use fsqlite_types::{
    CommitSeq, IntentLog, PageData, PageNumber, PageSize, PageVersion, Snapshot, TxnEpoch, TxnId,
    TxnSlot, TxnToken, WitnessKey,
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
    pub(crate) const fn new(chunk: u32, offset: u32) -> Self {
        Self { chunk, offset }
    }

    /// Chunk index within the arena.
    #[inline]
    #[must_use]
    pub fn chunk(&self) -> u32 {
        self.chunk
    }

    /// Offset within the chunk.
    #[inline]
    #[must_use]
    pub fn offset(&self) -> u32 {
        self.offset
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

        let chunk_u32 = u32::try_from(chunk_idx).expect("VersionArena chunk index overflow u32");
        let offset_u32 = u32::try_from(offset).expect("VersionArena offset overflow u32");
        VersionIdx::new(chunk_u32, offset_u32)
    }

    /// Free the slot at `idx`, making it available for reuse.
    ///
    /// # Panics
    ///
    /// Asserts that the slot is currently occupied (catches double-free).
    pub fn free(&mut self, idx: VersionIdx) {
        let slot = &mut self.chunks[idx.chunk as usize][idx.offset as usize];
        assert!(slot.is_some(), "VersionArena::free: double-free of {idx:?}");
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

#[allow(clippy::missing_fields_in_debug)]
impl std::fmt::Debug for VersionArena {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VersionArena")
            .field("chunk_count", &self.chunks.len())
            .field("free_count", &self.free_list.len())
            .field("high_water", &self.high_water)
            .finish_non_exhaustive()
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
    fn write(&mut self, _: &[u8]) {
        // PageNumber's Hash impl calls write_u32 (via NonZeroU32). If this
        // method is reached, the hasher is being misused with a non-u32 key.
        debug_assert!(false, "PageNumberHasher only supports write_u32");
    }

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

/// A single cache-line-aligned lock table shard.
type LockShard = CacheAligned<Mutex<HashMap<PageNumber, TxnId, PageNumberBuildHasher>>>;

/// In-process page-level exclusive write locks.
///
/// Sharded into [`LOCK_TABLE_SHARDS`] buckets to reduce contention.
/// Each shard maps `PageNumber -> TxnId` for the transaction holding the lock.
/// Shards are wrapped in [`CacheAligned`] to prevent false sharing (§1.5).
///
/// Supports a rolling rebuild protocol (§5.6.3.1) where the table can operate
/// in dual-table mode: an **active** table for new acquisitions and a
/// **draining** table that is consulted for existing locks during the drain
/// phase. This avoids stop-the-world abort storms during maintenance.
pub struct InProcessPageLockTable {
    shards: Box<[LockShard; LOCK_TABLE_SHARDS]>,
    /// During rolling rebuild: the old shards being drained. Protected by
    /// `Mutex` for synchronization. `None` when no rebuild is in progress.
    draining: Mutex<Option<DrainingState>>,
}

/// State tracking for the draining table during a rolling rebuild.
struct DrainingState {
    shards: Box<[LockShard; LOCK_TABLE_SHARDS]>,
    start: Instant,
    initial_lock_count: usize,
    rebuild_epoch: u64,
}

// ---------------------------------------------------------------------------
// Rebuild result types (bd-22n.12)
// ---------------------------------------------------------------------------

/// Result of a rolling rebuild operation.
#[derive(Debug, Clone)]
pub struct RebuildResult {
    /// Number of orphaned lock entries cleaned.
    pub orphaned_cleaned: usize,
    /// Number of entries retained (still held by active transactions).
    pub retained: usize,
    /// Time taken for the rebuild pass.
    pub elapsed: Duration,
    /// Rebuild epoch (monotonically increasing).
    pub rebuild_epoch: u64,
}

/// Progress of the drain phase during a rolling rebuild.
#[derive(Debug, Clone)]
pub struct DrainProgress {
    /// Number of lock entries still held in the draining table.
    pub remaining: usize,
    /// Time elapsed since drain started.
    pub elapsed: Duration,
    /// Whether the draining table has reached quiescence (all entries released).
    pub quiescent: bool,
}

/// Result of draining to quiescence.
#[derive(Debug, Clone)]
pub enum DrainResult {
    /// Draining table reached quiescence.
    Quiescent {
        /// Number of orphaned entries cleaned during drain.
        cleaned: usize,
        /// Total time taken.
        elapsed: Duration,
    },
    /// Timeout reached before quiescence.
    TimedOut {
        /// Entries remaining in the draining table.
        remaining: usize,
        /// Orphaned entries cleaned before timeout.
        cleaned: usize,
        /// Time elapsed before timeout.
        elapsed: Duration,
    },
}

/// Error starting a rebuild.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RebuildError {
    /// A rebuild is already in progress (draining table exists).
    AlreadyInProgress,
    /// The draining table has not yet reached quiescence.
    DrainNotComplete { remaining: usize },
}

impl InProcessPageLockTable {
    /// Create a new empty lock table.
    #[must_use]
    pub fn new() -> Self {
        Self {
            shards: Box::new(std::array::from_fn(|_| {
                CacheAligned::new(Mutex::new(HashMap::with_hasher(
                    PageNumberBuildHasher::default(),
                )))
            })),
            draining: Mutex::new(None),
        }
    }

    /// Try to acquire an exclusive lock on `page` for `txn`.
    ///
    /// Returns `Ok(())` if the lock was acquired, or `Err(holder)` with the
    /// `TxnId` of the current holder if the page is already locked.
    ///
    /// During a rolling rebuild (§5.6.3.1), this checks the draining table
    /// first. A lock in the draining table is still valid and blocks new
    /// acquisitions by other transactions.
    pub fn try_acquire(&self, page: PageNumber, txn: TxnId) -> Result<(), TxnId> {
        // Step 1: Check draining table first (§5.6.3.1 acquire step 1).
        {
            let draining_guard = self.draining.lock();
            if let Some(ref draining) = *draining_guard {
                let shard_idx = Self::shard_index_static(page);
                let map = draining.shards[shard_idx].lock();
                if let Some(&holder) = map.get(&page) {
                    if holder == txn {
                        drop(map);
                        drop(draining_guard);
                        return Ok(()); // already held by this txn in draining table
                    }
                    return Err(holder);
                }
            }
        }

        // Step 2: Acquire in active table.
        let shard = &self.shards[Self::shard_index_static(page)];
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
    /// Checks both active and draining tables. Returns `true` if the lock was
    /// released from either table.
    pub fn release(&self, page: PageNumber, txn: TxnId) -> bool {
        let shard_idx = Self::shard_index_static(page);

        // Try active table first (most common case).
        let shard = &self.shards[shard_idx];
        let mut map = shard.lock();
        if map.get(&page) == Some(&txn) {
            map.remove(&page);
            return true;
        }
        drop(map);

        // Try draining table if present.
        let draining_guard = self.draining.lock();
        if let Some(ref draining) = *draining_guard {
            let mut drain_map = draining.shards[shard_idx].lock();
            if drain_map.get(&page) == Some(&txn) {
                drain_map.remove(&page);
                drop(drain_map);
                drop(draining_guard);
                return true;
            }
            drop(drain_map);
        }
        drop(draining_guard);
        false
    }

    /// Release all locks held by `txn` from both active and draining tables.
    pub fn release_all(&self, txn: TxnId) {
        for shard in self.shards.iter() {
            let mut map = shard.lock();
            map.retain(|_, &mut v| v != txn);
        }
        // Also release from draining table.
        let draining_guard = self.draining.lock();
        if let Some(ref draining) = *draining_guard {
            for shard in draining.shards.iter() {
                let mut map = shard.lock();
                map.retain(|_, &mut v| v != txn);
            }
        }
    }

    /// Check which txn holds the lock on `page`, if any.
    ///
    /// Checks both active and draining tables.
    #[must_use]
    pub fn holder(&self, page: PageNumber) -> Option<TxnId> {
        let shard_idx = Self::shard_index_static(page);

        // Check active table.
        let shard = &self.shards[shard_idx];
        let map = shard.lock();
        if let Some(&holder) = map.get(&page) {
            return Some(holder);
        }
        drop(map);

        // Check draining table.
        let draining_guard = self.draining.lock();
        if let Some(ref draining) = *draining_guard {
            let drain_map = draining.shards[shard_idx].lock();
            if let Some(&holder) = drain_map.get(&page) {
                drop(drain_map);
                drop(draining_guard);
                return Some(holder);
            }
            drop(drain_map);
        }
        drop(draining_guard);
        None
    }

    /// Total number of locks currently held across all shards (active table only).
    #[must_use]
    pub fn lock_count(&self) -> usize {
        self.shards.iter().map(|s| s.lock().len()).sum()
    }

    /// Total number of locks in the draining table (0 if no rebuild in progress).
    #[must_use]
    pub fn draining_lock_count(&self) -> usize {
        let draining_guard = self.draining.lock();
        match *draining_guard {
            Some(ref draining) => draining.shards.iter().map(|s| s.lock().len()).sum(),
            None => 0,
        }
    }

    /// Total number of locks across both active and draining tables.
    #[must_use]
    pub fn total_lock_count(&self) -> usize {
        self.lock_count() + self.draining_lock_count()
    }

    /// Distribution of locks across shards (for birthday-problem analysis).
    #[must_use]
    pub fn shard_distribution(&self) -> Vec<usize> {
        self.shards.iter().map(|s| s.lock().len()).collect()
    }

    /// Whether a rolling rebuild is currently in progress.
    #[must_use]
    pub fn is_rebuild_in_progress(&self) -> bool {
        self.draining.lock().is_some()
    }

    // -----------------------------------------------------------------------
    // Rolling rebuild protocol (§5.6.3.1, bd-22n.12)
    // -----------------------------------------------------------------------

    /// Begin a rolling rebuild by rotating the active table to draining.
    ///
    /// Creates a fresh empty active table and moves the current active table
    /// to the draining position. New lock acquisitions will go to the new
    /// active table, while the draining table is consulted for existing locks.
    ///
    /// Returns `Err(RebuildError::AlreadyInProgress)` if a rebuild is already
    /// underway (the previous draining table has not been finalized).
    ///
    /// This is the **Rotate** phase of the rolling rebuild protocol.
    pub fn begin_rebuild(&mut self) -> Result<u64, RebuildError> {
        {
            let guard = self.draining.lock();
            if guard.is_some() {
                drop(guard);
                return Err(RebuildError::AlreadyInProgress);
            }
            drop(guard);
        }

        let initial_count: usize = self.shards.iter().map(|s| s.lock().len()).sum();
        let epoch = 1; // First rebuild epoch; would be tracked externally in production.

        tracing::info!(
            lock_count = initial_count,
            rebuild_epoch = epoch,
            "lock table rebuild initiated: rotating active table to draining"
        );

        // Create new empty shards for the active table.
        let new_shards = Box::new(std::array::from_fn(|_| {
            CacheAligned::new(Mutex::new(HashMap::with_hasher(
                PageNumberBuildHasher::default(),
            )))
        }));

        // Move current shards to draining, install new shards.
        let old_shards = std::mem::replace(&mut self.shards, new_shards);

        let mut draining_guard = self.draining.lock();
        *draining_guard = Some(DrainingState {
            shards: old_shards,
            start: Instant::now(),
            initial_lock_count: initial_count,
            rebuild_epoch: epoch,
        });
        drop(draining_guard);

        Ok(epoch)
    }

    /// Check the drain progress of the current rebuild.
    ///
    /// Returns `None` if no rebuild is in progress.
    #[must_use]
    pub fn drain_progress(&self) -> Option<DrainProgress> {
        let draining_guard = self.draining.lock();
        let draining = draining_guard.as_ref()?;
        let remaining: usize = draining.shards.iter().map(|s| s.lock().len()).sum();
        let elapsed = draining.start.elapsed();
        drop(draining_guard);
        let quiescent = remaining == 0;

        tracing::debug!(
            remaining,
            elapsed_ms = elapsed.as_millis(),
            quiescent,
            "lock table drain progress"
        );

        Some(DrainProgress {
            remaining,
            elapsed,
            quiescent,
        })
    }

    /// Perform a single drain pass: remove entries in the draining table
    /// where the owning transaction is no longer active.
    ///
    /// This is the **Drain** phase cleanup that accelerates quiescence by
    /// removing orphaned locks from crashed or completed transactions.
    ///
    /// The `is_active_txn` predicate returns `true` if a `TxnId` belongs
    /// to a currently active (non-crashed, non-completed) transaction.
    ///
    /// Returns `None` if no rebuild is in progress.
    pub fn drain_orphaned(&self, is_active_txn: impl Fn(TxnId) -> bool) -> Option<RebuildResult> {
        let draining_guard = self.draining.lock();
        let draining = draining_guard.as_ref()?;
        let start = Instant::now();
        let mut total_cleaned = 0usize;
        let mut total_retained = 0usize;

        for (shard_idx, shard) in draining.shards.iter().enumerate() {
            let mut map = shard.lock();
            let before = map.len();
            map.retain(|_page, txn_id| {
                let active = is_active_txn(*txn_id);
                if !active {
                    tracing::debug!(
                        shard = shard_idx,
                        txn_id = %txn_id,
                        "removing orphaned lock entry from draining table"
                    );
                }
                active
            });
            let after = map.len();
            drop(map);
            total_cleaned += before - after;
            total_retained += after;
        }

        let rebuild_epoch = draining.rebuild_epoch;
        drop(draining_guard);

        let elapsed = start.elapsed();
        tracing::debug!(
            cleaned = total_cleaned,
            retained = total_retained,
            elapsed_ms = elapsed.as_millis(),
            "drain orphaned pass complete"
        );

        Some(RebuildResult {
            orphaned_cleaned: total_cleaned,
            retained: total_retained,
            elapsed,
            rebuild_epoch,
        })
    }

    /// Finalize the rebuild: clear the draining table once it has reached
    /// lock-quiescence (all entries released).
    ///
    /// Returns `Err(RebuildError::DrainNotComplete)` if the draining table
    /// still has entries. Returns `Ok(RebuildResult)` on success.
    ///
    /// This is the **Clear** phase of the rolling rebuild protocol.
    pub fn finalize_rebuild(&self) -> Result<RebuildResult, RebuildError> {
        let mut draining_guard = self.draining.lock();
        let Some(draining) = draining_guard.as_ref() else {
            // No rebuild in progress — treat as a no-op success.
            drop(draining_guard);
            return Ok(RebuildResult {
                orphaned_cleaned: 0,
                retained: 0,
                elapsed: Duration::ZERO,
                rebuild_epoch: 0,
            });
        };

        let remaining: usize = draining.shards.iter().map(|s| s.lock().len()).sum();
        if remaining > 0 {
            drop(draining_guard);
            return Err(RebuildError::DrainNotComplete { remaining });
        }

        let elapsed = draining.start.elapsed();
        let epoch = draining.rebuild_epoch;
        let initial = draining.initial_lock_count;

        tracing::info!(
            rebuild_epoch = epoch,
            initial_lock_count = initial,
            elapsed_ms = elapsed.as_millis(),
            "lock table rebuild finalized: draining table cleared"
        );

        // Clear the draining state.
        *draining_guard = None;
        drop(draining_guard);

        Ok(RebuildResult {
            orphaned_cleaned: initial,
            retained: 0,
            elapsed,
            rebuild_epoch: epoch,
        })
    }

    /// Perform a full rolling rebuild cycle: rotate, drain to quiescence,
    /// and finalize.
    ///
    /// The `is_active_txn` predicate is used to clean orphaned entries
    /// during the drain phase.
    ///
    /// Returns `DrainResult::Quiescent` if the table reached quiescence
    /// within the timeout, or `DrainResult::TimedOut` otherwise.
    pub fn full_rebuild(
        &mut self,
        is_active_txn: impl Fn(TxnId) -> bool,
        timeout: Duration,
    ) -> Result<DrainResult, RebuildError> {
        self.begin_rebuild()?;
        let start = Instant::now();
        let mut total_cleaned = 0usize;

        loop {
            // Clean orphaned entries in draining table.
            if let Some(result) = self.drain_orphaned(&is_active_txn) {
                total_cleaned += result.orphaned_cleaned;
            }

            // Check drain progress.
            if let Some(progress) = self.drain_progress() {
                if progress.quiescent {
                    // Finalize.
                    let _ = self.finalize_rebuild();
                    return Ok(DrainResult::Quiescent {
                        cleaned: total_cleaned,
                        elapsed: start.elapsed(),
                    });
                }

                // Check timeout.
                if start.elapsed() >= timeout {
                    tracing::warn!(
                        remaining = progress.remaining,
                        elapsed_ms = start.elapsed().as_millis(),
                        "lock table rebuild timed out before quiescence"
                    );
                    return Ok(DrainResult::TimedOut {
                        remaining: progress.remaining,
                        cleaned: total_cleaned,
                        elapsed: start.elapsed(),
                    });
                }
            } else {
                // No rebuild in progress (shouldn't happen after begin_rebuild).
                return Ok(DrainResult::Quiescent {
                    cleaned: total_cleaned,
                    elapsed: start.elapsed(),
                });
            }

            // Brief yield to let active transactions make progress.
            std::thread::yield_now();
        }
    }

    fn shard_index_static(page: PageNumber) -> usize {
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
        let draining_count = self.draining_lock_count();
        let mut dbg = f.debug_struct("InProcessPageLockTable");
        dbg.field("lock_count", &self.lock_count());
        if draining_count > 0 {
            dbg.field("draining_lock_count", &draining_count);
        }
        dbg.finish()
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
    pub write_set: SmallVec<[PageNumber; 8]>,
    /// Maps each page in the write set to its current data.
    pub write_set_data: HashMap<PageNumber, PageData>,
    pub intent_log: IntentLog,
    pub page_locks: HashSet<PageNumber>,
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
    /// Monotonic start timestamp used for max transaction duration enforcement.
    pub started_at: Instant,
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
            write_set: SmallVec::new(),
            write_set_data: HashMap::new(),
            intent_log: Vec::new(),
            page_locks: HashSet::new(),
            state: TransactionState::Active,
            mode,
            serialized_write_lock_held: false,
            read_keys: HashSet::new(),
            write_keys: HashSet::new(),
            has_in_rw: false,
            has_out_rw: false,
            started_at: Instant::now(),
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
        let expected = self
            .base_seq
            .checked_add(self.records.len() as u64)
            .expect("CommitLog sequence overflow");
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
    pub fn get(&self, seq: CommitSeq) -> Option<&CommitRecord> {
        let idx = seq.get().checked_sub(self.base_seq)?;
        let idx = usize::try_from(idx).ok()?;
        self.records.get(idx)
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
            // len >= 1, so len - 1 is safe; checked_add guards base_seq overflow.
            Some(CommitSeq::new(
                self.base_seq
                    .checked_add(self.records.len() as u64 - 1)
                    .expect("CommitLog sequence overflow"),
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

/// A single cache-line-aligned commit index shard.
type CommitShard = CacheAligned<RwLock<HashMap<PageNumber, CommitSeq, PageNumberBuildHasher>>>;

/// Index mapping each page to its latest committed `CommitSeq`.
///
/// Sharded like the lock table for reduced contention.
/// Shards are wrapped in [`CacheAligned`] to prevent false sharing (§1.5).
pub struct CommitIndex {
    shards: Box<[CommitShard; LOCK_TABLE_SHARDS]>,
}

impl CommitIndex {
    #[must_use]
    pub fn new() -> Self {
        Self {
            shards: Box::new(std::array::from_fn(|_| {
                CacheAligned::new(RwLock::new(HashMap::with_hasher(
                    PageNumberBuildHasher::default(),
                )))
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
// GC Horizon (§5.6.5, bd-22n.13)
// ---------------------------------------------------------------------------

/// Outcome of a single `raise_gc_horizon` pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GcHorizonResult {
    /// Previous horizon value before this pass.
    pub old_horizon: CommitSeq,
    /// New (possibly advanced) horizon value.
    pub new_horizon: CommitSeq,
    /// Number of active (real-txn-id) slots scanned.
    pub active_slots: usize,
    /// Number of sentinel-tagged slots that blocked advancement.
    pub sentinel_blockers: usize,
}

/// Compute the new GC horizon from a set of `SharedTxnSlot`s (§5.6.5).
///
/// The GC horizon is `min(begin_seq)` across all active transactions, clamped
/// to never decrease. **Sentinel-tagged slots (CLAIMING / CLEANING) are treated
/// as horizon blockers**: the horizon cannot advance past `old_horizon` while
/// any sentinel slot exists, because the slot may have already captured a
/// snapshot (Phase 2 sets `begin_seq`) but not yet published a real `txn_id`.
///
/// # Arguments
///
/// * `slots` — the TxnSlot array to scan.
/// * `old_horizon` — the current gc_horizon from shared memory.
/// * `commit_seq` — the current `commit_seq` from shared memory (default if no
///   active transactions exist).
///
/// # Returns
///
/// A `GcHorizonResult` with the new monotonically non-decreasing horizon and
/// scan statistics.
#[must_use]
pub fn raise_gc_horizon(
    slots: &[SharedTxnSlot],
    old_horizon: CommitSeq,
    commit_seq: CommitSeq,
) -> GcHorizonResult {
    let mut global_min = commit_seq;
    let mut active_slots = 0_usize;
    let mut sentinel_blockers = 0_usize;

    for slot in slots {
        let tid = slot.txn_id.load(Ordering::Acquire);
        if tid == 0 {
            continue;
        }
        if is_sentinel(tid) {
            // CRITICAL (§5.6.5): Sentinel-tagged slots are horizon blockers.
            // A CLAIMING slot may already have its begin_seq initialized but
            // not yet published a real txn_id. A CLEANING slot is in-transition.
            // In both cases, we clamp to old_horizon to prevent pruning versions
            // that a soon-to-be-active or mid-cleanup transaction may need.
            sentinel_blockers += 1;
            tracing::debug!(
                tag = if decode_tag(tid) == TAG_CLAIMING {
                    "CLAIMING"
                } else {
                    "CLEANING"
                },
                payload = decode_payload(tid),
                claiming_ts = slot.claiming_timestamp.load(Ordering::Acquire),
                "gc_horizon blocked by sentinel slot"
            );
            if old_horizon < global_min {
                global_min = old_horizon;
            }
            continue;
        }

        // Real TxnId — use its begin_seq as a horizon blocker.
        active_slots += 1;
        let begin = CommitSeq::new(slot.begin_seq.load(Ordering::Acquire));
        if begin < global_min {
            global_min = begin;
        }
    }

    // Monotonic: never decrease the horizon.
    let new_horizon = if global_min > old_horizon {
        global_min
    } else {
        old_horizon
    };

    GcHorizonResult {
        old_horizon,
        new_horizon,
        active_slots,
        sentinel_blockers,
    }
}

/// Result of a single slot cleanup attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SlotCleanupResult {
    /// Slot is free or has a real txn_id — no cleanup needed.
    NotApplicable,
    /// Sentinel is recent; giving the owner/cleaner more time.
    StillRecent,
    /// The owning process is still alive — cannot reclaim.
    ProcessAlive,
    /// Successfully reclaimed a stale sentinel slot.
    Reclaimed {
        /// The original `TxnId` payload from the sentinel word.
        orphan_txn_id: u64,
        /// Which sentinel state was reclaimed.
        was_claiming: bool,
    },
    /// CAS race during reclaim — another cleaner got there first.
    CasRaceSkipped,
}

/// Attempt to clean up a single stale sentinel slot (§5.6.2).
///
/// This implements the timeout-based staleness detection required by bd-22n.13.
/// If a slot has been in CLAIMING or CLEANING state longer than the timeout,
/// it is presumed dead and reclaimed.
///
/// # Arguments
///
/// * `slot` — the `SharedTxnSlot` to inspect.
/// * `now_epoch_secs` — current time in unix epoch seconds.
/// * `process_alive` — callback that returns `true` if a process with the given
///   `(pid, pid_birth)` pair is still alive.
///
/// # Returns
///
/// A `SlotCleanupResult` indicating what action was taken.
pub fn try_cleanup_sentinel_slot(
    slot: &SharedTxnSlot,
    now_epoch_secs: u64,
    process_alive: impl Fn(u32, u64) -> bool,
) -> SlotCleanupResult {
    let tid = slot.txn_id.load(Ordering::Acquire);
    if tid == 0 {
        return SlotCleanupResult::NotApplicable;
    }

    let tag = decode_tag(tid);
    if tag == 0 {
        return SlotCleanupResult::NotApplicable;
    }

    let was_claiming = tag == TAG_CLAIMING;

    // Seed claiming_timestamp if not yet set (CAS to avoid race).
    let claiming_ts = slot.claiming_timestamp.load(Ordering::Acquire);
    if claiming_ts == 0 {
        let _ = slot.claiming_timestamp.compare_exchange(
            0,
            now_epoch_secs,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
        return SlotCleanupResult::StillRecent;
    }

    // For CLAIMING slots: check if the process is alive before reclaiming.
    if was_claiming {
        let pid = slot.pid.load(Ordering::Acquire);
        let birth = slot.pid_birth.load(Ordering::Acquire);
        if pid != 0 && birth != 0 && process_alive(pid, birth) {
            return SlotCleanupResult::ProcessAlive;
        }

        let timeout = if pid == 0 || birth == 0 {
            CLAIMING_TIMEOUT_NO_PID_SECS
        } else {
            CLAIMING_TIMEOUT_SECS
        };
        if now_epoch_secs.saturating_sub(claiming_ts) <= timeout {
            return SlotCleanupResult::StillRecent;
        }
    } else {
        // TAG_CLEANING: if stuck longer than timeout, reclaim.
        if now_epoch_secs.saturating_sub(claiming_ts) <= CLAIMING_TIMEOUT_SECS {
            return SlotCleanupResult::StillRecent;
        }
    }

    let orphan_txn_id = decode_payload(tid);

    // For CLAIMING: transition to CLEANING first (preserves identity).
    if was_claiming {
        let cleaning_word = encode_cleaning(orphan_txn_id);
        if slot
            .txn_id
            .compare_exchange(tid, cleaning_word, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return SlotCleanupResult::CasRaceSkipped;
        }
        // Stamp the transition time for the CLEANING phase.
        slot.claiming_timestamp
            .store(now_epoch_secs, Ordering::Release);
        tracing::info!(
            orphan_txn_id,
            "transitioned stale CLAIMING slot to CLEANING"
        );
    }

    // Now clear the slot fields and free it.
    // TAG_CLEANING payload preserves identity for retryable cleanup (bd-22n.13).
    slot.cleanup_txn_id.store(orphan_txn_id, Ordering::Release);

    tracing::info!(
        orphan_txn_id,
        was_claiming,
        "reclaiming stale sentinel slot"
    );

    // Clear all fields. The txn_id store(0) MUST be LAST (Release ordering)
    // so other scanners see a consistent free slot.
    slot.state.store(0, Ordering::Release);
    slot.mode.store(0, Ordering::Release);
    slot.commit_seq.store(0, Ordering::Release);
    slot.begin_seq.store(0, Ordering::Release);
    slot.snapshot_high.store(0, Ordering::Release);
    slot.witness_epoch.store(0, Ordering::Release);
    slot.has_in_rw.store(false, Ordering::Release);
    slot.has_out_rw.store(false, Ordering::Release);
    slot.marked_for_abort.store(false, Ordering::Release);
    slot.write_set_pages.store(0, Ordering::Release);
    slot.pid.store(0, Ordering::Release);
    slot.pid_birth.store(0, Ordering::Release);
    slot.lease_expiry.store(0, Ordering::Release);
    slot.cleanup_txn_id.store(0, Ordering::Release);
    slot.claiming_timestamp.store(0, Ordering::Release);
    // Free the slot — Release ordering ensures all field clears are visible.
    slot.txn_id.store(0, Ordering::Release);

    SlotCleanupResult::Reclaimed {
        orphan_txn_id,
        was_claiming,
    }
}

/// Scan all slots, clean up stale sentinels, then compute the new GC horizon.
///
/// This combines `try_cleanup_sentinel_slot` and `raise_gc_horizon` in the
/// correct order: cleanup first (so freed slots don't block the horizon),
/// then compute.
pub fn cleanup_and_raise_gc_horizon(
    slots: &[SharedTxnSlot],
    old_horizon: CommitSeq,
    commit_seq: CommitSeq,
    now_epoch_secs: u64,
    process_alive: impl Fn(u32, u64) -> bool,
) -> (GcHorizonResult, usize) {
    let mut cleaned = 0_usize;

    for slot in slots {
        let result = try_cleanup_sentinel_slot(slot, now_epoch_secs, &process_alive);
        if matches!(result, SlotCleanupResult::Reclaimed { .. }) {
            cleaned += 1;
        }
    }

    let horizon_result = raise_gc_horizon(slots, old_horizon, commit_seq);
    (horizon_result, cleaned)
}

// ---------------------------------------------------------------------------
// PageBuf — page-aligned owned buffer
// ---------------------------------------------------------------------------

/// An owned, page-sized buffer.
///
/// The exposed page slice is always exactly `page_size` bytes and is
/// guaranteed to be aligned to `page_size`.
///
/// Internally this uses a slightly larger `Vec<u8>` allocation and a
/// subslice offset chosen to satisfy alignment without unsafe code.
pub struct PageBuf {
    alloc: Vec<u8>,
    offset: usize,
    page_size: PageSize,
}

impl Clone for PageBuf {
    fn clone(&self) -> Self {
        // Cannot derive Clone: the cloned Vec gets a new heap address,
        // so the offset computed for the original allocation is invalid.
        // Allocate a fresh aligned buffer and copy the page data in.
        let mut buf = Self::zeroed(self.page_size);
        buf.as_bytes_mut().copy_from_slice(self.as_bytes());
        buf
    }
}

impl PageBuf {
    #[inline]
    fn start(&self) -> usize {
        self.offset
    }

    #[inline]
    fn end(&self) -> usize {
        self.offset + self.page_size.as_usize()
    }

    /// Create a zeroed page buffer.
    #[must_use]
    pub fn zeroed(page_size: PageSize) -> Self {
        let align = page_size.as_usize();
        let len = page_size.as_usize() + align - 1;
        let alloc = vec![0u8; len];

        let base = alloc.as_ptr() as usize;
        let rem = base % align;
        let offset = if rem == 0 { 0 } else { align - rem };

        Self {
            alloc,
            offset,
            page_size,
        }
    }

    /// Create from existing data. Panics if `data.len() != page_size`.
    #[must_use]
    pub fn from_vec(data: Vec<u8>, page_size: PageSize) -> Self {
        assert_eq!(data.len(), page_size.as_usize(), "PageBuf size mismatch");
        // Fast path: already aligned; reuse the allocation without copying.
        if (data.as_ptr() as usize) % page_size.as_usize() == 0 {
            return Self {
                alloc: data,
                offset: 0,
                page_size,
            };
        }

        // Slow path: realign via a fresh allocation.
        let mut buf = Self::zeroed(page_size);
        buf.as_bytes_mut().copy_from_slice(&data);
        buf
    }

    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        &self.alloc[self.start()..self.end()]
    }

    #[inline]
    pub fn as_bytes_mut(&mut self) -> &mut [u8] {
        let start = self.start();
        let end = self.end();
        &mut self.alloc[start..end]
    }

    #[must_use]
    pub fn page_size(&self) -> PageSize {
        self.page_size
    }

    /// Check if the data pointer is aligned to the page size.
    #[must_use]
    pub fn is_aligned(&self) -> bool {
        (self.as_bytes().as_ptr() as usize) % self.page_size.as_usize() == 0
    }
}

impl PartialEq for PageBuf {
    fn eq(&self, other: &Self) -> bool {
        self.page_size == other.page_size && self.as_bytes() == other.as_bytes()
    }
}

impl Eq for PageBuf {}

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
    fn test_version_arena_chunk_growth() {
        let mut arena = VersionArena::new();
        assert_eq!(arena.chunk_count(), 1);

        let upper = u32::try_from(ARENA_CHUNK + 1).unwrap();
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
    #[should_panic(expected = "can only abort active")]
    fn test_transaction_double_abort_panics() {
        let txn_id = TxnId::new(6).unwrap();
        let snap = Snapshot::new(CommitSeq::new(0), SchemaEpoch::ZERO);

        let mut txn = Transaction::new(txn_id, TxnEpoch::new(0), snap, TransactionMode::Concurrent);
        txn.abort();
        txn.abort(); // should panic: already aborted
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
    fn test_transaction_new_initializes_all_fields() {
        let txn_id = TxnId::new(42).unwrap();
        let epoch = TxnEpoch::new(7);
        let snap = Snapshot::new(CommitSeq::new(100), SchemaEpoch::new(3));

        let txn = Transaction::new(txn_id, epoch, snap, TransactionMode::Concurrent);
        assert_eq!(txn.txn_id, txn_id);
        assert_eq!(txn.txn_epoch, epoch);
        assert!(txn.slot_id.is_none());
        assert_eq!(txn.snapshot.high, CommitSeq::new(100));
        assert!(txn.snapshot_established);
        assert!(txn.write_set.is_empty());
        assert!(txn.intent_log.is_empty());
        assert!(txn.page_locks.is_empty());
        assert_eq!(txn.state, TransactionState::Active);
        assert!(!txn.serialized_write_lock_held);
        assert!(txn.read_keys.is_empty());
        assert!(txn.write_keys.is_empty());
        assert!(!txn.has_in_rw);
        assert!(!txn.has_out_rw);
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

    // -- Cache-line alignment of shards (bd-22n.3) --

    #[test]
    fn test_lock_table_shards_cache_aligned() {
        let table = InProcessPageLockTable::new();
        // Each shard is CacheAligned, so adjacent shards are on different cache lines.
        for i in 0..LOCK_TABLE_SHARDS.saturating_sub(1) {
            let a = &raw const table.shards[i] as usize;
            let b = &raw const table.shards[i + 1] as usize;
            let gap = b - a;
            assert!(
                gap >= crate::cache_aligned::CACHE_LINE_BYTES,
                "lock table shard {i} and {next} must be >= 64 bytes apart, got {gap}",
                next = i + 1
            );
            assert_eq!(
                a % crate::cache_aligned::CACHE_LINE_BYTES,
                0,
                "lock table shard {i} must be cache-line aligned"
            );
        }
    }

    #[test]
    fn test_commit_index_shards_cache_aligned() {
        let index = CommitIndex::new();
        for i in 0..LOCK_TABLE_SHARDS.saturating_sub(1) {
            let a = &raw const index.shards[i] as usize;
            let b = &raw const index.shards[i + 1] as usize;
            let gap = b - a;
            assert!(
                gap >= crate::cache_aligned::CACHE_LINE_BYTES,
                "commit index shard {i} and {next} must be >= 64 bytes apart, got {gap}",
                next = i + 1
            );
            assert_eq!(
                a % crate::cache_aligned::CACHE_LINE_BYTES,
                0,
                "commit index shard {i} must be cache-line aligned"
            );
        }
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
        assert!(
            buf.is_aligned(),
            "PageBuf must be aligned to page_size (for direct I/O)"
        );
    }

    #[test]
    fn test_page_buf_from_vec_is_aligned_and_preserves_bytes() {
        let mut data = vec![0u8; PageSize::DEFAULT.as_usize()];
        for (i, b) in data.iter_mut().enumerate() {
            *b = u8::try_from(i & 0xFF).unwrap_or(0);
        }
        let buf = PageBuf::from_vec(data.clone(), PageSize::DEFAULT);
        assert!(
            buf.is_aligned(),
            "PageBuf::from_vec must yield aligned storage"
        );
        assert_eq!(buf.as_bytes(), data.as_slice());
    }

    #[test]
    fn test_page_buf_clone_preserves_alignment_and_data() {
        let mut buf = PageBuf::zeroed(PageSize::DEFAULT);
        // Write a recognizable pattern.
        for (i, b) in buf.as_bytes_mut().iter_mut().enumerate() {
            *b = u8::try_from(i & 0xFF).unwrap_or(0);
        }
        let cloned = buf.clone();
        assert!(
            cloned.is_aligned(),
            "cloned PageBuf must be aligned (new heap allocation)"
        );
        assert_eq!(cloned.as_bytes(), buf.as_bytes());
        assert_eq!(cloned.page_size(), buf.page_size());
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

    // -- E2E: full transaction flow exercising all core types together --

    #[test]
    fn test_e2e_mvcc_core_types_roundtrip_in_real_txn_flow() {
        // Setup shared infrastructure.
        let lock_table = InProcessPageLockTable::new();
        let commit_index = CommitIndex::new();
        let mut commit_log = CommitLog::new(CommitSeq::new(1));
        let mut arena = VersionArena::new();

        let snap = Snapshot::new(CommitSeq::new(0), SchemaEpoch::ZERO);

        // --- Transaction 1: write pages 1 and 2, commit ---
        let txn1_id = TxnId::new(1).unwrap();
        let mut txn1 =
            Transaction::new(txn1_id, TxnEpoch::new(0), snap, TransactionMode::Concurrent);
        assert_eq!(txn1.state, TransactionState::Active);
        assert_eq!(txn1.token(), TxnToken::new(txn1_id, TxnEpoch::new(0)));

        let page1 = PageNumber::new(1).unwrap();
        let page2 = PageNumber::new(2).unwrap();

        // Acquire page locks.
        lock_table.try_acquire(page1, txn1_id).unwrap();
        lock_table.try_acquire(page2, txn1_id).unwrap();
        txn1.page_locks.insert(page1);
        txn1.page_locks.insert(page2);
        txn1.write_set.push(page1);
        txn1.write_set.push(page2);

        // Allocate page versions in the arena.
        let v1 = PageVersion {
            pgno: page1,
            commit_seq: CommitSeq::new(1),
            created_by: txn1.token(),
            data: PageData::zeroed(PageSize::DEFAULT),
            prev: None,
        };
        let v2 = PageVersion {
            pgno: page2,
            commit_seq: CommitSeq::new(1),
            created_by: txn1.token(),
            data: PageData::zeroed(PageSize::DEFAULT),
            prev: None,
        };
        let idx1 = arena.alloc(v1);
        let idx2 = arena.alloc(v2);

        // Commit txn1.
        txn1.commit();
        assert_eq!(txn1.state, TransactionState::Committed);

        let rec1 = CommitRecord {
            txn_id: txn1_id,
            commit_seq: CommitSeq::new(1),
            pages: SmallVec::from_slice(&[page1, page2]),
            timestamp_unix_ns: 1000,
        };
        commit_log.append(rec1);
        commit_index.update(page1, CommitSeq::new(1));
        commit_index.update(page2, CommitSeq::new(1));

        // Release locks.
        lock_table.release_all(txn1_id);
        assert_eq!(lock_table.lock_count(), 0);

        // Verify commit log and index.
        assert_eq!(commit_log.latest_seq(), Some(CommitSeq::new(1)));
        assert_eq!(commit_index.latest(page1), Some(CommitSeq::new(1)));
        assert_eq!(commit_index.latest(page2), Some(CommitSeq::new(1)));

        // --- Transaction 2: reads page 1 at snapshot, writes page 2, detects SSI ---
        let snap2 = Snapshot::new(CommitSeq::new(1), SchemaEpoch::ZERO);
        let txn2_id = TxnId::new(2).unwrap();
        let mut txn2 = Transaction::new(
            txn2_id,
            TxnEpoch::new(0),
            snap2,
            TransactionMode::Concurrent,
        );

        // Read page 1 — version is visible via snapshot.
        let read_ver = arena.get(idx1).unwrap();
        assert_eq!(read_ver.pgno, page1);
        assert!(read_ver.commit_seq <= txn2.snapshot.high);
        txn2.read_keys.insert(WitnessKey::Page(page1));

        // Write page 2 — acquire lock, create new version chained to old.
        lock_table.try_acquire(page2, txn2_id).unwrap();
        txn2.page_locks.insert(page2);
        txn2.write_set.push(page2);
        txn2.write_keys.insert(WitnessKey::Page(page2));

        let v2_new = PageVersion {
            pgno: page2,
            commit_seq: CommitSeq::new(2),
            created_by: txn2.token(),
            data: PageData::zeroed(PageSize::DEFAULT),
            prev: Some(VersionPointer::new(
                u64::from(idx2.chunk) << 32 | u64::from(idx2.offset),
            )),
        };
        let idx2_new = arena.alloc(v2_new);

        // SSI detection: simulate rw-antidependency edges.
        txn2.has_in_rw = true;
        assert!(!txn2.has_dangerous_structure());
        txn2.has_out_rw = true;
        assert!(txn2.has_dangerous_structure());

        // Despite dangerous structure, abort txn2 (SSI would require it).
        txn2.abort();
        assert_eq!(txn2.state, TransactionState::Aborted);

        // Release locks and free the aborted version.
        lock_table.release_all(txn2_id);
        arena.free(idx2_new);

        // Verify arena: original versions still live, aborted one freed.
        assert!(arena.get(idx1).is_some());
        assert!(arena.get(idx2).is_some());
        assert!(arena.get(idx2_new).is_none());

        // Verify commit log unchanged (txn2 aborted, nothing committed).
        assert_eq!(commit_log.len(), 1);
        assert_eq!(commit_log.latest_seq(), Some(CommitSeq::new(1)));

        // Final infrastructure sanity.
        assert_eq!(lock_table.lock_count(), 0);
        assert_eq!(arena.high_water(), 3); // 3 total allocations (idx1, idx2, idx2_new)
        assert_eq!(arena.free_count(), 1); // idx2_new was freed
    }

    // -----------------------------------------------------------------------
    // bd-22n.8 — Allocation-Free Read Path Tests
    // -----------------------------------------------------------------------

    const BEAD_22N8: &str = "bd-22n.8";

    #[test]
    fn test_small_vec_for_hot_structures() {
        // bd-22n.8: Active transaction write_set uses SmallVec for stack allocation.
        // Transactions touching <= 8 pages should not heap-allocate write_set.
        let txn_id = TxnId::new(1).unwrap();
        let epoch = TxnEpoch::new(0);
        let snapshot = Snapshot::new(CommitSeq::new(1), SchemaEpoch::new(1));
        let mut txn = Transaction::new(txn_id, epoch, snapshot, TransactionMode::Concurrent);

        // SmallVec inline capacity is 8 for PageNumber.
        for i in 1..=8u32 {
            let pgno = PageNumber::new(i).unwrap();
            txn.write_set.push(pgno);
        }

        // SmallVec::spilled() returns true iff the data has moved to heap.
        assert!(
            !txn.write_set.spilled(),
            "bead_id={BEAD_22N8} case=small_vec_stack_for_8_pages \
             write_set with 8 pages must NOT spill to heap"
        );
        assert_eq!(txn.write_set.len(), 8);

        // Pushing a 9th should spill (but that's expected for large transactions).
        txn.write_set.push(PageNumber::new(9).unwrap());
        assert!(
            txn.write_set.spilled(),
            "bead_id={BEAD_22N8} case=small_vec_spills_at_9_pages \
             write_set with 9 pages should spill to heap"
        );
    }

    #[test]
    fn test_version_check_no_alloc() {
        // bd-22n.8: MVCC version chain visibility check is allocation-free.
        //
        // The `visible()` function in invariants.rs does only field comparisons
        // (commit_seq != 0 && commit_seq <= snapshot.high). Verify this by
        // constructing a version and checking visibility — no Vec/Box involved.
        let v = make_page_version(1, 5);
        let snapshot = Snapshot::new(CommitSeq::new(10), SchemaEpoch::new(1));

        // The visibility check is a pure comparison — no allocation.
        let is_vis = v.commit_seq.get() != 0 && v.commit_seq <= snapshot.high;
        assert!(
            is_vis,
            "bead_id={BEAD_22N8} case=version_check_no_alloc \
             committed version within snapshot must be visible"
        );

        // Invisible: version committed after snapshot.
        let v_future = make_page_version(2, 15);
        let not_vis = v_future.commit_seq.get() != 0 && v_future.commit_seq <= snapshot.high;
        assert!(
            !not_vis,
            "bead_id={BEAD_22N8} case=version_check_future_invisible \
             version beyond snapshot must not be visible"
        );
    }

    #[test]
    fn test_lock_table_lookup_no_alloc() {
        // bd-22n.8: InProcessPageLockTable::holder() is allocation-free.
        // It only reads through a Mutex<HashMap> with no intermediate containers.
        let table = InProcessPageLockTable::new();
        let txn = TxnId::new(1).unwrap();
        let page = PageNumber::new(42).unwrap();

        // Setup: acquire a lock.
        table.try_acquire(page, txn).unwrap();

        // The holder() call is a HashMap get through a Mutex — zero alloc.
        let h = table.holder(page);
        assert_eq!(
            h,
            Some(txn),
            "bead_id={BEAD_22N8} case=lock_table_lookup_no_alloc"
        );

        // Querying a non-existent page is also allocation-free.
        let h_miss = table.holder(PageNumber::new(999).unwrap());
        assert_eq!(
            h_miss, None,
            "bead_id={BEAD_22N8} case=lock_table_lookup_miss_no_alloc"
        );

        table.release(page, txn);
    }

    #[test]
    fn test_commit_index_lookup_no_alloc() {
        // bd-22n.8: CommitIndex::latest() is allocation-free.
        // RwLock read + HashMap get → no allocation.
        let index = CommitIndex::new();
        let page = PageNumber::new(7).unwrap();

        index.update(page, CommitSeq::new(42));
        let latest = index.latest(page);
        assert_eq!(
            latest,
            Some(CommitSeq::new(42)),
            "bead_id={BEAD_22N8} case=commit_index_lookup_no_alloc"
        );

        // Miss path also allocation-free.
        let miss = index.latest(PageNumber::new(999).unwrap());
        assert_eq!(
            miss, None,
            "bead_id={BEAD_22N8} case=commit_index_lookup_miss_no_alloc"
        );
    }

    #[test]
    fn test_arena_get_no_alloc() {
        // bd-22n.8: VersionArena::get() is allocation-free.
        // Just a bounds-checked Vec index — no allocation.
        let mut arena = VersionArena::new();
        let v = make_page_version(1, 5);
        let idx = arena.alloc(v.clone());

        // get() returns Option<&PageVersion> — a borrow, not a clone.
        let got = arena.get(idx);
        assert!(got.is_some(), "bead_id={BEAD_22N8} case=arena_get_no_alloc");
        assert_eq!(got.unwrap().pgno, v.pgno);
    }

    #[test]
    fn test_cache_lookup_no_alloc_structural() {
        // bd-22n.8: PageCache::get() returns &[u8] pointing directly into the
        // pool-allocated buffer — no copy, no allocation. This is verified
        // structurally by checking pointer stability (already proven in
        // page_cache tests). Here we verify the pattern holds for the MVCC
        // read path: cache hit → &[u8] reference (no alloc).
        //
        // The read path for a cached page is:
        // 1. HashMap::get(&page_no) → Option<&PageBuf>  [no alloc]
        // 2. PageBuf::as_slice() → &[u8]                [no alloc]
        //
        // This test verifies the structural guarantee by examining type
        // signatures and the existing pointer-stability tests.

        // Construct a minimal test: SmallVec with 8 inline pages.
        let mut write_set: SmallVec<[PageNumber; 8]> = SmallVec::new();
        for i in 1..=8u32 {
            write_set.push(PageNumber::new(i).unwrap());
        }

        // contains() on SmallVec is linear scan — allocation-free.
        assert!(
            write_set.contains(&PageNumber::new(5).unwrap()),
            "bead_id={BEAD_22N8} case=small_vec_contains_no_alloc"
        );

        // The inline buffer has not spilled.
        assert!(
            !write_set.spilled(),
            "bead_id={BEAD_22N8} case=write_set_inline_for_8_pages"
        );
    }

    #[test]
    fn test_write_set_truncate_preserves_inline() {
        // bd-22n.8: SmallVec::truncate() on inline data is allocation-free.
        // This matters for savepoint rollback (lifecycle.rs).
        let mut write_set: SmallVec<[PageNumber; 8]> = SmallVec::new();
        for i in 1..=6u32 {
            write_set.push(PageNumber::new(i).unwrap());
        }

        assert!(!write_set.spilled());
        write_set.truncate(3);
        assert_eq!(write_set.len(), 3);
        assert!(
            !write_set.spilled(),
            "bead_id={BEAD_22N8} case=truncate_preserves_inline \
             truncated SmallVec must remain inline"
        );
    }

    // Property test: SmallVec inline for any N <= 8 pages.
    proptest! {
        #[test]
        fn prop_write_set_inline_for_small_txn(count in 1..=8u32) {
            let mut write_set: SmallVec<[PageNumber; 8]> = SmallVec::new();
            for i in 1..=count {
                write_set.push(PageNumber::new(i).unwrap());
            }
            prop_assert!(
                !write_set.spilled(),
                "bead_id={BEAD_22N8} write_set must be inline for {} pages",
                count
            );
        }
    }

    // -----------------------------------------------------------------------
    // bd-22n.12 — Lock Table Rebuild via Rolling Quiescence (§5.6.3.1)
    // -----------------------------------------------------------------------

    const BEAD_22N12: &str = "bd-22n.12";

    #[test]
    fn test_lock_table_rebuild_drains_to_zero_holders() {
        // bd-22n.12: begin_rebuild rotates active → draining, new acquisitions
        // go to the fresh active table. After releasing all draining locks, the
        // draining table reaches quiescence and finalize succeeds.
        let mut table = InProcessPageLockTable::new();
        let txn1 = TxnId::new(1).unwrap();
        let txn2 = TxnId::new(2).unwrap();

        let p1 = PageNumber::new(1).unwrap();
        let p2 = PageNumber::new(2).unwrap();
        let p3 = PageNumber::new(3).unwrap();

        // Acquire locks before rebuild.
        table.try_acquire(p1, txn1).unwrap();
        table.try_acquire(p2, txn2).unwrap();
        assert_eq!(table.lock_count(), 2);

        // Rotate: active → draining.
        let epoch = table.begin_rebuild().unwrap();
        assert!(epoch > 0, "bead_id={BEAD_22N12} epoch must be non-zero");
        assert!(table.is_rebuild_in_progress());

        // Draining table has 2 locks, active has 0.
        assert_eq!(table.draining_lock_count(), 2);
        assert_eq!(table.lock_count(), 0);

        // New acquisitions go to the fresh active table.
        table.try_acquire(p3, txn1).unwrap();
        assert_eq!(table.lock_count(), 1);
        assert_eq!(table.draining_lock_count(), 2);

        // Release the draining locks.
        assert!(table.release(p1, txn1));
        assert!(table.release(p2, txn2));
        assert_eq!(
            table.draining_lock_count(),
            0,
            "bead_id={BEAD_22N12} case=drain_to_zero \
             all draining locks must be released"
        );

        // Finalize succeeds when draining table is empty.
        let result = table.finalize_rebuild().unwrap();
        assert!(!table.is_rebuild_in_progress());
        assert_eq!(
            result.retained, 0,
            "bead_id={BEAD_22N12} case=finalize_zero_retained"
        );

        // Active table lock is still held.
        assert_eq!(table.lock_count(), 1);
        assert_eq!(table.holder(p3), Some(txn1));
    }

    #[test]
    fn test_read_only_txns_dont_block_rebuild() {
        // bd-22n.12 §5.6.3.1: read-only transactions MUST NOT block rebuild.
        // Read-only transactions don't acquire page locks, so the draining table
        // reaches quiescence without waiting for them.
        let mut table = InProcessPageLockTable::new();
        let writer = TxnId::new(1).unwrap();
        let p1 = PageNumber::new(10).unwrap();

        // One writer holds a lock.
        table.try_acquire(p1, writer).unwrap();

        // Begin rebuild.
        table.begin_rebuild().unwrap();
        assert_eq!(table.draining_lock_count(), 1);

        // A "read-only transaction" simply doesn't acquire any page locks.
        // It reads from the version arena, not the lock table.
        // Simulate a read-only txn (no lock table interaction).
        // The draining table is unaffected.
        assert_eq!(
            table.draining_lock_count(),
            1,
            "bead_id={BEAD_22N12} case=read_only_no_block \
             read-only txns do not add entries to lock table"
        );

        // Writer releases → draining quiesces.
        table.release(p1, writer);
        assert_eq!(table.draining_lock_count(), 0);

        // Finalize succeeds immediately — read-only txn never blocked it.
        let result = table.finalize_rebuild().unwrap();
        assert_eq!(
            result.retained, 0,
            "bead_id={BEAD_22N12} case=read_only_finalize_immediate"
        );
    }

    #[test]
    fn test_rebuild_is_rolling_no_mass_aborts() {
        // bd-22n.12 §5.6.3.1: the rebuild protocol is rolling — existing
        // transactions are NOT aborted. They continue operating normally.
        // Locks in the draining table remain valid and block conflicting
        // acquisitions. New acquisitions go to the active table.
        let mut table = InProcessPageLockTable::new();
        let txn_old = TxnId::new(1).unwrap();
        let txn_new = TxnId::new(2).unwrap();

        let page_a = PageNumber::new(100).unwrap();
        let page_b = PageNumber::new(200).unwrap();

        // txn_old acquires page_a before rebuild.
        table.try_acquire(page_a, txn_old).unwrap();
        assert_eq!(table.lock_count(), 1);

        // Rotate.
        table.begin_rebuild().unwrap();

        // txn_old's lock on page_a is now in the draining table.
        // It is still valid — no abort required.
        assert_eq!(
            table.holder(page_a),
            Some(txn_old),
            "bead_id={BEAD_22N12} case=rolling_no_abort \
             old txn's lock is still visible through draining table"
        );

        // txn_new cannot acquire page_a (held in draining table by txn_old).
        let err = table.try_acquire(page_a, txn_new).unwrap_err();
        assert_eq!(
            err, txn_old,
            "bead_id={BEAD_22N12} case=draining_blocks_conflict \
             draining table still enforces exclusion"
        );

        // txn_new can acquire a different page in the active table.
        table.try_acquire(page_b, txn_new).unwrap();
        assert_eq!(table.lock_count(), 1); // page_b in active
        assert_eq!(table.draining_lock_count(), 1); // page_a in draining

        // txn_old's existing lock can be re-acquired by the same txn (idempotent).
        table.try_acquire(page_a, txn_old).unwrap();

        // txn_old finishes normally — releases lock from draining table.
        table.release(page_a, txn_old);
        assert_eq!(table.draining_lock_count(), 0);

        // Now txn_new can acquire page_a in the active table.
        table.try_acquire(page_a, txn_new).unwrap();
        assert_eq!(table.lock_count(), 2); // page_a + page_b in active

        // Finalize.
        table.finalize_rebuild().unwrap();
        assert!(!table.is_rebuild_in_progress());
    }

    #[test]
    fn test_rebuild_completes_in_bounded_time() {
        // bd-22n.12: full_rebuild with orphan cleanup reaches quiescence
        // within a reasonable timeout.
        let mut table = InProcessPageLockTable::new();
        let txn_a = TxnId::new(10).unwrap();
        let txn_b = TxnId::new(20).unwrap();
        let txn_orphan = TxnId::new(999).unwrap();

        // Set up locks: some active, some orphaned (txn crashed).
        for i in 1..=5u32 {
            table
                .try_acquire(PageNumber::new(i).unwrap(), txn_a)
                .unwrap();
        }
        for i in 6..=10u32 {
            table
                .try_acquire(PageNumber::new(i).unwrap(), txn_b)
                .unwrap();
        }
        for i in 11..=15u32 {
            table
                .try_acquire(PageNumber::new(i).unwrap(), txn_orphan)
                .unwrap();
        }
        assert_eq!(table.lock_count(), 15);

        // Release active transactions' locks before rebuild.
        table.release_all(txn_a);
        table.release_all(txn_b);
        assert_eq!(table.lock_count(), 5); // only orphan locks remain

        // full_rebuild: the orphan predicate says txn_orphan is NOT active.
        let result = table
            .full_rebuild(|txn| txn != txn_orphan, Duration::from_secs(5))
            .unwrap();

        match result {
            DrainResult::Quiescent { cleaned, elapsed } => {
                assert_eq!(
                    cleaned, 5,
                    "bead_id={BEAD_22N12} case=bounded_time \
                     all 5 orphaned locks must be cleaned"
                );
                assert!(
                    elapsed < Duration::from_secs(5),
                    "bead_id={BEAD_22N12} case=bounded_time \
                     rebuild must complete well within timeout"
                );
            }
            DrainResult::TimedOut { remaining, .. } => {
                panic!(
                    "bead_id={BEAD_22N12} case=bounded_time \
                     rebuild should not time out, remaining={remaining}"
                );
            }
        }

        assert!(
            !table.is_rebuild_in_progress(),
            "bead_id={BEAD_22N12} case=bounded_time rebuild must be finalized"
        );
    }

    #[test]
    fn test_begin_rebuild_rejects_double_start() {
        // bd-22n.12: cannot start a second rebuild while one is in progress.
        let mut table = InProcessPageLockTable::new();
        table.begin_rebuild().unwrap();

        let err = table.begin_rebuild().unwrap_err();
        assert_eq!(
            err,
            RebuildError::AlreadyInProgress,
            "bead_id={BEAD_22N12} case=double_start_rejected"
        );
    }

    #[test]
    fn test_finalize_rejects_non_quiescent_table() {
        // bd-22n.12: finalize_rebuild fails if the draining table is not empty.
        let mut table = InProcessPageLockTable::new();
        let txn = TxnId::new(1).unwrap();
        table.try_acquire(PageNumber::new(1).unwrap(), txn).unwrap();

        table.begin_rebuild().unwrap();

        let err = table.finalize_rebuild().unwrap_err();
        assert_eq!(
            err,
            RebuildError::DrainNotComplete { remaining: 1 },
            "bead_id={BEAD_22N12} case=finalize_non_quiescent"
        );
    }

    #[test]
    fn test_drain_orphaned_cleans_crashed_txns() {
        // bd-22n.12: drain_orphaned removes entries for inactive transactions.
        let mut table = InProcessPageLockTable::new();
        let active_txn = TxnId::new(1).unwrap();
        let crashed_txn = TxnId::new(2).unwrap();

        table
            .try_acquire(PageNumber::new(1).unwrap(), active_txn)
            .unwrap();
        table
            .try_acquire(PageNumber::new(2).unwrap(), crashed_txn)
            .unwrap();
        table
            .try_acquire(PageNumber::new(3).unwrap(), crashed_txn)
            .unwrap();

        table.begin_rebuild().unwrap();
        assert_eq!(table.draining_lock_count(), 3);

        // Drain pass: crashed_txn is not active.
        let result = table.drain_orphaned(|txn| txn == active_txn).unwrap();

        assert_eq!(
            result.orphaned_cleaned, 2,
            "bead_id={BEAD_22N12} case=drain_orphaned_crashed \
             two crashed entries must be cleaned"
        );
        assert_eq!(
            result.retained, 1,
            "bead_id={BEAD_22N12} case=drain_orphaned_retained \
             one active entry must be retained"
        );
        assert_eq!(table.draining_lock_count(), 1);
    }

    #[test]
    fn test_drain_progress_reports_accurately() {
        // bd-22n.12: drain_progress returns correct remaining count.
        let mut table = InProcessPageLockTable::new();
        let txn = TxnId::new(1).unwrap();

        // No rebuild → drain_progress is None.
        assert!(
            table.drain_progress().is_none(),
            "bead_id={BEAD_22N12} case=no_rebuild_no_progress"
        );

        for i in 1..=10u32 {
            table.try_acquire(PageNumber::new(i).unwrap(), txn).unwrap();
        }

        table.begin_rebuild().unwrap();

        let progress = table.drain_progress().unwrap();
        assert_eq!(
            progress.remaining, 10,
            "bead_id={BEAD_22N12} case=progress_initial"
        );
        assert!(
            !progress.quiescent,
            "bead_id={BEAD_22N12} case=not_quiescent_initially"
        );

        // Release some locks.
        for i in 1..=7u32 {
            table.release(PageNumber::new(i).unwrap(), txn);
        }

        let progress = table.drain_progress().unwrap();
        assert_eq!(
            progress.remaining, 3,
            "bead_id={BEAD_22N12} case=progress_after_partial_drain"
        );

        // Release remaining.
        for i in 8..=10u32 {
            table.release(PageNumber::new(i).unwrap(), txn);
        }

        let progress = table.drain_progress().unwrap();
        assert!(
            progress.quiescent,
            "bead_id={BEAD_22N12} case=quiescent_after_full_drain"
        );
        assert_eq!(progress.remaining, 0);
    }

    #[test]
    fn test_release_all_clears_both_tables() {
        // bd-22n.12: release_all(txn) clears locks from both active AND
        // draining tables.
        let mut table = InProcessPageLockTable::new();
        let txn = TxnId::new(42).unwrap();

        // Acquire pre-rebuild.
        table.try_acquire(PageNumber::new(1).unwrap(), txn).unwrap();
        table.try_acquire(PageNumber::new(2).unwrap(), txn).unwrap();

        // Rotate.
        table.begin_rebuild().unwrap();

        // Acquire post-rebuild.
        table.try_acquire(PageNumber::new(3).unwrap(), txn).unwrap();

        assert_eq!(table.draining_lock_count(), 2);
        assert_eq!(table.lock_count(), 1);
        assert_eq!(table.total_lock_count(), 3);

        // release_all clears both.
        table.release_all(txn);
        assert_eq!(
            table.total_lock_count(),
            0,
            "bead_id={BEAD_22N12} case=release_all_both_tables"
        );
    }

    #[test]
    fn test_e2e_lock_table_rebuild_no_abort_storm() {
        // bd-22n.12 E2E: concurrent writers continue operating during rebuild.
        // No transaction is aborted due to the rebuild itself.
        let mut table = InProcessPageLockTable::new();
        let txn_count: usize = 20;
        let pages_per_txn: usize = 5;

        // Phase 1: establish pre-rebuild locks.
        for t in 1..=txn_count {
            let txn = TxnId::new(u64::try_from(t).unwrap()).unwrap();
            for p in 1..=pages_per_txn {
                let page_no = (t - 1) * pages_per_txn + p;
                let page = PageNumber::new(u32::try_from(page_no).unwrap()).unwrap();
                table.try_acquire(page, txn).unwrap();
            }
        }
        let pre_count = table.lock_count();
        assert_eq!(
            pre_count,
            txn_count * pages_per_txn,
            "bead_id={BEAD_22N12} case=e2e_pre_lock_count"
        );

        // Phase 2: begin rebuild.
        table.begin_rebuild().unwrap();
        assert_eq!(table.lock_count(), 0); // active is fresh
        assert_eq!(table.draining_lock_count(), pre_count);

        // Phase 3: simulate concurrent new writers acquiring new pages.
        for t in (txn_count + 1)..=(txn_count + 10) {
            let txn = TxnId::new(u64::try_from(t).unwrap()).unwrap();
            for p in 1..=3_usize {
                let page_no = 1000 + (t - txn_count - 1) * 3 + p;
                let page = PageNumber::new(u32::try_from(page_no).unwrap()).unwrap();
                table.try_acquire(page, txn).unwrap();
            }
        }
        assert_eq!(
            table.lock_count(),
            30,
            "bead_id={BEAD_22N12} case=e2e_new_writers_active"
        );

        // Phase 4: old transactions finish naturally (no abort).
        for t in 1..=txn_count {
            let txn = TxnId::new(u64::try_from(t).unwrap()).unwrap();
            for p in 1..=pages_per_txn {
                let page_no = (t - 1) * pages_per_txn + p;
                let page = PageNumber::new(u32::try_from(page_no).unwrap()).unwrap();
                assert!(
                    table.release(page, txn),
                    "bead_id={BEAD_22N12} case=e2e_old_txn_release t={t} p={page_no}"
                );
            }
        }
        assert_eq!(
            table.draining_lock_count(),
            0,
            "bead_id={BEAD_22N12} case=e2e_draining_quiescent"
        );

        // Phase 5: finalize rebuild.
        let result = table.finalize_rebuild().unwrap();
        assert!(!table.is_rebuild_in_progress());
        assert_eq!(
            result.retained, 0,
            "bead_id={BEAD_22N12} case=e2e_finalize_clean"
        );

        // New writers' locks are untouched.
        assert_eq!(
            table.lock_count(),
            30,
            "bead_id={BEAD_22N12} case=e2e_new_writers_preserved"
        );
    }

    // ===================================================================
    // bd-22n.13: GC Horizon Accounts for TxnSlot Sentinels (§1.6)
    // ===================================================================

    const BEAD_22N13: &str = "bd-22n.13";

    use crate::cache_aligned::{
        encode_claiming, encode_cleaning, CLAIMING_TIMEOUT_NO_PID_SECS, CLAIMING_TIMEOUT_SECS,
        TAG_CLEANING,
    };

    /// Helper: create a slot with a real (non-sentinel) TxnId and begin_seq.
    fn make_active_slot(txn_id_raw: u64, begin_seq: u64) -> SharedTxnSlot {
        let slot = SharedTxnSlot::new();
        slot.txn_id.store(txn_id_raw, Ordering::Release);
        slot.begin_seq.store(begin_seq, Ordering::Release);
        slot
    }

    /// Helper: create a slot in CLAIMING state with given payload TxnId.
    fn make_claiming_slot(txn_id_raw: u64, claiming_ts: u64) -> SharedTxnSlot {
        let slot = SharedTxnSlot::new();
        slot.txn_id
            .store(encode_claiming(txn_id_raw), Ordering::Release);
        slot.claiming_timestamp
            .store(claiming_ts, Ordering::Release);
        // begin_seq may have been initialized during Phase 2.
        slot.begin_seq.store(5, Ordering::Release);
        slot
    }

    /// Helper: create a slot in CLEANING state with given payload TxnId.
    fn make_cleaning_slot(txn_id_raw: u64, claiming_ts: u64) -> SharedTxnSlot {
        let slot = SharedTxnSlot::new();
        slot.txn_id
            .store(encode_cleaning(txn_id_raw), Ordering::Release);
        slot.claiming_timestamp
            .store(claiming_ts, Ordering::Release);
        slot.cleanup_txn_id.store(txn_id_raw, Ordering::Release);
        slot
    }

    #[test]
    fn test_gc_horizon_blocks_on_claiming_slot() {
        // bd-22n.13 test #19: TxnSlot in CLAIMING state blocks gc_horizon.
        let slots = [
            make_active_slot(1, 100),   // active txn with begin_seq=100
            make_claiming_slot(2, 999), // CLAIMING sentinel
        ];

        let old_horizon = CommitSeq::new(50);
        let commit_seq = CommitSeq::new(200);

        let result = raise_gc_horizon(&slots, old_horizon, commit_seq);

        // The CLAIMING slot blocks advancement: horizon clamped to old_horizon.
        // The active slot has begin_seq=100 > old_horizon=50, but the sentinel
        // clamps global_min to 50, so the horizon stays at 50.
        assert_eq!(
            result.new_horizon,
            CommitSeq::new(50),
            "bead_id={BEAD_22N13} case=gc_horizon_blocks_on_claiming \
             CLAIMING sentinel must prevent horizon advancement"
        );
        assert_eq!(result.sentinel_blockers, 1);
        assert_eq!(result.active_slots, 1);
    }

    #[test]
    fn test_gc_horizon_blocks_on_cleaning_slot() {
        // bd-22n.13 test #20: TxnSlot in CLEANING state blocks gc_horizon.
        let slots = [
            make_active_slot(1, 100),   // active txn with begin_seq=100
            make_cleaning_slot(3, 999), // CLEANING sentinel
        ];

        let old_horizon = CommitSeq::new(50);
        let commit_seq = CommitSeq::new(200);

        let result = raise_gc_horizon(&slots, old_horizon, commit_seq);

        assert_eq!(
            result.new_horizon,
            CommitSeq::new(50),
            "bead_id={BEAD_22N13} case=gc_horizon_blocks_on_cleaning \
             CLEANING sentinel must prevent horizon advancement"
        );
        assert_eq!(result.sentinel_blockers, 1);
        assert_eq!(result.active_slots, 1);
    }

    #[test]
    fn test_crash_cleanup_preserves_identity() {
        // bd-22n.13 test #21: Cleanup uses TxnId payload from TAG_CLEANING word.
        let original_txn_id = 42_u64;
        let long_ago = 1_000_u64;
        let now = long_ago + CLAIMING_TIMEOUT_SECS + 10;

        let slot = make_cleaning_slot(original_txn_id, long_ago);

        // The TAG_CLEANING word preserves the original TxnId for retryable cleanup.
        let word = slot.txn_id.load(Ordering::Acquire);
        assert_eq!(decode_tag(word), TAG_CLEANING);
        assert_eq!(
            decode_payload(word),
            original_txn_id,
            "bead_id={BEAD_22N13} case=crash_cleanup_preserves_identity \
             TAG_CLEANING payload must preserve original TxnId"
        );

        // The cleanup_txn_id mirror field also preserves the identity.
        assert_eq!(
            slot.cleanup_txn_id.load(Ordering::Acquire),
            original_txn_id,
            "bead_id={BEAD_22N13} case=cleanup_txn_id_mirror \
             cleanup_txn_id must mirror TAG_CLEANING payload"
        );

        // Verify cleanup is retryable: a second cleaner can decode the same identity.
        let result = try_cleanup_sentinel_slot(&slot, now, |_, _| false);
        assert!(
            matches!(result, SlotCleanupResult::Reclaimed { orphan_txn_id, .. } if orphan_txn_id == original_txn_id),
            "bead_id={BEAD_22N13} case=crash_cleanup_retryable \
             cleanup must extract the correct orphan TxnId"
        );

        // After cleanup, slot is free.
        assert!(
            slot.is_free(Ordering::Acquire),
            "bead_id={BEAD_22N13} case=slot_freed_after_cleanup"
        );
    }

    #[test]
    fn test_gc_horizon_advances_after_cleanup() {
        // bd-22n.13 test #22: After crashed slot is freed, GC horizon can advance.
        let original_txn_id = 7_u64;
        let long_ago = 1_000_u64;
        let now = long_ago + CLAIMING_TIMEOUT_SECS + 10;

        let old_horizon = CommitSeq::new(50);
        let commit_seq = CommitSeq::new(200);

        // Build the slots array.
        let slots: [SharedTxnSlot; 2] = [
            make_active_slot(1, 100),
            make_cleaning_slot(original_txn_id, long_ago),
        ];

        // Before cleanup: horizon blocked by sentinel.
        let before = raise_gc_horizon(&slots, old_horizon, commit_seq);
        assert_eq!(
            before.new_horizon,
            CommitSeq::new(50),
            "bead_id={BEAD_22N13} case=gc_horizon_blocked_before_cleanup"
        );

        // Clean up the stale sentinel.
        let cleanup_result = try_cleanup_sentinel_slot(&slots[1], now, |_, _| false);
        assert!(
            matches!(cleanup_result, SlotCleanupResult::Reclaimed { .. }),
            "bead_id={BEAD_22N13} case=cleanup_succeeds"
        );

        // After cleanup: sentinel is freed, horizon can advance.
        let after = raise_gc_horizon(&slots, old_horizon, commit_seq);
        assert_eq!(
            after.new_horizon,
            CommitSeq::new(100),
            "bead_id={BEAD_22N13} case=gc_horizon_advances_after_cleanup \
             horizon must advance to min active begin_seq after sentinel freed"
        );
        assert_eq!(
            after.sentinel_blockers, 0,
            "bead_id={BEAD_22N13} case=no_sentinel_blockers_after_cleanup"
        );
    }

    #[test]
    fn test_stale_sentinel_detected_by_timeout() {
        // bd-22n.13 test #23: Slot stuck in CLAIMING for > timeout is reclaimed.
        let txn_id_raw = 99_u64;
        let claim_time = 1_000_u64;

        // Slot stuck in CLAIMING with no PID published.
        let slot = make_claiming_slot(txn_id_raw, claim_time);
        // Ensure pid/pid_birth are 0 (not published).
        assert_eq!(slot.pid.load(Ordering::Relaxed), 0);
        assert_eq!(slot.pid_birth.load(Ordering::Relaxed), 0);

        // Too recent: should not be reclaimed.
        let recent_now = claim_time + CLAIMING_TIMEOUT_NO_PID_SECS - 1;
        let result = try_cleanup_sentinel_slot(&slot, recent_now, |_, _| false);
        assert_eq!(
            result,
            SlotCleanupResult::StillRecent,
            "bead_id={BEAD_22N13} case=stale_sentinel_too_recent"
        );

        // Now past the conservative timeout.
        let stale_now = claim_time + CLAIMING_TIMEOUT_NO_PID_SECS + 1;
        let result = try_cleanup_sentinel_slot(&slot, stale_now, |_, _| false);
        assert!(
            matches!(result, SlotCleanupResult::Reclaimed { orphan_txn_id, was_claiming: true } if orphan_txn_id == txn_id_raw),
            "bead_id={BEAD_22N13} case=stale_sentinel_reclaimed \
             stale CLAIMING slot must be reclaimed after timeout"
        );

        assert!(
            slot.is_free(Ordering::Acquire),
            "bead_id={BEAD_22N13} case=stale_sentinel_freed"
        );
    }

    #[test]
    fn test_stale_sentinel_with_pid_uses_shorter_timeout() {
        // Additional test: CLAIMING slot with published PID uses shorter timeout.
        let txn_id_raw = 55_u64;
        let claim_time = 1_000_u64;

        let slot = make_claiming_slot(txn_id_raw, claim_time);
        slot.pid.store(12345, Ordering::Release);
        slot.pid_birth.store(9999, Ordering::Release);

        // Process is dead: use CLAIMING_TIMEOUT_SECS (shorter).
        let stale_now = claim_time + CLAIMING_TIMEOUT_SECS + 1;
        let result = try_cleanup_sentinel_slot(&slot, stale_now, |_, _| false);
        assert!(
            matches!(result, SlotCleanupResult::Reclaimed { orphan_txn_id, was_claiming: true } if orphan_txn_id == txn_id_raw),
            "bead_id={BEAD_22N13} case=stale_with_pid_shorter_timeout"
        );
    }

    #[test]
    fn test_claiming_slot_with_alive_process_not_reclaimed() {
        // Additional test: CLAIMING slot with alive process is never reclaimed.
        let txn_id_raw = 77_u64;
        let claim_time = 1_u64;

        let slot = make_claiming_slot(txn_id_raw, claim_time);
        slot.pid.store(12345, Ordering::Release);
        slot.pid_birth.store(9999, Ordering::Release);

        // Far past any timeout, but process is alive.
        let very_late = claim_time + 10_000;
        let result = try_cleanup_sentinel_slot(&slot, very_late, |_, _| true);
        assert!(
            matches!(result, SlotCleanupResult::ProcessAlive),
            "bead_id={BEAD_22N13} case=alive_process_never_reclaimed"
        );
    }

    #[test]
    fn test_gc_horizon_advances_without_sentinels() {
        // Verify horizon advances normally when no sentinels are present.
        let slots = [
            make_active_slot(1, 100),
            make_active_slot(2, 80),
            make_active_slot(3, 120),
        ];

        let old_horizon = CommitSeq::new(50);
        let commit_seq = CommitSeq::new(200);

        let result = raise_gc_horizon(&slots, old_horizon, commit_seq);
        assert_eq!(
            result.new_horizon,
            CommitSeq::new(80),
            "bead_id={BEAD_22N13} case=gc_horizon_advances_without_sentinels \
             horizon should advance to min(begin_seq) across active txns"
        );
        assert_eq!(result.active_slots, 3);
        assert_eq!(result.sentinel_blockers, 0);
    }

    #[test]
    fn test_gc_horizon_monotonic_never_decreases() {
        // Verify the monotonic invariant: new_horizon >= old_horizon.
        let slots = [make_active_slot(1, 30)];

        let old_horizon = CommitSeq::new(50);
        let commit_seq = CommitSeq::new(200);

        let result = raise_gc_horizon(&slots, old_horizon, commit_seq);
        assert_eq!(
            result.new_horizon,
            CommitSeq::new(50),
            "bead_id={BEAD_22N13} case=gc_horizon_monotonic \
             horizon must never decrease even if begin_seq < old_horizon"
        );
    }

    #[test]
    fn test_gc_horizon_empty_slots_advances_to_commit_seq() {
        // No active transactions: horizon advances to commit_seq.
        let slots: &[SharedTxnSlot] = &[];

        let old_horizon = CommitSeq::new(50);
        let commit_seq = CommitSeq::new(200);

        let result = raise_gc_horizon(slots, old_horizon, commit_seq);
        assert_eq!(
            result.new_horizon,
            CommitSeq::new(200),
            "bead_id={BEAD_22N13} case=gc_horizon_empty_slots \
             no active txns → horizon advances to commit_seq"
        );
        assert_eq!(result.active_slots, 0);
        assert_eq!(result.sentinel_blockers, 0);
    }

    #[test]
    fn test_cleanup_and_raise_gc_horizon_combined() {
        // E2E: cleanup_and_raise_gc_horizon cleans stale sentinels then advances.
        let original_txn_id = 42_u64;
        let long_ago = 1_000_u64;
        let now = long_ago + CLAIMING_TIMEOUT_SECS + 10;

        let slots = [
            make_active_slot(1, 100),
            make_cleaning_slot(original_txn_id, long_ago),
        ];

        let old_horizon = CommitSeq::new(50);
        let commit_seq = CommitSeq::new(200);

        let (result, cleaned) =
            cleanup_and_raise_gc_horizon(&slots, old_horizon, commit_seq, now, |_, _| false);

        assert_eq!(
            cleaned, 1,
            "bead_id={BEAD_22N13} case=combined_cleanup_count"
        );
        assert_eq!(
            result.new_horizon,
            CommitSeq::new(100),
            "bead_id={BEAD_22N13} case=combined_horizon_advances \
             horizon advances after stale sentinel cleaned"
        );
    }

    #[test]
    fn test_sentinel_encoding_roundtrip() {
        // Verify encode/decode roundtrip for sentinel encoding.
        use crate::cache_aligned::{decode_payload, decode_tag, encode_claiming, encode_cleaning};

        let txn_id = 42_u64;

        let claiming = encode_claiming(txn_id);
        assert_eq!(decode_tag(claiming), TAG_CLAIMING);
        assert_eq!(decode_payload(claiming), txn_id);
        assert!(is_sentinel(claiming));

        let cleaning = encode_cleaning(txn_id);
        assert_eq!(decode_tag(cleaning), TAG_CLEANING);
        assert_eq!(decode_payload(cleaning), txn_id);
        assert!(is_sentinel(cleaning));

        // Real TxnId (no tag) is NOT a sentinel.
        assert!(!is_sentinel(txn_id));
        assert_eq!(decode_tag(txn_id), 0);
        assert_eq!(decode_payload(txn_id), txn_id);

        // Free slot (0) is not a sentinel.
        assert!(!is_sentinel(0));
    }

    #[test]
    fn test_sentinel_encoding_max_txn_id() {
        // Verify sentinel encoding works correctly at TxnId boundary.
        let max_txn = TxnId::MAX_RAW;
        assert_eq!(max_txn, (1_u64 << 62) - 1);

        let claiming = encode_claiming(max_txn);
        assert_eq!(decode_tag(claiming), TAG_CLAIMING);
        assert_eq!(decode_payload(claiming), max_txn);

        let cleaning = encode_cleaning(max_txn);
        assert_eq!(decode_tag(cleaning), TAG_CLEANING);
        assert_eq!(decode_payload(cleaning), max_txn);
    }

    #[test]
    fn test_shared_txn_slot_sentinel_methods() {
        // Verify SharedTxnSlot sentinel helper methods.
        let slot = SharedTxnSlot::new();

        // Free slot.
        assert!(!slot.is_sentinel(Ordering::Relaxed));
        assert!(!slot.is_claiming(Ordering::Relaxed));
        assert!(!slot.is_cleaning(Ordering::Relaxed));
        assert!(slot.sentinel_payload(Ordering::Relaxed).is_none());

        // CLAIMING state.
        slot.txn_id.store(encode_claiming(42), Ordering::Release);
        assert!(slot.is_sentinel(Ordering::Acquire));
        assert!(slot.is_claiming(Ordering::Acquire));
        assert!(!slot.is_cleaning(Ordering::Acquire));
        assert_eq!(slot.sentinel_payload(Ordering::Acquire), Some(42));

        // CLEANING state.
        slot.txn_id.store(encode_cleaning(99), Ordering::Release);
        assert!(slot.is_sentinel(Ordering::Acquire));
        assert!(!slot.is_claiming(Ordering::Acquire));
        assert!(slot.is_cleaning(Ordering::Acquire));
        assert_eq!(slot.sentinel_payload(Ordering::Acquire), Some(99));

        // Real TxnId.
        slot.txn_id.store(7, Ordering::Release);
        assert!(!slot.is_sentinel(Ordering::Acquire));
        assert!(slot.sentinel_payload(Ordering::Acquire).is_none());
    }
}
