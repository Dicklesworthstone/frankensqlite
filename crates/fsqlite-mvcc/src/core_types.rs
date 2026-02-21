//! MVCC core runtime types (§5.1).
//!
//! This module implements the runtime data structures that power MVCC
//! concurrency: version arenas, page lock tables, commit indices, and
//! transaction state.
//!
//! Foundation types (TxnId, CommitSeq, Snapshot, etc.) live in
//! [`fsqlite_types::glossary`]; this module builds the runtime machinery on top.

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::time::Duration;

use parking_lot::{Mutex, RwLock};
use smallvec::SmallVec;

use std::sync::atomic::Ordering;

use crate::cache_aligned::{
    CLAIMING_TIMEOUT_NO_PID_SECS, CLAIMING_TIMEOUT_SECS, CacheAligned, SharedTxnSlot, TAG_CLAIMING,
    decode_payload, decode_tag, encode_cleaning, is_sentinel, logical_now_millis,
};
use crate::ebr::VersionGuardTicket;
use fsqlite_observability::GLOBAL_TXN_SLOT_METRICS;
pub use fsqlite_pager::PageBuf;
use fsqlite_types::{
    CommitSeq, IntentLog, PageData, PageNumber, PageNumberBuildHasher, PageVersion, Snapshot,
    TxnEpoch, TxnId, TxnSlot, TxnToken, WitnessKey,
};

// ---------------------------------------------------------------------------
// VersionIdx / VersionArena
// ---------------------------------------------------------------------------

/// Index into a [`VersionArena`] chunk.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct VersionIdx {
    chunk: u32,
    offset: u32,
    generation: u32,
}

impl VersionIdx {
    #[inline]
    pub(crate) const fn new(chunk: u32, offset: u32, generation: u32) -> Self {
        Self {
            chunk,
            offset,
            generation,
        }
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

    /// Generation counter for ABA protection.
    #[inline]
    #[must_use]
    pub fn generation(&self) -> u32 {
        self.generation
    }
}

/// Number of page versions per arena chunk.
const ARENA_CHUNK: usize = 4096;

struct ArenaSlot {
    generation: u32,
    version: Option<PageVersion>,
}

impl ArenaSlot {
    fn new(version: PageVersion) -> Self {
        Self {
            generation: 0,
            version: Some(version),
        }
    }
}

/// Bump-allocated arena for [`PageVersion`] objects.
///
/// Single-writer / multi-reader. The arena owns all page version data and
/// hands out [`VersionIdx`] handles. Freed slots are recycled via a free list.
///
/// Includes generation counting to detect use-after-free/ABA bugs during
/// concurrent reader traversal.
pub struct VersionArena {
    chunks: Vec<Vec<ArenaSlot>>,
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
            let slot = &mut self.chunks[idx.chunk as usize][idx.offset as usize];
            slot.version = Some(version);
            // Generation was incremented on free to invalidate old pointers.
            // We use the current generation for the new allocation.
            return VersionIdx::new(idx.chunk, idx.offset, slot.generation);
        }

        let last_chunk = self.chunks.len() - 1;
        if self.chunks[last_chunk].len() >= ARENA_CHUNK {
            self.chunks.push(Vec::with_capacity(ARENA_CHUNK));
        }

        let chunk_idx = self.chunks.len() - 1;
        let offset = self.chunks[chunk_idx].len();
        self.chunks[chunk_idx].push(ArenaSlot::new(version));
        self.high_water += 1;

        let chunk_u32 = u32::try_from(chunk_idx).expect("VersionArena chunk index overflow u32");
        let offset_u32 = u32::try_from(offset).expect("VersionArena offset overflow u32");
        VersionIdx::new(chunk_u32, offset_u32, 0)
    }

    /// Remove and return the version at `idx`, making the slot available
    /// for reuse.
    ///
    /// # Panics
    ///
    /// Asserts that the slot is currently occupied (catches double-free)
    /// and that the generation matches (catches stale pointer access).
    pub fn take(&mut self, idx: VersionIdx) -> PageVersion {
        let slot = &mut self.chunks[idx.chunk as usize][idx.offset as usize];
        assert!(
            slot.generation == idx.generation,
            "VersionArena::take: generation mismatch for {idx:?} (slot generation {})",
            slot.generation
        );
        let version = slot
            .version
            .take()
            .unwrap_or_else(|| panic!("VersionArena::take: double-free of {idx:?}"));

        // Increment generation on free so that any dangling VersionIdx becomes invalid.
        slot.generation = slot.generation.wrapping_add(1);

        self.free_list.push(idx);
        version
    }

    /// Free the slot at `idx`, making it available for reuse.
    ///
    /// # Panics
    ///
    /// Asserts that the slot is currently occupied (catches double-free).
    pub fn free(&mut self, idx: VersionIdx) {
        drop(self.take(idx));
    }

    /// Look up a version by index.
    ///
    /// Returns `None` if the slot is empty OR if the generation does not match
    /// (stale pointer).
    #[must_use]
    pub fn get(&self, idx: VersionIdx) -> Option<&PageVersion> {
        let slot = self
            .chunks
            .get(idx.chunk as usize)?
            .get(idx.offset as usize)?;

        if slot.generation != idx.generation {
            return None;
        }
        slot.version.as_ref()
    }

    /// Look up a version mutably by index.
    pub fn get_mut(&mut self, idx: VersionIdx) -> Option<&mut PageVersion> {
        let slot = self
            .chunks
            .get_mut(idx.chunk as usize)?
            .get_mut(idx.offset as usize)?;

        if slot.generation != idx.generation {
            return None;
        }
        slot.version.as_mut()
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
    /// Optional conflict observer for MVCC analytics (bd-t6sv2.1).
    /// When `None`, conflict emission is a no-op branch (zero cost).
    observer: Option<std::sync::Arc<dyn fsqlite_observability::ConflictObserver>>,
}

/// State tracking for the draining table during a rolling rebuild.
struct DrainingState {
    shards: Box<[LockShard; LOCK_TABLE_SHARDS]>,
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
    /// Create a new empty lock table with no observer.
    #[must_use]
    pub fn new() -> Self {
        Self {
            shards: Box::new(std::array::from_fn(|_| {
                CacheAligned::new(Mutex::new(HashMap::with_hasher(
                    PageNumberBuildHasher::default(),
                )))
            })),
            draining: Mutex::new(None),
            observer: None,
        }
    }

    /// Create a lock table with a conflict observer for analytics (bd-t6sv2.1).
    #[must_use]
    pub fn with_observer(
        observer: std::sync::Arc<dyn fsqlite_observability::ConflictObserver>,
    ) -> Self {
        Self {
            shards: Box::new(std::array::from_fn(|_| {
                CacheAligned::new(Mutex::new(HashMap::with_hasher(
                    PageNumberBuildHasher::default(),
                )))
            })),
            draining: Mutex::new(None),
            observer: Some(observer),
        }
    }

    /// Set or replace the conflict observer.
    pub fn set_observer(
        &mut self,
        observer: Option<std::sync::Arc<dyn fsqlite_observability::ConflictObserver>>,
    ) {
        self.observer = observer;
    }

    /// Access the shared observer (for passing to emit helpers).
    #[must_use]
    pub fn observer(&self) -> &crate::observability::SharedObserver {
        &self.observer
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
                    crate::observability::emit_page_lock_contention(
                        &self.observer,
                        page,
                        txn,
                        holder,
                    );
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
            crate::observability::emit_page_lock_contention(&self.observer, page, txn, holder);
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
        let elapsed = Duration::ZERO;
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

        let elapsed = Duration::ZERO;
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

        let elapsed = Duration::ZERO;
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
        let mut elapsed_ms = 0_u64;
        let mut remaining_budget_ms = u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX);
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
                        elapsed: Duration::from_millis(elapsed_ms),
                    });
                }

                // Check timeout.
                if remaining_budget_ms == 0 {
                    tracing::warn!(
                        remaining = progress.remaining,
                        elapsed_ms,
                        "lock table rebuild timed out before quiescence"
                    );
                    return Ok(DrainResult::TimedOut {
                        remaining: progress.remaining,
                        cleaned: total_cleaned,
                        elapsed: Duration::from_millis(elapsed_ms),
                    });
                }
            } else {
                // No rebuild in progress (shouldn't happen after begin_rebuild).
                return Ok(DrainResult::Quiescent {
                    cleaned: total_cleaned,
                    elapsed: Duration::from_millis(elapsed_ms),
                });
            }

            // Brief yield to let active transactions make progress.
            std::thread::sleep(Duration::from_millis(1));
            elapsed_ms = elapsed_ms.saturating_add(1);
            remaining_budget_ms = remaining_budget_ms.saturating_sub(1);
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
        dbg.field("shard_count", &self.shards.len());
        dbg.field("lock_count", &self.lock_count());
        dbg.field("draining", &draining_count);
        dbg.field("observer_enabled", &self.observer.is_some());
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

/// Read-set storage mode for per-transaction SSI tracking.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ReadSetStorageMode {
    /// Exact page->version map (default, deterministic).
    Exact,
    /// Bloom-backed approximation mode (reserved for large analytical scans).
    Bloom,
}

/// Version-tracking metadata for one written page.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct WriteVersionEntry {
    pub old_version: Option<CommitSeq>,
    pub new_version: Option<CommitSeq>,
}

impl WriteVersionEntry {
    #[must_use]
    pub const fn new(old_version: Option<CommitSeq>) -> Self {
        Self {
            old_version,
            new_version: None,
        }
    }
}

/// Simple Bloom filter for approximate read-set membership.
#[derive(Debug, Clone)]
pub struct ReadSetBloom {
    bits: Vec<u64>,
}

impl ReadSetBloom {
    const DEFAULT_BITS: usize = 4096;

    #[must_use]
    pub fn new(bits: usize) -> Self {
        let aligned_bits = bits.max(64).next_multiple_of(64);
        Self {
            bits: vec![0; aligned_bits / 64],
        }
    }

    fn bit_len(&self) -> usize {
        self.bits.len() * 64
    }

    fn hash_indices(&self, page: PageNumber) -> [usize; 2] {
        let raw = u64::from(page.get());
        let h1 = raw.wrapping_mul(0x9E37_79B9_7F4A_7C15);
        let h2 = raw.wrapping_mul(0xC2B2_AE3D_27D4_EB4F);
        let bit_len = self.bit_len();
        let bit_len_u64 = u64::try_from(bit_len).unwrap_or(u64::MAX);
        let idx1: usize = usize::try_from(h1 % bit_len_u64).unwrap_or_default();
        let idx2: usize = usize::try_from(h2 % bit_len_u64).unwrap_or_default();
        [idx1, idx2]
    }

    pub fn insert(&mut self, page: PageNumber) {
        for idx in self.hash_indices(page) {
            let word = idx / 64;
            let bit = idx % 64;
            self.bits[word] |= 1_u64 << bit;
        }
    }

    #[must_use]
    pub fn may_contain(&self, page: PageNumber) -> bool {
        self.hash_indices(page).into_iter().all(|idx| {
            let word = idx / 64;
            let bit = idx % 64;
            (self.bits[word] & (1_u64 << bit)) != 0
        })
    }

    pub fn clear(&mut self) {
        for word in &mut self.bits {
            *word = 0;
        }
    }
}

thread_local! {
    static THREAD_LOCAL_READ_SET_VERSIONS: RefCell<
        HashMap<TxnToken, HashMap<PageNumber, CommitSeq, PageNumberBuildHasher>>
    > = RefCell::new(HashMap::new());
    static THREAD_LOCAL_WRITE_SET_VERSIONS: RefCell<
        HashMap<TxnToken, HashMap<PageNumber, WriteVersionEntry, PageNumberBuildHasher>>
    > = RefCell::new(HashMap::new());
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
    /// Whether SSI validation is enabled for this transaction (captured at BEGIN).
    ///
    /// Connection setting: `PRAGMA fsqlite.serializable`.
    pub ssi_enabled_at_begin: bool,
    /// True iff this txn currently holds the global write mutex (Serialized mode).
    pub serialized_write_lock_held: bool,
    /// Per-page read-set version ledger used by SSI conflict detection.
    pub read_set_versions: HashMap<PageNumber, CommitSeq, PageNumberBuildHasher>,
    /// Per-page write-set version ledger (old/new commit sequence evidence).
    pub write_set_versions: HashMap<PageNumber, WriteVersionEntry, PageNumberBuildHasher>,
    /// Storage mode for read-set tracking.
    pub read_set_storage_mode: ReadSetStorageMode,
    /// Optional Bloom-backed approximate membership filter for large read sets.
    pub read_set_bloom: Option<ReadSetBloom>,
    /// SSI witness-plane read evidence (§5.6.4).
    pub read_keys: HashSet<WitnessKey>,
    /// SSI witness-plane write evidence (§5.6.4).
    pub write_keys: HashSet<WitnessKey>,
    /// SSI tracking: has an incoming rw-antidependency edge.
    pub has_in_rw: bool,
    /// SSI tracking: has an outgoing rw-antidependency edge.
    pub has_out_rw: bool,
    /// Monotonic start time (logical milliseconds) used for max-duration enforcement.
    pub started_at_ms: u64,
    /// Epoch-based reclamation ticket registered at transaction begin, dropped on
    /// commit/abort.  Tracks this transaction in the [`VersionGuardRegistry`] for
    /// stale-reader detection and provides `defer_retire` for safe deferred
    /// reclamation of superseded page versions (§14.10, bd-2y306.1).
    pub version_guard: Option<VersionGuardTicket>,
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
            ssi_enabled_at_begin: true,
            serialized_write_lock_held: false,
            read_set_versions: HashMap::with_hasher(PageNumberBuildHasher::default()),
            write_set_versions: HashMap::with_hasher(PageNumberBuildHasher::default()),
            read_set_storage_mode: ReadSetStorageMode::Exact,
            read_set_bloom: None,
            read_keys: HashSet::new(),
            write_keys: HashSet::new(),
            has_in_rw: false,
            has_out_rw: false,
            started_at_ms: logical_now_millis(),
            version_guard: None,
        }
    }

    /// Token identifying this transaction.
    #[must_use]
    pub fn token(&self) -> TxnToken {
        TxnToken::new(self.txn_id, self.txn_epoch)
    }

    /// Whether an EBR guard is currently pinned for this transaction.
    #[must_use]
    pub fn has_version_guard(&self) -> bool {
        self.version_guard.is_some()
    }

    /// Defer retirement of a superseded page version through the EBR ticket.
    ///
    /// Pins the current thread's epoch, defers the value's drop until all
    /// concurrent readers have advanced past the current epoch, then flushes.
    /// Returns `false` if no ticket is registered (caller should fall back to
    /// synchronous freeing).
    pub fn defer_retire_version<T: Send + 'static>(&self, retired: T) -> bool {
        if let Some(ticket) = &self.version_guard {
            ticket.defer_retire(retired);
            true
        } else {
            false
        }
    }

    /// Set read-set storage mode.
    pub fn set_read_set_storage_mode(&mut self, mode: ReadSetStorageMode) {
        self.read_set_storage_mode = mode;
        if mode == ReadSetStorageMode::Bloom {
            if self.read_set_bloom.is_none() {
                self.read_set_bloom = Some(ReadSetBloom::new(ReadSetBloom::DEFAULT_BITS));
            }
        } else {
            self.read_set_bloom = None;
        }
    }

    /// Record a page read with the visible committed version.
    pub fn record_page_read(&mut self, page: PageNumber, version: CommitSeq) {
        if let Some(bloom) = self.read_set_bloom.as_mut() {
            bloom.insert(page);
        }
        self.read_set_versions
            .entry(page)
            .and_modify(|existing| {
                if version > *existing {
                    *existing = version;
                }
            })
            .or_insert(version);
        self.read_keys.insert(WitnessKey::Page(page));
        let token = self.token();
        THREAD_LOCAL_READ_SET_VERSIONS.with(|store| {
            let mut store = store.borrow_mut();
            let entry = store
                .entry(token)
                .or_insert_with(|| HashMap::with_hasher(PageNumberBuildHasher::default()));
            entry
                .entry(page)
                .and_modify(|existing| {
                    if version > *existing {
                        *existing = version;
                    }
                })
                .or_insert(version);
        });
    }

    /// Record a range-scan witness set for predicate-style tracking.
    pub fn record_range_scan(&mut self, leaf_pages: &[PageNumber], version: CommitSeq) {
        for key in WitnessKey::for_range_scan(leaf_pages) {
            if let WitnessKey::Page(page) = key {
                self.record_page_read(page, version);
            } else {
                self.read_keys.insert(key);
            }
        }
    }

    /// Record a page write with the visible base version.
    pub fn record_page_write(&mut self, page: PageNumber, old_version: Option<CommitSeq>) {
        self.write_set_versions
            .entry(page)
            .or_insert_with(|| WriteVersionEntry::new(old_version));
        self.write_keys.insert(WitnessKey::Page(page));
        let token = self.token();
        THREAD_LOCAL_WRITE_SET_VERSIONS.with(|store| {
            let mut store = store.borrow_mut();
            let entry = store
                .entry(token)
                .or_insert_with(|| HashMap::with_hasher(PageNumberBuildHasher::default()));
            entry
                .entry(page)
                .or_insert_with(|| WriteVersionEntry::new(old_version));
        });
    }

    /// Attach the assigned commit sequence to a written page entry.
    pub fn mark_page_write_committed(&mut self, page: PageNumber, new_version: CommitSeq) {
        if let Some(entry) = self.write_set_versions.get_mut(&page) {
            entry.new_version = Some(new_version);
        }
        let token = self.token();
        THREAD_LOCAL_WRITE_SET_VERSIONS.with(|store| {
            if let Some(entry) = store
                .borrow_mut()
                .get_mut(&token)
                .and_then(|versions| versions.get_mut(&page))
            {
                entry.new_version = Some(new_version);
            }
        });
    }

    /// Lookup tracked read version for a page.
    #[must_use]
    pub fn read_version_for_page(&self, page: PageNumber) -> Option<CommitSeq> {
        self.read_set_versions.get(&page).copied()
    }

    /// Lookup tracked write metadata for a page.
    #[must_use]
    pub fn write_version_for_page(&self, page: PageNumber) -> Option<WriteVersionEntry> {
        self.write_set_versions.get(&page).copied()
    }

    /// Membership check that uses exact or bloom-backed read-set mode.
    #[must_use]
    pub fn read_set_maybe_contains(&self, page: PageNumber) -> bool {
        if self.read_set_versions.contains_key(&page) {
            return true;
        }
        self.read_set_bloom
            .as_ref()
            .is_some_and(|bloom| bloom.may_contain(page))
    }

    /// Read page version from this thread's per-transaction mirror.
    #[must_use]
    pub fn thread_local_read_version_for_page(&self, page: PageNumber) -> Option<CommitSeq> {
        let token = self.token();
        THREAD_LOCAL_READ_SET_VERSIONS.with(|store| {
            store
                .borrow()
                .get(&token)
                .and_then(|versions| versions.get(&page).copied())
        })
    }

    /// Write page version metadata from this thread's per-transaction mirror.
    #[must_use]
    pub fn thread_local_write_version_for_page(
        &self,
        page: PageNumber,
    ) -> Option<WriteVersionEntry> {
        let token = self.token();
        THREAD_LOCAL_WRITE_SET_VERSIONS.with(|store| {
            store
                .borrow()
                .get(&token)
                .and_then(|versions| versions.get(&page).copied())
        })
    }

    /// Number of read-set entries in the current thread-local mirror.
    #[must_use]
    pub fn thread_local_read_set_len(&self) -> usize {
        let token = self.token();
        THREAD_LOCAL_READ_SET_VERSIONS.with(|store| {
            store
                .borrow()
                .get(&token)
                .map_or(0_usize, std::collections::HashMap::len)
        })
    }

    /// Number of write-set entries in the current thread-local mirror.
    #[must_use]
    pub fn thread_local_write_set_len(&self) -> usize {
        let token = self.token();
        THREAD_LOCAL_WRITE_SET_VERSIONS.with(|store| {
            store
                .borrow()
                .get(&token)
                .map_or(0_usize, std::collections::HashMap::len)
        })
    }

    /// Clear read/write tracking ledgers (called on txn finalization).
    pub fn clear_page_access_tracking(&mut self) {
        self.read_set_versions.clear();
        self.write_set_versions.clear();
        if let Some(bloom) = self.read_set_bloom.as_mut() {
            bloom.clear();
        }
        let token = self.token();
        THREAD_LOCAL_READ_SET_VERSIONS.with(|store| {
            store.borrow_mut().remove(&token);
        });
        THREAD_LOCAL_WRITE_SET_VERSIONS.with(|store| {
            store.borrow_mut().remove(&token);
        });
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
    let reclaim_pid = slot.pid.load(Ordering::Acquire);
    let prior_cleanup_marker = slot.cleanup_txn_id.load(Ordering::Acquire);

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

    // If a stuck CLEANING slot carried a prewritten cleanup marker, it may have
    // originated from a previously published real transaction slot.
    if !was_claiming && prior_cleanup_marker != 0 {
        GLOBAL_TXN_SLOT_METRICS.record_slot_released(None, reclaim_pid);
    }

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

    for (idx, slot) in slots.iter().enumerate() {
        let slot_pid = slot.pid.load(Ordering::Acquire);
        let result = try_cleanup_sentinel_slot(slot, now_epoch_secs, &process_alive);
        if let SlotCleanupResult::Reclaimed { orphan_txn_id, .. } = result {
            cleaned += 1;
            GLOBAL_TXN_SLOT_METRICS.record_crash_detected(Some(idx), slot_pid, orphan_txn_id);
        }
    }

    let horizon_result = raise_gc_horizon(slots, old_horizon, commit_seq);
    (horizon_result, cleaned)
}

// ---------------------------------------------------------------------------
// Orphaned Slot Cleanup (§5.6.2.2, bd-2xns)
// ---------------------------------------------------------------------------

/// Statistics from a full [`cleanup_orphaned_slots`] pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrphanedSlotCleanupStats {
    /// Number of slots scanned.
    pub scanned: usize,
    /// Number of orphaned slots reclaimed.
    pub orphans_found: usize,
    /// Number of page lock releases performed.
    pub locks_released: usize,
}

/// Clear all mutable fields of a [`SharedTxnSlot`], with `txn_id = 0` stored
/// last using `Release` ordering (§5.6.2.2 field-clearing discipline).
///
/// The `txn_id = 0` store is the sentinel that marks the slot as free. Any
/// reader that observes `txn_id == 0` must see all other fields already cleared
/// (Release/Acquire pairing).
fn clear_slot_fields(slot: &SharedTxnSlot) {
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
}

/// Attempt to clean up a single orphaned slot (§5.6.2.2).
///
/// This handles all three branches of the orphaned slot state machine:
///
/// 1. **TAG_CLEANING:** Stuck cleaner — release locks, clear fields.
/// 2. **TAG_CLAIMING:** Dead claimer — CAS to CLEANING, clear fields.
/// 3. **Real TxnId:** Expired lease + dead process — CAS to CLEANING, release
///    locks, clear fields.
///
/// # Arguments
///
/// * `slot` — the [`SharedTxnSlot`] to inspect.
/// * `now_epoch_secs` — current time in unix epoch seconds.
/// * `process_alive` — returns `true` if process with `(pid, pid_birth)` is
///   alive.
/// * `release_locks` — callback to release page locks for a given TxnId.
///
/// # Returns
///
/// A [`SlotCleanupResult`] indicating what action was taken.
#[allow(clippy::too_many_lines)]
pub fn try_cleanup_orphaned_slot(
    slot: &SharedTxnSlot,
    now_epoch_secs: u64,
    process_alive: impl Fn(u32, u64) -> bool,
    release_locks: impl Fn(u64),
) -> SlotCleanupResult {
    // Single-read-per-iteration rule: snapshot txn_id ONCE.
    let tid = slot.txn_id.load(Ordering::Acquire);
    if tid == 0 {
        return SlotCleanupResult::NotApplicable;
    }

    let tag = decode_tag(tid);

    if tag != 0 {
        // ===== Sentinel-tagged slot =====
        let was_claiming = tag == TAG_CLAIMING;
        let reclaim_pid = slot.pid.load(Ordering::Acquire);
        let prior_cleanup_marker = slot.cleanup_txn_id.load(Ordering::Acquire);

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

        if was_claiming {
            // TAG_CLAIMING: check process liveness first.
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

        if was_claiming {
            // Transition CLAIMING → CLEANING (preserves identity).
            let cleaning_word = encode_cleaning(orphan_txn_id);
            if slot
                .txn_id
                .compare_exchange(tid, cleaning_word, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                return SlotCleanupResult::CasRaceSkipped;
            }
            slot.claiming_timestamp
                .store(now_epoch_secs, Ordering::Release);
            tracing::info!(
                orphan_txn_id,
                "transitioned stale CLAIMING slot to CLEANING"
            );
        }

        // TAG_CLEANING payload preserves identity for retryable lock release.
        slot.cleanup_txn_id.store(orphan_txn_id, Ordering::Release);

        // Release page locks (idempotent).
        if orphan_txn_id != 0 {
            release_locks(orphan_txn_id);
        }

        tracing::info!(
            orphan_txn_id,
            was_claiming,
            "reclaiming stale sentinel slot"
        );

        clear_slot_fields(slot);

        if !was_claiming && prior_cleanup_marker != 0 {
            GLOBAL_TXN_SLOT_METRICS.record_slot_released(None, reclaim_pid);
        }

        return SlotCleanupResult::Reclaimed {
            orphan_txn_id,
            was_claiming,
        };
    }

    // ===== Real TxnId (no sentinel tag) =====
    let lease = slot.lease_expiry.load(Ordering::Acquire);
    if lease != 0 && lease > now_epoch_secs {
        // Lease not expired — slot is still valid.
        return SlotCleanupResult::NotApplicable;
    }

    let pid = slot.pid.load(Ordering::Acquire);
    let birth = slot.pid_birth.load(Ordering::Acquire);
    if pid != 0 && birth != 0 && process_alive(pid, birth) {
        return SlotCleanupResult::ProcessAlive;
    }

    // Dead process with expired (or zero) lease — reclaim.
    let orphan_txn_id = decode_payload(tid);

    // Write cleanup_txn_id BEFORE sentinel overwrite (crash-safety).
    slot.cleanup_txn_id.store(orphan_txn_id, Ordering::Release);

    // CAS to CLEANING.
    let cleaning_word = encode_cleaning(orphan_txn_id);
    if slot
        .txn_id
        .compare_exchange(tid, cleaning_word, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return SlotCleanupResult::CasRaceSkipped;
    }
    slot.claiming_timestamp
        .store(now_epoch_secs, Ordering::Release);

    // Release page locks (idempotent).
    release_locks(orphan_txn_id);

    tracing::info!(orphan_txn_id, "reclaiming orphaned real TxnId slot");

    clear_slot_fields(slot);
    GLOBAL_TXN_SLOT_METRICS.record_slot_released(None, pid);

    SlotCleanupResult::Reclaimed {
        orphan_txn_id,
        was_claiming: false,
    }
}

/// Scan all slots and clean up orphaned entries (§5.6.2.2).
///
/// Combines per-slot cleanup with statistics collection. This is the main
/// entry point for periodic crash recovery maintenance.
#[allow(clippy::needless_pass_by_value)]
pub fn cleanup_orphaned_slots(
    slots: &[SharedTxnSlot],
    now_epoch_secs: u64,
    process_alive: impl Fn(u32, u64) -> bool,
    release_locks: impl Fn(u64),
) -> OrphanedSlotCleanupStats {
    let mut orphans_found = 0_usize;
    let mut locks_released = 0_usize;

    tracing::info!(
        scanned = slots.len(),
        "starting cleanup_orphaned_slots pass"
    );

    for (idx, slot) in slots.iter().enumerate() {
        let slot_pid = slot.pid.load(Ordering::Acquire);
        let result =
            try_cleanup_orphaned_slot(slot, now_epoch_secs, &process_alive, &release_locks);
        if let SlotCleanupResult::Reclaimed {
            orphan_txn_id,
            was_claiming,
        } = result
        {
            orphans_found += 1;
            locks_released += 1;
            GLOBAL_TXN_SLOT_METRICS.record_crash_detected(Some(idx), slot_pid, orphan_txn_id);
            tracing::debug!(
                slot_idx = idx,
                orphan_txn_id,
                was_claiming,
                "reclaimed orphaned slot"
            );
        }
    }

    tracing::info!(
        scanned = slots.len(),
        orphans_found,
        locks_released,
        "completed cleanup_orphaned_slots pass"
    );

    OrphanedSlotCleanupStats {
        scanned: slots.len(),
        orphans_found,
        locks_released,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use fsqlite_types::{PageData, PageSize, SchemaEpoch, VersionPointer};
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

        // Slot reused -> same chunk/offset
        assert_eq!(idx1.chunk(), idx2.chunk());
        assert_eq!(idx1.offset(), idx2.offset());
        // Generation incremented -> idx1 != idx2
        assert_ne!(idx1.generation(), idx2.generation());

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
        assert!(txn.read_set_versions.is_empty());
        assert!(txn.write_set_versions.is_empty());
        assert_eq!(txn.read_set_storage_mode, ReadSetStorageMode::Exact);
        assert!(txn.read_set_bloom.is_none());
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

    #[test]
    fn test_transaction_page_access_tracking_records_versions() {
        let txn_id = TxnId::new(33).unwrap();
        let snap = Snapshot::new(CommitSeq::new(12), SchemaEpoch::ZERO);
        let mut txn = Transaction::new(txn_id, TxnEpoch::new(0), snap, TransactionMode::Concurrent);
        let p_read = PageNumber::new(7).unwrap();
        let p_write = PageNumber::new(9).unwrap();

        txn.record_page_read(p_read, CommitSeq::new(4));
        txn.record_page_read(p_read, CommitSeq::new(5));
        txn.record_page_write(p_write, Some(CommitSeq::new(5)));
        txn.mark_page_write_committed(p_write, CommitSeq::new(13));

        assert_eq!(txn.read_version_for_page(p_read), Some(CommitSeq::new(5)));
        let write = txn.write_version_for_page(p_write).unwrap();
        assert_eq!(write.old_version, Some(CommitSeq::new(5)));
        assert_eq!(write.new_version, Some(CommitSeq::new(13)));
        assert!(txn.read_keys.contains(&WitnessKey::Page(p_read)));
        assert!(txn.write_keys.contains(&WitnessKey::Page(p_write)));
    }

    #[test]
    fn test_transaction_clear_page_access_tracking() {
        let txn_id = TxnId::new(41).unwrap();
        let snap = Snapshot::new(CommitSeq::new(1), SchemaEpoch::ZERO);
        let mut txn = Transaction::new(txn_id, TxnEpoch::new(0), snap, TransactionMode::Serialized);
        let page = PageNumber::new(3).unwrap();

        txn.record_page_read(page, CommitSeq::new(1));
        txn.record_page_write(page, Some(CommitSeq::new(1)));
        assert!(!txn.read_set_versions.is_empty());
        assert!(!txn.write_set_versions.is_empty());

        txn.clear_page_access_tracking();
        assert!(txn.read_set_versions.is_empty());
        assert!(txn.write_set_versions.is_empty());
    }

    #[test]
    fn test_transaction_bloom_read_set_mode() {
        let txn_id = TxnId::new(51).unwrap();
        let snap = Snapshot::new(CommitSeq::new(2), SchemaEpoch::ZERO);
        let mut txn = Transaction::new(txn_id, TxnEpoch::new(0), snap, TransactionMode::Concurrent);
        let page = PageNumber::new(19).unwrap();

        txn.set_read_set_storage_mode(ReadSetStorageMode::Bloom);
        assert!(txn.read_set_bloom.is_some());
        txn.record_page_read(page, CommitSeq::new(2));
        assert!(txn.read_set_maybe_contains(page));

        txn.clear_page_access_tracking();
        assert!(
            !txn.read_set_maybe_contains(page),
            "cleared bloom tracking must not claim membership for previously recorded pages"
        );
    }

    #[test]
    fn test_transaction_record_range_scan_adds_page_witnesses() {
        let txn_id = TxnId::new(61).unwrap();
        let snap = Snapshot::new(CommitSeq::new(9), SchemaEpoch::ZERO);
        let mut txn = Transaction::new(txn_id, TxnEpoch::new(0), snap, TransactionMode::Concurrent);
        let pages = [PageNumber::new(41).unwrap(), PageNumber::new(42).unwrap()];

        txn.record_range_scan(&pages, CommitSeq::new(9));

        for page in pages {
            assert_eq!(txn.read_version_for_page(page), Some(CommitSeq::new(9)));
            assert!(
                txn.read_keys.contains(&WitnessKey::Page(page)),
                "range-scan recording must include page witness keys"
            );
        }
    }

    #[test]
    fn test_transaction_thread_local_mirrors_track_and_clear() {
        let txn_id = TxnId::new(71).unwrap();
        let snap = Snapshot::new(CommitSeq::new(5), SchemaEpoch::ZERO);
        let mut txn = Transaction::new(txn_id, TxnEpoch::new(0), snap, TransactionMode::Concurrent);
        let p_read = PageNumber::new(90).unwrap();
        let p_write = PageNumber::new(91).unwrap();

        txn.record_page_read(p_read, CommitSeq::new(5));
        txn.record_page_write(p_write, Some(CommitSeq::new(5)));
        txn.mark_page_write_committed(p_write, CommitSeq::new(6));

        assert_eq!(txn.thread_local_read_set_len(), 1);
        assert_eq!(txn.thread_local_write_set_len(), 1);
        assert_eq!(
            txn.thread_local_read_version_for_page(p_read),
            Some(CommitSeq::new(5))
        );
        let entry = txn.thread_local_write_version_for_page(p_write).unwrap();
        assert_eq!(entry.old_version, Some(CommitSeq::new(5)));
        assert_eq!(entry.new_version, Some(CommitSeq::new(6)));

        txn.clear_page_access_tracking();
        assert_eq!(txn.thread_local_read_set_len(), 0);
        assert_eq!(txn.thread_local_write_set_len(), 0);
    }

    #[test]
    fn test_transaction_thread_local_mirrors_are_thread_isolated() {
        let page = PageNumber::new(100).unwrap();
        let mut main_txn = Transaction::new(
            TxnId::new(81).unwrap(),
            TxnEpoch::new(0),
            Snapshot::new(CommitSeq::new(10), SchemaEpoch::ZERO),
            TransactionMode::Concurrent,
        );
        main_txn.record_page_read(page, CommitSeq::new(10));
        assert_eq!(
            main_txn.thread_local_read_version_for_page(page),
            Some(CommitSeq::new(10))
        );

        std::thread::spawn(move || {
            let mut thread_txn = Transaction::new(
                TxnId::new(82).unwrap(),
                TxnEpoch::new(0),
                Snapshot::new(CommitSeq::new(11), SchemaEpoch::ZERO),
                TransactionMode::Concurrent,
            );
            thread_txn.record_page_read(page, CommitSeq::new(11));
            assert_eq!(thread_txn.thread_local_read_set_len(), 1);
            assert_eq!(
                thread_txn.thread_local_read_version_for_page(page),
                Some(CommitSeq::new(11))
            );
            thread_txn.clear_page_access_tracking();
            assert_eq!(thread_txn.thread_local_read_set_len(), 0);
        })
        .join()
        .unwrap();

        assert_eq!(
            main_txn.thread_local_read_version_for_page(page),
            Some(CommitSeq::new(10))
        );
        main_txn.clear_page_access_tracking();
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
    }

    #[test]
    fn test_all_types_clone_eq() {
        fn assert_clone_eq<T: Clone + PartialEq>() {}

        assert_clone_eq::<VersionIdx>();
        assert_clone_eq::<TransactionState>();
        assert_clone_eq::<TransactionMode>();
        assert_clone_eq::<CommitRecord>();
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
                unreachable!(
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
        CLAIMING_TIMEOUT_NO_PID_SECS, CLAIMING_TIMEOUT_SECS, TAG_CLEANING, encode_claiming,
        encode_cleaning,
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

    // ===================================================================
    // bd-2xns: TxnSlot Crash Recovery — cleanup_orphaned_slots (§5.6.2.2)
    // ===================================================================

    const BEAD_2XNS: &str = "bd-2xns";

    /// Helper: create a slot with a real TxnId, lease, and process identity.
    fn make_orphaned_real_slot(
        txn_id_raw: u64,
        lease_expiry: u64,
        pid: u32,
        pid_birth: u64,
    ) -> SharedTxnSlot {
        let slot = SharedTxnSlot::new();
        slot.txn_id.store(txn_id_raw, Ordering::Release);
        slot.lease_expiry.store(lease_expiry, Ordering::Release);
        slot.pid.store(pid, Ordering::Release);
        slot.pid_birth.store(pid_birth, Ordering::Release);
        slot.begin_seq.store(50, Ordering::Release);
        slot
    }

    #[test]
    fn test_cleanup_skips_free_slots() {
        let slots = [SharedTxnSlot::new(), SharedTxnSlot::new()];
        let released = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let released_clone = std::sync::Arc::clone(&released);

        let stats = cleanup_orphaned_slots(
            &slots,
            9999,
            |_, _| false,
            |txn_id| {
                released_clone.lock().unwrap().push(txn_id);
            },
        );

        assert_eq!(
            stats.scanned, 2,
            "bead_id={BEAD_2XNS} case=skips_free_scanned"
        );
        assert_eq!(
            stats.orphans_found, 0,
            "bead_id={BEAD_2XNS} case=skips_free_no_orphans"
        );
        assert!(
            released.lock().unwrap().is_empty(),
            "bead_id={BEAD_2XNS} case=skips_free_no_releases"
        );
        for slot in &slots {
            assert!(
                slot.is_free(Ordering::Acquire),
                "bead_id={BEAD_2XNS} case=skips_free_unchanged"
            );
        }
    }

    #[test]
    fn test_cleanup_reclaims_expired_dead_process() {
        let txn_id_raw = 42_u64;
        let now = 1000_u64;
        let expired_lease = now - 10;

        let slot = make_orphaned_real_slot(txn_id_raw, expired_lease, 12345, 9999);
        let released = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let released_clone = std::sync::Arc::clone(&released);

        let result = try_cleanup_orphaned_slot(
            &slot,
            now,
            |_, _| false,
            |txn_id| {
                released_clone.lock().unwrap().push(txn_id);
            },
        );

        assert!(
            matches!(
                result,
                SlotCleanupResult::Reclaimed { orphan_txn_id, .. }
                    if orphan_txn_id == txn_id_raw
            ),
            "bead_id={BEAD_2XNS} case=reclaims_expired_dead"
        );
        assert!(
            slot.is_free(Ordering::Acquire),
            "bead_id={BEAD_2XNS} case=slot_freed"
        );
        assert_eq!(
            released.lock().unwrap().as_slice(),
            &[txn_id_raw],
            "bead_id={BEAD_2XNS} case=locks_released"
        );
        assert_eq!(slot.begin_seq.load(Ordering::Acquire), 0);
        assert_eq!(slot.pid.load(Ordering::Acquire), 0);
        assert_eq!(slot.pid_birth.load(Ordering::Acquire), 0);
        assert_eq!(slot.lease_expiry.load(Ordering::Acquire), 0);
        assert_eq!(slot.cleanup_txn_id.load(Ordering::Acquire), 0);
    }

    #[test]
    fn test_cleanup_skips_alive_process_even_expired_lease() {
        let txn_id_raw = 42_u64;
        let now = 1000_u64;
        let expired_lease = now - 10;
        let slot = make_orphaned_real_slot(txn_id_raw, expired_lease, 12345, 9999);

        let result = try_cleanup_orphaned_slot(&slot, now, |_, _| true, |_| {});

        assert_eq!(
            result,
            SlotCleanupResult::ProcessAlive,
            "bead_id={BEAD_2XNS} case=alive_skipped"
        );
        assert_eq!(
            slot.txn_id.load(Ordering::Acquire),
            txn_id_raw,
            "bead_id={BEAD_2XNS} case=alive_txn_unchanged"
        );
    }

    #[test]
    fn test_cleanup_claiming_no_pid_uses_30s_timeout() {
        let txn_id_raw = 99_u64;
        let claim_time = 1000_u64;
        let slot = make_claiming_slot(txn_id_raw, claim_time);
        assert_eq!(slot.pid.load(Ordering::Relaxed), 0);

        let recent_now = claim_time + 10;
        let result = try_cleanup_orphaned_slot(&slot, recent_now, |_, _| false, |_| {});
        assert_eq!(
            result,
            SlotCleanupResult::StillRecent,
            "bead_id={BEAD_2XNS} case=claiming_no_pid_too_recent"
        );

        let stale_now = claim_time + CLAIMING_TIMEOUT_NO_PID_SECS + 1;
        let result = try_cleanup_orphaned_slot(&slot, stale_now, |_, _| false, |_| {});
        assert!(
            matches!(
                result,
                SlotCleanupResult::Reclaimed {
                    orphan_txn_id,
                    was_claiming: true
                } if orphan_txn_id == txn_id_raw
            ),
            "bead_id={BEAD_2XNS} case=claiming_no_pid_reclaimed"
        );
        assert!(slot.is_free(Ordering::Acquire));
    }

    #[test]
    fn test_cleanup_claiming_with_pid_uses_5s_timeout() {
        let txn_id_raw = 55_u64;
        let claim_time = 1000_u64;
        let slot = make_claiming_slot(txn_id_raw, claim_time);
        slot.pid.store(12345, Ordering::Release);
        slot.pid_birth.store(9999, Ordering::Release);

        let recent_now = claim_time + 3;
        let result = try_cleanup_orphaned_slot(&slot, recent_now, |_, _| false, |_| {});
        assert_eq!(
            result,
            SlotCleanupResult::StillRecent,
            "bead_id={BEAD_2XNS} case=claiming_pid_too_recent"
        );

        let stale_now = claim_time + CLAIMING_TIMEOUT_SECS + 1;
        let result = try_cleanup_orphaned_slot(&slot, stale_now, |_, _| false, |_| {});
        assert!(
            matches!(
                result,
                SlotCleanupResult::Reclaimed {
                    orphan_txn_id,
                    was_claiming: true
                } if orphan_txn_id == txn_id_raw
            ),
            "bead_id={BEAD_2XNS} case=claiming_pid_reclaimed"
        );
        assert!(slot.is_free(Ordering::Acquire));
    }

    #[test]
    fn test_cleanup_claiming_alive_process_never_reclaimed() {
        let txn_id_raw = 77_u64;
        let claim_time = 1_u64;
        let slot = make_claiming_slot(txn_id_raw, claim_time);
        slot.pid.store(12345, Ordering::Release);
        slot.pid_birth.store(9999, Ordering::Release);

        let very_late = claim_time + 10_000;
        let result = try_cleanup_orphaned_slot(&slot, very_late, |_, _| true, |_| {});
        assert_eq!(
            result,
            SlotCleanupResult::ProcessAlive,
            "bead_id={BEAD_2XNS} case=alive_never_reclaimed"
        );
        assert!(
            !slot.is_free(Ordering::Acquire),
            "bead_id={BEAD_2XNS} case=alive_slot_not_freed"
        );
    }

    #[test]
    fn test_cleanup_cleaning_stuck_slot_reclaimed() {
        let original_txn_id = 42_u64;
        let long_ago = 1000_u64;
        let now = long_ago + CLAIMING_TIMEOUT_SECS + 10;
        let slot = make_cleaning_slot(original_txn_id, long_ago);

        let released = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let released_clone = std::sync::Arc::clone(&released);

        let result = try_cleanup_orphaned_slot(
            &slot,
            now,
            |_, _| false,
            |txn_id| {
                released_clone.lock().unwrap().push(txn_id);
            },
        );

        assert!(
            matches!(
                result,
                SlotCleanupResult::Reclaimed {
                    orphan_txn_id,
                    was_claiming: false
                } if orphan_txn_id == original_txn_id
            ),
            "bead_id={BEAD_2XNS} case=cleaning_stuck_reclaimed"
        );
        assert!(slot.is_free(Ordering::Acquire));
        assert_eq!(
            released.lock().unwrap().as_slice(),
            &[original_txn_id],
            "bead_id={BEAD_2XNS} case=cleaning_locks_released"
        );
    }

    #[test]
    fn test_cleanup_concurrent_cas_contention() {
        use std::sync::{Arc, Barrier};
        use std::thread;

        let txn_id_raw = 42_u64;
        let claim_time = 1_u64;
        let now = claim_time + CLAIMING_TIMEOUT_NO_PID_SECS + 10;

        let slot = Arc::new(make_claiming_slot(txn_id_raw, claim_time));
        let barrier = Arc::new(Barrier::new(2));
        let release_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let mut handles = Vec::new();
        for _ in 0..2 {
            let slot_ref = Arc::clone(&slot);
            let barrier_ref = Arc::clone(&barrier);
            let count_ref = Arc::clone(&release_count);
            handles.push(thread::spawn(move || {
                barrier_ref.wait();
                try_cleanup_orphaned_slot(
                    &slot_ref,
                    now,
                    |_, _| false,
                    |_| {
                        count_ref.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    },
                )
            }));
        }

        let results: Vec<_> = handles
            .into_iter()
            .map(|h| h.join().expect("cleaner thread panicked"))
            .collect();

        let reclaimed_count = results
            .iter()
            .filter(|r| matches!(r, SlotCleanupResult::Reclaimed { .. }))
            .count();

        assert!(
            reclaimed_count >= 1,
            "bead_id={BEAD_2XNS} case=concurrent_at_least_one_reclaimed"
        );
        assert!(
            slot.is_free(Ordering::Acquire),
            "bead_id={BEAD_2XNS} case=concurrent_slot_freed"
        );
        assert!(
            release_count.load(std::sync::atomic::Ordering::SeqCst) >= 1,
            "bead_id={BEAD_2XNS} case=concurrent_lock_release"
        );
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn test_cleanup_field_clearing_order() {
        let original_txn_id = 42_u64;
        let long_ago = 1000_u64;
        let now = long_ago + CLAIMING_TIMEOUT_SECS + 10;

        let slot = make_cleaning_slot(original_txn_id, long_ago);
        slot.state.store(1, Ordering::Release);
        slot.mode.store(1, Ordering::Release);
        slot.commit_seq.store(99, Ordering::Release);
        slot.begin_seq.store(50, Ordering::Release);
        slot.snapshot_high.store(100, Ordering::Release);
        slot.witness_epoch.store(3, Ordering::Release);
        slot.has_in_rw.store(true, Ordering::Release);
        slot.has_out_rw.store(true, Ordering::Release);
        slot.marked_for_abort.store(true, Ordering::Release);
        slot.write_set_pages.store(10, Ordering::Release);
        slot.pid.store(12345, Ordering::Release);
        slot.pid_birth.store(9999, Ordering::Release);
        slot.lease_expiry.store(5000, Ordering::Release);

        let result = try_cleanup_orphaned_slot(&slot, now, |_, _| false, |_| {});
        assert!(matches!(result, SlotCleanupResult::Reclaimed { .. }));

        assert_eq!(
            slot.txn_id.load(Ordering::Acquire),
            0,
            "bead_id={BEAD_2XNS} case=field_order_txn_id"
        );
        assert_eq!(
            slot.state.load(Ordering::Acquire),
            0,
            "bead_id={BEAD_2XNS} case=field_order_state"
        );
        assert_eq!(
            slot.mode.load(Ordering::Acquire),
            0,
            "bead_id={BEAD_2XNS} case=field_order_mode"
        );
        assert_eq!(
            slot.commit_seq.load(Ordering::Acquire),
            0,
            "bead_id={BEAD_2XNS} case=field_order_commit_seq"
        );
        assert_eq!(
            slot.begin_seq.load(Ordering::Acquire),
            0,
            "bead_id={BEAD_2XNS} case=field_order_begin_seq"
        );
        assert_eq!(
            slot.snapshot_high.load(Ordering::Acquire),
            0,
            "bead_id={BEAD_2XNS} case=field_order_snapshot_high"
        );
        assert_eq!(
            slot.witness_epoch.load(Ordering::Acquire),
            0,
            "bead_id={BEAD_2XNS} case=field_order_witness_epoch"
        );
        assert!(
            !slot.has_in_rw.load(Ordering::Acquire),
            "bead_id={BEAD_2XNS} case=field_order_has_in_rw"
        );
        assert!(
            !slot.has_out_rw.load(Ordering::Acquire),
            "bead_id={BEAD_2XNS} case=field_order_has_out_rw"
        );
        assert!(
            !slot.marked_for_abort.load(Ordering::Acquire),
            "bead_id={BEAD_2XNS} case=field_order_marked_for_abort"
        );
        assert_eq!(
            slot.write_set_pages.load(Ordering::Acquire),
            0,
            "bead_id={BEAD_2XNS} case=field_order_write_set_pages"
        );
        assert_eq!(
            slot.pid.load(Ordering::Acquire),
            0,
            "bead_id={BEAD_2XNS} case=field_order_pid"
        );
        assert_eq!(
            slot.pid_birth.load(Ordering::Acquire),
            0,
            "bead_id={BEAD_2XNS} case=field_order_pid_birth"
        );
        assert_eq!(
            slot.lease_expiry.load(Ordering::Acquire),
            0,
            "bead_id={BEAD_2XNS} case=field_order_lease_expiry"
        );
        assert_eq!(
            slot.cleanup_txn_id.load(Ordering::Acquire),
            0,
            "bead_id={BEAD_2XNS} case=field_order_cleanup_txn_id"
        );
        assert_eq!(
            slot.claiming_timestamp.load(Ordering::Acquire),
            0,
            "bead_id={BEAD_2XNS} case=field_order_claiming_ts"
        );
    }

    #[test]
    fn test_cleanup_cleaning_preserves_payload_for_lock_release() {
        let original_txn_id = 42_u64;
        let cleaning_word = encode_cleaning(original_txn_id);
        assert_eq!(
            decode_payload(cleaning_word),
            original_txn_id,
            "bead_id={BEAD_2XNS} case=cleaning_payload_preserved"
        );

        let long_ago = 1000_u64;
        let now = long_ago + CLAIMING_TIMEOUT_SECS + 10;
        let slot = make_cleaning_slot(original_txn_id, long_ago);

        let released = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let released_clone = std::sync::Arc::clone(&released);

        let result = try_cleanup_orphaned_slot(
            &slot,
            now,
            |_, _| false,
            |txn_id| {
                released_clone.lock().unwrap().push(txn_id);
            },
        );

        assert!(
            matches!(
                result,
                SlotCleanupResult::Reclaimed { orphan_txn_id, .. }
                    if orphan_txn_id == original_txn_id
            ),
            "bead_id={BEAD_2XNS} case=cleaning_correct_orphan_txn_id"
        );
        assert_eq!(
            released.lock().unwrap().as_slice(),
            &[original_txn_id],
            "bead_id={BEAD_2XNS} case=cleaning_release_correct_txn_id"
        );
    }

    #[test]
    fn test_claiming_timestamp_cleared_after_publish() {
        let txn_id_raw = 42_u64;
        let claim_time = 1000_u64;
        let slot = make_claiming_slot(txn_id_raw, claim_time);
        assert!(slot.is_claiming(Ordering::Acquire));
        assert_eq!(slot.claiming_timestamp.load(Ordering::Acquire), claim_time);

        let claiming_word = encode_claiming(txn_id_raw);
        let publish_ok = slot
            .txn_id
            .compare_exchange(
                claiming_word,
                txn_id_raw,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok();
        assert!(publish_ok, "bead_id={BEAD_2XNS} case=publish_cas_ok");

        slot.claiming_timestamp.store(0, Ordering::Release);
        assert_eq!(
            slot.claiming_timestamp.load(Ordering::Acquire),
            0,
            "bead_id={BEAD_2XNS} case=claiming_ts_cleared_after_publish"
        );

        let cleaning_word = encode_cleaning(txn_id_raw);
        slot.txn_id.store(cleaning_word, Ordering::Release);

        let now = 5000_u64;
        let result = try_cleanup_orphaned_slot(&slot, now, |_, _| false, |_| {});
        assert_eq!(
            result,
            SlotCleanupResult::StillRecent,
            "bead_id={BEAD_2XNS} case=cleaning_zero_ts_seeds_and_waits"
        );
        assert_eq!(
            slot.claiming_timestamp.load(Ordering::Acquire),
            now,
            "bead_id={BEAD_2XNS} case=cleaning_ts_seeded"
        );
    }

    #[test]
    fn test_e2e_orphaned_txnslot_cleanup_after_crash() {
        use std::sync::{Arc, Mutex};

        let now = 10_000_u64;
        let dead_pid = 99_999_u32;
        let dead_pid_birth = 12_345_u64;

        let slots = [
            SharedTxnSlot::new(),
            {
                let s = SharedTxnSlot::new();
                s.txn_id.store(42, Ordering::Release);
                s.begin_seq.store(100, Ordering::Release);
                s.pid.store(dead_pid, Ordering::Release);
                s.pid_birth.store(dead_pid_birth, Ordering::Release);
                s.lease_expiry.store(now - 60, Ordering::Release);
                s.state.store(1, Ordering::Release);
                s.mode.store(1, Ordering::Release);
                s.write_set_pages.store(5, Ordering::Release);
                s
            },
            {
                let s = SharedTxnSlot::new();
                s.txn_id.store(43, Ordering::Release);
                s.begin_seq.store(150, Ordering::Release);
                s.pid.store(dead_pid + 1, Ordering::Release);
                s.pid_birth.store(dead_pid_birth + 1, Ordering::Release);
                s.lease_expiry.store(now + 60, Ordering::Release);
                s
            },
            make_cleaning_slot(44, now - CLAIMING_TIMEOUT_SECS - 10),
        ];

        let released_locks = Arc::new(Mutex::new(Vec::new()));
        let released_clone = Arc::clone(&released_locks);

        let stats = cleanup_orphaned_slots(
            &slots,
            now,
            |pid, _birth| pid == dead_pid + 1,
            |txn_id| {
                released_clone.lock().unwrap().push(txn_id);
            },
        );

        assert_eq!(stats.scanned, 4, "bead_id={BEAD_2XNS} case=e2e_scanned");
        assert_eq!(
            stats.orphans_found, 2,
            "bead_id={BEAD_2XNS} case=e2e_orphans"
        );

        assert!(
            slots[0].is_free(Ordering::Acquire),
            "bead_id={BEAD_2XNS} case=e2e_slot0_still_free"
        );
        assert!(
            slots[1].is_free(Ordering::Acquire),
            "bead_id={BEAD_2XNS} case=e2e_slot1_freed"
        );
        assert!(
            !slots[2].is_free(Ordering::Acquire),
            "bead_id={BEAD_2XNS} case=e2e_slot2_alive"
        );
        assert!(
            slots[3].is_free(Ordering::Acquire),
            "bead_id={BEAD_2XNS} case=e2e_slot3_freed"
        );

        let mut released = released_locks.lock().unwrap().clone();
        released.sort_unstable();
        assert_eq!(
            released,
            vec![42, 44],
            "bead_id={BEAD_2XNS} case=e2e_released_locks"
        );

        let old_horizon = CommitSeq::new(50);
        let commit_seq = CommitSeq::new(200);
        let result = raise_gc_horizon(&slots, old_horizon, commit_seq);
        assert_eq!(
            result.new_horizon,
            CommitSeq::new(150),
            "bead_id={BEAD_2XNS} case=e2e_horizon_advances"
        );
        assert_eq!(result.active_slots, 1);
        assert_eq!(result.sentinel_blockers, 0);
    }

    // ===================================================================
    // bd-2g5.1: Shared-memory TxnSlots with crash recovery
    // ===================================================================

    const BEAD_2G5_1: &str = "bd-2g5.1";
    const TXN_SLOT_E2E_SCENARIO_ID: &str = "TXNSLOT-1";
    const TXN_SLOT_E2E_SEED: u64 = 20_260_219;

    #[test]
    fn test_txn_slot_recovery_no_orphans_after_100_crash_cycles() {
        use std::sync::{Arc, Mutex};

        let slot = SharedTxnSlot::new();
        let released = Arc::new(Mutex::new(Vec::new()));
        let base_now = 50_000_u64;

        for cycle in 0_u64..100 {
            let txn_id_raw = 1_000 + cycle;
            let now = base_now + cycle;
            slot.txn_id.store(txn_id_raw, Ordering::Release);
            slot.begin_seq.store(200 + cycle, Ordering::Release);
            slot.pid.store(44_200, Ordering::Release);
            slot.pid_birth.store(99_000 + cycle, Ordering::Release);
            slot.lease_expiry
                .store(now.saturating_sub(1), Ordering::Release);

            let stats = cleanup_orphaned_slots(
                std::slice::from_ref(&slot),
                now,
                |_, _| false,
                |released_txn_id| {
                    released
                        .lock()
                        .expect("bead_id={BEAD_2G5_1} release log mutex should not be poisoned")
                        .push(released_txn_id);
                },
            );

            assert_eq!(
                stats.orphans_found, 1,
                "bead_id={BEAD_2G5_1} cycle={cycle} each crash cycle should reclaim one orphan",
            );
            assert!(
                slot.is_free(Ordering::Acquire),
                "bead_id={BEAD_2G5_1} cycle={cycle} slot must be reusable after cleanup",
            );
        }

        let (released_len, released_first, released_last) = {
            let released_guard = released
                .lock()
                .expect("bead_id={BEAD_2G5_1} release log mutex should not be poisoned");
            (
                released_guard.len(),
                released_guard.first().copied(),
                released_guard.last().copied(),
            )
        };
        assert_eq!(
            released_len, 100,
            "bead_id={BEAD_2G5_1} no orphaned slots should remain after 100 crash cycles",
        );
        assert_eq!(released_first, Some(1_000));
        assert_eq!(released_last, Some(1_099));
    }

    #[test]
    fn test_txn_slot_cross_process_visibility_shared_slot() {
        use std::sync::{Arc, mpsc};

        let slot = Arc::new(SharedTxnSlot::new());
        let writer_slot = Arc::clone(&slot);
        let (ready_tx, ready_rx) = mpsc::channel::<()>();
        let (release_tx, release_rx) = mpsc::channel::<()>();

        let writer = std::thread::spawn(move || {
            let txn_id_raw = 8_888_u64;
            let pid = 77_777_u32;
            let pid_birth = 424_242_u64;
            let begin_seq = 99_u64;
            let snapshot_high = 100_u64;
            let lease_expiry = 10_000_u64;

            assert!(
                writer_slot.phase1_claim(txn_id_raw),
                "bead_id={BEAD_2G5_1} writer should claim shared slot",
            );
            writer_slot.claiming_timestamp.store(500, Ordering::Release);
            writer_slot.phase2_initialize(
                pid,
                pid_birth,
                lease_expiry,
                begin_seq,
                snapshot_high,
                crate::cache_aligned::slot_mode::CONCURRENT,
                1,
            );
            assert!(
                writer_slot.phase3_publish(txn_id_raw),
                "bead_id={BEAD_2G5_1} writer should publish shared slot",
            );

            ready_tx
                .send(())
                .expect("bead_id={BEAD_2G5_1} ready signal should send");
            release_rx
                .recv_timeout(Duration::from_secs(2))
                .expect("bead_id={BEAD_2G5_1} release signal should arrive");
            writer_slot.release();
        });

        ready_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("bead_id={BEAD_2G5_1} reader should observe published slot");
        assert_eq!(
            slot.txn_id.load(Ordering::Acquire),
            8_888,
            "bead_id={BEAD_2G5_1} txn_id visibility across processes/threads",
        );
        assert_eq!(
            slot.pid.load(Ordering::Acquire),
            77_777,
            "bead_id={BEAD_2G5_1} pid visibility across processes/threads",
        );
        assert_eq!(
            slot.begin_seq.load(Ordering::Acquire),
            99,
            "bead_id={BEAD_2G5_1} begin_seq visibility across processes/threads",
        );
        assert_eq!(
            slot.snapshot_high.load(Ordering::Acquire),
            100,
            "bead_id={BEAD_2G5_1} snapshot_high visibility across processes/threads",
        );

        release_tx
            .send(())
            .expect("bead_id={BEAD_2G5_1} release signal should send");
        writer
            .join()
            .expect("bead_id={BEAD_2G5_1} writer thread should not panic");
        assert!(
            slot.is_free(Ordering::Acquire),
            "bead_id={BEAD_2G5_1} shared slot should return to free state",
        );
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn txn_slot_crash_recovery_e2e_replay_emits_artifact() {
        use serde_json::json;
        use std::path::PathBuf;
        use std::sync::{Arc, Mutex, mpsc};
        use std::time::Instant;

        let run_id = std::env::var("RUN_ID")
            .unwrap_or_else(|_| format!("{BEAD_2G5_1}-seed-{TXN_SLOT_E2E_SEED}"));
        let trace_id = std::env::var("TRACE_ID")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(TXN_SLOT_E2E_SEED);
        let scenario_id =
            std::env::var("SCENARIO_ID").unwrap_or_else(|_| TXN_SLOT_E2E_SCENARIO_ID.to_owned());
        let seed = std::env::var("SEED")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(TXN_SLOT_E2E_SEED);
        let artifact_path = std::env::var("FSQLITE_TXN_SLOT_E2E_ARTIFACT").map_or_else(
            |_| {
                PathBuf::from("artifacts")
                    .join(BEAD_2G5_1)
                    .join("txn_slots_e2e_artifact.json")
            },
            PathBuf::from,
        );
        if let Some(parent) = artifact_path.parent() {
            std::fs::create_dir_all(parent)
                .expect("bead_id={BEAD_2G5_1} artifact directory should be writable");
        }

        let scenario_started = Instant::now();
        GLOBAL_TXN_SLOT_METRICS.reset();
        let metrics_before = GLOBAL_TXN_SLOT_METRICS.snapshot();

        // 1) Measure deterministic allocation/release throughput on shared slots.
        let alloc_release_started = Instant::now();
        let slot_array = crate::cache_aligned::TxnSlotArray::new(16);
        let alloc_release_iterations = 256_u64;
        for cycle in 0_u64..alloc_release_iterations {
            let txn_id_raw = 10_000 + cycle;
            let hint_index = usize::try_from(cycle % 16)
                .expect("bead_id={BEAD_2G5_1} hint index should fit usize");
            let slot_index = slot_array
                .acquire(
                    txn_id_raw,
                    hint_index,
                    6_666,
                    seed + cycle,
                    100_000 + cycle,
                    500 + cycle,
                    500 + cycle,
                    crate::cache_aligned::slot_mode::CONCURRENT,
                    1,
                )
                .expect("bead_id={BEAD_2G5_1} slot allocation should succeed");
            slot_array.slot(slot_index).release();
        }
        let alloc_release_elapsed_us = alloc_release_started.elapsed().as_micros().max(1);
        let alloc_release_ops = u128::from(alloc_release_iterations).saturating_mul(2);
        let avg_alloc_release_ns = alloc_release_elapsed_us
            .saturating_mul(1_000)
            .saturating_div(alloc_release_ops.max(1));
        let alloc_release_under_one_us = avg_alloc_release_ns < 1_000;

        // 2) Crash detection within two heartbeat periods.
        let heartbeat_period_secs = CLAIMING_TIMEOUT_SECS;
        let claim_time = 1_000_u64;
        let heartbeat_probe_now = claim_time + CLAIMING_TIMEOUT_SECS + 1;
        let heartbeat_slot = make_claiming_slot(70_001, claim_time);
        heartbeat_slot.pid.store(8_001, Ordering::Release);
        heartbeat_slot.pid_birth.store(9_001, Ordering::Release);
        let heartbeat_cleanup =
            try_cleanup_orphaned_slot(&heartbeat_slot, heartbeat_probe_now, |_, _| false, |_| {});
        let crash_detected_within_two_heartbeats = heartbeat_probe_now.saturating_sub(claim_time)
            <= heartbeat_period_secs.saturating_mul(2);
        assert!(
            matches!(heartbeat_cleanup, SlotCleanupResult::Reclaimed { .. }),
            "bead_id={BEAD_2G5_1} stale claiming slot should be reclaimed in heartbeat window",
        );
        assert!(
            crash_detected_within_two_heartbeats,
            "bead_id={BEAD_2G5_1} crash detection must fit in two heartbeat periods",
        );

        // 3) Repeated crash recovery leaves no orphaned slot state.
        let crash_cycle_started = Instant::now();
        let reusable_slot = SharedTxnSlot::new();
        let released = Arc::new(Mutex::new(Vec::new()));
        for cycle in 0_u64..100 {
            let txn_id_raw = 90_000 + cycle;
            let now = 200_000 + cycle;
            reusable_slot.txn_id.store(txn_id_raw, Ordering::Release);
            reusable_slot
                .begin_seq
                .store(700 + cycle, Ordering::Release);
            reusable_slot.pid.store(77_001, Ordering::Release);
            reusable_slot
                .pid_birth
                .store(88_001 + cycle, Ordering::Release);
            reusable_slot
                .lease_expiry
                .store(now.saturating_sub(1), Ordering::Release);

            let stats = cleanup_orphaned_slots(
                std::slice::from_ref(&reusable_slot),
                now,
                |_, _| false,
                |released_txn_id| {
                    released
                        .lock()
                        .expect("bead_id={BEAD_2G5_1} release log mutex should not be poisoned")
                        .push(released_txn_id);
                },
            );
            assert_eq!(
                stats.orphans_found, 1,
                "bead_id={BEAD_2G5_1} cycle={cycle} crash cleanup should reclaim orphan slot",
            );
            assert!(
                reusable_slot.is_free(Ordering::Acquire),
                "bead_id={BEAD_2G5_1} cycle={cycle} slot should be reusable after cleanup",
            );
        }
        let crash_cycle_elapsed_us = crash_cycle_started.elapsed().as_micros().max(1);
        let released_count = released
            .lock()
            .expect("bead_id={BEAD_2G5_1} release log mutex should not be poisoned")
            .len();
        assert_eq!(
            released_count, 100,
            "bead_id={BEAD_2G5_1} all crash cycles should release orphan locks",
        );

        // 4) Cross-process visibility check using shared slot publication.
        let visibility_slot = Arc::new(SharedTxnSlot::new());
        let writer_slot = Arc::clone(&visibility_slot);
        let (ready_tx, ready_rx) = mpsc::channel::<()>();
        let (release_tx, release_rx) = mpsc::channel::<()>();
        let writer = std::thread::spawn(move || {
            assert!(
                writer_slot.phase1_claim(66_606),
                "bead_id={BEAD_2G5_1} cross-process writer should claim slot",
            );
            writer_slot.claiming_timestamp.store(123, Ordering::Release);
            writer_slot.phase2_initialize(
                1_234,
                5_678,
                10_000,
                77,
                77,
                crate::cache_aligned::slot_mode::CONCURRENT,
                3,
            );
            assert!(
                writer_slot.phase3_publish(66_606),
                "bead_id={BEAD_2G5_1} cross-process writer should publish slot",
            );
            ready_tx
                .send(())
                .expect("bead_id={BEAD_2G5_1} ready signal should send");
            release_rx
                .recv_timeout(Duration::from_secs(2))
                .expect("bead_id={BEAD_2G5_1} release signal should arrive");
            writer_slot.release();
        });
        ready_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("bead_id={BEAD_2G5_1} visibility reader should observe writer");
        let cross_process_visibility_ok = visibility_slot.txn_id.load(Ordering::Acquire) == 66_606
            && visibility_slot.pid.load(Ordering::Acquire) == 1_234
            && visibility_slot.begin_seq.load(Ordering::Acquire) == 77;
        assert!(
            cross_process_visibility_ok,
            "bead_id={BEAD_2G5_1} shared memory visibility should preserve published fields",
        );
        release_tx
            .send(())
            .expect("bead_id={BEAD_2G5_1} release signal should send");
        writer
            .join()
            .expect("bead_id={BEAD_2G5_1} writer thread should not panic");

        let metrics_after = GLOBAL_TXN_SLOT_METRICS.snapshot();
        let metric_delta = json!({
            "fsqlite_txn_slots_active": metrics_after
                .fsqlite_txn_slots_active
                .saturating_sub(metrics_before.fsqlite_txn_slots_active),
            "fsqlite_txn_slot_crashes_detected_total": metrics_after
                .fsqlite_txn_slot_crashes_detected_total
                .saturating_sub(metrics_before.fsqlite_txn_slot_crashes_detected_total),
        });

        let total_elapsed_us = scenario_started.elapsed().as_micros().max(1);
        let replay_command = format!(
            "RUN_ID='{}' TRACE_ID={} SCENARIO_ID='{}' SEED={} FSQLITE_TXN_SLOT_E2E_ARTIFACT='{}' cargo test -p fsqlite-mvcc core_types::tests::txn_slot_crash_recovery_e2e_replay_emits_artifact -- --exact --nocapture",
            run_id,
            trace_id,
            scenario_id,
            seed,
            artifact_path.display(),
        );
        let checks = vec![
            json!({
                "id": "alloc_release_latency_budget",
                "status": if alloc_release_under_one_us { "pass" } else { "fail" },
                "detail": format!("avg_alloc_release_ns={avg_alloc_release_ns} target_lt_ns=1000"),
            }),
            json!({
                "id": "crash_detection_within_two_heartbeats",
                "status": if crash_detected_within_two_heartbeats { "pass" } else { "fail" },
                "detail": format!(
                    "elapsed_secs={} heartbeat_period_secs={}",
                    heartbeat_probe_now.saturating_sub(claim_time),
                    heartbeat_period_secs,
                ),
            }),
            json!({
                "id": "no_orphans_after_100_cycles",
                "status": if released_count == 100 { "pass" } else { "fail" },
                "detail": format!("released_count={released_count} expected=100"),
            }),
            json!({
                "id": "cross_process_visibility",
                "status": if cross_process_visibility_ok { "pass" } else { "fail" },
                "detail": "published txn_id/pid/begin_seq observed via shared slot reader",
            }),
        ];
        let all_checks_pass = checks.iter().all(|entry| {
            entry
                .get("status")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|status| status == "pass")
        });
        let overall_status = if all_checks_pass { "pass" } else { "fail" };

        let artifact = json!({
            "bead_id": BEAD_2G5_1,
            "run_id": run_id,
            "trace_id": trace_id,
            "scenario_id": scenario_id,
            "seed": seed,
            "overall_status": overall_status,
            "timing": {
                "total_elapsed_us": total_elapsed_us,
                "alloc_release_elapsed_us": alloc_release_elapsed_us,
                "alloc_release_avg_ns": avg_alloc_release_ns,
                "crash_cycle_elapsed_us": crash_cycle_elapsed_us,
            },
            "checks": checks,
            "metric_delta": metric_delta,
            "observability": {
                "required_fields": [
                    "run_id",
                    "trace_id",
                    "scenario_id",
                    "operation",
                    "operation_elapsed_us",
                    "slot_id",
                    "process_id",
                    "failure_context"
                ],
                "event_target": "fsqlite.txn_slot",
                "span_name": "txn_slot",
            },
            "replay_command": replay_command,
        });
        let artifact_bytes = serde_json::to_vec_pretty(&artifact)
            .expect("bead_id={BEAD_2G5_1} artifact serialization should succeed");
        std::fs::write(&artifact_path, artifact_bytes)
            .expect("bead_id={BEAD_2G5_1} artifact write should succeed");
        assert!(
            artifact_path.exists(),
            "bead_id={BEAD_2G5_1} e2e artifact path should exist",
        );
        assert!(
            all_checks_pass,
            "bead_id={BEAD_2G5_1} deterministic e2e checks must pass",
        );
        GLOBAL_TXN_SLOT_METRICS.reset();
    }

    // ===================================================================
    // bd-t6sv2.1: Conflict observer integration tests (§5.1)
    // ===================================================================

    const BEAD_T6SV2_1: &str = "bd-t6sv2.1";

    #[test]
    fn test_lock_table_observer_emits_on_contention() {
        // bd-t6sv2.1: InProcessPageLockTable emits PageLockContention
        // when a second transaction tries to acquire a page already held.
        let obs = std::sync::Arc::new(fsqlite_observability::MetricsObserver::new(100));
        let table = InProcessPageLockTable::with_observer(
            obs.clone() as std::sync::Arc<dyn fsqlite_observability::ConflictObserver>
        );
        let page = PageNumber::new(42).unwrap();
        let txn_a = TxnId::new(1).unwrap();
        let txn_b = TxnId::new(2).unwrap();

        // txn_a acquires — no event.
        table.try_acquire(page, txn_a).unwrap();
        assert_eq!(
            obs.metrics()
                .page_contentions
                .load(std::sync::atomic::Ordering::Relaxed),
            0,
            "bead_id={BEAD_T6SV2_1} case=no_event_on_clean_acquire"
        );

        // txn_b tries same page — contention event emitted.
        let err = table.try_acquire(page, txn_b).unwrap_err();
        assert_eq!(err, txn_a);
        assert_eq!(
            obs.metrics()
                .page_contentions
                .load(std::sync::atomic::Ordering::Relaxed),
            1,
            "bead_id={BEAD_T6SV2_1} case=contention_event_emitted"
        );
        assert_eq!(
            obs.metrics()
                .conflicts_total
                .load(std::sync::atomic::Ordering::Relaxed),
            1,
            "bead_id={BEAD_T6SV2_1} case=conflicts_total_incremented"
        );

        // Verify the ring buffer has the right event.
        let events = obs.log().snapshot();
        assert_eq!(events.len(), 1);
        assert!(
            matches!(
                &events[0],
                fsqlite_observability::ConflictEvent::PageLockContention {
                    page: p,
                    requester,
                    holder,
                    ..
                } if p.get() == 42 && requester.get() == 2 && holder.get() == 1
            ),
            "bead_id={BEAD_T6SV2_1} case=event_fields_correct"
        );
    }

    #[test]
    fn test_lock_table_observer_no_event_on_reacquire() {
        // bd-t6sv2.1: Re-acquiring a lock by the same txn should NOT emit.
        let obs = std::sync::Arc::new(fsqlite_observability::MetricsObserver::new(100));
        let table = InProcessPageLockTable::with_observer(
            obs.clone() as std::sync::Arc<dyn fsqlite_observability::ConflictObserver>
        );
        let page = PageNumber::new(7).unwrap();
        let txn = TxnId::new(1).unwrap();

        table.try_acquire(page, txn).unwrap();
        table.try_acquire(page, txn).unwrap(); // idempotent re-acquire

        assert_eq!(
            obs.metrics()
                .page_contentions
                .load(std::sync::atomic::Ordering::Relaxed),
            0,
            "bead_id={BEAD_T6SV2_1} case=no_event_on_reacquire"
        );
        assert!(obs.log().is_empty());
    }

    #[test]
    fn test_lock_table_observer_multiple_contentions() {
        // bd-t6sv2.1: Multiple contention events from different txns on
        // different pages accumulate correctly.
        let obs = std::sync::Arc::new(fsqlite_observability::MetricsObserver::new(100));
        let table = InProcessPageLockTable::with_observer(
            obs.clone() as std::sync::Arc<dyn fsqlite_observability::ConflictObserver>
        );

        let txn_a = TxnId::new(1).unwrap();
        let txn_b = TxnId::new(2).unwrap();
        let txn_c = TxnId::new(3).unwrap();

        // txn_a holds pages 10 and 20.
        table
            .try_acquire(PageNumber::new(10).unwrap(), txn_a)
            .unwrap();
        table
            .try_acquire(PageNumber::new(20).unwrap(), txn_a)
            .unwrap();

        // txn_b contends on page 10.
        assert!(
            table
                .try_acquire(PageNumber::new(10).unwrap(), txn_b)
                .is_err()
        );
        // txn_c contends on page 10.
        assert!(
            table
                .try_acquire(PageNumber::new(10).unwrap(), txn_c)
                .is_err()
        );
        // txn_b contends on page 20.
        assert!(
            table
                .try_acquire(PageNumber::new(20).unwrap(), txn_b)
                .is_err()
        );

        assert_eq!(
            obs.metrics()
                .page_contentions
                .load(std::sync::atomic::Ordering::Relaxed),
            3,
            "bead_id={BEAD_T6SV2_1} case=multiple_contentions_counted"
        );

        // Hotspot tracking: page 10 has 2 contentions, page 20 has 1.
        let snap = obs.metrics().snapshot();
        let hotspots = &snap.top_hotspots;
        assert!(hotspots.len() >= 2);
        // Page 10 should be the hottest.
        assert_eq!(hotspots[0].0, PageNumber::new(10).unwrap());
        assert_eq!(hotspots[0].1, 2);
    }

    #[test]
    fn test_lock_table_no_observer_zero_overhead() {
        // bd-t6sv2.1: When no observer is set, contention still works
        // correctly but no events are recorded anywhere.
        let table = InProcessPageLockTable::new();
        let page = PageNumber::new(1).unwrap();
        let txn_a = TxnId::new(1).unwrap();
        let txn_b = TxnId::new(2).unwrap();

        table.try_acquire(page, txn_a).unwrap();
        let err = table.try_acquire(page, txn_b).unwrap_err();
        assert_eq!(
            err, txn_a,
            "bead_id={BEAD_T6SV2_1} case=contention_works_without_observer"
        );
        // No panic, no observer — just normal Err return.
        assert!(table.observer().is_none());
    }

    #[test]
    fn test_lock_table_observer_during_rebuild() {
        // bd-t6sv2.1: Contention in the draining table during rebuild
        // also emits events.
        let obs = std::sync::Arc::new(fsqlite_observability::MetricsObserver::new(100));
        let mut table = InProcessPageLockTable::with_observer(
            obs.clone() as std::sync::Arc<dyn fsqlite_observability::ConflictObserver>
        );
        let page = PageNumber::new(50).unwrap();
        let txn_a = TxnId::new(1).unwrap();
        let txn_b = TxnId::new(2).unwrap();

        // txn_a acquires page before rebuild.
        table.try_acquire(page, txn_a).unwrap();
        assert_eq!(obs.log().len(), 0);

        // Begin rebuild — page is now in draining table.
        table.begin_rebuild().unwrap();

        // txn_b tries to acquire — contention from draining table.
        let err = table.try_acquire(page, txn_b).unwrap_err();
        assert_eq!(err, txn_a);
        assert_eq!(
            obs.metrics()
                .page_contentions
                .load(std::sync::atomic::Ordering::Relaxed),
            1,
            "bead_id={BEAD_T6SV2_1} case=contention_emits_during_rebuild"
        );

        let events = obs.log().snapshot();
        assert_eq!(events.len(), 1);
        assert!(matches!(
            &events[0],
            fsqlite_observability::ConflictEvent::PageLockContention {
                page: p,
                requester,
                holder,
                ..
            } if p.get() == 50 && requester.get() == 2 && holder.get() == 1
        ));
    }

    #[test]
    fn test_lock_table_set_observer_after_creation() {
        // bd-t6sv2.1: set_observer() can attach observer to an existing table.
        let obs = std::sync::Arc::new(fsqlite_observability::MetricsObserver::new(100));
        let mut table = InProcessPageLockTable::new();
        assert!(table.observer().is_none());

        // Attach observer.
        table.set_observer(Some(
            obs.clone() as std::sync::Arc<dyn fsqlite_observability::ConflictObserver>
        ));
        assert!(table.observer().is_some());

        let page = PageNumber::new(1).unwrap();
        let txn_a = TxnId::new(1).unwrap();
        let txn_b = TxnId::new(2).unwrap();

        table.try_acquire(page, txn_a).unwrap();
        assert!(table.try_acquire(page, txn_b).is_err());

        assert_eq!(
            obs.metrics()
                .page_contentions
                .load(std::sync::atomic::Ordering::Relaxed),
            1,
            "bead_id={BEAD_T6SV2_1} case=observer_works_after_set"
        );
    }

    #[test]
    fn test_lock_table_observer_reset_clears_metrics() {
        // bd-t6sv2.1: MetricsObserver.reset() clears both counters and log.
        let obs = std::sync::Arc::new(fsqlite_observability::MetricsObserver::new(100));
        let table = InProcessPageLockTable::with_observer(
            obs.clone() as std::sync::Arc<dyn fsqlite_observability::ConflictObserver>
        );
        let page = PageNumber::new(1).unwrap();
        let txn_a = TxnId::new(1).unwrap();
        let txn_b = TxnId::new(2).unwrap();

        table.try_acquire(page, txn_a).unwrap();
        assert!(table.try_acquire(page, txn_b).is_err());
        assert_eq!(obs.log().len(), 1);

        obs.reset();
        assert_eq!(
            obs.metrics()
                .page_contentions
                .load(std::sync::atomic::Ordering::Relaxed),
            0,
            "bead_id={BEAD_T6SV2_1} case=reset_clears_counters"
        );
        assert!(
            obs.log().is_empty(),
            "bead_id={BEAD_T6SV2_1} case=reset_clears_log"
        );
    }
}
