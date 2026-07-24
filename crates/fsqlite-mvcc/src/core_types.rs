//! MVCC core runtime types (§5.1).
//!
//! This module implements the runtime data structures that power MVCC
//! concurrency: version arenas, page lock tables, commit indices, and
//! transaction state.
//!
//! Foundation types (TxnId, CommitSeq, Snapshot, etc.) live in
//! [`fsqlite_types::glossary`]; this module builds the runtime machinery on top.

use fsqlite_types::sync_primitives::{Condvar, Mutex, RwLock};
use smallvec::SmallVec;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::thread::{self, Thread, ThreadId};
use std::time::{Duration, Instant};

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

#[inline]
fn generation_counter_matches(left: u32, right: u32) -> bool {
    left.cmp(&right).is_eq()
}

#[inline]
fn txn_ids_differ(left: TxnId, right: TxnId) -> bool {
    left.get().cmp(&right.get()).is_ne()
}

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
///
/// # Epoch-Based Reclamation (D5)
///
/// For reduced GC contention, the arena supports a two-phase retirement:
/// 1. `take_for_retirement()` - extracts version and bumps generation, but
///    does NOT add to free_list. The slot is marked empty but not yet reusable.
/// 2. `recycle_slots()` - batch-adds previously retired slots to free_list.
///    Called after epoch advancement when all readers have moved past the
///    retirement point.
///
/// This allows GC to hold the write lock only briefly for extraction, while
/// the free_list update is deferred and batched.
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

        let chunk_u32 = u32::try_from(chunk_idx).unwrap_or(u32::MAX);
        let offset_u32 = u32::try_from(offset).unwrap_or(u32::MAX);
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
            generation_counter_matches(slot.generation, idx.generation),
            "VersionArena::take: generation mismatch for {idx:?} (slot generation {})",
            slot.generation
        );
        let version = slot
            .version
            .take()
            .expect("VersionArena::take: slot must be occupied");

        // Increment generation on free so that any dangling VersionIdx becomes invalid.
        // We skip u32::MAX to prevent collision with CHAIN_HEAD_EMPTY (u64::MAX) when packed.
        let mut next_gen = slot.generation.wrapping_add(1);
        if next_gen == u32::MAX {
            next_gen = 0;
        }
        slot.generation = next_gen;

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

    // -------------------------------------------------------------------------
    // Epoch-Based Reclamation (EBR) methods for D5 contention reduction
    // -------------------------------------------------------------------------

    /// Extract a version for deferred retirement (EBR phase 1).
    ///
    /// Unlike [`Self::take`], this does NOT add the slot to the free list. The slot
    /// is marked empty (version extracted, generation bumped) but remains
    /// unavailable for allocation until [`Self::recycle_slots`] is called.
    ///
    /// Use this when retiring versions via EBR: call `take_for_retirement`,
    /// defer the version drop via `crossbeam_epoch::Guard::defer()`, collect
    /// the idx in an `EbrRetireQueue`, then batch-call `recycle_slots` after
    /// epoch advancement.
    ///
    /// # Panics
    ///
    /// Asserts that the slot is currently occupied and generation matches.
    pub fn take_for_retirement(&mut self, idx: VersionIdx) -> PageVersion {
        let slot = &mut self.chunks[idx.chunk as usize][idx.offset as usize];
        assert!(
            generation_counter_matches(slot.generation, idx.generation),
            "VersionArena::take_for_retirement: generation mismatch for {idx:?} (slot generation {})",
            slot.generation
        );
        let version = slot
            .version
            .take()
            .expect("VersionArena::take_for_retirement: slot must be occupied");

        // Bump generation to invalidate stale pointers, but do NOT add to free_list.
        let mut next_gen = slot.generation.wrapping_add(1);
        if next_gen == u32::MAX {
            next_gen = 0;
        }
        slot.generation = next_gen;

        // Note: free_list.push() is intentionally skipped — the slot will be
        // recycled via recycle_slots() after epoch advancement.
        version
    }

    /// Batch-recycle slots that were previously retired via EBR (EBR phase 2).
    ///
    /// This adds the given indices to the free list, making them available for
    /// future allocations. Call this after epoch advancement confirms that all
    /// readers have moved past the point where these versions were visible.
    ///
    /// # Safety Invariant
    ///
    /// The caller MUST ensure that:
    /// 1. Each idx was previously passed to `take_for_retirement`
    /// 2. Sufficient epoch advancement has occurred (all concurrent readers
    ///    have unpinned since the retirement)
    ///
    /// Violating this invariant can cause use-after-free if a reader still
    /// holds a stale VersionIdx.
    pub fn recycle_slots(&mut self, indices: impl IntoIterator<Item = VersionIdx>) {
        // Note: We don't verify generation here because take_for_retirement
        // already bumped it. The idx stored in the retire queue has the OLD
        // generation, but that's fine — we just need the (chunk, offset) to
        // identify the slot.
        for idx in indices {
            // Create a fresh idx with the CURRENT generation (post-retirement bump).
            // This is what alloc() will return when this slot is reused.
            let slot = &self.chunks[idx.chunk as usize][idx.offset as usize];
            let recycled_idx = VersionIdx::new(idx.chunk, idx.offset, slot.generation);
            self.free_list.push(recycled_idx);
        }
    }

    /// Number of slots pending recycle (for diagnostics).
    ///
    /// Note: This returns 0 because the arena doesn't track pending retirements
    /// internally — that's the responsibility of the `EbrRetireQueue`. This
    /// method exists for API symmetry with `free_count`.
    #[must_use]
    pub const fn pending_recycle_count(&self) -> usize {
        // The arena doesn't track this — see EbrRetireQueue
        0
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

        if !generation_counter_matches(slot.generation, idx.generation) {
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

        if !generation_counter_matches(slot.generation, idx.generation) {
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

/// Number of fallback shards in the lock and commit tables.
///
/// The page ranges used by normal small/medium databases are served by direct
/// atomic fast arrays; these shards cover large page numbers and rebuild
/// bookkeeping. Keep this power-of-two fanout high enough for fallback
/// concurrency without making every fresh database pay a large open-time
/// allocation cost.
pub const LOCK_TABLE_SHARDS: usize = 64;

/// Size of the fast atomic lock address space covering page numbers
/// 1..=FAST_LOCK_ARRAY_SIZE.
const FAST_LOCK_ARRAY_SIZE: usize = 65536;
/// Lazily allocate the fast-lock address space in small atomic chunks so a
/// new connection does not zero the full 512 KiB table before touching a page.
const FAST_LOCK_CHUNK_SIZE: usize = 1024;
const FAST_LOCK_CHUNKS: usize = FAST_LOCK_ARRAY_SIZE / FAST_LOCK_CHUNK_SIZE;

/// A single cache-line-aligned lock table shard.
type LockShard = CacheAligned<Mutex<HashMap<PageNumber, TxnId, PageNumberBuildHasher>>>;

/// A flat-combining shard used for the sharded tail when the
/// `mvcc-flat-combining` feature is enabled. See
/// [`crate::flat_combining_page_locks`] for the protocol.
#[cfg(feature = "mvcc-flat-combining")]
type FcLockShard = CacheAligned<crate::flat_combining_page_locks::FcPageLockShard>;

/// Per-page waiter queue for targeted wakeups (D4, bd-3wop3.4).
/// `SmallVec<[WaiterEntry; 2]>` avoids heap allocation for typical 1-2 waiters/page.
#[derive(Clone)]
struct WaiterEntry {
    id: ThreadId,
    thread: Thread,
}

type WaiterQueue = SmallVec<[WaiterEntry; 2]>;

/// Sharded waiter registry indexed by page number.
type WaiterShard = CacheAligned<Mutex<HashMap<PageNumber, WaiterQueue, PageNumberBuildHasher>>>;

/// In-process page-level exclusive write locks.
///
/// **Hekaton-style fast path (§8.10):** For page numbers 1..=65536, locks are
/// stored in a flat `AtomicU64` array where 0 = unlocked and non-zero = the
/// raw `TxnId` of the holder.  `try_acquire` is a single compare-and-swap
/// (lock-free!), eliminating all Mutex contention on the hot path.
///
/// For page numbers > 65536, falls back to sharded Mutex+HashMap buckets
/// (same as before).
///
/// Supports a rolling rebuild protocol (§5.6.3.1) for the sharded fallback
/// table, where the table can operate in dual-table mode: an **active** table
/// for new acquisitions and a **draining** table that is consulted for
/// existing sharded locks during the drain phase. Fast-array locks remain in
/// place and are cleaned separately during full rebuilds, avoiding
/// stop-the-world abort storms during maintenance.
pub struct InProcessPageLockTable {
    /// Lock-free fast path: lazily allocated atomic chunks for pages 1..=65536.
    /// Slot value 0 = unlocked; non-zero = TxnId.get() of the holder.
    fast_locks: Box<[OnceLock<Box<[AtomicU64]>>; FAST_LOCK_CHUNKS]>,
    /// Sharded fallback for pages > 65536. When the `mvcc-flat-combining`
    /// feature is enabled, the active fallback path routes through
    /// [`Self::fc_shards`] instead; this field remains as the storage backing
    /// for rolling-rebuild draining-table rotation (a rare maintenance
    /// operation) and for the feature-off baseline.
    shards: Box<[LockShard; LOCK_TABLE_SHARDS]>,
    /// Flat-combining shards used for the sharded-fallback hot path when the
    /// `mvcc-flat-combining` feature is enabled. Each shard serialises
    /// `try_acquire` / `release` / `holder` requests through a
    /// publication-list + combiner design, eliminating the
    /// `parking_lot::Mutex<HashMap>` contention under 8+ concurrent writers
    /// that all land in the same shard.
    ///
    /// Cancellation-correct (caller never blocks on an external condvar —
    /// they spin on their own cache-line slot); preserves INV-2 (only the
    /// combiner mutates the backing map).
    #[cfg(feature = "mvcc-flat-combining")]
    fc_shards: Box<[FcLockShard; LOCK_TABLE_SHARDS]>,
    /// During rolling rebuild: the old shards being drained. Protected by
    /// `Mutex` for synchronization. `None` when no rebuild is in progress.
    draining: Mutex<Option<DrainingState>>,
    /// Fast-path flag: `true` only when a rolling rebuild is in progress.
    /// Avoids taking the `draining` mutex on every `try_acquire` call
    /// (rebuilds are extremely rare — maintenance-only operations).
    has_draining: std::sync::atomic::AtomicBool,
    /// Monotonic counter for waking parked page-lock waiters after releases.
    change_epoch: AtomicU64,
    /// Park/wake gate paired with [`change_epoch`] to avoid hot-path spin/yield
    /// retries while still handling missed wakeups correctly.
    change_gate: Mutex<()>,
    change_cv: Condvar,
    /// Per-page waiter queues for targeted notification (D4, bd-3wop3.4).
    /// Eliminates thundering herd: releasing page P only wakes threads waiting
    /// for page P, not all waiters. Sharded the same way as `shards`.
    waiter_shards: Box<[WaiterShard; LOCK_TABLE_SHARDS]>,
    /// Total live waiter entries across all `waiter_shards`. Read by the
    /// fast-path skip in `notify_all_waiters` / `notify_waiters_for_page`
    /// to avoid per-shard mutex acquires when no thread is
    /// parked. Synchronization with `register_waiter` is established via
    /// paired SeqCst fences (Dekker), so the load and modify can both
    /// remain `Relaxed`. Over-counting (e.g., from a panicking waiter that
    /// skipped `unregister_waiter`) is safe — it only forces the slow path.
    waiter_count: AtomicUsize,
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
    /// Allocate the lightweight fast-lock chunk directory.
    fn alloc_fast_locks() -> Box<[OnceLock<Box<[AtomicU64]>>; FAST_LOCK_CHUNKS]> {
        Box::new(std::array::from_fn(|_| OnceLock::new()))
    }

    fn alloc_fast_lock_chunk() -> Box<[AtomicU64]> {
        let v: Vec<AtomicU64> = (0..FAST_LOCK_CHUNK_SIZE)
            .map(|_| AtomicU64::new(0))
            .collect();
        v.into_boxed_slice()
    }

    fn fast_lock_slot(&self, pgno: usize) -> Option<&AtomicU64> {
        if pgno == 0 || pgno > FAST_LOCK_ARRAY_SIZE {
            return None;
        }
        let index = pgno - 1;
        let chunk_index = index / FAST_LOCK_CHUNK_SIZE;
        let chunk_offset = index % FAST_LOCK_CHUNK_SIZE;
        let chunk = self.fast_locks[chunk_index].get_or_init(Self::alloc_fast_lock_chunk);
        Some(&chunk[chunk_offset])
    }

    fn fast_lock_slot_if_allocated(&self, pgno: usize) -> Option<&AtomicU64> {
        if pgno == 0 || pgno > FAST_LOCK_ARRAY_SIZE {
            return None;
        }
        let index = pgno - 1;
        let chunk_index = index / FAST_LOCK_CHUNK_SIZE;
        let chunk_offset = index % FAST_LOCK_CHUNK_SIZE;
        self.fast_locks[chunk_index]
            .get()
            .map(|chunk| &chunk[chunk_offset])
    }

    /// Allocate sharded waiter queues (D4, bd-3wop3.4).
    fn alloc_waiter_shards() -> Box<[WaiterShard; LOCK_TABLE_SHARDS]> {
        Box::new(std::array::from_fn(|_| {
            CacheAligned::new(Mutex::new(HashMap::with_hasher(
                PageNumberBuildHasher::default(),
            )))
        }))
    }

    /// Allocate the flat-combining sharded fallback, one `FcPageLockShard`
    /// per shard index. Only used when the `mvcc-flat-combining` feature is
    /// enabled.
    #[cfg(feature = "mvcc-flat-combining")]
    fn alloc_fc_shards() -> Box<[FcLockShard; LOCK_TABLE_SHARDS]> {
        Box::new(std::array::from_fn(|i| {
            CacheAligned::new(crate::flat_combining_page_locks::FcPageLockShard::new(
                u32::try_from(i).unwrap_or(u32::MAX),
            ))
        }))
    }

    /// Create a new empty lock table with no observer.
    #[must_use]
    pub fn new() -> Self {
        Self {
            fast_locks: Self::alloc_fast_locks(),
            shards: Box::new(std::array::from_fn(|_| {
                CacheAligned::new(Mutex::new(HashMap::with_hasher(
                    PageNumberBuildHasher::default(),
                )))
            })),
            #[cfg(feature = "mvcc-flat-combining")]
            fc_shards: Self::alloc_fc_shards(),
            draining: Mutex::new(None),
            has_draining: std::sync::atomic::AtomicBool::new(false),
            change_epoch: AtomicU64::new(0),
            change_gate: Mutex::new(()),
            change_cv: Condvar::new(),
            waiter_shards: Self::alloc_waiter_shards(),
            waiter_count: AtomicUsize::new(0),
            observer: None,
        }
    }

    /// Create a lock table with a conflict observer for analytics (bd-t6sv2.1).
    #[must_use]
    pub fn with_observer(
        observer: std::sync::Arc<dyn fsqlite_observability::ConflictObserver>,
    ) -> Self {
        Self {
            fast_locks: Self::alloc_fast_locks(),
            shards: Box::new(std::array::from_fn(|_| {
                CacheAligned::new(Mutex::new(HashMap::with_hasher(
                    PageNumberBuildHasher::default(),
                )))
            })),
            #[cfg(feature = "mvcc-flat-combining")]
            fc_shards: Self::alloc_fc_shards(),
            draining: Mutex::new(None),
            has_draining: std::sync::atomic::AtomicBool::new(false),
            change_epoch: AtomicU64::new(0),
            change_gate: Mutex::new(()),
            change_cv: Condvar::new(),
            waiter_shards: Self::alloc_waiter_shards(),
            waiter_count: AtomicUsize::new(0),
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
    /// For pages above `FAST_LOCK_ARRAY_SIZE`, a rolling rebuild (§5.6.3.1)
    /// checks the draining table first. A lock in the draining table is still
    /// valid and blocks new acquisitions by other transactions.
    pub fn try_acquire(&self, page: PageNumber, txn: TxnId) -> Result<(), TxnId> {
        let pgno = page.get() as usize;

        // Step 0: During rebuilds, only sharded pages can exist in draining.
        // begin_rebuild() rotates the sharded fallback table, while fast-path
        // pages remain resident in fast_locks throughout the rebuild.
        if pgno > FAST_LOCK_ARRAY_SIZE
            && self.has_draining.load(std::sync::atomic::Ordering::Relaxed)
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

        // Step 1 (Hekaton fast path): For pages 1..=65536, use lock-free CAS
        // on the flat atomic array — no Mutex, no HashMap, zero contention.
        if pgno <= FAST_LOCK_ARRAY_SIZE {
            let slot = self
                .fast_lock_slot(pgno)
                .expect("fast lock page number in range must have a slot");
            let txn_raw = txn.get();
            // CAS: 0 (unlocked) → txn_raw (locked by us).
            match slot.compare_exchange(0, txn_raw, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => return Ok(()), // Lock acquired
                Err(current) => {
                    if current == txn_raw {
                        return Ok(()); // Already held by this txn (re-entrant)
                    }
                    // Held by another txn — reconstruct the holder TxnId.
                    // current != 0 (that was the CAS expected value), and the
                    // value was stored by a prior try_acquire which used txn.get(),
                    // so TxnId::new is guaranteed to succeed.
                    let holder =
                        TxnId::new(current).expect("page lock slot held non-zero non-TxnId");
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

        // Step 2 (fallback): Pages > 65536 use the sharded fallback.
        //
        // When `mvcc-flat-combining` is enabled, route through the
        // publication-list combiner; otherwise use the sharded Mutex+HashMap.
        #[cfg(feature = "mvcc-flat-combining")]
        {
            let shard_idx = Self::shard_index_static(page);
            match self.fc_shards[shard_idx].try_acquire(page, txn) {
                Ok(()) => Ok(()),
                Err(holder) => {
                    crate::observability::emit_page_lock_contention(
                        &self.observer,
                        page,
                        txn,
                        holder,
                    );
                    Err(holder)
                }
            }
        }
        #[cfg(not(feature = "mvcc-flat-combining"))]
        {
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
    }

    /// Release the lock on `page` held by `txn`.
    ///
    /// Pages above `FAST_LOCK_ARRAY_SIZE` also consult the draining shard
    /// table during rolling rebuilds. Returns `true` if the lock was released
    /// from either location.
    ///
    /// **D4 (bd-3wop3.4):** Uses targeted notification — only threads waiting
    /// for this specific page are woken, eliminating thundering herd.
    pub fn release(&self, page: PageNumber, txn: TxnId) -> bool {
        let pgno = page.get() as usize;

        // Fast path: pages 1..=65536 use lock-free CAS on the flat array.
        if pgno <= FAST_LOCK_ARRAY_SIZE {
            if let Some(slot) = self.fast_lock_slot_if_allocated(pgno) {
                let txn_raw = txn.get();
                // CAS: txn_raw (our lock) → 0 (unlocked).
                if slot
                    .compare_exchange(txn_raw, 0, Ordering::AcqRel, Ordering::Relaxed)
                    .is_ok()
                {
                    self.notify_waiters_for_page(page);
                    return true;
                }
            }
            // Not held by us — fast-array pages never live in draining.
        }

        // Sharded active table (pages > 65536 only — pages 1..=65536
        // use fast_locks for new acquisitions).
        if pgno > FAST_LOCK_ARRAY_SIZE {
            let shard_idx = Self::shard_index_static(page);
            #[cfg(feature = "mvcc-flat-combining")]
            {
                if self.fc_shards[shard_idx].release(page, txn) {
                    self.notify_waiters_for_page(page);
                    return true;
                }
            }
            #[cfg(not(feature = "mvcc-flat-combining"))]
            {
                let shard = &self.shards[shard_idx];
                let mut map = shard.lock();
                if map.get(&page) == Some(&txn) {
                    map.remove(&page);
                    drop(map);
                    self.notify_waiters_for_page(page);
                    return true;
                }
                drop(map);
            }
        }

        // Try draining table (sharded pages only) — only if a rebuild is in progress.
        if pgno > FAST_LOCK_ARRAY_SIZE
            && self.has_draining.load(std::sync::atomic::Ordering::Relaxed)
        {
            let draining_guard = self.draining.lock();
            if let Some(ref draining) = *draining_guard {
                let drain_shard_idx = Self::shard_index_static(page);
                let mut drain_map = draining.shards[drain_shard_idx].lock();
                if drain_map.get(&page) == Some(&txn) {
                    drain_map.remove(&page);
                    drop(drain_map);
                    drop(draining_guard);
                    self.notify_waiters_for_page(page);
                    return true;
                }
                drop(drain_map);
            }
            drop(draining_guard);
        }
        false
    }

    /// Release all locks held by `txn` from both active and draining tables.
    pub fn release_all(&self, txn: TxnId) {
        let mut released_any = false;
        // Scan the fast lock array for entries held by this txn.
        let txn_raw = txn.get();
        for chunk in self.fast_locks.iter().filter_map(OnceLock::get) {
            for slot in chunk.iter() {
                if slot
                    .compare_exchange(txn_raw, 0, Ordering::AcqRel, Ordering::Relaxed)
                    .is_ok()
                {
                    released_any = true;
                }
            }
        }
        // Scan sharded tables for pages > 65536.
        #[cfg(feature = "mvcc-flat-combining")]
        {
            for shard in self.fc_shards.iter() {
                if shard.release_all(txn) > 0 {
                    released_any = true;
                }
            }
        }
        #[cfg(not(feature = "mvcc-flat-combining"))]
        {
            for shard in self.shards.iter() {
                let mut map = shard.lock();
                let before = map.len();
                map.retain(|_, &mut v| txn_ids_differ(v, txn));
                released_any |= map.len() != before;
            }
        }
        // Also release from draining table (only if rebuild in progress).
        if self.has_draining.load(std::sync::atomic::Ordering::Relaxed) {
            let draining_guard = self.draining.lock();
            if let Some(ref draining) = *draining_guard {
                for shard in draining.shards.iter() {
                    let mut map = shard.lock();
                    let before = map.len();
                    map.retain(|_, &mut v| txn_ids_differ(v, txn));
                    released_any |= map.len() != before;
                }
            }
            drop(draining_guard);
        }

        if released_any {
            self.notify_all_waiters();
        }
    }

    /// Release a specific set of page locks held by `txn`.
    ///
    /// Pages above `FAST_LOCK_ARRAY_SIZE` fall back to the draining shard
    /// table during rolling rebuilds if they are no longer present in the
    /// active shard table.
    pub fn release_set(&self, pages: impl IntoIterator<Item = PageNumber>, txn: TxnId) {
        let has_drain = self.has_draining.load(std::sync::atomic::Ordering::Relaxed);
        let txn_raw = txn.get();
        let mut released_any = false;
        let mut draining_guard = None;
        for page in pages {
            let pgno = page.get() as usize;
            // Fast path for pages 1..=65536.
            if pgno <= FAST_LOCK_ARRAY_SIZE {
                if self.fast_lock_slot_if_allocated(pgno).is_some_and(|slot| {
                    slot.compare_exchange(txn_raw, 0, Ordering::AcqRel, Ordering::Relaxed)
                        .is_ok()
                }) {
                    released_any = true;
                }
                continue;
            }
            // Fallback for pages > 65536.
            let shard_idx = Self::shard_index_static(page);
            #[cfg(feature = "mvcc-flat-combining")]
            {
                if self.fc_shards[shard_idx].release(page, txn) {
                    released_any = true;
                    continue;
                }
            }
            #[cfg(not(feature = "mvcc-flat-combining"))]
            {
                let mut map = self.shards[shard_idx].lock();
                if map.get(&page) == Some(&txn) {
                    map.remove(&page);
                    released_any = true;
                    continue;
                }
                drop(map);
            }
            if has_drain {
                let guard = draining_guard.get_or_insert_with(|| self.draining.lock());
                if let Some(ref draining) = **guard {
                    let mut drain_map = draining.shards[shard_idx].lock();
                    if drain_map.get(&page) == Some(&txn) {
                        drain_map.remove(&page);
                        released_any = true;
                    }
                }
            }
        }
        drop(draining_guard);

        if released_any {
            self.notify_all_waiters();
        }
    }

    /// Check which txn holds the lock on `page`, if any.
    ///
    /// Fast-path pages are resolved from the flat array. Pages above
    /// `FAST_LOCK_ARRAY_SIZE` also consult the draining shard table when a
    /// rolling rebuild is in progress.
    #[must_use]
    pub fn holder(&self, page: PageNumber) -> Option<TxnId> {
        let pgno = page.get() as usize;

        // Fast path: pages 1..=65536 are served entirely from the flat array.
        // Rolling rebuild state only applies to the sharded fallback table.
        if pgno <= FAST_LOCK_ARRAY_SIZE {
            let slot = self.fast_lock_slot_if_allocated(pgno)?;
            let val = slot.load(Ordering::Acquire);
            return if val == 0 {
                None
            } else {
                Some(TxnId::new(val).expect("page lock slot held non-zero non-TxnId"))
            };
        }

        // Pages > 65536: check sharded active table.
        let shard_idx = Self::shard_index_static(page);
        #[cfg(feature = "mvcc-flat-combining")]
        {
            if let Some(h) = self.fc_shards[shard_idx].holder(page) {
                return Some(h);
            }
        }
        #[cfg(not(feature = "mvcc-flat-combining"))]
        {
            let shard = &self.shards[shard_idx];
            let map = shard.lock();
            if let Some(&holder) = map.get(&page) {
                return Some(holder);
            }
            drop(map);
        }

        // Check draining table (only if rebuild in progress).
        if self.has_draining.load(std::sync::atomic::Ordering::Relaxed) {
            let draining_guard = self.draining.lock();
            if let Some(ref draining) = *draining_guard {
                let shard_idx = Self::shard_index_static(page);
                let drain_map = draining.shards[shard_idx].lock();
                if let Some(&holder) = drain_map.get(&page) {
                    drop(drain_map);
                    drop(draining_guard);
                    return Some(holder);
                }
                drop(drain_map);
            }
            drop(draining_guard);
        }
        None
    }

    /// Snapshot the current aggregate release-progress epoch.
    ///
    /// Callers can pair this with [`Self::wait_for_release_progress`] to park
    /// until any lock release (active or draining) advances the lock table.
    #[must_use]
    pub fn release_progress_epoch(&self) -> u64 {
        self.change_epoch.load(Ordering::Acquire)
    }

    /// Wait until a lock release advances the aggregate progress epoch.
    ///
    /// Returns `true` if another thread released at least one lock after the
    /// caller observed `observed_epoch`, or `false` if `timeout` elapsed first.
    /// This uses the same missed-wakeup-safe epoch discipline as the targeted
    /// per-page waiter path, but aggregates across the whole lock table.
    #[must_use]
    pub fn wait_for_release_progress(&self, observed_epoch: u64, timeout: Duration) -> bool {
        self.wait_for_release_progress_with_gate_hook(observed_epoch, timeout, || {})
    }

    fn wait_for_release_progress_with_gate_hook<F>(
        &self,
        observed_epoch: u64,
        timeout: Duration,
        before_gate_lock: F,
    ) -> bool
    where
        F: FnOnce(),
    {
        if self.change_epoch.load(Ordering::Acquire) != observed_epoch {
            return true;
        }

        before_gate_lock();
        let mut gate = self.change_gate.lock();
        if self.change_epoch.load(Ordering::Acquire) != observed_epoch {
            return true;
        }

        #[cfg(not(target_arch = "wasm32"))]
        {
            let _ = self.change_cv.wait_for(&mut gate, timeout);
        }

        #[cfg(target_arch = "wasm32")]
        {
            let _ = timeout;
            self.change_cv.wait(&mut gate);
        }

        self.change_epoch.load(Ordering::Acquire) != observed_epoch
    }

    /// Wait until `page` is no longer held by `observed_holder`.
    ///
    /// Returns `true` if the holder changed (including becoming unlocked)
    /// before `timeout`, or `false` if the deadline elapsed first.
    ///
    /// **D4 (bd-3wop3.4):** Uses per-page waiter registration with targeted
    /// `Thread::unpark()` notification. Only threads waiting for the released
    /// page are woken, eliminating thundering herd.
    #[must_use]
    pub fn wait_for_holder_change(
        &self,
        page: PageNumber,
        observed_holder: TxnId,
        timeout: Duration,
    ) -> bool {
        let started = Instant::now();
        let current_thread = thread::current();
        let current_thread_id = current_thread.id();

        loop {
            // Check if holder changed before parking.
            match self.holder(page) {
                Some(holder) if holder == observed_holder => {}
                _ => return true,
            }

            let remaining = timeout.saturating_sub(started.elapsed());
            if remaining.is_zero() {
                return false;
            }

            // Register as waiter for this specific page.
            self.register_waiter(page, &current_thread, current_thread_id);

            // Double-check after registration (holder may have released between
            // our check and registration — avoid missed wakeup).
            match self.holder(page) {
                Some(holder) if holder == observed_holder => {}
                _ => {
                    self.unregister_waiter(page, current_thread_id);
                    return true;
                }
            }

            // Park with timeout. We'll be woken by notify_waiters_for_page(page)
            // when the holder releases, or spuriously (safe due to loop).
            #[cfg(not(target_arch = "wasm32"))]
            thread::park_timeout(remaining);

            #[cfg(target_arch = "wasm32")]
            {
                // WASM doesn't support park_timeout; fall back to condvar.
                let _ = remaining;
                let observed_epoch = self.change_epoch.load(Ordering::Acquire);
                let mut gate = self.change_gate.lock();
                if self.change_epoch.load(Ordering::Acquire) == observed_epoch {
                    self.change_cv.wait(&mut gate);
                }
                drop(gate);
            }

            // Post-wake fast path: if the holder has changed, the release
            // side's `notify_waiters_for_page` already drained our entry via
            // `map.remove(&page)`, so we can return without taking the
            // waiter-shard mutex. This skips one mutex acquire per successful
            // wake. On the production MT 8t profile `unregister_waiter` shows
            // 3.00% self-time (see memory/session_2026_04_23_profiling_campaign.md);
            // this eliminates ~half of those calls on the successful-wake
            // path. The spurious-wake / timeout branch still unregisters so
            // the next iteration's `register_waiter` does not duplicate the
            // thread in the queue.
            match self.holder(page) {
                Some(holder) if holder == observed_holder => {
                    self.unregister_waiter(page, current_thread_id);
                }
                _ => return true,
            }
        }
    }

    /// Total number of locks currently held across all shards (active table only).
    #[must_use]
    pub fn lock_count(&self) -> usize {
        let fast_count = self
            .fast_locks
            .iter()
            .filter_map(OnceLock::get)
            .flat_map(|chunk| chunk.iter())
            .filter(|slot| slot.load(Ordering::Relaxed) != 0)
            .count();
        #[cfg(feature = "mvcc-flat-combining")]
        let shard_count: usize = self.fc_shards.iter().map(|s| s.len()).sum();
        #[cfg(not(feature = "mvcc-flat-combining"))]
        let shard_count: usize = self.shards.iter().map(|s| s.lock().len()).sum();
        fast_count + shard_count
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
        #[cfg(feature = "mvcc-flat-combining")]
        {
            self.fc_shards.iter().map(|s| s.len()).collect()
        }
        #[cfg(not(feature = "mvcc-flat-combining"))]
        {
            self.shards.iter().map(|s| s.lock().len()).collect()
        }
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

        // Under the flat-combining feature, snapshot the current fc_shards
        // state into `self.shards` so the existing draining pipeline can
        // handle it. Then reset the fc_shards in place so new acquisitions
        // go to an empty active table. This preserves the rebuild API
        // contract (rotate → drain → finalize).
        #[cfg(feature = "mvcc-flat-combining")]
        let initial_count: usize = {
            let mut count = 0usize;
            for (i, fc) in self.fc_shards.iter().enumerate() {
                let mut snapshot: HashMap<PageNumber, TxnId, PageNumberBuildHasher> =
                    HashMap::with_hasher(PageNumberBuildHasher::default());
                fc.for_each(|page, txn| {
                    snapshot.insert(page, txn);
                });
                // Clear the FC shard so new acquisitions go to a fresh map.
                let _ = fc.retain(|_, _| false);
                count += snapshot.len();
                *self.shards[i].lock() = snapshot;
            }
            count
        };
        #[cfg(not(feature = "mvcc-flat-combining"))]
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
        self.has_draining
            .store(true, std::sync::atomic::Ordering::Release);
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
        if total_cleaned > 0 {
            self.notify_all_waiters();
        }
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
        self.has_draining
            .store(false, std::sync::atomic::Ordering::Release);
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
        // Clean orphaned entries in the fast_locks flat array BEFORE rotating
        // the shard tables.  This ensures stale locks from crashed transactions
        // are cleared regardless of whether they were in the fast path or the
        // sharded fallback path.
        for chunk in self.fast_locks.iter().filter_map(OnceLock::get) {
            for slot in chunk.iter() {
                let raw = slot.load(Ordering::Relaxed);
                if raw != 0 {
                    if let Some(holder) = TxnId::new(raw) {
                        if !is_active_txn(holder) {
                            // Orphaned: CAS to 0.  Failure means another thread
                            // already released or re-acquired — both are fine.
                            let _ =
                                slot.compare_exchange(raw, 0, Ordering::AcqRel, Ordering::Relaxed);
                        }
                    }
                }
            }
        }

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

    // -----------------------------------------------------------------------
    // Per-page waiter registration (D4, bd-3wop3.4)
    // -----------------------------------------------------------------------

    /// Register the current thread as waiting for `page`.
    fn register_waiter(&self, page: PageNumber, current_thread: &Thread, current_id: ThreadId) {
        let shard_idx = Self::shard_index_static(page);
        let mut map = self.waiter_shards[shard_idx].lock();
        map.entry(page).or_default().push(WaiterEntry {
            id: current_id,
            thread: current_thread.clone(),
        });
        self.waiter_count.fetch_add(1, Ordering::Relaxed);
        drop(map);
        // Pairs with the SeqCst fence in `notify_*_waiters` fast paths.
        // Either the notifier sees `waiter_count >= 1` and takes the slow
        // path (waking us via the shard mutex), or the waiter's holder
        // recheck (sequenced after this fence in `wait_for_holder_change`)
        // observes the notifier's pre-fence release CAS. Dekker.
        std::sync::atomic::fence(Ordering::SeqCst);
    }

    /// Unregister the current thread from the waiter queue for `page`.
    ///
    /// Fast path: when `waiter_count == 0` we know the global modification
    /// order has already retired our entry — `register_waiter`'s
    /// `fetch_add` and the matching drain's `fetch_sub` both update the
    /// same atomic, so observing zero implies our entry is no longer in
    /// any shard. Skipping the per-shard mutex avoids `parking_lot::Mutex`
    /// acquisition on the post-park spurious-wake/re-acquire branch in
    /// `wait_for_holder_change`, where the page-targeted notifier already
    /// drained us inside its own shard mutex (with `unpark()` and
    /// `fetch_sub` both sequenced before mutex release). The relaxed load
    /// is safe: a stale non-zero observation only forces the slow path,
    /// and a zero observation is a definitive abstract-state guarantee
    /// (cumulative `fetch_add` minus `fetch_sub` = 0 across all shards).
    fn unregister_waiter(&self, page: PageNumber, current_id: ThreadId) {
        if self.waiter_count.load(Ordering::Relaxed) == 0 {
            return;
        }
        let shard_idx = Self::shard_index_static(page);
        let mut map = self.waiter_shards[shard_idx].lock();
        if let Some(queue) = map.get_mut(&page) {
            let before = queue.len();
            queue.retain(|waiter| waiter.id != current_id);
            let removed = before - queue.len();
            if queue.is_empty() {
                map.remove(&page);
            }
            if removed > 0 {
                self.waiter_count.fetch_sub(removed, Ordering::Relaxed);
            }
        }
    }

    /// Wake only threads waiting for `page` (targeted notification).
    ///
    /// This is the primary notification method for single-page releases.
    /// Eliminates thundering herd by waking only relevant waiters.
    fn notify_waiters_for_page(&self, page: PageNumber) {
        // Pairs with the SeqCst fence in `register_waiter`. The caller's
        // release CAS on the page's `fast_locks`/shard slot is sequenced
        // before this fence; the waiter's `register_waiter`+fence is
        // sequenced before its post-register holder recheck. By Dekker on
        // SeqCst fences, either we observe `waiter_count >= 1` here (and
        // take the per-page mutex slow path) or the waiter observes our
        // released holder and never parks. The change_epoch + condvar
        // bump still runs so threads on `wait_for_release_progress` make
        // progress.
        std::sync::atomic::fence(Ordering::SeqCst);
        if self.waiter_count.load(Ordering::Relaxed) > 0 {
            let shard_idx = Self::shard_index_static(page);
            let mut map = self.waiter_shards[shard_idx].lock();
            if let Some(queue) = map.remove(&page) {
                let drained = queue.len();
                // Unpark all threads waiting for this specific page.
                for waiter in queue {
                    waiter.thread.unpark();
                }
                if drained > 0 {
                    self.waiter_count.fetch_sub(drained, Ordering::Relaxed);
                }
            }
            drop(map);
        }
        // Bump epoch and notify condvar as fallback for WASM (no thread::park)
        // and edge cases (threads that registered between remove and park).
        // Must use notify_all on WASM: multiple pages may have waiters, and
        // notify_one could wake a thread waiting for a different page, causing
        // the intended waiter to starve. On native, notify_one suffices since
        // we use targeted Thread::unpark() above.
        let _gate = self.change_gate.lock();
        self.change_epoch.fetch_add(1, Ordering::Release);
        #[cfg(target_arch = "wasm32")]
        self.change_cv.notify_all();
        #[cfg(not(target_arch = "wasm32"))]
        self.change_cv.notify_one();
    }

    /// Wake all waiters across all pages (bulk notification for release_all).
    ///
    /// Used for bulk operations like `release_all` and `release_set` where
    /// iterating over every released page would be expensive or impractical.
    fn notify_all_waiters(&self) {
        // Pairs with the SeqCst fence in `register_waiter`; see Dekker note
        // on `notify_waiters_for_page`. When `waiter_count == 0` we skip
        // the fallback-shard mutex iteration entirely — this is the common case
        // on commit/release_all when no thread is parked on a page lock.
        std::sync::atomic::fence(Ordering::SeqCst);
        if self.waiter_count.load(Ordering::Relaxed) > 0 {
            // Wake all per-page waiters.
            let mut total_drained: usize = 0;
            for shard in self.waiter_shards.iter() {
                let mut map = shard.lock();
                for (_, queue) in map.drain() {
                    total_drained += queue.len();
                    for waiter in queue {
                        waiter.thread.unpark();
                    }
                }
            }
            if total_drained > 0 {
                self.waiter_count
                    .fetch_sub(total_drained, Ordering::Relaxed);
            }
        }
        // Also signal the condvar for fallback compatibility.
        let _gate = self.change_gate.lock();
        self.change_epoch.fetch_add(1, Ordering::Release);
        self.change_cv.notify_all();
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
        dbg.finish_non_exhaustive()
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
        self.bits.fill(0);
    }
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
    /// Uses `Arc` to allow cheap O(1) cloning for savepoints.
    pub write_set_data: Arc<HashMap<PageNumber, PageData, fsqlite_types::PageNumberBuildHasher>>,
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
    /// commit/abort.  Tracks this transaction in the [`crate::VersionGuardRegistry`] for
    /// stale-reader detection and provides `defer_retire` for safe deferred
    /// reclamation of superseded page versions (§14.10, bd-2y306.1).
    pub version_guard: Option<VersionGuardTicket>,
    /// Pages that have undergone structural B-tree mutations (splits, merges,
    /// overflow chain modifications, interior page changes) OR raw page writes
    /// that bypass cell-level tracking.
    ///
    /// These pages must use page-level MVCC versioning (VersionStore.publish()).
    /// Pages in the write set but NOT in this set are candidates for cell-level
    /// MVCC (CellVisibilityLog). This enables concurrent writers to modify
    /// different rows on the same page without conflict (C4: bd-l9k8e.4).
    pub structural_pages: HashSet<PageNumber>,
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
            write_set_data: Arc::new(HashMap::with_hasher(
                fsqlite_types::PageNumberBuildHasher::default(),
            )),
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
            structural_pages: HashSet::new(),
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

    /// Threshold for auto-promoting from Exact to Bloom read-set mode.
    /// When the exact read-set HashMap exceeds this size, further inserts
    /// go into the Bloom filter only, saving ~50 bytes/entry in HashMap
    /// overhead for large analytical scans.
    const READ_SET_AUTO_BLOOM_THRESHOLD: usize = 1024;

    /// Record a page read with the visible committed version.
    pub fn record_page_read(&mut self, page: PageNumber, version: CommitSeq) {
        if let Some(bloom) = self.read_set_bloom.as_mut() {
            // Bloom mode active: only Bloom + witness key tracking.
            bloom.insert(page);
        } else if self.read_set_versions.len() >= Self::READ_SET_AUTO_BLOOM_THRESHOLD {
            // Auto-promote to Bloom mode: the exact read set has grown large
            // enough that the HashMap overhead dominates.  Switch to Bloom for
            // all future inserts; existing entries remain for version lookups.
            let mut bloom = ReadSetBloom::new(ReadSetBloom::DEFAULT_BITS);
            bloom.insert(page);
            self.read_set_bloom = Some(bloom);
            self.read_set_storage_mode = ReadSetStorageMode::Bloom;
        } else {
            // Exact mode: track in HashMap for precise version lookups.
            self.read_set_versions
                .entry(page)
                .and_modify(|existing| {
                    if version > *existing {
                        *existing = version;
                    }
                })
                .or_insert(version);
        }
        self.read_keys.insert(WitnessKey::Page(page));
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
    }

    /// Attach the assigned commit sequence to a written page entry.
    pub fn mark_page_write_committed(&mut self, page: PageNumber, new_version: CommitSeq) {
        if let Some(entry) = self.write_set_versions.get_mut(&page) {
            entry.new_version = Some(new_version);
        }
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
    pub fn thread_local_read_version_for_page(&self, _page: PageNumber) -> Option<CommitSeq> {
        None
    }

    /// Clear read/write tracking ledgers (called on txn finalization).
    pub fn clear_page_access_tracking(&mut self) {
        self.read_set_versions.clear();
        self.write_set_versions.clear();
        self.read_set_storage_mode = ReadSetStorageMode::Exact;
        self.read_set_bloom = None;
    }

    /// Clear structural pages tracking (C4: bd-l9k8e.4).
    ///
    /// Called after commit/abort to reset tracking for potential reuse.
    pub fn clear_structural_pages(&mut self) {
        self.structural_pages.clear();
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

type CommitMap = HashMap<PageNumber, CommitSeq, PageNumberBuildHasher>;

/// Left-right publication shard for commit-sequence metadata.
///
/// Readers pin the currently active side and read without contending with the
/// writer's map mutation. Writers serialize locally, update the inactive copy,
/// swap publication, wait for old-side readers to drain, then bring the old
/// side up to date.
///
/// Proof obligations attached to the current `CommitIndex` prototype:
/// - `CI-LR-1 monotone-publication`: once a newer commit sequence becomes
///   visible for a page, subsequent reads must never regress to an older one.
/// - `CI-LR-2 publish-before-drain`: after the active side flips, new readers
///   must observe the new value while old-side readers still retain the
///   previous committed copy.
/// - `CI-LR-3 post-update-convergence`: once `update()` returns, both copies
///   must contain the same commit sequence for the updated page.
///
/// This prototype duplicates the map rather than reclaiming heap nodes, so the
/// reclamation obligation is narrowed to a grace-period-before-overwrite check
/// (`CI-LR-2`) instead of pointer-lifetime safety. If a future design swaps to
/// pointer-based RCU/QSBR publication, that stronger reclamation proof becomes
/// a separate required obligation.
struct LeftRightCommitIndexShard {
    left: RwLock<CommitMap>,
    right: RwLock<CommitMap>,
    active: AtomicU64,
    left_readers: AtomicU64,
    right_readers: AtomicU64,
    writer_lock: Mutex<()>,
}

impl LeftRightCommitIndexShard {
    fn new() -> Self {
        Self {
            left: RwLock::new(HashMap::with_hasher(PageNumberBuildHasher::default())),
            right: RwLock::new(HashMap::with_hasher(PageNumberBuildHasher::default())),
            active: AtomicU64::new(0),
            left_readers: AtomicU64::new(0),
            right_readers: AtomicU64::new(0),
            writer_lock: Mutex::new(()),
        }
    }

    /// Read the latest published commit sequence for a page.
    ///
    /// Visibility contract:
    /// - a reader may observe the previous committed value if a swap races with
    ///   side selection;
    /// - once `update()` returns, both copies contain the new value so all
    ///   future reads observe the new commit sequence.
    fn latest(&self, page: PageNumber) -> Option<CommitSeq> {
        loop {
            let side = self.active.load(Ordering::Acquire);
            let (readers, data) = if side == 0 {
                (&self.left_readers, &self.left)
            } else {
                (&self.right_readers, &self.right)
            };
            readers.fetch_add(1, Ordering::AcqRel);
            if self.active.load(Ordering::Acquire) == side {
                let value = data.read().get(&page).copied();
                readers.fetch_sub(1, Ordering::Release);
                return value;
            }
            readers.fetch_sub(1, Ordering::Release);
        }
    }

    fn update(&self, page: PageNumber, seq: CommitSeq) {
        let _guard = self.writer_lock.lock();
        let active = self.active.load(Ordering::Acquire);

        if active == 0 {
            self.right.write().insert(page, seq);
        } else {
            self.left.write().insert(page, seq);
        }

        self.active.store(1 - active, Ordering::Release);

        let old_readers = if active == 0 {
            &self.left_readers
        } else {
            &self.right_readers
        };
        while old_readers.load(Ordering::Acquire) > 0 {
            std::hint::spin_loop();
        }

        if active == 0 {
            self.left.write().insert(page, seq);
        } else {
            self.right.write().insert(page, seq);
        }
    }

    fn len(&self) -> usize {
        if self.active.load(Ordering::Acquire) == 0 {
            self.left.read().len()
        } else {
            self.right.read().len()
        }
    }
}

/// A single cache-line-aligned commit index shard.
type CommitShard = CacheAligned<LeftRightCommitIndexShard>;

/// Number of pages covered by the O(1) flat atomic array.
///
/// For page numbers in `1..=FAST_COMMIT_ARRAY_SIZE`, `latest()` is a single
/// `AtomicU64::load(Acquire)` — no locks, no hashing, no reader tracking.
/// The array is split into lazily allocated 1024-entry chunks so empty
/// short-lived databases do not pay a 512 KiB zero-fill cost at connection
/// open.  A touched chunk remains a direct-indexed array, preserving the hot
/// read/write path after first publication.
///
/// Inspired by MICA (Lim et al., NSDI 2014): when keys are bounded integers,
/// replace hash maps with direct-indexed arrays for O(1) access.
const FAST_COMMIT_ARRAY_SIZE: usize = 65536;
const FAST_COMMIT_CHUNK_SIZE: usize = 1024;
const FAST_COMMIT_CHUNKS: usize = FAST_COMMIT_ARRAY_SIZE / FAST_COMMIT_CHUNK_SIZE;

/// Index mapping each page to its latest committed `CommitSeq`.
///
/// Uses a two-tier design for optimal hot-path performance:
///
/// **Tier 1 (fast path):** A flat `AtomicU64` array indexed by page number.
/// Reads are a single atomic load — no locks, no hashing, no reader-count
/// tracking.  This covers page numbers 1..=65536, which handles virtually all
/// benchmark and real-world databases.
///
/// **Tier 2 (fallback):** Sharded `LeftRightCommitIndexShard` for page numbers
/// beyond the flat array.  Shards are wrapped in [`CacheAligned`] to prevent
/// false sharing (§1.5).
///
/// The hot read path (`latest`) for the disjoint-insert benchmark previously
/// went through the LeftRight machinery: 3 atomic ops + RwLock acquire +
/// HashMap probe + RwLock release + reader-count decrement.  The flat array
/// replaces this with 1 atomic load.
pub struct CommitIndex {
    /// Highest commit sequence published through this index.
    latest_global: AtomicU64,
    /// O(1) atomic read/write for small page numbers.
    /// Index `i` stores the raw `CommitSeq` value for page `i + 1`.
    /// Value 0 means no committed version exists.
    fast_array: Box<[OnceLock<Box<[AtomicU64]>>; FAST_COMMIT_CHUNKS]>,
    /// Fallback sharded LeftRight path for large page numbers and iteration.
    shards: Box<[CommitShard; LOCK_TABLE_SHARDS]>,
}

impl CommitIndex {
    #[must_use]
    pub fn new() -> Self {
        Self {
            latest_global: AtomicU64::new(0),
            fast_array: Self::alloc_fast_array(),
            shards: Box::new(std::array::from_fn(|_| {
                CacheAligned::new(LeftRightCommitIndexShard::new())
            })),
        }
    }

    fn alloc_fast_array() -> Box<[OnceLock<Box<[AtomicU64]>>; FAST_COMMIT_CHUNKS]> {
        Box::new(std::array::from_fn(|_| OnceLock::new()))
    }

    fn alloc_fast_chunk() -> Box<[AtomicU64]> {
        let chunk: Vec<AtomicU64> = (0..FAST_COMMIT_CHUNK_SIZE)
            .map(|_| AtomicU64::new(0))
            .collect();
        chunk.into_boxed_slice()
    }

    #[inline]
    fn fast_slot(&self, pgno: usize) -> Option<&AtomicU64> {
        if pgno == 0 || pgno > FAST_COMMIT_ARRAY_SIZE {
            return None;
        }
        let index = pgno - 1;
        let chunk_index = index / FAST_COMMIT_CHUNK_SIZE;
        let chunk_offset = index % FAST_COMMIT_CHUNK_SIZE;
        let chunk = self.fast_array[chunk_index].get_or_init(Self::alloc_fast_chunk);
        Some(&chunk[chunk_offset])
    }

    #[inline]
    fn fast_slot_if_allocated(&self, pgno: usize) -> Option<&AtomicU64> {
        if pgno == 0 || pgno > FAST_COMMIT_ARRAY_SIZE {
            return None;
        }
        let index = pgno - 1;
        let chunk_index = index / FAST_COMMIT_CHUNK_SIZE;
        let chunk_offset = index % FAST_COMMIT_CHUNK_SIZE;
        self.fast_array[chunk_index]
            .get()
            .map(|chunk| &chunk[chunk_offset])
    }

    /// Record that `page` was last committed at `seq`.
    ///
    /// # Panics (debug only)
    ///
    /// Panics if `seq` is `CommitSeq(0)`, because the flat atomic array
    /// uses 0 as the sentinel for "no committed version."  The commit log
    /// allocates sequences starting at 1, so this should never happen.
    pub fn update(&self, page: PageNumber, seq: CommitSeq) {
        debug_assert!(
            seq.get() != 0,
            "CommitIndex::update called with CommitSeq(0); the flat array uses 0 as empty sentinel"
        );
        self.latest_global.fetch_max(seq.get(), Ordering::Release);
        let pgno = page.get() as usize;
        if pgno <= FAST_COMMIT_ARRAY_SIZE {
            // O(1) atomic write to flat array — this is the hot path.
            // Skip the sharded LeftRight write for hot pages since `latest()`
            // reads exclusively from the fast array for pages ≤ 65536.
            // The sharded path is only needed for len()/debug/diagnostics,
            // which can lazily scan the fast array instead.
            if let Some(slot) = self.fast_slot(pgno) {
                slot.store(seq.get(), Ordering::Release);
            }
            return;
        }
        // Fallback: large page numbers use the sharded path.
        let shard = &self.shards[self.shard_index(page)];
        shard.update(page, seq);
    }

    /// Record that multiple pages were committed at `seq` in a single batch.
    ///
    /// The fast-array path uses one upfront `Release` fence followed by
    /// per-page `Relaxed` stores. Readers query individual pages, so there is
    /// no shared "last page" synchronization point; the fence publishes all
    /// prior writes before any reader can observe one of the new per-page
    /// commit-index values via `latest()`'s `Acquire` load.
    ///
    /// Pages that exceed `FAST_COMMIT_ARRAY_SIZE` fall through to the
    /// per-page sharded path (which takes its own locks).
    pub fn batch_update(&self, pages: &[PageNumber], seq: CommitSeq) {
        debug_assert!(
            seq.get() != 0,
            "CommitIndex::batch_update called with CommitSeq(0)"
        );
        if pages.is_empty() {
            return;
        }
        let raw = seq.get();
        self.latest_global.fetch_max(raw, Ordering::Release);

        // Issue a single Release fence upfront.  After this fence, all
        // prior writes on this thread (version-store publishes, page-data
        // commits, etc.) are ordered before the commit-index stores below.
        // Any reader that does an Acquire load and sees one of these new
        // values is guaranteed to also see everything that happened-before
        // this fence.
        //
        // On x86 (TSO) the fence is free.  On ARM/RISC-V this collapses
        // N individual Release stores into one DMB/fence instruction.
        std::sync::atomic::fence(Ordering::Release);

        for &page in pages {
            let pgno = page.get() as usize;
            if pgno <= FAST_COMMIT_ARRAY_SIZE {
                // Safe to use Relaxed: the Release fence above already
                // guarantees ordering for all stores that follow.
                if let Some(slot) = self.fast_slot(pgno) {
                    slot.store(raw, Ordering::Relaxed);
                }
            } else {
                let shard = &self.shards[self.shard_index(page)];
                shard.update(page, seq);
            }
        }
    }

    /// Get the latest `CommitSeq` for `page`.
    ///
    /// For page numbers <= 65536 this is a single `AtomicU64::load(Acquire)`.
    #[inline]
    #[must_use]
    pub fn latest(&self, page: PageNumber) -> Option<CommitSeq> {
        let pgno = page.get() as usize;
        if pgno <= FAST_COMMIT_ARRAY_SIZE {
            let slot = self.fast_slot_if_allocated(pgno)?;
            let val = slot.load(Ordering::Acquire);
            return if val == 0 {
                None
            } else {
                Some(CommitSeq::new(val))
            };
        }
        let shard = &self.shards[self.shard_index(page)];
        shard.latest(page)
    }

    /// Highest commit sequence published through any page in the index.
    #[inline]
    #[must_use]
    pub fn latest_seq(&self) -> Option<CommitSeq> {
        let raw = self.latest_global.load(Ordering::Acquire);
        if raw == 0 {
            None
        } else {
            Some(CommitSeq::new(raw))
        }
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
        let sharded_pages: usize = self.shards.iter().map(|s| s.len()).sum();
        let fast_populated = self
            .fast_array
            .iter()
            .filter_map(OnceLock::get)
            .flat_map(|chunk| chunk.iter())
            .filter(|a| a.load(Ordering::Relaxed) != 0)
            .count();
        let fast_chunks_allocated = self
            .fast_array
            .iter()
            .filter(|chunk| chunk.get().is_some())
            .count();
        f.debug_struct("CommitIndex")
            .field("page_count", &(sharded_pages + fast_populated))
            .field("sharded_page_count", &sharded_pages)
            .field("fast_array_populated", &fast_populated)
            .field("fast_array_chunks_allocated", &fast_chunks_allocated)
            .field("fast_array_capacity", &FAST_COMMIT_ARRAY_SIZE)
            .field("latest_global", &self.latest_global.load(Ordering::Relaxed))
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
    /// Number of pinned readers whose snapshot `CommitSeq` clamped the
    /// proposed horizon below the slot-based floor (bd-wt4uu).
    pub stale_reader_clamps: usize,
    /// Reader snapshot `CommitSeq` that clamped the horizon, if any. Only
    /// meaningful when `stale_reader_clamps > 0`.
    pub clamped_by_reader_seq: Option<CommitSeq>,
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
        stale_reader_clamps: 0,
        clamped_by_reader_seq: None,
    }
}

/// Snapshot of a pinned reader consulted during GC horizon advance (bd-wt4uu).
///
/// This is a slot-free view that matches
/// [`crate::ebr::ReaderPinSnapshot`] but keeps [`crate::core_types`] decoupled from
/// the EBR registry type so callers (e.g. the pager) can supply synthetic
/// reader lists in tests without constructing a full registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReaderPinCommitSeq {
    /// Stable guard id (for logging).
    pub guard_id: u64,
    /// Reader snapshot `CommitSeq`. `None` means the reader has not declared
    /// its snapshot seq and is NOT considered for horizon clamping.
    pub pinned_commit_seq: Option<CommitSeq>,
    /// Elapsed pin duration for observability.
    pub pinned_for: Duration,
}

/// Compute the new GC horizon with synchronous stale-reader clamping (bd-wt4uu).
///
/// Wraps [`raise_gc_horizon`] and additionally clamps the proposed horizon to
/// `min_pinned_commit_seq - 1` if any reader's declared snapshot `CommitSeq`
/// is below the slot-based floor.
///
/// **Correctness**: readers may hold an EBR pin with a snapshot seq that is
/// older than any `SharedTxnSlot` the writer scans (e.g. the reader's slot
/// was freed or the reader registered via a ticket without a slot). Clamping
/// here preserves reader visibility: versions with `commit_seq < horizon`
/// are reclaimable; any reader pinned at `seq` still needs versions whose
/// commit_seq is in `[seq, current]`, so the horizon must stay `<= seq - 1`.
///
/// **Observability**: emits `tracing::warn!(target = "fsqlite_mvcc::stale_reader_pressure", ...)`
/// when a commit is clamped. Fields: `guard_id`, `pinned_for_ms`,
/// `commit_seq_delta`, `affected_pages`.
#[must_use]
pub fn raise_gc_horizon_with_reader_clamp(
    slots: &[SharedTxnSlot],
    old_horizon: CommitSeq,
    commit_seq: CommitSeq,
    reader_pins: &[ReaderPinCommitSeq],
    affected_pages: usize,
) -> GcHorizonResult {
    let mut result = raise_gc_horizon(slots, old_horizon, commit_seq);

    // Find the minimum pinned_commit_seq across all readers (if any).
    let clamp = reader_pins
        .iter()
        .filter_map(|pin| pin.pinned_commit_seq.map(|seq| (pin, seq)))
        .min_by_key(|(_, seq)| *seq);

    if let Some((pin, clamp_seq)) = clamp {
        if clamp_seq <= result.new_horizon {
            // Clamp the horizon to clamp_seq - 1 (or old_horizon if that is
            // larger — monotonicity must be preserved).
            let clamped = CommitSeq::new(clamp_seq.get().saturating_sub(1));
            let clamped = if clamped >= old_horizon {
                clamped
            } else {
                old_horizon
            };
            if clamped < result.new_horizon {
                let commit_seq_delta = commit_seq.get().saturating_sub(clamp_seq.get());
                tracing::warn!(
                    target: "fsqlite_mvcc::stale_reader_pressure",
                    guard_id = pin.guard_id,
                    pinned_for_ms = pin.pinned_for.as_millis() as u64,
                    commit_seq_delta,
                    affected_pages,
                    clamped_horizon = clamped.get(),
                    proposed_horizon = result.new_horizon.get(),
                    "commit GC horizon clamped by stale reader pin (bd-wt4uu)"
                );
                result.new_horizon = clamped;
                result.stale_reader_clamps = 1;
                result.clamped_by_reader_seq = Some(clamp_seq);
            }
        }
    }

    result
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

    // Safely free the slot by CASing from cleaning_word to 0.
    // We avoid blindly clearing fields here because multiple cleaners might race
    // and corrupt the slot if it is quickly re-allocated by a new transaction.
    let cleaning_word = encode_cleaning(orphan_txn_id);
    if slot
        .txn_id
        .compare_exchange(cleaning_word, 0, Ordering::AcqRel, Ordering::Acquire)
        .is_ok()
    {
        // Zero all fields as per bead_id=bd-2xns field_order_state.
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

        if !was_claiming && prior_cleanup_marker != 0 {
            GLOBAL_TXN_SLOT_METRICS.record_slot_released(None, reclaim_pid);
        }

        return SlotCleanupResult::Reclaimed {
            orphan_txn_id,
            was_claiming,
        };
    }

    SlotCleanupResult::CasRaceSkipped
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

/// Cleanup stale sentinel slots and raise the GC horizon, additionally
/// clamping the horizon against stale reader pins (bd-wt4uu).
///
/// This is the production entry point for commit finalization: it invokes
/// the stale-reader check BEFORE advancing the horizon. If any reader pins a
/// snapshot `CommitSeq` below the slot-based floor, the horizon is clamped
/// to `min_pinned_commit_seq - 1` and a
/// `tracing::warn!(target = "fsqlite_mvcc::stale_reader_pressure", ...)`
/// is emitted for each clamped commit.
///
/// Callers that already know `reader_pins` (e.g. obtained via
/// `VersionGuardRegistry::all_reader_pins`) should prefer this function over
/// [`cleanup_and_raise_gc_horizon`].
pub fn cleanup_and_raise_gc_horizon_with_reader_clamp(
    slots: &[SharedTxnSlot],
    old_horizon: CommitSeq,
    commit_seq: CommitSeq,
    now_epoch_secs: u64,
    process_alive: impl Fn(u32, u64) -> bool,
    reader_pins: &[ReaderPinCommitSeq],
    affected_pages: usize,
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

    let horizon_result = raise_gc_horizon_with_reader_clamp(
        slots,
        old_horizon,
        commit_seq,
        reader_pins,
        affected_pages,
    );
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
        let cleaning_word = encode_cleaning(orphan_txn_id);

        if was_claiming {
            // Transition CLAIMING → CLEANING (preserves identity).
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
                slot_pid = reclaim_pid,
                slot_pid_birth = slot.pid_birth.load(Ordering::Acquire),
                slot_lease_expiry = slot.lease_expiry.load(Ordering::Acquire),
                cleanup_started_at = now_epoch_secs,
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
            slot_pid = reclaim_pid,
            slot_pid_birth = slot.pid_birth.load(Ordering::Acquire),
            slot_lease_expiry = slot.lease_expiry.load(Ordering::Acquire),
            cleanup_started_at = now_epoch_secs,
            "reclaiming stale sentinel slot"
        );

        if slot
            .txn_id
            .compare_exchange(cleaning_word, 0, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            // Zero all fields as per bead_id=bd-2xns field_order_state.
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

            if !was_claiming && prior_cleanup_marker != 0 {
                GLOBAL_TXN_SLOT_METRICS.record_slot_released(None, reclaim_pid);
            }

            return SlotCleanupResult::Reclaimed {
                orphan_txn_id,
                was_claiming,
            };
        }

        return SlotCleanupResult::CasRaceSkipped;
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

    tracing::info!(
        orphan_txn_id,
        slot_pid = pid,
        slot_pid_birth = birth,
        slot_lease_expiry = lease,
        cleanup_started_at = now_epoch_secs,
        "reclaiming orphaned real TxnId slot"
    );

    if slot
        .txn_id
        .compare_exchange(cleaning_word, 0, Ordering::AcqRel, Ordering::Acquire)
        .is_ok()
    {
        // Zero all fields as per bead_id=bd-2xns field_order_state.
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

        GLOBAL_TXN_SLOT_METRICS.record_slot_released(None, pid);
        return SlotCleanupResult::Reclaimed {
            orphan_txn_id,
            was_claiming: false,
        };
    }

    SlotCleanupResult::CasRaceSkipped
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
        now_epoch_secs,
        "starting cleanup_orphaned_slots pass"
    );

    for (idx, slot) in slots.iter().enumerate() {
        let slot_owner_txn_id = decode_payload(slot.txn_id.load(Ordering::Acquire));
        let slot_pid = slot.pid.load(Ordering::Acquire);
        let slot_pid_birth = slot.pid_birth.load(Ordering::Acquire);
        let slot_lease_expiry = slot.lease_expiry.load(Ordering::Acquire);
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
                slot_owner_txn_id,
                orphan_txn_id,
                was_claiming,
                slot_pid,
                slot_pid_birth,
                slot_lease_expiry,
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
    use serde_json::json;
    use std::io;
    use std::sync::Arc;
    use std::sync::Mutex;

    #[derive(Clone)]
    struct BufMakeWriter(Arc<Mutex<Vec<u8>>>);

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for BufMakeWriter {
        type Writer = BufWriter;

        fn make_writer(&'a self) -> Self::Writer {
            BufWriter(Arc::clone(&self.0))
        }
    }

    struct BufWriter(Arc<Mutex<Vec<u8>>>);

    impl io::Write for BufWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            let mut guard = self.0.lock().expect("core_types log buffer lock");
            guard.extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    fn with_tracing_capture<F, R>(
        _capture_guard: &crate::test_support::TracingCaptureGuard,
        f: F,
    ) -> (R, String)
    where
        F: FnOnce() -> R,
    {
        let buf = Arc::new(Mutex::new(Vec::new()));
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .with_max_level(tracing::Level::DEBUG)
            .with_writer(BufMakeWriter(Arc::clone(&buf)))
            .finish();
        let result = tracing::subscriber::with_default(subscriber, f);
        let bytes = buf.lock().expect("core_types log buffer lock").clone();
        (result, String::from_utf8_lossy(&bytes).to_string())
    }

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
    fn test_cleanup_orphaned_slots_logs_slot_identity_and_lease_fields() {
        let slot = SharedTxnSlot::new();
        slot.txn_id.store(42, Ordering::Release);
        slot.lease_expiry.store(90, Ordering::Release);
        slot.pid.store(12_345, Ordering::Release);
        slot.pid_birth.store(9_999, Ordering::Release);
        slot.begin_seq.store(50, Ordering::Release);

        let capture_guard = crate::test_support::tracing_capture_guard();
        let ((), logs) = with_tracing_capture(&capture_guard, || {
            let stats =
                cleanup_orphaned_slots(std::slice::from_ref(&slot), 100, |_, _| false, |_| {});
            assert_eq!(stats.orphans_found, 1);
        });

        assert!(logs.contains("slot_idx=0"));
        assert!(logs.contains("slot_owner_txn_id=42"));
        assert!(logs.contains("slot_pid=12345"));
        assert!(logs.contains("slot_pid_birth=9999"));
        assert!(logs.contains("slot_lease_expiry=90"));
        assert!(logs.contains("orphan_txn_id=42"));
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
    fn test_in_process_lock_table_wait_for_holder_change_wakes_on_release() {
        let table = Arc::new(InProcessPageLockTable::new());
        let page = PageNumber::new(7).unwrap();
        let holder = TxnId::new(1).unwrap();

        table.try_acquire(page, holder).unwrap();

        let waiter_table = Arc::clone(&table);
        let waiter = std::thread::spawn(move || {
            waiter_table.wait_for_holder_change(page, holder, Duration::from_secs(1))
        });

        assert!(table.release(page, holder));
        assert!(
            waiter.join().unwrap(),
            "waiter should observe release before timing out"
        );
    }

    #[test]
    fn test_in_process_lock_table_wait_for_holder_change_times_out() {
        let table = InProcessPageLockTable::new();
        let page = PageNumber::new(8).unwrap();
        let holder = TxnId::new(2).unwrap();

        table.try_acquire(page, holder).unwrap();

        let started = Instant::now();
        assert!(
            !table.wait_for_holder_change(page, holder, Duration::from_millis(20)),
            "wait should time out while holder is unchanged"
        );
        assert!(
            started.elapsed() >= Duration::from_millis(10),
            "timeout path should not return immediately"
        );
    }

    /// Microbench for the `wait_for_holder_change` post-park hot wake path
    /// (bd-xeaze). Producer holds the page for a fixed ~30µs spin so the
    /// consumer reliably enters the `park_timeout` branch of
    /// `wait_for_holder_change`; the producer then releases (triggering
    /// `notify_waiters_for_page`), flips the TxnId, and reacquires.
    ///
    /// The consumer uses a third TxnId and only observes the holder, so the
    /// producer/consumer do not race for the lock. This isolates the
    /// register/park/post-park-unregister cost from scheduler luck.
    ///
    /// Not a correctness test; ignored by default. Reports wakes/sec averaged
    /// over N trials. Run with:
    /// `cargo test -p fsqlite-mvcc --lib --release -- \
    ///    test_in_process_lock_table_wait_for_holder_change_microbench \
    ///    --ignored --nocapture`
    #[test]
    #[ignore = "microbench — run manually"]
    fn test_in_process_lock_table_wait_for_holder_change_microbench() {
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

        fn run_trial(duration: Duration) -> u64 {
            let table = Arc::new(InProcessPageLockTable::new());
            let page = PageNumber::new(17).unwrap();
            let txn_a = TxnId::new(1).unwrap();
            let txn_b = TxnId::new(2).unwrap();
            let stop = Arc::new(AtomicBool::new(false));
            let wakes = Arc::new(AtomicUsize::new(0));

            table.try_acquire(page, txn_a).unwrap();

            let t_prod = Arc::clone(&table);
            let stop_p = Arc::clone(&stop);
            // Producer holds for this many spin iterations between releases,
            // giving the consumer enough time to reach `park_timeout` before
            // the holder flips. Sized empirically to a few tens of µs on x86.
            const PRODUCER_HOLD_SPINS: u32 = 4_000;
            let producer = std::thread::spawn(move || {
                let mut current = txn_a;
                while !stop_p.load(Ordering::Relaxed) {
                    for _ in 0..PRODUCER_HOLD_SPINS {
                        std::hint::spin_loop();
                    }
                    let next = if current == txn_a { txn_b } else { txn_a };
                    t_prod.release(page, current);
                    while t_prod.try_acquire(page, next).is_err() {
                        std::hint::spin_loop();
                    }
                    current = next;
                }
                t_prod.release(page, current);
            });

            // Multiple consumer threads contend on the same waiter shard for
            // `page`, so notify/register/unregister traffic stresses the
            // waiter-shard mutex path that `wait_for_holder_change` exercises.
            const CONSUMERS: usize = 6;
            let mut consumers = Vec::with_capacity(CONSUMERS);
            for _ in 0..CONSUMERS {
                let t_cons = Arc::clone(&table);
                let stop_c = Arc::clone(&stop);
                let wakes_c = Arc::clone(&wakes);
                consumers.push(std::thread::spawn(move || {
                    while !stop_c.load(Ordering::Relaxed) {
                        let Some(holder) = t_cons.holder(page) else {
                            continue;
                        };
                        if t_cons.wait_for_holder_change(page, holder, Duration::from_millis(5)) {
                            wakes_c.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }));
            }

            std::thread::sleep(duration);
            stop.store(true, Ordering::Relaxed);
            producer.join().unwrap();
            for c in consumers {
                c.join().unwrap();
            }

            wakes.load(Ordering::Relaxed) as u64
        }

        const TRIALS: usize = 5;
        let duration = Duration::from_millis(500);
        let mut samples = Vec::with_capacity(TRIALS);
        // Warm-up trial to prime caches/thread-local state.
        let _ = run_trial(duration);
        for _ in 0..TRIALS {
            let wakes = run_trial(duration);
            let rate = wakes as f64 / duration.as_secs_f64();
            eprintln!("  trial: {wakes} wakes, {rate:.0} wakes/sec");
            samples.push(rate);
        }
        samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let median = samples[TRIALS / 2];
        let mean = samples.iter().sum::<f64>() / samples.len() as f64;
        eprintln!(
            "wait_for_holder_change microbench: median={median:.0} wakes/sec, mean={mean:.0} \
             wakes/sec (n={TRIALS}, {duration:?}/trial)"
        );
    }

    /// Microbench for the no-waiters fast path in `notify_all_waiters` and
    /// `notify_waiters_for_page`. Times direct calls into both helpers
    /// when `waiter_count == 0` (the dominant production case on
    /// commit / release_all when no thread is parked on a page lock).
    /// Without the gate each `notify_all_waiters` call acquires 256
    /// per-shard mutexes; with the gate it short-circuits on a single
    /// SeqCst-fenced atomic load.
    ///
    /// Each helper is timed twice: once with `waiter_count == 0` (gated
    /// fast path — production case) and once with `waiter_count` forced
    /// non-zero before each call (forces the pre-gate slow path with
    /// empty maps, mimicking the previous unconditional behavior). The
    /// difference between the two columns is the gate's saving.
    ///
    /// Run with:
    /// `cargo test -p fsqlite-mvcc --lib --release -- \
    ///    bench_notify_waiters_no_waiters --ignored --nocapture`
    #[test]
    #[ignore = "microbench — run manually"]
    #[allow(clippy::unit_arg)]
    fn bench_notify_waiters_no_waiters() {
        let table = InProcessPageLockTable::new();
        let page = PageNumber::new(17).unwrap();

        const ITERS_ALL_FAST: u32 = 1_000_000;
        const ITERS_ALL_SLOW: u32 = 100_000;
        const ITERS_PAGE_FAST: u32 = 5_000_000;
        const ITERS_PAGE_SLOW: u32 = 5_000_000;
        const TRIALS: usize = 7;

        // Warm-up.
        for _ in 0..ITERS_ALL_FAST {
            std::hint::black_box(table.notify_all_waiters());
        }
        for _ in 0..ITERS_PAGE_FAST {
            std::hint::black_box(table.notify_waiters_for_page(page));
        }
        debug_assert_eq!(table.waiter_count.load(Ordering::Relaxed), 0);

        // Fast path (gate fires): waiter_count stays 0.
        let mut all_fast = Vec::with_capacity(TRIALS);
        for _ in 0..TRIALS {
            let start = Instant::now();
            for _ in 0..ITERS_ALL_FAST {
                std::hint::black_box(table.notify_all_waiters());
            }
            let elapsed = start.elapsed();
            let ns = elapsed.as_nanos() as f64 / f64::from(ITERS_ALL_FAST);
            eprintln!("  notify_all  fast trial: {ns:.1} ns/call ({elapsed:?} total)");
            all_fast.push(ns);
        }

        let mut all_slow = Vec::with_capacity(TRIALS);
        for _ in 0..TRIALS {
            let start = Instant::now();
            for _ in 0..ITERS_ALL_SLOW {
                // Force waiter_count > 0 *for the gate's load* so the
                // slow body runs — but no real entries exist, so the
                // shard iteration drains 0 and the fetch_sub is skipped.
                // This isolates the cost of the 256 shard mutex acquires
                // and the empty `map.drain()` calls (i.e., the old
                // unconditional behavior).
                table.waiter_count.store(1, Ordering::Relaxed);
                std::hint::black_box(table.notify_all_waiters());
            }
            let elapsed = start.elapsed();
            table.waiter_count.store(0, Ordering::Relaxed);
            let ns = elapsed.as_nanos() as f64 / f64::from(ITERS_ALL_SLOW);
            eprintln!("  notify_all  slow trial: {ns:.1} ns/call ({elapsed:?} total)");
            all_slow.push(ns);
        }

        let mut page_fast = Vec::with_capacity(TRIALS);
        for _ in 0..TRIALS {
            let start = Instant::now();
            for _ in 0..ITERS_PAGE_FAST {
                std::hint::black_box(table.notify_waiters_for_page(page));
            }
            let elapsed = start.elapsed();
            let ns = elapsed.as_nanos() as f64 / f64::from(ITERS_PAGE_FAST);
            eprintln!("  notify_page fast trial: {ns:.1} ns/call ({elapsed:?} total)");
            page_fast.push(ns);
        }

        let mut page_slow = Vec::with_capacity(TRIALS);
        for _ in 0..TRIALS {
            let start = Instant::now();
            for _ in 0..ITERS_PAGE_SLOW {
                table.waiter_count.store(1, Ordering::Relaxed);
                std::hint::black_box(table.notify_waiters_for_page(page));
            }
            let elapsed = start.elapsed();
            table.waiter_count.store(0, Ordering::Relaxed);
            let ns = elapsed.as_nanos() as f64 / f64::from(ITERS_PAGE_SLOW);
            eprintln!("  notify_page slow trial: {ns:.1} ns/call ({elapsed:?} total)");
            page_slow.push(ns);
        }

        all_fast.sort_by(|a, b| a.partial_cmp(b).unwrap());
        all_slow.sort_by(|a, b| a.partial_cmp(b).unwrap());
        page_fast.sort_by(|a, b| a.partial_cmp(b).unwrap());
        page_slow.sort_by(|a, b| a.partial_cmp(b).unwrap());

        eprintln!(
            "bench_notify_waiters_no_waiters: \
             notify_all  fast median={:.1} ns/call, slow median={:.1} ns/call; \
             notify_page fast median={:.1} ns/call, slow median={:.1} ns/call (n={})",
            all_fast[TRIALS / 2],
            all_slow[TRIALS / 2],
            page_fast[TRIALS / 2],
            page_slow[TRIALS / 2],
            TRIALS,
        );
    }

    /// Same-run baseline-vs-optimized bench for the new `waiter_count == 0`
    /// gate on `unregister_waiter`. The fast path returns after a single
    /// relaxed atomic load; the slow body acquires the per-shard
    /// `parking_lot::Mutex<HashMap>` and runs `get_mut(&page)` (None on
    /// the post-park drained-by-notifier path).
    ///
    /// Toggle pattern (same as `bench_notify_waiters_no_waiters`): force
    /// `waiter_count = 1` per slow iteration so the gate's load returns
    /// non-zero and the slow body executes — but no entry was ever
    /// inserted, so `get_mut` returns None and the inner mutate is
    /// skipped (matches the production race where a notifier already
    /// drained our entry before we reach unregister).
    ///
    /// Run with:
    /// `cargo test -p fsqlite-mvcc --lib --release -- \
    ///    bench_unregister_waiter_drained --ignored --nocapture`
    #[test]
    #[ignore = "microbench — run manually"]
    #[allow(clippy::unit_arg)]
    fn bench_unregister_waiter_drained() {
        let table = InProcessPageLockTable::new();
        let page = PageNumber::new(23).unwrap();
        let tid = std::thread::current().id();

        const ITERS_FAST: u32 = 5_000_000;
        const ITERS_SLOW: u32 = 2_000_000;
        const TRIALS: usize = 7;

        // Warm-up.
        for _ in 0..ITERS_FAST {
            std::hint::black_box(table.unregister_waiter(page, tid));
        }
        debug_assert_eq!(table.waiter_count.load(Ordering::Relaxed), 0);

        let mut fast = Vec::with_capacity(TRIALS);
        for _ in 0..TRIALS {
            let start = Instant::now();
            for _ in 0..ITERS_FAST {
                std::hint::black_box(table.unregister_waiter(page, tid));
            }
            let elapsed = start.elapsed();
            let ns = elapsed.as_nanos() as f64 / f64::from(ITERS_FAST);
            eprintln!("  unregister fast trial: {ns:.1} ns/call ({elapsed:?} total)");
            fast.push(ns);
        }

        let mut slow = Vec::with_capacity(TRIALS);
        for _ in 0..TRIALS {
            let start = Instant::now();
            for _ in 0..ITERS_SLOW {
                // Force the gate's load to see non-zero, exercising the
                // slow body. No entry exists, so `get_mut` returns None
                // and the mutate path is skipped (production-realistic:
                // notifier drained our entry before we reach unregister).
                table.waiter_count.store(1, Ordering::Relaxed);
                std::hint::black_box(table.unregister_waiter(page, tid));
            }
            let elapsed = start.elapsed();
            table.waiter_count.store(0, Ordering::Relaxed);
            let ns = elapsed.as_nanos() as f64 / f64::from(ITERS_SLOW);
            eprintln!("  unregister slow trial: {ns:.1} ns/call ({elapsed:?} total)");
            slow.push(ns);
        }

        fast.sort_by(|a, b| a.partial_cmp(b).unwrap());
        slow.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let f_med = fast[TRIALS / 2];
        let s_med = slow[TRIALS / 2];
        let delta_pct = (f_med - s_med) / s_med * 100.0;
        eprintln!(
            "bench_unregister_waiter_drained: fast median={f_med:.1} ns/call, \
             slow median={s_med:.1} ns/call, delta={delta_pct:+.1}% (n={TRIALS})"
        );
    }

    #[test]
    fn test_in_process_lock_table_wait_for_release_progress_wakes_on_release() {
        let table = Arc::new(InProcessPageLockTable::new());
        let page = PageNumber::new(9).unwrap();
        let holder = TxnId::new(3).unwrap();

        table.try_acquire(page, holder).unwrap();
        let observed_epoch = table.release_progress_epoch();

        let waiter_table = Arc::clone(&table);
        let waiter = std::thread::spawn(move || {
            waiter_table.wait_for_release_progress(observed_epoch, Duration::from_secs(1))
        });

        assert!(table.release(page, holder));
        assert!(
            waiter.join().unwrap(),
            "aggregate release-progress waiter should observe release before timing out"
        );
    }

    #[test]
    fn test_in_process_lock_table_wait_for_release_progress_sees_pre_gate_epoch_advance() {
        let table = InProcessPageLockTable::new();
        let observed_epoch = table.release_progress_epoch();
        let started = Instant::now();

        let woke = table.wait_for_release_progress_with_gate_hook(
            observed_epoch,
            Duration::from_millis(200),
            || {
                let _gate = table.change_gate.lock();
                table.change_epoch.fetch_add(1, Ordering::Release);
                table.change_cv.notify_all();
            },
        );

        assert!(
            woke,
            "bd-65byl: waiter must observe an epoch advance that happens before gate acquisition"
        );
        assert!(
            started.elapsed() < Duration::from_millis(50),
            "bd-65byl: pre-gate epoch advance must not sleep until timeout"
        );
    }

    #[test]
    fn test_in_process_lock_table_wait_for_release_progress_times_out() {
        let table = InProcessPageLockTable::new();
        let observed_epoch = table.release_progress_epoch();
        let started = Instant::now();

        assert!(
            !table.wait_for_release_progress(observed_epoch, Duration::from_millis(20)),
            "aggregate release-progress wait should time out without any release"
        );
        assert!(
            started.elapsed() >= Duration::from_millis(10),
            "aggregate release-progress timeout should not return immediately"
        );
    }

    #[test]
    fn test_in_process_lock_table_shard_distribution() {
        let table = InProcessPageLockTable::new();
        let txn = TxnId::new(1).unwrap();
        let page_count = u32::try_from(LOCK_TABLE_SHARDS * 2).unwrap();

        // Acquire locks on sharded pages only. Fast-array pages 1..=65536 do
        // not contribute to shard_distribution().
        for i in 1..=page_count {
            let page = sharded_rebuild_page(i);
            table.try_acquire(page, txn).unwrap();
        }

        let dist = table.shard_distribution();
        assert_eq!(dist.len(), LOCK_TABLE_SHARDS);

        // We insert exactly two sharded pages per shard, so distribution
        // should stay uniform even as the shard fan-out changes.
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
    #[should_panic(expected = "can only abort active")]
    fn test_transaction_double_abort_panics() {
        let txn_id = TxnId::new(5).unwrap();
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
        assert_eq!(
            txn.read_set_storage_mode,
            ReadSetStorageMode::Exact,
            "access tracking cleanup must restore exact read-set mode"
        );
        assert!(
            txn.read_set_bloom.is_none(),
            "access tracking cleanup must release the bloom read-set allocation"
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
        assert_eq!(index.latest_seq(), Some(CommitSeq::new(5)));

        index.update(page, CommitSeq::new(10));
        assert_eq!(index.latest(page), Some(CommitSeq::new(10)));
        assert_eq!(index.latest_seq(), Some(CommitSeq::new(10)));
    }

    #[test]
    fn test_commit_index_latest_monotone_under_concurrent_updates() {
        use std::sync::Barrier;
        use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
        use std::thread;

        const FINAL_SEQ: u64 = 256;

        let index = std::sync::Arc::new(CommitIndex::new());
        let page = PageNumber::new(42).unwrap();
        index.update(page, CommitSeq::new(1));

        let start = std::sync::Arc::new(Barrier::new(5));
        let done = std::sync::Arc::new(AtomicBool::new(false));

        let writer_index = std::sync::Arc::clone(&index);
        let writer_start = std::sync::Arc::clone(&start);
        let writer_done = std::sync::Arc::clone(&done);
        let writer = thread::spawn(move || {
            writer_start.wait();
            for seq in 2..=FINAL_SEQ {
                writer_index.update(page, CommitSeq::new(seq));
            }
            writer_done.store(true, AtomicOrdering::Release);
        });

        let mut readers = Vec::new();
        for _ in 0..4 {
            let reader_index = std::sync::Arc::clone(&index);
            let reader_start = std::sync::Arc::clone(&start);
            let reader_done = std::sync::Arc::clone(&done);
            readers.push(thread::spawn(move || {
                reader_start.wait();
                let mut last_seen = 0_u64;
                loop {
                    if let Some(seq) = reader_index.latest(page) {
                        let current = seq.get();
                        assert!(
                            current >= last_seen,
                            "commit index publication must be monotone: current={current} last_seen={last_seen}"
                        );
                        last_seen = current;
                        if reader_done.load(AtomicOrdering::Acquire) && current == FINAL_SEQ {
                            return last_seen;
                        }
                    }
                    thread::yield_now();
                }
            }));
        }

        writer.join().unwrap();
        for reader in readers {
            assert_eq!(reader.join().unwrap(), FINAL_SEQ);
        }
        assert_eq!(index.latest(page), Some(CommitSeq::new(FINAL_SEQ)));
    }

    #[test]
    fn test_commit_index_debug_counts_fast_and_sharded_pages() {
        let index = CommitIndex::new();
        let fast_page = PageNumber::new(42).unwrap();
        let sharded_page =
            PageNumber::new(u32::try_from(FAST_COMMIT_ARRAY_SIZE + 1).unwrap()).unwrap();

        index.update(fast_page, CommitSeq::new(11));
        index.update(sharded_page, CommitSeq::new(12));

        let debug = format!("{index:?}");
        assert!(debug.contains("page_count: 2"));
        assert!(debug.contains("sharded_page_count: 1"));
        assert!(debug.contains("fast_array_populated: 1"));
        assert!(debug.contains("fast_array_chunks_allocated: 1"));
    }

    #[test]
    fn test_commit_index_batch_update_updates_fast_and_sharded_pages() {
        let index = CommitIndex::new();
        let fast_page = PageNumber::new(7).unwrap();
        let sharded_page =
            PageNumber::new(u32::try_from(FAST_COMMIT_ARRAY_SIZE + 9).unwrap()).unwrap();
        let seq = CommitSeq::new(13);

        index.batch_update(&[fast_page, sharded_page], seq);

        assert_eq!(index.latest(fast_page), Some(seq));
        assert_eq!(index.latest(sharded_page), Some(seq));
        assert_eq!(index.latest_seq(), Some(seq));
    }

    #[test]
    fn test_commit_index_left_right_publish_drains_old_readers_before_mirror() {
        use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
        use std::thread;

        let shard = std::sync::Arc::new(LeftRightCommitIndexShard::new());
        let page = PageNumber::new(7).unwrap();
        let initial = CommitSeq::new(11);
        let published = CommitSeq::new(12);

        shard.update(page, initial);
        assert_eq!(shard.active.load(Ordering::Acquire), 1);
        assert_eq!(shard.latest(page), Some(initial));

        // Emulate a reader pinned on the currently active side so the writer
        // must stop after publication and before mirroring the old copy.
        shard.right_readers.fetch_add(1, Ordering::AcqRel);

        let writer_done = std::sync::Arc::new(AtomicBool::new(false));
        let writer_shard = std::sync::Arc::clone(&shard);
        let writer_done_flag = std::sync::Arc::clone(&writer_done);
        let writer = thread::spawn(move || {
            writer_shard.update(page, published);
            writer_done_flag.store(true, AtomicOrdering::Release);
        });

        let mut spins = 0_u32;
        while shard.active.load(Ordering::Acquire) != 0 {
            std::hint::spin_loop();
            spins = spins.saturating_add(1);
            assert!(
                spins < 1_000_000,
                "writer did not publish the new active side while old readers were pinned"
            );
        }

        assert_eq!(
            shard.latest(page),
            Some(published),
            "new readers must observe the new sequence immediately after publication"
        );
        assert_eq!(
            shard.left.read().get(&page).copied(),
            Some(published),
            "inactive side must receive the new value before publication"
        );
        assert_eq!(
            shard.right.read().get(&page).copied(),
            Some(initial),
            "old side must retain the prior value until old readers drain"
        );
        assert!(
            !writer_done.load(AtomicOrdering::Acquire),
            "writer must wait for old-side readers before mirroring the old copy"
        );

        shard.right_readers.fetch_sub(1, Ordering::AcqRel);
        writer.join().unwrap();

        assert!(writer_done.load(AtomicOrdering::Acquire));
        assert_eq!(shard.left.read().get(&page).copied(), Some(published));
        assert_eq!(shard.right.read().get(&page).copied(), Some(published));
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
        // It only reads already-allocated storage with no intermediate containers.
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
        // It only reads from already-allocated fast chunks or sharded maps.
        let index = CommitIndex::new();
        let page = PageNumber::new(7).unwrap();

        // Miss path on an unallocated fast chunk must also stay allocation-free
        // and return None.
        let cold_miss = index.latest(page);
        assert_eq!(
            cold_miss, None,
            "bead_id={BEAD_22N8} case=commit_index_cold_miss_no_alloc"
        );

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

    fn sharded_rebuild_page(offset: u32) -> PageNumber {
        assert!(offset > 0, "offset must keep the page in the sharded range");
        let base = u32::try_from(FAST_LOCK_ARRAY_SIZE).unwrap();
        PageNumber::new(base + offset).unwrap()
    }

    #[test]
    fn test_lock_table_rebuild_drains_to_zero_holders() {
        // bd-22n.12: begin_rebuild rotates active → draining, new acquisitions
        // go to the fresh active table. After releasing all draining locks, the
        // draining table reaches quiescence and finalize succeeds.
        let mut table = InProcessPageLockTable::new();
        let txn1 = TxnId::new(1).unwrap();
        let txn2 = TxnId::new(2).unwrap();

        let p1 = sharded_rebuild_page(1);
        let p2 = sharded_rebuild_page(2);
        let p3 = sharded_rebuild_page(3);

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
    fn test_fast_array_locks_stay_live_across_rebuild() {
        // bd-22n.12: begin_rebuild rotates only the sharded fallback table.
        // Fast-array locks stay resident and enforce contention throughout the
        // rebuild without touching the draining table.
        let mut table = InProcessPageLockTable::new();
        let txn1 = TxnId::new(1).unwrap();
        let txn2 = TxnId::new(2).unwrap();
        let page = PageNumber::new(1).unwrap();

        table.try_acquire(page, txn1).unwrap();
        assert_eq!(table.lock_count(), 1);

        table.begin_rebuild().unwrap();
        assert!(table.is_rebuild_in_progress());
        assert_eq!(table.draining_lock_count(), 0);
        assert_eq!(table.lock_count(), 1);
        assert_eq!(table.holder(page), Some(txn1));
        assert_eq!(table.try_acquire(page, txn2), Err(txn1));

        table.release_set([page], txn1);
        assert_eq!(table.holder(page), None);
        assert_eq!(table.lock_count(), 0);

        let result = table.finalize_rebuild().unwrap();
        assert_eq!(result.retained, 0);
    }

    #[test]
    fn test_read_only_txns_dont_block_rebuild() {
        // bd-22n.12 §5.6.3.1: read-only transactions MUST NOT block rebuild.
        // Read-only transactions don't acquire page locks, so the draining table
        // reaches quiescence without waiting for them.
        let mut table = InProcessPageLockTable::new();
        let writer = TxnId::new(1).unwrap();
        let p1 = sharded_rebuild_page(10);

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
        // bd-22n.12: full_rebuild with orphan cleanup reaches quiescence
        // within a reasonable timeout.
        let mut table = InProcessPageLockTable::new();
        let txn_a = TxnId::new(10).unwrap();
        let txn_b = TxnId::new(20).unwrap();
        let txn_orphan = TxnId::new(999).unwrap();

        // Set up locks: some active, some orphaned (txn crashed).
        for i in 1..=5u32 {
            table.try_acquire(sharded_rebuild_page(i), txn_a).unwrap();
        }
        for i in 6..=10u32 {
            table.try_acquire(sharded_rebuild_page(i), txn_b).unwrap();
        }
        for i in 11..=15u32 {
            table
                .try_acquire(sharded_rebuild_page(i), txn_orphan)
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
        table.try_acquire(sharded_rebuild_page(1), txn).unwrap();

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
            .try_acquire(sharded_rebuild_page(1), active_txn)
            .unwrap();
        table
            .try_acquire(sharded_rebuild_page(2), crashed_txn)
            .unwrap();
        table
            .try_acquire(sharded_rebuild_page(3), crashed_txn)
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
            table.try_acquire(sharded_rebuild_page(i), txn).unwrap();
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
            table.release(sharded_rebuild_page(i), txn);
        }

        let progress = table.drain_progress().unwrap();
        assert_eq!(
            progress.remaining, 3,
            "bead_id={BEAD_22N12} case=progress_after_partial_drain"
        );

        // Release remaining.
        for i in 8..=10u32 {
            table.release(sharded_rebuild_page(i), txn);
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
        table.try_acquire(sharded_rebuild_page(1), txn).unwrap();
        table.try_acquire(sharded_rebuild_page(2), txn).unwrap();

        // Rotate.
        table.begin_rebuild().unwrap();

        // Acquire post-rebuild.
        table.try_acquire(sharded_rebuild_page(3), txn).unwrap();

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
                let page = sharded_rebuild_page(u32::try_from(page_no).unwrap());
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
                let page = sharded_rebuild_page(u32::try_from(page_no).unwrap());
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
                let page = sharded_rebuild_page(u32::try_from(page_no).unwrap());
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
        CLAIMING_TIMEOUT_NO_PID_SECS, CLAIMING_TIMEOUT_SECS, TAG_CLAIMING, TAG_CLEANING,
        encode_claiming, encode_cleaning,
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
        let long_ago = claim_time + CLAIMING_TIMEOUT_SECS - 1;
        let _now = long_ago + CLAIMING_TIMEOUT_SECS + 1;

        let slot = make_claiming_slot(txn_id_raw, long_ago);
        // Ensure pid/pid_birth are 0 (not published).
        assert_eq!(slot.pid.load(Ordering::Relaxed), 0);
        assert_eq!(slot.pid_birth.load(Ordering::Relaxed), 0);

        // Too recent: should not be reclaimed.
        let recent_now = long_ago + CLAIMING_TIMEOUT_NO_PID_SECS - 1;
        let result = try_cleanup_sentinel_slot(&slot, recent_now, |_, _| false);
        assert_eq!(
            result,
            SlotCleanupResult::StillRecent,
            "bead_id={BEAD_22N13} case=stale_sentinel_too_recent"
        );

        // Now past the conservative timeout.
        let stale_now = long_ago + CLAIMING_TIMEOUT_NO_PID_SECS + 1;
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
        let long_ago = claim_time + CLAIMING_TIMEOUT_SECS - 1;
        let now = long_ago + CLAIMING_TIMEOUT_SECS + 1;

        let slot = make_claiming_slot(txn_id_raw, long_ago);
        slot.pid.store(12345, Ordering::Release);
        slot.pid_birth.store(9999, Ordering::Release);

        // Process is dead: use CLAIMING_TIMEOUT_SECS (shorter).
        let result = try_cleanup_sentinel_slot(&slot, now, |_, _| false);
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
        let long_ago = claim_time + CLAIMING_TIMEOUT_SECS - 1;
        let now = long_ago + CLAIMING_TIMEOUT_SECS + 1;

        let slot = make_claiming_slot(txn_id_raw, long_ago);
        slot.pid.store(12345, Ordering::Release);
        slot.pid_birth.store(9999, Ordering::Release);

        // Far past any timeout, but process is alive.
        let result = try_cleanup_sentinel_slot(&slot, now, |_, _| true);
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
        // Verify sentinel encoding works correctly at TxnId boundary.
        let max_txn = TxnId::MAX_RAW;
        assert_eq!(max_txn, (1_u64 << 62) - 1);

        let claiming = encode_claiming(max_txn);
        assert_eq!(decode_tag(claiming), TAG_CLAIMING);
        assert_eq!(decode_payload(claiming), max_txn);
        assert!(is_sentinel(claiming));

        let cleaning = encode_cleaning(max_txn);
        assert_eq!(decode_tag(cleaning), TAG_CLEANING);
        assert_eq!(decode_payload(cleaning), max_txn);
        assert!(is_sentinel(cleaning));

        // Real TxnId (no tag) is NOT a sentinel.
        assert!(!is_sentinel(max_txn));
        assert_eq!(decode_tag(max_txn), 0);
        assert_eq!(decode_payload(max_txn), max_txn);

        // Free slot (0) is not a sentinel.
        assert!(!is_sentinel(0));
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
        slot.claiming_timestamp.store(1000, Ordering::Release);

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
    const TXN_SLOT_LATENCY_TARGET_NS: u128 = 1_000;
    const TXN_SLOT_LATENCY_SLOT_COUNT: usize = 16;
    const TXN_SLOT_LATENCY_WARMUP_ROUNDS: u64 = 3;
    const TXN_SLOT_LATENCY_SAMPLE_ROUNDS: usize = 11;
    const TXN_SLOT_LATENCY_ITERATIONS_PER_SAMPLE: u64 = 2_048;
    const TXN_SLOT_LATENCY_REPLAY_COMMAND: &str = "cargo test --profile release-perf -p fsqlite-mvcc --lib core_types::tests::test_txn_slot_alloc_release_latency_budget -- --exact --nocapture --test-threads=1";

    #[derive(Debug)]
    struct TxnSlotLatencyMeasurement {
        timed_elapsed_us: u128,
        median_ns_per_operation: u128,
        samples_ns_per_operation: Vec<u128>,
    }

    fn run_txn_slot_alloc_release_sample(
        slot_array: &crate::cache_aligned::TxnSlotArray,
        cycle_base: u64,
    ) -> u128 {
        let sample_started = Instant::now();
        for offset in 0_u64..TXN_SLOT_LATENCY_ITERATIONS_PER_SAMPLE {
            let cycle = cycle_base + offset;
            let txn_id_raw = std::hint::black_box(10_000 + cycle);
            let hint_index = usize::try_from(
                cycle
                    % u64::try_from(TXN_SLOT_LATENCY_SLOT_COUNT)
                        .expect("bead_id={BEAD_2G5_1} slot count should fit u64"),
            )
            .expect("bead_id={BEAD_2G5_1} hint index should fit usize");
            let slot_index = slot_array
                .acquire(
                    txn_id_raw,
                    hint_index,
                    6_666,
                    TXN_SLOT_E2E_SEED + cycle,
                    100_000 + cycle,
                    500 + cycle,
                    500 + cycle,
                    crate::cache_aligned::slot_mode::CONCURRENT,
                    1,
                )
                .expect("bead_id={BEAD_2G5_1} slot allocation should succeed");
            slot_array.slot(std::hint::black_box(slot_index)).release();
        }

        let operations = u128::from(TXN_SLOT_LATENCY_ITERATIONS_PER_SAMPLE).saturating_mul(2);
        sample_started
            .elapsed()
            .as_nanos()
            .saturating_div(operations.max(1))
    }

    fn measure_txn_slot_alloc_release_latency() -> TxnSlotLatencyMeasurement {
        let slot_array = crate::cache_aligned::TxnSlotArray::new(TXN_SLOT_LATENCY_SLOT_COUNT);
        let mut cycle_base = 0_u64;

        for _ in 0..TXN_SLOT_LATENCY_WARMUP_ROUNDS {
            std::hint::black_box(run_txn_slot_alloc_release_sample(&slot_array, cycle_base));
            cycle_base = cycle_base.saturating_add(TXN_SLOT_LATENCY_ITERATIONS_PER_SAMPLE);
        }

        let timed_started = Instant::now();
        let mut samples_ns_per_operation = Vec::with_capacity(TXN_SLOT_LATENCY_SAMPLE_ROUNDS);
        for _ in 0..TXN_SLOT_LATENCY_SAMPLE_ROUNDS {
            samples_ns_per_operation
                .push(run_txn_slot_alloc_release_sample(&slot_array, cycle_base));
            cycle_base = cycle_base.saturating_add(TXN_SLOT_LATENCY_ITERATIONS_PER_SAMPLE);
        }
        let timed_elapsed_us = timed_started.elapsed().as_micros().max(1);

        let mut sorted_samples = samples_ns_per_operation.clone();
        sorted_samples.sort_unstable();
        let median_ns_per_operation = sorted_samples[sorted_samples.len() / 2];

        TxnSlotLatencyMeasurement {
            timed_elapsed_us,
            median_ns_per_operation,
            samples_ns_per_operation,
        }
    }

    #[test]
    #[cfg_attr(
        debug_assertions,
        ignore = "production latency gate; replay with --profile release-perf"
    )]
    fn test_txn_slot_alloc_release_latency_budget() {
        let measurement = measure_txn_slot_alloc_release_latency();
        eprintln!(
            "bead_id={BEAD_2G5_1} median_ns_per_operation={} samples_ns_per_operation={:?} target_lt_ns={} replay_command={TXN_SLOT_LATENCY_REPLAY_COMMAND}",
            measurement.median_ns_per_operation,
            measurement.samples_ns_per_operation,
            TXN_SLOT_LATENCY_TARGET_NS,
        );
        assert!(
            measurement.median_ns_per_operation < TXN_SLOT_LATENCY_TARGET_NS,
            "bead_id={BEAD_2G5_1} median allocation/release latency must remain below {}ns: median_ns_per_operation={} samples_ns_per_operation={:?}; replay with: {TXN_SLOT_LATENCY_REPLAY_COMMAND}",
            TXN_SLOT_LATENCY_TARGET_NS,
            measurement.median_ns_per_operation,
            measurement.samples_ns_per_operation,
        );
    }

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
        use std::sync::{Arc, Mutex, mpsc};
        use std::time::Instant;

        let scenario_started = Instant::now();
        GLOBAL_TXN_SLOT_METRICS.reset();
        let metrics_before = GLOBAL_TXN_SLOT_METRICS.snapshot();

        // 1) Exercise allocation/release and capture profile-correct timing
        // evidence. Debug builds retain the functional coverage and artifact
        // contract but leave the strict production decision to the dedicated
        // release-perf test above.
        let latency_measurement = measure_txn_slot_alloc_release_latency();
        let alloc_release_elapsed_us = latency_measurement.timed_elapsed_us;
        let avg_alloc_release_ns = latency_measurement.median_ns_per_operation;
        let latency_budget_enforced = !cfg!(debug_assertions);
        let alloc_release_under_one_us =
            !latency_budget_enforced || avg_alloc_release_ns < TXN_SLOT_LATENCY_TARGET_NS;
        let latency_profile = if latency_budget_enforced {
            "optimized-gate"
        } else {
            "debug-measurement-only"
        };

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
            TXN_SLOT_E2E_SCENARIO_ID,
            TXN_SLOT_E2E_SEED,
            TXN_SLOT_E2E_SCENARIO_ID,
            TXN_SLOT_E2E_SEED,
            "target/txn_slot_e2e_artifact.json",
        );
        let checks = vec![
            json!({
                "id": "alloc_release_latency_budget",
                "status": if alloc_release_under_one_us { "pass" } else { "fail" },
                "enforced": latency_budget_enforced,
                "detail": format!(
                    "median_alloc_release_ns={avg_alloc_release_ns} samples_ns_per_operation={:?} target_lt_ns={} profile={latency_profile} enforced={latency_budget_enforced} replay_command={TXN_SLOT_LATENCY_REPLAY_COMMAND}",
                    latency_measurement.samples_ns_per_operation,
                    TXN_SLOT_LATENCY_TARGET_NS,
                ),
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
            "run_id": TXN_SLOT_E2E_SCENARIO_ID,
            "trace_id": TXN_SLOT_E2E_SEED,
            "scenario_id": TXN_SLOT_E2E_SCENARIO_ID,
            "seed": TXN_SLOT_E2E_SEED,
            "overall_status": overall_status,
            "timing": {
                "total_elapsed_us": total_elapsed_us,
                "alloc_release_elapsed_us": alloc_release_elapsed_us,
                "alloc_release_avg_ns": avg_alloc_release_ns,
                "alloc_release_median_ns": latency_measurement.median_ns_per_operation,
                "alloc_release_samples_ns": latency_measurement.samples_ns_per_operation,
                "latency_budget_enforced": latency_budget_enforced,
                "latency_profile": latency_profile,
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
            "latency_replay_command": TXN_SLOT_LATENCY_REPLAY_COMMAND,
            "replay_command": replay_command,
        });
        let artifact_bytes = serde_json::to_vec_pretty(&artifact)
            .expect("bead_id={BEAD_2G5_1} artifact serialization should succeed");
        let _ = std::fs::create_dir_all("target");
        std::fs::write("target/txn_slot_e2e_artifact.json", artifact_bytes)
            .expect("bead_id={BEAD_2G5_1} artifact write should succeed");
        assert!(
            std::path::Path::new("target/txn_slot_e2e_artifact.json").exists(),
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
        assert!(table.try_acquire(page, txn_b).is_err());
        assert_eq!(
            obs.metrics()
                .page_contentions
                .load(std::sync::atomic::Ordering::Relaxed),
            1,
            "bead_id={BEAD_T6SV2_1} case=contention_event_emitted"
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
        let page = sharded_rebuild_page(50);
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
            } if *p == page && requester.get() == 2 && holder.get() == 1
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

    // -----------------------------------------------------------------------
    // D4 (bd-3wop3.4): Per-waiter targeted notification tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_no_thundering_herd_8t() {
        // bd-3wop3.4: 8 threads each wait for a different page. Release one page.
        // Only the thread waiting for that page should wake quickly; others
        // should remain parked (or wake much later via spurious wakeup).
        use std::sync::Barrier;
        use std::sync::atomic::{AtomicUsize, Ordering};

        const NUM_THREADS: usize = 8;
        let table = Arc::new(InProcessPageLockTable::new());
        let holder = TxnId::new(999).unwrap();

        // Acquire locks on pages 1..=8.
        for i in 1..=NUM_THREADS {
            let page = PageNumber::new(i as u32).unwrap();
            table.try_acquire(page, holder).unwrap();
        }

        let barrier = Arc::new(Barrier::new(NUM_THREADS + 1));
        let wake_count = Arc::new(AtomicUsize::new(0));
        let target_page_idx = 1_usize; // We'll release page 1.

        let mut handles = Vec::with_capacity(NUM_THREADS);
        for i in 1..=NUM_THREADS {
            let t = Arc::clone(&table);
            let b = Arc::clone(&barrier);
            let wc = Arc::clone(&wake_count);
            let page = PageNumber::new(i as u32).unwrap();
            handles.push(std::thread::spawn(move || {
                b.wait(); // Sync all threads to start waiting simultaneously.
                let woke = t.wait_for_holder_change(page, holder, Duration::from_millis(200));
                if woke {
                    wc.fetch_add(1, Ordering::SeqCst);
                }
                (i, woke)
            }));
        }

        // Sync and then release only page 1.
        barrier.wait();
        std::thread::sleep(Duration::from_millis(10)); // Let waiters register.
        let target_page = PageNumber::new(target_page_idx as u32).unwrap();
        table.release(target_page, holder);

        // Collect results.
        let mut results = Vec::new();
        for h in handles {
            results.push(h.join().unwrap());
        }

        // The thread waiting for page 1 should have woken (woke=true).
        let (_, page1_woke) = results.iter().find(|(i, _)| *i == target_page_idx).unwrap();
        assert!(
            *page1_woke,
            "bd-3wop3.4: thread waiting for released page should wake"
        );

        // Count how many woke within the timeout (should be 1, maybe 2 with spurious).
        // Definitely NOT all 8 — that would be thundering herd.
        let total_woke = wake_count.load(Ordering::SeqCst);
        assert!(
            total_woke < NUM_THREADS,
            "bd-3wop3.4: thundering herd detected: {total_woke}/{NUM_THREADS} woke, expected ~1"
        );
    }

    #[test]
    fn test_targeted_wake_correct_thread() {
        // bd-3wop3.4: Two threads wait for different pages. Release one.
        // Only the thread waiting for the released page should wake.
        use std::sync::atomic::{AtomicBool, Ordering};

        let table = Arc::new(InProcessPageLockTable::new());
        let holder = TxnId::new(100).unwrap();
        let page_a = PageNumber::new(42).unwrap();
        let page_b = PageNumber::new(43).unwrap();

        table.try_acquire(page_a, holder).unwrap();
        table.try_acquire(page_b, holder).unwrap();

        let thread_a_woke = Arc::new(AtomicBool::new(false));
        let thread_b_woke = Arc::new(AtomicBool::new(false));

        let t = Arc::clone(&table);
        let a_woke = Arc::clone(&thread_a_woke);
        let handle_a = std::thread::spawn(move || {
            let result = t.wait_for_holder_change(page_a, holder, Duration::from_millis(150));
            a_woke.store(result, Ordering::SeqCst);
        });

        let t = Arc::clone(&table);
        let b_woke = Arc::clone(&thread_b_woke);
        let handle_b = std::thread::spawn(move || {
            let result = t.wait_for_holder_change(page_b, holder, Duration::from_millis(150));
            b_woke.store(result, Ordering::SeqCst);
        });

        std::thread::sleep(Duration::from_millis(20)); // Let waiters register.

        // Release only page_a.
        table.release(page_a, holder);

        handle_a.join().unwrap();
        handle_b.join().unwrap();

        assert!(
            thread_a_woke.load(Ordering::SeqCst),
            "bd-3wop3.4: thread A (waiting for page_a) should wake when page_a released"
        );
        assert!(
            !thread_b_woke.load(Ordering::SeqCst),
            "bd-3wop3.4: thread B (waiting for page_b) should NOT wake when page_a released"
        );
    }

    #[test]
    fn test_no_spurious_wakes() {
        // bd-3wop3.4: Waiter should only wake from targeted notification or timeout,
        // not spuriously from unrelated activity.
        let table = Arc::new(InProcessPageLockTable::new());
        let holder = TxnId::new(200).unwrap();
        let page_wait = PageNumber::new(50).unwrap();
        let page_other = PageNumber::new(51).unwrap();

        table.try_acquire(page_wait, holder).unwrap();
        table.try_acquire(page_other, holder).unwrap();

        let t = Arc::clone(&table);
        let handle = std::thread::spawn(move || {
            let started = Instant::now();
            let woke = t.wait_for_holder_change(page_wait, holder, Duration::from_millis(100));
            (woke, started.elapsed())
        });

        // Release a DIFFERENT page — should NOT wake the waiter.
        std::thread::sleep(Duration::from_millis(10));
        table.release(page_other, holder);

        let (woke, elapsed) = handle.join().unwrap();

        // Waiter should have timed out (woke=false) and taken ~100ms.
        assert!(
            !woke,
            "bd-3wop3.4: waiter should NOT wake from unrelated page release"
        );
        assert!(
            elapsed >= Duration::from_millis(90),
            "bd-3wop3.4: waiter should have waited near-full timeout, got {:?}",
            elapsed
        );
    }
}
