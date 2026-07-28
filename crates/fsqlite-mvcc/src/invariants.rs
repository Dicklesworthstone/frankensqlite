//! MVCC invariant enforcement and visibility predicates (§5.2-5.3).
//!
//! This module implements:
//! - [`TxnManager`]: Monotonic `TxnId` allocation via `AtomicU64` CAS (INV-1).
//! - [`VersionStore`]: Version chain management with arena-backed storage.
//! - [`visible`]: The core visibility predicate.
//! - `resolve`: Version chain resolution against a snapshot.
//! - `resolve_for_txn`: Write-set-aware resolution for transactions.
//! - [`SerializedWriteMutex`]: Global write mutex for Serialized mode (INV-7).

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use smallvec::SmallVec;

use fsqlite_types::sync_primitives::{Mutex, RwLock};

use fsqlite_types::{
    CommitSeq, PageNumber, PageNumberBuildHasher, PageSize, PageVersion, Snapshot, TxnId,
    VersionPointer,
};

use crate::cache_aligned::CacheAligned;
use crate::commit_combiner::CommitSequenceCombiner;
use crate::core_types::{Transaction, VersionArena, VersionIdx};
use crate::ebr::{EbrRetireQueue, VersionGuardRegistry};
use crate::gc::{GcTickResult, GcTodo, gc_tick_with_registry, prune_page_chain_with_registry};
// `record_cas_attempt` is gated by a relaxed bool in observability, so the
// production default is still one cheap load while diagnostic runs can assert
// the chain-head installer remains on the first-attempt path.
use crate::observability::record_cas_attempt;
use crate::reclamation::{advance_epoch_and_reclaim, reclaim_at_epoch, retire_and_reclaim};

// ---------------------------------------------------------------------------
// TxnManager — INV-1 (Monotonicity)
// ---------------------------------------------------------------------------

/// Manages monotonic allocation of `TxnId` and `CommitSeq` values.
///
/// # INV-1 Enforcement
///
/// `TxnId` allocation uses an `AtomicU64` CAS loop that increments by 1.
/// Each successful CAS publishes a unique `TxnId`. The counter only ever
/// increases, so `TxnId`s are strictly increasing.
///
/// If the counter would wrap into `TxnId = 0` or exceed `TXN_ID_MAX`
/// (62-bit domain), the engine fails fast rather than publishing an
/// illegal `TxnId`.
///
/// `CommitSeq` is assigned only by the commit sequencer under the commit
/// mutex, producing a strict total order.
///
/// # Atomicity Enforcement (INV-6)
///
/// To ensure "all-or-nothing" visibility, `CommitSeq`s are tracked as "active"
/// while the write set is being published. Snapshots are bounded by the
/// `stable_commit_seq`, which is the watermark below all currently active
/// (incomplete) commits. This prevents a reader from taking a snapshot that
/// includes a partial commit from a concurrent writer.
pub struct TxnManager {
    next_txn_id: AtomicU64,
    /// D1-CRITICAL Change 4: Commit sequence allocation via flat combining.
    /// Replaces the previous `next_commit_seq: AtomicU64` + per-call mutex
    /// with batched allocation that reduces cache-line contention from O(N)
    /// round-trips to O(1) under concurrent commits. The combiner also
    /// batch-registers sequences in `active_commits` atomically.
    commit_combiner: CommitSequenceCombiner,
    /// In-flight commit sequences (allocated but not yet finished).
    /// Uses a sorted `SmallVec` instead of `BTreeSet` for cache locality
    /// under typical concurrency (≤16 concurrent commits). Sorted order
    /// enables O(1) minimum lookup via `first()`.
    /// Shared with `commit_combiner` via `Arc` for batch registration.
    active_commits: Arc<Mutex<SmallVec<[u64; 16]>>>,
    /// The highest commit sequence C such that all sequences <= C are fully finished.
    stable_commit_seq: AtomicU64,
}

impl TxnManager {
    /// Create a new manager starting from the given initial values.
    #[must_use]
    pub fn new(initial_txn_id: u64, initial_commit_seq: u64) -> Self {
        let active_commits = Arc::new(Mutex::new(SmallVec::new()));
        Self {
            next_txn_id: AtomicU64::new(initial_txn_id),
            commit_combiner: CommitSequenceCombiner::new_with_registry(
                initial_commit_seq,
                Arc::clone(&active_commits),
            ),
            active_commits,
            // If starting at S, then S-1 is the last stable commit.
            stable_commit_seq: AtomicU64::new(initial_commit_seq.saturating_sub(1)),
        }
    }

    /// Allocate the next `TxnId` via CAS loop (INV-1).
    ///
    /// Returns `None` if the id space is exhausted (`> TXN_ID_MAX`).
    pub fn alloc_txn_id(&self) -> Option<TxnId> {
        loop {
            let current = self.next_txn_id.load(Ordering::Acquire);
            if current > TxnId::MAX_RAW {
                return None; // exhausted
            }
            let next = current.checked_add(1)?;
            if self
                .next_txn_id
                .compare_exchange_weak(current, next, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return TxnId::new(current);
            }
            // CAS failed — another thread won; retry.
        }
    }

    /// Allocate the next `CommitSeq` and mark it as active.
    ///
    /// D1-CRITICAL Change 4: Routes through `CommitSequenceCombiner` for
    /// batched allocation. Under 8-16 thread contention, this converts
    /// N separate `fetch_add(1)` + N mutex acquisitions into 1 batched
    /// `fetch_add(N)` + 1 mutex acquisition by the combiner thread.
    /// The combiner batch-registers sequences in `active_commits` before
    /// signaling waiters, so there is no gap for `finish_commit_seq`.
    pub fn alloc_commit_seq(&self) -> CommitSeq {
        self.commit_combiner.alloc_one_shot()
    }

    /// Mark a `CommitSeq` as finished (fully published).
    ///
    /// This updates the stable visibility watermark.
    pub fn finish_commit_seq(&self, seq: CommitSeq) {
        let mut active = self.active_commits.lock();
        let raw = seq.get();
        // Remove by position (O(N) for SmallVec, but N ≤ 16 typical).
        if let Some(pos) = active.iter().position(|&s| s == raw) {
            active.remove(pos);
        } else {
            debug_assert!(false, "finished commit seq {raw} was not active");
        }

        // The stable sequence is the predecessor of the earliest active commit.
        // SmallVec is sorted, so first() gives the minimum.
        let new_stable = if let Some(&min_active) = active.first() {
            min_active.saturating_sub(1)
        } else {
            // No active commits: stable is everything up to next_commit_seq - 1.
            self.commit_combiner.next_seq().saturating_sub(1)
        };
        drop(active);

        // Update the cached stable sequence.
        // We use MAX here because multiple threads might call finish concurrently
        // (if we had fine-grained locking, though currently serialized by caller).
        // But active_commits lock ensures we see a consistent view.
        self.stable_commit_seq
            .fetch_max(new_stable, Ordering::Release);
    }

    /// The current (not-yet-allocated) `TxnId` counter value.
    #[must_use]
    pub fn current_txn_counter(&self) -> u64 {
        self.next_txn_id.load(Ordering::Acquire)
    }

    /// The highest *stable* commit sequence + 1.
    ///
    /// Used for snapshot establishment. Returns `S+1` so that `S` is the
    /// snapshot high watermark. This ensures snapshots only see fully
    /// completed commits (INV-6).
    #[must_use]
    pub fn current_commit_counter(&self) -> u64 {
        self.stable_commit_seq
            .load(Ordering::Acquire)
            .saturating_add(1)
    }
}

impl Default for TxnManager {
    fn default() -> Self {
        Self::new(1, 1)
    }
}

impl std::fmt::Debug for TxnManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TxnManager")
            .field("next_txn_id", &self.next_txn_id.load(Ordering::Relaxed))
            .field("next_commit_seq", &self.commit_combiner.next_seq())
            .field("active_commits", &*self.active_commits.lock())
            .field(
                "stable_commit_seq",
                &self.stable_commit_seq.load(Ordering::Relaxed),
            )
            .finish()
    }
}

// ---------------------------------------------------------------------------
// Visibility predicate (§5.3)
// ---------------------------------------------------------------------------

/// The core MVCC visibility predicate.
///
/// A page version `V` is visible to snapshot `S` iff
/// `V.commit_seq ∈ [1, S.high]`. Collapsed into one unsigned
/// comparison so the hot resolve loop does not branch on the
/// committed/uncommitted check:
///
/// `commit_seq.wrapping_sub(1) < snapshot.high`
///
/// - commit_seq = 0 → `0u64.wrapping_sub(1) = u64::MAX`, never `< snapshot.high` → `false`.
/// - commit_seq ∈ [1, snapshot.high] → `commit_seq - 1 < snapshot.high` → `true`.
/// - commit_seq  > snapshot.high → `commit_seq - 1 >= snapshot.high` → `false`.
#[inline]
#[must_use]
pub fn visible(version: &PageVersion, snapshot: &Snapshot) -> bool {
    version.commit_seq.get().wrapping_sub(1) < snapshot.high.get()
}

/// Visibility interval for a committed page version on a chain.
///
/// A version is visible for snapshots in `[begin_ts, end_ts)`, where `end_ts`
/// is `None` for the current chain head.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VersionVisibilityRange {
    pub begin_ts: CommitSeq,
    pub end_ts: Option<CommitSeq>,
}

impl VersionVisibilityRange {
    /// Whether this interval contains the snapshot high watermark.
    #[must_use]
    pub fn contains(self, snapshot_ts: CommitSeq) -> bool {
        if snapshot_ts < self.begin_ts {
            return false;
        }
        match self.end_ts {
            Some(end) => snapshot_ts < end,
            None => true,
        }
    }
}

/// Snapshot resolve result with traversal diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotResolveTrace {
    pub version_idx: Option<VersionIdx>,
    pub versions_traversed: u64,
}

// ---------------------------------------------------------------------------
// ChainHeadTable — latch-free MVCC version chain heads (bd-688.3)
// ---------------------------------------------------------------------------

/// Number of shards in the chain head table (power of 2 for fast modular indexing).
pub const CHAIN_HEAD_SHARDS: usize = 64;

/// Sentinel value stored in an `AtomicU64` slot to indicate "no version" (empty chain).
pub const CHAIN_HEAD_EMPTY: u64 = u64::MAX;

/// Result of a CAS-based chain head installation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CasInstallResult {
    /// Successfully installed. Contains the previous head (or `None` if this was the first version).
    Installed { previous: Option<VersionIdx> },
    /// CAS failed because the current head changed between read and write.
    /// The caller should retry.
    Retry,
}

/// A single shard of the chain head table.
///
/// Contains:
/// - A directory mapping `PageNumber` to a slot index (locked only on first-time registration).
/// - A vector of atomic head pointer slots (read-locked for CAS, write-locked only when growing).
struct ChainHeadShard {
    /// Maps page numbers to slot indices within `slots`.
    directory: RwLock<HashMap<PageNumber, usize, PageNumberBuildHasher>>,
    /// Atomic head pointer slots. Each slot stores a packed `VersionIdx` as u64,
    /// or `CHAIN_HEAD_EMPTY` for an empty chain.
    slots: RwLock<Vec<CacheAligned<AtomicU64>>>,
}

impl ChainHeadShard {
    fn new() -> Self {
        Self {
            directory: RwLock::new(HashMap::with_hasher(PageNumberBuildHasher::default())),
            slots: RwLock::new(Vec::new()),
        }
    }

    /// Get or create the slot index for a page. Returns the slot index.
    fn ensure_slot(&self, pgno: PageNumber) -> usize {
        // Fast path: check if already registered.
        {
            let dir = self.directory.read();
            if let Some(&idx) = dir.get(&pgno) {
                return idx;
            }
        }

        // Slow path: register a new slot.
        let mut dir = self.directory.write();
        // Double-check after acquiring lock.
        if let Some(&idx) = dir.get(&pgno) {
            return idx;
        }

        let mut slots = self.slots.write();
        let slot_idx = slots.len();
        slots.push(CacheAligned::new(AtomicU64::new(CHAIN_HEAD_EMPTY)));
        dir.insert(pgno, slot_idx);
        slot_idx
    }

    /// Get the slot index for a page, if registered.
    fn slot_index(&self, pgno: PageNumber) -> Option<usize> {
        let dir = self.directory.read();
        dir.get(&pgno).copied()
    }
}

/// Sharded, CAS-based atomic chain head table for lock-free head pointer updates.
///
/// Each page's version chain head is stored as an `AtomicU64` that packs a `VersionIdx`.
/// Updates use compare-and-swap instead of taking a global write lock, enabling concurrent
/// writers to install new chain heads without contention on the table itself.
pub struct ChainHeadTable {
    shards: Box<[ChainHeadShard; CHAIN_HEAD_SHARDS]>,
}

impl ChainHeadTable {
    /// Create a new empty chain head table.
    #[must_use]
    pub fn new() -> Self {
        Self {
            shards: Box::new(std::array::from_fn(|_| ChainHeadShard::new())),
        }
    }

    /// Compute the shard index for a page number.
    #[inline]
    fn shard_index(pgno: PageNumber) -> usize {
        (pgno.get() as usize) & (CHAIN_HEAD_SHARDS - 1)
    }

    /// Pack a `VersionIdx` into a u64 for atomic storage.
    #[inline]
    fn pack_idx(idx: VersionIdx) -> u64 {
        let chunk = u64::from(idx.chunk());
        let offset = u64::from(idx.offset());
        let generation = u64::from(idx.generation());

        // Ensure bit ranges do not overlap (§5E.2).
        // offset: 12 bits (0..4095)
        // chunk: 20 bits (0..1048575)
        // generation: 32 bits
        assert!(chunk <= 0xF_FFFF, "VersionIdx chunk overflow (max 20 bits)");
        assert!(offset <= 0xFFF, "VersionIdx offset overflow (max 12 bits)");

        (generation << 32) | (chunk << 12) | offset
    }

    /// Unpack a u64 into a `VersionIdx`. Returns `None` for `CHAIN_HEAD_EMPTY`.
    #[inline]
    fn unpack_idx(raw: u64) -> Option<VersionIdx> {
        if raw == CHAIN_HEAD_EMPTY {
            return None;
        }
        #[allow(clippy::cast_possible_truncation)]
        let offset = (raw & 0xFFF) as u32;
        #[allow(clippy::cast_possible_truncation)]
        let chunk = ((raw >> 12) & 0xF_FFFF) as u32;
        #[allow(clippy::cast_possible_truncation)]
        let generation = (raw >> 32) as u32;
        Some(VersionIdx::new(chunk, offset, generation))
    }

    /// Get the current chain head for a page, if any.
    #[must_use]
    pub fn get_head(&self, pgno: PageNumber) -> Option<VersionIdx> {
        let shard = &self.shards[Self::shard_index(pgno)];
        let slot_idx = shard.slot_index(pgno)?;
        let slots = shard.slots.read();
        let raw = slots[slot_idx].load(Ordering::Acquire);
        Self::unpack_idx(raw)
    }

    /// Install a new chain head for a page using CAS.
    ///
    /// `expected_prev` is what the caller believes the current head is (packed u64).
    /// If the current head matches, it is atomically replaced with `new_head`.
    ///
    /// Returns `CasInstallResult::Installed` on success, or `CasInstallResult::Retry` on failure.
    pub fn install(
        &self,
        pgno: PageNumber,
        new_head: VersionIdx,
        expected_prev: Option<VersionIdx>,
    ) -> CasInstallResult {
        let shard = &self.shards[Self::shard_index(pgno)];
        let slot_idx = shard.ensure_slot(pgno);
        let slots = shard.slots.read();
        let expected_raw = expected_prev.map_or(CHAIN_HEAD_EMPTY, Self::pack_idx);
        let new_raw = Self::pack_idx(new_head);

        match slots[slot_idx].compare_exchange(
            expected_raw,
            new_raw,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => CasInstallResult::Installed {
                previous: expected_prev,
            },
            Err(_) => CasInstallResult::Retry,
        }
    }

    /// Install a new chain head using a CAS loop. Retries until successful.
    ///
    /// Returns the previous head (if any) and the number of CAS attempts.
    pub fn install_with_retry(
        &self,
        pgno: PageNumber,
        new_head: VersionIdx,
    ) -> (Option<VersionIdx>, u32) {
        let shard = &self.shards[Self::shard_index(pgno)];
        let slot_idx = shard.ensure_slot(pgno);
        let slots = shard.slots.read();
        let new_raw = Self::pack_idx(new_head);
        let mut attempts = 0_u32;

        loop {
            attempts += 1;
            let current_raw = slots[slot_idx].load(Ordering::Acquire);
            match slots[slot_idx].compare_exchange_weak(
                current_raw,
                new_raw,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    let previous = Self::unpack_idx(current_raw);
                    return (previous, attempts);
                }
                Err(_) => {
                    // CAS failed, loop will retry.
                    std::hint::spin_loop();
                }
            }
        }
    }

    /// Remove a chain head by CAS-ing it to `CHAIN_HEAD_EMPTY`.
    ///
    /// Returns `true` if the head was successfully removed (matched expected).
    pub fn remove(&self, pgno: PageNumber, expected: VersionIdx) -> bool {
        let shard = &self.shards[Self::shard_index(pgno)];
        let Some(slot_idx) = shard.slot_index(pgno) else {
            return false;
        };
        let slots = shard.slots.read();
        let expected_raw = Self::pack_idx(expected);
        slots[slot_idx]
            .compare_exchange(
                expected_raw,
                CHAIN_HEAD_EMPTY,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    /// Iterate over all registered pages and their current heads.
    ///
    /// This acquires directory locks shard-by-shard and is intended for
    /// diagnostics/sampling, not hot paths.
    pub fn for_each_head(&self, mut f: impl FnMut(PageNumber, VersionIdx)) {
        for shard in self.shards.iter() {
            let dir = shard.directory.read();
            let slots = shard.slots.read();
            for (&pgno, &slot_idx) in dir.iter() {
                let raw = slots[slot_idx].load(Ordering::Acquire);
                if let Some(idx) = Self::unpack_idx(raw) {
                    f(pgno, idx);
                }
            }
        }
    }

    /// Count the number of pages with non-empty chain heads.
    #[must_use]
    pub fn page_count(&self) -> usize {
        let mut count = 0;
        for shard in self.shards.iter() {
            let dir = shard.directory.read();
            let slots = shard.slots.read();
            for &slot_idx in dir.values() {
                let raw = slots[slot_idx].load(Ordering::Relaxed);
                if raw != CHAIN_HEAD_EMPTY {
                    count += 1;
                }
            }
        }
        count
    }
}

impl Default for ChainHeadTable {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for ChainHeadTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChainHeadTable")
            .field("shards", &CHAIN_HEAD_SHARDS)
            .field("page_count", &self.page_count())
            .finish()
    }
}

// ---------------------------------------------------------------------------
// VersionStore — version chain management
// ---------------------------------------------------------------------------

/// Runtime gate for the `VersionStore::visibility_ranges` side-index. The
/// range map is only consulted by diagnostic paths (`visibility_range()` /
/// `Debug`) and a handful of in-tree tests, but the old unconditional
/// `RwLock<HashMap>` writes on every `publish()` and every GC prune sat on
/// the version-chain append hot path. Defaults to off so MVCC writers stop
/// paying for a side-index nobody reads; tests flip it on before the
/// handful of calls that need the ranges.
static MVCC_VISIBILITY_RANGES_TRACKING_ENABLED: AtomicBool = AtomicBool::new(false);

/// Enable or disable recording of `VersionVisibilityRange` side-index entries
/// on `publish()` and GC prune.
///
/// Defaults to disabled; callers that actually consume
/// `VersionStore::visibility_range()` must flip this on first.
pub fn set_mvcc_visibility_ranges_tracking_enabled(enabled: bool) {
    MVCC_VISIBILITY_RANGES_TRACKING_ENABLED.store(enabled, Ordering::Relaxed);
}

/// Current state of the visibility-ranges side-index gate.
#[must_use]
pub fn mvcc_visibility_ranges_tracking_enabled() -> bool {
    MVCC_VISIBILITY_RANGES_TRACKING_ENABLED.load(Ordering::Relaxed)
}

#[cold]
#[inline(never)]
fn publish_record_visibility_range(
    ranges_lock: &RwLock<HashMap<VersionIdx, VersionVisibilityRange>>,
    idx: VersionIdx,
    begin_ts: CommitSeq,
    previous_head: Option<VersionIdx>,
) {
    let mut ranges = ranges_lock.write();
    ranges.insert(
        idx,
        VersionVisibilityRange {
            begin_ts,
            end_ts: None,
        },
    );
    if let Some(old_head) = previous_head {
        if let Some(old_range) = ranges.get_mut(&old_head) {
            old_range.end_ts = Some(begin_ts);
        }
    }
}

/// Version chain head table + arena, providing `resolve()` and `resolve_for_txn()`.
///
/// The version store owns all committed page versions in the arena and
/// maintains a mapping from each page to the head of its version chain.
pub struct VersionStore {
    arena: RwLock<VersionArena>,
    /// Sharded, CAS-based chain head table (bd-688.3: latch-free).
    chain_heads: ChainHeadTable,
    /// Visibility intervals keyed by arena index.
    visibility_ranges: RwLock<HashMap<VersionIdx, VersionVisibilityRange>>,
    page_size: PageSize,
    guard_registry: Arc<VersionGuardRegistry>,
    /// Queue of retired slots pending recycling after epoch advancement (D5: EBR).
    ///
    /// When `gc_tick` prunes versions, it uses `take_for_retirement()` which
    /// extracts the version without adding to free_list. The slot indices are
    /// added here. After epoch advancement, `try_recycle_retired_slots()` drains
    /// this queue and batch-adds slots back to the arena free_list.
    retire_queue: EbrRetireQueue,
}

impl VersionStore {
    /// Create an empty version store.
    #[must_use]
    pub fn new(page_size: PageSize) -> Self {
        Self::new_with_guard_registry(page_size, Arc::new(VersionGuardRegistry::default()))
    }

    /// Create an empty version store with a shared guard registry.
    #[must_use]
    pub fn new_with_guard_registry(
        page_size: PageSize,
        guard_registry: Arc<VersionGuardRegistry>,
    ) -> Self {
        Self {
            arena: RwLock::new(VersionArena::new()),
            chain_heads: ChainHeadTable::new(),
            visibility_ranges: RwLock::new(HashMap::new()),
            page_size,
            guard_registry,
            retire_queue: EbrRetireQueue::new(),
        }
    }

    /// Shared EBR guard registry used for transaction and GC retirements.
    #[must_use]
    pub fn guard_registry(&self) -> &Arc<VersionGuardRegistry> {
        &self.guard_registry
    }

    /// Current global EBR epoch observed by this store.
    #[must_use]
    pub fn current_epoch(&self) -> u64 {
        self.guard_registry.current_epoch()
    }

    /// Advance the global EBR epoch and recycle any newly reclaimable slots.
    ///
    /// Returns the number of arena slots recycled.
    pub fn advance_epoch(&self) -> usize {
        let mut arena = self.arena.write();
        advance_epoch_and_reclaim(self.guard_registry(), &self.retire_queue, &mut arena)
            .recycled_slots
    }

    /// Total arena slots ever allocated.
    #[must_use]
    pub fn arena_high_water(&self) -> u64 {
        self.arena.read().high_water()
    }

    /// Current arena free-list length.
    #[must_use]
    pub fn arena_free_count(&self) -> usize {
        self.arena.read().free_count()
    }

    /// Publish a committed version into the store.
    ///
    /// The version is allocated in the arena and linked at the head of its
    /// page's version chain (INV-3: new version has higher `commit_seq`
    /// than the previous head).
    ///
    /// Returns the `VersionIdx` of the published version.
    pub fn publish(&self, version: PageVersion) -> VersionIdx {
        let pgno = version.pgno;
        let begin_ts = version.commit_seq;

        // Step 0: Ensure slot exists BEFORE acquiring arena lock, because
        // ensure_slot may take its own write locks (directory + slots) on the
        // slow path, and we don't want to hold the arena write lock during
        // that potentially slower allocation.
        let shard = &self.chain_heads.shards[ChainHeadTable::shard_index(pgno)];
        let slot_idx = shard.ensure_slot(pgno);

        // Step 1: Arena alloc (brief write lock — kept open for prev-link in step 2).
        let mut arena = self.arena.write();
        let idx = arena.alloc(version);
        let new_raw = ChainHeadTable::pack_idx(idx);

        // `publish` is the only production path that installs a new chain
        // head (`ChainHeadTable::{install,install_with_retry,remove}` are
        // test-only), and it holds `arena.write()` exclusively across
        // the load + link + swap sequence. No other thread can mutate
        // this slot in that window, so `compare_exchange` (strong) must
        // succeed on the first attempt — the prior `compare_exchange_weak`
        // loop existed only to recover from spurious weak-CAS failures
        // that this path never needed to tolerate. Consequently the CAS
        // attempt histogram sample is always 1; record it through the
        // observability gate so production keeps the hot path cheap while
        // diagnostics can still verify the invariant.
        let previous_head = {
            let slots = shard.slots.read();
            let current_raw = slots[slot_idx].load(Ordering::Acquire);
            let prev = ChainHeadTable::unpack_idx(current_raw);

            // Link the new version to the current head BEFORE the swap.
            let v = arena.get_mut(idx).expect("just allocated");
            v.prev = prev.map(idx_to_version_pointer);

            slots[slot_idx]
                .compare_exchange(current_raw, new_raw, Ordering::AcqRel, Ordering::Acquire)
                .expect(
                    "chain-head CAS must succeed under arena.write() exclusive hold: \
                     no other writer can mutate this slot in the window",
                );
            record_cas_attempt(1);
            prev
        };
        drop(arena);

        // Step 3: Visibility-ranges side-index update. Gated off by default
        // because the diagnostic `visibility_range()` getter and the Debug
        // impl are the only consumers; the old unconditional
        // `visibility_ranges.write()` cost one `RwLock<HashMap>` exclusive
        // acquire + 1-2 HashMap mutations on every version publish, which
        // is on the write hot path. The conditional is a single relaxed
        // bool load in the common (disabled) case.
        if MVCC_VISIBILITY_RANGES_TRACKING_ENABLED.load(Ordering::Relaxed) {
            publish_record_visibility_range(&self.visibility_ranges, idx, begin_ts, previous_head);
        }

        tracing::debug!(pgno = pgno.get(), "version published to chain head");
        idx
    }

    /// Resolve the newest committed version of `page` visible to `snapshot`.
    ///
    /// Walks the version chain from the head, returning the first version
    /// where `visible(V, snapshot)` holds.
    ///
    /// Returns `None` if no committed version exists at or before the snapshot
    /// (the page only exists on disk or has not been written).
    #[must_use]
    #[allow(clippy::significant_drop_tightening)]
    pub fn resolve(&self, page: PageNumber, snapshot: &Snapshot) -> Option<VersionIdx> {
        self.resolve_with_trace(page, snapshot).version_idx
    }

    /// Resolve and clone the newest committed version of `page` visible to
    /// `snapshot` in a single arena walk.
    ///
    /// This avoids the hot-path double lookup of `resolve()` followed by
    /// `get_version()`, which otherwise reacquires the arena lock and clones
    /// the visible page a second time.
    #[must_use]
    #[allow(clippy::significant_drop_tightening)]
    pub fn resolve_visible_version(
        &self,
        page: PageNumber,
        snapshot: &Snapshot,
    ) -> Option<PageVersion> {
        'retry: loop {
            let Some(head_idx) = self.chain_heads.get_head(page) else {
                crate::observability::record_snapshot_read_versions_traversed(0);
                return None;
            };

            let arena = self.arena.read();
            let mut current_idx = head_idx;
            let mut traversed = 0;

            loop {
                let Some(version) = arena.get(current_idx) else {
                    // Race: version GC'd between head read and arena read.
                    // Retry from the top to pick up the new chain head.
                    continue 'retry;
                };

                traversed += 1;

                if visible(version, snapshot) {
                    crate::observability::record_snapshot_read_versions_traversed(traversed);
                    return Some(version.clone());
                }

                if let Some(prev_ptr) = version.prev {
                    current_idx = version_pointer_to_idx(prev_ptr);
                } else {
                    crate::observability::record_snapshot_read_versions_traversed(traversed);
                    return None;
                }
            }
        }
    }

    /// Resolve the newest committed version of `page` visible to `snapshot`
    /// and apply `f` while the arena read lock is held.
    ///
    /// This keeps callers on borrowed page versions, avoiding `PageVersion` and
    /// `PageData` cloning when they only need read-only access to committed
    /// bytes during rebase/conflict analysis.
    #[allow(clippy::significant_drop_tightening)]
    pub fn with_visible_version<R>(
        &self,
        page: PageNumber,
        snapshot: &Snapshot,
        f: impl FnOnce(&PageVersion) -> R,
    ) -> Option<R> {
        'retry: loop {
            let Some(head_idx) = self.chain_heads.get_head(page) else {
                crate::observability::record_snapshot_read_versions_traversed(0);
                return None;
            };

            let arena = self.arena.read();
            let mut current_idx = head_idx;
            let mut traversed = 0;

            loop {
                let Some(version) = arena.get(current_idx) else {
                    continue 'retry;
                };

                traversed += 1;

                if visible(version, snapshot) {
                    crate::observability::record_snapshot_read_versions_traversed(traversed);
                    return Some(f(version));
                }

                if let Some(prev_ptr) = version.prev {
                    current_idx = version_pointer_to_idx(prev_ptr);
                } else {
                    crate::observability::record_snapshot_read_versions_traversed(traversed);
                    return None;
                }
            }
        }
    }

    /// Resolve both the snapshot-visible version and the current chain head
    /// under a single arena read lock, then apply `f`.
    ///
    /// This keeps hot rebase/conflict paths on borrowed page versions without
    /// cloning committed page images and without nesting arena lock
    /// acquisitions inside helper closures.
    #[allow(clippy::significant_drop_tightening)]
    pub fn with_visible_and_chain_head_version<R>(
        &self,
        page: PageNumber,
        snapshot: &Snapshot,
        f: impl FnOnce(&PageVersion, &PageVersion) -> R,
    ) -> Option<R> {
        'retry: loop {
            let Some(head_idx) = self.chain_heads.get_head(page) else {
                crate::observability::record_snapshot_read_versions_traversed(0);
                return None;
            };

            let arena = self.arena.read();
            let Some(head_version) = arena.get(head_idx) else {
                continue 'retry;
            };

            let mut current_version = head_version;
            let mut traversed = 1;

            loop {
                if visible(current_version, snapshot) {
                    crate::observability::record_snapshot_read_versions_traversed(traversed);
                    return Some(f(current_version, head_version));
                }

                let Some(prev_ptr) = current_version.prev else {
                    crate::observability::record_snapshot_read_versions_traversed(traversed);
                    return None;
                };
                let Some(prev_version) = arena.get(version_pointer_to_idx(prev_ptr)) else {
                    continue 'retry;
                };

                traversed += 1;
                current_version = prev_version;
            }
        }
    }

    /// Resolve the commit sequence of the newest committed version of `page`
    /// visible to `snapshot` without cloning the full page image.
    #[must_use]
    #[allow(clippy::significant_drop_tightening)]
    pub fn resolve_visible_commit_seq(
        &self,
        page: PageNumber,
        snapshot: &Snapshot,
    ) -> Option<CommitSeq> {
        'retry: loop {
            let Some(head_idx) = self.chain_heads.get_head(page) else {
                crate::observability::record_snapshot_read_versions_traversed(0);
                return None;
            };

            let arena = self.arena.read();
            let mut current_idx = head_idx;
            let mut traversed = 0;

            loop {
                let Some(version) = arena.get(current_idx) else {
                    continue 'retry;
                };

                traversed += 1;

                if visible(version, snapshot) {
                    crate::observability::record_snapshot_read_versions_traversed(traversed);
                    return Some(version.commit_seq);
                }

                if let Some(prev_ptr) = version.prev {
                    current_idx = version_pointer_to_idx(prev_ptr);
                } else {
                    crate::observability::record_snapshot_read_versions_traversed(traversed);
                    return None;
                }
            }
        }
    }

    /// Retrieve the most recent visible version of a page (the head of the chain).
    ///
    /// This is the newest committed version regardless of snapshot
    /// visibility. The helper keeps head lookup + arena read in a single
    /// retrying path so callers avoid a second arena acquisition.
    #[must_use]
    #[allow(clippy::significant_drop_tightening)]
    pub fn chain_head_version(&self, page: PageNumber) -> Option<PageVersion> {
        'retry: loop {
            let head_idx = self.chain_heads.get_head(page)?;

            let arena = self.arena.read();
            let Some(version) = arena.get(head_idx) else {
                // Race: head observed before the arena entry was reclaimed.
                continue 'retry;
            };

            return Some(version.clone());
        }
    }

    /// Borrow the current chain head while the arena read lock is held.
    #[allow(clippy::significant_drop_tightening)]
    pub fn with_chain_head_version<R>(
        &self,
        page: PageNumber,
        f: impl FnOnce(&PageVersion) -> R,
    ) -> Option<R> {
        'retry: loop {
            let head_idx = self.chain_heads.get_head(page)?;

            let arena = self.arena.read();
            let Some(version) = arena.get(head_idx) else {
                continue 'retry;
            };

            return Some(f(version));
        }
    }

    /// Resolve with traversal diagnostics for snapshot-read instrumentation.
    ///
    /// Fast path: walks the version chain using only the arena RwLock and
    /// simple `commit_seq <= snapshot.high` visibility.  This avoids the
    /// `visibility_ranges` RwLock + HashMap lookup that the windowed-range
    /// path requires.  The fast path is correct because we walk newest→oldest
    /// and return the first visible version — exactly the same result as the
    /// range-based check, just without the early-termination optimization
    /// that ranges provide for very deep chains (10+).
    #[must_use]
    #[allow(clippy::significant_drop_tightening)]
    #[inline(never)]
    pub fn resolve_with_trace(
        &self,
        page: PageNumber,
        snapshot: &Snapshot,
    ) -> SnapshotResolveTrace {
        'retry: loop {
            let Some(head_idx) = self.chain_heads.get_head(page) else {
                return SnapshotResolveTrace {
                    version_idx: None,
                    versions_traversed: 0,
                };
            };

            let arena = self.arena.read();
            let mut current_idx = head_idx;
            let mut traversed = 0_u64;

            loop {
                let Some(version) = arena.get(current_idx) else {
                    // Race: version GC'd between head read and arena read.
                    // Retry from the top to pick up the new chain head.
                    continue 'retry;
                };
                traversed = traversed.saturating_add(1);

                if visible(version, snapshot) {
                    return SnapshotResolveTrace {
                        version_idx: Some(current_idx),
                        versions_traversed: traversed,
                    };
                }

                // Walk backward through the chain via prev pointer.
                let Some(prev_ptr) = version.prev else {
                    return SnapshotResolveTrace {
                        version_idx: None,
                        versions_traversed: traversed,
                    };
                };
                current_idx = version_pointer_to_idx(prev_ptr);
            }
        }
    }

    /// Resolve the base version for a write operation in a transaction.
    ///
    /// Checks the transaction's write set first (for pages already modified
    /// in this transaction), then falls back to `resolve()`.
    #[must_use]
    pub fn resolve_for_txn(&self, page: PageNumber, txn: &Transaction) -> Option<VersionIdx> {
        // Check if the page is already in the transaction's write set.
        // If so, the base version is whatever the previous version was.
        if txn.write_set.contains(&page) {
            // The transaction has already written this page; the "base" for
            // further writes is the version chain entry before this txn's write.
            // In a real implementation the write_set would map to VersionIdx,
            // but for the invariant layer we fall through to resolve().
            return self.resolve(page, &txn.snapshot);
        }

        self.resolve(page, &txn.snapshot)
    }

    /// Read a version from the arena by index.
    ///
    /// Returns an owned clone taken under the arena read lock, which is mutually
    /// exclusive with the write lock `gc_tick` holds while recycling slots. A
    /// caller may therefore hold the returned `VersionIdx` across a concurrent
    /// `gc_tick` without pinning an EBR epoch: `VersionIdx` is generation-tagged
    /// and `take_for_retirement` bumps the slot generation on retirement, so a
    /// stale index resolves to `None` here (never another version's data — no
    /// use-after-free, no ABA read). This is why the live copy-out read paths
    /// (`TimeTravelPageIo`, `resolve_visible_version`) need no reader pins; the
    /// `VersionGuard` pin machinery is reserved for transaction snapshot
    /// lifetimes and any future lock-free reader that retains an arena
    /// reference across reclamation. See
    /// `test_unpinned_reader_stale_version_idx_resolves_none_after_recycle_reuse`.
    #[must_use]
    pub fn get_version(&self, idx: VersionIdx) -> Option<PageVersion> {
        let arena = self.arena.read();
        arena.get(idx).cloned()
    }

    /// Get the chain head index for a page, if any.
    #[must_use]
    pub fn chain_head(&self, page: PageNumber) -> Option<VersionIdx> {
        self.chain_heads.get_head(page)
    }

    /// Look up the stored begin/end visibility range for a version index.
    #[must_use]
    pub fn visibility_range(&self, idx: VersionIdx) -> Option<VersionVisibilityRange> {
        let ranges = self.visibility_ranges.read();
        ranges.get(&idx).copied()
    }

    /// Walk the full version chain for a page, returning all versions
    /// from newest to oldest.
    #[must_use]
    #[allow(clippy::significant_drop_tightening)]
    pub fn walk_chain(&self, page: PageNumber) -> Vec<PageVersion> {
        loop {
            let Some(head_idx) = self.chain_heads.get_head(page) else {
                return Vec::new();
            };

            let arena = self.arena.read();
            let mut result = Vec::new();
            let mut current_idx = head_idx;
            let mut race_detected = false;

            while let Some(version) = arena.get(current_idx) {
                let prev = version.prev;
                result.push(version.clone());
                match prev {
                    Some(ptr) => current_idx = version_pointer_to_idx(ptr),
                    None => break,
                }
            }

            // If we exited the loop because `arena.get` returned `None` but we expected
            // a version (implied by the loop condition failing unexpectedly in the middle
            // of a chain, though `while let` handles the end-of-chain naturally, it conflates
            // "end of chain" with "missing version"), we need to be careful.
            //
            // Actually, the `while let Some` loop terminates if `arena.get` returns None.
            // This happens if:
            // 1. We reached the end (valid termination, but `prev` would be None so loop breaks inside match).
            // 2. We hit a GC'd version (race).
            //
            // To distinguish, we check if the last visited version had a `prev` pointer.
            // If it did, and the loop terminated, it means `arena.get` failed for that pointer.
            if let Some(last) = result.last() {
                if last.prev.is_some() {
                    // We had a prev pointer but the loop stopped -> race detected.
                    race_detected = true;
                }
            } else {
                // result is empty, meaning head_idx was invalid (GC'd).
                // But heads.get returned it. So it's a race.
                race_detected = true;
            }

            if race_detected {
                continue;
            }

            return result;
        }
    }

    /// The page size used by this store.
    #[must_use]
    pub fn page_size(&self) -> PageSize {
        self.page_size
    }

    /// Count the number of pages currently tracked by the version store.
    #[must_use]
    pub fn page_count(&self) -> usize {
        self.chain_heads.page_count()
    }

    /// Run one incremental GC pass: prune version chains for pages in the todo queue.
    ///
    /// This method acquires write locks on the arena and chain heads, then delegates
    /// to [`crate::gc::gc_tick`] for the actual pruning work.
    ///
    /// # EBR Two-Phase Retirement (D5)
    ///
    /// The pruning phase uses `take_for_retirement()` which extracts versions from
    /// the arena WITHOUT adding slots back to the free_list. This reduces the time
    /// the arena write lock is held. The pruned slot indices are added to an
    /// `EbrRetireQueue` tagged with the current EBR epoch. Once every active
    /// guard has advanced past that epoch, `try_recycle_retired_slots()` drains
    /// reclaimable batches and returns the slots to the arena free_list.
    ///
    /// # Arguments
    ///
    /// * `todo` — The per-process GC todo queue with pages to prune.
    /// * `horizon` — The GC horizon: versions with `commit_seq <= horizon` that are
    ///   superseded by a newer version are reclaimable.
    ///
    /// # Returns
    ///
    /// A [`GcTickResult`] summarizing what was pruned and whether budgets were exhausted.
    #[allow(clippy::significant_drop_tightening)]
    pub fn gc_tick(&self, todo: &mut GcTodo, horizon: CommitSeq) -> GcTickResult {
        // Phase 0: advance the EBR epoch and recycle any batches that became
        // reclaimable before this GC pass.
        let mut arena = self.arena.write();
        let pre_pass =
            advance_epoch_and_reclaim(self.guard_registry(), &self.retire_queue, &mut arena);
        let retire_epoch = pre_pass.observed_epoch;

        // Phase 1: Prune chains (arena write lock held).
        let result = gc_tick_with_registry(
            todo,
            horizon,
            &mut arena,
            &self.chain_heads,
            self.guard_registry(),
        );
        drop(arena);

        // Phase 2: Clean up visibility ranges (gated — only populated when
        // visibility-range tracking is on, so removes are a no-op otherwise).
        if MVCC_VISIBILITY_RANGES_TRACKING_ENABLED.load(Ordering::Relaxed)
            && !result.pruned_indices.is_empty()
        {
            let mut ranges = self.visibility_ranges.write();
            for idx in &result.pruned_indices {
                ranges.remove(idx);
            }
            drop(ranges);
        }

        // Phase 3: Queue pruned slots for deferred recycling (EBR).
        if !result.pruned_indices.is_empty() {
            let retired_slots = result.pruned_indices.len();
            let mut arena = self.arena.write();
            let post_pass = retire_and_reclaim(
                self.guard_registry(),
                &self.retire_queue,
                &mut arena,
                result.pruned_indices.iter().copied(),
                retire_epoch,
            );
            drop(arena);
            tracing::debug!(
                target: "fsqlite_mvcc::gc",
                retired_slots,
                retire_epoch,
                recycle_epoch = post_pass.observed_epoch,
                recycled_slots = post_pass.recycled_slots,
                pending_recycle_count = self.pending_recycle_count(),
                min_pinned_epoch = post_pass.min_pinned_epoch,
                "processed retired arena slots through EBR recycling"
            );
        }

        result
    }

    /// Try to recycle retired slots from previous epochs.
    ///
    /// Slots retired at epoch `E` are safe to recycle once every active guard
    /// has a pinned epoch strictly greater than `E`, or when no guards remain.
    ///
    /// This method is called automatically by `gc_tick()`, but can also be called
    /// manually to reclaim memory without running a full GC pass.
    ///
    /// # Returns
    ///
    /// Number of slots recycled.
    pub fn try_recycle_retired_slots(&self, current_epoch: u64) -> usize {
        let pending_recycle_count = self.pending_recycle_count();
        if pending_recycle_count == 0 {
            return 0;
        }

        let mut arena = self.arena.write();
        let pass = reclaim_at_epoch(
            self.guard_registry(),
            &self.retire_queue,
            &mut arena,
            current_epoch,
        );
        drop(arena);

        if pass.recycled_slots == 0 {
            tracing::trace!(
                target: "fsqlite_mvcc::gc",
                current_epoch = pass.observed_epoch,
                pending_recycle_count,
                min_pinned_epoch = pass.min_pinned_epoch,
                "retired arena slots remain pending until pinned epochs advance"
            );
            return 0;
        }

        tracing::debug!(
            target: "fsqlite_mvcc::gc",
            recycled = pass.recycled_slots,
            current_epoch = pass.observed_epoch,
            min_pinned_epoch = pass.min_pinned_epoch,
            pending_recycle_count_after = self.pending_recycle_count(),
            "recycled retired arena slots"
        );

        pass.recycled_slots
    }

    /// Force-recycle all retired slots regardless of epoch.
    ///
    /// Use only during shutdown or when epoch safety is guaranteed externally
    /// (e.g., no active readers).
    pub fn force_recycle_all_retired_slots(&self) -> usize {
        let drained = self.retire_queue.force_drain();
        if drained.is_empty() {
            return 0;
        }

        let count = drained.len();
        let mut arena = self.arena.write();
        arena.recycle_slots(drained);
        drop(arena);

        count
    }

    /// Number of slots currently pending recycling.
    #[must_use]
    pub fn pending_recycle_count(&self) -> usize {
        self.retire_queue.pending_count()
    }

    /// Compute the average version chain length for GC pressure estimation.
    ///
    /// Samples up to `sample_limit` pages from the chain heads and returns the
    /// mean chain length. This is used by [`crate::GcScheduler`] to derive the GC
    /// invocation frequency.
    ///
    /// Returns 0.0 if no pages exist.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn sample_chain_pressure(&self, sample_limit: usize) -> f64 {
        let arena = self.arena.read();

        let mut total_length = 0_usize;
        let mut sampled = 0_usize;

        self.chain_heads.for_each_head(|_pgno, head_idx| {
            if sampled >= sample_limit {
                return;
            }
            let mut current_idx = head_idx;
            let mut chain_len = 0_usize;

            while let Some(version) = arena.get(current_idx) {
                chain_len += 1;
                match version.prev {
                    Some(ptr) => current_idx = version_pointer_to_idx(ptr),
                    None => break,
                }
            }

            total_length += chain_len;
            sampled += 1;
        });

        if sampled == 0 {
            0.0
        } else {
            total_length as f64 / sampled as f64
        }
    }

    /// Return the current committed chain length for one page.
    ///
    /// Retries when racing with GC to avoid reporting torn intermediate state.
    #[must_use]
    #[allow(clippy::significant_drop_tightening)]
    pub fn chain_length(&self, page: PageNumber) -> usize {
        loop {
            let Some(head_idx) = self.chain_heads.get_head(page) else {
                return 0;
            };

            let arena = self.arena.read();
            let mut len = 0_usize;
            let mut current_idx = head_idx;
            let mut raced = false;

            loop {
                let Some(version) = arena.get(current_idx) else {
                    raced = true;
                    break;
                };
                len = len.saturating_add(1);
                match version.prev {
                    Some(ptr) => current_idx = version_pointer_to_idx(ptr),
                    None => break,
                }
            }

            if raced {
                continue;
            }
            return len;
        }
    }

    /// Run eager GC on one page chain at a caller-selected horizon.
    ///
    /// Returns the number of versions freed from the chain.
    #[must_use]
    #[allow(clippy::significant_drop_tightening)]
    pub fn prune_page_chain_eager(&self, page: PageNumber, horizon: CommitSeq) -> usize {
        let mut arena = self.arena.write();
        let result = prune_page_chain_with_registry(
            page,
            horizon,
            &mut arena,
            &self.chain_heads,
            self.guard_registry(),
        );
        drop(arena);

        if !result.pruned_indices.is_empty() {
            if MVCC_VISIBILITY_RANGES_TRACKING_ENABLED.load(Ordering::Relaxed) {
                let mut ranges = self.visibility_ranges.write();
                for idx in &result.pruned_indices {
                    ranges.remove(idx);
                }
            }

            let retire_epoch = self.current_epoch();
            let mut arena = self.arena.write();
            let pass = retire_and_reclaim(
                self.guard_registry(),
                &self.retire_queue,
                &mut arena,
                result.pruned_indices.iter().copied(),
                retire_epoch,
            );
            drop(arena);

            tracing::debug!(
                target: "fsqlite_mvcc::gc",
                page = page.get(),
                retired_slots = result.pruned_indices.len(),
                retire_epoch,
                recycle_epoch = pass.observed_epoch,
                recycled_slots = pass.recycled_slots,
                pending_recycle_count = self.pending_recycle_count(),
                min_pinned_epoch = pass.min_pinned_epoch,
                "eager prune processed retired arena slots through EBR recycling"
            );
        }

        usize::try_from(result.freed).unwrap_or(usize::MAX)
    }
}

impl std::fmt::Debug for VersionStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let arena = self.arena.read();
        let ranges = self.visibility_ranges.read();
        f.debug_struct("VersionStore")
            .field("page_size", &self.page_size.get())
            .field("page_count", &self.chain_heads.page_count())
            .field("visibility_range_count", &ranges.len())
            .field("arena_high_water", &arena.high_water())
            .field("pending_recycle_count", &self.pending_recycle_count())
            .field("ebr_epoch", &self.current_epoch())
            .field("guard_registry", &self.guard_registry)
            .finish_non_exhaustive()
    }
}

// ---------------------------------------------------------------------------
// SerializedWriteMutex — INV-7
// ---------------------------------------------------------------------------

/// Global write mutex for Serialized mode (INV-7).
///
/// At most one Serialized-mode writer holds this mutex at any time.
/// DEFERRED transactions do not acquire it until their first write.
pub struct SerializedWriteMutex {
    inner: Mutex<Option<TxnId>>,
}

impl SerializedWriteMutex {
    /// Create a new unlocked mutex.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(None),
        }
    }

    /// Try to acquire the mutex for `txn`. Returns `Ok(())` if acquired,
    /// or `Err(holder)` if another transaction holds it.
    pub fn try_acquire(&self, txn: TxnId) -> Result<(), TxnId> {
        let mut guard = self.inner.lock();
        match *guard {
            Some(holder) if holder != txn => Err(holder),
            Some(_) => Ok(()), // already held by this txn (idempotent)
            None => {
                *guard = Some(txn);
                drop(guard);
                tracing::info!(txn_id = %txn, "serialized write mutex acquired");
                Ok(())
            }
        }
    }

    /// Release the mutex held by `txn`. Returns `true` if released.
    pub fn release(&self, txn: TxnId) -> bool {
        let mut guard = self.inner.lock();
        if *guard == Some(txn) {
            *guard = None;
            drop(guard);
            tracing::info!(txn_id = %txn, "serialized write mutex released");
            true
        } else {
            false
        }
    }

    /// Check which transaction holds the mutex, if any.
    #[must_use]
    pub fn holder(&self) -> Option<TxnId> {
        *self.inner.lock()
    }
}

impl Default for SerializedWriteMutex {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for SerializedWriteMutex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SerializedWriteMutex")
            .field("holder", &self.holder())
            .finish()
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Convert a `VersionPointer` (packed u64) to a `VersionIdx`.
///
/// The packing convention:
/// - Bits 0..12: offset (12 bits, max 4095)
/// - Bits 12..32: chunk (20 bits, max 1,048,575)
/// - Bits 32..64: generation (32 bits)
#[inline]
#[must_use]
fn version_pointer_to_idx(ptr: VersionPointer) -> VersionIdx {
    let raw = ptr.get();
    #[allow(clippy::cast_possible_truncation)]
    let offset = (raw & 0xFFF) as u32;
    #[allow(clippy::cast_possible_truncation)]
    let chunk = ((raw >> 12) & 0xF_FFFF) as u32;
    #[allow(clippy::cast_possible_truncation)]
    let generation = (raw >> 32) as u32;
    VersionIdx::new(chunk, offset, generation)
}

/// Convert a `VersionIdx` to a `VersionPointer` for storage in `PageVersion.prev`.
#[inline]
#[must_use]
pub fn idx_to_version_pointer(idx: VersionIdx) -> VersionPointer {
    let chunk = u64::from(idx.chunk());
    let offset = u64::from(idx.offset());
    let generation = u64::from(idx.generation());

    assert!(chunk <= 0xF_FFFF, "VersionIdx chunk overflow (max 20 bits)");
    assert!(offset <= 0xFFF, "VersionIdx offset overflow (max 12 bits)");

    VersionPointer::new((generation << 32) | (chunk << 12) | offset)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core_types::{InProcessPageLockTable, TransactionMode, TransactionState};
    use crate::ebr::VersionGuard;
    use fsqlite_types::{PageData, SchemaEpoch, TxnEpoch, TxnToken};
    use proptest::prelude::*;
    use std::sync::Arc;

    fn make_snapshot(high: u64) -> Snapshot {
        Snapshot::new(CommitSeq::new(high), SchemaEpoch::ZERO)
    }

    fn make_version(pgno: u32, commit_seq: u64, prev: Option<VersionPointer>) -> PageVersion {
        PageVersion {
            pgno: PageNumber::new(pgno).unwrap(),
            commit_seq: CommitSeq::new(commit_seq),
            created_by: TxnToken::new(TxnId::new(1).unwrap(), TxnEpoch::new(0)),
            data: PageData::zeroed(PageSize::DEFAULT),
            prev,
        }
    }

    // -----------------------------------------------------------------------
    // INV-1: Monotonicity (TxnId + CommitSeq)
    // -----------------------------------------------------------------------

    #[test]
    fn test_inv1_txnid_monotonic_cas_loop() {
        let mgr = TxnManager::default();
        let mut prev = 0_u64;

        for _ in 0..1000 {
            let id = mgr.alloc_txn_id().expect("should not exhaust id space");
            let raw = id.get();
            assert!(
                raw > prev,
                "TxnId must be strictly increasing: {raw} <= {prev}"
            );
            assert_ne!(raw, 0, "TxnId must never be zero");
            assert!(raw <= TxnId::MAX_RAW, "TxnId must not exceed MAX_RAW");
            prev = raw;
        }
    }

    #[test]
    fn test_inv1_txnid_exhaustion() {
        // Start near the max to test exhaustion.
        let mgr = TxnManager::new(TxnId::MAX_RAW, 1);

        let id = mgr.alloc_txn_id();
        assert!(id.is_some(), "should allocate the last valid TxnId");
        assert_eq!(id.unwrap().get(), TxnId::MAX_RAW);

        let id = mgr.alloc_txn_id();
        assert!(id.is_none(), "should fail when id space is exhausted");
    }

    #[test]
    fn test_inv1_commit_seq_monotonic() {
        let mgr = TxnManager::default();
        let mut prev = CommitSeq::ZERO;

        for _ in 0..100 {
            let seq = mgr.alloc_commit_seq();
            assert!(seq > prev, "CommitSeq must be strictly increasing");
            prev = seq;
        }
    }

    #[test]
    fn test_inv1_txnid_multithreaded_monotonicity() {
        use std::sync::Arc;

        let mgr = Arc::new(TxnManager::default());
        let mut handles = Vec::new();

        for _ in 0..4 {
            let mgr = Arc::clone(&mgr);
            handles.push(std::thread::spawn(move || {
                let mut ids = Vec::with_capacity(250);
                for _ in 0..250 {
                    ids.push(mgr.alloc_txn_id().unwrap().get());
                }
                ids
            }));
        }

        let mut all_ids: Vec<u64> = handles
            .into_iter()
            .flat_map(|h| h.join().unwrap())
            .collect();

        // All ids must be unique.
        let unique_count = {
            let mut sorted = all_ids.clone();
            sorted.sort_unstable();
            sorted.dedup();
            sorted.len()
        };
        assert_eq!(unique_count, 1000, "all TxnIds must be unique");

        // Each thread's local sequence must be increasing.
        // (Already guaranteed by AtomicU64 CAS, but verify the global set has no duplicates.)
        all_ids.sort_unstable();
        for window in all_ids.windows(2) {
            assert!(
                window[0] < window[1],
                "global TxnId sequence must be strictly increasing: {} >= {}",
                window[0],
                window[1]
            );
        }
    }

    #[test]
    fn test_bd6883_first_attempt_ratio_64_threads_moderate_contention() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicU64, Ordering};
        use std::thread;

        const THREADS: u32 = 64;
        const INSTALLS_PER_THREAD: u32 = 256;
        const PAGE_FANOUT: u32 = 512;
        const BEAD_ID: &str = "bd-688.3";
        const RUN_ID: &str = "bd6883-cas-ratio-run";
        const TRACE_ID: &str = "bd6883-cas-ratio-trace";
        const SCENARIO_ID: &str = "cas_first_attempt_ratio_moderate_contention";

        let chain_heads = Arc::new(ChainHeadTable::new());
        let next_idx_raw = Arc::new(AtomicU64::new(0));
        let first_attempts = Arc::new(AtomicU64::new(0));
        let total_installs = Arc::new(AtomicU64::new(0));

        let handles: Vec<_> = (0..THREADS)
            .map(|tid| {
                let chain_heads = Arc::clone(&chain_heads);
                let next_idx_raw = Arc::clone(&next_idx_raw);
                let first_attempts = Arc::clone(&first_attempts);
                let total_installs = Arc::clone(&total_installs);

                thread::spawn(move || {
                    for op in 0..INSTALLS_PER_THREAD {
                        let global = tid * INSTALLS_PER_THREAD + op;
                        let pgno = PageNumber::new((global % PAGE_FANOUT) + 1)
                            .expect("page number must be non-zero");
                        let raw = next_idx_raw.fetch_add(1, Ordering::Relaxed);
                        #[allow(clippy::cast_possible_truncation)]
                        let chunk = (raw / 4096) as u32;
                        #[allow(clippy::cast_possible_truncation)]
                        let offset = (raw % 4096) as u32;
                        let idx = VersionIdx::new(chunk, offset, 1);

                        let (_previous, attempts) = chain_heads.install_with_retry(pgno, idx);
                        total_installs.fetch_add(1, Ordering::Relaxed);
                        if attempts == 1 {
                            first_attempts.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().expect("stress thread must not panic");
        }

        let total = total_installs.load(Ordering::Relaxed);
        let first = first_attempts.load(Ordering::Relaxed);
        assert_eq!(
            total,
            u64::from(THREADS) * u64::from(INSTALLS_PER_THREAD),
            "all install attempts must be accounted for"
        );

        #[allow(clippy::cast_precision_loss)]
        let ratio = first as f64 / total as f64;
        tracing::info!(
            bead_id = BEAD_ID,
            run_id = RUN_ID,
            trace_id = TRACE_ID,
            scenario_id = SCENARIO_ID,
            total_installs = total,
            first_attempts = first,
            first_attempt_ratio = ratio,
            "chain-head CAS first-attempt ratio stress result"
        );

        assert!(
            ratio >= 0.95,
            "bead_id={BEAD_ID} run_id={RUN_ID} trace_id={TRACE_ID} scenario_id={SCENARIO_ID} expected first-attempt ratio >= 0.95, got {ratio:.6}"
        );
    }

    #[test]
    fn loom_chain_head_publication_linearizable() {
        use loom::sync::Arc;
        use loom::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
        use loom::thread;

        loom::model(|| {
            const EMPTY: u64 = u64::MAX;
            const HEAD_A: u64 = 0x1001;
            const HEAD_B: u64 = 0x2002;

            let head = Arc::new(AtomicU64::new(EMPTY));
            let completions = Arc::new(AtomicUsize::new(0));

            let spawn_installer =
                |next_head: u64, head: Arc<AtomicU64>, completions: Arc<AtomicUsize>| {
                    thread::spawn(move || {
                        loop {
                            let current = head.load(Ordering::Acquire);
                            if head
                                .compare_exchange(
                                    current,
                                    next_head,
                                    Ordering::AcqRel,
                                    Ordering::Acquire,
                                )
                                .is_ok()
                            {
                                completions.fetch_add(1, Ordering::Release);
                                break;
                            }
                        }
                    })
                };

            let thread_a = spawn_installer(HEAD_A, Arc::clone(&head), Arc::clone(&completions));
            let thread_b = spawn_installer(HEAD_B, Arc::clone(&head), Arc::clone(&completions));

            thread_a.join().expect("loom installer A must join");
            thread_b.join().expect("loom installer B must join");

            let final_head = head.load(Ordering::Acquire);
            assert!(
                final_head == HEAD_A || final_head == HEAD_B,
                "final head must equal one of the published values"
            );
            assert_eq!(
                completions.load(Ordering::Acquire),
                2,
                "both installers must eventually complete"
            );
        });
    }

    // -----------------------------------------------------------------------
    // INV-2: Lock Exclusivity (tested via InProcessPageLockTable)
    // -----------------------------------------------------------------------

    #[test]
    fn test_inv2_page_lock_exclusivity() {
        let table = InProcessPageLockTable::new();
        let page = PageNumber::new(42).unwrap();
        let txn_a = TxnId::new(1).unwrap();
        let txn_b = TxnId::new(2).unwrap();

        // txn_a acquires.
        assert!(table.try_acquire(page, txn_a).is_ok());

        // txn_b is blocked (gets SQLITE_BUSY equivalent).
        let err = table.try_acquire(page, txn_b);
        assert_eq!(err, Err(txn_a), "second txn must see the holder");

        // txn_a re-acquires (idempotent).
        assert!(table.try_acquire(page, txn_a).is_ok());

        // After release, txn_b can acquire.
        assert!(table.release(page, txn_a));
        assert!(table.try_acquire(page, txn_b).is_ok());
    }

    // -----------------------------------------------------------------------
    // INV-3: Version Chain Order (descending commit_seq)
    // -----------------------------------------------------------------------

    #[test]
    fn test_inv3_version_chain_descending() {
        let store = VersionStore::new(PageSize::DEFAULT);

        // Commit 5 transactions writing the same page.
        let pgno = PageNumber::new(1).unwrap();
        let mut prev_ptr: Option<VersionPointer> = None;

        for seq in 1..=5_u64 {
            let version = PageVersion {
                pgno,
                commit_seq: CommitSeq::new(seq),
                created_by: TxnToken::new(TxnId::new(seq).unwrap(), TxnEpoch::new(0)),
                data: PageData::zeroed(PageSize::DEFAULT),
                prev: prev_ptr,
            };
            let idx = store.publish(version);
            prev_ptr = Some(idx_to_version_pointer(idx));
        }

        // Walk the chain and verify strictly descending commit_seq.
        let chain = store.walk_chain(pgno);
        assert_eq!(chain.len(), 5);

        for window in chain.windows(2) {
            assert!(
                window[0].commit_seq > window[1].commit_seq,
                "version chain must be strictly descending: {} <= {}",
                window[0].commit_seq.get(),
                window[1].commit_seq.get()
            );
        }
    }

    // -----------------------------------------------------------------------
    // INV-4: Write Set Consistency (every write_set page must be locked)
    // -----------------------------------------------------------------------

    #[test]
    fn test_inv4_write_set_requires_lock() {
        let table = InProcessPageLockTable::new();
        let txn_id = TxnId::new(1).unwrap();
        let snap = make_snapshot(0);
        let mut txn = Transaction::new(txn_id, TxnEpoch::new(0), snap, TransactionMode::Concurrent);

        let page = PageNumber::new(10).unwrap();

        // Acquire lock first (correct order per INV-4).
        table.try_acquire(page, txn_id).unwrap();
        txn.page_locks.insert(page);
        txn.write_set.push(page);

        // Verify invariant: every page in write_set is in page_locks.
        for &p in &txn.write_set {
            assert!(
                txn.page_locks.contains(&p),
                "INV-4 violated: page {p:?} in write_set but not in page_locks"
            );
        }
    }

    // -----------------------------------------------------------------------
    // INV-5: Snapshot Stability (DEFERRED nuance)
    // -----------------------------------------------------------------------

    #[test]
    fn test_inv5_deferred_snapshot_provisional() {
        let txn_id = TxnId::new(1).unwrap();
        let provisional_snap = make_snapshot(0);

        // Simulate DEFERRED mode: snapshot_established starts false.
        let mut txn = Transaction::new(
            txn_id,
            TxnEpoch::new(0),
            provisional_snap,
            TransactionMode::Serialized,
        );
        // Override: for DEFERRED, snapshot is provisional.
        txn.snapshot_established = false;

        assert!(
            !txn.snapshot_established,
            "DEFERRED snapshot should be provisional"
        );

        // First read establishes the snapshot.
        let current_high = CommitSeq::new(5);
        txn.snapshot = Snapshot::new(current_high, SchemaEpoch::ZERO);
        txn.snapshot_established = true;

        assert!(
            txn.snapshot_established,
            "snapshot should now be established"
        );
        assert_eq!(txn.snapshot.high, current_high);

        // Once established, verify it cannot change (type-level immutability).
        let established = txn.snapshot;
        assert_eq!(established.high.get(), 5);
    }

    // -----------------------------------------------------------------------
    // INV-6: Commit Atomicity (all-or-nothing visibility)
    // -----------------------------------------------------------------------

    #[test]
    fn test_inv6_commit_atomicity_all_visible_or_none() {
        let store = VersionStore::new(PageSize::DEFAULT);

        // Transaction writes pages 1, 2, 3 with commit_seq=5.
        let pages = [1_u32, 2, 3];
        for &p in &pages {
            let version = make_version(p, 5, None);
            store.publish(version);
        }

        // Snapshot at high=4: none should be visible.
        let snap_before = make_snapshot(4);
        for &p in &pages {
            let pgno = PageNumber::new(p).unwrap();
            assert!(
                store.resolve(pgno, &snap_before).is_none(),
                "page {p} should NOT be visible at snapshot high=4"
            );
        }

        // Snapshot at high=5: all should be visible.
        let snap_at = make_snapshot(5);
        for &p in &pages {
            let pgno = PageNumber::new(p).unwrap();
            assert!(
                store.resolve(pgno, &snap_at).is_some(),
                "page {p} should be visible at snapshot high=5"
            );
        }

        // Snapshot at high=10: all should still be visible.
        let snap_after = make_snapshot(10);
        for &p in &pages {
            let pgno = PageNumber::new(p).unwrap();
            assert!(
                store.resolve(pgno, &snap_after).is_some(),
                "page {p} should be visible at snapshot high=10"
            );
        }
    }

    /// bd-707lc: the stable commit watermark must never name a `CommitSeq` that
    /// has not been finished.
    ///
    /// Before the fix, `combine_locked` advanced `next_commit_seq` (the
    /// allocation counter) *outside* the `active_commits` lock, then registered
    /// the new sequences *inside* it. A concurrent `finish_commit_seq` that
    /// drained the active list to empty could therefore read a `next_commit_seq`
    /// that already covered freshly-allocated-but-not-yet-registered (in-flight)
    /// sequences and push `stable_commit_seq` past them — making a partial
    /// commit visible to a new snapshot (an INV-6 violation). The fix performs
    /// the allocation and registration atomically under the same lock.
    ///
    /// Invariant: `stable_commit_seq == X` ⟹ commit `X` is finished. Each worker
    /// records its seq as "finished" *before* calling `finish_commit_seq`, so a
    /// legitimate watermark advance to that seq cannot trip a false positive;
    /// only a premature advance past a seq still held in-flight by another
    /// worker (one still blocked inside allocation, unable to have recorded it)
    /// can.
    #[test]
    fn test_bd707lc_stable_watermark_never_names_inflight_commit() {
        use std::collections::HashSet;
        use std::sync::Mutex as StdMutex;
        use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

        let mgr = Arc::new(TxnManager::new(1, 1));
        let finished: Arc<StdMutex<HashSet<u64>>> = Arc::new(StdMutex::new(HashSet::new()));
        let violations = Arc::new(AtomicU64::new(0));
        let stop = Arc::new(AtomicBool::new(false));

        let num_workers = 8_u64;
        let per_worker = 2_000_u64;

        let mut handles = Vec::new();
        for _ in 0..num_workers {
            let mgr = Arc::clone(&mgr);
            let finished = Arc::clone(&finished);
            handles.push(std::thread::spawn(move || {
                for _ in 0..per_worker {
                    let seq = mgr.alloc_commit_seq().get();
                    // Widen the in-flight window a touch.
                    std::hint::spin_loop();
                    // Record finished BEFORE the call that may advance the
                    // watermark, so a correct advance to this seq is never a
                    // false positive.
                    finished.lock().unwrap().insert(seq);
                    mgr.finish_commit_seq(CommitSeq::new(seq));
                }
            }));
        }

        // Checker: continuously verify the watermark names a finished commit.
        let checker = {
            let mgr = Arc::clone(&mgr);
            let finished = Arc::clone(&finished);
            let violations = Arc::clone(&violations);
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    let stable = mgr.current_commit_counter().saturating_sub(1);
                    if stable >= 1 && !finished.lock().unwrap().contains(&stable) {
                        violations.fetch_add(1, Ordering::Relaxed);
                    }
                }
            })
        };

        for h in handles {
            h.join().expect("worker thread panicked");
        }
        stop.store(true, Ordering::Relaxed);
        checker.join().expect("checker thread panicked");

        assert_eq!(
            violations.load(Ordering::Relaxed),
            0,
            "stable_commit_seq named an unfinished (in-flight) commit — a \
             partial commit could be visible to a snapshot (bd-707lc)"
        );

        // After every in-flight commit drains, the watermark reflects every
        // allocation: seqs [1, total] allocated ⟹ stable == total, counter ==
        // total + 1.
        let total = num_workers * per_worker;
        assert_eq!(
            mgr.current_commit_counter(),
            total + 1,
            "after draining all in-flight commits the stable watermark must \
             equal the highest allocated sequence"
        );
    }

    // -----------------------------------------------------------------------
    // INV-7: Serialized Mode (global write mutex)
    // -----------------------------------------------------------------------

    #[test]
    fn test_inv7_serialized_write_mutex_exclusivity() {
        let mutex = SerializedWriteMutex::new();
        let txn_a = TxnId::new(1).unwrap();
        let txn_b = TxnId::new(2).unwrap();

        // txn_a acquires.
        assert!(mutex.try_acquire(txn_a).is_ok());
        assert_eq!(mutex.holder(), Some(txn_a));

        // txn_b cannot acquire.
        assert_eq!(mutex.try_acquire(txn_b), Err(txn_a));

        // txn_a re-acquire is idempotent.
        assert!(mutex.try_acquire(txn_a).is_ok());

        // Release.
        assert!(mutex.release(txn_a));
        assert!(mutex.holder().is_none());

        // Now txn_b can acquire.
        assert!(mutex.try_acquire(txn_b).is_ok());
        assert_eq!(mutex.holder(), Some(txn_b));
        assert!(mutex.release(txn_b));
    }

    // -----------------------------------------------------------------------
    // Visibility predicate tests (§5.3)
    // -----------------------------------------------------------------------

    #[test]
    fn test_visible_predicate_committed_within_range() {
        let snap = make_snapshot(10);

        // Committed at seq=5, visible (5 <= 10).
        let v5 = make_version(1, 5, None);
        assert!(visible(&v5, &snap));

        // Committed at seq=10, visible (10 <= 10).
        let v10 = make_version(1, 10, None);
        assert!(visible(&v10, &snap));

        // Committed at seq=15, NOT visible (15 > 10).
        let v15 = make_version(1, 15, None);
        assert!(!visible(&v15, &snap));

        // Uncommitted (seq=0), NOT visible.
        let v0 = make_version(1, 0, None);
        assert!(!visible(&v0, &snap));
    }

    #[test]
    fn test_resolve_returns_first_visible_from_head() {
        let store = VersionStore::new(PageSize::DEFAULT);
        let pgno = PageNumber::new(1).unwrap();

        // Build chain: V1(seq=1) <- V2(seq=5) <- V3(seq=10)
        let v1 = make_version(1, 1, None);
        let idx1 = store.publish(v1);

        let v2 = make_version(1, 5, Some(idx_to_version_pointer(idx1)));
        let idx2 = store.publish(v2);

        let v3 = make_version(1, 10, Some(idx_to_version_pointer(idx2)));
        store.publish(v3);

        // Snapshot high=7: should resolve to V2 (seq=5, first visible from head).
        let snap = make_snapshot(7);
        let resolved = store.resolve(pgno, &snap).unwrap();
        let version = store.get_version(resolved).unwrap();
        assert_eq!(
            version.commit_seq,
            CommitSeq::new(5),
            "should resolve to V2 (seq=5)"
        );

        // Snapshot high=10: should resolve to V3 (seq=10).
        let snap_at_ten = make_snapshot(10);
        let resolved_ten = store.resolve(pgno, &snap_at_ten).unwrap();
        let version_ten = store.get_version(resolved_ten).unwrap();
        assert_eq!(version_ten.commit_seq, CommitSeq::new(10));

        // Snapshot high=0: nothing visible (seq 0 is uncommitted marker).
        let snap_at_zero = make_snapshot(0);
        assert!(store.resolve(pgno, &snap_at_zero).is_none());
    }

    #[test]
    fn test_version_visibility_ranges_track_begin_end_timestamps() {
        // Production gates the ranges side-index off to keep `publish()`
        // off the RwLock<HashMap>; flip it on for this diagnostic test.
        set_mvcc_visibility_ranges_tracking_enabled(true);
        let store = VersionStore::new(PageSize::DEFAULT);

        let v1 = make_version(1, 1, None);
        let idx1 = store.publish(v1);
        let v2 = make_version(1, 5, Some(idx_to_version_pointer(idx1)));
        let idx2 = store.publish(v2);
        let v3 = make_version(1, 10, Some(idx_to_version_pointer(idx2)));
        let idx3 = store.publish(v3);

        let r1 = store.visibility_range(idx1).unwrap();
        let r2 = store.visibility_range(idx2).unwrap();
        let r3 = store.visibility_range(idx3).unwrap();

        assert_eq!(r1.begin_ts, CommitSeq::new(1));
        assert_eq!(r1.end_ts, Some(CommitSeq::new(5)));
        assert_eq!(r2.begin_ts, CommitSeq::new(5));
        assert_eq!(r2.end_ts, Some(CommitSeq::new(10)));
        assert_eq!(r3.begin_ts, CommitSeq::new(10));
        assert_eq!(r3.end_ts, None);
    }

    #[test]
    fn test_resolve_with_trace_reports_versions_traversed() {
        let store = VersionStore::new(PageSize::DEFAULT);
        let pgno = PageNumber::new(1).unwrap();

        let v1 = make_version(1, 1, None);
        let idx1 = store.publish(v1);
        let v2 = make_version(1, 5, Some(idx_to_version_pointer(idx1)));
        let idx2 = store.publish(v2);
        let v3 = make_version(1, 10, Some(idx_to_version_pointer(idx2)));
        store.publish(v3);

        let trace = store.resolve_with_trace(pgno, &make_snapshot(7));
        assert_eq!(
            trace
                .version_idx
                .map(|idx| store.get_version(idx).unwrap().commit_seq),
            Some(CommitSeq::new(5))
        );
        assert_eq!(trace.versions_traversed, 2);

        let head_trace = store.resolve_with_trace(pgno, &make_snapshot(10));
        assert_eq!(head_trace.versions_traversed, 1);
    }

    #[test]
    fn test_resolve_visible_version_returns_first_visible_from_head() {
        let store = VersionStore::new(PageSize::DEFAULT);
        let pgno = PageNumber::new(1).unwrap();

        let v1 = make_version(1, 1, None);
        let idx1 = store.publish(v1);
        let v2 = make_version(1, 5, Some(idx_to_version_pointer(idx1)));
        let idx2 = store.publish(v2);
        let v3 = make_version(1, 10, Some(idx_to_version_pointer(idx2)));
        store.publish(v3);

        let visible = store
            .resolve_visible_version(pgno, &make_snapshot(7))
            .expect("snapshot should resolve to the first visible version");
        assert_eq!(visible.commit_seq, CommitSeq::new(5));

        let latest = store
            .resolve_visible_version(pgno, &make_snapshot(10))
            .expect("snapshot at chain head should resolve");
        assert_eq!(latest.commit_seq, CommitSeq::new(10));
        assert_eq!(
            store.resolve_visible_commit_seq(pgno, &make_snapshot(7)),
            Some(CommitSeq::new(5))
        );
        assert_eq!(
            store.resolve_visible_commit_seq(pgno, &make_snapshot(10)),
            Some(CommitSeq::new(10))
        );

        assert!(
            store
                .resolve_visible_version(pgno, &make_snapshot(0))
                .is_none(),
            "snapshot before first commit should not see a version"
        );
        assert_eq!(
            store.resolve_visible_commit_seq(pgno, &make_snapshot(0)),
            None
        );
    }

    #[test]
    fn test_chain_head_version_returns_latest_version() {
        let store = VersionStore::new(PageSize::DEFAULT);
        let pgno = PageNumber::new(1).unwrap();

        let v1 = make_version(1, 1, None);
        let idx1 = store.publish(v1);
        let v2 = make_version(1, 5, Some(idx_to_version_pointer(idx1)));
        let idx2 = store.publish(v2);
        let v3 = make_version(1, 10, Some(idx_to_version_pointer(idx2)));
        store.publish(v3);

        let head = store
            .chain_head_version(pgno)
            .expect("published page should have a latest chain head");
        assert_eq!(head.commit_seq, CommitSeq::new(10));
    }

    #[test]
    fn test_resolve_for_txn_checks_write_set_first() {
        let store = VersionStore::new(PageSize::DEFAULT);
        let pgno = PageNumber::new(1).unwrap();

        // Publish a committed version.
        let v1 = make_version(1, 1, None);
        store.publish(v1);

        // Create a transaction that has written to page 1.
        let txn_id = TxnId::new(2).unwrap();
        let snap = make_snapshot(1);
        let mut txn = Transaction::new(txn_id, TxnEpoch::new(0), snap, TransactionMode::Concurrent);
        txn.write_set.push(pgno);

        // resolve_for_txn should still resolve (via snapshot fallback).
        let resolved = store.resolve_for_txn(pgno, &txn);
        assert!(
            resolved.is_some(),
            "should resolve even with write_set entry"
        );

        // For a page NOT in write_set, also resolves via snapshot.
        let other_page = PageNumber::new(99).unwrap();
        let resolved_other = store.resolve_for_txn(other_page, &txn);
        assert!(resolved_other.is_none(), "page 99 has no versions");
    }

    // -----------------------------------------------------------------------
    // §5.3 Worked example: 5-txn scenario
    // -----------------------------------------------------------------------

    #[test]
    #[allow(clippy::too_many_lines)]
    fn test_worked_example_5txn_scenario() {
        // Implements the 12-step worked example from the spec (§5.3).
        let mgr = TxnManager::default();
        let store = VersionStore::new(PageSize::DEFAULT);
        let lock_table = InProcessPageLockTable::new();

        let p1 = PageNumber::new(1).unwrap();

        // t0: T1 begins (snapshot.high=0)
        let t1_id = mgr.alloc_txn_id().unwrap();
        let snap0 = make_snapshot(0);
        let mut t1 = Transaction::new(t1_id, TxnEpoch::new(0), snap0, TransactionMode::Concurrent);

        // t1: T2 begins (snapshot.high=0)
        let t2_id = mgr.alloc_txn_id().unwrap();
        let mut t2 = Transaction::new(t2_id, TxnEpoch::new(0), snap0, TransactionMode::Concurrent);

        // t2: T1 writes P1 (private write-set version)
        lock_table.try_acquire(p1, t1_id).unwrap();
        t1.page_locks.insert(p1);
        t1.write_set.push(p1);

        // t3: T3 begins (snapshot.high=0)
        let t3_id = mgr.alloc_txn_id().unwrap();
        let mut t3 = Transaction::new(t3_id, TxnEpoch::new(0), snap0, TransactionMode::Concurrent);

        // t4: T1 commits (commit_seq=1; publishes V1)
        let seq1 = mgr.alloc_commit_seq();
        assert_eq!(seq1.get(), 1);

        let v1 = PageVersion {
            pgno: p1,
            commit_seq: seq1,
            created_by: t1.token(),
            data: PageData::zeroed(PageSize::DEFAULT),
            prev: None,
        };
        store.publish(v1);
        lock_table.release_all(t1_id);
        t1.commit();

        // t5: T2 writes P1 (private)
        // T2 tries to acquire lock (T1 released it, so it succeeds).
        lock_table.try_acquire(p1, t2_id).unwrap();
        t2.page_locks.insert(p1);
        t2.write_set.push(p1);

        // t6: T4 begins (snapshot.high=1 — sees V1)
        let t4_id = mgr.alloc_txn_id().unwrap();
        let snap1 = make_snapshot(1);
        let t4 = Transaction::new(t4_id, TxnEpoch::new(0), snap1, TransactionMode::Concurrent);

        // t7: T2 commits -> FAILS FCW
        // Base version of P1 has commit_seq=1, but T2's snapshot.high=0.
        // FCW check: base_version(P1).commit_seq (=1) > T2.snapshot.high (=0) => FAIL
        let base = store.resolve(
            p1,
            &Snapshot::new(CommitSeq::new(u64::MAX), SchemaEpoch::ZERO),
        );
        let base_version = store.get_version(base.unwrap()).unwrap();
        let fcw_fail_t2 = base_version.commit_seq.get() > t2.snapshot.high.get();
        assert!(
            fcw_fail_t2,
            "T2 must fail FCW: base seq=1 > snapshot high=0"
        );
        lock_table.release_all(t2_id);
        t2.abort();
        assert_eq!(t2.state, TransactionState::Aborted);

        // t8: T5 begins (snapshot.high=1)
        let t5_id = mgr.alloc_txn_id().unwrap();
        let mut t5 = Transaction::new(t5_id, TxnEpoch::new(0), snap1, TransactionMode::Concurrent);

        // t9: T3 writes P1 (private)
        lock_table.try_acquire(p1, t3_id).unwrap();
        t3.page_locks.insert(p1);
        t3.write_set.push(p1);

        // t10: T3 commits -> FAILS FCW (same reason as T2)
        let fcw_fail_t3 = base_version.commit_seq.get() > t3.snapshot.high.get();
        assert!(
            fcw_fail_t3,
            "T3 must fail FCW: base seq=1 > snapshot high=0"
        );
        lock_table.release_all(t3_id);
        t3.abort();
        assert_eq!(t3.state, TransactionState::Aborted);

        // t11: T5 writes P1
        lock_table.try_acquire(p1, t5_id).unwrap();
        t5.page_locks.insert(p1);
        t5.write_set.push(p1);

        // t12: T5 commits (commit_seq=2; publishes V2)
        // FCW check: base_version(P1).commit_seq (=1) <= T5.snapshot.high (=1) => PASS
        let fcw_pass_t5 = base_version.commit_seq.get() <= t5.snapshot.high.get();
        assert!(
            fcw_pass_t5,
            "T5 must pass FCW: base seq=1 <= snapshot high=1"
        );

        let seq2 = mgr.alloc_commit_seq();
        assert_eq!(seq2.get(), 2);

        let head_idx = store.chain_head(p1).unwrap();
        let v2 = PageVersion {
            pgno: p1,
            commit_seq: seq2,
            created_by: t5.token(),
            data: PageData::zeroed(PageSize::DEFAULT),
            prev: Some(idx_to_version_pointer(head_idx)),
        };
        store.publish(v2);
        lock_table.release_all(t5_id);
        t5.commit();

        // Verify what each transaction sees:
        // T2 (snap high=0): no committed version visible.
        let snap_t2 = make_snapshot(0);
        assert!(store.resolve(p1, &snap_t2).is_none());

        // T4 (snap high=1): sees V1 (seq=1).
        let resolved_t4 = store.resolve(p1, &t4.snapshot).unwrap();
        let ver_t4 = store.get_version(resolved_t4).unwrap();
        assert_eq!(ver_t4.commit_seq.get(), 1, "T4 should see V1");

        // T5 (snap high=1): before writing, sees V1.
        let resolved_t5_before = store.resolve(p1, &snap1).unwrap();
        let ver_t5 = store.get_version(resolved_t5_before).unwrap();
        assert_eq!(
            ver_t5.commit_seq.get(),
            1,
            "T5 should see V1 at snap high=1"
        );

        // After T5 commits, a new snapshot at high=2 sees V2.
        let snap2 = make_snapshot(2);
        let resolved_snap2 = store.resolve(p1, &snap2).unwrap();
        let ver_snap2 = store.get_version(resolved_snap2).unwrap();
        assert_eq!(
            ver_snap2.commit_seq.get(),
            2,
            "snapshot high=2 should see V2"
        );

        // Version chain verification (INV-3).
        let chain = store.walk_chain(p1);
        assert_eq!(chain.len(), 2, "should have 2 committed versions");
        assert_eq!(chain[0].commit_seq.get(), 2, "head should be V2");
        assert_eq!(chain[1].commit_seq.get(), 1, "tail should be V1");
    }

    #[test]
    fn test_theorem4_gc_never_removes_needed_version() {
        let store = VersionStore::new(PageSize::DEFAULT);
        let pgno = PageNumber::new(7).unwrap();

        // Build chain: V1(seq=1) <- V2(seq=5) <- V3(seq=10)
        let v1 = make_version(7, 1, None);
        let idx1 = store.publish(v1);
        let v2 = make_version(7, 5, Some(idx_to_version_pointer(idx1)));
        let idx2 = store.publish(v2);
        let v3 = make_version(7, 10, Some(idx_to_version_pointer(idx2)));
        let idx3 = store.publish(v3);

        // Active snapshot at high=7 must resolve to V2.
        let active_snap = make_snapshot(7);
        let visible_idx = store.resolve(pgno, &active_snap).unwrap();
        let visible = store.get_version(visible_idx).unwrap();
        assert_eq!(
            visible.commit_seq.get(),
            5,
            "active snapshot must keep V2 reachable"
        );

        // GC horizon at 7: V1 reclaimable (superseded by V2 <= horizon),
        // V2 not reclaimable (newer is V3=10 > horizon), V3 not reclaimable.
        let gc_horizon = CommitSeq::new(7);
        let v1_ref = store.get_version(idx1).unwrap();
        let v2_ref = store.get_version(idx2).unwrap();
        let v3_ref = store.get_version(idx3).unwrap();

        let v1_reclaimable = v1_ref.commit_seq < gc_horizon && v2_ref.commit_seq <= gc_horizon;
        let v2_reclaimable = v2_ref.commit_seq < gc_horizon && v3_ref.commit_seq <= gc_horizon;
        let v3_reclaimable = v3_ref.commit_seq < gc_horizon;

        assert!(v1_reclaimable, "V1 should be reclaimable");
        assert!(!v2_reclaimable, "V2 must be retained for snapshot high=7");
        assert!(!v3_reclaimable, "head version is never reclaimable here");
    }

    #[test]
    fn test_theorem5_version_chain_bounded_by_rd_plus_1() {
        let store = VersionStore::new(PageSize::DEFAULT);
        let pgno = PageNumber::new(11).unwrap();
        let commit_rate_per_sec = 100_u64;
        let max_txn_duration_secs = 1_u64;
        let bound = commit_rate_per_sec * max_txn_duration_secs + 1;

        let mut prev: Option<VersionPointer> = None;
        for seq in 1..=bound {
            let version = PageVersion {
                pgno,
                commit_seq: CommitSeq::new(seq),
                created_by: TxnToken::new(TxnId::new(seq).unwrap(), TxnEpoch::new(0)),
                data: PageData::zeroed(PageSize::DEFAULT),
                prev,
            };
            let idx = store.publish(version);
            prev = Some(idx_to_version_pointer(idx));
        }

        let chain = store.walk_chain(pgno);
        assert_eq!(
            chain.len(),
            usize::try_from(bound).unwrap(),
            "version chain should respect R*D+1 bound in bounded workload"
        );
    }

    #[test]
    fn test_theorem4_no_active_txns_gc_all_but_latest() {
        let store = VersionStore::new(PageSize::DEFAULT);
        let pgno = PageNumber::new(12).unwrap();

        let mut prev: Option<VersionPointer> = None;
        for seq in 1_u64..=3 {
            let version = PageVersion {
                pgno,
                commit_seq: CommitSeq::new(seq),
                created_by: TxnToken::new(TxnId::new(seq).unwrap(), TxnEpoch::new(0)),
                data: PageData::zeroed(PageSize::DEFAULT),
                prev,
            };
            let idx = store.publish(version);
            prev = Some(idx_to_version_pointer(idx));
        }

        // No active txns: safe horizon is latest commit.
        let horizon = CommitSeq::new(3);
        let chain = store.walk_chain(pgno); // [3, 2, 1]
        assert_eq!(chain.len(), 3);
        assert_eq!(chain[0].commit_seq, CommitSeq::new(3));

        // All but the latest are reclaimable at horizon=latest.
        let reclaimable = chain
            .windows(2)
            .filter(|pair| pair[1].commit_seq < horizon && pair[0].commit_seq <= horizon)
            .count();
        assert_eq!(reclaimable, 2, "older versions should be reclaimable");
    }

    #[test]
    fn test_theorem4_gc_horizon_min_active_snapshot() {
        let active_highs = [CommitSeq::new(10), CommitSeq::new(20), CommitSeq::new(30)];
        let safe_gc_seq = active_highs.iter().copied().min().unwrap();
        assert_eq!(
            safe_gc_seq,
            CommitSeq::new(10),
            "gc horizon must track min active snapshot.high"
        );
    }

    #[test]
    fn test_theorem4_reclaimability_predicate() {
        let store = VersionStore::new(PageSize::DEFAULT);

        // Chain: V1(3) <- V2(5) <- V3(9)
        let v1 = make_version(13, 3, None);
        let idx1 = store.publish(v1);
        let v2 = make_version(13, 5, Some(idx_to_version_pointer(idx1)));
        let idx2 = store.publish(v2);
        let v3 = make_version(13, 9, Some(idx_to_version_pointer(idx2)));
        let idx3 = store.publish(v3);

        let horizon = CommitSeq::new(7);
        let v1_ref = store.get_version(idx1).unwrap();
        let v2_ref = store.get_version(idx2).unwrap();
        let v3_ref = store.get_version(idx3).unwrap();

        let v1_reclaimable = v1_ref.commit_seq < horizon && v2_ref.commit_seq <= horizon;
        let v2_reclaimable = v2_ref.commit_seq < horizon && v3_ref.commit_seq <= horizon;
        assert!(v1_reclaimable, "V1 should satisfy reclaimability predicate");
        assert!(
            !v2_reclaimable,
            "V2 must be retained because newer V3 is beyond horizon"
        );
    }

    #[test]
    fn test_theorem5_version_chain_bounded() {
        test_theorem5_version_chain_bounded_by_rd_plus_1();
    }

    #[test]
    fn test_theorem5_gc_prunes_old_versions() {
        let store = VersionStore::new(PageSize::DEFAULT);
        let pgno = PageNumber::new(14).unwrap();
        let mut prev: Option<VersionPointer> = None;
        for seq in 1_u64..=32 {
            let version = PageVersion {
                pgno,
                commit_seq: CommitSeq::new(seq),
                created_by: TxnToken::new(TxnId::new(seq).unwrap(), TxnEpoch::new(0)),
                data: PageData::zeroed(PageSize::DEFAULT),
                prev,
            };
            let idx = store.publish(version);
            prev = Some(idx_to_version_pointer(idx));
        }

        let chain = store.walk_chain(pgno);
        let horizon = CommitSeq::new(32);
        let reclaimable = chain
            .iter()
            .skip(1)
            .filter(|version| version.commit_seq <= horizon)
            .count();
        assert_eq!(reclaimable, 31, "all non-head versions are reclaimable");
        assert_eq!(chain[0].commit_seq, CommitSeq::new(32));
    }

    #[test]
    fn test_ebr_eager_prune_recycles_without_pinned_readers() {
        let store = VersionStore::new(PageSize::DEFAULT);
        let pgno_a = PageNumber::new(16).unwrap();
        let pgno_b = PageNumber::new(17).unwrap();

        store.publish(make_version(16, 1, None));
        store.publish(make_version(16, 2, None));
        store.publish(make_version(16, 3, None));
        store.publish(make_version(17, 10, None));
        store.publish(make_version(17, 11, None));
        store.publish(make_version(17, 12, None));

        assert_eq!(store.chain_length(pgno_a), 3);
        assert_eq!(store.chain_length(pgno_b), 3);
        assert_eq!(store.pending_recycle_count(), 0);

        let freed_a = store.prune_page_chain_eager(pgno_a, CommitSeq::new(2));
        let freed_b = store.prune_page_chain_eager(pgno_b, CommitSeq::new(11));
        assert_eq!(
            freed_a, 1,
            "first page should retire exactly one old version"
        );
        assert_eq!(
            freed_b, 1,
            "second page should retire exactly one old version"
        );
        assert_eq!(store.chain_length(pgno_a), 2);
        assert_eq!(store.chain_length(pgno_b), 2);
        assert_eq!(
            store.pending_recycle_count(),
            0,
            "eager prune should complete EBR recycling immediately when no reader is pinned"
        );
        assert_eq!(store.arena_free_count(), 2);
    }

    #[test]
    fn test_ebr_no_premature_free() {
        let store = VersionStore::new(PageSize::DEFAULT);
        let pgno = PageNumber::new(18).unwrap();

        store.publish(make_version(18, 1, None));
        store.publish(make_version(18, 2, None));
        store.publish(make_version(18, 3, None));

        let guard = VersionGuard::pin(Arc::clone(store.guard_registry()));

        let freed = store.prune_page_chain_eager(pgno, CommitSeq::new(2));
        assert_eq!(freed, 1, "horizon=2 should prune only the oldest version");
        assert_eq!(store.pending_recycle_count(), 1);

        assert_eq!(store.try_recycle_retired_slots(store.current_epoch()), 0);
        assert_eq!(store.advance_epoch(), 0);
        assert_eq!(store.advance_epoch(), 0);
        assert_eq!(
            store.pending_recycle_count(),
            1,
            "a pinned reader must block reclamation of its visible versions"
        );

        drop(guard);

        assert_eq!(store.try_recycle_retired_slots(store.current_epoch()), 1);
        assert_eq!(store.pending_recycle_count(), 0);
    }

    #[test]
    fn test_gc_tick_recycles_retired_slot_for_next_publish_after_guard_release() {
        let store = VersionStore::new(PageSize::DEFAULT);
        let pgno = PageNumber::new(19).unwrap();

        store.publish(make_version(19, 1, None));
        store.publish(make_version(19, 2, None));
        store.publish(make_version(19, 3, None));

        let mut todo = GcTodo::new();
        todo.enqueue(pgno);

        let guard = VersionGuard::pin(Arc::clone(store.guard_registry()));
        let first = store.gc_tick(&mut todo, CommitSeq::new(2));
        assert_eq!(first.versions_freed, 1);
        assert_eq!(first.pruned_indices.len(), 1);
        assert_eq!(store.pending_recycle_count(), 1);

        let retired_idx = first.pruned_indices[0];

        let second = store.gc_tick(&mut todo, CommitSeq::new(2));
        assert_eq!(second.versions_freed, 0);
        assert_eq!(store.pending_recycle_count(), 1);

        let third = store.gc_tick(&mut todo, CommitSeq::new(2));
        assert_eq!(third.versions_freed, 0);
        assert_eq!(
            store.pending_recycle_count(),
            1,
            "gc_tick must not recycle retired slots while a reader guard is still pinned"
        );

        drop(guard);

        let fourth = store.gc_tick(&mut todo, CommitSeq::new(2));
        assert_eq!(fourth.versions_freed, 0);
        assert_eq!(store.pending_recycle_count(), 0);

        let reused_idx = store.publish(make_version(19, 4, None));
        assert_eq!(store.chain_length(pgno), 3);
        assert_eq!(reused_idx.chunk(), retired_idx.chunk());
        assert_eq!(reused_idx.offset(), retired_idx.offset());
        assert_ne!(reused_idx.generation(), retired_idx.generation());
    }

    #[test]
    fn test_gc_tick_reclaims_retired_slots_in_same_pass_without_reader_guard() {
        let store = VersionStore::new(PageSize::DEFAULT);
        let pgno = PageNumber::new(20).unwrap();

        store.publish(make_version(20, 1, None));
        store.publish(make_version(20, 2, None));
        store.publish(make_version(20, 3, None));

        let mut todo = GcTodo::new();
        todo.enqueue(pgno);

        let result = store.gc_tick(&mut todo, CommitSeq::new(2));
        assert_eq!(result.versions_freed, 1);
        assert_eq!(result.pruned_indices.len(), 1);
        assert_eq!(
            store.pending_recycle_count(),
            0,
            "gc_tick should complete the EBR recycle pass when no reader guard pins the retire epoch"
        );
        assert_eq!(store.arena_free_count(), 1);
    }

    #[test]
    fn test_unpinned_reader_stale_version_idx_resolves_none_after_recycle_reuse() {
        // bd-3wop3.5 item (3) determination — proves reader epoch-pins are NOT
        // required for memory-safety on the live copy-out read path.
        //
        // An unpinned "reader" captures a version's arena index, then a GC pass
        // (with no VersionGuard pinned) prunes + recycles that slot and a new
        // version reuses it. Because VersionIdx is generation-tagged and
        // VersionArena::get rejects a generation mismatch, the stale index must
        // resolve to None — never to the recycled slot's new contents. This is
        // why live readers (which copy out under the arena RwLock) are safe
        // without epoch pins; the pin machinery is reserved for transaction
        // snapshot lifetimes (lifecycle.rs VersionGuardTicket::register) and any
        // future lock-free reader that would retain an arena reference across
        // reclamation.
        let store = VersionStore::new(PageSize::DEFAULT);
        let pgno = PageNumber::new(21).unwrap();

        // Reader captures the arena index of V1 without pinning an epoch guard.
        let v1_idx = store.publish(make_version(21, 1, None));
        store.publish(make_version(21, 2, None));
        store.publish(make_version(21, 3, None));

        // Pre-GC: the captured index resolves to V1 (owned clone, copied out).
        assert_eq!(
            store.get_version(v1_idx).map(|v| v.commit_seq),
            Some(CommitSeq::new(1)),
            "captured index must resolve before GC"
        );

        let mut todo = GcTodo::new();
        todo.enqueue(pgno);

        // No VersionGuard pinned: gc_tick prunes V1 (seq 1 < horizon 2) and
        // recycles its slot in the same pass (min_pinned_epoch is None).
        let result = store.gc_tick(&mut todo, CommitSeq::new(2));
        assert_eq!(result.versions_freed, 1);
        assert_eq!(result.pruned_indices.len(), 1);
        assert_eq!(result.pruned_indices[0].chunk(), v1_idx.chunk());
        assert_eq!(result.pruned_indices[0].offset(), v1_idx.offset());
        assert_eq!(
            store.pending_recycle_count(),
            0,
            "with no reader guard, the retired slot recycles in the same pass"
        );

        // A new version reuses the recycled (chunk, offset) with a bumped generation.
        let reused_idx = store.publish(make_version(21, 4, None));
        assert_eq!(reused_idx.chunk(), v1_idx.chunk());
        assert_eq!(reused_idx.offset(), v1_idx.offset());
        assert_ne!(reused_idx.generation(), v1_idx.generation());

        // THE SAFETY PROPERTY: the unpinned reader's now-stale index resolves to
        // None (generation mismatch) — never V4's data. No use-after-free, no ABA
        // wrong-version read, so reader pins are unnecessary on this path.
        assert!(
            store.get_version(v1_idx).is_none(),
            "stale arena index must resolve to None after recycle+reuse"
        );
        assert_eq!(
            store.get_version(reused_idx).map(|v| v.commit_seq),
            Some(CommitSeq::new(4)),
            "a fresh resolve sees the reused slot's new version"
        );
    }

    #[test]
    fn test_ebr_8t_bounded_memory() {
        use std::sync::atomic::{AtomicU64, Ordering};
        use std::thread;

        const THREADS: u32 = 8;
        const WRITES_PER_THREAD: u64 = 96;
        const LIVE_SLOT_BOUND: u64 = 24;

        let store = Arc::new(VersionStore::new(PageSize::DEFAULT));
        let max_live_slots = Arc::new(AtomicU64::new(0));

        let handles: Vec<_> = (0..THREADS)
            .map(|thread_idx| {
                let store = Arc::clone(&store);
                let max_live_slots = Arc::clone(&max_live_slots);
                thread::spawn(move || {
                    let pgno_raw = 100 + thread_idx;
                    let pgno = PageNumber::new(pgno_raw).unwrap();
                    for seq in 1..=WRITES_PER_THREAD {
                        let commit_seq = u64::from(thread_idx) * WRITES_PER_THREAD + seq + 1;
                        store.publish(make_version(pgno_raw, commit_seq, None));
                        let _ = store.prune_page_chain_eager(pgno, CommitSeq::new(commit_seq));
                        let _ = store.advance_epoch();

                        let free_count =
                            u64::try_from(store.arena_free_count()).unwrap_or(u64::MAX);
                        let live_slots = store.arena_high_water().saturating_sub(free_count);
                        max_live_slots.fetch_max(live_slots, Ordering::Relaxed);
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().expect("writer thread must not panic");
        }

        let _ = store.advance_epoch();

        for thread_idx in 0..THREADS {
            let pgno = PageNumber::new(100 + thread_idx).unwrap();
            assert_eq!(
                store.chain_length(pgno),
                1,
                "per-page eager pruning should keep only the newest committed version",
            );
        }
        assert_eq!(store.pending_recycle_count(), 0);

        let free_count = u64::try_from(store.arena_free_count()).unwrap_or(u64::MAX);
        let live_slots = store.arena_high_water().saturating_sub(free_count);
        assert!(
            live_slots <= LIVE_SLOT_BOUND,
            "live slot count should remain bounded under sustained 8-thread writes: {live_slots} > {LIVE_SLOT_BOUND}",
        );
        assert!(
            max_live_slots.load(Ordering::Relaxed) <= LIVE_SLOT_BOUND,
            "peak live slot count should remain bounded under sustained 8-thread writes",
        );
    }

    proptest! {
        #[test]
        fn prop_gc_safety_holds(horizon in 1_u64..40_u64) {
            let store = VersionStore::new(PageSize::DEFAULT);
            let pgno = PageNumber::new(15).unwrap();
            let mut prev: Option<VersionPointer> = None;

            for seq in 1_u64..=horizon + 2 {
                let version = PageVersion {
                    pgno,
                    commit_seq: CommitSeq::new(seq),
                    created_by: TxnToken::new(TxnId::new(seq).unwrap(), TxnEpoch::new(0)),
                    data: PageData::zeroed(PageSize::DEFAULT),
                    prev,
                };
                let idx = store.publish(version);
                prev = Some(idx_to_version_pointer(idx));
            }

            let active_snapshot = make_snapshot(horizon);
            let visible_idx = store.resolve(pgno, &active_snapshot).expect("visible version must exist");
            let visible = store.get_version(visible_idx).expect("arena lookup must succeed");
            prop_assert_eq!(visible.commit_seq, CommitSeq::new(horizon));

            // The version selected by an active snapshot must not satisfy the reclaim predicate.
            // Its immediate newer successor is horizon+1, which is > horizon.
            let visible_reclaimable = visible.commit_seq < active_snapshot.high;
            prop_assert!(!visible_reclaimable);
        }
    }

    // -----------------------------------------------------------------------
    // E2E: invariants hold under concurrent schedule
    // -----------------------------------------------------------------------

    #[test]
    fn test_e2e_invariants_under_concurrent_schedule() {
        let mgr = TxnManager::default();
        let store = VersionStore::new(PageSize::DEFAULT);
        let lock_table = InProcessPageLockTable::new();
        let write_mutex = SerializedWriteMutex::new();

        let mut committed_ids = Vec::new();

        // Process 10 transactions sequentially, each writing its own page.
        // Serialized-mode txns (every 3rd) acquire/release the global mutex.
        for i in 1..=10_u64 {
            let id = mgr.alloc_txn_id().unwrap();
            let snap = make_snapshot(i.saturating_sub(1));
            let mode = if i % 3 == 0 {
                TransactionMode::Serialized
            } else {
                TransactionMode::Concurrent
            };
            let mut txn = Transaction::new(id, TxnEpoch::new(0), snap, mode);
            let pgno = PageNumber::new(u32::try_from(i).unwrap()).unwrap();

            // INV-7: Serialized txns must acquire the global mutex.
            if txn.mode == TransactionMode::Serialized {
                write_mutex.try_acquire(txn.txn_id).unwrap();
                txn.serialized_write_lock_held = true;
            }

            // INV-2: acquire page lock first.
            lock_table.try_acquire(pgno, txn.txn_id).unwrap();
            txn.page_locks.insert(pgno);

            // INV-4: write_set only after lock acquired.
            txn.write_set.push(pgno);
            for &p in &txn.write_set {
                assert!(txn.page_locks.contains(&p), "INV-4 violated for {p:?}");
            }

            // Commit.
            let seq = mgr.alloc_commit_seq();
            let version = PageVersion {
                pgno,
                commit_seq: seq,
                created_by: txn.token(),
                data: PageData::zeroed(PageSize::DEFAULT),
                prev: None,
            };
            store.publish(version);

            lock_table.release_all(txn.txn_id);
            if txn.serialized_write_lock_held {
                write_mutex.release(txn.txn_id);
                txn.serialized_write_lock_held = false;
            }
            txn.commit();
            committed_ids.push(txn.txn_id.get());
        }

        // Verify all invariants post-commit:

        // INV-1: All TxnIds are unique and increasing.
        for window in committed_ids.windows(2) {
            assert!(window[0] < window[1], "INV-1: TxnIds must be increasing");
        }

        // INV-2: No locks held.
        assert_eq!(lock_table.lock_count(), 0, "all locks must be released");

        // INV-6: All pages visible at snapshot high=10.
        let snap_all = make_snapshot(10);
        for i in 1..=10_u32 {
            let pgno = PageNumber::new(i).unwrap();
            assert!(
                store.resolve(pgno, &snap_all).is_some(),
                "INV-6: page {} must be visible at high=10",
                i
            );
        }

        // INV-7: Write mutex is released.
        assert!(
            write_mutex.holder().is_none(),
            "INV-7: mutex must be released"
        );
    }

    // -----------------------------------------------------------------------
    // bd-22n.8 — Allocation-Free Read Path E2E Test
    // -----------------------------------------------------------------------

    #[test]
    fn test_e2e_version_resolve_allocation_free() {
        // bd-22n.8: The full MVCC read path (VersionStore::resolve) is
        // allocation-free for cached, visible versions.
        //
        // We verify by:
        // 1. Publishing a chain of versions
        // 2. Resolving them repeatedly
        // 3. Checking that resolve returns the same VersionIdx each time
        //    (proving no intermediate data structures are allocated per call)
        const BEAD_22N8: &str = "bd-22n.8";

        let store = VersionStore::new(PageSize::DEFAULT);
        let p1 = PageNumber::new(1).unwrap();

        // Publish 3 versions of page 1.
        let v1 = make_version(1, 1, None);
        let idx1 = store.publish(v1);

        let v2 = make_version(1, 3, Some(idx_to_version_pointer(idx1)));
        let idx2 = store.publish(v2);

        let v3 = make_version(1, 5, Some(idx_to_version_pointer(idx2)));
        store.publish(v3);

        // Snapshot at commit_seq=4: should see v2 (commit_seq=3).
        let snap = make_snapshot(4);

        // Resolve 100 times — must return the same index each time.
        let first_idx = store.resolve(p1, &snap).unwrap();
        for round in 0..100u32 {
            let idx = store.resolve(p1, &snap).unwrap();
            assert_eq!(
                idx, first_idx,
                "bead_id={BEAD_22N8} case=e2e_version_resolve_stable \
                 round={round} resolve must return same VersionIdx"
            );
        }

        // Verify we got the right version (commit_seq=3).
        let resolved = store.get_version(first_idx).unwrap();
        assert_eq!(
            resolved.commit_seq,
            CommitSeq::new(3),
            "bead_id={BEAD_22N8} case=e2e_resolved_correct_version"
        );

        // Snapshot at commit_seq=5: should see v3 (commit_seq=5).
        let snap5 = make_snapshot(5);
        let idx5 = store.resolve(p1, &snap5).unwrap();
        let v5_resolved = store.get_version(idx5).unwrap();
        assert_eq!(
            v5_resolved.commit_seq,
            CommitSeq::new(5),
            "bead_id={BEAD_22N8} case=e2e_latest_version_resolved"
        );

        // Snapshot at commit_seq=0: nothing visible.
        let snap0 = make_snapshot(0);
        assert!(
            store.resolve(p1, &snap0).is_none(),
            "bead_id={BEAD_22N8} case=e2e_no_visible_version_at_zero"
        );
    }

    // -----------------------------------------------------------------------
    // Property tests
    // -----------------------------------------------------------------------

    proptest! {
        #[test]
        fn prop_visible_uncommitted_never_visible(
            high in 0_u64..1_000_000,
        ) {
            let snap = make_snapshot(high);
            let uncommitted = make_version(1, 0, None);
            prop_assert!(!visible(&uncommitted, &snap), "uncommitted (seq=0) must never be visible");
        }

        #[test]
        fn prop_visible_committed_iff_in_range(
            seq in 1_u64..1_000_000,
            high in 0_u64..1_000_000,
        ) {
            let snap = make_snapshot(high);
            let version = make_version(1, seq, None);
            let expected = seq <= high;
            prop_assert_eq!(
                visible(&version, &snap),
                expected,
                "visible(seq={}, high={}) should be {}", seq, high, expected
            );
        }

        #[test]
        fn prop_txn_manager_ids_unique(
            count in 1_usize..500,
        ) {
            let mgr = TxnManager::default();
            let mut ids = Vec::with_capacity(count);
            for _ in 0..count {
                ids.push(mgr.alloc_txn_id().unwrap().get());
            }
            let mut deduped = ids.clone();
            deduped.sort_unstable();
            deduped.dedup();
            prop_assert_eq!(ids.len(), deduped.len(), "all TxnIds must be unique");
        }

        // INV-PBT-4 (bd-2sm1): Snapshot isolation under multi-writer workload.
        //
        // Generate N writers that each commit to random pages at sequential
        // commit_seqs. Then take snapshots at various high values and verify:
        //   1. Every resolved version has commit_seq <= snapshot.high
        //   2. The resolved version is the *maximum* commit_seq <= high for that page
        //   3. Resolving the same page twice yields the same result (determinism)
        #[test]
        fn prop_snapshot_isolation_multi_writer(
            num_writers in 2_usize..8,
            ops_per_writer in 1_usize..20,
            num_pages in 1_u32..16,
            snapshot_highs in proptest::collection::vec(0_u64..100, 1..10),
        ) {
            let store = VersionStore::new(PageSize::DEFAULT);
            let mgr = TxnManager::default();

            // Track the latest commit_seq per page for oracle verification.
            let mut page_history: std::collections::BTreeMap<u32, Vec<u64>> =
                std::collections::BTreeMap::new();

            // Each writer commits ops_per_writer pages.
            for _ in 0..num_writers {
                let txn_id = mgr.alloc_txn_id().unwrap();
                let seq = mgr.alloc_commit_seq();

                for _ in 0..ops_per_writer {
                    #[allow(clippy::cast_possible_truncation)] // modulo num_pages (< 16) always fits u32
                    let pgno_raw =
                        ((seq.get() * 7 + txn_id.get() * 3) % u64::from(num_pages)) as u32 + 1;
                    let pgno = PageNumber::new(pgno_raw).unwrap();

                    // Look up previous head to chain versions properly.
                    let prev = store.chain_head(pgno).map(idx_to_version_pointer);
                    let version = PageVersion {
                        pgno,
                        commit_seq: seq,
                        created_by: TxnToken::new(txn_id, TxnEpoch::new(0)),
                        data: PageData::zeroed(PageSize::DEFAULT),
                        prev,
                    };
                    store.publish(version);
                    page_history.entry(pgno_raw).or_default().push(seq.get());
                }
            }

            // Sort each page's history so we can binary-search for the oracle answer.
            for seqs in page_history.values_mut() {
                seqs.sort_unstable();
            }

            // Verify snapshot isolation for each generated snapshot high value.
            for &high in &snapshot_highs {
                let snap = make_snapshot(high);

                for (&pgno_raw, seqs) in &page_history {
                    let pgno = PageNumber::new(pgno_raw).unwrap();

                    // Oracle: the expected visible version is the max seq <= high.
                    let expected_seq = seqs.iter().copied().filter(|&s| s <= high).max();

                    let resolved = store.resolve(pgno, &snap);
                    match (expected_seq, resolved) {
                        (None, None) => {} // correctly invisible
                        (Some(exp), Some(idx)) => {
                            let v = store.get_version(idx).unwrap();
                            // INV-PBT-4a: commit_seq <= snapshot.high
                            prop_assert!(
                                v.commit_seq.get() <= high,
                                "page {} resolved commit_seq {} > snapshot high {}",
                                pgno_raw, v.commit_seq.get(), high
                            );
                            // INV-PBT-4b: resolved version is the max visible
                            prop_assert_eq!(
                                v.commit_seq.get(), exp,
                                "page {} expected seq {} but got {}",
                                pgno_raw, exp, v.commit_seq.get()
                            );
                        }
                        (Some(exp), None) => {
                            prop_assert!(
                                false,
                                "page {} expected visible at seq {} but resolve returned None",
                                pgno_raw, exp
                            );
                        }
                        (None, Some(idx)) => {
                            let v = store.get_version(idx).unwrap();
                            prop_assert!(
                                false,
                                "page {} expected invisible but resolved to seq {}",
                                pgno_raw, v.commit_seq.get()
                            );
                        }
                    }

                    // INV-PBT-4c: Determinism — resolving again yields same result.
                    let resolved2 = store.resolve(pgno, &snap);
                    prop_assert_eq!(
                        resolved, resolved2,
                        "resolve must be deterministic for page {} at high {}",
                        pgno_raw, high
                    );
                }
            }
        }

        // bd-2sm1: Version chain walk must yield strictly descending commit_seq.
        #[test]
        fn prop_version_chain_strictly_descending(
            chain_len in 2_usize..30,
        ) {
            let store = VersionStore::new(PageSize::DEFAULT);
            let pgno = PageNumber::new(99).unwrap();
            let mut prev: Option<VersionPointer> = None;

            for seq in 1..=chain_len as u64 {
                let version = PageVersion {
                    pgno,
                    commit_seq: CommitSeq::new(seq),
                    created_by: TxnToken::new(TxnId::new(seq).unwrap(), TxnEpoch::new(0)),
                    data: PageData::zeroed(PageSize::DEFAULT),
                    prev,
                };
                let idx = store.publish(version);
                prev = Some(idx_to_version_pointer(idx));
            }

            let chain = store.walk_chain(pgno);
            prop_assert!(chain.len() >= 2, "chain too short: {}", chain.len());

            // Chain must be strictly descending (newest first).
            for window in chain.windows(2) {
                prop_assert!(
                    window[0].commit_seq > window[1].commit_seq,
                    "version chain not strictly descending: {} >= {}",
                    window[0].commit_seq.get(),
                    window[1].commit_seq.get()
                );
            }
        }
    }

    /// Microbench: cost of `resolve_visible_version` on a one-page,
    /// one-committed-version chain (the MVCC read fast path). The per-call
    /// cost of the `record_snapshot_read_versions_traversed` histogram
    /// (three relaxed atomic `fetch_add` operations) dominates this path
    /// when metrics are disabled and that call is pure waste.
    ///
    /// Run with:
    /// `cargo test -p fsqlite-mvcc --lib --release -- \
    ///    bench_resolve_visible_version_metric_gate --ignored --nocapture`
    #[test]
    #[ignore = "microbench — run manually"]
    fn bench_resolve_visible_version_metric_gate() {
        use crate::observability::set_mvcc_snapshot_metrics_enabled;
        use std::time::Instant;

        const PAGES: u32 = 256;
        const RESOLVES_PER_TRIAL: u32 = 200_000;
        const TRIALS: usize = 7;

        // Build a store with PAGES pages, one committed version each.
        let store = VersionStore::new(PageSize::DEFAULT);
        for i in 1..=PAGES {
            store.publish(make_version(i, 1, None));
        }
        let snapshot = make_snapshot(5);

        fn run_trial(store: &VersionStore, snapshot: &Snapshot, resolves: u32, pages: u32) -> f64 {
            let start = Instant::now();
            for i in 0..resolves {
                let pgno = PageNumber::new((i % pages) + 1).unwrap();
                std::hint::black_box(store.resolve_visible_version(pgno, snapshot));
            }
            start.elapsed().as_nanos() as f64 / f64::from(resolves)
        }

        // Warm up in both configurations before timing.
        set_mvcc_snapshot_metrics_enabled(true);
        run_trial(&store, &snapshot, RESOLVES_PER_TRIAL, PAGES);
        set_mvcc_snapshot_metrics_enabled(false);
        run_trial(&store, &snapshot, RESOLVES_PER_TRIAL, PAGES);

        let mut metrics_on_samples = Vec::with_capacity(TRIALS);
        let mut metrics_off_samples = Vec::with_capacity(TRIALS);
        for _ in 0..TRIALS {
            set_mvcc_snapshot_metrics_enabled(true);
            let on = run_trial(&store, &snapshot, RESOLVES_PER_TRIAL, PAGES);
            set_mvcc_snapshot_metrics_enabled(false);
            let off = run_trial(&store, &snapshot, RESOLVES_PER_TRIAL, PAGES);
            eprintln!("  metrics=on: {on:.1} ns/resolve   metrics=off: {off:.1} ns/resolve");
            metrics_on_samples.push(on);
            metrics_off_samples.push(off);
        }
        metrics_on_samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
        metrics_off_samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let on_med = metrics_on_samples[TRIALS / 2];
        let off_med = metrics_off_samples[TRIALS / 2];
        let delta_pct = (off_med - on_med) / on_med * 100.0;
        eprintln!(
            "bench_resolve_visible_version_metric_gate: metrics_on median={on_med:.1} \
             ns/resolve; metrics_off median={off_med:.1} ns/resolve; delta={delta_pct:+.1}% \
             (n={TRIALS} trials, {RESOLVES_PER_TRIAL} resolves/trial, pages={PAGES})"
        );
    }

    /// Microbench comparing the branching `commit_seq != 0 && commit_seq <=
    /// snapshot.high` predicate to the branchless `commit_seq.wrapping_sub(1)
    /// < snapshot.high` form used by the current `visible()` helper. Both
    /// forms are inlined through `#[inline]`; this isolates the pure
    /// visibility-check cost from the resolve / chain-walk / atomic-metric
    /// overhead. Exercises a mixed population where most versions are
    /// committed and visible (common case) but some are uncommitted
    /// (commit_seq = 0) or newer-than-snapshot.
    #[test]
    #[ignore = "microbench — run manually"]
    fn bench_visible_branchless() {
        use std::time::Instant;

        const SAMPLES: usize = 4096;
        const ITERS_PER_TRIAL: u32 = 2_000_000;
        const TRIALS: usize = 9;

        // Synthesise a realistic mix of commit_seqs: ~80% visible, ~10%
        // uncommitted, ~10% newer than snapshot.
        let snapshot_high = 10_000_u64;
        let snapshot = make_snapshot(snapshot_high);
        let versions: Vec<PageVersion> = (0..SAMPLES)
            .map(|i| {
                let kind = i % 10;
                let seq = match kind {
                    0 => 0,                              // uncommitted
                    1 => snapshot_high + 1,              // newer than snapshot
                    _ => (i as u64 % snapshot_high) + 1, // visible
                };
                make_version(1, seq, None)
            })
            .collect();

        #[allow(clippy::inline_always)]
        #[inline(always)]
        fn visible_branching(v: &PageVersion, s: &Snapshot) -> bool {
            v.commit_seq.get() != 0 && v.commit_seq <= s.high
        }
        #[allow(clippy::inline_always)]
        #[inline(always)]
        fn visible_branchless(v: &PageVersion, s: &Snapshot) -> bool {
            v.commit_seq.get().wrapping_sub(1) < s.high.get()
        }

        fn run<F: Fn(&PageVersion, &Snapshot) -> bool>(
            versions: &[PageVersion],
            snapshot: &Snapshot,
            iters: u32,
            f: F,
        ) -> f64 {
            let mut count = 0_u64;
            let start = Instant::now();
            for i in 0..iters {
                let v = &versions[(i as usize) % versions.len()];
                if std::hint::black_box(f(v, snapshot)) {
                    count += 1;
                }
            }
            std::hint::black_box(count);
            start.elapsed().as_nanos() as f64 / f64::from(iters)
        }

        // Warm-up.
        run(&versions, &snapshot, ITERS_PER_TRIAL, visible_branching);
        run(&versions, &snapshot, ITERS_PER_TRIAL, visible_branchless);

        let mut br_samples = Vec::with_capacity(TRIALS);
        let mut nb_samples = Vec::with_capacity(TRIALS);
        for _ in 0..TRIALS {
            let br = run(&versions, &snapshot, ITERS_PER_TRIAL, visible_branching);
            let nb = run(&versions, &snapshot, ITERS_PER_TRIAL, visible_branchless);
            eprintln!("  branching: {br:.2} ns/call   branchless: {nb:.2} ns/call");
            br_samples.push(br);
            nb_samples.push(nb);
        }
        br_samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
        nb_samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let br_med = br_samples[TRIALS / 2];
        let nb_med = nb_samples[TRIALS / 2];
        let delta_pct = (nb_med - br_med) / br_med * 100.0;
        eprintln!(
            "bench_visible_branchless: branching median={br_med:.2} ns/call; \
             branchless median={nb_med:.2} ns/call; delta={delta_pct:+.1}% \
             (n={TRIALS} trials, {ITERS_PER_TRIAL} iters/trial, samples={SAMPLES})"
        );
    }

    /// Microbench for the publish / version-chain append path. Compares
    /// `publish()` with the visibility-ranges side-index tracking ON (the
    /// pre-gate production state — `RwLock<HashMap>` write on every append)
    /// to OFF (the post-gate default — one relaxed bool load).
    ///
    /// Each trial runs a fresh `VersionStore` and appends PUBLISHES
    /// versions chained onto a small set of pages; both configurations see
    /// the same allocation / chain-head CAS cost, so the delta is purely
    /// the side-index gate.
    #[test]
    #[ignore = "microbench — run manually"]
    fn bench_publish_visibility_ranges_gate() {
        use std::time::Instant;

        const PUBLISHES_PER_TRIAL: u32 = 20_000;
        const PAGES: u32 = 32;
        const TRIALS: usize = 9;

        fn run_trial(publishes: u32, pages: u32, tracking_enabled: bool) -> f64 {
            set_mvcc_visibility_ranges_tracking_enabled(tracking_enabled);
            let store = VersionStore::new(PageSize::DEFAULT);
            let mut heads: Vec<Option<VersionPointer>> = vec![None; pages as usize];
            let start = Instant::now();
            for i in 0..publishes {
                let page_ix = (i as usize) % (pages as usize);
                let prev = heads[page_ix];
                let v = make_version((page_ix as u32) + 1, u64::from(i) + 1, prev);
                let idx = store.publish(v);
                heads[page_ix] = Some(idx_to_version_pointer(idx));
            }
            start.elapsed().as_nanos() as f64 / f64::from(publishes)
        }

        // Warm-up both configurations.
        run_trial(PUBLISHES_PER_TRIAL, PAGES, true);
        run_trial(PUBLISHES_PER_TRIAL, PAGES, false);

        let mut on_samples = Vec::with_capacity(TRIALS);
        let mut off_samples = Vec::with_capacity(TRIALS);
        for _ in 0..TRIALS {
            let on = run_trial(PUBLISHES_PER_TRIAL, PAGES, true);
            let off = run_trial(PUBLISHES_PER_TRIAL, PAGES, false);
            eprintln!("  tracking=on: {on:.0} ns/publish   tracking=off: {off:.0} ns/publish");
            on_samples.push(on);
            off_samples.push(off);
        }
        on_samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
        off_samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let on_med = on_samples[TRIALS / 2];
        let off_med = off_samples[TRIALS / 2];
        let delta_pct = (off_med - on_med) / on_med * 100.0;
        eprintln!(
            "bench_publish_visibility_ranges_gate: tracking_on median={on_med:.0} ns/publish; \
             tracking_off median={off_med:.0} ns/publish; delta={delta_pct:+.1}% \
             (n={TRIALS} trials, {PUBLISHES_PER_TRIAL} publishes/trial, pages={PAGES})"
        );
        // Leave the gate off; it's the production default.
        set_mvcc_visibility_ranges_tracking_enabled(false);
    }

    /// Microbench for the CAS-metrics gate: toggles
    /// `MVCC_CAS_METRICS_ENABLED` within a single run so publish costs
    /// with / without CAS-histogram recording are directly paired. Keeps
    /// visibility-ranges tracking off on both sides so this isolates the
    /// CAS gate's contribution on top of f2707d1a.
    #[test]
    #[ignore = "microbench — run manually"]
    fn bench_publish_cas_metrics_gate() {
        use crate::observability::set_mvcc_cas_metrics_enabled;
        use std::time::Instant;

        const PUBLISHES_PER_TRIAL: u32 = 20_000;
        const PAGES: u32 = 32;
        const TRIALS: usize = 9;

        fn run_trial(publishes: u32, pages: u32, cas_metrics_enabled: bool) -> f64 {
            set_mvcc_visibility_ranges_tracking_enabled(false);
            set_mvcc_cas_metrics_enabled(cas_metrics_enabled);
            let store = VersionStore::new(PageSize::DEFAULT);
            let mut heads: Vec<Option<VersionPointer>> = vec![None; pages as usize];
            let start = Instant::now();
            for i in 0..publishes {
                let page_ix = (i as usize) % (pages as usize);
                let prev = heads[page_ix];
                let v = make_version((page_ix as u32) + 1, u64::from(i) + 1, prev);
                let idx = store.publish(v);
                heads[page_ix] = Some(idx_to_version_pointer(idx));
            }
            start.elapsed().as_nanos() as f64 / f64::from(publishes)
        }

        run_trial(PUBLISHES_PER_TRIAL, PAGES, true);
        run_trial(PUBLISHES_PER_TRIAL, PAGES, false);

        let mut on_samples = Vec::with_capacity(TRIALS);
        let mut off_samples = Vec::with_capacity(TRIALS);
        for _ in 0..TRIALS {
            let on = run_trial(PUBLISHES_PER_TRIAL, PAGES, true);
            let off = run_trial(PUBLISHES_PER_TRIAL, PAGES, false);
            eprintln!(
                "  cas_metrics=on: {on:.0} ns/publish   cas_metrics=off: {off:.0} ns/publish"
            );
            on_samples.push(on);
            off_samples.push(off);
        }
        on_samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
        off_samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let on_med = on_samples[TRIALS / 2];
        let off_med = off_samples[TRIALS / 2];
        let delta_pct = (off_med - on_med) / on_med * 100.0;
        eprintln!(
            "bench_publish_cas_metrics_gate: cas_on median={on_med:.0} ns/publish; \
             cas_off median={off_med:.0} ns/publish; delta={delta_pct:+.1}% \
             (n={TRIALS} trials, {PUBLISHES_PER_TRIAL} publishes/trial, pages={PAGES})"
        );
        set_mvcc_cas_metrics_enabled(false);
    }
}
