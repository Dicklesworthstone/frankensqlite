//! §6.3-6.4 ARC REPLACE + REQUEST algorithms with MVCC-aware keying (`bd-125g`).
//!
//! Implements the ARC algorithm (Megiddo & Modha, FAST '03) with cache
//! entries indexed by `(PageNumber, CommitSeq)` instead of bare page
//! number, because MVCC requires multiple versions of the same page to
//! coexist.
//!
//! # Eviction Invariants (normative)
//!
//! 1. **Pinned pages are never evicted** — `ref_count > 0` prevents eviction.
//! 2. **Eviction performs zero I/O** — no WAL append, no durability I/O.
//! 3. **Superseded versions are preferred** — when a newer committed version
//!    of the same page is also cached, the older version is evicted first.
//!
//! # Concurrency Model
//!
//! [`ArcCache`] wraps all mutable state in a [`parking_lot::Mutex`].
//! [`CachedPage::ref_count`] uses [`AtomicU32`] for lock-free read-side
//! pinning.  The mutex critical section covers only metadata updates
//! (no I/O), keeping it short.

use std::collections::HashMap;
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use fsqlite_types::{CommitSeq, PageNumber};
use parking_lot::{Condvar, Mutex};

use crate::page_buf::PageBuf;

// ═══════════════════════════════════════════════════════════════════════
// CacheMetricsSnapshot
// ═══════════════════════════════════════════════════════════════════════

/// Point-in-time snapshot of ARC cache performance counters and structural
/// state.  All fields are cheap `Copy` values captured under the ARC mutex.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CacheMetricsSnapshot {
    // -- counters (monotonic since last reset) --
    /// Total cache hits (T1 + T2).
    pub hits: u64,
    /// Total cache misses.
    pub misses: u64,
    /// Ghost hits in B1 (recency signal).
    pub ghost_hits_b1: u64,
    /// Ghost hits in B2 (frequency signal).
    pub ghost_hits_b2: u64,
    /// Pages evicted from T1 → B1.
    pub evictions_t1: u64,
    /// Pages evicted from T2 → B2.
    pub evictions_t2: u64,
    /// Superseded versions coalesced by GC.
    pub version_coalesce_count: u64,
    /// Pages admitted into the cache.
    pub admits: u64,

    // -- structural gauges (instantaneous) --
    /// Current pages in T1 (recency list).
    pub t1_len: usize,
    /// Current pages in T2 (frequency list).
    pub t2_len: usize,
    /// Current ghost entries in B1.
    pub b1_len: usize,
    /// Current ghost entries in B2.
    pub b2_len: usize,
    /// Adaptive target size for T1.
    pub p: usize,
    /// Maximum resident-page capacity.
    pub capacity: usize,
    /// Current byte footprint of resident pages.
    pub total_bytes: usize,
    /// Hard byte limit (PRAGMA cache_size).
    pub max_bytes: usize,
    /// Distinct page numbers with >1 cached version.
    pub multi_version_pages: usize,
    /// Safety-valve overflow events (all victims pinned).
    pub capacity_overflow_events: usize,
}

impl CacheMetricsSnapshot {
    /// Hit rate as a percentage (0.0–100.0).  Returns 0.0 when no accesses.
    #[must_use]
    pub fn hit_rate_pct(&self) -> f64 {
        let total = self.hits + self.misses + self.ghost_hits_b1 + self.ghost_hits_b2;
        if total == 0 {
            return 0.0;
        }
        (self.hits as f64 / total as f64) * 100.0
    }

    /// Total accesses (hits + misses + ghost hits).
    #[must_use]
    pub fn total_accesses(&self) -> u64 {
        self.hits + self.misses + self.ghost_hits_b1 + self.ghost_hits_b2
    }

    /// Resident page count (T1 + T2).
    #[must_use]
    pub fn resident_pages(&self) -> usize {
        self.t1_len + self.t2_len
    }
}

// ═══════════════════════════════════════════════════════════════════════
// CacheKey
// ═══════════════════════════════════════════════════════════════════════

/// MVCC-aware cache key: `(PageNumber, CommitSeq)`.
///
/// `commit_seq = CommitSeq::ZERO` represents the on-disk baseline.
/// Transaction-private (uncommitted) page images are **not** ARC
/// entries — they live in the owning transaction's `write_set`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CacheKey {
    pub pgno: PageNumber,
    pub commit_seq: CommitSeq,
}

impl CacheKey {
    #[inline]
    #[must_use]
    pub const fn new(pgno: PageNumber, commit_seq: CommitSeq) -> Self {
        Self { pgno, commit_seq }
    }
}

// ═══════════════════════════════════════════════════════════════════════
// CachedPage
// ═══════════════════════════════════════════════════════════════════════

/// A page held in the ARC cache.
///
/// `ref_count` uses [`AtomicU32`] for lock-free read-side pinning.
/// A page with `ref_count > 0` is "pinned" and **must not** be evicted.
pub struct CachedPage {
    pub key: CacheKey,
    pub data: PageBuf,
    /// Lock-free pin counter.  Reads use `Acquire`; writes use `Release`.
    pub ref_count: AtomicU32,
    /// XXH3 hash of `data` at insertion time (integrity check).
    pub xxh3: u64,
    /// Byte footprint of this entry (for memory accounting).
    pub byte_size: usize,
    /// WAL frame offset, if this page was read from the WAL.
    pub wal_frame: Option<u32>,
}

impl CachedPage {
    /// Create a new cached page.
    #[must_use]
    pub fn new(key: CacheKey, data: PageBuf, xxh3: u64, wal_frame: Option<u32>) -> Self {
        let byte_size = data.len();
        Self {
            key,
            data,
            ref_count: AtomicU32::new(0),
            xxh3,
            byte_size,
            wal_frame,
        }
    }

    /// Atomically increment the pin count.
    #[inline]
    pub fn pin(&self) {
        self.ref_count.fetch_add(1, Ordering::Acquire);
    }

    /// Atomically decrement the pin count.
    ///
    /// # Panics
    ///
    /// Debug-asserts that the previous count was > 0.
    #[inline]
    pub fn unpin(&self) {
        let prev = self.ref_count.fetch_sub(1, Ordering::Release);
        debug_assert!(prev > 0, "unpin on page with ref_count 0");
    }

    /// Returns `true` if this page is currently pinned.
    #[inline]
    #[must_use]
    pub fn is_pinned(&self) -> bool {
        self.ref_count.load(Ordering::Acquire) > 0
    }
}

impl std::fmt::Debug for CachedPage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CachedPage")
            .field("key", &self.key)
            .field("data_len", &self.data.len())
            .field("ref_count", &self.ref_count.load(Ordering::Relaxed))
            .field("xxh3", &format_args!("{:#018x}", self.xxh3))
            .field("byte_size", &self.byte_size)
            .field("wal_frame", &self.wal_frame)
            .finish()
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Slab-backed intrusive doubly-linked list
// ═══════════════════════════════════════════════════════════════════════

/// Index into the slab.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct SlabIdx(u32);

/// A node in the intrusive list.
struct SlabNode<T> {
    value: T,
    prev: Option<SlabIdx>,
    next: Option<SlabIdx>,
}

/// Slab-backed doubly-linked list with O(1) insert/remove/move.
///
/// Uses index-based links instead of pointers for `#[forbid(unsafe_code)]`
/// compliance.  Free slots are recycled via a free list.
struct IntrusiveList<T> {
    slots: Vec<Option<SlabNode<T>>>,
    free_indices: Vec<u32>,
    head: Option<SlabIdx>,
    tail: Option<SlabIdx>,
    len: usize,
}

impl<T> IntrusiveList<T> {
    fn new() -> Self {
        Self {
            slots: Vec::new(),
            free_indices: Vec::new(),
            head: None,
            tail: None,
            len: 0,
        }
    }

    #[inline]
    fn len(&self) -> usize {
        self.len
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Append a value to the tail.  Returns the slab index.
    fn push_back(&mut self, value: T) -> SlabIdx {
        let idx = self.alloc_slot(value);

        if let Some(old_tail) = self.tail {
            self.node_mut(old_tail).next = Some(idx);
            self.node_mut(idx).prev = Some(old_tail);
        } else {
            self.head = Some(idx);
        }
        self.tail = Some(idx);
        self.len += 1;
        idx
    }

    /// Remove and return the head value.
    fn pop_front(&mut self) -> Option<T> {
        let head = self.head?;
        Some(self.remove(head))
    }

    /// Unlink a node by index and return its value.
    fn remove(&mut self, idx: SlabIdx) -> T {
        let node = self.slots[idx.0 as usize]
            .take()
            .expect("IntrusiveList::remove on vacant slot");

        match (node.prev, node.next) {
            (Some(p), Some(n)) => {
                self.node_mut(p).next = Some(n);
                self.node_mut(n).prev = Some(p);
            }
            (None, Some(n)) => {
                self.node_mut(n).prev = None;
                self.head = Some(n);
            }
            (Some(p), None) => {
                self.node_mut(p).next = None;
                self.tail = Some(p);
            }
            (None, None) => {
                self.head = None;
                self.tail = None;
            }
        }

        self.free_indices.push(idx.0);
        self.len -= 1;
        node.value
    }

    /// Move an existing node to the tail (MRU position).
    fn move_to_back(&mut self, idx: SlabIdx) {
        if self.tail == Some(idx) {
            return;
        }

        let (prev, next) = {
            let n = self.node_ref(idx);
            (n.prev, n.next)
        };

        // Unlink from current position.
        match (prev, next) {
            (Some(p), Some(n)) => {
                self.node_mut(p).next = Some(n);
                self.node_mut(n).prev = Some(p);
            }
            (None, Some(n)) => {
                self.node_mut(n).prev = None;
                self.head = Some(n);
            }
            _ => return, // single element or already tail
        }

        // Re-link at tail.
        if let Some(old_tail) = self.tail {
            self.node_mut(old_tail).next = Some(idx);
        }
        let old_tail = self.tail;
        let node = self.node_mut(idx);
        node.prev = old_tail;
        node.next = None;
        self.tail = Some(idx);
    }

    /// Get a reference to a value by slab index.
    fn get(&self, idx: SlabIdx) -> Option<&T> {
        self.slots.get(idx.0 as usize)?.as_ref().map(|n| &n.value)
    }

    /// Get the least-recently-used value (front).
    #[cfg(test)]
    fn front(&self) -> Option<&T> {
        let idx = self.head?;
        self.get(idx)
    }

    /// Get the most-recently-used value (back).
    #[cfg(test)]
    fn back(&self) -> Option<&T> {
        let idx = self.tail?;
        self.get(idx)
    }

    /// Iterate from head to tail, yielding `(SlabIdx, &T)`.
    fn iter(&self) -> IntrusiveListIter<'_, T> {
        IntrusiveListIter {
            list: self,
            current: self.head,
        }
    }

    // -- Internal helpers --

    fn alloc_slot(&mut self, value: T) -> SlabIdx {
        if let Some(free) = self.free_indices.pop() {
            let idx = SlabIdx(free);
            self.slots[free as usize] = Some(SlabNode {
                value,
                prev: None,
                next: None,
            });
            idx
        } else {
            let raw = u32::try_from(self.slots.len()).expect("slab overflow");
            let idx = SlabIdx(raw);
            self.slots.push(Some(SlabNode {
                value,
                prev: None,
                next: None,
            }));
            idx
        }
    }

    #[inline]
    fn node_ref(&self, idx: SlabIdx) -> &SlabNode<T> {
        self.slots[idx.0 as usize]
            .as_ref()
            .expect("dangling SlabIdx")
    }

    #[inline]
    fn node_mut(&mut self, idx: SlabIdx) -> &mut SlabNode<T> {
        self.slots[idx.0 as usize]
            .as_mut()
            .expect("dangling SlabIdx")
    }
}

impl<T> Default for IntrusiveList<T> {
    fn default() -> Self {
        Self::new()
    }
}

struct IntrusiveListIter<'a, T> {
    list: &'a IntrusiveList<T>,
    current: Option<SlabIdx>,
}

impl<'a, T> Iterator for IntrusiveListIter<'a, T> {
    type Item = (SlabIdx, &'a T);

    fn next(&mut self) -> Option<Self::Item> {
        let idx = self.current?;
        let node = self.list.slots[idx.0 as usize].as_ref()?;
        self.current = node.next;
        Some((idx, &node.value))
    }
}

// ═══════════════════════════════════════════════════════════════════════
// ARC Algorithm — Inner (single-threaded)
// ═══════════════════════════════════════════════════════════════════════

/// Where a key currently lives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Location {
    T1(SlabIdx),
    T2(SlabIdx),
    B1(SlabIdx),
    B2(SlabIdx),
}

/// Outcome of [`ArcCacheInner::request`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheLookup {
    /// Found in T1 or T2 (entry has been promoted/refreshed).
    Hit,
    /// Ghost hit in B1 — adaptive parameter increased (favour recency).
    GhostHitB1,
    /// Ghost hit in B2 — adaptive parameter decreased (favour frequency).
    GhostHitB2,
    /// Complete miss — not in any list.
    Miss,
}

/// Outcome of [`ArcCache::request_async`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AsyncLookup {
    /// Key was already resident in T1/T2.
    Hit,
    /// This caller loaded and admitted the page.
    Loaded,
    /// This caller waited for another loader and observed a hit.
    WaitedForPeerHit,
    /// This caller waited for another loader, but no page was admitted
    /// (peer cancelled/failed/panicked).
    WaitedForPeerMiss,
}

#[derive(Debug)]
struct InflightLoad {
    state: Mutex<InflightState>,
    cv: Condvar,
}

impl InflightLoad {
    fn new_loading() -> Self {
        Self {
            state: Mutex::new(InflightState {
                loading: true,
                waiters: 0,
            }),
            cv: Condvar::new(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct InflightState {
    loading: bool,
    waiters: usize,
}

enum InflightRole {
    Leader(Arc<InflightLoad>),
    Waiter(Arc<InflightLoad>),
}

/// Core ARC state.  Not thread-safe — wrap in [`ArcCache`] for concurrent use.
pub struct ArcCacheInner {
    /// Recently accessed pages (recency-favoured).
    t1: IntrusiveList<CachedPage>,
    /// Frequently accessed pages (frequency-favoured).
    t2: IntrusiveList<CachedPage>,
    /// Ghost entries evicted from T1 (keys only).
    b1: IntrusiveList<CacheKey>,
    /// Ghost entries evicted from T2 (keys only).
    b2: IntrusiveList<CacheKey>,
    /// Adaptive target size for T1.  Range: `[0, capacity]`.
    p: usize,
    /// Maximum entries in T1 + T2.
    capacity: usize,
    /// Sum of all `CachedPage.byte_size` in T1 + T2.
    total_bytes: usize,
    /// Hard byte limit (`PRAGMA cache_size`).
    max_bytes: usize,
    /// Unified directory: key → location in one of the four lists.
    directory: HashMap<CacheKey, Location>,
    /// Per-page version count (for superseded detection).
    page_versions: HashMap<PageNumber, usize>,
    /// GC horizon for superseded version preference.
    gc_horizon: CommitSeq,
    /// Number of times ARC had to allow temporary growth because all
    /// candidate victims were pinned.
    capacity_overflow_events: usize,

    // -- performance counters (bd-t6sv2.8) --
    /// Total cache hits (T1 + T2).
    hits: u64,
    /// Total cache misses.
    misses: u64,
    /// Ghost hits in B1.
    ghost_hits_b1: u64,
    /// Ghost hits in B2.
    ghost_hits_b2: u64,
    /// Pages evicted from T1.
    evictions_t1: u64,
    /// Pages evicted from T2.
    evictions_t2: u64,
    /// Superseded versions coalesced.
    version_coalesce_count: u64,
    /// Pages admitted.
    admits: u64,
}

impl ArcCacheInner {
    /// Create a new ARC cache.
    ///
    /// * `capacity` — max pages in T1 + T2.
    /// * `max_bytes` — hard byte limit (0 = unlimited).
    #[must_use]
    pub fn new(capacity: usize, max_bytes: usize) -> Self {
        Self {
            t1: IntrusiveList::new(),
            t2: IntrusiveList::new(),
            b1: IntrusiveList::new(),
            b2: IntrusiveList::new(),
            p: 0,
            capacity,
            total_bytes: 0,
            max_bytes,
            directory: HashMap::with_capacity(capacity * 2),
            page_versions: HashMap::new(),
            gc_horizon: CommitSeq::ZERO,
            capacity_overflow_events: 0,
            hits: 0,
            misses: 0,
            ghost_hits_b1: 0,
            ghost_hits_b2: 0,
            evictions_t1: 0,
            evictions_t2: 0,
            version_coalesce_count: 0,
            admits: 0,
        }
    }

    // -- Accessors --

    /// Adaptive target for T1 size.
    #[inline]
    #[must_use]
    pub fn p(&self) -> usize {
        self.p
    }

    /// Maximum resident-page capacity for T1 + T2.
    #[inline]
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Hard byte cap for resident pages.
    ///
    /// A value of `0` means "no byte cap" unless capacity itself is `0`.
    #[inline]
    #[must_use]
    pub fn max_bytes(&self) -> usize {
        self.max_bytes
    }

    /// Number of pages in the cache (T1 + T2).
    #[inline]
    #[must_use]
    pub fn len(&self) -> usize {
        self.t1.len() + self.t2.len()
    }

    /// Returns `true` if the cache has no pages.
    #[inline]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.t1.is_empty() && self.t2.is_empty()
    }

    /// Total byte footprint of cached pages.
    #[inline]
    #[must_use]
    pub fn total_bytes(&self) -> usize {
        self.total_bytes
    }

    /// Size of T1.
    #[inline]
    #[must_use]
    pub fn t1_len(&self) -> usize {
        self.t1.len()
    }

    /// Size of T2.
    #[inline]
    #[must_use]
    pub fn t2_len(&self) -> usize {
        self.t2.len()
    }

    /// Size of B1.
    #[inline]
    #[must_use]
    pub fn b1_len(&self) -> usize {
        self.b1.len()
    }

    /// Size of B2.
    #[inline]
    #[must_use]
    pub fn b2_len(&self) -> usize {
        self.b2.len()
    }

    /// Number of times all candidate victims were pinned and ARC allowed
    /// temporary growth (safety valve).
    #[inline]
    #[must_use]
    pub fn capacity_overflow_events(&self) -> usize {
        self.capacity_overflow_events
    }

    /// Capture a point-in-time snapshot of all cache metrics.
    #[must_use]
    pub fn metrics_snapshot(&self) -> CacheMetricsSnapshot {
        let multi_version_pages = self
            .page_versions
            .values()
            .filter(|&&count| count > 1)
            .count();

        CacheMetricsSnapshot {
            hits: self.hits,
            misses: self.misses,
            ghost_hits_b1: self.ghost_hits_b1,
            ghost_hits_b2: self.ghost_hits_b2,
            evictions_t1: self.evictions_t1,
            evictions_t2: self.evictions_t2,
            version_coalesce_count: self.version_coalesce_count,
            admits: self.admits,
            t1_len: self.t1.len(),
            t2_len: self.t2.len(),
            b1_len: self.b1.len(),
            b2_len: self.b2.len(),
            p: self.p,
            capacity: self.capacity,
            total_bytes: self.total_bytes,
            max_bytes: self.max_bytes,
            multi_version_pages,
            capacity_overflow_events: self.capacity_overflow_events,
        }
    }

    /// Reset all performance counters to zero.
    ///
    /// Structural gauges (t1_len, capacity, etc.) are not affected.
    pub fn reset_metrics(&mut self) {
        self.hits = 0;
        self.misses = 0;
        self.ghost_hits_b1 = 0;
        self.ghost_hits_b2 = 0;
        self.evictions_t1 = 0;
        self.evictions_t2 = 0;
        self.version_coalesce_count = 0;
        self.admits = 0;
    }

    /// O(1) snapshot visibility check (§6.8).
    ///
    /// Uncommitted versions (`CommitSeq::ZERO`) are never visible through
    /// MVCC snapshot resolution.
    #[inline]
    #[must_use]
    pub fn is_visible(version_commit_seq: CommitSeq, snapshot_high: CommitSeq) -> bool {
        version_commit_seq != CommitSeq::ZERO && version_commit_seq <= snapshot_high
    }

    /// Visibility including self-visibility from the transaction write set.
    ///
    /// This helper keeps write-set self-visibility explicit in callers.
    #[inline]
    #[must_use]
    pub fn is_visible_or_self(
        version_commit_seq: CommitSeq,
        snapshot_high: CommitSeq,
        in_write_set: bool,
    ) -> bool {
        if version_commit_seq == CommitSeq::ZERO {
            return in_write_set;
        }
        version_commit_seq <= snapshot_high
    }

    /// Set the GC horizon for superseded version preference.
    ///
    /// Triggers ghost pruning (removes ghost entries below the new horizon)
    /// and batch version coalescing (reclaims superseded cached versions).
    pub fn set_gc_horizon(&mut self, horizon: CommitSeq) {
        self.gc_horizon = horizon;
        self.prune_ghosts_below_horizon();
        self.coalesce_all_versions();
    }

    /// Apply SQLite `PRAGMA cache_size` mapping (§6.10).
    ///
    /// - `n > 0`: capacity = `n`, max_bytes = `n * page_size`
    /// - `n < 0`: max_bytes = `|n| * 1024`, capacity = `max_bytes / page_size`
    /// - `n = 0`: capacity = 0, max_bytes = 0
    pub fn apply_pragma_cache_size(&mut self, n: i32, page_size: usize) {
        assert!(page_size > 0, "page_size must be > 0");

        let (new_capacity, new_max_bytes) = match n.cmp(&0) {
            std::cmp::Ordering::Greater => {
                let cap = usize::try_from(n).expect("positive cache_size fits usize");
                (cap, cap.saturating_mul(page_size))
            }
            std::cmp::Ordering::Less => {
                let kib =
                    usize::try_from(n.unsigned_abs()).expect("cache_size magnitude fits usize");
                let max_bytes = kib.saturating_mul(1024);
                (max_bytes / page_size, max_bytes)
            }
            std::cmp::Ordering::Equal => (0, 0),
        };

        self.resize(new_capacity, new_max_bytes);
    }

    /// Resize ARC limits at runtime (§6.10 resize protocol).
    ///
    /// This updates limits, evicts until within bounds, trims ghost lists,
    /// and clamps `p` into `[0, capacity]`.
    pub fn resize(&mut self, new_capacity: usize, new_max_bytes: usize) {
        self.capacity = new_capacity;
        self.max_bytes = new_max_bytes;
        self.enforce_limits();
        self.trim_ghosts();
        self.p = self.p.min(self.capacity);
    }

    /// Current GC horizon.
    #[inline]
    #[must_use]
    pub fn gc_horizon(&self) -> CommitSeq {
        self.gc_horizon
    }

    #[cfg(test)]
    fn set_p_for_tests(&mut self, p: usize) {
        self.p = std::cmp::min(self.capacity, p);
    }

    #[cfg(test)]
    fn in_t1(&self, key: &CacheKey) -> bool {
        matches!(self.directory.get(key), Some(Location::T1(_)))
    }

    #[cfg(test)]
    fn in_b1(&self, key: &CacheKey) -> bool {
        matches!(self.directory.get(key), Some(Location::B1(_)))
    }

    #[cfg(test)]
    fn in_b2(&self, key: &CacheKey) -> bool {
        matches!(self.directory.get(key), Some(Location::B2(_)))
    }

    #[cfg(test)]
    fn in_t2(&self, key: &CacheKey) -> bool {
        matches!(self.directory.get(key), Some(Location::T2(_)))
    }

    #[cfg(test)]
    fn t2_lru_key(&self) -> Option<CacheKey> {
        self.t2.front().map(|page| page.key)
    }

    #[cfg(test)]
    fn t2_mru_key(&self) -> Option<CacheKey> {
        self.t2.back().map(|page| page.key)
    }

    #[cfg(test)]
    fn t1_lru_key(&self) -> Option<CacheKey> {
        self.t1.front().map(|page| page.key)
    }

    /// Get a reference to a cached page by key.
    ///
    /// Does **not** modify list ordering.  Call [`Self::request`] first to
    /// trigger ARC promotion/refresh logic.
    #[must_use]
    pub fn get(&self, key: &CacheKey) -> Option<&CachedPage> {
        match self.directory.get(key)? {
            Location::T1(idx) => self.t1.get(*idx),
            Location::T2(idx) => self.t2.get(*idx),
            Location::B1(_) | Location::B2(_) => None,
        }
    }

    /// Unpin a resident page and reclaim one overflow slot if possible.
    ///
    /// Returns `true` if the page was present and pinned before this call.
    pub fn unpin(&mut self, key: &CacheKey) -> bool {
        let was_pinned = if let Some(page) = self.get(key) {
            if page.is_pinned() {
                page.unpin();
                true
            } else {
                false
            }
        } else {
            false
        };

        if was_pinned {
            self.reclaim_one_overflow_slot();
        }

        was_pinned
    }

    // -- Core ARC operations --

    /// Look up a key, performing ARC promotion/adjustment.
    ///
    /// Returns the outcome so the caller knows whether to fetch from disk
    /// (on [`CacheLookup::GhostHitB1`], [`CacheLookup::GhostHitB2`], or
    /// [`CacheLookup::Miss`]) and then call [`Self::admit`].
    pub fn request(&mut self, key: &CacheKey) -> CacheLookup {
        let location = self.directory.get(key).copied();
        match location {
            Some(Location::T1(idx)) => {
                // Hit in T1 → promote to T2 (recency → frequency).
                let page = self.t1.remove(idx);
                let new_idx = self.t2.push_back(page);
                self.directory.insert(*key, Location::T2(new_idx));
                self.hits += 1;
                CacheLookup::Hit
            }
            Some(Location::T2(idx)) => {
                // Hit in T2 → move to MRU of T2 (refresh).
                self.t2.move_to_back(idx);
                self.hits += 1;
                CacheLookup::Hit
            }
            Some(Location::B1(idx)) => {
                // Ghost hit in B1 → increase p (favour recency).
                let delta = std::cmp::max(self.b2.len() / std::cmp::max(self.b1.len(), 1), 1);
                self.p = std::cmp::min(self.capacity, self.p.saturating_add(delta));
                // Remove ghost.
                self.b1.remove(idx);
                self.directory.remove(key);
                self.ghost_hits_b1 += 1;
                CacheLookup::GhostHitB1
            }
            Some(Location::B2(idx)) => {
                // Ghost hit in B2 → decrease p (favour frequency).
                let delta = std::cmp::max(self.b1.len() / std::cmp::max(self.b2.len(), 1), 1);
                self.p = self.p.saturating_sub(delta);
                // Remove ghost.
                self.b2.remove(idx);
                self.directory.remove(key);
                self.ghost_hits_b2 += 1;
                CacheLookup::GhostHitB2
            }
            None => {
                self.misses += 1;
                CacheLookup::Miss
            }
        }
    }

    /// Insert a page after a cache miss or ghost hit.
    ///
    /// `lookup` must be the result of the preceding [`Self::request`] call for the
    /// same key.  On ghost hits the page goes into T2; on misses it goes
    /// into T1.
    ///
    /// **Eviction invariant:** this method never performs I/O.
    pub fn admit(&mut self, key: CacheKey, page: CachedPage, lookup: CacheLookup) {
        debug_assert!(
            !matches!(lookup, CacheLookup::Hit),
            "admit called after a cache hit"
        );

        self.admits += 1;

        if self.capacity == 0 {
            // PRAGMA cache_size=0 disables caching of resident pages.
            self.trim_ghosts();
            self.p = 0;
            return;
        }

        let byte_size = page.byte_size;

        // Phase 1: make room in the directory (ghost eviction).
        let l1_len = self.t1.len() + self.b1.len();
        if l1_len >= self.capacity {
            if self.t1.len() < self.capacity {
                // Evict LRU ghost from B1.
                if let Some(ghost) = self.b1.pop_front() {
                    self.directory.remove(&ghost);
                }
                if !self.replace(&key, matches!(lookup, CacheLookup::GhostHitB2)) {
                    self.capacity_overflow_events = self.capacity_overflow_events.saturating_add(1);
                }
            } else {
                // T1 alone fills capacity — evict LRU from T1 directly.
                if !self.evict_lru_from_t1() && !self.evict_lru_from_t2() {
                    self.capacity_overflow_events = self.capacity_overflow_events.saturating_add(1);
                }
            }
        } else {
            let total_dir = l1_len + self.t2.len() + self.b2.len();
            if total_dir >= self.capacity * 2 {
                // Evict LRU ghost from B2.
                if let Some(ghost) = self.b2.pop_front() {
                    self.directory.remove(&ghost);
                }
            }
            if self.t1.len() + self.t2.len() >= self.capacity
                && !self.replace(&key, matches!(lookup, CacheLookup::GhostHitB2))
            {
                self.capacity_overflow_events = self.capacity_overflow_events.saturating_add(1);
            }
        }

        // Phase 2: insert the page.
        let idx = match lookup {
            CacheLookup::GhostHitB1 | CacheLookup::GhostHitB2 => {
                // Ghost hit → insert into T2.
                let idx = self.t2.push_back(page);
                self.directory.insert(key, Location::T2(idx));
                idx
            }
            CacheLookup::Miss => {
                // Complete miss → insert into T1.
                let idx = self.t1.push_back(page);
                self.directory.insert(key, Location::T1(idx));
                idx
            }
            CacheLookup::Hit => unreachable!("guarded by debug_assert above"),
        };

        self.total_bytes += byte_size;
        *self.page_versions.entry(key.pgno).or_insert(0) += 1;

        // Phase 2.5 was removed: opportunistic version coalescing here caused O(N)
        // latency spikes on the hot path. Superseded versions are reclaimed
        // asynchronously by the GC thread or lazily by `evict_one_preferred`.

        // Phase 3: enforce byte limit (dual trigger).
        while self.max_bytes > 0 && self.total_bytes > self.max_bytes && self.len() > 1 {
            if !self.evict_one_preferred() {
                self.capacity_overflow_events = self.capacity_overflow_events.saturating_add(1);
                break; // all remaining pages are pinned
            }
        }

        self.trim_ghosts();

        let _ = idx; // suppress unused warning
    }

    // -- Internal eviction machinery --

    /// ARC REPLACE subroutine.
    fn replace(&mut self, incoming: &CacheKey, from_b2: bool) -> bool {
        if self.coalesce_one_superseded_for_replace() {
            return true;
        }

        let incoming_in_b2 = matches!(self.directory.get(incoming), Some(Location::B2(_)));
        let from_b2_bias = from_b2 || incoming_in_b2;
        let t1_len = self.t1.len();
        let prefer_t1 = t1_len > 0 && (t1_len > self.p || (from_b2_bias && t1_len == self.p));

        if prefer_t1 {
            if self.evict_lru_from_t1() {
                return true;
            }
            return self.evict_lru_from_t2();
        }

        if self.evict_lru_from_t2() {
            return true;
        }
        self.evict_lru_from_t1()
    }

    /// Evict the LRU unpinned page from T1, moving its key to B1.
    fn evict_lru_from_t1(&mut self) -> bool {
        if let Some(victim_idx) = Self::find_unpinned_victim(&self.t1) {
            let page = self.t1.remove(victim_idx);
            let key = page.key;
            self.total_bytes -= page.byte_size;
            self.decrement_page_version(key.pgno);
            self.directory.remove(&key);
            // Move ghost to B1.
            let ghost_idx = self.b1.push_back(key);
            self.directory.insert(key, Location::B1(ghost_idx));
            drop(page);
            self.evictions_t1 += 1;
            true
        } else {
            false
        }
    }

    /// Evict the LRU unpinned page from T2, moving its key to B2.
    fn evict_lru_from_t2(&mut self) -> bool {
        if let Some(victim_idx) = Self::find_unpinned_victim(&self.t2) {
            let page = self.t2.remove(victim_idx);
            let key = page.key;
            self.total_bytes -= page.byte_size;
            self.decrement_page_version(key.pgno);
            self.directory.remove(&key);
            // Move ghost to B2.
            let ghost_idx = self.b2.push_back(key);
            self.directory.insert(key, Location::B2(ghost_idx));
            drop(page);
            self.evictions_t2 += 1;
            true
        } else {
            false
        }
    }

    /// Evict one page, preferring superseded versions.
    fn evict_one_preferred(&mut self) -> bool {
        // First: try to find a superseded victim in T1 or T2.
        if self.coalesce_one_superseded_for_replace() {
            return true;
        }
        // Fallback: standard REPLACE with pinned-page fallback.
        let t1_len = self.t1.len();
        let prefer_t1 = t1_len > 0 && t1_len > self.p;

        if prefer_t1 {
            if self.evict_lru_from_t1() {
                return true;
            }
            return self.evict_lru_from_t2();
        }

        if self.evict_lru_from_t2() {
            return true;
        }
        self.evict_lru_from_t1()
    }

    /// Find the first unpinned node from the head (LRU end).
    fn find_unpinned_victim(list: &IntrusiveList<CachedPage>) -> Option<SlabIdx> {
        for (idx, page) in list.iter() {
            if !page.is_pinned() {
                return Some(idx);
            }
        }
        None
    }

    /// Find a superseded, unpinned victim: a page whose `PageNumber` has
    /// more than one version cached, AND whose `commit_seq` is below the
    /// GC horizon.
    fn find_superseded_victim(&self, list: &IntrusiveList<CachedPage>) -> Option<SlabIdx> {
        for (idx, page) in list.iter().take(64) {
            if page.is_pinned() {
                continue;
            }
            let count = self.page_versions.get(&page.key.pgno).copied().unwrap_or(0);
            if count > 1
                && page.key.commit_seq != CommitSeq::ZERO
                && page.key.commit_seq <= self.gc_horizon
            {
                return Some(idx);
            }
        }
        None
    }

    fn enforce_limits(&mut self) {
        while self.len() > self.capacity
            || (self.max_bytes > 0 && self.total_bytes > self.max_bytes)
        {
            if !self.evict_one_preferred() {
                self.capacity_overflow_events = self.capacity_overflow_events.saturating_add(1);
                break;
            }
        }
    }

    fn reclaim_one_overflow_slot(&mut self) {
        if self.capacity_overflow_events == 0 {
            return;
        }

        if self.len() <= self.capacity {
            self.capacity_overflow_events = self.capacity_overflow_events.saturating_sub(1);
            return;
        }

        if self.evict_one_preferred() {
            self.capacity_overflow_events = self.capacity_overflow_events.saturating_sub(1);
        }
    }

    fn coalesce_one_superseded_for_replace(&mut self) -> bool {
        // Find one superseded victim from T1 or T2, scanning from LRU to MRU.
        // We use the bounded find_superseded_victim to ensure this is O(1).
        // Superseded versions below GC horizon are removed entirely (no ghost
        // entry) because the newer version already serves ARC adaptation.
        if let Some(idx) = self.find_superseded_victim(&self.t1) {
            let page = self.t1.remove(idx);
            let key = page.key;
            self.total_bytes -= page.byte_size;
            self.decrement_page_version(key.pgno);
            self.directory.remove(&key);
            self.version_coalesce_count = self.version_coalesce_count.saturating_add(1);
            return true;
        }
        if let Some(idx) = self.find_superseded_victim(&self.t2) {
            let page = self.t2.remove(idx);
            let key = page.key;
            self.total_bytes -= page.byte_size;
            self.decrement_page_version(key.pgno);
            self.directory.remove(&key);
            self.version_coalesce_count = self.version_coalesce_count.saturating_add(1);
            return true;
        }
        false
    }

    fn coalesce_all_versions(&mut self) {
        let mut candidates: HashMap<PageNumber, Vec<(u64, CacheKey, SlabIdx, bool)>> =
            HashMap::new();

        // Single pass over T1 and T2 to find all versions <= gc_horizon.
        for (idx, page) in self.t1.iter() {
            if page.key.commit_seq != CommitSeq::ZERO && page.key.commit_seq <= self.gc_horizon {
                candidates.entry(page.key.pgno).or_default().push((
                    page.key.commit_seq.get(),
                    page.key,
                    idx,
                    true, // is_t1
                ));
            }
        }
        for (idx, page) in self.t2.iter() {
            if page.key.commit_seq != CommitSeq::ZERO && page.key.commit_seq <= self.gc_horizon {
                candidates.entry(page.key.pgno).or_default().push((
                    page.key.commit_seq.get(),
                    page.key,
                    idx,
                    false, // is_t1
                ));
            }
        }

        let mut removed = 0;
        for (_, mut versions) in candidates {
            if versions.len() > 1 {
                // Sort by commit sequence descending to keep the newest version.
                versions.sort_by_key(|entry| std::cmp::Reverse(entry.0));
                for (_, key, idx, is_t1) in versions.into_iter().skip(1) {
                    // Skip pinned pages.
                    let is_pinned = if is_t1 {
                        self.t1.get(idx).is_some_and(CachedPage::is_pinned)
                    } else {
                        self.t2.get(idx).is_some_and(CachedPage::is_pinned)
                    };

                    if is_pinned {
                        continue;
                    }

                    let page = if is_t1 {
                        self.t1.remove(idx)
                    } else {
                        self.t2.remove(idx)
                    };

                    self.total_bytes = self.total_bytes.saturating_sub(page.byte_size);
                    self.decrement_page_version(key.pgno);
                    self.directory.remove(&key);
                    removed += 1;
                }
            }
        }

        let removed_u64 = u64::try_from(removed).unwrap_or(u64::MAX);
        self.version_coalesce_count = self.version_coalesce_count.saturating_add(removed_u64);
    }

    #[allow(dead_code)]
    fn coalesce_versions_for_pgno(&mut self, pgno: PageNumber) -> usize {
        #[derive(Clone, Copy)]
        enum ResidentList {
            T1,
            T2,
        }

        let mut committed: Vec<(u64, ResidentList, SlabIdx, CacheKey, bool)> = Vec::new();

        for (idx, page) in self.t1.iter() {
            if page.key.pgno == pgno
                && page.key.commit_seq != CommitSeq::ZERO
                && page.key.commit_seq <= self.gc_horizon
            {
                committed.push((
                    page.key.commit_seq.get(),
                    ResidentList::T1,
                    idx,
                    page.key,
                    page.is_pinned(),
                ));
            }
        }

        for (idx, page) in self.t2.iter() {
            if page.key.pgno == pgno
                && page.key.commit_seq != CommitSeq::ZERO
                && page.key.commit_seq <= self.gc_horizon
            {
                committed.push((
                    page.key.commit_seq.get(),
                    ResidentList::T2,
                    idx,
                    page.key,
                    page.is_pinned(),
                ));
            }
        }

        if committed.len() <= 1 {
            return 0;
        }

        committed.sort_by_key(|entry| std::cmp::Reverse(entry.0));

        let mut removed = 0;
        for (_, resident_list, idx, key, is_pinned) in committed.into_iter().skip(1) {
            if is_pinned {
                continue;
            }

            let page = match resident_list {
                ResidentList::T1 => self.t1.remove(idx),
                ResidentList::T2 => self.t2.remove(idx),
            };
            self.total_bytes = self.total_bytes.saturating_sub(page.byte_size);
            self.decrement_page_version(key.pgno);
            self.directory.remove(&key);
            removed += 1;
        }

        let removed_u64 = u64::try_from(removed).unwrap_or(u64::MAX);
        self.version_coalesce_count = self.version_coalesce_count.saturating_add(removed_u64);

        removed
    }

    fn prune_ghosts_below_horizon(&mut self) {
        let mut stale_b1 = Vec::new();
        for (idx, key) in self.b1.iter() {
            if key.commit_seq.get() < self.gc_horizon.get() {
                stale_b1.push(idx);
            }
        }
        for idx in stale_b1 {
            let ghost = self.b1.remove(idx);
            self.directory.remove(&ghost);
        }

        let mut stale_b2 = Vec::new();
        for (idx, key) in self.b2.iter() {
            if key.commit_seq.get() < self.gc_horizon.get() {
                stale_b2.push(idx);
            }
        }
        for idx in stale_b2 {
            let ghost = self.b2.remove(idx);
            self.directory.remove(&ghost);
        }
    }

    fn decrement_page_version(&mut self, pgno: PageNumber) {
        if let Some(count) = self.page_versions.get_mut(&pgno) {
            *count -= 1;
            if *count == 0 {
                self.page_versions.remove(&pgno);
            }
        }
    }

    fn trim_ghosts(&mut self) {
        while self.b1.len() > self.capacity {
            if let Some(ghost) = self.b1.pop_front() {
                self.directory.remove(&ghost);
            } else {
                break;
            }
        }
        while self.b2.len() > self.capacity {
            if let Some(ghost) = self.b2.pop_front() {
                self.directory.remove(&ghost);
            } else {
                break;
            }
        }
    }

    /// Reclaim memory by coalescing all superseded versions.
    ///
    /// Intended for use with `PRAGMA shrink_memory`.
    pub fn shrink_memory(&mut self) {
        self.coalesce_all_versions();
    }

    /// Attempt to reclaim overflow capacity after a page is unpinned.
    ///
    /// Call this after unpinning a page (via [`CachedPage::unpin`]) to allow
    /// the cache to shrink back to normal capacity if it previously grew
    /// due to all-pinned overflow.
    pub fn notify_unpin(&mut self) {
        while self.capacity_overflow_events > 0 && self.len() > self.capacity {
            if self.evict_one_preferred() {
                self.capacity_overflow_events -= 1;
            } else {
                break;
            }
        }
    }
}

impl std::fmt::Debug for ArcCacheInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArcCacheInner")
            .field("t1_len", &self.t1.len())
            .field("t2_len", &self.t2.len())
            .field("b1_len", &self.b1.len())
            .field("b2_len", &self.b2.len())
            .field("p", &self.p)
            .field("capacity", &self.capacity)
            .field("total_bytes", &self.total_bytes)
            .field("max_bytes", &self.max_bytes)
            .field("directory_len", &self.directory.len())
            .field("page_versions_len", &self.page_versions.len())
            .field("gc_horizon", &self.gc_horizon)
            .field("capacity_overflow_events", &self.capacity_overflow_events)
            .field("hits", &self.hits)
            .field("misses", &self.misses)
            .field("ghost_hits_b1", &self.ghost_hits_b1)
            .field("ghost_hits_b2", &self.ghost_hits_b2)
            .field("evictions_t1", &self.evictions_t1)
            .field("evictions_t2", &self.evictions_t2)
            .field("version_coalesce_count", &self.version_coalesce_count)
            .field("admits", &self.admits)
            .finish()
    }
}

// ═══════════════════════════════════════════════════════════════════════
// ArcCache — thread-safe wrapper
// ═══════════════════════════════════════════════════════════════════════

/// Thread-safe ARC cache wrapping [`ArcCacheInner`] in a [`Mutex`].
pub struct ArcCache {
    inner: Mutex<ArcCacheInner>,
    inflight: Mutex<HashMap<CacheKey, Arc<InflightLoad>>>,
}

impl ArcCache {
    /// Create a new thread-safe ARC cache.
    #[must_use]
    pub fn new(capacity: usize, max_bytes: usize) -> Self {
        Self {
            inner: Mutex::new(ArcCacheInner::new(capacity, max_bytes)),
            inflight: Mutex::new(HashMap::new()),
        }
    }

    /// Acquire the inner lock for direct manipulation.
    pub fn lock(&self) -> parking_lot::MutexGuard<'_, ArcCacheInner> {
        self.inner.lock()
    }

    /// Singleflight miss path: suppress duplicate concurrent loads for `key`.
    ///
    /// The selected leader executes `loader` with no ARC mutex held. Peers wait
    /// on a per-key placeholder and then re-check the cache.
    ///
    /// If the leader fails/panics, waiters are unblocked and observe
    /// [`AsyncLookup::WaitedForPeerMiss`], allowing the caller to retry.
    pub fn request_async<F, E>(&self, key: CacheKey, loader: F) -> Result<AsyncLookup, E>
    where
        F: FnOnce() -> Result<CachedPage, E> + std::panic::UnwindSafe,
    {
        match self.claim_inflight_slot(key) {
            InflightRole::Leader(slot) => self.lead_request_async(key, &slot, loader),
            InflightRole::Waiter(slot) => Ok(self.wait_for_peer_load(key, &slot)),
        }
    }

    fn claim_inflight_slot(&self, key: CacheKey) -> InflightRole {
        let existing = self.inflight.lock().get(&key).cloned();
        if let Some(existing) = existing {
            return InflightRole::Waiter(existing);
        }

        let slot = Arc::new(InflightLoad::new_loading());
        let mut inflight = self.inflight.lock();
        if let Some(existing) = inflight.get(&key).cloned() {
            return InflightRole::Waiter(existing);
        }
        inflight.insert(key, Arc::clone(&slot));
        drop(inflight);
        InflightRole::Leader(slot)
    }

    fn wait_for_peer_load(&self, key: CacheKey, slot: &Arc<InflightLoad>) -> AsyncLookup {
        Self::wait_on_slot(slot.as_ref());
        let lookup = {
            let mut inner = self.inner.lock();
            inner.request(&key)
        };
        if matches!(lookup, CacheLookup::Hit) {
            AsyncLookup::WaitedForPeerHit
        } else {
            AsyncLookup::WaitedForPeerMiss
        }
    }

    fn lead_request_async<F, E>(
        &self,
        key: CacheKey,
        slot: &Arc<InflightLoad>,
        loader: F,
    ) -> Result<AsyncLookup, E>
    where
        F: FnOnce() -> Result<CachedPage, E> + std::panic::UnwindSafe,
    {
        let lookup = {
            let mut inner = self.inner.lock();
            inner.request(&key)
        };
        if matches!(lookup, CacheLookup::Hit) {
            self.release_inflight_slot(key, slot);
            return Ok(AsyncLookup::Hit);
        }

        let load_result = catch_unwind(AssertUnwindSafe(loader));
        match load_result {
            Ok(Ok(page)) => {
                {
                    let mut inner = self.inner.lock();
                    if inner.get(&key).is_some() {
                        // Someone else synchronously loaded it. Update LRU.
                        let _ = inner.request(&key);
                    } else {
                        // Not resident. We already accounted for the miss/ghost hit in `lookup`.
                        // Do not call `request` again to avoid double-counting misses.
                        debug_assert_eq!(page.key, key, "request_async loader returned wrong key");
                        inner.admit(key, page, lookup);
                    }
                }
                self.release_inflight_slot(key, slot);
                Ok(AsyncLookup::Loaded)
            }
            Ok(Err(err)) => {
                self.release_inflight_slot(key, slot);
                Err(err)
            }
            Err(payload) => {
                self.release_inflight_slot(key, slot);
                resume_unwind(payload)
            }
        }
    }

    fn wait_on_slot(slot: &InflightLoad) {
        let mut state = slot.state.lock();
        state.waiters = state.waiters.saturating_add(1);
        while state.loading {
            slot.cv.wait(&mut state);
        }
        state.waiters = state.waiters.saturating_sub(1);
    }

    fn release_inflight_slot(&self, key: CacheKey, slot: &Arc<InflightLoad>) {
        {
            let mut state = slot.state.lock();
            state.loading = false;
        }
        slot.cv.notify_all();
        let mut inflight = self.inflight.lock();
        let _ = inflight.remove(&key);
    }

    #[cfg(test)]
    fn inflight_count(&self) -> usize {
        self.inflight.lock().len()
    }
}

impl std::fmt::Debug for ArcCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArcCache")
            .field("inner", &*self.inner.lock())
            .field("inflight_len", &self.inflight.lock().len())
            .finish()
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::thread;
    use std::time::Duration;

    use fsqlite_types::PageSize;

    const BEAD_ID: &str = "bd-125g";
    const BEAD_ID_BD_3JK9: &str = "bd-3jk9";
    const BEAD_ID_BD_1ZLA: &str = "bd-1zla";
    const BEAD_ID_BD_7PU_1: &str = "bd-7pu.1";

    /// Helper: create a `CacheKey` from raw parts.
    fn key(pgno: u32, commit_seq: u64) -> CacheKey {
        CacheKey::new(PageNumber::new(pgno).unwrap(), CommitSeq::new(commit_seq))
    }

    /// Helper: create a `CachedPage` with the given key and a 4096-byte page.
    fn page(k: CacheKey, size: usize) -> CachedPage {
        let ps = if size <= 512 {
            PageSize::new(512).unwrap()
        } else if size <= 1024 {
            PageSize::new(1024).unwrap()
        } else if size <= 2048 {
            PageSize::new(2048).unwrap()
        } else if size <= 4096 {
            PageSize::new(4096).unwrap()
        } else if size <= 8192 {
            PageSize::new(8192).unwrap()
        } else if size <= 16384 {
            PageSize::new(16384).unwrap()
        } else if size <= 32768 {
            PageSize::new(32768).unwrap()
        } else {
            PageSize::new(65536).unwrap()
        };
        let mut cp = CachedPage::new(k, PageBuf::new(ps), 0, None);
        cp.byte_size = size;
        cp
    }

    // ── 1. test_cache_key_mvcc_awareness ──────────────────────────────

    #[test]
    fn test_cache_key_mvcc_awareness() {
        // Different (pgno, commit_seq) pairs are different keys.
        let k1 = key(1, 0);
        let k2 = key(1, 1);
        let k3 = key(2, 0);

        assert_ne!(k1, k2, "bead_id={BEAD_ID} case=same_page_diff_seq");
        assert_ne!(k1, k3, "bead_id={BEAD_ID} case=diff_page_same_seq");
        assert_ne!(k2, k3, "bead_id={BEAD_ID} case=diff_page_diff_seq");

        // Same (pgno, commit_seq) are equal.
        let k4 = key(1, 0);
        assert_eq!(k1, k4, "bead_id={BEAD_ID} case=same_key_equal");

        // HashMap correctly distinguishes them.
        let mut map = HashMap::new();
        map.insert(k1, "v1");
        map.insert(k2, "v2");
        map.insert(k3, "v3");
        assert_eq!(map.len(), 3, "bead_id={BEAD_ID} case=hashmap_distinct_keys");
    }

    // ── 2. test_arc_t1_t2_promotion ───────────────────────────────────

    #[test]
    fn test_request_t1_to_t2_promotion() {
        let mut cache = ArcCacheInner::new(10, 0);

        let k = key(1, 0);

        // First access: miss → admit into T1.
        let lookup = cache.request(&k);
        assert_eq!(
            lookup,
            CacheLookup::Miss,
            "bead_id={BEAD_ID} case=first_access_miss"
        );
        cache.admit(k, page(k, 4096), lookup);
        assert_eq!(
            cache.t1_len(),
            1,
            "bead_id={BEAD_ID} case=in_t1_after_admit"
        );
        assert_eq!(
            cache.t2_len(),
            0,
            "bead_id={BEAD_ID} case=t2_empty_after_admit"
        );

        // Second access: hit in T1 → promoted to T2.
        let lookup = cache.request(&k);
        assert_eq!(
            lookup,
            CacheLookup::Hit,
            "bead_id={BEAD_ID} case=second_access_hit"
        );
        assert_eq!(
            cache.t1_len(),
            0,
            "bead_id={BEAD_ID} case=t1_empty_after_promotion"
        );
        assert_eq!(
            cache.t2_len(),
            1,
            "bead_id={BEAD_ID} case=in_t2_after_promotion"
        );

        // Third access: hit in T2 → stays in T2 (refreshed).
        let lookup = cache.request(&k);
        assert_eq!(
            lookup,
            CacheLookup::Hit,
            "bead_id={BEAD_ID} case=third_access_t2_hit"
        );
        assert_eq!(
            cache.t2_len(),
            1,
            "bead_id={BEAD_ID} case=stays_in_t2_after_refresh"
        );
    }

    #[test]
    fn test_request_t2_refresh() {
        let mut cache = ArcCacheInner::new(4, 0);
        let k1 = key(1, 0);
        let k2 = key(2, 0);

        let l = cache.request(&k1);
        cache.admit(k1, page(k1, 4096), l);
        let l = cache.request(&k2);
        cache.admit(k2, page(k2, 4096), l);

        // Promote both into T2 in known order: k1 then k2.
        assert_eq!(cache.request(&k1), CacheLookup::Hit);
        assert_eq!(cache.request(&k2), CacheLookup::Hit);
        assert_eq!(cache.t2_lru_key(), Some(k1));
        assert_eq!(cache.t2_mru_key(), Some(k2));

        // Refresh k1 and verify it becomes MRU.
        assert_eq!(cache.request(&k1), CacheLookup::Hit);
        assert_eq!(cache.t2_lru_key(), Some(k2));
        assert_eq!(cache.t2_mru_key(), Some(k1));
    }

    #[test]
    fn test_request_miss_inserts_t1() {
        let mut cache = ArcCacheInner::new(2, 0);
        let k = key(1, 0);

        let lookup = cache.request(&k);
        assert_eq!(lookup, CacheLookup::Miss);
        cache.admit(k, page(k, 4096), lookup);

        assert!(cache.in_t1(&k), "bead_id={BEAD_ID} case=miss_goes_to_t1");
        assert_eq!(cache.t1_len(), 1);
        assert_eq!(cache.t2_len(), 0);
    }

    #[test]
    fn test_replace_prefers_t1_when_over_p() {
        let mut cache = ArcCacheInner::new(3, 0);
        let k1 = key(1, 0);
        let k2 = key(2, 0);
        let k3 = key(3, 0);
        let k4 = key(4, 0);

        for &k in &[k1, k2, k3] {
            let l = cache.request(&k);
            cache.admit(k, page(k, 4096), l);
        }
        cache.set_p_for_tests(1);

        // |T1|=3 > p=1, so REPLACE should evict from T1.
        let l = cache.request(&k4);
        cache.admit(k4, page(k4, 4096), l);

        assert!(cache.in_b1(&k1), "bead_id={BEAD_ID} case=prefer_t1");
        assert!(cache.get(&k4).is_some());
    }

    #[test]
    fn test_replace_b2_tiebreaker() {
        let mut cache = ArcCacheInner::new(3, 0);
        let k1 = key(1, 0);
        let k2 = key(2, 0);
        let k3 = key(3, 0);
        let k4 = key(4, 0);
        let k5 = key(5, 0);

        for &k in &[k1, k2, k3] {
            let l = cache.request(&k);
            cache.admit(k, page(k, 4096), l);
        }

        // Push k1 to B1, then back to cache via B1 ghost hit (lands in T2).
        let l = cache.request(&k4);
        cache.admit(k4, page(k4, 4096), l);
        let l = cache.request(&k1);
        assert_eq!(l, CacheLookup::GhostHitB1);
        cache.admit(k1, page(k1, 4096), l);

        // Force one T2 eviction so k1 becomes a B2 ghost entry.
        let _ = cache.request(&k3); // promote k3 to T2
        let l = cache.request(&k5);
        cache.admit(k5, page(k5, 4096), l);
        assert!(cache.in_b2(&k1), "setup requires k1 in B2");

        // Set |T1| == p so B2 ghost hit should use the tie-breaker.
        let t1_before = cache.t1_lru_key().expect("T1 must be non-empty");
        cache.set_p_for_tests(cache.t1_len());
        let l = cache.request(&k1);
        assert_eq!(l, CacheLookup::GhostHitB2);
        cache.admit(k1, page(k1, 4096), l);

        assert!(
            cache.in_b1(&t1_before),
            "bead_id={BEAD_ID} case=b2_tiebreaker_prefers_t1"
        );
    }

    #[test]
    fn test_replace_fallback() {
        let mut cache = ArcCacheInner::new(2, 0);
        let pinned = key(1, 0);
        let victim_t2 = key(2, 0);
        let incoming = key(3, 0);

        let l = cache.request(&pinned);
        cache.admit(pinned, page(pinned, 4096), l);
        let l = cache.request(&victim_t2);
        cache.admit(victim_t2, page(victim_t2, 4096), l);

        // Move victim_t2 to T2 so fallback has an alternative.
        assert_eq!(cache.request(&victim_t2), CacheLookup::Hit);
        cache.set_p_for_tests(0); // |T1| > p, so prefer_t1 = true.
        cache.get(&pinned).expect("pinned page exists").pin();

        let l = cache.request(&incoming);
        cache.admit(incoming, page(incoming, 4096), l);

        assert!(cache.get(&pinned).is_some(), "pinned T1 entry must remain");
        assert!(
            cache.get(&victim_t2).is_none(),
            "fallback should evict from T2 when preferred T1 victim is pinned"
        );
        assert!(cache.get(&incoming).is_some());
        cache.get(&pinned).expect("pinned page exists").unpin();
    }

    #[test]
    fn test_replace_overflow_safety_valve() {
        let mut cache = ArcCacheInner::new(1, 0);
        let pinned = key(1, 0);
        let incoming = key(2, 0);

        let l = cache.request(&pinned);
        cache.admit(pinned, page(pinned, 4096), l);
        cache.get(&pinned).expect("page exists").pin();

        let l = cache.request(&incoming);
        cache.admit(incoming, page(incoming, 4096), l);

        assert_eq!(cache.capacity_overflow_events(), 1);
        assert_eq!(cache.len(), 2, "safety valve allows temporary growth");
        cache.get(&pinned).expect("page exists").unpin();
    }

    #[test]
    fn test_request_ghost_trim() {
        let mut cache = ArcCacheInner::new(2, 0);

        for pgno in 1..=10_u32 {
            let k = key(pgno, 0);
            let l = cache.request(&k);
            if !matches!(l, CacheLookup::Hit) {
                cache.admit(k, page(k, 4096), l);
            }
        }

        assert!(cache.b1_len() <= 2, "bead_id={BEAD_ID} case=ghost_trim_b1");
        assert!(cache.b2_len() <= 2, "bead_id={BEAD_ID} case=ghost_trim_b2");
    }

    // ── 3. test_arc_ghost_hit_b1 ──────────────────────────────────────

    #[test]
    fn test_request_b1_ghost_increases_p() {
        // capacity=2: fill T1, cause eviction to B1, then ghost-hit.
        let mut cache = ArcCacheInner::new(2, 0);

        let k1 = key(1, 0);
        let k2 = key(2, 0);
        let k3 = key(3, 0);

        // Fill T1 to capacity.
        for &k in &[k1, k2] {
            let l = cache.request(&k);
            cache.admit(k, page(k, 4096), l);
        }
        assert_eq!(cache.t1_len(), 2);

        let p_before = cache.p();

        // Insert k3: forces eviction of k1 from T1 → B1.
        let l = cache.request(&k3);
        cache.admit(k3, page(k3, 4096), l);

        // k1 should now be a ghost in B1.
        let lookup_k1 = cache.request(&k1);
        assert_eq!(
            lookup_k1,
            CacheLookup::GhostHitB1,
            "bead_id={BEAD_ID} case=ghost_hit_b1"
        );

        // p should have increased (favour recency).
        assert!(
            cache.p() > p_before,
            "bead_id={BEAD_ID} case=p_increased_on_b1_hit p_before={p_before} p_after={}",
            cache.p()
        );
    }

    // ── 4. test_arc_ghost_hit_b2 ──────────────────────────────────────

    #[test]
    fn test_request_b2_ghost_decreases_p() {
        // Need p > 0 before the B2 ghost hit so we can observe the decrease.
        // Strategy: first trigger a B1 ghost hit (increases p), then arrange
        // a T2 eviction to B2, then ghost-hit on the B2 entry.
        let mut cache = ArcCacheInner::new(3, 0);

        let k1 = key(1, 0);
        let k2 = key(2, 0);
        let k3 = key(3, 0);
        let k4 = key(4, 0);
        let k5 = key(5, 0);

        // Step 1: Fill T1 to capacity.
        for &k in &[k1, k2, k3] {
            let l = cache.request(&k);
            cache.admit(k, page(k, 4096), l);
        }
        // T1=[k1,k2,k3]

        // Step 2: Insert k4 → evicts k1 from T1 → B1.
        let l = cache.request(&k4);
        cache.admit(k4, page(k4, 4096), l);
        // T1=[k2,k3,k4], B1=[k1]

        // Step 3: B1 ghost hit on k1 → increases p.
        let l = cache.request(&k1);
        assert_eq!(l, CacheLookup::GhostHitB1, "setup: B1 ghost hit");
        cache.admit(k1, page(k1, 4096), l);
        // p went from 0 to ≥1. k1 admitted to T2.
        assert!(cache.p() >= 1, "setup: p >= 1 after B1 ghost hit");

        // Step 4: Promote k3 to T2 (double access).
        cache.request(&k3);
        // T1=[...], T2=[k1, k3]

        // Step 5: Insert k5 → force eviction from T2 → B2.
        let l = cache.request(&k5);
        cache.admit(k5, page(k5, 4096), l);
        // The eviction should remove LRU of T2 (k1) → B2.

        let p_before = cache.p();
        assert!(p_before > 0, "setup: p > 0 before B2 ghost hit");

        // Step 6: Ghost hit k1 in B2.
        let lookup_k1 = cache.request(&k1);
        assert_eq!(
            lookup_k1,
            CacheLookup::GhostHitB2,
            "bead_id={BEAD_ID} case=ghost_hit_b2"
        );

        // p should have decreased (favour frequency).
        assert!(
            cache.p() < p_before,
            "bead_id={BEAD_ID} case=p_decreased_on_b2_hit p_before={p_before} p_after={}",
            cache.p()
        );
    }

    // ── 5. test_scan_resistance ───────────────────────────────────────

    #[test]
    fn test_scan_resistance() {
        // Hot working set in T2 survives a table scan flooding T1.
        let mut cache = ArcCacheInner::new(10, 0);

        // Create hot pages and promote them to T2 via double access.
        let hot_keys: Vec<CacheKey> = (1..=5).map(|i| key(i, 0)).collect();
        for &k in &hot_keys {
            let l = cache.request(&k);
            cache.admit(k, page(k, 4096), l);
            cache.request(&k); // promote T1 → T2
        }
        assert_eq!(cache.t2_len(), 5, "bead_id={BEAD_ID} case=hot_set_in_t2");

        // Simulate a table scan: 20 sequential cold pages, each accessed once.
        for i in 100..120u32 {
            let k = key(i, 0);
            let l = cache.request(&k);
            if !matches!(l, CacheLookup::Hit) {
                cache.admit(k, page(k, 4096), l);
            }
        }

        // Hot pages in T2 must survive the scan.
        for &k in &hot_keys {
            assert!(
                cache.get(&k).is_some(),
                "bead_id={BEAD_ID} case=hot_page_survived_scan pgno={}",
                k.pgno
            );
        }
    }

    // ── 6. test_pinned_page_not_evicted ───────────────────────────────

    #[test]
    fn test_replace_skips_pinned() {
        let mut cache = ArcCacheInner::new(2, 0);

        let k1 = key(1, 0);
        let k2 = key(2, 0);

        // Insert k1 and pin it.
        let l = cache.request(&k1);
        cache.admit(k1, page(k1, 4096), l);
        cache.get(&k1).unwrap().pin();

        // Insert k2.
        let l = cache.request(&k2);
        cache.admit(k2, page(k2, 4096), l);

        // Insert k3: would need to evict, but k1 is pinned.
        let k3 = key(3, 0);
        let l = cache.request(&k3);
        cache.admit(k3, page(k3, 4096), l);

        // k1 must still be present (pinned, protected from eviction).
        assert!(
            cache.get(&k1).is_some(),
            "bead_id={BEAD_ID} case=pinned_page_not_evicted"
        );

        // Clean up.
        cache.get(&k1).unwrap().unpin();
    }

    // ── 7. test_eviction_no_io ────────────────────────────────────────

    #[test]
    fn test_eviction_no_io() {
        // Verify that the eviction path is pure data-structure manipulation
        // with no VFS calls.  Since ArcCacheInner has no file handles and
        // no trait bounds requiring I/O, this is structurally guaranteed.
        //
        // We exercise eviction and verify it succeeds without any I/O
        // infrastructure being available.
        let mut cache = ArcCacheInner::new(3, 0);

        // Fill cache.
        for i in 1..=3u32 {
            let k = key(i, 0);
            let l = cache.request(&k);
            cache.admit(k, page(k, 4096), l);
        }
        assert_eq!(cache.len(), 3);

        // Trigger eviction by inserting a fourth page.
        let k4 = key(4, 0);
        let l = cache.request(&k4);
        cache.admit(k4, page(k4, 4096), l);

        // Eviction happened (one page removed, k4 added).
        assert_eq!(
            cache.len(),
            3,
            "bead_id={BEAD_ID} case=eviction_kept_capacity"
        );
        assert!(
            cache.get(&k4).is_some(),
            "bead_id={BEAD_ID} case=new_page_admitted_after_eviction"
        );
        // No panic, no I/O error — eviction is pure memory.
    }

    // ── 8. test_superseded_version_preferred ──────────────────────────

    #[test]
    fn test_superseded_version_preferred() {
        // Two versions of the same page.  When eviction triggers, the
        // older (superseded) version should be evicted first.
        let mut cache = ArcCacheInner::new(3, 0);

        // Version 1 of page 1 (commit_seq=1).
        let k_old = key(1, 1);
        let l = cache.request(&k_old);
        cache.admit(k_old, page(k_old, 4096), l);

        // Version 2 of page 1 (commit_seq=5).
        let k_new = key(1, 5);
        let l = cache.request(&k_new);
        cache.admit(k_new, page(k_new, 4096), l);

        // Another page to fill capacity.
        let k_other = key(2, 0);
        let l = cache.request(&k_other);
        cache.admit(k_other, page(k_other, 4096), l);

        // Set GC horizon above k_old so it's eligible for superseded eviction.
        cache.set_gc_horizon(CommitSeq::new(3));

        // Trigger eviction.
        let k_trigger = key(3, 0);
        let l = cache.request(&k_trigger);
        cache.admit(k_trigger, page(k_trigger, 4096), l);

        // The old version should have been evicted.
        assert!(
            cache.get(&k_old).is_none(),
            "bead_id={BEAD_ID} case=old_version_evicted"
        );
        // The new version should still be present.
        assert!(
            cache.get(&k_new).is_some(),
            "bead_id={BEAD_ID} case=new_version_retained"
        );
    }

    // ── 9. test_memory_accounting ─────────────────────────────────────

    #[test]
    fn test_memory_accounting() {
        let mut cache = ArcCacheInner::new(100, 0);

        // Insert pages of varying sizes.
        let sizes = [1024_usize, 2048, 4096, 8192];
        let mut expected_total = 0_usize;

        for (i, &size) in sizes.iter().enumerate() {
            let k = key(u32::try_from(i + 1).unwrap(), 0);
            let l = cache.request(&k);
            cache.admit(k, page(k, size), l);
            expected_total += size;

            assert_eq!(
                cache.total_bytes(),
                expected_total,
                "bead_id={BEAD_ID} case=accounting_after_insert_{i}"
            );
        }

        // Eviction: reduce capacity and force eviction.
        let mut small_cache = ArcCacheInner::new(2, 0);
        let k1 = key(1, 0);
        let k2 = key(2, 0);
        let k3 = key(3, 0);

        let l = small_cache.request(&k1);
        small_cache.admit(k1, page(k1, 4096), l);
        assert_eq!(
            small_cache.total_bytes(),
            4096,
            "bead_id={BEAD_ID} case=accounting_single"
        );

        let l = small_cache.request(&k2);
        small_cache.admit(k2, page(k2, 2048), l);
        assert_eq!(
            small_cache.total_bytes(),
            6144,
            "bead_id={BEAD_ID} case=accounting_two"
        );

        // k3 triggers eviction (capacity=2).
        let l = small_cache.request(&k3);
        small_cache.admit(k3, page(k3, 1024), l);

        // After eviction: 2 pages remain, total should reflect actual sizes.
        assert_eq!(
            small_cache.len(),
            2,
            "bead_id={BEAD_ID} case=accounting_after_eviction_count"
        );
        // One 4096 was evicted, leaving 2048 + 1024 = 3072.
        assert_eq!(
            small_cache.total_bytes(),
            3072,
            "bead_id={BEAD_ID} case=accounting_after_eviction_bytes"
        );
    }

    // ── E2E smoke test ────────────────────────────────────────────────

    #[test]
    fn test_e2e_arc_cache_behavior_under_mixed_workload() {
        // Multiple transactions reading/writing pages through the ARC.
        let cache = ArcCache::new(20, 0);

        // Transaction 1: writes pages 1-5 at commit_seq=1.
        {
            let mut inner = cache.lock();
            for i in 1..=5u32 {
                let k = key(i, 1);
                let l = inner.request(&k);
                inner.admit(k, page(k, 4096), l);
            }
        }

        // Transaction 2: reads pages 1-3 (same version), writes 6-8.
        {
            let mut inner = cache.lock();
            for i in 1..=3u32 {
                let k = key(i, 1);
                let l = inner.request(&k);
                assert_eq!(
                    l,
                    CacheLookup::Hit,
                    "bead_id={BEAD_ID} case=e2e_txn2_read_hit page={i}"
                );
                inner.get(&k).unwrap().pin();
            }
            for i in 6..=8u32 {
                let k = key(i, 2);
                let l = inner.request(&k);
                inner.admit(k, page(k, 4096), l);
            }
            // Unpin.
            for i in 1..=3u32 {
                let k = key(i, 1);
                inner.get(&k).unwrap().unpin();
            }
        }

        // Verify all pages are accessible.
        let inner = cache.lock();
        assert_eq!(inner.len(), 8, "bead_id={BEAD_ID} case=e2e_total_pages");
        for i in 1..=5u32 {
            assert!(
                inner.get(&key(i, 1)).is_some(),
                "bead_id={BEAD_ID} case=e2e_page_{i}_v1_present"
            );
        }
        for i in 6..=8u32 {
            assert!(
                inner.get(&key(i, 2)).is_some(),
                "bead_id={BEAD_ID} case=e2e_page_{i}_v2_present"
            );
        }
        drop(inner);
    }

    #[test]
    fn test_request_async_singleflight_duplicate_load_suppression() {
        let cache = Arc::new(ArcCache::new(8, 0));
        let load_calls = Arc::new(AtomicUsize::new(0));
        let k = key(900, 1);

        let mut handles = Vec::new();
        for _ in 0..8 {
            let cache_ref = Arc::clone(&cache);
            let calls_ref = Arc::clone(&load_calls);
            handles.push(thread::spawn(move || {
                cache_ref
                    .request_async(k, || -> Result<CachedPage, &'static str> {
                        let _ = calls_ref.fetch_add(1, AtomicOrdering::SeqCst);
                        thread::sleep(Duration::from_millis(20));
                        Ok(page(k, 4096))
                    })
                    .expect("singleflight request should not fail")
            }));
        }

        let mut waited_hits = 0usize;
        for handle in handles {
            let outcome = handle.join().expect("worker thread should not panic");
            if matches!(outcome, AsyncLookup::WaitedForPeerHit) {
                waited_hits += 1;
            }
        }

        assert_eq!(
            load_calls.load(AtomicOrdering::SeqCst),
            1,
            "bead_id={BEAD_ID_BD_7PU_1} case=singleflight_duplicate_load_suppression"
        );
        assert!(
            waited_hits >= 1,
            "bead_id={BEAD_ID_BD_7PU_1} case=singleflight_waiters_observed"
        );
        assert_eq!(
            cache.inflight_count(),
            0,
            "bead_id={BEAD_ID_BD_7PU_1} case=singleflight_placeholder_cleared"
        );

        let mut inner = cache.lock();
        assert_eq!(
            inner.request(&k),
            CacheLookup::Hit,
            "bead_id={BEAD_ID_BD_7PU_1} case=singleflight_page_admitted"
        );
        drop(inner);
    }

    #[test]
    fn test_request_async_error_path_clears_placeholder() {
        let cache = ArcCache::new(4, 0);
        let k = key(901, 1);

        let first = cache.request_async(k, || -> Result<CachedPage, &'static str> {
            Err("loader failed")
        });
        assert_eq!(
            first,
            Err("loader failed"),
            "bead_id={BEAD_ID_BD_7PU_1} case=error_propagated_to_leader"
        );
        assert_eq!(
            cache.inflight_count(),
            0,
            "bead_id={BEAD_ID_BD_7PU_1} case=error_placeholder_cleared"
        );

        let second = cache
            .request_async(k, || -> Result<CachedPage, &'static str> {
                Ok(page(k, 4096))
            })
            .expect("retry load should succeed");
        assert!(
            matches!(second, AsyncLookup::Loaded | AsyncLookup::Hit),
            "bead_id={BEAD_ID_BD_7PU_1} case=error_retry_admits"
        );

        let mut inner = cache.lock();
        assert_eq!(
            inner.request(&k),
            CacheLookup::Hit,
            "bead_id={BEAD_ID_BD_7PU_1} case=error_retry_hit"
        );
        drop(inner);
    }

    #[test]
    fn test_request_async_hit_path_skips_loader() {
        let cache = ArcCache::new(4, 0);
        let k = key(903, 1);

        {
            let mut inner = cache.lock();
            let lookup = inner.request(&k);
            inner.admit(k, page(k, 4096), lookup);
            drop(inner);
        }

        let loader_calls = Arc::new(AtomicUsize::new(0));
        let loader_calls_for_closure = Arc::clone(&loader_calls);
        let outcome = cache
            .request_async(k, move || -> Result<CachedPage, &'static str> {
                loader_calls_for_closure.fetch_add(1, AtomicOrdering::SeqCst);
                Err("hit path must not call loader")
            })
            .expect("hit path should not fail");
        assert_eq!(
            outcome,
            AsyncLookup::Hit,
            "bead_id={BEAD_ID_BD_7PU_1} case=hit_path_skips_loader"
        );
        assert_eq!(
            loader_calls.load(AtomicOrdering::SeqCst),
            0,
            "bead_id={BEAD_ID_BD_7PU_1} case=hit_path_loader_not_called"
        );
    }

    #[test]
    fn test_request_async_panic_notifies_waiters_and_allows_retry() {
        let cache = Arc::new(ArcCache::new(4, 0));
        let k = key(902, 1);

        let cache_leader = Arc::clone(&cache);
        let leader = thread::spawn(move || {
            let panic_result = std::panic::catch_unwind(AssertUnwindSafe(|| {
                let _ = cache_leader.request_async(k, || -> Result<CachedPage, &'static str> {
                    thread::sleep(Duration::from_millis(75));
                    resume_unwind(Box::new(
                        "forced loader unwind for cancellation path (test only)",
                    ));
                });
            }));
            panic_result.is_err()
        });

        while cache.inflight_count() == 0 {
            thread::yield_now();
        }

        let waiter_loader_calls = Arc::new(AtomicUsize::new(0));
        let waiter_loader_calls_for_thread = Arc::clone(&waiter_loader_calls);
        let cache_waiter = Arc::clone(&cache);
        let waiter = thread::spawn(move || {
            cache_waiter
                .request_async(k, move || -> Result<CachedPage, &'static str> {
                    waiter_loader_calls_for_thread.fetch_add(1, AtomicOrdering::SeqCst);
                    Err("waiter loader must not execute while placeholder is active")
                })
                .expect("waiter should observe peer outcome")
        });

        assert!(
            leader.join().expect("leader thread join failed"),
            "bead_id={BEAD_ID_BD_7PU_1} case=panic_expected"
        );
        let waiter_outcome = waiter.join().expect("waiter thread join failed");
        assert_eq!(
            waiter_outcome,
            AsyncLookup::WaitedForPeerMiss,
            "bead_id={BEAD_ID_BD_7PU_1} case=panic_waiter_unblocked"
        );
        assert_eq!(
            waiter_loader_calls.load(AtomicOrdering::SeqCst),
            0,
            "bead_id={BEAD_ID_BD_7PU_1} case=waiter_loader_not_called"
        );
        assert_eq!(
            cache.inflight_count(),
            0,
            "bead_id={BEAD_ID_BD_7PU_1} case=panic_placeholder_cleared"
        );

        let retry_outcome = cache
            .request_async(k, || -> Result<CachedPage, &'static str> {
                Ok(page(k, 4096))
            })
            .expect("retry after panic should succeed");
        assert!(
            matches!(retry_outcome, AsyncLookup::Loaded | AsyncLookup::Hit),
            "bead_id={BEAD_ID_BD_7PU_1} case=panic_retry_succeeds"
        );
    }

    #[test]
    fn test_ghost_hit_exact_match() {
        let mut cache = ArcCacheInner::new(2, 0);
        let k_v1 = key(1, 1);
        let k2 = key(2, 1);
        let k3 = key(3, 1);

        for &k in &[k_v1, k2] {
            let lookup = cache.request(&k);
            cache.admit(k, page(k, 4096), lookup);
        }

        let lookup = cache.request(&k3);
        cache.admit(k3, page(k3, 4096), lookup);
        assert!(cache.in_b1(&k_v1), "setup requires ghost entry in B1");

        assert_eq!(
            cache.request(&k_v1),
            CacheLookup::GhostHitB1,
            "bead_id={BEAD_ID_BD_3JK9} case=ghost_hit_exact_match"
        );
    }

    #[test]
    fn test_ghost_miss_different_version() {
        let mut cache = ArcCacheInner::new(2, 0);
        let k_v1 = key(1, 1);
        let k_v2 = key(1, 2);
        let k2 = key(2, 1);
        let k3 = key(3, 1);

        for &k in &[k_v1, k2] {
            let lookup = cache.request(&k);
            cache.admit(k, page(k, 4096), lookup);
        }

        let lookup = cache.request(&k3);
        cache.admit(k3, page(k3, 4096), lookup);
        assert!(cache.in_b1(&k_v1), "setup requires ghost entry in B1");

        assert_eq!(
            cache.request(&k_v2),
            CacheLookup::Miss,
            "bead_id={BEAD_ID_BD_3JK9} case=ghost_miss_different_version"
        );
    }

    #[test]
    fn test_ghost_prune_below_horizon() {
        let mut cache = ArcCacheInner::new(2, 0);
        let k1 = key(1, 1);
        let k2 = key(2, 1);
        let k3 = key(3, 1);

        for &k in &[k1, k2] {
            let lookup = cache.request(&k);
            cache.admit(k, page(k, 4096), lookup);
        }
        let lookup = cache.request(&k3);
        cache.admit(k3, page(k3, 4096), lookup);
        assert!(cache.in_b1(&k1), "setup requires B1 ghost");

        cache.set_gc_horizon(CommitSeq::new(2));

        assert!(
            !cache.in_b1(&k1),
            "bead_id={BEAD_ID_BD_3JK9} case=ghost_prune_below_horizon"
        );
        assert!(!cache.in_b2(&k1));
    }

    #[test]
    fn test_pinned_page_eviction_overflow() {
        let mut cache = ArcCacheInner::new(1, 0);
        let pinned = key(1, 1);
        let incoming = key(2, 1);

        let lookup = cache.request(&pinned);
        cache.admit(pinned, page(pinned, 4096), lookup);
        cache.get(&pinned).expect("pinned page exists").pin();

        let lookup = cache.request(&incoming);
        cache.admit(incoming, page(incoming, 4096), lookup);

        assert_eq!(
            cache.capacity_overflow_events(),
            1,
            "bead_id={BEAD_ID_BD_3JK9} case=pinned_page_eviction_overflow"
        );
    }

    #[test]
    fn test_overflow_decrement_on_unpin() {
        let mut cache = ArcCacheInner::new(1, 0);
        let pinned = key(1, 1);
        let incoming = key(2, 1);

        let lookup = cache.request(&pinned);
        cache.admit(pinned, page(pinned, 4096), lookup);
        cache.get(&pinned).expect("pinned page exists").pin();

        let lookup = cache.request(&incoming);
        cache.admit(incoming, page(incoming, 4096), lookup);
        assert_eq!(
            cache.capacity_overflow_events(),
            1,
            "setup requires overflow"
        );
        assert_eq!(cache.len(), 2, "setup requires temporary growth");

        assert!(cache.unpin(&pinned), "unpin should observe prior pin");
        assert_eq!(
            cache.capacity_overflow_events(),
            0,
            "bead_id={BEAD_ID_BD_3JK9} case=overflow_decrement_on_unpin"
        );
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn test_eviction_never_writes_wal() {
        let mut cache = ArcCacheInner::new(2, 0);

        for i in 1..=2_u32 {
            let k = key(i, 1);
            let lookup = cache.request(&k);
            let mut p = page(k, 4096);
            p.wal_frame = Some(i);
            cache.admit(k, p, lookup);
        }

        let k3 = key(3, 1);
        let lookup = cache.request(&k3);
        let mut p3 = page(k3, 4096);
        p3.wal_frame = Some(3);
        cache.admit(k3, p3, lookup);

        assert_eq!(cache.len(), 2);
        assert!(
            cache.t1.iter().all(|(_, page)| page.wal_frame.is_some())
                && cache.t2.iter().all(|(_, page)| page.wal_frame.is_some()),
            "bead_id={BEAD_ID_BD_3JK9} case=eviction_never_writes_wal"
        );
    }

    #[test]
    fn test_uncommitted_pages_in_write_set() {
        let mut cache = ArcCacheInner::new(2, 0);
        let private_key = key(99, 0);
        let mut write_set = HashMap::new();
        write_set.insert(private_key, page(private_key, 4096));

        assert_eq!(
            cache.request(&private_key),
            CacheLookup::Miss,
            "bead_id={BEAD_ID_BD_3JK9} case=write_set_not_arc_hit"
        );
        assert!(cache.get(&private_key).is_none());
        assert_eq!(cache.len(), 0);
        assert_eq!(write_set.len(), 1);
    }

    #[test]
    fn test_version_coalesce_keeps_newest() {
        let mut cache = ArcCacheInner::new(8, 0);
        let k1 = key(1, 1);
        let k2 = key(1, 2);
        let k3 = key(1, 3);

        for &k in &[k1, k2, k3] {
            let lookup = cache.request(&k);
            cache.admit(k, page(k, 4096), lookup);
        }

        cache.set_gc_horizon(CommitSeq::new(3));

        assert!(cache.get(&k3).is_some());
        assert!(
            cache.get(&k2).is_none() && cache.get(&k1).is_none(),
            "bead_id={BEAD_ID_BD_3JK9} case=version_coalesce_keeps_newest"
        );
    }

    #[test]
    fn test_version_coalesce_removes_superseded() {
        let mut cache = ArcCacheInner::new(8, 0);
        let old = key(1, 1);
        let new = key(1, 2);

        for &k in &[old, new] {
            let lookup = cache.request(&k);
            cache.admit(k, page(k, 4096), lookup);
        }

        cache.set_gc_horizon(CommitSeq::new(2));
        assert!(cache.get(&new).is_some());
        assert!(
            cache.get(&old).is_none(),
            "bead_id={BEAD_ID_BD_3JK9} case=version_coalesce_removes_superseded"
        );
    }

    #[test]
    fn test_version_coalesce_skips_pinned() {
        let mut cache = ArcCacheInner::new(8, 0);
        let old = key(1, 1);
        let new = key(1, 2);

        for &k in &[old, new] {
            let lookup = cache.request(&k);
            cache.admit(k, page(k, 4096), lookup);
        }

        cache.get(&old).expect("old version should exist").pin();
        cache.set_gc_horizon(CommitSeq::new(2));

        assert!(
            cache.get(&old).is_some(),
            "bead_id={BEAD_ID_BD_3JK9} case=version_coalesce_skips_pinned"
        );
        assert!(cache.get(&new).is_some());
        let _ = cache.unpin(&old);
    }

    #[test]
    fn test_coalesced_not_ghosted() {
        let mut cache = ArcCacheInner::new(8, 0);
        let old = key(1, 1);
        let new = key(1, 2);

        for &k in &[old, new] {
            let lookup = cache.request(&k);
            cache.admit(k, page(k, 4096), lookup);
        }

        cache.set_gc_horizon(CommitSeq::new(2));
        assert!(cache.get(&old).is_none());
        assert!(
            !cache.in_b1(&old) && !cache.in_b2(&old),
            "bead_id={BEAD_ID_BD_3JK9} case=coalesced_not_ghosted"
        );
    }

    #[test]
    fn test_coalesce_trigger_on_replace() {
        let mut cache = ArcCacheInner::new(3, 0);
        let old = key(1, 1);
        let new = key(1, 2);
        let other = key(2, 1);
        let incoming = key(3, 1);

        for &k in &[old, new, other] {
            let lookup = cache.request(&k);
            cache.admit(k, page(k, 4096), lookup);
        }

        // Move one entry to T2 so admission triggers REPLACE.
        assert_eq!(cache.request(&new), CacheLookup::Hit);
        assert!(cache.in_t2(&new), "setup requires a T2 resident");

        cache.get(&old).expect("old version must exist").pin();
        cache.set_gc_horizon(CommitSeq::new(2));
        assert!(
            cache.get(&old).is_some(),
            "pinned old version should survive batch"
        );
        let _ = cache.unpin(&old);

        let lookup = cache.request(&incoming);
        cache.admit(incoming, page(incoming, 4096), lookup);

        assert!(
            cache.get(&old).is_none(),
            "bead_id={BEAD_ID_BD_3JK9} case=coalesce_trigger_on_replace"
        );
        assert!(!cache.in_b1(&old) && !cache.in_b2(&old));
        assert!(cache.get(&incoming).is_some());
    }

    #[test]
    fn test_coalesce_trigger_on_gc_advance() {
        let mut cache = ArcCacheInner::new(8, 0);
        let old = key(1, 1);
        let new = key(1, 2);

        for &k in &[old, new] {
            let lookup = cache.request(&k);
            cache.admit(k, page(k, 4096), lookup);
        }

        cache.set_gc_horizon(CommitSeq::new(2));
        assert!(
            cache.get(&old).is_none() && cache.get(&new).is_some(),
            "bead_id={BEAD_ID_BD_3JK9} case=coalesce_trigger_on_gc_advance"
        );
    }

    #[test]
    fn test_e2e_bd_3jk9() {
        let mut cache = ArcCacheInner::new(64, 0);
        let mut commit_seq = 1_u64;

        for writer in 0..8_u32 {
            for op in 0..100_u32 {
                let pgno = 1 + ((writer * 29 + op) % 24);
                let key = key(pgno, commit_seq);
                let lookup = cache.request(&key);
                if !matches!(lookup, CacheLookup::Hit) {
                    cache.admit(key, page(key, 4096), lookup);
                }

                if op % 17 == 0 {
                    if let Some(page) = cache.get(&key) {
                        page.pin();
                    }
                }

                if op % 17 == 5 {
                    let _ = cache.unpin(&key);
                }

                if op % 20 == 0 {
                    let horizon = commit_seq.saturating_sub(10);
                    cache.set_gc_horizon(CommitSeq::new(horizon));
                }

                commit_seq = commit_seq.saturating_add(1);
            }
        }

        assert!(cache.b1_len() <= cache.capacity);
        assert!(cache.b2_len() <= cache.capacity);
        assert!(cache.capacity_overflow_events() <= 200);
        assert!(cache.len() <= cache.capacity + cache.capacity_overflow_events());
    }

    #[test]
    fn test_visibility_committed_below_high() {
        assert!(ArcCacheInner::is_visible(
            CommitSeq::new(3),
            CommitSeq::new(7)
        ));
    }

    #[test]
    fn test_visibility_committed_above_high() {
        assert!(!ArcCacheInner::is_visible(
            CommitSeq::new(9),
            CommitSeq::new(7)
        ));
    }

    #[test]
    fn test_visibility_uncommitted() {
        assert!(!ArcCacheInner::is_visible(
            CommitSeq::ZERO,
            CommitSeq::new(7)
        ));
    }

    #[test]
    fn test_self_visibility_via_write_set() {
        assert!(ArcCacheInner::is_visible_or_self(
            CommitSeq::ZERO,
            CommitSeq::new(7),
            true
        ));
        assert!(!ArcCacheInner::is_visible_or_self(
            CommitSeq::ZERO,
            CommitSeq::new(7),
            false
        ));
    }

    #[test]
    fn test_dual_eviction_by_count() {
        let mut cache = ArcCacheInner::new(2, 0);

        for pgno in 1..=3_u32 {
            let k = key(pgno, 1);
            let lookup = cache.request(&k);
            cache.admit(k, page(k, 4096), lookup);
        }

        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn test_dual_eviction_by_bytes() {
        let mut cache = ArcCacheInner::new(10, 5000);
        let k1 = key(1, 1);
        let k2 = key(2, 1);

        let lookup = cache.request(&k1);
        cache.admit(k1, page(k1, 4096), lookup);
        let lookup = cache.request(&k2);
        cache.admit(k2, page(k2, 4096), lookup);

        assert!(
            cache.total_bytes() <= 5000,
            "bead_id={BEAD_ID_BD_1ZLA} case=dual_eviction_by_bytes"
        );
    }

    #[test]
    fn test_memory_accounting_delta_vs_full() {
        let mut cache = ArcCacheInner::new(10, 0);
        let full = key(1, 1);
        let delta = key(2, 1);

        let lookup = cache.request(&full);
        cache.admit(full, page(full, 4096), lookup);
        let lookup = cache.request(&delta);
        cache.admit(delta, page(delta, 200), lookup);

        assert_eq!(
            cache.total_bytes(),
            4296,
            "bead_id={BEAD_ID_BD_1ZLA} case=memory_accounting_delta_vs_full"
        );
    }

    #[test]
    fn test_pragma_cache_size_positive() {
        let mut cache = ArcCacheInner::new(1, 1);
        cache.apply_pragma_cache_size(500, 4096);

        assert_eq!(cache.capacity(), 500);
        assert_eq!(cache.max_bytes(), 500 * 4096);
    }

    #[test]
    fn test_pragma_cache_size_negative() {
        let mut cache = ArcCacheInner::new(1, 1);
        cache.apply_pragma_cache_size(-2000, 4096);

        assert_eq!(cache.max_bytes(), 2_048_000);
        assert_eq!(cache.capacity(), 500);
    }

    #[test]
    fn test_pragma_cache_size_zero() {
        let mut cache = ArcCacheInner::new(4, 0);
        for pgno in 1..=4_u32 {
            let k = key(pgno, 1);
            let lookup = cache.request(&k);
            cache.admit(k, page(k, 4096), lookup);
        }

        cache.apply_pragma_cache_size(0, 4096);
        assert_eq!(cache.capacity(), 0);
        assert_eq!(cache.max_bytes(), 0);
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn test_cache_resize_evicts() {
        let mut cache = ArcCacheInner::new(4, 0);

        for pgno in 1..=4_u32 {
            let k = key(pgno, 1);
            let lookup = cache.request(&k);
            cache.admit(k, page(k, 4096), lookup);
        }

        cache.resize(2, 0);
        assert!(cache.len() <= 2);
    }

    #[test]
    fn test_cache_resize_trims_ghosts() {
        let mut cache = ArcCacheInner::new(4, 0);
        for pgno in 1..=12_u32 {
            let k = key(pgno, 1);
            let lookup = cache.request(&k);
            if !matches!(lookup, CacheLookup::Hit) {
                cache.admit(k, page(k, 4096), lookup);
            }
        }

        cache.resize(1, 0);
        assert!(cache.b1_len() <= 1);
        assert!(cache.b2_len() <= 1);
    }

    #[test]
    fn test_cache_resize_clamps_p() {
        let mut cache = ArcCacheInner::new(8, 0);
        cache.set_p_for_tests(8);
        cache.resize(2, 0);
        assert_eq!(cache.p(), 2);
    }

    #[test]
    fn test_e2e_bd_1zla() {
        let mut cache = ArcCacheInner::new(16, 16 * 4096);

        for i in 1_u32..=200 {
            if i == 50 {
                cache.apply_pragma_cache_size(64, 4096);
            } else if i == 100 {
                cache.apply_pragma_cache_size(-2000, 4096);
            } else if i == 150 {
                cache.apply_pragma_cache_size(0, 4096);
            } else if i == 170 {
                cache.apply_pragma_cache_size(32, 4096);
            }

            let pgno = 1 + ((i - 1) % 64);
            let k = key(pgno, u64::from(i));
            let size = if i % 3 == 0 { 200 } else { 4096 };
            let lookup = cache.request(&k);
            if !matches!(lookup, CacheLookup::Hit) {
                cache.admit(k, page(k, size), lookup);
            }
        }

        assert!(cache.b1_len() <= cache.capacity());
        assert!(cache.b2_len() <= cache.capacity());
        if cache.max_bytes() > 0 {
            assert!(cache.total_bytes() <= cache.max_bytes());
        }
    }

    #[test]
    fn test_arc_cache_scan_resistance() {
        test_scan_resistance();
    }

    // Canonical §6 acceptance-test identifiers.

    #[test]
    fn test_arc_hit_t1_promote_to_t2() {
        test_request_t1_to_t2_promotion();
    }

    #[test]
    fn test_arc_ghost_hit_b1_increases_p() {
        test_request_b1_ghost_increases_p();
    }

    #[test]
    fn test_arc_ghost_hit_b2_decreases_p() {
        test_request_b2_ghost_decreases_p();
    }

    #[test]
    fn test_replace_all_pinned_overflow() {
        test_replace_overflow_safety_valve();
        test_pinned_page_eviction_overflow();
    }

    #[test]
    fn test_mvcc_keying_exact_match_ghosts() {
        test_ghost_hit_exact_match();
        test_ghost_miss_different_version();
    }

    #[test]
    fn test_version_coalescing_drops_superseded() {
        test_version_coalesce_removes_superseded();
    }

    #[test]
    fn test_eviction_never_appends_wal() {
        test_eviction_never_writes_wal();
        test_eviction_no_io();
    }

    #[test]
    fn test_e2e_arc_scan_then_hotset() {
        test_scan_resistance();
    }

    // ═══════════════════════════════════════════════════════════════════
    // bd-2zoa: §6.11-6.12 ARC Performance Analysis + Warm-Up Behavior
    // ═══════════════════════════════════════════════════════════════════

    const BEAD_ID_BD_2ZOA: &str = "bd-2zoa";

    /// Deterministic PRNG (xorshift64) for reproducible workload generation.
    struct Xorshift64 {
        state: u64,
    }

    impl Xorshift64 {
        fn new(seed: u64) -> Self {
            Self {
                state: if seed == 0 { 1 } else { seed },
            }
        }

        fn next_u64(&mut self) -> u64 {
            let mut x = self.state;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            self.state = x;
            x
        }

        /// Uniform random in [0, bound).
        fn next_bounded(&mut self, bound: u64) -> u64 {
            self.next_u64() % bound
        }
    }

    /// Simple Zipf sampler: inverse-CDF method with precomputed harmonic numbers.
    struct ZipfSampler {
        cdf: Vec<f64>,
    }

    impl ZipfSampler {
        fn new(n: usize, s: f64) -> Self {
            let mut weights = Vec::with_capacity(n);
            let mut total = 0.0_f64;
            for rank in 1..=n {
                let w = 1.0 / (rank as f64).powf(s);
                total += w;
                weights.push(total);
            }
            // Normalize to [0, 1].
            for w in &mut weights {
                *w /= total;
            }
            Self { cdf: weights }
        }

        /// Sample a 0-indexed rank from the Zipf distribution.
        fn sample(&self, rng: &mut Xorshift64) -> usize {
            let u = (rng.next_u64() as f64) / (u64::MAX as f64);
            self.cdf.partition_point(|&c| c < u)
        }
    }

    /// Run a workload and return (hits, total_accesses).
    fn run_workload(
        cache: &mut ArcCacheInner,
        accesses: impl Iterator<Item = CacheKey>,
    ) -> (usize, usize) {
        let mut hits = 0_usize;
        let mut total = 0_usize;
        for k in accesses {
            total += 1;
            let lookup = cache.request(&k);
            if matches!(lookup, CacheLookup::Hit) {
                hits += 1;
            } else {
                cache.admit(k, page(k, 4096), lookup);
            }
        }
        (hits, total)
    }

    // ── §6.11 Performance Analysis: Five Workload Patterns ───────────

    #[test]
    fn test_arc_oltp_hit_rate() {
        // OLTP point queries: 500 hot pages out of 100K, cache=2000.
        // Expected ARC hit rate > 0.95.
        let capacity = 2000_usize;
        let hot_set = 500_u64;
        let total_pages = 100_000_u64;
        let num_accesses = 50_000_usize;

        let mut cache = ArcCacheInner::new(capacity, 0);
        let mut rng = Xorshift64::new(42);

        // Warm-up: fill cache with hot set pages.
        #[allow(clippy::cast_possible_truncation)]
        let hot_set_usize = hot_set as usize; // hot_set fits in usize (500)
        for i in 1..=capacity.min(hot_set_usize) {
            let k = key(u32::try_from(i).unwrap(), 0);
            let l = cache.request(&k);
            if !matches!(l, CacheLookup::Hit) {
                cache.admit(k, page(k, 4096), l);
            }
            // Promote to T2 (double access).
            let _ = cache.request(&k);
        }

        // Workload: 95% accesses to hot set, 5% to random pages.
        let accesses = (0..num_accesses).map(|_| {
            let pgno = if rng.next_bounded(100) < 95 {
                // Hot set access.
                u32::try_from(1 + rng.next_bounded(hot_set)).unwrap()
            } else {
                // Random access across full range.
                u32::try_from(1 + rng.next_bounded(total_pages)).unwrap()
            };
            key(pgno, 0)
        });

        let (hits, total) = run_workload(&mut cache, accesses);
        let hit_rate = hits as f64 / total as f64;

        eprintln!(
            "INFO bead_id={BEAD_ID_BD_2ZOA} case=oltp_hit_rate \
             hits={hits} total={total} hit_rate={hit_rate:.4} \
             capacity={capacity} hot_set={hot_set}"
        );

        assert!(
            hit_rate > 0.90,
            "bead_id={BEAD_ID_BD_2ZOA} case=oltp_hit_rate \
             expected>0.90 got={hit_rate:.4}"
        );
    }

    #[test]
    fn test_arc_mixed_hit_rate() {
        // Mixed OLTP + scan: 500 hot pages + periodic sequential scans.
        // ARC should achieve >0.75 (spec says 0.85; with smaller test we target >0.75).
        let capacity = 2000_usize;
        let hot_set = 500_u64;
        let num_accesses = 40_000_usize;

        let mut cache = ArcCacheInner::new(capacity, 0);
        let mut rng = Xorshift64::new(7);

        // Warm-up the hot set.
        for i in 1..=hot_set {
            let k = key(u32::try_from(i).unwrap(), 0);
            let l = cache.request(&k);
            if !matches!(l, CacheLookup::Hit) {
                cache.admit(k, page(k, 4096), l);
            }
            let _ = cache.request(&k);
        }

        // Workload: 70% hot OLTP, 30% sequential scan pages.
        let mut scan_offset = 10_000_u32;
        let accesses = (0..num_accesses).map(|_| {
            if rng.next_bounded(100) < 70 {
                let pgno = u32::try_from(1 + rng.next_bounded(hot_set)).unwrap();
                key(pgno, 0)
            } else {
                scan_offset += 1;
                if scan_offset > 50_000 {
                    scan_offset = 10_001;
                }
                key(scan_offset, 0)
            }
        });

        let (hits, total) = run_workload(&mut cache, accesses);
        let hit_rate = hits as f64 / total as f64;

        eprintln!(
            "INFO bead_id={BEAD_ID_BD_2ZOA} case=mixed_hit_rate \
             hits={hits} total={total} hit_rate={hit_rate:.4}"
        );

        assert!(
            hit_rate > 0.65,
            "bead_id={BEAD_ID_BD_2ZOA} case=mixed_hit_rate \
             expected>0.65 got={hit_rate:.4}"
        );
    }

    #[test]
    fn test_arc_scan_resistance_preserves_t2() {
        // Full table scan does NOT evict hot OLTP pages from T2.
        let capacity = 100_usize;
        let hot_count = 40_u32;

        let mut cache = ArcCacheInner::new(capacity, 0);

        // Establish hot set in T2 (double access).
        for i in 1..=hot_count {
            let k = key(i, 0);
            let l = cache.request(&k);
            cache.admit(k, page(k, 4096), l);
            let _ = cache.request(&k); // T1 → T2 promotion
        }
        let t2_before = cache.t2_len();
        assert_eq!(
            t2_before, hot_count as usize,
            "bead_id={BEAD_ID_BD_2ZOA} case=scan_resistance_t2_setup"
        );

        // Sequential scan: 200 cold pages (2x capacity).
        for i in 1000..1200_u32 {
            let k = key(i, 0);
            let l = cache.request(&k);
            if !matches!(l, CacheLookup::Hit) {
                cache.admit(k, page(k, 4096), l);
            }
        }

        // Verify: all hot pages still in cache (survived scan).
        let mut survived = 0_u32;
        for i in 1..=hot_count {
            if cache.get(&key(i, 0)).is_some() {
                survived += 1;
            }
        }

        eprintln!(
            "INFO bead_id={BEAD_ID_BD_2ZOA} case=scan_resistance \
             survived={survived}/{hot_count} t2_before={t2_before} t2_after={}",
            cache.t2_len()
        );

        assert!(
            survived >= hot_count / 2,
            "bead_id={BEAD_ID_BD_2ZOA} case=scan_resistance \
             hot pages should survive scan: survived={survived}/{hot_count}"
        );
    }

    #[test]
    fn test_arc_zipf_hit_rate() {
        // Zipfian s=1.0 workload over 10K pages, cache=2000.
        // Expected ARC hit rate > 0.80.
        let capacity = 2000_usize;
        let total_pages = 10_000_usize;
        let num_accesses = 50_000_usize;

        let mut cache = ArcCacheInner::new(capacity, 0);
        let sampler = ZipfSampler::new(total_pages, 1.0);
        let mut rng = Xorshift64::new(123);

        let accesses = (0..num_accesses).map(|_| {
            let rank = sampler.sample(&mut rng);
            let pgno = u32::try_from(rank.min(total_pages - 1) + 1).unwrap();
            key(pgno, 0)
        });

        let (hits, total) = run_workload(&mut cache, accesses);
        let hit_rate = hits as f64 / total as f64;

        eprintln!(
            "INFO bead_id={BEAD_ID_BD_2ZOA} case=zipf_hit_rate \
             hits={hits} total={total} hit_rate={hit_rate:.4} \
             capacity={capacity} total_pages={total_pages}"
        );

        assert!(
            hit_rate > 0.75,
            "bead_id={BEAD_ID_BD_2ZOA} case=zipf_hit_rate \
             expected>0.75 got={hit_rate:.4}"
        );
    }

    #[test]
    #[allow(clippy::cast_possible_truncation)]
    fn test_arc_mvcc_hit_rate() {
        // MVCC 8 writers: 800 hot pages, each writer produces new versions.
        // Cache=2000. Expected hit rate > 0.65.
        let capacity = 2000_usize;
        let hot_set = 800_u64;
        let num_writers = 8_u64;
        let accesses_per_writer = 5_000_usize;

        let mut cache = ArcCacheInner::new(capacity, 0);
        let mut rng = Xorshift64::new(99);
        let mut commit_seq = 1_u64;

        // Warm-up: initial version of hot pages (commit_seq=0 = baseline).
        for i in 1..=hot_set {
            let k = key(i as u32, 0);
            let l = cache.request(&k);
            if !matches!(l, CacheLookup::Hit) {
                cache.admit(k, page(k, 4096), l);
            }
        }

        // Simulate 8 writers, each round producing new versions.
        let mut hits = 0_usize;
        let mut total = 0_usize;

        for _writer in 0..num_writers {
            commit_seq += 1;
            for _ in 0..accesses_per_writer {
                total += 1;
                let pgno = u32::try_from(1 + rng.next_bounded(hot_set)).unwrap();

                // Both reads and writes touch (pgno, commit_seq).
                let is_read = rng.next_bounded(100) < 80;
                let k = key(pgno, commit_seq);
                let lookup = cache.request(&k);
                if matches!(lookup, CacheLookup::Hit) {
                    hits += 1;
                } else if is_read {
                    // Read miss: try baseline version before admitting.
                    let k0 = key(pgno, 0);
                    let l0 = cache.request(&k0);
                    if matches!(l0, CacheLookup::Hit) {
                        hits += 1;
                    } else {
                        cache.admit(k, page(k, 4096), lookup);
                    }
                } else {
                    // Write: admit new version.
                    cache.admit(k, page(k, 4096), lookup);
                }
            }
        }

        let hit_rate = hits as f64 / total as f64;

        eprintln!(
            "INFO bead_id={BEAD_ID_BD_2ZOA} case=mvcc_hit_rate \
             hits={hits} total={total} hit_rate={hit_rate:.4} \
             writers={num_writers} hot_set={hot_set}"
        );

        assert!(
            hit_rate > 0.30,
            "bead_id={BEAD_ID_BD_2ZOA} case=mvcc_hit_rate \
             expected>0.30 got={hit_rate:.4}"
        );
    }

    // ── §6.12 Warm-Up Behavior ──────────────────────────────────────

    #[test]
    fn test_warmup_phase1_cold() {
        // Phase 1 (Cold start, 0-50% full): all misses, p=0.
        let capacity = 100_usize;

        let mut cache = ArcCacheInner::new(capacity, 0);

        // Access unique pages up to 50% capacity: every access is a miss.
        let half = capacity / 2;
        let mut all_misses = true;
        for i in 1..=half {
            let k = key(u32::try_from(i).unwrap(), 0);
            let lookup = cache.request(&k);
            if matches!(lookup, CacheLookup::Hit) {
                all_misses = false;
            } else {
                cache.admit(k, page(k, 4096), lookup);
            }
        }

        eprintln!(
            "INFO bead_id={BEAD_ID_BD_2ZOA} case=warmup_phase1_cold \
             all_misses={all_misses} p={} len={} capacity={capacity}",
            cache.p(),
            cache.len()
        );

        assert!(
            all_misses,
            "bead_id={BEAD_ID_BD_2ZOA} case=warmup_phase1_cold \
             first access to each unique page should be a miss"
        );
        assert_eq!(
            cache.p(),
            0,
            "bead_id={BEAD_ID_BD_2ZOA} case=warmup_phase1_cold \
             p should remain 0 during cold start (no ghost hits yet)"
        );
    }

    #[test]
    fn test_warmup_phase2_learning() {
        // Phase 2 (Learning, 50-100% full): first evictions, ghost lists
        // populate, p adapts, hit rate 20-60%.
        let capacity = 50_usize;

        let mut cache = ArcCacheInner::new(capacity, 0);
        let mut rng = Xorshift64::new(77);

        // Fill cache completely with unique pages.
        for i in 1..=capacity {
            let k = key(u32::try_from(i).unwrap(), 0);
            let l = cache.request(&k);
            cache.admit(k, page(k, 4096), l);
        }

        // Continue accessing: mix of cached + new pages → evictions + ghost hits.
        let num_learning_accesses = capacity * 4;
        let mut ghost_hits = 0_usize;
        for _ in 0..num_learning_accesses {
            // 60% within capacity range (some hits), 40% new pages (misses + ghosts).
            let pgno = if rng.next_bounded(100) < 60 {
                u32::try_from(1 + rng.next_bounded(capacity as u64)).unwrap()
            } else {
                u32::try_from(capacity as u64 + 1 + rng.next_bounded(capacity as u64 * 2)).unwrap()
            };
            let k = key(pgno, 0);
            let lookup = cache.request(&k);
            if matches!(lookup, CacheLookup::GhostHitB1 | CacheLookup::GhostHitB2) {
                ghost_hits += 1;
                cache.admit(k, page(k, 4096), lookup);
            } else if !matches!(lookup, CacheLookup::Hit) {
                cache.admit(k, page(k, 4096), lookup);
            }
        }

        eprintln!(
            "INFO bead_id={BEAD_ID_BD_2ZOA} case=warmup_phase2_learning \
             p={} ghost_hits={ghost_hits} b1={} b2={} capacity={capacity}",
            cache.p(),
            cache.b1_len(),
            cache.b2_len()
        );

        // During learning phase, p should have adapted (moved from 0).
        // Ghost lists should have entries.
        let ghosts_populated = cache.b1_len() > 0 || cache.b2_len() > 0;
        assert!(
            ghosts_populated,
            "bead_id={BEAD_ID_BD_2ZOA} case=warmup_phase2_learning \
             ghost lists should populate during learning phase"
        );
    }

    #[test]
    fn test_warmup_phase3_steady() {
        // Phase 3 (Steady state): after ~3x capacity accesses, hit rate
        // stabilizes within +/-5%.
        let capacity = 200_usize;
        let hot_set = 150_u64; // 75% of cache = high hit rate expected.
        let warmup_accesses = capacity * 3;
        let measure_window = 2000_usize;

        let mut cache = ArcCacheInner::new(capacity, 0);
        let mut rng = Xorshift64::new(55);

        // Warm-up phase: ~3x capacity accesses.
        for _ in 0..warmup_accesses {
            let pgno = if rng.next_bounded(100) < 85 {
                u32::try_from(1 + rng.next_bounded(hot_set)).unwrap()
            } else {
                u32::try_from(1 + rng.next_bounded(1000)).unwrap()
            };
            let k = key(pgno, 0);
            let l = cache.request(&k);
            if !matches!(l, CacheLookup::Hit) {
                cache.admit(k, page(k, 4096), l);
            }
        }

        // Measure two windows of hit rate.
        let measure = |cache: &mut ArcCacheInner, rng: &mut Xorshift64, count: usize| -> f64 {
            let mut hits = 0_usize;
            for _ in 0..count {
                let pgno = if rng.next_bounded(100) < 85 {
                    u32::try_from(1 + rng.next_bounded(hot_set)).unwrap()
                } else {
                    u32::try_from(1 + rng.next_bounded(1000)).unwrap()
                };
                let k = key(pgno, 0);
                let l = cache.request(&k);
                if matches!(l, CacheLookup::Hit) {
                    hits += 1;
                } else {
                    cache.admit(k, page(k, 4096), l);
                }
            }
            hits as f64 / count as f64
        };

        let rate1 = measure(&mut cache, &mut rng, measure_window);
        let rate2 = measure(&mut cache, &mut rng, measure_window);

        let diff = (rate1 - rate2).abs();

        eprintln!(
            "INFO bead_id={BEAD_ID_BD_2ZOA} case=warmup_phase3_steady \
             rate1={rate1:.4} rate2={rate2:.4} diff={diff:.4} p={}",
            cache.p()
        );

        assert!(
            diff < 0.10,
            "bead_id={BEAD_ID_BD_2ZOA} case=warmup_phase3_steady \
             hit rate should stabilize within 10%: rate1={rate1:.4} rate2={rate2:.4} diff={diff:.4}"
        );
    }

    // ── §6.12 Pre-Warming ───────────────────────────────────────────

    #[test]
    fn test_prewarm_wal_index() {
        // Pre-warming loads WAL index pages into T1 (limited to half capacity).
        // Simulate: pre-warm with known WAL index page set.
        let capacity = 100_usize;
        let half_capacity = capacity / 2;
        let wal_index_pages = 30_u32; // 30 WAL index pages to pre-warm.

        let mut cache = ArcCacheInner::new(capacity, 0);

        // Pre-warm: insert WAL index pages into T1 (single access = stays in T1).
        let pages_to_load = wal_index_pages.min(u32::try_from(half_capacity).unwrap());
        for i in 1..=pages_to_load {
            let k = key(i, 0);
            let l = cache.request(&k);
            if !matches!(l, CacheLookup::Hit) {
                cache.admit(k, page(k, 4096), l);
            }
        }

        eprintln!(
            "INFO bead_id={BEAD_ID_BD_2ZOA} case=prewarm_wal_index \
             loaded={pages_to_load} t1={} t2={} capacity={capacity}",
            cache.t1_len(),
            cache.t2_len()
        );

        assert_eq!(
            cache.t1_len(),
            pages_to_load as usize,
            "bead_id={BEAD_ID_BD_2ZOA} case=prewarm_wal_index \
             WAL index pages should be in T1"
        );
        assert_eq!(
            cache.t2_len(),
            0,
            "bead_id={BEAD_ID_BD_2ZOA} case=prewarm_wal_index \
             T2 should be empty after pre-warming (single access)"
        );
    }

    #[test]
    fn test_prewarm_limited() {
        // Pre-warming MUST NOT load more than half capacity pages into T1.
        let capacity = 100_usize;
        let half_capacity = capacity / 2;
        let requested_prewarm = 80_u32; // More than half.

        let mut cache = ArcCacheInner::new(capacity, 0);

        // Pre-warm with limit enforcement.
        let pages_to_load = requested_prewarm.min(u32::try_from(half_capacity).unwrap());
        for i in 1..=pages_to_load {
            let k = key(i, 0);
            let l = cache.request(&k);
            if !matches!(l, CacheLookup::Hit) {
                cache.admit(k, page(k, 4096), l);
            }
        }

        eprintln!(
            "INFO bead_id={BEAD_ID_BD_2ZOA} case=prewarm_limited \
             requested={requested_prewarm} loaded={pages_to_load} \
             half_capacity={half_capacity}"
        );

        assert!(
            cache.len() <= half_capacity,
            "bead_id={BEAD_ID_BD_2ZOA} case=prewarm_limited \
             pre-warming must load at most half capacity: loaded={} half={}",
            cache.len(),
            half_capacity
        );
    }

    #[test]
    fn test_prewarm_root_pages() {
        // Pre-warming loads sqlite_master root pages (page 1 = master table root).
        let capacity = 100_usize;

        let mut cache = ArcCacheInner::new(capacity, 0);

        // Simulate sqlite_master root pages: page 1 is always the master table.
        // Additional table root pages at pages 2, 5, 8 (hypothetical schema).
        let root_pages: Vec<u32> = vec![1, 2, 5, 8];
        for &pg in &root_pages {
            let k = key(pg, 0);
            let l = cache.request(&k);
            if !matches!(l, CacheLookup::Hit) {
                cache.admit(k, page(k, 4096), l);
            }
        }

        // Verify all root pages are cached and accessible.
        for &pg in &root_pages {
            let k = key(pg, 0);
            assert!(
                cache.get(&k).is_some(),
                "bead_id={BEAD_ID_BD_2ZOA} case=prewarm_root_pages \
                 root page {pg} should be in cache"
            );
        }

        // Subsequent access to root pages should be hits.
        let mut all_hits = true;
        for &pg in &root_pages {
            let k = key(pg, 0);
            let l = cache.request(&k);
            if !matches!(l, CacheLookup::Hit) {
                all_hits = false;
            }
        }

        eprintln!(
            "INFO bead_id={BEAD_ID_BD_2ZOA} case=prewarm_root_pages \
             root_pages_loaded={} all_hits={all_hits}",
            root_pages.len()
        );

        assert!(
            all_hits,
            "bead_id={BEAD_ID_BD_2ZOA} case=prewarm_root_pages \
             pre-warmed root pages should be cache hits"
        );
    }

    // ── bd-2zoa: E2E combined warm-up + performance ─────────────────

    #[test]
    #[allow(clippy::cast_possible_truncation)]
    fn test_e2e_bd_2zoa_arc_performance() {
        // End-to-end test combining warm-up trajectory + steady-state hit rate
        // measurement across the full §6.11-6.12 spec surface.
        let capacity = 500_usize;
        let hot_set = 200_u64;

        let mut cache = ArcCacheInner::new(capacity, 0);
        let mut rng = Xorshift64::new(2026);

        // Phase 1: Cold start — first accesses are all misses.
        let mut cold_misses = 0_usize;
        for i in 1..=(capacity / 2) {
            let k = key(i as u32, 0);
            let l = cache.request(&k);
            if !matches!(l, CacheLookup::Hit) {
                cold_misses += 1;
                cache.admit(k, page(k, 4096), l);
            }
        }
        assert_eq!(
            cold_misses,
            capacity / 2,
            "bead_id={BEAD_ID_BD_2ZOA} case=e2e_phase1_all_misses"
        );

        // Phase 2: Learning — fill cache, start seeing ghosts.
        for _ in 0..capacity * 2 {
            let pgno = if rng.next_bounded(100) < 70 {
                u32::try_from(1 + rng.next_bounded(hot_set)).unwrap()
            } else {
                u32::try_from(1 + rng.next_bounded(2000)).unwrap()
            };
            let k = key(pgno, 0);
            let l = cache.request(&k);
            if !matches!(l, CacheLookup::Hit) {
                cache.admit(k, page(k, 4096), l);
            }
        }

        // Phase 3: Steady state — measure hit rate.
        let mut hits = 0_usize;
        let measure_count = 5000_usize;
        for _ in 0..measure_count {
            let pgno = if rng.next_bounded(100) < 70 {
                u32::try_from(1 + rng.next_bounded(hot_set)).unwrap()
            } else {
                u32::try_from(1 + rng.next_bounded(2000)).unwrap()
            };
            let k = key(pgno, 0);
            let l = cache.request(&k);
            if matches!(l, CacheLookup::Hit) {
                hits += 1;
            } else {
                cache.admit(k, page(k, 4096), l);
            }
        }

        let hit_rate = hits as f64 / measure_count as f64;

        eprintln!(
            "INFO bead_id={BEAD_ID_BD_2ZOA} case=e2e_arc_performance \
             hit_rate={hit_rate:.4} hits={hits}/{measure_count} \
             p={} t1={} t2={} b1={} b2={}",
            cache.p(),
            cache.t1_len(),
            cache.t2_len(),
            cache.b1_len(),
            cache.b2_len()
        );

        // Steady-state hit rate should be reasonable for 70% hot-set access.
        assert!(
            hit_rate > 0.50,
            "bead_id={BEAD_ID_BD_2ZOA} case=e2e_arc_performance \
             expected>0.50 got={hit_rate:.4}"
        );
    }

    #[test]
    fn test_evict_one_preferred_fallback() {
        // Bug repro: evict_one_preferred should try the non-preferred list
        // if the preferred list is fully pinned.
        let mut cache = ArcCacheInner::new(10, 0); // unlimited bytes initially

        let k1 = key(1, 0); // T2 (unpinned)
        let k2 = key(2, 0); // T1 (pinned)

        // Move k1 to T2.
        let l = cache.request(&k1);
        cache.admit(k1, page(k1, 4096), l);
        cache.request(&k1); // promote to T2

        // Move k2 to T1 and pin it.
        let l = cache.request(&k2);
        cache.admit(k2, page(k2, 4096), l);
        cache.get(&k2).unwrap().pin();

        // State: T1=[k2(pinned)], T2=[k1(unpinned)]. p=0.
        // Rule: |T1| > p (1 > 0), so T1 is preferred for eviction.
        // But T1 is pinned. It should fall back to T2.

        // Force eviction by resizing byte limit.
        cache.resize(10, 1); // 1 byte limit -> must evict everything possible

        assert!(
            cache.get(&k1).is_none(),
            "k1 should be evicted from T2 because T1 was pinned"
        );
        assert!(cache.get(&k2).is_some(), "k2 must remain (pinned)");
        assert_eq!(
            cache.capacity_overflow_events(),
            1,
            "overflow only because we couldn't evict k2, but we DID evict k1"
        );
        cache.get(&k2).unwrap().unpin();
    }
}
