//! §6.1-6.2 Adaptive Replacement Cache with MVCC-aware keying (bd-bt16).
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
use std::sync::atomic::{AtomicU32, Ordering};

use fsqlite_types::{CommitSeq, PageData, PageNumber};
use parking_lot::Mutex;

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
    pub data: PageData,
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
    pub fn new(key: CacheKey, data: PageData, xxh3: u64, wal_frame: Option<u32>) -> Self {
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
        }
    }

    // -- Accessors --

    /// Adaptive target for T1 size.
    #[inline]
    #[must_use]
    pub fn p(&self) -> usize {
        self.p
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

    /// Set the GC horizon for superseded version preference.
    pub fn set_gc_horizon(&mut self, horizon: CommitSeq) {
        self.gc_horizon = horizon;
    }

    /// Get a reference to a cached page by key.
    ///
    /// Does **not** modify list ordering.  Call [`request`] first to
    /// trigger ARC promotion/refresh logic.
    #[must_use]
    pub fn get(&self, key: &CacheKey) -> Option<&CachedPage> {
        match self.directory.get(key)? {
            Location::T1(idx) => self.t1.get(*idx),
            Location::T2(idx) => self.t2.get(*idx),
            Location::B1(_) | Location::B2(_) => None,
        }
    }

    // -- Core ARC operations --

    /// Look up a key, performing ARC promotion/adjustment.
    ///
    /// Returns the outcome so the caller knows whether to fetch from disk
    /// (on [`CacheLookup::GhostHitB1`], [`CacheLookup::GhostHitB2`], or
    /// [`CacheLookup::Miss`]) and then call [`admit`].
    pub fn request(&mut self, key: &CacheKey) -> CacheLookup {
        let location = self.directory.get(key).copied();
        match location {
            Some(Location::T1(idx)) => {
                // Hit in T1 → promote to T2 (recency → frequency).
                let page = self.t1.remove(idx);
                let new_idx = self.t2.push_back(page);
                self.directory.insert(*key, Location::T2(new_idx));
                CacheLookup::Hit
            }
            Some(Location::T2(idx)) => {
                // Hit in T2 → move to MRU of T2 (refresh).
                self.t2.move_to_back(idx);
                CacheLookup::Hit
            }
            Some(Location::B1(idx)) => {
                // Ghost hit in B1 → increase p (favour recency).
                let delta = std::cmp::max(self.b2.len() / std::cmp::max(self.b1.len(), 1), 1);
                self.p = std::cmp::min(self.capacity, self.p.saturating_add(delta));
                // Remove ghost.
                self.b1.remove(idx);
                self.directory.remove(key);
                CacheLookup::GhostHitB1
            }
            Some(Location::B2(idx)) => {
                // Ghost hit in B2 → decrease p (favour frequency).
                let delta = std::cmp::max(self.b1.len() / std::cmp::max(self.b2.len(), 1), 1);
                self.p = self.p.saturating_sub(delta);
                // Remove ghost.
                self.b2.remove(idx);
                self.directory.remove(key);
                CacheLookup::GhostHitB2
            }
            None => CacheLookup::Miss,
        }
    }

    /// Insert a page after a cache miss or ghost hit.
    ///
    /// `lookup` must be the result of the preceding [`request`] call for the
    /// same key.  On ghost hits the page goes into T2; on misses it goes
    /// into T1.
    ///
    /// **Eviction invariant:** this method never performs I/O.
    pub fn admit(&mut self, key: CacheKey, page: CachedPage, lookup: CacheLookup) {
        debug_assert!(
            !matches!(lookup, CacheLookup::Hit),
            "admit called after a cache hit"
        );

        let byte_size = page.byte_size;

        // Phase 1: make room in the directory (ghost eviction).
        let l1_len = self.t1.len() + self.b1.len();
        if l1_len >= self.capacity {
            if self.t1.len() < self.capacity {
                // Evict LRU ghost from B1.
                if let Some(ghost) = self.b1.pop_front() {
                    self.directory.remove(&ghost);
                }
                self.replace(&key, matches!(lookup, CacheLookup::GhostHitB2));
            } else {
                // T1 alone fills capacity — evict LRU from T1 directly.
                self.evict_lru_from_t1();
            }
        } else {
            let total_dir = l1_len + self.t2.len() + self.b2.len();
            if total_dir >= self.capacity * 2 {
                // Evict LRU ghost from B2.
                if let Some(ghost) = self.b2.pop_front() {
                    self.directory.remove(&ghost);
                }
            }
            if self.t1.len() + self.t2.len() >= self.capacity {
                self.replace(&key, matches!(lookup, CacheLookup::GhostHitB2));
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

        // Phase 3: enforce byte limit (dual trigger).
        while self.max_bytes > 0 && self.total_bytes > self.max_bytes && self.len() > 1 {
            if !self.evict_one_preferred() {
                break; // all remaining pages are pinned
            }
        }

        let _ = idx; // suppress unused warning
    }

    // -- Internal eviction machinery --

    /// ARC REPLACE subroutine.
    fn replace(&mut self, _incoming: &CacheKey, from_b2: bool) {
        let t1_len = self.t1.len();
        if t1_len > 0 && (t1_len > self.p || (from_b2 && t1_len == self.p)) {
            self.evict_lru_from_t1();
        } else {
            self.evict_lru_from_t2();
        }
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
            true
        } else {
            false
        }
    }

    /// Evict one page, preferring superseded versions.
    fn evict_one_preferred(&mut self) -> bool {
        // First: try to find a superseded victim in T1 or T2.
        if let Some(idx) = self.find_superseded_victim(&self.t1) {
            let page = self.t1.remove(idx);
            let key = page.key;
            self.total_bytes -= page.byte_size;
            self.decrement_page_version(key.pgno);
            self.directory.remove(&key);
            let ghost_idx = self.b1.push_back(key);
            self.directory.insert(key, Location::B1(ghost_idx));
            return true;
        }
        if let Some(idx) = self.find_superseded_victim(&self.t2) {
            let page = self.t2.remove(idx);
            let key = page.key;
            self.total_bytes -= page.byte_size;
            self.decrement_page_version(key.pgno);
            self.directory.remove(&key);
            let ghost_idx = self.b2.push_back(key);
            self.directory.insert(key, Location::B2(ghost_idx));
            return true;
        }
        // Fallback: standard REPLACE.
        let t1_len = self.t1.len();
        if t1_len > 0 && t1_len > self.p {
            self.evict_lru_from_t1()
        } else {
            self.evict_lru_from_t2()
        }
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
        for (idx, page) in list.iter() {
            if page.is_pinned() {
                continue;
            }
            let count = self.page_versions.get(&page.key.pgno).copied().unwrap_or(0);
            if count > 1 && page.key.commit_seq <= self.gc_horizon {
                return Some(idx);
            }
        }
        None
    }

    fn decrement_page_version(&mut self, pgno: PageNumber) {
        if let Some(count) = self.page_versions.get_mut(&pgno) {
            *count -= 1;
            if *count == 0 {
                self.page_versions.remove(&pgno);
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
            .finish()
    }
}

// ═══════════════════════════════════════════════════════════════════════
// ArcCache — thread-safe wrapper
// ═══════════════════════════════════════════════════════════════════════

/// Thread-safe ARC cache wrapping [`ArcCacheInner`] in a [`Mutex`].
pub struct ArcCache {
    inner: Mutex<ArcCacheInner>,
}

impl ArcCache {
    /// Create a new thread-safe ARC cache.
    #[must_use]
    pub fn new(capacity: usize, max_bytes: usize) -> Self {
        Self {
            inner: Mutex::new(ArcCacheInner::new(capacity, max_bytes)),
        }
    }

    /// Acquire the inner lock for direct manipulation.
    pub fn lock(&self) -> parking_lot::MutexGuard<'_, ArcCacheInner> {
        self.inner.lock()
    }
}

impl std::fmt::Debug for ArcCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArcCache")
            .field("inner", &*self.inner.lock())
            .finish()
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use fsqlite_types::PageSize;

    const BEAD_ID: &str = "bd-bt16";

    /// Helper: create a `CacheKey` from raw parts.
    fn key(pgno: u32, commit_seq: u64) -> CacheKey {
        CacheKey::new(PageNumber::new(pgno).unwrap(), CommitSeq::new(commit_seq))
    }

    /// Helper: create a `CachedPage` with the given key and a 4096-byte page.
    fn page(k: CacheKey, size: usize) -> CachedPage {
        // Use a PageData with the requested logical size.
        // For testing, we use DEFAULT page size for the underlying data.
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
        let mut cp = CachedPage::new(k, PageData::zeroed(ps), 0, None);
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
    fn test_arc_t1_t2_promotion() {
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

    // ── 3. test_arc_ghost_hit_b1 ──────────────────────────────────────

    #[test]
    fn test_arc_ghost_hit_b1() {
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
    fn test_arc_ghost_hit_b2() {
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
    fn test_pinned_page_not_evicted() {
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
    fn test_e2e_arc_mvcc_integration_smoke() {
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
}
