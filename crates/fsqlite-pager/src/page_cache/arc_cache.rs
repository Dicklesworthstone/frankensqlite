//! Adaptive Replacement Cache (ARC) structures with MVCC-aware cache keys.
//!
//! This module implements the §6.1-6.2 data structures for `bd-bt16`:
//! - `CacheKey = (PageNumber, CommitSeq)`
//! - `CachedPage` metadata with pin tracking
//! - ARC sets `T1`, `T2`, `B1`, `B2` and adaptive target `p`
//!
//! The implementation is intentionally allocation-light and deterministic.
//! Eviction is a pure memory operation and never performs I/O.

use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
use std::sync::atomic::{AtomicU32, Ordering};

use fsqlite_types::{CommitSeq, PageNumber};
use xxhash_rust::xxh3::xxh3_64;

use crate::PageBuf;

/// MVCC-aware cache key.
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

/// A page cached by ARC.
pub struct CachedPage {
    pub key: CacheKey,
    pub data: PageBuf,
    pub ref_count: AtomicU32,
    pub xxh3: u64,
    pub byte_size: usize,
    pub wal_frame: Option<u32>,
}

impl CachedPage {
    /// Build a cached page and compute integrity metadata.
    #[must_use]
    pub fn new(key: CacheKey, data: PageBuf, wal_frame: Option<u32>) -> Self {
        let xxh3 = xxh3_64(data.as_slice());
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

    #[inline]
    pub fn pin(&self) {
        let _ = self.ref_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrease pin count if non-zero.
    #[inline]
    pub fn unpin(&self) {
        let mut current = self.ref_count.load(Ordering::Relaxed);
        while current > 0 {
            match self.ref_count.compare_exchange_weak(
                current,
                current - 1,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(observed) => current = observed,
            }
        }
    }

    #[inline]
    #[must_use]
    pub fn is_pinned(&self) -> bool {
        self.ref_count.load(Ordering::Relaxed) > 0
    }
}

impl fmt::Debug for CachedPage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CachedPage")
            .field("key", &self.key)
            .field("data", &format_args!("PageBuf(len={})", self.data.len()))
            .field("ref_count", &self.ref_count.load(Ordering::Relaxed))
            .field("xxh3", &format_args!("{:#018x}", self.xxh3))
            .field("byte_size", &self.byte_size)
            .field("wal_frame", &self.wal_frame)
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccessOutcome {
    Hit,
    MissInserted,
    MissInsertedOverflow,
}

#[derive(Debug, Default)]
struct Store {
    order: VecDeque<CacheKey>,
    set: HashSet<CacheKey>,
}

impl Store {
    fn contains(&self, key: CacheKey) -> bool {
        self.set.contains(&key)
    }

    fn len(&self) -> usize {
        self.order.len()
    }

    fn is_empty(&self) -> bool {
        self.order.is_empty()
    }

    fn push_back(&mut self, key: CacheKey) {
        if self.set.insert(key) {
            self.order.push_back(key);
        }
    }

    fn pop_front(&mut self) -> Option<CacheKey> {
        let key = self.order.pop_front()?;
        let _ = self.set.remove(&key);
        Some(key)
    }

    fn remove(&mut self, key: CacheKey) -> bool {
        if !self.set.remove(&key) {
            return false;
        }
        self.order.retain(|candidate| *candidate != key);
        true
    }

    fn move_to_back(&mut self, key: CacheKey) -> bool {
        if !self.remove(key) {
            return false;
        }
        self.push_back(key);
        true
    }

    fn ordered_keys(&self) -> impl Iterator<Item = CacheKey> + '_ {
        self.order.iter().copied()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ListKind {
    T1,
    T2,
}

/// ARC cache with MVCC-aware keys.
#[derive(Debug)]
pub struct ArcCache {
    t1: Store,
    t2: Store,
    b1: Store,
    b2: Store,
    p: usize,
    capacity: usize,
    total_bytes: usize,
    max_bytes: usize,
    index: HashMap<CacheKey, CachedPage>,
    evictions: usize,
    io_writes: usize,
    capacity_overflow: usize,
}

impl ArcCache {
    /// Create a cache with entry and byte caps.
    #[must_use]
    pub fn new(capacity: usize, max_bytes: usize) -> Self {
        assert!(capacity > 0, "capacity must be > 0");
        assert!(max_bytes > 0, "max_bytes must be > 0");
        Self {
            t1: Store::default(),
            t2: Store::default(),
            b1: Store::default(),
            b2: Store::default(),
            p: 0,
            capacity,
            total_bytes: 0,
            max_bytes,
            index: HashMap::new(),
            evictions: 0,
            io_writes: 0,
            capacity_overflow: 0,
        }
    }

    #[inline]
    #[must_use]
    pub fn len(&self) -> usize {
        self.index.len()
    }

    #[inline]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }

    #[inline]
    #[must_use]
    pub fn contains(&self, key: CacheKey) -> bool {
        self.index.contains_key(&key)
    }

    #[inline]
    #[must_use]
    pub fn get(&self, key: CacheKey) -> Option<&CachedPage> {
        self.index.get(&key)
    }

    #[inline]
    #[must_use]
    pub fn total_bytes(&self) -> usize {
        self.total_bytes
    }

    #[inline]
    #[must_use]
    pub fn p_target(&self) -> usize {
        self.p
    }

    /// Number of logical eviction events (memory-only operation).
    #[inline]
    #[must_use]
    pub fn evictions(&self) -> usize {
        self.evictions
    }

    /// Count of write I/O operations initiated by eviction.
    ///
    /// ARC eviction in this layer is memory-only, so this should remain zero.
    #[inline]
    #[must_use]
    pub fn io_writes(&self) -> usize {
        self.io_writes
    }

    /// Number of times all pages were pinned and temporary capacity overflow
    /// was used as a safety valve.
    #[inline]
    #[must_use]
    pub fn capacity_overflow_events(&self) -> usize {
        self.capacity_overflow
    }

    #[cfg(test)]
    fn in_t1(&self, key: CacheKey) -> bool {
        self.t1.contains(key)
    }

    #[cfg(test)]
    fn in_t2(&self, key: CacheKey) -> bool {
        self.t2.contains(key)
    }

    #[cfg(test)]
    fn in_b1(&self, key: CacheKey) -> bool {
        self.b1.contains(key)
    }

    #[cfg(test)]
    fn in_b2(&self, key: CacheKey) -> bool {
        self.b2.contains(key)
    }

    #[cfg(test)]
    fn t2_lru(&self) -> Option<CacheKey> {
        self.t2.order.front().copied()
    }

    #[cfg(test)]
    fn t2_mru(&self) -> Option<CacheKey> {
        self.t2.order.back().copied()
    }

    #[cfg(test)]
    fn b1_len(&self) -> usize {
        self.b1.len()
    }

    #[cfg(test)]
    fn set_p_for_tests(&mut self, p: usize) {
        self.p = p.min(self.capacity);
    }

    /// Register a hit without inserting a new page.
    pub fn access(&mut self, key: CacheKey) -> bool {
        if !self.index.contains_key(&key) {
            return false;
        }
        self.promote_hit(key);
        true
    }

    /// ARC request path: hit promotion or miss insertion.
    pub fn access_or_insert(&mut self, page: CachedPage) -> AccessOutcome {
        let key = page.key;
        if self.index.contains_key(&key) {
            self.promote_hit(key);
            return AccessOutcome::Hit;
        }

        let from_b1 = self.b1.contains(key);
        let from_b2 = self.b2.contains(key);

        if from_b1 {
            self.raise_p();
            let _ = self.b1.remove(key);
        } else if from_b2 {
            self.lower_p();
            let _ = self.b2.remove(key);
        }

        let room = self.ensure_room(page.byte_size, from_b2);

        if from_b1 || from_b2 {
            self.t2.push_back(key);
        } else {
            self.t1.push_back(key);
        }

        self.total_bytes += page.byte_size;
        let previous = self.index.insert(key, page);
        debug_assert!(
            previous.is_none(),
            "new miss should not replace existing key"
        );
        match room {
            RoomOutcome::Ready => AccessOutcome::MissInserted,
            RoomOutcome::Overflow => AccessOutcome::MissInsertedOverflow,
        }
    }

    fn promote_hit(&mut self, key: CacheKey) {
        if self.t1.contains(key) {
            let _ = self.t1.remove(key);
            self.t2.push_back(key);
            return;
        }

        let _ = self.t2.move_to_back(key);
    }

    fn raise_p(&mut self) {
        let delta = if self.b1.is_empty() {
            1
        } else {
            std::cmp::max(1, self.b2.len() / self.b1.len())
        };
        self.p = self.capacity.min(self.p.saturating_add(delta));
    }

    fn lower_p(&mut self) {
        let delta = if self.b2.is_empty() {
            1
        } else {
            std::cmp::max(1, self.b1.len() / self.b2.len())
        };
        self.p = self.p.saturating_sub(delta);
    }

    fn ensure_room(&mut self, incoming_bytes: usize, from_b2: bool) -> RoomOutcome {
        let mut b2_bias = from_b2;
        while self.index.len() >= self.capacity
            || self.total_bytes.saturating_add(incoming_bytes) > self.max_bytes
        {
            if !self.replace(b2_bias) {
                self.capacity_overflow = self.capacity_overflow.saturating_add(1);
                return RoomOutcome::Overflow;
            }
            b2_bias = false;
        }
        RoomOutcome::Ready
    }

    fn replace(&mut self, incoming_from_b2: bool) -> bool {
        let prefer_t1 = !self.t1.is_empty()
            && (self.t1.len() > self.p || (incoming_from_b2 && self.t1.len() == self.p));

        if prefer_t1 {
            if self.evict_from(ListKind::T1) {
                return true;
            }
            return self.evict_from(ListKind::T2);
        }

        if self.evict_from(ListKind::T2) {
            return true;
        }
        self.evict_from(ListKind::T1)
    }

    fn evict_from(&mut self, list: ListKind) -> bool {
        if self.list(list).is_empty() {
            return false;
        }

        if let Some(key) = self.pick_candidate(list, true) {
            self.finish_eviction(list, key);
            return true;
        }

        if let Some(key) = self.pick_candidate(list, false) {
            self.finish_eviction(list, key);
            return true;
        }

        false
    }

    fn pick_candidate(&mut self, list: ListKind, require_superseded: bool) -> Option<CacheKey> {
        let candidate = {
            self.list(list).ordered_keys().find(|key| {
                self.is_evictable(*key) && (!require_superseded || self.is_superseded(*key))
            })
        }?;
        let _ = self.list_mut(list).remove(candidate);
        Some(candidate)
    }

    fn is_evictable(&self, key: CacheKey) -> bool {
        self.index.get(&key).is_some_and(|page| !page.is_pinned())
    }

    fn is_superseded(&self, key: CacheKey) -> bool {
        self.index.keys().any(|candidate| {
            candidate.pgno == key.pgno && candidate.commit_seq.get() > key.commit_seq.get()
        })
    }

    fn finish_eviction(&mut self, list: ListKind, key: CacheKey) {
        let evicted = self.index.remove(&key);
        if let Some(page) = evicted {
            self.total_bytes = self.total_bytes.saturating_sub(page.byte_size);
            self.evictions = self.evictions.saturating_add(1);
            match list {
                ListKind::T1 => self.b1.push_back(key),
                ListKind::T2 => self.b2.push_back(key),
            }
            self.trim_ghosts();
        }
    }

    fn trim_ghosts(&mut self) {
        while self.b1.len() > self.capacity {
            let _ = self.b1.pop_front();
        }
        while self.b2.len() > self.capacity {
            let _ = self.b2.pop_front();
        }
    }

    fn list(&self, list: ListKind) -> &Store {
        match list {
            ListKind::T1 => &self.t1,
            ListKind::T2 => &self.t2,
        }
    }

    fn list_mut(&mut self, list: ListKind) -> &mut Store {
        match list {
            ListKind::T1 => &mut self.t1,
            ListKind::T2 => &mut self.t2,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RoomOutcome {
    Ready,
    Overflow,
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use fsqlite_types::PageSize;

    use super::{AccessOutcome, ArcCache, CacheKey, CachedPage};

    const BEAD_ID: &str = "bd-125g";

    fn key(pgno: u32, commit_seq: u64) -> CacheKey {
        CacheKey::new(
            fsqlite_types::PageNumber::new(pgno).expect("non-zero page number"),
            fsqlite_types::CommitSeq::new(commit_seq),
        )
    }

    fn page(key: CacheKey, page_size: PageSize, seed: u8) -> CachedPage {
        let mut data = crate::PageBuf::new(page_size);
        data.as_mut_slice().fill(seed);
        CachedPage::new(key, data, None)
    }

    #[test]
    fn test_cache_key_mvcc_awareness() {
        let pg = fsqlite_types::PageNumber::new(7).expect("non-zero page number");
        let k1 = CacheKey::new(pg, fsqlite_types::CommitSeq::new(1));
        let k2 = CacheKey::new(pg, fsqlite_types::CommitSeq::new(2));
        assert_ne!(k1, k2, "bead_id={BEAD_ID} case=cache_key_mvcc_awareness");

        let mut seen = HashSet::new();
        assert!(seen.insert(k1));
        assert!(seen.insert(k2));
        assert_eq!(seen.len(), 2);
    }

    #[test]
    fn test_arc_t1_t2_promotion() {
        let mut cache = ArcCache::new(4, 4 * 4096);
        let target = key(1, 0);
        assert_eq!(
            cache.access_or_insert(page(target, PageSize::DEFAULT, 0xAA)),
            AccessOutcome::MissInserted
        );
        assert!(cache.in_t1(target));
        assert!(!cache.in_t2(target));

        assert!(cache.access(target));
        assert!(!cache.in_t1(target));
        assert!(cache.in_t2(target));
    }

    #[test]
    fn test_arc_ghost_hit_b1() {
        let mut cache = ArcCache::new(2, 2 * 4096);
        let a = key(1, 0);
        let b = key(2, 0);
        let c = key(3, 0);

        let _ = cache.access_or_insert(page(a, PageSize::DEFAULT, 1));
        let _ = cache.access_or_insert(page(b, PageSize::DEFAULT, 2));
        let _ = cache.access_or_insert(page(c, PageSize::DEFAULT, 3));
        assert!(cache.in_b1(a), "bead_id={BEAD_ID} case=ghost_hit_b1_seed");

        let p_before = cache.p_target();
        let _ = cache.access_or_insert(page(a, PageSize::DEFAULT, 4));
        assert!(
            cache.p_target() > p_before,
            "bead_id={BEAD_ID} case=ghost_hit_b1_p_increase"
        );
        assert!(
            cache.in_t2(a),
            "bead_id={BEAD_ID} case=ghost_hit_b1_promote"
        );
    }

    #[test]
    fn test_arc_ghost_hit_b2() {
        let mut cache = ArcCache::new(1, 4096);
        let a = key(1, 0);
        let b = key(2, 0);
        let c = key(3, 0);

        let _ = cache.access_or_insert(page(a, PageSize::DEFAULT, 1));
        let _ = cache.access_or_insert(page(b, PageSize::DEFAULT, 2));
        assert!(cache.in_b1(a));

        let _ = cache.access_or_insert(page(a, PageSize::DEFAULT, 3));
        assert_eq!(cache.p_target(), 1);

        let _ = cache.access_or_insert(page(c, PageSize::DEFAULT, 4));
        assert!(cache.in_b2(a), "bead_id={BEAD_ID} case=ghost_hit_b2_seed");

        let p_before = cache.p_target();
        let _ = cache.access_or_insert(page(a, PageSize::DEFAULT, 5));
        assert!(
            cache.p_target() < p_before,
            "bead_id={BEAD_ID} case=ghost_hit_b2_p_decrease"
        );
    }

    #[test]
    fn test_replace_prefers_t1_when_over_p() {
        let mut cache = ArcCache::new(2, 2 * 4096);
        let a = key(1, 0);
        let b = key(2, 0);
        let c = key(3, 0);

        let _ = cache.access_or_insert(page(a, PageSize::DEFAULT, 1));
        let _ = cache.access_or_insert(page(b, PageSize::DEFAULT, 2));
        let _ = cache.access_or_insert(page(c, PageSize::DEFAULT, 3));

        assert!(cache.in_b1(a), "bead_id={BEAD_ID} case=replace_prefers_t1");
        assert!(!cache.in_b2(a));
    }

    #[test]
    fn test_replace_b2_tiebreaker() {
        let mut cache = ArcCache::new(2, 2 * 4096);
        let a = key(1, 0);
        let b = key(2, 0);
        let target = key(3, 0);

        let _ = cache.access_or_insert(page(a, PageSize::DEFAULT, 1));
        let _ = cache.access_or_insert(page(b, PageSize::DEFAULT, 2));
        let _ = cache.access(b); // b -> T2, a remains in T1

        // Deterministically seed target in B2, then choose p so that after
        // B2-hit adjustment we get |T1| == p with incoming_from_b2=true.
        cache.b2.push_back(target);
        cache.set_p_for_tests(2);

        let _ = cache.access_or_insert(page(target, PageSize::DEFAULT, 3));
        assert!(cache.in_b1(a), "bead_id={BEAD_ID} case=b2_tiebreaker_t1");
        assert!(cache.in_t2(target));
    }

    #[test]
    fn test_replace_skips_pinned() {
        let mut cache = ArcCache::new(2, 2 * 4096);
        let pinned = key(1, 0);
        let victim = key(2, 0);
        let incoming = key(3, 0);

        let _ = cache.access_or_insert(page(pinned, PageSize::DEFAULT, 1));
        let _ = cache.access_or_insert(page(victim, PageSize::DEFAULT, 2));
        cache.get(pinned).expect("pinned page should exist").pin();

        let _ = cache.access_or_insert(page(incoming, PageSize::DEFAULT, 3));
        assert!(cache.contains(pinned));
        assert!(!cache.contains(victim));
        assert!(cache.contains(incoming));
    }

    #[test]
    fn test_replace_overflow_safety_valve() {
        let mut cache = ArcCache::new(1, 4096);
        let a = key(1, 0);
        let b = key(2, 0);

        let _ = cache.access_or_insert(page(a, PageSize::DEFAULT, 1));
        cache.get(a).expect("page should exist").pin();

        let out = cache.access_or_insert(page(b, PageSize::DEFAULT, 2));
        assert_eq!(out, AccessOutcome::MissInsertedOverflow);
        assert_eq!(cache.capacity_overflow_events(), 1);
        assert_eq!(cache.len(), 2, "safety valve allows temporary growth");
    }

    #[test]
    fn test_replace_fallback() {
        let mut cache = ArcCache::new(2, 2 * 4096);
        let a = key(1, 0);
        let b = key(2, 0);
        let c = key(3, 0);

        let _ = cache.access_or_insert(page(a, PageSize::DEFAULT, 1));
        let _ = cache.access_or_insert(page(b, PageSize::DEFAULT, 2));
        let _ = cache.access(b); // b -> T2, a remains T1
        cache.get(a).expect("a should exist").pin();

        // prefer_t1=true (|T1|>p) but T1 candidate is pinned; must fallback to T2.
        let _ = cache.access_or_insert(page(c, PageSize::DEFAULT, 3));
        assert!(cache.contains(a), "pinned T1 entry must remain");
        assert!(!cache.contains(b), "fallback should evict from T2");
        assert!(cache.contains(c));
    }

    #[test]
    fn test_request_t1_to_t2_promotion() {
        let mut cache = ArcCache::new(2, 2 * 4096);
        let a = key(1, 0);
        let _ = cache.access_or_insert(page(a, PageSize::DEFAULT, 1));
        assert!(cache.in_t1(a));
        assert!(cache.access(a));
        assert!(cache.in_t2(a));
        assert!(!cache.in_t1(a));
    }

    #[test]
    fn test_request_t2_refresh() {
        let mut cache = ArcCache::new(4, 4 * 4096);
        let a = key(1, 0);
        let b = key(2, 0);

        let _ = cache.access_or_insert(page(a, PageSize::DEFAULT, 1));
        let _ = cache.access_or_insert(page(b, PageSize::DEFAULT, 2));
        let _ = cache.access(a);
        let _ = cache.access(b);
        assert_eq!(cache.t2_lru(), Some(a));
        assert_eq!(cache.t2_mru(), Some(b));

        let _ = cache.access(a);
        assert_eq!(cache.t2_lru(), Some(b));
        assert_eq!(cache.t2_mru(), Some(a));
    }

    #[test]
    fn test_request_b1_ghost_increases_p() {
        let mut cache = ArcCache::new(2, 2 * 4096);
        let a = key(1, 0);
        let b = key(2, 0);
        let c = key(3, 0);

        let _ = cache.access_or_insert(page(a, PageSize::DEFAULT, 1));
        let _ = cache.access_or_insert(page(b, PageSize::DEFAULT, 2));
        let _ = cache.access_or_insert(page(c, PageSize::DEFAULT, 3));
        assert!(cache.in_b1(a));
        let p_before = cache.p_target();
        let _ = cache.access_or_insert(page(a, PageSize::DEFAULT, 4));
        assert!(cache.p_target() > p_before);
    }

    #[test]
    fn test_request_b2_ghost_decreases_p() {
        let mut cache = ArcCache::new(1, 4096);
        let a = key(1, 0);
        let b = key(2, 0);
        let c = key(3, 0);

        let _ = cache.access_or_insert(page(a, PageSize::DEFAULT, 1));
        let _ = cache.access_or_insert(page(b, PageSize::DEFAULT, 2));
        let _ = cache.access_or_insert(page(a, PageSize::DEFAULT, 3)); // B1 hit -> p=1
        let _ = cache.access_or_insert(page(c, PageSize::DEFAULT, 4)); // push a to B2
        assert!(cache.in_b2(a));
        let p_before = cache.p_target();
        let _ = cache.access_or_insert(page(a, PageSize::DEFAULT, 5));
        assert!(cache.p_target() < p_before);
    }

    #[test]
    fn test_request_miss_inserts_t1() {
        let mut cache = ArcCache::new(2, 2 * 4096);
        let a = key(1, 0);
        let out = cache.access_or_insert(page(a, PageSize::DEFAULT, 1));
        assert_eq!(out, AccessOutcome::MissInserted);
        assert!(cache.in_t1(a));
    }

    #[test]
    fn test_request_ghost_trim() {
        let mut cache = ArcCache::new(2, 2 * 4096);
        for pgno in 1..=10 {
            let k = key(pgno, 0);
            let _ = cache.access_or_insert(page(
                k,
                PageSize::DEFAULT,
                u8::try_from(pgno).expect("pgno <= 10 fits in u8"),
            ));
        }
        assert!(
            cache.b1_len() <= 2,
            "bead_id={BEAD_ID} case=ghost_trim_b1_capacity"
        );
    }

    #[test]
    fn test_scan_resistance() {
        let mut cache = ArcCache::new(4, 4 * 4096);
        let hot_a = key(1, 0);
        let hot_b = key(2, 0);

        let _ = cache.access_or_insert(page(hot_a, PageSize::DEFAULT, 0xA1));
        let _ = cache.access_or_insert(page(hot_b, PageSize::DEFAULT, 0xA2));
        assert!(cache.access(hot_a));
        assert!(cache.access(hot_b));

        for pgno in 3..=10 {
            let key = key(pgno, 0);
            let _ = cache.access_or_insert(page(
                key,
                PageSize::DEFAULT,
                u8::try_from(pgno).expect("pgno <= 10 fits in u8"),
            ));
        }

        assert!(cache.contains(hot_a), "bead_id={BEAD_ID} case=scan_hot_a");
        assert!(cache.contains(hot_b), "bead_id={BEAD_ID} case=scan_hot_b");
        assert!(cache.in_t2(hot_a), "bead_id={BEAD_ID} case=scan_hot_a_t2");
        assert!(cache.in_t2(hot_b), "bead_id={BEAD_ID} case=scan_hot_b_t2");
    }

    #[test]
    fn test_pinned_page_not_evicted() {
        let mut cache = ArcCache::new(1, 4096);
        let pinned = key(1, 0);
        let next = key(2, 0);

        let _ = cache.access_or_insert(page(pinned, PageSize::DEFAULT, 0x11));
        cache.get(pinned).expect("pinned page should exist").pin();

        let outcome = cache.access_or_insert(page(next, PageSize::DEFAULT, 0x22));
        assert_eq!(outcome, AccessOutcome::MissInsertedOverflow);
        assert!(cache.contains(pinned));
        assert!(cache.contains(next));
        assert_eq!(cache.capacity_overflow_events(), 1);
    }

    #[test]
    fn test_eviction_no_io() {
        let mut cache = ArcCache::new(2, 2 * 4096);
        for pgno in 1..=8 {
            let key = key(pgno, 0);
            let _ = cache.access_or_insert(page(
                key,
                PageSize::DEFAULT,
                u8::try_from(pgno).expect("pgno <= 8 fits in u8"),
            ));
        }
        assert!(
            cache.evictions() > 0,
            "bead_id={BEAD_ID} case=eviction_no_io_seed"
        );
        assert_eq!(
            cache.io_writes(),
            0,
            "bead_id={BEAD_ID} case=eviction_no_io_counter"
        );
    }

    #[test]
    fn test_superseded_version_preferred() {
        let mut cache = ArcCache::new(2, 2 * 4096);
        let older = key(7, 1);
        let newer = key(7, 2);
        let other = key(8, 1);

        let _ = cache.access_or_insert(page(older, PageSize::DEFAULT, 0x31));
        let _ = cache.access_or_insert(page(newer, PageSize::DEFAULT, 0x32));
        let _ = cache.access_or_insert(page(other, PageSize::DEFAULT, 0x33));

        assert!(
            !cache.contains(older),
            "bead_id={BEAD_ID} case=superseded_evicted"
        );
        assert!(cache.contains(newer));
        assert!(cache.contains(other));
    }

    #[test]
    fn test_memory_accounting() {
        let tiny = PageSize::new(512).expect("valid page size");
        let mut cache = ArcCache::new(2, 1024);
        let a = key(1, 0);
        let b = key(2, 0);
        let c = key(3, 0);

        let _ = cache.access_or_insert(page(a, tiny, 1));
        assert_eq!(cache.total_bytes(), 512);

        let _ = cache.access_or_insert(page(b, tiny, 2));
        assert_eq!(cache.total_bytes(), 1024);

        let _ = cache.access_or_insert(page(c, tiny, 3));
        assert!(
            cache.total_bytes() <= 1024,
            "bead_id={BEAD_ID} case=memory_accounting_max_bytes"
        );
        assert_eq!(cache.total_bytes(), 1024);
    }

    #[test]
    fn test_e2e_arc_cache_behavior_under_mixed_workload() {
        use std::collections::{HashSet, VecDeque};

        #[derive(Default)]
        struct Lru {
            order: VecDeque<u32>,
            set: HashSet<u32>,
            cap: usize,
        }

        impl Lru {
            fn new(cap: usize) -> Self {
                Self {
                    cap,
                    ..Self::default()
                }
            }

            fn request(&mut self, pgno: u32) -> bool {
                if self.set.contains(&pgno) {
                    self.order.retain(|v| *v != pgno);
                    self.order.push_back(pgno);
                    return true;
                }
                if self.order.len() >= self.cap
                    && let Some(victim) = self.order.pop_front()
                {
                    let _ = self.set.remove(&victim);
                }
                self.order.push_back(pgno);
                let _ = self.set.insert(pgno);
                false
            }
        }

        let mut arc = ArcCache::new(4, 4 * 4096);
        let mut lru = Lru::new(4);

        let mut arc_hits = 0usize;
        let mut lru_hits = 0usize;

        // Mixed workload: scans plus recurring hot pages.
        for round in 0..20u32 {
            for pgno in 100..=115 {
                let key = key(pgno, 0);
                if arc.access_or_insert(page(
                    key,
                    PageSize::DEFAULT,
                    u8::try_from(pgno % 256).expect("fits in u8"),
                )) == AccessOutcome::Hit
                {
                    arc_hits += 1;
                }
                if lru.request(pgno) {
                    lru_hits += 1;
                }
            }

            for _ in 0..8 {
                for hot in [1u32, 2u32] {
                    let key = key(hot, 0);
                    if arc.access_or_insert(page(
                        key,
                        PageSize::DEFAULT,
                        u8::try_from((round + hot) % 255).expect("fits in u8"),
                    )) == AccessOutcome::Hit
                    {
                        arc_hits += 1;
                    }
                    if lru.request(hot) {
                        lru_hits += 1;
                    }
                }
            }
        }

        assert!(
            arc_hits > lru_hits,
            "bead_id={BEAD_ID} case=mixed_workload arc_hits={arc_hits} lru_hits={lru_hits}"
        );

        // Drive a deterministic B1 ghost hit to verify `p` adapts upward.
        let p_before = arc.p_target();
        let g1 = key(900, 0);
        let g2 = key(901, 0);
        let g3 = key(902, 0);
        let g4 = key(903, 0);
        let g5 = key(904, 0);
        let _ = arc.access_or_insert(page(g1, PageSize::DEFAULT, 1));
        let _ = arc.access_or_insert(page(g2, PageSize::DEFAULT, 2));
        let _ = arc.access_or_insert(page(g3, PageSize::DEFAULT, 3));
        let _ = arc.access_or_insert(page(g4, PageSize::DEFAULT, 4));
        let _ = arc.access_or_insert(page(g5, PageSize::DEFAULT, 5));
        assert!(
            arc.in_b1(g1),
            "bead_id={BEAD_ID} case=mixed_workload_b1_seed"
        );
        let _ = arc.access_or_insert(page(g1, PageSize::DEFAULT, 6));

        assert!(
            arc.p_target() > p_before,
            "bead_id={BEAD_ID} case=mixed_workload_p_adapts"
        );
    }

    #[test]
    fn test_e2e_arc_mvcc_integration_smoke() {
        let mut cache = ArcCache::new(6, 6 * 4096);

        let a_v1 = key(1, 1);
        let a_v2 = key(1, 2);
        let b_v1 = key(2, 1);
        let c_v1 = key(3, 1);
        let d_v1 = key(4, 1);
        let e_v1 = key(5, 1);

        let _ = cache.access_or_insert(page(a_v1, PageSize::DEFAULT, 0x10));
        let _ = cache.access_or_insert(page(b_v1, PageSize::DEFAULT, 0x20));
        let _ = cache.access_or_insert(page(c_v1, PageSize::DEFAULT, 0x30));
        let _ = cache.access_or_insert(page(d_v1, PageSize::DEFAULT, 0x40));
        let _ = cache.access_or_insert(page(e_v1, PageSize::DEFAULT, 0x50));

        let _ = cache.access_or_insert(page(a_v2, PageSize::DEFAULT, 0x11));
        cache.get(a_v2).expect("a_v2 should exist").pin();

        for pgno in 6..=14 {
            let key = key(pgno, 1);
            let _ = cache.access_or_insert(page(
                key,
                PageSize::DEFAULT,
                u8::try_from(pgno).expect("pgno <= 14 fits in u8"),
            ));
        }

        assert!(cache.contains(a_v2), "pinned newest version must remain");
        assert!(
            cache.total_bytes() <= 6 * 4096,
            "memory accounting should respect max_bytes"
        );
        assert_eq!(cache.io_writes(), 0, "eviction must remain memory-only");
    }

    // ═══════════════════════════════════════════════════════════════════
    // bd-2ttd8.2: Pager invariant suite — ARC structural invariants
    // ═══════════════════════════════════════════════════════════════════

    /// Assert all ARC structural invariants hold for a given cache state.
    fn assert_arc_invariants(cache: &ArcCache) {
        let bead = "bd-2ttd8.2";

        // INV-1: Every indexed page is in exactly T1 xor T2.
        for key in cache.index.keys().copied() {
            let in_t1 = cache.in_t1(key);
            let in_t2 = cache.in_t2(key);
            assert!(
                in_t1 ^ in_t2,
                "bead_id={bead} inv=resident_in_exactly_one key={key:?} in_t1={in_t1} in_t2={in_t2}"
            );
        }

        // INV-2: T1 and T2 entries are all in the index.
        for key in cache.t1.ordered_keys() {
            assert!(
                cache.index.contains_key(&key),
                "bead_id={bead} inv=t1_entry_in_index key={key:?}"
            );
        }
        for key in cache.t2.ordered_keys() {
            assert!(
                cache.index.contains_key(&key),
                "bead_id={bead} inv=t2_entry_in_index key={key:?}"
            );
        }

        // INV-3: |T1| + |T2| == |index|
        assert_eq!(
            cache.t1.len() + cache.t2.len(),
            cache.index.len(),
            "bead_id={bead} inv=resident_count_matches"
        );

        // INV-4: Ghost lists B1/B2 are disjoint from each other.
        for key in cache.b1.ordered_keys() {
            assert!(
                !cache.in_b2(key),
                "bead_id={bead} inv=b1_b2_disjoint key={key:?}"
            );
        }

        // INV-5: Ghost lists are disjoint from resident sets.
        for key in cache.b1.ordered_keys() {
            assert!(
                !cache.index.contains_key(&key),
                "bead_id={bead} inv=b1_not_resident key={key:?}"
            );
        }
        for key in cache.b2.ordered_keys() {
            assert!(
                !cache.index.contains_key(&key),
                "bead_id={bead} inv=b2_not_resident key={key:?}"
            );
        }

        // INV-6: total_bytes == Σ page.byte_size for all indexed pages.
        let computed_bytes: usize = cache.index.values().map(|p| p.byte_size).sum();
        assert_eq!(
            cache.total_bytes, computed_bytes,
            "bead_id={bead} inv=total_bytes_consistent"
        );

        // INV-7: 0 ≤ p ≤ capacity.
        assert!(
            cache.p <= cache.capacity,
            "bead_id={bead} inv=p_in_range p={} capacity={}",
            cache.p,
            cache.capacity
        );

        // INV-8: Ghost lists are bounded (≤ capacity after trim).
        assert!(
            cache.b1.len() <= cache.capacity,
            "bead_id={bead} inv=b1_bounded len={} cap={}",
            cache.b1.len(),
            cache.capacity
        );
        assert!(
            cache.b2.len() <= cache.capacity,
            "bead_id={bead} inv=b2_bounded len={} cap={}",
            cache.b2.len(),
            cache.capacity
        );

        // INV-9: io_writes always zero (eviction is memory-only).
        assert_eq!(
            cache.io_writes, 0,
            "bead_id={bead} inv=eviction_zero_io"
        );
    }

    #[test]
    fn test_inv_pin_unpin_symmetry_single() {
        let mut cache = ArcCache::new(4, 4 * 4096);
        let k = key(1, 0);
        let _ = cache.access_or_insert(page(k, PageSize::DEFAULT, 0xAA));

        let p = cache.get(k).unwrap();
        assert!(!p.is_pinned());

        p.pin();
        assert!(p.is_pinned());
        assert_eq!(p.ref_count.load(std::sync::atomic::Ordering::Relaxed), 1);

        p.unpin();
        assert!(!p.is_pinned());
        assert_eq!(p.ref_count.load(std::sync::atomic::Ordering::Relaxed), 0);
    }

    #[test]
    fn test_inv_pin_unpin_symmetry_multiple() {
        let mut cache = ArcCache::new(4, 4 * 4096);
        let k = key(1, 0);
        let _ = cache.access_or_insert(page(k, PageSize::DEFAULT, 0xBB));

        let p = cache.get(k).unwrap();
        for _ in 0..5 {
            p.pin();
        }
        assert_eq!(p.ref_count.load(std::sync::atomic::Ordering::Relaxed), 5);

        for i in (0..5).rev() {
            p.unpin();
            assert_eq!(
                p.ref_count.load(std::sync::atomic::Ordering::Relaxed),
                i as u32
            );
        }
        assert!(!p.is_pinned());
    }

    #[test]
    fn test_inv_structural_after_sequential_inserts() {
        let mut cache = ArcCache::new(4, 4 * 4096);
        for pgno in 1..=8 {
            let k = key(pgno, 0);
            let _ = cache.access_or_insert(page(
                k,
                PageSize::DEFAULT,
                u8::try_from(pgno).unwrap(),
            ));
            assert_arc_invariants(&cache);
        }
    }

    #[test]
    fn test_inv_structural_after_promotions() {
        let mut cache = ArcCache::new(4, 4 * 4096);
        for pgno in 1..=4 {
            let k = key(pgno, 0);
            let _ = cache.access_or_insert(page(k, PageSize::DEFAULT, pgno as u8));
        }
        // Promote pages 1 and 3 to T2.
        let _ = cache.access(key(1, 0));
        let _ = cache.access(key(3, 0));
        assert_arc_invariants(&cache);

        // Insert more to force evictions.
        for pgno in 5..=10 {
            let k = key(pgno, 0);
            let _ = cache.access_or_insert(page(k, PageSize::DEFAULT, pgno as u8));
            assert_arc_invariants(&cache);
        }
    }

    #[test]
    fn test_inv_structural_after_ghost_hits() {
        let mut cache = ArcCache::new(2, 2 * 4096);
        // Fill → evict → readmit via ghost.
        let a = key(1, 0);
        let b = key(2, 0);
        let c = key(3, 0);

        let _ = cache.access_or_insert(page(a, PageSize::DEFAULT, 1));
        let _ = cache.access_or_insert(page(b, PageSize::DEFAULT, 2));
        assert_arc_invariants(&cache);

        let _ = cache.access_or_insert(page(c, PageSize::DEFAULT, 3)); // evicts a → B1
        assert_arc_invariants(&cache);

        let _ = cache.access_or_insert(page(a, PageSize::DEFAULT, 4)); // B1 hit → T2
        assert_arc_invariants(&cache);
        assert!(cache.in_t2(a));
    }

    #[test]
    fn test_inv_structural_pinned_overflow() {
        let mut cache = ArcCache::new(2, 2 * 4096);
        let a = key(1, 0);
        let b = key(2, 0);

        let _ = cache.access_or_insert(page(a, PageSize::DEFAULT, 1));
        let _ = cache.access_or_insert(page(b, PageSize::DEFAULT, 2));
        cache.get(a).unwrap().pin();
        cache.get(b).unwrap().pin();

        // All pinned → overflow safety valve.
        let c = key(3, 0);
        let outcome = cache.access_or_insert(page(c, PageSize::DEFAULT, 3));
        assert_eq!(outcome, AccessOutcome::MissInsertedOverflow);
        assert_arc_invariants(&cache);
        assert_eq!(cache.len(), 3); // temporary growth
    }

    #[test]
    fn test_inv_mvcc_multi_version_coexistence() {
        let mut cache = ArcCache::new(4, 4 * 4096);
        let pg7_v1 = key(7, 1);
        let pg7_v2 = key(7, 2);
        let pg7_v3 = key(7, 3);

        let _ = cache.access_or_insert(page(pg7_v1, PageSize::DEFAULT, 0x10));
        let _ = cache.access_or_insert(page(pg7_v2, PageSize::DEFAULT, 0x20));
        let _ = cache.access_or_insert(page(pg7_v3, PageSize::DEFAULT, 0x30));

        // All three versions coexist within capacity.
        assert!(cache.contains(pg7_v1));
        assert!(cache.contains(pg7_v2));
        assert!(cache.contains(pg7_v3));
        assert_arc_invariants(&cache);
    }

    #[test]
    fn test_inv_mvcc_superseded_eviction_order() {
        let mut cache = ArcCache::new(3, 3 * 4096);
        let pg1_v1 = key(1, 1);
        let pg1_v2 = key(1, 2);
        let other = key(2, 1);

        let _ = cache.access_or_insert(page(pg1_v1, PageSize::DEFAULT, 0x10));
        let _ = cache.access_or_insert(page(pg1_v2, PageSize::DEFAULT, 0x20));
        let _ = cache.access_or_insert(page(other, PageSize::DEFAULT, 0x30));
        assert_arc_invariants(&cache);

        // Force eviction: v1 should be evicted first (superseded by v2).
        let trigger = key(3, 1);
        let _ = cache.access_or_insert(page(trigger, PageSize::DEFAULT, 0x40));

        assert!(!cache.contains(pg1_v1), "older version should be evicted first");
        assert!(cache.contains(pg1_v2), "newer version should remain");
        assert_arc_invariants(&cache);
    }

    #[test]
    fn test_inv_byte_limit_enforced() {
        let tiny = PageSize::new(512).unwrap();
        let mut cache = ArcCache::new(10, 2048); // 10 entries, but only 2048 bytes (4 × 512)

        for pgno in 1..=8 {
            let k = key(pgno, 0);
            let _ = cache.access_or_insert(page(k, tiny, pgno as u8));
            assert_arc_invariants(&cache);
            // Byte limit should constrain before entry limit.
            assert!(
                cache.total_bytes() <= 2048 || cache.capacity_overflow_events() > 0,
                "bead_id=bd-2ttd8.2 inv=byte_limit_respected total={}",
                cache.total_bytes()
            );
        }
    }

    #[test]
    fn test_inv_data_integrity_after_eviction_readmission() {
        let mut cache = ArcCache::new(2, 2 * 4096);
        let a = key(1, 0);
        let b = key(2, 0);
        let c = key(3, 0);

        let _ = cache.access_or_insert(page(a, PageSize::DEFAULT, 0xAA));
        let _ = cache.access_or_insert(page(b, PageSize::DEFAULT, 0xBB));

        // Evict a.
        let _ = cache.access_or_insert(page(c, PageSize::DEFAULT, 0xCC));
        assert!(!cache.contains(a));

        // Re-insert a with DIFFERENT data.
        let _ = cache.access_or_insert(page(a, PageSize::DEFAULT, 0xDD));
        let readback = cache.get(a).unwrap();
        assert_eq!(
            readback.data.as_slice()[0],
            0xDD,
            "re-admitted page must have new data, not stale"
        );
        assert_arc_invariants(&cache);
    }

    // ─────────────────────────────────────────────────────────────────
    // Property-based tests (proptest)
    // ─────────────────────────────────────────────────────────────────

    mod prop {
        use super::*;
        use proptest::prelude::*;

        /// Represents an operation on the ARC cache.
        #[derive(Debug, Clone)]
        enum ArcOp {
            Insert { pgno: u32, commit_seq: u64, seed: u8 },
            Access { pgno: u32, commit_seq: u64 },
            Pin { pgno: u32, commit_seq: u64 },
            Unpin { pgno: u32, commit_seq: u64 },
        }

        fn arc_op_strategy() -> impl Strategy<Value = ArcOp> {
            prop_oneof![
                4 => (1..20u32, 0..5u64, any::<u8>()).prop_map(|(pgno, cs, seed)| {
                    ArcOp::Insert { pgno, commit_seq: cs, seed }
                }),
                3 => (1..20u32, 0..5u64).prop_map(|(pgno, cs)| {
                    ArcOp::Access { pgno, commit_seq: cs }
                }),
                2 => (1..20u32, 0..5u64).prop_map(|(pgno, cs)| {
                    ArcOp::Pin { pgno, commit_seq: cs }
                }),
                1 => (1..20u32, 0..5u64).prop_map(|(pgno, cs)| {
                    ArcOp::Unpin { pgno, commit_seq: cs }
                }),
            ]
        }

        proptest! {
            #[test]
            fn prop_arc_structural_invariants_hold(
                capacity in 1..8usize,
                ops in proptest::collection::vec(arc_op_strategy(), 1..50)
            ) {
                let max_bytes = capacity * 4096;
                let mut cache = ArcCache::new(capacity, max_bytes);

                for op in &ops {
                    match op {
                        ArcOp::Insert { pgno, commit_seq, seed } => {
                            let k = key(*pgno, *commit_seq);
                            let _ = cache.access_or_insert(page(k, PageSize::DEFAULT, *seed));
                        }
                        ArcOp::Access { pgno, commit_seq } => {
                            let k = key(*pgno, *commit_seq);
                            let _ = cache.access(k);
                        }
                        ArcOp::Pin { pgno, commit_seq } => {
                            let k = key(*pgno, *commit_seq);
                            if let Some(p) = cache.get(k) {
                                p.pin();
                            }
                        }
                        ArcOp::Unpin { pgno, commit_seq } => {
                            let k = key(*pgno, *commit_seq);
                            if let Some(p) = cache.get(k) {
                                if p.is_pinned() {
                                    p.unpin();
                                }
                            }
                        }
                    }
                    assert_arc_invariants(&cache);
                }
            }

            #[test]
            fn prop_pinned_pages_never_evicted(
                capacity in 1..6usize,
                pin_pgno in 1..5u32,
                flood in proptest::collection::vec(5..30u32, 1..30)
            ) {
                let max_bytes = capacity * 4096;
                let mut cache = ArcCache::new(capacity, max_bytes);

                // Insert and pin a page.
                let pinned_key = key(pin_pgno, 0);
                let _ = cache.access_or_insert(page(pinned_key, PageSize::DEFAULT, 0xFF));
                cache.get(pinned_key).unwrap().pin();

                // Flood with other pages to force evictions.
                for pgno in flood {
                    let k = key(pgno, 0);
                    let _ = cache.access_or_insert(page(k, PageSize::DEFAULT, pgno as u8));
                    assert!(
                        cache.contains(pinned_key),
                        "pinned page {pin_pgno} must survive eviction"
                    );
                }
                assert_arc_invariants(&cache);
            }

            #[test]
            fn prop_eviction_metrics_consistent(
                capacity in 2..6usize,
                ops in proptest::collection::vec(1..50u32, 1..40)
            ) {
                let max_bytes = capacity * 4096;
                let mut cache = ArcCache::new(capacity, max_bytes);
                let mut total_inserts = 0usize;

                for pgno in &ops {
                    let k = key(*pgno, 0);
                    let outcome = cache.access_or_insert(page(k, PageSize::DEFAULT, *pgno as u8));
                    if outcome != AccessOutcome::Hit {
                        total_inserts += 1;
                    }
                }

                // Evictions ≤ total inserts (can't evict more than we inserted).
                assert!(
                    cache.evictions() <= total_inserts,
                    "evictions={} > inserts={total_inserts}",
                    cache.evictions()
                );

                // Current resident = inserts - evictions + overflows.
                // (overflow pages are admitted without eviction)
                assert_eq!(
                    cache.len(),
                    total_inserts - cache.evictions() + cache.capacity_overflow_events(),
                    "resident = inserts - evictions + overflows"
                );

                assert_arc_invariants(&cache);
            }
        }
    }
}
