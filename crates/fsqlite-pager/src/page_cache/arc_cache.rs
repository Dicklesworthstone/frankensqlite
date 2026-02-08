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
    MissDroppedAllPinned,
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

        if !self.ensure_room(page.byte_size, from_b2) {
            return AccessOutcome::MissDroppedAllPinned;
        }

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
        AccessOutcome::MissInserted
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

    fn ensure_room(&mut self, incoming_bytes: usize, from_b2: bool) -> bool {
        let mut b2_bias = from_b2;
        while self.index.len() >= self.capacity
            || self.total_bytes.saturating_add(incoming_bytes) > self.max_bytes
        {
            if !self.replace(b2_bias) {
                return false;
            }
            b2_bias = false;
        }
        true
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

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use fsqlite_types::PageSize;

    use super::{AccessOutcome, ArcCache, CacheKey, CachedPage};

    const BEAD_ID: &str = "bd-bt16";

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
        assert_eq!(outcome, AccessOutcome::MissDroppedAllPinned);
        assert!(cache.contains(pinned));
        assert!(!cache.contains(next));
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
}
