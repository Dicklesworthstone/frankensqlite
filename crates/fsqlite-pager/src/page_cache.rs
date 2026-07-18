//! Zero-copy page cache backed by [`PageBufPool`] (§1.5 Mechanical Sympathy, bd-22n.2).
//!
//! The cache stores pages as [`PageBuf`] handles indexed by [`PageNumber`].
//! Page reads go directly from VFS into a pool-allocated buffer with no
//! intermediate heap allocation.  Callers receive `&[u8]` references into
//! the cached buffer — never copies.
//!
//! This module is the *plumbing layer* for zero-copy I/O; the full ARC
//! eviction policy lives in a higher-level module (bd-7pu).
//!
//! # Sharded Page Cache (bd-3wop3.2)
//!
//! [`ShardedPageCache`] partitions the page-number space across a power-of-two
//! shard array, each protected by its own mutex. This eliminates the global lock contention
//! that limited concurrent writer throughput to 8-16 threads.
//!
//! Shard selection uses a multiplicative hash of the page number to ensure
//! good distribution even for sequential page access patterns (common during
//! B-tree scans). Each shard is cache-line aligned (64 bytes) to prevent
//! false sharing between adjacent shards.

use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

#[cfg(target_arch = "x86_64")]
use core::intrinsics::prefetch_read_data;

use fsqlite_error::{FrankenError, Result};
use fsqlite_observability::PageCacheEfficiencySnapshot;
use fsqlite_types::cx::Cx;
use fsqlite_types::sync_primitives::Mutex;
use fsqlite_types::{PageData, PageNumber, PageSize};
use fsqlite_vfs::VfsFile;

use crate::arc_cache::{ArcEvictionModel, CacheLookup};
use crate::page_buf::{PageBuf, PageBufPool};
use crate::s3_fifo::{QueueKind, S3Fifo, S3FifoConfig, S3FifoEvent};
use crate::thompson_partitioner::ThompsonPartitioner;

#[cfg(target_arch = "x86_64")]
#[inline]
fn prefetch_l1_read<T>(ptr: *const T) {
    if ptr.is_null() {
        return;
    }

    prefetch_read_data::<T, 3>(ptr);
}

#[cfg(not(target_arch = "x86_64"))]
#[inline]
fn prefetch_l1_read<T>(_ptr: *const T) {}

// ---------------------------------------------------------------------------
// Page buffer pool sizing
// ---------------------------------------------------------------------------

/// Default maximum number of page buffers when no explicit configuration is
/// provided.  At the standard 4 KiB page size this corresponds to **1 GiB** of
/// buffer memory.
///
/// The previous default was 65 536 (256 MiB), which proved insufficient for
/// multi-GiB databases with several indexed tables — normal B-tree operations
/// during INSERT transactions could exhaust the pool and surface
/// [`FrankenError::OutOfMemory`].
///
/// This value can be overridden at runtime via the `FSQLITE_PAGE_BUFFER_MAX`
/// environment variable, or programmatically through
/// [`PageCache::with_max_buffers`] / [`ShardedPageCache::with_max_buffers`].
pub const DEFAULT_PAGE_BUFFER_MAX: usize = 262_144;

fn default_page_buffer_max_from_env() -> usize {
    static DEFAULT_FROM_ENV: OnceLock<usize> = OnceLock::new();
    *DEFAULT_FROM_ENV.get_or_init(|| {
        std::env::var("FSQLITE_PAGE_BUFFER_MAX")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(DEFAULT_PAGE_BUFFER_MAX)
    })
}

/// Resolve the page-buffer-pool ceiling to use for a new cache.
///
/// Resolution order:
/// 1. If `explicit` is `Some`, use that value.
/// 2. If the `FSQLITE_PAGE_BUFFER_MAX` environment variable is set to a valid
///    `usize`, use that.
/// 3. Otherwise fall back to [`DEFAULT_PAGE_BUFFER_MAX`].
///
/// A value of `0` is silently promoted to `1` (a zero-capacity pool would be
/// useless).
pub fn resolve_page_buffer_max(explicit: Option<usize>) -> usize {
    let raw = explicit.unwrap_or_else(default_page_buffer_max_from_env);
    raw.max(1)
}

// ---------------------------------------------------------------------------
// PageCache
// ---------------------------------------------------------------------------

/// Cheap cache counters (no per-slot iteration, no eviction-policy lock).
///
/// Paired with [`ShardedPageCache::metrics_lightweight_snapshot`] for
/// hot-path callers (e.g. the e-process oracle) that only need the
/// aggregate hit/miss counters. The fields carry the same meaning as the
/// first six fields of [`PageCacheMetricsSnapshot`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PageCacheLightweightSnapshot {
    pub hits: u64,
    pub misses: u64,
    pub admits: u64,
    pub evictions: u64,
    pub cached_pages: usize,
    pub pool_capacity: usize,
}

impl PageCacheLightweightSnapshot {
    #[must_use]
    pub fn total_accesses(self) -> u64 {
        self.hits.saturating_add(self.misses)
    }

    #[must_use]
    pub fn hit_rate_percent(self) -> f64 {
        let total = self.total_accesses();
        if total == 0 {
            0.0
        } else {
            (self.hits as f64 * 100.0) / total as f64
        }
    }
}

/// Point-in-time page-cache counters and gauges.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PageCacheMetricsSnapshot {
    /// Number of successful cache probes.
    pub hits: u64,
    /// Number of failed cache probes.
    pub misses: u64,
    /// Number of fresh pages admitted into the cache.
    pub admits: u64,
    /// Number of pages evicted from the cache.
    pub evictions: u64,
    /// Number of pages currently resident in the cache.
    pub cached_pages: usize,
    /// Configured buffer-pool capacity (max page buffers).
    pub pool_capacity: usize,
    /// Percent of cached pages currently dirty (0-100).
    pub dirty_ratio_pct: u64,
    /// Adaptive cache "recent" list size (ARC-compatible gauge).
    pub t1_size: usize,
    /// Adaptive cache "frequent" list size (ARC-compatible gauge).
    pub t2_size: usize,
    /// ARC ghost list B1 size (compatibility gauge).
    pub b1_size: usize,
    /// ARC ghost list B2 size (compatibility gauge).
    pub b2_size: usize,
    /// ARC adaptive target parameter P (compatibility gauge).
    pub p_target: usize,
    /// Number of pages that currently have multiple visible MVCC versions.
    pub mvcc_multi_version_pages: usize,
}

/// Observer-only queue classification for a resident cache page.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageCacheQueueKind {
    T1,
    T2,
    B1,
    B2,
}

impl PageCacheQueueKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::T1 => "t1",
            Self::T2 => "t2",
            Self::B1 => "b1",
            Self::B2 => "b2",
        }
    }
}

/// Point-in-time diagnostics for one resident cache page.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PageCachePageSnapshot {
    pub page_no: PageNumber,
    /// Present when the cache implementation exposes multiple resident MVCC
    /// versions of the same logical page. The active page cache keeps only the
    /// published image, so this is currently `None`.
    pub version_txn_id: Option<u64>,
    pub queue: Option<PageCacheQueueKind>,
    pub dirty: bool,
    pub ref_count: u32,
    pub access_count: u64,
}

impl PageCacheMetricsSnapshot {
    /// Total cache probes (`hits + misses`).
    #[must_use]
    pub fn total_accesses(self) -> u64 {
        self.hits.saturating_add(self.misses)
    }

    /// Hit-rate as a percentage in `[0.0, 100.0]`.
    #[must_use]
    pub fn hit_rate_percent(self) -> f64 {
        let total = self.total_accesses();
        if total == 0 {
            0.0
        } else {
            (self.hits as f64 * 100.0) / total as f64
        }
    }

    /// Convert the raw pager counters into the shared observability snapshot.
    #[must_use]
    pub fn efficiency_snapshot(self) -> PageCacheEfficiencySnapshot {
        PageCacheEfficiencySnapshot {
            hits: self.hits,
            misses: self.misses,
            admits: self.admits,
            evictions: self.evictions,
            cached_pages: self.cached_pages,
            pool_capacity: self.pool_capacity,
            dirty_ratio_pct: self.dirty_ratio_pct,
            t1_size: self.t1_size,
            t2_size: self.t2_size,
            b1_size: self.b1_size,
            b2_size: self.b2_size,
            p_target: self.p_target,
            mvcc_multi_version_pages: self.mvcc_multi_version_pages,
        }
    }
}

/// Eviction policy used by [`PageCache`] and [`ShardedPageCache`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum PageCacheEvictionPolicy {
    /// Keep the existing best-effort arbitrary victim selection.
    #[default]
    Arbitrary,
    /// Reconstruct an S3-FIFO queue state from recent accesses and use it to
    /// choose the next victim.
    S3Fifo(S3FifoConfig),
    /// Reconstruct an ARC (Adaptive Replacement Cache) ordering from recent
    /// accesses and use its REPLACE rule to choose the next victim. ARC keeps a
    /// recency list (T1) and a frequency list (T2) with ghost history, giving
    /// strong scan resistance — see [`crate::arc_cache::ArcEvictionModel`].
    Arc(ArcEvictionConfig),
    /// S3-FIFO whose small/main (hot-partition) split is tuned online by a
    /// Thompson-sampling bandit ([`crate::thompson_partitioner::ThompsonPartitioner`])
    /// from cache hit/miss feedback. Starts from the given config's split and
    /// adapts the small-queue size toward whichever ratio yields more hits.
    S3FifoAdaptive(S3FifoConfig),
}

/// Configuration for the ARC eviction policy ([`PageCacheEvictionPolicy::Arc`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArcEvictionConfig {
    capacity: usize,
}

impl ArcEvictionConfig {
    /// Create an ARC eviction config with the given prototype capacity. The
    /// capacity is a sizing hint for the reconstructed model and access-trace
    /// bound; the live capacity used per eviction is the current resident count.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
        }
    }

    /// Prototype capacity hint.
    #[must_use]
    pub fn capacity(self) -> usize {
        self.capacity
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct S3FifoQueueSnapshot {
    small_len: usize,
    main_len: usize,
    ghost_len: usize,
    small_capacity: usize,
}

const S3_FIFO_RECONSTRUCTED_EVICTION_MAX_RESIDENTS: usize = 4_096;
const S3_FIFO_RECONSTRUCTED_EVICTION_MAX_TRACE_ENTRIES: usize = 8_192;

#[derive(Debug, Clone)]
struct S3FifoEvictionTracker {
    config: S3FifoConfig,
    adaptation_interval: usize,
    adaptive_bounds: (usize, usize),
    access_trace: VecDeque<PageNumber>,
    max_trace_entries: usize,
}

impl S3FifoEvictionTracker {
    fn new(config: S3FifoConfig) -> Self {
        let probe = S3Fifo::with_config(config);
        let max_trace_entries = config
            .capacity()
            .saturating_mul(8)
            .clamp(64, S3_FIFO_RECONSTRUCTED_EVICTION_MAX_TRACE_ENTRIES);
        Self {
            config,
            adaptation_interval: probe.adaptation_interval(),
            adaptive_bounds: probe.adaptive_bounds(),
            access_trace: VecDeque::with_capacity(max_trace_entries),
            max_trace_entries,
        }
    }

    fn record_access(&mut self, page_no: PageNumber) {
        if self.access_trace.len() >= self.max_trace_entries {
            let _ = self.access_trace.pop_front();
        }
        self.access_trace.push_back(page_no);
    }

    fn record_admit(&mut self, page_no: PageNumber) {
        self.record_access(page_no);
    }

    fn forget(&mut self, page_no: PageNumber) {
        if self.access_trace.len() > S3_FIFO_RECONSTRUCTED_EVICTION_MAX_RESIDENTS {
            return;
        }
        self.access_trace.retain(|candidate| *candidate != page_no);
    }

    fn clear_history(&mut self) {
        self.access_trace.clear();
    }

    fn choose_victim(&self, resident_pages: &[PageNumber]) -> Option<PageNumber> {
        let resident_set: HashSet<PageNumber> = resident_pages.iter().copied().collect();
        let mut model = self.build_model(resident_pages)?;
        let synthetic_miss = choose_synthetic_miss_page(&resident_set)?;
        let events = model.insert(synthetic_miss);
        events.iter().find_map(|event| match event {
            S3FifoEvent::EvictedFromSmallToGhost(page_no)
            | S3FifoEvent::EvictedFromMain(page_no)
                if resident_set.contains(page_no) =>
            {
                Some(*page_no)
            }
            _ => None,
        })
    }

    fn queue_snapshot(&self, resident_pages: &[PageNumber]) -> Option<S3FifoQueueSnapshot> {
        let resident_set: HashSet<PageNumber> = resident_pages.iter().copied().collect();
        let model = self.build_model(resident_pages)?;
        Some(S3FifoQueueSnapshot {
            small_len: model
                .small_pages()
                .into_iter()
                .filter(|page_no| resident_set.contains(page_no))
                .count(),
            main_len: model
                .main_pages()
                .into_iter()
                .filter(|page_no| resident_set.contains(page_no))
                .count(),
            ghost_len: model
                .ghost_pages()
                .into_iter()
                .filter(|page_no| !resident_set.contains(page_no))
                .count(),
            small_capacity: model.config().small_capacity(),
        })
    }

    fn queue_assignments(
        &self,
        resident_pages: &[PageNumber],
    ) -> HashMap<PageNumber, PageCacheQueueKind> {
        let Some(model) = self.build_model(resident_pages) else {
            return HashMap::new();
        };

        resident_pages
            .iter()
            .filter_map(|page_no| {
                let queue = match model.lookup(*page_no) {
                    Some(location) => match location.kind {
                        QueueKind::Small => Some(PageCacheQueueKind::T1),
                        QueueKind::Main => Some(PageCacheQueueKind::T2),
                        QueueKind::Ghost => Some(PageCacheQueueKind::B1),
                    },
                    None => None,
                }?;
                Some((*page_no, queue))
            })
            .collect()
    }

    fn build_model(&self, resident_pages: &[PageNumber]) -> Option<S3Fifo> {
        if resident_pages.is_empty()
            || resident_pages.len() > S3_FIFO_RECONSTRUCTED_EVICTION_MAX_RESIDENTS
        {
            return None;
        }

        let resident_set: HashSet<PageNumber> = resident_pages.iter().copied().collect();
        let mut resident_order = resident_pages.to_vec();
        resident_order.sort_unstable_by_key(|page_no| page_no.get());

        let mut model = S3Fifo::with_config(self.scaled_config(resident_pages.len()));
        model.set_adaptation_interval(self.adaptation_interval);
        let (min_bound, max_bound) = self.scaled_bounds(resident_pages.len());
        model.set_adaptive_bounds(min_bound, max_bound);

        for &page_no in &self.access_trace {
            if !resident_set.contains(&page_no) {
                continue;
            }
            if !model.access(page_no) {
                let _ = model.insert(page_no);
            }
        }

        let mut remaining_rounds = resident_order.len().saturating_mul(2).max(1);
        while remaining_rounds > 0 {
            let missing: Vec<PageNumber> = resident_order
                .iter()
                .copied()
                .filter(|page_no| {
                    !matches!(
                        model.lookup(*page_no),
                        Some(location) if location.kind != QueueKind::Ghost
                    )
                })
                .collect();
            if missing.is_empty() {
                break;
            }
            for page_no in missing {
                let _ = model.insert(page_no);
            }
            remaining_rounds = remaining_rounds.saturating_sub(1);
        }

        Some(model)
    }

    fn scaled_config(&self, resident_pages: usize) -> S3FifoConfig {
        let capacity = resident_pages.max(1);
        let prototype_capacity = self.config.capacity().max(1);
        let small_capacity = scale_nonzero_for_eviction_policy(
            self.config.small_capacity(),
            prototype_capacity,
            capacity,
        )
        .clamp(1, capacity);
        let ghost_capacity = scale_nonzero_for_eviction_policy(
            self.config.ghost_capacity(),
            prototype_capacity,
            capacity,
        )
        .max(1);
        S3FifoConfig::with_limits(
            capacity,
            small_capacity,
            ghost_capacity,
            self.config.max_reinsert(),
        )
    }

    fn scaled_bounds(&self, resident_pages: usize) -> (usize, usize) {
        let capacity = resident_pages.max(1);
        let prototype_capacity = self.config.capacity().max(1);
        let min_bound =
            scale_nonzero_for_eviction_policy(self.adaptive_bounds.0, prototype_capacity, capacity)
                .clamp(1, capacity);
        let max_bound =
            scale_nonzero_for_eviction_policy(self.adaptive_bounds.1, prototype_capacity, capacity)
                .clamp(min_bound, capacity);
        (min_bound, max_bound)
    }
}

/// Reconstruct-from-trace ARC victim selector (bd-q7zls).
///
/// Mirrors [`S3FifoEvictionTracker`]: keeps a bounded access trace and, on
/// [`Self::choose_victim`], replays it into a data-less [`ArcEvictionModel`]
/// scaled to the current resident set, then applies ARC's REPLACE rule. This
/// reuses the real ARC algorithm without allocating page-data buffers per
/// eviction.
#[derive(Debug, Clone)]
struct ArcEvictionTracker {
    config: ArcEvictionConfig,
    access_trace: VecDeque<PageNumber>,
    max_trace_entries: usize,
}

impl ArcEvictionTracker {
    fn new(config: ArcEvictionConfig) -> Self {
        let max_trace_entries = config
            .capacity()
            .saturating_mul(8)
            .clamp(64, S3_FIFO_RECONSTRUCTED_EVICTION_MAX_TRACE_ENTRIES);
        Self {
            config,
            access_trace: VecDeque::with_capacity(max_trace_entries),
            max_trace_entries,
        }
    }

    fn record_access(&mut self, page_no: PageNumber) {
        if self.access_trace.len() >= self.max_trace_entries {
            let _ = self.access_trace.pop_front();
        }
        self.access_trace.push_back(page_no);
    }

    fn record_admit(&mut self, page_no: PageNumber) {
        self.record_access(page_no);
    }

    fn forget(&mut self, page_no: PageNumber) {
        if self.access_trace.len() > S3_FIFO_RECONSTRUCTED_EVICTION_MAX_RESIDENTS {
            return;
        }
        self.access_trace.retain(|candidate| *candidate != page_no);
    }

    fn clear_history(&mut self) {
        self.access_trace.clear();
    }

    fn choose_victim(&self, resident_pages: &[PageNumber]) -> Option<PageNumber> {
        if resident_pages.is_empty()
            || resident_pages.len() > S3_FIFO_RECONSTRUCTED_EVICTION_MAX_RESIDENTS
        {
            return None;
        }
        let resident_set: HashSet<PageNumber> = resident_pages.iter().copied().collect();
        let mut model = ArcEvictionModel::new(resident_pages.len());

        // Replay the recorded trace (resident pages only) to rebuild ARC's
        // recency (T1) / frequency (T2) split: pages touched 2+ times promote to
        // T2 and are protected; single-touch pages stay in the recency lane.
        for &page_no in &self.access_trace {
            if !resident_set.contains(&page_no) {
                continue;
            }
            let lookup = model.request(page_no);
            if !matches!(lookup, CacheLookup::Hit) {
                model.admit(page_no, lookup);
            }
        }

        // Ensure every current resident is represented, in deterministic order.
        let mut resident_order = resident_pages.to_vec();
        resident_order.sort_unstable_by_key(|page_no| page_no.get());
        for &page_no in &resident_order {
            if !model.contains_resident(page_no) {
                let lookup = model.request(page_no);
                if !matches!(lookup, CacheLookup::Hit) {
                    model.admit(page_no, lookup);
                }
            }
        }

        let victim = model.peek_victim()?;
        resident_set.contains(&victim).then_some(victim)
    }
}

/// S3-FIFO eviction whose small/main (hot-partition) split is tuned online by a
/// Thompson-sampling bandit (bd-5ftij). The bandit rewards the current ratio on
/// cache hits and penalises it on misses; at each resample boundary the S3-FIFO
/// small-queue size is retargeted to the winning ratio. This makes
/// [`ThompsonPartitioner`] a live consumer of the cache hit/miss signal.
#[derive(Debug, Clone)]
struct S3FifoAdaptiveTracker {
    inner: S3FifoEvictionTracker,
    thompson: ThompsonPartitioner,
}

impl S3FifoAdaptiveTracker {
    fn new(config: S3FifoConfig) -> Self {
        Self {
            inner: S3FifoEvictionTracker::new(config),
            thompson: ThompsonPartitioner::new(),
        }
    }

    /// Feed the bandit one cache outcome and, on a resample boundary, retarget
    /// the S3-FIFO small-queue size to the newly selected hot-partition ratio.
    fn observe_outcome(&mut self, hit: bool) {
        self.thompson.record_outcome(hit);
        if self.thompson.tick() {
            self.apply_current_ratio();
        }
    }

    fn apply_current_ratio(&mut self) {
        let capacity = self.inner.config.capacity().max(1);
        let ratio = self.thompson.current_hot_ratio();
        #[allow(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            clippy::cast_precision_loss
        )]
        let small = (ratio * capacity as f64).round() as usize;
        let small = small.clamp(1, capacity);
        self.inner.config = S3FifoConfig::with_limits(
            capacity,
            small,
            self.inner.config.ghost_capacity(),
            self.inner.config.max_reinsert(),
        );
    }

    fn record_access(&mut self, page_no: PageNumber) {
        self.inner.record_access(page_no);
        self.observe_outcome(true);
    }

    fn record_admit(&mut self, page_no: PageNumber) {
        self.inner.record_admit(page_no);
        self.observe_outcome(false);
    }

    fn forget(&mut self, page_no: PageNumber) {
        self.inner.forget(page_no);
    }

    fn clear_history(&mut self) {
        self.inner.clear_history();
    }

    fn choose_victim(&self, resident_pages: &[PageNumber]) -> Option<PageNumber> {
        self.inner.choose_victim(resident_pages)
    }

    fn queue_snapshot(&self, resident_pages: &[PageNumber]) -> Option<S3FifoQueueSnapshot> {
        self.inner.queue_snapshot(resident_pages)
    }

    fn queue_assignments(
        &self,
        resident_pages: &[PageNumber],
    ) -> std::collections::HashMap<PageNumber, PageCacheQueueKind> {
        self.inner.queue_assignments(resident_pages)
    }
}

#[derive(Debug, Clone, Default)]
enum PageCacheEvictionTracker {
    #[default]
    Arbitrary,
    S3Fifo(S3FifoEvictionTracker),
    Arc(ArcEvictionTracker),
    S3FifoAdaptive(S3FifoAdaptiveTracker),
}

impl PageCacheEvictionTracker {
    fn from_policy(policy: PageCacheEvictionPolicy) -> Self {
        match policy {
            PageCacheEvictionPolicy::Arbitrary => Self::Arbitrary,
            PageCacheEvictionPolicy::S3Fifo(config) => {
                Self::S3Fifo(S3FifoEvictionTracker::new(config))
            }
            PageCacheEvictionPolicy::Arc(config) => Self::Arc(ArcEvictionTracker::new(config)),
            PageCacheEvictionPolicy::S3FifoAdaptive(config) => {
                Self::S3FifoAdaptive(S3FifoAdaptiveTracker::new(config))
            }
        }
    }

    fn policy(&self) -> PageCacheEvictionPolicy {
        match self {
            Self::Arbitrary => PageCacheEvictionPolicy::Arbitrary,
            Self::S3Fifo(tracker) => PageCacheEvictionPolicy::S3Fifo(tracker.config),
            Self::Arc(tracker) => PageCacheEvictionPolicy::Arc(tracker.config),
            Self::S3FifoAdaptive(tracker) => {
                PageCacheEvictionPolicy::S3FifoAdaptive(tracker.inner.config)
            }
        }
    }

    fn set_policy(&mut self, policy: PageCacheEvictionPolicy) {
        *self = Self::from_policy(policy);
    }

    fn record_access(&mut self, page_no: PageNumber) {
        match self {
            Self::Arbitrary => {}
            Self::S3Fifo(tracker) => tracker.record_access(page_no),
            Self::Arc(tracker) => tracker.record_access(page_no),
            Self::S3FifoAdaptive(tracker) => tracker.record_access(page_no),
        }
    }

    fn record_admit(&mut self, page_no: PageNumber) {
        match self {
            Self::Arbitrary => {}
            Self::S3Fifo(tracker) => tracker.record_admit(page_no),
            Self::Arc(tracker) => tracker.record_admit(page_no),
            Self::S3FifoAdaptive(tracker) => tracker.record_admit(page_no),
        }
    }

    fn forget(&mut self, page_no: PageNumber) {
        match self {
            Self::Arbitrary => {}
            Self::S3Fifo(tracker) => tracker.forget(page_no),
            Self::Arc(tracker) => tracker.forget(page_no),
            Self::S3FifoAdaptive(tracker) => tracker.forget(page_no),
        }
    }

    fn clear_history(&mut self) {
        match self {
            Self::Arbitrary => {}
            Self::S3Fifo(tracker) => tracker.clear_history(),
            Self::Arc(tracker) => tracker.clear_history(),
            Self::S3FifoAdaptive(tracker) => tracker.clear_history(),
        }
    }

    fn choose_victim(&self, resident_pages: &[PageNumber]) -> Option<PageNumber> {
        match self {
            Self::Arbitrary => None,
            Self::S3Fifo(tracker) => tracker.choose_victim(resident_pages),
            Self::Arc(tracker) => tracker.choose_victim(resident_pages),
            Self::S3FifoAdaptive(tracker) => tracker.choose_victim(resident_pages),
        }
    }

    fn queue_snapshot(&self, resident_pages: &[PageNumber]) -> Option<S3FifoQueueSnapshot> {
        match self {
            Self::Arbitrary | Self::Arc(_) => None,
            Self::S3Fifo(tracker) => tracker.queue_snapshot(resident_pages),
            Self::S3FifoAdaptive(tracker) => tracker.queue_snapshot(resident_pages),
        }
    }

    fn queue_assignments(
        &self,
        resident_pages: &[PageNumber],
    ) -> std::collections::HashMap<PageNumber, PageCacheQueueKind> {
        match self {
            Self::Arbitrary | Self::Arc(_) => std::collections::HashMap::new(),
            Self::S3Fifo(tracker) => tracker.queue_assignments(resident_pages),
            Self::S3FifoAdaptive(tracker) => tracker.queue_assignments(resident_pages),
        }
    }
}

fn scale_nonzero_for_eviction_policy(
    value: usize,
    from_capacity: usize,
    to_capacity: usize,
) -> usize {
    if value == 0 || to_capacity == 0 {
        return 0;
    }
    let numerator = value.saturating_mul(to_capacity);
    numerator.saturating_add(from_capacity.saturating_sub(1)) / from_capacity.max(1)
}

fn choose_synthetic_miss_page(resident_pages: &HashSet<PageNumber>) -> Option<PageNumber> {
    let mut candidate = u32::MAX - 1;
    loop {
        let page_no = PageNumber::new(candidate)?;
        if !resident_pages.contains(&page_no) {
            return Some(page_no);
        }
        if candidate == 1 {
            return None;
        }
        candidate = candidate.saturating_sub(1);
    }
}

/// Simple page cache: `PageNumber → PageBuf`.
///
/// All buffers are drawn from a shared [`PageBufPool`].  On eviction the
/// backing allocation is returned to the pool for reuse, avoiding hot-path
/// heap allocations.
///
/// The cache does **not** implement an eviction policy — that is the
/// responsibility of the higher-level ARC cache (§6).  This type is the
/// low-level storage layer that proves the zero-copy invariant.
pub struct PageCache {
    pool: PageBufPool,
    pages: std::collections::HashMap<PageNumber, PageBuf, foldhash::fast::FixedState>,
    page_size: PageSize,
    hits: Cell<u64>,
    misses: Cell<u64>,
    admits: Cell<u64>,
    evictions: Cell<u64>,
    eviction_policy: RefCell<PageCacheEvictionTracker>,
}

impl PageCache {
    /// Create a new, empty `PageCache` configured for the given `page_size`.
    ///
    /// The buffer-pool ceiling is determined by
    /// [`resolve_page_buffer_max`] — i.e. the `FSQLITE_PAGE_BUFFER_MAX`
    /// environment variable if set, otherwise [`DEFAULT_PAGE_BUFFER_MAX`]
    /// (262 144 buffers ≈ 1 GiB at 4 KiB pages).
    pub fn new(page_size: PageSize) -> Self {
        Self::with_max_buffers(page_size, resolve_page_buffer_max(None))
    }

    /// Create a new, empty `PageCache` with an explicit buffer-pool ceiling.
    ///
    /// `max_buffers` is the maximum number of live page buffers (idle + in-use)
    /// the underlying [`PageBufPool`] will allow.  Once the bound is reached,
    /// further buffer acquisitions fail with [`FrankenError::OutOfMemory`].
    pub fn with_max_buffers(page_size: PageSize, max_buffers: usize) -> Self {
        Self::with_pool(PageBufPool::new(page_size, max_buffers), page_size)
    }

    /// Create a new `PageCache` using an existing `PageBufPool`.
    pub fn with_pool(pool: PageBufPool, page_size: PageSize) -> Self {
        Self {
            pool,
            pages: std::collections::HashMap::with_hasher(foldhash::fast::FixedState::default()),
            page_size,
            hits: Cell::new(0),
            misses: Cell::new(0),
            admits: Cell::new(0),
            evictions: Cell::new(0),
            eviction_policy: RefCell::new(PageCacheEvictionTracker::default()),
        }
    }

    /// Access the underlying page pool.
    pub fn pool(&self) -> &PageBufPool {
        &self.pool
    }

    /// Set the eviction policy used by [`Self::evict_any`].
    pub fn set_eviction_policy(&self, policy: PageCacheEvictionPolicy) {
        self.eviction_policy.borrow_mut().set_policy(policy);
    }

    /// Return the current eviction policy.
    #[must_use]
    pub fn eviction_policy(&self) -> PageCacheEvictionPolicy {
        self.eviction_policy.borrow().policy()
    }

    /// Number of pages currently in the cache.
    pub fn len(&self) -> usize {
        self.pages.len()
    }

    /// Whether the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.pages.is_empty()
    }

    /// Retrieve a page from the cache, updating eviction metrics.
    pub fn get(&self, page_no: PageNumber) -> Option<&[u8]> {
        if let Some(page) = self.pages.get(&page_no) {
            self.hits.set(self.hits.get().saturating_add(1));
            self.eviction_policy.borrow_mut().record_access(page_no);
            Some(page.as_slice())
        } else {
            self.misses.set(self.misses.get().saturating_add(1));
            None
        }
    }

    /// Get a mutable reference to a cached page.
    ///
    /// Returns `None` if the page is not in the cache.  Callers can modify
    /// the page in place; the dirty-tracking flag is managed by the higher
    /// layer.
    #[inline]
    pub fn get_mut(&mut self, page_no: PageNumber) -> Option<&mut [u8]> {
        if self.pages.contains_key(&page_no) {
            self.hits.set(self.hits.get().saturating_add(1));
            self.eviction_policy.borrow_mut().record_access(page_no);
            self.pages.get_mut(&page_no).map(PageBuf::as_mut_slice)
        } else {
            self.misses.set(self.misses.get().saturating_add(1));
            None
        }
    }

    /// Returns `true` if the page is present in the cache.
    #[inline]
    #[must_use]
    pub fn contains(&self, page_no: PageNumber) -> bool {
        self.pages.contains_key(&page_no)
    }

    // --- Read / Write through VFS ---

    /// Read a page from a VFS file into the cache.
    ///
    /// If the page is already cached, this is a no-op and returns the
    /// existing reference.  Otherwise a buffer is acquired from the pool,
    /// the page is read directly into it via [`VfsFile::read`], and a
    /// reference to the cached data is returned.
    ///
    /// **Zero-copy guarantee:** the buffer passed to `VfsFile::read` is the
    /// same memory that the returned `&[u8]` points into.
    pub fn read_page(
        &mut self,
        cx: &Cx,
        file: &mut impl VfsFile,
        page_no: PageNumber,
    ) -> Result<&[u8]> {
        if self.contains(page_no) {
            self.eviction_policy.borrow_mut().record_access(page_no);
        } else {
            let mut buf = self.pool.acquire()?;
            let offset = page_offset(page_no, self.page_size);
            let bytes_read = file.read(cx, buf.as_mut_slice(), offset)?;
            if bytes_read < self.page_size.as_usize() {
                return Err(fsqlite_error::FrankenError::DatabaseCorrupt {
                    detail: format!(
                        "short read fetching page {page}: got {bytes_read} of {page_size}",
                        page = page_no.get(),
                        page_size = self.page_size.as_usize()
                    ),
                });
            }
            self.pages.insert(page_no, buf);
            self.admits.set(self.admits.get().saturating_add(1));
            self.eviction_policy.borrow_mut().record_admit(page_no);
        }
        // SAFETY (logical): we just ensured the key exists above.
        Ok(self.pages.get(&page_no).expect("just inserted").as_slice())
    }

    /// Write a cached page out to a VFS file.
    ///
    /// The page data is written directly from the pool-allocated buffer —
    /// no intermediate staging copy.
    ///
    /// Returns `Err` if the page is not in the cache.
    pub fn write_page(&self, cx: &Cx, file: &mut impl VfsFile, page_no: PageNumber) -> Result<()> {
        let Some(buf) = self.pages.get(&page_no) else {
            self.misses.set(self.misses.get().saturating_add(1));
            return Err(fsqlite_error::FrankenError::internal(format!(
                "page {} not in cache",
                page_no
            )));
        };
        self.hits.set(self.hits.get().saturating_add(1));
        self.eviction_policy.borrow_mut().record_access(page_no);
        let offset = page_offset(page_no, self.page_size);
        file.write(cx, buf.as_slice(), offset)?;
        Ok(())
    }

    /// Insert a fresh (zeroed) page into the cache.
    ///
    /// Returns a mutable reference so the caller can populate it.
    pub fn insert_fresh(&mut self, page_no: PageNumber) -> Result<&mut [u8]> {
        // Freshly acquired buffers from the pool may contain stale data.
        // Zero the buffer to match the "new page" semantics.
        let mut buf = self.pool.acquire()?;
        buf.as_mut_slice().fill(0);
        let admitted_new = !self.pages.contains_key(&page_no);
        if admitted_new {
            self.eviction_policy.borrow_mut().record_admit(page_no);
        } else {
            self.eviction_policy.borrow_mut().record_access(page_no);
        }

        let out = match self.pages.entry(page_no) {
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                entry.insert(buf);
                entry.into_mut().as_mut_slice()
            }
            std::collections::hash_map::Entry::Vacant(entry) => entry.insert(buf).as_mut_slice(),
        };
        if admitted_new {
            self.admits.set(self.admits.get().saturating_add(1));
        }
        Ok(out)
    }

    /// Directly insert an existing `PageBuf` into the cache.
    pub fn insert_buffer(&mut self, page_no: PageNumber, buf: PageBuf) {
        let admitted_new = !self.pages.contains_key(&page_no);
        if admitted_new {
            self.eviction_policy.borrow_mut().record_admit(page_no);
        } else {
            self.eviction_policy.borrow_mut().record_access(page_no);
        }
        let admitted_new = match self.pages.entry(page_no) {
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                entry.insert(buf);
                false
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(buf);
                true
            }
        };
        if admitted_new {
            self.admits.set(self.admits.get().saturating_add(1));
        }
    }

    // --- Eviction ---

    /// Evict a page from the cache, returning its buffer to the pool.
    ///
    /// Returns `true` if the page was present.
    pub fn evict(&mut self, page_no: PageNumber) -> bool {
        // Dropping the PageBuf returns it to the pool via Drop impl.
        let removed = self.pages.remove(&page_no).is_some();
        if removed {
            self.evictions.set(self.evictions.get().saturating_add(1));
            self.eviction_policy.borrow_mut().forget(page_no);
        }
        removed
    }

    /// Evict an arbitrary page from the cache to free up space.
    ///
    /// Returns `true` if a page was evicted, `false` if the cache was empty.
    pub fn evict_any(&mut self) -> bool {
        let policy = self.eviction_policy.borrow().policy();
        let key = match policy {
            PageCacheEvictionPolicy::S3Fifo(_)
            | PageCacheEvictionPolicy::Arc(_)
            | PageCacheEvictionPolicy::S3FifoAdaptive(_)
                if self.pages.len() <= S3_FIFO_RECONSTRUCTED_EVICTION_MAX_RESIDENTS =>
            {
                let residents: Vec<PageNumber> = self.pages.keys().copied().collect();
                let preferred = {
                    let tracker = self.eviction_policy.borrow();
                    tracker.choose_victim(&residents)
                };
                preferred.or_else(|| residents.first().copied())
            }
            PageCacheEvictionPolicy::Arbitrary
            | PageCacheEvictionPolicy::S3Fifo(_)
            | PageCacheEvictionPolicy::Arc(_)
            | PageCacheEvictionPolicy::S3FifoAdaptive(_) => self.pages.keys().next().copied(),
        };
        if let Some(key) = key {
            self.pages.remove(&key);
            self.evictions.set(self.evictions.get().saturating_add(1));
            self.eviction_policy.borrow_mut().forget(key);
            true
        } else {
            false
        }
    }

    /// Evict all pages from the cache.
    pub fn clear(&mut self) {
        let removed = self.pages.len();
        let removed_u64 = u64::try_from(removed).unwrap_or(u64::MAX);
        self.evictions
            .set(self.evictions.get().saturating_add(removed_u64));
        self.pages.clear();
        self.eviction_policy.borrow_mut().clear_history();
    }

    /// Capture current cache metrics.
    #[must_use]
    pub fn metrics_snapshot(&self) -> PageCacheMetricsSnapshot {
        let cached_pages = self.pages.len();
        let queue_snapshot = {
            let tracker = self.eviction_policy.borrow();
            if matches!(&*tracker, PageCacheEvictionTracker::S3Fifo(_)) {
                let residents: Vec<PageNumber> = self.pages.keys().copied().collect();
                tracker.queue_snapshot(&residents)
            } else {
                None
            }
        };
        let (t1_size, t2_size, b1_size, p_target) = if let Some(snapshot) = queue_snapshot {
            (
                snapshot.small_len,
                snapshot.main_len,
                snapshot.ghost_len,
                snapshot.small_capacity,
            )
        } else {
            (cached_pages, 0, 0, cached_pages)
        };
        PageCacheMetricsSnapshot {
            hits: self.hits.get(),
            misses: self.misses.get(),
            admits: self.admits.get(),
            evictions: self.evictions.get(),
            cached_pages,
            pool_capacity: self.pool.capacity(),
            // The legacy non-sharded cache still stores raw `PageBuf`s, so it
            // does not expose per-page dirty state yet.
            dirty_ratio_pct: 0,
            t1_size,
            t2_size,
            b1_size,
            b2_size: 0,
            p_target,
            mvcc_multi_version_pages: 0,
        }
    }

    /// Reset cache counters while preserving resident pages and configuration.
    pub fn reset_metrics(&mut self) {
        self.hits.set(0);
        self.misses.set(0);
        self.admits.set(0);
        self.evictions.set(0);
    }
}

impl std::fmt::Debug for PageCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PageCache")
            .field("page_size", &self.page_size)
            .field("cached_pages", &self.pages.len())
            .field("pool", &self.pool)
            .field("hits", &self.hits)
            .field("misses", &self.misses)
            .field("admits", &self.admits)
            .field("evictions", &self.evictions)
            .field("metrics", &self.metrics_snapshot())
            .finish_non_exhaustive()
    }
}

// ---------------------------------------------------------------------------
// ShardedPageCache (bd-3wop3.2)
// ---------------------------------------------------------------------------

/// Default number of shards in [`ShardedPageCache`].
///
/// Must be a power of 2 for efficient masking. The default 128 shards provide
/// good scalability up to ~64 concurrent writers while keeping memory
/// overhead reasonable (~8KB for shard metadata on 64-byte cache lines).
///
/// Future: consider scaling with `std::thread::available_parallelism()` for
/// small embedded targets (fewer shards) or large servers (more shards).
pub const DEFAULT_PAGE_CACHE_SHARDS: usize = 128;
const MIN_PAGE_CACHE_SHARDS: usize = 2;
const MAX_PAGE_CACHE_SHARDS: usize = 1024;

/// Golden ratio constant for multiplicative hashing.
///
/// This is the 32-bit fractional part of the golden ratio (2^32 / φ).
/// Multiplicative hashing with this constant provides excellent distribution
/// even for sequential keys, which is critical for B-tree scan patterns.
const GOLDEN_RATIO_32: u32 = 2_654_435_769;

/// Initial capacity for the fast page array (bd-fzr07).
/// 1024 pages = 4MB at default 4KB page size, covers most small databases.
const FAST_ARRAY_INITIAL_CAPACITY: usize = 1024;

// ---------------------------------------------------------------------------
// FlatPageSlots constants (bd-eorms)
// ---------------------------------------------------------------------------

/// Sentinel for an empty slot. [`PageNumber`] is `NonZeroU32`, so `0` is safe.
const SLOT_EMPTY: u32 = 0;

/// Sentinel for a deleted (tombstone) slot. `u32::MAX` is never a realistic
/// page number (would require a 16 TiB database at 4 KiB pages).
const SLOT_TOMBSTONE: u32 = u32::MAX;

/// Maximum linear probes before declaring a lookup miss. Expected probe
/// length at 50–70% load is 1–3; 32 handles worst-case clustering.
const MAX_PROBE_LENGTH: usize = 32;

/// Minimum flat-table capacity (power of 2). Covers ~350 pages at 70%
/// load (≈ 1.4 MiB at 4 KiB pages), which is enough for tiny databases while
/// keeping connection-open allocation proportional to observed size.
const FLAT_SLOTS_MIN_CAPACITY: usize = 512;
/// Maximum eagerly allocated flat-table capacity.
///
/// The page-buffer pool ceiling is a lazy upper bound used to avoid spurious
/// OOMs on large databases; it is not the steady-state hot set. Keep the
/// lock-free front-cache bounded so connection open does not scale with the
/// configured maximum buffer count while overflow shards still absorb the cold
/// tail.
const FLAT_SLOTS_TARGET_CAPACITY: usize = 16_384;

fn round_flat_slot_capacity(requested: usize) -> usize {
    requested
        .max(1)
        .checked_next_power_of_two()
        .unwrap_or(FLAT_SLOTS_TARGET_CAPACITY)
        .clamp(FLAT_SLOTS_MIN_CAPACITY, FLAT_SLOTS_TARGET_CAPACITY)
}

fn flat_slot_capacity_for_pool(max_buffers: usize) -> usize {
    round_flat_slot_capacity(max_buffers.saturating_mul(2))
}

fn flat_slot_capacity_for_initial_pages(max_buffers: usize, initial_pages: u32) -> usize {
    let page_hint = usize::try_from(initial_pages).unwrap_or(usize::MAX).max(1);
    let hot_page_bound = page_hint.min(max_buffers.max(1));
    round_flat_slot_capacity(hot_page_bound.saturating_mul(2))
}

fn normalize_page_cache_shard_count(requested: usize) -> usize {
    requested
        .clamp(MIN_PAGE_CACHE_SHARDS, MAX_PAGE_CACHE_SHARDS)
        .checked_next_power_of_two()
        .unwrap_or(MAX_PAGE_CACHE_SHARDS)
        .clamp(MIN_PAGE_CACHE_SHARDS, MAX_PAGE_CACHE_SHARDS)
}

#[derive(Debug)]
struct CachedPageEntry {
    buf: PageBuf,
    shared: Option<Arc<[u8]>>,
    access_count: AtomicU64,
    dirty: AtomicBool,
}

impl CachedPageEntry {
    #[inline]
    fn new(buf: PageBuf) -> Self {
        Self {
            buf,
            shared: None,
            access_count: AtomicU64::new(0),
            dirty: AtomicBool::new(false),
        }
    }

    #[inline]
    fn new_dirty(buf: PageBuf) -> Self {
        Self {
            buf,
            shared: None,
            access_count: AtomicU64::new(0),
            dirty: AtomicBool::new(true),
        }
    }

    #[inline]
    fn as_slice(&self) -> &[u8] {
        self.record_access();
        self.buf.as_slice()
    }

    #[inline]
    fn as_mut_slice(&mut self) -> &mut [u8] {
        self.record_access();
        self.mark_dirty();
        self.shared = None;
        self.buf.as_mut_slice()
    }

    #[inline]
    fn shared_page(&mut self) -> PageData {
        self.record_access();
        let shared = if let Some(shared) = self.shared.as_ref() {
            Arc::clone(shared)
        } else {
            let shared = Arc::<[u8]>::from(self.buf.as_slice());
            self.shared = Some(Arc::clone(&shared));
            shared
        };
        PageData::from_shared(shared)
    }

    #[inline]
    fn prefetch_hint(&self) {
        prefetch_l1_read(self.buf.as_slice().as_ptr());
    }

    #[inline]
    fn record_access(&self) {
        self.access_count.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    fn access_count(&self) -> u64 {
        self.access_count.load(Ordering::Relaxed)
    }

    #[inline]
    fn is_dirty(&self) -> bool {
        self.dirty.load(Ordering::Relaxed)
    }

    #[inline]
    fn mark_dirty(&self) {
        self.dirty.store(true, Ordering::Relaxed);
    }

    #[inline]
    fn mark_clean(&self) {
        self.dirty.store(false, Ordering::Relaxed);
    }

    #[inline]
    fn shared_ref_count(&self) -> u32 {
        self.shared.as_ref().map_or(0, |shared| {
            u32::try_from(Arc::strong_count(shared).saturating_sub(1)).unwrap_or(u32::MAX)
        })
    }
}

fn snapshot_cached_page(page_no: PageNumber, entry: &CachedPageEntry) -> PageCachePageSnapshot {
    PageCachePageSnapshot {
        page_no,
        version_txn_id: None,
        queue: None,
        dirty: entry.is_dirty(),
        ref_count: entry.shared_ref_count(),
        access_count: entry.access_count(),
    }
}

// ---------------------------------------------------------------------------
// FastPageArray (bd-fzr07)
// ---------------------------------------------------------------------------

/// Flat page array for single-connection :memory: mode (bd-fzr07).
///
/// For single-connection workloads, this provides O(1) page access via direct
/// Vec indexing, avoiding the hash computation and shard selection overhead
/// of [`ShardedPageCache`]. Pages are indexed by `(pgno - 1)` since SQLite
/// page numbers are 1-based.
///
/// # Performance
///
/// - **Read latency**: ~5-10ns (Vec index + bounds check) vs 50-150ns (sharded)
/// - **Memory**: Sparse array may waste space for databases with many gaps
/// - **Best for**: Sequential B-tree scans, single-writer :memory: workloads
struct FastPageArray {
    /// Pages indexed by `(pgno - 1)`. `None` = page not cached.
    pages: Vec<Option<CachedPageEntry>>,
    /// Packed record of slots touched since the last clear.
    ///
    /// A removed page deliberately leaves its bit set. Re-inserting the same
    /// page therefore cannot duplicate work in `touched_words`, while `clear`
    /// can visit only slots that might still be resident instead of scanning
    /// the full sparse `pages` allocation.
    touched_slot_bits: Vec<u64>,
    /// Indices of non-zero words in `touched_slot_bits`.
    touched_words: Vec<usize>,
    /// Number of non-None entries (tracked for O(1) len()).
    count: usize,
    /// Round-robin cursor for arbitrary eviction scans.
    next_eviction_scan_start: usize,
    /// Local hit counter.
    hits: u64,
    /// Local miss counter.
    misses: u64,
    /// Local admit counter.
    admits: u64,
    /// Local eviction counter.
    evictions: u64,
}

impl FastPageArray {
    /// Create a new fast page array with default initial capacity.
    fn new() -> Self {
        Self {
            pages: Vec::with_capacity(FAST_ARRAY_INITIAL_CAPACITY),
            touched_slot_bits: Vec::new(),
            touched_words: Vec::new(),
            count: 0,
            next_eviction_scan_start: 0,
            hits: 0,
            misses: 0,
            admits: 0,
            evictions: 0,
        }
    }

    /// Convert page number to array index.
    #[inline]
    fn pgno_to_idx(page_no: PageNumber) -> usize {
        (page_no.get() - 1) as usize
    }

    /// Ensure the array can hold the given page number.
    #[inline]
    fn ensure_capacity(&mut self, page_no: PageNumber) {
        let idx = Self::pgno_to_idx(page_no);
        if idx >= self.pages.len() {
            // Grow to at least idx + 1, but prefer doubling for amortized O(1)
            let new_len = (idx + 1)
                .max(self.pages.len() * 2)
                .max(FAST_ARRAY_INITIAL_CAPACITY);
            self.pages.resize_with(new_len, || None);
            self.touched_slot_bits.resize(new_len.div_ceil(64), 0);
        }
    }

    #[inline]
    fn note_touched_slot(&mut self, idx: usize) {
        let word_idx = idx / 64;
        let bit = 1_u64 << (idx % 64);
        let word = &mut self.touched_slot_bits[word_idx];
        if *word == 0 {
            self.touched_words.push(word_idx);
        }
        *word |= bit;
    }

    /// Get a page from the array.
    #[inline]
    fn get(&mut self, page_no: PageNumber) -> Option<&[u8]> {
        let idx = Self::pgno_to_idx(page_no);
        if let Some(Some(entry)) = self.pages.get(idx) {
            self.hits = self.hits.saturating_add(1);
            Some(entry.as_slice())
        } else {
            self.misses = self.misses.saturating_add(1);
            None
        }
    }

    #[inline]
    fn get_shared(&mut self, page_no: PageNumber) -> Option<PageData> {
        let idx = Self::pgno_to_idx(page_no);
        if let Some(Some(entry)) = self.pages.get_mut(idx) {
            self.hits = self.hits.saturating_add(1);
            Some(entry.shared_page())
        } else {
            self.misses = self.misses.saturating_add(1);
            None
        }
    }

    /// Get a mutable reference to a page.
    #[inline]
    fn get_mut(&mut self, page_no: PageNumber) -> Option<&mut [u8]> {
        let idx = Self::pgno_to_idx(page_no);
        if let Some(Some(entry)) = self.pages.get_mut(idx) {
            self.hits = self.hits.saturating_add(1);
            Some(entry.as_mut_slice())
        } else {
            self.misses = self.misses.saturating_add(1);
            None
        }
    }

    /// Check if a page is present.
    #[inline]
    fn contains(&self, page_no: PageNumber) -> bool {
        let idx = Self::pgno_to_idx(page_no);
        self.pages.get(idx).is_some_and(Option::is_some)
    }

    /// Insert a page buffer.
    fn insert(&mut self, page_no: PageNumber, buf: PageBuf) -> bool {
        self.ensure_capacity(page_no);
        let idx = Self::pgno_to_idx(page_no);
        let is_new = self.pages[idx].is_none();
        self.pages[idx] = Some(CachedPageEntry::new(buf));
        if is_new {
            self.note_touched_slot(idx);
            if self.count == 0 {
                self.next_eviction_scan_start = idx;
            }
            self.count += 1;
            self.admits = self.admits.saturating_add(1);
        }
        is_new
    }

    /// Remove a page.
    fn remove(&mut self, page_no: PageNumber) -> bool {
        self.take(page_no).is_some()
    }

    /// Remove and return a page entry.
    fn take(&mut self, page_no: PageNumber) -> Option<CachedPageEntry> {
        let idx = Self::pgno_to_idx(page_no);
        if let Some(slot) = self.pages.get_mut(idx) {
            if let Some(entry) = slot.take() {
                self.count = self.count.saturating_sub(1);
                if self.count == 0 {
                    self.next_eviction_scan_start = 0;
                }
                self.evictions = self.evictions.saturating_add(1);
                return Some(entry);
            }
        }
        None
    }

    /// Remove an arbitrary page (for eviction).
    fn remove_any(&mut self) -> Option<PageNumber> {
        if self.count == 0 || self.pages.is_empty() {
            self.next_eviction_scan_start = 0;
            return None;
        }

        let len = self.pages.len();
        let start = self.next_eviction_scan_start.min(len);
        for idx in start..len {
            if self.pages[idx].take().is_some() {
                self.count = self.count.saturating_sub(1);
                self.next_eviction_scan_start = if self.count == 0 || idx + 1 >= len {
                    0
                } else {
                    idx + 1
                };
                self.evictions = self.evictions.saturating_add(1);
                // Convert idx back to page number (1-based).
                // idx is bounded by pages.len() which fits in usize, and we only
                // store pages with valid PageNumber so idx+1 fits in u32.
                #[allow(clippy::cast_possible_truncation)]
                return PageNumber::new((idx + 1) as u32);
            }
        }
        for idx in 0..start {
            if self.pages[idx].take().is_some() {
                self.count = self.count.saturating_sub(1);
                self.next_eviction_scan_start = if self.count == 0 || idx + 1 >= len {
                    0
                } else {
                    idx + 1
                };
                self.evictions = self.evictions.saturating_add(1);
                #[allow(clippy::cast_possible_truncation)]
                return PageNumber::new((idx + 1) as u32);
            }
        }

        self.next_eviction_scan_start = 0;
        None
    }

    /// Clear all pages.
    fn clear(&mut self) -> usize {
        let removed = self.count;
        self.count = 0;
        self.next_eviction_scan_start = 0;
        for active_idx in 0..self.touched_words.len() {
            let word_idx = self.touched_words[active_idx];
            let mut word = std::mem::take(&mut self.touched_slot_bits[word_idx]);
            while word != 0 {
                let bit_idx = word.trailing_zeros() as usize;
                let slot_idx = word_idx * 64 + bit_idx;
                let _ = self.pages[slot_idx].take();
                word &= word - 1;
            }
        }
        self.touched_words.clear();
        self.evictions = self.evictions.saturating_add(removed as u64);
        removed
    }

    /// Drop all resident pages and release oversized sparse indexing storage.
    fn cold_reset(&mut self) -> usize {
        let removed = self.count;
        self.count = 0;
        self.next_eviction_scan_start = 0;
        self.pages = Vec::with_capacity(FAST_ARRAY_INITIAL_CAPACITY);
        self.touched_slot_bits = Vec::new();
        self.touched_words = Vec::new();
        self.evictions = self.evictions.saturating_add(removed as u64);
        removed
    }

    /// Number of cached pages (O(1)).
    #[inline]
    fn len(&self) -> usize {
        self.count
    }

    fn resident_pages(&self) -> Vec<PageNumber> {
        self.pages
            .iter()
            .enumerate()
            .filter_map(|(idx, slot)| {
                slot.as_ref()?;
                let pgno = u32::try_from(idx.saturating_add(1)).ok()?;
                PageNumber::new(pgno)
            })
            .collect()
    }

    /// Reset metrics counters.
    fn reset_metrics(&mut self) {
        self.hits = 0;
        self.misses = 0;
        self.admits = 0;
        self.evictions = 0;
    }

    /// Best-effort software prefetch for an imminent page lookup.
    fn prefetch_page_hint(&self, page_no: PageNumber) {
        let idx = Self::pgno_to_idx(page_no);
        let Some(slot) = self.pages.get(idx) else {
            return;
        };

        prefetch_l1_read(std::ptr::from_ref(slot));
        if let Some(entry) = slot.as_ref() {
            entry.prefetch_hint();
        }
    }
}

// ---------------------------------------------------------------------------
// FlatPageSlots — CAS-based flat hash page cache (bd-eorms)
// ---------------------------------------------------------------------------

/// A single slot in the flat hash page cache.
///
/// The `pgno` [`AtomicU32`] is the CAS-based state word checked via lock-free
/// atomic loads during probing. The per-slot [`Mutex`] on `data` is only
/// acquired after `pgno` confirms a match, so cache *misses* never take a lock.
struct PageSlot {
    /// `0` = empty, `u32::MAX` = tombstone, else = occupied page number.
    pgno: AtomicU32,
    /// Page data, locked only after `pgno` confirms a match.
    data: Mutex<Option<CachedPageEntry>>,
}

impl PageSlot {
    /// Observe a stable `(pgno, entry)` pair for diagnostics. Writers publish a
    /// new page number before swapping the slot payload, so readers need to
    /// validate the page number again after taking the payload lock.
    fn stable_snapshot(&self) -> Option<PageCachePageSnapshot> {
        self.stable_snapshot_impl(
            #[cfg(test)]
            None,
        )
    }

    fn stable_snapshot_impl(
        &self,
        #[cfg(test)] prelock_barrier: Option<&std::sync::Barrier>,
    ) -> Option<PageCachePageSnapshot> {
        const MAX_ATTEMPTS: usize = 3;

        for _attempt in 0..MAX_ATTEMPTS {
            let pgno_before = self.pgno.load(Ordering::Acquire);
            if pgno_before == SLOT_EMPTY || pgno_before == SLOT_TOMBSTONE {
                return None;
            }

            #[cfg(test)]
            if _attempt == 0
                && let Some(barrier) = prelock_barrier
            {
                barrier.wait();
            }

            let guard = self.data.lock();
            let pgno_after = self.pgno.load(Ordering::Acquire);
            if pgno_before != pgno_after {
                continue;
            }

            let entry = guard.as_ref()?;
            let page_no = PageNumber::new(pgno_after)?;
            return Some(snapshot_cached_page(page_no, entry));
        }

        None
    }

    #[cfg(test)]
    fn stable_snapshot_with_barrier(
        &self,
        prelock_barrier: &std::sync::Barrier,
    ) -> Option<PageCachePageSnapshot> {
        self.stable_snapshot_impl(Some(prelock_barrier))
    }
}

/// Flat hash page cache with CAS-based slot pinning (bd-eorms).
///
/// Models C SQLite's `pcache1`: pages stored in a power-of-2 flat array
/// indexed by `hash(pgno)` with linear probing. Slot claiming uses
/// compare-and-swap on the page-number word, so **cache misses are completely
/// lock-free** (only atomic reads of `pgno` words).
///
/// Cache *hits* acquire a per-slot [`Mutex`] to access page data — much
/// finer-grained than the per-shard mutex of [`ShardedPageCache`]'s
/// overflow path.
struct FlatPageSlots {
    slots: Box<[PageSlot]>,
    /// `slots.len() - 1` (capacity is always a power of two).
    mask: usize,
    /// Number of occupied (non-empty, non-tombstone) slots.
    count: AtomicUsize,
    /// Conservative hint that deleted slots may be present.
    ///
    /// Tombstones are cheap to reuse during ordinary insert churn, but a
    /// cache-wide clear is supposed to be a cold reset. This flag lets
    /// `clear()` keep the empty fast path without depending on an exact
    /// tombstone count that could race with concurrent evictions.
    has_tombstones: AtomicBool,
    /// Next slot index to probe first for arbitrary eviction.
    ///
    /// Sequential scans can force frequent evictions once the buffer pool
    /// saturates. Advancing a cursor across the flat table avoids repeatedly
    /// rescanning the same prefix on every eviction.
    eviction_cursor: AtomicUsize,
    hits: AtomicU64,
    misses: AtomicU64,
    admits: AtomicU64,
    evictions: AtomicU64,
}

impl FlatPageSlots {
    /// Create a flat page slot table with the given capacity (rounded up to
    /// the next power of two, clamped to [`FLAT_SLOTS_MIN_CAPACITY`]).
    fn new(capacity: usize) -> Self {
        let capacity = capacity.next_power_of_two().max(FLAT_SLOTS_MIN_CAPACITY);
        let slots: Vec<PageSlot> = (0..capacity)
            .map(|_| PageSlot {
                pgno: AtomicU32::new(SLOT_EMPTY),
                data: Mutex::new(None),
            })
            .collect();
        Self {
            mask: capacity - 1,
            slots: slots.into_boxed_slice(),
            count: AtomicUsize::new(0),
            has_tombstones: AtomicBool::new(false),
            eviction_cursor: AtomicUsize::new(0),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            admits: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
        }
    }

    /// Multiplicative hash of a page number → starting slot index.
    #[inline]
    fn hash_pgno(&self, pgno: u32) -> usize {
        // Use the upper bits of the product for better distribution of
        // sequential page numbers (common in B-tree scans).
        (pgno.wrapping_mul(GOLDEN_RATIO_32) >> 16) as usize & self.mask
    }

    /// Lock-free probe for a page. Returns the slot index if found.
    #[inline]
    fn find_slot(&self, page_no: PageNumber) -> Option<usize> {
        let pgno = page_no.get();
        let start = self.hash_pgno(pgno);
        for i in 0..MAX_PROBE_LENGTH {
            let idx = (start + i) & self.mask;
            let slot_pgno = self.slots[idx].pgno.load(Ordering::Acquire);
            if slot_pgno == pgno {
                return Some(idx);
            }
            if slot_pgno == SLOT_EMPTY {
                return None;
            }
        }
        None
    }

    /// Check if a page is present (lock-free).
    #[inline]
    fn contains(&self, page_no: PageNumber) -> bool {
        self.find_slot(page_no).is_some()
    }

    /// Check whether `page_no` still owns both the probed slot and payload.
    ///
    /// Unlike [`Self::contains`], this closes the probe-to-payload reuse race
    /// and is suitable for decisions that coordinate the flat and overflow
    /// tiers.
    fn contains_stable(&self, page_no: PageNumber) -> bool {
        let Some(idx) = self.find_slot(page_no) else {
            return false;
        };
        let data = self.slots[idx].data.lock();
        self.slots[idx].pgno.load(Ordering::Acquire) == page_no.get() && data.is_some()
    }

    /// Get page data as an owned copy.
    fn get_copy(&self, page_no: PageNumber) -> Option<Vec<u8>> {
        let idx = self.find_slot(page_no)?;
        let guard = self.slots[idx].data.lock();
        if self.slots[idx].pgno.load(Ordering::Acquire) != page_no.get() {
            return None;
        }
        self.hits.fetch_add(1, Ordering::Relaxed);
        guard.as_ref().map(|entry| entry.as_slice().to_vec())
    }

    #[cfg(test)]
    fn get_copy_after_probe_barrier(
        &self,
        page_no: PageNumber,
        barrier: &std::sync::Barrier,
    ) -> Option<Vec<u8>> {
        let idx = self.find_slot(page_no)?;
        barrier.wait();
        barrier.wait();
        let guard = self.slots[idx].data.lock();
        if self.slots[idx].pgno.load(Ordering::Acquire) != page_no.get() {
            return None;
        }
        guard.as_ref().map(|entry| entry.as_slice().to_vec())
    }

    /// Get page data as a shared [`PageData`].
    fn get_shared(&self, page_no: PageNumber) -> Option<PageData> {
        let idx = self.find_slot(page_no)?;
        let mut guard = self.slots[idx].data.lock();
        if self.slots[idx].pgno.load(Ordering::Acquire) != page_no.get() {
            return None;
        }
        self.hits.fetch_add(1, Ordering::Relaxed);
        guard.as_mut().map(CachedPageEntry::shared_page)
    }

    #[inline]
    fn prefetch_slot(&self, idx: usize) {
        let slot = &self.slots[idx & self.mask];
        prefetch_l1_read(std::ptr::from_ref(slot));
    }

    /// Best-effort software prefetch for the flat-slot probe chain and, when
    /// already resident, the page bytes themselves.
    fn prefetch_page_hint(&self, page_no: PageNumber) {
        let pgno = page_no.get();
        let start = self.hash_pgno(pgno);
        self.prefetch_slot(start);
        self.prefetch_slot(start + 1);

        for probe in 0..MAX_PROBE_LENGTH {
            let idx = (start + probe) & self.mask;
            let slot = &self.slots[idx];
            let slot_pgno = slot.pgno.load(Ordering::Acquire);
            if slot_pgno == pgno {
                self.prefetch_slot(idx);
                if let Some(guard) = slot.data.try_lock()
                    && slot.pgno.load(Ordering::Acquire) == pgno
                    && let Some(entry) = guard.as_ref()
                {
                    entry.prefetch_hint();
                }
                return;
            }
            if slot_pgno == SLOT_EMPTY {
                return;
            }
        }
    }

    /// Try to insert a page buffer. Returns `Ok(true)` if newly inserted,
    /// `Ok(false)` if an existing entry was updated, or `Err(buf)` if the
    /// table is full (caller should use overflow shards).
    #[allow(clippy::missing_errors_doc)]
    fn try_insert(&self, page_no: PageNumber, buf: PageBuf) -> std::result::Result<bool, PageBuf> {
        self.try_insert_with_dirty_state(page_no, buf, false)
    }

    /// Insert a newly modified page and publish its dirty state atomically
    /// with the cache entry.
    fn try_insert_dirty(
        &self,
        page_no: PageNumber,
        buf: PageBuf,
    ) -> std::result::Result<bool, PageBuf> {
        self.try_insert_with_dirty_state(page_no, buf, true)
    }

    fn try_insert_with_dirty_state(
        &self,
        page_no: PageNumber,
        buf: PageBuf,
        dirty: bool,
    ) -> std::result::Result<bool, PageBuf> {
        let pgno = page_no.get();
        // u32::MAX is our tombstone sentinel — cannot store it.
        if pgno == SLOT_TOMBSTONE {
            return Err(buf);
        }
        for _attempt in 0..MAX_PROBE_LENGTH {
            let start = self.hash_pgno(pgno);
            let mut first_available: Option<(usize, u32)> = None;

            for i in 0..MAX_PROBE_LENGTH {
                let idx = (start + i) & self.mask;
                let slot_pgno = self.slots[idx].pgno.load(Ordering::Acquire);

                if slot_pgno == pgno {
                    // Already present — update data. Re-probe if eviction or
                    // reuse changed slot ownership before the payload lock.
                    let mut data = self.slots[idx].data.lock();
                    if self.slots[idx].pgno.load(Ordering::Acquire) != pgno {
                        continue;
                    }
                    *data = Some(if dirty {
                        CachedPageEntry::new_dirty(buf)
                    } else {
                        CachedPageEntry::new(buf)
                    });
                    return Ok(false);
                }

                if (slot_pgno == SLOT_EMPTY || slot_pgno == SLOT_TOMBSTONE)
                    && first_available.is_none()
                {
                    first_available = Some((idx, slot_pgno));
                }

                if slot_pgno == SLOT_EMPTY {
                    break; // End of probe chain — page not present.
                }
            }

            let Some((avail_idx, expected)) = first_available else {
                return Err(buf); // Probe chain exhausted with no available slot.
            };

            // Hold the payload lock before publishing pgno: readers that
            // observe the new page number must block until bytes are installed.
            let mut data_guard = self.slots[avail_idx].data.lock();
            if self.slots[avail_idx]
                .pgno
                .compare_exchange(expected, pgno, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                *data_guard = Some(if dirty {
                    CachedPageEntry::new_dirty(buf)
                } else {
                    CachedPageEntry::new(buf)
                });
                self.count.fetch_add(1, Ordering::Relaxed);
                self.admits.fetch_add(1, Ordering::Relaxed);
                return Ok(true);
            }
            // Another thread claimed the slot between probe and CAS. Retry
            // the full probe so a same-page winner is updated in the flat tier
            // instead of creating a stale duplicate in the overflow shard.
        }

        Err(buf)
    }

    /// Remove a specific page. Returns `true` if evicted.
    fn remove(&self, page_no: PageNumber) -> bool {
        let Some(idx) = self.find_slot(page_no) else {
            return false;
        };
        let pgno = page_no.get();
        let mut data = self.slots[idx].data.lock();
        if self.slots[idx].pgno.load(Ordering::Acquire) != pgno {
            return false;
        }
        if self.slots[idx]
            .pgno
            .compare_exchange(pgno, SLOT_TOMBSTONE, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
        {
            let _ = data.take();
            self.count.fetch_sub(1, Ordering::Relaxed);
            self.has_tombstones.store(true, Ordering::Release);
            self.evictions.fetch_add(1, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    /// Remove `page_no` only while its cache entry is still clean.
    ///
    /// The payload lock spans the dirty check and the page-number CAS, so a
    /// concurrent mutable cache access cannot dirty the page between the
    /// eligibility decision and eviction.
    fn take_if_clean_from_pool(&self, page_no: PageNumber, pool: &PageBufPool) -> Option<PageBuf> {
        let idx = self.find_slot(page_no)?;
        let pgno = page_no.get();
        let mut data = self.slots[idx].data.lock();
        let reclaimable = data
            .as_ref()
            .is_some_and(|entry| !entry.is_dirty() && entry.buf.returns_to_pool(pool));
        if self.slots[idx].pgno.load(Ordering::Acquire) != pgno || !reclaimable {
            return None;
        }
        if self.slots[idx]
            .pgno
            .compare_exchange(pgno, SLOT_TOMBSTONE, Ordering::AcqRel, Ordering::Relaxed)
            .is_err()
        {
            return None;
        }

        let entry = data
            .take()
            .expect("reclaimable flat cache entry must still be present");
        self.count.fetch_sub(1, Ordering::Relaxed);
        self.has_tombstones.store(true, Ordering::Release);
        self.evictions.fetch_add(1, Ordering::Relaxed);
        Some(entry.buf)
    }

    /// Remove an arbitrary page (for eviction) and return its page number.
    fn remove_any_page(&self) -> Option<PageNumber> {
        let start = self.eviction_cursor.fetch_add(1, Ordering::Relaxed);
        for i in 0..self.slots.len() {
            let idx = (start + i) & self.mask;
            let slot_pgno = self.slots[idx].pgno.load(Ordering::Relaxed);
            if slot_pgno == SLOT_EMPTY || slot_pgno == SLOT_TOMBSTONE {
                continue;
            }
            let mut data = self.slots[idx].data.lock();
            if self.slots[idx].pgno.load(Ordering::Acquire) == slot_pgno
                && self.slots[idx]
                    .pgno
                    .compare_exchange(
                        slot_pgno,
                        SLOT_TOMBSTONE,
                        Ordering::AcqRel,
                        Ordering::Relaxed,
                    )
                    .is_ok()
            {
                let _ = data.take();
                self.count.fetch_sub(1, Ordering::Relaxed);
                self.has_tombstones.store(true, Ordering::Release);
                self.eviction_cursor
                    .store(idx.wrapping_add(1), Ordering::Relaxed);
                self.evictions.fetch_add(1, Ordering::Relaxed);
                return PageNumber::new(slot_pgno);
            }
        }
        None
    }

    /// Clear all pages. Returns the number of pages evicted.
    fn clear(&self) -> usize {
        // Short-circuit the common "already-empty" case so we don't walk
        // every slot on each clear call. Under MT-writer contention this
        // accounted for 3.02% self-time at 8t on the 2026-04-23 post-T1
        // capture (`fsqlite-mt-post-t1t2t7-184420`).
        let had_tombstones = self.has_tombstones.swap(false, Ordering::AcqRel);
        if self.count.load(Ordering::Acquire) == 0 && !had_tombstones {
            self.eviction_cursor.store(0, Ordering::Relaxed);
            return 0;
        }
        let mut removed = 0_usize;
        for slot in self.slots.iter() {
            // Pre-filter with Relaxed load: the AcqRel RMW is expensive on
            // every slot even when the slot is already empty, which is the
            // dominant shape for a mostly-drained flat table.
            let observed = slot.pgno.load(Ordering::Relaxed);
            if observed == SLOT_EMPTY {
                continue;
            }
            let pgno = slot.pgno.swap(SLOT_EMPTY, Ordering::AcqRel);
            if pgno != SLOT_EMPTY && pgno != SLOT_TOMBSTONE {
                let _ = slot.data.lock().take();
                removed += 1;
            }
        }
        self.count.store(0, Ordering::Relaxed);
        self.eviction_cursor.store(0, Ordering::Relaxed);
        #[allow(clippy::cast_possible_truncation)]
        self.evictions.fetch_add(removed as u64, Ordering::Relaxed);
        removed
    }

    /// Number of occupied slots.
    #[inline]
    fn len(&self) -> usize {
        self.count.load(Ordering::Relaxed)
    }

    fn resident_pages(&self) -> Vec<PageNumber> {
        self.slots
            .iter()
            .filter_map(PageSlot::stable_snapshot)
            .map(|snapshot| snapshot.page_no)
            .collect()
    }

    /// Reset metrics counters.
    fn reset_metrics(&self) {
        self.hits.store(0, Ordering::Relaxed);
        self.misses.store(0, Ordering::Relaxed);
        self.admits.store(0, Ordering::Relaxed);
        self.evictions.store(0, Ordering::Relaxed);
    }
}

impl std::fmt::Debug for FlatPageSlots {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FlatPageSlots")
            .field("capacity", &(self.mask + 1))
            .field("count", &self.count.load(Ordering::Relaxed))
            .field(
                "has_tombstones",
                &self.has_tombstones.load(Ordering::Relaxed),
            )
            .field("hits", &self.hits.load(Ordering::Relaxed))
            .field("misses", &self.misses.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

// ---------------------------------------------------------------------------
// PageCacheShard
// ---------------------------------------------------------------------------

/// A single shard of the page cache.
///
/// Each shard contains its own hash map and metrics counters. The shard is
/// cache-line aligned to prevent false sharing between adjacent shards when
/// accessed by different threads.
#[repr(align(64))]
struct PageCacheShard {
    pages: std::collections::HashMap<PageNumber, CachedPageEntry, foldhash::fast::FixedState>,
    /// Local hit counter (aggregated on metrics snapshot).
    hits: u64,
    /// Local miss counter.
    misses: u64,
    /// Local admit counter.
    admits: u64,
    /// Local eviction counter.
    evictions: u64,
}

impl PageCacheShard {
    /// Create a new empty shard.
    fn new() -> Self {
        Self {
            pages: std::collections::HashMap::with_hasher(foldhash::fast::FixedState::default()),
            hits: 0,
            misses: 0,
            admits: 0,
            evictions: 0,
        }
    }

    /// Number of pages in this shard.
    #[inline]
    fn len(&self) -> usize {
        self.pages.len()
    }

    fn resident_pages(&self) -> Vec<PageNumber> {
        self.pages.keys().copied().collect()
    }

    /// Check if a page is present in this shard.
    #[inline]
    fn contains(&self, page_no: PageNumber) -> bool {
        self.pages.contains_key(&page_no)
    }

    /// Get a page from this shard, updating hit/miss metrics.
    #[inline]
    fn get(&mut self, page_no: PageNumber) -> Option<&[u8]> {
        if let Some(page) = self.pages.get(&page_no) {
            self.hits = self.hits.saturating_add(1);
            Some(page.as_slice())
        } else {
            self.misses = self.misses.saturating_add(1);
            None
        }
    }

    #[inline]
    fn get_shared(&mut self, page_no: PageNumber) -> Option<PageData> {
        if let Some(page) = self.pages.get_mut(&page_no) {
            self.hits = self.hits.saturating_add(1);
            Some(page.shared_page())
        } else {
            self.misses = self.misses.saturating_add(1);
            None
        }
    }

    /// Get a mutable reference to a page in this shard.
    #[inline]
    fn get_mut(&mut self, page_no: PageNumber) -> Option<&mut [u8]> {
        if let Some(page) = self.pages.get_mut(&page_no) {
            self.hits = self.hits.saturating_add(1);
            Some(page.as_mut_slice())
        } else {
            self.misses = self.misses.saturating_add(1);
            None
        }
    }

    fn insert_with_dirty_state(&mut self, page_no: PageNumber, buf: PageBuf, dirty: bool) -> bool {
        let admitted_new = match self.pages.entry(page_no) {
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                entry.insert(if dirty {
                    CachedPageEntry::new_dirty(buf)
                } else {
                    CachedPageEntry::new(buf)
                });
                false
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(if dirty {
                    CachedPageEntry::new_dirty(buf)
                } else {
                    CachedPageEntry::new(buf)
                });
                true
            }
        };
        if admitted_new {
            self.admits = self.admits.saturating_add(1);
        }
        admitted_new
    }

    /// Remove a page from this shard.
    fn remove(&mut self, page_no: PageNumber) -> bool {
        let removed = self.pages.remove(&page_no).is_some();
        if removed {
            self.evictions = self.evictions.saturating_add(1);
        }
        removed
    }

    /// Remove an arbitrary page from this shard (for eviction).
    fn remove_any(&mut self) -> Option<PageNumber> {
        let key = self.pages.keys().next().copied();
        if let Some(k) = key {
            self.pages.remove(&k);
            self.evictions = self.evictions.saturating_add(1);
        }
        key
    }

    /// Clear all pages from this shard.
    fn clear(&mut self) -> usize {
        let removed = self.pages.len();
        self.evictions = self.evictions.saturating_add(removed as u64);
        self.pages.clear();
        removed
    }

    /// Reset metrics counters.
    fn reset_metrics(&mut self) {
        self.hits = 0;
        self.misses = 0;
        self.admits = 0;
        self.evictions = 0;
    }
}

impl std::fmt::Debug for PageCacheShard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PageCacheShard")
            .field("pages", &self.pages.len())
            .field("hits", &self.hits)
            .field("misses", &self.misses)
            .field("admits", &self.admits)
            .field("evictions", &self.evictions)
            .finish()
    }
}

/// Sharded page cache for high-concurrency workloads (bd-3wop3.2).
///
/// This cache partitions the page-number space across a configurable
/// power-of-two number of mutex-protected shards. Concurrent writers
/// operating on different pages acquire different shard locks, enabling
/// near-linear scaling up to ~64 threads with the default configuration.
///
/// # Design Rationale
///
/// - **Partitioned overflow map**: The default 128 shards balance lock
///   granularity and memory overhead. Each shard adds ~64 bytes of
///   cache-line-padded mutex overhead.
/// - **Multiplicative hash**: Ensures good distribution even for sequential
///   page access patterns (B-tree scans, bulk inserts).
/// - **Shared pool**: The underlying `PageBufPool` remains global because
///   buffer allocation is already lock-free (via atomic free-list).
/// - **Per-shard metrics**: Avoids false sharing on metric counters.
///
/// # Single-Connection Fast Path (bd-fzr07)
///
/// For single-connection `:memory:` workloads, the cache can use a flat
/// `FastPageArray` that provides O(1) page access via direct Vec indexing.
/// Enable with [`ShardedPageCache::new_single_connection`] or
/// [`ShardedPageCache::enable_fast_path`].
///
/// # Thread Safety
///
/// Each shard is protected by a `Mutex`. The shard selection is deterministic
/// (based on page number), so deadlock-free access is guaranteed as long as
/// callers don't hold multiple shard locks simultaneously. The API is designed
/// to make multi-shard locking unnecessary.
pub struct ShardedPageCache {
    /// CAS-based flat hash page cache (bd-eorms, pcache1 pattern).
    /// Tried first for all lookups; cache misses are lock-free.
    flat_slots: FlatPageSlots,
    /// Overflow: one cache-line aligned shard per configured partition.
    /// Used when the flat table is full or for CAS-failure fallback.
    shards: Box<[Mutex<PageCacheShard>]>,
    /// Conservative hint that one of `shards` may hold a page.
    ///
    /// Set by overflow inserts after the shard lock is released, and cleared
    /// by [`Self::clear`] / [`Self::enable_fast_path`] / [`Self::disable_fast_path`]
    /// after they walk every shard. When the flat-slot path absorbs every
    /// admitted page (the dominant case at MT8), the shards stay empty across
    /// the connection's lifetime; this flag lets the per-connection cold reset
    /// skip the 128-shard mutex walk that previously fired on every
    /// `refresh_committed_state` from `Connection::open_with_env_and_pager`.
    shards_dirty: AtomicBool,
    shard_mask: usize,
    shard_shift: u32,
    /// Shared page buffer pool (lock-free allocation).
    pool: PageBufPool,
    /// Configured page size.
    page_size: PageSize,
    /// Fast-path flat array for single-connection mode (bd-fzr07).
    /// When `Some`, page lookups bypass sharded mutexes and use direct indexing.
    fast_array: Option<Mutex<FastPageArray>>,
    /// Whether to use the fast path. Checked first on every operation.
    /// `Relaxed` ordering is sufficient since we're just reading a hint.
    use_fast_path: AtomicBool,
    /// Fast gate for eviction-policy bookkeeping on the read path.
    eviction_policy_enabled: AtomicBool,
    /// Shared eviction-policy tracker used by [`Self::evict_any`].
    eviction_policy: Mutex<PageCacheEvictionTracker>,
    /// Bayesian-mixture e-value evictor (IMPL-19 / AAC-P5).
    ///
    /// Present only when the `evalue-eviction` cargo feature is enabled.
    /// When present, [`Self::get`] / `read_cached_page` routes feed
    /// `record_access`, and [`Self::evalue_choose_victim`] can be consulted
    /// to pick the lowest-e page among a candidate set. When the feature is
    /// disabled this field is compiled out and all integration code is
    /// no-opped.
    #[cfg(feature = "evalue-eviction")]
    evalue_evictor: crate::evalue_eviction::EValueEvictor,
    /// Accesses observed since the last automatic e-value tick. When this
    /// crosses `DEFAULT_TICK_INTERVAL`, a single decay scan is issued.
    #[cfg(feature = "evalue-eviction")]
    evalue_accesses_since_tick: AtomicU64,
}

impl ShardedPageCache {
    /// Create a new sharded page cache with the given page size.
    ///
    /// The buffer-pool ceiling is determined by
    /// [`resolve_page_buffer_max`] — i.e. the `FSQLITE_PAGE_BUFFER_MAX`
    /// environment variable if set, otherwise [`DEFAULT_PAGE_BUFFER_MAX`]
    /// (262 144 buffers ≈ 1 GiB at 4 KiB pages).
    ///
    /// For single-connection `:memory:` workloads, prefer [`Self::new_single_connection`]
    /// which enables the fast-path flat array (bd-fzr07).
    pub fn new(page_size: PageSize) -> Self {
        Self::with_max_buffers_and_shards(
            page_size,
            resolve_page_buffer_max(None),
            DEFAULT_PAGE_CACHE_SHARDS,
        )
    }

    /// Create a new sharded page cache with an explicit buffer-pool ceiling.
    ///
    /// `max_buffers` is the maximum number of live page buffers (idle + in-use)
    /// the underlying [`PageBufPool`] will allow.  Once the bound is reached,
    /// further buffer acquisitions fail with [`FrankenError::OutOfMemory`].
    pub fn with_max_buffers(page_size: PageSize, max_buffers: usize) -> Self {
        Self::with_max_buffers_and_shards(page_size, max_buffers, DEFAULT_PAGE_CACHE_SHARDS)
    }

    /// Create a new sharded page cache with a front-cache sized from the
    /// currently observed database page count.
    ///
    /// The buffer-pool ceiling is a high-water safety bound, not a hot-set
    /// measurement. Empty and tiny databases should not pay to initialize the
    /// full 16K-slot lock-free front-cache on every connection open; existing
    /// larger databases still scale up to the same cap.
    pub fn with_max_buffers_for_initial_pages(
        page_size: PageSize,
        max_buffers: usize,
        initial_pages: u32,
    ) -> Self {
        Self::with_max_buffers_for_initial_pages_and_shards(
            page_size,
            max_buffers,
            initial_pages,
            DEFAULT_PAGE_CACHE_SHARDS,
        )
    }

    /// Create a sharded cache with an explicit partition count.
    ///
    /// `shard_count` is normalized to a power of two within a bounded range so
    /// shard selection can stay on the fast multiplicative-hash path.
    pub fn with_max_buffers_and_shards(
        page_size: PageSize,
        max_buffers: usize,
        shard_count: usize,
    ) -> Self {
        Self::with_pool_and_shards(
            PageBufPool::new(page_size, max_buffers),
            page_size,
            shard_count,
        )
    }

    fn with_max_buffers_for_initial_pages_and_shards(
        page_size: PageSize,
        max_buffers: usize,
        initial_pages: u32,
        shard_count: usize,
    ) -> Self {
        Self::with_pool_and_shards_and_flat_capacity(
            PageBufPool::new(page_size, max_buffers),
            page_size,
            shard_count,
            flat_slot_capacity_for_initial_pages(max_buffers, initial_pages),
        )
    }

    /// Create a new sharded page cache optimized for single-connection mode (bd-fzr07).
    ///
    /// Enables a flat page array that provides O(1) page access via direct Vec
    /// indexing, avoiding hash computation and shard selection overhead.
    ///
    /// # Performance
    ///
    /// - **Read latency**: ~5-10ns vs 50-150ns for sharded path
    /// - **Best for**: Single-writer `:memory:` databases, sequential B-tree scans
    pub fn new_single_connection(page_size: PageSize) -> Self {
        let mut cache = Self::new(page_size);
        cache.enable_fast_path();
        cache
    }

    /// Create a new sharded page cache using an existing `PageBufPool`.
    pub fn with_pool(pool: PageBufPool, page_size: PageSize) -> Self {
        Self::with_pool_and_shards(pool, page_size, DEFAULT_PAGE_CACHE_SHARDS)
    }

    fn with_pool_and_shards(pool: PageBufPool, page_size: PageSize, shard_count: usize) -> Self {
        let flat_capacity = flat_slot_capacity_for_pool(pool.capacity());
        Self::with_pool_and_shards_and_flat_capacity(pool, page_size, shard_count, flat_capacity)
    }

    fn with_pool_and_shards_and_flat_capacity(
        pool: PageBufPool,
        page_size: PageSize,
        shard_count: usize,
        flat_capacity: usize,
    ) -> Self {
        let shard_count = normalize_page_cache_shard_count(shard_count);
        let shard_shift = 32 - shard_count.trailing_zeros();
        let shard_mask = shard_count - 1;
        let shards = (0..shard_count)
            .map(|_| Mutex::new(PageCacheShard::new()))
            .collect::<Vec<_>>()
            .into_boxed_slice();

        Self {
            flat_slots: FlatPageSlots::new(flat_capacity),
            shards,
            shards_dirty: AtomicBool::new(false),
            shard_mask,
            shard_shift,
            pool,
            page_size,
            fast_array: None,
            use_fast_path: AtomicBool::new(false),
            eviction_policy_enabled: AtomicBool::new(false),
            eviction_policy: Mutex::new(PageCacheEvictionTracker::default()),
            #[cfg(feature = "evalue-eviction")]
            evalue_evictor: crate::evalue_eviction::EValueEvictor::new(),
            #[cfg(feature = "evalue-eviction")]
            evalue_accesses_since_tick: AtomicU64::new(0),
        }
    }

    /// Enable the single-connection fast path (bd-fzr07).
    ///
    /// Once enabled, all page operations will use the flat array instead of
    /// the sharded cache. Switching into fast-path mode performs a cold reset
    /// of every cache tier so that hidden pages from a previous mode cannot
    /// later reappear and overwrite newer data.
    pub fn enable_fast_path(&mut self) {
        if self.fast_array.is_none() {
            self.fast_array = Some(Mutex::new(FastPageArray::new()));
        }
        if !self.use_fast_path.load(Ordering::Relaxed) {
            if let Some(ref fast) = self.fast_array {
                fast.lock().cold_reset();
            }
            self.flat_slots.clear();
            if self.shards_dirty.swap(false, Ordering::AcqRel) {
                for shard in self.shards.iter() {
                    shard.lock().clear();
                }
            }
            self.clear_eviction_history();
        }
        self.use_fast_path.store(true, Ordering::Release);
    }

    /// Disable the fast path and switch back to sharded cache.
    ///
    /// Switching out of fast-path mode is a cold reset: fast-array residents
    /// are not migrated into the sharded cache, and keeping them hidden would
    /// only waste memory and risk stale pages surfacing after later mode
    /// changes.
    pub fn disable_fast_path(&mut self) {
        if self.use_fast_path.load(Ordering::Relaxed) {
            if let Some(ref fast) = self.fast_array {
                fast.lock().cold_reset();
            }
            self.flat_slots.clear();
            if self.shards_dirty.swap(false, Ordering::AcqRel) {
                for shard in self.shards.iter() {
                    shard.lock().clear();
                }
            }
            self.clear_eviction_history();
        }
        self.use_fast_path.store(false, Ordering::Release);
    }

    /// Check if fast path is enabled.
    #[inline]
    pub fn is_fast_path_enabled(&self) -> bool {
        self.use_fast_path.load(Ordering::Relaxed)
    }

    /// Set the eviction policy used by [`Self::evict_any`].
    pub fn set_eviction_policy(&self, policy: PageCacheEvictionPolicy) {
        *self.eviction_policy.lock() = PageCacheEvictionTracker::from_policy(policy);
        self.eviction_policy_enabled.store(
            !matches!(policy, PageCacheEvictionPolicy::Arbitrary),
            Ordering::Release,
        );
    }

    /// Return the current eviction policy.
    #[must_use]
    pub fn eviction_policy(&self) -> PageCacheEvictionPolicy {
        self.eviction_policy.lock().policy()
    }

    #[inline]
    fn eviction_tracking_enabled(&self) -> bool {
        self.eviction_policy_enabled.load(Ordering::Relaxed)
    }

    #[inline]
    fn record_eviction_access(&self, page_no: PageNumber) {
        if self.eviction_tracking_enabled() {
            self.eviction_policy.lock().record_access(page_no);
        }
        #[cfg(feature = "evalue-eviction")]
        self.evalue_record_access(page_no);
    }

    #[inline]
    fn record_eviction_admit(&self, page_no: PageNumber) {
        if self.eviction_tracking_enabled() {
            self.eviction_policy.lock().record_admit(page_no);
        }
        #[cfg(feature = "evalue-eviction")]
        self.evalue_record_access(page_no);
    }

    #[inline]
    fn forget_eviction_page(&self, page_no: PageNumber) {
        if self.eviction_tracking_enabled() {
            self.eviction_policy.lock().forget(page_no);
        }
        #[cfg(feature = "evalue-eviction")]
        self.evalue_evictor.forget(page_no);
    }

    #[inline]
    fn clear_eviction_history(&self) {
        if self.eviction_tracking_enabled() {
            self.eviction_policy.lock().clear_history();
        }
        #[cfg(feature = "evalue-eviction")]
        self.evalue_evictor.clear();
    }

    /// Feed an access into the e-value evictor and perform periodic decay.
    ///
    /// Only compiled in when the `evalue-eviction` feature is enabled.
    #[cfg(feature = "evalue-eviction")]
    #[inline]
    fn evalue_record_access(&self, page_no: PageNumber) {
        self.evalue_evictor.record_access(page_no);
        let observed = self
            .evalue_accesses_since_tick
            .fetch_add(1, Ordering::Relaxed);
        if observed + 1 >= crate::evalue_eviction::DEFAULT_TICK_INTERVAL {
            self.evalue_accesses_since_tick.store(0, Ordering::Relaxed);
            self.evalue_evictor.tick();
        }
    }

    /// Query the Ville p-value for a page tracked by the e-value evictor.
    ///
    /// Returns `1.0` for untracked pages (no evidence against the null).
    /// Only available when the `evalue-eviction` feature is enabled.
    #[cfg(feature = "evalue-eviction")]
    #[must_use]
    pub fn evalue_ville_pvalue(&self, page_no: PageNumber) -> f64 {
        self.evalue_evictor.ville_pvalue(page_no)
    }

    /// Pick the lowest-e candidate according to the e-value evictor.
    ///
    /// Returns `None` if `candidates` is empty. Untracked candidates are
    /// treated as if they had the initial e-value (the null boundary).
    /// Only available when the `evalue-eviction` feature is enabled.
    #[cfg(feature = "evalue-eviction")]
    #[must_use]
    pub fn evalue_choose_victim(&self, candidates: &[PageNumber]) -> Option<PageNumber> {
        self.evalue_evictor.choose_victim(candidates)
    }

    /// Access the underlying `EValueEvictor` for inspection or test hooks.
    #[cfg(feature = "evalue-eviction")]
    #[must_use]
    pub fn evalue_evictor(&self) -> &crate::evalue_eviction::EValueEvictor {
        &self.evalue_evictor
    }

    fn note_page_access_without_metrics(&self, page_no: PageNumber) {
        if self.use_fast_path.load(Ordering::Relaxed)
            && let Some(ref fast) = self.fast_array
        {
            let idx = FastPageArray::pgno_to_idx(page_no);
            if let Some(Some(entry)) = fast.lock().pages.get(idx) {
                entry.record_access();
            }
            return;
        }

        if let Some(slot_idx) = self.flat_slots.find_slot(page_no) {
            let guard = self.flat_slots.slots[slot_idx].data.lock();
            if self.flat_slots.slots[slot_idx].pgno.load(Ordering::Acquire) == page_no.get()
                && let Some(ref entry) = *guard
            {
                entry.record_access();
                return;
            }
        }

        let idx = self.shard_index(page_no);
        if let Some(entry) = self.shards[idx].lock().pages.get(&page_no) {
            entry.record_access();
        }
    }

    fn mark_page_clean(&self, page_no: PageNumber) {
        if self.use_fast_path.load(Ordering::Relaxed)
            && let Some(ref fast) = self.fast_array
        {
            let idx = FastPageArray::pgno_to_idx(page_no);
            if let Some(Some(entry)) = fast.lock().pages.get(idx) {
                entry.mark_clean();
            }
            return;
        }

        if let Some(slot_idx) = self.flat_slots.find_slot(page_no) {
            let guard = self.flat_slots.slots[slot_idx].data.lock();
            if self.flat_slots.slots[slot_idx].pgno.load(Ordering::Acquire) == page_no.get()
                && let Some(ref entry) = *guard
            {
                entry.mark_clean();
                return;
            }
        }

        let idx = self.shard_index(page_no);
        if let Some(entry) = self.shards[idx].lock().pages.get(&page_no) {
            entry.mark_clean();
        }
    }

    /// Remove an obsolete overflow copy after the authoritative insertion was
    /// admitted into the flat tier. This preserves the one-resident-image
    /// invariant across tombstone reuse and probe-chain contraction.
    fn remove_overflow_duplicate(&self, page_no: PageNumber) -> bool {
        let idx = self.shard_index(page_no);
        self.shards[idx].lock().remove(page_no)
    }

    /// Insert one authoritative cache image while preserving the invariant
    /// that a page resides in exactly one storage tier.
    fn insert_tiered(&self, page_no: PageNumber, buf: PageBuf, dirty: bool) -> bool {
        // Serialize every insertion through the page's overflow shard. The
        // flat table uses open addressing: without this keyed serialization,
        // two inserters can select different available slots for the same page
        // while a tombstone is created between their probes and CASes. Keeping
        // the shard lock across flat admission also makes overflow cleanup and
        // fallback publication one atomic tier-selection decision.
        let shard_idx = self.shard_index(page_no);
        let mut shard = self.shards[shard_idx].lock();
        let try_flat = |buf| {
            if dirty {
                self.flat_slots.try_insert_dirty(page_no, buf)
            } else {
                self.flat_slots.try_insert(page_no, buf)
            }
        };

        match try_flat(buf) {
            Ok(is_new) => {
                let duplicate_removed = shard.remove(page_no);
                is_new && !duplicate_removed
            }
            Err(buf) => {
                let admitted = shard.insert_with_dirty_state(page_no, buf, dirty);
                self.shards_dirty.store(true, Ordering::Release);
                admitted
            }
        }
    }

    /// Select the shard index for a given page number.
    ///
    /// Uses multiplicative hashing with the golden ratio constant for good
    /// distribution of sequential page numbers.
    #[inline]
    fn shard_index(&self, page_no: PageNumber) -> usize {
        Self::shard_index_for(page_no, self.shard_shift, self.shard_mask)
    }

    #[inline]
    fn shard_index_for(page_no: PageNumber, shard_shift: u32, shard_mask: usize) -> usize {
        let hash = page_no.get().wrapping_mul(GOLDEN_RATIO_32);
        (hash >> shard_shift) as usize & shard_mask
    }

    /// Number of configured partitions in the overflow shard map.
    #[must_use]
    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    fn resident_pages(&self) -> Vec<PageNumber> {
        if self.use_fast_path.load(Ordering::Relaxed) {
            if let Some(ref fast) = self.fast_array {
                return fast.lock().resident_pages();
            }
        }

        let mut residents = self.flat_slots.resident_pages();
        for shard in self.shards.iter() {
            residents.extend(shard.lock().resident_pages());
        }
        residents
    }

    /// Access the underlying page pool.
    pub fn pool(&self) -> &PageBufPool {
        &self.pool
    }

    /// Total number of pages across all shards (or fast array if enabled).
    ///
    /// Note: This acquires all shard locks briefly. For hot-path metrics,
    /// prefer `metrics_snapshot()` which aggregates all counters atomically.
    pub fn len(&self) -> usize {
        // Fast path (bd-fzr07)
        if self.use_fast_path.load(Ordering::Relaxed) {
            if let Some(ref fast) = self.fast_array {
                return fast.lock().len();
            }
        }
        // Flat slots (bd-eorms) + overflow shards
        self.flat_slots.len() + self.shards.iter().map(|s| s.lock().len()).sum::<usize>()
    }

    /// Whether the cache is empty.
    pub fn is_empty(&self) -> bool {
        // Fast path (bd-fzr07)
        if self.use_fast_path.load(Ordering::Relaxed) {
            if let Some(ref fast) = self.fast_array {
                return fast.lock().len() == 0;
            }
        }
        self.flat_slots.len() == 0 && self.shards.iter().all(|s| s.lock().pages.is_empty())
    }

    /// Check if a page is present in the cache.
    #[inline]
    pub fn contains(&self, page_no: PageNumber) -> bool {
        // Fast path (bd-fzr07)
        if self.use_fast_path.load(Ordering::Relaxed) {
            if let Some(ref fast) = self.fast_array {
                return fast.lock().contains(page_no);
            }
        }
        // Flat slots first (lock-free probe), then overflow shard
        if self.flat_slots.contains(page_no) {
            return true;
        }
        let idx = self.shard_index(page_no);
        self.shards[idx].lock().contains(page_no)
    }

    /// Retrieve a page from the cache.
    ///
    /// Returns `None` if the page is not cached. The returned slice is valid
    /// only while the internal lock is held, so this method returns owned data
    /// via a callback pattern for safety.
    ///
    /// For zero-copy access, use `with_page()` instead.
    #[inline]
    pub fn get(&self, page_no: PageNumber) -> Option<Vec<u8>> {
        // Fast path (bd-fzr07)
        if self.use_fast_path.load(Ordering::Relaxed) {
            if let Some(ref fast) = self.fast_array {
                let result = fast.lock().get(page_no).map(|s| s.to_vec());
                if result.is_some() {
                    self.record_eviction_access(page_no);
                }
                return result;
            }
        }
        // Flat slots (bd-eorms) — lock-free probe, per-slot Mutex on hit
        if let Some(data) = self.flat_slots.get_copy(page_no) {
            self.record_eviction_access(page_no);
            return Some(data);
        }
        // Overflow shard
        let idx = self.shard_index(page_no);
        let mut shard = self.shards[idx].lock();
        let result = shard.get(page_no).map(|slice| slice.to_vec());
        drop(shard);
        if result.is_some() {
            self.record_eviction_access(page_no);
        }
        result
    }

    /// Access a cached page via a callback (zero-copy pattern).
    ///
    /// The callback receives a reference to the page data. Returns `None` if
    /// the page is not cached.
    #[inline]
    pub fn with_page<R>(&self, page_no: PageNumber, f: impl FnOnce(&[u8]) -> R) -> Option<R> {
        // Fast path (bd-fzr07)
        if self.use_fast_path.load(Ordering::Relaxed) {
            if let Some(ref fast) = self.fast_array {
                let result = fast.lock().get(page_no).map(f);
                if result.is_some() {
                    self.record_eviction_access(page_no);
                }
                return result;
            }
        }
        // Flat slots (bd-eorms) — find_slot is lock-free; data Mutex on hit only
        if let Some(slot_idx) = self.flat_slots.find_slot(page_no) {
            let guard = self.flat_slots.slots[slot_idx].data.lock();
            if self.flat_slots.slots[slot_idx].pgno.load(Ordering::Acquire) == page_no.get()
                && let Some(ref buf) = *guard
            {
                self.flat_slots.hits.fetch_add(1, Ordering::Relaxed);
                let result = f(buf.as_slice());
                drop(guard);
                self.record_eviction_access(page_no);
                return Some(result);
            }
            // Data cleared between probe and lock (rare race). Fall through.
        }
        // Overflow shard
        let idx = self.shard_index(page_no);
        let mut shard = self.shards[idx].lock();
        let result = shard.get(page_no).map(f);
        drop(shard);
        if result.is_some() {
            self.record_eviction_access(page_no);
        }
        result
    }

    /// Access a cached page mutably via a callback.
    #[inline]
    pub fn with_page_mut<R>(
        &self,
        page_no: PageNumber,
        f: impl FnOnce(&mut [u8]) -> R,
    ) -> Option<R> {
        // Fast path (bd-fzr07)
        if self.use_fast_path.load(Ordering::Relaxed) {
            if let Some(ref fast) = self.fast_array {
                let result = fast.lock().get_mut(page_no).map(f);
                if result.is_some() {
                    self.record_eviction_access(page_no);
                }
                return result;
            }
        }
        // Flat slots (bd-eorms)
        if let Some(slot_idx) = self.flat_slots.find_slot(page_no) {
            let mut guard = self.flat_slots.slots[slot_idx].data.lock();
            if self.flat_slots.slots[slot_idx].pgno.load(Ordering::Acquire) == page_no.get()
                && let Some(ref mut buf) = *guard
            {
                self.flat_slots.hits.fetch_add(1, Ordering::Relaxed);
                let result = f(buf.as_mut_slice());
                drop(guard);
                self.record_eviction_access(page_no);
                return Some(result);
            }
        }
        // Overflow shard
        let idx = self.shard_index(page_no);
        let mut shard = self.shards[idx].lock();
        let result = shard.get_mut(page_no).map(f);
        drop(shard);
        if result.is_some() {
            self.record_eviction_access(page_no);
        }
        result
    }

    /// Read a page from a VFS file into the cache.
    ///
    /// If the page is already cached, returns the cached data via the callback.
    /// Otherwise, acquires a buffer from the pool, reads from VFS, caches it,
    /// and returns via the callback.
    pub fn read_page<R>(
        &self,
        cx: &Cx,
        file: &mut impl VfsFile,
        page_no: PageNumber,
        f: impl FnOnce(&[u8]) -> R,
    ) -> Result<R> {
        // Fast path (bd-fzr07)
        if self.use_fast_path.load(Ordering::Relaxed) {
            if let Some(ref fast) = self.fast_array {
                let mut arr = fast.lock();
                // Check for cache hit first
                if let Some(data) = arr.get(page_no) {
                    let result = f(data);
                    drop(arr);
                    self.record_eviction_access(page_no);
                    return Ok(result);
                }
                // Cache miss — read from VFS
                let mut buf = self.pool.acquire()?;
                let offset = page_offset(page_no, self.page_size);
                let bytes_read = file.read(cx, buf.as_mut_slice(), offset)?;
                if bytes_read < self.page_size.as_usize() {
                    return Err(FrankenError::DatabaseCorrupt {
                        detail: format!(
                            "short read fetching page {page}: got {bytes_read} of {page_size}",
                            page = page_no.get(),
                            page_size = self.page_size.as_usize()
                        ),
                    });
                }
                let result = f(buf.as_slice());
                arr.insert(page_no, buf);
                if let Some(Some(entry)) = arr.pages.get(FastPageArray::pgno_to_idx(page_no)) {
                    entry.record_access();
                }
                drop(arr);
                self.record_eviction_admit(page_no);
                return Ok(result);
            }
        }

        // Flat slots probe (bd-eorms) — lock-free miss path
        if let Some(slot_idx) = self.flat_slots.find_slot(page_no) {
            let guard = self.flat_slots.slots[slot_idx].data.lock();
            if self.flat_slots.slots[slot_idx].pgno.load(Ordering::Acquire) == page_no.get()
                && let Some(ref buf) = *guard
            {
                self.flat_slots.hits.fetch_add(1, Ordering::Relaxed);
                let result = f(buf.as_slice());
                drop(guard);
                self.record_eviction_access(page_no);
                return Ok(result);
            }
            // Data cleared between probe and lock (rare). Fall through.
        }

        // Overflow shard hit check
        let shard_idx = self.shard_index(page_no);
        {
            let mut shard = self.shards[shard_idx].lock();
            if shard.pages.contains_key(&page_no) {
                shard.hits = shard.hits.saturating_add(1);
                let data = shard.pages.get(&page_no).expect("just checked");
                let result = f(data.as_slice());
                drop(shard);
                self.record_eviction_access(page_no);
                return Ok(result);
            }
            shard.misses = shard.misses.saturating_add(1);
        }
        // Shard lock released before VFS I/O — better concurrency (bd-eorms).

        // Cache miss — read from VFS (no lock held)
        self.flat_slots.misses.fetch_add(1, Ordering::Relaxed);
        let mut buf = self.pool.acquire()?;
        let offset = page_offset(page_no, self.page_size);
        let bytes_read = file.read(cx, buf.as_mut_slice(), offset)?;

        if bytes_read < self.page_size.as_usize() {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "short read fetching page {page}: got {bytes_read} of {page_size}",
                    page = page_no.get(),
                    page_size = self.page_size.as_usize()
                ),
            });
        }

        let result = f(buf.as_slice());
        // Insert into flat slots; overflow to shard on CAS failure.
        self.insert_tiered(page_no, buf, false);
        self.note_page_access_without_metrics(page_no);
        self.record_eviction_admit(page_no);
        Ok(result)
    }

    /// Write a cached page out to a VFS file.
    pub fn write_page(&self, cx: &Cx, file: &mut impl VfsFile, page_no: PageNumber) -> Result<()> {
        // Fast path (bd-fzr07)
        if self.use_fast_path.load(Ordering::Relaxed) {
            if let Some(ref fast) = self.fast_array {
                let mut arr = fast.lock();
                if let Some(data) = arr.get(page_no) {
                    let offset = page_offset(page_no, self.page_size);
                    file.write(cx, data, offset)?;
                    if let Some(Some(entry)) = arr.pages.get(FastPageArray::pgno_to_idx(page_no)) {
                        entry.mark_clean();
                    }
                    drop(arr);
                    self.record_eviction_access(page_no);
                    return Ok(());
                }
                return Err(FrankenError::internal(format!(
                    "page {} not in cache",
                    page_no
                )));
            }
        }

        // Flat slots (bd-eorms)
        if let Some(slot_idx) = self.flat_slots.find_slot(page_no) {
            let guard = self.flat_slots.slots[slot_idx].data.lock();
            if self.flat_slots.slots[slot_idx].pgno.load(Ordering::Acquire) == page_no.get()
                && let Some(ref buf) = *guard
            {
                self.flat_slots.hits.fetch_add(1, Ordering::Relaxed);
                let offset = page_offset(page_no, self.page_size);
                file.write(cx, buf.as_slice(), offset)?;
                buf.mark_clean();
                drop(guard);
                self.record_eviction_access(page_no);
                return Ok(());
            }
        }

        // Overflow shard
        let idx = self.shard_index(page_no);
        let mut shard = self.shards[idx].lock();

        if !shard.pages.contains_key(&page_no) {
            shard.misses = shard.misses.saturating_add(1);
            return Err(FrankenError::internal(format!(
                "page {} not in cache",
                page_no
            )));
        }

        shard.hits = shard.hits.saturating_add(1);
        let buf = shard.pages.get(&page_no).expect("just checked");
        let offset = page_offset(page_no, self.page_size);
        file.write(cx, buf.as_slice(), offset)?;
        buf.mark_clean();
        drop(shard);
        self.record_eviction_access(page_no);
        Ok(())
    }

    /// Insert a fresh (zeroed) page into the cache.
    ///
    /// The callback receives a mutable reference to populate the page.
    pub fn insert_fresh<R>(
        &self,
        page_no: PageNumber,
        f: impl FnOnce(&mut [u8]) -> R,
    ) -> Result<R> {
        // Fast path (bd-fzr07)
        if self.use_fast_path.load(Ordering::Relaxed) {
            if let Some(ref fast) = self.fast_array {
                let mut arr = fast.lock();
                let mut buf = self.pool.acquire()?;
                buf.as_mut_slice().fill(0);
                let result = f(buf.as_mut_slice());
                let admitted_new = arr.insert(page_no, buf);
                if let Some(Some(entry)) = arr.pages.get(FastPageArray::pgno_to_idx(page_no)) {
                    entry.mark_dirty();
                    entry.record_access();
                }
                drop(arr);
                if admitted_new {
                    self.record_eviction_admit(page_no);
                } else {
                    self.record_eviction_access(page_no);
                }
                return Ok(result);
            }
        }

        // Allocate and zero the buffer, call f, then insert into flat slots.
        let mut buf = self.pool.acquire()?;
        buf.as_mut_slice().fill(0);
        let result = f(buf.as_mut_slice());
        let admitted_new = self.insert_tiered(page_no, buf, true);
        self.note_page_access_without_metrics(page_no);
        if admitted_new {
            self.record_eviction_admit(page_no);
        } else {
            self.record_eviction_access(page_no);
        }
        Ok(result)
    }

    /// Directly insert an existing `PageBuf` into the cache.
    pub fn insert_buffer(&self, page_no: PageNumber, buf: PageBuf) {
        // Fast path (bd-fzr07)
        if self.use_fast_path.load(Ordering::Relaxed) {
            if let Some(ref fast) = self.fast_array {
                let admitted_new = fast.lock().insert(page_no, buf);
                self.mark_page_clean(page_no);
                if admitted_new {
                    self.record_eviction_admit(page_no);
                } else {
                    self.record_eviction_access(page_no);
                }
                return;
            }
        }
        // Flat slots first; overflow to shard.
        let admitted_new = self.insert_tiered(page_no, buf, false);
        self.mark_page_clean(page_no);
        if admitted_new {
            self.record_eviction_admit(page_no);
        } else {
            self.record_eviction_access(page_no);
        }
    }

    /// Evict a specific page from the cache.
    pub fn evict(&self, page_no: PageNumber) -> bool {
        // Fast path (bd-fzr07)
        if self.use_fast_path.load(Ordering::Relaxed) {
            if let Some(ref fast) = self.fast_array {
                let removed = fast.lock().remove(page_no);
                if removed {
                    self.remove_overflow_duplicate(page_no);
                    self.forget_eviction_page(page_no);
                }
                return removed;
            }
        }
        // Try flat slots first, then overflow shard.
        if self.flat_slots.remove(page_no) {
            self.remove_overflow_duplicate(page_no);
            self.forget_eviction_page(page_no);
            return true;
        }
        let idx = self.shard_index(page_no);
        let removed = self.shards[idx].lock().remove(page_no);
        if removed {
            self.forget_eviction_page(page_no);
        }
        removed
    }

    /// Take a specific page only if its resident cache image is still clean
    /// and its buffer belongs to this cache's pool.
    ///
    /// Each storage tier rechecks dirtiness while holding the same lock that
    /// protects removal. Shared [`PageData`] snapshots remain valid because
    /// they own an independent `Arc<[u8]>`; evicting the cache's `PageBuf`
    /// cannot invalidate an already-published snapshot.
    fn take_clean_buffer_at(&self, page_no: PageNumber) -> Option<PageBuf> {
        if self.use_fast_path.load(Ordering::Relaxed)
            && let Some(ref fast) = self.fast_array
        {
            let mut fast = fast.lock();
            let idx = FastPageArray::pgno_to_idx(page_no);
            let clean = fast
                .pages
                .get(idx)
                .and_then(Option::as_ref)
                .is_some_and(|entry| !entry.is_dirty() && entry.buf.returns_to_pool(&self.pool));
            let entry = clean.then(|| fast.take(page_no)).flatten();
            drop(fast);
            if let Some(entry) = entry {
                self.forget_eviction_page(page_no);
                return Some(entry.buf);
            }
            return None;
        }

        let overflow_idx = self.shard_index(page_no);
        {
            let mut shard = self.shards[overflow_idx].lock();
            if shard.pages.contains_key(&page_no) {
                if self.flat_slots.contains_stable(page_no) {
                    // A duplicate violates the tier invariant. Refuse to
                    // remove either image and accidentally expose the other;
                    // the next authoritative insertion collapses it.
                    return None;
                }

                let clean = shard.pages.get(&page_no).is_some_and(|entry| {
                    !entry.is_dirty() && entry.buf.returns_to_pool(&self.pool)
                });
                if !clean {
                    return None;
                }
                let entry = shard
                    .pages
                    .remove(&page_no)
                    .expect("clean overflow cache entry must still be present");
                shard.evictions = shard.evictions.saturating_add(1);
                drop(shard);
                self.forget_eviction_page(page_no);
                return Some(entry.buf);
            }
        }

        if let Some(buffer) = self.flat_slots.take_if_clean_from_pool(page_no, &self.pool) {
            self.forget_eviction_page(page_no);
            return Some(buffer);
        }
        None
    }

    /// Atomically remove and return one clean buffer from this cache's pool.
    ///
    /// This is the write-admission counterpart to [`Self::evict_any`]. It is
    /// intentionally fail-closed for dirty entries: transaction staging must
    /// never recover capacity by discarding an unpublished or unflushed page.
    /// The path is only used after the shared [`PageBufPool`] reaches its
    /// configured ceiling, so the diagnostic snapshot and deterministic
    /// victim scan do not burden ordinary cache hits.
    pub(crate) fn take_clean_buffer(&self) -> Option<PageBuf> {
        if let Some(preferred) = self.preferred_reconstructed_victim()
            && let Some(buffer) = self.take_clean_buffer_at(preferred)
        {
            return Some(buffer);
        }

        let mut candidates = self.page_snapshots();
        candidates.sort_unstable_by_key(|snapshot| (snapshot.access_count, snapshot.page_no.get()));
        candidates
            .into_iter()
            .filter(|snapshot| !snapshot.dirty)
            .find_map(|snapshot| self.take_clean_buffer_at(snapshot.page_no))
    }

    /// Evict one clean pool-backed cache page and return its buffer to the
    /// free list. Callers that immediately need the allocation should prefer
    /// `Self::take_clean_buffer` to avoid a free-list race.
    pub fn evict_clean_any(&self) -> bool {
        self.take_clean_buffer().is_some()
    }

    /// Evict an arbitrary page from the cache.
    ///
    /// Tries flat slots first, then iterates shards.
    /// Returns `true` if a page was evicted.
    pub fn evict_any(&self) -> bool {
        let preferred_victim = self.preferred_reconstructed_victim();
        if let Some(page_no) = preferred_victim
            && self.evict(page_no)
        {
            return true;
        }

        // Fast path (bd-fzr07)
        if self.use_fast_path.load(Ordering::Relaxed) {
            if let Some(ref fast) = self.fast_array {
                let removed = fast.lock().remove_any();
                if let Some(page_no) = removed {
                    self.remove_overflow_duplicate(page_no);
                    self.forget_eviction_page(page_no);
                    return true;
                }
                return false;
            }
        }
        // Flat slots first (bd-eorms)
        if let Some(page_no) = self.flat_slots.remove_any_page() {
            self.remove_overflow_duplicate(page_no);
            self.forget_eviction_page(page_no);
            return true;
        }
        // Overflow shards. Spread the probe start across shards with a
        // deterministic round-robin cursor (bd-w4yc9). The previous
        // `std::time::Instant::now().elapsed()` source measured ~0 ns, so the
        // start was almost always shard 0 (poor spread) AND was a
        // non-deterministic ambient-authority use; a process-global atomic
        // round-robin avoids wall-clock time and actually rotates the start.
        static EVICT_PROBE_CURSOR: AtomicUsize = AtomicUsize::new(0);
        let start = EVICT_PROBE_CURSOR.fetch_add(1, Ordering::Relaxed) & self.shard_mask;
        for i in 0..self.shards.len() {
            let idx = (start + i) & self.shard_mask;
            let mut shard = self.shards[idx].lock();
            if let Some(page_no) = shard.remove_any() {
                drop(shard);
                self.forget_eviction_page(page_no);
                return true;
            }
        }
        false
    }

    fn preferred_reconstructed_victim(&self) -> Option<PageNumber> {
        if !self.eviction_tracking_enabled() {
            return None;
        }
        let tracks_reconstruction = {
            let tracker = self.eviction_policy.lock();
            matches!(
                &*tracker,
                PageCacheEvictionTracker::S3Fifo(_)
                    | PageCacheEvictionTracker::Arc(_)
                    | PageCacheEvictionTracker::S3FifoAdaptive(_)
            )
        };
        if !tracks_reconstruction
            || self.metrics_lightweight_snapshot().cached_pages
                > S3_FIFO_RECONSTRUCTED_EVICTION_MAX_RESIDENTS
        {
            return None;
        }

        let residents = self.resident_pages();
        let tracker = self.eviction_policy.lock();
        tracker.choose_victim(&residents)
    }

    /// Evict all pages from the cache.
    ///
    /// Unlike fast-path mode transitions, `clear()` preserves the fast-array's
    /// backing allocation so callers can cheaply reuse the same sparse working
    /// set after an explicit cache flush.
    pub fn clear(&self) {
        if let Some(ref fast) = self.fast_array {
            fast.lock().clear();
        }
        self.flat_slots.clear();
        // Skip the 128-shard mutex walk when overflow shards are
        // observably empty. The flag is set by overflow inserts after
        // their lock release, so any insert ordered before our swap
        // is visible to the subsequent shard walk; an insert ordered
        // after our swap re-arms the flag for the next clear.
        if self.shards_dirty.swap(false, Ordering::AcqRel) {
            for shard in self.shards.iter() {
                shard.lock().clear();
            }
        }
        self.clear_eviction_history();
    }

    /// Capture cheap-to-compute cache counters, skipping per-slot iteration.
    ///
    /// This is a hot-path-safe alternative to `metrics_snapshot()` for callers
    /// that only need `{hits, misses, admits, evictions, cached_pages,
    /// pool_capacity}` — notably `Connection::refresh_eprocess_oracle`, which
    /// samples a cache miss-ratio at ~1-of-64 statements.
    ///
    /// Full `metrics_snapshot()` walks every resident `PageSlot` via
    /// `stable_snapshot`, collects a `Vec<PageCachePageSnapshot>`, iterates
    /// again for `dirty_ratio_pct`, and locks the eviction policy to sample
    /// queue sizes. None of that data is needed for the e-process oracle,
    /// yet the iteration cost amortized to ~5% self-time at MT 8t on the
    /// 2026-04-23 hotspot capture (bd-m4s2c).
    #[must_use]
    pub fn metrics_lightweight_snapshot(&self) -> PageCacheLightweightSnapshot {
        if self.use_fast_path.load(Ordering::Relaxed) {
            if let Some(ref fast) = self.fast_array {
                let arr = fast.lock();
                return PageCacheLightweightSnapshot {
                    hits: arr.hits,
                    misses: arr.misses,
                    admits: arr.admits,
                    evictions: arr.evictions,
                    cached_pages: arr.len(),
                    pool_capacity: self.pool.capacity(),
                };
            }
        }
        let mut total_hits = self.flat_slots.hits.load(Ordering::Relaxed);
        let mut total_misses = self.flat_slots.misses.load(Ordering::Relaxed);
        let mut total_admits = self.flat_slots.admits.load(Ordering::Relaxed);
        let mut total_evictions = self.flat_slots.evictions.load(Ordering::Relaxed);
        let mut total_pages = self.flat_slots.len();
        for shard in self.shards.iter() {
            let s = shard.lock();
            total_hits = total_hits.saturating_add(s.hits);
            total_misses = total_misses.saturating_add(s.misses);
            total_admits = total_admits.saturating_add(s.admits);
            total_evictions = total_evictions.saturating_add(s.evictions);
            total_pages = total_pages.saturating_add(s.len());
        }
        PageCacheLightweightSnapshot {
            hits: total_hits,
            misses: total_misses,
            admits: total_admits,
            evictions: total_evictions,
            cached_pages: total_pages,
            pool_capacity: self.pool.capacity(),
        }
    }

    /// Capture current cache metrics aggregated across all shards.
    #[must_use]
    pub fn metrics_snapshot(&self) -> PageCacheMetricsSnapshot {
        // Fast path (bd-fzr07)
        if self.use_fast_path.load(Ordering::Relaxed) {
            if let Some(ref fast) = self.fast_array {
                let arr = fast.lock();
                let dirty_pages = arr
                    .pages
                    .iter()
                    .filter(|slot| slot.as_ref().is_some_and(CachedPageEntry::is_dirty))
                    .count();
                let queue_snapshot = {
                    let tracker = self.eviction_policy.lock();
                    if matches!(&*tracker, PageCacheEvictionTracker::S3Fifo(_)) {
                        let residents = arr.resident_pages();
                        tracker.queue_snapshot(&residents)
                    } else {
                        None
                    }
                };
                let (t1_size, t2_size, b1_size, p_target) = if let Some(snapshot) = queue_snapshot {
                    (
                        snapshot.small_len,
                        snapshot.main_len,
                        snapshot.ghost_len,
                        snapshot.small_capacity,
                    )
                } else {
                    (arr.len(), 0, 0, arr.len())
                };
                return PageCacheMetricsSnapshot {
                    hits: arr.hits,
                    misses: arr.misses,
                    admits: arr.admits,
                    evictions: arr.evictions,
                    cached_pages: arr.len(),
                    pool_capacity: self.pool.capacity(),
                    dirty_ratio_pct: percent_ratio_u64(dirty_pages, arr.len()),
                    t1_size,
                    t2_size,
                    b1_size,
                    b2_size: 0,
                    p_target,
                    mvcc_multi_version_pages: 0,
                };
            }
        }

        // Flat slots metrics (bd-eorms)
        let mut total_hits = self.flat_slots.hits.load(Ordering::Relaxed);
        let mut total_misses = self.flat_slots.misses.load(Ordering::Relaxed);
        let mut total_admits = self.flat_slots.admits.load(Ordering::Relaxed);
        let mut total_evictions = self.flat_slots.evictions.load(Ordering::Relaxed);
        let mut total_pages = self.flat_slots.len();
        let flat_snapshots: Vec<PageCachePageSnapshot> = self
            .flat_slots
            .slots
            .iter()
            .filter_map(PageSlot::stable_snapshot)
            .collect();
        let capture_queue_snapshot = {
            let tracker = self.eviction_policy.lock();
            matches!(&*tracker, PageCacheEvictionTracker::S3Fifo(_))
        };
        let mut dirty_pages = flat_snapshots
            .iter()
            .filter(|snapshot| snapshot.dirty)
            .count();
        let mut residents: Vec<PageNumber> = if capture_queue_snapshot {
            flat_snapshots
                .iter()
                .map(|snapshot| snapshot.page_no)
                .collect()
        } else {
            Vec::new()
        };

        // Add overflow shard metrics
        for shard in self.shards.iter() {
            let s = shard.lock();
            total_hits = total_hits.saturating_add(s.hits);
            total_misses = total_misses.saturating_add(s.misses);
            total_admits = total_admits.saturating_add(s.admits);
            total_evictions = total_evictions.saturating_add(s.evictions);
            total_pages += s.len();
            dirty_pages += s.pages.values().filter(|entry| entry.is_dirty()).count();
            if capture_queue_snapshot {
                residents.extend(s.pages.keys().copied());
            }
        }

        let queue_snapshot = if capture_queue_snapshot {
            self.eviction_policy.lock().queue_snapshot(&residents)
        } else {
            None
        };
        let (t1_size, t2_size, b1_size, p_target) = if let Some(snapshot) = queue_snapshot {
            (
                snapshot.small_len,
                snapshot.main_len,
                snapshot.ghost_len,
                snapshot.small_capacity,
            )
        } else {
            (total_pages, 0, 0, total_pages)
        };

        PageCacheMetricsSnapshot {
            hits: total_hits,
            misses: total_misses,
            admits: total_admits,
            evictions: total_evictions,
            cached_pages: total_pages,
            pool_capacity: self.pool.capacity(),
            dirty_ratio_pct: percent_ratio_u64(dirty_pages, total_pages),
            t1_size,
            t2_size,
            b1_size,
            b2_size: 0,
            p_target,
            mvcc_multi_version_pages: 0,
        }
    }

    /// Capture a read-only snapshot of the resident cache pages.
    #[must_use]
    pub fn page_snapshots(&self) -> Vec<PageCachePageSnapshot> {
        let mut snapshots = Vec::new();

        if self.use_fast_path.load(Ordering::Relaxed) {
            if let Some(ref fast) = self.fast_array {
                let arr = fast.lock();
                for (idx, slot) in arr.pages.iter().enumerate() {
                    let Some(entry) = slot.as_ref() else {
                        continue;
                    };
                    let Some(pgno) = u32::try_from(idx.saturating_add(1))
                        .ok()
                        .and_then(PageNumber::new)
                    else {
                        continue;
                    };
                    snapshots.push(snapshot_cached_page(pgno, entry));
                }
            }
        } else {
            snapshots.extend(
                self.flat_slots
                    .slots
                    .iter()
                    .filter_map(PageSlot::stable_snapshot),
            );

            for shard in self.shards.iter() {
                let shard = shard.lock();
                for (&page_no, entry) in &shard.pages {
                    snapshots.push(snapshot_cached_page(page_no, entry));
                }
            }
        }

        let resident_pages: Vec<PageNumber> =
            snapshots.iter().map(|snapshot| snapshot.page_no).collect();
        let queue_assignments = self
            .eviction_policy
            .lock()
            .queue_assignments(&resident_pages);
        for snapshot in &mut snapshots {
            snapshot.queue = queue_assignments.get(&snapshot.page_no).copied();
        }

        snapshots.sort_unstable_by_key(|snapshot| snapshot.page_no.get());
        snapshots
    }

    /// Reset cache counters while preserving resident pages.
    pub fn reset_metrics(&self) {
        // Fast path (bd-fzr07)
        if self.use_fast_path.load(Ordering::Relaxed) {
            if let Some(ref fast) = self.fast_array {
                fast.lock().reset_metrics();
                return;
            }
        }
        self.flat_slots.reset_metrics();
        for shard in self.shards.iter() {
            shard.lock().reset_metrics();
        }
    }

    /// Get the configured page size.
    #[must_use]
    pub fn page_size(&self) -> PageSize {
        self.page_size
    }

    /// Get shard distribution statistics (for testing/debugging).
    #[must_use]
    pub fn shard_distribution(&self) -> Vec<usize> {
        let mut distribution = vec![0usize; self.shards.len()];

        if self.use_fast_path.load(Ordering::Relaxed) {
            if let Some(ref fast) = self.fast_array {
                let resident_pages = fast.lock().resident_pages();
                for page_no in resident_pages {
                    let idx = self.shard_index(page_no);
                    distribution[idx] += 1;
                }
            }
            return distribution;
        }

        for snapshot in self
            .flat_slots
            .slots
            .iter()
            .filter_map(PageSlot::stable_snapshot)
        {
            let idx = self.shard_index(snapshot.page_no);
            distribution[idx] += 1;
        }

        for (idx, shard) in self.shards.iter().enumerate() {
            distribution[idx] += shard.lock().len();
        }

        distribution
    }

    /// Read a page from VFS and return an owned copy.
    ///
    /// This is a convenience method that wraps `read_page` with a copy
    /// operation, matching the common usage pattern in pager code.
    pub fn read_page_copy(
        &self,
        cx: &Cx,
        file: &mut impl VfsFile,
        page_no: PageNumber,
    ) -> Result<Vec<u8>> {
        self.read_page(cx, file, page_no, |data| data.to_vec())
    }

    /// Get a cached page and return an owned copy.
    ///
    /// Returns `None` if the page is not cached.
    #[inline]
    pub fn get_copy(&self, page_no: PageNumber) -> Option<Vec<u8>> {
        // Fast path (bd-fzr07)
        if self.use_fast_path.load(Ordering::Relaxed) {
            if let Some(ref fast) = self.fast_array {
                let result = fast.lock().get(page_no).map(|data| data.to_vec());
                if result.is_some() {
                    self.record_eviction_access(page_no);
                }
                return result;
            }
        }
        // Flat slots (bd-eorms)
        if let Some(data) = self.flat_slots.get_copy(page_no) {
            self.record_eviction_access(page_no);
            return Some(data);
        }
        // Overflow shard
        let idx = self.shard_index(page_no);
        let mut shard = self.shards[idx].lock();
        let result = shard.get(page_no).map(|data| data.to_vec());
        drop(shard);
        if result.is_some() {
            self.record_eviction_access(page_no);
        }
        result
    }

    /// bd-perf (V1.2): Return a shared `PageData` (Arc) instead of copying
    /// the page bytes. Each cache entry materializes at most one immutable
    /// shared snapshot, then hot reads clone that snapshot until a mutation
    /// invalidates it.
    pub fn get_shared(&self, page_no: PageNumber) -> Option<PageData> {
        // Fast path (bd-fzr07)
        if self.use_fast_path.load(Ordering::Relaxed) {
            if let Some(ref fast) = self.fast_array {
                let result = fast.lock().get_shared(page_no);
                if result.is_some() {
                    self.record_eviction_access(page_no);
                }
                return result;
            }
        }
        // Flat slots (bd-eorms)
        if let Some(data) = self.flat_slots.get_shared(page_no) {
            self.record_eviction_access(page_no);
            return Some(data);
        }
        // Overflow shard
        let idx = self.shard_index(page_no);
        let mut shard = self.shards[idx].lock();
        let result = shard.get_shared(page_no);
        drop(shard);
        if result.is_some() {
            self.record_eviction_access(page_no);
        }
        result
    }

    /// Best-effort software prefetch for an upcoming `page_no` lookup.
    ///
    /// This intentionally avoids blocking. It warms the likely flat-slot or
    /// shard metadata, and opportunistically prefetches resident page bytes
    /// when a non-blocking lock can observe them.
    pub fn prefetch_page_hint(&self, page_no: PageNumber) {
        if self.use_fast_path.load(Ordering::Relaxed) {
            if let Some(ref fast) = self.fast_array {
                prefetch_l1_read(std::ptr::from_ref(fast));
                if let Some(guard) = fast.try_lock() {
                    guard.prefetch_page_hint(page_no);
                }
            }
            return;
        }

        self.flat_slots.prefetch_page_hint(page_no);

        let shard_idx = self.shard_index(page_no);
        let shard = &self.shards[shard_idx];
        prefetch_l1_read(std::ptr::from_ref(shard));
        if let Some(guard) = shard.try_lock()
            && let Some(entry) = guard.pages.get(&page_no)
        {
            entry.prefetch_hint();
        }
    }
}

impl std::fmt::Debug for ShardedPageCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let metrics = self.metrics_snapshot();
        let fast_path = self.use_fast_path.load(Ordering::Relaxed);
        f.debug_struct("ShardedPageCache")
            .field("shard_count", &self.shards.len())
            .field("page_size", &self.page_size)
            .field("fast_path_enabled", &fast_path)
            .field("flat_slots", &self.flat_slots)
            .field("cached_pages", &metrics.cached_pages)
            .field("hits", &metrics.hits)
            .field("misses", &metrics.misses)
            .field("admits", &metrics.admits)
            .field("evictions", &metrics.evictions)
            .finish_non_exhaustive()
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn percent_ratio_u64(numerator: usize, denominator: usize) -> u64 {
    if denominator == 0 {
        return 0;
    }

    let numerator = u64::try_from(numerator).unwrap_or(u64::MAX);
    let denominator = u64::try_from(denominator).unwrap_or(u64::MAX).max(1);
    numerator
        .saturating_mul(100)
        .saturating_add(denominator / 2)
        .checked_div(denominator)
        .unwrap_or(0)
}

/// Compute the byte offset of a page within the database file.
///
/// Pages are 1-indexed, so page 1 starts at offset 0.
#[inline]
fn page_offset(page_no: PageNumber, page_size: PageSize) -> u64 {
    u64::from(page_no.get() - 1) * u64::from(page_size.get())
}

/// Read a database file header from a VFS file into a stack-allocated buffer.
///
/// The 100-byte SQLite database header is small enough for a stack buffer.
/// This does NOT violate the zero-copy principle — §1.5 explicitly permits
/// "small stack buffers for fixed-size headers."
///
/// Returns the raw header bytes.
pub fn read_db_header(cx: &Cx, file: &mut impl VfsFile) -> Result<[u8; 100]> {
    let mut header = [0u8; 100];
    let bytes_read = file.read(cx, &mut header, 0)?;
    if bytes_read < 100 {
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!("database header short read: expected 100 bytes, got {bytes_read}"),
        });
    }
    Ok(header)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::cast_possible_truncation)]
mod tests {
    use super::*;
    use crate::s3_fifo::{QueueKind, S3Fifo, S3FifoConfig, S3FifoEvent};
    use fsqlite_types::flags::VfsOpenFlags;
    use fsqlite_vfs::{MemoryVfs, Vfs};
    use serde_json::json;
    use std::collections::{HashMap, VecDeque};
    use std::hint::black_box;
    use std::path::Path;
    use std::time::{Duration, Instant};

    const BEAD_ID: &str = "bd-22n.2";
    const BEAD_TRACK_F: &str = "bd-pm1zd";
    const BEAD_TRACK_Q: &str = "bd-aztlm";
    const BEAD_TZLZB: &str = "bd-tzlzb";
    const BEAD_CACHE_MONITOR: &str = "bd-t6sv2.8";

    fn elapsed_ns(duration: Duration) -> u64 {
        u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
    }

    fn page_pattern(page_no: PageNumber) -> u8 {
        let folded = page_no.get().wrapping_mul(37).wrapping_add(11) & 0xFF;
        u8::try_from(folded).expect("masked page pattern must fit in u8")
    }

    fn percentile_u64(samples: &[u64], percentile: u32) -> u64 {
        assert!(
            !samples.is_empty(),
            "percentile input must contain at least one sample"
        );
        let mut sorted = samples.to_vec();
        sorted.sort_unstable();
        let pct = percentile.clamp(1, 100);
        let rank = ((sorted.len() - 1) * usize::try_from(pct).expect("pct fits")) / 100;
        sorted[rank]
    }

    #[allow(clippy::too_many_arguments)]
    fn emit_track_f_log(
        test_name: &str,
        phase: &str,
        elapsed: Duration,
        page_count: usize,
        lock_acquisitions: u64,
        cache_hits: u64,
        cache_misses: u64,
        extra: serde_json::Value,
    ) {
        eprintln!(
            "TRACK_F:{}",
            json!({
                "bead_id": BEAD_TRACK_F,
                "test_name": test_name,
                "phase": phase,
                "elapsed_ns": elapsed_ns(elapsed),
                "page_count": page_count,
                "lock_acquisitions": lock_acquisitions,
                "cache_hits": cache_hits,
                "cache_misses": cache_misses,
                "extra": extra
            })
        );
    }

    #[allow(clippy::too_many_arguments)]
    fn emit_track_q_log(
        test_name: &str,
        phase: &str,
        elapsed: Duration,
        page_count: usize,
        bucket_access_count: u64,
        chain_walk_count: u64,
        resize_count: u64,
        cache_hit_rate: f64,
        extra: serde_json::Value,
    ) {
        eprintln!(
            "TRACK_Q:{}",
            json!({
                "bead_id": BEAD_TRACK_Q,
                "test_name": test_name,
                "phase": phase,
                "elapsed_ns": elapsed_ns(elapsed),
                "page_count": page_count,
                "bucket_access_count": bucket_access_count,
                "chain_walk_count": chain_walk_count,
                "resize_count": resize_count,
                "cache_hit_rate": cache_hit_rate,
                "extra": extra
            })
        );
    }

    fn emit_cache_monitor_log(
        test_name: &str,
        workload_type: &str,
        elapsed: Duration,
        snapshot: PageCacheMetricsSnapshot,
        extra: serde_json::Value,
    ) {
        eprintln!(
            "CACHE_MONITOR:{}",
            json!({
                "bead_id": BEAD_CACHE_MONITOR,
                "test_name": test_name,
                "workload_type": workload_type,
                "elapsed_ns": elapsed_ns(elapsed),
                "hits": snapshot.hits,
                "misses": snapshot.misses,
                "total_accesses": snapshot.total_accesses(),
                "hit_rate_pct": snapshot.hit_rate_percent(),
                "eviction_count": snapshot.evictions,
                "dirty_ratio_pct": snapshot.dirty_ratio_pct,
                "t1_size": snapshot.t1_size,
                "t2_size": snapshot.t2_size,
                "b1_size": snapshot.b1_size,
                "b2_size": snapshot.b2_size,
                "p_target": snapshot.p_target,
                "cached_pages": snapshot.cached_pages,
                "pool_capacity": snapshot.pool_capacity,
                "mvcc_multi_version_pages": snapshot.mvcc_multi_version_pages,
                "extra": extra
            })
        );
    }

    fn track_q_page_buf(page_no: PageNumber) -> PageBuf {
        let pattern = page_pattern(page_no);
        let mut buf = PageBuf::new(PageSize::DEFAULT);
        buf.as_mut_slice().fill(pattern);
        buf.as_mut_slice()[..4].copy_from_slice(&page_no.get().to_le_bytes());
        buf
    }

    fn assert_track_q_page(page_no: PageNumber, data: &[u8]) {
        let header = page_no.get().to_le_bytes();
        assert_eq!(
            &data[..4],
            &header,
            "TRACK_Q page header mismatch for page {}",
            page_no.get()
        );
        assert_eq!(
            data[PageSize::DEFAULT.as_usize() - 1],
            page_pattern(page_no),
            "TRACK_Q page tail mismatch for page {}",
            page_no.get()
        );
    }

    fn track_q_probe_distance(slots: &FlatPageSlots, page_no: PageNumber) -> usize {
        let slot_idx = slots.find_slot(page_no).expect("page should be present");
        let start = slots.hash_pgno(page_no.get());
        slot_idx.wrapping_sub(start) & slots.mask
    }

    fn track_q_collision_pages(
        slots: &FlatPageSlots,
        target_bucket: usize,
        wanted: usize,
    ) -> Vec<PageNumber> {
        let mut pages = Vec::with_capacity(wanted);
        let mut candidate = 1_u32;
        while pages.len() < wanted {
            let page_no = PageNumber::new(candidate).expect("collision candidate page number");
            if slots.hash_pgno(candidate) == target_bucket {
                pages.push(page_no);
            }
            candidate = candidate
                .checked_add(1)
                .expect("collision search should not exhaust page numbers");
        }
        pages
    }

    fn track_q_hit_rate(hits: u64, misses: u64) -> f64 {
        let total = hits.saturating_add(misses);
        if total == 0 {
            return 1.0;
        }
        f64::from(u32::try_from(hits.min(u64::from(u32::MAX))).expect("hit count fits u32"))
            / f64::from(u32::try_from(total.min(u64::from(u32::MAX))).expect("total fits u32"))
    }

    fn monotonic_test_clock() -> Instant {
        let now = Instant::now;
        now()
    }

    fn populate_monitored_page(page_no: PageNumber, data: &mut [u8]) {
        data[..4].copy_from_slice(&page_no.get().to_le_bytes());
        data[4] = page_pattern(page_no);
        let last = data
            .len()
            .checked_sub(1)
            .expect("page buffer should never be empty");
        data[last] = page_pattern(page_no);
    }

    fn touch_monitored_page(cache: &ShardedPageCache, page_no: PageNumber) {
        if cache.get_copy(page_no).is_some() {
            return;
        }

        loop {
            match cache.insert_fresh(page_no, |data| populate_monitored_page(page_no, data)) {
                Ok(()) => return,
                Err(FrankenError::OutOfMemory) => {
                    assert!(
                        cache.evict_any(),
                        "cache monitor workload admission must free one victim"
                    );
                }
                Err(err) => {
                    assert!(
                        matches!(err, FrankenError::OutOfMemory),
                        "cache monitor workload insert failed for page {page_no}: {err}"
                    );
                }
            }
        }
    }

    fn lane_counter(data: &[u8], lane: usize) -> u32 {
        let offset = lane * std::mem::size_of::<u32>();
        let bytes: [u8; 4] = data[offset..offset + 4]
            .try_into()
            .expect("lane counter bytes");
        u32::from_le_bytes(bytes)
    }

    fn set_lane_counter(data: &mut [u8], lane: usize, value: u32) {
        let offset = lane * std::mem::size_of::<u32>();
        data[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
    }

    fn setup() -> (Cx, impl VfsFile) {
        let cx = Cx::new();
        let vfs = MemoryVfs::new();
        let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE;
        let (file, _) = vfs.open(&cx, Some(Path::new("test.db")), flags).unwrap();
        (cx, file)
    }

    #[cfg(unix)]
    #[test]
    fn test_spawn_blocking_io_read_page() {
        use asupersync::runtime::{RuntimeBuilder, spawn_blocking_io};
        use std::io::{ErrorKind, Write as _};
        use std::os::unix::fs::FileExt as _;
        use std::sync::Arc;
        use tempfile::NamedTempFile;

        fn read_exact_at(file: &std::fs::File, buf: &mut [u8], offset: u64) -> std::io::Result<()> {
            let mut total = 0_usize;
            while total < buf.len() {
                #[allow(clippy::cast_possible_truncation)]
                let off = offset + total as u64;
                let n = file.read_at(&mut buf[total..], off)?;
                if n == 0 {
                    return Err(std::io::Error::new(ErrorKind::UnexpectedEof, "short read"));
                }
                total += n;
            }
            Ok(())
        }

        let mut tmp = NamedTempFile::new().unwrap();
        let page_data: Vec<u8> = (0..4096u16)
            .map(|i| u8::try_from(i % 256).expect("i % 256 fits in u8"))
            .collect();
        tmp.as_file_mut().write_all(&page_data).unwrap();
        tmp.as_file_mut().flush().unwrap();

        let file = Arc::new(tmp.reopen().unwrap());
        let pool = PageBufPool::new(PageSize::DEFAULT, 1);

        let rt = RuntimeBuilder::low_latency()
            .worker_threads(1)
            .blocking_threads(1, 1)
            .build()
            .unwrap();

        let join = rt.handle().spawn(async move {
            let worker_tid = std::thread::current().id();

            let mut buf = pool.acquire().unwrap();
            let file2 = Arc::clone(&file);
            let (buf, io_tid) = spawn_blocking_io(move || {
                let io_tid = std::thread::current().id();
                read_exact_at(file2.as_ref(), buf.as_mut_slice(), 0)?;
                Ok::<_, std::io::Error>((buf, io_tid))
            })
            .await
            .unwrap();

            assert_ne!(
                io_tid, worker_tid,
                "spawn_blocking_io must dispatch work to a blocking thread"
            );
            assert_eq!(
                buf.as_slice(),
                page_data.as_slice(),
                "bead_id={BEAD_ID} case=spawn_blocking_io_read_page data mismatch"
            );

            drop(buf);
            assert_eq!(
                pool.available(),
                1,
                "bead_id={BEAD_ID} case=spawn_blocking_io_read_page buf must return to pool"
            );
        });

        rt.block_on(join);
    }

    #[test]
    fn test_spawn_blocking_io_no_unsafe() {
        // Workspace-wide lint gate: unsafe code is forbidden.
        let manifest = include_str!("../../../Cargo.toml");
        assert!(
            manifest.contains(r#"unsafe_code = "forbid""#),
            "workspace must keep unsafe_code=forbid for IO dispatch paths"
        );
    }

    #[test]
    fn test_blocking_pool_lab_mode_inline() {
        use asupersync::lab::{LabConfig, LabRuntime};
        use asupersync::runtime::spawn_blocking_io;
        use asupersync::types::Budget;
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering};

        let mut rt = LabRuntime::new(LabConfig::new(42));
        let region = rt.state.create_root_region(Budget::INFINITE);

        let ok = Arc::new(AtomicBool::new(false));
        let ok_task = Arc::clone(&ok);

        let (task_id, _handle) = rt
            .state
            .create_task(region, Budget::INFINITE, async move {
                let worker_tid = std::thread::current().id();
                let io_tid =
                    spawn_blocking_io(|| Ok::<_, std::io::Error>(std::thread::current().id()))
                        .await
                        .unwrap();
                ok_task.store(worker_tid == io_tid, Ordering::Release);
            })
            .unwrap();

        rt.scheduler.lock().schedule(task_id, 0);
        rt.run_until_quiescent();

        assert!(
            ok.load(Ordering::Acquire),
            "spawn_blocking_io must execute inline when no blocking pool exists (lab determinism)"
        );
    }

    #[test]
    fn test_cancel_mid_io_returns_buf_to_pool() {
        use asupersync::runtime::{RuntimeBuilder, spawn_blocking_io, yield_now};
        use std::future::poll_fn;
        use std::task::Poll;
        use std::time::Duration;

        let rt = RuntimeBuilder::low_latency()
            .worker_threads(1)
            .blocking_threads(1, 1)
            .build()
            .unwrap();

        let pool = PageBufPool::new(PageSize::DEFAULT, 1);
        let join = rt.handle().spawn(async move {
            let buf = pool.acquire().unwrap();

            let mut fut = Box::pin(spawn_blocking_io(move || {
                std::thread::sleep(Duration::from_millis(20));
                Ok::<_, std::io::Error>(buf)
            }));

            // Poll once to ensure the blocking task is enqueued, then drop the
            // future (soft cancel). The owned PageBuf must be returned to the pool.
            let mut polled = false;
            poll_fn(|cx| {
                if !polled {
                    polled = true;
                    let _ = fut.as_mut().poll(cx);
                }
                Poll::Ready(())
            })
            .await;

            drop(fut);

            // The blocking task sleeps 20ms then drops the PageBuf.
            // Yield in a loop with a brief real-time sleep per iteration so
            // the blocking thread has time to finish and return the buffer.
            for _ in 0..200u32 {
                if pool.available() == 1 {
                    break;
                }
                std::thread::sleep(Duration::from_millis(1));
                yield_now().await;
            }
            assert_eq!(
                pool.available(),
                1,
                "bead_id={BEAD_ID} case=cancel_mid_io_returns_buf_to_pool"
            );
        });

        rt.block_on(join);
    }

    #[test]
    fn test_pager_reads_pages_via_pool() {
        let (cx, mut file) = setup();
        let page_data = vec![0xAB_u8; 4096];
        file.write(&cx, &page_data, 0).unwrap();

        let pool = PageBufPool::new(PageSize::DEFAULT, 4);
        let mut cache = PageCache::with_pool(pool.clone(), PageSize::DEFAULT);
        let read = cache.read_page(&cx, &mut file, PageNumber::ONE).unwrap();
        assert_eq!(read, page_data.as_slice());
        assert_eq!(pool.available(), 0, "cached page still holds the buffer");

        assert!(cache.evict(PageNumber::ONE));
        assert_eq!(
            pool.available(),
            1,
            "evicting a cached page should return its buffer to the pool"
        );
    }

    // --- test_vfs_read_no_intermediate_alloc ---

    #[test]
    fn test_vfs_read_no_intermediate_alloc() {
        // Demonstrate that VfsFile::read writes directly into the PageBuf
        // memory with no intermediate buffer.  We verify by checking that
        // the data appears at the same pointer address as the PageBuf slice.
        let (cx, mut file) = setup();

        // Write a recognizable page to the file.
        let pattern: Vec<u8> = (0..4096u16)
            .map(|i| u8::try_from(i % 256).expect("i % 256 fits in u8"))
            .collect();
        file.write(&cx, &pattern, 0).unwrap();

        // Acquire a PageBuf from the pool and read directly into it.
        let pool = PageBufPool::new(PageSize::DEFAULT, 4);
        let mut buf = pool.acquire().unwrap();
        let ptr_before = buf.as_ptr();

        // VfsFile::read takes &mut [u8] — PageBuf::as_mut_slice gives us
        // a reference to the same aligned memory.
        file.read(&cx, buf.as_mut_slice(), 0).unwrap();

        let ptr_after = buf.as_ptr();
        assert_eq!(
            ptr_before, ptr_after,
            "bead_id={BEAD_ID} case=vfs_read_no_intermediate_alloc \
             pointer must not change — read goes directly into PageBuf"
        );
        assert_eq!(
            buf.as_slice(),
            pattern.as_slice(),
            "bead_id={BEAD_ID} case=vfs_read_data_correct"
        );
    }

    // --- test_vfs_write_no_intermediate_alloc ---

    #[test]
    fn test_vfs_write_no_intermediate_alloc() {
        // Demonstrate that VfsFile::write reads directly from the PageBuf
        // memory with no intermediate staging copy.
        let (cx, mut file) = setup();

        let pool = PageBufPool::new(PageSize::DEFAULT, 4);
        let mut buf = pool.acquire().unwrap();

        // Fill with a recognizable pattern.
        for (i, b) in buf.as_mut_slice().iter_mut().enumerate() {
            *b = u8::try_from(i % 251).expect("i % 251 fits in u8"); // prime-sized pattern
        }

        let ptr_before = buf.as_ptr();

        // VfsFile::write takes &[u8] — PageBuf::as_slice gives us a
        // reference to the same aligned memory, no copy.
        file.write(&cx, buf.as_slice(), 0).unwrap();

        let ptr_after = buf.as_ptr();
        assert_eq!(
            ptr_before, ptr_after,
            "bead_id={BEAD_ID} case=vfs_write_no_intermediate_alloc \
             PageBuf pointer must be stable through write"
        );

        // Verify the data was written correctly.
        let mut verify = vec![0u8; 4096];
        file.read(&cx, &mut verify, 0).unwrap();
        assert_eq!(
            verify.as_slice(),
            buf.as_slice(),
            "bead_id={BEAD_ID} case=vfs_write_data_roundtrip"
        );
    }

    // --- test_pager_returns_ref_not_copy ---

    #[test]
    fn test_pager_returns_ref_not_copy() {
        // PageCache::get() returns &[u8] that points to the same memory
        // as the stored PageBuf — a reference, not a copy.
        let (cx, mut file) = setup();

        // Write a page to the file.
        let data = vec![0xAB_u8; 4096];
        file.write(&cx, &data, 0).unwrap();

        let mut cache = PageCache::new(PageSize::DEFAULT);
        let page1 = PageNumber::ONE;

        // Read the page into cache.
        let ref1 = cache.read_page(&cx, &mut file, page1).unwrap();
        let ref1_ptr = ref1.as_ptr();
        assert_eq!(
            &ref1[..4096],
            data.as_slice(),
            "bead_id={BEAD_ID} case=pager_ref_data_correct"
        );

        // Get the same page again — must be same pointer (cached).
        let ref2 = cache.get(page1).unwrap();
        let ref2_ptr = ref2.as_ptr();
        assert_eq!(
            ref1_ptr, ref2_ptr,
            "bead_id={BEAD_ID} case=pager_returns_ref_not_copy \
             get() must return reference to same memory as read_page()"
        );
    }

    // --- test_wal_uses_buffered_io_compat ---

    #[test]
    fn test_wal_uses_buffered_io_compat() {
        // Verify that WAL frame size (24 + page_size) does NOT preserve
        // sector alignment, proving that WAL I/O requires buffered I/O
        // (not O_DIRECT) in compatibility mode.
        //
        // Per §1.5: "SQLite .wal frames are 24 + page_size bytes — they
        // do NOT preserve sector alignment at frame boundaries."
        let wal_header_size: u64 = 24;

        for &size in &[512u32, 1024, 2048, 4096, 8192, 16384, 32768, 65536] {
            let frame_size = wal_header_size + u64::from(size);

            // Sector alignment: 512 for HDD, 4096 for modern SSD.
            // WAL frame offset after N frames = 32 (WAL header) + N * frame_size.
            let wal_header_bytes: u64 = 32; // WAL file header
            let frame2_offset = wal_header_bytes + frame_size;

            // Frame 2 offset must NOT be sector-aligned for most page sizes.
            // 24 bytes of per-frame header breaks alignment.
            let _sector_4k_aligned = frame2_offset % 4096 == 0;

            // Even for 512-byte sector: 32 + (24+512) = 568, 568 % 512 = 56.
            let sector_512_aligned = frame2_offset % 512 == 0;
            assert!(
                !sector_512_aligned,
                "bead_id={BEAD_ID} case=wal_frame_not_512_aligned \
                 WAL frame 2 at offset {frame2_offset} should NOT be 512-byte aligned \
                 for page_size={size}"
            );
        }
    }

    // --- test_small_header_stack_buffer_ok ---

    #[test]
    fn test_small_header_stack_buffer_ok() {
        // Per §1.5: "Small stack buffers for fixed-size headers ARE permitted."
        // Demonstrate that reading the 100-byte DB header into a stack
        // buffer works correctly and does not violate zero-copy.
        let (cx, mut file) = setup();

        // Write a minimal SQLite header (first 16 bytes of magic string).
        let mut header_data = [0u8; 100];
        header_data[..16].copy_from_slice(b"SQLite format 3\0");
        header_data[16..18].copy_from_slice(&4096u16.to_be_bytes()); // page size
        file.write(&cx, &header_data, 0).unwrap();

        // Read using the stack-buffer helper.
        let header = read_db_header(&cx, &mut file).unwrap();
        assert_eq!(
            &header[..16],
            b"SQLite format 3\0",
            "bead_id={BEAD_ID} case=small_header_stack_buffer_ok"
        );

        // Verify page size field.
        let page_size = u16::from_be_bytes([header[16], header[17]]);
        assert_eq!(
            page_size, 4096,
            "bead_id={BEAD_ID} case=header_page_size_correct"
        );
    }

    // --- test_page_decode_bounds_checked ---

    #[test]
    fn test_page_decode_bounds_checked() {
        // Verify that page structures are decoded with bounds-checked reads
        // in safe Rust — no transmute of variable-length formats.
        //
        // We simulate decoding a B-tree page header from a cached page.
        let (cx, mut file) = setup();

        // Write a page with a simulated B-tree leaf header.
        let mut page_data = vec![0u8; 4096];
        page_data[0] = 0x0D; // leaf table b-tree page type
        page_data[3..5].copy_from_slice(&10u16.to_be_bytes()); // cell count = 10
        page_data[5..7].copy_from_slice(&100u16.to_be_bytes()); // cell content offset
        file.write(&cx, &page_data, 0).unwrap();

        // Read into cache.
        let mut cache = PageCache::new(PageSize::DEFAULT);
        let page = cache.read_page(&cx, &mut file, PageNumber::ONE).unwrap();

        // Bounds-checked decode: every access goes through slice indexing.
        let page_type = page[0];
        assert_eq!(page_type, 0x0D, "bead_id={BEAD_ID} case=page_decode_type");

        let cell_count = u16::from_be_bytes([page[3], page[4]]);
        assert_eq!(
            cell_count, 10,
            "bead_id={BEAD_ID} case=page_decode_cell_count"
        );

        let content_offset = u16::from_be_bytes([page[5], page[6]]);
        assert_eq!(
            content_offset, 100,
            "bead_id={BEAD_ID} case=page_decode_content_offset"
        );

        // Out of bounds access panics (safe Rust guarantee).
        // We verify by checking the page length is exactly page_size.
        assert_eq!(
            page.len(),
            4096,
            "bead_id={BEAD_ID} case=page_decode_bounds_checked"
        );
    }

    // --- Cache operation tests ---

    #[test]
    fn test_cache_insert_fresh_zeroed() {
        let mut cache = PageCache::new(PageSize::DEFAULT);
        let page1 = PageNumber::ONE;

        let data = cache.insert_fresh(page1).unwrap();
        assert!(
            data.iter().all(|&b| b == 0),
            "bead_id={BEAD_ID} case=insert_fresh_zeroed"
        );
        assert_eq!(data.len(), 4096);
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn test_cache_get_mut_modifies_in_place() {
        let mut cache = PageCache::new(PageSize::DEFAULT);
        let page1 = PageNumber::ONE;

        cache.insert_fresh(page1).unwrap();
        let data = cache.get_mut(page1).unwrap();
        data[0] = 0xFF;
        data[4095] = 0xEE;

        let read_back = cache.get(page1).unwrap();
        assert_eq!(read_back[0], 0xFF);
        assert_eq!(read_back[4095], 0xEE);
    }

    #[test]
    fn test_cache_evict_returns_to_pool() {
        let mut cache = PageCache::new(PageSize::DEFAULT);
        let page1 = PageNumber::ONE;

        assert_eq!(cache.pool().available(), 0);
        cache.insert_fresh(page1).unwrap();
        assert_eq!(cache.pool().available(), 0); // buffer is in use

        assert!(cache.evict(page1));
        assert!(!cache.contains(page1));
        // Buffer returned to pool via PageBuf::Drop.
        assert_eq!(
            cache.pool().available(),
            1,
            "bead_id={BEAD_ID} case=evict_returns_to_pool"
        );
    }

    #[test]
    fn test_cache_evict_nonexistent() {
        let mut cache = PageCache::new(PageSize::DEFAULT);
        let page1 = PageNumber::ONE;
        assert!(!cache.evict(page1));
    }

    #[test]
    fn test_cache_clear_returns_all_to_pool() {
        let mut cache = PageCache::new(PageSize::DEFAULT);

        for i in 1..=5u32 {
            let pn = PageNumber::new(i).unwrap();
            cache.insert_fresh(pn).unwrap();
        }
        assert_eq!(cache.len(), 5);
        assert_eq!(cache.pool().available(), 0);

        cache.clear();
        assert_eq!(cache.len(), 0);
        assert_eq!(
            cache.pool().available(),
            5,
            "bead_id={BEAD_ID} case=clear_returns_all_to_pool"
        );
    }

    #[test]
    fn test_cache_multiple_pages() {
        let (cx, mut file) = setup();

        // Write 3 pages with distinct content.
        for i in 0..3u32 {
            let seed = u8::try_from(i).expect("i <= 2");
            let data = vec![(seed + 1) * 0x11; 4096];
            let offset = u64::from(i) * 4096;
            file.write(&cx, &data, offset).unwrap();
        }

        let mut cache = PageCache::new(PageSize::DEFAULT);

        for i in 1..=3u32 {
            let pn = PageNumber::new(i).unwrap();
            let page = cache.read_page(&cx, &mut file, pn).unwrap();
            let expected = u8::try_from(i).expect("i <= 3") * 0x11;
            assert!(
                page.iter().all(|&b| b == expected),
                "bead_id={BEAD_ID} case=multiple_pages page={i} expected={expected:#x}"
            );
        }

        assert_eq!(cache.len(), 3);
    }

    #[test]
    fn test_cache_write_page_roundtrip() {
        let (cx, mut file) = setup();

        let mut cache = PageCache::new(PageSize::DEFAULT);
        let page1 = PageNumber::ONE;

        // Insert a fresh page, modify it, write to VFS.
        let data = cache.insert_fresh(page1).unwrap();
        data.fill(0xCD);

        cache.write_page(&cx, &mut file, page1).unwrap();

        // Read back from VFS directly (bypassing cache).
        let mut verify = vec![0u8; 4096];
        file.read(&cx, &mut verify, 0).unwrap();
        assert!(
            verify.iter().all(|&b| b == 0xCD),
            "bead_id={BEAD_ID} case=write_page_roundtrip"
        );
    }

    #[test]
    fn test_page_offset_calculation() {
        // Page 1 starts at offset 0.
        assert_eq!(
            page_offset(PageNumber::ONE, PageSize::DEFAULT),
            0,
            "bead_id={BEAD_ID} case=page_offset_page1"
        );

        // Page 2 starts at 4096.
        let p2 = PageNumber::new(2).unwrap();
        assert_eq!(
            page_offset(p2, PageSize::DEFAULT),
            4096,
            "bead_id={BEAD_ID} case=page_offset_page2"
        );

        // Page 100 with 512-byte pages starts at 99 * 512 = 50688.
        let p100 = PageNumber::new(100).unwrap();
        let ps512 = PageSize::new(512).unwrap();
        assert_eq!(
            page_offset(p100, ps512),
            50688,
            "bead_id={BEAD_ID} case=page_offset_page100_512"
        );
    }

    // --- E2E: combined zero-copy verification ---

    #[test]
    fn test_e2e_zero_copy_io_no_allocations() {
        // E2E: run a read-heavy workload (simulated point lookups) and
        // verify that steady-state reads are allocation-free by checking
        // pool reuse and pointer stability.
        let (cx, mut file) = setup();

        // Write 10 pages with distinct content.
        let num_pages: u32 = 10;
        for i in 0..num_pages {
            let byte = u8::try_from(i).expect("i <= 9").wrapping_add(0x10);
            let data = vec![byte; 4096];
            file.write(&cx, &data, u64::from(i) * 4096).unwrap();
        }

        let mut cache = PageCache::new(PageSize::DEFAULT);

        // Phase 1: Cold reads — pages load from VFS into cache.
        let mut ptrs: Vec<usize> = Vec::with_capacity(num_pages as usize);
        for i in 1..=num_pages {
            let pn = PageNumber::new(i).unwrap();
            let page = cache.read_page(&cx, &mut file, pn).unwrap();
            ptrs.push(page.as_ptr() as usize);
        }

        // Phase 2: Hot reads — all pages are cached.  Verify no new
        // allocations by checking pointer stability.
        for round in 0..5u32 {
            for i in 1..=num_pages {
                let pn = PageNumber::new(i).unwrap();
                let page = cache.get(pn).unwrap();
                let ptr = page.as_ptr() as usize;
                assert_eq!(
                    ptr,
                    ptrs[(i - 1) as usize],
                    "bead_id={BEAD_ID} case=e2e_pointer_stable \
                     round={round} page={i}"
                );

                // Verify data correctness.
                let expected = u8::try_from(i - 1).expect("i - 1 <= 9").wrapping_add(0x10);
                assert!(
                    page.iter().all(|&b| b == expected),
                    "bead_id={BEAD_ID} case=e2e_data_correct \
                     round={round} page={i}"
                );
            }
        }

        // Phase 3: Evict and re-read — pool reuse avoids new allocation.
        let pool_available_before = cache.pool().available();
        let old_ptr = ptrs[0];

        cache.evict(PageNumber::ONE);
        assert_eq!(
            cache.pool().available(),
            pool_available_before + 1,
            "bead_id={BEAD_ID} case=e2e_evict_returns_to_pool"
        );

        // Re-read page 1: should reuse pool buffer (no new heap alloc).
        let page1_reread = cache.read_page(&cx, &mut file, PageNumber::ONE).unwrap();
        let new_ptr = page1_reread.as_ptr() as usize;

        // The recycled buffer from the pool should be the same allocation.
        assert_eq!(
            new_ptr, old_ptr,
            "bead_id={BEAD_ID} case=e2e_pool_reuse_after_evict \
             Expected recycled buffer at {old_ptr:#x}, got {new_ptr:#x}"
        );

        // Summary (grep-friendly).
        eprintln!("pages_cached={}", cache.len());
        eprintln!("pool_available={}", cache.pool().available());
        eprintln!("pointer_checks_passed={}", num_pages * 5 + 1);
    }

    // --- Debug ---

    #[test]
    fn test_page_cache_debug() {
        let cache = PageCache::new(PageSize::DEFAULT);
        let debug = format!("{cache:?}");
        assert!(
            debug.contains("PageCache"),
            "bead_id={BEAD_ID} case=debug_format"
        );
    }

    #[test]
    fn test_metrics_snapshot_and_reset() {
        let mut cache = PageCache::new(PageSize::DEFAULT);
        let page1 = PageNumber::ONE;

        assert!(cache.get(page1).is_none());
        let fresh = cache.insert_fresh(page1).unwrap();
        fresh[0] = 7;
        assert!(cache.get(page1).is_some());
        assert!(cache.evict(page1));

        let snapshot = cache.metrics_snapshot();
        assert_eq!(snapshot.hits, 1, "bead_id={BEAD_ID} case=metrics_hits");
        assert_eq!(snapshot.misses, 1, "bead_id={BEAD_ID} case=metrics_misses");
        assert_eq!(snapshot.admits, 1, "bead_id={BEAD_ID} case=metrics_admits");
        assert_eq!(
            snapshot.evictions, 1,
            "bead_id={BEAD_ID} case=metrics_evictions"
        );
        assert_eq!(
            snapshot.total_accesses(),
            2,
            "bead_id={BEAD_ID} case=metrics_total_accesses"
        );
        assert!(
            (snapshot.hit_rate_percent() - 50.0).abs() < f64::EPSILON,
            "bead_id={BEAD_ID} case=metrics_hit_rate"
        );
        let efficiency = snapshot.efficiency_snapshot();
        assert_eq!(
            efficiency.hits, snapshot.hits,
            "bead_id={BEAD_ID} case=metrics_efficiency_hits"
        );
        assert_eq!(
            efficiency.misses, snapshot.misses,
            "bead_id={BEAD_ID} case=metrics_efficiency_misses"
        );
        assert_eq!(
            efficiency.evictions, snapshot.evictions,
            "bead_id={BEAD_ID} case=metrics_efficiency_evictions"
        );
        assert!(
            (efficiency.hit_rate_percent() - snapshot.hit_rate_percent()).abs() < f64::EPSILON,
            "bead_id={BEAD_ID} case=metrics_efficiency_hit_rate"
        );

        cache.reset_metrics();
        let reset = cache.metrics_snapshot();
        assert_eq!(reset.hits, 0, "bead_id={BEAD_ID} case=reset_hits");
        assert_eq!(reset.misses, 0, "bead_id={BEAD_ID} case=reset_misses");
        assert_eq!(reset.admits, 0, "bead_id={BEAD_ID} case=reset_admits");
        assert_eq!(reset.evictions, 0, "bead_id={BEAD_ID} case=reset_evictions");
    }

    #[test]
    fn test_page_cache_s3_fifo_evict_any_keeps_hot_pages() {
        let mut cache = PageCache::new(PageSize::DEFAULT);
        cache.set_eviction_policy(PageCacheEvictionPolicy::S3Fifo(S3FifoConfig::with_limits(
            4, 1, 1, 1,
        )));

        let hot_a = PageNumber::ONE;
        let hot_b = PageNumber::new(2).unwrap();
        let cold_a = PageNumber::new(3).unwrap();
        let cold_b = PageNumber::new(4).unwrap();

        for page_no in [hot_a, hot_b, cold_a, cold_b] {
            let page = cache.insert_fresh(page_no).unwrap();
            page[0] = u8::try_from(page_no.get()).unwrap();
        }

        for _ in 0..8 {
            assert!(
                cache.get(hot_a).is_some(),
                "hot page A must remain readable"
            );
            assert!(
                cache.get(hot_b).is_some(),
                "hot page B must remain readable"
            );
        }

        assert!(
            cache.evict_any(),
            "S3-FIFO cache should evict one cold page"
        );
        assert!(
            cache.contains(hot_a),
            "hot page A should survive S3-FIFO eviction"
        );
        assert!(
            cache.contains(hot_b),
            "hot page B should survive S3-FIFO eviction"
        );
        assert_eq!(
            [cold_a, cold_b]
                .into_iter()
                .filter(|page_no| cache.contains(*page_no))
                .count(),
            1,
            "exactly one cold page should remain resident after one eviction"
        );

        let snapshot = cache.metrics_snapshot();
        assert_eq!(snapshot.cached_pages, 3, "one page should be evicted");
        assert!(
            snapshot.t2_size >= 1,
            "S3-FIFO metrics should expose a non-empty main queue after hot re-access"
        );
    }

    #[test]
    fn test_sharded_page_cache_s3_fifo_evict_any_keeps_hot_pages() {
        let cache = ShardedPageCache::new(PageSize::DEFAULT);
        cache.set_eviction_policy(PageCacheEvictionPolicy::S3Fifo(S3FifoConfig::with_limits(
            4, 1, 1, 1,
        )));

        let hot_a = PageNumber::ONE;
        let hot_b = PageNumber::new(2).unwrap();
        let cold_a = PageNumber::new(3).unwrap();
        let cold_b = PageNumber::new(4).unwrap();

        for page_no in [hot_a, hot_b, cold_a, cold_b] {
            cache.insert_buffer(page_no, track_q_page_buf(page_no));
        }

        for _ in 0..8 {
            assert!(
                cache.get_copy(hot_a).is_some(),
                "hot shard page A must remain readable"
            );
            assert!(
                cache.get_copy(hot_b).is_some(),
                "hot shard page B must remain readable"
            );
        }

        assert!(
            cache.evict_any(),
            "sharded S3-FIFO cache should evict one cold page"
        );
        assert!(
            cache.contains(hot_a),
            "hot shard page A should survive S3-FIFO eviction"
        );
        assert!(
            cache.contains(hot_b),
            "hot shard page B should survive S3-FIFO eviction"
        );
        assert_eq!(
            [cold_a, cold_b]
                .into_iter()
                .filter(|page_no| cache.contains(*page_no))
                .count(),
            1,
            "exactly one cold shard page should remain resident after one eviction"
        );

        let snapshot = cache.metrics_snapshot();
        assert_eq!(
            snapshot.cached_pages, 3,
            "one sharded page should be evicted"
        );
        assert!(
            snapshot.t2_size >= 1,
            "S3-FIFO metrics should expose a non-empty main queue for sharded cache"
        );
    }

    #[test]
    fn gh_131_clean_eviction_never_discards_dirty_page() {
        let cache = ShardedPageCache::with_max_buffers(PageSize::DEFAULT, 2);
        let clean_page = PageNumber::ONE;
        let dirty_page = PageNumber::new(2).unwrap();

        let clean_buf = cache.pool().acquire().unwrap();
        cache.insert_buffer(clean_page, clean_buf);
        cache
            .insert_fresh(dirty_page, |bytes| bytes[0] = 0xA5)
            .unwrap();
        assert_eq!(cache.pool().available(), 0, "pool must start saturated");

        assert!(
            cache.evict_clean_any(),
            "clean-cache recovery should reclaim the eligible page"
        );
        assert!(!cache.contains(clean_page));
        assert!(cache.contains(dirty_page));
        assert_eq!(cache.pool().available(), 1);

        assert!(
            !cache.evict_clean_any(),
            "dirty-only cache must fail closed rather than discard data"
        );
        assert!(cache.contains(dirty_page));
    }

    #[test]
    fn gh_131_clean_eviction_skips_buffers_from_other_allocators() {
        let cache = ShardedPageCache::with_max_buffers(PageSize::DEFAULT, 1);
        for raw_page_no in 1..=8 {
            cache.insert_buffer(
                PageNumber::new(raw_page_no).unwrap(),
                PageBuf::new(PageSize::DEFAULT),
            );
        }
        let pooled_page = PageNumber::new(9).unwrap();
        cache.insert_buffer(pooled_page, cache.pool().acquire().unwrap());
        assert_eq!(cache.pool().available(), 0);

        assert!(cache.evict_clean_any());
        assert!(!cache.contains(pooled_page));
        assert_eq!(cache.pool().available(), 1);
        for raw_page_no in 1..=8 {
            assert!(cache.contains(PageNumber::new(raw_page_no).unwrap()));
        }
    }

    #[test]
    fn gh_131_clean_buffer_handoff_preserves_shared_page_snapshot() {
        let cache = ShardedPageCache::with_max_buffers(PageSize::DEFAULT, 1);
        let page_no = PageNumber::ONE;
        let mut buffer = cache.pool().acquire().unwrap();
        buffer.fill(0xA7);
        cache.insert_buffer(page_no, buffer);
        let snapshot = cache
            .get_shared(page_no)
            .expect("clean cached page should publish a shared snapshot");

        let mut reclaimed = cache
            .take_clean_buffer()
            .expect("shared snapshot must not pin the independent pool buffer");
        reclaimed.fill(0xB8);

        assert!(!cache.contains(page_no));
        assert!(snapshot.as_bytes().iter().all(|byte| *byte == 0xA7));
        assert!(reclaimed.iter().all(|byte| *byte == 0xB8));
    }

    #[test]
    fn gh_131_flat_slot_reader_rejects_reused_slot_payload() {
        let cache = Arc::new(ShardedPageCache::with_max_buffers(PageSize::DEFAULT, 2));
        let original_page = PageNumber::ONE;
        let mut original = PageBuf::new(PageSize::DEFAULT);
        original.fill(0xA1);
        cache.insert_buffer(original_page, original);
        let original_slot = cache.flat_slots.find_slot(original_page).unwrap();
        let replacement_page = (2..u32::MAX)
            .filter_map(PageNumber::new)
            .find(|candidate| {
                cache.flat_slots.hash_pgno(candidate.get()) == original_slot
                    && *candidate != original_page
            })
            .expect("a colliding replacement page must exist");

        let barrier = Arc::new(std::sync::Barrier::new(2));
        let reader_cache = Arc::clone(&cache);
        let reader_barrier = Arc::clone(&barrier);
        let reader = std::thread::spawn(move || {
            reader_cache
                .flat_slots
                .get_copy_after_probe_barrier(original_page, &reader_barrier)
        });

        barrier.wait();
        assert!(cache.evict(original_page));
        let mut replacement = PageBuf::new(PageSize::DEFAULT);
        replacement.fill(0xB2);
        cache.insert_buffer(replacement_page, replacement);
        assert_eq!(
            cache.flat_slots.find_slot(replacement_page),
            Some(original_slot),
            "replacement must reuse the probed slot to exercise the race"
        );
        barrier.wait();

        assert_eq!(
            reader.join().unwrap(),
            None,
            "a reader for the evicted page must not observe replacement bytes"
        );
    }

    #[test]
    fn gh_131_authoritative_flat_update_purges_overflow_duplicate() {
        let cache = ShardedPageCache::with_max_buffers(PageSize::DEFAULT, 3);
        let page_no = PageNumber::ONE;

        let mut flat = cache.pool().acquire().unwrap();
        flat.fill(0x11);
        cache.insert_buffer(page_no, flat);

        let mut stale = cache.pool().acquire().unwrap();
        stale.fill(0x22);
        let shard_idx = cache.shard_index(page_no);
        cache.shards[shard_idx]
            .lock()
            .insert_with_dirty_state(page_no, stale, false);

        let mut current = cache.pool().acquire().unwrap();
        current.fill(0x33);
        cache.insert_buffer(page_no, current);
        assert!(!cache.shards[shard_idx].lock().contains(page_no));
        assert!(
            cache
                .get_copy(page_no)
                .is_some_and(|bytes| bytes.iter().all(|byte| *byte == 0x33))
        );

        let reclaimed = cache.take_clean_buffer().unwrap();
        assert!(reclaimed.iter().all(|byte| *byte == 0x33));
        assert_eq!(cache.get_copy(page_no), None);
    }

    #[test]
    fn gh_131_concurrent_same_page_insertions_leave_one_authoritative_resident() {
        const WORKERS: usize = 8;
        const ROUNDS: usize = 128;
        const FINAL_BYTE: u8 = 0xA5;

        let cache = Arc::new(ShardedPageCache::with_max_buffers(PageSize::DEFAULT, 1));
        let page_no = PageNumber::new(17).unwrap();
        let start = Arc::new(std::sync::Barrier::new(WORKERS + 1));
        let mut workers = Vec::with_capacity(WORKERS);

        for worker in 0..WORKERS {
            let cache = Arc::clone(&cache);
            let start = Arc::clone(&start);
            workers.push(std::thread::spawn(move || {
                start.wait();
                for round in 0..ROUNDS {
                    if (worker + round) % 3 == 0 {
                        cache.evict(page_no);
                    }
                    let mut buffer = PageBuf::new(PageSize::DEFAULT);
                    buffer.fill(u8::try_from(worker + 1).unwrap());
                    cache.insert_buffer(page_no, buffer);
                }
            }));
        }
        start.wait();
        for worker in workers {
            worker.join().unwrap();
        }

        let mut final_buffer = PageBuf::new(PageSize::DEFAULT);
        final_buffer.fill(FINAL_BYTE);
        cache.insert_buffer(page_no, final_buffer);

        let flat_residents = cache
            .flat_slots
            .slots
            .iter()
            .filter_map(PageSlot::stable_snapshot)
            .filter(|snapshot| snapshot.page_no == page_no)
            .count();
        let shard_idx = cache.shard_index(page_no);
        let shard_residents = usize::from(cache.shards[shard_idx].lock().contains(page_no));
        assert_eq!(flat_residents + shard_residents, 1);
        assert!(
            cache
                .get_copy(page_no)
                .is_some_and(|bytes| bytes.iter().all(|byte| *byte == FINAL_BYTE))
        );
    }

    #[test]
    fn gh_131_clean_buffer_handoff_reclaims_overflow_only_resident() {
        let resident_count = MAX_PROBE_LENGTH + 1;
        let cache = ShardedPageCache::with_max_buffers(PageSize::DEFAULT, resident_count);
        let target_bucket = cache.flat_slots.hash_pgno(PageNumber::ONE.get());
        let colliders = track_q_collision_pages(&cache.flat_slots, target_bucket, resident_count);

        for page_no in colliders.iter().copied() {
            let mut buffer = cache.pool().acquire().unwrap();
            buffer.fill(u8::try_from(page_no.get() & 0xff).unwrap());
            cache.insert_buffer(page_no, buffer);
        }
        assert_eq!(cache.pool().available(), 0, "pool must start saturated");

        let overflow_page = colliders
            .iter()
            .copied()
            .find(|page_no| !cache.flat_slots.contains(*page_no))
            .expect("one forced collider must reside only in overflow");
        let overflow_shard = cache.shard_index(overflow_page);
        assert!(cache.shards[overflow_shard].lock().contains(overflow_page));

        let reclaimed = cache
            .take_clean_buffer_at(overflow_page)
            .expect("clean overflow-only resident must be reclaimable");
        assert!(
            reclaimed
                .iter()
                .all(|byte| *byte == u8::try_from(overflow_page.get() & 0xff).unwrap())
        );
        assert!(!cache.contains(overflow_page));
        drop(reclaimed);
        assert_eq!(cache.pool().available(), 1);
    }

    #[test]
    fn test_s3_fifo_reconstructed_tracker_bounds_large_resident_sets() {
        let mut tracker = S3FifoEvictionTracker::new(S3FifoConfig::new(
            S3_FIFO_RECONSTRUCTED_EVICTION_MAX_RESIDENTS + 1,
        ));
        let residents: Vec<PageNumber> = (1..=S3_FIFO_RECONSTRUCTED_EVICTION_MAX_RESIDENTS + 1)
            .map(|page_no| PageNumber::new(u32::try_from(page_no).unwrap()).unwrap())
            .collect();

        for page_no in 1..=S3_FIFO_RECONSTRUCTED_EVICTION_MAX_TRACE_ENTRIES + 10 {
            let page_no = PageNumber::new(u32::try_from(page_no).unwrap()).unwrap();
            tracker.record_access(page_no);
        }

        assert_eq!(
            tracker.access_trace.len(),
            S3_FIFO_RECONSTRUCTED_EVICTION_MAX_TRACE_ENTRIES,
            "large reconstructed S3-FIFO trackers must keep bounded history"
        );
        let len_before_forget = tracker.access_trace.len();
        tracker.forget(residents[0]);
        assert_eq!(
            tracker.access_trace.len(),
            len_before_forget,
            "large reconstructed S3-FIFO trackers must not linearly retain history on eviction"
        );
        assert!(
            tracker.choose_victim(&residents).is_none(),
            "large resident sets must fall back to the cache's O(1) arbitrary eviction path"
        );
        assert!(
            tracker.queue_snapshot(&residents).is_none(),
            "large resident sets must not rebuild S3-FIFO diagnostics on the hot path"
        );
    }

    #[test]
    fn test_sharded_s3_fifo_eviction_falls_back_before_collecting_large_residents() {
        let cache = ShardedPageCache::with_max_buffers_and_shards(
            PageSize::DEFAULT,
            S3_FIFO_RECONSTRUCTED_EVICTION_MAX_RESIDENTS + 2,
            1,
        );
        cache.set_eviction_policy(PageCacheEvictionPolicy::S3Fifo(S3FifoConfig::new(
            S3_FIFO_RECONSTRUCTED_EVICTION_MAX_RESIDENTS + 2,
        )));

        for page_no in 1..=S3_FIFO_RECONSTRUCTED_EVICTION_MAX_RESIDENTS + 1 {
            let page_no = PageNumber::new(u32::try_from(page_no).unwrap()).unwrap();
            cache.insert_buffer(page_no, track_q_page_buf(page_no));
        }

        assert!(
            cache.preferred_reconstructed_victim().is_none(),
            "large sharded caches must skip reconstructed S3-FIFO victim selection"
        );
        assert!(
            cache.evict_any(),
            "large sharded caches must still evict via arbitrary fallback"
        );
        assert_eq!(
            cache.metrics_lightweight_snapshot().cached_pages,
            S3_FIFO_RECONSTRUCTED_EVICTION_MAX_RESIDENTS,
            "arbitrary fallback must remove exactly one page"
        );
    }

    #[test]
    fn test_cache_monitor_sequential_scan_reports_recency_queue_bias() {
        let cache = ShardedPageCache::with_max_buffers_and_shards(PageSize::DEFAULT, 8, 1);
        cache.set_eviction_policy(PageCacheEvictionPolicy::S3Fifo(S3FifoConfig::new(8)));
        cache.reset_metrics();

        let started = Instant::now();
        for raw_pgno in 1..=32_u32 {
            touch_monitored_page(&cache, PageNumber::new(raw_pgno).unwrap());
        }
        let elapsed = started.elapsed();

        let snapshot = cache.metrics_snapshot();
        emit_cache_monitor_log(
            "test_cache_monitor_sequential_scan_reports_recency_queue_bias",
            "sequential",
            elapsed,
            snapshot,
            json!({
                "unique_pages": 32,
                "expected_capacity": 8,
                "replay_hint": "cargo test -p fsqlite-pager test_cache_monitor_sequential_scan_reports_recency_queue_bias -- --nocapture"
            }),
        );

        assert_eq!(
            snapshot.total_accesses(),
            32,
            "cache monitor sequential scenario should account for every page touch"
        );
        assert!(
            snapshot.hit_rate_percent() <= 10.0,
            "sequential scan should remain miss-heavy, got {:.2}%",
            snapshot.hit_rate_percent()
        );
        assert!(
            snapshot.evictions > 0,
            "sequential scan should force evictions once the working set exceeds capacity"
        );
        assert!(
            snapshot.t1_size >= snapshot.t2_size,
            "recency queue should dominate after a pure scan: t1={} t2={}",
            snapshot.t1_size,
            snapshot.t2_size
        );
        assert!(
            snapshot.cached_pages <= 8,
            "resident cache must stay bounded by configured capacity"
        );
    }

    #[test]
    fn test_cache_monitor_hotset_reports_frequency_queue_bias() {
        let cache = ShardedPageCache::with_max_buffers_and_shards(PageSize::DEFAULT, 16, 1);
        cache.set_eviction_policy(PageCacheEvictionPolicy::S3Fifo(S3FifoConfig::new(16)));
        cache.reset_metrics();

        let hot_pages = [
            PageNumber::ONE,
            PageNumber::new(2).unwrap(),
            PageNumber::new(3).unwrap(),
            PageNumber::new(4).unwrap(),
        ];

        let started = Instant::now();
        for round in 0..160_u32 {
            for page_no in hot_pages {
                touch_monitored_page(&cache, page_no);
            }
            let cold_page = PageNumber::new(100 + (round % 24)).unwrap();
            touch_monitored_page(&cache, cold_page);
        }
        let elapsed = started.elapsed();

        let snapshot = cache.metrics_snapshot();
        let page_snapshots = cache.page_snapshots();
        let queue_by_page: HashMap<PageNumber, Option<PageCacheQueueKind>> = page_snapshots
            .iter()
            .map(|entry| (entry.page_no, entry.queue))
            .collect();
        let hot_pages_in_t2 = hot_pages
            .iter()
            .filter(|page_no| queue_by_page.get(page_no) == Some(&Some(PageCacheQueueKind::T2)))
            .count();

        emit_cache_monitor_log(
            "test_cache_monitor_hotset_reports_frequency_queue_bias",
            "zipfian",
            elapsed,
            snapshot,
            json!({
                "hot_pages": hot_pages.iter().map(|page_no| page_no.get()).collect::<Vec<_>>(),
                "resident_pages": page_snapshots.len(),
                "hot_pages_in_t2": hot_pages_in_t2,
                "replay_hint": "cargo test -p fsqlite-pager test_cache_monitor_hotset_reports_frequency_queue_bias -- --nocapture"
            }),
        );

        assert_eq!(
            snapshot.total_accesses(),
            800,
            "hot-set workload should account for four hot reads plus one cold read per round"
        );
        assert!(
            snapshot.hit_rate_percent() >= 80.0,
            "hot-set workload should be hit-heavy, got {:.2}%",
            snapshot.hit_rate_percent()
        );
        assert!(
            snapshot.t2_size >= snapshot.t1_size,
            "frequency queue should dominate for the hot-set workload: t1={} t2={}",
            snapshot.t1_size,
            snapshot.t2_size
        );
        assert!(
            hot_pages_in_t2 >= 3,
            "most hot pages should graduate into the frequency queue, got {hot_pages_in_t2}"
        );
    }

    /// bd-q7zls: the ARC eviction policy is wired into `PageCache::evict_any`
    /// and reuses `ArcEvictionModel` to protect frequently-accessed pages. After
    /// pages 1 & 2 are promoted to ARC's frequency lane (T2), eviction must fall
    /// on a single-access recency page (3 or 4), never on a frequent page.
    #[test]
    fn test_page_cache_arc_policy_protects_frequently_accessed_pages() {
        let mut cache = PageCache::new(PageSize::DEFAULT);
        cache.set_eviction_policy(PageCacheEvictionPolicy::Arc(ArcEvictionConfig::new(8)));
        assert!(
            matches!(cache.eviction_policy(), PageCacheEvictionPolicy::Arc(_)),
            "bead_id=bd-q7zls case=arc_policy_round_trips"
        );

        let pages: [PageNumber; 4] = [1, 2, 3, 4].map(|n| PageNumber::new(n).unwrap());
        for &p in &pages {
            cache.insert_fresh(p).unwrap();
        }
        // Promote pages 1 and 2 into ARC's frequency lane (T2) via repeat access.
        for _ in 0..3 {
            assert!(cache.get(pages[0]).is_some());
            assert!(cache.get(pages[1]).is_some());
        }

        assert!(cache.evict_any(), "evict_any must free a page");
        assert!(
            cache.get(pages[0]).is_some(),
            "bead_id=bd-q7zls case=arc_keeps_frequent_page_1"
        );
        assert!(
            cache.get(pages[1]).is_some(),
            "bead_id=bd-q7zls case=arc_keeps_frequent_page_2"
        );
        let three_resident = cache.get(pages[2]).is_some();
        let four_resident = cache.get(pages[3]).is_some();
        assert!(
            three_resident ^ four_resident,
            "bead_id=bd-q7zls case=arc_evicts_exactly_one_recency_page three={three_resident} four={four_resident}"
        );
    }

    /// bd-5ftij: the Thompson-adaptive S3-FIFO tracker is a live consumer of the
    /// cache hit/miss signal. After a full resample interval of outcomes it
    /// retargets the S3-FIFO small-queue size to the bandit's selected
    /// hot-partition ratio (`round(ratio * capacity)`, ratio in [0.1, 0.9]).
    #[test]
    fn test_s3fifo_adaptive_tracker_retargets_small_queue_on_resample() {
        use crate::thompson_partitioner::RESAMPLE_INTERVAL;
        let mut tracker = S3FifoAdaptiveTracker::new(S3FifoConfig::new(100));
        let page = PageNumber::new(1).unwrap();
        // Drive one full resample interval of (hit) outcomes; tick() crosses the
        // RESAMPLE_INTERVAL boundary exactly once and retargets the small queue.
        for _ in 0..RESAMPLE_INTERVAL {
            tracker.record_access(page);
        }
        let small = tracker.inner.config.small_capacity();
        assert!(
            (10..=90).contains(&small),
            "bead_id=bd-5ftij case=thompson_retargets_small_queue small={small} (expected round(ratio*100), ratio in [0.1,0.9])"
        );
    }

    /// bd-5ftij: the adaptive policy round-trips through the cache and still
    /// protects frequently-accessed pages (it delegates victim choice to its
    /// inner S3-FIFO model, now with a Thompson-tuned split).
    #[test]
    fn test_page_cache_s3fifo_adaptive_policy_round_trips_and_evicts() {
        let mut cache = PageCache::new(PageSize::DEFAULT);
        cache.set_eviction_policy(PageCacheEvictionPolicy::S3FifoAdaptive(S3FifoConfig::new(
            8,
        )));
        assert!(
            matches!(
                cache.eviction_policy(),
                PageCacheEvictionPolicy::S3FifoAdaptive(_)
            ),
            "bead_id=bd-5ftij case=adaptive_policy_round_trips"
        );
        for n in 1..=4 {
            cache.insert_fresh(PageNumber::new(n).unwrap()).unwrap();
        }
        for _ in 0..3 {
            assert!(cache.get(PageNumber::new(1).unwrap()).is_some());
        }
        assert!(
            cache.evict_any(),
            "bead_id=bd-5ftij case=adaptive_policy_evicts"
        );
        assert!(
            cache.get(PageNumber::new(1).unwrap()).is_some(),
            "bead_id=bd-5ftij case=adaptive_keeps_frequent_page"
        );
    }

    /// bd-9johq: real-policy hit-rate comparison through the live
    /// `ShardedPageCache` (not the s3_fifo simulator). Deterministic hot-set +
    /// cold-scan workload — a small hot set touched every round, interleaved
    /// with cold-scan bursts that drive the resident set past capacity. Scan-
    /// resistant policies (S3-FIFO main, ARC T2) keep the hot set resident
    /// across the cold churn; recency-blind `Arbitrary` does not. The printed
    /// numbers feed the default-policy decision (bd-9johq); the asserts guard
    /// that the ARC / adaptive policies actually engage and protect the hot set
    /// end-to-end (not that one policy wins).
    #[test]
    fn bench_real_eviction_policies_hot_cold_scan_hit_rate() {
        const MAX_BUFFERS: usize = 64;
        const HOT: u32 = 8;
        const COLD_BURST: u32 = 24;
        const ROUNDS: usize = 200;

        fn touch(cache: &ShardedPageCache, page: u32) {
            let pn = PageNumber::new(page).unwrap();
            if cache.get_copy(pn).is_some() {
                return;
            }
            loop {
                match cache.insert_fresh(pn, |_| {}) {
                    Ok(()) => return,
                    Err(FrankenError::OutOfMemory) => assert!(
                        cache.evict_any(),
                        "bead_id=bd-9johq case=admission_must_free_a_victim"
                    ),
                    Err(err) => panic!("bead_id=bd-9johq insert_fresh failed: {err}"),
                }
            }
        }

        fn hot_cold_hit_rate(policy: PageCacheEvictionPolicy) -> f64 {
            let cache =
                ShardedPageCache::with_max_buffers_and_shards(PageSize::DEFAULT, MAX_BUFFERS, 1);
            cache.set_eviction_policy(policy);
            cache.reset_metrics();
            let mut cold = 1_000_u32;
            for _ in 0..ROUNDS {
                for hot in 1..=HOT {
                    touch(&cache, hot);
                }
                for _ in 0..COLD_BURST {
                    touch(&cache, cold);
                    cold += 1;
                }
            }
            cache.metrics_snapshot().hit_rate_percent()
        }

        let arbitrary = hot_cold_hit_rate(PageCacheEvictionPolicy::Arbitrary);
        let s3fifo = hot_cold_hit_rate(PageCacheEvictionPolicy::S3Fifo(S3FifoConfig::new(
            MAX_BUFFERS,
        )));
        let arc = hot_cold_hit_rate(PageCacheEvictionPolicy::Arc(ArcEvictionConfig::new(
            MAX_BUFFERS,
        )));
        let adaptive = hot_cold_hit_rate(PageCacheEvictionPolicy::S3FifoAdaptive(
            S3FifoConfig::new(MAX_BUFFERS),
        ));

        // Perfect hot-set protection yields ~HOT/(HOT+COLD_BURST).
        let ideal = f64::from(HOT) / f64::from(HOT + COLD_BURST) * 100.0;
        eprintln!(
            "bead_id=bd-9johq workload=hot_cold_scan max_buffers={MAX_BUFFERS} hot={HOT} cold_burst={COLD_BURST} rounds={ROUNDS} ideal_protected={ideal:.2}%"
        );
        eprintln!(
            "bead_id=bd-9johq hit_rate%: arbitrary={arbitrary:.2} s3fifo={s3fifo:.2} arc={arc:.2} s3fifo_adaptive={adaptive:.2}"
        );

        // The scan-resistant policies must protect the hot set (proves they are
        // wired end-to-end and choose cold victims) — within reach of the ideal.
        for (name, rate) in [
            ("s3fifo", s3fifo),
            ("arc", arc),
            ("s3fifo_adaptive", adaptive),
        ] {
            assert!(
                rate >= 20.0,
                "bead_id=bd-9johq case=scan_resistant_protects_hot_set policy={name} hit_rate={rate:.2} ideal={ideal:.2}"
            );
        }
        for (name, rate) in [
            ("arbitrary", arbitrary),
            ("s3fifo", s3fifo),
            ("arc", arc),
            ("s3fifo_adaptive", adaptive),
        ] {
            assert!(
                (0.0..=100.0).contains(&rate),
                "bead_id=bd-9johq case=valid_hit_rate policy={name} hit_rate={rate}"
            );
        }
    }

    /// bd-9johq (hardening): real-policy hit-rate comparison on a deterministic
    /// SKEWED-frequency (Zipf-ish) workload over a key space larger than the
    /// cache — the family where ARC's frequency-awareness and S3FifoAdaptive's
    /// Thompson resampling (the workload exceeds RESAMPLE_INTERVAL accesses)
    /// could most plausibly diverge from plain S3-FIFO. Confirms the "keep
    /// S3-FIFO default" decision beyond the scan-resistance workload.
    #[test]
    fn bench_real_eviction_policies_zipfian_hit_rate() {
        const MAX_BUFFERS: usize = 64;
        const DOMAIN: u32 = 512;
        const ACCESSES: usize = 20_000;
        const SKEW: f64 = 2.0;
        const SEED: u64 = 0x5DEE_CE66_D1B5_4A32;

        fn touch(cache: &ShardedPageCache, page: u32) {
            let pn = PageNumber::new(page).unwrap();
            if cache.get_copy(pn).is_some() {
                return;
            }
            loop {
                match cache.insert_fresh(pn, |_| {}) {
                    Ok(()) => return,
                    Err(FrankenError::OutOfMemory) => assert!(
                        cache.evict_any(),
                        "bead_id=bd-9johq case=zipf_admission_frees_victim"
                    ),
                    Err(err) => panic!("bead_id=bd-9johq zipf insert_fresh failed: {err}"),
                }
            }
        }

        // Deterministic SplitMix64 -> Zipf-ish key (low keys hot).
        fn zipf_key(state: &mut u64, domain: u32, skew: f64) -> u32 {
            *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = *state;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^= z >> 31;
            let bits = z >> 11; // 53 random bits
            #[allow(clippy::cast_precision_loss)]
            let u = (bits as f64) * (1.0_f64 / ((1_u64 << 53) as f64));
            #[allow(
                clippy::cast_possible_truncation,
                clippy::cast_sign_loss,
                clippy::cast_precision_loss
            )]
            let idx = (u.powf(skew) * f64::from(domain)) as u32;
            1 + idx.min(domain - 1)
        }

        fn zipf_hit_rate(policy: PageCacheEvictionPolicy) -> f64 {
            let cache =
                ShardedPageCache::with_max_buffers_and_shards(PageSize::DEFAULT, MAX_BUFFERS, 1);
            cache.set_eviction_policy(policy);
            cache.reset_metrics();
            let mut state = SEED;
            for _ in 0..ACCESSES {
                touch(&cache, zipf_key(&mut state, DOMAIN, SKEW));
            }
            cache.metrics_snapshot().hit_rate_percent()
        }

        let arbitrary = zipf_hit_rate(PageCacheEvictionPolicy::Arbitrary);
        let s3fifo = zipf_hit_rate(PageCacheEvictionPolicy::S3Fifo(S3FifoConfig::new(
            MAX_BUFFERS,
        )));
        let arc = zipf_hit_rate(PageCacheEvictionPolicy::Arc(ArcEvictionConfig::new(
            MAX_BUFFERS,
        )));
        let adaptive = zipf_hit_rate(PageCacheEvictionPolicy::S3FifoAdaptive(S3FifoConfig::new(
            MAX_BUFFERS,
        )));

        eprintln!(
            "bead_id=bd-9johq workload=zipfian domain={DOMAIN} max_buffers={MAX_BUFFERS} skew={SKEW} accesses={ACCESSES}"
        );
        eprintln!(
            "bead_id=bd-9johq hit_rate%: arbitrary={arbitrary:.2} s3fifo={s3fifo:.2} arc={arc:.2} s3fifo_adaptive={adaptive:.2}"
        );

        for (name, rate) in [
            ("arbitrary", arbitrary),
            ("s3fifo", s3fifo),
            ("arc", arc),
            ("s3fifo_adaptive", adaptive),
        ] {
            assert!(
                (0.0..=100.0).contains(&rate),
                "bead_id=bd-9johq case=zipf_valid_hit_rate policy={name} hit_rate={rate}"
            );
        }
        // Frequency-aware reconstruction policies must exploit the skew (keep
        // hot keys resident) — proves they engage end-to-end on this workload.
        for (name, rate) in [
            ("s3fifo", s3fifo),
            ("arc", arc),
            ("s3fifo_adaptive", adaptive),
        ] {
            assert!(
                rate >= 10.0,
                "bead_id=bd-9johq case=zipf_freq_aware_exploits_skew policy={name} hit_rate={rate:.2}"
            );
        }
    }

    #[test]
    fn test_cache_monitor_page_snapshots_rank_hot_pages_by_access_frequency() {
        let cache = ShardedPageCache::with_max_buffers_and_shards(PageSize::DEFAULT, 32, 1);
        cache.set_eviction_policy(PageCacheEvictionPolicy::S3Fifo(S3FifoConfig::new(32)));
        cache.reset_metrics();

        let hot_pages = [
            PageNumber::ONE,
            PageNumber::new(2).unwrap(),
            PageNumber::new(3).unwrap(),
        ];
        let cold_pages = [
            PageNumber::new(21).unwrap(),
            PageNumber::new(22).unwrap(),
            PageNumber::new(23).unwrap(),
            PageNumber::new(24).unwrap(),
            PageNumber::new(25).unwrap(),
            PageNumber::new(26).unwrap(),
        ];

        let clock_start = monotonic_test_clock();
        for page_no in cold_pages {
            touch_monitored_page(&cache, page_no);
        }
        for _ in 0..48 {
            for page_no in hot_pages {
                touch_monitored_page(&cache, page_no);
            }
        }
        let elapsed = clock_start.elapsed();

        let snapshot = cache.metrics_snapshot();
        let access_counts: HashMap<PageNumber, u64> = cache
            .page_snapshots()
            .into_iter()
            .map(|entry| (entry.page_no, entry.access_count))
            .collect();
        let hot_min = hot_pages
            .iter()
            .map(|page_no| {
                *access_counts
                    .get(page_no)
                    .expect("hot page must appear in page snapshots")
            })
            .min()
            .expect("hot pages should not be empty");
        let cold_max = cold_pages
            .iter()
            .map(|page_no| {
                *access_counts
                    .get(page_no)
                    .expect("cold page must appear in page snapshots")
            })
            .max()
            .expect("cold pages should not be empty");

        emit_cache_monitor_log(
            "test_cache_monitor_page_snapshots_rank_hot_pages_by_access_frequency",
            "working_set",
            elapsed,
            snapshot,
            json!({
                "hot_pages": hot_pages.iter().map(|page_no| page_no.get()).collect::<Vec<_>>(),
                "cold_pages": cold_pages.iter().map(|page_no| page_no.get()).collect::<Vec<_>>(),
                "hot_min_access_count": hot_min,
                "cold_max_access_count": cold_max,
                "replay_hint": "cargo test -p fsqlite-pager test_cache_monitor_page_snapshots_rank_hot_pages_by_access_frequency -- --nocapture"
            }),
        );

        assert!(
            hot_min > cold_max,
            "page snapshots should preserve access-frequency separation for working-set analysis: hot_min={hot_min} cold_max={cold_max}"
        );
    }

    #[derive(Debug, Clone, Copy)]
    enum BenchEvictionPolicy {
        Arbitrary,
        ReconstructedS3Fifo(S3FifoConfig),
    }

    #[derive(Debug, Clone)]
    struct PrototypeTrace {
        config: S3FifoConfig,
        adaptation_interval: usize,
        adaptive_bounds: (usize, usize),
        access_trace: VecDeque<PageNumber>,
        max_trace_entries: usize,
    }

    impl PrototypeTrace {
        fn new(config: S3FifoConfig) -> Self {
            let probe = S3Fifo::with_config(config);
            let max_trace_entries = config.capacity().saturating_mul(8).max(64);
            Self {
                config,
                adaptation_interval: probe.adaptation_interval(),
                adaptive_bounds: probe.adaptive_bounds(),
                access_trace: VecDeque::with_capacity(max_trace_entries),
                max_trace_entries,
            }
        }

        fn record_access(&mut self, page_no: PageNumber) {
            if self.access_trace.len() >= self.max_trace_entries {
                let _ = self.access_trace.pop_front();
            }
            self.access_trace.push_back(page_no);
        }

        fn record_admit(&mut self, page_no: PageNumber) {
            self.record_access(page_no);
        }

        fn choose_victim(&self, cache: &PageCache) -> Option<PageNumber> {
            let mut model = self.build_model(cache)?;
            let synthetic_miss = choose_synthetic_miss_page_for_bench(cache)?;
            let events = model.insert(synthetic_miss);
            events.iter().find_map(|event| match event {
                S3FifoEvent::EvictedFromSmallToGhost(page_no)
                | S3FifoEvent::EvictedFromMain(page_no)
                    if cache.pages.contains_key(page_no) =>
                {
                    Some(*page_no)
                }
                _ => None,
            })
        }

        fn build_model(&self, cache: &PageCache) -> Option<S3Fifo> {
            let resident_pages = cache.pages.len();
            if resident_pages == 0 {
                return None;
            }

            let resident_keys: std::collections::HashSet<PageNumber> =
                cache.pages.keys().copied().collect();
            let mut resident_order: Vec<PageNumber> = resident_keys.iter().copied().collect();
            resident_order.sort_unstable_by_key(|page_no| page_no.get());

            let mut model = S3Fifo::with_config(self.scaled_config(resident_pages));
            model.set_adaptation_interval(self.adaptation_interval);
            let (min_bound, max_bound) = self.scaled_bounds(resident_pages);
            model.set_adaptive_bounds(min_bound, max_bound);

            for &page_no in &self.access_trace {
                if !resident_keys.contains(&page_no) {
                    continue;
                }
                if !model.access(page_no) {
                    let _ = model.insert(page_no);
                }
            }

            let mut remaining_rounds = resident_order.len().saturating_mul(2).max(1);
            while remaining_rounds > 0 {
                let missing: Vec<PageNumber> = resident_order
                    .iter()
                    .copied()
                    .filter(|page_no| {
                        !matches!(
                            model.lookup(*page_no),
                            Some(location) if location.kind != QueueKind::Ghost
                        )
                    })
                    .collect();
                if missing.is_empty() {
                    break;
                }
                for page_no in missing {
                    let _ = model.insert(page_no);
                }
                remaining_rounds = remaining_rounds.saturating_sub(1);
            }

            Some(model)
        }

        fn scaled_config(&self, resident_pages: usize) -> S3FifoConfig {
            let capacity = resident_pages.max(1);
            let prototype_capacity = self.config.capacity().max(1);
            let small_capacity =
                scale_nonzero_for_bench(self.config.small_capacity(), prototype_capacity, capacity)
                    .clamp(1, capacity);
            let ghost_capacity =
                scale_nonzero_for_bench(self.config.ghost_capacity(), prototype_capacity, capacity)
                    .max(1);
            S3FifoConfig::with_limits(
                capacity,
                small_capacity,
                ghost_capacity,
                self.config.max_reinsert(),
            )
        }

        fn scaled_bounds(&self, resident_pages: usize) -> (usize, usize) {
            let capacity = resident_pages.max(1);
            let prototype_capacity = self.config.capacity().max(1);
            let min_bound =
                scale_nonzero_for_bench(self.adaptive_bounds.0, prototype_capacity, capacity)
                    .clamp(1, capacity);
            let max_bound =
                scale_nonzero_for_bench(self.adaptive_bounds.1, prototype_capacity, capacity)
                    .clamp(min_bound, capacity);
            (min_bound, max_bound)
        }
    }

    fn scale_nonzero_for_bench(value: usize, from_capacity: usize, to_capacity: usize) -> usize {
        if value == 0 || to_capacity == 0 {
            return 0;
        }
        let numerator = value.saturating_mul(to_capacity);
        numerator.saturating_add(from_capacity.saturating_sub(1)) / from_capacity.max(1)
    }

    fn choose_synthetic_miss_page_for_bench(cache: &PageCache) -> Option<PageNumber> {
        let mut candidate = u32::MAX - 1;
        loop {
            let page_no = PageNumber::new(candidate)?;
            if !cache.pages.contains_key(&page_no) {
                return Some(page_no);
            }
            if candidate == 1 {
                return None;
            }
            candidate = candidate.saturating_sub(1);
        }
    }

    #[derive(Debug, Clone, Copy)]
    struct HotColdEvictionBenchResult {
        elapsed: Duration,
        hot_hits: u64,
        hot_misses: u64,
        resident_hot_checks: u64,
        resident_hot_kept: u64,
        checksum: u64,
    }

    impl HotColdEvictionBenchResult {
        fn hot_hit_pct(self) -> f64 {
            let total = self.hot_hits.saturating_add(self.hot_misses);
            if total == 0 {
                0.0
            } else {
                (self.hot_hits as f64 * 100.0) / total as f64
            }
        }

        fn resident_hot_pct(self) -> f64 {
            if self.resident_hot_checks == 0 {
                0.0
            } else {
                (self.resident_hot_kept as f64 * 100.0) / self.resident_hot_checks as f64
            }
        }
    }

    fn benchmark_hot_cold_eviction(policy: BenchEvictionPolicy) -> HotColdEvictionBenchResult {
        const CAPACITY: usize = 64;
        const HOT_SET: usize = 8;
        const HOT_TOUCHES_PER_ROUND: usize = 6;
        const COLD_BURST: usize = 16;
        const ROUNDS: usize = 250;

        let mut cache = PageCache::new(PageSize::DEFAULT);
        let mut prototype = match policy {
            BenchEvictionPolicy::Arbitrary => None,
            BenchEvictionPolicy::ReconstructedS3Fifo(config) => Some(PrototypeTrace::new(config)),
        };
        let hot_pages = [7_u32, 113, 251, 389, 521, 659, 797, 941]
            .map(|page_no| PageNumber::new(page_no).unwrap());
        let mut next_cold_page = 10_000_u32;
        let mut hot_hits = 0_u64;
        let mut hot_misses = 0_u64;
        let mut resident_hot_checks = 0_u64;
        let mut resident_hot_kept = 0_u64;
        let mut checksum = 0_u64;

        for (idx, page_no) in hot_pages.iter().copied().enumerate() {
            let page = cache.insert_fresh(page_no).unwrap();
            page[0] = u8::try_from(idx + 1).unwrap();
            if let Some(trace) = &mut prototype {
                trace.record_admit(page_no);
            }
        }

        let started = Instant::now();
        for round in 0..ROUNDS {
            for _ in 0..HOT_TOUCHES_PER_ROUND {
                for (idx, page_no) in hot_pages.iter().copied().enumerate() {
                    if let Some(page) = cache.get_mut(page_no) {
                        hot_hits = hot_hits.saturating_add(1);
                        checksum = checksum.saturating_add(u64::from(page[0]));
                        if let Some(trace) = &mut prototype {
                            trace.record_access(page_no);
                        }
                    } else {
                        hot_misses = hot_misses.saturating_add(1);
                        let page = cache.insert_fresh(page_no).unwrap();
                        page[0] = u8::try_from((idx + round) % 251).unwrap_or(0);
                        if let Some(trace) = &mut prototype {
                            trace.record_admit(page_no);
                        }
                        if cache.len() > CAPACITY {
                            match &prototype {
                                Some(trace) => {
                                    let victim = trace
                                        .choose_victim(&cache)
                                        .or_else(|| cache.pages.keys().next().copied());
                                    assert!(
                                        victim.is_some_and(|page_no| cache.evict(page_no)),
                                        "test-only prototype eviction must free capacity"
                                    );
                                }
                                None => assert!(
                                    cache.evict_any(),
                                    "arbitrary hot-page reinsertion must free capacity"
                                ),
                            }
                        }
                    }
                }
            }

            for burst_idx in 0..COLD_BURST {
                let page_no = PageNumber::new(next_cold_page).unwrap();
                next_cold_page = next_cold_page.saturating_add(1);
                let page = cache.insert_fresh(page_no).unwrap();
                page[0] = u8::try_from((round + burst_idx) % 251).unwrap_or(0);
                if let Some(trace) = &mut prototype {
                    trace.record_admit(page_no);
                }
                if cache.len() > CAPACITY {
                    match &prototype {
                        Some(trace) => {
                            let victim = trace
                                .choose_victim(&cache)
                                .or_else(|| cache.pages.keys().next().copied());
                            assert!(
                                victim.is_some_and(|page_no| cache.evict(page_no)),
                                "test-only prototype eviction must free capacity"
                            );
                        }
                        None => {
                            assert!(cache.evict_any(), "cold-page admission must free capacity");
                        }
                    }
                }
            }

            resident_hot_checks = resident_hot_checks.saturating_add(HOT_SET as u64);
            resident_hot_kept = resident_hot_kept.saturating_add(
                hot_pages
                    .iter()
                    .filter(|page_no| cache.contains(**page_no))
                    .count() as u64,
            );
        }

        HotColdEvictionBenchResult {
            elapsed: started.elapsed(),
            hot_hits,
            hot_misses,
            resident_hot_checks,
            resident_hot_kept,
            checksum: black_box(checksum),
        }
    }

    fn median_duration(samples: &[HotColdEvictionBenchResult]) -> Duration {
        let mut elapsed_nanos: Vec<u128> = samples
            .iter()
            .map(|sample| sample.elapsed.as_nanos())
            .collect();
        elapsed_nanos.sort_unstable();
        let middle = elapsed_nanos[elapsed_nanos.len() / 2];
        let nanos = u64::try_from(middle).unwrap_or(u64::MAX);
        Duration::from_nanos(nanos)
    }

    #[test]
    #[ignore = "benchmark evidence only"]
    fn page_cache_hot_cold_eviction_microbench_report() {
        const SAMPLE_COUNT: usize = 3;

        let _ = benchmark_hot_cold_eviction(BenchEvictionPolicy::Arbitrary);
        let _ = benchmark_hot_cold_eviction(BenchEvictionPolicy::ReconstructedS3Fifo(
            S3FifoConfig::new(64),
        ));

        let arbitrary_samples: Vec<HotColdEvictionBenchResult> = (0..SAMPLE_COUNT)
            .map(|_| benchmark_hot_cold_eviction(BenchEvictionPolicy::Arbitrary))
            .collect();
        let prototype_samples: Vec<HotColdEvictionBenchResult> = (0..SAMPLE_COUNT)
            .map(|_| {
                benchmark_hot_cold_eviction(BenchEvictionPolicy::ReconstructedS3Fifo(
                    S3FifoConfig::new(64),
                ))
            })
            .collect();

        let arbitrary = arbitrary_samples[0];
        let prototype = prototype_samples[0];
        let arbitrary_median = median_duration(&arbitrary_samples);
        let prototype_median = median_duration(&prototype_samples);

        println!(
            "policy=Arbitrary median_ms={:.3} hot_hit_pct={:.2} resident_hot_pct={:.2} checksum={}",
            arbitrary_median.as_secs_f64() * 1_000.0,
            arbitrary.hot_hit_pct(),
            arbitrary.resident_hot_pct(),
            arbitrary.checksum
        );
        println!(
            "policy=ReconstructedS3Fifo median_ms={:.3} hot_hit_pct={:.2} resident_hot_pct={:.2} checksum={}",
            prototype_median.as_secs_f64() * 1_000.0,
            prototype.hot_hit_pct(),
            prototype.resident_hot_pct(),
            prototype.checksum
        );
        println!(
            "delta_hot_hit_pct={:.2} delta_resident_hot_pct={:.2} slowdown_x={:.2}",
            prototype.hot_hit_pct() - arbitrary.hot_hit_pct(),
            prototype.resident_hot_pct() - arbitrary.resident_hot_pct(),
            prototype_median.as_secs_f64() / arbitrary_median.as_secs_f64()
        );

        assert!(
            prototype.hot_hit_pct() > arbitrary.hot_hit_pct(),
            "prototype must improve hot hit rate on the focused hot/cold workload"
        );
        assert!(
            prototype.resident_hot_pct() > arbitrary.resident_hot_pct(),
            "prototype must retain more hot pages across cold bursts"
        );
    }

    // -----------------------------------------------------------------------
    // bd-22n.8 — Allocation-Free Read Path Tests (Pager Layer)
    // -----------------------------------------------------------------------

    const BEAD_22N8: &str = "bd-22n.8";

    #[test]
    fn test_cache_lookup_no_alloc() {
        // bd-22n.8: Buffer pool cache lookup is allocation-free.
        //
        // PageCache::get() returns Option<&[u8]> — a reference into the
        // pool-allocated buffer.  It does a HashMap::get + PageBuf::as_slice,
        // neither of which allocates.
        //
        // We verify by: (a) checking the returned &[u8] is the same pointer
        // as the original PageBuf, and (b) repeating the lookup many times
        // and verifying pointer stability (proves no reallocation).
        let (cx, mut file) = setup();

        let data = vec![0xBE_u8; 4096];
        file.write(&cx, &data, 0).unwrap();

        let mut cache = PageCache::new(PageSize::DEFAULT);
        let page1 = PageNumber::ONE;

        // Cold read — allocates from pool.
        let initial = cache.read_page(&cx, &mut file, page1).unwrap();
        let initial_ptr = initial.as_ptr();

        // Hot reads — must be allocation-free (same pointer).
        for round in 0..100u32 {
            let cached = cache.get(page1).unwrap();
            assert_eq!(
                cached.as_ptr(),
                initial_ptr,
                "bead_id={BEAD_22N8} case=cache_lookup_no_alloc \
                 round={round} pointer must be stable (no realloc)"
            );
        }
    }

    #[test]
    fn test_cache_lookup_hit_returns_reference() {
        // bd-22n.8: Verify structurally that get() returns a borrow, not a copy.
        // We insert a page, mutate it via get_mut, then verify get() sees the
        // mutation at the same pointer — proving it's a reference into the
        // same memory.
        let mut cache = PageCache::new(PageSize::DEFAULT);
        let page1 = PageNumber::ONE;

        // Insert a fresh page and write a sentinel.
        let fresh = cache.insert_fresh(page1).unwrap();
        fresh[0] = 0xAA;
        let ptr_after_insert = cache.get(page1).unwrap().as_ptr();

        // Mutate in place.
        let mutref = cache.get_mut(page1).unwrap();
        mutref[1] = 0xBB;

        // get() must see the mutation AND return the same pointer.
        let read_back = cache.get(page1).unwrap();
        assert_eq!(
            read_back.as_ptr(),
            ptr_after_insert,
            "bead_id={BEAD_22N8} case=cache_lookup_returns_reference \
             pointer must be stable through mutation"
        );
        assert_eq!(read_back[0], 0xAA);
        assert_eq!(read_back[1], 0xBB);
    }

    #[test]
    fn test_pool_reuse_avoids_alloc_on_reread() {
        // bd-22n.8: After eviction, re-reading a page reuses a pool buffer
        // rather than allocating fresh memory.  This ensures the read path
        // is allocation-free in steady state (pool has recycled buffers).
        let (cx, mut file) = setup();

        let data = vec![0xDD_u8; 4096];
        file.write(&cx, &data, 0).unwrap();

        let mut cache = PageCache::new(PageSize::DEFAULT);
        let page1 = PageNumber::ONE;

        // Cold read, then evict.
        let _ = cache.read_page(&cx, &mut file, page1).unwrap();
        assert_eq!(cache.pool().available(), 0);
        cache.evict(page1);
        assert_eq!(
            cache.pool().available(),
            1,
            "bead_id={BEAD_22N8} case=evicted_buffer_returned_to_pool"
        );

        // Re-read: pool has a buffer, so no new allocation needed.
        let reread = cache.read_page(&cx, &mut file, page1).unwrap();
        assert_eq!(
            reread,
            data.as_slice(),
            "bead_id={BEAD_22N8} case=pool_reuse_data_correct"
        );
        assert_eq!(
            cache.pool().available(),
            0,
            "bead_id={BEAD_22N8} case=pool_buffer_consumed_on_reread"
        );
    }

    // -----------------------------------------------------------------------
    // bd-3wop3.2 — ShardedPageCache Tests
    // -----------------------------------------------------------------------

    const BEAD_3WOP3_2: &str = "bd-3wop3.2";
    const SHARDED_CACHE_PERF_REPLAY: &str = "cargo test --profile release-perf -p \
        fsqlite-pager page_cache::tests::test_sharded_cache_throughput_vs_single -- \
        --exact --nocapture --test-threads=1";

    fn measure_single_cache_insert_read(iterations: u32) -> Duration {
        let mut cache = PageCache::new(PageSize::DEFAULT);
        let mut checksum = 0_u8;
        let started = Instant::now();
        for i in 1..=iterations {
            let page_no = PageNumber::new(i).unwrap();
            let value = u8::try_from(i & 0xff).unwrap();
            cache.insert_fresh(page_no).unwrap()[0] = value;
            checksum ^= black_box(cache.get(page_no).unwrap()[0]);
        }
        black_box(checksum);
        started.elapsed()
    }

    fn measure_sharded_cache_insert_read(iterations: u32) -> Duration {
        let cache = ShardedPageCache::new(PageSize::DEFAULT);
        let mut checksum = 0_u8;
        let started = Instant::now();
        for i in 1..=iterations {
            let page_no = PageNumber::new(i).unwrap();
            let value = u8::try_from(i & 0xff).unwrap();
            cache.insert_fresh(page_no, |data| data[0] = value).unwrap();
            checksum ^= black_box(cache.with_page(page_no, |data| data[0]).unwrap());
        }
        black_box(checksum);
        started.elapsed()
    }

    fn median_ratio(ratios: &mut [f64]) -> f64 {
        assert!(!ratios.is_empty(), "median requires at least one ratio");
        ratios.sort_by(f64::total_cmp);
        ratios[ratios.len() / 2]
    }

    #[test]
    fn test_sharded_cache_perf_median_uses_middle_sample() {
        let mut ratios = [9.0, 1.0, 7.0, 3.0, 5.0];
        assert_eq!(median_ratio(&mut ratios), 5.0);
    }

    #[test]
    fn test_sharded_cache_basic_operations() {
        // Basic insert/get/evict operations on sharded cache.
        let cache = ShardedPageCache::new(PageSize::DEFAULT);

        let p1 = PageNumber::ONE;
        let p2 = PageNumber::new(2).unwrap();

        // Insert two pages
        cache.insert_fresh(p1, |data| data[0] = 0xAA).unwrap();
        cache.insert_fresh(p2, |data| data[0] = 0xBB).unwrap();

        assert_eq!(cache.len(), 2);
        assert!(cache.contains(p1));
        assert!(cache.contains(p2));

        // Read back
        cache.with_page(p1, |data| assert_eq!(data[0], 0xAA));
        cache.with_page(p2, |data| assert_eq!(data[0], 0xBB));

        // Evict one
        assert!(cache.evict(p1));
        assert!(!cache.contains(p1));
        assert!(cache.contains(p2));
        assert_eq!(cache.len(), 1);

        // Metrics
        let m = cache.metrics_snapshot();
        assert_eq!(m.admits, 2, "bead_id={BEAD_3WOP3_2} case=basic_admits");
        assert_eq!(
            m.evictions, 1,
            "bead_id={BEAD_3WOP3_2} case=basic_evictions"
        );
    }

    #[test]
    fn test_sharded_cache_shard_distribution() {
        // Verify that pages are distributed across multiple shards.
        let cache = ShardedPageCache::new(PageSize::DEFAULT);

        // Insert 256 sequential pages (should use multiple shards)
        for i in 1..=256u32 {
            let pn = PageNumber::new(i).unwrap();
            cache.insert_fresh(pn, |_| {}).unwrap();
        }

        let dist = cache.shard_distribution();
        assert_eq!(dist.len(), cache.shard_count());

        // Count non-empty shards
        let non_empty = dist.iter().filter(|&&n| n > 0).count();

        // With 256 pages and the default shard count, we expect good distribution.
        // Multiplicative hashing should spread sequential keys well.
        assert!(
            non_empty >= 64,
            "bead_id={BEAD_3WOP3_2} case=shard_distribution \
             expected at least 64 non-empty shards, got {non_empty}"
        );

        // No shard should have more than ~10 pages (avg is 2)
        let max_per_shard = *dist.iter().max().unwrap();
        assert!(
            max_per_shard <= 16,
            "bead_id={BEAD_3WOP3_2} case=shard_balance \
             expected max 16 pages per shard, got {max_per_shard}"
        );
    }

    #[test]
    fn test_sharded_cache_cross_shard_eviction() {
        // Verify evict_any() can find pages across different shards.
        let cache = ShardedPageCache::new(PageSize::DEFAULT);

        // Insert pages that should land in different shards
        for i in 1..=16u32 {
            let pn = PageNumber::new(i * 100).unwrap();
            cache.insert_fresh(pn, |_| {}).unwrap();
        }

        assert_eq!(cache.len(), 16);

        // Evict all via evict_any()
        let mut evicted = 0;
        while cache.evict_any() {
            evicted += 1;
            assert!(
                evicted <= 100,
                "bead_id={BEAD_3WOP3_2} case=cross_shard_eviction infinite loop"
            );
        }

        assert_eq!(
            evicted, 16,
            "bead_id={BEAD_3WOP3_2} case=cross_shard_eviction_count"
        );
        assert!(cache.is_empty());
    }

    #[test]
    fn test_flat_slot_snapshot_retries_after_pgno_flip_before_lock() {
        let slots = std::sync::Arc::new(FlatPageSlots::new(8));
        let pool = PageBufPool::new(PageSize::DEFAULT, 2);
        let old_page = PageNumber::ONE;
        let new_page = PageNumber::new(2).unwrap();

        let old_buf = pool.acquire().unwrap();
        assert!(slots.try_insert(old_page, old_buf).unwrap());

        let slot_idx = slots.find_slot(old_page).unwrap();
        let slot = &slots.slots[slot_idx];
        let mut guard = slot.data.lock();
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
        let slots_for_thread = std::sync::Arc::clone(&slots);
        let barrier_for_thread = std::sync::Arc::clone(&barrier);

        let handle = std::thread::spawn(move || {
            slots_for_thread.slots[slot_idx].stable_snapshot_with_barrier(&barrier_for_thread)
        });

        barrier.wait();
        slot.pgno.store(new_page.get(), Ordering::Release);

        let new_buf = pool.acquire().unwrap();
        *guard = Some(CachedPageEntry::new(new_buf));
        drop(guard);

        let snapshot = handle
            .join()
            .unwrap()
            .expect("snapshot should retry and observe the replacement page");
        assert_eq!(
            snapshot.page_no, new_page,
            "snapshot must not pair a stale page number with a newer slot entry"
        );
    }

    #[test]
    fn test_atomic_slot_publish_waits_for_payload_install() {
        let slots = std::sync::Arc::new(FlatPageSlots::new(8));
        let page_no = PageNumber::ONE;
        let slot_idx = slots.hash_pgno(page_no.get());
        let slot = &slots.slots[slot_idx];
        let guard = slot.data.lock();
        let start = std::sync::Arc::new(std::sync::Barrier::new(2));
        let thread_slots = std::sync::Arc::clone(&slots);
        let thread_start = std::sync::Arc::clone(&start);

        let handle = std::thread::spawn(move || {
            thread_start.wait();
            let mut buf = PageBuf::new(PageSize::DEFAULT);
            buf.as_mut_slice().fill(page_pattern(page_no));
            thread_slots
                .try_insert(page_no, buf)
                .expect("atomic slot insert should stay in flat slots")
        });

        start.wait();
        let clock_start = monotonic_test_clock();
        while clock_start.elapsed() < Duration::from_millis(20) {
            assert_ne!(
                slot.pgno.load(Ordering::Acquire),
                page_no.get(),
                "bead_id={BEAD_TZLZB} case=publish_after_payload_install"
            );
            std::thread::yield_now();
        }

        drop(guard);
        assert!(
            handle.join().expect("slot insert thread must not panic"),
            "bead_id={BEAD_TZLZB} case=insert_new_after_payload_lock_released"
        );
        let copy = slots
            .get_copy(page_no)
            .expect("published page should be readable after payload install");
        assert_eq!(
            copy[0],
            page_pattern(page_no),
            "bead_id={BEAD_TZLZB} case=published_payload_matches"
        );
    }

    #[test]
    fn test_sharded_cache_clear() {
        let cache = ShardedPageCache::new(PageSize::DEFAULT);

        for i in 1..=100u32 {
            let pn = PageNumber::new(i).unwrap();
            cache.insert_fresh(pn, |_| {}).unwrap();
        }

        assert_eq!(cache.len(), 100);
        cache.clear();
        assert!(cache.is_empty());
        assert_eq!(cache.len(), 0);

        let m = cache.metrics_snapshot();
        assert_eq!(
            m.evictions, 100,
            "bead_id={BEAD_3WOP3_2} case=clear_evictions"
        );
    }

    /// Shape: connection-open `refresh_committed_state` calls `cache.clear()`
    /// on a freshly-allocated cache where the flat-slot fast path has absorbed
    /// every admit (the dominant case at MT8). Before the `shards_dirty`
    /// short-circuit, this still walked all 128 shard mutexes per call.
    ///
    /// Toggling the flag's "true at start" state via `shards_dirty.store(true)`
    /// lets us pair an apples-to-apples baseline (forced 128-shard walk) and
    /// optimized run (early swap returns false) inside one bench, in one build,
    /// avoiding cross-binary noise.
    ///
    /// Run via:
    ///   cargo test -p fsqlite-pager --lib --release --
    ///     --ignored --nocapture
    ///     bench_sharded_cache_clear_empty_shards_microbench
    #[test]
    #[ignore = "microbench, run with --ignored --nocapture"]
    fn bench_sharded_cache_clear_empty_shards_microbench() {
        const ITERS: usize = 1_000_000;
        let cache = ShardedPageCache::new(PageSize::DEFAULT);

        // Warmup the optimized fast path.
        for _ in 0..ITERS / 10 {
            cache.clear();
        }

        // Optimized: shards_dirty starts false, swap returns false, no walk.
        let start = std::time::Instant::now();
        for _ in 0..ITERS {
            cache.clear();
        }
        let elapsed_opt = start.elapsed();
        let per_call_opt = elapsed_opt / u32::try_from(ITERS).unwrap();

        // Baseline: force shards_dirty=true at the top of every clear so the
        // 128-shard mutex walk fires, the same shape the previous
        // unconditional walk paid on every connection open.
        let start = std::time::Instant::now();
        for _ in 0..ITERS {
            cache.shards_dirty.store(true, Ordering::Release);
            cache.clear();
        }
        let elapsed_base = start.elapsed();
        let per_call_base = elapsed_base / u32::try_from(ITERS).unwrap();

        let speedup = per_call_base.as_nanos() as f64 / per_call_opt.as_nanos().max(1) as f64;
        eprintln!(
            "bench_sharded_cache_clear_empty_shards_microbench: ITERS={ITERS} \
             baseline={per_call_base:?} optimized={per_call_opt:?} \
             speedup={speedup:.2}x"
        );
    }

    #[test]
    fn test_sharded_cache_metrics_aggregation() {
        let cache = ShardedPageCache::new(PageSize::DEFAULT);

        // Generate cache hits and misses across multiple shards
        for i in 1..=50u32 {
            let pn = PageNumber::new(i).unwrap();
            cache.insert_fresh(pn, |_| {}).unwrap();
        }

        // Hit existing pages
        for i in 1..=50u32 {
            let pn = PageNumber::new(i).unwrap();
            cache.with_page(pn, |_| {});
        }

        // Miss non-existent pages
        for i in 51..=100u32 {
            let pn = PageNumber::new(i).unwrap();
            cache.with_page(pn, |_| {});
        }

        let m = cache.metrics_snapshot();
        assert_eq!(m.admits, 50, "bead_id={BEAD_3WOP3_2} case=metrics_admits");
        assert_eq!(m.hits, 50, "bead_id={BEAD_3WOP3_2} case=metrics_hits");
        assert_eq!(m.misses, 50, "bead_id={BEAD_3WOP3_2} case=metrics_misses");
        assert_eq!(
            m.cached_pages, 50,
            "bead_id={BEAD_3WOP3_2} case=metrics_cached_pages"
        );

        cache.reset_metrics();
        let reset = cache.metrics_snapshot();
        assert_eq!(reset.hits, 0, "bead_id={BEAD_3WOP3_2} case=reset_metrics");
        assert_eq!(reset.misses, 0);
        assert_eq!(reset.admits, 0);
        // cached_pages should still be 50 (reset doesn't clear data)
        assert_eq!(reset.cached_pages, 50);
    }

    #[test]
    fn test_sharded_cache_shard_padding_alignment() {
        // Verify cache-line alignment by checking struct sizes.
        // PageCacheShard is #[repr(align(64))], so size must be multiple of 64.
        let shard_size = std::mem::size_of::<PageCacheShard>();
        assert!(
            shard_size >= 64,
            "bead_id={BEAD_3WOP3_2} case=shard_padding \
             PageCacheShard size {shard_size} should be >= 64 bytes"
        );
        assert_eq!(
            shard_size % 64,
            0,
            "bead_id={BEAD_3WOP3_2} case=shard_alignment \
             PageCacheShard size {shard_size} must be multiple of 64"
        );

        // Verify alignment requirement
        let shard_align = std::mem::align_of::<PageCacheShard>();
        assert_eq!(
            shard_align, 64,
            "bead_id={BEAD_3WOP3_2} case=shard_align_req \
             PageCacheShard alignment should be 64, got {shard_align}"
        );
    }

    #[test]
    fn test_sharded_cache_with_page_mut() {
        let cache = ShardedPageCache::new(PageSize::DEFAULT);
        let p1 = PageNumber::ONE;

        cache.insert_fresh(p1, |data| data.fill(0)).unwrap();

        // Mutate via callback
        cache.with_page_mut(p1, |data| {
            data[0] = 0x12;
            data[1] = 0x34;
        });

        // Verify mutation persisted
        cache.with_page(p1, |data| {
            assert_eq!(data[0], 0x12, "bead_id={BEAD_3WOP3_2} case=with_page_mut_0");
            assert_eq!(data[1], 0x34, "bead_id={BEAD_3WOP3_2} case=with_page_mut_1");
        });
    }

    #[test]
    fn test_sharded_cache_insert_buffer() {
        let cache = ShardedPageCache::new(PageSize::DEFAULT);
        let p1 = PageNumber::ONE;

        // Acquire a buffer from the pool
        let mut buf = cache.pool().acquire().unwrap();
        buf.as_mut_slice().fill(0xEE);

        cache.insert_buffer(p1, buf);

        assert!(cache.contains(p1));
        cache.with_page(p1, |data| {
            assert!(
                data.iter().all(|&b| b == 0xEE),
                "bead_id={BEAD_3WOP3_2} case=insert_buffer_data"
            );
        });
    }

    #[test]
    fn test_sharded_cache_vfs_read_write() {
        let (cx, mut file) = setup();

        // Write test data to VFS
        let test_data = vec![0xAB_u8; 4096];
        file.write(&cx, &test_data, 0).unwrap();

        let cache = ShardedPageCache::new(PageSize::DEFAULT);
        let p1 = PageNumber::ONE;

        // Read through cache
        let result = cache.read_page(&cx, &mut file, p1, |data| {
            assert_eq!(
                data,
                test_data.as_slice(),
                "bead_id={BEAD_3WOP3_2} case=vfs_read_data"
            );
            data[0]
        });
        assert_eq!(result.unwrap(), 0xAB);

        // Modify and write back
        cache.with_page_mut(p1, |data| data[0] = 0xCD);
        cache.write_page(&cx, &mut file, p1).unwrap();

        // Verify write
        let mut verify = vec![0u8; 4096];
        file.read(&cx, &mut verify, 0).unwrap();
        assert_eq!(
            verify[0], 0xCD,
            "bead_id={BEAD_3WOP3_2} case=vfs_write_verify"
        );
    }

    // --- Concurrency tests ---

    #[test]
    fn test_sharded_cache_8_threads_no_deadlock() {
        // 8 threads performing concurrent operations without deadlock.
        use std::sync::Arc;
        use std::thread;

        let cache = Arc::new(ShardedPageCache::new(PageSize::DEFAULT));
        let num_threads = 8;
        let ops_per_thread = 1000;

        let handles: Vec<_> = (0..num_threads)
            .map(|tid| {
                let c = Arc::clone(&cache);
                thread::spawn(move || {
                    for i in 0..ops_per_thread {
                        // Each thread works on different page ranges to avoid conflicts
                        let base = tid * 10000 + i;
                        let pn = PageNumber::new(base as u32 + 1).unwrap();

                        // Insert
                        c.insert_fresh(pn, |data| data[0] = (tid & 0xFF) as u8)
                            .unwrap();

                        // Read back
                        c.with_page(pn, |data| {
                            assert_eq!(data[0], (tid & 0xFF) as u8);
                        });

                        // Evict every 10th
                        if i % 10 == 0 {
                            c.evict(pn);
                        }
                    }
                })
            })
            .collect();

        for h in handles {
            h.join()
                .expect("bead_id={BEAD_3WOP3_2} case=8t_no_deadlock thread panic");
        }

        // Verify no deadlock occurred (we reached here)
        let m = cache.metrics_snapshot();
        assert!(
            m.admits >= (num_threads * ops_per_thread) as u64,
            "bead_id={BEAD_3WOP3_2} case=8t_admits"
        );
    }

    #[test]
    fn test_sharded_cache_16_threads_no_deadlock() {
        // 16 threads performing concurrent operations without deadlock.
        use std::sync::Arc;
        use std::thread;

        let cache = Arc::new(ShardedPageCache::new(PageSize::DEFAULT));
        let num_threads = 16;
        let ops_per_thread = 500;

        let handles: Vec<_> = (0..num_threads)
            .map(|tid| {
                let c = Arc::clone(&cache);
                thread::spawn(move || {
                    for i in 0..ops_per_thread {
                        let base = tid * 10000 + i;
                        let pn = PageNumber::new(base as u32 + 1).unwrap();

                        c.insert_fresh(pn, |data| data[0] = ((tid * 7) & 0xFF) as u8)
                            .unwrap();
                        c.with_page(pn, |data| {
                            assert_eq!(data[0], ((tid * 7) & 0xFF) as u8);
                        });

                        if i % 5 == 0 {
                            c.evict(pn);
                        }
                    }
                })
            })
            .collect();

        for h in handles {
            h.join()
                .expect("bead_id={BEAD_3WOP3_2} case=16t_no_deadlock thread panic");
        }

        let m = cache.metrics_snapshot();
        assert!(
            m.admits >= (num_threads * ops_per_thread) as u64,
            "bead_id={BEAD_3WOP3_2} case=16t_admits"
        );
    }

    #[test]
    #[cfg_attr(
        debug_assertions,
        ignore = "production performance gate; replay with --profile release-perf"
    )]
    fn test_sharded_cache_throughput_vs_single() {
        // This is a production-profile gate, not a debug-build timing smoke
        // test. Warm both paths, alternate their order to cancel sequential
        // drift, and gate on the median paired ratio so one scheduler outlier
        // cannot make an otherwise healthy full-workspace run fail.
        const ITERATIONS: u32 = 10_000;
        const SAMPLE_COUNT: usize = 11;

        let _ = measure_single_cache_insert_read(ITERATIONS);
        let _ = measure_sharded_cache_insert_read(ITERATIONS);

        let mut evidence = Vec::with_capacity(SAMPLE_COUNT);
        let mut ratios = Vec::with_capacity(SAMPLE_COUNT);
        for sample in 0..SAMPLE_COUNT {
            let (single_elapsed, sharded_elapsed, order) = if sample % 2 == 0 {
                (
                    measure_single_cache_insert_read(ITERATIONS),
                    measure_sharded_cache_insert_read(ITERATIONS),
                    "single_first",
                )
            } else {
                let sharded_elapsed = measure_sharded_cache_insert_read(ITERATIONS);
                let single_elapsed = measure_single_cache_insert_read(ITERATIONS);
                (single_elapsed, sharded_elapsed, "sharded_first")
            };
            let ratio =
                sharded_elapsed.as_secs_f64() / single_elapsed.as_secs_f64().max(f64::MIN_POSITIVE);
            evidence.push((sample, order, single_elapsed, sharded_elapsed, ratio));
            ratios.push(ratio);
        }
        let ratio_median = median_ratio(&mut ratios);

        for (sample, order, single_elapsed, sharded_elapsed, ratio) in &evidence {
            eprintln!(
                "bead_id={BEAD_3WOP3_2} sample={sample} order={order} \
                 throughput_ratio={ratio:.2}x single={single_elapsed:?} \
                 sharded={sharded_elapsed:?}"
            );
        }
        eprintln!(
            "bead_id={BEAD_3WOP3_2} median_throughput_ratio={ratio_median:.2}x \
             samples={SAMPLE_COUNT} iterations={ITERATIONS} replay={SHARDED_CACHE_PERF_REPLAY}"
        );

        // Preserve the original production budget: locking and callback
        // indirection may cost at most 3x the non-sharded path.
        assert!(
            ratio_median < 3.0,
            "bead_id={BEAD_3WOP3_2} case=throughput_overhead \
             sharded cache median is {ratio_median:.2}x slower than single \
             (max 3x allowed); evidence={evidence:?}; replay={SHARDED_CACHE_PERF_REPLAY}"
        );
    }

    #[test]
    fn test_sharded_cache_concurrent_same_shard() {
        // Multiple threads hitting the same shard should work correctly.
        use std::sync::Arc;
        use std::thread;

        let cache = Arc::new(ShardedPageCache::new(PageSize::DEFAULT));
        let num_threads = 4;
        let ops_per_thread = 500;

        // All threads use page numbers that hash to the same shard
        // We find pages with the same shard index
        let base_page = PageNumber::ONE;
        let base_shard = cache.shard_index(base_page);

        // Find other pages in the same shard
        let mut same_shard_pages = vec![1u32];
        for i in 2..10000u32 {
            let pn = PageNumber::new(i).unwrap();
            if cache.shard_index(pn) == base_shard {
                same_shard_pages.push(i);
                if same_shard_pages.len() >= (num_threads * ops_per_thread) {
                    break;
                }
            }
        }

        let pages = Arc::new(same_shard_pages);

        let handles: Vec<_> = (0..num_threads)
            .map(|tid| {
                let c = Arc::clone(&cache);
                let p = Arc::clone(&pages);
                thread::spawn(move || {
                    let start = tid * ops_per_thread;
                    for i in 0..ops_per_thread {
                        let idx = start + i;
                        if idx >= p.len() {
                            break;
                        }
                        let pn = PageNumber::new(p[idx]).unwrap();

                        c.insert_fresh(pn, |data| data[0] = (tid & 0xFF) as u8)
                            .unwrap();
                        c.with_page(pn, |_| {});
                    }
                })
            })
            .collect();

        for h in handles {
            h.join()
                .expect("bead_id={BEAD_3WOP3_2} case=concurrent_same_shard panic");
        }

        // Verify we inserted to the expected shard
        let dist = cache.shard_distribution();
        assert!(
            dist[base_shard] > 0,
            "bead_id={BEAD_3WOP3_2} case=same_shard_populated"
        );
    }

    #[test]
    fn test_sharded_cache_normalizes_configured_partition_count() {
        let cache = ShardedPageCache::with_max_buffers_and_shards(PageSize::DEFAULT, 64, 37);
        assert_eq!(
            cache.shard_count(),
            64,
            "bead_id={BEAD_3WOP3_2} case=normalize_to_next_power_of_two"
        );
        assert_eq!(cache.shard_distribution().len(), 64);

        let min_cache = ShardedPageCache::with_max_buffers_and_shards(PageSize::DEFAULT, 64, 1);
        assert_eq!(
            min_cache.shard_count(),
            2,
            "bead_id={BEAD_3WOP3_2} case=clamp_minimum_partitions"
        );

        let max_cache = ShardedPageCache::with_max_buffers_and_shards(PageSize::DEFAULT, 64, 8_192);
        assert_eq!(
            max_cache.shard_count(),
            1024,
            "bead_id={BEAD_3WOP3_2} case=clamp_maximum_partitions"
        );
    }

    // =========================================================================
    // FastPageArray (bd-fzr07) tests
    // =========================================================================

    const BEAD_FZR07: &str = "bd-fzr07";
    const BEAD_EORMS: &str = "bd-eorms";

    // --- FastPageArray unit tests ---

    #[test]
    fn test_fast_page_array_basic_insert_get() {
        let mut arr = FastPageArray::new();
        let pool = PageBufPool::new(PageSize::DEFAULT, 16);

        let p1 = PageNumber::ONE;
        let p2 = PageNumber::new(2).unwrap();
        let p10 = PageNumber::new(10).unwrap();

        // Insert page 1
        let mut buf1 = pool.acquire().unwrap();
        buf1.as_mut_slice().fill(0xAA);
        assert!(arr.insert(p1, buf1), "bead_id={BEAD_FZR07} case=insert_new");

        // Insert page 2
        let mut buf2 = pool.acquire().unwrap();
        buf2.as_mut_slice().fill(0xBB);
        assert!(
            arr.insert(p2, buf2),
            "bead_id={BEAD_FZR07} case=insert_new_2"
        );

        // Insert page 10 (sparse)
        let mut buf10 = pool.acquire().unwrap();
        buf10.as_mut_slice().fill(0xCC);
        assert!(
            arr.insert(p10, buf10),
            "bead_id={BEAD_FZR07} case=insert_sparse"
        );

        // Verify contents
        assert!(arr.contains(p1));
        assert!(arr.contains(p2));
        assert!(arr.contains(p10));
        assert!(!arr.contains(PageNumber::new(5).unwrap()));

        // Verify data
        let data1 = arr.get(p1).unwrap();
        assert!(
            data1.iter().all(|&b| b == 0xAA),
            "bead_id={BEAD_FZR07} case=get_data_1"
        );

        let data10 = arr.get(p10).unwrap();
        assert!(
            data10.iter().all(|&b| b == 0xCC),
            "bead_id={BEAD_FZR07} case=get_data_10"
        );
    }

    #[test]
    fn test_fast_page_array_get_mut() {
        let mut arr = FastPageArray::new();
        let pool = PageBufPool::new(PageSize::DEFAULT, 4);

        let p1 = PageNumber::ONE;
        let mut buf = pool.acquire().unwrap();
        buf.as_mut_slice().fill(0);
        arr.insert(p1, buf);

        // Mutate via get_mut
        let data = arr.get_mut(p1).unwrap();
        data[0] = 0x12;
        data[1] = 0x34;
        data[4095] = 0xFF;

        // Verify mutation persisted
        let read_back = arr.get(p1).unwrap();
        assert_eq!(read_back[0], 0x12, "bead_id={BEAD_FZR07} case=get_mut_0");
        assert_eq!(read_back[1], 0x34, "bead_id={BEAD_FZR07} case=get_mut_1");
        assert_eq!(
            read_back[4095], 0xFF,
            "bead_id={BEAD_FZR07} case=get_mut_4095"
        );
    }

    #[test]
    fn test_fast_page_array_remove() {
        let mut arr = FastPageArray::new();
        let pool = PageBufPool::new(PageSize::DEFAULT, 4);

        let p1 = PageNumber::ONE;
        let p2 = PageNumber::new(2).unwrap();

        arr.insert(p1, pool.acquire().unwrap());
        arr.insert(p2, pool.acquire().unwrap());

        assert_eq!(arr.len(), 2);

        // Remove existing page
        assert!(arr.remove(p1), "bead_id={BEAD_FZR07} case=remove_existing");
        assert!(!arr.contains(p1));
        assert!(arr.contains(p2));
        assert_eq!(arr.len(), 1);

        // Remove non-existing page
        assert!(
            !arr.remove(PageNumber::new(100).unwrap()),
            "bead_id={BEAD_FZR07} case=remove_nonexistent"
        );
    }

    #[test]
    fn test_fast_page_array_remove_any() {
        let mut arr = FastPageArray::new();
        let pool = PageBufPool::new(PageSize::DEFAULT, 8);

        // Insert pages 1, 5, 10
        for i in [1u32, 5, 10] {
            let pn = PageNumber::new(i).unwrap();
            arr.insert(pn, pool.acquire().unwrap());
        }

        assert_eq!(arr.len(), 3);

        // Remove any - should succeed 3 times
        let evicted1 = arr.remove_any();
        assert!(evicted1.is_some(), "bead_id={BEAD_FZR07} case=remove_any_1");

        let evicted2 = arr.remove_any();
        assert!(evicted2.is_some(), "bead_id={BEAD_FZR07} case=remove_any_2");

        let evicted3 = arr.remove_any();
        assert!(evicted3.is_some(), "bead_id={BEAD_FZR07} case=remove_any_3");

        // Now array is empty
        assert_eq!(arr.len(), 0);
        assert!(
            arr.remove_any().is_none(),
            "bead_id={BEAD_FZR07} case=remove_any_empty"
        );
    }

    #[test]
    fn test_fast_page_array_remove_any_honors_sparse_eviction_cursor() {
        let mut arr = FastPageArray::new();
        let pool = PageBufPool::new(PageSize::DEFAULT, 4);
        let low = PageNumber::ONE;
        let high = PageNumber::new(4096).unwrap();

        arr.insert(low, pool.acquire().unwrap());
        arr.insert(high, pool.acquire().unwrap());
        arr.next_eviction_scan_start = FastPageArray::pgno_to_idx(high);

        assert_eq!(
            arr.remove_any(),
            Some(high),
            "bead_id={BEAD_FZR07} case=cursor_prefers_high_sparse_slot"
        );
        assert_eq!(
            arr.next_eviction_scan_start, 0,
            "bead_id={BEAD_FZR07} case=cursor_wraps_after_high_slot"
        );
        assert_eq!(
            arr.remove_any(),
            Some(low),
            "bead_id={BEAD_FZR07} case=cursor_wraps_to_low_slot"
        );
        assert_eq!(
            arr.next_eviction_scan_start, 0,
            "bead_id={BEAD_FZR07} case=cursor_resets_when_empty"
        );
    }

    #[test]
    fn test_fast_page_array_clear() {
        let mut arr = FastPageArray::new();
        let pool = PageBufPool::new(PageSize::DEFAULT, 16);
        let sparse_page = PageNumber::new(4096).unwrap();

        for i in 1..=10u32 {
            arr.insert(PageNumber::new(i).unwrap(), pool.acquire().unwrap());
        }
        arr.insert(sparse_page, pool.acquire().unwrap());
        assert!(arr.remove(PageNumber::new(5).unwrap()));
        arr.insert(PageNumber::new(5).unwrap(), pool.acquire().unwrap());

        assert_eq!(arr.len(), 11);
        assert_eq!(
            arr.touched_words.len(),
            2,
            "bead_id={BEAD_FZR07} case=clear_tracks_sparse_words_once"
        );

        let removed = arr.clear();
        assert_eq!(removed, 11, "bead_id={BEAD_FZR07} case=clear_count");
        assert_eq!(arr.len(), 0);
        assert!(arr.touched_words.is_empty());
        assert!(arr.touched_slot_bits.iter().all(|&word| word == 0));
        assert!(!arr.contains(sparse_page));

        arr.insert(sparse_page, pool.acquire().unwrap());
        assert_eq!(arr.clear(), 1);
        assert!(arr.touched_words.is_empty());
    }

    #[test]
    fn test_fast_page_array_cold_reset_releases_sparse_index_storage() {
        let mut arr = FastPageArray::new();
        let pool = PageBufPool::new(PageSize::DEFAULT, 2);
        let sparse_page = PageNumber::new(4096).unwrap();

        arr.insert(sparse_page, pool.acquire().unwrap());
        assert!(
            arr.pages.len() > FAST_ARRAY_INITIAL_CAPACITY,
            "bead_id={BEAD_FZR07} case=sparse_insert_grows_backing_storage"
        );

        arr.cold_reset();

        assert_eq!(
            arr.len(),
            0,
            "bead_id={BEAD_FZR07} case=cold_reset_clears_entries"
        );
        assert_eq!(
            arr.pages.len(),
            0,
            "bead_id={BEAD_FZR07} case=cold_reset_releases_sparse_index_storage"
        );
        assert_eq!(
            arr.evictions, 1,
            "bead_id={BEAD_FZR07} case=cold_reset_tracks_eviction"
        );
    }

    #[test]
    fn test_fast_page_array_metrics() {
        let mut arr = FastPageArray::new();
        let pool = PageBufPool::new(PageSize::DEFAULT, 8);

        let p1 = PageNumber::ONE;
        let p2 = PageNumber::new(2).unwrap();

        // Insert (admits)
        arr.insert(p1, pool.acquire().unwrap());
        arr.insert(p2, pool.acquire().unwrap());
        assert_eq!(arr.admits, 2, "bead_id={BEAD_FZR07} case=metrics_admits");

        // Hits
        arr.get(p1);
        arr.get(p2);
        arr.get(p1);
        assert_eq!(arr.hits, 3, "bead_id={BEAD_FZR07} case=metrics_hits");

        // Misses
        arr.get(PageNumber::new(100).unwrap());
        arr.get(PageNumber::new(200).unwrap());
        assert_eq!(arr.misses, 2, "bead_id={BEAD_FZR07} case=metrics_misses");

        // Evictions
        arr.remove(p1);
        assert_eq!(
            arr.evictions, 1,
            "bead_id={BEAD_FZR07} case=metrics_evictions"
        );

        // Reset
        arr.reset_metrics();
        assert_eq!(arr.hits, 0, "bead_id={BEAD_FZR07} case=metrics_reset_hits");
        assert_eq!(
            arr.misses, 0,
            "bead_id={BEAD_FZR07} case=metrics_reset_misses"
        );
        assert_eq!(
            arr.admits, 0,
            "bead_id={BEAD_FZR07} case=metrics_reset_admits"
        );
        assert_eq!(
            arr.evictions, 0,
            "bead_id={BEAD_FZR07} case=metrics_reset_evictions"
        );
    }

    #[test]
    fn test_fast_page_array_capacity_growth() {
        let mut arr = FastPageArray::new();
        let pool = PageBufPool::new(PageSize::DEFAULT, 2048);

        // Insert page 2000 (sparse, requires growth)
        let p2000 = PageNumber::new(2000).unwrap();
        arr.insert(p2000, pool.acquire().unwrap());

        assert!(
            arr.pages.len() >= 2000,
            "bead_id={BEAD_FZR07} case=capacity_growth array grew to {}",
            arr.pages.len()
        );
        assert!(arr.contains(p2000));

        // Verify sparse pages are None
        assert!(!arr.contains(PageNumber::new(1000).unwrap()));
        assert!(!arr.contains(PageNumber::new(1999).unwrap()));
    }

    #[test]
    fn test_fast_page_array_overwrite() {
        let mut arr = FastPageArray::new();
        let pool = PageBufPool::new(PageSize::DEFAULT, 4);

        let p1 = PageNumber::ONE;

        // Insert first version
        let mut buf1 = pool.acquire().unwrap();
        buf1.as_mut_slice().fill(0xAA);
        assert!(arr.insert(p1, buf1));

        // Overwrite with second version
        let mut buf2 = pool.acquire().unwrap();
        buf2.as_mut_slice().fill(0xBB);
        assert!(
            !arr.insert(p1, buf2),
            "bead_id={BEAD_FZR07} case=overwrite_not_new"
        );

        // Verify overwritten data
        let data = arr.get(p1).unwrap();
        assert!(
            data.iter().all(|&b| b == 0xBB),
            "bead_id={BEAD_FZR07} case=overwrite_data"
        );
    }

    // --- ShardedPageCache fast path tests ---

    #[test]
    fn test_sharded_cache_new_single_connection() {
        let cache = ShardedPageCache::new_single_connection(PageSize::DEFAULT);

        assert!(
            cache.is_fast_path_enabled(),
            "bead_id={BEAD_FZR07} case=single_connection_enabled"
        );

        // Basic operations should work
        let p1 = PageNumber::ONE;
        cache.insert_fresh(p1, |data| data.fill(0xDD)).unwrap();

        assert!(cache.contains(p1));
        cache.with_page(p1, |data| {
            assert!(
                data.iter().all(|&b| b == 0xDD),
                "bead_id={BEAD_FZR07} case=single_connection_data"
            );
        });
    }

    #[test]
    fn test_sharded_cache_caps_eager_flat_slot_capacity() {
        let cache = ShardedPageCache::with_max_buffers(PageSize::DEFAULT, DEFAULT_PAGE_BUFFER_MAX);

        assert_eq!(
            cache.flat_slots.slots.len(),
            FLAT_SLOTS_TARGET_CAPACITY,
            "flat-slot front-cache should stay bounded even when the buffer pool ceiling is large"
        );
    }

    #[test]
    fn test_sharded_cache_initial_page_hint_keeps_tiny_db_open_small() {
        let cache = ShardedPageCache::with_max_buffers_for_initial_pages(
            PageSize::DEFAULT,
            DEFAULT_PAGE_BUFFER_MAX,
            1,
        );

        assert_eq!(
            cache.flat_slots.slots.len(),
            FLAT_SLOTS_MIN_CAPACITY,
            "tiny databases should not allocate the full flat-slot target on open"
        );
    }

    #[test]
    fn test_sharded_cache_initial_page_hint_scales_large_db_to_target() {
        let cache = ShardedPageCache::with_max_buffers_for_initial_pages(
            PageSize::DEFAULT,
            DEFAULT_PAGE_BUFFER_MAX,
            u32::try_from(FLAT_SLOTS_TARGET_CAPACITY).unwrap_or(u32::MAX),
        );

        assert_eq!(
            cache.flat_slots.slots.len(),
            FLAT_SLOTS_TARGET_CAPACITY,
            "large databases should still get the full lock-free front-cache cap"
        );
    }

    #[test]
    fn test_sharded_cache_enable_disable_fast_path() {
        let mut cache = ShardedPageCache::new(PageSize::DEFAULT);

        // Initially disabled
        assert!(
            !cache.is_fast_path_enabled(),
            "bead_id={BEAD_FZR07} case=initially_disabled"
        );

        // Enable fast path
        cache.enable_fast_path();
        assert!(
            cache.is_fast_path_enabled(),
            "bead_id={BEAD_FZR07} case=enabled"
        );

        // Insert some data while in fast path mode
        let p1 = PageNumber::ONE;
        cache.insert_fresh(p1, |data| data[0] = 0xEE).unwrap();
        assert_eq!(
            cache.fast_array.as_ref().unwrap().lock().len(),
            1,
            "bead_id={BEAD_FZR07} case=fast_array_contains_inserted_page_before_disable"
        );

        // Disable fast path
        cache.disable_fast_path();
        assert!(
            !cache.is_fast_path_enabled(),
            "bead_id={BEAD_FZR07} case=disabled"
        );
        assert_eq!(
            cache.fast_array.as_ref().unwrap().lock().len(),
            0,
            "bead_id={BEAD_FZR07} case=disable_clears_hidden_fast_array_pages"
        );

        // Operations now go through the sharded path with a cold cache.
        assert!(
            !cache.contains(p1),
            "bead_id={BEAD_FZR07} case=disabled_cold_reset_no_hidden_fast_page"
        );
        assert_eq!(
            cache.len(),
            0,
            "bead_id={BEAD_FZR07} case=disabled_len_reset"
        );

        cache.enable_fast_path();
        assert!(
            cache.is_fast_path_enabled(),
            "bead_id={BEAD_FZR07} case=re_enabled"
        );
        assert!(
            !cache.contains(p1),
            "bead_id={BEAD_FZR07} case=re_enabled_cold_resets_fast_array_visibility"
        );
        assert_eq!(
            cache.len(),
            0,
            "bead_id={BEAD_FZR07} case=re_enabled_len_reset"
        );
    }

    #[test]
    fn test_disable_fast_path_is_idempotent_for_sharded_cache() {
        let mut cache = ShardedPageCache::new(PageSize::DEFAULT);
        let p1 = PageNumber::ONE;

        cache.insert_fresh(p1, |data| data[0] = 0x5A).unwrap();
        assert!(
            cache.contains(p1),
            "bead_id={BEAD_FZR07} case=pre_disable_sharded_page_visible"
        );

        cache.disable_fast_path();

        assert!(
            !cache.is_fast_path_enabled(),
            "bead_id={BEAD_FZR07} case=disable_idempotent_stays_off"
        );
        assert!(
            cache.contains(p1),
            "bead_id={BEAD_FZR07} case=disable_idempotent_preserves_sharded_page"
        );
        assert_eq!(
            cache.len(),
            1,
            "bead_id={BEAD_FZR07} case=disable_idempotent_preserves_sharded_len"
        );
    }

    #[test]
    fn test_disable_fast_path_releases_sparse_fast_array_storage() {
        let mut cache = ShardedPageCache::new(PageSize::DEFAULT);
        let sparse_page = PageNumber::new(4096).unwrap();

        cache.enable_fast_path();
        cache
            .insert_fresh(sparse_page, |data| data[0] = 0xA5)
            .unwrap();
        assert!(
            cache.fast_array.as_ref().unwrap().lock().pages.len() > FAST_ARRAY_INITIAL_CAPACITY,
            "bead_id={BEAD_FZR07} case=sparse_fast_insert_grows_backing_storage"
        );

        cache.disable_fast_path();

        assert_eq!(
            cache.fast_array.as_ref().unwrap().lock().pages.len(),
            0,
            "bead_id={BEAD_FZR07} case=disable_releases_sparse_fast_storage"
        );
        assert_eq!(
            cache.fast_array.as_ref().unwrap().lock().len(),
            0,
            "bead_id={BEAD_FZR07} case=disable_releases_sparse_fast_entries"
        );
    }

    #[test]
    fn test_sharded_cache_clear_preserves_sparse_fast_path_storage() {
        let mut cache = ShardedPageCache::new(PageSize::DEFAULT);
        let sparse_page = PageNumber::new(4096).unwrap();

        cache.enable_fast_path();
        cache
            .insert_fresh(sparse_page, |data| data[0] = 0x7C)
            .unwrap();
        let pages_len_before_clear = cache.fast_array.as_ref().unwrap().lock().pages.len();
        assert!(
            pages_len_before_clear > FAST_ARRAY_INITIAL_CAPACITY,
            "bead_id={BEAD_FZR07} case=clear_sparse_fast_insert_grows_backing_storage"
        );

        cache.clear();

        assert_eq!(
            cache.fast_array.as_ref().unwrap().lock().pages.len(),
            pages_len_before_clear,
            "bead_id={BEAD_FZR07} case=clear_preserves_sparse_fast_storage"
        );
        assert_eq!(
            cache.fast_array.as_ref().unwrap().lock().len(),
            0,
            "bead_id={BEAD_FZR07} case=clear_releases_sparse_fast_entries"
        );
    }

    #[test]
    fn test_enable_fast_path_clears_now_inactive_sharded_tiers() {
        let mut cache = ShardedPageCache::new(PageSize::DEFAULT);
        let p1 = PageNumber::ONE;

        cache.insert_fresh(p1, |data| data[0] = 0xCD).unwrap();
        assert!(
            cache.contains(p1),
            "bead_id={BEAD_FZR07} case=pre_enable_contains"
        );

        cache.enable_fast_path();
        assert!(
            cache.is_empty(),
            "bead_id={BEAD_FZR07} case=enable_clears_inactive_sharded_tiers"
        );

        cache.disable_fast_path();
        assert!(
            !cache.contains(p1),
            "bead_id={BEAD_FZR07} case=disabled_does_not_resurrect_old_sharded_pages"
        );
        assert_eq!(
            cache.len(),
            0,
            "bead_id={BEAD_FZR07} case=enable_then_disable_keeps_old_sharded_tiers_cleared"
        );
    }

    #[test]
    fn test_enable_fast_path_drops_stale_fast_array_pages() {
        let mut cache = ShardedPageCache::new(PageSize::DEFAULT);
        let p1 = PageNumber::ONE;

        cache.enable_fast_path();
        cache.insert_fresh(p1, |data| data[0] = 0x11).unwrap();
        cache.disable_fast_path();

        cache.insert_fresh(p1, |data| data[0] = 0x22).unwrap();
        assert!(
            cache.contains(p1),
            "bead_id={BEAD_FZR07} case=sharded_write_visible"
        );

        cache.enable_fast_path();
        assert!(
            !cache.contains(p1),
            "bead_id={BEAD_FZR07} case=re_enable_discards_old_fast_array_state"
        );
        assert_eq!(
            cache.len(),
            0,
            "bead_id={BEAD_FZR07} case=re_enable_after_sharded_write_is_cold_reset"
        );
    }

    #[test]
    fn test_sharded_cache_clear_removes_inactive_fast_path_pages() {
        let mut cache = ShardedPageCache::new(PageSize::DEFAULT);
        cache.enable_fast_path();

        let p1 = PageNumber::ONE;
        cache.insert_fresh(p1, |data| data[0] = 0xAB).unwrap();
        cache.disable_fast_path();

        cache.clear();
        cache.enable_fast_path();

        assert!(
            !cache.contains(p1),
            "bead_id={BEAD_FZR07} case=clear_removes_hidden_fast_array_pages"
        );
        assert_eq!(
            cache.len(),
            0,
            "bead_id={BEAD_FZR07} case=clear_resets_all_tiers"
        );
    }

    #[test]
    fn test_sharded_cache_fast_path_basic_operations() {
        let cache = ShardedPageCache::new_single_connection(PageSize::DEFAULT);

        let p1 = PageNumber::ONE;
        let p2 = PageNumber::new(2).unwrap();
        let p100 = PageNumber::new(100).unwrap();

        // Insert
        cache.insert_fresh(p1, |data| data.fill(0x11)).unwrap();
        cache.insert_fresh(p2, |data| data.fill(0x22)).unwrap();
        cache.insert_fresh(p100, |data| data.fill(0x99)).unwrap();

        assert_eq!(cache.len(), 3, "bead_id={BEAD_FZR07} case=fp_len");

        // Contains
        assert!(cache.contains(p1));
        assert!(cache.contains(p100));
        assert!(!cache.contains(PageNumber::new(50).unwrap()));

        // Get
        let v1 = cache.get(p1).unwrap();
        assert!(
            v1.iter().all(|&b| b == 0x11),
            "bead_id={BEAD_FZR07} case=fp_get_1"
        );

        // With_page
        cache.with_page(p100, |data| {
            assert!(
                data.iter().all(|&b| b == 0x99),
                "bead_id={BEAD_FZR07} case=fp_with_page"
            );
        });

        // With_page_mut
        cache.with_page_mut(p2, |data| {
            data[0] = 0xFF;
        });
        cache.with_page(p2, |data| {
            assert_eq!(data[0], 0xFF, "bead_id={BEAD_FZR07} case=fp_with_page_mut");
        });

        // Evict
        assert!(cache.evict(p1));
        assert!(!cache.contains(p1));
        assert_eq!(cache.len(), 2);

        // Evict_any
        assert!(cache.evict_any());
        assert_eq!(cache.len(), 1);

        // Clear
        cache.clear();
        assert!(cache.is_empty());
    }

    #[test]
    fn test_sharded_cache_fast_path_metrics() {
        let cache = ShardedPageCache::new_single_connection(PageSize::DEFAULT);

        let p1 = PageNumber::ONE;
        let p2 = PageNumber::new(2).unwrap();

        // Insert (admits)
        cache.insert_fresh(p1, |_| {}).unwrap();
        cache.insert_fresh(p2, |_| {}).unwrap();

        // Hits
        cache.with_page(p1, |_| {});
        cache.with_page(p2, |_| {});
        cache.with_page(p1, |_| {});

        // Misses
        cache.with_page(PageNumber::new(100).unwrap(), |_| {});

        // Evictions
        cache.evict(p1);

        let m = cache.metrics_snapshot();
        assert_eq!(m.admits, 2, "bead_id={BEAD_FZR07} case=fp_metrics_admits");
        assert_eq!(m.hits, 3, "bead_id={BEAD_FZR07} case=fp_metrics_hits");
        assert_eq!(m.misses, 1, "bead_id={BEAD_FZR07} case=fp_metrics_misses");
        assert_eq!(
            m.evictions, 1,
            "bead_id={BEAD_FZR07} case=fp_metrics_evictions"
        );
        assert_eq!(
            m.cached_pages, 1,
            "bead_id={BEAD_FZR07} case=fp_metrics_cached"
        );

        cache.reset_metrics();
        let reset = cache.metrics_snapshot();
        assert_eq!(reset.hits, 0, "bead_id={BEAD_FZR07} case=fp_metrics_reset");
        assert_eq!(reset.misses, 0);
        assert_eq!(reset.admits, 0);
        assert_eq!(reset.evictions, 0);
        // cached_pages preserved
        assert_eq!(reset.cached_pages, 1);
    }

    #[test]
    fn test_sharded_cache_fast_path_vfs_roundtrip() {
        let (cx, mut file) = setup();

        // Write test data to VFS
        let test_data = vec![0xAB_u8; 4096];
        file.write(&cx, &test_data, 0).unwrap();

        let cache = ShardedPageCache::new_single_connection(PageSize::DEFAULT);
        let p1 = PageNumber::ONE;

        // Read through fast path
        let result = cache.read_page(&cx, &mut file, p1, |data| {
            assert_eq!(
                data,
                test_data.as_slice(),
                "bead_id={BEAD_FZR07} case=fp_vfs_read"
            );
            data[0]
        });
        assert_eq!(result.unwrap(), 0xAB);

        // Modify and write back
        cache.with_page_mut(p1, |data| data[0] = 0xCD);
        cache.write_page(&cx, &mut file, p1).unwrap();

        // Verify write
        let mut verify = vec![0u8; 4096];
        file.read(&cx, &mut verify, 0).unwrap();
        assert_eq!(verify[0], 0xCD, "bead_id={BEAD_FZR07} case=fp_vfs_write");
    }

    #[test]
    fn test_sharded_cache_fast_path_insert_buffer() {
        let cache = ShardedPageCache::new_single_connection(PageSize::DEFAULT);
        let p1 = PageNumber::ONE;

        // Acquire buffer from pool and fill it
        let mut buf = cache.pool().acquire().unwrap();
        buf.as_mut_slice().fill(0xFE);

        // Insert via fast path
        cache.insert_buffer(p1, buf);

        assert!(cache.contains(p1));
        cache.with_page(p1, |data| {
            assert!(
                data.iter().all(|&b| b == 0xFE),
                "bead_id={BEAD_FZR07} case=fp_insert_buffer"
            );
        });
    }

    #[test]
    fn test_sharded_cache_fast_path_get_copy() {
        let cache = ShardedPageCache::new_single_connection(PageSize::DEFAULT);
        let p1 = PageNumber::ONE;

        cache.insert_fresh(p1, |data| data.fill(0x77)).unwrap();

        let copy = cache.get_copy(p1);
        assert!(copy.is_some());
        let data = copy.unwrap();
        assert!(
            data.iter().all(|&b| b == 0x77),
            "bead_id={BEAD_FZR07} case=fp_get_copy"
        );
        assert_eq!(data.len(), 4096);
    }

    #[test]
    fn test_sharded_cache_fast_path_get_shared_reuses_snapshot_until_mutation() {
        let cache = ShardedPageCache::new_single_connection(PageSize::DEFAULT);
        let p1 = PageNumber::ONE;

        cache.insert_fresh(p1, |data| data.fill(0x77)).unwrap();

        let first = cache.get_shared(p1).unwrap();
        let second = cache.get_shared(p1).unwrap();

        assert_eq!(
            first.as_bytes().as_ptr(),
            second.as_bytes().as_ptr(),
            "bead_id={BEAD_FZR07} case=fp_get_shared_reuses_snapshot"
        );

        cache.with_page_mut(p1, |data| data[0] = 0x11).unwrap();

        let refreshed = cache.get_shared(p1).unwrap();
        assert_eq!(
            refreshed.as_bytes()[0],
            0x11,
            "bead_id={BEAD_FZR07} case=fp_get_shared_refreshes_after_mutation"
        );
        assert_ne!(
            second.as_bytes().as_ptr(),
            refreshed.as_bytes().as_ptr(),
            "bead_id={BEAD_FZR07} case=fp_get_shared_invalidates_stale_snapshot"
        );
    }

    #[test]
    fn test_sharded_cache_flat_slots_get_shared_reuses_snapshot_until_mutation() {
        let cache = ShardedPageCache::new(PageSize::DEFAULT);
        let p1 = PageNumber::ONE;

        cache.insert_fresh(p1, |data| data.fill(0x5A)).unwrap();

        let first = cache.get_shared(p1).unwrap();
        let second = cache.get_shared(p1).unwrap();

        assert_eq!(
            first.as_bytes().as_ptr(),
            second.as_bytes().as_ptr(),
            "bead_id={BEAD_EORMS} case=flat_get_shared_reuses_snapshot"
        );

        cache.with_page_mut(p1, |data| data[0] = 0x22).unwrap();

        let refreshed = cache.get_shared(p1).unwrap();
        assert_eq!(
            refreshed.as_bytes()[0],
            0x22,
            "bead_id={BEAD_EORMS} case=flat_get_shared_refreshes_after_mutation"
        );
        assert_ne!(
            second.as_bytes().as_ptr(),
            refreshed.as_bytes().as_ptr(),
            "bead_id={BEAD_EORMS} case=flat_get_shared_invalidates_stale_snapshot"
        );
    }

    #[test]
    fn test_sharded_cache_fast_path_read_page_copy() {
        let (cx, mut file) = setup();

        let test_data = vec![0x88_u8; 4096];
        file.write(&cx, &test_data, 0).unwrap();

        let cache = ShardedPageCache::new_single_connection(PageSize::DEFAULT);
        let p1 = PageNumber::ONE;

        let copy = cache.read_page_copy(&cx, &mut file, p1).unwrap();
        assert!(
            copy.iter().all(|&b| b == 0x88),
            "bead_id={BEAD_FZR07} case=fp_read_page_copy"
        );
    }

    #[test]
    fn test_track_f_flat_slots_lock_free_read_correctness() {
        use std::sync::{Arc, Barrier};
        use std::thread;

        const THREADS: usize = 8;
        const ITERATIONS_PER_THREAD: usize = 2_048;
        const HOT_PAGES: usize = 16;

        let slots = Arc::new(FlatPageSlots::new(FLAT_SLOTS_MIN_CAPACITY));
        for page_idx in 0..HOT_PAGES {
            let page_no = PageNumber::new(u32::try_from(page_idx + 1).expect("page idx fits"))
                .expect("hot page number");
            let pattern = page_pattern(page_no);
            let mut buf = PageBuf::new(PageSize::DEFAULT);
            buf.as_mut_slice().fill(pattern);
            let inserted = slots
                .try_insert(page_no, buf)
                .expect("hot page should stay in flat slots");
            assert!(inserted, "hot page should be newly inserted");
        }

        let start_barrier = Arc::new(Barrier::new(THREADS + 1));
        let started = Instant::now();
        let handles: Vec<_> = (0..THREADS)
            .map(|thread_idx| {
                let slots = Arc::clone(&slots);
                let start_barrier = Arc::clone(&start_barrier);
                thread::spawn(move || {
                    start_barrier.wait();
                    for iter in 0..ITERATIONS_PER_THREAD {
                        let hot_page_idx = (thread_idx + iter) % HOT_PAGES;
                        let hot_page = PageNumber::new(
                            u32::try_from(hot_page_idx + 1).expect("hot page idx fits"),
                        )
                        .expect("hot page");
                        let expected = page_pattern(hot_page);
                        let page = slots.get_copy(hot_page).expect("hot page must exist");
                        assert_eq!(
                            page[0],
                            expected,
                            "TRACK_F hot read returned wrong prefix byte for page {}",
                            hot_page.get()
                        );
                        assert_eq!(
                            page[PageSize::DEFAULT.as_usize() - 1],
                            expected,
                            "TRACK_F hot read returned wrong tail byte for page {}",
                            hot_page.get()
                        );

                        let cold_page = PageNumber::new(
                            10_000
                                + u32::try_from((thread_idx * HOT_PAGES) + hot_page_idx)
                                    .expect("cold page idx fits"),
                        )
                        .expect("cold page");
                        assert!(
                            slots.get_copy(cold_page).is_none(),
                            "cold page {} should miss the flat slots table",
                            cold_page.get()
                        );
                    }
                })
            })
            .collect();

        start_barrier.wait();
        for handle in handles {
            handle
                .join()
                .expect("flat-slot concurrent reader must not panic");
        }
        let elapsed = started.elapsed();

        let expected_hits = u64::try_from(THREADS * ITERATIONS_PER_THREAD).expect("hit count fits");
        assert_eq!(
            slots.hits.load(Ordering::Relaxed),
            expected_hits,
            "flat-slot concurrent hit count should match the hot-read workload"
        );
        assert_eq!(
            slots.len(),
            HOT_PAGES,
            "flat-slot table should keep every hot page resident"
        );

        emit_track_f_log(
            "test_track_f_flat_slots_lock_free_read_correctness",
            "verify",
            elapsed,
            HOT_PAGES,
            expected_hits,
            expected_hits,
            expected_hits,
            json!({
                "threads": THREADS,
                "iterations_per_thread": ITERATIONS_PER_THREAD,
                "resident_pages": slots.len(),
                "observed_misses": expected_hits
            }),
        );
    }

    #[test]
    fn test_track_f_page_cache_latency_microbenchmark_under_load() {
        use std::sync::{Arc, Barrier};
        use std::thread;

        const HOT_PAGES: usize = 32;
        const BACKGROUND_THREADS: usize = 4;
        const BACKGROUND_READS_PER_THREAD: usize = 4_096;
        const SAMPLES: usize = 2_048;

        let cache = Arc::new(ShardedPageCache::new(PageSize::DEFAULT));
        for page_idx in 0..HOT_PAGES {
            let page_no = PageNumber::new(u32::try_from(page_idx + 1).expect("page idx fits"))
                .expect("hot page");
            let pattern = page_pattern(page_no);
            cache
                .insert_fresh(page_no, |data| data.fill(pattern))
                .expect("hot page insert should succeed");
        }

        let before = cache.metrics_snapshot();
        let start_barrier = Arc::new(Barrier::new(BACKGROUND_THREADS + 1));
        let workers: Vec<_> = (0..BACKGROUND_THREADS)
            .map(|thread_idx| {
                let cache = Arc::clone(&cache);
                let start_barrier = Arc::clone(&start_barrier);
                thread::spawn(move || {
                    start_barrier.wait();
                    for iter in 0..BACKGROUND_READS_PER_THREAD {
                        let page_idx =
                            (thread_idx * BACKGROUND_READS_PER_THREAD + iter) % HOT_PAGES;
                        let page_no =
                            PageNumber::new(u32::try_from(page_idx + 1).expect("page idx fits"))
                                .expect("background hot page");
                        let page = cache
                            .get_copy(page_no)
                            .expect("background hot page should stay cached");
                        black_box(page[0]);
                    }
                })
            })
            .collect();

        start_barrier.wait();
        let started = Instant::now();
        let mut latencies = Vec::with_capacity(SAMPLES);
        for sample_idx in 0..SAMPLES {
            let page_no = PageNumber::new(
                u32::try_from((sample_idx % HOT_PAGES) + 1).expect("page idx fits"),
            )
            .expect("sample page");
            let read_started = Instant::now();
            let page = cache
                .get_copy(page_no)
                .expect("latency sample page should stay cached");
            latencies.push(elapsed_ns(read_started.elapsed()));
            black_box(page[0]);
        }
        for worker in workers {
            worker
                .join()
                .expect("background latency worker must not panic");
        }
        let elapsed = started.elapsed();

        let after = cache.metrics_snapshot();
        let hit_delta = after.hits.saturating_sub(before.hits);
        let miss_delta = after.misses.saturating_sub(before.misses);
        let p50 = percentile_u64(&latencies, 50);
        let p95 = percentile_u64(&latencies, 95);
        let p99 = percentile_u64(&latencies, 99);

        assert_eq!(
            latencies.len(),
            SAMPLES,
            "latency run should keep all samples"
        );
        assert!(
            p50 <= p95 && p95 <= p99,
            "latency percentiles must be monotonic: p50={p50} p95={p95} p99={p99}"
        );
        assert_eq!(
            miss_delta, 0,
            "hot-read latency microbenchmark should not miss the page cache"
        );
        assert!(
            hit_delta
                >= u64::try_from(SAMPLES + BACKGROUND_THREADS * BACKGROUND_READS_PER_THREAD)
                    .expect("read count fits"),
            "latency microbenchmark should record every hot read as a cache hit"
        );

        emit_track_f_log(
            "test_track_f_page_cache_latency_microbenchmark_under_load",
            "verify",
            elapsed,
            HOT_PAGES,
            hit_delta,
            hit_delta,
            miss_delta,
            json!({
                "samples": SAMPLES,
                "background_threads": BACKGROUND_THREADS,
                "background_reads_per_thread": BACKGROUND_READS_PER_THREAD,
                "p50_ns": p50,
                "p95_ns": p95,
                "p99_ns": p99
            }),
        );
    }

    #[test]
    fn test_track_f_page_cache_stress_concurrent_access() {
        use std::sync::{Arc, Barrier};
        use std::thread;

        const THREADS: usize = 8;
        const SHARED_PAGES: usize = 6;
        const ITERATIONS_PER_THREAD: usize = 512;

        let cache = Arc::new(ShardedPageCache::new(PageSize::DEFAULT));
        for page_idx in 0..SHARED_PAGES {
            let page_no = PageNumber::new(u32::try_from(page_idx + 1).expect("page idx fits"))
                .expect("shared page");
            cache
                .insert_fresh(page_no, |data| data.fill(0))
                .expect("shared page insert should succeed");
        }

        let before = cache.metrics_snapshot();
        let start_barrier = Arc::new(Barrier::new(THREADS + 1));
        let handles: Vec<_> = (0..THREADS)
            .map(|thread_idx| {
                let cache = Arc::clone(&cache);
                let start_barrier = Arc::clone(&start_barrier);
                thread::spawn(move || {
                    start_barrier.wait();
                    for iter in 0..ITERATIONS_PER_THREAD {
                        for page_idx in 0..SHARED_PAGES {
                            let page_no = PageNumber::new(
                                u32::try_from(page_idx + 1).expect("page idx fits"),
                            )
                            .expect("shared page");
                            cache
                                .with_page_mut(page_no, |data| {
                                    let next = lane_counter(data, thread_idx).saturating_add(1);
                                    set_lane_counter(data, thread_idx, next);
                                    data[256 + thread_idx] =
                                        u8::try_from((iter + page_idx) & 0xFF).expect("byte fits");
                                })
                                .expect("shared page must stay resident");

                            if page_idx % 2 == 0 {
                                let snapshot = cache
                                    .get_copy(page_no)
                                    .expect("shared page copy should succeed");
                                assert!(
                                    lane_counter(&snapshot, thread_idx) >= 1,
                                    "stress reader should observe at least one write on lane {thread_idx}"
                                );
                            }
                        }
                    }
                })
            })
            .collect();

        start_barrier.wait();
        let started = Instant::now();
        for handle in handles {
            handle
                .join()
                .expect("page-cache stress worker must not panic");
        }
        let elapsed = started.elapsed();

        for page_idx in 0..SHARED_PAGES {
            let page_no = PageNumber::new(u32::try_from(page_idx + 1).expect("page idx fits"))
                .expect("shared page");
            let page = cache
                .get_copy(page_no)
                .expect("shared page should remain cached after stress");
            for thread_idx in 0..THREADS {
                assert_eq!(
                    lane_counter(&page, thread_idx),
                    u32::try_from(ITERATIONS_PER_THREAD).expect("iteration count fits"),
                    "shared page {} lane {} lost writes under concurrent access",
                    page_no.get(),
                    thread_idx
                );
            }
        }

        let after = cache.metrics_snapshot();
        let hit_delta = after.hits.saturating_sub(before.hits);
        let miss_delta = after.misses.saturating_sub(before.misses);
        let mutation_ops =
            u64::try_from(THREADS * SHARED_PAGES * ITERATIONS_PER_THREAD).expect("ops fit");
        let read_ops = u64::try_from(THREADS * SHARED_PAGES.div_ceil(2) * ITERATIONS_PER_THREAD)
            .expect("ops fit");

        assert_eq!(
            miss_delta, 0,
            "stress workload should operate entirely on hot pages"
        );
        assert!(
            hit_delta >= mutation_ops.saturating_add(read_ops),
            "stress workload should account for every shared-page probe as a cache hit"
        );

        emit_track_f_log(
            "test_track_f_page_cache_stress_concurrent_access",
            "verify",
            elapsed,
            SHARED_PAGES,
            mutation_ops.saturating_add(read_ops),
            hit_delta,
            miss_delta,
            json!({
                "threads": THREADS,
                "iterations_per_thread": ITERATIONS_PER_THREAD,
                "shared_pages": SHARED_PAGES,
                "mutation_ops": mutation_ops,
                "read_ops": read_ops
            }),
        );
    }

    #[test]
    fn test_track_q_flat_hash_basic_insert_get_on_100_pages() {
        let slots = FlatPageSlots::new(128);

        for raw_pgno in 1_u32..=100 {
            let page_no = PageNumber::new(raw_pgno).expect("page number");
            let inserted = slots
                .try_insert(page_no, track_q_page_buf(page_no))
                .expect("basic insert should stay in flat slots");
            assert!(inserted, "page {} should be newly inserted", page_no.get());
        }

        assert_eq!(slots.len(), 100, "all basic pages should remain resident");
        slots.reset_metrics();

        let started = Instant::now();
        let mut bucket_access_count = 0_u64;
        let mut chain_walk_count = 0_u64;
        for raw_pgno in 1_u32..=100 {
            let page_no = PageNumber::new(raw_pgno).expect("page number");
            assert!(
                slots.contains(page_no),
                "page {} should be present",
                page_no.get()
            );
            let copy = slots
                .get_copy(page_no)
                .expect("inserted page should round-trip through flat slots");
            assert_track_q_page(page_no, &copy);
            let distance = u64::try_from(track_q_probe_distance(&slots, page_no))
                .expect("probe distance fits u64");
            bucket_access_count = bucket_access_count.saturating_add(distance.saturating_add(1));
            chain_walk_count = chain_walk_count.saturating_add(distance);
        }
        let elapsed = started.elapsed();

        let hits = slots.hits.load(Ordering::Relaxed);
        let misses = slots.misses.load(Ordering::Relaxed);
        assert_eq!(hits, 100, "basic readback should record one hit per page");
        assert_eq!(misses, 0, "basic readback should not record misses");

        emit_track_q_log(
            "test_track_q_flat_hash_basic_insert_get_on_100_pages",
            "verify",
            elapsed,
            100,
            bucket_access_count,
            chain_walk_count,
            0,
            track_q_hit_rate(hits, misses),
            json!({
                "resident_pages": slots.len(),
                "admits": slots.admits.load(Ordering::Relaxed),
                "capacity": slots.mask + 1
            }),
        );
    }

    #[test]
    fn test_track_q_flat_hash_reinsert_updates_existing_slot_in_place() {
        let slots = FlatPageSlots::new(64);
        let page_no = PageNumber::new(42).expect("page number");
        let original_slot = slots.hash_pgno(page_no.get());

        let inserted = slots
            .try_insert(page_no, track_q_page_buf(page_no))
            .expect("initial page insert should stay in flat slots");
        assert!(inserted, "initial page should be newly inserted");
        assert_eq!(
            slots.find_slot(page_no),
            Some(original_slot),
            "initial page should occupy its home bucket"
        );
        assert_eq!(
            slots.len(),
            1,
            "initial insert should add one resident page"
        );
        assert_eq!(
            slots.admits.load(Ordering::Relaxed),
            1,
            "initial insert should count as one flat-slot admission"
        );

        let mut replacement = track_q_page_buf(page_no);
        replacement.as_mut_slice()[4] = 0xA5;
        replacement.as_mut_slice()[PageSize::DEFAULT.as_usize() - 1] = 0x5A;

        let inserted = slots
            .try_insert(page_no, replacement)
            .expect("reinserted page should update the resident flat slot");
        assert!(
            !inserted,
            "reinserting the same page should report an in-place update"
        );
        assert_eq!(
            slots.find_slot(page_no),
            Some(original_slot),
            "same-page update must not move the flat-slot entry"
        );
        assert_eq!(
            slots.len(),
            1,
            "same-page update must not grow flat-slot occupancy"
        );
        assert_eq!(
            slots.admits.load(Ordering::Relaxed),
            1,
            "same-page update must not double-count flat-slot admissions"
        );

        let copy = slots
            .get_copy(page_no)
            .expect("updated page should remain readable");
        assert_eq!(
            &copy[..4],
            &page_no.get().to_le_bytes(),
            "updated page should retain the page-number header"
        );
        assert_eq!(copy[4], 0xA5, "updated page body byte should be visible");
        assert_eq!(
            copy[PageSize::DEFAULT.as_usize() - 1],
            0x5A,
            "updated page tail byte should be visible"
        );
    }

    #[test]
    fn test_track_q_flat_hash_forced_probe_collision_chain() {
        let slots = FlatPageSlots::new(64);
        let target_bucket = slots.hash_pgno(PageNumber::ONE.get());
        let colliders = track_q_collision_pages(&slots, target_bucket, 8);

        for (expected_distance, page_no) in colliders.iter().copied().enumerate() {
            let inserted = slots
                .try_insert(page_no, track_q_page_buf(page_no))
                .expect("forced-collision page should stay in flat slots");
            assert!(
                inserted,
                "collider {} should be newly inserted",
                page_no.get()
            );
            assert_eq!(
                track_q_probe_distance(&slots, page_no),
                expected_distance,
                "collider {} should occupy the next probe slot in the chain",
                page_no.get()
            );
        }

        slots.reset_metrics();
        let started = Instant::now();
        let mut bucket_access_count = 0_u64;
        let mut chain_walk_count = 0_u64;
        for page_no in colliders.iter().copied() {
            let copy = slots
                .get_copy(page_no)
                .expect("collider should be retrievable from probe chain");
            assert_track_q_page(page_no, &copy);
            let distance = u64::try_from(track_q_probe_distance(&slots, page_no))
                .expect("probe distance fits u64");
            bucket_access_count = bucket_access_count.saturating_add(distance.saturating_add(1));
            chain_walk_count = chain_walk_count.saturating_add(distance);
        }
        let elapsed = started.elapsed();

        let absent = track_q_collision_pages(&slots, target_bucket, colliders.len() + 1)
            .last()
            .copied()
            .expect("absent collider");
        assert!(
            slots.get_copy(absent).is_none(),
            "non-inserted collider should miss after walking the probe chain"
        );

        let hits = slots.hits.load(Ordering::Relaxed);
        assert_eq!(
            hits,
            u64::try_from(colliders.len()).expect("collider hit count fits u64"),
            "forced collision test should record one hit per inserted collider"
        );
        assert!(
            chain_walk_count > 0,
            "forced collision test should accumulate non-zero probe-chain walks"
        );

        emit_track_q_log(
            "test_track_q_flat_hash_forced_probe_collision_chain",
            "verify",
            elapsed,
            colliders.len(),
            bucket_access_count,
            chain_walk_count,
            0,
            1.0,
            json!({
                "target_bucket": target_bucket,
                "max_probe_distance": colliders.len() - 1,
                "resident_pages": slots.len()
            }),
        );
    }

    #[test]
    fn test_track_q_flat_hash_capacity_growth_uses_overflow_shards_without_resize() {
        let cache = ShardedPageCache::new(PageSize::DEFAULT);
        let target_bucket = cache.flat_slots.hash_pgno(PageNumber::ONE.get());
        let colliders =
            track_q_collision_pages(&cache.flat_slots, target_bucket, MAX_PROBE_LENGTH + 4);

        for page_no in colliders.iter().copied() {
            cache.insert_buffer(page_no, track_q_page_buf(page_no));
        }

        let overflow_pages = cache
            .shards
            .iter()
            .map(|shard| shard.lock().len())
            .sum::<usize>();
        assert_eq!(
            cache.flat_slots.len(),
            MAX_PROBE_LENGTH,
            "flat slots should saturate exactly at the probe-window limit for one bucket"
        );
        assert_eq!(
            overflow_pages,
            colliders.len() - MAX_PROBE_LENGTH,
            "pages beyond the probe window should spill into overflow shards"
        );
        assert_eq!(
            cache.len(),
            colliders.len(),
            "composite cache should retain every page even when flat slots saturate"
        );

        cache.reset_metrics();
        let started = Instant::now();
        let mut bucket_access_count = 0_u64;
        let mut chain_walk_count = 0_u64;
        for page_no in colliders.iter().copied() {
            let copy = cache
                .get_copy(page_no)
                .expect("all saturated pages should remain readable through the composite cache");
            assert_track_q_page(page_no, &copy);
            if cache.flat_slots.contains(page_no) {
                let distance = u64::try_from(track_q_probe_distance(&cache.flat_slots, page_no))
                    .expect("probe distance fits u64");
                bucket_access_count =
                    bucket_access_count.saturating_add(distance.saturating_add(1));
                chain_walk_count = chain_walk_count.saturating_add(distance);
            }
        }
        let elapsed = started.elapsed();

        let metrics = cache.metrics_snapshot();
        assert_eq!(
            metrics.cached_pages,
            colliders.len(),
            "metrics snapshot should include flat-slot and overflow pages"
        );
        assert!(
            metrics.hits >= u64::try_from(colliders.len()).expect("collider count fits u64"),
            "composite cache lookups should register hits for saturated pages"
        );

        emit_track_q_log(
            "test_track_q_flat_hash_capacity_growth_uses_overflow_shards_without_resize",
            "verify",
            elapsed,
            colliders.len(),
            bucket_access_count,
            chain_walk_count,
            0,
            track_q_hit_rate(metrics.hits, metrics.misses),
            json!({
                "target_bucket": target_bucket,
                "flat_slot_pages": cache.flat_slots.len(),
                "overflow_pages": overflow_pages,
                "probe_window_limit": MAX_PROBE_LENGTH
            }),
        );
    }

    #[test]
    fn test_track_q_flat_hash_eviction_under_pool_pressure_stays_bounded() {
        const POOL_CAPACITY: usize = 4;
        const PAGE_COUNT: u32 = 12;

        let cache =
            ShardedPageCache::with_max_buffers_and_shards(PageSize::DEFAULT, POOL_CAPACITY, 1);
        let started = monotonic_test_clock();
        let mut eviction_attempts = 0_u64;

        for raw_pgno in 1..=PAGE_COUNT {
            let page_no = PageNumber::new(raw_pgno).expect("page number");
            loop {
                let result = cache.insert_fresh(page_no, |data| {
                    let page = track_q_page_buf(page_no);
                    data.copy_from_slice(page.as_slice());
                });
                match result {
                    Ok(()) => break,
                    Err(FrankenError::OutOfMemory) => {
                        eviction_attempts = eviction_attempts.saturating_add(1);
                        assert!(
                            cache.evict_any(),
                            "bounded cache should have a resident victim before retrying page {}",
                            page_no.get()
                        );
                    }
                    Err(err) => {
                        assert!(
                            matches!(err, FrankenError::OutOfMemory),
                            "unexpected insert error for page {}: {err}",
                            page_no.get()
                        );
                    }
                }
            }

            assert!(
                cache.len() <= POOL_CAPACITY,
                "resident pages should stay within pool capacity after admitting page {}",
                page_no.get()
            );
            let copy = cache
                .get_copy(page_no)
                .expect("latest admitted page should survive pressure eviction");
            assert_track_q_page(page_no, &copy);
        }
        let elapsed = started.elapsed();

        let metrics = cache.metrics_snapshot();
        assert_eq!(
            metrics.pool_capacity, POOL_CAPACITY,
            "metrics should report the configured pressure-test pool capacity"
        );
        assert!(
            metrics.cached_pages <= POOL_CAPACITY,
            "metrics should never report more resident pages than the pool capacity"
        );
        assert!(
            metrics.evictions >= u64::from(PAGE_COUNT) - u64::try_from(POOL_CAPACITY).unwrap(),
            "pressure test should evict enough pages to admit the over-capacity workload"
        );

        emit_track_q_log(
            "test_track_q_flat_hash_eviction_under_pool_pressure_stays_bounded",
            "verify",
            elapsed,
            usize::try_from(PAGE_COUNT).expect("page count fits usize"),
            u64::from(PAGE_COUNT),
            0,
            metrics.evictions,
            track_q_hit_rate(metrics.hits, metrics.misses),
            json!({
                "pool_capacity": POOL_CAPACITY,
                "resident_pages": metrics.cached_pages,
                "eviction_attempts": eviction_attempts
            }),
        );
    }

    #[test]
    fn test_track_q_flat_hash_capacity_rehash_preserves_pages() {
        const COLLIDERS: usize = 16;

        let small_capacity = round_flat_slot_capacity(FLAT_SLOTS_MIN_CAPACITY / 2);
        let large_capacity = round_flat_slot_capacity(FLAT_SLOTS_MIN_CAPACITY + 1);
        assert_eq!(
            small_capacity, FLAT_SLOTS_MIN_CAPACITY,
            "small profile should clamp to the minimum flat-slot capacity"
        );
        assert_eq!(
            large_capacity,
            FLAT_SLOTS_MIN_CAPACITY * 2,
            "large profile should round to the next flat-slot capacity"
        );

        let small_slots = FlatPageSlots::new(small_capacity);
        let large_slots = FlatPageSlots::new(large_capacity);
        let small_bucket = small_slots.hash_pgno(PageNumber::ONE.get());
        let mut colliders = Vec::with_capacity(COLLIDERS);
        let mut candidate = 1_u32;
        let mut first_large_bucket = None;
        let mut split_across_large_buckets = false;
        while colliders.len() < COLLIDERS || !split_across_large_buckets {
            let page_no = PageNumber::new(candidate).expect("rehash candidate page number");
            if small_slots.hash_pgno(candidate) == small_bucket {
                let large_bucket = large_slots.hash_pgno(candidate);
                if let Some(first) = first_large_bucket {
                    split_across_large_buckets |= large_bucket != first;
                } else {
                    first_large_bucket = Some(large_bucket);
                }
                if colliders.len() < COLLIDERS {
                    colliders.push(page_no);
                }
            }
            candidate = candidate
                .checked_add(1)
                .expect("rehash collision search should not exhaust page numbers");
        }
        assert!(
            split_across_large_buckets,
            "larger flat-slot mask should rehash the small-bucket collision set"
        );

        for page_no in colliders.iter().copied() {
            assert!(
                small_slots
                    .try_insert(page_no, track_q_page_buf(page_no))
                    .expect("small-capacity collider should fit in the probe window"),
                "small-capacity collider should be newly inserted"
            );
            assert!(
                large_slots
                    .try_insert(page_no, track_q_page_buf(page_no))
                    .expect("large-capacity collider should fit after rehashing"),
                "large-capacity collider should be newly inserted"
            );
        }

        small_slots.reset_metrics();
        large_slots.reset_metrics();
        let started = monotonic_test_clock();
        let mut small_chain_walk_count = 0_u64;
        let mut large_chain_walk_count = 0_u64;
        for page_no in colliders.iter().copied() {
            let small_copy = small_slots
                .get_copy(page_no)
                .expect("small-capacity collider should remain readable");
            assert_track_q_page(page_no, &small_copy);
            let large_copy = large_slots
                .get_copy(page_no)
                .expect("large-capacity collider should remain readable");
            assert_track_q_page(page_no, &large_copy);

            small_chain_walk_count = small_chain_walk_count.saturating_add(
                u64::try_from(track_q_probe_distance(&small_slots, page_no))
                    .expect("small probe distance fits u64"),
            );
            large_chain_walk_count = large_chain_walk_count.saturating_add(
                u64::try_from(track_q_probe_distance(&large_slots, page_no))
                    .expect("large probe distance fits u64"),
            );
        }
        let elapsed = started.elapsed();

        assert_eq!(
            small_slots.len(),
            colliders.len(),
            "small-capacity table should retain the whole collision set"
        );
        assert_eq!(
            large_slots.len(),
            colliders.len(),
            "large-capacity table should retain the whole rehashed collision set"
        );
        assert!(
            large_chain_walk_count < small_chain_walk_count,
            "larger capacity should rehash the collision set into shorter probe chains"
        );

        let hits = large_slots.hits.load(Ordering::Relaxed);
        emit_track_q_log(
            "test_track_q_flat_hash_capacity_rehash_preserves_pages",
            "verify",
            elapsed,
            colliders.len(),
            hits,
            large_chain_walk_count,
            1,
            track_q_hit_rate(hits, large_slots.misses.load(Ordering::Relaxed)),
            json!({
                "small_capacity": small_capacity,
                "large_capacity": large_capacity,
                "small_chain_walk_count": small_chain_walk_count,
                "large_chain_walk_count": large_chain_walk_count,
                "small_bucket": small_bucket,
                "resident_pages": large_slots.len()
            }),
        );
    }

    #[test]
    fn test_track_q_flat_hash_fast_path_transitions_cold_reset_hidden_tiers() {
        let mut cache = ShardedPageCache::with_max_buffers_and_shards(PageSize::DEFAULT, 16, 1);
        let sharded_page = PageNumber::new(201).expect("sharded transition page");
        let fast_page = PageNumber::new(202).expect("fast transition page");
        let replacement_page = PageNumber::new(203).expect("replacement transition page");

        assert!(
            !cache.is_fast_path_enabled(),
            "Track Q transition test should start on the sharded flat-cache path"
        );
        cache.insert_buffer(sharded_page, track_q_page_buf(sharded_page));
        let sharded_copy = cache
            .get_copy(sharded_page)
            .expect("sharded page should be visible before enabling fast path");
        assert_track_q_page(sharded_page, &sharded_copy);
        assert!(
            cache.flat_slots.contains(sharded_page),
            "default sharded mode should admit the setup page into flat slots"
        );

        let started = monotonic_test_clock();
        cache.enable_fast_path();
        assert!(
            cache.is_fast_path_enabled(),
            "fast path should be enabled after transition"
        );
        assert!(
            cache.get_copy(sharded_page).is_none(),
            "enabling fast path should cold-reset hidden sharded pages"
        );
        assert_eq!(
            cache.len(),
            0,
            "fast-path enable should leave a cold resident set"
        );

        cache.insert_buffer(fast_page, track_q_page_buf(fast_page));
        let fast_copy = cache
            .get_copy(fast_page)
            .expect("fast-path page should be visible while fast path is enabled");
        assert_track_q_page(fast_page, &fast_copy);
        assert_eq!(
            cache
                .fast_array
                .as_ref()
                .expect("fast array should exist after enabling fast path")
                .lock()
                .len(),
            1,
            "fast path should hold exactly the fast setup page"
        );

        cache.disable_fast_path();
        assert!(
            !cache.is_fast_path_enabled(),
            "fast path should be disabled after transition"
        );
        assert!(
            cache.get_copy(fast_page).is_none(),
            "disabling fast path should cold-reset hidden fast-array pages"
        );
        assert_eq!(
            cache.len(),
            0,
            "fast-path disable should return to a cold sharded resident set"
        );

        cache.insert_buffer(replacement_page, track_q_page_buf(replacement_page));
        let replacement_copy = cache
            .get_copy(replacement_page)
            .expect("replacement sharded page should be visible after fast disable");
        assert_track_q_page(replacement_page, &replacement_copy);
        assert!(
            cache.flat_slots.contains(replacement_page),
            "replacement page should return to the flat-slot tier"
        );

        cache.enable_fast_path();
        assert!(
            cache.get_copy(replacement_page).is_none(),
            "re-enabling fast path should not resurrect replacement sharded pages"
        );
        cache.disable_fast_path();
        assert!(
            cache.get_copy(replacement_page).is_none(),
            "disabling again should keep the re-enabled fast path cold reset"
        );
        let elapsed = started.elapsed();

        let metrics = cache.metrics_snapshot();
        assert_eq!(
            metrics.cached_pages, 0,
            "final transition state should have no hidden resident pages"
        );

        emit_track_q_log(
            "test_track_q_flat_hash_fast_path_transitions_cold_reset_hidden_tiers",
            "verify",
            elapsed,
            3,
            metrics.hits,
            0,
            metrics.evictions,
            track_q_hit_rate(metrics.hits, metrics.misses),
            json!({
                "transitions": 4,
                "final_fast_path_enabled": cache.is_fast_path_enabled(),
                "final_resident_pages": metrics.cached_pages,
                "evictions": metrics.evictions
            }),
        );
    }

    #[test]
    fn test_track_q_flat_hash_remove_and_reclaim_tombstone_slots() {
        let slots = FlatPageSlots::new(64);
        let target_bucket = slots.hash_pgno(PageNumber::ONE.get());
        let colliders = track_q_collision_pages(&slots, target_bucket, 4);

        for page_no in colliders.iter().copied().take(3) {
            slots
                .try_insert(page_no, track_q_page_buf(page_no))
                .expect("reclaim setup insert should stay in flat slots");
        }

        let removed = colliders[1];
        let removed_slot = slots
            .find_slot(removed)
            .expect("removed page should be present");
        assert!(slots.remove(removed), "middle collider should be removable");
        assert_eq!(
            slots.len(),
            2,
            "remove should decrement occupied slot count"
        );
        assert!(
            slots.get_copy(removed).is_none(),
            "removed collider should no longer be visible"
        );

        let tail_copy = slots
            .get_copy(colliders[2])
            .expect("later collider must stay visible across the tombstone");
        assert_track_q_page(colliders[2], &tail_copy);

        let replacement = colliders[3];
        let inserted = slots
            .try_insert(replacement, track_q_page_buf(replacement))
            .expect("replacement collider should reuse the tombstone slot");
        assert!(inserted, "replacement collider should be newly inserted");
        assert_eq!(
            slots.find_slot(replacement).expect("replacement slot"),
            removed_slot,
            "replacement collider should reclaim the tombstoned slot"
        );

        slots.reset_metrics();
        let started = Instant::now();
        for page_no in [colliders[0], colliders[2], replacement] {
            let copy = slots
                .get_copy(page_no)
                .expect("resident collider should read after tombstone reuse");
            assert_track_q_page(page_no, &copy);
        }
        let elapsed = started.elapsed();

        let distances = [colliders[0], colliders[2], replacement]
            .into_iter()
            .map(|page_no| {
                u64::try_from(track_q_probe_distance(&slots, page_no)).expect("probe distance")
            })
            .collect::<Vec<_>>();
        let chain_walk_count = distances.iter().copied().sum::<u64>();
        let bucket_access_count = chain_walk_count
            .saturating_add(u64::try_from(distances.len()).expect("distance count fits u64"));
        let hits = slots.hits.load(Ordering::Relaxed);

        emit_track_q_log(
            "test_track_q_flat_hash_remove_and_reclaim_tombstone_slots",
            "verify",
            elapsed,
            slots.len(),
            bucket_access_count,
            chain_walk_count,
            0,
            track_q_hit_rate(hits, slots.misses.load(Ordering::Relaxed)),
            json!({
                "target_bucket": target_bucket,
                "removed_page": removed.get(),
                "replacement_page": replacement.get(),
                "reclaimed_slot": removed_slot
            }),
        );
    }

    #[test]
    fn test_track_q_flat_hash_clear_reclaims_tombstones_after_full_drain() {
        let slots = FlatPageSlots::new(64);
        let target_bucket = slots.hash_pgno(PageNumber::ONE.get());
        let colliders = track_q_collision_pages(&slots, target_bucket, 6);

        for page_no in colliders.iter().copied().take(4) {
            slots
                .try_insert(page_no, track_q_page_buf(page_no))
                .expect("tombstone-clear setup insert should stay in flat slots");
        }

        for page_no in colliders.iter().copied().take(4) {
            assert!(
                slots.remove(page_no),
                "setup page {} should be removable",
                page_no.get()
            );
        }

        assert_eq!(
            slots.len(),
            0,
            "all resident pages should be drained before clear"
        );
        assert!(
            slots.has_tombstones.load(Ordering::Acquire),
            "drained colliders should leave a tombstone cleanup hint before clear"
        );

        let removed = slots.clear();
        assert_eq!(
            removed, 0,
            "clearing a fully drained table should not report extra page evictions"
        );
        assert!(
            !slots.has_tombstones.load(Ordering::Acquire),
            "clear should consume the tombstone cleanup hint when no eviction races it"
        );
        assert!(
            slots
                .slots
                .iter()
                .all(|slot| slot.pgno.load(Ordering::Acquire) == SLOT_EMPTY),
            "clear should restore every flat slot to the empty sentinel"
        );

        let replacement = colliders[4];
        assert!(
            slots
                .try_insert(replacement, track_q_page_buf(replacement))
                .expect("replacement insert after clear should stay in flat slots"),
            "replacement page should be newly inserted after clear"
        );
        assert_eq!(
            track_q_probe_distance(&slots, replacement),
            0,
            "replacement should start a fresh probe chain after tombstone cleanup"
        );
    }

    #[test]
    fn test_track_q_flat_hash_latency_hot_probe_sub_15ns() {
        const ITERATIONS: u32 = 1_000_000;

        let slots = FlatPageSlots::new(64);
        let hot_page = PageNumber::ONE;
        let inserted = slots
            .try_insert(hot_page, track_q_page_buf(hot_page))
            .expect("hot latency page should stay in flat slots");
        assert!(inserted, "hot latency page should be newly inserted");
        assert_eq!(
            track_q_probe_distance(&slots, hot_page),
            0,
            "hot latency page should be a direct bucket hit"
        );

        let clock_start = monotonic_test_clock();
        let mut hits = 0_u64;
        for _ in 0..ITERATIONS {
            if slots.contains(hot_page) {
                hits = hits.saturating_add(1);
            }
        }
        let elapsed = clock_start.elapsed();
        let avg_ns = (elapsed.as_secs_f64() * 1_000_000_000.0) / f64::from(ITERATIONS);

        assert_eq!(
            hits,
            u64::from(ITERATIONS),
            "hot latency probe should hit on every iteration"
        );
        if !cfg!(debug_assertions) {
            assert!(
                avg_ns <= 15.0,
                "release/perf hot-probe average should stay under 15ns, got {avg_ns:.2}ns"
            );
        }

        emit_track_q_log(
            "test_track_q_flat_hash_latency_hot_probe_sub_15ns",
            "verify",
            elapsed,
            1,
            hits,
            0,
            0,
            1.0,
            json!({
                "iterations": ITERATIONS,
                "average_ns_per_probe": avg_ns,
                "debug_assertions": cfg!(debug_assertions)
            }),
        );
    }

    #[test]
    fn test_track_q_flat_hash_concurrent_reads_eight_threads() {
        use std::sync::{Arc, Barrier};
        use std::thread;

        const THREADS: usize = 8;
        const ITERATIONS_PER_THREAD: usize = 1_024;
        const HOT_COLLIDERS: usize = 8;

        let slots = Arc::new(FlatPageSlots::new(64));
        let target_bucket = slots.hash_pgno(PageNumber::ONE.get());
        let hot_pages = track_q_collision_pages(&slots, target_bucket, HOT_COLLIDERS);
        for page_no in hot_pages.iter().copied() {
            let inserted = slots
                .try_insert(page_no, track_q_page_buf(page_no))
                .expect("concurrent-read collider should stay in flat slots");
            assert!(inserted, "hot collider should be newly inserted");
        }
        slots.reset_metrics();

        let start_barrier = Arc::new(Barrier::new(THREADS + 1));
        let handles: Vec<_> = (0..THREADS)
            .map(|thread_idx| {
                let slots = Arc::clone(&slots);
                let start_barrier = Arc::clone(&start_barrier);
                let hot_pages = hot_pages.clone();
                thread::spawn(move || {
                    start_barrier.wait();
                    for iter in 0..ITERATIONS_PER_THREAD {
                        let page_no = hot_pages[(thread_idx + iter) % hot_pages.len()];
                        let copy = slots
                            .get_copy(page_no)
                            .expect("concurrent hot collider should remain readable");
                        assert_track_q_page(page_no, &copy);
                    }
                })
            })
            .collect();

        let started = Instant::now();
        start_barrier.wait();
        for handle in handles {
            handle
                .join()
                .expect("track q concurrent reader should not panic");
        }
        let elapsed = started.elapsed();

        let expected_hits =
            u64::try_from(THREADS * ITERATIONS_PER_THREAD).expect("expected hit count fits u64");
        assert_eq!(
            slots.hits.load(Ordering::Relaxed),
            expected_hits,
            "every concurrent hot read should register as a flat-slot hit"
        );

        let mut bucket_access_count = 0_u64;
        let mut chain_walk_count = 0_u64;
        for thread_idx in 0..THREADS {
            for iter in 0..ITERATIONS_PER_THREAD {
                let page_no = hot_pages[(thread_idx + iter) % hot_pages.len()];
                let distance = u64::try_from(track_q_probe_distance(&slots, page_no))
                    .expect("probe distance fits u64");
                bucket_access_count =
                    bucket_access_count.saturating_add(distance.saturating_add(1));
                chain_walk_count = chain_walk_count.saturating_add(distance);
            }
        }

        emit_track_q_log(
            "test_track_q_flat_hash_concurrent_reads_eight_threads",
            "verify",
            elapsed,
            hot_pages.len(),
            bucket_access_count,
            chain_walk_count,
            0,
            1.0,
            json!({
                "threads": THREADS,
                "iterations_per_thread": ITERATIONS_PER_THREAD,
                "target_bucket": target_bucket,
                "resident_pages": slots.len()
            }),
        );
    }

    #[test]
    fn test_track_q_flat_hash_concurrent_reader_writer_slot_consistency() {
        use std::sync::{Arc, Barrier};
        use std::thread;

        const READERS: usize = 4;
        const READS_PER_READER: usize = 512;
        const WRITE_GENERATIONS: u8 = 64;

        fn versioned_page_buf(page_no: PageNumber, generation: u8) -> PageBuf {
            let mut buf = track_q_page_buf(page_no);
            let data = buf.as_mut_slice();
            data[4] = generation;
            data[5] = generation ^ 0xFF;
            data[6] = generation.wrapping_add(17);
            buf
        }

        fn assert_versioned_page(page_no: PageNumber, data: &[u8]) {
            assert_track_q_page(page_no, data);
            let generation = data[4];
            assert_eq!(
                data[5],
                generation ^ 0xFF,
                "reader saw a torn version complement for page {}",
                page_no.get()
            );
            assert_eq!(
                data[6],
                generation.wrapping_add(17),
                "reader saw a torn version offset for page {}",
                page_no.get()
            );
        }

        let slots = Arc::new(FlatPageSlots::new(64));
        let page_no = PageNumber::new(117).expect("page number");
        assert!(
            slots
                .try_insert(page_no, versioned_page_buf(page_no, 0))
                .expect("initial versioned page should stay in flat slots"),
            "initial versioned page should be newly inserted"
        );
        slots.reset_metrics();

        let start_barrier = Arc::new(Barrier::new(READERS + 2));
        let reader_handles = (0..READERS)
            .map(|_| {
                let slots = Arc::clone(&slots);
                let start_barrier = Arc::clone(&start_barrier);
                thread::spawn(move || {
                    start_barrier.wait();
                    for _ in 0..READS_PER_READER {
                        let copy = slots
                            .get_copy(page_no)
                            .expect("concurrent reader should find the hot slot");
                        assert_versioned_page(page_no, &copy);
                    }
                })
            })
            .collect::<Vec<_>>();

        let writer_slots = Arc::clone(&slots);
        let writer_barrier = Arc::clone(&start_barrier);
        let writer_handle = thread::spawn(move || {
            writer_barrier.wait();
            for generation in 1..=WRITE_GENERATIONS {
                let inserted = writer_slots
                    .try_insert(page_no, versioned_page_buf(page_no, generation))
                    .expect("same-page writer update should stay in the resident flat slot");
                assert!(
                    !inserted,
                    "same-page writer update should replace the slot in place"
                );
            }
        });

        let started = monotonic_test_clock();
        start_barrier.wait();
        writer_handle
            .join()
            .expect("track q flat-slot writer should not panic");
        for handle in reader_handles {
            handle
                .join()
                .expect("track q flat-slot reader should not panic");
        }
        let elapsed = started.elapsed();

        let final_copy = slots
            .get_copy(page_no)
            .expect("hot slot should remain readable after writer completes");
        assert_versioned_page(page_no, &final_copy);
        assert_eq!(
            final_copy[4], WRITE_GENERATIONS,
            "final slot copy should contain the last writer generation"
        );
        assert_eq!(
            slots.len(),
            1,
            "reader/writer churn should keep a single resident slot"
        );
        assert_eq!(
            slots.admits.load(Ordering::Relaxed),
            0,
            "same-page updates after metric reset should not count as new admissions"
        );

        let expected_reader_hits =
            u64::try_from(READERS * READS_PER_READER).expect("reader hit count fits u64");
        let hits = slots.hits.load(Ordering::Relaxed);
        assert!(
            hits >= expected_reader_hits,
            "reader/writer test should record at least one hit per reader copy"
        );

        emit_track_q_log(
            "test_track_q_flat_hash_concurrent_reader_writer_slot_consistency",
            "verify",
            elapsed,
            1,
            hits,
            u64::try_from(track_q_probe_distance(&slots, page_no)).expect("probe distance fits"),
            0,
            track_q_hit_rate(hits, slots.misses.load(Ordering::Relaxed)),
            json!({
                "readers": READERS,
                "reads_per_reader": READS_PER_READER,
                "write_generations": WRITE_GENERATIONS,
                "resident_pages": slots.len()
            }),
        );
    }

    #[test]
    #[ignore = "benchmark evidence only"]
    fn test_fast_path_vs_sharded_latency_comparison() {
        // Compare latency of fast path vs sharded path for single-thread workload.
        use std::time::Instant;

        const ITERATIONS: u32 = 100_000;

        // Fast path (single connection mode)
        let fast_cache = ShardedPageCache::new_single_connection(PageSize::DEFAULT);
        let start = Instant::now();
        for i in 1..=ITERATIONS {
            let pn = PageNumber::new(i).unwrap();
            fast_cache.insert_fresh(pn, |_| {}).unwrap();
            fast_cache.with_page(pn, |_| {});
        }
        let fast_elapsed = start.elapsed();

        // Sharded path (normal mode)
        let sharded_cache = ShardedPageCache::new(PageSize::DEFAULT);
        let start = Instant::now();
        for i in 1..=ITERATIONS {
            let pn = PageNumber::new(i).unwrap();
            sharded_cache.insert_fresh(pn, |_| {}).unwrap();
            sharded_cache.with_page(pn, |_| {});
        }
        let sharded_elapsed = start.elapsed();

        let speedup = sharded_elapsed.as_nanos() as f64 / fast_elapsed.as_nanos() as f64;

        // Fast path should be faster (at least 1.2x for single-thread)
        eprintln!(
            "bead_id={BEAD_FZR07} fast_path={:?} sharded={:?} speedup={:.2}x",
            fast_elapsed, sharded_elapsed, speedup
        );

        assert!(
            speedup >= 1.2,
            "bead_id={BEAD_FZR07} case=latency_comparison \
             fast path should be at least 1.2x faster, got {speedup:.2}x"
        );
    }

    // -- Pure-logic snapshot/metric type tests --

    #[test]
    fn lightweight_snapshot_total_accesses() {
        let snap = PageCacheLightweightSnapshot {
            hits: 75,
            misses: 25,
            ..PageCacheLightweightSnapshot::default()
        };
        assert_eq!(snap.total_accesses(), 100);
    }

    #[test]
    fn lightweight_snapshot_hit_rate_zero_accesses() {
        let snap = PageCacheLightweightSnapshot::default();
        assert!((snap.hit_rate_percent() - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn lightweight_snapshot_hit_rate_percent() {
        let snap = PageCacheLightweightSnapshot {
            hits: 80,
            misses: 20,
            ..PageCacheLightweightSnapshot::default()
        };
        assert!((snap.hit_rate_percent() - 80.0).abs() < 0.01);
    }

    #[test]
    fn metrics_snapshot_total_accesses() {
        let snap = PageCacheMetricsSnapshot {
            hits: 50,
            misses: 50,
            ..PageCacheMetricsSnapshot::default()
        };
        assert_eq!(snap.total_accesses(), 100);
    }

    #[test]
    fn metrics_snapshot_hit_rate_zero() {
        let snap = PageCacheMetricsSnapshot::default();
        assert!((snap.hit_rate_percent() - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn metrics_snapshot_hit_rate_100() {
        let snap = PageCacheMetricsSnapshot {
            hits: 100,
            misses: 0,
            ..PageCacheMetricsSnapshot::default()
        };
        assert!((snap.hit_rate_percent() - 100.0).abs() < 0.01);
    }

    #[test]
    fn metrics_snapshot_efficiency_snapshot_preserves_fields() {
        let snap = PageCacheMetricsSnapshot {
            hits: 42,
            misses: 8,
            admits: 20,
            evictions: 10,
            cached_pages: 15,
            pool_capacity: 100,
            dirty_ratio_pct: 30,
            t1_size: 5,
            t2_size: 10,
            b1_size: 3,
            b2_size: 2,
            p_target: 50,
            mvcc_multi_version_pages: 1,
        };
        let eff = snap.efficiency_snapshot();
        assert_eq!(eff.hits, 42);
        assert_eq!(eff.misses, 8);
        assert_eq!(eff.admits, 20);
        assert_eq!(eff.evictions, 10);
        assert_eq!(eff.cached_pages, 15);
        assert_eq!(eff.pool_capacity, 100);
        assert_eq!(eff.dirty_ratio_pct, 30);
        assert_eq!(eff.t1_size, 5);
        assert_eq!(eff.t2_size, 10);
        assert_eq!(eff.mvcc_multi_version_pages, 1);
    }

    #[test]
    fn queue_kind_as_str_covers_all_variants() {
        assert_eq!(PageCacheQueueKind::T1.as_str(), "t1");
        assert_eq!(PageCacheQueueKind::T2.as_str(), "t2");
        assert_eq!(PageCacheQueueKind::B1.as_str(), "b1");
        assert_eq!(PageCacheQueueKind::B2.as_str(), "b2");
    }

    #[test]
    fn eviction_policy_default_is_arbitrary() {
        assert_eq!(
            PageCacheEvictionPolicy::default(),
            PageCacheEvictionPolicy::Arbitrary
        );
    }

    #[test]
    fn resolve_page_buffer_max_explicit_overrides_default() {
        let explicit = resolve_page_buffer_max(Some(42));
        assert_eq!(explicit, 42);
    }

    #[test]
    fn resolve_page_buffer_max_none_uses_default() {
        let default_val = resolve_page_buffer_max(None);
        assert!(default_val > 0);
    }

    #[test]
    fn lightweight_snapshot_total_accesses_and_hit_rate() {
        let snap = PageCacheLightweightSnapshot {
            hits: 80,
            misses: 20,
            admits: 0,
            evictions: 0,
            cached_pages: 0,
            pool_capacity: 0,
        };
        assert_eq!(snap.total_accesses(), 100);
        let rate = snap.hit_rate_percent();
        assert!((rate - 80.0).abs() < f64::EPSILON);
        let empty = PageCacheLightweightSnapshot::default();
        assert_eq!(empty.total_accesses(), 0);
        assert!((empty.hit_rate_percent() - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn metrics_snapshot_default_and_computed_methods() {
        let def = PageCacheMetricsSnapshot::default();
        assert_eq!(def.hits, 0);
        assert_eq!(def.total_accesses(), 0);
        assert!((def.hit_rate_percent() - 0.0).abs() < f64::EPSILON);
        let snap = PageCacheMetricsSnapshot {
            hits: 90,
            misses: 10,
            admits: 5,
            evictions: 3,
            cached_pages: 50,
            pool_capacity: 100,
            dirty_ratio_pct: 25,
            t1_size: 30,
            t2_size: 20,
            b1_size: 5,
            b2_size: 3,
            p_target: 40,
            mvcc_multi_version_pages: 2,
        };
        assert_eq!(snap.total_accesses(), 100);
        let eff = snap.efficiency_snapshot();
        assert_eq!(eff.hits, 90);
        assert_eq!(eff.pool_capacity, 100);
    }

    #[test]
    fn page_cache_page_snapshot_debug_clone_copy_eq() {
        let snap = PageCachePageSnapshot {
            page_no: PageNumber::new(7).unwrap(),
            version_txn_id: Some(42),
            queue: Some(PageCacheQueueKind::T2),
            dirty: true,
            ref_count: 3,
            access_count: 99,
        };
        let copied = snap;
        assert_eq!(copied, snap);
        let other = PageCachePageSnapshot {
            dirty: false,
            ..snap
        };
        assert_ne!(snap, other);
        let dbg = format!("{snap:?}");
        assert!(dbg.contains("PageCachePageSnapshot"));
    }

    #[test]
    fn page_cache_eviction_policy_default_is_arbitrary() {
        let def = PageCacheEvictionPolicy::default();
        assert_eq!(def, PageCacheEvictionPolicy::Arbitrary);
        let dbg = format!("{def:?}");
        assert!(dbg.contains("Arbitrary"));
        let copied = def;
        assert_eq!(copied, def);
    }

    // ---------------------------------------------------------------
    // bd-pm1zd Track F verification tests
    // ---------------------------------------------------------------

    #[test]
    fn test_flat_page_array_basic() {
        let mut arr = FastPageArray::new();
        let pool = PageBufPool::new(PageSize::DEFAULT, 8);
        let p1 = PageNumber::ONE;
        let p2 = PageNumber::new(2).unwrap();
        let p3 = PageNumber::new(3).unwrap();

        let mut b1 = pool.acquire().unwrap();
        b1.as_mut_slice()[0] = 0x11;
        arr.insert(p1, b1);

        let mut b2 = pool.acquire().unwrap();
        b2.as_mut_slice()[0] = 0x22;
        arr.insert(p2, b2);

        let mut b3 = pool.acquire().unwrap();
        b3.as_mut_slice()[0] = 0x33;
        arr.insert(p3, b3);

        assert_eq!(arr.get(p1).map(|s| s[0]), Some(0x11));
        assert_eq!(arr.get(p2).map(|s| s[0]), Some(0x22));
        assert_eq!(arr.get(p3).map(|s| s[0]), Some(0x33));

        arr.remove(p2);
        assert!(arr.get(p2).is_none());
        assert_eq!(arr.get(p1).map(|s| s[0]), Some(0x11));
        assert_eq!(arr.get(p3).map(|s| s[0]), Some(0x33));
    }

    #[test]
    fn test_flat_page_array_bounds() {
        let mut arr = FastPageArray::new();
        let pool = PageBufPool::new(PageSize::DEFAULT, 4);

        let within = PageNumber::new(32).unwrap();
        let mut bw = pool.acquire().unwrap();
        bw.as_mut_slice()[0] = 0xAA;
        arr.insert(within, bw);
        assert_eq!(arr.get(within).map(|s| s[0]), Some(0xAA));

        let beyond = PageNumber::new(33).unwrap();
        let mut bb = pool.acquire().unwrap();
        bb.as_mut_slice()[0] = 0xBB;
        arr.insert(beyond, bb);
        assert!(
            arr.get(beyond).is_some(),
            "FastPageArray should grow to accommodate pages beyond initial capacity"
        );
        assert_eq!(arr.get(beyond).map(|s| s[0]), Some(0xBB));
    }

    #[test]
    fn test_single_connection_fast_path_bypasses_shards() {
        let cache = ShardedPageCache::new_single_connection(PageSize::DEFAULT);
        assert!(cache.is_fast_path_enabled());

        let p1 = PageNumber::ONE;
        let p2 = PageNumber::new(100).unwrap();
        cache.insert_fresh(p1, |data| data[0] = 0x11).unwrap();
        cache.insert_fresh(p2, |data| data[0] = 0x22).unwrap();

        cache.with_page(p1, |data| assert_eq!(data[0], 0x11));
        cache.with_page(p2, |data| assert_eq!(data[0], 0x22));

        assert_eq!(cache.len(), 2, "fast array must retain both pages");
        assert_eq!(
            cache.flat_slots.len(),
            0,
            "single-connection fast path must bypass flat slots"
        );
        let total_in_shards: usize = cache.shards.iter().map(|shard| shard.lock().len()).sum();
        assert_eq!(
            total_in_shards, 0,
            "single-connection fast path should not populate overflow shards"
        );
    }

    #[test]
    fn test_concurrent_page_reads_8_threads() {
        use std::sync::Arc;
        use std::thread;

        let cache = Arc::new(ShardedPageCache::new(PageSize::DEFAULT));
        let page_count = 64_u32;
        for i in 1..=page_count {
            let pn = PageNumber::new(i).unwrap();
            cache
                .insert_fresh(pn, |data| {
                    data[0] = (i & 0xFF) as u8;
                    data[1] = ((i >> 8) & 0xFF) as u8;
                })
                .unwrap();
        }

        let handles: Vec<_> = (0..8)
            .map(|_| {
                let c = Arc::clone(&cache);
                thread::spawn(move || {
                    for round in 0..100 {
                        for i in 1..=page_count {
                            let pn = PageNumber::new(i).unwrap();
                            c.with_page(pn, |data| {
                                assert_eq!(
                                    data[0],
                                    (i & 0xFF) as u8,
                                    "round={round} page={i} byte mismatch"
                                );
                            });
                        }
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().expect("reader thread panicked");
        }

        let m = cache.metrics_snapshot();
        assert!(
            m.hits >= 8 * 100 * u64::from(page_count),
            "expected at least {} hits, got {}",
            8 * 100 * u64::from(page_count),
            m.hits
        );
    }

    #[test]
    fn test_page_eviction_correctness() {
        let cache = ShardedPageCache::with_max_buffers(PageSize::DEFAULT, 128);

        let p1 = PageNumber::new(1).unwrap();
        let p2 = PageNumber::new(2).unwrap();
        let p3 = PageNumber::new(3).unwrap();

        cache.insert_fresh(p1, |data| data.fill(0x11)).unwrap();
        cache.insert_fresh(p2, |data| data.fill(0x22)).unwrap();
        cache.insert_fresh(p3, |data| data.fill(0x33)).unwrap();

        assert!(cache.evict(p2));
        assert!(!cache.contains(p2));
        assert!(cache.contains(p1));
        assert!(cache.contains(p3));

        cache.insert_fresh(p2, |data| data.fill(0xDD)).unwrap();

        cache.with_page(p2, |data| {
            assert!(
                data.iter().all(|&b| b == 0xDD),
                "re-loaded page should have new data, not stale evicted data"
            );
        });

        cache.with_page(p1, |data| {
            assert!(
                data.iter().all(|&b| b == 0x11),
                "untouched page should still have original data"
            );
        });
        cache.with_page(p3, |data| {
            assert!(
                data.iter().all(|&b| b == 0x33),
                "untouched page should still have original data"
            );
        });
    }

    #[test]
    fn test_flat_slots_lock_free_miss() {
        let cache = ShardedPageCache::new(PageSize::DEFAULT);
        let absent = PageNumber::new(999).unwrap();

        assert!(!cache.contains(absent));
        assert!(cache.get(absent).is_none());
        assert!(cache.with_page(absent, |_| ()).is_none());
        assert!(cache.get_shared(absent).is_none());

        let m = cache.metrics_snapshot();
        assert!(m.misses >= 1, "cache miss on absent page should be tracked");
    }

    #[test]
    fn test_flat_slots_concurrent_insert_and_read() {
        use std::sync::Arc;
        use std::thread;

        let cache = Arc::new(ShardedPageCache::new(PageSize::DEFAULT));
        let barrier = Arc::new(std::sync::Barrier::new(4));

        let mut writer_handles: Vec<thread::JoinHandle<()>> = Vec::new();
        let mut reader_handles: Vec<thread::JoinHandle<u64>> = Vec::new();

        for tid in 0..2_u32 {
            let c = Arc::clone(&cache);
            let b = Arc::clone(&barrier);
            writer_handles.push(thread::spawn(move || {
                b.wait();
                for i in 0..500 {
                    let pn = PageNumber::new(tid * 10000 + i + 1).unwrap();
                    c.insert_fresh(pn, |data| data[0] = (tid & 0xFF) as u8)
                        .unwrap();
                }
            }));
        }

        for tid in 0..2_u32 {
            let c = Arc::clone(&cache);
            let b = Arc::clone(&barrier);
            reader_handles.push(thread::spawn(move || {
                b.wait();
                let mut found = 0_u64;
                for _ in 0..5 {
                    for i in 0..500 {
                        let pn = PageNumber::new(tid * 10000 + i + 1).unwrap();
                        if c.with_page(pn, |_| ()).is_some() {
                            found += 1;
                        }
                    }
                }
                found
            }));
        }

        for h in writer_handles {
            h.join().expect("writer thread panicked");
        }
        for h in reader_handles {
            h.join().expect("reader thread panicked");
        }

        assert!(cache.len() >= 1000);
    }
}
