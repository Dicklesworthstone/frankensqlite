//! NitroSketch probabilistic telemetry (bd-19u.4).
//!
//! Sub-linear memory data structures for runtime query statistics:
//!
//!   - [`CountMinSketch`]: frequency estimation with bounded overcount error.
//!   - [`StreamingHistogram`]: latency distribution with configurable bucket
//!     boundaries and percentile computation.
//!   - Global atomic metrics: `fsqlite_sketch_memory_bytes` gauge,
//!     `fsqlite_sketch_estimates` counter.
//!   - Tracing integration: `sketch_update` span emitted on observation.

use std::sync::atomic::{AtomicU64, Ordering};

use serde::Serialize;

use crate::conflict_model::mix64;

// ---------------------------------------------------------------------------
// Global metrics (atomic counters)
// ---------------------------------------------------------------------------

/// Total estimated memory held by active sketch instances (bytes).
static FSQLITE_SKETCH_MEMORY_BYTES: AtomicU64 = AtomicU64::new(0);

// Monotonic allocation/free totals make the Drop accounting test immune to
// unrelated sketches being created or destroyed by parallel unit tests. They
// are test-only so the production metrics surface remains unchanged.
#[cfg(test)]
static FSQLITE_SKETCH_TEST_ALLOCATED_BYTES_TOTAL: AtomicU64 = AtomicU64::new(0);
#[cfg(test)]
static FSQLITE_SKETCH_TEST_FREED_BYTES_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Total number of sketch estimate queries served (cardinality, frequency, etc.).
static FSQLITE_SKETCH_ESTIMATES_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Total observation events across all sketch instances.
static FSQLITE_SKETCH_OBSERVATIONS_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Snapshot of sketch telemetry metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct SketchTelemetryMetrics {
    pub fsqlite_sketch_memory_bytes: u64,
    pub fsqlite_sketch_estimates_total: u64,
    pub fsqlite_sketch_observations_total: u64,
}

/// Take a snapshot of the global sketch telemetry metrics.
#[must_use]
pub fn sketch_telemetry_metrics() -> SketchTelemetryMetrics {
    SketchTelemetryMetrics {
        fsqlite_sketch_memory_bytes: FSQLITE_SKETCH_MEMORY_BYTES.load(Ordering::Relaxed),
        fsqlite_sketch_estimates_total: FSQLITE_SKETCH_ESTIMATES_TOTAL.load(Ordering::Relaxed),
        fsqlite_sketch_observations_total: FSQLITE_SKETCH_OBSERVATIONS_TOTAL
            .load(Ordering::Relaxed),
    }
}

/// Reset all sketch telemetry metrics to zero.
pub fn reset_sketch_telemetry_metrics() {
    FSQLITE_SKETCH_MEMORY_BYTES.store(0, Ordering::Relaxed);
    FSQLITE_SKETCH_ESTIMATES_TOTAL.store(0, Ordering::Relaxed);
    FSQLITE_SKETCH_OBSERVATIONS_TOTAL.store(0, Ordering::Relaxed);
}

fn record_memory_add(bytes: u64) {
    FSQLITE_SKETCH_MEMORY_BYTES.fetch_add(bytes, Ordering::Relaxed);
    #[cfg(test)]
    FSQLITE_SKETCH_TEST_ALLOCATED_BYTES_TOTAL.fetch_add(bytes, Ordering::Relaxed);
}

fn record_memory_sub(bytes: u64) {
    // Saturating to avoid underflow if reset races with deallocation.
    // MSRV-safe: `try_update` is only stable since 1.95 (workspace MSRV is 1.85), so keep the
    // deprecated-on-nightly `fetch_update` and silence the deprecation.
    #[allow(deprecated)]
    let _ = FSQLITE_SKETCH_MEMORY_BYTES.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
        Some(v.saturating_sub(bytes))
    });
    #[cfg(test)]
    FSQLITE_SKETCH_TEST_FREED_BYTES_TOTAL.fetch_add(bytes, Ordering::Relaxed);
}

fn record_observation() {
    FSQLITE_SKETCH_OBSERVATIONS_TOTAL.fetch_add(1, Ordering::Relaxed);
}

fn record_estimate() {
    FSQLITE_SKETCH_ESTIMATES_TOTAL.fetch_add(1, Ordering::Relaxed);
}

// ---------------------------------------------------------------------------
// Count-Min Sketch
// ---------------------------------------------------------------------------

/// Count-Min Sketch version marker for evidence logs.
pub const CMS_VERSION: &str = "fsqlite:frequency:cms:v1";

/// Default width (number of counters per row).
pub const DEFAULT_CMS_WIDTH: usize = 2048;

/// Default depth (number of hash rows).
pub const DEFAULT_CMS_DEPTH: usize = 4;

/// Configuration for a [`CountMinSketch`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CountMinSketchConfig {
    /// Number of counters per row (`w`). Error bound: `ε = e / w`.
    pub width: usize,
    /// Number of hash rows (`d`). Probability bound: `δ = (1/e)^d`.
    pub depth: usize,
    /// Deterministic seed for hash derivation.
    pub seed: u64,
}

impl Default for CountMinSketchConfig {
    fn default() -> Self {
        Self {
            width: DEFAULT_CMS_WIDTH,
            depth: DEFAULT_CMS_DEPTH,
            seed: 0,
        }
    }
}

/// Count-Min Sketch for frequency estimation.
///
/// A probabilistic data structure that answers "how many times has item X been
/// observed?" with bounded overcount error. Never undercounts.
///
/// Space: `O(w * d)` counters. Update: `O(d)` hash computations.
/// Query: `O(d)` with `min` across rows.
///
/// Error guarantee: `count_hat(x) <= count(x) + ε * N` with probability `1 - δ`,
/// where `ε = e / w`, `δ = (1/e)^d`, and `N` is total observations.
pub struct CountMinSketch {
    width: usize,
    depth: usize,
    seeds: Vec<u64>,
    /// Row-major counter matrix: `counters[row * width + col]`.
    counters: Vec<u64>,
    total_count: u64,
}

impl std::fmt::Debug for CountMinSketch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CountMinSketch")
            .field("width", &self.width)
            .field("depth", &self.depth)
            .field("total_count", &self.total_count)
            .finish_non_exhaustive()
    }
}

impl CountMinSketch {
    /// Create a new Count-Min Sketch from configuration.
    #[must_use]
    pub fn new(config: &CountMinSketchConfig) -> Self {
        assert!(config.width > 0, "CMS width must be > 0");
        assert!(config.depth > 0, "CMS depth must be > 0");

        let seeds: Vec<u64> = (0..config.depth)
            .map(|d| {
                let mut hasher = blake3::Hasher::new();
                hasher.update(CMS_VERSION.as_bytes());
                hasher.update(&config.seed.to_le_bytes());
                #[allow(clippy::cast_possible_truncation)]
                hasher.update(&(d as u64).to_le_bytes());
                let hash = hasher.finalize();
                let bytes: [u8; 8] = hash.as_bytes()[..8].try_into().expect("8 bytes");
                u64::from_le_bytes(bytes)
            })
            .collect();

        let num_counters = config.width * config.depth;
        let mem_bytes = (num_counters * 8) as u64;
        record_memory_add(mem_bytes);

        Self {
            width: config.width,
            depth: config.depth,
            seeds,
            counters: vec![0; num_counters],
            total_count: 0,
        }
    }

    /// Observe an item (increment its frequency by 1).
    pub fn observe(&mut self, item: u64) {
        self.observe_n(item, 1);
    }

    /// Observe an item with a given count increment.
    #[allow(clippy::cast_possible_truncation)]
    pub fn observe_n(&mut self, item: u64, count: u64) {
        // ALIEN ARTIFACT: Count-Min Sketch with Conservative Update (CU).
        // Instead of unconditionally adding `count` to every hashed bucket,
        // we first find the current minimum estimate across all rows. We then
        // only increase a counter if it is less than `min_estimate + count`.
        // This mathematically bounds the overestimation error far more tightly
        // than standard CMS, providing strictly superior accuracy at zero memory cost.
        let mut min_count = u64::MAX;
        let mut indices = Vec::with_capacity(self.depth);

        for (row, seed) in self.seeds.iter().enumerate() {
            let hash = mix64(item ^ *seed);
            let col = (hash as usize) % self.width;
            let idx = row * self.width + col;
            indices.push(idx);
            min_count = min_count.min(self.counters[idx]);
        }

        let target_count = min_count.saturating_add(count);

        for &idx in &indices {
            if self.counters[idx] < target_count {
                self.counters[idx] = target_count;
            }
        }
        self.total_count = self.total_count.saturating_add(count);

        record_observation();
        tracing::trace!(
            target: "fsqlite.sketch_telemetry",
            sketch_type = "count_min",
            items_processed = 1u64,
            "sketch_update"
        );
    }

    /// Query the estimated frequency of an item.
    ///
    /// Returns the minimum count across all hash rows. This is an upper bound
    /// on the true count (never undercounts).
    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn estimate(&self, item: u64) -> u64 {
        record_estimate();
        let mut min_count = u64::MAX;
        for (row, seed) in self.seeds.iter().enumerate() {
            let hash = mix64(item ^ *seed);
            let col = (hash as usize) % self.width;
            min_count = min_count.min(self.counters[row * self.width + col]);
        }
        min_count
    }

    /// Total number of observations across all items.
    #[must_use]
    pub fn total_count(&self) -> u64 {
        self.total_count
    }

    /// Number of counter columns (width).
    #[must_use]
    pub fn width(&self) -> usize {
        self.width
    }

    /// Number of hash rows (depth).
    #[must_use]
    pub fn depth(&self) -> usize {
        self.depth
    }

    /// Memory footprint in bytes (counters only).
    #[must_use]
    pub fn memory_bytes(&self) -> usize {
        self.counters.len() * 8
    }

    /// Reset all counters to zero.
    pub fn clear(&mut self) {
        self.counters.fill(0);
        self.total_count = 0;
    }
}

impl Drop for CountMinSketch {
    fn drop(&mut self) {
        let mem_bytes = (self.width * self.depth * 8) as u64;
        record_memory_sub(mem_bytes);
    }
}

// ---------------------------------------------------------------------------
// Streaming Histogram
// ---------------------------------------------------------------------------

/// Version marker for streaming histogram evidence logs.
pub const HISTOGRAM_VERSION: &str = "fsqlite:histogram:streaming:v1";

/// Default bucket boundaries for latency histograms (microseconds).
pub const DEFAULT_LATENCY_BUCKETS_US: &[u64] = &[
    1, 5, 10, 25, 50, 100, 250, 500, 1_000, 2_500, 5_000, 10_000, 25_000, 50_000, 100_000, 250_000,
    500_000, 1_000_000,
];

/// Streaming histogram with configurable bucket boundaries.
///
/// Provides O(1) observation and O(buckets) percentile computation with
/// bounded memory. Bucket boundaries are fixed at construction time.
///
/// Each observation falls into the first bucket whose boundary >= the value,
/// or into the overflow bucket if the value exceeds all boundaries.
pub struct StreamingHistogram {
    /// Upper bounds (exclusive) for each bucket. Sorted ascending.
    boundaries: Vec<u64>,
    /// Bucket counts. Length = boundaries.len() + 1 (last is overflow).
    counts: Vec<u64>,
    /// Running sum of all observed values.
    sum: u64,
    /// Total number of observations.
    count: u64,
    /// Minimum observed value.
    min: u64,
    /// Maximum observed value.
    max: u64,
}

impl std::fmt::Debug for StreamingHistogram {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamingHistogram")
            .field("bucket_count", &self.boundaries.len())
            .field("total", &self.count)
            .field("min", &self.min)
            .field("max", &self.max)
            .finish_non_exhaustive()
    }
}

impl StreamingHistogram {
    /// Create a new histogram with the given bucket boundaries (upper bounds).
    ///
    /// Boundaries must be sorted ascending with no duplicates.
    #[must_use]
    pub fn new(boundaries: &[u64]) -> Self {
        debug_assert!(
            boundaries.windows(2).all(|w| w[0] < w[1]),
            "histogram boundaries must be sorted ascending with no duplicates"
        );

        let num_buckets = boundaries.len() + 1;
        let mem_bytes = (num_buckets * 8 + boundaries.len() * 8) as u64;
        record_memory_add(mem_bytes);

        Self {
            boundaries: boundaries.to_vec(),
            counts: vec![0; num_buckets],
            sum: 0,
            count: 0,
            min: u64::MAX,
            max: 0,
        }
    }

    /// Create a histogram with the default latency bucket boundaries (microseconds).
    #[must_use]
    pub fn new_latency_us() -> Self {
        Self::new(DEFAULT_LATENCY_BUCKETS_US)
    }

    /// Record a single observation.
    pub fn observe(&mut self, value: u64) {
        let bucket = match self.boundaries.binary_search(&value) {
            Ok(idx) | Err(idx) => idx,
        };
        // `bucket` is the index of the first boundary >= value, or boundaries.len()
        // for overflow.
        self.counts[bucket] = self.counts[bucket].saturating_add(1);
        self.sum = self.sum.saturating_add(value);
        self.count = self.count.saturating_add(1);
        self.min = self.min.min(value);
        self.max = self.max.max(value);

        record_observation();
        tracing::trace!(
            target: "fsqlite.sketch_telemetry",
            sketch_type = "histogram",
            items_processed = 1u64,
            "sketch_update"
        );
    }

    /// Total number of observations.
    #[must_use]
    pub fn count(&self) -> u64 {
        self.count
    }

    /// Sum of all observed values.
    #[must_use]
    pub fn sum(&self) -> u64 {
        self.sum
    }

    /// Minimum observed value (u64::MAX if no observations).
    #[must_use]
    pub fn min(&self) -> u64 {
        self.min
    }

    /// Maximum observed value.
    #[must_use]
    pub fn max(&self) -> u64 {
        self.max
    }

    /// Mean of all observed values.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn mean(&self) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            self.sum as f64 / self.count as f64
        }
    }

    /// Estimate the p-th percentile (0.0 .. 1.0).
    ///
    /// Uses linear interpolation within buckets. Returns the upper boundary of
    /// the bucket containing the target rank.
    #[must_use]
    #[allow(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss
    )]
    pub fn percentile(&self, p: f64) -> u64 {
        record_estimate();
        if self.count == 0 {
            return 0;
        }

        let target_rank = (p * self.count as f64).ceil() as u64;
        let mut cumulative = 0u64;

        for (i, &cnt) in self.counts.iter().enumerate() {
            cumulative = cumulative.saturating_add(cnt);
            if cumulative >= target_rank {
                // Return the upper boundary of this bucket.
                if i < self.boundaries.len() {
                    return self.boundaries[i];
                }
                // Overflow bucket — return max observed.
                return self.max();
            }
        }

        self.max() // fallback
    }

    /// Number of bucket boundaries (not including overflow).
    #[must_use]
    pub fn bucket_count(&self) -> usize {
        self.boundaries.len()
    }

    /// Get the raw bucket counts (length = boundaries.len() + 1).
    #[must_use]
    pub fn bucket_counts(&self) -> &[u64] {
        &self.counts
    }

    /// Get the bucket boundaries.
    #[must_use]
    pub fn bucket_boundaries(&self) -> &[u64] {
        &self.boundaries
    }

    /// Memory footprint in bytes.
    #[must_use]
    pub fn memory_bytes(&self) -> usize {
        self.counts.len() * 8 + self.boundaries.len() * 8
    }

    /// Reset all counters.
    pub fn clear(&mut self) {
        self.counts.fill(0);
        self.sum = 0;
        self.count = 0;
        self.min = u64::MAX;
        self.max = 0;
    }

    /// Produce a serializable snapshot.
    #[must_use]
    pub fn snapshot(&self) -> HistogramSnapshot {
        HistogramSnapshot {
            boundaries: self.boundaries.clone(),
            counts: self.counts.clone(),
            sum: self.sum,
            count: self.count,
            min: if self.count > 0 { self.min } else { 0 },
            max: self.max,
        }
    }
}

impl Drop for StreamingHistogram {
    fn drop(&mut self) {
        let mem_bytes = (self.counts.len() * 8 + self.boundaries.len() * 8) as u64;
        record_memory_sub(mem_bytes);
    }
}

/// Serializable snapshot of a streaming histogram.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct HistogramSnapshot {
    pub boundaries: Vec<u64>,
    pub counts: Vec<u64>,
    pub sum: u64,
    pub count: u64,
    pub min: u64,
    pub max: u64,
}

// ---------------------------------------------------------------------------
// Sliding window sketches (bd-xox.6)
// ---------------------------------------------------------------------------

/// NitroSketch streaming stats version marker.
pub const NITROSKETCH_STREAMING_VERSION: &str = "fsqlite:nitrosketch:streaming:v1";

/// Configuration for a sliding window sketch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct SlidingWindowConfig {
    /// Number of time slots in the window.
    pub num_slots: usize,
    /// Duration of each slot in microseconds.
    pub slot_duration_us: u64,
}

impl Default for SlidingWindowConfig {
    fn default() -> Self {
        Self {
            num_slots: 10,
            slot_duration_us: 1_000_000, // 1 second per slot = 10 second window
        }
    }
}

/// Sliding window histogram for frame-time / latency distribution.
///
/// Maintains `num_slots` histograms in a ring buffer. Observations are routed
/// to the current slot based on a monotonic timestamp. Stale slots are cleared
/// automatically. Queries aggregate across all active (non-expired) slots.
///
/// Memory: `O(num_slots * num_buckets)`. Amortized update: `O(1)`.
pub struct SlidingWindowHistogram {
    config: SlidingWindowConfig,
    boundaries: Vec<u64>,
    /// Ring buffer of per-slot bucket counts. Each slot has `boundaries.len() + 1` counters.
    slot_counts: Vec<Vec<u64>>,
    /// Per-slot observation count.
    slot_obs: Vec<u64>,
    /// Per-slot sum.
    slot_sum: Vec<u64>,
    /// Per-slot maximum value.
    slot_max: Vec<u64>,
    /// Timestamp (µs) when each slot was last written.
    slot_timestamps: Vec<u64>,
    /// Current head slot index.
    head: usize,
    /// Last known timestamp.
    last_ts: u64,
}

#[allow(clippy::missing_fields_in_debug)]
impl std::fmt::Debug for SlidingWindowHistogram {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SlidingWindowHistogram")
            .field("num_slots", &self.config.num_slots)
            .field("slot_duration_us", &self.config.slot_duration_us)
            .field("num_boundaries", &self.boundaries.len())
            .field("head", &self.head)
            .finish_non_exhaustive()
    }
}

impl SlidingWindowHistogram {
    /// Create a new sliding window histogram.
    #[must_use]
    pub fn new(boundaries: &[u64], config: SlidingWindowConfig) -> Self {
        debug_assert!(config.num_slots > 0, "need at least 1 slot");
        debug_assert!(config.slot_duration_us > 0, "slot_duration must be > 0");

        let num_buckets = boundaries.len() + 1;
        let slot_counts: Vec<Vec<u64>> = (0..config.num_slots)
            .map(|_| vec![0u64; num_buckets])
            .collect();
        let mem_bytes = (config.num_slots * num_buckets * 8
            + config.num_slots * 8 * 4
            + boundaries.len() * 8) as u64;
        record_memory_add(mem_bytes);

        tracing::debug!(
            target: "fsqlite.sketch_telemetry",
            sketch_type = "sliding_window_histogram",
            num_slots = config.num_slots,
            slot_duration_us = config.slot_duration_us,
            memory_bytes = mem_bytes,
            "sketch_created"
        );

        Self {
            config,
            boundaries: boundaries.to_vec(),
            slot_counts,
            slot_obs: vec![0; config.num_slots],
            slot_sum: vec![0; config.num_slots],
            slot_max: vec![0; config.num_slots],
            slot_timestamps: vec![0; config.num_slots],
            head: 0,
            last_ts: 0,
        }
    }

    /// Create a sliding window histogram with default latency buckets.
    #[must_use]
    pub fn new_latency(config: SlidingWindowConfig) -> Self {
        Self::new(DEFAULT_LATENCY_BUCKETS_US, config)
    }

    /// Advance the window to the given timestamp, clearing stale slots.
    #[allow(clippy::cast_possible_truncation)]
    fn advance_to(&mut self, now_us: u64) {
        if now_us <= self.last_ts {
            return;
        }
        let slots_elapsed = ((now_us - self.last_ts) / self.config.slot_duration_us) as usize;
        if slots_elapsed == 0 {
            return;
        }
        let slots_to_clear = slots_elapsed.min(self.config.num_slots);
        for i in 0..slots_to_clear {
            let idx = (self.head + 1 + i) % self.config.num_slots;
            self.slot_counts[idx].fill(0);
            self.slot_obs[idx] = 0;
            self.slot_sum[idx] = 0;
            self.slot_max[idx] = 0;
            self.slot_timestamps[idx] = 0;
        }
        self.head = (self.head + slots_elapsed) % self.config.num_slots;
        self.last_ts = now_us;
    }

    /// Observe a value at the given timestamp (microseconds).
    pub fn observe(&mut self, value: u64, now_us: u64) {
        self.advance_to(now_us);

        let bucket = match self.boundaries.binary_search(&value) {
            Ok(idx) | Err(idx) => idx,
        };
        self.slot_counts[self.head][bucket] = self.slot_counts[self.head][bucket].saturating_add(1);
        self.slot_obs[self.head] = self.slot_obs[self.head].saturating_add(1);
        self.slot_sum[self.head] = self.slot_sum[self.head].saturating_add(value);
        self.slot_max[self.head] = self.slot_max[self.head].max(value);
        self.slot_timestamps[self.head] = now_us;

        record_observation();
        tracing::trace!(
            target: "fsqlite.sketch_telemetry",
            sketch_type = "sliding_window_histogram",
            items_processed = 1u64,
            "nitrosketch.update"
        );
    }

    /// Total observation count across all active slots.
    #[must_use]
    pub fn count(&self) -> u64 {
        self.slot_obs.iter().sum()
    }

    /// Aggregate bucket counts across all active slots.
    #[must_use]
    pub fn aggregate_counts(&self) -> Vec<u64> {
        let num_buckets = self.boundaries.len() + 1;
        let mut agg = vec![0u64; num_buckets];
        for slot in &self.slot_counts {
            for (i, &c) in slot.iter().enumerate() {
                agg[i] = agg[i].saturating_add(c);
            }
        }
        agg
    }

    /// Estimate the p-th percentile across the entire window.
    #[must_use]
    #[allow(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss
    )]
    pub fn percentile(&self, p: f64) -> u64 {
        record_estimate();
        let total = self.count();
        if total == 0 {
            return 0;
        }
        let target_rank = (p * total as f64).ceil() as u64;
        let agg = self.aggregate_counts();
        let mut cumulative = 0u64;
        for (i, &cnt) in agg.iter().enumerate() {
            cumulative = cumulative.saturating_add(cnt);
            if cumulative >= target_rank {
                if i < self.boundaries.len() {
                    return self.boundaries[i];
                }
                // Overflow bucket.
                break;
            }
        }
        self.max() // fallback
    }

    /// Maximum observed value across all active slots.
    #[must_use]
    pub fn max(&self) -> u64 {
        self.slot_max.iter().copied().max().unwrap_or(0)
    }

    /// Mean across all active slots.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn mean(&self) -> f64 {
        let total_obs: u64 = self.slot_obs.iter().sum();
        if total_obs == 0 {
            return 0.0;
        }
        let total_sum: u64 = self.slot_sum.iter().sum();
        total_sum as f64 / total_obs as f64
    }

    /// Number of active (non-empty) slots.
    #[must_use]
    pub fn active_slots(&self) -> usize {
        self.slot_obs.iter().filter(|&&c| c > 0).count()
    }

    /// Produce a serializable snapshot.
    #[must_use]
    pub fn snapshot(&self) -> SlidingWindowHistogramSnapshot {
        SlidingWindowHistogramSnapshot {
            boundaries: self.boundaries.clone(),
            aggregate_counts: self.aggregate_counts(),
            count: self.count(),
            mean: self.mean(),
            active_slots: self.active_slots(),
            total_slots: self.config.num_slots,
        }
    }

    /// Memory footprint in bytes.
    #[must_use]
    pub fn memory_bytes(&self) -> usize {
        self.config.num_slots * (self.boundaries.len() + 1) * 8
            + self.config.num_slots * 8 * 4
            + self.boundaries.len() * 8
    }
}

impl Drop for SlidingWindowHistogram {
    fn drop(&mut self) {
        record_memory_sub(self.memory_bytes() as u64);
    }
}

/// Serializable snapshot of a sliding window histogram.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SlidingWindowHistogramSnapshot {
    pub boundaries: Vec<u64>,
    pub aggregate_counts: Vec<u64>,
    pub count: u64,
    pub mean: f64,
    pub active_slots: usize,
    pub total_slots: usize,
}

// ---------------------------------------------------------------------------
// Sliding window Count-Min Sketch
// ---------------------------------------------------------------------------

/// Sliding window CMS for event frequency estimation over a time window.
///
/// Maintains `num_slots` CMS instances in a ring buffer, advancing with time.
/// Frequency queries aggregate across all active slots.
pub struct SlidingWindowCms {
    config: SlidingWindowConfig,
    /// Ring buffer of per-slot counter matrices (flattened: row * width + col).
    slot_counters: Vec<Vec<u64>>,
    /// Per-slot total count.
    slot_totals: Vec<u64>,
    slot_timestamps: Vec<u64>,
    head: usize,
    last_ts: u64,
    width: usize,
    depth: usize,
    seeds: Vec<u64>,
}

#[allow(clippy::missing_fields_in_debug)]
impl std::fmt::Debug for SlidingWindowCms {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SlidingWindowCms")
            .field("width", &self.width)
            .field("depth", &self.depth)
            .field("num_slots", &self.config.num_slots)
            .finish_non_exhaustive()
    }
}

impl SlidingWindowCms {
    /// Create a new sliding window CMS.
    #[must_use]
    pub fn new(cms_config: CountMinSketchConfig, window_config: SlidingWindowConfig) -> Self {
        let n = window_config.num_slots;
        let w = cms_config.width;
        let d = cms_config.depth;

        // Derive hash seeds (same logic as CountMinSketch).
        let seeds: Vec<u64> = (0..d)
            .map(|i| {
                let mut hasher = blake3::Hasher::new();
                hasher.update(CMS_VERSION.as_bytes());
                hasher.update(&cms_config.seed.to_le_bytes());
                hasher.update(&(i as u64).to_le_bytes());
                let hash = hasher.finalize();
                let bytes: [u8; 8] = hash.as_bytes()[..8].try_into().expect("8 bytes");
                u64::from_le_bytes(bytes)
            })
            .collect();

        let slot_counters: Vec<Vec<u64>> = (0..n).map(|_| vec![0u64; w * d]).collect();
        let mem_bytes = (n * w * d * 8 + n * 8 * 2) as u64;
        record_memory_add(mem_bytes);

        tracing::debug!(
            target: "fsqlite.sketch_telemetry",
            sketch_type = "sliding_window_cms",
            num_slots = n,
            width = w,
            depth = d,
            memory_bytes = mem_bytes,
            "sketch_created"
        );

        Self {
            config: window_config,
            slot_counters,
            slot_totals: vec![0; n],
            slot_timestamps: vec![0; n],
            head: 0,
            last_ts: 0,
            width: w,
            depth: d,
            seeds,
        }
    }

    #[allow(clippy::cast_possible_truncation)]
    fn advance_to(&mut self, now_us: u64) {
        if now_us <= self.last_ts {
            return;
        }
        let slots_elapsed = ((now_us - self.last_ts) / self.config.slot_duration_us) as usize;
        if slots_elapsed == 0 {
            return;
        }
        let slots_to_clear = slots_elapsed.min(self.config.num_slots);
        for i in 0..slots_to_clear {
            let idx = (self.head + 1 + i) % self.config.num_slots;
            self.slot_counters[idx].fill(0);
            self.slot_totals[idx] = 0;
            self.slot_timestamps[idx] = 0;
        }
        self.head = (self.head + slots_elapsed) % self.config.num_slots;
        self.last_ts = now_us;
    }

    #[allow(clippy::cast_possible_truncation)]
    fn hash_to_col(&self, row: usize, item: u64) -> usize {
        (mix64(item ^ self.seeds[row]) as usize) % self.width
    }

    /// Observe an event at the given timestamp.
    pub fn observe(&mut self, item: u64, now_us: u64) {
        self.observe_n(item, 1, now_us);
    }

    /// Observe an event N times at the given timestamp.
    pub fn observe_n(&mut self, item: u64, count: u64, now_us: u64) {
        self.advance_to(now_us);

        for row in 0..self.depth {
            let col = self.hash_to_col(row, item);
            self.slot_counters[self.head][row * self.width + col] =
                self.slot_counters[self.head][row * self.width + col].saturating_add(count);
        }
        self.slot_totals[self.head] = self.slot_totals[self.head].saturating_add(count);

        record_observation();
    }

    /// Estimate the frequency of an item across the entire window.
    #[must_use]
    pub fn estimate(&self, item: u64) -> u64 {
        record_estimate();
        let mut min_est = u64::MAX;
        for row in 0..self.depth {
            let col = self.hash_to_col(row, item);
            let mut row_sum = 0u64;
            for slot in &self.slot_counters {
                row_sum = row_sum.saturating_add(slot[row * self.width + col]);
            }
            min_est = min_est.min(row_sum);
        }
        min_est
    }

    /// Total observations across all active slots.
    #[must_use]
    pub fn total_count(&self) -> u64 {
        self.slot_totals.iter().sum()
    }

    /// Number of active (non-empty) slots.
    #[must_use]
    pub fn active_slots(&self) -> usize {
        self.slot_totals.iter().filter(|&&c| c > 0).count()
    }

    /// Memory footprint in bytes.
    #[must_use]
    pub fn memory_bytes(&self) -> usize {
        self.config.num_slots * self.width * self.depth * 8 + self.config.num_slots * 8 * 2
    }
}

impl Drop for SlidingWindowCms {
    fn drop(&mut self) {
        record_memory_sub(self.memory_bytes() as u64);
    }
}

// ---------------------------------------------------------------------------
// Memory allocation tracker
// ---------------------------------------------------------------------------

/// Tracks memory allocation events with sketch-based profiling.
///
/// Records allocation sizes in a histogram and allocation sites (by caller ID)
/// in a CMS for frequency estimation. Provides aggregate stats without storing
/// individual allocations.
pub struct MemoryAllocationTracker {
    /// Histogram of allocation sizes.
    size_histogram: StreamingHistogram,
    /// CMS tracking allocation frequency by caller/site ID.
    site_frequency: CountMinSketch,
    /// Running totals.
    total_allocated: u64,
    total_freed: u64,
    alloc_count: u64,
    free_count: u64,
}

/// Default allocation size histogram boundaries (bytes).
pub const DEFAULT_ALLOC_SIZE_BUCKETS: &[u64] = &[
    16, 32, 64, 128, 256, 512, 1024, 4096, 16384, 65536, 262_144, 1_048_576,
];

#[allow(clippy::missing_fields_in_debug)]
impl std::fmt::Debug for MemoryAllocationTracker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryAllocationTracker")
            .field("total_allocated", &self.total_allocated)
            .field("total_freed", &self.total_freed)
            .field("alloc_count", &self.alloc_count)
            .field("free_count", &self.free_count)
            .finish_non_exhaustive()
    }
}

impl MemoryAllocationTracker {
    /// Create a new allocation tracker with default settings.
    #[must_use]
    pub fn new() -> Self {
        Self::with_config(
            DEFAULT_ALLOC_SIZE_BUCKETS,
            &CountMinSketchConfig {
                width: 1024,
                depth: 4,
                seed: 0xA110C,
            },
        )
    }

    /// Create with custom settings.
    #[must_use]
    pub fn with_config(size_buckets: &[u64], cms_config: &CountMinSketchConfig) -> Self {
        tracing::debug!(
            target: "fsqlite.sketch_telemetry",
            sketch_type = "memory_allocation_tracker",
            "sketch_created"
        );
        Self {
            size_histogram: StreamingHistogram::new(size_buckets),
            site_frequency: CountMinSketch::new(cms_config),
            total_allocated: 0,
            total_freed: 0,
            alloc_count: 0,
            free_count: 0,
        }
    }

    /// Record an allocation event.
    ///
    /// `site_id`: a hash or identifier for the allocation call site.
    /// `size`: number of bytes allocated.
    pub fn record_alloc(&mut self, site_id: u64, size: u64) {
        self.size_histogram.observe(size);
        self.site_frequency.observe(site_id);
        self.total_allocated = self.total_allocated.saturating_add(size);
        self.alloc_count += 1;
    }

    /// Record a deallocation event.
    pub fn record_free(&mut self, size: u64) {
        self.total_freed = self.total_freed.saturating_add(size);
        self.free_count += 1;
    }

    /// Current live memory (allocated - freed).
    #[must_use]
    pub fn live_bytes(&self) -> u64 {
        self.total_allocated.saturating_sub(self.total_freed)
    }

    /// Total bytes ever allocated.
    #[must_use]
    pub fn total_allocated(&self) -> u64 {
        self.total_allocated
    }

    /// Total bytes ever freed.
    #[must_use]
    pub fn total_freed(&self) -> u64 {
        self.total_freed
    }

    /// Total allocation events.
    #[must_use]
    pub fn alloc_count(&self) -> u64 {
        self.alloc_count
    }

    /// Total free events.
    #[must_use]
    pub fn free_count(&self) -> u64 {
        self.free_count
    }

    /// Estimate allocation frequency for a given call site.
    #[must_use]
    pub fn site_frequency(&self, site_id: u64) -> u64 {
        self.site_frequency.estimate(site_id)
    }

    /// Get the allocation size distribution percentile.
    #[must_use]
    pub fn size_percentile(&self, p: f64) -> u64 {
        self.size_histogram.percentile(p)
    }

    /// Produce a serializable snapshot.
    #[must_use]
    pub fn snapshot(&self) -> MemoryTrackerSnapshot {
        MemoryTrackerSnapshot {
            total_allocated: self.total_allocated,
            total_freed: self.total_freed,
            live_bytes: self.live_bytes(),
            alloc_count: self.alloc_count,
            free_count: self.free_count,
            size_distribution: self.size_histogram.snapshot(),
        }
    }
}

impl Default for MemoryAllocationTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// Serializable snapshot of memory allocation tracking.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MemoryTrackerSnapshot {
    pub total_allocated: u64,
    pub total_freed: u64,
    pub live_bytes: u64,
    pub alloc_count: u64,
    pub free_count: u64,
    pub size_distribution: HistogramSnapshot,
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- Count-Min Sketch tests --

    #[test]
    fn test_cms_basic_frequency() {
        let mut cms = CountMinSketch::new(&CountMinSketchConfig::default());
        cms.observe(42);
        cms.observe(42);
        cms.observe(42);
        cms.observe(99);

        assert_eq!(cms.estimate(42), 3);
        assert_eq!(cms.estimate(99), 1);
        assert_eq!(cms.total_count(), 4);

        println!("[PASS] CMS basic frequency: observe/estimate correct");
    }

    #[test]
    fn test_cms_never_undercounts() {
        let mut cms = CountMinSketch::new(&CountMinSketchConfig {
            width: 128,
            depth: 4,
            seed: 0xDEAD,
        });

        for i in 0..1000u64 {
            cms.observe_n(i, i + 1);
        }

        for i in 0..1000u64 {
            let estimate = cms.estimate(i);
            let true_count = i + 1;
            assert!(
                estimate >= true_count,
                "CMS undercount for item {i}: estimate={estimate} true={true_count}"
            );
        }

        println!("[PASS] CMS never undercounts: 1000 items verified");
    }

    #[test]
    fn test_cms_heavy_hitter_accuracy() {
        let mut cms = CountMinSketch::new(&CountMinSketchConfig {
            width: 2048,
            depth: 4,
            seed: 0,
        });

        // Insert one heavy hitter and many light items.
        cms.observe_n(1, 10_000);
        for i in 2..=100 {
            cms.observe(i);
        }

        let heavy_est = cms.estimate(1);
        assert_eq!(
            heavy_est, 10_000,
            "heavy hitter should be exact in a wide sketch"
        );

        let light_est = cms.estimate(50);
        assert!(
            light_est <= 5,
            "light item should have low overcount, got {light_est}"
        );

        println!("[PASS] CMS heavy hitter accuracy: exact at width=2048");
    }

    #[test]
    fn test_cms_clear() {
        let mut cms = CountMinSketch::new(&CountMinSketchConfig::default());
        cms.observe(1);
        cms.observe(2);
        assert_eq!(cms.total_count(), 2);

        cms.clear();
        assert_eq!(cms.total_count(), 0);
        assert_eq!(cms.estimate(1), 0);
        assert_eq!(cms.estimate(2), 0);

        println!("[PASS] CMS clear: counters zeroed");
    }

    #[test]
    fn test_cms_memory_bytes() {
        let cms = CountMinSketch::new(&CountMinSketchConfig {
            width: 256,
            depth: 4,
            seed: 0,
        });
        assert_eq!(cms.memory_bytes(), 256 * 4 * 8);

        println!("[PASS] CMS memory_bytes: {} bytes", cms.memory_bytes());
    }

    // -- Streaming Histogram tests --

    #[test]
    fn test_histogram_basic() {
        let mut h = StreamingHistogram::new(&[10, 50, 100, 500, 1000]);

        h.observe(5);
        h.observe(25);
        h.observe(75);
        h.observe(200);
        h.observe(999);
        h.observe(5000);

        assert_eq!(h.count(), 6);
        assert_eq!(h.min(), 5);
        assert_eq!(h.max(), 5000);
        assert_eq!(h.sum(), 5 + 25 + 75 + 200 + 999 + 5000);

        println!("[PASS] histogram basic: count/min/max/sum correct");
    }

    #[test]
    fn test_histogram_percentiles() {
        let mut h = StreamingHistogram::new(&[10, 20, 30, 40, 50]);

        // 100 observations, 20 per bucket: [<=10, <=20, <=30, <=40, <=50]
        for _ in 0..20 {
            h.observe(5);
        }
        for _ in 0..20 {
            h.observe(15);
        }
        for _ in 0..20 {
            h.observe(25);
        }
        for _ in 0..20 {
            h.observe(35);
        }
        for _ in 0..20 {
            h.observe(45);
        }

        assert_eq!(h.count(), 100);

        // p50 should be at the boundary of the 3rd bucket (30).
        let p50 = h.percentile(0.50);
        assert_eq!(p50, 30, "p50 should be 30");

        // p90 should be at the boundary of the 5th bucket (50).
        let p90 = h.percentile(0.90);
        assert_eq!(p90, 50, "p90 should be 50");

        // p10 should be at the boundary of the 1st bucket (10).
        let p10 = h.percentile(0.10);
        assert_eq!(p10, 10, "p10 should be 10");

        println!("[PASS] histogram percentiles: p10={p10} p50={p50} p90={p90}");
    }

    #[test]
    fn test_histogram_overflow_bucket() {
        let mut h = StreamingHistogram::new(&[10, 100]);

        h.observe(5); // bucket 0 (<=10)
        h.observe(50); // bucket 1 (<=100)
        h.observe(999); // bucket 2 (overflow)

        assert_eq!(h.bucket_counts(), &[1, 1, 1]);
        assert_eq!(h.percentile(0.99), 999);

        println!("[PASS] histogram overflow: overflow bucket captures >max_boundary");
    }

    #[test]
    fn test_histogram_latency_default() {
        let mut h = StreamingHistogram::new_latency_us();
        h.observe(1);
        h.observe(100);
        h.observe(10_000);
        h.observe(1_000_000);

        assert_eq!(h.count(), 4);
        assert_eq!(h.bucket_count(), 18);

        println!(
            "[PASS] histogram default latency: 18 buckets, {} obs",
            h.count()
        );
    }

    #[test]
    fn test_histogram_snapshot_serialization() {
        let mut h = StreamingHistogram::new(&[100, 500, 1000]);
        h.observe(50);
        h.observe(200);
        h.observe(999);

        let snap = h.snapshot();
        let json = serde_json::to_string(&snap).unwrap();
        assert!(json.contains("\"count\":3"));
        assert!(json.contains("\"boundaries\":[100,500,1000]"));

        println!("[PASS] histogram snapshot serialization");
    }

    #[test]
    fn test_histogram_clear() {
        let mut h = StreamingHistogram::new(&[10, 100]);
        h.observe(5);
        h.observe(50);
        assert_eq!(h.count(), 2);

        h.clear();
        assert_eq!(h.count(), 0);
        assert_eq!(h.sum(), 0);
        assert_eq!(h.min(), u64::MAX);
        assert_eq!(h.max(), 0);

        println!("[PASS] histogram clear: all counters zeroed");
    }

    // -- Global metrics tests --

    #[test]
    fn test_global_metrics() {
        // Delta-based: snapshot before, act, snapshot after.
        let before = sketch_telemetry_metrics();

        let mut cms = CountMinSketch::new(&CountMinSketchConfig {
            width: 64,
            depth: 2,
            seed: 0,
        });
        cms.observe(1);
        cms.observe(2);
        _ = cms.estimate(1);

        let after = sketch_telemetry_metrics();
        assert!(
            after.fsqlite_sketch_memory_bytes > 0,
            "memory gauge should be > 0"
        );
        let obs_delta =
            after.fsqlite_sketch_observations_total - before.fsqlite_sketch_observations_total;
        let est_delta =
            after.fsqlite_sketch_estimates_total - before.fsqlite_sketch_estimates_total;
        assert!(
            obs_delta >= 2,
            "expected at least 2 observations, got {obs_delta}"
        );
        assert!(
            est_delta >= 1,
            "expected at least 1 estimate, got {est_delta}"
        );

        println!(
            "[PASS] global metrics: mem={} obs_delta={} est_delta={}",
            after.fsqlite_sketch_memory_bytes, obs_delta, est_delta
        );
    }

    #[test]
    fn test_memory_tracking_on_drop() {
        // The live gauge is process-global, so another parallel test may
        // allocate or free a sketch between any two snapshots. Assert against
        // monotonic event totals instead: unrelated events can only increase
        // the observed delta and therefore cannot mask this instance's exact
        // allocation/free contribution.
        let allocated_before = FSQLITE_SKETCH_TEST_ALLOCATED_BYTES_TOTAL.load(Ordering::Relaxed);
        let cms = CountMinSketch::new(&CountMinSketchConfig {
            width: 128,
            depth: 2,
            seed: 0,
        });
        let own_bytes = cms.memory_bytes() as u64;
        let allocated_after = FSQLITE_SKETCH_TEST_ALLOCATED_BYTES_TOTAL.load(Ordering::Relaxed);
        assert!(
            allocated_after.saturating_sub(allocated_before) >= own_bytes,
            "allocation accounting must include this sketch's {own_bytes} bytes"
        );

        let freed_before = FSQLITE_SKETCH_TEST_FREED_BYTES_TOTAL.load(Ordering::Relaxed);
        drop(cms);
        let freed_after = FSQLITE_SKETCH_TEST_FREED_BYTES_TOTAL.load(Ordering::Relaxed);
        assert!(
            freed_after.saturating_sub(freed_before) >= own_bytes,
            "drop accounting must release this sketch's {own_bytes} bytes"
        );

        println!("[PASS] memory tracking on drop: allocation and free events accounted");
    }

    // -- Sliding Window Histogram tests --

    #[test]
    fn test_sliding_window_histogram_basic() {
        let config = SlidingWindowConfig {
            num_slots: 4,
            slot_duration_us: 1_000_000,
        };
        let mut swh = SlidingWindowHistogram::new(&[10, 50, 100, 500], config);

        // All observations in the same time slot.
        swh.observe(5, 1_000_000);
        swh.observe(25, 1_000_000);
        swh.observe(75, 1_000_000);
        swh.observe(200, 1_000_000);

        assert_eq!(swh.count(), 4);
        assert_eq!(swh.active_slots(), 1);

        println!(
            "[PASS] sliding_window_histogram basic: count={} active_slots={}",
            swh.count(),
            swh.active_slots()
        );
    }

    #[test]
    fn test_sliding_window_histogram_advance() {
        let config = SlidingWindowConfig {
            num_slots: 3,
            slot_duration_us: 1_000_000,
        };
        let mut swh = SlidingWindowHistogram::new(&[10, 100], config);

        // Slot 0: t=1s
        swh.observe(5, 1_000_000);
        swh.observe(50, 1_000_000);
        assert_eq!(swh.count(), 2);

        // Slot 1: t=2s
        swh.observe(5, 2_000_000);
        assert_eq!(swh.count(), 3);
        assert_eq!(swh.active_slots(), 2);

        // Slot 2: t=3s
        swh.observe(200, 3_000_000);
        assert_eq!(swh.count(), 4);
        assert_eq!(swh.active_slots(), 3);

        println!("[PASS] sliding_window_histogram advance: slots fill across time");
    }

    #[test]
    fn test_sliding_window_histogram_expiry() {
        let config = SlidingWindowConfig {
            num_slots: 3,
            slot_duration_us: 1_000_000,
        };
        let mut swh = SlidingWindowHistogram::new(&[10, 100], config);

        // Fill slot at t=1s
        swh.observe(5, 1_000_000);
        swh.observe(50, 1_000_000);

        // Jump ahead by 4 slots — all 3 slots should be cleared.
        swh.observe(99, 5_000_000);
        assert_eq!(swh.count(), 1, "old observations should be expired");
        assert_eq!(swh.active_slots(), 1);

        println!("[PASS] sliding_window_histogram expiry: stale slots cleared");
    }

    #[test]
    fn test_sliding_window_histogram_percentile() {
        let config = SlidingWindowConfig {
            num_slots: 2,
            slot_duration_us: 1_000_000,
        };
        let mut swh = SlidingWindowHistogram::new(&[10, 20, 30, 40, 50], config);

        // 50 observations across 2 time slots.
        for _ in 0..10 {
            swh.observe(5, 1_000_000);
        }
        for _ in 0..10 {
            swh.observe(15, 1_000_000);
        }
        for _ in 0..10 {
            swh.observe(25, 2_000_000);
        }
        for _ in 0..10 {
            swh.observe(35, 2_000_000);
        }
        for _ in 0..10 {
            swh.observe(45, 2_000_000);
        }

        assert_eq!(swh.count(), 50);
        let p50 = swh.percentile(0.50);
        assert_eq!(p50, 30, "p50 should be 30");

        println!("[PASS] sliding_window_histogram percentile: p50={p50}");
    }

    #[test]
    fn test_sliding_window_histogram_mean() {
        let config = SlidingWindowConfig {
            num_slots: 2,
            slot_duration_us: 1_000_000,
        };
        let mut swh = SlidingWindowHistogram::new(&[100], config);

        swh.observe(10, 1_000_000);
        swh.observe(30, 1_000_000);
        swh.observe(20, 2_000_000);

        let mean = swh.mean();
        assert!(
            (mean - 20.0).abs() < 0.001,
            "mean should be 20.0, got {mean}"
        );

        println!("[PASS] sliding_window_histogram mean: {mean}");
    }

    #[test]
    fn test_sliding_window_histogram_snapshot() {
        let config = SlidingWindowConfig {
            num_slots: 2,
            slot_duration_us: 1_000_000,
        };
        let mut swh = SlidingWindowHistogram::new(&[100, 500], config);
        swh.observe(50, 1_000_000);
        swh.observe(200, 2_000_000);

        let snap = swh.snapshot();
        assert_eq!(snap.count, 2);
        assert_eq!(snap.active_slots, 2);
        assert_eq!(snap.total_slots, 2);

        let json = serde_json::to_string(&snap).unwrap();
        assert!(json.contains("\"count\":2"));

        println!("[PASS] sliding_window_histogram snapshot serialization");
    }

    // -- Sliding Window CMS tests --

    #[test]
    fn test_sliding_window_cms_basic() {
        let cms_config = CountMinSketchConfig {
            width: 256,
            depth: 4,
            seed: 0,
        };
        let win_config = SlidingWindowConfig {
            num_slots: 4,
            slot_duration_us: 1_000_000,
        };
        let mut swcms = SlidingWindowCms::new(cms_config, win_config);

        swcms.observe(42, 1_000_000);
        swcms.observe(42, 1_000_000);
        swcms.observe(42, 1_000_000);
        swcms.observe(99, 1_000_000);

        assert_eq!(swcms.estimate(42), 3);
        assert_eq!(swcms.estimate(99), 1);
        assert_eq!(swcms.total_count(), 4);

        println!("[PASS] sliding_window_cms basic: frequency correct");
    }

    #[test]
    fn test_sliding_window_cms_expiry() {
        let cms_config = CountMinSketchConfig {
            width: 256,
            depth: 4,
            seed: 0,
        };
        let win_config = SlidingWindowConfig {
            num_slots: 3,
            slot_duration_us: 1_000_000,
        };
        let mut swcms = SlidingWindowCms::new(cms_config, win_config);

        // Observe at t=1s
        swcms.observe(42, 1_000_000);
        swcms.observe(42, 1_000_000);
        assert_eq!(swcms.estimate(42), 2);

        // Jump ahead by 4 slots — all should be expired.
        swcms.observe(99, 5_000_000);
        assert_eq!(swcms.estimate(42), 0, "item 42 should be expired");
        assert_eq!(swcms.estimate(99), 1);
        assert_eq!(swcms.total_count(), 1);

        println!("[PASS] sliding_window_cms expiry: stale data cleared");
    }

    #[test]
    fn test_sliding_window_cms_multi_slot() {
        let cms_config = CountMinSketchConfig {
            width: 256,
            depth: 4,
            seed: 0,
        };
        let win_config = SlidingWindowConfig {
            num_slots: 4,
            slot_duration_us: 1_000_000,
        };
        let mut swcms = SlidingWindowCms::new(cms_config, win_config);

        // Spread observations across 3 slots.
        swcms.observe(42, 1_000_000);
        swcms.observe(42, 2_000_000);
        swcms.observe(42, 3_000_000);

        assert_eq!(swcms.estimate(42), 3, "should aggregate across slots");
        assert_eq!(swcms.active_slots(), 3);

        println!("[PASS] sliding_window_cms multi-slot aggregation");
    }

    #[test]
    fn test_sliding_window_cms_never_undercounts() {
        let cms_config = CountMinSketchConfig {
            width: 128,
            depth: 4,
            seed: 0xBEEF,
        };
        let win_config = SlidingWindowConfig {
            num_slots: 2,
            slot_duration_us: 1_000_000,
        };
        let mut swcms = SlidingWindowCms::new(cms_config, win_config);

        for i in 0..200u64 {
            swcms.observe_n(i, i + 1, 1_000_000);
        }

        for i in 0..200u64 {
            let est = swcms.estimate(i);
            let true_count = i + 1;
            assert!(
                est >= true_count,
                "SlidingWindowCms undercount for item {i}: est={est} true={true_count}"
            );
        }

        println!("[PASS] sliding_window_cms never undercounts: 200 items verified");
    }

    // -- Memory Allocation Tracker tests --

    #[test]
    fn test_memory_tracker_basic() {
        let mut tracker = MemoryAllocationTracker::new();

        tracker.record_alloc(1, 1024);
        tracker.record_alloc(2, 4096);
        tracker.record_alloc(1, 512);
        tracker.record_free(1024);

        assert_eq!(tracker.alloc_count(), 3);
        assert_eq!(tracker.free_count(), 1);
        assert_eq!(tracker.total_allocated(), 1024 + 4096 + 512);
        assert_eq!(tracker.total_freed(), 1024);
        assert_eq!(tracker.live_bytes(), 4096 + 512);

        println!("[PASS] memory_tracker basic: alloc/free/live correct");
    }

    #[test]
    fn test_memory_tracker_site_frequency() {
        let mut tracker = MemoryAllocationTracker::new();

        tracker.record_alloc(100, 64);
        tracker.record_alloc(100, 128);
        tracker.record_alloc(100, 256);
        tracker.record_alloc(200, 64);

        let freq_100 = tracker.site_frequency(100);
        let freq_200 = tracker.site_frequency(200);
        assert_eq!(freq_100, 3, "site 100 should have frequency 3");
        assert_eq!(freq_200, 1, "site 200 should have frequency 1");

        println!("[PASS] memory_tracker site frequency: {freq_100} / {freq_200}");
    }

    #[test]
    fn test_memory_tracker_size_percentile() {
        let mut tracker = MemoryAllocationTracker::new();

        // Record many small allocations and a few large ones.
        for i in 0..80 {
            tracker.record_alloc(i, 32);
        }
        for i in 80..100 {
            tracker.record_alloc(i, 65536);
        }

        let p50 = tracker.size_percentile(0.50);
        let p99 = tracker.size_percentile(0.99);

        // p50 should land in a small bucket, p99 in a large one.
        assert!(p50 <= 64, "p50 should be small, got {p50}");
        assert!(p99 >= 16384, "p99 should be large, got {p99}");

        println!("[PASS] memory_tracker size percentile: p50={p50} p99={p99}");
    }

    #[test]
    fn test_memory_tracker_snapshot() {
        let mut tracker = MemoryAllocationTracker::new();
        tracker.record_alloc(1, 512);
        tracker.record_alloc(2, 1024);
        tracker.record_free(256);

        let snap = tracker.snapshot();
        assert_eq!(snap.total_allocated, 1536);
        assert_eq!(snap.total_freed, 256);
        assert_eq!(snap.live_bytes, 1280);
        assert_eq!(snap.alloc_count, 2);
        assert_eq!(snap.free_count, 1);

        let json = serde_json::to_string(&snap).unwrap();
        assert!(json.contains("\"live_bytes\":1280"));

        println!("[PASS] memory_tracker snapshot serialization");
    }

    #[test]
    fn test_memory_tracker_default() {
        let tracker = MemoryAllocationTracker::default();
        assert_eq!(tracker.alloc_count(), 0);
        assert_eq!(tracker.live_bytes(), 0);

        println!("[PASS] memory_tracker default construction");
    }

    #[test]
    fn test_sliding_window_histogram_memory_bytes() {
        let config = SlidingWindowConfig {
            num_slots: 4,
            slot_duration_us: 1_000_000,
        };
        let swh = SlidingWindowHistogram::new(&[10, 50, 100], config);
        let mem = swh.memory_bytes();
        // 4 slots * 4 buckets * 8 bytes + 4 slots * 4 vecs * 8 bytes + 3 boundaries * 8 bytes
        let expected = 4 * 4 * 8 + 4 * 8 * 4 + 3 * 8;
        assert_eq!(
            mem, expected,
            "memory_bytes should be {expected}, got {mem}"
        );

        println!("[PASS] sliding_window_histogram memory_bytes: {mem}");
    }

    #[test]
    fn test_sliding_window_cms_memory_bytes() {
        let cms_config = CountMinSketchConfig {
            width: 128,
            depth: 2,
            seed: 0,
        };
        let win_config = SlidingWindowConfig {
            num_slots: 3,
            slot_duration_us: 1_000_000,
        };
        let swcms = SlidingWindowCms::new(cms_config, win_config);
        let mem = swcms.memory_bytes();
        // 3 slots * 128 * 2 * 8 + 3 * 8 * 2
        let expected = 3 * 128 * 2 * 8 + 3 * 8 * 2;
        assert_eq!(
            mem, expected,
            "memory_bytes should be {expected}, got {mem}"
        );

        println!("[PASS] sliding_window_cms memory_bytes: {mem}");
    }
}
