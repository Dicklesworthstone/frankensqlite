//! Learned Index Structures for static lookup (§8.4)
//!
//! Implements a Piecewise Linear Approximation (PLA) index inspired by
//! Kraska et al. 2018. For sorted arrays, a learned model predicts the
//! position of a key, replacing B-tree traversal with model inference +
//! bounded local search.
//!
//! # Design
//!
//! - `LearnedIndex<K>` stores a sorted array of keys plus a piecewise
//!   linear model trained from the key distribution.
//! - Each segment covers a contiguous range of keys and uses a linear
//!   function (slope, intercept) to predict position.
//! - Lookup: binary search segments by key range, predict position,
//!   then bounded linear scan within [pos - max_error, pos + max_error].
//! - Training splits the key space into segments whenever the linear
//!   approximation error exceeds `max_error`.

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

// ── Metrics ──────────────────────────────────────────────────────────────

static LEARNED_INDEX_LOOKUPS_TOTAL: AtomicU64 = AtomicU64::new(0);
static LEARNED_INDEX_PREDICTION_ERROR_TOTAL: AtomicU64 = AtomicU64::new(0);
static LEARNED_INDEX_SEGMENTS_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Snapshot of learned index metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct LearnedIndexMetricsSnapshot {
    /// Total number of lookups performed.
    pub lookups_total: u64,
    /// Sum of prediction errors across all lookups.
    pub prediction_error_total: u64,
    /// Total number of segments across all trained models.
    pub segments_total: u64,
}

impl fmt::Display for LearnedIndexMetricsSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "lookups={} prediction_error_total={} segments={}",
            self.lookups_total, self.prediction_error_total, self.segments_total,
        )
    }
}

/// Return a snapshot of learned index metrics.
#[must_use]
pub fn learned_index_metrics_snapshot() -> LearnedIndexMetricsSnapshot {
    LearnedIndexMetricsSnapshot {
        lookups_total: LEARNED_INDEX_LOOKUPS_TOTAL.load(Ordering::Relaxed),
        prediction_error_total: LEARNED_INDEX_PREDICTION_ERROR_TOTAL.load(Ordering::Relaxed),
        segments_total: LEARNED_INDEX_SEGMENTS_TOTAL.load(Ordering::Relaxed),
    }
}

/// Reset learned index metrics.
pub fn reset_learned_index_metrics() {
    LEARNED_INDEX_LOOKUPS_TOTAL.store(0, Ordering::Relaxed);
    LEARNED_INDEX_PREDICTION_ERROR_TOTAL.store(0, Ordering::Relaxed);
    LEARNED_INDEX_SEGMENTS_TOTAL.store(0, Ordering::Relaxed);
}

fn record_lookup(error: usize) {
    LEARNED_INDEX_LOOKUPS_TOTAL.fetch_add(1, Ordering::Relaxed);
    LEARNED_INDEX_PREDICTION_ERROR_TOTAL.fetch_add(error as u64, Ordering::Relaxed);
}

// ── PiecewiseLinearModel ─────────────────────────────────────────────────

/// A single linear segment mapping keys to predicted positions.
#[derive(Debug, Clone)]
struct Segment {
    /// First key in the segment (inclusive).
    key_lo: u64,
    /// Last key in the segment (inclusive).
    key_hi: u64,
    /// Position of key_lo in the sorted array.
    #[allow(dead_code)]
    pos_lo: usize,
    /// Slope: (delta_pos) / (delta_key). Stored as f64 for precision.
    slope: f64,
    /// Intercept: position of key_lo.
    intercept: f64,
}

impl Segment {
    /// Predict the position of a key using the linear model.
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    fn predict(&self, key: u64) -> usize {
        let delta = key as f64 - self.key_lo as f64;
        let predicted = self.slope.mul_add(delta, self.intercept);
        predicted.round().max(0.0) as usize
    }
}

/// Configuration for building a learned index.
#[derive(Debug, Clone, Copy)]
pub struct LearnedIndexConfig {
    /// Maximum allowed prediction error (in positions).
    /// When the linear approximation error exceeds this threshold,
    /// a new segment is started.
    pub max_error: usize,
}

impl Default for LearnedIndexConfig {
    fn default() -> Self {
        Self { max_error: 16 }
    }
}

/// A learned index over a sorted array of u64 keys.
///
/// Uses piecewise linear approximation to predict key positions,
/// then performs bounded local search for exact lookup.
pub struct LearnedIndex {
    /// The sorted key array.
    keys: Vec<u64>,
    /// Piecewise linear model segments.
    segments: Vec<Segment>,
    /// Max error bound used during training.
    max_error: usize,
}

impl LearnedIndex {
    /// Build a learned index from a sorted slice of keys.
    ///
    /// # Panics
    ///
    /// Panics if `keys` is not sorted.
    pub fn build(keys: &[u64], config: LearnedIndexConfig) -> Self {
        assert!(keys.windows(2).all(|w| w[0] <= w[1]), "keys must be sorted");

        let segments = train_piecewise_linear(keys, config.max_error);

        LEARNED_INDEX_SEGMENTS_TOTAL.fetch_add(segments.len() as u64, Ordering::Relaxed);

        Self {
            keys: keys.to_vec(),
            segments,
            max_error: config.max_error,
        }
    }

    /// Look up a key, returning its index in the sorted array if found.
    pub fn lookup(&self, key: u64) -> Option<usize> {
        if self.keys.is_empty() {
            record_lookup(0);
            return None;
        }

        // Find the segment containing this key.
        let Some(seg_idx) = self.find_segment(key) else {
            record_lookup(0);
            return None;
        };
        let seg = &self.segments[seg_idx];

        // Predict position.
        let predicted = seg.predict(key);

        // Bounded search within [predicted - max_error, predicted + max_error].
        // `predict` is unbounded above, so clamp both ends into the key slice:
        // an out-of-range prediction must yield an empty scan, not a panic.
        let lo = predicted
            .saturating_sub(self.max_error)
            .min(self.keys.len());
        let hi = predicted
            .saturating_add(self.max_error)
            .saturating_add(1)
            .min(self.keys.len());

        // Linear scan within the bounded range.
        for (offset, existing) in self.keys[lo..hi].iter().enumerate() {
            let i = lo + offset;
            match existing.cmp(&key) {
                std::cmp::Ordering::Equal => {
                    let error = predicted.abs_diff(i);
                    record_lookup(error);
                    return Some(i);
                }
                std::cmp::Ordering::Greater => break,
                std::cmp::Ordering::Less => {}
            }
        }

        record_lookup(0);
        None
    }

    /// Return the number of keys in the index.
    #[must_use]
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    /// Return whether the index is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    /// Return the number of segments in the piecewise linear model.
    #[must_use]
    pub fn num_segments(&self) -> usize {
        self.segments.len()
    }

    /// Return the max error bound.
    #[must_use]
    pub fn max_error(&self) -> usize {
        self.max_error
    }

    /// Return the keys slice.
    #[must_use]
    pub fn keys(&self) -> &[u64] {
        &self.keys
    }

    /// Compute the maximum observed prediction error across all keys.
    /// Useful for validating that the model respects the error bound.
    #[must_use]
    pub fn max_observed_error(&self) -> usize {
        if self.keys.is_empty() {
            return 0;
        }
        let mut max_err = 0usize;
        for (actual_pos, &key) in self.keys.iter().enumerate() {
            if let Some(seg_idx) = self.find_segment(key) {
                let predicted = self.segments[seg_idx].predict(key);
                let err = predicted.abs_diff(actual_pos);
                max_err = max_err.max(err);
            }
        }
        max_err
    }

    /// Binary search for the segment containing the given key.
    fn find_segment(&self, key: u64) -> Option<usize> {
        if self.segments.is_empty() {
            return None;
        }

        // Binary search by key_lo.
        let idx = self.segments.partition_point(|seg| seg.key_lo <= key);

        if idx == 0 {
            // Key is before the first segment.
            None
        } else {
            let seg_idx = idx - 1;
            if key <= self.segments[seg_idx].key_hi {
                Some(seg_idx)
            } else {
                None
            }
        }
    }
}

impl fmt::Debug for LearnedIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LearnedIndex")
            .field("num_keys", &self.keys.len())
            .field("num_segments", &self.segments.len())
            .field("max_error", &self.max_error)
            .finish()
    }
}

/// Train a piecewise linear model from sorted keys.
///
/// Greedily extends each segment as long as the linear approximation
/// error stays within `max_error`. When the error exceeds the threshold,
/// a new segment is started.
fn train_piecewise_linear(keys: &[u64], max_error: usize) -> Vec<Segment> {
    if keys.is_empty() {
        return Vec::new();
    }

    if keys.len() == 1 {
        return vec![Segment {
            key_lo: keys[0],
            key_hi: keys[0],
            pos_lo: 0,
            slope: 0.0,
            intercept: 0.0,
        }];
    }

    let mut segments = Vec::new();
    let mut seg_start = 0usize;

    while seg_start < keys.len() {
        let mut seg_end = seg_start;
        let mut slope_min = 0.0f64;
        let mut slope_max = f64::INFINITY;

        for i in seg_start + 1..keys.len() {
            let dx = keys[i] as f64 - keys[seg_start] as f64;
            let dy = (i - seg_start) as f64;

            if dx == 0.0 {
                if i - seg_start > max_error {
                    break;
                }
                seg_end = i;
                continue;
            }

            let p_min = (dy - max_error as f64) / dx;
            let p_max = (dy + max_error as f64) / dx;

            let new_min = slope_min.max(p_min);
            let new_max = slope_max.min(p_max);

            if new_min > new_max {
                break;
            }

            slope_min = new_min;
            slope_max = new_max;
            seg_end = i;
        }

        let dx = keys[seg_end] as f64 - keys[seg_start] as f64;
        let slope = if dx > 0.0 {
            ((seg_end - seg_start) as f64 / dx).clamp(slope_min, slope_max)
        } else {
            0.0
        };

        segments.push(Segment {
            key_lo: keys[seg_start],
            key_hi: keys[seg_end],
            pos_lo: seg_start,
            slope,
            intercept: seg_start as f64,
        });

        seg_start = seg_end + 1;
    }

    segments
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_lookup() {
        let keys: Vec<u64> = (0..100).collect();
        let idx = LearnedIndex::build(&keys, LearnedIndexConfig::default());

        assert_eq!(idx.len(), 100);
        assert!(!idx.is_empty());

        for &k in &keys {
            #[allow(clippy::cast_possible_truncation)]
            let expected = k as usize;
            assert_eq!(idx.lookup(k), Some(expected));
        }

        assert_eq!(idx.lookup(100), None);
        assert_eq!(idx.lookup(999), None);
    }

    #[test]
    fn uniform_distribution() {
        // Perfectly uniform keys: one segment should suffice.
        let keys: Vec<u64> = (0..1000).map(|i| i * 10).collect();
        let idx = LearnedIndex::build(&keys, LearnedIndexConfig { max_error: 1 });

        assert!(
            idx.num_segments() <= 5,
            "uniform distribution should need few segments, got {}",
            idx.num_segments()
        );

        for (pos, &k) in keys.iter().enumerate() {
            assert_eq!(idx.lookup(k), Some(pos));
        }
    }

    #[test]
    fn empty_index() {
        let idx = LearnedIndex::build(&[], LearnedIndexConfig::default());
        assert!(idx.is_empty());
        assert_eq!(idx.len(), 0);
        assert_eq!(idx.num_segments(), 0);
        assert_eq!(idx.lookup(42), None);
    }

    #[test]
    fn single_key() {
        let idx = LearnedIndex::build(&[42], LearnedIndexConfig::default());
        assert_eq!(idx.len(), 1);
        assert_eq!(idx.num_segments(), 1);
        assert_eq!(idx.lookup(42), Some(0));
        assert_eq!(idx.lookup(43), None);
    }

    #[test]
    fn error_bound_respected() {
        let keys: Vec<u64> = (0..500).map(|i| i * i).collect(); // quadratic distribution
        let config = LearnedIndexConfig { max_error: 32 };
        let idx = LearnedIndex::build(&keys, config);

        let max_err = idx.max_observed_error();
        assert!(
            max_err <= config.max_error,
            "max observed error {max_err} exceeds bound {}",
            config.max_error
        );
    }

    #[test]
    fn max_error_and_keys_accessors_reflect_build_inputs() {
        // max_error() returns the configured error bound and keys() returns the
        // stored (sorted) key slice. Neither accessor was directly tested.
        let keys = vec![1_u64, 4, 9, 16, 25, 36];
        let idx = LearnedIndex::build(&keys, LearnedIndexConfig { max_error: 8 });

        assert_eq!(idx.max_error(), 8);
        assert_eq!(idx.keys(), keys.as_slice());
        assert_eq!(idx.len(), keys.len());

        // The default config's error bound (16) flows through unchanged.
        let def = LearnedIndex::build(&keys, LearnedIndexConfig::default());
        assert_eq!(def.max_error(), 16);
    }

    #[test]
    fn lookup_with_extreme_key_does_not_overflow() {
        let keys: Vec<u64> = (0..128).collect();
        let idx = LearnedIndex::build(&keys, LearnedIndexConfig::default());
        assert_eq!(idx.lookup(u64::MAX), None);
    }

    #[test]
    fn nonuniform_all_present_findable_and_in_range_gaps_absent() {
        // Quadratic keys produce growing gaps; the error-bounded search must
        // still resolve every present key, and reject keys that fall *inside*
        // the range (between consecutive squares) rather than only out-of-range.
        let keys: Vec<u64> = (0..300u64).map(|i| i * i).collect();
        let idx = LearnedIndex::build(&keys, LearnedIndexConfig { max_error: 16 });

        for (pos, &k) in keys.iter().enumerate() {
            assert_eq!(
                idx.lookup(k),
                Some(pos),
                "present key {k} must be found at {pos}"
            );
        }

        // For i >= 2, i*i - 1 is never a perfect square, so it is an in-range
        // gap that must resolve to None (not a false positive).
        for &k in &keys[2..60] {
            assert_eq!(
                idx.lookup(k - 1),
                None,
                "in-gap key {} must be absent",
                k - 1
            );
        }

        // A small clustered set with an obvious interior gap.
        let clustered = vec![0u64, 1, 2, 1000, 1001, 1002];
        let idx2 = LearnedIndex::build(&clustered, LearnedIndexConfig { max_error: 4 });
        for (pos, &k) in clustered.iter().enumerate() {
            assert_eq!(idx2.lookup(k), Some(pos));
        }
        assert_eq!(
            idx2.lookup(500),
            None,
            "a key in the large interior gap is absent"
        );
    }
}
