//! Conformal-prediction SLO-respecting SQLITE_BUSY retry budget.
//!
//! Exponential backoff on SQLITE_BUSY has no provable tail-latency property:
//! a writer may spin retrying for the full `busy_timeout` even when the
//! accumulated blocking has already blown past any reasonable latency
//! target. This module adds an opt-in latency-prediction cap.
//!
//! # Method
//!
//! Per-connection, we maintain a ring buffer of recent successful commit
//! latencies (size `K`, default 256). A one-sided conformal upper bound at
//! miscoverage alpha uses order-statistic rank `ceil((1 - alpha)(K + 1))`.
//! The bound is cached until calibration or configuration changes, so an
//! unchanged BUSY retry episode does not repeatedly allocate and select.
//!
//! Under exchangeability of the calibration and next-commit latencies, this
//! bound has marginal coverage at least `1 - alpha`. When the rank is `K + 1`,
//! no finite sample bound has the requested coverage; the predictor abstains
//! instead of substituting the sample maximum. Without a finite prediction,
//! retries use the configured SLO as a hard wall. With a prediction, retries
//! stop when elapsed time plus the predicted tail reaches that wall.
//!
//! This is an admission heuristic, not a bound on physical commit completion
//! or on a workload with drifting/non-exchangeable latency. A future commit
//! can exceed its prediction, and a stopped retry could have succeeded.
//!
//! # Safety
//!
//! Retry budgets control *blocking*, not isolation. A short-circuited BUSY
//! is behaviorally identical to a BUSY returned after the legacy
//! `busy_timeout` expires -- the MVCC invariants (SSI, snapshot visibility,
//! WAL ordering) are entirely untouched.
//!
//! # Defaults
//!
//! With `slo_ms = 0` (the default), the cap is disabled and the legacy
//! `busy_timeout` path runs unchanged. Users opt in via
//! `PRAGMA fsqlite.retry_slo_ms = <n>`.

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::time::Duration;

/// Minimum calibration window. This is only a warm-up floor: the requested
/// miscoverage may require more samples before a finite bound exists.
pub const MIN_CALIBRATION_SAMPLES: usize = 8;

/// Lower bound on the configurable ring-buffer size. Windows smaller
/// than `MIN_CALIBRATION_SAMPLES` would never produce a usable bound.
const MIN_CALIBRATION_WINDOW: usize = MIN_CALIBRATION_SAMPLES;

/// Upper bound on the configurable ring-buffer size. 4096 commit
/// timings is ~32 KiB; beyond this the quantile computation becomes
/// nontrivial without meaningfully tightening the bound.
const MAX_CALIBRATION_WINDOW: usize = 4096;

/// Default calibration window. Chosen to cover roughly the last few
/// minutes of commits on typical OLTP workloads while staying cheap to
/// select on the cold BUSY path.
pub const DEFAULT_CALIBRATION_WINDOW: usize = 256;

/// Default miscoverage bound for a 95% marginal prediction interval under
/// exchangeability. At least 19 samples are needed for a finite bound.
pub const DEFAULT_ALPHA: f64 = 0.05;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QuantileCache {
    Dirty,
    Unavailable,
    Bound(u64),
}

/// Per-connection retry budget configuration and calibration ring.
///
/// All state lives on the owning `Connection` and is accessed single-
/// threaded; no synchronization is needed. The ring stores nanoseconds
/// as `u64` -- `u64::MAX ns` is ~584 years, comfortably larger than any
/// real commit latency.
#[derive(Debug)]
pub struct ConformalRetryBudget {
    /// Target SLO in milliseconds. `0` means the budget is disabled and
    /// the legacy `busy_timeout` path is used unchanged.
    slo_ms: u64,
    /// Miscoverage bound. Must be strictly in `(0.0, 1.0)`.
    alpha: f64,
    /// Maximum ring-buffer capacity.
    calibration_window: usize,
    /// Ring buffer of recent successful commit latencies, in nanoseconds.
    latencies_ns: VecDeque<u64>,
    /// Includes unavailable bounds, so warm-up retries also avoid recomputing.
    quantile_cache: Cell<QuantileCache>,
}

impl Default for ConformalRetryBudget {
    fn default() -> Self {
        Self {
            slo_ms: 0,
            alpha: DEFAULT_ALPHA,
            calibration_window: DEFAULT_CALIBRATION_WINDOW,
            latencies_ns: VecDeque::with_capacity(DEFAULT_CALIBRATION_WINDOW),
            quantile_cache: Cell::new(QuantileCache::Dirty),
        }
    }
}

impl ConformalRetryBudget {
    /// Return the configured SLO in milliseconds, or `None` if disabled.
    pub const fn slo_ms(&self) -> Option<u64> {
        if self.slo_ms == 0 {
            None
        } else {
            Some(self.slo_ms)
        }
    }

    /// Return the configured miscoverage bound.
    pub const fn alpha(&self) -> f64 {
        self.alpha
    }

    /// Return the current calibration window size.
    pub const fn calibration_window(&self) -> usize {
        self.calibration_window
    }

    /// Return the number of calibration samples currently held.
    #[cfg(test)]
    pub fn sample_count(&self) -> usize {
        self.latencies_ns.len()
    }

    /// Configure the SLO budget. `0` disables the cap.
    pub const fn set_slo_ms(&mut self, slo_ms: u64) {
        self.slo_ms = slo_ms;
    }

    /// Configure miscoverage. Clamped into the open interval `(0, 1)`;
    /// callers that want input validation should check before calling.
    pub fn set_alpha(&mut self, alpha: f64) {
        let clamped = if alpha.is_nan() {
            DEFAULT_ALPHA
        } else if alpha <= 0.0 {
            f64::EPSILON
        } else if alpha >= 1.0 {
            1.0 - f64::EPSILON
        } else {
            alpha
        };
        self.alpha = clamped;
        self.quantile_cache.set(QuantileCache::Dirty);
    }

    /// Resize the calibration ring, preserving the most recent samples.
    pub fn set_calibration_window(&mut self, window: usize) {
        let window = window.clamp(MIN_CALIBRATION_WINDOW, MAX_CALIBRATION_WINDOW);
        self.calibration_window = window;
        while self.latencies_ns.len() > window {
            self.latencies_ns.pop_front();
        }
        // Avoid keeping an oversized allocation after a shrink.
        if self.latencies_ns.capacity() > window.saturating_mul(2) {
            self.latencies_ns.shrink_to(window);
        }
        self.quantile_cache.set(QuantileCache::Dirty);
    }

    /// Record a successful commit latency for future calibration.
    pub fn record_success(&mut self, latency: Duration) {
        let ns = u64::try_from(latency.as_nanos()).unwrap_or(u64::MAX);
        if self.latencies_ns.len() == self.calibration_window {
            self.latencies_ns.pop_front();
        }
        self.latencies_ns.push_back(ns);
        self.quantile_cache.set(QuantileCache::Dirty);
    }

    /// Compute the one-sided conformal upper bound on a future commit
    /// latency at miscoverage `alpha`.
    ///
    /// Returns `None` during warm-up or when the finite-sample rank exceeds
    /// the calibration set. In that case `retry_allowed` uses the raw SLO
    /// deadline without pretending to have a finite tail prediction.
    ///
    /// The finite bound is the `ceil((1 - alpha)(K + 1))`-th smallest
    /// calibration score. Its marginal coverage requires exchangeability.
    /// An unchanged calibration set and alpha reuse the cached result.
    pub fn quantile_bound(&self) -> Option<Duration> {
        match self.quantile_cache.get() {
            QuantileCache::Bound(ns) => return Some(Duration::from_nanos(ns)),
            QuantileCache::Unavailable => return None,
            QuantileCache::Dirty => {}
        }
        let bound = self.compute_quantile_bound_ns();
        self.quantile_cache.set(match bound {
            Some(ns) => QuantileCache::Bound(ns),
            None => QuantileCache::Unavailable,
        });
        bound.map(Duration::from_nanos)
    }

    fn compute_quantile_bound_ns(&self) -> Option<u64> {
        let k = self.latencies_ns.len();
        if k < MIN_CALIBRATION_SAMPLES {
            return None;
        }
        let rank = conformal_rank(k, self.alpha)?;
        let mut scratch: Vec<u64> = self.latencies_ns.iter().copied().collect();
        let (_, pivot, _) = scratch.select_nth_unstable(rank - 1);
        Some(*pivot)
    }

    /// Return the configured retry-admission deadline, or `None` if disabled.
    /// An uncalibrated predictor still honors this explicit hard wall.
    pub fn slo_budget(&self) -> Option<Duration> {
        self.slo_ms().map(Duration::from_millis)
    }

    /// Decide whether a BUSY retry is allowed given how long we have
    /// already been blocked. The engine's ordinary `busy_timeout` remains
    /// an independent limit; this budget does not extend it.
    pub fn retry_allowed(&self, elapsed: Duration) -> bool {
        let Some(budget) = self.slo_budget() else {
            return true;
        };
        let Some(predicted_tail) = self.quantile_bound() else {
            return elapsed < budget;
        };
        let projected = elapsed.saturating_add(predicted_tail);
        projected < budget
    }
}

/// One-based finite-sample rank, or no finite bound. Compute
/// `K + 1 - floor(alpha * (K + 1))` exactly for the configured binary float:
/// a rounded floating-point product can cross an integer boundary and select
/// a rank with less than the requested coverage. Clamping alpha here makes
/// its IEEE-754 exponent normal and the right shift lie in 53..=104; smaller
/// positive alphas cannot produce a finite bound within the bounded window.
fn conformal_rank(k: usize, alpha: f64) -> Option<usize> {
    let n = k.checked_add(1)?;
    let bits = alpha.clamp(f64::EPSILON, 1.0 - f64::EPSILON).to_bits();
    let exponent = (bits >> 52) & 0x7ff;
    let significand = (1_u128 << 52) | u128::from(bits & ((1_u64 << 52) - 1));
    let scaled = significand * u128::try_from(n).ok()?;
    let excluded = usize::try_from(scaled >> (1075 - exponent)).ok()?;
    let rank = n.checked_sub(excluded)?;
    (rank > 0 && rank <= k).then_some(rank)
}

/// RefCell wrapper for the single-threaded connection's retry budget.
pub type ConformalRetryBudgetCell = RefCell<ConformalRetryBudget>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_budget_is_disabled() {
        let b = ConformalRetryBudget::default();
        assert!(b.slo_ms().is_none());
        assert!(b.retry_allowed(Duration::from_secs(3600)));
        assert_eq!(b.quantile_cache.get(), QuantileCache::Dirty);
    }

    #[test]
    fn quantile_requires_minimum_samples() {
        let mut b = ConformalRetryBudget::default();
        b.set_alpha(0.2);
        for _ in 0..(MIN_CALIBRATION_SAMPLES - 1) {
            b.record_success(Duration::from_millis(1));
        }
        assert!(b.quantile_bound().is_none());
        b.record_success(Duration::from_millis(1));
        assert!(b.quantile_bound().is_some());
    }

    #[test]
    fn default_confidence_requires_nineteen_samples() {
        let mut b = ConformalRetryBudget::default();
        b.set_slo_ms(100);
        for _ in 0..18 {
            b.record_success(Duration::from_millis(50));
            assert!(b.quantile_bound().is_none());
            assert!(b.retry_allowed(Duration::from_millis(60)));
            assert!(!b.retry_allowed(Duration::from_millis(100)));
        }
        b.record_success(Duration::from_millis(50));
        assert_eq!(b.quantile_bound(), Some(Duration::from_millis(50)));
        assert!(!b.retry_allowed(Duration::from_millis(60)));
    }

    #[test]
    fn quantile_picks_correct_order_statistic() {
        let mut b = ConformalRetryBudget::default();
        b.set_alpha(0.2);
        for ms in 1u64..=10 {
            b.record_success(Duration::from_millis(ms));
        }
        let q = b.quantile_bound().expect("quantile with K=10");
        assert_eq!(q, Duration::from_millis(9));
    }

    #[test]
    fn finite_sample_rank_does_not_round_across_confidence_boundaries() {
        let alpha = 0.25_f64;
        let below = f64::from_bits(alpha.to_bits() - 1);
        let above = f64::from_bits(alpha.to_bits() + 1);
        assert_eq!(conformal_rank(15, below), Some(13));
        assert_eq!(conformal_rank(15, alpha), Some(12));
        assert_eq!(conformal_rank(15, above), Some(12));

        let minimum = 0.0625_f64;
        assert_eq!(conformal_rank(15, minimum), Some(15));
        assert_eq!(conformal_rank(15, f64::from_bits(minimum.to_bits() - 1)), None);
        assert_eq!(conformal_rank(MAX_CALIBRATION_WINDOW, f64::EPSILON), None);
    }

    #[test]
    fn finite_sample_ranks_match_exact_binary_fraction_oracle() {
        // Numerators / 1024 are exactly representable. Check the complete
        // confidence grid across window sizes, including unbounded ranks.
        for k in [8_usize, 15, 19, 32, 255, 256, 4096] {
            for numerator in 1_u32..1024 {
                let alpha = f64::from(numerator) / 1024.0;
                let numerator = usize::try_from(numerator).unwrap();
                let rank = (k + 1) - (numerator * (k + 1) / 1024);
                let expected = (rank <= k).then_some(rank);
                assert_eq!(conformal_rank(k, alpha), expected, "k={k} alpha={alpha}");
            }
        }
    }

    #[test]
    fn set_calibration_window_truncates_oldest() {
        let mut b = ConformalRetryBudget::default();
        for ms in 1u64..=100 {
            b.record_success(Duration::from_millis(ms));
        }
        assert!(b.quantile_bound().is_some());
        b.set_calibration_window(16);
        assert_eq!(b.sample_count(), 16);
        assert!(b.quantile_bound().is_none(), "16 samples cannot support 95% coverage");
        b.set_alpha(0.1);
        assert_eq!(b.quantile_bound(), Some(Duration::from_millis(100)));
    }

    #[test]
    fn cached_quantile_is_invalidated_by_samples_alpha_and_window() {
        let mut b = ConformalRetryBudget::default();
        b.set_alpha(0.2);
        b.set_calibration_window(8);
        assert!(b.quantile_bound().is_none());
        assert_eq!(b.quantile_cache.get(), QuantileCache::Unavailable);
        for ms in 1u64..=8 {
            b.record_success(Duration::from_millis(ms));
        }
        assert_eq!(b.quantile_cache.get(), QuantileCache::Dirty);
        for _ in 0..100 {
            assert_eq!(b.quantile_bound(), Some(Duration::from_millis(8)));
            assert_eq!(b.quantile_cache.get(), QuantileCache::Bound(8_000_000));
        }
        b.record_success(Duration::from_millis(100));
        assert_eq!(b.sample_count(), 8);
        assert_eq!(b.quantile_cache.get(), QuantileCache::Dirty);
        assert_eq!(b.quantile_bound(), Some(Duration::from_millis(100)));
        b.set_alpha(0.5);
        assert_eq!(b.quantile_cache.get(), QuantileCache::Dirty);
        assert_eq!(b.quantile_bound(), Some(Duration::from_millis(6)));
        b.set_calibration_window(16);
        assert_eq!(b.quantile_cache.get(), QuantileCache::Dirty);
        assert_eq!(b.quantile_bound(), Some(Duration::from_millis(6)));
        for _ in 0..8 {
            b.record_success(Duration::from_millis(200));
        }
        assert_eq!(b.quantile_bound(), Some(Duration::from_millis(200)));
        b.set_calibration_window(8);
        assert_eq!(b.quantile_cache.get(), QuantileCache::Dirty);
        assert_eq!(b.quantile_bound(), Some(Duration::from_millis(200)));
    }

    #[test]
    fn calibration_window_is_bounded() {
        let mut b = ConformalRetryBudget::default();
        b.set_calibration_window(0);
        assert_eq!(b.calibration_window(), MIN_CALIBRATION_WINDOW);
        b.set_calibration_window(usize::MAX);
        assert_eq!(b.calibration_window(), MAX_CALIBRATION_WINDOW);
    }

    #[test]
    fn retry_disallowed_when_projected_exceeds_slo() {
        let mut b = ConformalRetryBudget::default();
        b.set_slo_ms(100);
        b.set_alpha(0.1);
        for _ in 0..20 {
            b.record_success(Duration::from_millis(50));
        }
        assert!(!b.retry_allowed(Duration::from_millis(60)));
        assert!(b.retry_allowed(Duration::from_millis(10)));
    }

    #[test]
    fn alpha_bounds_are_enforced() {
        let mut b = ConformalRetryBudget::default();
        b.set_alpha(-1.0);
        assert!(b.alpha() > 0.0);
        b.set_alpha(2.0);
        assert!(b.alpha() < 1.0);
        b.set_alpha(f64::NAN);
        assert!(b.alpha() > 0.0 && b.alpha() < 1.0);
    }

    #[test]
    fn extreme_latencies_and_confidence_remain_bounded() {
        let mut b = ConformalRetryBudget::default();
        b.set_slo_ms(u64::MAX);
        for _ in 0..32 {
            b.record_success(Duration::MAX);
        }
        assert_eq!(b.quantile_bound(), Some(Duration::from_nanos(u64::MAX)));
        assert!(!b.retry_allowed(Duration::MAX));
        b.set_alpha(f64::NEG_INFINITY);
        assert!(b.quantile_bound().is_none());
        b.set_alpha(f64::INFINITY);
        assert_eq!(b.quantile_bound(), Some(Duration::from_nanos(u64::MAX)));
        b.set_alpha(f64::NAN);
        assert_eq!(b.quantile_bound(), Some(Duration::from_nanos(u64::MAX)));
        b.set_slo_ms(0);
        assert!(b.retry_allowed(Duration::MAX));
    }

    #[test]
    fn retry_allowed_when_slo_disabled() {
        let mut b = ConformalRetryBudget::default();
        for _ in 0..32 {
            b.record_success(Duration::from_secs(10));
        }
        assert!(b.retry_allowed(Duration::from_hours(24)));
    }
}
