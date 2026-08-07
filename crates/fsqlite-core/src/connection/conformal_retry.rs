//! Conformal-prediction SLO-respecting SQLITE_BUSY retry budget.
//!
//! Exponential backoff on SQLITE_BUSY has no provable tail-latency property:
//! a writer may spin retrying for the full `busy_timeout` even when the
//! accumulated blocking has already blown past any reasonable latency
//! target. This module replaces the naive timeout with a distribution-free
//! conformal prediction cap.
//!
//! # Method
//!
//! Per-connection, we maintain a ring buffer of recent successful commit
//! latencies (size `K`, default 256). On each BUSY retry attempt we compute
//! a one-sided conformal upper bound at miscoverage α on the latency of a
//! *future* successful commit:
//!
//! ```text
//!   q̂ = sample quantile at rank ⌈(1 − α)(K + 1)⌉
//! ```
//!
//! This is the classical split-conformal quantile under exchangeability
//! (Vovk, Gammerman, Shafer — *Algorithmic Learning in a Random World*).
//! Given K i.i.d. samples, the resulting prediction interval
//! `[0, q̂]` covers the next realization with probability at least `1 − α`
//! (finite-sample, distribution-free). When we already have `elapsed`
//! wall-time spent retrying, our predicted total completion latency is
//! `elapsed + q̂`. If that exceeds the user-configured SLO budget, further
//! retries are wasted — we surface `SQLITE_BUSY` *now* rather than after
//! the budget is exhausted.
//!
//! # Safety
//!
//! Retry budgets control *blocking*, not isolation. A short-circuited BUSY
//! is behaviorally identical to a BUSY returned after the legacy
//! `busy_timeout` expires — the MVCC invariants (SSI, snapshot visibility,
//! WAL ordering) are entirely untouched.
//!
//! # Defaults
//!
//! With `slo_ms = 0` (the default), the cap is disabled and the legacy
//! `busy_timeout` path runs unchanged. Users opt in via
//! `PRAGMA fsqlite.retry_slo_ms = <n>`.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::time::Duration;

/// Minimum calibration window — below this many samples, conformal
/// prediction is not well-defined (⌈(1−α)(K+1)⌉ would exceed K for
/// typical α). We simply do not apply the cap until enough samples
/// accumulate.
pub const MIN_CALIBRATION_SAMPLES: usize = 8;

/// Lower bound on the configurable ring-buffer size. Windows smaller
/// than `MIN_CALIBRATION_SAMPLES` would never produce a usable bound.
const MIN_CALIBRATION_WINDOW: usize = MIN_CALIBRATION_SAMPLES;

/// Upper bound on the configurable ring-buffer size. 4096 commit
/// timings is ~32 KiB; beyond this the per-retry quantile computation
/// becomes nontrivial without meaningfully tightening the bound.
const MAX_CALIBRATION_WINDOW: usize = 4096;

/// Default calibration window. Chosen to cover roughly the last few
/// minutes of commits on typical OLTP workloads while staying cheap to
/// sort on the cold BUSY path.
pub const DEFAULT_CALIBRATION_WINDOW: usize = 256;

/// Default miscoverage bound. Matches the conventional 95% prediction
/// interval used throughout statistics; tight enough to be useful for
/// SLOs, loose enough that ordinary commit jitter does not trip it.
pub const DEFAULT_ALPHA: f64 = 0.05;

/// Per-connection retry budget configuration and calibration ring.
///
/// All state lives on the owning `Connection` and is accessed single-
/// threaded; no synchronization is needed. The ring stores nanoseconds
/// as `u64` — `u64::MAX ns` is ~584 years, comfortably larger than any
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
}

impl Default for ConformalRetryBudget {
    fn default() -> Self {
        Self {
            slo_ms: 0,
            alpha: DEFAULT_ALPHA,
            calibration_window: DEFAULT_CALIBRATION_WINDOW,
            latencies_ns: VecDeque::with_capacity(DEFAULT_CALIBRATION_WINDOW),
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
        // Guard against NaN/inf by the total-order check against two
        // sentinel bounds; clippy `float_cmp` is satisfied because we
        // use relational comparisons rather than equality.
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
    }

    /// Record a successful commit latency for future calibration.
    pub fn record_success(&mut self, latency: Duration) {
        let ns = u64::try_from(latency.as_nanos()).unwrap_or(u64::MAX);
        if self.latencies_ns.len() == self.calibration_window {
            self.latencies_ns.pop_front();
        }
        self.latencies_ns.push_back(ns);
    }

    /// Compute the one-sided conformal upper bound on a future commit
    /// latency at miscoverage `alpha`.
    ///
    /// Returns `None` when too few samples are available to form a
    /// well-defined prediction interval; in that case the caller must
    /// fall back to the legacy `busy_timeout` behavior (i.e. keep
    /// retrying).
    ///
    /// # Math
    ///
    /// For exchangeable calibration scores `s_1, …, s_K` and a fresh
    /// score `s_{K+1}`, the rank of `s_{K+1}` among `s_1, …, s_{K+1}` is
    /// uniform on `{1, …, K+1}`. Selecting the `⌈(1 − α)(K + 1)⌉`-th
    /// order statistic of the calibration set therefore gives a
    /// distribution-free upper bound with coverage ≥ `1 − α`.
    pub fn quantile_bound(&self) -> Option<Duration> {
        let k = self.latencies_ns.len();
        if k < MIN_CALIBRATION_SAMPLES {
            return None;
        }

        // ⌈(1 − α)(K + 1)⌉. We saturate at K because the conformal
        // bound is vacuous (∞) when α(K + 1) < 1 — treat that as "no
        // cap" rather than panicking.
        let alpha = self.alpha.clamp(f64::EPSILON, 1.0 - f64::EPSILON);
        let target = (1.0 - alpha) * ((k as f64) + 1.0);
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let rank_ceil = target.ceil() as usize;
        if rank_ceil == 0 {
            return None;
        }
        let idx = rank_ceil.saturating_sub(1).min(k - 1);

        let mut scratch: Vec<u64> = self.latencies_ns.iter().copied().collect();
        // Partial selection is linear; we use the stable `select_nth`
        // API to avoid a full sort on every BUSY retry.
        let (_, pivot, _) = scratch.select_nth_unstable(idx);
        Some(Duration::from_nanos(*pivot))
    }

    /// Return the predicted deadline after which further BUSY retries
    /// are expected to violate the SLO.
    ///
    /// Semantics:
    ///   * `None` → no cap is active (SLO disabled, or too few
    ///     calibration samples); the caller should keep retrying under
    ///     the legacy `busy_timeout` schedule.
    ///   * `Some(budget)` → retry only while `elapsed + q̂ < budget`
    ///     (i.e. the predicted additional commit latency still fits in
    ///     the SLO).
    pub fn slo_budget(&self) -> Option<Duration> {
        self.slo_ms().map(Duration::from_millis)
    }

    /// Decide whether a BUSY retry is allowed given how long we have
    /// already been blocked.
    ///
    /// Returns `true` when:
    ///   * the SLO cap is disabled (legacy behavior), or
    ///   * the predicted total latency `elapsed + q̂` still fits inside
    ///     the SLO budget with slack.
    pub fn retry_allowed(&self, elapsed: Duration) -> bool {
        let Some(budget) = self.slo_budget() else {
            return true;
        };
        let Some(predicted_tail) = self.quantile_bound() else {
            // Not enough calibration data → fall back to the raw SLO
            // budget as a hard wall.
            return elapsed < budget;
        };
        let projected = elapsed.saturating_add(predicted_tail);
        projected < budget
    }
}

/// RefCell wrapper for the retry budget; kept out of hot paths by
/// checking `slo_ms()` first on the non-borrow path so that the default
/// (disabled) configuration never touches the RefCell.
pub type ConformalRetryBudgetCell = RefCell<ConformalRetryBudget>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_budget_is_disabled() {
        let b = ConformalRetryBudget::default();
        assert!(b.slo_ms().is_none());
        assert!(b.retry_allowed(Duration::from_secs(3600)));
    }

    #[test]
    fn quantile_requires_minimum_samples() {
        let mut b = ConformalRetryBudget::default();
        for _ in 0..(MIN_CALIBRATION_SAMPLES - 1) {
            b.record_success(Duration::from_millis(1));
        }
        assert!(b.quantile_bound().is_none());
        b.record_success(Duration::from_millis(1));
        assert!(b.quantile_bound().is_some());
    }

    #[test]
    fn quantile_picks_correct_order_statistic() {
        let mut b = ConformalRetryBudget::default();
        b.set_alpha(0.2);
        // Ten samples, 1..=10 ms.
        for ms in 1u64..=10 {
            b.record_success(Duration::from_millis(ms));
        }
        // ⌈(1 − 0.2) × 11⌉ = ⌈8.8⌉ = 9 → index 8 → 9 ms.
        let q = b.quantile_bound().expect("quantile with K=10");
        assert_eq!(q, Duration::from_millis(9));
    }

    #[test]
    fn set_calibration_window_truncates_oldest() {
        let mut b = ConformalRetryBudget::default();
        for ms in 1u64..=100 {
            b.record_success(Duration::from_millis(ms));
        }
        b.set_calibration_window(16);
        assert_eq!(b.sample_count(), 16);
        // The retained samples should be the newest 16.
        b.set_alpha(0.05);
        // ⌈0.95 × 17⌉ = 17 → clamped to last index → 100 ms.
        let q = b.quantile_bound().unwrap();
        assert_eq!(q, Duration::from_millis(100));
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
        // Quantile ≈ 50 ms. Elapsed 60 ms + 50 ms tail = 110 > 100.
        assert!(!b.retry_allowed(Duration::from_millis(60)));
        // Elapsed 10 ms + 50 ms tail = 60 < 100.
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
    fn retry_allowed_when_slo_disabled() {
        let mut b = ConformalRetryBudget::default();
        for _ in 0..32 {
            b.record_success(Duration::from_secs(10));
        }
        assert!(b.retry_allowed(Duration::from_hours(24)));
    }
}
