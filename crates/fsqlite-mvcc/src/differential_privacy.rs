//! Differential privacy for aggregate queries (bd-19u.7, §12.5).
//!
//! Privacy-preserving aggregate query results using calibrated noise addition.
//! Supports epsilon-delta privacy budget tracking per session with Laplace and
//! Gaussian mechanisms.
//!
//! # Privacy Model
//!
//! A randomized mechanism M satisfies (ε,δ)-differential privacy if for all
//! neighboring databases D, D' (differing in one record) and all subsets S of
//! outputs:
//!
//!   Pr[M(D) ∈ S] ≤ exp(ε) · Pr[M(D') ∈ S] + δ
//!
//! When δ=0, this is pure ε-differential privacy.
//!
//! # Mechanisms
//!
//! - **Laplace**: Adds Lap(0, Δf/ε) noise. Satisfies pure ε-DP.
//! - **Gaussian**: Adds N(0, σ²) noise where σ = Δf·√(2·ln(1.25/δ))/ε.
//!   Satisfies (ε,δ)-DP for δ > 0.
//!
//! # Reference
//!
//! Dwork, McSherry, Nissim, Smith 2006: "Calibrating Noise to Sensitivity in
//! Private Data Analysis"

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

use serde::Serialize;

// ---------------------------------------------------------------------------
// Global metrics
// ---------------------------------------------------------------------------

/// Total noise additions across all sessions.
static FSQLITE_DP_QUERIES_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Total epsilon spent across all sessions (stored as micros: ε × 1_000_000).
static FSQLITE_DP_EPSILON_SPENT_MICROS: AtomicU64 = AtomicU64::new(0);

/// Total budget exhaustion events.
static FSQLITE_DP_BUDGET_EXHAUSTED_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Snapshot of differential privacy metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct DpMetrics {
    pub fsqlite_dp_queries_total: u64,
    pub fsqlite_dp_epsilon_spent_micros: u64,
    pub fsqlite_dp_budget_exhausted_total: u64,
}

/// Take a snapshot of DP metrics.
#[must_use]
pub fn dp_metrics() -> DpMetrics {
    DpMetrics {
        fsqlite_dp_queries_total: FSQLITE_DP_QUERIES_TOTAL.load(Ordering::Relaxed),
        fsqlite_dp_epsilon_spent_micros: FSQLITE_DP_EPSILON_SPENT_MICROS.load(Ordering::Relaxed),
        fsqlite_dp_budget_exhausted_total: FSQLITE_DP_BUDGET_EXHAUSTED_TOTAL
            .load(Ordering::Relaxed),
    }
}

/// Reset DP metrics to zero.
pub fn reset_dp_metrics() {
    FSQLITE_DP_QUERIES_TOTAL.store(0, Ordering::Relaxed);
    FSQLITE_DP_EPSILON_SPENT_MICROS.store(0, Ordering::Relaxed);
    FSQLITE_DP_BUDGET_EXHAUSTED_TOTAL.store(0, Ordering::Relaxed);
}

#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
fn record_dp_query(epsilon: f64) {
    FSQLITE_DP_QUERIES_TOTAL.fetch_add(1, Ordering::Relaxed);
    let micros = (epsilon * 1_000_000.0) as u64;
    FSQLITE_DP_EPSILON_SPENT_MICROS.fetch_add(micros, Ordering::Relaxed);
}

fn record_budget_exhausted() {
    FSQLITE_DP_BUDGET_EXHAUSTED_TOTAL.fetch_add(1, Ordering::Relaxed);
}

// ---------------------------------------------------------------------------
// PRNG (SplitMix64)
// ---------------------------------------------------------------------------

/// Deterministic PRNG for noise generation. Uses SplitMix64.
///
/// This is NOT cryptographically secure, but DP security depends on the
/// privacy parameter ε, not on the unpredictability of the noise source.
/// For production deployment with adversarial users, consider a CSPRNG.
struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform f64 in (0, 1) — excludes 0 and 1.
    fn next_f64(&mut self) -> f64 {
        loop {
            let u = (self.next_u64() >> 11) as f64 / ((1u64 << 53) as f64);
            if u > 0.0 && u < 1.0 {
                return u;
            }
        }
    }

    /// Sample from Laplace(0, b) using inverse CDF.
    fn sample_laplace(&mut self, b: f64) -> f64 {
        let u = self.next_f64() - 0.5;
        -b * u.signum() * 2.0f64.mul_add(-u.abs(), 1.0).ln()
    }

    /// Sample from N(0, 1) using Box-Muller transform.
    fn sample_standard_normal(&mut self) -> f64 {
        let u1 = self.next_f64();
        let u2 = self.next_f64();
        (-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos()
    }
}

// ---------------------------------------------------------------------------
// Noise mechanism
// ---------------------------------------------------------------------------

/// The noise mechanism to use.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub enum NoiseMechanism {
    /// Laplace mechanism: pure ε-DP. Adds Lap(0, Δf/ε) noise.
    Laplace,
    /// Gaussian mechanism: (ε,δ)-DP. Adds N(0, σ²) noise.
    Gaussian {
        /// Privacy loss probability. Must be in (0, 1).
        delta: f64,
    },
}

impl fmt::Display for NoiseMechanism {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Laplace => write!(f, "Laplace"),
            Self::Gaussian { delta } => write!(f, "Gaussian(δ={delta:.2e})"),
        }
    }
}

// ---------------------------------------------------------------------------
// Privacy budget
// ---------------------------------------------------------------------------

/// Privacy budget error.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub enum DpError {
    /// Budget exhausted: requested ε exceeds remaining budget.
    BudgetExhausted { requested: f64, remaining: f64 },
    /// Invalid parameter.
    InvalidParameter(String),
}

impl fmt::Display for DpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BudgetExhausted {
                requested,
                remaining,
            } => write!(
                f,
                "DP budget exhausted: requested ε={requested:.4}, remaining ε={remaining:.4}"
            ),
            Self::InvalidParameter(msg) => write!(f, "DP invalid parameter: {msg}"),
        }
    }
}

/// A privacy budget tracker for one session.
///
/// Tracks cumulative epsilon spent and enforces a budget ceiling.
/// Uses sequential composition: total privacy loss ≤ Σεᵢ.
#[derive(Debug, Clone, Serialize)]
pub struct PrivacyBudget {
    /// Total epsilon budget for this session.
    total_epsilon: f64,
    /// Epsilon spent so far.
    spent_epsilon: f64,
    /// Number of queries charged against this budget.
    queries_charged: u64,
}

impl PrivacyBudget {
    /// Create a new privacy budget with the given epsilon ceiling.
    ///
    /// # Errors
    /// Returns `DpError::InvalidParameter` if `total_epsilon` is not positive.
    pub fn new(total_epsilon: f64) -> Result<Self, DpError> {
        if total_epsilon <= 0.0 || !total_epsilon.is_finite() {
            return Err(DpError::InvalidParameter(format!(
                "total_epsilon must be positive and finite, got {total_epsilon}"
            )));
        }
        Ok(Self {
            total_epsilon,
            spent_epsilon: 0.0,
            queries_charged: 0,
        })
    }

    /// Remaining epsilon budget.
    #[must_use]
    pub fn remaining(&self) -> f64 {
        (self.total_epsilon - self.spent_epsilon).max(0.0)
    }

    /// Total epsilon budget.
    #[must_use]
    pub fn total(&self) -> f64 {
        self.total_epsilon
    }

    /// Epsilon spent so far.
    #[must_use]
    pub fn spent(&self) -> f64 {
        self.spent_epsilon
    }

    /// Number of queries charged.
    #[must_use]
    pub fn queries_charged(&self) -> u64 {
        self.queries_charged
    }

    /// Check whether `epsilon` can be spent without exceeding the budget.
    #[must_use]
    pub fn can_spend(&self, epsilon: f64) -> bool {
        epsilon > 0.0 && self.spent_epsilon + epsilon <= self.total_epsilon
    }

    /// Charge `epsilon` against the budget.
    ///
    /// # Errors
    /// Returns `DpError::BudgetExhausted` if insufficient budget remains.
    fn charge(&mut self, epsilon: f64) -> Result<(), DpError> {
        if epsilon <= 0.0 || !epsilon.is_finite() {
            return Err(DpError::InvalidParameter(format!(
                "epsilon must be positive and finite, got {epsilon}"
            )));
        }
        if self.spent_epsilon + epsilon > self.total_epsilon {
            record_budget_exhausted();
            return Err(DpError::BudgetExhausted {
                requested: epsilon,
                remaining: self.remaining(),
            });
        }
        self.spent_epsilon += epsilon;
        self.queries_charged += 1;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// DP query result
// ---------------------------------------------------------------------------

/// Result of a differentially private aggregate query.
#[derive(Debug, Clone, Serialize)]
pub struct DpQueryResult {
    /// The noisy aggregate value.
    pub noisy_value: f64,
    /// Epsilon spent for this query.
    pub epsilon_spent: f64,
    /// The noise mechanism used.
    pub mechanism: NoiseMechanism,
    /// The sensitivity of the query function.
    pub sensitivity: f64,
    /// The noise magnitude (scale parameter).
    pub noise_scale: f64,
}

// ---------------------------------------------------------------------------
// DP engine
// ---------------------------------------------------------------------------

/// Differential privacy engine for a single session.
///
/// Manages a privacy budget and provides noise-addition mechanisms for
/// aggregate query results.
pub struct DpEngine {
    budget: PrivacyBudget,
    rng: SplitMix64,
}

#[allow(clippy::missing_fields_in_debug)]
impl fmt::Debug for DpEngine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DpEngine")
            .field("budget", &self.budget)
            .finish_non_exhaustive()
    }
}

impl DpEngine {
    /// Create a new DP engine with the given total epsilon budget and PRNG seed.
    ///
    /// # Errors
    /// Returns `DpError::InvalidParameter` if `total_epsilon` is invalid.
    pub fn new(total_epsilon: f64, seed: u64) -> Result<Self, DpError> {
        let budget = PrivacyBudget::new(total_epsilon)?;
        tracing::info!(
            target: "fsqlite.dp",
            total_epsilon,
            seed,
            "dp_engine_created"
        );
        Ok(Self {
            budget,
            rng: SplitMix64::new(seed),
        })
    }

    /// Get the current privacy budget state.
    #[must_use]
    pub fn budget(&self) -> &PrivacyBudget {
        &self.budget
    }

    /// Apply the Laplace mechanism to an aggregate value.
    ///
    /// Adds Lap(0, sensitivity/epsilon) noise. Satisfies pure ε-DP.
    ///
    /// # Arguments
    /// * `true_value` — the true aggregate value
    /// * `sensitivity` — L1 sensitivity Δf (max change from one record)
    /// * `epsilon` — privacy parameter for this query
    ///
    /// # Errors
    /// Returns `DpError::BudgetExhausted` if budget is insufficient, or
    /// `DpError::InvalidParameter` if sensitivity or epsilon are invalid.
    pub fn laplace(
        &mut self,
        true_value: f64,
        sensitivity: f64,
        epsilon: f64,
    ) -> Result<DpQueryResult, DpError> {
        self.validate_params(sensitivity, epsilon)?;
        self.budget.charge(epsilon)?;

        let b = sensitivity / epsilon;
        let noise = self.rng.sample_laplace(b);
        let noisy_value = true_value + noise;

        record_dp_query(epsilon);

        tracing::info!(
            target: "fsqlite.dp",
            epsilon_spent = epsilon,
            mechanism = "Laplace",
            sensitivity,
            noise_scale = b,
            "dp_query"
        );

        Ok(DpQueryResult {
            noisy_value,
            epsilon_spent: epsilon,
            mechanism: NoiseMechanism::Laplace,
            sensitivity,
            noise_scale: b,
        })
    }

    /// Apply the Gaussian mechanism to an aggregate value.
    ///
    /// Adds N(0, σ²) noise where σ = sensitivity·√(2·ln(1.25/δ))/ε.
    /// Satisfies (ε,δ)-DP.
    ///
    /// # Arguments
    /// * `true_value` — the true aggregate value
    /// * `sensitivity` — L2 sensitivity Δ₂f
    /// * `epsilon` — privacy parameter for this query
    /// * `delta` — privacy loss probability (must be in (0, 1))
    ///
    /// # Errors
    /// Returns `DpError::BudgetExhausted` or `DpError::InvalidParameter`.
    pub fn gaussian(
        &mut self,
        true_value: f64,
        sensitivity: f64,
        epsilon: f64,
        delta: f64,
    ) -> Result<DpQueryResult, DpError> {
        self.validate_params(sensitivity, epsilon)?;
        if delta <= 0.0 || delta >= 1.0 || !delta.is_finite() {
            return Err(DpError::InvalidParameter(format!(
                "delta must be in (0, 1), got {delta}"
            )));
        }

        self.budget.charge(epsilon)?;

        let sigma = sensitivity * (2.0 * (1.25_f64 / delta).ln()).sqrt() / epsilon;
        let noise = self.rng.sample_standard_normal() * sigma;
        let noisy_value = true_value + noise;

        record_dp_query(epsilon);

        tracing::info!(
            target: "fsqlite.dp",
            epsilon_spent = epsilon,
            mechanism = "Gaussian",
            sensitivity,
            noise_scale = sigma,
            delta,
            "dp_query"
        );

        Ok(DpQueryResult {
            noisy_value,
            epsilon_spent: epsilon,
            mechanism: NoiseMechanism::Gaussian { delta },
            sensitivity,
            noise_scale: sigma,
        })
    }

    #[allow(clippy::unused_self)]
    fn validate_params(&self, sensitivity: f64, epsilon: f64) -> Result<(), DpError> {
        if sensitivity <= 0.0 || !sensitivity.is_finite() {
            return Err(DpError::InvalidParameter(format!(
                "sensitivity must be positive and finite, got {sensitivity}"
            )));
        }
        if epsilon <= 0.0 || !epsilon.is_finite() {
            return Err(DpError::InvalidParameter(format!(
                "epsilon must be positive and finite, got {epsilon}"
            )));
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Sensitivity helpers
// ---------------------------------------------------------------------------

/// Common query sensitivity values.
pub mod sensitivity {
    /// COUNT query: adding/removing one row changes count by 1.
    pub const COUNT: f64 = 1.0;

    /// SUM query: sensitivity equals the maximum absolute value a single row
    /// can contribute. Must be provided by the user.
    pub const fn sum(max_contribution: f64) -> f64 {
        max_contribution
    }

    /// AVG query over n rows with bounded contribution: sensitivity = 2·max/n.
    /// This is the L1 sensitivity of the average.
    pub fn avg(max_contribution: f64, n: usize) -> f64 {
        if n == 0 {
            return 0.0;
        }
        2.0 * max_contribution / n as f64
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::float_cmp)]
mod tests {
    use super::*;

    #[test]
    fn test_budget_creation() {
        let b = PrivacyBudget::new(1.0).unwrap();
        assert_eq!(b.total(), 1.0);
        assert_eq!(b.remaining(), 1.0);
        assert_eq!(b.spent(), 0.0);
        assert_eq!(b.queries_charged(), 0);

        assert!(PrivacyBudget::new(0.0).is_err());
        assert!(PrivacyBudget::new(-1.0).is_err());
        assert!(PrivacyBudget::new(f64::NAN).is_err());
        assert!(PrivacyBudget::new(f64::INFINITY).is_err());

        println!("[PASS] budget creation: valid and invalid cases");
    }

    #[test]
    fn test_budget_charging() {
        let mut b = PrivacyBudget::new(1.0).unwrap();

        b.charge(0.3).unwrap();
        assert!((b.spent() - 0.3).abs() < 1e-10);
        assert!((b.remaining() - 0.7).abs() < 1e-10);
        assert_eq!(b.queries_charged(), 1);

        b.charge(0.5).unwrap();
        assert!((b.spent() - 0.8).abs() < 1e-10);
        assert_eq!(b.queries_charged(), 2);

        // Should fail: 0.3 remaining, requesting 0.5.
        let err = b.charge(0.5).unwrap_err();
        assert!(matches!(err, DpError::BudgetExhausted { .. }));

        println!("[PASS] budget charging: sequential composition enforced");
    }

    #[test]
    fn test_budget_can_spend() {
        let mut b = PrivacyBudget::new(1.0).unwrap();
        assert!(b.can_spend(0.5));
        assert!(b.can_spend(1.0));
        assert!(!b.can_spend(1.1));

        b.charge(0.6).unwrap();
        assert!(b.can_spend(0.4));
        assert!(!b.can_spend(0.5));

        println!("[PASS] can_spend: correct pre-check");
    }

    #[test]
    fn test_laplace_mechanism() {
        let mut engine = DpEngine::new(10.0, 42).unwrap();

        // COUNT query: true value = 100, sensitivity = 1, epsilon = 1.0.
        let result = engine.laplace(100.0, 1.0, 1.0).unwrap();

        // Noisy value should be near 100 (within a few scale units).
        // With Lap(0, 1/1) = Lap(0, 1), 99.7% of values are within [-6, 6].
        assert!(
            (result.noisy_value - 100.0).abs() < 50.0,
            "Laplace noise too extreme: {}",
            result.noisy_value
        );
        assert_eq!(result.epsilon_spent, 1.0);
        assert_eq!(result.mechanism, NoiseMechanism::Laplace);
        assert_eq!(result.sensitivity, 1.0);
        assert_eq!(result.noise_scale, 1.0);

        println!(
            "[PASS] Laplace mechanism: noisy_value={:.2}, noise={:.2}",
            result.noisy_value,
            result.noisy_value - 100.0
        );
    }

    #[test]
    fn test_gaussian_mechanism() {
        let mut engine = DpEngine::new(10.0, 42).unwrap();

        let result = engine.gaussian(100.0, 1.0, 1.0, 1e-5).unwrap();

        assert!(
            (result.noisy_value - 100.0).abs() < 50.0,
            "Gaussian noise too extreme: {}",
            result.noisy_value
        );
        assert_eq!(result.epsilon_spent, 1.0);
        assert!(matches!(result.mechanism, NoiseMechanism::Gaussian { .. }));
        assert!(result.noise_scale > 0.0);

        println!(
            "[PASS] Gaussian mechanism: noisy_value={:.2}, sigma={:.4}",
            result.noisy_value, result.noise_scale
        );
    }

    #[test]
    fn test_budget_exhaustion() {
        let mut engine = DpEngine::new(1.0, 42).unwrap();

        engine.laplace(50.0, 1.0, 0.5).unwrap();
        engine.laplace(50.0, 1.0, 0.3).unwrap();

        // 0.2 remaining, requesting 0.5 -> fail.
        let err = engine.laplace(50.0, 1.0, 0.5).unwrap_err();
        assert!(matches!(err, DpError::BudgetExhausted { .. }));

        // But 0.2 should still work.
        engine.laplace(50.0, 1.0, 0.2).unwrap();

        println!("[PASS] budget exhaustion: enforced at limit");
    }

    #[test]
    fn test_laplace_noise_distribution() {
        // Statistical test: Laplace(0, 1) should have mean ~0 and variance ~2.
        let mut rng = SplitMix64::new(12345);
        let n = 10_000;
        let mut sum = 0.0;
        let mut sum_sq = 0.0;

        for _ in 0..n {
            let x = rng.sample_laplace(1.0);
            sum += x;
            sum_sq = x.mul_add(x, sum_sq);
        }

        let mean = sum / f64::from(n);
        let variance = f64::mul_add(mean, -mean, sum_sq / f64::from(n));

        // Laplace(0, 1): E[X] = 0, Var[X] = 2b² = 2.
        assert!(mean.abs() < 0.1, "Laplace mean should be ~0, got {mean:.4}");
        assert!(
            (variance - 2.0).abs() < 0.3,
            "Laplace variance should be ~2, got {variance:.4}"
        );

        println!("[PASS] Laplace distribution: mean={mean:.4}, var={variance:.4}");
    }

    #[test]
    fn test_gaussian_noise_distribution() {
        let mut rng = SplitMix64::new(54321);
        let n = 10_000;
        let mut sum = 0.0;
        let mut sum_sq = 0.0;

        for _ in 0..n {
            let x = rng.sample_standard_normal();
            sum += x;
            sum_sq = x.mul_add(x, sum_sq);
        }

        let mean = sum / f64::from(n);
        let variance = f64::mul_add(mean, -mean, sum_sq / f64::from(n));

        // N(0, 1): E[X] = 0, Var[X] = 1.
        assert!(
            mean.abs() < 0.1,
            "Gaussian mean should be ~0, got {mean:.4}"
        );
        assert!(
            (variance - 1.0).abs() < 0.15,
            "Gaussian variance should be ~1, got {variance:.4}"
        );

        println!("[PASS] Gaussian distribution: mean={mean:.4}, var={variance:.4}");
    }

    #[test]
    fn test_sensitivity_helpers() {
        assert_eq!(sensitivity::COUNT, 1.0);
        assert_eq!(sensitivity::sum(100.0), 100.0);

        let avg_sens = sensitivity::avg(100.0, 1000);
        assert!(
            (avg_sens - 0.2).abs() < 1e-10,
            "avg sensitivity = 2*100/1000 = 0.2"
        );

        assert_eq!(sensitivity::avg(100.0, 0), 0.0);

        println!("[PASS] sensitivity helpers: count, sum, avg");
    }

    #[test]
    fn test_metrics_integration() {
        let before = dp_metrics();

        let mut engine = DpEngine::new(5.0, 42).unwrap();
        engine.laplace(100.0, 1.0, 0.5).unwrap();
        engine.gaussian(100.0, 1.0, 0.3, 1e-5).unwrap();

        let after = dp_metrics();
        let queries_delta = after.fsqlite_dp_queries_total - before.fsqlite_dp_queries_total;
        let epsilon_delta =
            after.fsqlite_dp_epsilon_spent_micros - before.fsqlite_dp_epsilon_spent_micros;
        assert!(
            queries_delta >= 2,
            "at least 2 queries, got {queries_delta}"
        );
        assert!(
            epsilon_delta >= 800_000,
            "at least 0.8ε in micros, got {epsilon_delta}"
        );

        let json = serde_json::to_string(&after).unwrap();
        assert!(json.contains("fsqlite_dp_queries_total"));
        assert!(json.contains("fsqlite_dp_epsilon_spent_micros"));

        println!("[PASS] metrics: queries_delta={queries_delta} epsilon_delta={epsilon_delta}");
    }

    #[test]
    fn test_deterministic_noise() {
        // Same seed + same parameters should produce same noise.
        let mut e1 = DpEngine::new(10.0, 42).unwrap();
        let mut e2 = DpEngine::new(10.0, 42).unwrap();

        let r1 = e1.laplace(100.0, 1.0, 1.0).unwrap();
        let r2 = e2.laplace(100.0, 1.0, 1.0).unwrap();

        assert!(
            (r1.noisy_value - r2.noisy_value).abs() < 1e-10,
            "same seed should produce same noise"
        );

        println!(
            "[PASS] deterministic noise: both engines produced {:.6}",
            r1.noisy_value
        );
    }

    #[test]
    fn test_invalid_parameters() {
        let mut engine = DpEngine::new(10.0, 42).unwrap();

        // Invalid sensitivity.
        assert!(engine.laplace(100.0, 0.0, 1.0).is_err());
        assert!(engine.laplace(100.0, -1.0, 1.0).is_err());

        // Invalid epsilon.
        assert!(engine.laplace(100.0, 1.0, 0.0).is_err());
        assert!(engine.laplace(100.0, 1.0, -1.0).is_err());

        // Invalid delta for Gaussian.
        assert!(engine.gaussian(100.0, 1.0, 1.0, 0.0).is_err());
        assert!(engine.gaussian(100.0, 1.0, 1.0, 1.0).is_err());
        assert!(engine.gaussian(100.0, 1.0, 1.0, -0.1).is_err());

        println!("[PASS] invalid parameters: all rejected");
    }
}
