//! Bayesian parity score engine with conformal confidence bands (bd-1dp9.1.3).
//!
//! Extends the point-estimate scorer from [`parity_taxonomy`](crate::parity_taxonomy) with:
//! - Beta-posterior uncertainty over per-category pass rates
//! - Conformal prediction intervals for finite-sample coverage guarantees
//! - Machine-readable scorecard for release-gating decisions
//!
//! # Statistical Model
//!
//! Each non-excluded feature in a category contributes an observation:
//! - `Passing` -> success (contribution 1.0, weighted by feature weight)
//! - `Partial` -> fractional success (contribution 0.5, weighted)
//! - `Missing` -> failure (contribution 0.0, weighted)
//!
//! The per-category pass rate theta_c is modeled as:
//!   theta_c ~ Beta(alpha_prior + sum(weighted_successes), beta_prior + sum(weighted_failures))
//!
//! The global score S_t is the weighted sum of category posterior means.
//! The lower confidence bound uses the Beta quantile for release decisions.
//!
//! # Conformal Bands
//!
//! The conformal band provides distribution-free finite-sample coverage. It is
//! calibrated from the per-category estimation residuals (gap between the
//! observed frequentist pass rate and the Bayesian posterior mean). With only
//! 9 categories, the band is necessarily conservative.
//!
//! # Determinism
//!
//! All arithmetic uses [`truncate_score`] for cross-platform reproducibility.

use std::collections::BTreeMap;
use std::f64::consts::PI;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::parity_taxonomy::{FeatureCategory, FeatureUniverse, truncate_score};
use crate::verification_contract_enforcement::{
    ContractEnforcementOutcome, enforce_gate_decision, evaluate_workspace_verification_contract,
};

/// Bead identifier for log correlation.
#[allow(dead_code)]
const BEAD_ID: &str = "bd-1dp9.1.3";

// ---------------------------------------------------------------------------
// Beta distribution
// ---------------------------------------------------------------------------

/// Parameters of a Beta(alpha, beta) distribution.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct BetaParams {
    pub alpha: f64,
    pub beta: f64,
}

impl BetaParams {
    /// Create a new Beta distribution with given parameters.
    ///
    /// # Panics
    ///
    /// Panics if `alpha` or `beta` are not positive.
    #[must_use]
    pub fn new(alpha: f64, beta: f64) -> Self {
        assert!(alpha > 0.0, "alpha must be positive, got {alpha}");
        assert!(beta > 0.0, "beta must be positive, got {beta}");
        Self { alpha, beta }
    }

    /// Posterior mean: alpha / (alpha + beta).
    #[must_use]
    pub fn mean(self) -> f64 {
        self.alpha / (self.alpha + self.beta)
    }

    /// Posterior variance: alpha*beta / ((alpha+beta)^2 * (alpha+beta+1)).
    #[must_use]
    pub fn variance(self) -> f64 {
        let ab = self.alpha + self.beta;
        (self.alpha * self.beta) / (ab * ab * (ab + 1.0))
    }

    /// Posterior mode (only defined for alpha > 1, beta > 1).
    #[must_use]
    pub fn mode(self) -> Option<f64> {
        if self.alpha > 1.0 && self.beta > 1.0 {
            Some((self.alpha - 1.0) / (self.alpha + self.beta - 2.0))
        } else {
            None
        }
    }

    /// Quantile function (inverse CDF) via bisection on the regularized
    /// incomplete beta function.
    ///
    /// Returns x such that I_x(alpha, beta) = p.
    #[must_use]
    #[allow(clippy::float_cmp)]
    pub fn quantile(self, p: f64) -> f64 {
        assert!((0.0..=1.0).contains(&p), "p must be in [0, 1], got {p}");
        if p == 0.0 {
            return 0.0;
        }
        if p == 1.0 {
            return 1.0;
        }
        beta_quantile(p, self.alpha, self.beta)
    }

    /// Equal-tailed credible interval at the given confidence level.
    ///
    /// Returns `(lower, upper)` where `lower = quantile((1-conf)/2)` and
    /// `upper = quantile((1+conf)/2)`.
    #[must_use]
    pub fn credible_interval(self, confidence: f64) -> (f64, f64) {
        let tail = (1.0 - confidence) / 2.0;
        (self.quantile(tail), self.quantile(1.0 - tail))
    }
}

// ---------------------------------------------------------------------------
// Numerical routines for Beta distribution
// ---------------------------------------------------------------------------

/// Log-gamma via Lanczos approximation (g=7, n=9 coefficients).
#[allow(clippy::excessive_precision)]
fn ln_gamma(x: f64) -> f64 {
    const COEFFS: [f64; 9] = [
        0.999_999_999_999_809_93,
        676.520_368_121_885_1,
        -1_259.139_216_722_402_8,
        771.323_428_777_653_13,
        -176.615_029_162_140_59,
        12.507_343_278_686_905,
        -0.138_571_095_265_720_12,
        9.984_369_578_019_571_6e-6,
        1.505_632_735_149_311_6e-7,
    ];

    if x < 0.5 {
        // Reflection formula
        let lnpi_sin = (PI * x).sin().abs().ln();
        PI.ln() - lnpi_sin - ln_gamma(1.0 - x)
    } else {
        let x = x - 1.0;
        let mut ag = COEFFS[0];
        for (i, &c) in COEFFS[1..].iter().enumerate() {
            #[allow(clippy::cast_precision_loss)]
            let denom = x + (i as f64) + 1.0;
            ag += c / denom;
        }
        let t = x + 7.5;
        0.5f64.mul_add((2.0 * PI).ln(), (x + 0.5) * t.ln()) - t + ag.ln()
    }
}

/// Log of the Beta function: ln B(a, b) = ln Gamma(a) + ln Gamma(b) - ln Gamma(a+b).
fn ln_beta_fn(a: f64, b: f64) -> f64 {
    ln_gamma(a) + ln_gamma(b) - ln_gamma(a + b)
}

/// Regularized incomplete beta function I_x(a, b) via continued fraction
/// (Lentz's modified method).
///
/// Reference: Numerical Recipes in C, 2nd ed., section 6.4.
#[allow(clippy::many_single_char_names)]
fn regularized_ibeta(x: f64, a: f64, b: f64) -> f64 {
    const EPS: f64 = 1e-15;
    const TINY: f64 = 1e-30;
    const MAX_ITER: usize = 200;

    if x <= 0.0 {
        return 0.0;
    }
    if x >= 1.0 {
        return 1.0;
    }

    // Use symmetry relation when x > (a+1)/(a+b+2) for faster convergence.
    if x > (a + 1.0) / (a + b + 2.0) {
        return 1.0 - regularized_ibeta(1.0 - x, b, a);
    }

    // Prefactor: x^a * (1-x)^b / (a * B(a,b))
    let ln_prefactor = a.mul_add(x.ln(), b * (1.0 - x).ln()) - ln_beta_fn(a, b) - a.ln();
    let prefactor = ln_prefactor.exp();

    // Lentz's modified method (Numerical Recipes betacf).
    // c and d are carried across iterations.
    let qab = a + b;
    let qap = a + 1.0;
    let qam = a - 1.0;

    let mut c = 1.0_f64;
    let mut d = 1.0 - qab * x / qap;
    if d.abs() < TINY {
        d = TINY;
    }
    d = 1.0 / d;
    let mut h = d;

    for m in 1..=MAX_ITER {
        #[allow(clippy::cast_precision_loss)]
        let m_f64 = m as f64;
        let m2 = 2.0 * m_f64;

        // Even step: a_{2m}
        let aa = m_f64 * (b - m_f64) * x / ((qam + m2) * (a + m2));

        d = 1.0 + aa * d;
        if d.abs() < TINY {
            d = TINY;
        }
        c = 1.0 + aa / c;
        if c.abs() < TINY {
            c = TINY;
        }
        d = 1.0 / d;
        h *= d * c;

        // Odd step: a_{2m+1}
        let aa = -((a + m_f64) * (qab + m_f64) * x) / ((a + m2) * (qap + m2));

        d = 1.0 + aa * d;
        if d.abs() < TINY {
            d = TINY;
        }
        c = 1.0 + aa / c;
        if c.abs() < TINY {
            c = TINY;
        }
        d = 1.0 / d;
        let delta = d * c;
        h *= delta;

        if (delta - 1.0).abs() < EPS {
            break;
        }
    }

    prefactor * h
}

/// Beta quantile via bisection on the regularized incomplete beta function.
fn beta_quantile(p: f64, a: f64, b: f64) -> f64 {
    const TOL: f64 = 1e-12;
    const MAX_ITER: usize = 100;

    let mut lo = 0.0_f64;
    let mut hi = 1.0_f64;

    for _ in 0..MAX_ITER {
        let mid = f64::midpoint(lo, hi);
        let cdf = regularized_ibeta(mid, a, b);

        if (cdf - p).abs() < TOL {
            return mid;
        }
        if cdf < p {
            lo = mid;
        } else {
            hi = mid;
        }
    }

    f64::midpoint(lo, hi)
}

// ---------------------------------------------------------------------------
// Prior configuration
// ---------------------------------------------------------------------------

/// Prior hyperparameters for the Bayesian score engine.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct PriorConfig {
    /// Prior alpha for Beta distribution (pseudo-successes).
    /// Default: 1.0 (uniform prior).
    pub alpha: f64,
    /// Prior beta for Beta distribution (pseudo-failures).
    /// Default: 1.0 (uniform prior).
    pub beta: f64,
    /// Confidence level for credible intervals (e.g., 0.95).
    pub confidence_level: f64,
}

impl Default for PriorConfig {
    fn default() -> Self {
        Self {
            alpha: 1.0,
            beta: 1.0,
            confidence_level: 0.95,
        }
    }
}

impl PriorConfig {
    /// Jeffreys prior: Beta(0.5, 0.5).
    #[must_use]
    pub fn jeffreys() -> Self {
        Self {
            alpha: 0.5,
            beta: 0.5,
            ..Default::default()
        }
    }

    /// Haldane prior: Beta(eps, eps) — nearly improper, maximally uninformative.
    #[must_use]
    pub fn haldane() -> Self {
        Self {
            alpha: 0.001,
            beta: 0.001,
            ..Default::default()
        }
    }

    /// Optimistic prior: Beta(2, 1) — slight bias toward pass.
    #[must_use]
    pub fn optimistic() -> Self {
        Self {
            alpha: 2.0,
            beta: 1.0,
            ..Default::default()
        }
    }
}

// ---------------------------------------------------------------------------
// Category posterior
// ---------------------------------------------------------------------------

/// Posterior summary for a single feature category.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CategoryPosterior {
    /// Category name.
    pub category: String,
    /// Beta posterior parameters.
    pub posterior: BetaParams,
    /// Point estimate (posterior mean).
    pub point_estimate: f64,
    /// Lower bound of credible interval.
    pub lower_bound: f64,
    /// Upper bound of credible interval.
    pub upper_bound: f64,
    /// Confidence level used.
    pub confidence_level: f64,
    /// Category weight in global score.
    pub global_weight: f64,
    /// Number of non-excluded features.
    pub effective_sample_size: usize,
    /// Weighted successes (sum of score contributions).
    pub weighted_successes: f64,
    /// Weighted failures.
    pub weighted_failures: f64,
}

// ---------------------------------------------------------------------------
// Conformal prediction band
// ---------------------------------------------------------------------------

/// Conformal prediction band for finite-sample coverage guarantees.
///
/// Calibrated from per-category estimation residuals (gap between observed
/// frequentist pass rate and Bayesian posterior mean). The band widens the
/// credible interval to guarantee distribution-free coverage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConformalBand {
    /// Nominal coverage level (e.g., 0.95).
    pub coverage: f64,
    /// Half-width of the conformal band.
    pub half_width: f64,
    /// Lower bound after conformal adjustment.
    pub lower: f64,
    /// Upper bound after conformal adjustment.
    pub upper: f64,
}

// ---------------------------------------------------------------------------
// Scorecard
// ---------------------------------------------------------------------------

/// Machine-readable scorecard combining Bayesian estimation with conformal bounds.
///
/// This is the primary output consumed by release-gating decisions. The
/// `release_ready` field is `true` iff statistical gates pass and any configured
/// verification-contract enforcement allows release.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BayesianScorecard {
    /// Bead ID for traceability.
    pub bead_id: String,
    /// Schema version.
    pub schema_version: u32,
    /// Prior configuration used.
    pub prior: PriorConfig,
    /// Global point estimate (posterior mean of weighted category scores).
    pub global_point_estimate: f64,
    /// Global lower confidence bound (release-gate threshold).
    pub global_lower_bound: f64,
    /// Global upper confidence bound.
    pub global_upper_bound: f64,
    /// Per-category posteriors.
    pub category_posteriors: BTreeMap<String, CategoryPosterior>,
    /// Conformal band for the global score.
    pub conformal_band: ConformalBand,
    /// Whether the score meets release criteria.
    pub release_ready: bool,
    /// Release threshold (lower bound must exceed this).
    pub release_threshold: f64,
    /// Total features analysed (non-excluded).
    pub effective_features: usize,
    /// Total features in taxonomy (including excluded).
    pub total_features: usize,
    /// Optional verification-contract enforcement payload (bd-1dp9.7.7).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verification_contract: Option<ContractEnforcementOutcome>,
}

impl BayesianScorecard {
    /// Serialize to deterministic JSON.
    ///
    /// # Errors
    ///
    /// Returns `Err` if serialisation fails.
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }

    /// Deserialize from JSON.
    ///
    /// # Errors
    ///
    /// Returns `Err` if the JSON is malformed.
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }
}

// ---------------------------------------------------------------------------
// Engine configuration
// ---------------------------------------------------------------------------

/// Configuration for the Bayesian score engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScoreEngineConfig {
    /// Prior hyperparameters.
    pub prior: PriorConfig,
    /// Release threshold: global lower bound must exceed this.
    pub release_threshold: f64,
    /// Conformal coverage level.
    pub conformal_coverage: f64,
}

impl Default for ScoreEngineConfig {
    fn default() -> Self {
        Self {
            prior: PriorConfig::default(),
            release_threshold: 1.0,
            conformal_coverage: 0.95,
        }
    }
}

// ---------------------------------------------------------------------------
// Engine
// ---------------------------------------------------------------------------

/// Compute the Bayesian scorecard from a feature universe.
///
/// This is the main entry point for the score engine. It computes per-category
/// Beta posteriors, derives credible intervals, calibrates conformal bands,
/// and returns a machine-readable scorecard.
#[must_use]
pub fn compute_bayesian_scorecard(
    universe: &FeatureUniverse,
    config: &ScoreEngineConfig,
) -> BayesianScorecard {
    let mut category_posteriors = BTreeMap::new();
    let mut total_effective = 0_usize;
    let mut category_residuals = Vec::new();

    for cat in FeatureCategory::ALL {
        let features: Vec<_> = universe
            .features
            .values()
            .filter(|f| f.category == cat)
            .collect();

        let mut successes = 0.0_f64;
        let mut failures = 0.0_f64;
        let mut effective = 0_usize;

        for feat in &features {
            if let Some(contrib) = feat.status.score_contribution() {
                let weighted = feat.weight * contrib;
                let weighted_fail = feat.weight * (1.0 - contrib);
                successes += weighted;
                failures += weighted_fail;
                effective += 1;
            }
        }

        total_effective += effective;

        let alpha = config.prior.alpha + successes;
        let beta = config.prior.beta + failures;
        let posterior = BetaParams::new(alpha, beta);

        let point_est = truncate_score(posterior.mean());
        let (lo, hi) = posterior.credible_interval(config.prior.confidence_level);

        // Compute frequentist pass rate for conformal calibration.
        let total_weight = successes + failures;
        let naive_rate = if total_weight > 0.0 {
            successes / total_weight
        } else {
            0.0
        };
        let residual = (naive_rate - point_est).abs();
        category_residuals.push(residual);

        category_posteriors.insert(
            cat.display_name().to_owned(),
            CategoryPosterior {
                category: cat.display_name().to_owned(),
                posterior,
                point_estimate: point_est,
                lower_bound: truncate_score(lo),
                upper_bound: truncate_score(hi.min(1.0)),
                confidence_level: config.prior.confidence_level,
                global_weight: cat.global_weight(),
                effective_sample_size: effective,
                weighted_successes: truncate_score(successes),
                weighted_failures: truncate_score(failures),
            },
        );
    }

    // Global point estimate: weighted sum of category posterior means.
    let global_point: f64 = category_posteriors
        .values()
        .map(|cp| cp.global_weight * cp.point_estimate)
        .sum();

    // Global lower bound: weighted sum of category lower bounds.
    let global_lower: f64 = category_posteriors
        .values()
        .map(|cp| cp.global_weight * cp.lower_bound)
        .sum();

    // Global upper bound: weighted sum of category upper bounds.
    let global_upper: f64 = category_posteriors
        .values()
        .map(|cp| cp.global_weight * cp.upper_bound)
        .sum();

    // Conformal band: empirical quantile of category residuals.
    let conformal_half_width =
        compute_conformal_half_width(&category_residuals, config.conformal_coverage);

    let conformal_lower = (global_point - conformal_half_width).max(0.0);
    let conformal_upper = (global_point + conformal_half_width).min(1.0);

    let conformal_band = ConformalBand {
        coverage: config.conformal_coverage,
        half_width: truncate_score(conformal_half_width),
        lower: truncate_score(conformal_lower),
        upper: truncate_score(conformal_upper),
    };

    // Release decision: the more conservative of the two lower bounds
    // (credible interval and conformal) must exceed threshold.
    let conservative_lower = truncate_score(global_lower.min(conformal_lower));
    let release_ready = conservative_lower >= config.release_threshold;

    BayesianScorecard {
        bead_id: BEAD_ID.to_owned(),
        schema_version: 1,
        prior: config.prior,
        global_point_estimate: truncate_score(global_point),
        global_lower_bound: truncate_score(global_lower),
        global_upper_bound: truncate_score(global_upper.min(1.0)),
        category_posteriors,
        conformal_band,
        release_ready,
        release_threshold: config.release_threshold,
        effective_features: total_effective,
        total_features: universe.features.len(),
        verification_contract: None,
    }
}

/// Compute a scorecard and enforce the parity verification contract for release.
///
/// # Errors
///
/// Returns `Err` if workspace parity evidence report generation fails.
pub fn compute_bayesian_scorecard_with_contract(
    workspace_root: &Path,
    universe: &FeatureUniverse,
    config: &ScoreEngineConfig,
) -> Result<BayesianScorecard, String> {
    let mut scorecard = compute_bayesian_scorecard(universe, config);
    let contract_report = evaluate_workspace_verification_contract(workspace_root)?;
    let enforcement = enforce_gate_decision(scorecard.release_ready, &contract_report);
    scorecard.release_ready = enforcement.final_gate_passed;
    scorecard.verification_contract = Some(enforcement);
    Ok(scorecard)
}

/// Compute conformal half-width from calibration residuals.
///
/// Uses the ceil((1-alpha)(n+1))-th smallest absolute residual, which
/// guarantees distribution-free marginal coverage >= 1 - alpha
/// (Vovk et al., 2005).
fn compute_conformal_half_width(residuals: &[f64], coverage: f64) -> f64 {
    if residuals.is_empty() {
        return 0.0;
    }

    let mut sorted: Vec<f64> = residuals.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    let n = sorted.len();
    #[allow(clippy::cast_precision_loss)]
    let n_f64 = n as f64;

    // Conformal quantile index: ceil(coverage * (n + 1)) - 1, clamped to [0, n-1].
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let idx = ((coverage * (n_f64 + 1.0)).ceil() as usize)
        .saturating_sub(1)
        .min(n - 1);

    sorted[idx]
}

// ---------------------------------------------------------------------------
// Internal unit tests for numerical routines
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::float_cmp)]
mod tests {
    use super::*;

    const TOL: f64 = 1e-6;

    // --- ln_gamma ---

    #[test]
    fn ln_gamma_at_one() {
        // Gamma(1) = 1, ln(1) = 0
        assert!((ln_gamma(1.0)).abs() < TOL);
    }

    #[test]
    fn ln_gamma_at_two() {
        // Gamma(2) = 1, ln(1) = 0
        assert!((ln_gamma(2.0)).abs() < TOL);
    }

    #[test]
    fn ln_gamma_at_half() {
        // Gamma(0.5) = sqrt(pi) ~ 1.7724538509
        let expected = (PI.sqrt()).ln();
        let actual = ln_gamma(0.5);
        assert!(
            (actual - expected).abs() < TOL,
            "ln_gamma(0.5): expected {expected}, got {actual}"
        );
    }

    #[test]
    fn ln_gamma_at_five() {
        // Gamma(5) = 4! = 24, ln(24) ~ 3.178054
        let expected = 24.0_f64.ln();
        let actual = ln_gamma(5.0);
        assert!(
            (actual - expected).abs() < TOL,
            "ln_gamma(5): expected {expected}, got {actual}"
        );
    }

    #[test]
    fn ln_gamma_at_ten() {
        // Gamma(10) = 9! = 362880
        let expected = 362_880.0_f64.ln();
        let actual = ln_gamma(10.0);
        assert!(
            (actual - expected).abs() < TOL,
            "ln_gamma(10): expected {expected}, got {actual}"
        );
    }

    // --- regularized_ibeta ---

    #[test]
    fn ibeta_boundaries() {
        assert_eq!(regularized_ibeta(0.0, 2.0, 3.0), 0.0);
        assert_eq!(regularized_ibeta(1.0, 2.0, 3.0), 1.0);
    }

    #[test]
    fn ibeta_uniform_is_identity() {
        // I_x(1,1) = x for all x
        for &x in &[0.1, 0.25, 0.5, 0.75, 0.9] {
            let actual = regularized_ibeta(x, 1.0, 1.0);
            assert!(
                (actual - x).abs() < TOL,
                "I_{x}(1,1): expected {x}, got {actual}"
            );
        }
    }

    #[test]
    fn ibeta_symmetric_at_half() {
        // I_0.5(a, a) = 0.5 for any a (by symmetry)
        for &a in &[2.0, 3.0, 5.0, 10.0] {
            let actual = regularized_ibeta(0.5, a, a);
            assert!(
                (actual - 0.5).abs() < TOL,
                "I_0.5({a},{a}): expected 0.5, got {actual}"
            );
        }
    }

    #[test]
    fn ibeta_known_value_beta_2_3() {
        // I_x(2,3) = x^2 * (6 - 8x + 3x^2) for x in [0,1]
        // At x = 0.3: 0.09 * (6 - 2.4 + 0.27) = 0.09 * 3.87 = 0.3483
        let x: f64 = 0.3;
        let expected = x * x * ((3.0_f64 * x).mul_add(x, 8.0_f64.mul_add(-x, 6.0)));
        let actual = regularized_ibeta(x, 2.0, 3.0);
        assert!(
            (actual - expected).abs() < 1e-4,
            "I_0.3(2,3): expected {expected}, got {actual}"
        );
    }

    // --- beta_quantile ---

    #[test]
    fn beta_quantile_uniform_median() {
        let q = beta_quantile(0.5, 1.0, 1.0);
        assert!(
            (q - 0.5).abs() < TOL,
            "Beta(1,1) median: expected 0.5, got {q}"
        );
    }

    #[test]
    fn beta_quantile_symmetric_median() {
        let q = beta_quantile(0.5, 3.0, 3.0);
        assert!(
            (q - 0.5).abs() < TOL,
            "Beta(3,3) median: expected 0.5, got {q}"
        );
    }

    #[test]
    fn beta_quantile_monotone() {
        // quantile must be monotonically increasing
        let a = 5.0;
        let b = 3.0;
        let mut prev = 0.0;
        for p in [0.1, 0.25, 0.5, 0.75, 0.9] {
            let q = beta_quantile(p, a, b);
            assert!(
                q >= prev,
                "Beta({a},{b}) quantile not monotone at p={p}: {q} < {prev}"
            );
            prev = q;
        }
    }

    // --- BetaParams ---

    #[test]
    fn beta_params_mean_uniform() {
        let b = BetaParams::new(1.0, 1.0);
        assert!((b.mean() - 0.5).abs() < TOL);
    }

    #[test]
    fn beta_params_mean_asymmetric() {
        let b = BetaParams::new(3.0, 7.0);
        assert!((b.mean() - 0.3).abs() < TOL);
    }

    #[test]
    fn beta_params_variance_uniform() {
        let b = BetaParams::new(1.0, 1.0);
        // Var(Beta(1,1)) = 1/12
        let expected = 1.0 / 12.0;
        assert!(
            (b.variance() - expected).abs() < TOL,
            "Beta(1,1) variance: expected {expected}, got {}",
            b.variance()
        );
    }

    #[test]
    fn beta_params_mode() {
        let b = BetaParams::new(3.0, 5.0);
        // mode = (3-1)/(3+5-2) = 2/6 = 1/3
        assert!(
            (b.mode().unwrap() - 1.0 / 3.0).abs() < TOL,
            "Beta(3,5) mode: expected 1/3, got {:?}",
            b.mode()
        );
    }

    #[test]
    fn beta_params_mode_undefined_for_small_params() {
        assert!(BetaParams::new(0.5, 0.5).mode().is_none());
        assert!(BetaParams::new(1.0, 1.0).mode().is_none());
    }

    #[test]
    fn beta_params_credible_interval_contains_mean() {
        let b = BetaParams::new(10.0, 5.0);
        let (lo, hi) = b.credible_interval(0.95);
        let mean = b.mean();
        assert!(
            lo <= mean && mean <= hi,
            "95% CI [{lo}, {hi}] does not contain mean {mean}"
        );
    }

    #[test]
    fn beta_params_credible_interval_symmetric_for_symmetric_dist() {
        let b = BetaParams::new(10.0, 10.0);
        let (lo, hi) = b.credible_interval(0.95);
        // For symmetric Beta, the CI should be symmetric around 0.5
        assert!(
            ((0.5 - lo) - (hi - 0.5)).abs() < 1e-4,
            "CI for symmetric Beta(10,10) should be symmetric: [{lo}, {hi}]"
        );
    }

    // --- conformal half-width ---

    #[test]
    fn conformal_empty_residuals() {
        assert_eq!(compute_conformal_half_width(&[], 0.95), 0.0);
    }

    #[test]
    fn conformal_single_residual() {
        let residuals = vec![0.05];
        let hw = compute_conformal_half_width(&residuals, 0.95);
        assert!((hw - 0.05).abs() < TOL);
    }

    #[test]
    fn conformal_multiple_residuals() {
        let residuals = vec![0.01, 0.05, 0.02, 0.03, 0.04];
        let hw = compute_conformal_half_width(&residuals, 0.95);
        // With n=5 and coverage=0.95, index = ceil(0.95*6)-1 = ceil(5.7)-1 = 5
        // clamped to n-1 = 4. Sorted residuals: [0.01, 0.02, 0.03, 0.04, 0.05]
        // hw = sorted[4] = 0.05
        assert!((hw - 0.05).abs() < TOL);
    }
}
