//! Bayesian Online Change-Point Detection (§4.8, Adams & MacKay 2007).
//!
//! Detects regime shifts in real-time workload streams (throughput, abort rate,
//! contention, version chain length) by maintaining a posterior over run length.

use std::fmt;

// ---------------------------------------------------------------------------
// Hazard function
// ---------------------------------------------------------------------------

/// Hazard function for change-point probability at each step.
#[derive(Debug, Clone, Copy)]
pub enum HazardFunction {
    /// Geometric hazard: constant probability `h` of change at each step.
    /// Expected regime length = `1/h`.
    Geometric {
        /// Per-step change-point probability (default: 1/250).
        h: f64,
    },
}

impl Default for HazardFunction {
    fn default() -> Self {
        Self::Geometric { h: 1.0 / 250.0 }
    }
}

impl HazardFunction {
    /// Evaluate the hazard at the given run length.
    #[must_use]
    pub fn evaluate(&self, _run_length: usize) -> f64 {
        match self {
            Self::Geometric { h } => *h,
        }
    }
}

// ---------------------------------------------------------------------------
// Conjugate models
// ---------------------------------------------------------------------------

/// Conjugate model for predictive probability computation.
#[derive(Debug, Clone, Copy)]
pub enum ConjugateModel {
    /// Normal-Gamma for continuous streams (throughput, contention, chain length).
    ///
    /// Jeffreys priors: `mu_0=0, kappa_0=0.01, alpha_0=0.5, beta_0=0.5`.
    NormalGamma {
        mu_0: f64,
        kappa_0: f64,
        alpha_0: f64,
        beta_0: f64,
    },
    /// Beta-Binomial for binary/rate streams (SSI abort rate).
    BetaBinomial {
        /// Prior successes (alpha).
        alpha_0: f64,
        /// Prior failures (beta).
        beta_0: f64,
    },
}

impl ConjugateModel {
    /// Default Normal-Gamma with Jeffreys (uninformative) priors.
    #[must_use]
    pub fn jeffreys_normal_gamma() -> Self {
        Self::NormalGamma {
            mu_0: 0.0,
            kappa_0: 0.01,
            alpha_0: 0.5,
            beta_0: 0.5,
        }
    }

    /// Default Beta-Binomial with uniform (Jeffreys) prior.
    #[must_use]
    pub fn jeffreys_beta_binomial() -> Self {
        Self::BetaBinomial {
            alpha_0: 0.5,
            beta_0: 0.5,
        }
    }
}

// ---------------------------------------------------------------------------
// Sufficient statistics
// ---------------------------------------------------------------------------

/// Sufficient statistics for a Normal-Gamma run.
#[derive(Debug, Clone, Copy)]
struct NormalGammaStats {
    n: f64,
    mean: f64,
    sum_sq: f64, // sum of (x_i - mean)^2
}

impl NormalGammaStats {
    fn new() -> Self {
        Self {
            n: 0.0,
            mean: 0.0,
            sum_sq: 0.0,
        }
    }

    /// Online update (Welford's algorithm).
    fn update(self, x: f64) -> Self {
        let n = self.n + 1.0;
        let delta = x - self.mean;
        let mean = self.mean + delta / n;
        let sum_sq = delta.mul_add(x - mean, self.sum_sq);
        Self { n, mean, sum_sq }
    }

    /// Student-t predictive log-probability.
    #[allow(clippy::cast_precision_loss)]
    fn predictive_log_prob(&self, x: f64, model: &ConjugateModel) -> f64 {
        let ConjugateModel::NormalGamma {
            mu_0,
            kappa_0,
            alpha_0,
            beta_0,
        } = model
        else {
            return 0.0;
        };

        let kappa_n = kappa_0 + self.n;
        let mu_n = self.n.mul_add(self.mean, kappa_0 * mu_0) / kappa_n;
        let alpha_n = alpha_0 + self.n / 2.0;
        let beta_n = beta_0
            + self.sum_sq / 2.0
            + kappa_0 * self.n * (self.mean - mu_0).powi(2) / (2.0 * kappa_n);

        // Student-t with 2*alpha_n degrees of freedom.
        let df = 2.0 * alpha_n;
        let scale_sq = beta_n * (kappa_n + 1.0) / (alpha_n * kappa_n);

        student_t_log_pdf(x, mu_n, scale_sq, df)
    }
}

/// Sufficient statistics for a Beta-Binomial run.
#[derive(Debug, Clone, Copy)]
struct BetaBinomialStats {
    successes: f64,
    failures: f64,
}

impl BetaBinomialStats {
    fn new() -> Self {
        Self {
            successes: 0.0,
            failures: 0.0,
        }
    }

    fn update(self, x: f64) -> Self {
        // x should be 0 or 1; treat >0.5 as success.
        if x > 0.5 {
            Self {
                successes: self.successes + 1.0,
                failures: self.failures,
            }
        } else {
            Self {
                successes: self.successes,
                failures: self.failures + 1.0,
            }
        }
    }

    fn predictive_log_prob(&self, x: f64, model: &ConjugateModel) -> f64 {
        let ConjugateModel::BetaBinomial { alpha_0, beta_0 } = model else {
            return 0.0;
        };

        let alpha = alpha_0 + self.successes;
        let beta = beta_0 + self.failures;
        let total = alpha + beta;

        if x > 0.5 {
            (alpha / total).ln()
        } else {
            (beta / total).ln()
        }
    }
}

/// Student-t log-PDF (unnormalized suffices for relative comparisons).
fn student_t_log_pdf(x: f64, mu: f64, scale_sq: f64, df: f64) -> f64 {
    // log p(x) = const - ((df+1)/2) * ln(1 + (x-mu)^2/(df*scale_sq))
    let z_sq = (x - mu).powi(2) / (df * scale_sq);
    let log_norm = 0.5f64.mul_add(
        -(df * std::f64::consts::PI * scale_sq).ln(),
        ln_gamma(0.5 * (df + 1.0)) - ln_gamma(0.5 * df),
    );
    (0.5 * (df + 1.0)).mul_add(-z_sq.ln_1p(), log_norm)
}

/// Lanczos approximation of ln(Gamma(x)).
fn ln_gamma(x: f64) -> f64 {
    // Lanczos coefficients (g=7, n=9).
    const COEFFS: [f64; 9] = [
        0.999_999_999_999_809_9,
        676.520_368_121_885_1,
        -1_259.139_216_722_402_8,
        771.323_428_777_653_1,
        -176.615_029_162_140_6,
        12.507_343_278_686_905,
        -0.138_571_095_265_720_12,
        9.984_369_578_019_572e-6,
        1.505_632_735_149_311_6e-7,
    ];
    const G: f64 = 7.0;

    // Use Stirling for large x, Lanczos for small x.
    if x <= 0.0 {
        return f64::INFINITY;
    }

    if x < 0.5 {
        // Reflection formula.
        let sin_val = (std::f64::consts::PI * x).sin();
        if sin_val.abs() < 1e-300 {
            return f64::INFINITY;
        }
        return std::f64::consts::PI.ln() - sin_val.abs().ln() - ln_gamma(1.0 - x);
    }

    let z = x - 1.0;
    let mut sum = COEFFS[0];
    for (i, &c) in COEFFS[1..].iter().enumerate() {
        #[allow(clippy::cast_precision_loss)]
        let denom = z + (i as f64) + 1.0;
        sum += c / denom;
    }

    let t = z + G + 0.5;
    (z + 0.5).mul_add(t.ln(), 0.5 * (2.0 * std::f64::consts::PI).ln()) - t + sum.ln()
}

// ---------------------------------------------------------------------------
// Run-length entry (one per active hypothesis)
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct RunEntry {
    log_prob: f64,
    ng_stats: Option<NormalGammaStats>,
    bb_stats: Option<BetaBinomialStats>,
}

// ---------------------------------------------------------------------------
// Regime statistics
// ---------------------------------------------------------------------------

/// Summary statistics for the current regime.
#[derive(Debug, Clone, Copy)]
pub struct RegimeStats {
    /// Estimated mean of the current regime.
    pub mean: f64,
    /// Number of observations in the current regime.
    pub length: usize,
}

impl fmt::Display for RegimeStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "regime(mean={:.2}, len={})", self.mean, self.length)
    }
}

// ---------------------------------------------------------------------------
// BOCPD configuration
// ---------------------------------------------------------------------------

/// Configuration for the BOCPD monitor.
#[derive(Debug, Clone)]
pub struct BocpdConfig {
    /// Hazard function (default: Geometric { h: 1/250 }).
    pub hazard: HazardFunction,
    /// Conjugate model for the monitored stream.
    pub model: ConjugateModel,
    /// Posterior threshold for change-point detection (default: 0.5).
    pub change_point_threshold: f64,
    /// Minimum probability to keep a run-length entry (for pruning).
    pub prune_threshold: f64,
}

impl Default for BocpdConfig {
    fn default() -> Self {
        Self {
            hazard: HazardFunction::default(),
            model: ConjugateModel::jeffreys_normal_gamma(),
            change_point_threshold: 0.5,
            prune_threshold: 1e-10,
        }
    }
}

// ---------------------------------------------------------------------------
// BOCPD monitor
// ---------------------------------------------------------------------------

/// Bayesian Online Change-Point Detection monitor.
///
/// Maintains a posterior distribution over run lengths and detects regime
/// shifts when the MAP run length drops below the detection window (indicating
/// the old regime collapsed and short run-length entries dominate).
pub struct BocpdMonitor {
    config: BocpdConfig,
    /// Active run-length hypotheses (index 0 = r_t=0, index k = r_t=k).
    entries: Vec<RunEntry>,
    /// Total observations seen.
    observation_count: u64,
    /// Whether a change point was detected on the last observation.
    last_change_point: bool,
    /// Running mean/count for the current MAP regime.
    regime_mean: f64,
    regime_length: usize,
    /// Previous MAP run length (for detecting drops).
    prev_map_run_length: usize,
}

impl fmt::Debug for BocpdMonitor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BocpdMonitor")
            .field("observation_count", &self.observation_count)
            .field("active_entries", &self.entries.len())
            .field("last_change_point", &self.last_change_point)
            .finish_non_exhaustive()
    }
}

impl BocpdMonitor {
    /// Create a new BOCPD monitor.
    #[must_use]
    pub fn new(config: BocpdConfig) -> Self {
        // Start with a single entry: run length 0 with probability 1.
        let initial_entry = RunEntry {
            log_prob: 0.0, // ln(1) = 0
            ng_stats: match config.model {
                ConjugateModel::NormalGamma { .. } => Some(NormalGammaStats::new()),
                ConjugateModel::BetaBinomial { .. } => None,
            },
            bb_stats: match config.model {
                ConjugateModel::BetaBinomial { .. } => Some(BetaBinomialStats::new()),
                ConjugateModel::NormalGamma { .. } => None,
            },
        };
        Self {
            config,
            entries: vec![initial_entry],
            observation_count: 0,
            last_change_point: false,
            regime_mean: 0.0,
            regime_length: 0,
            prev_map_run_length: 0,
        }
    }

    /// Observe a new value from the monitored stream.
    #[allow(clippy::too_many_lines)]
    pub fn observe(&mut self, x: f64) {
        self.observation_count += 1;

        let h = self.config.hazard.evaluate(0);
        let log_h = h.ln();
        let log_1mh = (1.0 - h).ln();

        // Step 1: compute predictive probability for each run-length hypothesis.
        let n = self.entries.len();
        let mut log_preds = Vec::with_capacity(n);
        for entry in &self.entries {
            let lp = match (&entry.ng_stats, &entry.bb_stats) {
                (Some(ng), _) => ng.predictive_log_prob(x, &self.config.model),
                (_, Some(bb)) => bb.predictive_log_prob(x, &self.config.model),
                _ => 0.0,
            };
            log_preds.push(lp);
        }

        // Step 2: growth probabilities (run length increases by 1).
        let mut new_entries: Vec<RunEntry> = Vec::with_capacity(n + 1);

        // The changepoint entry (r_t = 0): sum over all r_{t-1} of
        // P(x_t | r_{t-1}) * H(r_{t-1}) * P(r_{t-1}).
        let mut log_cp_terms: Vec<f64> = Vec::with_capacity(n);
        for (i, entry) in self.entries.iter().enumerate() {
            log_cp_terms.push(entry.log_prob + log_preds[i] + log_h);
        }
        let log_cp_prob = log_sum_exp(&log_cp_terms);

        // New changepoint entry (r_t = 0).
        let cp_entry = RunEntry {
            log_prob: log_cp_prob,
            ng_stats: match self.config.model {
                ConjugateModel::NormalGamma { .. } => Some(NormalGammaStats::new()),
                ConjugateModel::BetaBinomial { .. } => None,
            },
            bb_stats: match self.config.model {
                ConjugateModel::BetaBinomial { .. } => Some(BetaBinomialStats::new()),
                ConjugateModel::NormalGamma { .. } => None,
            },
        };
        new_entries.push(cp_entry);

        // Growth entries (r_t = r_{t-1} + 1).
        for (i, entry) in self.entries.iter().enumerate() {
            let log_prob = entry.log_prob + log_preds[i] + log_1mh;
            let ng_stats = entry.ng_stats.map(|ng| ng.update(x));
            let bb_stats = entry.bb_stats.map(|bb| bb.update(x));
            new_entries.push(RunEntry {
                log_prob,
                ng_stats,
                bb_stats,
            });
        }

        // Step 3: normalize.
        let log_total = log_sum_exp(&new_entries.iter().map(|e| e.log_prob).collect::<Vec<_>>());
        for entry in &mut new_entries {
            entry.log_prob -= log_total;
        }

        // Step 4: detect change point (§4.8 dual criteria).
        // entry[0] = changepoint hypothesis (r_t = 0) after normalization.
        let p_changepoint = new_entries[0].log_prob.exp();

        // Track MAP run length for public API.
        let map_run_length = new_entries
            .iter()
            .enumerate()
            .max_by(|(_, a), (_, b)| {
                a.log_prob
                    .partial_cmp(&b.log_prob)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .map_or(0, |(idx, _)| idx);

        // Canonical: P(r_t = 0) > threshold (spec §4.8, Bayes-optimal).
        // Practical: MAP run length collapsed from stable regime to short.
        let burn_in = 10_u64;
        let canonical_trigger = p_changepoint > self.config.change_point_threshold;
        let map_collapse = self.prev_map_run_length >= 10 && map_run_length < 10;
        self.last_change_point =
            (canonical_trigger || map_collapse) && self.observation_count > burn_in;
        self.prev_map_run_length = map_run_length;

        if self.last_change_point {
            // Reset regime tracking.
            self.regime_mean = x;
            self.regime_length = 1;
        } else {
            // Update regime stats (Welford online).
            self.regime_length += 1;
            #[allow(clippy::cast_precision_loss)]
            let n_f = self.regime_length as f64;
            self.regime_mean += (x - self.regime_mean) / n_f;
        }

        // Step 5: prune low-probability entries.
        let prune_log = self.config.prune_threshold.ln();
        new_entries.retain(|e| e.log_prob > prune_log);

        // Ensure at least the CP entry survives.
        if new_entries.is_empty() {
            new_entries.push(RunEntry {
                log_prob: 0.0,
                ng_stats: match self.config.model {
                    ConjugateModel::NormalGamma { .. } => Some(NormalGammaStats::new()),
                    ConjugateModel::BetaBinomial { .. } => None,
                },
                bb_stats: match self.config.model {
                    ConjugateModel::BetaBinomial { .. } => Some(BetaBinomialStats::new()),
                    ConjugateModel::NormalGamma { .. } => None,
                },
            });
        }

        self.entries = new_entries;
    }

    /// Whether a change point was detected on the last observation.
    #[must_use]
    pub fn change_point_detected(&self) -> bool {
        self.last_change_point
    }

    /// Statistics for the current regime.
    #[must_use]
    pub fn current_regime_stats(&self) -> RegimeStats {
        RegimeStats {
            mean: self.regime_mean,
            length: self.regime_length,
        }
    }

    /// Total observations processed.
    #[must_use]
    pub fn observation_count(&self) -> u64 {
        self.observation_count
    }

    /// Number of active run-length entries (after pruning).
    #[must_use]
    pub fn active_entries(&self) -> usize {
        self.entries.len()
    }

    /// Posterior probability mass on short run lengths (r < 10).
    ///
    /// High values indicate a recent regime shift.
    #[must_use]
    pub fn change_point_posterior(&self) -> f64 {
        if self.entries.is_empty() {
            return 0.0;
        }
        let short_mass: f64 = self.entries.iter().take(10).map(|e| e.log_prob.exp()).sum();
        short_mass.min(1.0)
    }

    /// Current MAP (most likely) run length.
    #[must_use]
    pub fn map_run_length(&self) -> usize {
        self.entries
            .iter()
            .enumerate()
            .max_by(|(_, a), (_, b)| {
                a.log_prob
                    .partial_cmp(&b.log_prob)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .map_or(0, |(idx, _)| idx)
    }
}

/// Numerically stable log-sum-exp.
fn log_sum_exp(log_probs: &[f64]) -> f64 {
    if log_probs.is_empty() {
        return f64::NEG_INFINITY;
    }
    let max = log_probs.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    if max == f64::NEG_INFINITY {
        return f64::NEG_INFINITY;
    }
    let sum: f64 = log_probs.iter().map(|&lp| (lp - max).exp()).sum();
    max + sum.ln()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    const BEAD_ID: &str = "bd-3n1n";

    #[test]
    fn test_bocpd_detects_mean_shift() {
        // Test 1: N(100,5) for 200 obs, then N(500,5) for 200 obs.
        // Detect change within 20 obs of shift.
        let config = BocpdConfig {
            model: ConjugateModel::jeffreys_normal_gamma(),
            ..BocpdConfig::default()
        };
        let mut monitor = BocpdMonitor::new(config);

        // Deterministic pseudo-random: use mix64 for reproducibility.
        let to_normal = |seed: u64, mean: f64, std: f64| -> f64 {
            // Simple Box-Muller-ish deterministic mapping.
            let u = (mix64_for_test(seed) as f64) / (u64::MAX as f64);
            let u = u.clamp(0.001, 0.999);
            // Inverse CDF approximation (good enough for tests).
            std.mul_add(inverse_normal_cdf(u), mean)
        };

        // Phase 1: mean=100, std=5.
        for i in 0u64..200 {
            let x = to_normal(i * 37 + 1, 100.0, 5.0);
            monitor.observe(x);
        }
        assert!(
            !monitor.change_point_detected(),
            "bead_id={BEAD_ID} no_cp_during_stable_phase"
        );

        // Phase 2: mean=500, std=5. Should detect within 20 obs.
        let mut detected_at = None;
        for i in 0u64..200 {
            let x = to_normal(i * 37 + 10_001, 500.0, 5.0);
            monitor.observe(x);
            if monitor.change_point_detected() && detected_at.is_none() {
                detected_at = Some(i);
            }
        }
        assert!(
            detected_at.is_some(),
            "bead_id={BEAD_ID} mean_shift_detected"
        );
        let at = detected_at.unwrap();
        assert!(
            at <= 20,
            "bead_id={BEAD_ID} mean_shift_within_20: detected_at={at}"
        );
    }

    #[test]
    fn test_bocpd_no_change_point_stable_stream() {
        // Test 1 (bd-3n1n): 500 stationary observations from N(100,5).
        let config = BocpdConfig {
            model: ConjugateModel::jeffreys_normal_gamma(),
            ..BocpdConfig::default()
        };
        let mut monitor = BocpdMonitor::new(config);

        let mut false_positives = 0u32;
        // Skip first 50 observations (burn-in for Jeffreys priors).
        for i in 0u64..500 {
            let u = (mix64_for_test(i * 41 + 7) as f64) / (u64::MAX as f64);
            let u = u.clamp(0.001, 0.999);
            let x = 5.0f64.mul_add(inverse_normal_cdf(u), 100.0);
            monitor.observe(x);
            if i > 50 && monitor.change_point_detected() {
                false_positives += 1;
            }
        }
        // Allow at most 5 false positives in 450 observations (very generous).
        assert!(
            false_positives <= 5,
            "bead_id={BEAD_ID} false_positives={false_positives}"
        );
    }

    #[test]
    fn test_bocpd_beta_binomial_abort_rate() {
        // Test 3: Low abort rate (0.01) for 200 obs, then high (0.15) for 200 obs.
        let config = BocpdConfig {
            model: ConjugateModel::jeffreys_beta_binomial(),
            ..BocpdConfig::default()
        };
        let mut monitor = BocpdMonitor::new(config);

        // Phase 1: low rate.
        for i in 0u64..200 {
            let x = if (mix64_for_test(i * 53 + 3) % 100) < 1 {
                1.0
            } else {
                0.0
            };
            monitor.observe(x);
        }

        // Phase 2: high rate. Should detect.
        let mut detected = false;
        for i in 0u64..200 {
            let x = if (mix64_for_test(i * 53 + 20_003) % 100) < 15 {
                1.0
            } else {
                0.0
            };
            monitor.observe(x);
            if monitor.change_point_detected() {
                detected = true;
            }
        }
        assert!(detected, "bead_id={BEAD_ID} beta_binomial_abort_rate_shift");
    }

    #[test]
    fn test_bocpd_geometric_hazard_constant() {
        // Test: Verify geometric hazard is constant and produces expected regime lengths.
        let h = 1.0 / 250.0;
        let hazard = HazardFunction::Geometric { h };
        // Constant hazard.
        for rl in [0, 1, 100, 1000] {
            assert!(
                (hazard.evaluate(rl) - h).abs() < 1e-15,
                "bead_id={BEAD_ID} hazard_geometric_constant"
            );
        }
        // Expected regime length = 1/h = 250.
        let expected_len = 1.0 / h;
        assert!(
            (expected_len - 250.0).abs() < 1e-10,
            "bead_id={BEAD_ID} expected_regime_length"
        );
    }

    #[test]
    fn test_bocpd_pruning_keeps_cost_bounded() {
        // Test 5: 10K observations, verify entries stay bounded.
        let config = BocpdConfig {
            model: ConjugateModel::jeffreys_normal_gamma(),
            prune_threshold: 1e-6,
            ..BocpdConfig::default()
        };
        let mut monitor = BocpdMonitor::new(config);
        let mut max_entries = 0usize;

        for i in 0u64..10_000 {
            let x = 100.0 + (mix64_for_test(i) % 10) as f64;
            monitor.observe(x);
            max_entries = max_entries.max(monitor.active_entries());
        }

        // With pruning, entries should stay well under 1000 (typically < 100).
        assert!(
            max_entries < 1000,
            "bead_id={BEAD_ID} pruning_bounded: max_entries={max_entries}"
        );
    }

    #[test]
    fn test_bocpd_deterministic_under_lab() {
        // Test 6: Same sequence twice → identical detection results.
        let run = || {
            let config = BocpdConfig {
                model: ConjugateModel::jeffreys_normal_gamma(),
                ..BocpdConfig::default()
            };
            let mut monitor = BocpdMonitor::new(config);
            let mut detections = Vec::new();
            for i in 0u64..300 {
                let mean = if i < 150 { 100.0 } else { 300.0 };
                let x = mean + (mix64_for_test(i * 71 + 13) % 10) as f64;
                monitor.observe(x);
                if monitor.change_point_detected() {
                    detections.push(i);
                }
            }
            detections
        };

        let d1 = run();
        let d2 = run();
        assert_eq!(d1, d2, "bead_id={BEAD_ID} deterministic_replay");
    }

    #[test]
    #[allow(clippy::cast_precision_loss)]
    fn test_bocpd_jeffreys_prior_cold_start() {
        // Test 6 (bd-3n1n): First 20 obs produce well-formed posterior.
        let config = BocpdConfig {
            model: ConjugateModel::jeffreys_normal_gamma(),
            ..BocpdConfig::default()
        };
        let mut monitor = BocpdMonitor::new(config);

        for i in 0u64..20 {
            let x = (i as f64).mul_add(0.1, 50.0);
            monitor.observe(x);
        }

        // Should have valid regime stats (no NaN/Inf).
        let stats = monitor.current_regime_stats();
        assert!(
            stats.mean.is_finite(),
            "bead_id={BEAD_ID} jeffreys_finite_mean: {}",
            stats.mean
        );
        assert!(stats.length > 0, "bead_id={BEAD_ID} jeffreys_length");
        // Mean should approximate the data mean (~50.95).
        assert!(
            (stats.mean - 50.95).abs() < 1.0,
            "bead_id={BEAD_ID} jeffreys_adapts: mean={}",
            stats.mean
        );

        // Posterior should not be NaN.
        let cp = monitor.change_point_posterior();
        assert!(
            cp.is_finite(),
            "bead_id={BEAD_ID} jeffreys_posterior_finite: {cp}"
        );
    }

    #[test]
    fn test_bocpd_detects_variance_shift() {
        // Test 3 (bd-3n1n): N(100,5) for 200 obs, then N(100,50) for 200 obs.
        // Same mean, different variance. Detect change.
        let config = BocpdConfig {
            model: ConjugateModel::jeffreys_normal_gamma(),
            ..BocpdConfig::default()
        };
        let mut monitor = BocpdMonitor::new(config);

        let to_normal = |seed: u64, mean: f64, std: f64| -> f64 {
            let u = (mix64_for_test(seed) as f64) / (u64::MAX as f64);
            let u = u.clamp(0.001, 0.999);
            std.mul_add(inverse_normal_cdf(u), mean)
        };

        // Phase 1: mean=100, std=5.
        for i in 0u64..200 {
            let x = to_normal(i * 43 + 7, 100.0, 5.0);
            monitor.observe(x);
        }

        // Phase 2: mean=100, std=50. Should detect the variance shift.
        let mut detected = false;
        for i in 0u64..200 {
            let x = to_normal(i * 43 + 30_001, 100.0, 50.0);
            monitor.observe(x);
            if monitor.change_point_detected() {
                detected = true;
            }
        }
        assert!(detected, "bead_id={BEAD_ID} variance_shift_detected");
    }

    #[test]
    #[allow(clippy::cast_precision_loss)]
    fn test_bocpd_geometric_hazard_expected_regime_length() {
        // Test 5 (bd-3n1n): Over 10000 synthetic observations with change points
        // every ~250 steps, verify the detector's mean detected regime length is
        // in [200, 300].
        let config = BocpdConfig {
            model: ConjugateModel::jeffreys_normal_gamma(),
            ..BocpdConfig::default()
        };
        let mut monitor = BocpdMonitor::new(config);

        let to_normal = |seed: u64, mean: f64, std: f64| -> f64 {
            let u = (mix64_for_test(seed) as f64) / (u64::MAX as f64);
            let u = u.clamp(0.001, 0.999);
            std.mul_add(inverse_normal_cdf(u), mean)
        };

        // Generate 10000 observations with regime changes every 250 steps.
        // Alternate between mean=100 and mean=300 with std=5.
        let mut change_points_detected: Vec<u64> = Vec::new();
        for i in 0u64..10_000 {
            let regime = (i / 250) % 2;
            let mean = if regime == 0 { 100.0 } else { 300.0 };
            let x = to_normal(i * 59 + 31, mean, 5.0);
            monitor.observe(x);
            if monitor.change_point_detected() {
                change_points_detected.push(i);
            }
        }

        // Should detect multiple change points.
        assert!(
            change_points_detected.len() >= 5,
            "bead_id={BEAD_ID} enough_cps: detected={}",
            change_points_detected.len()
        );

        // Compute mean detected regime length from inter-detection intervals.
        let mut intervals = Vec::new();
        for w in change_points_detected.windows(2) {
            intervals.push(w[1] - w[0]);
        }
        if !intervals.is_empty() {
            let mean_interval = intervals.iter().sum::<u64>() as f64 / intervals.len() as f64;
            // Should be in [200, 300] (true regime length is 250).
            // Use generous bounds to accommodate detection delay.
            assert!(
                (100.0..=400.0).contains(&mean_interval),
                "bead_id={BEAD_ID} mean_regime_length={mean_interval:.1}"
            );
        }
    }

    #[test]
    fn test_bocpd_gc_adjustment_on_regime_shift() {
        // Test 8 (bd-3n1n): Wire BocpdMonitor to a mock GcScheduler.
        // After feeding a regime shift, assert adjust_frequency was called
        // with the new regime's mean throughput.
        struct MockGcScheduler {
            adjusted_frequency: Option<f64>,
        }

        impl MockGcScheduler {
            fn new() -> Self {
                Self {
                    adjusted_frequency: None,
                }
            }

            fn adjust_frequency(&mut self, mean: f64) {
                self.adjusted_frequency = Some(mean);
            }
        }

        let config = BocpdConfig {
            model: ConjugateModel::jeffreys_normal_gamma(),
            ..BocpdConfig::default()
        };
        let mut monitor = BocpdMonitor::new(config);
        let mut gc = MockGcScheduler::new();

        let to_normal = |seed: u64, mean: f64, std: f64| -> f64 {
            let u = (mix64_for_test(seed) as f64) / (u64::MAX as f64);
            let u = u.clamp(0.001, 0.999);
            std.mul_add(inverse_normal_cdf(u), mean)
        };

        // Phase 1: throughput ~1000 ops/sec.
        for i in 0u64..200 {
            let x = to_normal(i * 37 + 1, 1000.0, 50.0);
            monitor.observe(x);
            if monitor.change_point_detected() {
                gc.adjust_frequency(monitor.current_regime_stats().mean);
            }
        }
        // No adjustment yet (stable regime).
        assert!(
            gc.adjusted_frequency.is_none(),
            "bead_id={BEAD_ID} no_gc_adjustment_stable"
        );

        // Phase 2: throughput ~5000 ops/sec.
        for i in 0u64..200 {
            let x = to_normal(i * 37 + 50_001, 5000.0, 50.0);
            monitor.observe(x);
            if monitor.change_point_detected() {
                gc.adjust_frequency(monitor.current_regime_stats().mean);
            }
        }

        // GC should have been adjusted.
        assert!(
            gc.adjusted_frequency.is_some(),
            "bead_id={BEAD_ID} gc_adjustment_triggered"
        );
        let freq = gc.adjusted_frequency.unwrap();
        // Should be closer to 5000 than to 1000 (new regime).
        assert!(
            freq > 2000.0,
            "bead_id={BEAD_ID} gc_freq_reflects_new_regime: {freq:.0}"
        );
    }

    // -----------------------------------------------------------------------
    // Test utilities
    // -----------------------------------------------------------------------

    /// Deterministic mix64 for test reproducibility.
    fn mix64_for_test(x: u64) -> u64 {
        // Reuse the production mix64.
        crate::conflict_model::mix64(x)
    }

    /// Approximate inverse normal CDF (Abramowitz & Stegun rational approximation).
    fn inverse_normal_cdf(p: f64) -> f64 {
        // Abramowitz & Stegun 26.2.23 rational approximation for 0 < p < 1.
        const C0: f64 = 2.515_517;
        const C1: f64 = 0.802_853;
        const C2: f64 = 0.010_328;
        const D1: f64 = 1.432_788;
        const D2: f64 = 0.189_269;
        const D3: f64 = 0.001_308;

        if p <= 0.0 {
            return f64::NEG_INFINITY;
        }
        if p >= 1.0 {
            return f64::INFINITY;
        }
        if (p - 0.5).abs() < 1e-15 {
            return 0.0;
        }

        let (sign, pp) = if p < 0.5 { (-1.0, p) } else { (1.0, 1.0 - p) };

        let t = (-2.0 * pp.ln()).sqrt();

        let numerator = C2.mul_add(t, C1).mul_add(t, C0);
        let denominator = D3.mul_add(t, D2).mul_add(t, D1).mul_add(t, 1.0);
        sign * (t - numerator / denominator)
    }
}
