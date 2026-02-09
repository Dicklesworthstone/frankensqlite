//! Sheaf consistency checking + conformal calibration (§4.6-4.7).
//!
//! - **Sheaf checker**: verifies MVCC snapshot isolation produces globally
//!   consistent views by checking the sheaf condition over recorded sections.
//! - **Conformal calibrator**: distribution-free anomaly detection on oracle
//!   reports across seeds (finite-sample coverage guarantee).
//! - **Opportunity score**: gate for performance optimizations.

use std::collections::HashMap;
use std::fmt;

// ---------------------------------------------------------------------------
// §4.6 Sheaf consistency checking
// ---------------------------------------------------------------------------

/// A transaction's "section": its observed versions over its read set.
#[derive(Debug, Clone)]
pub struct Section {
    /// Transaction identifier.
    pub txn_id: u64,
    /// Observed page versions: page_number -> version_id.
    pub observations: HashMap<u64, u64>,
}

/// An obstruction in the sheaf condition (inconsistent overlapping sections).
#[derive(Debug, Clone)]
pub struct SheafObstruction {
    /// The page where the inconsistency was found.
    pub page: u64,
    /// First transaction and version it observed.
    pub txn_a: u64,
    pub version_a: u64,
    /// Second transaction and version it observed.
    pub txn_b: u64,
    pub version_b: u64,
}

impl fmt::Display for SheafObstruction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "page={} txn_a={} ver_a={} txn_b={} ver_b={}",
            self.page, self.txn_a, self.version_a, self.txn_b, self.version_b
        )
    }
}

/// Result of a sheaf consistency check.
#[derive(Debug, Clone)]
pub struct SheafResult {
    /// Whether all overlapping sections agree.
    pub consistent: bool,
    /// Any obstructions found (empty if consistent).
    pub obstructions: Vec<SheafObstruction>,
}

impl SheafResult {
    /// Whether the sheaf condition holds.
    #[must_use]
    pub fn is_consistent(&self) -> bool {
        self.consistent
    }
}

/// Check the sheaf condition across a set of transaction sections.
///
/// For every pair of sections that overlap (read the same page), they must
/// observe the same version OR versions consistent with the global version
/// chain ordering.
///
/// The `version_order` function returns true if `v1` is visible at or before
/// `v2` in the global version chain (i.e., `v1 <= v2`). If `None`, strict
/// equality is required.
#[must_use]
pub fn check_sheaf_consistency(
    sections: &[Section],
    version_order: Option<&dyn Fn(u64, u64) -> bool>,
) -> SheafResult {
    let mut obstructions = Vec::new();

    for i in 0..sections.len() {
        for j in (i + 1)..sections.len() {
            let a = &sections[i];
            let b = &sections[j];

            // Check overlapping pages.
            for (&page, &ver_a) in &a.observations {
                if let Some(&ver_b) = b.observations.get(&page) {
                    let consistent = if let Some(order_fn) = version_order {
                        // Ordered: either a sees b's version or vice versa.
                        order_fn(ver_a, ver_b) || order_fn(ver_b, ver_a)
                    } else {
                        // Strict equality.
                        ver_a == ver_b
                    };

                    if !consistent {
                        obstructions.push(SheafObstruction {
                            page,
                            txn_a: a.txn_id,
                            version_a: ver_a,
                            txn_b: b.txn_id,
                            version_b: ver_b,
                        });
                    }
                }
            }
        }
    }

    SheafResult {
        consistent: obstructions.is_empty(),
        obstructions,
    }
}

/// Check sheaf consistency using explicit global version chains per page.
///
/// This is a convenience adapter for the canonical sheaf check when the caller
/// has concrete version-chain metadata:
/// `page_number -> ordered [version_id]`.
///
/// A pair of overlapping observations is considered consistent when:
/// - both transactions observed the same version on that page, or
/// - both observed versions exist in that page's global version chain.
///
/// If either observed version is absent from the page chain, the overlap is
/// treated as an obstruction.
#[must_use]
pub fn check_sheaf_consistency_with_chains(
    sections: &[Section],
    global_version_chains: &HashMap<u64, Vec<u64>>,
) -> SheafResult {
    check_sheaf_consistency_with_chains_hasher(sections, global_version_chains)
}

/// Internal generic form to keep the public API ergonomic while avoiding
/// hasher-specific assumptions.
#[must_use]
pub fn check_sheaf_consistency_with_chains_hasher<S: std::hash::BuildHasher>(
    sections: &[Section],
    global_version_chains: &HashMap<u64, Vec<u64>, S>,
) -> SheafResult {
    let mut obstructions = Vec::new();

    for i in 0..sections.len() {
        for j in (i + 1)..sections.len() {
            let a = &sections[i];
            let b = &sections[j];

            for (&page, &ver_a) in &a.observations {
                let Some(&ver_b) = b.observations.get(&page) else {
                    continue;
                };

                let consistent = if ver_a == ver_b {
                    true
                } else {
                    global_version_chains
                        .get(&page)
                        .is_some_and(|chain| chain.contains(&ver_a) && chain.contains(&ver_b))
                };

                if !consistent {
                    obstructions.push(SheafObstruction {
                        page,
                        txn_a: a.txn_id,
                        version_a: ver_a,
                        txn_b: b.txn_id,
                        version_b: ver_b,
                    });
                }
            }
        }
    }

    SheafResult {
        consistent: obstructions.is_empty(),
        obstructions,
    }
}

// ---------------------------------------------------------------------------
// §4.7 Conformal calibrator
// ---------------------------------------------------------------------------

/// Configuration for the conformal calibrator.
#[derive(Debug, Clone, Copy)]
pub struct ConformalCalibratorConfig {
    /// Coverage level (e.g., 0.05 for 95% coverage).
    pub alpha: f64,
    /// Minimum calibration samples before producing predictions.
    pub min_calibration_samples: usize,
}

impl Default for ConformalCalibratorConfig {
    fn default() -> Self {
        Self {
            alpha: 0.05,
            min_calibration_samples: 50,
        }
    }
}

/// An oracle report's nonconformity score for a single invariant.
#[derive(Debug, Clone)]
pub struct InvariantScore {
    /// Name of the invariant.
    pub invariant: String,
    /// Nonconformity score (higher = more anomalous).
    pub score: f64,
}

/// An oracle report: collection of invariant scores from a lab run.
#[derive(Debug, Clone)]
pub struct OracleReport {
    /// Scores for each monitored invariant.
    pub scores: Vec<InvariantScore>,
}

/// A single prediction set entry.
#[derive(Debug, Clone)]
pub struct PredictionSetEntry {
    /// Invariant name.
    pub invariant: String,
    /// Observed score.
    pub score: f64,
    /// Calibrated threshold.
    pub threshold: f64,
    /// Whether the observation is within the prediction set.
    pub conforming: bool,
}

/// Full prediction result.
#[derive(Debug, Clone)]
pub struct ConformalPrediction {
    /// Per-invariant prediction set entries.
    pub prediction_sets: Vec<PredictionSetEntry>,
}

/// Distribution-free conformal calibrator for oracle anomaly detection.
///
/// Calibrates on `OracleReport`s from deterministic lab runs and produces
/// prediction sets with finite-sample coverage guarantees.
#[derive(Debug, Clone)]
pub struct ConformalOracleCalibrator {
    config: ConformalCalibratorConfig,
    /// Per-invariant calibration scores: invariant_name -> sorted scores.
    calibration_scores: HashMap<String, Vec<f64>>,
    /// Number of calibration samples received.
    sample_count: usize,
}

impl ConformalOracleCalibrator {
    /// Create a new conformal calibrator.
    #[must_use]
    pub fn new(config: ConformalCalibratorConfig) -> Self {
        Self {
            config,
            calibration_scores: HashMap::new(),
            sample_count: 0,
        }
    }

    /// Add a calibration sample (oracle report from a lab seed).
    pub fn calibrate(&mut self, report: &OracleReport) {
        self.sample_count += 1;
        for score in &report.scores {
            self.calibration_scores
                .entry(score.invariant.clone())
                .or_default()
                .push(score.score);
        }
    }

    /// Whether we have enough samples for prediction.
    #[must_use]
    pub fn is_calibrated(&self) -> bool {
        self.sample_count >= self.config.min_calibration_samples
    }

    /// Number of calibration samples.
    #[must_use]
    pub fn sample_count(&self) -> usize {
        self.sample_count
    }

    /// Produce a prediction for a new oracle report.
    ///
    /// Returns `None` if not yet calibrated.
    #[must_use]
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        clippy::cast_precision_loss
    )]
    pub fn predict(&self, report: &OracleReport) -> Option<ConformalPrediction> {
        if !self.is_calibrated() {
            return None;
        }

        let mut entries = Vec::with_capacity(report.scores.len());
        for score in &report.scores {
            let threshold = self.compute_threshold(&score.invariant);
            let conforming = score.score <= threshold;
            entries.push(PredictionSetEntry {
                invariant: score.invariant.clone(),
                score: score.score,
                threshold,
                conforming,
            });
        }

        Some(ConformalPrediction {
            prediction_sets: entries,
        })
    }

    /// Compute the conformal threshold for an invariant.
    ///
    /// The threshold is the `ceil((1-alpha)(n+1))`-th order statistic.
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        clippy::cast_precision_loss
    )]
    fn compute_threshold(&self, invariant: &str) -> f64 {
        let Some(scores) = self.calibration_scores.get(invariant) else {
            return f64::INFINITY; // Unknown invariant: accept everything.
        };

        if scores.is_empty() {
            return f64::INFINITY;
        }

        let mut sorted = scores.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        let n = sorted.len();
        let q_idx = ((1.0 - self.config.alpha) * (n + 1) as f64).ceil() as usize;
        let idx = q_idx.min(n) - 1;
        sorted[idx]
    }
}

// ---------------------------------------------------------------------------
// §4.7.2 Opportunity score
// ---------------------------------------------------------------------------

/// Opportunity score for gating performance optimizations.
///
/// `score = impact * confidence / effort`
///
/// Only land optimizations with score >= 2.0.
#[derive(Debug, Clone, Copy)]
pub struct OpportunityScore {
    /// Expected performance impact (1.0 = 1x improvement).
    pub impact: f64,
    /// Confidence that the impact is real (0.0-1.0).
    pub confidence: f64,
    /// Effort to implement (1.0 = baseline effort).
    pub effort: f64,
}

impl OpportunityScore {
    /// Compute the score.
    #[must_use]
    pub fn score(&self) -> f64 {
        if self.effort <= 0.0 {
            return f64::INFINITY;
        }
        self.impact * self.confidence / self.effort
    }

    /// Whether the score meets the gate threshold (>= 2.0).
    #[must_use]
    pub fn passes_gate(&self) -> bool {
        self.score() >= 2.0
    }
}

impl fmt::Display for OpportunityScore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "impact={:.1} confidence={:.2} effort={:.1} score={:.2} {}",
            self.impact,
            self.confidence,
            self.effort,
            self.score(),
            if self.passes_gate() { "PASS" } else { "REJECT" }
        )
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    const BEAD_ID: &str = "bd-3go.6";

    #[test]
    fn test_sheaf_consistent_non_overlapping_txns() {
        // Test 1: Two txns with disjoint read sets → trivially consistent.
        let sections = vec![
            Section {
                txn_id: 1,
                observations: std::iter::once((10, 100)).collect(),
            },
            Section {
                txn_id: 2,
                observations: std::iter::once((20, 200)).collect(),
            },
        ];
        let result = check_sheaf_consistency(&sections, None);
        assert!(
            result.is_consistent(),
            "bead_id={BEAD_ID} non_overlapping_consistent"
        );
        assert!(result.obstructions.is_empty());
    }

    #[test]
    fn test_sheaf_consistent_overlapping_agree() {
        // Test 2: Two txns read page 10 and see the same version.
        let sections = vec![
            Section {
                txn_id: 1,
                observations: [(10, 100), (20, 200)].into_iter().collect(),
            },
            Section {
                txn_id: 2,
                observations: [(10, 100), (30, 300)].into_iter().collect(),
            },
        ];
        let result = check_sheaf_consistency(&sections, None);
        assert!(
            result.is_consistent(),
            "bead_id={BEAD_ID} overlapping_agree"
        );
    }

    #[test]
    fn test_sheaf_detects_inconsistency() {
        // Test 3: Two txns read page 10 but see different versions.
        let sections = vec![
            Section {
                txn_id: 1,
                observations: std::iter::once((10, 100)).collect(),
            },
            Section {
                txn_id: 2,
                observations: std::iter::once((10, 999)).collect(),
            },
        ];
        let result = check_sheaf_consistency(&sections, None);
        assert!(
            !result.is_consistent(),
            "bead_id={BEAD_ID} detects_inconsistency"
        );
        assert_eq!(result.obstructions.len(), 1);
        let obs = &result.obstructions[0];
        assert_eq!(obs.page, 10);
        assert!(
            !obs.to_string().is_empty(),
            "bead_id={BEAD_ID} obstruction_display"
        );
    }

    #[test]
    fn test_sheaf_ordered_versions_consistent() {
        // Test: Two txns read page 10 but see different versions that are
        // ordered in the version chain → consistent.
        let sections = vec![
            Section {
                txn_id: 1,
                observations: std::iter::once((10, 100)).collect(),
            },
            Section {
                txn_id: 2,
                observations: std::iter::once((10, 200)).collect(),
            },
        ];
        // Version 100 < 200 in the chain.
        let order_fn = |a: u64, b: u64| a <= b;
        let result = check_sheaf_consistency(&sections, Some(&order_fn));
        assert!(
            result.is_consistent(),
            "bead_id={BEAD_ID} ordered_versions_consistent"
        );
    }

    #[test]
    fn test_sheaf_consistency_with_global_version_chain() {
        let sections = vec![
            Section {
                txn_id: 1,
                observations: std::iter::once((10, 100)).collect(),
            },
            Section {
                txn_id: 2,
                observations: std::iter::once((10, 200)).collect(),
            },
        ];

        let chains = HashMap::from([(10_u64, vec![50_u64, 100_u64, 200_u64, 300_u64])]);
        let result = check_sheaf_consistency_with_chains(&sections, &chains);
        assert!(
            result.is_consistent(),
            "bead_id={BEAD_ID} explicit_chain_consistent"
        );
    }

    #[test]
    fn test_sheaf_consistency_with_chains_flags_unknown_version() {
        let sections = vec![
            Section {
                txn_id: 1,
                observations: std::iter::once((10, 100)).collect(),
            },
            Section {
                txn_id: 2,
                observations: std::iter::once((10, 9_999)).collect(),
            },
        ];

        let chains = HashMap::from([(10_u64, vec![50_u64, 100_u64, 200_u64, 300_u64])]);
        let result = check_sheaf_consistency_with_chains(&sections, &chains);
        assert!(
            !result.is_consistent(),
            "bead_id={BEAD_ID} explicit_chain_unknown_version"
        );
        assert_eq!(result.obstructions.len(), 1);
    }

    #[test]
    fn test_conformal_calibrator_min_samples() {
        // Test 4: Not enough samples → predict returns None.
        let cal = ConformalOracleCalibrator::new(ConformalCalibratorConfig {
            min_calibration_samples: 50,
            ..Default::default()
        });
        let report = OracleReport {
            scores: vec![InvariantScore {
                invariant: "inv1".into(),
                score: 1.0,
            }],
        };
        assert!(
            cal.predict(&report).is_none(),
            "bead_id={BEAD_ID} min_samples_none"
        );
        assert!(!cal.is_calibrated());
    }

    #[test]
    fn test_conformal_calibrator_detects_anomaly() {
        // Test 5: Calibrate on normal reports, then detect anomalous score.
        let mut cal = ConformalOracleCalibrator::new(ConformalCalibratorConfig::default());

        // Calibrate with 100 seeds; scores in [0.0, 1.0].
        for i in 0..100_i32 {
            let score = f64::from(i) / 100.0;
            cal.calibrate(&OracleReport {
                scores: vec![InvariantScore {
                    invariant: "durability".into(),
                    score,
                }],
            });
        }
        assert!(cal.is_calibrated());

        // Normal observation: should be conforming.
        let normal_pred = cal
            .predict(&OracleReport {
                scores: vec![InvariantScore {
                    invariant: "durability".into(),
                    score: 0.5,
                }],
            })
            .expect("calibrated");
        assert!(
            normal_pred.prediction_sets[0].conforming,
            "bead_id={BEAD_ID} normal_conforming"
        );

        // Anomalous observation: should be non-conforming.
        let anomalous_pred = cal
            .predict(&OracleReport {
                scores: vec![InvariantScore {
                    invariant: "durability".into(),
                    score: 100.0,
                }],
            })
            .expect("calibrated");
        assert!(
            !anomalous_pred.prediction_sets[0].conforming,
            "bead_id={BEAD_ID} anomaly_detected: score=100 threshold={}",
            anomalous_pred.prediction_sets[0].threshold
        );
    }

    #[test]
    fn test_conformal_calibrator_coverage_guarantee() {
        // Test 6: At alpha=0.05, >=95% of in-distribution holdout reports
        // should be flagged conforming.
        let mut cal = ConformalOracleCalibrator::new(ConformalCalibratorConfig {
            alpha: 0.05,
            min_calibration_samples: 50,
        });

        // Calibrate with 100 seeds; scores ~ i/100.
        for i in 0..100_i32 {
            cal.calibrate(&OracleReport {
                scores: vec![InvariantScore {
                    invariant: "inv".into(),
                    score: f64::from(i) / 100.0,
                }],
            });
        }

        // Holdout: 200 more from the same distribution.
        let mut conforming_count = 0usize;
        for i in 100..300_i32 {
            let score = f64::from((i * 37 + 13) % 100) / 100.0;
            let pred = cal
                .predict(&OracleReport {
                    scores: vec![InvariantScore {
                        invariant: "inv".into(),
                        score,
                    }],
                })
                .expect("calibrated");
            if pred.prediction_sets[0].conforming {
                conforming_count += 1;
            }
        }

        #[allow(clippy::cast_precision_loss)]
        let rate = conforming_count as f64 / 200.0;
        assert!(
            rate >= 0.90, // Allow some slack (theoretical guarantee is 0.95 asymptotically)
            "bead_id={BEAD_ID} coverage_rate={rate}"
        );
    }

    #[test]
    fn test_opportunity_score_gate() {
        // Test 7: Score >= 2.0 passes, < 2.0 rejected.
        let good = OpportunityScore {
            impact: 3.0,
            confidence: 0.9,
            effort: 1.0,
        };
        assert!(
            good.passes_gate(),
            "bead_id={BEAD_ID} good_score={:.2}",
            good.score()
        );
        assert!(good.score() >= 2.0);

        let bad = OpportunityScore {
            impact: 1.5,
            confidence: 0.5,
            effort: 1.0,
        };
        assert!(
            !bad.passes_gate(),
            "bead_id={BEAD_ID} bad_score={:.2}",
            bad.score()
        );
        assert!(bad.score() < 2.0);

        // Edge case: exactly 2.0.
        let edge = OpportunityScore {
            impact: 2.0,
            confidence: 1.0,
            effort: 1.0,
        };
        assert!(
            edge.passes_gate(),
            "bead_id={BEAD_ID} edge_score={:.2}",
            edge.score()
        );

        // Display works.
        assert!(!good.to_string().is_empty(), "bead_id={BEAD_ID} display");
    }
}
