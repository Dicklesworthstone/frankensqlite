//! Decision-theoretic SSI abort policy: victim selection + loss minimization (§5.7.3).
//!
//! Provides the Bayesian decision framework for WHEN and WHOM to abort when a
//! dangerous structure is detected, plus continuous monitoring via e-process and
//! conformal calibration.

use std::fmt;

// ---------------------------------------------------------------------------
// Loss matrix (§5.7.3 Bayesian Decision Framework)
// ---------------------------------------------------------------------------

/// Loss parameters for the SSI abort decision.
///
/// `L_miss` = cost of letting an anomaly through (data corruption risk).
/// `L_fp`   = cost of a false-positive abort (wasted work, retry).
#[derive(Debug, Clone, Copy, PartialEq)]
#[allow(clippy::derive_partial_eq_without_eq)] // f64 does not impl Eq
pub struct LossMatrix {
    /// Cost of a missed anomaly (default: 1000).
    pub l_miss: f64,
    /// Cost of a false-positive abort (default: 1).
    pub l_fp: f64,
}

impl Default for LossMatrix {
    fn default() -> Self {
        Self {
            l_miss: 1000.0,
            l_fp: 1.0,
        }
    }
}

impl LossMatrix {
    /// Compute the abort threshold: P(anomaly) > threshold ⟹ abort.
    ///
    /// `threshold = L_fp / (L_fp + L_miss)`
    #[must_use]
    pub fn abort_threshold(&self) -> f64 {
        self.l_fp / (self.l_fp + self.l_miss)
    }

    /// Expected loss of committing given P(anomaly).
    #[must_use]
    pub fn expected_loss_commit(&self, p_anomaly: f64) -> f64 {
        p_anomaly * self.l_miss
    }

    /// Expected loss of aborting given P(anomaly).
    #[must_use]
    pub fn expected_loss_abort(&self, p_anomaly: f64) -> f64 {
        (1.0 - p_anomaly) * self.l_fp
    }

    /// Should we abort? Returns true if `E[Loss|commit] > E[Loss|abort]`.
    #[must_use]
    pub fn should_abort(&self, p_anomaly: f64) -> bool {
        p_anomaly > self.abort_threshold()
    }
}

// ---------------------------------------------------------------------------
// Transaction cost estimation
// ---------------------------------------------------------------------------

/// Approximation of `L(T)` = cost of aborting a transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TxnCost {
    /// Number of pages in the write set.
    pub write_set_size: u32,
    /// Duration in microseconds.
    pub duration_us: u64,
}

impl TxnCost {
    /// Combined cost metric: write_set_size + duration_us/1000.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn loss(&self) -> f64 {
        f64::from(self.write_set_size) + (self.duration_us as f64) / 1000.0
    }
}

// ---------------------------------------------------------------------------
// Victim selection (§5.7.3 Policy)
// ---------------------------------------------------------------------------

/// Cycle status for a dangerous structure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CycleStatus {
    /// T1 and T3 both committed — confirmed anomaly.
    Confirmed,
    /// Only one end committed — potential anomaly.
    Potential,
}

/// Which transaction to abort.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Victim {
    /// Abort T2 (the pivot).
    Pivot,
    /// Abort T3 (the other active participant).
    Other,
}

/// Result of a victim selection decision.
#[derive(Debug, Clone)]
pub struct VictimDecision {
    pub victim: Victim,
    pub cycle_status: CycleStatus,
    pub pivot_cost: f64,
    pub other_cost: f64,
    pub reason: &'static str,
}

impl fmt::Display for VictimDecision {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "victim={:?} cycle={:?} pivot_cost={:.1} other_cost={:.1} reason={}",
            self.victim, self.cycle_status, self.pivot_cost, self.other_cost, self.reason
        )
    }
}

/// Select which transaction to abort in a dangerous structure.
///
/// # Policy
///
/// 1. **Confirmed cycle (T1, T3 both committed):** MUST abort T2 (pivot).
///    Safety is mandatory.
/// 2. **Potential cycle:** Compare costs. If `L(T2) << L(T3)`, prefer
///    aborting T2 to protect the heavier transaction. Default: abort pivot.
#[must_use]
pub fn select_victim(
    status: CycleStatus,
    pivot_cost: TxnCost,
    other_cost: TxnCost,
) -> VictimDecision {
    let pivot_l = pivot_cost.loss();
    let other_l = other_cost.loss();

    match status {
        CycleStatus::Confirmed => {
            // Safety first: MUST abort pivot. No choice.
            VictimDecision {
                victim: Victim::Pivot,
                cycle_status: status,
                pivot_cost: pivot_l,
                other_cost: other_l,
                reason: "confirmed_cycle_must_abort_pivot",
            }
        }
        CycleStatus::Potential => {
            // Optimistic: compare costs. Default to pivot unless other is much cheaper.
            // "Alien Rule": if pivot is significantly cheaper, abort it to protect heavy txn.
            if pivot_l <= other_l {
                VictimDecision {
                    victim: Victim::Pivot,
                    cycle_status: status,
                    pivot_cost: pivot_l,
                    other_cost: other_l,
                    reason: "potential_cycle_abort_cheaper_pivot",
                }
            } else {
                // Pivot is heavier. Default still aborts pivot (conservative),
                // but logs the cost difference for auditing.
                VictimDecision {
                    victim: Victim::Pivot,
                    cycle_status: status,
                    pivot_cost: pivot_l,
                    other_cost: other_l,
                    reason: "potential_cycle_default_abort_pivot",
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// SSI abort decision envelope (auditable logging)
// ---------------------------------------------------------------------------

/// Full audit record for an SSI abort/commit decision.
#[derive(Debug, Clone)]
pub struct AbortDecisionEnvelope {
    pub has_in_rw: bool,
    pub has_out_rw: bool,
    pub p_anomaly: f64,
    pub loss_matrix: LossMatrix,
    pub threshold: f64,
    pub expected_loss_commit: f64,
    pub expected_loss_abort: f64,
    pub decision: AbortDecision,
    pub victim: Option<VictimDecision>,
}

/// The binary decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AbortDecision {
    Commit,
    Abort,
}

impl AbortDecisionEnvelope {
    /// Build an envelope from evidence.
    #[must_use]
    pub fn evaluate(
        has_in_rw: bool,
        has_out_rw: bool,
        p_anomaly: f64,
        loss_matrix: LossMatrix,
        victim: Option<VictimDecision>,
    ) -> Self {
        let threshold = loss_matrix.abort_threshold();
        let el_commit = loss_matrix.expected_loss_commit(p_anomaly);
        let el_abort = loss_matrix.expected_loss_abort(p_anomaly);
        let decision = if has_in_rw && has_out_rw && loss_matrix.should_abort(p_anomaly) {
            AbortDecision::Abort
        } else {
            AbortDecision::Commit
        };
        Self {
            has_in_rw,
            has_out_rw,
            p_anomaly,
            loss_matrix,
            threshold,
            expected_loss_commit: el_commit,
            expected_loss_abort: el_abort,
            decision,
            victim,
        }
    }
}

// ---------------------------------------------------------------------------
// E-Process monitor for INV-SSI-FP (§5.7.3)
// ---------------------------------------------------------------------------

/// Configuration for the SSI false-positive e-process monitor.
#[derive(Debug, Clone, Copy)]
pub struct SsiFpMonitorConfig {
    /// Null hypothesis false-positive rate (e.g., 0.05 = 5%).
    pub p0: f64,
    /// Bet parameter (lambda) for the e-process.
    pub lambda: f64,
    /// Significance level alpha (reject when e-value > 1/alpha).
    pub alpha: f64,
    /// Maximum e-value (cap to prevent overflow).
    pub max_evalue: f64,
}

impl Default for SsiFpMonitorConfig {
    fn default() -> Self {
        Self {
            p0: 0.05,
            lambda: 0.3,
            alpha: 0.01,
            max_evalue: 1e12,
        }
    }
}

/// E-process monitor for tracking SSI false-positive rate.
///
/// Each observation is a binary: `true` = false positive, `false` = true positive.
/// The e-process multiplicatively updates with bet `lambda`:
///
/// `e_t = e_{t-1} * (1 + lambda * (X_t - p0))`
///
/// When `e_value > 1/alpha`, the null hypothesis (FP rate <= p0) is rejected.
#[derive(Debug, Clone)]
pub struct SsiFpMonitor {
    config: SsiFpMonitorConfig,
    e_value: f64,
    observations: u64,
    false_positives: u64,
    alert_triggered: bool,
}

impl SsiFpMonitor {
    #[must_use]
    pub fn new(config: SsiFpMonitorConfig) -> Self {
        Self {
            config,
            e_value: 1.0,
            observations: 0,
            false_positives: 0,
            alert_triggered: false,
        }
    }

    /// Observe one SSI abort outcome.
    ///
    /// `is_false_positive`: true if retrospective row-level replay shows the
    /// abort was unnecessary.
    pub fn observe(&mut self, is_false_positive: bool) {
        self.observations += 1;
        let x = if is_false_positive {
            self.false_positives += 1;
            1.0
        } else {
            0.0
        };

        // Multiplicative update: e_t = e_{t-1} * (1 + lambda * (X_t - p0))
        let factor = self.config.lambda.mul_add(x - self.config.p0, 1.0);
        self.e_value = (self.e_value * factor).min(self.config.max_evalue);

        // Clamp below 0 (can happen if p0 > 0 and we observe true positive).
        if self.e_value < 0.0 {
            self.e_value = 0.0;
        }

        // Check threshold.
        if self.e_value > 1.0 / self.config.alpha {
            self.alert_triggered = true;
        }
    }

    #[must_use]
    pub fn e_value(&self) -> f64 {
        self.e_value
    }

    #[must_use]
    pub fn observations(&self) -> u64 {
        self.observations
    }

    #[must_use]
    pub fn false_positives(&self) -> u64 {
        self.false_positives
    }

    #[must_use]
    pub fn alert_triggered(&self) -> bool {
        self.alert_triggered
    }

    /// The rejection threshold: 1/alpha.
    #[must_use]
    pub fn rejection_threshold(&self) -> f64 {
        1.0 / self.config.alpha
    }

    /// Observed false-positive rate.
    #[must_use]
    pub fn observed_fp_rate(&self) -> f64 {
        if self.observations == 0 {
            return 0.0;
        }
        #[allow(clippy::cast_precision_loss)]
        {
            self.false_positives as f64 / self.observations as f64
        }
    }
}

// ---------------------------------------------------------------------------
// Conformal Calibrator for page-level coarseness (§5.7.3)
// ---------------------------------------------------------------------------

/// Configuration for conformal calibration.
#[derive(Debug, Clone, Copy)]
pub struct ConformalConfig {
    /// Coverage level (e.g., 0.05 for 95% coverage).
    pub alpha: f64,
    /// Minimum number of calibration samples before producing bounds.
    pub min_calibration_samples: usize,
}

impl Default for ConformalConfig {
    fn default() -> Self {
        Self {
            alpha: 0.05,
            min_calibration_samples: 30,
        }
    }
}

/// Conformal calibrator: produces distribution-free prediction intervals
/// for the page-level vs row-level abort rate difference.
#[derive(Debug, Clone)]
pub struct ConformalCalibrator {
    config: ConformalConfig,
    /// Calibration residuals (abort rate deltas).
    residuals: Vec<f64>,
}

impl ConformalCalibrator {
    #[must_use]
    pub fn new(config: ConformalConfig) -> Self {
        Self {
            config,
            residuals: Vec::new(),
        }
    }

    /// Add a calibration sample: the difference between page-level and
    /// row-level abort rates for a workload window.
    pub fn add_sample(&mut self, abort_rate_delta: f64) {
        self.residuals.push(abort_rate_delta);
    }

    /// Whether we have enough samples to produce a bound.
    #[must_use]
    pub fn is_calibrated(&self) -> bool {
        self.residuals.len() >= self.config.min_calibration_samples
    }

    /// The upper bound of the prediction interval.
    ///
    /// At coverage `1-alpha`, the conformal quantile is the `ceil((1-alpha)*(n+1))`-th
    /// order statistic. Returns `None` if not yet calibrated.
    #[must_use]
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    pub fn upper_bound(&self) -> Option<f64> {
        if !self.is_calibrated() {
            return None;
        }
        let mut sorted = self.residuals.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        #[allow(clippy::cast_precision_loss)]
        let q_idx = ((1.0 - self.config.alpha) * (sorted.len() + 1) as f64).ceil() as usize;
        let idx = q_idx.min(sorted.len()) - 1;
        Some(sorted[idx])
    }

    /// Check whether a new observation is within the calibrated band.
    #[must_use]
    pub fn is_conforming(&self, abort_rate_delta: f64) -> Option<bool> {
        self.upper_bound().map(|ub| abort_rate_delta <= ub)
    }

    /// Number of calibration samples.
    #[must_use]
    pub fn sample_count(&self) -> usize {
        self.residuals.len()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    const BEAD_ID: &str = "bd-3t3.12";

    #[test]
    fn test_loss_matrix_abort_threshold() {
        // Test 1: Verify abort threshold = L_fp / (L_fp + L_miss).
        let default = LossMatrix::default();
        let threshold = default.abort_threshold();
        #[allow(clippy::approx_constant)]
        let expected = 1.0 / 1001.0;
        assert!(
            (threshold - expected).abs() < 1e-10,
            "bead_id={BEAD_ID} default_threshold={threshold} expected={expected}"
        );

        // Different ratios.
        let m2 = LossMatrix {
            l_miss: 100.0,
            l_fp: 10.0,
        };
        let t2 = m2.abort_threshold();
        assert!(
            (t2 - 10.0 / 110.0).abs() < 1e-10,
            "bead_id={BEAD_ID} ratio_100_10"
        );

        // Equal costs: threshold = 0.5.
        let m3 = LossMatrix {
            l_miss: 1.0,
            l_fp: 1.0,
        };
        assert!(
            (m3.abort_threshold() - 0.5).abs() < 1e-10,
            "bead_id={BEAD_ID} equal_costs"
        );
    }

    #[test]
    fn test_victim_selection_confirmed_cycle() {
        // Test 2: T1 committed, T3 committed → MUST abort T2.
        let pivot = TxnCost {
            write_set_size: 100,
            duration_us: 50_000,
        };
        let other = TxnCost {
            write_set_size: 1,
            duration_us: 100,
        };
        let decision = select_victim(CycleStatus::Confirmed, pivot, other);
        assert_eq!(
            decision.victim,
            Victim::Pivot,
            "bead_id={BEAD_ID} confirmed_aborts_pivot"
        );
        assert_eq!(decision.cycle_status, CycleStatus::Confirmed);
        assert!(decision.reason.contains("confirmed"));
    }

    #[test]
    fn test_victim_selection_potential_cycle_heavy_t3() {
        // Test 3: L(T2)=1, L(T3)=1000. Policy prefers aborting T2 (cheaper).
        let pivot = TxnCost {
            write_set_size: 1,
            duration_us: 0,
        };
        let other = TxnCost {
            write_set_size: 1000,
            duration_us: 0,
        };
        let decision = select_victim(CycleStatus::Potential, pivot, other);
        assert_eq!(
            decision.victim,
            Victim::Pivot,
            "bead_id={BEAD_ID} cheaper_pivot_aborted"
        );
        assert!(
            decision.pivot_cost < decision.other_cost,
            "bead_id={BEAD_ID} pivot_cost_lower"
        );
    }

    #[test]
    fn test_victim_selection_potential_cycle_equal_cost() {
        // Test 4: L(T2) ~ L(T3). Default: abort pivot T2.
        let cost = TxnCost {
            write_set_size: 50,
            duration_us: 10_000,
        };
        let decision = select_victim(CycleStatus::Potential, cost, cost);
        assert_eq!(
            decision.victim,
            Victim::Pivot,
            "bead_id={BEAD_ID} equal_cost_default_pivot"
        );
    }

    #[test]
    fn test_overapproximation_safety() {
        // Test 5: has_in_rw=true, has_out_rw=true, but T1 not yet committed
        // → still aborts (deliberate overapproximation). No false negative.
        let lm = LossMatrix::default();
        // Even tiny P(anomaly) exceeds threshold (1/1001 ~ 0.001).
        let p_anomaly = 0.01; // 1% chance — well above threshold.
        let envelope = AbortDecisionEnvelope::evaluate(true, true, p_anomaly, lm, None);
        assert_eq!(
            envelope.decision,
            AbortDecision::Abort,
            "bead_id={BEAD_ID} overapprox_aborts"
        );
    }

    #[test]
    fn test_eprocess_ssi_fp_monitor_under_threshold() {
        // Test 6: Feed 100 observations with FP rate=3%. E-process stays
        // below 1/alpha=100.
        let mut monitor = SsiFpMonitor::new(SsiFpMonitorConfig::default());
        for i in 0..100 {
            let is_fp = (i % 33) == 0; // ~3% FP rate.
            monitor.observe(is_fp);
        }
        assert!(
            monitor.e_value() < monitor.rejection_threshold(),
            "bead_id={BEAD_ID} under_threshold: e={} threshold={}",
            monitor.e_value(),
            monitor.rejection_threshold()
        );
        assert!(!monitor.alert_triggered(), "bead_id={BEAD_ID} no_alert");
    }

    #[test]
    fn test_eprocess_ssi_fp_monitor_exceeds_threshold() {
        // Test 7: Feed observations with FP rate=15%. E-process exceeds
        // 1/alpha=100.
        let mut monitor = SsiFpMonitor::new(SsiFpMonitorConfig {
            p0: 0.05,
            lambda: 0.3,
            alpha: 0.01,
            max_evalue: 1e12,
        });
        // 15% FP rate: 1 in ~7.
        for i in 0..200 {
            let is_fp = (i % 7) < 1; // ~14.3% FP rate.
            monitor.observe(is_fp);
        }
        assert!(
            monitor.alert_triggered(),
            "bead_id={BEAD_ID} alert_triggered: e={} threshold={}",
            monitor.e_value(),
            monitor.rejection_threshold()
        );
    }

    #[test]
    fn test_conformal_calibrator_within_band() {
        // Test 8: Page-level abort rate delta within calibrated band → conforming.
        let mut cal = ConformalCalibrator::new(ConformalConfig::default());
        // Calibration: deltas all between 0.01 and 0.05.
        for i in 0..30 {
            #[allow(clippy::cast_precision_loss)]
            let delta = 0.04f64.mul_add(f64::from(i) / 29.0, 0.01);
            cal.add_sample(delta);
        }
        assert!(cal.is_calibrated());
        let ub = cal.upper_bound().expect("calibrated");
        // Upper bound should be around 0.05.
        assert!(ub >= 0.04, "bead_id={BEAD_ID} upper_bound={ub}");

        // New observation within band.
        assert_eq!(
            cal.is_conforming(0.03),
            Some(true),
            "bead_id={BEAD_ID} within_band"
        );
    }

    #[test]
    fn test_conformal_calibrator_outside_band() {
        // Test 9: Page-level abort rate delta exceeds band → non-conforming.
        let mut cal = ConformalCalibrator::new(ConformalConfig::default());
        // Calibration: deltas between 0.01 and 0.03.
        for i in 0..30 {
            #[allow(clippy::cast_precision_loss)]
            let delta = 0.02f64.mul_add(f64::from(i) / 29.0, 0.01);
            cal.add_sample(delta);
        }
        assert!(cal.is_calibrated());

        // Observation way outside band.
        assert_eq!(
            cal.is_conforming(0.50),
            Some(false),
            "bead_id={BEAD_ID} outside_band"
        );
    }

    #[test]
    fn test_abort_decision_auditable_logging() {
        // Test 10: Verify abort decision logs all required fields.
        let lm = LossMatrix::default();
        let victim = select_victim(
            CycleStatus::Potential,
            TxnCost {
                write_set_size: 5,
                duration_us: 1000,
            },
            TxnCost {
                write_set_size: 50,
                duration_us: 10_000,
            },
        );
        let envelope = AbortDecisionEnvelope::evaluate(true, true, 0.5, lm, Some(victim));

        // All required fields present.
        assert!(envelope.has_in_rw);
        assert!(envelope.has_out_rw);
        assert!((envelope.p_anomaly - 0.5).abs() < 1e-10);
        assert!((envelope.threshold - lm.abort_threshold()).abs() < 1e-10);
        assert!(
            (envelope.expected_loss_commit - 500.0).abs() < 1e-10,
            "bead_id={BEAD_ID} el_commit={}",
            envelope.expected_loss_commit
        );
        assert!(
            (envelope.expected_loss_abort - 0.5).abs() < 1e-10,
            "bead_id={BEAD_ID} el_abort={}",
            envelope.expected_loss_abort
        );
        assert_eq!(envelope.decision, AbortDecision::Abort);
        let v = envelope.victim.expect("victim present");
        assert_eq!(v.victim, Victim::Pivot);
        assert!(
            !v.to_string().is_empty(),
            "bead_id={BEAD_ID} victim_display"
        );
    }
}
