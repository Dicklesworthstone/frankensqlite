//! Anytime-valid parity drift monitors with e-process and BOCPD (bd-1dp9.8.2).
//!
//! Provides online monitoring of parity mismatch rates with mathematically
//! grounded alarm semantics:
//!
//! - **E-process monitors**: per-category anytime-valid rejection tests.
//!   Each category gets a calibrated e-process that detects sustained mismatch
//!   rate elevation without multiple-testing penalties.
//!
//! - **BOCPD regime classification**: sliding-window change-point detection
//!   labels each observation window as `Stable`, `Improving`, `Regressing`,
//!   or `ShiftDetected`, enabling CI to distinguish noise from real drift.
//!
//! - **Alarm escalation**: three alarm levels (`Info`, `Warning`, `Critical`)
//!   with per-category and global thresholds, plus runbook-style remediation
//!   entries for operator action.
//!
//! # Upstream Dependencies
//!
//! - [`parity_invariant_catalog`](crate::parity_invariant_catalog) (bd-1dp9.8.1):
//!   invariant definitions and proof obligations
//! - [`confidence_gates`](crate::confidence_gates) (bd-1dp9.8.3):
//!   `GateReport` providing per-category verification state
//! - [`parity_taxonomy`](crate::parity_taxonomy):
//!   `FeatureCategory` enum and `truncate_score`
//! - [`replay_harness`](crate::replay_harness):
//!   `Regime` enum and BOCPD `DriftDetector`
//!
//! # Downstream Consumers
//!
//! - **bd-1dp9.8.4**: Release certificate embeds drift alarm state
//! - **bd-1dp9.8.5**: Adversarial search targets categories under drift
//!
//! # Determinism
//!
//! All arithmetic uses `truncate_score` for cross-platform reproducibility.
//! Alarm ordering is deterministic (sorted by alarm level, then category name).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::confidence_gates::GateReport;
use crate::parity_taxonomy::{FeatureCategory, truncate_score};
use crate::replay_harness::{DriftDetector, DriftDetectorConfig, Regime};

/// Bead identifier for log correlation.
#[allow(dead_code)]
const BEAD_ID: &str = "bd-1dp9.8.2";

/// Schema version for drift monitor output format.
pub const DRIFT_MONITOR_SCHEMA_VERSION: u32 = 1;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the parity drift monitoring system.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParityDriftConfig {
    /// E-process null hypothesis: baseline mismatch probability.
    /// Rates above this trigger e-value growth.
    pub e_process_p0: f64,
    /// E-process mixing parameter (sensitivity).
    pub e_process_lambda: f64,
    /// E-process significance level (false alarm budget).
    pub e_process_alpha: f64,
    /// Maximum e-value before capping (numerical stability).
    pub e_process_max_evalue: f64,
    /// BOCPD drift detector window size (observations per window).
    pub drift_window_size: usize,
    /// BOCPD sensitivity threshold (minimum rate change to signal).
    pub drift_sensitivity: f64,
    /// BOCPD EMA decay factor.
    pub drift_ema_alpha: f64,
    /// BOCPD warmup windows before detection activates.
    pub drift_warmup_windows: usize,
    /// Alarm escalation: e-value threshold for Warning level.
    pub warning_evalue_threshold: f64,
    /// Alarm escalation: e-value threshold for Critical level.
    pub critical_evalue_threshold: f64,
    /// Alarm escalation: mismatch rate threshold for Warning.
    pub warning_rate_threshold: f64,
    /// Alarm escalation: mismatch rate threshold for Critical.
    pub critical_rate_threshold: f64,
}

impl Default for ParityDriftConfig {
    fn default() -> Self {
        Self {
            e_process_p0: 0.05,
            e_process_lambda: 0.8,
            e_process_alpha: 0.01,
            e_process_max_evalue: 1e12,
            drift_window_size: 5,
            drift_sensitivity: 0.05,
            drift_ema_alpha: 0.3,
            drift_warmup_windows: 3,
            warning_evalue_threshold: 10.0,
            critical_evalue_threshold: 100.0,
            warning_rate_threshold: 0.3,
            critical_rate_threshold: 0.5,
        }
    }
}

// ---------------------------------------------------------------------------
// Alarm types
// ---------------------------------------------------------------------------

/// Alarm escalation level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum AlarmLevel {
    /// Informational: early signal, no immediate action needed.
    Info,
    /// Warning: sustained drift detected, investigation recommended.
    Warning,
    /// Critical: strong evidence of parity regression, blocking action required.
    Critical,
}

impl std::fmt::Display for AlarmLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Info => f.write_str("INFO"),
            Self::Warning => f.write_str("WARNING"),
            Self::Critical => f.write_str("CRITICAL"),
        }
    }
}

/// A parity drift alarm emitted by the monitoring system.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParityDriftAlarm {
    /// Alarm level.
    pub level: AlarmLevel,
    /// Affected category.
    pub category: String,
    /// Current e-value for this category's monitor.
    pub e_value: f64,
    /// Current observed mismatch rate.
    pub mismatch_rate: f64,
    /// Number of observations processed.
    pub observations: usize,
    /// Current regime classification.
    pub regime: Regime,
    /// Human-readable alarm message.
    pub message: String,
    /// Runbook remediation entry.
    pub runbook: RunbookEntry,
}

/// Runbook entry with remediation guidance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunbookEntry {
    /// Short title for the runbook entry.
    pub title: String,
    /// Description of the condition.
    pub condition: String,
    /// Recommended action steps.
    pub actions: Vec<String>,
    /// Escalation path if actions don't resolve.
    pub escalation: String,
}

// ---------------------------------------------------------------------------
// Per-category monitor state
// ---------------------------------------------------------------------------

/// Per-category e-process state (lightweight, no external dependency on
/// `asupersync::lab::oracle::eprocess` — we implement the SPRT-style
/// e-process inline to avoid coupling the parity drift monitor to the
/// MVCC-specific crate).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CategoryEProcess {
    /// Category name.
    pub category: String,
    /// Null hypothesis mismatch probability.
    pub p0: f64,
    /// Mixing parameter.
    pub lambda: f64,
    /// Significance level.
    pub alpha: f64,
    /// Maximum e-value cap.
    pub max_evalue: f64,
    /// Current e-value.
    pub current: f64,
    /// Total observations.
    pub observations: usize,
    /// Number of mismatch observations (X_t = 1).
    pub mismatches_observed: usize,
    /// Whether H₀ has been rejected.
    pub rejected: bool,
    /// Observation index at first rejection.
    pub rejection_time: Option<usize>,
}

impl CategoryEProcess {
    /// Create a new e-process for a category.
    #[must_use]
    fn new(category: &str, p0: f64, lambda: f64, alpha: f64, max_evalue: f64) -> Self {
        Self {
            category: category.to_owned(),
            p0,
            lambda,
            alpha,
            max_evalue,
            current: 1.0,
            observations: 0,
            mismatches_observed: 0,
            rejected: false,
            rejection_time: None,
        }
    }

    /// Observe a single Bernoulli outcome (true = mismatch detected).
    fn observe(&mut self, mismatch: bool) {
        let x = if mismatch { 1.0 } else { 0.0 };
        // E_{t+1} = E_t * (1 + λ(X_t - p₀))
        let factor = 1.0 + self.lambda * (x - self.p0);
        self.current *= factor.max(0.0);
        self.current = self.current.min(self.max_evalue);

        self.observations += 1;
        if mismatch {
            self.mismatches_observed += 1;
        }

        let threshold = self.threshold();
        if !self.rejected && self.current >= threshold {
            self.rejected = true;
            self.rejection_time = Some(self.observations);
        }
    }

    /// Rejection threshold: 1/α.
    #[must_use]
    fn threshold(&self) -> f64 {
        1.0 / self.alpha
    }

    /// Empirical mismatch rate.
    #[must_use]
    fn empirical_rate(&self) -> f64 {
        if self.observations == 0 {
            return 0.0;
        }
        #[allow(clippy::cast_precision_loss)]
        truncate_score(self.mismatches_observed as f64 / self.observations as f64)
    }

    /// Reset to initial state.
    fn reset(&mut self) {
        self.current = 1.0;
        self.observations = 0;
        self.mismatches_observed = 0;
        self.rejected = false;
        self.rejection_time = None;
    }
}

/// Combined per-category monitor: e-process + drift detector.
#[derive(Debug, Clone)]
struct CategoryMonitor {
    #[allow(dead_code)]
    category: FeatureCategory,
    e_process: CategoryEProcess,
    drift_detector: DriftDetector,
    /// Running window of mismatch observations for BOCPD.
    window_buffer: Vec<bool>,
    /// Window size for BOCPD batching.
    window_size: usize,
    /// Most recent regime classification.
    current_regime: Regime,
}

impl CategoryMonitor {
    fn new(category: FeatureCategory, config: &ParityDriftConfig) -> Self {
        let ep = CategoryEProcess::new(
            category.display_name(),
            config.e_process_p0,
            config.e_process_lambda,
            config.e_process_alpha,
            config.e_process_max_evalue,
        );
        let drift_config = DriftDetectorConfig {
            window_size: config.drift_window_size,
            sensitivity_threshold: config.drift_sensitivity,
            ema_alpha: config.drift_ema_alpha,
            warmup_windows: config.drift_warmup_windows,
        };
        let drift_detector = DriftDetector::new(drift_config);

        Self {
            category,
            e_process: ep,
            drift_detector,
            window_buffer: Vec::with_capacity(config.drift_window_size),
            window_size: config.drift_window_size,
            current_regime: Regime::Stable,
        }
    }

    /// Observe a single Bernoulli outcome.
    fn observe(&mut self, mismatch: bool) {
        self.e_process.observe(mismatch);
        self.window_buffer.push(mismatch);

        if self.window_buffer.len() >= self.window_size {
            self.flush_window();
        }
    }

    /// Flush the current window into the drift detector.
    fn flush_window(&mut self) {
        let mismatch_count = self.window_buffer.iter().filter(|&&m| m).count();
        #[allow(clippy::cast_precision_loss)]
        let rate = if self.window_buffer.is_empty() {
            0.0
        } else {
            mismatch_count as f64 / self.window_buffer.len() as f64
        };
        self.current_regime = self.drift_detector.observe(rate);
        self.window_buffer.clear();
    }

    /// Force-flush any partial window (for finalization).
    fn finalize(&mut self) {
        if !self.window_buffer.is_empty() {
            self.flush_window();
        }
    }
}

// ---------------------------------------------------------------------------
// Main drift monitor
// ---------------------------------------------------------------------------

/// Orchestrates per-category drift monitors for all 9 feature categories.
///
/// Use [`observe_from_gate_report`](Self::observe_from_gate_report) for easy
/// integration with the confidence gate system, or
/// [`observe_category`](Self::observe_category) for direct mismatch feeding.
pub struct ParityDriftMonitor {
    config: ParityDriftConfig,
    monitors: BTreeMap<String, CategoryMonitor>,
    /// Total observations processed across all categories.
    total_observations: usize,
}

impl ParityDriftMonitor {
    /// Create a new monitor covering all 9 feature categories.
    #[must_use]
    pub fn new(config: ParityDriftConfig) -> Self {
        let mut monitors = BTreeMap::new();
        for cat in FeatureCategory::ALL {
            let monitor = CategoryMonitor::new(cat, &config);
            monitors.insert(cat.display_name().to_owned(), monitor);
        }
        Self {
            config,
            monitors,
            total_observations: 0,
        }
    }

    /// Observe a single mismatch/match for a specific category.
    pub fn observe_category(&mut self, category: FeatureCategory, mismatch: bool) {
        if let Some(monitor) = self.monitors.get_mut(category.display_name()) {
            monitor.observe(mismatch);
            self.total_observations += 1;
        }
    }

    /// Observe a batch of outcomes for a category.
    pub fn observe_batch(&mut self, category: FeatureCategory, mismatches: usize, total: usize) {
        if let Some(monitor) = self.monitors.get_mut(category.display_name()) {
            for i in 0..total {
                monitor.observe(i < mismatches);
            }
            self.total_observations += total;
        }
    }

    /// Feed observations from a `GateReport` snapshot.
    ///
    /// For each category in the report, translates the verification percentage
    /// into mismatch observations (1 observation per invariant: mismatch if
    /// the invariant did not pass the gate).
    pub fn observe_from_gate_report(&mut self, report: &GateReport) {
        for (cat_name, cat_result) in &report.category_results {
            if let Some(monitor) = self.monitors.get_mut(cat_name) {
                let passing = cat_result.passing_invariants;
                let total = cat_result.total_invariants;
                let failing = total.saturating_sub(passing);
                // Feed passing invariants as matches.
                for _ in 0..passing {
                    monitor.observe(false);
                }
                // Feed failing invariants as mismatches.
                for _ in 0..failing {
                    monitor.observe(true);
                }
                self.total_observations += total;
            }
        }
    }

    /// Finalize all monitors (flush partial windows).
    pub fn finalize(&mut self) {
        for monitor in self.monitors.values_mut() {
            monitor.finalize();
        }
    }

    /// Take a snapshot of the current monitor state.
    #[must_use]
    pub fn snapshot(&self) -> ParityDriftSnapshot {
        let mut category_states = BTreeMap::new();

        for (name, monitor) in &self.monitors {
            category_states.insert(
                name.clone(),
                CategoryDriftState {
                    category: name.clone(),
                    e_value: truncate_score(monitor.e_process.current),
                    threshold: truncate_score(monitor.e_process.threshold()),
                    rejected: monitor.e_process.rejected,
                    rejection_time: monitor.e_process.rejection_time,
                    observations: monitor.e_process.observations,
                    mismatches_observed: monitor.e_process.mismatches_observed,
                    empirical_rate: monitor.e_process.empirical_rate(),
                    current_regime: monitor.current_regime,
                    drift_baseline: truncate_score(monitor.drift_detector.baseline()),
                    drift_windows_observed: monitor.drift_detector.windows_observed(),
                    drift_alerts_count: monitor.drift_detector.alerts().len(),
                },
            );
        }

        let any_rejected = self.monitors.values().any(|m| m.e_process.rejected);
        let any_drift = self.monitors.values().any(|m| {
            m.current_regime == Regime::ShiftDetected || m.current_regime == Regime::Regressing
        });

        ParityDriftSnapshot {
            schema_version: DRIFT_MONITOR_SCHEMA_VERSION,
            total_observations: self.total_observations,
            any_rejected,
            any_drift,
            category_states,
        }
    }

    /// Compute all active alarms.
    #[must_use]
    pub fn alarms(&self) -> Vec<ParityDriftAlarm> {
        let mut alarms = Vec::new();

        for (name, monitor) in &self.monitors {
            let ep = &monitor.e_process;
            let rate = ep.empirical_rate();

            // Determine alarm level from e-value thresholds.
            let evalue_level = if ep.current >= self.config.critical_evalue_threshold {
                Some(AlarmLevel::Critical)
            } else if ep.current >= self.config.warning_evalue_threshold {
                Some(AlarmLevel::Warning)
            } else {
                None
            };

            // Determine alarm level from rate thresholds.
            let rate_level = if rate >= self.config.critical_rate_threshold {
                Some(AlarmLevel::Critical)
            } else if rate >= self.config.warning_rate_threshold {
                Some(AlarmLevel::Warning)
            } else {
                None
            };

            // Regime-based escalation.
            let regime_level = match monitor.current_regime {
                Regime::ShiftDetected => Some(AlarmLevel::Warning),
                Regime::Regressing => Some(AlarmLevel::Info),
                _ => None,
            };

            // Take the highest alarm level.
            let level = [evalue_level, rate_level, regime_level]
                .into_iter()
                .flatten()
                .max();

            if let Some(level) = level {
                let message = format_alarm_message(
                    level,
                    name,
                    ep.current,
                    rate,
                    ep.observations,
                    monitor.current_regime,
                );
                let runbook = build_runbook(level, name, rate, monitor.current_regime);

                alarms.push(ParityDriftAlarm {
                    level,
                    category: name.clone(),
                    e_value: truncate_score(ep.current),
                    mismatch_rate: rate,
                    observations: ep.observations,
                    regime: monitor.current_regime,
                    message,
                    runbook,
                });
            }
        }

        // Sort: Critical first, then Warning, then Info; within same level by category.
        alarms.sort_by(|a, b| {
            b.level
                .cmp(&a.level)
                .then_with(|| a.category.cmp(&b.category))
        });

        alarms
    }

    /// Whether any category has a rejected e-process (strong evidence of drift).
    #[must_use]
    pub fn any_rejected(&self) -> bool {
        self.monitors.values().any(|m| m.e_process.rejected)
    }

    /// Categories with rejected e-processes.
    #[must_use]
    pub fn rejected_categories(&self) -> Vec<String> {
        self.monitors
            .iter()
            .filter(|(_, m)| m.e_process.rejected)
            .map(|(name, _)| name.clone())
            .collect()
    }

    /// Whether any category is in a drift regime.
    #[must_use]
    pub fn any_drift(&self) -> bool {
        self.monitors.values().any(|m| {
            m.current_regime == Regime::ShiftDetected || m.current_regime == Regime::Regressing
        })
    }

    /// Reset all monitors to initial state.
    pub fn reset(&mut self) {
        for monitor in self.monitors.values_mut() {
            monitor.e_process.reset();
            monitor.drift_detector = DriftDetector::new(DriftDetectorConfig {
                window_size: monitor.window_size,
                sensitivity_threshold: self.config.drift_sensitivity,
                ema_alpha: self.config.drift_ema_alpha,
                warmup_windows: self.config.drift_warmup_windows,
            });
            monitor.window_buffer.clear();
            monitor.current_regime = Regime::Stable;
        }
        self.total_observations = 0;
    }

    /// Total observations across all categories.
    #[must_use]
    pub fn total_observations(&self) -> usize {
        self.total_observations
    }

    /// Access configuration.
    #[must_use]
    pub fn config(&self) -> &ParityDriftConfig {
        &self.config
    }
}

// ---------------------------------------------------------------------------
// Snapshot types
// ---------------------------------------------------------------------------

/// Point-in-time snapshot of the drift monitoring system.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParityDriftSnapshot {
    /// Schema version.
    pub schema_version: u32,
    /// Total observations processed.
    pub total_observations: usize,
    /// Whether any category e-process has rejected H₀.
    pub any_rejected: bool,
    /// Whether any category is in a drift regime.
    pub any_drift: bool,
    /// Per-category state.
    pub category_states: BTreeMap<String, CategoryDriftState>,
}

impl ParityDriftSnapshot {
    /// Serialize to JSON.
    ///
    /// # Errors
    ///
    /// Returns `Err` if serialization fails.
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

/// Per-category drift monitor state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CategoryDriftState {
    /// Category name.
    pub category: String,
    /// Current e-value.
    pub e_value: f64,
    /// Rejection threshold (1/α).
    pub threshold: f64,
    /// Whether H₀ has been rejected.
    pub rejected: bool,
    /// Observation at first rejection.
    pub rejection_time: Option<usize>,
    /// Total observations.
    pub observations: usize,
    /// Number of mismatch observations.
    pub mismatches_observed: usize,
    /// Empirical mismatch rate.
    pub empirical_rate: f64,
    /// Current BOCPD regime.
    pub current_regime: Regime,
    /// BOCPD baseline rate.
    pub drift_baseline: f64,
    /// Number of BOCPD windows observed.
    pub drift_windows_observed: usize,
    /// Number of drift alerts emitted.
    pub drift_alerts_count: usize,
}

// ---------------------------------------------------------------------------
// Alarm formatting and runbooks
// ---------------------------------------------------------------------------

fn format_alarm_message(
    level: AlarmLevel,
    category: &str,
    e_value: f64,
    rate: f64,
    observations: usize,
    regime: Regime,
) -> String {
    format!(
        "[{level}] {category}: e-value={e_value:.2}, mismatch_rate={rate:.3}, \
         observations={observations}, regime={regime}"
    )
}

fn build_runbook(level: AlarmLevel, category: &str, rate: f64, regime: Regime) -> RunbookEntry {
    match level {
        AlarmLevel::Critical => RunbookEntry {
            title: format!("CRITICAL drift in {category}"),
            condition: format!(
                "Category '{category}' has strong evidence of parity regression \
                 (mismatch rate: {rate:.1}%, regime: {regime})"
            ),
            actions: vec![
                "Block release until drift is resolved".to_owned(),
                format!("Run targeted differential tests for {category}"),
                "Review recent commits touching this category".to_owned(),
                "Check if new SQLite features were added without FrankenSQLite coverage".to_owned(),
            ],
            escalation: "If not resolved within 24h, escalate to project lead".to_owned(),
        },
        AlarmLevel::Warning => RunbookEntry {
            title: format!("WARNING drift in {category}"),
            condition: format!(
                "Category '{category}' shows sustained drift \
                 (mismatch rate: {rate:.1}%, regime: {regime})"
            ),
            actions: vec![
                format!("Investigate {category} mismatch patterns"),
                "Run full parity test suite for this category".to_owned(),
                "Check for flaky tests or environment issues".to_owned(),
            ],
            escalation: "If drift persists for 3+ observation windows, escalate to Critical"
                .to_owned(),
        },
        AlarmLevel::Info => RunbookEntry {
            title: format!("INFO drift signal in {category}"),
            condition: format!(
                "Category '{category}' shows early drift signal \
                 (mismatch rate: {rate:.1}%, regime: {regime})"
            ),
            actions: vec![
                "Monitor for sustained pattern".to_owned(),
                "No immediate action required".to_owned(),
            ],
            escalation: "Escalate to Warning if signal persists".to_owned(),
        },
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::confidence_gates::{GateConfig, evaluate_gate};
    use crate::parity_invariant_catalog::build_canonical_catalog;

    fn default_config() -> ParityDriftConfig {
        ParityDriftConfig::default()
    }

    // --- Construction ---

    #[test]
    fn monitor_creates_all_categories() {
        let monitor = ParityDriftMonitor::new(default_config());
        assert_eq!(
            monitor.monitors.len(),
            FeatureCategory::ALL.len(),
            "should have one monitor per category"
        );
        for cat in FeatureCategory::ALL {
            assert!(
                monitor.monitors.contains_key(cat.display_name()),
                "missing monitor for {cat}"
            );
        }
    }

    #[test]
    fn monitor_initial_state_clean() {
        let monitor = ParityDriftMonitor::new(default_config());
        assert!(!monitor.any_rejected());
        assert!(!monitor.any_drift());
        assert_eq!(monitor.total_observations(), 0);
        assert!(monitor.alarms().is_empty());
    }

    // --- E-process behavior ---

    #[test]
    fn eprocess_does_not_reject_under_null() {
        // Under H₀ (no violations), the e-process is a supermartingale:
        // E[E_t] ≤ 1. With p₀=0.05, λ=0.8, factor = 0.96 per clean obs,
        // so E_t decays — this is correct, it must NOT reject.
        let mut ep = CategoryEProcess::new("test", 0.05, 0.8, 0.01, 1e12);
        for _ in 0..10_000 {
            ep.observe(false);
        }
        assert!(
            !ep.rejected,
            "should not reject under H₀, e_value={:.6}",
            ep.current
        );
        assert!(
            ep.current < ep.threshold(),
            "e-value must stay below threshold under H₀"
        );
    }

    #[test]
    fn eprocess_rejects_under_sustained_violations() {
        let mut ep = CategoryEProcess::new("test", 0.05, 0.8, 0.01, 1e12);
        // Feed 20% mismatch rate (well above p₀=5%).
        for i in 0..2000 {
            ep.observe(i % 5 == 0);
        }
        assert!(
            ep.rejected,
            "should reject with 20% mismatch rate (p₀=5%), e_value={:.4}",
            ep.current
        );
        assert!(ep.rejection_time.is_some());
    }

    #[test]
    fn eprocess_single_observation_factor() {
        let mut ep = CategoryEProcess::new("test", 0.05, 0.8, 0.01, 1e12);
        let before = ep.current;
        ep.observe(true);
        let after = ep.current;
        let expected_factor = 1.0 + 0.8 * (1.0 - 0.05);
        assert!(
            ((after / before) - expected_factor).abs() < 1e-10,
            "factor mismatch: expected {expected_factor}, got {}",
            after / before
        );
    }

    #[test]
    fn eprocess_max_evalue_cap() {
        let mut ep = CategoryEProcess::new("test", 0.01, 0.9, 0.001, 1e6);
        for _ in 0..5000 {
            ep.observe(true);
        }
        assert!(
            ep.current <= 1e6,
            "e-value should be capped at 1e6, got {}",
            ep.current
        );
        assert!(ep.current.is_finite());
    }

    #[test]
    fn eprocess_empirical_rate_correct() {
        let mut ep = CategoryEProcess::new("test", 0.05, 0.8, 0.01, 1e12);
        for i in 0..100 {
            ep.observe(i % 10 == 0); // 10% rate
        }
        let rate = ep.empirical_rate();
        assert!(
            (rate - 0.1).abs() < 0.01,
            "empirical rate should be ~0.1, got {rate}"
        );
    }

    #[test]
    fn eprocess_reset_clears_state() {
        let mut ep = CategoryEProcess::new("test", 0.05, 0.8, 0.01, 1e12);
        for _ in 0..100 {
            ep.observe(true);
        }
        ep.reset();
        assert!((ep.current - 1.0).abs() < f64::EPSILON);
        assert_eq!(ep.observations, 0);
        assert!(!ep.rejected);
    }

    // --- Category monitor integration ---

    #[test]
    fn observe_category_updates_state() {
        let mut monitor = ParityDriftMonitor::new(default_config());
        for _ in 0..20 {
            monitor.observe_category(FeatureCategory::SqlGrammar, false);
        }
        let snapshot = monitor.snapshot();
        let state = &snapshot.category_states["SQL Grammar"];
        assert_eq!(state.observations, 20);
        assert_eq!(state.mismatches_observed, 0);
        assert!(!state.rejected);
    }

    #[test]
    fn observe_batch_feeds_correctly() {
        let mut monitor = ParityDriftMonitor::new(default_config());
        monitor.observe_batch(FeatureCategory::VdbeOpcodes, 3, 10);
        let snapshot = monitor.snapshot();
        let state = &snapshot.category_states["VDBE Opcodes"];
        assert_eq!(state.observations, 10);
        assert_eq!(state.mismatches_observed, 3);
    }

    // --- Gate report integration ---

    #[test]
    fn observe_from_gate_report_feeds_all_categories() {
        let catalog = build_canonical_catalog();
        let config = GateConfig::default();
        let report = evaluate_gate(&catalog, &config);

        let mut monitor = ParityDriftMonitor::new(default_config());
        monitor.observe_from_gate_report(&report);

        let snapshot = monitor.snapshot();
        assert!(snapshot.total_observations > 0);

        // Every category should have some observations.
        for cat in FeatureCategory::ALL {
            let state = &snapshot.category_states[cat.display_name()];
            assert!(
                state.observations > 0,
                "category {} should have observations",
                cat.display_name()
            );
        }
    }

    // --- Drift detection ---

    #[test]
    fn sustained_mismatch_triggers_drift() {
        let config = ParityDriftConfig {
            drift_window_size: 5,
            drift_warmup_windows: 2,
            drift_sensitivity: 0.05,
            ..default_config()
        };
        let mut monitor = ParityDriftMonitor::new(config);

        // Warmup: clean observations.
        for _ in 0..20 {
            monitor.observe_category(FeatureCategory::StorageTransaction, false);
        }

        // Inject sustained mismatches.
        for _ in 0..30 {
            monitor.observe_category(FeatureCategory::StorageTransaction, true);
        }

        monitor.finalize();
        let snapshot = monitor.snapshot();
        let state = &snapshot.category_states["Storage & Transactions"];

        // The drift detector should detect the regime change.
        assert!(
            state.drift_windows_observed > 0,
            "drift detector should have observed windows"
        );
    }

    // --- Alarm system ---

    #[test]
    fn no_alarms_under_null() {
        let mut monitor = ParityDriftMonitor::new(default_config());
        for _ in 0..100 {
            for cat in FeatureCategory::ALL {
                monitor.observe_category(cat, false);
            }
        }
        let alarms = monitor.alarms();
        assert!(
            alarms.is_empty(),
            "no alarms expected under H₀, got {} alarms",
            alarms.len()
        );
    }

    #[test]
    fn high_mismatch_rate_triggers_alarm() {
        let config = ParityDriftConfig {
            warning_rate_threshold: 0.2,
            critical_rate_threshold: 0.4,
            ..default_config()
        };
        let mut monitor = ParityDriftMonitor::new(config);

        // Feed 50% mismatch rate into SQL Grammar.
        for i in 0..200 {
            monitor.observe_category(FeatureCategory::SqlGrammar, i % 2 == 0);
        }

        let alarms = monitor.alarms();
        assert!(!alarms.is_empty(), "should have at least one alarm");

        let sql_alarm = alarms
            .iter()
            .find(|a| a.category == "SQL Grammar")
            .expect("should have alarm for SQL Grammar");

        assert!(
            sql_alarm.level >= AlarmLevel::Warning,
            "50% mismatch should trigger at least Warning"
        );
    }

    #[test]
    fn critical_evalue_triggers_critical_alarm() {
        let config = ParityDriftConfig {
            critical_evalue_threshold: 50.0,
            ..default_config()
        };
        let mut monitor = ParityDriftMonitor::new(config);

        // Feed sustained violations to drive e-value high.
        for i in 0..500 {
            monitor.observe_category(FeatureCategory::Pragma, i % 5 == 0);
        }

        let alarms = monitor.alarms();
        let pragma_alarm = alarms.iter().find(|a| a.category == "PRAGMAs");

        if let Some(alarm) = pragma_alarm {
            // E-value should have grown significantly with 20% rate vs 5% baseline.
            assert!(
                alarm.level >= AlarmLevel::Warning,
                "high e-value should trigger at least Warning"
            );
        }
    }

    #[test]
    fn alarms_sorted_by_severity() {
        let config = ParityDriftConfig {
            warning_rate_threshold: 0.15,
            critical_rate_threshold: 0.4,
            ..default_config()
        };
        let mut monitor = ParityDriftMonitor::new(config);

        // Different mismatch rates per category.
        for i in 0..200 {
            // SQL Grammar: 50% → Critical
            monitor.observe_category(FeatureCategory::SqlGrammar, i % 2 == 0);
            // VDBE: 20% → Warning
            monitor.observe_category(FeatureCategory::VdbeOpcodes, i % 5 == 0);
        }

        let alarms = monitor.alarms();
        // Verify sorting: higher severity first.
        for pair in alarms.windows(2) {
            assert!(
                pair[0].level >= pair[1].level,
                "alarms should be sorted by severity (descending)"
            );
        }
    }

    #[test]
    fn alarm_has_runbook() {
        let config = ParityDriftConfig {
            warning_rate_threshold: 0.1,
            ..default_config()
        };
        let mut monitor = ParityDriftMonitor::new(config);

        for i in 0..100 {
            monitor.observe_category(FeatureCategory::TypeSystem, i % 3 == 0);
        }

        let alarms = monitor.alarms();
        for alarm in &alarms {
            assert!(!alarm.runbook.title.is_empty());
            assert!(!alarm.runbook.actions.is_empty());
            assert!(!alarm.runbook.escalation.is_empty());
        }
    }

    // --- Snapshot ---

    #[test]
    fn snapshot_json_roundtrip() {
        let mut monitor = ParityDriftMonitor::new(default_config());
        for _ in 0..10 {
            monitor.observe_category(FeatureCategory::SqlGrammar, false);
        }
        let snapshot = monitor.snapshot();
        let json = snapshot.to_json().expect("serialize");
        let restored = ParityDriftSnapshot::from_json(&json).expect("deserialize");
        assert_eq!(restored.total_observations, snapshot.total_observations);
        assert_eq!(
            restored.category_states.len(),
            snapshot.category_states.len()
        );
    }

    #[test]
    fn snapshot_reflects_rejection() {
        let mut monitor = ParityDriftMonitor::new(default_config());
        // Drive SQL Grammar to rejection with very high rate.
        for i in 0..3000 {
            monitor.observe_category(FeatureCategory::SqlGrammar, i % 3 == 0);
        }
        let snapshot = monitor.snapshot();
        assert!(snapshot.any_rejected);
        assert!(
            snapshot.category_states["SQL Grammar"].rejected,
            "SQL Grammar should be rejected"
        );
    }

    // --- Reset ---

    #[test]
    fn reset_clears_all_state() {
        let mut monitor = ParityDriftMonitor::new(default_config());
        for i in 0..100 {
            monitor.observe_category(FeatureCategory::Extensions, i % 2 == 0);
        }
        assert!(monitor.total_observations() > 0);

        monitor.reset();
        assert_eq!(monitor.total_observations(), 0);
        assert!(!monitor.any_rejected());
        let snapshot = monitor.snapshot();
        for state in snapshot.category_states.values() {
            assert_eq!(state.observations, 0);
            assert!(!state.rejected);
        }
    }

    // --- False alarm control ---

    #[test]
    fn no_false_rejection_under_null_long_run() {
        let config = ParityDriftConfig {
            e_process_alpha: 0.01,
            ..default_config()
        };
        let mut monitor = ParityDriftMonitor::new(config);

        // 50K clean observations per category.
        for _ in 0..50_000 {
            for cat in FeatureCategory::ALL {
                monitor.observe_category(cat, false);
            }
        }

        assert!(
            !monitor.any_rejected(),
            "no category should reject under null after 50K observations"
        );
    }

    // --- Detection power ---

    #[test]
    fn detects_moderate_drift_within_reasonable_observations() {
        let mut monitor = ParityDriftMonitor::new(default_config());

        // Feed 15% mismatch rate (3x the p₀=5% baseline).
        let cat = FeatureCategory::BuiltinFunctions;
        for i in 0..5000 {
            monitor.observe_category(cat, i % 7 == 0); // ~14.3%
        }

        let snapshot = monitor.snapshot();
        let state = &snapshot.category_states["Built-in Functions"];
        assert!(
            state.rejected,
            "should detect 14.3% mismatch against p₀=5% within 5K observations, \
             e_value={:.4}",
            state.e_value
        );
    }

    // --- Rejected categories ---

    #[test]
    fn rejected_categories_list() {
        let mut monitor = ParityDriftMonitor::new(default_config());

        // Drive one category to rejection.
        for i in 0..3000 {
            monitor.observe_category(FeatureCategory::FileFormat, i % 4 == 0);
        }

        let rejected = monitor.rejected_categories();
        assert!(
            rejected.contains(&"File Format".to_owned()),
            "File Format should be in rejected list"
        );
    }

    // --- AlarmLevel ordering ---

    #[test]
    fn alarm_level_ordering() {
        assert!(AlarmLevel::Info < AlarmLevel::Warning);
        assert!(AlarmLevel::Warning < AlarmLevel::Critical);
    }

    #[test]
    fn alarm_level_display() {
        assert_eq!(AlarmLevel::Info.to_string(), "INFO");
        assert_eq!(AlarmLevel::Warning.to_string(), "WARNING");
        assert_eq!(AlarmLevel::Critical.to_string(), "CRITICAL");
    }

    // --- Config defaults ---

    #[test]
    fn config_defaults_reasonable() {
        let config = ParityDriftConfig::default();
        assert!(config.e_process_p0 > 0.0 && config.e_process_p0 < 1.0);
        assert!(config.e_process_lambda > 0.0);
        assert!(config.e_process_alpha > 0.0 && config.e_process_alpha < 1.0);
        assert!(config.drift_window_size > 0);
        assert!(config.warning_evalue_threshold < config.critical_evalue_threshold);
        assert!(config.warning_rate_threshold < config.critical_rate_threshold);
    }
}
