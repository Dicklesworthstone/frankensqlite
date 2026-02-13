//! Real-database replay harness with drift and regime detection (bd-1dp9.2.4).
//!
//! This module provides deterministic replay of real-world SQLite workloads
//! against both C SQLite and FrankenSQLite, with:
//!
//! 1. **Deterministic replay**: reproducible execution via [`ExecutionEnvelope`]
//!    seeds and fixed ordering.
//! 2. **Drift detection**: BOCPD-inspired monitoring of mismatch rates over
//!    sequential replay batches to detect regime shifts (new failure patterns
//!    appearing or disappearing).
//! 3. **Regime classification**: labels each observation window as `Stable`,
//!    `Improving`, `Regressing`, or `ShiftDetected`.
//! 4. **Nightly artifacts**: JSON-serializable replay summaries with ranked
//!    impact alerts suitable for CI consumption.
//!
//! # Architecture
//!
//! ```text
//! CorpusManifest → ReplaySession → [DifferentialResult per entry]
//!                                       ↓
//!                              DriftDetector (BOCPD)
//!                                       ↓
//!                              ReplaySummary + DriftAlert[]
//! ```
//!
//! # Determinism
//!
//! All operations are deterministic given the same corpus and configuration.
//! Seeds propagate from the corpus manifest through execution envelopes.

use std::collections::BTreeMap;
use std::fmt;
use std::fmt::Write as _;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::differential_v2::{DifferentialResult, Outcome};
use crate::mismatch_minimizer::{
    minimize_workload, MinimalReproduction, MinimizerConfig, ReproducibilityTest, Subsystem,
};

/// Bead identifier for log correlation.
#[allow(dead_code)]
const BEAD_ID: &str = "bd-1dp9.2.4";

/// Schema version for replay harness output format.
pub const REPLAY_SCHEMA_VERSION: u32 = 1;
/// Schema version for replay-minimization package artifacts.
pub const REPLAY_MINIMIZATION_SCHEMA_VERSION: u32 = 1;
/// Bead identifier for failure replay/minimization packaging.
pub const REPLAY_MINIMIZATION_BEAD_ID: &str = "bd-mblr.2.3.2";
/// Schema version for bisect-ready replay manifest contracts.
pub const BISECT_REPLAY_MANIFEST_SCHEMA_VERSION: &str = "1.0.0";

// ===========================================================================
// Regime Classification
// ===========================================================================

/// Regime classification for a replay observation window.
///
/// Based on BOCPD (Bayesian Online Change Point Detection) principles:
/// the detector maintains a running estimate of mismatch rate and flags
/// when the rate deviates beyond a configurable threshold.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Regime {
    /// Mismatch rate is stable within normal bounds.
    Stable,
    /// Mismatch rate is decreasing (parity improving).
    Improving,
    /// Mismatch rate is increasing (parity regressing).
    Regressing,
    /// A statistically significant change point was detected.
    ShiftDetected,
}

impl fmt::Display for Regime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Stable => write!(f, "stable"),
            Self::Improving => write!(f, "improving"),
            Self::Regressing => write!(f, "regressing"),
            Self::ShiftDetected => write!(f, "shift_detected"),
        }
    }
}

// ===========================================================================
// Drift Alert
// ===========================================================================

/// A drift alert emitted when regime change is detected.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DriftAlert {
    /// Sequential window index where the alert fired.
    pub window_index: usize,
    /// Previous regime.
    pub previous_regime: Regime,
    /// New regime.
    pub new_regime: Regime,
    /// Mismatch rate in the current window.
    pub current_mismatch_rate: f64,
    /// Baseline mismatch rate (running average).
    pub baseline_mismatch_rate: f64,
    /// Magnitude of change (absolute delta).
    pub magnitude: f64,
    /// Impact ranking (0 = highest priority).
    pub impact_rank: usize,
    /// Human-readable summary.
    pub summary: String,
}

// ===========================================================================
// Replay Entry Result
// ===========================================================================

/// Result of replaying a single corpus entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayEntryResult {
    /// Corpus entry identifier.
    pub entry_id: String,
    /// Outcome of the differential comparison.
    pub outcome: Outcome,
    /// Number of statements executed.
    pub statements_total: usize,
    /// Number of mismatched statements.
    pub statements_mismatched: usize,
    /// Mismatch rate for this entry.
    pub mismatch_rate: f64,
    /// Attributed subsystem (if divergence occurred).
    pub subsystem: Option<Subsystem>,
    /// Envelope artifact ID for reproducibility.
    pub artifact_id: String,
}

// ===========================================================================
// Replay Window
// ===========================================================================

/// An observation window aggregating multiple replay entry results.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayWindow {
    /// Zero-based window index.
    pub index: usize,
    /// Number of entries in this window.
    pub entry_count: usize,
    /// Aggregate mismatch rate across all entries in the window.
    pub mismatch_rate: f64,
    /// Number of entries that diverged.
    pub divergent_entries: usize,
    /// Classified regime for this window.
    pub regime: Regime,
}

// ===========================================================================
// Drift Detector (BOCPD-inspired)
// ===========================================================================

/// Configuration for the drift detector.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DriftDetectorConfig {
    /// Number of entries per observation window.
    pub window_size: usize,
    /// Sensitivity threshold: minimum absolute rate change to trigger alert.
    pub sensitivity_threshold: f64,
    /// EMA (exponential moving average) decay factor for baseline.
    /// Higher values = faster adaptation. Must be in `(0, 1]`.
    pub ema_alpha: f64,
    /// Minimum number of windows before drift detection activates.
    pub warmup_windows: usize,
}

impl Default for DriftDetectorConfig {
    fn default() -> Self {
        Self {
            window_size: 10,
            sensitivity_threshold: 0.05,
            ema_alpha: 0.2,
            warmup_windows: 3,
        }
    }
}

/// BOCPD-inspired drift detector for mismatch rate monitoring.
///
/// Tracks an exponential moving average of the per-window mismatch rate
/// and classifies each window into a [`Regime`]. When the rate deviates
/// from baseline by more than `sensitivity_threshold`, a [`DriftAlert`]
/// is emitted.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DriftDetector {
    config: DriftDetectorConfig,
    /// Running EMA of mismatch rate.
    baseline: f64,
    /// Previous regime (for transition detection).
    previous_regime: Regime,
    /// Number of windows observed.
    windows_observed: usize,
    /// Emitted alerts.
    alerts: Vec<DriftAlert>,
    /// Per-window regime history.
    regime_history: Vec<Regime>,
}

impl DriftDetector {
    /// Create a new drift detector.
    #[must_use]
    pub fn new(config: DriftDetectorConfig) -> Self {
        Self {
            config,
            baseline: 0.0,
            previous_regime: Regime::Stable,
            windows_observed: 0,
            alerts: Vec::new(),
            regime_history: Vec::new(),
        }
    }

    /// Observe a new window's mismatch rate and classify the regime.
    pub fn observe(&mut self, window_mismatch_rate: f64) -> Regime {
        let regime = if self.windows_observed < self.config.warmup_windows {
            // During warmup, just accumulate baseline.
            if self.windows_observed == 0 {
                self.baseline = window_mismatch_rate;
            } else {
                self.baseline = self.ema_update(window_mismatch_rate);
            }
            Regime::Stable
        } else {
            let delta = window_mismatch_rate - self.baseline;
            let abs_delta = delta.abs();

            let regime = if abs_delta < self.config.sensitivity_threshold {
                Regime::Stable
            } else if delta < 0.0 {
                Regime::Improving
            } else {
                Regime::Regressing
            };

            // Check for regime transition → emit alert.
            let regime = if regime != self.previous_regime && regime != Regime::Stable {
                self.emit_alert(
                    self.windows_observed,
                    self.previous_regime,
                    regime,
                    window_mismatch_rate,
                    abs_delta,
                );
                Regime::ShiftDetected
            } else {
                regime
            };

            // Update baseline.
            self.baseline = self.ema_update(window_mismatch_rate);
            regime
        };

        self.previous_regime = regime;
        self.windows_observed += 1;
        self.regime_history.push(regime);
        regime
    }

    /// Get all emitted alerts, ranked by impact.
    #[must_use]
    pub fn alerts(&self) -> &[DriftAlert] {
        &self.alerts
    }

    /// Get the regime history.
    #[must_use]
    pub fn regime_history(&self) -> &[Regime] {
        &self.regime_history
    }

    /// Get the current baseline mismatch rate.
    #[must_use]
    pub fn baseline(&self) -> f64 {
        self.baseline
    }

    /// Number of windows observed.
    #[must_use]
    pub fn windows_observed(&self) -> usize {
        self.windows_observed
    }

    /// EMA update.
    fn ema_update(&self, new_value: f64) -> f64 {
        self.config
            .ema_alpha
            .mul_add(new_value, (1.0 - self.config.ema_alpha) * self.baseline)
    }

    /// Emit a drift alert.
    fn emit_alert(
        &mut self,
        window_index: usize,
        previous: Regime,
        new: Regime,
        current_rate: f64,
        magnitude: f64,
    ) {
        let impact_rank = self.alerts.len();
        let mut summary = String::new();
        let _ = write!(
            summary,
            "Regime shift at window {window_index}: {previous} -> {new} \
             (rate: {current_rate:.3}, baseline: {:.3}, delta: {magnitude:.3})",
            self.baseline
        );

        self.alerts.push(DriftAlert {
            window_index,
            previous_regime: previous,
            new_regime: new,
            current_mismatch_rate: current_rate,
            baseline_mismatch_rate: self.baseline,
            magnitude,
            impact_rank,
            summary,
        });
    }
}

// ===========================================================================
// Replay Session
// ===========================================================================

/// Configuration for a replay session.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayConfig {
    /// Drift detector configuration.
    pub drift_config: DriftDetectorConfig,
    /// Base seed for deterministic replay ordering.
    pub base_seed: u64,
    /// Maximum entries to replay (0 = unlimited).
    pub max_entries: usize,
    /// Whether to record per-entry details in the summary.
    pub record_entry_details: bool,
}

impl Default for ReplayConfig {
    fn default() -> Self {
        Self {
            drift_config: DriftDetectorConfig::default(),
            base_seed: 42,
            max_entries: 0,
            record_entry_details: true,
        }
    }
}

/// A replay session that processes entries and tracks drift.
#[derive(Debug)]
pub struct ReplaySession {
    config: ReplayConfig,
    detector: DriftDetector,
    entries: Vec<ReplayEntryResult>,
    current_window: Vec<ReplayEntryResult>,
    windows: Vec<ReplayWindow>,
}

impl ReplaySession {
    /// Create a new replay session.
    #[must_use]
    pub fn new(config: ReplayConfig) -> Self {
        let detector = DriftDetector::new(config.drift_config.clone());
        Self {
            config,
            detector,
            entries: Vec::new(),
            current_window: Vec::new(),
            windows: Vec::new(),
        }
    }

    /// Record the result of replaying a single entry.
    ///
    /// Returns the current regime if a window boundary was crossed.
    pub fn record_entry(&mut self, entry: ReplayEntryResult) -> Option<Regime> {
        self.current_window.push(entry.clone());
        if self.config.record_entry_details {
            self.entries.push(entry);
        }

        // Check if window is full.
        if self.current_window.len() >= self.config.drift_config.window_size {
            Some(self.flush_window())
        } else {
            None
        }
    }

    /// Record a differential result with a corpus entry ID.
    ///
    /// Convenience wrapper over [`record_entry`](Self::record_entry).
    pub fn record_differential(
        &mut self,
        entry_id: &str,
        result: &DifferentialResult,
    ) -> Option<Regime> {
        #[allow(clippy::cast_precision_loss)]
        let mismatch_rate = if result.statements_total == 0 {
            0.0
        } else {
            result.statements_mismatched as f64 / result.statements_total as f64
        };

        let subsystem = if result.outcome == Outcome::Divergence {
            Some(crate::mismatch_minimizer::attribute_subsystem(
                &result.divergences,
                &result.envelope.schema,
                &result.envelope.workload,
            ))
        } else {
            None
        };

        let entry = ReplayEntryResult {
            entry_id: entry_id.to_owned(),
            outcome: result.outcome,
            statements_total: result.statements_total,
            statements_mismatched: result.statements_mismatched,
            mismatch_rate,
            subsystem,
            artifact_id: result.artifact_hashes.envelope_id.clone(),
        };

        self.record_entry(entry)
    }

    /// Flush the current window and observe drift.
    fn flush_window(&mut self) -> Regime {
        let entry_count = self.current_window.len();
        let divergent_entries = self
            .current_window
            .iter()
            .filter(|e| e.outcome == Outcome::Divergence)
            .count();

        #[allow(clippy::cast_precision_loss)]
        let mismatch_rate = if entry_count == 0 {
            0.0
        } else {
            divergent_entries as f64 / entry_count as f64
        };

        let regime = self.detector.observe(mismatch_rate);

        let window = ReplayWindow {
            index: self.windows.len(),
            entry_count,
            mismatch_rate,
            divergent_entries,
            regime,
        };
        self.windows.push(window);
        self.current_window.clear();

        regime
    }

    /// Finalize the session: flush any remaining entries and produce summary.
    #[must_use]
    pub fn finalize(mut self) -> ReplaySummary {
        // Flush partial window if any.
        if !self.current_window.is_empty() {
            self.flush_window();
        }

        let total_entries = self.entries.len();
        let total_divergent = self
            .entries
            .iter()
            .filter(|e| e.outcome == Outcome::Divergence)
            .count();
        let total_errors = self
            .entries
            .iter()
            .filter(|e| e.outcome == Outcome::Error)
            .count();

        #[allow(clippy::cast_precision_loss)]
        let overall_mismatch_rate = if total_entries == 0 {
            0.0
        } else {
            total_divergent as f64 / total_entries as f64
        };

        // Subsystem breakdown.
        let mut subsystem_counts: BTreeMap<String, usize> = BTreeMap::new();
        for entry in &self.entries {
            if let Some(sub) = &entry.subsystem {
                *subsystem_counts.entry(sub.to_string()).or_insert(0) += 1;
            }
        }

        // Rank alerts by magnitude (highest first).
        let mut alerts = self.detector.alerts.clone();
        alerts.sort_by(|a, b| {
            b.magnitude
                .partial_cmp(&a.magnitude)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        for (i, alert) in alerts.iter_mut().enumerate() {
            alert.impact_rank = i;
        }

        // Compute artifact hash for the full summary.
        let mut hasher = Sha256::new();
        hasher.update(b"replay-v1:");
        hasher.update(self.config.base_seed.to_le_bytes());
        hasher.update(total_entries.to_le_bytes());
        hasher.update(total_divergent.to_le_bytes());
        let digest = hasher.finalize();
        let summary_hash = hex_encode_truncated(&digest, 16);

        ReplaySummary {
            schema_version: REPLAY_SCHEMA_VERSION,
            summary_hash,
            base_seed: self.config.base_seed,
            total_entries,
            total_divergent,
            total_errors,
            overall_mismatch_rate,
            subsystem_breakdown: subsystem_counts,
            windows: self.windows,
            alerts,
            regime_history: self.detector.regime_history,
            final_baseline: self.detector.baseline,
            entries: if self.config.record_entry_details {
                Some(self.entries)
            } else {
                None
            },
        }
    }
}

// ===========================================================================
// Replay Summary
// ===========================================================================

/// Complete summary of a replay session.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplaySummary {
    /// Schema version.
    pub schema_version: u32,
    /// Deterministic hash of the summary.
    pub summary_hash: String,
    /// Base seed used for replay ordering.
    pub base_seed: u64,
    /// Total corpus entries replayed.
    pub total_entries: usize,
    /// Total entries with divergences.
    pub total_divergent: usize,
    /// Total entries with errors.
    pub total_errors: usize,
    /// Overall mismatch rate.
    pub overall_mismatch_rate: f64,
    /// Subsystem breakdown of divergences.
    pub subsystem_breakdown: BTreeMap<String, usize>,
    /// Observation windows.
    pub windows: Vec<ReplayWindow>,
    /// Drift alerts ranked by impact.
    pub alerts: Vec<DriftAlert>,
    /// Regime history per window.
    pub regime_history: Vec<Regime>,
    /// Final baseline mismatch rate.
    pub final_baseline: f64,
    /// Per-entry details (if `record_entry_details` was true).
    pub entries: Option<Vec<ReplayEntryResult>>,
}

impl ReplaySummary {
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

    /// Whether any drift alerts were emitted.
    #[must_use]
    pub fn has_drift(&self) -> bool {
        !self.alerts.is_empty()
    }

    /// Number of regime shifts detected.
    #[must_use]
    pub fn shift_count(&self) -> usize {
        self.regime_history
            .iter()
            .filter(|r| **r == Regime::ShiftDetected)
            .count()
    }

    /// Human-readable summary line.
    #[must_use]
    pub fn summary_line(&self) -> String {
        format!(
            "Replay: {}/{} divergent ({:.1}%), {} windows, {} alerts, baseline={:.3}",
            self.total_divergent,
            self.total_entries,
            self.overall_mismatch_rate * 100.0,
            self.windows.len(),
            self.alerts.len(),
            self.final_baseline,
        )
    }
}

// ===========================================================================
// Replay + Minimization Package (bd-mblr.2.3.2)
// ===========================================================================

/// Compact failure snapshot used for before/after minimization evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplayFailureSnapshot {
    /// Deterministic hash of the failure evidence blob.
    pub evidence_hash: String,
    /// Statement count in the replay workload.
    pub statement_count: usize,
    /// Number of divergence sites in this snapshot.
    pub divergence_count: usize,
}

/// Before/after minimization evidence bundle.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReplayMinimizationEvidence {
    /// Snapshot from the original replay/failure evidence.
    pub before: ReplayFailureSnapshot,
    /// Snapshot after reduction/minimization.
    pub after: ReplayFailureSnapshot,
    /// Fractional reduction in statement count.
    pub reduction_ratio: f64,
    /// Whether invariant violation is still reproducible post-minimization.
    pub invariant_violation_preserved: bool,
}

/// Output package combining replay recipe and minimized reproducer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayMinimizationPackage {
    /// Artifact schema version.
    pub schema_version: u32,
    /// Bead identifier for traceability.
    pub bead_id: String,
    /// Run identifier tying package to structured logs.
    pub run_id: String,
    /// Deterministic seed used for replay ordering.
    pub base_seed: u64,
    /// Deterministic fault profile identifier.
    pub fault_profile_id: String,
    /// Artifact bundle identifier for source failure evidence.
    pub artifact_bundle_id: String,
    /// Operator replay recipe with seed/profile/artifact references.
    pub replay_recipe: String,
    /// Minimized reproducer with canonical signature.
    pub minimal_reproduction: MinimalReproduction,
    /// Before/after evidence proving reduction and violation preservation.
    pub evidence: ReplayMinimizationEvidence,
}

impl ReplayMinimizationPackage {
    /// Serialize package to pretty JSON.
    ///
    /// # Errors
    ///
    /// Returns `Err` if serialization fails.
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }

    /// Deserialize package from JSON.
    ///
    /// # Errors
    ///
    /// Returns `Err` if JSON is malformed.
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }
}

/// Request payload for building replay/minimization artifacts.
#[derive(Debug, Clone, Copy)]
pub struct ReplayMinimizationRequest<'a> {
    /// Run identifier tying package to structured logs.
    pub run_id: &'a str,
    /// Deterministic fault profile identifier.
    pub fault_profile_id: &'a str,
    /// Artifact bundle identifier for source failure evidence.
    pub artifact_bundle_id: &'a str,
    /// Schema setup statements for the failing workload.
    pub schema: &'a [String],
    /// Workload statements to minimize.
    pub workload: &'a [String],
    /// Minimizer behavior configuration.
    pub minimizer_config: &'a MinimizerConfig,
}

/// Build a replay + minimization output package from captured failure artifacts.
///
/// This function binds deterministic replay metadata (`seed`, `fault_profile_id`,
/// `artifact_bundle_id`) to a minimized reproducer and emits before/after evidence
/// suitable for operator triage or CI artifact publication.
pub fn build_replay_minimization_package(
    summary: &ReplaySummary,
    request: &ReplayMinimizationRequest<'_>,
    test_fn: &ReproducibilityTest,
) -> Option<ReplayMinimizationPackage> {
    if request.run_id.trim().is_empty()
        || request.fault_profile_id.trim().is_empty()
        || request.artifact_bundle_id.trim().is_empty()
    {
        return None;
    }

    let mut minimal = minimize_workload(
        request.schema,
        request.workload,
        request.minimizer_config,
        test_fn,
    )?;
    minimal.original_seed = summary.base_seed;

    let violation_preserved = test_fn(request.schema, &minimal.minimal_workload).is_some();
    if !violation_preserved {
        return None;
    }

    let before = ReplayFailureSnapshot {
        evidence_hash: summary.summary_hash.clone(),
        statement_count: request.workload.len(),
        divergence_count: summary.total_divergent,
    };
    let after = ReplayFailureSnapshot {
        evidence_hash: minimal.signature.hash.clone(),
        statement_count: minimal.minimal_workload.len(),
        divergence_count: minimal.divergences.len(),
    };
    let replay_recipe = format!(
        "cargo test -p fsqlite-harness -- --seed {} --fault-profile {} --artifact-bundle {}\n{}",
        summary.base_seed,
        request.fault_profile_id,
        request.artifact_bundle_id,
        minimal.repro_command
    );

    Some(ReplayMinimizationPackage {
        schema_version: REPLAY_MINIMIZATION_SCHEMA_VERSION,
        bead_id: REPLAY_MINIMIZATION_BEAD_ID.to_owned(),
        run_id: request.run_id.to_owned(),
        base_seed: summary.base_seed,
        fault_profile_id: request.fault_profile_id.to_owned(),
        artifact_bundle_id: request.artifact_bundle_id.to_owned(),
        replay_recipe,
        minimal_reproduction: minimal.clone(),
        evidence: ReplayMinimizationEvidence {
            before,
            after,
            reduction_ratio: minimal.reduction_ratio,
            invariant_violation_preserved: violation_preserved,
        },
    })
}

// ===========================================================================
// Bisect Replay Manifest Contract
// ===========================================================================

/// Pass/fail thresholds applied during bisect candidate evaluation.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplayPassCriteria {
    /// Maximum divergent entries allowed for a passing candidate.
    pub max_divergent_entries: usize,
    /// Maximum error entries allowed for a passing candidate.
    pub max_error_entries: usize,
    /// Maximum regime shifts/alerts allowed for a passing candidate.
    pub max_shift_alerts: usize,
}

/// Optional environment constraints that bisect candidates should match.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ReplayManifestEnvironment {
    /// Toolchain constraint (e.g., `nightly-2026-02-13`).
    pub toolchain: Option<String>,
    /// Platform constraint (e.g., `x86_64-unknown-linux-gnu`).
    pub platform: Option<String>,
}

/// Versioned, machine-readable replay contract for deterministic bisect runs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BisectReplayManifest {
    /// Manifest schema version for strict compatibility checks.
    pub schema_version: String,
    /// Deterministic manifest identifier.
    pub manifest_id: String,
    /// Owning bead identifier.
    pub bead_id: String,
    /// Correlates with structured logs and failure bundles.
    pub run_id: String,
    /// ISO-8601 UTC timestamp when this manifest was created.
    pub created_at: String,
    /// Summary hash this manifest was generated from.
    pub source_summary_hash: String,
    /// Deterministic base seed to replay.
    pub base_seed: u64,
    /// Number of entries expected in each candidate replay run.
    pub expected_entry_count: usize,
    /// Number of windows expected in each candidate replay run.
    pub expected_window_count: usize,
    /// Canonical command used to execute replay.
    pub replay_command: String,
    /// Pass/fail predicate thresholds.
    pub pass_criteria: ReplayPassCriteria,
    /// Optional environment constraints.
    pub environment: ReplayManifestEnvironment,
    /// Optional operator notes.
    pub notes: Vec<String>,
}

impl BisectReplayManifest {
    /// Construct a contract from a replay summary.
    #[must_use]
    pub fn from_summary(
        summary: &ReplaySummary,
        bead_id: &str,
        run_id: &str,
        created_at: &str,
        replay_command: &str,
        pass_criteria: ReplayPassCriteria,
    ) -> Self {
        let manifest_id = compute_manifest_id(summary, bead_id, run_id);
        Self {
            schema_version: BISECT_REPLAY_MANIFEST_SCHEMA_VERSION.to_owned(),
            manifest_id,
            bead_id: bead_id.to_owned(),
            run_id: run_id.to_owned(),
            created_at: created_at.to_owned(),
            source_summary_hash: summary.summary_hash.clone(),
            base_seed: summary.base_seed,
            expected_entry_count: summary.total_entries,
            expected_window_count: summary.windows.len(),
            replay_command: replay_command.to_owned(),
            pass_criteria,
            environment: ReplayManifestEnvironment::default(),
            notes: Vec::new(),
        }
    }

    /// Validate required manifest fields.
    #[must_use]
    pub fn validate(&self) -> Vec<String> {
        let mut errors = Vec::new();
        if self.schema_version != BISECT_REPLAY_MANIFEST_SCHEMA_VERSION {
            errors.push(format!(
                "schema_version mismatch: expected {BISECT_REPLAY_MANIFEST_SCHEMA_VERSION}, got {}",
                self.schema_version
            ));
        }
        if self.manifest_id.is_empty() {
            errors.push("manifest_id is empty".to_owned());
        }
        if self.bead_id.is_empty() {
            errors.push("bead_id is empty".to_owned());
        }
        if self.run_id.is_empty() {
            errors.push("run_id is empty".to_owned());
        }
        if self.created_at.is_empty() {
            errors.push("created_at is empty".to_owned());
        }
        if self.source_summary_hash.is_empty() {
            errors.push("source_summary_hash is empty".to_owned());
        }
        if self.replay_command.is_empty() {
            errors.push("replay_command is empty".to_owned());
        }
        if self.expected_entry_count == 0 {
            errors.push("expected_entry_count must be > 0".to_owned());
        }
        errors
    }

    /// Serialize contract to pretty JSON.
    ///
    /// # Errors
    ///
    /// Returns `Err` when serialization fails.
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }

    /// Deserialize contract from JSON.
    ///
    /// # Errors
    ///
    /// Returns `Err` when JSON is malformed.
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }

    /// Decode and enforce strict schema compatibility.
    ///
    /// # Errors
    ///
    /// Returns `Err` if schema version mismatches or validation fails.
    pub fn from_json_strict(json: &str) -> Result<Self, String> {
        let manifest: Self = serde_json::from_str(json)
            .map_err(|error| format!("manifest parse failed: {error}"))?;

        if manifest.schema_version != BISECT_REPLAY_MANIFEST_SCHEMA_VERSION {
            return Err(format!(
                "schema mismatch: expected {BISECT_REPLAY_MANIFEST_SCHEMA_VERSION}, got {}",
                manifest.schema_version
            ));
        }

        let errors = manifest.validate();
        if errors.is_empty() {
            Ok(manifest)
        } else {
            Err(format!("manifest validation failed: {}", errors.join("; ")))
        }
    }

    /// Evaluate a replay summary against this manifest's pass/fail predicate.
    #[must_use]
    pub fn evaluate_summary(&self, summary: &ReplaySummary) -> ReplayEvaluation {
        let mut reasons = Vec::new();

        if summary.base_seed != self.base_seed {
            reasons.push(format!(
                "base_seed mismatch: expected 0x{:016X}, got 0x{:016X}",
                self.base_seed, summary.base_seed
            ));
        }
        if summary.total_entries != self.expected_entry_count {
            reasons.push(format!(
                "entry_count mismatch: expected {}, got {}",
                self.expected_entry_count, summary.total_entries
            ));
        }
        if summary.windows.len() != self.expected_window_count {
            reasons.push(format!(
                "window_count mismatch: expected {}, got {}",
                self.expected_window_count,
                summary.windows.len()
            ));
        }
        if summary.total_divergent > self.pass_criteria.max_divergent_entries {
            reasons.push(format!(
                "divergent entries {} exceed threshold {}",
                summary.total_divergent, self.pass_criteria.max_divergent_entries
            ));
        }
        if summary.total_errors > self.pass_criteria.max_error_entries {
            reasons.push(format!(
                "error entries {} exceed threshold {}",
                summary.total_errors, self.pass_criteria.max_error_entries
            ));
        }
        let shift_alerts = summary.shift_count();
        if shift_alerts > self.pass_criteria.max_shift_alerts {
            reasons.push(format!(
                "shift alerts {} exceed threshold {}",
                shift_alerts, self.pass_criteria.max_shift_alerts
            ));
        }

        let verdict = if reasons.is_empty() {
            ReplayVerdict::Pass
        } else {
            ReplayVerdict::Fail
        };

        ReplayEvaluation {
            verdict,
            divergent_entries: summary.total_divergent,
            error_entries: summary.total_errors,
            shift_alerts,
            reasons,
        }
    }
}

/// Evaluation verdict for a bisect candidate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplayVerdict {
    /// Candidate satisfied all manifest predicates.
    Pass,
    /// Candidate violated one or more manifest predicates.
    Fail,
}

/// Result of evaluating a replay summary against a manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplayEvaluation {
    /// Pass/fail verdict.
    pub verdict: ReplayVerdict,
    /// Number of divergent entries observed.
    pub divergent_entries: usize,
    /// Number of error entries observed.
    pub error_entries: usize,
    /// Number of shift alerts observed.
    pub shift_alerts: usize,
    /// Human-readable violation reasons when verdict is `Fail`.
    pub reasons: Vec<String>,
}

// ===========================================================================
// Helpers
// ===========================================================================

fn compute_manifest_id(summary: &ReplaySummary, bead_id: &str, run_id: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"bisect-manifest-v1:");
    hasher.update(summary.summary_hash.as_bytes());
    hasher.update(bead_id.as_bytes());
    hasher.update(run_id.as_bytes());
    hasher.update(summary.base_seed.to_le_bytes());
    let digest = hasher.finalize();
    format!("rmf-{}", hex_encode_truncated(&digest, 16))
}

/// Encode bytes as hex, truncated to `max_chars` characters.
fn hex_encode_truncated(bytes: &[u8], max_chars: usize) -> String {
    let mut s = String::with_capacity(max_chars);
    for byte in bytes {
        if s.len() >= max_chars {
            break;
        }
        let _ = write!(s, "{byte:02x}");
    }
    s.truncate(max_chars);
    s
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::differential_v2::{NormalizedValue, StatementDivergence, StmtOutcome};

    fn make_entry_result(
        id: &str,
        outcome: Outcome,
        mismatched: usize,
        total: usize,
    ) -> ReplayEntryResult {
        #[allow(clippy::cast_precision_loss)]
        let mismatch_rate = if total == 0 {
            0.0
        } else {
            mismatched as f64 / total as f64
        };
        ReplayEntryResult {
            entry_id: id.to_owned(),
            outcome,
            statements_total: total,
            statements_mismatched: mismatched,
            mismatch_rate,
            subsystem: if outcome == Outcome::Divergence {
                Some(Subsystem::Vdbe)
            } else {
                None
            },
            artifact_id: format!("artifact-{id}"),
        }
    }

    fn make_manifest_summary() -> ReplaySummary {
        let config = ReplayConfig {
            drift_config: DriftDetectorConfig {
                window_size: 2,
                warmup_windows: 1,
                sensitivity_threshold: 1.0,
                ..DriftDetectorConfig::default()
            },
            ..ReplayConfig::default()
        };
        let mut session = ReplaySession::new(config);
        session.record_entry(make_entry_result("m1", Outcome::Pass, 0, 10));
        session.record_entry(make_entry_result("m2", Outcome::Divergence, 1, 10));
        session.finalize()
    }

    fn make_select_divergence(index: usize, sql: &str) -> StatementDivergence {
        StatementDivergence {
            index,
            sql: sql.to_owned(),
            csqlite_outcome: StmtOutcome::Rows(vec![vec![NormalizedValue::Integer(1)]]),
            fsqlite_outcome: StmtOutcome::Rows(vec![vec![NormalizedValue::Integer(2)]]),
        }
    }

    fn make_reproducibility_test(
    ) -> impl Fn(&[String], &[String]) -> Option<Vec<StatementDivergence>> {
        |_, workload| {
            let failing_index = workload
                .iter()
                .position(|stmt| stmt.contains("SELECT a FROM t WHERE a = 42"))?;
            Some(vec![make_select_divergence(
                failing_index,
                "SELECT a FROM t WHERE a = 42",
            )])
        }
    }

    // --- Drift Detector ---

    #[test]
    fn test_detector_warmup_stays_stable() {
        let config = DriftDetectorConfig {
            warmup_windows: 3,
            ..DriftDetectorConfig::default()
        };
        let mut detector = DriftDetector::new(config);

        assert_eq!(detector.observe(0.1), Regime::Stable);
        assert_eq!(detector.observe(0.2), Regime::Stable);
        assert_eq!(detector.observe(0.15), Regime::Stable);
        assert_eq!(detector.windows_observed(), 3);
    }

    #[test]
    fn test_detector_stable_regime() {
        let config = DriftDetectorConfig {
            warmup_windows: 2,
            sensitivity_threshold: 0.1,
            ..DriftDetectorConfig::default()
        };
        let mut detector = DriftDetector::new(config);

        // Warmup.
        detector.observe(0.3);
        detector.observe(0.3);

        // Stable observations (within threshold).
        assert_eq!(detector.observe(0.32), Regime::Stable);
        assert_eq!(detector.observe(0.28), Regime::Stable);
        assert!(detector.alerts().is_empty());
    }

    #[test]
    fn test_detector_regression_alert() {
        let config = DriftDetectorConfig {
            warmup_windows: 2,
            sensitivity_threshold: 0.05,
            ema_alpha: 0.5,
            ..DriftDetectorConfig::default()
        };
        let mut detector = DriftDetector::new(config);

        // Warmup with low rate.
        detector.observe(0.1);
        detector.observe(0.1);

        // Sudden spike.
        let regime = detector.observe(0.5);
        assert_eq!(regime, Regime::ShiftDetected);
        assert_eq!(detector.alerts().len(), 1);

        let alert = &detector.alerts()[0];
        assert_eq!(alert.new_regime, Regime::Regressing);
        assert!(alert.magnitude > 0.05);
    }

    #[test]
    fn test_detector_improving_alert() {
        let config = DriftDetectorConfig {
            warmup_windows: 2,
            sensitivity_threshold: 0.05,
            ema_alpha: 0.3,
            ..DriftDetectorConfig::default()
        };
        let mut detector = DriftDetector::new(config);

        // Warmup with high rate.
        detector.observe(0.5);
        detector.observe(0.5);

        // Sudden improvement.
        let regime = detector.observe(0.1);
        assert_eq!(regime, Regime::ShiftDetected);
        assert_eq!(detector.alerts().len(), 1);

        let alert = &detector.alerts()[0];
        assert_eq!(alert.new_regime, Regime::Improving);
    }

    #[test]
    fn test_detector_baseline_tracks_ema() {
        let config = DriftDetectorConfig {
            warmup_windows: 1,
            sensitivity_threshold: 0.5,
            ema_alpha: 0.5,
            ..DriftDetectorConfig::default()
        };
        let mut detector = DriftDetector::new(config);

        detector.observe(0.0); // warmup, baseline = 0.0
        detector.observe(1.0); // baseline = 0.5 * 1.0 + 0.5 * 0.0 = 0.5

        let baseline = detector.baseline();
        assert!(
            (baseline - 0.5).abs() < 0.01,
            "baseline should be ~0.5, got {baseline}"
        );
    }

    // --- Replay Session ---

    #[test]
    fn test_session_basic_flow() {
        let config = ReplayConfig {
            drift_config: DriftDetectorConfig {
                window_size: 2,
                warmup_windows: 1,
                sensitivity_threshold: 0.1,
                ..DriftDetectorConfig::default()
            },
            ..ReplayConfig::default()
        };
        let mut session = ReplaySession::new(config);

        // Record 4 passing entries (2 windows).
        session.record_entry(make_entry_result("e1", Outcome::Pass, 0, 10));
        session.record_entry(make_entry_result("e2", Outcome::Pass, 0, 10));
        session.record_entry(make_entry_result("e3", Outcome::Pass, 0, 10));
        session.record_entry(make_entry_result("e4", Outcome::Pass, 0, 10));

        let summary = session.finalize();
        assert_eq!(summary.total_entries, 4);
        assert_eq!(summary.total_divergent, 0);
        assert!((summary.overall_mismatch_rate).abs() < f64::EPSILON);
        assert_eq!(summary.windows.len(), 2);
    }

    #[test]
    fn test_session_detects_regression() {
        let config = ReplayConfig {
            drift_config: DriftDetectorConfig {
                window_size: 2,
                warmup_windows: 2,
                sensitivity_threshold: 0.05,
                ema_alpha: 0.5,
                ..DriftDetectorConfig::default()
            },
            ..ReplayConfig::default()
        };
        let mut session = ReplaySession::new(config);

        // Warmup: 2 windows of mostly passing.
        session.record_entry(make_entry_result("e1", Outcome::Pass, 0, 10));
        session.record_entry(make_entry_result("e2", Outcome::Pass, 0, 10));
        session.record_entry(make_entry_result("e3", Outcome::Pass, 0, 10));
        session.record_entry(make_entry_result("e4", Outcome::Pass, 0, 10));

        // Regression: window of all divergences.
        session.record_entry(make_entry_result("e5", Outcome::Divergence, 5, 10));
        session.record_entry(make_entry_result("e6", Outcome::Divergence, 5, 10));

        let summary = session.finalize();
        assert_eq!(summary.total_divergent, 2);
        assert!(summary.has_drift());
        assert!(summary.shift_count() > 0);
    }

    #[test]
    fn test_session_partial_window_flushed() {
        let config = ReplayConfig {
            drift_config: DriftDetectorConfig {
                window_size: 5,
                ..DriftDetectorConfig::default()
            },
            ..ReplayConfig::default()
        };
        let mut session = ReplaySession::new(config);

        // Record 3 entries (less than window size of 5).
        session.record_entry(make_entry_result("e1", Outcome::Pass, 0, 10));
        session.record_entry(make_entry_result("e2", Outcome::Divergence, 3, 10));
        session.record_entry(make_entry_result("e3", Outcome::Pass, 0, 10));

        let summary = session.finalize();
        assert_eq!(summary.total_entries, 3);
        assert_eq!(summary.windows.len(), 1); // Partial window flushed.
    }

    #[test]
    fn test_session_subsystem_breakdown() {
        let config = ReplayConfig::default();
        let mut session = ReplaySession::new(config);

        // Mix of outcomes.
        for i in 0..5 {
            let outcome = if i % 2 == 0 {
                Outcome::Divergence
            } else {
                Outcome::Pass
            };
            let mismatched = if outcome == Outcome::Divergence { 2 } else { 0 };
            session.record_entry(make_entry_result(&format!("e{i}"), outcome, mismatched, 10));
        }

        let summary = session.finalize();
        assert_eq!(summary.total_divergent, 3);
        assert!(summary.subsystem_breakdown.contains_key("vdbe"));
        assert_eq!(summary.subsystem_breakdown["vdbe"], 3);
    }

    #[test]
    fn test_session_empty() {
        let config = ReplayConfig::default();
        let session = ReplaySession::new(config);
        let summary = session.finalize();

        assert_eq!(summary.total_entries, 0);
        assert_eq!(summary.total_divergent, 0);
        assert!(!summary.has_drift());
        assert_eq!(summary.windows.len(), 0);
    }

    // --- Summary ---

    #[test]
    fn test_summary_json_roundtrip() {
        let config = ReplayConfig {
            drift_config: DriftDetectorConfig {
                window_size: 2,
                ..DriftDetectorConfig::default()
            },
            ..ReplayConfig::default()
        };
        let mut session = ReplaySession::new(config);

        session.record_entry(make_entry_result("e1", Outcome::Pass, 0, 10));
        session.record_entry(make_entry_result("e2", Outcome::Divergence, 3, 10));

        let summary = session.finalize();
        let json = summary.to_json().expect("serialize");
        let restored = ReplaySummary::from_json(&json).expect("deserialize");

        assert_eq!(restored.total_entries, summary.total_entries);
        assert_eq!(restored.summary_hash, summary.summary_hash);
        assert_eq!(restored.windows.len(), summary.windows.len());
    }

    #[test]
    fn test_summary_line_format() {
        let config = ReplayConfig {
            drift_config: DriftDetectorConfig {
                window_size: 2,
                ..DriftDetectorConfig::default()
            },
            ..ReplayConfig::default()
        };
        let mut session = ReplaySession::new(config);

        session.record_entry(make_entry_result("e1", Outcome::Pass, 0, 10));
        session.record_entry(make_entry_result("e2", Outcome::Divergence, 3, 10));

        let summary = session.finalize();
        let line = summary.summary_line();
        assert!(line.contains("Replay:"));
        assert!(line.contains("1/2"));
        assert!(line.contains("divergent"));
    }

    #[test]
    fn test_summary_hash_deterministic() {
        let make_summary = || {
            let config = ReplayConfig {
                drift_config: DriftDetectorConfig {
                    window_size: 2,
                    ..DriftDetectorConfig::default()
                },
                ..ReplayConfig::default()
            };
            let mut session = ReplaySession::new(config);
            session.record_entry(make_entry_result("e1", Outcome::Pass, 0, 10));
            session.record_entry(make_entry_result("e2", Outcome::Pass, 0, 10));
            session.finalize()
        };

        let s1 = make_summary();
        let s2 = make_summary();
        assert_eq!(s1.summary_hash, s2.summary_hash);
    }

    // --- Bisect replay manifest ---

    #[test]
    fn test_bisect_manifest_roundtrip_and_validate() {
        let summary = make_manifest_summary();
        let criteria = ReplayPassCriteria {
            max_divergent_entries: summary.total_divergent,
            max_error_entries: summary.total_errors,
            max_shift_alerts: summary.shift_count(),
        };
        let manifest = BisectReplayManifest::from_summary(
            &summary,
            "bd-mblr.7.6.1",
            "run-manifest-1",
            "2026-02-13T09:00:00Z",
            "cargo test -p fsqlite-harness bisect_manifest",
            criteria,
        );

        assert_eq!(
            manifest.schema_version,
            BISECT_REPLAY_MANIFEST_SCHEMA_VERSION
        );
        assert!(!manifest.manifest_id.is_empty());
        assert!(manifest.validate().is_empty());

        let json = manifest.to_json().expect("serialize");
        let restored = BisectReplayManifest::from_json(&json).expect("deserialize");
        let strict = BisectReplayManifest::from_json_strict(&json).expect("strict deserialize");
        assert_eq!(restored, manifest);
        assert_eq!(strict, manifest);
    }

    #[test]
    fn test_bisect_manifest_strict_rejects_incompatible_schema() {
        let summary = make_manifest_summary();
        let manifest = BisectReplayManifest::from_summary(
            &summary,
            "bd-mblr.7.6.1",
            "run-manifest-2",
            "2026-02-13T09:00:00Z",
            "cargo test -p fsqlite-harness bisect_manifest",
            ReplayPassCriteria::default(),
        );

        let mut json_value = serde_json::to_value(&manifest).expect("serialize value");
        json_value["schema_version"] = serde_json::Value::String("0.0.1".to_owned());
        let bad_json = serde_json::to_string(&json_value).expect("serialize json");

        let error =
            BisectReplayManifest::from_json_strict(&bad_json).expect_err("schema mismatch error");
        assert!(error.contains("schema mismatch"));
    }

    #[test]
    fn test_bisect_manifest_evaluate_summary_pass_and_fail() {
        let summary = make_manifest_summary();
        let criteria = ReplayPassCriteria {
            max_divergent_entries: summary.total_divergent,
            max_error_entries: summary.total_errors,
            max_shift_alerts: summary.shift_count(),
        };
        let manifest = BisectReplayManifest::from_summary(
            &summary,
            "bd-mblr.7.6.1",
            "run-manifest-3",
            "2026-02-13T09:00:00Z",
            "cargo test -p fsqlite-harness bisect_manifest",
            criteria,
        );

        let pass_eval = manifest.evaluate_summary(&summary);
        assert_eq!(pass_eval.verdict, ReplayVerdict::Pass);
        assert!(pass_eval.reasons.is_empty());

        let mut failing = summary.clone();
        failing.total_divergent += 1;
        let fail_eval = manifest.evaluate_summary(&failing);
        assert_eq!(fail_eval.verdict, ReplayVerdict::Fail);
        assert!(fail_eval
            .reasons
            .iter()
            .any(|reason| reason.contains("divergent entries")));
    }

    // --- Regime Display ---

    #[test]
    fn test_regime_display() {
        assert_eq!(Regime::Stable.to_string(), "stable");
        assert_eq!(Regime::Improving.to_string(), "improving");
        assert_eq!(Regime::Regressing.to_string(), "regressing");
        assert_eq!(Regime::ShiftDetected.to_string(), "shift_detected");
    }

    // --- Config defaults ---

    #[test]
    fn test_replay_config_defaults() {
        let config = ReplayConfig::default();
        assert_eq!(config.base_seed, 42);
        assert_eq!(config.max_entries, 0);
        assert!(config.record_entry_details);
    }

    #[test]
    fn test_drift_detector_config_defaults() {
        let config = DriftDetectorConfig::default();
        assert_eq!(config.window_size, 10);
        assert!((config.sensitivity_threshold - 0.05).abs() < f64::EPSILON);
        assert!((config.ema_alpha - 0.2).abs() < f64::EPSILON);
        assert_eq!(config.warmup_windows, 3);
    }

    // --- Window boundary ---

    #[test]
    fn test_window_boundary_returns_regime() {
        let config = ReplayConfig {
            drift_config: DriftDetectorConfig {
                window_size: 2,
                ..DriftDetectorConfig::default()
            },
            ..ReplayConfig::default()
        };
        let mut session = ReplaySession::new(config);

        // First entry: no window completed.
        let r1 = session.record_entry(make_entry_result("e1", Outcome::Pass, 0, 10));
        assert!(r1.is_none());

        // Second entry: window complete.
        let r2 = session.record_entry(make_entry_result("e2", Outcome::Pass, 0, 10));
        assert!(r2.is_some());
    }

    // --- Alert ranking ---

    #[test]
    fn test_alerts_ranked_by_magnitude() {
        let config = ReplayConfig {
            drift_config: DriftDetectorConfig {
                window_size: 1,
                warmup_windows: 2,
                sensitivity_threshold: 0.05,
                ema_alpha: 0.3,
            },
            ..ReplayConfig::default()
        };
        let mut session = ReplaySession::new(config);

        // Warmup.
        session.record_entry(make_entry_result("e1", Outcome::Pass, 0, 10));
        session.record_entry(make_entry_result("e2", Outcome::Pass, 0, 10));

        // Small regression.
        session.record_entry(make_entry_result("e3", Outcome::Divergence, 2, 10));

        // Back to passing, then bigger regression.
        session.record_entry(make_entry_result("e4", Outcome::Pass, 0, 10));
        session.record_entry(make_entry_result("e5", Outcome::Divergence, 10, 10));

        let summary = session.finalize();
        if summary.alerts.len() >= 2 {
            assert!(
                summary.alerts[0].magnitude >= summary.alerts[1].magnitude,
                "alerts should be ranked by magnitude descending"
            );
        }
    }

    // --- No-detail mode ---

    #[test]
    fn test_session_no_entry_details() {
        let config = ReplayConfig {
            record_entry_details: false,
            drift_config: DriftDetectorConfig {
                window_size: 2,
                ..DriftDetectorConfig::default()
            },
            ..ReplayConfig::default()
        };
        let mut session = ReplaySession::new(config);

        session.record_entry(make_entry_result("e1", Outcome::Pass, 0, 10));
        session.record_entry(make_entry_result("e2", Outcome::Pass, 0, 10));

        let summary = session.finalize();
        assert!(summary.entries.is_none());
    }

    // --- Replay minimization package (bd-mblr.2.3.2) ---

    #[test]
    fn test_replay_minimization_package_reduces_and_preserves_violation() {
        let summary = make_manifest_summary();
        let schema = vec!["CREATE TABLE t(a INTEGER PRIMARY KEY);".to_owned()];
        let workload = vec![
            "INSERT INTO t VALUES (1);".to_owned(),
            "INSERT INTO t VALUES (42);".to_owned(),
            "UPDATE t SET a = a + 1 WHERE a = 1;".to_owned(),
            "SELECT a FROM t WHERE a = 42;".to_owned(),
            "DELETE FROM t WHERE a = 99;".to_owned(),
        ];
        let config = MinimizerConfig::default();
        let repro = make_reproducibility_test();
        let request = ReplayMinimizationRequest {
            run_id: "run-replay-min-1",
            fault_profile_id: "fault-profile-alpha",
            artifact_bundle_id: "artifact-bundle-alpha",
            schema: &schema,
            workload: &workload,
            minimizer_config: &config,
        };

        let package = build_replay_minimization_package(&summary, &request, &repro)
            .expect("package should be produced");

        assert_eq!(
            package.schema_version, REPLAY_MINIMIZATION_SCHEMA_VERSION,
            "schema version must match package contract"
        );
        assert_eq!(package.bead_id, REPLAY_MINIMIZATION_BEAD_ID);
        assert_eq!(package.base_seed, summary.base_seed);
        assert!(
            package.evidence.invariant_violation_preserved,
            "minimized workload must still reproduce violation"
        );
        assert!(
            package.evidence.after.statement_count < package.evidence.before.statement_count,
            "minimization should reduce statement count (before={}, after={})",
            package.evidence.before.statement_count,
            package.evidence.after.statement_count,
        );
        assert!(
            !package.minimal_reproduction.minimal_workload.is_empty(),
            "minimal workload should not be empty"
        );
        assert!(
            package
                .replay_recipe
                .contains("--artifact-bundle artifact-bundle-alpha"),
            "replay recipe must include artifact bundle reference"
        );
    }

    #[test]
    fn test_replay_minimization_package_is_deterministic() {
        let summary = make_manifest_summary();
        let schema = vec!["CREATE TABLE t(a INTEGER PRIMARY KEY);".to_owned()];
        let workload = vec![
            "INSERT INTO t VALUES (1);".to_owned(),
            "INSERT INTO t VALUES (42);".to_owned(),
            "SELECT a FROM t WHERE a = 42;".to_owned(),
        ];
        let config = MinimizerConfig::default();
        let repro = make_reproducibility_test();
        let request = ReplayMinimizationRequest {
            run_id: "run-replay-min-det",
            fault_profile_id: "fault-profile-det",
            artifact_bundle_id: "artifact-bundle-det",
            schema: &schema,
            workload: &workload,
            minimizer_config: &config,
        };

        let p1 =
            build_replay_minimization_package(&summary, &request, &repro).expect("first package");
        let p2 =
            build_replay_minimization_package(&summary, &request, &repro).expect("second package");

        assert_eq!(
            p1.minimal_reproduction.signature.hash, p2.minimal_reproduction.signature.hash,
            "signature hash should be deterministic"
        );
        assert_eq!(
            p1.replay_recipe, p2.replay_recipe,
            "replay recipe should be deterministic"
        );
        assert_eq!(
            p1.evidence, p2.evidence,
            "before/after evidence should be deterministic"
        );
    }

    #[test]
    fn test_replay_minimization_package_json_roundtrip() {
        let summary = make_manifest_summary();
        let schema = vec!["CREATE TABLE t(a INTEGER PRIMARY KEY);".to_owned()];
        let workload = vec![
            "INSERT INTO t VALUES (1);".to_owned(),
            "INSERT INTO t VALUES (42);".to_owned(),
            "SELECT a FROM t WHERE a = 42;".to_owned(),
        ];
        let config = MinimizerConfig::default();
        let repro = make_reproducibility_test();
        let request = ReplayMinimizationRequest {
            run_id: "run-replay-min-json",
            fault_profile_id: "fault-profile-json",
            artifact_bundle_id: "artifact-bundle-json",
            schema: &schema,
            workload: &workload,
            minimizer_config: &config,
        };

        let package = build_replay_minimization_package(&summary, &request, &repro)
            .expect("package should be produced");
        let json = package.to_json().expect("serialize package");
        let decoded = ReplayMinimizationPackage::from_json(&json).expect("deserialize package");
        assert_eq!(decoded.schema_version, package.schema_version);
        assert_eq!(decoded.bead_id, package.bead_id);
        assert_eq!(decoded.run_id, package.run_id);
        assert_eq!(
            decoded.minimal_reproduction.signature.hash,
            package.minimal_reproduction.signature.hash
        );
        assert_eq!(decoded.evidence, package.evidence);
    }
}
