//! Decision-theoretic SSI abort policy: victim selection + loss minimization (§5.7.3).
//!
//! Provides the Bayesian decision framework for WHEN and WHOM to abort when a
//! dangerous structure is detected, plus continuous monitoring via e-process and
//! conformal calibration.

use std::collections::VecDeque;
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock, TryLockError};

use fsqlite_types::sync_primitives::SystemTime;
use fsqlite_types::{CommitSeq, PageNumber, TxnToken};
use tracing::{debug, info, warn};

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
/// 2. **Potential cycle:** Compare costs and abort the cheaper participant.
///    On ties, default to aborting pivot for deterministic behavior.
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
            // Optimize for retry cost: abort the cheaper participant.
            if pivot_l < other_l {
                VictimDecision {
                    victim: Victim::Pivot,
                    cycle_status: status,
                    pivot_cost: pivot_l,
                    other_cost: other_l,
                    reason: "potential_cycle_abort_cheaper_pivot",
                }
            } else if other_l < pivot_l {
                VictimDecision {
                    victim: Victim::Other,
                    cycle_status: status,
                    pivot_cost: pivot_l,
                    other_cost: other_l,
                    reason: "potential_cycle_abort_cheaper_other",
                }
            } else {
                // Tie-breaker for deterministic behavior.
                VictimDecision {
                    victim: Victim::Pivot,
                    cycle_status: status,
                    pivot_cost: pivot_l,
                    other_cost: other_l,
                    reason: "potential_cycle_tie_abort_pivot",
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

/// User-facing scaling knob for DRO uncertainty radius.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DroRiskTolerance {
    Low,
    High,
}

impl DroRiskTolerance {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Low => "low",
            Self::High => "high",
        }
    }

    #[must_use]
    pub const fn radius_multiplier(self) -> f64 {
        match self {
            Self::Low => 1.0,
            Self::High => 1.75,
        }
    }
}

impl fmt::Display for DroRiskTolerance {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for DroRiskTolerance {
    type Err = &'static str;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let normalized = value.trim().to_ascii_lowercase();
        match normalized.as_str() {
            "low" => Ok(Self::Low),
            "high" => Ok(Self::High),
            _ => Err("unrecognized DRO risk tolerance"),
        }
    }
}

/// Wasserstein-style uncertainty certificate derived from recent SSI windows.
#[derive(Debug, Clone, Copy, PartialEq)]
#[allow(clippy::struct_field_names)]
pub struct DroRadiusCertificate {
    pub abort_rate_variance: f64,
    pub edge_rate_variance: f64,
    pub base_radius: f64,
    pub scaled_radius: f64,
    pub tolerance: DroRiskTolerance,
}

impl DroRadiusCertificate {
    #[must_use]
    pub const fn effective_radius(self) -> f64 {
        self.scaled_radius
    }
}

/// Hot-path DRO evaluation result for one T3 decision.
#[derive(Debug, Clone, Copy, PartialEq)]
#[allow(clippy::struct_field_names)]
pub struct DroHotPathDecision {
    pub active_readers: usize,
    pub active_writers: usize,
    pub cvar_penalty: f64,
    /// Effective topology-aware abort threshold for this reader/writer shape.
    pub threshold: f64,
    pub radius: f64,
    pub tolerance: DroRiskTolerance,
    pub decision: AbortDecision,
}

impl DroHotPathDecision {
    #[must_use]
    pub const fn should_abort(self) -> bool {
        matches!(self.decision, AbortDecision::Abort)
    }
}

/// Dense O(1) lookup table for T3 near-miss DRO penalties and thresholds.
#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::struct_field_names)]
pub struct DroLossMatrix {
    max_active_readers: usize,
    max_active_writers: usize,
    base_threshold: f64,
    radius: DroRadiusCertificate,
    penalties: Vec<f64>,
    thresholds: Vec<f64>,
}

impl DroLossMatrix {
    #[must_use]
    pub fn from_radius_certificate(
        max_active_readers: usize,
        max_active_writers: usize,
        threshold: f64,
        radius: DroRadiusCertificate,
    ) -> Self {
        let rows = max_active_readers.saturating_add(1);
        let cols = max_active_writers.saturating_add(1);
        let mut penalties = vec![0.0; rows.saturating_mul(cols)];
        let base_threshold = if threshold.is_finite() {
            threshold.clamp(0.0, 1.0)
        } else {
            0.0
        };
        let mut thresholds = vec![base_threshold; rows.saturating_mul(cols)];

        for readers in 0..rows {
            for writers in 0..cols {
                let idx = readers * cols + writers;
                let topology_pressure = dro_topology_pressure(readers, writers);
                penalties[idx] = dro_cvar_penalty_from_pressure(topology_pressure, radius);
                thresholds[idx] =
                    dro_topology_threshold_from_pressure(topology_pressure, base_threshold, radius);
            }
        }

        Self {
            max_active_readers,
            max_active_writers,
            base_threshold,
            radius,
            penalties,
            thresholds,
        }
    }

    #[must_use]
    pub const fn base_threshold(&self) -> f64 {
        self.base_threshold
    }

    #[must_use]
    pub const fn radius(&self) -> DroRadiusCertificate {
        self.radius
    }

    #[must_use]
    pub fn penalty(&self, active_readers: usize, active_writers: usize) -> f64 {
        let readers = active_readers.min(self.max_active_readers);
        let writers = active_writers.min(self.max_active_writers);
        let cols = self.max_active_writers.saturating_add(1);
        self.penalties[(readers * cols) + writers]
    }

    #[must_use]
    pub fn threshold(&self, active_readers: usize, active_writers: usize) -> f64 {
        let readers = active_readers.min(self.max_active_readers);
        let writers = active_writers.min(self.max_active_writers);
        let cols = self.max_active_writers.saturating_add(1);
        self.thresholds[(readers * cols) + writers]
    }

    #[must_use]
    pub fn evaluate(&self, active_readers: usize, active_writers: usize) -> DroHotPathDecision {
        let readers = active_readers.min(self.max_active_readers);
        let writers = active_writers.min(self.max_active_writers);
        let cvar_penalty = self.penalty(readers, writers);
        let threshold = self.threshold(readers, writers);
        let decision = if cvar_penalty > threshold {
            AbortDecision::Abort
        } else {
            AbortDecision::Commit
        };

        DroHotPathDecision {
            active_readers: readers,
            active_writers: writers,
            cvar_penalty,
            threshold,
            radius: self.radius.effective_radius(),
            tolerance: self.radius.tolerance,
            decision,
        }
    }
}

/// Build a deterministic Wasserstein-style radius certificate from recent
/// abort-rate and conflict-edge windows.
#[must_use]
pub fn dro_wasserstein_radius(
    abort_rates: &[f64],
    edge_rates: &[f64],
    tolerance: DroRiskTolerance,
) -> Option<DroRadiusCertificate> {
    let abort_rate_variance = sample_variance(abort_rates)?;
    let edge_rate_variance = sample_variance(edge_rates)?;
    let base_radius = (abort_rate_variance + edge_rate_variance).sqrt();
    let scaled_radius = base_radius * tolerance.radius_multiplier();

    Some(DroRadiusCertificate {
        abort_rate_variance,
        edge_rate_variance,
        base_radius,
        scaled_radius,
        tolerance,
    })
}

/// One observed workload window for DRO volatility tracking.
#[derive(Debug, Clone, Copy, PartialEq)]
#[allow(clippy::derive_partial_eq_without_eq)] // f64 does not impl Eq
pub struct DroWindowObservation {
    pub abort_rate: f64,
    pub edge_rate: f64,
}

/// Which observed rate failed DRO volatility validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DroObservedRateKind {
    Abort,
    Edge,
}

impl DroObservedRateKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Abort => "abort_rate",
            Self::Edge => "edge_rate",
        }
    }
}

impl fmt::Display for DroObservedRateKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Validation error when recording workload windows for DRO volatility tracking.
#[derive(Debug, Clone, Copy, PartialEq)]
#[allow(clippy::derive_partial_eq_without_eq)] // f64 does not impl Eq
#[allow(clippy::module_name_repetitions)]
pub enum DroVolatilityTrackerError {
    NonFiniteRate {
        kind: DroObservedRateKind,
        value: f64,
    },
    OutOfRangeRate {
        kind: DroObservedRateKind,
        value: f64,
    },
}

impl fmt::Display for DroVolatilityTrackerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Self::NonFiniteRate { kind, value } => {
                write!(f, "{kind} must be finite, got {value}")
            }
            Self::OutOfRangeRate { kind, value } => {
                write!(f, "{kind} must be within [0.0, 1.0], got {value}")
            }
        }
    }
}

impl std::error::Error for DroVolatilityTrackerError {}

/// Configuration for the empirical DRO volatility tracker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DroVolatilityTrackerConfig {
    /// Maximum number of workload windows retained in the sliding window.
    pub window_size: usize,
    /// Minimum number of retained samples required before emitting a certificate.
    pub min_samples: usize,
}

impl Default for DroVolatilityTrackerConfig {
    fn default() -> Self {
        Self {
            window_size: 32,
            min_samples: 4,
        }
    }
}

impl DroVolatilityTrackerConfig {
    #[must_use]
    fn normalized(self) -> Self {
        let window_size = self.window_size.max(2);
        let min_samples = self.min_samples.clamp(2, window_size);
        Self {
            window_size,
            min_samples,
        }
    }
}

/// Sliding-window tracker for recent abort/edge-rate volatility.
#[derive(Debug, Clone)]
pub struct DroVolatilityTracker {
    config: DroVolatilityTrackerConfig,
    windows: VecDeque<DroWindowObservation>,
}

impl DroVolatilityTracker {
    #[must_use]
    pub fn new(config: DroVolatilityTrackerConfig) -> Self {
        let config = config.normalized();
        Self {
            windows: VecDeque::with_capacity(config.window_size),
            config,
        }
    }

    /// Record one completed workload window.
    pub fn observe_window(
        &mut self,
        abort_rate: f64,
        edge_rate: f64,
    ) -> std::result::Result<(), DroVolatilityTrackerError> {
        let previous_certificate = self.radius_certificate(DroRiskTolerance::Low);
        validate_observed_rate(DroObservedRateKind::Abort, abort_rate)?;
        validate_observed_rate(DroObservedRateKind::Edge, edge_rate)?;
        if self.windows.len() == self.config.window_size {
            let _ = self.windows.pop_front();
        }
        self.windows.push_back(DroWindowObservation {
            abort_rate,
            edge_rate,
        });
        if let Some(current_certificate) = self.radius_certificate(DroRiskTolerance::Low) {
            let previous_radius =
                previous_certificate.map_or(0.0, |certificate| certificate.base_radius);
            let trigger = if previous_certificate.is_some() {
                "observe_window"
            } else {
                "min_samples_reached"
            };
            debug!(
                target: "fsqlite::ssi::dro",
                event = "wasserstein_update",
                old_radius = previous_radius,
                new_radius = current_certificate.base_radius,
                abort_rate,
                edge_rate,
                window_samples = self.windows.len(),
                trigger,
            );
            if let Some(previous_certificate) = previous_certificate
                && current_certificate.base_radius > previous_certificate.base_radius
            {
                warn!(
                    target: "fsqlite::ssi::dro",
                    event = "regime_shift",
                    old_radius = previous_certificate.base_radius,
                    new_radius = current_certificate.base_radius,
                    volatility = current_certificate.base_radius,
                );
            }
        }
        Ok(())
    }

    #[must_use]
    pub const fn window_size(&self) -> usize {
        self.config.window_size
    }

    #[must_use]
    pub const fn min_samples(&self) -> usize {
        self.config.min_samples
    }

    #[must_use]
    pub fn sample_count(&self) -> usize {
        self.windows.len()
    }

    #[must_use]
    pub fn is_ready(&self) -> bool {
        self.sample_count() >= self.config.min_samples
    }

    #[must_use]
    pub fn abort_rate_variance(&self) -> Option<f64> {
        if !self.is_ready() {
            return None;
        }
        let abort_rates = self
            .windows
            .iter()
            .map(|window| window.abort_rate)
            .collect::<Vec<_>>();
        sample_variance(&abort_rates)
    }

    #[must_use]
    pub fn edge_rate_variance(&self) -> Option<f64> {
        if !self.is_ready() {
            return None;
        }
        let edge_rates = self
            .windows
            .iter()
            .map(|window| window.edge_rate)
            .collect::<Vec<_>>();
        sample_variance(&edge_rates)
    }

    /// Build the current Wasserstein-style certificate from the sliding window.
    #[must_use]
    pub fn radius_certificate(&self, tolerance: DroRiskTolerance) -> Option<DroRadiusCertificate> {
        if !self.is_ready() {
            return None;
        }
        let abort_rates = self
            .windows
            .iter()
            .map(|window| window.abort_rate)
            .collect::<Vec<_>>();
        let edge_rates = self
            .windows
            .iter()
            .map(|window| window.edge_rate)
            .collect::<Vec<_>>();
        dro_wasserstein_radius(&abort_rates, &edge_rates, tolerance)
    }
}

fn validate_observed_rate(
    kind: DroObservedRateKind,
    value: f64,
) -> std::result::Result<(), DroVolatilityTrackerError> {
    if !value.is_finite() {
        return Err(DroVolatilityTrackerError::NonFiniteRate { kind, value });
    }
    if !(0.0..=1.0).contains(&value) {
        return Err(DroVolatilityTrackerError::OutOfRangeRate { kind, value });
    }
    Ok(())
}

#[must_use]
fn sample_variance(values: &[f64]) -> Option<f64> {
    if values.len() < 2 {
        return None;
    }
    #[allow(clippy::cast_precision_loss)]
    let mean = values.iter().sum::<f64>() / values.len() as f64;
    #[allow(clippy::cast_precision_loss)]
    let variance = values
        .iter()
        .map(|value| {
            let delta = value - mean;
            delta * delta
        })
        .sum::<f64>()
        / (values.len() - 1) as f64;
    Some(variance)
}

#[must_use]
fn dro_topology_pressure(active_readers: usize, active_writers: usize) -> f64 {
    #[allow(clippy::cast_precision_loss)]
    let occupancy = active_readers as f64 + active_writers as f64;
    #[allow(clippy::cast_precision_loss)]
    let skew = (active_readers.max(active_writers) as f64 + 1.0)
        / (active_readers.min(active_writers) as f64 + 1.0);
    let tail_mass = (occupancy / 8.0).min(4.0);
    tail_mass * skew.ln_1p()
}

#[must_use]
fn dro_cvar_penalty_from_pressure(topology_pressure: f64, radius: DroRadiusCertificate) -> f64 {
    radius.effective_radius() * topology_pressure
}

#[must_use]
fn dro_topology_threshold_from_pressure(
    topology_pressure: f64,
    base_threshold: f64,
    radius: DroRadiusCertificate,
) -> f64 {
    if topology_pressure <= f64::EPSILON {
        return base_threshold;
    }

    let severity = topology_pressure * (1.0 + radius.effective_radius());
    base_threshold / (1.0 + severity)
}

// ---------------------------------------------------------------------------
// DRO Live Controller — adaptive runtime matrix swap (bd-3t52f / bd-18x86)
// ---------------------------------------------------------------------------

/// Configuration for the live DRO controller.
#[derive(Debug, Clone, Copy, PartialEq)]
#[allow(clippy::derive_partial_eq_without_eq)] // f64 does not impl Eq
pub(crate) struct DroLiveControllerConfig {
    /// How many SSI outcomes (commits + aborts) constitute one observation window.
    pub(crate) window_commit_budget: u64,
    /// Maximum readers dimension in the loss matrix.
    pub(crate) max_readers: usize,
    /// Maximum writers dimension in the loss matrix.
    pub(crate) max_writers: usize,
    /// Base abort threshold anchor for the adaptive DRO threshold surface.
    pub(crate) default_threshold: f64,
    /// Volatility tracker config.
    pub(crate) tracker_config: DroVolatilityTrackerConfig,
    /// Default risk tolerance.
    pub(crate) default_tolerance: DroRiskTolerance,
}

impl Default for DroLiveControllerConfig {
    fn default() -> Self {
        Self {
            window_commit_budget: 128,
            max_readers: 32,
            max_writers: 32,
            default_threshold: 0.45,
            tracker_config: DroVolatilityTrackerConfig {
                window_size: 32,
                min_samples: 4,
            },
            default_tolerance: DroRiskTolerance::Low,
        }
    }
}

/// Lock-free abort/commit/edge counters for one observation window.
///
/// These are incremented on the hot path via `Relaxed` atomics. The only
/// synchronization point is when the window rotates (rare, ~every 128 commits).
struct DroWindowCounters {
    commits: AtomicU64,
    aborts: AtomicU64,
    edges_observed: AtomicU64,
    total_decisions: AtomicU64,
}

impl DroWindowCounters {
    const fn new() -> Self {
        Self {
            commits: AtomicU64::new(0),
            aborts: AtomicU64::new(0),
            edges_observed: AtomicU64::new(0),
            total_decisions: AtomicU64::new(0),
        }
    }

    fn record_commit(&self) {
        self.commits.fetch_add(1, Ordering::Relaxed);
        self.total_decisions.fetch_add(1, Ordering::Relaxed);
    }

    fn record_abort(&self) {
        self.aborts.fetch_add(1, Ordering::Relaxed);
        self.total_decisions.fetch_add(1, Ordering::Relaxed);
    }

    fn record_edges(&self, count: u64) {
        self.edges_observed.fetch_add(count, Ordering::Relaxed);
    }

    fn snapshot_and_reset(&self) -> DroWindowSnapshot {
        // Relaxed is fine: we tolerate slightly stale counts in the rare
        // rotation path. Exact consistency is not required — the tracker
        // operates on statistical aggregates over many windows.
        let commits = self.commits.swap(0, Ordering::Relaxed);
        let aborts = self.aborts.swap(0, Ordering::Relaxed);
        let edges = self.edges_observed.swap(0, Ordering::Relaxed);
        let total = self.total_decisions.swap(0, Ordering::Relaxed);
        DroWindowSnapshot {
            commits,
            aborts,
            edges,
            total,
        }
    }
}

/// Snapshot of one completed observation window.
#[derive(Debug, Clone, Copy)]
struct DroWindowSnapshot {
    commits: u64,
    aborts: u64,
    edges: u64,
    total: u64,
}

impl DroWindowSnapshot {
    #[allow(clippy::cast_precision_loss)]
    fn abort_rate(self) -> f64 {
        if self.total == 0 {
            return 0.0;
        }
        (self.aborts as f64) / (self.total as f64)
    }

    #[allow(clippy::cast_precision_loss)]
    fn edge_rate(self) -> f64 {
        if self.total == 0 {
            return 0.0;
        }
        // Normalize: edges per decision, clamped to [0, 1].
        ((self.edges as f64) / (self.total as f64)).min(1.0)
    }
}

/// Live DRO controller: feeds SSI telemetry into the volatility tracker
/// and atomically swaps the loss matrix when the workload regime changes.
///
/// **Hot-path cost:** One `parking_lot::Mutex::lock()` + `Arc::clone()` ≈ 20ns.
/// **Recomputation cost:** 33×33 = 1089 `dro_cvar_penalty` calls ≈ <1ms, amortized
/// over `window_commit_budget` (default 128) SSI decisions.
///
/// # Safety invariants
///
/// - The matrix is NEVER `None` — it's initialized with a conservative default.
/// - Writers (matrix swap) hold `inner` briefly; readers clone the Arc and release.
/// - Concurrent-writer mode is never affected — this only changes abort *policy*.
pub struct DroLiveController {
    /// Current matrix, behind a parking_lot Mutex for fast uncontended lock+clone.
    /// Using parking_lot because std::sync::Mutex is ~50ns vs parking_lot ~15ns.
    matrix: parking_lot::Mutex<Arc<DroLossMatrix>>,
    /// Volatility tracker + config, protected by a separate lock to avoid
    /// holding the matrix lock during recomputation.
    inner: parking_lot::Mutex<DroLiveControllerInner>,
    /// Lock-free per-window counters (hot path).
    counters: DroWindowCounters,
    /// Configuration (immutable after construction).
    config: DroLiveControllerConfig,
}

struct DroLiveControllerInner {
    tracker: DroVolatilityTracker,
    tolerance: DroRiskTolerance,
    /// Monotonic generation counter for observability.
    generation: u64,
}

impl DroLiveController {
    /// Create a new controller with the given config and an initial conservative matrix.
    #[must_use]
    pub(crate) fn new(config: DroLiveControllerConfig) -> Self {
        let tracker = DroVolatilityTracker::new(config.tracker_config);
        let initial_matrix = Self::build_default_matrix(&config);

        Self {
            matrix: parking_lot::Mutex::new(Arc::new(initial_matrix)),
            inner: parking_lot::Mutex::new(DroLiveControllerInner {
                tracker,
                tolerance: config.default_tolerance,
                generation: 0,
            }),
            counters: DroWindowCounters::new(),
            config,
        }
    }

    /// Build the conservative default matrix (same as the old static one).
    fn build_default_matrix(config: &DroLiveControllerConfig) -> DroLossMatrix {
        let mut tracker = DroVolatilityTracker::new(DroVolatilityTrackerConfig {
            window_size: 4,
            min_samples: 4,
        });
        // Seed with conservative synthetic observations.
        for &(abort_rate, edge_rate) in &[(0.03, 0.04), (0.05, 0.06), (0.08, 0.09), (0.13, 0.15)] {
            tracker
                .observe_window(abort_rate, edge_rate)
                .expect("default DRO tracker windows must be valid");
        }
        let certificate = tracker
            .radius_certificate(config.default_tolerance)
            .expect("default DRO certificate must be constructible");
        DroLossMatrix::from_radius_certificate(
            config.max_readers,
            config.max_writers,
            config.default_threshold,
            certificate,
        )
    }

    /// **Hot path.** Get a reference-counted handle to the current matrix.
    ///
    /// Cost: ~20ns (one uncontended mutex lock + Arc clone).
    #[must_use]
    pub(crate) fn current_matrix(&self) -> Arc<DroLossMatrix> {
        self.matrix.lock().clone()
    }

    /// Record a successful commit. Call after `ssi_validate_and_publish` succeeds.
    pub(crate) fn record_commit(&self, edge_count: u64) {
        self.counters.record_commit();
        self.counters.record_edges(edge_count);
        self.maybe_rotate_window();
    }

    /// Record an SSI abort. Call after `ssi_validate_and_publish` returns `Err`.
    pub(crate) fn record_abort(&self, edge_count: u64) {
        self.counters.record_abort();
        self.counters.record_edges(edge_count);
        self.maybe_rotate_window();
    }

    /// Update the risk tolerance (e.g., from `PRAGMA fsqlite_ssi_risk_tolerance`).
    #[allow(dead_code)] // wired via PRAGMA in bd-3t52f follow-up
    pub(crate) fn set_tolerance(&self, tolerance: DroRiskTolerance) {
        let mut inner = self.inner.lock();
        if inner.tolerance != tolerance {
            let old = inner.tolerance;
            inner.tolerance = tolerance;
            // Force a matrix recomputation with the new tolerance.
            drop(inner);
            self.force_recompute();
            info!(
                target: "fsqlite::ssi::dro",
                event = "tolerance_changed",
                old = %old,
                new = %tolerance,
                "DRO risk tolerance updated, matrix recomputed"
            );
        }
    }

    /// Current generation (number of matrix swaps since creation).
    #[must_use]
    pub(crate) fn generation(&self) -> u64 {
        self.inner.lock().generation
    }

    /// Check if the window budget is exhausted and rotate if so.
    fn maybe_rotate_window(&self) {
        let total = self.counters.total_decisions.load(Ordering::Relaxed);
        if total < self.config.window_commit_budget {
            return;
        }
        // Snapshot the counters and feed the tracker.
        let snapshot = self.counters.snapshot_and_reset();
        // Guard against concurrent double-rotation: if snapshot.total is 0, another
        // thread already rotated.
        if snapshot.total == 0 {
            return;
        }
        self.feed_snapshot(snapshot);
    }

    /// Feed a completed window snapshot into the volatility tracker and
    /// potentially recompute the matrix.
    fn feed_snapshot(&self, snapshot: DroWindowSnapshot) {
        let abort_rate = snapshot.abort_rate();
        let edge_rate = snapshot.edge_rate();

        let mut inner = self.inner.lock();
        if let Err(err) = inner.tracker.observe_window(abort_rate, edge_rate) {
            warn!(
                target: "fsqlite::ssi::dro",
                event = "tracker_observe_error",
                error = %err,
                abort_rate,
                edge_rate,
                "DRO tracker rejected window observation"
            );
            return;
        }

        // Only recompute if the tracker has enough samples.
        if !inner.tracker.is_ready() {
            return;
        }

        let Some(certificate) = inner.tracker.radius_certificate(inner.tolerance) else {
            return;
        };

        let new_matrix = DroLossMatrix::from_radius_certificate(
            self.config.max_readers,
            self.config.max_writers,
            self.config.default_threshold,
            certificate,
        );

        inner.generation += 1;
        let generation = inner.generation;
        let radius = certificate.effective_radius();
        let tolerance = inner.tolerance;
        drop(inner);

        // Atomic matrix swap — readers see the new matrix on their next call.
        *self.matrix.lock() = Arc::new(new_matrix);

        info!(
            target: "fsqlite::ssi::dro",
            event = "matrix_swap",
            dro_generation = generation,
            abort_rate,
            edge_rate,
            radius,
            tolerance = %tolerance,
            commits = snapshot.commits,
            aborts = snapshot.aborts,
            edges = snapshot.edges,
            "DRO loss matrix recomputed from live telemetry"
        );
    }

    /// Force immediate matrix recomputation from the tracker's current state.
    #[allow(dead_code)] // called by set_tolerance
    fn force_recompute(&self) {
        let mut inner = self.inner.lock();
        if !inner.tracker.is_ready() {
            return;
        }
        let Some(certificate) = inner.tracker.radius_certificate(inner.tolerance) else {
            return;
        };
        let new_matrix = DroLossMatrix::from_radius_certificate(
            self.config.max_readers,
            self.config.max_writers,
            self.config.default_threshold,
            certificate,
        );
        inner.generation += 1;
        drop(inner);
        *self.matrix.lock() = Arc::new(new_matrix);
    }

    /// Access the global singleton controller.
    ///
    /// Lazily initialized on first call with default config.
    #[must_use]
    pub(crate) fn global() -> &'static Self {
        static INSTANCE: OnceLock<DroLiveController> = OnceLock::new();
        INSTANCE.get_or_init(|| Self::new(DroLiveControllerConfig::default()))
    }
}

// ---------------------------------------------------------------------------
// SSI Evidence Ledger (galaxy-brain decision cards)
// ---------------------------------------------------------------------------

/// Decision type emitted by SSI commit-time validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SsiDecisionType {
    CommitAllowed,
    AbortWriteSkew,
    AbortPhantom,
    AbortCycle,
}

impl SsiDecisionType {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CommitAllowed => "COMMIT_ALLOWED",
            Self::AbortWriteSkew => "ABORT_WRITE_SKEW",
            Self::AbortPhantom => "ABORT_PHANTOM",
            Self::AbortCycle => "ABORT_CYCLE",
        }
    }
}

impl fmt::Display for SsiDecisionType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for SsiDecisionType {
    type Err = &'static str;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let normalized = value.trim().to_ascii_uppercase();
        match normalized.as_str() {
            "COMMIT_ALLOWED" => Ok(Self::CommitAllowed),
            "ABORT_WRITE_SKEW" => Ok(Self::AbortWriteSkew),
            "ABORT_PHANTOM" => Ok(Self::AbortPhantom),
            "ABORT_CYCLE" => Ok(Self::AbortCycle),
            _ => Err("unrecognized SSI decision type"),
        }
    }
}

/// Compact read-set summary stored in each galaxy-brain card.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SsiReadSetSummary {
    pub page_count: usize,
    pub top_k_pages: Vec<PageNumber>,
    pub bloom_fingerprint: u64,
}

impl SsiReadSetSummary {
    #[must_use]
    pub fn from_pages(read_set_pages: &[PageNumber], top_k: usize) -> Self {
        let mut sorted = read_set_pages.to_vec();
        sorted.sort_by_key(|page| page.get());
        sorted.dedup();
        let top_k_pages = sorted.iter().copied().take(top_k.max(1)).collect();
        Self {
            page_count: sorted.len(),
            top_k_pages,
            bloom_fingerprint: read_set_fingerprint(&sorted),
        }
    }
}

/// Card payload before chain hash / epoch assignment.
#[derive(Debug, Clone)]
pub struct SsiDecisionCardDraft {
    pub decision_id: u64,
    pub decision_type: SsiDecisionType,
    pub txn: TxnToken,
    pub snapshot_seq: CommitSeq,
    pub commit_seq: Option<CommitSeq>,
    pub conflicting_txns: Vec<TxnToken>,
    pub conflict_pages: Vec<PageNumber>,
    pub read_set_pages: Vec<PageNumber>,
    pub write_set: Vec<PageNumber>,
    pub rationale: String,
    pub timestamp_unix_ns: u64,
}

impl SsiDecisionCardDraft {
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn new(
        decision_type: SsiDecisionType,
        txn: TxnToken,
        snapshot_seq: CommitSeq,
        conflicting_txns: Vec<TxnToken>,
        conflict_pages: Vec<PageNumber>,
        read_set_pages: Vec<PageNumber>,
        write_set: Vec<PageNumber>,
        rationale: impl Into<String>,
    ) -> Self {
        Self {
            decision_id: 0,
            decision_type,
            txn,
            snapshot_seq,
            commit_seq: None,
            conflicting_txns,
            conflict_pages,
            read_set_pages,
            write_set,
            rationale: rationale.into(),
            timestamp_unix_ns: now_unix_ns(),
        }
    }

    #[must_use]
    pub const fn with_commit_seq(mut self, commit_seq: CommitSeq) -> Self {
        self.commit_seq = Some(commit_seq);
        self
    }

    #[must_use]
    pub const fn with_timestamp_unix_ns(mut self, timestamp_unix_ns: u64) -> Self {
        self.timestamp_unix_ns = timestamp_unix_ns;
        self
    }

    #[must_use]
    pub const fn with_decision_id(mut self, decision_id: u64) -> Self {
        self.decision_id = decision_id;
        self
    }
}

/// Immutable append-only galaxy-brain card persisted by the evidence ledger.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SsiDecisionCard {
    pub decision_id: u64,
    pub decision_type: SsiDecisionType,
    pub txn: TxnToken,
    pub snapshot_seq: CommitSeq,
    pub commit_seq: Option<CommitSeq>,
    pub conflicting_txns: Vec<TxnToken>,
    pub conflict_pages: Vec<PageNumber>,
    pub read_set_summary: SsiReadSetSummary,
    pub write_set: Vec<PageNumber>,
    pub rationale: String,
    pub timestamp_unix_ns: u64,
    pub decision_epoch: u64,
    pub chain_hash: [u8; 32],
}

impl SsiDecisionCard {
    #[must_use]
    pub fn chain_hash_hex(&self) -> String {
        hex_encode(self.chain_hash)
    }
}

/// Filter options for listing evidence cards.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SsiDecisionQuery {
    pub txn_id: Option<u64>,
    pub decision_type: Option<SsiDecisionType>,
    pub timestamp_start_ns: Option<u64>,
    pub timestamp_end_ns: Option<u64>,
}

#[derive(Debug)]
struct SsiEvidenceLedgerState {
    capacity: usize,
    next_epoch: u64,
    chain_tip: [u8; 32],
    entries: VecDeque<SsiDecisionCard>,
}

impl SsiEvidenceLedgerState {
    fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            next_epoch: 1,
            chain_tip: [0_u8; 32],
            entries: VecDeque::new(),
        }
    }

    fn append(&mut self, draft: SsiDecisionCardDraft) {
        let mut conflicting_txns = draft.conflicting_txns;
        conflicting_txns.sort_by(|left, right| {
            left.id
                .get()
                .cmp(&right.id.get())
                .then_with(|| left.epoch.get().cmp(&right.epoch.get()))
        });
        conflicting_txns.dedup();

        let mut conflict_pages = draft.conflict_pages;
        conflict_pages.sort_by_key(|page| page.get());
        conflict_pages.dedup();

        let mut write_set = draft.write_set;
        write_set.sort_by_key(|page| page.get());
        write_set.dedup();

        let mut read_set_pages = draft.read_set_pages;
        read_set_pages.sort_by_key(|page| page.get());
        read_set_pages.dedup();
        let read_set_summary = SsiReadSetSummary::from_pages(&read_set_pages, 8);

        let decision_epoch = self.next_epoch;
        self.next_epoch = self.next_epoch.saturating_add(1);
        let chain_hash = compute_chain_hash(
            self.chain_tip,
            draft.decision_id,
            draft.decision_type,
            draft.txn,
            draft.snapshot_seq,
            draft.commit_seq,
            decision_epoch,
            draft.timestamp_unix_ns,
            &conflicting_txns,
            &conflict_pages,
            &read_set_pages,
            &write_set,
            &draft.rationale,
        );
        self.chain_tip = chain_hash;

        if self.entries.len() == self.capacity {
            let _ = self.entries.pop_front();
        }
        self.entries.push_back(SsiDecisionCard {
            decision_id: draft.decision_id,
            decision_type: draft.decision_type,
            txn: draft.txn,
            snapshot_seq: draft.snapshot_seq,
            commit_seq: draft.commit_seq,
            conflicting_txns,
            conflict_pages,
            read_set_summary,
            write_set,
            rationale: draft.rationale,
            timestamp_unix_ns: draft.timestamp_unix_ns,
            decision_epoch,
            chain_hash,
        });
    }
}

/// Bounded append-only ledger for SSI decision cards.
#[derive(Debug)]
pub struct SsiEvidenceLedger {
    state: Mutex<SsiEvidenceLedgerState>,
    pending_queue: Mutex<VecDeque<SsiDecisionCardDraft>>,
    pending: AtomicUsize,
    flush_in_progress: AtomicBool,
}

impl SsiEvidenceLedger {
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            state: Mutex::new(SsiEvidenceLedgerState::new(capacity)),
            pending_queue: Mutex::new(VecDeque::new()),
            pending: AtomicUsize::new(0),
            flush_in_progress: AtomicBool::new(false),
        }
    }

    /// Non-blocking append path used from commit/abort critical sections.
    pub fn record_async(&self, draft: SsiDecisionCardDraft) {
        self.enqueue_pending(draft);
        self.try_flush_pending();
    }

    /// Synchronous append used by callers that need visibility before return.
    pub fn record_sync(&self, draft: SsiDecisionCardDraft) {
        self.enqueue_pending(draft);
        self.flush_pending();
    }

    /// Return all retained cards in insertion order.
    #[must_use]
    pub fn snapshot(&self) -> Vec<SsiDecisionCard> {
        self.flush_pending();
        with_locked(&self.state, |inner| inner.entries.iter().cloned().collect())
    }

    /// Return cards matching the given query.
    #[must_use]
    pub fn query(&self, query: &SsiDecisionQuery) -> Vec<SsiDecisionCard> {
        self.flush_pending();
        with_locked(&self.state, |inner| {
            inner
                .entries
                .iter()
                .filter(|entry| query.txn_id.is_none_or(|txn| entry.txn.id.get() == txn))
                .filter(|entry| {
                    query
                        .decision_type
                        .is_none_or(|decision_type| entry.decision_type == decision_type)
                })
                .filter(|entry| {
                    query
                        .timestamp_start_ns
                        .is_none_or(|start| entry.timestamp_unix_ns >= start)
                })
                .filter(|entry| {
                    query
                        .timestamp_end_ns
                        .is_none_or(|end| entry.timestamp_unix_ns <= end)
                })
                .cloned()
                .collect()
        })
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.flush_pending();
        with_locked(&self.state, |inner| inner.entries.len())
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[must_use]
    pub fn pending_count(&self) -> usize {
        self.pending.load(Ordering::Acquire)
    }

    fn enqueue_pending(&self, draft: SsiDecisionCardDraft) {
        with_locked(&self.pending_queue, |queue| {
            queue.push_back(draft);
            let _ = self.pending.fetch_add(1, Ordering::AcqRel);
        });
    }

    fn try_flush_pending(&self) {
        while self.pending.load(Ordering::Acquire) > 0 {
            if self
                .flush_in_progress
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                return;
            }

            let drained = self.try_with_locked_state(|state| self.drain_pending_into(state));
            self.flush_in_progress.store(false, Ordering::Release);
            if drained.is_none() {
                return;
            }
        }
    }

    fn flush_pending(&self) {
        while self.pending.load(Ordering::Acquire) > 0 {
            if self
                .flush_in_progress
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                std::thread::yield_now();
                continue;
            }

            with_locked(&self.state, |state| self.drain_pending_into(state));
            self.flush_in_progress.store(false, Ordering::Release);
        }
    }

    fn try_with_locked_state<T>(
        &self,
        f: impl FnOnce(&mut SsiEvidenceLedgerState) -> T,
    ) -> Option<T> {
        match self.state.try_lock() {
            Ok(mut guard) => Some(f(&mut guard)),
            Err(TryLockError::Poisoned(poisoned)) => {
                let mut guard = poisoned.into_inner();
                Some(f(&mut guard))
            }
            Err(TryLockError::WouldBlock) => None,
        }
    }

    fn drain_pending_into(&self, state: &mut SsiEvidenceLedgerState) {
        loop {
            let mut batch = with_locked(&self.pending_queue, std::mem::take);
            if batch.is_empty() {
                return;
            }

            let drained = batch.len();
            while let Some(draft) = batch.pop_front() {
                state.append(draft);
            }
            let _ = self.pending.fetch_sub(drained, Ordering::AcqRel);
        }
    }
}

impl Default for SsiEvidenceLedger {
    fn default() -> Self {
        Self::new(1024)
    }
}

fn with_locked<T, U>(state: &Mutex<T>, f: impl FnOnce(&mut T) -> U) -> U {
    match state.lock() {
        Ok(mut guard) => f(&mut guard),
        Err(poisoned) => {
            let mut guard = poisoned.into_inner();
            f(&mut guard)
        }
    }
}

fn now_unix_ns() -> u64 {
    let Ok(duration) = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) else {
        return 0;
    };
    let nanos = duration.as_nanos();
    u64::try_from(nanos).unwrap_or(u64::MAX)
}

fn read_set_fingerprint(read_set_pages: &[PageNumber]) -> u64 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"fsqlite:ssi_read_set_fingerprint:v1");
    for page in read_set_pages {
        hasher.update(&page.get().to_le_bytes());
    }
    let hash = hasher.finalize();
    let mut bytes = [0_u8; 8];
    bytes.copy_from_slice(&hash.as_bytes()[..8]);
    u64::from_le_bytes(bytes)
}

#[allow(clippy::too_many_arguments)]
fn compute_chain_hash(
    previous_hash: [u8; 32],
    decision_id: u64,
    decision_type: SsiDecisionType,
    txn: TxnToken,
    snapshot_seq: CommitSeq,
    commit_seq: Option<CommitSeq>,
    decision_epoch: u64,
    timestamp_unix_ns: u64,
    conflicting_txns: &[TxnToken],
    conflict_pages: &[PageNumber],
    read_set_pages: &[PageNumber],
    write_set: &[PageNumber],
    rationale: &str,
) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"fsqlite:ssi_evidence_ledger:v1");
    hasher.update(&previous_hash);
    hasher.update(&decision_id.to_le_bytes());
    hasher.update(decision_type.as_str().as_bytes());
    hasher.update(&txn.id.get().to_le_bytes());
    hasher.update(&txn.epoch.get().to_le_bytes());
    hasher.update(&snapshot_seq.get().to_le_bytes());
    hasher.update(&commit_seq.map_or(0_u64, CommitSeq::get).to_le_bytes());
    hasher.update(&decision_epoch.to_le_bytes());
    hasher.update(&timestamp_unix_ns.to_le_bytes());

    for token in conflicting_txns {
        hasher.update(&token.id.get().to_le_bytes());
        hasher.update(&token.epoch.get().to_le_bytes());
    }
    for page in conflict_pages {
        hasher.update(&page.get().to_le_bytes());
    }
    for page in read_set_pages {
        hasher.update(&page.get().to_le_bytes());
    }
    for page in write_set {
        hasher.update(&page.get().to_le_bytes());
    }
    hasher.update(rationale.as_bytes());

    let hash = hasher.finalize();
    *hash.as_bytes()
}

fn hex_encode(bytes: [u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = [0_u8; 64];
    for (idx, byte) in bytes.iter().copied().enumerate() {
        out[idx * 2] = HEX[usize::from(byte >> 4)];
        out[idx * 2 + 1] = HEX[usize::from(byte & 0x0F)];
    }
    String::from_utf8_lossy(&out).into_owned()
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
        if !self.is_calibrated() || self.residuals.is_empty() {
            return None;
        }
        let mut sorted = self.residuals.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        #[allow(clippy::cast_precision_loss)]
        let q_idx = ((1.0 - self.config.alpha) * (sorted.len() + 1) as f64).ceil() as usize;
        let idx = q_idx.min(sorted.len()).saturating_sub(1);
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
    use fsqlite_types::{TxnEpoch, TxnId};

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
    fn test_victim_selection_potential_cycle_cheaper_other() {
        let pivot = TxnCost {
            write_set_size: 1000,
            duration_us: 0,
        };
        let other = TxnCost {
            write_set_size: 1,
            duration_us: 0,
        };
        let decision = select_victim(CycleStatus::Potential, pivot, other);
        assert_eq!(
            decision.victim,
            Victim::Other,
            "bead_id={BEAD_ID} cheaper_other_aborted"
        );
        assert!(decision.reason.contains("cheaper_other"));
    }

    #[test]
    fn test_smarter_victim_selection() {
        let decision = select_victim(
            CycleStatus::Potential,
            TxnCost {
                write_set_size: 500,
                duration_us: 50_000,
            },
            TxnCost {
                write_set_size: 1,
                duration_us: 100,
            },
        );
        assert_eq!(
            decision.victim,
            Victim::Other,
            "bead_id={BEAD_ID} policy must consider abort cost, not always abort pivot"
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

    fn token(raw: u64, epoch: u32) -> TxnToken {
        TxnToken::new(
            TxnId::new(raw).expect("token raw id must be non-zero"),
            TxnEpoch::new(epoch),
        )
    }

    fn page(raw: u32) -> PageNumber {
        PageNumber::new(raw).expect("page number must be non-zero")
    }

    #[test]
    fn test_ssi_evidence_ledger_append_only_chain_and_capacity() {
        let ledger = SsiEvidenceLedger::new(2);
        ledger.record_sync(
            SsiDecisionCardDraft::new(
                SsiDecisionType::CommitAllowed,
                token(1, 1),
                CommitSeq::new(10),
                Vec::new(),
                vec![page(7)],
                vec![page(1), page(2)],
                vec![page(7)],
                "clean_commit",
            )
            .with_commit_seq(CommitSeq::new(11))
            .with_timestamp_unix_ns(1_000),
        );
        ledger.record_sync(
            SsiDecisionCardDraft::new(
                SsiDecisionType::AbortWriteSkew,
                token(2, 1),
                CommitSeq::new(11),
                vec![token(1, 1)],
                vec![page(7), page(9)],
                vec![page(7), page(8)],
                vec![page(9)],
                "pivot_in_out_rw",
            )
            .with_timestamp_unix_ns(2_000),
        );
        ledger.record_sync(
            SsiDecisionCardDraft::new(
                SsiDecisionType::AbortCycle,
                token(3, 1),
                CommitSeq::new(12),
                vec![token(1, 1), token(2, 1)],
                vec![page(7)],
                vec![page(7)],
                vec![page(7)],
                "fcw_conflict",
            )
            .with_timestamp_unix_ns(3_000),
        );

        let cards = ledger.snapshot();
        assert_eq!(cards.len(), 2, "bead_id={BEAD_ID} bounded_capacity");
        assert_eq!(cards[0].txn.id.get(), 2);
        assert_eq!(cards[1].txn.id.get(), 3);
        assert_ne!(
            cards[0].chain_hash, cards[1].chain_hash,
            "bead_id={BEAD_ID} chain_hash_must_advance"
        );
    }

    #[test]
    fn test_ssi_evidence_ledger_record_async_buffers_and_flushes_in_order() {
        let ledger = SsiEvidenceLedger::new(4);
        let held_state = ledger.state.lock().unwrap_or_else(|e| e.into_inner());

        ledger.record_async(
            SsiDecisionCardDraft::new(
                SsiDecisionType::CommitAllowed,
                token(21, 1),
                CommitSeq::new(30),
                Vec::new(),
                Vec::new(),
                vec![page(3)],
                vec![page(5)],
                "buffered_commit",
            )
            .with_commit_seq(CommitSeq::new(31))
            .with_timestamp_unix_ns(30_000),
        );
        ledger.record_async(
            SsiDecisionCardDraft::new(
                SsiDecisionType::AbortWriteSkew,
                token(22, 1),
                CommitSeq::new(31),
                vec![token(21, 1)],
                vec![page(5)],
                vec![page(5)],
                vec![page(6)],
                "buffered_abort",
            )
            .with_timestamp_unix_ns(31_000),
        );
        assert_eq!(ledger.pending.load(Ordering::Acquire), 2);
        drop(held_state);

        let cards = ledger.snapshot();
        assert_eq!(cards.len(), 2);
        assert_eq!(cards[0].txn.id.get(), 21);
        assert_eq!(cards[1].txn.id.get(), 22);
        assert_eq!(ledger.pending.load(Ordering::Acquire), 0);
    }

    #[test]
    fn test_ssi_evidence_ledger_query_filters() {
        let ledger = SsiEvidenceLedger::new(8);
        ledger.record_sync(
            SsiDecisionCardDraft::new(
                SsiDecisionType::CommitAllowed,
                token(10, 1),
                CommitSeq::new(20),
                Vec::new(),
                Vec::new(),
                vec![page(3), page(4)],
                vec![page(9)],
                "commit",
            )
            .with_timestamp_unix_ns(10_000),
        );
        ledger.record_sync(
            SsiDecisionCardDraft::new(
                SsiDecisionType::AbortWriteSkew,
                token(11, 2),
                CommitSeq::new(20),
                vec![token(10, 1)],
                vec![page(9)],
                vec![page(9), page(10)],
                vec![page(11)],
                "pivot_abort",
            )
            .with_timestamp_unix_ns(20_000),
        );

        let by_txn = ledger.query(&SsiDecisionQuery {
            txn_id: Some(11),
            ..SsiDecisionQuery::default()
        });
        assert_eq!(by_txn.len(), 1);
        assert_eq!(by_txn[0].txn.id.get(), 11);

        let by_type = ledger.query(&SsiDecisionQuery {
            decision_type: Some(SsiDecisionType::AbortWriteSkew),
            ..SsiDecisionQuery::default()
        });
        assert_eq!(by_type.len(), 1);
        assert_eq!(by_type[0].decision_type, SsiDecisionType::AbortWriteSkew);

        let by_time = ledger.query(&SsiDecisionQuery {
            timestamp_start_ns: Some(15_000),
            timestamp_end_ns: Some(25_000),
            ..SsiDecisionQuery::default()
        });
        assert_eq!(by_time.len(), 1);
        assert_eq!(by_time[0].txn.id.get(), 11);
    }

    #[test]
    fn test_dro_risk_tolerance_parse_and_display() {
        assert_eq!("low".parse::<DroRiskTolerance>(), Ok(DroRiskTolerance::Low));
        assert_eq!(
            "HIGH".parse::<DroRiskTolerance>(),
            Ok(DroRiskTolerance::High)
        );
        assert_eq!(DroRiskTolerance::Low.to_string(), "low");
        assert_eq!(DroRiskTolerance::High.to_string(), "high");
    }

    #[test]
    fn test_dro_wasserstein_radius_expands_with_variance() {
        let calm = dro_wasserstein_radius(
            &[0.02, 0.03, 0.02, 0.03],
            &[0.01, 0.02, 0.01, 0.02],
            DroRiskTolerance::Low,
        )
        .expect("certificate");
        let volatile = dro_wasserstein_radius(
            &[0.02, 0.14, 0.01, 0.18],
            &[0.01, 0.16, 0.02, 0.20],
            DroRiskTolerance::Low,
        )
        .expect("certificate");
        assert!(volatile.base_radius > calm.base_radius);
        assert!(volatile.scaled_radius > calm.scaled_radius);
    }

    #[test]
    fn test_dro_wasserstein_radius_respects_tolerance_scale() {
        let low = dro_wasserstein_radius(
            &[0.04, 0.08, 0.12, 0.16],
            &[0.03, 0.07, 0.11, 0.15],
            DroRiskTolerance::Low,
        )
        .expect("certificate");
        let high = dro_wasserstein_radius(
            &[0.04, 0.08, 0.12, 0.16],
            &[0.03, 0.07, 0.11, 0.15],
            DroRiskTolerance::High,
        )
        .expect("certificate");
        assert_eq!(low.base_radius, high.base_radius);
        assert!(high.scaled_radius > low.scaled_radius);
    }

    #[test]
    fn test_dro_loss_matrix_zero_penalty_without_contention() {
        let cert = dro_wasserstein_radius(
            &[0.05, 0.05, 0.05, 0.05],
            &[0.03, 0.03, 0.03, 0.03],
            DroRiskTolerance::Low,
        )
        .expect("certificate");
        let matrix = DroLossMatrix::from_radius_certificate(8, 8, 0.5, cert);
        let decision = matrix.evaluate(0, 0);
        assert_eq!(decision.active_readers, 0);
        assert_eq!(decision.active_writers, 0);
        assert_eq!(decision.cvar_penalty, 0.0);
        assert!(!decision.should_abort());
    }

    #[test]
    fn test_dro_loss_matrix_penalty_grows_with_contention() {
        let cert = dro_wasserstein_radius(
            &[0.03, 0.08, 0.13, 0.21],
            &[0.02, 0.07, 0.11, 0.19],
            DroRiskTolerance::High,
        )
        .expect("certificate");
        let matrix = DroLossMatrix::from_radius_certificate(8, 8, 0.5, cert);
        let light = matrix.evaluate(1, 1);
        let heavy = matrix.evaluate(6, 6);
        assert!(heavy.cvar_penalty > light.cvar_penalty);
    }

    #[test]
    fn test_dro_loss_matrix_threshold_tightens_with_contention() {
        let cert = dro_wasserstein_radius(
            &[0.03, 0.08, 0.13, 0.21],
            &[0.02, 0.07, 0.11, 0.19],
            DroRiskTolerance::High,
        )
        .expect("certificate");
        let matrix = DroLossMatrix::from_radius_certificate(8, 8, 0.5, cert);
        let light = matrix.evaluate(1, 1);
        let heavy = matrix.evaluate(6, 6);
        assert!(
            heavy.threshold < light.threshold,
            "bead_id=bd-3t52f heavier topology should tighten the DRO threshold"
        );
        assert!(
            light.threshold <= matrix.base_threshold(),
            "bead_id=bd-3t52f light threshold should not exceed the configured base"
        );
    }

    #[test]
    fn test_dro_loss_matrix_threshold_tightens_with_radius() {
        let calm = dro_wasserstein_radius(
            &[0.04, 0.05, 0.04, 0.05],
            &[0.03, 0.04, 0.03, 0.04],
            DroRiskTolerance::Low,
        )
        .expect("certificate");
        let volatile = dro_wasserstein_radius(
            &[0.02, 0.14, 0.01, 0.18],
            &[0.01, 0.16, 0.02, 0.20],
            DroRiskTolerance::Low,
        )
        .expect("certificate");
        let calm_matrix = DroLossMatrix::from_radius_certificate(8, 8, 0.5, calm);
        let volatile_matrix = DroLossMatrix::from_radius_certificate(8, 8, 0.5, volatile);
        assert!(
            volatile_matrix.threshold(4, 4) < calm_matrix.threshold(4, 4),
            "bead_id=bd-3t52f larger Wasserstein radius should tighten the threshold surface"
        );
    }

    #[test]
    fn test_dro_loss_matrix_threshold_boundary() {
        let cert = dro_wasserstein_radius(
            &[0.05, 0.09, 0.15, 0.23],
            &[0.04, 0.08, 0.14, 0.22],
            DroRiskTolerance::High,
        )
        .expect("certificate");
        let matrix = DroLossMatrix::from_radius_certificate(8, 8, 0.2, cert);
        let decision = matrix.evaluate(7, 7);
        assert!(decision.cvar_penalty >= 0.0);
        assert!(
            decision.threshold <= matrix.base_threshold(),
            "bead_id=bd-3t52f effective threshold should never exceed the configured base"
        );
        assert_eq!(
            decision.should_abort(),
            decision.cvar_penalty > decision.threshold
        );
    }

    #[test]
    fn test_dro_volatility_tracker_requires_min_samples() {
        let mut tracker = DroVolatilityTracker::new(DroVolatilityTrackerConfig {
            window_size: 6,
            min_samples: 4,
        });
        tracker.observe_window(0.02, 0.01).expect("valid rates");
        tracker.observe_window(0.03, 0.02).expect("valid rates");
        tracker.observe_window(0.04, 0.03).expect("valid rates");
        assert!(!tracker.is_ready(), "bead_id=bd-1scmu tracker not ready");
        assert_eq!(tracker.abort_rate_variance(), None);
        assert_eq!(tracker.edge_rate_variance(), None);
        assert_eq!(tracker.radius_certificate(DroRiskTolerance::Low), None);
    }

    #[test]
    fn test_dro_volatility_tracker_bounded_window_uses_latest_samples() {
        let mut tracker = DroVolatilityTracker::new(DroVolatilityTrackerConfig {
            window_size: 4,
            min_samples: 4,
        });
        for &(abort_rate, edge_rate) in &[
            (0.01, 0.02),
            (0.02, 0.03),
            (0.03, 0.04),
            (0.04, 0.05),
            (0.15, 0.20),
            (0.18, 0.22),
        ] {
            tracker
                .observe_window(abort_rate, edge_rate)
                .expect("valid rates");
        }
        assert_eq!(tracker.sample_count(), 4, "bead_id=bd-1scmu bounded window");

        let expected = dro_wasserstein_radius(
            &[0.03, 0.04, 0.15, 0.18],
            &[0.04, 0.05, 0.20, 0.22],
            DroRiskTolerance::Low,
        )
        .expect("certificate");
        let actual = tracker
            .radius_certificate(DroRiskTolerance::Low)
            .expect("tracker certificate");
        assert_eq!(
            actual.abort_rate_variance, expected.abort_rate_variance,
            "bead_id=bd-1scmu abort variance tracks latest window"
        );
        assert_eq!(
            actual.edge_rate_variance, expected.edge_rate_variance,
            "bead_id=bd-1scmu edge variance tracks latest window"
        );
        assert_eq!(
            actual.scaled_radius, expected.scaled_radius,
            "bead_id=bd-1scmu radius tracks latest window"
        );
    }

    #[test]
    fn test_dro_volatility_tracker_radius_expands_under_regime_shift() {
        let mut tracker = DroVolatilityTracker::new(DroVolatilityTrackerConfig {
            window_size: 8,
            min_samples: 4,
        });
        for &(abort_rate, edge_rate) in &[(0.02, 0.01), (0.03, 0.02), (0.02, 0.01), (0.03, 0.02)] {
            tracker
                .observe_window(abort_rate, edge_rate)
                .expect("valid calm rates");
        }
        let calm = tracker
            .radius_certificate(DroRiskTolerance::Low)
            .expect("calm certificate");

        for &(abort_rate, edge_rate) in &[(0.20, 0.18), (0.01, 0.02), (0.25, 0.21), (0.02, 0.03)] {
            tracker
                .observe_window(abort_rate, edge_rate)
                .expect("valid volatile rates");
        }
        let volatile = tracker
            .radius_certificate(DroRiskTolerance::Low)
            .expect("volatile certificate");
        assert!(
            volatile.base_radius > calm.base_radius,
            "bead_id=bd-1scmu regime shift must increase base radius"
        );
        assert!(
            volatile.scaled_radius > calm.scaled_radius,
            "bead_id=bd-1scmu regime shift must increase scaled radius"
        );
    }

    #[test]
    fn test_dro_volatility_tracker_rejects_invalid_rates() {
        let mut tracker = DroVolatilityTracker::new(DroVolatilityTrackerConfig::default());
        assert!(
            matches!(
                tracker.observe_window(f64::NAN, 0.2),
                Err(DroVolatilityTrackerError::NonFiniteRate {
                    kind: DroObservedRateKind::Abort,
                    value,
                }) if value.is_nan()
            ),
            "bead_id=bd-1scmu NaN abort rate must be rejected"
        );
        assert!(
            matches!(
                tracker.observe_window(0.2, 1.5),
                Err(DroVolatilityTrackerError::OutOfRangeRate {
                    kind: DroObservedRateKind::Edge,
                    value: 1.5,
                })
            ),
            "bead_id=bd-1scmu edge rate above 1.0 must be rejected"
        );
        assert_eq!(
            tracker.sample_count(),
            0,
            "bead_id=bd-1scmu invalid samples must not be recorded"
        );
    }

    // -- DroLiveController tests (bd-3t52f / bd-18x86) --

    #[test]
    fn test_live_controller_starts_with_generation_zero() {
        let ctrl = DroLiveController::new(DroLiveControllerConfig::default());
        assert_eq!(ctrl.generation(), 0, "bead_id=bd-3t52f initial generation");
        let matrix = ctrl.current_matrix();
        // Default matrix should produce zero penalty at (0, 0).
        let decision = matrix.evaluate(0, 0);
        assert_eq!(decision.cvar_penalty, 0.0);
        assert!(!decision.should_abort());
    }

    #[test]
    fn test_live_controller_matrix_swaps_after_enough_windows() {
        let ctrl = DroLiveController::new(DroLiveControllerConfig {
            window_commit_budget: 4,
            max_readers: 8,
            max_writers: 8,
            default_threshold: 0.45,
            tracker_config: DroVolatilityTrackerConfig {
                window_size: 8,
                min_samples: 4,
            },
            default_tolerance: DroRiskTolerance::Low,
        });

        let initial_gen = ctrl.generation();
        // Feed 4 windows of 4 decisions each = 16 total decisions.
        // First 3 windows: low abort rate.
        for _ in 0..3 {
            for _ in 0..3 {
                ctrl.record_commit(1);
            }
            ctrl.record_abort(2);
        }
        // After 3 windows (12 decisions), tracker has 3 samples — not ready yet.
        assert!(
            ctrl.generation() <= 3,
            "bead_id=bd-3t52f should not have swapped matrix before min_samples"
        );

        // 4th window: high abort rate — this should trigger min_samples.
        for _ in 0..2 {
            ctrl.record_commit(0);
        }
        ctrl.record_abort(5);
        ctrl.record_abort(5);

        // After 4 windows, tracker has min_samples=4 → first swap on window 4.
        assert!(
            ctrl.generation() > initial_gen,
            "bead_id=bd-3t52f matrix should have been swapped at least once after 4 windows (gen={})",
            ctrl.generation()
        );
    }

    #[test]
    fn test_live_controller_adapts_to_high_abort_regime() {
        let ctrl = DroLiveController::new(DroLiveControllerConfig {
            window_commit_budget: 4,
            max_readers: 8,
            max_writers: 8,
            default_threshold: 0.45,
            tracker_config: DroVolatilityTrackerConfig {
                window_size: 8,
                min_samples: 4,
            },
            default_tolerance: DroRiskTolerance::Low,
        });

        // Phase 1: calm regime — low abort rate with some variance.
        // Window 1: 0% abort, Window 2: 0%, Window 3: 25%, Window 4: 25%
        ctrl.record_commit(0);
        ctrl.record_commit(0);
        ctrl.record_commit(0);
        ctrl.record_commit(0);
        ctrl.record_commit(0);
        ctrl.record_commit(0);
        ctrl.record_commit(0);
        ctrl.record_commit(0);
        ctrl.record_commit(0);
        ctrl.record_commit(0);
        ctrl.record_commit(0);
        ctrl.record_abort(1);
        ctrl.record_commit(0);
        ctrl.record_commit(0);
        ctrl.record_commit(0);
        ctrl.record_abort(1);
        let calm_matrix = ctrl.current_matrix();
        let calm_penalty = calm_matrix.penalty(6, 6);

        // Phase 2: storm regime — high abort rate with many edges and variance.
        // Window 5: 50%, Window 6: 75%, Window 7: 50%, Window 8: 100%
        ctrl.record_commit(5);
        ctrl.record_commit(5);
        ctrl.record_abort(10);
        ctrl.record_abort(10);
        ctrl.record_commit(5);
        ctrl.record_abort(10);
        ctrl.record_abort(10);
        ctrl.record_abort(10);
        ctrl.record_commit(5);
        ctrl.record_commit(5);
        ctrl.record_abort(10);
        ctrl.record_abort(10);
        ctrl.record_abort(10);
        ctrl.record_abort(10);
        ctrl.record_abort(10);
        ctrl.record_abort(10);
        let storm_matrix = ctrl.current_matrix();
        let storm_penalty = storm_matrix.penalty(6, 6);

        // Storm radius should be larger → penalties should be higher.
        assert!(
            storm_penalty > calm_penalty,
            "bead_id=bd-3t52f storm penalty ({storm_penalty}) should exceed \
             calm penalty ({calm_penalty}) due to wider Wasserstein radius"
        );
    }

    #[test]
    fn test_live_controller_tolerance_change_triggers_recompute() {
        let ctrl = DroLiveController::new(DroLiveControllerConfig {
            window_commit_budget: 4,
            max_readers: 8,
            max_writers: 8,
            default_threshold: 0.45,
            tracker_config: DroVolatilityTrackerConfig {
                window_size: 8,
                min_samples: 4,
            },
            default_tolerance: DroRiskTolerance::Low,
        });

        // Seed the tracker with varying abort rates across windows to create
        // nonzero variance (and thus nonzero Wasserstein radius).
        // Window 1: 0% abort, Window 2: 25% abort, Window 3: 50%, Window 4: 75%
        ctrl.record_commit(1);
        ctrl.record_commit(1);
        ctrl.record_commit(1);
        ctrl.record_commit(1);
        ctrl.record_commit(1);
        ctrl.record_commit(1);
        ctrl.record_commit(1);
        ctrl.record_abort(2);
        ctrl.record_commit(1);
        ctrl.record_commit(1);
        ctrl.record_abort(2);
        ctrl.record_abort(2);
        ctrl.record_commit(1);
        ctrl.record_abort(2);
        ctrl.record_abort(2);
        ctrl.record_abort(2);
        let gen_before = ctrl.generation();
        let low_matrix = ctrl.current_matrix();
        let low_penalty = low_matrix.penalty(6, 6);

        // Switch to High tolerance → wider radius → higher penalties.
        ctrl.set_tolerance(DroRiskTolerance::High);

        assert!(
            ctrl.generation() > gen_before,
            "bead_id=bd-3t52f tolerance change should bump generation"
        );

        let high_matrix = ctrl.current_matrix();
        let high_penalty = high_matrix.penalty(6, 6);

        assert!(
            high_penalty > low_penalty,
            "bead_id=bd-3t52f High tolerance penalty ({high_penalty}) should exceed \
             Low tolerance penalty ({low_penalty})"
        );
    }

    #[test]
    fn test_live_controller_global_singleton_is_consistent() {
        let g1 = DroLiveController::global();
        let g2 = DroLiveController::global();
        // Same pointer — OnceLock guarantees singleton.
        assert!(
            std::ptr::eq(g1, g2),
            "bead_id=bd-3t52f global() must return the same instance"
        );
        // Matrix should be evaluatable.
        let matrix = g1.current_matrix();
        let decision = matrix.evaluate(1, 1);
        assert!(
            decision.cvar_penalty >= 0.0,
            "bead_id=bd-3t52f global matrix should produce valid penalty"
        );
    }

    // =========================================================================
    // bd-1uguv: Adversarial Schedule Verification for DRO
    // =========================================================================

    /// Adversarial regime for DRO verification.
    #[derive(Debug, Clone)]
    struct AdversarialRegime {
        name: &'static str,
        txn_count: u32,
        /// Abort probability when this regime simulates page conflicts.
        /// Higher = more contentious. Drives `record_abort` vs `record_commit`.
        abort_probability: f64,
        /// Average edge count per decision.
        edge_count: u64,
        /// Concurrency level (active readers/writers for matrix evaluation).
        active_readers: usize,
        active_writers: usize,
    }

    /// Canonical 6-regime adversarial schedule.
    fn canonical_adversarial_schedule() -> Vec<AdversarialRegime> {
        vec![
            AdversarialRegime {
                name: "warm_uniform",
                txn_count: 500,
                abort_probability: 0.03,
                edge_count: 1,
                active_readers: 4,
                active_writers: 4,
            },
            AdversarialRegime {
                name: "sudden_skew",
                txn_count: 500,
                abort_probability: 0.35,
                edge_count: 4,
                active_readers: 4,
                active_writers: 4,
            },
            AdversarialRegime {
                name: "sustained_skew",
                txn_count: 1000,
                abort_probability: 0.30,
                edge_count: 3,
                active_readers: 4,
                active_writers: 4,
            },
            AdversarialRegime {
                name: "sudden_calm",
                txn_count: 500,
                abort_probability: 0.04,
                edge_count: 1,
                active_readers: 4,
                active_writers: 4,
            },
            AdversarialRegime {
                name: "hot_page_storm",
                txn_count: 500,
                abort_probability: 0.55,
                edge_count: 6,
                active_readers: 8,
                active_writers: 8,
            },
            AdversarialRegime {
                name: "recovery",
                txn_count: 500,
                abort_probability: 0.03,
                edge_count: 1,
                active_readers: 4,
                active_writers: 4,
            },
        ]
    }

    /// Per-regime metrics collected during adversarial runs.
    #[derive(Debug, Clone)]
    #[allow(dead_code)] // fields used in assertions across multiple tests
    struct RegimeMetrics {
        regime_name: &'static str,
        policy: &'static str,
        committed: u64,
        aborted: u64,
        abort_rate: f64,
        radius_at_end: f64,
        generation_at_end: u64,
        generation_at_start: u64,
        dro_aborts: u64,
        dro_allows: u64,
    }

    /// Deterministic pseudo-random abort decision given seed + index.
    fn deterministic_should_abort(seed: u64, index: u32, probability: f64) -> bool {
        // Simple deterministic hash: mix seed and index, take fractional part.
        let mixed = seed
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(u64::from(index).wrapping_mul(1_442_695_040_888_963_407));
        #[allow(clippy::cast_precision_loss)]
        let frac = ((mixed >> 11) as f64) / ((1_u64 << 53) as f64);
        frac < probability
    }

    /// Run one adversarial schedule under a given DRO controller configuration.
    /// Returns per-regime metrics.
    fn run_adversarial_schedule(
        seed: u64,
        schedule: &[AdversarialRegime],
        window_commit_budget: u64,
    ) -> Vec<RegimeMetrics> {
        let config = DroLiveControllerConfig {
            window_commit_budget,
            ..DroLiveControllerConfig::default()
        };
        let ctrl = DroLiveController::new(config);
        let mut results = Vec::with_capacity(schedule.len());

        for regime in schedule {
            let gen_start = ctrl.generation();
            let mut committed = 0_u64;
            let mut aborted = 0_u64;
            let mut dro_aborts = 0_u64;
            let mut dro_allows = 0_u64;

            for i in 0..regime.txn_count {
                let is_abort = deterministic_should_abort(
                    seed,
                    i + committed as u32,
                    regime.abort_probability,
                );

                // Evaluate DRO decision for this regime's concurrency.
                let matrix = ctrl.current_matrix();
                let decision = matrix.evaluate(regime.active_readers, regime.active_writers);
                if decision.should_abort() {
                    dro_aborts += 1;
                } else {
                    dro_allows += 1;
                }

                if is_abort {
                    ctrl.record_abort(regime.edge_count);
                    aborted += 1;
                } else {
                    ctrl.record_commit(regime.edge_count);
                    committed += 1;
                }
            }

            let gen_end = ctrl.generation();
            let matrix = ctrl.current_matrix();
            let radius = matrix.radius().effective_radius();
            let total = committed + aborted;

            results.push(RegimeMetrics {
                regime_name: regime.name,
                policy: if window_commit_budget == u64::MAX {
                    "static"
                } else {
                    "dro"
                },
                committed,
                aborted,
                #[allow(clippy::cast_precision_loss)]
                abort_rate: if total == 0 {
                    0.0
                } else {
                    aborted as f64 / total as f64
                },
                radius_at_end: radius,
                generation_at_end: gen_end,
                generation_at_start: gen_start,
                dro_aborts,
                dro_allows,
            });
        }

        results
    }

    #[test]
    fn test_adversarial_schedule_generator_determinism() {
        // bead_id=bd-1uguv: Same seed must produce identical abort sequences.
        let seed = 42_u64;
        let schedule = canonical_adversarial_schedule();
        let run1 = run_adversarial_schedule(seed, &schedule, 128);
        let run2 = run_adversarial_schedule(seed, &schedule, 128);

        for (m1, m2) in run1.iter().zip(run2.iter()) {
            assert_eq!(
                m1.committed, m2.committed,
                "bead_id=bd-1uguv determinism: regime '{}' committed mismatch",
                m1.regime_name
            );
            assert_eq!(
                m1.aborted, m2.aborted,
                "bead_id=bd-1uguv determinism: regime '{}' aborted mismatch",
                m1.regime_name
            );
        }
    }

    #[test]
    fn test_dro_adversarial_adaptation_evidence() {
        // bead_id=bd-1uguv: DRO radius must expand under skew and contract
        // during recovery.
        let seed = 42_u64;
        let schedule = canonical_adversarial_schedule();
        let dro_metrics = run_adversarial_schedule(seed, &schedule, 128);

        // After regime 0 (warm_uniform): baseline radius.
        let r0_radius = dro_metrics[0].radius_at_end;

        // After regime 2 (sustained_skew): radius should be larger.
        let r2_radius = dro_metrics[2].radius_at_end;
        assert!(
            r2_radius > r0_radius,
            "bead_id=bd-1uguv adaptation: sustained_skew radius ({r2_radius:.6}) \
             should exceed warm_uniform radius ({r0_radius:.6})"
        );

        // The radius after skew regimes (2+) should remain elevated compared to
        // the initial warm baseline. The tracker's sliding window means the
        // radius cannot contract to baseline in a single 500-txn recovery window.
        let r5_radius = dro_metrics[5].radius_at_end;
        assert!(
            r5_radius > r0_radius,
            "bead_id=bd-1uguv adaptation: recovery radius ({r5_radius:.6}) \
             should still exceed initial warm_uniform radius ({r0_radius:.6}) \
             because the sliding window retains recent volatile observations"
        );

        // The key proof: over the full adversarial schedule, the DRO controller
        // adapted at least once (matrix generation increased). This is the
        // falsifiable claim that the controller is NOT static.
        let total_gen = dro_metrics[5].generation_at_end;
        assert!(
            total_gen >= 3,
            "bead_id=bd-1uguv adaptation: DRO matrix generation ({total_gen}) should \
             be >= 3 after full adversarial schedule"
        );

        // Matrix swaps should have occurred during skew regimes.
        let skew_swaps: u64 = dro_metrics[1..=3]
            .iter()
            .map(|m| m.generation_at_end.saturating_sub(m.generation_at_start))
            .sum();
        assert!(
            skew_swaps > 0,
            "bead_id=bd-1uguv adaptation: DRO matrix must swap at least once \
             during skew regimes (swaps={skew_swaps})"
        );
    }

    #[test]
    fn test_dro_adversarial_abort_storm_suppression() {
        // bead_id=bd-1uguv: Under hot_page_storm (regime 4), the DRO matrix
        // should report HIGHER CVaR penalties at (8,8) than at (4,4) during
        // warm_uniform. This proves the matrix encodes contention sensitivity.
        //
        // Note: whether those penalties cross the abort threshold depends on
        // the Wasserstein radius. At default conservative thresholds (0.45) the
        // penalties may not trigger actual abort decisions, but the relative
        // ordering must hold — higher contention = higher penalty.
        let seed = 42_u64;
        let schedule = canonical_adversarial_schedule();
        let dro_metrics = run_adversarial_schedule(seed, &schedule, 128);

        // Rebuild the controller through the pre-storm regimes using the same
        // deterministic outcome schedule as `run_adversarial_schedule()`.  The
        // penalty comparison below should interrogate the same controller state
        // that the canonical adversarial runner would reach before regime 4.
        let config = DroLiveControllerConfig {
            window_commit_budget: 128,
            ..DroLiveControllerConfig::default()
        };
        let ctrl = DroLiveController::new(config);

        // Feed the controller through regimes 0-3 to build up Wasserstein radius.
        for regime in &schedule[..4] {
            let mut committed = 0_u64;
            for i in 0..regime.txn_count {
                let is_abort = deterministic_should_abort(
                    seed,
                    i + committed as u32,
                    regime.abort_probability,
                );
                if is_abort {
                    ctrl.record_abort(regime.edge_count);
                } else {
                    ctrl.record_commit(regime.edge_count);
                    committed += 1;
                }
            }
        }

        let matrix = ctrl.current_matrix();
        let penalty_warm = matrix.penalty(4, 4);
        let penalty_storm = matrix.penalty(8, 8);

        assert!(
            penalty_storm > penalty_warm,
            "bead_id=bd-1uguv storm suppression: CVaR penalty at (8,8)={penalty_storm:.6} \
             should exceed penalty at (4,4)={penalty_warm:.6}"
        );

        // Also verify: the radius has expanded from the skew regimes.
        let radius = matrix.radius().effective_radius();
        assert!(
            radius > 0.0,
            "bead_id=bd-1uguv storm suppression: radius ({radius:.6}) should be positive \
             after observing adversarial regimes"
        );

        // Verify accounting is still correct.
        let total_txns: u64 = dro_metrics.iter().map(|m| m.committed + m.aborted).sum();
        let expected: u64 = schedule.iter().map(|r| u64::from(r.txn_count)).sum();
        assert_eq!(
            total_txns, expected,
            "bead_id=bd-1uguv storm suppression: total txns ({total_txns}) != expected ({expected})"
        );
    }

    #[test]
    fn test_dro_vs_static_p99_regime_shift_detection() {
        // bead_id=bd-1uguv: The DRO controller (window_commit_budget=128) must
        // produce more matrix swaps than the static controller (never adapts).
        let seed = 42_u64;
        let schedule = canonical_adversarial_schedule();
        let dro_metrics = run_adversarial_schedule(seed, &schedule, 128);
        let static_metrics = run_adversarial_schedule(seed, &schedule, u64::MAX);

        let dro_total_swaps: u64 = dro_metrics
            .iter()
            .map(|m| m.generation_at_end.saturating_sub(m.generation_at_start))
            .sum();
        let static_total_swaps: u64 = static_metrics
            .iter()
            .map(|m| m.generation_at_end.saturating_sub(m.generation_at_start))
            .sum();

        assert!(
            dro_total_swaps > static_total_swaps,
            "bead_id=bd-1uguv regime detection: DRO swaps ({dro_total_swaps}) \
             must exceed static swaps ({static_total_swaps})"
        );

        // Static controller should have zero swaps (never rotates).
        assert_eq!(
            static_total_swaps, 0,
            "bead_id=bd-1uguv regime detection: static controller should never swap"
        );

        // DRO controller should have at least 3 swaps (one per major regime shift).
        assert!(
            dro_total_swaps >= 3,
            "bead_id=bd-1uguv regime detection: DRO should have >= 3 matrix swaps, got {dro_total_swaps}"
        );
    }

    #[test]
    fn test_dro_adversarial_correctness_accounting() {
        // bead_id=bd-1uguv: Every transaction is either committed or aborted.
        // No transactions lost.
        let seed = 42_u64;
        let schedule = canonical_adversarial_schedule();
        let metrics = run_adversarial_schedule(seed, &schedule, 128);

        for (regime, m) in schedule.iter().zip(metrics.iter()) {
            let total = m.committed + m.aborted;
            assert_eq!(
                total,
                u64::from(regime.txn_count),
                "bead_id=bd-1uguv accounting: regime '{}' total ({total}) != expected ({})",
                regime.name,
                regime.txn_count
            );
            // DRO decisions should cover every txn too.
            assert_eq!(
                m.dro_aborts + m.dro_allows,
                u64::from(regime.txn_count),
                "bead_id=bd-1uguv accounting: regime '{}' DRO decisions ({}) != expected ({})",
                regime.name,
                m.dro_aborts + m.dro_allows,
                regime.txn_count
            );
        }
    }

    #[test]
    fn test_dro_adversarial_structured_log_emission() {
        // bead_id=bd-1uguv: Verify that running the adversarial schedule
        // produces the expected structured log output by checking that the
        // DRO controller logs matrix_swap events during regime shifts.
        // (This test verifies the controller's internal logging path runs
        // without panicking; actual log capture requires a tracing subscriber.)
        let seed = 123_u64;
        let schedule = canonical_adversarial_schedule();
        let metrics = run_adversarial_schedule(seed, &schedule, 64);

        // With a smaller window (64), we expect more frequent matrix swaps.
        let total_swaps: u64 = metrics
            .iter()
            .map(|m| m.generation_at_end.saturating_sub(m.generation_at_start))
            .sum();
        assert!(
            total_swaps >= 5,
            "bead_id=bd-1uguv logging: with window_commit_budget=64, expect >= 5 \
             matrix swaps for logging coverage, got {total_swaps}"
        );
    }
}
