//! Anytime-valid MVCC invariant monitoring via e-processes (§4.3, bd-3go.3).
//!
//! Wraps [`asupersync::lab::oracle::eprocess`] with per-invariant calibration
//! for the 7 core FrankenSQLite MVCC invariants (INV-1 through INV-7).
//!
//! # E-Process Theory
//!
//! An e-process `(E_t)` is a non-negative supermartingale under H₀ with `E₀ = 1`.
//! Ville's inequality guarantees `P_H₀(∃ t : E_t ≥ 1/α) ≤ α`, enabling
//! anytime-valid rejection without multiple-testing corrections.
//!
//! # Per-Invariant Calibration
//!
//! Hardware/CAS-enforced invariants (INV-1, INV-2, INV-7) use aggressive
//! parameters: `p₀ = 1e-9, λ = 0.999, α = 1e-6`.
//!
//! Software-enforced invariants (INV-3 through INV-6) use moderate parameters:
//! `p₀ = 1e-6, λ = 0.9, α = 0.001`.
//!
//! # Global Aggregation
//!
//! The global e-value is the arithmetic mean of individual e-values:
//! `E_global(t) = Σ wᵢ Eᵢ(t)` with equal weights `wᵢ = 1/7`.
//! This is a valid e-process under the global null regardless of dependence.

use std::{cmp::Ordering, collections::HashMap, fmt};

use asupersync::lab::oracle::eprocess::{EProcess, EProcessConfig};
use serde::{Deserialize, Serialize};
use tracing::{debug, error, info, warn};

/// Bead identifier for tracing and log correlation.
const BEAD_ID: &str = "bd-3go.3";

// ---------------------------------------------------------------------------
// MVCC invariant enum
// ---------------------------------------------------------------------------

/// The 7 core MVCC invariants monitored by e-processes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MvccInvariant {
    /// INV-1: TxnId allocation is monotonically increasing (CAS-enforced).
    Monotonicity,
    /// INV-2: At most one transaction holds a page lock at a time (CAS-enforced).
    LockExclusivity,
    /// INV-3: Version chains are in descending `commit_seq` order.
    VersionChainOrder,
    /// INV-4: Every write-set page is in the page lock table.
    WriteSetConsistency,
    /// INV-5: Snapshot is immutable after first read (DEFERRED mode).
    SnapshotStability,
    /// INV-6: Committed transactions are all-or-nothing visible.
    CommitAtomicity,
    /// INV-7: At most one Serialized-mode writer (mutex-enforced).
    SerializedModeExclusivity,
    /// INV-SSI-FP: SSI false-positive drift monitor (statistical quality metric).
    SsiFalsePositiveRate,
}

impl MvccInvariant {
    /// All 7 MVCC invariants in canonical order.
    pub const ALL: &[Self] = &[
        Self::Monotonicity,
        Self::LockExclusivity,
        Self::VersionChainOrder,
        Self::WriteSetConsistency,
        Self::SnapshotStability,
        Self::CommitAtomicity,
        Self::SerializedModeExclusivity,
    ];

    /// INV-1..INV-7 plus INV-SSI-FP.
    pub const ALL_WITH_SSI_FP: &[Self] = &[
        Self::Monotonicity,
        Self::LockExclusivity,
        Self::VersionChainOrder,
        Self::WriteSetConsistency,
        Self::SnapshotStability,
        Self::CommitAtomicity,
        Self::SerializedModeExclusivity,
        Self::SsiFalsePositiveRate,
    ];

    /// Human-readable invariant name including the INV number.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::Monotonicity => "INV-1:Monotonicity",
            Self::LockExclusivity => "INV-2:LockExclusivity",
            Self::VersionChainOrder => "INV-3:VersionChainOrder",
            Self::WriteSetConsistency => "INV-4:WriteSetConsistency",
            Self::SnapshotStability => "INV-5:SnapshotStability",
            Self::CommitAtomicity => "INV-6:CommitAtomicity",
            Self::SerializedModeExclusivity => "INV-7:SerializedModeExclusivity",
            Self::SsiFalsePositiveRate => "INV-SSI-FP:FalsePositiveRate",
        }
    }

    /// Per-invariant e-process calibration from the spec (§4.3).
    #[must_use]
    pub fn config(self) -> EProcessConfig {
        match self {
            // Hardware/CAS-enforced: aggressive detection.
            Self::Monotonicity | Self::LockExclusivity | Self::SerializedModeExclusivity => {
                EProcessConfig {
                    p0: 1e-9,
                    lambda: 0.999,
                    alpha: 1e-6,
                    max_evalue: 1e15,
                }
            }
            // Software-enforced: moderate detection.
            Self::VersionChainOrder
            | Self::WriteSetConsistency
            | Self::SnapshotStability
            | Self::CommitAtomicity => EProcessConfig {
                p0: 1e-6,
                lambda: 0.9,
                alpha: 0.001,
                max_evalue: 1e15,
            },
            // Statistical drift monitor, not a hard invariant.
            Self::SsiFalsePositiveRate => EProcessConfig {
                p0: 0.05,
                lambda: 0.8,
                alpha: 0.01,
                max_evalue: 1e15,
            },
        }
    }

    /// Short numeric prefix (e.g., `1` for INV-1).
    #[must_use]
    pub fn number(self) -> u8 {
        match self {
            Self::Monotonicity => 1,
            Self::LockExclusivity => 2,
            Self::VersionChainOrder => 3,
            Self::WriteSetConsistency => 4,
            Self::SnapshotStability => 5,
            Self::CommitAtomicity => 6,
            Self::SerializedModeExclusivity => 7,
            Self::SsiFalsePositiveRate => 8,
        }
    }
}

/// Local harness representation of a transaction identifier.
pub type TxnId = u64;
/// Local harness representation of a page number.
pub type PageNumber = u32;

/// Transaction lifecycle state for lock observation cross-checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TxnState {
    Active,
    Committed,
    Aborted,
}

/// Per-transaction lock metadata used by lock-exclusivity observation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActiveTxnInfo {
    pub state: TxnState,
    pub page_locks: Vec<PageNumber>,
}

/// Factory for calibrated MVCC monitors (INV-1..INV-7 + INV-SSI-FP).
#[must_use]
pub fn create_mvcc_monitors() -> Vec<EProcess> {
    MvccInvariant::ALL_WITH_SSI_FP
        .iter()
        .map(|inv| EProcess::new(inv.name(), inv.config()))
        .collect()
}

/// Observation function for INV-2 lock exclusivity.
///
/// Returns `true` when a violation is detected, otherwise `false`.
#[must_use]
pub fn observe_lock_exclusivity<S1, S2>(
    lock_table: &HashMap<PageNumber, TxnId, S1>,
    active_transactions: &HashMap<TxnId, ActiveTxnInfo, S2>,
) -> bool
where
    S1: std::hash::BuildHasher,
    S2: std::hash::BuildHasher,
{
    // Build page -> holders map from active transaction lock sets.
    let mut page_holders: HashMap<PageNumber, Vec<TxnId>> = HashMap::new();
    for (&txn_id, txn) in active_transactions {
        if txn.state == TxnState::Active {
            for &page in &txn.page_locks {
                page_holders.entry(page).or_default().push(txn_id);
            }
        }
    }

    // Any page with more than one active holder is a violation.
    if page_holders.values().any(|holders| holders.len() > 1) {
        return true;
    }

    // Every lock-table entry must correspond to an active txn and lock-set membership.
    for (&page, &holder) in lock_table {
        let Some(txn) = active_transactions.get(&holder) else {
            return true; // ghost lock
        };
        if txn.state != TxnState::Active {
            return true; // inactive holder
        }
        if !txn.page_locks.contains(&page) {
            return true; // disagreement: table says lock exists, txn set does not
        }
    }

    // Reverse cross-check: active txn lock set must be represented in lock_table.
    for (&txn_id, txn) in active_transactions {
        if txn.state != TxnState::Active {
            continue;
        }
        for &page in &txn.page_locks {
            if lock_table.get(&page) != Some(&txn_id) {
                return true;
            }
        }
    }

    false
}

/// Point-in-time hard-invariant metrics for runtime checks.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct HardInvariantSample {
    pub prev_txn_id: TxnId,
    pub current_txn_id: TxnId,
    pub max_concurrent_holders_per_page: usize,
    pub chain_order_violations_per_1k: usize,
    pub unlocked_writes_per_1k: usize,
    pub snapshot_mutation_events_per_txn: usize,
    pub partial_visibility_observations: usize,
    pub concurrent_serialized_writers: usize,
}

/// INV-1: TxnId monotonicity.
#[must_use]
pub fn check_inv1_monotonicity(prev: TxnId, current: TxnId) -> bool {
    let ok = current > prev;
    debug_assert!(
        ok,
        "INV-1 violation: TxnId must be strictly increasing (prev={prev}, current={current})"
    );
    ok
}

/// INV-2: lock exclusivity.
#[must_use]
pub fn check_inv2_lock_exclusivity(max_holders: usize) -> bool {
    let ok = max_holders <= 1;
    debug_assert!(
        ok,
        "INV-2 violation: max concurrent holders per page must be <= 1 (got {max_holders})"
    );
    ok
}

/// INV-3: version chain order.
#[must_use]
pub fn check_inv3_version_chain_order(chain_order_violations_per_1k: usize) -> bool {
    let ok = chain_order_violations_per_1k == 0;
    debug_assert!(
        ok,
        "INV-3 violation: chain order violations per 1K ops must be 0 (got \
         {chain_order_violations_per_1k})"
    );
    ok
}

/// INV-4: write-set consistency.
#[must_use]
pub fn check_inv4_write_set_consistency(unlocked_writes_per_1k: usize) -> bool {
    let ok = unlocked_writes_per_1k == 0;
    debug_assert!(
        ok,
        "INV-4 violation: unlocked writes per 1K ops must be 0 (got {unlocked_writes_per_1k})"
    );
    ok
}

/// INV-5: snapshot stability.
#[must_use]
pub fn check_inv5_snapshot_stability(snapshot_mutation_events_per_txn: usize) -> bool {
    let ok = snapshot_mutation_events_per_txn == 0;
    debug_assert!(
        ok,
        "INV-5 violation: snapshot mutation events per txn must be 0 (got \
         {snapshot_mutation_events_per_txn})"
    );
    ok
}

/// INV-6: commit atomicity.
#[must_use]
pub fn check_inv6_commit_atomicity(partial_visibility_observations: usize) -> bool {
    let ok = partial_visibility_observations == 0;
    debug_assert!(
        ok,
        "INV-6 violation: partial visibility observations must be 0 (got \
         {partial_visibility_observations})"
    );
    ok
}

/// INV-7: serialized mode exclusivity.
#[must_use]
pub fn check_inv7_serialized_mode_exclusivity(concurrent_serialized_writers: usize) -> bool {
    let ok = concurrent_serialized_writers <= 1;
    debug_assert!(
        ok,
        "INV-7 violation: concurrent serialized writers must be <= 1 (got \
         {concurrent_serialized_writers})"
    );
    ok
}

/// Runs all hard-invariant checks (INV-1..INV-7) in one call.
#[must_use]
pub fn check_hard_invariants(sample: HardInvariantSample) -> bool {
    check_inv1_monotonicity(sample.prev_txn_id, sample.current_txn_id)
        && check_inv2_lock_exclusivity(sample.max_concurrent_holders_per_page)
        && check_inv3_version_chain_order(sample.chain_order_violations_per_1k)
        && check_inv4_write_set_consistency(sample.unlocked_writes_per_1k)
        && check_inv5_snapshot_stability(sample.snapshot_mutation_events_per_txn)
        && check_inv6_commit_atomicity(sample.partial_visibility_observations)
        && check_inv7_serialized_mode_exclusivity(sample.concurrent_serialized_writers)
}

/// Runtime monitoring facade: hard invariants via `debug_assert!`, SSI-FP via e-process.
pub struct RuntimeInvariantMonitor {
    ssi_fp: EProcess,
}

impl RuntimeInvariantMonitor {
    /// Constructs a runtime monitor with INV-SSI-FP calibration.
    #[must_use]
    pub fn new() -> Self {
        let config = MvccInvariant::SsiFalsePositiveRate.config();
        Self {
            ssi_fp: EProcess::new(MvccInvariant::SsiFalsePositiveRate.name(), config),
        }
    }

    /// Whether a specific invariant is monitored with an e-process at runtime.
    #[must_use]
    pub fn uses_eprocess_for(&self, invariant: MvccInvariant) -> bool {
        invariant == MvccInvariant::SsiFalsePositiveRate
    }

    /// Observe one SSI false-positive outcome (true = false-positive abort occurred).
    pub fn observe_ssi_false_positive(&mut self, false_positive_abort: bool) {
        self.ssi_fp.observe(false_positive_abort);
        info!(
            bead_id = "bd-1cx0",
            monitor = "INV-SSI-FP",
            status = if self.ssi_fp.rejected {
                "rejected"
            } else {
                "monitoring"
            },
            e_value = self.ssi_fp.current,
            threshold = self.ssi_fp.config.threshold(),
            "invariant monitor state change"
        );

        if self.ssi_fp.current >= 0.1 / self.ssi_fp.config.alpha {
            warn!(
                bead_id = "bd-1cx0",
                monitor = "INV-SSI-FP",
                evidence_id = format!("inv-ssi-fp-{}", self.ssi_fp.observations),
                e_value = self.ssi_fp.current,
                threshold = self.ssi_fp.config.threshold(),
                "drift/regime shift detected"
            );
        }
    }

    /// Runs hard-invariant assertions for one sample.
    #[must_use]
    pub fn check_hard_sample(&self, sample: HardInvariantSample) -> bool {
        check_hard_invariants(sample)
    }

    /// Access current SSI-FP e-process state.
    #[must_use]
    pub fn ssi_fp_state(&self) -> &EProcess {
        &self.ssi_fp
    }
}

impl Default for RuntimeInvariantMonitor {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for MvccInvariant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

// ---------------------------------------------------------------------------
// Rejection certificate
// ---------------------------------------------------------------------------

/// Certificate emitted when an e-process rejects H₀, providing evidence
/// that an MVCC invariant has been violated.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RejectionCertificate {
    /// Which invariant was violated.
    pub invariant: String,
    /// The e-value at rejection time.
    pub e_value: f64,
    /// The rejection threshold `1/α`.
    pub threshold: f64,
    /// Total observations processed.
    pub observation_count: usize,
    /// Violations observed before rejection.
    pub violations_observed: usize,
    /// Empirical violation rate at rejection time.
    pub empirical_rate: f64,
    /// Observation index at which rejection occurred.
    pub rejection_time: usize,
}

// ---------------------------------------------------------------------------
// Per-invariant monitor result
// ---------------------------------------------------------------------------

/// Summary of a single invariant's e-process state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MvccMonitorResult {
    /// Which invariant this result is for.
    pub invariant: MvccInvariant,
    /// Current e-value.
    pub e_value: f64,
    /// log₁₀ of the current e-value.
    pub log10_e_value: f64,
    /// Whether H₀ has been rejected.
    pub rejected: bool,
    /// Observation at which rejection first occurred, if any.
    pub rejection_time: Option<usize>,
    /// Total observations processed.
    pub observations: usize,
    /// Number of violations observed.
    pub violations_observed: usize,
    /// Empirical violation rate.
    pub empirical_rate: f64,
}

/// A weighted evidence contributor in a global e-value aggregation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvidenceContributor {
    /// Invariant producing this contribution.
    pub invariant: MvccInvariant,
    /// Weighted contribution `wᵢ * Eᵢ(t)`.
    pub weighted_evidence: f64,
    /// Contribution share in `(0, 1]` relative to `E_global(t)`.
    pub share: f64,
}

/// Evidence ledger entry for global aggregation at a decision point.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregatedEvidenceEntry {
    /// Aggregated e-value at decision time.
    pub e_global: f64,
    /// Rejection threshold `1 / alpha_global`.
    pub threshold: f64,
    /// Whether global H₀ has been rejected.
    pub rejected: bool,
    /// Number of observations consumed by the monitor.
    pub observation_count: usize,
    /// Top weighted contributors sorted descending.
    pub contributors: Vec<EvidenceContributor>,
}

// ---------------------------------------------------------------------------
// Mixture e-process
// ---------------------------------------------------------------------------

/// A mixture e-process over a log grid of λ values.
///
/// `E_mix(t) = Σⱼ wⱼ E_{λⱼ}(t)` achieves near-oracle power without
/// hand-tuning λ. The weights are uniform over the grid.
pub struct MixtureEProcess {
    components: Vec<EProcess>,
    weights: Vec<f64>,
}

impl MixtureEProcess {
    /// Creates a mixture e-process with `grid_size` λ values on a log grid.
    ///
    /// The grid spans `[lambda_min + ε, lambda_max - ε]` for the given `p0`.
    #[must_use]
    pub fn new(invariant: &str, p0: f64, alpha: f64, grid_size: usize) -> Self {
        assert!(grid_size >= 2, "grid_size must be >= 2");

        let lambda_max = 1.0 / p0;
        // Use 1% to 95% of the valid range to stay away from boundaries.
        let lo = 0.01 * lambda_max;
        let hi = 0.95 * lambda_max;
        let log_lo = lo.ln();
        let log_hi = hi.ln();

        let n = grid_size as f64;
        let weight = 1.0 / n;
        let mut components = Vec::with_capacity(grid_size);
        let mut weights = Vec::with_capacity(grid_size);

        for i in 0..grid_size {
            let t = if grid_size == 1 {
                0.5
            } else {
                i as f64 / (grid_size - 1) as f64
            };
            let lambda = (log_lo + t * (log_hi - log_lo)).exp();
            let config = EProcessConfig {
                p0,
                lambda,
                alpha,
                max_evalue: 1e15,
            };
            assert!(
                config.validate().is_ok(),
                "invalid config for lambda={lambda}: {}",
                config.validate().unwrap_err()
            );
            components.push(EProcess::new(invariant, config));
            weights.push(weight);
        }

        Self {
            components,
            weights,
        }
    }

    /// Processes a single observation across all mixture components.
    pub fn observe(&mut self, violated: bool) {
        for ep in &mut self.components {
            ep.observe(violated);
        }
    }

    /// The mixture e-value: weighted average of component e-values.
    #[must_use]
    pub fn e_value(&self) -> f64 {
        // Compute Σᵢ wᵢEᵢ using log-sum-exp for numerical stability.
        let mut max_log_term = f64::NEG_INFINITY;
        for (ep, weight) in self.components.iter().zip(&self.weights) {
            let log_term = weight.max(1e-300).ln() + ep.current.max(1e-300).ln();
            max_log_term = max_log_term.max(log_term);
        }
        if !max_log_term.is_finite() {
            return 0.0;
        }

        let sum_exp = self
            .components
            .iter()
            .zip(&self.weights)
            .map(|(ep, weight)| {
                let log_term = weight.max(1e-300).ln() + ep.current.max(1e-300).ln();
                (log_term - max_log_term).exp()
            })
            .sum::<f64>();

        (max_log_term + sum_exp.max(1e-300).ln()).exp()
    }

    /// Whether the mixture e-value has rejected H₀.
    #[must_use]
    pub fn rejected(&self, alpha: f64) -> bool {
        self.e_value() >= 1.0 / alpha
    }

    /// Number of observations processed (same for all components).
    #[must_use]
    pub fn observations(&self) -> usize {
        self.components.first().map_or(0, |ep| ep.observations)
    }

    /// Number of grid components.
    #[must_use]
    pub fn grid_size(&self) -> usize {
        self.components.len()
    }
}

// ---------------------------------------------------------------------------
// MVCC e-process monitor
// ---------------------------------------------------------------------------

/// Multi-invariant e-process monitor with per-invariant calibration for MVCC.
///
/// Each of the 7 MVCC invariants (INV-1..INV-7) gets its own [`EProcess`]
/// with calibration values from the spec (§4.3). The global e-value is the
/// arithmetic mean of individual e-values.
pub struct MvccEProcessMonitor {
    /// Per-invariant (invariant, e-process) pairs.
    processes: Vec<(MvccInvariant, EProcess)>,
    /// Weights for global aggregation (default: equal `1/7`).
    weights: Vec<f64>,
    /// Global significance level for the aggregated e-value.
    alpha_global: f64,
}

impl MvccEProcessMonitor {
    /// Creates a monitor for all 7 MVCC invariants with spec-defined calibration.
    #[must_use]
    pub fn new() -> Self {
        Self::with_alpha_global(0.001)
    }

    /// Creates a monitor with a custom global significance level.
    #[must_use]
    pub fn with_alpha_global(alpha_global: f64) -> Self {
        let invariants = MvccInvariant::ALL;
        let n = invariants.len() as f64;
        let processes: Vec<_> = invariants
            .iter()
            .map(|&inv| {
                let config = inv.config();
                info!(
                    bead_id = BEAD_ID,
                    invariant = inv.name(),
                    p0 = config.p0,
                    lambda = config.lambda,
                    alpha = config.alpha,
                    "E-process monitor created"
                );
                let ep = EProcess::new(inv.name(), config);
                (inv, ep)
            })
            .collect();
        let weights = vec![1.0 / n; invariants.len()];

        Self {
            processes,
            weights,
            alpha_global,
        }
    }

    /// Feed an observation for a specific invariant.
    ///
    /// `violated` is `true` if the invariant check detected a violation.
    pub fn observe(&mut self, invariant: MvccInvariant, violated: bool) {
        let Some((_, ep)) = self.processes.iter_mut().find(|(inv, _)| *inv == invariant) else {
            return;
        };

        ep.observe(violated);

        // DEBUG: periodic checkpoint every 100 observations.
        if ep.observations % 100 == 0 {
            debug!(
                bead_id = BEAD_ID,
                invariant = invariant.name(),
                e_value = ep.current,
                observations = ep.observations,
                trend = if ep.current > 1.0 { "rising" } else { "stable" },
                "E-process observation checkpoint"
            );
        }

        // WARN: approaching rejection threshold (E_t > 0.1/α).
        if !ep.rejected && ep.current >= 0.1 / ep.config.alpha {
            warn!(
                bead_id = BEAD_ID,
                invariant = invariant.name(),
                e_value = ep.current,
                threshold = ep.config.threshold(),
                observations = ep.observations,
                "E-process approaching rejection threshold"
            );
        }

        // ERROR: rejection detected (first time only).
        if ep.rejected && ep.rejection_time == Some(ep.observations - 1) {
            info!(
                bead_id = BEAD_ID,
                invariant = invariant.name(),
                alpha = ep.config.alpha,
                threshold = ep.config.threshold(),
                e_value = ep.current,
                t_reject = ep.observations,
                "E-process rejection event"
            );
            error!(
                bead_id = BEAD_ID,
                invariant = invariant.name(),
                e_value = ep.current,
                threshold = ep.config.threshold(),
                observations = ep.observations,
                violations = ep.violations_observed,
                "E-process REJECTED H₀ — invariant violation detected"
            );
        }
    }

    /// Feed observations for multiple invariants at once.
    pub fn observe_all(&mut self, observations: &[(MvccInvariant, bool)]) {
        for &(inv, violated) in observations {
            self.observe(inv, violated);
        }
    }

    /// Global e-value: arithmetic mean of individual e-values.
    ///
    /// `E_global(t) = Σᵢ wᵢ Eᵢ(t)` is a valid e-process under the
    /// intersection null `⋂ᵢ H₀ⁱ` regardless of dependence between monitors.
    #[must_use]
    pub fn global_e_value(&self) -> f64 {
        self.processes
            .iter()
            .zip(&self.weights)
            .map(|((_, ep), w)| w * ep.current)
            .sum()
    }

    /// Whether the global aggregated e-value has rejected.
    #[must_use]
    pub fn global_rejected(&self) -> bool {
        self.global_e_value() >= 1.0 / self.alpha_global
    }

    /// Whether any individual invariant has been rejected.
    #[must_use]
    pub fn any_rejected(&self) -> bool {
        self.processes.iter().any(|(_, ep)| ep.rejected)
    }

    /// Returns invariants that have been rejected.
    #[must_use]
    pub fn rejected_invariants(&self) -> Vec<MvccInvariant> {
        self.processes
            .iter()
            .filter(|(_, ep)| ep.rejected)
            .map(|(inv, _)| *inv)
            .collect()
    }

    /// Get the underlying e-process for a specific invariant.
    #[must_use]
    pub fn process(&self, invariant: MvccInvariant) -> Option<&EProcess> {
        self.processes
            .iter()
            .find(|(inv, _)| *inv == invariant)
            .map(|(_, ep)| ep)
    }

    /// Build a rejection certificate for a specific invariant.
    ///
    /// Returns `None` if the invariant hasn't been rejected.
    #[must_use]
    pub fn certificate_for(&self, invariant: MvccInvariant) -> Option<RejectionCertificate> {
        let (_, ep) = self.processes.iter().find(|(inv, _)| *inv == invariant)?;
        let rejection_time = ep.rejection_time?;

        Some(RejectionCertificate {
            invariant: invariant.name().to_owned(),
            e_value: ep.current,
            threshold: ep.config.threshold(),
            observation_count: ep.observations,
            violations_observed: ep.violations_observed,
            empirical_rate: ep.empirical_rate(),
            rejection_time,
        })
    }

    /// Returns per-invariant results for all 7 monitors.
    #[must_use]
    pub fn results(&self) -> Vec<MvccMonitorResult> {
        self.processes
            .iter()
            .map(|(inv, ep)| MvccMonitorResult {
                invariant: *inv,
                e_value: ep.current,
                log10_e_value: ep.log10_e_value(),
                rejected: ep.rejected,
                rejection_time: ep.rejection_time,
                observations: ep.observations,
                violations_observed: ep.violations_observed,
                empirical_rate: ep.empirical_rate(),
            })
            .collect()
    }

    /// Resets all e-processes to initial state.
    pub fn reset(&mut self) {
        for (_, ep) in &mut self.processes {
            ep.reset();
        }
    }

    /// Returns the global significance level.
    #[must_use]
    pub fn alpha_global(&self) -> f64 {
        self.alpha_global
    }

    /// Returns the weights used for global aggregation.
    #[must_use]
    pub fn weights(&self) -> &[f64] {
        &self.weights
    }

    /// Builds a global evidence entry with top weighted contributors.
    #[must_use]
    pub fn evidence_entry(&self, top_k: usize) -> AggregatedEvidenceEntry {
        let e_global = self.global_e_value();
        let threshold = 1.0 / self.alpha_global;
        let observation_count = self
            .processes
            .iter()
            .map(|(_, ep)| ep.observations)
            .max()
            .unwrap_or(0);

        let mut contributors = self
            .processes
            .iter()
            .zip(&self.weights)
            .map(|((invariant, ep), weight)| {
                let weighted_evidence = weight * ep.current;
                let share = if e_global > 0.0 {
                    weighted_evidence / e_global
                } else {
                    0.0
                };
                EvidenceContributor {
                    invariant: *invariant,
                    weighted_evidence,
                    share,
                }
            })
            .collect::<Vec<_>>();

        contributors.sort_by(|a, b| {
            b.weighted_evidence
                .partial_cmp(&a.weighted_evidence)
                .unwrap_or(Ordering::Equal)
        });

        if top_k < contributors.len() {
            contributors.truncate(top_k);
        }

        AggregatedEvidenceEntry {
            e_global,
            threshold,
            rejected: e_global >= threshold,
            observation_count,
            contributors,
        }
    }
}

impl Default for MvccEProcessMonitor {
    fn default() -> Self {
        Self::new()
    }
}

// ===========================================================================
// Tests (§4.3 unit test requirements)
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::panic::{self, AssertUnwindSafe};

    const TEST_BEAD_ID: &str = "bd-3go.3";
    const FRAMEWORK_BEAD_ID: &str = "bd-x1ww";
    const MONITOR_BEAD_ID: &str = "bd-3q2k";
    const RUNTIME_BEAD_ID: &str = "bd-1cx0";

    fn assert_debug_assert_behavior(f: impl FnOnce() + std::panic::UnwindSafe) {
        let panicked = panic::catch_unwind(AssertUnwindSafe(f)).is_err();
        if cfg!(debug_assertions) {
            assert!(
                panicked,
                "bead_id={RUNTIME_BEAD_ID} expected debug_assert panic in debug builds"
            );
        } else {
            assert!(
                !panicked,
                "bead_id={RUNTIME_BEAD_ID} expected no debug_assert panic in release builds"
            );
        }
    }

    #[test]
    fn test_eprocess_initial_state() {
        let config = EProcessConfig::default();
        let ep = EProcess::new("framework_init", config);
        assert!(
            (ep.current - 1.0).abs() < f64::EPSILON,
            "bead_id={FRAMEWORK_BEAD_ID} e-process must start at 1.0"
        );
        assert_eq!(
            ep.observations, 0,
            "bead_id={FRAMEWORK_BEAD_ID} observations must start at zero"
        );
        assert!(
            !ep.rejected,
            "bead_id={FRAMEWORK_BEAD_ID} monitor should not start rejected"
        );
    }

    // -- 1. test_eprocess_observe_no_violations_stays_near_one --

    #[test]
    fn test_eprocess_observe_no_violations_stays_near_one() {
        // Run 10,000 observations with X_t=0. Verify E_t fluctuates near 1.0.
        // Under H₀ with no violations, factor = 1 + λ(0 - p₀) = 1 - λp₀.
        // For INV-3 config: factor = 1 - 0.9 * 1e-6 ≈ 0.9999991.
        // After 10K steps: E_t ≈ (0.9999991)^10000 ≈ 0.991 — still near 1.0.
        let config = MvccInvariant::VersionChainOrder.config();
        let mut ep = EProcess::new("INV-3:VersionChainOrder", config);

        for _ in 0..10_000 {
            ep.observe(false);
        }

        assert!(
            ep.current > 0.5 && ep.current < 2.0,
            "bead_id={TEST_BEAD_ID} E_t should be near 1.0 after 10K clean observations, \
             got {:.6}",
            ep.current
        );
        assert!(
            !ep.rejected,
            "bead_id={TEST_BEAD_ID} should not reject under clean observations"
        );
    }

    // -- 2. test_eprocess_single_violation_jumps --

    #[test]
    fn test_eprocess_single_violation_jumps() {
        // Inject one violation. For INV-2 config (λ=0.999, p₀=1e-9):
        // factor = 1 + 0.999 * (1 - 1e-9) ≈ 1.999.
        // So E_t should jump by ~2x.
        let config = MvccInvariant::LockExclusivity.config();
        let mut ep = EProcess::new("INV-2:LockExclusivity", config);

        let before = ep.current;
        ep.observe(true); // violation
        let after = ep.current;

        let jump_factor = after / before;
        let expected_factor = 1.0 + 0.999 * (1.0 - 1e-9);

        assert!(
            (jump_factor - expected_factor).abs() < 1e-6,
            "bead_id={TEST_BEAD_ID} expected jump factor ~{expected_factor:.4}, \
             got {jump_factor:.6}"
        );
        assert!(
            jump_factor > 1.9 && jump_factor < 2.1,
            "bead_id={TEST_BEAD_ID} single violation should jump by ~2x, got {jump_factor:.4}"
        );
    }

    // -- 3. test_eprocess_repeated_violations_reject --

    #[test]
    fn test_eprocess_repeated_violations_reject() {
        // Inject violations at rate p₁=0.01 (10x above p₀=0.001 for INV-3 config).
        // Expected detection delay: N ~ log(1/α) / KL(p₁ || p₀).
        // KL(0.01 || 0.001) = 0.01 ln(0.01/0.001) + 0.99 ln(0.99/0.999) ≈ 0.02396
        // N ~ log(1000) / 0.02396 ≈ 288 observations.
        let config = EProcessConfig {
            p0: 0.001,
            lambda: 0.9,
            alpha: 0.001,
            max_evalue: 1e15,
        };
        let mut ep = EProcess::new("test_inv", config);

        // Deterministic violation pattern: violate every 100th observation (rate=0.01).
        let total_obs = 2000;
        for i in 0..total_obs {
            let violated = i % 100 == 0; // 1% violation rate
            ep.observe(violated);
        }

        assert!(
            ep.rejected,
            "bead_id={TEST_BEAD_ID} should reject after {total_obs} observations \
             with 1% violation rate (p₀=0.001), e_value={:.4}",
            ep.current
        );
        assert!(
            ep.rejection_time.is_some(),
            "bead_id={TEST_BEAD_ID} rejection_time should be set"
        );
    }

    // -- 4. test_eprocess_mixture_no_lambda_tuning --

    #[test]
    fn test_eprocess_mixture_no_lambda_tuning() {
        // Mixture e-process with 16 λ values detects violations across a range
        // of true rates without hand-tuning λ.
        let p0 = 0.001;
        let alpha = 0.001;

        // Test at several violation rates.
        let violation_rates = [0.005, 0.01, 0.05, 0.1];

        for &p1 in &violation_rates {
            let mut mixture = MixtureEProcess::new("test_mixture", p0, alpha, 16);

            // Deterministic: violate every (1/p1)-th observation.
            let period = (1.0 / p1) as usize;
            let total_obs = 5000;

            for i in 0..total_obs {
                let violated = i % period == 0;
                mixture.observe(violated);
            }

            assert!(
                mixture.rejected(alpha),
                "bead_id={TEST_BEAD_ID} mixture should detect violations at rate p₁={p1} \
                 within {total_obs} observations, e_value={:.4}",
                mixture.e_value()
            );
        }
    }

    #[test]
    fn test_mixture_eprocess_valid() {
        // Under H₀ (no violations), mixture must not reject.
        let p0 = 0.001;
        let alpha = 0.001;
        let mut under_null = MixtureEProcess::new("mixture_h0", p0, alpha, 16);
        for _ in 0..5_000 {
            under_null.observe(false);
        }
        assert!(
            !under_null.rejected(alpha),
            "bead_id={FRAMEWORK_BEAD_ID} mixture should not reject under H₀"
        );

        // Under sustained alternative (10%), rejection should happen quickly.
        let mut under_alt = MixtureEProcess::new("mixture_h1", p0, alpha, 16);
        for i in 0..500 {
            under_alt.observe(i % 10 == 0);
        }
        assert!(
            under_alt.rejected(alpha),
            "bead_id={FRAMEWORK_BEAD_ID} mixture should reject within 500 observations under H₁"
        );
    }

    // -- 5. test_evalue_arithmetic_mean_aggregation --

    #[test]
    fn test_evalue_arithmetic_mean_aggregation() {
        // 7 monitors with equal weights w_i=1/7.
        // Inject violation into INV-3 only.
        // Verify E_global grows while other individual E_i stay near 1.0.
        let mut monitor = MvccEProcessMonitor::new();

        for i in 0..500 {
            // Only INV-3 gets violations (every 50th observation = 2% rate).
            let inv3_violated = i % 50 == 0;
            for &inv in MvccInvariant::ALL {
                let violated = inv == MvccInvariant::VersionChainOrder && inv3_violated;
                monitor.observe(inv, violated);
            }
        }

        // INV-3 should have grown significantly.
        let inv3 = monitor.process(MvccInvariant::VersionChainOrder).unwrap();
        assert!(
            inv3.current > 10.0,
            "bead_id={TEST_BEAD_ID} INV-3 e-value should grow with violations, got {:.4}",
            inv3.current
        );

        // Other invariants should stay near 1.0.
        for &inv in MvccInvariant::ALL {
            if inv == MvccInvariant::VersionChainOrder {
                continue;
            }
            let ep = monitor.process(inv).unwrap();
            assert!(
                ep.current > 0.5 && ep.current < 2.0,
                "bead_id={TEST_BEAD_ID} {}: e-value should stay near 1.0 without violations, \
                 got {:.6}",
                inv.name(),
                ep.current
            );
        }

        // Global e-value should grow (dragged up by INV-3).
        let global = monitor.global_e_value();
        assert!(
            global > 1.0,
            "bead_id={TEST_BEAD_ID} global e-value should grow when INV-3 is violated, \
             got {global:.4}"
        );
    }

    // -- 6. test_evalue_global_rejects_on_single_invariant_failure --

    #[test]
    fn test_evalue_global_rejects_on_single_invariant_failure() {
        // With alpha_total=0.001, persistent violations in any single invariant
        // eventually cause E_global >= 1/alpha_total = 1000.
        let mut monitor = MvccEProcessMonitor::with_alpha_global(0.001);

        // Persistent violations in INV-2 (LockExclusivity) at high rate.
        for i in 0..5000 {
            let inv2_violated = i % 20 == 0; // 5% rate, well above p₀=1e-9
            for &inv in MvccInvariant::ALL {
                let violated = inv == MvccInvariant::LockExclusivity && inv2_violated;
                monitor.observe(inv, violated);
            }
        }

        assert!(
            monitor.global_rejected(),
            "bead_id={TEST_BEAD_ID} global e-value should reject when INV-2 has persistent \
             violations, global_e_value={:.4}, threshold={}",
            monitor.global_e_value(),
            1.0 / monitor.alpha_global()
        );

        // INV-2 should definitely be individually rejected too.
        assert!(
            monitor
                .rejected_invariants()
                .contains(&MvccInvariant::LockExclusivity),
            "bead_id={TEST_BEAD_ID} INV-2 should be individually rejected"
        );
    }

    // -- 7. test_evalue_no_false_alarm_under_null --

    #[test]
    fn test_evalue_no_false_alarm_under_null() {
        // Run all 7 monitors for 100K steps under H₀ (no violations).
        // Verify E_global never exceeds 1/alpha_total.
        let mut monitor = MvccEProcessMonitor::with_alpha_global(0.001);
        let threshold = 1.0 / monitor.alpha_global();

        for _ in 0..100_000 {
            for &inv in MvccInvariant::ALL {
                monitor.observe(inv, false);
            }

            // Check at every step that global e-value hasn't crossed.
            assert!(
                monitor.global_e_value() < threshold,
                "bead_id={TEST_BEAD_ID} false alarm: global e-value {:.4} >= threshold {threshold} \
                 under null hypothesis",
                monitor.global_e_value()
            );
        }

        // No individual invariant should have rejected either.
        assert!(
            !monitor.any_rejected(),
            "bead_id={TEST_BEAD_ID} no individual invariant should reject under H₀"
        );
    }

    // -- Calibration validation --

    #[test]
    fn test_per_invariant_calibration_matches_spec() {
        // Verify each MVCC monitor's (p₀, λ, α) matches the spec values.
        let cases: &[(MvccInvariant, f64, f64, f64)] = &[
            (MvccInvariant::Monotonicity, 1e-9, 0.999, 1e-6),
            (MvccInvariant::LockExclusivity, 1e-9, 0.999, 1e-6),
            (MvccInvariant::VersionChainOrder, 1e-6, 0.9, 0.001),
            (MvccInvariant::WriteSetConsistency, 1e-6, 0.9, 0.001),
            (MvccInvariant::SnapshotStability, 1e-6, 0.9, 0.001),
            (MvccInvariant::CommitAtomicity, 1e-6, 0.9, 0.001),
            (MvccInvariant::SerializedModeExclusivity, 1e-9, 0.999, 1e-6),
        ];

        for &(inv, expected_p0, expected_lambda, expected_alpha) in cases {
            let config = inv.config();
            assert_eq!(
                config.p0,
                expected_p0,
                "bead_id={TEST_BEAD_ID} {}: p0 mismatch",
                inv.name()
            );
            assert_eq!(
                config.lambda,
                expected_lambda,
                "bead_id={TEST_BEAD_ID} {}: lambda mismatch",
                inv.name()
            );
            assert_eq!(
                config.alpha,
                expected_alpha,
                "bead_id={TEST_BEAD_ID} {}: alpha mismatch",
                inv.name()
            );
            assert!(
                config.validate().is_ok(),
                "bead_id={TEST_BEAD_ID} {}: config validation failed: {}",
                inv.name(),
                config.validate().unwrap_err()
            );
        }
    }

    // -- Certificate tests --

    #[test]
    fn test_eprocess_certificate_on_rejection() {
        // When an e-process rejects, verify the certificate contains required fields.
        let mut monitor = MvccEProcessMonitor::new();

        // Inject rapid violations into INV-2.
        for _ in 0..100 {
            monitor.observe(MvccInvariant::LockExclusivity, true);
        }

        let cert = monitor
            .certificate_for(MvccInvariant::LockExclusivity)
            .expect("bead_id=bd-3go.3 INV-2 should be rejected and have a certificate");

        assert_eq!(cert.invariant, "INV-2:LockExclusivity");
        assert!(
            cert.e_value >= 1.0 / 1e-6,
            "e_value should exceed threshold"
        );
        assert!(
            (cert.threshold - 1e6).abs() < 1.0,
            "threshold should be 1/α = 1e6"
        );
        assert!(cert.observation_count > 0);
        assert!(cert.violations_observed > 0);
        assert!(cert.empirical_rate > 0.0);
    }

    // -- Monitor results --

    #[test]
    fn test_monitor_results_cover_all_invariants() {
        let monitor = MvccEProcessMonitor::new();
        let results = monitor.results();

        assert_eq!(
            results.len(),
            7,
            "bead_id={TEST_BEAD_ID} should have 7 invariant results"
        );

        for (i, result) in results.iter().enumerate() {
            assert_eq!(result.invariant, MvccInvariant::ALL[i]);
            assert!((result.e_value - 1.0).abs() < f64::EPSILON);
            assert!(!result.rejected);
            assert_eq!(result.observations, 0);
        }
    }

    // -- Detection delay --

    #[test]
    fn test_detection_delay_formula() {
        // For INV-3 (p₀=1e-6, λ=0.9, α=0.001) with p₁=0.01:
        // KL(0.01 || 1e-6) ≈ 0.01·ln(10000) + 0.99·ln(0.99/0.999999) ≈ 0.0921
        // N_detect ~ log(1/0.001) / KL ≈ 6.9 / 0.0921 ≈ 74.9
        // Allow 2x slack: empirical detection should be < 150.
        let config = MvccInvariant::VersionChainOrder.config();
        let mut ep = EProcess::new("INV-3", config);

        let period = 100; // 1% violation rate
        for i in 0..10_000 {
            let violated = i % period == 0;
            ep.observe(violated);
            if ep.rejected {
                break;
            }
        }

        assert!(
            ep.rejected,
            "bead_id={TEST_BEAD_ID} INV-3 should reject with 1% violations"
        );
        let detect_time = ep.rejection_time.unwrap();
        // With λ=0.9, p₀=1e-6: violation factor ≈ 1.9, need 1.9^n > 1/α = 1000.
        // n = ceil(log(1000)/log(1.9)) = 11 violations → ~1100 observations.
        // Allow 2x slack for the deterministic violation pattern.
        assert!(
            detect_time < 2500,
            "bead_id={TEST_BEAD_ID} detection delay {detect_time} exceeds 2x empirical bound"
        );
    }

    // -- MvccInvariant enum coverage --

    #[test]
    fn test_mvcc_invariant_all_has_seven_variants() {
        assert_eq!(MvccInvariant::ALL.len(), 7);
        for (i, inv) in MvccInvariant::ALL.iter().enumerate() {
            assert_eq!(inv.number() as usize, i + 1);
        }
    }

    #[test]
    fn test_mvcc_invariant_display() {
        assert_eq!(
            MvccInvariant::Monotonicity.to_string(),
            "INV-1:Monotonicity"
        );
        assert_eq!(
            MvccInvariant::SerializedModeExclusivity.to_string(),
            "INV-7:SerializedModeExclusivity"
        );
    }

    // -- Mixture e-process grid --

    #[test]
    fn test_mixture_eprocess_grid_size() {
        let mixture = MixtureEProcess::new("test", 0.001, 0.05, 16);
        assert_eq!(mixture.grid_size(), 16);
        assert_eq!(mixture.observations(), 0);
    }

    // -- Default trait --

    #[test]
    fn test_monitor_default_is_new() {
        let a = MvccEProcessMonitor::new();
        let b = MvccEProcessMonitor::default();
        assert_eq!(a.processes.len(), b.processes.len());
        assert!((a.alpha_global - b.alpha_global).abs() < f64::EPSILON);
    }

    // -- bd-3q2k: monitor factory + lock observation --

    #[test]
    fn test_create_mvcc_monitors_returns_eight() {
        let monitors = create_mvcc_monitors();
        assert_eq!(
            monitors.len(),
            8,
            "bead_id={MONITOR_BEAD_ID} factory should include INV-1..INV-7 + INV-SSI-FP"
        );
        assert_eq!(monitors[0].invariant, "INV-1:Monotonicity");
        assert_eq!(monitors[7].invariant, "INV-SSI-FP:FalsePositiveRate");
    }

    #[test]
    fn test_inv2_lock_exclusivity_no_violation() {
        let lock_table = HashMap::from([(1_u32, 10_u64), (2_u32, 11_u64)]);
        let active = HashMap::from([
            (
                10_u64,
                ActiveTxnInfo {
                    state: TxnState::Active,
                    page_locks: vec![1],
                },
            ),
            (
                11_u64,
                ActiveTxnInfo {
                    state: TxnState::Active,
                    page_locks: vec![2],
                },
            ),
        ]);

        assert!(
            !observe_lock_exclusivity(&lock_table, &active),
            "bead_id={MONITOR_BEAD_ID} disjoint single-holder pages must not violate INV-2"
        );
    }

    #[test]
    fn test_inv2_lock_exclusivity_dual_holder() {
        let lock_table = HashMap::from([(7_u32, 10_u64)]);
        let active = HashMap::from([
            (
                10_u64,
                ActiveTxnInfo {
                    state: TxnState::Active,
                    page_locks: vec![7],
                },
            ),
            (
                11_u64,
                ActiveTxnInfo {
                    state: TxnState::Active,
                    page_locks: vec![7],
                },
            ),
        ]);

        assert!(
            observe_lock_exclusivity(&lock_table, &active),
            "bead_id={MONITOR_BEAD_ID} dual holders on same page must violate INV-2"
        );
    }

    #[test]
    fn test_inv2_ghost_lock_detection() {
        let lock_table = HashMap::from([(5_u32, 999_u64)]);
        let active = HashMap::from([(
            10_u64,
            ActiveTxnInfo {
                state: TxnState::Active,
                page_locks: vec![10],
            },
        )]);

        assert!(
            observe_lock_exclusivity(&lock_table, &active),
            "bead_id={MONITOR_BEAD_ID} unknown lock holder must violate INV-2"
        );
    }

    #[test]
    fn test_inv2_lock_table_txn_disagreement() {
        let lock_table = HashMap::from([(42_u32, 10_u64)]);
        let active = HashMap::from([(
            10_u64,
            ActiveTxnInfo {
                state: TxnState::Active,
                page_locks: vec![],
            },
        )]);

        assert!(
            observe_lock_exclusivity(&lock_table, &active),
            "bead_id={MONITOR_BEAD_ID} lock-table/txn disagreement must violate INV-2"
        );
    }

    #[test]
    fn test_inv2_inactive_txn_holding_lock_detected() {
        let lock_table = HashMap::from([(9_u32, 10_u64)]);
        let active = HashMap::from([(
            10_u64,
            ActiveTxnInfo {
                state: TxnState::Committed,
                page_locks: vec![9],
            },
        )]);

        assert!(
            observe_lock_exclusivity(&lock_table, &active),
            "bead_id={MONITOR_BEAD_ID} inactive txn holding lock must be flagged"
        );
    }

    #[test]
    fn test_catastrophic_invariants_have_strict_config() {
        for inv in [
            MvccInvariant::Monotonicity,
            MvccInvariant::LockExclusivity,
            MvccInvariant::SerializedModeExclusivity,
        ] {
            let config = inv.config();
            assert!(
                config.p0 <= 1e-9,
                "bead_id={MONITOR_BEAD_ID} {} must use strict p0",
                inv.name()
            );
            assert!(
                config.lambda >= 0.999,
                "bead_id={MONITOR_BEAD_ID} {} must use aggressive lambda",
                inv.name()
            );
            assert!(
                config.alpha <= 1e-6,
                "bead_id={MONITOR_BEAD_ID} {} must use strict alpha",
                inv.name()
            );
        }
    }

    #[test]
    fn test_moderate_invariants_have_moderate_config() {
        for inv in [
            MvccInvariant::VersionChainOrder,
            MvccInvariant::WriteSetConsistency,
            MvccInvariant::SnapshotStability,
            MvccInvariant::CommitAtomicity,
        ] {
            let config = inv.config();
            assert_eq!(
                config.p0,
                1e-6,
                "bead_id={MONITOR_BEAD_ID} {} p0 mismatch",
                inv.name()
            );
            assert_eq!(
                config.lambda,
                0.9,
                "bead_id={MONITOR_BEAD_ID} {} lambda mismatch",
                inv.name()
            );
            assert_eq!(
                config.alpha,
                0.001,
                "bead_id={MONITOR_BEAD_ID} {} alpha mismatch",
                inv.name()
            );
        }
    }

    #[test]
    fn test_inv_ssi_fp_calibration_differs() {
        let config = MvccInvariant::SsiFalsePositiveRate.config();
        assert!(
            config.p0 > 0.001,
            "bead_id={MONITOR_BEAD_ID} INV-SSI-FP p0 must differ from hard-invariant calibration"
        );

        let mut near_baseline =
            EProcess::new(MvccInvariant::SsiFalsePositiveRate.name(), config.clone());
        for i in 0..10_000 {
            near_baseline.observe(i % 25 == 0); // 4%
        }
        assert!(
            !near_baseline.rejected,
            "bead_id={MONITOR_BEAD_ID} INV-SSI-FP should not reject near baseline (4%)"
        );

        let mut elevated = EProcess::new(MvccInvariant::SsiFalsePositiveRate.name(), config);
        for i in 0..500 {
            elevated.observe(i % 7 == 0); // ~14.3%
        }
        assert!(
            elevated.rejected,
            "bead_id={MONITOR_BEAD_ID} INV-SSI-FP should reject at elevated FP rate"
        );
    }

    #[test]
    fn test_e2e_invariant_monitors_catch_injected_violations() {
        let seed = 0xBADC_0DE_u64;

        // Baseline run: no violations for all monitors.
        let mut baseline = create_mvcc_monitors();
        for ep in &mut baseline {
            for _ in 0..2_000 {
                ep.observe(false);
            }
            assert!(
                !ep.rejected,
                "bead_id={MONITOR_BEAD_ID} baseline should not reject for {}",
                ep.invariant
            );
        }

        // Bug run: inject deterministic sustained violations for each monitor.
        let mut bug_run = create_mvcc_monitors();
        for ep in &mut bug_run {
            for i in 0..3_000 {
                // Deterministic replayable pattern (~14.3% violation rate).
                let violated = (i + (seed as usize % 7)) % 7 == 0;
                ep.observe(violated);
            }

            assert!(
                ep.rejected,
                "bead_id={MONITOR_BEAD_ID} injected bug run should reject for {}",
                ep.invariant
            );

            let evidence = serde_json::json!({
                "seed": seed,
                "invariant": ep.invariant,
                "e_value": ep.current,
                "threshold": ep.config.threshold(),
                "observations": ep.observations
            });
            assert_eq!(evidence["seed"], seed);
            assert!(
                evidence["e_value"].as_f64().unwrap_or(0.0)
                    >= evidence["threshold"].as_f64().unwrap_or(f64::INFINITY),
                "bead_id={MONITOR_BEAD_ID} evidence entry must reflect rejection threshold crossing"
            );
        }
    }

    // -- bd-x1ww: Initial state --

    #[test]
    fn test_initial_state() {
        let config = MvccInvariant::Monotonicity.config();
        let ep = EProcess::new("INV-1:Monotonicity", config);

        assert!(
            (ep.current - 1.0).abs() < f64::EPSILON,
            "bead_id={TEST_BEAD_ID} initial e_value must be 1.0"
        );
        assert_eq!(ep.observations, 0);
        assert!(!ep.rejected);
        assert!(ep.rejection_time.is_none());
        assert_eq!(ep.violations_observed, 0);
    }

    // -- bd-x1ww: Lambda constraint enforcement --

    #[test]
    #[should_panic(expected = "EProcessConfig validation failed")]
    fn test_lambda_constraint_enforced() {
        // For p0=0.001, valid lambda range is (-1/(1-0.001), 1/0.001) = (-1.001, 1000).
        // Lambda=1000 is OUT of range (must be strictly less than 1/p0).
        let config = EProcessConfig {
            p0: 0.001,
            lambda: 1000.0, // exactly 1/p0, which is out of range
            alpha: 0.05,
            max_evalue: 1e15,
        };
        let _ = EProcess::new("bad_lambda", config);
    }

    // -- bd-x1ww: Log-space stability --

    #[test]
    fn test_log_space_stability() {
        // Feed 100K alternating observations. Assert no NaN, Inf, or negative e-values.
        let config = MvccInvariant::VersionChainOrder.config();
        let mut ep = EProcess::new_without_history("stability_test", config);

        for i in 0..100_000 {
            let violated = i % 2 == 0; // alternating
            ep.observe(violated);

            assert!(
                ep.current.is_finite() && ep.current >= 0.0,
                "bead_id={TEST_BEAD_ID} e-value must be finite and non-negative at obs {i}, \
                 got {}",
                ep.current
            );
        }
    }

    // -- bd-x1ww: Max e-value cap --

    #[test]
    fn test_max_evalue_cap() {
        // With max_evalue=1e15 (default), continuous violations should clamp.
        let config = EProcessConfig {
            p0: 1e-9,
            lambda: 0.999,
            alpha: 1e-6,
            max_evalue: 1e15,
        };
        let mut ep = EProcess::new_without_history("cap_test", config);

        // Feed enough violations to drive e-value past the cap.
        for _ in 0..1000 {
            ep.observe(true);
        }

        assert!(
            ep.current <= 1e15,
            "bead_id={TEST_BEAD_ID} e-value should be capped at max_evalue=1e15, got {}",
            ep.current
        );
        assert!(
            ep.current.is_finite(),
            "bead_id={TEST_BEAD_ID} e-value must not overflow to infinity"
        );
        assert!(ep.rejected, "should have rejected");
    }

    // -- bd-x1ww: Correlated aggregation --

    #[test]
    fn test_evalue_aggregation_rejects_correlated() {
        // All monitors see the same violations (correlated). E_global should reject
        // and all invariants should be identified as contributors.
        let mut monitor = MvccEProcessMonitor::with_alpha_global(0.001);

        for i in 0..2000 {
            let violated = i % 50 == 0; // 2% rate, above all p₀ values
            for &inv in MvccInvariant::ALL {
                monitor.observe(inv, violated);
            }
        }

        assert!(
            monitor.global_rejected(),
            "bead_id={TEST_BEAD_ID} global e-value should reject under correlated violations, \
             global_e_value={:.4}",
            monitor.global_e_value()
        );

        // All invariants should be individually rejected too (correlated exposure).
        let rejected = monitor.rejected_invariants();
        assert_eq!(
            rejected.len(),
            7,
            "bead_id={TEST_BEAD_ID} all 7 invariants should be rejected under correlated \
             violations, got {}",
            rejected.len()
        );
    }

    // -- bd-x1ww: Alpha budget union bound --

    #[test]
    fn test_alpha_budget_union_bound() {
        // 7 monitors with sum(alpha_i) = 0.01 (via per-invariant alpha).
        // Under null (no violations), 100K observations, no false rejections.
        // We use the actual per-invariant configs which have alpha_i in {1e-6, 0.001}.
        // sum = 3*1e-6 + 4*0.001 = 0.004003, well under 0.01.
        let mut monitor = MvccEProcessMonitor::new();

        for _ in 0..100_000 {
            for &inv in MvccInvariant::ALL {
                monitor.observe(inv, false);
            }
        }

        assert!(
            !monitor.any_rejected(),
            "bead_id={TEST_BEAD_ID} no monitor should reject under null with union-bound alpha"
        );
    }

    // -- Property: supermartingale under H₀ --

    #[test]
    fn test_eprocess_supermartingale_empirical() {
        // For random observations under H₀ (X_t ~ Bernoulli(p₀)),
        // verify E[E_t | E_{t-1}] ≤ E_{t-1} empirically.
        // We use a deterministic pseudo-random sequence for reproducibility.
        let config = MvccInvariant::VersionChainOrder.config();
        let p0 = config.p0;

        // Simple LCG PRNG for deterministic test.
        let mut rng_state: u64 = 0xDEAD_BEEF;
        let lcg_next = |state: &mut u64| -> f64 {
            *state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1);
            (*state >> 33) as f64 / (1u64 << 31) as f64
        };

        let num_sequences = 200;
        let seq_length = 500;
        let mut final_e_values = Vec::with_capacity(num_sequences);

        for _ in 0..num_sequences {
            let mut ep = EProcess::new_without_history("test", config.clone());
            for _ in 0..seq_length {
                let violated = lcg_next(&mut rng_state) < p0;
                ep.observe(violated);
            }
            final_e_values.push(ep.current);
        }

        // Under H₀, E[E_t] = E₀ = 1.0 (martingale property).
        // The empirical mean should be close to 1.0.
        let mean: f64 = final_e_values.iter().sum::<f64>() / final_e_values.len() as f64;
        assert!(
            mean < 2.0,
            "bead_id={TEST_BEAD_ID} mean of final e-values under H₀ should be near 1.0, \
             got {mean:.4} (supermartingale property)"
        );
    }

    #[test]
    fn test_e2e_eprocess_monitor_trips_on_injected_violations() {
        let mut monitor = MvccEProcessMonitor::new();

        // H0-like phase: no violations observed.
        for _ in 0..2_000 {
            for &inv in MvccInvariant::ALL {
                monitor.observe(inv, false);
            }
        }
        assert!(
            !monitor.any_rejected(),
            "bead_id={FRAMEWORK_BEAD_ID} should not reject under H₀-like stream"
        );

        // Inject sustained violations for INV-2.
        for i in 0..3_000 {
            monitor.observe(MvccInvariant::LockExclusivity, i % 15 == 0);
            for &inv in MvccInvariant::ALL {
                if inv != MvccInvariant::LockExclusivity {
                    monitor.observe(inv, false);
                }
            }
        }

        assert!(
            monitor
                .rejected_invariants()
                .contains(&MvccInvariant::LockExclusivity),
            "bead_id={FRAMEWORK_BEAD_ID} INV-2 should reject under injected violations"
        );

        let cert = monitor
            .certificate_for(MvccInvariant::LockExclusivity)
            .expect("certificate expected after rejection");
        assert!(
            cert.e_value >= cert.threshold,
            "bead_id={FRAMEWORK_BEAD_ID} certificate must contain rejecting e-value"
        );

        let evidence = monitor.evidence_entry(3);
        assert!(
            evidence
                .contributors
                .iter()
                .any(|c| c.invariant == MvccInvariant::LockExclusivity),
            "bead_id={FRAMEWORK_BEAD_ID} evidence entry should identify INV-2 contribution"
        );
    }

    // -- bd-1cx0 runtime invariant monitoring --

    #[test]
    fn test_inv1_monotonicity_violation_detected() {
        assert_debug_assert_behavior(|| {
            let _ = check_inv1_monotonicity(100, 100);
        });
    }

    #[test]
    fn test_inv2_lock_exclusivity_violation_detected() {
        assert_debug_assert_behavior(|| {
            let _ = check_inv2_lock_exclusivity(2);
        });
    }

    #[test]
    fn test_inv3_version_chain_order_violation() {
        assert_debug_assert_behavior(|| {
            let _ = check_inv3_version_chain_order(1);
        });
    }

    #[test]
    fn test_inv4_unlocked_write_detected() {
        assert_debug_assert_behavior(|| {
            let _ = check_inv4_write_set_consistency(1);
        });
    }

    #[test]
    fn test_inv5_snapshot_stability_mutation() {
        assert_debug_assert_behavior(|| {
            let _ = check_inv5_snapshot_stability(1);
        });
    }

    #[test]
    fn test_inv6_partial_visibility_detected() {
        assert_debug_assert_behavior(|| {
            let _ = check_inv6_commit_atomicity(1);
        });
    }

    #[test]
    fn test_inv7_concurrent_serialized_writers_detected() {
        assert_debug_assert_behavior(|| {
            let _ = check_inv7_serialized_mode_exclusivity(2);
        });
    }

    #[test]
    fn test_inv1_through_inv7_100_threads() {
        let handles = (0..100)
            .map(|tid| {
                std::thread::spawn(move || {
                    let base = 10_000_u64 * (tid as u64 + 1);
                    let sample = HardInvariantSample {
                        prev_txn_id: base,
                        current_txn_id: base + 1,
                        max_concurrent_holders_per_page: 1,
                        chain_order_violations_per_1k: 0,
                        unlocked_writes_per_1k: 0,
                        snapshot_mutation_events_per_txn: 0,
                        partial_visibility_observations: 0,
                        concurrent_serialized_writers: 1,
                    };
                    for _ in 0..1_000 {
                        assert!(check_hard_invariants(sample));
                    }
                })
            })
            .collect::<Vec<_>>();

        for handle in handles {
            handle
                .join()
                .expect("bead_id=bd-1cx0 hard-invariant stress thread should not panic");
        }
    }

    #[test]
    fn test_inv_ssi_fp_eprocess_normal_rate() {
        let mut monitor = RuntimeInvariantMonitor::new();
        for i in 0..20_000 {
            monitor.observe_ssi_false_positive(i % 25 == 0); // 4% < 5% baseline
        }
        assert!(
            !monitor.ssi_fp_state().rejected,
            "bead_id={RUNTIME_BEAD_ID} SSI-FP should remain below rejection threshold at normal rate"
        );
    }

    #[test]
    fn test_inv_ssi_fp_eprocess_elevated_rate() {
        let mut monitor = RuntimeInvariantMonitor::new();
        for i in 0..3_000 {
            monitor.observe_ssi_false_positive(i % 6 == 0); // ~16.7%
        }
        assert!(
            monitor.ssi_fp_state().rejected,
            "bead_id={RUNTIME_BEAD_ID} SSI-FP should reject under elevated false-positive rate"
        );
        assert!(
            monitor.ssi_fp_state().current >= 100.0,
            "bead_id={RUNTIME_BEAD_ID} threshold should be 1/alpha = 100"
        );
    }

    #[test]
    fn test_debug_assert_zero_overhead() {
        // In release builds debug assertions compile out; in debug builds they trap.
        let panicked = panic::catch_unwind(AssertUnwindSafe(|| {
            let _ = check_inv2_lock_exclusivity(2);
        }))
        .is_err();
        assert_eq!(
            panicked,
            cfg!(debug_assertions),
            "bead_id={RUNTIME_BEAD_ID} debug_assert behavior should match build profile"
        );
    }

    #[test]
    fn test_eprocess_not_used_for_hard_invariants() {
        let monitor = RuntimeInvariantMonitor::new();
        for &inv in MvccInvariant::ALL {
            assert!(
                !monitor.uses_eprocess_for(inv),
                "bead_id={RUNTIME_BEAD_ID} hard invariant {} should not use e-process runtime checks",
                inv.name()
            );
        }
        assert!(
            monitor.uses_eprocess_for(MvccInvariant::SsiFalsePositiveRate),
            "bead_id={RUNTIME_BEAD_ID} INV-SSI-FP should use e-process runtime checks"
        );
    }

    #[test]
    fn test_e2e_bd_1cx0() {
        let mut monitor = RuntimeInvariantMonitor::new();

        // Baseline phase: clean hard-invariant samples + low SSI-FP.
        for i in 0..5_000 {
            let sample = HardInvariantSample {
                prev_txn_id: i + 1,
                current_txn_id: i + 2,
                max_concurrent_holders_per_page: 1,
                chain_order_violations_per_1k: 0,
                unlocked_writes_per_1k: 0,
                snapshot_mutation_events_per_txn: 0,
                partial_visibility_observations: 0,
                concurrent_serialized_writers: 1,
            };
            assert!(monitor.check_hard_sample(sample));
            monitor.observe_ssi_false_positive(i % 50 == 0); // 2%
        }
        assert!(
            !monitor.ssi_fp_state().rejected,
            "bead_id={RUNTIME_BEAD_ID} baseline phase should not reject"
        );

        // Inject a deliberate hard-invariant violation.
        assert_debug_assert_behavior(|| {
            let _ = monitor.check_hard_sample(HardInvariantSample {
                prev_txn_id: 42,
                current_txn_id: 42, // INV-1 violation
                max_concurrent_holders_per_page: 1,
                chain_order_violations_per_1k: 0,
                unlocked_writes_per_1k: 0,
                snapshot_mutation_events_per_txn: 0,
                partial_visibility_observations: 0,
                concurrent_serialized_writers: 1,
            });
        });

        // Inject elevated SSI-FP regime.
        for i in 0..3_000 {
            monitor.observe_ssi_false_positive(i % 6 == 0); // ~16.7%
        }
        assert!(
            monitor.ssi_fp_state().rejected,
            "bead_id={RUNTIME_BEAD_ID} elevated regime should trigger SSI-FP rejection"
        );
    }
}
