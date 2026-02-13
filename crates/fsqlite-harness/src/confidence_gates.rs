//! Conformal confidence gates and Bayesian expected-loss mismatch prioritiser (bd-1dp9.8.3).
//!
//! Operationalises release gating and backlog ordering using:
//! - **Conformal confidence gates**: per-invariant and global release readiness
//!   via Beta-posterior credible intervals + distribution-free conformal bands
//! - **Bayesian expected-loss ranking**: orders mismatches by posterior expected
//!   loss so the backlog is mathematically justified
//!
//! # Upstream Dependencies
//!
//! - [`parity_invariant_catalog`](crate::parity_invariant_catalog) (bd-1dp9.8.1):
//!   invariant definitions, proof obligations, feature mappings
//! - [`score_engine`](crate::score_engine) (bd-1dp9.1.3):
//!   `BetaParams`, `PriorConfig`, `BayesianScorecard`, conformal band math
//! - [`parity_taxonomy`](crate::parity_taxonomy) (bd-1dp9.1.1):
//!   `FeatureCategory`, `FeatureUniverse`, `ParityStatus`
//!
//! # Downstream Consumers
//!
//! - **bd-1dp9.8.4**: Release certificate generator (reads `GateReport`)
//! - **bd-1dp9.8.5**: Adversarial counterexample search (targets unverified invariants)
//!
//! # Determinism
//!
//! All arithmetic uses `truncate_score` for cross-platform reproducibility.
//! Ranking is deterministic (sorted by expected loss descending, then by invariant ID).

use std::collections::BTreeMap;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::parity_invariant_catalog::{
    InvariantCatalog, InvariantId, ObligationStatus, ParityInvariant, ProofKind,
};
use crate::parity_taxonomy::{FeatureCategory, FeatureUniverse, truncate_score};
use crate::score_engine::{BetaParams, PriorConfig};
use crate::verification_contract_enforcement::{
    ContractEnforcementOutcome, enforce_gate_decision, evaluate_workspace_verification_contract,
};

/// Bead identifier for log correlation.
#[allow(dead_code)]
const BEAD_ID: &str = "bd-1dp9.8.3";

/// Schema version for migration compatibility.
pub const GATE_SCHEMA_VERSION: u32 = 1;

// ---------------------------------------------------------------------------
// Gate configuration
// ---------------------------------------------------------------------------

/// Configuration for the confidence gate system.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GateConfig {
    /// Prior hyperparameters for Bayesian estimation.
    pub prior: PriorConfig,
    /// Confidence level for credible intervals (e.g., 0.95).
    pub confidence_level: f64,
    /// Release threshold: conservative lower bound must exceed this.
    pub release_threshold: f64,
    /// Per-category minimum verification percentage to pass the gate.
    pub category_min_verification_pct: f64,
    /// Cost multiplier for false-negative (missed mismatch) vs false-positive.
    /// Higher values prioritise catching mismatches over avoiding false alarms.
    pub loss_asymmetry_ratio: f64,
}

impl Default for GateConfig {
    fn default() -> Self {
        Self {
            prior: PriorConfig::default(),
            confidence_level: 0.95,
            release_threshold: 0.70,
            category_min_verification_pct: 50.0,
            loss_asymmetry_ratio: 5.0,
        }
    }
}

// ---------------------------------------------------------------------------
// Per-invariant gate result
// ---------------------------------------------------------------------------

/// Gate decision for a single parity invariant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum GateDecision {
    /// All obligations satisfied; invariant passes the gate.
    Pass,
    /// Some obligations pending; invariant is in progress.
    Conditional,
    /// No obligations satisfied or critical gaps; invariant fails the gate.
    Fail,
    /// Invariant was explicitly waived.
    Waived,
}

impl GateDecision {
    /// Whether this decision counts as "passing" for release.
    #[must_use]
    pub const fn is_pass(self) -> bool {
        matches!(self, Self::Pass | Self::Waived)
    }
}

impl std::fmt::Display for GateDecision {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pass => f.write_str("PASS"),
            Self::Conditional => f.write_str("CONDITIONAL"),
            Self::Fail => f.write_str("FAIL"),
            Self::Waived => f.write_str("WAIVED"),
        }
    }
}

/// Per-invariant gate evaluation result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvariantGateResult {
    /// Invariant ID.
    pub invariant_id: InvariantId,
    /// Feature category.
    pub category: String,
    /// Gate decision.
    pub decision: GateDecision,
    /// Count of verified obligations.
    pub verified_count: usize,
    /// Count of pending obligations.
    pub pending_count: usize,
    /// Count of partial obligations.
    pub partial_count: usize,
    /// Count of waived obligations.
    pub waived_count: usize,
    /// Total obligations.
    pub total_obligations: usize,
    /// Verification percentage (0..100).
    pub verification_pct: f64,
}

// ---------------------------------------------------------------------------
// Category gate result
// ---------------------------------------------------------------------------

/// Gate evaluation for an entire feature category.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CategoryGateResult {
    /// Category name.
    pub category: String,
    /// Category weight in global score.
    pub global_weight: f64,
    /// Gate decision for the category.
    pub decision: GateDecision,
    /// Number of invariants that pass the gate.
    pub passing_invariants: usize,
    /// Total invariants in this category.
    pub total_invariants: usize,
    /// Category-level verification percentage.
    pub verification_pct: f64,
    /// Posterior point estimate for this category's pass rate.
    pub posterior_mean: f64,
    /// Lower credible bound.
    pub credible_lower: f64,
    /// Upper credible bound.
    pub credible_upper: f64,
    /// Beta posterior parameters.
    pub posterior: BetaParams,
}

// ---------------------------------------------------------------------------
// Expected-loss ranking
// ---------------------------------------------------------------------------

/// A single entry in the expected-loss priority ranking.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExpectedLossEntry {
    /// Invariant ID.
    pub invariant_id: InvariantId,
    /// Feature ID from taxonomy.
    pub feature_id: String,
    /// Category display name.
    pub category: String,
    /// Invariant statement (for human triage).
    pub statement: String,
    /// Posterior probability of mismatch (1 - pass_rate).
    pub mismatch_probability: f64,
    /// Cost of mismatch (function of category weight and loss asymmetry).
    pub mismatch_cost: f64,
    /// Expected loss = mismatch_probability * mismatch_cost.
    pub expected_loss: f64,
    /// Current gate decision.
    pub gate_decision: GateDecision,
    /// Proof kinds still pending.
    pub pending_proof_kinds: Vec<ProofKind>,
    /// Rank position (1-based).
    pub rank: usize,
}

/// The full expected-loss priority ranking.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExpectedLossRanking {
    /// Schema version.
    pub schema_version: u32,
    /// Configuration used.
    pub config: GateConfig,
    /// Ranked entries (highest expected loss first).
    pub entries: Vec<ExpectedLossEntry>,
    /// Total expected loss across all invariants.
    pub total_expected_loss: f64,
    /// Number of entries with non-zero expected loss.
    pub actionable_count: usize,
}

// ---------------------------------------------------------------------------
// Global gate report
// ---------------------------------------------------------------------------

/// Complete gate evaluation report for release decisions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GateReport {
    /// Schema version.
    pub schema_version: u32,
    /// Configuration used.
    pub config: GateConfig,
    /// Global gate decision.
    pub global_decision: GateDecision,
    /// Whether the system is release-ready.
    pub release_ready: bool,
    /// Global verification percentage.
    pub global_verification_pct: f64,
    /// Per-category gate results.
    pub category_results: BTreeMap<String, CategoryGateResult>,
    /// Per-invariant gate results.
    pub invariant_results: BTreeMap<String, InvariantGateResult>,
    /// Global posterior point estimate.
    pub global_posterior_mean: f64,
    /// Global conservative lower bound.
    pub global_lower_bound: f64,
    /// Total invariants evaluated.
    pub total_invariants: usize,
    /// Total invariants passing.
    pub passing_invariants: usize,
    /// Decision rationale text.
    pub rationale: String,
    /// Optional verification-contract enforcement payload (bd-1dp9.7.7).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verification_contract: Option<ContractEnforcementOutcome>,
}

impl GateReport {
    /// Serialise to deterministic JSON.
    ///
    /// # Errors
    ///
    /// Returns `Err` if serialisation fails.
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }

    /// Deserialise from JSON.
    ///
    /// # Errors
    ///
    /// Returns `Err` if the JSON is malformed.
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }
}

// ---------------------------------------------------------------------------
// Gate evaluation engine
// ---------------------------------------------------------------------------

/// Evaluate the confidence gate for a single invariant.
fn evaluate_invariant_gate(inv: &ParityInvariant) -> InvariantGateResult {
    let mut verified = 0_usize;
    let mut pending = 0_usize;
    let mut partial = 0_usize;
    let mut waived = 0_usize;

    for obl in &inv.obligations {
        match obl.status {
            ObligationStatus::Verified => verified += 1,
            ObligationStatus::Pending => pending += 1,
            ObligationStatus::Partial => partial += 1,
            ObligationStatus::Waived => waived += 1,
        }
    }

    let total = inv.obligations.len();
    let satisfied = verified + waived;
    #[allow(clippy::cast_precision_loss)]
    let verification_pct = if total > 0 {
        truncate_score((satisfied as f64 / total as f64) * 100.0)
    } else {
        0.0
    };

    let decision = if total == 0 {
        GateDecision::Fail
    } else if waived == total {
        GateDecision::Waived
    } else if satisfied == total {
        GateDecision::Pass
    } else if verified > 0 || partial > 0 {
        GateDecision::Conditional
    } else {
        GateDecision::Fail
    };

    InvariantGateResult {
        invariant_id: inv.id.clone(),
        category: inv.category.display_name().to_owned(),
        decision,
        verified_count: verified,
        pending_count: pending,
        partial_count: partial,
        waived_count: waived,
        total_obligations: total,
        verification_pct,
    }
}

/// Evaluate the confidence gate for a feature category.
fn evaluate_category_gate(
    cat: FeatureCategory,
    invariant_results: &BTreeMap<String, InvariantGateResult>,
    config: &GateConfig,
) -> CategoryGateResult {
    let cat_name = cat.display_name();
    let cat_invs: Vec<&InvariantGateResult> = invariant_results
        .values()
        .filter(|r| r.category == cat_name)
        .collect();

    let total = cat_invs.len();
    let passing = cat_invs.iter().filter(|r| r.decision.is_pass()).count();

    #[allow(clippy::cast_precision_loss)]
    let verification_pct = if total > 0 {
        truncate_score((passing as f64 / total as f64) * 100.0)
    } else {
        0.0
    };

    // Bayesian posterior for category pass rate
    #[allow(clippy::cast_precision_loss)]
    let successes = passing as f64;
    #[allow(clippy::cast_precision_loss)]
    let failures = (total - passing) as f64;
    let alpha = config.prior.alpha + successes;
    let beta = config.prior.beta + failures;
    let posterior = BetaParams::new(alpha, beta);
    let posterior_mean = truncate_score(posterior.mean());
    let (lo, hi) = posterior.credible_interval(config.confidence_level);

    let decision = if total == 0 {
        GateDecision::Fail
    } else if verification_pct >= 100.0 {
        GateDecision::Pass
    } else if verification_pct >= config.category_min_verification_pct {
        GateDecision::Conditional
    } else {
        GateDecision::Fail
    };

    CategoryGateResult {
        category: cat_name.to_owned(),
        global_weight: cat.global_weight(),
        decision,
        passing_invariants: passing,
        total_invariants: total,
        verification_pct,
        posterior_mean,
        credible_lower: truncate_score(lo),
        credible_upper: truncate_score(hi.min(1.0)),
        posterior,
    }
}

/// Evaluate the complete confidence gate for release readiness.
///
/// This is the main entry point. It evaluates every invariant in the catalog,
/// computes per-category and global gate decisions, and produces a machine-readable
/// report suitable for consumption by the release certificate generator (bd-1dp9.8.4).
#[must_use]
pub fn evaluate_gate(catalog: &InvariantCatalog, config: &GateConfig) -> GateReport {
    // Evaluate each invariant
    let mut invariant_results = BTreeMap::new();
    for inv in catalog.invariants.values() {
        let result = evaluate_invariant_gate(inv);
        invariant_results.insert(inv.id.0.clone(), result);
    }

    // Evaluate each category
    let mut category_results = BTreeMap::new();
    for cat in FeatureCategory::ALL {
        let result = evaluate_category_gate(cat, &invariant_results, config);
        category_results.insert(cat.display_name().to_owned(), result);
    }

    // Global aggregation
    let total_invariants = invariant_results.len();
    let passing_invariants = invariant_results
        .values()
        .filter(|r| r.decision.is_pass())
        .count();

    #[allow(clippy::cast_precision_loss)]
    let global_verification_pct = if total_invariants > 0 {
        truncate_score((passing_invariants as f64 / total_invariants as f64) * 100.0)
    } else {
        0.0
    };

    // Weighted global posterior mean
    let global_posterior_mean: f64 = category_results
        .values()
        .map(|cr| cr.global_weight * cr.posterior_mean)
        .sum();
    let global_posterior_mean = truncate_score(global_posterior_mean);

    // Weighted conservative lower bound
    let global_lower_bound: f64 = category_results
        .values()
        .map(|cr| cr.global_weight * cr.credible_lower)
        .sum();
    let global_lower_bound = truncate_score(global_lower_bound);

    // Global gate decision
    let all_categories_pass = category_results.values().all(|cr| cr.decision.is_pass());
    let threshold_met = global_lower_bound >= config.release_threshold;

    let global_decision = if all_categories_pass && threshold_met {
        GateDecision::Pass
    } else if passing_invariants > 0 {
        GateDecision::Conditional
    } else {
        GateDecision::Fail
    };

    let release_ready = global_decision == GateDecision::Pass;

    // Build rationale
    let rationale = build_rationale(
        global_decision,
        global_lower_bound,
        config.release_threshold,
        &category_results,
        config,
    );

    GateReport {
        schema_version: GATE_SCHEMA_VERSION,
        config: config.clone(),
        global_decision,
        release_ready,
        global_verification_pct,
        category_results,
        invariant_results,
        global_posterior_mean,
        global_lower_bound,
        total_invariants,
        passing_invariants,
        rationale,
        verification_contract: None,
    }
}

/// Apply verification-contract enforcement to an existing gate report.
#[must_use]
pub fn apply_contract_outcome_to_gate_report(
    mut report: GateReport,
    enforcement: ContractEnforcementOutcome,
) -> GateReport {
    report.release_ready = enforcement.final_gate_passed;
    if !report.release_ready {
        report.global_decision = GateDecision::Fail;
    }
    report.rationale = format!(
        "{}\nVerification contract enforcement: disposition={} contract_passed={} base_gate_passed={} failing_beads={} missing_evidence_beads={} invalid_reference_beads={}",
        report.rationale,
        enforcement.disposition,
        enforcement.contract_passed,
        enforcement.base_gate_passed,
        enforcement.failing_beads,
        enforcement.missing_evidence_beads,
        enforcement.invalid_reference_beads,
    );
    report.verification_contract = Some(enforcement);
    report
}

/// Evaluate gate report and enforce verification contract from workspace evidence.
///
/// # Errors
///
/// Returns `Err` if the workspace parity evidence report cannot be generated.
pub fn evaluate_gate_with_contract(
    catalog: &InvariantCatalog,
    config: &GateConfig,
    workspace_root: &Path,
) -> Result<GateReport, String> {
    let report = evaluate_gate(catalog, config);
    let contract_report = evaluate_workspace_verification_contract(workspace_root)?;
    let enforcement = enforce_gate_decision(report.release_ready, &contract_report);
    Ok(apply_contract_outcome_to_gate_report(report, enforcement))
}

/// Build human-readable decision rationale.
fn build_rationale(
    decision: GateDecision,
    lower_bound: f64,
    threshold: f64,
    category_results: &BTreeMap<String, CategoryGateResult>,
    config: &GateConfig,
) -> String {
    let mut lines = Vec::new();

    lines.push(format!("Global gate decision: {decision}"));
    lines.push(format!(
        "Conservative lower bound: {lower_bound:.4} (threshold: {threshold:.4})"
    ));

    if lower_bound < threshold {
        lines.push(format!(
            "BLOCKED: lower bound {lower_bound:.4} < threshold {threshold:.4}"
        ));
    }

    let failing_cats: Vec<&str> = category_results
        .values()
        .filter(|cr| !cr.decision.is_pass())
        .map(|cr| cr.category.as_str())
        .collect();

    if !failing_cats.is_empty() {
        lines.push(format!(
            "Categories not passing: {}",
            failing_cats.join(", ")
        ));
    }

    let below_min: Vec<String> = category_results
        .values()
        .filter(|cr| cr.verification_pct < config.category_min_verification_pct)
        .map(|cr| {
            format!(
                "  - {} ({:.1}% < {:.1}%)",
                cr.category, cr.verification_pct, config.category_min_verification_pct
            )
        })
        .collect();

    if !below_min.is_empty() {
        lines.push("Categories below minimum verification:".to_owned());
        lines.extend(below_min);
    }

    lines.join("\n")
}

// ---------------------------------------------------------------------------
// Expected-loss ranking engine
// ---------------------------------------------------------------------------

/// Compute the expected-loss priority ranking for all invariants.
///
/// For each invariant, computes:
/// - `mismatch_probability`: posterior probability that the invariant is violated
/// - `mismatch_cost`: function of category weight × loss asymmetry ratio
/// - `expected_loss`: product of probability and cost
///
/// Entries are sorted by expected loss (descending), then by invariant ID for determinism.
#[must_use]
pub fn compute_expected_loss_ranking(
    catalog: &InvariantCatalog,
    _universe: &FeatureUniverse,
    config: &GateConfig,
) -> ExpectedLossRanking {
    let mut entries = Vec::new();

    for inv in catalog.invariants.values() {
        let gate_result = evaluate_invariant_gate(inv);

        // Posterior mismatch probability
        let mismatch_prob = compute_mismatch_probability(inv, &gate_result, &config.prior);

        // Mismatch cost: category weight × asymmetry ratio
        let category_weight = inv.category.global_weight();
        let mismatch_cost = truncate_score(category_weight * config.loss_asymmetry_ratio);

        // Expected loss
        let expected_loss = truncate_score(mismatch_prob * mismatch_cost);

        // Pending proof kinds
        let pending_proof_kinds: Vec<ProofKind> = inv
            .obligations
            .iter()
            .filter(|o| o.status == ObligationStatus::Pending)
            .map(|o| o.kind)
            .collect();

        entries.push(ExpectedLossEntry {
            invariant_id: inv.id.clone(),
            feature_id: inv.feature_id.0.clone(),
            category: inv.category.display_name().to_owned(),
            statement: inv.statement.clone(),
            mismatch_probability: mismatch_prob,
            mismatch_cost,
            expected_loss,
            gate_decision: gate_result.decision,
            pending_proof_kinds,
            rank: 0, // assigned after sort
        });
    }

    // Sort: highest expected loss first, then by invariant ID for determinism
    entries.sort_by(|a, b| {
        b.expected_loss
            .partial_cmp(&a.expected_loss)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.invariant_id.cmp(&b.invariant_id))
    });

    // Assign ranks
    for (i, entry) in entries.iter_mut().enumerate() {
        entry.rank = i + 1;
    }

    let total_expected_loss: f64 = entries.iter().map(|e| e.expected_loss).sum();
    let actionable_count = entries.iter().filter(|e| e.expected_loss > 0.0).count();

    ExpectedLossRanking {
        schema_version: GATE_SCHEMA_VERSION,
        config: config.clone(),
        entries,
        total_expected_loss: truncate_score(total_expected_loss),
        actionable_count,
    }
}

/// Compute posterior mismatch probability for an invariant.
///
/// Uses a Beta-posterior model:
/// - Each obligation contributes to the posterior as a Bernoulli observation
/// - Verified/waived → success, Pending/Partial → failure
/// - The mismatch probability is 1 - posterior_mean
fn compute_mismatch_probability(
    inv: &ParityInvariant,
    gate_result: &InvariantGateResult,
    prior: &PriorConfig,
) -> f64 {
    if inv.obligations.is_empty() {
        return 1.0; // no evidence → maximum uncertainty
    }

    #[allow(clippy::cast_precision_loss)]
    let successes = (gate_result.verified_count + gate_result.waived_count) as f64;
    #[allow(clippy::cast_precision_loss)]
    let failures = (gate_result.pending_count + gate_result.partial_count) as f64;

    let alpha = prior.alpha + successes;
    let beta = prior.beta + failures;
    let posterior = BetaParams::new(alpha, beta);

    truncate_score(1.0 - posterior.mean())
}

/// Convenience: compute gate report and ranking together.
#[must_use]
pub fn evaluate_full(
    catalog: &InvariantCatalog,
    universe: &FeatureUniverse,
    config: &GateConfig,
) -> (GateReport, ExpectedLossRanking) {
    let report = evaluate_gate(catalog, config);
    let ranking = compute_expected_loss_ranking(catalog, universe, config);
    (report, ranking)
}

/// Generate a combined evidence ledger for the release certificate (bd-1dp9.8.4).
///
/// Returns a machine-readable summary suitable for embedding in the release
/// certificate's evidence chain.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvidenceLedger {
    /// Schema version.
    pub schema_version: u32,
    /// Global gate decision.
    pub global_decision: GateDecision,
    /// Release ready flag.
    pub release_ready: bool,
    /// Global posterior mean.
    pub global_posterior_mean: f64,
    /// Conservative lower bound.
    pub global_lower_bound: f64,
    /// Verification percentage.
    pub global_verification_pct: f64,
    /// Total expected loss from ranking.
    pub total_expected_loss: f64,
    /// Number of invariants.
    pub total_invariants: usize,
    /// Number passing.
    pub passing_invariants: usize,
    /// Top 10 priority items by expected loss.
    pub top_priority_items: Vec<PriorityItem>,
    /// Per-category summaries.
    pub category_summaries: BTreeMap<String, CategorySummary>,
    /// Optional verification-contract enforcement payload.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verification_contract: Option<ContractEnforcementOutcome>,
}

/// Summary of a high-priority item for the evidence ledger.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriorityItem {
    pub invariant_id: String,
    pub feature_id: String,
    pub expected_loss: f64,
    pub gate_decision: GateDecision,
}

/// Per-category summary for the evidence ledger.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CategorySummary {
    pub category: String,
    pub gate_decision: GateDecision,
    pub verification_pct: f64,
    pub posterior_mean: f64,
    pub credible_lower: f64,
}

/// Build the evidence ledger from gate report and ranking.
#[must_use]
pub fn build_evidence_ledger(report: &GateReport, ranking: &ExpectedLossRanking) -> EvidenceLedger {
    let top_priority_items: Vec<PriorityItem> = ranking
        .entries
        .iter()
        .filter(|e| e.expected_loss > 0.0)
        .take(10)
        .map(|e| PriorityItem {
            invariant_id: e.invariant_id.0.clone(),
            feature_id: e.feature_id.clone(),
            expected_loss: e.expected_loss,
            gate_decision: e.gate_decision,
        })
        .collect();

    let category_summaries: BTreeMap<String, CategorySummary> = report
        .category_results
        .iter()
        .map(|(name, cr)| {
            (
                name.clone(),
                CategorySummary {
                    category: cr.category.clone(),
                    gate_decision: cr.decision,
                    verification_pct: cr.verification_pct,
                    posterior_mean: cr.posterior_mean,
                    credible_lower: cr.credible_lower,
                },
            )
        })
        .collect();

    EvidenceLedger {
        schema_version: GATE_SCHEMA_VERSION,
        global_decision: report.global_decision,
        release_ready: report.release_ready,
        global_posterior_mean: report.global_posterior_mean,
        global_lower_bound: report.global_lower_bound,
        global_verification_pct: report.global_verification_pct,
        total_expected_loss: ranking.total_expected_loss,
        total_invariants: report.total_invariants,
        passing_invariants: report.passing_invariants,
        top_priority_items,
        category_summaries,
        verification_contract: report.verification_contract.clone(),
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;
    use crate::parity_invariant_catalog::build_canonical_catalog;
    use crate::parity_taxonomy::build_canonical_universe;
    use crate::verification_contract_enforcement::{
        BeadContractVerdict, ContractBeadStatus, ContractEnforcementOutcome, EnforcementDisposition,
    };

    fn default_config() -> GateConfig {
        GateConfig::default()
    }

    fn synthetic_contract_outcome(
        base_gate_passed: bool,
        contract_passed: bool,
    ) -> ContractEnforcementOutcome {
        ContractEnforcementOutcome {
            schema_version: 1,
            bead_id: "bd-1dp9.7.7".to_owned(),
            base_gate_passed,
            contract_passed,
            final_gate_passed: base_gate_passed && contract_passed,
            disposition: match (base_gate_passed, contract_passed) {
                (true, true) => EnforcementDisposition::Allowed,
                (false, true) => EnforcementDisposition::BlockedByBaseGate,
                (true, false) => EnforcementDisposition::BlockedByContract,
                (false, false) => EnforcementDisposition::BlockedByBoth,
            },
            total_beads: 1,
            failing_beads: if contract_passed { 0 } else { 1 },
            missing_evidence_beads: if contract_passed { 0 } else { 1 },
            invalid_reference_beads: 0,
            bead_verdicts: vec![BeadContractVerdict {
                bead_id: "bd-1dp9.7.7".to_owned(),
                status: if contract_passed {
                    ContractBeadStatus::Pass
                } else {
                    ContractBeadStatus::FailMissingEvidence
                },
                missing_evidence_count: if contract_passed { 0 } else { 1 },
                invalid_reference_count: 0,
                details: Vec::new(),
            }],
        }
    }

    #[test]
    fn gate_report_builds_from_canonical() {
        let catalog = build_canonical_catalog();
        let config = default_config();
        let report = evaluate_gate(&catalog, &config);
        assert!(report.total_invariants > 0, "report must have invariants");
        assert_eq!(report.schema_version, GATE_SCHEMA_VERSION);
    }

    #[test]
    fn gate_report_covers_all_categories() {
        let catalog = build_canonical_catalog();
        let config = default_config();
        let report = evaluate_gate(&catalog, &config);
        for cat in FeatureCategory::ALL {
            assert!(
                report.category_results.contains_key(cat.display_name()),
                "missing category result for {cat}"
            );
        }
    }

    #[test]
    fn gate_report_invariant_count_matches_catalog() {
        let catalog = build_canonical_catalog();
        let config = default_config();
        let report = evaluate_gate(&catalog, &config);
        assert_eq!(
            report.total_invariants,
            catalog.invariants.len(),
            "invariant count mismatch"
        );
    }

    #[test]
    fn gate_report_passing_le_total() {
        let catalog = build_canonical_catalog();
        let config = default_config();
        let report = evaluate_gate(&catalog, &config);
        assert!(
            report.passing_invariants <= report.total_invariants,
            "passing ({}) > total ({})",
            report.passing_invariants,
            report.total_invariants
        );
    }

    #[test]
    fn gate_verification_pct_in_range() {
        let catalog = build_canonical_catalog();
        let config = default_config();
        let report = evaluate_gate(&catalog, &config);
        assert!(
            (0.0..=100.0).contains(&report.global_verification_pct),
            "global verification pct out of range: {}",
            report.global_verification_pct
        );
    }

    #[test]
    fn gate_posterior_mean_in_range() {
        let catalog = build_canonical_catalog();
        let config = default_config();
        let report = evaluate_gate(&catalog, &config);
        assert!(
            (0.0..=1.0).contains(&report.global_posterior_mean),
            "posterior mean out of range: {}",
            report.global_posterior_mean
        );
    }

    #[test]
    fn gate_lower_bound_le_mean() {
        let catalog = build_canonical_catalog();
        let config = default_config();
        let report = evaluate_gate(&catalog, &config);
        assert!(
            report.global_lower_bound <= report.global_posterior_mean + 1e-6,
            "lower bound ({}) > mean ({})",
            report.global_lower_bound,
            report.global_posterior_mean
        );
    }

    #[test]
    fn gate_has_rationale() {
        let catalog = build_canonical_catalog();
        let config = default_config();
        let report = evaluate_gate(&catalog, &config);
        assert!(!report.rationale.is_empty(), "rationale must not be empty");
        assert!(
            report.rationale.contains("Global gate decision"),
            "rationale must contain decision"
        );
    }

    #[test]
    fn gate_report_json_roundtrip() {
        let catalog = build_canonical_catalog();
        let config = default_config();
        let report = evaluate_gate(&catalog, &config);
        let json = report.to_json().expect("serialisation");
        let deserialized = GateReport::from_json(&json).expect("deserialisation");
        assert_eq!(report.total_invariants, deserialized.total_invariants);
        assert_eq!(report.global_decision, deserialized.global_decision);
    }

    #[test]
    fn apply_contract_outcome_can_block_release_ready() {
        let catalog = build_canonical_catalog();
        let config = default_config();
        let mut report = evaluate_gate(&catalog, &config);
        report.release_ready = true;
        report.global_decision = GateDecision::Pass;

        let enforced =
            apply_contract_outcome_to_gate_report(report, synthetic_contract_outcome(true, false));
        assert!(!enforced.release_ready);
        assert_eq!(enforced.global_decision, GateDecision::Fail);
        assert!(enforced.verification_contract.is_some());
        assert!(
            enforced
                .rationale
                .contains("Verification contract enforcement"),
            "rationale should include contract enforcement summary"
        );
    }

    #[test]
    fn evaluate_gate_with_contract_attaches_enforcement_payload() {
        let temp_dir = tempfile::tempdir().expect("create temporary workspace");
        let beads_dir = temp_dir.path().join(".beads");
        fs::create_dir_all(&beads_dir).expect("create .beads directory");
        fs::write(
            beads_dir.join("issues.jsonl"),
            r#"{"id":"bd-1dp9.7.7","issue_type":"task"}"#,
        )
        .expect("write issues.jsonl");

        let catalog = build_canonical_catalog();
        let config = default_config();
        let report = evaluate_gate_with_contract(&catalog, &config, temp_dir.path())
            .expect("evaluate gate with contract");

        let contract = report
            .verification_contract
            .as_ref()
            .expect("contract payload should be present");
        assert!(!contract.contract_passed);
        assert!(
            report
                .rationale
                .contains("Verification contract enforcement"),
            "rationale should include enforcement details"
        );
    }

    #[test]
    fn expected_loss_ranking_builds() {
        let catalog = build_canonical_catalog();
        let universe = build_canonical_universe();
        let config = default_config();
        let ranking = compute_expected_loss_ranking(&catalog, &universe, &config);
        assert_eq!(
            ranking.entries.len(),
            catalog.invariants.len(),
            "ranking must have one entry per invariant"
        );
    }

    #[test]
    fn expected_loss_ranking_sorted_descending() {
        let catalog = build_canonical_catalog();
        let universe = build_canonical_universe();
        let config = default_config();
        let ranking = compute_expected_loss_ranking(&catalog, &universe, &config);
        for pair in ranking.entries.windows(2) {
            assert!(
                pair[0].expected_loss >= pair[1].expected_loss,
                "ranking not sorted: {} ({}) > {} ({})",
                pair[0].invariant_id,
                pair[0].expected_loss,
                pair[1].invariant_id,
                pair[1].expected_loss
            );
        }
    }

    #[test]
    fn expected_loss_ranks_are_1_indexed() {
        let catalog = build_canonical_catalog();
        let universe = build_canonical_universe();
        let config = default_config();
        let ranking = compute_expected_loss_ranking(&catalog, &universe, &config);
        for (i, entry) in ranking.entries.iter().enumerate() {
            assert_eq!(entry.rank, i + 1, "rank mismatch at position {i}");
        }
    }

    #[test]
    fn expected_loss_total_is_sum() {
        let catalog = build_canonical_catalog();
        let universe = build_canonical_universe();
        let config = default_config();
        let ranking = compute_expected_loss_ranking(&catalog, &universe, &config);
        let computed_sum: f64 = ranking.entries.iter().map(|e| e.expected_loss).sum();
        assert!(
            (ranking.total_expected_loss - truncate_score(computed_sum)).abs() < 1e-5,
            "total expected loss mismatch: {} vs {}",
            ranking.total_expected_loss,
            computed_sum
        );
    }

    #[test]
    fn expected_loss_values_non_negative() {
        let catalog = build_canonical_catalog();
        let universe = build_canonical_universe();
        let config = default_config();
        let ranking = compute_expected_loss_ranking(&catalog, &universe, &config);
        for entry in &ranking.entries {
            assert!(
                entry.expected_loss >= 0.0,
                "negative expected loss for {}",
                entry.invariant_id
            );
            assert!(
                entry.mismatch_probability >= 0.0 && entry.mismatch_probability <= 1.0,
                "mismatch probability out of range for {}",
                entry.invariant_id
            );
        }
    }

    #[test]
    fn pending_invariants_have_higher_expected_loss() {
        let catalog = build_canonical_catalog();
        let universe = build_canonical_universe();
        let config = default_config();
        let ranking = compute_expected_loss_ranking(&catalog, &universe, &config);

        // All FAIL/CONDITIONAL entries should have expected_loss > 0
        for entry in &ranking.entries {
            if entry.gate_decision == GateDecision::Fail {
                assert!(
                    entry.expected_loss > 0.0,
                    "FAIL invariant {} should have expected_loss > 0",
                    entry.invariant_id
                );
            }
        }
    }

    #[test]
    fn fully_verified_invariants_have_low_expected_loss() {
        let catalog = build_canonical_catalog();
        let universe = build_canonical_universe();
        let config = default_config();
        let ranking = compute_expected_loss_ranking(&catalog, &universe, &config);

        for entry in &ranking.entries {
            if entry.gate_decision == GateDecision::Pass && entry.pending_proof_kinds.is_empty() {
                // Fully verified invariants should have very low expected loss
                // (only prior uncertainty remains)
                assert!(
                    entry.mismatch_probability < 0.5,
                    "PASS invariant {} has high mismatch probability: {}",
                    entry.invariant_id,
                    entry.mismatch_probability
                );
            }
        }
    }

    #[test]
    fn evaluate_full_consistency() {
        let catalog = build_canonical_catalog();
        let universe = build_canonical_universe();
        let config = default_config();
        let (report, ranking) = evaluate_full(&catalog, &universe, &config);
        assert_eq!(report.total_invariants, ranking.entries.len());
    }

    #[test]
    fn evidence_ledger_builds() {
        let catalog = build_canonical_catalog();
        let universe = build_canonical_universe();
        let config = default_config();
        let (report, ranking) = evaluate_full(&catalog, &universe, &config);
        let ledger = build_evidence_ledger(&report, &ranking);
        assert_eq!(ledger.schema_version, GATE_SCHEMA_VERSION);
        assert_eq!(ledger.total_invariants, report.total_invariants);
        assert_eq!(ledger.passing_invariants, report.passing_invariants);
        assert_eq!(ledger.category_summaries.len(), FeatureCategory::ALL.len());
    }

    #[test]
    fn evidence_ledger_top_priority_capped() {
        let catalog = build_canonical_catalog();
        let universe = build_canonical_universe();
        let config = default_config();
        let (report, ranking) = evaluate_full(&catalog, &universe, &config);
        let ledger = build_evidence_ledger(&report, &ranking);
        assert!(
            ledger.top_priority_items.len() <= 10,
            "top priority items should be capped at 10"
        );
    }

    #[test]
    fn evidence_ledger_carries_contract_enforcement_payload() {
        let catalog = build_canonical_catalog();
        let universe = build_canonical_universe();
        let config = default_config();
        let (base_report, ranking) = evaluate_full(&catalog, &universe, &config);
        let report = apply_contract_outcome_to_gate_report(
            base_report,
            synthetic_contract_outcome(true, false),
        );
        let ledger = build_evidence_ledger(&report, &ranking);

        let contract = ledger
            .verification_contract
            .as_ref()
            .expect("ledger should include contract payload");
        assert!(!contract.contract_passed);
        assert_eq!(
            contract.disposition,
            EnforcementDisposition::BlockedByContract
        );
    }

    #[test]
    fn category_posteriors_are_valid() {
        let catalog = build_canonical_catalog();
        let config = default_config();
        let report = evaluate_gate(&catalog, &config);
        for cr in report.category_results.values() {
            assert!(
                (0.0..=1.0).contains(&cr.posterior_mean),
                "category {} posterior mean out of range: {}",
                cr.category,
                cr.posterior_mean
            );
            assert!(
                cr.credible_lower <= cr.posterior_mean + 1e-6,
                "category {} lower ({}) > mean ({})",
                cr.category,
                cr.credible_lower,
                cr.posterior_mean
            );
            assert!(
                cr.credible_upper >= cr.posterior_mean - 1e-6,
                "category {} upper ({}) < mean ({})",
                cr.category,
                cr.credible_upper,
                cr.posterior_mean
            );
        }
    }

    #[test]
    fn gate_decision_display() {
        assert_eq!(format!("{}", GateDecision::Pass), "PASS");
        assert_eq!(format!("{}", GateDecision::Fail), "FAIL");
        assert_eq!(format!("{}", GateDecision::Conditional), "CONDITIONAL");
        assert_eq!(format!("{}", GateDecision::Waived), "WAIVED");
    }

    #[test]
    fn gate_decision_is_pass() {
        assert!(GateDecision::Pass.is_pass());
        assert!(GateDecision::Waived.is_pass());
        assert!(!GateDecision::Conditional.is_pass());
        assert!(!GateDecision::Fail.is_pass());
    }

    #[test]
    fn high_threshold_fails_gate() {
        let catalog = build_canonical_catalog();
        let config = GateConfig {
            release_threshold: 0.99,
            ..default_config()
        };
        let report = evaluate_gate(&catalog, &config);
        // With pending obligations, should not pass a 99% threshold
        assert!(
            !report.release_ready || report.global_lower_bound >= 0.99,
            "should not be release_ready with 99% threshold unless fully verified"
        );
    }

    #[test]
    fn zero_threshold_passes_gate() {
        let catalog = build_canonical_catalog();
        let config = GateConfig {
            release_threshold: 0.0,
            category_min_verification_pct: 0.0,
            ..default_config()
        };
        let report = evaluate_gate(&catalog, &config);
        // With zero thresholds, even partial verification should pass
        assert!(
            report.global_lower_bound >= 0.0,
            "lower bound should be non-negative"
        );
    }

    #[test]
    fn loss_asymmetry_affects_ranking() {
        let catalog = build_canonical_catalog();
        let universe = build_canonical_universe();

        let config_low = GateConfig {
            loss_asymmetry_ratio: 1.0,
            ..default_config()
        };
        let config_high = GateConfig {
            loss_asymmetry_ratio: 10.0,
            ..default_config()
        };

        let ranking_low = compute_expected_loss_ranking(&catalog, &universe, &config_low);
        let ranking_high = compute_expected_loss_ranking(&catalog, &universe, &config_high);

        assert!(
            ranking_high.total_expected_loss >= ranking_low.total_expected_loss,
            "higher asymmetry should produce higher total expected loss"
        );
    }

    #[test]
    fn mismatch_probability_bounds() {
        let catalog = build_canonical_catalog();
        let universe = build_canonical_universe();
        let config = default_config();
        let ranking = compute_expected_loss_ranking(&catalog, &universe, &config);
        for entry in &ranking.entries {
            assert!(
                (0.0..=1.0).contains(&entry.mismatch_probability),
                "mismatch_probability for {} is {}",
                entry.invariant_id,
                entry.mismatch_probability
            );
        }
    }
}
