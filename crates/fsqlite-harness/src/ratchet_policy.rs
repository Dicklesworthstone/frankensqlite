//! Monotone ratchet policy and gate semantics for parity progression (bd-1dp9.1.4).
//!
//! Enforces the invariant that no merge may reduce the lower confidence bound
//! on the parity score. The ratchet is the backbone of the 40%→100% progression
//! guarantee: once a score threshold is attained, it can never regress.
//!
//! # Policy
//!
//! The ratchet evaluates a candidate [`BayesianScorecard`] against the persisted
//! [`RatchetState`] and produces a [`RatchetDecision`]:
//!
//! - **Allow**: candidate lower bound >= historical high-water mark (or within
//!   tolerance).
//! - **Block**: candidate regressed below the high-water mark beyond tolerance.
//! - **Quarantine**: candidate regressed but a temporary quarantine exemption is
//!   active (e.g., for known-breaking refactors).
//! - **Waiver**: an explicit waiver was granted for this evaluation.
//!
//! # Persistence
//!
//! [`RatchetState`] is serialized to JSON and lives alongside the beads
//! database. The canonical path is `reports/ratchet_state.json`.
//!
//! # Determinism
//!
//! All comparisons use [`truncate_score`] from the parity taxonomy to ensure
//! cross-platform reproducibility.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::parity_taxonomy::truncate_score;
use crate::score_engine::BayesianScorecard;

/// Bead identifier for log correlation.
#[allow(dead_code)]
const BEAD_ID: &str = "bd-1dp9.1.4";

/// Schema version for migration compatibility.
pub const RATCHET_SCHEMA_VERSION: u32 = 1;

// ---------------------------------------------------------------------------
// Policy configuration
// ---------------------------------------------------------------------------

/// Configuration for the monotone ratchet gate.
///
/// Controls tolerance, quarantine, and waiver behaviour.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RatchetPolicy {
    /// Maximum allowed regression in the global lower bound before blocking.
    ///
    /// A value of 0.0 enforces strict monotonicity: any decrease blocks.
    /// A small positive value (e.g., 0.005) allows for statistical noise.
    pub regression_tolerance: f64,

    /// Maximum allowed regression per category before flagging.
    ///
    /// Categories that regress beyond this tolerance are listed in the
    /// decision's `regressed_categories` field.
    pub category_regression_tolerance: f64,

    /// If `true`, a quarantine exemption can temporarily allow regressions.
    pub quarantine_enabled: bool,

    /// Maximum number of consecutive quarantined evaluations before the
    /// quarantine expires and regressions are blocked.
    pub quarantine_max_evaluations: u32,

    /// If `true`, explicit waivers can override a block decision.
    pub waivers_enabled: bool,

    /// Minimum global lower bound required for release readiness.
    /// This mirrors `ScoreEngineConfig::release_threshold` but is checked
    /// at the ratchet level for defense-in-depth.
    pub minimum_release_threshold: f64,
}

impl Default for RatchetPolicy {
    fn default() -> Self {
        Self {
            regression_tolerance: 0.0,
            category_regression_tolerance: 0.005,
            quarantine_enabled: true,
            quarantine_max_evaluations: 5,
            waivers_enabled: true,
            minimum_release_threshold: 1.0,
        }
    }
}

impl RatchetPolicy {
    /// Strict policy: zero tolerance, no quarantine, no waivers.
    #[must_use]
    pub fn strict() -> Self {
        Self {
            regression_tolerance: 0.0,
            category_regression_tolerance: 0.0,
            quarantine_enabled: false,
            quarantine_max_evaluations: 0,
            waivers_enabled: false,
            minimum_release_threshold: 1.0,
        }
    }

    /// Relaxed policy: small tolerance, quarantine enabled.
    #[must_use]
    pub fn relaxed() -> Self {
        Self {
            regression_tolerance: 0.01,
            category_regression_tolerance: 0.02,
            quarantine_enabled: true,
            quarantine_max_evaluations: 10,
            waivers_enabled: true,
            minimum_release_threshold: 1.0,
        }
    }
}

// ---------------------------------------------------------------------------
// Persisted state
// ---------------------------------------------------------------------------

/// Persisted ratchet state tracking the high-water mark and evaluation history.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RatchetState {
    /// Schema version for forward compatibility.
    pub schema_version: u32,

    /// The highest global lower bound ever observed.
    pub high_water_mark: f64,

    /// The highest global point estimate ever observed.
    pub high_water_point_estimate: f64,

    /// Per-category high-water marks (lower bounds).
    pub category_high_water_marks: BTreeMap<String, f64>,

    /// Number of evaluations recorded.
    pub evaluation_count: u64,

    /// Number of consecutive quarantined evaluations (resets on allow).
    pub quarantine_streak: u32,

    /// Active quarantine reason, if any.
    pub quarantine_reason: Option<String>,

    /// Active waiver, if any.
    pub active_waiver: Option<Waiver>,

    /// History of the last N evaluations (ring buffer, most recent last).
    pub recent_evaluations: Vec<EvaluationRecord>,
}

/// Maximum number of recent evaluations to retain.
const MAX_RECENT_EVALUATIONS: usize = 50;

impl Default for RatchetState {
    fn default() -> Self {
        Self {
            schema_version: RATCHET_SCHEMA_VERSION,
            high_water_mark: 0.0,
            high_water_point_estimate: 0.0,
            category_high_water_marks: BTreeMap::new(),
            evaluation_count: 0,
            quarantine_streak: 0,
            quarantine_reason: None,
            active_waiver: None,
            recent_evaluations: Vec::new(),
        }
    }
}

impl RatchetState {
    /// Serialize to deterministic JSON.
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

    /// Create a fresh state seeded from a scorecard (first evaluation).
    #[must_use]
    pub fn from_scorecard(scorecard: &BayesianScorecard) -> Self {
        let mut category_marks = BTreeMap::new();
        for (name, posterior) in &scorecard.category_posteriors {
            category_marks.insert(name.clone(), truncate_score(posterior.lower_bound));
        }
        Self {
            schema_version: RATCHET_SCHEMA_VERSION,
            high_water_mark: truncate_score(scorecard.global_lower_bound),
            high_water_point_estimate: truncate_score(scorecard.global_point_estimate),
            category_high_water_marks: category_marks,
            evaluation_count: 1,
            quarantine_streak: 0,
            quarantine_reason: None,
            active_waiver: None,
            recent_evaluations: vec![EvaluationRecord {
                evaluation_id: 1,
                global_lower_bound: truncate_score(scorecard.global_lower_bound),
                global_point_estimate: truncate_score(scorecard.global_point_estimate),
                decision: RatchetVerdict::Allow,
                bead_id: scorecard.bead_id.clone(),
            }],
        }
    }
}

// ---------------------------------------------------------------------------
// Waiver
// ---------------------------------------------------------------------------

/// An explicit waiver that overrides a block decision.
///
/// Waivers must be created with a reason and an expiry (evaluation count).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Waiver {
    /// Human-readable reason for the waiver.
    pub reason: String,

    /// The evaluation count at which the waiver was granted.
    pub granted_at_evaluation: u64,

    /// Number of evaluations the waiver is valid for.
    pub valid_for_evaluations: u32,

    /// Who granted the waiver (agent name or human).
    pub granted_by: String,
}

impl Waiver {
    /// Check if the waiver has expired given the current evaluation count.
    #[must_use]
    pub fn is_expired(&self, current_evaluation: u64) -> bool {
        let elapsed = current_evaluation.saturating_sub(self.granted_at_evaluation);
        elapsed > u64::from(self.valid_for_evaluations)
    }
}

// ---------------------------------------------------------------------------
// Quarantine request
// ---------------------------------------------------------------------------

/// A request to enter quarantine mode (allows temporary regressions).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuarantineRequest {
    /// Human-readable reason for entering quarantine.
    pub reason: String,
}

// ---------------------------------------------------------------------------
// Evaluation record
// ---------------------------------------------------------------------------

/// A single evaluation record in the ratchet history.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvaluationRecord {
    /// Sequential evaluation ID.
    pub evaluation_id: u64,
    /// Global lower bound at this evaluation.
    pub global_lower_bound: f64,
    /// Global point estimate at this evaluation.
    pub global_point_estimate: f64,
    /// Decision rendered.
    pub decision: RatchetVerdict,
    /// Bead ID that triggered this evaluation.
    pub bead_id: String,
}

// ---------------------------------------------------------------------------
// Decision output
// ---------------------------------------------------------------------------

/// The verdict of a ratchet evaluation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RatchetVerdict {
    /// Score meets or exceeds the high-water mark (within tolerance).
    Allow,
    /// Score regressed beyond tolerance; merge is blocked.
    Block,
    /// Score regressed but quarantine exemption is active.
    Quarantine,
    /// Score regressed but an explicit waiver overrides the block.
    Waiver,
}

/// A category-level regression detail.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CategoryRegression {
    /// Category name.
    pub category: String,
    /// Previous high-water mark for this category.
    pub previous_lower_bound: f64,
    /// Current lower bound.
    pub current_lower_bound: f64,
    /// Magnitude of the regression.
    pub regression_magnitude: f64,
}

/// Full decision output from a ratchet evaluation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RatchetDecision {
    /// The verdict.
    pub verdict: RatchetVerdict,

    /// Previous global high-water mark.
    pub previous_high_water_mark: f64,

    /// Candidate global lower bound.
    pub candidate_lower_bound: f64,

    /// Candidate global point estimate.
    pub candidate_point_estimate: f64,

    /// Global regression magnitude (positive = regression, zero/negative = improvement).
    pub global_regression: f64,

    /// Categories that regressed beyond tolerance.
    pub regressed_categories: Vec<CategoryRegression>,

    /// Categories that improved.
    pub improved_categories: Vec<String>,

    /// Whether the candidate meets the minimum release threshold.
    pub meets_release_threshold: bool,

    /// Evaluation ID assigned.
    pub evaluation_id: u64,

    /// Human-readable summary.
    pub summary: String,
}

// ---------------------------------------------------------------------------
// Core evaluation logic
// ---------------------------------------------------------------------------

/// Evaluate a candidate scorecard against the ratchet state.
///
/// Returns a [`RatchetDecision`] and mutates `state` to reflect the evaluation.
/// The caller is responsible for persisting the updated state.
#[must_use]
#[allow(clippy::too_many_lines)]
pub fn evaluate_ratchet(
    scorecard: &BayesianScorecard,
    state: &mut RatchetState,
    policy: &RatchetPolicy,
    quarantine_request: Option<&QuarantineRequest>,
) -> RatchetDecision {
    let candidate_lower = truncate_score(scorecard.global_lower_bound);
    let candidate_point = truncate_score(scorecard.global_point_estimate);
    let previous_hwm = state.high_water_mark;

    let global_regression = truncate_score(previous_hwm - candidate_lower);
    let meets_threshold = candidate_lower >= policy.minimum_release_threshold;

    // Detect per-category regressions.
    let mut regressed_categories = Vec::new();
    let mut improved_categories = Vec::new();

    for (name, posterior) in &scorecard.category_posteriors {
        let current = truncate_score(posterior.lower_bound);
        let previous = state
            .category_high_water_marks
            .get(name)
            .copied()
            .unwrap_or(0.0);
        let cat_regression = truncate_score(previous - current);

        if cat_regression > policy.category_regression_tolerance {
            regressed_categories.push(CategoryRegression {
                category: name.clone(),
                previous_lower_bound: previous,
                current_lower_bound: current,
                regression_magnitude: cat_regression,
            });
        } else if current > previous {
            improved_categories.push(name.clone());
        }
    }

    // Determine verdict.
    let verdict = if global_regression <= policy.regression_tolerance {
        // No regression (or within tolerance): allow and advance high-water mark.
        RatchetVerdict::Allow
    } else if let Some(waiver) = &state.active_waiver {
        // Check if waiver is still active.
        if policy.waivers_enabled && !waiver.is_expired(state.evaluation_count + 1) {
            RatchetVerdict::Waiver
        } else if let Some(qr) = quarantine_request {
            try_quarantine(state, policy, qr)
        } else if state.quarantine_reason.is_some()
            && state.quarantine_streak < policy.quarantine_max_evaluations
        {
            RatchetVerdict::Quarantine
        } else {
            RatchetVerdict::Block
        }
    } else if let Some(qr) = quarantine_request {
        try_quarantine(state, policy, qr)
    } else if state.quarantine_reason.is_some()
        && policy.quarantine_enabled
        && state.quarantine_streak < policy.quarantine_max_evaluations
    {
        RatchetVerdict::Quarantine
    } else {
        RatchetVerdict::Block
    };

    // Build summary message.
    let summary = build_summary(
        verdict,
        previous_hwm,
        candidate_lower,
        global_regression,
        &regressed_categories,
        meets_threshold,
    );

    let evaluation_id = state.evaluation_count + 1;

    // Update state based on verdict.
    match verdict {
        RatchetVerdict::Allow => {
            // Advance high-water marks.
            if candidate_lower > state.high_water_mark {
                state.high_water_mark = candidate_lower;
            }
            if candidate_point > state.high_water_point_estimate {
                state.high_water_point_estimate = candidate_point;
            }
            for (name, posterior) in &scorecard.category_posteriors {
                let current = truncate_score(posterior.lower_bound);
                let entry = state
                    .category_high_water_marks
                    .entry(name.clone())
                    .or_insert(0.0);
                if current > *entry {
                    *entry = current;
                }
            }
            // Reset quarantine streak on successful evaluation.
            state.quarantine_streak = 0;
            state.quarantine_reason = None;
        }
        RatchetVerdict::Quarantine => {
            state.quarantine_streak += 1;
            if quarantine_request.is_some() {
                state.quarantine_reason = quarantine_request.map(|qr| qr.reason.clone());
            }
        }
        RatchetVerdict::Waiver | RatchetVerdict::Block => {
            // No state changes for block/waiver beyond recording.
        }
    }

    // Record evaluation in history.
    state.evaluation_count = evaluation_id;
    state.recent_evaluations.push(EvaluationRecord {
        evaluation_id,
        global_lower_bound: candidate_lower,
        global_point_estimate: candidate_point,
        decision: verdict,
        bead_id: scorecard.bead_id.clone(),
    });
    if state.recent_evaluations.len() > MAX_RECENT_EVALUATIONS {
        let excess = state.recent_evaluations.len() - MAX_RECENT_EVALUATIONS;
        state.recent_evaluations.drain(..excess);
    }

    // Expire waiver if needed.
    if let Some(waiver) = &state.active_waiver {
        if waiver.is_expired(evaluation_id) {
            state.active_waiver = None;
        }
    }

    RatchetDecision {
        verdict,
        previous_high_water_mark: previous_hwm,
        candidate_lower_bound: candidate_lower,
        candidate_point_estimate: candidate_point,
        global_regression,
        regressed_categories,
        improved_categories,
        meets_release_threshold: meets_threshold,
        evaluation_id,
        summary,
    }
}

/// Grant a waiver on the ratchet state.
pub fn grant_waiver(state: &mut RatchetState, reason: &str, valid_for: u32, granted_by: &str) {
    state.active_waiver = Some(Waiver {
        reason: reason.to_owned(),
        granted_at_evaluation: state.evaluation_count,
        valid_for_evaluations: valid_for,
        granted_by: granted_by.to_owned(),
    });
}

/// Revoke any active waiver.
pub fn revoke_waiver(state: &mut RatchetState) {
    state.active_waiver = None;
}

/// Enter quarantine mode.
pub fn enter_quarantine(state: &mut RatchetState, reason: &str) {
    state.quarantine_reason = Some(reason.to_owned());
    state.quarantine_streak = 0;
}

/// Exit quarantine mode.
pub fn exit_quarantine(state: &mut RatchetState) {
    state.quarantine_reason = None;
    state.quarantine_streak = 0;
}

// ---------------------------------------------------------------------------
// Rollback signal detection
// ---------------------------------------------------------------------------

/// Signals that the ratchet should trigger an automatic rollback.
///
/// This is advisory — the CI system reads this to decide whether to revert.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RollbackSignal {
    /// Whether a rollback is recommended.
    pub should_rollback: bool,
    /// Reason for the rollback recommendation.
    pub reason: String,
    /// The evaluation that triggered the signal.
    pub triggering_evaluation_id: u64,
    /// The regression magnitude that triggered the signal.
    pub regression_magnitude: f64,
}

/// Check whether a rollback signal should be emitted.
///
/// A rollback is signalled when:
/// 1. The verdict is `Block`, AND
/// 2. The regression exceeds twice the policy tolerance (catastrophic regression), OR
/// 3. Three consecutive blocked evaluations have occurred.
#[must_use]
pub fn check_rollback_signal(
    decision: &RatchetDecision,
    state: &RatchetState,
    policy: &RatchetPolicy,
) -> RollbackSignal {
    if decision.verdict != RatchetVerdict::Block {
        return RollbackSignal {
            should_rollback: false,
            reason: "verdict is not Block".to_owned(),
            triggering_evaluation_id: decision.evaluation_id,
            regression_magnitude: decision.global_regression,
        };
    }

    let catastrophic_threshold = policy.regression_tolerance.mul_add(2.0, 0.01);
    let is_catastrophic = decision.global_regression > catastrophic_threshold;

    let recent_blocks = state
        .recent_evaluations
        .iter()
        .rev()
        .take(3)
        .filter(|rec| rec.decision == RatchetVerdict::Block)
        .count();
    let consecutive_blocks = recent_blocks >= 3;

    let should_rollback = is_catastrophic || consecutive_blocks;
    let reason = if is_catastrophic {
        format!(
            "catastrophic regression: {:.6} exceeds threshold {:.6}",
            decision.global_regression, catastrophic_threshold
        )
    } else if consecutive_blocks {
        "3 consecutive blocked evaluations".to_owned()
    } else {
        "regression within rollback threshold".to_owned()
    };

    RollbackSignal {
        should_rollback,
        reason,
        triggering_evaluation_id: decision.evaluation_id,
        regression_magnitude: decision.global_regression,
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn try_quarantine(
    state: &RatchetState,
    policy: &RatchetPolicy,
    _request: &QuarantineRequest,
) -> RatchetVerdict {
    if policy.quarantine_enabled && state.quarantine_streak < policy.quarantine_max_evaluations {
        RatchetVerdict::Quarantine
    } else {
        RatchetVerdict::Block
    }
}

fn build_summary(
    verdict: RatchetVerdict,
    previous_hwm: f64,
    candidate_lower: f64,
    global_regression: f64,
    regressed_categories: &[CategoryRegression],
    meets_threshold: bool,
) -> String {
    use std::fmt::Write;

    let mut s = String::new();
    let _ = write!(
        s,
        "Ratchet {verdict:?}: candidate_lower={candidate_lower:.6} hwm={previous_hwm:.6}"
    );

    if global_regression > 0.0 {
        let _ = write!(s, " regression={global_regression:.6}");
    } else {
        let improvement = -global_regression;
        let _ = write!(s, " improvement={improvement:.6}");
    }

    if !regressed_categories.is_empty() {
        let _ = write!(s, " regressed_categories=[");
        for (i, cat) in regressed_categories.iter().enumerate() {
            if i > 0 {
                let _ = write!(s, ", ");
            }
            let _ = write!(
                s,
                "{}({:.6}->{:.6})",
                cat.category, cat.previous_lower_bound, cat.current_lower_bound
            );
        }
        let _ = write!(s, "]");
    }

    if !meets_threshold {
        let _ = write!(s, " BELOW_RELEASE_THRESHOLD");
    }

    s
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parity_taxonomy::build_canonical_universe;
    use crate::score_engine::{BayesianScorecard, ScoreEngineConfig, compute_bayesian_scorecard};

    const TOL: f64 = 1e-6;

    fn make_scorecard() -> BayesianScorecard {
        let universe = build_canonical_universe();
        let config = ScoreEngineConfig::default();
        compute_bayesian_scorecard(&universe, &config)
    }

    #[allow(dead_code)]
    fn make_scorecard_with_threshold(threshold: f64) -> BayesianScorecard {
        let universe = build_canonical_universe();
        let config = ScoreEngineConfig {
            release_threshold: threshold,
            ..Default::default()
        };
        compute_bayesian_scorecard(&universe, &config)
    }

    // --- State construction ---

    #[test]
    fn test_ratchet_state_from_scorecard() {
        let sc = make_scorecard();
        let state = RatchetState::from_scorecard(&sc);

        assert_eq!(state.schema_version, RATCHET_SCHEMA_VERSION);
        assert!((state.high_water_mark - sc.global_lower_bound).abs() < TOL);
        assert_eq!(state.evaluation_count, 1);
        assert_eq!(state.quarantine_streak, 0);
        assert!(state.quarantine_reason.is_none());
        assert!(state.active_waiver.is_none());
        assert_eq!(state.recent_evaluations.len(), 1);
        assert_eq!(state.recent_evaluations[0].decision, RatchetVerdict::Allow);
    }

    // --- Allow on equal or improved score ---

    #[test]
    fn test_allow_on_same_score() {
        let sc = make_scorecard();
        let mut state = RatchetState::from_scorecard(&sc);
        let policy = RatchetPolicy::default();

        let decision = evaluate_ratchet(&sc, &mut state, &policy, None);

        assert_eq!(decision.verdict, RatchetVerdict::Allow);
        assert!(decision.global_regression <= TOL);
        assert_eq!(state.evaluation_count, 2);
    }

    #[test]
    fn test_allow_on_improved_score() {
        let sc = make_scorecard();
        let mut state = RatchetState::default(); // hwm = 0.0
        let policy = RatchetPolicy::default();

        let decision = evaluate_ratchet(&sc, &mut state, &policy, None);

        assert_eq!(decision.verdict, RatchetVerdict::Allow);
        assert!(state.high_water_mark > 0.0);
        assert!(state.high_water_mark >= sc.global_lower_bound - TOL);
    }

    // --- Block on regression ---

    #[test]
    fn test_block_on_regression_strict_policy() {
        let sc = make_scorecard();
        let mut state = RatchetState::from_scorecard(&sc);

        // Artificially inflate the high-water mark to force regression.
        state.high_water_mark = 0.99;

        let policy = RatchetPolicy::strict();
        let decision = evaluate_ratchet(&sc, &mut state, &policy, None);

        assert_eq!(decision.verdict, RatchetVerdict::Block);
        assert!(decision.global_regression > 0.0);
    }

    #[test]
    fn test_block_respects_tolerance() {
        let sc = make_scorecard();
        let mut state = RatchetState::from_scorecard(&sc);

        // Set HWM slightly above candidate — within tolerance.
        let candidate_lower = sc.global_lower_bound;
        state.high_water_mark = candidate_lower + 0.005;

        let policy = RatchetPolicy {
            regression_tolerance: 0.01,
            ..Default::default()
        };
        let decision = evaluate_ratchet(&sc, &mut state, &policy, None);

        assert_eq!(
            decision.verdict,
            RatchetVerdict::Allow,
            "regression within tolerance should allow"
        );
    }

    #[test]
    fn test_block_when_regression_exceeds_tolerance() {
        let sc = make_scorecard();
        let mut state = RatchetState::from_scorecard(&sc);

        // Set HWM well above candidate.
        state.high_water_mark = sc.global_lower_bound + 0.1;

        let policy = RatchetPolicy {
            regression_tolerance: 0.01,
            quarantine_enabled: false,
            waivers_enabled: false,
            ..Default::default()
        };
        let decision = evaluate_ratchet(&sc, &mut state, &policy, None);

        assert_eq!(decision.verdict, RatchetVerdict::Block);
    }

    // --- Quarantine ---

    #[test]
    fn test_quarantine_allows_temporary_regression() {
        let sc = make_scorecard();
        let mut state = RatchetState::from_scorecard(&sc);
        state.high_water_mark = 0.99; // Force regression

        let policy = RatchetPolicy {
            quarantine_enabled: true,
            quarantine_max_evaluations: 5,
            waivers_enabled: false,
            ..Default::default()
        };

        let qr = QuarantineRequest {
            reason: "Major refactor in progress".to_owned(),
        };

        let decision = evaluate_ratchet(&sc, &mut state, &policy, Some(&qr));

        assert_eq!(decision.verdict, RatchetVerdict::Quarantine);
        assert_eq!(state.quarantine_streak, 1);
        assert!(state.quarantine_reason.is_some());
    }

    #[test]
    fn test_quarantine_expires_after_max_evaluations() {
        let sc = make_scorecard();
        let mut state = RatchetState::from_scorecard(&sc);
        state.high_water_mark = 0.99; // Force regression

        let policy = RatchetPolicy {
            quarantine_enabled: true,
            quarantine_max_evaluations: 2,
            waivers_enabled: false,
            ..Default::default()
        };

        let qr = QuarantineRequest {
            reason: "refactor".to_owned(),
        };

        // First quarantine evaluation.
        let d1 = evaluate_ratchet(&sc, &mut state, &policy, Some(&qr));
        assert_eq!(d1.verdict, RatchetVerdict::Quarantine);

        // Second quarantine evaluation (still within max).
        let d2 = evaluate_ratchet(&sc, &mut state, &policy, Some(&qr));
        assert_eq!(d2.verdict, RatchetVerdict::Quarantine);

        // Third — exceeds max, should block.
        let d3 = evaluate_ratchet(&sc, &mut state, &policy, Some(&qr));
        assert_eq!(d3.verdict, RatchetVerdict::Block);
    }

    // --- Waivers ---

    #[test]
    fn test_waiver_overrides_block() {
        let sc = make_scorecard();
        let mut state = RatchetState::from_scorecard(&sc);
        state.high_water_mark = 0.99; // Force regression

        let policy = RatchetPolicy {
            waivers_enabled: true,
            quarantine_enabled: false,
            ..Default::default()
        };

        // Grant waiver.
        grant_waiver(&mut state, "Known regression for feature X", 3, "admin");
        assert!(state.active_waiver.is_some());

        let decision = evaluate_ratchet(&sc, &mut state, &policy, None);
        assert_eq!(decision.verdict, RatchetVerdict::Waiver);
    }

    #[test]
    fn test_waiver_expires() {
        let sc = make_scorecard();
        let mut state = RatchetState::from_scorecard(&sc);
        state.high_water_mark = 0.99;

        let policy = RatchetPolicy {
            waivers_enabled: true,
            quarantine_enabled: false,
            ..Default::default()
        };

        // Grant waiver valid for 1 evaluation.
        grant_waiver(&mut state, "temp", 1, "admin");

        // First eval: waiver active.
        let d1 = evaluate_ratchet(&sc, &mut state, &policy, None);
        assert_eq!(d1.verdict, RatchetVerdict::Waiver);

        // Second eval: waiver expired, should block.
        let d2 = evaluate_ratchet(&sc, &mut state, &policy, None);
        assert_eq!(d2.verdict, RatchetVerdict::Block);
        assert!(state.active_waiver.is_none());
    }

    #[test]
    fn test_revoke_waiver() {
        let sc = make_scorecard();
        let mut state = RatchetState::from_scorecard(&sc);
        state.high_water_mark = 0.99;

        grant_waiver(&mut state, "temp", 10, "admin");
        assert!(state.active_waiver.is_some());

        revoke_waiver(&mut state);
        assert!(state.active_waiver.is_none());

        let policy = RatchetPolicy {
            waivers_enabled: true,
            quarantine_enabled: false,
            ..Default::default()
        };
        let decision = evaluate_ratchet(&sc, &mut state, &policy, None);
        assert_eq!(decision.verdict, RatchetVerdict::Block);
    }

    // --- Quarantine enter/exit ---

    #[test]
    fn test_enter_exit_quarantine() {
        let mut state = RatchetState::default();

        enter_quarantine(&mut state, "planned regression");
        assert_eq!(
            state.quarantine_reason.as_deref(),
            Some("planned regression")
        );
        assert_eq!(state.quarantine_streak, 0);

        exit_quarantine(&mut state);
        assert!(state.quarantine_reason.is_none());
        assert_eq!(state.quarantine_streak, 0);
    }

    // --- Rollback signal ---

    #[test]
    fn test_rollback_signal_not_emitted_on_allow() {
        let sc = make_scorecard();
        let mut state = RatchetState::from_scorecard(&sc);
        let policy = RatchetPolicy::default();

        let decision = evaluate_ratchet(&sc, &mut state, &policy, None);
        let signal = check_rollback_signal(&decision, &state, &policy);

        assert!(!signal.should_rollback);
    }

    #[test]
    fn test_rollback_signal_on_catastrophic_regression() {
        let sc = make_scorecard();
        let mut state = RatchetState::from_scorecard(&sc);
        state.high_water_mark = 0.99;

        let policy = RatchetPolicy::strict();
        let decision = evaluate_ratchet(&sc, &mut state, &policy, None);
        let signal = check_rollback_signal(&decision, &state, &policy);

        assert_eq!(decision.verdict, RatchetVerdict::Block);
        assert!(signal.should_rollback);
        assert!(signal.reason.contains("catastrophic"));
    }

    // --- Serialization round-trip ---

    #[test]
    fn test_state_json_roundtrip() {
        let sc = make_scorecard();
        let state = RatchetState::from_scorecard(&sc);
        let json = state.to_json().expect("serialize");
        let restored = RatchetState::from_json(&json).expect("deserialize");

        assert_eq!(restored.schema_version, state.schema_version);
        assert!((restored.high_water_mark - state.high_water_mark).abs() < TOL);
        assert_eq!(restored.evaluation_count, state.evaluation_count);
    }

    #[test]
    fn test_policy_json_roundtrip() {
        let policy = RatchetPolicy::default();
        let json = serde_json::to_string_pretty(&policy).expect("serialize");
        let restored: RatchetPolicy = serde_json::from_str(&json).expect("deserialize");

        assert!((restored.regression_tolerance - policy.regression_tolerance).abs() < TOL);
        assert_eq!(
            restored.quarantine_max_evaluations,
            policy.quarantine_max_evaluations
        );
    }

    // --- Category regression tracking ---

    #[test]
    fn test_category_regression_detected() {
        let sc = make_scorecard();
        let mut state = RatchetState::from_scorecard(&sc);

        // Inflate all category HWMs.
        for value in state.category_high_water_marks.values_mut() {
            *value = 0.99;
        }
        state.high_water_mark = 0.99;

        let policy = RatchetPolicy::strict();
        let decision = evaluate_ratchet(&sc, &mut state, &policy, None);

        assert_eq!(decision.verdict, RatchetVerdict::Block);
        assert!(
            !decision.regressed_categories.is_empty(),
            "should detect category regressions"
        );
    }

    // --- High-water mark monotonicity ---

    #[test]
    fn test_hwm_only_advances_never_decreases() {
        let sc = make_scorecard();
        let mut state = RatchetState::default();
        let policy = RatchetPolicy::default();

        // First evaluation: sets HWM.
        let _d1 = evaluate_ratchet(&sc, &mut state, &policy, None);
        let hwm1 = state.high_water_mark;
        assert!(hwm1 > 0.0);

        // Second evaluation with same score: HWM unchanged.
        let _d2 = evaluate_ratchet(&sc, &mut state, &policy, None);
        let hwm2 = state.high_water_mark;
        assert!(hwm2 >= hwm1 - TOL);
    }

    // --- Recent evaluations ring buffer ---

    #[test]
    fn test_recent_evaluations_bounded() {
        let sc = make_scorecard();
        let mut state = RatchetState::default();
        let policy = RatchetPolicy::default();

        for _ in 0..60 {
            let _ = evaluate_ratchet(&sc, &mut state, &policy, None);
        }

        assert!(
            state.recent_evaluations.len() <= MAX_RECENT_EVALUATIONS,
            "recent_evaluations should be bounded at {}",
            MAX_RECENT_EVALUATIONS
        );
        assert_eq!(state.evaluation_count, 60);
    }

    // --- Release threshold ---

    #[test]
    fn test_meets_release_threshold_reported() {
        let sc = make_scorecard();
        let mut state = RatchetState::default();

        // Use a threshold we know the current score doesn't meet.
        let policy = RatchetPolicy {
            minimum_release_threshold: 0.99,
            ..Default::default()
        };

        let decision = evaluate_ratchet(&sc, &mut state, &policy, None);
        // The verdict may still be Allow (ratchet doesn't block on threshold,
        // only reports), but meets_release_threshold should be false if score < 0.99.
        if sc.global_lower_bound < 0.99 {
            assert!(
                !decision.meets_release_threshold,
                "should report below threshold"
            );
        }
    }

    // --- Default policy values ---

    #[test]
    fn test_default_policy() {
        let p = RatchetPolicy::default();
        assert!((p.regression_tolerance - 0.0).abs() < TOL);
        assert!((p.category_regression_tolerance - 0.005).abs() < TOL);
        assert!(p.quarantine_enabled);
        assert_eq!(p.quarantine_max_evaluations, 5);
        assert!(p.waivers_enabled);
        assert!((p.minimum_release_threshold - 1.0).abs() < TOL);
    }

    #[test]
    fn test_strict_policy() {
        let p = RatchetPolicy::strict();
        assert!((p.regression_tolerance - 0.0).abs() < TOL);
        assert!(!p.quarantine_enabled);
        assert!(!p.waivers_enabled);
    }

    #[test]
    fn test_relaxed_policy() {
        let p = RatchetPolicy::relaxed();
        assert!((p.regression_tolerance - 0.01).abs() < TOL);
        assert!(p.quarantine_enabled);
        assert!(p.waivers_enabled);
    }

    // --- Waiver expiry ---

    #[test]
    fn test_waiver_expiry_logic() {
        let w = Waiver {
            reason: "test".to_owned(),
            granted_at_evaluation: 5,
            valid_for_evaluations: 3,
            granted_by: "test".to_owned(),
        };

        assert!(!w.is_expired(5)); // At grant
        assert!(!w.is_expired(6)); // 1 evaluation later
        assert!(!w.is_expired(8)); // 3 evaluations later (last valid)
        assert!(w.is_expired(9)); // Expired
    }

    // --- Summary format ---

    #[test]
    fn test_summary_contains_key_info() {
        let sc = make_scorecard();
        let mut state = RatchetState::default();
        let policy = RatchetPolicy::default();

        let decision = evaluate_ratchet(&sc, &mut state, &policy, None);
        assert!(decision.summary.contains("Ratchet Allow"));
        assert!(decision.summary.contains("candidate_lower="));
        assert!(decision.summary.contains("hwm="));
    }
}
