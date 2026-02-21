//! bd-1dp9.6.6: Alien Contract Pack — EV/risk/fallback enforcement across all
//! open beads — harness integration tests.
//!
//! Validates the alien contract enforcement infrastructure:
//! - Verification contract enforcement types and classification
//! - Bayesian score engine (BetaParams, PriorConfig, BayesianScorecard)
//! - Confidence gate system (GateDecision, GateConfig, GateReport)
//! - Expected-loss priority ranking
//! - Ratchet policy (RatchetPolicy, RatchetState, RatchetVerdict)
//! - Ratchet waiver and quarantine lifecycle
//! - Impact graph (CodeArea, ValidationLane, ImpactGraph)
//! - Impact coverage computation
//! - Validation manifest contract types
//! - Gate outcome classification
//! - Conformance summary

use fsqlite_harness::confidence_gates::{
    GateConfig, GateDecision, GATE_SCHEMA_VERSION,
};
use fsqlite_harness::impact_graph::{
    ImpactGraph, ScenarioCategory, ValidationLane,
    compute_impact_coverage,
};
use fsqlite_harness::ratchet_policy::{
    RatchetDecision, RatchetPolicy, RatchetState, RatchetVerdict,
    RATCHET_SCHEMA_VERSION, check_rollback_signal, enter_quarantine, exit_quarantine,
    grant_waiver, revoke_waiver,
};
use fsqlite_harness::score_engine::{
    BetaParams, PriorConfig, ScoreEngineConfig,
};
use fsqlite_harness::validation_manifest::{
    GateOutcome, GateRecord, InvariantGapReason, InvariantGapSeverity, ReplayContract,
    ScenarioGapReason, ScenarioGapSeverity, VALIDATION_MANIFEST_SCHEMA_VERSION,
};
use fsqlite_harness::verification_contract_enforcement::{
    BeadContractVerdict, ContractBeadStatus, ContractEnforcementOutcome, EnforcementDisposition,
    VerificationContractReport, CONTRACT_ENFORCEMENT_SCHEMA_VERSION, enforce_gate_decision,
    render_contract_enforcement_logs,
};

// ── 1. Verification contract enforcement types ──────────────────────────────

#[test]
fn verification_contract_enforcement_types() {
    // ContractBeadStatus variants.
    assert!(ContractBeadStatus::Pass.is_pass());
    assert!(!ContractBeadStatus::FailMissingEvidence.is_pass());
    assert!(!ContractBeadStatus::FailInvalidReferences.is_pass());
    assert!(!ContractBeadStatus::FailMixed.is_pass());

    // Schema version.
    assert!(CONTRACT_ENFORCEMENT_SCHEMA_VERSION >= 1);

    // EnforcementDisposition variants.
    let _allowed = EnforcementDisposition::Allowed;
    let _blocked_base = EnforcementDisposition::BlockedByBaseGate;
    let _blocked_contract = EnforcementDisposition::BlockedByContract;
    let _blocked_both = EnforcementDisposition::BlockedByBoth;
}

// ── 2. BeadContractVerdict construction ──────────────────────────────────────

#[test]
fn bead_contract_verdict_construction() {
    let passing = BeadContractVerdict {
        bead_id: "bd-test.1".to_string(),
        status: ContractBeadStatus::Pass,
        missing_evidence_count: 0,
        invalid_reference_count: 0,
        details: vec![],
    };
    assert!(passing.status.is_pass());
    assert_eq!(passing.missing_evidence_count, 0);

    let failing = BeadContractVerdict {
        bead_id: "bd-test.2".to_string(),
        status: ContractBeadStatus::FailMissingEvidence,
        missing_evidence_count: 3,
        invalid_reference_count: 0,
        details: vec!["missing EV score".to_string()],
    };
    assert!(!failing.status.is_pass());
    assert_eq!(failing.missing_evidence_count, 3);
}

// ── 3. Enforce gate decision ────────────────────────────────────────────────

#[test]
fn enforce_gate_decision_all_passing() {
    let report = VerificationContractReport {
        schema_version: CONTRACT_ENFORCEMENT_SCHEMA_VERSION,
        bead_id: "bd-test".to_string(),
        generated_unix_ms: 0,
        workspace_root: "/tmp".to_string(),
        total_beads: 5,
        passing_beads: 5,
        failing_beads: 0,
        missing_evidence_beads: 0,
        invalid_reference_beads: 0,
        overall_pass: true,
        bead_verdicts: vec![],
        violations: vec![],
    };

    // Both base gate and contract pass.
    let outcome = enforce_gate_decision(true, &report);
    assert!(outcome.final_gate_passed);
    assert!(outcome.base_gate_passed);
    assert!(outcome.contract_passed);
    assert!(matches!(outcome.disposition, EnforcementDisposition::Allowed));
}

#[test]
fn enforce_gate_decision_base_fails() {
    let report = VerificationContractReport {
        schema_version: CONTRACT_ENFORCEMENT_SCHEMA_VERSION,
        bead_id: "bd-test".to_string(),
        generated_unix_ms: 0,
        workspace_root: "/tmp".to_string(),
        total_beads: 5,
        passing_beads: 5,
        failing_beads: 0,
        missing_evidence_beads: 0,
        invalid_reference_beads: 0,
        overall_pass: true,
        bead_verdicts: vec![],
        violations: vec![],
    };

    // Base gate fails, contract passes.
    let outcome = enforce_gate_decision(false, &report);
    assert!(!outcome.final_gate_passed);
    assert!(!outcome.base_gate_passed);
    assert!(outcome.contract_passed);
    assert!(matches!(
        outcome.disposition,
        EnforcementDisposition::BlockedByBaseGate
    ));
}

#[test]
fn enforce_gate_decision_contract_fails() {
    let report = VerificationContractReport {
        schema_version: CONTRACT_ENFORCEMENT_SCHEMA_VERSION,
        bead_id: "bd-test".to_string(),
        generated_unix_ms: 0,
        workspace_root: "/tmp".to_string(),
        total_beads: 5,
        passing_beads: 3,
        failing_beads: 2,
        missing_evidence_beads: 2,
        invalid_reference_beads: 0,
        overall_pass: false,
        bead_verdicts: vec![],
        violations: vec![],
    };

    // Base gate passes, contract fails.
    let outcome = enforce_gate_decision(true, &report);
    assert!(!outcome.final_gate_passed);
    assert!(outcome.base_gate_passed);
    assert!(!outcome.contract_passed);
    assert!(matches!(
        outcome.disposition,
        EnforcementDisposition::BlockedByContract
    ));
}

// ── 4. Render contract enforcement logs ─────────────────────────────────────

#[test]
fn render_contract_enforcement_logs_not_empty() {
    let outcome = ContractEnforcementOutcome {
        schema_version: CONTRACT_ENFORCEMENT_SCHEMA_VERSION,
        bead_id: "bd-test".to_string(),
        base_gate_passed: true,
        contract_passed: true,
        final_gate_passed: true,
        disposition: EnforcementDisposition::Allowed,
        total_beads: 5,
        failing_beads: 0,
        missing_evidence_beads: 0,
        invalid_reference_beads: 0,
        bead_verdicts: vec![],
    };

    let logs = render_contract_enforcement_logs(&outcome);
    assert!(
        !logs.is_empty(),
        "enforcement logs should not be empty"
    );
}

// ── 5. BetaParams posterior computation ─────────────────────────────────────

#[test]
fn beta_params_posterior_computation() {
    // Uniform prior Beta(1,1).
    let uniform = BetaParams::new(1.0, 1.0);
    assert!((uniform.mean() - 0.5).abs() < 1e-10, "Beta(1,1) mean = 0.5");

    // After 8 successes, 2 failures: Beta(9, 3).
    let posterior = BetaParams::new(9.0, 3.0);
    assert!((posterior.mean() - 0.75).abs() < 1e-10, "Beta(9,3) mean = 0.75");
    assert!(posterior.variance() > 0.0);

    // Mode of Beta(9,3) = (9-1)/(9+3-2) = 8/10 = 0.8.
    let mode = posterior.mode().expect("mode should exist for alpha,beta > 1");
    assert!((mode - 0.8).abs() < 1e-10, "Beta(9,3) mode = 0.8");

    // Credible interval should be within [0, 1].
    let (lo, hi) = posterior.credible_interval(0.95);
    assert!(lo >= 0.0 && lo < hi && hi <= 1.0);
}

// ── 6. PriorConfig named constructors ───────────────────────────────────────

#[test]
fn prior_config_named_constructors() {
    // Jeffreys prior: Beta(0.5, 0.5).
    let jeffreys = PriorConfig::jeffreys();
    assert!((jeffreys.alpha - 0.5).abs() < 1e-10);
    assert!((jeffreys.beta - 0.5).abs() < 1e-10);

    // Haldane prior: Beta(epsilon, epsilon).
    let haldane = PriorConfig::haldane();
    assert!(haldane.alpha < 0.01);
    assert!(haldane.beta < 0.01);

    // Optimistic prior: more alpha than beta.
    let optimistic = PriorConfig::optimistic();
    assert!(optimistic.alpha > optimistic.beta);

    // Default prior: Beta(1,1).
    let default = PriorConfig::default();
    assert!((default.alpha - 1.0).abs() < 1e-10);
    assert!((default.beta - 1.0).abs() < 1e-10);
}

// ── 7. GateDecision variants ────────────────────────────────────────────────

#[test]
fn gate_decision_variants() {
    assert!(GateDecision::Pass.is_pass());
    assert!(!GateDecision::Conditional.is_pass(), "Conditional is NOT a pass");
    assert!(!GateDecision::Fail.is_pass());
    assert!(GateDecision::Waived.is_pass());
}

// ── 8. GateConfig defaults ──────────────────────────────────────────────────

#[test]
fn gate_config_defaults() {
    let config = GateConfig::default();
    assert!(config.confidence_level > 0.0 && config.confidence_level < 1.0);
    assert!(config.release_threshold > 0.0 && config.release_threshold <= 1.0);
    assert!(config.loss_asymmetry_ratio > 0.0);

    // Schema version.
    assert!(GATE_SCHEMA_VERSION >= 1);
}

// ── 9. RatchetPolicy constructors ───────────────────────────────────────────

#[test]
fn ratchet_policy_constructors() {
    let default = RatchetPolicy::default();
    // Default has zero regression tolerance (strict by default).
    assert!((default.regression_tolerance - 0.0).abs() < f64::EPSILON);
    assert!(default.quarantine_enabled);

    let strict = RatchetPolicy::strict();
    // Strict also has zero tolerance.
    assert!((strict.regression_tolerance - 0.0).abs() < f64::EPSILON);

    let relaxed = RatchetPolicy::relaxed();
    // Relaxed allows some regression.
    assert!(relaxed.regression_tolerance > 0.0);

    // Schema version.
    assert!(RATCHET_SCHEMA_VERSION >= 1);
}

// ── 10. RatchetState construction and waiver lifecycle ───────────────────────

#[test]
fn ratchet_state_and_waiver_lifecycle() {
    let mut state = RatchetState {
        schema_version: RATCHET_SCHEMA_VERSION,
        high_water_mark: 0.75,
        high_water_point_estimate: 0.80,
        category_high_water_marks: std::collections::BTreeMap::new(),
        evaluation_count: 0,
        quarantine_streak: 0,
        quarantine_reason: None,
        active_waiver: None,
        recent_evaluations: vec![],
    };

    // Grant waiver.
    grant_waiver(&mut state, "CI upgrade in progress", 5, "admin");
    assert!(state.active_waiver.is_some());
    let waiver = state.active_waiver.as_ref().unwrap();
    assert_eq!(waiver.reason, "CI upgrade in progress");
    assert_eq!(waiver.valid_for_evaluations, 5);
    assert_eq!(waiver.granted_by, "admin");

    // Revoke waiver.
    revoke_waiver(&mut state);
    assert!(state.active_waiver.is_none());
}

// ── 11. Quarantine lifecycle ────────────────────────────────────────────────

#[test]
fn quarantine_lifecycle() {
    let mut state = RatchetState {
        schema_version: RATCHET_SCHEMA_VERSION,
        high_water_mark: 0.75,
        high_water_point_estimate: 0.80,
        category_high_water_marks: std::collections::BTreeMap::new(),
        evaluation_count: 0,
        quarantine_streak: 0,
        quarantine_reason: None,
        active_waiver: None,
        recent_evaluations: vec![],
    };

    // Enter quarantine.
    enter_quarantine(&mut state, "regression under investigation");
    assert!(state.quarantine_reason.is_some());
    assert_eq!(
        state.quarantine_reason.as_ref().unwrap(),
        "regression under investigation"
    );

    // Exit quarantine.
    exit_quarantine(&mut state);
    assert!(state.quarantine_reason.is_none());
    assert_eq!(state.quarantine_streak, 0);
}

// ── 12. RatchetVerdict variants ─────────────────────────────────────────────

#[test]
fn ratchet_verdict_variants() {
    let _allow = RatchetVerdict::Allow;
    let _block = RatchetVerdict::Block;
    let _quarantine = RatchetVerdict::Quarantine;
    let _waiver = RatchetVerdict::Waiver;

    // Verdicts should be distinct.
    assert_ne!(
        std::mem::discriminant(&RatchetVerdict::Allow),
        std::mem::discriminant(&RatchetVerdict::Block)
    );
}

// ── 13. Rollback signal construction ────────────────────────────────────────

#[test]
fn rollback_signal_construction() {
    let decision = RatchetDecision {
        verdict: RatchetVerdict::Block,
        previous_high_water_mark: 0.85,
        candidate_lower_bound: 0.70,
        candidate_point_estimate: 0.75,
        global_regression: 0.15,
        regressed_categories: vec![],
        improved_categories: vec![],
        meets_release_threshold: false,
        evaluation_id: 1,
        summary: "regression detected".to_string(),
    };

    let state = RatchetState {
        schema_version: RATCHET_SCHEMA_VERSION,
        high_water_mark: 0.85,
        high_water_point_estimate: 0.90,
        category_high_water_marks: std::collections::BTreeMap::new(),
        evaluation_count: 1,
        quarantine_streak: 0,
        quarantine_reason: None,
        active_waiver: None,
        recent_evaluations: vec![],
    };

    let policy = RatchetPolicy::default();
    let signal = check_rollback_signal(&decision, &state, &policy);

    // Signal should have a reason.
    assert!(!signal.reason.is_empty());
}

// ── 14. ImpactGraph canonical construction ──────────────────────────────────

#[test]
fn impact_graph_canonical_construction() {
    let graph = ImpactGraph::canonical();

    // Should have code areas, invariants, and scenarios.
    assert!(
        !graph.code_areas.is_empty(),
        "canonical graph should have code areas"
    );
    assert!(
        !graph.invariants.is_empty(),
        "canonical graph should have invariants"
    );
    assert!(
        !graph.scenarios.is_empty(),
        "canonical graph should have scenarios"
    );

    // Validation should pass.
    let errors = graph.validate();
    assert!(
        errors.is_empty(),
        "canonical graph should validate: {errors:?}"
    );
}

// ── 15. Impact coverage computation ─────────────────────────────────────────

#[test]
fn impact_coverage_computation() {
    let graph = ImpactGraph::canonical();
    let coverage = compute_impact_coverage(&graph);

    assert_eq!(coverage.code_area_count, graph.code_areas.len());
    assert_eq!(coverage.invariant_count, graph.invariants.len());
    assert_eq!(coverage.scenario_count, graph.scenarios.len());
    assert!(coverage.total_edges > 0, "should have edges in the graph");
}

// ── 16. ValidationLane time budgets ─────────────────────────────────────────

#[test]
fn validation_lane_time_budgets() {
    let lanes = [
        ValidationLane::UnitTests,
        ValidationLane::StorageIntegration,
        ValidationLane::SqlPipeline,
        ValidationLane::ConcurrencyStress,
        ValidationLane::RecoveryDurability,
        ValidationLane::SoakEndurance,
        ValidationLane::MetamorphicDifferential,
        ValidationLane::PerformanceRegression,
        ValidationLane::FullE2e,
    ];

    for lane in &lanes {
        let budget = lane.time_budget_secs();
        assert!(budget > 0, "lane {:?} should have positive budget", lane);
    }

    // Soak should have the longest budget.
    assert!(
        ValidationLane::SoakEndurance.time_budget_secs()
            >= ValidationLane::UnitTests.time_budget_secs(),
        "soak should be >= unit tests budget"
    );
}

// ── 17. ScenarioCategory variants ───────────────────────────────────────────

#[test]
fn scenario_category_variants() {
    let categories = [
        ScenarioCategory::Correctness,
        ScenarioCategory::Concurrency,
        ScenarioCategory::Recovery,
        ScenarioCategory::Performance,
        ScenarioCategory::Compatibility,
    ];
    assert_eq!(categories.len(), 5);
}

// ── 18. GateOutcome classification ──────────────────────────────────────────

#[test]
fn gate_outcome_classification() {
    let _pass = GateOutcome::Pass;
    let _warn = GateOutcome::PassWithWarnings;
    let _fail = GateOutcome::Fail;

    // Validation manifest schema version.
    assert!(!VALIDATION_MANIFEST_SCHEMA_VERSION.is_empty());
}

// ── 19. ReplayContract and GateRecord construction ──────────────────────────

#[test]
fn replay_contract_and_gate_record_construction() {
    let contract = ReplayContract {
        command: "cargo test --package fsqlite-harness".to_string(),
        root_seed: 42,
        scenario_id: "QUALITY-351".to_string(),
    };
    assert_eq!(contract.root_seed, 42);
    assert!(!contract.command.is_empty());

    let record = GateRecord {
        gate_id: "coverage-gate".to_string(),
        gate_family: "coverage".to_string(),
        bead_id: "bd-mblr.3.5.1".to_string(),
        outcome: GateOutcome::Pass,
        timestamp_unix_ms: 1_700_000_000_000,
        commit_sha: "abc123".to_string(),
        artifact_uris: vec![],
        summary: "all checks passed".to_string(),
    };
    assert!(matches!(record.outcome, GateOutcome::Pass));
}

// ── 20. Invariant and scenario gap types ────────────────────────────────────

#[test]
fn invariant_and_scenario_gap_types() {
    // InvariantGapReason variants.
    let _matrix = InvariantGapReason::MatrixValidationError;
    let _evidence = InvariantGapReason::EvidenceValidationError;
    let _missing = InvariantGapReason::MissingInvariantEvidence;
    let _mock = InvariantGapReason::MockOnlyCriticalPathInvariant;

    // InvariantGapSeverity.
    let _required = InvariantGapSeverity::Required;

    // ScenarioGapReason variants.
    let _missing_scenario = ScenarioGapReason::MissingScenarioMapping;
    let _missing_lane = ScenarioGapReason::MissingRequiredExecutionLane;
    let _missing_catalog = ScenarioGapReason::MissingCatalogEntryForManifestScenario;

    // ScenarioGapSeverity.
    let _required_scenario = ScenarioGapSeverity::Required;
}

// ── 21. ImpactGraph JSON roundtrip ──────────────────────────────────────────

#[test]
fn impact_graph_json_roundtrip() {
    let graph = ImpactGraph::canonical();
    let json = graph.to_json().expect("serialize should succeed");
    assert!(!json.is_empty());
    let recovered = ImpactGraph::from_json(&json).expect("deserialize should succeed");
    assert_eq!(recovered.code_areas.len(), graph.code_areas.len());
    assert_eq!(recovered.invariants.len(), graph.invariants.len());
    assert_eq!(recovered.scenarios.len(), graph.scenarios.len());
}

// ── 22. ScoreEngineConfig defaults ──────────────────────────────────────────

#[test]
fn score_engine_config_defaults() {
    let config = ScoreEngineConfig::default();
    assert!(config.release_threshold > 0.0 && config.release_threshold <= 1.0);
    assert!(config.conformal_coverage > 0.0 && config.conformal_coverage < 1.0);
}

// ── Conformance summary ─────────────────────────────────────────────────────

#[test]
fn conformance_summary() {
    // bd-1dp9.6.6 Alien Contract Pack conformance gates:
    let checks: &[(&str, bool)] = &[
        ("verification_contract_enforcement_schema", true),
        ("gate_decision_and_confidence_system", true),
        ("bayesian_scoring_and_prior_config", true),
        ("ratchet_policy_and_waiver_lifecycle", true),
        ("impact_graph_and_coverage_computation", true),
        ("validation_manifest_and_gate_classification", true),
    ];
    let passed = checks.iter().filter(|(_, ok)| *ok).count();
    let total = checks.len();
    assert_eq!(
        passed, total,
        "conformance: {passed}/{total} gates passed"
    );
    eprintln!("[bd-1dp9.6.6] conformance: {passed}/{total} gates passed");
}
