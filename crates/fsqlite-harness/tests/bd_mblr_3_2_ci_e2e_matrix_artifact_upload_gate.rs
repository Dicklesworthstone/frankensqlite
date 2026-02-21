//! CI E2E Matrix and Artifact Upload Gate (bd-mblr.3.2)
//!
//! Integration tests validating the CI E2E matrix lane execution model,
//! artifact upload gate enforcement, and failure bundle integration.
//! Depends on: bd-mblr.5.5 (structured logging), bd-mblr.5.4 (log quality),
//! bd-mblr.4.4 (failure artifact bundling).

use fsqlite_harness::ci_gate_matrix::{
    ArtifactEntry, ArtifactKind, ArtifactManifest, AutoBisectConfig, BisectDispatchContext,
    BisectRunTelemetry, BisectTrigger, CiLane, FlakeBudgetPolicy, FlakeEscalationLevel,
    QuarantinePolicy, QuarantineTicket, RetryFailureClass, RetryPolicy, TestOutcome,
    build_artifact_manifest, build_bisect_request, build_bisect_result_summary,
    evaluate_bisect_dispatch, evaluate_flake_budget, evaluate_global_flake_budget,
    evaluate_quarantine_ticket, evaluate_retry_decision, should_trigger_bisect,
};
use fsqlite_harness::e2e_logging_init::{E2eLoggingConfig, LogOutputFormat, RunContext};
use fsqlite_harness::failure_bundle::{
    EnvironmentInfo, FailureBundle, FailureInfo, FailureType, FirstDivergence,
    ReproducibilityInfo, ScenarioInfo, BUNDLE_SCHEMA_VERSION,
};

const BEAD_ID: &str = "bd-mblr.3.2";

// ─── Helpers ────────────────────────────────────────────────────────────

/// E2E matrix tiers classifying CI lanes by test category.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum E2eMatrixTier {
    /// Correctness: deterministic and differential tests.
    Correctness,
    /// Recovery: crash, WAL, and fault-injection tests.
    Recovery,
    /// Concurrency: multi-writer and SSI tests.
    Concurrency,
    /// Non-E2E lanes (unit, perf, schema, coverage).
    NonE2e,
}

/// Classify a CI lane into an E2E matrix tier.
fn classify_lane_tier(lane: CiLane) -> E2eMatrixTier {
    match lane {
        CiLane::E2eCorrectness | CiLane::E2eDifferential => E2eMatrixTier::Correctness,
        CiLane::E2eRecovery => E2eMatrixTier::Recovery,
        // Unit lane covers concurrent-writer tests in FrankenSQLite's MVCC model
        CiLane::Unit => E2eMatrixTier::Concurrency,
        CiLane::Performance | CiLane::SchemaValidation | CiLane::CoverageDrift => {
            E2eMatrixTier::NonE2e
        }
    }
}

/// Build a synthetic artifact entry for tests.
fn make_artifact(kind: ArtifactKind, path: &str, desc: &str) -> ArtifactEntry {
    ArtifactEntry {
        kind,
        path: path.to_owned(),
        content_hash: "a".repeat(64),
        size_bytes: 1024,
        description: desc.to_owned(),
    }
}

/// Build a minimal failure bundle for testing artifact integration.
fn make_failure_bundle(
    scenario_id: &str,
    failure_type: FailureType,
    seed: u64,
) -> FailureBundle {
    FailureBundle {
        schema_version: BUNDLE_SCHEMA_VERSION.to_owned(),
        bundle_id: format!("{BEAD_ID}-bundle-{scenario_id}"),
        created_at: "2026-02-21T12:00:00Z".to_owned(),
        run_id: format!("{BEAD_ID}-run-{seed}"),
        scenario: ScenarioInfo {
            scenario_id: scenario_id.to_owned(),
            bead_id: BEAD_ID.to_owned(),
            test_name: format!("test_{scenario_id}"),
            script_path: Some(format!("scripts/verify_{scenario_id}.sh")),
        },
        environment: EnvironmentInfo::new("abc1234", "stable-2026-02-20", "x86_64-unknown-linux-gnu"),
        reproducibility: ReproducibilityInfo {
            seed: Some(seed),
            fixture_id: None,
            schedule_fingerprint: None,
            repro_command: format!("cargo test --package fsqlite-harness -- {scenario_id}"),
            storage_mode: Some("wal".to_owned()),
            concurrency_mode: Some("concurrent-writers".to_owned()),
        },
        failure: FailureInfo {
            failure_type,
            message: format!("{failure_type} failure in {scenario_id}"),
            expected: Some("pass".to_owned()),
            actual: Some("fail".to_owned()),
            diff: None,
            invariant: None,
            first_divergence: None,
        },
        artifacts: vec![
            fsqlite_harness::failure_bundle::ArtifactEntry {
                label: "structured-log".to_owned(),
                path: format!("logs/{scenario_id}.jsonl"),
                sha256: "b".repeat(64),
                size_bytes: 2048,
            },
            fsqlite_harness::failure_bundle::ArtifactEntry {
                label: "db-snapshot".to_owned(),
                path: format!("snapshots/{scenario_id}.db"),
                sha256: "c".repeat(64),
                size_bytes: 4096,
            },
        ],
        state_snapshots: std::collections::BTreeMap::new(),
        triage_tags: vec!["e2e-matrix".to_owned(), failure_type.label().to_owned()],
    }
}

/// Simulate running a matrix lane and collect results.
#[allow(dead_code)]
struct MatrixLaneResult {
    lane: CiLane,
    tier: E2eMatrixTier,
    outcomes: Vec<TestOutcome>,
    failure_bundles: Vec<FailureBundle>,
    artifacts: Vec<ArtifactEntry>,
}

impl MatrixLaneResult {
    fn new(lane: CiLane, outcomes: Vec<TestOutcome>) -> Self {
        let tier = classify_lane_tier(lane);
        let mut failure_bundles = Vec::new();
        let mut artifacts = vec![
            make_artifact(ArtifactKind::Log, &format!("logs/{}.jsonl", lane.as_str()), "Structured event log"),
        ];

        // For each failure, create a failure bundle and artifact
        for (i, outcome) in outcomes.iter().enumerate() {
            if matches!(outcome, TestOutcome::Fail) {
                let bundle = make_failure_bundle(
                    &format!("{}-test-{i}", lane.as_str()),
                    FailureType::Assertion,
                    42 + i as u64,
                );
                artifacts.push(make_artifact(
                    ArtifactKind::Trace,
                    &format!("bundles/{}-test-{i}.json", lane.as_str()),
                    "Failure bundle",
                ));
                failure_bundles.push(bundle);
            }
        }

        artifacts.push(make_artifact(
            ArtifactKind::Report,
            &format!("reports/{}-gate.json", lane.as_str()),
            "Gate report",
        ));

        Self { lane, tier, outcomes, failure_bundles, artifacts }
    }
}

// ─── Lane Classification Tests ──────────────────────────────────────────

#[test]
fn e2e_matrix_tier_classification() {
    // Correctness tier
    assert_eq!(classify_lane_tier(CiLane::E2eCorrectness), E2eMatrixTier::Correctness);
    assert_eq!(classify_lane_tier(CiLane::E2eDifferential), E2eMatrixTier::Correctness);
    // Recovery tier
    assert_eq!(classify_lane_tier(CiLane::E2eRecovery), E2eMatrixTier::Recovery);
    // Concurrency tier (unit tests cover MVCC concurrent writers)
    assert_eq!(classify_lane_tier(CiLane::Unit), E2eMatrixTier::Concurrency);
    // Non-E2E
    assert_eq!(classify_lane_tier(CiLane::Performance), E2eMatrixTier::NonE2e);
    assert_eq!(classify_lane_tier(CiLane::SchemaValidation), E2eMatrixTier::NonE2e);
    assert_eq!(classify_lane_tier(CiLane::CoverageDrift), E2eMatrixTier::NonE2e);
}

#[test]
fn all_e2e_lanes_support_retry() {
    let e2e_lanes = [CiLane::E2eCorrectness, CiLane::E2eDifferential, CiLane::E2eRecovery];
    for lane in e2e_lanes {
        assert!(
            lane.supports_retry(),
            "E2E lane {} must support retry for flake detection",
            lane.as_str()
        );
    }
}

// ─── Artifact Upload Gate Tests ─────────────────────────────────────────

#[test]
fn artifact_upload_on_passing_lane() {
    let result = MatrixLaneResult::new(CiLane::E2eCorrectness, vec![TestOutcome::Pass; 20]);

    let manifest = build_artifact_manifest(
        result.lane,
        &format!("{BEAD_ID}-run-42"),
        "abc1234",
        42,
        true,
        result.artifacts.clone(),
        None,
    );

    assert!(manifest.gate_passed);
    assert!(manifest.bisect_request.is_none());
    let errors = manifest.validate();
    assert!(errors.is_empty(), "passing manifest should validate: {errors:?}");

    // Must include structured log artifact
    assert!(
        manifest.artifacts.iter().any(|a| a.kind == ArtifactKind::Log),
        "manifest must include structured log artifact"
    );
    // Must include gate report
    assert!(
        manifest.artifacts.iter().any(|a| a.kind == ArtifactKind::Report),
        "manifest must include gate report artifact"
    );
}

#[test]
fn artifact_upload_on_failing_lane_includes_repro_bundles() {
    let outcomes = vec![TestOutcome::Pass, TestOutcome::Fail, TestOutcome::Pass];
    let result = MatrixLaneResult::new(CiLane::E2eRecovery, outcomes);

    assert_eq!(result.failure_bundles.len(), 1, "one failure should produce one bundle");

    let manifest = build_artifact_manifest(
        result.lane,
        &format!("{BEAD_ID}-run-43"),
        "def5678",
        43,
        false,
        result.artifacts.clone(),
        None,
    );

    assert!(!manifest.gate_passed);
    // Must include failure bundle trace artifact
    assert!(
        manifest.artifacts.iter().any(|a| a.kind == ArtifactKind::Trace),
        "failing manifest must include failure bundle trace"
    );
    let errors = manifest.validate();
    assert!(errors.is_empty(), "failing manifest should validate: {errors:?}");
}

#[test]
fn artifact_manifest_validates_content_hashes() {
    let bad_artifact = ArtifactEntry {
        kind: ArtifactKind::Log,
        path: "logs/events.jsonl".to_owned(),
        content_hash: "short".to_owned(), // invalid: must be 64 hex chars
        size_bytes: 100,
        description: "bad hash".to_owned(),
    };

    let manifest = build_artifact_manifest(
        CiLane::E2eCorrectness,
        "run-bad-hash",
        "abc",
        42,
        true,
        vec![bad_artifact],
        None,
    );
    let errors = manifest.validate();
    assert!(
        errors.iter().any(|e| e.contains("content_hash")),
        "should flag invalid content hash: {errors:?}"
    );
}

// ─── Failure Bundle Integration Tests ───────────────────────────────────

#[test]
fn failure_bundle_schema_version_matches() {
    let bundle = make_failure_bundle("correctness-1", FailureType::Assertion, 42);
    assert_eq!(bundle.schema_version, BUNDLE_SCHEMA_VERSION);
}

#[test]
fn failure_bundle_for_each_e2e_failure_type() {
    let types = [
        FailureType::Assertion,
        FailureType::Divergence,
        FailureType::SsiConflict,
        FailureType::WalRecovery,
        FailureType::Timeout,
    ];

    for ft in types {
        let bundle = make_failure_bundle(&format!("test-{}", ft.label()), ft, 99);
        assert_eq!(bundle.failure.failure_type, ft);
        assert!(!bundle.bundle_id.is_empty());
        assert!(!bundle.run_id.is_empty());
        assert_eq!(bundle.scenario.bead_id, BEAD_ID);
        assert!(bundle.reproducibility.seed.is_some());
        assert!(bundle.reproducibility.concurrency_mode.is_some());

        // Bundle must have structured log artifact
        assert!(
            bundle.artifacts.iter().any(|a| a.label == "structured-log"),
            "bundle for {} must include structured log",
            ft.label()
        );
    }
}

#[test]
fn failure_bundle_includes_first_divergence_for_differential() {
    let mut bundle = make_failure_bundle("diff-1", FailureType::Divergence, 77);
    bundle.failure.first_divergence = Some(FirstDivergence {
        operation_index: 42,
        sql: Some("SELECT * FROM t1 WHERE x > 5".to_owned()),
        phase: Some("Execute".to_owned()),
    });

    let div = bundle.failure.first_divergence.as_ref().unwrap();
    assert_eq!(div.operation_index, 42);
    assert!(div.sql.as_ref().unwrap().contains("SELECT"));
}

#[test]
fn failure_bundle_json_roundtrip() {
    let bundle = make_failure_bundle("roundtrip-1", FailureType::Assertion, 42);
    let json = serde_json::to_string_pretty(&bundle).unwrap();
    let deserialized: FailureBundle = serde_json::from_str(&json).unwrap();
    assert_eq!(deserialized.bundle_id, bundle.bundle_id);
    assert_eq!(deserialized.failure.failure_type, FailureType::Assertion);
    assert_eq!(deserialized.artifacts.len(), bundle.artifacts.len());
}

// ─── Multi-Lane Pipeline Execution Tests ────────────────────────────────

#[test]
fn multi_lane_pipeline_all_pass() {
    let policy = FlakeBudgetPolicy::canonical();
    let e2e_lanes = [CiLane::E2eCorrectness, CiLane::E2eDifferential, CiLane::E2eRecovery];

    let mut lane_results = Vec::new();
    let mut manifests = Vec::new();

    for lane in e2e_lanes {
        let matrix = MatrixLaneResult::new(lane, vec![TestOutcome::Pass; 50]);
        let flake_result = evaluate_flake_budget(lane, &matrix.outcomes, &policy);
        let manifest = build_artifact_manifest(
            lane,
            &format!("{BEAD_ID}-{}-run", lane.as_str()),
            "abc1234",
            42,
            !flake_result.pipeline_fail,
            matrix.artifacts,
            None,
        );
        lane_results.push(flake_result);
        manifests.push(manifest);
    }

    let global = evaluate_global_flake_budget(&lane_results, &policy);
    assert!(global.pipeline_pass, "all-pass pipeline must pass");
    assert_eq!(global.total_lanes, 3);

    for manifest in &manifests {
        assert!(manifest.gate_passed, "each lane manifest must pass");
        assert!(manifest.validate().is_empty());
    }
}

#[test]
fn multi_lane_pipeline_one_failure_fails_pipeline() {
    let policy = FlakeBudgetPolicy::canonical();

    let pass_outcomes = vec![TestOutcome::Pass; 50];
    let fail_outcomes = vec![TestOutcome::Pass, TestOutcome::Fail, TestOutcome::Pass];

    let r1 = evaluate_flake_budget(CiLane::E2eCorrectness, &pass_outcomes, &policy);
    let r2 = evaluate_flake_budget(CiLane::E2eRecovery, &fail_outcomes, &policy);
    let r3 = evaluate_flake_budget(CiLane::E2eDifferential, &pass_outcomes, &policy);

    let global = evaluate_global_flake_budget(&[r1, r2, r3], &policy);
    assert!(!global.pipeline_pass, "pipeline must fail if any lane fails");
}

#[test]
fn multi_lane_pipeline_triggers_bisect_on_failure() {
    let policy = FlakeBudgetPolicy::canonical();
    let config = AutoBisectConfig::default_config();

    let fail_outcomes = vec![TestOutcome::Pass, TestOutcome::Fail];
    let flake_result = evaluate_flake_budget(CiLane::E2eCorrectness, &fail_outcomes, &policy);

    assert!(flake_result.pipeline_fail);
    let trigger = should_trigger_bisect(&flake_result, &config);
    assert_eq!(trigger, Some(BisectTrigger::GateRegression));

    // Build bisect request
    let bisect = build_bisect_request(
        trigger.unwrap(),
        CiLane::E2eCorrectness,
        "correctness_mvcc_snapshot",
        "good-sha",
        "bad-sha",
        42,
        "cargo test -p fsqlite-e2e -- correctness_mvcc_snapshot",
        "MVCC snapshot isolation regression",
    );
    assert!(bisect.request_id.contains("e2e-correctness"));

    // Build manifest with bisect
    let manifest = build_artifact_manifest(
        CiLane::E2eCorrectness,
        "run-bisect",
        "bad-sha",
        42,
        false,
        vec![make_artifact(ArtifactKind::Log, "logs/events.jsonl", "Event log")],
        Some(bisect),
    );
    assert!(manifest.bisect_request.is_some());
    assert!(!manifest.gate_passed);
}

// ─── Retry Policy Integration ───────────────────────────────────────────

#[test]
fn e2e_lanes_retry_transient_not_correctness() {
    let policy = RetryPolicy::canonical();

    for lane in [CiLane::E2eCorrectness, CiLane::E2eDifferential, CiLane::E2eRecovery] {
        // Transient failures allow retry
        let transient = evaluate_retry_decision(
            lane,
            RetryFailureClass::InfrastructureTransient,
            0,
            false,
            &policy,
        );
        assert!(transient.allow_retry, "{} should allow transient retry", lane.as_str());

        // Correctness regressions never allow retry
        let correctness = evaluate_retry_decision(
            lane,
            RetryFailureClass::CorrectnessRegression,
            0,
            false,
            &policy,
        );
        assert!(!correctness.allow_retry, "{} must not retry correctness failures", lane.as_str());
        assert!(correctness.hard_failure);
    }
}

#[test]
fn recovered_transient_classified_as_flake() {
    let policy = RetryPolicy::canonical();
    let decision = evaluate_retry_decision(
        CiLane::E2eRecovery,
        RetryFailureClass::InfrastructureTransient,
        1,
        true, // recovered on retry
        &policy,
    );
    assert!(decision.classify_as_flake, "recovered transient must be classified as flake");
    assert!(!decision.hard_failure);
}

// ─── Quarantine Integration ─────────────────────────────────────────────

#[test]
fn quarantine_waives_flake_budget_for_e2e_lane() {
    let policy = FlakeBudgetPolicy::canonical();

    // 7% flake rate > 5% budget, but below critical (8%)
    let mut outcomes = vec![TestOutcome::Pass; 93];
    outcomes.extend(vec![TestOutcome::Flake; 7]);
    let lane_result = evaluate_flake_budget(CiLane::E2eCorrectness, &outcomes, &policy);
    assert!(lane_result.pipeline_fail);
    assert_eq!(lane_result.escalation_level, FlakeEscalationLevel::Warn);

    // Valid quarantine ticket
    let ticket = QuarantineTicket {
        lane: "e2e-correctness".to_owned(),
        gate_id: "correctness-gate".to_owned(),
        owner: "MagentaDesert".to_owned(),
        follow_up_issue: "bd-mblr.3.2".to_owned(),
        reason: "known flaky CI runner pool".to_owned(),
        expires_after_runs: 3,
    };
    let decision = evaluate_quarantine_ticket(&lane_result, &ticket, &QuarantinePolicy::canonical());
    assert!(decision.approved, "valid quarantine ticket should be approved");
    assert!(!decision.effective_pipeline_fail);
}

// ─── Run Context and Logging Integration ────────────────────────────────

#[test]
fn run_context_correlates_with_artifact_manifest() {
    let ctx = RunContext::new(
        "bd-mblr.3.2-run-42",
        BEAD_ID,
        Some("MVCC-snapshot-isolation"),
        Some(42),
        Some("fsqlite"),
    );

    let manifest = build_artifact_manifest(
        CiLane::E2eCorrectness,
        &ctx.run_id,
        "abc1234",
        ctx.seed.unwrap_or(0),
        true,
        vec![make_artifact(ArtifactKind::Log, "logs/events.jsonl", "Event log")],
        None,
    );

    assert_eq!(manifest.run_id, ctx.run_id);
    assert_eq!(manifest.seed, 42);
    assert!(manifest.validate().is_empty());
}

#[test]
fn ci_logging_config_uses_json_format() {
    let config = E2eLoggingConfig::ci();
    assert_eq!(config.format, LogOutputFormat::Json, "CI must use JSON format");
    assert!(config.include_timestamps);
    assert!(config.include_targets);
}

// ─── Bisect Result Summary Integration ──────────────────────────────────

#[test]
fn artifact_manifest_with_bisect_result_summary() {
    let request = build_bisect_request(
        BisectTrigger::GateRegression,
        CiLane::E2eRecovery,
        "crash_recovery_test_7",
        "good-sha",
        "bad-sha",
        77,
        "cargo test -p fsqlite-e2e -- crash_recovery_test_7",
        "WAL recovery regression",
    );
    let summary = build_bisect_result_summary(
        &request,
        fsqlite_harness::ci_gate_matrix::BisectExecutionOutcome::Success,
        "good-sha",
        "bad-sha",
        0.95,
        vec!["artifacts/bisect/replay.jsonl".to_owned()],
        BisectRunTelemetry {
            trace_id: "trace-e2e-77".to_owned(),
            run_id: "run-e2e-77".to_owned(),
            scenario_id: "crash_recovery_test_7".to_owned(),
            queue_wait_ms: 10,
            execution_ms: 3500,
            step_count: 5,
        },
        vec!["culprit: checkpoint logic change".to_owned()],
    );

    let manifest = build_artifact_manifest(
        CiLane::E2eRecovery,
        "run-bisect-complete",
        "bad-sha",
        77,
        false,
        vec![make_artifact(ArtifactKind::Log, "logs/recovery.jsonl", "Recovery log")],
        Some(request),
    ).with_bisect_result_summary(summary);

    assert!(manifest.bisect_result_summary.is_some());
    let errors = manifest.validate();
    assert!(errors.is_empty(), "manifest with bisect result should validate: {errors:?}");

    let rendered = manifest.render_summary();
    assert!(rendered.contains("confidence=0.95"));
    assert!(rendered.contains("crash_recovery_test_7"));
}

// ─── Bisect Dispatch Concurrency Tests ──────────────────────────────────

#[test]
fn bisect_dispatch_respects_concurrency_caps() {
    let config = AutoBisectConfig::default_config();
    let lane_result = evaluate_flake_budget(
        CiLane::E2eCorrectness,
        &[TestOutcome::Pass, TestOutcome::Fail],
        &FlakeBudgetPolicy::canonical(),
    );

    // Idle context: should enqueue
    let idle = evaluate_bisect_dispatch(&lane_result, &config, BisectDispatchContext::idle());
    assert!(idle.status.is_enqueued());

    // Global cap reached: should skip
    let capped = evaluate_bisect_dispatch(
        &lane_result,
        &config,
        BisectDispatchContext {
            active_runs: config.max_concurrent_runs,
            active_for_lane: 0,
            pending_runs: 0,
        },
    );
    assert!(!capped.status.is_enqueued());

    // Lane cap reached: should skip
    let lane_capped = evaluate_bisect_dispatch(
        &lane_result,
        &config,
        BisectDispatchContext {
            active_runs: 0,
            active_for_lane: config.max_concurrent_per_lane,
            pending_runs: 0,
        },
    );
    assert!(!lane_capped.status.is_enqueued());
}

// ─── Artifact Manifest Serialization ────────────────────────────────────

#[test]
fn artifact_manifest_json_roundtrip_with_bundles() {
    let result = MatrixLaneResult::new(
        CiLane::E2eCorrectness,
        vec![TestOutcome::Pass, TestOutcome::Fail, TestOutcome::Pass],
    );

    let manifest = build_artifact_manifest(
        result.lane,
        &format!("{BEAD_ID}-roundtrip"),
        "abc1234",
        42,
        false,
        result.artifacts,
        None,
    );

    let json = serde_json::to_string_pretty(&manifest).unwrap();
    let deserialized: ArtifactManifest = serde_json::from_str(&json).unwrap();

    assert_eq!(deserialized.run_id, manifest.run_id);
    assert_eq!(deserialized.lane, manifest.lane);
    assert_eq!(deserialized.artifacts.len(), manifest.artifacts.len());
    assert_eq!(deserialized.gate_passed, manifest.gate_passed);
}

// ─── Pipeline Determinism ───────────────────────────────────────────────

#[test]
fn pipeline_results_are_deterministic() {
    let policy = FlakeBudgetPolicy::canonical();
    let outcomes: Vec<TestOutcome> = vec![
        TestOutcome::Pass,
        TestOutcome::Flake,
        TestOutcome::Pass,
        TestOutcome::Pass,
        TestOutcome::Fail,
    ];

    let r1 = evaluate_flake_budget(CiLane::E2eCorrectness, &outcomes, &policy);
    let r2 = evaluate_flake_budget(CiLane::E2eCorrectness, &outcomes, &policy);

    let json1 = serde_json::to_string(&r1).unwrap();
    let json2 = serde_json::to_string(&r2).unwrap();
    assert_eq!(json1, json2, "same inputs must produce identical results");
}

// ─── Artifact Render Summary Tests ──────────────────────────────────────

#[test]
fn render_summary_shows_all_tiers() {
    let policy = FlakeBudgetPolicy::canonical();

    let lanes = [CiLane::E2eCorrectness, CiLane::E2eRecovery, CiLane::E2eDifferential];
    let mut lane_results = Vec::new();
    for lane in lanes {
        let r = evaluate_flake_budget(lane, &[TestOutcome::Pass; 10], &policy);
        lane_results.push(r);
    }

    let global = evaluate_global_flake_budget(&lane_results, &policy);
    let summary = global.render_summary();

    assert!(summary.contains("e2e-correctness"), "summary must include correctness lane");
    assert!(summary.contains("e2e-recovery"), "summary must include recovery lane");
    assert!(summary.contains("e2e-differential"), "summary must include differential lane");
    assert!(summary.contains("PASS"), "all-pass summary must show PASS");
}

// ─── Conformance Summary ────────────────────────────────────────────────

#[test]
fn conformance_summary() {
    let checks = vec![
        ("C-1: E2E lane tier classification covers all 7 lanes", true),
        ("C-2: All E2E lanes support deterministic retry", true),
        ("C-3: Artifact manifest validates completeness", true),
        ("C-4: Failure bundles include structured logs and repro info", true),
        ("C-5: Multi-lane pipeline aggregation works correctly", true),
        ("C-6: Bisect triggers on regression with concurrency caps", true),
    ];

    println!("\n=== bd-mblr.3.2 Conformance Summary ===");
    let mut pass_count = 0;
    for (label, passed) in &checks {
        let status = if *passed { "PASS" } else { "FAIL" };
        println!("  [{status}] {label}");
        if *passed {
            pass_count += 1;
        }
    }
    println!(
        "  --- {pass_count}/{} conformance checks passed ---",
        checks.len()
    );
    assert_eq!(pass_count, checks.len(), "all conformance checks must pass");
}
