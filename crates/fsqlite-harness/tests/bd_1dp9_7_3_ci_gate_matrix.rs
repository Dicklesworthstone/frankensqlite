//! E2E tests for bd-1dp9.7.3: CI gate matrix, flake budgets, auto-bisect hooks, artifact publication.
//!
//! Exercises the full CI pipeline simulation: define lanes → evaluate flake budgets →
//! trigger auto-bisect → publish artifact manifests → verify deterministic output.

use fsqlite_harness::ci_gate_matrix::{
    ArtifactEntry, ArtifactKind, AutoBisectConfig, BisectDispatchContext, BisectDispatchStatus,
    BisectExecutionOutcome, BisectRunTelemetry, BisectTrigger, CiLane, FlakeBudgetPolicy,
    QuarantinePolicy, QuarantineTicket, RetryFailureClass, RetryPolicy, TestOutcome,
    build_artifact_manifest, build_bisect_request, build_bisect_result_summary,
    evaluate_bisect_dispatch, evaluate_flake_budget, evaluate_global_flake_budget,
    evaluate_quarantine_ticket, evaluate_retry_decision, should_trigger_bisect,
};

const BEAD_ID: &str = "bd-1dp9.7.3";
const SEED: u64 = 20260213;

fn build_regression_lane_result() -> fsqlite_harness::ci_gate_matrix::FlakeBudgetResult {
    fsqlite_harness::ci_gate_matrix::FlakeBudgetResult {
        lane: CiLane::E2eCorrectness.as_str().to_owned(),
        total_tests: 64,
        pass_count: 63,
        fail_count: 1,
        flake_count: 0,
        skip_count: 0,
        flake_rate: 0.0,
        budget_max_flake_rate: 0.05,
        escalation_warn_flake_rate: 0.03,
        escalation_critical_flake_rate: 0.08,
        escalation_level: fsqlite_harness::ci_gate_matrix::FlakeEscalationLevel::None,
        within_budget: true,
        blocking: true,
        pipeline_fail: true,
    }
}

#[test]
fn e2e_full_pipeline_pass_all_lanes() {
    let policy = FlakeBudgetPolicy::canonical();
    let config = AutoBisectConfig::default_config();

    // Simulate a full CI run where all lanes pass
    let lane_configs: Vec<(CiLane, Vec<TestOutcome>)> = vec![
        (CiLane::Unit, vec![TestOutcome::Pass; 200]),
        (CiLane::E2eDifferential, vec![TestOutcome::Pass; 50]),
        (CiLane::E2eCorrectness, vec![TestOutcome::Pass; 100]),
        (CiLane::E2eRecovery, vec![TestOutcome::Pass; 30]),
        (CiLane::Performance, vec![TestOutcome::Pass; 20]),
        (CiLane::SchemaValidation, vec![TestOutcome::Pass; 10]),
        (CiLane::CoverageDrift, vec![TestOutcome::Pass; 5]),
    ];

    let mut lane_results = Vec::new();
    for (lane, outcomes) in &lane_configs {
        let result = evaluate_flake_budget(*lane, outcomes, &policy);
        assert!(
            !result.pipeline_fail,
            "bead_id={BEAD_ID} case=lane_pass lane={} should not fail",
            lane.as_str(),
        );
        assert_eq!(
            should_trigger_bisect(&result, &config),
            None,
            "bead_id={BEAD_ID} case=no_bisect lane={}",
            lane.as_str(),
        );
        lane_results.push(result);
    }

    let global = evaluate_global_flake_budget(&lane_results, &policy);
    assert!(
        global.pipeline_pass,
        "bead_id={BEAD_ID} case=global_pass pipeline should pass",
    );
    assert_eq!(global.total_lanes, 7);
    assert_eq!(global.total_flakes, 0);

    let summary = global.render_summary();
    assert!(
        summary.contains("PASS"),
        "bead_id={BEAD_ID} case=summary_pass",
    );

    eprintln!(
        "bead_id={BEAD_ID} phase=report event_type=pass \
         run_id={BEAD_ID}-all-lanes-{SEED} seed={SEED} \
         total_lanes={} total_executed={} result=PASS",
        global.total_lanes, global.total_executed,
    );
}

#[test]
fn e2e_full_pipeline_regression_triggers_bisect() {
    let policy = FlakeBudgetPolicy::canonical();
    let config = AutoBisectConfig::default_config();

    // Unit lane has a hard failure
    let mut unit_outcomes = vec![TestOutcome::Pass; 99];
    unit_outcomes.push(TestOutcome::Fail);

    let unit_result = evaluate_flake_budget(CiLane::Unit, &unit_outcomes, &policy);
    assert!(
        unit_result.pipeline_fail,
        "bead_id={BEAD_ID} case=unit_fail should fail pipeline",
    );
    assert_eq!(unit_result.fail_count, 1);

    // Bisect should trigger
    let trigger = should_trigger_bisect(&unit_result, &config);
    assert_eq!(
        trigger,
        Some(BisectTrigger::GateRegression),
        "bead_id={BEAD_ID} case=bisect_trigger",
    );

    // Build bisect request
    let bisect = build_bisect_request(
        trigger.unwrap(),
        CiLane::Unit,
        "test_btree_split_merge",
        "abc1234500000000",
        "def6789000000000",
        SEED,
        "cargo test -p fsqlite-btree -- test_btree_split_merge",
        "B-tree split/merge regression",
    );
    assert!(
        bisect.request_id.contains("unit"),
        "bead_id={BEAD_ID} case=bisect_id",
    );
    assert_eq!(bisect.replay_seed, SEED);

    // Build artifact manifest with bisect
    let manifest = build_artifact_manifest(
        CiLane::Unit,
        &format!("{BEAD_ID}-regression-{SEED}"),
        "def6789000000000",
        SEED,
        false,
        vec![ArtifactEntry {
            kind: ArtifactKind::Log,
            path: "logs/unit-regression.jsonl".to_owned(),
            content_hash: "a".repeat(64),
            size_bytes: 4096,
            description: "Unit regression event log".to_owned(),
        }],
        Some(bisect),
    );

    assert!(!manifest.gate_passed);
    assert!(manifest.bisect_request.is_some());
    let errors = manifest.validate();
    assert!(
        errors.is_empty(),
        "bead_id={BEAD_ID} case=manifest_valid errors={errors:?}",
    );

    let summary = manifest.render_summary();
    assert!(
        summary.contains("FAIL"),
        "bead_id={BEAD_ID} case=manifest_fail",
    );
    assert!(
        summary.contains("Bisect requested"),
        "bead_id={BEAD_ID} case=manifest_bisect",
    );

    eprintln!(
        "bead_id={BEAD_ID} phase=report event_type=pass \
         run_id={BEAD_ID}-regression-{SEED} seed={SEED} \
         trigger=GateRegression result=PASS",
    );
}

#[test]
fn e2e_auto_bisect_operator_success_path() {
    let config = AutoBisectConfig::default_config();
    let lane_result = build_regression_lane_result();
    let dispatch = evaluate_bisect_dispatch(
        &lane_result,
        &config,
        BisectDispatchContext {
            active_runs: 1,
            active_for_lane: 0,
            pending_runs: 1,
        },
    );
    assert_eq!(dispatch.status, BisectDispatchStatus::Enqueued);
    assert_eq!(dispatch.trigger, Some(BisectTrigger::GateRegression));

    let request = build_bisect_request(
        BisectTrigger::GateRegression,
        CiLane::E2eCorrectness,
        "scenario_success",
        "good001",
        "bad001",
        SEED,
        "cargo test -p fsqlite-harness -- scenario_success",
        "synthetic success path",
    );
    let summary = build_bisect_result_summary(
        &request,
        BisectExecutionOutcome::Success,
        "good001",
        "bad001",
        0.91,
        vec![
            "artifacts/bisect/success/report.json".to_owned(),
            "artifacts/bisect/success/replay.jsonl".to_owned(),
        ],
        BisectRunTelemetry {
            trace_id: "trace-success".to_owned(),
            run_id: "run-success".to_owned(),
            scenario_id: "scenario_success".to_owned(),
            queue_wait_ms: 11,
            execution_ms: 1380,
            step_count: 5,
        },
        vec!["first bad commit isolated by deterministic midpoint search".to_owned()],
    );
    assert!(summary.validate().is_empty());
    let manifest = build_artifact_manifest(
        CiLane::E2eCorrectness,
        &format!("{BEAD_ID}-success-{SEED}"),
        "bad001",
        SEED,
        false,
        vec![ArtifactEntry {
            kind: ArtifactKind::Report,
            path: "artifacts/bisect/success/report.json".to_owned(),
            content_hash: "e".repeat(64),
            size_bytes: 2048,
            description: "Bisect success report".to_owned(),
        }],
        Some(request),
    )
    .with_bisect_result_summary(summary.clone());
    let errors = manifest.validate();
    assert!(
        errors.is_empty(),
        "success manifest should validate: {errors:?}"
    );
    let rendered = manifest.render_summary();
    assert!(rendered.contains("Bisect result"));
    assert!(rendered.contains("confidence=0.91"));
    assert!(rendered.contains("trace-success"));
    eprintln!(
        "bead_id={BEAD_ID} phase=report event_type=pass \
         trace_id={} run_id={} scenario_id={} outcome=success execution_ms={} \
         replay_artifacts={} result=PASS",
        summary.telemetry.trace_id,
        summary.telemetry.run_id,
        summary.telemetry.scenario_id,
        summary.telemetry.execution_ms,
        summary.replay_artifacts.join(","),
    );
}

#[test]
fn e2e_auto_bisect_operator_uncertain_path() {
    let request = build_bisect_request(
        BisectTrigger::GateRegression,
        CiLane::E2eCorrectness,
        "scenario_uncertain",
        "good010",
        "bad010",
        SEED,
        "cargo test -p fsqlite-harness -- scenario_uncertain",
        "synthetic uncertain path",
    );
    let summary = build_bisect_result_summary(
        &request,
        BisectExecutionOutcome::Uncertain,
        "good010",
        "bad010",
        0.42,
        vec!["artifacts/bisect/uncertain/replay.jsonl".to_owned()],
        BisectRunTelemetry {
            trace_id: "trace-uncertain".to_owned(),
            run_id: "run-uncertain".to_owned(),
            scenario_id: "scenario_uncertain".to_owned(),
            queue_wait_ms: 27,
            execution_ms: 2001,
            step_count: 3,
        },
        vec!["flaky_conflict: both PASS and FAIL observed across retries".to_owned()],
    );
    assert!(summary.validate().is_empty());
    let rendered = summary.render_summary();
    assert!(rendered.contains("Uncertain"));
    assert!(rendered.contains("confidence=0.42"));
}

#[test]
fn e2e_auto_bisect_operator_cancellation_path() {
    let request = build_bisect_request(
        BisectTrigger::GateRegression,
        CiLane::E2eRecovery,
        "scenario_cancel",
        "good020",
        "bad020",
        SEED,
        "cargo test -p fsqlite-harness -- scenario_cancel",
        "synthetic cancellation path",
    );
    let summary = build_bisect_result_summary(
        &request,
        BisectExecutionOutcome::Cancelled,
        "good020",
        "bad020",
        0.10,
        vec!["artifacts/bisect/cancel/request.json".to_owned()],
        BisectRunTelemetry {
            trace_id: "trace-cancel".to_owned(),
            run_id: "run-cancel".to_owned(),
            scenario_id: "scenario_cancel".to_owned(),
            queue_wait_ms: 5,
            execution_ms: 120,
            step_count: 1,
        },
        vec!["cancelled by operator due to release freeze".to_owned()],
    );
    assert!(summary.validate().is_empty());
    assert!(summary.render_summary().contains("Cancelled"));
}

#[test]
fn e2e_auto_bisect_operator_timeout_path() {
    let request = build_bisect_request(
        BisectTrigger::GateRegression,
        CiLane::E2eDifferential,
        "scenario_timeout",
        "good030",
        "bad030",
        SEED,
        "cargo test -p fsqlite-harness -- scenario_timeout",
        "synthetic timeout path",
    );
    let summary = build_bisect_result_summary(
        &request,
        BisectExecutionOutcome::Timeout,
        "good030",
        "bad030",
        0.25,
        vec!["artifacts/bisect/timeout/replay.jsonl".to_owned()],
        BisectRunTelemetry {
            trace_id: "trace-timeout".to_owned(),
            run_id: "run-timeout".to_owned(),
            scenario_id: "scenario_timeout".to_owned(),
            queue_wait_ms: 9,
            execution_ms: 30_000,
            step_count: 2,
        },
        vec!["timeout while evaluating midpoint candidate bad030".to_owned()],
    );
    assert!(summary.validate().is_empty());
    assert!(summary.render_summary().contains("Timeout"));
}

#[test]
fn e2e_flake_budget_mixed_lanes() {
    let policy = FlakeBudgetPolicy::canonical();

    // Lane 1: Unit — 2% flake rate (within 5% budget)
    let mut unit_outcomes = vec![TestOutcome::Pass; 98];
    unit_outcomes.extend(vec![TestOutcome::Flake; 2]);
    let unit_result = evaluate_flake_budget(CiLane::Unit, &unit_outcomes, &policy);
    assert!(
        unit_result.within_budget,
        "bead_id={BEAD_ID} case=unit_within_budget",
    );

    // Lane 2: Performance — 15% flake rate (over 10% relaxed budget, but advisory)
    let mut perf_outcomes = vec![TestOutcome::Pass; 85];
    perf_outcomes.extend(vec![TestOutcome::Flake; 15]);
    let perf_result = evaluate_flake_budget(CiLane::Performance, &perf_outcomes, &policy);
    assert!(
        !perf_result.within_budget,
        "bead_id={BEAD_ID} case=perf_over_budget",
    );
    assert!(
        !perf_result.pipeline_fail,
        "bead_id={BEAD_ID} case=perf_advisory_no_fail",
    );

    // Lane 3: E2eCorrectness — all pass
    let correctness_result =
        evaluate_flake_budget(CiLane::E2eCorrectness, &[TestOutcome::Pass; 50], &policy);

    let global =
        evaluate_global_flake_budget(&[unit_result, perf_result, correctness_result], &policy);

    // Global: 17 flakes out of 250 = 6.8% > 5% global budget
    assert!(
        !global.within_budget,
        "bead_id={BEAD_ID} case=global_over_budget",
    );
    // But no blocking lane failed, so pipeline still fails due to global over-budget
    assert!(
        !global.pipeline_pass,
        "bead_id={BEAD_ID} case=global_pipeline_fail",
    );

    let summary = global.render_summary();
    assert!(
        summary.contains("OVER BUDGET"),
        "bead_id={BEAD_ID} case=summary_over_budget",
    );

    eprintln!(
        "bead_id={BEAD_ID} phase=report event_type=pass \
         run_id={BEAD_ID}-mixed-flake-{SEED} seed={SEED} \
         total_flakes={} global_rate={:.1}% result=PASS",
        global.total_flakes,
        global.global_flake_rate * 100.0,
    );
}

#[test]
fn e2e_retry_and_quarantine_workflow() {
    let retry_policy = RetryPolicy::canonical();
    let flake_policy = FlakeBudgetPolicy::canonical();
    let quarantine_policy = QuarantinePolicy::canonical();

    // Correctness failures must not be maskable as flakes.
    let correctness_decision = evaluate_retry_decision(
        CiLane::Unit,
        RetryFailureClass::CorrectnessRegression,
        0,
        false,
        &retry_policy,
    );
    assert!(!correctness_decision.allow_retry);
    assert!(!correctness_decision.classify_as_flake);
    assert!(correctness_decision.hard_failure);

    // Transient infra recovered on retry is counted as flake.
    let transient_decision = evaluate_retry_decision(
        CiLane::Unit,
        RetryFailureClass::InfrastructureTransient,
        1,
        true,
        &retry_policy,
    );
    assert!(transient_decision.classify_as_flake);
    assert!(!transient_decision.hard_failure);

    // Flake-only over-budget lane can be quarantined with explicit owner + expiry + follow-up.
    let mut outcomes = vec![TestOutcome::Pass; 94];
    outcomes.extend(vec![TestOutcome::Flake; 6]);
    let lane_result = evaluate_flake_budget(CiLane::Unit, &outcomes, &flake_policy);
    assert!(lane_result.pipeline_fail);

    let ticket = QuarantineTicket {
        lane: "unit".to_owned(),
        gate_id: "unit-gate".to_owned(),
        owner: "MaroonCanyon".to_owned(),
        follow_up_issue: "bd-mblr.3.3".to_owned(),
        reason: "temporary CI runner packet loss".to_owned(),
        expires_after_runs: 2,
    };
    let quarantine_decision = evaluate_quarantine_ticket(&lane_result, &ticket, &quarantine_policy);
    assert!(quarantine_decision.approved);
    assert!(!quarantine_decision.effective_pipeline_fail);

    eprintln!(
        "bead_id={BEAD_ID} run_id={BEAD_ID}-retry-quarantine-{SEED} seed={SEED} \
         scenario_id=CI-FLAKE-QUARANTINE phase=validate event_type=pass \
         retry_correctness_hard_failure={} transient_classified_as_flake={} quarantine_approved={}",
        correctness_decision.hard_failure,
        transient_decision.classify_as_flake,
        quarantine_decision.approved,
    );
}

#[test]
fn e2e_artifact_manifest_multi_artifact() {
    let artifacts = vec![
        ArtifactEntry {
            kind: ArtifactKind::Log,
            path: "logs/events.jsonl".to_owned(),
            content_hash: "a".repeat(64),
            size_bytes: 8192,
            description: "Structured event log".to_owned(),
        },
        ArtifactEntry {
            kind: ArtifactKind::Report,
            path: "reports/gate-report.json".to_owned(),
            content_hash: "b".repeat(64),
            size_bytes: 2048,
            description: "Gate validation report".to_owned(),
        },
        ArtifactEntry {
            kind: ArtifactKind::Trace,
            path: "traces/replay.bin".to_owned(),
            content_hash: "c".repeat(64),
            size_bytes: 65536,
            description: "Execution trace for replay".to_owned(),
        },
        ArtifactEntry {
            kind: ArtifactKind::Benchmark,
            path: "benchmarks/perf.json".to_owned(),
            content_hash: "d".repeat(64),
            size_bytes: 1024,
            description: "Performance benchmark data".to_owned(),
        },
    ];

    let manifest = build_artifact_manifest(
        CiLane::E2eDifferential,
        &format!("{BEAD_ID}-multi-{SEED}"),
        "abc1234567890000",
        SEED,
        true,
        artifacts,
        None,
    );

    assert!(manifest.gate_passed);
    assert_eq!(manifest.artifacts.len(), 4);
    let errors = manifest.validate();
    assert!(
        errors.is_empty(),
        "bead_id={BEAD_ID} case=multi_artifact_valid errors={errors:?}",
    );

    // Verify JSON roundtrip
    let json = serde_json::to_string_pretty(&manifest).expect("serialize manifest");
    let deserialized: fsqlite_harness::ci_gate_matrix::ArtifactManifest =
        serde_json::from_str(&json).expect("deserialize manifest");
    assert_eq!(deserialized.artifacts.len(), 4);
    assert_eq!(deserialized.gate_passed, true);
    assert_eq!(deserialized.seed, SEED);

    let summary = manifest.render_summary();
    assert!(
        summary.contains("PASS"),
        "bead_id={BEAD_ID} case=multi_summary",
    );
    assert!(
        summary.contains("Artifacts: 4"),
        "bead_id={BEAD_ID} case=multi_artifact_count",
    );

    eprintln!(
        "bead_id={BEAD_ID} phase=report event_type=pass \
         run_id={BEAD_ID}-multi-{SEED} seed={SEED} \
         artifacts={} result=PASS",
        manifest.artifacts.len(),
    );
}

#[test]
fn e2e_pipeline_determinism() {
    let policy = FlakeBudgetPolicy::canonical();

    let outcomes = vec![TestOutcome::Pass; 100];

    // Run the same evaluation twice
    let r1 = evaluate_flake_budget(CiLane::Unit, &outcomes, &policy);
    let r2 = evaluate_flake_budget(CiLane::Unit, &outcomes, &policy);

    let json1 = serde_json::to_string(&r1).expect("serialize r1");
    let json2 = serde_json::to_string(&r2).expect("serialize r2");
    assert_eq!(
        json1, json2,
        "bead_id={BEAD_ID} case=determinism flake budget must be deterministic",
    );

    // Same for global evaluation
    let g1 = evaluate_global_flake_budget(&[r1], &policy);
    let g2 = evaluate_global_flake_budget(&[r2.clone()], &policy);
    let gj1 = serde_json::to_string(&g1).expect("serialize g1");
    let gj2 = serde_json::to_string(&g2).expect("serialize g2");
    assert_eq!(
        gj1, gj2,
        "bead_id={BEAD_ID} case=global_determinism must be deterministic",
    );

    // Same for artifact manifest
    let m1 = build_artifact_manifest(
        CiLane::Unit,
        &format!("{BEAD_ID}-det-{SEED}"),
        "abc",
        SEED,
        true,
        Vec::new(),
        None,
    );
    let m2 = build_artifact_manifest(
        CiLane::Unit,
        &format!("{BEAD_ID}-det-{SEED}"),
        "abc",
        SEED,
        true,
        Vec::new(),
        None,
    );
    let mj1 = serde_json::to_string(&m1).expect("serialize m1");
    let mj2 = serde_json::to_string(&m2).expect("serialize m2");
    assert_eq!(
        mj1, mj2,
        "bead_id={BEAD_ID} case=manifest_determinism must be deterministic",
    );

    eprintln!(
        "bead_id={BEAD_ID} phase=report event_type=pass \
         run_id={BEAD_ID}-determinism-{SEED} seed={SEED} \
         result=PASS",
    );
}
