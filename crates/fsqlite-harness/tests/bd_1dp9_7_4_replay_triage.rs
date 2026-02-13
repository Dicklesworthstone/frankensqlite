//! Integration tests for bd-1dp9.7.4 — Failure Replay/Minimization Harness.
//!
//! Tests the replay-triage orchestrator that ties together the triage session,
//! divergence extraction, replay config, and reproducibility checklist.

use std::collections::BTreeMap;

use fsqlite_harness::ci_gate_matrix::{
    ArtifactEntry, ArtifactKind, BisectTrigger, CiLane, build_artifact_manifest,
    build_bisect_request,
};
use fsqlite_harness::e2e_log_schema::{LogEventSchema, LogEventType, LogPhase};
use fsqlite_harness::log_schema_validator::encode_jsonl_stream;
use fsqlite_harness::replay_triage::{
    REPLAY_TRIAGE_BEAD_ID, ReplayTriageConfig, ReplayTriageReport, ReplayTriageVerdict,
    load_replay_triage_report, run_replay_triage_workflow, write_replay_triage_report,
};

const BEAD_ID: &str = "bd-1dp9.7.4";
const SEED: u64 = 20260213;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn build_failing_manifest() -> fsqlite_harness::ci_gate_matrix::ArtifactManifest {
    let bisect = build_bisect_request(
        BisectTrigger::GateRegression,
        CiLane::E2eDifferential,
        "test_mvcc_isolation",
        "abc1234500000000",
        "def6789000000000",
        SEED,
        "cargo test -p fsqlite-harness -- test_mvcc_isolation",
        "MVCC isolation regression",
    );

    build_artifact_manifest(
        CiLane::E2eDifferential,
        &format!("{BEAD_ID}-{SEED}"),
        "def6789000000000",
        SEED,
        false,
        vec![ArtifactEntry {
            kind: ArtifactKind::Log,
            path: "logs/events.jsonl".to_owned(),
            content_hash: "a".repeat(64),
            size_bytes: 4096,
            description: "Event log".to_owned(),
        }],
        Some(bisect),
    )
}

fn build_clean_manifest() -> fsqlite_harness::ci_gate_matrix::ArtifactManifest {
    build_artifact_manifest(
        CiLane::E2eDifferential,
        &format!("{BEAD_ID}-clean-{SEED}"),
        "def6789000000000",
        SEED,
        true,
        vec![ArtifactEntry {
            kind: ArtifactKind::Log,
            path: "logs/events.jsonl".to_owned(),
            content_hash: "b".repeat(64),
            size_bytes: 2048,
            description: "Event log".to_owned(),
        }],
        None,
    )
}

fn build_failing_jsonl() -> String {
    let events = vec![
        LogEventSchema {
            run_id: format!("{BEAD_ID}-{SEED}"),
            timestamp: "2026-02-13T09:00:00.000Z".to_owned(),
            phase: LogPhase::Setup,
            event_type: LogEventType::Start,
            scenario_id: Some("MVCC-3".to_owned()),
            seed: Some(SEED),
            backend: Some("both".to_owned()),
            artifact_hash: None,
            context: BTreeMap::new(),
        },
        LogEventSchema {
            run_id: format!("{BEAD_ID}-{SEED}"),
            timestamp: "2026-02-13T09:00:01.000Z".to_owned(),
            phase: LogPhase::Execute,
            event_type: LogEventType::Info,
            scenario_id: Some("MVCC-3".to_owned()),
            seed: Some(SEED),
            backend: Some("fsqlite".to_owned()),
            artifact_hash: None,
            context: BTreeMap::new(),
        },
        LogEventSchema {
            run_id: format!("{BEAD_ID}-{SEED}"),
            timestamp: "2026-02-13T09:00:02.000Z".to_owned(),
            phase: LogPhase::Validate,
            event_type: LogEventType::FirstDivergence,
            scenario_id: Some("MVCC-3".to_owned()),
            seed: Some(SEED),
            backend: Some("both".to_owned()),
            artifact_hash: None,
            context: {
                let mut ctx = BTreeMap::new();
                ctx.insert("divergence_point".to_owned(), "row 42 column 3".to_owned());
                ctx.insert("artifact_paths".to_owned(), "divergence.json".to_owned());
                ctx
            },
        },
        LogEventSchema {
            run_id: format!("{BEAD_ID}-{SEED}"),
            timestamp: "2026-02-13T09:00:03.000Z".to_owned(),
            phase: LogPhase::Validate,
            event_type: LogEventType::Fail,
            scenario_id: Some("MVCC-3".to_owned()),
            seed: Some(SEED),
            backend: Some("both".to_owned()),
            artifact_hash: None,
            context: BTreeMap::new(),
        },
        LogEventSchema {
            run_id: format!("{BEAD_ID}-{SEED}"),
            timestamp: "2026-02-13T09:00:04.000Z".to_owned(),
            phase: LogPhase::Teardown,
            event_type: LogEventType::Info,
            scenario_id: Some("MVCC-3".to_owned()),
            seed: Some(SEED),
            backend: None,
            artifact_hash: None,
            context: BTreeMap::new(),
        },
    ];

    encode_jsonl_stream(&events).unwrap()
}

fn build_clean_jsonl() -> String {
    let events = vec![
        LogEventSchema {
            run_id: format!("{BEAD_ID}-clean-{SEED}"),
            timestamp: "2026-02-13T09:00:00.000Z".to_owned(),
            phase: LogPhase::Setup,
            event_type: LogEventType::Start,
            scenario_id: Some("INFRA-1".to_owned()),
            seed: Some(SEED),
            backend: Some("both".to_owned()),
            artifact_hash: None,
            context: BTreeMap::new(),
        },
        LogEventSchema {
            run_id: format!("{BEAD_ID}-clean-{SEED}"),
            timestamp: "2026-02-13T09:00:01.000Z".to_owned(),
            phase: LogPhase::Validate,
            event_type: LogEventType::Pass,
            scenario_id: Some("INFRA-1".to_owned()),
            seed: Some(SEED),
            backend: Some("fsqlite".to_owned()),
            artifact_hash: Some("b".repeat(64)),
            context: BTreeMap::new(),
        },
    ];

    encode_jsonl_stream(&events).unwrap()
}

// ---------------------------------------------------------------------------
// Full workflow — failing run
// ---------------------------------------------------------------------------

#[test]
fn workflow_with_failures_produces_report() {
    let manifest = build_failing_manifest();
    let jsonl = build_failing_jsonl();
    let config = ReplayTriageConfig::default();
    let report = run_replay_triage_workflow(&manifest, &jsonl, &config);

    assert_eq!(
        report.bead_id, REPLAY_TRIAGE_BEAD_ID,
        "bead_id={BEAD_ID} case=bead_id"
    );
    assert_eq!(
        report.schema_version, 1,
        "bead_id={BEAD_ID} case=schema_version"
    );
}

#[test]
fn workflow_failing_run_verdict_fail() {
    let manifest = build_failing_manifest();
    let jsonl = build_failing_jsonl();
    let config = ReplayTriageConfig::default();
    let report = run_replay_triage_workflow(&manifest, &jsonl, &config);

    // Failing run with full bisect context (5/5 repro) → Fail
    assert_eq!(
        report.verdict,
        ReplayTriageVerdict::Fail,
        "bead_id={BEAD_ID} case=failing_verdict"
    );
}

#[test]
fn workflow_failing_run_has_divergences() {
    let manifest = build_failing_manifest();
    let jsonl = build_failing_jsonl();
    let config = ReplayTriageConfig::default();
    let report = run_replay_triage_workflow(&manifest, &jsonl, &config);

    assert_eq!(
        report.session.divergences.len(),
        1,
        "bead_id={BEAD_ID} case=divergence_count"
    );
    assert_eq!(
        report.session.failure_indices.len(),
        1,
        "bead_id={BEAD_ID} case=failure_count"
    );
}

#[test]
fn workflow_failing_run_has_triage_text() {
    let manifest = build_failing_manifest();
    let jsonl = build_failing_jsonl();
    let config = ReplayTriageConfig::default();
    let report = run_replay_triage_workflow(&manifest, &jsonl, &config);

    assert!(
        report.triage_report_text.contains("Failure Triage Report"),
        "bead_id={BEAD_ID} case=triage_header"
    );
    assert!(
        report.triage_report_text.contains("row 42 column 3"),
        "bead_id={BEAD_ID} case=divergence_point"
    );
}

#[test]
fn workflow_failing_run_has_repro_checklist() {
    let manifest = build_failing_manifest();
    let jsonl = build_failing_jsonl();
    let config = ReplayTriageConfig::default();
    let report = run_replay_triage_workflow(&manifest, &jsonl, &config);

    assert!(
        report.reproducibility_text.is_some(),
        "bead_id={BEAD_ID} case=repro_present"
    );
    assert!(
        report.reproducibility_score >= 4,
        "bead_id={BEAD_ID} case=repro_score score={}",
        report.reproducibility_score,
    );
}

#[test]
fn workflow_failing_run_has_divergence_contexts() {
    let manifest = build_failing_manifest();
    let jsonl = build_failing_jsonl();
    let config = ReplayTriageConfig::default();
    let report = run_replay_triage_workflow(&manifest, &jsonl, &config);

    assert_eq!(
        report.divergence_contexts.len(),
        1,
        "bead_id={BEAD_ID} case=context_count"
    );
    assert!(
        report.divergence_contexts[0].contains(">>>"),
        "bead_id={BEAD_ID} case=context_marker"
    );
}

// ---------------------------------------------------------------------------
// Full workflow — clean run
// ---------------------------------------------------------------------------

#[test]
fn workflow_clean_run_passes() {
    let manifest = build_clean_manifest();
    let jsonl = build_clean_jsonl();
    let config = ReplayTriageConfig::default();
    let report = run_replay_triage_workflow(&manifest, &jsonl, &config);

    assert_eq!(
        report.verdict,
        ReplayTriageVerdict::Pass,
        "bead_id={BEAD_ID} case=clean_pass"
    );
}

#[test]
fn workflow_clean_run_no_divergences() {
    let manifest = build_clean_manifest();
    let jsonl = build_clean_jsonl();
    let config = ReplayTriageConfig::default();
    let report = run_replay_triage_workflow(&manifest, &jsonl, &config);

    assert!(
        report.session.divergences.is_empty(),
        "bead_id={BEAD_ID} case=no_divergences"
    );
    assert!(
        report.session.failure_indices.is_empty(),
        "bead_id={BEAD_ID} case=no_failures"
    );
    assert!(
        report.divergence_contexts.is_empty(),
        "bead_id={BEAD_ID} case=no_contexts"
    );
}

#[test]
fn workflow_clean_run_no_repro() {
    let manifest = build_clean_manifest();
    let jsonl = build_clean_jsonl();
    let config = ReplayTriageConfig::default();
    let report = run_replay_triage_workflow(&manifest, &jsonl, &config);

    assert!(
        report.reproducibility_text.is_none(),
        "bead_id={BEAD_ID} case=no_repro"
    );
    assert_eq!(
        report.reproducibility_score, 0,
        "bead_id={BEAD_ID} case=zero_repro_score"
    );
}

// ---------------------------------------------------------------------------
// Verdict logic
// ---------------------------------------------------------------------------

#[test]
fn verdict_display() {
    assert_eq!(ReplayTriageVerdict::Pass.to_string(), "PASS");
    assert_eq!(ReplayTriageVerdict::Warning.to_string(), "WARNING");
    assert_eq!(ReplayTriageVerdict::Fail.to_string(), "FAIL");
}

#[test]
fn verdict_warning_on_low_reproducibility() {
    let manifest = build_failing_manifest();
    let jsonl = build_failing_jsonl();
    let config = ReplayTriageConfig {
        context_window: 3,
        min_reproducibility: 6, // impossible threshold → forces Warning
    };
    let report = run_replay_triage_workflow(&manifest, &jsonl, &config);

    assert_eq!(
        report.verdict,
        ReplayTriageVerdict::Warning,
        "bead_id={BEAD_ID} case=warning_verdict"
    );
}

// ---------------------------------------------------------------------------
// Report serialization
// ---------------------------------------------------------------------------

#[test]
fn report_json_roundtrip() {
    let manifest = build_failing_manifest();
    let jsonl = build_failing_jsonl();
    let config = ReplayTriageConfig::default();
    let report = run_replay_triage_workflow(&manifest, &jsonl, &config);

    let json = report.to_json().expect("serialize");
    let parsed = ReplayTriageReport::from_json(&json).expect("parse");

    assert_eq!(parsed.bead_id, report.bead_id);
    assert_eq!(parsed.verdict, report.verdict);
    assert_eq!(parsed.reproducibility_score, report.reproducibility_score);
    assert_eq!(
        parsed.session.divergences.len(),
        report.session.divergences.len()
    );
}

#[test]
fn report_file_roundtrip() {
    let manifest = build_failing_manifest();
    let jsonl = build_failing_jsonl();
    let config = ReplayTriageConfig::default();
    let report = run_replay_triage_workflow(&manifest, &jsonl, &config);

    let dir = std::env::temp_dir().join("fsqlite-replay-triage-test");
    std::fs::create_dir_all(&dir).expect("create temp dir");
    let path = dir.join("replay-triage-test.json");

    write_replay_triage_report(&path, &report).expect("write");
    let loaded = load_replay_triage_report(&path).expect("load");

    assert_eq!(loaded.verdict, report.verdict);
    assert_eq!(loaded.reproducibility_score, report.reproducibility_score);
    assert_eq!(loaded.bead_id, report.bead_id);

    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_dir(&dir);
}

// ---------------------------------------------------------------------------
// Triage line & summary
// ---------------------------------------------------------------------------

#[test]
fn triage_line_contains_key_info() {
    let manifest = build_failing_manifest();
    let jsonl = build_failing_jsonl();
    let config = ReplayTriageConfig::default();
    let report = run_replay_triage_workflow(&manifest, &jsonl, &config);
    let line = report.triage_line();

    assert!(
        line.contains("divergences="),
        "bead_id={BEAD_ID} case=triage_divergences"
    );
    assert!(
        line.contains("failures="),
        "bead_id={BEAD_ID} case=triage_failures"
    );
    assert!(
        line.contains("repro="),
        "bead_id={BEAD_ID} case=triage_repro"
    );
    assert!(
        line.contains("events="),
        "bead_id={BEAD_ID} case=triage_events"
    );
}

#[test]
fn summary_is_nonempty() {
    let manifest = build_failing_manifest();
    let jsonl = build_failing_jsonl();
    let config = ReplayTriageConfig::default();
    let report = run_replay_triage_workflow(&manifest, &jsonl, &config);

    assert!(
        !report.summary.is_empty(),
        "bead_id={BEAD_ID} case=summary_nonempty"
    );
    assert!(
        report.summary.contains("divergence"),
        "bead_id={BEAD_ID} case=summary_content"
    );
}

// ---------------------------------------------------------------------------
// Determinism
// ---------------------------------------------------------------------------

#[test]
fn workflow_is_deterministic() {
    let manifest = build_failing_manifest();
    let jsonl = build_failing_jsonl();
    let config = ReplayTriageConfig::default();

    let r1 = run_replay_triage_workflow(&manifest, &jsonl, &config);
    let r2 = run_replay_triage_workflow(&manifest, &jsonl, &config);

    assert_eq!(
        r1.verdict, r2.verdict,
        "bead_id={BEAD_ID} case=deterministic_verdict"
    );
    assert_eq!(
        r1.reproducibility_score, r2.reproducibility_score,
        "bead_id={BEAD_ID} case=deterministic_score"
    );
    assert_eq!(
        r1.triage_report_text, r2.triage_report_text,
        "bead_id={BEAD_ID} case=deterministic_report"
    );
    assert_eq!(
        r1.to_json().unwrap(),
        r2.to_json().unwrap(),
        "bead_id={BEAD_ID} case=deterministic_json"
    );
}

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

#[test]
fn config_default_is_reasonable() {
    let config = ReplayTriageConfig::default();
    assert!(
        config.context_window > 0,
        "bead_id={BEAD_ID} case=context_window"
    );
    assert!(
        config.min_reproducibility <= 5,
        "bead_id={BEAD_ID} case=min_repro"
    );
}
