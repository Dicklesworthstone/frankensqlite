//! Integration tests for bd-mblr.7.5 — Failure Forensics Navigator.
//!
//! Tests the forensics workflow orchestrator that ties together the evidence
//! index (bd-mblr.7.5.1) and forensics queries (bd-mblr.7.5.2).

use fsqlite_harness::evidence_index::{
    ArtifactKind, ArtifactRecord, EvidenceIndex, InvariantCheck, InvariantVerdict, LogReference,
    RunId, RunRecord, ScenarioOutcome, ScenarioVerdict,
};
use fsqlite_harness::forensics_navigator::{
    FORENSICS_BEAD_ID, ForensicsVerdict, ForensicsWorkflowConfig, ForensicsWorkflowReport,
    QueryFilters, Severity, load_forensics_report, run_forensics_workflow, write_forensics_report,
};

const BEAD_ID: &str = "bd-mblr.7.5";

fn make_run(id: &str, success: bool, git_sha: &str) -> RunRecord {
    RunRecord {
        schema_version: 1,
        run_id: RunId(id.to_owned()),
        started_at: "2026-02-13T10:00:00Z".to_owned(),
        completed_at: Some("2026-02-13T10:01:00Z".to_owned()),
        seed: 42,
        profile: "test".to_owned(),
        git_sha: git_sha.to_owned(),
        toolchain: "nightly".to_owned(),
        platform: "linux-x86_64".to_owned(),
        success,
        scenarios: vec![ScenarioOutcome {
            scenario_id: "SCN-001".to_owned(),
            scenario_name: "basic_test".to_owned(),
            verdict: if success {
                ScenarioVerdict::Pass
            } else {
                ScenarioVerdict::Fail
            },
            duration_ms: 100,
            first_divergence: None,
            error_message: if success {
                None
            } else {
                Some("test failure".to_owned())
            },
            code_areas: vec!["parser".to_owned()],
        }],
        invariants: vec![InvariantCheck {
            invariant_id: "INV-1".to_owned(),
            invariant_name: "monotone_counters".to_owned(),
            verdict: InvariantVerdict::Held,
            violation_detail: None,
            violation_timestamp: None,
        }],
        artifacts: vec![ArtifactRecord {
            kind: ArtifactKind::Log,
            path: format!("logs/{id}.jsonl"),
            content_hash: "abc123".to_owned(),
            size_bytes: 1024,
            generated_at: "2026-02-13T10:01:00Z".to_owned(),
            description: Some("test log".to_owned()),
        }],
        logs: vec![LogReference {
            path: format!("logs/{id}.jsonl"),
            schema_version: "1".to_owned(),
            event_count: 10,
            phases: vec!["setup".to_owned(), "run".to_owned()],
            has_divergence_markers: false,
        }],
        bead_ids: vec!["bd-mblr.7.5".to_owned()],
        feature_flags: Vec::new(),
        fault_profile: None,
        metadata: std::collections::BTreeMap::new(),
    }
}

fn make_critical_run(id: &str) -> RunRecord {
    let mut run = make_run(id, false, "sha-crit");
    run.invariants[0].verdict = InvariantVerdict::Violated;
    run.invariants[0].violation_detail = Some("counter regression".to_owned());
    run
}

fn build_test_index() -> EvidenceIndex {
    let mut index = EvidenceIndex::new();
    index.insert(make_run("run-001", true, "sha-aaa"));
    index.insert(make_run("run-002", true, "sha-bbb"));
    index.insert(make_run("run-003", false, "sha-ccc"));
    index
}

// ---------------------------------------------------------------------------
// Workflow basics
// ---------------------------------------------------------------------------

#[test]
fn forensics_workflow_runs_on_empty_index() {
    let index = EvidenceIndex::new();
    let config = ForensicsWorkflowConfig {
        min_runs: 0,
        ..Default::default()
    };
    let report = run_forensics_workflow(&index, &config);
    assert_eq!(report.index_run_count, 0);
    assert_eq!(report.bead_id, FORENSICS_BEAD_ID);
}

#[test]
fn forensics_workflow_passes_with_clean_runs() {
    let mut index = EvidenceIndex::new();
    index.insert(make_run("run-001", true, "sha-aaa"));
    index.insert(make_run("run-002", true, "sha-bbb"));

    let config = ForensicsWorkflowConfig::default();
    let report = run_forensics_workflow(&index, &config);

    assert_eq!(
        report.verdict,
        ForensicsVerdict::Pass,
        "bead_id={BEAD_ID} case=clean_runs_pass"
    );
    assert_eq!(report.critical_event_count, 0);
}

#[test]
fn forensics_workflow_warns_on_high_events() {
    let index = build_test_index(); // has one failed run
    let config = ForensicsWorkflowConfig::default();
    let report = run_forensics_workflow(&index, &config);

    assert_eq!(
        report.verdict,
        ForensicsVerdict::Warning,
        "bead_id={BEAD_ID} case=high_events_warn"
    );
    assert!(report.high_event_count > 0);
}

#[test]
fn forensics_workflow_fails_on_critical_events() {
    let mut index = build_test_index();
    index.insert(make_critical_run("run-crit"));

    let config = ForensicsWorkflowConfig::default();
    let report = run_forensics_workflow(&index, &config);

    assert_eq!(
        report.verdict,
        ForensicsVerdict::Fail,
        "bead_id={BEAD_ID} case=critical_events_fail"
    );
    assert!(report.critical_event_count > 0);
}

#[test]
fn forensics_workflow_fails_on_insufficient_runs() {
    let index = EvidenceIndex::new();
    let config = ForensicsWorkflowConfig {
        min_runs: 5,
        ..Default::default()
    };
    let report = run_forensics_workflow(&index, &config);

    assert_eq!(
        report.verdict,
        ForensicsVerdict::Fail,
        "bead_id={BEAD_ID} case=insufficient_runs"
    );
}

// ---------------------------------------------------------------------------
// Query integration
// ---------------------------------------------------------------------------

#[test]
fn forensics_workflow_applies_filters() {
    let index = build_test_index();
    let config = ForensicsWorkflowConfig {
        filters: QueryFilters {
            commit: Some("sha-aaa".to_owned()),
            ..Default::default()
        },
        ..Default::default()
    };
    let report = run_forensics_workflow(&index, &config);

    assert_eq!(
        report.query_result.matched_run_count, 1,
        "bead_id={BEAD_ID} case=filtered_by_commit"
    );
}

#[test]
fn forensics_workflow_counts_correlations() {
    let index = build_test_index();
    let config = ForensicsWorkflowConfig::default();
    let report = run_forensics_workflow(&index, &config);

    assert!(
        report.correlation_count > 0,
        "bead_id={BEAD_ID} case=has_correlations"
    );
}

#[test]
fn forensics_workflow_has_text_report() {
    let index = build_test_index();
    let config = ForensicsWorkflowConfig::default();
    let report = run_forensics_workflow(&index, &config);

    assert!(
        !report.text_report.is_empty(),
        "bead_id={BEAD_ID} case=text_report_nonempty"
    );
    assert!(report.text_report.contains("forensics report"));
}

// ---------------------------------------------------------------------------
// Report serialization
// ---------------------------------------------------------------------------

#[test]
fn forensics_report_json_roundtrip() {
    let index = build_test_index();
    let config = ForensicsWorkflowConfig::default();
    let report = run_forensics_workflow(&index, &config);

    let json = report.to_json().expect("serialize");
    let parsed = ForensicsWorkflowReport::from_json(&json).expect("parse");

    assert_eq!(parsed.bead_id, report.bead_id);
    assert_eq!(parsed.verdict, report.verdict);
    assert_eq!(parsed.index_run_count, report.index_run_count);
}

#[test]
fn forensics_report_file_roundtrip() {
    let index = build_test_index();
    let config = ForensicsWorkflowConfig::default();
    let report = run_forensics_workflow(&index, &config);

    let dir = std::env::temp_dir().join("fsqlite-forensics-test");
    std::fs::create_dir_all(&dir).expect("create temp dir");
    let path = dir.join("forensics-test.json");

    write_forensics_report(&path, &report).expect("write");
    let loaded = load_forensics_report(&path).expect("load");

    assert_eq!(loaded.verdict, report.verdict);
    assert_eq!(loaded.index_run_count, report.index_run_count);

    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_dir(&dir);
}

// ---------------------------------------------------------------------------
// Triage and display
// ---------------------------------------------------------------------------

#[test]
fn forensics_triage_line_contains_key_info() {
    let index = build_test_index();
    let config = ForensicsWorkflowConfig::default();
    let report = run_forensics_workflow(&index, &config);
    let line = report.triage_line();

    assert!(line.contains("runs indexed"), "bead_id={BEAD_ID}");
    assert!(line.contains("matched"), "bead_id={BEAD_ID}");
}

#[test]
fn forensics_verdict_display() {
    assert_eq!(ForensicsVerdict::Pass.to_string(), "PASS");
    assert_eq!(ForensicsVerdict::Warning.to_string(), "WARNING");
    assert_eq!(ForensicsVerdict::Fail.to_string(), "FAIL");
}

#[test]
fn forensics_summary_is_nonempty() {
    let index = build_test_index();
    let config = ForensicsWorkflowConfig::default();
    let report = run_forensics_workflow(&index, &config);

    assert!(
        !report.summary.is_empty(),
        "bead_id={BEAD_ID} case=summary_nonempty"
    );
}

// ---------------------------------------------------------------------------
// Index statistics integration
// ---------------------------------------------------------------------------

#[test]
fn forensics_reports_unique_scenarios() {
    let index = build_test_index();
    let config = ForensicsWorkflowConfig::default();
    let report = run_forensics_workflow(&index, &config);

    assert!(
        report.unique_scenarios > 0,
        "bead_id={BEAD_ID} case=unique_scenarios"
    );
}

#[test]
fn forensics_reports_unique_invariants() {
    let index = build_test_index();
    let config = ForensicsWorkflowConfig::default();
    let report = run_forensics_workflow(&index, &config);

    assert!(
        report.unique_invariants > 0,
        "bead_id={BEAD_ID} case=unique_invariants"
    );
}

#[test]
fn forensics_severity_parse() {
    assert_eq!(Severity::parse("critical"), Some(Severity::Critical));
    assert_eq!(Severity::parse("high"), Some(Severity::High));
    assert_eq!(Severity::parse("medium"), Some(Severity::Medium));
    assert_eq!(Severity::parse("low"), Some(Severity::Low));
    assert_eq!(Severity::parse("unknown"), None);
}
