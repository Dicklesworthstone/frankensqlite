//! Integration tests for bd-mblr.7.9 — Risk-Aware Change-Impact Lane Selection.
//!
//! Tests the lane selection audit orchestrator that ties together the impact
//! graph (bd-mblr.7.9.1) and lane selector (bd-mblr.7.9.2).

use fsqlite_harness::impact_graph::{ImpactGraph, compute_impact_coverage};
use fsqlite_harness::lane_selector::{
    LANE_SELECTION_BEAD_ID, LaneSelectionAuditConfig, LaneSelectionAuditReport,
    LaneSelectionVerdict, load_lane_audit_report, run_lane_selection_audit,
    write_lane_audit_report,
};

const BEAD_ID: &str = "bd-mblr.7.9";

// ---------------------------------------------------------------------------
// Audit — full pipeline
// ---------------------------------------------------------------------------

#[test]
fn lane_audit_runs_default_scenarios() {
    let config = LaneSelectionAuditConfig::default();
    let report = run_lane_selection_audit(&config);

    assert_eq!(
        report.bead_id, LANE_SELECTION_BEAD_ID,
        "bead_id={BEAD_ID} case=bead_id"
    );
    assert_eq!(
        report.schema_version, 1,
        "bead_id={BEAD_ID} case=schema_version"
    );
    assert!(
        report.total_scenarios > 0,
        "bead_id={BEAD_ID} case=scenarios_nonzero"
    );
}

#[test]
fn lane_audit_passes_with_known_paths() {
    let config = LaneSelectionAuditConfig::default();
    let report = run_lane_selection_audit(&config);

    // With canonical graph and well-known paths, should not fail
    assert_ne!(
        report.verdict,
        LaneSelectionVerdict::Fail,
        "bead_id={BEAD_ID} case=known_paths_not_fail verdict={:?}",
        report.verdict
    );
}

#[test]
fn lane_audit_reports_scenario_results() {
    let config = LaneSelectionAuditConfig::default();
    let report = run_lane_selection_audit(&config);

    assert_eq!(
        report.scenario_results.len(),
        config.test_scenarios.len(),
        "bead_id={BEAD_ID} case=scenario_result_count"
    );

    for result in &report.scenario_results {
        assert!(
            result.lanes_selected > 0,
            "bead_id={BEAD_ID} case=scenario_has_lanes paths={:?}",
            result.changed_paths
        );
    }
}

#[test]
fn lane_audit_with_single_path() {
    let config = LaneSelectionAuditConfig {
        test_scenarios: vec![vec!["crates/fsqlite-pager/src/lib.rs".to_owned()]],
        ..Default::default()
    };
    let report = run_lane_selection_audit(&config);

    assert_eq!(report.total_scenarios, 1);
    assert!(
        report.scenario_results[0].lanes_selected > 0,
        "bead_id={BEAD_ID} case=single_path_selects_lanes"
    );
}

#[test]
fn lane_audit_with_unknown_paths_triggers_fallback() {
    let config = LaneSelectionAuditConfig {
        test_scenarios: vec![vec!["nonexistent/src/unknown.rs".to_owned()]],
        ..Default::default()
    };
    let report = run_lane_selection_audit(&config);

    // Unknown paths should trigger fallback or unresolved
    let result = &report.scenario_results[0];
    assert!(
        result.fallback_triggered || result.unresolved_count > 0,
        "bead_id={BEAD_ID} case=unknown_path_fallback_or_unresolved"
    );
}

// ---------------------------------------------------------------------------
// Verdict logic
// ---------------------------------------------------------------------------

#[test]
fn lane_selection_verdict_display() {
    assert_eq!(LaneSelectionVerdict::Pass.to_string(), "PASS");
    assert_eq!(LaneSelectionVerdict::Warning.to_string(), "WARNING");
    assert_eq!(LaneSelectionVerdict::Fail.to_string(), "FAIL");
}

#[test]
fn lane_audit_fails_when_fallback_disallowed_but_triggered() {
    let config = LaneSelectionAuditConfig {
        test_scenarios: vec![vec!["nonexistent/src/unknown.rs".to_owned()]],
        allow_fallback: false,
        max_unresolved_paths: 0,
        ..Default::default()
    };
    let report = run_lane_selection_audit(&config);

    // Should fail because unresolved paths exceed threshold or fallback disallowed
    assert_eq!(
        report.verdict,
        LaneSelectionVerdict::Fail,
        "bead_id={BEAD_ID} case=disallowed_fallback_fails"
    );
}

// ---------------------------------------------------------------------------
// Report serialization
// ---------------------------------------------------------------------------

#[test]
fn lane_audit_report_json_roundtrip() {
    let config = LaneSelectionAuditConfig::default();
    let report = run_lane_selection_audit(&config);

    let json = report.to_json().expect("serialize");
    let parsed = LaneSelectionAuditReport::from_json(&json).expect("parse");

    assert_eq!(parsed.bead_id, report.bead_id);
    assert_eq!(parsed.verdict, report.verdict);
    assert_eq!(parsed.total_scenarios, report.total_scenarios);
}

#[test]
fn lane_audit_report_file_roundtrip() {
    let config = LaneSelectionAuditConfig::default();
    let report = run_lane_selection_audit(&config);

    let dir = std::env::temp_dir().join("fsqlite-lane-audit-test");
    std::fs::create_dir_all(&dir).expect("create temp dir");
    let path = dir.join("lane-audit-test.json");

    write_lane_audit_report(&path, &report).expect("write");
    let loaded = load_lane_audit_report(&path).expect("load");

    assert_eq!(loaded.verdict, report.verdict);
    assert_eq!(loaded.total_scenarios, report.total_scenarios);

    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_dir(&dir);
}

// ---------------------------------------------------------------------------
// Triage
// ---------------------------------------------------------------------------

#[test]
fn lane_audit_triage_line_contains_key_info() {
    let config = LaneSelectionAuditConfig::default();
    let report = run_lane_selection_audit(&config);
    let line = report.triage_line();

    assert!(line.contains("scenarios"), "bead_id={BEAD_ID}");
}

#[test]
fn lane_audit_summary_is_nonempty() {
    let config = LaneSelectionAuditConfig::default();
    let report = run_lane_selection_audit(&config);

    assert!(
        !report.summary.is_empty(),
        "bead_id={BEAD_ID} case=summary_nonempty"
    );
}

// ---------------------------------------------------------------------------
// Graph validation
// ---------------------------------------------------------------------------

#[test]
fn lane_audit_validates_impact_graph() {
    let config = LaneSelectionAuditConfig::default();
    let report = run_lane_selection_audit(&config);

    assert!(
        report.graph_validation_errors.is_empty(),
        "bead_id={BEAD_ID} case=graph_valid errors={:?}",
        report.graph_validation_errors
    );
}

// ---------------------------------------------------------------------------
// Child bead integration
// ---------------------------------------------------------------------------

#[test]
fn impact_graph_canonical_is_valid() {
    let graph = ImpactGraph::canonical();
    let errors = graph.validate();
    assert!(
        errors.is_empty(),
        "bead_id={BEAD_ID} case=canonical_graph_valid errors={errors:?}"
    );
}

#[test]
fn impact_coverage_is_complete() {
    let graph = ImpactGraph::canonical();
    let cov = compute_impact_coverage(&graph);

    assert!(cov.code_area_count > 0, "bead_id={BEAD_ID} case=code_areas");
    assert!(cov.invariant_count > 0, "bead_id={BEAD_ID} case=invariants");
    assert!(cov.scenario_count > 0, "bead_id={BEAD_ID} case=scenarios");
    assert!(cov.total_edges > 0, "bead_id={BEAD_ID} case=edges");
}

#[test]
fn lane_audit_deterministic() {
    let config = LaneSelectionAuditConfig::default();
    let r1 = run_lane_selection_audit(&config);
    let r2 = run_lane_selection_audit(&config);

    assert_eq!(
        r1.verdict, r2.verdict,
        "bead_id={BEAD_ID} case=deterministic_verdict"
    );
    assert_eq!(
        r1.total_unresolved, r2.total_unresolved,
        "bead_id={BEAD_ID} case=deterministic_unresolved"
    );
}
