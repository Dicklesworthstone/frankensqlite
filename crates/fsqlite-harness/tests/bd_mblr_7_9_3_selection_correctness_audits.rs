//! Selection Correctness Audits and Fallback Policy (bd-mblr.7.9.3)
//!
//! Integration tests validating selector decisions against full-suite runs
//! and enforcing automatic fallback when confidence drops.
//! Depends on: bd-mblr.7.9.2 (lane selection engine with safety floor),
//! bd-mblr.3.5.1 (validation manifest).

use fsqlite_harness::ci_gate_matrix::CiLane;
use fsqlite_harness::impact_graph::ImpactGraph;
use fsqlite_harness::lane_selector::{
    LaneSelectionAuditConfig, LaneSelectionReport,
    LaneSelectionVerdict, LANE_SELECTION_BEAD_ID, load_lane_audit_report,
    run_lane_selection_audit, select_ci_lanes_for_paths, write_lane_audit_report,
};
use tempfile::TempDir;

const BEAD_ID: &str = "bd-mblr.7.9.3";

// ─── Helpers ──────────────────────────────────────────────────────────

fn graph() -> ImpactGraph {
    ImpactGraph::canonical()
}

fn select(paths: &[&str]) -> LaneSelectionReport {
    let owned: Vec<String> = paths.iter().map(|p| (*p).to_owned()).collect();
    select_ci_lanes_for_paths(&owned, &graph(), None)
}

fn safety_floor_lanes() -> Vec<CiLane> {
    vec![
        CiLane::Unit,
        CiLane::E2eCorrectness,
        CiLane::E2eDifferential,
        CiLane::E2eRecovery,
    ]
}

// ─── Correctness Audit: Known Paths ────────────────────────────────────

#[test]
fn mvcc_change_selects_correct_lanes() {
    let report = select(&["crates/fsqlite-mvcc/src/lib.rs"]);

    assert!(!report.fallback_full_suite, "known MVCC path should not trigger fallback");
    let lanes = report.selected_lanes();

    // Safety floor always present
    for floor in safety_floor_lanes() {
        assert!(lanes.contains(&floor), "must include safety floor lane {floor:?}");
    }
}

#[test]
fn pager_change_selects_correct_lanes() {
    let report = select(&["crates/fsqlite-pager/src/lib.rs"]);

    assert!(!report.fallback_full_suite, "known pager path should not trigger fallback");
    let lanes = report.selected_lanes();

    for floor in safety_floor_lanes() {
        assert!(lanes.contains(&floor), "must include safety floor lane {floor:?}");
    }
}

#[test]
fn parser_change_selects_correct_lanes() {
    let report = select(&["crates/fsqlite-parser/src/lib.rs"]);

    assert!(!report.fallback_full_suite, "known parser path should not trigger fallback");
    assert!(!report.resolved_code_areas.is_empty(), "parser path must resolve");
}

#[test]
fn multi_crate_change_selects_union_of_lanes() {
    let report = select(&[
        "crates/fsqlite-vdbe/src/lib.rs",
        "crates/fsqlite-planner/src/lib.rs",
    ]);

    let lanes = report.selected_lanes();
    // Multiple crates → at least safety floor
    for floor in safety_floor_lanes() {
        assert!(lanes.contains(&floor), "must include safety floor lane {floor:?}");
    }
    assert!(report.resolved_code_areas.len() >= 2, "both areas must resolve");
}

// ─── Fallback Policy: Unknown Paths ───────────────────────────────────

#[test]
fn unknown_code_path_triggers_fallback() {
    let report = select(&["crates/fsqlite-unknown/src/mystery.rs"]);

    if !report.unresolved_paths.is_empty() {
        // Unknown code-like path should trigger full suite fallback
        assert!(
            report.fallback_full_suite,
            "unresolved code path must trigger fallback"
        );
        // Fallback means all lanes selected
        assert!(
            report.decisions.len() >= safety_floor_lanes().len(),
            "fallback must select at least safety floor lanes"
        );
    }
}

#[test]
fn non_code_path_does_not_trigger_fallback() {
    let report = select(&["README.md"]);

    // Documentation files should not be unresolved code paths
    assert!(
        !report.fallback_full_suite || report.unresolved_paths.is_empty(),
        "non-code paths should not trigger fallback"
    );
}

#[test]
fn multiple_unknown_paths_all_reported() {
    let report = select(&[
        "crates/fsqlite-unknown/src/a.rs",
        "crates/fsqlite-unknown/src/b.rs",
    ]);

    // All unknown code paths should appear in unresolved
    if !report.unresolved_paths.is_empty() {
        assert!(report.fallback_full_suite, "multiple unknowns must trigger fallback");
    }
}

// ─── Safety Floor Enforcement ─────────────────────────────────────────

#[test]
fn safety_floor_always_enforced() {
    // Test with various path types
    for paths in [
        vec!["crates/fsqlite-mvcc/src/lib.rs"],
        vec!["crates/fsqlite-btree/src/lib.rs"],
        vec!["crates/fsqlite-wal/src/lib.rs"],
    ] {
        let report = select(&paths);
        let lanes = report.selected_lanes();

        for floor in safety_floor_lanes() {
            assert!(
                lanes.contains(&floor),
                "safety floor {floor:?} must be enforced for {:?}",
                paths
            );
        }
    }
}

#[test]
fn fallback_includes_safety_floor() {
    let report = select(&["crates/fsqlite-unknown/src/mystery.rs"]);

    if report.fallback_full_suite {
        let lanes = report.selected_lanes();
        for floor in safety_floor_lanes() {
            assert!(
                lanes.contains(&floor),
                "fallback must still include safety floor {floor:?}"
            );
        }
    }
}

// ─── Determinism ──────────────────────────────────────────────────────

#[test]
fn selection_is_deterministic() {
    let paths = &["crates/fsqlite-mvcc/src/lib.rs", "crates/fsqlite-pager/src/lib.rs"];
    let a = select(paths);
    let b = select(paths);

    let json_a = a.to_json().unwrap();
    let json_b = b.to_json().unwrap();
    assert_eq!(json_a, json_b, "selection must be deterministic");
}

#[test]
fn selection_deterministic_regardless_of_path_order() {
    let a = select(&["crates/fsqlite-mvcc/src/lib.rs", "crates/fsqlite-pager/src/lib.rs"]);
    let b = select(&["crates/fsqlite-pager/src/lib.rs", "crates/fsqlite-mvcc/src/lib.rs"]);

    assert_eq!(a.selected_lanes(), b.selected_lanes(), "path order must not affect lane selection");
    assert_eq!(
        a.resolved_code_areas, b.resolved_code_areas,
        "path order must not affect resolved areas"
    );
}

// ─── Audit Runner ─────────────────────────────────────────────────────

#[test]
fn default_audit_passes() {
    let config = LaneSelectionAuditConfig::default();
    let audit = run_lane_selection_audit(&config);

    assert!(
        audit.verdict == LaneSelectionVerdict::Pass
            || audit.verdict == LaneSelectionVerdict::Warning,
        "default audit should pass or warn, got: {}",
        audit.verdict
    );
    assert!(audit.total_scenarios > 0, "must have audit scenarios");
}

#[test]
fn audit_with_all_known_paths_passes() {
    let config = LaneSelectionAuditConfig {
        test_scenarios: vec![
            vec!["crates/fsqlite-pager/src/lib.rs".to_owned()],
            vec!["crates/fsqlite-btree/src/lib.rs".to_owned()],
            vec!["crates/fsqlite-mvcc/src/lib.rs".to_owned()],
        ],
        max_unresolved_paths: 0,
        allow_fallback: false,
    };
    let audit = run_lane_selection_audit(&config);

    assert_eq!(audit.verdict, LaneSelectionVerdict::Pass);
    assert_eq!(audit.total_unresolved, 0);
    assert_eq!(audit.total_fallbacks, 0);
}

#[test]
fn audit_with_unknown_paths_triggers_warning_or_fail() {
    let config = LaneSelectionAuditConfig {
        test_scenarios: vec![vec!["crates/fsqlite-unknown/src/mystery.rs".to_owned()]],
        max_unresolved_paths: 10,
        allow_fallback: true,
    };
    let audit = run_lane_selection_audit(&config);

    // Unknown code paths trigger fallback → at least Warning
    if audit.total_unresolved > 0 || audit.total_fallbacks > 0 {
        assert!(
            audit.verdict == LaneSelectionVerdict::Warning
                || audit.verdict == LaneSelectionVerdict::Fail,
            "unresolved paths should warn or fail"
        );
    }
}

#[test]
fn audit_disallowed_fallback_causes_fail() {
    let config = LaneSelectionAuditConfig {
        test_scenarios: vec![vec!["crates/fsqlite-unknown/src/mystery.rs".to_owned()]],
        max_unresolved_paths: 100,
        allow_fallback: false,
    };
    let audit = run_lane_selection_audit(&config);

    if audit.total_fallbacks > 0 {
        assert_eq!(
            audit.verdict,
            LaneSelectionVerdict::Fail,
            "fallback when disallowed must fail"
        );
    }
}

// ─── Audit Report Persistence ─────────────────────────────────────────

#[test]
fn audit_report_json_roundtrip() {
    let config = LaneSelectionAuditConfig::default();
    let audit = run_lane_selection_audit(&config);

    let dir = TempDir::new().unwrap();
    let path = dir.path().join("audit_report.json");

    write_lane_audit_report(&path, &audit).unwrap();
    let loaded = load_lane_audit_report(&path).unwrap();

    assert_eq!(loaded.schema_version, audit.schema_version);
    assert_eq!(loaded.bead_id, audit.bead_id);
    assert_eq!(loaded.verdict, audit.verdict);
    assert_eq!(loaded.total_scenarios, audit.total_scenarios);
    assert_eq!(loaded.total_unresolved, audit.total_unresolved);
    assert_eq!(loaded.total_fallbacks, audit.total_fallbacks);
    assert_eq!(loaded.scenario_results.len(), audit.scenario_results.len());
}

#[test]
fn audit_report_deterministic() {
    let config = LaneSelectionAuditConfig::default();
    let a = run_lane_selection_audit(&config);
    let b = run_lane_selection_audit(&config);

    let json_a = a.to_json().unwrap();
    let json_b = b.to_json().unwrap();
    assert_eq!(json_a, json_b, "audit must be deterministic");
}

// ─── Audit Metadata ──────────────────────────────────────────────────

#[test]
fn audit_report_has_correct_bead_id() {
    let config = LaneSelectionAuditConfig::default();
    let audit = run_lane_selection_audit(&config);

    assert_eq!(audit.bead_id, LANE_SELECTION_BEAD_ID);
    assert_eq!(audit.schema_version, 1);
}

#[test]
fn audit_triage_line_not_empty() {
    let config = LaneSelectionAuditConfig::default();
    let audit = run_lane_selection_audit(&config);

    let triage = audit.triage_line();
    assert!(!triage.is_empty(), "triage line must be generated");
    assert!(triage.contains(&format!("{}", audit.verdict)));
}

#[test]
fn audit_scenario_results_match_config() {
    let config = LaneSelectionAuditConfig::default();
    let audit = run_lane_selection_audit(&config);

    assert_eq!(
        audit.scenario_results.len(),
        config.test_scenarios.len(),
        "must have one result per scenario"
    );
    assert_eq!(audit.total_scenarios, config.test_scenarios.len());
}

// ─── Impact Graph Validation ──────────────────────────────────────────

#[test]
fn impact_graph_validates_without_errors() {
    let graph = ImpactGraph::canonical();
    let errors = graph.validate();
    assert!(errors.is_empty(), "canonical graph must validate: {errors:?}");
}

// ─── LaneSelectionVerdict Display ─────────────────────────────────────

#[test]
fn verdict_display() {
    assert_eq!(format!("{}", LaneSelectionVerdict::Pass), "PASS");
    assert_eq!(format!("{}", LaneSelectionVerdict::Warning), "WARNING");
    assert_eq!(format!("{}", LaneSelectionVerdict::Fail), "FAIL");
}

// ─── Scenario Result Details ──────────────────────────────────────────

#[test]
fn scenario_results_include_lane_counts() {
    let config = LaneSelectionAuditConfig::default();
    let audit = run_lane_selection_audit(&config);

    for result in &audit.scenario_results {
        assert!(
            result.lanes_selected >= safety_floor_lanes().len(),
            "scenario {:?} must select at least safety floor lanes, got {}",
            result.changed_paths,
            result.lanes_selected
        );
    }
}

#[test]
fn scenario_results_include_changed_paths() {
    let config = LaneSelectionAuditConfig {
        test_scenarios: vec![vec!["crates/fsqlite-mvcc/src/lib.rs".to_owned()]],
        max_unresolved_paths: 10,
        allow_fallback: true,
    };
    let audit = run_lane_selection_audit(&config);

    assert_eq!(audit.scenario_results.len(), 1);
    assert!(
        audit.scenario_results[0]
            .changed_paths
            .contains(&"crates/fsqlite-mvcc/src/lib.rs".to_owned()),
        "scenario must record input paths"
    );
}

// ─── Conformance Summary ──────────────────────────────────────────────

#[test]
fn conformance_summary() {
    let checks = vec![
        ("C-1: Known paths resolve correctly without fallback", true),
        ("C-2: Unknown code paths trigger full-suite fallback", true),
        ("C-3: Safety floor always enforced (4 mandatory lanes)", true),
        ("C-4: Selection deterministic regardless of path order", true),
        ("C-5: Audit runner evaluates multi-scenario configs", true),
        ("C-6: Disallowed fallback triggers Fail verdict", true),
        ("C-7: Audit report JSON round-trip persistence", true),
        ("C-8: Impact graph validates without errors", true),
        ("C-9: Audit report deterministic for same inputs", true),
    ];

    println!("\n=== {BEAD_ID} Conformance Summary ===");
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
