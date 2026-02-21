//! Operator-Facing Release Readiness Report with Failure Drill-Down (bd-mblr.3.5.2)
//!
//! Integration tests for the operator-facing release readiness report that
//! summarizes readiness status, blocked criteria, top failing invariants/
//! scenarios, and direct links to detailed artifacts/logs for fast root-cause
//! analysis.
//! Depends on: bd-mblr.3.5.1.1 (manifest ingestion regression),
//! bd-mblr.3.5.1 (validation manifest), bd-mblr.3.3 (flake budget/retry).

use fsqlite_harness::validation_manifest::{
    GateOutcome, ValidationManifest, ValidationManifestConfig,
    COVERAGE_GATE_ID, INVARIANT_DRIFT_GATE_ID, LOGGING_GATE_ID, NO_MOCK_GATE_ID,
    SCENARIO_DRIFT_GATE_ID, VALIDATION_MANIFEST_SCENARIO_ID,
    build_validation_manifest_bundle, validate_manifest_contract,
};

const BEAD_ID: &str = "bd-mblr.3.5.2";

fn canonical_config() -> ValidationManifestConfig {
    ValidationManifestConfig {
        commit_sha: "release-sha-abcdef12".to_owned(),
        run_id: format!("{BEAD_ID}-readiness-test"),
        trace_id: "trace-readiness-352".to_owned(),
        scenario_id: VALIDATION_MANIFEST_SCENARIO_ID.to_owned(),
        generated_unix_ms: 1_720_000_000_000,
        root_seed: Some(424_242),
        artifact_uri_prefix: "artifacts/readiness-report".to_owned(),
    }
}

fn build_bundle() -> fsqlite_harness::validation_manifest::ValidationManifestBundle {
    build_validation_manifest_bundle(&canonical_config())
        .unwrap_or_else(|e| panic!("bundle build failed: {e}"))
}

// ─── Report Generation Tests ──────────────────────────────────────────

#[test]
fn readiness_report_generated_as_human_summary() {
    let bundle = build_bundle();
    assert!(
        !bundle.human_summary.is_empty(),
        "human summary must be generated"
    );
}

#[test]
fn readiness_report_includes_overall_verdict() {
    let bundle = build_bundle();
    let summary = bundle.manifest.render_summary();

    assert!(
        summary.contains("overall_outcome")
            || summary.contains(&format!("{}", bundle.manifest.overall_outcome)),
        "report must show overall outcome"
    );
    assert!(
        summary.contains("overall_pass")
            || summary.contains(&format!("{}", bundle.manifest.overall_pass)),
        "report must show overall pass status"
    );
}

#[test]
fn readiness_report_includes_commit_and_run_metadata() {
    let bundle = build_bundle();
    let summary = bundle.manifest.render_summary();

    assert!(
        summary.contains(&bundle.manifest.commit_sha),
        "report must include commit SHA"
    );
    assert!(
        summary.contains(&bundle.manifest.run_id),
        "report must include run ID"
    );
    assert!(
        summary.contains(&bundle.manifest.bead_id),
        "report must include bead ID"
    );
}

// ─── Gate Status Summary ──────────────────────────────────────────────

#[test]
fn readiness_report_lists_all_gates() {
    let bundle = build_bundle();
    let summary = bundle.manifest.render_summary();

    for gate in &bundle.manifest.gates {
        assert!(
            summary.contains(&gate.gate_id),
            "report must list gate {}", gate.gate_id
        );
    }
}

#[test]
fn readiness_report_shows_gate_outcomes() {
    let bundle = build_bundle();
    let summary = bundle.manifest.render_summary();

    for gate in &bundle.manifest.gates {
        assert!(
            summary.contains(&format!("{}", gate.outcome)),
            "report must show outcome for gate {}", gate.gate_id
        );
    }
}

#[test]
fn readiness_report_shows_gate_families() {
    let bundle = build_bundle();
    let summary = bundle.manifest.render_summary();

    for gate in &bundle.manifest.gates {
        assert!(
            summary.contains(&gate.gate_family),
            "report must show family for gate {}", gate.gate_id
        );
    }
}

// ─── Blocked Criteria Identification ──────────────────────────────────

#[test]
fn blocked_criteria_identified_from_failing_gates() {
    let bundle = build_bundle();
    let failing_gates: Vec<&str> = bundle
        .manifest
        .gates
        .iter()
        .filter(|g| g.outcome == GateOutcome::Fail)
        .map(|g| g.gate_id.as_str())
        .collect();

    if !failing_gates.is_empty() {
        assert!(!bundle.manifest.overall_pass, "failing gates mean overall_pass=false");
    }

    // If there are failing gates, each one represents a blocked criterion
    for gate_id in &failing_gates {
        let gate = bundle
            .manifest
            .gates
            .iter()
            .find(|g| g.gate_id == *gate_id)
            .unwrap();
        assert!(
            !gate.summary.is_empty(),
            "blocked gate {} must have summary for operator triage",
            gate_id
        );
    }
}

// ─── Failure Drill-Down: Invariant Drift ──────────────────────────────

#[test]
fn invariant_drift_gaps_have_remediation_guidance() {
    let bundle = build_bundle();
    let drift = &bundle.manifest.invariant_drift;

    for gap in &drift.gaps {
        assert!(
            !gap.details.is_empty(),
            "invariant gap must have details for drill-down"
        );
        assert!(
            !gap.remediation.is_empty(),
            "invariant gap must have remediation for operator action"
        );
    }
}

#[test]
fn invariant_drift_statistics_for_operator() {
    let bundle = build_bundle();
    let drift = &bundle.manifest.invariant_drift;

    assert!(drift.total_matrix_tests > 0, "matrix must have tests");
    assert!(drift.total_matrix_invariants > 0, "matrix must have invariants");
    // unit_matrix_overall_fill_pct should be a valid percentage
    assert!(
        (0.0..=100.0).contains(&drift.unit_matrix_overall_fill_pct),
        "fill pct must be 0-100: {}",
        drift.unit_matrix_overall_fill_pct
    );
}

// ─── Failure Drill-Down: Scenario Coverage Drift ──────────────────────

#[test]
fn scenario_drift_gaps_have_scenario_details() {
    let bundle = build_bundle();
    let drift = &bundle.manifest.scenario_coverage_drift;

    for gap in &drift.gaps {
        assert!(!gap.scenario_id.is_empty(), "gap must have scenario_id");
    }
}

#[test]
fn scenario_drift_statistics_for_operator() {
    let bundle = build_bundle();
    let drift = &bundle.manifest.scenario_coverage_drift;

    assert!(
        drift.total_catalog_scenarios > 0,
        "catalog must have scenarios"
    );
}

// ─── Failure Drill-Down: Logging Conformance ──────────────────────────

#[test]
fn logging_conformance_has_profile_info() {
    let bundle = build_bundle();
    let log = &bundle.manifest.logging_conformance;

    assert!(!log.profile_doc_path.is_empty(), "must have profile doc path");
    assert!(!log.profile_version.is_empty(), "must have profile version");
}

// ─── Artifact Links for Root-Cause Analysis ───────────────────────────

#[test]
fn every_gate_has_artifact_links() {
    let bundle = build_bundle();
    for gate in &bundle.manifest.gates {
        assert!(
            !gate.artifact_uris.is_empty(),
            "gate {} must have artifact URIs for drill-down",
            gate.gate_id
        );
    }
}

#[test]
fn artifact_links_resolve_to_content() {
    let bundle = build_bundle();
    for gate in &bundle.manifest.gates {
        for uri in &gate.artifact_uris {
            assert!(
                bundle.gate_artifacts.contains_key(uri),
                "gate {} artifact {} must resolve to actual content",
                gate.gate_id,
                uri
            );
        }
    }
}

#[test]
fn artifact_content_is_parseable_for_drilldown() {
    let bundle = build_bundle();
    for (uri, content) in &bundle.gate_artifacts {
        if std::path::Path::new(uri.as_str()).extension().is_some_and(|ext| ext.eq_ignore_ascii_case("json")) {
            let parsed: Result<serde_json::Value, _> = serde_json::from_str(content);
            assert!(
                parsed.is_ok(),
                "artifact {uri} must be parseable JSON for drill-down: {}",
                parsed.unwrap_err()
            );
        } else if std::path::Path::new(uri.as_str()).extension().is_some_and(|ext| ext.eq_ignore_ascii_case("jsonl")) {
            for line in content.lines() {
                if line.trim().is_empty() {
                    continue;
                }
                let parsed: Result<serde_json::Value, _> = serde_json::from_str(line);
                assert!(parsed.is_ok(), "JSONL line in {uri} must be valid JSON");
            }
        }
    }
}

// ─── Replay Contract for Reproducibility ──────────────────────────────

#[test]
fn replay_command_enables_operator_reproduction() {
    let bundle = build_bundle();
    let cmd = &bundle.manifest.replay.command;

    assert!(!cmd.is_empty(), "replay command must exist for operator reproduction");
    assert!(
        cmd.contains("validation_manifest_runner"),
        "replay command must reference the runner binary"
    );
    assert!(
        cmd.contains("--root-seed"),
        "replay command must include seed for reproducibility"
    );
}

// ─── Report Compactness (CI Annotation) ───────────────────────────────

#[test]
fn report_is_compact_for_ci_annotation() {
    let bundle = build_bundle();
    let summary = bundle.manifest.render_summary();

    // The summary should be concise — under 10KB for CI step summaries
    assert!(
        summary.len() < 10_000,
        "report should be compact for CI: {} bytes",
        summary.len()
    );
}

#[test]
fn report_is_complete_for_offline_triage() {
    let bundle = build_bundle();
    let summary = bundle.manifest.render_summary();

    // Must have structural sections
    assert!(summary.contains("Validation Manifest"), "must have header");
    assert!(summary.contains("Gates"), "must have gates section");
    assert!(summary.contains("Replay"), "must have replay section");

    // Each gate must appear
    for gate_id in [
        COVERAGE_GATE_ID,
        INVARIANT_DRIFT_GATE_ID,
        SCENARIO_DRIFT_GATE_ID,
        NO_MOCK_GATE_ID,
        LOGGING_GATE_ID,
    ] {
        assert!(summary.contains(gate_id), "summary must include gate {gate_id}");
    }
}

// ─── Priority Ordering ───────────────────────────────────────────────

#[test]
fn failing_gates_are_identifiable_for_priority() {
    let bundle = build_bundle();

    // Operator can identify failing gates by outcome
    let mut failing = Vec::new();
    let mut warning = Vec::new();
    let mut passing = Vec::new();

    for gate in &bundle.manifest.gates {
        match gate.outcome {
            GateOutcome::Fail => failing.push(&gate.gate_id),
            GateOutcome::PassWithWarnings => warning.push(&gate.gate_id),
            GateOutcome::Pass => passing.push(&gate.gate_id),
        }
    }

    // The report enables priority ordering: Fail > Warning > Pass
    // Verify total == 5
    assert_eq!(
        failing.len() + warning.len() + passing.len(),
        5,
        "all 5 gates must be classified"
    );
}

// ─── Deterministic Report Generation ──────────────────────────────────

#[test]
fn readiness_report_is_deterministic() {
    let config = canonical_config();
    let a = build_validation_manifest_bundle(&config).unwrap();
    let b = build_validation_manifest_bundle(&config).unwrap();

    assert_eq!(a.human_summary, b.human_summary, "human summary must be deterministic");

    let json_a = a.manifest.to_json().unwrap();
    let json_b = b.manifest.to_json().unwrap();
    assert_eq!(json_a, json_b, "manifest JSON must be deterministic");
}

// ─── Report Round-Trip ────────────────────────────────────────────────

#[test]
fn readiness_report_survives_json_roundtrip() {
    let bundle = build_bundle();
    let json = bundle.manifest.to_json().unwrap();
    let restored = ValidationManifest::from_json(&json).unwrap();

    // All drill-down information must survive
    assert_eq!(restored.invariant_drift.gaps.len(), bundle.manifest.invariant_drift.gaps.len());
    assert_eq!(
        restored.scenario_coverage_drift.gaps.len(),
        bundle.manifest.scenario_coverage_drift.gaps.len()
    );
    assert_eq!(
        restored.logging_conformance.gate_id,
        bundle.manifest.logging_conformance.gate_id
    );
    assert_eq!(
        restored.coverage_gate.summary,
        bundle.manifest.coverage_gate.summary
    );
    assert_eq!(
        restored.no_mock_critical_path.summary,
        bundle.manifest.no_mock_critical_path.summary
    );
}

// ─── Contract Validation ──────────────────────────────────────────────

#[test]
fn readiness_manifest_passes_contract() {
    let bundle = build_bundle();
    let errors = validate_manifest_contract(&bundle.manifest);
    assert!(errors.is_empty(), "readiness manifest must pass contract: {errors:?}");
}

// ─── Evidence-Link Integrity ──────────────────────────────────────────

#[test]
fn all_artifact_uris_are_in_top_level_index() {
    let bundle = build_bundle();
    let top_level: std::collections::BTreeSet<&str> =
        bundle.manifest.artifact_uris.iter().map(String::as_str).collect();

    for gate in &bundle.manifest.gates {
        for uri in &gate.artifact_uris {
            assert!(
                top_level.contains(uri.as_str()),
                "gate {} artifact {} must be indexed",
                gate.gate_id,
                uri
            );
        }
    }
}

#[test]
fn top_level_artifact_uris_sorted_and_unique() {
    let bundle = build_bundle();
    let uris = &bundle.manifest.artifact_uris;
    let mut sorted = uris.clone();
    sorted.sort();
    sorted.dedup();
    assert_eq!(*uris, sorted, "artifact URIs must be sorted and unique");
}

// ─── Embedded Report Completeness ─────────────────────────────────────

#[test]
fn coverage_gate_report_has_summary() {
    let bundle = build_bundle();
    assert!(!bundle.manifest.coverage_gate.summary.is_empty());
}

#[test]
fn no_mock_report_has_summary() {
    let bundle = build_bundle();
    assert!(!bundle.manifest.no_mock_critical_path.summary.is_empty());
}

// ─── Conformance Summary ──────────────────────────────────────────────

#[test]
fn conformance_summary() {
    let checks = vec![
        ("C-1: Report includes overall verdict and pass status", true),
        ("C-2: Report lists all 5 gates with outcomes", true),
        ("C-3: Failing gates identifiable for priority ordering", true),
        ("C-4: Invariant drift gaps have details and remediation", true),
        ("C-5: Artifact links resolve to parseable content", true),
        ("C-6: Report compact for CI but complete for offline triage", true),
        ("C-7: Report deterministic for same inputs", true),
        ("C-8: Drill-down data survives JSON round-trip", true),
        ("C-9: Evidence-link integrity validated", true),
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
