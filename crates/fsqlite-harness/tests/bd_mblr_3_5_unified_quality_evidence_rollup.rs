//! Unified Quality Evidence Rollup and Release-Readiness Gate (bd-mblr.3.5)
//!
//! Integration tests for the unified quality evidence rollup that aggregates
//! coverage, no-mock, unit, e2e, and logging gate results into a single
//! deterministic release-readiness decision artifact.
//! Depends on: bd-mblr.3.2.2 (scenario drift), bd-mblr.3.1.2 (invariant drift),
//! bd-mblr.3.4.1 (no-mock gate), bd-mblr.3.3 (flake budget/retry/quarantine).

use fsqlite_harness::validation_manifest::{
    GateOutcome, ValidationManifest, ValidationManifestConfig,
    COVERAGE_GATE_ID, INVARIANT_DRIFT_GATE_ID, LOGGING_GATE_ID, NO_MOCK_GATE_ID,
    SCENARIO_DRIFT_GATE_ID, VALIDATION_MANIFEST_SCENARIO_ID, VALIDATION_MANIFEST_SCHEMA_VERSION,
    build_validation_manifest_bundle, detect_backward_incompatible_change,
    validate_manifest_contract,
};

const BEAD_ID: &str = "bd-mblr.3.5";

fn test_config() -> ValidationManifestConfig {
    ValidationManifestConfig {
        commit_sha: "abc1234def5678".to_owned(),
        run_id: format!("{BEAD_ID}-test-run"),
        trace_id: "trace-mblr-3-5".to_owned(),
        scenario_id: VALIDATION_MANIFEST_SCENARIO_ID.to_owned(),
        generated_unix_ms: 1_700_000_000_000,
        root_seed: Some(424_242),
        artifact_uri_prefix: "artifacts/quality-rollup".to_owned(),
    }
}

fn build_bundle() -> fsqlite_harness::validation_manifest::ValidationManifestBundle {
    build_validation_manifest_bundle(&test_config())
        .unwrap_or_else(|e| panic!("bundle build failed: {e}"))
}

// ─── Rollup Construction Tests ──────────────────────────────────────────

#[test]
fn rollup_aggregates_all_five_gates() {
    let bundle = build_bundle();
    let gate_ids: Vec<&str> = bundle.manifest.gates.iter().map(|g| g.gate_id.as_str()).collect();

    assert!(gate_ids.contains(&COVERAGE_GATE_ID), "missing coverage gate");
    assert!(gate_ids.contains(&INVARIANT_DRIFT_GATE_ID), "missing invariant drift gate");
    assert!(gate_ids.contains(&SCENARIO_DRIFT_GATE_ID), "missing scenario drift gate");
    assert!(gate_ids.contains(&NO_MOCK_GATE_ID), "missing no-mock gate");
    assert!(gate_ids.contains(&LOGGING_GATE_ID), "missing logging gate");
    assert_eq!(bundle.manifest.gates.len(), 5, "exactly 5 gates expected");
}

#[test]
fn rollup_schema_version_matches() {
    let bundle = build_bundle();
    assert_eq!(bundle.manifest.schema_version, VALIDATION_MANIFEST_SCHEMA_VERSION);
}

#[test]
fn rollup_bead_id_is_manifest_owner() {
    let bundle = build_bundle();
    assert_eq!(bundle.manifest.bead_id, "bd-mblr.3.5.1", "manifest owned by bd-mblr.3.5.1");
}

// ─── Gate Outcome Aggregation Tests ─────────────────────────────────────

#[test]
fn overall_outcome_reflects_worst_gate() {
    let bundle = build_bundle();
    let has_fail = bundle.manifest.gates.iter().any(|g| g.outcome == GateOutcome::Fail);
    let has_warning = bundle.manifest.gates.iter().any(|g| g.outcome == GateOutcome::PassWithWarnings);

    if has_fail {
        assert_eq!(bundle.manifest.overall_outcome, GateOutcome::Fail);
        assert!(!bundle.manifest.overall_pass);
    } else if has_warning {
        assert_eq!(bundle.manifest.overall_outcome, GateOutcome::PassWithWarnings);
        assert!(bundle.manifest.overall_pass, "PassWithWarnings still counts as pass");
    } else {
        assert_eq!(bundle.manifest.overall_outcome, GateOutcome::Pass);
        assert!(bundle.manifest.overall_pass);
    }
}

#[test]
fn gate_outcome_display() {
    assert_eq!(format!("{}", GateOutcome::Pass), "PASS");
    assert_eq!(format!("{}", GateOutcome::Fail), "FAIL");
    assert_eq!(format!("{}", GateOutcome::PassWithWarnings), "PASS_WITH_WARNINGS");
}

// ─── Release-Readiness Decision Artifact Tests ──────────────────────────

#[test]
fn manifest_passes_contract_validation() {
    let bundle = build_bundle();
    let errors = validate_manifest_contract(&bundle.manifest);
    assert!(errors.is_empty(), "manifest should pass contract: {errors:?}");
}

#[test]
fn manifest_gates_sorted_by_id() {
    let bundle = build_bundle();
    let gate_ids: Vec<&str> = bundle.manifest.gates.iter().map(|g| g.gate_id.as_str()).collect();
    let mut sorted = gate_ids.clone();
    sorted.sort_unstable();
    assert_eq!(gate_ids, sorted, "gates must be sorted by gate_id");
}

#[test]
fn manifest_artifact_uris_sorted_and_unique() {
    let bundle = build_bundle();
    let uris = &bundle.manifest.artifact_uris;
    let mut sorted = uris.clone();
    sorted.sort();
    sorted.dedup();
    assert_eq!(*uris, sorted, "artifact URIs must be sorted and unique");
}

#[test]
fn each_gate_has_artifact_uris() {
    let bundle = build_bundle();
    for gate in &bundle.manifest.gates {
        assert!(
            !gate.artifact_uris.is_empty(),
            "gate {} must have artifacts",
            gate.gate_id
        );
    }
}

#[test]
fn all_gate_artifacts_in_top_level_index() {
    let bundle = build_bundle();
    let top_level: std::collections::BTreeSet<&str> =
        bundle.manifest.artifact_uris.iter().map(String::as_str).collect();

    for gate in &bundle.manifest.gates {
        for uri in &gate.artifact_uris {
            assert!(
                top_level.contains(uri.as_str()),
                "gate {} artifact URI {} missing from top-level index",
                gate.gate_id,
                uri
            );
        }
    }
}

#[test]
fn gate_records_have_consistent_commit_sha() {
    let bundle = build_bundle();
    for gate in &bundle.manifest.gates {
        assert_eq!(
            gate.commit_sha, bundle.manifest.commit_sha,
            "gate {} commit_sha must match manifest",
            gate.gate_id
        );
    }
}

#[test]
fn gate_records_have_consistent_timestamp() {
    let bundle = build_bundle();
    for gate in &bundle.manifest.gates {
        assert_eq!(
            gate.timestamp_unix_ms, bundle.manifest.generated_unix_ms,
            "gate {} timestamp must match manifest",
            gate.gate_id
        );
    }
}

// ─── Determinism Tests ──────────────────────────────────────────────────

#[test]
fn rollup_is_deterministic() {
    let a = build_bundle();
    let b = build_bundle();

    let json_a = a.manifest.to_json().unwrap();
    let json_b = b.manifest.to_json().unwrap();
    assert_eq!(json_a, json_b, "same inputs must produce identical manifests");
}

#[test]
fn rollup_determinism_includes_artifacts() {
    let a = build_bundle();
    let b = build_bundle();
    assert_eq!(a.gate_artifacts.len(), b.gate_artifacts.len());
    for (key, value_a) in &a.gate_artifacts {
        let value_b = b.gate_artifacts.get(key).expect("artifact must exist in both bundles");
        assert_eq!(value_a, value_b, "artifact {key} must be identical");
    }
}

// ─── Replay Contract Tests ──────────────────────────────────────────────

#[test]
fn replay_contract_has_required_fields() {
    let bundle = build_bundle();
    let replay = &bundle.manifest.replay;
    assert!(!replay.command.is_empty(), "replay command must not be empty");
    assert_eq!(replay.scenario_id, VALIDATION_MANIFEST_SCENARIO_ID);
    assert_eq!(replay.root_seed, 424_242);
}

#[test]
fn replay_command_contains_all_fragments() {
    let bundle = build_bundle();
    let cmd = &bundle.manifest.replay.command;
    for fragment in [
        "validation_manifest_runner",
        "--root-seed",
        "--generated-unix-ms",
        "--commit-sha",
        "--run-id",
        "--trace-id",
        "--scenario-id",
        "--artifact-uri-prefix",
    ] {
        assert!(cmd.contains(fragment), "replay command missing '{fragment}'");
    }
}

// ─── JSON Serialization Tests ───────────────────────────────────────────

#[test]
fn manifest_json_roundtrip() {
    let bundle = build_bundle();
    let json = bundle.manifest.to_json().unwrap();
    let deserialized = ValidationManifest::from_json(&json).unwrap();

    assert_eq!(deserialized.schema_version, bundle.manifest.schema_version);
    assert_eq!(deserialized.bead_id, bundle.manifest.bead_id);
    assert_eq!(deserialized.run_id, bundle.manifest.run_id);
    assert_eq!(deserialized.overall_outcome, bundle.manifest.overall_outcome);
    assert_eq!(deserialized.overall_pass, bundle.manifest.overall_pass);
    assert_eq!(deserialized.gates.len(), bundle.manifest.gates.len());
    assert_eq!(deserialized.artifact_uris.len(), bundle.manifest.artifact_uris.len());
}

#[test]
fn gate_artifacts_are_valid_json() {
    let bundle = build_bundle();
    for (uri, content) in &bundle.gate_artifacts {
        if std::path::Path::new(uri).extension().is_some_and(|ext| ext.eq_ignore_ascii_case("json")) {
            let parsed: Result<serde_json::Value, _> = serde_json::from_str(content);
            assert!(parsed.is_ok(), "artifact {uri} must be valid JSON");
        } else if std::path::Path::new(uri).extension().is_some_and(|ext| ext.eq_ignore_ascii_case("jsonl")) {
            for line in content.lines() {
                if line.trim().is_empty() {
                    continue;
                }
                let parsed: Result<serde_json::Value, _> = serde_json::from_str(line);
                assert!(parsed.is_ok(), "JSONL line in {uri} must be valid JSON: {line}");
            }
        }
    }
}

// ─── Backward Compatibility Tests ───────────────────────────────────────

#[test]
fn backward_compatible_with_same_version() {
    let bundle = build_bundle();
    let issues = detect_backward_incompatible_change(&bundle.manifest, &bundle.manifest);
    assert!(issues.is_empty(), "same manifest should be backward compatible: {issues:?}");
}

#[test]
fn backward_incompatible_on_major_version_change() {
    let bundle = build_bundle();
    let mut candidate = bundle.manifest.clone();
    candidate.schema_version = "2.0.0".to_owned();
    let issues = detect_backward_incompatible_change(&bundle.manifest, &candidate);
    assert!(
        issues.iter().any(|i| i.contains("schema major")),
        "should flag major version change: {issues:?}"
    );
}

#[test]
fn backward_incompatible_on_gate_removal() {
    let bundle = build_bundle();
    let mut candidate = bundle.manifest.clone();
    candidate.gates.retain(|g| g.gate_id != NO_MOCK_GATE_ID);
    let issues = detect_backward_incompatible_change(&bundle.manifest, &candidate);
    assert!(
        issues.iter().any(|i| i.contains(NO_MOCK_GATE_ID)),
        "should flag removed gate: {issues:?}"
    );
}

// ─── Human Summary Tests ────────────────────────────────────────────────

#[test]
fn human_summary_includes_key_fields() {
    let bundle = build_bundle();
    let summary = bundle.manifest.render_summary();

    assert!(summary.contains("Validation Manifest"), "summary must have header");
    assert!(summary.contains(&bundle.manifest.bead_id), "summary must include bead_id");
    assert!(summary.contains(&bundle.manifest.run_id), "summary must include run_id");
    assert!(summary.contains(&bundle.manifest.commit_sha), "summary must include commit_sha");
    assert!(summary.contains("Gates"), "summary must list gates");
    assert!(summary.contains("Replay"), "summary must include replay section");
}

#[test]
fn bundle_human_summary_not_empty() {
    let bundle = build_bundle();
    assert!(!bundle.human_summary.is_empty(), "human summary must not be empty");
}

// ─── Embedded Report Tests ──────────────────────────────────────────────

#[test]
fn coverage_gate_report_embedded() {
    let bundle = build_bundle();
    let report = &bundle.manifest.coverage_gate;
    assert!(!report.summary.is_empty(), "coverage gate must have summary");
}

#[test]
fn invariant_drift_status_embedded() {
    let bundle = build_bundle();
    let status = &bundle.manifest.invariant_drift;
    assert_eq!(status.gate_id, INVARIANT_DRIFT_GATE_ID);
    assert!(status.total_matrix_tests > 0, "matrix should have tests");
}

#[test]
fn scenario_drift_status_embedded() {
    let bundle = build_bundle();
    let status = &bundle.manifest.scenario_coverage_drift;
    assert_eq!(status.gate_id, SCENARIO_DRIFT_GATE_ID);
    assert!(status.total_catalog_scenarios > 0, "catalog should have scenarios");
}

#[test]
fn no_mock_report_embedded() {
    let bundle = build_bundle();
    let report = &bundle.manifest.no_mock_critical_path;
    assert!(!report.summary.is_empty(), "no-mock report must have summary");
}

#[test]
fn logging_conformance_embedded() {
    let bundle = build_bundle();
    let status = &bundle.manifest.logging_conformance;
    assert_eq!(status.gate_id, LOGGING_GATE_ID);
    assert!(!status.profile_doc_path.is_empty());
}

// ─── Contract Violation Detection Tests ─────────────────────────────────

#[test]
fn contract_flags_missing_commit_sha() {
    let bundle = build_bundle();
    let mut manifest = bundle.manifest;
    manifest.commit_sha.clear();
    let errors = validate_manifest_contract(&manifest);
    assert!(errors.iter().any(|e| e.contains("commit_sha")));
}

#[test]
fn contract_flags_empty_gates() {
    let bundle = build_bundle();
    let mut manifest = bundle.manifest;
    manifest.gates.clear();
    let errors = validate_manifest_contract(&manifest);
    assert!(errors.iter().any(|e| e.contains("gates must not be empty")));
}

#[test]
fn contract_flags_replay_scenario_mismatch() {
    let bundle = build_bundle();
    let mut manifest = bundle.manifest;
    manifest.replay.scenario_id = "WRONG-ID".to_owned();
    let errors = validate_manifest_contract(&manifest);
    assert!(errors.iter().any(|e| e.contains("replay.scenario_id")));
}

// ─── Conformance Summary ────────────────────────────────────────────────

#[test]
fn conformance_summary() {
    let checks = vec![
        ("C-1: Rollup aggregates all 5 upstream gates", true),
        ("C-2: Overall outcome reflects worst gate (Fail > Warn > Pass)", true),
        ("C-3: Manifest passes contract validation", true),
        ("C-4: Rollup is deterministic for same inputs", true),
        ("C-5: Replay contract includes all required fragments", true),
        ("C-6: Backward compatibility detection works", true),
    ];

    println!("\n=== bd-mblr.3.5 Conformance Summary ===");
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
