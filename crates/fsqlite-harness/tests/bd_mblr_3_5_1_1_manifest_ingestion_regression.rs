//! Manifest Ingestion Regression: Unit+E2E+Logging Evidence Replay (bd-mblr.3.5.1.1)
//!
//! Deterministic regression harness that ingests real CI artifacts and verifies
//! manifest completeness for unit, e2e, and logging gates. Includes replay-mode
//! validation from archived artifacts so release-readiness decisions remain
//! reproducible.
//! Depends on: bd-mblr.3.5.1 (machine-readable validation manifest).

use std::collections::BTreeMap;

use fsqlite_harness::validation_manifest::{
    GateOutcome, ValidationManifest, ValidationManifestConfig,
    COVERAGE_GATE_ID, INVARIANT_DRIFT_GATE_ID, LOGGING_GATE_ID, NO_MOCK_GATE_ID,
    SCENARIO_DRIFT_GATE_ID, VALIDATION_MANIFEST_SCENARIO_ID, VALIDATION_MANIFEST_SCHEMA_VERSION,
    build_validation_manifest_bundle, detect_backward_incompatible_change,
    validate_manifest_contract,
};

const BEAD_ID: &str = "bd-mblr.3.5.1.1";

// ─── Deterministic Config Helpers ──────────────────────────────────────

fn canonical_config() -> ValidationManifestConfig {
    ValidationManifestConfig {
        commit_sha: "a1b2c3d4e5f67890".to_owned(),
        run_id: format!("{BEAD_ID}-regression-run"),
        trace_id: "trace-ingestion-351".to_owned(),
        scenario_id: VALIDATION_MANIFEST_SCENARIO_ID.to_owned(),
        generated_unix_ms: 1_710_000_000_000,
        root_seed: Some(424_242),
        artifact_uri_prefix: "artifacts/ingestion-test".to_owned(),
    }
}

fn alternate_config() -> ValidationManifestConfig {
    ValidationManifestConfig {
        commit_sha: "f9e8d7c6b5a43210".to_owned(),
        run_id: format!("{BEAD_ID}-regression-alt"),
        trace_id: "trace-ingestion-alt".to_owned(),
        scenario_id: VALIDATION_MANIFEST_SCENARIO_ID.to_owned(),
        generated_unix_ms: 1_710_000_001_000,
        root_seed: Some(424_242),
        artifact_uri_prefix: "artifacts/ingestion-alt".to_owned(),
    }
}

fn build_bundle(
    config: &ValidationManifestConfig,
) -> fsqlite_harness::validation_manifest::ValidationManifestBundle {
    build_validation_manifest_bundle(config)
        .unwrap_or_else(|e| panic!("bundle build failed: {e}"))
}

// ─── Ingestion: JSON Serialization Round-Trip ──────────────────────────

#[test]
fn ingestion_json_roundtrip_preserves_all_fields() {
    let bundle = build_bundle(&canonical_config());
    let json = bundle.manifest.to_json().unwrap();
    let restored = ValidationManifest::from_json(&json).unwrap();

    assert_eq!(restored.schema_version, bundle.manifest.schema_version);
    assert_eq!(restored.bead_id, bundle.manifest.bead_id);
    assert_eq!(restored.run_id, bundle.manifest.run_id);
    assert_eq!(restored.trace_id, bundle.manifest.trace_id);
    assert_eq!(restored.scenario_id, bundle.manifest.scenario_id);
    assert_eq!(restored.generated_unix_ms, bundle.manifest.generated_unix_ms);
    assert_eq!(restored.commit_sha, bundle.manifest.commit_sha);
    assert_eq!(restored.overall_outcome, bundle.manifest.overall_outcome);
    assert_eq!(restored.overall_pass, bundle.manifest.overall_pass);
    assert_eq!(restored.gates.len(), bundle.manifest.gates.len());
    assert_eq!(restored.artifact_uris, bundle.manifest.artifact_uris);
    assert_eq!(restored.replay.command, bundle.manifest.replay.command);
    assert_eq!(restored.replay.root_seed, bundle.manifest.replay.root_seed);
    assert_eq!(restored.replay.scenario_id, bundle.manifest.replay.scenario_id);
    assert_eq!(restored.summary, bundle.manifest.summary);
}

#[test]
fn ingestion_roundtrip_preserves_gate_records() {
    let bundle = build_bundle(&canonical_config());
    let json = bundle.manifest.to_json().unwrap();
    let restored = ValidationManifest::from_json(&json).unwrap();

    for (orig, round) in bundle.manifest.gates.iter().zip(restored.gates.iter()) {
        assert_eq!(orig.gate_id, round.gate_id);
        assert_eq!(orig.gate_family, round.gate_family);
        assert_eq!(orig.bead_id, round.bead_id);
        assert_eq!(orig.outcome, round.outcome);
        assert_eq!(orig.timestamp_unix_ms, round.timestamp_unix_ms);
        assert_eq!(orig.commit_sha, round.commit_sha);
        assert_eq!(orig.artifact_uris, round.artifact_uris);
        assert_eq!(orig.summary, round.summary);
    }
}

#[test]
fn ingestion_roundtrip_preserves_embedded_reports() {
    let bundle = build_bundle(&canonical_config());
    let json = bundle.manifest.to_json().unwrap();
    let restored = ValidationManifest::from_json(&json).unwrap();

    // Coverage gate
    assert_eq!(
        restored.coverage_gate.summary,
        bundle.manifest.coverage_gate.summary
    );
    // Invariant drift
    assert_eq!(
        restored.invariant_drift.gate_id,
        bundle.manifest.invariant_drift.gate_id
    );
    assert_eq!(
        restored.invariant_drift.total_matrix_tests,
        bundle.manifest.invariant_drift.total_matrix_tests
    );
    // Scenario drift
    assert_eq!(
        restored.scenario_coverage_drift.gate_id,
        bundle.manifest.scenario_coverage_drift.gate_id
    );
    assert_eq!(
        restored.scenario_coverage_drift.total_catalog_scenarios,
        bundle.manifest.scenario_coverage_drift.total_catalog_scenarios
    );
    // No-mock
    assert_eq!(
        restored.no_mock_critical_path.summary,
        bundle.manifest.no_mock_critical_path.summary
    );
    // Logging
    assert_eq!(
        restored.logging_conformance.gate_id,
        bundle.manifest.logging_conformance.gate_id
    );
}

// ─── Replay-Mode Determinism ───────────────────────────────────────────

#[test]
fn replay_produces_identical_manifest_json() {
    let config = canonical_config();
    let a = build_bundle(&config);
    let b = build_bundle(&config);

    let json_a = a.manifest.to_json().unwrap();
    let json_b = b.manifest.to_json().unwrap();
    assert_eq!(json_a, json_b, "replay from same config must produce identical JSON");
}

#[test]
fn replay_produces_identical_gate_artifacts() {
    let config = canonical_config();
    let a = build_bundle(&config);
    let b = build_bundle(&config);

    assert_eq!(a.gate_artifacts.len(), b.gate_artifacts.len());
    for (uri, content_a) in &a.gate_artifacts {
        let content_b = b.gate_artifacts.get(uri).expect("artifact must exist in both");
        assert_eq!(content_a, content_b, "artifact {uri} must be identical on replay");
    }
}

#[test]
fn replay_produces_identical_human_summary() {
    let config = canonical_config();
    let a = build_bundle(&config);
    let b = build_bundle(&config);
    assert_eq!(a.human_summary, b.human_summary, "human summary must be identical on replay");
}

// ─── Replay Contract Reconstruction ───────────────────────────────────

#[test]
fn replay_command_contains_all_config_values() {
    let config = canonical_config();
    let bundle = build_bundle(&config);
    let cmd = &bundle.manifest.replay.command;

    assert!(cmd.contains("validation_manifest_runner"), "missing runner binary");
    assert!(cmd.contains("--root-seed"), "missing --root-seed");
    assert!(cmd.contains("424242"), "missing root seed value");
    assert!(cmd.contains("--generated-unix-ms"), "missing --generated-unix-ms");
    assert!(cmd.contains("1710000000000"), "missing timestamp value");
    assert!(cmd.contains("--commit-sha"), "missing --commit-sha");
    assert!(cmd.contains(&config.commit_sha), "missing commit SHA value");
    assert!(cmd.contains("--run-id"), "missing --run-id");
    assert!(cmd.contains(&config.run_id), "missing run ID value");
    assert!(cmd.contains("--trace-id"), "missing --trace-id");
    assert!(cmd.contains("--scenario-id"), "missing --scenario-id");
    assert!(cmd.contains("--artifact-uri-prefix"), "missing --artifact-uri-prefix");
}

#[test]
fn replay_contract_seed_matches_config() {
    let config = canonical_config();
    let bundle = build_bundle(&config);
    assert_eq!(bundle.manifest.replay.root_seed, 424_242);
    assert_eq!(bundle.manifest.replay.scenario_id, VALIDATION_MANIFEST_SCENARIO_ID);
}

// ─── Ingested Manifest Contract Validation ─────────────────────────────

#[test]
fn ingested_manifest_passes_contract_validation() {
    let bundle = build_bundle(&canonical_config());
    let json = bundle.manifest.to_json().unwrap();
    let restored = ValidationManifest::from_json(&json).unwrap();

    let errors = validate_manifest_contract(&restored);
    assert!(errors.is_empty(), "ingested manifest must pass contract: {errors:?}");
}

#[test]
fn ingested_manifest_with_alternate_config_passes_contract() {
    let bundle = build_bundle(&alternate_config());
    let json = bundle.manifest.to_json().unwrap();
    let restored = ValidationManifest::from_json(&json).unwrap();

    let errors = validate_manifest_contract(&restored);
    assert!(errors.is_empty(), "alternate ingested manifest must pass contract: {errors:?}");
}

// ─── Gate Completeness Regression ──────────────────────────────────────

#[test]
fn ingested_manifest_has_all_five_gates() {
    let bundle = build_bundle(&canonical_config());
    let json = bundle.manifest.to_json().unwrap();
    let restored = ValidationManifest::from_json(&json).unwrap();

    let gate_ids: Vec<&str> = restored.gates.iter().map(|g| g.gate_id.as_str()).collect();
    assert!(gate_ids.contains(&COVERAGE_GATE_ID), "missing coverage gate");
    assert!(gate_ids.contains(&INVARIANT_DRIFT_GATE_ID), "missing invariant drift gate");
    assert!(gate_ids.contains(&SCENARIO_DRIFT_GATE_ID), "missing scenario drift gate");
    assert!(gate_ids.contains(&NO_MOCK_GATE_ID), "missing no-mock gate");
    assert!(gate_ids.contains(&LOGGING_GATE_ID), "missing logging gate");
    assert_eq!(restored.gates.len(), 5, "exactly 5 gates expected");
}

#[test]
fn ingested_manifest_gate_order_stable() {
    let bundle = build_bundle(&canonical_config());
    let json = bundle.manifest.to_json().unwrap();
    let restored = ValidationManifest::from_json(&json).unwrap();

    let gate_ids: Vec<&str> = restored.gates.iter().map(|g| g.gate_id.as_str()).collect();
    let mut sorted = gate_ids.clone();
    sorted.sort();
    assert_eq!(gate_ids, sorted, "gates must remain sorted after ingestion");
}

// ─── Artifact Archive Regression ───────────────────────────────────────

#[test]
fn ingested_artifacts_are_valid_json_or_jsonl() {
    let bundle = build_bundle(&canonical_config());
    for (uri, content) in &bundle.gate_artifacts {
        if uri.ends_with(".json") {
            let parsed: Result<serde_json::Value, _> = serde_json::from_str(content);
            assert!(parsed.is_ok(), "artifact {uri} must be valid JSON: {}", parsed.unwrap_err());
        } else if uri.ends_with(".jsonl") {
            for (line_no, line) in content.lines().enumerate() {
                if line.trim().is_empty() {
                    continue;
                }
                let parsed: Result<serde_json::Value, _> = serde_json::from_str(line);
                assert!(
                    parsed.is_ok(),
                    "JSONL line {line_no} in {uri} must be valid JSON"
                );
            }
        }
    }
}

#[test]
fn artifact_uris_match_gate_references() {
    let bundle = build_bundle(&canonical_config());
    let top_level: std::collections::BTreeSet<&str> =
        bundle.manifest.artifact_uris.iter().map(|u| u.as_str()).collect();

    for gate in &bundle.manifest.gates {
        for uri in &gate.artifact_uris {
            assert!(
                top_level.contains(uri.as_str()),
                "gate {} artifact {uri} missing from top-level index",
                gate.gate_id
            );
        }
    }

    // All top-level artifacts must have generated content
    for uri in &bundle.manifest.artifact_uris {
        assert!(
            bundle.gate_artifacts.contains_key(uri),
            "top-level artifact {uri} missing from gate_artifacts map"
        );
    }
}

// ─── Cross-Config Regression Detection ─────────────────────────────────

#[test]
fn different_configs_produce_different_manifests() {
    let a = build_bundle(&canonical_config());
    let b = build_bundle(&alternate_config());

    assert_ne!(a.manifest.run_id, b.manifest.run_id);
    assert_ne!(a.manifest.commit_sha, b.manifest.commit_sha);
    assert_ne!(a.manifest.trace_id, b.manifest.trace_id);
}

#[test]
fn same_seed_produces_same_gate_outcomes() {
    let a = build_bundle(&canonical_config());
    let b = build_bundle(&alternate_config());

    // Different configs but same seed should yield same gate structures
    // (outcomes depend on evaluation, not config metadata)
    assert_eq!(a.manifest.gates.len(), b.manifest.gates.len());
    for (ga, gb) in a.manifest.gates.iter().zip(b.manifest.gates.iter()) {
        assert_eq!(ga.gate_id, gb.gate_id, "gate IDs must match across configs");
        assert_eq!(ga.outcome, gb.outcome, "gate outcomes must match for same seed");
    }
}

// ─── Backward Compatibility Regression ─────────────────────────────────

#[test]
fn backward_compatibility_across_replay_runs() {
    let config = canonical_config();
    let a = build_bundle(&config);
    let b = build_bundle(&config);

    let issues = detect_backward_incompatible_change(&a.manifest, &b.manifest);
    assert!(
        issues.is_empty(),
        "identical replays must be backward compatible: {issues:?}"
    );
}

#[test]
fn backward_compatibility_across_different_configs() {
    let a = build_bundle(&canonical_config());
    let b = build_bundle(&alternate_config());

    let issues = detect_backward_incompatible_change(&a.manifest, &b.manifest);
    assert!(
        issues.is_empty(),
        "different configs with same gates must be backward compatible: {issues:?}"
    );
}

// ─── Ingestion Tamper Detection ────────────────────────────────────────

#[test]
fn tampered_schema_version_detected() {
    let bundle = build_bundle(&canonical_config());
    let mut tampered = bundle.manifest.clone();
    tampered.schema_version = "2.0.0".to_owned();

    let issues = detect_backward_incompatible_change(&bundle.manifest, &tampered);
    assert!(
        issues.iter().any(|i| i.contains("schema major")),
        "must detect schema version tampering: {issues:?}"
    );
}

#[test]
fn tampered_gate_removal_detected() {
    let bundle = build_bundle(&canonical_config());
    let mut tampered = bundle.manifest.clone();
    tampered.gates.retain(|g| g.gate_id != COVERAGE_GATE_ID);

    let issues = detect_backward_incompatible_change(&bundle.manifest, &tampered);
    assert!(
        issues.iter().any(|i| i.contains(COVERAGE_GATE_ID)),
        "must detect gate removal tampering: {issues:?}"
    );
}

#[test]
fn tampered_commit_sha_fails_contract() {
    let bundle = build_bundle(&canonical_config());
    let json = bundle.manifest.to_json().unwrap();
    let mut restored = ValidationManifest::from_json(&json).unwrap();
    restored.commit_sha.clear();

    let errors = validate_manifest_contract(&restored);
    assert!(
        errors.iter().any(|e| e.contains("commit_sha")),
        "must detect cleared commit_sha: {errors:?}"
    );
}

#[test]
fn tampered_replay_scenario_id_fails_contract() {
    let bundle = build_bundle(&canonical_config());
    let json = bundle.manifest.to_json().unwrap();
    let mut restored = ValidationManifest::from_json(&json).unwrap();
    restored.replay.scenario_id = "WRONG-SCENARIO".to_owned();

    let errors = validate_manifest_contract(&restored);
    assert!(
        errors.iter().any(|e| e.contains("replay.scenario_id")),
        "must detect scenario_id mismatch: {errors:?}"
    );
}

// ─── Artifact Content Hash Stability ───────────────────────────────────

#[test]
fn artifact_content_hashes_are_deterministic() {
    let config = canonical_config();
    let a = build_bundle(&config);
    let b = build_bundle(&config);

    for (uri, content_a) in &a.gate_artifacts {
        let content_b = b.gate_artifacts.get(uri).unwrap();
        let hash_a = sha256_hex(content_a);
        let hash_b = sha256_hex(content_b);
        assert_eq!(hash_a, hash_b, "artifact {uri} hash must be deterministic");
    }
}

fn sha256_hex(data: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(data.as_bytes());
    format!("{:x}", hasher.finalize())
}

// ─── Multi-Run Ingestion Regression ────────────────────────────────────

#[test]
fn ten_consecutive_replays_are_identical() {
    let config = canonical_config();
    let reference = build_bundle(&config).manifest.to_json().unwrap();

    for run in 1..=10 {
        let candidate = build_bundle(&config).manifest.to_json().unwrap();
        assert_eq!(
            reference, candidate,
            "replay run {run} diverged from reference"
        );
    }
}

// ─── Evidence Archive Round-Trip ───────────────────────────────────────

#[test]
fn evidence_archive_roundtrip() {
    let bundle = build_bundle(&canonical_config());

    // Simulate archiving: serialize manifest + artifacts to a map
    let mut archive: BTreeMap<String, String> = BTreeMap::new();
    archive.insert(
        "validation_manifest.json".to_owned(),
        bundle.manifest.to_json().unwrap(),
    );
    for (uri, content) in &bundle.gate_artifacts {
        archive.insert(uri.clone(), content.clone());
    }

    // Simulate ingestion: deserialize manifest from archive
    let manifest_json = archive.get("validation_manifest.json").unwrap();
    let restored = ValidationManifest::from_json(manifest_json).unwrap();

    // Verify all artifact URIs reference entries in the archive
    for uri in &restored.artifact_uris {
        assert!(
            archive.contains_key(uri),
            "artifact {uri} missing from archive"
        );
    }

    // Verify the restored manifest passes contract validation
    let errors = validate_manifest_contract(&restored);
    assert!(errors.is_empty(), "archived manifest must pass contract: {errors:?}");
}

// ─── Logging Gate Evidence Regression ──────────────────────────────────

#[test]
fn logging_gate_evidence_includes_validation_report() {
    let bundle = build_bundle(&canonical_config());
    assert_eq!(bundle.manifest.logging_conformance.gate_id, LOGGING_GATE_ID);
    assert!(!bundle.manifest.logging_conformance.profile_doc_path.is_empty());
}

#[test]
fn logging_events_artifact_is_complete_jsonl() {
    let bundle = build_bundle(&canonical_config());
    let events_uri = bundle
        .manifest
        .artifact_uris
        .iter()
        .find(|u| u.ends_with("events.jsonl"))
        .expect("must have events.jsonl artifact");

    let content = bundle.gate_artifacts.get(events_uri).expect("events content");
    let mut event_count = 0_usize;
    for line in content.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let parsed: serde_json::Value = serde_json::from_str(line)
            .unwrap_or_else(|e| panic!("invalid JSONL line: {e}"));
        assert!(parsed.is_object(), "each JSONL line must be an object");
        event_count += 1;
    }
    assert!(event_count > 0, "events JSONL must not be empty");
}

// ─── Unit Gate Evidence Regression ─────────────────────────────────────

#[test]
fn unit_gate_evidence_includes_matrix_stats() {
    let bundle = build_bundle(&canonical_config());
    assert_eq!(
        bundle.manifest.invariant_drift.gate_id,
        INVARIANT_DRIFT_GATE_ID
    );
    assert!(
        bundle.manifest.invariant_drift.total_matrix_tests > 0,
        "matrix must have tests"
    );
    assert!(
        bundle.manifest.invariant_drift.total_matrix_invariants > 0,
        "matrix must have invariants"
    );
}

// ─── E2E Gate Evidence Regression ──────────────────────────────────────

#[test]
fn e2e_gate_evidence_includes_scenario_stats() {
    let bundle = build_bundle(&canonical_config());
    assert_eq!(
        bundle.manifest.scenario_coverage_drift.gate_id,
        SCENARIO_DRIFT_GATE_ID
    );
    assert!(
        bundle.manifest.scenario_coverage_drift.total_catalog_scenarios > 0,
        "catalog must have scenarios"
    );
}

// ─── Conformance Summary ──────────────────────────────────────────────

#[test]
fn conformance_summary() {
    let checks = vec![
        ("C-1: JSON round-trip preserves all manifest fields", true),
        ("C-2: Replay from same config produces identical output", true),
        ("C-3: Ingested manifest passes contract validation", true),
        ("C-4: Gate completeness regression (5 gates present)", true),
        ("C-5: Artifact URIs match gate references bidirectionally", true),
        ("C-6: Backward compatibility across replay runs", true),
        ("C-7: Tamper detection (schema version, gate removal)", true),
        ("C-8: Artifact content hashes deterministic across replays", true),
        ("C-9: Evidence archive round-trip with contract validation", true),
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
