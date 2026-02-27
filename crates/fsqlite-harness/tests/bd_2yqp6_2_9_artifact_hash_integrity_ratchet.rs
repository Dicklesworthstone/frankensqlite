//! Contract tests for B9 artifact bundle hash stability + integrity ratchet.
//!
//! Bead: bd-2yqp6.2.9

use fsqlite_harness::validation_manifest::{
    ARTIFACT_HASH_RATCHET_BASELINE_SCHEMA_VERSION, ARTIFACT_HASH_RATCHET_BEAD_ID,
    ValidationManifestConfig, build_validation_manifest_bundle, evaluate_artifact_hash_ratchet,
    validate_artifact_bundle_integrity_report,
};
use proptest::prelude::{prop_assert_eq, proptest};

const BEAD_ID: &str = "bd-2yqp6.2.9";

fn deterministic_config() -> ValidationManifestConfig {
    ValidationManifestConfig {
        commit_sha: "ratchet-sha-01234567".to_owned(),
        run_id: format!("{BEAD_ID}-hash-integrity"),
        trace_id: "trace-b9-integrity".to_owned(),
        scenario_id: "ARTIFACT-HASH-RATCHET-B9".to_owned(),
        generated_unix_ms: 1_730_000_000_000,
        root_seed: Some(9_209),
        artifact_uri_prefix: "artifacts/bd-2yqp6.2.9".to_owned(),
    }
}

fn build_bundle() -> fsqlite_harness::validation_manifest::ValidationManifestBundle {
    build_validation_manifest_bundle(&deterministic_config())
        .unwrap_or_else(|error| panic!("bundle build failed: {error}"))
}

#[test]
fn integrity_report_is_deterministic_for_identical_inputs() {
    let a = build_bundle();
    let b = build_bundle();

    assert_eq!(a.artifact_bundle_integrity, b.artifact_bundle_integrity);
    assert_eq!(
        a.artifact_bundle_integrity.bundle_hash,
        b.artifact_bundle_integrity.bundle_hash
    );
    assert_eq!(
        a.artifact_bundle_integrity.canonical_payload,
        b.artifact_bundle_integrity.canonical_payload
    );
}

#[test]
fn integrity_validation_detects_tampered_payload() {
    let bundle = build_bundle();
    let mut tampered = bundle.gate_artifacts.clone();
    let first_uri = bundle
        .manifest
        .artifact_uris
        .first()
        .expect("manifest must contain artifacts")
        .clone();
    let original = tampered
        .get(&first_uri)
        .expect("artifact payload must exist")
        .clone();
    tampered.insert(first_uri.clone(), format!("{original}\n# tampered"));

    let errors = validate_artifact_bundle_integrity_report(
        &bundle.manifest,
        &tampered,
        &bundle.artifact_bundle_integrity,
    );
    assert!(
        errors
            .iter()
            .any(|error| error.contains("artifact payload hash mismatch")),
        "expected tamper mismatch error, got: {errors:?}"
    );
}

#[test]
fn integrity_validation_detects_omitted_artifact_payload() {
    let bundle = build_bundle();
    let mut missing = bundle.gate_artifacts.clone();
    let first_uri = bundle
        .manifest
        .artifact_uris
        .first()
        .expect("manifest must contain artifacts")
        .clone();
    missing.remove(&first_uri);

    let errors = validate_artifact_bundle_integrity_report(
        &bundle.manifest,
        &missing,
        &bundle.artifact_bundle_integrity,
    );
    assert!(
        errors
            .iter()
            .any(|error| error.contains("artifact payload missing")),
        "expected omission error, got: {errors:?}"
    );
}

#[test]
fn integrity_validation_rejects_unsorted_integrity_entries() {
    let bundle = build_bundle();
    let mut report = bundle.artifact_bundle_integrity.clone();
    if report.artifact_payload_hashes.len() < 2 {
        panic!("expected at least two integrity hash entries");
    }
    report.artifact_payload_hashes.swap(0, 1);

    let errors = validate_artifact_bundle_integrity_report(
        &bundle.manifest,
        &bundle.gate_artifacts,
        &report,
    );
    assert!(
        errors
            .iter()
            .any(|error| error.contains("artifact_payload_hashes must be sorted")),
        "expected ordering error, got: {errors:?}"
    );
}

#[test]
fn ratchet_blocks_hash_drift_without_review_reason() {
    let baseline = fsqlite_harness::validation_manifest::ArtifactHashRatchetBaseline {
        schema_version: ARTIFACT_HASH_RATCHET_BASELINE_SCHEMA_VERSION.to_owned(),
        bead_id: ARTIFACT_HASH_RATCHET_BEAD_ID.to_owned(),
        scenario_id: "ARTIFACT-HASH-RATCHET-B9".to_owned(),
        bundle_hash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
        updated_unix_ms: 1_730_000_000_000,
        update_reason: "initial approved baseline".to_owned(),
    };

    let decision = evaluate_artifact_hash_ratchet(
        Some(&baseline),
        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        None,
        false,
    );
    assert!(!decision.approved);
    assert!(decision.requires_baseline_update);
    assert_eq!(decision.reason, "hash_drift_without_review_reason");
}

#[test]
fn ratchet_allows_hash_drift_with_review_reason() {
    let baseline = fsqlite_harness::validation_manifest::ArtifactHashRatchetBaseline {
        schema_version: ARTIFACT_HASH_RATCHET_BASELINE_SCHEMA_VERSION.to_owned(),
        bead_id: ARTIFACT_HASH_RATCHET_BEAD_ID.to_owned(),
        scenario_id: "ARTIFACT-HASH-RATCHET-B9".to_owned(),
        bundle_hash: "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc".to_owned(),
        updated_unix_ms: 1_730_000_000_000,
        update_reason: "initial approved baseline".to_owned(),
    };

    let decision = evaluate_artifact_hash_ratchet(
        Some(&baseline),
        "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
        Some("approved update after schema review"),
        false,
    );
    assert!(decision.approved);
    assert!(decision.requires_baseline_update);
    assert_eq!(decision.reason, "hash_drift_reviewed");
}

#[test]
fn ratchet_bootstrap_requires_allow_flag_and_reason() {
    let blocked = evaluate_artifact_hash_ratchet(
        None,
        "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
        Some("bootstrap baseline"),
        false,
    );
    assert!(!blocked.approved);
    assert_eq!(blocked.reason, "baseline_missing_bootstrap_disallowed");

    let allowed = evaluate_artifact_hash_ratchet(
        None,
        "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
        Some("bootstrap baseline"),
        true,
    );
    assert!(allowed.approved);
    assert!(allowed.requires_baseline_update);
    assert_eq!(allowed.reason, "baseline_bootstrap_reviewed");
}

proptest! {
    #[test]
    fn invalid_candidate_hashes_are_rejected(candidate in "[^a-f0-9]{1,80}") {
        let decision = evaluate_artifact_hash_ratchet(None, &candidate, Some("reason"), true);
        prop_assert_eq!(decision.approved, false);
        prop_assert_eq!(decision.reason, "candidate_hash_invalid_format");
    }
}
