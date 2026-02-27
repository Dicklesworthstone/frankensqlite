//! Contract tests for parity_release_threshold_policy.toml (bd-2yqp6.7.1).
//!
//! Enforces strict 100% release thresholds and deterministic policy signature
//! verification for release-gating defaults.

use std::fs;
use std::path::{Path, PathBuf};

use fsqlite_harness::confidence_gates::GateConfig;
use fsqlite_harness::ratchet_policy::RatchetPolicy;
use fsqlite_harness::score_engine::ScoreEngineConfig;
use serde::Deserialize;
use sha2::{Digest, Sha256};

const BEAD_ID: &str = "bd-2yqp6.7.1";

#[derive(Debug, Deserialize)]
struct ThresholdPolicyDocument {
    meta: PolicyMeta,
    thresholds: ThresholdPolicy,
    evidence: EvidencePolicy,
    signature: PolicySignature,
    references: PolicyReferences,
}

#[derive(Debug, Deserialize)]
struct PolicyMeta {
    schema_version: String,
    policy_version: String,
    bead_id: String,
    track_id: String,
    generated_at: String,
    policy_owner: String,
}

#[derive(Debug, Deserialize)]
struct ThresholdPolicy {
    declared_surface_parity_min: f64,
    required_suite_pass_rate_min: f64,
    score_engine_release_threshold: f64,
    confidence_gate_release_threshold: f64,
    ratchet_minimum_release_threshold: f64,
    allow_threshold_downgrade: bool,
}

#[derive(Debug, Deserialize)]
struct EvidencePolicy {
    max_evidence_age_hours: u64,
    require_fresh_evidence_for_release: bool,
}

#[derive(Debug, Deserialize)]
struct PolicySignature {
    algorithm: String,
    canonical_payload: String,
    sha256: String,
}

#[derive(Debug, Deserialize)]
struct PolicyReferences {
    parity_score_contract: String,
    supported_surface_matrix: String,
    score_engine_module: String,
    confidence_gates_module: String,
    ratchet_policy_module: String,
}

#[derive(Debug, Deserialize)]
struct ParityScoreContractDocument {
    hundred_percent: HundredPercentPolicy,
}

#[derive(Debug, Deserialize)]
struct HundredPercentPolicy {
    required_score: f64,
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn read_text(path: &Path) -> String {
    fs::read_to_string(path).unwrap_or_else(|error| {
        panic!("failed to read {}: {error}", path.display());
    })
}

fn load_threshold_policy() -> ThresholdPolicyDocument {
    let path = workspace_root().join("parity_release_threshold_policy.toml");
    toml::from_str(&read_text(&path)).unwrap_or_else(|error| {
        panic!("failed to parse {}: {error}", path.display());
    })
}

fn canonical_payload(policy: &ThresholdPolicyDocument) -> String {
    let thresholds = &policy.thresholds;
    let evidence = &policy.evidence;
    format!(
        "policy_version={}|declared_surface_parity_min={:.6}|required_suite_pass_rate_min={:.6}|score_engine_release_threshold={:.6}|confidence_gate_release_threshold={:.6}|ratchet_minimum_release_threshold={:.6}|allow_threshold_downgrade={}|max_evidence_age_hours={}|require_fresh_evidence_for_release={}",
        policy.meta.policy_version,
        thresholds.declared_surface_parity_min,
        thresholds.required_suite_pass_rate_min,
        thresholds.score_engine_release_threshold,
        thresholds.confidence_gate_release_threshold,
        thresholds.ratchet_minimum_release_threshold,
        thresholds.allow_threshold_downgrade,
        evidence.max_evidence_age_hours,
        evidence.require_fresh_evidence_for_release
    )
}

fn sha256_hex(input: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    let digest = hasher.finalize();
    format!("{digest:x}")
}

#[test]
fn policy_meta_and_thresholds_are_strict() {
    let policy = load_threshold_policy();

    assert_eq!(policy.meta.schema_version, "1.0.0");
    assert_eq!(policy.meta.policy_version, "strict-100.v1");
    assert_eq!(policy.meta.bead_id, BEAD_ID);
    assert_eq!(policy.meta.track_id, "bd-2yqp6.7");
    assert!(!policy.meta.generated_at.trim().is_empty());
    assert!(!policy.meta.policy_owner.trim().is_empty());

    assert!((policy.thresholds.declared_surface_parity_min - 1.0).abs() < f64::EPSILON);
    assert!((policy.thresholds.required_suite_pass_rate_min - 1.0).abs() < f64::EPSILON);
    assert!((policy.thresholds.score_engine_release_threshold - 1.0).abs() < f64::EPSILON);
    assert!((policy.thresholds.confidence_gate_release_threshold - 1.0).abs() < f64::EPSILON);
    assert!((policy.thresholds.ratchet_minimum_release_threshold - 1.0).abs() < f64::EPSILON);
    assert!(!policy.thresholds.allow_threshold_downgrade);

    assert!(policy.evidence.max_evidence_age_hours > 0);
    assert!(policy.evidence.require_fresh_evidence_for_release);
}

#[test]
fn policy_signature_matches_canonical_payload() {
    let policy = load_threshold_policy();

    assert_eq!(policy.signature.algorithm, "sha256");
    let canonical = canonical_payload(&policy);
    assert_eq!(policy.signature.canonical_payload, canonical);
    assert_eq!(policy.signature.sha256, sha256_hex(&canonical));
}

#[test]
fn defaults_match_policy_thresholds() {
    let policy = load_threshold_policy();

    let score_default = ScoreEngineConfig::default().release_threshold;
    let gate_default = GateConfig::default().release_threshold;
    let ratchet_default = RatchetPolicy::default().minimum_release_threshold;
    let ratchet_strict = RatchetPolicy::strict().minimum_release_threshold;
    let ratchet_relaxed = RatchetPolicy::relaxed().minimum_release_threshold;

    assert!(
        (score_default - policy.thresholds.score_engine_release_threshold).abs() < f64::EPSILON
    );
    assert!(
        (gate_default - policy.thresholds.confidence_gate_release_threshold).abs() < f64::EPSILON
    );
    assert!(
        (ratchet_default - policy.thresholds.ratchet_minimum_release_threshold).abs()
            < f64::EPSILON
    );
    assert!(
        (ratchet_strict - policy.thresholds.ratchet_minimum_release_threshold).abs() < f64::EPSILON
    );
    assert!(
        (ratchet_relaxed - policy.thresholds.ratchet_minimum_release_threshold).abs()
            < f64::EPSILON
    );
}

#[test]
fn policy_references_exist_and_align_with_parity_contract() {
    let policy = load_threshold_policy();
    let root = workspace_root();

    for rel in [
        policy.references.parity_score_contract.as_str(),
        policy.references.supported_surface_matrix.as_str(),
        policy.references.score_engine_module.as_str(),
        policy.references.confidence_gates_module.as_str(),
        policy.references.ratchet_policy_module.as_str(),
    ] {
        let path = root.join(rel);
        assert!(
            path.exists(),
            "referenced file does not exist: {}",
            path.display()
        );
    }

    let contract_path = root.join(&policy.references.parity_score_contract);
    let parity_contract: ParityScoreContractDocument = toml::from_str(&read_text(&contract_path))
        .unwrap_or_else(|error| {
            panic!("failed to parse {}: {error}", contract_path.display());
        });
    assert!(
        (parity_contract.hundred_percent.required_score
            - policy.thresholds.declared_surface_parity_min)
            .abs()
            < f64::EPSILON
    );
}
