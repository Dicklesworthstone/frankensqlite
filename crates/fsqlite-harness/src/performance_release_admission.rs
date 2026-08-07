//! Typed, provenance-bound performance release-admission evidence.
//!
//! This module deliberately does not encode an acceptance margin.  The policy
//! that owns those values is an immutable input whose bytes are hash-bound by
//! an admission pack.  A missing policy is therefore a visible blocker, not a
//! reason to invent a threshold in a verifier.

use std::{
    collections::BTreeSet,
    fs,
    path::{Component, Path, PathBuf},
    process::Command,
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const ADMISSION_GATE_SCHEMA_V2: &str = "fsqlite.performance_release_admission.v2";
pub const ADMISSION_PACK_SCHEMA_V2: &str = "fsqlite.performance_release_admission_pack.v2";
const SHA256_ALGORITHM: &str = "sha2-256";
const LEGACY_REPORT_SCHEMA_V9: &str = "fsqlite-e2e.mt_mvcc_bench_report.v9";
const POLICY_BLOCKER: &str = "missing_authoritative_performance_policy";
const POLICY_SCHEMA_V1: &str = "fsqlite.performance_admission_policy.v1";
const MEASUREMENT_REPORT_SCHEMA_V1: &str = "fsqlite.performance_admission_measurement.v1";
const MEASUREMENT_MANIFEST_SCHEMA_V1: &str =
    "fsqlite.performance_admission_measurement_manifest.v1";
const CALIBRATION_RECEIPT_SCHEMA_V1: &str = "fsqlite.performance_admission_calibration_receipt.v1";
const SENSITIVITY_RECEIPT_SCHEMA_V1: &str = "fsqlite.performance_admission_sensitivity_receipt.v1";

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PerformanceAdmissionGate {
    pub schema_version: String,
    pub status: String,
    pub release_authorized: bool,
    pub blockers: Vec<String>,
    pub rationale: String,
    pub admission_pack: Option<AdmissionPackReference>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AdmissionPackReference {
    pub path: String,
    pub sha256: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PerformanceAdmissionPack {
    pub schema_version: String,
    pub baseline: CandidateProvenance,
    pub tested: CandidateProvenance,
    pub policy: ArtifactDigest,
    pub calibration_receipt: ArtifactDigest,
    pub sensitivity_receipt: ArtifactDigest,
    pub predicates: AdmissionPredicates,
    /// Test-only fixtures must declare themselves. Production admission rejects
    /// them even when every structural predicate otherwise passes.
    pub synthetic_fixture: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CandidateProvenance {
    pub source_commit: String,
    pub host_fingerprint_sha256: String,
    pub toolchain_sha256: String,
    pub profiles: Vec<ProfileEvidence>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ProfileEvidence {
    pub profile: String,
    pub report_schema: String,
    pub raw_report: ArtifactDigest,
    pub raw_manifest: ArtifactDigest,
    pub feature_graph_sha256: String,
    pub binary_nonce: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ArtifactDigest {
    pub path: String,
    pub digest_algorithm: String,
    pub sha256: String,
}

/// The report is deliberately provenance-only: the policy artifact, rather
/// than this verifier, owns any acceptance margin or numerical decision.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MeasurementReport {
    schema_version: String,
    source_commit: String,
    profile: String,
    host_fingerprint_sha256: String,
    toolchain_sha256: String,
    feature_graph_sha256: String,
    binary_nonce: String,
    policy_id: String,
    policy_version: String,
    policy_sha256: String,
    raw_manifest_path: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MeasurementManifest {
    schema_version: String,
    source_commit: String,
    profile: String,
    host_fingerprint_sha256: String,
    toolchain_sha256: String,
    feature_graph_sha256: String,
    binary_nonce: String,
    policy_id: String,
    policy_version: String,
    policy_sha256: String,
    raw_report_sha256: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AdmissionPolicy {
    schema_version: String,
    policy_id: String,
    policy_version: String,
    required_profiles: Vec<String>,
    required_workloads: Vec<String>,
    metric_rules: Vec<MetricRule>,
    counterbalance_order: Vec<String>,
    calibration_noise_multiplier: f64,
    sensitivity_injected_slowdown_minimum: f64,
    sensitivity_detection_required: bool,
    no_waiver: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MetricRule {
    metric: String,
    direction: String,
    max_regression_fraction: f64,
    confidence_level: f64,
    minimum_samples: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AdmissionReceipt {
    schema_version: String,
    baseline_source_commit: String,
    tested_source_commit: String,
    policy_sha256: String,
    policy_id: String,
    policy_version: String,
    manifest_bindings: Vec<ReceiptManifestBinding>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReceiptManifestBinding {
    side: String,
    profile: String,
    manifest_sha256: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[allow(clippy::struct_excessive_bools)]
pub struct AdmissionPredicates {
    pub source_provenance: bool,
    pub strict_ancestry: bool,
    pub policy_hash: bool,
    pub raw_evidence_hashes: bool,
    pub environment_binding: bool,
    pub calibration_receipt: bool,
    pub sensitivity_receipt: bool,
}

impl AdmissionPredicates {
    fn all_pass(&self) -> bool {
        self.source_provenance
            && self.strict_ancestry
            && self.policy_hash
            && self.raw_evidence_hashes
            && self.environment_binding
            && self.calibration_receipt
            && self.sensitivity_receipt
    }
}

pub fn blocked_missing_authoritative_policy() -> PerformanceAdmissionGate {
    PerformanceAdmissionGate {
        schema_version: ADMISSION_GATE_SCHEMA_V2.to_owned(),
        status: "blocked_missing_authoritative_performance_policy".to_owned(),
        release_authorized: false,
        blockers: vec![POLICY_BLOCKER.to_owned()],
        rationale: "No authoritative acceptance policy artifact is available. The v2 admission contract intentionally carries no local acceptance margin; a policy hash, immutable B/T provenance, raw evidence hashes, calibration, and sensitivity receipts are required before authorization.".to_owned(),
        admission_pack: None,
    }
}

pub fn validate_gate(
    workspace_root: &Path,
    tested_commit: &str,
    gate: &PerformanceAdmissionGate,
) -> Result<(), String> {
    if gate.schema_version != ADMISSION_GATE_SCHEMA_V2 {
        return Err("performance admission gate must use v2 schema".to_owned());
    }
    match (gate.release_authorized, gate.status.as_str(), &gate.admission_pack) {
        (false, "blocked_missing_authoritative_performance_policy", None) => {
            if gate.blockers.as_slice() != [POLICY_BLOCKER] || gate.rationale.trim().is_empty() {
                return Err("missing-policy admission blocker is malformed".to_owned());
            }
            Ok(())
        }
        (true, "authorized", Some(reference)) if gate.blockers.is_empty() => {
            let pack_path = checked_path(workspace_root, &reference.path, "admission pack")?;
            let pack_bytes = read_regular(&pack_path, "admission pack")?;
            if sha256(&pack_bytes) != reference.sha256 || !is_lower_hex(&reference.sha256, 64) {
                return Err("admission pack SHA-256 does not bind its bytes".to_owned());
            }
            let pack: PerformanceAdmissionPack = serde_json::from_slice(&pack_bytes)
                .map_err(|error| format!("invalid v2 admission pack: {error}"))?;
            validate_pack(workspace_root, tested_commit, &pack, false)
        }
        (false, _, Some(_)) => Err(
            "a supplied admission pack must not be silently downgraded; emit its validated decision"
                .to_owned(),
        ),
        _ => Err("performance admission gate authorization shape is invalid".to_owned()),
    }
}

pub fn validate_pack(
    workspace_root: &Path,
    expected_tested_commit: &str,
    pack: &PerformanceAdmissionPack,
    allow_synthetic_fixture: bool,
) -> Result<(), String> {
    if pack.schema_version != ADMISSION_PACK_SCHEMA_V2 {
        return Err("performance admission pack must use v2 schema".to_owned());
    }
    if pack.synthetic_fixture && !allow_synthetic_fixture {
        return Err("synthetic performance fixtures can never authorize release".to_owned());
    }
    if !pack.predicates.all_pass() {
        return Err("every typed performance-admission predicate must pass".to_owned());
    }
    validate_commit(&pack.baseline.source_commit, "baseline source commit")?;
    validate_commit(&pack.tested.source_commit, "tested source commit")?;
    if pack.tested.source_commit != expected_tested_commit {
        return Err(
            "admission pack tested source commit does not match frozen candidate".to_owned(),
        );
    }
    if pack.baseline.source_commit == pack.tested.source_commit {
        return Err("baseline and tested source commits must be distinct".to_owned());
    }
    require_ancestor(
        workspace_root,
        &pack.baseline.source_commit,
        &pack.tested.source_commit,
    )?;
    validate_candidate(&pack.baseline, "baseline")?;
    validate_candidate(&pack.tested, "tested")?;
    if pack.baseline.host_fingerprint_sha256 != pack.tested.host_fingerprint_sha256 {
        return Err("baseline and tested host fingerprints must match".to_owned());
    }
    if pack.baseline.toolchain_sha256 != pack.tested.toolchain_sha256 {
        return Err("baseline and tested toolchain hashes must match".to_owned());
    }
    for profile in ["release", "release-perf"] {
        let baseline = profile_by_name(&pack.baseline, profile)?;
        let tested = profile_by_name(&pack.tested, profile)?;
        if baseline.feature_graph_sha256 != tested.feature_graph_sha256 {
            return Err(format!(
                "{profile} feature graph hashes must match across B/T"
            ));
        }
        if baseline.binary_nonce == tested.binary_nonce {
            return Err(format!(
                "{profile} baseline/tested binary nonces must differ"
            ));
        }
    }
    let mut nonces = BTreeSet::new();
    for candidate in [&pack.baseline, &pack.tested] {
        for profile in &candidate.profiles {
            if !nonces.insert(profile.binary_nonce.as_str()) {
                return Err(
                    "binary nonce reuse is forbidden across the B/T admission pack".to_owned(),
                );
            }
        }
    }
    let policy = validate_policy(workspace_root, &pack.policy)?;
    for candidate in [&pack.baseline, &pack.tested] {
        for profile in &candidate.profiles {
            validate_profile_artifacts(
                workspace_root,
                candidate,
                profile,
                if candidate.source_commit == pack.baseline.source_commit {
                    "baseline"
                } else {
                    "tested"
                },
                &policy,
                &pack.policy.sha256,
            )?;
        }
    }
    validate_receipt(
        workspace_root,
        &pack.calibration_receipt,
        CALIBRATION_RECEIPT_SCHEMA_V1,
        pack,
        &policy,
        "calibration receipt",
    )?;
    validate_receipt(
        workspace_root,
        &pack.sensitivity_receipt,
        SENSITIVITY_RECEIPT_SCHEMA_V1,
        pack,
        &policy,
        "sensitivity receipt",
    )?;
    Ok(())
}

fn validate_profile_artifacts(
    workspace_root: &Path,
    candidate: &CandidateProvenance,
    profile: &ProfileEvidence,
    side: &str,
    policy: &AdmissionPolicy,
    policy_sha256: &str,
) -> Result<(), String> {
    let report_bytes = artifact_bytes(
        workspace_root,
        &profile.raw_report,
        "raw measurement report",
    )?;
    let manifest_bytes = artifact_bytes(
        workspace_root,
        &profile.raw_manifest,
        "raw measurement manifest",
    )?;
    let report: MeasurementReport = serde_json::from_slice(&report_bytes).map_err(|error| {
        format!(
            "{side} {} report is not a typed v2 measurement report: {error}",
            profile.profile
        )
    })?;
    let manifest: MeasurementManifest =
        serde_json::from_slice(&manifest_bytes).map_err(|error| {
            format!(
                "{side} {} manifest is not a typed v2 measurement manifest: {error}",
                profile.profile
            )
        })?;
    if report.schema_version != profile.report_schema
        || manifest.schema_version != MEASUREMENT_MANIFEST_SCHEMA_V1
    {
        return Err(format!(
            "{side} {} report/manifest schemas do not match the v2 pack",
            profile.profile
        ));
    }
    for (field, report_value, manifest_value, expected) in [
        (
            "source commit",
            &report.source_commit,
            &manifest.source_commit,
            &candidate.source_commit,
        ),
        (
            "profile",
            &report.profile,
            &manifest.profile,
            &profile.profile,
        ),
        (
            "host fingerprint",
            &report.host_fingerprint_sha256,
            &manifest.host_fingerprint_sha256,
            &candidate.host_fingerprint_sha256,
        ),
        (
            "toolchain hash",
            &report.toolchain_sha256,
            &manifest.toolchain_sha256,
            &candidate.toolchain_sha256,
        ),
        (
            "feature graph hash",
            &report.feature_graph_sha256,
            &manifest.feature_graph_sha256,
            &profile.feature_graph_sha256,
        ),
        (
            "binary nonce",
            &report.binary_nonce,
            &manifest.binary_nonce,
            &profile.binary_nonce,
        ),
    ] {
        if report_value != expected || manifest_value != expected {
            return Err(format!(
                "{side} {} {field} is not bound to the v2 pack",
                profile.profile
            ));
        }
    }
    for (field, report_value, manifest_value, expected) in [
        (
            "policy id",
            report.policy_id.as_str(),
            manifest.policy_id.as_str(),
            policy.policy_id.as_str(),
        ),
        (
            "policy version",
            report.policy_version.as_str(),
            manifest.policy_version.as_str(),
            policy.policy_version.as_str(),
        ),
        (
            "policy SHA-256",
            report.policy_sha256.as_str(),
            manifest.policy_sha256.as_str(),
            policy_sha256,
        ),
    ] {
        if report_value != expected || manifest_value != expected {
            return Err(format!(
                "{side} {} {field} is not bound to the typed policy",
                profile.profile
            ));
        }
    }
    if report.raw_manifest_path != profile.raw_manifest.path
        || manifest.raw_report_sha256 != profile.raw_report.sha256
    {
        return Err(format!(
            "{side} {} report/manifest hashes are not mutually bound",
            profile.profile
        ));
    }
    Ok(())
}

fn validate_receipt(
    workspace_root: &Path,
    artifact: &ArtifactDigest,
    expected_schema: &str,
    pack: &PerformanceAdmissionPack,
    policy: &AdmissionPolicy,
    label: &str,
) -> Result<(), String> {
    let bytes = artifact_bytes(workspace_root, artifact, label)?;
    let receipt: AdmissionReceipt = serde_json::from_slice(&bytes)
        .map_err(|error| format!("{label} is not a typed v2 receipt: {error}"))?;
    if receipt.schema_version != expected_schema
        || receipt.baseline_source_commit != pack.baseline.source_commit
        || receipt.tested_source_commit != pack.tested.source_commit
        || receipt.policy_sha256 != pack.policy.sha256
        || receipt.policy_id != policy.policy_id
        || receipt.policy_version != policy.policy_version
    {
        return Err(format!(
            "{label} is not bound to the B/T source commits and policy"
        ));
    }
    let expected = expected_receipt_manifest_bindings(pack);
    if receipt.manifest_bindings.len() != expected.len() {
        return Err(format!(
            "{label} manifest provenance bindings must be unique and complete"
        ));
    }
    let actual = receipt
        .manifest_bindings
        .into_iter()
        .map(|binding| (binding.side, binding.profile, binding.manifest_sha256))
        .collect::<BTreeSet<_>>();
    if actual != expected {
        return Err(format!(
            "{label} manifest provenance bindings do not exactly cover B/T profiles"
        ));
    }
    Ok(())
}

fn validate_policy(
    workspace_root: &Path,
    artifact: &ArtifactDigest,
) -> Result<AdmissionPolicy, String> {
    let bytes = artifact_bytes(workspace_root, artifact, "policy")?;
    let policy: AdmissionPolicy = serde_json::from_slice(&bytes)
        .map_err(|error| format!("policy is not a typed v2 production policy: {error}"))?;
    if policy.schema_version != POLICY_SCHEMA_V1
        || policy.policy_id.trim().is_empty()
        || policy.policy_version.trim().is_empty()
    {
        return Err("policy schema, id, and version are required for v2 authorization".to_owned());
    }
    if policy
        .required_profiles
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>()
        != ["release", "release-perf"]
        || !unique_nonempty(&policy.required_workloads)
        || !unique_nonempty(&policy.counterbalance_order)
        || policy.counterbalance_order.len() != 2
        || !policy
            .counterbalance_order
            .iter()
            .any(|order| order == "baseline_first")
        || !policy
            .counterbalance_order
            .iter()
            .any(|order| order == "tested_first")
        || !policy.calibration_noise_multiplier.is_finite()
        || policy.calibration_noise_multiplier <= 0.0
        || !policy.sensitivity_injected_slowdown_minimum.is_finite()
        || !(0.0..=1.0).contains(&policy.sensitivity_injected_slowdown_minimum)
        || policy.sensitivity_injected_slowdown_minimum <= 0.0
        || !policy.sensitivity_detection_required
        || !policy.no_waiver
    {
        return Err("policy acceptance-rule fields are incomplete or out of range".to_owned());
    }
    if policy.metric_rules.is_empty()
        || !policy.metric_rules.iter().all(valid_metric_rule)
        || policy
            .metric_rules
            .iter()
            .map(|rule| rule.metric.as_str())
            .collect::<BTreeSet<_>>()
            .len()
            != policy.metric_rules.len()
    {
        return Err("policy metric rules must be unique, typed, and in range".to_owned());
    }
    Ok(policy)
}

fn unique_nonempty(values: &[String]) -> bool {
    !values.is_empty()
        && values.iter().all(|value| !value.trim().is_empty())
        && values.iter().collect::<BTreeSet<_>>().len() == values.len()
}

fn valid_metric_rule(rule: &MetricRule) -> bool {
    !rule.metric.trim().is_empty()
        && matches!(
            rule.direction.as_str(),
            "higher_is_better" | "lower_is_better"
        )
        && rule.max_regression_fraction.is_finite()
        && (0.0..1.0).contains(&rule.max_regression_fraction)
        && rule.confidence_level.is_finite()
        && (0.0..1.0).contains(&rule.confidence_level)
        && rule.minimum_samples > 0
}

fn expected_receipt_manifest_bindings(
    pack: &PerformanceAdmissionPack,
) -> BTreeSet<(String, String, String)> {
    let mut bindings = BTreeSet::new();
    for (side, candidate) in [("baseline", &pack.baseline), ("tested", &pack.tested)] {
        for profile in &candidate.profiles {
            bindings.insert((
                side.to_owned(),
                profile.profile.clone(),
                profile.raw_manifest.sha256.clone(),
            ));
        }
    }
    bindings
}

fn validate_candidate(candidate: &CandidateProvenance, label: &str) -> Result<(), String> {
    for (name, value) in [
        ("host fingerprint", &candidate.host_fingerprint_sha256),
        ("toolchain hash", &candidate.toolchain_sha256),
    ] {
        if !is_lower_hex(value, 64) {
            return Err(format!("{label} {name} must be a lowercase SHA-256"));
        }
    }
    let profiles = candidate
        .profiles
        .iter()
        .map(|profile| profile.profile.as_str())
        .collect::<Vec<_>>();
    if profiles != ["release", "release-perf"] {
        return Err(format!(
            "{label} profiles must be exactly release then release-perf"
        ));
    }
    for profile in &candidate.profiles {
        if profile.report_schema == LEGACY_REPORT_SCHEMA_V9 {
            return Err("legacy v9 benchmark reports cannot authorize release".to_owned());
        }
        if profile.report_schema != MEASUREMENT_REPORT_SCHEMA_V1
            || !is_lower_hex(&profile.feature_graph_sha256, 64)
            || !is_lower_hex(&profile.binary_nonce, 64)
        {
            return Err(format!(
                "{label} profile provenance must use the typed v2 report schema"
            ));
        }
    }
    Ok(())
}

fn profile_by_name<'a>(
    candidate: &'a CandidateProvenance,
    expected: &str,
) -> Result<&'a ProfileEvidence, String> {
    candidate
        .profiles
        .iter()
        .find(|profile| profile.profile == expected)
        .ok_or_else(|| format!("missing required profile `{expected}`"))
}

fn artifact_bytes(
    workspace_root: &Path,
    artifact: &ArtifactDigest,
    label: &str,
) -> Result<Vec<u8>, String> {
    if artifact.digest_algorithm != SHA256_ALGORITHM || !is_lower_hex(&artifact.sha256, 64) {
        return Err(format!("{label} must carry a lowercase sha2-256 digest"));
    }
    let path = checked_path(workspace_root, &artifact.path, label)?;
    let bytes = read_regular(&path, label)?;
    if sha256(&bytes) != artifact.sha256 {
        return Err(format!("{label} digest does not match `{}`", artifact.path));
    }
    Ok(bytes)
}

/// Returns the exact immutable files that make an authorizing decision.
///
/// Phase 5 consumers use this inventory to reject a manifest that omits a pack
/// member or carries an unrelated extra member under authorization.
pub fn authorized_artifact_paths(
    workspace_root: &Path,
    tested_commit: &str,
    gate: &PerformanceAdmissionGate,
) -> Result<BTreeSet<String>, String> {
    validate_gate(workspace_root, tested_commit, gate)?;
    let Some(reference) = &gate.admission_pack else {
        return Ok(BTreeSet::new());
    };
    let pack_path = checked_path(workspace_root, &reference.path, "admission pack")?;
    let pack: PerformanceAdmissionPack =
        serde_json::from_slice(&read_regular(&pack_path, "admission pack")?)
            .map_err(|error| format!("invalid v2 admission pack: {error}"))?;
    let mut paths = BTreeSet::from([reference.path.clone(), pack.policy.path]);
    for artifact in [&pack.calibration_receipt, &pack.sensitivity_receipt] {
        paths.insert(artifact.path.clone());
    }
    for candidate in [&pack.baseline, &pack.tested] {
        for profile in &candidate.profiles {
            paths.insert(profile.raw_report.path.clone());
            paths.insert(profile.raw_manifest.path.clone());
        }
    }
    Ok(paths)
}

fn checked_path(workspace_root: &Path, raw: &str, label: &str) -> Result<PathBuf, String> {
    let relative = Path::new(raw);
    if raw.trim().is_empty()
        || relative.is_absolute()
        || relative.components().any(|component| {
            matches!(
                component,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        })
    {
        return Err(format!("{label} path must be a non-empty relative path"));
    }
    Ok(workspace_root.join(relative))
}

fn read_regular(path: &Path, label: &str) -> Result<Vec<u8>, String> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|error| format!("unable to inspect {label}: {error}"))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(format!("{label} must be a regular non-symlink file"));
    }
    fs::read(path).map_err(|error| format!("unable to read {label}: {error}"))
}

fn validate_commit(value: &str, label: &str) -> Result<(), String> {
    if is_lower_hex(value, 40) {
        Ok(())
    } else {
        Err(format!("{label} must be a full lowercase Git SHA"))
    }
}

fn require_ancestor(root: &Path, baseline: &str, tested: &str) -> Result<(), String> {
    let status = Command::new("git")
        .arg("-C")
        .arg(root)
        .args(["merge-base", "--is-ancestor", baseline, tested])
        .status()
        .map_err(|error| format!("unable to verify B/T ancestry: {error}"))?;
    if status.success() {
        Ok(())
    } else {
        Err("baseline source commit must be a strict ancestor of tested source commit".to_owned())
    }
}

fn is_lower_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn sha256(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    crate::bytes_to_lower_hex(hasher.finalize())
}

#[cfg(test)]
mod tests {
    use std::{fs, path::Path};

    use super::{
        ADMISSION_GATE_SCHEMA_V2, ADMISSION_PACK_SCHEMA_V2, AdmissionPackReference,
        AdmissionPredicates, ArtifactDigest, CandidateProvenance, PerformanceAdmissionGate,
        PerformanceAdmissionPack, ProfileEvidence, authorized_artifact_paths,
        blocked_missing_authoritative_policy, sha256, validate_candidate, validate_gate,
        validate_pack,
    };

    const TEST_POLICY_ID: &str = "test-only-structural-keeper-policy";
    const TEST_POLICY_VERSION: &str = "test-v1";

    fn git(root: &Path, arguments: &[&str]) {
        assert!(
            std::process::Command::new("git")
                .arg("-C")
                .arg(root)
                .args(arguments)
                .status()
                .expect("run git")
                .success()
        );
    }

    fn receipt(root: &Path, path: &str, bytes: &[u8]) -> ArtifactDigest {
        let target = root.join(path);
        fs::create_dir_all(target.parent().expect("parent")).expect("create receipt parent");
        fs::write(&target, bytes).expect("write receipt");
        ArtifactDigest {
            path: path.to_owned(),
            digest_algorithm: "sha2-256".to_owned(),
            sha256: sha256(bytes),
        }
    }

    fn profile(
        root: &Path,
        side: &str,
        profile: &str,
        nonce: char,
        source_commit: &str,
        policy_sha256: &str,
    ) -> ProfileEvidence {
        let report_path = format!("evidence/{side}/{profile}/report.json");
        let manifest_path = format!("evidence/{side}/{profile}/manifest.json");
        let report = serde_json::json!({
            "schema_version": "fsqlite.performance_admission_measurement.v1",
            "source_commit": source_commit,
            "profile": profile,
            "host_fingerprint_sha256": "a".repeat(64),
            "toolchain_sha256": "b".repeat(64),
            "feature_graph_sha256": "e".repeat(64),
            "binary_nonce": nonce.to_string().repeat(64),
            "policy_id": TEST_POLICY_ID,
            "policy_version": TEST_POLICY_VERSION,
            "policy_sha256": policy_sha256,
            "raw_manifest_path": manifest_path,
        });
        let raw_report = receipt(
            root,
            &report_path,
            &serde_json::to_vec(&report).expect("serialize report"),
        );
        let manifest = serde_json::json!({
            "schema_version": "fsqlite.performance_admission_measurement_manifest.v1",
            "source_commit": source_commit,
            "profile": profile,
            "host_fingerprint_sha256": "a".repeat(64),
            "toolchain_sha256": "b".repeat(64),
            "feature_graph_sha256": "e".repeat(64),
            "binary_nonce": nonce.to_string().repeat(64),
            "policy_id": TEST_POLICY_ID,
            "policy_version": TEST_POLICY_VERSION,
            "policy_sha256": policy_sha256,
            "raw_report_sha256": raw_report.sha256.clone(),
        });
        ProfileEvidence {
            profile: profile.to_owned(),
            report_schema: "fsqlite.performance_admission_measurement.v1".to_owned(),
            raw_report,
            raw_manifest: receipt(
                root,
                &manifest_path,
                &serde_json::to_vec(&manifest).expect("serialize manifest"),
            ),
            feature_graph_sha256: "e".repeat(64),
            binary_nonce: nonce.to_string().repeat(64),
        }
    }

    fn keeper(root: &Path, baseline: String, tested: String) -> PerformanceAdmissionPack {
        let policy = receipt(
            root,
            "evidence/policy.json",
            &serde_json::to_vec(&serde_json::json!({
                "schema_version": "fsqlite.performance_admission_policy.v1",
                "policy_id": TEST_POLICY_ID,
                "policy_version": TEST_POLICY_VERSION,
                "required_profiles": ["release", "release-perf"],
                "required_workloads": ["synthetic-keeper-workload"],
                "metric_rules": [{
                    "metric": "synthetic-throughput",
                    "direction": "higher_is_better",
                    "max_regression_fraction": 0.1,
                    "confidence_level": 0.95,
                    "minimum_samples": 8,
                }],
                "counterbalance_order": ["baseline_first", "tested_first"],
                "calibration_noise_multiplier": 1.5,
                "sensitivity_injected_slowdown_minimum": 0.05,
                "sensitivity_detection_required": true,
                "no_waiver": true,
            }))
            .expect("serialize typed policy"),
        );
        let baseline_provenance = CandidateProvenance {
            source_commit: baseline.clone(),
            host_fingerprint_sha256: "a".repeat(64),
            toolchain_sha256: "b".repeat(64),
            profiles: vec![
                profile(root, "baseline", "release", '1', &baseline, &policy.sha256),
                profile(
                    root,
                    "baseline",
                    "release-perf",
                    '2',
                    &baseline,
                    &policy.sha256,
                ),
            ],
        };
        let tested_provenance = CandidateProvenance {
            source_commit: tested.clone(),
            host_fingerprint_sha256: "a".repeat(64),
            toolchain_sha256: "b".repeat(64),
            profiles: vec![
                profile(root, "tested", "release", '3', &tested, &policy.sha256),
                profile(root, "tested", "release-perf", '4', &tested, &policy.sha256),
            ],
        };
        let manifest_bindings = [&baseline_provenance, &tested_provenance]
            .into_iter()
            .zip(["baseline", "tested"])
            .flat_map(|(candidate, side)| {
                candidate.profiles.iter().map(move |profile| {
                    serde_json::json!({
                        "side": side,
                        "profile": profile.profile.clone(),
                        "manifest_sha256": profile.raw_manifest.sha256.clone(),
                    })
                })
            })
            .collect::<Vec<_>>();
        PerformanceAdmissionPack {
            schema_version: ADMISSION_PACK_SCHEMA_V2.to_owned(),
            baseline: baseline_provenance,
            tested: tested_provenance,
            policy: policy.clone(),
            calibration_receipt: receipt(
                root,
                "evidence/calibration.json",
                &serde_json::to_vec(&serde_json::json!({
                    "schema_version": "fsqlite.performance_admission_calibration_receipt.v1",
                    "baseline_source_commit": baseline,
                    "tested_source_commit": tested,
                    "policy_sha256": policy.sha256,
                    "policy_id": TEST_POLICY_ID,
                    "policy_version": TEST_POLICY_VERSION,
                    "manifest_bindings": manifest_bindings,
                }))
                .expect("serialize calibration receipt"),
            ),
            sensitivity_receipt: receipt(
                root,
                "evidence/sensitivity.json",
                &serde_json::to_vec(&serde_json::json!({
                    "schema_version": "fsqlite.performance_admission_sensitivity_receipt.v1",
                    "baseline_source_commit": baseline,
                    "tested_source_commit": tested,
                    "policy_sha256": policy.sha256,
                    "policy_id": TEST_POLICY_ID,
                    "policy_version": TEST_POLICY_VERSION,
                    "manifest_bindings": manifest_bindings,
                }))
                .expect("serialize sensitivity receipt"),
            ),
            predicates: AdmissionPredicates {
                source_provenance: true,
                strict_ancestry: true,
                policy_hash: true,
                raw_evidence_hashes: true,
                environment_binding: true,
                calibration_receipt: true,
                sensitivity_receipt: true,
            },
            synthetic_fixture: false,
        }
    }

    #[test]
    fn synthetic_keeper_exercises_authorizing_gate_without_a_real_performance_claim() {
        let repo = tempfile::tempdir().expect("repo");
        git(repo.path(), &["init", "--initial-branch=main"]);
        git(repo.path(), &["config", "user.name", "Keeper"]);
        git(
            repo.path(),
            &["config", "user.email", "keeper@example.invalid"],
        );
        fs::write(repo.path().join("b"), "b").expect("write baseline");
        git(repo.path(), &["add", "b"]);
        git(repo.path(), &["commit", "-m", "baseline"]);
        let baseline = String::from_utf8(
            std::process::Command::new("git")
                .arg("-C")
                .arg(repo.path())
                .args(["rev-parse", "HEAD"])
                .output()
                .expect("baseline rev")
                .stdout,
        )
        .expect("utf8")
        .trim()
        .to_owned();
        fs::write(repo.path().join("t"), "t").expect("write tested");
        git(repo.path(), &["add", "t"]);
        git(repo.path(), &["commit", "-m", "tested"]);
        let tested = String::from_utf8(
            std::process::Command::new("git")
                .arg("-C")
                .arg(repo.path())
                .args(["rev-parse", "HEAD"])
                .output()
                .expect("tested rev")
                .stdout,
        )
        .expect("utf8")
        .trim()
        .to_owned();
        let pack = keeper(repo.path(), baseline, tested.clone());
        let bytes = serde_json::to_vec(&pack).expect("serialize keeper");
        fs::write(repo.path().join("evidence/admission-pack.json"), &bytes).expect("write keeper");
        let gate = PerformanceAdmissionGate {
            schema_version: ADMISSION_GATE_SCHEMA_V2.to_owned(),
            status: "authorized".to_owned(),
            release_authorized: true,
            blockers: Vec::new(),
            rationale: "synthetic keeper only; no performance claim".to_owned(),
            admission_pack: Some(AdmissionPackReference {
                path: "evidence/admission-pack.json".to_owned(),
                sha256: sha256(&bytes),
            }),
        };
        validate_gate(repo.path(), &tested, &gate).expect("keeper authorizes structurally");
        assert_eq!(
            authorized_artifact_paths(repo.path(), &tested, &gate)
                .expect("authorized artifact inventory")
                .len(),
            12,
            "pack, policy, two receipts, and B/T report/manifest pairs"
        );
        let mut mismatched = pack;
        mismatched.baseline.host_fingerprint_sha256 = "f".repeat(64);
        mismatched.tested.host_fingerprint_sha256 = "f".repeat(64);
        assert!(
            validate_pack(repo.path(), &tested, &mismatched, false)
                .expect_err("report provenance must be cross-checked")
                .contains("host fingerprint")
        );
    }

    #[test]
    fn v9_and_failed_predicate_cannot_authorize() {
        let gate = blocked_missing_authoritative_policy();
        assert_eq!(gate.schema_version, ADMISSION_GATE_SCHEMA_V2);
        assert!(!gate.release_authorized);
        let mut pack = PerformanceAdmissionPack {
            schema_version: ADMISSION_PACK_SCHEMA_V2.to_owned(),
            baseline: CandidateProvenance {
                source_commit: "a".repeat(40),
                host_fingerprint_sha256: "a".repeat(64),
                toolchain_sha256: "b".repeat(64),
                profiles: Vec::new(),
            },
            tested: CandidateProvenance {
                source_commit: "b".repeat(40),
                host_fingerprint_sha256: "a".repeat(64),
                toolchain_sha256: "b".repeat(64),
                profiles: Vec::new(),
            },
            policy: ArtifactDigest {
                path: "policy".to_owned(),
                digest_algorithm: "sha2-256".to_owned(),
                sha256: "c".repeat(64),
            },
            calibration_receipt: ArtifactDigest {
                path: "calibration".to_owned(),
                digest_algorithm: "sha2-256".to_owned(),
                sha256: "d".repeat(64),
            },
            sensitivity_receipt: ArtifactDigest {
                path: "sensitivity".to_owned(),
                digest_algorithm: "sha2-256".to_owned(),
                sha256: "e".repeat(64),
            },
            predicates: AdmissionPredicates {
                source_provenance: false,
                strict_ancestry: true,
                policy_hash: true,
                raw_evidence_hashes: true,
                environment_binding: true,
                calibration_receipt: true,
                sensitivity_receipt: true,
            },
            synthetic_fixture: false,
        };
        assert!(validate_pack(Path::new("."), &pack.tested.source_commit, &pack, false).is_err());
        pack.predicates.source_provenance = true;
        pack.baseline.profiles = vec![
            ProfileEvidence {
                profile: "release".to_owned(),
                report_schema: "fsqlite-e2e.mt_mvcc_bench_report.v9".to_owned(),
                raw_report: pack.policy.clone(),
                raw_manifest: pack.policy.clone(),
                feature_graph_sha256: "f".repeat(64),
                binary_nonce: "1".repeat(64),
            },
            ProfileEvidence {
                profile: "release-perf".to_owned(),
                report_schema: "x".to_owned(),
                raw_report: pack.policy.clone(),
                raw_manifest: pack.policy.clone(),
                feature_graph_sha256: "f".repeat(64),
                binary_nonce: "2".repeat(64),
            },
        ];
        pack.tested.profiles = pack.baseline.profiles.clone();
        pack.tested.profiles[0].binary_nonce = "3".repeat(64);
        pack.tested.profiles[1].binary_nonce = "4".repeat(64);
        assert!(
            validate_candidate(&pack.baseline, "baseline")
                .expect_err("v9 must be rejected directly")
                .contains("legacy v9")
        );
        let malformed_authorization = PerformanceAdmissionGate {
            schema_version: ADMISSION_GATE_SCHEMA_V2.to_owned(),
            status: "authorized".to_owned(),
            release_authorized: true,
            blockers: Vec::new(),
            rationale: "test".to_owned(),
            admission_pack: Some(AdmissionPackReference {
                path: "missing.json".to_owned(),
                sha256: "0".repeat(64),
            }),
        };
        assert!(
            validate_gate(
                Path::new("."),
                &pack.tested.source_commit,
                &malformed_authorization
            )
            .is_err()
        );
    }
}
