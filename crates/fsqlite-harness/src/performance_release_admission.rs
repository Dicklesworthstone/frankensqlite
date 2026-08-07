//! Typed, provenance-bound performance release-admission evidence.
//!
//! This module deliberately does not encode an acceptance margin.  The policy
//! that owns those values is an immutable input whose bytes are hash-bound by
//! an admission pack.  A missing policy is therefore a visible blocker, not a
//! reason to invent a threshold in a verifier.

use std::{
    collections::{BTreeMap, BTreeSet},
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
/// One-sided normal bound over paired natural-log ratios.  The supported
/// confidence levels and critical values are frozen below, so policy
/// evaluation cannot drift with a statistics-library upgrade.
const CONFIDENCE_METHOD_V1: &str = "paired_log_ratio_normal_one_sided_v1";

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
    /// Test-only fixtures must declare themselves. Production admission rejects
    /// them even when every evidence-derived decision otherwise passes.
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

/// Provenance plus the raw paired observations used by the verifier. The
/// policy owns thresholds; this verifier owns recomputation of every decision.
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
    measurements: Vec<MetricMeasurements>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct MetricMeasurements {
    workload: String,
    metric: String,
    observations: Vec<MeasurementObservation>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct MeasurementObservation {
    pair_id: String,
    order: String,
    value: f64,
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
    confidence_method: String,
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
struct CalibrationReceipt {
    schema_version: String,
    baseline_source_commit: String,
    tested_source_commit: String,
    policy_sha256: String,
    policy_id: String,
    policy_version: String,
    manifest_bindings: Vec<ReceiptManifestBinding>,
    confidence_method: String,
    outcomes: Vec<CalibrationOutcome>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SensitivityReceipt {
    schema_version: String,
    baseline_source_commit: String,
    tested_source_commit: String,
    policy_sha256: String,
    policy_id: String,
    policy_version: String,
    manifest_bindings: Vec<ReceiptManifestBinding>,
    confidence_method: String,
    outcomes: Vec<SensitivityOutcome>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReceiptManifestBinding {
    side: String,
    profile: String,
    manifest_sha256: String,
}

struct ReceiptProvenance<'a> {
    schema: &'a str,
    baseline_source_commit: &'a str,
    tested_source_commit: &'a str,
    policy_sha256: &'a str,
    policy_id: &'a str,
    policy_version: &'a str,
    manifest_bindings: &'a [ReceiptManifestBinding],
    confidence_method: &'a str,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CalibrationOutcome {
    profile: String,
    workload: String,
    metric: String,
    observations: Vec<CalibrationObservation>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct CalibrationObservation {
    pair_id: String,
    order: String,
    reference_value: f64,
    calibrated_value: f64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SensitivityOutcome {
    profile: String,
    workload: String,
    metric: String,
    observations: Vec<SensitivityObservation>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct SensitivityObservation {
    pair_id: String,
    order: String,
    control_value: f64,
    injected_value: f64,
}

trait OutcomeKey {
    fn key(&self) -> (&str, &str, &str);
}

impl OutcomeKey for CalibrationOutcome {
    fn key(&self) -> (&str, &str, &str) {
        (&self.profile, &self.workload, &self.metric)
    }
}

impl OutcomeKey for SensitivityOutcome {
    fn key(&self) -> (&str, &str, &str) {
        (&self.profile, &self.workload, &self.metric)
    }
}

#[derive(Clone, Copy)]
enum BoundSide {
    Lower,
    Upper,
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
            let artifact_root = pack_path
                .parent()
                .ok_or_else(|| "admission pack must have a parent directory".to_owned())?;
            validate_pack_at(
                workspace_root,
                artifact_root,
                tested_commit,
                &pack,
                false,
            )
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
    validate_pack_at(
        workspace_root,
        workspace_root,
        expected_tested_commit,
        pack,
        allow_synthetic_fixture,
    )
}

/// Validates a pack whose immutable artifact paths are relative to
/// `artifact_root`, while Git ancestry is verified in `workspace_root`.
///
/// Keeping these roots separate lets capture relocate an already-authoritative
/// directory tree byte-for-byte without rewriting any provenance fields.
pub fn validate_pack_at(
    workspace_root: &Path,
    artifact_root: &Path,
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
    let policy = validate_policy(artifact_root, &pack.policy)?;
    for profile_name in &policy.required_profiles {
        let baseline_profile = profile_by_name(&pack.baseline, profile_name)?;
        let tested_profile = profile_by_name(&pack.tested, profile_name)?;
        let baseline_report = validate_profile_artifacts(
            artifact_root,
            &pack.baseline,
            baseline_profile,
            "baseline",
            &policy,
            &pack.policy.sha256,
        )?;
        let tested_report = validate_profile_artifacts(
            artifact_root,
            &pack.tested,
            tested_profile,
            "tested",
            &policy,
            &pack.policy.sha256,
        )?;
        evaluate_profile_measurements(
            &policy,
            profile_name,
            &baseline_report.measurements,
            &tested_report.measurements,
        )?;
    }
    validate_calibration_receipt(artifact_root, &pack.calibration_receipt, pack, &policy)?;
    validate_sensitivity_receipt(artifact_root, &pack.sensitivity_receipt, pack, &policy)?;
    Ok(())
}

fn validate_profile_artifacts(
    workspace_root: &Path,
    candidate: &CandidateProvenance,
    profile: &ProfileEvidence,
    side: &str,
    policy: &AdmissionPolicy,
    policy_sha256: &str,
) -> Result<MeasurementReport, String> {
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
    Ok(report)
}

fn validate_receipt_provenance(
    receipt: ReceiptProvenance<'_>,
    expected_schema: &str,
    pack: &PerformanceAdmissionPack,
    policy: &AdmissionPolicy,
    label: &str,
) -> Result<(), String> {
    if receipt.schema != expected_schema
        || receipt.baseline_source_commit != pack.baseline.source_commit
        || receipt.tested_source_commit != pack.tested.source_commit
        || receipt.policy_sha256 != pack.policy.sha256
        || receipt.policy_id != policy.policy_id
        || receipt.policy_version != policy.policy_version
        || receipt.confidence_method != policy.confidence_method
    {
        return Err(format!(
            "{label} is not bound to the B/T source commits, policy, and confidence method"
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
        .iter()
        .map(|binding| {
            (
                binding.side.clone(),
                binding.profile.clone(),
                binding.manifest_sha256.clone(),
            )
        })
        .collect::<BTreeSet<_>>();
    if actual != expected {
        return Err(format!(
            "{label} manifest provenance bindings do not exactly cover B/T profiles"
        ));
    }
    Ok(())
}

fn validate_calibration_receipt(
    workspace_root: &Path,
    artifact: &ArtifactDigest,
    pack: &PerformanceAdmissionPack,
    policy: &AdmissionPolicy,
) -> Result<(), String> {
    let label = "calibration receipt";
    let bytes = artifact_bytes(workspace_root, artifact, label)?;
    let receipt: CalibrationReceipt = serde_json::from_slice(&bytes)
        .map_err(|error| format!("{label} is not a typed v2 receipt: {error}"))?;
    validate_receipt_provenance(
        ReceiptProvenance {
            schema: &receipt.schema_version,
            baseline_source_commit: &receipt.baseline_source_commit,
            tested_source_commit: &receipt.tested_source_commit,
            policy_sha256: &receipt.policy_sha256,
            policy_id: &receipt.policy_id,
            policy_version: &receipt.policy_version,
            manifest_bindings: &receipt.manifest_bindings,
            confidence_method: &receipt.confidence_method,
        },
        CALIBRATION_RECEIPT_SCHEMA_V1,
        pack,
        policy,
        label,
    )?;
    let outcomes = outcome_map(&receipt.outcomes, policy, label)?;
    for (key, outcome) in outcomes {
        let rule = metric_rule(policy, &key.2)?;
        let log_noise = validate_calibration_observations(
            &outcome.observations,
            policy,
            rule.minimum_samples,
            label,
        )?;
        let upper = confidence_bound(&log_noise, rule.confidence_level, BoundSide::Upper)?;
        if upper > policy.calibration_noise_multiplier.ln() {
            return Err(format!(
                "calibration outcome exceeds policy noise bound for {}/{}/{}",
                key.0, key.1, key.2
            ));
        }
    }
    Ok(())
}

fn validate_sensitivity_receipt(
    workspace_root: &Path,
    artifact: &ArtifactDigest,
    pack: &PerformanceAdmissionPack,
    policy: &AdmissionPolicy,
) -> Result<(), String> {
    let label = "sensitivity receipt";
    let bytes = artifact_bytes(workspace_root, artifact, label)?;
    let receipt: SensitivityReceipt = serde_json::from_slice(&bytes)
        .map_err(|error| format!("{label} is not a typed v2 receipt: {error}"))?;
    validate_receipt_provenance(
        ReceiptProvenance {
            schema: &receipt.schema_version,
            baseline_source_commit: &receipt.baseline_source_commit,
            tested_source_commit: &receipt.tested_source_commit,
            policy_sha256: &receipt.policy_sha256,
            policy_id: &receipt.policy_id,
            policy_version: &receipt.policy_version,
            manifest_bindings: &receipt.manifest_bindings,
            confidence_method: &receipt.confidence_method,
        },
        SENSITIVITY_RECEIPT_SCHEMA_V1,
        pack,
        policy,
        label,
    )?;
    let outcomes = outcome_map(&receipt.outcomes, policy, label)?;
    for (key, outcome) in outcomes {
        let rule = metric_rule(policy, &key.2)?;
        let regressions =
            validate_sensitivity_observations(&outcome.observations, policy, rule, label)?;
        let lower = confidence_bound(&regressions, rule.confidence_level, BoundSide::Lower)?;
        if lower < policy.sensitivity_injected_slowdown_minimum.ln_1p() {
            return Err(format!(
                "sensitivity perturbation was not detected for {}/{}/{}",
                key.0, key.1, key.2
            ));
        }
    }
    Ok(())
}

fn evaluate_profile_measurements(
    policy: &AdmissionPolicy,
    profile: &str,
    baseline: &[MetricMeasurements],
    tested: &[MetricMeasurements],
) -> Result<(), String> {
    let baseline = measurement_map(baseline, policy, &format!("baseline {profile}"))?;
    let tested = measurement_map(tested, policy, &format!("tested {profile}"))?;
    for (key, baseline_series) in baseline {
        let tested_series = tested.get(&key).ok_or_else(|| {
            format!(
                "tested {profile} is missing workload/metric {}/{}",
                key.0, key.1
            )
        })?;
        let rule = metric_rule(policy, &key.1)?;
        let baseline_pairs = measurement_pairs(
            &baseline_series.observations,
            policy,
            rule.minimum_samples,
            &format!("baseline {profile} {}/{}", key.0, key.1),
        )?;
        let tested_pairs = measurement_pairs(
            &tested_series.observations,
            policy,
            rule.minimum_samples,
            &format!("tested {profile} {}/{}", key.0, key.1),
        )?;
        if baseline_pairs.keys().collect::<Vec<_>>() != tested_pairs.keys().collect::<Vec<_>>() {
            return Err(format!(
                "B/T pair IDs do not match for {profile} {}/{}",
                key.0, key.1
            ));
        }
        let mut regressions = Vec::with_capacity(baseline_pairs.len());
        for (pair_id, (baseline_order, baseline_value)) in baseline_pairs {
            let (tested_order, tested_value) = tested_pairs
                .get(&pair_id)
                .ok_or_else(|| format!("tested measurement is missing pair `{pair_id}`"))?;
            if &baseline_order != tested_order {
                return Err(format!(
                    "B/T counterbalance order differs for pair `{pair_id}` in {profile} {}/{}",
                    key.0, key.1
                ));
            }
            regressions.push(normalized_log_regression(
                baseline_value,
                *tested_value,
                &rule.direction,
            )?);
        }
        let upper = confidence_bound(&regressions, rule.confidence_level, BoundSide::Upper)?;
        if upper > rule.max_regression_fraction.ln_1p() {
            return Err(format!(
                "measured regression exceeds policy for {profile} {}/{}",
                key.0, key.1
            ));
        }
    }
    Ok(())
}

fn measurement_map<'a>(
    measurements: &'a [MetricMeasurements],
    policy: &AdmissionPolicy,
    label: &str,
) -> Result<BTreeMap<(String, String), &'a MetricMeasurements>, String> {
    let mut actual = BTreeMap::new();
    for measurement in measurements {
        if measurement.workload.trim().is_empty() || measurement.metric.trim().is_empty() {
            return Err(format!("{label} has an empty workload or metric"));
        }
        let key = (measurement.workload.clone(), measurement.metric.clone());
        if actual.insert(key.clone(), measurement).is_some() {
            return Err(format!(
                "{label} has duplicate workload/metric {}/{}",
                key.0, key.1
            ));
        }
    }
    let expected = expected_metric_keys(policy);
    if actual.keys().cloned().collect::<BTreeSet<_>>() != expected {
        return Err(format!(
            "{label} workload/metric coverage does not exactly match policy"
        ));
    }
    Ok(actual)
}

fn outcome_map<'a, T: OutcomeKey>(
    outcomes: &'a [T],
    policy: &AdmissionPolicy,
    label: &str,
) -> Result<BTreeMap<(String, String, String), &'a T>, String> {
    let mut actual = BTreeMap::new();
    for outcome in outcomes {
        let (profile, workload, metric) = outcome.key();
        if profile.trim().is_empty() || workload.trim().is_empty() || metric.trim().is_empty() {
            return Err(format!("{label} has an empty profile, workload, or metric"));
        }
        let key = (profile.to_owned(), workload.to_owned(), metric.to_owned());
        if actual.insert(key.clone(), outcome).is_some() {
            return Err(format!(
                "{label} has duplicate outcome {}/{}/{}",
                key.0, key.1, key.2
            ));
        }
    }
    if actual.keys().cloned().collect::<BTreeSet<_>>() != expected_outcome_keys(policy) {
        return Err(format!(
            "{label} outcome coverage does not exactly match policy"
        ));
    }
    Ok(actual)
}

fn expected_metric_keys(policy: &AdmissionPolicy) -> BTreeSet<(String, String)> {
    policy
        .required_workloads
        .iter()
        .flat_map(|workload| {
            policy
                .metric_rules
                .iter()
                .map(move |rule| (workload.clone(), rule.metric.clone()))
        })
        .collect()
}

fn expected_outcome_keys(policy: &AdmissionPolicy) -> BTreeSet<(String, String, String)> {
    policy
        .required_profiles
        .iter()
        .flat_map(|profile| {
            expected_metric_keys(policy)
                .into_iter()
                .map(move |(workload, metric)| (profile.clone(), workload, metric))
        })
        .collect()
}

fn metric_rule<'a>(policy: &'a AdmissionPolicy, metric: &str) -> Result<&'a MetricRule, String> {
    policy
        .metric_rules
        .iter()
        .find(|rule| rule.metric == metric)
        .ok_or_else(|| format!("metric `{metric}` is not defined by policy"))
}

fn measurement_pairs(
    observations: &[MeasurementObservation],
    policy: &AdmissionPolicy,
    minimum_samples: u64,
    label: &str,
) -> Result<BTreeMap<String, (String, f64)>, String> {
    let minimum_samples = usize::try_from(minimum_samples)
        .map_err(|_| format!("{label} minimum sample count does not fit this platform"))?;
    if observations.len() < minimum_samples {
        return Err(format!("{label} has insufficient samples"));
    }
    let mut pairs = BTreeMap::new();
    let mut orders = BTreeSet::new();
    for observation in observations {
        validate_pair_fields(
            &observation.pair_id,
            &observation.order,
            observation.value,
            policy,
            label,
        )?;
        orders.insert(observation.order.clone());
        if pairs
            .insert(
                observation.pair_id.clone(),
                (observation.order.clone(), observation.value),
            )
            .is_some()
        {
            return Err(format!("{label} has duplicate pair IDs"));
        }
    }
    require_exact_order_coverage(&orders, policy, label)?;
    Ok(pairs)
}

fn validate_calibration_observations(
    observations: &[CalibrationObservation],
    policy: &AdmissionPolicy,
    minimum_samples: u64,
    label: &str,
) -> Result<Vec<f64>, String> {
    let minimum_samples = usize::try_from(minimum_samples)
        .map_err(|_| format!("{label} minimum sample count does not fit this platform"))?;
    if observations.len() < minimum_samples {
        return Err(format!("{label} has insufficient calibration samples"));
    }
    let mut pair_ids = BTreeSet::new();
    let mut orders = BTreeSet::new();
    let mut log_noise = Vec::with_capacity(observations.len());
    for observation in observations {
        validate_pair_fields(
            &observation.pair_id,
            &observation.order,
            observation.reference_value,
            policy,
            label,
        )?;
        validate_positive_finite(observation.calibrated_value, label)?;
        if !pair_ids.insert(observation.pair_id.as_str()) {
            return Err(format!("{label} has duplicate calibration pair IDs"));
        }
        orders.insert(observation.order.clone());
        log_noise.push(
            (observation.calibrated_value / observation.reference_value)
                .ln()
                .abs(),
        );
    }
    require_exact_order_coverage(&orders, policy, label)?;
    Ok(log_noise)
}

fn validate_sensitivity_observations(
    observations: &[SensitivityObservation],
    policy: &AdmissionPolicy,
    rule: &MetricRule,
    label: &str,
) -> Result<Vec<f64>, String> {
    let minimum_samples = usize::try_from(rule.minimum_samples)
        .map_err(|_| format!("{label} minimum sample count does not fit this platform"))?;
    if observations.len() < minimum_samples {
        return Err(format!("{label} has insufficient sensitivity samples"));
    }
    let mut pair_ids = BTreeSet::new();
    let mut orders = BTreeSet::new();
    let mut regressions = Vec::with_capacity(observations.len());
    for observation in observations {
        validate_pair_fields(
            &observation.pair_id,
            &observation.order,
            observation.control_value,
            policy,
            label,
        )?;
        validate_positive_finite(observation.injected_value, label)?;
        if !pair_ids.insert(observation.pair_id.as_str()) {
            return Err(format!("{label} has duplicate sensitivity pair IDs"));
        }
        orders.insert(observation.order.clone());
        regressions.push(normalized_log_regression(
            observation.control_value,
            observation.injected_value,
            &rule.direction,
        )?);
    }
    require_exact_order_coverage(&orders, policy, label)?;
    Ok(regressions)
}

fn validate_pair_fields(
    pair_id: &str,
    order: &str,
    value: f64,
    policy: &AdmissionPolicy,
    label: &str,
) -> Result<(), String> {
    if pair_id.trim().is_empty() || !policy.counterbalance_order.iter().any(|item| item == order) {
        return Err(format!(
            "{label} has an invalid pair ID or counterbalance order"
        ));
    }
    validate_positive_finite(value, label)
}

fn validate_positive_finite(value: f64, label: &str) -> Result<(), String> {
    if value.is_finite() && value > 0.0 {
        Ok(())
    } else {
        Err(format!(
            "{label} observations must be finite and strictly positive"
        ))
    }
}

fn require_exact_order_coverage(
    actual: &BTreeSet<String>,
    policy: &AdmissionPolicy,
    label: &str,
) -> Result<(), String> {
    let expected = policy
        .counterbalance_order
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    if actual == &expected {
        Ok(())
    } else {
        Err(format!(
            "{label} does not exactly cover required counterbalance orders"
        ))
    }
}

fn normalized_log_regression(
    reference: f64,
    candidate: f64,
    direction: &str,
) -> Result<f64, String> {
    match direction {
        "higher_is_better" => Ok((reference / candidate).ln()),
        "lower_is_better" => Ok((candidate / reference).ln()),
        _ => Err("metric direction is not supported by the confidence method".to_owned()),
    }
}

fn confidence_bound(values: &[f64], confidence_level: f64, side: BoundSide) -> Result<f64, String> {
    if values.len() < 2 || values.iter().any(|value| !value.is_finite()) {
        return Err("confidence input must contain at least two finite samples".to_owned());
    }
    let count = f64::from(
        u32::try_from(values.len())
            .map_err(|_| "confidence input contains too many samples".to_owned())?,
    );
    let mean = values.iter().sum::<f64>() / count;
    let variance = values
        .iter()
        .map(|value| {
            let residual = value - mean;
            residual * residual
        })
        .sum::<f64>()
        / (count - 1.0);
    let standard_error = (variance / count).sqrt();
    let critical = normal_critical_value(confidence_level).ok_or_else(|| {
        format!("unsupported confidence level {confidence_level} for {CONFIDENCE_METHOD_V1}")
    })?;
    Ok(match side {
        BoundSide::Lower => critical.mul_add(-standard_error, mean),
        BoundSide::Upper => critical.mul_add(standard_error, mean),
    })
}

fn normal_critical_value(confidence_level: f64) -> Option<f64> {
    const LEVELS: [(f64, f64); 4] = [
        (0.90, 1.281_551_565_544_600_4),
        (0.95, 1.644_853_626_951_472_2),
        (0.975, 1.959_963_984_540_054),
        (0.99, 2.326_347_874_040_840_8),
    ];
    LEVELS
        .into_iter()
        .find(|(level, _)| (confidence_level - level).abs() < f64::EPSILON)
        .map(|(_, critical)| critical)
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
        || policy.confidence_method != CONFIDENCE_METHOD_V1
        || !policy.calibration_noise_multiplier.is_finite()
        || policy.calibration_noise_multiplier <= 1.0
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
        && normal_critical_value(rule.confidence_level).is_some()
        && rule.minimum_samples >= 2
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
    let pack_parent = Path::new(&reference.path)
        .parent()
        .ok_or_else(|| "admission pack reference must have a parent".to_owned())?;
    let mut paths = BTreeSet::from([
        reference.path.clone(),
        relocated_artifact_path(pack_parent, &pack.policy.path)?,
    ]);
    for artifact in [&pack.calibration_receipt, &pack.sensitivity_receipt] {
        paths.insert(relocated_artifact_path(pack_parent, &artifact.path)?);
    }
    for candidate in [&pack.baseline, &pack.tested] {
        for profile in &candidate.profiles {
            paths.insert(relocated_artifact_path(
                pack_parent,
                &profile.raw_report.path,
            )?);
            paths.insert(relocated_artifact_path(
                pack_parent,
                &profile.raw_manifest.path,
            )?);
        }
    }
    for path in &paths {
        checked_path(workspace_root, path, "authorized performance artifact")?;
    }
    Ok(paths)
}

fn relocated_artifact_path(parent: &Path, raw: &str) -> Result<String, String> {
    let relative = Path::new(raw);
    if raw.trim().is_empty()
        || relative.is_absolute()
        || relative
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(
            "admission artifact path must contain only normal relative components".to_owned(),
        );
    }
    parent
        .join(relative)
        .to_str()
        .map(str::to_owned)
        .ok_or_else(|| "admission artifact path is not UTF-8".to_owned())
}

fn checked_path(workspace_root: &Path, raw: &str, label: &str) -> Result<PathBuf, String> {
    let relative = Path::new(raw);
    if raw.trim().is_empty()
        || relative.is_absolute()
        || relative
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(format!("{label} path must be a non-empty relative path"));
    }
    let canonical_root = workspace_root
        .canonicalize()
        .map_err(|error| format!("unable to canonicalize artifact root: {error}"))?;
    let joined = canonical_root.join(relative);
    let metadata = fs::symlink_metadata(&joined)
        .map_err(|error| format!("unable to inspect {label}: {error}"))?;
    if metadata.file_type().is_symlink() {
        return Err(format!("{label} must not be a symlink"));
    }
    let canonical = joined
        .canonicalize()
        .map_err(|error| format!("unable to canonicalize {label}: {error}"))?;
    if !canonical.starts_with(&canonical_root) {
        return Err(format!(
            "{label} resolves outside the canonical artifact root"
        ));
    }
    Ok(canonical)
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
        AdmissionPolicy, ArtifactDigest, CalibrationObservation, CandidateProvenance,
        MetricMeasurements, MetricRule, PerformanceAdmissionGate, PerformanceAdmissionPack,
        ProfileEvidence, SensitivityObservation, artifact_bytes, authorized_artifact_paths,
        blocked_missing_authoritative_policy, evaluate_profile_measurements, sha256,
        validate_calibration_observations, validate_candidate, validate_gate, validate_pack,
        validate_sensitivity_observations,
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
            "measurements": [{
                "workload": "synthetic-keeper-workload",
                "metric": "synthetic-throughput",
                "observations": observations(100.0),
            }],
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

    fn observations(value: f64) -> Vec<serde_json::Value> {
        (0..8)
            .map(|index| {
                serde_json::json!({
                    "pair_id": format!("pair-{index}"),
                    "order": if index % 2 == 0 { "baseline_first" } else { "tested_first" },
                    "value": value,
                })
            })
            .collect()
    }

    fn calibration_observations() -> Vec<serde_json::Value> {
        (0..8)
            .map(|index| {
                serde_json::json!({
                    "pair_id": format!("calibration-{index}"),
                    "order": if index % 2 == 0 { "baseline_first" } else { "tested_first" },
                    "reference_value": 100.0,
                    "calibrated_value": 100.0,
                })
            })
            .collect()
    }

    fn sensitivity_observations() -> Vec<serde_json::Value> {
        (0..8)
            .map(|index| {
                serde_json::json!({
                    "pair_id": format!("sensitivity-{index}"),
                    "order": if index % 2 == 0 { "baseline_first" } else { "tested_first" },
                    "control_value": 100.0,
                    "injected_value": 90.0,
                })
            })
            .collect()
    }

    fn receipt_outcomes(kind: &str) -> Vec<serde_json::Value> {
        ["release", "release-perf"]
            .into_iter()
            .map(|profile| {
                let observations = if kind == "calibration" {
                    calibration_observations()
                } else {
                    sensitivity_observations()
                };
                serde_json::json!({
                    "profile": profile,
                    "workload": "synthetic-keeper-workload",
                    "metric": "synthetic-throughput",
                    "observations": observations,
                })
            })
            .collect()
    }

    fn evaluation_policy() -> AdmissionPolicy {
        AdmissionPolicy {
            schema_version: "fsqlite.performance_admission_policy.v1".to_owned(),
            policy_id: TEST_POLICY_ID.to_owned(),
            policy_version: TEST_POLICY_VERSION.to_owned(),
            required_profiles: vec!["release".to_owned(), "release-perf".to_owned()],
            required_workloads: vec!["synthetic-keeper-workload".to_owned()],
            metric_rules: vec![MetricRule {
                metric: "synthetic-throughput".to_owned(),
                direction: "higher_is_better".to_owned(),
                max_regression_fraction: 0.1,
                confidence_level: 0.95,
                minimum_samples: 8,
            }],
            counterbalance_order: vec!["baseline_first".to_owned(), "tested_first".to_owned()],
            confidence_method: "paired_log_ratio_normal_one_sided_v1".to_owned(),
            calibration_noise_multiplier: 1.5,
            sensitivity_injected_slowdown_minimum: 0.05,
            sensitivity_detection_required: true,
            no_waiver: true,
        }
    }

    fn metric_measurements(value: f64) -> Vec<MetricMeasurements> {
        vec![MetricMeasurements {
            workload: "synthetic-keeper-workload".to_owned(),
            metric: "synthetic-throughput".to_owned(),
            observations: (0..8)
                .map(|index| super::MeasurementObservation {
                    pair_id: format!("pair-{index}"),
                    order: if index % 2 == 0 {
                        "baseline_first"
                    } else {
                        "tested_first"
                    }
                    .to_owned(),
                    value,
                })
                .collect(),
        }]
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
                "confidence_method": "paired_log_ratio_normal_one_sided_v1",
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
                    "confidence_method": "paired_log_ratio_normal_one_sided_v1",
                    "outcomes": receipt_outcomes("calibration"),
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
                    "confidence_method": "paired_log_ratio_normal_one_sided_v1",
                    "outcomes": receipt_outcomes("sensitivity"),
                }))
                .expect("serialize sensitivity receipt"),
            ),
            synthetic_fixture: false,
        }
    }

    fn keeper_with_history() -> (tempfile::TempDir, String, PerformanceAdmissionPack) {
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
        (repo, tested, pack)
    }

    fn rewrite_json_artifact(
        root: &Path,
        artifact: &mut ArtifactDigest,
        mutate: impl FnOnce(&mut serde_json::Value),
    ) {
        let path = root.join(&artifact.path);
        let mut value: serde_json::Value =
            serde_json::from_slice(&fs::read(&path).expect("read artifact"))
                .expect("parse artifact");
        mutate(&mut value);
        let bytes = serde_json::to_vec(&value).expect("serialize artifact");
        fs::write(path, &bytes).expect("rewrite artifact");
        artifact.sha256 = sha256(&bytes);
    }

    #[test]
    fn synthetic_keeper_exercises_authorizing_gate_without_a_real_performance_claim() {
        let (repo, tested, pack) = keeper_with_history();
        let bytes = serde_json::to_vec(&pack).expect("serialize keeper");
        fs::write(repo.path().join("admission-pack.json"), &bytes).expect("write keeper");
        let gate = PerformanceAdmissionGate {
            schema_version: ADMISSION_GATE_SCHEMA_V2.to_owned(),
            status: "authorized".to_owned(),
            release_authorized: true,
            blockers: Vec::new(),
            rationale: "synthetic keeper only; no performance claim".to_owned(),
            admission_pack: Some(AdmissionPackReference {
                path: "admission-pack.json".to_owned(),
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
    fn raw_measurement_evaluator_rejects_untrusted_or_incomplete_series() {
        let policy = evaluation_policy();
        let baseline = metric_measurements(100.0);
        let tested = metric_measurements(100.0);
        evaluate_profile_measurements(&policy, "release", &baseline, &tested)
            .expect("complete paired observations pass");

        let mut swapped_order = tested.clone();
        swapped_order[0].observations[0].order = "tested_first".to_owned();
        assert!(
            evaluate_profile_measurements(&policy, "release", &baseline, &swapped_order)
                .expect_err("B/T order mismatch must fail")
                .contains("counterbalance order differs")
        );

        let mut mismatched_pair = tested.clone();
        mismatched_pair[0].observations[0].pair_id = "different-pair".to_owned();
        assert!(
            evaluate_profile_measurements(&policy, "release", &baseline, &mismatched_pair)
                .expect_err("B/T pair mismatch must fail")
                .contains("pair IDs do not match")
        );

        let mut duplicate_pair = baseline.clone();
        duplicate_pair[0].observations[1].pair_id = "pair-0".to_owned();
        assert!(
            evaluate_profile_measurements(&policy, "release", &duplicate_pair, &tested)
                .expect_err("duplicate pair IDs must fail")
                .contains("duplicate pair IDs")
        );

        let mut missing_metric = baseline.clone();
        missing_metric.clear();
        assert!(
            evaluate_profile_measurements(&policy, "release", &missing_metric, &tested)
                .expect_err("missing workload/metric must fail")
                .contains("coverage")
        );

        let mut duplicate_metric = baseline.clone();
        duplicate_metric.push(duplicate_metric[0].clone());
        assert!(
            evaluate_profile_measurements(&policy, "release", &duplicate_metric, &tested)
                .expect_err("duplicate workload/metric must fail")
                .contains("duplicate workload/metric")
        );

        let mut insufficient = baseline.clone();
        insufficient[0].observations.pop();
        assert!(
            evaluate_profile_measurements(&policy, "release", &insufficient, &tested)
                .expect_err("insufficient samples must fail")
                .contains("insufficient samples")
        );

        for invalid in [0.0, f64::INFINITY, f64::NAN] {
            let mut invalid_values = baseline.clone();
            invalid_values[0].observations[0].value = invalid;
            assert!(
                evaluate_profile_measurements(&policy, "release", &invalid_values, &tested)
                    .expect_err("nonpositive or nonfinite observations must fail")
                    .contains("finite and strictly positive")
            );
        }

        let regressed = metric_measurements(80.0);
        assert!(
            evaluate_profile_measurements(&policy, "release", &baseline, &regressed)
                .expect_err("measured regression above policy must fail")
                .contains("measured regression exceeds policy")
        );
    }

    #[test]
    fn calibration_and_sensitivity_are_recomputed_from_raw_outcomes() {
        let policy = evaluation_policy();
        let rule = &policy.metric_rules[0];
        let calibration = (0..8)
            .map(|index| CalibrationObservation {
                pair_id: format!("calibration-{index}"),
                order: if index % 2 == 0 {
                    "baseline_first"
                } else {
                    "tested_first"
                }
                .to_owned(),
                reference_value: 100.0,
                calibrated_value: 200.0,
            })
            .collect::<Vec<_>>();
        let noise = validate_calibration_observations(
            &calibration,
            &policy,
            rule.minimum_samples,
            "calibration receipt",
        )
        .expect("typed calibration samples");
        assert!(
            super::confidence_bound(&noise, rule.confidence_level, super::BoundSide::Upper)
                .expect("calibration confidence bound")
                > policy.calibration_noise_multiplier.ln(),
            "failed calibration is detected from values"
        );

        let sensitivity = (0..8)
            .map(|index| SensitivityObservation {
                pair_id: format!("sensitivity-{index}"),
                order: if index % 2 == 0 {
                    "baseline_first"
                } else {
                    "tested_first"
                }
                .to_owned(),
                control_value: 100.0,
                injected_value: 100.0,
            })
            .collect::<Vec<_>>();
        let regressions =
            validate_sensitivity_observations(&sensitivity, &policy, rule, "sensitivity receipt")
                .expect("typed sensitivity samples");
        assert!(
            super::confidence_bound(&regressions, rule.confidence_level, super::BoundSide::Lower,)
                .expect("sensitivity confidence bound")
                < policy.sensitivity_injected_slowdown_minimum.ln_1p(),
            "undetected perturbation is derived from values"
        );
    }

    #[test]
    fn failed_receipts_and_post_hoc_unbound_input_cannot_authorize() {
        let (repo, tested, mut pack) = keeper_with_history();

        let original_calibration = pack.calibration_receipt.clone();
        let original_calibration_bytes =
            fs::read(repo.path().join(&original_calibration.path)).expect("calibration bytes");
        rewrite_json_artifact(repo.path(), &mut pack.calibration_receipt, |document| {
            for observation in document["outcomes"][0]["observations"]
                .as_array_mut()
                .expect("calibration observations")
            {
                observation["calibrated_value"] = serde_json::json!(200.0);
            }
        });
        assert!(
            validate_pack(repo.path(), &tested, &pack, false)
                .expect_err("failed calibration must not authorize")
                .contains("calibration outcome exceeds")
        );
        fs::write(
            repo.path().join(&original_calibration.path),
            original_calibration_bytes,
        )
        .expect("restore calibration");
        pack.calibration_receipt = original_calibration;

        let original_sensitivity = pack.sensitivity_receipt.clone();
        let original_sensitivity_bytes =
            fs::read(repo.path().join(&original_sensitivity.path)).expect("sensitivity bytes");
        rewrite_json_artifact(repo.path(), &mut pack.sensitivity_receipt, |document| {
            for observation in document["outcomes"][0]["observations"]
                .as_array_mut()
                .expect("sensitivity observations")
            {
                observation["injected_value"] = serde_json::json!(100.0);
            }
        });
        assert!(
            validate_pack(repo.path(), &tested, &pack, false)
                .expect_err("failed sensitivity must not authorize")
                .contains("sensitivity perturbation was not detected")
        );
        fs::write(
            repo.path().join(&original_sensitivity.path),
            original_sensitivity_bytes,
        )
        .expect("restore sensitivity");
        pack.sensitivity_receipt = original_sensitivity;

        rewrite_json_artifact(
            repo.path(),
            &mut pack.baseline.profiles[0].raw_report,
            |document| {
                document["raw_manifest_path"] = serde_json::json!("attacker/manifest.json");
            },
        );
        assert!(
            validate_pack(repo.path(), &tested, &pack, false)
                .expect_err("unbound source cannot be repaired after capture")
                .contains("report/manifest hashes are not mutually bound")
        );
    }

    #[cfg(unix)]
    #[test]
    fn symlinked_parent_cannot_escape_artifact_root() {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir().expect("root");
        let outside = tempfile::tempdir().expect("outside");
        fs::write(outside.path().join("policy.json"), b"outside").expect("outside file");
        symlink(outside.path(), root.path().join("evidence")).expect("symlinked parent");
        let artifact = ArtifactDigest {
            path: "evidence/policy.json".to_owned(),
            digest_algorithm: "sha2-256".to_owned(),
            sha256: sha256(b"outside"),
        };
        assert!(
            artifact_bytes(root.path(), &artifact, "policy")
                .expect_err("symlink-parent escape must fail")
                .contains("outside the canonical artifact root")
        );
    }

    #[test]
    fn v9_and_fake_predicates_cannot_authorize() {
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
            synthetic_fixture: false,
        };
        assert!(validate_pack(Path::new("."), &pack.tested.source_commit, &pack, false).is_err());
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
        let mut pack_json = serde_json::to_value(&pack).expect("serialize pack");
        pack_json.as_object_mut().expect("pack object").insert(
            "predicates".to_owned(),
            serde_json::json!({
                "source_provenance": true,
                "strict_ancestry": true,
                "policy_hash": true,
                "raw_evidence_hashes": true,
                "environment_binding": true,
                "calibration_receipt": true,
                "sensitivity_receipt": true,
            }),
        );
        assert!(
            serde_json::from_value::<PerformanceAdmissionPack>(pack_json).is_err(),
            "self-asserted predicates are outside the authorizing schema"
        );
    }
}
