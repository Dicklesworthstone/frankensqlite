use std::env;
use std::fs;
use std::fs::OpenOptions;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};
use std::time::{SystemTime, UNIX_EPOCH};

use fsqlite_harness::adversarial_search::CampaignResult;
use fsqlite_harness::certification_policy::{
    CERTIFICATION_POLICY_ID, CERTIFICATION_POLICY_SCHEMA_VERSION, CertificationPolicy,
    CertificationRatchetBaseline, CertificationRatchetCandidate, evaluate_certification_ratchets,
};
use fsqlite_harness::ci_gate_matrix::{ArtifactManifest, GlobalFlakeBudgetResult};
use fsqlite_harness::confidence_gates::{
    EvidenceLedger, ExpectedLossRanking, GateDecision, GateReport,
};
use fsqlite_harness::drift_monitor::ParityDriftSnapshot;
use fsqlite_harness::no_mock_critical_path_gate::{NoMockCriticalPathReport, NoMockVerdict};
use fsqlite_harness::parity_invariant_catalog::build_canonical_catalog;
use fsqlite_harness::parity_verification_workflow::{
    BEAD_ID, WorkflowInput, build_workflow_report, render_workflow_markdown,
};
use fsqlite_harness::release_certificate::{
    CertificateConfig, CertificateInputs, CertificateVerdict, build_certificate,
};
use fsqlite_harness::score_engine::BayesianScorecard;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const DEFAULT_OUTPUT_DIR: &str = "artifacts/parity-verification-workflow";
const CERTIFICATE_INPUT_SCHEMA: &str = "fsqlite.parity_certificate_input.v1";
const MAX_EVIDENCE_AGE_MS: u128 = 24 * 60 * 60 * 1_000;
const REQUIRED_CERTIFICATION_LANES: [&str; 6] = [
    "unit",
    "e2e-differential",
    "e2e-correctness",
    "e2e-recovery",
    "schema-validation",
    "coverage-drift",
];

#[derive(Debug, Deserialize)]
struct EvidenceRef {
    path: String,
    sha256: String,
    observed_unix_ms: u128,
}

#[derive(Debug, Deserialize)]
struct LaneManifestRef {
    lane: String,
    #[serde(flatten)]
    evidence: EvidenceRef,
}

#[derive(Debug, Deserialize)]
struct CertificateEvidenceInput {
    schema_version: String,
    candidate_git_sha: String,
    generated_unix_ms: u128,
    gate_report: EvidenceRef,
    expected_loss_ranking: EvidenceRef,
    evidence_ledger: EvidenceRef,
    drift_snapshot: EvidenceRef,
    adversarial_campaign: EvidenceRef,
    ci_flake_budget: EvidenceRef,
    certification_policy: EvidenceRef,
    ratchet_baseline: EvidenceRef,
    ratchet_candidate: EvidenceRef,
    critical_path_evidence: EvidenceRef,
    results_jsonl: EvidenceRef,
    scorecard: EvidenceRef,
    candidate_artifact_manifest: EvidenceRef,
    required_lane_manifests: Vec<LaneManifestRef>,
}

#[derive(Debug)]
struct CertificateRunConfig {
    workspace_root: PathBuf,
    evidence_root: PathBuf,
    evidence_json: PathBuf,
    candidate_git_sha: String,
    output_dir: PathBuf,
}

#[derive(Debug, Deserialize, Serialize)]
struct CertificateBundleManifest {
    schema_version: String,
    candidate_git_sha: String,
    input_path: String,
    input_sha256: String,
    certificate_sha256: String,
    summary_sha256: String,
}

#[derive(Debug)]
struct LoadedEvidence<T> {
    workspace_relative_path: String,
    sha256: String,
    value: T,
}

#[derive(Debug)]
struct Config {
    workspace_root: PathBuf,
    input_json: PathBuf,
    output_json: PathBuf,
    output_human: PathBuf,
}

impl Config {
    fn parse(args: &[String]) -> Result<Self, String> {
        let mut workspace_root = default_workspace_root();
        let mut input_json: Option<PathBuf> = None;
        let mut output_dir: Option<PathBuf> = None;
        let mut output_json: Option<PathBuf> = None;
        let mut output_human: Option<PathBuf> = None;

        let mut index = 0_usize;
        while let Some(arg) = args.get(index) {
            match arg.as_str() {
                "--workspace-root" => {
                    index += 1;
                    workspace_root = PathBuf::from(required_arg(args, index, "--workspace-root")?);
                }
                "--input-json" => {
                    index += 1;
                    input_json = Some(PathBuf::from(required_arg(args, index, "--input-json")?));
                }
                "--output-dir" => {
                    index += 1;
                    output_dir = Some(PathBuf::from(required_arg(args, index, "--output-dir")?));
                }
                "--output-json" => {
                    index += 1;
                    output_json = Some(PathBuf::from(required_arg(args, index, "--output-json")?));
                }
                "--output-human" => {
                    index += 1;
                    output_human =
                        Some(PathBuf::from(required_arg(args, index, "--output-human")?));
                }
                "-h" | "--help" => {
                    print_help();
                    return Err(String::new());
                }
                unknown => return Err(format!("unknown argument: {unknown}")),
            }
            index += 1;
        }

        let output_dir = output_dir.unwrap_or_else(|| workspace_root.join(DEFAULT_OUTPUT_DIR));
        let input_json = input_json.ok_or_else(|| "--input-json is required".to_owned())?;
        let output_json =
            output_json.unwrap_or_else(|| output_dir.join("parity_verification_workflow.json"));
        let output_human =
            output_human.unwrap_or_else(|| output_dir.join("parity_verification_workflow.md"));

        Ok(Self {
            workspace_root,
            input_json,
            output_json,
            output_human,
        })
    }
}

fn print_help() {
    let help = "\
parity_verification_workflow_runner -- user-facing parity workflow navigator (bd-2yqp6.7.8)

USAGE:
    cargo run -p fsqlite-harness --bin parity_verification_workflow_runner -- --input-json <PATH> [OPTIONS]
    cargo run -p fsqlite-harness --bin parity_verification_workflow_runner -- \\
        --certificate-evidence-json <PATH> --candidate-git-sha <SHA> \\
        --certificate-output-dir <NEW_PATH>

OPTIONS:
    --workspace-root <PATH>   Workspace root (default: current checkout)
    --input-json <PATH>       Workflow observation JSON from the one-command wrapper
    --output-dir <PATH>       Output directory (default: artifacts/parity-verification-workflow)
    --output-json <PATH>      JSON workflow report path
    --output-human <PATH>     Markdown workflow navigator path
    --certificate-evidence-json <PATH>
                              Strict final-candidate evidence bundle (exclusive mode)
    --certificate-evidence-root <PATH>
                              Existing workspace-contained root for every evidence input
    --candidate-git-sha <SHA> Candidate SHA; must equal checked-out HEAD
    --certificate-output-dir <PATH>
                              New directory, published atomically with certificate.json,
                              certificate.md, and bundle-manifest.json
    -h, --help                Show this help
";
    println!("{help}");
}

fn required_arg<'a>(args: &'a [String], index: usize, flag: &str) -> Result<&'a str, String> {
    args.get(index)
        .map(String::as_str)
        .ok_or_else(|| format!("{flag} requires a value"))
}

fn default_workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .unwrap_or_else(|_| PathBuf::from("."))
}

fn resolve_path(workspace_root: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        workspace_root.join(path)
    }
}

fn read_input(config: &Config) -> Result<WorkflowInput, String> {
    let input_path = resolve_path(&config.workspace_root, &config.input_json);
    let payload = fs::read_to_string(&input_path).map_err(|error| {
        format!(
            "workflow_input_read_failed path={} error={error}",
            input_path.display()
        )
    })?;
    serde_json::from_str(&payload).map_err(|error| {
        format!(
            "workflow_input_parse_failed path={} error={error}",
            input_path.display()
        )
    })
}

fn sha256_file(path: &Path) -> Result<String, String> {
    let payload = fs::read(path)
        .map_err(|error| format!("artifact_read_failed path={} error={error}", path.display()))?;
    let mut hasher = Sha256::new();
    hasher.update(payload);
    Ok(fsqlite_harness::bytes_to_lower_hex(hasher.finalize()))
}

fn sha256_bytes(payload: &[u8]) -> String {
    fsqlite_harness::bytes_to_lower_hex(Sha256::digest(payload))
}

fn enrich_artifact_hashes(config: &Config, input: &mut WorkflowInput) -> Result<(), String> {
    for artifact in &mut input.artifacts {
        if !artifact.sha256.trim().is_empty() {
            continue;
        }
        let path = resolve_path(&config.workspace_root, Path::new(&artifact.path));
        artifact.sha256 = sha256_file(&path)?;
    }
    Ok(())
}

fn write_text(path: &Path, payload: &str) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|error| {
            format!(
                "output_parent_create_failed path={} error={error}",
                parent.display()
            )
        })?;
    }
    fs::write(path, payload)
        .map_err(|error| format!("output_write_failed path={} error={error}", path.display()))
}

fn parse_certificate_config(args: &[String]) -> Result<CertificateRunConfig, String> {
    let mut workspace_root = default_workspace_root();
    let mut evidence_json = None;
    let mut candidate_git_sha = None;
    let mut evidence_root = None;
    let mut output_dir = None;
    let mut index = 0_usize;

    while let Some(arg) = args.get(index) {
        match arg.as_str() {
            "--workspace-root" => {
                index += 1;
                workspace_root = PathBuf::from(required_arg(args, index, "--workspace-root")?);
            }
            "--certificate-evidence-json" => {
                index += 1;
                evidence_json = Some(PathBuf::from(required_arg(
                    args,
                    index,
                    "--certificate-evidence-json",
                )?));
            }
            "--candidate-git-sha" => {
                index += 1;
                candidate_git_sha =
                    Some(required_arg(args, index, "--candidate-git-sha")?.to_owned());
            }
            "--certificate-evidence-root" => {
                index += 1;
                evidence_root = Some(PathBuf::from(required_arg(
                    args,
                    index,
                    "--certificate-evidence-root",
                )?));
            }
            "--certificate-output-dir" => {
                index += 1;
                output_dir = Some(PathBuf::from(required_arg(
                    args,
                    index,
                    "--certificate-output-dir",
                )?));
            }
            "--input-json"
            | "--output-dir"
            | "--output-json"
            | "--output-human"
            | "--certificate-output-json"
            | "--certificate-output-human" => {
                return Err("certificate mode is mutually exclusive with workflow mode".to_owned());
            }
            "-h" | "--help" => {
                print_help();
                return Err(String::new());
            }
            unknown => return Err(format!("unknown certificate argument: {unknown}")),
        }
        index += 1;
    }

    Ok(CertificateRunConfig {
        workspace_root,
        evidence_json: evidence_json
            .ok_or_else(|| "--certificate-evidence-json is required".to_owned())?,
        evidence_root: evidence_root
            .ok_or_else(|| "--certificate-evidence-root is required".to_owned())?,
        candidate_git_sha: candidate_git_sha
            .ok_or_else(|| "--candidate-git-sha is required".to_owned())?,
        output_dir: output_dir.ok_or_else(|| "--certificate-output-dir is required".to_owned())?,
    })
}

fn is_lower_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn is_lower_git_sha(value: &str) -> bool {
    value.len() == 40
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn current_unix_ms() -> Result<u128, String> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .map_err(|error| format!("system clock before UNIX epoch: {error}"))
}

fn canonical_workspace_path(workspace_root: &Path, path: &Path) -> Result<PathBuf, String> {
    let root = workspace_root
        .canonicalize()
        .map_err(|error| format!("workspace_root_canonicalize_failed: {error}"))?;
    let candidate = resolve_path(&root, path).canonicalize().map_err(|error| {
        format!(
            "evidence_path_canonicalize_failed path={}: {error}",
            path.display()
        )
    })?;
    if !candidate.starts_with(&root) {
        return Err(format!(
            "evidence_path_outside_workspace path={}",
            candidate.display()
        ));
    }
    Ok(candidate)
}

fn canonical_workspace_relative_path(workspace_root: &Path, path: &Path) -> Result<String, String> {
    let root = workspace_root
        .canonicalize()
        .map_err(|error| format!("workspace_root_canonicalize_failed: {error}"))?;
    let canonical = canonical_workspace_path(&root, path)?;
    canonical
        .strip_prefix(root)
        .map_err(|error| format!("workspace_relative_path_failed: {error}"))
        .map(|relative| relative.to_string_lossy().into_owned())
}

fn canonical_evidence_path(
    workspace_root: &Path,
    evidence_root: &Path,
    path: &Path,
) -> Result<PathBuf, String> {
    let canonical_root = workspace_root
        .canonicalize()
        .map_err(|error| format!("workspace_root_canonicalize_failed: {error}"))?;
    let canonical_evidence_root = canonical_workspace_path(&canonical_root, evidence_root)?;
    if !canonical_evidence_root.starts_with(&canonical_root) {
        return Err("evidence_root_outside_workspace".to_owned());
    }
    let canonical_path = canonical_workspace_path(&canonical_root, path)?;
    if !canonical_path.starts_with(&canonical_evidence_root) {
        return Err(format!(
            "evidence_path_outside_evidence_root path={}",
            canonical_path.display()
        ));
    }
    Ok(canonical_path)
}

fn validate_evidence_timestamps(
    reference: &EvidenceRef,
    generated_unix_ms: u128,
    now_unix_ms: u128,
) -> Result<(), String> {
    if reference.observed_unix_ms > generated_unix_ms {
        return Err(format!(
            "evidence_observed_after_bundle_generation path={}",
            reference.path
        ));
    }
    if generated_unix_ms > now_unix_ms {
        return Err("certificate_input_generated_in_future".to_owned());
    }
    let age = now_unix_ms
        .checked_sub(reference.observed_unix_ms)
        .ok_or_else(|| format!("evidence_timestamp_in_future path={}", reference.path))?;
    if age > MAX_EVIDENCE_AGE_MS {
        return Err(format!(
            "stale_evidence path={} age_ms={age}",
            reference.path
        ));
    }
    Ok(())
}

fn read_evidence_bytes(
    workspace_root: &Path,
    evidence_root: &Path,
    reference: &EvidenceRef,
    generated_unix_ms: u128,
    now_unix_ms: u128,
) -> Result<(PathBuf, Vec<u8>), String> {
    if !is_lower_sha256(&reference.sha256) {
        return Err(format!("invalid_evidence_sha256 path={}", reference.path));
    }
    validate_evidence_timestamps(reference, generated_unix_ms, now_unix_ms)?;
    let path = canonical_evidence_path(workspace_root, evidence_root, Path::new(&reference.path))?;
    let payload = fs::read(&path)
        .map_err(|error| format!("evidence_read_failed path={}: {error}", path.display()))?;
    let observed = sha256_bytes(&payload);
    if observed != reference.sha256 {
        return Err(format!("evidence_hash_mismatch path={}", path.display()));
    }
    Ok((path, payload))
}

fn load_evidence<T: serde::de::DeserializeOwned>(
    workspace_root: &Path,
    evidence_root: &Path,
    reference: &EvidenceRef,
    generated_unix_ms: u128,
    now_unix_ms: u128,
) -> Result<LoadedEvidence<T>, String> {
    let (path, payload) = read_evidence_bytes(
        workspace_root,
        evidence_root,
        reference,
        generated_unix_ms,
        now_unix_ms,
    )?;
    let value = serde_json::from_slice(&payload)
        .map_err(|error| format!("evidence_parse_failed path={}: {error}", path.display()))?;
    Ok(LoadedEvidence {
        workspace_relative_path: canonical_workspace_relative_path(workspace_root, &path)?,
        sha256: sha256_bytes(&payload),
        value,
    })
}

fn load_raw_evidence(
    workspace_root: &Path,
    evidence_root: &Path,
    reference: &EvidenceRef,
    generated_unix_ms: u128,
    now_unix_ms: u128,
) -> Result<LoadedEvidence<Vec<u8>>, String> {
    let (path, payload) = read_evidence_bytes(
        workspace_root,
        evidence_root,
        reference,
        generated_unix_ms,
        now_unix_ms,
    )?;
    Ok(LoadedEvidence {
        workspace_relative_path: canonical_workspace_relative_path(workspace_root, &path)?,
        sha256: sha256_bytes(&payload),
        value: payload,
    })
}

fn validate_results_jsonl(payload: &[u8], candidate_git_sha: &str) -> Result<(), String> {
    let text =
        std::str::from_utf8(payload).map_err(|error| format!("results_jsonl_not_utf8: {error}"))?;
    let mut record_count = 0_usize;
    for (line_number, line) in text.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let value: serde_json::Value = serde_json::from_str(line)
            .map_err(|error| format!("results_jsonl_parse_failed line={line_number}: {error}"))?;
        let passed = value.get("passed").and_then(serde_json::Value::as_bool);
        let git_sha = value
            .get("candidate_git_sha")
            .and_then(serde_json::Value::as_str);
        if passed != Some(true) || git_sha != Some(candidate_git_sha) {
            return Err(format!("results_jsonl_not_green line={line_number}"));
        }
        record_count += 1;
    }
    if record_count == 0 {
        return Err("results_jsonl_empty".to_owned());
    }
    Ok(())
}

fn validate_certification_policy(policy: &CertificationPolicy) -> Result<(), String> {
    if policy.schema_version != CERTIFICATION_POLICY_SCHEMA_VERSION
        || policy.policy_id != CERTIFICATION_POLICY_ID
        || policy.min_verification_pct != 100.0
        || policy.required_suite_pass_rate_pct != 100.0
        || policy.max_high_severity_counterexamples != 0
        || policy.max_evidence_age_hours != 24
        || policy.gates.iter().any(|gate| !gate.blocking)
        || policy.ratchets.iter().any(|ratchet| !ratchet.blocking)
    {
        return Err("certification_policy_not_strict_release_policy".to_owned());
    }
    Ok(())
}

fn validate_scorecard(scorecard: &BayesianScorecard) -> Result<(), String> {
    let contract = scorecard
        .verification_contract
        .as_ref()
        .ok_or_else(|| "scorecard_missing_verification_contract".to_owned())?;
    if !scorecard.release_ready
        || scorecard.global_lower_bound < scorecard.release_threshold
        || !contract.contract_passed
        || !contract.final_gate_passed
        || contract.missing_evidence_beads != 0
        || contract.invalid_reference_beads != 0
    {
        return Err("scorecard_not_release_ready".to_owned());
    }
    Ok(())
}

fn validate_critical_path(report: &NoMockCriticalPathReport) -> Result<(), String> {
    if report.verdict != NoMockVerdict::Pass
        || report.total_critical_invariants == 0
        || report.real_evidence_count != report.total_critical_invariants
        || report.exception_count != 0
        || report.missing_evidence_count != 0
        || report.blocking_count != 0
        || !report.violations.is_empty()
    {
        return Err("critical_path_evidence_not_green".to_owned());
    }
    Ok(())
}

fn validate_manifest_artifact(
    workspace_root: &Path,
    path: &str,
    sha256: &str,
) -> Result<(), String> {
    if !is_lower_sha256(sha256) {
        return Err(format!("invalid_manifest_artifact_sha256 path={path}"));
    }
    let canonical = canonical_workspace_path(workspace_root, Path::new(path))?;
    if sha256_file(&canonical)? != sha256 {
        return Err(format!(
            "manifest_artifact_hash_mismatch path={}",
            canonical.display()
        ));
    }
    Ok(())
}

fn validate_candidate_manifest(
    workspace_root: &Path,
    manifest: &ArtifactManifest,
    candidate_git_sha: &str,
) -> Result<(), String> {
    let errors = manifest.validate();
    if !errors.is_empty() {
        return Err(format!("artifact_manifest_invalid: {}", errors.join("; ")));
    }
    if !manifest.gate_passed || manifest.git_sha != candidate_git_sha {
        return Err("artifact_manifest_not_passing_for_candidate".to_owned());
    }
    let contract = manifest
        .verification_contract
        .as_ref()
        .ok_or_else(|| "artifact_manifest_missing_verification_contract".to_owned())?;
    if !contract.contract_passed
        || !contract.final_gate_passed
        || contract.missing_evidence_beads != 0
        || contract.invalid_reference_beads != 0
    {
        return Err("verification_contract_not_release_ready".to_owned());
    }
    let g9 = manifest
        .fallback_transparency_gate
        .as_ref()
        .ok_or_else(|| "artifact_manifest_missing_g9_gate".to_owned())?;
    if !g9.gate_passed()
        || g9.source_commit != candidate_git_sha
        || g9.backend_identity_summary != "fsqlite:pager_wal_mvcc_btree:parity_cert_strict"
        || !g9.missing_boundary_ids.is_empty()
        || !g9.stale_artifacts.is_empty()
        || g9.certifying_fallback_events != 0
        || !g9.gate_failures.is_empty()
    {
        return Err("g9_gate_not_release_ready".to_owned());
    }
    validate_manifest_artifact(
        workspace_root,
        &g9.inventory.path,
        &g9.inventory.content_hash,
    )?;
    validate_manifest_artifact(
        workspace_root,
        &g9.schema_validation.path,
        &g9.schema_validation.content_hash,
    )?;
    validate_manifest_artifact(
        workspace_root,
        &g9.replay_bundle.path,
        &g9.replay_bundle.content_hash,
    )?;
    for artifact in &manifest.artifacts {
        validate_manifest_artifact(workspace_root, &artifact.path, &artifact.content_hash)?;
    }
    Ok(())
}

fn validate_manifest_binding<T>(
    workspace_root: &Path,
    manifest: &ArtifactManifest,
    evidence: &LoadedEvidence<T>,
) -> Result<(), String> {
    let is_bound = manifest.artifacts.iter().any(|artifact| {
        canonical_workspace_relative_path(workspace_root, Path::new(&artifact.path))
            .is_ok_and(|path| path == evidence.workspace_relative_path)
            && artifact.content_hash == evidence.sha256
    });
    if !is_bound {
        return Err(format!(
            "mandatory_evidence_not_bound_by_candidate_manifest path={}",
            evidence.workspace_relative_path
        ));
    }
    Ok(())
}

fn validate_required_lanes(
    workspace_root: &Path,
    evidence_root: &Path,
    candidate_manifest: &ArtifactManifest,
    lanes: &[LaneManifestRef],
    generated_unix_ms: u128,
    now_unix_ms: u128,
    candidate_git_sha: &str,
) -> Result<(), String> {
    if lanes.len() != REQUIRED_CERTIFICATION_LANES.len() {
        return Err("required_lane_manifest_count_mismatch".to_owned());
    }
    let mut seen = std::collections::BTreeSet::new();
    for lane in lanes {
        let manifest: LoadedEvidence<ArtifactManifest> = load_evidence(
            workspace_root,
            evidence_root,
            &lane.evidence,
            generated_unix_ms,
            now_unix_ms,
        )?;
        validate_manifest_binding(workspace_root, candidate_manifest, &manifest)?;
        if lane.lane != manifest.value.lane {
            return Err(format!("required_lane_name_mismatch lane={}", lane.lane));
        }
        validate_candidate_manifest(workspace_root, &manifest.value, candidate_git_sha).map_err(
            |error| {
                format!(
                    "required_lane_not_release_ready lane={}: {error}",
                    lane.lane
                )
            },
        )?;
        if manifest.value.git_sha != candidate_git_sha {
            return Err(format!("required_lane_not_passing lane={}", lane.lane));
        }
        seen.insert(lane.lane.as_str());
    }
    if REQUIRED_CERTIFICATION_LANES
        .iter()
        .any(|lane| !seen.contains(lane))
    {
        return Err("required_certification_lane_missing".to_owned());
    }
    Ok(())
}

fn current_head(workspace_root: &Path) -> Result<String, String> {
    let output = Command::new("git")
        .arg("-C")
        .arg(workspace_root)
        .args(["rev-parse", "HEAD"])
        .output()
        .map_err(|error| format!("git_head_query_failed: {error}"))?;
    if !output.status.success() {
        return Err("git_head_query_failed".to_owned());
    }
    String::from_utf8(output.stdout)
        .map_err(|error| format!("git_head_not_utf8: {error}"))
        .map(|value| value.trim().to_owned())
}

fn require_clean_tracked_checkout(workspace_root: &Path) -> Result<(), String> {
    let output = Command::new("git")
        .arg("-C")
        .arg(workspace_root)
        .args(["status", "--porcelain=v1", "--untracked-files=no"])
        .output()
        .map_err(|error| format!("git_cleanliness_query_failed: {error}"))?;
    if !output.status.success() {
        return Err("git_cleanliness_query_failed".to_owned());
    }
    if !output.stdout.is_empty() {
        return Err("candidate_checkout_has_tracked_changes".to_owned());
    }
    Ok(())
}

fn prepare_certificate_output_dir(
    workspace_root: &Path,
    output_dir: &Path,
) -> Result<(PathBuf, PathBuf), String> {
    let output_path = resolve_path(workspace_root, output_dir);
    if fs::symlink_metadata(&output_path).is_ok() {
        return Err(format!(
            "refusing_to_overwrite_output_directory path={}",
            output_path.display()
        ));
    }
    let parent = output_path.parent().ok_or_else(|| {
        format!(
            "output_directory_has_no_parent path={}",
            output_path.display()
        )
    })?;
    if !parent.exists() {
        return Err(format!("output_parent_missing path={}", parent.display()));
    }
    let canonical_root = workspace_root
        .canonicalize()
        .map_err(|error| format!("workspace_root_canonicalize_failed: {error}"))?;
    let canonical_parent = parent
        .canonicalize()
        .map_err(|error| format!("output_parent_canonicalize_failed: {error}"))?;
    if !canonical_parent.starts_with(&canonical_root) {
        return Err(format!(
            "output_parent_outside_workspace path={}",
            parent.display()
        ));
    }
    let output_name = output_path.file_name().ok_or_else(|| {
        format!(
            "output_directory_has_no_name path={}",
            output_path.display()
        )
    })?;
    let output_path = canonical_parent.join(output_name);
    let staging_name = format!(
        ".{}.certificate-staging-{}-{}",
        output_name.to_string_lossy(),
        std::process::id(),
        current_unix_ms()?
    );
    let staging_path = canonical_parent.join(staging_name);
    fs::create_dir(&staging_path).map_err(|error| {
        format!(
            "certificate_staging_create_failed path={}: {error}",
            staging_path.display()
        )
    })?;
    Ok((output_path, staging_path))
}

fn write_new_bundle_file(path: &Path, payload: &[u8]) -> Result<(), String> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|error| format!("output_create_failed path={}: {error}", path.display()))?;
    use std::io::Write;
    file.write_all(payload)
        .map_err(|error| format!("output_write_failed path={}: {error}", path.display()))
}

fn publish_certificate_bundle(
    workspace_root: &Path,
    output_dir: &Path,
    candidate_git_sha: &str,
    input_path: &str,
    input_bytes: &[u8],
    certificate_json: &str,
    summary_markdown: &str,
) -> Result<(), String> {
    let (final_path, staging_path) = prepare_certificate_output_dir(workspace_root, output_dir)?;
    let certificate_bytes = certificate_json.as_bytes();
    let summary_bytes = summary_markdown.as_bytes();
    write_new_bundle_file(&staging_path.join("certificate.json"), certificate_bytes)?;
    write_new_bundle_file(&staging_path.join("certificate.md"), summary_bytes)?;
    let bundle_manifest = CertificateBundleManifest {
        schema_version: "fsqlite.release_certificate_bundle.v1".to_owned(),
        candidate_git_sha: candidate_git_sha.to_owned(),
        input_path: input_path.to_owned(),
        input_sha256: sha256_bytes(input_bytes),
        certificate_sha256: sha256_bytes(certificate_bytes),
        summary_sha256: sha256_bytes(summary_bytes),
    };
    let manifest_bytes = serde_json::to_vec_pretty(&bundle_manifest)
        .map_err(|error| format!("certificate_bundle_manifest_serialize_failed: {error}"))?;
    write_new_bundle_file(&staging_path.join("bundle-manifest.json"), &manifest_bytes)?;
    fs::rename(&staging_path, &final_path).map_err(|error| {
        format!(
            "certificate_bundle_publish_rename_failed staging={} final={}: {error}",
            staging_path.display(),
            final_path.display()
        )
    })
}

fn run_certificate(args: &[String]) -> Result<i32, String> {
    let config = parse_certificate_config(args)?;
    if !is_lower_git_sha(&config.candidate_git_sha) {
        return Err("candidate_git_sha_must_be_lowercase_40_hex".to_owned());
    }
    if current_head(&config.workspace_root)? != config.candidate_git_sha {
        return Err("candidate_git_sha_does_not_match_checked_out_head".to_owned());
    }
    require_clean_tracked_checkout(&config.workspace_root)?;
    let evidence_path = canonical_evidence_path(
        &config.workspace_root,
        &config.evidence_root,
        &config.evidence_json,
    )?;
    let input_bytes = fs::read(&evidence_path)
        .map_err(|error| format!("certificate_input_read_failed: {error}"))?;
    let input: CertificateEvidenceInput = serde_json::from_slice(&input_bytes)
        .map_err(|error| format!("certificate_input_parse_failed: {error}"))?;
    let now = current_unix_ms()?;
    if input.schema_version != CERTIFICATE_INPUT_SCHEMA
        || input.candidate_git_sha != config.candidate_git_sha
        || input.generated_unix_ms > now
        || now
            .checked_sub(input.generated_unix_ms)
            .is_none_or(|age| age > MAX_EVIDENCE_AGE_MS)
    {
        return Err("certificate_input_not_current_candidate_evidence".to_owned());
    }

    let gate_report: LoadedEvidence<GateReport> = load_evidence(
        &config.workspace_root,
        &config.evidence_root,
        &input.gate_report,
        input.generated_unix_ms,
        now,
    )?;
    if !gate_report.value.release_ready
        || gate_report.value.global_decision != GateDecision::Pass
        || gate_report.value.global_verification_pct != 100.0
    {
        return Err("confidence_gate_not_release_ready".to_owned());
    }
    let ranking: LoadedEvidence<ExpectedLossRanking> = load_evidence(
        &config.workspace_root,
        &config.evidence_root,
        &input.expected_loss_ranking,
        input.generated_unix_ms,
        now,
    )?;
    let ledger: LoadedEvidence<EvidenceLedger> = load_evidence(
        &config.workspace_root,
        &config.evidence_root,
        &input.evidence_ledger,
        input.generated_unix_ms,
        now,
    )?;
    let drift: LoadedEvidence<ParityDriftSnapshot> = load_evidence(
        &config.workspace_root,
        &config.evidence_root,
        &input.drift_snapshot,
        input.generated_unix_ms,
        now,
    )?;
    if drift.value.any_rejected || drift.value.any_drift {
        return Err("drift_snapshot_not_green".to_owned());
    }
    let campaign: LoadedEvidence<CampaignResult> = load_evidence(
        &config.workspace_root,
        &config.evidence_root,
        &input.adversarial_campaign,
        input.generated_unix_ms,
        now,
    )?;
    if !campaign.value.passed {
        return Err("adversarial_campaign_not_green".to_owned());
    }
    let flake_budget: LoadedEvidence<GlobalFlakeBudgetResult> = load_evidence(
        &config.workspace_root,
        &config.evidence_root,
        &input.ci_flake_budget,
        input.generated_unix_ms,
        now,
    )?;
    if !flake_budget.value.within_budget || !flake_budget.value.pipeline_pass {
        return Err("ci_flake_budget_not_green".to_owned());
    }
    let policy: LoadedEvidence<CertificationPolicy> = load_evidence(
        &config.workspace_root,
        &config.evidence_root,
        &input.certification_policy,
        input.generated_unix_ms,
        now,
    )?;
    validate_certification_policy(&policy.value)?;
    let ratchet_baseline: LoadedEvidence<CertificationRatchetBaseline> = load_evidence(
        &config.workspace_root,
        &config.evidence_root,
        &input.ratchet_baseline,
        input.generated_unix_ms,
        now,
    )?;
    if ratchet_baseline.value.schema_version != CERTIFICATION_POLICY_SCHEMA_VERSION
        || ratchet_baseline.value.policy_id != CERTIFICATION_POLICY_ID
    {
        return Err("ratchet_baseline_not_strict_policy".to_owned());
    }
    let ratchet_candidate: LoadedEvidence<CertificationRatchetCandidate> = load_evidence(
        &config.workspace_root,
        &config.evidence_root,
        &input.ratchet_candidate,
        input.generated_unix_ms,
        now,
    )?;
    let critical_path: LoadedEvidence<NoMockCriticalPathReport> = load_evidence(
        &config.workspace_root,
        &config.evidence_root,
        &input.critical_path_evidence,
        input.generated_unix_ms,
        now,
    )?;
    validate_critical_path(&critical_path.value)?;
    let results_jsonl = load_raw_evidence(
        &config.workspace_root,
        &config.evidence_root,
        &input.results_jsonl,
        input.generated_unix_ms,
        now,
    )?;
    validate_results_jsonl(&results_jsonl.value, &config.candidate_git_sha)?;
    let scorecard: LoadedEvidence<BayesianScorecard> = load_evidence(
        &config.workspace_root,
        &config.evidence_root,
        &input.scorecard,
        input.generated_unix_ms,
        now,
    )?;
    validate_scorecard(&scorecard.value)?;
    let manifest: LoadedEvidence<ArtifactManifest> = load_evidence(
        &config.workspace_root,
        &config.evidence_root,
        &input.candidate_artifact_manifest,
        input.generated_unix_ms,
        now,
    )?;
    validate_candidate_manifest(
        &config.workspace_root,
        &manifest.value,
        &config.candidate_git_sha,
    )?;
    validate_manifest_binding(&config.workspace_root, &manifest.value, &gate_report)?;
    validate_manifest_binding(&config.workspace_root, &manifest.value, &ranking)?;
    validate_manifest_binding(&config.workspace_root, &manifest.value, &ledger)?;
    validate_manifest_binding(&config.workspace_root, &manifest.value, &drift)?;
    validate_manifest_binding(&config.workspace_root, &manifest.value, &campaign)?;
    validate_manifest_binding(&config.workspace_root, &manifest.value, &flake_budget)?;
    validate_manifest_binding(&config.workspace_root, &manifest.value, &policy)?;
    validate_manifest_binding(&config.workspace_root, &manifest.value, &ratchet_baseline)?;
    validate_manifest_binding(&config.workspace_root, &manifest.value, &ratchet_candidate)?;
    validate_manifest_binding(&config.workspace_root, &manifest.value, &critical_path)?;
    validate_manifest_binding(&config.workspace_root, &manifest.value, &results_jsonl)?;
    validate_manifest_binding(&config.workspace_root, &manifest.value, &scorecard)?;
    validate_required_lanes(
        &config.workspace_root,
        &config.evidence_root,
        &manifest.value,
        &input.required_lane_manifests,
        input.generated_unix_ms,
        now,
        &config.candidate_git_sha,
    )?;

    let catalog = build_canonical_catalog();
    let traceability = catalog.release_traceability();
    let manifest_artifact_paths: Result<std::collections::BTreeSet<_>, _> = manifest
        .value
        .artifacts
        .iter()
        .map(|artifact| {
            canonical_workspace_relative_path(&config.workspace_root, Path::new(&artifact.path))
        })
        .collect();
    let manifest_artifact_paths = manifest_artifact_paths?;
    if !traceability.release_ready
        || traceability.entries.iter().any(|entry| {
            entry.artifact_refs.iter().any(|reference| {
                canonical_workspace_relative_path(&config.workspace_root, Path::new(reference))
                    .map_or(true, |path| !manifest_artifact_paths.contains(&path))
            })
        })
    {
        return Err("certificate_traceability_not_fully_linked".to_owned());
    }
    let expected_ratchet_candidate = CertificationRatchetCandidate {
        global_lower_bound: gate_report.value.global_lower_bound,
        category_lower_bounds: gate_report
            .value
            .category_results
            .iter()
            .map(|(category, result)| (category.clone(), result.credible_lower))
            .collect(),
        required_suite_pass_rate_pct: 100.0,
        traceability_link_coverage_pct: traceability.verification_pct,
    };
    if ratchet_candidate.value.global_lower_bound != expected_ratchet_candidate.global_lower_bound
        || ratchet_candidate.value.category_lower_bounds
            != expected_ratchet_candidate.category_lower_bounds
        || ratchet_candidate.value.required_suite_pass_rate_pct
            != expected_ratchet_candidate.required_suite_pass_rate_pct
        || ratchet_candidate.value.traceability_link_coverage_pct
            != expected_ratchet_candidate.traceability_link_coverage_pct
        || !evaluate_certification_ratchets(&ratchet_baseline.value, &ratchet_candidate.value)
            .passed
    {
        return Err("certification_ratchet_not_preserved".to_owned());
    }
    let certificate = build_certificate(
        &CertificateInputs {
            gate_report: gate_report.value,
            expected_loss_ranking: ranking.value,
            evidence_ledger: ledger.value,
            catalog_stats: catalog.stats(),
            traceability,
            drift_snapshot: drift.value,
            campaign_result: campaign.value,
            ci_flake_budget: Some(flake_budget.value),
            artifact_manifest: Some(manifest.value),
        },
        &CertificateConfig::default(),
    );
    if certificate.verdict != CertificateVerdict::Approved {
        return Err(format!(
            "certificate_not_approved verdict={}",
            certificate.verdict
        ));
    }
    let json = certificate
        .to_json()
        .map_err(|error| format!("certificate_serialize_failed: {error}"))?;
    let markdown = format!(
        "# FrankenSQLite release certificate\n\n{}\n",
        certificate.summary
    );
    let input_path = canonical_workspace_relative_path(&config.workspace_root, &evidence_path)?;
    publish_certificate_bundle(
        &config.workspace_root,
        &config.output_dir,
        &config.candidate_git_sha,
        &input_path,
        &input_bytes,
        &json,
        &markdown,
    )?;
    println!(
        "INFO release_certificate_bundle_written output_dir={}",
        config.output_dir.display()
    );
    Ok(0)
}

fn run(args: &[String]) -> Result<i32, String> {
    if args.iter().any(|argument| {
        matches!(
            argument.as_str(),
            "--certificate-evidence-json"
                | "--certificate-evidence-root"
                | "--candidate-git-sha"
                | "--certificate-output-dir"
                | "--certificate-output-json"
                | "--certificate-output-human"
        )
    }) {
        return run_certificate(args);
    }
    let config = Config::parse(args)?;
    let mut input = read_input(&config)?;
    enrich_artifact_hashes(&config, &mut input)?;
    let report = build_workflow_report(input);
    let json = serde_json::to_string_pretty(&report)
        .map_err(|error| format!("workflow_report_serialize_failed: {error}"))?;
    let markdown = render_workflow_markdown(&report);

    write_text(&config.output_json, &json)?;
    write_text(&config.output_human, &markdown)?;

    println!(
        "INFO parity_verification_workflow_written bead_id={BEAD_ID} path={} human_path={} workflow_complete={} certificate_ready={} violations={}",
        config.output_json.display(),
        config.output_human.display(),
        report.workflow_complete,
        report.certificate_ready,
        report.validation_violations.len(),
    );

    Ok(i32::from(!report.workflow_complete))
}

#[cfg(test)]
mod certificate_tests {
    use super::*;

    #[test]
    fn certificate_evidence_timestamps_reject_stale_and_future_inputs() {
        let reference = EvidenceRef {
            path: "evidence.json".to_owned(),
            sha256: "a".repeat(64),
            observed_unix_ms: 100,
        };
        assert!(validate_evidence_timestamps(&reference, 100, 100 + MAX_EVIDENCE_AGE_MS).is_ok());
        assert!(validate_evidence_timestamps(&reference, 99, 100).is_err());
        assert!(validate_evidence_timestamps(&reference, 100, 101 + MAX_EVIDENCE_AGE_MS).is_err());
    }

    #[test]
    fn evidence_loader_rejects_tampering_after_hash_declaration() {
        let temp_dir = tempfile::tempdir().expect("temporary evidence root");
        let workspace_root = temp_dir.path().join("workspace");
        let evidence_root = workspace_root.join("evidence");
        fs::create_dir_all(&evidence_root).expect("create evidence root");
        let evidence_path = evidence_root.join("gate.json");
        let original = b"{\"passed\":true}\n";
        fs::write(&evidence_path, original).expect("write evidence");
        let reference = EvidenceRef {
            path: "evidence/gate.json".to_owned(),
            sha256: sha256_bytes(original),
            observed_unix_ms: 10,
        };

        assert!(load_raw_evidence(&workspace_root, &evidence_root, &reference, 10, 10).is_ok());
        fs::write(&evidence_path, b"{\"passed\":false}\n").expect("tamper evidence");
        assert!(
            load_raw_evidence(&workspace_root, &evidence_root, &reference, 10, 10)
                .expect_err("tampered evidence must be rejected")
                .starts_with("evidence_hash_mismatch")
        );
    }

    #[test]
    fn results_jsonl_requires_candidate_bound_green_records() {
        let candidate = "a".repeat(40);
        let valid = format!("{{\"candidate_git_sha\":\"{candidate}\",\"passed\":true}}\n");
        assert!(validate_results_jsonl(valid.as_bytes(), &candidate).is_ok());
        assert!(
            validate_results_jsonl(
                b"{\"candidate_git_sha\":\"wrong\",\"passed\":true}\n",
                &candidate
            )
            .is_err()
        );
        assert!(
            validate_results_jsonl(
                b"{\"candidate_git_sha\":\"wrong\",\"passed\":false}\n",
                &candidate
            )
            .is_err()
        );
    }

    #[test]
    fn bundle_publication_creates_one_complete_directory() {
        let temp_dir = tempfile::tempdir().expect("temporary workspace");
        let root = temp_dir.path().join("workspace");
        fs::create_dir(&root).expect("create workspace");
        let output_dir = root.join("certificate-output");
        let candidate = "b".repeat(40);

        publish_certificate_bundle(
            &root,
            &output_dir,
            &candidate,
            "evidence/input.json",
            b"{\"input\":true}\n",
            "{\"certificate\":true}\n",
            "# Certificate\n",
        )
        .expect("publish certificate bundle");

        assert!(output_dir.join("certificate.json").is_file());
        assert!(output_dir.join("certificate.md").is_file());
        let manifest: CertificateBundleManifest = serde_json::from_slice(
            &fs::read(output_dir.join("bundle-manifest.json")).expect("read bundle manifest"),
        )
        .expect("parse bundle manifest");
        assert_eq!(manifest.candidate_git_sha, candidate);
        assert_eq!(
            manifest.certificate_sha256,
            sha256_bytes(b"{\"certificate\":true}\n")
        );
        assert_eq!(manifest.summary_sha256, sha256_bytes(b"# Certificate\n"));
    }

    #[test]
    fn bundle_publication_refuses_existing_directory_through_alias_path() {
        let temp_dir = tempfile::tempdir().expect("temporary workspace");
        let root = temp_dir.path().join("workspace");
        fs::create_dir_all(root.join("nested")).expect("create workspace");
        let existing = root.join("existing-output");
        fs::create_dir(&existing).expect("create existing output");
        let alias = root.join("nested").join("..").join("existing-output");

        let error = publish_certificate_bundle(
            &root,
            &alias,
            &"c".repeat(40),
            "evidence/input.json",
            b"{}",
            "{}",
            "# Certificate\n",
        )
        .expect_err("existing output alias must be refused");

        assert!(error.starts_with("refusing_to_overwrite_output_directory"));
        assert!(existing.is_dir(), "existing output must be preserved");
    }

    #[test]
    fn any_certificate_only_flag_selects_certificate_mode() {
        let error = run(&["--candidate-git-sha".to_owned()])
            .expect_err("certificate-only flag must not fall through to workflow mode");
        assert_eq!(error, "--certificate-evidence-json is required");
    }
}

fn main() -> ExitCode {
    let args: Vec<String> = env::args().skip(1).collect();
    match run(&args) {
        Ok(0) => ExitCode::SUCCESS,
        Ok(1) => ExitCode::from(1),
        Ok(_) => ExitCode::from(2),
        Err(error) if error.is_empty() => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!(
                "ERROR bead_id={BEAD_ID} parity_verification_workflow_runner failed: {error}"
            );
            ExitCode::from(2)
        }
    }
}
