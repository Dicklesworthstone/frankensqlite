//! Strict, signer-agnostic Phase-5 evidence producer.
//!
//! `--plan` is deliberately side-effect free. The capture path is fail closed:
//! every Cargo invocation is a direct `rch exec -- cargo …` proof-lane request,
//! and the generated manifest is only an input for the out-of-tree DSR signer.

use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Component, Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Output, Stdio};
use std::sync::Mutex;
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread;
use std::time::{Duration, Instant};

use fsqlite_harness::performance_release_admission::{
    AdmissionPackReference, ArtifactDigest, PerformanceAdmissionGate, PerformanceAdmissionPack,
    blocked_missing_authoritative_policy, validate_pack as validate_performance_admission_pack,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

const SCHEMA_VERSION: u32 = 3;
const DIGEST_ALGORITHM: &str = "blake3-256";
const SHA256_ALGORITHM: &str = "sha2-256";
const RCH_RECEIPT_SCHEMA: &str = "fsqlite.phase5.rch_execution_receipt.v1";
const EVIDENCE_ROOT: &str = "tests/artifacts/release-evidence";
const MANIFEST_NAME: &str = "manifest.json";
const BASELINE: &str = "tests/regression_baseline.json";
const GUARD_LOCATOR: &str = "crates/fsqlite-harness/tests/phase5_regression_guard.rs::phase5_regression_guard_full_workspace_against_baseline";
const MAX_CAPTURE_WORKERS: usize = 2;
const USAGE: &str = "usage: phase5_evidence_capture (--plan | --baseline-only --baseline-output-dir <absolute-external-dir> | --output <tests/artifacts/release-evidence/<commit>/manifest.json> --c1-pack-dir <absolute-external-dir> --persistent-release-pack-dir <absolute-external-dir> --persistent-release-perf-pack-dir <absolute-external-dir> [--performance-admission-pack-dir <absolute-external-dir>]) [--tested-commit <40-hex>]";

#[derive(Debug, Deserialize)]
struct Baseline {
    ignored_tests: Vec<IgnoredTest>,
}

#[derive(Debug, Deserialize)]
struct IgnoredTest {
    source_path: String,
    test_name: String,
    cfg_condition: Option<String>,
    policy: String,
    evidence: Requirement,
}

#[derive(Debug, Deserialize)]
struct Requirement {
    requirement: String,
}

impl IgnoredTest {
    fn locator(&self) -> String {
        format!("{}::{}", self.source_path, self.test_name)
    }
}

#[derive(Debug, Serialize)]
struct Manifest {
    schema_version: u32,
    tested_commit: String,
    signature_path: String,
    signer_attestation: EvidenceLeaf,
    cargo_lock: EvidenceLeaf,
    rust_toolchain: EvidenceLeaf,
    pre_capture_untracked: EvidenceLeaf,
    compiler_inventory_attestation: EvidenceLeaf,
    workspace: RunEvidence,
    run_receipts: Vec<RunReceipt>,
    auxiliary_scorecards: Scorecards,
    performance_regression_gate: PerformanceAdmissionGate,
    evidence_pack: Vec<EvidenceLeaf>,
}

#[derive(Debug, Clone, Serialize)]
struct RunEvidence {
    execution: CommandEvidence,
    runner_receipt: EvidenceLeaf,
}

#[derive(Debug, Serialize)]
struct RunReceipt {
    source_path: String,
    test_name: String,
    requirement_blake3: String,
    evidence: RunEvidence,
}

#[derive(Debug, Clone, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
struct EvidenceLeaf {
    path: String,
    digest_algorithm: String,
    digest: String,
}

#[derive(Debug, Serialize)]
struct Scorecards {
    c1: ScorecardEvidence,
    persistent: PersistentProfileScorecards,
}

#[derive(Debug, Serialize)]
struct PersistentProfileScorecards {
    release: ScorecardEvidence,
    release_perf: ScorecardEvidence,
}

#[derive(Debug, Serialize)]
struct ScorecardEvidence {
    scorecard: EvidenceLeaf,
    pack_manifest: EvidenceLeaf,
    commit_provenance: EvidenceLeaf,
}

#[derive(Debug, Clone, Serialize)]
struct CommandEvidence {
    argv: Vec<String>,
    exit_status: i32,
    stdout: StreamEvidence,
    stderr: StreamEvidence,
    transcript: EvidenceLeaf,
}

#[derive(Debug, Clone, Serialize)]
struct StreamEvidence {
    capture: String,
    leaf: EvidenceLeaf,
}

#[derive(Debug, Deserialize, Serialize)]
struct RchReceipt {
    schema_version: String,
    inner_cargo_argv: Vec<String>,
    job_id: String,
    active_status: EvidenceLeaf,
    completed_status: EvidenceLeaf,
}

#[derive(Debug, Serialize)]
struct CompilerInventory {
    tested_tree_blake3: String,
    cargo_metadata_blake3: String,
    target_mappings_blake3: String,
    active_identities_blake3: String,
    ignored_identities_blake3: String,
    doctest_identities_blake3: String,
    expanded_identities_blake3: String,
    cfg_profile: String,
    inventory_runs: CompilerInventoryRuns,
    targets: Vec<CompilerTarget>,
    inventory_leaves: Vec<InventoryLeaf>,
}

#[derive(Debug, Clone, Serialize)]
struct CompilerInventoryRuns {
    all_targets: RunEvidence,
    all_targets_ignored: RunEvidence,
    doctests: RunEvidence,
    doctests_ignored: RunEvidence,
}

#[derive(Debug, Serialize)]
struct CompilerTarget {
    target: String,
    source_inventory_blake3: String,
}

#[derive(Debug, Clone, Serialize)]
struct InventoryLeaf {
    role: String,
    path: String,
    sha256_algorithm: &'static str,
    sha256: String,
    blake3_algorithm: &'static str,
    blake3: String,
}

#[derive(Debug, Clone, Eq, Ord, PartialEq, PartialOrd, Serialize)]
struct InventoryIdentity {
    target: String,
    name: String,
    kind: String,
}

#[derive(Debug, Serialize, Clone)]
struct ListedTest {
    name: String,
    kind: String,
}

#[derive(Debug, Serialize)]
struct SigningRequest {
    attestation_kind: &'static str,
    tested_commit: String,
    manifest_path: String,
    signature_path: String,
    signer: &'static str,
    verdict: &'static str,
}

#[derive(Debug)]
struct Options {
    plan: bool,
    baseline_only: bool,
    baseline_output_dir: Option<PathBuf>,
    output: Option<String>,
    tested_commit: Option<String>,
    c1_pack_dir: Option<PathBuf>,
    persistent_release_pack_dir: Option<PathBuf>,
    persistent_release_perf_pack_dir: Option<PathBuf>,
    performance_admission_pack_dir: Option<PathBuf>,
}

#[derive(Debug, Serialize)]
struct BaselineEvidence {
    source_commit: String,
    workspace: RunEvidence,
}

#[derive(Debug)]
struct ValidatedPackInputs {
    c1_scorecard: Vec<u8>,
    c1_manifest: Vec<u8>,
    c1_provenance: Vec<u8>,
    persistent_release_scorecard: Vec<u8>,
    persistent_release_manifest: Vec<u8>,
    persistent_release_provenance: Vec<u8>,
    persistent_release_perf_scorecard: Vec<u8>,
    persistent_release_perf_manifest: Vec<u8>,
    persistent_release_perf_provenance: Vec<u8>,
}

struct StagedPerformanceAdmission {
    gate: PerformanceAdmissionGate,
    leaves: Vec<EvidenceLeaf>,
}

#[derive(Debug)]
struct StrictOutput {
    status: ExitStatus,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
    job_id: String,
    active_status: Vec<u8>,
    completed_status: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct AdoptedRchJob {
    id: u64,
    project_id: String,
    worker_id: String,
    command: Vec<String>,
}

#[derive(Debug, Clone, Copy)]
enum StreamKind {
    Stdout,
    Stderr,
}

#[derive(Debug)]
struct StreamChunk {
    kind: StreamKind,
    bytes: Vec<u8>,
}

fn main() {
    if let Err(error) = run() {
        eprintln!("phase5 evidence capture failed: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let options = parse_options(&env::args().collect::<Vec<_>>())?;
    let root = repository_root()?;
    let tested_commit = tested_commit(&root, options.tested_commit.as_deref())?;
    if options.baseline_only {
        return capture_baseline_only(
            &root,
            &tested_commit,
            options
                .baseline_output_dir
                .as_deref()
                .ok_or_else(|| format!("missing --baseline-output-dir; {USAGE}"))?,
        );
    }
    let baseline = load_baseline(&root)?;
    let runs = release_runs(&baseline)?;
    let plan = plan_json(&tested_commit, &runs)?;
    if options.plan {
        println!(
            "{}",
            serde_json::to_string_pretty(&plan).map_err(|error| error.to_string())?
        );
        return Ok(());
    }
    let output = options
        .output
        .ok_or_else(|| format!("missing --output; {USAGE}"))?;
    let c1_pack_dir = options
        .c1_pack_dir
        .ok_or_else(|| format!("missing --c1-pack-dir; {USAGE}"))?;
    let persistent_release_pack_dir = options
        .persistent_release_pack_dir
        .ok_or_else(|| format!("missing --persistent-release-pack-dir; {USAGE}"))?;
    let persistent_release_perf_pack_dir = options
        .persistent_release_perf_pack_dir
        .ok_or_else(|| format!("missing --persistent-release-perf-pack-dir; {USAGE}"))?;
    let performance_admission_pack_dir = options.performance_admission_pack_dir;
    let namespace = expected_namespace(&tested_commit);
    if output != format!("{namespace}/{MANIFEST_NAME}") {
        return Err(format!("--output must be `{namespace}/{MANIFEST_NAME}`"));
    }
    require_pristine_capture_checkout(&root)?;
    let pack_inputs = validate_pack_inputs(
        &root,
        &tested_commit,
        &c1_pack_dir,
        &persistent_release_pack_dir,
        &persistent_release_perf_pack_dir,
    )?;
    let workers = required_workers()?;
    let primary_worker = workers
        .first()
        .ok_or_else(|| "phase-5 worker pool unexpectedly resolved empty".to_owned())?;
    let namespace_path = root.join(&namespace);
    prepare_evidence_namespace(&namespace_path, &namespace)?;
    let staged_performance_admission = performance_admission_pack_dir
        .as_deref()
        .map(|directory| stage_performance_admission(&root, directory, &namespace, &tested_commit))
        .transpose()?;

    let c1_scorecard = write_raw_new(
        &root,
        &format!("{namespace}/performance/c1/c1_scorecard.json"),
        &pack_inputs.c1_scorecard,
    )?;
    let persistent_release_scorecard = write_raw_new(
        &root,
        &format!("{namespace}/performance/persistent/release/persistent_scorecard.json"),
        &pack_inputs.persistent_release_scorecard,
    )?;
    let persistent_release_perf_scorecard = write_raw_new(
        &root,
        &format!("{namespace}/performance/persistent/release-perf/persistent_scorecard.json"),
        &pack_inputs.persistent_release_perf_scorecard,
    )?;
    let census = pre_capture_untracked(&root, &tested_commit)?;
    let pre_capture_untracked = write_raw_new(
        &root,
        &format!("{namespace}/inputs/pre-capture-git-status.z"),
        &census,
    )?;
    let c1_manifest = write_raw_new(
        &root,
        &format!("{namespace}/performance/c1/manifest.json"),
        &pack_inputs.c1_manifest,
    )?;
    let c1_provenance = write_raw_new(
        &root,
        &format!("{namespace}/performance/c1/build_metadata.json"),
        &pack_inputs.c1_provenance,
    )?;
    let persistent_release_manifest = write_raw_new(
        &root,
        &format!("{namespace}/performance/persistent/release/manifest.json"),
        &pack_inputs.persistent_release_manifest,
    )?;
    let persistent_release_provenance = write_raw_new(
        &root,
        &format!("{namespace}/performance/persistent/release/provenance/citation_receipt.json"),
        &pack_inputs.persistent_release_provenance,
    )?;
    let persistent_release_perf_manifest = write_raw_new(
        &root,
        &format!("{namespace}/performance/persistent/release-perf/manifest.json"),
        &pack_inputs.persistent_release_perf_manifest,
    )?;
    let persistent_release_perf_provenance = write_raw_new(
        &root,
        &format!(
            "{namespace}/performance/persistent/release-perf/provenance/citation_receipt.json"
        ),
        &pack_inputs.persistent_release_perf_provenance,
    )?;

    let tree = tested_tree_hash(&root, &tested_commit)?;
    let cargo_lock = write_raw_new(
        &root,
        &format!("{namespace}/inputs/Cargo.lock"),
        &git_blob_at_commit(&root, &tested_commit, "Cargo.lock")?,
    )?;
    let rust_toolchain = write_raw_new(
        &root,
        &format!("{namespace}/inputs/rust-toolchain.toml"),
        &git_blob_at_commit(&root, &tested_commit, "rust-toolchain.toml")?,
    )?;

    let all_targets = capture_run(
        &root,
        &root,
        &namespace,
        "compiler-all-targets",
        primary_worker,
        &[
            "cargo",
            "test",
            "--locked",
            "--workspace",
            "--all-targets",
            "--",
            "--list",
        ],
    )?;
    let all_targets_ignored = capture_run(
        &root,
        &root,
        &namespace,
        "compiler-all-targets-ignored",
        primary_worker,
        &[
            "cargo",
            "test",
            "--locked",
            "--workspace",
            "--all-targets",
            "--",
            "--list",
            "--ignored",
        ],
    )?;
    let doctests = capture_run(
        &root,
        &root,
        &namespace,
        "compiler-doctests",
        primary_worker,
        &[
            "cargo",
            "test",
            "--locked",
            "--workspace",
            "--doc",
            "--",
            "--list",
        ],
    )?;
    let doctests_ignored = capture_run(
        &root,
        &root,
        &namespace,
        "compiler-doctests-ignored",
        primary_worker,
        &[
            "cargo",
            "test",
            "--locked",
            "--workspace",
            "--doc",
            "--",
            "--list",
            "--ignored",
        ],
    )?;
    let compiler_runs = CompilerInventoryRuns {
        all_targets,
        all_targets_ignored,
        doctests,
        doctests_ignored,
    };
    let compiler = compiler_inventory(&root, &namespace, &compiler_runs, &tree)?;
    let inventory_evidence = compiler
        .inventory_leaves
        .iter()
        .map(|leaf| EvidenceLeaf {
            path: leaf.path.clone(),
            digest_algorithm: leaf.blake3_algorithm.to_owned(),
            digest: leaf.blake3.clone(),
        })
        .collect::<Vec<_>>();
    let compiler_path = format!("{namespace}/compiler/compiler-inventory.json");
    let compiler_leaf = write_json_new(&root, &compiler_path, &compiler)?;

    let workspace = capture_run(
        &root,
        &root,
        &namespace,
        "workspace",
        primary_worker,
        &[
            "cargo",
            "test",
            "--locked",
            "--workspace",
            "--",
            "--test-threads=1",
        ],
    )?;
    let receipts = capture_release_runs(&root, &namespace, &runs, &workers)?;
    let signing_path = format!("{namespace}/signing/signer-attestation.json");
    let signature_path = format!("{namespace}/signing/manifest.minisig");
    let provisional = Manifest {
        schema_version: SCHEMA_VERSION,
        tested_commit: tested_commit.clone(),
        signature_path: signature_path.clone(),
        signer_attestation: EvidenceLeaf {
            path: signing_path.clone(),
            digest_algorithm: DIGEST_ALGORITHM.to_owned(),
            digest: String::new(),
        },
        cargo_lock,
        rust_toolchain,
        pre_capture_untracked,
        compiler_inventory_attestation: compiler_leaf,
        workspace,
        run_receipts: receipts,
        auxiliary_scorecards: Scorecards {
            c1: ScorecardEvidence {
                scorecard: c1_scorecard,
                pack_manifest: c1_manifest,
                commit_provenance: c1_provenance,
            },
            persistent: PersistentProfileScorecards {
                release: ScorecardEvidence {
                    scorecard: persistent_release_scorecard,
                    pack_manifest: persistent_release_manifest,
                    commit_provenance: persistent_release_provenance,
                },
                release_perf: ScorecardEvidence {
                    scorecard: persistent_release_perf_scorecard,
                    pack_manifest: persistent_release_perf_manifest,
                    commit_provenance: persistent_release_perf_provenance,
                },
            },
        },
        performance_regression_gate: staged_performance_admission
            .as_ref()
            .map_or_else(blocked_missing_authoritative_policy, |staged| {
                staged.gate.clone()
            }),
        evidence_pack: Vec::new(),
    };
    let manifest_path = format!("{namespace}/{MANIFEST_NAME}");
    let signing_leaf = write_json_new(
        &root,
        &signing_path,
        &SigningRequest {
            attestation_kind: "dsr_detached_signature_request",
            tested_commit: tested_commit.clone(),
            manifest_path: manifest_path.clone(),
            signature_path,
            signer: "DSR",
            verdict: "unsigned_manifest_ready_for_detached_signing",
        },
    )?;
    let mut manifest = provisional;
    manifest.signer_attestation = signing_leaf;
    manifest.evidence_pack = evidence_pack(&root, &manifest, &compiler_runs, &inventory_evidence)?;
    if let Some(staged) = staged_performance_admission {
        manifest.evidence_pack.extend(staged.leaves);
        manifest.evidence_pack.sort();
    }
    write_json_new(&root, &manifest_path, &manifest)?;
    println!("{}", serde_json::to_string_pretty(&serde_json::json!({"manifest": manifest_path, "tested_commit": tested_commit, "runs": manifest.run_receipts.len(), "signature": manifest.signature_path})).map_err(|error| error.to_string())?);
    Ok(())
}

fn parse_options(arguments: &[String]) -> Result<Options, String> {
    let mut plan = false;
    let mut baseline_only = false;
    let mut baseline_output_dir = None;
    let mut output = None;
    let mut tested_commit = None;
    let mut c1_pack_dir = None;
    let mut persistent_release_pack_dir = None;
    let mut persistent_release_perf_pack_dir = None;
    let mut performance_admission_pack_dir = None;
    let mut iter = arguments.iter().skip(1);
    while let Some(flag) = iter.next() {
        match flag.as_str() {
            "--plan" if !plan => plan = true,
            "--baseline-only" if !baseline_only => baseline_only = true,
            "--baseline-output-dir" if baseline_output_dir.is_none() => {
                baseline_output_dir = Some(PathBuf::from(iter.next().ok_or_else(|| {
                    format!("missing value after --baseline-output-dir; {USAGE}")
                })?));
            }
            "--output" if output.is_none() => {
                output = Some(
                    iter.next()
                        .ok_or_else(|| format!("missing value after --output; {USAGE}"))?
                        .to_owned(),
                )
            }
            "--tested-commit" if tested_commit.is_none() => {
                tested_commit = Some(
                    iter.next()
                        .ok_or_else(|| format!("missing value after --tested-commit; {USAGE}"))?
                        .to_owned(),
                )
            }
            "--c1-pack-dir" if c1_pack_dir.is_none() => {
                c1_pack_dir =
                    Some(PathBuf::from(iter.next().ok_or_else(|| {
                        format!("missing value after --c1-pack-dir; {USAGE}")
                    })?));
            }
            "--persistent-release-pack-dir" if persistent_release_pack_dir.is_none() => {
                persistent_release_pack_dir =
                    Some(PathBuf::from(iter.next().ok_or_else(|| {
                        format!("missing value after --persistent-release-pack-dir; {USAGE}")
                    })?));
            }
            "--persistent-release-perf-pack-dir" if persistent_release_perf_pack_dir.is_none() => {
                persistent_release_perf_pack_dir =
                    Some(PathBuf::from(iter.next().ok_or_else(|| {
                        format!("missing value after --persistent-release-perf-pack-dir; {USAGE}")
                    })?));
            }
            "--performance-admission-pack-dir" if performance_admission_pack_dir.is_none() => {
                performance_admission_pack_dir =
                    Some(PathBuf::from(iter.next().ok_or_else(|| {
                        format!("missing value after --performance-admission-pack-dir; {USAGE}")
                    })?));
            }
            _ => return Err(format!("unrecognized or duplicate `{flag}`; {USAGE}")),
        }
    }
    let capture = output.is_some();
    let modes = usize::from(plan) + usize::from(baseline_only) + usize::from(capture);
    let baseline_shape = baseline_only
        && baseline_output_dir.is_some()
        && output.is_none()
        && c1_pack_dir.is_none()
        && persistent_release_pack_dir.is_none()
        && persistent_release_perf_pack_dir.is_none();
    let capture_shape = capture
        && !baseline_only
        && baseline_output_dir.is_none()
        && c1_pack_dir.is_some()
        && persistent_release_pack_dir.is_some()
        && persistent_release_perf_pack_dir.is_some();
    let plan_shape = plan
        && !baseline_only
        && baseline_output_dir.is_none()
        && output.is_none()
        && c1_pack_dir.is_none()
        && persistent_release_pack_dir.is_none()
        && persistent_release_perf_pack_dir.is_none()
        && performance_admission_pack_dir.is_none();
    if modes != 1 || !(baseline_shape || capture_shape || plan_shape) {
        return Err(USAGE.to_owned());
    }
    Ok(Options {
        plan,
        baseline_only,
        baseline_output_dir,
        output,
        tested_commit,
        c1_pack_dir,
        persistent_release_pack_dir,
        persistent_release_perf_pack_dir,
        performance_admission_pack_dir,
    })
}

fn stage_performance_admission(
    root: &Path,
    input: &Path,
    namespace: &str,
    tested_commit: &str,
) -> Result<StagedPerformanceAdmission, String> {
    if !input.is_absolute() || !input.is_dir() {
        return Err("--performance-admission-pack-dir must name an absolute directory".to_owned());
    }
    let pack_bytes = fs::read(input.join("admission-pack.json"))
        .map_err(|error| format!("unable to read admission-pack.json: {error}"))?;
    let mut pack: PerformanceAdmissionPack = serde_json::from_slice(&pack_bytes)
        .map_err(|error| format!("invalid admission-pack.json: {error}"))?;
    let staging_dir = root.join(format!("{namespace}/performance-admission"));
    fs::create_dir(&staging_dir)
        .map_err(|error| format!("could not create {}: {error}", staging_dir.display()))?;
    let mut leaves = Vec::new();
    let mut index = 0_usize;
    let mut next_target = |source_path: &str| -> Result<String, String> {
        let file_name = Path::new(source_path)
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| "admission artifact name is not UTF-8".to_owned())?;
        let target = format!("{namespace}/performance-admission/{index:02}-{file_name}");
        index += 1;
        Ok(target)
    };
    let mut stage_bytes =
        |artifact: &mut ArtifactDigest, target: String, bytes: Vec<u8>| -> Result<(), String> {
            let leaf = write_raw_new(root, &target, &bytes)?;
            artifact.path = target;
            artifact.sha256 = sha256_bytes(&bytes);
            artifact.digest_algorithm = SHA256_ALGORITHM.to_owned();
            leaves.push(leaf);
            Ok(())
        };
    let policy_source = admission_input_path(input, &pack.policy.path)?;
    let policy_target = next_target(&pack.policy.path)?;
    stage_bytes(
        &mut pack.policy,
        policy_target,
        read_regular(&policy_source)?,
    )?;
    for candidate in [&mut pack.baseline, &mut pack.tested] {
        for profile in &mut candidate.profiles {
            let report_source = admission_input_path(input, &profile.raw_report.path)?;
            let manifest_source = admission_input_path(input, &profile.raw_manifest.path)?;
            let report_target = next_target(&profile.raw_report.path)?;
            let manifest_target = next_target(&profile.raw_manifest.path)?;
            let mut report: Value = serde_json::from_slice(&read_regular(&report_source)?)
                .map_err(|error| format!("invalid typed measurement report: {error}"))?;
            report
                .as_object_mut()
                .ok_or_else(|| "typed measurement report must be a JSON object".to_owned())?
                .insert(
                    "raw_manifest_path".to_owned(),
                    Value::String(manifest_target.clone()),
                );
            let report_bytes = serde_json::to_vec_pretty(&report)
                .map_err(|error| format!("serialize staged measurement report: {error}"))?;
            stage_bytes(&mut profile.raw_report, report_target, report_bytes)?;
            let mut manifest: Value = serde_json::from_slice(&read_regular(&manifest_source)?)
                .map_err(|error| format!("invalid typed measurement manifest: {error}"))?;
            manifest
                .as_object_mut()
                .ok_or_else(|| "typed measurement manifest must be a JSON object".to_owned())?
                .insert(
                    "raw_report_sha256".to_owned(),
                    Value::String(profile.raw_report.sha256.clone()),
                );
            let manifest_bytes = serde_json::to_vec_pretty(&manifest)
                .map_err(|error| format!("serialize staged measurement manifest: {error}"))?;
            stage_bytes(&mut profile.raw_manifest, manifest_target, manifest_bytes)?;
        }
    }
    let manifest_bindings = [&pack.baseline, &pack.tested]
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
    for receipt in [&mut pack.calibration_receipt, &mut pack.sensitivity_receipt] {
        let source = admission_input_path(input, &receipt.path)?;
        let target = next_target(&receipt.path)?;
        let mut document: Value = serde_json::from_slice(&read_regular(&source)?)
            .map_err(|error| format!("invalid typed admission receipt: {error}"))?;
        document
            .as_object_mut()
            .ok_or_else(|| "typed admission receipt must be a JSON object".to_owned())?
            .insert(
                "manifest_bindings".to_owned(),
                Value::Array(manifest_bindings.clone()),
            );
        let bytes = serde_json::to_vec_pretty(&document)
            .map_err(|error| format!("serialize staged admission receipt: {error}"))?;
        stage_bytes(receipt, target, bytes)?;
    }
    let pack_path = format!("{namespace}/performance-admission/admission-pack.json");
    let rewritten = serde_json::to_vec_pretty(&pack).map_err(|error| error.to_string())?;
    let pack_leaf = write_raw_new(root, &pack_path, &rewritten)?;
    validate_performance_admission_pack(root, tested_commit, &pack, false)?;
    leaves.push(pack_leaf);
    Ok(StagedPerformanceAdmission {
        gate: PerformanceAdmissionGate {
            schema_version:
                fsqlite_harness::performance_release_admission::ADMISSION_GATE_SCHEMA_V2.to_owned(),
            status: "authorized".to_owned(),
            release_authorized: true,
            blockers: Vec::new(),
            rationale: "Authorization is derived from the staged immutable v2 B/T admission pack."
                .to_owned(),
            admission_pack: Some(AdmissionPackReference {
                path: pack_path,
                sha256: sha256_bytes(&rewritten),
            }),
        },
        leaves,
    })
}

fn admission_input_path(input: &Path, declared: &str) -> Result<PathBuf, String> {
    let relative = Path::new(declared);
    if declared.trim().is_empty()
        || relative.is_absolute()
        || relative
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(
            "admission input artifact paths must be non-empty normal relative paths".to_owned(),
        );
    }
    Ok(input.join(relative))
}

fn repository_root() -> Result<PathBuf, String> {
    let output = local(
        &PathBuf::from("."),
        "git",
        &["rev-parse", "--show-toplevel"],
    )?;
    PathBuf::from(text(&output)?)
        .canonicalize()
        .map_err(|error| error.to_string())
}

fn tested_commit(root: &Path, requested: Option<&str>) -> Result<String, String> {
    let head = text(&local(root, "git", &["rev-parse", "--verify", "HEAD"])?)?;
    if !full_hash(&head) {
        return Err("HEAD is not a canonical Git commit hash".to_owned());
    }
    if requested.is_some_and(|value| value != head) {
        return Err("--tested-commit must equal HEAD".to_owned());
    }
    Ok(head)
}

fn load_baseline(root: &Path) -> Result<Baseline, String> {
    serde_json::from_slice(&read_regular(&root.join(BASELINE))?)
        .map_err(|error| format!("invalid {BASELINE}: {error}"))
}

fn release_runs(baseline: &Baseline) -> Result<Vec<&IgnoredTest>, String> {
    let mut entries = baseline
        .ignored_tests
        .iter()
        .filter(|entry| entry.policy == "run_for_release" && entry.locator() != GUARD_LOCATOR)
        .collect::<Vec<_>>();
    entries.sort_by_key(|entry| entry.locator());
    if entries.is_empty() {
        return Err("baseline has no noncircular run_for_release entries".to_owned());
    }
    Ok(entries)
}

fn exact_argv(entry: &IgnoredTest) -> Result<Vec<String>, String> {
    let components = entry.source_path.split('/').collect::<Vec<_>>();
    if components.len() < 4 || components[0] != "crates" {
        return Err(format!(
            "unsupported release source `{}`",
            entry.source_path
        ));
    }
    let package = components[1];
    let library = components[2] == "src" && components[3] != "main.rs" && components[3] != "bin";
    let integration = components[2] == "tests" && components.len() == 4;
    if !library && !integration {
        return Err(format!("ambiguous Cargo target `{}`", entry.source_path));
    }
    let mut argv = vec![
        "cargo".into(),
        "test".into(),
        "--locked".into(),
        "--profile".into(),
        "release-perf".into(),
        "--package".into(),
        package.into(),
    ];
    if library {
        argv.push("--lib".into());
    } else {
        argv.extend([
            "--test".into(),
            Path::new(components[3])
                .file_stem()
                .and_then(|part| part.to_str())
                .ok_or_else(|| "bad integration target".to_owned())?
                .into(),
        ]);
    }
    argv.push(runtime_name(entry, library)?);
    argv.extend(["--".into(), "--exact".into()]);
    match entry.cfg_condition.as_deref() {
        None | Some("test") => argv.push("--ignored".into()),
        Some("debug_assertions" | "all(debug_assertions,test)") => {}
        Some(condition) => return Err(format!("unsupported cfg condition `{condition}`")),
    }
    argv.extend(["--nocapture".into(), "--test-threads=1".into()]);
    Ok(argv)
}

fn runtime_name(entry: &IgnoredTest, library: bool) -> Result<String, String> {
    if !library {
        return Ok(entry.test_name.clone());
    }
    let parts = entry.source_path.split('/').collect::<Vec<_>>();
    let relative = &parts[3..];
    let mut modules = Vec::new();
    for (index, part) in relative.iter().enumerate() {
        if index + 1 == relative.len() {
            let stem = part
                .strip_suffix(".rs")
                .ok_or_else(|| "library source is not Rust".to_owned())?;
            if stem != "lib" {
                modules.push(stem.to_owned());
            }
        } else {
            modules.push((*part).to_owned());
        }
    }
    Ok(if modules.is_empty() {
        entry.test_name.clone()
    } else {
        format!("{}::{}", modules.join("::"), entry.test_name)
    })
}

fn plan_json(tested_commit: &str, runs: &[&IgnoredTest]) -> Result<Value, String> {
    Ok(serde_json::json!({
        "mode": "plan", "side_effect_free": true, "tested_commit": tested_commit,
        "manifest": format!("{}/{MANIFEST_NAME}", expected_namespace(tested_commit)),
        "workspace": ["cargo", "test", "--locked", "--workspace", "--", "--test-threads=1"],
        "compiler_inventory_runs": [
            ["cargo", "test", "--locked", "--workspace", "--all-targets", "--", "--list"],
            ["cargo", "test", "--locked", "--workspace", "--all-targets", "--", "--list", "--ignored"],
            ["cargo", "test", "--locked", "--workspace", "--doc", "--", "--list"],
            ["cargo", "test", "--locked", "--workspace", "--doc", "--", "--list", "--ignored"]
        ],
        "runs": runs.iter().map(|entry| exact_argv(entry)).collect::<Result<Vec<_>, _>>()?,
        "required_environment": [
            "exactly one of FSQLITE_PHASE5_RCH_WORKER or FSQLITE_PHASE5_RCH_WORKERS",
            "NO_COLOR=1",
            "RCH_LOG_FORMAT=json"
        ],
        "worker_pool": {"maximum": MAX_CAPTURE_WORKERS, "receipt_order": "baseline locator order"},
        "required_capture_inputs": ["--c1-pack-dir <absolute external dir>", "--persistent-release-pack-dir <absolute external dir>", "--persistent-release-perf-pack-dir <absolute external dir>"],
        "signing": "DSR detached signature is intentionally not produced by this runner"
    }))
}

fn expected_namespace(commit: &str) -> String {
    format!("{EVIDENCE_ROOT}/{commit}")
}

fn parse_worker_pool(value: &str) -> Result<Vec<String>, String> {
    let workers = value.split(',').map(str::to_owned).collect::<Vec<_>>();
    if workers.is_empty() || workers.len() > MAX_CAPTURE_WORKERS {
        return Err(format!(
            "worker pool must contain between one and {MAX_CAPTURE_WORKERS} workers"
        ));
    }
    let mut unique = BTreeSet::new();
    for worker in &workers {
        if worker.is_empty()
            || !worker
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
        {
            return Err("worker pool entries must be canonical worker ids".to_owned());
        }
        if !unique.insert(worker.clone()) {
            return Err("worker pool must not contain duplicate worker ids".to_owned());
        }
    }
    Ok(workers)
}

fn parse_single_worker(value: &str) -> Result<Vec<String>, String> {
    let workers = parse_worker_pool(value)?;
    if workers.len() != 1 {
        return Err("FSQLITE_PHASE5_RCH_WORKER accepts exactly one worker id".to_owned());
    }
    Ok(workers)
}

fn required_workers() -> Result<Vec<String>, String> {
    let single = env::var("FSQLITE_PHASE5_RCH_WORKER");
    let pool = env::var("FSQLITE_PHASE5_RCH_WORKERS");
    match (single, pool) {
        (Ok(_), Ok(_)) => Err(
            "set exactly one of FSQLITE_PHASE5_RCH_WORKER or FSQLITE_PHASE5_RCH_WORKERS"
                .to_owned(),
        ),
        (Ok(worker), Err(env::VarError::NotPresent)) => parse_single_worker(&worker),
        (Err(env::VarError::NotPresent), Ok(workers)) => parse_worker_pool(&workers),
        (Err(env::VarError::NotPresent), Err(env::VarError::NotPresent)) => Err(
            "FSQLITE_PHASE5_RCH_WORKER or FSQLITE_PHASE5_RCH_WORKERS is required for strict capture"
                .to_owned(),
        ),
        (Err(error), _) | (_, Err(error)) => {
            Err(format!("phase-5 worker environment is not Unicode: {error}"))
        }
    }
}

fn controlled_env(worker: &str) -> BTreeMap<String, String> {
    BTreeMap::from([
        ("CARGO_TERM_COLOR".to_owned(), "never".to_owned()),
        ("NO_COLOR".to_owned(), "1".to_owned()),
        ("RCH_LOG_FORMAT".to_owned(), "json".to_owned()),
        ("RCH_NO_SELF_HEALING".to_owned(), "1".to_owned()),
        ("RCH_REQUIRE_REMOTE".to_owned(), "1".to_owned()),
        ("RCH_TEST_TIMEOUT_SEC".to_owned(), "7200".to_owned()),
        ("RCH_WORKER".to_owned(), worker.to_owned()),
    ])
}

const BUILD_SHAPING_ENV_EXACT: &[&str] = &[
    "AR",
    "CC",
    "CFLAGS",
    "CPP",
    "CPPFLAGS",
    "CXX",
    "CXXFLAGS",
    "HOST_CC",
    "HOST_CFLAGS",
    "HOST_CXX",
    "HOST_CXXFLAGS",
    "LDFLAGS",
    "RANLIB",
    "RUSTC",
    "RUSTC_BOOTSTRAP",
    "RUSTC_WRAPPER",
    "RUSTC_WORKSPACE_WRAPPER",
    "RUSTDOC",
    "RUSTDOCFLAGS",
    "RUSTFLAGS",
    "RUSTUP_TOOLCHAIN",
    "TARGET_CC",
    "TARGET_CFLAGS",
    "TARGET_CXX",
    "TARGET_CXXFLAGS",
];

const NATIVE_TOOL_ENV_STEMS: &[&str] = &[
    "AR", "CC", "CFLAGS", "CPP", "CPPFLAGS", "CXX", "CXXFLAGS", "LDFLAGS", "RANLIB",
];

fn is_target_qualified_native_env(key: &str) -> bool {
    NATIVE_TOOL_ENV_STEMS.iter().any(|stem| {
        key.strip_prefix(stem)
            .is_some_and(|target| target.starts_with('_') && target.len() > 1)
            || key
                .strip_suffix(stem)
                .is_some_and(|target| target.ends_with('_') && target.len() > 1)
    })
}

fn is_build_shaping_env(key: &str) -> bool {
    BUILD_SHAPING_ENV_EXACT.contains(&key)
        || key == "CARGO_ENCODED_RUSTFLAGS"
        || key == "CARGO_INCREMENTAL"
        || key.starts_with("CARGO_BUILD_")
        || key.starts_with("CARGO_PROFILE_")
        || key.starts_with("CARGO_TARGET_")
        || is_target_qualified_native_env(key)
}

fn spawn_stream_reader<R: Read + Send + 'static>(
    reader: R,
    kind: StreamKind,
    sender: Sender<StreamChunk>,
) -> thread::JoinHandle<Result<(), String>> {
    thread::spawn(move || {
        let mut reader = BufReader::new(reader);
        loop {
            let mut bytes = Vec::new();
            let count = reader
                .read_until(b'\n', &mut bytes)
                .map_err(|error| error.to_string())?;
            if count == 0 {
                return Ok(());
            }
            sender
                .send(StreamChunk { kind, bytes })
                .map_err(|error| error.to_string())?;
        }
    })
}

fn append_chunks(receiver: &Receiver<StreamChunk>, stdout: &mut Vec<u8>, stderr: &mut Vec<u8>) {
    while let Ok(chunk) = receiver.try_recv() {
        match chunk.kind {
            StreamKind::Stdout => stdout.extend_from_slice(&chunk.bytes),
            StreamKind::Stderr => stderr.extend_from_slice(&chunk.bytes),
        }
    }
}

fn missing_adopted_job_error(worker: &str, stderr: &[u8]) -> String {
    let detail = String::from_utf8_lossy(stderr);
    let detail = detail.trim();
    if detail.is_empty() {
        format!("RCH command was never observed as the sole active job on `{worker}`")
    } else {
        format!("RCH command was never observed as the sole active job on `{worker}`: {detail}")
    }
}

fn rch_status(root: &Path) -> Result<Vec<u8>, String> {
    let output = Command::new("rch")
        .args([
            "--no-self-healing",
            "status",
            "--workers",
            "--jobs",
            "--json",
            "--no-color",
        ])
        .current_dir(root)
        .env("NO_COLOR", "1")
        .env("RCH_LOG_FORMAT", "json")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .map_err(|error| format!("could not capture RCH status: {error}"))?;
    if !output.status.success() || output.stdout.contains(&0x1b) {
        return Err(format!(
            "RCH status capture failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    parse_rch_status_envelope(&output.stdout)?;
    Ok(output.stdout)
}

fn parse_rch_status_envelope(bytes: &[u8]) -> Result<Value, String> {
    let value = serde_json::from_slice::<Value>(bytes)
        .map_err(|error| format!("RCH status was not JSON: {error}"))?;
    if value.get("api_version").and_then(Value::as_str) != Some("1.0")
        || value.get("command").and_then(Value::as_str) != Some("status")
        || value.get("success").and_then(Value::as_bool) != Some(true)
        || value
            .pointer("/data/daemon/active_builds")
            .and_then(Value::as_array)
            .is_none()
        || value
            .pointer("/data/daemon/recent_builds")
            .and_then(Value::as_array)
            .is_none()
    {
        return Err(
            "RCH status must be a successful API v1 status envelope with daemon job arrays"
                .to_owned(),
        );
    }
    Ok(value)
}

fn adopt_active_job(
    bytes: &[u8],
    worker: &str,
    argv: &[String],
) -> Result<Option<AdoptedRchJob>, String> {
    let value = parse_rch_status_envelope(bytes)?;
    let active = value
        .pointer("/data/daemon/active_builds")
        .and_then(Value::as_array)
        .ok_or_else(|| "validated status lost active build array".to_owned())?;
    let worker_builds = active
        .iter()
        .filter(|build| build.get("worker_id").and_then(Value::as_str) == Some(worker))
        .collect::<Vec<_>>();
    match worker_builds.as_slice() {
        [] => Ok(None),
        [build] => {
            let id = build
                .get("id")
                .and_then(Value::as_u64)
                .ok_or_else(|| "active RCH job id is not an exact u64 integer".to_owned())?;
            let project_id = build
                .get("project_id")
                .and_then(Value::as_str)
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| "active RCH job has no project identity".to_owned())?;
            let command = parse_rch_command_tokens(
                build
                    .get("command")
                    .and_then(Value::as_str)
                    .ok_or_else(|| "active RCH job has no command".to_owned())?,
            )?;
            if command != argv {
                return Err(format!(
                    "requested worker `{worker}` is occupied by a distinct RCH command"
                ));
            }
            Ok(Some(AdoptedRchJob {
                id,
                project_id: project_id.to_owned(),
                worker_id: worker.to_owned(),
                command,
            }))
        }
        _ => Err(format!(
            "requested worker `{worker}` has co-resident active RCH jobs"
        )),
    }
}

fn pin_adopted_job(
    adopted: &mut Option<AdoptedRchJob>,
    candidate: AdoptedRchJob,
    worker: &str,
) -> Result<(), String> {
    if adopted
        .as_ref()
        .is_some_and(|current| current != &candidate)
    {
        return Err(format!(
            "requested worker `{worker}` exposed distinct active job identities"
        ));
    }
    adopted.get_or_insert(candidate);
    Ok(())
}

fn completed_status_matches(bytes: &[u8], adopted: &AdoptedRchJob) -> Result<bool, String> {
    let value = parse_rch_status_envelope(bytes)?;
    let recent = value
        .pointer("/data/daemon/recent_builds")
        .and_then(Value::as_array)
        .ok_or_else(|| "validated status lost recent build array".to_owned())?;
    let matches = recent
        .iter()
        .filter(|build| build.get("id").and_then(Value::as_u64) == Some(adopted.id))
        .collect::<Vec<_>>();
    let build = match matches.as_slice() {
        [] => return Ok(false),
        [build] => *build,
        _ => return Err("completed RCH history contains a duplicate adopted job id".to_owned()),
    };
    let command = build
        .get("command")
        .and_then(Value::as_str)
        .ok_or_else(|| "completed RCH job has no command".to_owned())
        .and_then(parse_rch_command_tokens)?;
    if build.get("project_id").and_then(Value::as_str) != Some(adopted.project_id.as_str())
        || build.get("worker_id").and_then(Value::as_str) != Some(adopted.worker_id.as_str())
        || command != adopted.command
        || build.get("location").and_then(Value::as_str) != Some("remote")
        || build.get("exit_code").and_then(Value::as_i64) != Some(0)
        || !build.get("cancellation").is_some_and(Value::is_null)
    {
        return Err("completed RCH job does not match the adopted remote identity".to_owned());
    }
    Ok(true)
}

fn parse_rch_command_tokens(command: &str) -> Result<Vec<String>, String> {
    #[derive(Clone, Copy, Eq, PartialEq)]
    enum Quote {
        None,
        Single,
        Double,
    }
    let mut quote = Quote::None;
    let mut escaped = false;
    let mut started = false;
    let mut token = String::new();
    let mut tokens = Vec::new();
    for character in command.chars() {
        if matches!(character, '\0' | '\n' | '\r') {
            return Err("RCH status command contains a forbidden control character".to_owned());
        }
        if escaped {
            token.push(character);
            escaped = false;
            started = true;
            continue;
        }
        match quote {
            Quote::Single => {
                if character == '\'' {
                    quote = Quote::None;
                } else {
                    token.push(character);
                }
                started = true;
            }
            Quote::Double => {
                match character {
                    '"' => quote = Quote::None,
                    '\\' => escaped = true,
                    '$' | '`' => {
                        return Err(
                            "RCH status command uses unsupported shell expansion syntax".to_owned()
                        );
                    }
                    _ => token.push(character),
                }
                started = true;
            }
            Quote::None => match character {
                '\'' => {
                    quote = Quote::Single;
                    started = true;
                }
                '"' => {
                    quote = Quote::Double;
                    started = true;
                }
                '\\' => {
                    escaped = true;
                    started = true;
                }
                value if value.is_whitespace() => {
                    if started {
                        tokens.push(std::mem::take(&mut token));
                        started = false;
                    }
                }
                '$' | '`' | ';' | '|' | '&' | '<' | '>' => {
                    return Err("RCH status command uses unsupported shell syntax".to_owned());
                }
                _ => {
                    token.push(character);
                    started = true;
                }
            },
        }
    }
    if escaped || quote != Quote::None {
        return Err("RCH status command has an incomplete escape or quote".to_owned());
    }
    if started {
        tokens.push(token);
    }
    if tokens.is_empty() || tokens.iter().any(String::is_empty) {
        return Err("RCH status command must contain nonempty argument tokens".to_owned());
    }
    Ok(tokens)
}

fn one_rch_stderr_value(stderr: &str, marker: &str) -> Result<String, String> {
    let values = stderr
        .lines()
        .filter_map(|line| line.split_once(marker).map(|(_, suffix)| suffix))
        .map(|suffix| {
            suffix
                .split_whitespace()
                .next()
                .unwrap_or_default()
                .trim_matches(|character: char| {
                    !character.is_ascii_alphanumeric() && !matches!(character, '-' | '_')
                })
                .to_owned()
        })
        .collect::<Vec<_>>();
    match values.as_slice() {
        [value] if !value.is_empty() => Ok(value.clone()),
        _ => Err(format!(
            "RCH stderr must contain exactly one nonempty `{marker}` value"
        )),
    }
}

fn collect_child_streams(
    child: &mut Child,
    receiver: &Receiver<StreamChunk>,
    stdout: &mut Vec<u8>,
    stderr: &mut Vec<u8>,
) -> Result<ExitStatus, String> {
    loop {
        match receiver.recv_timeout(Duration::from_millis(25)) {
            Ok(chunk) => match chunk.kind {
                StreamKind::Stdout => stdout.extend_from_slice(&chunk.bytes),
                StreamKind::Stderr => stderr.extend_from_slice(&chunk.bytes),
            },
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                return child.wait().map_err(|error| error.to_string());
            }
        }
        if let Some(status) = child.try_wait().map_err(|error| error.to_string())? {
            append_chunks(receiver, stdout, stderr);
            return Ok(status);
        }
    }
}

fn controlled_cargo_target_dir(root: &Path) -> PathBuf {
    let root_digest = blake3::hash(root.to_string_lossy().as_bytes())
        .to_hex()
        .to_string();
    root.parent()
        .unwrap_or_else(|| Path::new("/"))
        .join(format!(".phase5-rch-target-{}", &root_digest[..16]))
}

fn validate_remote_target_mapping(stderr: &str, root: &Path, worker: &str) -> Result<(), String> {
    const PREFIX: &str = "Rewriting CARGO_TARGET_DIR for remote execution (worker-scoped path): ";
    let mappings = stderr
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .filter_map(|entry| {
            entry
                .pointer("/fields/message")
                .and_then(Value::as_str)
                .and_then(|message| message.strip_prefix(PREFIX))
                .map(str::to_owned)
        })
        .collect::<Vec<_>>();
    let [mapping] = mappings.as_slice() else {
        return Err("RCH stderr must contain exactly one structured target-dir rewrite".to_owned());
    };
    let (local, remote) = mapping
        .split_once(" -> ")
        .ok_or_else(|| "RCH target-dir rewrite is missing its remote path".to_owned())?;
    let expected_local = controlled_cargo_target_dir(root);
    if Path::new(local) != expected_local {
        return Err("RCH target-dir rewrite does not bind the controlled local sibling".to_owned());
    }
    let remote = Path::new(remote);
    let expected_prefix = format!(".rch-target-{worker}-pool-");
    if !remote.is_absolute()
        || remote == expected_local
        || !remote.starts_with(root)
        || !remote
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.starts_with(&expected_prefix))
    {
        return Err(
            "RCH target-dir rewrite does not bind an absolute worker-scoped pool".to_owned(),
        );
    }
    Ok(())
}

fn strict_rch_command(root: &Path, worker: &str, argv: &[String]) -> Command {
    let cargo_target_dir = controlled_cargo_target_dir(root);
    let mut command = Command::new("rch");
    for key in BUILD_SHAPING_ENV_EXACT {
        command.env_remove(key);
    }
    for (key, _) in env::vars_os() {
        if key.to_str().is_some_and(is_build_shaping_env) {
            command.env_remove(key);
        }
    }
    command
        .args(["--no-self-healing", "exec", "--no-self-healing", "--"])
        .args(argv)
        .current_dir(root)
        .envs(controlled_env(worker))
        .env("CARGO_TARGET_DIR", cargo_target_dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    command
}

fn strict_cargo(root: &Path, worker: &str, argv: &[String]) -> Result<StrictOutput, String> {
    if argv.first().map(String::as_str) != Some("cargo") {
        return Err("strict runner accepts direct Cargo argv only".to_owned());
    }
    let mut child = strict_rch_command(root, worker, argv)
        .spawn()
        .map_err(|error| format!("could not invoke strict RCH: {error}"))?;
    let stdout_pipe = child
        .stdout
        .take()
        .ok_or_else(|| "missing RCH stdout".to_owned())?;
    let stderr_pipe = child
        .stderr
        .take()
        .ok_or_else(|| "missing RCH stderr".to_owned())?;
    let (sender, receiver) = mpsc::channel();
    let stdout_reader = spawn_stream_reader(stdout_pipe, StreamKind::Stdout, sender.clone());
    let stderr_reader = spawn_stream_reader(stderr_pipe, StreamKind::Stderr, sender.clone());
    drop(sender);
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let mut active_status = None;
    let mut adopted_job = None;
    let mut last_status_poll = Instant::now()
        .checked_sub(Duration::from_secs(1))
        .unwrap_or_else(Instant::now);
    loop {
        match receiver.recv_timeout(Duration::from_millis(25)) {
            Ok(chunk) => match chunk.kind {
                StreamKind::Stdout => stdout.extend_from_slice(&chunk.bytes),
                StreamKind::Stderr => stderr.extend_from_slice(&chunk.bytes),
            },
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }
        // Keep the worker-isolation receipt live without hammering the daemon
        // on every 25 ms stream poll during a multi-hour workspace test.
        if last_status_poll.elapsed() >= Duration::from_secs(1) {
            let status = rch_status(root)?;
            if let Some(candidate) = adopt_active_job(&status, worker, argv)? {
                pin_adopted_job(&mut adopted_job, candidate, worker)?;
                active_status.get_or_insert(status);
            }
            last_status_poll = Instant::now();
        }
        if child
            .try_wait()
            .map_err(|error| error.to_string())?
            .is_some()
        {
            break;
        }
    }
    let status = collect_child_streams(&mut child, &receiver, &mut stdout, &mut stderr)?;
    stdout_reader
        .join()
        .map_err(|_| "RCH stdout reader panicked".to_owned())??;
    stderr_reader
        .join()
        .map_err(|_| "RCH stderr reader panicked".to_owned())??;
    append_chunks(&receiver, &mut stdout, &mut stderr);
    let adopted_job = adopted_job.ok_or_else(|| missing_adopted_job_error(worker, &stderr))?;
    let job_id = adopted_job.id.to_string();
    let active_status = active_status
        .ok_or_else(|| format!("RCH job {job_id} was never observed active on `{worker}`"))?;
    if !status.success() {
        return Err(format!(
            "strict RCH job {job_id} failed: {}",
            String::from_utf8_lossy(&stderr).trim()
        ));
    }
    let mut completed_status = None;
    for _ in 0..300 {
        let candidate = rch_status(root)?;
        if completed_status_matches(&candidate, &adopted_job)? {
            completed_status = Some(candidate);
            break;
        }
        thread::sleep(Duration::from_millis(100));
    }
    let completed_status = completed_status
        .ok_or_else(|| format!("RCH job {job_id} did not enter successful remote history"))?;
    if stdout.contains(&0x1b) || stderr.contains(&0x1b) {
        return Err(format!("strict RCH job {job_id} emitted an ANSI escape"));
    }
    let stderr_text = std::str::from_utf8(&stderr)
        .map_err(|error| format!("strict RCH stderr is not UTF-8: {error}"))?;
    if one_rch_stderr_value(stderr_text, "Selected worker: ")? != worker
        || one_rch_stderr_value(stderr_text, "Remote command finished: exit=")? != "0"
    {
        return Err(format!(
            "strict RCH job {job_id} stderr does not bind worker `{worker}` and remote exit 0"
        ));
    }
    validate_remote_target_mapping(stderr_text, root, worker)?;
    Ok(StrictOutput {
        status,
        stdout,
        stderr,
        job_id,
        active_status,
        completed_status,
    })
}

#[derive(Debug)]
struct ParallelMapState {
    next: usize,
    stopped: bool,
}

fn parallel_map_ordered<T, R, F>(
    items: &[T],
    workers: &[String],
    operation: F,
) -> Result<Vec<R>, String>
where
    T: Sync,
    R: Send,
    F: Fn(usize, &T, &str) -> Result<R, String> + Sync,
{
    if workers.is_empty() {
        return Err("parallel capture requires at least one worker".to_owned());
    }
    let state = Mutex::new(ParallelMapState {
        next: 0,
        stopped: false,
    });
    let results = Mutex::new(
        (0..items.len())
            .map(|_| None)
            .collect::<Vec<Option<Result<R, String>>>>(),
    );
    thread::scope(|scope| {
        let mut handles = Vec::with_capacity(workers.len());
        for worker in workers {
            let operation = &operation;
            let state = &state;
            let results = &results;
            handles.push(scope.spawn(move || -> Result<(), String> {
                loop {
                    let index = {
                        let mut schedule = state.lock().map_err(|_| {
                            "parallel capture scheduler lock was poisoned".to_owned()
                        })?;
                        if schedule.stopped || schedule.next >= items.len() {
                            return Ok(());
                        }
                        let index = schedule.next;
                        schedule.next += 1;
                        index
                    };
                    let result = operation(index, &items[index], worker);
                    let failed = result.is_err();
                    if failed {
                        state
                            .lock()
                            .map_err(|_| "parallel capture scheduler lock was poisoned".to_owned())?
                            .stopped = true;
                    }
                    let mut slots = results
                        .lock()
                        .map_err(|_| "parallel capture result lock was poisoned".to_owned())?;
                    if slots[index].replace(result).is_some() {
                        return Err(format!(
                            "parallel capture produced duplicate result index {index}"
                        ));
                    }
                    drop(slots);
                    if failed {
                        return Ok(());
                    }
                }
            }));
        }
        for handle in handles {
            handle
                .join()
                .map_err(|_| "parallel capture worker thread panicked".to_owned())??;
        }
        Ok::<(), String>(())
    })?;

    let slots = results
        .into_inner()
        .map_err(|_| "parallel capture result lock was poisoned".to_owned())?;
    if let Some((index, error)) = slots.iter().enumerate().find_map(|(index, result)| {
        result
            .as_ref()
            .and_then(|result| result.as_ref().err())
            .map(|error| (index, error))
    }) {
        return Err(format!("parallel capture item {index} failed: {error}"));
    }
    slots
        .into_iter()
        .enumerate()
        .map(|(index, result)| match result {
            Some(Ok(value)) => Ok(value),
            Some(Err(_)) => Err(format!(
                "parallel capture item {index} lost its reported failure"
            )),
            None => Err(format!(
                "parallel capture stopped without a result for item {index}"
            )),
        })
        .collect()
}

fn capture_release_runs(
    root: &Path,
    namespace: &str,
    runs: &[&IgnoredTest],
    workers: &[String],
) -> Result<Vec<RunReceipt>, String> {
    let prepared = runs
        .iter()
        .map(|entry| {
            Ok((
                *entry,
                format!("run-{}", blake3::hash(entry.locator().as_bytes()).to_hex()),
                exact_argv(entry)?,
            ))
        })
        .collect::<Result<Vec<_>, String>>()?;
    parallel_map_ordered(&prepared, workers, |_, (entry, label, argv), worker| {
        let evidence = capture_run(root, root, namespace, label, worker, argv)?;
        Ok(RunReceipt {
            source_path: entry.source_path.clone(),
            test_name: entry.test_name.clone(),
            requirement_blake3: blake3::hash(entry.evidence.requirement.as_bytes())
                .to_hex()
                .to_string(),
            evidence,
        })
    })
}

fn capture_run<T: AsRef<str>>(
    command_root: &Path,
    storage_root: &Path,
    namespace: &str,
    label: &str,
    worker: &str,
    argv: &[T],
) -> Result<RunEvidence, String> {
    let argv = argv
        .iter()
        .map(|argument| argument.as_ref().to_owned())
        .collect::<Vec<_>>();
    let result = strict_cargo(command_root, worker, &argv)?;
    let exit_status = result
        .status
        .code()
        .ok_or_else(|| "strict RCH process terminated without an exit code".to_owned())?;
    let stdout = write_raw_new(
        storage_root,
        &format!("{namespace}/transcripts/{label}.stdout"),
        &result.stdout,
    )?;
    let stderr = write_raw_new(
        storage_root,
        &format!("{namespace}/transcripts/{label}.stderr"),
        &result.stderr,
    )?;
    let active_status = write_raw_new(
        storage_root,
        &format!("{namespace}/runner/{label}.active-status.json"),
        &result.active_status,
    )?;
    let completed_status = write_raw_new(
        storage_root,
        &format!("{namespace}/runner/{label}.completed-status.json"),
        &result.completed_status,
    )?;
    let receipt = RchReceipt {
        schema_version: RCH_RECEIPT_SCHEMA.to_owned(),
        inner_cargo_argv: argv.clone(),
        job_id: result.job_id,
        active_status,
        completed_status,
    };
    let receipt_leaf = write_json_new(
        storage_root,
        &format!("{namespace}/runner/{label}.json"),
        &receipt,
    )?;
    Ok(RunEvidence {
        execution: CommandEvidence {
            argv,
            exit_status,
            stdout: StreamEvidence {
                capture: "observed".to_owned(),
                leaf: stdout,
            },
            stderr: StreamEvidence {
                capture: "observed".to_owned(),
                leaf: stderr.clone(),
            },
            transcript: stderr,
        },
        runner_receipt: receipt_leaf,
    })
}

fn compiler_inventory(
    root: &Path,
    namespace: &str,
    runs: &CompilerInventoryRuns,
    tree: &str,
) -> Result<CompilerInventory, String> {
    let active = identities_from_run(root, &runs.all_targets)?;
    let ignored = identities_from_run(root, &runs.all_targets_ignored)?;
    let doctests = identities_from_run(root, &runs.doctests)?;
    let ignored_doctests = identities_from_run(root, &runs.doctests_ignored)?;
    if active.is_empty() || doctests.is_empty() {
        return Err(
            "compiler all-target and doctest list transcripts must both be nonempty".to_owned(),
        );
    }
    if ignored
        .iter()
        .any(|identity| active.binary_search(identity).is_err())
        || ignored_doctests
            .iter()
            .any(|identity| doctests.binary_search(identity).is_err())
    {
        return Err("ignored compiler identities are not subsets of their full lists".to_owned());
    }

    let target_names = active
        .iter()
        .map(|identity| identity.target.clone())
        .collect::<BTreeSet<_>>();
    let mut targets = Vec::with_capacity(target_names.len());
    let mut leaves = Vec::with_capacity(target_names.len() + 6);
    for target in target_names {
        let target_hash = blake3::hash(target.as_bytes()).to_hex().to_string();
        let role_prefix = format!("target:{target}");
        let full_entries = active
            .iter()
            .filter(|identity| identity.target == target)
            .map(|identity| ListedTest {
                name: identity.name.clone(),
                kind: identity.kind.clone(),
            })
            .collect::<Vec<_>>();
        let ignored_entries = ignored
            .iter()
            .filter(|identity| identity.target == target)
            .map(|identity| ListedTest {
                name: identity.name.clone(),
                kind: identity.kind.clone(),
            })
            .collect::<Vec<_>>();
        let source = serde_json::json!({
            "executable": target,
            "full_inventory": {"entries": full_entries},
            "ignored_inventory": {"entries": ignored_entries},
        });
        let source_leaf = write_json_new(
            root,
            &format!("{namespace}/compiler/targets/{target_hash}-source-inventory.json"),
            &source,
        )?;
        leaves.push(inventory_leaf(
            root,
            format!("{role_prefix}:source_inventory"),
            source_leaf.clone(),
        )?);
        targets.push(CompilerTarget {
            target,
            source_inventory_blake3: source_leaf.digest,
        });
    }
    let target_mappings = targets
        .iter()
        .map(|target| target.target.clone())
        .collect::<Vec<_>>();
    let expanded = serde_json::json!({
        "derivation": "retained remote Cargo list transcripts",
        "active_identities": &active,
        "ignored_identities": &ignored,
        "doctest_identities": &doctests,
        "ignored_doctest_identities": &ignored_doctests,
    });
    let global_values = [
        (
            "cargo_metadata",
            serde_json::json!({
                "derivation": "remote cargo test --list",
                "targets": &target_mappings,
            }),
        ),
        (
            "target_mappings",
            serde_json::to_value(&target_mappings).map_err(|error| error.to_string())?,
        ),
        (
            "active_identities",
            serde_json::to_value(&active).map_err(|error| error.to_string())?,
        ),
        (
            "ignored_identities",
            serde_json::to_value(&ignored).map_err(|error| error.to_string())?,
        ),
        (
            "doctest_identities",
            serde_json::to_value(&doctests).map_err(|error| error.to_string())?,
        ),
        ("expanded_identities", expanded),
    ];
    let mut global_hashes = BTreeMap::new();
    for (role, value) in global_values {
        let leaf = write_json_new(root, &format!("{namespace}/compiler/{role}.json"), &value)?;
        global_hashes.insert(role, leaf.digest.clone());
        leaves.push(inventory_leaf(root, role.to_owned(), leaf)?);
    }
    leaves.sort_by(|left, right| (&left.role, &left.path).cmp(&(&right.role, &right.path)));
    if leaves
        .windows(2)
        .any(|pair| pair[0].role == pair[1].role || pair[0].path == pair[1].path)
    {
        return Err("compiler inventory leaves must have unique roles and paths".to_owned());
    }
    Ok(CompilerInventory {
        tested_tree_blake3: tree.to_owned(),
        cargo_metadata_blake3: required_global_hash(&mut global_hashes, "cargo_metadata")?,
        target_mappings_blake3: required_global_hash(&mut global_hashes, "target_mappings")?,
        active_identities_blake3: required_global_hash(&mut global_hashes, "active_identities")?,
        ignored_identities_blake3: required_global_hash(&mut global_hashes, "ignored_identities")?,
        doctest_identities_blake3: required_global_hash(&mut global_hashes, "doctest_identities")?,
        expanded_identities_blake3: required_global_hash(&mut global_hashes, "expanded_identities")?,
        cfg_profile: "compiler-derived locked workspace all-target and doctest list inventories; observed remote RCH streams".to_owned(),
        inventory_runs: CompilerInventoryRuns {
            all_targets: runs.all_targets.clone(),
            all_targets_ignored: runs.all_targets_ignored.clone(),
            doctests: runs.doctests.clone(),
            doctests_ignored: runs.doctests_ignored.clone(),
        },
        targets,
        inventory_leaves: leaves,
    })
}

fn identities_from_run(root: &Path, run: &RunEvidence) -> Result<Vec<InventoryIdentity>, String> {
    let bytes = read_evidence_leaf(root, &run.execution.stderr.leaf)?;
    let transcript = std::str::from_utf8(&bytes)
        .map_err(|error| format!("compiler list stderr is not UTF-8: {error}"))?;
    compiler_list_identities(transcript)
}

fn cargo_target_section(line: &str) -> Option<&str> {
    if let Some(section) = line.strip_prefix("     Running ") {
        if !section.is_empty() && section.contains(" (") && section.ends_with(')') {
            return Some(section);
        }
    }
    let section = line.strip_prefix("   Doc-tests ")?;
    (!section.trim().is_empty() && section == section.trim_end()).then_some(section)
}

fn compiler_list_identities(transcript: &str) -> Result<Vec<InventoryIdentity>, String> {
    struct TargetSection {
        target: String,
        tests: usize,
        benchmarks: usize,
    }
    fn parse_list_summary(line: &str) -> Option<(usize, usize)> {
        fn parse_count(value: &str, singular: &str, plural: &str) -> Option<usize> {
            let (count, used_singular) = if let Some(count) = value.strip_suffix(singular) {
                (count, true)
            } else {
                (value.strip_suffix(plural)?, false)
            };
            let count = count.parse::<usize>().ok()?;
            ((count == 1) == used_singular).then_some(count)
        }
        let (tests, benchmarks) = line.trim().split_once(", ")?;
        Some((
            parse_count(tests, " test", " tests")?,
            parse_count(benchmarks, " benchmark", " benchmarks")?,
        ))
    }

    let mut target: Option<TargetSection> = None;
    let mut identities = Vec::new();
    for line in transcript.lines() {
        if let Some(section) = cargo_target_section(line) {
            if let Some(previous) = target.take() {
                return Err(format!(
                    "compiler list target `{}` is missing its canonical count summary",
                    previous.target
                ));
            }
            let target_name = if line.starts_with("   Doc-tests ") {
                format!("doc:{section}")
            } else {
                section
                    .rsplit_once(" (")
                    .and_then(|(_, path)| path.strip_suffix(')'))
                    .map(str::to_owned)
                    .ok_or_else(|| {
                        format!("compiler list target has no canonical binary path: `{line}`")
                    })?
            };
            target = Some(TargetSection {
                target: target_name,
                tests: 0,
                benchmarks: 0,
            });
            continue;
        }
        if let Some((declared_tests, declared_benchmarks)) = parse_list_summary(line) {
            let section = target.take().ok_or_else(|| {
                "compiler list transcript contains a duplicate or unframed count summary".to_owned()
            })?;
            if section.tests != declared_tests || section.benchmarks != declared_benchmarks {
                return Err(format!(
                    "compiler list target `{}` declares {declared_tests} tests and {declared_benchmarks} benchmarks but retained {} tests and {} benchmarks",
                    section.target, section.tests, section.benchmarks
                ));
            }
            continue;
        }
        let Some((name, kind)) = line.rsplit_once(": ") else {
            if target.is_some() && !line.trim().is_empty() {
                return Err(format!(
                    "compiler list target contains an unrecognized pre-summary line: `{line}`"
                ));
            }
            continue;
        };
        if !matches!(kind, "test" | "benchmark") {
            if target.is_some() {
                return Err(format!(
                    "compiler list target contains an unrecognized identity kind: `{line}`"
                ));
            }
            continue;
        }
        let Some(section) = target.as_mut() else {
            return Err(format!(
                "compiler list identity `{name}` appeared outside a Cargo target section"
            ));
        };
        if kind == "test" {
            section.tests += 1;
        } else {
            section.benchmarks += 1;
        }
        identities.push(InventoryIdentity {
            target: section.target.clone(),
            name: name.to_owned(),
            kind: kind.to_owned(),
        });
    }
    if let Some(section) = target {
        return Err(format!(
            "compiler list target `{}` is truncated before its canonical count summary",
            section.target
        ));
    }
    identities.sort();
    if identities.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(
            "compiler list transcript must contain a unique normalized identity set".to_owned(),
        );
    }
    Ok(identities)
}

fn required_global_hash(
    hashes: &mut BTreeMap<&str, String>,
    role: &'static str,
) -> Result<String, String> {
    hashes
        .remove(role)
        .ok_or_else(|| format!("missing compiler inventory global `{role}`"))
}

fn inventory_leaf(root: &Path, role: String, leaf: EvidenceLeaf) -> Result<InventoryLeaf, String> {
    let bytes = read_evidence_leaf(root, &leaf)?;
    Ok(InventoryLeaf {
        role,
        path: leaf.path,
        sha256_algorithm: SHA256_ALGORITHM,
        sha256: sha256_bytes(&bytes),
        blake3_algorithm: DIGEST_ALGORITHM,
        blake3: leaf.digest,
    })
}

fn evidence_pack(
    root: &Path,
    manifest: &Manifest,
    compiler_runs: &CompilerInventoryRuns,
    inventory_leaves: &[EvidenceLeaf],
) -> Result<Vec<EvidenceLeaf>, String> {
    let mut leaves = vec![
        manifest.signer_attestation.clone(),
        manifest.cargo_lock.clone(),
        manifest.rust_toolchain.clone(),
        manifest.pre_capture_untracked.clone(),
        manifest.compiler_inventory_attestation.clone(),
        manifest.auxiliary_scorecards.c1.scorecard.clone(),
        manifest.auxiliary_scorecards.c1.pack_manifest.clone(),
        manifest.auxiliary_scorecards.c1.commit_provenance.clone(),
        manifest
            .auxiliary_scorecards
            .persistent
            .release
            .scorecard
            .clone(),
        manifest
            .auxiliary_scorecards
            .persistent
            .release
            .pack_manifest
            .clone(),
        manifest
            .auxiliary_scorecards
            .persistent
            .release
            .commit_provenance
            .clone(),
        manifest
            .auxiliary_scorecards
            .persistent
            .release_perf
            .scorecard
            .clone(),
        manifest
            .auxiliary_scorecards
            .persistent
            .release_perf
            .pack_manifest
            .clone(),
        manifest
            .auxiliary_scorecards
            .persistent
            .release_perf
            .commit_provenance
            .clone(),
    ];
    leaves.extend(run_evidence_leaves(root, &manifest.workspace)?);
    for receipt in &manifest.run_receipts {
        leaves.extend(run_evidence_leaves(root, &receipt.evidence)?);
    }
    for run in [
        &compiler_runs.all_targets,
        &compiler_runs.all_targets_ignored,
        &compiler_runs.doctests,
        &compiler_runs.doctests_ignored,
    ] {
        leaves.extend(run_evidence_leaves(root, run)?);
    }
    leaves.extend(inventory_leaves.iter().cloned());
    leaves.sort();
    if leaves.windows(2).any(|pair| pair[0].path == pair[1].path) {
        return Err("evidence pack contains duplicate paths".to_owned());
    }
    Ok(leaves)
}

fn run_evidence_leaves(root: &Path, run: &RunEvidence) -> Result<Vec<EvidenceLeaf>, String> {
    let receipt =
        serde_json::from_slice::<RchReceipt>(&read_evidence_leaf(root, &run.runner_receipt)?)
            .map_err(|error| format!("unable to parse retained RCH receipt: {error}"))?;
    Ok(vec![
        run.execution.stdout.leaf.clone(),
        run.execution.stderr.leaf.clone(),
        run.runner_receipt.clone(),
        receipt.active_status,
        receipt.completed_status,
    ])
}

fn prepare_evidence_namespace(namespace_path: &Path, namespace: &str) -> Result<(), String> {
    if namespace_path.exists() {
        return Err(format!(
            "refusing to reuse existing evidence root `{namespace}`"
        ));
    }
    fs::create_dir(namespace_path)
        .map_err(|error| format!("could not create evidence root `{namespace}`: {error}"))?;
    for directory in [
        "performance",
        "performance/c1",
        "performance/persistent",
        "performance/persistent/release",
        "performance/persistent/release/provenance",
        "performance/persistent/release-perf",
        "performance/persistent/release-perf/provenance",
        "inputs",
        "transcripts",
        "runner",
        "compiler",
        "compiler/targets",
        "signing",
    ] {
        fs::create_dir(namespace_path.join(directory))
            .map_err(|error| format!("could not create `{directory}` in `{namespace}`: {error}"))?;
    }
    Ok(())
}

fn require_pristine_capture_checkout(root: &Path) -> Result<(), String> {
    let status = local(
        root,
        "git",
        &[
            "status",
            "--porcelain=v1",
            "-z",
            "--untracked-files=all",
            "--ignored=matching",
        ],
    )?;
    if status
        .stdout
        .split(|byte| *byte == 0)
        .filter(|record| !record.is_empty())
        .all(|record| record.starts_with(b"!! target/"))
    {
        Ok(())
    } else {
        Err(
            "strict evidence capture requires a clean checkout; use --plan for keeper validation"
                .to_owned(),
        )
    }
}
fn local(root: &Path, program: &str, args: &[&str]) -> Result<Output, String> {
    if program != "git" {
        return Err(format!("unapproved local program `{program}`"));
    }
    let mut command = Command::new("git");
    let output = command
        .args(args)
        .current_dir(root)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .map_err(|error| format!("could not run `{program}`: {error}"))?;
    if output.status.success() {
        Ok(output)
    } else {
        Err(format!(
            "`{program}` failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ))
    }
}
fn text(output: &Output) -> Result<String, String> {
    String::from_utf8(output.stdout.clone())
        .map(|value| value.trim_end_matches(['\r', '\n']).to_owned())
        .map_err(|error| error.to_string())
}
fn full_hash(value: &str) -> bool {
    value.len() == 40
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}
fn read_regular(path: &Path) -> Result<Vec<u8>, String> {
    let metadata = fs::symlink_metadata(path).map_err(|error| error.to_string())?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(format!("regular file required: `{}`", path.display()));
    }
    fs::read(path).map_err(|error| error.to_string())
}
fn sha256_bytes(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut hash = Sha256::new();
    hash.update(bytes);
    let digest = hash.finalize();
    let mut encoded = String::with_capacity(digest.len() * 2);
    for byte in digest {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn write_json_new<T: Serialize>(
    root: &Path,
    path: &str,
    value: &T,
) -> Result<EvidenceLeaf, String> {
    let mut bytes = serde_json::to_vec_pretty(value).map_err(|error| error.to_string())?;
    bytes.push(b'\n');
    write_raw_new(root, path, &bytes)
}

fn write_raw_new(root: &Path, path: &str, bytes: &[u8]) -> Result<EvidenceLeaf, String> {
    let absolute = root.join(path);
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&absolute)
        .map_err(|error| format!("could not create `{path}`: {error}"))?;
    file.write_all(bytes)
        .and_then(|()| file.sync_all())
        .map_err(|error| error.to_string())?;
    Ok(EvidenceLeaf {
        path: path.to_owned(),
        digest_algorithm: DIGEST_ALGORITHM.to_owned(),
        digest: blake3::hash(bytes).to_hex().to_string(),
    })
}

fn read_evidence_leaf(root: &Path, leaf: &EvidenceLeaf) -> Result<Vec<u8>, String> {
    if leaf.digest_algorithm != DIGEST_ALGORITHM {
        return Err(format!("unsupported digest algorithm for `{}`", leaf.path));
    }
    let bytes = read_regular(&root.join(&leaf.path))?;
    if blake3::hash(&bytes).to_hex().as_str() != leaf.digest {
        return Err(format!("evidence digest mismatch for `{}`", leaf.path));
    }
    Ok(bytes)
}

fn tested_tree_hash(root: &Path, commit: &str) -> Result<String, String> {
    let output = local(root, "git", &["ls-tree", "-r", "--full-tree", commit])?;
    Ok(blake3::hash(&output.stdout).to_hex().to_string())
}

fn git_blob_at_commit(root: &Path, commit: &str, path: &str) -> Result<Vec<u8>, String> {
    let object = format!("{commit}:{path}");
    let output = local(root, "git", &["show", &object])?;
    Ok(output.stdout)
}

fn pre_capture_untracked(root: &Path, commit: &str) -> Result<Vec<u8>, String> {
    let output = local(
        root,
        "git",
        &["status", "--porcelain=v1", "-z", "--untracked-files=all"],
    )?;
    let expected = format!(
        "?? {EVIDENCE_ROOT}/{commit}/performance/c1/c1_scorecard.json\0?? {EVIDENCE_ROOT}/{commit}/performance/persistent/release-perf/persistent_scorecard.json\0?? {EVIDENCE_ROOT}/{commit}/performance/persistent/release/persistent_scorecard.json\0"
    )
    .into_bytes();
    if output.stdout != expected {
        return Err(format!(
            "pre-capture status must contain exactly the sorted three scorecards (c1, persistent/release, persistent/release-perf); found {:?}",
            output
                .stdout
                .split(|byte| *byte == 0)
                .filter(|record| !record.is_empty())
                .map(|record| String::from_utf8_lossy(record).into_owned())
                .collect::<Vec<_>>()
        ));
    }
    Ok(output.stdout)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PackKind {
    C1,
    Persistent,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PersistentProfile {
    Release,
    ReleasePerf,
}

impl PersistentProfile {
    fn receipt_name(self) -> &'static str {
        match self {
            Self::Release => "release",
            Self::ReleasePerf => "release-perf",
        }
    }

    fn manifest_name(self) -> &'static str {
        match self {
            Self::Release => "release",
            Self::ReleasePerf => "release-perf",
        }
    }

    fn expected_opt_level(self) -> &'static str {
        match self {
            Self::Release => "z",
            Self::ReleasePerf => "3",
        }
    }
}

/// Cross-profile pairing contract for the two persistent citation identities.
///
/// The release and release-perf captures must come from the same worker, host,
/// and workload so their numbers are comparable, yet must carry distinct build
/// nonces because each profile produces its own binary. Equal nonces mean one
/// build identity was replayed under both profile labels, which would let a
/// single `release-perf` binary masquerade as the portable `release` capture.
fn validate_persistent_profile_pairing(
    release: &PersistentCitationIdentity,
    release_perf: &PersistentCitationIdentity,
) -> Result<(), String> {
    if release.actual_worker != release_perf.actual_worker
        || release.actual_host != release_perf.actual_host
        || release.workload != release_perf.workload
    {
        return Err(
            "persistent release and release-perf packs must retain the same worker, host, and workload contract"
                .to_owned(),
        );
    }
    if release.nonce == release_perf.nonce {
        return Err(format!(
            "persistent release and release-perf packs must record distinct build nonces; both recorded `{}`",
            release.nonce
        ));
    }
    Ok(())
}

/// Exact dual-profile build binding shared by the persistent scorecard and its
/// pack manifest. Both documents must name the same Cargo profile and the opt
/// level that profile is defined to produce (`release` -> `z`, `release-perf`
/// -> `3`). A swapped, absent, or mislabelled pair fails closed here so a
/// release-perf capture can never be presented as the portable release pack.
fn validate_cargo_profile_binding(
    document: &str,
    profile: PersistentProfile,
    cargo_profile: Option<&str>,
    cargo_profile_expected_opt_level: Option<&str>,
) -> Result<(), String> {
    if cargo_profile != Some(profile.manifest_name())
        || cargo_profile_expected_opt_level != Some(profile.expected_opt_level())
    {
        return Err(format!(
            "persistent {} {document} must bind cargo_profile={} and cargo_profile_expected_opt_level={}; found cargo_profile={:?} and cargo_profile_expected_opt_level={:?}",
            profile.manifest_name(),
            profile.manifest_name(),
            profile.expected_opt_level(),
            cargo_profile,
            cargo_profile_expected_opt_level,
        ));
    }
    Ok(())
}

#[derive(Debug, Eq, PartialEq)]
struct PersistentCitationIdentity {
    actual_worker: String,
    actual_host: String,
    /// Canonical 64-hex build nonce. Carried on the identity so the dual-profile
    /// comparison can prove the two captures are independent builds rather than
    /// one build replayed under two profile labels.
    nonce: String,
    workload: PersistentWorkload,
}

#[derive(Debug, Deserialize, Eq, PartialEq)]
struct PersistentWorkload {
    benchmark: String,
    rows_per_thread: u64,
    synchronous: String,
    threads: Vec<u64>,
    criterion: PersistentCriterion,
}

#[derive(Debug, Deserialize, Eq, PartialEq)]
struct PersistentCriterion {
    sample_size: u64,
    warmup_secs: u64,
    measurement_secs: u64,
    export_root: String,
    headline_source: String,
}

fn external_pack_source_names(kind: PackKind) -> [&'static str; 3] {
    match kind {
        PackKind::C1 => [
            "c1_scorecard.json",
            "c1_pack_manifest.json",
            "build_metadata.json",
        ],
        PackKind::Persistent => [
            "persistent_scorecard.json",
            "persistent_pack_manifest.json",
            "provenance/citation_receipt.json",
        ],
    }
}

fn read_external_pack(
    root: &Path,
    directory: &Path,
    kind: PackKind,
) -> Result<[Vec<u8>; 3], String> {
    if !directory.is_absolute() {
        return Err(format!(
            "{} pack directory must be absolute",
            pack_kind_name(kind)
        ));
    }
    let metadata = fs::symlink_metadata(directory).map_err(|error| {
        format!(
            "unable to inspect {} pack directory `{}`: {error}",
            pack_kind_name(kind),
            directory.display()
        )
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(format!(
            "{} pack directory must be a real directory",
            pack_kind_name(kind)
        ));
    }
    let canonical = directory
        .canonicalize()
        .map_err(|error| error.to_string())?;
    if canonical.starts_with(root) {
        return Err(format!(
            "{} pack directory must be outside the repository",
            pack_kind_name(kind)
        ));
    }
    let [scorecard, manifest, provenance] = external_pack_source_names(kind);
    Ok([
        read_regular(&canonical.join(scorecard))?,
        read_regular(&canonical.join(manifest))?,
        read_regular(&canonical.join(provenance))?,
    ])
}

fn pack_kind_name(kind: PackKind) -> &'static str {
    match kind {
        PackKind::C1 => "C1",
        PackKind::Persistent => "persistent",
    }
}

fn validate_persistent_citation_receipt(
    bytes: &[u8],
    tested_commit: &str,
    expected_profile: PersistentProfile,
) -> Result<PersistentCitationIdentity, String> {
    const SCHEMA: &str = "fsqlite.release_persistent_phase_pack_citation_receipt.v2";
    const RUSTFLAGS_POLICY: &str =
        "all three values must be empty; remote cargo is invoked through env -u for each";

    #[derive(Deserialize)]
    struct Citation {
        schema_version: String,
        source: Source,
        build: Build,
        rch: Rch,
        rch_scheduler_isolation: SchedulerIsolation,
        workload: PersistentWorkload,
    }
    #[derive(Deserialize)]
    struct Source {
        commit: String,
        clean: bool,
    }
    #[derive(Deserialize)]
    struct Build {
        profile: String,
        expected_opt_level: String,
        rustflags: Rustflags,
        nonce: String,
    }
    #[derive(Deserialize)]
    struct Rustflags {
        #[allow(non_snake_case)]
        RUSTFLAGS: String,
        #[allow(non_snake_case)]
        CARGO_ENCODED_RUSTFLAGS: String,
        #[allow(non_snake_case)]
        CARGO_BUILD_RUSTFLAGS: String,
        policy: String,
    }
    #[derive(Deserialize)]
    struct Rch {
        actual_worker: String,
        actual_host: String,
        require_remote: bool,
        no_self_healing: bool,
    }
    #[derive(Deserialize)]
    struct SchedulerIsolation {
        build_status_trace: String,
        build_status_trace_sha256: String,
        build_completion_snapshot: String,
        build_completion_snapshot_sha256: String,
        build_job_id: String,
        build_active_samples: u64,
        job_id_encoding: String,
        phase_traces: BTreeMap<String, PhaseTrace>,
    }
    #[derive(Deserialize)]
    struct PhaseTrace {
        path: String,
        sha256: String,
        job_id: String,
        active_samples: u64,
        completion: String,
        completion_sha256: String,
    }
    fn canonical_job_id(value: &str) -> bool {
        !value.is_empty()
            && value.bytes().all(|byte| byte.is_ascii_digit())
            && (value.len() == 1 || !value.starts_with('0'))
    }
    fn canonical_sha256(value: &str) -> bool {
        value.len() == 64
            && value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    }

    let citation = serde_json::from_slice::<Citation>(bytes)
        .map_err(|error| format!("invalid persistent citation receipt: {error}"))?;
    if citation.schema_version != SCHEMA
        || citation.source.commit != tested_commit
        || !citation.source.clean
    {
        return Err(
            "persistent citation receipt must use v2 and bind the clean tested commit".to_owned(),
        );
    }
    if citation.build.profile != expected_profile.receipt_name()
        || citation.build.expected_opt_level != expected_profile.expected_opt_level()
        || citation.build.rustflags.RUSTFLAGS != ""
        || citation.build.rustflags.CARGO_ENCODED_RUSTFLAGS != ""
        || citation.build.rustflags.CARGO_BUILD_RUSTFLAGS != ""
        || citation.build.rustflags.policy != RUSTFLAGS_POLICY
        || !canonical_sha256(&citation.build.nonce)
        || citation.rch.actual_worker.trim().is_empty()
        || citation.rch.actual_host.trim().is_empty()
        || !citation.rch.require_remote
        || !citation.rch.no_self_healing
    {
        return Err(format!(
            "persistent {} citation receipt must bind the portable {} profile, its opt level, empty Rust flags, canonical build nonce, and remote worker identity",
            expected_profile.receipt_name(),
            expected_profile.receipt_name(),
        ));
    }
    if citation.workload.benchmark != "persistent_concurrent_write_{1,8,16}t"
        || citation.workload.rows_per_thread != 1000
        || citation.workload.synchronous != "NORMAL"
        || citation.workload.threads.as_slice() != &[1, 8, 16]
        || citation.workload.criterion.sample_size == 0
        || citation.workload.criterion.warmup_secs == 0
        || citation.workload.criterion.measurement_secs == 0
        || citation.workload.criterion.export_root != "{phase}/criterion_measurements"
        || citation.workload.criterion.headline_source
            != "{phase}/criterion_measurements/{label}/{engine}/base/estimates.json"
    {
        return Err(
            "persistent citation receipt does not retain the fixed release workload contract"
                .to_owned(),
        );
    }
    let isolation = citation.rch_scheduler_isolation;
    if isolation.build_status_trace != "provenance/rch_build_status.jsonl"
        || isolation.build_completion_snapshot != "provenance/rch_build_status_completion.json"
        || !canonical_sha256(&isolation.build_status_trace_sha256)
        || !canonical_sha256(&isolation.build_completion_snapshot_sha256)
        || !canonical_job_id(&isolation.build_job_id)
        || isolation.build_active_samples == 0
        || isolation.job_id_encoding != "decimal_string"
        || isolation.phase_traces.len() != 3
    {
        return Err(
            "persistent citation receipt lacks its canonical build scheduler binding".to_owned(),
        );
    }
    for phase in ["1t", "8t", "16t"] {
        let trace = isolation.phase_traces.get(phase).ok_or_else(|| {
            format!("persistent citation receipt omits the {phase} scheduler binding")
        })?;
        if trace.path != format!("{phase}/rch_status.jsonl")
            || trace.completion != format!("{phase}/rch_status_completion.json")
            || !canonical_sha256(&trace.sha256)
            || !canonical_sha256(&trace.completion_sha256)
            || !canonical_job_id(&trace.job_id)
            || trace.active_samples == 0
        {
            return Err(format!(
                "persistent citation receipt carries a malformed {phase} scheduler binding"
            ));
        }
    }
    Ok(PersistentCitationIdentity {
        actual_worker: citation.rch.actual_worker,
        actual_host: citation.rch.actual_host,
        nonce: citation.build.nonce,
        workload: citation.workload,
    })
}

fn validate_pack_inputs(
    root: &Path,
    tested_commit: &str,
    c1_directory: &Path,
    persistent_release_directory: &Path,
    persistent_release_perf_directory: &Path,
) -> Result<ValidatedPackInputs, String> {
    const C1_SCORECARD_SCHEMA: &str = "bd-db300.c1_evidence_pack_scorecard.v1";
    const C1_MANIFEST_SCHEMA: &str = "bd-db300.c1_evidence_pack_manifest.v1";
    const PERSISTENT_SCORECARD_SCHEMA: &str = "bd-db300.persistent_phase_pack_scorecard.v5";
    const PERSISTENT_MANIFEST_SCHEMA: &str = "bd-db300.persistent_phase_pack_manifest.v4";
    #[derive(Deserialize)]
    struct Scorecard {
        schema_version: String,
        run_id: String,
        honest_gate_summary: HonestGate,
        cargo_profile: Option<String>,
        cargo_profile_expected_opt_level: Option<String>,
    }
    #[derive(Deserialize)]
    struct HonestGate {
        verdict: String,
    }
    #[derive(Deserialize)]
    struct PackManifest {
        schema_version: String,
        run_id: String,
        build_metadata_json: Option<String>,
        build_metadata: Option<Value>,
        citation_receipt_json: Option<String>,
        cargo_profile: Option<String>,
        cargo_profile_expected_opt_level: Option<String>,
    }
    #[derive(Deserialize)]
    struct C1Provenance {
        run_id: String,
        release_mode: bool,
        frozen_commit: Option<String>,
        clean_checkout: bool,
    }
    let [c1_scorecard, c1_manifest, c1_provenance] =
        read_external_pack(root, c1_directory, PackKind::C1)?;
    let [
        persistent_release_scorecard,
        persistent_release_manifest,
        persistent_release_provenance,
    ] = read_external_pack(root, persistent_release_directory, PackKind::Persistent)?;
    let [
        persistent_release_perf_scorecard,
        persistent_release_perf_manifest,
        persistent_release_perf_provenance,
    ] = read_external_pack(
        root,
        persistent_release_perf_directory,
        PackKind::Persistent,
    )?;
    let c1_score: Scorecard = serde_json::from_slice(&c1_scorecard)
        .map_err(|error| format!("invalid C1 scorecard: {error}"))?;
    let c1_pack: PackManifest = serde_json::from_slice(&c1_manifest)
        .map_err(|error| format!("invalid C1 pack manifest: {error}"))?;
    let c1_source: C1Provenance = serde_json::from_slice(&c1_provenance)
        .map_err(|error| format!("invalid C1 build metadata: {error}"))?;
    let c1_source_value: Value = serde_json::from_slice(&c1_provenance)
        .map_err(|error| format!("invalid C1 build metadata value: {error}"))?;
    if c1_score.schema_version != C1_SCORECARD_SCHEMA
        || c1_pack.schema_version != C1_MANIFEST_SCHEMA
        || c1_score.run_id.trim().is_empty()
        || c1_score.run_id != c1_pack.run_id
        || c1_score.run_id != c1_source.run_id
        || c1_score.honest_gate_summary.verdict != "pass"
        || c1_pack.build_metadata_json.as_deref() != Some("build_metadata.json")
        || c1_pack.build_metadata.as_ref() != Some(&c1_source_value)
        || c1_pack.citation_receipt_json.is_some()
        || !c1_source.release_mode
        || !c1_source.clean_checkout
        || c1_source.frozen_commit.as_deref() != Some(tested_commit)
    {
        return Err(
            "C1 evidence pack does not bind a passing run to the clean tested commit".to_owned(),
        );
    }

    let validate_persistent_pack = |scorecard: &[u8],
                                    manifest: &[u8],
                                    provenance: &[u8],
                                    profile: PersistentProfile|
     -> Result<PersistentCitationIdentity, String> {
        let score: Scorecard = serde_json::from_slice(scorecard).map_err(|error| {
            format!(
                "invalid persistent {} scorecard: {error}",
                profile.manifest_name()
            )
        })?;
        let pack: PackManifest = serde_json::from_slice(manifest).map_err(|error| {
            format!(
                "invalid persistent {} pack manifest: {error}",
                profile.manifest_name()
            )
        })?;
        if score.schema_version != PERSISTENT_SCORECARD_SCHEMA
            || pack.schema_version != PERSISTENT_MANIFEST_SCHEMA
            || score.run_id.trim().is_empty()
            || score.run_id != pack.run_id
            || score.honest_gate_summary.verdict != "pass"
            || pack.citation_receipt_json.as_deref() != Some("provenance/citation_receipt.json")
            || pack.build_metadata_json.is_some()
            || pack.build_metadata.is_some()
        {
            return Err(format!(
                "persistent {} evidence pack does not bind a passing run to the clean tested commit",
                profile.manifest_name()
            ));
        }
        validate_cargo_profile_binding(
            "scorecard",
            profile,
            score.cargo_profile.as_deref(),
            score.cargo_profile_expected_opt_level.as_deref(),
        )?;
        validate_cargo_profile_binding(
            "pack manifest",
            profile,
            pack.cargo_profile.as_deref(),
            pack.cargo_profile_expected_opt_level.as_deref(),
        )?;
        validate_persistent_citation_receipt(provenance, tested_commit, profile)
    };
    let release_identity = validate_persistent_pack(
        &persistent_release_scorecard,
        &persistent_release_manifest,
        &persistent_release_provenance,
        PersistentProfile::Release,
    )?;
    let release_perf_identity = validate_persistent_pack(
        &persistent_release_perf_scorecard,
        &persistent_release_perf_manifest,
        &persistent_release_perf_provenance,
        PersistentProfile::ReleasePerf,
    )?;
    validate_persistent_profile_pairing(&release_identity, &release_perf_identity)?;
    Ok(ValidatedPackInputs {
        c1_scorecard,
        c1_manifest,
        c1_provenance,
        persistent_release_scorecard,
        persistent_release_manifest,
        persistent_release_provenance,
        persistent_release_perf_scorecard,
        persistent_release_perf_manifest,
        persistent_release_perf_provenance,
    })
}

fn capture_baseline_only(
    root: &Path,
    source_commit: &str,
    output_directory: &Path,
) -> Result<(), String> {
    require_pristine_capture_checkout(root)?;
    let workers = required_workers()?;
    let worker = workers
        .first()
        .ok_or_else(|| "phase-5 worker pool unexpectedly resolved empty".to_owned())?;
    prepare_external_baseline_root(root, output_directory, source_commit)?;
    let namespace = format!("{EVIDENCE_ROOT}/{source_commit}/baseline");
    let workspace = capture_run(
        root,
        output_directory,
        &namespace,
        "workspace",
        worker,
        &[
            "cargo",
            "test",
            "--locked",
            "--workspace",
            "--",
            "--test-threads=1",
        ],
    )?;
    let receipt_path = format!("{namespace}/baseline-evidence.json");
    write_json_new(
        output_directory,
        &receipt_path,
        &BaselineEvidence {
            source_commit: source_commit.to_owned(),
            workspace,
        },
    )?;
    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({
            "mode": "baseline-only",
            "source_commit": source_commit,
            "copy_root": output_directory.join(EVIDENCE_ROOT).join(source_commit).display().to_string(),
            "baseline_evidence": output_directory.join(&receipt_path).display().to_string(),
        }))
        .map_err(|error| error.to_string())?
    );
    Ok(())
}

fn prepare_external_baseline_root(
    repository_root: &Path,
    output_directory: &Path,
    source_commit: &str,
) -> Result<(), String> {
    if !output_directory.is_absolute() {
        return Err("--baseline-output-dir must be absolute".to_owned());
    }
    if output_directory.exists() {
        return Err("--baseline-output-dir must not already exist".to_owned());
    }
    let parent = output_directory
        .parent()
        .ok_or_else(|| "baseline output directory has no parent".to_owned())?;
    let parent = parent
        .canonicalize()
        .map_err(|error| format!("unable to canonicalize baseline output parent: {error}"))?;
    if parent.starts_with(repository_root) {
        return Err("baseline output directory must be outside the repository".to_owned());
    }
    fs::create_dir(output_directory)
        .map_err(|error| format!("unable to create baseline output directory: {error}"))?;
    let namespace = output_directory
        .join(EVIDENCE_ROOT)
        .join(source_commit)
        .join("baseline");
    fs::create_dir_all(&namespace)
        .map_err(|error| format!("unable to create baseline evidence namespace: {error}"))?;
    for directory in ["transcripts", "runner"] {
        fs::create_dir(namespace.join(directory)).map_err(|error| {
            format!("unable to create baseline `{directory}` directory: {error}")
        })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        AdoptedRchJob, BUILD_SHAPING_ENV_EXACT, PackKind, PersistentCitationIdentity,
        PersistentCriterion, PersistentProfile, PersistentWorkload, adopt_active_job,
        completed_status_matches, external_pack_source_names, is_build_shaping_env,
        missing_adopted_job_error, parallel_map_ordered, parse_options, parse_single_worker,
        parse_worker_pool, pin_adopted_job, strict_rch_command, validate_cargo_profile_binding,
        validate_persistent_citation_receipt, validate_persistent_profile_pairing,
        validate_remote_target_mapping,
    };
    use serde_json::Value;

    #[test]
    fn capture_options_require_distinct_release_and_release_perf_packs() {
        let canonical = [
            "phase5_evidence_capture",
            "--output",
            "tests/artifacts/release-evidence/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/manifest.json",
            "--c1-pack-dir",
            "/tmp/c1",
            "--persistent-release-pack-dir",
            "/tmp/release",
            "--persistent-release-perf-pack-dir",
            "/tmp/release-perf",
        ]
        .map(str::to_owned);
        assert!(parse_options(&canonical).is_ok());
        assert!(parse_options(&canonical[..7]).is_err());

        let legacy = [
            "phase5_evidence_capture",
            "--output",
            "tests/artifacts/release-evidence/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/manifest.json",
            "--c1-pack-dir",
            "/tmp/c1",
            "--persistent-pack-dir",
            "/tmp/persistent",
        ]
        .map(str::to_owned);
        assert!(parse_options(&legacy).is_err());
    }

    #[test]
    fn worker_pool_parser_is_bounded_unique_and_canonical() {
        assert_eq!(
            parse_worker_pool("ovh-a,vmi1227854"),
            Ok(vec!["ovh-a".to_owned(), "vmi1227854".to_owned()])
        );
        assert_eq!(parse_worker_pool("ovh-a"), Ok(vec!["ovh-a".to_owned()]));
        assert_eq!(parse_single_worker("ovh-a"), Ok(vec!["ovh-a".to_owned()]));
        assert!(parse_single_worker("ovh-a,vmi1227854").is_err());
        for invalid in [
            "",
            "ovh-a,",
            ",ovh-a",
            "ovh-a, ovh-b",
            "ovh-a,ovh-a",
            "ovh-a,ovh-b,ovh-c",
            "ovh/a",
        ] {
            assert!(parse_worker_pool(invalid).is_err(), "accepted `{invalid}`");
        }
    }

    #[test]
    fn missing_adopted_job_error_retains_rch_failure_detail() {
        assert_eq!(
            missing_adopted_job_error("ovh-a", b""),
            "RCH command was never observed as the sole active job on `ovh-a`"
        );
        assert_eq!(
            missing_adopted_job_error("ovh-a", b"  \n\t"),
            "RCH command was never observed as the sole active job on `ovh-a`"
        );
        assert_eq!(
            missing_adopted_job_error("ovh-a", b"  RCH-E301: remote execution refused\n"),
            "RCH command was never observed as the sole active job on `ovh-a`: RCH-E301: remote execution refused"
        );
        assert_eq!(
            missing_adopted_job_error("ovh-a", b"\xff remote failure"),
            "RCH command was never observed as the sole active job on `ovh-a`: \u{fffd} remote failure"
        );
    }

    #[test]
    fn parallel_map_preserves_input_order_across_out_of_order_completion() {
        let items = [0_u64, 1, 2, 3];
        let workers = ["worker-a".to_owned(), "worker-b".to_owned()];
        let output = parallel_map_ordered(&items, &workers, |index, item, _| {
            if index == 0 {
                std::thread::sleep(std::time::Duration::from_millis(20));
            }
            Ok(format!("{index}:{item}"))
        })
        .expect("parallel ordered map");
        assert_eq!(output, ["0:0", "1:1", "2:2", "3:3"]);
    }

    #[test]
    fn parallel_map_stops_claiming_after_first_observed_failure() {
        let items = [0_u8, 1, 2, 3];
        let workers = ["worker-a".to_owned(), "worker-b".to_owned()];
        let barrier = std::sync::Barrier::new(2);
        let claimed = std::sync::Mutex::new(Vec::new());
        let error = parallel_map_ordered(&items, &workers, |index, _, _| {
            claimed.lock().expect("claimed indices").push(index);
            if index < 2 {
                barrier.wait();
            }
            if index == 1 {
                return Err("synthetic failure".to_owned());
            }
            if index == 0 {
                std::thread::sleep(std::time::Duration::from_millis(20));
            }
            Ok(index)
        })
        .expect_err("failure must abort parallel map");
        let mut claimed = claimed.into_inner().expect("claimed indices");
        claimed.sort_unstable();
        assert_eq!(claimed, [0, 1]);
        assert!(error.contains("item 1 failed: synthetic failure"));
    }

    #[test]
    fn build_shaping_environment_is_removed_without_hiding_credentials() {
        for key in [
            "RUSTFLAGS",
            "RUSTDOCFLAGS",
            "RUSTUP_TOOLCHAIN",
            "CARGO_BUILD_TARGET",
            "CARGO_PROFILE_RELEASE_LTO",
            "CARGO_INCREMENTAL",
            "CARGO_ENCODED_RUSTFLAGS",
            "CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER",
            "RUSTC_WRAPPER",
            "RUSTC_WORKSPACE_WRAPPER",
            "CC",
            "CFLAGS",
            "CC_x86_64_unknown_linux_gnu",
            "CFLAGS_x86_64-unknown-linux-gnu",
            "CXX_x86_64_unknown_linux_gnu",
            "AR_x86_64-unknown-linux-gnu",
            "x86_64_unknown_linux_gnu_CC",
            "x86_64-unknown-linux-gnu_CXXFLAGS",
            "aarch64_unknown_linux_gnu_RANLIB",
            "aarch64-unknown-linux-gnu_LDFLAGS",
        ] {
            assert!(is_build_shaping_env(key), "expected removal of {key}");
        }
        for key in [
            "PATH",
            "RUSTUP_HOME",
            "CARGO_HOME",
            "CARGO_REGISTRY_TOKEN",
            "RCH_WORKER",
            "PKG_CONFIG_PATH",
            "CMAKE_GENERATOR",
        ] {
            assert!(!is_build_shaping_env(key), "must preserve access env {key}");
        }
        let root = std::path::Path::new("/clean/ancestor/source");
        let argv = ["cargo", "check"].map(str::to_owned);
        let command = strict_rch_command(root, "ovh-a", &argv);
        for key in BUILD_SHAPING_ENV_EXACT {
            assert!(
                command
                    .get_envs()
                    .any(|(candidate, value)| candidate == *key && value.is_none()),
                "command must explicitly remove {key}"
            );
        }
    }

    #[test]
    fn strict_rch_command_overrides_hostile_relative_cargo_target_dir() {
        let root = std::path::Path::new("/clean/ancestor/source");
        let argv = ["cargo", "check"].map(str::to_owned);
        let command = strict_rch_command(root, "ovh-a", &argv);
        let target_dir = command
            .get_envs()
            .find(|(key, _)| *key == "CARGO_TARGET_DIR")
            .and_then(|(_, value)| value)
            .map(std::path::Path::new)
            .expect("controlled CARGO_TARGET_DIR overlay");
        assert!(target_dir.is_absolute());
        assert!(!target_dir.starts_with(root));
        assert_ne!(target_dir, std::path::Path::new("hostile/relative-target"));
    }

    #[test]
    fn strict_rch_command_sets_the_long_workspace_test_timeout() {
        let root = std::path::Path::new("/clean/ancestor/source");
        let argv = [
            "cargo",
            "test",
            "--locked",
            "--workspace",
            "--",
            "--test-threads=1",
        ]
        .map(str::to_owned);
        let command = strict_rch_command(root, "ovh-a", &argv);
        let timeout = command
            .get_envs()
            .find(|(key, _)| *key == "RCH_TEST_TIMEOUT_SEC")
            .and_then(|(_, value)| value);
        assert_eq!(timeout, Some(std::ffi::OsStr::new("7200")));
    }

    #[test]
    fn structured_remote_target_mapping_binds_controlled_sibling_and_worker_pool() {
        let root = std::path::Path::new("/clean/ancestor/source");
        let worker = "ovh-a";
        let argv = ["cargo", "check"].map(str::to_owned);
        let command = strict_rch_command(root, worker, &argv);
        let local = command
            .get_envs()
            .find(|(key, _)| *key == "CARGO_TARGET_DIR")
            .and_then(|(_, value)| value)
            .and_then(|value| value.to_str())
            .expect("controlled CARGO_TARGET_DIR");
        let message = format!(
            "Rewriting CARGO_TARGET_DIR for remote execution (worker-scoped path): {local} -> /clean/ancestor/source/.rch-target-ovh-a-pool-deadbeef"
        );
        let stderr = serde_json::json!({"fields": {"message": message}}).to_string();
        validate_remote_target_mapping(&stderr, root, worker).expect("accept exact mapping");

        let wrong_worker = stderr.replace(".rch-target-ovh-a-", ".rch-target-ovh-b-");
        assert!(validate_remote_target_mapping(&wrong_worker, root, worker).is_err());
        let wrong_local = stderr.replace(local, "/tmp/ambient-target");
        assert!(validate_remote_target_mapping(&wrong_local, root, worker).is_err());
    }

    #[test]
    fn daemon_adoption_preserves_live_u64_job_identity_above_f64_range() {
        let job_id = 29_960_952_048_779_266_u64;
        assert!(job_id > (1_u64 << 53));
        let status = serde_json::json!({
            "api_version": "1.0",
            "command": "status",
            "success": true,
            "data": {"daemon": {
                "active_builds": [{
                    "id": job_id,
                    "project_id": "frankensqlite-df8c83ae",
                    "worker_id": "ovh-a",
                    "command": "cargo test --locked --workspace -- --test-threads=1",
                }],
                "recent_builds": [],
            }},
        });
        let argv = [
            "cargo",
            "test",
            "--locked",
            "--workspace",
            "--",
            "--test-threads=1",
        ]
        .map(str::to_owned);
        let adopted = adopt_active_job(
            &serde_json::to_vec(&status).expect("encode live status"),
            "ovh-a",
            &argv,
        )
        .expect("accept live-shaped status")
        .expect("adopt matching active job");
        assert_eq!(adopted.id, job_id);
        assert_eq!(adopted.id.to_string(), "29960952048779266");
    }

    #[test]
    fn daemon_adoption_rejects_ambiguous_or_foreign_active_work() {
        let argv = ["cargo", "check", "--workspace"].map(str::to_owned);
        let build = |id, worker: &str, command: &str| {
            serde_json::json!({
                "id": id,
                "project_id": "frankensqlite-df8c83ae",
                "worker_id": worker,
                "command": command,
            })
        };
        let status = |active: Vec<Value>| {
            serde_json::to_vec(&serde_json::json!({
                "api_version": "1.0",
                "command": "status",
                "success": true,
                "data": {"daemon": {"active_builds": active, "recent_builds": []}},
            }))
            .expect("encode active status")
        };

        assert_eq!(
            adopt_active_job(&status(Vec::new()), "ovh-a", &argv),
            Ok(None)
        );
        let foreign = status(vec![build(1, "ovh-a", "cargo check -p fsqlite-core")]);
        assert!(
            adopt_active_job(&foreign, "ovh-a", &argv)
                .expect_err("foreign command must fail")
                .contains("distinct RCH command")
        );
        let ambiguous = status(vec![
            build(1, "ovh-a", "cargo check --workspace"),
            build(2, "ovh-a", "cargo check --workspace"),
        ]);
        assert!(
            adopt_active_job(&ambiguous, "ovh-a", &argv)
                .expect_err("co-resident jobs must fail")
                .contains("co-resident")
        );
    }

    #[test]
    fn adopted_job_identity_stays_pinned_across_status_samples() {
        let job = |id| AdoptedRchJob {
            id,
            project_id: "frankensqlite-df8c83ae".to_owned(),
            worker_id: "ovh-a".to_owned(),
            command: ["cargo", "check", "--workspace"]
                .map(str::to_owned)
                .to_vec(),
        };
        let mut adopted = None;
        pin_adopted_job(&mut adopted, job(41), "ovh-a").expect("adopt first identity");
        pin_adopted_job(&mut adopted, job(41), "ovh-a").expect("accept same identity");
        assert_eq!(adopted.as_ref().map(|candidate| candidate.id), Some(41));
        assert!(
            pin_adopted_job(&mut adopted, job(42), "ovh-a")
                .expect_err("identity drift must fail")
                .contains("distinct active job identities")
        );
        assert_eq!(adopted.as_ref().map(|candidate| candidate.id), Some(41));
    }

    #[test]
    fn completed_history_distinguishes_absent_success_and_cancellation() {
        let adopted = AdoptedRchJob {
            id: 42,
            project_id: "frankensqlite-df8c83ae".to_owned(),
            worker_id: "ovh-a".to_owned(),
            command: ["cargo", "check", "--workspace"]
                .map(str::to_owned)
                .to_vec(),
        };
        let status = |recent_builds: Vec<Value>| {
            serde_json::to_vec(&serde_json::json!({
                "api_version": "1.0",
                "command": "status",
                "success": true,
                "data": {"daemon": {"active_builds": [], "recent_builds": recent_builds}},
            }))
            .expect("encode completed status")
        };
        let completed = |cancellation: Value| {
            serde_json::json!({
                "id": 42,
                "project_id": "frankensqlite-df8c83ae",
                "worker_id": "ovh-a",
                "command": "cargo check --workspace",
                "location": "remote",
                "exit_code": 0,
                "cancellation": cancellation,
            })
        };

        assert_eq!(
            completed_status_matches(&status(Vec::new()), &adopted),
            Ok(false)
        );
        assert_eq!(
            completed_status_matches(&status(vec![completed(Value::Null)]), &adopted),
            Ok(true)
        );
        assert!(
            completed_status_matches(
                &status(vec![completed(Value::String("cancelled".to_owned()))]),
                &adopted,
            )
            .expect_err("cancelled history must fail")
            .contains("does not match")
        );
    }

    #[test]
    fn external_pack_manifest_source_names_match_real_producers() {
        assert_eq!(
            external_pack_source_names(PackKind::C1),
            [
                "c1_scorecard.json",
                "c1_pack_manifest.json",
                "build_metadata.json"
            ]
        );
        assert_eq!(
            external_pack_source_names(PackKind::Persistent),
            [
                "persistent_scorecard.json",
                "persistent_pack_manifest.json",
                "provenance/citation_receipt.json",
            ]
        );
    }

    #[test]
    fn generic_manifest_source_name_is_refused_by_both_pack_contracts() {
        for kind in [PackKind::C1, PackKind::Persistent] {
            assert!(!external_pack_source_names(kind).contains(&"manifest.json"));
        }
    }

    #[test]
    fn persistent_citation_requires_v2_decimal_scheduler_receipts() {
        let commit = "a".repeat(40);
        let digest = "b".repeat(64);
        let trace = |phase: &str, job_id: &str| {
            serde_json::json!({
                "path": format!("{phase}/rch_status.jsonl"),
                "sha256": digest,
                "job_id": job_id,
                "active_samples": 3,
                "completion": format!("{phase}/rch_status_completion.json"),
                "completion_sha256": digest,
            })
        };
        let mut citation = serde_json::json!({
            "schema_version": "fsqlite.release_persistent_phase_pack_citation_receipt.v2",
            "source": {"commit": commit, "clean": true},
            "build": {
                "profile": "release",
                "expected_opt_level": "z",
                "rustflags": {
                    "RUSTFLAGS": "",
                    "CARGO_ENCODED_RUSTFLAGS": "",
                    "CARGO_BUILD_RUSTFLAGS": "",
                    "policy": "all three values must be empty; remote cargo is invoked through env -u for each",
                },
                "nonce": "c".repeat(64),
            },
            "rch": {
                "actual_worker": "ovh-a",
                "actual_host": "worker.example.test",
                "require_remote": true,
                "no_self_healing": true,
            },
            "rch_scheduler_isolation": {
                "build_status_trace": "provenance/rch_build_status.jsonl",
                "build_status_trace_sha256": digest,
                "build_completion_snapshot": "provenance/rch_build_status_completion.json",
                "build_completion_snapshot_sha256": digest,
                "build_job_id": "29960952048779266",
                "build_active_samples": 3,
                "job_id_encoding": "decimal_string",
                "phase_traces": {
                    "1t": trace("1t", "29960952048779267"),
                    "8t": trace("8t", "29960952048779268"),
                    "16t": trace("16t", "29960952048779269"),
                },
            },
            "workload": {
                "benchmark": "persistent_concurrent_write_{1,8,16}t",
                "rows_per_thread": 1000,
                "synchronous": "NORMAL",
                "threads": [1, 8, 16],
                "criterion": {
                    "sample_size": 10,
                    "warmup_secs": 1,
                    "measurement_secs": 1,
                    "export_root": "{phase}/criterion_measurements",
                    "headline_source": "{phase}/criterion_measurements/{label}/{engine}/base/estimates.json",
                },
            },
        });
        assert!(
            validate_persistent_citation_receipt(
                &serde_json::to_vec(&citation).expect("encode v2 citation"),
                &commit,
                PersistentProfile::Release,
            )
            .is_ok()
        );

        assert!(
            validate_persistent_citation_receipt(
                &serde_json::to_vec(&citation).expect("encode release citation"),
                &commit,
                PersistentProfile::ReleasePerf,
            )
            .is_err()
        );

        citation["schema_version"] =
            serde_json::json!("fsqlite.release_persistent_phase_pack_citation_receipt.v1");
        assert!(
            validate_persistent_citation_receipt(
                &serde_json::to_vec(&citation).expect("encode v1 citation"),
                &commit,
                PersistentProfile::Release,
            )
            .is_err()
        );

        citation["schema_version"] =
            serde_json::json!("fsqlite.release_persistent_phase_pack_citation_receipt.v2");
        citation["rch_scheduler_isolation"]["build_job_id"] =
            serde_json::json!(29_960_952_048_779_266_u64);
        assert!(
            validate_persistent_citation_receipt(
                &serde_json::to_vec(&citation).expect("encode numeric job id"),
                &commit,
                PersistentProfile::Release,
            )
            .is_err()
        );
    }

    #[test]
    fn capture_cargo_profile_binding_rejects_swapped_profiles() {
        assert!(
            validate_cargo_profile_binding(
                "scorecard",
                PersistentProfile::Release,
                Some("release"),
                Some("z"),
            )
            .is_ok()
        );
        assert!(
            validate_cargo_profile_binding(
                "pack manifest",
                PersistentProfile::ReleasePerf,
                Some("release-perf"),
                Some("3"),
            )
            .is_ok()
        );

        // Swapped whole pairs, and single-field swaps in either direction.
        for (profile, cargo_profile, opt_level) in [
            (PersistentProfile::Release, "release-perf", "3"),
            (PersistentProfile::ReleasePerf, "release", "z"),
            (PersistentProfile::Release, "release", "3"),
            (PersistentProfile::ReleasePerf, "release-perf", "z"),
            (PersistentProfile::Release, "release-perf", "z"),
            (PersistentProfile::ReleasePerf, "release", "3"),
        ] {
            assert!(
                validate_cargo_profile_binding(
                    "scorecard",
                    profile,
                    Some(cargo_profile),
                    Some(opt_level),
                )
                .is_err(),
                "{cargo_profile}/{opt_level} must not satisfy the {} binding",
                profile.manifest_name()
            );
        }

        // Omitting or blanking the binding is never a silent exemption.
        for (cargo_profile, opt_level) in [
            (None, None),
            (Some("release"), None),
            (None, Some("z")),
            (Some(""), Some("")),
        ] {
            assert!(
                validate_cargo_profile_binding(
                    "pack manifest",
                    PersistentProfile::Release,
                    cargo_profile,
                    opt_level,
                )
                .is_err(),
                "absent or empty cargo profile binding must fail closed"
            );
        }
    }

    #[test]
    fn capture_persistent_profiles_reject_equal_build_nonces() {
        let workload = || PersistentWorkload {
            benchmark: "persistent_concurrent_write_{1,8,16}t".to_owned(),
            rows_per_thread: 1000,
            synchronous: "NORMAL".to_owned(),
            threads: vec![1, 8, 16],
            criterion: PersistentCriterion {
                sample_size: 10,
                warmup_secs: 1,
                measurement_secs: 1,
                export_root: "{phase}/criterion_measurements".to_owned(),
                headline_source:
                    "{phase}/criterion_measurements/{label}/{engine}/base/estimates.json".to_owned(),
            },
        };
        let identity = |nonce: &str, worker: &str, host: &str| PersistentCitationIdentity {
            actual_worker: worker.to_owned(),
            actual_host: host.to_owned(),
            nonce: nonce.to_owned(),
            workload: workload(),
        };
        let release_nonce = "c".repeat(64);
        let release_perf_nonce = "d".repeat(64);

        assert!(
            validate_persistent_profile_pairing(
                &identity(&release_nonce, "ovh-a", "worker.example.test"),
                &identity(&release_perf_nonce, "ovh-a", "worker.example.test"),
            )
            .is_ok()
        );

        let replayed = validate_persistent_profile_pairing(
            &identity(&release_nonce, "ovh-a", "worker.example.test"),
            &identity(&release_nonce, "ovh-a", "worker.example.test"),
        )
        .expect_err("equal build nonces must not pair");
        assert!(
            replayed.contains("distinct build nonces"),
            "equal-nonce rejection must name the nonce contract; got `{replayed}`"
        );

        // A fresh nonce cannot buy admission for a cross-worker or cross-host pair.
        assert!(
            validate_persistent_profile_pairing(
                &identity(&release_nonce, "ovh-a", "worker.example.test"),
                &identity(&release_perf_nonce, "ovh-b", "worker.example.test"),
            )
            .is_err()
        );
        assert!(
            validate_persistent_profile_pairing(
                &identity(&release_nonce, "ovh-a", "worker.example.test"),
                &identity(&release_perf_nonce, "ovh-a", "other.example.test"),
            )
            .is_err()
        );
    }
}
