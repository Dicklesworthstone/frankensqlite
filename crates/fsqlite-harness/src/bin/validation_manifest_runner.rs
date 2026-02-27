use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};
use std::time::{SystemTime, UNIX_EPOCH};

use sha2::{Digest, Sha256};

use fsqlite_harness::validation_manifest::{
    ARTIFACT_HASH_RATCHET_BASELINE_SCHEMA_VERSION, ARTIFACT_HASH_RATCHET_BEAD_ID,
    ArtifactHashRatchetBaseline, VALIDATION_MANIFEST_SCENARIO_ID, ValidationManifestConfig,
    build_validation_manifest_bundle, evaluate_artifact_hash_ratchet,
    validate_artifact_bundle_integrity_report, validate_artifact_hash_baseline,
    validate_manifest_contract,
};

const DEFAULT_ARTIFACT_PREFIX: &str = "artifacts/validation-manifest";

#[derive(Debug, Clone)]
struct Config {
    workspace_root: PathBuf,
    output_dir: PathBuf,
    output_json: PathBuf,
    output_human: PathBuf,
    commit_sha: String,
    run_id: String,
    trace_id: String,
    scenario_id: String,
    root_seed: Option<u64>,
    generated_unix_ms: u128,
    artifact_uri_prefix: String,
    artifact_hash_baseline: Option<PathBuf>,
    artifact_hash_update_reason: Option<String>,
    allow_artifact_hash_bootstrap: bool,
    write_artifact_hash_baseline: bool,
}

impl Config {
    #[allow(clippy::too_many_lines)]
    fn parse() -> Result<Self, String> {
        let mut workspace_root = default_workspace_root()?;
        let mut output_dir = workspace_root.join(DEFAULT_ARTIFACT_PREFIX);
        let mut output_json: Option<PathBuf> = None;
        let mut output_human: Option<PathBuf> = None;
        let mut commit_sha: Option<String> = None;
        let mut run_id: Option<String> = None;
        let mut trace_id: Option<String> = None;
        let mut scenario_id = VALIDATION_MANIFEST_SCENARIO_ID.to_owned();
        let mut root_seed = Some(424_242_u64);
        let mut generated_unix_ms = now_unix_ms();
        let mut artifact_uri_prefix = DEFAULT_ARTIFACT_PREFIX.to_owned();
        let mut artifact_hash_baseline: Option<PathBuf> = None;
        let mut artifact_hash_update_reason: Option<String> = None;
        let mut allow_artifact_hash_bootstrap = false;
        let mut write_artifact_hash_baseline = false;

        let args: Vec<String> = env::args().skip(1).collect();
        let mut index = 0_usize;
        while index < args.len() {
            match args[index].as_str() {
                "--workspace-root" => {
                    index += 1;
                    let value = args
                        .get(index)
                        .ok_or_else(|| "missing value for --workspace-root".to_owned())?;
                    workspace_root = PathBuf::from(value);
                }
                "--output-dir" => {
                    index += 1;
                    let value = args
                        .get(index)
                        .ok_or_else(|| "missing value for --output-dir".to_owned())?;
                    output_dir = PathBuf::from(value);
                }
                "--output-json" => {
                    index += 1;
                    let value = args
                        .get(index)
                        .ok_or_else(|| "missing value for --output-json".to_owned())?;
                    output_json = Some(PathBuf::from(value));
                }
                "--output-human" => {
                    index += 1;
                    let value = args
                        .get(index)
                        .ok_or_else(|| "missing value for --output-human".to_owned())?;
                    output_human = Some(PathBuf::from(value));
                }
                "--commit-sha" => {
                    index += 1;
                    let value = args
                        .get(index)
                        .ok_or_else(|| "missing value for --commit-sha".to_owned())?;
                    commit_sha = Some(value.to_owned());
                }
                "--run-id" => {
                    index += 1;
                    let value = args
                        .get(index)
                        .ok_or_else(|| "missing value for --run-id".to_owned())?;
                    run_id = Some(value.to_owned());
                }
                "--trace-id" => {
                    index += 1;
                    let value = args
                        .get(index)
                        .ok_or_else(|| "missing value for --trace-id".to_owned())?;
                    trace_id = Some(value.to_owned());
                }
                "--scenario-id" => {
                    index += 1;
                    let value = args
                        .get(index)
                        .ok_or_else(|| "missing value for --scenario-id".to_owned())?;
                    value.clone_into(&mut scenario_id);
                }
                "--root-seed" => {
                    index += 1;
                    let value = args
                        .get(index)
                        .ok_or_else(|| "missing value for --root-seed".to_owned())?;
                    root_seed =
                        Some(value.parse::<u64>().map_err(|error| {
                            format!("invalid --root-seed value={value}: {error}")
                        })?);
                }
                "--no-root-seed" => {
                    root_seed = None;
                }
                "--generated-unix-ms" => {
                    index += 1;
                    let value = args
                        .get(index)
                        .ok_or_else(|| "missing value for --generated-unix-ms".to_owned())?;
                    generated_unix_ms = value.parse::<u128>().map_err(|error| {
                        format!("invalid --generated-unix-ms value={value}: {error}")
                    })?;
                }
                "--artifact-uri-prefix" => {
                    index += 1;
                    let value = args
                        .get(index)
                        .ok_or_else(|| "missing value for --artifact-uri-prefix".to_owned())?;
                    value.clone_into(&mut artifact_uri_prefix);
                }
                "--artifact-hash-baseline" => {
                    index += 1;
                    let value = args
                        .get(index)
                        .ok_or_else(|| "missing value for --artifact-hash-baseline".to_owned())?;
                    artifact_hash_baseline = Some(PathBuf::from(value));
                }
                "--no-artifact-hash-baseline" => {
                    artifact_hash_baseline = None;
                }
                "--artifact-hash-update-reason" => {
                    index += 1;
                    let value = args.get(index).ok_or_else(|| {
                        "missing value for --artifact-hash-update-reason".to_owned()
                    })?;
                    artifact_hash_update_reason = Some(value.to_owned());
                }
                "--allow-artifact-hash-bootstrap" => {
                    allow_artifact_hash_bootstrap = true;
                }
                "--write-artifact-hash-baseline" => {
                    write_artifact_hash_baseline = true;
                }
                "--help" | "-h" => {
                    print_help();
                    std::process::exit(0);
                }
                other => return Err(format!("unknown_argument: {other}")),
            }
            index += 1;
        }

        let commit_sha = commit_sha.unwrap_or_else(|| resolve_commit_sha(&workspace_root));
        let run_id = run_id.unwrap_or_else(|| {
            format!("bd-mblr.3.5.1-{}-{}", generated_unix_ms, std::process::id())
        });
        let trace_id = trace_id.unwrap_or_else(|| build_trace_id(&run_id));

        let output_json =
            output_json.unwrap_or_else(|| output_dir.join("validation_manifest.json"));
        let output_human =
            output_human.unwrap_or_else(|| output_dir.join("validation_manifest.md"));

        if artifact_uri_prefix.trim().is_empty() {
            return Err("artifact_uri_prefix must be non-empty".to_owned());
        }
        if scenario_id.trim().is_empty() {
            return Err("scenario_id must be non-empty".to_owned());
        }

        Ok(Self {
            workspace_root,
            output_dir,
            output_json,
            output_human,
            commit_sha,
            run_id,
            trace_id,
            scenario_id,
            root_seed,
            generated_unix_ms,
            artifact_uri_prefix,
            artifact_hash_baseline,
            artifact_hash_update_reason,
            allow_artifact_hash_bootstrap,
            write_artifact_hash_baseline,
        })
    }
}

fn print_help() {
    println!(
        "\
validation_manifest_runner — machine-readable validation manifest generator (bd-mblr.3.5.1)

USAGE:
  cargo run -p fsqlite-harness --bin validation_manifest_runner -- [OPTIONS]

OPTIONS:
  --workspace-root <PATH>      Workspace root (default: auto-detected)
  --output-dir <PATH>          Output artifact directory (default: artifacts/validation-manifest)
  --output-json <PATH>         Final manifest JSON path (default: <output-dir>/validation_manifest.json)
  --output-human <PATH>        Human summary path (default: <output-dir>/validation_manifest.md)
  --commit-sha <SHA>           Commit SHA to embed (default: git rev-parse HEAD or unknown)
  --run-id <ID>                Deterministic run identifier
  --trace-id <ID>              Deterministic trace identifier
  --scenario-id <ID>           Scenario identifier (default: QUALITY-351)
  --root-seed <U64>            Deterministic orchestrator root seed (default: 424242)
  --no-root-seed               Use canonical orchestrator default seed source
  --generated-unix-ms <U128>   Deterministic timestamp for manifest and gate records
  --artifact-uri-prefix <URI>  URI prefix for gate artifacts (default: artifacts/validation-manifest)
  --artifact-hash-baseline <PATH>
                               Artifact hash ratchet baseline JSON path (optional)
  --no-artifact-hash-baseline  Disable artifact hash ratchet baseline checks
  --artifact-hash-update-reason <TEXT>
                               Reviewed rationale for expected hash drift / baseline bootstrap
  --allow-artifact-hash-bootstrap
                               Allow baseline bootstrap when baseline file is missing
  --write-artifact-hash-baseline
                               Write/update baseline file when ratchet decision requires update
  -h, --help                   Show help
"
    );
}

fn default_workspace_root() -> Result<PathBuf, String> {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .map_err(|error| format!("workspace_root_canonicalize_failed: {error}"))
}

fn now_unix_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis())
}

fn build_trace_id(run_id: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(run_id.as_bytes());
    let digest = hasher.finalize();
    let hex = format!("{digest:x}");
    let short = &hex[..16];
    format!("trace-{short}")
}

fn resolve_commit_sha(workspace_root: &Path) -> String {
    let output = Command::new("git")
        .args([
            "-C",
            &workspace_root.display().to_string(),
            "rev-parse",
            "HEAD",
        ])
        .output();
    match output {
        Ok(result) if result.status.success() => {
            let value = String::from_utf8_lossy(&result.stdout).trim().to_owned();
            if value.is_empty() {
                "unknown".to_owned()
            } else {
                value
            }
        }
        _ => "unknown".to_owned(),
    }
}

fn write_text(path: &Path, content: &str) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|error| {
            format!(
                "output_parent_create_failed path={} error={error}",
                parent.display()
            )
        })?;
    }
    fs::write(path, content)
        .map_err(|error| format!("output_write_failed path={} error={error}", path.display()))
}

fn read_optional_artifact_hash_baseline(
    baseline_path: &Path,
) -> Result<Option<ArtifactHashRatchetBaseline>, String> {
    if !baseline_path.exists() {
        return Ok(None);
    }
    let raw = fs::read_to_string(baseline_path).map_err(|error| {
        format!(
            "artifact_hash_baseline_read_failed path={} error={error}",
            baseline_path.display()
        )
    })?;
    let baseline = ArtifactHashRatchetBaseline::from_json(&raw).map_err(|error| {
        format!(
            "artifact_hash_baseline_parse_failed path={} error={error}",
            baseline_path.display()
        )
    })?;
    let baseline_errors = validate_artifact_hash_baseline(&baseline);
    if !baseline_errors.is_empty() {
        return Err(format!(
            "artifact_hash_baseline_validation_failed path={} errors={}",
            baseline_path.display(),
            baseline_errors.join("; ")
        ));
    }
    Ok(Some(baseline))
}

fn build_artifact_hash_baseline(
    scenario_id: &str,
    bundle_hash: &str,
    generated_unix_ms: u128,
    update_reason: &str,
) -> ArtifactHashRatchetBaseline {
    ArtifactHashRatchetBaseline {
        schema_version: ARTIFACT_HASH_RATCHET_BASELINE_SCHEMA_VERSION.to_owned(),
        bead_id: ARTIFACT_HASH_RATCHET_BEAD_ID.to_owned(),
        scenario_id: scenario_id.to_owned(),
        bundle_hash: bundle_hash.to_owned(),
        updated_unix_ms: generated_unix_ms,
        update_reason: update_reason.to_owned(),
    }
}

fn run() -> Result<bool, String> {
    let config = Config::parse()?;

    fs::create_dir_all(&config.output_dir).map_err(|error| {
        format!(
            "output_dir_create_failed path={} error={error}",
            config.output_dir.display()
        )
    })?;

    let bundle = build_validation_manifest_bundle(&ValidationManifestConfig {
        commit_sha: config.commit_sha.clone(),
        run_id: config.run_id.clone(),
        trace_id: config.trace_id.clone(),
        scenario_id: config.scenario_id.clone(),
        generated_unix_ms: config.generated_unix_ms,
        root_seed: config.root_seed,
        artifact_uri_prefix: config.artifact_uri_prefix.clone(),
    })?;

    let manifest_errors = validate_manifest_contract(&bundle.manifest);
    if !manifest_errors.is_empty() {
        return Err(format!(
            "manifest_contract_validation_failed: {}",
            manifest_errors.join("; ")
        ));
    }

    let integrity_errors = validate_artifact_bundle_integrity_report(
        &bundle.manifest,
        &bundle.gate_artifacts,
        &bundle.artifact_bundle_integrity,
    );
    if !integrity_errors.is_empty() {
        return Err(format!(
            "artifact_bundle_integrity_validation_failed: {}",
            integrity_errors.join("; ")
        ));
    }

    if let Some(baseline_path) = &config.artifact_hash_baseline {
        let baseline = read_optional_artifact_hash_baseline(baseline_path)?;
        if let Some(existing) = &baseline
            && existing.scenario_id != bundle.manifest.scenario_id
        {
            return Err(format!(
                "artifact_hash_baseline_scenario_mismatch baseline={} manifest={}",
                existing.scenario_id, bundle.manifest.scenario_id
            ));
        }

        let decision = evaluate_artifact_hash_ratchet(
            baseline.as_ref(),
            &bundle.artifact_bundle_integrity.bundle_hash,
            config.artifact_hash_update_reason.as_deref(),
            config.allow_artifact_hash_bootstrap,
        );

        if !decision.approved {
            return Err(format!(
                "artifact_hash_ratchet_blocked reason={} baseline_hash={} candidate_hash={} update_reason={}",
                decision.reason,
                decision
                    .baseline_hash
                    .clone()
                    .unwrap_or_else(|| "none".to_owned()),
                decision.candidate_hash,
                decision
                    .update_reason
                    .clone()
                    .unwrap_or_else(|| "none".to_owned()),
            ));
        }

        if decision.requires_baseline_update {
            if !config.write_artifact_hash_baseline {
                return Err(format!(
                    "artifact_hash_ratchet_requires_baseline_update reason={} baseline_path={} (rerun with --write-artifact-hash-baseline)",
                    decision.reason,
                    baseline_path.display()
                ));
            }
            let reason = decision
                .update_reason
                .as_deref()
                .ok_or_else(|| "artifact_hash_ratchet_missing_update_reason".to_owned())?;
            let updated_baseline = build_artifact_hash_baseline(
                &bundle.manifest.scenario_id,
                &bundle.artifact_bundle_integrity.bundle_hash,
                config.generated_unix_ms,
                reason,
            );
            let baseline_json = updated_baseline
                .to_json()
                .map_err(|error| format!("artifact_hash_baseline_serialize_failed: {error}"))?;
            write_text(baseline_path, &baseline_json)?;
        }
    }

    for (artifact_uri, payload) in &bundle.gate_artifacts {
        let artifact_path = config.workspace_root.join(artifact_uri);
        write_text(&artifact_path, payload)?;
    }

    let manifest_json = bundle
        .manifest
        .to_json()
        .map_err(|error| format!("manifest_serialize_failed: {error}"))?;
    write_text(&config.output_json, &manifest_json)?;
    write_text(&config.output_human, &bundle.human_summary)?;

    println!(
        "INFO validation_manifest_written path={} outcome={} gates={} artifacts={}",
        config.output_json.display(),
        bundle.manifest.overall_outcome,
        bundle.manifest.gates.len(),
        bundle.manifest.artifact_uris.len(),
    );
    println!(
        "INFO validation_manifest_summary_written path={}",
        config.output_human.display()
    );
    println!(
        "INFO validation_manifest_replay command=\"{}\"",
        bundle.manifest.replay.command
    );
    println!(
        "INFO artifact_bundle_integrity hash={} artifacts={}",
        bundle.artifact_bundle_integrity.bundle_hash,
        bundle
            .artifact_bundle_integrity
            .artifact_payload_hashes
            .len(),
    );

    Ok(bundle.manifest.overall_pass)
}

fn main() -> ExitCode {
    match run() {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => {
            eprintln!("ERROR validation_manifest_runner overall_pass=false");
            ExitCode::from(1)
        }
        Err(error) => {
            eprintln!("ERROR validation_manifest_runner failed: {error}");
            ExitCode::from(2)
        }
    }
}
