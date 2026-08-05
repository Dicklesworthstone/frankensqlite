use std::collections::BTreeSet;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use fsqlite_harness::parity_verification_workflow::{
    BEAD_ID, WorkflowInput, build_workflow_report, render_workflow_markdown,
};
use fsqlite_harness::release_certificate::{
    StrictCertificateRunConfig, build_and_publish_strict_certificate,
};
use sha2::{Digest, Sha256};

const DEFAULT_OUTPUT_DIR: &str = "artifacts/parity-verification-workflow";
const CERTIFICATE_ONLY_SELECTORS: &[&str] = &[
    "--certificate-evidence-root",
    "--certificate-evidence-json",
    "--candidate-git-sha",
    "--baseline-metadata-git-sha",
    "--candidate-rch-project-id",
    "--baseline-rch-project-id",
    "--certificate-output-dir",
    "--certificate-output-json",
    "--certificate-output-human",
];

#[derive(Debug)]
struct Config {
    workspace_root: PathBuf,
    input_json: PathBuf,
    output_json: PathBuf,
    output_human: PathBuf,
}

#[derive(Debug)]
struct CertificateCliConfig {
    run: StrictCertificateRunConfig,
}

impl CertificateCliConfig {
    fn parse(args: &[String]) -> Result<Self, String> {
        Self::parse_with_default_workspace(args, default_workspace_root)
    }

    fn parse_with_default_workspace(
        args: &[String],
        default_workspace_root: impl FnOnce() -> PathBuf,
    ) -> Result<Self, String> {
        reject_duplicate_certificate_selectors(args)?;

        let mut workspace_root = default_workspace_root();
        let mut evidence_root = None;
        let mut evidence_json = None;
        let mut candidate_git_sha = None;
        let mut baseline_metadata_git_sha = None;
        let mut candidate_rch_project_id = None;
        let mut baseline_rch_project_id = None;
        let mut output_dir = None;

        let mut index = 0_usize;
        while let Some(arg) = args.get(index) {
            match arg.as_str() {
                "--workspace-root" => {
                    index += 1;
                    workspace_root = PathBuf::from(required_arg(args, index, "--workspace-root")?);
                }
                "--certificate-evidence-root" => {
                    index += 1;
                    evidence_root = Some(PathBuf::from(required_arg(
                        args,
                        index,
                        "--certificate-evidence-root",
                    )?));
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
                "--baseline-metadata-git-sha" => {
                    index += 1;
                    baseline_metadata_git_sha =
                        Some(required_arg(args, index, "--baseline-metadata-git-sha")?.to_owned());
                }
                "--candidate-rch-project-id" => {
                    index += 1;
                    candidate_rch_project_id =
                        Some(required_arg(args, index, "--candidate-rch-project-id")?.to_owned());
                }
                "--baseline-rch-project-id" => {
                    index += 1;
                    baseline_rch_project_id =
                        Some(required_arg(args, index, "--baseline-rch-project-id")?.to_owned());
                }
                "--certificate-output-dir" => {
                    index += 1;
                    output_dir = Some(PathBuf::from(required_arg(
                        args,
                        index,
                        "--certificate-output-dir",
                    )?));
                }
                "--certificate-output-json" | "--certificate-output-human" => {
                    return Err(format!(
                        "{arg} is unsupported; strict certificate mode publishes one atomic four-file directory via --certificate-output-dir"
                    ));
                }
                "-h" | "--help" => {
                    print_help();
                    return Err(String::new());
                }
                unknown => {
                    return Err(format!(
                        "certificate mode does not accept workflow argument: {unknown}"
                    ));
                }
            }
            index += 1;
        }

        let evidence_root =
            evidence_root.ok_or_else(|| "--certificate-evidence-root is required".to_owned())?;
        Ok(Self {
            run: StrictCertificateRunConfig {
                workspace_root: workspace_root.clone(),
                evidence_root: resolve_path(&workspace_root, &evidence_root),
                evidence_json: evidence_json
                    .ok_or_else(|| "--certificate-evidence-json is required".to_owned())?,
                candidate_git_sha: candidate_git_sha
                    .ok_or_else(|| "--candidate-git-sha is required".to_owned())?,
                baseline_metadata_git_sha: baseline_metadata_git_sha
                    .ok_or_else(|| "--baseline-metadata-git-sha is required".to_owned())?,
                candidate_rch_project_id: candidate_rch_project_id
                    .ok_or_else(|| "--candidate-rch-project-id is required".to_owned())?,
                baseline_rch_project_id: baseline_rch_project_id
                    .ok_or_else(|| "--baseline-rch-project-id is required".to_owned())?,
                output_dir: output_dir
                    .ok_or_else(|| "--certificate-output-dir is required".to_owned())?,
            },
        })
    }
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

OPTIONS:
    --workspace-root <PATH>   Workspace root (default: current checkout)
    --input-json <PATH>       Workflow observation JSON from the one-command wrapper
    --output-dir <PATH>       Output directory (default: artifacts/parity-verification-workflow)
    --output-json <PATH>      JSON workflow report path
    --output-human <PATH>     Markdown workflow navigator path
    -h, --help                Show this help

STRICT CERTIFICATE MODE:
    --certificate-evidence-root <PATH>
                              Explicit trust root; must resolve to the candidate workspace root
    --certificate-evidence-json <RELATIVE_PATH>
                              Strict candidate evidence manifest below the evidence root
    --candidate-git-sha <SHA> Exact lowercase 40-hex checked-out candidate
    --baseline-metadata-git-sha <SHA>
                              Exact commit containing the baseline and its historical receipts
    --candidate-rch-project-id <ID>
                              Exact RCH project identity for candidate and live-guard runs
    --baseline-rch-project-id <ID>
                              Exact RCH project identity for the historical baseline run
    --certificate-output-dir <NEW_PATH>
                              New atomic bundle directory containing exactly four files
    Every strict-certificate selector, including --workspace-root, may be supplied at most once.

REJECTED CERTIFICATE SELECTORS:
    --certificate-output-json <PATH>
                              Split output would bypass atomic bundle publication
    --certificate-output-human <PATH>
                              Split output would bypass atomic bundle publication
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

fn run(args: &[String]) -> Result<i32, String> {
    if certificate_mode_requested(args) {
        let config = CertificateCliConfig::parse(args)?;
        let certificate = build_and_publish_strict_certificate(&config.run)?;
        println!(
            "INFO release_certificate_bundle_written candidate_git_sha={} output_dir={} verdict={}",
            config.run.candidate_git_sha,
            config.run.output_dir.display(),
            certificate.verdict,
        );
        return Ok(0);
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

fn is_certificate_mode_selector(argument: &str) -> bool {
    CERTIFICATE_ONLY_SELECTORS.contains(&argument)
}

fn is_duplicate_guarded_certificate_selector(argument: &str) -> bool {
    argument == "--workspace-root" || is_certificate_mode_selector(argument)
}

fn reject_duplicate_certificate_selectors(args: &[String]) -> Result<(), String> {
    let mut seen = BTreeSet::new();
    for argument in args {
        let selector = argument.as_str();
        if is_duplicate_guarded_certificate_selector(selector) && !seen.insert(selector) {
            return Err(format!(
                "duplicate certificate selector: {selector} may be specified at most once"
            ));
        }
    }
    Ok(())
}

fn certificate_mode_requested(args: &[String]) -> bool {
    args.iter()
        .any(|argument| is_certificate_mode_selector(argument))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_certificate_only_selector_activates_strict_mode() {
        for selector in CERTIFICATE_ONLY_SELECTORS.iter().copied() {
            assert!(certificate_mode_requested(&[selector.to_owned()]));
        }
        assert!(!certificate_mode_requested(&["--input-json".to_owned()]));
    }

    #[test]
    fn duplicate_certificate_selectors_fail_before_workspace_resolution() {
        for selector in
            std::iter::once("--workspace-root").chain(CERTIFICATE_ONLY_SELECTORS.iter().copied())
        {
            let default_workspace_was_resolved = std::cell::Cell::new(false);
            let error = CertificateCliConfig::parse_with_default_workspace(
                &[
                    selector.to_owned(),
                    "first-value".to_owned(),
                    selector.to_owned(),
                    "second-value".to_owned(),
                ],
                || {
                    default_workspace_was_resolved.set(true);
                    PathBuf::from("unexpected-default-workspace")
                },
            )
            .expect_err("a repeated certificate selector must be rejected");

            assert_eq!(
                error,
                format!("duplicate certificate selector: {selector} may be specified at most once")
            );
            assert!(
                !default_workspace_was_resolved.get(),
                "duplicate {selector} resolved the default workspace before returning"
            );
        }
    }

    #[test]
    fn certificate_mode_rejects_split_output_flags() {
        let error = CertificateCliConfig::parse(&[
            "--certificate-output-json".to_owned(),
            "certificate.json".to_owned(),
        ])
        .expect_err("split output would bypass atomic publication");
        assert!(error.contains("unsupported"));
    }

    #[test]
    fn certificate_mode_requires_and_preserves_chronology_and_project_proofs() {
        let parsed = CertificateCliConfig::parse(&[
            "--workspace-root".to_owned(),
            ".".to_owned(),
            "--certificate-evidence-root".to_owned(),
            ".".to_owned(),
            "--certificate-evidence-json".to_owned(),
            "strict-input.json".to_owned(),
            "--candidate-git-sha".to_owned(),
            "a".repeat(40),
            "--baseline-metadata-git-sha".to_owned(),
            "b".repeat(40),
            "--candidate-rch-project-id".to_owned(),
            "candidate-project".to_owned(),
            "--baseline-rch-project-id".to_owned(),
            "baseline-project".to_owned(),
            "--certificate-output-dir".to_owned(),
            "certificate".to_owned(),
        ])
        .expect("complete strict invocation");
        assert_eq!(parsed.run.baseline_metadata_git_sha, "b".repeat(40));
        assert_eq!(parsed.run.candidate_rch_project_id, "candidate-project");
        assert_eq!(parsed.run.baseline_rch_project_id, "baseline-project");

        let error = CertificateCliConfig::parse(&[
            "--certificate-evidence-root".to_owned(),
            ".".to_owned(),
            "--certificate-evidence-json".to_owned(),
            "strict-input.json".to_owned(),
            "--candidate-git-sha".to_owned(),
            "a".repeat(40),
            "--certificate-output-dir".to_owned(),
            "certificate".to_owned(),
        ])
        .expect_err("missing chronology proof must fail");
        assert_eq!(error, "--baseline-metadata-git-sha is required");
    }
}
