use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use fsqlite_harness::parity_verification_workflow::{
    BEAD_ID, WorkflowInput, build_workflow_report, render_workflow_markdown,
};
use sha2::{Digest, Sha256};

const DEFAULT_OUTPUT_DIR: &str = "artifacts/parity-verification-workflow";

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

OPTIONS:
    --workspace-root <PATH>   Workspace root (default: current checkout)
    --input-json <PATH>       Workflow observation JSON from the one-command wrapper
    --output-dir <PATH>       Output directory (default: artifacts/parity-verification-workflow)
    --output-json <PATH>      JSON workflow report path
    --output-human <PATH>     Markdown workflow navigator path
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
