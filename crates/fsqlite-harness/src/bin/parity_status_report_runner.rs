use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::{SystemTime, UNIX_EPOCH};

use fsqlite_harness::oracle_preflight_doctor::OraclePreflightReport;
use fsqlite_harness::parity_status_report::{
    DEFAULT_EVIDENCE_FRESHNESS_BUDGET_MS, DifferentialStatusInput, PARITY_STATUS_REPORT_BEAD_ID,
    ParityStatusReportConfig, generate_canonical_parity_status_report,
    render_parity_status_markdown,
};
use serde_json::Value;
use sha2::{Digest, Sha256};

const DEFAULT_OUTPUT_DIR: &str = "artifacts/parity-status-report";

#[derive(Debug, Clone)]
struct Config {
    workspace_root: PathBuf,
    output_json: PathBuf,
    output_human: PathBuf,
    oracle_preflight_json: Option<PathBuf>,
    differential_manifest_json: Option<PathBuf>,
    generated_unix_ms: u128,
    freshness_budget_ms: u128,
    allow_incomplete: bool,
}

impl Config {
    fn parse(args: &[String]) -> Result<Self, String> {
        let mut workspace_root = default_workspace_root();
        let mut output_dir: Option<PathBuf> = None;
        let mut output_json: Option<PathBuf> = None;
        let mut output_human: Option<PathBuf> = None;
        let mut oracle_preflight_json: Option<PathBuf> = None;
        let mut differential_manifest_json: Option<PathBuf> = None;
        let mut generated_unix_ms = now_unix_ms();
        let mut freshness_budget_ms = DEFAULT_EVIDENCE_FRESHNESS_BUDGET_MS;
        let mut allow_incomplete = false;

        let mut index = 0_usize;
        while let Some(arg) = args.get(index) {
            match arg.as_str() {
                "--workspace-root" => {
                    index += 1;
                    workspace_root = PathBuf::from(required_arg(args, index, "--workspace-root")?);
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
                "--oracle-preflight-json" => {
                    index += 1;
                    oracle_preflight_json = Some(PathBuf::from(required_arg(
                        args,
                        index,
                        "--oracle-preflight-json",
                    )?));
                }
                "--differential-manifest-json" => {
                    index += 1;
                    differential_manifest_json = Some(PathBuf::from(required_arg(
                        args,
                        index,
                        "--differential-manifest-json",
                    )?));
                }
                "--generated-unix-ms" => {
                    index += 1;
                    let value = required_arg(args, index, "--generated-unix-ms")?;
                    generated_unix_ms = value.parse::<u128>().map_err(|error| {
                        format!("invalid --generated-unix-ms value={value}: {error}")
                    })?;
                }
                "--freshness-budget-ms" => {
                    index += 1;
                    let value = required_arg(args, index, "--freshness-budget-ms")?;
                    freshness_budget_ms = value.parse::<u128>().map_err(|error| {
                        format!("invalid --freshness-budget-ms value={value}: {error}")
                    })?;
                }
                "--allow-incomplete" => {
                    allow_incomplete = true;
                }
                "--help" | "-h" => {
                    print_help();
                    return Err(String::new());
                }
                "--feature-id" => {
                    index += 1;
                    let _ = required_arg(args, index, "--feature-id")?;
                    // Accepted for stable replay pointers; report generation remains global.
                }
                unknown => return Err(format!("unknown argument: {unknown}")),
            }
            index += 1;
        }

        let output_dir = output_dir.unwrap_or_else(|| workspace_root.join(DEFAULT_OUTPUT_DIR));
        let output_json =
            output_json.unwrap_or_else(|| output_dir.join("parity_status_report.json"));
        let output_human =
            output_human.unwrap_or_else(|| output_dir.join("parity_status_report.md"));

        Ok(Self {
            workspace_root,
            output_json,
            output_human,
            oracle_preflight_json,
            differential_manifest_json,
            generated_unix_ms,
            freshness_budget_ms,
            allow_incomplete,
        })
    }
}

fn print_help() {
    let help = "\
parity_status_report_runner — user-facing parity status report (bd-2yqp6.7.5)

USAGE:
    cargo run -p fsqlite-harness --bin parity_status_report_runner -- [OPTIONS]

OPTIONS:
    --workspace-root <PATH>              Workspace root (default: current checkout)
    --output-dir <PATH>                  Output directory (default: artifacts/parity-status-report)
    --output-json <PATH>                 JSON report path
    --output-human <PATH>                Markdown report path
    --oracle-preflight-json <PATH>       Oracle preflight doctor JSON artifact
    --differential-manifest-json <PATH>  Differential manifest JSON artifact
    --generated-unix-ms <MS>             Deterministic report timestamp
    --freshness-budget-ms <MS>           Evidence freshness budget (default: 24h)
    --allow-incomplete                   Emit artifacts even when publication validation fails
    --feature-id <ID>                    Accepted replay pointer selector; currently informational
    -h, --help                           Show this help
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

fn now_unix_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis())
}

fn resolve_path(workspace_root: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        workspace_root.join(path)
    }
}

fn read_json_value(path: &Path) -> Result<Value, String> {
    let payload = fs::read_to_string(path)
        .map_err(|error| format!("json_read_failed path={} error={error}", path.display()))?;
    serde_json::from_str(&payload)
        .map_err(|error| format!("json_parse_failed path={} error={error}", path.display()))
}

fn read_preflight(path: &Path) -> Result<OraclePreflightReport, String> {
    let payload = fs::read_to_string(path).map_err(|error| {
        format!(
            "oracle_preflight_read_failed path={} error={error}",
            path.display()
        )
    })?;
    serde_json::from_str(&payload).map_err(|error| {
        format!(
            "oracle_preflight_parse_failed path={} error={error}",
            path.display()
        )
    })
}

fn sha256_file(path: &Path) -> Result<String, String> {
    let payload = fs::read(path).map_err(|error| {
        format!(
            "artifact_hash_read_failed path={} error={error}",
            path.display()
        )
    })?;
    let mut hasher = Sha256::new();
    hasher.update(payload);
    let digest = hasher.finalize();
    Ok(fsqlite_harness::bytes_to_lower_hex(digest))
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
    let preflight = config
        .oracle_preflight_json
        .as_ref()
        .map(|path| read_preflight(&resolve_path(&config.workspace_root, path)))
        .transpose()?;
    let differential = config
        .differential_manifest_json
        .as_ref()
        .map(|path| {
            let resolved = resolve_path(&config.workspace_root, path);
            let value = read_json_value(&resolved)?;
            let sha256 = sha256_file(&resolved)?;
            DifferentialStatusInput::from_manifest_value(
                &value,
                Some(resolved.display().to_string()),
                Some(sha256),
            )
        })
        .transpose()?;

    let report = generate_canonical_parity_status_report(
        preflight.as_ref(),
        differential,
        ParityStatusReportConfig {
            generated_unix_ms: config.generated_unix_ms,
            freshness_budget_ms: config.freshness_budget_ms,
        },
    );
    let json = serde_json::to_string_pretty(&report)
        .map_err(|error| format!("status_report_serialize_failed: {error}"))?;
    let markdown = render_parity_status_markdown(&report);

    write_text(&config.output_json, &json)?;
    write_text(&config.output_human, &markdown)?;

    println!(
        "INFO parity_status_report_written path={} human_path={} run_complete={} feature_count={} violation_count={}",
        config.output_json.display(),
        config.output_human.display(),
        report.report_complete,
        report.features.len(),
        report.validation_violations.len(),
    );

    if report.report_complete || config.allow_incomplete {
        return Ok(0);
    }
    Ok(1)
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
                "ERROR bead_id={PARITY_STATUS_REPORT_BEAD_ID} parity_status_report_runner failed: {error}"
            );
            ExitCode::from(2)
        }
    }
}
