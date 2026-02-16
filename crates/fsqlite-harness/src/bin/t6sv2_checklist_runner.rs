use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use fsqlite_harness::e2e_traceability::TraceabilityMatrix;
use fsqlite_harness::t6sv2_checklist::{
    BEAD_ID, generate_t6sv2_checklist_report, render_violation_diagnostics,
};
use fsqlite_harness::unit_matrix::UnitMatrix;

#[derive(Debug)]
struct CliConfig {
    workspace_root: PathBuf,
    issues_path: Option<PathBuf>,
    unit_matrix_override: Option<PathBuf>,
    traceability_override: Option<PathBuf>,
    output_path: Option<PathBuf>,
    output_human_path: Option<PathBuf>,
    generated_unix_ms: Option<u128>,
}

fn print_help() {
    println!(
        "\
t6sv2_checklist_runner — observability-program checklist validator (bd-t6sv2.16)

USAGE:
  cargo run -p fsqlite-harness --bin t6sv2_checklist_runner -- [OPTIONS]

OPTIONS:
  --workspace-root <PATH>         Workspace root (default: current dir)
  --issues-path <PATH>            Issues JSONL path (default: <workspace>/.beads/issues.jsonl)
  --unit-matrix-override <PATH>   UnitMatrix JSON override file
  --traceability-override <PATH>  TraceabilityMatrix JSON override file
  --output <PATH>                 Write JSON report to file (stdout when omitted)
  --output-human <PATH>           Write Markdown summary to file
  --generated-unix-ms <U128>      Deterministic timestamp override
  -h, --help                      Show this help
"
    );
}

fn parse_args(args: &[String]) -> Result<CliConfig, String> {
    let mut workspace_root = PathBuf::from(".");
    let mut issues_path: Option<PathBuf> = None;
    let mut unit_matrix_override: Option<PathBuf> = None;
    let mut traceability_override: Option<PathBuf> = None;
    let mut output_path: Option<PathBuf> = None;
    let mut output_human_path: Option<PathBuf> = None;
    let mut generated_unix_ms: Option<u128> = None;

    let mut index = 0_usize;
    while index < args.len() {
        match args[index].as_str() {
            "--workspace-root" => {
                index = index.saturating_add(1);
                let Some(value) = args.get(index) else {
                    return Err(String::from("missing value for --workspace-root"));
                };
                workspace_root = PathBuf::from(value);
            }
            "--issues-path" => {
                index = index.saturating_add(1);
                let Some(value) = args.get(index) else {
                    return Err(String::from("missing value for --issues-path"));
                };
                issues_path = Some(PathBuf::from(value));
            }
            "--unit-matrix-override" => {
                index = index.saturating_add(1);
                let Some(value) = args.get(index) else {
                    return Err(String::from("missing value for --unit-matrix-override"));
                };
                unit_matrix_override = Some(PathBuf::from(value));
            }
            "--traceability-override" => {
                index = index.saturating_add(1);
                let Some(value) = args.get(index) else {
                    return Err(String::from("missing value for --traceability-override"));
                };
                traceability_override = Some(PathBuf::from(value));
            }
            "--output" => {
                index = index.saturating_add(1);
                let Some(value) = args.get(index) else {
                    return Err(String::from("missing value for --output"));
                };
                output_path = Some(PathBuf::from(value));
            }
            "--output-human" => {
                index = index.saturating_add(1);
                let Some(value) = args.get(index) else {
                    return Err(String::from("missing value for --output-human"));
                };
                output_human_path = Some(PathBuf::from(value));
            }
            "--generated-unix-ms" => {
                index = index.saturating_add(1);
                let Some(value) = args.get(index) else {
                    return Err(String::from("missing value for --generated-unix-ms"));
                };
                generated_unix_ms = Some(
                    value
                        .parse::<u128>()
                        .map_err(|error| format!("invalid --generated-unix-ms: {error}"))?,
                );
            }
            "-h" | "--help" => {
                print_help();
                return Err(String::new());
            }
            unknown => return Err(format!("unknown option: {unknown}")),
        }
        index = index.saturating_add(1);
    }

    Ok(CliConfig {
        workspace_root,
        issues_path,
        unit_matrix_override,
        traceability_override,
        output_path,
        output_human_path,
        generated_unix_ms,
    })
}

fn resolve_path(workspace_root: &Path, candidate: &Path) -> PathBuf {
    if candidate.is_absolute() {
        candidate.to_path_buf()
    } else {
        workspace_root.join(candidate)
    }
}

fn load_json_file<T: serde::de::DeserializeOwned>(path: &Path, label: &str) -> Result<T, String> {
    let payload = fs::read_to_string(path)
        .map_err(|error| format!("{label}_read_failed path={} error={error}", path.display()))?;
    serde_json::from_str(&payload)
        .map_err(|error| format!("{label}_parse_failed path={} error={error}", path.display()))
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

fn run(args: &[String]) -> Result<i32, String> {
    let config = parse_args(args)?;

    let issues_path = config
        .issues_path
        .as_ref()
        .map_or_else(
            || config.workspace_root.join(".beads/issues.jsonl"),
            |path| resolve_path(&config.workspace_root, path),
        );

    let unit_matrix = match config.unit_matrix_override {
        Some(path) => {
            let resolved = resolve_path(&config.workspace_root, &path);
            load_json_file::<UnitMatrix>(&resolved, "unit_matrix_override")?
        }
        None => fsqlite_harness::unit_matrix::build_canonical_matrix(),
    };

    let traceability = match config.traceability_override {
        Some(path) => {
            let resolved = resolve_path(&config.workspace_root, &path);
            load_json_file::<TraceabilityMatrix>(&resolved, "traceability_override")?
        }
        None => fsqlite_harness::e2e_traceability::build_canonical_inventory(),
    };

    let report = generate_t6sv2_checklist_report(
        &config.workspace_root,
        &issues_path,
        &unit_matrix,
        &traceability,
        config.generated_unix_ms,
    )?;
    let report_json = serde_json::to_string_pretty(&report)
        .map_err(|error| format!("report_serialize_failed: {error}"))?;

    if let Some(path) = config.output_path.as_ref() {
        write_text(path, &report_json)?;
    } else {
        println!("{report_json}");
    }

    if let Some(path) = config.output_human_path.as_ref() {
        write_text(path, &report.render_markdown())?;
    }

    if report.summary.overall_pass {
        return Ok(0);
    }

    for line in render_violation_diagnostics(&report) {
        eprintln!("WARN bead_id={BEAD_ID} {line}");
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
            eprintln!("ERROR bead_id={BEAD_ID} t6sv2_checklist_runner failed: {error}");
            ExitCode::from(2)
        }
    }
}
