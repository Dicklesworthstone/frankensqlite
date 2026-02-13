use std::env;
use std::path::PathBuf;
use std::process::ExitCode;

use fsqlite_harness::parity_evidence_matrix::{
    BEAD_ID, generate_workspace_parity_evidence_report, render_violation_diagnostics,
};

#[derive(Debug)]
struct CliConfig {
    workspace_root: PathBuf,
    output_path: Option<PathBuf>,
}

fn print_help() {
    let help = "\
parity_evidence_matrix_gate — parity evidence contract validator (bd-1dp9.7.5)

USAGE:
    cargo run -p fsqlite-harness --bin parity_evidence_matrix_gate -- [OPTIONS]

OPTIONS:
    --workspace-root <PATH>   Workspace root containing .beads/issues.jsonl (default: current dir)
    --output <PATH>           Write JSON report to path (stdout when omitted)
    -h, --help                Show this help
";
    println!("{help}");
}

fn parse_args(args: &[String]) -> Result<CliConfig, String> {
    let mut workspace_root = PathBuf::from(".");
    let mut output_path: Option<PathBuf> = None;

    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--workspace-root" => {
                index += 1;
                if index >= args.len() {
                    return Err("--workspace-root requires a value".to_owned());
                }
                workspace_root = PathBuf::from(&args[index]);
            }
            "--output" => {
                index += 1;
                if index >= args.len() {
                    return Err("--output requires a value".to_owned());
                }
                output_path = Some(PathBuf::from(&args[index]));
            }
            "-h" | "--help" => {
                print_help();
                return Err(String::new());
            }
            unknown => {
                return Err(format!("unknown option: {unknown}"));
            }
        }
        index += 1;
    }

    Ok(CliConfig {
        workspace_root,
        output_path,
    })
}

fn run(args: &[String]) -> Result<i32, String> {
    let config = parse_args(args)?;
    let report = generate_workspace_parity_evidence_report(&config.workspace_root)?;

    let payload = serde_json::to_string_pretty(&report)
        .map_err(|error| format!("report_serialize_failed: {error}"))?;

    if let Some(output_path) = &config.output_path {
        std::fs::write(output_path, payload).map_err(|error| {
            format!(
                "report_write_failed path={} error={error}",
                output_path.display()
            )
        })?;
    } else {
        println!("{payload}");
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
            eprintln!("ERROR bead_id={BEAD_ID} parity_evidence_matrix_gate failed: {error}");
            ExitCode::from(2)
        }
    }
}
