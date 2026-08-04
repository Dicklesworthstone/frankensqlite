use std::env;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::str::FromStr;

use fsqlite_harness::sql_pipeline_optimization::{
    CandidateDirection, CandidateKey, CandidatePreflightVerdict,
    SQL_PIPELINE_CANDIDATE_PREFLIGHT_BEAD_ID, load_candidate_registry_from_negative_results_ledger,
};

#[derive(Debug)]
struct Config {
    ledger_path: PathBuf,
    workload: String,
    operation: String,
    direction: CandidateDirection,
    benchmark_name: String,
    source_surface: String,
    json: bool,
}

impl Config {
    fn parse() -> Result<Self, String> {
        let mut ledger_path = default_ledger_path();
        let mut workload = "VDBE".to_owned();
        let mut operation = None;
        let mut direction = None;
        let mut benchmark_name = "unknown".to_owned();
        let mut source_surface = "unknown".to_owned();
        let mut json = false;

        let args: Vec<String> = env::args().skip(1).collect();
        let mut idx = 0_usize;
        while let Some(arg) = args.get(idx) {
            match arg.as_str() {
                "--ledger" => {
                    idx += 1;
                    ledger_path = PathBuf::from(required_arg(&args, idx, "--ledger")?);
                }
                "--workload" => {
                    idx += 1;
                    workload = required_arg(&args, idx, "--workload")?;
                }
                "--operation" => {
                    idx += 1;
                    operation = Some(required_arg(&args, idx, "--operation")?);
                }
                "--direction" => {
                    idx += 1;
                    let value = required_arg(&args, idx, "--direction")?;
                    direction = Some(CandidateDirection::from_str(&value)?);
                }
                "--benchmark" => {
                    idx += 1;
                    benchmark_name = required_arg(&args, idx, "--benchmark")?;
                }
                "--source-surface" => {
                    idx += 1;
                    source_surface = required_arg(&args, idx, "--source-surface")?;
                }
                "--json" => json = true,
                "-h" | "--help" => {
                    print_help();
                    std::process::exit(0);
                }
                other => {
                    return Err(format!(
                        "bead_id={SQL_PIPELINE_CANDIDATE_PREFLIGHT_BEAD_ID} unknown_argument={other}"
                    ));
                }
            }
            idx += 1;
        }

        let operation = operation.ok_or_else(|| {
            format!(
                "bead_id={SQL_PIPELINE_CANDIDATE_PREFLIGHT_BEAD_ID} missing_required_arg=--operation"
            )
        })?;
        let direction = direction.ok_or_else(|| {
            format!(
                "bead_id={SQL_PIPELINE_CANDIDATE_PREFLIGHT_BEAD_ID} missing_required_arg=--direction"
            )
        })?;

        Ok(Self {
            ledger_path,
            workload,
            operation,
            direction,
            benchmark_name,
            source_surface,
            json,
        })
    }
}

fn default_ledger_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../docs/progress/perf-negative-results.md")
}

fn required_arg(args: &[String], idx: usize, name: &str) -> Result<String, String> {
    args.get(idx).cloned().ok_or_else(|| {
        format!("bead_id={SQL_PIPELINE_CANDIDATE_PREFLIGHT_BEAD_ID} missing_value_for={name}")
    })
}

fn print_help() {
    println!(
        "\
sql_pipeline_candidate_preflight - T6.2 duplicate perf-candidate gate

USAGE:
  cargo run -p fsqlite-harness --bin sql_pipeline_candidate_preflight -- \\
    --operation <OPERATION> --direction <DIRECTION> [OPTIONS]

OPTIONS:
  --operation <NAME>          Opcode or operation, for example ZeroOrNull
  --direction <DIRECTION>     hot-dispatch-removal | hot-dispatch-promotion | no-retry-harness | other
  --benchmark <NAME>          Benchmark name, for example vdbe_pipeline_execute_zeroornull
  --source-surface <SURFACE>  Source surface, for example try_execute_hot_opcode
  --workload <NAME>           Workload family (default: VDBE)
  --ledger <PATH>             Negative-results ledger path
  --json                      Emit machine-readable report JSON
  -h, --help                  Show this help

Exit 0 means the candidate was not found in the no-retry registry.
Exit 2 means either a sound rejected/non-candidate duplicate was found or a
matched REJECT lacks a recorded same-invocation A/A null control. In the latter
case, re-run the exact candidate under the corrected harness before proposing a
new source mutation.
"
    );
}

fn run() -> Result<ExitCode, String> {
    let config = Config::parse()?;
    let registry = load_candidate_registry_from_negative_results_ledger(&config.ledger_path)?;
    let requested_key = CandidateKey::new(
        config.workload,
        config.operation,
        config.direction,
        config.benchmark_name,
        config.source_surface,
    );
    let report = registry.preflight(&requested_key);

    if config.json {
        let payload = serde_json::to_string_pretty(&report).map_err(|error| {
            format!(
                "bead_id={SQL_PIPELINE_CANDIDATE_PREFLIGHT_BEAD_ID} case=preflight_json_serialize_failed error={error}"
            )
        })?;
        println!("{payload}");
    } else {
        println!("{}", report.summary);
        for record in &report.matched_records {
            println!(
                "match decision={} date={} ledger={} null_control_recorded={} retry_condition={}",
                record.decision,
                record.date,
                record.ledger_entry,
                record.null_control_recorded,
                record.retry_condition
            );
        }
    }

    if report.verdict == CandidatePreflightVerdict::Allowed {
        Ok(ExitCode::SUCCESS)
    } else {
        Ok(ExitCode::from(2))
    }
}

fn main() -> ExitCode {
    match run() {
        Ok(code) => code,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::from(64)
        }
    }
}
