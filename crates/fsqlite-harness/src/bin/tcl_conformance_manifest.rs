use std::env;
use std::path::PathBuf;
use std::process::ExitCode;

use fsqlite_harness::tcl_conformance::{
    build_validated_tcl_harness_suite, execute_tcl_harness_suite, write_tcl_execution_summary_json,
    write_tcl_suite_json, TclExecutionMode, TclExecutionOptions, BEAD_ID, DEFAULT_TIMEOUT_SECS,
};

#[derive(Debug)]
struct CliConfig {
    output: Option<PathBuf>,
    mode: OutputMode,
    timeout_secs: u64,
    max_scenarios: Option<usize>,
    runner_override: Option<PathBuf>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OutputMode {
    SuiteJson,
    DryRunSummary,
    ExecuteSummary,
}

fn print_help() {
    let help = "\
tcl_conformance_manifest — TCL harness conformance orchestrator (bd-3plop.7)

USAGE:
    cargo run -p fsqlite-harness --bin tcl_conformance_manifest -- [OPTIONS]

OPTIONS:
    --output <PATH>        Write JSON payload to file (stdout if omitted)
    --dry-run              Emit dry-run execution summary JSON
    --execute              Execute scenarios and emit execution summary JSON
    --timeout-secs <u64>   Scenario timeout in seconds (default: 1800)
    --max-scenarios <N>    Limit number of scenarios to execute
    --runner <PATH>        Override testrunner path
    -h, --help             Show this help
";
    println!("{help}");
}

fn set_mode(mode: &mut OutputMode, next: OutputMode, flag: &str) -> Result<(), String> {
    if *mode != OutputMode::SuiteJson {
        return Err(format!(
            "{flag} cannot be combined with another execution mode flag"
        ));
    }
    *mode = next;
    Ok(())
}

fn parse_u64(value: &str) -> Result<u64, String> {
    value
        .parse::<u64>()
        .map_err(|_| format!("invalid u64 value: {value}"))
}

fn parse_usize(value: &str) -> Result<usize, String> {
    value
        .parse::<usize>()
        .map_err(|_| format!("invalid usize value: {value}"))
}

fn parse_args(args: &[String]) -> Result<CliConfig, String> {
    let mut config = CliConfig {
        output: None,
        mode: OutputMode::SuiteJson,
        timeout_secs: DEFAULT_TIMEOUT_SECS,
        max_scenarios: None,
        runner_override: None,
    };

    let mut index = 0usize;
    while index < args.len() {
        match args[index].as_str() {
            "--output" => {
                index += 1;
                if index >= args.len() {
                    return Err("--output requires a value".to_owned());
                }
                config.output = Some(PathBuf::from(&args[index]));
            }
            "--dry-run" => set_mode(&mut config.mode, OutputMode::DryRunSummary, "--dry-run")?,
            "--execute" => set_mode(&mut config.mode, OutputMode::ExecuteSummary, "--execute")?,
            "--timeout-secs" => {
                index += 1;
                if index >= args.len() {
                    return Err("--timeout-secs requires a value".to_owned());
                }
                config.timeout_secs = parse_u64(&args[index])?.max(1);
            }
            "--max-scenarios" => {
                index += 1;
                if index >= args.len() {
                    return Err("--max-scenarios requires a value".to_owned());
                }
                let max = parse_usize(&args[index])?;
                if max == 0 {
                    return Err("--max-scenarios must be greater than zero".to_owned());
                }
                config.max_scenarios = Some(max);
            }
            "--runner" => {
                index += 1;
                if index >= args.len() {
                    return Err("--runner requires a value".to_owned());
                }
                config.runner_override = Some(PathBuf::from(&args[index]));
            }
            "-h" | "--help" => {
                print_help();
                return Err(String::new());
            }
            unknown => return Err(format!("unknown option: {unknown}")),
        }
        index += 1;
    }

    Ok(config)
}

fn run(args: &[String]) -> Result<(), String> {
    let config = parse_args(args)?;
    let suite = build_validated_tcl_harness_suite()?;

    match config.mode {
        OutputMode::SuiteJson => {
            if let Some(path) = config.output.as_deref() {
                write_tcl_suite_json(path, &suite)?;
            } else {
                let payload = serde_json::to_string_pretty(&suite)
                    .map_err(|error| format!("suite_json_serialize_failed: {error}"))?;
                println!("{payload}");
            }
            Ok(())
        }
        OutputMode::DryRunSummary | OutputMode::ExecuteSummary => {
            let mode = if config.mode == OutputMode::ExecuteSummary {
                TclExecutionMode::Execute
            } else {
                TclExecutionMode::DryRun
            };
            let summary = execute_tcl_harness_suite(
                &suite,
                TclExecutionOptions {
                    mode,
                    timeout_secs: config.timeout_secs,
                    max_scenarios: config.max_scenarios,
                    runner_override: config.runner_override,
                    run_id_override: None,
                },
            )?;

            if let Some(path) = config.output.as_deref() {
                write_tcl_execution_summary_json(path, &summary)?;
            } else {
                let payload = serde_json::to_string_pretty(&summary)
                    .map_err(|error| format!("summary_json_serialize_failed: {error}"))?;
                println!("{payload}");
            }

            Ok(())
        }
    }
}

fn main() -> ExitCode {
    let args: Vec<String> = env::args().skip(1).collect();
    match run(&args) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) if error.is_empty() => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("ERROR bead_id={BEAD_ID} tcl_conformance_manifest failed: {error}");
            ExitCode::from(2)
        }
    }
}
