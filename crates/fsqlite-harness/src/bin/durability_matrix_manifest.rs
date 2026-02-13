use std::env;
use std::path::PathBuf;
use std::process::ExitCode;

use fsqlite_harness::durability_matrix::{
    BEAD_ID, DEFAULT_EXECUTION_TIMEOUT_SECS, DEFAULT_ROOT_SEED, DurabilityExecutionMode,
    DurabilityExecutionOptions, build_validated_durability_matrix, execute_durability_matrix,
    render_operator_workflow, write_execution_summary_json, write_matrix_json,
};

#[derive(Debug)]
struct CliConfig {
    root_seed: u64,
    output: Option<PathBuf>,
    mode: OutputMode,
    timeout_secs: u64,
    max_probes: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OutputMode {
    MatrixJson,
    Workflow,
    ProbeDryRun,
    ProbeExecute,
}

fn print_help() {
    let help = "\
durability_matrix_manifest — deterministic durability matrix generator (bd-mblr.7.4)

USAGE:
    cargo run -p fsqlite-harness --bin durability_matrix_manifest -- [OPTIONS]

OPTIONS:
    --root-seed <u64>     Root seed for deterministic matrix generation
                          (default: 0xB740_0000_0000_0001)
    --output <PATH>       Write output to file (stdout when omitted)
    --workflow            Emit operator workflow text instead of JSON
    --probe-dry-run       Emit dry-run probe execution summary JSON
    --probe-execute       Execute host-compatible probes and emit execution summary JSON
    --timeout-secs <u64>  Timeout budget in seconds for probe execution
                          (default: 1800; only used by probe modes)
    --max-probes <usize>  Limit probes processed from deterministic order
                          (only used by probe modes)
    -h, --help            Show this help
";
    println!("{help}");
}

fn parse_u64(value: &str) -> Result<u64, String> {
    if let Some(hex) = value
        .strip_prefix("0x")
        .or_else(|| value.strip_prefix("0X"))
    {
        u64::from_str_radix(hex, 16).map_err(|_| format!("invalid hex u64 value: {value}"))
    } else {
        value
            .parse::<u64>()
            .map_err(|_| format!("invalid u64 value: {value}"))
    }
}

fn parse_usize(value: &str) -> Result<usize, String> {
    value
        .parse::<usize>()
        .map_err(|_| format!("invalid usize value: {value}"))
}

fn set_mode(mode: &mut OutputMode, next: OutputMode, flag: &str) -> Result<(), String> {
    if *mode != OutputMode::MatrixJson {
        return Err(format!(
            "{flag} cannot be combined with another output mode flag"
        ));
    }
    *mode = next;
    Ok(())
}

fn parse_args(args: &[String]) -> Result<CliConfig, String> {
    let mut config = CliConfig {
        root_seed: DEFAULT_ROOT_SEED,
        output: None,
        mode: OutputMode::MatrixJson,
        timeout_secs: DEFAULT_EXECUTION_TIMEOUT_SECS,
        max_probes: None,
    };

    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--root-seed" => {
                index += 1;
                if index >= args.len() {
                    return Err("--root-seed requires a value".to_owned());
                }
                config.root_seed = parse_u64(&args[index])?;
            }
            "--output" => {
                index += 1;
                if index >= args.len() {
                    return Err("--output requires a value".to_owned());
                }
                config.output = Some(PathBuf::from(&args[index]));
            }
            "--workflow" => set_mode(&mut config.mode, OutputMode::Workflow, "--workflow")?,
            "--probe-dry-run" => {
                set_mode(&mut config.mode, OutputMode::ProbeDryRun, "--probe-dry-run")?
            }
            "--probe-execute" => set_mode(
                &mut config.mode,
                OutputMode::ProbeExecute,
                "--probe-execute",
            )?,
            "--timeout-secs" => {
                index += 1;
                if index >= args.len() {
                    return Err("--timeout-secs requires a value".to_owned());
                }
                config.timeout_secs = parse_u64(&args[index])?.max(1);
            }
            "--max-probes" => {
                index += 1;
                if index >= args.len() {
                    return Err("--max-probes requires a value".to_owned());
                }
                let max_probes = parse_usize(&args[index])?;
                if max_probes == 0 {
                    return Err("--max-probes must be greater than zero".to_owned());
                }
                config.max_probes = Some(max_probes);
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
    let matrix = build_validated_durability_matrix(config.root_seed)?;

    match config.mode {
        OutputMode::Workflow => {
            let workflow = render_operator_workflow(&matrix);
            if let Some(output) = &config.output {
                std::fs::write(output, workflow.as_bytes()).map_err(|error| {
                    format!(
                        "durability_matrix_workflow_write_failed path={} error={error}",
                        output.display()
                    )
                })?;
            } else {
                println!("{workflow}");
            }
            Ok(())
        }
        OutputMode::ProbeDryRun | OutputMode::ProbeExecute => {
            let mode = if matches!(config.mode, OutputMode::ProbeExecute) {
                DurabilityExecutionMode::Execute
            } else {
                DurabilityExecutionMode::DryRun
            };
            let summary = execute_durability_matrix(
                &matrix,
                DurabilityExecutionOptions {
                    mode,
                    timeout_secs: config.timeout_secs,
                    max_probes: config.max_probes,
                },
            )?;
            if let Some(output) = &config.output {
                write_execution_summary_json(output, &summary)?;
            } else {
                let payload = serde_json::to_string_pretty(&summary).map_err(|error| {
                    format!("durability_execution_json_serialize_failed: {error}")
                })?;
                println!("{payload}");
            }
            Ok(())
        }
        OutputMode::MatrixJson => {
            if let Some(output) = &config.output {
                write_matrix_json(output, &matrix)?;
            } else {
                let payload = serde_json::to_string_pretty(&matrix)
                    .map_err(|error| format!("durability_matrix_json_serialize_failed: {error}"))?;
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
            eprintln!("ERROR bead_id={BEAD_ID} durability_matrix_manifest failed: {error}");
            ExitCode::from(2)
        }
    }
}
