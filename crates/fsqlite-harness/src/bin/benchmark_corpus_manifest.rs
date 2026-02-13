use std::env;
use std::path::PathBuf;
use std::process::ExitCode;

use fsqlite_harness::benchmark_corpus::{
    BEAD_ID, DEFAULT_ROOT_SEED, build_validated_benchmark_corpus, render_operator_workflow,
    write_corpus_json,
};

#[derive(Debug)]
struct CliConfig {
    root_seed: u64,
    output: Option<PathBuf>,
    workflow: bool,
}

fn print_help() {
    let help = "\
benchmark_corpus_manifest — deterministic benchmark corpus generator (bd-mblr.7.3.1)

USAGE:
    cargo run -p fsqlite-harness --bin benchmark_corpus_manifest -- [OPTIONS]

OPTIONS:
    --root-seed <u64>     Root seed for deterministic corpus generation
                          (default: 0xB731_71A0_0000_0001)
    --output <PATH>       Write output to file (stdout when omitted)
    --workflow            Emit operator workflow text instead of JSON
    -h, --help            Show this help
";
    println!("{help}");
}

fn parse_args(args: &[String]) -> Result<CliConfig, String> {
    let mut config = CliConfig {
        root_seed: DEFAULT_ROOT_SEED,
        output: None,
        workflow: false,
    };

    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--root-seed" => {
                index += 1;
                if index >= args.len() {
                    return Err("--root-seed requires a value".to_owned());
                }
                config.root_seed = args[index]
                    .parse::<u64>()
                    .map_err(|_| format!("invalid --root-seed value: {}", args[index]))?;
            }
            "--output" => {
                index += 1;
                if index >= args.len() {
                    return Err("--output requires a value".to_owned());
                }
                config.output = Some(PathBuf::from(&args[index]));
            }
            "--workflow" => config.workflow = true,
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
    let corpus = build_validated_benchmark_corpus(config.root_seed)?;

    if config.workflow {
        let workflow = render_operator_workflow(&corpus);
        if let Some(output) = &config.output {
            std::fs::write(output, workflow.as_bytes()).map_err(|error| {
                format!(
                    "benchmark_corpus_workflow_write_failed path={} error={error}",
                    output.display()
                )
            })?;
        } else {
            println!("{workflow}");
        }
        return Ok(());
    }

    if let Some(output) = &config.output {
        write_corpus_json(output, &corpus)?;
    } else {
        let payload = serde_json::to_string_pretty(&corpus)
            .map_err(|error| format!("benchmark_corpus_json_serialize_failed: {error}"))?;
        println!("{payload}");
    }

    Ok(())
}

fn main() -> ExitCode {
    let args: Vec<String> = env::args().skip(1).collect();
    match run(&args) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) if error.is_empty() => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("ERROR bead_id={BEAD_ID} benchmark_corpus_manifest failed: {error}");
            ExitCode::from(2)
        }
    }
}
