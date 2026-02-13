use std::env;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use fsqlite_harness::forensics_navigator::{
    QueryFilters, Severity, load_index_from_jsonl, query_index, render_text_report,
};

const BEAD_ID: &str = "bd-mblr.7.5.2";

#[derive(Debug)]
struct CliConfig {
    index_jsonl: PathBuf,
    filters: QueryFilters,
    json_output: bool,
}

fn default_workspace_root() -> Result<PathBuf, String> {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .map_err(|error| format!("workspace_root_canonicalize_failed: {error}"))
}

fn resolve_path(workspace_root: &Path, raw: &str) -> PathBuf {
    let path = PathBuf::from(raw);
    if path.is_absolute() {
        path
    } else {
        workspace_root.join(path)
    }
}

fn print_help() {
    let help = "\
forensics_navigator — timeline/correlation query CLI for evidence index (bd-mblr.7.5.2)

USAGE:
    cargo run -p fsqlite-harness --bin forensics_navigator -- [OPTIONS]

OPTIONS:
    --index-jsonl <PATH>       Evidence index JSONL path (default: artifacts/evidence_index.jsonl)
    --issue-id <ID>            Filter by bead/issue ID associated with a run
    --commit <SHA>             Filter by git SHA
    --seed <u64>               Filter by deterministic run seed
    --component <NAME>         Filter by component/code-area
    --severity <LEVEL>         Filter by severity: critical|high|medium|low
    --limit <N>                Limit matched runs after deterministic sort
    --json                     Emit machine-readable JSON output
    -h, --help                 Show this help
";
    println!("{help}");
}

fn parse_args(args: &[String]) -> Result<CliConfig, String> {
    let workspace_root = default_workspace_root()?;
    let mut config = CliConfig {
        index_jsonl: workspace_root.join("artifacts/evidence_index.jsonl"),
        filters: QueryFilters::default(),
        json_output: false,
    };

    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--index-jsonl" => {
                index += 1;
                if index >= args.len() {
                    return Err("--index-jsonl requires a value".to_owned());
                }
                config.index_jsonl = resolve_path(&workspace_root, &args[index]);
            }
            "--issue-id" => {
                index += 1;
                if index >= args.len() {
                    return Err("--issue-id requires a value".to_owned());
                }
                config.filters.issue_id = Some(args[index].clone());
            }
            "--commit" => {
                index += 1;
                if index >= args.len() {
                    return Err("--commit requires a value".to_owned());
                }
                config.filters.commit = Some(args[index].clone());
            }
            "--seed" => {
                index += 1;
                if index >= args.len() {
                    return Err("--seed requires a value".to_owned());
                }
                let seed = args[index]
                    .parse::<u64>()
                    .map_err(|_| format!("invalid --seed value: {}", args[index]))?;
                config.filters.seed = Some(seed);
            }
            "--component" => {
                index += 1;
                if index >= args.len() {
                    return Err("--component requires a value".to_owned());
                }
                config.filters.component = Some(args[index].clone());
            }
            "--severity" => {
                index += 1;
                if index >= args.len() {
                    return Err("--severity requires a value".to_owned());
                }
                let normalized = args[index].to_ascii_lowercase();
                let severity = Severity::parse(&normalized).ok_or_else(|| {
                    format!(
                        "invalid --severity value: {} (expected critical|high|medium|low)",
                        args[index]
                    )
                })?;
                config.filters.severity = Some(severity);
            }
            "--limit" => {
                index += 1;
                if index >= args.len() {
                    return Err("--limit requires a value".to_owned());
                }
                let limit = args[index]
                    .parse::<usize>()
                    .map_err(|_| format!("invalid --limit value: {}", args[index]))?;
                config.filters.limit = Some(limit);
            }
            "--json" => config.json_output = true,
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
    let evidence_index = load_index_from_jsonl(&config.index_jsonl)?;
    let result = query_index(&evidence_index, &config.filters);

    if config.json_output {
        let json = serde_json::to_string_pretty(&result)
            .map_err(|error| format!("forensics_result_json_serialize_failed: {error}"))?;
        println!("{json}");
    } else {
        println!("{}", render_text_report(&result));
    }

    Ok(())
}

fn main() -> ExitCode {
    let args: Vec<String> = env::args().skip(1).collect();
    match run(&args) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) if error.is_empty() => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("ERROR bead_id={BEAD_ID} forensics_navigator failed: {error}");
            ExitCode::from(2)
        }
    }
}
