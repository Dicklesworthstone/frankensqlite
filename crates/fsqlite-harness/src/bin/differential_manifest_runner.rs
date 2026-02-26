use std::env;
use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;
use sha2::{Digest, Sha256};

use fsqlite_harness::corpus_ingest::{
    generate_seed_corpus, ingest_conformance_fixtures, CorpusBuilder,
};
use fsqlite_harness::differential_runner::{
    run_metamorphic_differential, DifferentialRunReport, RunConfig,
};
use fsqlite_harness::differential_v2::{CsqliteExecutor, FsqliteExecutor};

const BEAD_ID: &str = "bd-mblr.7.1.2";
const DEFAULT_SCENARIO_ID: &str = "DIFF-712";
const DEFAULT_OUTPUT_PREFIX: &str = "artifacts/differential-manifest";
const DEFAULT_FIXTURES_DIR: &str = "conformance";

#[derive(Debug, Clone)]
struct Config {
    workspace_root: PathBuf,
    output_json: PathBuf,
    output_human: PathBuf,
    fixtures_dir: PathBuf,
    run_id: String,
    trace_id: String,
    scenario_id: String,
    root_seed: u64,
    max_cases_per_entry: usize,
    max_entries: Option<usize>,
    generated_unix_ms: u128,
    skip_fixtures: bool,
}

impl Config {
    #[allow(clippy::too_many_lines)]
    fn parse() -> Result<Self, String> {
        let mut workspace_root = default_workspace_root()?;
        let mut output_dir = workspace_root.join(DEFAULT_OUTPUT_PREFIX);
        let mut output_json: Option<PathBuf> = None;
        let mut output_human: Option<PathBuf> = None;
        let mut fixtures_dir: Option<PathBuf> = None;
        let mut run_id: Option<String> = None;
        let mut trace_id: Option<String> = None;
        let mut scenario_id = DEFAULT_SCENARIO_ID.to_owned();
        let mut root_seed = 424_242_u64;
        let mut max_cases_per_entry = RunConfig::default().max_cases_per_entry;
        let mut max_entries: Option<usize> = None;
        let mut generated_unix_ms = now_unix_ms();
        let mut skip_fixtures = false;

        let args: Vec<String> = env::args().skip(1).collect();
        let mut index = 0_usize;
        while index < args.len() {
            match args[index].as_str() {
                "--workspace-root" => {
                    index += 1;
                    let value = args
                        .get(index)
                        .ok_or_else(|| "missing value for --workspace-root".to_owned())?;
                    workspace_root = PathBuf::from(value);
                }
                "--output-dir" => {
                    index += 1;
                    let value = args
                        .get(index)
                        .ok_or_else(|| "missing value for --output-dir".to_owned())?;
                    output_dir = PathBuf::from(value);
                }
                "--output-json" => {
                    index += 1;
                    let value = args
                        .get(index)
                        .ok_or_else(|| "missing value for --output-json".to_owned())?;
                    output_json = Some(PathBuf::from(value));
                }
                "--output-human" => {
                    index += 1;
                    let value = args
                        .get(index)
                        .ok_or_else(|| "missing value for --output-human".to_owned())?;
                    output_human = Some(PathBuf::from(value));
                }
                "--fixtures-dir" => {
                    index += 1;
                    let value = args
                        .get(index)
                        .ok_or_else(|| "missing value for --fixtures-dir".to_owned())?;
                    fixtures_dir = Some(PathBuf::from(value));
                }
                "--run-id" => {
                    index += 1;
                    let value = args
                        .get(index)
                        .ok_or_else(|| "missing value for --run-id".to_owned())?;
                    run_id = Some(value.to_owned());
                }
                "--trace-id" => {
                    index += 1;
                    let value = args
                        .get(index)
                        .ok_or_else(|| "missing value for --trace-id".to_owned())?;
                    trace_id = Some(value.to_owned());
                }
                "--scenario-id" => {
                    index += 1;
                    let value = args
                        .get(index)
                        .ok_or_else(|| "missing value for --scenario-id".to_owned())?;
                    value.clone_into(&mut scenario_id);
                }
                "--root-seed" => {
                    index += 1;
                    let value = args
                        .get(index)
                        .ok_or_else(|| "missing value for --root-seed".to_owned())?;
                    root_seed = value
                        .parse::<u64>()
                        .map_err(|error| format!("invalid --root-seed value={value}: {error}"))?;
                }
                "--max-cases-per-entry" => {
                    index += 1;
                    let value = args
                        .get(index)
                        .ok_or_else(|| "missing value for --max-cases-per-entry".to_owned())?;
                    max_cases_per_entry = value.parse::<usize>().map_err(|error| {
                        format!("invalid --max-cases-per-entry value={value}: {error}")
                    })?;
                }
                "--max-entries" => {
                    index += 1;
                    let value = args
                        .get(index)
                        .ok_or_else(|| "missing value for --max-entries".to_owned())?;
                    let parsed = value
                        .parse::<usize>()
                        .map_err(|error| format!("invalid --max-entries value={value}: {error}"))?;
                    max_entries = Some(parsed);
                }
                "--generated-unix-ms" => {
                    index += 1;
                    let value = args
                        .get(index)
                        .ok_or_else(|| "missing value for --generated-unix-ms".to_owned())?;
                    generated_unix_ms = value.parse::<u128>().map_err(|error| {
                        format!("invalid --generated-unix-ms value={value}: {error}")
                    })?;
                }
                "--skip-fixtures" => {
                    skip_fixtures = true;
                }
                "--help" | "-h" => {
                    print_help();
                    std::process::exit(0);
                }
                other => return Err(format!("unknown_argument: {other}")),
            }
            index += 1;
        }

        if max_cases_per_entry == 0 {
            return Err("--max-cases-per-entry must be > 0".to_owned());
        }
        if scenario_id.trim().is_empty() {
            return Err("--scenario-id must be non-empty".to_owned());
        }

        let run_id = run_id
            .unwrap_or_else(|| format!("{BEAD_ID}-{}-{}", generated_unix_ms, std::process::id()));
        let trace_id = trace_id.unwrap_or_else(|| build_trace_id(&run_id));
        let output_json =
            output_json.unwrap_or_else(|| output_dir.join("differential_manifest.json"));
        let output_human =
            output_human.unwrap_or_else(|| output_dir.join("differential_manifest.md"));
        let fixtures_dir =
            fixtures_dir.unwrap_or_else(|| workspace_root.join(DEFAULT_FIXTURES_DIR));

        Ok(Self {
            workspace_root,
            output_json,
            output_human,
            fixtures_dir,
            run_id,
            trace_id,
            scenario_id,
            root_seed,
            max_cases_per_entry,
            max_entries,
            generated_unix_ms,
            skip_fixtures,
        })
    }
}

#[derive(Debug, Clone, Serialize)]
struct ReplayCommand {
    command: String,
}

#[derive(Debug, Clone, Serialize)]
struct DifferentialManifest {
    schema_version: u32,
    bead_id: String,
    run_id: String,
    trace_id: String,
    scenario_id: String,
    generated_unix_ms: u128,
    commit_sha: String,
    root_seed: u64,
    fixture_entries_ingested: usize,
    corpus_entries: usize,
    overall_pass: bool,
    run_report: DifferentialRunReport,
    replay: ReplayCommand,
}

fn print_help() {
    println!(
        "\
differential_manifest_runner — deterministic differential manifest generator ({BEAD_ID})

USAGE:
  cargo run -p fsqlite-harness --bin differential_manifest_runner -- [OPTIONS]

OPTIONS:
  --workspace-root <PATH>        Workspace root (default: auto-detected)
  --output-dir <PATH>            Output directory (default: artifacts/differential-manifest)
  --output-json <PATH>           Output JSON path (default: <output-dir>/differential_manifest.json)
  --output-human <PATH>          Output Markdown summary path (default: <output-dir>/differential_manifest.md)
  --fixtures-dir <PATH>          Conformance fixtures directory (default: <workspace-root>/conformance)
  --skip-fixtures                Skip conformance fixture ingestion (seed corpus only)
  --run-id <ID>                  Deterministic run identifier
  --trace-id <ID>                Deterministic trace identifier
  --scenario-id <ID>             Scenario identifier (default: DIFF-712)
  --root-seed <U64>              Root seed for corpus + runner config (default: 424242)
  --max-cases-per-entry <N>      Metamorphic cases per corpus entry (default: 8)
  --max-entries <N>              Optional cap on corpus entries for faster deterministic runs
  --generated-unix-ms <U128>     Deterministic timestamp for manifest fields
  -h, --help                     Show help
"
    );
}

fn default_workspace_root() -> Result<PathBuf, String> {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .map_err(|error| format!("workspace_root_canonicalize_failed: {error}"))
}

fn now_unix_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis())
}

fn build_trace_id(run_id: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(run_id.as_bytes());
    let digest = hasher.finalize();
    let hex = format!("{digest:x}");
    let short = &hex[..16];
    format!("trace-{short}")
}

fn resolve_commit_sha(workspace_root: &Path) -> String {
    let output = Command::new("git")
        .args([
            "-C",
            &workspace_root.display().to_string(),
            "rev-parse",
            "HEAD",
        ])
        .output();
    match output {
        Ok(result) if result.status.success() => {
            let value = String::from_utf8_lossy(&result.stdout).trim().to_owned();
            if value.is_empty() {
                "unknown".to_owned()
            } else {
                value
            }
        }
        _ => "unknown".to_owned(),
    }
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

fn build_replay_command(config: &Config) -> String {
    let mut replay = format!(
        "cargo run -p fsqlite-harness --bin differential_manifest_runner -- --workspace-root {} --run-id {} --trace-id {} --scenario-id {} --root-seed {} --max-cases-per-entry {} --generated-unix-ms {}",
        config.workspace_root.display(),
        config.run_id,
        config.trace_id,
        config.scenario_id,
        config.root_seed,
        config.max_cases_per_entry,
        config.generated_unix_ms,
    );
    if let Some(max_entries) = config.max_entries {
        let _ = write!(replay, " --max-entries {max_entries}");
    }
    if config.skip_fixtures {
        replay.push_str(" --skip-fixtures");
    } else {
        let _ = write!(replay, " --fixtures-dir {}", config.fixtures_dir.display());
    }
    let _ = write!(
        replay,
        " --output-json {} --output-human {}",
        config.output_json.display(),
        config.output_human.display(),
    );
    replay
}

fn build_human_summary(manifest: &DifferentialManifest) -> String {
    format!(
        "# Differential Manifest ({BEAD_ID})\n\n\
run_id: `{}`\n\
trace_id: `{}`\n\
scenario_id: `{}`\n\
commit_sha: `{}`\n\
root_seed: `{}`\n\
corpus_entries: `{}`\n\
fixture_entries_ingested: `{}`\n\
total_cases: `{}`\n\
passed: `{}`\n\
diverged: `{}`\n\
overall_pass: `{}`\n\
data_hash: `{}`\n\n\
## Replay\n\n\
`{}`\n",
        manifest.run_id,
        manifest.trace_id,
        manifest.scenario_id,
        manifest.commit_sha,
        manifest.root_seed,
        manifest.corpus_entries,
        manifest.fixture_entries_ingested,
        manifest.run_report.total_cases,
        manifest.run_report.passed,
        manifest.run_report.diverged,
        manifest.overall_pass,
        manifest.run_report.data_hash,
        manifest.replay.command,
    )
}

fn run() -> Result<bool, String> {
    let config = Config::parse()?;

    let mut builder = CorpusBuilder::new(config.root_seed);
    generate_seed_corpus(&mut builder);

    let fixture_entries_ingested = if config.skip_fixtures || !config.fixtures_dir.exists() {
        0
    } else {
        ingest_conformance_fixtures(&config.fixtures_dir, &mut builder)?
    };

    let mut entries = builder.build().entries;
    if let Some(max_entries) = config.max_entries {
        entries.truncate(max_entries);
    }

    let run_config = RunConfig {
        base_seed: config.root_seed,
        max_cases_per_entry: config.max_cases_per_entry,
        ..RunConfig::default()
    };

    let run_report = run_metamorphic_differential(
        &entries,
        &run_config,
        FsqliteExecutor::open_in_memory,
        CsqliteExecutor::open_in_memory,
    )?;

    let manifest = DifferentialManifest {
        schema_version: 1,
        bead_id: BEAD_ID.to_owned(),
        run_id: config.run_id.clone(),
        trace_id: config.trace_id.clone(),
        scenario_id: config.scenario_id.clone(),
        generated_unix_ms: config.generated_unix_ms,
        commit_sha: resolve_commit_sha(&config.workspace_root),
        root_seed: config.root_seed,
        fixture_entries_ingested,
        corpus_entries: entries.len(),
        overall_pass: run_report.diverged == 0,
        run_report,
        replay: ReplayCommand {
            command: build_replay_command(&config),
        },
    };

    let json = serde_json::to_string_pretty(&manifest)
        .map_err(|error| format!("manifest_serialize_failed: {error}"))?;
    let human = build_human_summary(&manifest);

    write_text(&config.output_json, &json)?;
    write_text(&config.output_human, &human)?;

    println!(
        "INFO differential_manifest_written path={} diverged={} total_cases={} data_hash={}",
        config.output_json.display(),
        manifest.run_report.diverged,
        manifest.run_report.total_cases,
        manifest.run_report.data_hash,
    );
    println!(
        "INFO differential_manifest_summary_written path={}",
        config.output_human.display()
    );
    println!(
        "INFO differential_manifest_replay command=\"{}\"",
        manifest.replay.command
    );

    Ok(manifest.overall_pass)
}

fn main() -> ExitCode {
    match run() {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => {
            eprintln!("ERROR differential_manifest_runner overall_pass=false");
            ExitCode::from(1)
        }
        Err(error) => {
            eprintln!("ERROR differential_manifest_runner failed: {error}");
            ExitCode::from(2)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fsqlite_harness::differential_runner::CoverageSummary;
    use fsqlite_harness::mismatch_minimizer::DeduplicatedFailures;

    fn test_config() -> Config {
        Config {
            workspace_root: PathBuf::from("/tmp/workspace"),
            output_json: PathBuf::from(
                "artifacts/differential-manifest/differential_manifest.json",
            ),
            output_human: PathBuf::from("artifacts/differential-manifest/differential_manifest.md"),
            fixtures_dir: PathBuf::from("conformance"),
            run_id: "bd-mblr.7.1.2-test-run".to_owned(),
            trace_id: "trace-test".to_owned(),
            scenario_id: "DIFF-712".to_owned(),
            root_seed: 424_242,
            max_cases_per_entry: 8,
            max_entries: Some(64),
            generated_unix_ms: 1_700_000_000_000,
            skip_fixtures: true,
        }
    }

    #[test]
    fn build_trace_id_is_deterministic() {
        let left = build_trace_id("example-run");
        let right = build_trace_id("example-run");
        assert_eq!(left, right);
        assert!(left.starts_with("trace-"));
        assert_ne!(
            build_trace_id("example-run-a"),
            build_trace_id("example-run-b")
        );
    }

    #[test]
    fn replay_command_includes_deterministic_controls() {
        let config = test_config();
        let replay = build_replay_command(&config);

        assert!(replay.contains("--root-seed 424242"));
        assert!(replay.contains("--max-cases-per-entry 8"));
        assert!(replay.contains("--max-entries 64"));
        assert!(replay.contains("--generated-unix-ms 1700000000000"));
        assert!(replay.contains("--skip-fixtures"));
        assert!(replay
            .contains("--output-json artifacts/differential-manifest/differential_manifest.json"));
        assert!(replay
            .contains("--output-human artifacts/differential-manifest/differential_manifest.md"));
    }

    #[test]
    fn human_summary_contains_replay_and_counts() {
        let config = test_config();
        let replay = build_replay_command(&config);
        let run_report = DifferentialRunReport {
            bead_id: BEAD_ID.to_owned(),
            data_hash: "abc123".to_owned(),
            base_seed: config.root_seed,
            total_cases: 12,
            passed: 11,
            diverged: 1,
            skipped: 0,
            divergent_cases: Vec::new(),
            deduplicated: DeduplicatedFailures::default(),
            coverage_summary: CoverageSummary::default(),
        };
        let manifest = DifferentialManifest {
            schema_version: 1,
            bead_id: BEAD_ID.to_owned(),
            run_id: config.run_id.clone(),
            trace_id: config.trace_id.clone(),
            scenario_id: config.scenario_id.clone(),
            generated_unix_ms: config.generated_unix_ms,
            commit_sha: "deadbeef".to_owned(),
            root_seed: config.root_seed,
            fixture_entries_ingested: 4,
            corpus_entries: 16,
            overall_pass: false,
            run_report,
            replay: ReplayCommand {
                command: replay.clone(),
            },
        };

        let human = build_human_summary(&manifest);
        assert!(human.contains("run_id: `bd-mblr.7.1.2-test-run`"));
        assert!(human.contains("diverged: `1`"));
        assert!(human.contains("overall_pass: `false`"));
        assert!(human.contains(&replay));
    }
}
