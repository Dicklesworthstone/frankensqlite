use std::env;
use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;
use sha2::{Digest, Sha256};

use fsqlite_harness::corpus_ingest::{
    CorpusBuilder, FixtureIngestReport, SltIngestReport, generate_seed_corpus,
    ingest_conformance_fixtures_with_report, ingest_slt_files_with_report,
};
use fsqlite_harness::differential_runner::{
    DifferentialRunReport, DivergenceSource, RunConfig, run_metamorphic_differential,
};
use fsqlite_harness::differential_v2::{CsqliteExecutor, FsqliteExecutor};

const BEAD_ID: &str = "bd-mblr.7.1.2";
const DEFAULT_SCENARIO_ID: &str = "DIFF-712";
const DEFAULT_OUTPUT_PREFIX: &str = "artifacts/differential-manifest";
const DEFAULT_FIXTURES_DIR: &str = "crates/fsqlite-harness/conformance";
const DEFAULT_SLT_DIR: &str = "conformance/slt";
const DEFAULT_MIN_FIXTURE_JSON_FILES: usize = 8;
const DEFAULT_MIN_FIXTURE_ENTRIES: usize = 8;
const DEFAULT_MIN_FIXTURE_SQL_STATEMENTS: usize = 40;
const DEFAULT_MIN_SLT_FILES: usize = 1;
const DEFAULT_MIN_SLT_ENTRIES: usize = 1;
const DEFAULT_MIN_SLT_SQL_STATEMENTS: usize = 1;

#[derive(Debug, Clone)]
struct Config {
    workspace_root: PathBuf,
    output_json: PathBuf,
    output_human: PathBuf,
    fixtures_dir: PathBuf,
    slt_dir: PathBuf,
    run_id: String,
    trace_id: String,
    scenario_id: String,
    root_seed: u64,
    max_cases_per_entry: usize,
    max_entries: Option<usize>,
    min_fixture_json_files: usize,
    min_fixture_entries: usize,
    min_fixture_sql_statements: usize,
    min_slt_files: usize,
    min_slt_entries: usize,
    min_slt_sql_statements: usize,
    generated_unix_ms: u128,
    skip_fixtures: bool,
    skip_slt: bool,
}

impl Config {
    #[allow(clippy::too_many_lines)]
    fn parse() -> Result<Self, String> {
        let mut workspace_root = default_workspace_root()?;
        let mut output_dir = workspace_root.join(DEFAULT_OUTPUT_PREFIX);
        let mut output_json: Option<PathBuf> = None;
        let mut output_human: Option<PathBuf> = None;
        let mut fixtures_dir: Option<PathBuf> = None;
        let mut slt_dir: Option<PathBuf> = None;
        let mut run_id: Option<String> = None;
        let mut trace_id: Option<String> = None;
        let mut scenario_id = DEFAULT_SCENARIO_ID.to_owned();
        let mut root_seed = 424_242_u64;
        let mut max_cases_per_entry = RunConfig::default().max_cases_per_entry;
        let mut max_entries: Option<usize> = None;
        let mut min_fixture_json_files = DEFAULT_MIN_FIXTURE_JSON_FILES;
        let mut min_fixture_entries = DEFAULT_MIN_FIXTURE_ENTRIES;
        let mut min_fixture_sql_statements = DEFAULT_MIN_FIXTURE_SQL_STATEMENTS;
        let mut min_slt_files = DEFAULT_MIN_SLT_FILES;
        let mut min_slt_entries = DEFAULT_MIN_SLT_ENTRIES;
        let mut min_slt_sql_statements = DEFAULT_MIN_SLT_SQL_STATEMENTS;
        let mut generated_unix_ms = now_unix_ms();
        let mut skip_fixtures = false;
        let mut skip_slt = true;

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
                "--slt-dir" => {
                    index += 1;
                    let value = args
                        .get(index)
                        .ok_or_else(|| "missing value for --slt-dir".to_owned())?;
                    slt_dir = Some(PathBuf::from(value));
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
                "--min-fixture-json-files" => {
                    index += 1;
                    let value = args
                        .get(index)
                        .ok_or_else(|| "missing value for --min-fixture-json-files".to_owned())?;
                    min_fixture_json_files = value.parse::<usize>().map_err(|error| {
                        format!("invalid --min-fixture-json-files value={value}: {error}")
                    })?;
                }
                "--min-fixture-entries" => {
                    index += 1;
                    let value = args
                        .get(index)
                        .ok_or_else(|| "missing value for --min-fixture-entries".to_owned())?;
                    min_fixture_entries = value.parse::<usize>().map_err(|error| {
                        format!("invalid --min-fixture-entries value={value}: {error}")
                    })?;
                }
                "--min-fixture-sql-statements" => {
                    index += 1;
                    let value = args.get(index).ok_or_else(|| {
                        "missing value for --min-fixture-sql-statements".to_owned()
                    })?;
                    min_fixture_sql_statements = value.parse::<usize>().map_err(|error| {
                        format!("invalid --min-fixture-sql-statements value={value}: {error}")
                    })?;
                }
                "--min-slt-files" => {
                    index += 1;
                    let value = args
                        .get(index)
                        .ok_or_else(|| "missing value for --min-slt-files".to_owned())?;
                    min_slt_files = value.parse::<usize>().map_err(|error| {
                        format!("invalid --min-slt-files value={value}: {error}")
                    })?;
                }
                "--min-slt-entries" => {
                    index += 1;
                    let value = args
                        .get(index)
                        .ok_or_else(|| "missing value for --min-slt-entries".to_owned())?;
                    min_slt_entries = value.parse::<usize>().map_err(|error| {
                        format!("invalid --min-slt-entries value={value}: {error}")
                    })?;
                }
                "--min-slt-sql-statements" => {
                    index += 1;
                    let value = args
                        .get(index)
                        .ok_or_else(|| "missing value for --min-slt-sql-statements".to_owned())?;
                    min_slt_sql_statements = value.parse::<usize>().map_err(|error| {
                        format!("invalid --min-slt-sql-statements value={value}: {error}")
                    })?;
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
                "--skip-slt" => {
                    skip_slt = true;
                }
                "--enable-slt" => {
                    skip_slt = false;
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
        if let Some(max_entries) = max_entries {
            require_positive("--max-entries", max_entries)?;
        }
        require_positive("--min-fixture-json-files", min_fixture_json_files)?;
        require_positive("--min-fixture-entries", min_fixture_entries)?;
        require_positive("--min-fixture-sql-statements", min_fixture_sql_statements)?;
        if !skip_slt {
            require_positive("--min-slt-files", min_slt_files)?;
            require_positive("--min-slt-entries", min_slt_entries)?;
            require_positive("--min-slt-sql-statements", min_slt_sql_statements)?;
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
        let slt_dir = slt_dir.unwrap_or_else(|| workspace_root.join(DEFAULT_SLT_DIR));

        Ok(Self {
            workspace_root,
            output_json,
            output_human,
            fixtures_dir,
            slt_dir,
            run_id,
            trace_id,
            scenario_id,
            root_seed,
            max_cases_per_entry,
            max_entries,
            min_fixture_json_files,
            min_fixture_entries,
            min_fixture_sql_statements,
            min_slt_files,
            min_slt_entries,
            min_slt_sql_statements,
            generated_unix_ms,
            skip_fixtures,
            skip_slt,
        })
    }
}

#[derive(Debug, Clone, Serialize)]
struct ReplayCommand {
    command: String,
}

#[derive(Debug, Clone, Serialize)]
struct FirstFailureReplay {
    case_id: String,
    transform_name: String,
    divergence_source: String,
    statement_index: Option<usize>,
    replay_command: String,
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
    fixture_json_files_seen: usize,
    fixture_entries_ingested: usize,
    fixture_sql_statements_ingested: usize,
    min_fixture_json_files: usize,
    min_fixture_entries: usize,
    min_fixture_sql_statements: usize,
    slt_files_seen: usize,
    slt_entries_ingested: usize,
    slt_sql_statements_ingested: usize,
    min_slt_files: usize,
    min_slt_entries: usize,
    min_slt_sql_statements: usize,
    corpus_entries: usize,
    overall_pass: bool,
    run_report: DifferentialRunReport,
    first_failure: Option<FirstFailureReplay>,
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
  --fixtures-dir <PATH>          Conformance fixtures directory (default: <workspace-root>/crates/fsqlite-harness/conformance)
  --slt-dir <PATH>               SQLLogicTest directory (default: <workspace-root>/conformance/slt)
  --min-fixture-json-files <N>   Minimum fixture JSON files required when fixtures are enabled (default: 8, must be > 0)
  --min-fixture-entries <N>      Minimum fixture entries ingested when fixtures are enabled (default: 8, must be > 0)
  --min-fixture-sql-statements <N>
                                 Minimum SQL statements extracted from fixtures (default: 40, must be > 0)
  --min-slt-files <N>            Minimum SLT files required when SLT ingestion is enabled (default: 1, must be > 0)
  --min-slt-entries <N>          Minimum parsed SLT entries when SLT ingestion is enabled (default: 1, must be > 0)
  --min-slt-sql-statements <N>   Minimum SQL statements extracted from SLT when enabled (default: 1, must be > 0)
  --skip-fixtures                Skip conformance fixture ingestion (seed corpus only)
  --enable-slt                   Enable SQLLogicTest ingestion into the corpus pipeline
  --skip-slt                     Disable SQLLogicTest ingestion (default)
  --run-id <ID>                  Deterministic run identifier
  --trace-id <ID>                Deterministic trace identifier
  --scenario-id <ID>             Scenario identifier (default: DIFF-712)
  --root-seed <U64>              Root seed for corpus + runner config (default: 424242)
  --max-cases-per-entry <N>      Metamorphic cases per corpus entry (default: 8)
  --max-entries <N>              Optional cap on corpus entries for faster deterministic runs (must be > 0)
  --generated-unix-ms <U128>     Deterministic timestamp for manifest fields
  -h, --help                     Show help
"
    );
}

fn require_positive(name: &str, value: usize) -> Result<(), String> {
    if value == 0 {
        Err(format!("{name} must be > 0"))
    } else {
        Ok(())
    }
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

fn shell_single_quote(value: &str) -> String {
    let escaped = value.replace('\'', "'\"'\"'");
    format!("'{escaped}'")
}

fn path_to_utf8(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

fn normalize_one_command(command: &str) -> Option<String> {
    let normalized = command.trim();
    if normalized.is_empty() || normalized.contains('\n') || normalized.contains('\r') {
        return None;
    }
    Some(normalized.to_owned())
}

fn divergence_source_label(source: Option<DivergenceSource>) -> &'static str {
    match source {
        Some(DivergenceSource::Original) => "original",
        Some(DivergenceSource::Transformed) => "transformed",
        Some(DivergenceSource::CrossVariant) => "cross-variant",
        None => "unknown",
    }
}

fn first_failure_replay(
    run_report: &DifferentialRunReport,
    fallback_replay: &str,
) -> Option<FirstFailureReplay> {
    let first_case = run_report.divergent_cases.first()?;
    let fallback = fallback_replay.to_owned();
    let replay_command = first_case
        .minimal_reproduction
        .as_ref()
        .and_then(|repro| normalize_one_command(&repro.repro_command))
        .unwrap_or(fallback);

    Some(FirstFailureReplay {
        case_id: first_case.case_id.clone(),
        transform_name: first_case.transform_name.clone(),
        divergence_source: divergence_source_label(first_case.divergence_source).to_owned(),
        statement_index: first_case
            .minimal_reproduction
            .as_ref()
            .and_then(|repro| repro.first_divergence_index),
        replay_command,
    })
}

fn build_replay_command(config: &Config) -> String {
    let mut replay = format!(
        "cargo run -p fsqlite-harness --bin differential_manifest_runner -- --workspace-root {} --run-id {} --trace-id {} --scenario-id {} --root-seed {} --max-cases-per-entry {} --generated-unix-ms {}",
        shell_single_quote(&path_to_utf8(&config.workspace_root)),
        shell_single_quote(&config.run_id),
        shell_single_quote(&config.trace_id),
        shell_single_quote(&config.scenario_id),
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
        let _ = write!(
            replay,
            " --fixtures-dir {}",
            shell_single_quote(&path_to_utf8(&config.fixtures_dir))
        );
        let _ = write!(
            replay,
            " --min-fixture-json-files {} --min-fixture-entries {} --min-fixture-sql-statements {}",
            config.min_fixture_json_files,
            config.min_fixture_entries,
            config.min_fixture_sql_statements,
        );
    }
    if config.skip_slt {
        replay.push_str(" --skip-slt");
    } else {
        replay.push_str(" --enable-slt");
        let _ = write!(
            replay,
            " --slt-dir {}",
            shell_single_quote(&path_to_utf8(&config.slt_dir))
        );
        let _ = write!(
            replay,
            " --min-slt-files {} --min-slt-entries {} --min-slt-sql-statements {}",
            config.min_slt_files, config.min_slt_entries, config.min_slt_sql_statements,
        );
    }
    let _ = write!(
        replay,
        " --output-json {} --output-human {}",
        shell_single_quote(&path_to_utf8(&config.output_json)),
        shell_single_quote(&path_to_utf8(&config.output_human)),
    );
    replay
}

fn build_human_summary(manifest: &DifferentialManifest) -> String {
    let first_failure_case_id = manifest
        .first_failure
        .as_ref()
        .map_or_else(|| "none".to_owned(), |failure| failure.case_id.clone());
    let first_failure_statement_index = manifest
        .first_failure
        .as_ref()
        .and_then(|failure| failure.statement_index)
        .map_or_else(|| "none".to_owned(), |idx| idx.to_string());
    let first_failure_replay = manifest.first_failure.as_ref().map_or_else(
        || "none".to_owned(),
        |failure| failure.replay_command.clone(),
    );

    format!(
        "# Differential Manifest ({BEAD_ID})\n\n\
run_id: `{}`\n\
trace_id: `{}`\n\
scenario_id: `{}`\n\
commit_sha: `{}`\n\
root_seed: `{}`\n\
corpus_entries: `{}`\n\
fixture_json_files_seen: `{}`\n\
fixture_entries_ingested: `{}`\n\
fixture_sql_statements_ingested: `{}`\n\
min_fixture_json_files: `{}`\n\
min_fixture_entries: `{}`\n\
min_fixture_sql_statements: `{}`\n\
slt_files_seen: `{}`\n\
slt_entries_ingested: `{}`\n\
slt_sql_statements_ingested: `{}`\n\
min_slt_files: `{}`\n\
min_slt_entries: `{}`\n\
min_slt_sql_statements: `{}`\n\
total_cases: `{}`\n\
passed: `{}`\n\
diverged: `{}`\n\
overall_pass: `{}`\n\
data_hash: `{}`\n\n\
first_failure_case_id: `{}`\n\
first_failure_statement_index: `{}`\n\n\
## Replay\n\n\
`{}`\n\n\
## First Failure Replay\n\n\
`{}`\n",
        manifest.run_id,
        manifest.trace_id,
        manifest.scenario_id,
        manifest.commit_sha,
        manifest.root_seed,
        manifest.corpus_entries,
        manifest.fixture_json_files_seen,
        manifest.fixture_entries_ingested,
        manifest.fixture_sql_statements_ingested,
        manifest.min_fixture_json_files,
        manifest.min_fixture_entries,
        manifest.min_fixture_sql_statements,
        manifest.slt_files_seen,
        manifest.slt_entries_ingested,
        manifest.slt_sql_statements_ingested,
        manifest.min_slt_files,
        manifest.min_slt_entries,
        manifest.min_slt_sql_statements,
        manifest.run_report.total_cases,
        manifest.run_report.passed,
        manifest.run_report.diverged,
        manifest.overall_pass,
        manifest.run_report.data_hash,
        first_failure_case_id,
        first_failure_statement_index,
        manifest.replay.command,
        first_failure_replay,
    )
}

fn run() -> Result<bool, String> {
    let config = Config::parse()?;

    let mut builder = CorpusBuilder::new(config.root_seed);
    generate_seed_corpus(&mut builder);

    let fixture_report = if config.skip_fixtures {
        FixtureIngestReport::default()
    } else {
        if !config.fixtures_dir.exists() {
            return Err(format!(
                "fixture_ingest_failed: fixtures directory does not exist: {}. \
                 remediation: pass --fixtures-dir <PATH> (recommended: {}/{}), \
                 or run with --skip-fixtures if seed-only execution is intentional.",
                config.fixtures_dir.display(),
                config.workspace_root.display(),
                DEFAULT_FIXTURES_DIR,
            ));
        }

        let report = ingest_conformance_fixtures_with_report(&config.fixtures_dir, &mut builder)?;
        enforce_fixture_sanity(&report, &config)?;
        report
    };

    let slt_report = if config.skip_slt {
        SltIngestReport::default()
    } else {
        if !config.slt_dir.exists() {
            return Err(format!(
                "slt_ingest_failed: slt directory does not exist: {}. \
                 remediation: pass --slt-dir <PATH> (recommended: {}/{}), \
                 or run with --skip-slt.",
                config.slt_dir.display(),
                config.workspace_root.display(),
                DEFAULT_SLT_DIR,
            ));
        }
        let report = ingest_slt_files_with_report(&config.slt_dir, &mut builder)?;
        enforce_slt_sanity(&report, &config)?;
        report
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

    let replay_command = build_replay_command(&config);
    let replay_command = normalize_one_command(&replay_command)
        .ok_or_else(|| "replay_command_generation_failed: empty_or_multiline_command".to_owned())?;
    let first_failure = first_failure_replay(&run_report, &replay_command);

    let manifest = DifferentialManifest {
        schema_version: 1,
        bead_id: BEAD_ID.to_owned(),
        run_id: config.run_id.clone(),
        trace_id: config.trace_id.clone(),
        scenario_id: config.scenario_id.clone(),
        generated_unix_ms: config.generated_unix_ms,
        commit_sha: resolve_commit_sha(&config.workspace_root),
        root_seed: config.root_seed,
        fixture_json_files_seen: fixture_report.fixture_json_files_seen,
        fixture_entries_ingested: fixture_report.fixture_entries_ingested,
        fixture_sql_statements_ingested: fixture_report.sql_statements_ingested,
        min_fixture_json_files: config.min_fixture_json_files,
        min_fixture_entries: config.min_fixture_entries,
        min_fixture_sql_statements: config.min_fixture_sql_statements,
        slt_files_seen: slt_report.slt_files_seen,
        slt_entries_ingested: slt_report.slt_entries_ingested,
        slt_sql_statements_ingested: slt_report.sql_statements_ingested,
        min_slt_files: config.min_slt_files,
        min_slt_entries: config.min_slt_entries,
        min_slt_sql_statements: config.min_slt_sql_statements,
        corpus_entries: entries.len(),
        overall_pass: run_report.diverged == 0,
        run_report,
        first_failure,
        replay: ReplayCommand {
            command: replay_command,
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

fn enforce_fixture_sanity(report: &FixtureIngestReport, config: &Config) -> Result<(), String> {
    let mut violations = Vec::new();
    if report.fixture_json_files_seen < config.min_fixture_json_files {
        violations.push(format!(
            "fixture_json_files_seen={} < min_fixture_json_files={}",
            report.fixture_json_files_seen, config.min_fixture_json_files
        ));
    }
    if report.fixture_entries_ingested < config.min_fixture_entries {
        violations.push(format!(
            "fixture_entries_ingested={} < min_fixture_entries={}",
            report.fixture_entries_ingested, config.min_fixture_entries
        ));
    }
    if report.sql_statements_ingested < config.min_fixture_sql_statements {
        violations.push(format!(
            "fixture_sql_statements_ingested={} < min_fixture_sql_statements={}",
            report.sql_statements_ingested, config.min_fixture_sql_statements
        ));
    }

    if violations.is_empty() {
        return Ok(());
    }

    let mut message = String::from("fixture_ingest_sanity_failed: ");
    let _ = write!(message, "{}", violations.join("; "));

    if !report.skipped_files.is_empty() {
        let mut skipped = report
            .skipped_files
            .iter()
            .map(|detail| format!("{} ({})", detail.file, detail.reason))
            .collect::<Vec<_>>();
        skipped.sort();
        skipped.truncate(5);
        let _ = write!(message, ". skipped_fixture_examples={}", skipped.join(", "));
    }

    let _ = write!(
        message,
        ". remediation: verify fixture quality/size in {} (recommended default: \
         {}/{}). If seed-only execution was intended, rerun with --skip-fixtures.",
        config.fixtures_dir.display(),
        config.workspace_root.display(),
        DEFAULT_FIXTURES_DIR,
    );

    Err(message)
}

fn enforce_slt_sanity(report: &SltIngestReport, config: &Config) -> Result<(), String> {
    let mut violations = Vec::new();
    if report.slt_files_seen < config.min_slt_files {
        violations.push(format!(
            "slt_files_seen={} < min_slt_files={}",
            report.slt_files_seen, config.min_slt_files
        ));
    }
    if report.slt_entries_ingested < config.min_slt_entries {
        violations.push(format!(
            "slt_entries_ingested={} < min_slt_entries={}",
            report.slt_entries_ingested, config.min_slt_entries
        ));
    }
    if report.sql_statements_ingested < config.min_slt_sql_statements {
        violations.push(format!(
            "slt_sql_statements_ingested={} < min_slt_sql_statements={}",
            report.sql_statements_ingested, config.min_slt_sql_statements
        ));
    }

    if violations.is_empty() {
        return Ok(());
    }

    let mut message = String::from("slt_ingest_sanity_failed: ");
    let _ = write!(message, "{}", violations.join("; "));

    if !report.skipped_files.is_empty() {
        let mut skipped = report
            .skipped_files
            .iter()
            .map(|detail| format!("{} ({})", detail.file, detail.reason))
            .collect::<Vec<_>>();
        skipped.sort();
        skipped.truncate(5);
        let _ = write!(message, ". skipped_slt_examples={}", skipped.join(", "));
    }

    let _ = write!(
        message,
        ". remediation: verify SQLLogicTest suite quality/size in {} (recommended default: \
         {}/{}). If SLT ingestion was not intended, rerun with --skip-slt.",
        config.slt_dir.display(),
        config.workspace_root.display(),
        DEFAULT_SLT_DIR,
    );
    Err(message)
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
            fixtures_dir: PathBuf::from(DEFAULT_FIXTURES_DIR),
            slt_dir: PathBuf::from(DEFAULT_SLT_DIR),
            run_id: "bd-mblr.7.1.2-test-run".to_owned(),
            trace_id: "trace-test".to_owned(),
            scenario_id: "DIFF-712".to_owned(),
            root_seed: 424_242,
            max_cases_per_entry: 8,
            max_entries: Some(64),
            min_fixture_json_files: DEFAULT_MIN_FIXTURE_JSON_FILES,
            min_fixture_entries: DEFAULT_MIN_FIXTURE_ENTRIES,
            min_fixture_sql_statements: DEFAULT_MIN_FIXTURE_SQL_STATEMENTS,
            min_slt_files: DEFAULT_MIN_SLT_FILES,
            min_slt_entries: DEFAULT_MIN_SLT_ENTRIES,
            min_slt_sql_statements: DEFAULT_MIN_SLT_SQL_STATEMENTS,
            generated_unix_ms: 1_700_000_000_000,
            skip_fixtures: true,
            skip_slt: true,
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
        assert!(replay.contains("--skip-slt"));
        assert!(
            replay.contains(
                "--output-json artifacts/differential-manifest/differential_manifest.json"
            )
        );
        assert!(
            replay.contains(
                "--output-human artifacts/differential-manifest/differential_manifest.md"
            )
        );
    }

    #[test]
    fn replay_command_includes_fixture_thresholds_when_enabled() {
        let mut config = test_config();
        config.skip_fixtures = false;
        let replay = build_replay_command(&config);

        assert!(replay.contains("--fixtures-dir crates/fsqlite-harness/conformance"));
        assert!(replay.contains("--min-fixture-json-files 8"));
        assert!(replay.contains("--min-fixture-entries 8"));
        assert!(replay.contains("--min-fixture-sql-statements 40"));
    }

    #[test]
    fn replay_command_includes_slt_controls_when_enabled() {
        let mut config = test_config();
        config.skip_slt = false;
        let replay = build_replay_command(&config);

        assert!(replay.contains("--enable-slt"));
        assert!(replay.contains("--slt-dir conformance/slt"));
        assert!(replay.contains("--min-slt-files 1"));
        assert!(replay.contains("--min-slt-entries 1"));
        assert!(replay.contains("--min-slt-sql-statements 1"));
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
            fixture_json_files_seen: 9,
            fixture_entries_ingested: 4,
            fixture_sql_statements_ingested: 24,
            min_fixture_json_files: config.min_fixture_json_files,
            min_fixture_entries: config.min_fixture_entries,
            min_fixture_sql_statements: config.min_fixture_sql_statements,
            slt_files_seen: 2,
            slt_entries_ingested: 6,
            slt_sql_statements_ingested: 6,
            min_slt_files: config.min_slt_files,
            min_slt_entries: config.min_slt_entries,
            min_slt_sql_statements: config.min_slt_sql_statements,
            corpus_entries: 16,
            overall_pass: false,
            run_report,
            first_failure: None,
            replay: ReplayCommand {
                command: replay.clone(),
            },
        };

        let human = build_human_summary(&manifest);
        assert!(human.contains("run_id: `bd-mblr.7.1.2-test-run`"));
        assert!(human.contains("diverged: `1`"));
        assert!(human.contains("overall_pass: `false`"));
        assert!(human.contains(&replay));
        assert!(human.contains("fixture_json_files_seen: `9`"));
        assert!(human.contains("fixture_sql_statements_ingested: `24`"));
        assert!(human.contains("slt_files_seen: `2`"));
        assert!(human.contains("slt_entries_ingested: `6`"));
    }

    #[test]
    fn require_positive_rejects_zero() {
        let error = require_positive("--max-entries", 0).expect_err("zero should fail");
        assert_eq!(error, "--max-entries must be > 0");
        require_positive("--max-entries", 1).expect("non-zero should pass");
    }

    #[test]
    fn enforce_fixture_sanity_reports_actionable_remediation() {
        let mut config = test_config();
        config.skip_fixtures = false;
        config.fixtures_dir = PathBuf::from("/tmp/fixtures");
        config.min_fixture_json_files = 3;
        config.min_fixture_entries = 3;
        config.min_fixture_sql_statements = 10;

        let report = FixtureIngestReport {
            fixture_json_files_seen: 1,
            fixture_entries_ingested: 1,
            sql_statements_ingested: 2,
            skipped_files: vec![
                fsqlite_harness::corpus_ingest::FixtureSkipDetail {
                    file: "a.json".to_owned(),
                    reason: "missing ops array".to_owned(),
                },
                fsqlite_harness::corpus_ingest::FixtureSkipDetail {
                    file: "b.json".to_owned(),
                    reason: "no ops[].sql statements found".to_owned(),
                },
            ],
        };

        let error = enforce_fixture_sanity(&report, &config).expect_err("threshold violation");
        assert!(error.contains("fixture_ingest_sanity_failed"));
        assert!(error.contains("fixture_json_files_seen=1 < min_fixture_json_files=3"));
        assert!(error.contains("skipped_fixture_examples="));
        assert!(error.contains("--skip-fixtures"));
    }

    #[test]
    fn enforce_slt_sanity_reports_actionable_remediation() {
        let mut config = test_config();
        config.skip_slt = false;
        config.slt_dir = PathBuf::from("/tmp/slt");
        config.min_slt_files = 2;
        config.min_slt_entries = 3;
        config.min_slt_sql_statements = 5;

        let report = SltIngestReport {
            slt_files_seen: 1,
            slt_entries_ingested: 1,
            sql_statements_ingested: 2,
            skipped_files: vec![
                fsqlite_harness::corpus_ingest::SltSkipDetail {
                    file: "bad.slt".to_owned(),
                    reason: "no SLT entries parsed".to_owned(),
                },
                fsqlite_harness::corpus_ingest::SltSkipDetail {
                    file: "empty.test".to_owned(),
                    reason: "parsed SLT entries contained no SQL statements".to_owned(),
                },
            ],
        };

        let error = enforce_slt_sanity(&report, &config).expect_err("threshold violation");
        assert!(error.contains("slt_ingest_sanity_failed"));
        assert!(error.contains("slt_files_seen=1 < min_slt_files=2"));
        assert!(error.contains("skipped_slt_examples="));
        assert!(error.contains("--skip-slt"));
    }
}
