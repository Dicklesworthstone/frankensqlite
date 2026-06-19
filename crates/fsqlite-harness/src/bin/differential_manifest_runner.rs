use std::env;
use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use serde::Serialize;
use sha2::{Digest, Sha256};

use fsqlite_harness::corpus_ingest::{
    CorpusBuilder, FixtureIngestReport, SltIngestReport, generate_seed_corpus,
    ingest_conformance_fixtures_with_report, ingest_slt_files_with_report,
};
use fsqlite_harness::differential_runner::{
    CaseResult, DifferentialRunReport, DivergenceSource, RunConfig, run_metamorphic_differential,
};
use fsqlite_harness::differential_v2::{CsqliteExecutor, FsqliteExecutor};
use fsqlite_harness::fixture_root_contract::{
    DEFAULT_FIXTURE_ROOT_MANIFEST_PATH, enforce_fixture_contract_alignment,
    load_fixture_root_contract,
};
use fsqlite_harness::metamorphic::MismatchClassification;
use fsqlite_harness::mismatch_minimizer::Subsystem;

const BEAD_ID: &str = "bd-mblr.7.1.2";
const DEFAULT_SCENARIO_ID: &str = "DIFF-712";
const DEFAULT_OUTPUT_PREFIX: &str = "artifacts/differential-manifest";
const MANIFEST_JSON_ARTIFACT_ENTRY: &str = "differential_manifest_json";
const MANIFEST_SUMMARY_ARTIFACT_ENTRY: &str = "differential_manifest_summary";
const RCH_STREAM_ARTIFACTS_ENV: &str = "FSQLITE_RCH_STREAM_ARTIFACTS";
const RCH_JSON_ARTIFACT_BEGIN: &str = "__FSQLITE_RCH_JSON_ARTIFACT_BEGIN__";
const RCH_JSON_ARTIFACT_END: &str = "__FSQLITE_RCH_JSON_ARTIFACT_END__";
const RCH_HUMAN_ARTIFACT_BEGIN: &str = "__FSQLITE_RCH_HUMAN_ARTIFACT_BEGIN__";
const RCH_HUMAN_ARTIFACT_END: &str = "__FSQLITE_RCH_HUMAN_ARTIFACT_END__";

#[derive(Debug, Clone)]
struct Config {
    workspace_root: PathBuf,
    output_json: PathBuf,
    output_human: PathBuf,
    fixture_root_manifest_path: PathBuf,
    fixture_root_manifest_sha256: String,
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
        let mut fixture_root_manifest_path: Option<PathBuf> = None;
        let mut fixtures_dir: Option<PathBuf> = None;
        let mut slt_dir: Option<PathBuf> = None;
        let mut run_id: Option<String> = None;
        let mut trace_id: Option<String> = None;
        let mut scenario_id = DEFAULT_SCENARIO_ID.to_owned();
        let mut root_seed = 424_242_u64;
        let mut max_cases_per_entry = RunConfig::default().max_cases_per_entry;
        let mut max_entries: Option<usize> = None;
        let mut min_fixture_json_files: Option<usize> = None;
        let mut min_fixture_entries: Option<usize> = None;
        let mut min_fixture_sql_statements: Option<usize> = None;
        let mut min_slt_files: Option<usize> = None;
        let mut min_slt_entries: Option<usize> = None;
        let mut min_slt_sql_statements: Option<usize> = None;
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
                "--fixture-root-manifest" => {
                    index += 1;
                    let value = args
                        .get(index)
                        .ok_or_else(|| "missing value for --fixture-root-manifest".to_owned())?;
                    fixture_root_manifest_path = Some(PathBuf::from(value));
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
                    min_fixture_json_files = Some(value.parse::<usize>().map_err(|error| {
                        format!("invalid --min-fixture-json-files value={value}: {error}")
                    })?);
                }
                "--min-fixture-entries" => {
                    index += 1;
                    let value = args
                        .get(index)
                        .ok_or_else(|| "missing value for --min-fixture-entries".to_owned())?;
                    min_fixture_entries = Some(value.parse::<usize>().map_err(|error| {
                        format!("invalid --min-fixture-entries value={value}: {error}")
                    })?);
                }
                "--min-fixture-sql-statements" => {
                    index += 1;
                    let value = args.get(index).ok_or_else(|| {
                        "missing value for --min-fixture-sql-statements".to_owned()
                    })?;
                    min_fixture_sql_statements = Some(value.parse::<usize>().map_err(|error| {
                        format!("invalid --min-fixture-sql-statements value={value}: {error}")
                    })?);
                }
                "--min-slt-files" => {
                    index += 1;
                    let value = args
                        .get(index)
                        .ok_or_else(|| "missing value for --min-slt-files".to_owned())?;
                    min_slt_files = Some(value.parse::<usize>().map_err(|error| {
                        format!("invalid --min-slt-files value={value}: {error}")
                    })?);
                }
                "--min-slt-entries" => {
                    index += 1;
                    let value = args
                        .get(index)
                        .ok_or_else(|| "missing value for --min-slt-entries".to_owned())?;
                    min_slt_entries = Some(value.parse::<usize>().map_err(|error| {
                        format!("invalid --min-slt-entries value={value}: {error}")
                    })?);
                }
                "--min-slt-sql-statements" => {
                    index += 1;
                    let value = args
                        .get(index)
                        .ok_or_else(|| "missing value for --min-slt-sql-statements".to_owned())?;
                    min_slt_sql_statements = Some(value.parse::<usize>().map_err(|error| {
                        format!("invalid --min-slt-sql-statements value={value}: {error}")
                    })?);
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

        if scenario_id.trim().is_empty() {
            return Err("--scenario-id must be non-empty".to_owned());
        }
        if max_cases_per_entry == 0 {
            return Err("--max-cases-per-entry must be > 0".to_owned());
        }
        if let Some(max_entries) = max_entries {
            require_positive("--max-entries", max_entries)?;
        }

        let fixture_root_manifest_path = fixture_root_manifest_path
            .unwrap_or_else(|| PathBuf::from(DEFAULT_FIXTURE_ROOT_MANIFEST_PATH));
        let fixture_root_manifest_path = if fixture_root_manifest_path.is_relative() {
            workspace_root.join(fixture_root_manifest_path)
        } else {
            fixture_root_manifest_path
        };
        let fixture_root_contract =
            load_fixture_root_contract(&workspace_root, &fixture_root_manifest_path)?;

        let run_id = run_id
            .unwrap_or_else(|| format!("{BEAD_ID}-{}-{}", generated_unix_ms, std::process::id()));
        let trace_id = trace_id.unwrap_or_else(|| build_trace_id(&run_id));
        let output_json =
            output_json.unwrap_or_else(|| output_dir.join("differential_manifest.json"));
        let output_human =
            output_human.unwrap_or_else(|| output_dir.join("differential_manifest.md"));
        let fixtures_dir =
            fixtures_dir.unwrap_or_else(|| fixture_root_contract.fixtures_dir.clone());
        let fixtures_dir = if fixtures_dir.is_relative() {
            workspace_root.join(fixtures_dir)
        } else {
            fixtures_dir
        };
        let slt_dir = slt_dir.unwrap_or_else(|| fixture_root_contract.slt_dir.clone());
        let slt_dir = if slt_dir.is_relative() {
            workspace_root.join(slt_dir)
        } else {
            slt_dir
        };
        let min_fixture_json_files =
            min_fixture_json_files.unwrap_or(fixture_root_contract.min_fixture_json_files);
        let min_fixture_entries =
            min_fixture_entries.unwrap_or(fixture_root_contract.min_fixture_entries);
        let min_fixture_sql_statements =
            min_fixture_sql_statements.unwrap_or(fixture_root_contract.min_fixture_sql_statements);
        let min_slt_files = min_slt_files.unwrap_or(fixture_root_contract.min_slt_files);
        let min_slt_entries = min_slt_entries.unwrap_or(fixture_root_contract.min_slt_entries);
        let min_slt_sql_statements =
            min_slt_sql_statements.unwrap_or(fixture_root_contract.min_slt_sql_statements);

        require_positive("--min-fixture-json-files", min_fixture_json_files)?;
        require_positive("--min-fixture-entries", min_fixture_entries)?;
        require_positive("--min-fixture-sql-statements", min_fixture_sql_statements)?;
        require_positive("--min-slt-files", min_slt_files)?;
        require_positive("--min-slt-entries", min_slt_entries)?;
        require_positive("--min-slt-sql-statements", min_slt_sql_statements)?;
        enforce_fixture_contract_alignment(
            &fixture_root_contract,
            &fixtures_dir,
            &slt_dir,
            min_fixture_json_files,
            min_fixture_entries,
            min_fixture_sql_statements,
            min_slt_files,
            min_slt_entries,
            min_slt_sql_statements,
        )?;

        Ok(Self {
            workspace_root,
            output_json,
            output_human,
            fixture_root_manifest_path: fixture_root_contract.manifest_path,
            fixture_root_manifest_sha256: fixture_root_contract.manifest_sha256,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum RootCauseDomain {
    Parser,
    Planner,
    Vdbe,
    Storage,
    Harness,
    Fixture,
}

impl RootCauseDomain {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Parser => "parser",
            Self::Planner => "planner",
            Self::Vdbe => "vdbe",
            Self::Storage => "storage",
            Self::Harness => "harness",
            Self::Fixture => "fixture",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct RemediationPlaybook {
    summary: String,
    next_commands: Vec<String>,
    evidence_json_pointer: String,
    owner_hint: String,
}

#[derive(Debug, Clone, Serialize)]
struct FirstFailureReplay {
    case_id: String,
    transform_name: String,
    divergence_source: String,
    statement_index: Option<usize>,
    root_cause_domain: RootCauseDomain,
    remediation_playbook: RemediationPlaybook,
    replay_command: String,
    diagnostic_json_pointer: String,
    minimal_reproduction_json_pointer: Option<String>,
    artifact_entries: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct SampledPassingReplay {
    case_id: String,
    transform_name: String,
    seed: u64,
    replay_command: String,
    diagnostic_json_pointer: String,
    artifact_entries: Vec<String>,
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
    fixture_root_manifest_path: String,
    fixture_root_manifest_sha256: String,
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
    sampled_passing_replays: Vec<SampledPassingReplay>,
    replay: ReplayCommand,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FirstFailureLogFields<'a> {
    root_cause_domain: &'a str,
    diagnostic_json_pointer: &'a str,
    replay_present: bool,
    artifact_entries: usize,
}

fn print_help() {
    println!(
        "\
differential_manifest_runner — deterministic differential manifest generator ({BEAD_ID})

USAGE:
  cargo run -p fsqlite-harness --bin differential_manifest_runner -- [OPTIONS]

OPTIONS:
  --workspace-root <PATH>        Workspace root (default: auto-detected)
  --fixture-root-manifest <PATH> Canonical fixture-root manifest (default: <workspace-root>/docs/contracts/corpus_manifest.toml)
  --output-dir <PATH>            Output directory (default: artifacts/differential-manifest)
  --output-json <PATH>           Output JSON path (default: <output-dir>/differential_manifest.json)
  --output-human <PATH>          Output Markdown summary path (default: <output-dir>/differential_manifest.md)
  --fixtures-dir <PATH>          Conformance fixtures directory (must align with fixture-root manifest)
  --slt-dir <PATH>               SQLLogicTest directory (must align with fixture-root manifest)
  --min-fixture-json-files <N>   Minimum fixture JSON files required when fixtures are enabled (must align with fixture-root manifest)
  --min-fixture-entries <N>      Minimum fixture entries ingested when fixtures are enabled (must align with fixture-root manifest)
  --min-fixture-sql-statements <N>
                                 Minimum SQL statements extracted from fixtures (must align with fixture-root manifest)
  --min-slt-files <N>            Minimum SLT files required when SLT ingestion is enabled (must align with fixture-root manifest)
  --min-slt-entries <N>          Minimum parsed SLT entries when SLT ingestion is enabled (must align with fixture-root manifest)
  --min-slt-sql-statements <N>   Minimum SQL statements extracted from SLT when enabled (must align with fixture-root manifest)
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
    let hex = fsqlite_harness::bytes_to_lower_hex(digest);
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

fn maybe_stream_rch_artifacts(json: &str, human: &str) {
    if env::var_os(RCH_STREAM_ARTIFACTS_ENV).is_none() {
        return;
    }

    println!("\n{RCH_JSON_ARTIFACT_BEGIN}");
    println!("{json}");
    println!("{RCH_JSON_ARTIFACT_END}");
    println!("\n{RCH_HUMAN_ARTIFACT_BEGIN}");
    println!("{human}");
    println!("{RCH_HUMAN_ARTIFACT_END}");
}

fn shell_single_quote(value: &str) -> String {
    let escaped = value.replace('\'', "'\"'\"'");
    format!("'{escaped}'")
}

fn path_to_utf8(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

fn normalize_one_command(command: &str) -> Option<String> {
    if command.contains('\n') || command.contains('\r') {
        return None;
    }
    let normalized = command.trim();
    if normalized.is_empty() {
        return None;
    }
    Some(normalized.to_owned())
}

fn ensure_one_command(command: &str, field_name: &str) -> Result<String, String> {
    normalize_one_command(command)
        .ok_or_else(|| format!("{field_name}: must be non-empty, single-line command"))
}

fn ensure_artifact_entries(entries: &[String], field_name: &str) -> Result<(), String> {
    if entries.is_empty() {
        return Err(format!("{field_name} must be non-empty"));
    }
    if entries
        .iter()
        .any(|entry| normalize_one_command(entry).is_none())
    {
        return Err(format!(
            "{field_name} must contain non-empty, single-line values"
        ));
    }
    Ok(())
}

fn replay_artifact_entries() -> Vec<String> {
    vec![
        MANIFEST_JSON_ARTIFACT_ENTRY.to_owned(),
        MANIFEST_SUMMARY_ARTIFACT_ENTRY.to_owned(),
    ]
}

fn divergence_source_label(source: Option<DivergenceSource>) -> &'static str {
    match source {
        Some(DivergenceSource::Original) => "original",
        Some(DivergenceSource::Transformed) => "transformed",
        Some(DivergenceSource::CrossVariant) => "cross-variant",
        None => "unknown",
    }
}

fn root_cause_domain_from_subsystem(subsystem: Subsystem) -> RootCauseDomain {
    match subsystem {
        Subsystem::Parser => RootCauseDomain::Parser,
        Subsystem::Resolver | Subsystem::Planner => RootCauseDomain::Planner,
        Subsystem::Vdbe
        | Subsystem::Functions
        | Subsystem::Extension
        | Subsystem::TypeSystem
        | Subsystem::Pragma => RootCauseDomain::Vdbe,
        Subsystem::Storage | Subsystem::Wal | Subsystem::Mvcc => RootCauseDomain::Storage,
        Subsystem::Unknown => RootCauseDomain::Harness,
    }
}

fn contains_any(haystack: &str, needles: &[&str]) -> bool {
    needles.iter().any(|needle| haystack.contains(needle))
}

fn root_cause_domain_from_classification(
    classification: &MismatchClassification,
) -> RootCauseDomain {
    match classification {
        MismatchClassification::FalsePositive { reason } => {
            let normalized = reason.to_ascii_lowercase();
            if contains_any(&normalized, &["fixture", "corpus", "slt"]) {
                RootCauseDomain::Fixture
            } else {
                RootCauseDomain::Harness
            }
        }
        MismatchClassification::TrueDivergence { description } => {
            let normalized = description.to_ascii_lowercase();
            if contains_any(&normalized, &["parse", "parser", "syntax", "token"]) {
                RootCauseDomain::Parser
            } else if contains_any(
                &normalized,
                &[
                    "planner",
                    "optimizer",
                    "index",
                    "resolver",
                    "name resolution",
                ],
            ) {
                RootCauseDomain::Planner
            } else if contains_any(
                &normalized,
                &["btree", "b-tree", "page", "pager", "wal", "mvcc", "storage"],
            ) {
                RootCauseDomain::Storage
            } else if contains_any(&normalized, &["fixture", "corpus", "slt"]) {
                RootCauseDomain::Fixture
            } else {
                RootCauseDomain::Vdbe
            }
        }
        MismatchClassification::OrderDependentDifference
        | MismatchClassification::TypeAffinityDifference
        | MismatchClassification::NullHandlingDifference
        | MismatchClassification::FloatingPointDifference { .. } => RootCauseDomain::Vdbe,
    }
}

fn classify_root_cause_domain(first_case: &CaseResult) -> RootCauseDomain {
    if let Some(repro) = &first_case.minimal_reproduction {
        let subsystem_domain = root_cause_domain_from_subsystem(repro.signature.subsystem);
        match subsystem_domain {
            RootCauseDomain::Harness => {}
            domain => return domain,
        }
    }

    if let Some(classification) = &first_case.classification {
        return root_cause_domain_from_classification(classification);
    }

    let case_context = format!(
        "{} {}",
        first_case.case_id.to_ascii_lowercase(),
        first_case.transform_name.to_ascii_lowercase()
    );
    if contains_any(&case_context, &["fixture", "corpus", "slt"]) {
        return RootCauseDomain::Fixture;
    }

    match first_case.divergence_source {
        Some(DivergenceSource::Original | DivergenceSource::Transformed) => RootCauseDomain::Vdbe,
        Some(DivergenceSource::CrossVariant) | None => RootCauseDomain::Harness,
    }
}

fn domain_validation_command(domain: RootCauseDomain) -> &'static str {
    match domain {
        RootCauseDomain::Parser => "cargo test -p fsqlite-parser",
        RootCauseDomain::Planner => "cargo test -p fsqlite-planner",
        RootCauseDomain::Vdbe => "cargo test -p fsqlite-vdbe",
        RootCauseDomain::Storage => {
            "cargo test -p fsqlite-btree && cargo test -p fsqlite-pager && cargo test -p fsqlite-wal && cargo test -p fsqlite-mvcc"
        }
        RootCauseDomain::Harness | RootCauseDomain::Fixture => {
            "cargo test -p fsqlite-harness --bin differential_manifest_runner"
        }
    }
}

fn owner_hint(domain: RootCauseDomain) -> &'static str {
    match domain {
        RootCauseDomain::Parser => "parser owners: inspect crates/fsqlite-parser first",
        RootCauseDomain::Planner => "planner owners: inspect crates/fsqlite-planner first",
        RootCauseDomain::Vdbe => "vdbe owners: inspect crates/fsqlite-vdbe first",
        RootCauseDomain::Storage => {
            "storage owners: inspect btree, pager, wal, and mvcc crates first"
        }
        RootCauseDomain::Harness => "harness owners: inspect crates/fsqlite-harness first",
        RootCauseDomain::Fixture => {
            "fixture owners: inspect corpus fixtures, SLT intake, and fixture-root contract first"
        }
    }
}

fn playbook_summary(first_case: &CaseResult, domain: RootCauseDomain) -> String {
    let classification = first_case
        .classification
        .as_ref()
        .map_or_else(|| "unclassified".to_owned(), ToString::to_string);
    let repro_summary = first_case.minimal_reproduction.as_ref().map_or_else(
        || "no minimized reproduction attached".to_owned(),
        |repro| {
            format!(
                "signature={} subsystem={} minimal_statements={}",
                repro.signature.hash,
                repro.signature.subsystem,
                repro.signature.minimal_statement_count
            )
        },
    );

    format!(
        "Start in the {} domain for case `{}` transform `{}`; classification={}; {}.",
        domain.as_str(),
        first_case.case_id,
        first_case.transform_name,
        classification,
        repro_summary
    )
}

fn build_remediation_playbook(
    first_case: &CaseResult,
    domain: RootCauseDomain,
    replay_command: &str,
    evidence_json_pointer: &str,
) -> Result<RemediationPlaybook, String> {
    let next_commands = [
        replay_command.to_owned(),
        domain_validation_command(domain).to_owned(),
    ]
    .into_iter()
    .map(|command| ensure_one_command(&command, "first_failure.remediation_playbook.next_commands"))
    .collect::<Result<Vec<_>, _>>()?;

    Ok(RemediationPlaybook {
        summary: playbook_summary(first_case, domain),
        next_commands,
        evidence_json_pointer: evidence_json_pointer.to_owned(),
        owner_hint: owner_hint(domain).to_owned(),
    })
}

fn first_failure_replay(
    run_report: &DifferentialRunReport,
    fallback_replay: &str,
) -> Result<Option<FirstFailureReplay>, String> {
    let Some(first_case) = run_report.divergent_cases.first() else {
        return Ok(None);
    };
    let fallback = fallback_replay.to_owned();
    let replay_command = first_case
        .minimal_reproduction
        .as_ref()
        .and_then(|repro| normalize_one_command(&repro.repro_command))
        .unwrap_or(fallback);
    let replay_command = ensure_one_command(&replay_command, "first_failure.replay_command")?;
    let diagnostic_json_pointer = "/run_report/divergent_cases/0".to_owned();
    let root_cause_domain = classify_root_cause_domain(first_case);
    let remediation_playbook = build_remediation_playbook(
        first_case,
        root_cause_domain,
        &replay_command,
        &diagnostic_json_pointer,
    )?;

    Ok(Some(FirstFailureReplay {
        case_id: first_case.case_id.clone(),
        transform_name: first_case.transform_name.clone(),
        divergence_source: divergence_source_label(first_case.divergence_source).to_owned(),
        statement_index: first_case
            .minimal_reproduction
            .as_ref()
            .and_then(|repro| repro.first_divergence_index),
        root_cause_domain,
        remediation_playbook,
        replay_command,
        diagnostic_json_pointer,
        minimal_reproduction_json_pointer: first_case
            .minimal_reproduction
            .as_ref()
            .map(|_| "/run_report/divergent_cases/0/minimal_reproduction".to_owned()),
        artifact_entries: replay_artifact_entries(),
    }))
}

fn sampled_passing_replays(
    run_report: &DifferentialRunReport,
    fallback_replay: &str,
) -> Result<Vec<SampledPassingReplay>, String> {
    let replay_command = ensure_one_command(fallback_replay, "sampled_passing_replay.command")?;
    let samples = run_report
        .sampled_passing_cases
        .iter()
        .enumerate()
        .map(|(index, sample)| SampledPassingReplay {
            case_id: sample.case_id.clone(),
            transform_name: sample.transform_name.clone(),
            seed: sample.seed,
            replay_command: replay_command.clone(),
            diagnostic_json_pointer: format!("/run_report/sampled_passing_cases/{index}"),
            artifact_entries: replay_artifact_entries(),
        })
        .collect::<Vec<_>>();

    if run_report.passed > 0 && samples.is_empty() {
        return Err(
            "sampled_passing_replay_generation_failed: passed>0 but no replay samples".to_owned(),
        );
    }
    if run_report.passed == 0 && !samples.is_empty() {
        return Err(
            "sampled_passing_replay_generation_failed: samples present with passed=0".to_owned(),
        );
    }

    Ok(samples)
}

fn build_replay_command(config: &Config) -> String {
    let mut replay = format!(
        "cargo run -p fsqlite-harness --bin differential_manifest_runner -- --workspace-root {} --fixture-root-manifest {} --run-id {} --trace-id {} --scenario-id {} --root-seed {} --max-cases-per-entry {} --generated-unix-ms {}",
        shell_single_quote(&path_to_utf8(&config.workspace_root)),
        shell_single_quote(&path_to_utf8(&config.fixture_root_manifest_path)),
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
    replay
}

fn validate_manifest_replay_contract(manifest: &DifferentialManifest) -> Result<(), String> {
    ensure_one_command(&manifest.replay.command, "replay.command")?;

    if manifest.run_report.diverged > 0 && manifest.first_failure.is_none() {
        return Err("first_failure missing while run_report.diverged > 0".to_owned());
    }
    if manifest.run_report.passed > 0 && manifest.sampled_passing_replays.is_empty() {
        return Err("sampled_passing_replays missing while run_report.passed > 0".to_owned());
    }

    if let Some(first_failure) = &manifest.first_failure {
        ensure_one_command(
            &first_failure.replay_command,
            "first_failure.replay_command",
        )?;
        if first_failure.remediation_playbook.summary.trim().is_empty() {
            return Err("first_failure.remediation_playbook.summary must be non-empty".to_owned());
        }
        if first_failure
            .remediation_playbook
            .evidence_json_pointer
            .trim()
            .is_empty()
        {
            return Err(
                "first_failure.remediation_playbook.evidence_json_pointer must be non-empty"
                    .to_owned(),
            );
        }
        if first_failure
            .remediation_playbook
            .owner_hint
            .trim()
            .is_empty()
        {
            return Err(
                "first_failure.remediation_playbook.owner_hint must be non-empty".to_owned(),
            );
        }
        if first_failure.remediation_playbook.next_commands.is_empty() {
            return Err(
                "first_failure.remediation_playbook.next_commands must be non-empty".to_owned(),
            );
        }
        for command in &first_failure.remediation_playbook.next_commands {
            ensure_one_command(command, "first_failure.remediation_playbook.next_commands")?;
        }
        if first_failure.diagnostic_json_pointer.trim().is_empty() {
            return Err("first_failure.diagnostic_json_pointer must be non-empty".to_owned());
        }
        ensure_artifact_entries(
            &first_failure.artifact_entries,
            "first_failure.artifact_entries",
        )?;
    }

    for sample in &manifest.sampled_passing_replays {
        ensure_one_command(&sample.replay_command, "sampled_passing_replay.command")?;
        if sample.diagnostic_json_pointer.trim().is_empty() {
            return Err(
                "sampled_passing_replay.diagnostic_json_pointer must be non-empty".to_owned(),
            );
        }
        ensure_artifact_entries(
            &sample.artifact_entries,
            "sampled_passing_replay.artifact_entries",
        )?;
    }

    Ok(())
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
    let first_failure_pointer = manifest.first_failure.as_ref().map_or_else(
        || "none".to_owned(),
        |failure| failure.diagnostic_json_pointer.clone(),
    );
    let first_failure_minimal_pointer = manifest.first_failure.as_ref().map_or_else(
        || "none".to_owned(),
        |failure| {
            failure
                .minimal_reproduction_json_pointer
                .clone()
                .unwrap_or_else(|| "none".to_owned())
        },
    );
    let first_failure_artifact_entries = manifest.first_failure.as_ref().map_or_else(
        || "none".to_owned(),
        |failure| failure.artifact_entries.join(", "),
    );
    let first_failure_root_cause_domain = manifest.first_failure.as_ref().map_or_else(
        || "none".to_owned(),
        |failure| failure.root_cause_domain.as_str().to_owned(),
    );
    let first_failure_remediation = manifest.first_failure.as_ref().map_or_else(
        || "none".to_owned(),
        |failure| {
            let commands = failure
                .remediation_playbook
                .next_commands
                .iter()
                .map(|command| format!("- `{command}`"))
                .collect::<Vec<_>>()
                .join("\n");
            format!(
                "summary: {}\nowner_hint: {}\nevidence_json_pointer: {}\nnext_commands:\n{}",
                failure.remediation_playbook.summary,
                failure.remediation_playbook.owner_hint,
                failure.remediation_playbook.evidence_json_pointer,
                commands,
            )
        },
    );
    let sampled_passing_section = if manifest.sampled_passing_replays.is_empty() {
        "none".to_owned()
    } else {
        manifest
            .sampled_passing_replays
            .iter()
            .map(|sample| {
                format!(
                    "- case_id=`{}` transform=`{}` seed=`{}` pointer=`{}` artifacts=`{}`\n  replay=`{}`",
                    sample.case_id,
                    sample.transform_name,
                    sample.seed,
                    sample.diagnostic_json_pointer,
                    sample.artifact_entries.join(", "),
                    sample.replay_command
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    };

    format!(
        "# Differential Manifest ({BEAD_ID})\n\n\
run_id: `{}`\n\
trace_id: `{}`\n\
scenario_id: `{}`\n\
commit_sha: `{}`\n\
root_seed: `{}`\n\
fixture_root_manifest_path: `{}`\n\
fixture_root_manifest_sha256: `{}`\n\
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
first_failure_json_pointer: `{}`\n\
first_failure_minimal_repro_pointer: `{}`\n\
first_failure_artifact_entries: `{}`\n\n\
first_failure_root_cause_domain: `{}`\n\n\
## Replay\n\n\
`{}`\n\n\
## First Failure Replay\n\n\
`{}`\n\n\
## First Failure Remediation\n\n\
{}\n\n\
## Sampled Passing Replays\n\n\
{}\n",
        manifest.run_id,
        manifest.trace_id,
        manifest.scenario_id,
        manifest.commit_sha,
        manifest.root_seed,
        manifest.fixture_root_manifest_path,
        manifest.fixture_root_manifest_sha256,
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
        first_failure_pointer,
        first_failure_minimal_pointer,
        first_failure_artifact_entries,
        first_failure_root_cause_domain,
        manifest.replay.command,
        first_failure_replay,
        first_failure_remediation,
        sampled_passing_section,
    )
}

fn first_failure_log_fields(manifest: &DifferentialManifest) -> FirstFailureLogFields<'_> {
    manifest.first_failure.as_ref().map_or(
        FirstFailureLogFields {
            root_cause_domain: "none",
            diagnostic_json_pointer: "none",
            replay_present: false,
            artifact_entries: 0,
        },
        |failure| FirstFailureLogFields {
            root_cause_domain: failure.root_cause_domain.as_str(),
            diagnostic_json_pointer: failure.diagnostic_json_pointer.as_str(),
            replay_present: !failure.replay_command.trim().is_empty(),
            artifact_entries: failure.artifact_entries.len(),
        },
    )
}

fn outcome_label(overall_pass: bool) -> &'static str {
    if overall_pass { "pass" } else { "warn" }
}

fn differential_manifest_written_log_line(
    output_json: &Path,
    manifest: &DifferentialManifest,
    elapsed_ms: u128,
) -> String {
    let first_failure_log = first_failure_log_fields(manifest);
    format!(
        "INFO differential_manifest_written path={} run_id={} trace_id={} scenario_id={} seed={} elapsed_ms={} outcome={} diverged={} total_cases={} data_hash={} first_failure_domain={} first_failure_pointer={} first_failure_replay_present={} first_failure_artifact_entries={}",
        output_json.display(),
        manifest.run_id,
        manifest.trace_id,
        manifest.scenario_id,
        manifest.root_seed,
        elapsed_ms,
        outcome_label(manifest.overall_pass),
        manifest.run_report.diverged,
        manifest.run_report.total_cases,
        manifest.run_report.data_hash,
        first_failure_log.root_cause_domain,
        first_failure_log.diagnostic_json_pointer,
        first_failure_log.replay_present,
        first_failure_log.artifact_entries,
    )
}

fn run() -> Result<bool, String> {
    let run_started = Instant::now();
    let config = Config::parse()?;

    let mut builder = CorpusBuilder::new(config.root_seed);
    generate_seed_corpus(&mut builder);

    let fixture_report = if config.skip_fixtures {
        FixtureIngestReport::default()
    } else {
        if !config.fixtures_dir.exists() {
            return Err(format!(
                "fixture_ingest_failed: fixtures directory does not exist: {}. \
                 remediation: pass --fixtures-dir <PATH> matching fixture-root contract in {}, \
                 or run with --skip-fixtures if seed-only execution is intentional.",
                config.fixtures_dir.display(),
                config.fixture_root_manifest_path.display(),
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
                 remediation: pass --slt-dir <PATH> matching fixture-root contract in {}, \
                 or run with --skip-slt.",
                config.slt_dir.display(),
                config.fixture_root_manifest_path.display(),
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

    let replay_command = ensure_one_command(
        &build_replay_command(&config),
        "replay_command_generation_failed",
    )?;
    let first_failure = first_failure_replay(&run_report, &replay_command)?;
    let sampled_passing_replays = sampled_passing_replays(&run_report, &replay_command)?;

    let manifest = DifferentialManifest {
        schema_version: 1,
        bead_id: BEAD_ID.to_owned(),
        run_id: config.run_id.clone(),
        trace_id: config.trace_id.clone(),
        scenario_id: config.scenario_id.clone(),
        generated_unix_ms: config.generated_unix_ms,
        commit_sha: resolve_commit_sha(&config.workspace_root),
        root_seed: config.root_seed,
        fixture_root_manifest_path: path_to_utf8(&config.fixture_root_manifest_path),
        fixture_root_manifest_sha256: config.fixture_root_manifest_sha256.clone(),
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
        sampled_passing_replays,
        replay: ReplayCommand {
            command: replay_command,
        },
    };
    validate_manifest_replay_contract(&manifest)?;

    let json = serde_json::to_string_pretty(&manifest)
        .map_err(|error| format!("manifest_serialize_failed: {error}"))?;
    let human = build_human_summary(&manifest);

    write_text(&config.output_json, &json)?;
    write_text(&config.output_human, &human)?;
    maybe_stream_rch_artifacts(&json, &human);

    let run_elapsed_ms = run_started.elapsed().as_millis();
    println!(
        "{}",
        differential_manifest_written_log_line(&config.output_json, &manifest, run_elapsed_ms)
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
        ". remediation: verify fixture quality/size in {} and confirm canonical fixture-root contract in {}. If seed-only execution was intended, rerun with --skip-fixtures.",
        config.fixtures_dir.display(),
        config.fixture_root_manifest_path.display(),
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
        ". remediation: verify SQLLogicTest suite quality/size in {} and confirm canonical fixture-root contract in {}. If SLT ingestion was not intended, rerun with --skip-slt.",
        config.slt_dir.display(),
        config.fixture_root_manifest_path.display(),
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
    use fsqlite_harness::metamorphic::EquivalenceExpectation;
    use fsqlite_harness::mismatch_minimizer::{
        DeduplicatedFailures, MinimalReproduction, MismatchSignature,
    };
    use proptest::prelude::*;

    fn test_config() -> Config {
        Config {
            workspace_root: PathBuf::from("/tmp/workspace"),
            output_json: PathBuf::from(
                "artifacts/differential-manifest/differential_manifest.json",
            ),
            output_human: PathBuf::from("artifacts/differential-manifest/differential_manifest.md"),
            fixture_root_manifest_path: PathBuf::from("/tmp/workspace/corpus_manifest.toml"),
            fixture_root_manifest_sha256:
                "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_owned(),
            fixtures_dir: PathBuf::from("/tmp/workspace/crates/fsqlite-harness/conformance"),
            slt_dir: PathBuf::from("/tmp/workspace/conformance/slt"),
            run_id: "bd-mblr.7.1.2-test-run".to_owned(),
            trace_id: "trace-test".to_owned(),
            scenario_id: "DIFF-712".to_owned(),
            root_seed: 424_242,
            max_cases_per_entry: 8,
            max_entries: Some(64),
            min_fixture_json_files: 8,
            min_fixture_entries: 8,
            min_fixture_sql_statements: 40,
            min_slt_files: 1,
            min_slt_entries: 1,
            min_slt_sql_statements: 1,
            generated_unix_ms: 1_700_000_000_000,
            skip_fixtures: true,
            skip_slt: true,
        }
    }

    fn empty_run_report(
        passed: usize,
        diverged: usize,
        divergent_cases: Vec<CaseResult>,
    ) -> DifferentialRunReport {
        DifferentialRunReport {
            bead_id: BEAD_ID.to_owned(),
            data_hash: "abc123".to_owned(),
            base_seed: 424_242,
            total_cases: passed + diverged,
            passed,
            diverged,
            sampled_passing_cases: Vec::new(),
            skipped: 0,
            divergent_cases,
            deduplicated: DeduplicatedFailures::default(),
            coverage_summary: CoverageSummary::default(),
        }
    }

    fn manifest_for_validation(
        run_report: DifferentialRunReport,
        first_failure: Option<FirstFailureReplay>,
        sampled_passing_replays: Vec<SampledPassingReplay>,
    ) -> DifferentialManifest {
        let config = test_config();
        let replay = build_replay_command(&config);

        DifferentialManifest {
            schema_version: 1,
            bead_id: BEAD_ID.to_owned(),
            run_id: config.run_id.clone(),
            trace_id: config.trace_id.clone(),
            scenario_id: config.scenario_id.clone(),
            generated_unix_ms: config.generated_unix_ms,
            commit_sha: "deadbeef".to_owned(),
            root_seed: config.root_seed,
            fixture_root_manifest_path: path_to_utf8(&config.fixture_root_manifest_path),
            fixture_root_manifest_sha256: config.fixture_root_manifest_sha256.clone(),
            fixture_json_files_seen: 0,
            fixture_entries_ingested: 0,
            fixture_sql_statements_ingested: 0,
            min_fixture_json_files: config.min_fixture_json_files,
            min_fixture_entries: config.min_fixture_entries,
            min_fixture_sql_statements: config.min_fixture_sql_statements,
            slt_files_seen: 0,
            slt_entries_ingested: 0,
            slt_sql_statements_ingested: 0,
            min_slt_files: config.min_slt_files,
            min_slt_entries: config.min_slt_entries,
            min_slt_sql_statements: config.min_slt_sql_statements,
            corpus_entries: 1,
            overall_pass: run_report.diverged == 0,
            run_report,
            first_failure,
            sampled_passing_replays,
            replay: ReplayCommand { command: replay },
        }
    }

    fn valid_first_failure(root_cause_domain: RootCauseDomain) -> FirstFailureReplay {
        FirstFailureReplay {
            case_id: "case-1".to_owned(),
            transform_name: "identity".to_owned(),
            divergence_source: "original".to_owned(),
            statement_index: Some(0),
            root_cause_domain,
            remediation_playbook: RemediationPlaybook {
                summary: "Start with the harness evidence".to_owned(),
                next_commands: vec![
                    "cargo run -p fsqlite-harness --bin differential_manifest_runner".to_owned(),
                ],
                evidence_json_pointer: "/run_report/divergent_cases/0".to_owned(),
                owner_hint: "harness owners: inspect crates/fsqlite-harness first".to_owned(),
            },
            replay_command: "cargo run -p fsqlite-harness --bin differential_manifest_runner"
                .to_owned(),
            diagnostic_json_pointer: "/run_report/divergent_cases/0".to_owned(),
            minimal_reproduction_json_pointer: None,
            artifact_entries: replay_artifact_entries(),
        }
    }

    fn valid_sampled_passing_replay() -> SampledPassingReplay {
        SampledPassingReplay {
            case_id: "case-pass-1".to_owned(),
            transform_name: "identity".to_owned(),
            seed: 11,
            replay_command: "cargo run -p fsqlite-harness --bin differential_manifest_runner"
                .to_owned(),
            diagnostic_json_pointer: "/run_report/sampled_passing_cases/0".to_owned(),
            artifact_entries: replay_artifact_entries(),
        }
    }

    fn minimal_reproduction(subsystem: Subsystem) -> MinimalReproduction {
        MinimalReproduction {
            schema_version: fsqlite_harness::mismatch_minimizer::MINIMIZER_SCHEMA_VERSION,
            signature: MismatchSignature {
                hash: "feedfacecafebeef".to_owned(),
                classification: MismatchClassification::TrueDivergence {
                    description: "pager page image mismatch".to_owned(),
                },
                subsystem,
                minimal_statement_count: 1,
                first_diverging_sql: "SELECT * FROM t".to_owned(),
            },
            original_seed: 7,
            schema: vec!["CREATE TABLE t(x)".to_owned()],
            minimal_workload: vec!["SELECT * FROM t".to_owned()],
            original_workload_size: 4,
            reduction_ratio: 0.75,
            first_divergence_index: Some(0),
            divergences: Vec::new(),
            repro_command: "cargo run -p fsqlite-harness --bin replay -- --case case-1".to_owned(),
        }
    }

    fn divergent_case_with_repro(subsystem: Subsystem) -> CaseResult {
        CaseResult {
            case_id: "case-1".to_owned(),
            transform_name: "identity".to_owned(),
            equivalence: EquivalenceExpectation::ExactRowMatch,
            original_passed: false,
            transformed_passed: true,
            classification: Some(MismatchClassification::TrueDivergence {
                description: "pager page image mismatch".to_owned(),
            }),
            minimal_reproduction: Some(minimal_reproduction(subsystem)),
            divergence_source: Some(DivergenceSource::Original),
        }
    }

    fn divergent_case_with_classification(
        case_id: &str,
        classification: MismatchClassification,
        divergence_source: Option<DivergenceSource>,
    ) -> CaseResult {
        CaseResult {
            case_id: case_id.to_owned(),
            transform_name: "identity".to_owned(),
            equivalence: EquivalenceExpectation::ExactRowMatch,
            original_passed: false,
            transformed_passed: true,
            classification: Some(classification),
            minimal_reproduction: None,
            divergence_source,
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

        assert!(replay.contains("--fixture-root-manifest '/tmp/workspace/corpus_manifest.toml'"));
        assert!(replay.contains("--root-seed 424242"));
        assert!(replay.contains("--max-cases-per-entry 8"));
        assert!(replay.contains("--max-entries 64"));
        assert!(replay.contains("--generated-unix-ms 1700000000000"));
        assert!(replay.contains("--skip-fixtures"));
        assert!(replay.contains("--skip-slt"));
        assert!(!replay.contains("--output-json"));
        assert!(!replay.contains("--output-human"));
    }

    #[test]
    fn replay_command_is_one_line_after_normalization() {
        let replay = build_replay_command(&test_config());
        let normalized = ensure_one_command(&replay, "replay.command")
            .expect("runner replay command should be one-line");
        assert_eq!(normalized, replay);
    }

    #[test]
    fn first_failure_replay_classifies_minimized_storage_and_builds_playbook() {
        let report = empty_run_report(0, 1, vec![divergent_case_with_repro(Subsystem::Wal)]);
        let first_failure = first_failure_replay(&report, "cargo run -p fsqlite-harness")
            .expect("first failure generation should succeed")
            .expect("divergent case should produce first failure");

        assert_eq!(first_failure.root_cause_domain, RootCauseDomain::Storage);
        assert_eq!(
            first_failure.minimal_reproduction_json_pointer.as_deref(),
            Some("/run_report/divergent_cases/0/minimal_reproduction")
        );
        assert_eq!(
            first_failure.replay_command,
            "cargo run -p fsqlite-harness --bin replay -- --case case-1"
        );
        assert!(
            first_failure
                .remediation_playbook
                .summary
                .contains("signature=feedfacecafebeef")
        );
        assert!(
            first_failure
                .remediation_playbook
                .next_commands
                .iter()
                .any(|command| command.contains("cargo test -p fsqlite-btree"))
        );
    }

    #[test]
    fn first_failure_replay_classifies_fixture_false_positive_without_repro() {
        let divergent_case = CaseResult {
            case_id: "fixture-case".to_owned(),
            transform_name: "fixture-load".to_owned(),
            equivalence: EquivalenceExpectation::ExactRowMatch,
            original_passed: false,
            transformed_passed: true,
            classification: Some(MismatchClassification::FalsePositive {
                reason: "fixture corpus is missing expected rows".to_owned(),
            }),
            minimal_reproduction: None,
            divergence_source: Some(DivergenceSource::CrossVariant),
        };
        let report = empty_run_report(0, 1, vec![divergent_case]);
        let first_failure = first_failure_replay(&report, "cargo run -p fsqlite-harness")
            .expect("first failure generation should succeed")
            .expect("divergent case should produce first failure");

        assert_eq!(first_failure.root_cause_domain, RootCauseDomain::Fixture);
        assert!(first_failure.minimal_reproduction_json_pointer.is_none());
        assert!(
            first_failure
                .remediation_playbook
                .owner_hint
                .contains("fixture owners")
        );
    }

    #[test]
    fn first_failure_replay_classifies_all_root_cause_domains() {
        let cases = vec![
            (
                "parser",
                divergent_case_with_repro(Subsystem::Parser),
                RootCauseDomain::Parser,
                "cargo test -p fsqlite-parser",
                "parser owners",
            ),
            (
                "planner",
                divergent_case_with_classification(
                    "planner-case",
                    MismatchClassification::TrueDivergence {
                        description: "name resolution failed before optimizer index choice"
                            .to_owned(),
                    },
                    Some(DivergenceSource::Original),
                ),
                RootCauseDomain::Planner,
                "cargo test -p fsqlite-planner",
                "planner owners",
            ),
            (
                "vdbe",
                divergent_case_with_classification(
                    "vdbe-case",
                    MismatchClassification::TypeAffinityDifference,
                    Some(DivergenceSource::Transformed),
                ),
                RootCauseDomain::Vdbe,
                "cargo test -p fsqlite-vdbe",
                "vdbe owners",
            ),
            (
                "storage",
                divergent_case_with_repro(Subsystem::Mvcc),
                RootCauseDomain::Storage,
                "cargo test -p fsqlite-btree",
                "storage owners",
            ),
            (
                "harness",
                divergent_case_with_classification(
                    "harness-case",
                    MismatchClassification::FalsePositive {
                        reason: "runner timeout produced invalid comparison evidence".to_owned(),
                    },
                    Some(DivergenceSource::CrossVariant),
                ),
                RootCauseDomain::Harness,
                "cargo test -p fsqlite-harness --bin differential_manifest_runner",
                "harness owners",
            ),
            (
                "fixture",
                divergent_case_with_classification(
                    "fixture-case",
                    MismatchClassification::FalsePositive {
                        reason: "fixture corpus expected rows were wrong".to_owned(),
                    },
                    Some(DivergenceSource::CrossVariant),
                ),
                RootCauseDomain::Fixture,
                "cargo test -p fsqlite-harness --bin differential_manifest_runner",
                "fixture owners",
            ),
        ];

        for (label, divergent_case, expected_domain, expected_command, expected_owner) in cases {
            let report = empty_run_report(0, 1, vec![divergent_case]);
            let first_failure = first_failure_replay(&report, "cargo run -p fsqlite-harness")
                .expect("first failure generation should succeed")
                .expect("divergent case should produce first failure");

            assert_eq!(
                first_failure.root_cause_domain, expected_domain,
                "unexpected domain for {label}"
            );
            assert!(
                first_failure
                    .remediation_playbook
                    .next_commands
                    .iter()
                    .any(|command| command.contains(expected_command)),
                "missing validation command for {label}"
            );
            assert!(
                first_failure
                    .remediation_playbook
                    .owner_hint
                    .contains(expected_owner),
                "missing owner hint for {label}"
            );
        }
    }

    proptest! {
        #[test]
        fn normalize_one_command_trims_single_line_commands(
            prefix in "[ \\t]{0,4}",
            core in "[!-~][ -~]{0,63}",
            suffix in "[ \\t]{0,4}",
        ) {
            let command = format!("{prefix}{core}{suffix}");
            let normalized = normalize_one_command(&command)
                .expect("non-empty single-line command should normalize");

            prop_assert_eq!(normalized, command.trim());
        }

        #[test]
        fn ensure_one_command_rejects_blank_and_multiline_commands(
            left in "[ -~]{0,32}",
            right in "[ -~]{0,32}",
            separator in prop_oneof![Just("\n"), Just("\r"), Just("\r\n")],
        ) {
            let blank_error = ensure_one_command(" \t ", "test.field")
                .expect_err("blank command should be rejected");
            prop_assert!(blank_error.contains("test.field"));

            let multiline = format!("{left}{separator}{right}");
            let multiline_error = ensure_one_command(&multiline, "test.field")
                .expect_err("multiline command should be rejected");
            prop_assert!(multiline_error.contains("single-line command"));
        }
    }

    #[test]
    fn replay_command_includes_fixture_thresholds_when_enabled() {
        let mut config = test_config();
        config.skip_fixtures = false;
        let replay = build_replay_command(&config);

        assert!(
            replay.contains("--fixtures-dir '/tmp/workspace/crates/fsqlite-harness/conformance'")
        );
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
        assert!(replay.contains("--slt-dir '/tmp/workspace/conformance/slt'"));
        assert!(replay.contains("--min-slt-files 1"));
        assert!(replay.contains("--min-slt-entries 1"));
        assert!(replay.contains("--min-slt-sql-statements 1"));
    }

    #[test]
    fn replay_command_is_stable_across_output_path_changes() {
        let mut config_a = test_config();
        config_a.output_json = PathBuf::from("/tmp/out/run-a.json");
        config_a.output_human = PathBuf::from("/tmp/out/run-a.md");

        let mut config_b = test_config();
        config_b.output_json = PathBuf::from("/tmp/out/run-b.json");
        config_b.output_human = PathBuf::from("/tmp/out/run-b.md");

        assert_eq!(
            build_replay_command(&config_a),
            build_replay_command(&config_b)
        );
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
            sampled_passing_cases: vec![fsqlite_harness::differential_runner::PassingCaseSample {
                case_id: "mm-corpus-pass-1".to_owned(),
                transform_name: "noop".to_owned(),
                seed: 42,
            }],
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
            fixture_root_manifest_path: path_to_utf8(&config.fixture_root_manifest_path),
            fixture_root_manifest_sha256: config.fixture_root_manifest_sha256.clone(),
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
            sampled_passing_replays: vec![SampledPassingReplay {
                case_id: "mm-corpus-pass-1".to_owned(),
                transform_name: "noop".to_owned(),
                seed: 42,
                replay_command: replay.clone(),
                diagnostic_json_pointer: "/run_report/sampled_passing_cases/0".to_owned(),
                artifact_entries: replay_artifact_entries(),
            }],
            replay: ReplayCommand {
                command: replay.clone(),
            },
        };

        let human = build_human_summary(&manifest);
        assert!(human.contains("run_id: `bd-mblr.7.1.2-test-run`"));
        assert!(human.contains("diverged: `1`"));
        assert!(human.contains("overall_pass: `false`"));
        assert!(human.contains(&replay));
        assert!(
            human.contains("fixture_root_manifest_path: `/tmp/workspace/corpus_manifest.toml`")
        );
        assert!(
            human.contains(
                "fixture_root_manifest_sha256: `0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef`"
            )
        );
        assert!(human.contains("fixture_json_files_seen: `9`"));
        assert!(human.contains("fixture_sql_statements_ingested: `24`"));
        assert!(human.contains("slt_files_seen: `2`"));
        assert!(human.contains("slt_entries_ingested: `6`"));
        assert!(human.contains("Sampled Passing Replays"));
    }

    #[test]
    fn human_summary_contains_first_failure_explainer_and_playbook() {
        let first_failure = valid_first_failure(RootCauseDomain::Planner);
        let run_report = empty_run_report(0, 1, Vec::new());
        let manifest = manifest_for_validation(run_report, Some(first_failure), Vec::new());

        let human = build_human_summary(&manifest);

        assert!(human.contains("first_failure_root_cause_domain: `planner`"));
        assert!(human.contains("## First Failure Remediation"));
        assert!(human.contains("summary: Start with the harness evidence"));
        assert!(
            human.contains("- `cargo run -p fsqlite-harness --bin differential_manifest_runner`")
        );
    }

    #[test]
    fn first_failure_log_fields_report_absence_explicitly() {
        let run_report = empty_run_report(0, 0, Vec::new());
        let manifest = manifest_for_validation(run_report, None, Vec::new());

        let fields = first_failure_log_fields(&manifest);

        assert_eq!(
            fields,
            FirstFailureLogFields {
                root_cause_domain: "none",
                diagnostic_json_pointer: "none",
                replay_present: false,
                artifact_entries: 0,
            }
        );
    }

    #[test]
    fn first_failure_log_fields_surface_diagnostics() {
        let first_failure = valid_first_failure(RootCauseDomain::Parser);
        let run_report = empty_run_report(0, 1, Vec::new());
        let manifest = manifest_for_validation(run_report, Some(first_failure), Vec::new());

        let fields = first_failure_log_fields(&manifest);

        assert_eq!(fields.root_cause_domain, "parser");
        assert_eq!(
            fields.diagnostic_json_pointer,
            "/run_report/divergent_cases/0"
        );
        assert!(fields.replay_present);
        assert_eq!(fields.artifact_entries, 2);
    }

    #[test]
    fn manifest_written_log_line_contains_ci_schema_fields() {
        let first_failure = valid_first_failure(RootCauseDomain::Parser);
        let run_report = empty_run_report(0, 1, Vec::new());
        let manifest = manifest_for_validation(run_report, Some(first_failure), Vec::new());

        let line = differential_manifest_written_log_line(
            Path::new("artifacts/differential-manifest/differential_manifest.json"),
            &manifest,
            123,
        );

        assert!(line.starts_with("INFO differential_manifest_written "));
        assert!(line.contains("run_id=bd-mblr.7.1.2-test-run"));
        assert!(line.contains("trace_id=trace-test"));
        assert!(line.contains("scenario_id=DIFF-712"));
        assert!(line.contains("seed=424242"));
        assert!(line.contains("elapsed_ms=123"));
        assert!(line.contains("outcome=warn"));
        assert!(line.contains("first_failure_domain=parser"));
        assert!(line.contains("first_failure_pointer=/run_report/divergent_cases/0"));
        assert!(line.contains("first_failure_replay_present=true"));
        assert!(line.contains("first_failure_artifact_entries=2"));
    }

    #[test]
    fn validate_manifest_replay_contract_requires_samples_when_passed() {
        let config = test_config();
        let replay = build_replay_command(&config);
        let run_report = DifferentialRunReport {
            bead_id: BEAD_ID.to_owned(),
            data_hash: "abc123".to_owned(),
            base_seed: config.root_seed,
            total_cases: 2,
            passed: 1,
            diverged: 0,
            sampled_passing_cases: vec![fsqlite_harness::differential_runner::PassingCaseSample {
                case_id: "mm-pass".to_owned(),
                transform_name: "noop".to_owned(),
                seed: 7,
            }],
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
            fixture_root_manifest_path: path_to_utf8(&config.fixture_root_manifest_path),
            fixture_root_manifest_sha256: config.fixture_root_manifest_sha256.clone(),
            fixture_json_files_seen: 0,
            fixture_entries_ingested: 0,
            fixture_sql_statements_ingested: 0,
            min_fixture_json_files: config.min_fixture_json_files,
            min_fixture_entries: config.min_fixture_entries,
            min_fixture_sql_statements: config.min_fixture_sql_statements,
            slt_files_seen: 0,
            slt_entries_ingested: 0,
            slt_sql_statements_ingested: 0,
            min_slt_files: config.min_slt_files,
            min_slt_entries: config.min_slt_entries,
            min_slt_sql_statements: config.min_slt_sql_statements,
            corpus_entries: 1,
            overall_pass: true,
            run_report,
            first_failure: None,
            sampled_passing_replays: Vec::new(),
            replay: ReplayCommand { command: replay },
        };

        let error = validate_manifest_replay_contract(&manifest)
            .expect_err("missing sampled passing replay contract should fail");
        assert!(error.contains("sampled_passing_replays"));
    }

    #[test]
    fn validate_manifest_replay_contract_rejects_empty_playbook_command() {
        let mut first_failure = valid_first_failure(RootCauseDomain::Harness);
        first_failure.remediation_playbook.next_commands = vec![" ".to_owned()];
        let run_report = empty_run_report(0, 1, Vec::new());
        let manifest = manifest_for_validation(run_report, Some(first_failure), Vec::new());

        let error = validate_manifest_replay_contract(&manifest)
            .expect_err("empty remediation command should fail");

        assert!(error.contains("first_failure.remediation_playbook.next_commands"));
    }

    #[test]
    fn validate_manifest_replay_contract_rejects_empty_first_failure_artifacts() {
        let mut first_failure = valid_first_failure(RootCauseDomain::Harness);
        first_failure.artifact_entries.clear();
        let run_report = empty_run_report(0, 1, Vec::new());
        let manifest = manifest_for_validation(run_report, Some(first_failure), Vec::new());

        let error = validate_manifest_replay_contract(&manifest)
            .expect_err("missing first-failure artifacts should fail");

        assert!(error.contains("first_failure.artifact_entries"));
    }

    #[test]
    fn validate_manifest_replay_contract_rejects_multiline_first_failure_artifacts() {
        let mut first_failure = valid_first_failure(RootCauseDomain::Harness);
        first_failure.artifact_entries = vec!["manifest\nsummary".to_owned()];
        let run_report = empty_run_report(0, 1, Vec::new());
        let manifest = manifest_for_validation(run_report, Some(first_failure), Vec::new());

        let error = validate_manifest_replay_contract(&manifest)
            .expect_err("multiline first-failure artifact should fail");

        assert!(error.contains("first_failure.artifact_entries"));
    }

    #[test]
    fn validate_manifest_replay_contract_rejects_empty_sampled_passing_artifacts() {
        let mut sample = valid_sampled_passing_replay();
        sample.artifact_entries.clear();
        let run_report = empty_run_report(1, 0, Vec::new());
        let manifest = manifest_for_validation(run_report, None, vec![sample]);

        let error = validate_manifest_replay_contract(&manifest)
            .expect_err("missing sampled passing artifacts should fail");

        assert!(error.contains("sampled_passing_replay.artifact_entries"));
    }

    #[test]
    fn validate_manifest_replay_contract_rejects_multiline_sampled_passing_artifacts() {
        let mut sample = valid_sampled_passing_replay();
        sample.artifact_entries = vec!["manifest\nsummary".to_owned()];
        let run_report = empty_run_report(1, 0, Vec::new());
        let manifest = manifest_for_validation(run_report, None, vec![sample]);

        let error = validate_manifest_replay_contract(&manifest)
            .expect_err("multiline sampled passing artifact should fail");

        assert!(error.contains("sampled_passing_replay.artifact_entries"));
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
