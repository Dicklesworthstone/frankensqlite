use std::env;
use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use fsqlite_harness::differential_v2::TARGET_SQLITE_VERSION;
use fsqlite_harness::oracle_preflight_doctor::{
    BEAD_ID, DEFAULT_SCENARIO_ID, DoctorConfig, DoctorOutcome, OraclePreflightReport,
    run_oracle_preflight_doctor,
};
use sha2::{Digest, Sha256};

const DEFAULT_OUTPUT_DIR: &str = "artifacts/oracle-preflight-doctor";

#[derive(Debug, Clone)]
struct Config {
    doctor: DoctorConfig,
    output_json: PathBuf,
    output_human: PathBuf,
}

impl Config {
    #[allow(clippy::too_many_lines)]
    fn parse() -> Result<Self, String> {
        let workspace_root = default_workspace_root()?;
        let mut doctor = DoctorConfig::new(workspace_root.clone());
        let mut output_dir = workspace_root.join(DEFAULT_OUTPUT_DIR);
        let mut output_json: Option<PathBuf> = None;
        let mut output_human: Option<PathBuf> = None;
        let mut workspace_root_overridden = false;
        let mut fixtures_dir_overridden = false;
        let mut fixture_manifest_overridden = false;
        let mut output_dir_overridden = false;

        let args: Vec<String> = env::args().skip(1).collect();
        let mut index = 0_usize;
        while index < args.len() {
            match args[index].as_str() {
                "--workspace-root" => {
                    index += 1;
                    let value = args
                        .get(index)
                        .ok_or_else(|| "missing value for --workspace-root".to_owned())?;
                    doctor.workspace_root = PathBuf::from(value);
                    workspace_root_overridden = true;
                }
                "--fixtures-dir" => {
                    index += 1;
                    let value = args
                        .get(index)
                        .ok_or_else(|| "missing value for --fixtures-dir".to_owned())?;
                    doctor.fixtures_dir = PathBuf::from(value);
                    fixtures_dir_overridden = true;
                }
                "--fixture-manifest-path" => {
                    index += 1;
                    let value = args
                        .get(index)
                        .ok_or_else(|| "missing value for --fixture-manifest-path".to_owned())?;
                    doctor.fixture_manifest_path = PathBuf::from(value);
                    fixture_manifest_overridden = true;
                }
                "--output-dir" => {
                    index += 1;
                    let value = args
                        .get(index)
                        .ok_or_else(|| "missing value for --output-dir".to_owned())?;
                    output_dir = PathBuf::from(value);
                    output_dir_overridden = true;
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
                "--run-id" => {
                    index += 1;
                    let value = args
                        .get(index)
                        .ok_or_else(|| "missing value for --run-id".to_owned())?;
                    value.clone_into(&mut doctor.run_id);
                    doctor.trace_id = build_trace_id(value);
                }
                "--trace-id" => {
                    index += 1;
                    let value = args
                        .get(index)
                        .ok_or_else(|| "missing value for --trace-id".to_owned())?;
                    value.clone_into(&mut doctor.trace_id);
                }
                "--scenario-id" => {
                    index += 1;
                    let value = args
                        .get(index)
                        .ok_or_else(|| "missing value for --scenario-id".to_owned())?;
                    value.clone_into(&mut doctor.scenario_id);
                }
                "--seed" => {
                    index += 1;
                    let value = args
                        .get(index)
                        .ok_or_else(|| "missing value for --seed".to_owned())?;
                    doctor.seed = value
                        .parse::<u64>()
                        .map_err(|error| format!("invalid --seed value={value}: {error}"))?;
                }
                "--generated-unix-ms" => {
                    index += 1;
                    let value = args
                        .get(index)
                        .ok_or_else(|| "missing value for --generated-unix-ms".to_owned())?;
                    doctor.generated_unix_ms = value.parse::<u128>().map_err(|error| {
                        format!("invalid --generated-unix-ms value={value}: {error}")
                    })?;
                }
                "--min-fixture-json-files" => {
                    index += 1;
                    let value = args
                        .get(index)
                        .ok_or_else(|| "missing value for --min-fixture-json-files".to_owned())?;
                    doctor.min_fixture_json_files = value.parse::<usize>().map_err(|error| {
                        format!("invalid --min-fixture-json-files value={value}: {error}")
                    })?;
                }
                "--min-fixture-entries" => {
                    index += 1;
                    let value = args
                        .get(index)
                        .ok_or_else(|| "missing value for --min-fixture-entries".to_owned())?;
                    doctor.min_fixture_entries = value.parse::<usize>().map_err(|error| {
                        format!("invalid --min-fixture-entries value={value}: {error}")
                    })?;
                }
                "--min-fixture-sql-statements" => {
                    index += 1;
                    let value = args.get(index).ok_or_else(|| {
                        "missing value for --min-fixture-sql-statements".to_owned()
                    })?;
                    doctor.min_fixture_sql_statements =
                        value.parse::<usize>().map_err(|error| {
                            format!("invalid --min-fixture-sql-statements value={value}: {error}")
                        })?;
                }
                "--expected-sqlite-version-prefix" => {
                    index += 1;
                    let value = args.get(index).ok_or_else(|| {
                        "missing value for --expected-sqlite-version-prefix".to_owned()
                    })?;
                    value.clone_into(&mut doctor.expected_sqlite_version_prefix);
                }
                "--expected-subject-identity" => {
                    index += 1;
                    let value = args.get(index).ok_or_else(|| {
                        "missing value for --expected-subject-identity".to_owned()
                    })?;
                    value.clone_into(&mut doctor.expected_subject_identity);
                }
                "--expected-reference-identity" => {
                    index += 1;
                    let value = args.get(index).ok_or_else(|| {
                        "missing value for --expected-reference-identity".to_owned()
                    })?;
                    value.clone_into(&mut doctor.expected_reference_identity);
                }
                "--oracle-binary" => {
                    index += 1;
                    let value = args
                        .get(index)
                        .ok_or_else(|| "missing value for --oracle-binary".to_owned())?;
                    doctor.oracle_binary_override = Some(PathBuf::from(value));
                }
                "--help" | "-h" => {
                    print_help();
                    std::process::exit(0);
                }
                other => return Err(format!("unknown_argument: {other}")),
            }
            index += 1;
        }

        if workspace_root_overridden && !fixtures_dir_overridden {
            doctor.fixtures_dir = doctor
                .workspace_root
                .join("crates/fsqlite-harness/conformance");
        }
        if workspace_root_overridden && !fixture_manifest_overridden {
            doctor.fixture_manifest_path = doctor.workspace_root.join("corpus_manifest.toml");
        }
        if workspace_root_overridden && !output_dir_overridden {
            output_dir = doctor.workspace_root.join(DEFAULT_OUTPUT_DIR);
        }

        if doctor.run_id.trim().is_empty() {
            doctor.run_id = format!(
                "{BEAD_ID}-doctor-{}-{}",
                doctor.generated_unix_ms,
                std::process::id()
            );
        }
        if doctor.trace_id.trim().is_empty() {
            doctor.trace_id = build_trace_id(&doctor.run_id);
        }
        if doctor.scenario_id.trim().is_empty() {
            DEFAULT_SCENARIO_ID.clone_into(&mut doctor.scenario_id);
        }

        if doctor.fixtures_dir.is_relative() {
            doctor.fixtures_dir = doctor.workspace_root.join(&doctor.fixtures_dir);
        }
        if doctor.fixture_manifest_path.is_relative() {
            doctor.fixture_manifest_path =
                doctor.workspace_root.join(&doctor.fixture_manifest_path);
        }
        if output_dir.is_relative() {
            output_dir = doctor.workspace_root.join(output_dir);
        }

        let output_json =
            output_json.unwrap_or_else(|| output_dir.join("oracle_preflight_doctor.json"));
        let output_human =
            output_human.unwrap_or_else(|| output_dir.join("oracle_preflight_doctor.md"));

        Ok(Self {
            doctor,
            output_json,
            output_human,
        })
    }
}

fn print_help() {
    println!(
        "\
oracle_preflight_doctor_runner — deterministic oracle readiness doctor ({BEAD_ID})

USAGE:
  cargo run -p fsqlite-harness --bin oracle_preflight_doctor_runner -- [OPTIONS]

OPTIONS:
  --workspace-root <PATH>             Workspace root (default: auto-detected)
  --fixtures-dir <PATH>               Conformance fixture directory
  --fixture-manifest-path <PATH>      Fixture manifest path
  --output-dir <PATH>                 Output directory (default: artifacts/oracle-preflight-doctor)
  --output-json <PATH>                Output JSON path
  --output-human <PATH>               Output Markdown summary path
  --run-id <ID>                       Deterministic run identifier
  --trace-id <ID>                     Deterministic trace identifier
  --scenario-id <ID>                  Scenario identifier (default: {DEFAULT_SCENARIO_ID})
  --seed <U64>                        Deterministic seed (default: 424242)
  --generated-unix-ms <U128>          Deterministic timestamp
  --oracle-binary <PATH>              Optional sqlite3 binary override path
  --expected-sqlite-version-prefix <S>
                                     Expected sqlite3 version prefix (default: {TARGET_SQLITE_VERSION})
  --expected-subject-identity <S>     Expected subject identity (default: frankensqlite)
  --expected-reference-identity <S>   Expected reference identity (default: csqlite-oracle)
  --min-fixture-json-files <N>        Fixture JSON threshold (>0)
  --min-fixture-entries <N>           Fixture entry threshold (>0)
  --min-fixture-sql-statements <N>    Fixture SQL statement threshold (>0)
  -h, --help                          Show help
"
    );
}

fn default_workspace_root() -> Result<PathBuf, String> {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .map_err(|error| format!("workspace_root_canonicalize_failed: {error}"))
}

fn build_trace_id(run_id: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(run_id.as_bytes());
    let digest = hasher.finalize();
    let hex = format!("{digest:x}");
    format!("trace-{}", &hex[..16])
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

fn build_human_summary(report: &OraclePreflightReport) -> String {
    let mut summary = format!(
        "# Oracle Preflight Doctor ({BEAD_ID})\n\n\
run_id: `{}`\n\
trace_id: `{}`\n\
scenario_id: `{}`\n\
seed: `{}`\n\
outcome: `{}`\n\
certifying: `{}`\n\
timing_ms: `{}`\n\
oracle_binary: `{}`\n\
oracle_version: `{}`\n\
expected_sqlite_version_prefix: `{}`\n\
fixtures_dir: `{}`\n\
fixture_manifest_path: `{}`\n\
fixture_json_files_seen: `{}`\n\
fixture_entries_ingested: `{}`\n\
fixture_sql_statements_ingested: `{}`\n\
skipped_fixture_files: `{}`\n",
        report.run_id,
        report.trace_id,
        report.scenario_id,
        report.seed,
        report.outcome,
        report.certifying,
        report.timing_ms,
        report
            .checks
            .oracle_binary_path
            .clone()
            .unwrap_or_else(|| "none".to_owned()),
        report
            .checks
            .oracle_version
            .clone()
            .unwrap_or_else(|| "none".to_owned()),
        report.checks.expected_sqlite_version_prefix,
        report.checks.fixtures_dir,
        report.checks.fixture_manifest_path,
        report.checks.fixture_json_files_seen,
        report.checks.fixture_entries_ingested,
        report.checks.fixture_sql_statements_ingested,
        report.checks.skipped_fixture_files,
    );

    if let Some(first_failure) = &report.first_failure {
        let _ = write!(
            summary,
            "first_failure: `{:?}` — `{}`\nfirst_failure_fix_command: `{}`\n",
            first_failure.remediation_class, first_failure.summary, first_failure.fix_command
        );
    } else {
        summary.push_str("first_failure: `none`\n");
    }

    summary.push_str("\n## Findings\n\n");
    if report.findings.is_empty() {
        summary.push_str("- none\n");
    } else {
        for finding in &report.findings {
            let _ = write!(
                summary,
                "- outcome=`{}` class=`{:?}` summary=`{}`\n  details: {}\n  fix_command: `{}`\n",
                finding.outcome,
                finding.remediation_class,
                finding.summary,
                finding.details,
                finding.fix_command,
            );
        }
    }

    let _ = write!(summary, "\n## Replay\n\n`{}`\n", report.replay_command);
    summary
}

fn run() -> Result<DoctorOutcome, String> {
    let config = Config::parse()?;
    let report = run_oracle_preflight_doctor(&config.doctor);

    let json = serde_json::to_string_pretty(&report)
        .map_err(|error| format!("report_serialize_failed: {error}"))?;
    let human = build_human_summary(&report);

    write_text(&config.output_json, &json)?;
    write_text(&config.output_human, &human)?;

    println!(
        "INFO oracle_preflight_doctor_report path={} outcome={} certifying={} findings={}",
        config.output_json.display(),
        report.outcome,
        report.certifying,
        report.findings.len(),
    );
    println!(
        "INFO oracle_preflight_doctor_summary path={}",
        config.output_human.display()
    );
    println!(
        "INFO oracle_preflight_doctor_replay command=\"{}\"",
        report.replay_command
    );
    Ok(report.outcome)
}

fn main() -> ExitCode {
    match run() {
        Ok(DoctorOutcome::Green) => ExitCode::SUCCESS,
        Ok(DoctorOutcome::Yellow) => ExitCode::from(1),
        Ok(DoctorOutcome::Red) => ExitCode::from(2),
        Err(error) => {
            eprintln!("ERROR oracle_preflight_doctor_runner failed: {error}");
            ExitCode::from(2)
        }
    }
}
