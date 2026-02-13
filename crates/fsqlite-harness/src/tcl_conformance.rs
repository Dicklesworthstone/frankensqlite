//! SQLite TCL harness conformance planning and execution support.
//!
//! Bead: `bd-3plop.7`.
//!
//! This module adds deterministic, machine-readable control-plane artifacts for
//! running upstream SQLite `testrunner.tcl` against FrankenSQLite integration
//! targets. The default execution mode is dry-run so CI can always produce
//! reproducible manifests even when TCL toolchain prerequisites are absent.

use std::collections::BTreeSet;
use std::fs;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

/// Bead identifier for logging and evidence correlation.
pub const BEAD_ID: &str = "bd-3plop.7";
/// Schema version for suite manifests.
pub const SUITE_SCHEMA_VERSION: u32 = 1;
/// Schema version for execution summaries.
pub const EXECUTION_SCHEMA_VERSION: u32 = 1;
/// Canonical logging standard reference.
pub const LOG_STANDARD_REF: &str = "bd-1fpm";
/// Default timeout budget for TCL harness execution.
pub const DEFAULT_TIMEOUT_SECS: u64 = 1_800;
/// Default SQLite source-tree-relative testrunner path.
pub const DEFAULT_TCL_RUNNER_REL_PATH: &str = "legacy_sqlite_code/sqlite/test/testrunner.tcl";

const DEFAULT_SQLITE_ROOT_REL_PATH: &str = "legacy_sqlite_code/sqlite";
const DEFAULT_POLL_INTERVAL_MILLIS: u64 = 25;

/// Execution mode for TCL harness orchestration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TclExecutionMode {
    DryRun,
    Execute,
}

/// Outcome classification for a scenario run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TclHarnessOutcome {
    Pass,
    Fail,
    Skipped,
    Timeout,
    Error,
}

/// Parsed testrunner summary counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TclRunnerCounts {
    /// Number of test errors reported by `testrunner.tcl`.
    pub errors: u64,
    /// Number of tests reported by `testrunner.tcl`.
    pub tests: u64,
    /// Number of jobs skipped due to prior failures.
    pub skipped_jobs: u64,
}

/// Declarative scenario describing one deterministic TCL harness invocation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TclHarnessScenario {
    pub id: String,
    pub description: String,
    pub args: Vec<String>,
}

/// Declarative TCL harness suite.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TclHarnessSuite {
    pub bead_id: String,
    pub schema_version: u32,
    pub log_standard_ref: String,
    pub runner_rel_path: String,
    pub scenarios: Vec<TclHarnessScenario>,
}

/// Runtime options for suite execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TclExecutionOptions {
    pub mode: TclExecutionMode,
    pub timeout_secs: u64,
    pub max_scenarios: Option<usize>,
    pub runner_override: Option<PathBuf>,
    pub run_id_override: Option<String>,
}

impl Default for TclExecutionOptions {
    fn default() -> Self {
        Self {
            mode: TclExecutionMode::DryRun,
            timeout_secs: DEFAULT_TIMEOUT_SECS,
            max_scenarios: None,
            runner_override: None,
            run_id_override: None,
        }
    }
}

/// Per-scenario execution result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TclHarnessScenarioResult {
    pub scenario_id: String,
    pub command: String,
    pub outcome: TclHarnessOutcome,
    pub reason: Option<String>,
    pub exit_code: Option<i32>,
    pub tests: u64,
    pub errors: u64,
    pub skipped_jobs: u64,
    pub elapsed_ms: u64,
    pub log_path: String,
}

/// Aggregated execution summary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TclHarnessExecutionSummary {
    pub bead_id: String,
    pub schema_version: u32,
    pub log_standard_ref: String,
    pub run_id: String,
    pub mode: TclExecutionMode,
    pub timeout_secs: u64,
    pub runner_path: String,
    pub total_scenarios: usize,
    pub passed_scenarios: usize,
    pub failed_scenarios: usize,
    pub skipped_scenarios: usize,
    pub timeout_scenarios: usize,
    pub error_scenarios: usize,
    pub results: Vec<TclHarnessScenarioResult>,
}

/// Build the canonical TCL suite definition for `bd-3plop.7`.
#[must_use]
pub fn canonical_tcl_harness_suite() -> TclHarnessSuite {
    TclHarnessSuite {
        bead_id: BEAD_ID.to_owned(),
        schema_version: SUITE_SCHEMA_VERSION,
        log_standard_ref: LOG_STANDARD_REF.to_owned(),
        runner_rel_path: DEFAULT_TCL_RUNNER_REL_PATH.to_owned(),
        scenarios: vec![
            TclHarnessScenario {
                id: "release_quick".to_owned(),
                description: "Run release permutation on quick.test smoke script".to_owned(),
                args: vec!["release".to_owned(), "quick.test".to_owned()],
            },
            TclHarnessScenario {
                id: "release_subset".to_owned(),
                description: "Run release permutation on selected transaction tests".to_owned(),
                args: vec![
                    "release".to_owned(),
                    "trans*.test".to_owned(),
                    "savepoint*.test".to_owned(),
                ],
            },
        ],
    }
}

/// Build and validate canonical suite.
///
/// # Errors
///
/// Returns diagnostics when canonical suite invariants are violated.
pub fn build_validated_tcl_harness_suite() -> Result<TclHarnessSuite, String> {
    let suite = canonical_tcl_harness_suite();
    let diagnostics = validate_tcl_harness_suite(&suite);
    if diagnostics.is_empty() {
        return Ok(suite);
    }
    Err(format!(
        "tcl_harness_suite_invalid diagnostics={diagnostics:?}"
    ))
}

/// Validate a suite and return deterministic diagnostics.
#[must_use]
pub fn validate_tcl_harness_suite(suite: &TclHarnessSuite) -> Vec<String> {
    let mut diagnostics = Vec::new();

    if suite.bead_id != BEAD_ID {
        diagnostics.push(format!(
            "bead_id_mismatch expected={BEAD_ID} actual={}",
            suite.bead_id
        ));
    }
    if suite.schema_version != SUITE_SCHEMA_VERSION {
        diagnostics.push(format!(
            "schema_version_mismatch expected={SUITE_SCHEMA_VERSION} actual={}",
            suite.schema_version
        ));
    }
    if suite.log_standard_ref != LOG_STANDARD_REF {
        diagnostics.push(format!(
            "log_standard_ref_mismatch expected={LOG_STANDARD_REF} actual={}",
            suite.log_standard_ref
        ));
    }
    if suite.runner_rel_path.trim().is_empty() {
        diagnostics.push("runner_rel_path_empty".to_owned());
    }
    if suite.scenarios.is_empty() {
        diagnostics.push("scenario_set_empty".to_owned());
    }

    let mut scenario_ids = BTreeSet::new();
    for scenario in &suite.scenarios {
        if scenario.id.trim().is_empty() {
            diagnostics.push("scenario_id_empty".to_owned());
        }
        if scenario.description.trim().is_empty() {
            diagnostics.push(format!("scenario_description_empty id={}", scenario.id));
        }
        if scenario.args.is_empty() {
            diagnostics.push(format!("scenario_args_empty id={}", scenario.id));
        }
        if !scenario_ids.insert(scenario.id.clone()) {
            diagnostics.push(format!("scenario_id_duplicate id={}", scenario.id));
        }
    }

    diagnostics
}

/// Parse summary counters from `testrunner.tcl` output.
#[must_use]
pub fn parse_testrunner_counts(output: &str) -> Option<TclRunnerCounts> {
    let mut final_errors: Option<u64> = None;
    let mut final_tests: Option<u64> = None;
    let mut skipped_jobs: u64 = 0;

    for line in output.lines() {
        if let Some((errors, tests)) = parse_errors_out_of_tests_line(line) {
            final_errors = Some(errors);
            final_tests = Some(tests);
        }
        if let Some(skipped) = parse_skipped_jobs_line(line) {
            skipped_jobs = skipped;
        }
    }

    match (final_errors, final_tests) {
        (Some(errors), Some(tests)) => Some(TclRunnerCounts {
            errors,
            tests,
            skipped_jobs,
        }),
        _ => None,
    }
}

/// Execute a suite in dry-run or execute mode.
///
/// # Errors
///
/// Returns an error when output directories cannot be created or when
/// command-orchestration state cannot be initialized.
pub fn execute_tcl_harness_suite(
    suite: &TclHarnessSuite,
    options: TclExecutionOptions,
) -> Result<TclHarnessExecutionSummary, String> {
    let diagnostics = validate_tcl_harness_suite(suite);
    if !diagnostics.is_empty() {
        return Err(format!("invalid_suite diagnostics={diagnostics:?}"));
    }

    let workspace_root = workspace_root()?;
    let sqlite_root = workspace_root.join(DEFAULT_SQLITE_ROOT_REL_PATH);
    let runner_path = options
        .runner_override
        .unwrap_or_else(|| workspace_root.join(&suite.runner_rel_path));

    let run_id = options.run_id_override.unwrap_or_else(generate_run_id);
    let output_dir = workspace_root.join("test-results").join("bd_3plop_7");
    let run_dir = output_dir.join(&run_id);
    fs::create_dir_all(&run_dir).map_err(|error| {
        format!(
            "run_dir_create_failed path={} error={error}",
            run_dir.display()
        )
    })?;

    let scenario_limit = options.max_scenarios.unwrap_or(suite.scenarios.len());
    let scenario_count = scenario_limit.min(suite.scenarios.len());

    let mut results = Vec::with_capacity(scenario_count);
    for scenario in suite.scenarios.iter().take(scenario_count) {
        let log_path = run_dir.join(format!("{}.log", scenario.id));
        let command = format_command(&runner_path, &scenario.args);
        let result = match options.mode {
            TclExecutionMode::DryRun => TclHarnessScenarioResult {
                scenario_id: scenario.id.clone(),
                command,
                outcome: TclHarnessOutcome::Skipped,
                reason: Some("dry_run_mode".to_owned()),
                exit_code: None,
                tests: 0,
                errors: 0,
                skipped_jobs: 0,
                elapsed_ms: 0,
                log_path: path_to_string(&log_path),
            },
            TclExecutionMode::Execute => execute_scenario(
                scenario,
                &runner_path,
                &sqlite_root,
                options.timeout_secs,
                &log_path,
            )?,
        };
        results.push(result);
    }

    let mut passed_scenarios = 0usize;
    let mut failed_scenarios = 0usize;
    let mut skipped_scenarios = 0usize;
    let mut timeout_scenarios = 0usize;
    let mut error_scenarios = 0usize;

    for result in &results {
        match result.outcome {
            TclHarnessOutcome::Pass => passed_scenarios += 1,
            TclHarnessOutcome::Fail => failed_scenarios += 1,
            TclHarnessOutcome::Skipped => skipped_scenarios += 1,
            TclHarnessOutcome::Timeout => timeout_scenarios += 1,
            TclHarnessOutcome::Error => error_scenarios += 1,
        }
    }

    Ok(TclHarnessExecutionSummary {
        bead_id: BEAD_ID.to_owned(),
        schema_version: EXECUTION_SCHEMA_VERSION,
        log_standard_ref: LOG_STANDARD_REF.to_owned(),
        run_id,
        mode: options.mode,
        timeout_secs: options.timeout_secs,
        runner_path: path_to_string(&runner_path),
        total_scenarios: results.len(),
        passed_scenarios,
        failed_scenarios,
        skipped_scenarios,
        timeout_scenarios,
        error_scenarios,
        results,
    })
}

/// Write suite JSON payload.
///
/// # Errors
///
/// Returns an error when serialization or file writing fails.
pub fn write_tcl_suite_json(path: &Path, suite: &TclHarnessSuite) -> Result<(), String> {
    let payload = serde_json::to_string_pretty(suite)
        .map_err(|error| format!("tcl_suite_serialize_failed: {error}"))?;
    fs::write(path, payload).map_err(|error| {
        format!(
            "tcl_suite_write_failed path={} error={error}",
            path.display()
        )
    })
}

/// Write execution summary JSON payload.
///
/// # Errors
///
/// Returns an error when serialization or file writing fails.
pub fn write_tcl_execution_summary_json(
    path: &Path,
    summary: &TclHarnessExecutionSummary,
) -> Result<(), String> {
    let payload = serde_json::to_string_pretty(summary)
        .map_err(|error| format!("tcl_execution_summary_serialize_failed: {error}"))?;
    fs::write(path, payload).map_err(|error| {
        format!(
            "tcl_execution_summary_write_failed path={} error={error}",
            path.display()
        )
    })
}

fn workspace_root() -> Result<PathBuf, String> {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .map_err(|error| format!("workspace_root_canonicalize_failed: {error}"))
}

fn generate_run_id() -> String {
    let timestamp = match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(duration) => duration.as_secs(),
        Err(_) => 0,
    };
    format!("bd-3plop-7-{timestamp}")
}

fn path_to_string(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

fn format_command(runner_path: &Path, args: &[String]) -> String {
    let mut rendered = String::from("tclsh ");
    rendered.push_str(&shell_escape(&path_to_string(runner_path)));
    for arg in args {
        rendered.push(' ');
        rendered.push_str(&shell_escape(arg));
    }
    rendered
}

fn shell_escape(raw: &str) -> String {
    if raw
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || b"/._-*=+".contains(&byte))
    {
        return raw.to_owned();
    }
    let escaped = raw.replace('\'', "'\"'\"'");
    format!("'{escaped}'")
}

fn parse_errors_out_of_tests_line(line: &str) -> Option<(u64, u64)> {
    let marker = " errors out of ";
    let tests_marker = " tests";
    let marker_pos = line.find(marker)?;
    let before = line[..marker_pos].trim();
    let after_marker = &line[marker_pos + marker.len()..];
    let tests_pos = after_marker.find(tests_marker)?;
    let tests_token = after_marker[..tests_pos].trim();

    let errors = before.split_whitespace().last()?.parse::<u64>().ok()?;
    let tests = tests_token.split_whitespace().last()?.parse::<u64>().ok()?;
    Some((errors, tests))
}

fn parse_skipped_jobs_line(line: &str) -> Option<u64> {
    if !line.contains("jobs skipped due to prior failures") {
        return None;
    }
    line.split_whitespace().next()?.parse::<u64>().ok()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SpawnScenarioError {
    TclshNotFound,
    Io,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ScenarioProcessStatus {
    exit_code: Option<i32>,
    timed_out: bool,
    elapsed_ms: u64,
}

fn execute_scenario(
    scenario: &TclHarnessScenario,
    runner_path: &Path,
    sqlite_root: &Path,
    timeout_secs: u64,
    log_path: &Path,
) -> Result<TclHarnessScenarioResult, String> {
    let command = format_command(runner_path, &scenario.args);
    if !runner_path.is_file() {
        return Ok(build_scenario_result(
            scenario,
            &command,
            TclHarnessOutcome::Skipped,
            Some(format!("runner_not_found path={}", runner_path.display())),
            None,
            None,
            0,
            log_path,
        ));
    }
    if !sqlite_root.is_dir() {
        return Ok(build_scenario_result(
            scenario,
            &command,
            TclHarnessOutcome::Skipped,
            Some(format!(
                "sqlite_root_not_found path={}",
                sqlite_root.display()
            )),
            None,
            None,
            0,
            log_path,
        ));
    }

    let mut child = match spawn_tclsh_process(scenario, runner_path, sqlite_root, log_path) {
        Ok(child) => child,
        Err(SpawnScenarioError::TclshNotFound) => {
            return Ok(build_scenario_result(
                scenario,
                &command,
                TclHarnessOutcome::Skipped,
                Some("tclsh_not_found".to_owned()),
                None,
                None,
                0,
                log_path,
            ));
        }
        Err(SpawnScenarioError::Io) => {
            return Ok(build_scenario_result(
                scenario,
                &command,
                TclHarnessOutcome::Error,
                Some("spawn_failed".to_owned()),
                None,
                None,
                0,
                log_path,
            ));
        }
    };

    let process_status = match wait_for_child(&mut child, timeout_secs, &scenario.id) {
        Ok(status) => status,
        Err(error) => {
            return Ok(build_scenario_result(
                scenario,
                &command,
                TclHarnessOutcome::Error,
                Some(format!("wait_failed error={error}")),
                None,
                None,
                0,
                log_path,
            ));
        }
    };

    let log_text = fs::read_to_string(log_path).unwrap_or_default();
    let parsed_counts = parse_testrunner_counts(&log_text);
    let (outcome, reason) = classify_process_result(
        process_status.timed_out,
        process_status.exit_code,
        parsed_counts,
    );

    Ok(build_scenario_result(
        scenario,
        &command,
        outcome,
        reason,
        process_status.exit_code,
        parsed_counts,
        process_status.elapsed_ms,
        log_path,
    ))
}

fn spawn_tclsh_process(
    scenario: &TclHarnessScenario,
    runner_path: &Path,
    sqlite_root: &Path,
    log_path: &Path,
) -> Result<std::process::Child, SpawnScenarioError> {
    let stdout_file = File::create(log_path).map_err(|_| SpawnScenarioError::Io)?;
    let stderr_file = stdout_file
        .try_clone()
        .map_err(|_| SpawnScenarioError::Io)?;
    Command::new("tclsh")
        .arg(runner_path)
        .args(&scenario.args)
        .current_dir(sqlite_root)
        .stdout(Stdio::from(stdout_file))
        .stderr(Stdio::from(stderr_file))
        .spawn()
        .map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                SpawnScenarioError::TclshNotFound
            } else {
                SpawnScenarioError::Io
            }
        })
}

fn wait_for_child(
    child: &mut std::process::Child,
    timeout_secs: u64,
    scenario_id: &str,
) -> Result<ScenarioProcessStatus, String> {
    let started_at = Instant::now();
    let timeout = Duration::from_secs(timeout_secs.max(1));
    let poll_interval = Duration::from_millis(DEFAULT_POLL_INTERVAL_MILLIS);

    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                return Ok(ScenarioProcessStatus {
                    exit_code: status.code(),
                    timed_out: false,
                    elapsed_ms: elapsed_ms_u64(started_at.elapsed()),
                });
            }
            Ok(None) => {
                if started_at.elapsed() >= timeout {
                    let _ = child.kill();
                    let status = child.wait().map_err(|error| {
                        format!("scenario_wait_after_kill_failed id={scenario_id} error={error}")
                    })?;
                    return Ok(ScenarioProcessStatus {
                        exit_code: status.code(),
                        timed_out: true,
                        elapsed_ms: elapsed_ms_u64(started_at.elapsed()),
                    });
                }
                thread::sleep(poll_interval);
            }
            Err(error) => {
                return Err(format!(
                    "scenario_try_wait_failed id={scenario_id} error={error}"
                ))
            }
        }
    }
}

fn classify_process_result(
    timed_out: bool,
    exit_code: Option<i32>,
    parsed_counts: Option<TclRunnerCounts>,
) -> (TclHarnessOutcome, Option<String>) {
    if timed_out {
        return (
            TclHarnessOutcome::Timeout,
            Some("timeout_exceeded".to_owned()),
        );
    }
    if let Some(counts) = parsed_counts {
        if counts.errors == 0 {
            return (TclHarnessOutcome::Pass, None);
        }
        return (
            TclHarnessOutcome::Fail,
            Some(format!(
                "testrunner_reported_errors errors={} tests={}",
                counts.errors, counts.tests
            )),
        );
    }
    if exit_code == Some(0) {
        return (
            TclHarnessOutcome::Error,
            Some("summary_not_found".to_owned()),
        );
    }
    (
        TclHarnessOutcome::Fail,
        Some("non_zero_exit_without_summary".to_owned()),
    )
}

fn build_scenario_result(
    scenario: &TclHarnessScenario,
    command: &str,
    outcome: TclHarnessOutcome,
    reason: Option<String>,
    exit_code: Option<i32>,
    parsed_counts: Option<TclRunnerCounts>,
    elapsed_ms: u64,
    log_path: &Path,
) -> TclHarnessScenarioResult {
    let counts = parsed_counts.unwrap_or(TclRunnerCounts {
        errors: 0,
        tests: 0,
        skipped_jobs: 0,
    });
    TclHarnessScenarioResult {
        scenario_id: scenario.id.clone(),
        command: command.to_owned(),
        outcome,
        reason,
        exit_code,
        tests: counts.tests,
        errors: counts.errors,
        skipped_jobs: counts.skipped_jobs,
        elapsed_ms,
        log_path: path_to_string(log_path),
    }
}

fn elapsed_ms_u64(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}
