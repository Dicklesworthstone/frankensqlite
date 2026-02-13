//! Cross-platform durability matrix specification and environment contracts.
//!
//! Bead: bd-mblr.7.4
//!
//! This module defines a deterministic durability matrix connecting:
//! - environment contracts (`os`, `filesystem`, toolchain),
//! - crash/recovery scenarios,
//! - probe definitions used by CI/workflows for parity and drift detection.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::io;
use std::path::Path;
use std::process::Command;
use std::time::Instant;

use serde::{Deserialize, Serialize};

/// Bead identifier for evidence/log correlation.
pub const BEAD_ID: &str = "bd-mblr.7.4";
/// Serialization schema version for `DurabilityMatrix`.
pub const MATRIX_SCHEMA_VERSION: u32 = 1;
/// Default deterministic root seed for scenario seed derivation.
pub const DEFAULT_ROOT_SEED: u64 = 0xB740_0000_0000_0001;
/// Canonical logging/reference standard.
pub const LOG_STANDARD_REF: &str = "bd-1fpm";
/// Serialization schema version for `DurabilityExecutionSummary`.
pub const EXECUTION_SCHEMA_VERSION: u32 = 1;
/// Default timeout for probe execution mode.
pub const DEFAULT_EXECUTION_TIMEOUT_SECS: u64 = 1_800;

/// Operating system family for an environment contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum OperatingSystem {
    Linux,
    MacOs,
    Windows,
    FreeBsd,
}

/// Filesystem class relevant to durability semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum FilesystemClass {
    Ext4Ordered,
    XfsBarrier,
    Apfs,
    Ntfs,
    Zfs,
}

/// Toolchain variant used when executing durability probes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum ToolchainVariant {
    Nightly,
    Stable,
}

/// Crash pattern represented by a scenario.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum CrashMode {
    MidCommit,
    PostCommitPreCheckpoint,
    DuringCheckpoint,
    CorruptionInjection,
}

/// Validation lane expected to run for a probe.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum DurabilityLane {
    RecoveryReplay,
    CorruptionRecovery,
    CheckpointParity,
    FullSuiteFallback,
}

/// Deterministic contract for a single execution environment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnvironmentContract {
    pub id: String,
    pub os: OperatingSystem,
    pub filesystem: FilesystemClass,
    pub toolchain: ToolchainVariant,
    pub requires_atomic_rename: bool,
    pub requires_fsync_durability: bool,
    pub notes: String,
}

/// One durability scenario executed across one or more environments.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DurabilityScenario {
    pub id: String,
    pub crash_mode: CrashMode,
    pub command: String,
    pub scenario_ids: Vec<String>,
    pub invariants: Vec<String>,
    pub seed_offset: u64,
}

impl DurabilityScenario {
    /// Deterministically derive this scenario seed from matrix root seed.
    #[must_use]
    pub fn derived_seed(&self, root_seed: u64) -> u64 {
        root_seed.wrapping_add(self.seed_offset)
    }
}

/// Concrete probe linking an environment to a scenario.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DurabilityProbe {
    pub id: String,
    pub environment_id: String,
    pub scenario_id: String,
    pub required_lanes: Vec<DurabilityLane>,
}

/// Canonical durability matrix used by durability-gate workflows.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DurabilityMatrix {
    pub bead_id: String,
    pub schema_version: u32,
    pub root_seed: u64,
    pub log_standard_ref: String,
    pub environments: Vec<EnvironmentContract>,
    pub scenarios: Vec<DurabilityScenario>,
    pub probes: Vec<DurabilityProbe>,
}

/// Execution mode for durability probes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DurabilityExecutionMode {
    DryRun,
    Execute,
}

/// Probe outcome classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DurabilityProbeOutcome {
    Pass,
    Fail,
    Timeout,
    Error,
    Skipped,
}

/// Runtime options for executing durability probes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DurabilityExecutionOptions {
    pub mode: DurabilityExecutionMode,
    pub timeout_secs: u64,
    pub max_probes: Option<usize>,
}

impl Default for DurabilityExecutionOptions {
    fn default() -> Self {
        Self {
            mode: DurabilityExecutionMode::DryRun,
            timeout_secs: DEFAULT_EXECUTION_TIMEOUT_SECS,
            max_probes: None,
        }
    }
}

/// Per-probe execution result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DurabilityProbeExecution {
    pub probe_id: String,
    pub environment_id: String,
    pub scenario_id: String,
    pub seed: u64,
    pub command: String,
    pub outcome: DurabilityProbeOutcome,
    pub reason: Option<String>,
    pub exit_code: Option<i32>,
    pub elapsed_ms: u64,
}

/// Aggregated summary for a matrix execution pass.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DurabilityExecutionSummary {
    pub bead_id: String,
    pub schema_version: u32,
    pub root_seed: u64,
    pub host_os: String,
    pub mode: DurabilityExecutionMode,
    pub timeout_secs: u64,
    pub total_probes: usize,
    pub passed_probes: usize,
    pub failed_probes: usize,
    pub timeout_probes: usize,
    pub error_probes: usize,
    pub skipped_probes: usize,
    pub results: Vec<DurabilityProbeExecution>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CommandExecution {
    exit_code: Option<i32>,
    timed_out: bool,
    succeeded: bool,
    elapsed_ms: u64,
}

impl DurabilityMatrix {
    /// Build canonical matrix for a given deterministic root seed.
    #[must_use]
    pub fn canonical(root_seed: u64) -> Self {
        build_durability_matrix(root_seed)
    }

    /// Validate this matrix, returning all diagnostics.
    #[must_use]
    pub fn validate(&self) -> Vec<String> {
        validate_durability_matrix(self)
    }

    /// Return probes targeting a specific environment id.
    #[must_use]
    pub fn probes_for_environment(&self, environment_id: &str) -> Vec<&DurabilityProbe> {
        self.probes
            .iter()
            .filter(|probe| probe.environment_id == environment_id)
            .collect()
    }

    /// Return probes targeting a specific scenario id.
    #[must_use]
    pub fn probes_for_scenario(&self, scenario_id: &str) -> Vec<&DurabilityProbe> {
        self.probes
            .iter()
            .filter(|probe| probe.scenario_id == scenario_id)
            .collect()
    }

    /// Serialize the matrix in a deterministic pretty JSON format.
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }
}

/// Write matrix JSON to a file path.
pub fn write_matrix_json(path: &Path, matrix: &DurabilityMatrix) -> Result<(), String> {
    let payload = serde_json::to_string_pretty(matrix)
        .map_err(|error| format!("durability_matrix_serialize_failed: {error}"))?;
    std::fs::write(path, payload).map_err(|error| {
        format!(
            "durability_matrix_write_failed path={} error={error}",
            path.display()
        )
    })
}

/// Write execution summary JSON to a file path.
///
/// # Errors
///
/// Returns an error when serialization fails or when the destination file
/// cannot be written.
pub fn write_execution_summary_json(
    path: &Path,
    summary: &DurabilityExecutionSummary,
) -> Result<(), String> {
    let payload = serde_json::to_string_pretty(summary)
        .map_err(|error| format!("durability_execution_serialize_failed: {error}"))?;
    std::fs::write(path, payload).map_err(|error| {
        format!(
            "durability_execution_write_failed path={} error={error}",
            path.display()
        )
    })
}

/// Execute durability probes and return a normalized execution summary.
///
/// # Errors
///
/// Returns an error when matrix validation fails.
pub fn execute_durability_matrix(
    matrix: &DurabilityMatrix,
    options: DurabilityExecutionOptions,
) -> Result<DurabilityExecutionSummary, String> {
    execute_durability_matrix_with(matrix, options, host_operating_system(), run_probe_command)
}

#[allow(clippy::too_many_lines)]
fn execute_durability_matrix_with<F>(
    matrix: &DurabilityMatrix,
    options: DurabilityExecutionOptions,
    host_os: Option<OperatingSystem>,
    runner: F,
) -> Result<DurabilityExecutionSummary, String>
where
    F: Fn(&str, u64, &[(String, String)]) -> Result<CommandExecution, String>,
{
    let diagnostics = matrix.validate();
    if !diagnostics.is_empty() {
        return Err(format!(
            "durability_matrix_validation_failed: {}",
            diagnostics.join("; ")
        ));
    }

    let environment_index: BTreeMap<&str, &EnvironmentContract> = matrix
        .environments
        .iter()
        .map(|environment| (environment.id.as_str(), environment))
        .collect();
    let scenario_index: BTreeMap<&str, &DurabilityScenario> = matrix
        .scenarios
        .iter()
        .map(|scenario| (scenario.id.as_str(), scenario))
        .collect();

    let mut probes: Vec<&DurabilityProbe> = matrix.probes.iter().collect();
    probes.sort_by(|left, right| left.id.cmp(&right.id));
    if let Some(max_probes) = options.max_probes {
        probes.truncate(max_probes);
    }

    let timeout_secs = options.timeout_secs.max(1);
    let host_label = format_host_os(host_os);
    let mut results = Vec::with_capacity(probes.len());

    for probe in probes {
        let Some(environment) = environment_index
            .get(probe.environment_id.as_str())
            .copied()
        else {
            results.push(DurabilityProbeExecution {
                probe_id: probe.id.clone(),
                environment_id: probe.environment_id.clone(),
                scenario_id: probe.scenario_id.clone(),
                seed: matrix.root_seed,
                command: String::new(),
                outcome: DurabilityProbeOutcome::Error,
                reason: Some(format!(
                    "unknown_environment_contract id={}",
                    probe.environment_id
                )),
                exit_code: None,
                elapsed_ms: 0,
            });
            continue;
        };
        let Some(scenario) = scenario_index.get(probe.scenario_id.as_str()).copied() else {
            results.push(DurabilityProbeExecution {
                probe_id: probe.id.clone(),
                environment_id: probe.environment_id.clone(),
                scenario_id: probe.scenario_id.clone(),
                seed: matrix.root_seed,
                command: String::new(),
                outcome: DurabilityProbeOutcome::Error,
                reason: Some(format!(
                    "unknown_scenario_contract id={}",
                    probe.scenario_id
                )),
                exit_code: None,
                elapsed_ms: 0,
            });
            continue;
        };

        let scenario_seed = scenario.derived_seed(matrix.root_seed);
        let command = scenario.command.clone();
        if !host_matches_environment(host_os, environment) {
            results.push(DurabilityProbeExecution {
                probe_id: probe.id.clone(),
                environment_id: environment.id.clone(),
                scenario_id: scenario.id.clone(),
                seed: scenario_seed,
                command,
                outcome: DurabilityProbeOutcome::Skipped,
                reason: Some(format!(
                    "host_os_mismatch host={} target={:?}",
                    host_label, environment.os
                )),
                exit_code: None,
                elapsed_ms: 0,
            });
            continue;
        }

        if matches!(options.mode, DurabilityExecutionMode::DryRun) {
            results.push(DurabilityProbeExecution {
                probe_id: probe.id.clone(),
                environment_id: environment.id.clone(),
                scenario_id: scenario.id.clone(),
                seed: scenario_seed,
                command,
                outcome: DurabilityProbeOutcome::Skipped,
                reason: Some("dry_run_mode".to_owned()),
                exit_code: None,
                elapsed_ms: 0,
            });
            continue;
        }

        let env_values = vec![
            ("FSQLITE_DURABILITY_PROBE_ID".to_owned(), probe.id.clone()),
            (
                "FSQLITE_DURABILITY_ENVIRONMENT_ID".to_owned(),
                environment.id.clone(),
            ),
            (
                "FSQLITE_DURABILITY_SCENARIO_ID".to_owned(),
                scenario.id.clone(),
            ),
            (
                "FSQLITE_DURABILITY_SEED".to_owned(),
                scenario_seed.to_string(),
            ),
        ];

        match runner(scenario.command.as_str(), timeout_secs, &env_values) {
            Ok(run) if run.succeeded => results.push(DurabilityProbeExecution {
                probe_id: probe.id.clone(),
                environment_id: environment.id.clone(),
                scenario_id: scenario.id.clone(),
                seed: scenario_seed,
                command,
                outcome: DurabilityProbeOutcome::Pass,
                reason: None,
                exit_code: run.exit_code,
                elapsed_ms: run.elapsed_ms,
            }),
            Ok(run) if run.timed_out => results.push(DurabilityProbeExecution {
                probe_id: probe.id.clone(),
                environment_id: environment.id.clone(),
                scenario_id: scenario.id.clone(),
                seed: scenario_seed,
                command,
                outcome: DurabilityProbeOutcome::Timeout,
                reason: Some(format!("timeout_secs_exceeded={timeout_secs}")),
                exit_code: run.exit_code,
                elapsed_ms: run.elapsed_ms,
            }),
            Ok(run) => results.push(DurabilityProbeExecution {
                probe_id: probe.id.clone(),
                environment_id: environment.id.clone(),
                scenario_id: scenario.id.clone(),
                seed: scenario_seed,
                command,
                outcome: DurabilityProbeOutcome::Fail,
                reason: Some(format_exit_reason(run.exit_code)),
                exit_code: run.exit_code,
                elapsed_ms: run.elapsed_ms,
            }),
            Err(error) => results.push(DurabilityProbeExecution {
                probe_id: probe.id.clone(),
                environment_id: environment.id.clone(),
                scenario_id: scenario.id.clone(),
                seed: scenario_seed,
                command,
                outcome: DurabilityProbeOutcome::Error,
                reason: Some(error),
                exit_code: None,
                elapsed_ms: 0,
            }),
        }
    }

    let mut passed_probes = 0usize;
    let mut failed_probes = 0usize;
    let mut timeout_probes = 0usize;
    let mut error_probes = 0usize;
    let mut skipped_probes = 0usize;
    for result in &results {
        match result.outcome {
            DurabilityProbeOutcome::Pass => passed_probes += 1,
            DurabilityProbeOutcome::Fail => failed_probes += 1,
            DurabilityProbeOutcome::Timeout => timeout_probes += 1,
            DurabilityProbeOutcome::Error => error_probes += 1,
            DurabilityProbeOutcome::Skipped => skipped_probes += 1,
        }
    }

    Ok(DurabilityExecutionSummary {
        bead_id: BEAD_ID.to_owned(),
        schema_version: EXECUTION_SCHEMA_VERSION,
        root_seed: matrix.root_seed,
        host_os: host_label,
        mode: options.mode,
        timeout_secs,
        total_probes: results.len(),
        passed_probes,
        failed_probes,
        timeout_probes,
        error_probes,
        skipped_probes,
        results,
    })
}

fn format_exit_reason(exit_code: Option<i32>) -> String {
    match exit_code {
        Some(code) => format!("exit_code={code}"),
        None => "process_terminated_by_signal".to_owned(),
    }
}

fn host_matches_environment(
    host_os: Option<OperatingSystem>,
    environment: &EnvironmentContract,
) -> bool {
    matches!(host_os, Some(current) if current == environment.os)
}

fn format_host_os(host_os: Option<OperatingSystem>) -> String {
    match host_os {
        Some(OperatingSystem::Linux) => "linux".to_owned(),
        Some(OperatingSystem::MacOs) => "macos".to_owned(),
        Some(OperatingSystem::Windows) => "windows".to_owned(),
        Some(OperatingSystem::FreeBsd) => "freebsd".to_owned(),
        None => "unknown".to_owned(),
    }
}

fn host_operating_system() -> Option<OperatingSystem> {
    if cfg!(target_os = "linux") {
        Some(OperatingSystem::Linux)
    } else if cfg!(target_os = "macos") {
        Some(OperatingSystem::MacOs)
    } else if cfg!(target_os = "windows") {
        Some(OperatingSystem::Windows)
    } else if cfg!(target_os = "freebsd") {
        Some(OperatingSystem::FreeBsd)
    } else {
        None
    }
}

fn apply_env(command: &mut Command, env_values: &[(String, String)]) {
    for (key, value) in env_values {
        command.env(key, value);
    }
}

#[cfg(target_os = "windows")]
fn shell_command(command: &str) -> Command {
    let mut shell = Command::new("cmd");
    shell.arg("/C").arg(command);
    shell
}

#[cfg(not(target_os = "windows"))]
fn shell_command(command: &str) -> Command {
    let mut shell = Command::new("bash");
    shell.arg("-lc").arg(command);
    shell
}

fn run_probe_command(
    command: &str,
    timeout_secs: u64,
    env_values: &[(String, String)],
) -> Result<CommandExecution, String> {
    let start = Instant::now();

    #[cfg(target_os = "windows")]
    let output = {
        let mut shell = shell_command(command);
        apply_env(&mut shell, env_values);
        shell
            .output()
            .map_err(|error| format!("durability_probe_spawn_failed: {error}"))?
    };

    #[cfg(not(target_os = "windows"))]
    let output = {
        let mut timeout_cmd = Command::new("timeout");
        timeout_cmd
            .arg(format!("{}s", timeout_secs.max(1)))
            .arg("bash")
            .arg("-lc")
            .arg(command);
        apply_env(&mut timeout_cmd, env_values);

        match timeout_cmd.output() {
            Ok(output) => output,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                let mut fallback = shell_command(command);
                apply_env(&mut fallback, env_values);
                fallback.output().map_err(|fallback_error| {
                    format!("durability_probe_spawn_failed: {fallback_error}")
                })?
            }
            Err(error) => return Err(format!("durability_probe_spawn_failed: {error}")),
        }
    };

    let elapsed_ms = u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX);
    let exit_code = output.status.code();
    let timed_out = exit_code == Some(124);
    let succeeded = output.status.success() && !timed_out;

    Ok(CommandExecution {
        exit_code,
        timed_out,
        succeeded,
        elapsed_ms,
    })
}

/// Render an operator-friendly workflow from the matrix.
#[must_use]
pub fn render_operator_workflow(matrix: &DurabilityMatrix) -> String {
    let mut out = String::new();
    writeln!(
        out,
        "durability_matrix bead_id={} schema_version={} root_seed={} log_standard_ref={}",
        matrix.bead_id, matrix.schema_version, matrix.root_seed, matrix.log_standard_ref
    )
    .expect("writing to string cannot fail");
    writeln!(out, "environments:").expect("writing to string cannot fail");
    for environment in &matrix.environments {
        writeln!(
            out,
            "- id={} os={:?} fs={:?} toolchain={:?} atomic_rename={} fsync={} notes={}",
            environment.id,
            environment.os,
            environment.filesystem,
            environment.toolchain,
            environment.requires_atomic_rename,
            environment.requires_fsync_durability,
            environment.notes
        )
        .expect("writing to string cannot fail");
    }
    writeln!(out, "scenarios:").expect("writing to string cannot fail");
    for scenario in &matrix.scenarios {
        writeln!(
            out,
            "- id={} crash_mode={:?} seed={} command={} scenario_ids={} invariants={}",
            scenario.id,
            scenario.crash_mode,
            scenario.derived_seed(matrix.root_seed),
            scenario.command,
            scenario.scenario_ids.join(","),
            scenario.invariants.join(",")
        )
        .expect("writing to string cannot fail");
    }
    writeln!(out, "probes:").expect("writing to string cannot fail");
    for probe in &matrix.probes {
        let lanes = probe
            .required_lanes
            .iter()
            .map(|lane| format!("{lane:?}"))
            .collect::<Vec<_>>()
            .join(",");
        writeln!(
            out,
            "- id={} environment={} scenario={} lanes={}",
            probe.id, probe.environment_id, probe.scenario_id, lanes
        )
        .expect("writing to string cannot fail");
    }
    out
}

/// Build and validate the canonical matrix.
pub fn build_validated_durability_matrix(root_seed: u64) -> Result<DurabilityMatrix, String> {
    let matrix = build_durability_matrix(root_seed);
    let diagnostics = validate_durability_matrix(&matrix);
    if diagnostics.is_empty() {
        Ok(matrix)
    } else {
        Err(diagnostics.join("; "))
    }
}

/// Build the canonical durability matrix.
#[must_use]
pub fn build_durability_matrix(root_seed: u64) -> DurabilityMatrix {
    let mut environments = canonical_environments();
    environments.sort_by(|left, right| left.id.cmp(&right.id));

    let mut scenarios = canonical_scenarios();
    scenarios.sort_by(|left, right| left.id.cmp(&right.id));

    let mut probes = Vec::with_capacity(environments.len() * scenarios.len());
    for environment in &environments {
        for scenario in &scenarios {
            probes.push(DurabilityProbe {
                id: format!("probe-{}-{}", environment.id, scenario.id.to_lowercase()),
                environment_id: environment.id.clone(),
                scenario_id: scenario.id.clone(),
                required_lanes: lanes_for_scenario(&scenario.id),
            });
        }
    }
    probes.sort_by(|left, right| left.id.cmp(&right.id));

    DurabilityMatrix {
        bead_id: BEAD_ID.to_owned(),
        schema_version: MATRIX_SCHEMA_VERSION,
        root_seed,
        log_standard_ref: LOG_STANDARD_REF.to_owned(),
        environments,
        scenarios,
        probes,
    }
}

/// Validate durability matrix consistency and minimum coverage constraints.
#[must_use]
pub fn validate_durability_matrix(matrix: &DurabilityMatrix) -> Vec<String> {
    let mut diagnostics = Vec::new();

    if matrix.bead_id != BEAD_ID {
        diagnostics.push(format!(
            "unexpected bead_id: {} (expected {BEAD_ID})",
            matrix.bead_id
        ));
    }
    if matrix.schema_version != MATRIX_SCHEMA_VERSION {
        diagnostics.push(format!(
            "unexpected schema_version: {} (expected {MATRIX_SCHEMA_VERSION})",
            matrix.schema_version
        ));
    }
    if matrix.log_standard_ref != LOG_STANDARD_REF {
        diagnostics.push(format!(
            "unexpected log_standard_ref: {} (expected {LOG_STANDARD_REF})",
            matrix.log_standard_ref
        ));
    }

    if matrix.environments.is_empty() {
        diagnostics.push("matrix has no environments".to_owned());
    }
    if matrix.scenarios.is_empty() {
        diagnostics.push("matrix has no scenarios".to_owned());
    }
    if matrix.probes.is_empty() {
        diagnostics.push("matrix has no probes".to_owned());
    }

    let environment_ids: BTreeSet<_> = matrix.environments.iter().map(|e| e.id.as_str()).collect();
    if environment_ids.len() != matrix.environments.len() {
        diagnostics.push("duplicate environment ids detected".to_owned());
    }

    let scenario_ids: BTreeSet<_> = matrix.scenarios.iter().map(|s| s.id.as_str()).collect();
    if scenario_ids.len() != matrix.scenarios.len() {
        diagnostics.push("duplicate scenario ids detected".to_owned());
    }

    let probe_ids: BTreeSet<_> = matrix.probes.iter().map(|p| p.id.as_str()).collect();
    if probe_ids.len() != matrix.probes.len() {
        diagnostics.push("duplicate probe ids detected".to_owned());
    }

    for probe in &matrix.probes {
        if !environment_ids.contains(probe.environment_id.as_str()) {
            diagnostics.push(format!(
                "probe {} references unknown environment_id {}",
                probe.id, probe.environment_id
            ));
        }
        if !scenario_ids.contains(probe.scenario_id.as_str()) {
            diagnostics.push(format!(
                "probe {} references unknown scenario_id {}",
                probe.id, probe.scenario_id
            ));
        }
        if probe.required_lanes.is_empty() {
            diagnostics.push(format!("probe {} has no required lanes", probe.id));
        }
    }

    let os_coverage: BTreeSet<_> = matrix.environments.iter().map(|e| e.os).collect();
    for required_os in [
        OperatingSystem::Linux,
        OperatingSystem::MacOs,
        OperatingSystem::Windows,
    ] {
        if !os_coverage.contains(&required_os) {
            diagnostics.push(format!("missing required OS coverage: {required_os:?}"));
        }
    }

    for scenario in &matrix.scenarios {
        if scenario.command.trim().is_empty() {
            diagnostics.push(format!("scenario {} has empty command", scenario.id));
        }
        if scenario.scenario_ids.is_empty() {
            diagnostics.push(format!("scenario {} has empty scenario_ids", scenario.id));
        }
        if scenario.invariants.is_empty() {
            diagnostics.push(format!("scenario {} has empty invariants", scenario.id));
        }
    }

    diagnostics
}

fn canonical_environments() -> Vec<EnvironmentContract> {
    vec![
        EnvironmentContract {
            id: "env-linux-ext4-nightly".to_owned(),
            os: OperatingSystem::Linux,
            filesystem: FilesystemClass::Ext4Ordered,
            toolchain: ToolchainVariant::Nightly,
            requires_atomic_rename: true,
            requires_fsync_durability: true,
            notes: "Primary CI lane baseline".to_owned(),
        },
        EnvironmentContract {
            id: "env-linux-xfs-nightly".to_owned(),
            os: OperatingSystem::Linux,
            filesystem: FilesystemClass::XfsBarrier,
            toolchain: ToolchainVariant::Nightly,
            requires_atomic_rename: true,
            requires_fsync_durability: true,
            notes: "Barrier-heavy metadata ordering behavior".to_owned(),
        },
        EnvironmentContract {
            id: "env-linux-ext4-stable".to_owned(),
            os: OperatingSystem::Linux,
            filesystem: FilesystemClass::Ext4Ordered,
            toolchain: ToolchainVariant::Stable,
            requires_atomic_rename: true,
            requires_fsync_durability: true,
            notes: "Regression guard against nightly drift".to_owned(),
        },
        EnvironmentContract {
            id: "env-macos-apfs-nightly".to_owned(),
            os: OperatingSystem::MacOs,
            filesystem: FilesystemClass::Apfs,
            toolchain: ToolchainVariant::Nightly,
            requires_atomic_rename: true,
            requires_fsync_durability: true,
            notes: "APFS crash-recovery ordering semantics".to_owned(),
        },
        EnvironmentContract {
            id: "env-windows-ntfs-nightly".to_owned(),
            os: OperatingSystem::Windows,
            filesystem: FilesystemClass::Ntfs,
            toolchain: ToolchainVariant::Nightly,
            requires_atomic_rename: true,
            requires_fsync_durability: true,
            notes: "NTFS rename + flush behavior".to_owned(),
        },
        EnvironmentContract {
            id: "env-freebsd-zfs-nightly".to_owned(),
            os: OperatingSystem::FreeBsd,
            filesystem: FilesystemClass::Zfs,
            toolchain: ToolchainVariant::Nightly,
            requires_atomic_rename: true,
            requires_fsync_durability: true,
            notes: "ZFS durability semantics and metadata checksums".to_owned(),
        },
    ]
}

fn canonical_scenarios() -> Vec<DurabilityScenario> {
    vec![
        DurabilityScenario {
            id: "REC-1".to_owned(),
            crash_mode: CrashMode::MidCommit,
            command: "cargo test -p fsqlite-e2e --test recovery_crash_wal_replay -- --nocapture"
                .to_owned(),
            scenario_ids: vec!["REC-1".to_owned(), "WAL-1".to_owned()],
            invariants: vec!["INV-5".to_owned(), "WAL-1".to_owned(), "PAGER-1".to_owned()],
            seed_offset: 0x101,
        },
        DurabilityScenario {
            id: "REC-2".to_owned(),
            crash_mode: CrashMode::CorruptionInjection,
            command: "cargo test -p fsqlite-e2e --test recovery_single_page -- --nocapture"
                .to_owned(),
            scenario_ids: vec!["REC-2".to_owned()],
            invariants: vec!["PAGER-1".to_owned(), "BTREE-1".to_owned()],
            seed_offset: 0x102,
        },
        DurabilityScenario {
            id: "REC-3".to_owned(),
            crash_mode: CrashMode::CorruptionInjection,
            command: "cargo test -p fsqlite-e2e --test recovery_wal_corruption -- --nocapture"
                .to_owned(),
            scenario_ids: vec!["REC-3".to_owned(), "CORRUPT-1".to_owned()],
            invariants: vec!["WAL-1".to_owned(), "INV-6".to_owned(), "INV-7".to_owned()],
            seed_offset: 0x103,
        },
        DurabilityScenario {
            id: "WAL-2".to_owned(),
            crash_mode: CrashMode::DuringCheckpoint,
            command: "cargo test -p fsqlite-e2e --test correctness_transactions -- --nocapture"
                .to_owned(),
            scenario_ids: vec!["WAL-2".to_owned(), "TXN-2".to_owned()],
            invariants: vec!["WAL-1".to_owned(), "INV-4".to_owned()],
            seed_offset: 0x104,
        },
    ]
}

fn lanes_for_scenario(scenario_id: &str) -> Vec<DurabilityLane> {
    match scenario_id {
        "REC-1" => vec![
            DurabilityLane::RecoveryReplay,
            DurabilityLane::CheckpointParity,
            DurabilityLane::FullSuiteFallback,
        ],
        "REC-2" | "REC-3" => vec![
            DurabilityLane::CorruptionRecovery,
            DurabilityLane::FullSuiteFallback,
        ],
        _ => vec![
            DurabilityLane::CheckpointParity,
            DurabilityLane::FullSuiteFallback,
        ],
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use tempfile::tempdir;

    use super::*;

    #[test]
    fn canonical_matrix_validates() {
        let matrix = DurabilityMatrix::canonical(DEFAULT_ROOT_SEED);
        let diagnostics = matrix.validate();
        assert!(
            diagnostics.is_empty(),
            "bead_id={BEAD_ID} expected valid matrix, got diagnostics: {diagnostics:?}"
        );
    }

    #[test]
    fn canonical_matrix_has_cross_platform_coverage() {
        let matrix = DurabilityMatrix::canonical(DEFAULT_ROOT_SEED);
        let os_coverage: BTreeSet<_> = matrix.environments.iter().map(|e| e.os).collect();
        assert!(os_coverage.contains(&OperatingSystem::Linux));
        assert!(os_coverage.contains(&OperatingSystem::MacOs));
        assert!(os_coverage.contains(&OperatingSystem::Windows));
    }

    #[test]
    fn probes_exist_for_every_environment_and_scenario() {
        let matrix = DurabilityMatrix::canonical(DEFAULT_ROOT_SEED);
        for environment in &matrix.environments {
            let probes = matrix.probes_for_environment(&environment.id);
            assert!(
                !probes.is_empty(),
                "environment {} should have at least one probe",
                environment.id
            );
        }
        for scenario in &matrix.scenarios {
            let probes = matrix.probes_for_scenario(&scenario.id);
            assert!(
                !probes.is_empty(),
                "scenario {} should have at least one probe",
                scenario.id
            );
        }
    }

    #[test]
    fn scenario_seed_derivation_is_deterministic() {
        let matrix = DurabilityMatrix::canonical(DEFAULT_ROOT_SEED);
        let first = matrix
            .scenarios
            .first()
            .expect("canonical matrix has at least one scenario");
        let seed_1 = first.derived_seed(matrix.root_seed);
        let seed_2 = first.derived_seed(matrix.root_seed);
        assert_eq!(seed_1, seed_2, "derived seeds must be deterministic");
    }

    #[test]
    fn json_roundtrip_preserves_shape() {
        let matrix = DurabilityMatrix::canonical(DEFAULT_ROOT_SEED);
        let json = matrix.to_json().expect("serialize matrix");
        let restored: DurabilityMatrix = serde_json::from_str(&json).expect("deserialize matrix");
        assert_eq!(restored.bead_id, matrix.bead_id);
        assert_eq!(restored.environments.len(), matrix.environments.len());
        assert_eq!(restored.scenarios.len(), matrix.scenarios.len());
        assert_eq!(restored.probes.len(), matrix.probes.len());
    }

    #[test]
    fn operator_workflow_includes_core_sections() {
        let matrix = DurabilityMatrix::canonical(DEFAULT_ROOT_SEED);
        let workflow = render_operator_workflow(&matrix);
        assert!(workflow.contains("durability_matrix bead_id=bd-mblr.7.4"));
        assert!(workflow.contains("environments:"));
        assert!(workflow.contains("scenarios:"));
        assert!(workflow.contains("probes:"));
        assert!(workflow.contains("id=env-linux-ext4-nightly"));
        assert!(workflow.contains("id=REC-1"));
    }

    #[test]
    fn write_matrix_json_roundtrip() {
        let matrix = DurabilityMatrix::canonical(DEFAULT_ROOT_SEED);
        let temp = tempdir().expect("create tempdir");
        let path = temp.path().join("durability_matrix.json");
        write_matrix_json(&path, &matrix).expect("write matrix json");
        let payload = std::fs::read_to_string(&path).expect("read matrix json");
        let restored: DurabilityMatrix =
            serde_json::from_str(&payload).expect("deserialize matrix json");
        assert_eq!(restored.bead_id, matrix.bead_id);
        assert_eq!(restored.schema_version, MATRIX_SCHEMA_VERSION);
    }

    #[test]
    fn dry_run_summary_skips_all_probes() {
        let matrix = DurabilityMatrix::canonical(DEFAULT_ROOT_SEED);
        let options = DurabilityExecutionOptions {
            mode: DurabilityExecutionMode::DryRun,
            timeout_secs: 30,
            max_probes: None,
        };

        let summary = execute_durability_matrix_with(
            &matrix,
            options,
            Some(OperatingSystem::Linux),
            |_, _, _| -> Result<CommandExecution, String> {
                panic!("runner should not be invoked in dry-run mode");
            },
        )
        .expect("build dry-run summary");

        assert_eq!(summary.total_probes, matrix.probes.len());
        assert_eq!(summary.skipped_probes, matrix.probes.len());
        assert_eq!(summary.passed_probes, 0);
        assert_eq!(summary.failed_probes, 0);
        assert_eq!(summary.timeout_probes, 0);
        assert_eq!(summary.error_probes, 0);

        assert!(
            summary
                .results
                .iter()
                .any(|result| result.reason.as_deref() == Some("dry_run_mode"))
        );
        assert!(summary.results.iter().any(|result| {
            result
                .reason
                .as_deref()
                .is_some_and(|reason| reason.contains("host_os_mismatch"))
        }));
    }

    #[test]
    fn execute_mode_runs_only_host_compatible_probes() {
        let matrix = DurabilityMatrix::canonical(DEFAULT_ROOT_SEED);
        let options = DurabilityExecutionOptions {
            mode: DurabilityExecutionMode::Execute,
            timeout_secs: 15,
            max_probes: None,
        };
        let call_count = Cell::new(0usize);

        let summary = execute_durability_matrix_with(
            &matrix,
            options,
            Some(OperatingSystem::Linux),
            |_, _, _| -> Result<CommandExecution, String> {
                call_count.set(call_count.get() + 1);
                Ok(CommandExecution {
                    exit_code: Some(0),
                    timed_out: false,
                    succeeded: true,
                    elapsed_ms: 7,
                })
            },
        )
        .expect("execute matrix");

        let expected_runs = matrix
            .environments
            .iter()
            .filter(|environment| environment.os == OperatingSystem::Linux)
            .count()
            * matrix.scenarios.len();
        assert_eq!(call_count.get(), expected_runs);
        assert_eq!(summary.passed_probes, expected_runs);
        assert_eq!(summary.skipped_probes, summary.total_probes - expected_runs);
    }

    #[test]
    fn execute_mode_classifies_timeout_fail_and_error() {
        let matrix = DurabilityMatrix::canonical(DEFAULT_ROOT_SEED);
        let options = DurabilityExecutionOptions {
            mode: DurabilityExecutionMode::Execute,
            timeout_secs: 45,
            max_probes: None,
        };

        let summary = execute_durability_matrix_with(
            &matrix,
            options,
            Some(OperatingSystem::Linux),
            |_, _, env_values| -> Result<CommandExecution, String> {
                let Some((_, scenario_id)) = env_values
                    .iter()
                    .find(|(key, _)| key == "FSQLITE_DURABILITY_SCENARIO_ID")
                else {
                    return Err("missing_scenario_env".to_owned());
                };
                match scenario_id.as_str() {
                    "REC-1" => Ok(CommandExecution {
                        exit_code: Some(0),
                        timed_out: false,
                        succeeded: true,
                        elapsed_ms: 11,
                    }),
                    "REC-2" => Ok(CommandExecution {
                        exit_code: Some(124),
                        timed_out: true,
                        succeeded: false,
                        elapsed_ms: 45_000,
                    }),
                    "REC-3" => Ok(CommandExecution {
                        exit_code: Some(1),
                        timed_out: false,
                        succeeded: false,
                        elapsed_ms: 9,
                    }),
                    "WAL-2" => Err("injected_runner_error".to_owned()),
                    _ => Err("unknown_scenario".to_owned()),
                }
            },
        )
        .expect("execute matrix with injected outcomes");

        let expected_per_scenario = matrix
            .environments
            .iter()
            .filter(|environment| environment.os == OperatingSystem::Linux)
            .count();
        assert_eq!(summary.passed_probes, expected_per_scenario);
        assert_eq!(summary.timeout_probes, expected_per_scenario);
        assert_eq!(summary.failed_probes, expected_per_scenario);
        assert_eq!(summary.error_probes, expected_per_scenario);
        assert!(summary.results.iter().any(|result| {
            result
                .reason
                .as_deref()
                .is_some_and(|reason| reason.contains("injected_runner_error"))
        }));
    }

    #[test]
    fn write_execution_summary_json_roundtrip() {
        let matrix = DurabilityMatrix::canonical(DEFAULT_ROOT_SEED);
        let options = DurabilityExecutionOptions::default();
        let summary = execute_durability_matrix_with(
            &matrix,
            options,
            Some(OperatingSystem::Linux),
            |_, _, _| -> Result<CommandExecution, String> {
                panic!("runner should not be invoked in dry-run mode");
            },
        )
        .expect("dry-run summary");

        let temp = tempdir().expect("create tempdir");
        let path = temp.path().join("durability_execution_summary.json");
        write_execution_summary_json(&path, &summary).expect("write summary json");
        let payload = std::fs::read_to_string(&path).expect("read summary json");
        let restored: DurabilityExecutionSummary =
            serde_json::from_str(&payload).expect("deserialize summary json");
        assert_eq!(restored.bead_id, BEAD_ID);
        assert_eq!(restored.schema_version, EXECUTION_SCHEMA_VERSION);
        assert_eq!(restored.total_probes, summary.total_probes);
    }
}
