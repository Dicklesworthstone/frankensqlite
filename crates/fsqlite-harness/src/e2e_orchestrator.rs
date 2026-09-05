//! Deterministic full-suite E2E script orchestrator and manifest (bd-mblr.4.5.2).
//!
//! Defines a single orchestrated execution manifest for all required E2E scripts,
//! including ordering, shardability, retries policy, timeout budgets, and required
//! artifacts. Cross-references the traceability matrix from bd-mblr.4.5.1 and
//! the deterministic seed taxonomy from bd-mblr.4.3.1.
//!
//! # Usage
//!
//! ```rust,ignore
//! let manifest = build_execution_manifest(0xDEADBEEF);
//! let errors = manifest.validate();
//! assert!(errors.is_empty());
//! let summary = manifest.dry_run_summary();
//! ```

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io;
use std::path::Path;
use std::process::Command;
use std::time::Instant;

use crate::e2e_traceability::{self, ConcurrencyMode, ScriptKind, StorageMode};
use crate::seed_taxonomy::SeedTaxonomy;

#[allow(dead_code)]
const BEAD_ID: &str = "bd-mblr.4.5.2";

/// Root seed for the orchestrator's own seed derivation.
const ORCHESTRATOR_ROOT_SEED: u64 = 0x4F52_4348_4553_5452; // ASCII "ORCHESTR"

/// Default timeout budget per script (seconds).
const DEFAULT_TIMEOUT_SECS: u32 = 300;

/// Maximum retry attempts for flaky scripts.
const MAX_RETRIES: u32 = 2;

/// Schema version for execution summary output.
const EXECUTION_SUMMARY_SCHEMA_VERSION: &str = "1.1.0";

// ─── Manifest Types ─────────────────────────────────────────────────────

/// Execution phase for ordering scripts within the manifest.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ExecutionPhase {
    /// Infrastructure validation (workspace layering, toolchain checks).
    Preflight,
    /// Core correctness tests (SQL, transactions, storage).
    CoreCorrectness,
    /// Concurrency and MVCC tests.
    Concurrency,
    /// Recovery and WAL tests.
    Recovery,
    /// Compatibility and format tests.
    Compatibility,
    /// Extension tests (FTS, JSON, etc.).
    Extensions,
    /// Performance and stress tests (run last, longest).
    Performance,
    /// Summary and report generation.
    Postflight,
}

/// Retry policy for a script execution.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum RetryPolicy {
    /// No retries; fail immediately.
    NoRetry,
    /// Retry up to N times on failure.
    RetryOnFailure { max_attempts: u32 },
    /// Retry only on specific exit codes (e.g., timeout).
    RetryOnTimeout { max_attempts: u32 },
}

/// Shard assignment for parallel execution.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShardAssignment {
    /// Shard index (0-based).
    pub shard_id: u32,
    /// Total number of shards.
    pub total_shards: u32,
}

/// A single script entry in the execution manifest.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ManifestEntry {
    /// Workspace-relative path to the script.
    pub path: String,
    /// Execution phase for ordering.
    pub phase: ExecutionPhase,
    /// Execution order within phase (lower = earlier).
    pub order_within_phase: u32,
    /// Deterministic seed for this script's execution.
    pub seed: u64,
    /// Derived seed taxonomy for this script.
    pub seed_taxonomy: SeedFields,
    /// Invocation command.
    pub command: String,
    /// Required environment variables.
    pub env_vars: Vec<EnvVar>,
    /// Timeout budget in seconds.
    pub timeout_secs: u32,
    /// Retry policy.
    pub retry_policy: RetryPolicy,
    /// Whether this script supports JSON structured output.
    pub json_output: bool,
    /// Scenario IDs this script covers.
    pub scenario_ids: Vec<String>,
    /// Expected artifact paths (workspace-relative).
    pub expected_artifacts: Vec<String>,
    /// Whether this script can be sharded across parallel workers.
    pub shardable: bool,
    /// Script kind from traceability matrix.
    pub kind: ScriptKind,
    /// Storage modes tested.
    pub storage_modes: Vec<StorageMode>,
    /// Concurrency modes tested.
    pub concurrency_modes: Vec<ConcurrencyMode>,
}

/// Flattened seed taxonomy fields for serialization.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct SeedFields {
    pub schedule: u64,
    pub entropy: u64,
    pub fault: u64,
    pub fuzz: u64,
}

/// Environment variable requirement.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EnvVar {
    pub name: String,
    pub required: bool,
    pub default_value: Option<String>,
}

/// Whether execution covers the full catalog or an explicitly selected subset.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ManifestRunScope {
    /// All executable catalog entries, with the full scenario denominator.
    FullSuite,
    /// Exact catalog paths selected by the caller; never a full-suite certificate.
    SelectedScripts {
        /// Number of executable entries before selection.
        catalog_script_count: usize,
        /// Selected paths in canonical execution order.
        selected_paths: Vec<String>,
        /// Entries intentionally omitted, in canonical execution order.
        omitted_paths: Vec<String>,
    },
}

/// The execution manifest and its explicit catalog scope.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionManifest {
    /// Schema version for the manifest format.
    pub schema_version: String,
    /// Bead ID.
    pub bead_id: String,
    /// Root seed used to derive all per-script seeds.
    pub root_seed: u64,
    /// Full-suite or explicit subset identity, preserved in every summary.
    pub run_scope: ManifestRunScope,
    /// Total timeout budget across all scripts (seconds).
    pub total_timeout_budget_secs: u64,
    /// All script entries in execution order.
    pub entries: Vec<ManifestEntry>,
    /// Scenario coverage summary.
    pub coverage: ManifestCoverage,
}

/// Coverage summary for the manifest.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestCoverage {
    /// Total unique scenario IDs covered.
    pub total_scenario_ids: usize,
    /// Scenario IDs not covered by any script.
    pub uncovered_scenario_ids: Vec<String>,
    /// Total scripts in manifest.
    pub total_scripts: usize,
    /// Scripts per phase.
    pub scripts_per_phase: BTreeMap<String, usize>,
    /// Scripts per kind.
    pub scripts_per_kind: BTreeMap<String, usize>,
}

/// Dry-run summary of what the orchestrator would execute.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DryRunSummary {
    pub run_scope: ManifestRunScope,
    pub total_scripts: usize,
    pub total_timeout_budget_secs: u64,
    pub phases: Vec<PhaseSummary>,
    pub total_scenario_ids: usize,
    pub shardable_scripts: usize,
    pub retryable_scripts: usize,
}

/// Summary of a single execution phase.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PhaseSummary {
    pub phase: String,
    pub script_count: usize,
    pub timeout_budget_secs: u64,
    pub scenario_ids: usize,
}

impl ExecutionManifest {
    /// Select exact catalog paths while preserving commands, seeds, and order.
    ///
    /// Empty, unknown, duplicate, and repeated selections are errors. Coverage
    /// continues to report the full catalog's uncovered scenario denominator.
    pub fn select_scripts(mut self, paths: &[String]) -> Result<Self, String> {
        if !matches!(self.run_scope, ManifestRunScope::FullSuite) {
            return Err("script selection requires a full catalog manifest".to_owned());
        }
        if paths.is_empty() {
            return Err("script selection must contain at least one catalog path".to_owned());
        }
        let mut requested = BTreeSet::new();
        for path in paths {
            if !requested.insert(path.as_str()) {
                return Err(format!("duplicate script selector: {path}"));
            }
            if !self.entries.iter().any(|entry| entry.path == *path) {
                return Err(format!("unknown executable catalog path: {path}"));
            }
        }

        let catalog_script_count = self.entries.len();
        let mut selected_paths = Vec::new();
        let mut omitted_paths = Vec::new();
        self.entries.retain(|entry| {
            if requested.contains(entry.path.as_str()) {
                selected_paths.push(entry.path.clone());
                true
            } else {
                omitted_paths.push(entry.path.clone());
                false
            }
        });
        self.total_timeout_budget_secs = self
            .entries
            .iter()
            .map(|entry| u64::from(entry.timeout_secs))
            .sum();
        self.coverage = compute_coverage(
            &self.entries,
            &e2e_traceability::build_canonical_inventory(),
        );
        self.run_scope = ManifestRunScope::SelectedScripts {
            catalog_script_count,
            selected_paths,
            omitted_paths,
        };
        Ok(self)
    }

    /// Validate the manifest for internal consistency.
    pub fn validate(&self) -> Vec<String> {
        let mut errors = Vec::new();

        if let ManifestRunScope::SelectedScripts {
            catalog_script_count,
            selected_paths,
            omitted_paths,
        } = &self.run_scope
        {
            let actual_paths: Vec<_> = self.entries.iter().map(|entry| &entry.path).collect();
            if selected_paths.is_empty()
                || selected_paths.iter().collect::<Vec<_>>() != actual_paths
            {
                errors.push("Selected script identity does not match manifest entries".to_owned());
            }
            let all_paths: BTreeSet<_> = selected_paths.iter().chain(omitted_paths).collect();
            if all_paths.len() != *catalog_script_count
                || selected_paths.len() + omitted_paths.len() != *catalog_script_count
            {
                errors.push("Selected and omitted paths do not partition the catalog".to_owned());
            }
        }

        // No duplicate paths
        let mut seen_paths = BTreeSet::new();
        for entry in &self.entries {
            if !seen_paths.insert(&entry.path) {
                errors.push(format!("Duplicate path in manifest: {}", entry.path));
            }
        }

        // No duplicate seeds (each script gets a unique seed)
        let mut seen_seeds = BTreeSet::new();
        for entry in &self.entries {
            if !seen_seeds.insert(entry.seed) {
                errors.push(format!(
                    "Duplicate seed {} for script {}",
                    entry.seed, entry.path
                ));
            }
        }

        // All entries have at least one scenario ID
        for entry in &self.entries {
            if entry.scenario_ids.is_empty() {
                errors.push(format!("Script {} has no scenario IDs", entry.path));
            }
        }

        // Timeout budget consistency
        let sum_timeouts: u64 = self.entries.iter().map(|e| u64::from(e.timeout_secs)).sum();
        if sum_timeouts != self.total_timeout_budget_secs {
            errors.push(format!(
                "Total timeout budget mismatch: sum={sum_timeouts} declared={}",
                self.total_timeout_budget_secs
            ));
        }

        // Coverage stats consistency
        let actual_scripts = self.entries.len();
        if actual_scripts != self.coverage.total_scripts {
            errors.push(format!(
                "Coverage script count mismatch: actual={actual_scripts} declared={}",
                self.coverage.total_scripts
            ));
        }

        // Ordering within phases is monotonically increasing
        let mut phase_orders: BTreeMap<ExecutionPhase, Vec<u32>> = BTreeMap::new();
        for entry in &self.entries {
            phase_orders
                .entry(entry.phase)
                .or_default()
                .push(entry.order_within_phase);
        }
        for (phase, orders) in &phase_orders {
            for window in orders.windows(2) {
                if window[0] > window[1] {
                    errors.push(format!(
                        "Non-monotonic ordering in phase {phase:?}: {} > {}",
                        window[0], window[1]
                    ));
                }
            }
        }

        errors
    }

    /// Generate a dry-run summary without executing anything.
    #[must_use]
    pub fn dry_run_summary(&self) -> DryRunSummary {
        let mut phases: BTreeMap<ExecutionPhase, (usize, u64, BTreeSet<String>)> = BTreeMap::new();

        for entry in &self.entries {
            let phase_data = phases
                .entry(entry.phase)
                .or_insert_with(|| (0, 0, BTreeSet::new()));
            phase_data.0 += 1;
            phase_data.1 += u64::from(entry.timeout_secs);
            for sid in &entry.scenario_ids {
                phase_data.2.insert(sid.clone());
            }
        }

        let phase_summaries: Vec<PhaseSummary> = phases
            .into_iter()
            .map(|(phase, (count, timeout, sids))| PhaseSummary {
                phase: format!("{phase:?}"),
                script_count: count,
                timeout_budget_secs: timeout,
                scenario_ids: sids.len(),
            })
            .collect();

        let all_sids: BTreeSet<_> = self
            .entries
            .iter()
            .flat_map(|e| e.scenario_ids.iter().cloned())
            .collect();

        let shardable = self.entries.iter().filter(|e| e.shardable).count();
        let retryable = self
            .entries
            .iter()
            .filter(|e| !matches!(e.retry_policy, RetryPolicy::NoRetry))
            .count();

        DryRunSummary {
            run_scope: self.run_scope.clone(),
            total_scripts: self.entries.len(),
            total_timeout_budget_secs: self.total_timeout_budget_secs,
            phases: phase_summaries,
            total_scenario_ids: all_sids.len(),
            shardable_scripts: shardable,
            retryable_scripts: retryable,
        }
    }

    /// Serialize to JSON.
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }
}

// ─── Phase Classification ───────────────────────────────────────────────

fn classify_phase(
    path: &str,
    scenario_ids: &[String],
    concurrency_modes: &[ConcurrencyMode],
) -> ExecutionPhase {
    // Infrastructure and layering checks
    if path.contains("layering") || path.contains("logging_standard") {
        return ExecutionPhase::Preflight;
    }

    // Performance tests
    if path.contains("perf")
        || path.contains("benchmark")
        || path.contains("stress")
        || path.contains("hotspot")
        || path.contains("arc_warmup")
    {
        return ExecutionPhase::Performance;
    }

    // Recovery and WAL tests
    if path.contains("recovery")
        || path.contains("crash")
        || path.contains("wal_replay")
        || path.contains("wal_checksum")
        || path.contains("wal_integrity")
        || scenario_ids
            .iter()
            .any(|s| s.starts_with("REC-") || s.starts_with("WAL-"))
    {
        return ExecutionPhase::Recovery;
    }

    // Concurrency and MVCC tests
    if path.contains("concurrent")
        || path.contains("mvcc")
        || path.contains("ssi")
        || path.contains("write_skew")
        || path.contains("isolation")
        || concurrency_modes.iter().any(|m| {
            matches!(
                m,
                ConcurrencyMode::ConcurrentWriters
                    | ConcurrencyMode::MvccIsolation
                    | ConcurrencyMode::Ssi
            )
        })
    {
        return ExecutionPhase::Concurrency;
    }

    // Extension tests
    if path.contains("fts")
        || path.contains("json1")
        || path.contains("rtree")
        || scenario_ids.iter().any(|s| s.starts_with("EXT-"))
    {
        return ExecutionPhase::Extensions;
    }

    // Compatibility tests
    if path.contains("compat")
        || path.contains("realdb")
        || path.contains("format")
        || path.contains("quirks")
        || scenario_ids.iter().any(|s| s.starts_with("COMPAT-"))
    {
        return ExecutionPhase::Compatibility;
    }

    // Default: core correctness
    ExecutionPhase::CoreCorrectness
}

fn classify_retry_policy(kind: ScriptKind, concurrency_modes: &[ConcurrencyMode]) -> RetryPolicy {
    // Concurrency tests may have inherent non-determinism in scheduling
    let has_concurrency = concurrency_modes.iter().any(|m| {
        matches!(
            m,
            ConcurrencyMode::ConcurrentWriters
                | ConcurrencyMode::MvccIsolation
                | ConcurrencyMode::Ssi
        )
    });

    if has_concurrency {
        return RetryPolicy::RetryOnFailure {
            max_attempts: MAX_RETRIES,
        };
    }

    // Shell scripts may timeout
    if matches!(kind, ScriptKind::ShellE2e) {
        return RetryPolicy::RetryOnTimeout {
            max_attempts: MAX_RETRIES,
        };
    }

    RetryPolicy::NoRetry
}

fn is_shardable(concurrency_modes: &[ConcurrencyMode]) -> bool {
    // Scripts that don't require concurrent writers can be safely sharded
    // (they don't contend on shared database files)
    !concurrency_modes.iter().any(|m| {
        matches!(
            m,
            ConcurrencyMode::ConcurrentWriters
                | ConcurrencyMode::MvccIsolation
                | ConcurrencyMode::Ssi
        )
    })
}

// ─── Build the Manifest ────────────────────────────────────────────────

/// Build the deterministic execution manifest from the traceability matrix.
///
/// The `root_seed` is used to derive unique per-script seeds via the
/// seed taxonomy. Use `ORCHESTRATOR_ROOT_SEED` for the canonical default.
#[must_use]
#[allow(clippy::too_many_lines)]
pub fn build_execution_manifest(root_seed: u64) -> ExecutionManifest {
    let matrix = e2e_traceability::build_canonical_inventory();

    // Phase -> entries (for ordering)
    let mut phase_entries: BTreeMap<ExecutionPhase, Vec<ManifestEntry>> = BTreeMap::new();

    for (idx, script) in matrix.scripts.iter().enumerate() {
        // Skip utility scripts that aren't directly executable tests
        if matches!(script.kind, ScriptKind::ShellUtility) {
            continue;
        }

        let phase = classify_phase(
            &script.path,
            &script.scenario_ids,
            &script.concurrency_modes,
        );

        // Derive unique seed for this script using its path as scope
        let taxonomy = SeedTaxonomy::derive(root_seed, &script.path);
        let script_seed = taxonomy.schedule;
        let seed_fields = SeedFields {
            schedule: taxonomy.schedule,
            entropy: taxonomy.entropy,
            fault: taxonomy.fault,
            fuzz: taxonomy.fuzz,
        };

        let timeout = script
            .invocation
            .timeout_secs
            .unwrap_or(DEFAULT_TIMEOUT_SECS);
        let retry_policy = classify_retry_policy(script.kind, &script.concurrency_modes);
        let shardable = is_shardable(&script.concurrency_modes);

        let env_vars: Vec<EnvVar> = script
            .invocation
            .env_vars
            .iter()
            .map(|name| EnvVar {
                name: name.clone(),
                required: true,
                default_value: None,
            })
            .collect();

        // Add FRANKEN_SEED env var for seed propagation
        let mut all_env_vars = vec![EnvVar {
            name: "FRANKEN_SEED".to_owned(),
            required: false,
            default_value: Some(script_seed.to_string()),
        }];
        all_env_vars.extend(env_vars);

        let entry = ManifestEntry {
            path: script.path.clone(),
            phase,
            order_within_phase: u32::try_from(idx).unwrap_or(u32::MAX),
            seed: script_seed,
            seed_taxonomy: seed_fields,
            command: script.invocation.command.clone(),
            env_vars: all_env_vars,
            timeout_secs: timeout,
            retry_policy,
            json_output: script.invocation.json_output,
            scenario_ids: script.scenario_ids.clone(),
            expected_artifacts: script.artifact_paths.clone(),
            shardable,
            kind: script.kind,
            storage_modes: script.storage_modes.clone(),
            concurrency_modes: script.concurrency_modes.clone(),
        };

        phase_entries.entry(phase).or_default().push(entry);
    }

    // Flatten into execution order: phases are ordered by enum variant,
    // within each phase entries keep their original order
    let mut entries = Vec::new();
    for (_phase, mut phase_scripts) in phase_entries {
        // Re-number within phase
        for (i, entry) in phase_scripts.iter_mut().enumerate() {
            entry.order_within_phase = u32::try_from(i).unwrap_or(u32::MAX);
        }
        entries.extend(phase_scripts);
    }

    let total_timeout: u64 = entries.iter().map(|e| u64::from(e.timeout_secs)).sum();
    let coverage = compute_coverage(&entries, &matrix);

    ExecutionManifest {
        schema_version: "1.1.0".to_owned(),
        bead_id: BEAD_ID.to_owned(),
        root_seed,
        run_scope: ManifestRunScope::FullSuite,
        total_timeout_budget_secs: total_timeout,
        entries,
        coverage,
    }
}

/// Build the manifest with the default orchestrator root seed.
#[must_use]
pub fn build_default_manifest() -> ExecutionManifest {
    build_execution_manifest(ORCHESTRATOR_ROOT_SEED)
}

fn compute_coverage(
    entries: &[ManifestEntry],
    matrix: &e2e_traceability::TraceabilityMatrix,
) -> ManifestCoverage {
    // Collect all scenario IDs from manifest
    let manifest_sids: BTreeSet<String> = entries
        .iter()
        .flat_map(|e| e.scenario_ids.iter().cloned())
        .collect();

    // Collect all scenario IDs from the traceability matrix
    let matrix_sids: BTreeSet<String> = matrix
        .scripts
        .iter()
        .flat_map(|s| s.scenario_ids.iter().cloned())
        .collect();

    // Uncovered = in matrix but not in manifest (due to filtered-out utility scripts)
    let uncovered: Vec<String> = matrix_sids.difference(&manifest_sids).cloned().collect();

    // Scripts per phase
    let mut scripts_per_phase: BTreeMap<String, usize> = BTreeMap::new();
    for entry in entries {
        *scripts_per_phase
            .entry(format!("{:?}", entry.phase))
            .or_insert(0) += 1;
    }

    // Scripts per kind
    let mut scripts_per_kind: BTreeMap<String, usize> = BTreeMap::new();
    for entry in entries {
        *scripts_per_kind
            .entry(format!("{:?}", entry.kind))
            .or_insert(0) += 1;
    }

    ManifestCoverage {
        total_scenario_ids: manifest_sids.len(),
        uncovered_scenario_ids: uncovered,
        total_scripts: entries.len(),
        scripts_per_phase,
        scripts_per_kind,
    }
}

// ─── Execution Runner ──────────────────────────────────────────────────

/// How manifest entries should be processed.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ManifestExecutionMode {
    /// Build and validate summary contracts without launching scripts.
    DryRun,
    /// Execute scripts in canonical order with retries/timeouts/artifact checks.
    Execute,
}

/// Per-script status from orchestrator execution.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ScriptExecutionStatus {
    /// Script was not executed because mode was dry-run.
    DryRun,
    /// Script command completed successfully and all expected artifacts were present.
    Passed,
    /// Script command returned non-zero.
    Failed,
    /// Script command exceeded timeout budget.
    TimedOut,
    /// Script command succeeded but one or more expected artifacts were missing.
    MissingArtifacts,
}

/// Artifact presence record for one script+path pair.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArtifactIndexEntry {
    pub script_path: String,
    pub artifact_path: String,
    pub checked: bool,
    pub present: bool,
}

/// Per-script execution record in canonical manifest order.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScriptExecutionRecord {
    pub path: String,
    pub phase: ExecutionPhase,
    pub command: String,
    pub attempts: u32,
    pub status: ScriptExecutionStatus,
    pub exit_code: Option<i32>,
    pub timed_out: bool,
    pub elapsed_ms: u64,
    pub stdout_log: Option<String>,
    pub stderr_log: Option<String>,
    pub missing_artifacts: Vec<String>,
}

/// Deterministic execution summary for the full-suite orchestrator.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestExecutionSummary {
    pub schema_version: String,
    pub bead_id: String,
    pub manifest_schema_version: String,
    pub execution_mode: ManifestExecutionMode,
    pub root_seed: u64,
    pub run_scope: ManifestRunScope,
    pub total_scripts: usize,
    pub passed_scripts: usize,
    pub failed_scripts: usize,
    pub timed_out_scripts: usize,
    pub missing_artifact_scripts: usize,
    pub missing_scenarios: Vec<String>,
    pub artifact_index: Vec<ArtifactIndexEntry>,
    pub scripts: Vec<ScriptExecutionRecord>,
    /// All selected entries actually executed and passed; false for dry runs.
    pub selected_scripts_pass: bool,
    /// Full-suite contract result only; always false for an explicit subset.
    pub overall_pass: bool,
}

impl ManifestExecutionSummary {
    /// Serialize to pretty JSON for CI artifacts and operator review.
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }
}

#[derive(Debug)]
struct ScriptAttemptOutput {
    exit_code: Option<i32>,
    timed_out: bool,
    succeeded: bool,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
    elapsed_ms: u64,
}

/// Execute the canonical manifest in deterministic order and return a summary.
///
/// In `DryRun`, no commands are executed; status and artifact checks are declared
/// but not evaluated. In `Execute`, each script command is launched with timeout
/// and retry policy enforcement.
#[allow(clippy::too_many_lines)]
pub fn execute_manifest(
    workspace_root: &Path,
    run_dir: &Path,
    manifest: &ExecutionManifest,
    mode: ManifestExecutionMode,
) -> io::Result<ManifestExecutionSummary> {
    let validation_errors = manifest.validate();
    if !validation_errors.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            validation_errors.join("; "),
        ));
    }
    if matches!(mode, ManifestExecutionMode::Execute) {
        fs::create_dir_all(run_dir)?;
    }

    let mut missing_scenarios = manifest.coverage.uncovered_scenario_ids.clone();
    missing_scenarios.sort_unstable();

    let mut scripts = Vec::with_capacity(manifest.entries.len());
    let mut artifact_index = Vec::new();

    for (entry_index, entry) in manifest.entries.iter().enumerate() {
        let mut attempts = 0_u32;
        let mut final_attempt = ScriptAttemptOutput {
            exit_code: None,
            timed_out: false,
            succeeded: false,
            stdout: Vec::new(),
            stderr: Vec::new(),
            elapsed_ms: 0,
        };

        if matches!(mode, ManifestExecutionMode::Execute) {
            let env_values = resolve_env_values(entry);
            let max_attempts = max_attempts(entry.retry_policy);
            loop {
                attempts = attempts.saturating_add(1);
                final_attempt = run_script_attempt(workspace_root, entry, &env_values)?;
                if attempts >= max_attempts
                    || !should_retry(
                        entry.retry_policy,
                        final_attempt.timed_out,
                        final_attempt.succeeded,
                    )
                {
                    break;
                }
            }
        }

        let stem = format!("{entry_index:03}_{}", sanitize_file_stem(&entry.path));

        let (stdout_log, stderr_log) = if matches!(mode, ManifestExecutionMode::Execute) {
            let stdout_path = run_dir.join(format!("{stem}.stdout.log"));
            let stderr_path = run_dir.join(format!("{stem}.stderr.log"));
            fs::write(&stdout_path, &final_attempt.stdout)?;
            fs::write(&stderr_path, &final_attempt.stderr)?;
            (
                Some(relativize_for_summary(&stdout_path, workspace_root)),
                Some(relativize_for_summary(&stderr_path, workspace_root)),
            )
        } else {
            (None, None)
        };

        let mut missing_artifacts = Vec::new();
        for artifact_path in &entry.expected_artifacts {
            let present = if matches!(mode, ManifestExecutionMode::Execute) {
                workspace_root.join(artifact_path).exists()
            } else {
                false
            };
            if matches!(mode, ManifestExecutionMode::Execute) && !present {
                missing_artifacts.push(artifact_path.clone());
            }
            artifact_index.push(ArtifactIndexEntry {
                script_path: entry.path.clone(),
                artifact_path: artifact_path.clone(),
                checked: matches!(mode, ManifestExecutionMode::Execute),
                present,
            });
        }

        let status = if matches!(mode, ManifestExecutionMode::DryRun) {
            ScriptExecutionStatus::DryRun
        } else if final_attempt.timed_out {
            ScriptExecutionStatus::TimedOut
        } else if !final_attempt.succeeded {
            ScriptExecutionStatus::Failed
        } else if !missing_artifacts.is_empty() {
            ScriptExecutionStatus::MissingArtifacts
        } else {
            ScriptExecutionStatus::Passed
        };

        scripts.push(ScriptExecutionRecord {
            path: entry.path.clone(),
            phase: entry.phase,
            command: entry.command.clone(),
            attempts,
            status,
            exit_code: final_attempt.exit_code,
            timed_out: final_attempt.timed_out,
            elapsed_ms: final_attempt.elapsed_ms,
            stdout_log,
            stderr_log,
            missing_artifacts,
        });
    }

    artifact_index.sort_by(|a, b| {
        a.script_path
            .cmp(&b.script_path)
            .then_with(|| a.artifact_path.cmp(&b.artifact_path))
    });

    let passed_scripts = scripts
        .iter()
        .filter(|record| matches!(record.status, ScriptExecutionStatus::Passed))
        .count();
    let failed_scripts = scripts
        .iter()
        .filter(|record| matches!(record.status, ScriptExecutionStatus::Failed))
        .count();
    let timed_out_scripts = scripts
        .iter()
        .filter(|record| matches!(record.status, ScriptExecutionStatus::TimedOut))
        .count();
    let missing_artifact_scripts = scripts
        .iter()
        .filter(|record| matches!(record.status, ScriptExecutionStatus::MissingArtifacts))
        .count();

    let selected_scripts_pass = matches!(mode, ManifestExecutionMode::Execute)
        && !scripts.is_empty()
        && passed_scripts == scripts.len();
    let overall_pass = matches!(manifest.run_scope, ManifestRunScope::FullSuite)
        && missing_scenarios.is_empty()
        && selected_scripts_pass;

    Ok(ManifestExecutionSummary {
        schema_version: EXECUTION_SUMMARY_SCHEMA_VERSION.to_owned(),
        bead_id: BEAD_ID.to_owned(),
        manifest_schema_version: manifest.schema_version.clone(),
        execution_mode: mode,
        root_seed: manifest.root_seed,
        run_scope: manifest.run_scope.clone(),
        total_scripts: scripts.len(),
        passed_scripts,
        failed_scripts,
        timed_out_scripts,
        missing_artifact_scripts,
        missing_scenarios,
        artifact_index,
        scripts,
        selected_scripts_pass,
        overall_pass,
    })
}

fn max_attempts(policy: RetryPolicy) -> u32 {
    match policy {
        RetryPolicy::NoRetry => 1,
        RetryPolicy::RetryOnFailure { max_attempts }
        | RetryPolicy::RetryOnTimeout { max_attempts } => max_attempts.max(1),
    }
}

fn should_retry(policy: RetryPolicy, timed_out: bool, succeeded: bool) -> bool {
    if succeeded {
        return false;
    }
    match policy {
        RetryPolicy::NoRetry => false,
        RetryPolicy::RetryOnFailure { .. } => true,
        RetryPolicy::RetryOnTimeout { .. } => timed_out,
    }
}

fn resolve_env_values(entry: &ManifestEntry) -> Vec<(String, String)> {
    entry
        .env_vars
        .iter()
        .filter_map(|env| {
            if let Some(default_value) = &env.default_value {
                return Some((env.name.clone(), default_value.clone()));
            }
            if let Ok(existing) = std::env::var(&env.name) {
                return Some((env.name.clone(), existing));
            }
            if env.required {
                return Some((env.name.clone(), String::new()));
            }
            None
        })
        .collect()
}

fn run_script_attempt(
    workspace_root: &Path,
    entry: &ManifestEntry,
    env_values: &[(String, String)],
) -> io::Result<ScriptAttemptOutput> {
    let start = Instant::now();

    let mut timeout_cmd = Command::new("timeout");
    timeout_cmd
        .arg(format!("{}s", entry.timeout_secs))
        .arg("bash")
        .arg("-lc")
        .arg(&entry.command)
        .current_dir(workspace_root);
    for (key, value) in env_values {
        timeout_cmd.env(key, value);
    }

    let output = match timeout_cmd.output() {
        Ok(output) => output,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            let mut fallback = Command::new("bash");
            fallback
                .arg("-lc")
                .arg(&entry.command)
                .current_dir(workspace_root);
            for (key, value) in env_values {
                fallback.env(key, value);
            }
            fallback.output()?
        }
        Err(error) => return Err(error),
    };

    let elapsed_ms = u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX);
    let exit_code = output.status.code();
    let timed_out = exit_code == Some(124);
    let succeeded = output.status.success() && !timed_out;

    Ok(ScriptAttemptOutput {
        exit_code,
        timed_out,
        succeeded,
        stdout: output.stdout,
        stderr: output.stderr,
        elapsed_ms,
    })
}

fn sanitize_file_stem(path: &str) -> String {
    let sanitized: String = path
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
        .collect();
    sanitized.trim_matches('_').to_owned()
}

fn relativize_for_summary(path: &Path, workspace_root: &Path) -> String {
    if let Ok(relative) = path.strip_prefix(workspace_root) {
        return relative.to_string_lossy().into_owned();
    }
    path.to_string_lossy().into_owned()
}

// ─── Tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn manifest_builds() {
        let manifest = build_default_manifest();
        assert!(!manifest.entries.is_empty());
        assert_eq!(manifest.schema_version, "1.1.0");
        assert_eq!(manifest.bead_id, BEAD_ID);
    }

    #[test]
    fn manifest_validates() {
        let manifest = build_default_manifest();
        let errors = manifest.validate();
        assert!(
            errors.is_empty(),
            "Validation errors:\n{}",
            errors.join("\n")
        );
    }

    #[test]
    fn manifest_deterministic() {
        let m1 = build_execution_manifest(0x1234);
        let m2 = build_execution_manifest(0x1234);
        assert_eq!(m1.entries.len(), m2.entries.len());
        assert_eq!(m1.total_timeout_budget_secs, m2.total_timeout_budget_secs);
        for (a, b) in m1.entries.iter().zip(m2.entries.iter()) {
            assert_eq!(a.seed, b.seed, "Seeds differ for {}", a.path);
            assert_eq!(a.path, b.path);
        }
    }

    #[test]
    fn different_seeds_produce_different_manifests() {
        let m1 = build_execution_manifest(0x1111);
        let m2 = build_execution_manifest(0x2222);
        // Same structure but different seeds
        assert_eq!(m1.entries.len(), m2.entries.len());
        // At least some seeds should differ
        let differs = m1
            .entries
            .iter()
            .zip(m2.entries.iter())
            .any(|(a, b)| a.seed != b.seed);
        assert!(
            differs,
            "Different root seeds should produce different per-script seeds"
        );
    }

    #[test]
    fn unique_seeds_per_script() {
        let manifest = build_default_manifest();
        let mut seeds = BTreeSet::new();
        for entry in &manifest.entries {
            assert!(
                seeds.insert(entry.seed),
                "Duplicate seed {} for script {}",
                entry.seed,
                entry.path
            );
        }
    }

    #[test]
    fn phases_are_ordered() {
        let manifest = build_default_manifest();
        let mut last_phase = None;
        for entry in &manifest.entries {
            if let Some(prev) = last_phase {
                assert!(
                    entry.phase >= prev,
                    "Phase ordering violated: {:?} after {:?} for {}",
                    entry.phase,
                    prev,
                    entry.path
                );
            }
            last_phase = Some(entry.phase);
        }
    }

    #[test]
    fn all_entries_have_scenarios() {
        let manifest = build_default_manifest();
        for entry in &manifest.entries {
            assert!(
                !entry.scenario_ids.is_empty(),
                "Script {} has no scenario IDs",
                entry.path
            );
        }
    }

    #[test]
    fn timeout_budget_consistent() {
        let manifest = build_default_manifest();
        let sum: u64 = manifest
            .entries
            .iter()
            .map(|e| u64::from(e.timeout_secs))
            .sum();
        assert_eq!(sum, manifest.total_timeout_budget_secs);
    }

    #[test]
    fn coverage_stats_consistent() {
        let manifest = build_default_manifest();
        assert_eq!(manifest.coverage.total_scripts, manifest.entries.len());
        let phase_sum: usize = manifest.coverage.scripts_per_phase.values().sum();
        assert_eq!(phase_sum, manifest.entries.len());
        let kind_sum: usize = manifest.coverage.scripts_per_kind.values().sum();
        assert_eq!(kind_sum, manifest.entries.len());
    }

    #[test]
    fn dry_run_summary() {
        let manifest = build_default_manifest();
        let summary = manifest.dry_run_summary();
        assert_eq!(summary.total_scripts, manifest.entries.len());
        assert_eq!(
            summary.total_timeout_budget_secs,
            manifest.total_timeout_budget_secs
        );
        assert!(!summary.phases.is_empty());
    }

    #[test]
    fn json_roundtrip() {
        let manifest = build_default_manifest();
        let json = manifest.to_json().expect("serialize");
        let deserialized: ExecutionManifest = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(deserialized.entries.len(), manifest.entries.len());
        assert_eq!(deserialized.root_seed, manifest.root_seed);
    }

    #[test]
    fn script_selection_preserves_canonical_identity_order_and_denominator() {
        let full = build_execution_manifest(0x1234);
        let oracle_path = "crates/fsqlite-e2e/tests/concurrent_writer_mvcc_oracle_e2e.rs";
        let oracle = full
            .entries
            .iter()
            .find(|entry| entry.path == oracle_path)
            .expect("canonical oracle entry");
        let first = &full.entries[0];
        assert_ne!(first.path, oracle.path);
        let selected = full
            .clone()
            .select_scripts(&[oracle.path.clone(), first.path.clone()])
            .expect("exact catalog selectors");

        assert_eq!(selected.entries, vec![first.clone(), oracle.clone()]);
        assert_eq!(
            selected.run_scope,
            ManifestRunScope::SelectedScripts {
                catalog_script_count: full.entries.len(),
                selected_paths: vec![first.path.clone(), oracle.path.clone()],
                omitted_paths: full
                    .entries
                    .iter()
                    .filter(|entry| entry.path != first.path && entry.path != oracle.path)
                    .map(|entry| entry.path.clone())
                    .collect(),
            }
        );
        assert!(selected.validate().is_empty());
        assert_eq!(selected.coverage.total_scripts, 2);
        assert_eq!(selected.dry_run_summary().run_scope, selected.run_scope);
        let full_scenarios: BTreeSet<_> = full
            .entries
            .iter()
            .flat_map(|entry| entry.scenario_ids.iter().cloned())
            .chain(full.coverage.uncovered_scenario_ids.iter().cloned())
            .collect();
        let selected_denominator: BTreeSet<_> = selected
            .entries
            .iter()
            .flat_map(|entry| entry.scenario_ids.iter().cloned())
            .chain(selected.coverage.uncovered_scenario_ids.iter().cloned())
            .collect();
        assert_eq!(selected_denominator, full_scenarios);
        let decoded: ExecutionManifest =
            serde_json::from_str(&selected.to_json().expect("selected manifest JSON"))
                .expect("selected manifest roundtrip");
        assert_eq!(decoded.run_scope, selected.run_scope);
    }

    #[test]
    fn script_selection_rejects_empty_unknown_duplicate_and_reselection() {
        let full = build_default_manifest();
        let path = full.entries[0].path.clone();
        assert!(full.clone().select_scripts(&[]).is_err());
        for unknown in ["", "*", "scripts/absent.sh", "echo arbitrary command"] {
            let error = full
                .clone()
                .select_scripts(&[path.clone(), unknown.to_owned()])
                .expect_err("unknown selector must reject the whole request");
            assert!(error.contains("unknown executable catalog path"));
        }
        assert!(
            full.clone()
                .select_scripts(&[path.clone(), path.clone()])
                .expect_err("duplicate selector")
                .contains("duplicate")
        );
        let selected = full
            .select_scripts(std::slice::from_ref(&path))
            .expect("selection");
        assert!(selected.select_scripts(&[path]).is_err());
    }

    #[test]
    fn selected_scope_identity_mismatch_refuses_before_execution() {
        let temp = tempdir().expect("tempdir");
        let full = build_default_manifest();
        let path = full.entries[0].path.clone();
        let mut selected = full.select_scripts(&[path]).expect("selection");
        selected.entries.clear();
        let run_dir = temp.path().join("must-not-create");
        let error = execute_manifest(
            temp.path(),
            &run_dir,
            &selected,
            ManifestExecutionMode::Execute,
        )
        .expect_err("identity mismatch must not execute commands");
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert!(error.to_string().contains("Selected script identity"));
        assert!(!run_dir.exists());
    }

    #[test]
    fn concurrency_scripts_are_retryable() {
        let manifest = build_default_manifest();
        for entry in &manifest.entries {
            let has_concurrency = entry.concurrency_modes.iter().any(|m| {
                matches!(
                    m,
                    ConcurrencyMode::ConcurrentWriters
                        | ConcurrencyMode::MvccIsolation
                        | ConcurrencyMode::Ssi
                )
            });
            if has_concurrency {
                assert!(
                    !matches!(entry.retry_policy, RetryPolicy::NoRetry),
                    "Concurrent script {} should be retryable",
                    entry.path
                );
            }
        }
    }

    #[test]
    fn seed_taxonomy_fields_populated() {
        let manifest = build_default_manifest();
        for entry in &manifest.entries {
            // All four derived seeds should be non-zero and distinct
            let fields = entry.seed_taxonomy;
            assert_ne!(
                fields.schedule, 0,
                "schedule seed is zero for {}",
                entry.path
            );
            assert_ne!(fields.entropy, 0, "entropy seed is zero for {}", entry.path);
            assert_ne!(fields.fault, 0, "fault seed is zero for {}", entry.path);
            assert_ne!(fields.fuzz, 0, "fuzz seed is zero for {}", entry.path);
            // All four should differ from each other
            let set: BTreeSet<u64> = [fields.schedule, fields.entropy, fields.fault, fields.fuzz]
                .into_iter()
                .collect();
            assert_eq!(
                set.len(),
                4,
                "Seed taxonomy fields not all distinct for {}",
                entry.path
            );
        }
    }

    #[test]
    fn franken_seed_env_var_present() {
        let manifest = build_default_manifest();
        for entry in &manifest.entries {
            let has_seed_env = entry.env_vars.iter().any(|v| v.name == "FRANKEN_SEED");
            assert!(
                has_seed_env,
                "Script {} missing FRANKEN_SEED env var",
                entry.path
            );
        }
    }

    #[test]
    fn no_utility_scripts_in_manifest() {
        let manifest = build_default_manifest();
        for entry in &manifest.entries {
            assert_ne!(
                entry.kind,
                ScriptKind::ShellUtility,
                "Utility script {} should not be in execution manifest",
                entry.path
            );
        }
    }

    fn test_entry(
        command: &str,
        expected_artifacts: &[&str],
        retry_policy: RetryPolicy,
    ) -> ManifestEntry {
        ManifestEntry {
            path: "tests/sample_case.sh".to_owned(),
            phase: ExecutionPhase::CoreCorrectness,
            order_within_phase: 0,
            seed: 42,
            seed_taxonomy: SeedFields {
                schedule: 1,
                entropy: 2,
                fault: 3,
                fuzz: 4,
            },
            command: command.to_owned(),
            env_vars: vec![EnvVar {
                name: "FRANKEN_SEED".to_owned(),
                required: false,
                default_value: Some("42".to_owned()),
            }],
            timeout_secs: 10,
            retry_policy,
            json_output: true,
            scenario_ids: vec!["SQL-1".to_owned()],
            expected_artifacts: expected_artifacts
                .iter()
                .map(|path| (*path).to_owned())
                .collect(),
            shardable: true,
            kind: ScriptKind::RustHarnessTest,
            storage_modes: vec![StorageMode::InMemory],
            concurrency_modes: vec![ConcurrencyMode::Sequential],
        }
    }

    fn test_manifest(entry: ManifestEntry) -> ExecutionManifest {
        let mut scripts_per_phase = BTreeMap::new();
        scripts_per_phase.insert("CoreCorrectness".to_owned(), 1);
        let mut scripts_per_kind = BTreeMap::new();
        scripts_per_kind.insert("RustHarnessTest".to_owned(), 1);

        ExecutionManifest {
            schema_version: "1.1.0".to_owned(),
            bead_id: BEAD_ID.to_owned(),
            root_seed: 42,
            run_scope: ManifestRunScope::FullSuite,
            total_timeout_budget_secs: u64::from(entry.timeout_secs),
            entries: vec![entry],
            coverage: ManifestCoverage {
                total_scenario_ids: 1,
                uncovered_scenario_ids: Vec::new(),
                total_scripts: 1,
                scripts_per_phase,
                scripts_per_kind,
            },
        }
    }

    #[test]
    fn execute_manifest_dry_run_produces_contract() {
        let temp = tempdir().expect("tempdir");
        let manifest = test_manifest(test_entry(
            "echo dry-run",
            &["artifacts/out.txt"],
            RetryPolicy::NoRetry,
        ));

        let summary = execute_manifest(
            temp.path(),
            &temp.path().join("run"),
            &manifest,
            ManifestExecutionMode::DryRun,
        )
        .expect("dry-run summary");

        assert_eq!(summary.total_scripts, 1);
        assert_eq!(summary.scripts[0].status, ScriptExecutionStatus::DryRun);
        assert_eq!(summary.scripts[0].attempts, 0);
        assert!(!summary.overall_pass);
        assert!(!summary.selected_scripts_pass);
        assert_eq!(summary.artifact_index.len(), 1);
        assert!(!summary.artifact_index[0].checked);
    }

    #[test]
    fn execute_manifest_runs_script_and_validates_artifacts() {
        let temp = tempdir().expect("tempdir");
        let manifest = test_manifest(test_entry(
            "mkdir -p artifacts && echo ok > artifacts/generated.txt",
            &["artifacts/generated.txt"],
            RetryPolicy::NoRetry,
        ));

        let summary = execute_manifest(
            temp.path(),
            &temp.path().join("run"),
            &manifest,
            ManifestExecutionMode::Execute,
        )
        .expect("execute summary");

        assert!(summary.overall_pass);
        assert_eq!(summary.passed_scripts, 1);
        assert_eq!(summary.scripts[0].status, ScriptExecutionStatus::Passed);
        assert_eq!(summary.scripts[0].attempts, 1);
        assert!(temp.path().join("artifacts/generated.txt").exists());
    }

    #[test]
    fn selected_execution_success_never_claims_full_suite() {
        let temp = tempdir().expect("tempdir");
        // This is an orchestrator contract test, not concurrency evidence.
        let mut manifest =
            test_manifest(test_entry("echo selected-entry", &[], RetryPolicy::NoRetry));
        manifest.run_scope = ManifestRunScope::SelectedScripts {
            catalog_script_count: 2,
            selected_paths: vec![manifest.entries[0].path.clone()],
            omitted_paths: vec!["tests/not-selected.sh".to_owned()],
        };
        manifest.coverage.uncovered_scenario_ids = vec!["SQL-OMITTED".to_owned()];
        let summary = execute_manifest(
            temp.path(),
            &temp.path().join("run"),
            &manifest,
            ManifestExecutionMode::Execute,
        )
        .expect("selected execution");
        assert_eq!(summary.total_scripts, 1);
        assert_eq!(summary.passed_scripts, 1);
        assert!(summary.selected_scripts_pass);
        assert!(!summary.overall_pass);
        assert_eq!(summary.run_scope, manifest.run_scope);
        assert_eq!(summary.missing_scenarios, vec!["SQL-OMITTED".to_owned()]);
        assert_eq!(summary.scripts[0].attempts, 1);
        let stdout = summary.scripts[0]
            .stdout_log
            .as_ref()
            .expect("actual stdout log");
        assert_eq!(
            fs::read_to_string(temp.path().join(stdout)).expect("read stdout"),
            "selected-entry\n"
        );
    }

    #[test]
    fn execute_manifest_detects_missing_artifacts() {
        let temp = tempdir().expect("tempdir");
        let manifest = test_manifest(test_entry(
            "echo no-artifact",
            &["artifacts/missing.txt"],
            RetryPolicy::NoRetry,
        ));

        let summary = execute_manifest(
            temp.path(),
            &temp.path().join("run"),
            &manifest,
            ManifestExecutionMode::Execute,
        )
        .expect("execute summary");

        assert!(!summary.overall_pass);
        assert_eq!(summary.missing_artifact_scripts, 1);
        assert_eq!(
            summary.scripts[0].status,
            ScriptExecutionStatus::MissingArtifacts
        );
        assert_eq!(
            summary.scripts[0].missing_artifacts,
            vec!["artifacts/missing.txt".to_owned()]
        );
    }

    #[test]
    fn execute_manifest_retries_retry_on_failure() {
        let temp = tempdir().expect("tempdir");
        let manifest = test_manifest(test_entry(
            "if [ -f .retry_once_ok ]; then mkdir -p artifacts && echo ok > artifacts/retried.txt; exit 0; else touch .retry_once_ok; exit 1; fi",
            &["artifacts/retried.txt"],
            RetryPolicy::RetryOnFailure { max_attempts: 2 },
        ));

        let summary = execute_manifest(
            temp.path(),
            &temp.path().join("run"),
            &manifest,
            ManifestExecutionMode::Execute,
        )
        .expect("execute summary");

        assert!(summary.overall_pass);
        assert_eq!(summary.scripts[0].status, ScriptExecutionStatus::Passed);
        assert_eq!(summary.scripts[0].attempts, 2);
    }

    #[test]
    fn execute_manifest_fails_when_manifest_has_uncovered_scenarios() {
        let temp = tempdir().expect("tempdir");
        let mut manifest = test_manifest(test_entry("echo ok", &[], RetryPolicy::NoRetry));
        manifest.coverage.uncovered_scenario_ids = vec!["SQL-999".to_owned()];

        let summary = execute_manifest(
            temp.path(),
            &temp.path().join("run"),
            &manifest,
            ManifestExecutionMode::DryRun,
        )
        .expect("dry-run summary");

        assert!(!summary.overall_pass);
        assert_eq!(summary.missing_scenarios, vec!["SQL-999".to_owned()]);
    }
}
