//! Machine-readable validation manifest (bd-mblr.3.5.1).
//!
//! Aggregates coverage, invariant drift, scenario coverage drift, no-mock
//! critical-path, and logging conformance into one deterministic manifest.
//! The manifest is intended for CI policy checks and operator replay.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::path::Path;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::ci_coverage_gate::{
    CoverageGateReport, CoverageThresholds, CoverageVerdict, evaluate_coverage_gate,
};
use crate::e2e_log_schema::{
    self, LogEventSchema, LogEventType, LogPhase, ScenarioCriticality, ShellScriptConformanceReport,
};
use crate::e2e_orchestrator::{
    ManifestExecutionMode, build_default_manifest, build_execution_manifest, execute_manifest,
};
use crate::e2e_traceability;
use crate::log_schema_validator::{ValidationReport, validate_event_stream};
use crate::no_mock_critical_path_gate::{
    DEFAULT_CRITICAL_CATEGORIES, NoMockCriticalPathReport, NoMockVerdict,
    evaluate_no_mock_critical_path_gate,
};
use crate::no_mock_evidence::{self, NoMockEvidenceEntry};
use crate::parity_taxonomy::FeatureCategory;
use crate::unit_matrix;

const BEAD_ID: &str = "bd-mblr.3.5.1";

/// Schema version of the machine-readable validation manifest.
pub const VALIDATION_MANIFEST_SCHEMA_VERSION: &str = "1.0.0";
/// Canonical scenario ID used by validation-manifest runs.
pub const VALIDATION_MANIFEST_SCENARIO_ID: &str = "QUALITY-351";

/// Gate ID for coverage threshold enforcement.
pub const COVERAGE_GATE_ID: &str = "bd-mblr.3.1.1";
/// Gate ID for invariant drift enforcement.
pub const INVARIANT_DRIFT_GATE_ID: &str = "bd-mblr.3.1.2";
/// Gate ID for scenario coverage drift enforcement.
pub const SCENARIO_DRIFT_GATE_ID: &str = "bd-mblr.3.2.2";
/// Gate ID for no-mock critical-path enforcement.
pub const NO_MOCK_GATE_ID: &str = "bd-mblr.3.4.1";
/// Gate ID for shell-script logging conformance.
pub const LOGGING_GATE_ID: &str = "bd-mblr.5.5.1";

const COVERAGE_ARTIFACT_NAME: &str = "coverage_gate_report.json";
const INVARIANT_DRIFT_ARTIFACT_NAME: &str = "invariant_drift_report.json";
const SCENARIO_DRIFT_ARTIFACT_NAME: &str = "scenario_coverage_drift_report.json";
const NO_MOCK_ARTIFACT_NAME: &str = "no_mock_critical_path_report.json";
const LOGGING_ARTIFACT_NAME: &str = "logging_conformance_report.json";
const LOG_EVENTS_ARTIFACT_NAME: &str = "validation_manifest_events.jsonl";

/// Outcome class for a single gate or aggregate manifest verdict.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum GateOutcome {
    /// Gate passed with no warnings.
    Pass,
    /// Gate passed with warnings.
    PassWithWarnings,
    /// Gate failed.
    Fail,
}

impl std::fmt::Display for GateOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pass => write!(f, "PASS"),
            Self::PassWithWarnings => write!(f, "PASS_WITH_WARNINGS"),
            Self::Fail => write!(f, "FAIL"),
        }
    }
}

/// Minimal per-gate record embedded in the top-level manifest.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GateRecord {
    /// Stable gate identifier.
    pub gate_id: String,
    /// Gate family for grouping in UI/reporting.
    pub gate_family: String,
    /// Bead that owns this gate contract.
    pub bead_id: String,
    /// Pass/fail verdict.
    pub outcome: GateOutcome,
    /// Deterministic timestamp (caller-provided for reproducibility).
    pub timestamp_unix_ms: u128,
    /// Commit SHA associated with this manifest build.
    pub commit_sha: String,
    /// Machine-readable artifact URIs generated for this gate.
    pub artifact_uris: Vec<String>,
    /// Human summary line.
    pub summary: String,
}

/// Replay contract for deterministic reruns.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplayContract {
    /// Canonical replay command.
    pub command: String,
    /// Deterministic root seed for scenario execution.
    pub root_seed: u64,
    /// Scenario identifier associated with this manifest run.
    pub scenario_id: String,
}

/// Invariant-drift gap severity.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum InvariantGapSeverity {
    /// Required contract violation.
    Required,
}

/// Invariant-drift gap classification.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum InvariantGapReason {
    /// Canonical unit matrix failed structural validation.
    MatrixValidationError,
    /// No-mock evidence map failed structural validation.
    EvidenceValidationError,
    /// A matrix invariant lacks any evidence entry.
    MissingInvariantEvidence,
    /// A critical-path invariant has no real non-exception evidence.
    MockOnlyCriticalPathInvariant,
}

/// A single invariant-drift gap.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InvariantDriftGap {
    /// Gap reason.
    pub reason: InvariantGapReason,
    /// Gap severity.
    pub severity: InvariantGapSeverity,
    /// Unit matrix test identifier if applicable.
    pub matrix_test_id: Option<String>,
    /// Feature category display name if applicable.
    pub category: Option<String>,
    /// Invariant text if applicable.
    pub invariant: Option<String>,
    /// Human-readable details.
    pub details: String,
    /// Actionable remediation guidance.
    pub remediation: String,
}

/// Invariant-drift status included in the top-level manifest.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct InvariantDriftStatus {
    /// Status schema version.
    pub schema_version: String,
    /// Gate identifier.
    pub gate_id: String,
    /// Unit matrix schema version.
    pub unit_matrix_schema_version: String,
    /// Evidence map schema version.
    pub evidence_schema_version: String,
    /// Number of matrix tests.
    pub total_matrix_tests: usize,
    /// Number of matrix invariants.
    pub total_matrix_invariants: usize,
    /// Overall matrix fill percentage.
    pub unit_matrix_overall_fill_pct: f64,
    /// Number of evidence entries.
    pub total_evidence_entries: usize,
    /// Evidence coverage percentage.
    pub evidence_coverage_pct: f64,
    /// Number of critical-path invariants.
    pub critical_invariant_count: usize,
    /// Number of critical invariants with real evidence.
    pub critical_invariants_with_real_evidence: usize,
    /// Required gap count.
    pub required_gap_count: usize,
    /// Overall pass/fail.
    pub overall_pass: bool,
    /// Drift gaps.
    pub gaps: Vec<InvariantDriftGap>,
}

/// Scenario-drift gap reason.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum ScenarioGapReason {
    /// Required catalog scenario has no script mapping.
    MissingScenarioMapping,
    /// Required scenario missing from required execution lanes.
    MissingRequiredExecutionLane,
    /// Execution manifest references scenario absent from catalog.
    MissingCatalogEntryForManifestScenario,
}

/// Scenario-drift gap severity.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum ScenarioGapSeverity {
    /// Required contract violation.
    Required,
}

/// Single scenario-drift gap.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScenarioCoverageGap {
    /// Scenario identifier.
    pub scenario_id: String,
    /// Gap reason.
    pub reason: ScenarioGapReason,
    /// Gap severity.
    pub severity: ScenarioGapSeverity,
    /// Criticality if the scenario is catalogued.
    pub criticality: Option<ScenarioCriticality>,
    /// Scenario description if known.
    pub description: Option<String>,
    /// Covering script paths.
    pub covering_scripts: Vec<String>,
    /// Replay command if known.
    pub replay_command: Option<String>,
}

/// Scenario coverage drift status.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScenarioCoverageDriftStatus {
    /// Status schema version.
    pub schema_version: String,
    /// Gate identifier.
    pub gate_id: String,
    /// Root seed used for deterministic manifest generation.
    pub root_seed: u64,
    /// Number of scenarios in the coverage catalog.
    pub total_catalog_scenarios: usize,
    /// Number of required scenarios (critical+important).
    pub required_catalog_scenarios: usize,
    /// Number of scenarios in execution manifest coverage index.
    pub total_manifest_scenarios: usize,
    /// Number of scenarios missing from execution lanes.
    pub total_manifest_missing: usize,
    /// Required gap count.
    pub required_gap_count: usize,
    /// Overall pass/fail.
    pub overall_pass: bool,
    /// Drift gaps.
    pub gaps: Vec<ScenarioCoverageGap>,
}

/// Logging conformance status.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConformanceStatus {
    /// Status schema version.
    pub schema_version: String,
    /// Gate identifier.
    pub gate_id: String,
    /// Profile document path consumed by shell scripts.
    pub profile_doc_path: String,
    /// Profile version.
    pub profile_version: String,
    /// Errors from shell profile integrity validation.
    pub profile_errors: Vec<String>,
    /// Structured log validation report for manifest-generation events.
    pub log_validation: ValidationReport,
    /// Static shell-entrypoint conformance report.
    pub shell_script_conformance: ShellScriptConformanceReport,
    /// Overall pass/fail.
    pub overall_pass: bool,
}

/// Full machine-readable validation manifest.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationManifest {
    /// Manifest schema version.
    pub schema_version: String,
    /// Owning bead identifier.
    pub bead_id: String,
    /// Correlation run identifier.
    pub run_id: String,
    /// Correlation trace identifier.
    pub trace_id: String,
    /// Scenario identifier for replay and triage.
    pub scenario_id: String,
    /// Deterministic timestamp (caller-provided).
    pub generated_unix_ms: u128,
    /// Commit SHA associated with this build.
    pub commit_sha: String,
    /// Aggregate gate outcome.
    pub overall_outcome: GateOutcome,
    /// Convenience boolean for fail-closed checks.
    pub overall_pass: bool,
    /// Replay contract.
    pub replay: ReplayContract,
    /// Flattened artifact URI index.
    pub artifact_uris: Vec<String>,
    /// Ordered gate records.
    pub gates: Vec<GateRecord>,
    /// Coverage gate report.
    pub coverage_gate: CoverageGateReport,
    /// Invariant drift status.
    pub invariant_drift: InvariantDriftStatus,
    /// Scenario coverage drift status.
    pub scenario_coverage_drift: ScenarioCoverageDriftStatus,
    /// No-mock critical-path report.
    pub no_mock_critical_path: NoMockCriticalPathReport,
    /// Logging conformance status.
    pub logging_conformance: LoggingConformanceStatus,
    /// Human-readable summary.
    pub summary: String,
}

impl ValidationManifest {
    /// Serialize to deterministic pretty JSON.
    ///
    /// # Errors
    ///
    /// Returns `Err` when serialization fails.
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }

    /// Deserialize from JSON.
    ///
    /// # Errors
    ///
    /// Returns `Err` for malformed JSON.
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }

    /// Render a concise human summary.
    #[must_use]
    pub fn render_summary(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(out, "# Validation Manifest");
        let _ = writeln!(out);
        let _ = writeln!(out, "- bead_id: `{}`", self.bead_id);
        let _ = writeln!(out, "- schema_version: `{}`", self.schema_version);
        let _ = writeln!(out, "- run_id: `{}`", self.run_id);
        let _ = writeln!(out, "- trace_id: `{}`", self.trace_id);
        let _ = writeln!(out, "- scenario_id: `{}`", self.scenario_id);
        let _ = writeln!(out, "- commit_sha: `{}`", self.commit_sha);
        let _ = writeln!(out, "- generated_unix_ms: `{}`", self.generated_unix_ms);
        let _ = writeln!(out, "- overall_outcome: `{}`", self.overall_outcome);
        let _ = writeln!(out, "- overall_pass: `{}`", self.overall_pass);
        let _ = writeln!(out);
        let _ = writeln!(out, "## Gates");
        for gate in &self.gates {
            let _ = writeln!(
                out,
                "- `{}` [{}] outcome=`{}` artifacts={} summary=\"{}\"",
                gate.gate_id,
                gate.gate_family,
                gate.outcome,
                gate.artifact_uris.join(","),
                gate.summary
            );
        }
        let _ = writeln!(out);
        let _ = writeln!(out, "## Replay");
        let _ = writeln!(out, "- command: `{}`", self.replay.command);
        out
    }
}

/// Build configuration for `build_validation_manifest_bundle`.
#[derive(Debug, Clone)]
pub struct ValidationManifestConfig {
    /// Commit SHA for artifact correlation.
    pub commit_sha: String,
    /// Correlation run identifier.
    pub run_id: String,
    /// Correlation trace identifier.
    pub trace_id: String,
    /// Scenario identifier used in log events and replay contract.
    pub scenario_id: String,
    /// Deterministic timestamp for this build.
    pub generated_unix_ms: u128,
    /// Root seed used by scenario manifest generation.
    pub root_seed: Option<u64>,
    /// URI prefix used for generated gate artifacts.
    pub artifact_uri_prefix: String,
}

impl Default for ValidationManifestConfig {
    fn default() -> Self {
        Self {
            commit_sha: "unknown".to_owned(),
            run_id: format!("{BEAD_ID}-run"),
            trace_id: "trace-unknown".to_owned(),
            scenario_id: VALIDATION_MANIFEST_SCENARIO_ID.to_owned(),
            generated_unix_ms: 1_700_000_000_000,
            root_seed: Some(424_242),
            artifact_uri_prefix: "artifacts/validation-manifest".to_owned(),
        }
    }
}

/// Build output containing the manifest and per-gate serialized artifacts.
#[derive(Debug, Clone)]
pub struct ValidationManifestBundle {
    /// Final normalized manifest.
    pub manifest: ValidationManifest,
    /// URI -> serialized JSON/JSONL content.
    pub gate_artifacts: BTreeMap<String, String>,
    /// Human-readable summary derived from the manifest.
    pub human_summary: String,
}

/// Build the validation manifest and all per-gate artifacts.
///
/// # Errors
///
/// Returns `Err` when any gate evaluation or serialization fails.
#[allow(clippy::too_many_lines)]
pub fn build_validation_manifest_bundle(
    config: &ValidationManifestConfig,
) -> Result<ValidationManifestBundle, String> {
    let coverage_gate = evaluate_coverage_gate(&CoverageThresholds::default());
    let invariant_drift = evaluate_invariant_drift_status();
    let scenario_coverage_drift = evaluate_scenario_coverage_drift_status(config.root_seed)?;
    let no_mock_critical_path = evaluate_no_mock_critical_path_gate(&DEFAULT_CRITICAL_CATEGORIES);

    let artifact_prefix = normalize_artifact_prefix(&config.artifact_uri_prefix);
    let coverage_uri = format!("{artifact_prefix}/{COVERAGE_ARTIFACT_NAME}");
    let invariant_uri = format!("{artifact_prefix}/{INVARIANT_DRIFT_ARTIFACT_NAME}");
    let scenario_uri = format!("{artifact_prefix}/{SCENARIO_DRIFT_ARTIFACT_NAME}");
    let no_mock_uri = format!("{artifact_prefix}/{NO_MOCK_ARTIFACT_NAME}");
    let logging_uri = format!("{artifact_prefix}/{LOGGING_ARTIFACT_NAME}");
    let log_events_uri = format!("{artifact_prefix}/{LOG_EVENTS_ARTIFACT_NAME}");

    let mut gate_artifacts = BTreeMap::new();
    gate_artifacts.insert(
        coverage_uri.clone(),
        coverage_gate
            .to_json()
            .map_err(|error| format!("coverage_report_serialize_failed: {error}"))?,
    );
    gate_artifacts.insert(
        invariant_uri.clone(),
        serde_json::to_string_pretty(&invariant_drift)
            .map_err(|error| format!("invariant_drift_serialize_failed: {error}"))?,
    );
    gate_artifacts.insert(
        scenario_uri.clone(),
        serde_json::to_string_pretty(&scenario_coverage_drift)
            .map_err(|error| format!("scenario_drift_serialize_failed: {error}"))?,
    );
    gate_artifacts.insert(
        no_mock_uri.clone(),
        no_mock_critical_path
            .to_json()
            .map_err(|error| format!("no_mock_report_serialize_failed: {error}"))?,
    );

    let mut gate_records = vec![
        GateRecord {
            gate_id: COVERAGE_GATE_ID.to_owned(),
            gate_family: "coverage".to_owned(),
            bead_id: "bd-mblr.3.1.1".to_owned(),
            outcome: coverage_outcome(&coverage_gate),
            timestamp_unix_ms: config.generated_unix_ms,
            commit_sha: config.commit_sha.clone(),
            artifact_uris: vec![coverage_uri.clone()],
            summary: coverage_gate.summary.clone(),
        },
        GateRecord {
            gate_id: INVARIANT_DRIFT_GATE_ID.to_owned(),
            gate_family: "invariant_drift".to_owned(),
            bead_id: "bd-mblr.3.1.2".to_owned(),
            outcome: if invariant_drift.overall_pass {
                GateOutcome::Pass
            } else {
                GateOutcome::Fail
            },
            timestamp_unix_ms: config.generated_unix_ms,
            commit_sha: config.commit_sha.clone(),
            artifact_uris: vec![invariant_uri.clone()],
            summary: format!(
                "invariant drift: required_gap_count={} critical_real={}/{}",
                invariant_drift.required_gap_count,
                invariant_drift.critical_invariants_with_real_evidence,
                invariant_drift.critical_invariant_count
            ),
        },
        GateRecord {
            gate_id: SCENARIO_DRIFT_GATE_ID.to_owned(),
            gate_family: "scenario_drift".to_owned(),
            bead_id: "bd-mblr.3.2.2".to_owned(),
            outcome: if scenario_coverage_drift.overall_pass {
                GateOutcome::Pass
            } else {
                GateOutcome::Fail
            },
            timestamp_unix_ms: config.generated_unix_ms,
            commit_sha: config.commit_sha.clone(),
            artifact_uris: vec![scenario_uri.clone()],
            summary: format!(
                "scenario drift: required_gap_count={} manifest_missing={}",
                scenario_coverage_drift.required_gap_count,
                scenario_coverage_drift.total_manifest_missing
            ),
        },
        GateRecord {
            gate_id: NO_MOCK_GATE_ID.to_owned(),
            gate_family: "no_mock_critical_path".to_owned(),
            bead_id: "bd-mblr.3.4.1".to_owned(),
            outcome: no_mock_outcome(&no_mock_critical_path),
            timestamp_unix_ms: config.generated_unix_ms,
            commit_sha: config.commit_sha.clone(),
            artifact_uris: vec![no_mock_uri.clone()],
            summary: no_mock_critical_path.summary.clone(),
        },
    ];

    normalize_gate_records(&mut gate_records);

    let preliminary_overall_outcome =
        aggregate_outcome_from_iter(gate_records.iter().map(|gate| gate.outcome));

    let log_events = build_manifest_log_events(
        config,
        &gate_records,
        preliminary_overall_outcome,
        &[coverage_uri, invariant_uri, scenario_uri, no_mock_uri],
    );
    let log_events_jsonl = crate::log_schema_validator::encode_jsonl_stream(&log_events)
        .map_err(|error| format!("manifest_log_events_encode_failed: {error}"))?;
    gate_artifacts.insert(log_events_uri.clone(), log_events_jsonl);

    let log_validation = validate_event_stream(&log_events);
    let shell_profile = e2e_log_schema::build_shell_script_log_profile();
    let profile_errors = e2e_log_schema::validate_shell_script_log_profile(&shell_profile);
    let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .map_err(|error| format!("workspace_root_resolve_failed: {error}"))?;
    let traceability = e2e_traceability::build_canonical_inventory();
    let shell_script_conformance =
        e2e_log_schema::assess_shell_script_profile_conformance(&workspace_root, &traceability)
            .map_err(|error| format!("shell_script_conformance_failed: {error}"))?;
    let logging_pass =
        profile_errors.is_empty() && log_validation.passed && shell_script_conformance.overall_pass;

    let logging_conformance = LoggingConformanceStatus {
        schema_version: VALIDATION_MANIFEST_SCHEMA_VERSION.to_owned(),
        gate_id: LOGGING_GATE_ID.to_owned(),
        profile_doc_path: e2e_log_schema::SHELL_SCRIPT_LOG_PROFILE_DOC_PATH.to_owned(),
        profile_version: shell_profile.profile_version,
        profile_errors,
        log_validation,
        shell_script_conformance,
        overall_pass: logging_pass,
    };
    gate_artifacts.insert(
        logging_uri.clone(),
        serde_json::to_string_pretty(&logging_conformance)
            .map_err(|error| format!("logging_conformance_serialize_failed: {error}"))?,
    );

    gate_records.push(GateRecord {
        gate_id: LOGGING_GATE_ID.to_owned(),
        gate_family: "logging_conformance".to_owned(),
        bead_id: "bd-mblr.5.5.1".to_owned(),
        outcome: logging_outcome(&logging_conformance),
        timestamp_unix_ms: config.generated_unix_ms,
        commit_sha: config.commit_sha.clone(),
        artifact_uris: vec![logging_uri, log_events_uri],
        summary: format!(
            "logging conformance: profile_errors={} schema_errors={} warnings={} shell_errors={} shell_warnings={}",
            logging_conformance.profile_errors.len(),
            logging_conformance.log_validation.stats.error_count,
            logging_conformance.log_validation.stats.warning_count,
            logging_conformance.shell_script_conformance.error_count,
            logging_conformance.shell_script_conformance.warning_count,
        ),
    });
    normalize_gate_records(&mut gate_records);

    let overall_outcome = aggregate_outcome_from_iter(gate_records.iter().map(|gate| gate.outcome));
    let overall_pass = overall_outcome != GateOutcome::Fail;

    let root_seed = config.root_seed.unwrap_or(424_242);
    let replay = ReplayContract {
        command: build_replay_command(config, root_seed, &artifact_prefix),
        root_seed,
        scenario_id: config.scenario_id.clone(),
    };

    let mut artifact_uris = gate_records
        .iter()
        .flat_map(|gate| gate.artifact_uris.iter().cloned())
        .collect::<Vec<_>>();
    artifact_uris = unique_sorted_strings(artifact_uris);

    let mut manifest = ValidationManifest {
        schema_version: VALIDATION_MANIFEST_SCHEMA_VERSION.to_owned(),
        bead_id: BEAD_ID.to_owned(),
        run_id: config.run_id.clone(),
        trace_id: config.trace_id.clone(),
        scenario_id: config.scenario_id.clone(),
        generated_unix_ms: config.generated_unix_ms,
        commit_sha: config.commit_sha.clone(),
        overall_outcome,
        overall_pass,
        replay,
        artifact_uris,
        gates: gate_records,
        coverage_gate,
        invariant_drift,
        scenario_coverage_drift,
        no_mock_critical_path,
        logging_conformance,
        summary: String::new(),
    };

    normalize_manifest(&mut manifest);
    manifest.summary = build_manifest_summary(&manifest);

    let contract_errors = validate_manifest_contract(&manifest);
    if !contract_errors.is_empty() {
        return Err(format!(
            "manifest_contract_validation_failed: {}",
            contract_errors.join("; ")
        ));
    }

    let human_summary = manifest.render_summary();
    Ok(ValidationManifestBundle {
        manifest,
        gate_artifacts,
        human_summary,
    })
}

fn coverage_outcome(report: &CoverageGateReport) -> GateOutcome {
    match report.verdict {
        CoverageVerdict::Pass => GateOutcome::Pass,
        CoverageVerdict::PassWithWarnings => GateOutcome::PassWithWarnings,
        CoverageVerdict::Fail => GateOutcome::Fail,
    }
}

fn no_mock_outcome(report: &NoMockCriticalPathReport) -> GateOutcome {
    match report.verdict {
        NoMockVerdict::Pass => GateOutcome::Pass,
        NoMockVerdict::PassWithWarnings => GateOutcome::PassWithWarnings,
        NoMockVerdict::Fail => GateOutcome::Fail,
    }
}

fn logging_outcome(status: &LoggingConformanceStatus) -> GateOutcome {
    if !status.overall_pass {
        GateOutcome::Fail
    } else if status.log_validation.stats.warning_count > 0 {
        GateOutcome::PassWithWarnings
    } else {
        GateOutcome::Pass
    }
}

fn build_manifest_summary(manifest: &ValidationManifest) -> String {
    format!(
        "Validation manifest {}: outcome={} gates={} artifacts={} commit_sha={}",
        manifest.bead_id,
        manifest.overall_outcome,
        manifest.gates.len(),
        manifest.artifact_uris.len(),
        manifest.commit_sha
    )
}

fn normalize_artifact_prefix(prefix: &str) -> String {
    prefix.trim_matches('/').to_owned()
}

fn shell_single_quote(value: &str) -> String {
    if value.is_empty() {
        "''".to_owned()
    } else {
        let escaped = value.replace('\'', "'\"'\"'");
        format!("'{escaped}'")
    }
}

fn build_replay_command(
    config: &ValidationManifestConfig,
    root_seed: u64,
    artifact_prefix: &str,
) -> String {
    format!(
        "cargo run -p fsqlite-harness --bin validation_manifest_runner -- \
--root-seed {root_seed} --generated-unix-ms {} --commit-sha {} --run-id {} --trace-id {} --scenario-id {} --artifact-uri-prefix {}",
        config.generated_unix_ms,
        shell_single_quote(&config.commit_sha),
        shell_single_quote(&config.run_id),
        shell_single_quote(&config.trace_id),
        shell_single_quote(&config.scenario_id),
        shell_single_quote(artifact_prefix),
    )
}

fn unique_sorted_strings(mut values: Vec<String>) -> Vec<String> {
    values.sort();
    values.dedup();
    values
}

fn is_sorted_unique(values: &[String]) -> bool {
    values.windows(2).all(|pair| pair[0] < pair[1])
}

fn normalize_gate_records(gates: &mut [GateRecord]) {
    for gate in gates.iter_mut() {
        gate.artifact_uris = unique_sorted_strings(std::mem::take(&mut gate.artifact_uris));
    }
    gates.sort_by(|left, right| left.gate_id.cmp(&right.gate_id));
}

fn normalize_manifest(manifest: &mut ValidationManifest) {
    normalize_gate_records(&mut manifest.gates);
    manifest.artifact_uris = unique_sorted_strings(std::mem::take(&mut manifest.artifact_uris));
    manifest.invariant_drift.gaps.sort_by(|left, right| {
        left.reason
            .cmp(&right.reason)
            .then_with(|| left.matrix_test_id.cmp(&right.matrix_test_id))
            .then_with(|| left.invariant.cmp(&right.invariant))
            .then_with(|| left.details.cmp(&right.details))
    });
    manifest
        .scenario_coverage_drift
        .gaps
        .sort_by(|left, right| {
            left.scenario_id
                .cmp(&right.scenario_id)
                .then_with(|| left.reason.cmp(&right.reason))
        });
}

fn _aggregate_outcome(outcomes: &[GateOutcome]) -> GateOutcome {
    if outcomes.contains(&GateOutcome::Fail) {
        GateOutcome::Fail
    } else if outcomes.contains(&GateOutcome::PassWithWarnings) {
        GateOutcome::PassWithWarnings
    } else {
        GateOutcome::Pass
    }
}

fn aggregate_outcome_from_iter<I>(outcomes: I) -> GateOutcome
where
    I: Iterator<Item = GateOutcome>,
{
    let mut saw_warning = false;
    for outcome in outcomes {
        if outcome == GateOutcome::Fail {
            return GateOutcome::Fail;
        }
        if outcome == GateOutcome::PassWithWarnings {
            saw_warning = true;
        }
    }
    if saw_warning {
        GateOutcome::PassWithWarnings
    } else {
        GateOutcome::Pass
    }
}

fn sha256_hex(input: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    let digest = hasher.finalize();
    format!("{digest:x}")
}

fn synthetic_timestamp(unix_ms: u128, offset_ms: u128) -> String {
    let total = unix_ms.saturating_add(offset_ms);
    let millis = total % 1_000;
    let seconds = (total / 1_000) % 60;
    let minutes = (total / 60_000) % 60;
    let hours = (total / 3_600_000) % 24;
    let day = ((total / 86_400_000) % 28) + 1;
    format!("2026-01-{day:02}T{hours:02}:{minutes:02}:{seconds:02}.{millis:03}Z")
}

fn make_event_context(
    trace_id: &str,
    outcome: &str,
    duration_ms: &str,
    artifact_paths: &str,
    invariant_ids: &str,
) -> BTreeMap<String, String> {
    let mut context = BTreeMap::new();
    context.insert("trace_id".to_owned(), trace_id.to_owned());
    context.insert("level".to_owned(), "INFO".to_owned());
    context.insert("outcome".to_owned(), outcome.to_owned());
    context.insert("duration_ms".to_owned(), duration_ms.to_owned());
    context.insert("retry_attempt".to_owned(), "0".to_owned());
    context.insert("artifact_paths".to_owned(), artifact_paths.to_owned());
    context.insert("invariant_ids".to_owned(), invariant_ids.to_owned());
    context
}

fn build_manifest_log_events(
    config: &ValidationManifestConfig,
    gates: &[GateRecord],
    aggregate: GateOutcome,
    artifact_uris: &[String],
) -> Vec<LogEventSchema> {
    let artifact_paths = artifact_uris.join(",");
    let mut events = Vec::with_capacity(gates.len() + 2);

    events.push(LogEventSchema {
        run_id: config.run_id.clone(),
        timestamp: synthetic_timestamp(config.generated_unix_ms, 0),
        phase: LogPhase::Setup,
        event_type: LogEventType::Start,
        scenario_id: Some(config.scenario_id.clone()),
        seed: config.root_seed,
        backend: Some("fsqlite".to_owned()),
        artifact_hash: Some(sha256_hex(&artifact_paths)),
        context: make_event_context(&config.trace_id, "start", "0", &artifact_paths, BEAD_ID),
    });

    for (index, gate) in gates.iter().enumerate() {
        let offset = u128::try_from(index).unwrap_or(0).saturating_add(1) * 10;
        let joined_gate_artifacts = gate.artifact_uris.join(",");
        events.push(LogEventSchema {
            run_id: config.run_id.clone(),
            timestamp: synthetic_timestamp(config.generated_unix_ms, offset),
            phase: LogPhase::Report,
            event_type: LogEventType::ArtifactGenerated,
            scenario_id: Some(config.scenario_id.clone()),
            seed: config.root_seed,
            backend: Some("fsqlite".to_owned()),
            artifact_hash: Some(sha256_hex(&joined_gate_artifacts)),
            context: make_event_context(
                &config.trace_id,
                &gate.outcome.to_string(),
                "0",
                &joined_gate_artifacts,
                &gate.gate_id,
            ),
        });
    }

    let final_type = if aggregate == GateOutcome::Fail {
        LogEventType::Fail
    } else {
        LogEventType::Pass
    };
    let final_offset = u128::try_from(gates.len()).unwrap_or(0).saturating_add(2) * 10;
    events.push(LogEventSchema {
        run_id: config.run_id.clone(),
        timestamp: synthetic_timestamp(config.generated_unix_ms, final_offset),
        phase: LogPhase::Report,
        event_type: final_type,
        scenario_id: Some(config.scenario_id.clone()),
        seed: config.root_seed,
        backend: Some("fsqlite".to_owned()),
        artifact_hash: Some(sha256_hex(&artifact_paths)),
        context: make_event_context(
            &config.trace_id,
            &aggregate.to_string(),
            "0",
            &artifact_paths,
            BEAD_ID,
        ),
    });

    events
}

fn is_critical_category(category: FeatureCategory) -> bool {
    matches!(
        category,
        FeatureCategory::SqlGrammar
            | FeatureCategory::VdbeOpcodes
            | FeatureCategory::StorageTransaction
    )
}

fn index_evidence(
    entries: &[NoMockEvidenceEntry],
) -> BTreeMap<(String, String), Vec<&NoMockEvidenceEntry>> {
    let mut index: BTreeMap<(String, String), Vec<&NoMockEvidenceEntry>> = BTreeMap::new();
    for entry in entries {
        index
            .entry((entry.matrix_test_id.clone(), entry.invariant.clone()))
            .or_default()
            .push(entry);
    }
    index
}

/// Evaluate invariant drift using canonical matrix and evidence map.
#[must_use]
#[allow(clippy::too_many_lines)]
pub fn evaluate_invariant_drift_status() -> InvariantDriftStatus {
    let matrix = unit_matrix::build_canonical_matrix();
    let evidence_map = no_mock_evidence::build_evidence_map();
    let evidence_index = index_evidence(&evidence_map.entries);

    let total_matrix_invariants: usize =
        matrix.tests.iter().map(|test| test.invariants.len()).sum();
    let mut gaps = Vec::new();

    for error in matrix.validate() {
        gaps.push(InvariantDriftGap {
            reason: InvariantGapReason::MatrixValidationError,
            severity: InvariantGapSeverity::Required,
            matrix_test_id: None,
            category: None,
            invariant: None,
            details: error,
            remediation: "Fix canonical unit matrix structure/invariants before merging".to_owned(),
        });
    }

    for error in evidence_map.validate() {
        gaps.push(InvariantDriftGap {
            reason: InvariantGapReason::EvidenceValidationError,
            severity: InvariantGapSeverity::Required,
            matrix_test_id: None,
            category: None,
            invariant: None,
            details: error,
            remediation: "Update no-mock evidence map to satisfy evidence schema and invariants"
                .to_owned(),
        });
    }

    let mut critical_invariant_count = 0_usize;
    let mut critical_invariants_with_real_evidence = 0_usize;

    for test in &matrix.tests {
        for invariant in &test.invariants {
            let key = (test.test_id.clone(), invariant.clone());
            let evidence_entries = evidence_index.get(&key).cloned().unwrap_or_default();
            let is_critical = is_critical_category(test.category);
            if is_critical {
                critical_invariant_count = critical_invariant_count.saturating_add(1);
            }

            if evidence_entries.is_empty() {
                gaps.push(InvariantDriftGap {
                    reason: InvariantGapReason::MissingInvariantEvidence,
                    severity: InvariantGapSeverity::Required,
                    matrix_test_id: Some(test.test_id.clone()),
                    category: Some(test.category.display_name().to_owned()),
                    invariant: Some(invariant.clone()),
                    details: "Invariant present in canonical matrix has no evidence entry"
                        .to_owned(),
                    remediation:
                        "Add a no-mock evidence entry linking this invariant to a real test"
                            .to_owned(),
                });
                continue;
            }

            let has_real = evidence_entries
                .iter()
                .any(|entry| !entry.is_exception && !entry.real_components.is_empty());
            if is_critical {
                if has_real {
                    critical_invariants_with_real_evidence =
                        critical_invariants_with_real_evidence.saturating_add(1);
                } else {
                    gaps.push(InvariantDriftGap {
                        reason: InvariantGapReason::MockOnlyCriticalPathInvariant,
                        severity: InvariantGapSeverity::Required,
                        matrix_test_id: Some(test.test_id.clone()),
                        category: Some(test.category.display_name().to_owned()),
                        invariant: Some(invariant.clone()),
                        details: "Critical-path invariant lacks real non-exception evidence"
                            .to_owned(),
                        remediation:
                            "Add or restore real-component evidence for this critical invariant"
                                .to_owned(),
                    });
                }
            }
        }
    }

    gaps.sort_by(|left, right| {
        left.reason
            .cmp(&right.reason)
            .then_with(|| left.matrix_test_id.cmp(&right.matrix_test_id))
            .then_with(|| left.invariant.cmp(&right.invariant))
            .then_with(|| left.details.cmp(&right.details))
    });

    let required_gap_count = gaps.len();
    InvariantDriftStatus {
        schema_version: VALIDATION_MANIFEST_SCHEMA_VERSION.to_owned(),
        gate_id: INVARIANT_DRIFT_GATE_ID.to_owned(),
        unit_matrix_schema_version: matrix.schema_version.clone(),
        evidence_schema_version: evidence_map.schema_version,
        total_matrix_tests: matrix.tests.len(),
        total_matrix_invariants,
        unit_matrix_overall_fill_pct: matrix.overall_fill_pct(),
        total_evidence_entries: evidence_map.entries.len(),
        evidence_coverage_pct: evidence_map.stats.coverage_pct,
        critical_invariant_count,
        critical_invariants_with_real_evidence,
        required_gap_count,
        overall_pass: required_gap_count == 0,
        gaps,
    }
}

#[derive(Debug, Clone)]
struct ScenarioScriptIndex {
    scripts: Vec<String>,
    replay_command: Option<String>,
}

fn build_scenario_script_index() -> BTreeMap<String, ScenarioScriptIndex> {
    let matrix = e2e_traceability::build_canonical_inventory();
    let mut index: BTreeMap<String, ScenarioScriptIndex> = BTreeMap::new();
    for script in matrix.scripts {
        for scenario_id in script.scenario_ids {
            let entry = index
                .entry(scenario_id)
                .or_insert_with(|| ScenarioScriptIndex {
                    scripts: Vec::new(),
                    replay_command: None,
                });
            if !entry.scripts.iter().any(|path| path == &script.path) {
                entry.scripts.push(script.path.clone());
                entry.scripts.sort();
            }
            if entry.replay_command.is_none() {
                entry.replay_command = Some(script.invocation.command.clone());
            }
        }
    }
    index
}

/// Evaluate scenario coverage drift using canonical traceability and orchestrator dry-run output.
///
/// # Errors
///
/// Returns `Err` if dry-run execution summary generation fails.
#[allow(clippy::too_many_lines)]
pub fn evaluate_scenario_coverage_drift_status(
    root_seed: Option<u64>,
) -> Result<ScenarioCoverageDriftStatus, String> {
    let manifest = if let Some(seed) = root_seed {
        build_execution_manifest(seed)
    } else {
        build_default_manifest()
    };

    let coverage_report = e2e_log_schema::build_coverage_report();
    let execution_summary = execute_manifest(
        Path::new("."),
        Path::new("artifacts/validation-manifest-dry-run"),
        &manifest,
        ManifestExecutionMode::DryRun,
    )
    .map_err(|error| format!("scenario_drift_execute_manifest_failed: {error}"))?;

    let required_catalog_scenarios: BTreeMap<String, (ScenarioCriticality, String)> =
        coverage_report
            .scenarios
            .iter()
            .filter(|scenario| {
                matches!(
                    scenario.criticality,
                    ScenarioCriticality::Critical | ScenarioCriticality::Important
                )
            })
            .map(|scenario| {
                (
                    scenario.scenario_id.clone(),
                    (scenario.criticality, scenario.description.clone()),
                )
            })
            .collect();

    let missing_manifest: BTreeSet<String> = execution_summary
        .missing_scenarios
        .iter()
        .cloned()
        .collect();
    let scenario_index = build_scenario_script_index();

    let mut gaps = Vec::new();
    for scenario in &coverage_report.scenarios {
        let is_required = matches!(
            scenario.criticality,
            ScenarioCriticality::Critical | ScenarioCriticality::Important
        );
        if !is_required {
            continue;
        }
        if !scenario.covered {
            gaps.push(ScenarioCoverageGap {
                scenario_id: scenario.scenario_id.clone(),
                reason: ScenarioGapReason::MissingScenarioMapping,
                severity: ScenarioGapSeverity::Required,
                criticality: Some(scenario.criticality),
                description: Some(scenario.description.clone()),
                covering_scripts: scenario.covering_scripts.clone(),
                replay_command: scenario.replay_command.clone(),
            });
            continue;
        }
        if missing_manifest.contains(&scenario.scenario_id) {
            gaps.push(ScenarioCoverageGap {
                scenario_id: scenario.scenario_id.clone(),
                reason: ScenarioGapReason::MissingRequiredExecutionLane,
                severity: ScenarioGapSeverity::Required,
                criticality: Some(scenario.criticality),
                description: Some(scenario.description.clone()),
                covering_scripts: scenario.covering_scripts.clone(),
                replay_command: scenario.replay_command.clone(),
            });
        }
    }

    for missing in &execution_summary.missing_scenarios {
        if required_catalog_scenarios.contains_key(missing) {
            continue;
        }
        let idx = scenario_index.get(missing);
        gaps.push(ScenarioCoverageGap {
            scenario_id: missing.clone(),
            reason: ScenarioGapReason::MissingCatalogEntryForManifestScenario,
            severity: ScenarioGapSeverity::Required,
            criticality: None,
            description: None,
            covering_scripts: idx.map_or_else(Vec::new, |entry| entry.scripts.clone()),
            replay_command: idx.and_then(|entry| entry.replay_command.clone()),
        });
    }

    gaps.sort_by(|left, right| {
        left.scenario_id
            .cmp(&right.scenario_id)
            .then_with(|| left.reason.cmp(&right.reason))
    });

    let required_gap_count = gaps.len();
    Ok(ScenarioCoverageDriftStatus {
        schema_version: VALIDATION_MANIFEST_SCHEMA_VERSION.to_owned(),
        gate_id: SCENARIO_DRIFT_GATE_ID.to_owned(),
        root_seed: manifest.root_seed,
        total_catalog_scenarios: coverage_report.stats.total_scenarios,
        required_catalog_scenarios: required_catalog_scenarios.len(),
        total_manifest_scenarios: manifest.coverage.total_scenario_ids,
        total_manifest_missing: execution_summary.missing_scenarios.len(),
        required_gap_count,
        overall_pass: required_gap_count == 0,
        gaps,
    })
}

/// Validate top-level manifest contract invariants.
#[must_use]
pub fn validate_manifest_contract(manifest: &ValidationManifest) -> Vec<String> {
    let mut errors = validate_manifest_top_level_fields(manifest);
    errors.extend(validate_manifest_replay_contract(manifest));
    errors.extend(validate_manifest_gate_records(manifest));
    errors.extend(validate_manifest_embedded_reports(manifest));
    errors.extend(validate_manifest_gate_artifact_refs(manifest));
    errors
}

fn validate_manifest_top_level_fields(manifest: &ValidationManifest) -> Vec<String> {
    let mut errors = Vec::new();
    if manifest.schema_version.trim().is_empty() {
        errors.push("schema_version must be non-empty".to_owned());
    }
    if manifest.bead_id != BEAD_ID {
        errors.push(format!(
            "bead_id '{}' must equal '{BEAD_ID}'",
            manifest.bead_id
        ));
    }
    if manifest.run_id.trim().is_empty() {
        errors.push("run_id must be non-empty".to_owned());
    }
    if manifest.trace_id.trim().is_empty() {
        errors.push("trace_id must be non-empty".to_owned());
    }
    if manifest.scenario_id.trim().is_empty() {
        errors.push("scenario_id must be non-empty".to_owned());
    }
    if manifest.commit_sha.trim().is_empty() {
        errors.push("commit_sha must be non-empty".to_owned());
    }
    if manifest.gates.is_empty() {
        errors.push("gates must not be empty".to_owned());
    }
    if manifest.artifact_uris.is_empty() {
        errors.push("artifact_uris must not be empty".to_owned());
    }
    if !is_sorted_unique(&manifest.artifact_uris) {
        errors.push("artifact_uris must be sorted and unique".to_owned());
    }
    errors
}

fn validate_manifest_replay_contract(manifest: &ValidationManifest) -> Vec<String> {
    let mut errors = Vec::new();
    if manifest.replay.command.trim().is_empty() {
        errors.push("replay.command must be non-empty".to_owned());
    } else {
        for required_fragment in [
            "validation_manifest_runner",
            "--root-seed",
            "--generated-unix-ms",
            "--commit-sha",
            "--run-id",
            "--trace-id",
            "--scenario-id",
            "--artifact-uri-prefix",
        ] {
            if !manifest.replay.command.contains(required_fragment) {
                errors.push(format!(
                    "replay.command missing required fragment '{}'",
                    required_fragment
                ));
            }
        }
    }
    if manifest.replay.scenario_id != manifest.scenario_id {
        errors.push("replay.scenario_id must equal manifest.scenario_id".to_owned());
    }
    errors
}

fn validate_manifest_gate_records(manifest: &ValidationManifest) -> Vec<String> {
    let mut errors = Vec::new();
    let mut gate_ids = BTreeSet::new();
    let mut previous_gate: Option<&str> = None;

    for gate in &manifest.gates {
        if gate.gate_id.trim().is_empty() {
            errors.push("gate_id must be non-empty".to_owned());
        }
        if gate.gate_family.trim().is_empty() {
            errors.push(format!(
                "gate {} gate_family must be non-empty",
                gate.gate_id
            ));
        }
        if gate.bead_id.trim().is_empty() {
            errors.push(format!("gate {} bead_id must be non-empty", gate.gate_id));
        }
        if gate.summary.trim().is_empty() {
            errors.push(format!("gate {} summary must be non-empty", gate.gate_id));
        }
        if gate.timestamp_unix_ms != manifest.generated_unix_ms {
            errors.push(format!(
                "gate {} timestamp_unix_ms {} must equal generated_unix_ms {}",
                gate.gate_id, gate.timestamp_unix_ms, manifest.generated_unix_ms
            ));
        }
        if gate.commit_sha.trim().is_empty() {
            errors.push(format!(
                "gate {} commit_sha must be non-empty",
                gate.gate_id
            ));
        }
        if gate.commit_sha != manifest.commit_sha {
            errors.push(format!(
                "gate {} commit_sha '{}' must equal manifest commit_sha '{}'",
                gate.gate_id, gate.commit_sha, manifest.commit_sha
            ));
        }
        if gate.artifact_uris.is_empty() {
            errors.push(format!("gate {} must have artifact URIs", gate.gate_id));
        }
        if !is_sorted_unique(&gate.artifact_uris) {
            errors.push(format!(
                "gate {} artifact_uris must be sorted and unique",
                gate.gate_id
            ));
        }
        if !gate_ids.insert(gate.gate_id.clone()) {
            errors.push(format!("duplicate gate_id '{}'", gate.gate_id));
        }
        if let Some(prev) = previous_gate
            && prev > gate.gate_id.as_str()
        {
            errors.push("gates must be sorted by gate_id".to_owned());
        }
        previous_gate = Some(gate.gate_id.as_str());
    }

    errors
}

fn validate_manifest_embedded_reports(manifest: &ValidationManifest) -> Vec<String> {
    let mut errors = Vec::new();
    if manifest.invariant_drift.gate_id != INVARIANT_DRIFT_GATE_ID {
        errors.push("invariant_drift gate_id mismatch".to_owned());
    }
    if manifest.scenario_coverage_drift.gate_id != SCENARIO_DRIFT_GATE_ID {
        errors.push("scenario_coverage_drift gate_id mismatch".to_owned());
    }
    if manifest.logging_conformance.gate_id != LOGGING_GATE_ID {
        errors.push("logging_conformance gate_id mismatch".to_owned());
    }
    if manifest
        .logging_conformance
        .shell_script_conformance
        .bead_id
        != "bd-mblr.5.5"
    {
        errors.push("logging_conformance shell_script_conformance bead_id mismatch".to_owned());
    }
    if manifest.logging_conformance.overall_pass
        && !manifest
            .logging_conformance
            .shell_script_conformance
            .overall_pass
    {
        errors.push(
            "logging_conformance overall_pass inconsistent with shell_script_conformance"
                .to_owned(),
        );
    }
    errors
}

fn validate_manifest_gate_artifact_refs(manifest: &ValidationManifest) -> Vec<String> {
    let artifact_set = manifest
        .artifact_uris
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    let mut errors = Vec::new();

    for gate in &manifest.gates {
        for uri in &gate.artifact_uris {
            if !artifact_set.contains(uri) {
                errors.push(format!(
                    "gate '{}' references missing artifact URI '{}'",
                    gate.gate_id, uri
                ));
            }
        }
    }

    errors
}

/// Detect backward-incompatible changes between two manifest instances.
///
/// This checks major-version jumps and required gate removal.
#[must_use]
pub fn detect_backward_incompatible_change(
    previous: &ValidationManifest,
    candidate: &ValidationManifest,
) -> Vec<String> {
    let mut issues = Vec::new();
    let previous_major = parse_major(&previous.schema_version);
    let candidate_major = parse_major(&candidate.schema_version);

    match (previous_major, candidate_major) {
        (Some(prev), Some(next)) => {
            if next != prev {
                issues.push(format!(
                    "schema major changed from {prev} to {next}; treat as backward-incompatible"
                ));
            }
        }
        _ => issues.push("unable to parse schema_version major component".to_owned()),
    }

    let previous_gates = previous
        .gates
        .iter()
        .map(|gate| gate.gate_id.clone())
        .collect::<BTreeSet<_>>();
    let candidate_gates = candidate
        .gates
        .iter()
        .map(|gate| gate.gate_id.clone())
        .collect::<BTreeSet<_>>();
    for missing in previous_gates.difference(&candidate_gates) {
        issues.push(format!(
            "candidate manifest removed required gate '{}'",
            missing
        ));
    }

    issues
}

fn parse_major(version: &str) -> Option<u64> {
    version
        .split('.')
        .next()
        .and_then(|major| major.parse::<u64>().ok())
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn fixed_config() -> ValidationManifestConfig {
        ValidationManifestConfig {
            commit_sha: "0123456789abcdef".to_owned(),
            run_id: "bd-mblr.3.5.1-seed-424242".to_owned(),
            trace_id: "trace-424242".to_owned(),
            scenario_id: VALIDATION_MANIFEST_SCENARIO_ID.to_owned(),
            generated_unix_ms: 1_700_000_000_000,
            root_seed: Some(424_242),
            artifact_uri_prefix: "artifacts/validation-manifest".to_owned(),
        }
    }

    #[test]
    fn bundle_builds_with_required_gate_families() {
        let bundle = build_validation_manifest_bundle(&fixed_config())
            .unwrap_or_else(|error| panic!("bundle build failed: {error}"));
        let gate_ids = bundle
            .manifest
            .gates
            .iter()
            .map(|gate| gate.gate_id.as_str())
            .collect::<Vec<_>>();
        assert!(gate_ids.contains(&COVERAGE_GATE_ID));
        assert!(gate_ids.contains(&INVARIANT_DRIFT_GATE_ID));
        assert!(gate_ids.contains(&SCENARIO_DRIFT_GATE_ID));
        assert!(gate_ids.contains(&NO_MOCK_GATE_ID));
        assert!(gate_ids.contains(&LOGGING_GATE_ID));
    }

    #[test]
    fn bundle_is_deterministic_for_same_inputs() {
        let a = build_validation_manifest_bundle(&fixed_config())
            .unwrap_or_else(|error| panic!("bundle A failed: {error}"));
        let b = build_validation_manifest_bundle(&fixed_config())
            .unwrap_or_else(|error| panic!("bundle B failed: {error}"));
        let json_a = a
            .manifest
            .to_json()
            .unwrap_or_else(|error| panic!("serialize A failed: {error}"));
        let json_b = b
            .manifest
            .to_json()
            .unwrap_or_else(|error| panic!("serialize B failed: {error}"));
        assert_eq!(json_a, json_b);
    }

    #[test]
    fn contract_validator_flags_missing_required_fields() {
        let bundle = build_validation_manifest_bundle(&fixed_config())
            .unwrap_or_else(|error| panic!("bundle build failed: {error}"));
        let mut manifest = bundle.manifest;
        manifest.commit_sha.clear();
        manifest.gates.clear();
        manifest.artifact_uris.clear();
        let errors = validate_manifest_contract(&manifest);
        assert!(errors.iter().any(|error| error.contains("commit_sha")));
        assert!(errors.iter().any(|error| error.contains("gates")));
        assert!(errors.iter().any(|error| error.contains("artifact_uris")));
    }

    #[test]
    fn contract_validator_flags_shell_conformance_bead_id_mismatch() {
        let bundle = build_validation_manifest_bundle(&fixed_config())
            .unwrap_or_else(|error| panic!("bundle build failed: {error}"));
        let mut manifest = bundle.manifest;
        manifest
            .logging_conformance
            .shell_script_conformance
            .bead_id = "bd-mblr.invalid".to_owned();
        let errors = validate_manifest_contract(&manifest);
        assert!(errors.iter().any(|error| {
            error.contains("logging_conformance shell_script_conformance bead_id mismatch")
        }));
    }

    #[test]
    fn contract_validator_flags_shell_conformance_pass_inconsistency() {
        let bundle = build_validation_manifest_bundle(&fixed_config())
            .unwrap_or_else(|error| panic!("bundle build failed: {error}"));
        let mut manifest = bundle.manifest;
        manifest.logging_conformance.overall_pass = true;
        manifest
            .logging_conformance
            .shell_script_conformance
            .overall_pass = false;
        let errors = validate_manifest_contract(&manifest);
        assert!(errors.iter().any(|error| {
            error.contains(
                "logging_conformance overall_pass inconsistent with shell_script_conformance",
            )
        }));
    }

    #[test]
    fn contract_validator_flags_gate_commit_sha_mismatch() {
        let bundle = build_validation_manifest_bundle(&fixed_config())
            .unwrap_or_else(|error| panic!("bundle build failed: {error}"));
        let mut manifest = bundle.manifest;
        manifest.gates[0].commit_sha = "mismatch".to_owned();
        let errors = validate_manifest_contract(&manifest);
        assert!(errors.iter().any(|error| {
            error.contains("must equal manifest commit_sha")
                && error.contains(&manifest.gates[0].gate_id)
        }));
    }

    #[test]
    fn contract_validator_flags_gate_timestamp_mismatch() {
        let bundle = build_validation_manifest_bundle(&fixed_config())
            .unwrap_or_else(|error| panic!("bundle build failed: {error}"));
        let mut manifest = bundle.manifest;
        manifest.gates[0].timestamp_unix_ms = manifest.generated_unix_ms.saturating_add(1);
        let errors = validate_manifest_contract(&manifest);
        assert!(errors.iter().any(|error| {
            error.contains("timestamp_unix_ms") && error.contains(&manifest.gates[0].gate_id)
        }));
    }

    #[test]
    fn contract_validator_flags_unsorted_artifact_uris() {
        let bundle = build_validation_manifest_bundle(&fixed_config())
            .unwrap_or_else(|error| panic!("bundle build failed: {error}"));
        let mut manifest = bundle.manifest;
        let gate_id = manifest.gates[0].gate_id.clone();
        manifest.artifact_uris = vec!["z.json".to_owned(), "a.json".to_owned()];
        manifest.gates[0].artifact_uris = vec!["b.json".to_owned(), "a.json".to_owned()];
        let errors = validate_manifest_contract(&manifest);
        assert!(
            errors
                .iter()
                .any(|error| error.contains("artifact_uris must be sorted and unique"))
        );
        assert!(errors.iter().any(|error| {
            error.contains("artifact_uris must be sorted and unique") && error.contains(&gate_id)
        }));
    }

    #[test]
    fn replay_command_contains_required_fragments() {
        let bundle = build_validation_manifest_bundle(&fixed_config())
            .unwrap_or_else(|error| panic!("bundle build failed: {error}"));
        let replay = &bundle.manifest.replay.command;
        for required_fragment in [
            "validation_manifest_runner",
            "--root-seed",
            "--generated-unix-ms",
            "--commit-sha",
            "--run-id",
            "--trace-id",
            "--scenario-id",
            "--artifact-uri-prefix",
        ] {
            assert!(
                replay.contains(required_fragment),
                "missing required replay fragment: {required_fragment}"
            );
        }
        assert!(
            replay.contains("'artifacts/validation-manifest'"),
            "artifact uri prefix should be shell-quoted in replay command"
        );
    }

    #[test]
    fn replay_command_shell_quotes_dynamic_values() {
        let config = ValidationManifestConfig {
            commit_sha: "sha with spaces and 'quote'".to_owned(),
            run_id: "run id with spaces".to_owned(),
            trace_id: "trace'xyz".to_owned(),
            scenario_id: "QUALITY 'SCENARIO'".to_owned(),
            generated_unix_ms: 1_700_000_000_111,
            root_seed: Some(7),
            artifact_uri_prefix: "artifacts/manifest special".to_owned(),
        };
        let bundle = build_validation_manifest_bundle(&config)
            .unwrap_or_else(|error| panic!("bundle build failed: {error}"));
        let replay = &bundle.manifest.replay.command;

        assert!(replay.contains("'sha with spaces and '\"'\"'quote'\"'\"''"));
        assert!(replay.contains("'run id with spaces'"));
        assert!(replay.contains("'trace'\"'\"'xyz'"));
        assert!(replay.contains("'QUALITY '\"'\"'SCENARIO'\"'\"''"));
        assert!(replay.contains("'artifacts/manifest special'"));
    }

    #[test]
    fn backward_compatibility_check_flags_major_change() {
        let bundle = build_validation_manifest_bundle(&fixed_config())
            .unwrap_or_else(|error| panic!("bundle build failed: {error}"));
        let previous = bundle.manifest.clone();
        let mut candidate = bundle.manifest;
        candidate.schema_version = "2.0.0".to_owned();
        let issues = detect_backward_incompatible_change(&previous, &candidate);
        assert!(issues.iter().any(|issue| issue.contains("schema major")));
    }

    #[test]
    fn backward_compatibility_check_flags_removed_gate() {
        let bundle = build_validation_manifest_bundle(&fixed_config())
            .unwrap_or_else(|error| panic!("bundle build failed: {error}"));
        let previous = bundle.manifest.clone();
        let mut candidate = bundle.manifest;
        candidate
            .gates
            .retain(|gate| gate.gate_id != LOGGING_GATE_ID);
        let issues = detect_backward_incompatible_change(&previous, &candidate);
        assert!(issues.iter().any(|issue| issue.contains(LOGGING_GATE_ID)));
    }

    proptest! {
        #[test]
        fn unique_sorted_helper_is_deterministic(values in proptest::collection::vec("[a-z]{1,6}", 0..32)) {
            let input = values.iter().map(std::string::ToString::to_string).collect::<Vec<_>>();
            let mut expected = input.clone();
            expected.sort();
            expected.dedup();
            let normalized = unique_sorted_strings(input);
            prop_assert_eq!(normalized, expected);
        }
    }
}
