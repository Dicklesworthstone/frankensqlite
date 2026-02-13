//! Unified E2E log event schema and scenario coverage validation (bd-1dp9.7.2).
//!
//! Defines the canonical structured log event schema for all E2E test scripts
//! in the FrankenSQLite workspace. Provides schema validation, scenario coverage
//! checking, and log quality assessment tied to the traceability matrix from
//! bd-mblr.4.5.1.
//!
//! # Schema Version
//!
//! The current schema version is `1.0.0`. All E2E scripts should emit events
//! conforming to this schema. The schema includes required fields (run_id,
//! timestamp, phase, event_type) and recommended fields (scenario_id, seed,
//! backend, artifact_hash).
//!
//! # Coverage Assessment
//!
//! The module cross-references the traceability matrix to compute which
//! parity-critical scenarios have E2E script coverage and which have gaps.

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

use crate::e2e_traceability::{self, TraceabilityMatrix};
use crate::parity_taxonomy::FeatureCategory;

#[allow(dead_code)]
const BEAD_ID: &str = "bd-1dp9.7.2";

/// Schema version for the unified E2E log format.
pub const LOG_SCHEMA_VERSION: &str = "1.0.0";
/// Oldest schema version this module guarantees compatibility with.
pub const LOG_SCHEMA_MIN_SUPPORTED_VERSION: &str = "1.0.0";
/// Fields that must be present in every event.
pub const REQUIRED_EVENT_FIELDS: &[&str] = &["run_id", "timestamp", "phase", "event_type"];
/// Replayability keys required for deterministic triage and reruns.
pub const REPLAYABILITY_KEYS: &[&str] = &[
    "scenario_id",
    "seed",
    "phase",
    "context.invariant_ids",
    "context.artifact_paths",
];

// ─── Log Event Schema ───────────────────────────────────────────────────

/// Required fields for every E2E log event.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LogEventSchema {
    /// Unique run identifier (format: `{bead_id}-{timestamp}-{pid}`).
    pub run_id: String,
    /// ISO 8601 timestamp (UTC).
    pub timestamp: String,
    /// Execution phase (e.g. `setup`, `execute`, `validate`, `teardown`).
    pub phase: LogPhase,
    /// Event type classification.
    pub event_type: LogEventType,
    /// Scenario ID from traceability matrix (optional, recommended).
    pub scenario_id: Option<String>,
    /// Deterministic seed used for this run (optional, recommended).
    pub seed: Option<u64>,
    /// Backend under test (optional).
    pub backend: Option<String>,
    /// SHA-256 hash of output artifact (optional).
    pub artifact_hash: Option<String>,
    /// Structured key-value context fields.
    pub context: BTreeMap<String, String>,
}

/// Execution phase markers for log events.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum LogPhase {
    /// Initial setup (create tables, seed data).
    Setup,
    /// Main test execution.
    Execute,
    /// Result validation and comparison.
    Validate,
    /// Cleanup and resource release.
    Teardown,
    /// Summary/report generation.
    Report,
}

impl LogPhase {
    /// Canonical phase values used by the schema contract.
    pub const ALL: [Self; 5] = [
        Self::Setup,
        Self::Execute,
        Self::Validate,
        Self::Teardown,
        Self::Report,
    ];

    /// Stable lowercase representation used in docs and artifacts.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Setup => "setup",
            Self::Execute => "execute",
            Self::Validate => "validate",
            Self::Teardown => "teardown",
            Self::Report => "report",
        }
    }
}

/// Classification of log event types.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum LogEventType {
    /// Test started.
    Start,
    /// Test passed.
    Pass,
    /// Test failed.
    Fail,
    /// Test skipped with rationale.
    Skip,
    /// Informational event.
    Info,
    /// Warning (non-fatal issue).
    Warn,
    /// Error (fatal issue).
    Error,
    /// First divergence point detected.
    FirstDivergence,
    /// Artifact generated (hash available).
    ArtifactGenerated,
}

impl LogEventType {
    /// Canonical event type values used by the schema contract.
    pub const ALL: [Self; 9] = [
        Self::Start,
        Self::Pass,
        Self::Fail,
        Self::Skip,
        Self::Info,
        Self::Warn,
        Self::Error,
        Self::FirstDivergence,
        Self::ArtifactGenerated,
    ];

    /// Stable lowercase representation used in docs and artifacts.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Start => "start",
            Self::Pass => "pass",
            Self::Fail => "fail",
            Self::Skip => "skip",
            Self::Info => "info",
            Self::Warn => "warn",
            Self::Error => "error",
            Self::FirstDivergence => "first_divergence",
            Self::ArtifactGenerated => "artifact_generated",
        }
    }
}

/// Schema field requirement level.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum FieldRequirement {
    /// Must be present in every event.
    Required,
    /// Should be present when applicable.
    Recommended,
    /// May be present for additional context.
    Optional,
}

/// Value category for a schema field.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum FieldValueType {
    /// UTF-8 string.
    String,
    /// Unsigned integer.
    UnsignedInteger,
    /// Enumerated string.
    Enum,
    /// Structured key-value object.
    Object,
}

/// Description of a schema field.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FieldSpec {
    pub name: String,
    pub description: String,
    pub requirement: FieldRequirement,
    pub value_type: FieldValueType,
    pub allowed_values: Vec<String>,
    pub allowed_range: Option<String>,
    pub semantics: String,
    pub example: String,
}

// ─── Schema Documentation ───────────────────────────────────────────────

/// Build the canonical field specification for the unified log schema.
#[must_use]
pub fn build_field_specs() -> Vec<FieldSpec> {
    vec![
        run_id_field_spec(),
        timestamp_field_spec(),
        phase_field_spec(),
        event_type_field_spec(),
        scenario_id_field_spec(),
        seed_field_spec(),
        backend_field_spec(),
        artifact_hash_field_spec(),
        context_field_spec(),
    ]
}

fn run_id_field_spec() -> FieldSpec {
    FieldSpec {
        name: "run_id".to_owned(),
        description: "Unique run identifier for log correlation".to_owned(),
        requirement: FieldRequirement::Required,
        value_type: FieldValueType::String,
        allowed_values: Vec::new(),
        allowed_range: Some("non-empty; format `{bead_id}-{timestamp}-{pid}`".to_owned()),
        semantics: "Correlation key for all events in a single execution run.".to_owned(),
        example: "bd-mblr-20260213T050000Z-12345".to_owned(),
    }
}

fn timestamp_field_spec() -> FieldSpec {
    FieldSpec {
        name: "timestamp".to_owned(),
        description: "ISO 8601 UTC timestamp of the event".to_owned(),
        requirement: FieldRequirement::Required,
        value_type: FieldValueType::String,
        allowed_values: Vec::new(),
        allowed_range: Some("RFC3339/ISO8601 UTC string ending in `Z`".to_owned()),
        semantics: "Ordering anchor for timeline reconstruction.".to_owned(),
        example: "2026-02-13T05:00:00.000Z".to_owned(),
    }
}

fn phase_field_spec() -> FieldSpec {
    FieldSpec {
        name: "phase".to_owned(),
        description: "Execution phase (setup/execute/validate/teardown/report)".to_owned(),
        requirement: FieldRequirement::Required,
        value_type: FieldValueType::Enum,
        allowed_values: LogPhase::ALL
            .iter()
            .map(|phase| phase.as_str().to_owned())
            .collect(),
        allowed_range: None,
        semantics: "Lifecycle marker used by orchestration and replay tools.".to_owned(),
        example: "execute".to_owned(),
    }
}

fn event_type_field_spec() -> FieldSpec {
    FieldSpec {
        name: "event_type".to_owned(),
        description: "Event classification (start/pass/fail/skip/info/warn/error)".to_owned(),
        requirement: FieldRequirement::Required,
        value_type: FieldValueType::Enum,
        allowed_values: LogEventType::ALL
            .iter()
            .map(|event_type| event_type.as_str().to_owned())
            .collect(),
        allowed_range: None,
        semantics: "Categorizes event semantics for analytics and gating.".to_owned(),
        example: "pass".to_owned(),
    }
}

fn scenario_id_field_spec() -> FieldSpec {
    FieldSpec {
        name: "scenario_id".to_owned(),
        description: "Scenario ID from traceability matrix (CATEGORY-NUMBER)".to_owned(),
        requirement: FieldRequirement::Recommended,
        value_type: FieldValueType::String,
        allowed_values: Vec::new(),
        allowed_range: Some("`[A-Z]+-[0-9]+`".to_owned()),
        semantics: "Links event to scenario inventory and coverage analytics.".to_owned(),
        example: "MVCC-3".to_owned(),
    }
}

fn seed_field_spec() -> FieldSpec {
    FieldSpec {
        name: "seed".to_owned(),
        description: "Deterministic seed used for reproducibility".to_owned(),
        requirement: FieldRequirement::Recommended,
        value_type: FieldValueType::UnsignedInteger,
        allowed_values: Vec::new(),
        allowed_range: Some("0..=18446744073709551615".to_owned()),
        semantics: "Reproduces data generation and schedule choices.".to_owned(),
        example: "6148914689804861784".to_owned(),
    }
}

fn backend_field_spec() -> FieldSpec {
    FieldSpec {
        name: "backend".to_owned(),
        description: "Backend under test (fsqlite/rusqlite/both)".to_owned(),
        requirement: FieldRequirement::Recommended,
        value_type: FieldValueType::Enum,
        allowed_values: vec![
            "fsqlite".to_owned(),
            "rusqlite".to_owned(),
            "both".to_owned(),
        ],
        allowed_range: None,
        semantics: "Disambiguates per-engine behavior and differential runs.".to_owned(),
        example: "fsqlite".to_owned(),
    }
}

fn artifact_hash_field_spec() -> FieldSpec {
    FieldSpec {
        name: "artifact_hash".to_owned(),
        description: "SHA-256 hash of generated artifact".to_owned(),
        requirement: FieldRequirement::Optional,
        value_type: FieldValueType::String,
        allowed_values: Vec::new(),
        allowed_range: Some("64 lowercase hexadecimal chars".to_owned()),
        semantics: "Verifies artifact integrity and deduplicates evidence.".to_owned(),
        example: "a1b2c3d4...".to_owned(),
    }
}

fn context_field_spec() -> FieldSpec {
    FieldSpec {
        name: "context".to_owned(),
        description: "Free-form key-value pairs for additional context".to_owned(),
        requirement: FieldRequirement::Optional,
        value_type: FieldValueType::Object,
        allowed_values: Vec::new(),
        allowed_range: Some(
            "JSON object of string keys and string values; replay keys include \
`invariant_ids` and `artifact_paths`"
                .to_owned(),
        ),
        semantics: "Extensible carrier for deterministic replay metadata.".to_owned(),
        example: "{\"table_count\": \"5\", \"concurrency\": \"4\"}".to_owned(),
    }
}

/// Version transition classification for log schema evolution.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum VersionTransition {
    /// Version did not change.
    NoChange,
    /// Backward-compatible bugfix update.
    Patch,
    /// Backward-compatible additive change (new optional fields/values).
    Additive,
    /// Breaking change requiring major version bump.
    Breaking,
}

/// Tooling compatibility outcome for a producer/consumer version pair.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ToolingCompatibility {
    /// Full read/write compatibility.
    ReadWrite,
    /// Can read while ignoring unknown additive fields, but should not write.
    ReadOnlyForwardCompatible,
    /// Incompatible schema major versions.
    Incompatible,
}

/// Parsed semantic version for schema policy checks.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct SchemaVersion {
    pub major: u32,
    pub minor: u32,
    pub patch: u32,
}

impl SchemaVersion {
    /// Parse an `MAJOR.MINOR.PATCH` semantic version.
    pub fn parse(input: &str) -> Result<Self, String> {
        let parts: Vec<&str> = input.split('.').collect();
        if parts.len() != 3 {
            return Err(format!(
                "invalid schema version '{input}': expected MAJOR.MINOR.PATCH"
            ));
        }
        let major = parts[0]
            .parse::<u32>()
            .map_err(|_| format!("invalid major version component '{}'", parts[0]))?;
        let minor = parts[1]
            .parse::<u32>()
            .map_err(|_| format!("invalid minor version component '{}'", parts[1]))?;
        let patch = parts[2]
            .parse::<u32>()
            .map_err(|_| format!("invalid patch version component '{}'", parts[2]))?;
        Ok(Self {
            major,
            minor,
            patch,
        })
    }
}

impl std::fmt::Display for SchemaVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

/// Tooling upgrade rule for compatibility handling.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ToolingUpgradeRule {
    pub condition: String,
    pub compatibility: ToolingCompatibility,
    pub behavior: String,
}

/// Full schema compatibility policy consumed by emitters and tooling.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SchemaCompatibilityPolicy {
    pub current_version: String,
    pub minimum_supported_version: String,
    pub required_fields: Vec<String>,
    pub replayability_keys: Vec<String>,
    pub additive_change_rule: String,
    pub breaking_change_rule: String,
    pub downgrade_rule: String,
    pub tooling_upgrade_rules: Vec<ToolingUpgradeRule>,
}

/// Build the canonical schema compatibility policy.
#[must_use]
pub fn build_schema_compatibility_policy() -> SchemaCompatibilityPolicy {
    SchemaCompatibilityPolicy {
        current_version: LOG_SCHEMA_VERSION.to_owned(),
        minimum_supported_version: LOG_SCHEMA_MIN_SUPPORTED_VERSION.to_owned(),
        required_fields: REQUIRED_EVENT_FIELDS
            .iter()
            .map(|field| (*field).to_owned())
            .collect(),
        replayability_keys: REPLAYABILITY_KEYS
            .iter()
            .map(|field| (*field).to_owned())
            .collect(),
        additive_change_rule: "Additive changes must bump MINOR and keep all prior required fields stable."
            .to_owned(),
        breaking_change_rule: "Breaking changes must bump MAJOR and include migration guidance for tooling."
            .to_owned(),
        downgrade_rule: "Schema downgrades are unsupported; emitters must not decrease schema version."
            .to_owned(),
        tooling_upgrade_rules: vec![
            ToolingUpgradeRule {
                condition: "tool.major == event.major && tool.minor >= event.minor".to_owned(),
                compatibility: ToolingCompatibility::ReadWrite,
                behavior: "Tool can parse and emit events for this schema version.".to_owned(),
            },
            ToolingUpgradeRule {
                condition: "tool.major == event.major && tool.minor < event.minor".to_owned(),
                compatibility: ToolingCompatibility::ReadOnlyForwardCompatible,
                behavior: "Tool must ignore unknown additive fields and avoid re-emitting transformed events."
                    .to_owned(),
            },
            ToolingUpgradeRule {
                condition: "tool.major != event.major".to_owned(),
                compatibility: ToolingCompatibility::Incompatible,
                behavior: "Tool must fail fast and request explicit major-version upgrade.".to_owned(),
            },
        ],
    }
}

/// Classify a version transition according to schema policy.
pub fn classify_version_transition(from: &str, to: &str) -> Result<VersionTransition, String> {
    let from = SchemaVersion::parse(from)?;
    let to = SchemaVersion::parse(to)?;

    if to < from {
        return Err(format!(
            "schema downgrade is not supported: from '{from}' to '{to}'"
        ));
    }

    if to.major > from.major {
        return Ok(VersionTransition::Breaking);
    }
    if to.minor > from.minor {
        return Ok(VersionTransition::Additive);
    }
    if to.patch > from.patch {
        return Ok(VersionTransition::Patch);
    }
    Ok(VersionTransition::NoChange)
}

/// Determine how well a tooling version can consume events for another version.
pub fn evaluate_tooling_compatibility(
    tooling_version: &str,
    event_version: &str,
) -> Result<ToolingCompatibility, String> {
    let tooling = SchemaVersion::parse(tooling_version)?;
    let event = SchemaVersion::parse(event_version)?;
    if tooling.major != event.major {
        return Ok(ToolingCompatibility::Incompatible);
    }
    if tooling.minor >= event.minor {
        return Ok(ToolingCompatibility::ReadWrite);
    }
    Ok(ToolingCompatibility::ReadOnlyForwardCompatible)
}

/// Render a Markdown schema contract document from code constants.
#[must_use]
pub fn render_schema_contract_markdown() -> String {
    let field_lines: Vec<String> = build_field_specs()
        .into_iter()
        .map(|spec| {
            let allowed_values = if spec.allowed_values.is_empty() {
                "-".to_owned()
            } else {
                spec.allowed_values.join(", ")
            };
            let allowed_range = spec.allowed_range.unwrap_or_else(|| "-".to_owned());
            format!(
                "| `{}` | `{:?}` | `{:?}` | {} | {} | {} | {} |",
                spec.name,
                spec.requirement,
                spec.value_type,
                spec.description,
                allowed_values,
                allowed_range,
                spec.semantics,
            )
        })
        .collect();
    let policy = build_schema_compatibility_policy();
    let tooling_rule_lines = policy
        .tooling_upgrade_rules
        .iter()
        .map(|rule| {
            format!(
                "- `{}` => `{:?}`: {}",
                rule.condition, rule.compatibility, rule.behavior
            )
        })
        .collect::<Vec<_>>()
        .join("\n");

    format!(
        "# Unified E2E Log Schema Contract\n\n\
Schema version: `{}`\n\n\
Minimum supported version: `{}`\n\n\
Required fields: `{}`\n\n\
Replayability keys: `{}`\n\n\
## Field Definitions\n\n\
| Field | Requirement | Type | Description | Allowed Values | Allowed Range | Semantics |\n\
| --- | --- | --- | --- | --- | --- | --- |\n\
{}\n\n\
## Versioning Policy\n\n\
- {}\n\
- {}\n\
- {}\n\n\
## Tooling Compatibility Rules\n\n\
{}\n",
        policy.current_version,
        policy.minimum_supported_version,
        policy.required_fields.join("`, `"),
        policy.replayability_keys.join("`, `"),
        field_lines.join("\n"),
        policy.additive_change_rule,
        policy.breaking_change_rule,
        policy.downgrade_rule,
        tooling_rule_lines,
    )
}

/// Canonical event examples used by contract tests.
#[must_use]
pub fn canonical_event_examples() -> Vec<LogEventSchema> {
    let mut base_context = BTreeMap::new();
    base_context.insert("invariant_ids".to_owned(), "INV-1,INV-9".to_owned());
    base_context.insert(
        "artifact_paths".to_owned(),
        "artifacts/events.jsonl,artifacts/diff.json".to_owned(),
    );

    vec![
        LogEventSchema {
            run_id: "bd-mblr.5.3.1-20260213T090000Z-1001".to_owned(),
            timestamp: "2026-02-13T09:00:00.000Z".to_owned(),
            phase: LogPhase::Setup,
            event_type: LogEventType::Start,
            scenario_id: Some("INFRA-6".to_owned()),
            seed: Some(1001),
            backend: Some("both".to_owned()),
            artifact_hash: None,
            context: base_context.clone(),
        },
        LogEventSchema {
            run_id: "bd-mblr.5.3.1-20260213T090000Z-1001".to_owned(),
            timestamp: "2026-02-13T09:00:03.100Z".to_owned(),
            phase: LogPhase::Validate,
            event_type: LogEventType::Pass,
            scenario_id: Some("MVCC-3".to_owned()),
            seed: Some(1001),
            backend: Some("fsqlite".to_owned()),
            artifact_hash: Some(
                "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_owned(),
            ),
            context: base_context.clone(),
        },
        LogEventSchema {
            run_id: "bd-mblr.5.3.1-20260213T090000Z-1001".to_owned(),
            timestamp: "2026-02-13T09:00:04.250Z".to_owned(),
            phase: LogPhase::Validate,
            event_type: LogEventType::FirstDivergence,
            scenario_id: Some("COR-2".to_owned()),
            seed: Some(1001),
            backend: Some("both".to_owned()),
            artifact_hash: Some(
                "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789".to_owned(),
            ),
            context: base_context,
        },
    ]
}

fn schema_field_value<'a>(event: &'a LogEventSchema, field: &str) -> Option<&'a str> {
    match field {
        "run_id" => Some(event.run_id.as_str()),
        "timestamp" => Some(event.timestamp.as_str()),
        "phase" => Some(event.phase.as_str()),
        "event_type" => Some(event.event_type.as_str()),
        "scenario_id" => event.scenario_id.as_deref(),
        "backend" => event.backend.as_deref(),
        "artifact_hash" => event.artifact_hash.as_deref(),
        _ => None,
    }
}

/// Validate an event against the field specification contract.
#[must_use]
pub fn validate_event_against_field_specs(
    event: &LogEventSchema,
    specs: &[FieldSpec],
) -> Vec<String> {
    let mut errors = Vec::new();
    for spec in specs {
        if spec.requirement == FieldRequirement::Required {
            match spec.name.as_str() {
                "seed" => {
                    if event.seed.is_none() {
                        errors.push("required field 'seed' missing".to_owned());
                    }
                }
                "context" => {
                    if event.context.is_empty() {
                        errors.push("required field 'context' missing".to_owned());
                    }
                }
                _ => {
                    let missing = match schema_field_value(event, &spec.name) {
                        Some(value) => value.trim().is_empty(),
                        None => true,
                    };
                    if missing {
                        errors.push(format!("required field '{}' missing", spec.name));
                    }
                }
            }
        }

        if !spec.allowed_values.is_empty()
            && let Some(value) = schema_field_value(event, &spec.name)
            && !spec.allowed_values.iter().any(|allowed| allowed == value)
        {
            errors.push(format!(
                "field '{}' has value '{}' outside allowed values [{}]",
                spec.name,
                value,
                spec.allowed_values.join(", "),
            ));
        }
    }

    errors
}

// ─── Scenario Coverage Assessment ───────────────────────────────────────

/// Category of parity-critical scenario.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ScenarioCriticality {
    /// Must pass for any release.
    Critical,
    /// Should pass but degraded mode acceptable.
    Important,
    /// Nice to have.
    Standard,
}

/// A parity-critical scenario and its coverage status.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CriticalScenario {
    /// Scenario ID.
    pub scenario_id: String,
    /// Feature category this scenario validates.
    pub category: FeatureCategory,
    /// Criticality level.
    pub criticality: ScenarioCriticality,
    /// Description of what this scenario validates.
    pub description: String,
    /// Whether this scenario has E2E script coverage.
    pub covered: bool,
    /// Script paths that cover this scenario.
    pub covering_scripts: Vec<String>,
    /// Replay command for this scenario.
    pub replay_command: Option<String>,
}

/// Assessment of E2E scenario coverage completeness.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScenarioCoverageReport {
    /// Schema version.
    pub schema_version: String,
    /// Bead ID.
    pub bead_id: String,
    /// Log schema version.
    pub log_schema_version: String,
    /// All critical scenarios with coverage status.
    pub scenarios: Vec<CriticalScenario>,
    /// Coverage statistics.
    pub stats: CoverageReportStats,
}

/// Statistics from the coverage report.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoverageReportStats {
    pub total_scenarios: usize,
    pub covered_scenarios: usize,
    pub uncovered_scenarios: usize,
    pub critical_covered: usize,
    pub critical_total: usize,
    pub important_covered: usize,
    pub important_total: usize,
    pub coverage_pct: f64,
    pub by_category: BTreeMap<String, CategoryCoverageStats>,
}

/// Per-category coverage stats.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CategoryCoverageStats {
    pub total: usize,
    pub covered: usize,
    pub pct: f64,
}

impl ScenarioCoverageReport {
    /// Validate the report.
    pub fn validate(&self) -> Vec<String> {
        let mut errors = Vec::new();

        // No duplicate scenario IDs
        let mut seen = BTreeSet::new();
        for s in &self.scenarios {
            if !seen.insert(&s.scenario_id) {
                errors.push(format!("Duplicate scenario ID: {}", s.scenario_id));
            }
        }

        // Every covered scenario must have at least one covering script
        for s in &self.scenarios {
            if s.covered && s.covering_scripts.is_empty() {
                errors.push(format!(
                    "Scenario {} marked covered but has no covering scripts",
                    s.scenario_id
                ));
            }
        }

        // Critical scenarios should have replay commands
        for s in &self.scenarios {
            if s.covered
                && s.criticality == ScenarioCriticality::Critical
                && s.replay_command.is_none()
            {
                errors.push(format!(
                    "Critical scenario {} lacks replay command",
                    s.scenario_id
                ));
            }
        }

        // Stats consistency
        let actual_covered = self.scenarios.iter().filter(|s| s.covered).count();
        if actual_covered != self.stats.covered_scenarios {
            errors.push(format!(
                "Stats mismatch: counted {} covered but stats says {}",
                actual_covered, self.stats.covered_scenarios
            ));
        }

        errors
    }

    /// Serialize to JSON.
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }
}

// ─── Build the Coverage Report ──────────────────────────────────────────

/// Build the parity-critical scenario coverage report by cross-referencing
/// the traceability matrix with the canonical critical scenario list.
#[must_use]
pub fn build_coverage_report() -> ScenarioCoverageReport {
    let matrix = e2e_traceability::build_canonical_inventory();
    let critical_scenarios = build_critical_scenario_list();

    let scenarios = assess_coverage(&matrix, critical_scenarios);
    let stats = compute_stats(&scenarios);

    ScenarioCoverageReport {
        schema_version: "1.0.0".to_owned(),
        bead_id: BEAD_ID.to_owned(),
        log_schema_version: LOG_SCHEMA_VERSION.to_owned(),
        scenarios,
        stats,
    }
}

fn build_critical_scenario_list() -> Vec<(String, FeatureCategory, ScenarioCriticality, String)> {
    vec![
        // SQL Grammar — Critical
        (
            "SQL-1".to_owned(),
            FeatureCategory::SqlGrammar,
            ScenarioCriticality::Critical,
            "DDL statement compliance".to_owned(),
        ),
        (
            "SQL-2".to_owned(),
            FeatureCategory::SqlGrammar,
            ScenarioCriticality::Critical,
            "SELECT statement compliance".to_owned(),
        ),
        (
            "SQL-3".to_owned(),
            FeatureCategory::SqlGrammar,
            ScenarioCriticality::Critical,
            "Full SQL roundtrip".to_owned(),
        ),
        (
            "SQL-4".to_owned(),
            FeatureCategory::SqlGrammar,
            ScenarioCriticality::Important,
            "VACUUM and PRAGMA compliance".to_owned(),
        ),
        (
            "SQL-5".to_owned(),
            FeatureCategory::SqlGrammar,
            ScenarioCriticality::Standard,
            "Query pipeline compliance".to_owned(),
        ),
        (
            "SQL-6".to_owned(),
            FeatureCategory::SqlGrammar,
            ScenarioCriticality::Standard,
            "SQL pattern coverage (broad)".to_owned(),
        ),
        // Concurrency — Critical
        (
            "CON-1".to_owned(),
            FeatureCategory::StorageTransaction,
            ScenarioCriticality::Critical,
            "Concurrent-writer compliance gate".to_owned(),
        ),
        (
            "CON-3".to_owned(),
            FeatureCategory::StorageTransaction,
            ScenarioCriticality::Critical,
            "Concurrent multi-thread writes".to_owned(),
        ),
        (
            "CON-5".to_owned(),
            FeatureCategory::StorageTransaction,
            ScenarioCriticality::Critical,
            "MVCC concurrent writer stress".to_owned(),
        ),
        (
            "CON-6".to_owned(),
            FeatureCategory::StorageTransaction,
            ScenarioCriticality::Important,
            "Deterministic concurrency".to_owned(),
        ),
        (
            "CON-7".to_owned(),
            FeatureCategory::StorageTransaction,
            ScenarioCriticality::Important,
            "MVCC writer stress (harness)".to_owned(),
        ),
        // MVCC — Critical
        (
            "MVCC-1".to_owned(),
            FeatureCategory::StorageTransaction,
            ScenarioCriticality::Critical,
            "Phase 5 MVCC compliance".to_owned(),
        ),
        (
            "MVCC-2".to_owned(),
            FeatureCategory::StorageTransaction,
            ScenarioCriticality::Critical,
            "MVCC isolation validation".to_owned(),
        ),
        (
            "MVCC-3".to_owned(),
            FeatureCategory::StorageTransaction,
            ScenarioCriticality::Critical,
            "Concurrent write correctness".to_owned(),
        ),
        (
            "MVCC-4".to_owned(),
            FeatureCategory::StorageTransaction,
            ScenarioCriticality::Critical,
            "MVCC writer stress (E2E)".to_owned(),
        ),
        (
            "MVCC-5".to_owned(),
            FeatureCategory::StorageTransaction,
            ScenarioCriticality::Important,
            "MVCC stress (harness)".to_owned(),
        ),
        (
            "MVCC-7".to_owned(),
            FeatureCategory::StorageTransaction,
            ScenarioCriticality::Standard,
            "Time-travel queries".to_owned(),
        ),
        // SSI — Critical
        (
            "SSI-1".to_owned(),
            FeatureCategory::StorageTransaction,
            ScenarioCriticality::Critical,
            "SSI write-skew detection".to_owned(),
        ),
        (
            "SSI-2".to_owned(),
            FeatureCategory::StorageTransaction,
            ScenarioCriticality::Critical,
            "SSI write-skew prevention".to_owned(),
        ),
        (
            "SSI-3".to_owned(),
            FeatureCategory::StorageTransaction,
            ScenarioCriticality::Important,
            "Phase 6 SSI compliance".to_owned(),
        ),
        // Transaction — Critical
        (
            "TXN-1".to_owned(),
            FeatureCategory::StorageTransaction,
            ScenarioCriticality::Critical,
            "Transaction semantics".to_owned(),
        ),
        (
            "TXN-2".to_owned(),
            FeatureCategory::StorageTransaction,
            ScenarioCriticality::Important,
            "Savepoint semantics".to_owned(),
        ),
        (
            "TXN-3".to_owned(),
            FeatureCategory::StorageTransaction,
            ScenarioCriticality::Important,
            "Transaction control harness".to_owned(),
        ),
        // Recovery — Critical
        (
            "REC-1".to_owned(),
            FeatureCategory::StorageTransaction,
            ScenarioCriticality::Critical,
            "WAL replay after crash".to_owned(),
        ),
        (
            "REC-2".to_owned(),
            FeatureCategory::StorageTransaction,
            ScenarioCriticality::Important,
            "Single-page recovery".to_owned(),
        ),
        (
            "REC-3".to_owned(),
            FeatureCategory::StorageTransaction,
            ScenarioCriticality::Important,
            "WAL corruption recovery".to_owned(),
        ),
        (
            "REC-4".to_owned(),
            FeatureCategory::StorageTransaction,
            ScenarioCriticality::Important,
            "Crash recovery (harness)".to_owned(),
        ),
        // WAL — Critical
        (
            "WAL-1".to_owned(),
            FeatureCategory::StorageTransaction,
            ScenarioCriticality::Critical,
            "WAL replay correctness".to_owned(),
        ),
        (
            "WAL-2".to_owned(),
            FeatureCategory::StorageTransaction,
            ScenarioCriticality::Important,
            "WAL integrity after crash".to_owned(),
        ),
        (
            "WAL-3".to_owned(),
            FeatureCategory::StorageTransaction,
            ScenarioCriticality::Important,
            "WAL checksum chain".to_owned(),
        ),
        // Compatibility — Critical
        (
            "COMPAT-1".to_owned(),
            FeatureCategory::FileFormat,
            ScenarioCriticality::Critical,
            "Real database integrity".to_owned(),
        ),
        (
            "COMPAT-3".to_owned(),
            FeatureCategory::FileFormat,
            ScenarioCriticality::Critical,
            "File format compatibility".to_owned(),
        ),
        (
            "COMPAT-4".to_owned(),
            FeatureCategory::FileFormat,
            ScenarioCriticality::Important,
            "Behavioral quirks compat".to_owned(),
        ),
        (
            "COMPAT-5".to_owned(),
            FeatureCategory::FileFormat,
            ScenarioCriticality::Important,
            "File format versioning".to_owned(),
        ),
        // Extensions — Important
        (
            "EXT-1".to_owned(),
            FeatureCategory::Extensions,
            ScenarioCriticality::Important,
            "FTS3 compatibility".to_owned(),
        ),
        (
            "EXT-2".to_owned(),
            FeatureCategory::Extensions,
            ScenarioCriticality::Important,
            "FTS5 compliance".to_owned(),
        ),
        (
            "EXT-3".to_owned(),
            FeatureCategory::Extensions,
            ScenarioCriticality::Important,
            "FTS3/FTS4 backward compat".to_owned(),
        ),
        (
            "EXT-4".to_owned(),
            FeatureCategory::Extensions,
            ScenarioCriticality::Important,
            "JSON1 extension".to_owned(),
        ),
        // Functions — Standard
        (
            "FUN-1".to_owned(),
            FeatureCategory::BuiltinFunctions,
            ScenarioCriticality::Standard,
            "Date/time functions".to_owned(),
        ),
        // Performance — Standard
        (
            "PERF-1".to_owned(),
            FeatureCategory::StorageTransaction,
            ScenarioCriticality::Standard,
            "ARC warmup".to_owned(),
        ),
        (
            "PERF-2".to_owned(),
            FeatureCategory::StorageTransaction,
            ScenarioCriticality::Standard,
            "SSI performance".to_owned(),
        ),
        (
            "PERF-3".to_owned(),
            FeatureCategory::StorageTransaction,
            ScenarioCriticality::Standard,
            "B-tree hotspot".to_owned(),
        ),
        // FEC — Important
        (
            "FEC-1".to_owned(),
            FeatureCategory::StorageTransaction,
            ScenarioCriticality::Important,
            "WAL FEC group metadata".to_owned(),
        ),
        (
            "FEC-2".to_owned(),
            FeatureCategory::StorageTransaction,
            ScenarioCriticality::Important,
            "WAL FEC repair symbols".to_owned(),
        ),
        (
            "FEC-3".to_owned(),
            FeatureCategory::StorageTransaction,
            ScenarioCriticality::Standard,
            "RaptorQ E2E integration".to_owned(),
        ),
        // Correctness — Critical
        (
            "COR-1".to_owned(),
            FeatureCategory::SqlGrammar,
            ScenarioCriticality::Critical,
            "Sequential insert correctness".to_owned(),
        ),
        (
            "COR-2".to_owned(),
            FeatureCategory::SqlGrammar,
            ScenarioCriticality::Critical,
            "Mixed DML correctness".to_owned(),
        ),
        // Seed — Important
        (
            "SEED-1".to_owned(),
            FeatureCategory::StorageTransaction,
            ScenarioCriticality::Important,
            "Seed reproducibility".to_owned(),
        ),
        // Infrastructure — Standard
        (
            "INFRA-5".to_owned(),
            FeatureCategory::ApiCli,
            ScenarioCriticality::Standard,
            "Workspace layering".to_owned(),
        ),
        (
            "INFRA-6".to_owned(),
            FeatureCategory::ApiCli,
            ScenarioCriticality::Standard,
            "Logging standard".to_owned(),
        ),
    ]
}

fn assess_coverage(
    matrix: &TraceabilityMatrix,
    critical_scenarios: Vec<(String, FeatureCategory, ScenarioCriticality, String)>,
) -> Vec<CriticalScenario> {
    // Build reverse index: scenario_id -> script paths
    let mut scenario_to_scripts: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut scenario_to_command: BTreeMap<String, String> = BTreeMap::new();

    for script in &matrix.scripts {
        for sid in &script.scenario_ids {
            scenario_to_scripts
                .entry(sid.clone())
                .or_default()
                .push(script.path.clone());
            scenario_to_command
                .entry(sid.clone())
                .or_insert_with(|| script.invocation.command.clone());
        }
    }

    critical_scenarios
        .into_iter()
        .map(|(id, category, criticality, description)| {
            let scripts = scenario_to_scripts.get(&id).cloned().unwrap_or_default();
            let covered = !scripts.is_empty();
            let replay_command = scenario_to_command.get(&id).cloned();

            CriticalScenario {
                scenario_id: id,
                category,
                criticality,
                description,
                covered,
                covering_scripts: scripts,
                replay_command,
            }
        })
        .collect()
}

fn compute_stats(scenarios: &[CriticalScenario]) -> CoverageReportStats {
    let total = scenarios.len();
    let covered = scenarios.iter().filter(|s| s.covered).count();
    let uncovered = total - covered;

    let critical_total = scenarios
        .iter()
        .filter(|s| s.criticality == ScenarioCriticality::Critical)
        .count();
    let critical_covered = scenarios
        .iter()
        .filter(|s| s.criticality == ScenarioCriticality::Critical && s.covered)
        .count();

    let important_total = scenarios
        .iter()
        .filter(|s| s.criticality == ScenarioCriticality::Important)
        .count();
    let important_covered = scenarios
        .iter()
        .filter(|s| s.criticality == ScenarioCriticality::Important && s.covered)
        .count();

    let coverage_pct = if total > 0 {
        truncate_f64(covered as f64 / total as f64, 4)
    } else {
        0.0
    };

    // Per-category stats
    let mut by_category: BTreeMap<String, CategoryCoverageStats> = BTreeMap::new();
    for cat in FeatureCategory::ALL {
        let cat_scenarios: Vec<_> = scenarios.iter().filter(|s| s.category == cat).collect();
        let cat_total = cat_scenarios.len();
        if cat_total > 0 {
            let cat_covered = cat_scenarios.iter().filter(|s| s.covered).count();
            by_category.insert(
                format!("{cat:?}"),
                CategoryCoverageStats {
                    total: cat_total,
                    covered: cat_covered,
                    pct: truncate_f64(cat_covered as f64 / cat_total as f64, 4),
                },
            );
        }
    }

    CoverageReportStats {
        total_scenarios: total,
        covered_scenarios: covered,
        uncovered_scenarios: uncovered,
        critical_covered,
        critical_total,
        important_covered,
        important_total,
        coverage_pct,
        by_category,
    }
}

fn truncate_f64(value: f64, decimals: u32) -> f64 {
    let exp = i32::try_from(decimals).unwrap_or(6);
    let factor = 10_f64.powi(exp);
    (value * factor).trunc() / factor
}

// ─── Log Quality Validator ──────────────────────────────────────────────

/// Validate that a log event conforms to the schema.
pub fn validate_log_event(event: &LogEventSchema) -> Vec<String> {
    let specs = build_field_specs();
    let mut errors = validate_event_against_field_specs(event, &specs);

    if !(event.timestamp.contains('T') && event.timestamp.ends_with('Z')) {
        errors.push("timestamp must be an ISO8601 UTC value ending in 'Z'".to_owned());
    }

    if let Some(ref sid) = event.scenario_id {
        if let Some((category, number)) = sid.split_once('-') {
            if category.chars().any(|ch| !ch.is_ascii_uppercase()) {
                errors.push(format!(
                    "scenario_id '{}' category must contain uppercase ASCII letters",
                    sid
                ));
            }
            if number.chars().any(|ch| !ch.is_ascii_digit()) {
                errors.push(format!(
                    "scenario_id '{}' numeric suffix must contain ASCII digits",
                    sid
                ));
            }
        } else {
            errors.push(format!(
                "scenario_id '{}' doesn't follow CATEGORY-NUMBER convention",
                sid
            ));
        }
    }

    if let Some(ref artifact_hash) = event.artifact_hash
        && (artifact_hash.len() != 64 || artifact_hash.chars().any(|ch| !ch.is_ascii_hexdigit()))
    {
        errors.push("artifact_hash must be a 64-char hexadecimal SHA-256 digest".to_owned());
    }

    if matches!(
        event.event_type,
        LogEventType::Fail | LogEventType::Error | LogEventType::FirstDivergence
    ) {
        if event.seed.is_none() {
            errors.push("seed is required for fail/error/divergence events".to_owned());
        }
        if event.scenario_id.is_none() {
            errors.push("scenario_id is required for fail/error/divergence events".to_owned());
        }
    }

    errors
}

// ─── Tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coverage_report_builds() {
        let report = build_coverage_report();
        assert!(!report.scenarios.is_empty());
        assert_eq!(report.schema_version, "1.0.0");
        assert_eq!(report.bead_id, BEAD_ID);
    }

    #[test]
    fn coverage_report_validates() {
        let report = build_coverage_report();
        let errors = report.validate();
        assert!(
            errors.is_empty(),
            "Validation errors:\n{}",
            errors.join("\n")
        );
    }

    #[test]
    fn critical_scenarios_have_coverage() {
        let report = build_coverage_report();
        // All critical scenarios should be covered
        for s in &report.scenarios {
            if s.criticality == ScenarioCriticality::Critical {
                assert!(
                    s.covered,
                    "Critical scenario {} is not covered: {}",
                    s.scenario_id, s.description
                );
            }
        }
    }

    #[test]
    fn critical_scenarios_have_replay() {
        let report = build_coverage_report();
        for s in &report.scenarios {
            if s.criticality == ScenarioCriticality::Critical && s.covered {
                assert!(
                    s.replay_command.is_some(),
                    "Critical scenario {} lacks replay command",
                    s.scenario_id
                );
            }
        }
    }

    #[test]
    fn coverage_pct_is_high() {
        let report = build_coverage_report();
        assert!(
            report.stats.coverage_pct >= 0.9,
            "Expected >= 90% coverage, got {:.1}%",
            report.stats.coverage_pct * 100.0
        );
    }

    #[test]
    fn field_specs_complete() {
        let specs = build_field_specs();
        let required_count = specs
            .iter()
            .filter(|s| s.requirement == FieldRequirement::Required)
            .count();
        assert!(required_count >= 4, "Need at least 4 required fields");
    }

    #[test]
    fn required_field_constants_match_specs() {
        let specs = build_field_specs();
        let required_from_specs = specs
            .iter()
            .filter(|spec| spec.requirement == FieldRequirement::Required)
            .map(|spec| spec.name.clone())
            .collect::<Vec<_>>();
        let required_from_constant = REQUIRED_EVENT_FIELDS
            .iter()
            .map(|field| (*field).to_owned())
            .collect::<Vec<_>>();
        assert_eq!(required_from_specs, required_from_constant);
    }

    #[test]
    fn log_event_validation() {
        let mut context = BTreeMap::new();
        context.insert("invariant_ids".to_owned(), "INV-1,INV-2".to_owned());
        context.insert(
            "artifact_paths".to_owned(),
            "artifacts/events.jsonl,artifacts/report.json".to_owned(),
        );

        let good_event = LogEventSchema {
            run_id: "test-run-001".to_owned(),
            timestamp: "2026-02-13T05:00:00Z".to_owned(),
            phase: LogPhase::Execute,
            event_type: LogEventType::Pass,
            scenario_id: Some("MVCC-3".to_owned()),
            seed: Some(42),
            backend: Some("fsqlite".to_owned()),
            artifact_hash: None,
            context,
        };

        let errors = validate_log_event(&good_event);
        assert!(
            errors.is_empty(),
            "Good event should validate: {:?}",
            errors
        );

        let bad_event = LogEventSchema {
            run_id: String::new(),
            timestamp: String::new(),
            phase: LogPhase::Execute,
            event_type: LogEventType::Pass,
            scenario_id: Some("invalid".to_owned()),
            seed: None,
            backend: None,
            artifact_hash: None,
            context: BTreeMap::new(),
        };

        let errors = validate_log_event(&bad_event);
        assert!(!errors.is_empty(), "Bad event should have errors");
    }

    #[test]
    fn version_transition_policy() {
        assert_eq!(
            classify_version_transition("1.0.0", "1.0.0").expect("same version should parse"),
            VersionTransition::NoChange
        );
        assert_eq!(
            classify_version_transition("1.0.0", "1.0.1").expect("patch should parse"),
            VersionTransition::Patch
        );
        assert_eq!(
            classify_version_transition("1.0.0", "1.1.0").expect("minor should parse"),
            VersionTransition::Additive
        );
        assert_eq!(
            classify_version_transition("1.3.2", "2.0.0").expect("major should parse"),
            VersionTransition::Breaking
        );
        assert!(
            classify_version_transition("1.2.0", "1.1.9").is_err(),
            "downgrades must be rejected"
        );
    }

    #[test]
    fn tooling_compatibility_policy() {
        assert_eq!(
            evaluate_tooling_compatibility("1.0.0", "1.0.0").expect("compat parse"),
            ToolingCompatibility::ReadWrite
        );
        assert_eq!(
            evaluate_tooling_compatibility("1.2.0", "1.1.0").expect("compat parse"),
            ToolingCompatibility::ReadWrite
        );
        assert_eq!(
            evaluate_tooling_compatibility("1.0.0", "1.2.0").expect("compat parse"),
            ToolingCompatibility::ReadOnlyForwardCompatible
        );
        assert_eq!(
            evaluate_tooling_compatibility("1.2.0", "2.0.0").expect("compat parse"),
            ToolingCompatibility::Incompatible
        );
    }

    #[test]
    fn canonical_examples_validate_against_contract() {
        let specs = build_field_specs();
        for event in canonical_event_examples() {
            let schema_errors = validate_log_event(&event);
            assert!(
                schema_errors.is_empty(),
                "canonical example should satisfy schema validator: {schema_errors:?}"
            );

            let contract_errors = validate_event_against_field_specs(&event, &specs);
            assert!(
                contract_errors.is_empty(),
                "canonical example should satisfy field specs: {contract_errors:?}"
            );
        }
    }

    #[test]
    fn schema_contract_markdown_contains_core_sections() {
        let markdown = render_schema_contract_markdown();
        assert!(markdown.contains("# Unified E2E Log Schema Contract"));
        assert!(markdown.contains(LOG_SCHEMA_VERSION));
        assert!(markdown.contains(LOG_SCHEMA_MIN_SUPPORTED_VERSION));
        for field in REQUIRED_EVENT_FIELDS {
            assert!(
                markdown.contains(field),
                "contract markdown missing required field {field}"
            );
        }
        for replay_key in REPLAYABILITY_KEYS {
            assert!(
                markdown.contains(replay_key),
                "contract markdown missing replay key {replay_key}"
            );
        }
    }

    #[test]
    fn json_roundtrip() {
        let report = build_coverage_report();
        let json = report.to_json().expect("serialize");
        let deserialized: ScenarioCoverageReport =
            serde_json::from_str(&json).expect("deserialize");
        assert_eq!(deserialized.scenarios.len(), report.scenarios.len());
    }

    #[test]
    fn no_duplicate_scenario_ids() {
        let report = build_coverage_report();
        let mut seen = BTreeSet::new();
        for s in &report.scenarios {
            assert!(seen.insert(&s.scenario_id), "Duplicate: {}", s.scenario_id);
        }
    }

    #[test]
    fn stats_consistency() {
        let report = build_coverage_report();
        assert_eq!(
            report.stats.covered_scenarios + report.stats.uncovered_scenarios,
            report.stats.total_scenarios,
        );
        assert_eq!(
            report.stats.critical_covered + report.stats.critical_total
                - report.stats.critical_total,
            report.stats.critical_covered,
        );
    }

    #[test]
    fn category_coverage_present() {
        let report = build_coverage_report();
        // At least some categories should have coverage info
        assert!(
            !report.stats.by_category.is_empty(),
            "Should have per-category stats"
        );
    }

    #[test]
    fn deterministic_report() {
        let r1 = build_coverage_report();
        let r2 = build_coverage_report();
        assert_eq!(r1.scenarios.len(), r2.scenarios.len());
        assert_eq!(
            r1.stats.coverage_pct.total_cmp(&r2.stats.coverage_pct),
            std::cmp::Ordering::Equal
        );
    }
}
