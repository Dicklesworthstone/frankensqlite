//! Agent-swarm SQL workload trace schema and privacy scrubber.
//!
//! The replay lab records the shape of concurrent agent activity without
//! storing user data. This module owns the canonical JSON schema for those
//! traces plus the deterministic scrubber used before writing fixtures or
//! replay bundles.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fmt::Write as _;
use std::fs;
use std::path::Path;
use std::time::{Duration, Instant};

use crate::differential_v2::{
    CsqliteExecutor, EngineIdentity, FsqliteExecutor, SqlExecutor, StmtOutcome,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Bead that introduced the agent-swarm trace contract.
#[allow(dead_code)]
const BEAD_ID: &str = "bd-agent-swarm-replay-lab-1cc5y.1";

/// Schema version for serialized agent-swarm traces.
pub const SWARM_TRACE_SCHEMA_VERSION: &str = "1.0.0";

/// Version of the privacy scrubber contract.
pub const SWARM_TRACE_SCRUBBER_VERSION: &str = "1.0.0";

/// Version of the fixed-seed synthetic trace generator.
pub const SYNTHETIC_TRACE_GENERATOR_VERSION: &str = "1.0.0";

/// Version of the deterministic agent-swarm replay harness.
pub const AGENT_SWARM_REPLAY_HARNESS_VERSION: &str = "1.0.0";

/// Version of the resource envelope and fairness scorecard contract.
pub const AGENT_SWARM_RESOURCE_SCORECARD_VERSION: &str = "1.0.0";

/// Version of the coordination bridge fields attached to replay scorecards.
pub const AGENT_SWARM_COORDINATION_BRIDGE_VERSION: &str = "1.0.0";

/// Version of the replay evidence manifest contract.
pub const AGENT_SWARM_EVIDENCE_MANIFEST_VERSION: &str = "1.0.0";

/// Version of the CI smoke replay artifact contract.
pub const AGENT_SWARM_CI_SMOKE_VERSION: &str = "1.0.0";

/// Marker used in structured logs when no failure diagnostic exists yet.
pub const FIRST_FAILURE_DIAGNOSTIC_ABSENT: &str = "none";

/// Coordination trace fields that must remain scrubbed and scorecard-visible.
pub const AGENT_SWARM_COORDINATION_TRACE_FIELDS: [&str; 15] = [
    "queue_claim_id",
    "queue_release_reason",
    "lease_owner",
    "lease_generation",
    "lease_expires_ms",
    "worker_range_start",
    "worker_range_end",
    "range_imbalance_reason",
    "explain_concurrency_reason",
    "fallback_reason",
    "statement_fingerprint",
    "resource_governor_decision",
    "coordination_correctness",
    "conflict_transparency",
    "fairness_resource_pressure",
];

const MIB: u64 = 1_024 * 1_024;
const GIB: u64 = 1_024 * MIB;

/// Complete sanitized trace for an agent-swarm workload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentSwarmTrace {
    /// Version of this serialized schema.
    pub schema_version: String,
    /// Stable trace identifier used across artifacts.
    pub trace_id: String,
    /// Replay or capture run identifier.
    pub run_id: String,
    /// Scenario identifier shared with logs and future replay harnesses.
    pub scenario_id: String,
    /// Version of the scrubber that produced the sanitized statements.
    pub scrubber_version: String,
    /// Sanitized provenance for the trace as a whole.
    pub metadata: TraceMetadata,
    /// Sanitized statements in deterministic replay order.
    pub statements: Vec<TraceStatement>,
    /// Aggregate redaction counts for quick audit checks.
    pub redaction_summary: RedactionSummary,
}

impl AgentSwarmTrace {
    /// Build a trace and compute aggregate redaction metadata.
    pub fn new(
        trace_id: impl Into<String>,
        run_id: impl Into<String>,
        scenario_id: impl Into<String>,
        metadata: TraceMetadata,
        statements: Vec<TraceStatement>,
    ) -> Result<Self, TraceScrubError> {
        let trace_id = required_owned("trace_id", trace_id.into())?;
        let run_id = required_owned("run_id", run_id.into())?;
        let scenario_id = required_owned("scenario_id", scenario_id.into())?;
        if statements.is_empty() {
            return Err(TraceScrubError::EmptyStatements);
        }
        let redaction_summary = RedactionSummary::from_statements(&statements);
        Ok(Self {
            schema_version: SWARM_TRACE_SCHEMA_VERSION.to_owned(),
            trace_id,
            run_id,
            scenario_id,
            scrubber_version: SWARM_TRACE_SCRUBBER_VERSION.to_owned(),
            metadata,
            statements,
            redaction_summary,
        })
    }

    /// Number of statements in the trace.
    pub fn statement_count(&self) -> usize {
        self.statements.len()
    }

    /// Number of distinct transaction identifiers preserved in the trace.
    pub fn transaction_count(&self) -> usize {
        self.statements
            .iter()
            .filter_map(|statement| statement.transaction_id.as_deref())
            .collect::<BTreeSet<_>>()
            .len()
    }
}

/// Sanitized provenance for an agent-swarm trace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TraceMetadata {
    /// High-level source category, for example `agent-mail` or `cass`.
    pub source_kind: String,
    /// Sanitized source identifier suitable for artifact references.
    pub source_id: String,
    /// Digest of the raw source before scrubbing, when available.
    pub source_digest: Option<String>,
    /// Logical capture clock or deterministic source ordering label.
    pub logical_clock: Option<String>,
    /// Extra non-sensitive provenance tags.
    pub tags: BTreeMap<String, String>,
}

impl TraceMetadata {
    /// Create minimal trace metadata.
    pub fn new(
        source_kind: impl Into<String>,
        source_id: impl Into<String>,
    ) -> Result<Self, TraceScrubError> {
        Ok(Self {
            source_kind: required_owned("source_kind", source_kind.into())?,
            source_id: required_owned("source_id", source_id.into())?,
            source_digest: None,
            logical_clock: None,
            tags: BTreeMap::new(),
        })
    }
}

/// Sanitized provenance for a single statement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatementSource {
    /// Source category, for example `agent-mail-message` or `bead-comment`.
    pub source_kind: String,
    /// Sanitized reference within the source category.
    pub source_ref: String,
    /// Optional digest of the raw source record.
    pub source_digest: Option<String>,
}

impl StatementSource {
    /// Create statement-level provenance.
    pub fn new(
        source_kind: impl Into<String>,
        source_ref: impl Into<String>,
    ) -> Result<Self, TraceScrubError> {
        Ok(Self {
            source_kind: required_owned("source_kind", source_kind.into())?,
            source_ref: required_owned("source_ref", source_ref.into())?,
            source_digest: None,
        })
    }
}

/// Raw statement data before SQL scrubbing.
#[derive(Debug, Clone)]
pub struct RawTraceStatement<'a> {
    /// Deterministic replay order within the trace.
    pub logical_order: u64,
    /// Optional logical timestamp from the capture source.
    pub logical_timestamp: Option<&'a str>,
    /// Logical actor that issued the statement.
    pub actor_id: &'a str,
    /// Logical connection used by the actor.
    pub connection_id: &'a str,
    /// Transaction identifier, when the statement belongs to a transaction.
    pub transaction_id: Option<&'a str>,
    /// Transaction boundary marker for this statement.
    pub transaction_boundary: TransactionBoundary,
    /// Group of statements that may overlap concurrently.
    pub concurrency_group: &'a str,
    /// Workload phase, for example `setup`, `hot-write`, or `verify`.
    pub workload_phase: &'a str,
    /// Raw SQL statement text.
    pub sql: &'a str,
    /// Expected high-level outcome class.
    pub expected_result_class: ExpectedResultClass,
    /// Expected row-count bucket, when known.
    pub row_count_class: RowCountClass,
    /// Error class, when the expected outcome is an error-like result.
    pub error_class: Option<&'a str>,
    /// Sanitized source metadata.
    pub source: StatementSource,
}

/// Sanitized trace statement ready for serialization.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TraceStatement {
    /// Deterministic replay order within the trace.
    pub logical_order: u64,
    /// Optional logical timestamp from the capture source.
    pub logical_timestamp: Option<String>,
    /// Logical actor that issued the statement.
    pub actor_id: String,
    /// Logical connection used by the actor.
    pub connection_id: String,
    /// Transaction identifier, when present.
    pub transaction_id: Option<String>,
    /// Transaction boundary marker.
    pub transaction_boundary: TransactionBoundary,
    /// Group of statements that may overlap concurrently.
    pub concurrency_group: String,
    /// Workload phase, for example `setup`, `hot-write`, or `verify`.
    pub workload_phase: String,
    /// SQL after privacy-preserving literal redaction.
    pub scrubbed_sql: String,
    /// SHA-256 hash of `scrubbed_sql`, encoded as lowercase hex.
    pub statement_shape: String,
    /// Number of bind parameters in the original SQL shape.
    pub parameter_count: usize,
    /// Redactions performed while producing `scrubbed_sql`.
    pub redactions: RedactionCounts,
    /// Expected high-level outcome class.
    pub expected_result_class: ExpectedResultClass,
    /// Expected row-count bucket, when known.
    pub row_count_class: RowCountClass,
    /// Error class, when known.
    pub error_class: Option<String>,
    /// Sanitized statement source metadata.
    pub source: StatementSource,
}

impl TraceStatement {
    /// Scrub a raw statement and validate required topology fields.
    pub fn from_raw(raw: RawTraceStatement<'_>) -> Result<Self, TraceScrubError> {
        if raw.sql.trim().is_empty() {
            return Err(TraceScrubError::EmptySql);
        }
        if raw.transaction_boundary != TransactionBoundary::None && raw.transaction_id.is_none() {
            return Err(TraceScrubError::BoundaryWithoutTransactionId);
        }
        let scrubbed = scrub_sql_statement(raw.sql);
        Ok(Self {
            logical_order: raw.logical_order,
            logical_timestamp: raw.logical_timestamp.map(ToOwned::to_owned),
            actor_id: required_ref("actor_id", raw.actor_id)?.to_owned(),
            connection_id: required_ref("connection_id", raw.connection_id)?.to_owned(),
            transaction_id: raw.transaction_id.map(ToOwned::to_owned),
            transaction_boundary: raw.transaction_boundary,
            concurrency_group: required_ref("concurrency_group", raw.concurrency_group)?.to_owned(),
            workload_phase: required_ref("workload_phase", raw.workload_phase)?.to_owned(),
            scrubbed_sql: scrubbed.sql,
            statement_shape: scrubbed.statement_shape,
            parameter_count: scrubbed.parameter_count,
            redactions: scrubbed.redactions,
            expected_result_class: raw.expected_result_class,
            row_count_class: raw.row_count_class,
            error_class: raw.error_class.map(ToOwned::to_owned),
            source: raw.source,
        })
    }
}

/// Transaction boundary marker preserved for replay topology.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransactionBoundary {
    /// The statement is not itself a transaction boundary.
    None,
    /// Transaction begin marker.
    Begin,
    /// Transaction commit marker.
    Commit,
    /// Transaction rollback marker.
    Rollback,
}

/// High-level expected statement outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExpectedResultClass {
    /// Statement is expected to succeed.
    Success,
    /// Statement is expected to hit a busy/locked condition.
    Busy,
    /// Statement is expected to hit a page-level conflict.
    Conflict,
    /// Statement is expected to fail with an error.
    Error,
    /// Expected result is not known yet.
    Unknown,
}

/// Coarse row-count bucket for replay comparisons.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RowCountClass {
    /// Statement returns or affects no rows.
    Zero,
    /// Statement returns or affects one row.
    One,
    /// Statement returns or affects a small bounded count.
    Few,
    /// Statement returns or affects many rows.
    Many,
    /// Row-count behavior is not known yet.
    Unknown,
}

/// Result of scrubbing one SQL statement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScrubbedSql {
    /// Sanitized SQL text.
    pub sql: String,
    /// SHA-256 hash of the sanitized SQL shape.
    pub statement_shape: String,
    /// Number of bind parameters preserved in SQL shape.
    pub parameter_count: usize,
    /// Redaction counts by literal class.
    pub redactions: RedactionCounts,
}

/// Redaction counts for a single statement.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RedactionCounts {
    /// Single-quoted string literals replaced with `?s`.
    pub string_literals: usize,
    /// Numeric literals replaced with `?n`.
    pub numeric_literals: usize,
    /// SQLite blob literals replaced with `?b`.
    pub blob_literals: usize,
    /// SQL comments replaced with comment markers.
    pub comments: usize,
}

impl RedactionCounts {
    /// Total number of redacted spans.
    pub const fn total(self) -> usize {
        self.string_literals + self.numeric_literals + self.blob_literals + self.comments
    }
}

/// Aggregate redaction summary for a trace.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RedactionSummary {
    /// Number of statements included in the trace.
    pub statement_count: usize,
    /// Number of distinct transactions represented in the trace.
    pub transaction_count: usize,
    /// Total redacted spans across all statements.
    pub redaction_count: usize,
    /// Single-quoted string literal redactions.
    pub string_literals: usize,
    /// Numeric literal redactions.
    pub numeric_literals: usize,
    /// SQLite blob literal redactions.
    pub blob_literals: usize,
    /// SQL comment redactions.
    pub comments: usize,
}

impl RedactionSummary {
    /// Summarize statements after scrubbing.
    pub fn from_statements(statements: &[TraceStatement]) -> Self {
        let mut summary = Self {
            statement_count: statements.len(),
            transaction_count: statements
                .iter()
                .filter_map(|statement| statement.transaction_id.as_deref())
                .collect::<BTreeSet<_>>()
                .len(),
            ..Self::default()
        };
        for statement in statements {
            summary.string_literals += statement.redactions.string_literals;
            summary.numeric_literals += statement.redactions.numeric_literals;
            summary.blob_literals += statement.redactions.blob_literals;
            summary.comments += statement.redactions.comments;
        }
        summary.redaction_count = summary.string_literals
            + summary.numeric_literals
            + summary.blob_literals
            + summary.comments;
        summary
    }
}

/// Workload family used by the deterministic synthetic trace generator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SyntheticTraceScenario {
    /// Agents append session events and tool-call logs.
    SessionEventAppend,
    /// Agents repeatedly claim, update, and complete task-queue rows.
    TaskQueueClaimLoop,
    /// Artifact index writes are mixed with read-heavy metadata lookups.
    ArtifactIndexMixedLookup,
    /// Many agents update shared counters and status rows.
    HotCounterStatusRows,
    /// Long readers overlap with bursty writers.
    LongReaderBurstWriter,
}

impl SyntheticTraceScenario {
    /// Stable scenario identifier used in trace and log metadata.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SessionEventAppend => "session_event_append",
            Self::TaskQueueClaimLoop => "task_queue_claim_loop",
            Self::ArtifactIndexMixedLookup => "artifact_index_mixed_lookup",
            Self::HotCounterStatusRows => "hot_counter_status_rows",
            Self::LongReaderBurstWriter => "long_reader_burst_writer",
        }
    }

    fn expected_contention_shape(self, hot_key_ratio: u8) -> String {
        let skew = match hot_key_ratio {
            0..=24 => "low_skew",
            25..=74 => "mixed_skew",
            _ => "high_skew",
        };
        let base = match self {
            Self::SessionEventAppend => "append_heavy_session_streams",
            Self::TaskQueueClaimLoop => "claim_update_status_rows",
            Self::ArtifactIndexMixedLookup => "metadata_read_write_mix",
            Self::HotCounterStatusRows => "shared_counter_hotspots",
            Self::LongReaderBurstWriter => "long_reader_writer_overlap",
        };
        format!("{base}:{skew}")
    }

    fn target_invariants(self) -> &'static str {
        match self {
            Self::SessionEventAppend => {
                "schema_conformance,deterministic_seed,transaction_boundaries,append_order_shape"
            }
            Self::TaskQueueClaimLoop => {
                "schema_conformance,deterministic_seed,transaction_boundaries,no_double_claim_shape"
            }
            Self::ArtifactIndexMixedLookup => {
                "schema_conformance,deterministic_seed,transaction_boundaries,read_write_shape"
            }
            Self::HotCounterStatusRows => {
                "schema_conformance,deterministic_seed,transaction_boundaries,hotspot_conflict_shape"
            }
            Self::LongReaderBurstWriter => {
                "schema_conformance,deterministic_seed,transaction_boundaries,mvcc_visibility_shape"
            }
        }
    }

    fn phase(self, is_read: bool) -> &'static str {
        match (self, is_read) {
            (Self::SessionEventAppend, true) => "session-read",
            (Self::SessionEventAppend, false) => "session-append",
            (Self::TaskQueueClaimLoop, true) => "queue-peek",
            (Self::TaskQueueClaimLoop, false) => "queue-claim",
            (Self::ArtifactIndexMixedLookup, true) => "artifact-lookup",
            (Self::ArtifactIndexMixedLookup, false) => "artifact-index-write",
            (Self::HotCounterStatusRows, true) => "counter-read",
            (Self::HotCounterStatusRows, false) => "counter-update",
            (Self::LongReaderBurstWriter, true) => "long-read",
            (Self::LongReaderBurstWriter, false) => "bursty-write",
        }
    }
}

/// All synthetic scenario families currently emitted by the replay lab.
pub const SYNTHETIC_TRACE_SCENARIOS: [SyntheticTraceScenario; 5] = [
    SyntheticTraceScenario::SessionEventAppend,
    SyntheticTraceScenario::TaskQueueClaimLoop,
    SyntheticTraceScenario::ArtifactIndexMixedLookup,
    SyntheticTraceScenario::HotCounterStatusRows,
    SyntheticTraceScenario::LongReaderBurstWriter,
];

/// Configuration for deterministic synthetic agent-swarm trace generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyntheticTraceConfig {
    /// Workload family to synthesize.
    pub scenario: SyntheticTraceScenario,
    /// Fixed seed for deterministic generation.
    pub seed: u64,
    /// Number of logical agents represented in the trace.
    pub agent_count: usize,
    /// Number of transactions to emit.
    pub transaction_count: usize,
    /// Relative read weight in the transaction mix.
    pub read_weight: u16,
    /// Relative write weight in the transaction mix.
    pub write_weight: u16,
    /// Percentage of transactions routed to hot keys, from 0 to 100.
    pub hot_key_ratio: u8,
}

impl SyntheticTraceConfig {
    /// Create a synthetic trace configuration.
    pub const fn new(
        scenario: SyntheticTraceScenario,
        seed: u64,
        agent_count: usize,
        transaction_count: usize,
    ) -> Self {
        Self {
            scenario,
            seed,
            agent_count,
            transaction_count,
            read_weight: 40,
            write_weight: 60,
            hot_key_ratio: 70,
        }
    }

    /// Override the read/write transaction mix.
    pub const fn with_read_write_mix(mut self, read_weight: u16, write_weight: u16) -> Self {
        self.read_weight = read_weight;
        self.write_weight = write_weight;
        self
    }

    /// Override the hot-key skew percentage.
    pub const fn with_hot_key_ratio(mut self, hot_key_ratio: u8) -> Self {
        self.hot_key_ratio = hot_key_ratio;
        self
    }

    /// Stable read/write mix label used in metadata and logs.
    pub fn read_write_mix(&self) -> String {
        format!("read:{}:write:{}", self.read_weight, self.write_weight)
    }

    fn validate(&self) -> Result<(), SyntheticTraceError> {
        if self.agent_count == 0 {
            return Err(SyntheticTraceError::AgentCountZero);
        }
        if self.transaction_count == 0 {
            return Err(SyntheticTraceError::TransactionCountZero);
        }
        if self.read_write_total() == 0 {
            return Err(SyntheticTraceError::EmptyReadWriteMix);
        }
        if self.hot_key_ratio > 100 {
            return Err(SyntheticTraceError::HotKeyRatioOutOfRange(
                self.hot_key_ratio,
            ));
        }
        Ok(())
    }

    fn read_write_total(&self) -> u64 {
        u64::from(self.read_weight) + u64::from(self.write_weight)
    }

    fn choose_read(&self, rng: &mut SyntheticTraceRng) -> bool {
        rng.next_bounded(self.read_write_total()) < u64::from(self.read_weight)
    }
}

/// Error raised while generating a synthetic trace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyntheticTraceError {
    /// `agent_count` must be greater than zero.
    AgentCountZero,
    /// `transaction_count` must be greater than zero.
    TransactionCountZero,
    /// At least one read or write weight must be non-zero.
    EmptyReadWriteMix,
    /// Hot-key skew is a percentage and must be at most 100.
    HotKeyRatioOutOfRange(u8),
    /// Generated raw trace data failed schema validation.
    Trace(TraceScrubError),
}

impl fmt::Display for SyntheticTraceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AgentCountZero => {
                f.write_str("synthetic trace agent_count must be greater than zero")
            }
            Self::TransactionCountZero => {
                f.write_str("synthetic trace transaction_count must be greater than zero")
            }
            Self::EmptyReadWriteMix => {
                f.write_str("synthetic trace read/write mix must include at least one operation")
            }
            Self::HotKeyRatioOutOfRange(value) => {
                write!(
                    f,
                    "synthetic trace hot_key_ratio must be <= 100, got {value}"
                )
            }
            Self::Trace(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for SyntheticTraceError {}

impl From<TraceScrubError> for SyntheticTraceError {
    fn from(value: TraceScrubError) -> Self {
        Self::Trace(value)
    }
}

/// Error raised while validating trace fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TraceScrubError {
    /// A required field was empty after trimming.
    EmptyField(&'static str),
    /// A SQL statement was empty after trimming.
    EmptySql,
    /// A boundary marker was provided without a transaction id.
    BoundaryWithoutTransactionId,
    /// A trace was created without statements.
    EmptyStatements,
}

impl fmt::Display for TraceScrubError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyField(field) => write!(f, "required trace field `{field}` is empty"),
            Self::EmptySql => f.write_str("trace statement SQL is empty"),
            Self::BoundaryWithoutTransactionId => {
                f.write_str("transaction boundary requires a transaction id")
            }
            Self::EmptyStatements => f.write_str("agent-swarm trace must contain statements"),
        }
    }
}

impl std::error::Error for TraceScrubError {}

/// Error raised while loading or replaying an agent-swarm trace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentSwarmReplayError {
    /// Serialized trace schema does not match the replay harness contract.
    SchemaMismatch {
        /// Expected schema version.
        expected: String,
        /// Actual schema version observed in the trace.
        actual: String,
    },
    /// Trace JSON could not be decoded.
    JsonDecode(String),
    /// Trace fixture could not be read from disk.
    FixtureRead(String),
    /// Evidence manifest or minimized-trace JSON could not be encoded.
    JsonEncode(String),
    /// Evidence artifact could not be written to disk.
    EvidenceWrite {
        /// Artifact path.
        path: String,
        /// Filesystem error message.
        message: String,
    },
    /// A built-in SQL backend could not be opened.
    BackendOpen {
        /// Backend that failed to initialize.
        backend: AgentSwarmReplayBackend,
        /// Error message from the backend.
        message: String,
    },
}

impl fmt::Display for AgentSwarmReplayError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SchemaMismatch { expected, actual } => {
                write!(
                    f,
                    "agent-swarm trace schema mismatch: expected {expected}, got {actual}"
                )
            }
            Self::JsonDecode(message) => {
                write!(f, "agent-swarm trace JSON decode failed: {message}")
            }
            Self::FixtureRead(message) => {
                write!(f, "agent-swarm trace fixture read failed: {message}")
            }
            Self::JsonEncode(message) => {
                write!(f, "agent-swarm evidence JSON encode failed: {message}")
            }
            Self::EvidenceWrite { path, message } => {
                write!(f, "agent-swarm evidence write failed for {path}: {message}")
            }
            Self::BackendOpen { backend, message } => {
                write!(
                    f,
                    "failed to open {} replay backend: {message}",
                    backend.as_str()
                )
            }
        }
    }
}

impl std::error::Error for AgentSwarmReplayError {}

/// SQL backend role used by the agent-swarm replay report.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentSwarmReplayBackend {
    /// FrankenSQLite with its concurrent-writer default left enabled.
    FrankenSqliteConcurrent,
    /// C SQLite/rusqlite oracle baseline.
    CSqliteOracle,
}

impl AgentSwarmReplayBackend {
    /// Stable backend label used in reports and structured logs.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::FrankenSqliteConcurrent => "frankensqlite_concurrent",
            Self::CSqliteOracle => "csqlite_oracle",
        }
    }

    const fn comparison_role(self) -> &'static str {
        match self {
            Self::FrankenSqliteConcurrent => "subject_concurrent_writer_default",
            Self::CSqliteOracle => "baseline_csqlite_oracle",
        }
    }

    const fn concurrent_writer_default(self) -> bool {
        matches!(self, Self::FrankenSqliteConcurrent)
    }
}

/// Deterministic schedule used when replaying trace statements.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentSwarmReplaySchedule {
    /// Preserve sanitized trace order by `logical_order`.
    TraceOrder,
    /// Group by `concurrency_group`, then replay by `logical_order`.
    ConcurrencyGroupThenOrder,
}

/// One-command entry points embedded into replay reports for operators.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentSwarmReplayCommands {
    /// Fast smoke command for a small trace fixture.
    pub smoke_command: String,
    /// Heavy command shape intended for rch-offloaded traces.
    pub heavy_command: String,
}

impl Default for AgentSwarmReplayCommands {
    fn default() -> Self {
        Self {
            smoke_command: "cargo test -p fsqlite-harness --lib agent_swarm_ci_smoke -- --nocapture"
                .to_owned(),
            heavy_command: "timeout 1200 rch exec -- env CARGO_TARGET_DIR=/data/tmp/frankensqlite-agent-swarm-replay cargo test -p fsqlite-harness --lib agent_swarm_replay -- --nocapture".to_owned(),
        }
    }
}

/// Configuration for deterministic agent-swarm trace replay.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentSwarmReplayConfig {
    /// Seed recorded in the replay report for deterministic scheduling.
    pub seed: u64,
    /// Statement scheduling policy.
    pub schedule: AgentSwarmReplaySchedule,
    /// Optional schema/setup SQL. Empty means infer schema for synthetic traces.
    pub schema_sql: Vec<String>,
    /// Operator commands for smoke and heavy replay paths.
    pub commands: AgentSwarmReplayCommands,
}

impl AgentSwarmReplayConfig {
    /// Build a smoke replay configuration with default commands.
    pub fn smoke(seed: u64) -> Self {
        Self {
            seed,
            schedule: AgentSwarmReplaySchedule::TraceOrder,
            schema_sql: Vec::new(),
            commands: AgentSwarmReplayCommands::default(),
        }
    }

    /// Override setup SQL used before trace statements.
    #[must_use]
    pub fn with_schema_sql(
        mut self,
        schema_sql: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.schema_sql = schema_sql.into_iter().map(Into::into).collect();
        self
    }

    /// Override deterministic scheduling policy.
    #[must_use]
    pub const fn with_schedule(mut self, schedule: AgentSwarmReplaySchedule) -> Self {
        self.schedule = schedule;
        self
    }
}

/// Backend identity metadata captured by a replay run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentSwarmBackendIdentity {
    /// Harness-level backend role.
    pub backend: AgentSwarmReplayBackend,
    /// Executor identity observed from the backend implementation.
    pub engine_identity: String,
    /// Comparison role label.
    pub comparison_role: String,
    /// Whether this backend represents FrankenSQLite's concurrent-writer default.
    pub concurrent_writer_default: bool,
}

/// Normalized statement outcome class for replay summaries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentSwarmReplayOutcomeClass {
    /// Statement completed successfully.
    Success,
    /// Statement produced a busy or locked outcome.
    Busy,
    /// Statement produced a conflict-like outcome.
    Conflict,
    /// Statement produced another error.
    Error,
}

impl AgentSwarmReplayOutcomeClass {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Busy => "busy",
            Self::Conflict => "conflict",
            Self::Error => "error",
        }
    }
}

/// Failure while applying backend setup SQL.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentSwarmSchemaFailure {
    /// Setup statement index.
    pub schema_index: usize,
    /// Setup SQL that failed.
    pub sql: String,
    /// Backend error message.
    pub error: String,
}

/// An actual engine routing event, with no SQL, parameters or database paths.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentSwarmFallbackEvent {
    /// Monotone event identity on the executing connection.
    pub event_id: u64,
    /// Actual engine connection identity, distinct from the trace actor label.
    pub connection_id: Option<u64>,
    /// Actual engine statement identity.
    pub statement_id: u64,
    /// Actual concurrent transaction identity, when present.
    pub transaction_id: Option<u64>,
    /// Engine statement kind.
    pub statement_kind: String,
    /// Stable reason emitted at the routing boundary.
    pub decision_reason: String,
    /// Admission outcome; this does not certify statement success.
    pub decision_outcome: String,
    /// Engine source boundary that emitted the event.
    pub source_context: String,
    /// Actual pager backend.
    pub storage_backend: String,
}

/// Bounded engine evidence emitted during this statement's synchronous replay.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentSwarmFallbackEvidence {
    /// Newly emitted events still retained after execution.
    pub events: Vec<AgentSwarmFallbackEvent>,
    /// Increase in the engine's dropped-event counter during execution.
    /// This can include older events evicted from the shared bounded ring.
    pub dropped_events: u64,
}

/// Replay record for one trace statement on one backend.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentSwarmStatementReplay {
    /// Backend that executed the statement.
    pub backend: AgentSwarmReplayBackend,
    /// Index in the trace's `statements` array.
    pub trace_statement_index: usize,
    /// Deterministic trace order.
    pub logical_order: u64,
    /// Logical actor identity from the trace.
    pub actor_id: String,
    /// Logical connection identity from the trace.
    pub connection_id: String,
    /// Logical transaction identity from the trace.
    pub transaction_id: Option<String>,
    /// Preserved transaction boundary marker.
    pub transaction_boundary: TransactionBoundary,
    /// Preserved concurrency group.
    pub concurrency_group: String,
    /// Preserved workload phase.
    pub workload_phase: String,
    /// Sanitized statement shape hash.
    pub statement_shape: String,
    /// Replay SQL after deterministic literal materialization.
    pub materialized_sql: String,
    /// Expected result class from the trace.
    pub expected_result_class: ExpectedResultClass,
    /// Actual statement outcome.
    pub outcome: StmtOutcome,
    /// Actual fallback evidence; `None` means the executor does not expose it.
    pub fallback_evidence: Option<AgentSwarmFallbackEvidence>,
    /// Normalized outcome class.
    pub outcome_class: AgentSwarmReplayOutcomeClass,
    /// Statement latency in nanoseconds.
    pub latency_ns: u64,
    /// Retry count observed by this deterministic replay path.
    pub retry_count: u32,
    /// Whether actual outcome class matched the trace expectation.
    pub expected_matched: bool,
    /// First-failure diagnostic attached to structured logs.
    pub first_failure_diag: String,
}

/// Aggregated backend replay metrics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentSwarmBackendSummary {
    /// Trace statements attempted on this backend.
    pub statements_total: usize,
    /// Statements that completed successfully.
    pub success_count: usize,
    /// Statements that returned an error-like outcome.
    pub error_count: usize,
    /// Statements whose actual class did not match the trace expectation.
    pub expected_mismatch_count: usize,
    /// Explicit rollback/error abort count.
    pub abort_count: usize,
    /// Total retries observed.
    pub retry_count: u64,
    /// Outcome/error classes seen during replay.
    pub conflict_classes: BTreeMap<String, usize>,
    /// Sum of statement latencies.
    pub latency_total_ns: u64,
    /// p50 latency.
    pub latency_p50_ns: u64,
    /// p95 latency.
    pub latency_p95_ns: u64,
    /// p99 latency.
    pub latency_p99_ns: u64,
    /// Statements per second multiplied by 1000.
    pub throughput_statements_per_second_x1000: u64,
    /// Deterministic high-water estimate for replay inputs held in memory.
    pub memory_high_water_bytes: usize,
}

/// Full replay output for one backend.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentSwarmBackendReplay {
    /// Backend identity metadata.
    pub identity: AgentSwarmBackendIdentity,
    /// Setup SQL failures, if any.
    pub schema_failures: Vec<AgentSwarmSchemaFailure>,
    /// Per-statement replay records.
    pub statements: Vec<AgentSwarmStatementReplay>,
    /// Aggregated metrics.
    pub summary: AgentSwarmBackendSummary,
}

/// Cross-backend statement mismatch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentSwarmBackendMismatch {
    /// Index in the trace's `statements` array.
    pub trace_statement_index: usize,
    /// Deterministic trace order.
    pub logical_order: u64,
    /// Statement shape hash.
    pub statement_shape: String,
    /// FrankenSQLite outcome.
    pub subject_outcome: StmtOutcome,
    /// C SQLite oracle outcome.
    pub reference_outcome: StmtOutcome,
}

/// First-failure bundle with a minimal reproducer trace slice.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentSwarmFirstFailureBundle {
    /// Backend associated with the failure, when backend-local.
    pub backend: Option<AgentSwarmReplayBackend>,
    /// Human-readable deterministic reason code.
    pub reason: String,
    /// Index in the trace's `statements` array.
    pub trace_statement_index: usize,
    /// Trace identifier.
    pub trace_id: String,
    /// Run identifier.
    pub run_id: String,
    /// Scenario identifier.
    pub scenario_id: String,
    /// Minimal transaction/window slice needed to reproduce the failure.
    pub minimal_reproducer_trace_slice: Vec<TraceStatement>,
    /// Materialized SQL for the minimal slice.
    pub materialized_replay_sql: Vec<String>,
}

/// Full agent-swarm replay report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentSwarmReplayReport {
    /// Replay report schema version.
    pub schema_version: String,
    /// Harness implementation version.
    pub harness_version: String,
    /// Trace identifier.
    pub trace_id: String,
    /// Run identifier.
    pub run_id: String,
    /// Scenario identifier.
    pub scenario_id: String,
    /// Replay seed.
    pub seed: u64,
    /// Scheduling policy used.
    pub schedule: AgentSwarmReplaySchedule,
    /// Operator commands for replaying this harness.
    pub commands: AgentSwarmReplayCommands,
    /// Setup statement count.
    pub schema_statement_count: usize,
    /// Trace statement count.
    pub statement_count: usize,
    /// Distinct transaction count.
    pub transaction_count: usize,
    /// Topology validation errors found before execution.
    pub topology_errors: Vec<String>,
    /// Backend replay outputs.
    pub backends: Vec<AgentSwarmBackendReplay>,
    /// Statement-level mismatches between subject and reference backends.
    pub cross_backend_mismatches: Vec<AgentSwarmBackendMismatch>,
    /// First failure bundle, if replay found a failure or mismatch.
    pub first_failure: Option<AgentSwarmFirstFailureBundle>,
}

/// Resource profile identifier for agent-swarm replay scorecards.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentSwarmResourceProfileId {
    /// Small local smoke budget for developer machines.
    LocalSmoke,
    /// Workstation-class profile for focused manual runs.
    Workstation,
    /// High-capacity server profile for 64+ core / 256GB+ RAM swarm runs.
    HighCapacityServer,
}

impl AgentSwarmResourceProfileId {
    const fn as_str(self) -> &'static str {
        match self {
            Self::LocalSmoke => "local_smoke",
            Self::Workstation => "workstation",
            Self::HighCapacityServer => "high_capacity_server",
        }
    }
}

/// Resource envelope profile attached to replay scorecards.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentSwarmResourceProfile {
    /// Stable profile identifier.
    pub profile_id: AgentSwarmResourceProfileId,
    /// Human-readable label for artifacts.
    pub profile_label: String,
    /// CPU cores available to the profile.
    pub core_count: u32,
    /// Memory budget for the profile.
    pub memory_limit_bytes: u64,
    /// Page-cache budget used for scorecard utilization.
    pub page_cache_limit_bytes: u64,
    /// Observation window used for deterministic CPU utilization estimates.
    pub cpu_observation_window_ns: u64,
    /// Maximum local smoke statements expected on developer machines.
    pub local_statement_limit: usize,
    /// Local smoke command shape.
    pub local_smoke_command: String,
    /// CPU-heavy command shape that must be offloaded through rch.
    pub heavy_rch_command: String,
}

impl AgentSwarmResourceProfile {
    /// Local smoke replay profile.
    pub fn local_smoke() -> Self {
        Self {
            profile_id: AgentSwarmResourceProfileId::LocalSmoke,
            profile_label: "local smoke".to_owned(),
            core_count: 4,
            memory_limit_bytes: 512 * MIB,
            page_cache_limit_bytes: 64 * MIB,
            cpu_observation_window_ns: 1_000_000_000,
            local_statement_limit: 250,
            local_smoke_command: "cargo test -p fsqlite-harness agent_swarm_scorecard -- --nocapture"
                .to_owned(),
            heavy_rch_command: "timeout 1200 rch exec -- env CARGO_TARGET_DIR=/data/tmp/frankensqlite-agent-swarm-scorecard cargo test -p fsqlite-harness agent_swarm_scorecard -- --nocapture".to_owned(),
        }
    }

    /// Workstation replay profile.
    pub fn workstation() -> Self {
        Self {
            profile_id: AgentSwarmResourceProfileId::Workstation,
            profile_label: "workstation".to_owned(),
            core_count: 16,
            memory_limit_bytes: 32 * GIB,
            page_cache_limit_bytes: 4 * GIB,
            cpu_observation_window_ns: 5_000_000_000,
            local_statement_limit: 10_000,
            local_smoke_command: "cargo test -p fsqlite-harness agent_swarm_scorecard -- --nocapture"
                .to_owned(),
            heavy_rch_command: "timeout 1200 rch exec -- env CARGO_TARGET_DIR=/data/tmp/frankensqlite-agent-swarm-scorecard cargo test -p fsqlite-harness agent_swarm_scorecard -- --nocapture".to_owned(),
        }
    }

    /// High-capacity replay profile for large agent-swarm machines.
    pub fn high_capacity_server() -> Self {
        Self {
            profile_id: AgentSwarmResourceProfileId::HighCapacityServer,
            profile_label: "high capacity server".to_owned(),
            core_count: 64,
            memory_limit_bytes: 256 * GIB,
            page_cache_limit_bytes: 64 * GIB,
            cpu_observation_window_ns: 30_000_000_000,
            local_statement_limit: 100_000,
            local_smoke_command: "cargo test -p fsqlite-harness agent_swarm_scorecard -- --nocapture"
                .to_owned(),
            heavy_rch_command: "timeout 1200 rch exec -- env CARGO_TARGET_DIR=/data/tmp/frankensqlite-agent-swarm-scorecard cargo test -p fsqlite-harness agent_swarm_scorecard -- --nocapture".to_owned(),
        }
    }
}

/// Default resource profiles for smoke, workstation, and high-capacity runs.
pub fn default_agent_swarm_resource_profiles() -> Vec<AgentSwarmResourceProfile> {
    vec![
        AgentSwarmResourceProfile::local_smoke(),
        AgentSwarmResourceProfile::workstation(),
        AgentSwarmResourceProfile::high_capacity_server(),
    ]
}

/// Whether a scorecard is ready to support README numeric performance claims.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentSwarmPerformanceClaimStatus {
    /// Profile/scorecard only; numeric README claims still need artifact evidence.
    ProfileOnly,
    /// Numeric README claims can cite the attached benchmark artifact metadata.
    ArtifactBacked,
}

/// Guardrail metadata for README performance claims.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentSwarmPerformanceClaimGuard {
    /// Current claim status.
    pub status: AgentSwarmPerformanceClaimStatus,
    /// Benchmark artifact path, when a numeric claim is artifact-backed.
    pub benchmark_artifact_path: Option<String>,
    /// Commit used for the benchmark artifact, when known.
    pub benchmark_commit: Option<String>,
    /// Benchmark run date, when a numeric claim is artifact-backed.
    pub benchmark_run_date: Option<String>,
    /// Policy text copied into artifacts to prevent uncited numeric claims.
    pub policy: String,
}

impl AgentSwarmPerformanceClaimGuard {
    /// Build a guard for profile-only scorecards.
    pub fn profile_only() -> Self {
        Self {
            status: AgentSwarmPerformanceClaimStatus::ProfileOnly,
            benchmark_artifact_path: None,
            benchmark_commit: None,
            benchmark_run_date: None,
            policy: "README numeric performance claims require a benchmark artifact path, commit, and run date; this scorecard alone is profile-only evidence.".to_owned(),
        }
    }

    /// Build a guard for scorecards tied to a benchmark artifact.
    pub fn artifact_backed(
        artifact_path: impl Into<String>,
        commit: impl Into<String>,
        run_date: impl Into<String>,
    ) -> Self {
        Self {
            status: AgentSwarmPerformanceClaimStatus::ArtifactBacked,
            benchmark_artifact_path: Some(artifact_path.into()),
            benchmark_commit: Some(commit.into()),
            benchmark_run_date: Some(run_date.into()),
            policy: "README numeric performance claims may cite this scorecard only with its artifact path, commit, and run date.".to_owned(),
        }
    }
}

/// Configuration for producing a resource envelope scorecard.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentSwarmResourceScorecardConfig {
    /// Resource profile to score against.
    pub profile: AgentSwarmResourceProfile,
    /// README performance-claim guardrail metadata.
    pub performance_claim_guard: AgentSwarmPerformanceClaimGuard,
}

impl AgentSwarmResourceScorecardConfig {
    /// Build scorecard config for a profile-only run.
    pub fn new(profile: AgentSwarmResourceProfile) -> Self {
        Self {
            profile,
            performance_claim_guard: AgentSwarmPerformanceClaimGuard::profile_only(),
        }
    }

    /// Attach benchmark artifact metadata so README numeric claims can cite it.
    pub fn with_readme_artifact(
        mut self,
        artifact_path: impl Into<String>,
        commit: impl Into<String>,
        run_date: impl Into<String>,
    ) -> Self {
        self.performance_claim_guard =
            AgentSwarmPerformanceClaimGuard::artifact_backed(artifact_path, commit, run_date);
        self
    }
}

/// Coordination scenario family surfaced by replay scorecards.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentSwarmCoordinationScenario {
    /// Baseline successful coordination path.
    Normal,
    /// Conflict-heavy replay or retry path.
    ConflictHeavy,
    /// Compatibility fallback path is intentionally visible.
    FallbackHeavy,
    /// Lease expiration or takeover path.
    ExpiredLease,
    /// Worker-range imbalance or split/merge path.
    RangeImbalance,
}

/// Replay-lab bridge metrics for coordination primitives and SLO inputs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentSwarmCoordinationMetrics {
    /// Bridge implementation version.
    pub bridge_version: String,
    /// Scenario families covered by this replay backend.
    pub scenarios: Vec<AgentSwarmCoordinationScenario>,
    /// Queue claim operations observed by replay.
    pub queue_claim_count: u64,
    /// Queue release or completion operations observed by replay.
    pub queue_release_count: u64,
    /// Lease acquire operations observed by replay.
    pub lease_acquire_count: u64,
    /// Lease renew operations observed by replay.
    pub lease_renew_count: u64,
    /// Lease expiration or takeover operations observed by replay.
    pub lease_expiration_count: u64,
    /// Worker-range allocation/split/merge operations observed by replay.
    pub range_allocation_count: u64,
    /// Actual EXPLAIN CONCURRENCY rows, unavailable without an engine producer.
    pub explain_concurrency_row_count: Option<u64>,
    /// Actual newly emitted fallback events retained by the replay executor.
    pub fallback_reason_count: u64,
    /// Actual governor decisions, unavailable without an engine producer.
    pub resource_governor_decision_count: Option<u64>,
    /// Score of expected-vs-actual coordination outcomes, per mille.
    pub coordination_correctness_per_mille: u16,
    /// Score of conflict/fallback diagnostic visibility, per mille.
    pub conflict_transparency_per_mille: u16,
    /// Fairness/resource pressure, per mille, where higher means more pressure.
    pub fairness_resource_pressure_per_mille: u16,
    /// Whether the replay inputs still satisfy the privacy scrubber contract.
    pub privacy_scrubber_preserved: bool,
    /// Field names that must stay artifact-visible and scrubbed.
    pub trace_fields: Vec<String>,
    /// First coordination bridge diagnostic.
    pub first_failure_diag: String,
}

/// Resource and fairness scorecard for one replay backend.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentSwarmBackendResourceScorecard {
    /// Backend that produced the scorecard.
    pub backend: AgentSwarmReplayBackend,
    /// Comparison role label.
    pub comparison_role: String,
    /// Whether this backend is FrankenSQLite's concurrent-writer default.
    pub concurrent_writer_default: bool,
    /// CPU cores in the active profile.
    pub core_count: u32,
    /// Memory limit in the active profile.
    pub memory_limit_bytes: u64,
    /// Deterministic CPU utilization estimate, per mille of profile capacity.
    pub cpu_utilization_per_mille: u16,
    /// Basis for the CPU utilization estimate.
    pub cpu_utilization_basis: String,
    /// Replay memory high-water estimate.
    pub memory_high_water_bytes: u64,
    /// Whether replay high-water exceeded the profile memory budget.
    pub memory_limit_exceeded: bool,
    /// Estimated page-cache footprint counted against the profile budget.
    pub page_cache_footprint_bytes: u64,
    /// Page-cache limit in the active profile.
    pub page_cache_limit_bytes: u64,
    /// Page-cache utilization, per mille of the profile budget.
    pub page_cache_utilization_per_mille: u16,
    /// Backend abort/error rate per thousand statements.
    pub abort_rate_per_mille: u64,
    /// Backend retry rate per thousand statements.
    pub retry_rate_per_mille: u64,
    /// Jain fairness index over per-actor throughput, per mille.
    pub fairness_index_per_mille: u16,
    /// Statement counts by actor.
    pub per_actor_statement_count: BTreeMap<String, usize>,
    /// Per-actor throughput using the backend replay window.
    pub per_actor_throughput_statements_per_second_x1000: BTreeMap<String, u64>,
    /// p50 latency in nanoseconds.
    pub latency_p50_ns: u64,
    /// p95 latency in nanoseconds.
    pub latency_p95_ns: u64,
    /// p99 latency in nanoseconds.
    pub latency_p99_ns: u64,
    /// Tail spread between p99 and p50.
    pub tail_latency_spread_ns: u64,
    /// Backend throughput.
    pub throughput_statements_per_second_x1000: u64,
    /// Coordination bridge metrics consumed by replay and SLO proof packs.
    pub coordination_metrics: AgentSwarmCoordinationMetrics,
}

/// Full resource envelope and fairness scorecard.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentSwarmResourceScorecard {
    /// Scorecard schema version.
    pub schema_version: String,
    /// Scorecard implementation version.
    pub scorecard_version: String,
    /// Trace identifier.
    pub trace_id: String,
    /// Run identifier.
    pub run_id: String,
    /// Scenario identifier.
    pub scenario_id: String,
    /// Resource profile used for scoring.
    pub profile: AgentSwarmResourceProfile,
    /// README performance-claim guardrail metadata.
    pub performance_claim_guard: AgentSwarmPerformanceClaimGuard,
    /// First-failure diagnostic copied from the replay report.
    pub first_failure_diag: String,
    /// Backend scorecards.
    pub backends: Vec<AgentSwarmBackendResourceScorecard>,
}

/// Replay failure category used by evidence manifests and regression proposals.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentSwarmFailureCategory {
    /// SQL parser or syntax surface.
    Parser,
    /// Query planner surface.
    Planner,
    /// VDBE bytecode or execution surface.
    Vdbe,
    /// Pager or page-cache surface.
    Pager,
    /// WAL or durability surface.
    Wal,
    /// MVCC/concurrent-writer/conflict surface.
    Mvcc,
    /// Extension-specific surface.
    Extension,
    /// Resource envelope, memory, CPU, or fairness surface.
    ResourceEnvelope,
    /// Unclassified or no failure.
    Unknown,
}

impl AgentSwarmFailureCategory {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Parser => "parser",
            Self::Planner => "planner",
            Self::Vdbe => "vdbe",
            Self::Pager => "pager",
            Self::Wal => "wal",
            Self::Mvcc => "mvcc",
            Self::Extension => "extension",
            Self::ResourceEnvelope => "resource_envelope",
            Self::Unknown => "unknown",
        }
    }
}

/// Existing Beads summary used for duplicate detection before proposing regressions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentSwarmExistingBeadSummary {
    /// Existing Bead id.
    pub id: String,
    /// Existing Bead title.
    pub title: String,
    /// Existing Bead status.
    pub status: String,
    /// Existing Bead labels.
    pub labels: Vec<String>,
    /// Explicit duplicate keys such as minimized trace hashes or trace ids.
    pub duplicate_keys: Vec<String>,
}

impl AgentSwarmExistingBeadSummary {
    /// Create a summary with no labels or duplicate keys.
    pub fn new(id: impl Into<String>, title: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            title: title.into(),
            status: "open".to_owned(),
            labels: Vec::new(),
            duplicate_keys: Vec::new(),
        }
    }

    /// Attach labels used by duplicate detection.
    pub fn with_labels(mut self, labels: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.labels = labels.into_iter().map(Into::into).collect();
        self
    }

    /// Attach explicit duplicate keys used by duplicate detection.
    pub fn with_duplicate_keys(
        mut self,
        duplicate_keys: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.duplicate_keys = duplicate_keys.into_iter().map(Into::into).collect();
        self
    }
}

/// Configuration for producing an evidence manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentSwarmEvidenceManifestConfig {
    /// Path to the full trace artifact.
    pub trace_artifact_path: String,
    /// Optional precomputed hash of the full trace artifact.
    pub trace_artifact_hash: Option<String>,
    /// Command line used to produce the replay report.
    pub replay_command: String,
    /// Directory/prefix for derived evidence artifacts.
    pub artifact_root: String,
    /// Existing Beads checked before proposing new regression work.
    pub existing_beads: Vec<AgentSwarmExistingBeadSummary>,
    /// Whether duplicate-free failures should emit patch-ready Bead proposals.
    pub propose_regression_bead: bool,
}

impl AgentSwarmEvidenceManifestConfig {
    /// Build evidence-manifest config for a trace artifact and replay command.
    pub fn new(trace_artifact_path: impl Into<String>, replay_command: impl Into<String>) -> Self {
        Self {
            trace_artifact_path: trace_artifact_path.into(),
            trace_artifact_hash: None,
            replay_command: replay_command.into(),
            artifact_root: "tests/artifacts/agent-swarm-replay".to_owned(),
            existing_beads: Vec::new(),
            propose_regression_bead: true,
        }
    }

    /// Attach a known trace artifact hash.
    pub fn with_trace_artifact_hash(mut self, trace_artifact_hash: impl Into<String>) -> Self {
        self.trace_artifact_hash = Some(trace_artifact_hash.into());
        self
    }

    /// Override the artifact root used for minimized traces.
    pub fn with_artifact_root(mut self, artifact_root: impl Into<String>) -> Self {
        self.artifact_root = artifact_root.into();
        self
    }

    /// Attach existing Beads for duplicate checks.
    pub fn with_existing_beads(
        mut self,
        existing_beads: impl IntoIterator<Item = AgentSwarmExistingBeadSummary>,
    ) -> Self {
        self.existing_beads = existing_beads.into_iter().collect();
        self
    }

    /// Disable regression proposal emission.
    pub const fn without_regression_proposals(mut self) -> Self {
        self.propose_regression_bead = false;
        self
    }
}

/// Backend identity snapshot included in evidence manifests.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentSwarmEvidenceBackendVersion {
    /// Backend role.
    pub backend: AgentSwarmReplayBackend,
    /// Executor identity string.
    pub engine_identity: String,
    /// Comparison role.
    pub comparison_role: String,
    /// Whether the backend is FrankenSQLite's concurrent-writer default path.
    pub concurrent_writer_default: bool,
}

/// Backend result metrics included in evidence manifests.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentSwarmEvidenceBackendMetrics {
    /// Backend role.
    pub backend: AgentSwarmReplayBackend,
    /// Statement count.
    pub statements_total: usize,
    /// Throughput from the replay report.
    pub throughput_statements_per_second_x1000: u64,
    /// p50 latency.
    pub latency_p50_ns: u64,
    /// p95 latency.
    pub latency_p95_ns: u64,
    /// p99 latency.
    pub latency_p99_ns: u64,
    /// Abort/error count.
    pub abort_count: usize,
    /// Retry count.
    pub retry_count: u64,
    /// Expected-result mismatch count.
    pub expected_mismatch_count: usize,
    /// Conflict/error classes.
    pub conflict_classes: BTreeMap<String, usize>,
    /// Replay memory high-water estimate.
    pub memory_high_water_bytes: usize,
}

/// Patch-ready regression Bead proposal emitted only for duplicate-free failures.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentSwarmRegressionBeadProposal {
    /// Deterministic proposed Bead id/key.
    pub proposed_bead_id: String,
    /// Proposed title.
    pub title: String,
    /// Proposed description body.
    pub description: String,
    /// Proposed labels.
    pub labels: Vec<String>,
    /// Duplicate key future agents should record on the created Bead.
    pub duplicate_key: String,
}

/// Machine-readable replay evidence manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentSwarmEvidenceManifest {
    /// Manifest schema version.
    pub schema_version: String,
    /// Manifest implementation version.
    pub manifest_version: String,
    /// Trace identifier.
    pub trace_id: String,
    /// Run identifier.
    pub run_id: String,
    /// Scenario identifier.
    pub scenario_id: String,
    /// Stable hash of this evidence artifact payload.
    pub artifact_hash: String,
    /// Path to the full trace artifact.
    pub trace_artifact_path: String,
    /// Hash of the full trace artifact or deterministic path fallback.
    pub trace_artifact_hash: String,
    /// Command line used to produce the replay report.
    pub replay_command: String,
    /// Smoke command embedded in the replay report.
    pub smoke_command: String,
    /// Heavy rch command embedded in the replay report.
    pub heavy_command: String,
    /// Backend identity snapshots.
    pub backend_versions: Vec<AgentSwarmEvidenceBackendVersion>,
    /// Backend result metrics.
    pub result_metrics: Vec<AgentSwarmEvidenceBackendMetrics>,
    /// First-failure diagnostic.
    pub first_failure_diag: String,
    /// Assigned failure category.
    pub failure_category: AgentSwarmFailureCategory,
    /// Hash of the minimized failing trace slice.
    pub minimized_trace_hash: String,
    /// Path where the minimized trace slice should be stored.
    pub minimized_trace_artifact_path: Option<String>,
    /// Minimized failing trace slice.
    pub minimized_trace_slice: Vec<TraceStatement>,
    /// Duplicate candidate Bead ids found before proposal.
    pub duplicate_candidate_ids: Vec<String>,
    /// Patch-ready proposal for duplicate-free reproducible failures.
    pub proposed_regression_bead: Option<AgentSwarmRegressionBeadProposal>,
}

/// Backend-specific row in the CI smoke replay artifact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentSwarmCiSmokeBackendArtifact {
    /// Backend that produced these smoke metrics.
    pub backend: AgentSwarmReplayBackend,
    /// Executor identity observed during replay.
    pub engine_identity: String,
    /// Resource profile used for the smoke scorecard.
    pub profile_id: AgentSwarmResourceProfileId,
    /// Statements replayed on this backend.
    pub statement_count: usize,
    /// Expected-result mismatches observed on this backend.
    pub expected_mismatch_count: usize,
    /// Abort/error count observed on this backend.
    pub abort_count: usize,
    /// Retry count observed on this backend.
    pub retry_count: u64,
    /// p95 latency recorded by the deterministic replay report.
    pub latency_p95_ns: u64,
    /// First-failure diagnostic for this backend, or `none`.
    pub first_failure_diag: String,
}

/// Machine-readable artifact emitted by the fast CI smoke replay path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentSwarmCiSmokeArtifact {
    /// Serialized trace schema version validated by the smoke path.
    pub schema_version: String,
    /// CI smoke artifact contract version.
    pub smoke_version: String,
    /// Scrubber version validated by the smoke path.
    pub scrubber_version: String,
    /// Replay harness version used by the smoke path.
    pub replay_harness_version: String,
    /// Evidence manifest version linked by the smoke artifact.
    pub evidence_manifest_version: String,
    /// Stable hash of the smoke artifact payload.
    pub artifact_hash: String,
    /// Trace identifier shared across logs and artifacts.
    pub trace_id: String,
    /// Replay or capture run identifier.
    pub run_id: String,
    /// Scenario identifier shared with logs and artifacts.
    pub scenario_id: String,
    /// Command line used to produce the smoke replay report.
    pub command: String,
    /// Resource profile used for this smoke replay.
    pub profile_id: AgentSwarmResourceProfileId,
    /// Path to the full trace artifact.
    pub trace_artifact_path: String,
    /// Hash of the full trace artifact.
    pub trace_artifact_hash: String,
    /// Path where the evidence manifest is stored.
    pub artifact_manifest_path: String,
    /// Hash of the linked evidence manifest payload.
    pub evidence_manifest_hash: String,
    /// First-failure diagnostic for the overall smoke run, or `none`.
    pub first_failure_diag: String,
    /// Backend-specific smoke rows.
    pub backend_artifacts: Vec<AgentSwarmCiSmokeBackendArtifact>,
}

/// Scrub sensitive SQL literals while preserving statement topology.
pub fn scrub_sql_statement(sql: &str) -> ScrubbedSql {
    let chars = sql.chars().collect::<Vec<_>>();
    let mut out = String::with_capacity(sql.len());
    let mut redactions = RedactionCounts::default();
    let mut parameter_count = 0;
    let mut i = 0;

    while i < chars.len() {
        if starts_blob_literal(&chars, i) {
            out.push_str("?b");
            redactions.blob_literals += 1;
            i = skip_single_quoted(&chars, i + 2);
            continue;
        }

        let ch = chars[i];
        if ch == '\'' {
            out.push_str("?s");
            redactions.string_literals += 1;
            i = skip_single_quoted(&chars, i + 1);
            continue;
        }

        if ch == '-' && chars.get(i + 1) == Some(&'-') {
            out.push_str("--?c");
            redactions.comments += 1;
            i += 2;
            while i < chars.len() && chars[i] != '\n' {
                i += 1;
            }
            continue;
        }

        if ch == '/' && chars.get(i + 1) == Some(&'*') {
            out.push_str("/*?c*/");
            redactions.comments += 1;
            i += 2;
            while i + 1 < chars.len() && !(chars[i] == '*' && chars[i + 1] == '/') {
                i += 1;
            }
            if i + 1 < chars.len() {
                i += 2;
            }
            continue;
        }

        if starts_numeric_literal(&chars, i) {
            out.push_str("?n");
            redactions.numeric_literals += 1;
            i = consume_numeric_literal(&chars, i);
            continue;
        }

        if let Some(parameter_end) = consume_parameter(&chars, i) {
            parameter_count += 1;
            for parameter_char in &chars[i..parameter_end] {
                out.push(*parameter_char);
            }
            i = parameter_end;
            continue;
        }

        out.push(ch);
        i += 1;
    }

    let statement_shape = sha256_hex(out.as_bytes());
    ScrubbedSql {
        sql: out,
        statement_shape,
        parameter_count,
        redactions,
    }
}

/// Generate a deterministic synthetic trace from a fixed seed and workload mix.
pub fn generate_synthetic_swarm_trace(
    config: SyntheticTraceConfig,
) -> Result<AgentSwarmTrace, SyntheticTraceError> {
    config.validate()?;

    let mut rng = SyntheticTraceRng::new(config.seed);
    let mut statements = Vec::with_capacity(config.transaction_count.saturating_mul(3));
    let mut logical_order = 0_u64;

    for tx_index in 0..config.transaction_count {
        let context = synthetic_transaction_context(&config, &mut rng, tx_index);
        statements.push(synthetic_statement(
            logical_order,
            &context,
            SyntheticStatementSpec::begin(),
        )?);
        logical_order += 1;

        let is_read = config.choose_read(&mut rng);
        let sequence = rng.next_u64();
        statements.push(synthetic_statement(
            logical_order,
            &context,
            synthetic_operation_spec(&config, &context, sequence, is_read),
        )?);
        logical_order += 1;

        statements.push(synthetic_statement(
            logical_order,
            &context,
            SyntheticStatementSpec::commit(),
        )?);
        logical_order += 1;
    }

    let trace = AgentSwarmTrace::new(
        synthetic_trace_id(&config),
        synthetic_run_id(&config),
        synthetic_scenario_id(config.scenario),
        synthetic_trace_metadata(&config)?,
        statements,
    )?;
    log_synthetic_swarm_trace_generation(&config, &trace, None);
    Ok(trace)
}

/// Emit the structured summary fields required by replay-lab operators.
pub fn log_swarm_trace_scrub_summary(trace: &AgentSwarmTrace, first_failure_diag: Option<&str>) {
    tracing::info!(
        target: "fsqlite.agent_swarm_trace",
        trace_id = %trace.trace_id,
        run_id = %trace.run_id,
        scenario_id = %trace.scenario_id,
        scrubber_version = %trace.scrubber_version,
        redaction_count = trace.redaction_summary.redaction_count,
        statement_count = trace.statement_count(),
        transaction_count = trace.transaction_count(),
        first_failure_diag = first_failure_diag.unwrap_or(FIRST_FAILURE_DIAGNOSTIC_ABSENT),
        "agent swarm trace scrub summary",
    );
}

/// Emit the structured fields required for synthetic trace generation logs.
pub fn log_synthetic_swarm_trace_generation(
    config: &SyntheticTraceConfig,
    trace: &AgentSwarmTrace,
    first_failure_diag: Option<&str>,
) {
    tracing::info!(
        target: "fsqlite.agent_swarm_trace.synthetic",
        trace_id = %trace.trace_id,
        run_id = %trace.run_id,
        scenario_id = %trace.scenario_id,
        seed = config.seed,
        agent_count = config.agent_count,
        transaction_count = config.transaction_count,
        hot_key_ratio = config.hot_key_ratio,
        read_write_mix = %config.read_write_mix(),
        generator_version = SYNTHETIC_TRACE_GENERATOR_VERSION,
        first_failure_diag = first_failure_diag.unwrap_or(FIRST_FAILURE_DIAGNOSTIC_ABSENT),
        "synthetic agent swarm trace generated",
    );
}

/// Decode a JSON trace fixture and validate its schema version.
pub fn load_agent_swarm_trace_json(json: &str) -> Result<AgentSwarmTrace, AgentSwarmReplayError> {
    let trace = serde_json::from_str(json)
        .map_err(|error| AgentSwarmReplayError::JsonDecode(error.to_string()))?;
    validate_agent_swarm_trace_schema(&trace)?;
    Ok(trace)
}

/// Load a JSON trace fixture from disk and validate its schema version.
pub fn load_agent_swarm_trace_file(
    path: impl AsRef<Path>,
) -> Result<AgentSwarmTrace, AgentSwarmReplayError> {
    let json = fs::read_to_string(path)
        .map_err(|error| AgentSwarmReplayError::FixtureRead(error.to_string()))?;
    load_agent_swarm_trace_json(&json)
}

/// Materialize scrubber placeholders into deterministic safe literals.
#[must_use]
pub fn materialize_scrubbed_sql(sql: &str) -> String {
    let chars = sql.chars().collect::<Vec<_>>();
    let mut out = String::with_capacity(sql.len());
    let mut i = 0;
    while i < chars.len() {
        match (chars.get(i), chars.get(i + 1)) {
            (Some('?'), Some('s')) => {
                out.push_str("'redacted-text'");
                i += 2;
            }
            (Some('?'), Some('n')) => {
                out.push('0');
                i += 2;
            }
            (Some('?'), Some('b')) => {
                out.push_str("X''");
                i += 2;
            }
            (Some(ch), _) => {
                out.push(*ch);
                i += 1;
            }
            (None, _) => break,
        }
    }
    out
}

/// Return deterministic setup SQL for synthetic agent-swarm scenarios.
#[must_use]
pub fn synthetic_replay_schema(scenario: SyntheticTraceScenario) -> Vec<String> {
    match scenario {
        SyntheticTraceScenario::SessionEventAppend => vec![
            "CREATE TABLE session_events(session_id TEXT, agent_id TEXT, event_kind TEXT, payload TEXT, seq INTEGER, logical_clock INTEGER, event_id INTEGER PRIMARY KEY);".to_owned(),
        ],
        SyntheticTraceScenario::TaskQueueClaimLoop => vec![
            "CREATE TABLE task_queue(id INTEGER PRIMARY KEY, shard TEXT, status TEXT, priority INTEGER, owner TEXT, claim_seq INTEGER);".to_owned(),
            "INSERT INTO task_queue(shard, status, priority) VALUES ('redacted-text', 'ready', 100);".to_owned(),
            "INSERT INTO task_queue(shard, status, priority) VALUES ('redacted-text', 'ready', 90);".to_owned(),
        ],
        SyntheticTraceScenario::ArtifactIndexMixedLookup => vec![
            "CREATE TABLE artifact_index(workspace TEXT, artifact_id TEXT, path_hash TEXT, content_hash TEXT, updated_by TEXT);".to_owned(),
        ],
        SyntheticTraceScenario::HotCounterStatusRows => vec![
            "CREATE TABLE swarm_counters(counter_key TEXT PRIMARY KEY, value INTEGER, updated_by TEXT, update_seq INTEGER);".to_owned(),
            "INSERT INTO swarm_counters(counter_key, value, updated_by, update_seq) VALUES ('redacted-text', 0, 'seed', 0);".to_owned(),
        ],
        SyntheticTraceScenario::LongReaderBurstWriter => vec![
            "CREATE TABLE session_events(session_id TEXT, agent_id TEXT, event_kind TEXT, payload TEXT, seq INTEGER, logical_clock INTEGER, event_id INTEGER PRIMARY KEY);".to_owned(),
            "CREATE TABLE session_status(session_id TEXT, agent_id TEXT, status TEXT, logical_clock INTEGER);".to_owned(),
        ],
    }
}

/// Replay a trace against FrankenSQLite concurrent default and C SQLite oracle backends.
pub fn replay_agent_swarm_trace(
    trace: &AgentSwarmTrace,
    config: &AgentSwarmReplayConfig,
) -> Result<AgentSwarmReplayReport, AgentSwarmReplayError> {
    let fsqlite = FsqliteExecutor::open_in_memory().map_err(|message| {
        AgentSwarmReplayError::BackendOpen {
            backend: AgentSwarmReplayBackend::FrankenSqliteConcurrent,
            message,
        }
    })?;
    let csqlite = CsqliteExecutor::open_in_memory().map_err(|message| {
        AgentSwarmReplayError::BackendOpen {
            backend: AgentSwarmReplayBackend::CSqliteOracle,
            message,
        }
    })?;
    replay_agent_swarm_trace_with_executors(trace, config, &fsqlite, &csqlite)
}

/// Replay a trace and write a durable evidence manifest plus minimized trace artifact.
pub fn replay_agent_swarm_trace_with_evidence(
    trace: &AgentSwarmTrace,
    replay_config: &AgentSwarmReplayConfig,
    evidence_config: &AgentSwarmEvidenceManifestConfig,
    manifest_path: impl AsRef<Path>,
) -> Result<(AgentSwarmReplayReport, AgentSwarmEvidenceManifest), AgentSwarmReplayError> {
    let report = replay_agent_swarm_trace(trace, replay_config)?;
    let manifest = build_agent_swarm_evidence_manifest(&report, evidence_config);
    write_agent_swarm_evidence_manifest_files(&manifest, manifest_path)?;
    Ok((report, manifest))
}

/// Replay a trace against caller-provided subject and reference executors.
pub fn replay_agent_swarm_trace_with_executors<F, C>(
    trace: &AgentSwarmTrace,
    config: &AgentSwarmReplayConfig,
    fsqlite_exec: &F,
    csqlite_exec: &C,
) -> Result<AgentSwarmReplayReport, AgentSwarmReplayError>
where
    F: SqlExecutor,
    C: SqlExecutor,
{
    validate_agent_swarm_trace_schema(trace)?;

    let schema_sql = replay_schema_sql(trace, config);
    let scheduled_indices = scheduled_statement_indices(trace, config.schedule);
    let topology_errors = validate_trace_topology(trace);
    let memory_high_water_bytes = estimate_memory_high_water(trace, &schema_sql);

    let subject = replay_backend(
        AgentSwarmReplayBackend::FrankenSqliteConcurrent,
        fsqlite_exec,
        trace,
        &schema_sql,
        &scheduled_indices,
        memory_high_water_bytes,
    );
    let reference = replay_backend(
        AgentSwarmReplayBackend::CSqliteOracle,
        csqlite_exec,
        trace,
        &schema_sql,
        &scheduled_indices,
        memory_high_water_bytes,
    );
    let cross_backend_mismatches = compare_backend_records(&subject, &reference);
    let backends = vec![subject, reference];
    let first_failure = build_first_failure_bundle(
        trace,
        &backends,
        &cross_backend_mismatches,
        &topology_errors,
    );

    let report = AgentSwarmReplayReport {
        schema_version: SWARM_TRACE_SCHEMA_VERSION.to_owned(),
        harness_version: AGENT_SWARM_REPLAY_HARNESS_VERSION.to_owned(),
        trace_id: trace.trace_id.clone(),
        run_id: trace.run_id.clone(),
        scenario_id: trace.scenario_id.clone(),
        seed: config.seed,
        schedule: config.schedule,
        commands: config.commands.clone(),
        schema_statement_count: schema_sql.len(),
        statement_count: trace.statement_count(),
        transaction_count: trace.transaction_count(),
        topology_errors,
        backends,
        cross_backend_mismatches,
        first_failure,
    };
    log_agent_swarm_replay_summary(&report);
    Ok(report)
}

/// Score a replay report against a resource envelope profile.
pub fn score_agent_swarm_resource_envelope(
    report: &AgentSwarmReplayReport,
    config: &AgentSwarmResourceScorecardConfig,
) -> AgentSwarmResourceScorecard {
    let first_failure_diag = report
        .first_failure
        .as_ref()
        .map_or(FIRST_FAILURE_DIAGNOSTIC_ABSENT, |failure| {
            failure.reason.as_str()
        })
        .to_owned();
    let backends = report
        .backends
        .iter()
        .map(|backend| score_backend_resource_envelope(backend, &config.profile))
        .collect();
    let scorecard = AgentSwarmResourceScorecard {
        schema_version: SWARM_TRACE_SCHEMA_VERSION.to_owned(),
        scorecard_version: AGENT_SWARM_RESOURCE_SCORECARD_VERSION.to_owned(),
        trace_id: report.trace_id.clone(),
        run_id: report.run_id.clone(),
        scenario_id: report.scenario_id.clone(),
        profile: config.profile.clone(),
        performance_claim_guard: config.performance_claim_guard.clone(),
        first_failure_diag,
        backends,
    };
    log_agent_swarm_resource_scorecard(&scorecard);
    scorecard
}

/// Build a durable evidence manifest from a replay report.
pub fn build_agent_swarm_evidence_manifest(
    report: &AgentSwarmReplayReport,
    config: &AgentSwarmEvidenceManifestConfig,
) -> AgentSwarmEvidenceManifest {
    let first_failure_diag = report
        .first_failure
        .as_ref()
        .map_or(FIRST_FAILURE_DIAGNOSTIC_ABSENT, |failure| {
            failure.reason.as_str()
        })
        .to_owned();
    let failure_category = classify_agent_swarm_failure(report);
    let minimized_trace_slice = report
        .first_failure
        .as_ref()
        .map_or_else(Vec::new, |failure| {
            failure.minimal_reproducer_trace_slice.clone()
        });
    let minimized_trace_hash = stable_json_hash(&minimized_trace_slice);
    let minimized_trace_artifact_path = report.first_failure.as_ref().map(|_| {
        format!(
            "{}/{}-{}-minimized-trace.json",
            config.artifact_root, report.trace_id, report.run_id
        )
    });
    let trace_artifact_hash = config
        .trace_artifact_hash
        .clone()
        .unwrap_or_else(|| sha256_hex(config.trace_artifact_path.as_bytes()));
    let backend_versions = report
        .backends
        .iter()
        .map(evidence_backend_version)
        .collect::<Vec<_>>();
    let result_metrics = report
        .backends
        .iter()
        .map(evidence_backend_metrics)
        .collect::<Vec<_>>();
    let duplicate_candidate_ids = duplicate_candidate_ids(
        config,
        report,
        failure_category,
        &minimized_trace_hash,
        &first_failure_diag,
    );
    let proposed_regression_bead = if config.propose_regression_bead
        && report.first_failure.is_some()
        && duplicate_candidate_ids.is_empty()
    {
        Some(regression_bead_proposal(
            report,
            failure_category,
            &minimized_trace_hash,
            &first_failure_diag,
        ))
    } else {
        None
    };
    let artifact_hash = evidence_artifact_hash(
        report,
        config,
        &trace_artifact_hash,
        failure_category,
        &minimized_trace_hash,
        &duplicate_candidate_ids,
        &result_metrics,
    );
    let manifest = AgentSwarmEvidenceManifest {
        schema_version: SWARM_TRACE_SCHEMA_VERSION.to_owned(),
        manifest_version: AGENT_SWARM_EVIDENCE_MANIFEST_VERSION.to_owned(),
        trace_id: report.trace_id.clone(),
        run_id: report.run_id.clone(),
        scenario_id: report.scenario_id.clone(),
        artifact_hash,
        trace_artifact_path: config.trace_artifact_path.clone(),
        trace_artifact_hash,
        replay_command: config.replay_command.clone(),
        smoke_command: report.commands.smoke_command.clone(),
        heavy_command: report.commands.heavy_command.clone(),
        backend_versions,
        result_metrics,
        first_failure_diag,
        failure_category,
        minimized_trace_hash,
        minimized_trace_artifact_path,
        minimized_trace_slice,
        duplicate_candidate_ids,
        proposed_regression_bead,
    };
    log_agent_swarm_evidence_manifest(&manifest);
    manifest
}

/// Build the compact artifact used by CI to prove the smoke replay path.
pub fn build_agent_swarm_ci_smoke_artifact(
    trace: &AgentSwarmTrace,
    report: &AgentSwarmReplayReport,
    scorecard: &AgentSwarmResourceScorecard,
    manifest: &AgentSwarmEvidenceManifest,
    artifact_manifest_path: impl Into<String>,
) -> AgentSwarmCiSmokeArtifact {
    let first_failure_diag = report
        .first_failure
        .as_ref()
        .map_or(FIRST_FAILURE_DIAGNOSTIC_ABSENT, |failure| {
            failure.reason.as_str()
        })
        .to_owned();
    let backend_artifacts = report
        .backends
        .iter()
        .map(|backend| {
            let scorecard_backend = scorecard
                .backends
                .iter()
                .find(|candidate| candidate.backend == backend.identity.backend);
            AgentSwarmCiSmokeBackendArtifact {
                backend: backend.identity.backend,
                engine_identity: backend.identity.engine_identity.clone(),
                profile_id: scorecard.profile.profile_id,
                statement_count: backend.summary.statements_total,
                expected_mismatch_count: backend.summary.expected_mismatch_count,
                abort_count: backend.summary.abort_count,
                retry_count: backend.summary.retry_count,
                latency_p95_ns: scorecard_backend
                    .map_or(backend.summary.latency_p95_ns, |candidate| {
                        candidate.latency_p95_ns
                    }),
                first_failure_diag: backend
                    .statements
                    .iter()
                    .find(|statement| {
                        statement.first_failure_diag != FIRST_FAILURE_DIAGNOSTIC_ABSENT
                    })
                    .map_or_else(
                        || first_failure_diag.clone(),
                        |statement| statement.first_failure_diag.clone(),
                    ),
            }
        })
        .collect::<Vec<_>>();
    let mut artifact = AgentSwarmCiSmokeArtifact {
        schema_version: trace.schema_version.clone(),
        smoke_version: AGENT_SWARM_CI_SMOKE_VERSION.to_owned(),
        scrubber_version: trace.scrubber_version.clone(),
        replay_harness_version: report.harness_version.clone(),
        evidence_manifest_version: manifest.manifest_version.clone(),
        artifact_hash: String::new(),
        trace_id: trace.trace_id.clone(),
        run_id: trace.run_id.clone(),
        scenario_id: trace.scenario_id.clone(),
        command: manifest.replay_command.clone(),
        profile_id: scorecard.profile.profile_id,
        trace_artifact_path: manifest.trace_artifact_path.clone(),
        trace_artifact_hash: manifest.trace_artifact_hash.clone(),
        artifact_manifest_path: artifact_manifest_path.into(),
        evidence_manifest_hash: manifest.artifact_hash.clone(),
        first_failure_diag,
        backend_artifacts,
    };
    artifact.artifact_hash = ci_smoke_artifact_hash(&artifact);
    log_agent_swarm_ci_smoke_artifact(&artifact);
    artifact
}

/// Write an evidence manifest and its minimized failing trace slice.
pub fn write_agent_swarm_evidence_manifest_files(
    manifest: &AgentSwarmEvidenceManifest,
    manifest_path: impl AsRef<Path>,
) -> Result<(), AgentSwarmReplayError> {
    let manifest_json = serde_json::to_vec_pretty(manifest)
        .map_err(|error| AgentSwarmReplayError::JsonEncode(error.to_string()))?;
    write_agent_swarm_artifact_file(manifest_path.as_ref(), &manifest_json)?;

    if let Some(minimized_trace_path) = manifest.minimized_trace_artifact_path.as_deref() {
        let minimized_trace_json = serde_json::to_vec_pretty(&manifest.minimized_trace_slice)
            .map_err(|error| AgentSwarmReplayError::JsonEncode(error.to_string()))?;
        write_agent_swarm_artifact_file(Path::new(minimized_trace_path), &minimized_trace_json)?;
    }

    Ok(())
}

fn validate_agent_swarm_trace_schema(trace: &AgentSwarmTrace) -> Result<(), AgentSwarmReplayError> {
    if trace.schema_version.as_str().eq(SWARM_TRACE_SCHEMA_VERSION) {
        Ok(())
    } else {
        Err(AgentSwarmReplayError::SchemaMismatch {
            expected: SWARM_TRACE_SCHEMA_VERSION.to_owned(),
            actual: trace.schema_version.clone(),
        })
    }
}

fn replay_schema_sql(trace: &AgentSwarmTrace, config: &AgentSwarmReplayConfig) -> Vec<String> {
    if !config.schema_sql.is_empty() {
        return config.schema_sql.clone();
    }

    trace
        .metadata
        .tags
        .get("scenario_family")
        .and_then(|scenario| synthetic_scenario_from_str(scenario))
        .map_or_else(Vec::new, synthetic_replay_schema)
}

fn synthetic_scenario_from_str(value: &str) -> Option<SyntheticTraceScenario> {
    SYNTHETIC_TRACE_SCENARIOS
        .iter()
        .copied()
        .find(|scenario| scenario.as_str() == value)
}

fn scheduled_statement_indices(
    trace: &AgentSwarmTrace,
    schedule: AgentSwarmReplaySchedule,
) -> Vec<usize> {
    let mut indexed = trace
        .statements
        .iter()
        .enumerate()
        .map(|(index, statement)| {
            (
                index,
                statement.logical_order,
                statement.concurrency_group.as_str(),
            )
        })
        .collect::<Vec<_>>();

    match schedule {
        AgentSwarmReplaySchedule::TraceOrder => {
            indexed.sort_by_key(|(index, logical_order, _)| (*logical_order, *index));
        }
        AgentSwarmReplaySchedule::ConcurrencyGroupThenOrder => {
            indexed.sort_by(|left, right| {
                left.2
                    .cmp(right.2)
                    .then_with(|| left.1.cmp(&right.1))
                    .then_with(|| left.0.cmp(&right.0))
            });
        }
    }

    indexed.into_iter().map(|(index, _, _)| index).collect()
}

fn validate_trace_topology(trace: &AgentSwarmTrace) -> Vec<String> {
    let mut open_transactions = BTreeMap::<&str, &str>::new();
    let mut errors = Vec::new();
    let mut indices = scheduled_statement_indices(trace, AgentSwarmReplaySchedule::TraceOrder);
    indices.sort_by_key(|index| {
        (
            trace.statements[*index].logical_order,
            u64::try_from(*index).unwrap_or(u64::MAX),
        )
    });

    for index in indices {
        let statement = &trace.statements[index];
        let connection_id = statement.connection_id.as_str();
        match statement.transaction_boundary {
            TransactionBoundary::Begin => {
                if let Some(open_txn) = open_transactions.insert(
                    connection_id,
                    statement.transaction_id.as_deref().unwrap_or_default(),
                ) {
                    errors.push(format!(
                        "connection {} begins transaction {} before closing {} at statement {}",
                        connection_id,
                        statement.transaction_id.as_deref().unwrap_or_default(),
                        open_txn,
                        index
                    ));
                }
            }
            TransactionBoundary::Commit | TransactionBoundary::Rollback => {
                match open_transactions.remove(connection_id) {
                    Some(open_txn) if Some(open_txn) == statement.transaction_id.as_deref() => {}
                    Some(open_txn) => errors.push(format!(
                        "connection {} closes transaction {:?} while {} is open at statement {}",
                        connection_id, statement.transaction_id, open_txn, index
                    )),
                    None => errors.push(format!(
                        "connection {} closes transaction {:?} without an open transaction at statement {}",
                        connection_id, statement.transaction_id, index
                    )),
                }
            }
            TransactionBoundary::None => {
                if let Some(transaction_id) = statement.transaction_id.as_deref() {
                    match open_transactions.get(connection_id) {
                        Some(open_txn) if *open_txn == transaction_id => {}
                        Some(open_txn) => errors.push(format!(
                            "connection {} statement {} belongs to {} while {} is open",
                            connection_id, index, transaction_id, open_txn
                        )),
                        None => errors.push(format!(
                            "connection {} statement {} belongs to {} without an open transaction",
                            connection_id, index, transaction_id
                        )),
                    }
                }
            }
        }
    }

    for (connection_id, transaction_id) in open_transactions {
        errors.push(format!(
            "connection {connection_id} leaves transaction {transaction_id} open at trace end"
        ));
    }

    errors
}

fn replay_backend<E>(
    backend: AgentSwarmReplayBackend,
    executor: &E,
    trace: &AgentSwarmTrace,
    schema_sql: &[String],
    scheduled_indices: &[usize],
    memory_high_water_bytes: usize,
) -> AgentSwarmBackendReplay
where
    E: SqlExecutor,
{
    let identity = AgentSwarmBackendIdentity {
        backend,
        engine_identity: engine_identity_label(executor.engine_identity()).to_owned(),
        comparison_role: backend.comparison_role().to_owned(),
        concurrent_writer_default: backend.concurrent_writer_default(),
    };
    let schema_failures = apply_replay_schema(executor, schema_sql);
    let mut statements = Vec::with_capacity(scheduled_indices.len());
    for trace_statement_index in scheduled_indices {
        let statement = &trace.statements[*trace_statement_index];
        statements.push(replay_statement(
            backend,
            executor,
            *trace_statement_index,
            statement,
            trace,
        ));
    }
    let summary = summarize_backend_replay(&statements, memory_high_water_bytes);
    AgentSwarmBackendReplay {
        identity,
        schema_failures,
        statements,
        summary,
    }
}

fn apply_replay_schema<E>(executor: &E, schema_sql: &[String]) -> Vec<AgentSwarmSchemaFailure>
where
    E: SqlExecutor,
{
    schema_sql
        .iter()
        .enumerate()
        .filter_map(|(schema_index, sql)| match executor.run_stmt(sql) {
            StmtOutcome::Error(error) => Some(AgentSwarmSchemaFailure {
                schema_index,
                sql: sql.clone(),
                error,
            }),
            StmtOutcome::Rows(_) | StmtOutcome::Execute(_) => None,
        })
        .collect()
}

fn replay_statement<E>(
    backend: AgentSwarmReplayBackend,
    executor: &E,
    trace_statement_index: usize,
    statement: &TraceStatement,
    trace: &AgentSwarmTrace,
) -> AgentSwarmStatementReplay
where
    E: SqlExecutor,
{
    let materialized_sql = materialize_replay_sql_for_backend(backend, &statement.scrubbed_sql);
    let fallback_before = executor.fallback_execution_snapshot();
    let started_at = Instant::now();
    let outcome = executor.run_stmt(&materialized_sql);
    let latency_ns = elapsed_nanos_u64(started_at.elapsed());
    let fallback_evidence = fallback_before
        .zip(executor.fallback_execution_snapshot())
        .map(|(before, after)| {
            let previous_id = before.events.last().map_or(0, |event| event.event_id);
            AgentSwarmFallbackEvidence {
                events: after
                    .events
                    .into_iter()
                    .filter(|event| event.event_id > previous_id)
                    .map(|event| AgentSwarmFallbackEvent {
                        event_id: event.event_id,
                        connection_id: event.connection_id,
                        statement_id: event.statement_id,
                        transaction_id: event.transaction_id,
                        statement_kind: event.statement_kind.to_owned(),
                        decision_reason: event.decision_reason.to_owned(),
                        decision_outcome: event.decision_outcome.to_owned(),
                        source_context: event.source_context.to_owned(),
                        storage_backend: event.storage_backend.to_owned(),
                    })
                    .collect(),
                dropped_events: after.dropped_events.saturating_sub(before.dropped_events),
            }
        });
    let outcome_class = classify_stmt_outcome(&outcome);
    let expected_matched = expected_result_matches(statement.expected_result_class, outcome_class);
    let first_failure_diag = if expected_matched {
        FIRST_FAILURE_DIAGNOSTIC_ABSENT.to_owned()
    } else {
        format!(
            "expected_{}_got_{}",
            expected_result_class_label(statement.expected_result_class),
            outcome_class.as_str()
        )
    };

    let record = AgentSwarmStatementReplay {
        backend,
        trace_statement_index,
        logical_order: statement.logical_order,
        actor_id: statement.actor_id.clone(),
        connection_id: statement.connection_id.clone(),
        transaction_id: statement.transaction_id.clone(),
        transaction_boundary: statement.transaction_boundary,
        concurrency_group: statement.concurrency_group.clone(),
        workload_phase: statement.workload_phase.clone(),
        statement_shape: statement.statement_shape.clone(),
        materialized_sql,
        expected_result_class: statement.expected_result_class,
        outcome,
        fallback_evidence,
        outcome_class,
        latency_ns,
        retry_count: 0,
        expected_matched,
        first_failure_diag,
    };
    log_agent_swarm_replay_statement(trace, &record);
    record
}

fn materialize_replay_sql_for_backend(backend: AgentSwarmReplayBackend, sql: &str) -> String {
    let materialized_sql = materialize_scrubbed_sql(sql);
    if backend != AgentSwarmReplayBackend::CSqliteOracle {
        return materialized_sql;
    }

    let trimmed = materialized_sql.trim();
    if trimmed.eq_ignore_ascii_case("BEGIN CONCURRENT") {
        "BEGIN".to_owned()
    } else if trimmed.eq_ignore_ascii_case("BEGIN CONCURRENT;") {
        "BEGIN;".to_owned()
    } else {
        materialized_sql
    }
}

fn classify_stmt_outcome(outcome: &StmtOutcome) -> AgentSwarmReplayOutcomeClass {
    match outcome {
        StmtOutcome::Rows(_) | StmtOutcome::Execute(_) => AgentSwarmReplayOutcomeClass::Success,
        StmtOutcome::Error(message) => {
            let lower = message.to_ascii_lowercase();
            if lower.contains("busy") || lower.contains("locked") {
                AgentSwarmReplayOutcomeClass::Busy
            } else if lower.contains("conflict")
                || lower.contains("concurrent")
                || lower.contains("serialization")
            {
                AgentSwarmReplayOutcomeClass::Conflict
            } else {
                AgentSwarmReplayOutcomeClass::Error
            }
        }
    }
}

const fn expected_result_matches(
    expected: ExpectedResultClass,
    actual: AgentSwarmReplayOutcomeClass,
) -> bool {
    match expected {
        ExpectedResultClass::Success => matches!(actual, AgentSwarmReplayOutcomeClass::Success),
        ExpectedResultClass::Busy => matches!(actual, AgentSwarmReplayOutcomeClass::Busy),
        ExpectedResultClass::Conflict => matches!(actual, AgentSwarmReplayOutcomeClass::Conflict),
        ExpectedResultClass::Error => !matches!(actual, AgentSwarmReplayOutcomeClass::Success),
        ExpectedResultClass::Unknown => true,
    }
}

const fn expected_result_class_label(expected: ExpectedResultClass) -> &'static str {
    match expected {
        ExpectedResultClass::Success => "success",
        ExpectedResultClass::Busy => "busy",
        ExpectedResultClass::Conflict => "conflict",
        ExpectedResultClass::Error => "error",
        ExpectedResultClass::Unknown => "unknown",
    }
}

fn summarize_backend_replay(
    statements: &[AgentSwarmStatementReplay],
    memory_high_water_bytes: usize,
) -> AgentSwarmBackendSummary {
    let mut latencies = statements
        .iter()
        .map(|statement| statement.latency_ns)
        .collect::<Vec<_>>();
    latencies.sort_unstable();

    let mut conflict_classes = BTreeMap::new();
    let mut success_count = 0;
    let mut error_count = 0;
    let mut expected_mismatch_count = 0;
    let mut abort_count = 0;
    let mut retry_count = 0_u64;
    let mut latency_total_ns = 0_u64;

    for statement in statements {
        if statement.outcome_class == AgentSwarmReplayOutcomeClass::Success {
            success_count += 1;
        } else {
            error_count += 1;
            *conflict_classes
                .entry(statement.outcome_class.as_str().to_owned())
                .or_insert(0) += 1;
        }
        if !statement.expected_matched {
            expected_mismatch_count += 1;
        }
        if statement.transaction_boundary == TransactionBoundary::Rollback
            || statement.outcome_class != AgentSwarmReplayOutcomeClass::Success
        {
            abort_count += 1;
        }
        retry_count = retry_count.saturating_add(u64::from(statement.retry_count));
        latency_total_ns = latency_total_ns.saturating_add(statement.latency_ns);
    }

    AgentSwarmBackendSummary {
        statements_total: statements.len(),
        success_count,
        error_count,
        expected_mismatch_count,
        abort_count,
        retry_count,
        conflict_classes,
        latency_total_ns,
        latency_p50_ns: percentile_latency(&latencies, 50),
        latency_p95_ns: percentile_latency(&latencies, 95),
        latency_p99_ns: percentile_latency(&latencies, 99),
        throughput_statements_per_second_x1000: throughput_x1000(
            statements.len(),
            latency_total_ns,
        ),
        memory_high_water_bytes,
    }
}

fn percentile_latency(sorted_latencies: &[u64], percentile: usize) -> u64 {
    if sorted_latencies.is_empty() {
        return 0;
    }
    let last = sorted_latencies.len() - 1;
    let rank = last.saturating_mul(percentile).div_ceil(100);
    sorted_latencies[rank.min(last)]
}

fn throughput_x1000(statement_count: usize, latency_total_ns: u64) -> u64 {
    if statement_count == 0 || latency_total_ns == 0 {
        return 0;
    }
    let numerator = u128::try_from(statement_count)
        .unwrap_or(u128::MAX)
        .saturating_mul(1_000_000_000_000);
    let throughput = numerator / u128::from(latency_total_ns);
    u64::try_from(throughput).unwrap_or(u64::MAX)
}

fn score_backend_resource_envelope(
    backend: &AgentSwarmBackendReplay,
    profile: &AgentSwarmResourceProfile,
) -> AgentSwarmBackendResourceScorecard {
    let mut per_actor_statement_count = BTreeMap::new();
    for statement in &backend.statements {
        *per_actor_statement_count
            .entry(statement.actor_id.clone())
            .or_insert(0) += 1;
    }
    let per_actor_throughput_statements_per_second_x1000 = per_actor_statement_count
        .iter()
        .map(|(actor_id, count)| {
            (
                actor_id.clone(),
                throughput_x1000(*count, backend.summary.latency_total_ns),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let fairness_index_per_mille = jain_fairness_index_per_mille(
        per_actor_throughput_statements_per_second_x1000
            .values()
            .copied(),
    );
    let coordination_metrics = coordination_metrics_for_backend(backend, fairness_index_per_mille);
    let memory_high_water_bytes = usize_to_u64(backend.summary.memory_high_water_bytes);
    let page_cache_footprint_bytes = memory_high_water_bytes.min(profile.page_cache_limit_bytes);
    let cpu_capacity_ns = profile
        .cpu_observation_window_ns
        .saturating_mul(u64::from(profile.core_count.max(1)));

    AgentSwarmBackendResourceScorecard {
        backend: backend.identity.backend,
        comparison_role: backend.identity.comparison_role.clone(),
        concurrent_writer_default: backend.identity.concurrent_writer_default,
        core_count: profile.core_count,
        memory_limit_bytes: profile.memory_limit_bytes,
        cpu_utilization_per_mille: ratio_per_mille_capped(
            backend.summary.latency_total_ns,
            cpu_capacity_ns,
        ),
        cpu_utilization_basis:
            "latency_total_ns divided by profile core_count * observation_window_ns".to_owned(),
        memory_high_water_bytes,
        memory_limit_exceeded: memory_high_water_bytes > profile.memory_limit_bytes,
        page_cache_footprint_bytes,
        page_cache_limit_bytes: profile.page_cache_limit_bytes,
        page_cache_utilization_per_mille: ratio_per_mille_capped(
            page_cache_footprint_bytes,
            profile.page_cache_limit_bytes,
        ),
        abort_rate_per_mille: ratio_per_mille(
            usize_to_u64(backend.summary.abort_count),
            usize_to_u64(backend.summary.statements_total),
        ),
        retry_rate_per_mille: ratio_per_mille(
            backend.summary.retry_count,
            usize_to_u64(backend.summary.statements_total),
        ),
        fairness_index_per_mille,
        per_actor_statement_count,
        per_actor_throughput_statements_per_second_x1000,
        latency_p50_ns: backend.summary.latency_p50_ns,
        latency_p95_ns: backend.summary.latency_p95_ns,
        latency_p99_ns: backend.summary.latency_p99_ns,
        tail_latency_spread_ns: backend
            .summary
            .latency_p99_ns
            .saturating_sub(backend.summary.latency_p50_ns),
        throughput_statements_per_second_x1000: backend
            .summary
            .throughput_statements_per_second_x1000,
        coordination_metrics,
    }
}

fn coordination_metrics_for_backend(
    backend: &AgentSwarmBackendReplay,
    fairness_index_per_mille: u16,
) -> AgentSwarmCoordinationMetrics {
    let mut queue_claim_count = 0_u64;
    let mut queue_release_count = 0_u64;
    let mut lease_acquire_count = 0_u64;
    let mut lease_renew_count = 0_u64;
    let mut lease_expiration_count = 0_u64;
    let mut range_allocation_count = 0_u64;
    let mut fallback_reason_count = 0_u64;
    let mut scenarios = BTreeSet::new();

    if backend.summary.success_count > 0 {
        scenarios.insert(AgentSwarmCoordinationScenario::Normal);
    }
    if backend.summary.abort_count > 0 || backend.summary.retry_count > 0 {
        scenarios.insert(AgentSwarmCoordinationScenario::ConflictHeavy);
    }

    for statement in &backend.statements {
        if statement_has_signal(statement, &["fsqlite_queue", "task_queue", "queue"]) {
            if statement_has_signal(statement, &["claim", "ready", "queue_claim"]) {
                queue_claim_count = queue_claim_count.saturating_add(1);
            }
            if statement_has_signal(statement, &["release", "complete", "done", "abandon"]) {
                queue_release_count = queue_release_count.saturating_add(1);
            }
        }
        if statement_has_signal(
            statement,
            &[
                "fsqlite_lease",
                "lease-",
                "lease_owner",
                "lease_generation",
                "lease_expires_ms",
            ],
        ) {
            lease_acquire_count = lease_acquire_count.saturating_add(1);
            if statement_has_signal(statement, &["renew", "heartbeat"]) {
                lease_renew_count = lease_renew_count.saturating_add(1);
            }
            if statement_has_signal(statement, &["expire", "expired", "takeover"]) {
                lease_expiration_count = lease_expiration_count.saturating_add(1);
                scenarios.insert(AgentSwarmCoordinationScenario::ExpiredLease);
            }
        }
        if statement_has_signal(
            statement,
            &["fsqlite_worker_ranges", "worker_range", "range"],
        ) {
            range_allocation_count = range_allocation_count.saturating_add(1);
            if statement_has_signal(statement, &["imbalance", "split", "merge"]) {
                scenarios.insert(AgentSwarmCoordinationScenario::RangeImbalance);
            }
        }
        if let Some(evidence) = &statement.fallback_evidence
            && !evidence.events.is_empty()
        {
            fallback_reason_count =
                fallback_reason_count.saturating_add(usize_to_u64(evidence.events.len()));
            scenarios.insert(AgentSwarmCoordinationScenario::FallbackHeavy);
        }
    }

    AgentSwarmCoordinationMetrics {
        bridge_version: AGENT_SWARM_COORDINATION_BRIDGE_VERSION.to_owned(),
        scenarios: scenarios.into_iter().collect(),
        queue_claim_count,
        queue_release_count,
        lease_acquire_count,
        lease_renew_count,
        lease_expiration_count,
        range_allocation_count,
        // SQL text and labels cannot provide these engine observations.
        explain_concurrency_row_count: None,
        fallback_reason_count,
        resource_governor_decision_count: None,
        coordination_correctness_per_mille: coordination_correctness_per_mille(&backend.summary),
        conflict_transparency_per_mille: conflict_transparency_per_mille(
            &backend.summary,
            0,
            fallback_reason_count,
        ),
        fairness_resource_pressure_per_mille: 1_000_u16.saturating_sub(fairness_index_per_mille),
        privacy_scrubber_preserved: privacy_scrubber_preserved(&backend.statements),
        trace_fields: AGENT_SWARM_COORDINATION_TRACE_FIELDS
            .iter()
            .map(|field| (*field).to_owned())
            .collect(),
        first_failure_diag: first_coordination_failure_diag(&backend.statements),
    }
}

fn statement_has_signal(statement: &AgentSwarmStatementReplay, needles: &[&str]) -> bool {
    let sql = statement.materialized_sql.to_ascii_lowercase();
    let phase = statement.workload_phase.to_ascii_lowercase();
    let group = statement.concurrency_group.to_ascii_lowercase();
    needles
        .iter()
        .any(|needle| sql.contains(needle) || phase.contains(needle) || group.contains(needle))
}

fn coordination_correctness_per_mille(summary: &AgentSwarmBackendSummary) -> u16 {
    let failures = usize_to_u64(
        summary
            .expected_mismatch_count
            .saturating_add(summary.abort_count),
    );
    let total = usize_to_u64(summary.statements_total);
    let failure_rate = ratio_per_mille(failures, total).min(1_000);
    u16::try_from(1_000_u64.saturating_sub(failure_rate)).unwrap_or(0)
}

fn conflict_transparency_per_mille(
    summary: &AgentSwarmBackendSummary,
    explain_rows: u64,
    fallback_reasons: u64,
) -> u16 {
    let conflict_count = usize_to_u64(summary.abort_count)
        .saturating_add(summary.retry_count)
        .saturating_add(usize_to_u64(
            summary.conflict_classes.values().copied().sum::<usize>(),
        ));
    if conflict_count == 0 {
        return 1_000;
    }
    let diagnostic_count = explain_rows
        .saturating_add(fallback_reasons)
        .min(conflict_count);
    u16::try_from(ratio_per_mille(diagnostic_count, conflict_count).min(1_000)).unwrap_or(1_000)
}

fn privacy_scrubber_preserved(statements: &[AgentSwarmStatementReplay]) -> bool {
    !statements.iter().any(|statement| {
        let sql = statement.materialized_sql.to_ascii_lowercase();
        ["/home/", "token", "password", "secret", "sqlite:///"]
            .iter()
            .any(|needle| sql.contains(needle))
    })
}

fn first_coordination_failure_diag(statements: &[AgentSwarmStatementReplay]) -> String {
    statements
        .iter()
        .find(|statement| statement.first_failure_diag != FIRST_FAILURE_DIAGNOSTIC_ABSENT)
        .map_or_else(
            || FIRST_FAILURE_DIAGNOSTIC_ABSENT.to_owned(),
            |statement| statement.first_failure_diag.clone(),
        )
}

fn ratio_per_mille(numerator: u64, denominator: u64) -> u64 {
    if denominator == 0 {
        return 0;
    }
    numerator.saturating_mul(1_000) / denominator
}

fn ratio_per_mille_capped(numerator: u64, denominator: u64) -> u16 {
    u16::try_from(ratio_per_mille(numerator, denominator).min(1_000)).unwrap_or(1_000)
}

fn jain_fairness_index_per_mille(values: impl IntoIterator<Item = u64>) -> u16 {
    let mut actor_count = 0_u128;
    let mut sum = 0_u128;
    let mut sum_squares = 0_u128;
    for value in values {
        actor_count += 1;
        let value = u128::from(value);
        sum = sum.saturating_add(value);
        sum_squares = sum_squares.saturating_add(value.saturating_mul(value));
    }
    if actor_count == 0 || sum == 0 || sum_squares == 0 {
        return 0;
    }
    let numerator = sum.saturating_mul(sum).saturating_mul(1_000);
    let denominator = actor_count.saturating_mul(sum_squares);
    u16::try_from((numerator / denominator).min(1_000)).unwrap_or(1_000)
}

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn evidence_backend_version(backend: &AgentSwarmBackendReplay) -> AgentSwarmEvidenceBackendVersion {
    AgentSwarmEvidenceBackendVersion {
        backend: backend.identity.backend,
        engine_identity: backend.identity.engine_identity.clone(),
        comparison_role: backend.identity.comparison_role.clone(),
        concurrent_writer_default: backend.identity.concurrent_writer_default,
    }
}

fn evidence_backend_metrics(backend: &AgentSwarmBackendReplay) -> AgentSwarmEvidenceBackendMetrics {
    AgentSwarmEvidenceBackendMetrics {
        backend: backend.identity.backend,
        statements_total: backend.summary.statements_total,
        throughput_statements_per_second_x1000: backend
            .summary
            .throughput_statements_per_second_x1000,
        latency_p50_ns: backend.summary.latency_p50_ns,
        latency_p95_ns: backend.summary.latency_p95_ns,
        latency_p99_ns: backend.summary.latency_p99_ns,
        abort_count: backend.summary.abort_count,
        retry_count: backend.summary.retry_count,
        expected_mismatch_count: backend.summary.expected_mismatch_count,
        conflict_classes: backend.summary.conflict_classes.clone(),
        memory_high_water_bytes: backend.summary.memory_high_water_bytes,
    }
}

fn classify_agent_swarm_failure(report: &AgentSwarmReplayReport) -> AgentSwarmFailureCategory {
    let Some(failure) = report.first_failure.as_ref() else {
        return AgentSwarmFailureCategory::Unknown;
    };
    let reason = failure.reason.to_ascii_lowercase();
    if reason.contains("syntax") || reason.contains("parser") || reason.contains("parse") {
        AgentSwarmFailureCategory::Parser
    } else if reason.contains("planner") || reason.contains("query plan") {
        AgentSwarmFailureCategory::Planner
    } else if reason.contains("vdbe") || reason.contains("opcode") || reason.contains("bytecode") {
        AgentSwarmFailureCategory::Vdbe
    } else if reason.contains("pager")
        || reason.contains("page-cache")
        || reason.contains("page cache")
    {
        AgentSwarmFailureCategory::Pager
    } else if reason.contains("wal")
        || reason.contains("durability")
        || reason.contains("checkpoint")
    {
        AgentSwarmFailureCategory::Wal
    } else if reason.contains("mvcc")
        || reason.contains("conflict")
        || reason.contains("serialization")
        || reason.contains("concurrent")
    {
        AgentSwarmFailureCategory::Mvcc
    } else if reason.contains("fts")
        || reason.contains("rtree")
        || reason.contains("json")
        || reason.contains("icu")
        || reason.contains("extension")
    {
        AgentSwarmFailureCategory::Extension
    } else if reason.contains("resource")
        || reason.contains("memory")
        || reason.contains("cpu")
        || reason.contains("fairness")
        || reason.contains("latency")
    {
        AgentSwarmFailureCategory::ResourceEnvelope
    } else {
        AgentSwarmFailureCategory::Unknown
    }
}

fn duplicate_candidate_ids(
    config: &AgentSwarmEvidenceManifestConfig,
    report: &AgentSwarmReplayReport,
    failure_category: AgentSwarmFailureCategory,
    minimized_trace_hash: &str,
    first_failure_diag: &str,
) -> Vec<String> {
    let duplicate_keys = [
        minimized_trace_hash,
        report.trace_id.as_str(),
        report.scenario_id.as_str(),
        failure_category.as_str(),
        first_failure_diag,
    ];
    config
        .existing_beads
        .iter()
        .filter(|bead| existing_bead_matches_duplicate(bead, &duplicate_keys))
        .map(|bead| bead.id.clone())
        .collect()
}

fn existing_bead_matches_duplicate(
    bead: &AgentSwarmExistingBeadSummary,
    duplicate_keys: &[&str],
) -> bool {
    duplicate_keys.iter().any(|key| {
        let key = key.trim();
        !key.is_empty()
            && (bead.duplicate_keys.iter().any(|candidate| candidate == key)
                || bead.title.contains(key)
                || bead.labels.iter().any(|label| label == key))
    })
}

fn regression_bead_proposal(
    report: &AgentSwarmReplayReport,
    failure_category: AgentSwarmFailureCategory,
    minimized_trace_hash: &str,
    first_failure_diag: &str,
) -> AgentSwarmRegressionBeadProposal {
    let short_hash = minimized_trace_hash
        .get(..12)
        .unwrap_or(minimized_trace_hash);
    let proposed_bead_id = format!("bd-agent-swarm-regression-{short_hash}");
    AgentSwarmRegressionBeadProposal {
        proposed_bead_id,
        title: format!(
            "REGRESSION: {} failure in {}",
            failure_category.as_str(),
            report.scenario_id
        ),
        description: format!(
            "Reproducible agent-swarm replay failure.\n\ntrace_id: {}\nrun_id: {}\nscenario_id: {}\nfailure_category: {}\nminimized_trace_hash: {}\nfirst_failure_diag: {}\n\nUse the evidence manifest replay_command and minimized trace artifact before implementing a fix.",
            report.trace_id,
            report.run_id,
            report.scenario_id,
            failure_category.as_str(),
            minimized_trace_hash,
            first_failure_diag
        ),
        labels: vec![
            "agent-swarm".to_owned(),
            "regression".to_owned(),
            failure_category.as_str().to_owned(),
        ],
        duplicate_key: minimized_trace_hash.to_owned(),
    }
}

fn evidence_artifact_hash(
    report: &AgentSwarmReplayReport,
    config: &AgentSwarmEvidenceManifestConfig,
    trace_artifact_hash: &str,
    failure_category: AgentSwarmFailureCategory,
    minimized_trace_hash: &str,
    duplicate_candidate_ids: &[String],
    result_metrics: &[AgentSwarmEvidenceBackendMetrics],
) -> String {
    #[derive(Serialize)]
    struct EvidenceDigest<'a> {
        manifest_version: &'a str,
        trace_id: &'a str,
        run_id: &'a str,
        scenario_id: &'a str,
        trace_artifact_path: &'a str,
        trace_artifact_hash: &'a str,
        replay_command: &'a str,
        failure_category: AgentSwarmFailureCategory,
        minimized_trace_hash: &'a str,
        duplicate_candidate_ids: &'a [String],
        result_metrics: &'a [AgentSwarmEvidenceBackendMetrics],
    }

    stable_json_hash(&EvidenceDigest {
        manifest_version: AGENT_SWARM_EVIDENCE_MANIFEST_VERSION,
        trace_id: &report.trace_id,
        run_id: &report.run_id,
        scenario_id: &report.scenario_id,
        trace_artifact_path: &config.trace_artifact_path,
        trace_artifact_hash,
        replay_command: &config.replay_command,
        failure_category,
        minimized_trace_hash,
        duplicate_candidate_ids,
        result_metrics,
    })
}

fn ci_smoke_artifact_hash(artifact: &AgentSwarmCiSmokeArtifact) -> String {
    #[derive(Serialize)]
    struct CiSmokeDigest<'a> {
        schema_version: &'a str,
        smoke_version: &'a str,
        scrubber_version: &'a str,
        replay_harness_version: &'a str,
        evidence_manifest_version: &'a str,
        trace_id: &'a str,
        run_id: &'a str,
        scenario_id: &'a str,
        command: &'a str,
        profile_id: AgentSwarmResourceProfileId,
        trace_artifact_path: &'a str,
        trace_artifact_hash: &'a str,
        artifact_manifest_path: &'a str,
        evidence_manifest_hash: &'a str,
        first_failure_diag: &'a str,
        backend_artifacts: &'a [AgentSwarmCiSmokeBackendArtifact],
    }

    stable_json_hash(&CiSmokeDigest {
        schema_version: &artifact.schema_version,
        smoke_version: &artifact.smoke_version,
        scrubber_version: &artifact.scrubber_version,
        replay_harness_version: &artifact.replay_harness_version,
        evidence_manifest_version: &artifact.evidence_manifest_version,
        trace_id: &artifact.trace_id,
        run_id: &artifact.run_id,
        scenario_id: &artifact.scenario_id,
        command: &artifact.command,
        profile_id: artifact.profile_id,
        trace_artifact_path: &artifact.trace_artifact_path,
        trace_artifact_hash: &artifact.trace_artifact_hash,
        artifact_manifest_path: &artifact.artifact_manifest_path,
        evidence_manifest_hash: &artifact.evidence_manifest_hash,
        first_failure_diag: &artifact.first_failure_diag,
        backend_artifacts: &artifact.backend_artifacts,
    })
}

fn stable_json_hash<T>(value: &T) -> String
where
    T: Serialize,
{
    serde_json::to_vec(value).map_or_else(
        |error| sha256_hex(error.to_string().as_bytes()),
        |bytes| sha256_hex(&bytes),
    )
}

fn write_agent_swarm_artifact_file(path: &Path, bytes: &[u8]) -> Result<(), AgentSwarmReplayError> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent).map_err(|error| AgentSwarmReplayError::EvidenceWrite {
            path: parent.display().to_string(),
            message: error.to_string(),
        })?;
    }
    fs::write(path, bytes).map_err(|error| AgentSwarmReplayError::EvidenceWrite {
        path: path.display().to_string(),
        message: error.to_string(),
    })
}

fn compare_backend_records(
    subject: &AgentSwarmBackendReplay,
    reference: &AgentSwarmBackendReplay,
) -> Vec<AgentSwarmBackendMismatch> {
    subject
        .statements
        .iter()
        .zip(reference.statements.iter())
        .filter_map(|(subject_record, reference_record)| {
            if subject_record.outcome == reference_record.outcome {
                None
            } else {
                Some(AgentSwarmBackendMismatch {
                    trace_statement_index: subject_record.trace_statement_index,
                    logical_order: subject_record.logical_order,
                    statement_shape: subject_record.statement_shape.clone(),
                    subject_outcome: subject_record.outcome.clone(),
                    reference_outcome: reference_record.outcome.clone(),
                })
            }
        })
        .collect()
}

fn build_first_failure_bundle(
    trace: &AgentSwarmTrace,
    backends: &[AgentSwarmBackendReplay],
    cross_backend_mismatches: &[AgentSwarmBackendMismatch],
    topology_errors: &[String],
) -> Option<AgentSwarmFirstFailureBundle> {
    if let Some(error) = topology_errors.first() {
        return Some(first_failure_bundle(
            trace,
            None,
            "topology_error",
            0,
            Some(error),
        ));
    }

    let backend_failure = backends
        .iter()
        .flat_map(|backend| backend.statements.iter())
        .filter(|statement| !statement.expected_matched)
        .min_by_key(|statement| statement.logical_order);
    if let Some(statement) = backend_failure {
        return Some(first_failure_bundle(
            trace,
            Some(statement.backend),
            &statement.first_failure_diag,
            statement.trace_statement_index,
            None,
        ));
    }

    cross_backend_mismatches.first().map(|mismatch| {
        first_failure_bundle(
            trace,
            None,
            "cross_backend_outcome_mismatch",
            mismatch.trace_statement_index,
            None,
        )
    })
}

fn first_failure_bundle(
    trace: &AgentSwarmTrace,
    backend: Option<AgentSwarmReplayBackend>,
    reason: &str,
    trace_statement_index: usize,
    detail: Option<&str>,
) -> AgentSwarmFirstFailureBundle {
    let reason = detail.map_or_else(
        || reason.to_owned(),
        |message| format!("{reason}: {message}"),
    );
    let minimal_reproducer_trace_slice =
        minimal_reproducer_trace_slice(trace, trace_statement_index);
    let materialized_replay_sql = minimal_reproducer_trace_slice
        .iter()
        .map(|statement| materialize_scrubbed_sql(&statement.scrubbed_sql))
        .collect();
    AgentSwarmFirstFailureBundle {
        backend,
        reason,
        trace_statement_index,
        trace_id: trace.trace_id.clone(),
        run_id: trace.run_id.clone(),
        scenario_id: trace.scenario_id.clone(),
        minimal_reproducer_trace_slice,
        materialized_replay_sql,
    }
}

fn minimal_reproducer_trace_slice(
    trace: &AgentSwarmTrace,
    trace_statement_index: usize,
) -> Vec<TraceStatement> {
    let Some(statement) = trace.statements.get(trace_statement_index) else {
        return Vec::new();
    };

    if let Some(transaction_id) = statement.transaction_id.as_deref() {
        let transaction_slice = trace
            .statements
            .iter()
            .filter(|candidate| candidate.transaction_id.as_deref() == Some(transaction_id))
            .cloned()
            .collect::<Vec<_>>();
        if !transaction_slice.is_empty() {
            return transaction_slice;
        }
    }

    let start = trace_statement_index.saturating_sub(1);
    let end = (trace_statement_index + 2).min(trace.statements.len());
    trace.statements[start..end].to_vec()
}

fn log_agent_swarm_replay_statement(trace: &AgentSwarmTrace, record: &AgentSwarmStatementReplay) {
    tracing::info!(
        target: "fsqlite.agent_swarm_trace.replay.statement",
        trace_id = %trace.trace_id,
        run_id = %trace.run_id,
        scenario_id = %trace.scenario_id,
        seed = trace.metadata.tags.get("seed").map(String::as_str).unwrap_or("unknown"),
        backend = record.backend.as_str(),
        concurrency_group = %record.concurrency_group,
        transaction_id = record.transaction_id.as_deref().unwrap_or("none"),
        statement_shape = %record.statement_shape,
        outcome = record.outcome_class.as_str(),
        latency_ns = record.latency_ns,
        retry_count = record.retry_count,
        first_failure_diag = %record.first_failure_diag,
        "agent swarm replay statement",
    );
}

fn log_agent_swarm_replay_summary(report: &AgentSwarmReplayReport) {
    tracing::info!(
        target: "fsqlite.agent_swarm_trace.replay.summary",
        trace_id = %report.trace_id,
        run_id = %report.run_id,
        scenario_id = %report.scenario_id,
        seed = report.seed,
        statement_count = report.statement_count,
        transaction_count = report.transaction_count,
        backend_count = report.backends.len(),
        mismatch_count = report.cross_backend_mismatches.len(),
        first_failure_diag = report.first_failure.as_ref().map(|failure| failure.reason.as_str()).unwrap_or(FIRST_FAILURE_DIAGNOSTIC_ABSENT),
        "agent swarm replay summary",
    );
}

fn log_agent_swarm_resource_scorecard(scorecard: &AgentSwarmResourceScorecard) {
    for backend in &scorecard.backends {
        tracing::info!(
            target: "fsqlite.agent_swarm_trace.resource_scorecard",
            trace_id = %scorecard.trace_id,
            run_id = %scorecard.run_id,
            scenario_id = %scorecard.scenario_id,
            profile_id = scorecard.profile.profile_id.as_str(),
            core_count = backend.core_count,
            memory_limit_bytes = backend.memory_limit_bytes,
            backend = backend.backend.as_str(),
            fairness_index = backend.fairness_index_per_mille,
            p95_ns = backend.latency_p95_ns,
            p99_ns = backend.latency_p99_ns,
            abort_rate = backend.abort_rate_per_mille,
            retry_rate = backend.retry_rate_per_mille,
            queue_claim_count = backend.coordination_metrics.queue_claim_count,
            lease_expiration_count = backend.coordination_metrics.lease_expiration_count,
            range_allocation_count = backend.coordination_metrics.range_allocation_count,
            fallback_reason_count = backend.coordination_metrics.fallback_reason_count,
            coordination_correctness = backend.coordination_metrics.coordination_correctness_per_mille,
            conflict_transparency = backend.coordination_metrics.conflict_transparency_per_mille,
            fairness_resource_pressure = backend.coordination_metrics.fairness_resource_pressure_per_mille,
            privacy_scrubber_preserved = backend.coordination_metrics.privacy_scrubber_preserved,
            first_failure_diag = %scorecard.first_failure_diag,
            "agent swarm resource scorecard",
        );
    }
}

fn log_agent_swarm_evidence_manifest(manifest: &AgentSwarmEvidenceManifest) {
    tracing::info!(
        target: "fsqlite.agent_swarm_trace.evidence_manifest",
        trace_id = %manifest.trace_id,
        run_id = %manifest.run_id,
        scenario_id = %manifest.scenario_id,
        artifact_hash = %manifest.artifact_hash,
        minimized_trace_hash = %manifest.minimized_trace_hash,
        failure_category = manifest.failure_category.as_str(),
        duplicate_candidate_ids = %manifest.duplicate_candidate_ids.join(","),
        proposed_bead_id = manifest.proposed_regression_bead.as_ref().map(|proposal| proposal.proposed_bead_id.as_str()).unwrap_or("none"),
        first_failure_diag = %manifest.first_failure_diag,
        "agent swarm evidence manifest",
    );
}

fn log_agent_swarm_ci_smoke_artifact(artifact: &AgentSwarmCiSmokeArtifact) {
    for backend in &artifact.backend_artifacts {
        tracing::info!(
            target: "fsqlite.agent_swarm_trace.ci_smoke",
            trace_id = %artifact.trace_id,
            run_id = %artifact.run_id,
            scenario_id = %artifact.scenario_id,
            command = %artifact.command,
            backend = backend.backend.as_str(),
            profile_id = artifact.profile_id.as_str(),
            artifact_manifest_path = %artifact.artifact_manifest_path,
            artifact_hash = %artifact.artifact_hash,
            evidence_manifest_hash = %artifact.evidence_manifest_hash,
            statement_count = backend.statement_count,
            expected_mismatch_count = backend.expected_mismatch_count,
            abort_count = backend.abort_count,
            retry_count = backend.retry_count,
            first_failure_diag = %backend.first_failure_diag,
            "agent swarm ci smoke artifact",
        );
    }
}

fn estimate_memory_high_water(trace: &AgentSwarmTrace, schema_sql: &[String]) -> usize {
    let trace_bytes = serde_json::to_vec(trace).map_or(0, |bytes| bytes.len());
    let schema_bytes = schema_sql.iter().map(String::len).sum::<usize>();
    let materialized_bytes = trace
        .statements
        .iter()
        .map(|statement| materialize_scrubbed_sql(&statement.scrubbed_sql).len())
        .sum::<usize>();
    trace_bytes
        .saturating_add(schema_bytes)
        .saturating_add(materialized_bytes)
}

const fn engine_identity_label(identity: EngineIdentity) -> &'static str {
    match identity {
        EngineIdentity::FrankenSqlite => "frankensqlite",
        EngineIdentity::CSqliteOracle => "csqlite-oracle",
        EngineIdentity::Unknown => "unknown",
    }
}

fn elapsed_nanos_u64(elapsed: Duration) -> u64 {
    u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX)
}

struct SyntheticTraceRng {
    state: u64,
}

impl SyntheticTraceRng {
    const fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut value = self.state;
        value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        value ^ (value >> 31)
    }

    fn next_bounded(&mut self, bound: u64) -> u64 {
        debug_assert!(bound > 0);
        self.next_u64() % bound
    }

    fn choose_hot_key(&mut self, hot_key_ratio: u8) -> bool {
        self.next_bounded(100) < u64::from(hot_key_ratio)
    }
}

struct SyntheticTxnContext {
    actor_id: String,
    connection_id: String,
    transaction_id: String,
    concurrency_group: String,
    workload_key: String,
    source_prefix: String,
}

struct SyntheticStatementSpec {
    boundary: TransactionBoundary,
    workload_phase: &'static str,
    sql: String,
    expected_result_class: ExpectedResultClass,
    row_count_class: RowCountClass,
}

impl SyntheticStatementSpec {
    fn begin() -> Self {
        Self {
            boundary: TransactionBoundary::Begin,
            workload_phase: "transaction-begin",
            sql: "BEGIN CONCURRENT".to_owned(),
            expected_result_class: ExpectedResultClass::Success,
            row_count_class: RowCountClass::Zero,
        }
    }

    fn commit() -> Self {
        Self {
            boundary: TransactionBoundary::Commit,
            workload_phase: "transaction-commit",
            sql: "COMMIT".to_owned(),
            expected_result_class: ExpectedResultClass::Success,
            row_count_class: RowCountClass::Zero,
        }
    }
}

fn synthetic_transaction_context(
    config: &SyntheticTraceConfig,
    rng: &mut SyntheticTraceRng,
    tx_index: usize,
) -> SyntheticTxnContext {
    let agent_index = rng.next_bounded(config.agent_count as u64);
    let actor_id = format!("agent-{agent_index:04}");
    let connection_slot = rng.next_bounded(4);
    let connection_id = format!("{actor_id}-conn-{connection_slot:02}");
    let transaction_id = format!("txn-{}-{tx_index:08}", config.scenario.as_str());
    let workload_key = synthetic_workload_key(config, rng);
    let key_temperature = if workload_key.starts_with("hot-") {
        "hot"
    } else {
        "cold"
    };
    let concurrency_group = format!(
        "{}-{key_temperature}-{}",
        config.scenario.as_str(),
        workload_key
    );
    let source_prefix = format!("{}:{tx_index:08}", config.scenario.as_str());

    SyntheticTxnContext {
        actor_id,
        connection_id,
        transaction_id,
        concurrency_group,
        workload_key,
        source_prefix,
    }
}

fn synthetic_workload_key(config: &SyntheticTraceConfig, rng: &mut SyntheticTraceRng) -> String {
    if rng.choose_hot_key(config.hot_key_ratio) {
        let hot_set = (config.agent_count.max(1) as u64).min(16);
        format!("hot-{:02}", rng.next_bounded(hot_set))
    } else {
        format!("cold-{:08x}", rng.next_u64() & 0xFFFF_FFFF)
    }
}

fn synthetic_statement(
    logical_order: u64,
    context: &SyntheticTxnContext,
    spec: SyntheticStatementSpec,
) -> Result<TraceStatement, SyntheticTraceError> {
    let logical_timestamp = format!("tick-{logical_order:012}");
    let source_ref = format!("{}:stmt-{logical_order:012}", context.source_prefix);
    Ok(TraceStatement::from_raw(RawTraceStatement {
        logical_order,
        logical_timestamp: Some(&logical_timestamp),
        actor_id: &context.actor_id,
        connection_id: &context.connection_id,
        transaction_id: Some(&context.transaction_id),
        transaction_boundary: spec.boundary,
        concurrency_group: &context.concurrency_group,
        workload_phase: spec.workload_phase,
        sql: &spec.sql,
        expected_result_class: spec.expected_result_class,
        row_count_class: spec.row_count_class,
        error_class: None,
        source: StatementSource::new("synthetic-generator", source_ref)?,
    })?)
}

fn synthetic_operation_spec(
    config: &SyntheticTraceConfig,
    context: &SyntheticTxnContext,
    sequence: u64,
    is_read: bool,
) -> SyntheticStatementSpec {
    let key = &context.workload_key;
    let actor = &context.actor_id;
    let sequence_bucket = sequence % 1_000_000;
    let sql = match (config.scenario, is_read) {
        (SyntheticTraceScenario::SessionEventAppend, true) => format!(
            "SELECT COUNT(*) FROM session_events WHERE session_id = '{key}' AND agent_id = '{actor}';"
        ),
        (SyntheticTraceScenario::SessionEventAppend, false) => format!(
            "INSERT INTO session_events(session_id, agent_id, event_kind, payload, seq) VALUES ('{key}', '{actor}', 'tool_call', 'payload-{sequence_bucket}', {sequence_bucket});"
        ),
        (SyntheticTraceScenario::TaskQueueClaimLoop, true) => format!(
            "SELECT id, status FROM task_queue WHERE shard = '{key}' AND status = 'ready' ORDER BY priority DESC LIMIT 1;"
        ),
        (SyntheticTraceScenario::TaskQueueClaimLoop, false) => format!(
            "UPDATE task_queue SET status = 'claimed', owner = '{actor}', claim_seq = {sequence_bucket} WHERE shard = '{key}' AND status = 'ready';"
        ),
        (SyntheticTraceScenario::ArtifactIndexMixedLookup, true) => format!(
            "SELECT artifact_id, content_hash FROM artifact_index WHERE workspace = '{key}' AND path_hash = 'path-{sequence_bucket}';"
        ),
        (SyntheticTraceScenario::ArtifactIndexMixedLookup, false) => format!(
            "INSERT INTO artifact_index(workspace, artifact_id, path_hash, content_hash, updated_by) VALUES ('{key}', 'artifact-{sequence_bucket}', 'path-{sequence_bucket}', 'hash-{sequence_bucket}', '{actor}');"
        ),
        (SyntheticTraceScenario::HotCounterStatusRows, true) => {
            format!("SELECT value, updated_by FROM swarm_counters WHERE counter_key = '{key}';")
        }
        (SyntheticTraceScenario::HotCounterStatusRows, false) => format!(
            "UPDATE swarm_counters SET value = value + 1, updated_by = '{actor}', update_seq = {sequence_bucket} WHERE counter_key = '{key}';"
        ),
        (SyntheticTraceScenario::LongReaderBurstWriter, true) => format!(
            "SELECT event_id, payload FROM session_events WHERE session_id = '{key}' AND logical_clock BETWEEN {sequence_bucket} AND {} ORDER BY logical_clock;",
            sequence_bucket + 250
        ),
        (SyntheticTraceScenario::LongReaderBurstWriter, false) => format!(
            "INSERT INTO session_status(session_id, agent_id, status, logical_clock) VALUES ('{key}', '{actor}', 'checkpoint', {sequence_bucket});"
        ),
    };

    SyntheticStatementSpec {
        boundary: TransactionBoundary::None,
        workload_phase: config.scenario.phase(is_read),
        sql,
        expected_result_class: ExpectedResultClass::Success,
        row_count_class: if is_read {
            RowCountClass::Few
        } else {
            RowCountClass::One
        },
    }
}

fn synthetic_trace_metadata(
    config: &SyntheticTraceConfig,
) -> Result<TraceMetadata, SyntheticTraceError> {
    let mut metadata = TraceMetadata::new(
        "synthetic-agent-swarm",
        format!("{}:{:016x}", config.scenario.as_str(), config.seed),
    )?;
    metadata.logical_clock = Some(format!("splitmix64-seed-{:016x}", config.seed));
    metadata.tags.insert(
        "generator_version".to_owned(),
        SYNTHETIC_TRACE_GENERATOR_VERSION.to_owned(),
    );
    metadata.tags.insert(
        "scenario_family".to_owned(),
        config.scenario.as_str().to_owned(),
    );
    metadata
        .tags
        .insert("seed".to_owned(), config.seed.to_string());
    metadata
        .tags
        .insert("agent_count".to_owned(), config.agent_count.to_string());
    metadata.tags.insert(
        "transaction_count".to_owned(),
        config.transaction_count.to_string(),
    );
    metadata
        .tags
        .insert("hot_key_ratio".to_owned(), config.hot_key_ratio.to_string());
    metadata
        .tags
        .insert("read_write_mix".to_owned(), config.read_write_mix());
    metadata.tags.insert(
        "expected_contention_shape".to_owned(),
        config
            .scenario
            .expected_contention_shape(config.hot_key_ratio),
    );
    metadata.tags.insert(
        "target_invariants".to_owned(),
        config.scenario.target_invariants().to_owned(),
    );
    Ok(metadata)
}

fn synthetic_trace_id(config: &SyntheticTraceConfig) -> String {
    format!(
        "trace-synthetic-{}-{:016x}",
        config.scenario.as_str(),
        config.seed
    )
}

fn synthetic_run_id(config: &SyntheticTraceConfig) -> String {
    format!("run-synthetic-{:016x}", config.seed)
}

fn synthetic_scenario_id(scenario: SyntheticTraceScenario) -> String {
    format!("scenario-synthetic-{}", scenario.as_str())
}

fn required_owned(field: &'static str, value: String) -> Result<String, TraceScrubError> {
    if value.trim().is_empty() {
        Err(TraceScrubError::EmptyField(field))
    } else {
        Ok(value)
    }
}

fn required_ref<'a>(field: &'static str, value: &'a str) -> Result<&'a str, TraceScrubError> {
    if value.trim().is_empty() {
        Err(TraceScrubError::EmptyField(field))
    } else {
        Ok(value)
    }
}

fn starts_blob_literal(chars: &[char], i: usize) -> bool {
    let Some(ch) = chars.get(i) else {
        return false;
    };
    (*ch == 'x' || *ch == 'X') && chars.get(i + 1) == Some(&'\'') && is_left_boundary(chars, i)
}

fn skip_single_quoted(chars: &[char], mut i: usize) -> usize {
    while i < chars.len() {
        if chars[i] == '\'' {
            if chars.get(i + 1) == Some(&'\'') {
                i += 2;
            } else {
                return i + 1;
            }
        } else {
            i += 1;
        }
    }
    i
}

fn starts_numeric_literal(chars: &[char], i: usize) -> bool {
    let Some(ch) = chars.get(i) else {
        return false;
    };
    if !is_left_boundary(chars, i) {
        return false;
    }
    ch.is_ascii_digit()
        || (*ch == '.' && chars.get(i + 1).is_some_and(|next| next.is_ascii_digit()))
}

fn consume_numeric_literal(chars: &[char], i: usize) -> usize {
    if chars.get(i) == Some(&'0') && matches!(chars.get(i + 1), Some('x' | 'X')) {
        let mut j = i + 2;
        while chars
            .get(j)
            .is_some_and(|ch| ch.is_ascii_hexdigit() || *ch == '_')
        {
            j += 1;
        }
        return j;
    }

    let mut j = i;
    while chars
        .get(j)
        .is_some_and(|ch| ch.is_ascii_digit() || *ch == '_')
    {
        j += 1;
    }

    if chars.get(j) == Some(&'.') && chars.get(j + 1).is_some_and(|ch| ch.is_ascii_digit()) {
        j += 1;
        while chars
            .get(j)
            .is_some_and(|ch| ch.is_ascii_digit() || *ch == '_')
        {
            j += 1;
        }
    }

    if matches!(chars.get(j), Some('e' | 'E')) {
        let mut exp = j + 1;
        if matches!(chars.get(exp), Some('+' | '-')) {
            exp += 1;
        }
        if chars.get(exp).is_some_and(|ch| ch.is_ascii_digit()) {
            j = exp + 1;
            while chars
                .get(j)
                .is_some_and(|ch| ch.is_ascii_digit() || *ch == '_')
            {
                j += 1;
            }
        }
    }

    j
}

fn consume_parameter(chars: &[char], i: usize) -> Option<usize> {
    match chars.get(i)? {
        '?' => {
            let mut j = i + 1;
            while chars.get(j).is_some_and(|ch| ch.is_ascii_digit()) {
                j += 1;
            }
            Some(j)
        }
        ':' | '@' | '$' => {
            let mut j = i + 1;
            if !chars
                .get(j)
                .is_some_and(|ch| ch.is_ascii_alphabetic() || *ch == '_')
            {
                return None;
            }
            j += 1;
            while chars
                .get(j)
                .is_some_and(|ch| ch.is_ascii_alphanumeric() || *ch == '_')
            {
                j += 1;
            }
            Some(j)
        }
        _ => None,
    }
}

fn is_left_boundary(chars: &[char], i: usize) -> bool {
    i == 0 || !is_identifier_continue(chars[i - 1])
}

fn is_identifier_continue(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '_'
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(64);
    for byte in digest {
        write!(out, "{byte:02x}").expect("writing to a string cannot fail");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    const GOLDEN: &str = include_str!("../conformance/agent_swarm_trace_sanitized_golden.json");

    #[test]
    fn scrubber_redacts_literal_classes_and_preserves_parameters() {
        let scrubbed = scrub_sql_statement(
            "INSERT INTO t(a,b,c,d,e) VALUES ('secret', 42, X'DEADBEEF', :name, ?1);",
        );

        assert_eq!(
            scrubbed.sql,
            "INSERT INTO t(a,b,c,d,e) VALUES (?s, ?n, ?b, :name, ?1);"
        );
        assert_eq!(scrubbed.parameter_count, 2);
        assert_eq!(scrubbed.redactions.string_literals, 1);
        assert_eq!(scrubbed.redactions.numeric_literals, 1);
        assert_eq!(scrubbed.redactions.blob_literals, 1);
    }

    #[test]
    fn scrubber_handles_multiline_sql_and_comments() {
        let scrubbed = scrub_sql_statement(
            "UPDATE inbox\n/* token: abc */\nSET body = 'line1''line2'\nWHERE id = 9001 -- owner=alice\n",
        );

        assert_eq!(
            scrubbed.sql,
            "UPDATE inbox\n/*?c*/\nSET body = ?s\nWHERE id = ?n --?c\n"
        );
        assert_eq!(scrubbed.redactions.comments, 2);
        assert_eq!(scrubbed.redactions.string_literals, 1);
        assert_eq!(scrubbed.redactions.numeric_literals, 1);
    }

    #[test]
    fn trace_statement_preserves_transaction_boundary_topology() {
        let statement = TraceStatement::from_raw(raw_statement(
            0,
            "agent-a",
            Some("txn-a"),
            TransactionBoundary::Begin,
            "BEGIN CONCURRENT",
        ))
        .expect("valid begin boundary");

        assert_eq!(statement.transaction_id.as_deref(), Some("txn-a"));
        assert_eq!(statement.transaction_boundary, TransactionBoundary::Begin);
        assert_eq!(statement.scrubbed_sql, "BEGIN CONCURRENT");
    }

    #[test]
    fn invalid_input_rejects_empty_fields_and_unattached_boundary() {
        let empty_actor = TraceStatement::from_raw(raw_statement(
            0,
            " ",
            Some("txn-a"),
            TransactionBoundary::Begin,
            "BEGIN",
        ));
        assert_eq!(empty_actor, Err(TraceScrubError::EmptyField("actor_id")));

        let missing_transaction = TraceStatement::from_raw(raw_statement(
            0,
            "agent-a",
            None,
            TransactionBoundary::Commit,
            "COMMIT",
        ));
        assert_eq!(
            missing_transaction,
            Err(TraceScrubError::BoundaryWithoutTransactionId)
        );

        let empty_sql = TraceStatement::from_raw(raw_statement(
            0,
            "agent-a",
            None,
            TransactionBoundary::None,
            "  ",
        ));
        assert_eq!(empty_sql, Err(TraceScrubError::EmptySql));
    }

    #[test]
    fn deterministic_statement_shape_hashes_scrubbed_sql() {
        let first = scrub_sql_statement("SELECT * FROM t WHERE id = 1 AND owner = 'alice'");
        let second = scrub_sql_statement("SELECT * FROM t WHERE id = 2 AND owner = 'bob'");
        let different_shape = scrub_sql_statement("SELECT * FROM t WHERE owner = 'bob'");

        assert_eq!(first.sql, second.sql);
        assert_eq!(first.statement_shape, second.statement_shape);
        assert_ne!(first.statement_shape, different_shape.statement_shape);
        assert_eq!(first.statement_shape.len(), 64);
    }

    #[test]
    fn trace_summary_counts_statements_transactions_and_redactions() {
        let trace = sample_trace();

        assert_eq!(trace.statement_count(), 4);
        assert_eq!(trace.transaction_count(), 1);
        assert_eq!(trace.redaction_summary.statement_count, 4);
        assert_eq!(trace.redaction_summary.transaction_count, 1);
        assert_eq!(trace.redaction_summary.redaction_count, 8);
    }

    #[test]
    fn golden_sanitized_fixture_matches_schema_and_hashes() {
        let trace: AgentSwarmTrace = serde_json::from_str(GOLDEN).expect("golden fixture parses");

        assert_eq!(trace.schema_version, SWARM_TRACE_SCHEMA_VERSION);
        assert_eq!(trace.scrubber_version, SWARM_TRACE_SCRUBBER_VERSION);
        assert_eq!(
            trace.redaction_summary.statement_count,
            trace.statement_count()
        );
        assert_eq!(
            trace.redaction_summary.transaction_count,
            trace.transaction_count()
        );
        for statement in &trace.statements {
            assert_eq!(
                statement.statement_shape,
                scrub_sql_statement(&statement.scrubbed_sql).statement_shape
            );
        }
    }

    #[test]
    fn synthetic_trace_generator_is_seed_deterministic() {
        let config = SyntheticTraceConfig::new(
            SyntheticTraceScenario::TaskQueueClaimLoop,
            0xA11C_E5ED,
            8,
            12,
        )
        .with_read_write_mix(30, 70)
        .with_hot_key_ratio(85);

        let first = generate_synthetic_swarm_trace(config.clone()).expect("first trace");
        let second = generate_synthetic_swarm_trace(config.clone()).expect("second trace");
        assert_eq!(first, second);

        let changed_seed = SyntheticTraceConfig {
            seed: 0xA11C_E5EE,
            ..config
        };
        let third = generate_synthetic_swarm_trace(changed_seed).expect("third trace");
        assert_ne!(first.statements, third.statements);
    }

    #[test]
    fn synthetic_trace_generator_varies_by_scenario_family() {
        let mut operation_shapes = BTreeSet::new();

        for scenario in SYNTHETIC_TRACE_SCENARIOS {
            let trace = generate_synthetic_swarm_trace(SyntheticTraceConfig::new(
                scenario,
                0x51A7_E5E1,
                4,
                4,
            ))
            .expect("scenario trace");

            assert_eq!(
                trace
                    .metadata
                    .tags
                    .get("scenario_family")
                    .map(String::as_str),
                Some(scenario.as_str())
            );
            operation_shapes.insert(trace.statements[1].statement_shape.clone());
        }

        assert_eq!(operation_shapes.len(), SYNTHETIC_TRACE_SCENARIOS.len());
    }

    #[test]
    fn synthetic_trace_generator_records_contention_metadata_and_invariants() {
        let config = SyntheticTraceConfig::new(
            SyntheticTraceScenario::LongReaderBurstWriter,
            0xFEED_FACE,
            6,
            5,
        )
        .with_read_write_mix(80, 20)
        .with_hot_key_ratio(90);

        let trace = generate_synthetic_swarm_trace(config.clone()).expect("synthetic trace");
        let encoded = serde_json::to_string(&trace).expect("trace serializes");
        let decoded: AgentSwarmTrace = serde_json::from_str(&encoded).expect("trace deserializes");

        assert_eq!(trace, decoded);
        assert_eq!(trace.statement_count(), config.transaction_count * 3);
        assert_eq!(trace.transaction_count(), config.transaction_count);
        assert_eq!(
            trace
                .metadata
                .tags
                .get("generator_version")
                .map(String::as_str),
            Some(SYNTHETIC_TRACE_GENERATOR_VERSION)
        );
        assert_eq!(
            trace.metadata.tags.get("seed").map(String::as_str),
            Some("4277009102")
        );
        assert_eq!(
            trace.metadata.tags.get("agent_count").map(String::as_str),
            Some("6")
        );
        assert_eq!(
            trace
                .metadata
                .tags
                .get("transaction_count")
                .map(String::as_str),
            Some("5")
        );
        assert_eq!(
            trace.metadata.tags.get("hot_key_ratio").map(String::as_str),
            Some("90")
        );
        assert_eq!(
            trace
                .metadata
                .tags
                .get("read_write_mix")
                .map(String::as_str),
            Some("read:80:write:20")
        );
        assert!(
            trace
                .metadata
                .tags
                .get("expected_contention_shape")
                .is_some_and(|shape| shape.contains("high_skew"))
        );
        assert!(
            trace
                .metadata
                .tags
                .get("target_invariants")
                .is_some_and(|invariants| invariants.contains("mvcc_visibility_shape"))
        );
        assert!(
            trace
                .statements
                .iter()
                .all(|statement| statement.logical_timestamp.is_some())
        );
        assert!(
            trace
                .statements
                .iter()
                .step_by(3)
                .all(|statement| statement.transaction_boundary == TransactionBoundary::Begin)
        );
    }

    #[test]
    fn synthetic_trace_generator_rejects_invalid_config() {
        let no_agents =
            SyntheticTraceConfig::new(SyntheticTraceScenario::SessionEventAppend, 1, 0, 1);
        assert_eq!(
            generate_synthetic_swarm_trace(no_agents),
            Err(SyntheticTraceError::AgentCountZero)
        );

        let no_transactions =
            SyntheticTraceConfig::new(SyntheticTraceScenario::SessionEventAppend, 1, 1, 0);
        assert_eq!(
            generate_synthetic_swarm_trace(no_transactions),
            Err(SyntheticTraceError::TransactionCountZero)
        );

        let no_mix = SyntheticTraceConfig::new(SyntheticTraceScenario::SessionEventAppend, 1, 1, 1)
            .with_read_write_mix(0, 0);
        assert_eq!(
            generate_synthetic_swarm_trace(no_mix),
            Err(SyntheticTraceError::EmptyReadWriteMix)
        );

        let invalid_hot_key_ratio =
            SyntheticTraceConfig::new(SyntheticTraceScenario::SessionEventAppend, 1, 1, 1)
                .with_hot_key_ratio(101);
        assert_eq!(
            generate_synthetic_swarm_trace(invalid_hot_key_ratio),
            Err(SyntheticTraceError::HotKeyRatioOutOfRange(101))
        );
    }

    #[test]
    fn agent_swarm_replay_materializes_scrubbed_literals_without_touching_parameters() {
        let sql = "VALUES (?s, ?n, ?b, ?1, :name, --?c\n /*?c*/ ?s);";

        assert_eq!(
            materialize_scrubbed_sql(sql),
            "VALUES ('redacted-text', 0, X'', ?1, :name, --?c\n /*?c*/ 'redacted-text');"
        );
    }

    #[test]
    fn agent_swarm_replay_loads_fixtures_and_rejects_schema_mismatch() {
        let trace = sample_trace();
        let json = serde_json::to_string(&trace).expect("trace json");

        assert_eq!(
            load_agent_swarm_trace_json(&json).expect("valid fixture"),
            trace
        );

        let mut stale = trace;
        stale.schema_version = "0.9.0".to_owned();
        let stale_json = serde_json::to_string(&stale).expect("stale json");
        assert!(matches!(
            load_agent_swarm_trace_json(&stale_json),
            Err(AgentSwarmReplayError::SchemaMismatch { .. })
        ));
    }

    #[test]
    fn agent_swarm_replay_preserves_trace_order_and_backend_identity() {
        let trace = generate_synthetic_swarm_trace(SyntheticTraceConfig::new(
            SyntheticTraceScenario::TaskQueueClaimLoop,
            0xA6E1_5A7E,
            3,
            4,
        ))
        .expect("synthetic trace");
        let config = AgentSwarmReplayConfig::smoke(0xA6E1_5A7E);
        let fsqlite = RecordingExecutor::new(EngineIdentity::FrankenSqlite);
        let csqlite = RecordingExecutor::new(EngineIdentity::CSqliteOracle);

        let report = replay_agent_swarm_trace_with_executors(&trace, &config, &fsqlite, &csqlite)
            .expect("replay succeeds");

        assert_eq!(report.seed, 0xA6E1_5A7E);
        assert!(report.commands.heavy_command.contains("rch exec"));
        assert!(report.schema_statement_count > 0);
        assert!(report.first_failure.is_none());
        assert!(report.cross_backend_mismatches.is_empty());
        assert_eq!(report.backends.len(), 2);
        assert_eq!(
            report.backends[0].identity.backend,
            AgentSwarmReplayBackend::FrankenSqliteConcurrent
        );
        assert!(report.backends[0].identity.concurrent_writer_default);
        assert_eq!(
            report.backends[1].identity.backend,
            AgentSwarmReplayBackend::CSqliteOracle
        );
        assert!(!report.backends[1].identity.concurrent_writer_default);
        assert_eq!(
            report.backends[0].statements[0].transaction_boundary,
            TransactionBoundary::Begin
        );
        assert_eq!(
            report.backends[0].statements[0].materialized_sql,
            "BEGIN CONCURRENT"
        );
        assert_eq!(report.backends[1].statements[0].materialized_sql, "BEGIN");
        assert!(
            report.backends[0]
                .statements
                .windows(2)
                .all(|window| window[0].logical_order <= window[1].logical_order)
        );
    }

    #[test]
    fn agent_swarm_replay_counts_expected_busy_without_first_failure() {
        let trace = busy_trace(ExpectedResultClass::Busy);
        let config = AgentSwarmReplayConfig::smoke(7).with_schema_sql([
            "CREATE TABLE swarm_counters(counter_key TEXT PRIMARY KEY, value INTEGER);",
        ]);
        let fsqlite =
            RecordingExecutor::failing(EngineIdentity::FrankenSqlite, "UPDATE", "database is busy");
        let csqlite =
            RecordingExecutor::failing(EngineIdentity::CSqliteOracle, "UPDATE", "database is busy");

        let report = replay_agent_swarm_trace_with_executors(&trace, &config, &fsqlite, &csqlite)
            .expect("busy replay succeeds");

        let summary = &report.backends[0].summary;
        assert_eq!(summary.error_count, 1);
        assert_eq!(summary.expected_mismatch_count, 0);
        assert_eq!(summary.conflict_classes.get("busy"), Some(&1));
        assert!(report.first_failure.is_none());
    }

    #[test]
    fn agent_swarm_replay_builds_first_failure_bundle_for_unexpected_error() {
        let trace = busy_trace(ExpectedResultClass::Success);
        let config = AgentSwarmReplayConfig::smoke(11).with_schema_sql([
            "CREATE TABLE swarm_counters(counter_key TEXT PRIMARY KEY, value INTEGER);",
        ]);
        let fsqlite =
            RecordingExecutor::failing(EngineIdentity::FrankenSqlite, "UPDATE", "database is busy");
        let csqlite =
            RecordingExecutor::failing(EngineIdentity::CSqliteOracle, "UPDATE", "database is busy");

        let report = replay_agent_swarm_trace_with_executors(&trace, &config, &fsqlite, &csqlite)
            .expect("failure replay succeeds");
        let failure = report.first_failure.expect("first failure bundle");

        assert_eq!(
            failure.backend,
            Some(AgentSwarmReplayBackend::FrankenSqliteConcurrent)
        );
        assert!(failure.reason.contains("expected_success_got_busy"));
        assert_eq!(failure.trace_statement_index, 1);
        assert_eq!(failure.minimal_reproducer_trace_slice.len(), 3);
        assert_eq!(failure.materialized_replay_sql.len(), 3);
    }

    #[test]
    fn agent_swarm_scorecard_profiles_include_rch_and_local_limits() {
        let profiles = default_agent_swarm_resource_profiles();

        assert_eq!(profiles.len(), 3);
        assert!(
            profiles
                .iter()
                .all(|profile| profile.heavy_rch_command.contains("rch exec"))
        );
        assert!(
            profiles
                .iter()
                .all(|profile| profile.local_statement_limit > 0)
        );
        assert!(profiles.iter().any(|profile| {
            profile.profile_id == AgentSwarmResourceProfileId::HighCapacityServer
                && profile.core_count >= 64
                && profile.memory_limit_bytes >= 256 * GIB
        }));
    }

    #[test]
    fn agent_swarm_scorecard_handles_zero_workload() {
        let report = resource_scorecard_report(Vec::new(), 0);
        let config =
            AgentSwarmResourceScorecardConfig::new(AgentSwarmResourceProfile::local_smoke());

        let scorecard = score_agent_swarm_resource_envelope(&report, &config);
        let backend = &scorecard.backends[0];

        assert_eq!(
            scorecard.performance_claim_guard.status,
            AgentSwarmPerformanceClaimStatus::ProfileOnly
        );
        assert_eq!(backend.cpu_utilization_per_mille, 0);
        assert_eq!(backend.memory_high_water_bytes, 0);
        assert!(!backend.memory_limit_exceeded);
        assert_eq!(backend.page_cache_utilization_per_mille, 0);
        assert_eq!(backend.abort_rate_per_mille, 0);
        assert_eq!(backend.retry_rate_per_mille, 0);
        assert_eq!(backend.fairness_index_per_mille, 0);
        assert!(backend.per_actor_statement_count.is_empty());
        assert_eq!(backend.tail_latency_spread_ns, 0);
    }

    #[test]
    fn agent_swarm_scorecard_detects_skewed_actor_fairness_and_rates() {
        let mut statements = (0..9)
            .map(|index| {
                resource_replay_record(
                    "agent-hot",
                    index,
                    100,
                    AgentSwarmReplayOutcomeClass::Success,
                    TransactionBoundary::None,
                    0,
                )
            })
            .collect::<Vec<_>>();
        statements.push(resource_replay_record(
            "agent-cold",
            9,
            400,
            AgentSwarmReplayOutcomeClass::Conflict,
            TransactionBoundary::Rollback,
            2,
        ));
        let report =
            resource_scorecard_report(statements, usize::try_from(128 * MIB).expect("fits"));
        let config =
            AgentSwarmResourceScorecardConfig::new(AgentSwarmResourceProfile::workstation())
                .with_readme_artifact(
                    "tests/artifacts/agent-swarm-scorecard.json",
                    "62de1594",
                    "2026-05-23",
                );

        let scorecard = score_agent_swarm_resource_envelope(&report, &config);
        let backend = &scorecard.backends[0];

        assert_eq!(
            scorecard.performance_claim_guard.status,
            AgentSwarmPerformanceClaimStatus::ArtifactBacked
        );
        assert_eq!(backend.per_actor_statement_count.get("agent-hot"), Some(&9));
        assert_eq!(
            backend.per_actor_statement_count.get("agent-cold"),
            Some(&1)
        );
        assert!(backend.fairness_index_per_mille > 500);
        assert!(backend.fairness_index_per_mille < 700);
        assert_eq!(backend.abort_rate_per_mille, 100);
        assert_eq!(backend.retry_rate_per_mille, 200);
        assert_eq!(backend.latency_p95_ns, 400);
        assert_eq!(backend.latency_p99_ns, 400);
        assert_eq!(backend.tail_latency_spread_ns, 300);
        assert!(backend.concurrent_writer_default);
    }

    #[test]
    fn agent_swarm_replay_captures_only_new_fallback_events_across_eviction_and_disable()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut statements = Vec::new();
        for logical_order in 0..71 {
            statements.push(TraceStatement::from_raw(RawTraceStatement {
                logical_order,
                logical_timestamp: None,
                actor_id: "actor",
                connection_id: "trace-connection",
                transaction_id: None,
                transaction_boundary: TransactionBoundary::None,
                concurrency_group: "ordinary-queries",
                workload_phase: "ordinary-query",
                sql: if logical_order < 70 {
                    "WITH source(value) AS (SELECT 7) SELECT value FROM source"
                } else {
                    "SELECT 7"
                },
                expected_result_class: ExpectedResultClass::Success,
                row_count_class: RowCountClass::Unknown,
                error_class: None,
                source: StatementSource::new("inline-harness", "bd-6hdwo.14")?,
            })?);
        }
        let trace = AgentSwarmTrace::new(
            "trace-actual-fallback",
            "run-actual-fallback",
            "actual-fallback",
            TraceMetadata::new("inline-harness", "bd-6hdwo.14")?,
            statements,
        )?;
        for enabled in [true, false] {
            let subject = FsqliteExecutor::open_in_memory()?;
            let oracle = CsqliteExecutor::open_in_memory()?;
            subject.query("WITH source(value) AS (SELECT 9) SELECT value FROM source")?;
            let previous = subject.fallback_execution_snapshot().unwrap();
            assert_eq!(previous.events.len(), 1);
            let previous_id = previous.events[0].event_id;
            if !enabled {
                subject.execute("PRAGMA fsqlite.fallback_capture = OFF")?;
                assert_eq!(
                    subject.query("PRAGMA fsqlite.fallback_capture")?,
                    vec![vec![crate::differential_v2::NormalizedValue::Integer(0)]]
                );
            }
            let report = replay_agent_swarm_trace_with_executors(
                &trace,
                &AgentSwarmReplayConfig::smoke(0xFA11_BACC),
                &subject,
                &oracle,
            )?;
            assert!(report.first_failure.is_none(), "{report:?}");
            let replay = &report.backends[0];
            let mut event_ids = BTreeSet::new();
            let mut statement_ids = BTreeSet::new();
            let mut drops = 0;
            for statement in &replay.statements {
                let evidence = statement.fallback_evidence.as_ref().unwrap();
                drops += evidence.dropped_events;
                assert_eq!(
                    evidence.events.len(),
                    usize::from(enabled && statement.logical_order < 70)
                );
                for event in &evidence.events {
                    assert!(event.event_id > previous_id);
                    assert!(event_ids.insert(event.event_id));
                    assert!(statement_ids.insert(event.statement_id));
                    assert_eq!(event.decision_reason, "with_clause_materialization");
                    assert_eq!(event.storage_backend, "memory");
                }
            }
            assert_eq!(event_ids.len(), if enabled { 70 } else { 0 });
            assert_eq!(drops, if enabled { 7 } else { 0 });
            let scorecard = score_agent_swarm_resource_envelope(
                &report,
                &AgentSwarmResourceScorecardConfig::new(AgentSwarmResourceProfile::local_smoke()),
            );
            assert_eq!(
                scorecard.backends[0]
                    .coordination_metrics
                    .fallback_reason_count,
                if enabled { 70 } else { 0 }
            );
            assert_eq!(
                scorecard.backends[1]
                    .coordination_metrics
                    .fallback_reason_count,
                0
            );
            assert!(
                report.backends[1]
                    .statements
                    .iter()
                    .all(|statement| statement.fallback_evidence.is_none())
            );
        }
        Ok(())
    }

    #[test]
    fn agent_swarm_scorecard_exposes_coordination_bridge_metrics() {
        let mut queue_claim = resource_replay_record(
            "agent-a",
            0,
            100,
            AgentSwarmReplayOutcomeClass::Success,
            TransactionBoundary::None,
            0,
        );
        queue_claim.workload_phase = "queue-claim".to_owned();
        queue_claim.materialized_sql =
            "UPDATE fsqlite_queue SET owner='redacted-text' WHERE status='ready' RETURNING queue_claim_id"
                .to_owned();

        let mut queue_release = resource_replay_record(
            "agent-a",
            1,
            110,
            AgentSwarmReplayOutcomeClass::Success,
            TransactionBoundary::None,
            0,
        );
        queue_release.workload_phase = "queue-release".to_owned();
        queue_release.materialized_sql =
            "UPDATE fsqlite_queue SET status='done' RETURNING queue_release_reason".to_owned();

        let mut expired_lease = resource_replay_record(
            "agent-b",
            2,
            120,
            AgentSwarmReplayOutcomeClass::Success,
            TransactionBoundary::None,
            0,
        );
        expired_lease.workload_phase = "expired-lease-takeover".to_owned();
        expired_lease.materialized_sql =
            "UPDATE fsqlite_lease SET lease_owner='redacted-text' WHERE lease_expires_ms < 100 RETURNING lease_generation"
                .to_owned();

        let mut range_split = resource_replay_record(
            "agent-b",
            3,
            130,
            AgentSwarmReplayOutcomeClass::Success,
            TransactionBoundary::None,
            0,
        );
        range_split.workload_phase = "range-imbalance-split".to_owned();
        range_split.materialized_sql =
            "INSERT INTO fsqlite_worker_ranges(worker_range_start, worker_range_end, range_imbalance_reason) VALUES (0, 99, 'redacted-text')"
                .to_owned();

        let mut explain_conflict = resource_replay_record(
            "agent-c",
            4,
            400,
            AgentSwarmReplayOutcomeClass::Conflict,
            TransactionBoundary::Rollback,
            2,
        );
        explain_conflict.workload_phase = "explain-concurrency-conflict".to_owned();
        explain_conflict.materialized_sql =
            "EXPLAIN CONCURRENCY SELECT * FROM fsqlite_coordination_events WHERE explain_concurrency_reason='hot_page_predicted'"
                .to_owned();

        let mut fallback = resource_replay_record(
            "agent-c",
            5,
            160,
            AgentSwarmReplayOutcomeClass::Success,
            TransactionBoundary::None,
            0,
        );
        fallback.workload_phase = "compatibility-fallback".to_owned();
        fallback.materialized_sql =
            "SELECT fallback_reason FROM fsqlite_coordination_events WHERE fallback_reason='compatibility_fallback'"
                .to_owned();

        let mut governor = resource_replay_record(
            "agent-d",
            6,
            140,
            AgentSwarmReplayOutcomeClass::Success,
            TransactionBoundary::None,
            0,
        );
        governor.workload_phase = "resource-governor-shadow".to_owned();
        governor.materialized_sql =
            "SELECT resource_governor_decision FROM fsqlite_coordination_events".to_owned();

        let report = resource_scorecard_report(
            vec![
                queue_claim,
                queue_release,
                expired_lease,
                range_split,
                explain_conflict,
                fallback,
                governor,
            ],
            8_192,
        );
        let config =
            AgentSwarmResourceScorecardConfig::new(AgentSwarmResourceProfile::local_smoke());

        let scorecard = score_agent_swarm_resource_envelope(&report, &config);
        let metrics = &scorecard.backends[0].coordination_metrics;

        assert_eq!(
            metrics.bridge_version,
            AGENT_SWARM_COORDINATION_BRIDGE_VERSION
        );
        assert_eq!(metrics.queue_claim_count, 1);
        assert_eq!(metrics.queue_release_count, 1);
        assert_eq!(metrics.lease_acquire_count, 1);
        assert_eq!(metrics.lease_expiration_count, 1);
        assert_eq!(metrics.range_allocation_count, 1);
        assert_eq!(metrics.explain_concurrency_row_count, None);
        assert_eq!(metrics.fallback_reason_count, 0);
        assert_eq!(metrics.resource_governor_decision_count, None);
        assert!(metrics.coordination_correctness_per_mille < 1_000);
        assert_eq!(metrics.conflict_transparency_per_mille, 0);
        assert!(metrics.fairness_resource_pressure_per_mille > 0);
        assert!(metrics.privacy_scrubber_preserved);
        assert!(
            metrics
                .scenarios
                .contains(&AgentSwarmCoordinationScenario::ConflictHeavy)
        );
        assert!(
            !metrics
                .scenarios
                .contains(&AgentSwarmCoordinationScenario::FallbackHeavy)
        );
        assert!(
            metrics
                .scenarios
                .contains(&AgentSwarmCoordinationScenario::ExpiredLease)
        );
        assert!(
            metrics
                .scenarios
                .contains(&AgentSwarmCoordinationScenario::RangeImbalance)
        );
        assert!(
            metrics
                .trace_fields
                .iter()
                .any(|field| field == "fallback_reason")
        );
    }

    #[test]
    fn agent_swarm_evidence_manifest_hash_is_stable_and_links_metrics() {
        let report = resource_scorecard_report(
            vec![resource_replay_record(
                "agent-a",
                0,
                500,
                AgentSwarmReplayOutcomeClass::Success,
                TransactionBoundary::None,
                0,
            )],
            4_096,
        );
        let config = AgentSwarmEvidenceManifestConfig::new(
            "tests/artifacts/trace.json",
            "cargo test -p fsqlite-harness agent_swarm_trace",
        )
        .with_trace_artifact_hash("trace-hash");

        let first = build_agent_swarm_evidence_manifest(&report, &config);
        let second = build_agent_swarm_evidence_manifest(&report, &config);

        assert_eq!(first.artifact_hash, second.artifact_hash);
        assert_eq!(first.trace_artifact_hash, "trace-hash");
        assert_eq!(first.failure_category, AgentSwarmFailureCategory::Unknown);
        assert!(first.minimized_trace_slice.is_empty());
        assert!(first.proposed_regression_bead.is_none());
        assert_eq!(first.backend_versions.len(), 1);
        assert!(first.backend_versions[0].concurrent_writer_default);
        assert_eq!(first.result_metrics[0].statements_total, 1);
        assert!(first.result_metrics[0].throughput_statements_per_second_x1000 > 0);
    }

    #[test]
    fn agent_swarm_ci_smoke_builds_operator_artifact_end_to_end() {
        let trace = generate_synthetic_swarm_trace(
            SyntheticTraceConfig::new(SyntheticTraceScenario::SessionEventAppend, 0xC1_5E_ED, 2, 2)
                .with_read_write_mix(0, 100),
        )
        .expect("synthetic smoke trace");
        let trace_json = serde_json::to_string(&trace).expect("trace json");
        let loaded_trace = load_agent_swarm_trace_json(&trace_json).expect("schema-valid trace");

        assert_eq!(loaded_trace.scrubber_version, SWARM_TRACE_SCRUBBER_VERSION);
        assert!(loaded_trace.redaction_summary.redaction_count > 0);

        let replay_config = AgentSwarmReplayConfig::smoke(0xC1_5E_ED);
        let fsqlite = RecordingExecutor::new(EngineIdentity::FrankenSqlite);
        let csqlite = RecordingExecutor::new(EngineIdentity::CSqliteOracle);
        let report = replay_agent_swarm_trace_with_executors(
            &loaded_trace,
            &replay_config,
            &fsqlite,
            &csqlite,
        )
        .expect("smoke replay succeeds");
        let scorecard_config =
            AgentSwarmResourceScorecardConfig::new(AgentSwarmResourceProfile::local_smoke());
        let scorecard = score_agent_swarm_resource_envelope(&report, &scorecard_config);
        let manifest_config = AgentSwarmEvidenceManifestConfig::new(
            "tests/artifacts/agent-swarm-ci-smoke-trace.json",
            "cargo test -p fsqlite-harness --lib agent_swarm_ci_smoke -- --nocapture",
        )
        .with_trace_artifact_hash(stable_json_hash(&loaded_trace))
        .without_regression_proposals();
        let manifest = build_agent_swarm_evidence_manifest(&report, &manifest_config);
        let artifact = build_agent_swarm_ci_smoke_artifact(
            &loaded_trace,
            &report,
            &scorecard,
            &manifest,
            "tests/artifacts/agent-swarm-ci-smoke-manifest.json",
        );

        assert_eq!(artifact.schema_version, SWARM_TRACE_SCHEMA_VERSION);
        assert_eq!(artifact.smoke_version, AGENT_SWARM_CI_SMOKE_VERSION);
        assert_eq!(artifact.trace_id, loaded_trace.trace_id);
        assert_eq!(artifact.run_id, loaded_trace.run_id);
        assert_eq!(artifact.scenario_id, loaded_trace.scenario_id);
        assert_eq!(artifact.profile_id, AgentSwarmResourceProfileId::LocalSmoke);
        assert_eq!(
            artifact.artifact_manifest_path,
            "tests/artifacts/agent-swarm-ci-smoke-manifest.json"
        );
        assert_eq!(artifact.first_failure_diag, FIRST_FAILURE_DIAGNOSTIC_ABSENT);
        assert_eq!(artifact.backend_artifacts.len(), 2);
        assert!(
            artifact
                .backend_artifacts
                .iter()
                .any(|backend| backend.backend == AgentSwarmReplayBackend::FrankenSqliteConcurrent)
        );
        assert!(
            artifact
                .backend_artifacts
                .iter()
                .all(|backend| backend.profile_id == AgentSwarmResourceProfileId::LocalSmoke)
        );
        assert!(artifact.command.contains("agent_swarm_ci_smoke"));
        assert_eq!(artifact.evidence_manifest_hash, manifest.artifact_hash);
        assert_eq!(artifact.artifact_hash, ci_smoke_artifact_hash(&artifact));
    }

    #[test]
    fn agent_swarm_evidence_minimization_preserves_transaction_slice() {
        let report = failure_report_with_reason("mvcc serialization conflict on hot page");
        let config = AgentSwarmEvidenceManifestConfig::new(
            "tests/artifacts/failing-trace.json",
            "cargo test -p fsqlite-harness agent_swarm_trace",
        );

        let manifest = build_agent_swarm_evidence_manifest(&report, &config);

        assert_eq!(manifest.failure_category, AgentSwarmFailureCategory::Mvcc);
        assert_eq!(manifest.minimized_trace_slice.len(), 3);
        assert_eq!(
            manifest.minimized_trace_hash,
            stable_json_hash(&manifest.minimized_trace_slice)
        );
        assert!(
            manifest
                .minimized_trace_slice
                .iter()
                .all(|statement| statement.transaction_id.as_deref() == Some("txn-busy"))
        );
        assert!(
            manifest
                .minimized_trace_slice
                .windows(2)
                .all(|window| window[0].concurrency_group == window[1].concurrency_group)
        );
        assert!(manifest.proposed_regression_bead.is_some());
    }

    #[test]
    fn agent_swarm_evidence_duplicate_detection_suppresses_proposal() {
        let report = failure_report_with_reason("wal checksum mismatch during replay");
        let first_config = AgentSwarmEvidenceManifestConfig::new(
            "tests/artifacts/failing-trace.json",
            "cargo test -p fsqlite-harness agent_swarm_trace",
        );
        let first = build_agent_swarm_evidence_manifest(&report, &first_config);
        let duplicate = AgentSwarmExistingBeadSummary::new(
            "bd-existing-regression",
            "Existing WAL replay regression",
        )
        .with_labels(["agent-swarm", "regression"])
        .with_duplicate_keys([first.minimized_trace_hash.clone()]);
        let duplicate_config = first_config.with_existing_beads([duplicate]);

        let manifest = build_agent_swarm_evidence_manifest(&report, &duplicate_config);

        assert_eq!(manifest.failure_category, AgentSwarmFailureCategory::Wal);
        assert_eq!(
            manifest.duplicate_candidate_ids,
            vec!["bd-existing-regression".to_owned()]
        );
        assert!(manifest.proposed_regression_bead.is_none());
    }

    #[test]
    fn agent_swarm_evidence_category_assignment_covers_core_surfaces() {
        let cases = [
            ("parser syntax error", AgentSwarmFailureCategory::Parser),
            (
                "planner chose stale index",
                AgentSwarmFailureCategory::Planner,
            ),
            ("vdbe opcode mismatch", AgentSwarmFailureCategory::Vdbe),
            ("pager page-cache miss", AgentSwarmFailureCategory::Pager),
            ("wal checkpoint mismatch", AgentSwarmFailureCategory::Wal),
            (
                "mvcc conflict false positive",
                AgentSwarmFailureCategory::Mvcc,
            ),
            (
                "json extension mismatch",
                AgentSwarmFailureCategory::Extension,
            ),
            (
                "resource memory high-water exceeded",
                AgentSwarmFailureCategory::ResourceEnvelope,
            ),
            ("opaque failure", AgentSwarmFailureCategory::Unknown),
        ];

        for (reason, expected) in cases {
            let report = failure_report_with_reason(reason);
            assert_eq!(classify_agent_swarm_failure(&report), expected);
        }
    }

    proptest! {
        #[test]
        fn synthetic_trace_generation_preserves_bounded_config(
            seed in any::<u64>(),
            scenario_index in 0usize..SYNTHETIC_TRACE_SCENARIOS.len(),
            agent_count in 1usize..12,
            transaction_count in 1usize..20,
            read_weight in 0u16..100,
            write_weight in 0u16..100,
            hot_key_ratio in 0u8..=100,
        ) {
            prop_assume!(u64::from(read_weight) + u64::from(write_weight) > 0);

            let scenario = SYNTHETIC_TRACE_SCENARIOS[scenario_index];
            let config = SyntheticTraceConfig::new(
                scenario,
                seed,
                agent_count,
                transaction_count,
            )
            .with_read_write_mix(read_weight, write_weight)
            .with_hot_key_ratio(hot_key_ratio);
            let trace = generate_synthetic_swarm_trace(config.clone()).expect("synthetic trace");

            prop_assert_eq!(trace.statement_count(), transaction_count * 3);
            prop_assert_eq!(trace.transaction_count(), transaction_count);
            let expected_seed = seed.to_string();
            let expected_agent_count = agent_count.to_string();
            let expected_transaction_count = transaction_count.to_string();
            let expected_hot_key_ratio = hot_key_ratio.to_string();
            let expected_read_write_mix = config.read_write_mix();
            prop_assert_eq!(
                trace.metadata.tags.get("scenario_family").map(String::as_str),
                Some(scenario.as_str())
            );
            prop_assert_eq!(
                trace.metadata.tags.get("seed").map(String::as_str),
                Some(expected_seed.as_str())
            );
            prop_assert_eq!(
                trace.metadata.tags.get("agent_count").map(String::as_str),
                Some(expected_agent_count.as_str())
            );
            prop_assert_eq!(
                trace
                    .metadata
                    .tags
                    .get("transaction_count")
                    .map(String::as_str),
                Some(expected_transaction_count.as_str())
            );
            prop_assert_eq!(
                trace.metadata.tags.get("hot_key_ratio").map(String::as_str),
                Some(expected_hot_key_ratio.as_str())
            );
            prop_assert_eq!(
                trace.metadata.tags.get("read_write_mix").map(String::as_str),
                Some(expected_read_write_mix.as_str())
            );
            let schema_safe = trace
                .statements
                .iter()
                .all(TraceStatement::schema_safe_for_synthetic_property);
            prop_assert!(schema_safe);
        }
    }

    fn failure_report_with_reason(reason: &str) -> AgentSwarmReplayReport {
        let trace = busy_trace(ExpectedResultClass::Success);
        let statements = vec![
            resource_replay_record(
                "agent-a",
                0,
                100,
                AgentSwarmReplayOutcomeClass::Success,
                TransactionBoundary::Begin,
                0,
            ),
            resource_replay_record(
                "agent-a",
                1,
                250,
                AgentSwarmReplayOutcomeClass::Conflict,
                TransactionBoundary::None,
                1,
            ),
            resource_replay_record(
                "agent-a",
                2,
                100,
                AgentSwarmReplayOutcomeClass::Success,
                TransactionBoundary::Commit,
                0,
            ),
        ];
        let mut report = resource_scorecard_report(statements, 8_192);
        report.trace_id = trace.trace_id.clone();
        report.run_id = trace.run_id.clone();
        report.scenario_id = trace.scenario_id.clone();
        report.first_failure = Some(AgentSwarmFirstFailureBundle {
            backend: Some(AgentSwarmReplayBackend::FrankenSqliteConcurrent),
            reason: reason.to_owned(),
            trace_statement_index: 1,
            trace_id: trace.trace_id,
            run_id: trace.run_id,
            scenario_id: trace.scenario_id,
            minimal_reproducer_trace_slice: trace.statements.clone(),
            materialized_replay_sql: trace
                .statements
                .iter()
                .map(|statement| materialize_scrubbed_sql(&statement.scrubbed_sql))
                .collect(),
        });
        report
    }

    fn raw_statement<'a>(
        logical_order: u64,
        actor_id: &'a str,
        transaction_id: Option<&'a str>,
        transaction_boundary: TransactionBoundary,
        sql: &'a str,
    ) -> RawTraceStatement<'a> {
        RawTraceStatement {
            logical_order,
            logical_timestamp: None,
            actor_id,
            connection_id: "conn-a",
            transaction_id,
            transaction_boundary,
            concurrency_group: "hot-pages",
            workload_phase: "hot-write",
            sql,
            expected_result_class: ExpectedResultClass::Success,
            row_count_class: RowCountClass::Unknown,
            error_class: None,
            source: StatementSource::new("unit-test", "statement").expect("valid source"),
        }
    }

    fn sample_trace() -> AgentSwarmTrace {
        let metadata = TraceMetadata::new("unit-test", "sample").expect("valid metadata");
        let source = StatementSource::new("unit-test", "statement").expect("valid source");
        let statements = vec![
            TraceStatement::from_raw(RawTraceStatement {
                source: source.clone(),
                ..raw_statement(
                    0,
                    "agent-a",
                    Some("txn-a"),
                    TransactionBoundary::Begin,
                    "BEGIN CONCURRENT",
                )
            })
            .expect("begin statement"),
            TraceStatement::from_raw(RawTraceStatement {
                logical_order: 1,
                source: source.clone(),
                ..raw_statement(
                    1,
                    "agent-a",
                    Some("txn-a"),
                    TransactionBoundary::None,
                    "INSERT INTO inbox(owner, body, raw, retries) VALUES ('agent-a', 'secret', X'DEADBEEF', 3); -- sensitive",
                )
            })
            .expect("insert statement"),
            TraceStatement::from_raw(RawTraceStatement {
                logical_order: 2,
                source: source.clone(),
                ..raw_statement(
                    2,
                    "agent-b",
                    Some("txn-a"),
                    TransactionBoundary::None,
                    "UPDATE inbox SET body = :body, retries = retries + 1 WHERE owner = 'agent-a' AND id = 98765;",
                )
            })
            .expect("update statement"),
            TraceStatement::from_raw(RawTraceStatement {
                source,
                ..raw_statement(
                    3,
                    "agent-a",
                    Some("txn-a"),
                    TransactionBoundary::Commit,
                    "COMMIT",
                )
            })
            .expect("commit statement"),
        ];
        AgentSwarmTrace::new(
            "trace-sample",
            "run-sample",
            "scenario-sample",
            metadata,
            statements,
        )
        .expect("sample trace")
    }

    fn busy_trace(expected_result_class: ExpectedResultClass) -> AgentSwarmTrace {
        let metadata = TraceMetadata::new("unit-test", "busy").expect("valid metadata");
        let source = StatementSource::new("unit-test", "busy-statement").expect("valid source");
        let mut update = raw_statement(
            1,
            "agent-a",
            Some("txn-busy"),
            TransactionBoundary::None,
            "UPDATE swarm_counters SET value = value + 1 WHERE counter_key = 'hot';",
        );
        update.expected_result_class = expected_result_class;
        update.error_class = Some("busy");

        let statements = vec![
            TraceStatement::from_raw(RawTraceStatement {
                source: source.clone(),
                ..raw_statement(
                    0,
                    "agent-a",
                    Some("txn-busy"),
                    TransactionBoundary::Begin,
                    "BEGIN CONCURRENT",
                )
            })
            .expect("begin"),
            TraceStatement::from_raw(RawTraceStatement {
                source: source.clone(),
                ..update
            })
            .expect("update"),
            TraceStatement::from_raw(RawTraceStatement {
                source,
                ..raw_statement(
                    2,
                    "agent-a",
                    Some("txn-busy"),
                    TransactionBoundary::Commit,
                    "COMMIT",
                )
            })
            .expect("commit"),
        ];

        AgentSwarmTrace::new(
            "trace-busy",
            "run-busy",
            "scenario-busy",
            metadata,
            statements,
        )
        .expect("busy trace")
    }

    fn resource_scorecard_report(
        statements: Vec<AgentSwarmStatementReplay>,
        memory_high_water_bytes: usize,
    ) -> AgentSwarmReplayReport {
        let statement_count = statements.len();
        let summary = summarize_backend_replay(&statements, memory_high_water_bytes);
        let backend = AgentSwarmBackendReplay {
            identity: AgentSwarmBackendIdentity {
                backend: AgentSwarmReplayBackend::FrankenSqliteConcurrent,
                engine_identity: "frankensqlite".to_owned(),
                comparison_role: "subject_concurrent_writer_default".to_owned(),
                concurrent_writer_default: true,
            },
            schema_failures: Vec::new(),
            statements,
            summary,
        };
        AgentSwarmReplayReport {
            schema_version: SWARM_TRACE_SCHEMA_VERSION.to_owned(),
            harness_version: AGENT_SWARM_REPLAY_HARNESS_VERSION.to_owned(),
            trace_id: "trace-scorecard".to_owned(),
            run_id: "run-scorecard".to_owned(),
            scenario_id: "scenario-scorecard".to_owned(),
            seed: 0x5C0E_C4A2D,
            schedule: AgentSwarmReplaySchedule::TraceOrder,
            commands: AgentSwarmReplayCommands::default(),
            schema_statement_count: 0,
            statement_count,
            transaction_count: statement_count,
            topology_errors: Vec::new(),
            backends: vec![backend],
            cross_backend_mismatches: Vec::new(),
            first_failure: None,
        }
    }

    fn resource_replay_record(
        actor_id: &str,
        trace_statement_index: usize,
        latency_ns: u64,
        outcome_class: AgentSwarmReplayOutcomeClass,
        transaction_boundary: TransactionBoundary,
        retry_count: u32,
    ) -> AgentSwarmStatementReplay {
        let outcome = match outcome_class {
            AgentSwarmReplayOutcomeClass::Success => StmtOutcome::Execute(1),
            AgentSwarmReplayOutcomeClass::Busy => StmtOutcome::Error("database is busy".to_owned()),
            AgentSwarmReplayOutcomeClass::Conflict => {
                StmtOutcome::Error("serialization conflict".to_owned())
            }
            AgentSwarmReplayOutcomeClass::Error => StmtOutcome::Error("other error".to_owned()),
        };
        let expected_matched = outcome_class == AgentSwarmReplayOutcomeClass::Success;
        AgentSwarmStatementReplay {
            backend: AgentSwarmReplayBackend::FrankenSqliteConcurrent,
            trace_statement_index,
            logical_order: usize_to_u64(trace_statement_index),
            actor_id: actor_id.to_owned(),
            connection_id: format!("{actor_id}-conn"),
            transaction_id: Some(format!("{actor_id}-txn-{trace_statement_index}")),
            transaction_boundary,
            concurrency_group: "scorecard-group".to_owned(),
            workload_phase: "scorecard-phase".to_owned(),
            statement_shape: sha256_hex(format!("shape-{trace_statement_index}").as_bytes()),
            materialized_sql: "SELECT 1".to_owned(),
            expected_result_class: ExpectedResultClass::Success,
            outcome,
            fallback_evidence: None,
            outcome_class,
            latency_ns,
            retry_count,
            expected_matched,
            first_failure_diag: if expected_matched {
                FIRST_FAILURE_DIAGNOSTIC_ABSENT.to_owned()
            } else {
                format!("expected_success_got_{}", outcome_class.as_str())
            },
        }
    }

    #[derive(Debug, Clone)]
    struct RecordingExecutor {
        identity: EngineIdentity,
        fail_on: Option<&'static str>,
        error: &'static str,
    }

    impl RecordingExecutor {
        const fn new(identity: EngineIdentity) -> Self {
            Self {
                identity,
                fail_on: None,
                error: "forced failure",
            }
        }

        const fn failing(
            identity: EngineIdentity,
            fail_on: &'static str,
            error: &'static str,
        ) -> Self {
            Self {
                identity,
                fail_on: Some(fail_on),
                error,
            }
        }
    }

    impl SqlExecutor for RecordingExecutor {
        fn execute(&self, sql: &str) -> Result<usize, String> {
            if self.fail_on.is_some_and(|needle| sql.contains(needle)) {
                Err(self.error.to_owned())
            } else {
                Ok(1)
            }
        }

        fn query(
            &self,
            _sql: &str,
        ) -> Result<Vec<Vec<crate::differential_v2::NormalizedValue>>, String> {
            Ok(Vec::new())
        }

        fn engine_identity(&self) -> EngineIdentity {
            self.identity
        }

        fn stmt_returns_rows(&self, sql: &str) -> Result<bool, String> {
            Ok(sql
                .split_whitespace()
                .next()
                .is_some_and(|word| word.eq_ignore_ascii_case("SELECT")))
        }
    }

    trait SyntheticTraceStatementTestExt {
        fn schema_safe_for_synthetic_property(&self) -> bool;
    }

    impl SyntheticTraceStatementTestExt for TraceStatement {
        fn schema_safe_for_synthetic_property(&self) -> bool {
            !self.actor_id.trim().is_empty()
                && !self.connection_id.trim().is_empty()
                && !self.concurrency_group.trim().is_empty()
                && !self.workload_phase.trim().is_empty()
                && !self.scrubbed_sql.trim().is_empty()
                && self.statement_shape.len() == 64
                && self.transaction_id.is_some()
        }
    }
}
