//! Automatic mismatch minimizer and canonical signature pipeline (bd-1dp9.2.3).
//!
//! Given a failing differential test (a
//! [`crate::differential_v2::DifferentialResult`] or
//! [`crate::metamorphic::MetamorphicTestCase`] that diverges), this module:
//!
//! 1. **Minimizes** the workload to the smallest subset of SQL statements
//!    that still reproduces the divergence (delta debugging).
//! 2. **Extracts a canonical signature** from the minimal reproduction to
//!    enable deduplication of repeated failures.
//! 3. **Classifies** and **triages** minimized failures by subsystem and
//!    severity.
//!
//! # Minimization Strategy
//!
//! The minimizer uses a binary-search delta-debugging algorithm:
//!
//! 1. **Binary partition**: split the workload in half and test each half.
//! 2. **Recursive narrowing**: if one half still fails, recurse into it.
//! 3. **1-minimal**: try removing each remaining statement individually.
//! 4. **Schema preservation**: schema setup statements are never removed
//!    (they define the tables/indexes required by the workload).
//!
//! # Canonical Signatures
//!
//! A [`MismatchSignature`] is a content-addressed fingerprint of a minimal
//! divergence. It captures:
//! - The mismatch classification
//! - The subsystem attribution (parser, planner, VDBE, storage, etc.)
//! - A hash of the minimal SQL + schema
//!
//! Signatures enable deduplication: if two failures produce the same
//! signature, they are the same root-cause bug.
//!
//! # Determinism
//!
//! All operations are deterministic given the same input. Hashes use
//! SHA-256 truncated to 16 hex characters for readability.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fmt::Write as _;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::differential_v2::StatementDivergence;
use crate::failure_bundle::FailureBundle;
use crate::metamorphic::MismatchClassification;
use crate::serializability_oracle::{
    Anomaly, HistoryOperation, OracleVerdict, SerializabilityReport, TransactionHistory,
    check_history, validate_serializability_failure_bundle,
};
use crate::test_inventory::ExecutionLane;
use crate::typed_sql_generator::{
    Expr, GeneratedStatement, Identifier, Select, SqlValue, Statement as GeneratedAstStatement,
    StatementRole, TransactionStatement,
};

/// Bead identifier for log correlation.
#[allow(dead_code)]
const BEAD_ID: &str = "bd-1dp9.2.3";

/// Schema version for the minimizer output format.
pub const MINIMIZER_SCHEMA_VERSION: u32 = 1;
/// Schema version for generator-AST reduction evidence.
pub const TYPED_REDUCTION_SCHEMA_VERSION: &str = "fsqlite.typed-reduction.v1";
/// Schema version for transaction-history reduction evidence.
pub const HISTORY_REDUCTION_SCHEMA_VERSION: &str = "fsqlite.history-reduction.v1";
/// Canonical failure-bundle snapshot key for history reduction evidence.
pub const HISTORY_REDUCTION_SNAPSHOT_KEY: &str = "history_reduction_v1";

// ===========================================================================
// Subsystem Attribution
// ===========================================================================

/// Likely subsystem responsible for a divergence.
///
/// Attribution is heuristic-based: the minimizer inspects the failing SQL
/// and divergence pattern to guess which FrankenSQLite subsystem is at fault.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Subsystem {
    /// SQL parser or tokenizer.
    Parser,
    /// Name resolver or schema lookup.
    Resolver,
    /// Query planner or optimizer.
    Planner,
    /// VDBE bytecode compiler or VM execution.
    Vdbe,
    /// B-tree or page-level storage.
    Storage,
    /// WAL, pager, or checkpoint logic.
    Wal,
    /// MVCC version chain or conflict detection.
    Mvcc,
    /// Built-in function implementation.
    Functions,
    /// Extension module (FTS, JSON, R-tree, etc.).
    Extension,
    /// Type system, affinity, or collation.
    TypeSystem,
    /// PRAGMA handling.
    Pragma,
    /// Unknown or cross-cutting.
    Unknown,
}

impl fmt::Display for Subsystem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Parser => write!(f, "parser"),
            Self::Resolver => write!(f, "resolver"),
            Self::Planner => write!(f, "planner"),
            Self::Vdbe => write!(f, "vdbe"),
            Self::Storage => write!(f, "storage"),
            Self::Wal => write!(f, "wal"),
            Self::Mvcc => write!(f, "mvcc"),
            Self::Functions => write!(f, "functions"),
            Self::Extension => write!(f, "extension"),
            Self::TypeSystem => write!(f, "type_system"),
            Self::Pragma => write!(f, "pragma"),
            Self::Unknown => write!(f, "unknown"),
        }
    }
}

// ===========================================================================
// Canonical Signature
// ===========================================================================

/// A content-addressed fingerprint of a minimal divergence.
///
/// Two failures with the same signature are considered duplicates of the
/// same root-cause bug. The signature is stable across runs as long as
/// the minimized SQL and classification are identical.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct MismatchSignature {
    /// Truncated SHA-256 of the canonical minimal reproduction.
    pub hash: String,
    /// Mismatch classification.
    pub classification: MismatchClassification,
    /// Attributed subsystem.
    pub subsystem: Subsystem,
    /// Number of statements in the minimal reproduction.
    pub minimal_statement_count: usize,
    /// First diverging SQL statement (for human readability).
    pub first_diverging_sql: String,
}

impl MismatchSignature {
    /// Compute a signature from a minimal reproduction.
    #[must_use]
    pub fn compute(
        schema: &[String],
        minimal_workload: &[String],
        classification: &MismatchClassification,
        subsystem: Subsystem,
        first_divergence: Option<&StatementDivergence>,
    ) -> Self {
        // Build canonical content for hashing.
        let mut hasher = Sha256::new();
        hasher.update(b"sig-v1:");
        hasher.update(classification.to_string().as_bytes());
        hasher.update(b":");
        hasher.update(subsystem.to_string().as_bytes());
        hasher.update(b":");
        for stmt in schema {
            hasher.update(stmt.as_bytes());
            hasher.update(b"\n");
        }
        hasher.update(b"---workload---\n");
        for stmt in minimal_workload {
            hasher.update(stmt.as_bytes());
            hasher.update(b"\n");
        }
        let digest = hasher.finalize();
        let hash = hex_encode_truncated(&digest, 16);

        let first_diverging_sql = first_divergence.map(|d| d.sql.clone()).unwrap_or_default();

        Self {
            hash,
            classification: classification.clone(),
            subsystem,
            minimal_statement_count: minimal_workload.len(),
            first_diverging_sql,
        }
    }
}

impl fmt::Display for MismatchSignature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "SIG-{}/{}/{}stmts",
            self.hash, self.subsystem, self.minimal_statement_count
        )
    }
}

// ===========================================================================
// Minimized Reproduction
// ===========================================================================

/// A minimal reproduction of a differential failure.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MinimalReproduction {
    /// Schema version.
    pub schema_version: u32,
    /// Canonical signature for deduplication.
    pub signature: MismatchSignature,
    /// Original envelope seed.
    pub original_seed: u64,
    /// Schema setup SQL (preserved from original).
    pub schema: Vec<String>,
    /// Minimal workload that reproduces the divergence.
    pub minimal_workload: Vec<String>,
    /// Original workload size.
    pub original_workload_size: usize,
    /// Reduction ratio: `1 - (minimal / original)`.
    pub reduction_ratio: f64,
    /// Index of the first diverging statement in the minimal workload.
    pub first_divergence_index: Option<usize>,
    /// The statement divergences in the minimal reproduction.
    pub divergences: Vec<StatementDivergence>,
    /// Reproduction command.
    pub repro_command: String,
}

impl MinimalReproduction {
    /// Serialize to deterministic JSON.
    ///
    /// # Errors
    ///
    /// Returns `Err` if serialization fails.
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }

    /// Deserialize from JSON.
    ///
    /// # Errors
    ///
    /// Returns `Err` if the JSON is malformed.
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }
}

// ===========================================================================
// Minimizer Configuration
// ===========================================================================

/// Configuration for the mismatch minimizer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MinimizerConfig {
    /// Maximum number of delta-debugging iterations before giving up.
    pub max_iterations: usize,
    /// Whether to attempt 1-minimal reduction (try removing each statement).
    pub one_minimal: bool,
    /// Maximum workload size to attempt minimization on.
    /// Larger workloads skip straight to signature extraction.
    pub max_workload_size: usize,
}

impl Default for MinimizerConfig {
    fn default() -> Self {
        Self {
            max_iterations: 100,
            one_minimal: true,
            max_workload_size: 1000,
        }
    }
}

/// Deterministic budget and cancellation controls for structured reduction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TypedReductionConfig {
    /// Maximum verifier calls after the required full-case verification.
    pub max_attempts: usize,
    /// Cancel before this zero-based attempt number.
    pub cancel_after_attempts: Option<usize>,
}

impl Default for TypedReductionConfig {
    fn default() -> Self {
        Self {
            max_attempts: 1_000,
            cancel_after_attempts: None,
        }
    }
}

/// Stable reducer categories used in traces and aggregate accounting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TypedReductionKind {
    Statement,
    Transaction,
    Clause,
    Join,
    Projection,
    Predicate,
    OrderTerm,
    Index,
    Expression,
    SchemaTable,
    SchemaColumn,
    InsertRow,
    Value,
}

/// Exact oracle identity that every accepted candidate must preserve.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TypedReductionObservation {
    pub mismatch_signature: String,
    pub required_lanes: Vec<ExecutionLane>,
}

impl TypedReductionObservation {
    fn validate(&self) -> Result<(), String> {
        if self.mismatch_signature.trim().is_empty() {
            return Err("structured reducer mismatch signature is empty".to_owned());
        }
        if self.required_lanes.is_empty() {
            return Err("structured reducer required lane set is empty".to_owned());
        }
        Ok(())
    }
}

/// Why structured reduction stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TypedReductionStatus {
    Complete,
    BudgetExhausted,
    Cancelled,
}

impl TypedReductionStatus {
    #[must_use]
    pub const fn is_complete(self) -> bool {
        matches!(self, Self::Complete)
    }
}

/// One deterministic candidate decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TypedReductionAttempt {
    pub ordinal: usize,
    pub kind: TypedReductionKind,
    pub path: String,
    pub before_sha256: String,
    pub after_sha256: String,
    pub accepted: bool,
    pub rationale: String,
}

/// Stable aggregate statistics for one reduction run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TypedReductionStats {
    pub original_statements: usize,
    pub minimized_statements: usize,
    pub original_bytes: usize,
    pub minimized_bytes: usize,
    pub attempts: usize,
    pub accepted_candidates: usize,
    pub rejected_candidates: usize,
}

/// Canonical structured-reduction artifact retained by bundles and replay.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TypedReductionResult {
    pub schema_version: String,
    pub config: TypedReductionConfig,
    pub original_statements: Vec<GeneratedStatement>,
    pub minimized_statements: Vec<GeneratedStatement>,
    pub observation: TypedReductionObservation,
    pub trace: Vec<TypedReductionAttempt>,
    pub stats: TypedReductionStats,
    pub status: TypedReductionStatus,
    pub first_rejected_invariant: Option<String>,
    pub content_hash: String,
}

impl TypedReductionResult {
    #[must_use]
    pub fn deterministic_hash(&self) -> String {
        let mut canonical = self.clone();
        canonical.content_hash.clear();
        let bytes = serde_json::to_vec(&canonical)
            .expect("typed reduction artifact serialization must succeed");
        sha256_hex(&bytes)
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.schema_version != TYPED_REDUCTION_SCHEMA_VERSION {
            return Err("typed reduction schema version is unsupported".to_owned());
        }
        if self.original_statements.is_empty() || self.minimized_statements.is_empty() {
            return Err("typed reduction statement sets must be non-empty".to_owned());
        }
        self.observation.validate()?;
        let lanes = self
            .observation
            .required_lanes
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        if lanes.len() != self.observation.required_lanes.len() {
            return Err("typed reduction required lanes contain duplicates".to_owned());
        }
        if self.stats.original_statements != self.original_statements.len()
            || self.stats.minimized_statements != self.minimized_statements.len()
            || self.stats.original_bytes != statement_payload_bytes(&self.original_statements)
            || self.stats.minimized_bytes != statement_payload_bytes(&self.minimized_statements)
            || self.stats.attempts != self.trace.len()
            || self.stats.accepted_candidates
                != self.trace.iter().filter(|attempt| attempt.accepted).count()
            || self.stats.rejected_candidates
                != self
                    .trace
                    .iter()
                    .filter(|attempt| !attempt.accepted)
                    .count()
        {
            return Err("typed reduction statistics do not match the payload".to_owned());
        }
        if self.trace.len() > self.config.max_attempts {
            return Err("typed reduction trace exceeds its attempt budget".to_owned());
        }
        match self.status {
            TypedReductionStatus::Complete => {}
            TypedReductionStatus::BudgetExhausted
                if self.trace.len() == self.config.max_attempts => {}
            TypedReductionStatus::Cancelled
                if self
                    .config
                    .cancel_after_attempts
                    .is_some_and(|limit| self.trace.len() == limit) => {}
            TypedReductionStatus::BudgetExhausted | TypedReductionStatus::Cancelled => {
                return Err("typed reduction status contradicts its budget controls".to_owned());
            }
        }
        let mut current_hash = statement_payload_hash(&self.original_statements);
        for (ordinal, attempt) in self.trace.iter().enumerate() {
            if attempt.ordinal != ordinal
                || attempt.path.trim().is_empty()
                || attempt.rationale.trim().is_empty()
                || !is_sha256_hex_64(&attempt.before_sha256)
                || !is_sha256_hex_64(&attempt.after_sha256)
                || attempt.before_sha256 != current_hash
                || attempt.before_sha256 == attempt.after_sha256
            {
                return Err("typed reduction trace is malformed or discontinuous".to_owned());
            }
            if attempt.accepted {
                current_hash.clone_from(&attempt.after_sha256);
            }
        }
        if current_hash != statement_payload_hash(&self.minimized_statements) {
            return Err("typed reduction trace does not produce the minimized payload".to_owned());
        }
        let first_rejected = self
            .trace
            .iter()
            .find(|attempt| !attempt.accepted)
            .map(|attempt| attempt.rationale.as_str());
        if first_rejected != self.first_rejected_invariant.as_deref() {
            return Err("typed reduction first rejected invariant drifted".to_owned());
        }
        if self.content_hash != self.deterministic_hash() {
            return Err("typed reduction content hash mismatch".to_owned());
        }
        Ok(())
    }

    pub fn to_json(&self) -> Result<String, String> {
        self.validate()?;
        serde_json::to_string_pretty(self).map_err(|error| error.to_string())
    }

    pub fn from_json_strict(json: &str) -> Result<Self, String> {
        let result: Self = serde_json::from_str(json)
            .map_err(|error| format!("typed reduction decode failed: {error}"))?;
        result.validate()?;
        Ok(result)
    }
}

/// History plus scheduler-owned dimensions that are not duplicated in the
/// transaction-history event schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryReductionCase {
    pub history: TransactionHistory,
    pub schedule_events: Vec<String>,
    pub yield_choices: Vec<String>,
    pub observed_fields: BTreeMap<String, String>,
}

impl HistoryReductionCase {
    /// Validate the canonical history and reducer-owned scheduler dimensions.
    pub fn validate(&self) -> Result<SerializabilityReport, String> {
        let report = check_history(&self.history)?;
        validate_named_values("schedule event", &self.schedule_events)?;
        validate_named_values("yield choice", &self.yield_choices)?;
        if self
            .observed_fields
            .iter()
            .any(|(key, value)| key.trim().is_empty() || value.trim().is_empty())
        {
            return Err(
                "history reduction observed fields contain an empty key or value".to_owned(),
            );
        }
        Ok(report)
    }

    /// Stable identity used by the reduction trace.
    #[must_use]
    pub fn deterministic_hash(&self) -> String {
        let bytes =
            serde_json::to_vec(self).expect("history reduction case serialization must succeed");
        sha256_hex(&bytes)
    }
}

/// Exact failure identity that every accepted history candidate must retain.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryReductionObservation {
    pub verdict: OracleVerdict,
    pub minimal_witness: Option<Anomaly>,
    pub failure_signature: String,
    pub required_lanes: Vec<ExecutionLane>,
    pub final_state_sha256: String,
}

impl HistoryReductionObservation {
    /// Derive the canonical oracle, lane, and final-state identity for a case.
    pub fn from_case(
        case: &HistoryReductionCase,
        failure_signature: impl Into<String>,
    ) -> Result<Self, String> {
        let report = case.validate()?;
        let mut required_lanes = case
            .history
            .execution_lane_evidence
            .iter()
            .map(|evidence| evidence.required_lane)
            .collect::<Vec<_>>();
        required_lanes.sort_unstable();
        required_lanes.dedup();
        let observation = Self {
            verdict: report.verdict,
            minimal_witness: report.minimal_witness,
            failure_signature: failure_signature.into(),
            required_lanes,
            final_state_sha256: case.history.final_state_sha256.clone(),
        };
        observation.validate()?;
        Ok(observation)
    }

    fn validate(&self) -> Result<(), String> {
        if self.failure_signature.trim().is_empty() {
            return Err("history reduction failure signature is empty".to_owned());
        }
        if self.required_lanes.is_empty() {
            return Err("history reduction required lane set is empty".to_owned());
        }
        if self
            .required_lanes
            .windows(2)
            .any(|lanes| lanes[0] >= lanes[1])
        {
            return Err("history reduction required lanes are not canonical".to_owned());
        }
        if !is_sha256_hex_64(&self.final_state_sha256) {
            return Err("history reduction final-state hash is malformed".to_owned());
        }
        Ok(())
    }
}

/// Stable history reducer categories used by traces and dimensional accounting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HistoryReductionKind {
    Transaction,
    Worker,
    Operation,
    Checkpoint,
    CrashPoint,
    ScheduleEvent,
    YieldChoice,
    ObservedField,
}

/// One deterministic history candidate decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryReductionAttempt {
    pub ordinal: usize,
    pub kind: HistoryReductionKind,
    pub path: String,
    pub before_sha256: String,
    pub after_sha256: String,
    pub accepted: bool,
    pub rationale: String,
}

/// Dimensional history size retained for original and minimized artifacts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryReductionSize {
    pub transactions: usize,
    pub operations: usize,
    pub workers: usize,
    pub schedule_events: usize,
    pub yield_choices: usize,
    pub checkpoints: usize,
    pub crash_points: usize,
    pub observed_fields: usize,
}

impl HistoryReductionSize {
    fn for_case(case: &HistoryReductionCase) -> Self {
        let transactions = case
            .history
            .events
            .iter()
            .filter_map(|event| event.transaction_id.as_deref())
            .collect::<BTreeSet<_>>()
            .len();
        let workers = case
            .history
            .events
            .iter()
            .map(|event| event.process_id.as_str())
            .collect::<BTreeSet<_>>()
            .len();
        let checkpoints = case
            .history
            .events
            .iter()
            .filter(|event| matches!(event.operation, HistoryOperation::Checkpoint { .. }))
            .count();
        let crash_points = case
            .history
            .events
            .iter()
            .filter_map(|event| match &event.operation {
                HistoryOperation::Crash { crash_id } => Some(crash_id.as_str()),
                _ => None,
            })
            .collect::<BTreeSet<_>>()
            .len();
        Self {
            transactions,
            operations: case.history.events.len(),
            workers,
            schedule_events: case.schedule_events.len(),
            yield_choices: case.yield_choices.len(),
            checkpoints,
            crash_points,
            observed_fields: case.observed_fields.len(),
        }
    }

    const fn componentwise_le(self, other: Self) -> bool {
        self.transactions <= other.transactions
            && self.operations <= other.operations
            && self.workers <= other.workers
            && self.schedule_events <= other.schedule_events
            && self.yield_choices <= other.yield_choices
            && self.checkpoints <= other.checkpoints
            && self.crash_points <= other.crash_points
            && self.observed_fields <= other.observed_fields
    }
}

/// Aggregate statistics for one history reduction run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryReductionStats {
    pub original: HistoryReductionSize,
    pub minimized: HistoryReductionSize,
    pub attempts: usize,
    pub accepted_candidates: usize,
    pub rejected_candidates: usize,
}

/// Canonical history reduction artifact retained by replay bundles.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryReductionResult {
    pub schema_version: String,
    pub config: TypedReductionConfig,
    pub original: HistoryReductionCase,
    pub minimized: HistoryReductionCase,
    pub observation: HistoryReductionObservation,
    pub trace: Vec<HistoryReductionAttempt>,
    pub stats: HistoryReductionStats,
    pub status: TypedReductionStatus,
    pub first_rejected_invariant: Option<String>,
    pub content_hash: String,
}

impl HistoryReductionResult {
    /// Hash replay-relevant content while excluding the hash field itself.
    #[must_use]
    pub fn deterministic_hash(&self) -> String {
        let mut canonical = self.clone();
        canonical.content_hash.clear();
        let bytes = serde_json::to_vec(&canonical)
            .expect("history reduction artifact serialization must succeed");
        sha256_hex(&bytes)
    }

    /// Validate schemas, dimensions, trace continuity, and exact oracle identity.
    pub fn validate(&self) -> Result<(), String> {
        if self.schema_version != HISTORY_REDUCTION_SCHEMA_VERSION {
            return Err("history reduction schema version is unsupported".to_owned());
        }
        self.original.validate()?;
        self.minimized.validate()?;
        self.observation.validate()?;
        let minimized_observation = HistoryReductionObservation::from_case(
            &self.minimized,
            self.observation.failure_signature.clone(),
        )?;
        // ubs:ignore - deterministic reducer evidence identity, not authentication material.
        if minimized_observation != self.observation {
            return Err(
                "history reduction minimized witness, lane, or state identity drifted".to_owned(),
            );
        }
        let expected_stats = HistoryReductionStats {
            original: HistoryReductionSize::for_case(&self.original),
            minimized: HistoryReductionSize::for_case(&self.minimized),
            attempts: self.trace.len(),
            accepted_candidates: self.trace.iter().filter(|attempt| attempt.accepted).count(),
            rejected_candidates: self
                .trace
                .iter()
                .filter(|attempt| !attempt.accepted)
                .count(),
        };
        if self.stats != expected_stats
            || !self.stats.minimized.componentwise_le(self.stats.original)
        {
            return Err(
                "history reduction statistics do not match monotonic payload dimensions".to_owned(),
            );
        }
        validate_reduction_status(self.status, &self.config, self.trace.len())?;
        let mut current_hash = self.original.deterministic_hash();
        for (ordinal, attempt) in self.trace.iter().enumerate() {
            if attempt.ordinal != ordinal
                || attempt.path.trim().is_empty()
                || attempt.rationale.trim().is_empty()
                || !is_sha256_hex_64(&attempt.before_sha256)
                || !is_sha256_hex_64(&attempt.after_sha256)
                || attempt.before_sha256 != current_hash
                || attempt.before_sha256 == attempt.after_sha256
            {
                return Err("history reduction trace is malformed or discontinuous".to_owned());
            }
            if attempt.accepted {
                current_hash.clone_from(&attempt.after_sha256);
            }
        }
        if current_hash != self.minimized.deterministic_hash() {
            return Err("history reduction trace does not produce the minimized case".to_owned());
        }
        let first_rejected = self
            .trace
            .iter()
            .find(|attempt| !attempt.accepted)
            .map(|attempt| attempt.rationale.as_str());
        if first_rejected != self.first_rejected_invariant.as_deref() {
            return Err("history reduction first rejected invariant drifted".to_owned());
        }
        if self.content_hash != self.deterministic_hash() {
            return Err("history reduction content hash mismatch".to_owned());
        }
        Ok(())
    }

    /// Serialize a validated history reduction artifact.
    pub fn to_json(&self) -> Result<String, String> {
        self.validate()?;
        serde_json::to_string_pretty(self).map_err(|error| error.to_string())
    }

    /// Strictly decode and validate a history reduction artifact.
    pub fn from_json_strict(json: &str) -> Result<Self, String> {
        let result: Self = serde_json::from_str(json)
            .map_err(|error| format!("history reduction decode failed: {error}"))?;
        result.validate()?;
        Ok(result)
    }

    /// Attach reduction evidence to the existing canonical serializability bundle.
    pub fn attach_to_failure_bundle(&self, bundle: &mut FailureBundle) -> Result<(), String> {
        self.validate()?;
        let (history, report) = validate_serializability_failure_bundle(bundle)?;
        // ubs:ignore - typed history payload identity, not authentication material.
        if history != self.minimized.history {
            return Err(
                "history reduction bundle does not contain the minimized history".to_owned(),
            );
        }
        // ubs:ignore - deterministic oracle witness identity, not authentication material.
        if report.verdict != self.observation.verdict
            // ubs:ignore - deterministic oracle witness identity, not authentication material.
            || report.minimal_witness != self.observation.minimal_witness
        {
            return Err("history reduction bundle witness identity drifted".to_owned());
        }
        let mut attached = bundle.clone();
        attached
            .state_snapshots
            .insert(HISTORY_REDUCTION_SNAPSHOT_KEY.to_owned(), self.to_json()?);
        if !attached
            .triage_tags
            .iter()
            .any(|tag| tag == "history-reduced")
        {
            attached.triage_tags.push("history-reduced".to_owned());
        }
        attached.content_hash = attached.deterministic_bundle_hash();
        let errors = attached.validate();
        if errors.is_empty() {
            *bundle = attached;
            Ok(())
        } else {
            Err(format!(
                "history reduction produced an invalid failure bundle: {}",
                errors.join("; ")
            ))
        }
    }
}

/// Candidate verifier used by the history-domain reducer.
pub type HistoryReproducibilityTest<'a> =
    dyn Fn(&HistoryReductionCase) -> Result<HistoryReductionObservation, String> + 'a;

#[derive(Debug, Clone)]
struct HistoryReductionCandidate {
    kind: HistoryReductionKind,
    path: String,
    case: HistoryReductionCase,
}

/// Reduce a typed history while preserving exact witness, lane, and final-state identity.
pub fn minimize_history_case(
    case: &HistoryReductionCase,
    config: &TypedReductionConfig,
    test_fn: &HistoryReproducibilityTest<'_>,
) -> Result<HistoryReductionResult, String> {
    case.validate()?;
    let observation = test_fn(case)?;
    observation.validate()?;
    if HistoryReductionObservation::from_case(case, observation.failure_signature.clone())?
        != observation
    {
        return Err("history reduction verifier returned inconsistent initial evidence".to_owned());
    }

    let original = case.clone();
    let mut current = original.clone();
    let mut trace = Vec::new();
    let mut status = TypedReductionStatus::Complete;
    let mut first_rejected_invariant = None;

    'passes: loop {
        let candidates = history_reduction_candidates(&current);
        let mut accepted_in_pass = false;
        for candidate in candidates {
            if config
                .cancel_after_attempts
                .is_some_and(|limit| trace.len() >= limit)
            {
                status = TypedReductionStatus::Cancelled;
                break 'passes;
            }
            if trace.len() >= config.max_attempts {
                status = TypedReductionStatus::BudgetExhausted;
                break 'passes;
            }

            let before_sha256 = current.deterministic_hash();
            let after_sha256 = candidate.case.deterministic_hash();
            let (accepted, rationale) = match candidate.case.validate() {
                Err(error) => (
                    false,
                    format!("rejected: lifecycle or causal invariant: {error}"),
                ),
                Ok(_) => match test_fn(&candidate.case) {
                    // ubs:ignore - deterministic reducer evidence identity, not authentication material.
                    Ok(candidate_observation) if candidate_observation == observation => (
                        true,
                        "exact history witness, required lanes, and final state preserved"
                            .to_owned(),
                    ),
                    Ok(candidate_observation)
                        // ubs:ignore - deterministic failure signature, not authentication material.
                        if candidate_observation.failure_signature
                            != observation.failure_signature
                            // ubs:ignore - deterministic oracle verdict, not authentication material.
                            || candidate_observation.verdict != observation.verdict
                            // ubs:ignore - deterministic oracle witness, not authentication material.
                            || candidate_observation.minimal_witness
                                != observation.minimal_witness =>
                    {
                        (
                            false,
                            "rejected: exact history or crash witness drifted".to_owned(),
                        )
                    }
                    Ok(candidate_observation)
                        // ubs:ignore - deterministic lane identity, not authentication material.
                        if candidate_observation.required_lanes != observation.required_lanes =>
                    {
                        (
                            false,
                            "rejected: required execution lane identity drifted".to_owned(),
                        )
                    }
                    Ok(_) => (false, "rejected: final-state identity drifted".to_owned()),
                    Err(error) => (false, format!("rejected: candidate replay failed: {error}")),
                },
            };
            if !accepted && first_rejected_invariant.is_none() {
                first_rejected_invariant = Some(rationale.clone());
            }
            trace.push(HistoryReductionAttempt {
                ordinal: trace.len(),
                kind: candidate.kind,
                path: candidate.path,
                before_sha256,
                after_sha256,
                accepted,
                rationale,
            });
            if accepted {
                current = candidate.case;
                accepted_in_pass = true;
                break;
            }
        }
        if !accepted_in_pass {
            break;
        }
    }

    let stats = HistoryReductionStats {
        original: HistoryReductionSize::for_case(&original),
        minimized: HistoryReductionSize::for_case(&current),
        attempts: trace.len(),
        accepted_candidates: trace.iter().filter(|attempt| attempt.accepted).count(),
        rejected_candidates: trace.iter().filter(|attempt| !attempt.accepted).count(),
    };
    tracing::info!(
        target: "fsqlite.history_reduction",
        original_transactions = stats.original.transactions,
        minimized_transactions = stats.minimized.transactions,
        original_operations = stats.original.operations,
        minimized_operations = stats.minimized.operations,
        witness = %observation.failure_signature,
        attempts = stats.attempts,
        complete = status.is_complete(),
        "history reduction completed"
    );
    let mut result = HistoryReductionResult {
        schema_version: HISTORY_REDUCTION_SCHEMA_VERSION.to_owned(),
        config: config.clone(),
        original,
        minimized: current,
        observation,
        trace,
        stats,
        status,
        first_rejected_invariant,
        content_hash: String::new(),
    };
    result.content_hash = result.deterministic_hash();
    result.validate()?;
    Ok(result)
}

fn validate_named_values(kind: &str, values: &[String]) -> Result<(), String> {
    if values.iter().any(|value| value.trim().is_empty()) {
        return Err(format!("history reduction {kind} is empty"));
    }
    if values.iter().collect::<BTreeSet<_>>().len() != values.len() {
        return Err(format!(
            "history reduction {kind} values contain duplicates"
        ));
    }
    Ok(())
}

fn validate_reduction_status(
    status: TypedReductionStatus,
    config: &TypedReductionConfig,
    attempts: usize,
) -> Result<(), String> {
    if attempts > config.max_attempts {
        return Err("history reduction trace exceeds its attempt budget".to_owned());
    }
    match status {
        TypedReductionStatus::Complete => Ok(()),
        TypedReductionStatus::BudgetExhausted if attempts == config.max_attempts => Ok(()),
        TypedReductionStatus::Cancelled
            if config
                .cancel_after_attempts
                .is_some_and(|limit| attempts == limit) =>
        {
            Ok(())
        }
        TypedReductionStatus::BudgetExhausted | TypedReductionStatus::Cancelled => {
            Err("history reduction status contradicts its budget controls".to_owned())
        }
    }
}

fn history_reduction_candidates(case: &HistoryReductionCase) -> Vec<HistoryReductionCandidate> {
    let mut candidates = Vec::new();
    let mut seen = BTreeSet::new();
    let worker_ids = case
        .history
        .events
        .iter()
        .map(|event| event.process_id.clone())
        .collect::<BTreeSet<_>>();
    for worker_id in worker_ids {
        let mut candidate = case.clone();
        candidate
            .history
            .events
            .retain(|event| event.process_id != worker_id);
        push_history_candidate(
            &mut candidates,
            &mut seen,
            HistoryReductionKind::Worker,
            format!("worker:{worker_id}"),
            candidate,
        );
    }

    let transaction_ids = case
        .history
        .events
        .iter()
        .filter_map(|event| event.transaction_id.clone())
        .collect::<BTreeSet<_>>();
    for transaction_id in transaction_ids {
        let mut candidate = case.clone();
        candidate
            .history
            .events
            .retain(|event| event.transaction_id.as_deref() != Some(transaction_id.as_str()));
        push_history_candidate(
            &mut candidates,
            &mut seen,
            HistoryReductionKind::Transaction,
            format!("transaction:{transaction_id}"),
            candidate,
        );
    }

    for (index, event) in case.history.events.iter().enumerate() {
        if matches!(
            event.operation,
            HistoryOperation::Crash { .. }
                | HistoryOperation::Restart { .. }
                | HistoryOperation::Checkpoint { .. }
        ) {
            continue;
        }
        let mut candidate = case.clone();
        candidate.history.events.remove(index);
        push_history_candidate(
            &mut candidates,
            &mut seen,
            HistoryReductionKind::Operation,
            format!("operation:event-{}", event.event_id),
            candidate,
        );
    }

    for (index, event) in case.history.events.iter().enumerate() {
        if !matches!(event.operation, HistoryOperation::Checkpoint { .. }) {
            continue;
        }
        let mut candidate = case.clone();
        candidate.history.events.remove(index);
        push_history_candidate(
            &mut candidates,
            &mut seen,
            HistoryReductionKind::Checkpoint,
            format!("checkpoint:event-{}", event.event_id),
            candidate,
        );
    }

    let crash_ids = case
        .history
        .events
        .iter()
        .filter_map(|event| match &event.operation {
            HistoryOperation::Crash { crash_id } => Some(crash_id.clone()),
            _ => None,
        })
        .collect::<BTreeSet<_>>();
    for crash_id in crash_ids {
        let mut candidate = case.clone();
        candidate.history.events.retain(|event| {
            !matches!(
                &event.operation,
                HistoryOperation::Crash { crash_id: id }
                    | HistoryOperation::Restart { crash_id: id }
                    if id == &crash_id
            )
        });
        push_history_candidate(
            &mut candidates,
            &mut seen,
            HistoryReductionKind::CrashPoint,
            format!("crash:{crash_id}"),
            candidate,
        );
    }

    for index in 0..case.schedule_events.len() {
        let mut candidate = case.clone();
        candidate.schedule_events.remove(index);
        push_history_candidate(
            &mut candidates,
            &mut seen,
            HistoryReductionKind::ScheduleEvent,
            format!("schedule_event:{index}"),
            candidate,
        );
    }
    for index in 0..case.yield_choices.len() {
        let mut candidate = case.clone();
        candidate.yield_choices.remove(index);
        push_history_candidate(
            &mut candidates,
            &mut seen,
            HistoryReductionKind::YieldChoice,
            format!("yield_choice:{index}"),
            candidate,
        );
    }
    for key in case.observed_fields.keys() {
        let mut candidate = case.clone();
        candidate.observed_fields.remove(key);
        push_history_candidate(
            &mut candidates,
            &mut seen,
            HistoryReductionKind::ObservedField,
            format!("observed_field:{key}"),
            candidate,
        );
    }
    candidates
}

fn push_history_candidate(
    candidates: &mut Vec<HistoryReductionCandidate>,
    seen: &mut BTreeSet<String>,
    kind: HistoryReductionKind,
    path: String,
    mut case: HistoryReductionCase,
) {
    for (event_id, event) in case.history.events.iter_mut().enumerate() {
        event.event_id = u64::try_from(event_id).expect("history event count must fit u64");
    }
    let hash = case.deterministic_hash();
    if seen.insert(hash) {
        candidates.push(HistoryReductionCandidate { kind, path, case });
    }
}

/// Candidate verifier used by the structured reducer.
pub type TypedReproducibilityTest<'a> =
    dyn Fn(&[GeneratedStatement]) -> Result<TypedReductionObservation, String> + 'a;

#[derive(Debug, Clone)]
struct TypedReductionCandidate {
    kind: TypedReductionKind,
    path: String,
    statements: Vec<GeneratedStatement>,
}

/// Reduce generator-owned statement trees while preserving the exact oracle
/// signature and required execution lanes returned by `test_fn`.
pub fn minimize_typed_statements(
    statements: &[GeneratedStatement],
    config: &TypedReductionConfig,
    test_fn: &TypedReproducibilityTest<'_>,
) -> Result<TypedReductionResult, String> {
    if statements.is_empty() {
        return Err("structured reduction requires at least one statement".to_owned());
    }
    let observation = test_fn(statements)?;
    observation.validate()?;

    let original = statements.to_vec();
    let mut current = original.clone();
    let mut trace = Vec::new();
    let mut status = TypedReductionStatus::Complete;
    let mut first_rejected_invariant = None;

    'passes: loop {
        let candidates = typed_reduction_candidates(&current);
        let mut accepted_in_pass = false;
        for candidate in candidates {
            if config
                .cancel_after_attempts
                .is_some_and(|limit| trace.len() >= limit)
            {
                status = TypedReductionStatus::Cancelled;
                break 'passes;
            }
            if trace.len() >= config.max_attempts {
                status = TypedReductionStatus::BudgetExhausted;
                break 'passes;
            }

            let before_sha256 = statement_payload_hash(&current);
            let after_sha256 = statement_payload_hash(&candidate.statements);
            let (accepted, rationale) = match test_fn(&candidate.statements) {
                // ubs:ignore - deterministic reducer evidence identity, not authentication material.
                Ok(candidate_observation) if candidate_observation == observation => (
                    true,
                    "exact mismatch signature and required lanes preserved".to_owned(),
                ),
                Ok(candidate_observation)
                    if candidate_observation.mismatch_signature
                        != observation.mismatch_signature =>
                {
                    (
                        false,
                        "rejected: exact result/error mismatch signature drifted".to_owned(),
                    )
                }
                Ok(_) => (
                    false,
                    "rejected: required execution lane identity drifted".to_owned(),
                ),
                Err(error) => (false, format!("rejected: candidate invalid: {error}")),
            };
            if !accepted && first_rejected_invariant.is_none() {
                first_rejected_invariant = Some(rationale.clone());
            }
            trace.push(TypedReductionAttempt {
                ordinal: trace.len(),
                kind: candidate.kind,
                path: candidate.path,
                before_sha256,
                after_sha256,
                accepted,
                rationale,
            });
            if accepted {
                current = candidate.statements;
                accepted_in_pass = true;
                break;
            }
        }
        if !accepted_in_pass {
            break;
        }
    }

    let original_bytes = statement_payload_bytes(&original);
    let minimized_bytes = statement_payload_bytes(&current);
    let accepted_candidates = trace.iter().filter(|attempt| attempt.accepted).count();
    let mut result = TypedReductionResult {
        schema_version: TYPED_REDUCTION_SCHEMA_VERSION.to_owned(),
        config: config.clone(),
        original_statements: original,
        minimized_statements: current,
        observation,
        stats: TypedReductionStats {
            original_statements: statements.len(),
            minimized_statements: 0,
            original_bytes,
            minimized_bytes,
            attempts: trace.len(),
            accepted_candidates,
            rejected_candidates: trace.len().saturating_sub(accepted_candidates),
        },
        trace,
        status,
        first_rejected_invariant,
        content_hash: String::new(),
    };
    result.stats.minimized_statements = result.minimized_statements.len();
    result.content_hash = result.deterministic_hash();
    result.validate()?;
    Ok(result)
}

fn typed_reduction_candidates(statements: &[GeneratedStatement]) -> Vec<TypedReductionCandidate> {
    let mut candidates = Vec::new();
    let mut seen = BTreeSet::new();

    for index in 0..statements.len() {
        let mut reduced = statements.to_vec();
        let removed = reduced.remove(index);
        let kind = if matches!(removed.ast, GeneratedAstStatement::CreateIndex { .. }) {
            TypedReductionKind::Index
        } else {
            TypedReductionKind::Statement
        };
        push_typed_candidate(
            &mut candidates,
            &mut seen,
            kind,
            format!("statements[{index}]"),
            reduced,
        );
    }

    for (begin, end) in transaction_ranges(statements) {
        let reduced = statements
            .iter()
            .enumerate()
            .filter(|(index, _)| *index != begin && *index != end)
            .map(|(_, statement)| statement.clone())
            .collect();
        push_typed_candidate(
            &mut candidates,
            &mut seen,
            TypedReductionKind::Transaction,
            format!("transaction[{begin}..={end}].boundaries"),
            reduced,
        );
    }

    for (index, statement) in statements.iter().enumerate() {
        for (kind, path, ast) in statement_reduction_candidates(&statement.ast) {
            let mut reduced = statements.to_vec();
            reduced[index].ast = ast;
            reduced[index].sql = reduced[index].ast.to_sql();
            push_typed_candidate(
                &mut candidates,
                &mut seen,
                kind,
                format!("statements[{index}].{path}"),
                reduced,
            );
        }
    }

    for (table, column) in schema_column_candidates(statements) {
        if let Some(reduced) = remove_schema_column(statements, &table, &column) {
            push_typed_candidate(
                &mut candidates,
                &mut seen,
                TypedReductionKind::SchemaColumn,
                format!("schema.{}.column.{}", table.as_str(), column.as_str()),
                reduced,
            );
        }
    }
    for table in schema_table_candidates(statements) {
        let reduced = statements
            .iter()
            .filter(|statement| !statement_references_table(&statement.ast, &table))
            .cloned()
            .collect();
        push_typed_candidate(
            &mut candidates,
            &mut seen,
            TypedReductionKind::SchemaTable,
            format!("schema.table.{}", table.as_str()),
            reduced,
        );
    }
    candidates
}

fn push_typed_candidate(
    candidates: &mut Vec<TypedReductionCandidate>,
    seen: &mut BTreeSet<String>,
    kind: TypedReductionKind,
    path: String,
    statements: Vec<GeneratedStatement>,
) {
    if statements.is_empty()
        || !statements
            .iter()
            .any(|statement| statement.role == StatementRole::Subject)
    {
        return;
    }
    let hash = statement_payload_hash(&statements);
    if seen.insert(hash) {
        candidates.push(TypedReductionCandidate {
            kind,
            path,
            statements,
        });
    }
}

fn transaction_ranges(statements: &[GeneratedStatement]) -> Vec<(usize, usize)> {
    let mut ranges = Vec::new();
    let mut begin = None;
    for (index, statement) in statements.iter().enumerate() {
        match statement.ast {
            GeneratedAstStatement::Transaction {
                statement: TransactionStatement::Begin,
            } => begin = Some(index),
            GeneratedAstStatement::Transaction {
                statement: TransactionStatement::Commit | TransactionStatement::Rollback,
            } => {
                if let Some(start) = begin.take() {
                    ranges.push((start, index));
                }
            }
            _ => {}
        }
    }
    ranges
}

fn statement_reduction_candidates(
    statement: &GeneratedAstStatement,
) -> Vec<(TypedReductionKind, String, GeneratedAstStatement)> {
    let mut candidates = Vec::new();
    match statement {
        GeneratedAstStatement::CreateTable { table, columns } => {
            if columns.len() > 1 {
                for index in 0..columns.len() {
                    let mut reduced = columns.clone();
                    reduced.remove(index);
                    candidates.push((
                        TypedReductionKind::SchemaColumn,
                        format!("create_table.columns[{index}]"),
                        GeneratedAstStatement::CreateTable {
                            table: table.clone(),
                            columns: reduced,
                        },
                    ));
                }
            }
        }
        GeneratedAstStatement::CreateIndex {
            index,
            table,
            columns,
            unique,
        } => {
            if columns.len() > 1 {
                for position in 0..columns.len() {
                    let mut reduced = columns.clone();
                    reduced.remove(position);
                    candidates.push((
                        TypedReductionKind::Index,
                        format!("create_index.columns[{position}]"),
                        GeneratedAstStatement::CreateIndex {
                            index: index.clone(),
                            table: table.clone(),
                            columns: reduced,
                            unique: *unique,
                        },
                    ));
                }
            }
        }
        GeneratedAstStatement::Insert {
            table,
            columns,
            rows,
        } => {
            if rows.len() > 1 {
                for row_index in 0..rows.len() {
                    let mut reduced_rows = rows.clone();
                    reduced_rows.remove(row_index);
                    candidates.push((
                        TypedReductionKind::InsertRow,
                        format!("insert.rows[{row_index}]"),
                        GeneratedAstStatement::Insert {
                            table: table.clone(),
                            columns: columns.clone(),
                            rows: reduced_rows,
                        },
                    ));
                }
            }
            for (row_index, row) in rows.iter().enumerate() {
                for (value_index, value) in row.iter().enumerate() {
                    for reduced_value in reduced_values(value) {
                        let mut reduced_rows = rows.clone();
                        reduced_rows[row_index][value_index] = reduced_value;
                        candidates.push((
                            TypedReductionKind::Value,
                            format!("insert.rows[{row_index}].values[{value_index}]"),
                            GeneratedAstStatement::Insert {
                                table: table.clone(),
                                columns: columns.clone(),
                                rows: reduced_rows,
                            },
                        ));
                    }
                }
            }
        }
        GeneratedAstStatement::Update {
            table,
            assignments,
            predicate,
        } => {
            if assignments.len() > 1 {
                for index in 0..assignments.len() {
                    let mut reduced = assignments.clone();
                    reduced.remove(index);
                    candidates.push((
                        TypedReductionKind::Projection,
                        format!("update.assignments[{index}]"),
                        GeneratedAstStatement::Update {
                            table: table.clone(),
                            assignments: reduced,
                            predicate: predicate.clone(),
                        },
                    ));
                }
            }
            for (index, (_, expression)) in assignments.iter().enumerate() {
                for (path, reduced_expression) in expression_reductions(expression) {
                    let mut reduced = assignments.clone();
                    reduced[index].1 = reduced_expression;
                    candidates.push((
                        TypedReductionKind::Expression,
                        format!("update.assignments[{index}].{path}"),
                        GeneratedAstStatement::Update {
                            table: table.clone(),
                            assignments: reduced,
                            predicate: predicate.clone(),
                        },
                    ));
                }
            }
            if let Some(expression) = predicate {
                candidates.push((
                    TypedReductionKind::Predicate,
                    "update.predicate".to_owned(),
                    GeneratedAstStatement::Update {
                        table: table.clone(),
                        assignments: assignments.clone(),
                        predicate: None,
                    },
                ));
                for (path, reduced_expression) in expression_reductions(expression) {
                    candidates.push((
                        TypedReductionKind::Expression,
                        format!("update.predicate.{path}"),
                        GeneratedAstStatement::Update {
                            table: table.clone(),
                            assignments: assignments.clone(),
                            predicate: Some(reduced_expression),
                        },
                    ));
                }
            }
        }
        GeneratedAstStatement::Delete { table, predicate } => {
            if let Some(expression) = predicate {
                candidates.push((
                    TypedReductionKind::Predicate,
                    "delete.predicate".to_owned(),
                    GeneratedAstStatement::Delete {
                        table: table.clone(),
                        predicate: None,
                    },
                ));
                for (path, reduced_expression) in expression_reductions(expression) {
                    candidates.push((
                        TypedReductionKind::Expression,
                        format!("delete.predicate.{path}"),
                        GeneratedAstStatement::Delete {
                            table: table.clone(),
                            predicate: Some(reduced_expression),
                        },
                    ));
                }
            }
        }
        GeneratedAstStatement::Select { select } => {
            for (kind, path, reduced) in select_reduction_candidates(select) {
                candidates.push((
                    kind,
                    path,
                    GeneratedAstStatement::Select { select: reduced },
                ));
            }
        }
        GeneratedAstStatement::Transaction { .. } => {}
    }
    candidates
}

fn select_reduction_candidates(select: &Select) -> Vec<(TypedReductionKind, String, Select)> {
    let mut candidates = Vec::new();
    if select.distinct {
        let mut reduced = select.clone();
        reduced.distinct = false;
        candidates.push((
            TypedReductionKind::Clause,
            "select.distinct".to_owned(),
            reduced,
        ));
    }
    if select.projection.len() > 1 {
        for index in 0..select.projection.len() {
            let mut reduced = select.clone();
            reduced.projection.remove(index);
            candidates.push((
                TypedReductionKind::Projection,
                format!("select.projection[{index}]"),
                reduced,
            ));
        }
    }
    for (index, item) in select.projection.iter().enumerate() {
        for (path, expression) in expression_reductions(&item.expr) {
            let mut reduced = select.clone();
            reduced.projection[index].expr = expression;
            candidates.push((
                TypedReductionKind::Expression,
                format!("select.projection[{index}].{path}"),
                reduced,
            ));
        }
    }
    for index in 0..select.joins.len() {
        let mut reduced = select.clone();
        reduced.joins.remove(index);
        candidates.push((
            TypedReductionKind::Join,
            format!("select.joins[{index}]"),
            reduced,
        ));
    }
    if let Some(predicate) = &select.predicate {
        let mut reduced = select.clone();
        reduced.predicate = None;
        candidates.push((
            TypedReductionKind::Predicate,
            "select.predicate".to_owned(),
            reduced,
        ));
        for (path, expression) in expression_reductions(predicate) {
            let mut reduced = select.clone();
            reduced.predicate = Some(expression);
            candidates.push((
                TypedReductionKind::Expression,
                format!("select.predicate.{path}"),
                reduced,
            ));
        }
    }
    for index in 0..select.group_by.len() {
        let mut reduced = select.clone();
        reduced.group_by.remove(index);
        candidates.push((
            TypedReductionKind::Clause,
            format!("select.group_by[{index}]"),
            reduced,
        ));
    }
    if select.having.is_some() {
        let mut reduced = select.clone();
        reduced.having = None;
        candidates.push((
            TypedReductionKind::Predicate,
            "select.having".to_owned(),
            reduced,
        ));
    }
    if select.compound.is_some() {
        let mut reduced = select.clone();
        reduced.compound = None;
        candidates.push((
            TypedReductionKind::Clause,
            "select.compound".to_owned(),
            reduced,
        ));
    }
    for index in 0..select.order_by.len() {
        let mut reduced = select.clone();
        reduced.order_by.remove(index);
        candidates.push((
            TypedReductionKind::OrderTerm,
            format!("select.order_by[{index}]"),
            reduced,
        ));
    }
    if select.limit.is_some() {
        let mut reduced = select.clone();
        reduced.limit = None;
        candidates.push((
            TypedReductionKind::Clause,
            "select.limit".to_owned(),
            reduced,
        ));
    }
    candidates
}

fn expression_reductions(expression: &Expr) -> Vec<(String, Expr)> {
    let mut candidates = Vec::new();
    match expression {
        Expr::Value { value } => {
            for reduced in reduced_values(value) {
                candidates.push(("value".to_owned(), Expr::Value { value: reduced }));
            }
        }
        Expr::Column { .. } | Expr::ScalarSubquery { .. } => {}
        Expr::Unary { op, expr } => {
            candidates.push(("unwrap".to_owned(), expr.as_ref().clone()));
            for (path, reduced) in expression_reductions(expr) {
                candidates.push((
                    format!("unary.{path}"),
                    Expr::Unary {
                        op: *op,
                        expr: Box::new(reduced),
                    },
                ));
            }
        }
        Expr::Binary { left, op, right } => {
            candidates.push(("left".to_owned(), left.as_ref().clone()));
            candidates.push(("right".to_owned(), right.as_ref().clone()));
            for (path, reduced) in expression_reductions(left) {
                candidates.push((
                    format!("left.{path}"),
                    Expr::Binary {
                        left: Box::new(reduced),
                        op: *op,
                        right: right.clone(),
                    },
                ));
            }
            for (path, reduced) in expression_reductions(right) {
                candidates.push((
                    format!("right.{path}"),
                    Expr::Binary {
                        left: left.clone(),
                        op: *op,
                        right: Box::new(reduced),
                    },
                ));
            }
        }
        Expr::IsNull { expr, negated } => {
            candidates.push(("unwrap".to_owned(), expr.as_ref().clone()));
            for (path, reduced) in expression_reductions(expr) {
                candidates.push((
                    format!("is_null.{path}"),
                    Expr::IsNull {
                        expr: Box::new(reduced),
                        negated: *negated,
                    },
                ));
            }
        }
        Expr::Aggregate {
            function,
            expr,
            distinct,
        } => {
            if let Some(inner) = expr {
                candidates.push(("aggregate.unwrap".to_owned(), inner.as_ref().clone()));
                for (path, reduced) in expression_reductions(inner) {
                    candidates.push((
                        format!("aggregate.{path}"),
                        Expr::Aggregate {
                            function: *function,
                            expr: Some(Box::new(reduced)),
                            distinct: *distinct,
                        },
                    ));
                }
            }
            if *distinct {
                candidates.push((
                    "aggregate.distinct".to_owned(),
                    Expr::Aggregate {
                        function: *function,
                        expr: expr.clone(),
                        distinct: false,
                    },
                ));
            }
        }
        Expr::InSubquery {
            expr,
            subquery,
            negated,
        } => {
            candidates.push(("in_subquery.left".to_owned(), expr.as_ref().clone()));
            for (path, reduced) in expression_reductions(expr) {
                candidates.push((
                    format!("in_subquery.{path}"),
                    Expr::InSubquery {
                        expr: Box::new(reduced),
                        subquery: subquery.clone(),
                        negated: *negated,
                    },
                ));
            }
        }
    }
    candidates
}

fn reduced_values(value: &SqlValue) -> Vec<SqlValue> {
    match value {
        SqlValue::Null => Vec::new(),
        SqlValue::Integer(value) if *value == 0 => Vec::new(),
        SqlValue::Integer(value) => {
            let mut values = vec![SqlValue::Integer(0)];
            let halved = *value / 2;
            if halved != 0 {
                values.push(SqlValue::Integer(halved));
            }
            values
        }
        SqlValue::Real(value) if value.as_str() == "0.0" => Vec::new(),
        SqlValue::Real(_) => crate::typed_sql_generator::RealLiteral::new("0.0")
            .map(SqlValue::Real)
            .into_iter()
            .collect(),
        SqlValue::Text(value) if value.is_empty() => Vec::new(),
        SqlValue::Text(value) => {
            let midpoint = value.len() / 2;
            let mut values = vec![SqlValue::Text(String::new())];
            if value.is_char_boundary(midpoint) && midpoint > 0 {
                values.push(SqlValue::Text(value[..midpoint].to_owned()));
            }
            values
        }
        SqlValue::Blob(value) if value.is_empty() => Vec::new(),
        SqlValue::Blob(value) => {
            let mut values = vec![SqlValue::Blob(Vec::new())];
            if value.len() > 1 {
                values.push(SqlValue::Blob(value[..value.len() / 2].to_vec()));
            }
            values
        }
    }
}

fn schema_column_candidates(statements: &[GeneratedStatement]) -> Vec<(Identifier, Identifier)> {
    let mut candidates = Vec::new();
    for statement in statements {
        if let GeneratedAstStatement::CreateTable { table, columns } = &statement.ast {
            for column in columns {
                if !column.primary_key {
                    candidates.push((table.clone(), column.name.clone()));
                }
            }
        }
    }
    candidates
}

fn schema_table_candidates(statements: &[GeneratedStatement]) -> Vec<Identifier> {
    statements
        .iter()
        .filter_map(|statement| match &statement.ast {
            GeneratedAstStatement::CreateTable { table, .. } => Some(table.clone()),
            _ => None,
        })
        .collect()
}

fn remove_schema_column(
    statements: &[GeneratedStatement],
    table: &Identifier,
    column: &Identifier,
) -> Option<Vec<GeneratedStatement>> {
    let mut reduced = Vec::with_capacity(statements.len());
    let mut changed = false;
    for statement in statements {
        let mut statement = statement.clone();
        match &mut statement.ast {
            GeneratedAstStatement::CreateTable {
                table: statement_table,
                columns,
            } if statement_table == table => {
                let original = columns.len();
                columns.retain(|candidate| candidate.name != *column);
                changed |= columns.len() != original;
                if columns.is_empty() {
                    return None;
                }
            }
            GeneratedAstStatement::CreateIndex {
                table: statement_table,
                columns,
                ..
            } if statement_table == table => {
                columns.retain(|candidate| candidate != column);
                if columns.is_empty() {
                    changed = true;
                    continue;
                }
            }
            GeneratedAstStatement::Insert {
                table: statement_table,
                columns,
                rows,
            } if statement_table == table => {
                if let Some(position) = columns.iter().position(|candidate| candidate == column) {
                    columns.remove(position);
                    for row in rows {
                        if position >= row.len() {
                            return None;
                        }
                        row.remove(position);
                    }
                    changed = true;
                }
                if columns.is_empty() {
                    continue;
                }
            }
            GeneratedAstStatement::Update {
                table: statement_table,
                assignments,
                ..
            } if statement_table == table => {
                let original = assignments.len();
                assignments.retain(|(candidate, _)| candidate != column);
                changed |= assignments.len() != original;
                if assignments.is_empty() {
                    continue;
                }
            }
            _ => {}
        }
        statement.sql = statement.ast.to_sql();
        reduced.push(statement);
    }
    changed.then_some(reduced)
}

fn statement_references_table(statement: &GeneratedAstStatement, table: &Identifier) -> bool {
    match statement {
        GeneratedAstStatement::CreateTable {
            table: statement_table,
            ..
        }
        | GeneratedAstStatement::CreateIndex {
            table: statement_table,
            ..
        }
        | GeneratedAstStatement::Insert {
            table: statement_table,
            ..
        }
        | GeneratedAstStatement::Update {
            table: statement_table,
            ..
        }
        | GeneratedAstStatement::Delete {
            table: statement_table,
            ..
        } => statement_table == table,
        GeneratedAstStatement::Select { select } => select_references_table(select, table),
        GeneratedAstStatement::Transaction { .. } => false,
    }
}

fn select_references_table(select: &Select, table: &Identifier) -> bool {
    select
        .from
        .as_ref()
        .is_some_and(|from| from.table == *table)
        || select.joins.iter().any(|join| join.table == *table)
        || select
            .projection
            .iter()
            .any(|item| expression_references_table(&item.expr, table))
        || select
            .predicate
            .as_ref()
            .is_some_and(|expression| expression_references_table(expression, table))
        || select
            .group_by
            .iter()
            .any(|expression| expression_references_table(expression, table))
        || select
            .having
            .as_ref()
            .is_some_and(|expression| expression_references_table(expression, table))
        || select
            .compound
            .as_ref()
            .is_some_and(|compound| select_references_table(&compound.right, table))
}

fn expression_references_table(expression: &Expr, table: &Identifier) -> bool {
    match expression {
        Expr::Value { .. } | Expr::Aggregate { expr: None, .. } => false,
        Expr::Column {
            table: expression_table,
            ..
        } => expression_table
            .as_ref()
            .is_some_and(|candidate| candidate == table),
        Expr::Unary { expr, .. }
        | Expr::IsNull { expr, .. }
        | Expr::Aggregate {
            expr: Some(expr), ..
        } => expression_references_table(expr, table),
        Expr::Binary { left, right, .. } => {
            expression_references_table(left, table) || expression_references_table(right, table)
        }
        Expr::InSubquery { expr, subquery, .. } => {
            expression_references_table(expr, table) || select_references_table(subquery, table)
        }
        Expr::ScalarSubquery { subquery } => select_references_table(subquery, table),
    }
}

fn statement_payload_bytes(statements: &[GeneratedStatement]) -> usize {
    serde_json::to_vec(statements)
        .expect("generated statements must serialize for deterministic reduction")
        .len()
}

fn statement_payload_hash(statements: &[GeneratedStatement]) -> String {
    let bytes = serde_json::to_vec(statements)
        .expect("generated statements must serialize for deterministic reduction");
    sha256_hex(&bytes)
}

// ===========================================================================
// Subsystem Attribution Heuristics
// ===========================================================================

/// Attribute a divergence to a likely subsystem based on SQL content and
/// divergence pattern.
#[must_use]
#[allow(clippy::too_many_lines)]
pub fn attribute_subsystem(
    divergences: &[StatementDivergence],
    schema: &[String],
    workload: &[String],
) -> Subsystem {
    // Collect all SQL for keyword analysis.
    let all_sql: String = schema
        .iter()
        .chain(workload.iter())
        .map(String::as_str)
        .collect::<Vec<_>>()
        .join(" ");
    let all_upper = all_sql.to_uppercase();

    // Check for diverging SQL content.
    let diverging_sql: String = divergences
        .iter()
        .map(|d| d.sql.as_str())
        .collect::<Vec<_>>()
        .join(" ");
    let div_upper = diverging_sql.to_uppercase();

    // Extension signals.
    if div_upper.contains("JSON") || div_upper.contains("JSON_") {
        return Subsystem::Extension;
    }
    if div_upper.contains("FTS") || div_upper.contains("MATCH") {
        return Subsystem::Extension;
    }
    if div_upper.contains("RTREE") || div_upper.contains("GEOPOLY") {
        return Subsystem::Extension;
    }

    // Window functions -> VDBE (check before general functions since
    // window functions like ROW_NUMBER also appear in the function list).
    if div_upper.contains("OVER(") || div_upper.contains("OVER (") {
        return Subsystem::Vdbe;
    }

    // Function signals.
    let function_keywords = [
        "ABS(",
        "AVG(",
        "COUNT(",
        "GROUP_CONCAT(",
        "LENGTH(",
        "LOWER(",
        "MAX(",
        "MIN(",
        "SUM(",
        "TOTAL(",
        "TYPEOF(",
        "UNICODE(",
        "ZEROBLOB(",
        "SUBSTR(",
        "REPLACE(",
        "TRIM(",
        "ROUND(",
        "RANDOM(",
        "INSTR(",
        "COALESCE(",
        "IFNULL(",
        "NULLIF(",
        "IIF(",
        "LIKELIHOOD(",
        "LIKELY(",
        "UNLIKELY(",
        "ROW_NUMBER(",
        "RANK(",
        "DENSE_RANK(",
        "NTILE(",
        "LAG(",
        "LEAD(",
        "FIRST_VALUE(",
        "LAST_VALUE(",
        "NTH_VALUE(",
        "CUME_DIST(",
        "PERCENT_RANK(",
    ];
    if function_keywords.iter().any(|kw| div_upper.contains(kw)) {
        return Subsystem::Functions;
    }

    // PRAGMA signals.
    if div_upper.contains("PRAGMA") {
        return Subsystem::Pragma;
    }

    // Type system signals.
    if div_upper.contains("CAST(") || div_upper.contains("TYPEOF(") {
        return Subsystem::TypeSystem;
    }
    if div_upper.contains("COLLATE") {
        return Subsystem::TypeSystem;
    }

    // Storage/WAL signals.
    if all_upper.contains("JOURNAL_MODE") || all_upper.contains("WAL") {
        return Subsystem::Wal;
    }
    if all_upper.contains("VACUUM") || all_upper.contains("INTEGRITY_CHECK") {
        return Subsystem::Storage;
    }

    // Planner signals (index hints, EXPLAIN).
    if div_upper.contains("EXPLAIN") || div_upper.contains("INDEXED BY") {
        return Subsystem::Planner;
    }

    // Complex query structure -> likely planner/VDBE.
    if div_upper.contains("JOIN")
        || div_upper.contains("UNION")
        || div_upper.contains("INTERSECT")
        || div_upper.contains("EXCEPT")
        || div_upper.contains("WITH RECURSIVE")
    {
        return Subsystem::Planner;
    }

    // Simple DML -> VDBE.
    if div_upper.contains("INSERT") || div_upper.contains("UPDATE") || div_upper.contains("DELETE")
    {
        return Subsystem::Vdbe;
    }

    // Simple SELECT -> VDBE.
    if div_upper.contains("SELECT") {
        return Subsystem::Vdbe;
    }

    Subsystem::Unknown
}

// ===========================================================================
// Delta Debugging Minimizer
// ===========================================================================

/// Test function signature for the minimizer.
///
/// Given a schema and workload, returns `true` if the divergence is
/// still reproducible.
pub type ReproducibilityTest = dyn Fn(&[String], &[String]) -> Option<Vec<StatementDivergence>>;

/// Minimize a differential failure to its smallest reproducing workload.
///
/// The `test_fn` is called with `(schema, candidate_workload)` and should
/// return `Some(divergences)` if the failure reproduces, or `None` if it
/// does not.
///
/// Returns `None` if the failure cannot be reproduced even with the full
/// workload, or if the workload is empty.
pub fn minimize_workload(
    schema: &[String],
    workload: &[String],
    config: &MinimizerConfig,
    test_fn: &ReproducibilityTest,
) -> Option<MinimalReproduction> {
    if workload.is_empty() {
        return None;
    }

    // Verify the full workload reproduces.
    let original_divergences = test_fn(schema, workload)?;

    if workload.len() > config.max_workload_size {
        // Too large for delta debugging; just extract signature.
        return Some(build_reproduction(
            schema,
            workload,
            workload,
            &original_divergences,
        ));
    }

    let mut current = workload.to_vec();
    let mut iterations = 0;

    // Phase 1: Binary partition reduction.
    let mut granularity = 2;
    while granularity <= current.len() && iterations < config.max_iterations {
        let chunk_size = current.len().div_ceil(granularity);
        let mut reduced = false;

        for chunk_idx in 0..granularity {
            let start = chunk_idx * chunk_size;
            let end = (start + chunk_size).min(current.len());

            // Try removing this chunk.
            let mut candidate: Vec<String> = Vec::with_capacity(current.len() - (end - start));
            candidate.extend_from_slice(&current[..start]);
            candidate.extend_from_slice(&current[end..]);

            if candidate.is_empty() {
                continue;
            }

            iterations += 1;
            if let Some(_divs) = test_fn(schema, &candidate) {
                current = candidate;
                reduced = true;
                break;
            }
        }

        if reduced {
            // Reset granularity to try larger chunks again.
            granularity = 2;
        } else {
            granularity *= 2;
        }
    }

    // Phase 2: 1-minimal reduction (try removing each statement).
    if config.one_minimal {
        let mut i = 0;
        while i < current.len() && iterations < config.max_iterations {
            let mut candidate = current.clone();
            candidate.remove(i);

            if candidate.is_empty() {
                i += 1;
                continue;
            }

            iterations += 1;
            if test_fn(schema, &candidate).is_some() {
                current = candidate;
                // Don't increment i — the next statement moved into position i.
            } else {
                i += 1;
            }
        }
    }

    // Final verification.
    let final_divergences = test_fn(schema, &current)?;

    Some(build_reproduction(
        schema,
        workload,
        &current,
        &final_divergences,
    ))
}

/// Build a `MinimalReproduction` from the minimized workload.
fn build_reproduction(
    schema: &[String],
    original_workload: &[String],
    minimal_workload: &[String],
    divergences: &[StatementDivergence],
) -> MinimalReproduction {
    let subsystem = attribute_subsystem(divergences, schema, minimal_workload);

    let classification = if divergences.is_empty() {
        MismatchClassification::TrueDivergence {
            description: "empty divergence list".to_owned(),
        }
    } else {
        // Use the first divergence for classification.
        MismatchClassification::TrueDivergence {
            description: format!(
                "statement {} diverged: {}",
                divergences[0].index, divergences[0].sql
            ),
        }
    };

    let signature = MismatchSignature::compute(
        schema,
        minimal_workload,
        &classification,
        subsystem,
        divergences.first(),
    );

    #[allow(clippy::cast_precision_loss)]
    let reduction_ratio = if original_workload.is_empty() {
        0.0
    } else {
        1.0 - (minimal_workload.len() as f64 / original_workload.len() as f64)
    };

    let first_divergence_index = divergences.first().map(|d| d.index);

    let repro_command = format!(
        "# Minimal reproduction ({} statements from original {}):\n{}",
        minimal_workload.len(),
        original_workload.len(),
        minimal_workload.join("\n")
    );

    MinimalReproduction {
        schema_version: MINIMIZER_SCHEMA_VERSION,
        signature,
        original_seed: 0, // Caller should set this.
        schema: schema.to_vec(),
        minimal_workload: minimal_workload.to_vec(),
        original_workload_size: original_workload.len(),
        reduction_ratio,
        first_divergence_index,
        divergences: divergences.to_vec(),
        repro_command,
    }
}

// ===========================================================================
// Deduplication
// ===========================================================================

/// A collection of minimized reproductions, deduplicated by signature.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DeduplicatedFailures {
    /// Unique failures keyed by signature hash.
    pub unique_failures: Vec<MinimalReproduction>,
    /// Total failures before deduplication.
    pub total_before_dedup: usize,
    /// Duplicate count per signature hash.
    pub duplicate_counts: Vec<(String, usize)>,
}

/// Deduplicate a collection of minimized reproductions by signature.
#[must_use]
pub fn deduplicate(reproductions: &[MinimalReproduction]) -> DeduplicatedFailures {
    let mut seen: BTreeSet<String> = BTreeSet::new();
    let mut unique = Vec::new();
    let mut counts: std::collections::BTreeMap<String, usize> = std::collections::BTreeMap::new();

    for repro in reproductions {
        *counts.entry(repro.signature.hash.clone()).or_insert(0) += 1;
        if seen.insert(repro.signature.hash.clone()) {
            unique.push(repro.clone());
        }
    }

    // Sort by triage priority (most actionable first).
    unique.sort_by(|a, b| {
        a.signature
            .classification
            .triage_priority()
            .cmp(&b.signature.classification.triage_priority())
            .then_with(|| a.signature.hash.cmp(&b.signature.hash))
    });

    let duplicate_counts: Vec<(String, usize)> =
        counts.into_iter().filter(|(_, count)| *count > 1).collect();

    DeduplicatedFailures {
        unique_failures: unique,
        total_before_dedup: reproductions.len(),
        duplicate_counts,
    }
}

// ===========================================================================
// Helpers
// ===========================================================================

/// Encode bytes as hex, truncated to `max_chars` characters.
fn hex_encode_truncated(bytes: &[u8], max_chars: usize) -> String {
    let mut s = String::with_capacity(max_chars);
    for byte in bytes {
        if s.len() >= max_chars {
            break;
        }
        let _ = write!(s, "{byte:02x}");
    }
    s.truncate(max_chars);
    s
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut output = String::with_capacity(64);
    for byte in digest {
        write!(output, "{byte:02x}").expect("write to String");
    }
    output
}

fn is_sha256_hex_64(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::differential_v2::{NormalizedValue, StmtOutcome};
    use crate::typed_sql_generator::{
        BinaryOp, ColumnAffinity, ColumnSpec, FromItem, Join, JoinKind, OrderDirection, OrderTerm,
        SelectItem,
    };
    use proptest::prelude::*;

    fn identifier(value: &str) -> Identifier {
        Identifier::new(value).expect("valid test identifier")
    }

    fn generated_statement(
        ordinal: u32,
        role: StatementRole,
        ast: GeneratedAstStatement,
    ) -> GeneratedStatement {
        GeneratedStatement {
            ordinal,
            seed_path: format!("test/{ordinal}"),
            derived_seed: u64::from(ordinal) + 1,
            role,
            construct: match &ast {
                GeneratedAstStatement::CreateTable { .. } => {
                    crate::typed_sql_generator::Construct::CreateTable
                }
                GeneratedAstStatement::CreateIndex { .. } => {
                    crate::typed_sql_generator::Construct::CreateIndex
                }
                GeneratedAstStatement::Insert { .. } => {
                    crate::typed_sql_generator::Construct::Insert
                }
                GeneratedAstStatement::Update { .. } => {
                    crate::typed_sql_generator::Construct::Update
                }
                GeneratedAstStatement::Delete { .. } => {
                    crate::typed_sql_generator::Construct::Delete
                }
                GeneratedAstStatement::Select { .. } => {
                    crate::typed_sql_generator::Construct::Select
                }
                GeneratedAstStatement::Transaction { .. } => {
                    crate::typed_sql_generator::Construct::Transaction
                }
            },
            profile_feature_id: Some(format!("feature.{ordinal}")),
            sql: ast.to_sql(),
            ast,
        }
    }

    fn structured_fixture() -> Vec<GeneratedStatement> {
        let columns = vec![
            ColumnSpec {
                name: identifier("id"),
                affinity: ColumnAffinity::Integer,
                primary_key: true,
                not_null: true,
            },
            ColumnSpec {
                name: identifier("payload"),
                affinity: ColumnAffinity::Text,
                primary_key: false,
                not_null: false,
            },
        ];
        vec![
            generated_statement(
                0,
                StatementRole::Setup,
                GeneratedAstStatement::CreateTable {
                    table: identifier("left_table"),
                    columns: columns.clone(),
                },
            ),
            generated_statement(
                1,
                StatementRole::Setup,
                GeneratedAstStatement::CreateTable {
                    table: identifier("right_table"),
                    columns: columns.clone(),
                },
            ),
            generated_statement(
                2,
                StatementRole::Setup,
                GeneratedAstStatement::CreateIndex {
                    index: identifier("left_index"),
                    table: identifier("left_table"),
                    columns: vec![identifier("id"), identifier("payload")],
                    unique: false,
                },
            ),
            generated_statement(
                3,
                StatementRole::Subject,
                GeneratedAstStatement::Insert {
                    table: identifier("left_table"),
                    columns: vec![identifier("id"), identifier("payload")],
                    rows: vec![
                        vec![SqlValue::Integer(42), SqlValue::Text("abcdef".to_owned())],
                        vec![SqlValue::Integer(84), SqlValue::Text("ghijkl".to_owned())],
                    ],
                },
            ),
            generated_statement(
                4,
                StatementRole::Subject,
                GeneratedAstStatement::Transaction {
                    statement: TransactionStatement::Begin,
                },
            ),
            generated_statement(
                5,
                StatementRole::Subject,
                GeneratedAstStatement::Select {
                    select: Select {
                        distinct: true,
                        projection: vec![
                            SelectItem {
                                expr: Expr::Column {
                                    table: Some(identifier("lhs")),
                                    column: identifier("id"),
                                },
                                alias: None,
                            },
                            SelectItem {
                                expr: Expr::Binary {
                                    left: Box::new(Expr::Value {
                                        value: SqlValue::Integer(42),
                                    }),
                                    op: BinaryOp::Add,
                                    right: Box::new(Expr::Value {
                                        value: SqlValue::Integer(7),
                                    }),
                                },
                                alias: Some(identifier("answer")),
                            },
                        ],
                        from: Some(FromItem {
                            table: identifier("left_table"),
                            alias: Some(identifier("lhs")),
                        }),
                        joins: vec![Join {
                            kind: JoinKind::Inner,
                            table: identifier("right_table"),
                            alias: Some(identifier("rhs")),
                            on: Expr::Binary {
                                left: Box::new(Expr::Column {
                                    table: Some(identifier("lhs")),
                                    column: identifier("id"),
                                }),
                                op: BinaryOp::Equal,
                                right: Box::new(Expr::Column {
                                    table: Some(identifier("rhs")),
                                    column: identifier("id"),
                                }),
                            },
                        }],
                        predicate: Some(Expr::Binary {
                            left: Box::new(Expr::Column {
                                table: Some(identifier("lhs")),
                                column: identifier("id"),
                            }),
                            op: BinaryOp::Greater,
                            right: Box::new(Expr::Value {
                                value: SqlValue::Integer(1),
                            }),
                        }),
                        group_by: Vec::new(),
                        having: None,
                        compound: None,
                        order_by: vec![
                            OrderTerm {
                                expr: Expr::Column {
                                    table: Some(identifier("lhs")),
                                    column: identifier("id"),
                                },
                                direction: OrderDirection::Asc,
                            },
                            OrderTerm {
                                expr: Expr::Value {
                                    value: SqlValue::Integer(2),
                                },
                                direction: OrderDirection::Desc,
                            },
                        ],
                        limit: Some(10),
                    },
                },
            ),
            generated_statement(
                6,
                StatementRole::Subject,
                GeneratedAstStatement::Transaction {
                    statement: TransactionStatement::Commit,
                },
            ),
        ]
    }

    fn make_divergence(index: usize, sql: &str) -> StatementDivergence {
        StatementDivergence {
            index,
            sql: sql.to_owned(),
            csqlite_outcome: StmtOutcome::Rows(vec![vec![NormalizedValue::Integer(1)]]),
            fsqlite_outcome: StmtOutcome::Rows(vec![vec![NormalizedValue::Integer(2)]]),
        }
    }

    fn stable_observation() -> TypedReductionObservation {
        TypedReductionObservation {
            mismatch_signature: "mismatch-rows-1-vs-2".to_owned(),
            required_lanes: vec![ExecutionLane::SqlResultOnly],
        }
    }

    #[test]
    fn typed_candidate_inventory_covers_every_sql_reducer_family() {
        let candidates = typed_reduction_candidates(&structured_fixture());
        let kinds = candidates
            .iter()
            .map(|candidate| candidate.kind)
            .collect::<BTreeSet<_>>();
        assert_eq!(
            kinds,
            BTreeSet::from([
                TypedReductionKind::Statement,
                TypedReductionKind::Transaction,
                TypedReductionKind::Clause,
                TypedReductionKind::Join,
                TypedReductionKind::Projection,
                TypedReductionKind::Predicate,
                TypedReductionKind::OrderTerm,
                TypedReductionKind::Index,
                TypedReductionKind::Expression,
                TypedReductionKind::SchemaTable,
                TypedReductionKind::SchemaColumn,
                TypedReductionKind::InsertRow,
                TypedReductionKind::Value,
            ])
        );
    }

    #[test]
    fn typed_reduction_is_deterministic_and_round_trips() {
        let fixture = structured_fixture();
        let verifier = |candidate: &[GeneratedStatement]| {
            if candidate
                .iter()
                .any(|statement| matches!(statement.ast, GeneratedAstStatement::Select { .. }))
            {
                Ok(stable_observation())
            } else {
                Err("SELECT witness was removed".to_owned())
            }
        };
        let first =
            minimize_typed_statements(&fixture, &TypedReductionConfig::default(), &verifier)
                .expect("reduce fixture");
        let second =
            minimize_typed_statements(&fixture, &TypedReductionConfig::default(), &verifier)
                .expect("repeat reduction");
        assert_eq!(first, second);
        assert!(first.status.is_complete());
        assert!(first.stats.minimized_statements < first.stats.original_statements);
        assert!(first.stats.minimized_bytes < first.stats.original_bytes);
        assert_eq!(
            TypedReductionResult::from_json_strict(&first.to_json().unwrap()).unwrap(),
            first
        );
    }

    #[test]
    fn typed_reduction_rejects_signature_lane_and_dependency_drift() {
        let fixture = structured_fixture();
        let verifier = |candidate: &[GeneratedStatement]| {
            let has_left_table = candidate.iter().any(|statement| {
                matches!(
                    &statement.ast,
                    GeneratedAstStatement::CreateTable { table, .. }
                        if table.as_str() == "left_table"
                )
            });
            if !has_left_table {
                return Err("setup dependency missing: left_table".to_owned());
            }
            let has_42 = candidate
                .iter()
                .any(|statement| statement.sql.contains("42"));
            let lane = if candidate
                .iter()
                .any(|statement| statement.sql.contains("LIMIT"))
            {
                ExecutionLane::SqlResultOnly
            } else {
                ExecutionLane::PlannerRequired
            };
            Ok(TypedReductionObservation {
                mismatch_signature: if has_42 {
                    "signature-42".to_owned()
                } else {
                    "signature-drift".to_owned()
                },
                required_lanes: vec![lane],
            })
        };
        let result = minimize_typed_statements(
            &fixture,
            &TypedReductionConfig {
                max_attempts: 200,
                cancel_after_attempts: None,
            },
            &verifier,
        )
        .expect("reduction returns guarded result");
        assert!(result.trace.iter().any(|attempt| {
            !attempt.accepted && attempt.rationale.contains("signature drifted")
        }));
        assert!(result.trace.iter().any(|attempt| {
            !attempt.accepted && attempt.rationale.contains("lane identity drifted")
        }));
        assert!(result.trace.iter().any(|attempt| {
            !attempt.accepted && attempt.rationale.contains("setup dependency missing")
        }));
        assert_eq!(result.observation.mismatch_signature, "signature-42");
        assert_eq!(
            result.observation.required_lanes,
            [ExecutionLane::SqlResultOnly]
        );
    }

    #[test]
    fn typed_reduction_budget_and_cancellation_return_valid_partial_results() {
        let fixture = structured_fixture();
        let verifier = |_candidate: &[GeneratedStatement]| Ok(stable_observation());
        let exhausted = minimize_typed_statements(
            &fixture,
            &TypedReductionConfig {
                max_attempts: 0,
                cancel_after_attempts: None,
            },
            &verifier,
        )
        .expect("budget exhaustion returns partial result");
        assert_eq!(exhausted.status, TypedReductionStatus::BudgetExhausted);
        assert_eq!(exhausted.minimized_statements, fixture);
        exhausted.validate().unwrap();

        let cancelled = minimize_typed_statements(
            &fixture,
            &TypedReductionConfig {
                max_attempts: usize::MAX,
                cancel_after_attempts: Some(0),
            },
            &verifier,
        )
        .expect("cancellation returns partial result");
        assert_eq!(cancelled.status, TypedReductionStatus::Cancelled);
        assert_eq!(cancelled.minimized_statements, fixture);
        cancelled.validate().unwrap();
    }

    #[test]
    fn typed_reduction_rejects_empty_and_corrupt_artifacts() {
        let verifier = |_candidate: &[GeneratedStatement]| Ok(stable_observation());
        assert!(
            minimize_typed_statements(&[], &TypedReductionConfig::default(), &verifier).is_err()
        );
        let fixture = structured_fixture();
        let result = minimize_typed_statements(
            &fixture,
            &TypedReductionConfig {
                max_attempts: 0,
                cancel_after_attempts: None,
            },
            &verifier,
        )
        .unwrap();
        let truncated = &result.to_json().unwrap()[..32];
        assert!(TypedReductionResult::from_json_strict(truncated).is_err());
        let mut corrupted = result;
        corrupted.content_hash = "0".repeat(64);
        assert!(corrupted.validate().is_err());

        let mut malformed = minimize_typed_statements(
            &fixture,
            &TypedReductionConfig {
                max_attempts: 1,
                cancel_after_attempts: None,
            },
            &verifier,
        )
        .unwrap();
        malformed.trace[0].ordinal = 99;
        malformed.content_hash = malformed.deterministic_hash();
        assert!(malformed.validate().is_err());

        let mut unknown = serde_json::to_value(
            minimize_typed_statements(
                &fixture,
                &TypedReductionConfig {
                    max_attempts: 0,
                    cancel_after_attempts: None,
                },
                &verifier,
            )
            .unwrap(),
        )
        .unwrap();
        unknown
            .as_object_mut()
            .unwrap()
            .insert("unexpected".to_owned(), serde_json::Value::Bool(true));
        assert!(
            TypedReductionResult::from_json_strict(&serde_json::to_string(&unknown).unwrap())
                .is_err()
        );
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(24))]

        #[test]
        fn typed_reduction_is_stable_at_arbitrary_budget_boundaries(
            max_attempts in 0_usize..=96,
            cancel_after_attempts in proptest::option::of(0_usize..=96),
        ) {
            let fixture = structured_fixture();
            let verifier = |_candidate: &[GeneratedStatement]| Ok(stable_observation());
            let config = TypedReductionConfig {
                max_attempts,
                cancel_after_attempts,
            };
            let first = minimize_typed_statements(&fixture, &config, &verifier).unwrap();
            let second = minimize_typed_statements(&fixture, &config, &verifier).unwrap();
            prop_assert_eq!(&first, &second);
            prop_assert!(first.stats.attempts <= max_attempts);
            if let Some(cancel_after) = cancel_after_attempts {
                prop_assert!(first.stats.attempts <= cancel_after);
            }
            prop_assert!(first.validate().is_ok());
        }
    }

    // --- Signature ---

    #[test]
    fn test_signature_deterministic() {
        let schema = vec!["CREATE TABLE t(a INTEGER);".to_owned()];
        let workload = vec!["SELECT a FROM t;".to_owned()];
        let classification = MismatchClassification::TrueDivergence {
            description: "test".to_owned(),
        };

        let sig1 =
            MismatchSignature::compute(&schema, &workload, &classification, Subsystem::Vdbe, None);
        let sig2 =
            MismatchSignature::compute(&schema, &workload, &classification, Subsystem::Vdbe, None);

        assert_eq!(sig1.hash, sig2.hash);
    }

    #[test]
    fn test_signature_differs_on_classification() {
        let schema = vec!["CREATE TABLE t(a INTEGER);".to_owned()];
        let workload = vec!["SELECT a FROM t;".to_owned()];

        let sig1 = MismatchSignature::compute(
            &schema,
            &workload,
            &MismatchClassification::TrueDivergence {
                description: "test".to_owned(),
            },
            Subsystem::Vdbe,
            None,
        );
        let sig2 = MismatchSignature::compute(
            &schema,
            &workload,
            &MismatchClassification::OrderDependentDifference,
            Subsystem::Vdbe,
            None,
        );

        assert_ne!(sig1.hash, sig2.hash);
    }

    #[test]
    fn test_signature_differs_on_subsystem() {
        let schema = vec!["CREATE TABLE t(a INTEGER);".to_owned()];
        let workload = vec!["SELECT a FROM t;".to_owned()];
        let classification = MismatchClassification::TrueDivergence {
            description: "test".to_owned(),
        };

        let sig1 =
            MismatchSignature::compute(&schema, &workload, &classification, Subsystem::Vdbe, None);
        let sig2 = MismatchSignature::compute(
            &schema,
            &workload,
            &classification,
            Subsystem::Parser,
            None,
        );

        assert_ne!(sig1.hash, sig2.hash);
    }

    #[test]
    fn test_signature_display() {
        let sig = MismatchSignature {
            hash: "abcdef0123456789".to_owned(),
            classification: MismatchClassification::TrueDivergence {
                description: "test".to_owned(),
            },
            subsystem: Subsystem::Vdbe,
            minimal_statement_count: 3,
            first_diverging_sql: "SELECT 1".to_owned(),
        };

        let display = sig.to_string();
        assert!(display.contains("SIG-"));
        assert!(display.contains("vdbe"));
        assert!(display.contains("3stmts"));
    }

    // --- Subsystem attribution ---

    #[test]
    fn test_attribute_json_to_extension() {
        let divs = vec![make_divergence(
            0,
            "SELECT json_extract(data, '$.a') FROM t",
        )];
        let subsystem = attribute_subsystem(
            &divs,
            &[],
            &["SELECT json_extract(data, '$.a') FROM t".to_owned()],
        );
        assert_eq!(subsystem, Subsystem::Extension);
    }

    #[test]
    fn test_attribute_pragma_to_pragma() {
        let divs = vec![make_divergence(0, "PRAGMA table_info(t)")];
        let subsystem = attribute_subsystem(&divs, &[], &["PRAGMA table_info(t)".to_owned()]);
        assert_eq!(subsystem, Subsystem::Pragma);
    }

    #[test]
    fn test_attribute_window_to_vdbe() {
        let divs = vec![make_divergence(
            0,
            "SELECT row_number() OVER (ORDER BY a) FROM t",
        )];
        let subsystem = attribute_subsystem(
            &divs,
            &[],
            &["SELECT row_number() OVER (ORDER BY a) FROM t".to_owned()],
        );
        assert_eq!(subsystem, Subsystem::Vdbe);
    }

    #[test]
    fn test_attribute_join_to_planner() {
        let divs = vec![make_divergence(
            0,
            "SELECT * FROM t1 JOIN t2 ON t1.a = t2.b",
        )];
        let subsystem = attribute_subsystem(
            &divs,
            &[],
            &["SELECT * FROM t1 JOIN t2 ON t1.a = t2.b".to_owned()],
        );
        assert_eq!(subsystem, Subsystem::Planner);
    }

    #[test]
    fn test_attribute_function_to_functions() {
        let divs = vec![make_divergence(0, "SELECT ABS(-5)")];
        let subsystem = attribute_subsystem(&divs, &[], &["SELECT ABS(-5)".to_owned()]);
        assert_eq!(subsystem, Subsystem::Functions);
    }

    #[test]
    fn test_attribute_cast_to_type_system() {
        let divs = vec![make_divergence(0, "SELECT CAST(42 AS TEXT)")];
        let subsystem = attribute_subsystem(&divs, &[], &["SELECT CAST(42 AS TEXT)".to_owned()]);
        assert_eq!(subsystem, Subsystem::TypeSystem);
    }

    // --- Delta debugging ---

    #[test]
    fn test_minimize_single_statement() {
        let schema = vec!["CREATE TABLE t(a INTEGER);".to_owned()];
        let workload = vec!["SELECT a FROM t;".to_owned()];
        let config = MinimizerConfig::default();

        let result = minimize_workload(&schema, &workload, &config, &|_s, w| {
            if w.iter().any(|s| s.contains("SELECT")) {
                Some(vec![make_divergence(0, "SELECT a FROM t;")])
            } else {
                None
            }
        });

        let repro = result.expect("should produce reproduction");
        assert_eq!(repro.minimal_workload.len(), 1);
        assert!(repro.minimal_workload[0].contains("SELECT"));
    }

    #[test]
    fn test_minimize_removes_non_contributing() {
        let schema = vec!["CREATE TABLE t(a INTEGER);".to_owned()];
        let workload = vec![
            "INSERT INTO t VALUES(1);".to_owned(),
            "INSERT INTO t VALUES(2);".to_owned(),
            "INSERT INTO t VALUES(3);".to_owned(),
            "SELECT a FROM t;".to_owned(), // Only this diverges
        ];
        let config = MinimizerConfig::default();

        let result = minimize_workload(&schema, &workload, &config, &|_s, w| {
            // Only diverge if the SELECT is present
            if w.iter().any(|s| s.contains("SELECT")) {
                Some(vec![make_divergence(w.len() - 1, "SELECT a FROM t;")])
            } else {
                None
            }
        });

        let repro = result.expect("should produce reproduction");
        assert!(
            repro.minimal_workload.len() < workload.len(),
            "should reduce workload from {} statements",
            workload.len()
        );
        assert!(repro.reduction_ratio > 0.0);
    }

    #[test]
    fn test_minimize_preserves_required_statements() {
        let schema = vec!["CREATE TABLE t(a INTEGER);".to_owned()];
        let workload = vec![
            "INSERT INTO t VALUES(1);".to_owned(),
            "INSERT INTO t VALUES(2);".to_owned(),
            "SELECT a FROM t WHERE a = 2;".to_owned(),
        ];
        let config = MinimizerConfig::default();

        let result = minimize_workload(&schema, &workload, &config, &|_s, w| {
            // Need both the INSERT(2) and the SELECT to diverge
            let has_insert_2 = w.iter().any(|s| s.contains("VALUES(2)"));
            let has_select = w.iter().any(|s| s.contains("SELECT"));
            if has_insert_2 && has_select {
                Some(vec![make_divergence(2, "SELECT a FROM t WHERE a = 2;")])
            } else {
                None
            }
        });

        let repro = result.expect("should produce reproduction");
        assert!(repro.minimal_workload.len() >= 2);
        assert!(
            repro
                .minimal_workload
                .iter()
                .any(|s| s.contains("VALUES(2)"))
        );
        assert!(repro.minimal_workload.iter().any(|s| s.contains("SELECT")));
    }

    #[test]
    fn test_minimize_empty_workload() {
        let schema = vec!["CREATE TABLE t(a INTEGER);".to_owned()];
        let workload: Vec<String> = vec![];
        let config = MinimizerConfig::default();

        let result = minimize_workload(&schema, &workload, &config, &|_, _| None);
        assert!(result.is_none());
    }

    #[test]
    fn test_minimize_no_reproduction() {
        let schema = vec!["CREATE TABLE t(a INTEGER);".to_owned()];
        let workload = vec!["SELECT 1;".to_owned()];
        let config = MinimizerConfig::default();

        let result = minimize_workload(&schema, &workload, &config, &|_, _| None);
        assert!(result.is_none());
    }

    // --- Deduplication ---

    #[test]
    fn test_deduplicate_identical_signatures() {
        let schema = vec!["CREATE TABLE t(a);".to_owned()];
        let workload = vec!["SELECT a FROM t;".to_owned()];
        let divs = vec![make_divergence(0, "SELECT a FROM t;")];

        let repro1 = build_reproduction(&schema, &workload, &workload, &divs);
        let repro2 = build_reproduction(&schema, &workload, &workload, &divs);

        let deduped = deduplicate(&[repro1, repro2]);
        assert_eq!(deduped.unique_failures.len(), 1);
        assert_eq!(deduped.total_before_dedup, 2);
        assert_eq!(deduped.duplicate_counts.len(), 1);
        assert_eq!(deduped.duplicate_counts[0].1, 2);
    }

    #[test]
    fn test_deduplicate_different_signatures() {
        let schema = vec!["CREATE TABLE t(a);".to_owned()];
        let workload1 = vec!["SELECT a FROM t;".to_owned()];
        let workload2 = vec!["SELECT a + 1 FROM t;".to_owned()];
        let divs1 = vec![make_divergence(0, "SELECT a FROM t;")];
        let divs2 = vec![make_divergence(0, "SELECT a + 1 FROM t;")];

        let repro1 = build_reproduction(&schema, &workload1, &workload1, &divs1);
        let repro2 = build_reproduction(&schema, &workload2, &workload2, &divs2);

        let deduped = deduplicate(&[repro1, repro2]);
        assert_eq!(deduped.unique_failures.len(), 2);
        assert_eq!(deduped.total_before_dedup, 2);
        assert!(deduped.duplicate_counts.is_empty());
    }

    // --- JSON round-trip ---

    #[test]
    fn test_reproduction_json_roundtrip() {
        let schema = vec!["CREATE TABLE t(a);".to_owned()];
        let workload = vec!["SELECT a FROM t;".to_owned()];
        let divs = vec![make_divergence(0, "SELECT a FROM t;")];

        let repro = build_reproduction(&schema, &workload, &workload, &divs);
        let json = repro.to_json().expect("serialize");
        let restored = MinimalReproduction::from_json(&json).expect("deserialize");

        assert_eq!(restored.signature.hash, repro.signature.hash);
        assert_eq!(restored.minimal_workload, repro.minimal_workload);
    }

    // --- Config defaults ---

    #[test]
    fn test_minimizer_config_defaults() {
        let config = MinimizerConfig::default();
        assert_eq!(config.max_iterations, 100);
        assert!(config.one_minimal);
        assert_eq!(config.max_workload_size, 1000);
    }
}
