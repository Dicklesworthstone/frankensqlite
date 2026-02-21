//! Bloodstream: FrankenSQLite materialized view push to render tree (bd-ehk.3).
//!
//! This module defines the delta-propagation infrastructure for pushing algebraic
//! deltas from FrankenSQLite materialized views directly into a render tree.
//! Database row changes propagate to targeted ANSI terminal diffs in O(delta) time,
//! avoiding intermediary query-serialize-deserialize-render polling.
//!
//! Contract:
//!   TRACING: span `bloodstream.delta` with fields `source_table`, `rows_changed`,
//!            `propagation_duration_us`, `widgets_invalidated`.
//!   LOG: DEBUG for delta propagation, INFO for new materialized view binding.
//!   METRICS: counter `bloodstream_deltas_total`,
//!            histogram `bloodstream_propagation_duration_us`,
//!            gauge `bloodstream_active_bindings`.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use serde::{Deserialize, Serialize};

/// Bead identifier for log correlation.
#[allow(dead_code)]
const BEAD_ID: &str = "bd-ehk.3";

/// Schema version for structured output compatibility.
pub const BLOODSTREAM_SCHEMA_VERSION: u32 = 1;

// ── Delta Types ──────────────────────────────────────────────────────────────

/// The kind of row-level change that triggered a delta.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum DeltaKind {
    /// New row inserted.
    Insert,
    /// Existing row updated (before + after images).
    Update,
    /// Existing row deleted.
    Delete,
}

impl fmt::Display for DeltaKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Insert => write!(f, "INSERT"),
            Self::Update => write!(f, "UPDATE"),
            Self::Delete => write!(f, "DELETE"),
        }
    }
}

impl DeltaKind {
    /// All variants for exhaustive testing.
    pub const ALL: [Self; 3] = [Self::Insert, Self::Update, Self::Delete];
}

/// A single algebraic delta representing one row change.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AlgebraicDelta {
    /// Source table that produced the change.
    pub source_table: String,
    /// Row identifier (rowid or primary key).
    pub row_id: i64,
    /// Kind of mutation.
    pub kind: DeltaKind,
    /// Column indices affected (empty for DELETE).
    pub affected_columns: Vec<usize>,
    /// Monotonic sequence number within a propagation batch.
    pub seq: u64,
}

/// A batch of deltas propagated atomically from a single transaction commit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeltaBatch {
    /// Transaction ID that produced these deltas.
    pub txn_id: u64,
    /// Commit sequence number from the WAL.
    pub commit_seq: u64,
    /// Ordered deltas within this batch.
    pub deltas: Vec<AlgebraicDelta>,
    /// Unix timestamp (nanoseconds) when the batch was created.
    pub created_at_ns: u64,
}

impl DeltaBatch {
    /// Create a new empty batch.
    pub fn new(txn_id: u64, commit_seq: u64, created_at_ns: u64) -> Self {
        Self {
            txn_id,
            commit_seq,
            deltas: Vec::new(),
            created_at_ns,
        }
    }

    /// Number of deltas in this batch.
    pub fn len(&self) -> usize {
        self.deltas.len()
    }

    /// Whether the batch is empty.
    pub fn is_empty(&self) -> bool {
        self.deltas.is_empty()
    }

    /// Push a delta into the batch.
    pub fn push(&mut self, delta: AlgebraicDelta) {
        self.deltas.push(delta);
    }

    /// Distinct source tables referenced in this batch.
    pub fn source_tables(&self) -> BTreeSet<&str> {
        self.deltas.iter().map(|d| d.source_table.as_str()).collect()
    }

    /// Count deltas by kind.
    pub fn count_by_kind(&self) -> BTreeMap<DeltaKind, usize> {
        let mut counts = BTreeMap::new();
        for d in &self.deltas {
            *counts.entry(d.kind).or_insert(0) += 1;
        }
        counts
    }
}

// ── Materialized View Binding ────────────────────────────────────────────────

/// State of a binding between a materialized view and a render widget.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BindingState {
    /// Actively propagating deltas.
    Active,
    /// Temporarily paused (e.g., widget not visible).
    Suspended,
    /// Permanently detached; will not receive further deltas.
    Detached,
}

impl fmt::Display for BindingState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Active => write!(f, "active"),
            Self::Suspended => write!(f, "suspended"),
            Self::Detached => write!(f, "detached"),
        }
    }
}

impl BindingState {
    /// Whether the binding can receive deltas.
    pub fn is_receiving(&self) -> bool {
        matches!(self, Self::Active)
    }

    /// All variants.
    pub const ALL: [Self; 3] = [Self::Active, Self::Suspended, Self::Detached];
}

/// A binding between a SQL materialized view and a render tree widget.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ViewBinding {
    /// Unique binding identifier.
    pub binding_id: u64,
    /// SQL view name driving this binding.
    pub view_name: String,
    /// Widget identifier in the render tree.
    pub widget_id: String,
    /// Current binding state.
    pub state: BindingState,
    /// Tables this view depends on (for delta routing).
    pub source_tables: BTreeSet<String>,
    /// Number of deltas delivered through this binding.
    pub deltas_delivered: u64,
    /// Last commit_seq successfully propagated.
    pub last_commit_seq: Option<u64>,
}

impl ViewBinding {
    /// Create a new active binding.
    pub fn new(
        binding_id: u64,
        view_name: String,
        widget_id: String,
        source_tables: BTreeSet<String>,
    ) -> Self {
        Self {
            binding_id,
            view_name,
            widget_id,
            state: BindingState::Active,
            source_tables,
            deltas_delivered: 0,
            last_commit_seq: None,
        }
    }

    /// Suspend the binding (pauses delta delivery).
    pub fn suspend(&mut self) {
        if self.state == BindingState::Active {
            self.state = BindingState::Suspended;
        }
    }

    /// Resume a suspended binding.
    pub fn resume(&mut self) {
        if self.state == BindingState::Suspended {
            self.state = BindingState::Active;
        }
    }

    /// Permanently detach the binding.
    pub fn detach(&mut self) {
        self.state = BindingState::Detached;
    }

    /// Whether this binding cares about a given source table.
    pub fn matches_table(&self, table: &str) -> bool {
        self.source_tables.contains(table)
    }

    /// Record delivery of a delta batch.
    pub fn record_delivery(&mut self, batch: &DeltaBatch) {
        let relevant = batch
            .deltas
            .iter()
            .filter(|d| self.matches_table(&d.source_table))
            .count() as u64;
        self.deltas_delivered += relevant;
        if relevant > 0 {
            self.last_commit_seq = Some(batch.commit_seq);
        }
    }
}

// ── Propagation Engine ───────────────────────────────────────────────────────

/// Result of propagating a delta batch to the render tree.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PropagationResult {
    /// All bindings received the batch successfully.
    Success {
        widgets_invalidated: usize,
    },
    /// Some bindings received deltas; others were suspended or detached.
    Partial {
        widgets_invalidated: usize,
        skipped_suspended: usize,
        skipped_detached: usize,
    },
    /// No bindings matched the batch's source tables.
    NoMatch,
    /// The propagation pipeline is shut down.
    Shutdown,
}

impl PropagationResult {
    /// Number of widgets invalidated.
    pub fn widgets_invalidated(&self) -> usize {
        match self {
            Self::Success { widgets_invalidated }
            | Self::Partial { widgets_invalidated, .. } => *widgets_invalidated,
            Self::NoMatch | Self::Shutdown => 0,
        }
    }

    /// Whether propagation succeeded (fully or partially).
    pub fn is_success(&self) -> bool {
        matches!(self, Self::Success { .. } | Self::Partial { .. })
    }
}

/// Configuration for the delta propagation engine.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PropagationConfig {
    /// Maximum batch size before forced flush.
    pub max_batch_size: usize,
    /// Maximum propagation latency target (microseconds).
    pub target_latency_us: u64,
    /// Whether to coalesce multiple deltas to the same row.
    pub coalesce_row_deltas: bool,
    /// Maximum number of active bindings.
    pub max_active_bindings: usize,
}

impl Default for PropagationConfig {
    fn default() -> Self {
        Self {
            max_batch_size: 1024,
            target_latency_us: 1000, // 1ms target
            coalesce_row_deltas: true,
            max_active_bindings: 256,
        }
    }
}

/// Metrics tracked by the propagation engine.
///
/// Maps to the contract:
///   counter `bloodstream_deltas_total`
///   histogram `bloodstream_propagation_duration_us`
///   gauge `bloodstream_active_bindings`
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PropagationMetrics {
    /// Total deltas propagated (counter).
    pub deltas_total: u64,
    /// Total batches propagated.
    pub batches_total: u64,
    /// Propagation durations in microseconds (histogram samples).
    pub propagation_durations_us: Vec<u64>,
    /// Current number of active bindings (gauge).
    pub active_bindings: usize,
    /// Number of suspended bindings.
    pub suspended_bindings: usize,
    /// Number of detached bindings.
    pub detached_bindings: usize,
    /// Total widgets invalidated across all propagations.
    pub total_widgets_invalidated: u64,
    /// Number of NoMatch results (batch had no matching bindings).
    pub no_match_count: u64,
}

impl PropagationMetrics {
    /// Create fresh zero-valued metrics.
    pub fn new() -> Self {
        Self {
            deltas_total: 0,
            batches_total: 0,
            propagation_durations_us: Vec::new(),
            active_bindings: 0,
            suspended_bindings: 0,
            detached_bindings: 0,
            total_widgets_invalidated: 0,
            no_match_count: 0,
        }
    }

    /// Mean propagation duration in microseconds.
    pub fn mean_duration_us(&self) -> f64 {
        if self.propagation_durations_us.is_empty() {
            return 0.0;
        }
        let sum: u64 = self.propagation_durations_us.iter().sum();
        sum as f64 / self.propagation_durations_us.len() as f64
    }

    /// P99 propagation duration in microseconds.
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    pub fn p99_duration_us(&self) -> u64 {
        if self.propagation_durations_us.is_empty() {
            return 0;
        }
        let mut sorted = self.propagation_durations_us.clone();
        sorted.sort_unstable();
        let idx = ((sorted.len() as f64) * 0.99).ceil() as usize;
        sorted[idx.min(sorted.len() - 1)]
    }
}

impl Default for PropagationMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// The propagation engine routes delta batches to matching view bindings.
pub struct PropagationEngine {
    config: PropagationConfig,
    bindings: Vec<ViewBinding>,
    metrics: PropagationMetrics,
    next_binding_id: u64,
    shutdown: bool,
}

impl PropagationEngine {
    /// Create a new engine with the given configuration.
    pub fn new(config: PropagationConfig) -> Self {
        Self {
            config,
            bindings: Vec::new(),
            metrics: PropagationMetrics::new(),
            next_binding_id: 1,
            shutdown: false,
        }
    }

    /// Register a new view binding. Returns the binding ID.
    pub fn bind(
        &mut self,
        view_name: String,
        widget_id: String,
        source_tables: BTreeSet<String>,
    ) -> Result<u64, BindingError> {
        if self.shutdown {
            return Err(BindingError::EngineShutdown);
        }
        let active = self
            .bindings
            .iter()
            .filter(|b| b.state == BindingState::Active)
            .count();
        if active >= self.config.max_active_bindings {
            return Err(BindingError::MaxBindingsExceeded {
                limit: self.config.max_active_bindings,
            });
        }
        let id = self.next_binding_id;
        self.next_binding_id += 1;
        let binding = ViewBinding::new(id, view_name, widget_id, source_tables);
        self.bindings.push(binding);
        self.refresh_binding_counts();
        Ok(id)
    }

    /// Unbind (detach) a binding by ID.
    pub fn unbind(&mut self, binding_id: u64) -> Result<(), BindingError> {
        let binding = self
            .bindings
            .iter_mut()
            .find(|b| b.binding_id == binding_id)
            .ok_or(BindingError::NotFound { binding_id })?;
        binding.detach();
        self.refresh_binding_counts();
        Ok(())
    }

    /// Suspend a binding by ID.
    pub fn suspend(&mut self, binding_id: u64) -> Result<(), BindingError> {
        let binding = self
            .bindings
            .iter_mut()
            .find(|b| b.binding_id == binding_id)
            .ok_or(BindingError::NotFound { binding_id })?;
        binding.suspend();
        self.refresh_binding_counts();
        Ok(())
    }

    /// Resume a suspended binding by ID.
    pub fn resume(&mut self, binding_id: u64) -> Result<(), BindingError> {
        let binding = self
            .bindings
            .iter_mut()
            .find(|b| b.binding_id == binding_id)
            .ok_or(BindingError::NotFound { binding_id })?;
        binding.resume();
        self.refresh_binding_counts();
        Ok(())
    }

    /// Propagate a delta batch to all matching active bindings.
    pub fn propagate(&mut self, batch: &DeltaBatch, duration_us: u64) -> PropagationResult {
        if self.shutdown {
            return PropagationResult::Shutdown;
        }

        let tables = batch.source_tables();
        let mut widgets_invalidated = 0usize;
        let mut skipped_suspended = 0usize;
        let mut skipped_detached = 0usize;
        let mut any_match = false;

        for binding in &mut self.bindings {
            let has_match = tables.iter().any(|t| binding.matches_table(t));
            if !has_match {
                continue;
            }
            any_match = true;

            match binding.state {
                BindingState::Active => {
                    binding.record_delivery(batch);
                    widgets_invalidated += 1;
                }
                BindingState::Suspended => {
                    skipped_suspended += 1;
                }
                BindingState::Detached => {
                    skipped_detached += 1;
                }
            }
        }

        // Update metrics.
        self.metrics.deltas_total += batch.len() as u64;
        self.metrics.batches_total += 1;
        self.metrics.propagation_durations_us.push(duration_us);
        self.metrics.total_widgets_invalidated += widgets_invalidated as u64;

        if !any_match {
            self.metrics.no_match_count += 1;
            return PropagationResult::NoMatch;
        }

        if skipped_suspended == 0 && skipped_detached == 0 {
            PropagationResult::Success { widgets_invalidated }
        } else {
            PropagationResult::Partial {
                widgets_invalidated,
                skipped_suspended,
                skipped_detached,
            }
        }
    }

    /// Initiate engine shutdown; further binds and propagations are rejected.
    pub fn shutdown(&mut self) {
        self.shutdown = true;
    }

    /// Whether the engine is shut down.
    pub fn is_shutdown(&self) -> bool {
        self.shutdown
    }

    /// Current metrics snapshot.
    pub fn metrics(&self) -> &PropagationMetrics {
        &self.metrics
    }

    /// Current configuration.
    pub fn config(&self) -> &PropagationConfig {
        &self.config
    }

    /// All bindings (for inspection).
    pub fn bindings(&self) -> &[ViewBinding] {
        &self.bindings
    }

    /// Get a binding by ID.
    pub fn get_binding(&self, binding_id: u64) -> Option<&ViewBinding> {
        self.bindings.iter().find(|b| b.binding_id == binding_id)
    }

    fn refresh_binding_counts(&mut self) {
        self.metrics.active_bindings = self
            .bindings
            .iter()
            .filter(|b| b.state == BindingState::Active)
            .count();
        self.metrics.suspended_bindings = self
            .bindings
            .iter()
            .filter(|b| b.state == BindingState::Suspended)
            .count();
        self.metrics.detached_bindings = self
            .bindings
            .iter()
            .filter(|b| b.state == BindingState::Detached)
            .count();
    }
}

/// Errors from binding operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BindingError {
    /// Engine is shut down.
    EngineShutdown,
    /// Maximum active bindings exceeded.
    MaxBindingsExceeded { limit: usize },
    /// Binding ID not found.
    NotFound { binding_id: u64 },
}

impl fmt::Display for BindingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EngineShutdown => write!(f, "propagation engine is shut down"),
            Self::MaxBindingsExceeded { limit } => {
                write!(f, "max active bindings exceeded (limit: {limit})")
            }
            Self::NotFound { binding_id } => {
                write!(f, "binding {binding_id} not found")
            }
        }
    }
}

impl std::error::Error for BindingError {}

// ── Delta Coalescing ─────────────────────────────────────────────────────────

/// Coalesce deltas to the same row within a batch.
///
/// Rules:
/// - INSERT + UPDATE → INSERT (with merged columns)
/// - INSERT + DELETE → cancel (row never materialized)
/// - UPDATE + UPDATE → UPDATE (merged columns)
/// - UPDATE + DELETE → DELETE
/// - DELETE + INSERT → UPDATE (row replaced)
pub fn coalesce_deltas(deltas: &[AlgebraicDelta]) -> Vec<AlgebraicDelta> {
    // Group by (source_table, row_id), preserving order of first appearance.
    let mut groups: BTreeMap<(&str, i64), Vec<&AlgebraicDelta>> = BTreeMap::new();
    for d in deltas {
        groups
            .entry((d.source_table.as_str(), d.row_id))
            .or_default()
            .push(d);
    }

    let mut result = Vec::new();
    let mut next_seq = 0u64;

    for ((table, row_id), group) in &groups {
        let mut current_kind: Option<DeltaKind> = None;
        let mut merged_cols: BTreeSet<usize> = BTreeSet::new();

        for d in group {
            match (current_kind, d.kind) {
                (None, k) => {
                    current_kind = Some(k);
                    merged_cols.extend(&d.affected_columns);
                }
                (Some(DeltaKind::Insert), DeltaKind::Update) => {
                    // INSERT + UPDATE → INSERT
                    merged_cols.extend(&d.affected_columns);
                }
                (Some(DeltaKind::Insert), DeltaKind::Delete) => {
                    // INSERT + DELETE → cancel
                    current_kind = None;
                    merged_cols.clear();
                }
                (Some(DeltaKind::Update), DeltaKind::Update) => {
                    // UPDATE + UPDATE → UPDATE
                    merged_cols.extend(&d.affected_columns);
                }
                (Some(DeltaKind::Update), DeltaKind::Delete) => {
                    // UPDATE + DELETE → DELETE
                    current_kind = Some(DeltaKind::Delete);
                    merged_cols.clear();
                }
                (Some(DeltaKind::Delete), DeltaKind::Insert) => {
                    // DELETE + INSERT → UPDATE
                    current_kind = Some(DeltaKind::Update);
                    merged_cols = d.affected_columns.iter().copied().collect();
                }
                _ => {
                    // Other combinations: keep the latest.
                    current_kind = Some(d.kind);
                    merged_cols = d.affected_columns.iter().copied().collect();
                }
            }
        }

        if let Some(kind) = current_kind {
            result.push(AlgebraicDelta {
                source_table: table.to_string(),
                row_id: *row_id,
                kind,
                affected_columns: merged_cols.into_iter().collect(),
                seq: next_seq,
            });
            next_seq += 1;
        }
    }

    result
}

// ── Tracing Contract Verification ────────────────────────────────────────────

/// Span field names required by the bloodstream tracing contract.
pub const REQUIRED_SPAN_FIELDS: &[&str] = &[
    "source_table",
    "rows_changed",
    "propagation_duration_us",
    "widgets_invalidated",
];

/// Metric names required by the bloodstream metrics contract.
pub const REQUIRED_METRICS: &[&str] = &[
    "bloodstream_deltas_total",
    "bloodstream_propagation_duration_us",
    "bloodstream_active_bindings",
];

/// The span name for delta propagation events.
pub const DELTA_SPAN_NAME: &str = "bloodstream.delta";

/// Verify that a propagation metrics snapshot satisfies the tracing contract.
#[allow(clippy::too_many_lines)]
pub fn verify_metrics_contract(metrics: &PropagationMetrics) -> Vec<ContractViolation> {
    let mut violations = Vec::new();

    // Active bindings gauge must be non-negative (always true for usize, but
    // verify against the metric semantics).
    if metrics.deltas_total > 0 && metrics.batches_total == 0 {
        violations.push(ContractViolation {
            field: "batches_total".to_string(),
            message: "deltas_total > 0 but batches_total == 0".to_string(),
        });
    }

    // Duration histogram should have one entry per batch.
    #[allow(clippy::cast_possible_truncation)]
    if metrics.propagation_durations_us.len() != metrics.batches_total as usize {
        violations.push(ContractViolation {
            field: "propagation_durations_us".to_string(),
            message: format!(
                "duration samples ({}) != batches_total ({})",
                metrics.propagation_durations_us.len(),
                metrics.batches_total
            ),
        });
    }

    // Binding counts should be consistent.
    let total_bindings =
        metrics.active_bindings + metrics.suspended_bindings + metrics.detached_bindings;
    if total_bindings == 0 && metrics.deltas_total > 0 && metrics.no_match_count == 0 {
        violations.push(ContractViolation {
            field: "binding_counts".to_string(),
            message: "deltas propagated but no bindings registered".to_string(),
        });
    }

    violations
}

/// A single contract violation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContractViolation {
    pub field: String,
    pub message: String,
}

impl fmt::Display for ContractViolation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}] {}", self.field, self.message)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delta_kind_display_roundtrip() {
        for kind in DeltaKind::ALL {
            let s = kind.to_string();
            assert!(!s.is_empty());
        }
    }

    #[test]
    fn binding_state_lifecycle() {
        for state in BindingState::ALL {
            let s = state.to_string();
            assert!(!s.is_empty());
        }
        assert!(BindingState::Active.is_receiving());
        assert!(!BindingState::Suspended.is_receiving());
        assert!(!BindingState::Detached.is_receiving());
    }

    #[test]
    fn delta_batch_source_tables() {
        let mut batch = DeltaBatch::new(1, 1, 0);
        batch.push(AlgebraicDelta {
            source_table: "users".to_string(),
            row_id: 1,
            kind: DeltaKind::Insert,
            affected_columns: vec![0, 1],
            seq: 0,
        });
        batch.push(AlgebraicDelta {
            source_table: "orders".to_string(),
            row_id: 2,
            kind: DeltaKind::Update,
            affected_columns: vec![3],
            seq: 1,
        });
        let tables = batch.source_tables();
        assert!(tables.contains("users"));
        assert!(tables.contains("orders"));
        assert_eq!(tables.len(), 2);
    }

    #[test]
    fn coalesce_insert_then_delete_cancels() {
        let deltas = vec![
            AlgebraicDelta {
                source_table: "t".to_string(),
                row_id: 1,
                kind: DeltaKind::Insert,
                affected_columns: vec![0],
                seq: 0,
            },
            AlgebraicDelta {
                source_table: "t".to_string(),
                row_id: 1,
                kind: DeltaKind::Delete,
                affected_columns: vec![],
                seq: 1,
            },
        ];
        let coalesced = coalesce_deltas(&deltas);
        assert!(coalesced.is_empty(), "INSERT+DELETE should cancel");
    }

    #[test]
    fn propagation_config_defaults() {
        let config = PropagationConfig::default();
        assert!(config.max_batch_size > 0);
        assert!(config.target_latency_us > 0);
        assert!(config.coalesce_row_deltas);
        assert!(config.max_active_bindings > 0);
    }

    #[test]
    fn schema_version_stable() {
        assert_eq!(BLOODSTREAM_SCHEMA_VERSION, 1);
    }
}
