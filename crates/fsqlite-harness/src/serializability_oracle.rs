//! Typed transaction histories and the authoritative serializability/SSI oracle.
//!
//! The oracle is deliberately independent of the engine's SSI implementation.
//! It consumes observations from either deterministic schedules or public-path
//! OS-thread runs, validates their provenance, constructs a dependency graph,
//! and emits a stable minimal witness. Observation-only histories remain useful
//! regression evidence, but never claim deterministic schedule replay.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::failure_bundle::{
    EnvironmentInfo, ExecutionLaneEvidence, FailureBundle, FailureBundleBuilder, FailureInfo,
    FailureType, ReproducibilityInfo, ScenarioInfo,
};

/// Bead owning this contract.
pub const SERIALIZABILITY_ORACLE_BEAD_ID: &str = "bd-turso-test-adaptation-zu081.7";
/// Typed history schema identifier.
pub const TRANSACTION_HISTORY_SCHEMA_VERSION: &str = "fsqlite.transaction-history.v1";
/// Oracle report schema identifier.
pub const SERIALIZABILITY_REPORT_SCHEMA_VERSION: &str = "fsqlite.serializability-report.v1";
/// Canonical failure-bundle snapshot containing the typed history.
pub const HISTORY_SNAPSHOT_KEY: &str = "serializability_history_json";
/// Canonical failure-bundle snapshot containing the oracle report.
pub const REPORT_SNAPSHOT_KEY: &str = "serializability_report_json";

fn sha256_hex(bytes: &[u8]) -> String {
    crate::bytes_to_lower_hex(Sha256::digest(bytes))
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn require_nonempty(errors: &mut Vec<String>, field: &str, value: &str) {
    if value.trim().is_empty() {
        errors.push(format!("{field} is empty"));
    }
}

/// Workload semantics enforced in addition to serializability.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum HistoryWorkload {
    Register,
    ListAppend,
    Bank { allow_negative: bool },
    UniqueAllocation,
    WriteSkew { minimum_sum: i64 },
}

/// Whether a history can reproduce the scheduler decisions that produced it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScheduleControl {
    ObservationOnly,
    Deterministic,
}

/// Scheduler evidence carried by every history.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScheduleProvenance {
    pub control: ScheduleControl,
    pub observation_source: String,
    pub schedule_id: Option<String>,
    pub schedule_sha256: Option<String>,
    pub replay_command: Option<String>,
}

impl ScheduleProvenance {
    /// Provenance for an OS-thread run whose interleaving was observed, not controlled.
    #[must_use]
    pub fn observation_only(source: impl Into<String>) -> Self {
        Self {
            control: ScheduleControl::ObservationOnly,
            observation_source: source.into(),
            schedule_id: None,
            schedule_sha256: None,
            replay_command: None,
        }
    }

    /// Provenance for a fully captured, replayable deterministic schedule.
    #[must_use]
    pub fn deterministic(
        source: impl Into<String>,
        schedule_id: impl Into<String>,
        schedule_sha256: impl Into<String>,
        replay_command: impl Into<String>,
    ) -> Self {
        Self {
            control: ScheduleControl::Deterministic,
            observation_source: source.into(),
            schedule_id: Some(schedule_id.into()),
            schedule_sha256: Some(schedule_sha256.into()),
            replay_command: Some(replay_command.into()),
        }
    }

    /// Whether the provenance supports deterministic schedule replay.
    #[must_use]
    pub const fn deterministic_replay_claim(&self) -> bool {
        matches!(self.control, ScheduleControl::Deterministic)
    }

    fn validate(&self) -> Vec<String> {
        let mut errors = Vec::new();
        require_nonempty(
            &mut errors,
            "schedule.observation_source",
            &self.observation_source,
        );
        match self.control {
            ScheduleControl::ObservationOnly => {
                if self.schedule_id.is_some()
                    || self.schedule_sha256.is_some()
                    || self.replay_command.is_some()
                {
                    errors.push(
                        "observation-only schedule must not carry deterministic replay fields"
                            .to_owned(),
                    );
                }
            }
            ScheduleControl::Deterministic => {
                match self.schedule_id.as_deref() {
                    Some(value) => require_nonempty(&mut errors, "schedule.schedule_id", value),
                    None => errors.push("deterministic schedule lacks schedule_id".to_owned()),
                }
                match self.schedule_sha256.as_deref() {
                    Some(value) if is_sha256(value) => {}
                    Some(_) => errors
                        .push("deterministic schedule_sha256 must be lowercase SHA-256".to_owned()),
                    None => errors.push("deterministic schedule lacks schedule_sha256".to_owned()),
                }
                match self.replay_command.as_deref() {
                    Some(value) if !value.trim().is_empty() && !value.contains(['\n', '\r']) => {}
                    Some(_) => errors.push(
                        "deterministic replay_command must be non-empty and single-line".to_owned(),
                    ),
                    None => errors.push("deterministic schedule lacks replay_command".to_owned()),
                }
            }
        }
        errors
    }
}

/// Transaction begin mode observed on the production SQL path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BeginMode {
    Deferred,
    Immediate,
    Exclusive,
    Concurrent,
}

/// Stable, JSON-safe values used by history operations and state snapshots.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum HistoryValue {
    Null,
    Integer(i64),
    Text(String),
    Blob(Vec<u8>),
    List(Vec<Self>),
}

/// One observed history operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum HistoryOperation {
    Begin {
        mode: BeginMode,
    },
    Read {
        key: String,
        value: HistoryValue,
        version: Option<String>,
        source_transaction_id: Option<String>,
    },
    Write {
        key: String,
        value: HistoryValue,
        page_number: Option<u32>,
    },
    Append {
        key: String,
        value: HistoryValue,
    },
    Allocate {
        namespace: String,
        value: String,
    },
    Commit,
    Rollback {
        reason: String,
    },
    Conflict {
        reason: String,
    },
    Retry {
        attempt: u32,
        reason: String,
    },
    Cancel {
        reason: String,
    },
    Timeout {
        budget_ms: u64,
    },
    Indeterminate {
        reason: String,
    },
    Crash {
        crash_id: String,
    },
    Restart {
        crash_id: String,
    },
    Checkpoint {
        mode: String,
    },
}

impl HistoryOperation {
    const fn requires_transaction(&self) -> bool {
        !matches!(
            self,
            Self::Crash { .. } | Self::Restart { .. } | Self::Checkpoint { .. }
        )
    }

    const fn terminal_status(&self) -> Option<TerminalStatus> {
        match self {
            Self::Commit => Some(TerminalStatus::Committed),
            Self::Rollback { .. } => Some(TerminalStatus::RolledBack),
            Self::Cancel { .. } => Some(TerminalStatus::Cancelled),
            Self::Timeout { .. } => Some(TerminalStatus::TimedOut),
            Self::Indeterminate { .. } => Some(TerminalStatus::Indeterminate),
            _ => None,
        }
    }
}

/// Terminal transaction classification used by reports and exclusion accounting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TerminalStatus {
    Committed,
    RolledBack,
    Cancelled,
    TimedOut,
    Indeterminate,
}

/// One invocation/completion observation in logical order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryEvent {
    pub event_id: u64,
    pub logical_time: u64,
    pub process_id: String,
    pub connection_id: String,
    pub transaction_id: Option<String>,
    pub operation: HistoryOperation,
}

/// Complete typed transaction history consumed by the oracle.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransactionHistory {
    pub schema_version: String,
    pub run_id: String,
    pub trace_id: String,
    pub scenario_id: String,
    pub seed: u64,
    pub engine_git_sha: String,
    pub engine_dirty: bool,
    pub workload: HistoryWorkload,
    pub schedule: ScheduleProvenance,
    pub execution_lane_evidence: Vec<ExecutionLaneEvidence>,
    pub concurrent_mode_enabled: bool,
    pub reopen_concurrent_mode_enabled: Option<bool>,
    pub initial_state: BTreeMap<String, HistoryValue>,
    pub final_state: BTreeMap<String, HistoryValue>,
    pub final_state_sha256: String,
    pub events: Vec<HistoryEvent>,
}

impl TransactionHistory {
    /// Compute the canonical final-state hash.
    #[must_use]
    pub fn canonical_final_state_hash(&self) -> String {
        let bytes = serde_json::to_vec(&self.final_state)
            .expect("history final-state serialization must succeed");
        sha256_hex(&bytes)
    }

    /// Compute the history identity after validating its embedded state hash.
    #[must_use]
    pub fn deterministic_hash(&self) -> String {
        let bytes = serde_json::to_vec(self).expect("history serialization must succeed");
        sha256_hex(&bytes)
    }

    /// Recompute the final-state hash after constructing or changing a history.
    pub fn refresh_final_state_hash(&mut self) {
        self.final_state_sha256 = self.canonical_final_state_hash();
    }

    /// Validate structural integrity, terminal semantics, and provenance.
    #[must_use]
    pub fn validate(&self) -> Vec<String> {
        let mut errors = Vec::new();
        if self.schema_version != TRANSACTION_HISTORY_SCHEMA_VERSION {
            errors.push(format!(
                "history.schema_version must equal {TRANSACTION_HISTORY_SCHEMA_VERSION}"
            ));
        }
        for (field, value) in [
            ("history.run_id", self.run_id.as_str()),
            ("history.trace_id", self.trace_id.as_str()),
            ("history.scenario_id", self.scenario_id.as_str()),
            ("history.engine_git_sha", self.engine_git_sha.as_str()),
        ] {
            require_nonempty(&mut errors, field, value);
        }
        errors.extend(self.schedule.validate());
        if !self.concurrent_mode_enabled {
            errors.push("history.concurrent_mode_enabled must be true".to_owned());
        }
        if self
            .events
            .iter()
            .any(|event| matches!(event.operation, HistoryOperation::Restart { .. }))
            && self.reopen_concurrent_mode_enabled != Some(true)
        {
            errors.push(
                "restart histories must prove reopen_concurrent_mode_enabled=true".to_owned(),
            );
        }
        if self.execution_lane_evidence.is_empty() {
            errors.push("history.execution_lane_evidence is empty".to_owned());
        }
        for (index, evidence) in self.execution_lane_evidence.iter().enumerate() {
            for error in evidence.validate() {
                errors.push(format!("history.execution_lane_evidence[{index}]: {error}"));
            }
            if evidence.run_id != self.run_id
                || evidence.trace_id != self.trace_id
                || evidence.scenario_id != self.scenario_id
            {
                errors.push(format!(
                    "history.execution_lane_evidence[{index}] identity mismatch"
                ));
            }
        }
        if !is_sha256(&self.final_state_sha256)
            || self.final_state_sha256 != self.canonical_final_state_hash()
        {
            errors.push("history.final_state_sha256 mismatch".to_owned());
        }
        errors.extend(self.validate_events());
        errors
    }

    fn validate_events(&self) -> Vec<String> {
        #[derive(Default)]
        struct Lifecycle {
            process_id: String,
            connection_id: String,
            began: bool,
            terminal: Option<TerminalStatus>,
        }

        let mut errors = Vec::new();
        let mut lifecycles: BTreeMap<&str, Lifecycle> = BTreeMap::new();
        let mut prior_time = None;
        let mut crash_ids = BTreeSet::new();
        for (index, event) in self.events.iter().enumerate() {
            if event.event_id != u64::try_from(index).expect("event count must fit u64") {
                errors.push(format!(
                    "history.events[{index}].event_id must be contiguous from zero"
                ));
            }
            if prior_time.is_some_and(|time| event.logical_time < time) {
                errors.push(format!(
                    "history.events[{index}].logical_time is not monotonic"
                ));
            }
            prior_time = Some(event.logical_time);
            require_nonempty(
                &mut errors,
                &format!("history.events[{index}].process_id"),
                &event.process_id,
            );
            require_nonempty(
                &mut errors,
                &format!("history.events[{index}].connection_id"),
                &event.connection_id,
            );
            if event.operation.requires_transaction() != event.transaction_id.is_some() {
                errors.push(format!(
                    "history.events[{index}] transaction identity conflicts with operation scope"
                ));
                continue;
            }
            match &event.operation {
                HistoryOperation::Crash { crash_id } => {
                    require_nonempty(
                        &mut errors,
                        &format!("history.events[{index}].crash_id"),
                        crash_id,
                    );
                    if !crash_ids.insert(crash_id.as_str()) {
                        errors.push(format!(
                            "history.events[{index}] duplicates crash_id {crash_id}"
                        ));
                    }
                }
                HistoryOperation::Restart { crash_id }
                    if !crash_ids.contains(crash_id.as_str()) =>
                {
                    errors.push(format!(
                        "history.events[{index}] restarts unknown crash_id {crash_id}"
                    ));
                }
                _ => {}
            }
            let Some(txn_id) = event.transaction_id.as_deref() else {
                continue;
            };
            require_nonempty(
                &mut errors,
                &format!("history.events[{index}].transaction_id"),
                txn_id,
            );
            let lifecycle = lifecycles.entry(txn_id).or_default();
            if lifecycle.process_id.is_empty() {
                lifecycle.process_id.clone_from(&event.process_id);
                lifecycle.connection_id.clone_from(&event.connection_id);
            } else if lifecycle.process_id != event.process_id
                || lifecycle.connection_id != event.connection_id
            {
                errors.push(format!(
                    "transaction {txn_id} changes process or connection identity"
                ));
            }
            if lifecycle.terminal.is_some() {
                errors.push(format!(
                    "history.events[{index}] occurs after terminal event for {txn_id}"
                ));
                continue;
            }
            match event.operation {
                HistoryOperation::Begin { .. } => {
                    if lifecycle.began {
                        errors.push(format!("transaction {txn_id} has duplicate begin"));
                    }
                    lifecycle.began = true;
                }
                _ if !lifecycle.began => {
                    errors.push(format!(
                        "history.events[{index}] precedes begin for transaction {txn_id}"
                    ));
                }
                _ => {}
            }
            if let Some(status) = event.operation.terminal_status() {
                lifecycle.terminal = Some(status);
            }
        }
        for (txn_id, lifecycle) in lifecycles {
            if !lifecycle.began {
                errors.push(format!("transaction {txn_id} lacks begin"));
            }
            if lifecycle.terminal.is_none() {
                errors.push(format!("transaction {txn_id} lacks terminal event"));
            }
        }
        errors
    }

    /// Strict JSON encoding for durable evidence artifacts.
    pub fn to_json(&self) -> Result<String, String> {
        let errors = self.validate();
        if !errors.is_empty() {
            return Err(format!(
                "transaction history invalid: {}",
                errors.join("; ")
            ));
        }
        serde_json::to_string_pretty(self).map_err(|error| error.to_string())
    }

    /// Strict JSON decoding that rejects unknown fields, truncation, and bad hashes.
    pub fn from_json_strict(json: &str) -> Result<Self, String> {
        let history: Self = serde_json::from_str(json)
            .map_err(|error| format!("transaction history decode failed: {error}"))?;
        let errors = history.validate();
        if errors.is_empty() {
            Ok(history)
        } else {
            Err(format!(
                "transaction history invalid: {}",
                errors.join("; ")
            ))
        }
    }
}

/// A committed transaction projected from a history or an existing test.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommittedTransaction {
    pub transaction_id: String,
    pub start_order: u64,
    pub commit_order: u64,
    pub read_set: BTreeSet<String>,
    pub write_set: BTreeSet<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub read_sources: BTreeMap<String, String>,
}

/// Dependency edge classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DependencyKind {
    WriteRead,
    ReadWrite,
    WriteWrite,
}

/// One stable graph edge with the key that established it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DependencyEdge {
    pub from: String,
    pub to: String,
    pub kind: DependencyKind,
    pub key: String,
}

/// Dependency graph and its shortest lexicographically stable cycle.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SerializationGraph {
    pub transactions: Vec<String>,
    pub edges: Vec<DependencyEdge>,
    pub cycle: Option<Vec<DependencyEdge>>,
}

/// Construct a deterministic serialization graph from committed transactions.
#[must_use]
pub fn build_serialization_graph(transactions: &[CommittedTransaction]) -> SerializationGraph {
    let mut txns = transactions.to_vec();
    txns.sort_by(|left, right| left.transaction_id.cmp(&right.transaction_id));
    let mut edges = BTreeSet::new();

    for reader in &txns {
        for (key, source) in &reader.read_sources {
            if source != &reader.transaction_id
                && txns
                    .iter()
                    .any(|transaction| &transaction.transaction_id == source)
            {
                edges.insert(DependencyEdge {
                    from: source.clone(),
                    to: reader.transaction_id.clone(),
                    kind: DependencyKind::WriteRead,
                    key: key.clone(),
                });
            }
        }
    }

    for left_index in 0..txns.len() {
        for right_index in (left_index + 1)..txns.len() {
            let left = &txns[left_index];
            let right = &txns[right_index];
            for key in left.write_set.intersection(&right.write_set) {
                let (from, to) = if (left.commit_order, &left.transaction_id)
                    <= (right.commit_order, &right.transaction_id)
                {
                    (&left.transaction_id, &right.transaction_id)
                } else {
                    (&right.transaction_id, &left.transaction_id)
                };
                edges.insert(DependencyEdge {
                    from: from.clone(),
                    to: to.clone(),
                    kind: DependencyKind::WriteWrite,
                    key: key.clone(),
                });
            }
            add_read_write_edges(left, right, &mut edges);
            add_read_write_edges(right, left, &mut edges);
        }
    }

    let edges = edges.into_iter().collect::<Vec<_>>();
    let transaction_ids = txns
        .iter()
        .map(|transaction| transaction.transaction_id.clone())
        .collect::<Vec<_>>();
    let cycle = shortest_cycle(&transaction_ids, &edges);
    SerializationGraph {
        transactions: transaction_ids,
        edges,
        cycle,
    }
}

fn add_read_write_edges(
    writer: &CommittedTransaction,
    reader: &CommittedTransaction,
    edges: &mut BTreeSet<DependencyEdge>,
) {
    for key in writer.write_set.intersection(&reader.read_set) {
        if reader.read_sources.get(key) == Some(&writer.transaction_id) {
            continue;
        }
        let (from, to, kind) = if writer.commit_order <= reader.start_order {
            (
                &writer.transaction_id,
                &reader.transaction_id,
                DependencyKind::WriteRead,
            )
        } else {
            (
                &reader.transaction_id,
                &writer.transaction_id,
                DependencyKind::ReadWrite,
            )
        };
        edges.insert(DependencyEdge {
            from: from.clone(),
            to: to.clone(),
            kind,
            key: key.clone(),
        });
    }
}

fn shortest_cycle(nodes: &[String], edges: &[DependencyEdge]) -> Option<Vec<DependencyEdge>> {
    let mut outgoing: BTreeMap<&str, Vec<&DependencyEdge>> = BTreeMap::new();
    for edge in edges {
        outgoing.entry(&edge.from).or_default().push(edge);
    }
    for values in outgoing.values_mut() {
        values.sort_unstable();
    }
    let mut candidates = Vec::new();
    for start in nodes {
        let mut queue = VecDeque::from([(start.as_str(), Vec::<DependencyEdge>::new())]);
        let mut best_depth: BTreeMap<&str, usize> = BTreeMap::from([(start.as_str(), 0)]);
        while let Some((node, path)) = queue.pop_front() {
            for edge in outgoing.get(node).into_iter().flatten() {
                let mut next_path = path.clone();
                next_path.push((*edge).clone());
                if edge.to == *start {
                    candidates.push(canonicalize_cycle(next_path));
                    continue;
                }
                if next_path.len() >= nodes.len() {
                    continue;
                }
                let depth = next_path.len();
                if best_depth
                    .get(edge.to.as_str())
                    .is_none_or(|seen| depth <= *seen)
                {
                    best_depth.insert(&edge.to, depth);
                    queue.push_back((&edge.to, next_path));
                }
            }
        }
    }
    candidates.sort_by(|left, right| left.len().cmp(&right.len()).then_with(|| left.cmp(right)));
    candidates.into_iter().next()
}

fn canonicalize_cycle(cycle: Vec<DependencyEdge>) -> Vec<DependencyEdge> {
    let mut rotations = (0..cycle.len())
        .map(|offset| {
            cycle[offset..]
                .iter()
                .chain(&cycle[..offset])
                .cloned()
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    rotations.sort();
    rotations.into_iter().next().unwrap_or_default()
}

/// Oracle verdict. Indeterminate histories cannot establish acceptance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OracleVerdict {
    Serializable,
    Rejected,
    Inconclusive,
}

/// Stable anomaly classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AnomalyKind {
    G1Cycle,
    G2Cycle,
    LostUpdate,
    FirstCommitterWinsViolation,
    WriteSkew,
    DirtyRead,
    FracturedRead,
    ModelInvariantViolation,
}

/// One anomaly with the smallest stable transaction/key witness available.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Anomaly {
    pub kind: AnomalyKind,
    pub transaction_ids: Vec<String>,
    pub keys: Vec<String>,
    pub detail: String,
}

/// Explicit accounting for transactions excluded from committed graph analysis.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExcludedTransactions {
    pub rolled_back: Vec<String>,
    pub cancelled: Vec<String>,
    pub timed_out: Vec<String>,
    pub indeterminate: Vec<String>,
}

/// Complete stable oracle result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SerializabilityReport {
    pub schema_version: String,
    pub run_id: String,
    pub trace_id: String,
    pub scenario_id: String,
    pub history_sha256: String,
    pub schedule_control: ScheduleControl,
    pub deterministic_replay_claim: bool,
    pub verdict: OracleVerdict,
    pub committed_transaction_count: usize,
    pub excluded_transactions: ExcludedTransactions,
    pub graph: SerializationGraph,
    pub anomalies: Vec<Anomaly>,
    pub minimal_witness: Option<Anomaly>,
    pub report_sha256: String,
}

impl SerializabilityReport {
    /// Hash report content while excluding the hash field itself.
    #[must_use]
    pub fn deterministic_hash(&self) -> String {
        let mut canonical = self.clone();
        canonical.report_sha256.clear();
        let bytes = serde_json::to_vec(&canonical)
            .expect("serializability report serialization must succeed");
        sha256_hex(&bytes)
    }

    /// Validate hash, verdict, witness, and schedule cross-links.
    pub fn validate_against(&self, history: &TransactionHistory) -> Result<(), String> {
        let errors = history.validate();
        if !errors.is_empty() {
            return Err(format!(
                "transaction history invalid: {}",
                errors.join("; ")
            ));
        }
        if self.schema_version != SERIALIZABILITY_REPORT_SCHEMA_VERSION {
            return Err("serializability report schema mismatch".to_owned());
        }
        if (
            self.run_id.as_str(),
            self.trace_id.as_str(),
            self.scenario_id.as_str(),
        ) != (
            history.run_id.as_str(),
            history.trace_id.as_str(),
            history.scenario_id.as_str(),
        ) {
            return Err("serializability report identity mismatch".to_owned());
        }
        if self.history_sha256 != history.deterministic_hash() {
            return Err("serializability report history hash mismatch".to_owned());
        }
        if self.schedule_control != history.schedule.control
            || self.deterministic_replay_claim != history.schedule.deterministic_replay_claim()
        {
            return Err("serializability report schedule provenance mismatch".to_owned());
        }
        if self.minimal_witness != self.anomalies.first().cloned() {
            return Err("serializability report minimal witness is not canonical".to_owned());
        }
        let expected_verdict = if self.anomalies.is_empty() {
            if self.excluded_transactions.indeterminate.is_empty() {
                OracleVerdict::Serializable
            } else {
                OracleVerdict::Inconclusive
            }
        } else {
            OracleVerdict::Rejected
        };
        if self.verdict != expected_verdict {
            return Err("serializability report verdict conflicts with evidence".to_owned());
        }
        if !is_sha256(&self.report_sha256) || self.report_sha256 != self.deterministic_hash() {
            return Err("serializability report hash mismatch".to_owned());
        }
        Ok(())
    }

    /// Strict JSON encoding for artifact publication.
    pub fn to_json(&self, history: &TransactionHistory) -> Result<String, String> {
        self.validate_against(history)?;
        serde_json::to_string_pretty(self).map_err(|error| error.to_string())
    }

    /// Strict JSON decoding and cross-validation against the typed history.
    pub fn from_json_strict(json: &str, history: &TransactionHistory) -> Result<Self, String> {
        let report: Self = serde_json::from_str(json)
            .map_err(|error| format!("serializability report decode failed: {error}"))?;
        report.validate_against(history)?;
        Ok(report)
    }
}

#[derive(Default)]
struct ProjectedTransaction {
    transaction_id: String,
    start_order: u64,
    commit_order: Option<u64>,
    terminal: Option<TerminalStatus>,
    reads: BTreeMap<String, Vec<(u64, HistoryValue, Option<String>)>>,
    writes: BTreeMap<String, HistoryValue>,
    appends: Vec<(String, HistoryValue)>,
    allocations: Vec<(String, String)>,
}

/// Validate and check one history using the independent oracle.
pub fn check_history(history: &TransactionHistory) -> Result<SerializabilityReport, String> {
    let errors = history.validate();
    if !errors.is_empty() {
        return Err(format!(
            "transaction history invalid: {}",
            errors.join("; ")
        ));
    }
    let projected = project_transactions(history);
    let committed = project_committed(&projected);
    let graph = build_serialization_graph(&committed);
    let excluded_transactions = classify_excluded(&projected);
    let mut anomalies = detect_history_anomalies(history, &projected, &committed, &graph);
    anomalies.sort();
    anomalies.dedup();
    let verdict = if anomalies.is_empty() {
        if excluded_transactions.indeterminate.is_empty() {
            OracleVerdict::Serializable
        } else {
            OracleVerdict::Inconclusive
        }
    } else {
        OracleVerdict::Rejected
    };
    let mut report = SerializabilityReport {
        schema_version: SERIALIZABILITY_REPORT_SCHEMA_VERSION.to_owned(),
        run_id: history.run_id.clone(),
        trace_id: history.trace_id.clone(),
        scenario_id: history.scenario_id.clone(),
        history_sha256: history.deterministic_hash(),
        schedule_control: history.schedule.control,
        deterministic_replay_claim: history.schedule.deterministic_replay_claim(),
        verdict,
        committed_transaction_count: committed.len(),
        excluded_transactions,
        graph,
        minimal_witness: anomalies.first().cloned(),
        anomalies,
        report_sha256: String::new(),
    };
    report.report_sha256 = report.deterministic_hash();
    report.validate_against(history)?;
    emit_report_diagnostics(&report);
    Ok(report)
}

fn project_transactions(history: &TransactionHistory) -> BTreeMap<String, ProjectedTransaction> {
    let mut projected = BTreeMap::new();
    for event in &history.events {
        let Some(transaction_id) = event.transaction_id.as_ref() else {
            continue;
        };
        let transaction =
            projected
                .entry(transaction_id.clone())
                .or_insert_with(|| ProjectedTransaction {
                    transaction_id: transaction_id.clone(),
                    ..ProjectedTransaction::default()
                });
        match &event.operation {
            HistoryOperation::Begin { .. } => transaction.start_order = event.logical_time,
            HistoryOperation::Read {
                key,
                value,
                source_transaction_id,
                ..
            } => transaction.reads.entry(key.clone()).or_default().push((
                event.logical_time,
                value.clone(),
                source_transaction_id.clone(),
            )),
            HistoryOperation::Write { key, value, .. } => {
                transaction.writes.insert(key.clone(), value.clone());
            }
            HistoryOperation::Append { key, value } => {
                transaction.appends.push((key.clone(), value.clone()));
            }
            HistoryOperation::Allocate { namespace, value } => {
                transaction
                    .allocations
                    .push((namespace.clone(), value.clone()));
            }
            operation if operation.terminal_status().is_some() => {
                transaction.terminal = operation.terminal_status();
                if transaction.terminal == Some(TerminalStatus::Committed) {
                    transaction.commit_order = Some(event.logical_time);
                }
            }
            _ => {}
        }
    }
    projected
}

fn project_committed(
    projected: &BTreeMap<String, ProjectedTransaction>,
) -> Vec<CommittedTransaction> {
    projected
        .values()
        .filter_map(|transaction| {
            let commit_order = transaction.commit_order?;
            let mut read_sources = BTreeMap::new();
            for (key, observations) in &transaction.reads {
                if let Some(source) = observations
                    .iter()
                    .rev()
                    .find_map(|(_, _, source)| source.clone())
                {
                    read_sources.insert(key.clone(), source);
                }
            }
            Some(CommittedTransaction {
                transaction_id: transaction.transaction_id.clone(),
                start_order: transaction.start_order,
                commit_order,
                read_set: transaction.reads.keys().cloned().collect(),
                write_set: transaction
                    .writes
                    .keys()
                    .chain(transaction.appends.iter().map(|(key, _)| key))
                    .cloned()
                    .collect(),
                read_sources,
            })
        })
        .collect()
}

fn classify_excluded(projected: &BTreeMap<String, ProjectedTransaction>) -> ExcludedTransactions {
    let mut excluded = ExcludedTransactions::default();
    for transaction in projected.values() {
        let destination = match transaction.terminal {
            Some(TerminalStatus::RolledBack) => Some(&mut excluded.rolled_back),
            Some(TerminalStatus::Cancelled) => Some(&mut excluded.cancelled),
            Some(TerminalStatus::TimedOut) => Some(&mut excluded.timed_out),
            Some(TerminalStatus::Indeterminate) => Some(&mut excluded.indeterminate),
            Some(TerminalStatus::Committed) | None => None,
        };
        if let Some(destination) = destination {
            destination.push(transaction.transaction_id.clone());
        }
    }
    excluded
}

fn detect_history_anomalies(
    history: &TransactionHistory,
    projected: &BTreeMap<String, ProjectedTransaction>,
    committed: &[CommittedTransaction],
    graph: &SerializationGraph,
) -> Vec<Anomaly> {
    let mut anomalies = Vec::new();
    if let Some(cycle) = &graph.cycle {
        let kind = if cycle
            .iter()
            .any(|edge| edge.kind == DependencyKind::ReadWrite)
        {
            AnomalyKind::G2Cycle
        } else {
            AnomalyKind::G1Cycle
        };
        anomalies.push(Anomaly {
            kind,
            transaction_ids: cycle.iter().map(|edge| edge.from.clone()).collect(),
            keys: cycle.iter().map(|edge| edge.key.clone()).collect(),
            detail: "serialization dependency cycle".to_owned(),
        });
    }
    detect_dirty_and_fractured_reads(projected, &mut anomalies);
    detect_overlapping_writers(committed, projected, &mut anomalies);
    detect_workload_invariants(history, projected, &mut anomalies);
    anomalies
}

fn detect_dirty_and_fractured_reads(
    projected: &BTreeMap<String, ProjectedTransaction>,
    anomalies: &mut Vec<Anomaly>,
) {
    for transaction in projected.values() {
        for (key, observations) in &transaction.reads {
            if let Some((_, first_value, _)) = observations.first()
                && observations
                    .iter()
                    .any(|(_, value, _)| value != first_value)
            {
                anomalies.push(Anomaly {
                    kind: AnomalyKind::FracturedRead,
                    transaction_ids: vec![transaction.transaction_id.clone()],
                    keys: vec![key.clone()],
                    detail: "one transaction observed multiple values for the same key".to_owned(),
                });
            }
            for (read_time, _, source) in observations {
                let Some(source) = source else {
                    continue;
                };
                if source == &transaction.transaction_id {
                    continue;
                }
                let clean_source = projected.get(source).is_some_and(|writer| {
                    writer
                        .commit_order
                        .is_some_and(|commit_time| commit_time <= *read_time)
                });
                if !clean_source {
                    anomalies.push(Anomaly {
                        kind: AnomalyKind::DirtyRead,
                        transaction_ids: vec![source.clone(), transaction.transaction_id.clone()],
                        keys: vec![key.clone()],
                        detail: "read observed a source transaction before its commit".to_owned(),
                    });
                }
            }
        }
    }
}

fn detect_overlapping_writers(
    committed: &[CommittedTransaction],
    projected: &BTreeMap<String, ProjectedTransaction>,
    anomalies: &mut Vec<Anomaly>,
) {
    for left_index in 0..committed.len() {
        for right_index in (left_index + 1)..committed.len() {
            let left = &committed[left_index];
            let right = &committed[right_index];
            let overlap =
                left.start_order < right.commit_order && right.start_order < left.commit_order;
            if !overlap {
                continue;
            }
            for key in left.write_set.intersection(&right.write_set) {
                let both_read_key = projected
                    .get(&left.transaction_id)
                    .is_some_and(|txn| txn.reads.contains_key(key))
                    && projected
                        .get(&right.transaction_id)
                        .is_some_and(|txn| txn.reads.contains_key(key));
                anomalies.push(Anomaly {
                    kind: if both_read_key {
                        AnomalyKind::LostUpdate
                    } else {
                        AnomalyKind::FirstCommitterWinsViolation
                    },
                    transaction_ids: vec![
                        left.transaction_id.clone(),
                        right.transaction_id.clone(),
                    ],
                    keys: vec![key.clone()],
                    detail: "overlapping writers committed the same logical key".to_owned(),
                });
            }
        }
    }
}

fn detect_workload_invariants(
    history: &TransactionHistory,
    projected: &BTreeMap<String, ProjectedTransaction>,
    anomalies: &mut Vec<Anomaly>,
) {
    match history.workload {
        HistoryWorkload::Register => {}
        HistoryWorkload::ListAppend => {
            for (key, initial) in &history.initial_state {
                let HistoryValue::List(initial) = initial else {
                    push_model_violation(anomalies, key, "list-append initial state is not a list");
                    continue;
                };
                let mut expected = initial.clone();
                let mut committed = projected
                    .values()
                    .filter(|transaction| transaction.commit_order.is_some())
                    .collect::<Vec<_>>();
                committed.sort_by_key(|transaction| transaction.commit_order);
                for transaction in committed {
                    expected.extend(
                        transaction
                            .appends
                            .iter()
                            .filter(|(append_key, _)| append_key == key)
                            .map(|(_, value)| value.clone()),
                    );
                }
                if history.final_state.get(key) != Some(&HistoryValue::List(expected)) {
                    push_model_violation(
                        anomalies,
                        key,
                        "committed list appends are not preserved",
                    );
                }
            }
        }
        HistoryWorkload::Bank { allow_negative } => {
            let initial = integer_sum(&history.initial_state);
            let final_sum = integer_sum(&history.final_state);
            if initial != final_sum {
                push_model_violation(anomalies, "bank", "bank balance sum changed");
            }
            if !allow_negative
                && history
                    .final_state
                    .values()
                    .any(|value| matches!(value, HistoryValue::Integer(number) if *number < 0))
            {
                push_model_violation(anomalies, "bank", "bank balance became negative");
            }
        }
        HistoryWorkload::UniqueAllocation => {
            let mut allocations = BTreeSet::new();
            for transaction in projected
                .values()
                .filter(|transaction| transaction.commit_order.is_some())
            {
                for (namespace, value) in &transaction.allocations {
                    if !allocations.insert((namespace, value)) {
                        anomalies.push(Anomaly {
                            kind: AnomalyKind::ModelInvariantViolation,
                            transaction_ids: vec![transaction.transaction_id.clone()],
                            keys: vec![namespace.clone()],
                            detail: format!("duplicate unique allocation {namespace}/{value}"),
                        });
                    }
                }
            }
        }
        HistoryWorkload::WriteSkew { minimum_sum } => {
            if integer_sum(&history.final_state) < Some(minimum_sum) {
                anomalies.push(Anomaly {
                    kind: AnomalyKind::WriteSkew,
                    transaction_ids: projected
                        .values()
                        .filter(|transaction| transaction.commit_order.is_some())
                        .map(|transaction| transaction.transaction_id.clone())
                        .collect(),
                    keys: history.final_state.keys().cloned().collect(),
                    detail: format!("final integer sum fell below {minimum_sum}"),
                });
            }
        }
    }
}

fn integer_sum(state: &BTreeMap<String, HistoryValue>) -> Option<i64> {
    state.values().try_fold(0_i64, |sum, value| match value {
        HistoryValue::Integer(number) => sum.checked_add(*number),
        _ => None,
    })
}

fn push_model_violation(anomalies: &mut Vec<Anomaly>, key: &str, detail: &str) {
    anomalies.push(Anomaly {
        kind: AnomalyKind::ModelInvariantViolation,
        transaction_ids: Vec::new(),
        keys: vec![key.to_owned()],
        detail: detail.to_owned(),
    });
}

fn emit_report_diagnostics(report: &SerializabilityReport) {
    tracing::info!(
        target: "fsqlite.serializability_oracle",
        run_id = %report.run_id,
        trace_id = %report.trace_id,
        scenario_id = %report.scenario_id,
        verdict = ?report.verdict,
        committed_transaction_count = report.committed_transaction_count,
        anomaly_count = report.anomalies.len(),
        schedule_control = ?report.schedule_control,
        deterministic_replay_claim = report.deterministic_replay_claim,
        report_sha256 = %report.report_sha256,
        "serializability oracle completed"
    );
    if let Some(witness) = &report.minimal_witness {
        tracing::error!(
            target: "fsqlite.serializability_oracle",
            run_id = %report.run_id,
            anomaly = ?witness.kind,
            transaction_ids = ?witness.transaction_ids,
            keys = ?witness.keys,
            detail = %witness.detail,
            "serializability oracle rejected history"
        );
    }
    for edge in &report.graph.edges {
        tracing::debug!(
            target: "fsqlite.serializability_oracle",
            from = %edge.from,
            to = %edge.to,
            kind = ?edge.kind,
            key = %edge.key,
            "serialization dependency edge"
        );
    }
}

/// Metadata needed to bind a rejected or inconclusive report to a canonical bundle.
pub struct SerializabilityBundleContext {
    pub bundle_id: String,
    pub created_at: String,
    pub test_name: String,
    pub script_path: Option<String>,
    pub repro_command: String,
    pub environment: EnvironmentInfo,
}

/// Build a canonical failure bundle without introducing a parallel artifact format.
pub fn build_serializability_failure_bundle(
    history: &TransactionHistory,
    report: &SerializabilityReport,
    context: SerializabilityBundleContext,
) -> Result<FailureBundle, String> {
    report.validate_against(history)?;
    if report.verdict == OracleVerdict::Serializable {
        return Err("serializable history must not produce a failure bundle".to_owned());
    }
    let history_json = history.to_json()?;
    let report_json = report.to_json(history)?;
    let failure_message = report.minimal_witness.as_ref().map_or_else(
        || "serializability result is inconclusive".to_owned(),
        |witness| format!("{:?}: {}", witness.kind, witness.detail),
    );
    let schedule_fingerprint = history.schedule.schedule_sha256.clone();
    let primary_lane = history
        .execution_lane_evidence
        .first()
        .cloned()
        .ok_or_else(|| "serializability history lacks lane evidence".to_owned())?;
    let mut builder = FailureBundleBuilder::new()
        .bundle_id(&context.bundle_id)
        .created_at(&context.created_at)
        .run_id(&history.run_id)
        .execution_lane_evidence(primary_lane)
        .scenario(ScenarioInfo {
            scenario_id: history.scenario_id.clone(),
            bead_id: SERIALIZABILITY_ORACLE_BEAD_ID.to_owned(),
            test_name: context.test_name,
            script_path: context.script_path,
        })
        .failure(FailureInfo {
            failure_type: FailureType::SsiConflict,
            message: failure_message,
            expected: Some("serializable SSI history".to_owned()),
            actual: Some(format!("{:?}", report.verdict)),
            diff: None,
            invariant: Some("SERIALIZABILITY-SSI".to_owned()),
            first_divergence: None,
        })
        .reproducibility(ReproducibilityInfo {
            seed: Some(history.seed),
            fixture_id: Some(report.history_sha256.clone()),
            schedule_fingerprint,
            repro_command: context.repro_command,
            storage_mode: Some("public-sql-path".to_owned()),
            concurrency_mode: Some("concurrent-writers-ssi".to_owned()),
        })
        .environment(context.environment)
        .state_snapshot(HISTORY_SNAPSHOT_KEY, &history_json)
        .state_snapshot(REPORT_SNAPSHOT_KEY, &report_json)
        .triage_tag("serializability-history")
        .triage_tag(match history.schedule.control {
            ScheduleControl::ObservationOnly => "observation-only",
            ScheduleControl::Deterministic => "deterministic-schedule",
        });
    if let Some(witness) = &report.minimal_witness {
        builder = builder.triage_tag(&format!("anomaly-{:?}", witness.kind).to_lowercase());
    }
    let bundle = builder.build()?;
    validate_serializability_failure_bundle(&bundle)?;
    Ok(bundle)
}

/// Validate all typed snapshots and provenance links in a canonical bundle.
pub fn validate_serializability_failure_bundle(
    bundle: &FailureBundle,
) -> Result<(TransactionHistory, SerializabilityReport), String> {
    let bundle_errors = bundle.validate();
    if !bundle_errors.is_empty() {
        return Err(format!(
            "failure bundle invalid: {}",
            bundle_errors.join("; ")
        ));
    }
    if bundle.failure.failure_type != FailureType::SsiConflict
        || !bundle
            .triage_tags
            .iter()
            .any(|tag| tag == "serializability-history")
    {
        return Err("bundle is not canonical serializability evidence".to_owned());
    }
    let history_json = bundle
        .state_snapshots
        .get(HISTORY_SNAPSHOT_KEY)
        .ok_or_else(|| "bundle lacks serializability history snapshot".to_owned())?;
    let history = TransactionHistory::from_json_strict(history_json)?;
    let report_json = bundle
        .state_snapshots
        .get(REPORT_SNAPSHOT_KEY)
        .ok_or_else(|| "bundle lacks serializability report snapshot".to_owned())?;
    let report = SerializabilityReport::from_json_strict(report_json, &history)?;
    let rerun = check_history(&history)?;
    if rerun != report {
        return Err("bundle oracle report does not match independent replay".to_owned());
    }
    if bundle.run_id != history.run_id
        || bundle.scenario.scenario_id != history.scenario_id
        || bundle.execution_lane_evidence != history.execution_lane_evidence[0]
        || bundle.reproducibility.seed != Some(history.seed)
        || bundle.reproducibility.fixture_id.as_deref() != Some(report.history_sha256.as_str())
    {
        return Err("bundle serializability provenance mismatch".to_owned());
    }
    match history.schedule.control {
        ScheduleControl::ObservationOnly => {
            if bundle.reproducibility.schedule_fingerprint.is_some()
                || !bundle
                    .triage_tags
                    .iter()
                    .any(|tag| tag == "observation-only")
            {
                return Err("observation-only bundle makes deterministic replay claim".to_owned());
            }
        }
        ScheduleControl::Deterministic => {
            if bundle.reproducibility.schedule_fingerprint != history.schedule.schedule_sha256
                || !bundle
                    .triage_tags
                    .iter()
                    .any(|tag| tag == "deterministic-schedule")
            {
                return Err("deterministic bundle schedule provenance mismatch".to_owned());
            }
        }
    }
    if report.verdict == OracleVerdict::Serializable {
        return Err("serializable report must not be bundled as a failure".to_owned());
    }
    Ok((history, report))
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;
    use crate::replay_harness::{SerializabilityReplayArtifact, replay_serializability_oracle};

    fn lane(run_id: &str, trace_id: &str, scenario_id: &str) -> ExecutionLaneEvidence {
        ExecutionLaneEvidence::semantic_only(trace_id, run_id, scenario_id, "transaction-history")
    }

    fn event(
        event_id: u64,
        logical_time: u64,
        txn: Option<&str>,
        operation: HistoryOperation,
    ) -> HistoryEvent {
        HistoryEvent {
            event_id,
            logical_time,
            process_id: "process-0".to_owned(),
            connection_id: txn.map_or("control", |value| value).to_owned(),
            transaction_id: txn.map(str::to_owned),
            operation,
        }
    }

    fn base_history(workload: HistoryWorkload, events: Vec<HistoryEvent>) -> TransactionHistory {
        let run_id = "run-ssi-1";
        let trace_id = "trace-ssi-1";
        let scenario_id = "TURSO-SSI-GOLDEN";
        let mut history = TransactionHistory {
            schema_version: TRANSACTION_HISTORY_SCHEMA_VERSION.to_owned(),
            run_id: run_id.to_owned(),
            trace_id: trace_id.to_owned(),
            scenario_id: scenario_id.to_owned(),
            seed: 7,
            engine_git_sha: "0123456789abcdef".to_owned(),
            engine_dirty: false,
            workload,
            schedule: ScheduleProvenance::observation_only("unit-test"),
            execution_lane_evidence: vec![lane(run_id, trace_id, scenario_id)],
            concurrent_mode_enabled: true,
            reopen_concurrent_mode_enabled: None,
            initial_state: BTreeMap::new(),
            final_state: BTreeMap::new(),
            final_state_sha256: String::new(),
            events,
        };
        history.refresh_final_state_hash();
        history
    }

    fn serial_history() -> TransactionHistory {
        base_history(
            HistoryWorkload::Register,
            vec![
                event(
                    0,
                    0,
                    Some("t1"),
                    HistoryOperation::Begin {
                        mode: BeginMode::Concurrent,
                    },
                ),
                event(
                    1,
                    1,
                    Some("t1"),
                    HistoryOperation::Write {
                        key: "x".to_owned(),
                        value: HistoryValue::Integer(1),
                        page_number: Some(2),
                    },
                ),
                event(2, 2, Some("t1"), HistoryOperation::Commit),
                event(
                    3,
                    3,
                    Some("t2"),
                    HistoryOperation::Begin {
                        mode: BeginMode::Concurrent,
                    },
                ),
                event(
                    4,
                    4,
                    Some("t2"),
                    HistoryOperation::Read {
                        key: "x".to_owned(),
                        value: HistoryValue::Integer(1),
                        version: Some("v1".to_owned()),
                        source_transaction_id: Some("t1".to_owned()),
                    },
                ),
                event(5, 5, Some("t2"), HistoryOperation::Commit),
            ],
        )
    }

    fn cycle_history() -> TransactionHistory {
        base_history(
            HistoryWorkload::Register,
            vec![
                event(
                    0,
                    0,
                    Some("t1"),
                    HistoryOperation::Begin {
                        mode: BeginMode::Concurrent,
                    },
                ),
                event(
                    1,
                    1,
                    Some("t2"),
                    HistoryOperation::Begin {
                        mode: BeginMode::Concurrent,
                    },
                ),
                event(
                    2,
                    2,
                    Some("t1"),
                    HistoryOperation::Read {
                        key: "y".to_owned(),
                        value: HistoryValue::Integer(0),
                        version: None,
                        source_transaction_id: None,
                    },
                ),
                event(
                    3,
                    3,
                    Some("t2"),
                    HistoryOperation::Read {
                        key: "x".to_owned(),
                        value: HistoryValue::Integer(0),
                        version: None,
                        source_transaction_id: None,
                    },
                ),
                event(
                    4,
                    4,
                    Some("t1"),
                    HistoryOperation::Write {
                        key: "x".to_owned(),
                        value: HistoryValue::Integer(1),
                        page_number: None,
                    },
                ),
                event(
                    5,
                    5,
                    Some("t2"),
                    HistoryOperation::Write {
                        key: "y".to_owned(),
                        value: HistoryValue::Integer(1),
                        page_number: None,
                    },
                ),
                event(6, 6, Some("t1"), HistoryOperation::Commit),
                event(7, 7, Some("t2"), HistoryOperation::Commit),
            ],
        )
    }

    #[test]
    fn accepts_valid_serial_history_without_si_shortcut() {
        let history = serial_history();
        let report = check_history(&history).expect("check serial history");
        assert_eq!(report.verdict, OracleVerdict::Serializable);
        assert!(report.anomalies.is_empty());
        assert!(!report.deterministic_replay_claim);
    }

    #[test]
    fn rejects_g2_write_skew_cycle_with_stable_minimal_witness() {
        let history = cycle_history();
        let first = check_history(&history).expect("check cycle history");
        let second = check_history(&history).expect("repeat cycle history");
        assert_eq!(first, second);
        assert_eq!(first.verdict, OracleVerdict::Rejected);
        assert_eq!(
            first.minimal_witness.as_ref().map(|item| item.kind),
            Some(AnomalyKind::G2Cycle)
        );
        assert_eq!(first.graph.cycle.as_ref().map(Vec::len), Some(2));
    }

    #[test]
    fn rejects_g1_cycle_from_declared_information_flow() {
        let mut history = cycle_history();
        for event in &mut history.events {
            if let HistoryOperation::Read {
                source_transaction_id,
                ..
            } = &mut event.operation
            {
                *source_transaction_id = Some(
                    if event.transaction_id.as_deref() == Some("t1") {
                        "t2"
                    } else {
                        "t1"
                    }
                    .to_owned(),
                );
            }
        }
        let report = check_history(&history).expect("check G1 history");
        assert!(
            report
                .anomalies
                .iter()
                .any(|item| item.kind == AnomalyKind::G1Cycle)
        );
        assert!(
            report
                .anomalies
                .iter()
                .any(|item| item.kind == AnomalyKind::DirtyRead)
        );
    }

    #[test]
    fn rejects_lost_update_and_first_committer_wins_violation() {
        let mut history = cycle_history();
        for event in &mut history.events {
            match &mut event.operation {
                HistoryOperation::Read { key, .. } | HistoryOperation::Write { key, .. } => {
                    *key = "x".to_owned();
                }
                _ => {}
            }
        }
        let report = check_history(&history).expect("check lost update");
        assert!(
            report
                .anomalies
                .iter()
                .any(|item| item.kind == AnomalyKind::LostUpdate)
        );
    }

    #[test]
    fn rejects_dirty_and_fractured_reads() {
        let mut history = serial_history();
        history.events.insert(
            5,
            event(
                5,
                5,
                Some("t2"),
                HistoryOperation::Read {
                    key: "x".to_owned(),
                    value: HistoryValue::Integer(2),
                    version: Some("v2".to_owned()),
                    source_transaction_id: Some("missing".to_owned()),
                },
            ),
        );
        for (index, event) in history.events.iter_mut().enumerate() {
            event.event_id = u64::try_from(index).expect("event id");
        }
        let report = check_history(&history).expect("check read anomalies");
        assert!(
            report
                .anomalies
                .iter()
                .any(|item| item.kind == AnomalyKind::DirtyRead)
        );
        assert!(
            report
                .anomalies
                .iter()
                .any(|item| item.kind == AnomalyKind::FracturedRead)
        );
    }

    #[test]
    fn terminal_states_are_explicit_and_indeterminate_is_inconclusive() {
        let terminal_operations = [
            HistoryOperation::Rollback {
                reason: "conflict".to_owned(),
            },
            HistoryOperation::Cancel {
                reason: "region cancelled".to_owned(),
            },
            HistoryOperation::Timeout { budget_ms: 10 },
            HistoryOperation::Indeterminate {
                reason: "process crashed".to_owned(),
            },
        ];
        let mut events = Vec::new();
        for (index, operation) in terminal_operations.into_iter().enumerate() {
            let txn = format!("t{index}");
            events.push(event(
                u64::try_from(events.len()).expect("event id"),
                u64::try_from(events.len()).expect("logical time"),
                Some(&txn),
                HistoryOperation::Begin {
                    mode: BeginMode::Concurrent,
                },
            ));
            events.push(event(
                u64::try_from(events.len()).expect("event id"),
                u64::try_from(events.len()).expect("logical time"),
                Some(&txn),
                operation,
            ));
        }
        let report = check_history(&base_history(HistoryWorkload::Register, events))
            .expect("check terminal variants");
        assert_eq!(report.verdict, OracleVerdict::Inconclusive);
        assert_eq!(report.excluded_transactions.rolled_back, ["t0"]);
        assert_eq!(report.excluded_transactions.cancelled, ["t1"]);
        assert_eq!(report.excluded_transactions.timed_out, ["t2"]);
        assert_eq!(report.excluded_transactions.indeterminate, ["t3"]);
    }

    #[test]
    fn workload_models_reject_invalid_final_state() {
        let workloads = [
            HistoryWorkload::ListAppend,
            HistoryWorkload::Bank {
                allow_negative: false,
            },
            HistoryWorkload::UniqueAllocation,
            HistoryWorkload::WriteSkew { minimum_sum: 2 },
        ];
        for workload in workloads {
            let mut history = base_history(workload.clone(), Vec::new());
            match workload {
                HistoryWorkload::ListAppend => {
                    history.initial_state.insert(
                        "list".to_owned(),
                        HistoryValue::List(vec![HistoryValue::Integer(1)]),
                    );
                    history
                        .final_state
                        .insert("list".to_owned(), HistoryValue::List(Vec::new()));
                }
                HistoryWorkload::Bank { .. } => {
                    history
                        .initial_state
                        .insert("a".to_owned(), HistoryValue::Integer(2));
                    history
                        .final_state
                        .insert("a".to_owned(), HistoryValue::Integer(-1));
                }
                HistoryWorkload::UniqueAllocation => {
                    history.events = vec![
                        event(
                            0,
                            0,
                            Some("t1"),
                            HistoryOperation::Begin {
                                mode: BeginMode::Concurrent,
                            },
                        ),
                        event(
                            1,
                            1,
                            Some("t1"),
                            HistoryOperation::Allocate {
                                namespace: "n".to_owned(),
                                value: "v".to_owned(),
                            },
                        ),
                        event(2, 2, Some("t1"), HistoryOperation::Commit),
                        event(
                            3,
                            3,
                            Some("t2"),
                            HistoryOperation::Begin {
                                mode: BeginMode::Concurrent,
                            },
                        ),
                        event(
                            4,
                            4,
                            Some("t2"),
                            HistoryOperation::Allocate {
                                namespace: "n".to_owned(),
                                value: "v".to_owned(),
                            },
                        ),
                        event(5, 5, Some("t2"), HistoryOperation::Commit),
                    ];
                }
                HistoryWorkload::WriteSkew { .. } => {
                    history
                        .final_state
                        .insert("a".to_owned(), HistoryValue::Integer(0));
                    history
                        .final_state
                        .insert("b".to_owned(), HistoryValue::Integer(1));
                }
                HistoryWorkload::Register => unreachable!(),
            }
            history.refresh_final_state_hash();
            let report = check_history(&history).expect("check workload invariant");
            assert_eq!(report.verdict, OracleVerdict::Rejected, "{workload:?}");
        }
    }

    #[test]
    fn restart_requires_concurrent_mode_on_reopen() {
        let mut history = base_history(
            HistoryWorkload::Register,
            vec![
                event(
                    0,
                    0,
                    None,
                    HistoryOperation::Crash {
                        crash_id: "c1".to_owned(),
                    },
                ),
                event(
                    1,
                    1,
                    None,
                    HistoryOperation::Restart {
                        crash_id: "c1".to_owned(),
                    },
                ),
            ],
        );
        let errors = history.validate();
        assert!(
            errors
                .iter()
                .any(|error| error.contains("reopen_concurrent_mode"))
        );
        history.reopen_concurrent_mode_enabled = Some(true);
        assert!(history.validate().is_empty());
    }

    #[test]
    fn observation_only_cannot_smuggle_replay_claim() {
        let mut history = serial_history();
        history.schedule.schedule_id = Some("fake".to_owned());
        assert!(
            history
                .validate()
                .iter()
                .any(|error| error.contains("observation-only"))
        );
    }

    #[test]
    fn strict_history_and_report_decoders_reject_corruption() {
        let history = serial_history();
        let report = check_history(&history).expect("check history");
        let history_json = history.to_json().expect("encode history");
        let report_json = report.to_json(&history).expect("encode report");
        assert_eq!(
            TransactionHistory::from_json_strict(&history_json).expect("decode"),
            history
        );
        assert_eq!(
            SerializabilityReport::from_json_strict(&report_json, &history).expect("decode"),
            report
        );
        assert!(
            TransactionHistory::from_json_strict(&history_json[..history_json.len() / 2]).is_err()
        );
        let corrupt = report_json.replace(&report.report_sha256, &"0".repeat(64));
        assert!(SerializabilityReport::from_json_strict(&corrupt, &history).is_err());
    }

    #[test]
    fn rejected_history_round_trips_through_canonical_failure_bundle() {
        let history = cycle_history();
        let report = check_history(&history).expect("check cycle");
        let bundle = build_serializability_failure_bundle(
            &history,
            &report,
            SerializabilityBundleContext {
                bundle_id: "fb-run-ssi-1-1".to_owned(),
                created_at: "2026-08-05T00:00:00Z".to_owned(),
                test_name: "cycle_golden".to_owned(),
                script_path: Some(
                    "crates/fsqlite-harness/src/serializability_oracle.rs".to_owned(),
                ),
                repro_command: "cargo test -p fsqlite-harness cycle_golden".to_owned(),
                environment: EnvironmentInfo::new("0123456789abcdef", "nightly", "test"),
            },
        )
        .expect("build bundle");
        let (decoded_history, decoded_report) =
            validate_serializability_failure_bundle(&bundle).expect("validate bundle");
        assert_eq!(decoded_history, history);
        assert_eq!(decoded_report, report);
        let artifact =
            SerializabilityReplayArtifact::from_history(history.clone(), Some(bundle.clone()))
                .expect("build replay artifact");
        let encoded = artifact.to_json().expect("encode replay artifact");
        let decoded = SerializabilityReplayArtifact::from_json_strict(&encoded)
            .expect("decode replay artifact");
        assert_eq!(
            replay_serializability_oracle(&decoded).expect("replay oracle"),
            report
        );
        assert!(
            SerializabilityReplayArtifact::from_json_strict(&encoded[..encoded.len() / 2]).is_err()
        );
        let unknown = encoded.replacen(
            "\"schema_version\"",
            "\"unknown\":true,\"schema_version\"",
            1,
        );
        assert!(SerializabilityReplayArtifact::from_json_strict(&unknown).is_err());
        let mut corrupt = bundle;
        corrupt
            .state_snapshots
            .insert(HISTORY_SNAPSHOT_KEY.to_owned(), "{".to_owned());
        corrupt.content_hash = corrupt.deterministic_bundle_hash();
        assert!(validate_serializability_failure_bundle(&corrupt).is_err());
    }

    type TransactionProjection = (String, u64, u64, BTreeSet<String>, BTreeSet<String>);

    proptest! {
        #[test]
        fn graph_result_is_stable_under_input_order(transactions in prop::collection::vec(
            ("[a-z]{1,4}", 0_u64..20, 20_u64..40, prop::collection::btree_set("[a-c]", 0..3), prop::collection::btree_set("[a-c]", 0..3)),
            0..12,
        )) {
            let make = |values: &[TransactionProjection]| {
                values.iter().enumerate().map(|(index, (name, start, commit, reads, writes))| CommittedTransaction {
                    transaction_id: format!("{name}-{index}"),
                    start_order: *start,
                    commit_order: *commit,
                    read_set: reads.clone(),
                    write_set: writes.clone(),
                    read_sources: BTreeMap::new(),
                }).collect::<Vec<_>>()
            };
            let mut projected = make(&transactions);
            let expected = build_serialization_graph(&projected);
            projected.reverse();
            prop_assert_eq!(build_serialization_graph(&projected), expected);
        }
    }
}
