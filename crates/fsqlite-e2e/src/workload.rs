//! Deterministic workload generation with seeded RNG.
//!
//! This module is deliberately **pure computation** (no I/O, no `Cx`) so it can
//! be used in both unit tests and higher-level harness orchestration.

use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

use crate::oplog::{
    ConcurrencyModel, ExpectedResult, OpKind, OpLog, OpLogHeader, OpRecord, RngSpec,
};

/// Policy governing how multi-worker transaction batches should be interpreted
/// by an executor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitOrderPolicy {
    /// Executors should follow a deterministic commit order (typically op_id / round-robin).
    Deterministic,
    /// Executors may run workers as fast as they can (workloads should be commutative if used).
    Free,
    /// Executors must synchronize workers after each transaction batch.
    Barrier,
}

impl CommitOrderPolicy {
    fn as_str(self) -> &'static str {
        match self {
            Self::Deterministic => "deterministic",
            Self::Free => "free",
            Self::Barrier => "barrier",
        }
    }
}

/// Operation mix weights for the random portion of a workload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OperationMix {
    pub insert_weight: u32,
    pub update_weight: u32,
    pub delete_weight: u32,
    pub select_weight: u32,
}

impl OperationMix {
    #[must_use]
    pub fn total_weight(self) -> u32 {
        self.insert_weight
            .saturating_add(self.update_weight)
            .saturating_add(self.delete_weight)
            .saturating_add(self.select_weight)
    }
}

impl Default for OperationMix {
    fn default() -> Self {
        Self {
            insert_weight: 60,
            update_weight: 15,
            delete_weight: 5,
            select_weight: 20,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TemplateOp {
    Insert,
    Update,
    Delete,
    Select,
}

fn choose_template_op(rng: &mut StdRng, mix: OperationMix) -> TemplateOp {
    let total = mix.total_weight().max(1);
    let mut x = rng.random_range(0..total);
    if x < mix.insert_weight {
        return TemplateOp::Insert;
    }
    x = x.saturating_sub(mix.insert_weight);
    if x < mix.update_weight {
        return TemplateOp::Update;
    }
    x = x.saturating_sub(mix.update_weight);
    if x < mix.delete_weight {
        return TemplateOp::Delete;
    }
    TemplateOp::Select
}

/// A single table schema definition used by the generator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableSpec {
    pub name: String,
    pub create_sql: String,
}

impl TableSpec {
    /// A small, fixed schema that exercises TEXT and REAL values with an INTEGER PK.
    #[must_use]
    pub fn simple(name: impl Into<String>) -> Self {
        let name = name.into();
        Self {
            create_sql: format!(
                "CREATE TABLE IF NOT EXISTS {name} (id INTEGER PRIMARY KEY, val TEXT, num REAL)"
            ),
            name,
        }
    }

    /// Schema with secondary indexes — exercises B-tree maintenance during mutations.
    #[must_use]
    pub fn with_index(name: impl Into<String>) -> Self {
        let name = name.into();
        Self {
            create_sql: format!(
                "CREATE TABLE IF NOT EXISTS {name} (\
                    id INTEGER PRIMARY KEY, \
                    category TEXT NOT NULL, \
                    val TEXT, \
                    num REAL, \
                    created_at INTEGER DEFAULT 0)"
            ),
            name,
        }
    }

    /// DDL statements that create secondary indexes for a [`TableSpec::with_index`] table.
    ///
    /// Callers should emit these as separate `OpKind::Sql` records after the CREATE TABLE.
    #[must_use]
    pub fn index_ddl(table_name: &str) -> Vec<String> {
        vec![
            format!(
                "CREATE INDEX IF NOT EXISTS idx_{table_name}_category \
                    ON {table_name} (category)"
            ),
            format!(
                "CREATE INDEX IF NOT EXISTS idx_{table_name}_num \
                    ON {table_name} (num)"
            ),
            format!(
                "CREATE INDEX IF NOT EXISTS idx_{table_name}_created \
                    ON {table_name} (created_at)"
            ),
        ]
    }
}

/// Configuration controlling workload generation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkloadConfig {
    /// Identifier linking this workload to a golden/work fixture.
    pub fixture_id: String,
    /// Base seed.
    pub seed: u64,
    /// Total number of non-setup operations to generate across all workers.
    pub num_operations: usize,
    /// Number of workers (1 = serial).
    pub worker_count: u16,
    /// Number of operations per transaction before committing (per worker).
    pub transaction_size: u32,
    /// Commit ordering policy.
    pub commit_order_policy: CommitOrderPolicy,
    /// Weighted operation mix for the randomized portion.
    pub operation_mix: OperationMix,
    /// Table schemas to target.
    pub tables: Vec<TableSpec>,
}

impl Default for WorkloadConfig {
    fn default() -> Self {
        Self {
            fixture_id: "generated".to_owned(),
            seed: 42,
            num_operations: 200,
            worker_count: 1,
            transaction_size: 50,
            commit_order_policy: CommitOrderPolicy::Deterministic,
            operation_mix: OperationMix::default(),
            tables: vec![TableSpec::simple("t0")],
        }
    }
}

#[derive(Debug)]
struct WorkerState {
    rng: StdRng,
    next_key: i64,
    live_keys_per_table: Vec<Vec<i64>>,
}

impl WorkerState {
    fn new(worker: u16, seed: u64, table_count: usize) -> Self {
        // Disjoint key ranges keep cross-worker conflicts rare and reduce flaky
        // comparisons when the executor runs with real concurrency.
        const KEY_STRIDE: i64 = 1_000_000;
        let base = i64::from(worker).saturating_mul(KEY_STRIDE);
        Self {
            rng: StdRng::seed_from_u64(derive_worker_seed(seed, worker)),
            next_key: base,
            live_keys_per_table: vec![Vec::new(); table_count],
        }
    }

    fn gen_text(&mut self) -> String {
        let len = self.rng.random_range(1..=24);
        (0..len)
            .map(|_| (b'a' + self.rng.random_range(0..26)) as char)
            .collect()
    }

    fn gen_real(&mut self) -> f64 {
        self.rng.random_range(-1000.0..=1000.0)
    }

    fn insert_op(&mut self, table: &str, table_idx: usize) -> OpKind {
        self.next_key = self.next_key.saturating_add(1);
        let key = self.next_key;
        self.live_keys_per_table[table_idx].push(key);
        OpKind::Insert {
            table: table.to_owned(),
            key,
            values: vec![
                ("val".to_owned(), self.gen_text()),
                ("num".to_owned(), format!("{:.6}", self.gen_real())),
            ],
        }
    }

    fn choose_live_key(&mut self, table_idx: usize) -> Option<i64> {
        let keys = &self.live_keys_per_table[table_idx];
        if keys.is_empty() {
            return None;
        }
        let idx = self.rng.random_range(0..keys.len());
        Some(keys[idx])
    }

    fn update_op(&mut self, table: &str, key: i64) -> OpKind {
        OpKind::Update {
            table: table.to_owned(),
            key,
            values: vec![
                ("val".to_owned(), self.gen_text()),
                ("num".to_owned(), format!("{:.6}", self.gen_real())),
            ],
        }
    }

    fn delete_sql(&mut self, table: &str, table_idx: usize, key: i64) -> OpKind {
        if let Some(pos) = self.live_keys_per_table[table_idx]
            .iter()
            .position(|k| *k == key)
        {
            self.live_keys_per_table[table_idx].swap_remove(pos);
        }
        OpKind::Sql {
            statement: format!("DELETE FROM {table} WHERE id = {key}"),
        }
    }

    fn select_sql(&mut self, table: &str, table_idx: usize) -> OpKind {
        // Prefer selecting a live key if we have one, otherwise fall back to COUNT(*).
        if let Some(key) = self.choose_live_key(table_idx) {
            OpKind::Sql {
                statement: format!("SELECT id, val, num FROM {table} WHERE id = {key}"),
            }
        } else {
            OpKind::Sql {
                statement: format!("SELECT COUNT(*) FROM {table}"),
            }
        }
    }
}

/// Deterministic workload generator backed by seeded PRNG streams.
#[derive(Debug)]
pub struct WorkloadGenerator {
    cfg: WorkloadConfig,
    workers: Vec<WorkerState>,
}

impl WorkloadGenerator {
    #[must_use]
    pub fn new(cfg: WorkloadConfig) -> Self {
        let WorkloadConfig {
            fixture_id,
            seed,
            num_operations,
            worker_count,
            transaction_size,
            commit_order_policy,
            operation_mix,
            mut tables,
        } = cfg;

        let worker_count = worker_count.max(1);
        let transaction_size = transaction_size.max(1);

        if tables.is_empty() {
            tables.push(TableSpec::simple("t0"));
        }
        let table_count = tables.len();

        let workers = (0..worker_count)
            .map(|w| WorkerState::new(w, seed, table_count))
            .collect();

        Self {
            cfg: WorkloadConfig {
                fixture_id,
                seed,
                num_operations,
                worker_count,
                transaction_size,
                commit_order_policy,
                operation_mix,
                tables,
            },
            workers,
        }
    }

    /// Generate a full `OpLog` (header + ordered records).
    ///
    /// The output is deterministic given the same config.
    #[must_use]
    pub fn generate(&mut self) -> OpLog {
        let worker_count = self.cfg.worker_count.max(1);
        let transaction_size = self.cfg.transaction_size.max(1);
        let header = OpLogHeader {
            fixture_id: self.cfg.fixture_id.clone(),
            seed: self.cfg.seed,
            rng: RngSpec::default(),
            concurrency: ConcurrencyModel {
                worker_count,
                transaction_size,
                commit_order_policy: self.cfg.commit_order_policy.as_str().to_owned(),
            },
            preset: None,
        };

        let per_worker_ops = self.generate_per_worker_ops(worker_count, transaction_size);
        let records = interleave_ops(worker_count, self.cfg.commit_order_policy, per_worker_ops);
        OpLog { header, records }
    }

    /// Generate per-worker record batches (grouped by worker index).
    ///
    /// This is convenient for executors that want to maintain one queue per worker.
    #[must_use]
    pub fn generate_concurrent(&mut self) -> Vec<Vec<OpRecord>> {
        let log = self.generate();
        let mut per_worker = vec![Vec::new(); usize::from(log.header.concurrency.worker_count)];
        for rec in log.records {
            per_worker[usize::from(rec.worker)].push(rec);
        }
        per_worker
    }

    fn generate_per_worker_ops(
        &mut self,
        worker_count: u16,
        transaction_size: u32,
    ) -> Vec<Vec<OpKind>> {
        let total_ops = self.cfg.num_operations;
        let wc = usize::from(worker_count);
        let base = total_ops / wc;
        let rem = total_ops % wc;

        let mut out = Vec::with_capacity(wc);
        for w in 0..worker_count {
            let extra = usize::from(w) < rem;
            let budget = base + usize::from(extra);
            out.push(self.generate_one_worker_ops(w, budget, transaction_size));
        }

        out
    }

    fn generate_one_worker_ops(
        &mut self,
        worker: u16,
        budget: usize,
        transaction_size: u32,
    ) -> Vec<OpKind> {
        let tables = self.cfg.tables.clone();
        let table_count = tables.len();
        let ws = &mut self.workers[usize::from(worker)];

        // Setup statements (DDL). We do not count these against the operation budget.
        let mut setup = Vec::with_capacity(table_count);
        for t in &tables {
            setup.push(OpKind::Sql {
                statement: t.create_sql.clone(),
            });
        }

        // Build the operation list (excluding Begin/Commit).
        let mut ops: Vec<OpKind> = Vec::with_capacity(budget);

        // Seed at least one insert per table when possible, so UPDATE/DELETE have live keys.
        let mut remaining = budget;
        for (idx, t) in tables.iter().enumerate() {
            if remaining == 0 {
                break;
            }
            ops.push(ws.insert_op(&t.name, idx));
            remaining -= 1;
        }

        while remaining > 0 {
            let table_idx = ws.rng.random_range(0..table_count);
            let table = &tables[table_idx].name;
            let tmpl = choose_template_op(&mut ws.rng, self.cfg.operation_mix);

            let op = match tmpl {
                TemplateOp::Insert => ws.insert_op(table, table_idx),
                TemplateOp::Update => {
                    if let Some(key) = ws.choose_live_key(table_idx) {
                        ws.update_op(table, key)
                    } else {
                        ws.insert_op(table, table_idx)
                    }
                }
                TemplateOp::Delete => {
                    if let Some(key) = ws.choose_live_key(table_idx) {
                        ws.delete_sql(table, table_idx, key)
                    } else {
                        ws.insert_op(table, table_idx)
                    }
                }
                TemplateOp::Select => ws.select_sql(table, table_idx),
            };

            ops.push(op);
            remaining -= 1;
        }

        // Wrap operations into transactions.
        let mut seq = setup;
        for chunk in ops.chunks(transaction_size as usize) {
            seq.push(OpKind::Begin);
            seq.extend_from_slice(chunk);
            seq.push(OpKind::Commit);
        }
        seq
    }
}

fn interleave_ops(
    worker_count: u16,
    policy: CommitOrderPolicy,
    per_worker_ops: Vec<Vec<OpKind>>,
) -> Vec<OpRecord> {
    let wc = usize::from(worker_count);
    let mut cursors = vec![0usize; wc];
    let mut records = Vec::new();
    let mut op_id: u64 = 0;

    match policy {
        CommitOrderPolicy::Barrier => {
            let mut batches: Vec<Vec<Vec<OpKind>>> =
                per_worker_ops.into_iter().map(split_into_batches).collect();
            let mut batch_idx = 0usize;
            loop {
                let mut any = false;
                for worker in 0..worker_count {
                    let w = usize::from(worker);
                    if batch_idx < batches[w].len() {
                        any = true;
                        for kind in std::mem::take(&mut batches[w][batch_idx]) {
                            records.push(OpRecord {
                                op_id,
                                worker,
                                kind,
                                expected: None,
                            });
                            op_id += 1;
                        }
                    }
                }
                if !any {
                    break;
                }
                batch_idx += 1;
            }
        }
        CommitOrderPolicy::Deterministic | CommitOrderPolicy::Free => loop {
            let mut any = false;
            for worker in 0..worker_count {
                let w = usize::from(worker);
                let ops = &per_worker_ops[w];
                let idx = cursors[w];
                if idx < ops.len() {
                    any = true;
                    records.push(OpRecord {
                        op_id,
                        worker,
                        kind: ops[idx].clone(),
                        expected: None,
                    });
                    op_id += 1;
                    cursors[w] += 1;
                }
            }
            if !any {
                break;
            }
        },
    }

    records
}

fn split_into_batches(ops: Vec<OpKind>) -> Vec<Vec<OpKind>> {
    let mut batches: Vec<Vec<OpKind>> = Vec::new();
    let mut cur: Vec<OpKind> = Vec::new();
    for op in ops {
        cur.push(op);
        if matches!(cur.last(), Some(OpKind::Commit)) {
            batches.push(cur);
            cur = Vec::new();
        }
    }
    if !cur.is_empty() {
        batches.push(cur);
    }
    batches
}

fn derive_worker_seed(seed: u64, worker: u16) -> u64 {
    // SplitMix64-style mixing; deterministic and cheap.
    let mut x = seed ^ (u64::from(worker) << 1);
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

/// Schema version for stateful operation-plan artifacts.
pub const STATEFUL_PLAN_SCHEMA_VERSION: &str = "stateful-operation-plan.v1";

const STATEFUL_TABLE: &str = "stateful_kv";
const STATEFUL_REQUIRED_LANE: &str = "pager_backed_required";

/// Configuration for the deterministic stateful operation-plan campaign.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatefulPlanConfig {
    /// Artifact and fixture identifier.
    pub fixture_id: String,
    /// Base seed used for deterministic value generation.
    pub seed: u64,
    /// Upper bound for emitted model steps. Values below the campaign minimum
    /// are normalized upward so generated plans remain semantically complete.
    pub max_steps: usize,
    /// Required execution lane attached to every step.
    pub required_lane: String,
    /// Canonical feature IDs covered by the generated plan.
    pub feature_ids: Vec<String>,
    /// Whether the plan includes the close/reopen boundary.
    pub include_close_reopen: bool,
}

impl Default for StatefulPlanConfig {
    fn default() -> Self {
        Self {
            fixture_id: "stateful-op-plan".to_owned(),
            seed: 0x0054_5552_534F_0020,
            max_steps: 18,
            required_lane: STATEFUL_REQUIRED_LANE.to_owned(),
            feature_ids: vec![
                "SURF-SQL-CORE-001".to_owned(),
                "SURF-TXN-MVCC-CONCURRENT-006".to_owned(),
                "SURF-WAL-CRASH-RECOVERY-008".to_owned(),
            ],
            include_close_reopen: true,
        }
    }
}

/// Top-level metadata retained with a stateful operation plan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatefulPlanMetadata {
    /// Artifact schema version.
    pub schema_version: String,
    /// Fixture identifier.
    pub fixture_id: String,
    /// Base seed.
    pub seed: u64,
    /// Normalized step bound used by the generator.
    pub max_steps: usize,
    /// Required execution lane.
    pub required_lane: String,
    /// Feature IDs covered by this plan.
    pub feature_ids: Vec<String>,
    /// Stable hash of generation inputs.
    pub profile_hash: String,
    /// Owning bead for traceability.
    pub owner_bead: String,
}

/// Deterministic stateful plan plus final independent model snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatefulOperationPlan {
    /// Stable plan metadata.
    pub metadata: StatefulPlanMetadata,
    /// Ordered model steps.
    pub steps: Vec<StatefulPlanStep>,
    /// Final model state after all steps.
    pub final_model: StatefulModelSnapshot,
}

impl StatefulOperationPlan {
    /// Validate preconditions, transitions, postconditions, and final state.
    ///
    /// # Errors
    ///
    /// Returns a diagnostic string when a step is internally inconsistent or an
    /// artifact was hand-edited into an invalid model state.
    pub fn validate(&self) -> Result<StatefulPlanAudit, String> {
        let mut model = StatefulModel::default();
        let mut projected_record_count = 0_usize;
        let mut close_reopen_count = 0_usize;

        for step in &self.steps {
            for condition in &step.preconditions {
                condition.verify(&model, step)?;
            }

            let transition = model.apply(&step.operation)?;
            if transition != step.expected_transition {
                return Err(format!(
                    "stateful_step_transition_mismatch step_id={} expected={:?} actual={:?}",
                    step.step_id, step.expected_transition, transition
                ));
            }

            for condition in &step.postconditions {
                condition.verify(&model, step)?;
            }

            if step.operation.to_op_kind().is_some() {
                projected_record_count += 1;
            }
            if matches!(step.operation, StatefulOperation::CloseReopen) {
                close_reopen_count += 1;
            }
        }

        let final_model = model.snapshot();
        if final_model != self.final_model {
            return Err(format!(
                "stateful_final_model_mismatch expected={:?} actual={:?}",
                self.final_model, final_model
            ));
        }

        Ok(StatefulPlanAudit {
            schema_version: STATEFUL_PLAN_SCHEMA_VERSION.to_owned(),
            fixture_id: self.metadata.fixture_id.clone(),
            step_count: self.steps.len(),
            projected_record_count,
            close_reopen_count,
            final_model_hash: final_model
                .stable_hash()
                .map_err(|error| format!("stateful_final_model_hash_error {error}"))?,
        })
    }

    /// Project executable steps into the existing OpLog format.
    ///
    /// The non-SQL close/reopen boundary remains in the stateful artifact and is
    /// intentionally not encoded as fake SQL.
    ///
    /// # Errors
    ///
    /// Returns an error if the stateful plan fails model validation.
    pub fn to_oplog(&self) -> Result<OpLog, String> {
        self.validate()?;

        let mut op_id = 0_u64;
        let mut records = Vec::new();
        for step in &self.steps {
            if let Some((kind, expected)) = step.operation.to_op_kind() {
                records.push(OpRecord {
                    op_id,
                    worker: 0,
                    kind,
                    expected,
                });
                op_id = op_id.saturating_add(1);
            }
        }

        Ok(OpLog {
            header: OpLogHeader {
                fixture_id: self.metadata.fixture_id.clone(),
                seed: self.metadata.seed,
                rng: RngSpec::default(),
                concurrency: ConcurrencyModel {
                    worker_count: 1,
                    transaction_size: 1,
                    commit_order_policy: "deterministic".to_owned(),
                },
                preset: Some("stateful-operation-plan".to_owned()),
            },
            records,
        })
    }

    /// Serialize the plan using the stable serde field order.
    ///
    /// # Errors
    ///
    /// Returns a serde error if JSON serialization fails.
    pub fn canonical_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }

    /// Compute a stable hash over the canonical JSON artifact.
    ///
    /// # Errors
    ///
    /// Returns a serde error if JSON serialization fails.
    pub fn stable_artifact_hash(&self) -> Result<String, serde_json::Error> {
        self.canonical_json()
            .map(|json| sha256_hex(json.as_bytes()))
    }

    /// Build stable SQL, trace, and metadata artifacts for public replay.
    ///
    /// # Errors
    ///
    /// Returns an error if the plan is invalid or artifact serialization fails.
    pub fn to_sql_artifact(&self) -> Result<StatefulSqlArtifact, String> {
        let audit = self.validate()?;
        let plan_hash = self
            .stable_artifact_hash()
            .map_err(|error| format!("stateful_plan_hash_error {error}"))?;
        let final_model_hash = self
            .final_model
            .stable_hash()
            .map_err(|error| format!("stateful_final_model_hash_error {error}"))?;
        let mut schema = Vec::new();
        let mut workload = Vec::new();
        let mut trace = Vec::with_capacity(self.steps.len());

        for step in &self.steps {
            let executable_sql = step.operation.to_sql_statement();
            if let Some(sql) = &executable_sql {
                if matches!(step.operation, StatefulOperation::CreateSchema) {
                    schema.push(sql.clone());
                } else {
                    workload.push(sql.clone());
                }
            }
            trace.push(StatefulTraceEntry {
                step_id: step.step_id,
                origin_seed: step.origin_seed,
                required_lane: step.required_lane.clone(),
                feature_ids: step.feature_ids.clone(),
                operation: step.operation.clone(),
                expected_transition: step.expected_transition.clone(),
                preconditions: step.preconditions.clone(),
                postconditions: step.postconditions.clone(),
                executable_sql,
            });
        }

        let metadata = StatefulArtifactMetadata {
            schema_version: self.metadata.schema_version.clone(),
            fixture_id: self.metadata.fixture_id.clone(),
            owner_bead: self.metadata.owner_bead.clone(),
            seed: self.metadata.seed,
            profile_hash: self.metadata.profile_hash.clone(),
            required_lane: self.metadata.required_lane.clone(),
            feature_ids: self.metadata.feature_ids.clone(),
            plan_hash,
            final_model_hash,
            audit,
            supported_statuses: StatefulExecutionStatus::fail_closed_statuses(),
        };
        let sql_text = stable_sql_text(&schema, &workload);
        let trace_json = serde_json::to_string(&trace)
            .map_err(|error| format!("stateful_trace_json_error {error}"))?;
        let metadata_json = serde_json::to_string(&metadata)
            .map_err(|error| format!("stateful_metadata_json_error {error}"))?;

        Ok(StatefulSqlArtifact {
            schema,
            workload,
            trace,
            metadata,
            sql_hash: sha256_hex(sql_text.as_bytes()),
            trace_hash: sha256_hex(trace_json.as_bytes()),
            metadata_hash: sha256_hex(metadata_json.as_bytes()),
        })
    }
}

/// One independently modeled stateful step.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatefulPlanStep {
    /// Deterministic step ID.
    pub step_id: u64,
    /// Conditions that must hold before the operation is applied.
    pub preconditions: Vec<StatefulCondition>,
    /// Operation payload.
    pub operation: StatefulOperation,
    /// Expected independent-model transition.
    pub expected_transition: StatefulTransition,
    /// Conditions that must hold after the transition.
    pub postconditions: Vec<StatefulCondition>,
    /// Required execution lane.
    pub required_lane: String,
    /// Feature IDs exercised by the step.
    pub feature_ids: Vec<String>,
    /// Per-step deterministic seed lineage.
    pub origin_seed: u64,
}

/// Stateful operation vocabulary owned by the e2e workload model.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StatefulOperation {
    /// Create the canonical table.
    CreateSchema,
    /// Insert a key/value row.
    Insert { key: i64, value: String },
    /// Update an existing key.
    Update { key: i64, value: String },
    /// Delete an existing key.
    Delete { key: i64 },
    /// Begin a transaction.
    Begin,
    /// Commit the active transaction.
    Commit,
    /// Roll back the active transaction.
    Rollback,
    /// Create a savepoint.
    Savepoint { name: String },
    /// Roll back to a savepoint.
    RollbackTo { name: String },
    /// Release a savepoint.
    Release { name: String },
    /// Observe the table row count.
    SelectCount,
    /// Run a supported integrity check.
    IntegrityCheck,
    /// Close and reopen the file-backed database boundary.
    CloseReopen,
}

impl StatefulOperation {
    /// Render this operation as SQL for public replay, when it has a SQL form.
    #[must_use]
    pub fn to_sql_statement(&self) -> Option<String> {
        let statement = match self {
            Self::CreateSchema => format!(
                "CREATE TABLE IF NOT EXISTS {STATEFUL_TABLE} (\
                 id INTEGER PRIMARY KEY, val TEXT NOT NULL, num REAL DEFAULT 0)"
            ),
            Self::Insert { key, value } => format!(
                "INSERT INTO {STATEFUL_TABLE} (id, val, num) VALUES ({key}, {}, {key})",
                stateful_sql_literal(value)
            ),
            Self::Update { key, value } => format!(
                "UPDATE {STATEFUL_TABLE} SET val = {} WHERE id = {key}",
                stateful_sql_literal(value)
            ),
            Self::Delete { key } => {
                format!("DELETE FROM {STATEFUL_TABLE} WHERE id = {key}")
            }
            Self::Begin => "BEGIN".to_owned(),
            Self::Commit => "COMMIT".to_owned(),
            Self::Rollback => "ROLLBACK".to_owned(),
            Self::Savepoint { name } => format!("SAVEPOINT {name}"),
            Self::RollbackTo { name } => format!("ROLLBACK TO {name}"),
            Self::Release { name } => format!("RELEASE {name}"),
            Self::SelectCount => format!("SELECT COUNT(*) FROM {STATEFUL_TABLE}"),
            Self::IntegrityCheck => "PRAGMA integrity_check".to_owned(),
            Self::CloseReopen => return None,
        };
        Some(statement)
    }

    fn to_op_kind(&self) -> Option<(OpKind, Option<ExpectedResult>)> {
        let projected = match self {
            Self::CreateSchema => (
                OpKind::Sql {
                    statement: self.to_sql_statement()?,
                },
                None,
            ),
            Self::Insert { key, value } => (
                OpKind::Insert {
                    table: STATEFUL_TABLE.to_owned(),
                    key: *key,
                    values: vec![
                        ("val".to_owned(), value.clone()),
                        ("num".to_owned(), key.to_string()),
                    ],
                },
                Some(ExpectedResult::AffectedRows(1)),
            ),
            Self::Update { key, value } => (
                OpKind::Update {
                    table: STATEFUL_TABLE.to_owned(),
                    key: *key,
                    values: vec![("val".to_owned(), value.clone())],
                },
                Some(ExpectedResult::AffectedRows(1)),
            ),
            Self::Delete { .. } => (
                OpKind::Sql {
                    statement: self.to_sql_statement()?,
                },
                Some(ExpectedResult::AffectedRows(1)),
            ),
            Self::Begin => (OpKind::Begin, None),
            Self::Commit => (OpKind::Commit, None),
            Self::Rollback => (OpKind::Rollback, None),
            Self::Savepoint { .. } => (
                OpKind::Sql {
                    statement: self.to_sql_statement()?,
                },
                None,
            ),
            Self::RollbackTo { .. } => (
                OpKind::Sql {
                    statement: self.to_sql_statement()?,
                },
                None,
            ),
            Self::Release { .. } => (
                OpKind::Sql {
                    statement: self.to_sql_statement()?,
                },
                None,
            ),
            Self::SelectCount => (
                OpKind::Sql {
                    statement: self.to_sql_statement()?,
                },
                Some(ExpectedResult::RowCount(1)),
            ),
            Self::IntegrityCheck => (
                OpKind::Sql {
                    statement: self.to_sql_statement()?,
                },
                Some(ExpectedResult::RowCount(1)),
            ),
            Self::CloseReopen => return None,
        };

        Some(projected)
    }
}

/// Expected independent-model transition for a step.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StatefulTransition {
    /// Schema was created.
    SchemaCreated,
    /// Row was inserted.
    RowInserted { key: i64 },
    /// Row was updated.
    RowUpdated { key: i64 },
    /// Row was deleted.
    RowDeleted { key: i64 },
    /// Transaction began.
    TransactionBegun,
    /// Transaction committed.
    TransactionCommitted,
    /// Transaction rolled back.
    TransactionRolledBack,
    /// Savepoint was created.
    SavepointCreated { name: String },
    /// State was restored to a savepoint.
    SavepointRolledBack { name: String },
    /// Savepoint was released.
    SavepointReleased { name: String },
    /// Read-only observation occurred.
    Observation { row_count: usize },
    /// Integrity check completed.
    IntegrityChecked,
    /// Close/reopen preserved committed state.
    CloseReopen { row_count: usize },
}

/// Preconditions and postconditions checked by the independent model.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StatefulCondition {
    /// Schema exists.
    SchemaExists,
    /// Row exists.
    RowExists { key: i64 },
    /// Row is absent.
    RowAbsent { key: i64 },
    /// The model is in a transaction.
    InTransaction,
    /// The model is not in a transaction.
    NotInTransaction,
    /// Savepoint exists.
    SavepointExists { name: String },
    /// Current committed/logical row count matches.
    RowCount { count: usize },
    /// Integrity is clean according to the model.
    IntegrityClean,
    /// Step carries the expected required lane.
    LaneObserved { lane: String },
}

impl StatefulCondition {
    fn verify(&self, model: &StatefulModel, step: &StatefulPlanStep) -> Result<(), String> {
        let satisfied = match self {
            Self::SchemaExists => model.schema_created,
            Self::RowExists { key } => model.rows.contains_key(key),
            Self::RowAbsent { key } => !model.rows.contains_key(key),
            Self::InTransaction => model.transaction_snapshot.is_some(),
            Self::NotInTransaction => model.transaction_snapshot.is_none(),
            Self::SavepointExists { name } => model.savepoints.contains_key(name),
            Self::RowCount { count } => model.rows.len() == *count,
            Self::IntegrityClean => model.schema_created,
            Self::LaneObserved { lane } => step.required_lane == *lane,
        };

        if satisfied {
            Ok(())
        } else {
            Err(format!(
                "stateful_condition_failed step_id={} condition={self:?}",
                step.step_id
            ))
        }
    }
}

/// Stable model snapshot retained in plan artifacts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatefulModelSnapshot {
    /// Whether schema creation has occurred.
    pub schema_created: bool,
    /// Stable row state.
    pub rows: Vec<(i64, String)>,
    /// Whether a transaction is active.
    pub in_transaction: bool,
    /// Active savepoint names.
    pub savepoints: Vec<String>,
}

impl StatefulModelSnapshot {
    /// Compute a stable hash over the model snapshot.
    ///
    /// # Errors
    ///
    /// Returns a serde error if JSON serialization fails.
    pub fn stable_hash(&self) -> Result<String, serde_json::Error> {
        serde_json::to_vec(self).map(|bytes| sha256_hex(&bytes))
    }
}

/// Validation summary for a stateful plan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatefulPlanAudit {
    /// Artifact schema version.
    pub schema_version: String,
    /// Fixture identifier.
    pub fixture_id: String,
    /// Number of stateful steps.
    pub step_count: usize,
    /// Number of steps projected into executable OpLog records.
    pub projected_record_count: usize,
    /// Number of close/reopen boundaries retained in the stateful artifact.
    pub close_reopen_count: usize,
    /// Stable hash of final independent model state.
    pub final_model_hash: String,
}

/// Stable SQL, trace, and metadata projection for a stateful plan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatefulSqlArtifact {
    /// Schema statements run before the workload in public differential replay.
    pub schema: Vec<String>,
    /// Workload statements run after schema setup.
    pub workload: Vec<String>,
    /// Step-by-step stable trace, including non-SQL boundaries.
    pub trace: Vec<StatefulTraceEntry>,
    /// Stable metadata retained with the artifact.
    pub metadata: StatefulArtifactMetadata,
    /// Stable hash of schema/workload SQL text.
    pub sql_hash: String,
    /// Stable hash of the trace JSON.
    pub trace_hash: String,
    /// Stable hash of the metadata JSON.
    pub metadata_hash: String,
}

/// Stable per-step trace entry for stateful replay artifacts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatefulTraceEntry {
    /// Step identifier.
    pub step_id: u64,
    /// Deterministic seed lineage for this step.
    pub origin_seed: u64,
    /// Required execution lane for the step.
    pub required_lane: String,
    /// Feature IDs exercised by the step.
    pub feature_ids: Vec<String>,
    /// Original model operation.
    pub operation: StatefulOperation,
    /// Expected independent-model transition.
    pub expected_transition: StatefulTransition,
    /// Preconditions checked before applying the operation.
    pub preconditions: Vec<StatefulCondition>,
    /// Postconditions checked after applying the operation.
    pub postconditions: Vec<StatefulCondition>,
    /// SQL emitted for public replay, absent for non-SQL boundaries.
    pub executable_sql: Option<String>,
}

/// Stable metadata for a stateful SQL artifact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatefulArtifactMetadata {
    /// Artifact schema version.
    pub schema_version: String,
    /// Fixture identifier.
    pub fixture_id: String,
    /// Owning bead for traceability.
    pub owner_bead: String,
    /// Base seed.
    pub seed: u64,
    /// Stable hash of the generation profile.
    pub profile_hash: String,
    /// Required execution lane.
    pub required_lane: String,
    /// Feature IDs covered by the artifact.
    pub feature_ids: Vec<String>,
    /// Stable hash of the full plan JSON.
    pub plan_hash: String,
    /// Stable hash of the final independent model state.
    pub final_model_hash: String,
    /// Validation audit for the plan.
    pub audit: StatefulPlanAudit,
    /// Distinct fail-closed execution statuses used by this lane.
    pub supported_statuses: Vec<StatefulExecutionStatus>,
}

/// Distinct completion/fail-closed categories for stateful execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StatefulExecutionStatus {
    /// All required steps completed and validation passed.
    Completed,
    /// The plan is malformed or violates the independent model.
    InvalidPlan,
    /// The plan asked for a feature outside the explicit supported-core set.
    UnsupportedFeature,
    /// The configured execution budget expired.
    Timeout,
    /// The run was cancelled before a semantic verdict.
    Cancelled,
    /// The run exhausted its exploration budget before covering required work.
    IncompleteExploration,
}

impl StatefulExecutionStatus {
    /// All statuses that must remain distinct in stateful diagnostics.
    #[must_use]
    pub fn fail_closed_statuses() -> Vec<Self> {
        vec![
            Self::Completed,
            Self::InvalidPlan,
            Self::UnsupportedFeature,
            Self::Timeout,
            Self::Cancelled,
            Self::IncompleteExploration,
        ]
    }
}

#[derive(Debug, Clone, Default)]
struct StatefulModel {
    schema_created: bool,
    rows: BTreeMap<i64, String>,
    transaction_snapshot: Option<BTreeMap<i64, String>>,
    savepoints: BTreeMap<String, BTreeMap<i64, String>>,
}

impl StatefulModel {
    fn apply(&mut self, operation: &StatefulOperation) -> Result<StatefulTransition, String> {
        match operation {
            StatefulOperation::CreateSchema => {
                if self.transaction_snapshot.is_some() {
                    return Err("stateful_create_schema_inside_transaction".to_owned());
                }
                self.schema_created = true;
                Ok(StatefulTransition::SchemaCreated)
            }
            StatefulOperation::Insert { key, value } => {
                self.require_schema()?;
                if self.rows.contains_key(key) {
                    return Err(format!("stateful_insert_duplicate_key key={key}"));
                }
                self.rows.insert(*key, value.clone());
                Ok(StatefulTransition::RowInserted { key: *key })
            }
            StatefulOperation::Update { key, value } => {
                self.require_schema()?;
                let row = self
                    .rows
                    .get_mut(key)
                    .ok_or_else(|| format!("stateful_update_missing_key key={key}"))?;
                *row = value.clone();
                Ok(StatefulTransition::RowUpdated { key: *key })
            }
            StatefulOperation::Delete { key } => {
                self.require_schema()?;
                if self.rows.remove(key).is_none() {
                    return Err(format!("stateful_delete_missing_key key={key}"));
                }
                Ok(StatefulTransition::RowDeleted { key: *key })
            }
            StatefulOperation::Begin => {
                self.require_schema()?;
                if self.transaction_snapshot.is_some() {
                    return Err("stateful_nested_begin".to_owned());
                }
                self.transaction_snapshot = Some(self.rows.clone());
                Ok(StatefulTransition::TransactionBegun)
            }
            StatefulOperation::Commit => {
                self.require_active_transaction()?;
                self.transaction_snapshot = None;
                self.savepoints.clear();
                Ok(StatefulTransition::TransactionCommitted)
            }
            StatefulOperation::Rollback => {
                let snapshot = self
                    .transaction_snapshot
                    .take()
                    .ok_or_else(|| "stateful_rollback_without_transaction".to_owned())?;
                self.rows = snapshot;
                self.savepoints.clear();
                Ok(StatefulTransition::TransactionRolledBack)
            }
            StatefulOperation::Savepoint { name } => {
                self.require_active_transaction()?;
                self.savepoints.insert(name.clone(), self.rows.clone());
                Ok(StatefulTransition::SavepointCreated { name: name.clone() })
            }
            StatefulOperation::RollbackTo { name } => {
                let snapshot = self
                    .savepoints
                    .get(name)
                    .ok_or_else(|| format!("stateful_unknown_savepoint name={name}"))?
                    .clone();
                self.rows = snapshot;
                Ok(StatefulTransition::SavepointRolledBack { name: name.clone() })
            }
            StatefulOperation::Release { name } => {
                if self.savepoints.remove(name).is_none() {
                    return Err(format!("stateful_release_unknown_savepoint name={name}"));
                }
                Ok(StatefulTransition::SavepointReleased { name: name.clone() })
            }
            StatefulOperation::SelectCount => {
                self.require_schema()?;
                Ok(StatefulTransition::Observation {
                    row_count: self.rows.len(),
                })
            }
            StatefulOperation::IntegrityCheck => {
                self.require_schema()?;
                Ok(StatefulTransition::IntegrityChecked)
            }
            StatefulOperation::CloseReopen => {
                self.require_schema()?;
                if self.transaction_snapshot.is_some() {
                    return Err("stateful_close_reopen_inside_transaction".to_owned());
                }
                Ok(StatefulTransition::CloseReopen {
                    row_count: self.rows.len(),
                })
            }
        }
    }

    fn snapshot(&self) -> StatefulModelSnapshot {
        StatefulModelSnapshot {
            schema_created: self.schema_created,
            rows: self
                .rows
                .iter()
                .map(|(key, value)| (*key, value.clone()))
                .collect(),
            in_transaction: self.transaction_snapshot.is_some(),
            savepoints: self.savepoints.keys().cloned().collect(),
        }
    }

    fn require_schema(&self) -> Result<(), String> {
        if self.schema_created {
            Ok(())
        } else {
            Err("stateful_schema_missing".to_owned())
        }
    }

    fn require_active_transaction(&self) -> Result<(), String> {
        if self.transaction_snapshot.is_some() {
            Ok(())
        } else {
            Err("stateful_transaction_missing".to_owned())
        }
    }
}

/// Generate a deterministic stateful operation plan.
///
/// # Errors
///
/// Returns an error only if the generated model would violate its own
/// preconditions, which indicates a bug in the generator.
pub fn generate_stateful_operation_plan(
    cfg: StatefulPlanConfig,
) -> Result<StatefulOperationPlan, String> {
    let normalized_min = if cfg.include_close_reopen { 18 } else { 17 };
    let max_steps = cfg.max_steps.max(normalized_min);
    let mut normalized = cfg.clone();
    normalized.max_steps = max_steps;

    let metadata = StatefulPlanMetadata {
        schema_version: STATEFUL_PLAN_SCHEMA_VERSION.to_owned(),
        fixture_id: normalized.fixture_id.clone(),
        seed: normalized.seed,
        max_steps,
        required_lane: normalized.required_lane.clone(),
        feature_ids: normalized.feature_ids.clone(),
        profile_hash: stateful_profile_hash(&normalized),
        owner_bead: "bd-turso-test-adaptation-zu081.20".to_owned(),
    };

    let mut model = StatefulModel::default();
    let mut steps = Vec::with_capacity(max_steps);
    let mut seen_seeds = BTreeSet::new();

    push_stateful_step(
        &metadata,
        &mut model,
        &mut steps,
        StatefulOperation::CreateSchema,
    )?;
    push_stateful_step(
        &metadata,
        &mut model,
        &mut steps,
        StatefulOperation::Insert {
            key: 1,
            value: stateful_value(normalized.seed, 1),
        },
    )?;
    push_stateful_step(&metadata, &mut model, &mut steps, StatefulOperation::Begin)?;
    push_stateful_step(
        &metadata,
        &mut model,
        &mut steps,
        StatefulOperation::Insert {
            key: 2,
            value: stateful_value(normalized.seed, 2),
        },
    )?;
    push_stateful_step(
        &metadata,
        &mut model,
        &mut steps,
        StatefulOperation::Savepoint {
            name: "sp_stateful".to_owned(),
        },
    )?;
    push_stateful_step(
        &metadata,
        &mut model,
        &mut steps,
        StatefulOperation::Update {
            key: 2,
            value: stateful_value(normalized.seed, 20),
        },
    )?;
    push_stateful_step(
        &metadata,
        &mut model,
        &mut steps,
        StatefulOperation::RollbackTo {
            name: "sp_stateful".to_owned(),
        },
    )?;
    push_stateful_step(
        &metadata,
        &mut model,
        &mut steps,
        StatefulOperation::Release {
            name: "sp_stateful".to_owned(),
        },
    )?;
    push_stateful_step(&metadata, &mut model, &mut steps, StatefulOperation::Commit)?;
    push_stateful_step(&metadata, &mut model, &mut steps, StatefulOperation::Begin)?;
    push_stateful_step(
        &metadata,
        &mut model,
        &mut steps,
        StatefulOperation::Insert {
            key: 3,
            value: stateful_value(normalized.seed, 3),
        },
    )?;
    push_stateful_step(
        &metadata,
        &mut model,
        &mut steps,
        StatefulOperation::Rollback,
    )?;
    push_stateful_step(
        &metadata,
        &mut model,
        &mut steps,
        StatefulOperation::Insert {
            key: 4,
            value: stateful_value(normalized.seed, 4),
        },
    )?;
    push_stateful_step(
        &metadata,
        &mut model,
        &mut steps,
        StatefulOperation::Delete { key: 4 },
    )?;
    push_stateful_step(
        &metadata,
        &mut model,
        &mut steps,
        StatefulOperation::SelectCount,
    )?;
    push_stateful_step(
        &metadata,
        &mut model,
        &mut steps,
        StatefulOperation::IntegrityCheck,
    )?;
    if normalized.include_close_reopen {
        push_stateful_step(
            &metadata,
            &mut model,
            &mut steps,
            StatefulOperation::CloseReopen,
        )?;
    }
    push_stateful_step(
        &metadata,
        &mut model,
        &mut steps,
        StatefulOperation::SelectCount,
    )?;

    for step in &steps {
        if !seen_seeds.insert(step.origin_seed) {
            return Err(format!(
                "stateful_duplicate_origin_seed step_id={}",
                step.step_id
            ));
        }
    }

    Ok(StatefulOperationPlan {
        metadata,
        steps,
        final_model: model.snapshot(),
    })
}

fn push_stateful_step(
    metadata: &StatefulPlanMetadata,
    model: &mut StatefulModel,
    steps: &mut Vec<StatefulPlanStep>,
    operation: StatefulOperation,
) -> Result<(), String> {
    let step_id = u64::try_from(steps.len()).unwrap_or(u64::MAX);
    let preconditions = stateful_preconditions(model, &operation, &metadata.required_lane);
    let transition = model.apply(&operation)?;
    let postconditions = stateful_postconditions(model, &operation, &metadata.required_lane);
    steps.push(StatefulPlanStep {
        step_id,
        preconditions,
        operation,
        expected_transition: transition,
        postconditions,
        required_lane: metadata.required_lane.clone(),
        feature_ids: metadata.feature_ids.clone(),
        origin_seed: derive_worker_seed(metadata.seed, u16::try_from(step_id).unwrap_or(u16::MAX)),
    });
    Ok(())
}

fn stateful_preconditions(
    model: &StatefulModel,
    operation: &StatefulOperation,
    required_lane: &str,
) -> Vec<StatefulCondition> {
    let mut conditions = vec![StatefulCondition::LaneObserved {
        lane: required_lane.to_owned(),
    }];
    match operation {
        StatefulOperation::CreateSchema => conditions.push(StatefulCondition::NotInTransaction),
        StatefulOperation::Insert { key, .. } => {
            conditions.push(StatefulCondition::SchemaExists);
            conditions.push(StatefulCondition::RowAbsent { key: *key });
        }
        StatefulOperation::Update { key, .. } | StatefulOperation::Delete { key } => {
            conditions.push(StatefulCondition::SchemaExists);
            conditions.push(StatefulCondition::RowExists { key: *key });
        }
        StatefulOperation::Begin | StatefulOperation::CloseReopen => {
            conditions.push(StatefulCondition::SchemaExists);
            conditions.push(StatefulCondition::NotInTransaction);
        }
        StatefulOperation::Commit | StatefulOperation::Rollback => {
            conditions.push(StatefulCondition::InTransaction);
        }
        StatefulOperation::Savepoint { .. } => conditions.push(StatefulCondition::InTransaction),
        StatefulOperation::RollbackTo { name } | StatefulOperation::Release { name } => {
            conditions.push(StatefulCondition::InTransaction);
            conditions.push(StatefulCondition::SavepointExists { name: name.clone() });
        }
        StatefulOperation::SelectCount | StatefulOperation::IntegrityCheck => {
            conditions.push(StatefulCondition::SchemaExists);
        }
    }
    conditions.push(StatefulCondition::RowCount {
        count: model.rows.len(),
    });
    conditions
}

fn stateful_postconditions(
    model: &StatefulModel,
    operation: &StatefulOperation,
    required_lane: &str,
) -> Vec<StatefulCondition> {
    let mut conditions = vec![
        StatefulCondition::LaneObserved {
            lane: required_lane.to_owned(),
        },
        StatefulCondition::SchemaExists,
        StatefulCondition::RowCount {
            count: model.rows.len(),
        },
    ];
    match operation {
        StatefulOperation::Insert { key, .. } | StatefulOperation::Update { key, .. } => {
            conditions.push(StatefulCondition::RowExists { key: *key });
        }
        StatefulOperation::Delete { key } => {
            conditions.push(StatefulCondition::RowAbsent { key: *key });
        }
        StatefulOperation::Begin | StatefulOperation::Savepoint { .. } => {
            conditions.push(StatefulCondition::InTransaction);
        }
        StatefulOperation::Commit
        | StatefulOperation::Rollback
        | StatefulOperation::CloseReopen => {
            conditions.push(StatefulCondition::NotInTransaction);
        }
        StatefulOperation::RollbackTo { name } => {
            conditions.push(StatefulCondition::InTransaction);
            conditions.push(StatefulCondition::SavepointExists { name: name.clone() });
        }
        StatefulOperation::Release { .. } => {
            conditions.push(StatefulCondition::InTransaction);
        }
        StatefulOperation::CreateSchema
        | StatefulOperation::SelectCount
        | StatefulOperation::IntegrityCheck => {}
    }
    if matches!(operation, StatefulOperation::IntegrityCheck) {
        conditions.push(StatefulCondition::IntegrityClean);
    }
    conditions
}

fn stateful_value(seed: u64, key: i64) -> String {
    let rotation = u32::try_from(key.rem_euclid(32)).unwrap_or(0);
    format!("stateful_{:016x}_{key}", seed.rotate_left(rotation))
}

fn stateful_sql_literal(value: &str) -> String {
    if value.parse::<i64>().is_ok() || value.parse::<f64>().is_ok() {
        value.to_owned()
    } else {
        format!("'{}'", value.replace('\'', "''"))
    }
}

fn stable_sql_text(schema: &[String], workload: &[String]) -> String {
    let mut text = String::new();
    for statement in schema.iter().chain(workload) {
        text.push_str(statement);
        text.push('\n');
    }
    text
}

fn stateful_profile_hash(cfg: &StatefulPlanConfig) -> String {
    let input = format!(
        "{}:{}:{}:{}:{}:{}",
        cfg.fixture_id,
        cfg.seed,
        cfg.max_steps,
        cfg.required_lane,
        cfg.include_close_reopen,
        cfg.feature_ids.join(",")
    );
    sha256_hex(input.as_bytes())
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    crate::bytes_to_lower_hex(hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_seed_produces_identical_jsonl() {
        let cfg = WorkloadConfig {
            fixture_id: "t".to_owned(),
            seed: 42,
            num_operations: 200,
            worker_count: 4,
            transaction_size: 25,
            commit_order_policy: CommitOrderPolicy::Barrier,
            operation_mix: OperationMix {
                insert_weight: 40,
                update_weight: 30,
                delete_weight: 10,
                select_weight: 20,
            },
            tables: vec![TableSpec::simple("t0"), TableSpec::simple("t1")],
        };

        let a = WorkloadGenerator::new(cfg.clone())
            .generate()
            .to_jsonl()
            .expect("to_jsonl should succeed");
        let b = WorkloadGenerator::new(cfg)
            .generate()
            .to_jsonl()
            .expect("to_jsonl should succeed");
        assert_eq!(a, b);
    }

    #[test]
    fn different_seeds_differ() {
        let mut cfg = WorkloadConfig {
            seed: 1,
            ..WorkloadConfig::default()
        };
        let a = WorkloadGenerator::new(cfg.clone())
            .generate()
            .to_jsonl()
            .expect("to_jsonl should succeed");
        cfg.seed = 2;
        let b = WorkloadGenerator::new(cfg)
            .generate()
            .to_jsonl()
            .expect("to_jsonl should succeed");
        assert_ne!(a, b);
    }

    #[test]
    fn generator_emits_all_op_categories() {
        let cfg = WorkloadConfig {
            fixture_id: "t".to_owned(),
            seed: 7,
            num_operations: 200,
            worker_count: 2,
            transaction_size: 20,
            commit_order_policy: CommitOrderPolicy::Deterministic,
            operation_mix: OperationMix {
                insert_weight: 25,
                update_weight: 25,
                delete_weight: 25,
                select_weight: 25,
            },
            tables: vec![TableSpec::simple("t0")],
        };
        let log = WorkloadGenerator::new(cfg).generate();
        let mut saw_insert = false;
        let mut saw_update = false;
        let mut saw_delete = false;
        let mut saw_select = false;
        for rec in &log.records {
            match &rec.kind {
                OpKind::Insert { .. } => saw_insert = true,
                OpKind::Update { .. } => saw_update = true,
                OpKind::Sql { statement } => {
                    let kw = statement.split_whitespace().next().unwrap_or("");
                    if kw.eq_ignore_ascii_case("DELETE") {
                        saw_delete = true;
                    }
                    if kw.eq_ignore_ascii_case("SELECT") {
                        saw_select = true;
                    }
                }
                OpKind::Begin | OpKind::Commit | OpKind::Rollback => {}
            }
        }
        assert!(saw_insert);
        assert!(saw_update);
        assert!(saw_delete);
        assert!(saw_select);
    }

    #[test]
    fn operation_mix_ratios_within_tolerance() {
        // With 10,000 ops and 60/15/5/20 weights, actual ratios should be
        // within 5% of expected.
        let cfg = WorkloadConfig {
            fixture_id: "mix".to_owned(),
            seed: 123,
            num_operations: 10_000,
            worker_count: 1,
            transaction_size: 100,
            ..WorkloadConfig::default()
        };
        let log = WorkloadGenerator::new(cfg).generate();

        let mut inserts: u32 = 0;
        let mut updates: u32 = 0;
        let mut deletes: u32 = 0;
        let mut selects: u32 = 0;

        for rec in &log.records {
            match &rec.kind {
                OpKind::Insert { .. } => inserts += 1,
                OpKind::Update { .. } => updates += 1,
                OpKind::Sql { statement } => {
                    let kw = statement.split_whitespace().next().unwrap_or("");
                    if kw.eq_ignore_ascii_case("DELETE") {
                        deletes += 1;
                    } else if kw.eq_ignore_ascii_case("SELECT") {
                        selects += 1;
                    }
                }
                OpKind::Begin | OpKind::Commit | OpKind::Rollback => {}
            }
        }

        let total = inserts + updates + deletes + selects;
        assert!(total > 0, "should have non-tx operations");

        // Inserts get a boost from fallback (when update/delete can't find a live key)
        // so we just check that all categories are present and selects are roughly 20%.
        let select_pct = f64::from(selects) / f64::from(total) * 100.0;
        assert!(
            (10.0..=30.0).contains(&select_pct),
            "select ratio {select_pct:.1}% should be roughly 20% (10-30% tolerance)"
        );
    }

    #[test]
    fn concurrent_distribution_disjoint_key_ranges() {
        let cfg = WorkloadConfig {
            fixture_id: "conc".to_owned(),
            seed: 42,
            num_operations: 400,
            worker_count: 4,
            transaction_size: 50,
            ..WorkloadConfig::default()
        };
        let log = WorkloadGenerator::new(cfg).generate();

        // Collect insert keys per worker.
        let mut keys_by_worker: std::collections::HashMap<u16, Vec<i64>> =
            std::collections::HashMap::new();
        for rec in &log.records {
            if let OpKind::Insert { key, .. } = &rec.kind {
                keys_by_worker.entry(rec.worker).or_default().push(*key);
            }
        }

        // Verify disjoint ranges: each worker's keys should not overlap with others.
        let all_keys: Vec<(u16, i64)> = keys_by_worker
            .iter()
            .flat_map(|(w, keys)| keys.iter().map(move |k| (*w, *k)))
            .collect();
        for i in 0..all_keys.len() {
            for j in (i + 1)..all_keys.len() {
                if all_keys[i].0 != all_keys[j].0 {
                    assert_ne!(
                        all_keys[i].1, all_keys[j].1,
                        "key {} appears in worker {} and worker {}",
                        all_keys[i].1, all_keys[i].0, all_keys[j].0
                    );
                }
            }
        }

        // Each worker should have some inserts.
        assert_eq!(keys_by_worker.len(), 4, "all 4 workers should have inserts");
    }

    #[test]
    fn update_targets_previously_inserted_keys() {
        let cfg = WorkloadConfig {
            fixture_id: "upd".to_owned(),
            seed: 99,
            num_operations: 500,
            worker_count: 1,
            transaction_size: 50,
            operation_mix: OperationMix {
                insert_weight: 40,
                update_weight: 40,
                delete_weight: 0,
                select_weight: 20,
            },
            ..WorkloadConfig::default()
        };
        let log = WorkloadGenerator::new(cfg).generate();

        let mut inserted_keys: std::collections::HashSet<i64> = std::collections::HashSet::new();
        for rec in &log.records {
            match &rec.kind {
                OpKind::Insert { key, .. } => {
                    inserted_keys.insert(*key);
                }
                OpKind::Update { key, .. } => {
                    assert!(
                        inserted_keys.contains(key),
                        "UPDATE targets key {key} which was never inserted"
                    );
                }
                _ => {}
            }
        }
    }

    #[test]
    fn delete_targets_existing_keys() {
        let cfg = WorkloadConfig {
            fixture_id: "del".to_owned(),
            seed: 77,
            num_operations: 500,
            worker_count: 1,
            transaction_size: 50,
            operation_mix: OperationMix {
                insert_weight: 40,
                update_weight: 10,
                delete_weight: 30,
                select_weight: 20,
            },
            ..WorkloadConfig::default()
        };
        let log = WorkloadGenerator::new(cfg).generate();

        let mut live_keys: std::collections::HashSet<i64> = std::collections::HashSet::new();
        for rec in &log.records {
            match &rec.kind {
                OpKind::Insert { key, .. } => {
                    live_keys.insert(*key);
                }
                OpKind::Sql { statement } => {
                    if let Some(rest) = statement.strip_prefix("DELETE FROM t0 WHERE id = ") {
                        let key: i64 = rest.parse().expect("delete key should be parseable");
                        assert!(
                            live_keys.contains(&key),
                            "DELETE targets key {key} which is not live"
                        );
                        live_keys.remove(&key);
                    }
                }
                _ => {}
            }
        }
    }

    #[test]
    fn zero_operations_produces_setup_only() {
        let cfg = WorkloadConfig {
            num_operations: 0,
            ..WorkloadConfig::default()
        };
        let log = WorkloadGenerator::new(cfg).generate();

        // Should have setup SQL (CREATE TABLE) but no data operations.
        assert!(
            !log.records
                .iter()
                .any(|r| matches!(r.kind, OpKind::Insert { .. } | OpKind::Update { .. })),
            "0-operation workload should have no data operations"
        );
        // Should still have the CREATE TABLE.
        assert!(
            log.records
                .iter()
                .any(|r| matches!(&r.kind, OpKind::Sql { statement } if statement.contains("CREATE TABLE"))),
            "should have setup DDL"
        );
    }

    #[test]
    fn single_operation_workload() {
        let cfg = WorkloadConfig {
            num_operations: 1,
            ..WorkloadConfig::default()
        };
        let log = WorkloadGenerator::new(cfg).generate();

        // Should have exactly 1 data operation (the seeded insert) plus
        // setup (CREATE TABLE) and transaction wrappers (BEGIN/COMMIT).
        let data_ops: usize = log
            .records
            .iter()
            .filter(|r| {
                matches!(r.kind, OpKind::Insert { .. } | OpKind::Update { .. })
                    || matches!(&r.kind, OpKind::Sql { statement } if
                    statement.starts_with("DELETE") || statement.starts_with("SELECT"))
            })
            .count();
        assert_eq!(
            data_ops, 1,
            "single-operation workload should have 1 data op"
        );
    }

    #[test]
    fn large_workload_completes_without_panic() {
        let cfg = WorkloadConfig {
            fixture_id: "large".to_owned(),
            seed: 0,
            num_operations: 100_000,
            worker_count: 8,
            transaction_size: 200,
            ..WorkloadConfig::default()
        };
        let log = WorkloadGenerator::new(cfg).generate();
        assert!(
            log.records.len() > 100_000,
            "100K ops + setup + tx wrappers should exceed 100K records"
        );
    }

    #[test]
    fn transaction_wrapping_begin_commit_pairs() {
        let cfg = WorkloadConfig {
            fixture_id: "tx".to_owned(),
            seed: 42,
            num_operations: 100,
            worker_count: 1,
            transaction_size: 25,
            ..WorkloadConfig::default()
        };
        let log = WorkloadGenerator::new(cfg).generate();

        let mut begin_count: usize = 0;
        let mut commit_count: usize = 0;
        let mut in_tx = false;

        for rec in &log.records {
            match &rec.kind {
                OpKind::Begin => {
                    assert!(!in_tx, "nested BEGIN without COMMIT");
                    in_tx = true;
                    begin_count += 1;
                }
                OpKind::Commit => {
                    assert!(in_tx, "COMMIT without matching BEGIN");
                    in_tx = false;
                    commit_count += 1;
                }
                _ => {}
            }
        }

        assert!(
            !in_tx,
            "final transaction should be committed (not left open)"
        );
        assert_eq!(
            begin_count, commit_count,
            "BEGIN and COMMIT counts must match"
        );
        // 100 ops / 25 per tx = 4 transactions.
        assert_eq!(
            begin_count, 4,
            "expected 4 transactions for 100 ops at size 25"
        );
    }

    #[test]
    fn schema_aware_insert_columns_match_table_spec() {
        let tables = vec![
            TableSpec::simple("users"),
            TableSpec {
                name: "logs".to_owned(),
                create_sql:
                    "CREATE TABLE IF NOT EXISTS logs (id INTEGER PRIMARY KEY, val TEXT, num REAL)"
                        .to_owned(),
            },
        ];
        let cfg = WorkloadConfig {
            fixture_id: "schema".to_owned(),
            seed: 42,
            num_operations: 100,
            tables,
            ..WorkloadConfig::default()
        };
        let log = WorkloadGenerator::new(cfg).generate();

        for rec in &log.records {
            if let OpKind::Insert { table, values, .. } = &rec.kind {
                // TableSpec::simple generates (id, val, num) columns.
                // Insert provides val and num values.
                assert_eq!(
                    values.len(),
                    2,
                    "insert into {table} should have 2 non-key columns (val, num)"
                );
                assert_eq!(values[0].0, "val", "first column should be 'val'");
                assert_eq!(values[1].0, "num", "second column should be 'num'");
            }
        }
    }

    #[test]
    fn stateful_plan_validates_and_projects_to_oplog() {
        let plan = generate_stateful_operation_plan(StatefulPlanConfig::default())
            .expect("default stateful plan should generate");
        let audit = plan
            .validate()
            .expect("default stateful plan should validate");
        assert_eq!(audit.schema_version, STATEFUL_PLAN_SCHEMA_VERSION);
        assert_eq!(audit.step_count, plan.steps.len());
        assert_eq!(audit.close_reopen_count, 1);

        let oplog = plan.to_oplog().expect("stateful plan should project");
        assert_eq!(
            oplog.header.preset.as_deref(),
            Some("stateful-operation-plan")
        );
        assert_eq!(oplog.header.concurrency.worker_count, 1);
        assert_eq!(oplog.records.len(), audit.projected_record_count);
        assert!(oplog.records.len() < plan.steps.len());
        assert!(
            oplog
                .records
                .iter()
                .any(|record| matches!(&record.kind, OpKind::Sql { statement } if statement.starts_with("SAVEPOINT ")))
        );
        assert!(
            oplog
                .records
                .iter()
                .any(|record| matches!(&record.kind, OpKind::Sql { statement } if statement == "PRAGMA integrity_check"))
        );

        let artifact = plan
            .to_sql_artifact()
            .expect("stateful SQL artifact should build");
        assert_eq!(artifact.metadata.audit, audit);
        assert_eq!(artifact.metadata.required_lane, STATEFUL_REQUIRED_LANE);
        assert!(artifact.schema[0].starts_with("CREATE TABLE IF NOT EXISTS"));
        assert!(
            artifact
                .workload
                .iter()
                .any(|sql| sql == "SAVEPOINT sp_stateful")
        );
        assert!(artifact.trace.iter().any(|entry| {
            matches!(entry.operation, StatefulOperation::CloseReopen)
                && entry.executable_sql.is_none()
        }));
        assert_eq!(
            artifact.metadata.supported_statuses.len(),
            artifact
                .metadata
                .supported_statuses
                .iter()
                .map(|status| format!("{status:?}"))
                .collect::<BTreeSet<_>>()
                .len()
        );
    }

    #[test]
    fn stateful_plan_artifact_hash_is_stable_for_same_seed() {
        let cfg = StatefulPlanConfig {
            fixture_id: "stable".to_owned(),
            seed: 0x1234,
            ..StatefulPlanConfig::default()
        };
        let first = generate_stateful_operation_plan(cfg.clone())
            .expect("first stateful plan should generate")
            .stable_artifact_hash()
            .expect("hash first plan");
        let second = generate_stateful_operation_plan(cfg)
            .expect("second stateful plan should generate")
            .stable_artifact_hash()
            .expect("hash second plan");
        assert_eq!(first, second);

        let cfg = StatefulPlanConfig {
            fixture_id: "stable".to_owned(),
            seed: 0x1234,
            ..StatefulPlanConfig::default()
        };
        let first_artifact = generate_stateful_operation_plan(cfg.clone())
            .expect("first stateful plan should generate")
            .to_sql_artifact()
            .expect("first stateful artifact should build");
        let second_artifact = generate_stateful_operation_plan(cfg)
            .expect("second stateful plan should generate")
            .to_sql_artifact()
            .expect("second stateful artifact should build");
        assert_eq!(first_artifact.sql_hash, second_artifact.sql_hash);
        assert_eq!(first_artifact.trace_hash, second_artifact.trace_hash);
        assert_eq!(first_artifact.metadata_hash, second_artifact.metadata_hash);
    }

    #[test]
    fn stateful_plan_validation_rejects_tampered_transition() {
        let mut plan = generate_stateful_operation_plan(StatefulPlanConfig::default())
            .expect("default stateful plan should generate");
        plan.steps[1].expected_transition = StatefulTransition::RowDeleted { key: 1 };
        let error = plan
            .validate()
            .expect_err("tampered transition must fail closed");
        assert!(error.contains("stateful_step_transition_mismatch"));
    }

    #[test]
    fn stateful_plan_can_omit_close_reopen_boundary() {
        let plan = generate_stateful_operation_plan(StatefulPlanConfig {
            include_close_reopen: false,
            ..StatefulPlanConfig::default()
        })
        .expect("stateful plan without reopen should generate");
        let audit = plan.validate().expect("stateful plan should validate");
        assert_eq!(audit.close_reopen_count, 0);
        assert_eq!(plan.steps.len(), audit.projected_record_count);
    }
}
