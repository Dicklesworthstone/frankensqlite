//! Deterministic workload generation with seeded RNG.
//!
//! This module is deliberately **pure computation** (no I/O, no `Cx`) so it can
//! be used in both unit tests and higher-level harness orchestration.

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use crate::oplog::{ConcurrencyModel, OpKind, OpLog, OpLogHeader, OpRecord, RngSpec};

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
    let mut x = rng.gen_range(0..total);
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
}

/// Configuration controlling workload generation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkloadConfig {
    /// Identifier linking this workload to a golden/work fixture.
    pub fixture_id: String,
    /// Master seed.
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
        let len = self.rng.gen_range(1..=24);
        (0..len)
            .map(|_| (b'a' + self.rng.gen_range(0..26)) as char)
            .collect()
    }

    fn gen_real(&mut self) -> f64 {
        self.rng.gen_range(-1000.0..=1000.0)
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
        let idx = self.rng.gen_range(0..keys.len());
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
            let table_idx = ws.rng.gen_range(0..table_count);
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
                        for kind in batches[w][batch_idx].drain(..) {
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
}
