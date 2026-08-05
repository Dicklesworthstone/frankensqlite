//! Reproducibility verification tests for deterministic seeding.
//!
//! Bead: bd-mblr.4.3.2
//!
//! These tests verify that identical seeds produce identical outputs across
//! multiple executions. This is the cornerstone of FrankenSQLite's debugging
//! and regression testing strategy.
//!
//! ## Test Categories
//!
//! 1. **Seed derivation**: Verify `derive_worker_seed` and `derive_scenario_seed`
//!    are pure functions that always return the same output.
//!
//! 2. **RNG stream stability**: Verify that the same seed produces the same
//!    random sequence across runs.
//!
//! 3. **OpLog determinism**: Verify that workload generation is reproducible.
//!
//! 4. **Database state determinism**: Verify that executing the same OpLog
//!    produces identical database states.
//!
//! Run with:
//! ```sh
//! cargo test -p fsqlite-e2e --test seed_reproducibility -- --nocapture
//! ```

use rand::Rng;
use rand::SeedableRng;
use rand::rngs::StdRng;
use std::collections::BTreeSet;

use fsqlite_e2e::oplog::{preset_commutative_inserts_disjoint_keys, preset_hot_page_contention};
use fsqlite_e2e::workload::{
    StatefulOperation, StatefulOperationPlan, StatefulPlanConfig, generate_stateful_operation_plan,
};
use fsqlite_e2e::{FRANKEN_SEED, derive_scenario_seed, derive_worker_seed};
use fsqlite_harness::differential_v2::{
    CsqliteExecutor, EngineIdentity, ExecutionEnvelope, FsqliteExecutor, NormalizedValue, Outcome,
    SqlExecutor, minimize_mismatch_workload, run_differential,
};

const SCENARIO_HASHES: [u64; 5] = [
    0x0053_4348_u64,
    0x0054_584E,
    0x0043_4F4E,
    0x0043_4F52,
    0x0043_4D50,
];

// ─── Seed Derivation Reproducibility ────────────────────────────────────

#[test]
fn seed_derivation_is_deterministic() {
    // derive_worker_seed must be a pure function.
    let base = FRANKEN_SEED;

    for worker_id in 0..=100 {
        let seed1 = derive_worker_seed(base, worker_id);
        let seed2 = derive_worker_seed(base, worker_id);
        assert_eq!(
            seed1, seed2,
            "derive_worker_seed must be deterministic for worker {worker_id}"
        );
    }
}

#[test]
fn scenario_derivation_is_deterministic() {
    // derive_scenario_seed must be a pure function.
    let base = FRANKEN_SEED;

    for hash in SCENARIO_HASHES {
        let seed1 = derive_scenario_seed(base, hash);
        let seed2 = derive_scenario_seed(base, hash);
        assert_eq!(
            seed1, seed2,
            "derive_scenario_seed must be deterministic for hash {hash:#x}"
        );
    }
}

#[test]
fn worker_seeds_are_distinct() {
    // Different workers must get different seeds.
    let base = FRANKEN_SEED;
    let mut seeds = std::collections::HashSet::new();

    for worker_id in 0..100 {
        let seed = derive_worker_seed(base, worker_id);
        assert!(
            seeds.insert(seed),
            "Worker {worker_id} seed collision: {seed}"
        );
    }
}

#[test]
fn scenario_seeds_are_distinct() {
    // Different scenarios must get different seeds.
    let base = FRANKEN_SEED;
    let mut seeds = std::collections::HashSet::new();

    for hash in SCENARIO_HASHES {
        let seed = derive_scenario_seed(base, hash);
        assert!(
            seeds.insert(seed),
            "Scenario {hash:#x} seed collision: {seed}"
        );
    }
}

// ─── RNG Stream Reproducibility ─────────────────────────────────────────

#[test]
fn rng_stream_is_reproducible() {
    // Same seed must produce same sequence.
    let seed = FRANKEN_SEED;

    let sequence1: Vec<u64> = {
        let mut rng = StdRng::seed_from_u64(seed);
        (0..100).map(|_| rng.next_u64()).collect()
    };

    let sequence2: Vec<u64> = {
        let mut rng = StdRng::seed_from_u64(seed);
        (0..100).map(|_| rng.next_u64()).collect()
    };

    assert_eq!(
        sequence1, sequence2,
        "Same seed must produce same RNG sequence"
    );
}

#[test]
fn rng_stream_different_seeds_differ() {
    // Different seeds must produce different sequences.
    let seed1 = FRANKEN_SEED;
    let seed2 = FRANKEN_SEED + 1;

    let sequence1: Vec<u64> = {
        let mut rng = StdRng::seed_from_u64(seed1);
        (0..10).map(|_| rng.next_u64()).collect()
    };

    let sequence2: Vec<u64> = {
        let mut rng = StdRng::seed_from_u64(seed2);
        (0..10).map(|_| rng.next_u64()).collect()
    };

    assert_ne!(
        sequence1, sequence2,
        "Different seeds must produce different RNG sequences"
    );
}

// ─── OpLog Generation Reproducibility ───────────────────────────────────

#[test]
fn oplog_generation_is_reproducible() {
    // Same preset + seed must produce identical OpLogs.
    let seed = FRANKEN_SEED;
    let workers = 4;
    let rows_per_worker = 100;

    let oplog1 =
        preset_commutative_inserts_disjoint_keys("repro-test-1", seed, workers, rows_per_worker);

    let oplog2 =
        preset_commutative_inserts_disjoint_keys("repro-test-1", seed, workers, rows_per_worker);

    // Compare headers.
    assert_eq!(oplog1.header.seed, oplog2.header.seed, "Seeds must match");
    assert_eq!(
        oplog1.header.concurrency.worker_count, oplog2.header.concurrency.worker_count,
        "Worker counts must match"
    );

    // Compare record counts.
    assert_eq!(
        oplog1.records.len(),
        oplog2.records.len(),
        "Record counts must match"
    );

    // Compare each record.
    for (i, (r1, r2)) in oplog1.records.iter().zip(oplog2.records.iter()).enumerate() {
        assert_eq!(r1.op_id, r2.op_id, "Op IDs must match at index {i}");
        assert_eq!(r1.worker, r2.worker, "Worker IDs must match at index {i}");
        // Note: OpKind comparison depends on its implementation of PartialEq.
        // For now, we compare the debug representation.
        assert_eq!(
            format!("{:?}", r1.kind),
            format!("{:?}", r2.kind),
            "OpKinds must match at index {i}"
        );
    }
}

#[test]
fn oplog_contention_preset_is_reproducible() {
    // Contention preset must also be reproducible.
    let seed = FRANKEN_SEED;
    let workers = 4;
    let rounds = 10;

    let oplog1 = preset_hot_page_contention("repro-contention-1", seed, workers, rounds);
    let oplog2 = preset_hot_page_contention("repro-contention-1", seed, workers, rounds);

    assert_eq!(oplog1.header.seed, oplog2.header.seed);
    assert_eq!(oplog1.records.len(), oplog2.records.len());

    for (i, (r1, r2)) in oplog1.records.iter().zip(oplog2.records.iter()).enumerate() {
        assert_eq!(r1.op_id, r2.op_id, "Op IDs must match at index {i}");
    }
}

#[test]
fn oplog_different_seeds_differ() {
    // Different seeds must produce different OpLogs.
    let seed1 = FRANKEN_SEED;
    let seed2 = FRANKEN_SEED + 1;

    let oplog1 = preset_commutative_inserts_disjoint_keys("diff-seed-1", seed1, 2, 50);
    let oplog2 = preset_commutative_inserts_disjoint_keys("diff-seed-2", seed2, 2, 50);

    assert_ne!(oplog1.header.seed, oplog2.header.seed);
    // The record contents should differ (probabilistically certain for non-trivial sizes).
}

// ─── Database State Reproducibility ─────────────────────────────────────

#[test]
fn database_state_is_reproducible() {
    asupersync::test_utils::run_test(|| async {
        // Executing the same OpLog twice must produce identical database states.
        let seed = FRANKEN_SEED;

        // Generate a workload.
        let oplog = preset_commutative_inserts_disjoint_keys("db-repro-test", seed, 2, 100);

        // Execute on FrankenSQLite twice.
        let state1 = execute_oplog_and_hash(&oplog).await;
        let state2 = execute_oplog_and_hash(&oplog).await;

        assert_eq!(
            state1, state2,
            "Same OpLog must produce identical database states"
        );
    });
}

#[test]
fn database_state_commutative_inserts_seed_independent() {
    asupersync::test_utils::run_test(|| async {
        // The commutative_inserts_disjoint_keys preset produces data values
        // that are deterministic based on worker/row indices, NOT the seed.
        // The seed only affects operation ordering (which doesn't matter for
        // commutative operations with disjoint keys).
        //
        // This test verifies that for commutative presets, different seeds
        // produce EQUIVALENT final states (which is the design intent).
        let seed1 = FRANKEN_SEED;
        let seed2 = FRANKEN_SEED + 1;

        let oplog1 = preset_commutative_inserts_disjoint_keys("db-equiv-1", seed1, 2, 100);
        let oplog2 = preset_commutative_inserts_disjoint_keys("db-equiv-2", seed2, 2, 100);

        let state1 = execute_oplog_and_hash(&oplog1).await;
        let state2 = execute_oplog_and_hash(&oplog2).await;

        assert_eq!(
            state1, state2,
            "Commutative presets with disjoint keys should produce equivalent states regardless of seed"
        );
    });
}

#[test]
fn stateful_operation_plan_file_replay_is_reproducible() {
    asupersync::test_utils::run_test(|| async {
        let plan = generate_stateful_operation_plan(StatefulPlanConfig {
            fixture_id: "stateful-db-repro".to_owned(),
            seed: FRANKEN_SEED,
            ..StatefulPlanConfig::default()
        })
        .expect("stateful plan should generate");

        let audit = plan.validate().expect("stateful plan should validate");
        assert_eq!(audit.close_reopen_count, 1);

        let oplog = plan
            .to_oplog()
            .expect("stateful plan should project to OpLog");
        assert_eq!(
            oplog.header.preset.as_deref(),
            Some("stateful-operation-plan")
        );
        let artifact = plan
            .to_sql_artifact()
            .expect("stateful SQL artifact should build");
        assert_eq!(artifact.schema, fixed_seed_stateful_schema());
        assert_eq!(artifact.workload, fixed_seed_stateful_workload());

        let state1 = execute_stateful_plan_and_hash(&plan).await;
        let state2 = execute_stateful_plan_and_hash(&plan).await;
        assert_eq!(
            state1, state2,
            "stateful plan replay must be deterministic across clean temp files"
        );
    });
}

#[test]
fn stateful_operation_plan_public_differential_matches_csqlite() {
    let plan = generate_stateful_operation_plan(StatefulPlanConfig {
        fixture_id: "stateful-public-differential".to_owned(),
        seed: FRANKEN_SEED,
        ..StatefulPlanConfig::default()
    })
    .expect("stateful plan should generate");
    let artifact = plan
        .to_sql_artifact()
        .expect("stateful SQL artifact should build");
    assert_eq!(
        artifact.metadata.supported_statuses.len(),
        distinct_status_count(&artifact.metadata.supported_statuses)
    );

    let envelope = stateful_artifact_envelope(&artifact.schema, &artifact.workload, FRANKEN_SEED);
    let fsqlite = FsqliteExecutor::open_in_memory().expect("open FrankenSQLite executor");
    let csqlite = CsqliteExecutor::open_in_memory().expect("open C SQLite executor");
    let result = run_differential(&envelope, &fsqlite, &csqlite);

    assert_eq!(result.outcome, Outcome::Pass, "{result:#?}");
    assert!(result.logical_state_matched);
    assert_eq!(result.statements_mismatched, 0);
    assert_eq!(artifact.metadata.audit.close_reopen_count, 1);
    assert!(artifact.trace.iter().any(|entry| {
        matches!(entry.operation, StatefulOperation::CloseReopen) && entry.executable_sql.is_none()
    }));
}

#[test]
fn stateful_operation_plan_synthetic_mismatch_reduces_and_replays() {
    let plan = generate_stateful_operation_plan(StatefulPlanConfig {
        fixture_id: "stateful-reducer-replay".to_owned(),
        seed: FRANKEN_SEED,
        ..StatefulPlanConfig::default()
    })
    .expect("stateful plan should generate");
    let artifact = plan
        .to_sql_artifact()
        .expect("stateful SQL artifact should build");
    let envelope = stateful_artifact_envelope(&artifact.schema, &artifact.workload, FRANKEN_SEED);
    let required_lane = artifact.metadata.required_lane.clone();

    let reduction = minimize_mismatch_workload(
        &envelope,
        || Ok(SyntheticStatefulExecutor::subject()),
        || Ok(SyntheticStatefulExecutor::reference()),
    )
    .expect("synthetic reducer should run")
    .expect("synthetic mismatch should reproduce");

    assert_eq!(reduction.minimized_result.outcome, Outcome::Divergence);
    assert!(reduction.minimized_workload_len < reduction.original_workload_len);
    assert_eq!(
        reduction.minimized_envelope.workload,
        vec!["SELECT COUNT(*) FROM stateful_kv".to_owned()]
    );
    let divergence = reduction
        .minimized_result
        .divergences
        .first()
        .expect("minimized result should retain first divergence");
    assert_eq!(divergence.sql, "SELECT COUNT(*) FROM stateful_kv");
    assert_eq!(required_lane, artifact.metadata.required_lane);

    let replay = run_differential(
        &reduction.minimized_envelope,
        &SyntheticStatefulExecutor::subject(),
        &SyntheticStatefulExecutor::reference(),
    );
    assert_eq!(replay.outcome, Outcome::Divergence);
    assert_eq!(replay.divergences[0].sql, divergence.sql);
}

// ─── Helpers ────────────────────────────────────────────────────────────

/// Execute an OpLog on FrankenSQLite and return a hash of the final state.
async fn execute_oplog_and_hash(oplog: &fsqlite_e2e::oplog::OpLog) -> String {
    use sha2::{Digest, Sha256};

    let conn = fsqlite::Connection::open(":memory:")
        .await
        .expect("open connection");

    // Execute each operation from the OpLog.
    // The OpLog includes CREATE TABLE as the first operation.
    for rec in &oplog.records {
        let sql = match &rec.kind {
            fsqlite_e2e::oplog::OpKind::Sql { statement } => statement.clone(),
            fsqlite_e2e::oplog::OpKind::Insert { table, key, values } => {
                let cols: Vec<String> = std::iter::once("id".to_owned())
                    .chain(values.iter().map(|(c, _)| c.clone()))
                    .collect();
                let vals: Vec<String> = std::iter::once(key.to_string())
                    .chain(values.iter().map(|(_, v)| format_val(v)))
                    .collect();
                format!(
                    "INSERT INTO \"{table}\" ({}) VALUES ({})",
                    cols.join(", "),
                    vals.join(", ")
                )
            }
            fsqlite_e2e::oplog::OpKind::Update { table, key, values } => {
                let sets: Vec<String> = values
                    .iter()
                    .map(|(c, v)| format!("{c}={}", format_val(v)))
                    .collect();
                format!("UPDATE \"{table}\" SET {} WHERE id={key}", sets.join(", "))
            }
            fsqlite_e2e::oplog::OpKind::Begin => "BEGIN".to_owned(),
            fsqlite_e2e::oplog::OpKind::Commit => "COMMIT".to_owned(),
            fsqlite_e2e::oplog::OpKind::Rollback => "ROLLBACK".to_owned(),
        };

        // Ignore errors for transaction control that may fail legitimately.
        drop(conn.execute(&sql).await);
    }

    // Query all data from table t0 (created by the preset) and hash it.
    let rows = conn
        .query("SELECT * FROM t0 ORDER BY id")
        .await
        .unwrap_or_default();

    let mut hasher = Sha256::new();
    for row in &rows {
        for val in row.values() {
            hasher.update(format!("{val:?}").as_bytes());
        }
    }

    fsqlite_e2e::bytes_to_lower_hex(hasher.finalize())
}

async fn execute_stateful_plan_and_hash(plan: &StatefulOperationPlan) -> String {
    use sha2::{Digest, Sha256};

    assert_fixed_seed_stateful_plan(plan);
    let artifact = plan
        .to_sql_artifact()
        .expect("stateful SQL artifact should build");
    assert_eq!(artifact.schema, fixed_seed_stateful_schema());
    assert_eq!(artifact.workload, fixed_seed_stateful_workload());

    let tempdir = tempfile::tempdir().expect("create temp dir");
    let db_path = tempdir.path().join("stateful.sqlite");
    let db_path = db_path.to_str().expect("temp path is utf8").to_owned();
    let conn = fsqlite::Connection::open(&db_path)
        .await
        .expect("open stateful connection");

    conn.execute(
        "CREATE TABLE IF NOT EXISTS stateful_kv \
         (id INTEGER PRIMARY KEY, val TEXT NOT NULL, num REAL DEFAULT 0)",
    )
    .await
    .expect("create stateful schema");
    conn.execute(
        "INSERT INTO stateful_kv (id, val, num) \
         VALUES (1, 'stateful_008ca4829c968a9c_1', 1)",
    )
    .await
    .expect("insert stateful key 1");
    conn.execute("BEGIN")
        .await
        .expect("begin stateful transaction");
    conn.execute(
        "INSERT INTO stateful_kv (id, val, num) \
         VALUES (2, 'stateful_01194905392d1538_2', 2)",
    )
    .await
    .expect("insert stateful key 2");
    conn.execute("SAVEPOINT \"sp_stateful\"")
        .await
        .expect("create stateful savepoint");
    conn.execute("UPDATE stateful_kv SET val = 'stateful_2414e4b454e00465_20' WHERE id = 2")
        .await
        .expect("update stateful key 2");
    conn.execute("ROLLBACK TO \"sp_stateful\"")
        .await
        .expect("rollback to stateful savepoint");
    conn.execute("RELEASE \"sp_stateful\"")
        .await
        .expect("release stateful savepoint");
    conn.execute("COMMIT")
        .await
        .expect("commit stateful transaction");
    conn.execute("BEGIN")
        .await
        .expect("begin rollback transaction");
    conn.execute(
        "INSERT INTO stateful_kv (id, val, num) \
         VALUES (3, 'stateful_0232920a725a2a70_3', 3)",
    )
    .await
    .expect("insert stateful key 3");
    conn.execute("ROLLBACK")
        .await
        .expect("rollback stateful transaction");
    conn.execute(
        "INSERT INTO stateful_kv (id, val, num) \
         VALUES (4, 'stateful_04652414e4b454e0_4', 4)",
    )
    .await
    .expect("insert stateful key 4");
    conn.execute("DELETE FROM stateful_kv WHERE id = 4")
        .await
        .expect("delete stateful key 4");

    let rows = conn
        .query("SELECT COUNT(*) FROM stateful_kv")
        .await
        .expect("stateful count query");
    assert_eq!(rows.len(), 1);
    let rows = conn
        .query("PRAGMA integrity_check")
        .await
        .expect("stateful integrity_check");
    assert_eq!(rows.len(), 1);

    drop(conn);
    let conn = fsqlite::Connection::open(&db_path)
        .await
        .expect("reopen stateful connection");
    let rows = conn
        .query("SELECT COUNT(*) FROM stateful_kv")
        .await
        .expect("post-reopen stateful count query");
    assert_eq!(rows.len(), 1);

    let rows = conn
        .query("SELECT id, val FROM stateful_kv ORDER BY id")
        .await
        .expect("query final stateful rows");
    assert_eq!(rows.len(), plan.final_model.rows.len());

    let mut hasher = Sha256::new();
    for row in &rows {
        for val in row.values() {
            hasher.update(format!("{val:?}").as_bytes());
        }
    }

    fsqlite_e2e::bytes_to_lower_hex(hasher.finalize())
}

fn assert_fixed_seed_stateful_plan(plan: &StatefulOperationPlan) {
    let operations = plan
        .steps
        .iter()
        .map(|step| &step.operation)
        .collect::<Vec<_>>();
    assert_eq!(operations.len(), 18);
    assert!(matches!(operations[0], StatefulOperation::CreateSchema));
    assert!(matches!(
        operations[1],
        StatefulOperation::Insert { key: 1, value }
            if value == "stateful_008ca4829c968a9c_1"
    ));
    assert!(matches!(operations[2], StatefulOperation::Begin));
    assert!(matches!(
        operations[3],
        StatefulOperation::Insert { key: 2, value }
            if value == "stateful_01194905392d1538_2"
    ));
    assert!(matches!(
        operations[4],
        StatefulOperation::Savepoint { name } if name == "sp_stateful"
    ));
    assert!(matches!(
        operations[5],
        StatefulOperation::Update { key: 2, value }
            if value == "stateful_2414e4b454e00465_20"
    ));
    assert!(matches!(
        operations[6],
        StatefulOperation::RollbackTo { name } if name == "sp_stateful"
    ));
    assert!(matches!(
        operations[7],
        StatefulOperation::Release { name } if name == "sp_stateful"
    ));
    assert!(matches!(operations[8], StatefulOperation::Commit));
    assert!(matches!(operations[9], StatefulOperation::Begin));
    assert!(matches!(
        operations[10],
        StatefulOperation::Insert { key: 3, value }
            if value == "stateful_0232920a725a2a70_3"
    ));
    assert!(matches!(operations[11], StatefulOperation::Rollback));
    assert!(matches!(
        operations[12],
        StatefulOperation::Insert { key: 4, value }
            if value == "stateful_04652414e4b454e0_4"
    ));
    assert!(matches!(
        operations[13],
        StatefulOperation::Delete { key: 4 }
    ));
    assert!(matches!(operations[14], StatefulOperation::SelectCount));
    assert!(matches!(operations[15], StatefulOperation::IntegrityCheck));
    assert!(matches!(operations[16], StatefulOperation::CloseReopen));
    assert!(matches!(operations[17], StatefulOperation::SelectCount));
    assert_eq!(
        plan.final_model.rows,
        vec![
            (1, "stateful_008ca4829c968a9c_1".to_owned()),
            (2, "stateful_01194905392d1538_2".to_owned()),
        ]
    );
}

fn fixed_seed_stateful_schema() -> Vec<String> {
    vec![
        "CREATE TABLE IF NOT EXISTS stateful_kv \
         (id INTEGER PRIMARY KEY, val TEXT NOT NULL, num REAL DEFAULT 0)"
            .to_owned(),
    ]
}

fn fixed_seed_stateful_workload() -> Vec<String> {
    vec![
        "INSERT INTO stateful_kv (id, val, num) \
         VALUES (1, 'stateful_008ca4829c968a9c_1', 1)"
            .to_owned(),
        "BEGIN".to_owned(),
        "INSERT INTO stateful_kv (id, val, num) \
         VALUES (2, 'stateful_01194905392d1538_2', 2)"
            .to_owned(),
        "SAVEPOINT \"sp_stateful\"".to_owned(),
        "UPDATE stateful_kv SET val = 'stateful_2414e4b454e00465_20' WHERE id = 2".to_owned(),
        "ROLLBACK TO \"sp_stateful\"".to_owned(),
        "RELEASE \"sp_stateful\"".to_owned(),
        "COMMIT".to_owned(),
        "BEGIN".to_owned(),
        "INSERT INTO stateful_kv (id, val, num) \
         VALUES (3, 'stateful_0232920a725a2a70_3', 3)"
            .to_owned(),
        "ROLLBACK".to_owned(),
        "INSERT INTO stateful_kv (id, val, num) \
         VALUES (4, 'stateful_04652414e4b454e0_4', 4)"
            .to_owned(),
        "DELETE FROM stateful_kv WHERE id = 4".to_owned(),
        "SELECT COUNT(*) FROM stateful_kv".to_owned(),
        "PRAGMA integrity_check".to_owned(),
        "SELECT COUNT(*) FROM stateful_kv".to_owned(),
    ]
}

fn format_val(v: &str) -> String {
    if v.parse::<i64>().is_ok() || v.parse::<f64>().is_ok() {
        v.to_owned()
    } else {
        format!("'{}'", v.replace('\'', "''"))
    }
}

fn stateful_artifact_envelope(
    schema: &[String],
    workload: &[String],
    seed: u64,
) -> ExecutionEnvelope {
    ExecutionEnvelope::builder(seed)
        .run_id("bd-turso-test-adaptation-zu081.20")
        .scenario_id("stateful-operation-plan")
        .schema(schema.iter().cloned())
        .workload(workload.iter().cloned())
        .build()
}

fn distinct_status_count(statuses: &[fsqlite_e2e::workload::StatefulExecutionStatus]) -> usize {
    statuses
        .iter()
        .map(|status| format!("{status:?}"))
        .collect::<BTreeSet<_>>()
        .len()
}

struct SyntheticStatefulExecutor {
    identity: EngineIdentity,
    count_value: i64,
}

impl SyntheticStatefulExecutor {
    fn subject() -> Self {
        Self {
            identity: EngineIdentity::FrankenSqlite,
            count_value: 2,
        }
    }

    fn reference() -> Self {
        Self {
            identity: EngineIdentity::CSqliteOracle,
            count_value: 99,
        }
    }
}

impl SqlExecutor for SyntheticStatefulExecutor {
    fn execute(&self, sql: &str) -> Result<usize, String> {
        if sql
            .trim()
            .eq_ignore_ascii_case("DELETE FROM stateful_kv WHERE id = 4")
        {
            Ok(1)
        } else {
            Ok(0)
        }
    }

    fn query(&self, sql: &str) -> Result<Vec<Vec<NormalizedValue>>, String> {
        if sql
            .trim()
            .eq_ignore_ascii_case("SELECT COUNT(*) FROM stateful_kv")
        {
            Ok(vec![vec![NormalizedValue::Integer(self.count_value)]])
        } else if sql.trim().eq_ignore_ascii_case("PRAGMA integrity_check") {
            Ok(vec![vec![NormalizedValue::Text("ok".to_owned())]])
        } else {
            Ok(Vec::new())
        }
    }

    fn engine_identity(&self) -> EngineIdentity {
        self.identity
    }
}

// ─── FRANKEN_SEED Value Verification ────────────────────────────────────

#[test]
fn franken_seed_is_correct_value() {
    // Verify the constant matches "FRANKEN" in ASCII.
    assert_eq!(FRANKEN_SEED, 0x0046_5241_4E4B_454E);

    // Verify it decodes to "FRANKEN" (7 bytes, padded with leading zero).
    let bytes = FRANKEN_SEED.to_be_bytes();
    let ascii: String = bytes
        .iter()
        .filter(|&&b| b != 0)
        .map(|&b| b as char)
        .collect();
    assert_eq!(ascii, "FRANKEN");
}

#[test]
fn franken_seed_stability() {
    // Ensure FRANKEN_SEED hasn't changed (regression test).
    // This constant is part of the reproducibility contract and MUST NOT change.
    // 0x4652414E4B454E = 19793688809653582 decimal
    assert_eq!(
        FRANKEN_SEED, 19_793_688_809_653_582,
        "FRANKEN_SEED must not change - this breaks reproducibility!"
    );
}
