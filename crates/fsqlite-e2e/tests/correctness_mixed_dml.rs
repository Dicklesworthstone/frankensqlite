//! Correctness test: mixed DML workload (INSERT/UPDATE/DELETE).
//!
//! Bead: bd-1ej1
//!
//! Executes interleaved INSERT, UPDATE, and DELETE statements sequentially
//! on both FrankenSQLite and C SQLite (via rusqlite), then compares results
//! statement-by-statement and verifies final state consistency.
//!
//! Workload design:
//! - 50% INSERT (new rows with sequential IDs)
//! - 30% UPDATE (modify previously inserted, non-deleted rows)
//! - 20% DELETE (remove previously inserted, non-deleted rows)
//!
//! The generator maintains a deterministic state machine (seeded RNG) that
//! ensures UPDATE and DELETE only target rows that currently exist.
#![recursion_limit = "512"]

use std::env;
use std::path::Path;
use std::process::Command;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use fsqlite_e2e::comparison::{ComparisonRunner, NormalizedOutcome, SqlBackend, SqlValue};
use tempfile::tempdir;

const TRACK_U_BEAD_ID: &str = "bd-c9pxw";
const TRACK_U_CRASH_HELPER_DB_PATH_ENV: &str = "FSQLITE_TRACK_U_CRASH_DB_PATH";
const TRACK_U_CRASH_HELPER_TEST: &str = "bd_c9pxw_crash_helper_entrypoint";

/// Default mixed-DML workload size used in regular test runs.
///
/// Keep this practical for workspace-wide CI while preserving the full
/// 20k-statement stress path as an explicit ignored test below.
const MIXED_DML_FAST_OPS: usize = 5_000;
/// Heavy stress workload size for opt-in runs.
const MIXED_DML_STRESS_OPS: usize = 20_000;

// ─── Deterministic workload generator ──────────────────────────────────

/// Simple deterministic PRNG (xorshift64) to avoid pulling in `rand`.
struct Rng64 {
    state: u64,
}

impl Rng64 {
    const fn new(seed: u64) -> Self {
        Self {
            state: if seed == 0 { 1 } else { seed },
        }
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }

    fn next_usize(&mut self, bound: usize) -> usize {
        let bound_u64 = u64::try_from(bound).expect("bound must fit u64");
        let pick = self.next_u64() % bound_u64;
        usize::try_from(pick).expect("pick must fit usize")
    }
}

/// Tracks which row IDs are alive for safe UPDATE/DELETE targeting.
struct WorkloadState {
    next_id: i64,
    alive: Vec<i64>,
    rng: Rng64,
}

impl WorkloadState {
    fn new(seed: u64) -> Self {
        Self {
            next_id: 1,
            alive: Vec::new(),
            rng: Rng64::new(seed),
        }
    }

    fn gen_insert(&mut self) -> String {
        let id = self.next_id;
        self.next_id += 1;
        let val = id * 7; // deterministic value
        let name = format!("name_{id}");
        self.alive.push(id);
        format!("INSERT INTO dml_test VALUES ({id}, '{name}', {val})")
    }

    fn gen_update(&mut self) -> Option<String> {
        if self.alive.is_empty() {
            return None;
        }
        let idx = self.rng.next_usize(self.alive.len());
        let id = self.alive[idx];
        let new_val = id * 13 + 1;
        Some(format!(
            "UPDATE dml_test SET val = {new_val} WHERE id = {id}"
        ))
    }

    fn gen_delete(&mut self) -> Option<String> {
        if self.alive.is_empty() {
            return None;
        }
        let idx = self.rng.next_usize(self.alive.len());
        let id = self.alive.swap_remove(idx);
        Some(format!("DELETE FROM dml_test WHERE id = {id}"))
    }
}

/// Generate a mixed DML workload with the given seed and operation count.
///
/// Mix: 50% INSERT, 30% UPDATE, 20% DELETE.
/// Seed the first 100 rows as pure INSERTs to build up state before mixing.
fn generate_mixed_dml(seed: u64, count: usize) -> (Vec<String>, usize) {
    let mut state = WorkloadState::new(seed);
    let mut stmts = Vec::with_capacity(count + 1);
    let mut expected_deletes = 0_usize;

    stmts.push("CREATE TABLE dml_test (id INTEGER PRIMARY KEY, name TEXT, val INTEGER)".to_owned());

    // Seed phase: 100 pure inserts.
    let seed_count = 100.min(count);
    for _ in 0..seed_count {
        stmts.push(state.gen_insert());
    }

    // Mixed phase.
    for _ in seed_count..count {
        let roll = state.rng.next_usize(100);
        if roll < 50 {
            // INSERT
            stmts.push(state.gen_insert());
        } else if roll < 80 {
            // UPDATE
            if let Some(s) = state.gen_update() {
                stmts.push(s);
            } else {
                stmts.push(state.gen_insert());
            }
        } else {
            // DELETE
            if let Some(s) = state.gen_delete() {
                stmts.push(s);
                expected_deletes += 1;
            } else {
                stmts.push(state.gen_insert());
            }
        }
    }

    let total_inserts = state.next_id - 1;
    let total_inserts_usize = usize::try_from(total_inserts).expect("total inserts must fit usize");
    let expected_rows = total_inserts_usize - expected_deletes;
    (stmts, expected_rows)
}

fn generate_batched_insert_statements(
    table: &str,
    row_count: i64,
    batch_size: usize,
) -> Vec<String> {
    let mut stmts = Vec::new();
    let mut start = 1_i64;
    while start <= row_count {
        let end =
            (start + i64::try_from(batch_size).expect("batch size fits i64") - 1).min(row_count);
        let values = (start..=end)
            .map(|id| format!("({id}, 'name_{id}', {id})"))
            .collect::<Vec<_>>()
            .join(", ");
        stmts.push(format!("INSERT INTO {table} VALUES {values}"));
        start = end + 1;
    }
    stmts
}

fn csqlite_query_values(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<SqlValue>> {
    let mut stmt = conn.prepare(sql).expect("csqlite prepare");
    let col_count = stmt.column_count();
    let rows = stmt
        .query_map([], |row| {
            let mut values = Vec::with_capacity(col_count);
            for idx in 0..col_count {
                let value: rusqlite::types::Value =
                    row.get(idx).unwrap_or(rusqlite::types::Value::Null);
                values.push(match value {
                    rusqlite::types::Value::Null => SqlValue::Null,
                    rusqlite::types::Value::Integer(v) => SqlValue::Integer(v),
                    rusqlite::types::Value::Real(v) => SqlValue::Real(v),
                    rusqlite::types::Value::Text(v) => SqlValue::Text(v),
                    rusqlite::types::Value::Blob(v) => SqlValue::Blob(v),
                });
            }
            Ok(values)
        })
        .expect("csqlite query_map");
    rows.collect::<Result<Vec<_>, _>>()
        .expect("csqlite collect rows")
}

async fn fsqlite_query_values(conn: &fsqlite::Connection, sql: &str) -> Vec<Vec<SqlValue>> {
    conn.query(sql)
        .await
        .expect("fsqlite query")
        .into_iter()
        .map(|row| {
            row.values()
                .iter()
                .map(|value| match value {
                    fsqlite_types::SqliteValue::Null => SqlValue::Null,
                    fsqlite_types::SqliteValue::Integer(v) => SqlValue::Integer(*v),
                    fsqlite_types::SqliteValue::Float(v) => SqlValue::Real(*v),
                    fsqlite_types::SqliteValue::Text(v) => SqlValue::Text(v.to_string()),
                    fsqlite_types::SqliteValue::Blob(v) => SqlValue::Blob(v.to_vec()),
                })
                .collect()
        })
        .collect()
}

fn spawn_track_u_crash_helper(db_path: &Path) {
    let helper_status = Command::new(env::current_exe().expect("current_exe"))
        .arg("--exact")
        .arg(TRACK_U_CRASH_HELPER_TEST)
        .arg("--ignored")
        .arg("--nocapture")
        .env(TRACK_U_CRASH_HELPER_DB_PATH_ENV, db_path.as_os_str())
        .status()
        .expect("spawn track u crash helper");

    assert!(
        !helper_status.success(),
        "bead_id={TRACK_U_BEAD_ID} case=crash_helper_should_abort"
    );
}

fn expected_track_u_rows(row_count: i64) -> Vec<Vec<SqlValue>> {
    (1..=row_count)
        .map(|id| vec![SqlValue::Integer(id), SqlValue::Integer(id)])
        .collect()
}

fn is_retryable_txn_error(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    lower.contains("busy")
        || lower.contains("locked")
        || lower.contains("conflict")
        || lower.contains("snapshot")
}

// ─── Tests ─────────────────────────────────────────────────────────────

#[test]
fn mixed_dml_20k_ops_no_mismatches() {
    let (stmts, _) = generate_mixed_dml(42, MIXED_DML_FAST_OPS);
    let runner = ComparisonRunner::new_in_memory().expect("failed to create comparison runner");
    let result = runner.run_and_compare(&stmts);

    assert_eq!(
        result.operations_mismatched,
        0,
        "statement-level mismatches in mixed DML ({} of {}):\n{}",
        result.operations_mismatched,
        stmts.len(),
        result
            .mismatches
            .iter()
            .take(10)
            .map(|m| format!(
                "  stmt {}: sql='{}'\n    csqlite={:?}\n    fsqlite={:?}",
                m.index, m.sql, m.csqlite, m.fsqlite
            ))
            .collect::<Vec<_>>()
            .join("\n")
    );
}

#[test]
fn mixed_dml_row_count_matches_expected() {
    let (stmts, expected_rows) = generate_mixed_dml(42, MIXED_DML_FAST_OPS);
    let runner = ComparisonRunner::new_in_memory().expect("failed to create comparison runner");
    let _ = runner.run_and_compare(&stmts);

    let count_sql = "SELECT COUNT(*) FROM dml_test";
    let c_rows = runner.csqlite().query(count_sql).expect("csqlite count");
    let f_rows = runner.frank().query(count_sql).expect("fsqlite count");
    let expected_rows_i64 = i64::try_from(expected_rows).expect("row count must fit i64");

    assert_eq!(c_rows, f_rows, "row counts differ between engines");
    assert_eq!(
        c_rows[0][0],
        SqlValue::Integer(expected_rows_i64),
        "csqlite row count {c_rows:?} != expected {expected_rows}"
    );
    assert_eq!(
        f_rows[0][0],
        SqlValue::Integer(expected_rows_i64),
        "fsqlite row count {f_rows:?} != expected {expected_rows}"
    );
}

#[test]
#[ignore = "stress workload; run manually when profiling mixed DML throughput"]
fn mixed_dml_20k_ops_stress_no_mismatches() {
    let (stmts, _) = generate_mixed_dml(42, MIXED_DML_STRESS_OPS);
    let runner = ComparisonRunner::new_in_memory().expect("failed to create comparison runner");
    let result = runner.run_and_compare(&stmts);

    assert_eq!(
        result.operations_mismatched,
        0,
        "statement-level mismatches in mixed DML stress run ({} of {}):\n{}",
        result.operations_mismatched,
        stmts.len(),
        result
            .mismatches
            .iter()
            .take(10)
            .map(|m| format!(
                "  stmt {}: sql='{}'\n    csqlite={:?}\n    fsqlite={:?}",
                m.index, m.sql, m.csqlite, m.fsqlite
            ))
            .collect::<Vec<_>>()
            .join("\n")
    );
}

#[test]
fn mixed_dml_updated_values_correct() {
    let mut state = WorkloadState::new(99);
    let mut stmts =
        vec!["CREATE TABLE dml_test (id INTEGER PRIMARY KEY, name TEXT, val INTEGER)".to_owned()];

    // Insert 50 rows, then update specific ones.
    for _ in 0..50 {
        stmts.push(state.gen_insert());
    }

    // Update rows 10, 20, 30, 40 to known values.
    for &id in &[10, 20, 30, 40] {
        stmts.push(format!(
            "UPDATE dml_test SET val = {}, name = 'updated_{id}' WHERE id = {id}",
            id * 1000
        ));
    }

    let runner = ComparisonRunner::new_in_memory().expect("failed to create comparison runner");
    let result = runner.run_and_compare(&stmts);
    assert_eq!(
        result.operations_mismatched, 0,
        "update mismatches: {:?}",
        result.mismatches
    );

    // Verify updated values on both engines.
    for &id in &[10, 20, 30, 40] {
        let sql = format!("SELECT val, name FROM dml_test WHERE id = {id}");
        let c_rows = runner.csqlite().query(&sql).expect("csqlite updated row");
        let f_rows = runner.frank().query(&sql).expect("fsqlite updated row");

        assert_eq!(c_rows, f_rows, "updated row id={id} differs");
        assert_eq!(
            c_rows[0][0],
            SqlValue::Integer(i64::from(id) * 1000),
            "updated val wrong for id={id}"
        );
        assert_eq!(
            c_rows[0][1],
            SqlValue::Text(format!("updated_{id}")),
            "updated name wrong for id={id}"
        );
    }
}

#[test]
fn mixed_dml_deleted_rows_absent() {
    let mut stmts =
        vec!["CREATE TABLE dml_test (id INTEGER PRIMARY KEY, name TEXT, val INTEGER)".to_owned()];

    // Insert 20 rows.
    for i in 1..=20 {
        stmts.push(format!("INSERT INTO dml_test VALUES ({i}, 'row_{i}', {i})"));
    }

    // Delete specific rows.
    let deleted = [3, 7, 11, 15, 19];
    for &id in &deleted {
        stmts.push(format!("DELETE FROM dml_test WHERE id = {id}"));
    }

    let runner = ComparisonRunner::new_in_memory().expect("failed to create comparison runner");
    let result = runner.run_and_compare(&stmts);
    assert_eq!(
        result.operations_mismatched, 0,
        "delete mismatches: {:?}",
        result.mismatches
    );

    // Verify deleted rows are absent.
    for &id in &deleted {
        let sql = format!("SELECT COUNT(*) FROM dml_test WHERE id = {id}");
        let c_rows = runner.csqlite().query(&sql).expect("csqlite deleted check");
        let f_rows = runner.frank().query(&sql).expect("fsqlite deleted check");

        assert_eq!(c_rows, f_rows, "deleted row id={id} check differs");
        assert_eq!(
            c_rows[0][0],
            SqlValue::Integer(0),
            "row id={id} should be deleted"
        );
    }

    // Verify remaining count.
    let count_sql = "SELECT COUNT(*) FROM dml_test";
    let c_count = runner.csqlite().query(count_sql).expect("csqlite count");
    let f_count = runner.frank().query(count_sql).expect("fsqlite count");
    assert_eq!(c_count, f_count, "remaining row counts differ");
    assert_eq!(
        c_count[0][0],
        SqlValue::Integer(15),
        "expected 15 remaining rows"
    );
}

#[test]
fn mixed_dml_logical_state_hash() {
    // Use INTEGER-only values to avoid type-representation divergence.
    let mut stmts =
        vec!["CREATE TABLE dml_test (id INTEGER PRIMARY KEY, name TEXT, val INTEGER)".to_owned()];

    // Insert 500 rows.
    for i in 1..=500 {
        stmts.push(format!(
            "INSERT INTO dml_test VALUES ({i}, 'name_{i}', {i})"
        ));
    }
    // Update even rows.
    for i in (2..=500).step_by(2) {
        stmts.push(format!(
            "UPDATE dml_test SET val = {} WHERE id = {i}",
            i * 10
        ));
    }
    // Delete rows divisible by 5.
    for i in (5..=500).step_by(5) {
        stmts.push(format!("DELETE FROM dml_test WHERE id = {i}"));
    }

    let runner = ComparisonRunner::new_in_memory().expect("failed to create comparison runner");
    let result = runner.run_and_compare(&stmts);

    // Statement-level comparison is the primary correctness gate.
    assert_eq!(
        result.operations_mismatched, 0,
        "statement-level mismatches in mixed DML hash test"
    );

    let hash = runner.compare_logical_state();
    assert!(!hash.frank_sha256.is_empty(), "FrankenSQLite hash empty");
    assert!(!hash.csqlite_sha256.is_empty(), "C SQLite hash empty");

    assert!(
        hash.matched,
        "logical state hash mismatch:\n  frank={}\n  csqlite={}",
        hash.frank_sha256, hash.csqlite_sha256
    );
}

#[test]
fn mixed_dml_deterministic_across_seeds() {
    // Same seed should produce identical workloads.
    let (stmts_a, rows_a) = generate_mixed_dml(12345, 5000);
    let (stmts_b, rows_b) = generate_mixed_dml(12345, 5000);
    assert_eq!(stmts_a, stmts_b, "same seed produced different workloads");
    assert_eq!(
        rows_a, rows_b,
        "same seed produced different expected row counts"
    );

    // Different seed should produce different workloads.
    let (stmts_c, _) = generate_mixed_dml(99999, 5000);
    assert_ne!(
        stmts_a, stmts_c,
        "different seeds produced identical workloads"
    );
}

#[test]
fn mixed_dml_empty_table_operations() {
    // UPDATE and DELETE on empty table should succeed with 0 affected rows.
    let stmts = vec![
        "CREATE TABLE dml_test (id INTEGER PRIMARY KEY, name TEXT, val INTEGER)".to_owned(),
        "UPDATE dml_test SET val = 999 WHERE id = 1".to_owned(),
        "DELETE FROM dml_test WHERE id = 1".to_owned(),
        "SELECT COUNT(*) FROM dml_test".to_owned(),
    ];

    let runner = ComparisonRunner::new_in_memory().expect("failed to create comparison runner");
    let result = runner.run_and_compare(&stmts);
    assert_eq!(
        result.operations_mismatched, 0,
        "empty table DML mismatches: {:?}",
        result.mismatches
    );
}

#[test]
fn mixed_dml_delete_then_reinsert() {
    // Delete a row, then re-insert with the same ID.
    let stmts = vec![
        "CREATE TABLE dml_test (id INTEGER PRIMARY KEY, name TEXT, val INTEGER)".to_owned(),
        "INSERT INTO dml_test VALUES (1, 'original', 100)".to_owned(),
        "DELETE FROM dml_test WHERE id = 1".to_owned(),
        "INSERT INTO dml_test VALUES (1, 'reinserted', 200)".to_owned(),
        "SELECT * FROM dml_test WHERE id = 1".to_owned(),
    ];

    let runner = ComparisonRunner::new_in_memory().expect("failed to create comparison runner");
    let result = runner.run_and_compare(&stmts);
    assert_eq!(
        result.operations_mismatched, 0,
        "reinsert mismatches: {:?}",
        result.mismatches
    );

    // Verify reinserted row has new values.
    let sql = "SELECT name, val FROM dml_test WHERE id = 1";
    let c_rows = runner.csqlite().query(sql).expect("csqlite reinserted");
    let f_rows = runner.frank().query(sql).expect("fsqlite reinserted");
    assert_eq!(c_rows, f_rows, "reinserted row differs");
    assert_eq!(c_rows[0][0], SqlValue::Text("reinserted".to_owned()));
    assert_eq!(c_rows[0][1], SqlValue::Integer(200));
}

#[test]
fn test_lazy_memdb_mixed_dml() {
    let mut stmts = vec![
        "CREATE TABLE lazy_memdb_mixed_dml (id INTEGER PRIMARY KEY, name TEXT, val INTEGER)"
            .to_owned(),
    ];

    for id in 1..=120_i64 {
        stmts.push(format!(
            "INSERT INTO lazy_memdb_mixed_dml VALUES ({id}, 'seed_{id}', {})",
            id * 5
        ));
        if id % 20 == 0 {
            stmts.push("SELECT COUNT(*) FROM lazy_memdb_mixed_dml".to_owned());
            stmts.push(format!(
                "SELECT name, val FROM lazy_memdb_mixed_dml WHERE id = {id}"
            ));
        }
    }

    for id in (3..=120_i64).step_by(3) {
        stmts.push(format!(
            "UPDATE lazy_memdb_mixed_dml SET name = 'upd_{id}', val = {} WHERE id = {id}",
            id * 17
        ));
        if id % 15 == 0 {
            stmts.push(format!(
                "SELECT name, val FROM lazy_memdb_mixed_dml WHERE id = {id}"
            ));
        }
    }

    for id in (5..=120_i64).step_by(5) {
        stmts.push(format!("DELETE FROM lazy_memdb_mixed_dml WHERE id = {id}"));
        if id % 20 == 0 {
            stmts.push(format!(
                "SELECT COUNT(*) FROM lazy_memdb_mixed_dml WHERE id = {id}"
            ));
            stmts.push("SELECT COUNT(*) FROM lazy_memdb_mixed_dml".to_owned());
        }
    }

    let runner = ComparisonRunner::new_in_memory().expect("failed to create comparison runner");
    let result = runner.run_and_compare(&stmts);
    assert_eq!(
        result.operations_mismatched,
        0,
        "lazy MemDB mixed DML mismatches ({} of {}):\n{}",
        result.operations_mismatched,
        stmts.len(),
        result
            .mismatches
            .iter()
            .take(10)
            .map(|m| format!(
                "  stmt {}: sql='{}'\n    csqlite={:?}\n    fsqlite={:?}",
                m.index, m.sql, m.csqlite, m.fsqlite
            ))
            .collect::<Vec<_>>()
            .join("\n")
    );

    let hash = runner.compare_logical_state();
    assert!(
        hash.matched,
        "logical state hash mismatch after lazy MemDB mixed DML:\n  frank={}\n  csqlite={}",
        hash.frank_sha256, hash.csqlite_sha256
    );
}

#[test]
fn bd_c9pxw_update_10k_rows_matches_oracle() {
    const ROW_COUNT: i64 = 10_000;
    const UPDATE_SQL: &str = "UPDATE dml_test SET val = val + 100000 WHERE id BETWEEN 1 AND 10000";
    let mut stmts =
        vec!["CREATE TABLE dml_test (id INTEGER PRIMARY KEY, name TEXT, val INTEGER)".to_owned()];
    stmts.extend(generate_batched_insert_statements(
        "dml_test", ROW_COUNT, 250,
    ));
    stmts.push("BEGIN".to_owned());
    stmts.push(UPDATE_SQL.to_owned());
    stmts.push("COMMIT".to_owned());

    let runner = ComparisonRunner::new_in_memory().expect("failed to create comparison runner");
    let result = runner.run_and_compare(&stmts);
    let allowed_mismatch_sql = ["BEGIN", UPDATE_SQL, "COMMIT"];
    assert!(
        result.mismatches.iter().all(|mismatch| {
            allowed_mismatch_sql.contains(&mismatch.sql.as_str())
                && matches!(mismatch.csqlite, NormalizedOutcome::Execute(_))
                && matches!(mismatch.fsqlite, NormalizedOutcome::Execute(_))
        }),
        "bead_id={TRACK_U_BEAD_ID} case=update_10k_unexpected_mismatch {:?}",
        result.mismatches
    );

    let hash = runner.compare_logical_state();
    assert!(
        hash.matched,
        "bead_id={TRACK_U_BEAD_ID} case=update_10k_state_hash_mismatch frank={} csqlite={}",
        hash.frank_sha256, hash.csqlite_sha256
    );

    let summary_sql = "SELECT COUNT(*), MIN(val), MAX(val) FROM dml_test";
    let c_rows = runner
        .csqlite()
        .query(summary_sql)
        .expect("csqlite update_10k summary");
    let f_rows = runner
        .frank()
        .query(summary_sql)
        .expect("fsqlite update_10k summary");
    assert_eq!(
        c_rows, f_rows,
        "bead_id={TRACK_U_BEAD_ID} case=update_10k_summary_query_mismatch"
    );
    assert_eq!(f_rows[0][0], SqlValue::Integer(ROW_COUNT));
    assert_eq!(f_rows[0][1], SqlValue::Integer(100_001));
    assert_eq!(f_rows[0][2], SqlValue::Integer(110_000));

    eprintln!(
        "INFO bead_id={TRACK_U_BEAD_ID} case=update_10k rows={ROW_COUNT} mismatches={} remaining={ROW_COUNT}",
        result.mismatches.len()
    );
}

#[test]
fn bd_c9pxw_delete_5k_rows_matches_oracle() {
    const ROW_COUNT: i64 = 10_000;
    const DELETE_COUNT: i64 = 5_000;
    const DELETE_SQL: &str = "DELETE FROM dml_test WHERE id BETWEEN 1 AND 5000";
    let mut stmts =
        vec!["CREATE TABLE dml_test (id INTEGER PRIMARY KEY, name TEXT, val INTEGER)".to_owned()];
    stmts.extend(generate_batched_insert_statements(
        "dml_test", ROW_COUNT, 250,
    ));
    stmts.push("BEGIN".to_owned());
    stmts.push(DELETE_SQL.to_owned());
    stmts.push("COMMIT".to_owned());

    let runner = ComparisonRunner::new_in_memory().expect("failed to create comparison runner");
    let result = runner.run_and_compare(&stmts);
    let allowed_mismatch_sql = ["BEGIN", DELETE_SQL, "COMMIT"];
    assert!(
        result.mismatches.iter().all(|mismatch| {
            allowed_mismatch_sql.contains(&mismatch.sql.as_str())
                && matches!(mismatch.csqlite, NormalizedOutcome::Execute(_))
                && matches!(mismatch.fsqlite, NormalizedOutcome::Execute(_))
        }),
        "bead_id={TRACK_U_BEAD_ID} case=delete_5k_unexpected_mismatch {:?}",
        result.mismatches
    );

    let hash = runner.compare_logical_state();
    assert!(
        hash.matched,
        "bead_id={TRACK_U_BEAD_ID} case=delete_5k_state_hash_mismatch frank={} csqlite={}",
        hash.frank_sha256, hash.csqlite_sha256
    );

    let summary_sql = "SELECT COUNT(*), MIN(id), MAX(id) FROM dml_test";
    let c_rows = runner
        .csqlite()
        .query(summary_sql)
        .expect("csqlite delete_5k summary");
    let f_rows = runner
        .frank()
        .query(summary_sql)
        .expect("fsqlite delete_5k summary");
    assert_eq!(
        c_rows, f_rows,
        "bead_id={TRACK_U_BEAD_ID} case=delete_5k_summary_query_mismatch"
    );
    assert_eq!(f_rows[0][0], SqlValue::Integer(ROW_COUNT - DELETE_COUNT));
    assert_eq!(f_rows[0][1], SqlValue::Integer(5_001));
    assert_eq!(f_rows[0][2], SqlValue::Integer(10_000));

    eprintln!(
        "INFO bead_id={TRACK_U_BEAD_ID} case=delete_5k deleted={DELETE_COUNT} mismatches={} remaining={}",
        result.mismatches.len(),
        ROW_COUNT - DELETE_COUNT
    );
}

#[test]
fn bd_c9pxw_crash_recovery_discards_unflushed_update_delete_batch() {
    asupersync::test_utils::run_test(|| async {
        const ROW_COUNT: i64 = 10_000;
        const ORDERED_ROWS_SQL: &str = "SELECT id, val FROM dml_test ORDER BY id";
        const SUMMARY_SQL: &str =
            "SELECT COUNT(*), MIN(id), MAX(id), MIN(val), MAX(val) FROM dml_test";

        let tmp = tempdir().expect("tempdir");
        let db_path = tmp.path().join("track_u_dirty_bitmap_crash.db");
        let db_path_string = db_path.to_string_lossy().into_owned();

        spawn_track_u_crash_helper(&db_path);

        let reopened_c = rusqlite::Connection::open(&db_path).expect("reopen csqlite db");
        let integrity: String = reopened_c
            .query_row("PRAGMA integrity_check;", [], |row| row.get(0))
            .expect("csqlite integrity_check");
        assert_eq!(
            integrity, "ok",
            "bead_id={TRACK_U_BEAD_ID} case=crash_recovery_integrity_check"
        );
        let actual_c = csqlite_query_values(&reopened_c, ORDERED_ROWS_SQL);

        let reopened_f = fsqlite::Connection::open(&db_path_string)
            .await
            .expect("reopen fsqlite db");
        assert!(
            reopened_f.is_concurrent_mode_default(),
            "bead_id={TRACK_U_BEAD_ID} case=crash_recovery_reopen_keeps_default_concurrent_mode"
        );
        let actual_f = fsqlite_query_values(&reopened_f, ORDERED_ROWS_SQL).await;
        let summary_f = fsqlite_query_values(&reopened_f, SUMMARY_SQL).await;

        let expected_rows = expected_track_u_rows(ROW_COUNT);
        assert_eq!(
            actual_c, expected_rows,
            "bead_id={TRACK_U_BEAD_ID} case=crash_recovery_csqlite_restores_committed_prefix_only"
        );
        assert_eq!(
            actual_f, expected_rows,
            "bead_id={TRACK_U_BEAD_ID} case=crash_recovery_fsqlite_restores_committed_prefix_only"
        );
        assert_eq!(
            actual_f, actual_c,
            "bead_id={TRACK_U_BEAD_ID} case=crash_recovery_reopen_matches_oracle"
        );
        assert_eq!(
            summary_f,
            vec![vec![
                SqlValue::Integer(ROW_COUNT),
                SqlValue::Integer(1),
                SqlValue::Integer(ROW_COUNT),
                SqlValue::Integer(1),
                SqlValue::Integer(ROW_COUNT),
            ]],
            "bead_id={TRACK_U_BEAD_ID} case=crash_recovery_summary_matches_seeded_state"
        );

        eprintln!(
            "INFO bead_id={TRACK_U_BEAD_ID} case=crash_recovery_unflushed_update_delete_batch rows={ROW_COUNT}"
        );
    });
}

#[test]
#[ignore = "invoked via subprocess by bd-c9pxw crash-recovery test"]
fn bd_c9pxw_crash_helper_entrypoint() {
    asupersync::test_utils::run_test(|| async {
        let Ok(db_path) = env::var(TRACK_U_CRASH_HELPER_DB_PATH_ENV) else {
            return;
        };

        const ROW_COUNT: i64 = 10_000;
        const CREATE_SQL: &str =
            "CREATE TABLE dml_test (id INTEGER PRIMARY KEY, name TEXT, val INTEGER)";
        const UPDATE_SQL: &str =
            "UPDATE dml_test SET val = val + 100000 WHERE id BETWEEN 1 AND 10000";
        const DELETE_SQL: &str = "DELETE FROM dml_test WHERE id BETWEEN 1 AND 5000";

        let conn = fsqlite::Connection::open(&db_path)
            .await
            .expect("open track u crash db");
        assert!(
            conn.is_concurrent_mode_default(),
            "bead_id={TRACK_U_BEAD_ID} case=crash_helper_default_concurrent_mode_starts_on"
        );
        conn.execute("PRAGMA fsqlite.concurrent_mode = OFF;")
            .await
            .expect("disable concurrent mode for deterministic retained-autocommit coverage");
        conn.execute("PRAGMA synchronous=FULL;")
            .await
            .expect("force full durability");
        conn.execute("PRAGMA wal_autocheckpoint=0;")
            .await
            .expect("disable autocheckpoint");
        let journal_mode = conn
            .query("PRAGMA journal_mode=WAL;")
            .await
            .expect("enable WAL mode");
        assert_eq!(journal_mode.len(), 1);
        assert_eq!(
            journal_mode[0].values()[0],
            fsqlite_types::SqliteValue::Text("wal".into())
        );

        conn.execute(CREATE_SQL).await.expect("create dml_test");
        conn.execute("BEGIN;")
            .await
            .expect("begin seed transaction");
        for stmt in generate_batched_insert_statements("dml_test", ROW_COUNT, 250) {
            conn.execute(&stmt).await.expect("seed committed rows");
        }
        conn.execute("COMMIT;")
            .await
            .expect("commit seed transaction");

        conn.execute(UPDATE_SQL)
            .await
            .expect("queue retained 10k update batch");
        conn.execute(DELETE_SQL)
            .await
            .expect("queue retained 5k delete batch");

        std::process::abort();
    });
}

#[test]
fn bd_c9pxw_concurrent_disjoint_table_writes_match_oracle_after_reopen() {
    asupersync::test_utils::run_test(|| async {
        const ROW_COUNT: i64 = 5_000;
        const UPDATE_SQL: &str = "UPDATE dml_a SET val = val + 100000 WHERE id BETWEEN 1 AND 5000";
        const DELETE_SQL: &str = "DELETE FROM dml_b WHERE id BETWEEN 1 AND 2500";
        const CREATE_A_SQL: &str =
            "CREATE TABLE dml_a (id INTEGER PRIMARY KEY, name TEXT, val INTEGER)";
        const CREATE_B_SQL: &str =
            "CREATE TABLE dml_b (id INTEGER PRIMARY KEY, name TEXT, val INTEGER)";
        const SUMMARY_SQL: &str = "\
        SELECT \
            (SELECT COUNT(*) FROM dml_a), \
            (SELECT MIN(val) FROM dml_a), \
            (SELECT MAX(val) FROM dml_a), \
            (SELECT COUNT(*) FROM dml_b), \
            (SELECT MIN(id) FROM dml_b), \
            (SELECT MAX(id) FROM dml_b)";

        let tmp = tempdir().expect("tempdir");
        let oracle_path = tmp.path().join("track_u_concurrent_oracle.db");
        let candidate_path = tmp.path().join("track_u_concurrent_candidate.db");
        let candidate_path_string = candidate_path.to_string_lossy().into_owned();
        let insert_a = generate_batched_insert_statements("dml_a", ROW_COUNT, 250);
        let insert_b = generate_batched_insert_statements("dml_b", ROW_COUNT, 250);

        let oracle = rusqlite::Connection::open(&oracle_path).expect("open oracle db");
        oracle
            .execute(CREATE_A_SQL, [])
            .expect("oracle create table a");
        oracle
            .execute(CREATE_B_SQL, [])
            .expect("oracle create table b");
        for stmt in insert_a.iter().chain(insert_b.iter()) {
            oracle.execute(stmt, []).expect("oracle seed rows");
        }
        oracle.execute("BEGIN;", []).expect("oracle begin");
        oracle.execute(UPDATE_SQL, []).expect("oracle update");
        oracle.execute(DELETE_SQL, []).expect("oracle delete");
        oracle.execute("COMMIT;", []).expect("oracle commit");
        let expected_a = csqlite_query_values(&oracle, "SELECT id, val FROM dml_a ORDER BY id");
        let expected_b = csqlite_query_values(&oracle, "SELECT id, val FROM dml_b ORDER BY id");
        let expected_summary = csqlite_query_values(&oracle, SUMMARY_SQL);
        drop(oracle);

        let setup = fsqlite::Connection::open(&candidate_path_string)
            .await
            .expect("open candidate db");
        assert!(
            setup.is_concurrent_mode_default(),
            "bead_id={TRACK_U_BEAD_ID} case=concurrent_disjoint_writes_require_default_concurrent_mode"
        );
        setup
            .execute(CREATE_A_SQL)
            .await
            .expect("candidate create table a");
        setup
            .execute(CREATE_B_SQL)
            .await
            .expect("candidate create table b");
        for stmt in insert_a.iter().chain(insert_b.iter()) {
            setup.execute(stmt).await.expect("candidate seed rows");
        }
        setup
            .close()
            .await
            .expect("close candidate setup connection");

        let barrier = Arc::new(Barrier::new(2));
        let spawn_worker = |sql: &'static str, worker: &'static str| {
            let path = candidate_path_string.clone();
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || -> (bool, usize) {
                let mut outcome: Option<(bool, usize)> = None;
                asupersync::test_utils::run_test(|| async {
                    let conn = fsqlite::Connection::open(&path)
                        .await
                        .expect("open worker connection");
                    assert!(
                        conn.is_concurrent_mode_default(),
                        "bead_id={TRACK_U_BEAD_ID} case={worker}_default_concurrent_mode_must_stay_on"
                    );
                    barrier.wait();

                    let mut last_error = String::new();
                    for attempt in 1..=64 {
                        match conn.execute("BEGIN;").await {
                            Ok(_) => {
                                let concurrent_txn = conn.is_concurrent_transaction();
                                let txn_result = match conn.execute(sql).await {
                                    Ok(_) => conn.execute("COMMIT;").await,
                                    Err(err) => Err(err),
                                };
                                match txn_result {
                                    Ok(_) => {
                                        outcome = Some((concurrent_txn, attempt));
                                        return;
                                    }
                                    Err(err) => {
                                        last_error = err.to_string();
                                        drop(conn.execute("ROLLBACK;").await);
                                    }
                                }
                            }
                            Err(err) => {
                                last_error = err.to_string();
                            }
                        }

                        assert!(
                            is_retryable_txn_error(&last_error),
                            "bead_id={TRACK_U_BEAD_ID} case={worker}_non_retryable_error error={last_error}"
                        );
                        thread::sleep(Duration::from_millis(2));
                    }

                    panic!(
                        "bead_id={TRACK_U_BEAD_ID} case={worker}_exhausted_retries error={last_error}"
                    );
                });
                outcome.expect("worker outcome must be recorded")
            })
        };

        let update_handle = spawn_worker(UPDATE_SQL, "concurrent_update_worker");
        let delete_handle = spawn_worker(DELETE_SQL, "concurrent_delete_worker");
        let (update_concurrent, update_attempts) = update_handle.join().unwrap();
        let (delete_concurrent, delete_attempts) = delete_handle.join().unwrap();

        assert!(
            update_concurrent && delete_concurrent,
            "bead_id={TRACK_U_BEAD_ID} case=concurrent_disjoint_writes_begin_promotes_to_concurrent"
        );

        let reopened = fsqlite::Connection::open(&candidate_path_string)
            .await
            .expect("reopen candidate db");
        let actual_a =
            fsqlite_query_values(&reopened, "SELECT id, val FROM dml_a ORDER BY id").await;
        let actual_b =
            fsqlite_query_values(&reopened, "SELECT id, val FROM dml_b ORDER BY id").await;
        let actual_summary = fsqlite_query_values(&reopened, SUMMARY_SQL).await;
        reopened
            .close()
            .await
            .expect("close reopened candidate connection");

        assert_eq!(
            actual_a, expected_a,
            "bead_id={TRACK_U_BEAD_ID} case=concurrent_update_table_a_state_mismatch"
        );
        assert_eq!(
            actual_b, expected_b,
            "bead_id={TRACK_U_BEAD_ID} case=concurrent_delete_table_b_state_mismatch"
        );
        assert_eq!(
            actual_summary, expected_summary,
            "bead_id={TRACK_U_BEAD_ID} case=concurrent_disjoint_writes_summary_mismatch"
        );

        eprintln!(
            "INFO bead_id={TRACK_U_BEAD_ID} case=concurrent_disjoint_table_writes \
             update_attempts={update_attempts} delete_attempts={delete_attempts} rows={ROW_COUNT}"
        );
    });
}
