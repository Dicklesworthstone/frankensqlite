//! Correctness test: transaction patterns (BEGIN/COMMIT/ROLLBACK/SAVEPOINT).
//!
//! Bead: bd-24uu
//!
//! Verifies that FrankenSQLite's transaction handling produces identical
//! results to C SQLite across all major transaction patterns:
//! - Simple commit
//! - Transaction rollback
//! - Savepoint with partial rollback
//! - Nested savepoints
//! - Implicit autocommit
//! - Large transactional batches
//!
//! Transaction control statements (BEGIN, COMMIT, ROLLBACK, SAVEPOINT, RELEASE)
//! may report different `changes()` counts across engines (e.g., rusqlite
//! returns 1 for COMMIT while FrankenSQLite returns 0). This is cosmetic.
//! These tests verify **state correctness**: the data visible after each
//! transaction pattern must be identical on both engines.
#![recursion_limit = "512"]

use fsqlite_e2e::comparison::{ComparisonRunner, SqlBackend, SqlValue};
use serde_json::json;
use std::env;
use std::path::Path;
use std::process::Command;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;
use tempfile::tempdir;

use fsqlite_core::connection::{
    hot_path_profile_enabled, hot_path_profile_snapshot, reset_hot_path_profile,
    set_hot_path_profile_enabled,
};

const SCENARIO_COMPLETENESS_BEAD_ID: &str = "bd-mblr.4";
const SCENARIO_COMPLETENESS_SEED: u64 = 0x006D_626C_722E_3400;
const SCENARIO_COMPLETENESS_REPLAY: &str =
    "cargo test -p fsqlite-e2e --test correctness_transactions -- --nocapture --test-threads=1";
const RETAINED_AUTOCOMMIT_CRASH_HELPER_DB_PATH_ENV: &str =
    "FSQLITE_RETAINED_AUTOCOMMIT_CRASH_DB_PATH";
const RETAINED_AUTOCOMMIT_CRASH_HELPER_TEST: &str = "retained_autocommit_crash_helper_entrypoint";

// ─── Helpers ───────────────────────────────────────────────────────────

/// Execute all statements on both backends (ignoring per-statement row-count
/// differences for transaction control), then verify the listed queries
/// produce identical results on both engines.
fn run_scenario(setup: &[&str], verify: &[(&str, &[SqlValue])]) {
    let runner = ComparisonRunner::new_in_memory().expect("failed to create comparison runner");

    // Execute all setup statements on both engines.
    for sql in setup {
        let c_res = runner.csqlite().execute(sql);
        let f_res = runner.frank().execute(sql);

        // Both must succeed (but we don't compare affected row counts for
        // transaction control statements).
        let is_txn_ctrl = {
            let upper = sql.trim().to_uppercase();
            upper.starts_with("BEGIN")
                || upper.starts_with("COMMIT")
                || upper.starts_with("ROLLBACK")
                || upper.starts_with("SAVEPOINT")
                || upper.starts_with("RELEASE")
                || upper.starts_with("END")
        };

        if is_txn_ctrl {
            // Both must succeed (or both fail).
            assert!(
                c_res.is_ok() == f_res.is_ok(),
                "txn control outcome diverged for '{sql}':\n  csqlite={c_res:?}\n  fsqlite={f_res:?}"
            );
        } else {
            // For DML/DDL, both must succeed.
            assert!(c_res.is_ok(), "csqlite failed on '{sql}': {c_res:?}");
            assert!(f_res.is_ok(), "fsqlite failed on '{sql}': {f_res:?}");
        }
    }

    // Verify final state matches.
    for (sql, expected_first_row) in verify {
        let c_rows = runner.csqlite().query(sql).expect("csqlite verify");
        let f_rows = runner.frank().query(sql).expect("fsqlite verify");
        assert_eq!(c_rows, f_rows, "verify query differs: {sql}");
        if !expected_first_row.is_empty() {
            assert!(!c_rows.is_empty(), "expected rows for verify query: {sql}");
            assert_eq!(
                &c_rows[0][..expected_first_row.len()],
                *expected_first_row,
                "first row mismatch for: {sql}"
            );
        }
    }
}

fn csqlite_query_values(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<SqlValue>> {
    let mut stmt = conn.prepare(sql).expect("csqlite prepare");
    let col_count = stmt.column_count();
    let rows = stmt
        .query_map([], |row| {
            let mut values = Vec::with_capacity(col_count);
            for i in 0..col_count {
                let value: rusqlite::types::Value =
                    row.get(i).unwrap_or(rusqlite::types::Value::Null);
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

async fn fsqlite_count_probe(conn: &fsqlite::Connection, sql: &str) -> String {
    match conn.query(sql).await {
        Ok(rows) => rows
            .first()
            .and_then(|row| row.values().first())
            .map_or_else(|| "no-row".to_owned(), ToString::to_string),
        Err(err) => format!("error:{err}"),
    }
}

async fn mode_mix_post_rollback_probe(path: &str, conn: &fsqlite::Connection, id: i64) -> String {
    let sql = format!("SELECT COUNT(*) FROM mode_mix WHERE id = {id};");
    let same_connection = fsqlite_count_probe(conn, &sql).await;
    let fresh_connection = match fsqlite::Connection::open(path).await {
        Ok(fresh) => {
            let count = fsqlite_count_probe(&fresh, &sql).await;
            let close_result = fresh.close().await;
            if let Err(err) = close_result {
                format!("{count};close_error:{err}")
            } else {
                count
            }
        }
        Err(err) => format!("open_error:{err}"),
    };
    format!("post-rollback id={id} same_conn={same_connection} fresh_conn={fresh_connection}")
}

struct HotPathProfileGuard {
    was_enabled: bool,
}

impl HotPathProfileGuard {
    fn new() -> Self {
        let was_enabled = hot_path_profile_enabled();
        reset_hot_path_profile();
        set_hot_path_profile_enabled(true);
        Self { was_enabled }
    }
}

impl Drop for HotPathProfileGuard {
    fn drop(&mut self) {
        set_hot_path_profile_enabled(self.was_enabled);
        reset_hot_path_profile();
    }
}

fn spawn_retained_autocommit_crash_helper(db_path: &Path) {
    let helper_status = Command::new(env::current_exe().expect("current_exe"))
        .arg("--exact")
        .arg(RETAINED_AUTOCOMMIT_CRASH_HELPER_TEST)
        .arg("--ignored")
        .arg("--nocapture")
        .env(
            RETAINED_AUTOCOMMIT_CRASH_HELPER_DB_PATH_ENV,
            db_path.as_os_str(),
        )
        .status()
        .expect("spawn retained-autocommit crash helper");

    assert!(
        !helper_status.success(),
        "retained-autocommit crash helper should abort"
    );
}

fn checkpoint_triplet(rows: &[Vec<SqlValue>], label: &str) -> (i64, i64, i64) {
    assert_eq!(rows.len(), 1, "{label}: expected one row");
    assert_eq!(rows[0].len(), 3, "{label}: expected three columns");
    let read_i64 = |idx: usize| -> i64 {
        if let SqlValue::Integer(v) = &rows[0][idx] {
            *v
        } else {
            assert!(
                matches!(rows[0][idx], SqlValue::Integer(_)),
                "{label}: expected integer at index {idx}, got {:?}",
                rows[0][idx]
            );
            0
        }
    };
    (read_i64(0), read_i64(1), read_i64(2))
}

fn emit_scenario_completeness_log(test_name: &str, phase: &str, extra: serde_json::Value) {
    eprintln!(
        "SCENARIO_COMPLETENESS:{}",
        json!({
            "bead_id": SCENARIO_COMPLETENESS_BEAD_ID,
            "seed": SCENARIO_COMPLETENESS_SEED,
            "replay_command": SCENARIO_COMPLETENESS_REPLAY,
            "test_name": test_name,
            "phase": phase,
            "extra": extra
        })
    );
}

fn sql_rows_to_json(rows: &[Vec<SqlValue>]) -> serde_json::Value {
    serde_json::Value::Array(
        rows.iter()
            .map(|row| {
                serde_json::Value::Array(
                    row.iter()
                        .map(|value| match value {
                            SqlValue::Null => serde_json::Value::Null,
                            SqlValue::Integer(v) => json!(*v),
                            SqlValue::Real(v) => json!(*v),
                            SqlValue::Text(v) => json!(v),
                            SqlValue::Blob(v) => json!(v),
                        })
                        .collect(),
                )
            })
            .collect(),
    )
}

fn is_retryable_txn_error(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    lower.contains("busy")
        || lower.contains("locked")
        || lower.contains("conflict")
        || lower.contains("snapshot")
}

async fn configure_connection_mode(conn: &fsqlite::Connection, concurrent_mode: bool) {
    let pragma = if concurrent_mode {
        "PRAGMA fsqlite.concurrent_mode=ON;"
    } else {
        "PRAGMA fsqlite.concurrent_mode=OFF;"
    };
    conn.execute(pragma)
        .await
        .expect("set concurrent_mode pragma");
    assert_eq!(
        conn.is_concurrent_mode_default(),
        concurrent_mode,
        "concurrent_mode pragma must remain connection-local"
    );
}

// ─── Scenario A: Simple transaction commit ─────────────────────────────

#[test]
fn txn_simple_commit() {
    run_scenario(
        &[
            "CREATE TABLE test (id INTEGER PRIMARY KEY, val TEXT)",
            "BEGIN",
            "INSERT INTO test VALUES (1, 'a')",
            "INSERT INTO test VALUES (2, 'b')",
            "COMMIT",
        ],
        &[
            ("SELECT COUNT(*) FROM test", &[SqlValue::Integer(2)]),
            (
                "SELECT val FROM test WHERE id = 1",
                &[SqlValue::Text("a".to_owned())],
            ),
            (
                "SELECT val FROM test WHERE id = 2",
                &[SqlValue::Text("b".to_owned())],
            ),
        ],
    );
}

// ─── Scenario B: Transaction rollback ──────────────────────────────────

#[test]
fn txn_rollback_discards_all() {
    run_scenario(
        &[
            "CREATE TABLE test (id INTEGER PRIMARY KEY, val TEXT)",
            "INSERT INTO test VALUES (99, 'pre_existing')",
            "BEGIN",
            "INSERT INTO test VALUES (1, 'a')",
            "INSERT INTO test VALUES (2, 'b')",
            "ROLLBACK",
        ],
        &[
            ("SELECT COUNT(*) FROM test", &[SqlValue::Integer(1)]),
            (
                "SELECT val FROM test WHERE id = 99",
                &[SqlValue::Text("pre_existing".to_owned())],
            ),
            (
                "SELECT COUNT(*) FROM test WHERE id IN (1, 2)",
                &[SqlValue::Integer(0)],
            ),
        ],
    );
}

// ─── Scenario C: Savepoint with partial rollback ───────────────────────

#[test]
fn txn_savepoint_partial_rollback() {
    run_scenario(
        &[
            "CREATE TABLE test (id INTEGER PRIMARY KEY, val TEXT)",
            "BEGIN",
            "INSERT INTO test VALUES (1, 'a')",
            "SAVEPOINT sp1",
            "INSERT INTO test VALUES (2, 'b')",
            "ROLLBACK TO sp1",
            "INSERT INTO test VALUES (3, 'c')",
            "COMMIT",
        ],
        &[
            ("SELECT COUNT(*) FROM test", &[SqlValue::Integer(2)]),
            (
                "SELECT val FROM test WHERE id = 1",
                &[SqlValue::Text("a".to_owned())],
            ),
            (
                "SELECT COUNT(*) FROM test WHERE id = 2",
                &[SqlValue::Integer(0)],
            ),
            (
                "SELECT val FROM test WHERE id = 3",
                &[SqlValue::Text("c".to_owned())],
            ),
        ],
    );
}

// ─── Scenario D: Nested savepoints ─────────────────────────────────────

#[test]
fn txn_nested_savepoints() {
    run_scenario(
        &[
            "CREATE TABLE test (id INTEGER PRIMARY KEY, val TEXT)",
            "BEGIN",
            "SAVEPOINT sp1",
            "INSERT INTO test VALUES (1, 'a')",
            "SAVEPOINT sp2",
            "INSERT INTO test VALUES (2, 'b')",
            "ROLLBACK TO sp2",
            "RELEASE sp1",
            "COMMIT",
        ],
        &[
            ("SELECT COUNT(*) FROM test", &[SqlValue::Integer(1)]),
            (
                "SELECT val FROM test WHERE id = 1",
                &[SqlValue::Text("a".to_owned())],
            ),
            (
                "SELECT COUNT(*) FROM test WHERE id = 2",
                &[SqlValue::Integer(0)],
            ),
        ],
    );
}

// ─── Scenario E: Implicit autocommit ───────────────────────────────────

#[test]
fn txn_implicit_autocommit() {
    run_scenario(
        &[
            "CREATE TABLE test (id INTEGER PRIMARY KEY, val TEXT)",
            "INSERT INTO test VALUES (1, 'a')",
            "INSERT INTO test VALUES (2, 'b')",
        ],
        &[
            ("SELECT COUNT(*) FROM test", &[SqlValue::Integer(2)]),
            (
                "SELECT val FROM test WHERE id = 1",
                &[SqlValue::Text("a".to_owned())],
            ),
            (
                "SELECT val FROM test WHERE id = 2",
                &[SqlValue::Text("b".to_owned())],
            ),
        ],
    );
}

// ─── Scenario F: Large transactional batch ─────────────────────────────

#[test]
fn txn_large_batch_commit() {
    let mut stmts: Vec<&str> = Vec::new();
    let owned: Vec<String>;
    {
        let mut v = Vec::with_capacity(1125);
        v.push("CREATE TABLE test (id INTEGER PRIMARY KEY, val INTEGER)".to_owned());
        v.push("BEGIN".to_owned());
        for i in 1..=1000 {
            v.push(format!("INSERT INTO test VALUES ({i}, {})", i * 3));
        }
        for i in (10..=1000).step_by(10) {
            v.push(format!("UPDATE test SET val = {} WHERE id = {i}", i * 100));
        }
        for i in (5..=100).step_by(5) {
            v.push(format!("DELETE FROM test WHERE id = {i}"));
        }
        v.push("COMMIT".to_owned());
        owned = v;
    }
    stmts.extend(owned.iter().map(String::as_str));

    let runner = ComparisonRunner::new_in_memory().expect("failed to create comparison runner");
    for sql in &stmts {
        let c_res = runner.csqlite().execute(sql);
        assert!(
            c_res.is_ok(),
            "csqlite: {:?} on {}",
            c_res.as_ref().err(),
            sql
        );
        let _ = c_res.unwrap();

        let f_res = runner.frank().execute(sql);
        assert!(
            f_res.is_ok(),
            "fsqlite: {:?} on {}",
            f_res.as_ref().err(),
            sql
        );
        let _ = f_res.unwrap();
    }

    // Expected: 1000 inserts - 20 deletes = 980 rows.
    let c_count = runner
        .csqlite()
        .query("SELECT COUNT(*) FROM test")
        .expect("csqlite count");
    let f_count = runner
        .frank()
        .query("SELECT COUNT(*) FROM test")
        .expect("fsqlite count");
    assert_eq!(c_count, f_count, "row counts differ");
    assert_eq!(c_count[0][0], SqlValue::Integer(980));

    // Verify an updated row (id=110 is divisible by 10 but not by 5 in
    // the delete range 5..=100, so it was updated but not deleted).
    let c_val = runner
        .csqlite()
        .query("SELECT val FROM test WHERE id = 110")
        .expect("csqlite check");
    let f_val = runner
        .frank()
        .query("SELECT val FROM test WHERE id = 110")
        .expect("fsqlite check");
    assert_eq!(c_val, f_val, "updated row differs");
    assert_eq!(c_val[0][0], SqlValue::Integer(11000));
}

// ─── Scenario G: Large batch rollback ──────────────────────────────────

#[test]
fn txn_large_batch_rollback() {
    let runner = ComparisonRunner::new_in_memory().expect("failed to create comparison runner");

    let setup = [
        "CREATE TABLE test (id INTEGER PRIMARY KEY, val INTEGER)",
        "INSERT INTO test VALUES (1, 100)",
        "BEGIN",
    ];
    for sql in &setup {
        runner.csqlite().execute(sql).expect("csqlite setup");
        runner.frank().execute(sql).expect("fsqlite setup");
    }

    for i in 2..=500 {
        let sql = format!("INSERT INTO test VALUES ({i}, {i})");
        runner.csqlite().execute(&sql).expect("csqlite insert");
        runner.frank().execute(&sql).expect("fsqlite insert");
    }

    runner
        .csqlite()
        .execute("ROLLBACK")
        .expect("csqlite rollback");
    runner
        .frank()
        .execute("ROLLBACK")
        .expect("fsqlite rollback");

    let c_count = runner
        .csqlite()
        .query("SELECT COUNT(*) FROM test")
        .expect("csqlite count");
    let f_count = runner
        .frank()
        .query("SELECT COUNT(*) FROM test")
        .expect("fsqlite count");
    assert_eq!(c_count, f_count, "row counts differ after rollback");
    assert_eq!(c_count[0][0], SqlValue::Integer(1));
}

// ─── Scenario H: Savepoint release collapses into parent ───────────────

#[test]
fn txn_savepoint_release_collapses() {
    run_scenario(
        &[
            "CREATE TABLE test (id INTEGER PRIMARY KEY, val TEXT)",
            "BEGIN",
            "INSERT INTO test VALUES (1, 'before_sp')",
            "SAVEPOINT sp1",
            "INSERT INTO test VALUES (2, 'in_sp')",
            "RELEASE sp1",
            "COMMIT",
        ],
        &[
            ("SELECT COUNT(*) FROM test", &[SqlValue::Integer(2)]),
            (
                "SELECT val FROM test WHERE id = 2",
                &[SqlValue::Text("in_sp".to_owned())],
            ),
        ],
    );
}

// ─── Scenario I: Multiple savepoints, rollback middle one ──────────────

#[test]
fn txn_multiple_savepoints_rollback_middle() {
    run_scenario(
        &[
            "CREATE TABLE test (id INTEGER PRIMARY KEY, val TEXT)",
            "BEGIN",
            "INSERT INTO test VALUES (1, 'base')",
            "SAVEPOINT sp1",
            "INSERT INTO test VALUES (2, 'sp1_data')",
            "SAVEPOINT sp2",
            "INSERT INTO test VALUES (3, 'sp2_data')",
            "SAVEPOINT sp3",
            "INSERT INTO test VALUES (4, 'sp3_data')",
            "ROLLBACK TO sp2",
            "INSERT INTO test VALUES (5, 'after_rollback')",
            "RELEASE sp1",
            "COMMIT",
        ],
        &[
            ("SELECT COUNT(*) FROM test", &[SqlValue::Integer(3)]),
            (
                "SELECT val FROM test WHERE id = 1",
                &[SqlValue::Text("base".to_owned())],
            ),
            (
                "SELECT val FROM test WHERE id = 2",
                &[SqlValue::Text("sp1_data".to_owned())],
            ),
            (
                "SELECT COUNT(*) FROM test WHERE id IN (3, 4)",
                &[SqlValue::Integer(0)],
            ),
            (
                "SELECT val FROM test WHERE id = 5",
                &[SqlValue::Text("after_rollback".to_owned())],
            ),
        ],
    );
}

// ─── Scenario J: Rollback to savepoint then re-use same name ───────────

#[test]
fn txn_savepoint_reuse_name() {
    run_scenario(
        &[
            "CREATE TABLE test (id INTEGER PRIMARY KEY, val TEXT)",
            "BEGIN",
            "SAVEPOINT sp1",
            "INSERT INTO test VALUES (1, 'first')",
            "ROLLBACK TO sp1",
            "SAVEPOINT sp1",
            "INSERT INTO test VALUES (2, 'second')",
            "RELEASE sp1",
            "COMMIT",
        ],
        &[
            ("SELECT COUNT(*) FROM test", &[SqlValue::Integer(1)]),
            (
                "SELECT COUNT(*) FROM test WHERE id = 1",
                &[SqlValue::Integer(0)],
            ),
            (
                "SELECT val FROM test WHERE id = 2",
                &[SqlValue::Text("second".to_owned())],
            ),
        ],
    );
}

#[test]
fn test_lazy_memdb_cross_statement_visibility() {
    let runner = ComparisonRunner::new_in_memory().expect("failed to create comparison runner");
    let steps = [
        (
            "CREATE TABLE lazy_memdb_txn (id INTEGER PRIMARY KEY, val TEXT, note INTEGER)",
            false,
        ),
        ("BEGIN", false),
        ("INSERT INTO lazy_memdb_txn VALUES (1, 'alpha', 10)", false),
        ("SELECT val, note FROM lazy_memdb_txn WHERE id = 1", true),
        ("INSERT INTO lazy_memdb_txn VALUES (2, 'beta', 20)", false),
        ("SELECT COUNT(*) FROM lazy_memdb_txn", true),
        (
            "UPDATE lazy_memdb_txn SET val = 'alpha2', note = 11 WHERE id = 1",
            false,
        ),
        ("SELECT val, note FROM lazy_memdb_txn WHERE id = 1", true),
        ("DELETE FROM lazy_memdb_txn WHERE id = 2", false),
        ("SELECT COUNT(*) FROM lazy_memdb_txn WHERE id = 2", true),
        ("INSERT INTO lazy_memdb_txn VALUES (3, 'gamma', 30)", false),
        ("SELECT id, val FROM lazy_memdb_txn ORDER BY id", true),
        ("COMMIT", false),
        ("SELECT id, val, note FROM lazy_memdb_txn ORDER BY id", true),
    ];

    let is_txn_control = |sql: &str| {
        let upper = sql.trim().to_uppercase();
        upper.starts_with("BEGIN")
            || upper.starts_with("COMMIT")
            || upper.starts_with("ROLLBACK")
            || upper.starts_with("SAVEPOINT")
            || upper.starts_with("RELEASE")
            || upper.starts_with("END")
    };

    for (sql, is_query) in steps {
        if is_query {
            let c_rows = runner.csqlite().query(sql).expect("csqlite query");
            let f_rows = runner.frank().query(sql).expect("fsqlite query");
            assert_eq!(
                c_rows, f_rows,
                "cross-statement visibility mismatch for query `{sql}`:\n  csqlite={c_rows:?}\n  fsqlite={f_rows:?}"
            );
            continue;
        }

        let c_res = runner.csqlite().execute(sql);
        let f_res = runner.frank().execute(sql);
        if is_txn_control(sql) {
            assert!(
                c_res.is_ok() == f_res.is_ok(),
                "transaction-control outcome diverged for `{sql}`:\n  csqlite={c_res:?}\n  fsqlite={f_res:?}"
            );
        } else {
            let c_changes = c_res.unwrap_or_else(|err| panic!("csqlite failed on `{sql}`: {err}"));
            let f_changes = f_res.unwrap_or_else(|err| panic!("fsqlite failed on `{sql}`: {err}"));
            assert_eq!(
                c_changes, f_changes,
                "affected-row count mismatch for `{sql}`: csqlite={c_changes} fsqlite={f_changes}"
            );
        }
    }

    let expected = vec![
        vec![
            SqlValue::Integer(1),
            SqlValue::Text("alpha2".to_owned()),
            SqlValue::Integer(11),
        ],
        vec![
            SqlValue::Integer(3),
            SqlValue::Text("gamma".to_owned()),
            SqlValue::Integer(30),
        ],
    ];
    let c_rows = runner
        .csqlite()
        .query("SELECT id, val, note FROM lazy_memdb_txn ORDER BY id")
        .expect("csqlite final query");
    let f_rows = runner
        .frank()
        .query("SELECT id, val, note FROM lazy_memdb_txn ORDER BY id")
        .expect("fsqlite final query");
    assert_eq!(c_rows, expected, "unexpected csqlite final rows");
    assert_eq!(f_rows, expected, "unexpected fsqlite final rows");
}

#[test]
fn test_lazy_memdb_multi_table_clean_read_then_dirty_visibility() {
    let runner = ComparisonRunner::new_in_memory().expect("failed to create comparison runner");
    let steps = [
        (
            "CREATE TABLE lazy_memdb_dirty_a (id INTEGER PRIMARY KEY, val TEXT)",
            false,
        ),
        (
            "CREATE TABLE lazy_memdb_clean_b (id INTEGER PRIMARY KEY, val TEXT)",
            false,
        ),
        ("INSERT INTO lazy_memdb_clean_b VALUES (1, 'seed')", false),
        ("BEGIN", false),
        ("INSERT INTO lazy_memdb_dirty_a VALUES (1, 'alpha')", false),
        ("SELECT id, val FROM lazy_memdb_clean_b ORDER BY id", true),
        ("SELECT id, val FROM lazy_memdb_dirty_a ORDER BY id", true),
        (
            "UPDATE lazy_memdb_dirty_a SET val = 'alpha2' WHERE id = 1",
            false,
        ),
        ("SELECT val FROM lazy_memdb_dirty_a WHERE id = 1", true),
        ("COMMIT", false),
        ("SELECT id, val FROM lazy_memdb_dirty_a ORDER BY id", true),
        ("SELECT id, val FROM lazy_memdb_clean_b ORDER BY id", true),
    ];

    let is_txn_control = |sql: &str| {
        let upper = sql.trim().to_uppercase();
        upper.starts_with("BEGIN")
            || upper.starts_with("COMMIT")
            || upper.starts_with("ROLLBACK")
            || upper.starts_with("SAVEPOINT")
            || upper.starts_with("RELEASE")
            || upper.starts_with("END")
    };

    for (sql, is_query) in steps {
        if is_query {
            let c_rows = runner.csqlite().query(sql).expect("csqlite query");
            let f_rows = runner.frank().query(sql).expect("fsqlite query");
            assert_eq!(
                c_rows, f_rows,
                "multi-table lazy dirty visibility mismatch for query `{sql}`:\n  csqlite={c_rows:?}\n  fsqlite={f_rows:?}"
            );
            continue;
        }

        let c_res = runner.csqlite().execute(sql);
        let f_res = runner.frank().execute(sql);
        if is_txn_control(sql) {
            assert!(
                c_res.is_ok() == f_res.is_ok(),
                "transaction-control outcome diverged for `{sql}`:\n  csqlite={c_res:?}\n  fsqlite={f_res:?}"
            );
        } else {
            let c_changes = c_res.unwrap_or_else(|err| panic!("csqlite failed on `{sql}`: {err}"));
            let f_changes = f_res.unwrap_or_else(|err| panic!("fsqlite failed on `{sql}`: {err}"));
            assert_eq!(
                c_changes, f_changes,
                "affected-row count mismatch for `{sql}`: csqlite={c_changes} fsqlite={f_changes}"
            );
        }
    }

    let expected_dirty = vec![vec![
        SqlValue::Integer(1),
        SqlValue::Text("alpha2".to_owned()),
    ]];
    let expected_clean = vec![vec![
        SqlValue::Integer(1),
        SqlValue::Text("seed".to_owned()),
    ]];
    let c_dirty = runner
        .csqlite()
        .query("SELECT id, val FROM lazy_memdb_dirty_a ORDER BY id")
        .expect("csqlite final dirty query");
    let f_dirty = runner
        .frank()
        .query("SELECT id, val FROM lazy_memdb_dirty_a ORDER BY id")
        .expect("fsqlite final dirty query");
    let c_clean = runner
        .csqlite()
        .query("SELECT id, val FROM lazy_memdb_clean_b ORDER BY id")
        .expect("csqlite final clean query");
    let f_clean = runner
        .frank()
        .query("SELECT id, val FROM lazy_memdb_clean_b ORDER BY id")
        .expect("fsqlite final clean query");
    assert_eq!(c_dirty, expected_dirty, "unexpected csqlite dirty rows");
    assert_eq!(f_dirty, expected_dirty, "unexpected fsqlite dirty rows");
    assert_eq!(c_clean, expected_clean, "unexpected csqlite clean rows");
    assert_eq!(f_clean, expected_clean, "unexpected fsqlite clean rows");
}

#[test]
fn test_lazy_memdb_multi_table_post_read_insert_count_regression() {
    let runner = ComparisonRunner::new_in_memory().expect("failed to create comparison runner");
    let steps = [
        (
            "CREATE TABLE lazy_memdb_dirty_a (id INTEGER PRIMARY KEY, val TEXT)",
            false,
        ),
        (
            "CREATE TABLE lazy_memdb_clean_b (id INTEGER PRIMARY KEY, val TEXT)",
            false,
        ),
        ("INSERT INTO lazy_memdb_clean_b VALUES (1, 'seed')", false),
        ("BEGIN", false),
        ("INSERT INTO lazy_memdb_dirty_a VALUES (1, 'alpha')", false),
        ("SELECT id, val FROM lazy_memdb_clean_b ORDER BY id", true),
        ("SELECT id, val FROM lazy_memdb_dirty_a ORDER BY id", true),
        (
            "UPDATE lazy_memdb_dirty_a SET val = 'alpha2' WHERE id = 1",
            false,
        ),
        ("SELECT val FROM lazy_memdb_dirty_a WHERE id = 1", true),
        ("INSERT INTO lazy_memdb_dirty_a VALUES (2, 'beta')", false),
        ("SELECT COUNT(*) FROM lazy_memdb_dirty_a", true),
        ("SELECT id, val FROM lazy_memdb_dirty_a ORDER BY id", true),
        ("COMMIT", false),
        ("SELECT id, val FROM lazy_memdb_dirty_a ORDER BY id", true),
        ("SELECT id, val FROM lazy_memdb_clean_b ORDER BY id", true),
    ];

    let is_txn_control = |sql: &str| {
        let upper = sql.trim().to_uppercase();
        upper.starts_with("BEGIN")
            || upper.starts_with("COMMIT")
            || upper.starts_with("ROLLBACK")
            || upper.starts_with("SAVEPOINT")
            || upper.starts_with("RELEASE")
            || upper.starts_with("END")
    };

    for (sql, is_query) in steps {
        if is_query {
            let c_rows = runner.csqlite().query(sql).expect("csqlite query");
            let f_rows = runner.frank().query(sql).expect("fsqlite query");
            assert_eq!(
                c_rows, f_rows,
                "post-read insert lazy dirty visibility mismatch for query `{sql}`:\n  csqlite={c_rows:?}\n  fsqlite={f_rows:?}"
            );
            continue;
        }

        let c_res = runner.csqlite().execute(sql);
        let f_res = runner.frank().execute(sql);
        if is_txn_control(sql) {
            assert!(
                c_res.is_ok() == f_res.is_ok(),
                "transaction-control outcome diverged for `{sql}`:\n  csqlite={c_res:?}\n  fsqlite={f_res:?}"
            );
        } else {
            let c_changes = c_res.unwrap_or_else(|err| panic!("csqlite failed on `{sql}`: {err}"));
            let f_changes = f_res.unwrap_or_else(|err| panic!("fsqlite failed on `{sql}`: {err}"));
            assert_eq!(
                c_changes, f_changes,
                "affected-row count mismatch for `{sql}`: csqlite={c_changes} fsqlite={f_changes}"
            );
        }
    }
}

// ─── Scenario K: WAL/checkpoint/journal-mode parity transitions ───────

#[test]
fn txn_wal_checkpoint_journal_mode_transitions_file_backed() {
    asupersync::test_utils::run_test(|| async {
        const BEAD_ID: &str = "bd-1dp9.4.1";
        const SEED: u64 = 0x1D94_0401;
        let run_id = format!("bd-1dp9.4.1-seed-{SEED}-wal-checkpoint-journal-transitions");

        let tmp = tempdir().expect("tempdir");
        let c_path = tmp.path().join("oracle_csqlite.db");
        let f_path = tmp.path().join("candidate_fsqlite.db");

        let c_conn = rusqlite::Connection::open(&c_path).expect("open csqlite db");
        let f_conn = fsqlite::Connection::open(f_path.to_string_lossy().as_ref())
            .await
            .expect("open fsqlite db");

        eprintln!(
            "bead_id={BEAD_ID} run_id={run_id} seed={SEED} phase=start c_db={} f_db={}",
            c_path.display(),
            f_path.display()
        );

        let c_mode_wal = csqlite_query_values(&c_conn, "PRAGMA journal_mode=WAL;");
        let f_mode_wal = fsqlite_query_values(&f_conn, "PRAGMA journal_mode=WAL;").await;
        eprintln!(
            "bead_id={BEAD_ID} run_id={run_id} seed={SEED} phase=mode_switch_wal c_mode={c_mode_wal:?} f_mode={f_mode_wal:?}"
        );
        assert_eq!(c_mode_wal, f_mode_wal, "journal_mode WAL response mismatch");

        let setup_sql = [
            "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);",
            "INSERT INTO t VALUES (1, 'alpha');",
            "INSERT INTO t VALUES (2, 'beta');",
        ];
        for sql in setup_sql {
            c_conn.execute(sql, []).expect("csqlite setup exec");
            f_conn.execute(sql).await.expect("fsqlite setup exec");
        }

        let c_ckpt_passive = csqlite_query_values(&c_conn, "PRAGMA wal_checkpoint(PASSIVE);");
        let f_ckpt_passive = fsqlite_query_values(&f_conn, "PRAGMA wal_checkpoint(PASSIVE);").await;
        let c_triplet = checkpoint_triplet(&c_ckpt_passive, "csqlite passive");
        let f_triplet = checkpoint_triplet(&f_ckpt_passive, "fsqlite passive");
        eprintln!(
            "bead_id={BEAD_ID} run_id={run_id} seed={SEED} phase=checkpoint_passive c={c_triplet:?} f={f_triplet:?}"
        );
        assert_eq!(c_triplet.0, 0, "csqlite passive busy should be 0");
        assert_eq!(f_triplet.0, 0, "fsqlite passive busy should be 0");
        assert!(c_triplet.1 >= 0 && c_triplet.2 >= 0);
        assert!(f_triplet.1 >= 0 && f_triplet.2 >= 0);

        let c_mode_delete = csqlite_query_values(&c_conn, "PRAGMA journal_mode='delete';");
        let f_mode_delete = fsqlite_query_values(&f_conn, "PRAGMA journal_mode='delete';").await;
        eprintln!(
            "bead_id={BEAD_ID} run_id={run_id} seed={SEED} phase=mode_switch_delete c_mode={c_mode_delete:?} f_mode={f_mode_delete:?}"
        );
        assert_eq!(
            c_mode_delete, f_mode_delete,
            "journal_mode DELETE response mismatch"
        );

        let c_ckpt_nonwal = csqlite_query_values(&c_conn, "PRAGMA wal_checkpoint(TRUNCATE);");
        let f_ckpt_nonwal = fsqlite_query_values(&f_conn, "PRAGMA wal_checkpoint(TRUNCATE);").await;
        eprintln!(
            "bead_id={BEAD_ID} run_id={run_id} seed={SEED} phase=checkpoint_nonwal c_rows={c_ckpt_nonwal:?} f_rows={f_ckpt_nonwal:?}"
        );
        assert_eq!(
            c_ckpt_nonwal, f_ckpt_nonwal,
            "non-WAL wal_checkpoint sentinel mismatch"
        );
        assert_eq!(
            f_ckpt_nonwal,
            vec![vec![
                SqlValue::Integer(0),
                SqlValue::Integer(-1),
                SqlValue::Integer(-1)
            ]],
            "expected SQLite sentinel row (0,-1,-1) in non-WAL mode"
        );

        let c_count = csqlite_query_values(&c_conn, "SELECT COUNT(*) FROM t;");
        let f_count = fsqlite_query_values(&f_conn, "SELECT COUNT(*) FROM t;").await;
        eprintln!(
            "bead_id={BEAD_ID} run_id={run_id} seed={SEED} phase=visibility_after_nonwal_ckpt c_count={c_count:?} f_count={f_count:?}"
        );
        assert_eq!(
            c_count, f_count,
            "row visibility mismatch after non-WAL checkpoint"
        );
    });
}

// ─── Scenario L: Journal mode PRAGMA response parity for all modes ────
//
// SQLite supports 6 journal modes: delete, truncate, persist, memory, wal, off.
// This test verifies that both engines return the same PRAGMA response string
// for each mode transition on a file-backed database.

#[test]
fn txn_journal_mode_all_modes_response_parity() {
    asupersync::test_utils::run_test(|| async {
        const BEAD_ID: &str = "bd-1dp9.4.1";
        const SEED: u64 = 0x1D94_0402;
        let run_id = format!("bd-1dp9.4.1-seed-{SEED}-journal-mode-all-modes");

        let tmp = tempdir().expect("tempdir");
        let c_path = tmp.path().join("oracle_csqlite_L.db");
        let f_path = tmp.path().join("candidate_fsqlite_L.db");

        let c_conn = rusqlite::Connection::open(&c_path).expect("open csqlite db");
        let f_conn = fsqlite::Connection::open(f_path.to_string_lossy().as_ref())
            .await
            .expect("open fsqlite db");

        eprintln!("bead_id={BEAD_ID} run_id={run_id} seed={SEED} phase=start");

        // Test each journal_mode transition. SQLite echoes back the mode that was
        // actually set (which may differ from the requested mode on some backends).
        let modes = [
            "wal", "delete", "truncate", "persist", "memory", "off", "wal",
        ];

        for mode in modes {
            let sql = format!("PRAGMA journal_mode='{mode}';");
            let c_resp = csqlite_query_values(&c_conn, &sql);
            let f_resp = fsqlite_query_values(&f_conn, &sql).await;
            eprintln!(
                "bead_id={BEAD_ID} run_id={run_id} seed={SEED} phase=mode_set mode={mode} c={c_resp:?} f={f_resp:?}"
            );
            assert_eq!(
                c_resp, f_resp,
                "journal_mode response mismatch for mode='{mode}'"
            );

            // Query should also match.
            let c_query = csqlite_query_values(&c_conn, "PRAGMA journal_mode;");
            let f_query = fsqlite_query_values(&f_conn, "PRAGMA journal_mode;").await;
            assert_eq!(
                c_query, f_query,
                "journal_mode query mismatch after setting mode='{mode}'"
            );
        }

        // Verify data integrity is maintained through mode transitions.
        c_conn
            .execute("CREATE TABLE t_L(id INTEGER PRIMARY KEY, v TEXT)", [])
            .expect("csqlite create");
        f_conn
            .execute("CREATE TABLE t_L(id INTEGER PRIMARY KEY, v TEXT)")
            .await
            .expect("fsqlite create");
        c_conn
            .execute("INSERT INTO t_L VALUES (1, 'mode_test')", [])
            .expect("csqlite insert");
        f_conn
            .execute("INSERT INTO t_L VALUES (1, 'mode_test')")
            .await
            .expect("fsqlite insert");

        let c_count = csqlite_query_values(&c_conn, "SELECT COUNT(*) FROM t_L;");
        let f_count = fsqlite_query_values(&f_conn, "SELECT COUNT(*) FROM t_L;").await;
        eprintln!(
            "bead_id={BEAD_ID} run_id={run_id} seed={SEED} phase=data_integrity c={c_count:?} f={f_count:?}"
        );
        assert_eq!(
            c_count, f_count,
            "data integrity mismatch after mode transitions"
        );
    });
}

// ─── Scenario M: Checkpoint modes with WAL data ──────────────────────
//
// Verifies that all 4 checkpoint modes (PASSIVE, FULL, RESTART, TRUNCATE)
// return parity results when WAL contains actual data.

#[test]
fn txn_checkpoint_all_modes_with_data() {
    asupersync::test_utils::run_test(|| async {
        const BEAD_ID: &str = "bd-1dp9.4.1";
        const SEED: u64 = 0x1D94_0403;
        let run_id = format!("bd-1dp9.4.1-seed-{SEED}-checkpoint-all-modes");

        let tmp = tempdir().expect("tempdir");
        let c_path = tmp.path().join("oracle_csqlite_M.db");
        let f_path = tmp.path().join("candidate_fsqlite_M.db");

        let c_conn = rusqlite::Connection::open(&c_path).expect("open csqlite db");
        let f_conn = fsqlite::Connection::open(f_path.to_string_lossy().as_ref())
            .await
            .expect("open fsqlite db");

        // Ensure both in WAL mode.
        csqlite_query_values(&c_conn, "PRAGMA journal_mode=WAL;");
        drop(fsqlite_query_values(&f_conn, "PRAGMA journal_mode=WAL;").await);

        // Disable auto-checkpoint so we can control when it happens.
        csqlite_query_values(&c_conn, "PRAGMA wal_autocheckpoint=0;");
        drop(fsqlite_query_values(&f_conn, "PRAGMA wal_autocheckpoint=0;").await);

        eprintln!("bead_id={BEAD_ID} run_id={run_id} seed={SEED} phase=start");

        // Insert data to populate the WAL.
        let setup_sql = [
            "CREATE TABLE t_M(id INTEGER PRIMARY KEY, v TEXT);",
            "INSERT INTO t_M VALUES (1, 'alpha');",
            "INSERT INTO t_M VALUES (2, 'beta');",
            "INSERT INTO t_M VALUES (3, 'gamma');",
        ];
        for sql in setup_sql {
            c_conn.execute(sql, []).expect("csqlite setup");
            f_conn.execute(sql).await.expect("fsqlite setup");
        }

        // Test PASSIVE checkpoint with data in WAL.
        let c_passive = csqlite_query_values(&c_conn, "PRAGMA wal_checkpoint(PASSIVE);");
        let f_passive = fsqlite_query_values(&f_conn, "PRAGMA wal_checkpoint(PASSIVE);").await;
        let c_trip = checkpoint_triplet(&c_passive, "csqlite passive");
        let f_trip = checkpoint_triplet(&f_passive, "fsqlite passive");
        eprintln!(
            "bead_id={BEAD_ID} run_id={run_id} seed={SEED} phase=checkpoint_passive c={c_trip:?} f={f_trip:?}"
        );
        // busy=0 for both (no concurrent readers in single-connection test).
        assert_eq!(c_trip.0, 0, "csqlite passive busy");
        assert_eq!(f_trip.0, 0, "fsqlite passive busy");

        // Add more data for next checkpoint.
        for i in 4..=6 {
            let sql = format!("INSERT INTO t_M VALUES ({i}, 'row_{i}');");
            c_conn.execute(&sql, []).expect("csqlite insert");
            f_conn.execute(&sql).await.expect("fsqlite insert");
        }

        // Test FULL checkpoint.
        let c_full = csqlite_query_values(&c_conn, "PRAGMA wal_checkpoint(FULL);");
        let f_full = fsqlite_query_values(&f_conn, "PRAGMA wal_checkpoint(FULL);").await;
        let c_trip = checkpoint_triplet(&c_full, "csqlite full");
        let f_trip = checkpoint_triplet(&f_full, "fsqlite full");
        eprintln!(
            "bead_id={BEAD_ID} run_id={run_id} seed={SEED} phase=checkpoint_full c={c_trip:?} f={f_trip:?}"
        );
        assert_eq!(c_trip.0, 0, "csqlite full busy");
        assert_eq!(f_trip.0, 0, "fsqlite full busy");

        // Add more data for RESTART checkpoint.
        for i in 7..=9 {
            let sql = format!("INSERT INTO t_M VALUES ({i}, 'row_{i}');");
            c_conn.execute(&sql, []).expect("csqlite insert");
            f_conn.execute(&sql).await.expect("fsqlite insert");
        }

        // Test RESTART checkpoint (resets WAL after checkpoint).
        let c_restart = csqlite_query_values(&c_conn, "PRAGMA wal_checkpoint(RESTART);");
        let f_restart = fsqlite_query_values(&f_conn, "PRAGMA wal_checkpoint(RESTART);").await;
        let c_trip = checkpoint_triplet(&c_restart, "csqlite restart");
        let f_trip = checkpoint_triplet(&f_restart, "fsqlite restart");
        eprintln!(
            "bead_id={BEAD_ID} run_id={run_id} seed={SEED} phase=checkpoint_restart c={c_trip:?} f={f_trip:?}"
        );
        assert_eq!(c_trip.0, 0, "csqlite restart busy");
        assert_eq!(f_trip.0, 0, "fsqlite restart busy");

        // Add more data for TRUNCATE checkpoint.
        for i in 10..=12 {
            let sql = format!("INSERT INTO t_M VALUES ({i}, 'row_{i}');");
            c_conn.execute(&sql, []).expect("csqlite insert");
            f_conn.execute(&sql).await.expect("fsqlite insert");
        }

        // Test TRUNCATE checkpoint (truncates WAL to zero after checkpoint).
        let c_truncate = csqlite_query_values(&c_conn, "PRAGMA wal_checkpoint(TRUNCATE);");
        let f_truncate = fsqlite_query_values(&f_conn, "PRAGMA wal_checkpoint(TRUNCATE);").await;
        let c_trip = checkpoint_triplet(&c_truncate, "csqlite truncate");
        let f_trip = checkpoint_triplet(&f_truncate, "fsqlite truncate");
        eprintln!(
            "bead_id={BEAD_ID} run_id={run_id} seed={SEED} phase=checkpoint_truncate c={c_trip:?} f={f_trip:?}"
        );
        assert_eq!(c_trip.0, 0, "csqlite truncate busy");
        assert_eq!(f_trip.0, 0, "fsqlite truncate busy");

        // Verify all data is accessible after all checkpoint modes.
        let c_count = csqlite_query_values(&c_conn, "SELECT COUNT(*) FROM t_M;");
        let f_count = fsqlite_query_values(&f_conn, "SELECT COUNT(*) FROM t_M;").await;
        eprintln!(
            "bead_id={BEAD_ID} run_id={run_id} seed={SEED} phase=final_data_check c={c_count:?} f={f_count:?}"
        );
        assert_eq!(
            c_count, f_count,
            "row count mismatch after all checkpoint modes"
        );
        assert_eq!(
            f_count,
            vec![vec![SqlValue::Integer(12)]],
            "expected 12 rows after all inserts"
        );
    });
}

#[test]
fn txn_file_backed_retained_autocommit_interleaved_read_write_close_reopen_matches_rusqlite() {
    asupersync::test_utils::run_test(|| async {
        let tmp = tempdir().expect("tempdir");
        let c_path = tmp.path().join("oracle_retained_autocommit.db");
        let f_path = tmp.path().join("candidate_retained_autocommit.db");
        let f_path_string = f_path.to_string_lossy().into_owned();

        let c_conn = rusqlite::Connection::open(&c_path).expect("open csqlite db");
        let f_conn = fsqlite::Connection::open(&f_path_string)
            .await
            .expect("open fsqlite db");
        f_conn
            .execute("PRAGMA fsqlite.concurrent_mode = OFF;")
            .await
            .expect("disable concurrent mode for deterministic retained-autocommit coverage");

        let schema_sql = "CREATE TABLE msgs(id INTEGER PRIMARY KEY, val TEXT NOT NULL);";
        c_conn.execute(schema_sql, []).expect("csqlite schema");
        f_conn.execute(schema_sql).await.expect("fsqlite schema");

        for step in 1_u32..=24 {
            let rowid = i64::from(step);
            let insert_sql = format!("INSERT INTO msgs VALUES ({rowid}, 'v{rowid}');");
            c_conn.execute(&insert_sql, []).expect("csqlite insert");
            f_conn.execute(&insert_sql).await.expect("fsqlite insert");

            let point_lookup_sql = format!("SELECT id, val FROM msgs WHERE id = {rowid};");
            assert_eq!(
                csqlite_query_values(&c_conn, &point_lookup_sql),
                fsqlite_query_values(&f_conn, &point_lookup_sql).await,
                "read-after-write point lookup diverged after INSERT step {step}"
            );

            if step.is_multiple_of(6) {
                let target = rowid - 1;
                let update_sql = format!("UPDATE msgs SET val = 'u{target}' WHERE id = {target};");
                c_conn.execute(&update_sql, []).expect("csqlite update");
                f_conn.execute(&update_sql).await.expect("fsqlite update");

                let verify_update_sql = format!("SELECT id, val FROM msgs WHERE id = {target};");
                assert_eq!(
                    csqlite_query_values(&c_conn, &verify_update_sql),
                    fsqlite_query_values(&f_conn, &verify_update_sql).await,
                    "read-after-write point lookup diverged after UPDATE step {step}"
                );
            }
        }

        let delete_sql = "DELETE FROM msgs WHERE id IN (3, 7, 11, 19);";
        c_conn.execute(delete_sql, []).expect("csqlite delete");
        f_conn.execute(delete_sql).await.expect("fsqlite delete");

        let post_delete_sql = "SELECT COUNT(*), MIN(id), MAX(id) FROM msgs;";
        assert_eq!(
            csqlite_query_values(&c_conn, post_delete_sql),
            fsqlite_query_values(&f_conn, post_delete_sql).await,
            "post-delete retained autocommit state diverged before close"
        );

        let full_dump_sql = "SELECT id, val FROM msgs ORDER BY id;";
        let before_close_c = csqlite_query_values(&c_conn, full_dump_sql);
        let before_close_f = fsqlite_query_values(&f_conn, full_dump_sql).await;
        assert_eq!(
            before_close_c, before_close_f,
            "file-backed retained autocommit should match the oracle before close"
        );

        f_conn.close().await.expect("close fsqlite connection");
        drop(c_conn);

        let reopened_c = rusqlite::Connection::open(&c_path).expect("reopen csqlite db");
        let reopened_f = fsqlite::Connection::open(&f_path_string)
            .await
            .expect("reopen fsqlite db");
        assert_eq!(
            csqlite_query_values(&reopened_c, full_dump_sql),
            fsqlite_query_values(&reopened_f, full_dump_sql).await,
            "close+reopen must flush retained autocommit state identically to the oracle"
        );
    });
}

#[test]
fn txn_file_backed_retained_autocommit_schema_change_boundary_matches_rusqlite() {
    asupersync::test_utils::run_test(|| async {
        let tmp = tempdir().expect("tempdir");
        let c_path = tmp
            .path()
            .join("oracle_retained_autocommit_schema_change.db");
        let f_path = tmp
            .path()
            .join("candidate_retained_autocommit_schema_change.db");
        let f_path_string = f_path.to_string_lossy().into_owned();

        let c_conn = rusqlite::Connection::open(&c_path).expect("open csqlite db");
        let f_conn = fsqlite::Connection::open(&f_path_string)
            .await
            .expect("open fsqlite db");
        f_conn
            .execute("PRAGMA fsqlite.concurrent_mode = OFF;")
            .await
            .expect("disable concurrent mode for deterministic retained-autocommit coverage");

        let schema_sql = "CREATE TABLE msgs(id INTEGER PRIMARY KEY, val TEXT NOT NULL);";
        c_conn.execute(schema_sql, []).expect("csqlite schema");
        f_conn.execute(schema_sql).await.expect("fsqlite schema");

        c_conn
            .execute("INSERT INTO msgs VALUES (1, 'alpha');", [])
            .expect("csqlite insert");
        f_conn
            .execute("INSERT INTO msgs VALUES (1, 'alpha');")
            .await
            .expect("fsqlite insert");
        assert_eq!(
            csqlite_query_values(&c_conn, "SELECT id, val FROM msgs ORDER BY id;"),
            fsqlite_query_values(&f_conn, "SELECT id, val FROM msgs ORDER BY id;").await,
            "same-connection retained read-after-write should match before the schema boundary"
        );

        let ddl_sql = "CREATE TABLE aux_msgs(id INTEGER PRIMARY KEY, note TEXT NOT NULL);";
        c_conn
            .execute(ddl_sql, [])
            .expect("csqlite schema boundary");
        f_conn
            .execute(ddl_sql)
            .await
            .expect("fsqlite schema boundary");
        assert_eq!(
            csqlite_query_values(
                &c_conn,
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'aux_msgs';",
            ),
            fsqlite_query_values(
                &f_conn,
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'aux_msgs';",
            )
            .await,
            "schema boundary should preserve the same table catalog view as the oracle"
        );
        assert_eq!(
            csqlite_query_values(&c_conn, "SELECT id, val FROM msgs ORDER BY id;"),
            fsqlite_query_values(&f_conn, "SELECT id, val FROM msgs ORDER BY id;").await,
            "schema boundary should preserve previously retained rows"
        );

        c_conn
            .execute("INSERT INTO aux_msgs VALUES (1, 'schema');", [])
            .expect("csqlite aux insert");
        f_conn
            .execute("INSERT INTO aux_msgs VALUES (1, 'schema');")
            .await
            .expect("fsqlite aux insert");
        assert_eq!(
            csqlite_query_values(&c_conn, "SELECT id, note FROM aux_msgs ORDER BY id;"),
            fsqlite_query_values(&f_conn, "SELECT id, note FROM aux_msgs ORDER BY id;").await,
            "new table writes after the schema boundary should match the oracle"
        );

        let msgs_dump_sql = "SELECT id, val FROM msgs ORDER BY id;";
        let aux_dump_sql = "SELECT id, note FROM aux_msgs ORDER BY id;";
        f_conn.close().await.expect("close fsqlite connection");
        drop(c_conn);

        let reopened_c = rusqlite::Connection::open(&c_path).expect("reopen csqlite db");
        let reopened_f = fsqlite::Connection::open(&f_path_string)
            .await
            .expect("reopen fsqlite db");
        assert_eq!(
            csqlite_query_values(&reopened_c, msgs_dump_sql),
            fsqlite_query_values(&reopened_f, msgs_dump_sql).await,
            "schema-boundary retained-autocommit state must match after close+reopen"
        );
        assert_eq!(
            csqlite_query_values(&reopened_c, aux_dump_sql),
            fsqlite_query_values(&reopened_f, aux_dump_sql).await,
            "post-boundary writes must remain durable after close+reopen"
        );
    });
}

#[test]
fn txn_file_backed_retained_autocommit_10k_profile_matches_rusqlite_after_close_reopen() {
    asupersync::test_utils::run_test(|| async {
        const ROW_COUNT: i64 = 10_000;

        let _profile_guard = HotPathProfileGuard::new();
        let tmp = tempdir().expect("tempdir");
        let c_path = tmp.path().join("oracle_retained_autocommit_10k.db");
        let f_path = tmp.path().join("candidate_retained_autocommit_10k.db");
        let f_path_string = f_path.to_string_lossy().into_owned();

        let c_conn = rusqlite::Connection::open(&c_path).expect("open csqlite db");
        let f_conn = fsqlite::Connection::open(&f_path_string)
            .await
            .expect("open fsqlite db");
        f_conn
            .execute("PRAGMA fsqlite.concurrent_mode = OFF;")
            .await
            .expect("disable concurrent mode for deterministic retained-autocommit coverage");

        let schema_sql = "CREATE TABLE msgs(id INTEGER PRIMARY KEY, val TEXT NOT NULL);";
        c_conn.execute(schema_sql, []).expect("csqlite schema");
        f_conn.execute(schema_sql).await.expect("fsqlite schema");

        reset_hot_path_profile();
        let started = std::time::Instant::now();
        for rowid in 1_i64..=ROW_COUNT {
            let insert_sql = format!("INSERT INTO msgs VALUES ({rowid}, 'v{rowid}');");
            c_conn.execute(&insert_sql, []).expect("csqlite insert");
            f_conn.execute(&insert_sql).await.expect("fsqlite insert");
        }
        let elapsed = started.elapsed();
        let profile = hot_path_profile_snapshot();

        eprintln!(
            "bd-iuvw4 retained-autocommit-10k elapsed_ms={} reuses={} parks={} flushes={}",
            elapsed.as_millis(),
            profile.retained_autocommit_reuses,
            profile.retained_autocommit_parks,
            profile.retained_autocommit_flushes
        );

        assert!(
            profile.retained_autocommit_reuses >= 9_000,
            "10k pure-write autocommit should heavily reuse the retained txn path: {profile:?}"
        );
        assert!(
            profile.retained_autocommit_parks >= profile.retained_autocommit_reuses,
            "10k pure-write autocommit should keep re-parking the retained txn across the reused fast path: {profile:?}"
        );
        assert!(
            profile.retained_autocommit_flushes <= 64,
            "10k pure-write autocommit should batch flushes rather than flushing every statement: {profile:?}"
        );

        let full_dump_sql = "SELECT id, val FROM msgs ORDER BY id;";
        assert_eq!(
            csqlite_query_values(&c_conn, full_dump_sql),
            fsqlite_query_values(&f_conn, full_dump_sql).await,
            "10k retained autocommit state diverged before close"
        );

        f_conn.close().await.expect("close fsqlite connection");
        drop(c_conn);

        let reopened_c = rusqlite::Connection::open(&c_path).expect("reopen csqlite db");
        let reopened_f = fsqlite::Connection::open(&f_path_string)
            .await
            .expect("reopen fsqlite db");
        assert_eq!(
            csqlite_query_values(&reopened_c, full_dump_sql),
            fsqlite_query_values(&reopened_f, full_dump_sql).await,
            "10k retained autocommit close+reopen must match the oracle"
        );
    });
}

#[test]
fn txn_file_backed_retained_autocommit_crash_recovery_discards_unflushed_batch() {
    asupersync::test_utils::run_test(|| async {
        let tmp = tempdir().expect("tempdir");
        let db_path = tmp.path().join("retained_autocommit_crash_recovery.db");
        let f_path_string = db_path.to_string_lossy().into_owned();

        spawn_retained_autocommit_crash_helper(&db_path);

        let reopened_f = fsqlite::Connection::open(&f_path_string)
            .await
            .expect("reopen fsqlite db");
        let reopened_c = rusqlite::Connection::open(&db_path).expect("reopen csqlite db");
        let dump_sql = "SELECT id, val FROM msgs ORDER BY id;";
        let expected = vec![vec![
            SqlValue::Integer(1),
            SqlValue::Text("committed".to_owned()),
        ]];

        assert_eq!(
            csqlite_query_values(&reopened_c, dump_sql),
            expected,
            "stock SQLite should recover only the flushed retained-autocommit prefix"
        );
        assert_eq!(
            fsqlite_query_values(&reopened_f, dump_sql).await,
            expected,
            "FrankenSQLite should recover only the flushed retained-autocommit prefix"
        );
    });
}

#[test]
#[ignore = "invoked via subprocess by retained-autocommit crash-recovery test"]
fn retained_autocommit_crash_helper_entrypoint() {
    asupersync::test_utils::run_test(|| async {
        let Ok(db_path) = env::var(RETAINED_AUTOCOMMIT_CRASH_HELPER_DB_PATH_ENV) else {
            return;
        };

        let conn = fsqlite::Connection::open(&db_path)
            .await
            .expect("open retained-autocommit crash db");
        conn.execute("PRAGMA fsqlite.concurrent_mode = OFF;")
            .await
            .expect("disable concurrent mode");
        conn.execute("PRAGMA synchronous=FULL;")
            .await
            .expect("force WAL durability");
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
        conn.execute("CREATE TABLE msgs(id INTEGER PRIMARY KEY, val TEXT NOT NULL);")
            .await
            .expect("create table");

        conn.execute("INSERT INTO msgs VALUES (1, 'committed');")
            .await
            .expect("insert flushed retained prefix");
        conn.execute("BEGIN;")
            .await
            .expect("explicit begin should flush retained prefix");
        conn.execute("COMMIT;")
            .await
            .expect("finish explicit boundary");

        conn.execute("INSERT INTO msgs VALUES (2, 'pending_2');")
            .await
            .expect("insert first unflushed retained row");
        conn.execute("INSERT INTO msgs VALUES (3, 'pending_3');")
            .await
            .expect("insert second unflushed retained row");

        std::process::abort();
    });
}

#[test]
fn txn_mixed_connection_modes_remain_local_and_preserve_rows() {
    let tmp = tempdir().expect("tempdir");
    let oracle_path = tmp.path().join("oracle_mixed_modes.db");
    let candidate_path = tmp.path().join("candidate_mixed_modes.db");
    let candidate_path_string = candidate_path.to_string_lossy().into_owned();
    let barrier = Arc::new(Barrier::new(2));

    let oracle = rusqlite::Connection::open(&oracle_path).expect("open oracle db");
    oracle
        .execute(
            "CREATE TABLE mode_mix(id INTEGER PRIMARY KEY, source TEXT NOT NULL, val INTEGER NOT NULL);",
            [],
        )
        .expect("oracle schema");
    for id in 0_i64..8 {
        oracle
            .execute(
                "INSERT INTO mode_mix(id, source, val) VALUES (?1, 'mvcc', ?2);",
                rusqlite::params![id, id * 10],
            )
            .expect("oracle mvcc insert");
    }
    for id in 100_i64..108 {
        oracle
            .execute(
                "INSERT INTO mode_mix(id, source, val) VALUES (?1, 'serialized', ?2);",
                rusqlite::params![id, id * 10],
            )
            .expect("oracle serialized insert");
    }

    asupersync::test_utils::run_test(|| async {
        let setup = fsqlite::Connection::open(&candidate_path_string)
            .await
            .expect("open candidate db");
        setup.execute(
            "CREATE TABLE mode_mix(id INTEGER PRIMARY KEY, source TEXT NOT NULL, val INTEGER NOT NULL);",
        )
        .await
        .expect("candidate schema");
        setup.close().await.expect("close setup connection");
    });

    let mvcc_path = candidate_path_string.clone();
    let mvcc_barrier = Arc::clone(&barrier);
    let mvcc = thread::spawn(move || -> (bool, bool, usize) {
        let mut outcome: Option<(bool, bool, usize)> = None;
        asupersync::test_utils::run_test(|| async {
            let conn = fsqlite::Connection::open(&mvcc_path)
                .await
                .expect("open mvcc connection");
            configure_connection_mode(&conn, true).await;
            let mode_default = conn.is_concurrent_mode_default();
            mvcc_barrier.wait();

            let mut last_error = String::new();
            let mut error_history = Vec::new();
            for attempt in 1..=64 {
                match conn.execute("BEGIN;").await {
                    Ok(_) => {
                        let concurrent_txn = conn.is_concurrent_transaction();
                        let mut failed = false;
                        for id in 0_i64..8 {
                            let sql = format!(
                                "INSERT INTO mode_mix(id, source, val) VALUES ({id}, 'mvcc', {});",
                                id * 10
                            );
                            if let Err(err) = conn.execute(&sql).await {
                                last_error = err.to_string();
                                error_history
                                    .push(format!("attempt {attempt} insert id={id}: {err}"));
                                failed = true;
                                break;
                            }
                        }
                        if failed {
                            if let Err(err) = conn.execute("ROLLBACK;").await {
                                error_history.push(format!(
                                    "attempt {attempt} rollback after insert error: {err}"
                                ));
                            }
                        } else {
                            match conn.execute("COMMIT;").await {
                                Ok(_) => {
                                    outcome = Some((mode_default, concurrent_txn, attempt));
                                    return;
                                }
                                Err(err) => {
                                    last_error = err.to_string();
                                    error_history.push(format!("attempt {attempt} commit: {err}"));
                                    match conn.execute("ROLLBACK;").await {
                                        Ok(_) => error_history.push(format!(
                                            "attempt {attempt} {}",
                                            mode_mix_post_rollback_probe(&mvcc_path, &conn, 0)
                                                .await
                                        )),
                                        Err(rollback_err) => {
                                            error_history.push(format!(
                                                "attempt {attempt} rollback after commit error: {rollback_err}"
                                            ));
                                            last_error = format!(
                                                "{last_error}; rollback after failed commit also failed: {rollback_err}"
                                            );
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Err(err) => {
                        last_error = err.to_string();
                        error_history.push(format!("attempt {attempt} begin: {err}"));
                    }
                }

                assert!(
                    is_retryable_txn_error(&last_error),
                    "mixed-mode MVCC worker hit non-retryable error: {last_error}; history={error_history:?}"
                );
                thread::sleep(Duration::from_millis(2));
            }

            panic!(
                "mixed-mode MVCC worker exhausted retries: {last_error}; history={error_history:?}"
            );
        });
        outcome.expect("mixed-mode MVCC worker outcome")
    });

    let serialized_path = candidate_path_string.clone();
    let serialized_barrier = Arc::clone(&barrier);
    let serialized = thread::spawn(move || -> (bool, bool, usize) {
        let mut outcome: Option<(bool, bool, usize)> = None;
        asupersync::test_utils::run_test(|| async {
            let conn = fsqlite::Connection::open(&serialized_path)
                .await
                .expect("open serialized connection");
            configure_connection_mode(&conn, false).await;
            let mode_default = conn.is_concurrent_mode_default();
            serialized_barrier.wait();

            let mut last_error = String::new();
            let mut error_history = Vec::new();
            for attempt in 1..=64 {
                match conn.execute("BEGIN;").await {
                    Ok(_) => {
                        let concurrent_txn = conn.is_concurrent_transaction();
                        let mut failed = false;
                        for id in 100_i64..108 {
                            let sql = format!(
                                "INSERT INTO mode_mix(id, source, val) VALUES ({id}, 'serialized', {});",
                                id * 10
                            );
                            if let Err(err) = conn.execute(&sql).await {
                                last_error = err.to_string();
                                error_history
                                    .push(format!("attempt {attempt} insert id={id}: {err}"));
                                failed = true;
                                break;
                            }
                        }
                        if failed {
                            if let Err(err) = conn.execute("ROLLBACK;").await {
                                error_history.push(format!(
                                    "attempt {attempt} rollback after insert error: {err}"
                                ));
                            }
                        } else {
                            match conn.execute("COMMIT;").await {
                                Ok(_) => {
                                    outcome = Some((mode_default, concurrent_txn, attempt));
                                    return;
                                }
                                Err(err) => {
                                    last_error = err.to_string();
                                    error_history.push(format!("attempt {attempt} commit: {err}"));
                                    match conn.execute("ROLLBACK;").await {
                                        Ok(_) => error_history.push(format!(
                                            "attempt {attempt} {}",
                                            mode_mix_post_rollback_probe(
                                                &serialized_path,
                                                &conn,
                                                100
                                            )
                                            .await
                                        )),
                                        Err(rollback_err) => {
                                            error_history.push(format!(
                                                "attempt {attempt} rollback after commit error: {rollback_err}"
                                            ));
                                            last_error = format!(
                                                "{last_error}; rollback after failed commit also failed: {rollback_err}"
                                            );
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Err(err) => {
                        last_error = err.to_string();
                        error_history.push(format!("attempt {attempt} begin: {err}"));
                    }
                }

                assert!(
                    is_retryable_txn_error(&last_error),
                    "mixed-mode serialized worker hit non-retryable error: {last_error}; history={error_history:?}"
                );
                thread::sleep(Duration::from_millis(2));
            }

            panic!(
                "mixed-mode serialized worker exhausted retries: {last_error}; history={error_history:?}"
            );
        });
        outcome.expect("mixed-mode serialized worker outcome")
    });

    let (mvcc_default, mvcc_txn, mvcc_attempts) = mvcc.join().expect("join mvcc worker");
    let (serialized_default, serialized_txn, serialized_attempts) =
        serialized.join().expect("join serialized worker");

    let mut candidate_rows: Vec<Vec<SqlValue>> = Vec::new();
    let full_dump_sql = "SELECT id, source, val FROM mode_mix ORDER BY id;";
    let oracle_rows = csqlite_query_values(&oracle, full_dump_sql);
    asupersync::test_utils::run_test(|| async {
        let candidate = fsqlite::Connection::open(&candidate_path_string)
            .await
            .expect("reopen candidate db");
        candidate_rows = fsqlite_query_values(&candidate, full_dump_sql).await;
    });
    assert_eq!(
        oracle_rows, candidate_rows,
        "mixed connection modes should preserve the same logical rows as the oracle"
    );

    emit_scenario_completeness_log(
        "txn_mixed_connection_modes_remain_local_and_preserve_rows",
        "result",
        json!({
            "mvcc_mode_default": mvcc_default,
            "mvcc_transaction_concurrent": mvcc_txn,
            "mvcc_attempts": mvcc_attempts,
            "serialized_mode_default": serialized_default,
            "serialized_transaction_concurrent": serialized_txn,
            "serialized_attempts": serialized_attempts,
            "row_count": candidate_rows.len()
        }),
    );

    assert!(
        mvcc_default,
        "MVCC worker must keep concurrent mode enabled"
    );
    assert!(
        mvcc_txn,
        "BEGIN on MVCC connection must promote to concurrent txn"
    );
    assert!(
        !serialized_default,
        "serialized worker must keep concurrent mode opt-out local to its connection"
    );
    assert!(
        !serialized_txn,
        "BEGIN on serialized connection must stay single-writer"
    );
}

#[test]
fn txn_concurrent_schema_and_dml_preserve_index_and_rows() {
    let tmp = tempdir().expect("tempdir");
    let oracle_path = tmp.path().join("oracle_schema_dml.db");
    let candidate_path = tmp.path().join("candidate_schema_dml.db");
    let candidate_path_string = candidate_path.to_string_lossy().into_owned();
    let barrier = Arc::new(Barrier::new(2));

    let oracle = rusqlite::Connection::open(&oracle_path).expect("open oracle db");
    oracle
        .execute(
            "CREATE TABLE schema_mix(id INTEGER PRIMARY KEY, category TEXT NOT NULL, payload TEXT NOT NULL);",
            [],
        )
        .expect("oracle schema");
    for id in 0_i64..512 {
        oracle
            .execute(
                "INSERT INTO schema_mix(id, category, payload) VALUES (?1, ?2, ?3);",
                rusqlite::params![
                    id,
                    if id % 2 == 0 { "seed_even" } else { "seed_odd" },
                    format!("seed-payload-{id:04}-{}", "x".repeat(48))
                ],
            )
            .expect("oracle seed insert");
    }
    oracle
        .execute(
            "CREATE INDEX IF NOT EXISTS idx_schema_mix_category ON schema_mix(category);",
            [],
        )
        .expect("oracle create index");
    for id in 1_000_i64..1_064 {
        oracle
            .execute(
                "INSERT INTO schema_mix(id, category, payload) VALUES (?1, 'writer', ?2);",
                rusqlite::params![id, format!("writer-payload-{id:04}")],
            )
            .expect("oracle writer insert");
    }

    asupersync::test_utils::run_test(|| async {
        let setup = fsqlite::Connection::open(&candidate_path_string)
            .await
            .expect("open candidate db");
        setup.execute(
            "CREATE TABLE schema_mix(id INTEGER PRIMARY KEY, category TEXT NOT NULL, payload TEXT NOT NULL);",
        )
        .await
        .expect("candidate schema");
        for id in 0_i64..512 {
            let payload = format!("seed-payload-{id:04}-{}", "x".repeat(48));
            let category = if id % 2 == 0 { "seed_even" } else { "seed_odd" };
            let sql = format!(
                "INSERT INTO schema_mix(id, category, payload) VALUES ({id}, '{category}', '{payload}');"
            );
            setup.execute(&sql).await.expect("candidate seed insert");
        }
        setup.close().await.expect("close setup connection");
    });

    let schema_path = candidate_path_string.clone();
    let schema_barrier = Arc::clone(&barrier);
    let schema_thread = thread::spawn(move || -> usize {
        let mut outcome: Option<usize> = None;
        asupersync::test_utils::run_test(|| async {
            let conn = fsqlite::Connection::open(&schema_path)
                .await
                .expect("open schema connection");
            configure_connection_mode(&conn, true).await;
            schema_barrier.wait();

            let mut last_error = String::new();
            for attempt in 1..=64 {
                match conn
                    .execute(
                        "CREATE INDEX IF NOT EXISTS idx_schema_mix_category ON schema_mix(category);",
                    )
                    .await
                {
                    Ok(_) => {
                        outcome = Some(attempt);
                        return;
                    }
                    Err(err) => {
                        last_error = err.to_string();
                        assert!(
                            is_retryable_txn_error(&last_error),
                            "schema worker hit non-retryable error: {last_error}"
                        );
                        thread::sleep(Duration::from_millis(2));
                    }
                }
            }

            panic!("schema worker exhausted retries: {last_error}");
        });
        outcome.expect("schema worker outcome")
    });

    let writer_path = candidate_path_string.clone();
    let writer_barrier = Arc::clone(&barrier);
    let writer_thread = thread::spawn(move || -> usize {
        let mut max_attempts = 0_usize;
        asupersync::test_utils::run_test(|| async {
            let conn = fsqlite::Connection::open(&writer_path)
                .await
                .expect("open writer connection");
            configure_connection_mode(&conn, true).await;
            writer_barrier.wait();

            for id in 1_000_i64..1_064 {
                let sql = format!(
                    "INSERT INTO schema_mix(id, category, payload) VALUES ({id}, 'writer', 'writer-payload-{id:04}');"
                );
                let mut last_error = String::new();
                let mut inserted = false;
                for attempt in 1..=64 {
                    max_attempts = max_attempts.max(attempt);
                    match conn.execute(&sql).await {
                        Ok(_) => {
                            inserted = true;
                            break;
                        }
                        Err(err) => {
                            last_error = err.to_string();
                            assert!(
                                is_retryable_txn_error(&last_error),
                                "writer hit non-retryable error: {last_error}"
                            );
                            thread::sleep(Duration::from_millis(1));
                        }
                    }
                }
                assert!(
                    inserted,
                    "writer exhausted retries for row {id}: {last_error}"
                );
            }
        });
        max_attempts
    });

    let schema_attempts = schema_thread.join().expect("join schema thread");
    let writer_attempts = writer_thread.join().expect("join writer thread");

    asupersync::test_utils::run_test(|| async {
        let candidate = fsqlite::Connection::open(&candidate_path_string)
            .await
            .expect("reopen candidate db");
        let state_sql = "SELECT COUNT(*), MIN(id), MAX(id) FROM schema_mix;";
        let index_sql = "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='idx_schema_mix_category';";
        let writer_sql = "SELECT COUNT(*) FROM schema_mix WHERE category='writer';";
        assert_eq!(
            csqlite_query_values(&oracle, state_sql),
            fsqlite_query_values(&candidate, state_sql).await,
            "concurrent schema + DML row-state diverged from oracle"
        );
        assert_eq!(
            csqlite_query_values(&oracle, index_sql),
            fsqlite_query_values(&candidate, index_sql).await,
            "concurrent schema + DML index state diverged from oracle"
        );
        assert_eq!(
            csqlite_query_values(&oracle, writer_sql),
            fsqlite_query_values(&candidate, writer_sql).await,
            "concurrent schema + DML inserted writer rows diverged from oracle"
        );

        let integrity: String = rusqlite::Connection::open(&candidate_path)
            .expect("open candidate with rusqlite")
            .query_row("PRAGMA integrity_check;", [], |row| row.get(0))
            .expect("integrity_check");
        assert_eq!(integrity, "ok", "candidate db must remain integrity-clean");

        let state_rows = fsqlite_query_values(&candidate, state_sql).await;
        let index_rows = fsqlite_query_values(&candidate, index_sql).await;
        let writer_rows = fsqlite_query_values(&candidate, writer_sql).await;
        emit_scenario_completeness_log(
            "txn_concurrent_schema_and_dml_preserve_index_and_rows",
            "result",
            json!({
                "schema_attempts": schema_attempts,
                "writer_max_attempts": writer_attempts,
                "integrity_check": integrity,
                "state": sql_rows_to_json(&state_rows),
                "index_state": sql_rows_to_json(&index_rows),
                "writer_rows": sql_rows_to_json(&writer_rows)
            }),
        );
    });
}

#[test]
fn txn_attach_cross_db_insert_select_is_statement_atomic() {
    asupersync::test_utils::run_test(|| async {
        let temp = tempdir().expect("tempdir");
        let oracle_db1 = temp.path().join("oracle_db1.db");
        let oracle_db2 = temp.path().join("oracle_db2.db");
        let candidate_db1 = temp.path().join("candidate_db1.db");
        let candidate_db2 = temp.path().join("candidate_db2.db");

        let oracle_db1_sql = oracle_db1.to_string_lossy().replace('\'', "''");
        let oracle_db2_sql = oracle_db2.to_string_lossy().replace('\'', "''");
        let candidate_db1_sql = candidate_db1.to_string_lossy().replace('\'', "''");
        let candidate_db2_sql = candidate_db2.to_string_lossy().replace('\'', "''");

        let oracle = rusqlite::Connection::open_in_memory().expect("open oracle main");
        oracle
            .execute(&format!("ATTACH DATABASE '{oracle_db1_sql}' AS db1;"), [])
            .expect("oracle attach db1");
        oracle
            .execute(&format!("ATTACH DATABASE '{oracle_db2_sql}' AS db2;"), [])
            .expect("oracle attach db2");
        oracle
            .execute(
                "CREATE TABLE db1.dest (id INTEGER PRIMARY KEY, value TEXT UNIQUE);",
                [],
            )
            .expect("oracle create dest");
        oracle
            .execute("CREATE TABLE db2.src (id INTEGER, value TEXT);", [])
            .expect("oracle create src");
        oracle
            .execute("INSERT INTO db2.src VALUES (1, 'dup');", [])
            .expect("oracle insert src row 1");
        oracle
            .execute("INSERT INTO db2.src VALUES (2, 'dup');", [])
            .expect("oracle insert src row 2");
        let oracle_err = oracle
            .execute(
                "INSERT INTO db1.dest SELECT id, value FROM db2.src ORDER BY id;",
                [],
            )
            .expect_err("oracle duplicate insert should fail atomically");
        assert!(
            matches!(
                oracle_err,
                rusqlite::Error::SqliteFailure(_, _) | rusqlite::Error::StatementChangedRows(_)
            ) || oracle_err
                .to_string()
                .to_ascii_lowercase()
                .contains("constraint"),
            "unexpected oracle error: {oracle_err:?}"
        );

        let candidate = fsqlite::Connection::open(":memory:")
            .await
            .expect("open candidate main");
        candidate
            .execute(&format!("ATTACH DATABASE '{candidate_db1_sql}' AS db1;"))
            .await
            .expect("candidate attach db1");
        candidate
            .execute(&format!("ATTACH DATABASE '{candidate_db2_sql}' AS db2;"))
            .await
            .expect("candidate attach db2");
        candidate
            .execute("CREATE TABLE db1.dest (id INTEGER PRIMARY KEY, value TEXT UNIQUE);")
            .await
            .expect("candidate create dest");
        candidate
            .execute("CREATE TABLE db2.src (id INTEGER, value TEXT);")
            .await
            .expect("candidate create src");
        candidate
            .execute("INSERT INTO db2.src VALUES (1, 'dup');")
            .await
            .expect("candidate insert src row 1");
        candidate
            .execute("INSERT INTO db2.src VALUES (2, 'dup');")
            .await
            .expect("candidate insert src row 2");
        let candidate_err = candidate
            .execute("INSERT INTO db1.dest SELECT id, value FROM db2.src ORDER BY id;")
            .await
            .expect_err("candidate duplicate insert should fail atomically");
        assert!(
            candidate_err
                .to_string()
                .to_ascii_lowercase()
                .contains("unique"),
            "unexpected candidate error: {candidate_err:?}"
        );

        let expected_dest_rows = csqlite_query_values(
            &rusqlite::Connection::open(&oracle_db1).expect("open oracle db1"),
            "SELECT id, value FROM dest ORDER BY id;",
        );
        let candidate_dest_rows = fsqlite_query_values(
            &fsqlite::Connection::open(
                candidate_db1
                    .to_str()
                    .expect("candidate db1 path should be utf-8"),
            )
            .await
            .expect("open candidate db1"),
            "SELECT id, value FROM dest ORDER BY id;",
        )
        .await;
        assert_eq!(
            expected_dest_rows, candidate_dest_rows,
            "cross-db INSERT ... SELECT should leave the destination identical after duplicate failure"
        );
        assert!(
            candidate_dest_rows.is_empty(),
            "destination should stay empty after the failed cross-db statement"
        );

        let expected_src_rows = csqlite_query_values(
            &rusqlite::Connection::open(&oracle_db2).expect("open oracle db2"),
            "SELECT id, value FROM src ORDER BY id;",
        );
        let candidate_src_rows = fsqlite_query_values(
            &fsqlite::Connection::open(
                candidate_db2
                    .to_str()
                    .expect("candidate db2 path should be utf-8"),
            )
            .await
            .expect("open candidate db2"),
            "SELECT id, value FROM src ORDER BY id;",
        )
        .await;
        assert_eq!(
            expected_src_rows, candidate_src_rows,
            "source database contents should remain unchanged after the failed cross-db statement"
        );
    });
}
