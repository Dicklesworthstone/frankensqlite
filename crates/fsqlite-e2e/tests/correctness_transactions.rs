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

use fsqlite_e2e::comparison::{ComparisonRunner, SqlBackend, SqlValue};

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
