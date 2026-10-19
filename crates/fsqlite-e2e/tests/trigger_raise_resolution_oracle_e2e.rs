//! bd-zk8sp — Oracle-parity e2e: RAISE() trigger resolution forms vs rusqlite.
//!
//! A trigger body's `SELECT RAISE(...)` can resolve four ways, and the
//! differences between them are subtle — exactly where a clean-room engine tends
//! to over-simplify:
//!   * `RAISE(ABORT, msg)` — the current statement fails AND all of its changes
//!     (rows inserted before the failing one) are rolled back; the surrounding
//!     transaction survives.
//!   * `RAISE(FAIL, msg)`  — the current statement fails but rows already changed
//!     earlier in the same statement are KEPT (no statement-level rollback).
//!   * `RAISE(ROLLBACK, msg)` — the entire transaction is rolled back, including
//!     changes made before the failing statement.
//!   * `RAISE(IGNORE)` — the current row (and the rest of this trigger program)
//!     is silently abandoned with NO error; the statement keeps going.
//! Each uses a BEFORE INSERT trigger that fires on one row of a multi-row insert,
//! then inspects the surviving table state. rusqlite is the oracle.

use fsqlite::Connection;
use fsqlite_types::SqliteValue;

fn render_frank(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!(
            "X'{}'",
            b.iter().map(|x| format!("{x:02X}")).collect::<String>()
        ),
    }
}

async fn frank_rows(conn: &Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let rows = conn.query(sql).await.map_err(|e| e.to_string())?;
    Ok(rows
        .iter()
        .map(|row| row.values().iter().map(render_frank).collect())
        .collect())
}

fn sqlite_rows(conn: &rusqlite::Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
    let n = stmt.column_count();
    stmt.query_map([], |row| {
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            let v: rusqlite::types::Value = row.get_unwrap(i);
            out.push(match v {
                rusqlite::types::Value::Null => "NULL".to_owned(),
                rusqlite::types::Value::Integer(x) => x.to_string(),
                rusqlite::types::Value::Real(f) => format!("{f}"),
                rusqlite::types::Value::Text(s) => format!("'{s}'"),
                rusqlite::types::Value::Blob(b) => format!(
                    "X'{}'",
                    b.iter().map(|x| format!("{x:02X}")).collect::<String>()
                ),
            });
        }
        Ok(out)
    })
    .map_err(|e| e.to_string())?
    .collect::<Result<Vec<_>, _>>()
    .map_err(|e| e.to_string())
}

/// Run setup statements asserting per-statement success/failure agreement, then
/// compare the result of each query. A statement that errors on BOTH engines
/// (e.g. the RAISE-triggering INSERT) counts as agreement — the point of the test
/// is the surviving table state, captured by the queries.
async fn scenario(stmts: &[&str], queries: &[&str], label: &str) {
    let f = Connection::open(":memory:").await.expect("open frank");
    let r = rusqlite::Connection::open_in_memory().expect("open rusqlite");
    for s in stmts {
        let fe = f.execute(s).await;
        let re = r.execute_batch(s);
        match (&fe, &re) {
            (Ok(_), Ok(())) | (Err(_), Err(_)) => {}
            (Ok(_), Err(e)) => panic!("{label}: `{s}`\n  frank: OK\n  csql:  ERROR({e})"),
            (Err(e), Ok(())) => panic!("{label}: `{s}`\n  frank: ERROR({e})\n  csql:  OK"),
        }
    }
    let mut mismatches = Vec::new();
    for q in queries {
        match (frank_rows(&f, q).await, sqlite_rows(&r, q)) {
            (Ok(a), Ok(b)) if a == b => {}
            (Ok(a), Ok(b)) => {
                mismatches.push(format!("MISMATCH: {q}\n  frank: {a:?}\n  csql:  {b:?}"))
            }
            (Err(e), Ok(b)) => mismatches.push(format!(
                "FRANK_ERR: {q}\n  frank: ERROR({e})\n  csql:  {b:?}"
            )),
            (Ok(a), Err(e)) => {
                mismatches.push(format!("CSQL_ERR: {q}\n  frank: {a:?}\n  csql: ERROR({e})"))
            }
            (Err(_), Err(_)) => {}
        }
    }
    assert!(
        mismatches.is_empty(),
        "{label}: {} mismatch(es)\n{}",
        mismatches.len(),
        mismatches.join("\n")
    );
}

#[test]
fn raise_abort_rolls_back_statement() {
    asupersync::test_utils::run_test(|| async {
        // ABORT on row 3 -> the whole multi-row INSERT is undone, table stays empty.
        scenario(
            &[
                "CREATE TABLE t (x INTEGER)",
                "CREATE TRIGGER tr BEFORE INSERT ON t WHEN NEW.x = 3 \
                 BEGIN SELECT RAISE(ABORT, 'no 3'); END",
                "INSERT INTO t VALUES (1),(2),(3),(4)", // errors on both
            ],
            &[
                "SELECT x FROM t ORDER BY x",
                "SELECT changes(), total_changes()",
            ], // [] (ABORT undid rows 1,2)
            "raise_abort_rolls_back_statement",
        )
        .await;
    });
}

#[test]
fn raise_fail_keeps_prior_rows() {
    asupersync::test_utils::run_test(|| async {
        // FAIL on row 3 -> rows inserted before it (1,2) are KEPT; 3,4 are not.
        scenario(
            &[
                "CREATE TABLE t (x INTEGER)",
                "CREATE TRIGGER tr BEFORE INSERT ON t WHEN NEW.x = 3 \
                 BEGIN SELECT RAISE(FAIL, 'fail 3'); END",
                "INSERT INTO t VALUES (1),(2),(3),(4)", // errors on both
            ],
            &[
                "SELECT x FROM t ORDER BY x",
                "SELECT changes(), total_changes()",
            ], // [1,2] (FAIL kept prior rows)
            "raise_fail_keeps_prior_rows",
        )
        .await;
    });
}

#[test]
fn raise_rollback_undoes_whole_transaction() {
    asupersync::test_utils::run_test(|| async {
        // ROLLBACK undoes the entire transaction, including the (10) row inserted
        // before the failing statement.
        scenario(
            &[
                "CREATE TABLE t (x INTEGER)",
                "CREATE TRIGGER tr BEFORE INSERT ON t WHEN NEW.x = 99 \
                 BEGIN SELECT RAISE(ROLLBACK, 'rb'); END",
                "BEGIN",
                "INSERT INTO t VALUES (10)", // a prior change in the txn
                "INSERT INTO t VALUES (99)", // triggers ROLLBACK -> errors on both
            ],
            &[
                "SELECT x FROM t ORDER BY x",
                "SELECT changes(), total_changes()",
            ], // [] (whole txn rolled back, even the 10)
            "raise_rollback_undoes_whole_transaction",
        )
        .await;
    });
}

#[test]
fn raise_ignore_skips_row_no_error() {
    asupersync::test_utils::run_test(|| async {
        // IGNORE silently drops row 3 and continues -> 1,2,4 inserted, no error.
        scenario(
            &[
                "CREATE TABLE t (x INTEGER)",
                "CREATE TRIGGER tr BEFORE INSERT ON t WHEN NEW.x = 3 \
                 BEGIN SELECT RAISE(IGNORE); END",
                "INSERT INTO t VALUES (1),(2),(3),(4)", // succeeds on both (no error)
            ],
            &[
                "SELECT x FROM t ORDER BY x",
                "SELECT changes(), total_changes()",
            ], // [1,2,4] (3 ignored)
            "raise_ignore_skips_row_no_error",
        )
        .await;
    });
}

#[test]
fn gh448_raise_fail_updates_changes_and_total_changes() {
    asupersync::test_utils::run_test(|| async {
        // GH#448 (bd-sf0pl):
        // 1. When BEFORE INSERT raises RAISE(FAIL) on the very first row after a
        //    prior multi-row DML statement, changes() must reset to 0 rather than
        //    returning the previous statement's stale count.
        scenario(
            &[
                "CREATE TABLE t (x INTEGER)",
                "CREATE TRIGGER tr BEFORE INSERT ON t WHEN NEW.x = 99 \
                 BEGIN SELECT RAISE(FAIL, 'fail first row'); END",
                "INSERT INTO t VALUES (10), (20)",
                "INSERT INTO t VALUES (99)", // fails on row 1 via BEFORE INSERT RAISE(FAIL)
            ],
            &[
                "SELECT x FROM t ORDER BY x",
                "SELECT changes(), total_changes()",
            ],
            "gh448_before_insert_first_row_raise_fail_resets_changes_to_zero",
        )
        .await;

        // 2. Multi-row INSERT failing on row 3 via BEFORE INSERT RAISE(FAIL)
        //    after a prior 5-row INSERT must report changes() = 2 (not 5 or 0),
        //    and total_changes() = 7.
        scenario(
            &[
                "CREATE TABLE t (x INTEGER)",
                "CREATE TRIGGER tr BEFORE INSERT ON t WHEN NEW.x = 3 \
                 BEGIN SELECT RAISE(FAIL, 'fail 3'); END",
                "INSERT INTO t VALUES (10),(20),(30),(40),(50)",
                "INSERT INTO t VALUES (1),(2),(3),(4)",
            ],
            &[
                "SELECT x FROM t ORDER BY x",
                "SELECT changes(), total_changes()",
            ],
            "gh448_before_insert_mid_statement_raise_fail_changes",
        )
        .await;

        // 3. Multi-row INSERT with a trigger body that logs to an audit table
        //    before raising RAISE(FAIL) on row 2: both row 1 in `t` and the
        //    audit rows from row 1 and row 2 survive, and changes()/total_changes()
        //    match stock SQLite.
        scenario(
            &[
                "CREATE TABLE t (x INTEGER)",
                "CREATE TABLE audit (logged_x INTEGER)",
                "CREATE TRIGGER tr BEFORE INSERT ON t BEGIN \
                 INSERT INTO audit VALUES (NEW.x); \
                 SELECT CASE WHEN NEW.x = 2 THEN RAISE(FAIL, 'stop on 2') END; \
                 END",
                "INSERT INTO t VALUES (100),(200)",
                "INSERT INTO t VALUES (1),(2),(3)",
            ],
            &[
                "SELECT x FROM t ORDER BY x",
                "SELECT logged_x FROM audit ORDER BY rowid",
                "SELECT changes(), total_changes()",
            ],
            "gh448_before_insert_trigger_dml_then_raise_fail",
        )
        .await;

        // 4. AFTER INSERT, AFTER UPDATE, and AFTER DELETE triggers raising
        //    RAISE(FAIL) mid-statement preserve the row that fired the AFTER
        //    trigger and report matching changes()/total_changes().
        scenario(
            &[
                "CREATE TABLE t (x INTEGER)",
                "CREATE TRIGGER tr_ai AFTER INSERT ON t WHEN NEW.x = 2 \
                 BEGIN SELECT RAISE(FAIL, 'after insert fail 2'); END",
                "INSERT INTO t VALUES (1),(2),(3)",
            ],
            &[
                "SELECT x FROM t ORDER BY x",
                "SELECT changes(), total_changes()",
            ],
            "gh448_after_insert_raise_fail_changes",
        )
        .await;

        scenario(
            &[
                "CREATE TABLE t (x INTEGER)",
                "INSERT INTO t VALUES (1),(2),(3)",
                "CREATE TRIGGER tr_au AFTER UPDATE ON t WHEN NEW.x = 20 \
                 BEGIN SELECT RAISE(FAIL, 'after update fail 20'); END",
                "UPDATE t SET x = x * 10",
            ],
            &[
                "SELECT x FROM t ORDER BY x",
                "SELECT changes(), total_changes()",
            ],
            "gh448_after_update_raise_fail_changes",
        )
        .await;

        scenario(
            &[
                "CREATE TABLE t (x INTEGER)",
                "INSERT INTO t VALUES (1),(2),(3)",
                "CREATE TRIGGER tr_ad AFTER DELETE ON t WHEN OLD.x = 2 \
                 BEGIN SELECT RAISE(FAIL, 'after delete fail 2'); END",
                "DELETE FROM t",
            ],
            &[
                "SELECT x FROM t ORDER BY x",
                "SELECT changes(), total_changes()",
            ],
            "gh448_after_delete_raise_fail_changes",
        )
        .await;
    });
}
