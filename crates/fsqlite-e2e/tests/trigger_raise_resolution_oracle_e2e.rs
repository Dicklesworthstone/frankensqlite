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

fn frank_rows(conn: &Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let rows = conn.query(sql).map_err(|e| e.to_string())?;
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
fn scenario(stmts: &[&str], queries: &[&str], label: &str) {
    let f = Connection::open(":memory:").expect("open frank");
    let r = rusqlite::Connection::open_in_memory().expect("open rusqlite");
    for s in stmts {
        let fe = f.execute(s);
        let re = r.execute_batch(s);
        match (&fe, &re) {
            (Ok(_), Ok(())) | (Err(_), Err(_)) => {}
            (Ok(_), Err(e)) => panic!("{label}: `{s}`\n  frank: OK\n  csql:  ERROR({e})"),
            (Err(e), Ok(())) => panic!("{label}: `{s}`\n  frank: ERROR({e})\n  csql:  OK"),
        }
    }
    let mut mismatches = Vec::new();
    for q in queries {
        match (frank_rows(&f, q), sqlite_rows(&r, q)) {
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
    // ABORT on row 3 -> the whole multi-row INSERT is undone, table stays empty.
    scenario(
        &[
            "CREATE TABLE t (x INTEGER)",
            "CREATE TRIGGER tr BEFORE INSERT ON t WHEN NEW.x = 3 \
             BEGIN SELECT RAISE(ABORT, 'no 3'); END",
            "INSERT INTO t VALUES (1),(2),(3),(4)", // errors on both
        ],
        &["SELECT x FROM t ORDER BY x"], // [] (ABORT undid rows 1,2)
        "raise_abort_rolls_back_statement",
    );
}

#[test]
fn raise_fail_keeps_prior_rows() {
    // FAIL on row 3 -> rows inserted before it (1,2) are KEPT; 3,4 are not.
    scenario(
        &[
            "CREATE TABLE t (x INTEGER)",
            "CREATE TRIGGER tr BEFORE INSERT ON t WHEN NEW.x = 3 \
             BEGIN SELECT RAISE(FAIL, 'fail 3'); END",
            "INSERT INTO t VALUES (1),(2),(3),(4)", // errors on both
        ],
        &["SELECT x FROM t ORDER BY x"], // [1,2] (FAIL kept prior rows)
        "raise_fail_keeps_prior_rows",
    );
}

#[test]
fn raise_rollback_undoes_whole_transaction() {
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
        &["SELECT x FROM t ORDER BY x"], // [] (whole txn rolled back, even the 10)
        "raise_rollback_undoes_whole_transaction",
    );
}

#[test]
fn raise_ignore_skips_row_no_error() {
    // IGNORE silently drops row 3 and continues -> 1,2,4 inserted, no error.
    scenario(
        &[
            "CREATE TABLE t (x INTEGER)",
            "CREATE TRIGGER tr BEFORE INSERT ON t WHEN NEW.x = 3 \
             BEGIN SELECT RAISE(IGNORE); END",
            "INSERT INTO t VALUES (1),(2),(3),(4)", // succeeds on both (no error)
        ],
        &["SELECT x FROM t ORDER BY x"], // [1,2,4] (3 ignored)
        "raise_ignore_skips_row_no_error",
    );
}
