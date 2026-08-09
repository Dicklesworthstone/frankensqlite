//! bd-6h608 — Oracle-parity e2e: UPDATE OR <conflict> clauses vs rusqlite.
//!
//! unique_conflict_oracle covers the INSERT OR ... prefix; this covers the
//! UPDATE side, whose conflict path is separate: `UPDATE OR IGNORE` leaves a
//! row that would violate a uniqueness constraint unchanged, `UPDATE OR REPLACE`
//! deletes the existing row it collides with (on a UNIQUE column or the PK), and
//! the default `UPDATE` (= OR ABORT) / `UPDATE OR FAIL` raise on a violation.
//! Each scenario asserts per-statement agreement with rusqlite, then compares
//! the resulting rows.

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
fn update_or_ignore_skips_conflicting_row() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, u INTEGER UNIQUE)",
                "INSERT INTO t VALUES (1,10),(2,20),(3,30)",
                "UPDATE OR IGNORE t SET u = 20 WHERE id = 1", // collides with id2 -> skipped
                "UPDATE OR IGNORE t SET u = 99 WHERE id = 3", // no collision -> applied
            ],
            &["SELECT id, u FROM t ORDER BY id"], // (1,10),(2,20),(3,99)
            "update_or_ignore_skips_conflicting_row",
        )
        .await;
    });
}

#[test]
fn update_or_replace_deletes_conflict_on_unique() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, u INTEGER UNIQUE, label TEXT)",
                "INSERT INTO t VALUES (1,10,'a'),(2,20,'b'),(3,30,'c')",
                // Setting id1.u=20 collides with id2 -> REPLACE deletes id2.
                "UPDATE OR REPLACE t SET u = 20 WHERE id = 1",
            ],
            &["SELECT id, u, label FROM t ORDER BY id"], // (1,20,'a'),(3,30,'c')
            "update_or_replace_deletes_conflict_on_unique",
        )
        .await;
    });
}

#[test]
fn update_or_replace_on_primary_key() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, label TEXT)",
                "INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'c')",
                // Changing id1 -> 2 collides with existing id2 -> REPLACE removes old id2.
                "UPDATE OR REPLACE t SET id = 2 WHERE id = 1",
            ],
            &["SELECT id, label FROM t ORDER BY id"], // (2,'a'),(3,'c')
            "update_or_replace_on_primary_key",
        )
        .await;
    });
}

#[test]
fn update_default_abort_errors_on_violation() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, u INTEGER UNIQUE)",
                "INSERT INTO t VALUES (1,10),(2,20)",
                "UPDATE t SET u = 20 WHERE id = 1", // default OR ABORT -> error on both
            ],
            &["SELECT id, u FROM t ORDER BY id"], // unchanged (1,10),(2,20)
            "update_default_abort_errors_on_violation",
        )
        .await;
    });
}

#[test]
fn update_or_fail_errors_on_violation() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, u INTEGER UNIQUE)",
                "INSERT INTO t VALUES (1,10),(2,20)",
                "UPDATE OR FAIL t SET u = 20 WHERE id = 1", // conflict -> error on both
            ],
            &["SELECT id, u FROM t ORDER BY id"], // unchanged
            "update_or_fail_errors_on_violation",
        )
        .await;
    });
}

/// bd-yuj70 — REPLACE conflict resolution fires the victim's DELETE triggers
/// if and only if `PRAGMA recursive_triggers = ON` (rowid-table arm; the
/// WITHOUT ROWID arm lives in without_rowid_dml_oracle_e2e).
#[test]
fn update_or_replace_victim_delete_triggers_default_off() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, u INTEGER UNIQUE)",
                "CREATE TABLE log (ev TEXT, victim INTEGER)",
                "CREATE TRIGGER t_del AFTER DELETE ON t BEGIN \
                 INSERT INTO log VALUES ('delete', OLD.id); END",
                "INSERT INTO t VALUES (1,10),(2,20)",
                "UPDATE OR REPLACE t SET u = 20 WHERE id = 1", // id2 is the victim
            ],
            &[
                "SELECT id, u FROM t ORDER BY id",
                "SELECT ev, victim FROM log ORDER BY victim", // empty: OFF suppresses
            ],
            "update_or_replace_victim_delete_triggers_default_off",
        )
        .await;
    });
}

#[test]
fn update_or_replace_victim_delete_triggers_recursive_on() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "PRAGMA recursive_triggers = ON",
                "CREATE TABLE t (id INTEGER PRIMARY KEY, u INTEGER UNIQUE)",
                "CREATE TABLE log (ev TEXT, victim INTEGER)",
                "CREATE TRIGGER t_del AFTER DELETE ON t BEGIN \
                 INSERT INTO log VALUES ('delete', OLD.id); END",
                "INSERT INTO t VALUES (1,10),(2,20)",
                "UPDATE OR REPLACE t SET u = 20 WHERE id = 1", // id2 is the victim
            ],
            &[
                "SELECT id, u FROM t ORDER BY id",
                "SELECT ev, victim FROM log ORDER BY victim", // ('delete', 2)
            ],
            "update_or_replace_victim_delete_triggers_recursive_on",
        )
        .await;
    });
}

/// bd-yuj70 — same rule for INSERT OR REPLACE victims.
#[test]
fn insert_or_replace_victim_delete_triggers_recursive_on() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "PRAGMA recursive_triggers = ON",
                "CREATE TABLE t (id INTEGER PRIMARY KEY, u INTEGER UNIQUE)",
                "CREATE TABLE log (ev TEXT, victim INTEGER)",
                "CREATE TRIGGER t_del AFTER DELETE ON t BEGIN \
                 INSERT INTO log VALUES ('delete', OLD.id); END",
                "INSERT INTO t VALUES (1,10),(2,20)",
                "INSERT OR REPLACE INTO t VALUES (2, 99)", // id2 replaced
            ],
            &[
                "SELECT id, u FROM t ORDER BY id",
                "SELECT ev, victim FROM log ORDER BY victim", // ('delete', 2)
            ],
            "insert_or_replace_victim_delete_triggers_recursive_on",
        )
        .await;
    });
}
