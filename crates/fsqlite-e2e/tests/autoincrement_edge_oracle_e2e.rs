//! bd-hyw7c — Oracle-parity e2e: AUTOINCREMENT edge cases vs rusqlite.
//!
//! AUTOINCREMENT keeps a high-water mark in sqlite_sequence: the next auto rowid
//! is always max-ever-used + 1, never reused. Edge cases: an explicit insert of
//! a value HIGHER than the current sequence bumps it; deleting all rows does NOT
//! reset it; and once the high-water reaches the largest 64-bit integer, a
//! further auto-insert has no rowid to assign and fails (SQLITE_FULL). These
//! verify all of that against rusqlite. (autoincrement_is_monotonic in
//! rowid_oracle covers the delete-max-then-insert case.)
#![recursion_limit = "512"]

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
fn autoincrement_explicit_higher_bumps_sequence() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)",
                "INSERT INTO t(v) VALUES ('a')",        // 1
                "INSERT INTO t(id,v) VALUES (100,'b')", // explicit 100 bumps the sequence
                "INSERT INTO t(v) VALUES ('c')",        // 101 (not 2)
            ],
            &[
                "SELECT id, v FROM t ORDER BY id", // (1,a),(100,b),(101,c)
                "SELECT seq FROM sqlite_sequence WHERE name='t'", // 101
            ],
            "autoincrement_explicit_higher_bumps_sequence",
        )
        .await;
    });
}

#[test]
fn autoincrement_continues_after_delete_all() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)",
                "INSERT INTO t(v) VALUES ('a'),('b'),('c')", // 1,2,3
                "DELETE FROM t",                             // all gone, but sequence persists
                "INSERT INTO t(v) VALUES ('d')",             // 4, not 1
            ],
            &[
                "SELECT id, v FROM t ORDER BY id",                // (4,d)
                "SELECT seq FROM sqlite_sequence WHERE name='t'", // 4
            ],
            "autoincrement_continues_after_delete_all",
        )
        .await;
    });
}

#[test]
fn autoincrement_at_max_rowid_errors() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)",
                "INSERT INTO t(id,v) VALUES (9223372036854775807, 'max')", // largest i64
                "INSERT INTO t(v) VALUES ('next')", // no rowid above max -> SQLITE_FULL error on both
            ],
            &[
                "SELECT id, v FROM t ORDER BY id", // only the max row remains
            ],
            "autoincrement_at_max_rowid_errors",
        )
        .await;
    });
}
