//! bd-rox2z — Oracle-parity e2e: changes() / total_changes() vs rusqlite.
//!
//! `changes()` returns the number of rows the most recent INSERT/UPDATE/DELETE
//! modified; `total_changes()` accumulates them over the connection's life.
//! SQLite counts rows matched by an UPDATE/DELETE (even a value-preserving
//! UPDATE), a multi-row INSERT counts every row, an UPDATE/DELETE matching no
//! rows yields 0, and INSERT OR REPLACE counts the inserted row. A trailing
//! SELECT does not reset the counter, so each scenario runs its DML last and
//! reads the counter as a query. Compared against rusqlite.
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
fn changes_after_multirow_insert() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)",
                "INSERT INTO t VALUES (1,10),(2,20),(3,30)",
            ],
            &[
                "SELECT changes()",       // 3
                "SELECT total_changes()", // 3
            ],
            "changes_after_multirow_insert",
        )
        .await;
    });
}

#[test]
fn changes_after_update_n() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)",
                "INSERT INTO t VALUES (1,10),(2,20),(3,30),(4,40)",
                "UPDATE t SET v = 0 WHERE v >= 30", // matches id3,id4
            ],
            &["SELECT changes()"], // 2
            "changes_after_update_n",
        )
        .await;
    });
}

#[test]
fn changes_after_delete_n() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)",
                "INSERT INTO t VALUES (1,10),(2,20),(3,30),(4,40)",
                "DELETE FROM t WHERE v < 25", // matches id1,id2
            ],
            &["SELECT changes()", "SELECT count(*) FROM t"], // 2, 2
            "changes_after_delete_n",
        )
        .await;
    });
}

#[test]
fn changes_after_delete_all() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)",
                "INSERT INTO t VALUES (1,10),(2,20),(3,30)",
                "DELETE FROM t",
            ],
            &["SELECT changes()"], // 3
            "changes_after_delete_all",
        )
        .await;
    });
}

#[test]
fn changes_zero_when_no_match() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)",
                "INSERT INTO t VALUES (1,10),(2,20)",
                "UPDATE t SET v = 99 WHERE v = 999", // matches nothing
            ],
            &["SELECT changes()"], // 0
            "changes_zero_when_no_match",
        )
        .await;
    });
}

#[test]
fn total_changes_accumulates() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)",
                "INSERT INTO t VALUES (1,10),(2,20),(3,30)", // +3
                "UPDATE t SET v = v WHERE 1",                // +3 (all rows matched)
                "DELETE FROM t WHERE id = 1",                // +1
            ],
            &[
                "SELECT changes()",       // 1 (last stmt = delete)
                "SELECT total_changes()", // 7
            ],
            "total_changes_accumulates",
        )
        .await;
    });
}

#[test]
fn changes_after_insert_or_replace() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)",
                "INSERT INTO t VALUES (1,10),(2,20)",
                "INSERT OR REPLACE INTO t VALUES (1, 99)", // replaces id1
            ],
            &["SELECT changes()", "SELECT v FROM t WHERE id = 1"], // 1, 99
            "changes_after_insert_or_replace",
        )
        .await;
    });
}
