//! bd-0e0a8 — Oracle-parity e2e: scalar subquery column arity vs rusqlite.
//!
//! A scalar subquery — `(SELECT ...)` used as a value expression — must produce
//! exactly one column. SQLite rejects a multi-column scalar subquery
//! ("sub-select returns N columns - expected 1"). It does NOT reject multiple
//! rows: a scalar subquery returning many rows quietly takes the first. This
//! pins the column-arity error and the multi-row tolerance, plus a working
//! single-column scalar subquery.

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

async fn engines() -> (Connection, rusqlite::Connection) {
    let f = Connection::open(":memory:").await.expect("open frank");
    let r = rusqlite::Connection::open_in_memory().expect("open rusqlite");
    for s in [
        "CREATE TABLE t (a INTEGER)",
        "INSERT INTO t VALUES (10),(20),(30)",
    ] {
        f.execute(s).await.unwrap();
        r.execute_batch(s).unwrap();
    }
    (f, r)
}

async fn check(queries: &[&str], label: &str) {
    let (f, r) = engines().await;
    let mut mismatches = Vec::new();
    for q in queries {
        let frank = frank_rows(&f, q).await;
        match (frank, sqlite_rows(&r, q)) {
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
fn scalar_subquery_single_column_ok() {
    asupersync::test_utils::run_test(|| async {
        check(
            &[
                "SELECT (SELECT 1)",                     // 1
                "SELECT (SELECT a FROM t WHERE a = 20)", // 20
                // multi-row is NOT an error -- SQLite takes the first
                "SELECT (SELECT a FROM t ORDER BY a)", // 10
            ],
            "scalar_subquery_single_column_ok",
        )
        .await;
    });
}

#[test]
fn scalar_subquery_multi_column_rejected() {
    // Multi-column scalar subquery -> SQLite errors. (Multi-row is fine; multi-
    // *column* is not.)
    asupersync::test_utils::run_test(|| async {
        check(
            &[
                "SELECT (SELECT 1, 2)",
                "SELECT (SELECT 1, 2, 3)",
                "SELECT (SELECT a, a*2 FROM t LIMIT 1)",
            ],
            "scalar_subquery_multi_column_rejected",
        )
        .await;
    });
}
