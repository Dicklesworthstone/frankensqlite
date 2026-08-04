//! bd-k8oxp — Oracle-parity e2e: aggregate-in-GROUP-BY rejection vs rusqlite.
//!
//! agg_window_misuse_oracle covers aggregate-in-WHERE, nested aggregate, and
//! window-in-{WHERE,HAVING,GROUP BY}. SQLite also rejects a plain aggregate inside
//! `GROUP BY` itself ("aggregate functions are not allowed in the GROUP BY
//! clause"). This pins that frank rejects it too, while still accepting a regular
//! GROUP BY plus aggregates in the SELECT list.
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

async fn engines() -> (Connection, rusqlite::Connection) {
    let f = Connection::open(":memory:").await.expect("open frank");
    let r = rusqlite::Connection::open_in_memory().expect("open rusqlite");
    for s in [
        "CREATE TABLE t (a INTEGER, b INTEGER)",
        "INSERT INTO t VALUES (1,10),(1,20),(2,30)",
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
fn group_by_aggregate_rejected() {
    asupersync::test_utils::run_test(|| async {
        // Aggregate inside GROUP BY itself -> SQLite error on both engines.
        check(
            &[
                "SELECT a FROM t GROUP BY count(*)",
                "SELECT a FROM t GROUP BY sum(a)",
                "SELECT a FROM t GROUP BY a + count(*)", // aggregate buried in expression
            ],
            "group_by_aggregate_rejected",
        )
        .await;
    });
}

#[test]
fn ordinary_group_by_ok() {
    asupersync::test_utils::run_test(|| async {
        // The well-formed contrast: GROUP BY a column, aggregate in the SELECT list.
        check(
            &[
                "SELECT a, count(*), sum(b) FROM t GROUP BY a ORDER BY a", // (1,2,30),(2,1,30)
            ],
            "ordinary_group_by_ok",
        )
        .await;
    });
}
