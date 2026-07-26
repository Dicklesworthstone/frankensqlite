//! bd-kq9go — Oracle-parity e2e: iif() + scalar min/max selection functions.
//!
//! `iif(cond, a, b)` (SQLite 3.32+) is `CASE WHEN cond THEN a ELSE b END` — a
//! non-true (false OR NULL) condition selects `b`. The scalar `min(...)` /
//! `max(...)` (2+ arguments) have a notorious gotcha: unlike the aggregate
//! min/max (which ignore NULL), the SCALAR forms return NULL if ANY argument is
//! NULL. These verify both against rusqlite, plus a coalesce contrast to make
//! the NULL behaviours explicit.
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

async fn assert_scalar(queries: &[&str], label: &str) {
    let f = Connection::open(":memory:").await.expect("open frank");
    let r = rusqlite::Connection::open_in_memory().expect("open rusqlite");
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
fn iif_basic_and_null_condition() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "SELECT iif(1, 'yes', 'no')",    // 'yes'
                "SELECT iif(0, 'yes', 'no')",    // 'no'
                "SELECT iif(NULL, 'yes', 'no')", // 'no' (NULL is not true)
                "SELECT iif(5 > 3, 10, 20)",     // 10
                "SELECT iif(5 < 3, 10, 20)",     // 20
                "SELECT typeof(iif(1, 1, 'x'))", // integer (chosen branch's type)
                "SELECT typeof(iif(0, 1, 'x'))", // text
            ],
            "iif_basic_and_null_condition",
        )
        .await;
    });
}

#[test]
fn scalar_min_max_multi_arg() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "SELECT min(3, 1, 2), max(3, 1, 2)",           // 1, 3
                "SELECT min('b', 'a', 'c'), max('b','a','c')", // 'a', 'c'
                "SELECT min(1, 2.5), max(1, 2.5)",             // 1, 2.5
                "SELECT max(-5, -1, -10)",                     // -1
            ],
            "scalar_min_max_multi_arg",
        )
        .await;
    });
}

#[test]
fn scalar_min_max_null_returns_null() {
    // The scalar (not aggregate) min/max return NULL if ANY argument is NULL.
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "SELECT max(1, NULL), min(1, NULL)", // NULL, NULL
                "SELECT max(NULL, NULL)",            // NULL
                "SELECT max(5, NULL, 3)",            // NULL
                "SELECT min(NULL, 'a', 'b')",        // NULL
                // Contrast: coalesce skips NULL and returns the first non-NULL.
                "SELECT coalesce(NULL, 5, NULL)", // 5
            ],
            "scalar_min_max_null_returns_null",
        )
        .await;
    });
}
