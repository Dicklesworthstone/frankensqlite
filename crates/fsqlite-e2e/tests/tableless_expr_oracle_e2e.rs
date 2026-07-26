//! bd-9tzr8 — Oracle-parity e2e: constant / table-less SELECT expressions.
//!
//! A `SELECT <expr>` with no FROM clause flows through connection.rs's `emit_expr`
//! path, whose catch-all (connection.rs:76299) has already been found missing
//! arms for some expression variants (JsonAccess -> bd-m87j8, constant RowValue
//! -> bd-l2si0). This maps that surface across the common forms: scalar
//! subqueries (incl. nested and in arithmetic), EXISTS / NOT EXISTS, IN against a
//! subquery and a list, LIKE / GLOB, COLLATE, BETWEEN, CASE, CAST, and IS/IS NOT.
//! Each is evaluated as a constant SELECT and compared against rusqlite; the
//! subquery-bearing forms are isolated so a divergence is clean.

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
fn tableless_like_glob_collate_between() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "SELECT 'abc' LIKE 'a%'",          // 1
                "SELECT 'abc' LIKE 'A%'",          // 1 (LIKE is ASCII case-insensitive)
                "SELECT 'abc' GLOB 'a*'",          // 1
                "SELECT 'abc' GLOB 'A*'",          // 0 (GLOB is case-sensitive)
                "SELECT 'a' = 'A' COLLATE NOCASE", // 1
                "SELECT 5 BETWEEN 1 AND 10",       // 1
                "SELECT 5 NOT BETWEEN 1 AND 10",   // 0
            ],
            "tableless_like_glob_collate_between",
        )
        .await;
    });
}

#[test]
fn tableless_case_cast_is() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "SELECT CASE WHEN 1 THEN 'y' ELSE 'n' END", // 'y'
                "SELECT CAST('5' AS INTEGER) + 1",          // 6
                "SELECT 1 IS NOT NULL, NULL IS NULL",       // 1, 1
                "SELECT 1 IS 1, 1 IS NOT 2",                // 1, 1
            ],
            "tableless_case_cast_is",
        )
        .await;
    });
}

/// Scalar subqueries with no outer FROM clause (the constant path).
#[test]
fn tableless_scalar_subquery() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "SELECT (SELECT 42)",                                                     // 42
                "SELECT (SELECT (SELECT 7))",     // 7 (nested)
                "SELECT (SELECT 3) + (SELECT 4)", // 7 (arithmetic over subqueries)
                "SELECT (SELECT count(*) FROM (SELECT 1 UNION SELECT 2 UNION SELECT 3))", // 3
            ],
            "tableless_scalar_subquery",
        )
        .await;
    });
}

/// EXISTS / NOT EXISTS as a constant SELECT.
#[test]
fn tableless_exists() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "SELECT EXISTS(SELECT 1)",             // 1
                "SELECT EXISTS(SELECT 1 WHERE 0)",     // 0
                "SELECT NOT EXISTS(SELECT 1 WHERE 0)", // 1
            ],
            "tableless_exists",
        )
        .await;
    });
}

/// IN against a subquery as a constant SELECT.
#[test]
fn tableless_in_subquery() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "SELECT 1 IN (SELECT 1 UNION SELECT 2)",     // 1
                "SELECT 5 IN (SELECT 1 UNION SELECT 2)",     // 0
                "SELECT 5 NOT IN (SELECT 1 UNION SELECT 2)", // 1
            ],
            "tableless_in_subquery",
        )
        .await;
    });
}
