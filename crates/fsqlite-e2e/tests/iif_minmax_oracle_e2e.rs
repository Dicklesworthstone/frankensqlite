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

// SQLite 3.48+: iif()/if() take any number of arguments from two up and are
// compiled as CASE, so only the selected branch is evaluated.
#[test]
fn iif_variadic_lazy_and_if_alias() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "SELECT iif(0, 'a', 1, 'b', 'c')",
                "SELECT iif(0, 'a', 0, 'b', 'c')",
                "SELECT iif(0, 'a', 0, 'b')",
                "SELECT iif(NULL, 1, 0.5, 2, 3)",
                "SELECT iif(0, 1, 0, 2, 0, 3, 4)",
                "SELECT if(0, 'a', 1, 'b', 'c')",
                "SELECT if(1, 'x')",
                // The unselected branches would raise "integer overflow".
                "SELECT iif(1, 'ok', abs(-9223372036854775808))",
                "SELECT iif(0, abs(-9223372036854775808), 'else-ok')",
                "SELECT iif(0, abs(-9223372036854775808), 1, 'second', abs(-9223372036854775808))",
                // Arity errors stay errors in both engines.
                "SELECT iif(1)",
                "SELECT iif()",
            ],
            "iif_variadic_lazy_and_if_alias",
        )
        .await;
    });
}

#[test]
fn iif_variadic_over_table_rows() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.expect("open frank");
        let r = rusqlite::Connection::open_in_memory().expect("open rusqlite");
        let setup = "CREATE TABLE t(a INTEGER, b TEXT); \
                     INSERT INTO t VALUES (0, 'z'), (1, 'o'), (2, 't'), (NULL, 'n'), (5, 'f');";
        f.execute_batch(setup).await.expect("frank setup");
        r.execute_batch(setup).expect("rusqlite setup");
        let mut mismatches = Vec::new();
        for q in [
            "SELECT a, iif(a > 1, 'big', a = 1, 'one', a = 0, 'zero', 'null') FROM t ORDER BY a",
            "SELECT b FROM t WHERE iif(a IS NULL, 0, a > 1, 1, b = 'z') ORDER BY b",
            "SELECT iif(count(*) > 3, 'many', count(*) > 1, 'some', 'few') FROM t",
            "SELECT iif(a = 5, b, a = 2, upper(b)) FROM t ORDER BY a",
            "SELECT sum(iif(a > 0, a, a IS NULL, 100, -1)) FROM t",
        ] {
            match (frank_rows(&f, q).await, sqlite_rows(&r, q)) {
                (Ok(a), Ok(b)) if a == b => {}
                (a, b) => mismatches.push(format!("{q}\n  frank: {a:?}\n  csql:  {b:?}")),
            }
        }
        assert!(
            mismatches.is_empty(),
            "iif_variadic_over_table_rows: {} mismatch(es)\n{}",
            mismatches.len(),
            mismatches.join("\n")
        );
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
