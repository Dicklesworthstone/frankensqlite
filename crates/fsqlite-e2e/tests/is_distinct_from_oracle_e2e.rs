//! bd-in5db — Oracle-parity e2e: IS [NOT] DISTINCT FROM vs rusqlite.
//!
//! The NULL-safe comparison operators (SQLite 3.39+): `a IS NOT DISTINCT FROM b`
//! is TRUE iff a and b are equal OR both NULL (never NULL itself); `a IS DISTINCT
//! FROM b` is its negation (TRUE if they differ or exactly one is NULL). Unlike
//! `=`, these always return 0 or 1, never NULL. These verify the scalar truth
//! table (incl. NULL/NULL and NULL/value), use in a WHERE clause to match NULL
//! rows and values, and text/mixed operands. Compared against rusqlite.
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

async fn check(f: &Connection, r: &rusqlite::Connection, queries: &[&str], label: &str) {
    let mut mismatches = Vec::new();
    for q in queries {
        match (frank_rows(f, q).await, sqlite_rows(r, q)) {
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

async fn assert_scalar(queries: &[&str], label: &str) {
    let f = Connection::open(":memory:").await.expect("open frank");
    let r = rusqlite::Connection::open_in_memory().expect("open rusqlite");
    check(&f, &r, queries, label).await;
}

#[test]
fn is_distinct_from_scalar() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "SELECT 1 IS DISTINCT FROM 1",       // 0 (equal)
                "SELECT 1 IS DISTINCT FROM 2",       // 1
                "SELECT NULL IS DISTINCT FROM NULL", // 0 (both NULL)
                "SELECT NULL IS DISTINCT FROM 1",    // 1 (one NULL)
                "SELECT 1 IS DISTINCT FROM NULL",    // 1
            ],
            "is_distinct_from_scalar",
        )
        .await;
    });
}

#[test]
fn is_not_distinct_from_scalar() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "SELECT 1 IS NOT DISTINCT FROM 1",       // 1
                "SELECT 1 IS NOT DISTINCT FROM 2",       // 0
                "SELECT NULL IS NOT DISTINCT FROM NULL", // 1 (NULL-safe equal)
                "SELECT NULL IS NOT DISTINCT FROM 1",    // 0
                "SELECT 1 IS NOT DISTINCT FROM NULL",    // 0
            ],
            "is_not_distinct_from_scalar",
        )
        .await;
    });
}

#[test]
fn is_distinct_from_text_and_mixed() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "SELECT 'a' IS DISTINCT FROM 'a'",      // 0
                "SELECT 'a' IS DISTINCT FROM 'b'",      // 1
                "SELECT 'x' IS NOT DISTINCT FROM NULL", // 0
                // No-affinity comparison: 1 vs '1' differ -> distinct.
                "SELECT 1 IS DISTINCT FROM '1'", // 1
            ],
            "is_distinct_from_text_and_mixed",
        )
        .await;
    });
}

#[test]
fn is_distinct_from_in_where() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for s in [
            "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)",
            "INSERT INTO t VALUES (1,10),(2,NULL),(3,20),(4,NULL)",
        ] {
            f.execute(s).await.unwrap();
            r.execute_batch(s).unwrap();
        }
        check(
            &f,
            &r,
            &[
                // NULL-safe match of the NULL rows (unlike `= NULL` which matches none).
                "SELECT id FROM t WHERE v IS NOT DISTINCT FROM NULL ORDER BY id", // 2,4
                // Distinct-from a value includes the NULL rows.
                "SELECT id FROM t WHERE v IS DISTINCT FROM 10 ORDER BY id", // 2,3,4
                "SELECT id FROM t WHERE v IS NOT DISTINCT FROM 10 ORDER BY id", // 1
            ],
            "is_distinct_from_in_where",
        )
        .await;
    });
}
