//! bd-e92i9 — Oracle-parity e2e: likely()/unlikely()/likelihood() vs rusqlite.
//!
//! These are SQLite optimizer hints with NO runtime effect: `likely(X)` and
//! `unlikely(X)` return X unchanged (telling the planner X is probably true /
//! false); `likelihood(X, p)` returns X and takes a constant probability
//! `0.0 <= p <= 1.0`. Result-wise they are the identity on their first argument,
//! preserve its storage class, pass NULL through, and never change a WHERE
//! filter's outcome. `likelihood` with a probability outside [0,1] (or a
//! non-constant one) is an error. Compared against rusqlite.
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
fn likely_unlikely_identity() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "SELECT likely(1)",               // 1
                "SELECT unlikely(0)",             // 0
                "SELECT likely('x')",             // 'x'
                "SELECT unlikely(3.5)",           // 3.5
                "SELECT likely(NULL)",            // NULL
                "SELECT typeof(likely(5))",       // integer
                "SELECT typeof(likely(5.0))",     // real
                "SELECT typeof(unlikely('a'))",   // text
                "SELECT likely(2) + unlikely(3)", // 5
            ],
            "likely_unlikely_identity",
        )
        .await;
    });
}

#[test]
fn likelihood_with_probability() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "SELECT likelihood(42, 0.5)",         // 42
                "SELECT likelihood('a', 0.9)",        // 'a'
                "SELECT likelihood(NULL, 0.1)",       // NULL
                "SELECT likelihood(7, 0.0)",          // 7 (boundary)
                "SELECT likelihood(7, 1.0)",          // 7 (boundary)
                "SELECT typeof(likelihood(9, 0.25))", // integer
            ],
            "likelihood_with_probability",
        )
        .await;
    });
}

#[test]
fn likely_unlikely_in_where_filter() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for s in [
            "CREATE TABLE t (id INTEGER PRIMARY KEY, x INTEGER)",
            "INSERT INTO t VALUES (1,10),(2,3),(3,7),(4,5)",
        ] {
            f.execute(s).await.unwrap();
            r.execute_batch(s).unwrap();
        }
        check(
            &f,
            &r,
            &[
                // the hint must not change which rows match
                "SELECT id FROM t WHERE likely(x > 5) ORDER BY id", // 1,3
                "SELECT id FROM t WHERE unlikely(x = 3) ORDER BY id", // 2
                "SELECT id FROM t WHERE likelihood(x >= 5, 0.5) ORDER BY id", // 1,3,4
            ],
            "likely_unlikely_in_where_filter",
        )
        .await;
    });
}
