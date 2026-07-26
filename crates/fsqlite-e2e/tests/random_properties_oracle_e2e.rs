//! bd-22dhp — Oracle-parity e2e: random()/randomblob() deterministic properties.
//!
//! random() and randomblob(N) are non-deterministic, so their VALUES can't be
//! compared. But their TYPE, LENGTH, and RANGE properties are deterministic and
//! match between engines: `random()` returns an INTEGER in the full signed i64
//! range; `randomblob(N)` returns a BLOB of exactly N bytes (0 -> empty blob).
//! These properties pin that frank implements both functions and that they
//! behave the same shape-wise.

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
fn random_returns_integer_in_i64_range() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "SELECT typeof(random())", // integer
                // every random() result fits in signed i64 -- deterministic predicate.
                "SELECT random() BETWEEN -9223372036854775808 AND 9223372036854775807",
            ],
            "random_returns_integer_in_i64_range",
        )
        .await;
    });
}

#[test]
fn randomblob_type_and_length() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "SELECT typeof(randomblob(16))",  // blob
                "SELECT length(randomblob(0))",   // 0 -> empty blob
                "SELECT length(randomblob(1))",   // 1
                "SELECT length(randomblob(16))",  // 16
                "SELECT length(randomblob(256))", // 256
                "SELECT randomblob(0) = X''",     // 1 (empty matches empty)
            ],
            "randomblob_type_and_length",
        )
        .await;
    });
}
