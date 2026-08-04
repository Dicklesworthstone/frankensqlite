//! bd-8ix5s — Oracle-parity e2e: VALUES row-arity consistency vs rusqlite.
//!
//! Every row of a `VALUES` clause must have the same number of terms; SQLite
//! rejects a ragged VALUES list ("all VALUES must have the same number of
//! terms"). This pins that a consistent multi-column VALUES is accepted and a
//! ragged one is rejected — confirming frank does not silently pad/truncate. The
//! rejected cases agree by both engines erroring; the accepted cases compare rows.

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
fn values_consistent_arity_ok() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "SELECT * FROM (VALUES (1,2),(3,4),(5,6)) ORDER BY 1", // (1,2),(3,4),(5,6)
                "SELECT * FROM (VALUES ('a',1),('b',2)) ORDER BY 1",   // (a,1),(b,2)
                "SELECT * FROM (VALUES (1)) ",                         // single 1-col row
            ],
            "values_consistent_arity_ok",
        )
        .await;
    });
}

#[test]
fn values_ragged_arity_rejected() {
    // SQLite rejects each of these ("all VALUES must have the same number of
    // terms"); the test confirms frank rejects them too (no silent pad/truncate).
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "VALUES (1,2),(3)",
                "VALUES (1),(2,3)",
                "SELECT * FROM (VALUES (1,2,3),(4,5))",
                "SELECT * FROM (VALUES (1),(2),(3,4)) ORDER BY 1",
            ],
            "values_ragged_arity_rejected",
        )
        .await;
    });
}
