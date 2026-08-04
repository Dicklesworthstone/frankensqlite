//! bd-mide4 — Oracle-parity e2e: instr() blob/NULL/numeric edges vs rusqlite.
//!
//! scalar_function_oracle / string_function_oracle cover the text-text instr()
//! happy path and the empty-needle / not-found cases. SQLite's `instr(X, Y)`
//! also has documented behaviour for non-text arguments: if BOTH X and Y are
//! BLOBs the function works in byte space and returns the 1-based byte position;
//! mixed text/blob arguments are coerced to text; NULL on either side propagates
//! to NULL; numeric arguments are coerced to their text form. This pins those.
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
fn instr_blob_in_blob() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                // X'48656C6C6F' is 'Hello'; X'6C' is 'l' -> byte position 3.
                "SELECT instr(X'48656C6C6F', X'6C')",
                // not found -> 0
                "SELECT instr(X'48656C6C6F', X'7A')",
                // empty needle -> 1
                "SELECT instr(X'48656C6C6F', X'')",
                // empty haystack -> 0
                "SELECT instr(X'', X'00')",
                "SELECT typeof(instr(X'4142', X'42'))", // integer
            ],
            "instr_blob_in_blob",
        )
        .await;
    });
}

#[test]
fn instr_null_and_numeric() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "SELECT instr(NULL, 'x')",   // NULL propagation
                "SELECT instr('abc', NULL)", // NULL propagation
                "SELECT instr(NULL, NULL)",  // NULL
                // numeric argument coerced to its text form
                "SELECT instr(12345, '23')", // '12345' contains '23' at 2 -> 2
                "SELECT instr(12345, 9)",    // '12345' / '9' -> 0
                // mixed blob+text: both interpreted as text -> X'4142' = 'AB', find 'B' at 2
                "SELECT instr(X'4142', 'B')",
            ],
            "instr_null_and_numeric",
        )
        .await;
    });
}
