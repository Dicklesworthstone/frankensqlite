//! bd-vcghf — Oracle-parity e2e: abs() text/blob numeric coercion vs rusqlite.
//!
//! math_function_oracle / scalar_function_oracle exercise abs() on numeric and
//! NULL inputs. SQLite also applies numeric affinity when the input is text or a
//! blob: `abs('-5')` -> 5 (integer), `abs('-5.5')` -> 5.5 (real), non-numeric
//! text yields 0, and a blob is read as its text bytes before the same numeric
//! coercion (`abs(X'2D35')` = `abs('-5')` -> 5). This pins those.
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
fn abs_on_text_coerces() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "SELECT abs('-5')",          // -5 -> 5 (integer)
                "SELECT abs('5')",           // 5
                "SELECT abs('-5.5')",        // -5.5 -> 5.5 (real)
                "SELECT abs('abc')",         // non-numeric -> 0
                "SELECT abs('')",            // empty text -> 0
                "SELECT typeof(abs('5'))",   // integer
                "SELECT typeof(abs('5.5'))", // real
                "SELECT typeof(abs('abc'))", // integer (0)
            ],
            "abs_on_text_coerces",
        )
        .await;
    });
}

#[test]
fn abs_on_blob_coerces() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                // X'2D35' is the bytes for '-5' -> -5 -> abs -> 5
                "SELECT abs(X'2D35')",
                "SELECT typeof(abs(X'2D35'))", // integer
                "SELECT abs(X'')",             // empty blob -> 0
            ],
            "abs_on_blob_coerces",
        )
        .await;
    });
}
