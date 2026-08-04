//! bd-jqzmo — Oracle-parity e2e: unhex(X, ignore_chars) 2-arg form vs rusqlite.
//!
//! newer_string_functions_oracle covers the 1-arg `unhex(X)`. SQLite 3.41 also
//! takes a second argument: a set of characters that may appear between hex
//! digits and are stripped before decoding (`unhex('48 65', ' ')` -> X'4865').
//! Any character that is neither a hex digit nor in the ignore set makes the
//! result NULL; an empty ignore set behaves like the 1-arg form; a NULL argument
//! propagates. Compared against rusqlite.

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
fn unhex_two_arg_ignores_chars() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "SELECT unhex('48 65 6C 6C 6F', ' ')", // X'48656C6C6F' (spaces stripped)
                "SELECT unhex('48-65', '-')",          // X'4865'
                "SELECT unhex('48:65:6C', ':')",       // X'48656C'
                "SELECT unhex('4865', '')",            // X'4865' (empty ignore == 1-arg)
                "SELECT typeof(unhex('41 42', ' '))",  // blob
            ],
            "unhex_two_arg_ignores_chars",
        )
        .await;
    });
}

#[test]
fn unhex_two_arg_invalid_and_null() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "SELECT unhex('48x65', ' ')", // NULL (x neither hex nor ignored)
                "SELECT unhex('4 8', '')",    // NULL (space invalid with empty ignore)
                "SELECT unhex(NULL, ' ')",    // NULL (null propagates)
                "SELECT unhex('48', NULL)",   // NULL (null ignore arg)
            ],
            "unhex_two_arg_invalid_and_null",
        )
        .await;
    });
}
