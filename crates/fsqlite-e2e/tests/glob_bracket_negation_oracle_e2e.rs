//! bd-uowxe — Oracle-parity e2e: GLOB bracket negation vs rusqlite.
//!
//! SQLite's GLOB inverts a bracket expression only with a leading `^`. A leading
//! `!` is NOT a negator (unlike some shell globs); `[!a]` literally matches a `!`
//! or an `a`. like_glob_oracle covers `[^...]`; this pins the `[!...]` literal
//! interpretation alongside it.
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
fn glob_caret_negates_bracket() {
    asupersync::test_utils::run_test(|| async {
        // SQLite negates a bracket expression with a leading '^'.
        assert_scalar(
            &[
                "SELECT 'a' GLOB '[^a]'",   // 0 (a excluded)
                "SELECT 'b' GLOB '[^a]'",   // 1
                "SELECT 'a' GLOB '[^abc]'", // 0
                "SELECT 'd' GLOB '[^abc]'", // 1
            ],
            "glob_caret_negates_bracket",
        )
        .await;
    });
}

#[test]
fn glob_exclamation_is_literal_inside_bracket() {
    asupersync::test_utils::run_test(|| async {
        // '!' is NOT a negation character in SQLite GLOB; '[!a]' matches '!' or 'a'.
        assert_scalar(
            &[
                "SELECT 'a' GLOB '[!a]'", // 1 (matches 'a')
                "SELECT '!' GLOB '[!a]'", // 1 (matches '!')
                "SELECT 'b' GLOB '[!a]'", // 0 (b is neither '!' nor 'a')
            ],
            "glob_exclamation_is_literal_inside_bracket",
        )
        .await;
    });
}
