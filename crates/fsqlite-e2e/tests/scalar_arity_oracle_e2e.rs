//! bd-v7932 — Oracle-parity e2e: scalar function arity validation vs rusqlite.
//!
//! SQLite rejects a call with the wrong number of arguments ("wrong number of
//! arguments to function X()"). This pins that frank enforces the same arity for
//! a representative set of fixed-arity builtins (abs/length/upper/lower take 1,
//! ifnull/nullif take 2, coalesce takes >= 2, quote takes 1) — both too few and
//! too many — rather than silently ignoring or padding extra/missing arguments.
//! Rejected calls agree by both engines erroring; the correct-arity contrast
//! cases compare results.

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
        let frank = frank_rows(&f, q).await;
        match (frank, sqlite_rows(&r, q)) {
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
fn scalar_arity_wrong_count_rejected() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "SELECT abs()",           // too few (needs 1)
                "SELECT abs(1, 2)",       // too many
                "SELECT length()",        // too few
                "SELECT upper()",         // too few
                "SELECT lower('a', 'b')", // too many
                "SELECT ifnull(1)",       // needs 2
                "SELECT ifnull(1, 2, 3)", // too many
                "SELECT nullif(1)",       // needs 2
                "SELECT coalesce(1)",     // needs >= 2
                "SELECT quote()",         // needs 1
                "SELECT quote(1, 2)",     // too many
            ],
            "scalar_arity_wrong_count_rejected",
        )
        .await;
    });
}

#[test]
fn scalar_arity_correct_ok() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "SELECT abs(-3)",                 // 3
                "SELECT length('abc')",           // 3
                "SELECT upper('a')",              // 'A'
                "SELECT lower('AB')",             // 'ab'
                "SELECT ifnull(NULL, 5)",         // 5
                "SELECT nullif(1, 2)",            // 1
                "SELECT coalesce(NULL, NULL, 7)", // 7
                "SELECT quote('x')",              // 'x' quoted
            ],
            "scalar_arity_correct_ok",
        )
        .await;
    });
}
