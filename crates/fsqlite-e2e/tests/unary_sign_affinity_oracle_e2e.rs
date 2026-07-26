//! bd-kx2yp — Oracle-parity e2e: unary +/- type semantics vs rusqlite.
//!
//! A famous SQLite subtlety: the unary `+` operator is a pure no-op that returns
//! its operand UNCHANGED, preserving the original storage class — `+'5'` is still
//! the text `'5'`, `+X'41'` is still that blob. The unary `-` operator, by
//! contrast, applies numeric affinity: `-'5'` is the integer -5, `-'5.5'` is the
//! real -5.5, and `-'abc'` is the integer 0. NULL passes through both. typeof_
//! result_oracle only checks `typeof(+7)`; this pins the text/blob/NULL cases for
//! both operators, against rusqlite.

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
fn unary_plus_is_type_preserving_noop() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "SELECT +'5'",            // '5' (still TEXT, not coerced)
                "SELECT typeof(+'5')",    // text
                "SELECT +'abc'",          // 'abc'
                "SELECT typeof(+'abc')",  // text
                "SELECT +X'41'",          // X'41' (blob unchanged)
                "SELECT typeof(+X'41')",  // blob
                "SELECT +NULL",           // NULL
                "SELECT typeof(+NULL)",   // null
                "SELECT +5",              // 5
                "SELECT typeof(+5.0)",    // real
                "SELECT typeof(+(+'5'))", // text (still no-op when nested)
            ],
            "unary_plus_is_type_preserving_noop",
        )
        .await;
    });
}

#[test]
fn unary_minus_applies_numeric_affinity() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "SELECT -'5'",            // -5
                "SELECT typeof(-'5')",    // integer
                "SELECT -'5.5'",          // -5.5
                "SELECT typeof(-'5.5')",  // real
                "SELECT -'abc'",          // 0 (non-numeric text -> 0)
                "SELECT typeof(-'abc')",  // integer
                "SELECT -NULL",           // NULL
                "SELECT typeof(-NULL)",   // null
                "SELECT typeof(-(-'5'))", // integer (coercion sticks)
            ],
            "unary_minus_applies_numeric_affinity",
        )
        .await;
    });
}
