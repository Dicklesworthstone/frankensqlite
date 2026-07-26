//! bd-0xt8n — Oracle-parity e2e: literal forms & boolean keywords vs rusqlite.
//!
//! Parser/literal edges not covered elsewhere: string escaping via doubled
//! single quotes ('it''s') and the empty string, blob literals (`X'..'`,
//! lowercase `x'..'`, empty `X''`), hexadecimal integer literals (`0xFF`),
//! floating-point literal forms (`.5`, `5.`, scientific `1e3`/`1.5e-2`),
//! `char()`/`unicode()` round-trips, the `TRUE`/`FALSE` keywords (which are
//! integer 1/0), and the `IS TRUE` / `IS FALSE` / `IS NOT TRUE` predicates
//! (SQLite 3.23+). Each scenario compares constant-expression results against
//! rusqlite; the riskier float-form and IS-TRUE/FALSE cases are isolated.
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
fn lit_string_escaping_and_empty() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "SELECT 'it''s'",                     // it's
                "SELECT ''",                          // empty
                "SELECT 'a''b''c'",                   // a'b'c
                "SELECT length(''), length('it''s')", // 0, 4
                "SELECT typeof(''), typeof('x')",     // text, text
            ],
            "lit_string_escaping_and_empty",
        )
        .await;
    });
}

#[test]
fn lit_blob_forms() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "SELECT X'48656C6C6F'",                // 'Hello' bytes
                "SELECT x'48'",                        // lowercase prefix
                "SELECT X''",                          // empty blob
                "SELECT typeof(X'00'), length(X'00')", // blob, 1
                "SELECT hex(X'DEADBEEF')",             // 'DEADBEEF'
            ],
            "lit_blob_forms",
        )
        .await;
    });
}

#[test]
fn lit_integer_forms() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "SELECT 0xFF, 0x10, 0x0", // 255, 16, 0
                "SELECT typeof(0xFF)",    // integer
                "SELECT 1000000, -42, +7",
                "SELECT 9223372036854775807", // max int64
            ],
            "lit_integer_forms",
        )
        .await;
    });
}

#[test]
fn lit_float_forms() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "SELECT .5, 1.5",                             // 0.5, 1.5
                "SELECT 5.",                                  // 5.0
                "SELECT 1e3, 1.5e2, 2E3",                     // 1000.0, 150.0, 2000.0
                "SELECT 1.5e-2",                              // 0.015
                "SELECT typeof(.5), typeof(5.), typeof(1e3)", // real, real, real
            ],
            "lit_float_forms",
        )
        .await;
    });
}

#[test]
fn lit_char_unicode_roundtrip() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "SELECT char(72, 73)",       // 'HI'
                "SELECT char(65)",           // 'A'
                "SELECT unicode('A')",       // 65
                "SELECT unicode(char(233))", // 233 (é round-trip)
            ],
            "lit_char_unicode_roundtrip",
        )
        .await;
    });
}

#[test]
fn bool_true_false_keywords() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "SELECT TRUE, FALSE",                   // 1, 0
                "SELECT typeof(TRUE), typeof(FALSE)",   // integer, integer
                "SELECT TRUE AND FALSE, TRUE OR FALSE", // 0, 1
                "SELECT NOT TRUE, NOT FALSE",           // 0, 1
            ],
            "bool_true_false_keywords",
        )
        .await;
    });
}

#[test]
fn bool_is_true_is_false() {
    asupersync::test_utils::run_test(|| async {
        // IS TRUE / IS FALSE / IS NOT TRUE / IS NOT FALSE (SQLite 3.23+).
        assert_scalar(
            &[
                "SELECT 2 IS TRUE, 0 IS FALSE",               // 1, 1
                "SELECT 0 IS TRUE, 5 IS FALSE",               // 0, 0
                "SELECT NULL IS TRUE, NULL IS FALSE",         // 0, 0 (NULL is neither)
                "SELECT NULL IS NOT TRUE, NULL IS NOT FALSE", // 1, 1
            ],
            "bool_is_true_is_false",
        )
        .await;
    });
}
