//! bd-8mifl — Oracle-parity e2e: CAST from BLOB vs rusqlite.
//!
//! cast_expression_oracle covers CAST *to* BLOB; this covers casting a BLOB to
//! other types. `CAST(blob AS TEXT)` reinterprets the raw bytes as text;
//! `CAST(blob AS INTEGER/REAL)` first treats the bytes as text and then applies
//! the usual text->number prefix parse (so `X'3132'` = "12" -> 12, `X'6162'` =
//! "ab" -> 0). NUMERIC and NULL/typeof variants are included. Compared against
//! rusqlite. Invalid UTF-8 cases project through `hex(CAST(... AS BLOB))` so
//! the oracle comparison observes the exact bytes without asking Rust strings
//! to represent them.
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
    f.close().await.expect("close frank oracle database");
    assert!(
        mismatches.is_empty(),
        "{label}: {} mismatch(es)\n{}",
        mismatches.len(),
        mismatches.join("\n")
    );
}

#[test]
fn cast_blob_to_text() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "SELECT CAST(X'48656C6C6F' AS TEXT)",         // 'Hello'
                "SELECT CAST(X'31' AS TEXT)",                 // '1'
                "SELECT CAST(X'' AS TEXT)",                   // '' (empty)
                "SELECT typeof(CAST(X'41' AS TEXT))",         // text
                "SELECT length(CAST(X'48656C6C6F' AS TEXT))", // 5
            ],
            "cast_blob_to_text",
        )
        .await;
    });
}

#[test]
fn cast_blob_to_text_preserves_invalid_utf8_bytes() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "SELECT hex(CAST(CAST(X'80' AS TEXT) AS BLOB))",
                "SELECT hex(CAST(CAST(X'C0AF' AS TEXT) AS BLOB))",
                "SELECT hex(CAST(CAST(X'EDA080' AS TEXT) AS BLOB))",
                "SELECT hex(CAST(CAST(X'F08080AF' AS TEXT) AS BLOB))",
                "SELECT typeof(CAST(X'80' AS TEXT))",
            ],
            "cast_blob_to_text_preserves_invalid_utf8_bytes",
        )
        .await;
    });
}

#[test]
fn invalid_utf8_text_record_round_trips_without_substitution() {
    asupersync::test_utils::run_test(|| async {
        let frank = Connection::open(":memory:").await.expect("open frank");
        let sqlite = rusqlite::Connection::open_in_memory().expect("open rusqlite");
        let setup = "CREATE TABLE raw_text(x TEXT);
                     INSERT INTO raw_text VALUES
                         (CAST(X'80' AS TEXT)),
                         (CAST(X'C0AF' AS TEXT)),
                         (CAST(X'EDA080' AS TEXT)),
                         (CAST(X'F08080AF' AS TEXT));";
        frank
            .execute_batch(setup)
            .await
            .expect("insert frank raw TEXT");
        sqlite.execute_batch(setup).expect("insert sqlite raw TEXT");

        let query = "SELECT typeof(x), hex(CAST(x AS BLOB)) FROM raw_text ORDER BY rowid";
        let frank_rows = frank_rows(&frank, query)
            .await
            .expect("query frank raw TEXT");
        let sqlite_rows = sqlite_rows(&sqlite, query).expect("query sqlite raw TEXT");
        assert_eq!(
            frank_rows, sqlite_rows,
            "stored SQLite TEXT must retain its exact invalid UTF-8 payload"
        );
        frank.close().await.expect("close frank raw TEXT database");
    });
}

#[test]
fn cast_blob_to_integer() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "SELECT CAST(X'31' AS INTEGER)",         // '1' -> 1
                "SELECT CAST(X'313233' AS INTEGER)",     // '123' -> 123
                "SELECT CAST(X'2D3432' AS INTEGER)",     // '-42' -> -42
                "SELECT CAST(X'3132616263' AS INTEGER)", // '12abc' -> 12 (prefix)
                "SELECT CAST(X'6162' AS INTEGER)",       // 'ab' -> 0
                "SELECT CAST(X'' AS INTEGER)",           // empty -> 0
            ],
            "cast_blob_to_integer",
        )
        .await;
    });
}

#[test]
fn cast_blob_to_real_and_numeric() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "SELECT CAST(X'332E3134' AS REAL)",     // '3.14' -> 3.14
                "SELECT CAST(X'3235' AS REAL)",         // '25' -> 25.0
                "SELECT CAST(X'332E30' AS NUMERIC)",    // '3.0' -> 3 (numeric reduces)
                "SELECT CAST(X'332E35' AS NUMERIC)",    // '3.5' -> 3.5
                "SELECT typeof(CAST(X'3235' AS REAL))", // real
            ],
            "cast_blob_to_real_and_numeric",
        )
        .await;
    });
}
