//! bd-ftra6 — Oracle-parity e2e: newer SQLite string/blob functions vs rusqlite.
//!
//! SQLite added these between 3.41 and 3.44; the bundled reference build has
//! them. They have semantics that are easy to get subtly wrong:
//!   * `concat(...)` (3.44) — concatenates every argument as text but treats
//!     NULL as the empty string, NOT propagating NULL the way `||` does.
//!   * `concat_ws(sep, ...)` (3.44) — joins the non-NULL value arguments with
//!     `sep`, skipping NULLs (no doubled separators); a NULL separator makes the
//!     whole result NULL.
//!   * `string_agg(X, sep)` (3.44) — the aggregate spelling of
//!     `group_concat(X, sep)`, NULL inputs skipped.
//!   * `octet_length(X)` (3.43) — byte length of the value's text/blob form
//!     (differs from `length()` for multibyte text).
//!   * `unhex(X)` (3.41) — inverse of `hex()`, returning a BLOB or NULL on bad
//!     input.
//! Each is compared against rusqlite.

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

async fn check(f: &Connection, r: &rusqlite::Connection, queries: &[&str], label: &str) {
    let mut mismatches = Vec::new();
    for q in queries {
        match (frank_rows(f, q).await, sqlite_rows(r, q)) {
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

async fn assert_scalar(queries: &[&str], label: &str) {
    let f = Connection::open(":memory:").await.expect("open frank");
    let r = rusqlite::Connection::open_in_memory().expect("open rusqlite");
    check(&f, &r, queries, label).await;
}

#[test]
fn concat_treats_null_as_empty() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "SELECT concat('a','b','c')",    // 'abc'
                "SELECT concat('a', NULL, 'b')", // 'ab' (NULL skipped, NOT NULL like ||)
                "SELECT concat(1, 2, 3)",        // '123' (numbers coerced to text)
                "SELECT concat('x', 1, 2.5)",    // 'x12.5'
                "SELECT concat(NULL, NULL)",     // '' (empty string, never NULL)
                "SELECT concat('a')",            // 'a'
                "SELECT typeof(concat('a', 1))", // text
                // contrast: || propagates NULL where concat() does not
                "SELECT 'a' || NULL || 'b'", // NULL
            ],
            "concat_treats_null_as_empty",
        )
        .await;
    });
}

#[test]
fn concat_ws_separator_rules() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "SELECT concat_ws('-', 'a', 'b', 'c')",  // 'a-b-c'
                "SELECT concat_ws('-', 'a', NULL, 'c')", // 'a-c' (NULL skipped, no doubled sep)
                "SELECT concat_ws(',', 1, 2, 3)",        // '1,2,3'
                "SELECT concat_ws('-', 'only')",         // 'only' (no trailing sep)
                "SELECT concat_ws(',', NULL, NULL)",     // '' (all values NULL -> empty)
                "SELECT concat_ws(NULL, 'a', 'b')",      // NULL (NULL separator -> NULL)
                "SELECT typeof(concat_ws(',', 'a'))",    // text
            ],
            "concat_ws_separator_rules",
        )
        .await;
    });
}

#[test]
fn string_agg_aggregate() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.expect("open frank");
        let r = rusqlite::Connection::open_in_memory().expect("open rusqlite");
        for s in [
            "CREATE TABLE t (id INTEGER PRIMARY KEY, grp TEXT, v TEXT)",
            "INSERT INTO t VALUES (1,'x','a'),(2,'x','b'),(3,'y','c'),(4,'y',NULL),(5,'y','d')",
        ] {
            f.execute(s).await.unwrap();
            r.execute_batch(s).unwrap();
        }
        check(
            &f,
            &r,
            &[
                // Whole-table aggregate in rowid order (NULL skipped).
                "SELECT string_agg(v, ',') FROM t", // 'a,b,c,d'
                // Per group, NULL skipped, no doubled separator.
                "SELECT grp, string_agg(v, '-') FROM t GROUP BY grp ORDER BY grp", // x:'a-b', y:'c-d'
                // string_agg is the aggregate spelling of group_concat with a separator.
                "SELECT group_concat(v, ',') FROM t", // same as string_agg form
            ],
            "string_agg_aggregate",
        )
        .await;
    });
}

#[test]
fn octet_length_and_unhex() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "SELECT octet_length('abc')",      // 3
                "SELECT octet_length('')",         // 0
                "SELECT octet_length('é')",        // 2 (UTF-8 two bytes)
                "SELECT length('é')",              // 1 (one character) — contrast
                "SELECT octet_length(X'010203')",  // 3
                "SELECT octet_length(12345)",      // 5 (text form '12345')
                "SELECT unhex('414243')",          // X'414243'  ('ABC' bytes)
                "SELECT typeof(unhex('41'))",      // blob
                "SELECT hex(unhex('48656C6C6F'))", // '48656C6C6F' round trip
                "SELECT unhex('xyz')",             // NULL (invalid hex)
                "SELECT unhex('')",                // X'' (empty blob)
            ],
            "octet_length_and_unhex",
        )
        .await;
    });
}
