//! bd-9efrj — Oracle-parity e2e: PRAGMA case_sensitive_like vs rusqlite.
//!
//! By default SQLite's LIKE folds ASCII case (`'A' LIKE 'a'` is true) but only
//! for the 26 ASCII letters — non-ASCII letters are never folded. The write-only
//! `PRAGMA case_sensitive_like = ON` makes LIKE byte-exact (case-sensitive); `OFF`
//! restores the default; the setting is per-connection and can be toggled back
//! and forth. GLOB is always case-sensitive and is completely unaffected by the
//! pragma. These verify the toggle, its scope (LIKE only), GLOB's independence,
//! and the behavior inside a WHERE filter, against rusqlite. The pragma is
//! set-only (it returns no rows), so each phase sets it on both engines and then
//! compares query results.
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

/// Run a setter statement (e.g. a PRAGMA) on both engines, asserting they agree
/// on success/failure.
async fn exec_both(f: &Connection, r: &rusqlite::Connection, sql: &str, label: &str) {
    let fe = f.execute(sql).await;
    let re = r.execute_batch(sql);
    match (&fe, &re) {
        (Ok(_), Ok(())) | (Err(_), Err(_)) => {}
        (Ok(_), Err(e)) => panic!("{label}: `{sql}`\n  frank: OK\n  csql:  ERROR({e})"),
        (Err(e), Ok(())) => panic!("{label}: `{sql}`\n  frank: ERROR({e})\n  csql:  OK"),
    }
}

#[test]
fn like_default_is_ascii_case_insensitive() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        check(
            &f,
            &r,
            &[
                "SELECT 'A' LIKE 'a'",         // 1 (ASCII fold)
                "SELECT 'apple' LIKE 'APPLE'", // 1
                "SELECT 'a' LIKE 'A'",         // 1
                "SELECT 'aBcD' LIKE 'AbCd'",   // 1
                "SELECT 'abc' LIKE 'abx'",     // 0 (real mismatch)
                // Non-ASCII letters are NEVER folded by LIKE, pragma or not.
                "SELECT 'À' LIKE 'à'", // 0
            ],
            "like_default_is_ascii_case_insensitive",
        )
        .await;
    });
}

#[test]
fn like_case_sensitive_pragma_on() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        exec_both(
            &f,
            &r,
            "PRAGMA case_sensitive_like = ON",
            "like_case_sensitive_pragma_on",
        )
        .await;
        check(
            &f,
            &r,
            &[
                "SELECT 'A' LIKE 'a'",         // 0 (now case-sensitive)
                "SELECT 'A' LIKE 'A'",         // 1
                "SELECT 'apple' LIKE 'apple'", // 1
                "SELECT 'apple' LIKE 'APPLE'", // 0
                // wildcards still work, but the literal part is now case-sensitive
                "SELECT 'Apple' LIKE 'A%'", // 1
                "SELECT 'apple' LIKE 'A%'", // 0
            ],
            "like_case_sensitive_pragma_on",
        )
        .await;
    });
}

#[test]
fn like_pragma_toggles_back_off() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        // Phase 1: ON -> case-sensitive.
        exec_both(
            &f,
            &r,
            "PRAGMA case_sensitive_like = ON",
            "like_pragma_toggles_back_off",
        )
        .await;
        check(&f, &r, &["SELECT 'A' LIKE 'a'"], "toggle ON phase").await; // 0
        // Phase 2: OFF -> restores ASCII-insensitive default.
        exec_both(
            &f,
            &r,
            "PRAGMA case_sensitive_like = OFF",
            "like_pragma_toggles_back_off",
        )
        .await;
        check(&f, &r, &["SELECT 'A' LIKE 'a'"], "toggle OFF phase").await; // 1
    });
}

#[test]
fn glob_unaffected_by_case_sensitive_like() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        // GLOB is always case-sensitive in the default state...
        check(
            &f,
            &r,
            &[
                "SELECT 'A' GLOB 'a'", // 0
                "SELECT 'a' GLOB 'a'", // 1
            ],
            "glob default",
        )
        .await;
        // ...and turning the LIKE pragma ON leaves GLOB exactly the same.
        exec_both(
            &f,
            &r,
            "PRAGMA case_sensitive_like = ON",
            "glob_unaffected_by_case_sensitive_like",
        )
        .await;
        check(
            &f,
            &r,
            &[
                "SELECT 'A' GLOB 'a'", // still 0
                "SELECT 'a' GLOB 'a'", // still 1
            ],
            "glob after pragma ON",
        )
        .await;
    });
}

#[test]
fn like_pragma_in_where_filter() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for s in [
            "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)",
            "INSERT INTO t VALUES (1,'apple'),(2,'Apple'),(3,'APPLE'),(4,'banana')",
        ] {
            exec_both(&f, &r, s, "like_pragma_in_where_filter").await;
        }
        // Default: case-insensitive -> all three apple variants.
        check(
            &f,
            &r,
            &["SELECT id FROM t WHERE name LIKE 'apple' ORDER BY id"], // 1,2,3
            "where default",
        )
        .await;
        // ON: case-sensitive -> only the exact 'apple'.
        exec_both(
            &f,
            &r,
            "PRAGMA case_sensitive_like = ON",
            "like_pragma_in_where_filter",
        )
        .await;
        check(
            &f,
            &r,
            &["SELECT id FROM t WHERE name LIKE 'apple' ORDER BY id"], // 1
            "where pragma ON",
        )
        .await;
    });
}
