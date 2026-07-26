//! bd-dty93 — Oracle-parity e2e: LIKE / GLOB / ESCAPE pattern matching vs rusqlite.
//!
//! SQLite's pattern operators have precise, divergence-prone rules: LIKE is
//! case-insensitive for ASCII a-z/A-Z only (non-ASCII is NOT folded), `%`
//! matches any run and `_` any single character, ESCAPE makes the next pattern
//! char literal; GLOB is case-sensitive with Unix-glob `*`/`?` plus `[...]`
//! character classes including ranges `[a-z]` and negation `[^...]`. NULL
//! operands yield NULL. All inputs are fixed and deterministic.
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

async fn assert_scalar_parity(queries: &[&str], label: &str) {
    let f = Connection::open(":memory:").await.expect("open frank");
    let r = rusqlite::Connection::open_in_memory().expect("open rusqlite");
    check(&f, &r, queries, label).await;
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

#[test]
fn like_wildcards_basic() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar_parity(
            &[
                "SELECT 'hello' LIKE 'h%'",
                "SELECT 'hello' LIKE '%o'",
                "SELECT 'hello' LIKE '%ell%'",
                "SELECT 'hello' LIKE 'h_llo'",
                "SELECT 'hello' LIKE 'h__lo'",
                "SELECT 'hello' LIKE 'hello'",
                "SELECT 'hello' LIKE 'h%z'",
                "SELECT 'hello' LIKE '_____'", // exactly 5 chars
                "SELECT 'hello' LIKE '____'",  // 4 chars -> no
                "SELECT '' LIKE '%'",          // % matches empty
                "SELECT '' LIKE '_'",          // _ requires one char -> no
            ],
            "like_wildcards_basic",
        )
        .await;
    });
}

#[test]
fn like_ascii_case_insensitive() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar_parity(
            &[
                "SELECT 'ABC' LIKE 'abc'", // ASCII case-insensitive -> 1
                "SELECT 'Hello World' LIKE 'hello%'",
                "SELECT 'XyZ' LIKE 'xYz'",
                // Non-ASCII is NOT folded by the default LIKE.
                "SELECT 'É' LIKE 'é'", // -> 0
                "SELECT 'STRASSE' LIKE 'strasse'",
            ],
            "like_ascii_case_insensitive",
        )
        .await;
    });
}

#[test]
fn like_escape_clause() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar_parity(
            &[
                // ESCAPE makes %/_ literal.
                "SELECT '100%' LIKE '100\\%' ESCAPE '\\'",
                "SELECT '100x' LIKE '100\\%' ESCAPE '\\'", // x is not % -> 0
                "SELECT 'a_b' LIKE 'a\\_b' ESCAPE '\\'",
                "SELECT 'axb' LIKE 'a\\_b' ESCAPE '\\'", // literal _ required -> 0
                "SELECT '50%off' LIKE '50\\%off' ESCAPE '\\'",
                // Escaping the escape char itself.
                "SELECT 'a\\b' LIKE 'a\\\\b' ESCAPE '\\'",
                // Custom escape character.
                "SELECT '10%' LIKE '10#%' ESCAPE '#'",
            ],
            "like_escape_clause",
        )
        .await;
    });
}

#[test]
fn glob_wildcards_case_sensitive() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar_parity(
            &[
                "SELECT 'hello' GLOB 'h*'",
                "SELECT 'hello' GLOB '*o'",
                "SELECT 'hello' GLOB 'h?llo'",
                "SELECT 'hello' GLOB 'h*z'",
                // GLOB is case-SENSITIVE.
                "SELECT 'ABC' GLOB 'abc'", // -> 0
                "SELECT 'ABC' GLOB 'ABC'", // -> 1
                "SELECT 'abc' GLOB 'a?c'",
                "SELECT 'abc' GLOB 'a??'",
            ],
            "glob_wildcards_case_sensitive",
        )
        .await;
    });
}

#[test]
fn glob_character_classes() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar_parity(
            &[
                "SELECT 'cat' GLOB '[cb]at'",
                "SELECT 'bat' GLOB '[cb]at'",
                "SELECT 'rat' GLOB '[cb]at'",  // -> 0
                "SELECT 'a5z' GLOB 'a[0-9]z'", // digit range
                "SELECT 'aXz' GLOB 'a[0-9]z'", // -> 0
                "SELECT 'mid' GLOB '[a-z][a-z][a-z]'",
                // Negation.
                "SELECT 'dog' GLOB '[^c]og'", // d not c -> 1
                "SELECT 'cog' GLOB '[^c]og'", // -> 0
                // Range + case sensitivity.
                "SELECT 'B' GLOB '[A-Z]'",
                "SELECT 'b' GLOB '[A-Z]'", // -> 0
                "SELECT 'b' GLOB '[a-z]'",
            ],
            "glob_character_classes",
        )
        .await;
    });
}

#[test]
fn like_glob_not_and_null() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar_parity(
            &[
                "SELECT 'hello' NOT LIKE 'h%'", // -> 0
                "SELECT 'hello' NOT LIKE 'z%'", // -> 1
                "SELECT 'abc' NOT GLOB 'a*'",   // -> 0
                // NULL operands -> NULL.
                "SELECT NULL LIKE 'a%'",
                "SELECT 'abc' LIKE NULL",
                "SELECT NULL GLOB '*'",
                "SELECT 'abc' GLOB NULL",
                // Numbers are coerced to text for matching.
                "SELECT 12345 LIKE '123%'",
                "SELECT 3.5 LIKE '3.%'",
            ],
            "like_glob_not_and_null",
        )
        .await;
    });
}

#[test]
fn like_glob_in_where_clause() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for s in [
            "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)",
            "INSERT INTO t VALUES (1,'apple'),(2,'Apricot'),(3,'banana'),(4,'avocado'),(5,'CHERRY'),(6,'date_1')",
        ] {
            f.execute(s).await.unwrap();
            r.execute_batch(s).unwrap();
        }
        check(
            &f,
            &r,
            &[
                "SELECT id FROM t WHERE name LIKE 'a%' ORDER BY id", // LIKE case-insensitive: apple, Apricot, avocado
                "SELECT id FROM t WHERE name GLOB 'a*' ORDER BY id", // GLOB case-sensitive: apple, avocado
                "SELECT id FROM t WHERE name LIKE '%a%a%' ORDER BY id",
                "SELECT id FROM t WHERE name GLOB '[A-Z]*' ORDER BY id", // uppercase first letter
                "SELECT id FROM t WHERE name LIKE 'date\\_%' ESCAPE '\\' ORDER BY id",
                "SELECT name FROM t WHERE name NOT LIKE '%a%' ORDER BY id",
            ],
            "like_glob_in_where_clause",
        )
        .await;
    });
}
