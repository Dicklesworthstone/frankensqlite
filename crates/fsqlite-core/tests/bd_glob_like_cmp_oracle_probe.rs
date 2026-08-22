#![recursion_limit = "512"]

//! Differential oracle sweep (pane af49, 2026-08-20): frank vs rusqlite over
//! LIKE/GLOB pattern semantics (ASCII case-folding, wildcards, ESCAPE, char
//! classes), and comparison/affinity edges (BETWEEN, IN with mixed types, NULL
//! three-valued logic, numeric-vs-text ordering). Pass = parity coverage keeper;
//! a mismatch is a leaf divergence. Typed structural compare.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => format!("int:{n}"),
        SqliteValue::Float(f) => format!("real:{f}"),
        SqliteValue::Text(s) => format!("text:{s}"),
        SqliteValue::Blob(b) => format!("blob:{b:?}"),
    }
}
fn tag_r(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => format!("int:{n}"),
        rusqlite::types::Value::Real(f) => format!("real:{f}"),
        rusqlite::types::Value::Text(s) => format!("text:{s}"),
        rusqlite::types::Value::Blob(b) => format!("blob:{b:?}"),
    }
}

async fn fval(conn: &Connection, sql: &str) -> String {
    let rows = conn
        .query(sql)
        .await
        .unwrap_or_else(|e| panic!("frank `{sql}`: {e:?}"));
    assert_eq!(rows.len(), 1, "frank `{sql}` returned {} rows", rows.len());
    tag_f(&rows[0].values()[0])
}
fn rval(conn: &rusqlite::Connection, sql: &str) -> String {
    conn.query_row(sql, [], |row| {
        Ok(tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(0)))
    })
    .unwrap_or_else(|e| panic!("rusqlite `{sql}`: {e:?}"))
}

#[test]
fn glob_like_and_comparison_match_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();

        let exprs = [
            // ── LIKE (ASCII case-insensitive, % and _ wildcards) ────────────
            "SELECT 'Hello' LIKE 'hello'",
            "SELECT 'Hello' LIKE 'h_llo'",
            "SELECT 'Hello' LIKE 'h%o'",
            "SELECT 'HELLO' LIKE '%ell%'",
            "SELECT 'abc' LIKE 'ABC'",
            "SELECT 'ÄBC' LIKE 'äbc'", // non-ASCII: LIKE folds ASCII only
            "SELECT '100%' LIKE '100\\%' ESCAPE '\\'",
            "SELECT 'a_b' LIKE 'a\\_b' ESCAPE '\\'",
            "SELECT 'axb' LIKE 'a\\_b' ESCAPE '\\'",
            "SELECT '' LIKE '%'",
            "SELECT '' LIKE '_'",
            "SELECT 'x' LIKE ''",
            "SELECT NULL LIKE 'a'",
            "SELECT 'a' LIKE NULL",
            // ── GLOB (case-sensitive, [] classes, * and ?) ──────────────────
            "SELECT 'Hello' GLOB 'hello'",
            "SELECT 'Hello' GLOB 'H*o'",
            "SELECT 'Hello' GLOB 'H?llo'",
            "SELECT 'abc' GLOB 'a[b-d]c'",
            "SELECT 'aXc' GLOB 'a[^b-d]c'",
            "SELECT 'a]c' GLOB 'a[]]c'",
            "SELECT 'a*c' GLOB 'a[*]c'",
            "SELECT '5' GLOB '[0-9]'",
            // ── comparison / affinity / 3-valued logic ──────────────────────
            "SELECT 1 = '1'",
            "SELECT '1' = 1",
            "SELECT 1 = 1.0",
            "SELECT 'abc' < 'abd'",
            "SELECT 'Z' < 'a'",
            "SELECT 2 BETWEEN 1 AND 3",
            "SELECT 'b' BETWEEN 'a' AND 'c'",
            "SELECT 5 IN (1,2,5)",
            "SELECT '5' IN (1,2,5)",
            "SELECT NULL IN (1,2)",
            "SELECT 1 IN (NULL,2)",
            "SELECT NULL = NULL",
            "SELECT NULL IS NULL",
            "SELECT NULL IS NOT NULL",
            "SELECT (NULL AND 0),(NULL OR 1),(NULL AND 1)",
            "SELECT 2 < 10, '2' < '10', '2' < 10",
            "SELECT CASE WHEN NULL THEN 'a' ELSE 'b' END",
            "SELECT max(1,'2',3), min(1,'2',3)",
        ];

        let mut diffs = Vec::new();
        for e in exprs {
            let fv = fval(&f, e).await;
            let rv = rval(&r, e);
            if fv != rv {
                diffs.push(format!("  `{e}`\n     frank= {fv}\n     stock= {rv}"));
            }
        }
        assert!(
            diffs.is_empty(),
            "{} glob/like/comparison divergence(s) vs rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}
