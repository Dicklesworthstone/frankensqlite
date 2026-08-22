//! Differential oracle: LIKE / GLOB pattern matching vs rusqlite (bundled
//! SQLite 3.53). A probe sweep found this surface stock-correct across 20 cases;
//! this keeper locks it in.
//!
//! Key semantics asserted: LIKE is ASCII case-insensitive and uses `%`/`_`;
//! GLOB is case-sensitive and uses `*`/`?`/`[set]`/`[a-z]`/`[^set]`; the ESCAPE
//! clause makes `%`/`_` literal; a NULL operand yields NULL (row excluded);
//! `%` matches the empty string.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn tag_f(v: &SqliteValue) -> String {
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
fn tag_r(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => n.to_string(),
        rusqlite::types::Value::Real(f) => format!("{f}"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => format!(
            "X'{}'",
            b.iter().map(|x| format!("{x:02X}")).collect::<String>()
        ),
    }
}

async fn fq(f: &Connection, sql: &str) -> Vec<Vec<String>> {
    match f.query_with_params(sql, &[]).await {
        Ok(rows) => rows
            .iter()
            .map(|r| r.values().iter().map(tag_f).collect())
            .collect(),
        Err(e) => vec![vec![format!("<ERR {e:?}>")]],
    }
}
fn rq(r: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut st = match r.prepare(sql) {
        Ok(st) => st,
        Err(e) => return vec![vec![format!("<ERR {e}>")]],
    };
    let n = st.column_count();
    st.query_map([], |row| {
        Ok((0..n)
            .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
            .collect())
    })
    .unwrap()
    .collect::<Result<Vec<_>, _>>()
    .unwrap()
}

async fn agree(setup: &[&str], sql: &str, msg: &str) {
    let f = Connection::open(":memory:").await.unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    for s in setup {
        let _ = f.execute(s).await;
        let _ = r.execute_batch(s);
    }
    let fr = fq(&f, sql).await;
    let rr = rq(&r, sql);
    assert_eq!(
        fr, rr,
        "{msg}\n  sql   ={sql}\n  frank ={fr:?}\n  sqlite={rr:?}"
    );
}

/// Shared corpus: mixed case, wildcard-literal, dot, empty, and a "100%" row.
const W: &[&str] = &[
    "CREATE TABLE t(s TEXT)",
    "INSERT INTO t VALUES ('apple'),('Apple'),('APRICOT'),('banana'),('grape'),('a_b'),('a%b'),('a.b'),(''),('100%')",
];

#[test]
fn like_wildcards() {
    asupersync::test_utils::run_test(|| async {
        agree(
            W,
            "SELECT s FROM t WHERE s LIKE 'a%' ORDER BY s",
            "LIKE % prefix",
        )
        .await;
        agree(
            W,
            "SELECT s FROM t WHERE s LIKE 'a_b' ORDER BY s",
            "LIKE _ single char",
        )
        .await;
    });
}

#[test]
fn like_case_insensitive() {
    asupersync::test_utils::run_test(|| async {
        agree(
            W,
            "SELECT s FROM t WHERE s LIKE 'apple' ORDER BY s",
            "LIKE is ASCII case-insensitive",
        )
        .await;
        agree(
            W,
            "SELECT s FROM t WHERE s LIKE 'ap%' ORDER BY s",
            "LIKE ci prefix",
        )
        .await;
    });
}

#[test]
fn not_like() {
    asupersync::test_utils::run_test(|| async {
        agree(
            W,
            "SELECT s FROM t WHERE s NOT LIKE 'a%' ORDER BY s",
            "NOT LIKE",
        )
        .await;
    });
}

#[test]
fn like_escape_clause() {
    asupersync::test_utils::run_test(|| async {
        agree(
            W,
            "SELECT s FROM t WHERE s LIKE '100\\%' ESCAPE '\\' ORDER BY s",
            "ESCAPE literal % suffix",
        )
        .await;
        agree(
            W,
            "SELECT s FROM t WHERE s LIKE 'a\\_b' ESCAPE '\\' ORDER BY s",
            "ESCAPE literal _",
        )
        .await;
        agree(
            W,
            "SELECT s FROM t WHERE s LIKE 'a\\%b' ESCAPE '\\' ORDER BY s",
            "ESCAPE literal % mid",
        )
        .await;
    });
}

#[test]
fn like_null_and_empty() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &[
                "CREATE TABLE t(s TEXT)",
                "INSERT INTO t VALUES ('x'),(NULL)",
            ],
            "SELECT s FROM t WHERE s LIKE 'x'",
            "LIKE against NULL is NULL (row excluded)",
        )
        .await;
        agree(
            W,
            "SELECT s FROM t WHERE s LIKE '' ORDER BY s",
            "empty pattern matches only empty string",
        )
        .await;
        agree(
            W,
            "SELECT count(*) FROM t WHERE s LIKE '%'",
            "% matches every row",
        )
        .await;
    });
}

#[test]
fn glob_case_sensitive_wildcards() {
    asupersync::test_utils::run_test(|| async {
        agree(
            W,
            "SELECT s FROM t WHERE s GLOB 'a*' ORDER BY s",
            "GLOB * (case-sensitive)",
        )
        .await;
        agree(
            W,
            "SELECT s FROM t WHERE s GLOB 'A*' ORDER BY s",
            "GLOB is case-sensitive",
        )
        .await;
        agree(
            W,
            "SELECT s FROM t WHERE s GLOB 'a?b' ORDER BY s",
            "GLOB ? single char",
        )
        .await;
    });
}

#[test]
fn glob_character_classes() {
    asupersync::test_utils::run_test(|| async {
        agree(
            W,
            "SELECT s FROM t WHERE s GLOB '[ab]*' ORDER BY s",
            "GLOB [set]",
        )
        .await;
        agree(
            W,
            "SELECT s FROM t WHERE s GLOB '[a-c]*' ORDER BY s",
            "GLOB [range]",
        )
        .await;
        agree(
            W,
            "SELECT s FROM t WHERE s GLOB '[^a]*' ORDER BY s",
            "GLOB [^negated]",
        )
        .await;
        agree(
            W,
            "SELECT s FROM t WHERE s GLOB '*[%]*' ORDER BY s",
            "GLOB [%] literal percent",
        )
        .await;
    });
}

#[test]
fn not_glob() {
    asupersync::test_utils::run_test(|| async {
        agree(
            W,
            "SELECT s FROM t WHERE s NOT GLOB 'a*' ORDER BY s",
            "NOT GLOB",
        )
        .await;
    });
}

#[test]
fn like_underscore_matches_special_char() {
    asupersync::test_utils::run_test(|| async {
        agree(
            W,
            "SELECT s FROM t WHERE s LIKE 'a_b' AND s GLOB 'a[._%]b' ORDER BY s",
            "_ matches any single char incl. special",
        )
        .await;
    });
}
