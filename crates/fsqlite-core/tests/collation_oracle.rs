//! Differential oracle: collation (BINARY / NOCASE / RTRIM) vs rusqlite
//! (bundled SQLite 3.53). A probe sweep found this surface stock-correct across
//! 15 cases; this keeper locks it in.
//!
//! Covers a NOCASE-declared column driving equality, ORDER BY, DISTINCT,
//! GROUP BY, IN, `<`, and min/max; explicit `COLLATE` on a BINARY column and in
//! ORDER BY/GROUP BY; explicit-COLLATE precedence over the column collation;
//! BINARY (default) uppercase-before-lowercase ordering; and RTRIM
//! (trailing-space-insensitive) equality and ordering.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!("X'{}'", b.iter().map(|x| format!("{x:02X}")).collect::<String>()),
    }
}
fn tag_r(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => n.to_string(),
        rusqlite::types::Value::Real(f) => format!("{f}"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => format!("X'{}'", b.iter().map(|x| format!("{x:02X}")).collect::<String>()),
    }
}

async fn fq(f: &Connection, sql: &str) -> Vec<Vec<String>> {
    match f.query_with_params(sql, &[]).await {
        Ok(rows) => rows.iter().map(|r| r.values().iter().map(tag_f).collect()).collect(),
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
        Ok((0..n).map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i))).collect())
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
    assert_eq!(fr, rr, "{msg}\n  sql   ={sql}\n  frank ={fr:?}\n  sqlite={rr:?}");
}

/// A NOCASE-declared column with case-variant rows.
const N: &[&str] = &[
    "CREATE TABLE t(s TEXT COLLATE NOCASE)",
    "INSERT INTO t VALUES ('Apple'),('apple'),('BANANA'),('banana'),('Cherry')",
];

#[test]
fn nocase_column() {
    asupersync::test_utils::run_test(|| async {
        agree(N, "SELECT s FROM t WHERE s = 'apple' ORDER BY rowid", "NOCASE equality").await;
        agree(N, "SELECT s FROM t ORDER BY s", "NOCASE ORDER BY").await;
        agree(N, "SELECT DISTINCT s FROM t ORDER BY s", "NOCASE DISTINCT").await;
        agree(N, "SELECT count(*) FROM t GROUP BY s ORDER BY s", "NOCASE GROUP BY").await;
        agree(N, "SELECT s FROM t WHERE s < 'c' ORDER BY s", "NOCASE < comparison").await;
        agree(N, "SELECT s FROM t WHERE s IN ('APPLE','banana') ORDER BY rowid", "NOCASE IN list").await;
        agree(N, "SELECT min(s), max(s) FROM t", "NOCASE min/max").await;
    });
}

#[test]
fn explicit_collate() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &["CREATE TABLE b(s TEXT)", "INSERT INTO b VALUES ('ABC'),('abc'),('xyz')"],
            "SELECT s FROM b WHERE s = 'abc' COLLATE NOCASE ORDER BY rowid",
            "explicit COLLATE NOCASE on a BINARY column",
        ).await;
        agree(
            &["CREATE TABLE b(s TEXT)", "INSERT INTO b VALUES ('b'),('A'),('c'),('B'),('a')"],
            "SELECT s FROM b ORDER BY s COLLATE NOCASE, s",
            "explicit COLLATE NOCASE in ORDER BY",
        ).await;
        agree(
            &["CREATE TABLE b(s TEXT)", "INSERT INTO b VALUES ('X'),('x'),('y'),('Y')"],
            "SELECT count(*) FROM b GROUP BY s COLLATE NOCASE ORDER BY s COLLATE NOCASE",
            "explicit COLLATE in GROUP BY",
        ).await;
        agree(N, "SELECT s FROM t WHERE s = 'apple' COLLATE BINARY ORDER BY rowid",
              "explicit COLLATE BINARY overrides the column's NOCASE").await;
    });
}

#[test]
fn binary_and_rtrim() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &["CREATE TABLE b(s TEXT)", "INSERT INTO b VALUES ('b'),('A'),('a'),('B')"],
            "SELECT s FROM b ORDER BY s",
            "BINARY default: uppercase sorts before lowercase",
        ).await;
        agree(
            &["CREATE TABLE b(s TEXT)", "INSERT INTO b VALUES ('Z'),('a'),('M')"],
            "SELECT s FROM b ORDER BY s COLLATE NOCASE",
            "NOCASE affects ASCII letters only",
        ).await;
        agree(
            &["CREATE TABLE t(s TEXT COLLATE RTRIM)", "INSERT INTO t VALUES ('hi'),('hi   '),('hix')"],
            "SELECT s FROM t WHERE s = 'hi' ORDER BY rowid",
            "RTRIM ignores trailing spaces in equality",
        ).await;
        agree(
            &["CREATE TABLE t(s TEXT COLLATE RTRIM)", "INSERT INTO t VALUES ('b '),('a'),('a  '),('c')"],
            "SELECT quote(s) FROM t ORDER BY s",
            "RTRIM ORDER BY treats trailing-space variants as equal",
        ).await;
    });
}
