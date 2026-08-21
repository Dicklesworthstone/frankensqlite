//! Differential oracle: generated columns (STORED/VIRTUAL) + deterministic
//! DEFAULT expressions vs rusqlite (bundled SQLite 3.53). A probe sweep found
//! this surface stock-correct across 13 cases (computed-column values,
//! typeof/affinity, gencol in WHERE / feeding an aggregate / CASE body, and
//! DEFAULT arithmetic/function/override/explicit-NULL/unary forms). This keeper
//! locks that parity in so a regression is caught.
//!
//! Only deterministic DEFAULTs are asserted here — no now()/random()/CURRENT_*.

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

/// Set up identical schema on both engines, run one SELECT, assert agreement.
async fn agree(setup: &[&str], sql: &str, msg: &str) {
    let f = Connection::open(":memory:").await.unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    for s in setup {
        let _ = f.execute(s).await;
        r.execute(s, []).unwrap();
    }
    let fr = fq(&f, sql).await;
    let rr = rq(&r, sql);
    assert_eq!(fr, rr, "{msg}\n  sql   ={sql}\n  frank ={fr:?}\n  sqlite={rr:?}");
}

// ─────────────────────────── generated columns ───────────────────────────

#[test]
fn gencol_stored_and_virtual() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &["CREATE TABLE t(a INT, b INT AS (a*2) STORED, c INT AS (a+b) VIRTUAL)",
              "INSERT INTO t(a) VALUES (3),(5)"],
            "SELECT a, b, c FROM t ORDER BY a",
            "STORED and VIRTUAL generated columns must compute like stock",
        ).await;
    });
}

#[test]
fn gencol_text_concat() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &["CREATE TABLE t(x TEXT, y TEXT AS (x || '!') VIRTUAL)",
              "INSERT INTO t(x) VALUES ('hi'),('bye')"],
            "SELECT x, y FROM t ORDER BY x",
            "text-concat generated column",
        ).await;
    });
}

#[test]
fn gencol_typeof_and_affinity() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &["CREATE TABLE t(a INT, b TEXT AS (a) STORED, c REAL AS (a) VIRTUAL)",
              "INSERT INTO t(a) VALUES (7)"],
            "SELECT typeof(b), typeof(c), b, c FROM t",
            "generated-column declared-type affinity must coerce like stock",
        ).await;
    });
}

#[test]
fn gencol_in_where() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &["CREATE TABLE t(a INT, doubled INT AS (a*2) VIRTUAL)",
              "INSERT INTO t(a) VALUES (1),(2),(3),(4)"],
            "SELECT a FROM t WHERE doubled > 4 ORDER BY a",
            "generated column usable in WHERE",
        ).await;
    });
}

#[test]
fn gencol_multi_column_expr() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &["CREATE TABLE t(w INT, h INT, area INT AS (w*h) STORED)",
              "INSERT INTO t(w,h) VALUES (2,3),(4,5)"],
            "SELECT w, h, area FROM t ORDER BY w",
            "generated column over multiple columns",
        ).await;
    });
}

#[test]
fn gencol_feeding_aggregate() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &["CREATE TABLE t(a INT, sq INT AS (a*a) VIRTUAL)",
              "INSERT INTO t(a) VALUES (1),(2),(3)"],
            "SELECT sum(sq), max(sq) FROM t",
            "generated column feeding an aggregate",
        ).await;
    });
}

#[test]
fn gencol_case_body() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &["CREATE TABLE t(a INT, sign TEXT AS (CASE WHEN a<0 THEN 'neg' WHEN a=0 THEN 'zero' ELSE 'pos' END) VIRTUAL)",
              "INSERT INTO t(a) VALUES (-2),(0),(5)"],
            "SELECT a, sign FROM t ORDER BY a",
            "CASE expression inside a generated column",
        ).await;
    });
}

// ─────────────────────── deterministic DEFAULT exprs ──────────────────────

#[test]
fn default_arithmetic_expr() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &["CREATE TABLE t(id INT, n INT DEFAULT (1+2*3))",
              "INSERT INTO t(id) VALUES (1)"],
            "SELECT id, n FROM t",
            "DEFAULT (arithmetic expression)",
        ).await;
    });
}

#[test]
fn default_function_expr() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &["CREATE TABLE t(id INT, v INT DEFAULT (abs(-9)))",
              "INSERT INTO t(id) VALUES (1)"],
            "SELECT id, v FROM t",
            "DEFAULT (function call)",
        ).await;
    });
}

#[test]
fn default_literals() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &["CREATE TABLE t(id INT, s TEXT DEFAULT 'hello', k INT DEFAULT 42)",
              "INSERT INTO t(id) VALUES (1)"],
            "SELECT id, s, k FROM t",
            "DEFAULT literals (string + integer)",
        ).await;
    });
}

#[test]
fn default_overridden_by_explicit_value() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &["CREATE TABLE t(id INT, n INT DEFAULT (100))",
              "INSERT INTO t(id, n) VALUES (1, 7)", "INSERT INTO t(id) VALUES (2)"],
            "SELECT id, n FROM t ORDER BY id",
            "explicit value overrides DEFAULT; omitted uses DEFAULT",
        ).await;
    });
}

#[test]
fn default_explicit_null_keeps_null() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &["CREATE TABLE t(id INT, n INT DEFAULT (5))",
              "INSERT INTO t(id, n) VALUES (1, NULL)", "INSERT INTO t(id) VALUES (2)"],
            "SELECT id, n FROM t ORDER BY id",
            "explicit NULL stays NULL; omitted column uses DEFAULT",
        ).await;
    });
}

#[test]
fn default_unary_and_concat() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &["CREATE TABLE t(id INT, a INT DEFAULT (-5), b TEXT DEFAULT ('x' || 'y'))",
              "INSERT INTO t(id) VALUES (1)"],
            "SELECT id, a, b FROM t",
            "DEFAULT with unary minus and string concatenation",
        ).await;
    });
}
