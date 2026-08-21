//! Differential oracle: VALUES as a row source, jsonb round-trip, and remaining
//! scalar/conditional forms vs rusqlite (bundled SQLite 3.53). A probe sweep
//! found this surface stock-correct across 15 cases; this keeper locks it in.
//!
//! Covers VALUES standalone / in a derived table / in a compound (UNION ALL) /
//! in a CTE / joined to a table; jsonb round-trip and extract; coalesce /
//! ifnull / nullif / iif; BETWEEN; IN with mixed types + subquery + NULL;
//! searched vs simple CASE; typeof across storage classes; scalar min/max.

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

#[test]
fn values_as_row_source() {
    asupersync::test_utils::run_test(|| async {
        agree(&[], "VALUES (1,'a'),(2,'b'),(3,'c')", "VALUES standalone").await;
        agree(&[], "SELECT column1, column2 FROM (VALUES (1,'x'),(2,'y')) ORDER BY column1", "VALUES in a derived table").await;
        agree(&[], "SELECT 1 AS n UNION ALL VALUES (2),(3)", "VALUES in a compound").await;
        agree(&[], "WITH v(a,b) AS (VALUES (1,10),(2,20)) SELECT a, b FROM v ORDER BY a", "VALUES in a CTE").await;
        agree(
            &["CREATE TABLE t(id INT, name TEXT)", "INSERT INTO t VALUES (1,'one'),(2,'two')"],
            "WITH v(id) AS (VALUES (1),(2)) SELECT t.name FROM t JOIN v ON v.id=t.id ORDER BY t.name",
            "VALUES CTE joined to a table",
        ).await;
    });
}

#[test]
fn jsonb_roundtrip() {
    asupersync::test_utils::run_test(|| async {
        agree(&[], "SELECT json(jsonb('{\"a\":1,\"b\":[2,3]}'))", "jsonb round-trip via json()").await;
        agree(&[], "SELECT json_extract(jsonb('{\"a\":{\"b\":5}}'), '$.a.b')", "json_extract over jsonb").await;
    });
}

#[test]
fn conditional_scalars() {
    asupersync::test_utils::run_test(|| async {
        agree(&[], "SELECT coalesce(NULL, NULL, 3, 4), coalesce(NULL, NULL)", "coalesce").await;
        agree(&[], "SELECT ifnull(NULL, 'x'), ifnull(5, 'x'), nullif(1,1), nullif(1,2)", "ifnull/nullif").await;
        agree(&[], "SELECT iif(1>0, 'yes', 'no'), iif(0, 'yes', 'no'), iif(NULL, 'y', 'n')", "iif").await;
        agree(&[], "SELECT CASE WHEN 1>2 THEN 'a' WHEN 2>1 THEN 'b' ELSE 'c' END, CASE 3 WHEN 1 THEN 'x' WHEN 3 THEN 'y' ELSE 'z' END", "searched vs simple CASE").await;
    });
}

#[test]
fn between_in_typeof_minmax() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &["CREATE TABLE t(x INT)", "INSERT INTO t VALUES (1),(5),(10),(15)"],
            "SELECT x FROM t WHERE x BETWEEN 5 AND 10 ORDER BY x", "BETWEEN",
        ).await;
        agree(&[], "SELECT 1 IN (1,2,3), 'a' IN ('x','a'), 5 IN (SELECT 5), NULL IN (1,2)", "IN mixed/subquery/NULL").await;
        agree(&[], "SELECT typeof(1), typeof(1.5), typeof('s'), typeof(NULL), typeof(X'00')", "typeof across storage classes").await;
        agree(&[], "SELECT min(3,1,2), max(3,1,2), min('b','a','c')", "scalar (multi-arg) min/max").await;
    });
}
