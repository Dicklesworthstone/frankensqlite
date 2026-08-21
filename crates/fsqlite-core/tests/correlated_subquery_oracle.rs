//! Differential oracle: correlated subqueries, EXISTS, and scalar subqueries
//! vs rusqlite (bundled SQLite 3.53). A probe sweep found this surface
//! stock-correct across 14 cases; this keeper locks it in.
//!
//! Key semantics asserted: a correlated scalar subquery over no matching rows
//! yields NULL; a scalar subquery yields its first row when several match;
//! EXISTS/NOT EXISTS bind the outer row; `NOT IN (subquery containing NULL)`
//! makes the whole predicate NULL (no rows); correlation works in the SELECT
//! list, WHERE, HAVING, and nested two levels deep.

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

/// cust(id,name) + ord(id,cust,amt); cust 3 has no orders, ord 13 has NULL amt.
const D: &[&str] = &[
    "CREATE TABLE cust(id INT, name TEXT)",
    "CREATE TABLE ord(id INT, cust INT, amt INT)",
    "INSERT INTO cust VALUES (1,'a'),(2,'b'),(3,'c')",
    "INSERT INTO ord VALUES (10,1,100),(11,1,50),(12,2,200),(13,2,NULL)",
];

#[test]
fn scalar_subquery_in_select_list() {
    asupersync::test_utils::run_test(|| async {
        agree(D, "SELECT name, (SELECT count(*) FROM ord WHERE ord.cust = cust.id) AS n FROM cust ORDER BY name",
              "correlated scalar subquery (per-customer count)").await;
        agree(D, "SELECT name, (SELECT sum(amt) FROM ord WHERE ord.cust = cust.id) FROM cust ORDER BY name",
              "correlated sum yields NULL for a customer with no orders").await;
        agree(D, "SELECT name, (SELECT max(amt) FROM ord WHERE ord.cust=cust.id) mx FROM cust ORDER BY name",
              "correlated max per group").await;
        agree(D, "SELECT name, (SELECT count(*) FROM ord WHERE ord.cust=cust.id) * 10 AS score FROM cust ORDER BY name",
              "correlated scalar subquery inside an expression").await;
    });
}

#[test]
fn exists_and_not_exists() {
    asupersync::test_utils::run_test(|| async {
        agree(D, "SELECT name FROM cust WHERE EXISTS (SELECT 1 FROM ord WHERE ord.cust = cust.id) ORDER BY name",
              "EXISTS correlated").await;
        agree(D, "SELECT name FROM cust WHERE NOT EXISTS (SELECT 1 FROM ord WHERE ord.cust = cust.id) ORDER BY name",
              "NOT EXISTS correlated").await;
        agree(D, "SELECT name FROM cust WHERE EXISTS (SELECT 1 FROM ord WHERE ord.cust = cust.id AND ord.amt < 0) ORDER BY name",
              "EXISTS with an always-false correlated condition").await;
    });
}

#[test]
fn correlated_where_and_in() {
    asupersync::test_utils::run_test(|| async {
        agree(D, "SELECT DISTINCT c.name FROM cust c WHERE EXISTS (SELECT 1 FROM ord o WHERE o.cust=c.id AND o.amt > (SELECT avg(amt) FROM ord o2 WHERE o2.cust=c.id)) ORDER BY c.name",
              "correlated comparison against a per-group avg").await;
        agree(D, "SELECT name FROM cust WHERE id IN (SELECT cust FROM ord WHERE amt >= 100) ORDER BY name",
              "IN a (semi-correlated) subquery").await;
    });
}

#[test]
fn not_in_with_null_is_empty() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &["CREATE TABLE t(x INT)","CREATE TABLE u(y INT)",
              "INSERT INTO t VALUES (1),(2),(3)","INSERT INTO u VALUES (2),(NULL)"],
            "SELECT x FROM t WHERE x NOT IN (SELECT y FROM u) ORDER BY x",
            "NOT IN a subquery containing NULL yields no rows",
        ).await;
    });
}

#[test]
fn scalar_subquery_empty_and_multi() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &["CREATE TABLE t(x INT)","INSERT INTO t VALUES (1)"],
            "SELECT (SELECT x FROM t WHERE x = 99)",
            "scalar subquery over no rows is NULL",
        ).await;
        agree(
            &["CREATE TABLE t(x INT)","INSERT INTO t VALUES (5),(6),(7)"],
            "SELECT (SELECT x FROM t ORDER BY x)",
            "scalar subquery yields its first row when several match",
        ).await;
    });
}

#[test]
fn correlated_having_and_nested() {
    asupersync::test_utils::run_test(|| async {
        agree(D, "SELECT cust, count(*) c FROM ord GROUP BY cust HAVING count(*) > (SELECT count(*) FROM ord o2 WHERE o2.cust = ord.cust AND o2.amt IS NULL) ORDER BY cust",
              "correlated subquery in HAVING").await;
        agree(D, "SELECT name FROM cust c WHERE EXISTS (SELECT 1 FROM ord o WHERE o.cust=c.id AND EXISTS (SELECT 1 FROM ord o3 WHERE o3.cust=o.cust AND o3.amt > o.amt)) ORDER BY name",
              "nested two-level correlation").await;
    });
}
