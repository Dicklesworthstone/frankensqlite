#![recursion_limit = "512"]

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
            &[
                "CREATE TABLE t(x INT)",
                "CREATE TABLE u(y INT)",
                "INSERT INTO t VALUES (1),(2),(3)",
                "INSERT INTO u VALUES (2),(NULL)",
            ],
            "SELECT x FROM t WHERE x NOT IN (SELECT y FROM u) ORDER BY x",
            "NOT IN a subquery containing NULL yields no rows",
        )
        .await;
    });
}

#[test]
fn scalar_subquery_empty_and_multi() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &["CREATE TABLE t(x INT)", "INSERT INTO t VALUES (1)"],
            "SELECT (SELECT x FROM t WHERE x = 99)",
            "scalar subquery over no rows is NULL",
        )
        .await;
        agree(
            &["CREATE TABLE t(x INT)", "INSERT INTO t VALUES (5),(6),(7)"],
            "SELECT (SELECT x FROM t ORDER BY x)",
            "scalar subquery yields its first row when several match",
        )
        .await;
    });
}

/// The same comparison on a file-backed database, whose subqueries run
/// through the VDBE rather than the in-memory evaluator.
async fn agree_file_backed(setup: &[&str], sql: &str, msg: &str) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("oracle.db");
    let f = Connection::open(path.to_str().unwrap()).await.unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    for s in setup {
        let _ = f.execute(s).await;
        let _ = r.execute_batch(s);
    }
    let fr = fq(&f, sql).await;
    let rr = rq(&r, sql);
    assert_eq!(
        fr, rr,
        "{msg} (file-backed)\n  sql   ={sql}\n  frank ={fr:?}\n  sqlite={rr:?}"
    );
}

/// u(id,n) is read through a FROM-clause subquery inside a scalar subquery
/// that runs once per row of t(id,a).
const DERIVED: &[&str] = &[
    "CREATE TABLE u(id INTEGER PRIMARY KEY, n INTEGER)",
    "INSERT INTO u VALUES (1,10),(2,20),(3,30),(4,40)",
    "CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER)",
    "INSERT INTO t VALUES (1,15),(2,25),(3,35)",
];

/// bd-j9tg3: a bare outer column under a derived table resolves outward, the
/// derived table's own columns resolve inward, and correlation inside the
/// derived table binds the outer row. Every derived table used to be an
/// opaque scope, and each of these failed with "no such column: a".
#[test]
fn bare_outer_reference_under_a_derived_table() {
    let cases: &[(&str, &str)] = &[
        (
            "SELECT a, (SELECT count(*) FROM (SELECT n FROM u) WHERE n > a) FROM t ORDER BY id",
            "derived-table column beside a bare outer column",
        ),
        (
            "SELECT a, (SELECT count(*) FROM (SELECT n AS m FROM u) WHERE m > a) FROM t ORDER BY id",
            "aliased derived-table column",
        ),
        (
            "SELECT a, (SELECT count(*) FROM (SELECT * FROM u) WHERE n > a) FROM t ORDER BY id",
            "derived table over *",
        ),
        (
            "SELECT a, (SELECT count(*) FROM (SELECT u.* FROM u) WHERE n > a) FROM t ORDER BY id",
            "derived table over u.*",
        ),
        (
            "SELECT a, (SELECT count(*) FROM (VALUES (10),(20),(30)) WHERE column1 > a) FROM t ORDER BY id",
            "derived VALUES table",
        ),
        (
            "SELECT a, (SELECT count(*) FROM (SELECT n, n + 1 FROM u) WHERE n > a) FROM t ORDER BY id",
            "derived table with an unaliased expression column",
        ),
        (
            "SELECT a, (SELECT sum(c) FROM (SELECT n, count(*) AS c FROM u GROUP BY n) WHERE n > a) FROM t ORDER BY id",
            "grouped derived table",
        ),
        (
            "SELECT a, (SELECT count(*) FROM (SELECT n FROM u UNION ALL SELECT n FROM u) WHERE n > a) FROM t ORDER BY id",
            "compound derived table (names from its first arm)",
        ),
        (
            "SELECT a, (SELECT count(*) FROM (SELECT n FROM u) AS d JOIN u AS e ON e.n = d.n WHERE d.n > a) FROM t ORDER BY id",
            "derived table joined to a base table",
        ),
        (
            "SELECT a, EXISTS (SELECT 1 FROM (SELECT id, n FROM u) WHERE n = a + 5) FROM t ORDER BY id",
            "EXISTS over a derived table",
        ),
        // Negative: the derived table's `id` shadows t.id, so the subquery is
        // the same for every row (stock 2, 2, 2). An inventory that missed
        // `id` would bind t.id instead.
        (
            "SELECT id, (SELECT count(*) FROM (SELECT id FROM u) WHERE id > 2) FROM t ORDER BY id",
            "derived-table column shadows the outer column of the same name",
        ),
        (
            "SELECT a, (SELECT count(*) FROM (SELECT n FROM u WHERE n > a)) FROM t ORDER BY id",
            "correlation inside the derived table",
        ),
        (
            "SELECT a, (SELECT count(*) FROM (SELECT n FROM u WHERE n > a) WHERE n < a + 20) FROM t ORDER BY id",
            "correlation inside the derived table and in the scope that reads it",
        ),
    ];
    asupersync::test_utils::run_test(|| async {
        for (sql, msg) in cases {
            agree(DERIVED, sql, msg).await;
            agree_file_backed(DERIVED, sql, msg).await;
        }
    });
}

/// bd-j9tg3: stock resolves `rowid` inside a derived table's scope locally
/// (the derived table's own NULL rowid), not to t's rowid. fsqlite has no
/// derived-table rowid yet and refuses with "no such column", which is the
/// fail-closed answer. Binding t.rowid would give 0, 4, 4.
#[test]
fn rowid_under_a_derived_table_never_binds_the_outer_rowid() {
    asupersync::test_utils::run_test(|| async {
        let sql = "SELECT a, (SELECT count(*) FROM (SELECT * FROM u) WHERE rowid > 1) FROM t ORDER BY id";
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for s in DERIVED {
            f.execute(s).await.unwrap();
            r.execute_batch(s).unwrap();
        }
        let fr = fq(&f, sql).await;
        let rr = rq(&r, sql);
        let refused = fr.len() == 1 && fr[0].len() == 1 && fr[0][0].starts_with("<ERR");
        assert!(
            fr == rr || refused,
            "rowid under a derived table must resolve locally or refuse\n  frank ={fr:?}\n  sqlite={rr:?}"
        );
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
