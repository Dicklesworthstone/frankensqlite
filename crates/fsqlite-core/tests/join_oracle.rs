//! Differential oracle: JOIN semantics vs rusqlite (bundled SQLite 3.53). A
//! probe sweep found this surface stock-correct across 14 cases; this keeper
//! locks it in.
//!
//! Covers INNER (comma and explicit), LEFT OUTER with NULL padding, the
//! ON-clause vs WHERE-clause filter distinction (a right-table predicate in ON
//! preserves unmatched left rows; in WHERE it drops them), CROSS/cartesian,
//! self-join, a three-way chain, USING (single coalesced join column), NATURAL
//! and NATURAL LEFT (join on all common columns), LEFT JOIN feeding a per-group
//! count, and a join against a derived table.

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

/// a(id,name), b(id,aid,val): a=3 has no b; b id=13 references a missing a (99).
const D: &[&str] = &[
    "CREATE TABLE a(id INT, name TEXT)",
    "CREATE TABLE b(id INT, aid INT, val INT)",
    "INSERT INTO a VALUES (1,'x'),(2,'y'),(3,'z')",
    "INSERT INTO b VALUES (10,1,100),(11,1,200),(12,2,300),(13,99,400)",
];

#[test]
fn inner_joins() {
    asupersync::test_utils::run_test(|| async {
        agree(
            D,
            "SELECT a.name, b.val FROM a JOIN b ON a.id = b.aid ORDER BY a.name, b.val",
            "explicit INNER JOIN",
        )
        .await;
        agree(
            D,
            "SELECT a.name, b.val FROM a, b WHERE a.id = b.aid ORDER BY a.name, b.val",
            "comma INNER JOIN",
        )
        .await;
        agree(
            &[
                "CREATE TABLE emp(id INT, mgr INT, name TEXT)",
                "INSERT INTO emp VALUES (1,NULL,'boss'),(2,1,'a'),(3,1,'b')",
            ],
            "SELECT e.name, m.name FROM emp e JOIN emp m ON e.mgr = m.id ORDER BY e.name",
            "self-join",
        )
        .await;
        agree(
            &["CREATE TABLE t1(id INT)","CREATE TABLE t2(id INT, t1 INT)","CREATE TABLE t3(id INT, t2 INT)",
              "INSERT INTO t1 VALUES (1)","INSERT INTO t2 VALUES (10,1)","INSERT INTO t3 VALUES (100,10),(101,10)"],
            "SELECT t1.id, t2.id, t3.id FROM t1 JOIN t2 ON t2.t1=t1.id JOIN t3 ON t3.t2=t2.id ORDER BY t3.id",
            "three-way join",
        ).await;
    });
}

#[test]
fn left_joins() {
    asupersync::test_utils::run_test(|| async {
        agree(
            D,
            "SELECT a.name, b.val FROM a LEFT JOIN b ON a.id = b.aid ORDER BY a.name, b.val",
            "LEFT JOIN NULL padding",
        )
        .await;
        agree(
            D,
            "SELECT a.name FROM a LEFT JOIN b ON a.id = b.aid WHERE b.id IS NULL ORDER BY a.name",
            "anti-join",
        )
        .await;
        agree(D, "SELECT a.name, b.val FROM a LEFT JOIN b ON a.id = b.aid AND b.val > 150 ORDER BY a.name, b.val", "right-table predicate in ON preserves unmatched rows").await;
        agree(D, "SELECT a.name, b.val FROM a LEFT JOIN b ON a.id = b.aid WHERE b.val > 150 OR b.val IS NULL ORDER BY a.name, b.val", "right-table predicate in WHERE").await;
        agree(D, "SELECT a.name, count(b.id) FROM a LEFT JOIN b ON a.id = b.aid GROUP BY a.id ORDER BY a.name", "LEFT JOIN feeding a per-group count").await;
    });
}

#[test]
fn cross_using_natural_derived() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &[
                "CREATE TABLE x(n INT)",
                "CREATE TABLE y(c TEXT)",
                "INSERT INTO x VALUES (1),(2)",
                "INSERT INTO y VALUES ('a'),('b')",
            ],
            "SELECT n, c FROM x CROSS JOIN y ORDER BY n, c",
            "CROSS JOIN cartesian product",
        )
        .await;
        agree(
            &[
                "CREATE TABLE p(id INT, v INT)",
                "CREATE TABLE q(id INT, w INT)",
                "INSERT INTO p VALUES (1,10),(2,20)",
                "INSERT INTO q VALUES (1,100),(3,300)",
            ],
            "SELECT id, v, w FROM p JOIN q USING (id) ORDER BY id",
            "USING coalesces the join column",
        )
        .await;
        agree(
            &[
                "CREATE TABLE p(id INT, v INT)",
                "CREATE TABLE q(id INT, w INT)",
                "INSERT INTO p VALUES (1,10),(2,20)",
                "INSERT INTO q VALUES (1,100),(2,200)",
            ],
            "SELECT id, v, w FROM p NATURAL JOIN q ORDER BY id",
            "NATURAL JOIN on all common columns",
        )
        .await;
        agree(
            &[
                "CREATE TABLE p(id INT, v INT)",
                "CREATE TABLE q(id INT, w INT)",
                "INSERT INTO p VALUES (1,10),(2,20),(3,30)",
                "INSERT INTO q VALUES (1,100),(2,200)",
            ],
            "SELECT id, v, w FROM p NATURAL LEFT JOIN q ORDER BY id",
            "NATURAL LEFT JOIN",
        )
        .await;
        agree(D, "SELECT a.name, s.total FROM a JOIN (SELECT aid, sum(val) total FROM b GROUP BY aid) s ON s.aid = a.id ORDER BY a.name", "join against a derived table").await;
    });
}
