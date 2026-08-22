#![recursion_limit = "512"]

//! Subquery-edge leaf-hunt (pane af49, 2026-08-21): frank vs rusqlite over
//! scalar-subquery cardinality (multi-row -> first, no-row -> NULL), correlated
//! subqueries in the SELECT list / WHERE / HAVING, EXISTS/NOT EXISTS
//! correlation, nested subqueries, and subqueries with aggregates/GROUP BY.
//! Result sets compared. Pass = coverage keeper; a mismatch is a leaf.

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

async fn fq(conn: &Connection, sql: &str) -> Vec<Vec<String>> {
    match conn.query(sql).await {
        Ok(rows) => rows
            .iter()
            .map(|r| r.values().iter().map(tag_f).collect())
            .collect(),
        Err(_) => vec![vec!["ERR".to_owned()]],
    }
}
fn rq(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let Ok(mut st) = conn.prepare(sql) else {
        return vec![vec!["ERR".to_owned()]];
    };
    let n = st.column_count();
    match st.query_map([], |row| {
        Ok((0..n)
            .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
            .collect::<Vec<_>>())
    }) {
        Ok(rows) => rows
            .collect::<Result<Vec<_>, _>>()
            .unwrap_or_else(|_| vec![vec!["ERR".to_owned()]]),
        Err(_) => vec![vec!["ERR".to_owned()]],
    }
}

#[test]
fn subquery_edges_match_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for s in [
            "CREATE TABLE t(a INTEGER, g TEXT, v INTEGER)",
            "INSERT INTO t VALUES (1,'x',10),(2,'x',20),(3,'y',30),(4,'y',NULL),(5,'z',5)",
        ] {
            f.execute(s).await.unwrap();
            r.execute(s, []).unwrap();
        }

        let queries = [
            // scalar subquery cardinality: multi-row -> first row's value
            "SELECT (SELECT v FROM t ORDER BY a)",
            "SELECT (SELECT v FROM t WHERE a=99)",
            "SELECT (SELECT v FROM t WHERE 0)",
            "SELECT (SELECT count(*) FROM t), (SELECT sum(v) FROM t)",
            // correlated in SELECT list
            "SELECT a, (SELECT count(*) FROM t t2 WHERE t2.v > t.v) AS gt FROM t ORDER BY a",
            "SELECT a, (SELECT g FROM t t2 WHERE t2.a = t.a+1) FROM t ORDER BY a",
            // correlated in WHERE
            "SELECT a FROM t WHERE v = (SELECT max(v) FROM t t2 WHERE t2.g = t.g) ORDER BY a",
            "SELECT a FROM t WHERE v > (SELECT avg(v) FROM t) ORDER BY a",
            // EXISTS / NOT EXISTS correlated
            "SELECT g FROM t WHERE EXISTS (SELECT 1 FROM t t2 WHERE t2.g=t.g AND t2.v IS NULL) ORDER BY g",
            "SELECT DISTINCT g FROM t WHERE NOT EXISTS (SELECT 1 FROM t t2 WHERE t2.g=t.g AND t2.v IS NULL) ORDER BY g",
            // IN with correlated subquery
            "SELECT a FROM t WHERE a IN (SELECT a FROM t t2 WHERE t2.g = t.g AND t2.v IS NOT NULL) ORDER BY a",
            // GROUP BY + HAVING with a subquery
            "SELECT g, count(*) FROM t GROUP BY g HAVING count(*) > (SELECT count(*) FROM t WHERE g='z') ORDER BY g",
            "SELECT g, sum(v) FROM t GROUP BY g HAVING sum(v) > (SELECT avg(v) FROM t) ORDER BY g",
            // nested scalar subqueries
            "SELECT (SELECT (SELECT max(v) FROM t) + 1)",
            "SELECT a FROM t WHERE a = (SELECT min(a) FROM t WHERE v IN (SELECT v FROM t WHERE g='x')) ORDER BY a",
            // scalar subquery in an expression
            "SELECT a, v - (SELECT min(v) FROM t) AS delta FROM t WHERE v IS NOT NULL ORDER BY a",
        ];

        let mut diffs = Vec::new();
        for q in queries {
            let fr = fq(&f, q).await;
            let rr = rq(&r, q);
            if fr != rr {
                diffs.push(format!("  `{q}`\n     frank= {fr:?}\n     stock= {rr:?}"));
            }
        }
        assert!(
            diffs.is_empty(),
            "{} subquery-edge divergence(s) vs rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}
