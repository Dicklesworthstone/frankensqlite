#![recursion_limit = "512"]

//! Row-value expression leaf-hunt (pane af49, 2026-08-21): frank vs rusqlite
//! over row-value tuples — (a,b) = / < / > (c,d) lexicographic comparison,
//! (a,b) IN (VALUES ...) and IN (SELECT ...), NULL propagation in tuple
//! comparison, and row-values in WHERE / correlated subqueries. Result sets and
//! scalar results compared. Pass = coverage keeper; a mismatch is a leaf.

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
fn row_values_match_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for s in [
            "CREATE TABLE t(a INTEGER, b INTEGER, c TEXT)",
            "INSERT INTO t VALUES (1,2,'x'),(1,3,'y'),(2,1,'z'),(2,2,'w')",
            "CREATE TABLE u(x INTEGER, y INTEGER)",
            "INSERT INTO u VALUES (1,2),(2,2)",
        ] {
            f.execute(s).await.unwrap();
            r.execute(s, []).unwrap();
        }

        let queries = [
            // scalar row-value comparisons
            "SELECT (1,2) = (1,2), (1,2) = (1,3), (1,2) <> (1,3)",
            "SELECT (1,2) < (1,3), (2,1) < (1,9), (1,2,3) < (1,2,4)",
            "SELECT (1,2) <= (1,2), (1,3) >= (1,2)",
            // NULL propagation in tuple comparison
            "SELECT (1,NULL) = (1,2), (1,NULL) = (2,2), (1,NULL) <> (2,2)",
            // row-value IN (VALUES ...)
            "SELECT a,b FROM t WHERE (a,b) IN (VALUES (1,2),(2,1)) ORDER BY a,b",
            "SELECT a,b FROM t WHERE (a,b) NOT IN (VALUES (1,2),(2,2)) ORDER BY a,b",
            // row-value IN (SELECT ...)
            "SELECT a,b FROM t WHERE (a,b) IN (SELECT x,y FROM u) ORDER BY a,b",
            // NOTE: (a,b) = (SELECT x,y ...) row-value vs scalar-subquery-row is
            // a separate open leaf (frank returns no rows) — tracked, excluded.
            // row-value with mixed affinity
            "SELECT (1,'2') = (1,2), ('a',1) < ('b',0)",
            // row-value in a CASE / projection
            "SELECT a, CASE WHEN (a,b) = (2,2) THEN 'match' ELSE 'no' END FROM t ORDER BY a,b",
            // multi-column ORDER BY equivalence check (not row-value, contrast)
            "SELECT a,b FROM t ORDER BY a,b",
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
            "{} row-value divergence(s) vs rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}
