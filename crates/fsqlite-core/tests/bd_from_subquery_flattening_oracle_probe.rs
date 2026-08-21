#![recursion_limit = "512"]

//! FROM-subquery flattening-correctness leaf-hunt (pane af49, 2026-08-21): frank
//! vs rusqlite over subqueries in FROM whose inner ORDER BY / LIMIT / aggregate /
//! DISTINCT / GROUP BY must be honored and NOT be incorrectly flattened into the
//! outer query. Inner LIMIT with an outer WHERE (the LIMIT must apply first),
//! inner aggregate feeding an outer join, inner DISTINCT under an outer count,
//! doubly-nested subqueries, an inner ORDER BY that the outer LIMIT slices, and
//! an outer aggregate over an inner GROUP BY. Ordered result sets compared.
//! Pass = coverage keeper; a mismatch is a leaf divergence.

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
        Ok(rows) => rows.iter().map(|r| r.values().iter().map(tag_f).collect()).collect(),
        Err(_) => vec![vec!["ERR".to_owned()]],
    }
}
fn rq(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let Ok(mut st) = conn.prepare(sql) else { return vec![vec!["ERR".to_owned()]] };
    let n = st.column_count();
    match st.query_map([], |row| {
        Ok((0..n).map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i))).collect::<Vec<_>>())
    }) {
        Ok(rows) => rows.collect::<Result<Vec<_>, _>>().unwrap_or_else(|_| vec![vec!["ERR".to_owned()]]),
        Err(_) => vec![vec!["ERR".to_owned()]],
    }
}

#[test]
fn from_subquery_flattening_match_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for s in [
            "CREATE TABLE t(id INTEGER PRIMARY KEY, g TEXT, v INTEGER)",
            "INSERT INTO t VALUES (1,'a',50),(2,'a',10),(3,'a',30),(4,'b',40),(5,'b',20),(6,'c',60)",
        ] {
            f.execute(s).await.unwrap();
            r.execute(s, []).unwrap();
        }

        let queries = [
            // inner ORDER BY + LIMIT must be applied BEFORE the outer sees rows
            "SELECT id, v FROM (SELECT id, v FROM t ORDER BY v DESC LIMIT 3) ORDER BY id",
            // outer WHERE over an inner LIMIT (the LIMIT slices first, then WHERE filters)
            "SELECT id FROM (SELECT id, v FROM t ORDER BY v LIMIT 3) WHERE v > 20 ORDER BY id",
            // inner aggregate (per-group) feeding an outer filter
            "SELECT g, mx FROM (SELECT g, max(v) AS mx FROM t GROUP BY g) WHERE mx >= 40 ORDER BY g",
            // outer aggregate over an inner GROUP BY result
            "SELECT sum(mx), count(*) FROM (SELECT g, max(v) AS mx FROM t GROUP BY g)",
            // inner DISTINCT under an outer count
            "SELECT count(*) FROM (SELECT DISTINCT g FROM t)",
            // doubly-nested with an inner LIMIT at each level
            "SELECT id FROM (SELECT id FROM (SELECT id FROM t ORDER BY v DESC LIMIT 4) ORDER BY id LIMIT 2) ORDER BY id",
            // inner ORDER BY sliced by an OUTER limit
            "SELECT g, v FROM (SELECT g, v FROM t ORDER BY v) LIMIT 3",
            // join between an inner-aggregated subquery and the base table
            "SELECT t.id, t.v, s.mx FROM t JOIN (SELECT g, max(v) AS mx FROM t GROUP BY g) s ON t.g=s.g WHERE t.v=s.mx ORDER BY t.id",
            // inner LIMIT 0 -> empty
            "SELECT count(*) FROM (SELECT v FROM t LIMIT 0)",
            // inner subquery with WHERE + ORDER BY + LIMIT + OFFSET
            "SELECT id, v FROM (SELECT id, v FROM t WHERE g IN ('a','b') ORDER BY v DESC LIMIT 3 OFFSET 1) ORDER BY id",
            // aggregate over a limited subquery (sum of the top-2 values)
            "SELECT sum(v) FROM (SELECT v FROM t ORDER BY v DESC LIMIT 2)",
            // correlated-ish: outer references only the subquery alias columns
            "SELECT a.g, a.total FROM (SELECT g, sum(v) AS total FROM t GROUP BY g) a WHERE a.total = (SELECT max(total2) FROM (SELECT g, sum(v) AS total2 FROM t GROUP BY g))",
        ];

        let mut diffs = Vec::new();
        for q in queries {
            let fr = fq(&f, q).await;
            let rr = rq(&r, q);
            if fr != rr {
                diffs.push(format!("  `{q}`\n     frank= {fr:?}\n     stock= {rr:?}"));
            }
        }
        assert!(diffs.is_empty(), "{} FROM-subquery flattening divergence(s) vs rusqlite:\n{}", diffs.len(), diffs.join("\n"));
    });
}
