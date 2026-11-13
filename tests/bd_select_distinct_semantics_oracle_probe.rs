#![recursion_limit = "512"]

//! SELECT DISTINCT semantics leaf-hunt (pane af49, 2026-08-21): frank vs
//! rusqlite over DISTINCT — single- and multi-column DISTINCT, NULL treated as
//! equal-to-NULL for dedup purposes (so duplicate NULL rows collapse), DISTINCT
//! over an expression, DISTINCT combined with ORDER BY and LIMIT/OFFSET, the
//! DISTINCT-vs-GROUP BY equivalence, affinity-driven distinctness (int 1 vs
//! text '1' vs real 1.0 are distinct storage classes), and DISTINCT over a
//! join. Ordered result sets compared. Pass = coverage keeper.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => format!("int:{n}"),
        SqliteValue::Float(f) => format!("real:{f:?}"),
        SqliteValue::Text(s) => format!("text:{s}"),
        SqliteValue::Blob(b) => format!("blob:{b:?}"),
    }
}
fn tag_r(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => format!("int:{n}"),
        rusqlite::types::Value::Real(f) => format!("real:{f:?}"),
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
fn select_distinct_semantics_match_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for s in [
            "CREATE TABLE t(g TEXT, n INTEGER, x REAL)",
            "INSERT INTO t VALUES \
              ('a',1,1.0),('a',1,1.0),('a',2,2.0),\
              ('b',NULL,NULL),('b',NULL,NULL),('b',3,3.5),\
              (NULL,1,NULL),(NULL,1,NULL)",
        ] {
            f.execute(s).await.unwrap();
            r.execute(s, []).unwrap();
        }

        let queries = [
            // single-column DISTINCT with a NULL group (NULLs collapse to one)
            "SELECT DISTINCT g FROM t ORDER BY g",
            "SELECT DISTINCT n FROM t ORDER BY n",
            // multi-column DISTINCT: (g,n) pairs, NULLs equal for dedup
            "SELECT DISTINCT g, n FROM t ORDER BY g, n",
            // DISTINCT over all three including NULL-bearing rows
            "SELECT DISTINCT g, n, x FROM t ORDER BY g, n, x",
            // DISTINCT over an expression
            "SELECT DISTINCT n * 2 AS d FROM t ORDER BY d",
            "SELECT DISTINCT g || '/' || COALESCE(n,-1) FROM t ORDER BY 1",
            // DISTINCT + ORDER BY DESC + LIMIT/OFFSET
            "SELECT DISTINCT n FROM t ORDER BY n DESC LIMIT 2",
            "SELECT DISTINCT g FROM t ORDER BY g LIMIT 2 OFFSET 1",
            // count of distinct rows
            "SELECT count(*) FROM (SELECT DISTINCT g, n FROM t)",
            // DISTINCT-vs-GROUP BY equivalence
            "SELECT g, n FROM t GROUP BY g, n ORDER BY g, n",
            // affinity: mixed storage classes stay distinct (int 1 vs text '1' vs real 1.0);
            // CTE column-list alias is the SQLite-supported positional-rename form.
            "WITH z(v) AS (VALUES (1),('1'),(1.0),(NULL),(NULL)) SELECT DISTINCT v FROM z ORDER BY typeof(v), v",
            // DISTINCT over a join
            "SELECT DISTINCT t1.g FROM t t1 JOIN t t2 ON t1.g=t2.g ORDER BY t1.g",
            // DISTINCT with a WHERE filter
            "SELECT DISTINCT n FROM t WHERE g='a' ORDER BY n",
            // DISTINCT *
            "SELECT DISTINCT * FROM t ORDER BY g, n, x",
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
            "{} DISTINCT divergence(s) vs rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}
