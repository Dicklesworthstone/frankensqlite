#![recursion_limit = "512"]

//! Compound-SELECT ordering/limit leaf-hunt (pane af49, 2026-08-21): frank vs
//! rusqlite over UNION / UNION ALL / INTERSECT / EXCEPT combined with a trailing
//! ORDER BY (which binds to the whole compound, not the last arm), ORDER BY by
//! output-column ordinal and by the first arm's result-column name/alias,
//! LIMIT / OFFSET applied to the compound result, nested compounds with mixed
//! operators (SQLite evaluates them left-to-right with equal precedence), and
//! INTERSECT/EXCEPT set semantics with duplicates. Ordered result sets compared.
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
fn compound_select_order_limit_match_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for s in [
            "CREATE TABLE a(x INTEGER, y TEXT)",
            "CREATE TABLE b(x INTEGER, y TEXT)",
            "INSERT INTO a VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d')",
            "INSERT INTO b VALUES (3,'c'),(4,'d'),(5,'e'),(6,'f')",
        ] {
            f.execute(s).await.unwrap();
            r.execute(s, []).unwrap();
        }

        let queries = [
            // trailing ORDER BY binds the whole compound
            "SELECT x FROM a UNION SELECT x FROM b ORDER BY x",
            "SELECT x FROM a UNION ALL SELECT x FROM b ORDER BY x",
            "SELECT x FROM a UNION ALL SELECT x FROM b ORDER BY x DESC",
            // INTERSECT / EXCEPT set semantics
            "SELECT x FROM a INTERSECT SELECT x FROM b ORDER BY x",
            "SELECT x FROM a EXCEPT SELECT x FROM b ORDER BY x",
            "SELECT x FROM b EXCEPT SELECT x FROM a ORDER BY x",
            // ORDER BY by output ordinal
            "SELECT x,y FROM a UNION SELECT x,y FROM b ORDER BY 1 DESC, 2",
            // ORDER BY by the first arm's result-column name
            "SELECT x AS k FROM a UNION SELECT x FROM b ORDER BY k",
            // ORDER BY referencing a column name that exists in the first SELECT
            "SELECT x, y FROM a UNION ALL SELECT x, y FROM b ORDER BY y, x",
            // LIMIT / OFFSET on the compound
            "SELECT x FROM a UNION SELECT x FROM b ORDER BY x LIMIT 3",
            "SELECT x FROM a UNION SELECT x FROM b ORDER BY x LIMIT 3 OFFSET 2",
            "SELECT x FROM a UNION ALL SELECT x FROM b ORDER BY x DESC LIMIT 2 OFFSET 1",
            // nested compounds, mixed operators, left-to-right precedence
            "SELECT x FROM a UNION SELECT x FROM b EXCEPT SELECT 4 ORDER BY x",
            "SELECT x FROM a INTERSECT SELECT x FROM b UNION SELECT 100 ORDER BY x",
            // compound with a VALUES arm
            "SELECT x FROM a UNION VALUES (10),(2) ORDER BY x",
            // duplicate handling: UNION collapses, UNION ALL keeps
            "SELECT x FROM a UNION SELECT x FROM a ORDER BY x",
            "SELECT x FROM a UNION ALL SELECT x FROM a ORDER BY x",
            // EXCEPT then ORDER BY DESC with LIMIT
            "SELECT x FROM b EXCEPT SELECT x FROM a ORDER BY x DESC LIMIT 1",
            // compound inside a subquery, outer ORDER BY
            "SELECT x FROM (SELECT x FROM a UNION SELECT x FROM b) ORDER BY x DESC",
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
            "{} compound-SELECT ordering divergence(s) vs rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}
