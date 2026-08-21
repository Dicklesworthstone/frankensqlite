#![recursion_limit = "512"]

//! Differential oracle sweep (pane af49, 2026-08-20): frank vs rusqlite over
//! window functions — aggregate windows with ROWS/RANGE frames, ranking
//! (row_number/rank/dense_rank/ntile), navigation (lag/lead/first_value/
//! last_value/nth_value), PARTITION BY + ORDER BY, and frame boundaries
//! (UNBOUNDED, CURRENT ROW, N PRECEDING/FOLLOWING). Pass = parity coverage
//! keeper; a mismatch is a leaf divergence. Full ordered result sets compared.

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
    conn.query(sql)
        .await
        .unwrap_or_else(|e| panic!("frank `{sql}`: {e:?}"))
        .iter()
        .map(|r| r.values().iter().map(tag_f).collect())
        .collect()
}
fn rq(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut st = conn.prepare(sql).unwrap();
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

#[test]
fn window_functions_match_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for s in [
            "CREATE TABLE w(g TEXT, id INTEGER, v INTEGER)",
            "INSERT INTO w VALUES \
             ('a',1,10),('a',2,10),('a',3,30),('a',4,NULL),\
             ('b',5,5),('b',6,15),('b',7,25),('c',8,100)",
        ] {
            f.execute(s).await.unwrap();
            r.execute(s, []).unwrap();
        }

        let queries = [
            "SELECT id, row_number() OVER (ORDER BY v, id) FROM w ORDER BY id",
            "SELECT id, rank() OVER (ORDER BY v) FROM w ORDER BY id",
            "SELECT id, dense_rank() OVER (ORDER BY v) FROM w ORDER BY id",
            "SELECT id, ntile(3) OVER (ORDER BY id) FROM w ORDER BY id",
            "SELECT g, id, sum(v) OVER (PARTITION BY g ORDER BY id) FROM w ORDER BY g, id",
            "SELECT id, sum(v) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM w ORDER BY id",
            "SELECT id, avg(v) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM w ORDER BY id",
            "SELECT id, count(v) OVER (ORDER BY v RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM w ORDER BY id",
            "SELECT id, lag(v,1,-1) OVER (ORDER BY id) FROM w ORDER BY id",
            "SELECT id, lead(v) OVER (ORDER BY id) FROM w ORDER BY id",
            "SELECT g, id, first_value(v) OVER (PARTITION BY g ORDER BY id) FROM w ORDER BY g, id",
            "SELECT g, id, last_value(v) OVER (PARTITION BY g ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM w ORDER BY g, id",
            "SELECT id, nth_value(v,2) OVER (ORDER BY id) FROM w ORDER BY id",
            "SELECT id, sum(v) OVER () FROM w ORDER BY id",
            "SELECT g, sum(v) OVER (PARTITION BY g) FROM w ORDER BY g, id",
            "SELECT id, max(v) OVER (ORDER BY id DESC) FROM w ORDER BY id",
            "SELECT id, group_concat(v,',') OVER (ORDER BY id) FROM w ORDER BY id",
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
            "{} window-function divergence(s) vs rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}
