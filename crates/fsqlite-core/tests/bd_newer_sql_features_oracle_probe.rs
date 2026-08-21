#![recursion_limit = "512"]

//! Newer-SQL-feature leaf-hunt (pane af49, 2026-08-21): frank vs rusqlite over
//! features added in recent SQLite that (like JSON5, bd-qear2) may be missing —
//! timediff() (3.43+), aggregate FILTER clause, ordered aggregates
//! (group_concat(x ORDER BY y)), string_agg (3.44+), and window-frame EXCLUDE.
//! Error-tolerant so a missing feature surfaces as a divergence. Pass =
//! coverage keeper; a mismatch/ERR-vs-value is a leaf.

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
    let Ok(mut st) = conn.prepare(sql) else {
        return vec![vec!["ERR".to_owned()]];
    };
    let n = st.column_count();
    match st.query_map([], |row| {
        Ok((0..n)
            .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
            .collect::<Vec<_>>())
    }) {
        Ok(rows) => rows.collect::<Result<Vec<_>, _>>().unwrap_or_else(|_| vec![vec!["ERR".to_owned()]]),
        Err(_) => vec![vec!["ERR".to_owned()]],
    }
}

#[test]
fn newer_sql_features_match_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for s in [
            "CREATE TABLE t(g TEXT, k INTEGER, v INTEGER)",
            "INSERT INTO t VALUES ('a',1,10),('a',2,30),('a',3,20),('b',4,5),('b',5,25),('b',6,NULL)",
        ] {
            f.execute(s).await.unwrap();
            r.execute(s, []).unwrap();
        }

        let queries = [
            // timediff() (3.43+)
            "SELECT timediff('2024-03-05 00:00:00','2024-01-01 00:00:00')",
            "SELECT timediff('2024-01-01','2024-03-05')",
            "SELECT timediff('2024-01-01 12:30:00','2024-01-01 10:00:00')",
            // ordered aggregate
            "SELECT group_concat(v ORDER BY v) FROM t",
            "SELECT group_concat(k ORDER BY v DESC) FROM t",
            "SELECT group_concat(v, '|' ORDER BY k DESC) FROM t",
            // string_agg (3.44+)
            "SELECT string_agg(g, ',') FROM t",
            "SELECT string_agg(CAST(k AS TEXT), '-' ORDER BY k DESC) FROM t",
            // aggregate FILTER clause
            "SELECT count(*) FILTER (WHERE v > 15) FROM t",
            "SELECT sum(v) FILTER (WHERE g='a') FROM t",
            "SELECT g, count(*) FILTER (WHERE v IS NOT NULL) FROM t GROUP BY g ORDER BY g",
            "SELECT avg(v) FILTER (WHERE v > 10), max(v) FILTER (WHERE g='b') FROM t",
            // window frame EXCLUDE
            "SELECT k, sum(v) OVER (ORDER BY k ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE CURRENT ROW) FROM t ORDER BY k",
            "SELECT k, count(*) OVER (ORDER BY k GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING EXCLUDE GROUP) FROM t ORDER BY k",
            "SELECT k, sum(v) OVER (ORDER BY k ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING EXCLUDE TIES) FROM t ORDER BY k",
            // window FILTER
            "SELECT k, sum(v) FILTER (WHERE v > 10) OVER (ORDER BY k) FROM t ORDER BY k",
            // ordered aggregate inside window? (group_concat window)
            "SELECT k, group_concat(v) OVER (ORDER BY k) FROM t ORDER BY k",
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
            "{} newer-SQL-feature divergence(s) vs rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}
