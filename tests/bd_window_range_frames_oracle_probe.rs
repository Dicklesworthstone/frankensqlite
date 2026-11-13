#![recursion_limit = "512"]

//! RANGE/GROUPS window-frame leaf-hunt (pane af49, 2026-08-21): frank vs
//! rusqlite over value-offset frames — RANGE BETWEEN N PRECEDING AND M
//! FOLLOWING (value-distance peer framing, distinct from ROWS row-count
//! framing), RANGE with CURRENT ROW peers and duplicate order keys, GROUPS
//! frames, and combinations with PARTITION BY. Full ordered result sets
//! compared. Pass = coverage keeper; a mismatch is a leaf divergence.

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
fn window_range_frames_match_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for s in [
            "CREATE TABLE w(id INTEGER PRIMARY KEY, k INTEGER, g TEXT, v INTEGER)",
            // duplicate k values create RANGE peers
            "INSERT INTO w VALUES (1,1,'a',10),(2,1,'a',20),(3,3,'a',30),(4,4,'a',40),\
             (5,4,'b',50),(6,7,'b',60),(7,8,'b',70),(8,10,'b',80)",
        ] {
            f.execute(s).await.unwrap();
            r.execute(s, []).unwrap();
        }

        let queries = [
            // value-offset RANGE frames
            "SELECT id, sum(v) OVER (ORDER BY k RANGE BETWEEN 2 PRECEDING AND 2 FOLLOWING) FROM w ORDER BY id",
            "SELECT id, count(*) OVER (ORDER BY k RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM w ORDER BY id",
            "SELECT id, avg(v) OVER (ORDER BY k RANGE BETWEEN CURRENT ROW AND 3 FOLLOWING) FROM w ORDER BY id",
            // RANGE CURRENT ROW = peer group (all rows with equal k)
            "SELECT id, k, sum(v) OVER (ORDER BY k RANGE CURRENT ROW) FROM w ORDER BY id",
            "SELECT id, k, count(*) OVER (ORDER BY k RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM w ORDER BY id",
            // RANGE with PARTITION
            "SELECT id, g, sum(v) OVER (PARTITION BY g ORDER BY k RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM w ORDER BY id",
            // GROUPS frames (peer-group offsets)
            "SELECT id, sum(v) OVER (ORDER BY k GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM w ORDER BY id",
            "SELECT id, count(*) OVER (ORDER BY k GROUPS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM w ORDER BY id",
            // RANGE with descending order
            "SELECT id, sum(v) OVER (ORDER BY k DESC RANGE BETWEEN 2 PRECEDING AND 2 FOLLOWING) FROM w ORDER BY id",
            // first/last value under a RANGE frame
            "SELECT id, first_value(v) OVER (ORDER BY k RANGE BETWEEN 2 PRECEDING AND 2 FOLLOWING), last_value(v) OVER (ORDER BY k RANGE BETWEEN 2 PRECEDING AND 2 FOLLOWING) FROM w ORDER BY id",
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
            "{} RANGE/GROUPS window-frame divergence(s) vs rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}
