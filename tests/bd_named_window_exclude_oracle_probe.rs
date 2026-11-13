#![recursion_limit = "512"]

//! Named-WINDOW + frame-EXCLUDE leaf-hunt (pane af49, 2026-08-21): frank vs
//! rusqlite over the less-trodden window-clause corners — a named WINDOW
//! definition reused by several window functions, a window that inherits from
//! another named window (base-window reference), and the four frame EXCLUDE
//! options (NO OTHERS / CURRENT ROW / GROUP / TIES) over ROWS and RANGE frames
//! with duplicate order keys. Full ordered result sets compared. Pass = keeper.

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
fn named_window_exclude_match_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for s in [
            "CREATE TABLE w(id INTEGER PRIMARY KEY, k INTEGER, v INTEGER)",
            // duplicate k values -> RANGE/GROUPS peers and EXCLUDE TIES matter
            "INSERT INTO w VALUES (1,1,10),(2,1,20),(3,2,30),(4,2,40),(5,2,50),(6,3,60),(7,3,70)",
        ] {
            f.execute(s).await.unwrap();
            r.execute(s, []).unwrap();
        }

        let queries = [
            // named WINDOW reused by several functions
            "SELECT id, sum(v) OVER win, avg(v) OVER win FROM w WINDOW win AS (ORDER BY k) ORDER BY id",
            // two named windows in one WINDOW clause
            "SELECT id, sum(v) OVER a, count(*) OVER b FROM w \
             WINDOW a AS (ORDER BY k), b AS (PARTITION BY k) ORDER BY id",
            // a window that inherits from another named window (base ref + added frame)
            "SELECT id, sum(v) OVER w2 FROM w \
             WINDOW w1 AS (ORDER BY k), w2 AS (w1 ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) ORDER BY id",
            // EXCLUDE NO OTHERS (default) vs the others over a ROWS frame
            "SELECT id, sum(v) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING EXCLUDE NO OTHERS) FROM w ORDER BY id",
            "SELECT id, sum(v) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING EXCLUDE CURRENT ROW) FROM w ORDER BY id",
            // EXCLUDE GROUP / TIES over a RANGE frame with peers (duplicate k)
            "SELECT id, k, sum(v) OVER (ORDER BY k RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE GROUP) FROM w ORDER BY id",
            "SELECT id, k, sum(v) OVER (ORDER BY k RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE TIES) FROM w ORDER BY id",
            "SELECT id, k, sum(v) OVER (ORDER BY k RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE CURRENT ROW) FROM w ORDER BY id",
            // EXCLUDE with count over GROUPS frame
            "SELECT id, k, count(*) OVER (ORDER BY k GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING EXCLUDE GROUP) FROM w ORDER BY id",
            // EXCLUDE TIES keeps current row but drops its peers
            "SELECT id, k, count(*) OVER (ORDER BY k RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE TIES) FROM w ORDER BY id",
            // named window carrying a frame + EXCLUDE, reused
            "SELECT id, min(v) OVER win, max(v) OVER win FROM w \
             WINDOW win AS (ORDER BY k RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING EXCLUDE CURRENT ROW) ORDER BY id",
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
            "{} named-window/EXCLUDE divergence(s) vs rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}
