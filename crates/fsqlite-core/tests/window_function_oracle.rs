//! Differential oracle: window functions vs rusqlite (bundled SQLite 3.53). A
//! probe sweep found this surface stock-correct across 20 cases; this keeper
//! locks it in. Deterministic ORDER BY keys avoid tie-ordering ambiguity.
//!
//! Covers ranking (ROW_NUMBER/RANK/DENSE_RANK/NTILE), running aggregates over
//! ROWS and RANGE frames (including RANGE ties and UNBOUNDED FOLLOWING),
//! navigation (LAG/LEAD with offset+default, FIRST_VALUE/LAST_VALUE), PARTITION
//! BY, named WINDOW clauses, and a window function over a grouped aggregate.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!("X'{}'", b.iter().map(|x| format!("{x:02X}")).collect::<String>()),
    }
}
fn tag_r(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => n.to_string(),
        rusqlite::types::Value::Real(f) => format!("{f}"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => format!("X'{}'", b.iter().map(|x| format!("{x:02X}")).collect::<String>()),
    }
}

async fn fq(f: &Connection, sql: &str) -> Vec<Vec<String>> {
    match f.query_with_params(sql, &[]).await {
        Ok(rows) => rows.iter().map(|r| r.values().iter().map(tag_f).collect()).collect(),
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
        Ok((0..n).map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i))).collect())
    })
    .unwrap()
    .collect::<Result<Vec<_>, _>>()
    .unwrap()
}

async fn agree(sql: &str, msg: &str) {
    let f = Connection::open(":memory:").await.unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    for s in SETUP {
        let _ = f.execute(s).await;
        let _ = r.execute_batch(s);
    }
    let fr = fq(&f, sql).await;
    let rr = rq(&r, sql);
    assert_eq!(fr, rr, "{msg}\n  sql   ={sql}\n  frank ={fr:?}\n  sqlite={rr:?}");
}

const SETUP: &[&str] = &[
    "CREATE TABLE s(g TEXT, id INT, v INT)",
    "INSERT INTO s VALUES ('a',1,10),('a',2,20),('a',3,20),('a',4,40),('b',5,5),('b',6,15),('b',7,25)",
];

#[test]
fn ranking_functions() {
    asupersync::test_utils::run_test(|| async {
        agree("SELECT id, row_number() OVER (ORDER BY id) FROM s ORDER BY id", "ROW_NUMBER").await;
        agree("SELECT id, v, rank() OVER (ORDER BY v) FROM s ORDER BY id", "RANK with ties").await;
        agree("SELECT id, v, dense_rank() OVER (ORDER BY v) FROM s ORDER BY id", "DENSE_RANK with ties").await;
        agree("SELECT id, ntile(3) OVER (ORDER BY id) FROM s ORDER BY id", "NTILE(3)").await;
        agree("SELECT g, id, v, rank() OVER (PARTITION BY g ORDER BY v DESC) FROM s ORDER BY g, id", "RANK partitioned").await;
    });
}

#[test]
fn running_frames() {
    asupersync::test_utils::run_test(|| async {
        agree("SELECT id, sum(v) OVER (ORDER BY id) FROM s ORDER BY id", "default running sum").await;
        agree("SELECT id, sum(v) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM s ORDER BY id", "ROWS 1 preceding").await;
        agree("SELECT id, v, sum(v) OVER (ORDER BY v RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM s ORDER BY id", "RANGE with ties").await;
        agree("SELECT id, sum(v) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM s ORDER BY id", "ROWS unbounded following").await;
    });
}

#[test]
fn navigation_functions() {
    asupersync::test_utils::run_test(|| async {
        agree("SELECT id, lag(v) OVER (ORDER BY id) FROM s ORDER BY id", "LAG").await;
        agree("SELECT id, lag(v, 1, -1) OVER (ORDER BY id) FROM s ORDER BY id", "LAG with offset+default").await;
        agree("SELECT id, lead(v) OVER (ORDER BY id) FROM s ORDER BY id", "LEAD").await;
        agree("SELECT id, lead(v, 2) OVER (ORDER BY id) FROM s ORDER BY id", "LEAD offset 2").await;
        agree("SELECT id, first_value(v) OVER (PARTITION BY g ORDER BY id) FROM s ORDER BY id", "FIRST_VALUE").await;
        agree("SELECT id, last_value(v) OVER (PARTITION BY g ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM s ORDER BY id", "LAST_VALUE full frame").await;
    });
}

#[test]
fn partition_named_and_over_group() {
    asupersync::test_utils::run_test(|| async {
        agree("SELECT g, id, row_number() OVER (PARTITION BY g ORDER BY id) FROM s ORDER BY g, id", "ROW_NUMBER partitioned").await;
        agree("SELECT g, id, avg(v) OVER (PARTITION BY g ORDER BY id) FROM s ORDER BY g, id", "partitioned running avg").await;
        agree("SELECT id, count(*) OVER (PARTITION BY g) FROM s ORDER BY id", "count(*) over partition").await;
        agree("SELECT id, sum(v) OVER w, avg(v) OVER w FROM s WINDOW w AS (ORDER BY id) ORDER BY id", "named window").await;
        agree("SELECT g, sum(v) tot, rank() OVER (ORDER BY sum(v) DESC) FROM s GROUP BY g ORDER BY g", "window over grouped aggregate").await;
    });
}
