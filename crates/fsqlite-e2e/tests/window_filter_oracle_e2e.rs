//! bd-9ezyr — Oracle-parity e2e: FILTER(WHERE) on window aggregates vs rusqlite.
//!
//! aggregate_filter_oracle covers `agg(x) FILTER (WHERE p)` on a GROUP BY query.
//! This file covers the orthogonal combination `agg(x) FILTER (WHERE p) OVER
//! (...)` — the FILTER restricts which rows inside the window frame contribute to
//! the aggregate, while every input row still produces an output row. Covered:
//! running aggregate over the default frame, partitioned running count, a frame
//! in which the filter matches nothing (sum->NULL, count->0), and a sliding ROWS
//! frame with avg/total. Fixed data with a unique ORDER BY key so frames are
//! unambiguous; compared against rusqlite.

use fsqlite::Connection;
use fsqlite_types::SqliteValue;

fn render_frank(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!(
            "X'{}'",
            b.iter().map(|x| format!("{x:02X}")).collect::<String>()
        ),
    }
}

async fn frank_rows(conn: &Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let rows = conn.query(sql).await.map_err(|e| e.to_string())?;
    Ok(rows
        .iter()
        .map(|row| row.values().iter().map(render_frank).collect())
        .collect())
}

fn sqlite_rows(conn: &rusqlite::Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
    let n = stmt.column_count();
    stmt.query_map([], |row| {
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            let v: rusqlite::types::Value = row.get_unwrap(i);
            out.push(match v {
                rusqlite::types::Value::Null => "NULL".to_owned(),
                rusqlite::types::Value::Integer(x) => x.to_string(),
                rusqlite::types::Value::Real(f) => format!("{f}"),
                rusqlite::types::Value::Text(s) => format!("'{s}'"),
                rusqlite::types::Value::Blob(b) => format!(
                    "X'{}'",
                    b.iter().map(|x| format!("{x:02X}")).collect::<String>()
                ),
            });
        }
        Ok(out)
    })
    .map_err(|e| e.to_string())?
    .collect::<Result<Vec<_>, _>>()
    .map_err(|e| e.to_string())
}

async fn setup() -> (Connection, rusqlite::Connection) {
    let f = Connection::open(":memory:").await.expect("open frank");
    let r = rusqlite::Connection::open_in_memory().expect("open rusqlite");
    for s in [
        "CREATE TABLE t (id INTEGER PRIMARY KEY, g TEXT, x INTEGER)",
        "INSERT INTO t VALUES (1,'a',10),(2,'a',-5),(3,'a',20),(4,'b',3),(5,'b',-1),(6,'b',8)",
    ] {
        f.execute(s).await.unwrap();
        r.execute_batch(s).unwrap();
    }
    (f, r)
}

async fn check(f: &Connection, r: &rusqlite::Connection, queries: &[&str], label: &str) {
    let mut mismatches = Vec::new();
    for q in queries {
        match (frank_rows(f, q).await, sqlite_rows(r, q)) {
            (Ok(a), Ok(b)) if a == b => {}
            (Ok(a), Ok(b)) => {
                mismatches.push(format!("MISMATCH: {q}\n  frank: {a:?}\n  csql:  {b:?}"))
            }
            (Err(e), Ok(b)) => mismatches.push(format!(
                "FRANK_ERR: {q}\n  frank: ERROR({e})\n  csql:  {b:?}"
            )),
            (Ok(a), Err(e)) => {
                mismatches.push(format!("CSQL_ERR: {q}\n  frank: {a:?}\n  csql: ERROR({e})"))
            }
            (Err(_), Err(_)) => {}
        }
    }
    assert!(
        mismatches.is_empty(),
        "{label}: {} mismatch(es)\n{}",
        mismatches.len(),
        mismatches.join("\n")
    );
}

#[test]
fn filter_on_running_window_sum() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup().await;
        // Cumulative sum of only the positive x, over the default frame.
        check(
            &f,
            &r,
            &[
                "SELECT id, sum(x) FILTER (WHERE x > 0) OVER (ORDER BY id) FROM t ORDER BY id",
                // (1,10),(2,10),(3,30),(4,33),(5,33),(6,41)
            ],
            "filter_on_running_window_sum",
        )
        .await;
    });
}

#[test]
fn filter_on_partitioned_window_count() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup().await;
        // Per-group running count of positive x.
        check(
            &f,
            &r,
            &[
                "SELECT id, count(*) FILTER (WHERE x > 0) OVER (PARTITION BY g ORDER BY id) \
                 FROM t ORDER BY id",
                // a: 1,1,2  b: 1,1,2  -> (1,1),(2,1),(3,2),(4,1),(5,1),(6,2)
            ],
            "filter_on_partitioned_window_count",
        )
        .await;
    });
}

#[test]
fn filter_matches_nothing_in_frame() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup().await;
        // No row satisfies the predicate: windowed sum is NULL, count is 0, everywhere.
        check(
            &f,
            &r,
            &[
                "SELECT id, \
                    sum(x)   FILTER (WHERE x > 1000) OVER (ORDER BY id), \
                    count(*) FILTER (WHERE x > 1000) OVER (ORDER BY id) \
                 FROM t ORDER BY id",
                // (id, NULL, 0) for every row
            ],
            "filter_matches_nothing_in_frame",
        )
        .await;
    });
}

#[test]
fn filter_with_sliding_rows_frame() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup().await;
        // 2-row sliding frame (previous + current), positives only.
        check(
            &f,
            &r,
            &[
                "SELECT id, \
                    avg(x)   FILTER (WHERE x > 0) OVER w, \
                    total(x) FILTER (WHERE x > 0) OVER w \
                 FROM t \
                 WINDOW w AS (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) \
                 ORDER BY id",
                // (1,10,10),(2,10,10),(3,20,20),(4,11.5,23),(5,3,3),(6,8,8)
            ],
            "filter_with_sliding_rows_frame",
        )
        .await;
    });
}
