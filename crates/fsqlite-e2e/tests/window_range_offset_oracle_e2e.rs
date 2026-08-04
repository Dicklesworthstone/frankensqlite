//! bd-yjah4 — Oracle-parity e2e: value-offset RANGE window frames vs rusqlite.
//!
//! window_frame_oracle covers ROWS (row counts), GROUPS (peer-group counts), and
//! RANGE with UNBOUNDED/peer bounds. This covers the distinct value-offset RANGE
//! form: `RANGE BETWEEN N PRECEDING AND M FOLLOWING` includes every row whose
//! ORDER BY value lies in `[current - N, current + M]` (a value window, not a row
//! count). The dataset has gaps in the ordering column (so value windows differ
//! from row windows) and a ties variant (peers share a frame). Compared against
//! rusqlite; sums are integers.

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

// Gaps in `ord` so value windows differ from row windows.
async fn gapped() -> (Connection, rusqlite::Connection) {
    let f = Connection::open(":memory:").await.unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    for s in [
        "CREATE TABLE t (id INTEGER PRIMARY KEY, ord INTEGER, x INTEGER)",
        "INSERT INTO t VALUES (1,1,10),(2,2,20),(3,5,30),(4,6,40),(5,10,50)",
    ] {
        f.execute(s).await.unwrap();
        r.execute_batch(s).unwrap();
    }
    (f, r)
}

#[test]
fn range_value_offset_preceding() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = gapped().await;
        check(
        &f,
        &r,
        &[
            // ord in [ord-2, ord]: 10,30,30,70,50
            "SELECT id, sum(x) OVER (ORDER BY ord RANGE BETWEEN 2 PRECEDING AND CURRENT ROW) FROM t ORDER BY id",
            // ord in [ord-1, ord+1]: 30,30,70,70,50
            "SELECT id, sum(x) OVER (ORDER BY ord RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM t ORDER BY id",
        ],
        "range_value_offset_preceding",
    )
    .await;
    });
}

#[test]
fn range_value_offset_following() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = gapped().await;
        check(
        &f,
        &r,
        &[
            // ord in [ord, ord+3]: 30,50,70,40,50
            "SELECT id, sum(x) OVER (ORDER BY ord RANGE BETWEEN CURRENT ROW AND 3 FOLLOWING) FROM t ORDER BY id",
        ],
        "range_value_offset_following",
    )
    .await;
    });
}

#[test]
fn range_unbounded_running_by_value() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = gapped().await;
        check(
        &f,
        &r,
        &[
            // Distinct ord -> running total: 10,30,60,100,150
            "SELECT id, sum(x) OVER (ORDER BY ord RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t ORDER BY id",
            // Running count by peer (distinct ord): 1,2,3,4,5
            "SELECT id, count(*) OVER (ORDER BY ord RANGE UNBOUNDED PRECEDING) FROM t ORDER BY id",
        ],
        "range_unbounded_running_by_value",
    )
    .await;
    });
}

#[test]
fn range_value_offset_with_ties() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for s in [
            "CREATE TABLE t (id INTEGER PRIMARY KEY, ord INTEGER, x INTEGER)",
            "INSERT INTO t VALUES (1,1,10),(2,1,20),(3,2,30),(4,2,40),(5,3,50)",
        ] {
            f.execute(s).await.unwrap();
            r.execute_batch(s).unwrap();
        }
        check(
        &f,
        &r,
        &[
            // ord in [ord-1, ord]; peers (same ord) share a frame:
            // ord1 rows -> 30; ord2 rows -> 100; ord3 -> 120
            "SELECT id, sum(x) OVER (ORDER BY ord RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t ORDER BY id",
        ],
        "range_value_offset_with_ties",
    )
    .await;
    });
}
