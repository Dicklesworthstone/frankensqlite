//! bd-xu82f — Oracle-parity e2e: window frame specs vs rusqlite.
//!
//! Extends window_function_oracle_e2e (which covers ranking / RANGE peer groups
//! / named windows) into the frame mechanics it omits: explicit ROWS frames with
//! `N PRECEDING`/`N FOLLOWING`/`CURRENT ROW` bounds (a sliding fixed-width
//! window), the GROUPS frame type (bounds measured in peer groups, not rows),
//! and the EXCLUDE clause (NO OTHERS / CURRENT ROW / GROUP / TIES). The dataset
//! has deliberate ties so peer-group and EXCLUDE behaviour is observable. Each
//! scenario compares against rusqlite; GROUPS and EXCLUDE live in their own
//! tests so a divergence isolates cleanly.

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

// Ties in x make peer groups observable: groups {10,10},{20,20,20},{30}.
async fn data() -> (Connection, rusqlite::Connection) {
    let f = Connection::open(":memory:").await.unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    for s in [
        "CREATE TABLE t (id INTEGER PRIMARY KEY, x INTEGER)",
        "INSERT INTO t VALUES (1,10),(2,10),(3,20),(4,20),(5,20),(6,30)",
    ] {
        f.execute(s).await.unwrap();
        r.execute_batch(s).unwrap();
    }
    (f, r)
}

#[test]
fn window_rows_frame_sliding() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = data().await;
        check(
        &f,
        &r,
        &[
            // Sliding 3-row window (1 preceding .. 1 following).
            "SELECT id, sum(x) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM t ORDER BY id",
            // Running total.
            "SELECT id, sum(x) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t ORDER BY id",
            // Trailing 3-row count.
            "SELECT id, count(*) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM t ORDER BY id",
            // Forward-looking window.
            "SELECT id, sum(x) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING) FROM t ORDER BY id",
        ],
        "window_rows_frame_sliding",
    )
    .await;
    });
}

#[test]
fn window_groups_frame() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = data().await;
        check(
        &f,
        &r,
        &[
            // GROUPS counts peer groups (ties in x). Current group +/- 1 group.
            "SELECT id, x, sum(x) OVER (ORDER BY x GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM t ORDER BY id",
            // Cumulative over peer groups.
            "SELECT id, x, sum(x) OVER (ORDER BY x GROUPS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t ORDER BY id",
        ],
        "window_groups_frame",
    )
    .await;
    });
}

#[test]
fn window_exclude_clause() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = data().await;
        check(
        &f,
        &r,
        &[
            // Whole-partition sum minus the current row.
            "SELECT id, x, sum(x) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE CURRENT ROW) FROM t ORDER BY id",
            // EXCLUDE GROUP removes the whole peer group (use RANGE so peers share a frame).
            "SELECT id, x, sum(x) OVER (ORDER BY x RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE GROUP) FROM t ORDER BY id",
            // EXCLUDE TIES removes peers but keeps the current row.
            "SELECT id, x, sum(x) OVER (ORDER BY x RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE TIES) FROM t ORDER BY id",
            // EXCLUDE NO OTHERS is the default (full frame).
            "SELECT id, x, sum(x) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE NO OTHERS) FROM t ORDER BY id",
        ],
        "window_exclude_clause",
    )
    .await;
    });
}
