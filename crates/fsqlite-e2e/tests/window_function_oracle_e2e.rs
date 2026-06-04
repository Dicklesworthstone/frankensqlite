//! bd-3frbk — Oracle-parity e2e: window functions vs rusqlite (real SQLite).
//!
//! Focuses on window-function *semantics*: the ranking family (ROW_NUMBER,
//! RANK, DENSE_RANK, NTILE), the value family (LAG/LEAD with offset+default,
//! FIRST_VALUE, LAST_VALUE, NTH_VALUE), running aggregates over explicit ROWS
//! vs RANGE frames (RANGE forms peer groups on equal ORDER BY keys), the
//! default-frame LAST_VALUE gotcha (defaults to CURRENT ROW, not partition
//! end), PARTITION BY, and named WINDOW definitions. Only integer-valued
//! results are compared so the string rendering is unambiguous. Deterministic
//! data with unique tiebreakers in every window/outer ORDER BY.

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

fn frank_rows(conn: &Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let rows = conn.query(sql).map_err(|e| e.to_string())?;
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

fn setup() -> (Connection, rusqlite::Connection) {
    let ddl = [
        "CREATE TABLE s (id INTEGER PRIMARY KEY, grp TEXT, score INTEGER)",
        // Note repeated scores within a group to exercise RANK ties / RANGE peers.
        "INSERT INTO s VALUES \
         (1,'a',10),(2,'a',20),(3,'a',20),(4,'a',30),\
         (5,'b',5),(6,'b',15),(7,'b',15),(8,'b',25),(9,'b',25)",
    ];
    let f = Connection::open(":memory:").expect("open frank");
    let r = rusqlite::Connection::open_in_memory().expect("open rusqlite");
    for stmt in ddl {
        f.execute(stmt)
            .unwrap_or_else(|e| panic!("frank `{stmt}`: {e}"));
        r.execute_batch(stmt)
            .unwrap_or_else(|e| panic!("rusqlite `{stmt}`: {e}"));
    }
    (f, r)
}

fn check(f: &Connection, r: &rusqlite::Connection, queries: &[&str], label: &str) {
    let mut mismatches = Vec::new();
    for q in queries {
        match (frank_rows(f, q), sqlite_rows(r, q)) {
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
fn window_ranking_functions() {
    let (f, r) = setup();
    check(
        &f,
        &r,
        &[
            "SELECT id, ROW_NUMBER() OVER (ORDER BY score, id) FROM s ORDER BY id",
            "SELECT id, RANK() OVER (ORDER BY score) FROM s ORDER BY id",
            "SELECT id, DENSE_RANK() OVER (ORDER BY score) FROM s ORDER BY id",
            // Partitioned ranking.
            "SELECT id, grp, RANK() OVER (PARTITION BY grp ORDER BY score) FROM s ORDER BY id",
            "SELECT id, grp, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY score, id) FROM s ORDER BY id",
        ],
        "window_ranking_functions",
    );
}

#[test]
fn window_ntile() {
    let (f, r) = setup();
    check(
        &f,
        &r,
        &[
            "SELECT id, NTILE(3) OVER (ORDER BY score, id) FROM s ORDER BY id",
            "SELECT id, grp, NTILE(2) OVER (PARTITION BY grp ORDER BY score, id) FROM s ORDER BY id",
            "SELECT id, NTILE(100) OVER (ORDER BY id) FROM s ORDER BY id",
        ],
        "window_ntile",
    );
}

#[test]
fn window_lag_lead() {
    let (f, r) = setup();
    check(
        &f,
        &r,
        &[
            "SELECT id, LAG(score) OVER (ORDER BY id) FROM s ORDER BY id",
            "SELECT id, LEAD(score) OVER (ORDER BY id) FROM s ORDER BY id",
            // Offset + default.
            "SELECT id, LAG(score, 2, -1) OVER (ORDER BY id) FROM s ORDER BY id",
            "SELECT id, LEAD(score, 1, 0) OVER (ORDER BY id) FROM s ORDER BY id",
            // Partitioned LAG.
            "SELECT id, grp, LAG(score, 1, -99) OVER (PARTITION BY grp ORDER BY id) FROM s ORDER BY id",
        ],
        "window_lag_lead",
    );
}

#[test]
fn window_first_last_nth_value() {
    let (f, r) = setup();
    check(
        &f,
        &r,
        &[
            "SELECT id, FIRST_VALUE(score) OVER (PARTITION BY grp ORDER BY id) FROM s ORDER BY id",
            // Default-frame LAST_VALUE = CURRENT ROW (the classic gotcha).
            "SELECT id, LAST_VALUE(score) OVER (PARTITION BY grp ORDER BY id) FROM s ORDER BY id",
            // Full-frame LAST_VALUE = partition's last row.
            "SELECT id, LAST_VALUE(score) OVER (PARTITION BY grp ORDER BY id \
               ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM s ORDER BY id",
            "SELECT id, NTH_VALUE(score, 2) OVER (PARTITION BY grp ORDER BY id \
               ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM s ORDER BY id",
        ],
        "window_first_last_nth_value",
    );
}

#[test]
fn window_running_aggregates_rows_frame() {
    let (f, r) = setup();
    check(
        &f,
        &r,
        &[
            // Running sum/count/min/max over an explicit ROWS frame.
            "SELECT id, SUM(score) OVER (ORDER BY id ROWS UNBOUNDED PRECEDING) FROM s ORDER BY id",
            "SELECT id, COUNT(*) OVER (ORDER BY id ROWS UNBOUNDED PRECEDING) FROM s ORDER BY id",
            "SELECT id, MAX(score) OVER (ORDER BY id ROWS UNBOUNDED PRECEDING) FROM s ORDER BY id",
            // Sliding 3-row window.
            "SELECT id, SUM(score) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM s ORDER BY id",
            // Partitioned running total.
            "SELECT id, grp, SUM(score) OVER (PARTITION BY grp ORDER BY id ROWS UNBOUNDED PRECEDING) FROM s ORDER BY id",
        ],
        "window_running_aggregates_rows_frame",
    );
}

#[test]
fn window_range_frame_peer_groups() {
    let (f, r) = setup();
    check(
        &f,
        &r,
        &[
            // RANGE frame: rows with equal ORDER BY key are peers and share the
            // same frame end, so equal scores get the same running sum.
            "SELECT id, score, SUM(score) OVER (ORDER BY score RANGE UNBOUNDED PRECEDING) FROM s ORDER BY id",
            "SELECT id, score, COUNT(*) OVER (ORDER BY score RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM s ORDER BY id",
            // Contrast: ROWS frame distinguishes equal-score rows by position.
            "SELECT id, score, SUM(score) OVER (ORDER BY score, id ROWS UNBOUNDED PRECEDING) FROM s ORDER BY id",
        ],
        "window_range_frame_peer_groups",
    );
}

#[test]
fn window_count_star_counts_null_order_peers() {
    let (f, r) = setup();
    check(
        &f,
        &r,
        &[
            "SELECT id, COUNT(*) OVER (
                 ORDER BY CASE WHEN id <= 2 THEN NULL ELSE id END
                 RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
             ) FROM s ORDER BY id",
            "SELECT id, COUNT() OVER (
                 ORDER BY CASE WHEN id <= 2 THEN NULL ELSE id END
                 RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
             ) FROM s ORDER BY id",
        ],
        "window_count_star_counts_null_order_peers",
    );
}

#[test]
fn group_by_window_materializes_args_and_filter() {
    let (f, r) = setup();
    check(
        &f,
        &r,
        &[
            "SELECT grp,
                    SUM(SUM(score)) FILTER (WHERE grp = 'b') OVER ()
             FROM s
             GROUP BY grp
             ORDER BY grp",
            "SELECT grp,
                    COUNT() OVER (
                        ORDER BY CASE WHEN grp = 'a' THEN NULL ELSE grp END
                        RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    )
             FROM s
             GROUP BY grp
             ORDER BY grp",
        ],
        "group_by_window_materializes_args_and_filter",
    );
}

#[test]
fn window_named_window_clause() {
    let (f, r) = setup();
    check(
        &f,
        &r,
        &[
            "SELECT id, RANK() OVER w, ROW_NUMBER() OVER w FROM s \
             WINDOW w AS (PARTITION BY grp ORDER BY score, id) ORDER BY id",
            "SELECT id, SUM(score) OVER w FROM s \
             WINDOW w AS (PARTITION BY grp ORDER BY id ROWS UNBOUNDED PRECEDING) ORDER BY id",
        ],
        "window_named_window_clause",
    );
}
