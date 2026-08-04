//! bd-w1dgg — Oracle-parity e2e: derived tables (subquery in FROM) vs rusqlite.
//!
//! subquery_oracle covers subqueries in SELECT/WHERE; this covers inline views
//! in the FROM clause: a derived table whose aggregate result is filtered by the
//! outer query, a derived table joined to a base table, nested derived tables,
//! a top-N derived table (inner ORDER BY + LIMIT) re-ordered outside, a window
//! function computed inside a derived table and filtered outside, an aggregate
//! over an aggregate, and two derived tables joined together. Each scenario
//! compares query results against rusqlite; row order is pinned with ORDER BY.
#![recursion_limit = "512"]

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

async fn data() -> (Connection, rusqlite::Connection) {
    let f = Connection::open(":memory:").await.unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    for s in [
        "CREATE TABLE sales (id INTEGER PRIMARY KEY, region TEXT, amount INTEGER)",
        "INSERT INTO sales VALUES (1,'east',50),(2,'east',70),(3,'west',30),(4,'west',90),(5,'north',120),(6,'east',10)",
    ] {
        f.execute(s).await.unwrap();
        r.execute_batch(s).unwrap();
    }
    (f, r)
}

#[test]
fn derived_aggregate_feeds_outer_filter() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = data().await;
        check(
            &f,
            &r,
            &[
                // region totals: east 130, west 120, north 120; filter >100 -> all.
                "SELECT region, total FROM (SELECT region, sum(amount) AS total FROM sales GROUP BY region) WHERE total > 125 ORDER BY region",
                "SELECT region, total FROM (SELECT region, sum(amount) AS total FROM sales GROUP BY region) ORDER BY total DESC, region",
            ],
            "derived_aggregate_feeds_outer_filter",
        )
        .await;
    });
}

#[test]
fn derived_joined_to_base_table() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = data().await;
        check(
            &f,
            &r,
            &["SELECT s.id, agg.region_total FROM sales s \
                 JOIN (SELECT region, sum(amount) AS region_total FROM sales GROUP BY region) agg \
                 ON s.region = agg.region ORDER BY s.id"],
            "derived_joined_to_base_table",
        )
        .await;
    });
}

#[test]
fn derived_nested() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = data().await;
        check(
            &f,
            &r,
            &[
                "SELECT x FROM (SELECT n*2 AS x FROM (SELECT amount AS n FROM sales WHERE amount > 50)) ORDER BY x",
            ],
            "derived_nested",
        )
        .await;
    });
}

#[test]
fn derived_top_n_then_reorder() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = data().await;
        check(
            &f,
            &r,
            &[
                // Inner picks top-3 by amount; outer re-orders ascending.
                "SELECT id, amount FROM (SELECT id, amount FROM sales ORDER BY amount DESC LIMIT 3) ORDER BY amount, id",
            ],
            "derived_top_n_then_reorder",
        )
        .await;
    });
}

#[test]
fn derived_window_filtered_outside() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = data().await;
        check(
            &f,
            &r,
            &[
                // Rank inside the derived table, filter rnk <= 2 outside.
                "SELECT id, rnk FROM (SELECT id, rank() OVER (ORDER BY amount DESC) AS rnk FROM sales) WHERE rnk <= 2 ORDER BY rnk, id",
            ],
            "derived_window_filtered_outside",
        )
        .await;
    });
}

#[test]
fn derived_aggregate_of_aggregate() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = data().await;
        check(
            &f,
            &r,
            &[
                // max over per-region totals (130).
                "SELECT max(region_total), min(region_total) FROM (SELECT region, sum(amount) AS region_total FROM sales GROUP BY region)",
                "SELECT avg(region_total) FROM (SELECT region, sum(amount) AS region_total FROM sales GROUP BY region)",
            ],
            "derived_aggregate_of_aggregate",
        )
        .await;
    });
}

#[test]
fn derived_two_joined() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = data().await;
        check(
            &f,
            &r,
            &[
                // Two independent derived tables joined on region.
                "SELECT a.region, a.cnt, b.total FROM \
                 (SELECT region, count(*) AS cnt FROM sales GROUP BY region) a \
                 JOIN (SELECT region, sum(amount) AS total FROM sales GROUP BY region) b \
                 ON a.region = b.region ORDER BY a.region",
            ],
            "derived_two_joined",
        )
        .await;
    });
}
