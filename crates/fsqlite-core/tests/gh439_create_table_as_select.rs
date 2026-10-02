#![recursion_limit = "512"]

//! GH#439 / GH#438: CREATE TABLE ... AS SELECT replays its materialized rows
//! through one prepared INSERT, and the VDBE retains materialized result rows
//! at their own width. Both were large constant factors on million-row
//! statements. Pinned against stock SQLite (rusqlite):
//!
//! - the copied rows and their storage classes, for
//!   empty, one-row, wide and multi-thousand-row sources;
//! - changes(), total_changes() and last_insert_rowid(), which stock leaves
//!   untouched across CREATE TABLE ... AS SELECT (fsqlite used to report the
//!   replayed INSERTs), in autocommit and inside an explicit transaction;
//! - ROLLBACK of an explicit transaction removes the table and its rows.

use fsqlite_core::connection::Connection;
use fsqlite_types::SqliteValue;
use rusqlite::types::Value;

fn value(value: Value) -> SqliteValue {
    match value {
        Value::Null => SqliteValue::Null,
        Value::Integer(value) => SqliteValue::Integer(value),
        Value::Real(value) => SqliteValue::Float(value),
        Value::Text(value) => SqliteValue::from(value.as_str()),
        Value::Blob(value) => SqliteValue::Blob(value.into()),
    }
}

async fn frank_rows(conn: &Connection, sql: &str) -> Vec<Vec<SqliteValue>> {
    conn.query(sql)
        .await
        .unwrap_or_else(|e| panic!("FrankenSQLite: `{sql}`: {e:?}"))
        .iter()
        .map(|row| row.values().to_vec())
        .collect()
}

fn stock_rows(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<SqliteValue>> {
    let mut stmt = conn
        .prepare(sql)
        .unwrap_or_else(|e| panic!("SQLite: `{sql}`: {e}"));
    let width = stmt.column_count();
    stmt.query_map([], |row| {
        (0..width)
            .map(|i| row.get::<_, Value>(i).map(value))
            .collect::<rusqlite::Result<Vec<_>>>()
    })
    .unwrap()
    .collect::<rusqlite::Result<Vec<_>>>()
    .unwrap()
}

async fn run_both(frank: &Connection, stock: &rusqlite::Connection, sql: &str) {
    frank
        .execute(sql)
        .await
        .unwrap_or_else(|e| panic!("FrankenSQLite: `{sql}`: {e:?}"));
    stock
        .execute_batch(sql)
        .unwrap_or_else(|e| panic!("SQLite: `{sql}`: {e}"));
}

async fn compare(frank: &Connection, stock: &rusqlite::Connection, sql: &str) {
    assert_eq!(
        frank_rows(frank, sql).await,
        stock_rows(stock, sql),
        "`{sql}` differs from SQLite"
    );
}

const COUNTERS: &str = "SELECT changes(), total_changes(), last_insert_rowid()";

#[test]
fn create_table_as_select_matches_stock_rows_and_counters() {
    asupersync::test_utils::run_test(|| async {
        let frank = Connection::open(":memory:").await.unwrap();
        let stock = rusqlite::Connection::open_in_memory().unwrap();
        // A recursive CTE stands in for generate_series, which rusqlite's
        // bundled SQLite lacks; 12,345 rows cross every 10,000-row boundary.
        for sql in [
            "CREATE TABLE src(i INTEGER, t TEXT, r REAL, b BLOB, n)",
            "INSERT INTO src WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c \
             WHERE x < 12345) SELECT x, 'v' || x, x / 4.0, x'00ff', \
             CASE WHEN x % 3 = 0 THEN NULL WHEN x % 3 = 1 THEN x ELSE 'n' || x END FROM c",
            "CREATE TABLE seed(a)",
            "INSERT INTO seed VALUES (1), (2)",
        ] {
            run_both(&frank, &stock, sql).await;
        }
        compare(&frank, &stock, COUNTERS).await;

        for sql in [
            "CREATE TABLE copy_all AS SELECT * FROM src",
            "CREATE TABLE copy_expr AS SELECT i * 2 AS d, upper(t) AS u, n FROM src WHERE i % 7 = 0",
            "CREATE TABLE copy_one AS SELECT 42 AS answer",
            "CREATE TABLE copy_none AS SELECT * FROM src WHERE 0",
            "CREATE TABLE copy_agg AS SELECT n IS NULL AS k, count(*) AS c, sum(i) AS s \
             FROM src GROUP BY 1",
        ] {
            run_both(&frank, &stock, sql).await;
            compare(&frank, &stock, COUNTERS).await;
        }
        for table in ["copy_all", "copy_expr", "copy_one", "copy_none", "copy_agg"] {
            compare(
                &frank,
                &stock,
                &format!("SELECT rowid, * FROM {table} ORDER BY rowid"),
            )
            .await;
        }
        compare(
            &frank,
            &stock,
            "SELECT typeof(i), typeof(t), typeof(r), typeof(b), typeof(n), count(*) \
             FROM copy_all GROUP BY 1, 2, 3, 4, 5 ORDER BY 1, 2, 3, 4, 5",
        )
        .await;

        // Inside an explicit transaction: counters untouched, and ROLLBACK
        // removes the table with its rows.
        run_both(&frank, &stock, "BEGIN").await;
        run_both(&frank, &stock, "INSERT INTO seed VALUES (3)").await;
        run_both(&frank, &stock, "CREATE TABLE in_txn AS SELECT i FROM src WHERE i <= 500").await;
        compare(&frank, &stock, COUNTERS).await;
        compare(&frank, &stock, "SELECT count(*), sum(i) FROM in_txn").await;
        run_both(&frank, &stock, "ROLLBACK").await;
        compare(
            &frank,
            &stock,
            "SELECT count(*) FROM sqlite_master WHERE name = 'in_txn'",
        )
        .await;
        compare(&frank, &stock, "SELECT count(*), sum(a) FROM seed").await;

        // A materialized FROM-clause subquery and a large result set read
        // back through the retained-row buffer.
        compare(
            &frank,
            &stock,
            "SELECT count(*), sum(d), max(u) FROM (SELECT i * 2 AS d, t AS u FROM src)",
        )
        .await;
        compare(&frank, &stock, "SELECT i, t, n FROM src ORDER BY i DESC").await;
    });
}
