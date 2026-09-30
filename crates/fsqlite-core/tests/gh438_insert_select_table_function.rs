#![recursion_limit = "512"]

//! GH#438: `INSERT INTO t SELECT ... FROM <table-valued function>` took time
//! quadratic in the row count. The materialized fallback injected
//! LIMIT/OFFSET chunks, and each chunk re-ran the function from its first row.
//! A SELECT that reads only table-valued functions is now materialized once.
//! This pins the inserted rows at counts that cross the 10,000-row chunk
//! boundary. The expected values are computed in closed form, because
//! rusqlite's bundled SQLite lacks `generate_series`.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

async fn ints(conn: &Connection, sql: &str) -> Vec<i64> {
    let rows = conn
        .query(sql)
        .await
        .unwrap_or_else(|e| panic!("`{sql}`: {e:?}"));
    rows[0]
        .values()
        .iter()
        .map(|value| match value {
            SqliteValue::Integer(n) => *n,
            other => panic!("`{sql}`: expected an integer, got {other:?}"),
        })
        .collect()
}

#[test]
fn insert_select_from_table_functions_across_chunk_boundaries() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        for sql in [
            "CREATE TABLE t2(value)",
            "CREATE TABLE t3(a INTEGER, b INTEGER)",
            "CREATE TABLE t4(id INTEGER PRIMARY KEY)",
            "INSERT INTO t4 SELECT value FROM generate_series(1, 5000)",
            "INSERT INTO t2 SELECT value FROM generate_series(1, 25001)",
            "INSERT INTO t2 SELECT value * 2 FROM generate_series(1, 30000) WHERE value % 3 = 0",
            "INSERT INTO t3(a, b) SELECT x.value, y.value FROM generate_series(1, 15000) AS x JOIN generate_series(1, 2) AS y",
            "INSERT OR IGNORE INTO t4 SELECT value FROM generate_series(1, 20000)",
        ] {
            conn.execute(sql)
                .await
                .unwrap_or_else(|e| panic!("`{sql}`: {e:?}"));
        }
        // 1..=25001, then 2*v for the 10,000 multiples of 3 in 1..=30000.
        assert_eq!(
            ints(&conn, "SELECT count(*), sum(value), min(value), max(value) FROM t2").await,
            vec![35_001, 25_001 * 25_002 / 2 + 6 * (10_000 * 10_001 / 2), 1, 60_000]
        );
        // Every x in 1..=15000 paired with y in {1, 2}.
        assert_eq!(
            ints(&conn, "SELECT count(*), sum(a), sum(b), count(DISTINCT a) FROM t3").await,
            vec![30_000, 15_000 * 15_001, 45_000, 15_000]
        );
        // 1..=5000 existed; OR IGNORE adds 5001..=20000.
        assert_eq!(
            ints(&conn, "SELECT count(*), min(id), max(id) FROM t4").await,
            vec![20_000, 1, 20_000]
        );
    });
}
