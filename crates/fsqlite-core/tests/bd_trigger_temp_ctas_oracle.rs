#![recursion_limit = "512"]

//! Trigger / CREATE TABLE AS parity gaps, pinned against stock SQLite
//! (rusqlite), in memory and file-backed:
//!
//! - bd-y26jy: `CREATE TEMP TABLE x AS SELECT ...` (and `temp.x`) creates the
//!   table in the temp schema, so `CREATE INDEX temp.i ON x(...)` works and
//!   `temp.sqlite_master` lists it. fsqlite used to create a main table.

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

/// Runs `body` once against in-memory databases and once against fresh files.
fn for_each_backing<F, Fut>(body: F)
where
    F: Fn(Connection, rusqlite::Connection) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    asupersync::test_utils::run_test(|| async {
        let frank = Connection::open(":memory:").await.unwrap();
        let stock = rusqlite::Connection::open_in_memory().unwrap();
        body(frank, stock).await;

        let dir = tempfile::tempdir().unwrap();
        let frank_path = dir.path().join("frank.db");
        let stock_path = dir.path().join("stock.db");
        let frank = Connection::open(frank_path.to_str().unwrap()).await.unwrap();
        let stock = rusqlite::Connection::open(&stock_path).unwrap();
        body(frank, stock).await;
    });
}

#[test]
fn create_temp_table_as_select_lives_in_temp_schema() {
    for_each_backing(|frank, stock| async move {
        for sql in [
            "CREATE TABLE src(a INTEGER, b TEXT)",
            "INSERT INTO src VALUES (1, 'x'), (2, 'y'), (3, 'z')",
            "CREATE TEMP TABLE x AS SELECT * FROM src",
            "CREATE INDEX temp.ix ON x(a)",
            "CREATE TABLE temp.y AS SELECT a * 10 AS m FROM src WHERE a > 1",
            "CREATE TEMP TABLE IF NOT EXISTS x AS SELECT 99",
            // A main table may share the name of a temp table.
            "CREATE TABLE x AS SELECT b FROM src WHERE a = 1",
        ] {
            run_both(&frank, &stock, sql).await;
        }
        for sql in [
            "SELECT type, name, tbl_name FROM temp.sqlite_master ORDER BY name",
            "SELECT type, name, tbl_name FROM main.sqlite_master ORDER BY name",
            "SELECT * FROM x ORDER BY 1",
            "SELECT * FROM temp.x ORDER BY a",
            "SELECT * FROM main.x",
            "SELECT * FROM y ORDER BY m",
            "SELECT typeof(a), typeof(b) FROM temp.x ORDER BY a",
        ] {
            compare(&frank, &stock, sql).await;
        }
        let duplicate = "CREATE TEMP TABLE x AS SELECT 1";
        assert!(frank.execute(duplicate).await.is_err());
        assert!(stock.execute_batch(duplicate).is_err());
    });
}
