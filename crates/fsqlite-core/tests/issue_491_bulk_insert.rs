#![recursion_limit = "512"]
#![cfg(feature = "ext-misc")]

//! Correctness guardrails for GH#491's indexed INSERT ... SELECT hot path.
//! INT PRIMARY KEY is deliberately NOT replaced with the rowid-alias spelling.
//! Stock uses a materialized integer source because bundled rusqlite does not
//! enable the shell's generate_series module; this is a correctness oracle,
//! not an apples-to-apples generator performance comparison.
//!
//! cargo test --locked -p fsqlite-core --test issue_491_bulk_insert -- --nocapture

use std::path::Path;

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;
use rusqlite::{Connection as Stock, OpenFlags};

type Rows = Vec<Vec<Option<i64>>>;

fn stock_rows(db: &Stock, sql: &str) -> Rows {
    let mut statement = db.prepare(sql).unwrap();
    let columns = statement.column_count();
    statement
        .query_map([], |row| (0..columns).map(|column| row.get(column)).collect())
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap()
}

async fn assert_rows(db: &Connection, stock: &Stock, sql: &str) {
    let actual: Rows = db
        .query(sql)
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
        .iter()
        .map(|row| {
            row.values()
                .iter()
                .map(|value| match value {
                    SqliteValue::Null => None,
                    SqliteValue::Integer(value) => Some(*value),
                    other => panic!("unexpected value in integer fixture: {other:?}"),
                })
                .collect()
        })
        .collect();
    assert_eq!(actual, stock_rows(stock, sql), "{sql}");
}

async fn execute_both(db: &Connection, stock: &Stock, sql: &str) {
    stock.execute_batch(sql).unwrap();
    db.execute(sql)
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error}"));
}

const CHECKS: &[&str] = &[
    "SELECT rowid,a,b,c FROM t NOT INDEXED ORDER BY rowid",
    "SELECT rowid,a,b,c FROM t INDEXED BY by_b ORDER BY b,a,rowid",
    "SELECT rowid,a,b,c FROM t INDEXED BY by_c ORDER BY c,rowid",
    "SELECT rowid,a,b,c FROM t INDEXED BY by_b WHERE b=8 ORDER BY a,rowid",
    "SELECT rowid,a,b,c FROM t INDEXED BY by_c WHERE c>=9900 ORDER BY c,rowid",
];

async fn assert_all(db: &Connection, stock: &Stock) {
    for sql in CHECKS {
        assert_rows(db, stock, sql).await;
    }
}

async fn rejected_statement(db: &Connection, stock: &Stock, sql: &str) {
    let before = stock_rows(stock, CHECKS[0]);
    let stock_error = stock.execute_batch(sql).expect_err("stock must reject duplicate");
    assert_eq!(stock_error.sqlite_error_code(), Some(rusqlite::ErrorCode::ConstraintViolation));
    let error = db.execute(sql).await.expect_err("FrankenSQLite must reject duplicate");
    assert!(error.to_string().to_ascii_lowercase().contains("constraint"), "{sql}: {error}");
    // Check the oracle's statement atomicity too, rather than accepting matching
    // partial writes by accident. The surrounding transaction must remain usable.
    assert_eq!(stock_rows(stock, CHECKS[0]), before);
    assert_all(db, stock).await;
}

async fn indexed_insert_oracle(path: Option<&Path>) {
    let name = path.map_or_else(|| ":memory:".to_owned(), |p| p.to_string_lossy().into_owned());
    let db = Connection::open(&name).await.unwrap();
    let stock = Stock::open_in_memory().unwrap();
    if path.is_some() {
        // Do not turn off concurrency, fsync, or the WAL to obtain a faster run.
        db.execute("PRAGMA journal_mode=WAL").await.unwrap();
    }
    for sql in [
        "CREATE TABLE source(value INTEGER PRIMARY KEY)",
        "CREATE TABLE t(a INT PRIMARY KEY,b INT,c INT)",
        "CREATE INDEX by_b ON t(b,a)",
        "CREATE UNIQUE INDEX by_c ON t(c)",
    ] {
        execute_both(&db, &stock, sql).await;
    }
    stock.execute_batch(
        "WITH RECURSIVE s(value) AS (VALUES(1) UNION ALL SELECT value+1 FROM s WHERE value<4096) INSERT INTO source SELECT value FROM s",
    ).unwrap();
    db.execute("INSERT INTO source SELECT value FROM generate_series(1,4096)")
        .await.unwrap();
    assert_rows(&db, &stock, "SELECT value FROM source ORDER BY value").await;
    // Ascending primary keys, repeated nonunique keys, descending unique keys:
    // a rightmost-only shortcut must not leak to the other two index cursors.
    stock.execute_batch("INSERT INTO t SELECT value,value%17,10000-value FROM source").unwrap();
    db.execute("INSERT INTO t SELECT value,value%17,10000-value FROM generate_series(1,4096)")
        .await.unwrap();
    assert_all(&db, &stock).await;
    execute_both(&db, &stock, "INSERT INTO t VALUES(NULL,9,NULL),(NULL,9,NULL)").await;
    assert_all(&db, &stock).await;

    execute_both(&db, &stock, "BEGIN").await;
    execute_both(&db, &stock, "INSERT INTO t VALUES(5000,3,5000)").await;
    rejected_statement(&db, &stock,
        "INSERT INTO t SELECT CASE WHEN value=3 THEN 2 ELSE value+6999 END,8,value+20000 FROM source WHERE value<=5",
    ).await;
    execute_both(&db, &stock, "COMMIT").await;
    assert_all(&db, &stock).await;

    execute_both(&db, &stock, "BEGIN").await;
    execute_both(&db, &stock, "INSERT INTO t VALUES(6000,5,60000)").await;
    rejected_statement(&db, &stock,
        "INSERT INTO t SELECT value+8000,8,CASE WHEN value=3 THEN 9999 ELSE value+30000 END FROM source WHERE value<=5",
    ).await;
    execute_both(&db, &stock, "ROLLBACK").await;
    assert_all(&db, &stock).await;

    execute_both(&db, &stock, "SAVEPOINT bulk").await;
    execute_both(&db, &stock,
        "INSERT INTO t SELECT value+10000,value%17,value+40000 FROM source WHERE value<=100",
    ).await;
    assert_all(&db, &stock).await;
    execute_both(&db, &stock, "ROLLBACK TO bulk").await;
    execute_both(&db, &stock, "RELEASE bulk").await;
    assert_all(&db, &stock).await;
    execute_both(&db, &stock,
        "INSERT OR IGNORE INTO t SELECT CASE WHEN value<=10 THEN value ELSE value+20000 END,value%17,value+60000 FROM source WHERE value<=20",
    ).await;
    assert_all(&db, &stock).await;
    // This conflicts with TWO existing rows: the PK of a=1 and the c key of
    // a=2. REPLACE must remove both old rows and every old secondary entry.
    execute_both(&db, &stock, "INSERT OR REPLACE INTO t VALUES(1,11,9998)").await;
    assert_all(&db, &stock).await;
    assert_eq!(stock_rows(&stock, "SELECT count(*) FROM t"), vec![vec![Some(4108)]]);

    if let Some(path) = path {
        let checkpoint = db.query("PRAGMA wal_checkpoint(TRUNCATE)").await.unwrap();
        assert_eq!(checkpoint.len(), 1);
        assert!(matches!(checkpoint[0].values().first(), Some(SqliteValue::Integer(0))));
        drop(db);
        // Read stock BEFORE FrankenSQLite reopens; do not let engine recovery
        // or a later index build conceal an incompatible persisted index.
        let persisted = Stock::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
        let integrity: Vec<String> = persisted.prepare("PRAGMA integrity_check").unwrap()
            .query_map([], |row| row.get(0)).unwrap()
            .collect::<rusqlite::Result<_>>().unwrap();
        assert_eq!(integrity, vec!["ok".to_owned()]);
        for sql in CHECKS {
            assert_eq!(stock_rows(&persisted, sql), stock_rows(&stock, sql), "persisted: {sql}");
        }
        drop(persisted);
        let reopened = Connection::open(&name).await.unwrap();
        assert_all(&reopened, &stock).await;
    }
}

#[test]
fn indexed_insert_select_in_memory_matches_stock() {
    asupersync::test_utils::run_test(|| async { indexed_insert_oracle(None).await });
}

#[test]
fn indexed_insert_select_wal_file_matches_stock() {
    asupersync::test_utils::run_test(|| async {
        let directory = tempfile::tempdir().unwrap();
        indexed_insert_oracle(Some(&directory.path().join("issue491.db"))).await;
    });
}

#[test]
fn int_primary_key_is_not_an_integer_primary_key_alias() {
    asupersync::test_utils::run_test(|| async {
        let db = Connection::open(":memory:").await.unwrap();
        let stock = Stock::open_in_memory().unwrap();
        for sql in [
            "CREATE TABLE indexed(a INT PRIMARY KEY)",
            "CREATE TABLE aliased(a INTEGER PRIMARY KEY)",
            "INSERT INTO indexed VALUES(42),(NULL),(NULL)",
            "INSERT INTO aliased VALUES(42),(NULL),(NULL)",
        ] {
            execute_both(&db, &stock, sql).await;
        }
        for sql in [
            "SELECT rowid,a FROM indexed ORDER BY rowid",
            "SELECT rowid,a FROM aliased ORDER BY rowid",
        ] {
            assert_rows(&db, &stock, sql).await;
        }
        assert_eq!(stock_rows(&stock, "SELECT rowid,a FROM indexed ORDER BY rowid"),
            vec![vec![Some(1), Some(42)], vec![Some(2), None], vec![Some(3), None]]);
        assert_eq!(stock_rows(&stock, "SELECT rowid,a FROM aliased ORDER BY rowid"),
            vec![vec![Some(42), Some(42)], vec![Some(43), Some(43)], vec![Some(44), Some(44)]]);
    });
}
