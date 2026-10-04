//! A transaction opened by an outermost `SAVEPOINT` can commit a schema change.
//!
//! The implicit begin did not sample the in-process write generation the way
//! `BEGIN` and the autocommit begin do. The sample left over from the previous
//! transaction made this connection's own earlier write commit look like a
//! peer commit since begin, so `CREATE TABLE t ...; SAVEPOINT s; CREATE INDEX
//! ...; RELEASE s` failed with `BusySnapshot` ("database is busy (snapshot
//! conflict on pages: )"), in memory and file-backed, since at least 0.4.7.
//! Migration tools and ORMs wrap DDL in exactly this pattern.
//!
//! Each script runs on FrankenSQLite (in memory and file-backed) and on stock
//! SQLite (rusqlite); every statement's outcome and the final schema and rows
//! must match, and the file must pass stock `integrity_check`.

use fsqlite_core::connection::Connection;

const SCRIPTS: [&[&str]; 8] = [
    &[
        "CREATE TABLE t1 (id INTEGER PRIMARY KEY, a INT, b TEXT, c BLOB)",
        "SAVEPOINT sp0",
        "CREATE UNIQUE INDEX ix_t1_b ON t1(b)",
        "RELEASE sp0",
    ],
    &[
        "CREATE TABLE t1 (id INTEGER PRIMARY KEY, b)",
        "SAVEPOINT s",
        "CREATE INDEX ix ON t1(b)",
        "RELEASE s",
    ],
    &[
        "CREATE TABLE t1 (id INTEGER PRIMARY KEY, b)",
        "SAVEPOINT s",
        "CREATE TABLE t2 (x)",
        "RELEASE s",
    ],
    &[
        "CREATE TABLE t1 (id INTEGER PRIMARY KEY, b)",
        "INSERT INTO t1 VALUES (1, 2), (2, 3)",
        "SAVEPOINT s",
        "CREATE UNIQUE INDEX ix ON t1(b)",
        "RELEASE s",
    ],
    &[
        "CREATE TABLE t1 (id INTEGER PRIMARY KEY, b)",
        "INSERT INTO t1 VALUES (1, 2)",
        "SAVEPOINT outer_sp",
        "INSERT INTO t1 VALUES (2, 3)",
        "SAVEPOINT inner_sp",
        "ALTER TABLE t1 ADD COLUMN c DEFAULT 5",
        "RELEASE inner_sp",
        "CREATE INDEX ix ON t1(c, b)",
        "RELEASE outer_sp",
    ],
    &[
        "CREATE TABLE t1 (id INTEGER PRIMARY KEY, b)",
        "SAVEPOINT s",
        "CREATE TABLE t2 (x)",
        "ROLLBACK TO s",
        "CREATE TABLE t3 (y)",
        "RELEASE s",
    ],
    &[
        "CREATE TABLE t1 (id INTEGER PRIMARY KEY, b)",
        "SAVEPOINT s",
        "DROP TABLE t1",
        "RELEASE s",
        "SAVEPOINT s2",
        "CREATE TABLE t1 (z)",
        "RELEASE s2",
    ],
    // Repeated cycles: each implicit begin must take a fresh sample.
    &[
        "CREATE TABLE t1 (id INTEGER PRIMARY KEY, b)",
        "SAVEPOINT a",
        "CREATE INDEX i1 ON t1(b)",
        "RELEASE a",
        "INSERT INTO t1 VALUES (1, 1)",
        "SAVEPOINT b",
        "CREATE TABLE t2 (x)",
        "RELEASE b",
        "SAVEPOINT c",
        "DROP INDEX i1",
        "RELEASE c",
    ],
];

const CHECKS: [&str; 2] = [
    "SELECT type, name, tbl_name, sql FROM sqlite_master ORDER BY name",
    "SELECT * FROM t1 ORDER BY 1",
];

async fn frank_rows(conn: &Connection, sql: &str) -> Result<Vec<String>, String> {
    conn.query(sql)
        .await
        .map(|rows| {
            rows.iter()
                .map(|row| format!("{:?}", row.values()))
                .collect()
        })
        .map_err(|e| e.to_string())
}

fn stock_rows(conn: &rusqlite::Connection, sql: &str) -> Result<Vec<String>, String> {
    let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
    let width = stmt.column_count();
    stmt.query_map([], |row| {
        let values = (0..width)
            .map(|i| row.get::<_, rusqlite::types::Value>(i))
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(values
            .into_iter()
            .map(|value| match value {
                rusqlite::types::Value::Null => fsqlite_types::SqliteValue::Null,
                rusqlite::types::Value::Integer(v) => fsqlite_types::SqliteValue::Integer(v),
                rusqlite::types::Value::Real(v) => fsqlite_types::SqliteValue::Float(v),
                rusqlite::types::Value::Text(v) => fsqlite_types::SqliteValue::from(v.as_str()),
                rusqlite::types::Value::Blob(v) => fsqlite_types::SqliteValue::Blob(v.into()),
            })
            .collect::<Vec<_>>())
    })
    .map_err(|e| e.to_string())?
    .map(|row| {
        row.map(|values| format!("{values:?}"))
            .map_err(|e| e.to_string())
    })
    .collect()
}

#[test]
fn schema_changes_inside_an_implicit_savepoint_transaction_commit() {
    asupersync::test_utils::run_test(|| async {
        let mut failures = Vec::new();
        for (index, script) in SCRIPTS.iter().enumerate() {
            for file_backed in [false, true] {
                let label = format!("script {index} file={file_backed}");
                let dir = tempfile::tempdir().expect("temp dir");
                let path = dir.path().join("frank.db");
                let frank = if file_backed {
                    Connection::open(path.to_str().expect("utf-8 path"))
                        .await
                        .expect("open")
                } else {
                    Connection::open(":memory:").await.expect("open")
                };
                let stock = rusqlite::Connection::open_in_memory().expect("stock open");
                for sql in *script {
                    let f = frank.execute(sql).await;
                    let s = stock.execute_batch(sql);
                    if f.is_ok() != s.is_ok() {
                        failures.push(format!(
                            "{label} `{sql}`: FrankenSQLite {f:?} vs SQLite {s:?}"
                        ));
                    }
                }
                for check in CHECKS {
                    let (f, s) = (frank_rows(&frank, check).await, stock_rows(&stock, check));
                    if f != s {
                        failures.push(format!("{label} `{check}`:\n  frank {f:?}\n  stock {s:?}"));
                    }
                }
                frank.close().await.expect("close");
                if file_backed {
                    let checked = rusqlite::Connection::open(&path).expect("stock open");
                    let verdict: String = checked
                        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
                        .unwrap_or_else(|e| e.to_string());
                    if verdict != "ok" {
                        failures.push(format!("{label} stock integrity_check: {verdict}"));
                    }
                }
            }
        }
        assert!(failures.is_empty(), "{}", failures.join("\n"));
    });
}
