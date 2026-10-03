#![recursion_limit = "512"]

//! bd-9zuif: a statement prepared before ANOTHER connection's DDL re-prepares
//! transparently, as `sqlite3_prepare_v2` statements do, instead of failing
//! with `SchemaChanged`. Same-connection DDL already re-prepared (GH #239).
//!
//! Stock SQLite's `sqlite3_step` sees the new schema cookie, recompiles the
//! original SQL and runs it, binding the same parameters. If the statement no
//! longer compiles (a dropped table or column) it returns that real error.
//! rusqlite inherits this, so it is the oracle: each scenario runs on a
//! file-backed database with two FrankenSQLite connections and, separately,
//! two rusqlite connections, and the results must agree.
//!
//! Inside an explicit transaction a cross-connection schema change still
//! surfaces `SchemaChanged` (the transaction already read under the old
//! schema); the statement re-prepares once the transaction ends.

use fsqlite_core::connection::Connection;
use fsqlite_error::FrankenError;
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

fn rusqlite_params(params: &[SqliteValue]) -> Vec<Value> {
    params
        .iter()
        .map(|p| match p {
            SqliteValue::Null => Value::Null,
            SqliteValue::Integer(v) => Value::Integer(*v),
            SqliteValue::Float(v) => Value::Real(*v),
            SqliteValue::Text(v) => Value::Text(v.to_string()),
            SqliteValue::Blob(v) => Value::Blob(v.to_vec()),
        })
        .collect()
}

fn stock_query(
    stmt: &mut rusqlite::Statement<'_>,
    params: &[SqliteValue],
) -> Result<Vec<Vec<SqliteValue>>, String> {
    let params = rusqlite_params(params);
    let mut rows = stmt
        .query(rusqlite::params_from_iter(params.iter()))
        .map_err(|e| e.to_string())?;
    let mut out = Vec::new();
    while let Some(row) = rows.next().map_err(|e| e.to_string())? {
        // Read the width from the row: a transparent re-prepare can widen it.
        let width = row.as_ref().column_count();
        out.push(
            (0..width)
                .map(|i| row.get::<_, Value>(i).map(value))
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(|e| e.to_string())?,
        );
    }
    Ok(out)
}

fn frank_error_text(error: &FrankenError) -> String {
    error.to_string()
}

/// One scenario: `setup`, prepare `stmt_sql` on the second connection, run it
/// once, apply `ddl` on the first connection, then run the stale statement
/// twice more (with `params`), comparing every outcome with stock.
struct Scenario {
    name: &'static str,
    setup: &'static str,
    stmt_sql: &'static str,
    params: &'static [i64],
    ddl: &'static str,
    /// Query read back after the statement (through the second connection).
    check: &'static str,
}

const SCENARIOS: [Scenario; 9] = [
    Scenario {
        name: "select_star_after_add_column",
        setup: "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT); INSERT INTO t VALUES (1,'x'),(2,'y');",
        stmt_sql: "SELECT * FROM t WHERE a >= ?1 ORDER BY a",
        params: &[1],
        ddl: "ALTER TABLE t ADD COLUMN c DEFAULT 'fresh'",
        check: "SELECT * FROM t ORDER BY a",
    },
    Scenario {
        name: "select_columns_after_create_index",
        setup: "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT); INSERT INTO t VALUES (1,'x'),(2,'y');",
        stmt_sql: "SELECT a FROM t WHERE b = 'y'",
        params: &[],
        ddl: "CREATE INDEX t_b ON t(b)",
        check: "SELECT a, b FROM t ORDER BY a",
    },
    Scenario {
        name: "select_after_unrelated_create_table",
        setup: "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT); INSERT INTO t VALUES (1,'x');",
        stmt_sql: "SELECT b FROM t WHERE a = ?1",
        params: &[1],
        ddl: "CREATE TABLE other(z)",
        check: "SELECT name FROM sqlite_master ORDER BY name",
    },
    Scenario {
        name: "insert_after_add_column_default",
        setup: "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT);",
        stmt_sql: "INSERT INTO t(b) VALUES ('row' || ?1)",
        params: &[7],
        ddl: "ALTER TABLE t ADD COLUMN c DEFAULT 42",
        check: "SELECT * FROM t ORDER BY a",
    },
    Scenario {
        name: "update_after_add_column",
        setup: "CREATE TABLE t(a INTEGER PRIMARY KEY, b INTEGER); INSERT INTO t VALUES (1,10),(2,20);",
        stmt_sql: "UPDATE t SET b = b + 1 WHERE a = ?1",
        params: &[2],
        ddl: "ALTER TABLE t ADD COLUMN c",
        check: "SELECT * FROM t ORDER BY a",
    },
    Scenario {
        name: "delete_after_create_index",
        setup: "CREATE TABLE t(a INTEGER PRIMARY KEY, b INTEGER); \
                INSERT INTO t VALUES (1,10),(2,20),(3,30),(4,40);",
        stmt_sql: "DELETE FROM t WHERE a = ?1",
        params: &[1],
        ddl: "CREATE INDEX t_b ON t(b)",
        check: "SELECT * FROM t ORDER BY a",
    },
    Scenario {
        name: "select_after_drop_table",
        setup: "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT); INSERT INTO t VALUES (1,'x');",
        stmt_sql: "SELECT b FROM t WHERE a = ?1",
        params: &[1],
        ddl: "DROP TABLE t",
        check: "SELECT count(*) FROM sqlite_master",
    },
    Scenario {
        name: "select_after_column_vanishes",
        setup: "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT); INSERT INTO t VALUES (1,'x');",
        stmt_sql: "SELECT b FROM t WHERE a = ?1",
        params: &[1],
        ddl: "DROP TABLE t; CREATE TABLE t(a INTEGER PRIMARY KEY, z TEXT); \
              INSERT INTO t VALUES (1,'z')",
        check: "SELECT * FROM t",
    },
    Scenario {
        name: "select_after_rename_column",
        setup: "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT); INSERT INTO t VALUES (1,'x');",
        stmt_sql: "SELECT * FROM t WHERE a = ?1",
        params: &[1],
        ddl: "ALTER TABLE t RENAME COLUMN b TO bb",
        check: "SELECT * FROM t",
    },
];

/// Errors are compared by their stock message; FrankenSQLite's `Display` may
/// add context, so the stock text must appear in it.
fn outcomes_agree(
    frank: &Result<Vec<Vec<SqliteValue>>, String>,
    stock: &Result<Vec<Vec<SqliteValue>>, String>,
) -> bool {
    match (frank, stock) {
        (Ok(f), Ok(s)) => f == s,
        (Err(f), Err(s)) => f.contains(s.as_str()) || s.contains(f.as_str()),
        _ => false,
    }
}

async fn run_scenario(scenario: &Scenario, failures: &mut Vec<String>) {
    let dir = tempfile::tempdir().expect("temp dir");
    let frank_path = dir.path().join("frank.db");
    let frank_path = frank_path.to_str().expect("utf-8").to_owned();
    let stock_path = dir.path().join("stock.db");

    let frank_a = Connection::open(&frank_path).await.expect("open a");
    frank_a
        .execute_batch(scenario.setup)
        .await
        .expect("frank setup");
    let frank_b = Connection::open(&frank_path).await.expect("open b");

    let stock_a = rusqlite::Connection::open(&stock_path).expect("stock a");
    stock_a
        .execute_batch("PRAGMA journal_mode=WAL;")
        .expect("stock wal");
    stock_a.execute_batch(scenario.setup).expect("stock setup");
    let stock_b = rusqlite::Connection::open(&stock_path).expect("stock b");

    let params: Vec<SqliteValue> = scenario
        .params
        .iter()
        .map(|p| SqliteValue::Integer(*p))
        .collect();
    let frank_stmt = frank_b
        .prepare(scenario.stmt_sql)
        .await
        .expect("frank prepare");
    let mut stock_stmt = stock_b.prepare(scenario.stmt_sql).expect("stock prepare");

    for round in 0..3 {
        if round == 1 {
            frank_a
                .execute_batch(scenario.ddl)
                .await
                .expect("frank ddl");
            stock_a.execute_batch(scenario.ddl).expect("stock ddl");
        }
        // DML runs through `execute*` on both engines and is compared by its
        // change count; FrankenSQLite's `query*` refuses DML statements.
        let is_query = scenario.stmt_sql.starts_with("SELECT");
        let (frank, stock) = if is_query {
            (
                frank_stmt
                    .query_with_params(&params)
                    .await
                    .map(|rows| rows.iter().map(|row| row.values().to_vec()).collect())
                    .map_err(|e| frank_error_text(&e)),
                stock_query(&mut stock_stmt, &params),
            )
        } else {
            let count = |n: usize| vec![vec![SqliteValue::Integer(i64::try_from(n).unwrap())]];
            (
                frank_stmt
                    .execute_with_params(&params)
                    .await
                    .map(count)
                    .map_err(|e| frank_error_text(&e)),
                stock_stmt
                    .execute(rusqlite::params_from_iter(rusqlite_params(&params).iter()))
                    .map(count)
                    .map_err(|e| e.to_string()),
            )
        };
        if !outcomes_agree(&frank, &stock) {
            failures.push(format!(
                "[{}] round {round}: FrankenSQLite {frank:?} vs SQLite {stock:?}",
                scenario.name
            ));
        }
        let frank_check = frank_b
            .query(scenario.check)
            .await
            .map(|rows| rows.iter().map(|row| row.values().to_vec()).collect())
            .map_err(|e| frank_error_text(&e));
        let mut check_stmt = stock_b.prepare(scenario.check).expect("stock check prepare");
        let stock_check = stock_query(&mut check_stmt, &[]);
        if !outcomes_agree(&frank_check, &stock_check) {
            failures.push(format!(
                "[{}] round {round} `{}`: FrankenSQLite {frank_check:?} vs SQLite {stock_check:?}",
                scenario.name, scenario.check
            ));
        }
    }
}

#[test]
fn stale_statements_reprepare_after_another_connections_ddl_like_stock() {
    asupersync::test_utils::run_test(|| async {
        let mut failures = Vec::new();
        for scenario in &SCENARIOS {
            run_scenario(scenario, &mut failures).await;
        }
        assert!(
            failures.is_empty(),
            "{} mismatches:\n{}",
            failures.len(),
            failures.join("\n")
        );
    });
}

/// Every public execution entry point re-prepares, and `execute` reports the
/// changed row count through the re-prepared statement.
#[test]
fn every_prepared_entry_point_reprepares_after_cross_connection_ddl() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("entry.db");
        let path = path.to_str().expect("utf-8").to_owned();
        let a = Connection::open(&path).await.expect("open a");
        a.execute_batch(
            "CREATE TABLE t(a INTEGER PRIMARY KEY, b INTEGER);
             INSERT INTO t VALUES (1, 10), (2, 20);",
        )
        .await
        .expect("setup");
        let b = Connection::open(&path).await.expect("open b");
        let select = b.prepare("SELECT b FROM t ORDER BY a").await.expect("prep");
        let select_one = b
            .prepare("SELECT b FROM t WHERE a = ?1")
            .await
            .expect("prep");
        let count = b.prepare("SELECT count(*) FROM t").await.expect("prep");
        let update = b
            .prepare("UPDATE t SET b = b + 1 WHERE a = ?1")
            .await
            .expect("prep");
        let insert = b
            .prepare("INSERT INTO t(b) VALUES (99)")
            .await
            .expect("prep");

        a.execute("ALTER TABLE t ADD COLUMN c DEFAULT 5")
            .await
            .expect("ddl");

        let rows = select.query().await.expect("query");
        assert_eq!(rows.len(), 2);
        let row = select_one
            .query_row_with_params(&[SqliteValue::Integer(2)])
            .await
            .expect("query_row_with_params");
        assert_eq!(row.values(), &[SqliteValue::Integer(20)]);
        let row = count.query_row().await.expect("query_row");
        assert_eq!(row.values(), &[SqliteValue::Integer(2)]);
        let mut seen = Vec::new();
        select
            .query_with_params_for_each(&[], |row| {
                seen.push(row.values().to_vec());
                Ok(())
            })
            .await
            .expect("for_each");
        assert_eq!(seen.len(), 2);
        assert_eq!(
            update
                .execute_with_params(&[SqliteValue::Integer(1)])
                .await
                .expect("execute_with_params"),
            1
        );
        assert_eq!(insert.execute().await.expect("execute"), 1);
        let rows = b
            .query("SELECT a, b, c FROM t ORDER BY a")
            .await
            .expect("read back");
        let values: Vec<Vec<SqliteValue>> = rows.iter().map(|r| r.values().to_vec()).collect();
        assert_eq!(
            values,
            vec![
                vec![
                    SqliteValue::Integer(1),
                    SqliteValue::Integer(11),
                    SqliteValue::Integer(5)
                ],
                vec![
                    SqliteValue::Integer(2),
                    SqliteValue::Integer(20),
                    SqliteValue::Integer(5)
                ],
                vec![
                    SqliteValue::Integer(3),
                    SqliteValue::Integer(99),
                    SqliteValue::Integer(5)
                ],
            ]
        );
    });
}

/// Inside an explicit transaction the cross-connection change still reports
/// `SchemaChanged` (and does not spin); after the transaction ends the same
/// statement re-prepares.
#[test]
fn explicit_transaction_keeps_schema_changed_then_reprepares_after_commit() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("txn.db");
        let path = path.to_str().expect("utf-8").to_owned();
        let a = Connection::open(&path).await.expect("open a");
        a.execute_batch("CREATE TABLE t(a INTEGER PRIMARY KEY, b); INSERT INTO t VALUES (1, 'x');")
            .await
            .expect("setup");
        let b = Connection::open(&path).await.expect("open b");
        let stmt = b
            .prepare("SELECT b FROM t WHERE a = ?1")
            .await
            .expect("prep");
        b.execute("BEGIN").await.expect("begin");
        assert_eq!(
            stmt.query_with_params(&[SqliteValue::Integer(1)])
                .await
                .expect("read in txn")
                .len(),
            1
        );
        a.execute("ALTER TABLE t ADD COLUMN c").await.expect("ddl");
        let in_txn = stmt.query_with_params(&[SqliteValue::Integer(1)]).await;
        assert!(
            matches!(&in_txn, Ok(rows) if rows.len() == 1)
                || matches!(in_txn, Err(FrankenError::SchemaChanged)),
            "in-transaction outcome must be the old snapshot or SchemaChanged: {in_txn:?}"
        );
        b.execute("COMMIT").await.expect("commit");
        let rows = stmt
            .query_with_params(&[SqliteValue::Integer(1)])
            .await
            .expect("re-prepares after the transaction");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values(), &[SqliteValue::from("x")]);
    });
}
