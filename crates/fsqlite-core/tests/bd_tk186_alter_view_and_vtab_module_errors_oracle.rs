//! bd-tk186: ALTER TABLE on a view, and CREATE VIRTUAL TABLE with a module
//! stock does not ship, fail with stock SQLite 3.54.1's text at stock's
//! time.
//!
//! - ALTER TABLE on a view is refused at prepare: RENAME COLUMN `cannot
//!   rename columns of view "v"`, DROP COLUMN `cannot drop column from view
//!   "v"` (the view's stored name), ADD COLUMN "Cannot add a column to a
//!   view", RENAME TO "view v may not be altered". fsqlite said "view v may
//!   not be altered" for all three, and only when the statement ran.
//! - CREATE VIRTUAL TABLE with a module stock does not ship prepares, and
//!   fails when it runs with "no such module: X". fsqlite said "not
//!   implemented: ... no module registered", which it keeps for a module
//!   stock ships that it has not implemented (fts3, geopoly; pinned by
//!   fsqlite-harness bd_t6sv2_14).
//!
//! Every statement runs on bundled SQLite 3.54.1 and on fsqlite, on `:memory:`
//! and on a file, executed directly and prepared then stepped. Each outcome
//! (rows, or the error text and whether prepare or the step raised it) must
//! equal stock's, and is pinned to stock's so the comparison cannot pass
//! vacuously.
//!
//! Negative cases a naive fix fails: an unqualified name that meets a TEMP
//! view, a `main.` and a `temp.` qualifier, and an upper-case name all name
//! the stored view; a missing column on a view is still the view's error;
//! an existing name is still refused at prepare before any module lookup,
//! and IF NOT EXISTS on it is still a no-op; nothing is created.

use fsqlite_core::connection::Connection;
use fsqlite_error::FrankenError;
use fsqlite_types::value::SqliteValue;

fn render_fsqlite(value: &SqliteValue) -> String {
    match value {
        SqliteValue::Null => "NULL".into(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f:?}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!("x{b:02x?}"),
    }
}

fn render_stock(value: &rusqlite::types::Value) -> String {
    match value {
        rusqlite::types::Value::Null => "NULL".into(),
        rusqlite::types::Value::Integer(n) => n.to_string(),
        rusqlite::types::Value::Real(f) => format!("{f:?}"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => format!("x{b:02x?}"),
    }
}

fn stock_message(error: &rusqlite::Error) -> String {
    match error {
        rusqlite::Error::SqliteFailure(_, Some(message))
        | rusqlite::Error::SqlInputError { msg: message, .. } => message.clone(),
        other => other.to_string(),
    }
}

/// Stock's outcome, prepared then stepped: rows, or which of the two failed.
fn stock_outcome(conn: &rusqlite::Connection, sql: &str) -> String {
    let mut statement = match conn.prepare(sql) {
        Ok(statement) => statement,
        Err(error) => return format!("prepare error: {}", stock_message(&error)),
    };
    let columns = statement.column_count();
    let mut cursor = statement.raw_query();
    let mut rows = Vec::new();
    loop {
        match cursor.next() {
            Ok(Some(row)) => rows.push(
                (0..columns)
                    .map(|i| render_stock(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
                    .collect::<Vec<_>>(),
            ),
            Ok(None) => return format!("rows={rows:?}"),
            Err(error) => return format!("step error: {}", stock_message(&error)),
        }
    }
}

fn render_rows(rows: &[fsqlite_core::connection::Row]) -> String {
    let rows: Vec<Vec<String>> = rows
        .iter()
        .map(|row| row.values().iter().map(render_fsqlite).collect())
        .collect();
    format!("rows={rows:?}")
}

async fn fsqlite_outcome(conn: &Connection, sql: &str, prepared: bool) -> String {
    let returns_rows = sql.starts_with("SELECT");
    if !prepared {
        let result = if returns_rows {
            conn.query(sql).await
        } else {
            conn.execute(sql).await.map(|_| Vec::new())
        };
        return match result {
            Ok(rows) => render_rows(&rows),
            Err(error) => format!("error: {error}"),
        };
    }
    let statement = match conn.prepare(sql).await {
        Ok(statement) => statement,
        Err(error) => return format!("prepare error: {error}"),
    };
    let result = if returns_rows {
        statement.query().await
    } else {
        statement.execute().await.map(|_| Vec::new())
    };
    match result {
        Ok(rows) => render_rows(&rows),
        Err(error) => format!("step error: {error}"),
    }
}

/// An outcome as a direct `execute` or `query` reports it, which cannot tell
/// prepare from step.
fn untimed(outcome: &str) -> String {
    outcome
        .strip_prefix("prepare ")
        .or_else(|| outcome.strip_prefix("step "))
        .unwrap_or(outcome)
        .to_owned()
}

const SETUP: &[&str] = &[
    "CREATE TABLE t(a, b)",
    "CREATE VIEW v AS SELECT a FROM t",
    "CREATE TEMP VIEW tv AS SELECT 1 AS x",
];

const STATEMENTS: &[(&str, &str)] = &[
    (
        "ALTER TABLE v RENAME COLUMN a TO b",
        "prepare error: cannot rename columns of view \"v\"",
    ),
    (
        "ALTER TABLE v DROP COLUMN a",
        "prepare error: cannot drop column from view \"v\"",
    ),
    (
        "ALTER TABLE v ADD COLUMN z",
        "prepare error: Cannot add a column to a view",
    ),
    (
        "ALTER TABLE v RENAME TO v2",
        "prepare error: view v may not be altered",
    ),
    (
        "ALTER TABLE V RENAME COLUMN a TO b",
        "prepare error: cannot rename columns of view \"v\"",
    ),
    (
        "ALTER TABLE main.v DROP COLUMN a",
        "prepare error: cannot drop column from view \"v\"",
    ),
    (
        "ALTER TABLE tv RENAME COLUMN x TO y",
        "prepare error: cannot rename columns of view \"tv\"",
    ),
    (
        "ALTER TABLE temp.tv DROP COLUMN x",
        "prepare error: cannot drop column from view \"tv\"",
    ),
    (
        "ALTER TABLE temp.tv RENAME TO tv2",
        "prepare error: view tv may not be altered",
    ),
    (
        "ALTER TABLE v RENAME COLUMN nosuch TO b",
        "prepare error: cannot rename columns of view \"v\"",
    ),
    (
        "SELECT type, name, sql FROM sqlite_master ORDER BY name",
        "rows=[[\"'table'\", \"'t'\", \"'CREATE TABLE t(a, b)'\"], \
         [\"'view'\", \"'v'\", \"'CREATE VIEW v AS SELECT a FROM t'\"]]",
    ),
    (
        "SELECT type, name FROM sqlite_temp_master ORDER BY name",
        "rows=[[\"'view'\", \"'tv'\"]]",
    ),
    (
        "CREATE VIRTUAL TABLE x USING nosuchmod(a)",
        "step error: no such module: nosuchmod",
    ),
    (
        "CREATE VIRTUAL TABLE x USING NoSuchMod",
        "step error: no such module: NoSuchMod",
    ),
    (
        "CREATE VIRTUAL TABLE temp.x USING nosuchmod(a)",
        "step error: no such module: nosuchmod",
    ),
    (
        "CREATE VIRTUAL TABLE IF NOT EXISTS x USING nosuchmod(a)",
        "step error: no such module: nosuchmod",
    ),
    (
        "CREATE VIRTUAL TABLE t USING nosuchmod(a)",
        "prepare error: table t already exists",
    ),
    ("CREATE VIRTUAL TABLE IF NOT EXISTS t USING nosuchmod(a)", "rows=[]"),
    (
        "CREATE VIRTUAL TABLE v USING nosuchmod(a)",
        "prepare error: view v already exists",
    ),
    (
        "SELECT count(*) FROM sqlite_master WHERE name = 'x'",
        "rows=[[\"0\"]]",
    ),
];

#[test]
fn alter_on_a_view_and_an_unknown_vtab_module_fail_like_stock() {
    let stock = rusqlite::Connection::open_in_memory().unwrap();
    for sql in SETUP {
        stock.execute_batch(sql).unwrap();
    }
    // Not vacuous: stock gives each pinned outcome.
    for (sql, pinned) in STATEMENTS {
        assert_eq!(stock_outcome(&stock, sql), *pinned, "stock on `{sql}`");
    }
    asupersync::test_utils::run_test(|| async {
        let mut mismatches = Vec::new();
        for file_backed in [false, true] {
            for prepared in [false, true] {
                let dir = tempfile::tempdir().unwrap();
                let path = if file_backed {
                    dir.path().join("bd_tk186.db").to_str().unwrap().to_owned()
                } else {
                    ":memory:".to_owned()
                };
                let conn = Connection::open(&path).await.unwrap();
                for sql in SETUP {
                    conn.execute(sql).await.unwrap();
                }
                for (sql, pinned) in STATEMENTS {
                    let want = if prepared {
                        (*pinned).to_owned()
                    } else {
                        untimed(pinned)
                    };
                    let got = fsqlite_outcome(&conn, sql, prepared).await;
                    if got != want {
                        mismatches.push(format!(
                            "file_backed={file_backed} prepared={prepared} `{sql}`:\n    \
                             got  {got}\n    want {want}"
                        ));
                    }
                }
            }
        }
        assert!(
            mismatches.is_empty(),
            "{} outcome(s) differ from stock:\n{}",
            mismatches.len(),
            mismatches.join("\n")
        );
    });
}

/// A module stock ships that fsqlite has not implemented keeps fsqlite's
/// "not implemented" (fsqlite-harness bd_t6sv2_14 pins its text), not "no
/// such module".
#[test]
fn a_stock_module_fsqlite_lacks_is_still_not_implemented() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        for sql in [
            "CREATE VIRTUAL TABLE f3 USING fts3(content)",
            "CREATE VIRTUAL TABLE f4 USING FTS4(content)",
            "CREATE VIRTUAL TABLE g USING geopoly(_shape)",
        ] {
            let error = conn.execute(sql).await.unwrap_err();
            assert!(
                matches!(&error, FrankenError::NotImplemented(message) if message.contains("no module registered")),
                "`{sql}`: {error:?}"
            );
        }
    });
}
