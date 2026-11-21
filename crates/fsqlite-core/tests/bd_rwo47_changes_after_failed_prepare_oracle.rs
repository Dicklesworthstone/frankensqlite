//! bd-rwo47: a statement that fails while stock prepares it never runs, so
//! stock leaves `changes()`, `total_changes()` and `last_insert_rowid()` as
//! the previous statement set them; it sets `changes()` to 0 only when a
//! statement that ran fails. fsqlite reset `changes()` to 0 after every
//! failed INSERT, UPDATE or DELETE.
//!
//! The statements run in order on bundled SQLite 3.54.1 and on fsqlite, on
//! `:memory:` and on a file, directly and prepared. After each one, both
//! engines' `SELECT changes(), total_changes(), last_insert_rowid()` must
//! agree, and so must the statement's own outcome.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

/// Each statement, with the counters stock reads after it
/// (`changes|total_changes|last_insert_rowid`), pinned so that the
/// comparison cannot pass with both engines doing the same wrong thing.
const SEQUENCE: &[(&str, &str)] = &[
    ("CREATE TABLE w(x, y)", "0|0|0"),
    ("INSERT INTO w VALUES (1, 2)", "1|1|1"),
    // Prepare-time failures keep the previous counts.
    ("UPDATE nosuch SET x = 1", "1|1|1"),
    ("UPDATE w SET (x, nope) = (SELECT 1, 2, 3)", "1|1|1"),
    ("INSERT INTO w VALUES (3, 4), (5, 6)", "2|3|3"),
    ("UPDATE w SET nope = 1", "2|3|3"),
    ("DELETE FROM nosuch", "2|3|3"),
    ("INSERT INTO nosuch VALUES (1)", "2|3|3"),
    ("INSERT INTO w VALUES (1, 2, 3)", "2|3|3"),
    ("INSERT INTO w(nope) VALUES (1)", "2|3|3"),
    ("DELETE FROM w WHERE nope = 1", "2|3|3"),
    ("UPDATE w SET x = nofunc(1)", "2|3|3"),
    ("UPDATE w SET x = abs(1, 2)", "2|3|3"),
    ("INSERT INTO w SELECT * FROM nosuch", "2|3|3"),
    ("UPDATE w SET x = (SELECT 1, 2)", "2|3|3"),
    ("SELEC 1", "2|3|3"),
    // Non-DML statements leave them too.
    ("SELECT * FROM w", "2|3|3"),
    ("CREATE TABLE u(a UNIQUE)", "2|3|3"),
    // A successful DML sets them; one that matches no row sets 0.
    ("UPDATE w SET x = x + 1", "3|6|3"),
    ("UPDATE w SET x = 0 WHERE 0", "0|6|3"),
    ("INSERT INTO u VALUES (1), (2), (3)", "3|9|3"),
    // A run-time failure part-way through a multi-row UPDATE rolls the
    // statement back and sets 0.
    ("UPDATE u SET a = 2 WHERE a >= 1", "0|9|3"),
    ("INSERT INTO w VALUES (9, 9)", "1|10|4"),
    ("INSERT INTO u VALUES (7), (1)", "0|10|4"),
    // A prepare-time failure after a run-time one keeps the 0.
    ("UPDATE nosuch SET x = 1", "0|10|4"),
    ("INSERT INTO w VALUES (8, 8)", "1|11|5"),
    ("INSERT INTO nosuch VALUES (1)", "1|11|5"),
];

fn stock_counters(conn: &rusqlite::Connection) -> String {
    conn.query_row(
        "SELECT changes(), total_changes(), last_insert_rowid()",
        [],
        |row| {
            Ok(format!(
                "{}|{}|{}",
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?
            ))
        },
    )
    .unwrap()
}

fn stock_run(conn: &rusqlite::Connection, sql: &str) -> String {
    match conn.execute_batch(sql) {
        Ok(()) => "ok".to_owned(),
        Err(rusqlite::Error::SqliteFailure(_, Some(message))) => format!("error: {message}"),
        Err(error) => format!("error: {error}"),
    }
}

async fn fsqlite_counters(conn: &Connection) -> String {
    let rows = conn
        .query("SELECT changes(), total_changes(), last_insert_rowid()")
        .await
        .unwrap();
    rows[0]
        .values()
        .iter()
        .map(|value| match value {
            SqliteValue::Integer(n) => n.to_string(),
            other => format!("{other:?}"),
        })
        .collect::<Vec<_>>()
        .join("|")
}

/// `sql` on fsqlite, executed directly or prepared first. fsqlite prepares
/// SELECT, DML and PRAGMA only, so DDL always runs directly.
async fn fsqlite_run(conn: &Connection, sql: &str, prepared: bool) -> String {
    let result = if prepared && !sql.starts_with("CREATE") {
        match conn.prepare(sql).await {
            Ok(statement) => statement.execute().await.map(|_| ()),
            Err(error) => Err(error),
        }
    } else {
        conn.execute(sql).await.map(|_| ())
    };
    match result {
        Ok(()) => "ok".to_owned(),
        Err(error) => format!("error: {error}"),
    }
}

#[test]
fn changes_after_a_statement_that_fails_to_prepare_match_stock() {
    let stock = rusqlite::Connection::open_in_memory().unwrap();
    let expected: Vec<(String, String)> = SEQUENCE
        .iter()
        .map(|(sql, pinned)| {
            let outcome = stock_run(&stock, sql);
            let counters = stock_counters(&stock);
            assert_eq!(&counters, pinned, "stock after `{sql}` ({outcome})");
            (outcome, counters)
        })
        .collect();
    // Not vacuous: the prepare-time failures fail in stock.
    assert!(expected[2].0.starts_with("error: no such table"));
    assert!(expected[15].0.starts_with("error: near"));
    asupersync::test_utils::run_test(|| async {
        for file_backed in [false, true] {
            for prepared in [false, true] {
                let dir = tempfile::tempdir().unwrap();
                let path = if file_backed {
                    dir.path().join("bd_rwo47.db").to_str().unwrap().to_owned()
                } else {
                    ":memory:".to_owned()
                };
                let conn = Connection::open(&path).await.unwrap();
                for ((sql, _), (stock_outcome, stock_counters)) in SEQUENCE.iter().zip(&expected) {
                    let outcome = fsqlite_run(&conn, sql, prepared).await;
                    let counters = fsqlite_counters(&conn).await;
                    let context = format!("file_backed={file_backed} prepared={prepared}: `{sql}`");
                    // The outcome's kind must match; the error texts are
                    // other beads' concern.
                    assert_eq!(
                        outcome.starts_with("error"),
                        stock_outcome.starts_with("error"),
                        "{context}: fsqlite {outcome}, stock {stock_outcome}"
                    );
                    assert_eq!(&counters, stock_counters, "{context} ({outcome})");
                }
            }
        }
    });
}
