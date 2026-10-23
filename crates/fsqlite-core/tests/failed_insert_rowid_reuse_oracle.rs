#![recursion_limit = "512"]

//! Bead .33: after a failed INSERT the next automatic rowid is
//! `max(rowid) + 1`, as in stock SQLite (rusqlite), including when the failed
//! INSERT is the connection's first write to the table.
//!
//! The failing statement's row lands in the table b-tree before its index
//! entry hits the UNIQUE (or PRIMARY KEY) conflict. The shared rowid
//! allocator's rewind for a table first allocated in the aborted transaction
//! targeted a floor already raised past that row, so the rowid stayed burned
//! and the next INSERT got `max(rowid) + 2`. An `OR FAIL` statement keeps its
//! earlier rows and runs no rewind, so the rowid of its failing row stayed
//! burned whatever constraint failed, AUTOINCREMENT included. Each case seeds a
//! table, runs a failing statement and then plain inserts, and compares every
//! outcome and the final rows with stock. A case runs in memory, in a file
//! FrankenSQLite seeded with another connection, and in a file stock seeded;
//! stock then checks the file.

use fsqlite_core::connection::{Connection, Row};
use fsqlite_types::value::SqliteValue;

/// (name, schema and seed, statements after the open)
const CASES: &[(&str, &[&str], &[&str])] = &[
    (
        "unique",
        &["CREATE TABLE u(value UNIQUE)", "INSERT INTO u VALUES (7)"],
        &["INSERT INTO u VALUES (7)", "INSERT INTO u VALUES (30)"],
    ),
    (
        "partial_unique",
        &[
            "CREATE TABLE u(value INTEGER)",
            "CREATE UNIQUE INDEX u_value ON u(value) WHERE rowid > 0",
            "INSERT INTO u VALUES (7)",
        ],
        &["INSERT INTO u VALUES (7)", "INSERT INTO u VALUES (30)"],
    ),
    (
        "text_primary_key",
        &["CREATE TABLE u(value TEXT PRIMARY KEY)", "INSERT INTO u VALUES ('a')"],
        &["INSERT INTO u VALUES ('a')", "INSERT INTO u VALUES ('b')"],
    ),
    (
        "multi_row_abort",
        &["CREATE TABLE u(value UNIQUE)", "INSERT INTO u VALUES (7)"],
        &["INSERT INTO u VALUES (8), (7)", "INSERT INTO u VALUES (30)"],
    ),
    (
        "multi_row_fail_keeps_its_first_row",
        &["CREATE TABLE u(value UNIQUE)", "INSERT INTO u VALUES (7)"],
        &["INSERT OR FAIL INTO u VALUES (8), (7)", "INSERT INTO u VALUES (30)"],
    ),
    (
        "multi_row_fail_not_null",
        &[
            "CREATE TABLE u(value UNIQUE, n NOT NULL CHECK (n >= 0))",
            "INSERT INTO u VALUES (7, 1)",
        ],
        &["INSERT OR FAIL INTO u VALUES (8, 1), (9, NULL)", "INSERT INTO u VALUES (30, 1)"],
    ),
    (
        "multi_row_fail_check",
        &[
            "CREATE TABLE u(value UNIQUE, n NOT NULL CHECK (n >= 0))",
            "INSERT INTO u VALUES (7, 1)",
        ],
        &["INSERT OR FAIL INTO u VALUES (8, 1), (9, -1)", "INSERT INTO u VALUES (30, 1)"],
    ),
    (
        "column_on_conflict_fail",
        &["CREATE TABLE u(value UNIQUE ON CONFLICT FAIL)", "INSERT INTO u VALUES (7)"],
        &["INSERT INTO u VALUES (8), (7)", "INSERT INTO u VALUES (30)"],
    ),
    (
        "fail_insert_select",
        &["CREATE TABLE u(value UNIQUE)", "INSERT INTO u VALUES (7)"],
        &[
            "INSERT OR FAIL INTO u SELECT 8 UNION ALL SELECT 7",
            "INSERT INTO u VALUES (30)",
        ],
    ),
    (
        "fail_in_explicit_transaction",
        &["CREATE TABLE u(value UNIQUE)", "INSERT INTO u VALUES (7)"],
        &[
            "BEGIN",
            "INSERT OR FAIL INTO u VALUES (8), (7)",
            "INSERT INTO u VALUES (30)",
            "COMMIT",
        ],
    ),
    (
        // A failed statement never runs stock's sqlite_sequence update, so
        // its rowids are not burned even under AUTOINCREMENT.
        "autoincrement_fail",
        &[
            "CREATE TABLE u(id INTEGER PRIMARY KEY AUTOINCREMENT, value UNIQUE, n NOT NULL)",
            "INSERT INTO u(value, n) VALUES (7, 1)",
        ],
        &[
            "INSERT OR FAIL INTO u(value, n) VALUES (8, 1), (7, 1)",
            "INSERT OR FAIL INTO u(value, n) VALUES (9, 1), (10, NULL)",
            "INSERT INTO u(value, n) VALUES (30, 1)",
            "SELECT name, seq FROM sqlite_sequence",
        ],
    ),
    (
        // A statement that succeeds keeps an AUTOINCREMENT rowid it burned.
        "autoincrement_ignore_burns",
        &[
            "CREATE TABLE u(id INTEGER PRIMARY KEY AUTOINCREMENT, value UNIQUE)",
            "INSERT INTO u(value) VALUES (7)",
        ],
        &[
            "INSERT OR IGNORE INTO u(value) VALUES (7)",
            "INSERT INTO u(value) VALUES (30)",
            "SELECT name, seq FROM sqlite_sequence",
        ],
    ),
    (
        "two_failures",
        &["CREATE TABLE u(value UNIQUE)", "INSERT INTO u VALUES (7)"],
        &[
            "INSERT INTO u VALUES (7)",
            "INSERT INTO u VALUES (7)",
            "INSERT INTO u VALUES (30)",
        ],
    ),
    (
        "success_then_failure",
        &["CREATE TABLE u(value UNIQUE)", "INSERT INTO u VALUES (7)"],
        &[
            "INSERT INTO u VALUES (9)",
            "INSERT INTO u VALUES (7)",
            "INSERT INTO u VALUES (30)",
        ],
    ),
    (
        "explicit_transaction",
        &["CREATE TABLE u(value UNIQUE)", "INSERT INTO u VALUES (7)"],
        &[
            "BEGIN",
            "INSERT INTO u VALUES (7)",
            "INSERT INTO u VALUES (30)",
            "COMMIT",
        ],
    ),
    (
        "rolled_back_transaction",
        &["CREATE TABLE u(value UNIQUE)", "INSERT INTO u VALUES (7)"],
        &[
            "BEGIN",
            "INSERT INTO u VALUES (8)",
            "INSERT INTO u VALUES (7)",
            "ROLLBACK",
            "INSERT INTO u VALUES (30)",
        ],
    ),
    (
        "autoincrement",
        &[
            "CREATE TABLE u(id INTEGER PRIMARY KEY AUTOINCREMENT, value UNIQUE)",
            "INSERT INTO u(value) VALUES (7)",
        ],
        &["INSERT INTO u(value) VALUES (7)", "INSERT INTO u(value) VALUES (30)"],
    ),
];

const ROWS: &str = "SELECT rowid, * FROM u ORDER BY rowid";

/// Who wrote the seed rows the statements run against.
#[derive(Clone, Copy, Debug)]
enum Seed {
    /// The same in-memory connection.
    Memory,
    /// Another FrankenSQLite connection, closed before the open.
    Fsqlite,
    /// Stock.
    Stock,
}

fn render_fsqlite(value: &SqliteValue) -> String {
    match value {
        SqliteValue::Integer(v) => format!("i:{v}"),
        SqliteValue::Float(v) => format!("f:{v}"),
        SqliteValue::Text(v) => format!("t:{v}"),
        SqliteValue::Blob(v) => format!("b:{:?}", v.to_vec()),
        SqliteValue::Null => "null".to_owned(),
    }
}

fn render_rusqlite(value: rusqlite::types::ValueRef<'_>) -> String {
    match value {
        rusqlite::types::ValueRef::Integer(v) => format!("i:{v}"),
        rusqlite::types::ValueRef::Real(v) => format!("f:{v}"),
        rusqlite::types::ValueRef::Text(v) => format!("t:{}", String::from_utf8_lossy(v)),
        rusqlite::types::ValueRef::Blob(v) => format!("b:{v:?}"),
        rusqlite::types::ValueRef::Null => "null".to_owned(),
    }
}

fn render_rows(rows: &[Row]) -> Vec<String> {
    rows.iter()
        .map(|row| {
            row.values()
                .iter()
                .map(render_fsqlite)
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect()
}

fn stock_query(conn: &rusqlite::Connection, sql: &str) -> String {
    let run = || -> rusqlite::Result<Vec<String>> {
        let mut stmt = conn.prepare(sql)?;
        let width = stmt.column_count();
        let mut rows = stmt.query([])?;
        let mut out = Vec::new();
        while let Some(row) = rows.next()? {
            out.push(
                (0..width)
                    .map(|i| render_rusqlite(row.get_ref(i).expect("stock cell")))
                    .collect::<Vec<_>>()
                    .join("|"),
            );
        }
        Ok(out)
    };
    match run() {
        Ok(rows) => format!("{sql} => ok {rows:?}"),
        Err(rusqlite::Error::SqliteFailure(_, Some(message))) => format!("{sql} => error:{message}"),
        Err(error) => format!("{sql} => error:{error}"),
    }
}

async fn fsqlite_query(conn: &Connection, sql: &str) -> String {
    match conn.query(sql).await {
        Ok(rows) => format!("{sql} => ok {:?}", render_rows(&rows)),
        Err(error) => format!("{sql} => error:{error}"),
    }
}

async fn fsqlite_execute(conn: &Connection, sql: &str) -> String {
    match conn.execute(sql).await {
        Ok(_) => format!("{sql} => ok []"),
        Err(error) => format!("{sql} => error:{error}"),
    }
}

/// Every statement's outcome and the final rows, in one stock connection
/// after the seed one closed.
fn stock_transcript(path: &str, seed: &[&str], steps: &[&str]) -> Vec<String> {
    {
        let conn = rusqlite::Connection::open(path).expect("stock open");
        for sql in seed {
            conn.execute_batch(sql).expect("stock seed");
        }
    }
    let conn = rusqlite::Connection::open(path).expect("stock reopen");
    let mut transcript: Vec<String> = steps.iter().map(|sql| stock_query(&conn, sql)).collect();
    transcript.push(stock_query(&conn, ROWS));
    transcript
}

async fn fsqlite_transcript(path: &str, seed_by: Seed, seed: &[&str], steps: &[&str]) -> Vec<String> {
    let conn = match seed_by {
        Seed::Memory => {
            let conn = Connection::open(":memory:").await.expect("fsqlite open");
            for sql in seed {
                conn.execute(sql).await.expect("fsqlite seed");
            }
            conn
        }
        Seed::Fsqlite => {
            let seeder = Connection::open(path).await.expect("fsqlite open");
            for sql in seed {
                seeder.execute(sql).await.expect("fsqlite seed");
            }
            seeder.close().await.expect("fsqlite close");
            Connection::open(path).await.expect("fsqlite reopen")
        }
        Seed::Stock => {
            let stock = rusqlite::Connection::open(path).expect("stock open");
            for sql in seed {
                stock.execute_batch(sql).expect("stock seed");
            }
            drop(stock);
            Connection::open(path).await.expect("fsqlite open")
        }
    };
    let mut transcript = Vec::new();
    for sql in steps {
        transcript.push(if sql.starts_with("SELECT") {
            fsqlite_query(&conn, sql).await
        } else {
            fsqlite_execute(&conn, sql).await
        });
    }
    transcript.push(fsqlite_query(&conn, ROWS).await);
    conn.close().await.expect("fsqlite close");
    transcript
}

#[test]
fn next_rowid_after_a_failed_insert_matches_stock() {
    let dir = tempfile::tempdir().expect("temp dir");
    let dir_path = dir.path().to_owned();
    let path_of = |name: &str| {
        dir_path
            .join(format!("{name}.db"))
            .to_str()
            .expect("utf-8 path")
            .to_owned()
    };
    let expected: Vec<Vec<String>> = CASES
        .iter()
        .map(|(name, seed, steps)| stock_transcript(&path_of(&format!("stock_{name}")), seed, steps))
        .collect();
    // The oracle itself: the insert after the failure lands at max(rowid) + 1.
    // (So does AUTOINCREMENT's: the failed statement's sqlite_sequence update
    // rolls back with it.)
    assert!(
        expected[0].last().is_some_and(|rows| rows.ends_with("[\"i:1|i:7\", \"i:2|i:30\"]")),
        "stock: {:#?}",
        expected[0]
    );
    asupersync::test_utils::run_test(|| async move {
        let mut mismatches = Vec::new();
        for ((name, seed, steps), want) in CASES.iter().zip(&expected) {
            for seed_by in [Seed::Memory, Seed::Fsqlite, Seed::Stock] {
                let path = path_of(&format!("{name}_{seed_by:?}"));
                let got = fsqlite_transcript(&path, seed_by, seed, steps).await;
                if &got != want {
                    mismatches.push(format!(
                        "{name} ({seed_by:?})\n  fsqlite: {got:#?}\n  stock:   {want:#?}"
                    ));
                }
                if !matches!(seed_by, Seed::Memory) {
                    let stock = rusqlite::Connection::open(&path).expect("stock reopen");
                    let check = stock_query(&stock, "PRAGMA integrity_check");
                    let rows = stock_query(&stock, ROWS);
                    if check != "PRAGMA integrity_check => ok [\"t:ok\"]"
                        || Some(&rows) != want.last()
                    {
                        mismatches.push(format!(
                            "{name} ({seed_by:?}): stock reading the fsqlite file\n  {check}\n  {rows}"
                        ));
                    }
                }
            }
        }
        for mismatch in &mismatches {
            eprintln!("event=failed_insert_rowid_mismatch {mismatch}");
        }
        assert!(
            mismatches.is_empty(),
            "{} mismatches; first: {}",
            mismatches.len(),
            mismatches[0]
        );
    });
}
