#![recursion_limit = "512"]
//! bd-jlbrk (the two rules its preflight tests found):
//!
//! 1. A UNIQUE violation of an index with an expression term names the index,
//!    as stock's sqlite3UniqueConstraint does: `UNIQUE constraint failed:
//!    index 't_expr'`. fsqlite printed the terms (`t.lower(b)`). An index of
//!    plain columns keeps the column label, with a COLLATE or DESC term and
//!    with a partial-index WHERE alike.
//! 2. An AUTOINCREMENT table's sqlite_sequence advances to an explicit rowid
//!    even when UNIQUE ... ON CONFLICT IGNORE or OR IGNORE discards that row:
//!    stock records the rowid (OP_MemMax) before the constraint checks.
//!    fsqlite advanced it only for a rowid NewRowid allocated (GH #186). A row
//!    an ABORT rejects rolls its statement back and records nothing, and an
//!    explicit rowid below the sequence does not lower it.
//!
//! Every step's outcome and every checkpoint match bundled SQLite 3.54.1 on
//! `:memory:` and on a file, with each statement run directly and prepared:
//! INSERT, UPDATE and CREATE UNIQUE INDEX over duplicates, rowid and WITHOUT
//! ROWID tables.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn render_fsqlite(value: &SqliteValue) -> String {
    match value {
        SqliteValue::Null => "NULL".into(),
        SqliteValue::Integer(n) => format!("int:{n}"),
        SqliteValue::Float(f) => format!("real:{f:?}"),
        SqliteValue::Text(s) => format!("text:{s}"),
        SqliteValue::Blob(b) => format!("blob:{b:02x?}"),
    }
}

fn render_stock(value: &rusqlite::types::Value) -> String {
    match value {
        rusqlite::types::Value::Null => "NULL".into(),
        rusqlite::types::Value::Integer(n) => format!("int:{n}"),
        rusqlite::types::Value::Real(f) => format!("real:{f:?}"),
        rusqlite::types::Value::Text(s) => format!("text:{s}"),
        rusqlite::types::Value::Blob(b) => format!("blob:{b:02x?}"),
    }
}

/// A statement's outcome: its rows, or its error message.
fn stock_outcome(conn: &rusqlite::Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let mut statement = conn.prepare(sql).map_err(|e| sqlite_message(&e))?;
    let columns = statement.column_count();
    let rows = statement
        .query_map([], |row| {
            Ok((0..columns)
                .map(|i| render_stock(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
                .collect::<Vec<_>>())
        })
        .map_err(|e| sqlite_message(&e))?;
    rows.collect::<Result<Vec<_>, _>>()
        .map_err(|e| sqlite_message(&e))
}

fn sqlite_message(error: &rusqlite::Error) -> String {
    match error {
        rusqlite::Error::SqliteFailure(_, Some(message)) => message.clone(),
        other => other.to_string(),
    }
}

async fn fsqlite_outcome(
    conn: &Connection,
    sql: &str,
    prepared: bool,
) -> Result<Vec<Vec<String>>, String> {
    let is_query = sql.starts_with("SELECT");
    // prepare() takes DML and SELECT; DDL runs directly in both modes.
    let prepared = prepared && !sql.starts_with("CREATE");
    let rows = match (prepared, is_query) {
        (true, true) => {
            let statement = conn.prepare(sql).await.map_err(|e| e.to_string())?;
            statement.query().await.map_err(|e| e.to_string())?
        }
        (true, false) => {
            let statement = conn.prepare(sql).await.map_err(|e| e.to_string())?;
            statement.execute().await.map_err(|e| e.to_string())?;
            Vec::new()
        }
        (false, true) => conn.query(sql).await.map_err(|e| e.to_string())?,
        (false, false) => {
            conn.execute(sql).await.map_err(|e| e.to_string())?;
            Vec::new()
        }
    };
    Ok(rows
        .iter()
        .map(|row| row.values().iter().map(render_fsqlite).collect())
        .collect())
}

const SETUP: &[&str] = &[
    "CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT COLLATE NOCASE, b TEXT, c TEXT)",
    "CREATE UNIQUE INDEX t_expr ON t(lower(b))",
    "CREATE UNIQUE INDEX t_mixed ON t(c, length(b))",
    "CREATE UNIQUE INDEX t_coll ON t(a COLLATE NOCASE, c DESC)",
    "CREATE UNIQUE INDEX t_part ON t(c) WHERE id % 2 = 1",
    "CREATE UNIQUE INDEX t_pexpr ON t(upper(c)) WHERE id > 100",
    "INSERT INTO t VALUES (1, 'x', 'Q', 'k1')",
    "CREATE TABLE ai(id INTEGER PRIMARY KEY AUTOINCREMENT, a TEXT UNIQUE ON CONFLICT IGNORE)",
    "CREATE TABLE ab(id INTEGER PRIMARY KEY AUTOINCREMENT, a TEXT UNIQUE)",
    "CREATE TABLE w(k TEXT PRIMARY KEY, v) WITHOUT ROWID",
    "CREATE UNIQUE INDEX w_e ON w(lower(v))",
];

const SEQ: &str = "SELECT name, seq FROM sqlite_sequence ORDER BY name";

const STEPS: &[&str] = &[
    // An expression index: named by the index.
    "INSERT INTO t VALUES (2, 'y', 'q', 'k2')",
    "INSERT INTO t VALUES (4, 'z', 'rr', 'k1')",
    // Plain columns with COLLATE and DESC: the column label.
    "INSERT INTO t VALUES (6, 'X', 'w', 'k1')",
    // A partial index on a plain column: the column label.
    "INSERT INTO t VALUES (3, 'v', 'u', 'k1')",
    "INSERT INTO t VALUES (101, 'p', 'e', 'zz')",
    // A partial expression index: named by the index.
    "INSERT INTO t VALUES (103, 'p2', 'f', 'ZZ')",
    // The same rule for UPDATE, and for the expression term of a mixed index.
    "UPDATE t SET b = 'q' WHERE id = 4",
    "UPDATE t SET c = 'k1', b = 'zz' WHERE id = 101",
    "SELECT id, a, b, c FROM t ORDER BY id",
    "INSERT INTO ai VALUES (10, 'alpha')",
    // Ignored by the column's ON CONFLICT IGNORE: the sequence still moves.
    "INSERT INTO ai VALUES (200, 'alpha')",
    SEQ,
    // Below the sequence: no change.
    "INSERT INTO ai VALUES (50, 'alpha')",
    SEQ,
    // The ignored row carries the statement's highest rowid.
    "INSERT INTO ai VALUES (60, 'beta'), (300, 'alpha'), (70, 'gamma')",
    SEQ,
    "INSERT INTO ai VALUES (NULL, 'delta')",
    "SELECT * FROM ai ORDER BY id",
    "INSERT INTO ab VALUES (5, 'alpha')",
    "INSERT OR IGNORE INTO ab VALUES (400, 'alpha')",
    SEQ,
    // ABORT: the statement rolls back and records nothing.
    "INSERT INTO ab VALUES (500, 'alpha')",
    SEQ,
    "INSERT OR IGNORE INTO ab SELECT 600, 'alpha' UNION ALL SELECT 7, 'beta'",
    SEQ,
    "INSERT INTO ab VALUES (NULL, 'eps')",
    "SELECT * FROM ab ORDER BY id",
    // Building a UNIQUE index over duplicates: the same two labels.
    "CREATE UNIQUE INDEX t_dupe ON t(substr(c, 1, 1))",
    "CREATE UNIQUE INDEX t_dupc ON t(c)",
    // A WITHOUT ROWID table's expression index.
    "INSERT INTO w VALUES ('a', 'Q'), ('b', 'q')",
    "SELECT * FROM w",
];

#[test]
fn expression_index_messages_and_ignored_autoincrement_rowids_match_stock() {
    let stock = rusqlite::Connection::open_in_memory().unwrap();
    for sql in SETUP {
        stock.execute_batch(sql).unwrap();
    }
    let expected: Vec<_> = STEPS.iter().map(|sql| stock_outcome(&stock, sql)).collect();
    // Not vacuous: stock names the expression index, keeps the column label
    // for plain columns, and moves the sequence past an ignored rowid.
    assert_eq!(
        expected[0],
        Err("UNIQUE constraint failed: index 't_expr'".to_owned())
    );
    assert_eq!(expected[2], Err("UNIQUE constraint failed: t.a, t.c".to_owned()));
    assert_eq!(expected[3], Err("UNIQUE constraint failed: t.c".to_owned()));
    assert_eq!(
        expected[11],
        Ok(vec![vec!["text:ai".to_owned(), "int:200".to_owned()]])
    );
    asupersync::test_utils::run_test(|| async move {
        for file_backed in [false, true] {
            for prepared in [false, true] {
                let dir = tempfile::tempdir().unwrap();
                let path = if file_backed {
                    dir.path().join("jlbrk.db").to_str().unwrap().to_owned()
                } else {
                    ":memory:".to_owned()
                };
                let conn = Connection::open(&path).await.unwrap();
                for sql in SETUP {
                    conn.execute(sql).await.unwrap();
                }
                for (sql, want) in STEPS.iter().zip(&expected) {
                    assert_eq!(
                        &fsqlite_outcome(&conn, sql, prepared).await,
                        want,
                        "file_backed={file_backed} prepared={prepared}: {sql}"
                    );
                }
            }
        }
    });
}
