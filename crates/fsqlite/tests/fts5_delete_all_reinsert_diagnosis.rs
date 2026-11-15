//! frankensqlite#94 / bd-qxdk4: `DELETE FROM <fts5>` with no WHERE clause, then
//! a re-`INSERT` of the same rowids, compared statement by statement with
//! stock SQLite (rusqlite's bundled 3.54.1) in every content mode.
//!
//! What stock does, and why each mode is here:
//! - stored (`fts5(command)`): the table owns `%_content`, whose INTEGER
//!   PRIMARY KEY makes a second INSERT of a live rowid fail. Delete-all clears
//!   it, so the re-INSERT succeeds. A fix that just drops the rowid check
//!   breaks this mode.
//! - contentless with `contentless_delete=1`: delete-all removes every rowid;
//!   a second INSERT of a live rowid is not checked and lands on top of it.
//! - legacy contentless (`content=''`): DELETE is refused; a second INSERT of
//!   a live rowid is not checked; `INSERT OR REPLACE` is a plain insert (stock
//!   honours the conflict mode only for stored and contentless_delete tables).
//! - external content (`content='base'`): a full scan reads the content table,
//!   so with an empty `base` the scan is empty and `DELETE FROM t` deletes
//!   NOTHING. The postings stay: the re-INSERTs succeed and `MATCH 'doc'` still
//!   finds 1, 2, 3 beside `MATCH 'redoc'`. A delete-all that clears the index,
//!   or a re-INSERT that drops the old postings, fails here.
//!
//! The first version of this file asserted a pre-delete scan of [1, 2, 3] for
//! the external-content table, which is not stock's answer (stock scans the
//! empty `base`), so it failed before it reached the reinsert. It is now
//! compared with rusqlite at every step instead of with hand-written values.
//!
//! Each mode runs on `:memory:`, on a file, and on a file reopened between the
//! seed rows and the delete-all (the reopened table reads its postings from the
//! persisted segments). File runs are also reopened at the end, and stock then
//! reads the file FrankenSQLite wrote.
//!
//! Not compared here (known, recorded on bd-qxdk4): bm25 scores after a second
//! INSERT of a live rowid (stock's totals count every insert), and a
//! contentless_delete=1 DELETE of a rowid inserted twice (stock tombstones only
//! the newest segment's entry).

#![cfg(feature = "fts5")]

use std::path::Path;

use fsqlite::{Connection, FrankenError, SqliteValue};

const STORED: &[&str] = &["CREATE VIRTUAL TABLE t USING fts5(command)"];
const CONTENTLESS_DELETE: &[&str] =
    &["CREATE VIRTUAL TABLE t USING fts5(command, content='', contentless_delete=1)"];
const CONTENTLESS_LEGACY: &[&str] = &["CREATE VIRTUAL TABLE t USING fts5(command, content='')"];
const EXTERNAL: &[&str] = &[
    "CREATE TABLE base (id INTEGER PRIMARY KEY AUTOINCREMENT, command TEXT NOT NULL)",
    "CREATE VIRTUAL TABLE t USING fts5(command, content='base', content_rowid='id')",
];

const MODES: &[(&str, &[&str])] = &[
    ("stored", STORED),
    ("contentless_delete", CONTENTLESS_DELETE),
    ("contentless_legacy", CONTENTLESS_LEGACY),
    ("external", EXTERNAL),
];

const SEED: &[&str] = &[
    "INSERT INTO t(rowid, command) VALUES (1, 'doc 1')",
    "INSERT INTO t(rowid, command) VALUES (2, 'doc 2')",
    "INSERT INTO t(rowid, command) VALUES (3, 'doc 3')",
];

/// The delete-all and the re-INSERT of the same rowids (the GH#94 sequence).
const DELETE_ALL_REINSERT: &[&str] = &[
    "SELECT rowid FROM t ORDER BY rowid",
    "SELECT rowid FROM t WHERE t MATCH 'doc' ORDER BY rowid",
    "DELETE FROM t",
    "SELECT rowid FROM t ORDER BY rowid",
    "SELECT rowid FROM t WHERE t MATCH 'doc' ORDER BY rowid",
    "INSERT INTO t(rowid, command) VALUES (1, 'redoc 1')",
    "INSERT INTO t(rowid, command) VALUES (2, 'redoc 2')",
    "INSERT INTO t(rowid, command) VALUES (3, 'redoc 3')",
    "SELECT rowid FROM t ORDER BY rowid",
    "SELECT count(*) FROM t",
];

/// More writes onto rowids that are already live.
const LIVE_ROWID_WRITES: &[&str] = &[
    // Stored tables refuse it; the other modes put the new row on top.
    "INSERT INTO t(rowid, command) VALUES (2, 'again 2')",
    // Stored and contentless_delete replace; the other modes insert.
    "INSERT OR REPLACE INTO t(rowid, command) VALUES (3, 'swap 3')",
    // A rolled-back write onto a live rowid leaves nothing behind.
    "BEGIN",
    "INSERT INTO t(rowid, command) VALUES (1, 'ghost 1')",
    "ROLLBACK",
    "SELECT rowid FROM t WHERE t MATCH 'ghost' ORDER BY rowid",
    "SELECT rowid FROM t ORDER BY rowid",
    "SELECT count(*) FROM t",
];

/// Reads compared after the reinsert, after the later writes, and after reopens.
const READS: &[&str] = &[
    "SELECT rowid FROM t WHERE t MATCH 'redoc' ORDER BY rowid",
    "SELECT rowid FROM t WHERE t MATCH 'doc' ORDER BY rowid",
    "SELECT rowid FROM t WHERE t MATCH 'again' ORDER BY rowid",
    "SELECT rowid FROM t WHERE t MATCH 'swap' ORDER BY rowid",
    "SELECT rowid FROM t WHERE t MATCH '2' ORDER BY rowid",
    "SELECT rowid FROM t WHERE t MATCH '\"again 2\"' ORDER BY rowid",
    "SELECT rowid FROM t WHERE t MATCH '\"redoc 2\"' ORDER BY rowid",
    "SELECT rowid FROM t WHERE t MATCH 'redoc OR swap OR again OR doc' ORDER BY rowid",
    "SELECT rowid FROM t WHERE t MATCH 'r*' ORDER BY rowid",
];

/// Run by stock over a finished file, its own and the one FrankenSQLite wrote.
const STOCK_FILE_CHECKS: &[&str] = &[
    "SELECT rowid FROM t ORDER BY rowid",
    "SELECT count(*) FROM t",
    "INSERT INTO t(t) VALUES('integrity-check')",
    "PRAGMA integrity_check",
];

#[derive(Clone, Copy, Debug)]
enum Storage {
    Memory,
    File,
    /// The file is closed and reopened after the seed rows.
    Reopened,
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

fn is_query(sql: &str) -> bool {
    let head = sql.trim_start();
    head.starts_with("SELECT") || head.starts_with("PRAGMA")
}

/// Only an INSERT or DELETE reports its own change count. After DDL or
/// BEGIN/ROLLBACK, `sqlite3_changes()` still holds an earlier count (stock's
/// CREATE VIRTUAL TABLE ... fts5 leaves its internal `%_config` insert there).
fn is_dml(sql: &str) -> bool {
    let head = sql.trim_start();
    head.starts_with("INSERT") || head.starts_with("DELETE")
}

fn render_success(sql: &str, changes: usize) -> String {
    if is_dml(sql) {
        format!("changes:{changes}")
    } else {
        "ok".to_owned()
    }
}

/// One statement's outcome: its rows, its change count, or its primary error
/// code. Error texts are not compared (stock says "constraint failed" for the
/// stored duplicate, FrankenSQLite names the PRIMARY KEY).
fn fsqlite_error(error: &FrankenError) -> String {
    format!("error:{}", error.error_code() as i32)
}

async fn fsqlite_step(conn: &Connection, sql: &str) -> String {
    if is_query(sql) {
        match conn.query(sql).await {
            Ok(rows) => {
                let rendered: Vec<String> = rows
                    .iter()
                    .map(|row| {
                        row.values()
                            .iter()
                            .map(render_fsqlite)
                            .collect::<Vec<_>>()
                            .join(",")
                    })
                    .collect();
                format!("rows:[{}]", rendered.join(" | "))
            }
            Err(error) => fsqlite_error(&error),
        }
    } else {
        match conn.execute(sql).await {
            Ok(changes) => render_success(sql, changes),
            Err(error) => fsqlite_error(&error),
        }
    }
}

fn stock_error(error: &rusqlite::Error) -> String {
    match error {
        rusqlite::Error::SqliteFailure(failure, _) => {
            format!("error:{}", failure.extended_code & 0xff)
        }
        other => panic!("unexpected rusqlite error shape: {other:?}"),
    }
}

fn stock_step(conn: &rusqlite::Connection, sql: &str) -> String {
    let mut stmt = match conn.prepare(sql) {
        Ok(stmt) => stmt,
        Err(error) => return stock_error(&error),
    };
    if stmt.column_count() == 0 {
        return match stmt.execute([]) {
            Ok(changes) => render_success(sql, changes),
            Err(error) => stock_error(&error),
        };
    }
    let column_count = stmt.column_count();
    let mut rows = match stmt.query([]) {
        Ok(rows) => rows,
        Err(error) => return stock_error(&error),
    };
    let mut rendered = Vec::new();
    loop {
        match rows.next() {
            Ok(Some(row)) => rendered.push(
                (0..column_count)
                    .map(|index| render_rusqlite(row.get_ref_unwrap(index)))
                    .collect::<Vec<_>>()
                    .join(","),
            ),
            Ok(None) => break,
            Err(error) => return stock_error(&error),
        }
    }
    format!("rows:[{}]", rendered.join(" | "))
}

async fn compare_steps(
    label: &str,
    conn: &Connection,
    stock: &rusqlite::Connection,
    statements: &[&str],
) {
    for sql in statements {
        let ours = fsqlite_step(conn, sql).await;
        let theirs = stock_step(stock, sql);
        assert_eq!(ours, theirs, "[{label}] `{sql}` diverges from stock");
    }
}

async fn open_fsqlite(path: &Path) -> Connection {
    Connection::open(path.to_str().expect("utf-8 temp path"))
        .await
        .expect("open fsqlite file")
}

fn open_stock(path: &Path) -> rusqlite::Connection {
    rusqlite::Connection::open(path).expect("open stock file")
}

async fn run_mode(mode: &str, create: &[&str], storage: Storage) {
    let label = format!("{mode}/{storage:?}");
    let dir = tempfile::tempdir().expect("tempdir");
    let ours_path = dir.path().join("fsqlite.db");
    let stock_path = dir.path().join("stock.db");

    let (mut conn, mut stock) = match storage {
        Storage::Memory => (
            Connection::open(":memory:").await.expect("open fsqlite"),
            rusqlite::Connection::open_in_memory().expect("open stock"),
        ),
        Storage::File | Storage::Reopened => {
            (open_fsqlite(&ours_path).await, open_stock(&stock_path))
        }
    };

    compare_steps(&label, &conn, &stock, create).await;
    compare_steps(&label, &conn, &stock, SEED).await;
    if matches!(storage, Storage::Reopened) {
        conn.close().await.expect("close fsqlite");
        drop(stock);
        conn = open_fsqlite(&ours_path).await;
        stock = open_stock(&stock_path);
    }
    compare_steps(&label, &conn, &stock, DELETE_ALL_REINSERT).await;
    compare_steps(&label, &conn, &stock, READS).await;
    compare_steps(&label, &conn, &stock, LIVE_ROWID_WRITES).await;
    compare_steps(&label, &conn, &stock, READS).await;

    if matches!(storage, Storage::Memory) {
        return;
    }
    // FrankenSQLite reads back its own file.
    conn.close().await.expect("close fsqlite");
    let conn = open_fsqlite(&ours_path).await;
    compare_steps(&format!("{label}/reopened-at-end"), &conn, &stock, READS).await;
    conn.close().await.expect("close fsqlite");
    // Stock reads the file FrankenSQLite wrote, and must see what it sees in
    // its own.
    let ours_by_stock = open_stock(&ours_path);
    for sql in READS.iter().chain(STOCK_FILE_CHECKS) {
        let on_ours = stock_step(&ours_by_stock, sql);
        let on_its_own = stock_step(&stock, sql);
        assert_eq!(
            on_ours, on_its_own,
            "[{label}] stock reads `{sql}` differently on FrankenSQLite's file"
        );
    }
}

fn run_all_storages(mode: &str, create: &'static [&'static str]) {
    for storage in [Storage::Memory, Storage::File, Storage::Reopened] {
        let mode = mode.to_owned();
        asupersync::test_utils::run_test(move || async move {
            run_mode(&mode, create, storage).await;
        });
    }
}

#[test]
fn delete_all_then_reinsert_stored() {
    run_all_storages(MODES[0].0, MODES[0].1);
}

#[test]
fn delete_all_then_reinsert_contentless_delete() {
    run_all_storages(MODES[1].0, MODES[1].1);
}

#[test]
fn delete_all_rejected_then_reinsert_contentless_legacy() {
    run_all_storages(MODES[2].0, MODES[2].1);
}

#[test]
fn delete_all_then_reinsert_external() {
    run_all_storages(MODES[3].0, MODES[3].1);
}

/// A WHERE-clause DELETE removes only the matching rowid, after which that
/// rowid can be inserted again, in every mode.
#[test]
fn delete_where_rowid_then_reinsert() {
    for (mode, create) in MODES {
        asupersync::test_utils::run_test(move || async move {
            let label = format!("{mode}/where");
            let conn = Connection::open(":memory:").await.expect("open fsqlite");
            let stock = rusqlite::Connection::open_in_memory().expect("open stock");
            compare_steps(&label, &conn, &stock, create).await;
            compare_steps(&label, &conn, &stock, SEED).await;
            compare_steps(
                &label,
                &conn,
                &stock,
                &[
                    "DELETE FROM t WHERE rowid = 2",
                    "SELECT rowid FROM t ORDER BY rowid",
                    "INSERT INTO t(rowid, command) VALUES (2, 'redoc 2')",
                    "SELECT rowid FROM t ORDER BY rowid",
                    "SELECT count(*) FROM t",
                ],
            )
            .await;
            compare_steps(&label, &conn, &stock, READS).await;
        });
    }
}
