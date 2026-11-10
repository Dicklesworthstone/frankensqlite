//! bd-1q134 — writes to a stock auto-vacuum database are refused, not corrupting.
//!
//! A database created by stock SQLite with `PRAGMA auto_vacuum = FULL` (or
//! `INCREMENTAL`) keeps pointer-map pages, and its header's largest-root-page
//! field (bytes 52..56) is nonzero. FrankenSQLite does not maintain pointer
//! maps yet. Before this guard, writing such a file succeeded and FrankenSQLite's
//! own `integrity_check` said `ok`, but stock SQLite then reported
//! `Freelist: Failed to read ptrmap key=...` (2026-11-09 reality check).
//!
//! The pager now refuses every writer upgrade on such a file. This test asks C
//! SQLite for the verdict on the file afterwards, and keeps a negative control:
//! the same workload on a non-auto-vacuum stock database must still write.

use std::cell::RefCell;
use std::path::Path;

use fsqlite::Connection;
use fsqlite_types::SqliteValue;

const ROWS: i64 = 4_000;

fn create_stock_database(path: &Path, auto_vacuum: &str) {
    let c = rusqlite::Connection::open(path).expect("rusqlite open");
    c.execute_batch(&format!(
        "PRAGMA auto_vacuum = {auto_vacuum};
         CREATE TABLE t(id INTEGER PRIMARY KEY, b TEXT);
         CREATE INDEX tb ON t(b);
         WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < {ROWS})
         INSERT INTO t SELECT i, hex(randomblob(150)) FROM n;"
    ))
    .expect("stock setup");
}

fn stock_verdict(path: &Path) -> (String, i64, i64) {
    let c = rusqlite::Connection::open(path).expect("rusqlite reopen");
    let integrity: String = c
        .query_row("PRAGMA integrity_check;", [], |row| row.get(0))
        .expect("stock integrity_check");
    let auto_vacuum: i64 = c
        .query_row("PRAGMA auto_vacuum;", [], |row| row.get(0))
        .expect("stock auto_vacuum");
    let rows: i64 = c
        .query_row("SELECT count(*) FROM t;", [], |row| row.get(0))
        .expect("stock count");
    (integrity, auto_vacuum, rows)
}

/// Runs reads then a battery of writes through FrankenSQLite. Returns the row
/// count it read and every write's outcome.
fn exercise_with_fsqlite(path: &Path) -> (i64, Vec<Result<(), String>>) {
    let db = path.to_str().expect("utf-8 path").to_owned();
    let cell = RefCell::new(None);
    let outcome = &cell;
    asupersync::test_utils::run_test(move || async move {
        let conn = Connection::open(&db).await.expect("fsqlite open");
        let rows = conn
            .query("SELECT count(*) FROM t;")
            .await
            .expect("fsqlite read");
        let count = match &rows[0].values()[0] {
            SqliteValue::Integer(n) => *n,
            other => panic!("count(*) returned {other:?}"),
        };
        let mut writes = Vec::new();
        for sql in [
            "INSERT INTO t(b) VALUES ('new row');",
            "DELETE FROM t WHERE id % 3 = 0;",
            "UPDATE t SET b = b || 'x' WHERE id < 50;",
            "CREATE TABLE u(x);",
            "DROP INDEX tb;",
        ] {
            writes.push(
                conn.execute(sql)
                    .await
                    .map(|_| ())
                    .map_err(|e| e.to_string()),
            );
        }
        conn.close().await.expect("fsqlite close");
        *outcome.borrow_mut() = Some((count, writes));
    });
    cell.into_inner().expect("fsqlite run produced an outcome")
}

fn temp_dir() -> tempfile::TempDir {
    tempfile::tempdir_in(std::env::temp_dir())
        .or_else(|_| tempfile::tempdir_in("."))
        .expect("tempdir")
}

#[test]
fn stock_auto_vacuum_database_is_readable_but_refuses_writes() {
    for mode in ["FULL", "INCREMENTAL"] {
        let dir = temp_dir();
        let path = dir.path().join("av.db");
        create_stock_database(&path, mode);
        let (_, auto_vacuum_before, _) = stock_verdict(&path);
        assert_ne!(
            auto_vacuum_before, 0,
            "{mode}: stock must have enabled auto_vacuum"
        );

        let (count, writes) = exercise_with_fsqlite(&path);
        assert_eq!(count, ROWS, "{mode}: reads must keep working");
        for (i, w) in writes.iter().enumerate() {
            let err = w
                .as_ref()
                .expect_err(&format!("{mode}: write #{i} must be refused"));
            assert!(
                err.contains("auto_vacuum"),
                "{mode}: write #{i} must name the reason, got: {err}"
            );
        }

        let (integrity, auto_vacuum_after, rows_after) = stock_verdict(&path);
        assert_eq!(
            integrity, "ok",
            "{mode}: stock must still find the file intact"
        );
        assert_eq!(
            auto_vacuum_after, auto_vacuum_before,
            "{mode}: mode unchanged"
        );
        assert_eq!(
            rows_after, ROWS,
            "{mode}: refused writes must not change data"
        );
    }
}

#[test]
fn stock_database_without_auto_vacuum_stays_writable() {
    // Negative control: the guard must key on the header, not on stock origin.
    let dir = temp_dir();
    let path = dir.path().join("plain.db");
    create_stock_database(&path, "NONE");

    let (count, writes) = exercise_with_fsqlite(&path);
    assert_eq!(count, ROWS);
    for (i, w) in writes.iter().enumerate() {
        assert!(
            w.is_ok(),
            "write #{i} must succeed on a non-auto-vacuum file: {w:?}"
        );
    }

    let (integrity, auto_vacuum, rows_after) = stock_verdict(&path);
    assert_eq!(integrity, "ok");
    assert_eq!(auto_vacuum, 0);
    // One insert, then every third row (by id) deleted.
    assert!(
        rows_after < ROWS,
        "the DELETE must have taken effect: {rows_after}"
    );
}
