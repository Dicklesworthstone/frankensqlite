//! bd-1q134 — stock auto-vacuum databases accept mixed DML/DDL writes with full
//! pointer-map maintenance and pass C SQLite's `PRAGMA integrity_check`.
//!
//! Previously, before pointer-map maintenance was implemented, the pager had a
//! stop-gap write guard refusing writers on stock `auto_vacuum = FULL` /
//! `INCREMENTAL` databases to avoid silently corrupting pointer maps. Now that
//! `PRAGMA auto_vacuum`, pointer-map maintenance, root compaction, and
//! truncation are wired end-to-end, FrankenSQLite must read and mutate stock
//! `FULL`, `INCREMENTAL`, and `NONE` databases cleanly while preserving their
//! mode and passing stock SQLite's `PRAGMA integrity_check = ok`.

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

fn stock_verdict(path: &Path) -> (String, i64, i64, i64, i64) {
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
    let page_count: i64 = c
        .query_row("PRAGMA page_count;", [], |row| row.get(0))
        .expect("stock page_count");
    let freelist_count: i64 = c
        .query_row("PRAGMA freelist_count;", [], |row| row.get(0))
        .expect("stock freelist_count");
    (integrity, auto_vacuum, rows, page_count, freelist_count)
}

/// Runs reads then a battery of mixed DML/DDL writes through FrankenSQLite.
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
fn stock_auto_vacuum_database_accepts_writes_and_preserves_integrity() {
    for mode in ["FULL", "INCREMENTAL"] {
        let dir = temp_dir();
        let path = dir.path().join("av.db");
        create_stock_database(&path, mode);
        let (_, auto_vacuum_before, _, pages_before, _) = stock_verdict(&path);
        assert_ne!(
            auto_vacuum_before, 0,
            "{mode}: stock must have enabled auto_vacuum"
        );

        let (count, writes) = exercise_with_fsqlite(&path);
        assert_eq!(count, ROWS, "{mode}: reads must keep working");
        for (i, w) in writes.iter().enumerate() {
            assert!(
                w.is_ok(),
                "{mode}: write #{i} must succeed with pointer-map maintenance: {w:?}"
            );
        }

        let (integrity, auto_vacuum_after, rows_after, pages_after, freelist_after) =
            stock_verdict(&path);
        assert_eq!(
            integrity, "ok",
            "{mode}: stock integrity_check must pass after FrankenSQLite mutations"
        );
        assert_eq!(
            auto_vacuum_after, auto_vacuum_before,
            "{mode}: auto_vacuum mode must be preserved"
        );
        assert!(
            rows_after < ROWS,
            "{mode}: DELETE must have taken effect (rows_after={rows_after}, ROWS={ROWS})"
        );
        if mode == "FULL" {
            assert!(
                pages_after < pages_before,
                "FULL: deleting 1/3 of rows + dropping index must shrink page_count ({pages_before} -> {pages_after})"
            );
            assert_eq!(
                freelist_after, 0,
                "FULL: freelist_count must be 0 after commit"
            );
        } else {
            assert!(
                freelist_after > 0,
                "INCREMENTAL: deleted pages must remain on freelist until PRAGMA incremental_vacuum"
            );
        }
    }
}

#[test]
fn stock_database_without_auto_vacuum_stays_writable() {
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

    let (integrity, auto_vacuum, rows_after, _, _) = stock_verdict(&path);
    assert_eq!(integrity, "ok");
    assert_eq!(auto_vacuum, 0);
    // One insert, then every third row (by id) deleted.
    assert!(
        rows_after < ROWS,
        "the DELETE must have taken effect: {rows_after}"
    );
}
