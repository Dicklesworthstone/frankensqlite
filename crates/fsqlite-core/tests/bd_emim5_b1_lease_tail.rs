//! bd-emim5 lever B1 keeper, at the connection level with stock SQLite reading
//! the file: a CONCURRENT growth commit that leaves part of its page lease
//! unused commits a page count that excludes the unused tail and publishes no
//! freelist for it; stock `integrity_check` reads the file clean (every page in
//! a b-tree, none "never used"); and a reopen grows into the old tail at EOF.
//!
//! The pager hands EOF pages to a CONCURRENT transaction in leases of 8 after
//! its first allocation. Before B1 the lease popped its HIGHEST page first, so a
//! transaction that used part of a lease left the unused pages below its top
//! written page: inside its commit's page count, published as free pages
//! (`freelist_count` > 0 after a pure-insert growth). That is the RED on the
//! parent. With B1 the lease hands out its lowest page first and the commit
//! returns the unused top tail to EOF allocation.
//!
//! Planted mutant (the 2026-11-26 decision's): drop the unused lease pages at
//! commit without excluding them from the page count. Stock then reports
//! "Page N is never used": RED.
#![cfg(unix)]

use std::path::Path;
use std::time::Duration;

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;
use rusqlite::OpenFlags;

/// About four rows per 4096-byte leaf, so a 20-row transaction splits leaves
/// several times and uses some, not all, of one lease.
const PAD: usize = 1000;
/// Rows per CONCURRENT transaction, in order. The first round alone uses part
/// of a fresh lease; the later rounds vary the share of a lease that is used.
const ROUNDS: [i64; 6] = [20, 5, 11, 2, 33, 8];
const REOPEN_ROWS: i64 = 12;
const BUSY_TIMEOUT: Duration = Duration::from_millis(5000);

async fn open(db: &Path) -> Connection {
    let conn = Connection::open(db.to_string_lossy().into_owned())
        .await
        .expect("open");
    conn.execute("PRAGMA busy_timeout=5000")
        .await
        .expect("busy_timeout");
    conn
}

async fn scalar(conn: &Connection, sql: &str) -> i64 {
    let rows = conn.query(sql).await.expect("query");
    match rows[0].values()[0] {
        SqliteValue::Integer(value) => value,
        ref other => panic!("expected an integer from {sql}, got {other:?}"),
    }
}

async fn fsqlite_integrity(conn: &Connection) -> String {
    let rows = conn
        .query("PRAGMA integrity_check")
        .await
        .expect("fsqlite integrity_check");
    rows.iter()
        .map(|row| match row.values()[0] {
            SqliteValue::Text(ref text) => text.to_string(),
            ref other => format!("{other:?}"),
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// One CONCURRENT transaction inserting `rows` rows; returns the next id.
async fn grow(conn: &Connection, first_id: i64, rows: i64) -> i64 {
    conn.execute("BEGIN CONCURRENT").await.expect("begin");
    for id in first_id..first_id + rows {
        conn.execute(&format!(
            "INSERT INTO t(id, pad) VALUES ({id}, '{}')",
            "x".repeat(PAD)
        ))
        .await
        .unwrap_or_else(|error| panic!("insert id {id}: {error:?}"));
    }
    conn.execute("COMMIT").await.expect("commit");
    first_id + rows
}

#[derive(Debug)]
struct StockReport {
    integrity: String,
    page_count: i64,
    freelist_count: i64,
    rows: i64,
}

fn stock_read(db: &Path) -> StockReport {
    let stock = rusqlite::Connection::open_with_flags(
        db,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .expect("stock open");
    stock.busy_timeout(BUSY_TIMEOUT).expect("stock busy");
    let integrity: Vec<String> = {
        let mut stmt = stock
            .prepare("PRAGMA integrity_check(100000)")
            .expect("prepare integrity_check");
        stmt.query_map([], |row| row.get::<_, String>(0))
            .expect("integrity_check")
            .collect::<Result<_, _>>()
            .expect("integrity rows")
    };
    let count = |sql: &str| -> i64 { stock.query_row(sql, [], |r| r.get(0)).expect(sql) };
    StockReport {
        integrity: integrity.join("\n"),
        page_count: count("PRAGMA page_count"),
        freelist_count: count("PRAGMA freelist_count"),
        rows: count("SELECT count(*) FROM t"),
    }
}

#[test]
fn concurrent_growth_commit_returns_its_unused_lease_tail_to_eof() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = dir.path().join("b1.db");

        let conn = open(&db).await;
        conn.execute("PRAGMA journal_mode=WAL").await.expect("wal");
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, pad TEXT)")
            .await
            .expect("create");
        let mut pages = scalar(&conn, "PRAGMA page_count").await;
        assert_eq!(
            scalar(&conn, "PRAGMA freelist_count").await,
            0,
            "precondition: an empty freelist, so every new page comes from EOF"
        );

        let mut next_id = 1;
        let mut rounds_with_a_tail = 0;
        for (round, rows) in ROUNDS.into_iter().enumerate() {
            next_id = grow(&conn, next_id, rows).await;
            let after = scalar(&conn, "PRAGMA page_count").await;
            let free = scalar(&conn, "PRAGMA freelist_count").await;
            assert_eq!(
                free, 0,
                "bd-emim5 B1 round {round} ({rows} rows, page count {pages} -> {after}): a \
                 pure-insert growth commit leaves freelist_count unchanged; its unused lease \
                 pages are neither inside its page count nor published as free"
            );
            // A transaction's first page comes alone and later ones in leases of 8,
            // so a growth of g pages left an unused tail unless g - 1 is a whole
            // number of leases.
            let grown = after - pages;
            if grown >= 2 && (grown - 1) % 8 != 0 {
                rounds_with_a_tail += 1;
            }
            pages = after;
        }
        let total_rows = next_id - 1;
        conn.close().await.expect("close");

        let stock = stock_read(&db);
        assert_eq!(
            stock.integrity, "ok",
            "bd-emim5 B1: stock integrity_check (no page 'never used'): {stock:?}"
        );
        assert_eq!(
            (stock.page_count, stock.freelist_count, stock.rows),
            (pages, 0, total_rows),
            "bd-emim5 B1: stock sees the committed page count, no free page and every row"
        );
        // Checked after stock's verdict, so a build that drops the unused pages
        // inside its page count (growth in whole leases) fails on "never used".
        assert!(
            rounds_with_a_tail >= 2,
            "vacuity: at least two rounds must use part of a lease ({rounds_with_a_tail})"
        );

        // Reopen: the next growth starts at the first page past the committed size,
        // where the returned tail began.
        let conn = open(&db).await;
        assert_eq!(scalar(&conn, "PRAGMA page_count").await, pages);
        next_id = grow(&conn, next_id, REOPEN_ROWS).await;
        let grown_pages = scalar(&conn, "PRAGMA page_count").await;
        assert!(
            grown_pages > pages,
            "vacuity: the reopened connection grows the file ({pages} -> {grown_pages})"
        );
        assert_eq!(fsqlite_integrity(&conn).await, "ok", "fsqlite after reopen");
        conn.close().await.expect("close reopened");

        let stock = stock_read(&db);
        assert_eq!(
            stock.integrity, "ok",
            "bd-emim5 B1: stock integrity_check after the reopened growth: {stock:?}"
        );
        assert_eq!(
            (stock.page_count, stock.freelist_count, stock.rows),
            (grown_pages, 0, next_id - 1),
            "bd-emim5 B1: the reopened growth is contiguous from the old committed size"
        );
    });
}
