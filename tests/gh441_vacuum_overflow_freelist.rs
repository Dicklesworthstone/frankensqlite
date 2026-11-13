#![recursion_limit = "512"]

//! GH#441: VACUUM of a database whose rows spill onto overflow pages left the
//! rebuilt file with a large freelist (44-64% of its pages), where stock
//! SQLite's VACUUM leaves none. The live page count already matched stock, so
//! the rebuilt image carried free pages it never needed.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn int(conn_rows: &[fsqlite_core::connection::Row]) -> i64 {
    match conn_rows[0].values()[0] {
        SqliteValue::Integer(v) => v,
        ref other => panic!("expected integer, got {other:?}"),
    }
}

async fn pragma(conn: &Connection, sql: &str) -> i64 {
    int(&conn.query(sql).await.expect(sql))
}

/// Stock SQLite page/freelist counts after the same workload and VACUUM.
fn stock_counts(path: &std::path::Path, rows: usize, value_bytes: usize) -> (i64, i64) {
    let conn = rusqlite::Connection::open(path).expect("stock open");
    conn.execute_batch(
        "PRAGMA journal_mode=WAL;
         CREATE TABLE t (id INTEGER PRIMARY KEY, k TEXT NOT NULL, v TEXT NOT NULL);
         CREATE INDEX t_k ON t (k);
         CREATE INDEX t_v ON t (v);",
    )
    .expect("stock ddl");
    conn.execute_batch("BEGIN").expect("stock begin");
    for i in 0..rows {
        let k = (i * 7919) % rows;
        conn.execute(
            "INSERT INTO t (id, k, v) VALUES (?1, ?2, ?3)",
            rusqlite::params![
                i64::try_from(i).expect("id"),
                format!("key-{k:012}"),
                "x".repeat(value_bytes)
            ],
        )
        .expect("stock insert");
    }
    conn.execute_batch("COMMIT; DROP INDEX t_v; DELETE FROM t WHERE id % 2 = 0; VACUUM;")
        .expect("stock workload");
    let free: i64 = conn
        .query_row("PRAGMA freelist_count", [], |r| r.get(0))
        .expect("stock freelist");
    let pages: i64 = conn
        .query_row("PRAGMA page_count", [], |r| r.get(0))
        .expect("stock pages");
    (free, pages)
}

async fn check(rows: usize, value_bytes: usize) {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("v.db");
    let path_str = path.to_string_lossy().into_owned();
    {
        let conn = Connection::open(&path_str).await.expect("open");
        conn.execute(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, k TEXT NOT NULL, v TEXT NOT NULL);",
        )
        .await
        .expect("create table");
        conn.execute("CREATE INDEX t_k ON t (k);").await.expect("index k");
        conn.execute("CREATE INDEX t_v ON t (v);").await.expect("index v");
        conn.execute("BEGIN;").await.expect("begin");
        let value = "x".repeat(value_bytes);
        for i in 0..rows {
            let k = (i * 7919) % rows;
            conn.execute(&format!(
                "INSERT INTO t (id, k, v) VALUES ({i}, 'key-{k:012}', '{value}');"
            ))
            .await
            .expect("insert");
        }
        conn.execute("COMMIT;").await.expect("commit");
        conn.execute("DROP INDEX t_v;").await.expect("drop index");
        conn.execute("DELETE FROM t WHERE id % 2 = 0;")
            .await
            .expect("delete");
        conn.close().await.expect("close");
    }

    let conn = Connection::open(&path_str).await.expect("reopen");
    conn.execute("VACUUM;").await.expect("vacuum");
    let free = pragma(&conn, "PRAGMA freelist_count;").await;
    let pages = pragma(&conn, "PRAGMA page_count;").await;
    let left = pragma(&conn, "SELECT COUNT(*) FROM t;").await;
    let check = conn.query("PRAGMA integrity_check;").await.expect("check");
    conn.close().await.expect("close vacuumed");

    assert_eq!(left, i64::try_from(rows / 2).expect("rows"));
    assert_eq!(
        check[0].values()[0],
        SqliteValue::Text("ok".into()),
        "integrity_check after VACUUM"
    );

    let stock_dir = tempfile::tempdir().expect("stock dir");
    let (stock_free, stock_pages) = stock_counts(&stock_dir.path().join("s.db"), rows, value_bytes);
    assert_eq!(stock_free, 0);
    assert_eq!(
        free, 0,
        "VACUUM must leave no free pages (rows={rows} value_bytes={value_bytes}, \
         page_count={pages}, stock page_count={stock_pages})"
    );
    // The rebuilt b-trees need not be byte-identical to stock, but the file
    // must be the size of its live pages, which matched stock within a page.
    assert!(
        (pages - stock_pages).abs() <= 2,
        "page_count {pages} vs stock {stock_pages} (rows={rows} value_bytes={value_bytes})"
    );

    // Stock SQLite agrees the vacuumed file is sound.
    let stock = rusqlite::Connection::open(&path).expect("stock reopen");
    let stock_check: String = stock
        .query_row("PRAGMA integrity_check", [], |r| r.get(0))
        .expect("stock integrity_check");
    assert_eq!(stock_check, "ok");
}

#[test]
fn gh441_vacuum_leaves_no_free_pages_with_overflow_rows() {
    asupersync::test_utils::run_test(|| async {
        check(600, 10_000).await;
        check(300, 40_000).await;
        check(2_000, 120).await;
    });
}
