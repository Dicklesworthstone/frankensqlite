//! Stock-differential keeper for `bd-71uar`: sequential `INSERT ... SELECT`
//! leaf/interior packing and zero freelist pages across depth-2 and depth-3
//! tables, WAL and rollback-journal (`DELETE`) modes, autocommit and explicit
//! transactions, and `:memory:`.
//!
//! Compares every scenario against live bundled SQLite 3.54.1 (`rusqlite`)
//! in the same run and verifies that stock SQLite reads every fsqlite-written
//! file with `PRAGMA integrity_check = ok` and identical row contents.

use std::path::Path;

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;
use rusqlite::OpenFlags;

#[derive(Debug, Clone, PartialEq, Eq)]
struct DbGeometry {
    integrity: String,
    page_count: i64,
    freelist_count: i64,
    table_leaves: i64,
    table_internal: i64,
    min_leaf_cells: i64,
    row_count: i64,
    checksum: i64,
}

fn inspect_with_stock(path: &Path) -> DbGeometry {
    let conn = rusqlite::Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .expect("stock open");
    let integrity: String = conn
        .query_row("PRAGMA integrity_check(100000);", [], |r| r.get(0))
        .expect("integrity_check");
    let page_count: i64 = conn
        .query_row("PRAGMA page_count;", [], |r| r.get(0))
        .expect("page_count");
    let freelist_count: i64 = conn
        .query_row("PRAGMA freelist_count;", [], |r| r.get(0))
        .expect("freelist_count");
    let table_leaves: i64 = conn
        .query_row(
            "SELECT count(*) FROM dbstat WHERE name = 't' AND pagetype = 'leaf';",
            [],
            |r| r.get(0),
        )
        .expect("table_leaves");
    let table_internal: i64 = conn
        .query_row(
            "SELECT count(*) FROM dbstat WHERE name = 't' AND pagetype = 'internal';",
            [],
            |r| r.get(0),
        )
        .expect("table_internal");
    let min_leaf_cells: i64 = conn
        .query_row(
            "SELECT coalesce(min(ncell), 0) FROM dbstat WHERE name = 't' AND pagetype = 'leaf';",
            [],
            |r| r.get(0),
        )
        .expect("min_leaf_cells");
    let (row_count, checksum): (i64, i64) = conn
        .query_row(
            "SELECT count(*), coalesce(sum(id * length(pad)), 0) FROM t;",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .expect("row summary");
    DbGeometry {
        integrity,
        page_count,
        freelist_count,
        table_leaves,
        table_internal,
        min_leaf_cells,
        row_count,
        checksum,
    }
}

async fn fsqlite_scalar_i64(conn: &Connection, sql: &str) -> i64 {
    let rows = conn.query(sql).await.expect(sql);
    match rows[0].values()[0] {
        SqliteValue::Integer(v) => v,
        ref other => panic!("expected integer from `{sql}`, got {other:?}"),
    }
}

async fn run_file_case(
    dir: &Path,
    label: &str,
    journal_mode: &str,
    explicit_txn: bool,
    rows: i64,
) -> (DbGeometry, DbGeometry) {
    let fpath = dir.join(format!("fsqlite_{label}.db"));
    let spath = dir.join(format!("stock_{label}.db"));
    let fpath_str = fpath.to_str().expect("utf-8 fpath").to_owned();

    let insert_sql = format!(
        "WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x + 1 FROM c WHERE x < {rows}) \
         INSERT INTO t SELECT x, printf('%0500d', x) FROM c;"
    );

    // Build stock reference file.
    {
        let stock = rusqlite::Connection::open(&spath).expect("stock open");
        let _: String = stock
            .query_row(&format!("PRAGMA journal_mode = {journal_mode};"), [], |r| {
                r.get(0)
            })
            .expect("stock journal_mode");
        if explicit_txn {
            stock.execute_batch("BEGIN;").expect("stock begin");
        }
        stock
            .execute_batch("CREATE TABLE t(id INTEGER PRIMARY KEY, pad TEXT);")
            .expect("stock create");
        stock.execute_batch(&insert_sql).expect("stock insert");
        if explicit_txn {
            stock.execute_batch("COMMIT;").expect("stock commit");
        }
        if journal_mode.eq_ignore_ascii_case("wal") {
            let _: (i64, i64, i64) = stock
                .query_row("PRAGMA wal_checkpoint(TRUNCATE);", [], |r| {
                    Ok((r.get(0)?, r.get(1)?, r.get(2)?))
                })
                .expect("stock checkpoint");
        }
    }

    // Build FrankenSQLite file.
    {
        let conn = Connection::open(&fpath_str).await.expect("fsqlite open");
        conn.query(&format!("PRAGMA journal_mode = {journal_mode};"))
            .await
            .expect("fsqlite journal_mode");
        if explicit_txn {
            conn.execute("BEGIN;").await.expect("fsqlite begin");
        }
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, pad TEXT);")
            .await
            .expect("fsqlite create");
        conn.execute(&insert_sql).await.expect("fsqlite insert");
        if explicit_txn {
            conn.execute("COMMIT;").await.expect("fsqlite commit");
        }
        if journal_mode.eq_ignore_ascii_case("wal") {
            conn.query("PRAGMA wal_checkpoint(TRUNCATE);")
                .await
                .expect("fsqlite checkpoint");
        }
        conn.close().await.expect("fsqlite close");
    }

    let stock_geo = inspect_with_stock(&spath);
    let fsqlite_geo = inspect_with_stock(&fpath);
    eprintln!("bd-71uar [{label}] stock={stock_geo:?} fsqlite={fsqlite_geo:?}");
    (stock_geo, fsqlite_geo)
}

#[test]
fn bd_71uar_sequential_insert_select_matches_stock_leaf_packing_and_zero_freelist() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");

        for (label, mode, explicit_txn, rows) in [
            ("wal_autocommit_400", "WAL", false, 400),
            ("wal_explicit_400", "WAL", true, 400),
            ("delete_autocommit_400", "DELETE", false, 400),
            ("delete_explicit_400", "DELETE", true, 400),
            ("wal_autocommit_4000", "WAL", false, 4000),
            ("wal_autocommit_8000", "WAL", false, 8000),
        ] {
            let (stock, ours) = run_file_case(dir.path(), label, mode, explicit_txn, rows).await;
            assert_eq!(ours.integrity, "ok", "{label}: stock integrity_check");
            assert_eq!(ours.row_count, stock.row_count, "{label}: row_count");
            assert_eq!(ours.checksum, stock.checksum, "{label}: checksum");
            assert_eq!(
                ours.freelist_count, 0,
                "{label}: pure INSERT...SELECT must leave zero freelist pages (stock={stock:?}, ours={ours:?})"
            );
            assert_eq!(
                ours.table_leaves, stock.table_leaves,
                "{label}: leaf page count must match stock (stock={stock:?}, ours={ours:?})"
            );
            assert_eq!(
                ours.table_internal, stock.table_internal,
                "{label}: internal page count must match stock (stock={stock:?}, ours={ours:?})"
            );
            assert_eq!(
                ours.page_count, stock.page_count,
                "{label}: total page_count must match stock (stock={stock:?}, ours={ours:?})"
            );
            assert_eq!(
                ours.min_leaf_cells, stock.min_leaf_cells,
                "{label}: every leaf must stay packed like stock (stock={stock:?}, ours={ours:?})"
            );
        }

        // Also check :memory: autocommit and explicit transaction page/freelist parity.
        for (label, explicit_txn) in [("mem_autocommit_400", false), ("mem_explicit_400", true)] {
            let insert_sql = "WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x + 1 FROM c WHERE x < 400) \
                              INSERT INTO t SELECT x, printf('%0500d', x) FROM c;";

            let stock = rusqlite::Connection::open_in_memory().expect("stock mem");
            if explicit_txn {
                stock.execute_batch("BEGIN;").expect("stock mem begin");
            }
            stock
                .execute_batch("CREATE TABLE t(id INTEGER PRIMARY KEY, pad TEXT);")
                .expect("stock mem create");
            stock.execute_batch(insert_sql).expect("stock mem insert");
            if explicit_txn {
                stock.execute_batch("COMMIT;").expect("stock mem commit");
            }
            let stock_pages: i64 = stock
                .query_row("PRAGMA page_count;", [], |r| r.get(0))
                .expect("stock mem page_count");
            let stock_free: i64 = stock
                .query_row("PRAGMA freelist_count;", [], |r| r.get(0))
                .expect("stock mem freelist_count");

            let conn = Connection::open(":memory:").await.expect("fsqlite mem");
            if explicit_txn {
                conn.execute("BEGIN;").await.expect("fsqlite mem begin");
            }
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, pad TEXT);")
                .await
                .expect("fsqlite mem create");
            conn.execute(insert_sql).await.expect("fsqlite mem insert");
            if explicit_txn {
                conn.execute("COMMIT;").await.expect("fsqlite mem commit");
            }
            let our_pages = fsqlite_scalar_i64(&conn, "PRAGMA page_count;").await;
            let our_free = fsqlite_scalar_i64(&conn, "PRAGMA freelist_count;").await;
            eprintln!(
                "bd-71uar [{label}] stock=({stock_pages},{stock_free}) fsqlite=({our_pages},{our_free})"
            );
            assert_eq!(our_free, stock_free, "{label}: freelist_count");
            assert_eq!(our_pages, stock_pages, "{label}: page_count");
            conn.close().await.expect("fsqlite mem close");
        }
    });
}

#[test]
fn bd_71uar_non_append_and_page1_root_split_preserve_integrity_and_rows() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let fpath = dir.path().join("fsqlite_non_append.db");
        let spath = dir.path().join("stock_non_append.db");
        let fpath_str = fpath.to_str().expect("utf-8 fpath").to_owned();

        let stock = rusqlite::Connection::open(&spath).expect("stock open");
        let conn = Connection::open(&fpath_str).await.expect("fsqlite open");

        // Force sqlite_schema (root_page == 1, 100-byte DB header) to split
        // across multiple pages, then exercise out-of-order / middle / descending
        // inserts on a user table.
        stock.execute_batch("BEGIN;").expect("stock begin");
        conn.execute("BEGIN;").await.expect("fsqlite begin");
        for i in 0..60 {
            let ddl = format!(
                "CREATE TABLE extra_{i:02}(id INTEGER PRIMARY KEY, col_a TEXT, col_b TEXT, col_c TEXT);"
            );
            stock.execute_batch(&ddl).expect("stock ddl");
            conn.execute(&ddl).await.expect("fsqlite ddl");
        }
        let create_t = "CREATE TABLE t(id INTEGER PRIMARY KEY, pad TEXT);";
        stock.execute_batch(create_t).expect("stock create t");
        conn.execute(create_t).await.expect("fsqlite create t");

        // Interleave descending, middle, and ascending keys across multiple splits.
        let mut keys = Vec::new();
        for i in (1..=100).rev() {
            keys.push(i * 4);
        }
        for i in 1..=100 {
            keys.push(i * 4 - 2);
        }
        for i in 1..=100 {
            keys.push(i * 4 - 1);
        }
        for i in 1..=100 {
            keys.push(i * 4 - 3);
        }
        for key in keys {
            let sql = format!("INSERT INTO t VALUES ({key}, printf('%0500d', {key}));");
            stock.execute_batch(&sql).expect("stock insert");
            conn.execute(&sql).await.expect("fsqlite insert");
        }
        stock.execute_batch("COMMIT;").expect("stock commit");
        conn.execute("COMMIT;").await.expect("fsqlite commit");
        conn.query("PRAGMA wal_checkpoint(TRUNCATE);")
            .await
            .expect("fsqlite checkpoint");
        conn.close().await.expect("fsqlite close");
        drop(stock);

        let stock_geo = inspect_with_stock(&spath);
        let fsqlite_geo = inspect_with_stock(&fpath);
        eprintln!("bd-71uar [non_append_and_page1] stock={stock_geo:?} fsqlite={fsqlite_geo:?}");
        assert_eq!(fsqlite_geo.integrity, "ok", "stock integrity_check");
        assert_eq!(fsqlite_geo.row_count, stock_geo.row_count, "row_count");
        assert_eq!(fsqlite_geo.checksum, stock_geo.checksum, "checksum");
    });
}
