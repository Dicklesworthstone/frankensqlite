#![recursion_limit = "512"]

//! bd-uaxab / GH #275: `PRAGMA cache_spill` set/get.
//!
//! The full clamp algebra (OFF -> 0, ON does not reset the threshold, negative
//! is a KiB budget, cache_size drives cache_pages) is covered by the fsqlite-vdbe
//! unit test `test_connection_pragma_cache_spill_gh275`. This integration test
//! covers what the unit test cannot: that an *assignment* emits ZERO rows while a
//! bare query emits exactly one integer row = `max(cache_pages, szSpill)`, all
//! surfaced through `Connection::query`. A positive `cache_size` is used so
//! `cache_pages` is exactly that many pages, independent of the C-build `szExtra`
//! artifact (which FrankenSQLite deliberately does not match byte-for-byte).

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

async fn rows(conn: &Connection, sql: &str) -> Vec<Vec<SqliteValue>> {
    conn.query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
        .iter()
        .map(|r| r.values().to_vec())
        .collect()
}

async fn one_int(conn: &Connection, sql: &str) -> i64 {
    let r = rows(conn, sql).await;
    assert_eq!(r.len(), 1, "`{sql}` must emit exactly one row, got {r:?}");
    match &r[0][0] {
        SqliteValue::Integer(n) => *n,
        other => panic!("`{sql}` must emit an integer, got {other:?}"),
    }
}

#[test]
fn cache_spill_no_rows_on_assign_and_algebra_gh275() {
    asupersync::test_utils::run_test(|| async {
        let c = Connection::open(":memory:").await.unwrap();

        // Positive cache_size => cache_pages is exactly 1000 pages.
        c.execute("PRAGMA cache_size=1000").await.unwrap();

        // Assignment emits NO rows.
        assert!(
            rows(&c, "PRAGMA cache_spill=500").await.is_empty(),
            "`PRAGMA cache_spill=500` must emit no rows"
        );
        // Bare query emits one row = max(cache_pages=1000, szSpill=500) = 1000.
        assert_eq!(one_int(&c, "PRAGMA cache_spill").await, 1000);

        // Raise the threshold above cache_pages: readback follows the threshold.
        assert!(rows(&c, "PRAGMA cache_spill=5000").await.is_empty());
        assert_eq!(one_int(&c, "PRAGMA cache_spill").await, 5000);

        // OFF disables spilling: assignment still emits no rows; bare reads 0.
        assert!(rows(&c, "PRAGMA cache_spill=OFF").await.is_empty());
        assert_eq!(one_int(&c, "PRAGMA cache_spill").await, 0);

        // ON re-enables without resetting the threshold (still 5000).
        assert!(rows(&c, "PRAGMA cache_spill=ON").await.is_empty());
        assert_eq!(one_int(&c, "PRAGMA cache_spill").await, 5000);
    });
}
