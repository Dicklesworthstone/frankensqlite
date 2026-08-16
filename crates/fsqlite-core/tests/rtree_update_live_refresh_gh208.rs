//! GH #208 (bd-gh-live-vtab-update-routing): an UPDATE on a live R*Tree virtual
//! table must be visible on the same connection.
//!
//! Before the fix the UPDATE persisted to the backing b-tree (a reopen showed
//! the new bounds) but the in-memory live RtreeVirtualTable instance serving
//! same-connection scans was never refreshed, so `SELECT` returned the stale
//! pre-update bounds while `changes()` still reported 1. The UPDATE is now
//! routed through the module (delete old row + insert recomputed row).
//!
//! Requires `--features ext-rtree`.

#![cfg(feature = "ext-rtree")]

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

async fn row(conn: &Connection, sql: &str) -> Vec<SqliteValue> {
    let rows = conn
        .query(sql)
        .await
        .unwrap_or_else(|e| panic!("query `{sql}`: {e}"));
    rows.first()
        .unwrap_or_else(|| panic!("query `{sql}`: no rows"))
        .values()
        .to_vec()
}

#[test]
fn rtree_update_is_visible_on_the_same_connection() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE VIRTUAL TABLE demo USING rtree(id, minx, maxx, miny, maxy)")
            .await
            .unwrap();
        conn.execute("INSERT INTO demo VALUES (1, 0.0, 1.0, 0.0, 1.0)")
            .await
            .unwrap();

        // Full UPDATE of every bound must be visible immediately (was stale).
        conn.execute("UPDATE demo SET minx=5.0, maxx=6.0, miny=5.0, maxy=6.0 WHERE id=1")
            .await
            .unwrap();
        assert_eq!(
            row(
                &conn,
                "SELECT id, minx, maxx, miny, maxy FROM demo WHERE id=1"
            )
            .await,
            vec![
                SqliteValue::Integer(1),
                SqliteValue::Float(5.0),
                SqliteValue::Float(6.0),
                SqliteValue::Float(5.0),
                SqliteValue::Float(6.0),
            ],
        );

        // Partial UPDATE — only maxx changes; the rest must be preserved.
        conn.execute("UPDATE demo SET maxx=20.0 WHERE id=1")
            .await
            .unwrap();
        assert_eq!(
            row(
                &conn,
                "SELECT id, minx, maxx, miny, maxy FROM demo WHERE id=1"
            )
            .await,
            vec![
                SqliteValue::Integer(1),
                SqliteValue::Float(5.0),
                SqliteValue::Float(20.0),
                SqliteValue::Float(5.0),
                SqliteValue::Float(6.0),
            ],
        );

        // The row count is unchanged (update, not insert/delete).
        assert_eq!(
            row(&conn, "SELECT count(*) FROM demo").await,
            vec![SqliteValue::Integer(1)],
        );
    });
}
