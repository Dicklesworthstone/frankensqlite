//! bd-zqc1b: lazy `contentless_delete` FTS5 — DELETE and full-column UPDATE
//! after a reopen must not corrupt the row count or invert the index.
//!
//! These guard the GH#358 lazy-default blast radius:
//!   A. `count(*)` after deleting one of N rows on a reopened (lazy) table must
//!      report N-1, not 0 (regression: the promote fallback emptied the index,
//!      so the re-persisted averages `total_rows` collapsed to 0).
//!   B. A full-column UPDATE on a reopened lazy table must reindex to the new
//!      term and drop the old one — matching stock — not keep the old term
//!      and suppress the new one.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn rowids(rows: &[fsqlite_core::connection::Row]) -> Vec<i64> {
    rows.iter()
        .map(|row| match row.values()[0] {
            SqliteValue::Integer(n) => n,
            ref other => panic!("expected rowid integer, got {other:?}"),
        })
        .collect()
}

fn scalar_i64(rows: &[fsqlite_core::connection::Row]) -> i64 {
    assert_eq!(rows.len(), 1, "expected exactly one row");
    match rows[0].values()[0] {
        SqliteValue::Integer(n) => n,
        ref other => panic!("expected integer scalar, got {other:?}"),
    }
}

#[test]
fn bd_zqc1b_count_after_delete_on_reopened_lazy() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("zqc1b_delete.db");
        let db_str = db_path.to_string_lossy().into_owned();

        // Write a 3-row contentless_delete corpus, then reopen so it binds lazily.
        {
            let conn = Connection::open(&db_str).await.unwrap();
            conn.execute(
                "CREATE VIRTUAL TABLE t USING fts5(body, content='', contentless_delete=1);",
            )
            .await
            .unwrap();
            for id in 1..=3 {
                conn.execute(&format!(
                    "INSERT INTO t(rowid, body) VALUES ({id}, 'common term{id}');"
                ))
                .await
                .unwrap();
            }
            conn.close().await.unwrap();
        }

        let conn = Connection::open(&db_str).await.unwrap();
        let before = conn.query("SELECT count(*) FROM t;").await.unwrap();
        assert_eq!(scalar_i64(&before), 3, "reopened count before delete");

        // Delete the middle row.
        conn.execute("DELETE FROM t WHERE rowid = 2;")
            .await
            .unwrap();

        let after = conn.query("SELECT count(*) FROM t;").await.unwrap();
        assert_eq!(
            scalar_i64(&after),
            2,
            "count(*) after deleting 1 of 3 rows must be 2, not 0"
        );

        // The two survivors are still matchable; the deleted one is gone.
        let survivors = conn
            .query("SELECT rowid FROM t WHERE t MATCH 'common' ORDER BY rowid;")
            .await
            .unwrap();
        assert_eq!(rowids(&survivors), vec![1, 3], "survivors after delete");
        conn.close().await.unwrap();

        // Bundled stock reads the same post-delete image.
        let stock = rusqlite::Connection::open(&db_path).unwrap();
        let integrity: String = stock
            .query_row("PRAGMA integrity_check;", [], |r| r.get(0))
            .unwrap();
        assert_eq!(integrity, "ok", "stock integrity_check after delete");
        let matched: Vec<i64> = stock
            .prepare("SELECT rowid FROM t WHERE t MATCH 'common' ORDER BY rowid")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(matched, vec![1, 3], "stock: survivors after delete");
    });
}

#[test]
fn bd_zqc1b_full_column_update_on_reopened_lazy() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("zqc1b_update.db");
        let db_str = db_path.to_string_lossy().into_owned();

        // Single-column contentless_delete corpus; reopen to bind lazily.
        {
            let conn = Connection::open(&db_str).await.unwrap();
            conn.execute(
                "CREATE VIRTUAL TABLE t USING fts5(body, content='', contentless_delete=1);",
            )
            .await
            .unwrap();
            conn.execute("INSERT INTO t(rowid, body) VALUES (1, 'alpha');")
                .await
                .unwrap();
            conn.execute("INSERT INTO t(rowid, body) VALUES (2, 'gamma');")
                .await
                .unwrap();
            conn.close().await.unwrap();
        }

        let conn = Connection::open(&db_str).await.unwrap();
        // Full-column UPDATE (single column => assigns every column) replaces
        // rowid 1's term alpha -> beta.
        conn.execute("UPDATE t SET body = 'beta' WHERE rowid = 1;")
            .await
            .unwrap();

        let beta = conn
            .query("SELECT rowid FROM t WHERE t MATCH 'beta' ORDER BY rowid;")
            .await
            .unwrap();
        assert_eq!(rowids(&beta), vec![1], "new term beta is indexed for rowid 1");
        let alpha = conn
            .query("SELECT rowid FROM t WHERE t MATCH 'alpha';")
            .await
            .unwrap();
        assert!(alpha.is_empty(), "old term alpha dropped after full UPDATE");
        // The untouched row is unaffected.
        let gamma = conn
            .query("SELECT rowid FROM t WHERE t MATCH 'gamma' ORDER BY rowid;")
            .await
            .unwrap();
        assert_eq!(rowids(&gamma), vec![2], "untouched row still matches");
        conn.close().await.unwrap();

        // Bundled stock reads the same post-update image.
        let stock = rusqlite::Connection::open(&db_path).unwrap();
        let integrity: String = stock
            .query_row("PRAGMA integrity_check;", [], |r| r.get(0))
            .unwrap();
        assert_eq!(integrity, "ok", "stock integrity_check after update");
        let beta_stock: Vec<i64> = stock
            .prepare("SELECT rowid FROM t WHERE t MATCH 'beta' ORDER BY rowid")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(beta_stock, vec![1], "stock: beta indexed after update");
        let alpha_stock: Vec<i64> = stock
            .prepare("SELECT rowid FROM t WHERE t MATCH 'alpha'")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        assert!(alpha_stock.is_empty(), "stock: alpha dropped after update");
    });
}
