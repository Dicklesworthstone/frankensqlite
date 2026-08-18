//! bd-fts5-lazy-shadow-reads-itcc4.3 special commands: `'delete-all'`.
//!
//! `INSERT INTO t(t) VALUES('delete-all')` on a lazy `contentless_delete` FTS5
//! table clears the whole index WITHOUT hydrating the corpus, re-insert works
//! afterwards, and bundled stock SQLite reads the cleared image identically.
//! `'delete-all'` on a stored-content table is rejected, matching stock.

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

#[test]
fn bd_fts5_lazy_delete_all_clears_without_promote() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("fts5_delete_all.db");
        let db_str = db_path.to_string_lossy().into_owned();

        // Write a contentless_delete corpus, then reopen so it binds lazily.
        {
            let conn = Connection::open(&db_str).await.unwrap();
            conn.execute(
                "CREATE VIRTUAL TABLE t USING fts5(body, content='', contentless_delete=1);",
            )
            .await
            .unwrap();
            for id in 1..=6 {
                conn.execute(&format!(
                    "INSERT INTO t(rowid, body) VALUES ({id}, 'common term{id}');"
                ))
                .await
                .unwrap();
            }
            conn.close().await.unwrap();
        }

        let conn = Connection::open(&db_str).await.unwrap();
        let before = conn
            .query("SELECT rowid FROM t WHERE t MATCH 'common' ORDER BY rowid;")
            .await
            .unwrap();
        assert_eq!(rowids(&before), vec![1, 2, 3, 4, 5, 6], "reopened corpus");

        // delete-all clears every row.
        conn.execute("INSERT INTO t(t) VALUES('delete-all');")
            .await
            .unwrap();
        let after = conn
            .query("SELECT rowid FROM t WHERE t MATCH 'common';")
            .await
            .unwrap();
        assert!(after.is_empty(), "delete-all removed every row");

        // Re-insert works after the reset.
        conn.execute("INSERT INTO t(rowid, body) VALUES (100, 'fresh start');")
            .await
            .unwrap();
        let fresh = conn
            .query("SELECT rowid FROM t WHERE t MATCH 'fresh';")
            .await
            .unwrap();
        assert_eq!(rowids(&fresh), vec![100], "insert after delete-all");
        // The deleted terms stay gone.
        let common_gone = conn
            .query("SELECT rowid FROM t WHERE t MATCH 'common';")
            .await
            .unwrap();
        assert!(common_gone.is_empty(), "old terms did not resurrect");
        conn.close().await.unwrap();

        // Stock reads the cleared + re-inserted image.
        let stock = rusqlite::Connection::open(&db_path).unwrap();
        let integrity: String = stock
            .query_row("PRAGMA integrity_check;", [], |r| r.get(0))
            .unwrap();
        assert_eq!(integrity, "ok", "stock integrity_check after delete-all");
        let matched: Vec<i64> = stock
            .prepare("SELECT rowid FROM t WHERE t MATCH 'common'")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        assert!(matched.is_empty(), "stock: delete-all cleared the corpus");
        let fresh_stock: Vec<i64> = stock
            .prepare("SELECT rowid FROM t WHERE t MATCH 'fresh'")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(fresh_stock, vec![100], "stock reads the post-reset insert");
    });
}

#[test]
fn bd_fts5_delete_all_rejects_non_contentless() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("fts5_delete_all_reject.db");
        let db_str = db_path.to_string_lossy().into_owned();

        let conn = Connection::open(&db_str).await.unwrap();
        // Stored-content table (no `content=` option): stock rejects 'delete-all'.
        conn.execute("CREATE VIRTUAL TABLE t USING fts5(body);")
            .await
            .unwrap();
        conn.execute("INSERT INTO t(rowid, body) VALUES (1, 'hello world');")
            .await
            .unwrap();

        let err = conn
            .execute("INSERT INTO t(t) VALUES('delete-all');")
            .await
            .expect_err("'delete-all' must be rejected on a stored-content table");
        assert!(
            err.to_string().contains("contentless"),
            "unexpected error: {err}"
        );
        conn.close().await.unwrap();
    });
}
