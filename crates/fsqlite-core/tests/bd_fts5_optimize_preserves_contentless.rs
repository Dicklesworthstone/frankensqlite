//! Regression: `optimize` (and any other promote path) on a lazy `contentless_delete`
//! FTS5 table must hydrate the in-memory index from the persisted `_data` segments,
//! NOT rebuild from the (empty) `_content` shadow.
//!
//! A contentless table keeps no `_content`, so the segments are the only copy of the
//! corpus. Before the fix, `promote_lazy_fts5_table` rebuilt from empty content on
//! `optimize` — blanking the in-memory index for the session, and permanently
//! dropping the corpus on the next write (which re-encodes `_data` from the empty
//! index). Now exposed by default since lazy rebind is default-on (GH#358).

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
fn bd_fts5_optimize_preserves_lazy_contentless_corpus() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("fts5_optimize_preserve.db");
        let db_str = db_path.to_string_lossy().into_owned();

        // Write a contentless_delete corpus, then reopen so it binds lazily.
        {
            let conn = Connection::open(&db_str).await.unwrap();
            conn.execute(
                "CREATE VIRTUAL TABLE t USING fts5(body, content='', contentless_delete=1);",
            )
            .await
            .unwrap();
            for id in 1..=5 {
                conn.execute(&format!(
                    "INSERT INTO t(rowid, body) VALUES ({id}, 'common term{id}');"
                ))
                .await
                .unwrap();
            }
            conn.close().await.unwrap();
        }

        let conn = Connection::open(&db_str).await.unwrap();
        assert_eq!(
            rowids(
                &conn
                    .query("SELECT rowid FROM t WHERE t MATCH 'common' ORDER BY rowid;")
                    .await
                    .unwrap()
            ),
            vec![1, 2, 3, 4, 5],
            "reopened lazily with the full corpus"
        );

        // optimize promotes the lazy table — must hydrate from segments, not blank it.
        conn.execute("INSERT INTO t(t) VALUES('optimize');")
            .await
            .unwrap();
        assert_eq!(
            rowids(
                &conn
                    .query("SELECT rowid FROM t WHERE t MATCH 'common' ORDER BY rowid;")
                    .await
                    .unwrap()
            ),
            vec![1, 2, 3, 4, 5],
            "corpus survives optimize (was blanked before the fix)"
        );

        // A write after promote must not drop the old corpus.
        conn.execute("INSERT INTO t(rowid, body) VALUES (100, 'common fresh');")
            .await
            .unwrap();
        assert_eq!(
            rowids(
                &conn
                    .query("SELECT rowid FROM t WHERE t MATCH 'common' ORDER BY rowid;")
                    .await
                    .unwrap()
            ),
            vec![1, 2, 3, 4, 5, 100],
            "old rows + the new row after a post-optimize write"
        );
        conn.close().await.unwrap();

        // Stock reads the durable image: nothing was lost on disk.
        let stock = rusqlite::Connection::open(&db_path).unwrap();
        let integrity: String = stock
            .query_row("PRAGMA integrity_check;", [], |r| r.get(0))
            .unwrap();
        assert_eq!(integrity, "ok");
        let matched: Vec<i64> = stock
            .prepare("SELECT rowid FROM t WHERE t MATCH 'common' ORDER BY rowid")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(matched, vec![1, 2, 3, 4, 5, 100], "durable corpus intact");
    });
}
