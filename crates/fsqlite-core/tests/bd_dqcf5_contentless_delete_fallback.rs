//! bd-dqcf5: a contentless_delete FTS5 table, reopened lazily, that takes the
//! origin-NULL DELETE fallback (`_docsize.origin` is NULL, so the incremental
//! tombstone delete returns None and the connection promotes + full-re-encodes)
//! must NOT lose the rest of the corpus.
//!
//! The bd-dqcf5 guard (ad2cc0dad) aborts with a typed error if the full
//! re-encode would write an empty index over a table with live rows. This
//! keeper drives the exact connection path — promote from on-disk segments,
//! in-memory delete of one row, full re-encode — and asserts the surviving
//! corpus is intact (or, if the guard fires, that the bug is live at the
//! connection level and the guard caught data loss).
#![cfg(feature = "ext-fts5")]

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

async fn match_rowids(conn: &Connection, term: &str) -> Vec<i64> {
    let mut ids: Vec<i64> = conn
        .query(&format!(
            "SELECT rowid FROM t WHERE t MATCH '{term}' ORDER BY rowid;"
        ))
        .await
        .unwrap_or_else(|e| panic!("MATCH '{term}' failed: {e}"))
        .iter()
        .map(|row| match row.values()[0] {
            SqliteValue::Integer(n) => n,
            ref other => panic!("expected rowid integer, got {other:?}"),
        })
        .collect();
    ids.sort_unstable();
    ids
}

#[test]
fn bd_dqcf5_contentless_delete_origin_null_fallback_preserves_corpus() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("temp dir");
        let db_path = dir.path().join("bd_dqcf5.db");
        let db_str = db_path.to_string_lossy().into_owned();

        // A contentless_delete table with several rows across incremental
        // segments, then close so the reopen binds lazily.
        {
            let conn = Connection::open(&db_str).await.expect("open franken");
            conn.execute(
                "CREATE VIRTUAL TABLE t USING fts5(body, content='', contentless_delete=1);",
            )
            .await
            .expect("create contentless_delete fts5");
            for id in 1..=8 {
                conn.execute(&format!(
                    "INSERT INTO t(rowid, body) VALUES ({id}, 'common word{id}');"
                ))
                .await
                .expect("insert");
            }
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
                .await
                .expect("checkpoint");
            conn.close().await.expect("close");
        }

        // Make row 3's `_docsize.origin` NULL, simulating a row that predates
        // origin tracking — this forces the incremental-delete tombstone path
        // to return None and the connection to fall back to promote +
        // full-re-encode.
        {
            let stock = rusqlite::Connection::open(&db_path).expect("stock open");
            // _docsize is (id, sz, origin) for contentless_delete; NULL the origin.
            let changed = stock
                .execute("UPDATE t_docsize SET origin = NULL WHERE id = 3", [])
                .expect("null the origin of row 3");
            assert_eq!(changed, 1, "exactly one docsize row updated");
            let integrity: String = stock
                .query_row("PRAGMA integrity_check;", [], |r| r.get(0))
                .expect("stock integrity_check after origin NULL");
            assert_eq!(integrity, "ok");
        }

        // Reopen lazily and delete row 3. This takes the origin-NULL fallback:
        // promote from segments -> in-memory delete -> full re-encode.
        {
            let conn = Connection::open(&db_str).await.expect("reopen franken");
            assert_eq!(
                match_rowids(&conn, "common").await,
                vec![1, 2, 3, 4, 5, 6, 7, 8],
                "lazy reopen reads the full corpus"
            );

            // If the bd-dqcf5 bug is live, this DELETE's full re-encode blanks
            // the index; the guard turns that into an error. Surface either
            // outcome explicitly.
            let deleted = conn.execute("DELETE FROM t WHERE rowid = 3;").await;
            match deleted {
                Ok(_) => {}
                Err(e) => panic!(
                    "DELETE hit the bd-dqcf5 guard (the connection full re-encode was empty): {e}"
                ),
            }

            // The other seven rows must survive the origin-NULL delete.
            assert_eq!(
                match_rowids(&conn, "common").await,
                vec![1, 2, 4, 5, 6, 7, 8],
                "corpus minus the deleted row survives the origin-NULL delete (bd-dqcf5)"
            );
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
                .await
                .expect("checkpoint");
            conn.close().await.expect("close");
        }

        // Reopen and re-check, and verify stock still sees the seven rows.
        {
            let conn = Connection::open(&db_str).await.expect("second reopen");
            assert_eq!(
                match_rowids(&conn, "common").await,
                vec![1, 2, 4, 5, 6, 7, 8],
                "the seven rows persist across reopen"
            );
            conn.close().await.expect("close");
        }
        let stock = rusqlite::Connection::open(&db_path).expect("stock open");
        let integrity: String = stock
            .query_row("PRAGMA integrity_check;", [], |r| r.get(0))
            .expect("final stock integrity_check");
        assert_eq!(integrity, "ok");
        let docsize_rows: i64 = stock
            .query_row("SELECT count(*) FROM t_docsize", [], |r| r.get(0))
            .expect("docsize count");
        assert_eq!(docsize_rows, 7, "seven live docsize rows remain");
    });
}
