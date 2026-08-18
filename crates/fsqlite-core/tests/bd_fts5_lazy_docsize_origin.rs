//! bd-fts5-lazy-shadow-reads-itcc4.3 Stage 1 (v2 origin-targeted lazy DELETE) —
//! `_docsize.origin` persistence oracle.
//!
//! A `contentless_delete=1` FTS5 table's `_docsize` shadow is
//! `(id INTEGER PRIMARY KEY, sz BLOB, origin INTEGER)`. The `origin` column
//! identifies the segment a rowid was inserted into, so a later *lazy* DELETE
//! can tombstone the owning segment without hydrating the whole corpus. Until
//! this slice, fsqlite created the column but never populated it (origin =
//! NULL), so origin-targeted delete had nothing to key on.
//!
//! This oracle proves fsqlite now populates `origin` on the incremental append
//! path (the one used for lazy contentless inserts), that each batch's rows
//! carry that batch's segment origin (monotonic across batches), and that a
//! bundled stock C SQLite reopens the image, passes `integrity_check`, and
//! still answers MATCH over the full corpus — i.e. writing the 3rd column did
//! not corrupt the shadow.
//!
//! Known boundary (Stage 1b follow-up): the *first* INSERT into a fresh table
//! lands via the full re-encode path, which still writes a 2-column docsize
//! row, so those rows read back `origin = NULL` and a delete of them falls back
//! to promote (correct, just not yet lazy). The assertions below pin that
//! boundary explicitly.

use fsqlite_core::connection::Connection;

#[test]
fn bd_fts5_lazy_docsize_origin_populated_and_stock_readable() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("fts5_lazy_docsize_origin.db");
        let db_str = db_path.to_string_lossy().into_owned();

        {
            let conn = Connection::open(&db_str).await.expect("open fresh db");
            conn.execute(
                "CREATE VIRTUAL TABLE t USING fts5(body, content='', contentless_delete=1);",
            )
            .await
            .expect("create contentless_delete fts5");

            // Batch 1 (rows 1,2): first insert into a fresh table -> full
            // re-encode path (lays down the first segment + shadows). Docsize
            // origin stays NULL here (the Stage 1b boundary).
            conn.execute("INSERT INTO t(rowid, body) VALUES (1, 'alpha rust'), (2, 'beta rust');")
                .await
                .expect("batch 1 insert");
            // Batch 2 (rows 3,4): incremental append -> a new segment with its
            // own origin; docsize origin is populated for these rows.
            conn.execute("INSERT INTO t(rowid, body) VALUES (3, 'gamma rust'), (4, 'delta search');")
                .await
                .expect("batch 2 insert");
            // Batch 3 (rows 5,6): another incremental append -> a later, larger
            // origin than batch 2.
            conn.execute("INSERT INTO t(rowid, body) VALUES (5, 'epsilon rust'), (6, 'zeta search');")
                .await
                .expect("batch 3 insert");

            conn.close().await.expect("flush + close");
        }

        // ── Stock C SQLite reopens the fsqlite image. ─────────────────────
        let stock = rusqlite::Connection::open(&db_path).expect("rusqlite open of fsqlite image");

        let integrity: String = stock
            .query_row("PRAGMA integrity_check;", [], |r| r.get(0))
            .unwrap();
        assert_eq!(integrity, "ok", "stock integrity_check on the fsqlite image");

        // Corpus intact + matchable (5 rows contain 'rust': 1,2,3,5).
        let matched: Vec<i64> = stock
            .prepare("SELECT rowid FROM t WHERE t MATCH 'rust' ORDER BY rowid")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(matched, vec![1, 2, 3, 5], "stock MATCH over the full corpus");

        // ── The `_docsize.origin` column: read every row's origin. ────────
        let origins: Vec<(i64, Option<i64>)> = stock
            .prepare("SELECT id, origin FROM t_docsize ORDER BY id")
            .unwrap()
            .query_map([], |r| Ok((r.get(0)?, r.get::<_, Option<i64>>(1)?)))
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(origins.len(), 6, "one docsize row per document");

        let origin_of = |id: i64| origins.iter().find(|(rid, _)| *rid == id).unwrap().1;

        // Batch 1 (full re-encode): origin NULL — the documented Stage 1b gap.
        assert_eq!(origin_of(1), None, "batch-1 row 1 origin is NULL (full-encode path)");
        assert_eq!(origin_of(2), None, "batch-1 row 2 origin is NULL (full-encode path)");

        // Batch 2 (incremental): both rows share one non-NULL segment origin.
        let o2 = origin_of(3).expect("batch-2 row 3 origin populated");
        assert_eq!(origin_of(4), Some(o2), "batch-2 rows share one segment origin");

        // Batch 3 (incremental): both rows share a later, larger origin.
        let o3 = origin_of(5).expect("batch-3 row 5 origin populated");
        assert_eq!(origin_of(6), Some(o3), "batch-3 rows share one segment origin");
        assert!(o3 > o2, "later batch has a larger origin (monotonic): {o3} > {o2}");
    });
}
