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

fn stock_i64(stock: &rusqlite::Connection, sql: &str) -> i64 {
    stock
        .query_row(sql, [], |r| r.get(0))
        .unwrap_or_else(|e| panic!("stock query failed: {sql}: {e}"))
}

fn stock_rowids(stock: &rusqlite::Connection, term: &str) -> Vec<i64> {
    stock
        .prepare(&format!(
            "SELECT rowid FROM t WHERE t MATCH '{term}' ORDER BY rowid"
        ))
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap()
}

/// Number of distinct segments referenced by `_data` leaf rows: segment-leaf
/// rowids encode `(segid << 37) | pgno`, and ids 1 (averages) / 10 (structure)
/// sit below the shift.
fn stock_distinct_segids(stock: &rusqlite::Connection) -> i64 {
    stock_i64(
        stock,
        "SELECT count(DISTINCT id >> 37) FROM t_data WHERE id > 10",
    )
}

/// Byte-exact fingerprint of the whole `_data` shadow.
fn stock_data_fingerprint(stock: &rusqlite::Connection) -> String {
    stock
        .query_row(
            "SELECT group_concat(id || ':' || hex(block), '|') FROM (SELECT id, block FROM t_data ORDER BY id)",
            [],
            |r| r.get(0),
        )
        .unwrap()
}

/// bd-aks56: `'optimize'` must do what stock's does (fts5_index.c
/// `sqlite3Fts5IndexOptimize`) — merge every segment into ONE freshly written
/// segment — instead of promoting the table and writing nothing back. The
/// rewritten segment carries `%_idx` seek rows and verifies under stock; a
/// second `'optimize'` on that single clean segment is a byte-exact no-op
/// (stock's `fts5IndexOptimizeStruct` short-circuit); and a later INSERT still
/// appends incrementally instead of blanking the corpus.
#[test]
fn bd_aks56_optimize_merges_every_segment_into_one_stock_shaped_segment() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("fts5_optimize_merge.db");
        let db_str = db_path.to_string_lossy().into_owned();

        // Three single-statement inserts = three appended segments (below the
        // automerge threshold, so nothing collapses them on its own).
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
        {
            let stock = rusqlite::Connection::open(&db_path).unwrap();
            let segments = stock_distinct_segids(&stock);
            assert!(
                segments >= 2,
                "precondition: the appends must leave a multi-segment index, got {segments}"
            );
            assert_eq!(
                stock.query_row::<String, _, _>("PRAGMA integrity_check;", [], |r| r.get(0)).unwrap(),
                "ok"
            );
        }

        // optimize: one segment, seek rows present, stock-verifiable, parity.
        {
            let conn = Connection::open(&db_str).await.unwrap();
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
                vec![1, 2, 3],
                "corpus intact right after optimize"
            );
            conn.close().await.unwrap();
        }
        let fingerprint = {
            let stock = rusqlite::Connection::open(&db_path).unwrap();
            assert_eq!(
                stock_distinct_segids(&stock),
                1,
                "optimize merges every segment into one"
            );
            assert!(
                stock_i64(&stock, "SELECT count(*) FROM t_idx") >= 1,
                "the merged segment carries %_idx seek rows"
            );
            assert_eq!(
                stock.query_row::<String, _, _>("PRAGMA integrity_check;", [], |r| r.get(0)).unwrap(),
                "ok",
                "stock verifies the optimized image"
            );
            assert_eq!(stock_rowids(&stock, "common"), vec![1, 2, 3]);
            assert_eq!(stock_rowids(&stock, "term2"), vec![2]);
            stock_data_fingerprint(&stock)
        };

        // A second optimize on one clean, seekable segment rewrites nothing.
        {
            let conn = Connection::open(&db_str).await.unwrap();
            conn.execute("INSERT INTO t(t) VALUES('optimize');")
                .await
                .unwrap();
            conn.close().await.unwrap();
        }
        {
            let stock = rusqlite::Connection::open(&db_path).unwrap();
            assert_eq!(
                stock_data_fingerprint(&stock),
                fingerprint,
                "optimize on an already-optimized index is a byte-exact no-op"
            );
        }

        // A write after optimize appends incrementally; nothing is blanked.
        {
            let conn = Connection::open(&db_str).await.unwrap();
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
                vec![1, 2, 3, 100]
            );
            conn.close().await.unwrap();
        }
        let stock = rusqlite::Connection::open(&db_path).unwrap();
        assert_eq!(
            stock.query_row::<String, _, _>("PRAGMA integrity_check;", [], |r| r.get(0)).unwrap(),
            "ok"
        );
        assert_eq!(stock_rowids(&stock, "common"), vec![1, 2, 3, 100]);
        assert!(
            stock_distinct_segids(&stock) >= 2,
            "the post-optimize insert appended a segment instead of re-encoding"
        );
    });
}
