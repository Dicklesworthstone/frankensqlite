//! GH#358 lazy-rebind readiness: a broad cross-engine conformance check for the
//! lazy on-disk FTS5 read path.
//!
//! Stock (`rusqlite`) writes a diverse `contentless_delete` corpus at `pgsz=64`
//! (so common terms' doclists spill across leaf pages), deletes a few docs
//! (tombstones), and optimizes. FrankenSQLite opens the file — a rootpage-zero
//! contentless table binds the persisted segments lazily — and every query type
//! (exact, prefix, shared-term, AND, OR) must return exactly the rowids stock
//! returns. This is the broad real-corpus evidence #358 asks for before enabling
//! lazy rebind by default; it exercises the GH#360 page-boundary stitcher and the
//! tombstone/recency merge across many terms at once.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn frank_rowids(rows: &[fsqlite_core::connection::Row]) -> Vec<i64> {
    rows.iter()
        .map(|row| match row.values()[0] {
            SqliteValue::Integer(n) => n,
            ref other => panic!("expected rowid integer, got {other:?}"),
        })
        .collect()
}

fn stock_rowids(stock: &rusqlite::Connection, query: &str) -> Vec<i64> {
    stock
        .prepare("SELECT rowid FROM t WHERE t MATCH ? ORDER BY rowid")
        .unwrap()
        .query_map(rusqlite::params![query], |r| r.get(0))
        .unwrap()
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap()
}

#[test]
fn bd_fts5_lazy_conformance_matches_stock_across_query_types() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("fts5_conformance.db");

        // --- Stock writes a diverse corpus and deletes a few docs. ---
        {
            let stock = rusqlite::Connection::open(&db_path).unwrap();
            stock
                .execute_batch(
                    "CREATE VIRTUAL TABLE t USING fts5(x, content='', contentless_delete=1);\n\
                     INSERT INTO t(t, rank) VALUES('pgsz', 64);",
                )
                .unwrap();
            // 50 docs: 'common' in all (spills), unique 'alphaN' (prefix), a shared
            // 'bodyM' bucket (M = id % 5), plus doc 1 with an 80-position 'common'
            // poslist so the split lands mid-poslist.
            for id in 1..=50 {
                let text = format!("common alpha{id} body{}", id % 5);
                stock
                    .execute(
                        "INSERT INTO t(rowid, x) VALUES (?1, ?2)",
                        rusqlite::params![id, text],
                    )
                    .unwrap();
            }
            let big = format!("{} alpha1 body1", vec!["common"; 80].join(" "));
            stock
                .execute(
                    "UPDATE t SET x = ?1 WHERE rowid = 1",
                    rusqlite::params![big],
                )
                .unwrap();
            // Delete a few docs -> tombstones the lazy reader must honor.
            for id in [10_i64, 20, 33] {
                stock
                    .execute("DELETE FROM t WHERE rowid = ?1", rusqlite::params![id])
                    .unwrap();
            }
            stock
                .execute_batch("INSERT INTO t(t) VALUES('optimize');")
                .unwrap();
            let leaf_pages: i64 = stock
                .query_row("SELECT count(*) FROM t_data WHERE id > 100", [], |r| {
                    r.get(0)
                })
                .unwrap();
            assert!(
                leaf_pages > 1,
                "corpus must spill across leaves, got {leaf_pages}"
            );
        }

        // --- FrankenSQLite lazy-reads and must match stock on every query. ---
        let conn = Connection::open(db_path.to_str().unwrap()).await.unwrap();
        let stock = rusqlite::Connection::open(&db_path).unwrap();

        let queries = [
            "common",  // spilled doclist across many leaves
            "alpha5",  // exact single-doc term
            "alpha10", // exact term of a DELETED doc (must be empty)
            "alpha1*", // prefix: alpha1, alpha10..alpha19 (minus deleted)
            "body0",   // shared bucket term
            "body1",
            "common AND body2",
            "body0 OR body1",
        ];
        for q in queries {
            let frank = conn
                .query(&format!(
                    "SELECT rowid FROM t WHERE t MATCH '{q}' ORDER BY rowid;"
                ))
                .await
                .unwrap_or_else(|e| panic!("frank query {q:?} failed: {e}"));
            let expected = stock_rowids(&stock, q);
            assert_eq!(
                frank_rowids(&frank),
                expected,
                "lazy read diverged from stock for MATCH {q:?}"
            );
        }
        conn.close().await.unwrap();
    });
}
