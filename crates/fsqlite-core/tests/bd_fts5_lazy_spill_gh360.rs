//! GH#360 / bd-lmw9y end-to-end keeper: FrankenSQLite's lazy on-disk FTS5 reader
//! must read a doclist that stock SQLite split across leaf pages.
//!
//! Stock (`rusqlite`) writes a `contentless_delete` FTS5 corpus at `pgsz=64`
//! where the term `common`'s doclist spills across many leaves (doc 1 carries an
//! 80-position poslist, so the split lands mid-poslist). FrankenSQLite opens the
//! file — a rootpage-zero contentless FTS5 table binds the persisted `_data`
//! segments lazily by default — and `MATCH` must return exactly the rowids stock
//! returns. Before the page-boundary doclist stitcher this failed with
//! `fts5: corrupt %_data record: truncated poslist body`.

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
fn bd_fts5_lazy_spill_gh360_reads_stock_split_doclist() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("fts5_spill_gh360.db");

        // --- Stock writes a spilling contentless corpus. ---
        {
            let stock = rusqlite::Connection::open(&db_path).unwrap();
            stock
                .execute_batch(
                    "CREATE VIRTUAL TABLE t USING fts5(x, content='', contentless_delete=1);\n\
                     INSERT INTO t(t, rank) VALUES('pgsz', 64);",
                )
                .unwrap();
            // 40 docs all containing 'common' plus a unique term.
            for id in 1..=40 {
                stock
                    .execute(
                        "INSERT INTO t(rowid, x) VALUES (?1, ?2)",
                        rusqlite::params![id, format!("common w{id}")],
                    )
                    .unwrap();
            }
            // Doc 1 gets an 80-position 'common' poslist so the split is mid-poslist.
            // (This also removes doc 1's `w1` token.)
            let big = vec!["common"; 80].join(" ");
            stock
                .execute("UPDATE t SET x = ?1 WHERE rowid = 1", rusqlite::params![big])
                .unwrap();
            // Merge into one segment: 'common' becomes one long doclist over
            // consecutive leaf pages.
            stock.execute_batch("INSERT INTO t(t) VALUES('optimize');").unwrap();

            // Guard: the corpus must actually span multiple leaf pages, else this
            // test would not exercise the stitcher.
            let leaf_pages: i64 = stock
                .query_row("SELECT count(*) FROM t_data WHERE id > 100", [], |row| {
                    row.get(0)
                })
                .unwrap();
            assert!(
                leaf_pages > 1,
                "fixture must spill across leaf pages, got {leaf_pages}"
            );
            // Oracle: stock returns rowids 1..=40 for 'common'.
            let stock_common: Vec<i64> = stock
                .prepare("SELECT rowid FROM t WHERE t MATCH 'common' ORDER BY rowid")
                .unwrap()
                .query_map([], |row| row.get(0))
                .unwrap()
                .collect::<std::result::Result<Vec<_>, _>>()
                .unwrap();
            assert_eq!(stock_common, (1..=40).collect::<Vec<i64>>());
        }

        // --- FrankenSQLite lazy-reads the stock-split doclist. ---
        let conn = Connection::open(db_path.to_str().unwrap()).await.unwrap();
        let common = conn
            .query("SELECT rowid FROM t WHERE t MATCH 'common' ORDER BY rowid;")
            .await
            .unwrap();
        assert_eq!(
            rowids(&common),
            (1..=40).collect::<Vec<i64>>(),
            "frank must stitch the page-split 'common' doclist and match stock"
        );

        // A single-doc term (its doclist never spills) is unaffected.
        let w7 = conn
            .query("SELECT rowid FROM t WHERE t MATCH 'w7';")
            .await
            .unwrap();
        assert_eq!(rowids(&w7), vec![7]);

        // A prefix query walks the stitched term stream too. Doc 1's `w1` token
        // was overwritten by the UPDATE (it is now 80 `common`s), so `w1*` matches
        // only w10..w19 -> rowids 10..=19, exactly as stock does.
        let prefixed = conn
            .query("SELECT rowid FROM t WHERE t MATCH 'w1*' ORDER BY rowid;")
            .await
            .unwrap();
        assert_eq!(
            rowids(&prefixed),
            (10..=19).collect::<Vec<i64>>(),
            "prefix over stitched segment matches stock"
        );

        conn.close().await.unwrap();
    });
}
