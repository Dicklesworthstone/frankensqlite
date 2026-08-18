//! bd-fts5-lazy-shadow-reads-itcc4.3 merge policy — e2e keeper.
//!
//! FrankenSQLite inserts past `automerge` into a lazy `contentless_delete` FTS5
//! table (with a prefix index): the on-disk segments MERGE — the `_data` leaf
//! count drops well below one-per-insert — without losing any row or corrupting
//! the prefix-index doclists, a DELETE after the merge still tombstones, and
//! bundled stock C SQLite reads the merged (and post-delete) image identically.

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
fn bd_fts5_lazy_merge_bounds_segments_and_stays_stock_readable() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("fts5_lazy_merge.db");
        let db_str = db_path.to_string_lossy().into_owned();

        let conn = Connection::open(&db_str).await.unwrap();
        // The prefix index ('2 3') makes the merge carry prefix-index doclists,
        // exercising the raw-empty-key term enumerator.
        conn.execute(
            "CREATE VIRTUAL TABLE t USING fts5(body, content='', contentless_delete=1, prefix='2 3');",
        )
        .await
        .unwrap();

        // Five single-row INSERT statements => five level-0 segments; automerge=4
        // fires after the fourth append and merges the oldest run.
        for id in 1..=5 {
            conn.execute(&format!(
                "INSERT INTO t(rowid, body) VALUES ({id}, 'common rust term{id}');"
            ))
            .await
            .unwrap();
        }

        // Without a merge, `_data` would hold >= 5 segment leaves (+ structure +
        // averages) = >= 7 rows; the merge collapses them below the insert count.
        let count_rows = conn.query("SELECT count(*) FROM t_data;").await.unwrap();
        let data_count = match count_rows[0].values()[0] {
            SqliteValue::Integer(n) => n,
            ref other => panic!("count(*) not an integer: {other:?}"),
        };
        assert!(
            data_count < 5,
            "merge must collapse the segment leaves: {data_count} _data rows for 5 single-row inserts"
        );

        // No row lost by the merge: all five match 'common'.
        let common = conn
            .query("SELECT rowid FROM t WHERE t MATCH 'common' ORDER BY rowid;")
            .await
            .unwrap();
        assert_eq!(rowids(&common), vec![1, 2, 3, 4, 5], "every row survives the merge");

        // DELETE after the merge must still tombstone (validates the merged
        // segment's [min, max] origin span).
        conn.execute("DELETE FROM t WHERE rowid = 3;").await.unwrap();
        let after_delete = conn
            .query("SELECT rowid FROM t WHERE t MATCH 'common' ORDER BY rowid;")
            .await
            .unwrap();
        assert_eq!(
            rowids(&after_delete),
            vec![1, 2, 4, 5],
            "delete-after-merge tombstones the merged row"
        );
        conn.close().await.unwrap();

        // Stock C SQLite reads the merged, post-delete image.
        let stock = rusqlite::Connection::open(&db_path).unwrap();
        let integrity: String = stock
            .query_row("PRAGMA integrity_check;", [], |r| r.get(0))
            .unwrap();
        assert_eq!(integrity, "ok", "stock integrity_check on the merged image");

        let matched: Vec<i64> = stock
            .prepare("SELECT rowid FROM t WHERE t MATCH 'common' ORDER BY rowid")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(matched, vec![1, 2, 4, 5], "stock reads the merged corpus minus the deleted row");

        // The prefix-index doclists survived the merge: a stock prefix MATCH
        // ('ter*' -> 'term1'..'term5') still finds every live row.
        let prefixed: Vec<i64> = stock
            .prepare("SELECT rowid FROM t WHERE t MATCH 'ter*' ORDER BY rowid")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(prefixed, vec![1, 2, 4, 5], "prefix-index doclists survive the merge");
    });
}
