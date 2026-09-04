// Keeper for GH#408: a schema reload must PRESERVE an unchanged live fts5 index
// instead of re-tokenizing the whole corpus (`rebuild_documents`) on every
// statement boundary that follows an unrelated write. The fix stamps a shared
// per-file `(path, table) -> last-modified CommitSeq` registry from every
// live-vtab write dispatcher and preserves an instance whose stamp is <= the
// instance's built_at.
//
// Two guarantees, and they pull in opposite directions so both matter:
//  1. CORRECTNESS (peer_write_is_observed): a *different* Connection's write to
//     the fts5 table bumps the stamp, so the first connection's next MATCH
//     rebuilds and sees the new row. An unsound gate would preserve a stale
//     index and silently miss the peer's row.
//  2. PERFORMANCE (untouched_table_is_not_rebuilt): interleaving writes to
//     ANOTHER table between MATCHes must NOT rebuild the untouched fts5 index —
//     the per-connection reload-rebuild counter stays flat. Pre-fix this grew by
//     one per interleaved write.
#![cfg(feature = "ext-fts5")]

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn rowid(v: &SqliteValue) -> i64 {
    match v {
        SqliteValue::Integer(n) => *n,
        other => panic!("expected INTEGER rowid, got {other:?}"),
    }
}

async fn match_rowids(conn: &Connection, term: &str) -> Vec<i64> {
    let sql = format!("SELECT rowid FROM ft WHERE ft MATCH '{term}' ORDER BY rowid");
    let rows = conn.query_with_params(&sql, &[]).await.expect("MATCH query");
    rows.iter().map(|r| rowid(&r.values()[0])).collect()
}

#[test]
fn peer_write_is_observed_after_reload_preserve_gh408() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("temp dir");
        let db = dir.path().join("gh408_peer.db").to_string_lossy().into_owned();

        let a = Connection::open(&db).await.expect("open A");
        a.execute("CREATE VIRTUAL TABLE ft USING fts5(content)")
            .await
            .expect("create fts5");
        a.execute("INSERT INTO ft(rowid, content) VALUES (1, 'alpha beta'), (2, 'beta gamma')")
            .await
            .expect("seed rows");
        // Build + cache A's live in-memory index.
        assert_eq!(match_rowids(&a, "gamma").await, vec![2]);
        assert_eq!(match_rowids(&a, "delta").await, Vec::<i64>::new());

        // A SEPARATE connection to the same file commits a new row.
        let b = Connection::open(&db).await.expect("open B");
        b.execute("INSERT INTO ft(rowid, content) VALUES (3, 'gamma delta')")
            .await
            .expect("peer insert");

        // A's next MATCH must observe B's row (stamp bumped -> A rebuilt). An
        // unsound preserve gate would return the stale [] / [2] here.
        assert_eq!(
            match_rowids(&a, "delta").await,
            vec![3],
            "connection A must observe peer connection B's committed fts5 row"
        );
        assert_eq!(match_rowids(&a, "gamma").await, vec![2, 3]);
    });
}

#[test]
fn untouched_fts5_table_is_not_rebuilt_across_unrelated_writes_gh408() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("temp dir");
        let db = dir.path().join("gh408_flat.db").to_string_lossy().into_owned();

        let c = Connection::open(&db).await.expect("open");
        c.execute("CREATE VIRTUAL TABLE ft USING fts5(content)")
            .await
            .expect("create fts5");
        c.execute("INSERT INTO ft(rowid, content) VALUES (1, 'alpha'), (2, 'beta')")
            .await
            .expect("seed fts5");
        c.execute("CREATE TABLE other(id INTEGER PRIMARY KEY, v TEXT)")
            .await
            .expect("create other");

        // Warm up: force any initial reload rebuild + set the fts5 built_at, so
        // the baseline below is stable regardless of first-touch materialization.
        c.execute("INSERT INTO other(v) VALUES ('warm')").await.unwrap();
        let _ = match_rowids(&c, "alpha").await;
        c.execute("INSERT INTO other(v) VALUES ('warm2')").await.unwrap();
        let _ = match_rowids(&c, "alpha").await;

        let baseline = c.fts5_reload_rebuild_count();

        // Interleave writes to `other` with MATCHes on the untouched fts5 table.
        for i in 0..8 {
            c.execute(&format!("INSERT INTO other(v) VALUES ('row{i}')"))
                .await
                .unwrap();
            assert_eq!(match_rowids(&c, "alpha").await, vec![1]);
        }
        assert_eq!(
            c.fts5_reload_rebuild_count(),
            baseline,
            "an fts5 table untouched by interleaved writes must not be re-tokenized on reload \
             (pre-fix this grew by one per interleaved write)"
        );

        // Control: a PEER connection's write to the fts5 table must force THIS
        // connection to rebuild on its next read (and observe the new row). Note
        // an OWN write does NOT rebuild -- the receipt preserves the in-memory
        // instance that already carries the write -- so the rebuild counter only
        // moves when a foreign change requires re-reading from disk.
        let b = Connection::open(&db).await.expect("open peer B");
        b.execute("INSERT INTO ft(rowid, content) VALUES (3, 'alpha gamma')")
            .await
            .unwrap();
        assert_eq!(
            match_rowids(&c, "alpha").await,
            vec![1, 3],
            "connection c must observe peer B's committed fts5 row"
        );
        assert!(
            c.fts5_reload_rebuild_count() > baseline,
            "a peer write to the fts5 table must force this connection to rebuild it"
        );
    });
}
