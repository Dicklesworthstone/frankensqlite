// Keeper for GH#408: a schema reload must PRESERVE an unchanged live fts5 index
// instead of re-deriving the whole corpus on every statement boundary that
// follows an unrelated write.
//
// The preserve decision is CONTENT-ADDRESSED. At each reload the connection
// point-reads the table's `%_data` averages (row 1) and structure (row 10)
// records plus its small `%_config` table, and preserves the live instance only
// when those bytes are identical to the ones it was last built from. FTS5
// rewrites both `%_data` metadata records on every index mutation, so equal
// bytes prove an unchanged posting-list image — no matter who wrote it.
//
// Three guarantees, and they pull in opposite directions so all three matter:
//  1. CORRECTNESS, same process (peer_write_is_observed): a *different*
//     Connection's write to the fts5 table changes the stamp, so the first
//     connection's next MATCH rebuilds and sees the new row.
//  2. CORRECTNESS, foreign writer (foreign_writer_is_observed): a stock C
//     SQLite connection writing the same file is likewise observed. This is the
//     case a bookkeeping-based gate (a process-global "last modified by us"
//     registry) silently gets wrong, because no fsqlite code runs for that
//     write; only reading the persisted bytes catches it.
//  3. PERFORMANCE (untouched_table_is_not_rebuilt, reload_work_is_flat): writes
//     to ANOTHER table interleaved between MATCHes must NOT re-derive the
//     untouched fts5 index, and the per-boundary cost must not grow with the
//     size of that index. Pre-fix, every boundary hydrated the whole persisted
//     index: 16,144 shadow rows at 2k documents and 64,488 at 8k — exactly
//     linear, ~29 ms per statement, which is the reported runaway at 631k rows.
#![cfg(feature = "ext-fts5")]

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;
use std::time::{Duration, Instant};

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

/// GH#408 correctness, foreign writer: a *stock C SQLite* connection commits a
/// row into the fts5 table while the fsqlite connection stays open. Because the
/// preserve gate proves its property by reading the persisted `%_data` records
/// — not by tracking which writes this process performed — the next MATCH sees
/// the foreign row.
///
/// A bookkeeping gate cannot see this write at all: no fsqlite write dispatcher
/// runs for it, so nothing bumps a process-local stamp and the stale in-memory
/// index is preserved forever. That returned `[1]` here instead of `[1, 2]`.
#[test]
fn foreign_writer_is_observed_after_reload_preserve_gh408() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("temp dir");
        let db_path = dir.path().join("gh408_foreign.db");
        let db = db_path.to_string_lossy().into_owned();

        let a = Connection::open(&db).await.expect("open");
        a.execute("CREATE TABLE other(id INTEGER PRIMARY KEY, v TEXT)")
            .await
            .expect("create other");
        a.execute("CREATE VIRTUAL TABLE ft USING fts5(content)")
            .await
            .expect("create fts5");
        a.execute("INSERT INTO ft(rowid, content) VALUES (1, 'alpha common')")
            .await
            .expect("seed");
        // Materialize the live instance and record its content stamp.
        assert_eq!(match_rowids(&a, "common").await, vec![1]);

        // A foreign process-equivalent writer commits into the same file.
        {
            let stock = rusqlite::Connection::open(&db_path).expect("stock open");
            stock
                .execute("INSERT INTO ft(rowid, content) VALUES (2, 'beta common')", [])
                .expect("stock insert");
            let integrity: String = stock
                .query_row("PRAGMA integrity_check;", [], |r| r.get(0))
                .expect("integrity_check");
            assert_eq!(integrity, "ok", "stock image is sound before the re-read");
        }

        // An unrelated local commit moves visibility, which is what drives the
        // reload that used to re-derive (and now must re-validate) the index.
        a.execute("INSERT INTO other(v) VALUES ('unrelated')")
            .await
            .expect("unrelated write");

        assert_eq!(
            match_rowids(&a, "common").await,
            vec![1, 2],
            "GH#408: the preserve gate must observe a foreign writer's committed \
             fts5 row — proving unchanged-ness from persisted bytes, not from \
             which writes this process happened to perform"
        );
        a.close().await.expect("close");
    });
}

/// GH#408 performance: the per-statement-boundary reload cost must not grow
/// with the size of the fts5 index.
///
/// Shaped like the report: a populated fts5 table sits untouched while small
/// writes to another table commit between reads, so every boundary reloads with
/// advanced visibility. Pre-fix each boundary hydrated the entire persisted
/// index, so quadrupling the corpus quadrupled the per-boundary cost. The
/// keeper measures a 4x-larger corpus and requires the cost NOT to scale with
/// it; a full re-derivation shows up as a multiple.
///
/// `external content` is the shape that keeps this honest: contentless and
/// content-backed tables can also fall back to the O(1) lazy segment bind, but
/// an external-content index has no lazy read path, so preservation is the only
/// thing standing between this workload and O(index) per statement.
#[test]
fn reload_work_is_flat_in_fts5_table_size_gh408() {
    asupersync::test_utils::run_test(|| async {
        /// Build an external-content fts5 index of `rows` documents, then time
        /// statement boundaries that follow an unrelated committed write.
        async fn boundary_cost(dir: &std::path::Path, rows: i64, tag: &str) -> (Duration, u64) {
            let db_path = dir.join(format!("gh408_scale_{tag}.db"));
            let db = db_path.to_string_lossy().into_owned();
            let conn = Connection::open(&db).await.expect("open");
            conn.execute("CREATE TABLE other(id INTEGER PRIMARY KEY, v TEXT)")
                .await
                .unwrap();
            conn.execute("CREATE TABLE src(id INTEGER PRIMARY KEY, body TEXT)")
                .await
                .unwrap();
            conn.execute(
                "CREATE VIRTUAL TABLE ft USING fts5(body, content='src', content_rowid='id')",
            )
            .await
            .unwrap();

            conn.execute("BEGIN;").await.unwrap();
            let mut id = 0_i64;
            while id < rows {
                let mut src_sql = String::from("INSERT INTO src(id, body) VALUES ");
                let mut fts_sql = String::from("INSERT INTO ft(rowid, body) VALUES ");
                for k in 0..100 {
                    id += 1;
                    if k > 0 {
                        src_sql.push(',');
                        fts_sql.push(',');
                    }
                    src_sql.push_str(&format!("({id}, 'common corpus token{id}')"));
                    fts_sql.push_str(&format!("({id}, 'common corpus token{id}')"));
                }
                conn.execute(&format!("{src_sql};")).await.unwrap();
                conn.execute(&format!("{fts_sql};")).await.unwrap();
            }
            conn.execute("COMMIT;").await.unwrap();

            // Warm up so first-touch materialization is not in the measurement.
            conn.execute("INSERT INTO other(v) VALUES ('warm')")
                .await
                .unwrap();
            assert_eq!(match_rowids(&conn, "token7").await, vec![7]);

            let rebuilds_before = conn.fts5_reload_rebuild_count();
            const BOUNDARIES: u32 = 8;
            let started = Instant::now();
            for i in 0..BOUNDARIES {
                conn.execute(&format!("INSERT INTO other(v) VALUES ('row{i}')"))
                    .await
                    .unwrap();
                assert_eq!(match_rowids(&conn, "token7").await, vec![7]);
            }
            let per_boundary = started.elapsed() / BOUNDARIES;
            let rebuilds = conn.fts5_reload_rebuild_count() - rebuilds_before;
            conn.close().await.expect("close");

            // The engine's own image must still be readable by stock SQLite.
            let stock = rusqlite::Connection::open(&db_path).expect("stock open");
            let integrity: String = stock
                .query_row("PRAGMA integrity_check;", [], |r| r.get(0))
                .expect("integrity_check");
            assert_eq!(integrity, "ok", "stock integrity_check after {rows} rows");
            let stock_hit: i64 = stock
                .query_row("SELECT rowid FROM ft WHERE ft MATCH 'token7'", [], |r| {
                    r.get(0)
                })
                .expect("stock MATCH");
            assert_eq!(stock_hit, 7, "stock reads the same index fsqlite preserved");

            (per_boundary, rebuilds)
        }

        let dir = tempfile::tempdir().expect("temp dir");
        let (small, small_rebuilds) = boundary_cost(dir.path(), 2_000, "small").await;
        let (large, large_rebuilds) = boundary_cost(dir.path(), 8_000, "large").await;

        eprintln!(
            "GH#408 boundary cost: 2k docs = {small:?} ({small_rebuilds} rebuilds), \
             8k docs = {large:?} ({large_rebuilds} rebuilds)"
        );

        // The sharp assertion: an untouched index is never re-derived, at any size.
        assert_eq!(
            (small_rebuilds, large_rebuilds),
            (0, 0),
            "GH#408: a statement boundary must not re-derive an unchanged fts5 index \
             (pre-fix: one full hydration per boundary, 16144 shadow rows at 2k docs \
             and 64488 at 8k)"
        );

        // And the property that assertion exists to protect, measured end to end.
        // Pre-fix this ratio tracked the 4x corpus growth (8.1 ms -> 29.0 ms);
        // 2.5 leaves generous headroom for timer noise and for the MATCH itself,
        // whose own cost does grow a little with the corpus.
        let ratio = large.as_secs_f64() / small.as_secs_f64().max(1e-9);
        assert!(
            ratio < 2.5,
            "GH#408: per-boundary reload cost is growing with the fts5 table size — \
             2k docs = {small:?}, 8k docs = {large:?}, ratio = {ratio:.2}"
        );
    });
}
