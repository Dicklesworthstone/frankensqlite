#![recursion_limit = "512"]

//! GH#385 / bd-reality-check-1022-llk6g.7: a file-backed autocommit loop with
//! default settings must reset the WAL once a checkpoint has backfilled it, and
//! checkpoints must copy each WAL frame back about once.
//!
//! Before the fix the routine autocheckpoint was PASSIVE only (never resets),
//! write pressure deferred it to the 4,000-frame urgent threshold, and from
//! there EVERY commit ran a FULL checkpoint that re-walked the whole WAL from
//! frame 0 without resetting it: ~10 ms per autocommit, 353x stock at 10k rows.
//!
//! The oracle is WAL-metrics counting, not wall-clock time: frames copied back
//! by checkpoints versus frames written. The counters are process-global, so
//! this binary holds exactly one test.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

const COMMITS: usize = 9_000;
const URGENT_WAL_FRAMES_DEFAULT: u64 = 4_000;

#[test]
fn autocommit_loop_resets_wal_and_backfills_each_frame_about_once() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("gh385_wal_restart.db");
        let wal_path = dir.path().join("gh385_wal_restart.db-wal");
        let conn = Connection::open(path.to_str().expect("utf8 path"))
            .await
            .expect("open");
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .await
            .expect("create");
        let page_rows = conn.query("PRAGMA page_size").await.expect("page_size");
        let SqliteValue::Integer(page_size) = page_rows[0].values()[0] else {
            panic!("PRAGMA page_size must return an integer");
        };
        let frame_bytes = u64::try_from(page_size).expect("page size") + 24;

        let before = fsqlite_wal::GLOBAL_WAL_METRICS.snapshot();
        let mut max_wal_bytes = 0_u64;
        for i in 0..COMMITS {
            conn.execute(&format!("INSERT INTO t(v) VALUES ('row-{i}')"))
                .await
                .expect("autocommit insert");
            if i % 250 == 249 {
                let wal_bytes = std::fs::metadata(&wal_path).map_or(0, |m| m.len());
                max_wal_bytes = max_wal_bytes.max(wal_bytes);
                let now = fsqlite_wal::GLOBAL_WAL_METRICS.snapshot();
                let written = now.frames_written_total - before.frames_written_total;
                let copied = now.checkpoint_frames_backfilled_total
                    - before.checkpoint_frames_backfilled_total;
                // Fail fast: the old per-commit full re-walk exceeds this within
                // a few dozen commits of the urgent threshold.
                assert!(
                    copied <= written.saturating_mul(4) + 4_096,
                    "after {} commits checkpoints copied {copied} frames for {written} written: \
                     each checkpoint is re-copying the whole WAL",
                    i + 1
                );
            }
        }
        let after = fsqlite_wal::GLOBAL_WAL_METRICS.snapshot();
        let written = after.frames_written_total - before.frames_written_total;
        let copied =
            after.checkpoint_frames_backfilled_total - before.checkpoint_frames_backfilled_total;
        let resets = after.wal_resets_total - before.wal_resets_total;
        let checkpoints = after.checkpoint_count - before.checkpoint_count;
        eprintln!(
            "event=gh385_autocheckpoint_wal_restart commits={COMMITS} frames_written={written} \
             frames_backfilled={copied} checkpoints={checkpoints} wal_resets={resets} \
             max_wal_bytes={max_wal_bytes} frame_bytes={frame_bytes}"
        );
        assert!(
            written >= u64::try_from(COMMITS).expect("commits"),
            "every autocommit appends at least one frame"
        );
        assert!(
            resets >= 1,
            "the WAL was never reset ({checkpoints} checkpoints)"
        );
        assert!(
            copied <= written.saturating_mul(2) + 4_096,
            "checkpoints copied {copied} frames for {written} written"
        );
        // The WAL file keeps its high-water size across a restart, so this
        // bounds the largest WAL the loop ever produced.
        assert!(
            max_wal_bytes <= (URGENT_WAL_FRAMES_DEFAULT + 1_000) * frame_bytes + 32,
            "WAL high-water mark {max_wal_bytes} bytes exceeds the urgent threshold"
        );
        conn.close().await.expect("close");

        let stock = rusqlite::Connection::open(&path).expect("stock open");
        let rows: i64 = stock
            .query_row("SELECT count(*) FROM t", [], |row| row.get(0))
            .expect("stock count");
        assert_eq!(rows, i64::try_from(COMMITS).expect("commits"));
        let integrity: String = stock
            .query_row("PRAGMA integrity_check", [], |row| row.get(0))
            .expect("stock integrity");
        assert_eq!(integrity, "ok");
    });
}
