//! Track I autocommit retained-txn test coverage for `bd-iuvw4`.
//!
//! Tests verify that retained autocommit transactions:
//! - Reduce begin/commit overhead for consecutive autocommit writes
//! - Maintain read-after-write correctness
//! - Flush properly on connection close
//! - Work correctly with interleaved read/write patterns
#![recursion_limit = "512"]

use std::{
    path::Path,
    sync::Mutex,
    time::{Duration, Instant},
};

use fsqlite_core::connection::{
    HotPathProfileSnapshot, hot_path_profile_snapshot, reset_hot_path_profile,
    set_hot_path_profile_enabled,
};
use fsqlite_types::SqliteValue;
use tempfile::tempdir;

const BEAD_ID: &str = "bd-iuvw4";
const REPLAY_COMMAND: &str =
    "cargo test -p fsqlite-e2e --test bd_iuvw4_track_i_autocommit -- --nocapture --test-threads=1";

static TRACK_I_E2E_LOCK: Mutex<()> = Mutex::new(());

async fn capture_hot_path_metrics<T, Fut: std::future::Future<Output = T>>(
    f: impl FnOnce() -> Fut,
) -> (T, HotPathProfileSnapshot) {
    set_hot_path_profile_enabled(true);
    reset_hot_path_profile();
    let result = f().await;
    let snapshot = hot_path_profile_snapshot();
    reset_hot_path_profile();
    set_hot_path_profile_enabled(false);
    (result, snapshot)
}

async fn open_fsqlite(path: &Path) -> fsqlite::Connection {
    let path = path.to_str().expect("utf-8 db path");
    let conn = fsqlite::Connection::open(path)
        .await
        .expect("open fsqlite connection");
    conn.execute("PRAGMA journal_mode=WAL")
        .await
        .expect("enable FrankenSQLite WAL");
    conn
}

fn rows_per_sec(rows: i64, elapsed: Duration) -> f64 {
    let secs = elapsed.as_secs_f64();
    if secs == 0.0 {
        return rows as f64;
    }
    rows as f64 / secs
}

#[test]
fn bd_iuvw4_track_i_retained_autocommit_reduces_flush_overhead() {
    let _guard = TRACK_I_E2E_LOCK
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());

    asupersync::test_utils::run_test(|| async {
        // Retention is intentionally memory-only: file writes must be durable
        // before acknowledgement. A file-backed zero-reuse run is no evidence
        // that retained transactions reduce begin/commit work.
        let conn = fsqlite::Connection::open(":memory:")
            .await
            .expect("open memory connection");
        assert!(
            conn.is_concurrent_mode_default(),
            "Track I tests must keep concurrent_mode_default enabled"
        );

        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .await
            .expect("create table");

        const INSERT_COUNT: i64 = 100;

        let (_result, profile) = capture_hot_path_metrics(|| async {
            for rowid in 1..=INSERT_COUNT {
                conn.execute(&format!("INSERT INTO t VALUES ({rowid}, 'v{rowid}')"))
                    .await
                    .expect("autocommit insert");
            }
        })
        .await;

        let flush_count = profile.retained_autocommit_flushes;
        let reuse_count = profile.retained_autocommit_reuses;
        assert!(
            reuse_count >= 90,
            "retention must actually run: {profile:?}"
        );
        assert_eq!(
            profile.retained_autocommit_parks + profile.pager_commit.commit_calls,
            INSERT_COUNT as u64,
            "each write must park or finish a retained batch"
        );
        assert!(profile.pager_commit.commit_calls <= 10, "{profile:?}");

        // Verify correctness: all rows should be queryable
        let rows = conn
            .query("SELECT COUNT(*) FROM t")
            .await
            .expect("count query")
            .into_iter()
            .map(|row| match row.get(0) {
                Some(SqliteValue::Integer(count)) => *count,
                other => panic!("expected INTEGER count, got {other:?}"),
            })
            .next()
            .expect("count result");
        assert_eq!(rows, INSERT_COUNT, "all rows should be persisted");

        eprintln!(
            "INFO bead_id={BEAD_ID} scenario=RETAINED-AUTOCOMMIT-100 inserts={} flushes={} reuses={} parks={} replay_command={REPLAY_COMMAND}",
            INSERT_COUNT, flush_count, reuse_count, profile.retained_autocommit_parks,
        );

        conn.close().await.expect("close memory connection");
    });
}

#[test]
fn bd_iuvw4_track_i_read_after_write_returns_correct_data() {
    let _guard = TRACK_I_E2E_LOCK
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());

    asupersync::test_utils::run_test(|| async {
        let temp = tempdir().expect("tempdir");
        let fsqlite_db = temp.path().join("track_i_read_after_write.db");

        let conn = open_fsqlite(&fsqlite_db).await;
        assert!(
            conn.is_concurrent_mode_default(),
            "Track I tests must keep concurrent_mode_default enabled"
        );

        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT, score INTEGER)")
            .await
            .expect("create table");

        // Insert then immediately read - this MUST return the just-inserted data
        for rowid in 1..=10_i64 {
            let val = format!("value-{rowid}");
            let score = rowid * 100;

            conn.execute(&format!("INSERT INTO t VALUES ({rowid}, '{val}', {score})"))
                .await
                .expect("insert");

            let rows: Vec<(i64, String, i64)> = conn
                .query(&format!("SELECT id, val, score FROM t WHERE id = {rowid}"))
                .await
                .expect("select")
                .into_iter()
                .map(|row| {
                    let id = match row.get(0) {
                        Some(SqliteValue::Integer(v)) => *v,
                        other => panic!("expected INTEGER id, got {other:?}"),
                    };
                    let val = match row.get(1) {
                        Some(SqliteValue::Text(v)) => v.to_string(),
                        other => panic!("expected TEXT val, got {other:?}"),
                    };
                    let score = match row.get(2) {
                        Some(SqliteValue::Integer(v)) => *v,
                        other => panic!("expected INTEGER score, got {other:?}"),
                    };
                    (id, val, score)
                })
                .collect();

            assert_eq!(
                rows.len(),
                1,
                "read-after-write must return exactly one row for rowid {rowid}"
            );
            assert_eq!(
                rows[0],
                (rowid, val, score),
                "read-after-write must return correct data for rowid {rowid}"
            );
        }

        eprintln!(
            "INFO bead_id={BEAD_ID} scenario=READ-AFTER-WRITE test_rows=10 replay_command={REPLAY_COMMAND}"
        );
        conn.close()
            .await
            .expect("close read-after-write connection");
    });
}

#[test]
fn bd_iuvw4_track_i_connection_close_flushes_pending_writes() {
    let _guard = TRACK_I_E2E_LOCK
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());

    asupersync::test_utils::run_test(|| async {
        let temp = tempdir().expect("tempdir");
        let fsqlite_db = temp.path().join("track_i_close_flush.db");

        const INSERT_COUNT: i64 = 50;

        // First connection: insert rows and close
        {
            let conn = open_fsqlite(&fsqlite_db).await;
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
                .await
                .expect("create table");

            for rowid in 1..=INSERT_COUNT {
                conn.execute(&format!("INSERT INTO t VALUES ({rowid}, 'v{rowid}')"))
                    .await
                    .expect("insert");
            }
            conn.close().await.expect("await close before reopening");
        }

        // Second connection: verify all rows are visible
        {
            let conn = open_fsqlite(&fsqlite_db).await;
            let count: i64 = conn
                .query("SELECT COUNT(*) FROM t")
                .await
                .expect("count query")
                .into_iter()
                .map(|row| match row.get(0) {
                    Some(SqliteValue::Integer(count)) => *count,
                    other => panic!("expected INTEGER count, got {other:?}"),
                })
                .next()
                .expect("count result");

            assert_eq!(
                count, INSERT_COUNT,
                "all rows must be visible after close+reopen"
            );
            conn.close().await.expect("close reopened connection");
        }

        eprintln!(
            "INFO bead_id={BEAD_ID} scenario=CLOSE-FLUSH inserts={INSERT_COUNT} replay_command={REPLAY_COMMAND}"
        );
    });
}

#[test]
fn bd_iuvw4_track_i_autocommit_10k_throughput_with_oracle() {
    let _guard = TRACK_I_E2E_LOCK
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());

    asupersync::test_utils::run_test(|| async {
        const INSERT_COUNT: i64 = 10_000;
        let statements: Vec<_> = (1..=INSERT_COUNT)
            .map(|rowid| format!("INSERT INTO t VALUES ({rowid}, 'v{rowid}')"))
            .collect();
        let stock = rusqlite::Connection::open_in_memory().expect("open stock oracle");
        stock
            .execute_batch(
                "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT); CREATE INDEX t_val ON t(val);",
            )
            .expect("stock schema");
        for sql in &statements {
            stock.execute(sql, []).expect("stock insert");
        }
        let expected: Vec<(i64, String)> = stock
            .prepare("SELECT id, val FROM t ORDER BY id")
            .expect("stock prepare")
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .expect("stock query")
            .collect::<rusqlite::Result<_>>()
            .expect("stock rows");

        // One warmup pair, then four pairs with alternating execution order.
        // This is a same-binary retention ablation, not a comparison between
        // engines or a release-performance gate. Emit every raw timing; never
        // fail correctness on a shared host's wall-clock variance.
        for round in 0..5 {
            let order = if round % 2 == 0 {
                [false, true]
            } else {
                [true, false]
            };
            for retain in order {
                let conn = fsqlite::Connection::open(":memory:")
                    .await
                    .expect("open memory sample");
                assert!(conn.is_concurrent_mode_default());
                conn.execute(&format!(
                    "PRAGMA fsqlite.autocommit_retain = {}",
                    u8::from(retain)
                ))
                .await
                .expect("configure retained transactions");
                conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)")
                    .await
                    .expect("sample table");
                conn.execute("CREATE INDEX t_val ON t(val)")
                    .await
                    .expect("sample index");
                let ((elapsed, before_flush), after_flush) = capture_hot_path_metrics(|| async {
                    let start = Instant::now();
                    for sql in &statements {
                        conn.execute(sql).await.expect("sample insert");
                    }
                    let before_flush = hot_path_profile_snapshot();
                    // COUNT can read the retained overlay without flushing.
                    // This public boundary flushes the final batch before the
                    // timer stops; both arms execute the identical command.
                    conn.execute("PRAGMA fsqlite.autocommit_retain = OFF")
                        .await
                        .expect("flush final batch");
                    let elapsed = start.elapsed();
                    let count = conn.query("SELECT COUNT(*) FROM t").await.expect("count");
                    assert_eq!(count.len(), 1);
                    assert_eq!(count[0].values(), &[SqliteValue::Integer(INSERT_COUNT)]);
                    (elapsed, before_flush)
                })
                .await;
                if retain {
                    assert!(before_flush.retained_autocommit_reuses >= 9_000);
                    assert_eq!(
                        before_flush.retained_autocommit_parks
                            + before_flush.pager_commit.commit_calls,
                        INSERT_COUNT as u64
                    );
                    assert!((1..=1_000).contains(&before_flush.pager_commit.commit_calls));
                    assert!(
                        after_flush.retained_autocommit_flushes
                            > before_flush.retained_autocommit_flushes
                    );
                } else {
                    assert_eq!(before_flush.retained_autocommit_reuses, 0);
                    assert_eq!(before_flush.retained_autocommit_parks, 0);
                    // The immediate-commit path may retain a *committed*
                    // writer handle. Its successful commits are counted at
                    // cached_write_txn_parks, not the normal pager commit arm.
                    assert_eq!(
                        before_flush.pager_commit.commit_calls
                            + before_flush.cached_write_txn_parks,
                        INSERT_COUNT as u64
                    );
                }
                assert_eq!(after_flush.single_writer_filebacked_commits, 0);
                let actual = conn
                    .query("SELECT id, val FROM t ORDER BY id")
                    .await
                    .expect("sample ordered rows");
                assert_eq!(actual.len(), expected.len());
                for (row, (id, val)) in actual.iter().zip(&expected) {
                    assert_eq!(
                        row.values(),
                        &[
                            SqliteValue::Integer(*id),
                            SqliteValue::Text(val.clone().into())
                        ]
                    );
                }
                let indexed = conn
                    .query("SELECT id, val FROM t WHERE val = 'v5000'")
                    .await
                    .expect("sample index lookup");
                assert_eq!(indexed.len(), 1);
                assert_eq!(indexed[0].values(), actual[4_999].values());
                assert_eq!(
                    conn.query("PRAGMA integrity_check")
                        .await
                        .expect("integrity")[0]
                        .values(),
                    &[SqliteValue::Text("ok".into())]
                );
                conn.close().await.expect("close sample");
                eprintln!(
                    "INFO bead_id={BEAD_ID} event=autocommit_10k_sample round={round} warmup={} retain={retain} concurrent_default=true inserts={INSERT_COUNT} elapsed_ns={} rows_per_sec={:.1} reuses={} parks={} normal_commits={} committed_writer_parks={} final_flushes={} oracle=verified sqlite={} replay_command={REPLAY_COMMAND}",
                    round == 0,
                    elapsed.as_nanos(),
                    rows_per_sec(INSERT_COUNT, elapsed),
                    before_flush.retained_autocommit_reuses,
                    before_flush.retained_autocommit_parks,
                    before_flush.pager_commit.commit_calls,
                    before_flush.cached_write_txn_parks,
                    after_flush.retained_autocommit_flushes
                        - before_flush.retained_autocommit_flushes,
                    rusqlite::version(),
                );
            }
        }
    });
}

#[test]
fn bd_iuvw4_track_i_interleaved_read_write_correctness() {
    let _guard = TRACK_I_E2E_LOCK
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());

    asupersync::test_utils::run_test(|| async {
        let temp = tempdir().expect("tempdir");
        let fsqlite_db = temp.path().join("track_i_interleaved.db");

        let conn = open_fsqlite(&fsqlite_db).await;
        assert!(
            conn.is_concurrent_mode_default(),
            "Track I tests must keep concurrent_mode_default enabled"
        );

        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .await
            .expect("create table");

        const CYCLE_COUNT: i64 = 100;

        let (_result, profile) = capture_hot_path_metrics(|| async {
            for cycle in 1..=CYCLE_COUNT {
                // Write
                conn.execute(&format!("INSERT INTO t VALUES ({cycle}, 'v{cycle}')"))
                    .await
                    .expect("insert");

                // Immediate read
                let count: i64 = conn
                    .query("SELECT COUNT(*) FROM t")
                    .await
                    .expect("count")
                    .into_iter()
                    .map(|row| match row.get(0) {
                        Some(SqliteValue::Integer(count)) => *count,
                        other => panic!("expected INTEGER, got {other:?}"),
                    })
                    .next()
                    .expect("count result");

                assert_eq!(
                    count, cycle,
                    "interleaved count must match after cycle {cycle}"
                );
            }
        })
        .await;

        // Final verification
        let final_count: i64 = conn
            .query("SELECT COUNT(*) FROM t")
            .await
            .expect("final count")
            .into_iter()
            .map(|row| match row.get(0) {
                Some(SqliteValue::Integer(count)) => *count,
                other => panic!("expected INTEGER, got {other:?}"),
            })
            .next()
            .expect("final count result");

        assert_eq!(final_count, CYCLE_COUNT, "final count mismatch");

        eprintln!(
            "INFO bead_id={BEAD_ID} scenario=INTERLEAVED cycles={} flushes={} read_after_write_flushes={} overlay_hits={} overlay_misses={} replay_command={REPLAY_COMMAND}",
            CYCLE_COUNT,
            profile.retained_autocommit_flushes,
            profile.retained_autocommit_read_after_write_flushes,
            profile.retained_autocommit_overlay_hits,
            profile.retained_autocommit_overlay_misses,
        );
        conn.close().await.expect("close interleaved connection");
    });
}
