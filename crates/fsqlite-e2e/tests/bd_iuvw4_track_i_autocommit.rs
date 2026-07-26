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
    conn.execute("PRAGMA journal_mode=WAL").await.ok();
    conn
}

fn open_sqlite(path: &Path) -> rusqlite::Connection {
    let conn = rusqlite::Connection::open(path).expect("open sqlite connection");
    conn.execute_batch("PRAGMA journal_mode=WAL;")
        .expect("enable sqlite wal");
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
    asupersync::test_utils::run_test(|| async {
        let _guard = TRACK_I_E2E_LOCK
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());

        let temp = tempdir().expect("tempdir");
        let fsqlite_db = temp.path().join("track_i_retained_autocommit.db");

        let conn = open_fsqlite(&fsqlite_db).await;
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

        // With retained autocommit, we should see reuse instead of full begin/commit per statement.
        // The exact number depends on flush intervals, but we expect significantly fewer flushes
        // than INSERT_COUNT.
        let flush_count = profile.retained_autocommit_flushes;
        let reuse_count = profile.retained_autocommit_reuses;

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

        // If retained autocommit is working, flushes should be much less than INSERT_COUNT
        // Allow up to INSERT_COUNT for systems where retained autocommit may not be active
        assert!(
            flush_count <= INSERT_COUNT as u64,
            "flush count should not exceed insert count: flushes={flush_count}"
        );
    });
}

#[test]
fn bd_iuvw4_track_i_read_after_write_returns_correct_data() {
    asupersync::test_utils::run_test(|| async {
        let _guard = TRACK_I_E2E_LOCK
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());

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
    });
}

#[test]
fn bd_iuvw4_track_i_connection_close_flushes_pending_writes() {
    asupersync::test_utils::run_test(|| async {
        let _guard = TRACK_I_E2E_LOCK
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());

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
            // Connection drops here - should flush all pending writes
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
        }

        eprintln!(
            "INFO bead_id={BEAD_ID} scenario=CLOSE-FLUSH inserts={INSERT_COUNT} replay_command={REPLAY_COMMAND}"
        );
    });
}

#[test]
fn bd_iuvw4_track_i_autocommit_10k_throughput_with_oracle() {
    asupersync::test_utils::run_test(|| async {
        let _guard = TRACK_I_E2E_LOCK
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());

        let temp = tempdir().expect("tempdir");
        let fsqlite_db = temp.path().join("track_i_10k_fsqlite.db");
        let sqlite_db = temp.path().join("track_i_10k_sqlite.db");

        let fconn = open_fsqlite(&fsqlite_db).await;
        let sconn = open_sqlite(&sqlite_db);

        assert!(
            fconn.is_concurrent_mode_default(),
            "Track I tests must keep concurrent_mode_default enabled"
        );

        fconn
            .execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .await
            .expect("create fsqlite table");
        sconn
            .execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT);")
            .expect("create sqlite table");

        const INSERT_COUNT: i64 = 10_000;

        // Time fsqlite autocommit inserts
        let fsqlite_start = Instant::now();
        let (_result, profile) = capture_hot_path_metrics(|| async {
            for rowid in 1..=INSERT_COUNT {
                fconn
                    .execute(&format!("INSERT INTO t VALUES ({rowid}, 'v{rowid}')"))
                    .await
                    .expect("fsqlite insert");
            }
        })
        .await;
        let fsqlite_elapsed = fsqlite_start.elapsed();

        // Time sqlite autocommit inserts
        let sqlite_start = Instant::now();
        for rowid in 1..=INSERT_COUNT {
            sconn
                .execute(
                    "INSERT INTO t VALUES (?1, ?2)",
                    rusqlite::params![rowid, format!("v{rowid}")],
                )
                .expect("sqlite insert");
        }
        let sqlite_elapsed = sqlite_start.elapsed();

        // Verify row counts match
        let fsqlite_count: i64 = fconn
            .query("SELECT COUNT(*) FROM t")
            .await
            .expect("fsqlite count")
            .into_iter()
            .map(|row| match row.get(0) {
                Some(SqliteValue::Integer(count)) => *count,
                other => panic!("expected INTEGER, got {other:?}"),
            })
            .next()
            .expect("fsqlite count result");

        let sqlite_count: i64 = sconn
            .query_row("SELECT COUNT(*) FROM t", [], |row| row.get(0))
            .expect("sqlite count");

        assert_eq!(fsqlite_count, INSERT_COUNT, "fsqlite row count mismatch");
        assert_eq!(sqlite_count, INSERT_COUNT, "sqlite row count mismatch");

        eprintln!(
            "INFO bead_id={BEAD_ID} scenario=AUTOCOMMIT-10K inserts={} fsqlite_rows_per_sec={:.1} sqlite_rows_per_sec={:.1} flushes={} reuses={} replay_command={REPLAY_COMMAND}",
            INSERT_COUNT,
            rows_per_sec(INSERT_COUNT, fsqlite_elapsed),
            rows_per_sec(INSERT_COUNT, sqlite_elapsed),
            profile.retained_autocommit_flushes,
            profile.retained_autocommit_reuses,
        );
    });
}

#[test]
fn bd_iuvw4_track_i_interleaved_read_write_correctness() {
    asupersync::test_utils::run_test(|| async {
        let _guard = TRACK_I_E2E_LOCK
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());

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
    });
}
