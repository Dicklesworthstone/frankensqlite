//! Track I autocommit durability coverage for `bd-iuvw4`.
//!
//! Tests verify that successful autocommit statements:
//! - Commit before returning success
//! - Maintain read-after-write correctness
//! - Remain durable across connection close and reopen
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

async fn fsqlite_count(conn: &fsqlite::Connection, sql: &str) -> i64 {
    let rows = conn.query(sql).await.expect("query fsqlite row count");
    match rows.as_slice() {
        [row] => match row.get(0) {
            Some(SqliteValue::Integer(count)) => *count,
            other => panic!("expected one INTEGER count, got {other:?}"),
        },
        other => panic!("expected one count row, got {other:?}"),
    }
}

#[test]
fn bd_iuvw4_track_i_each_success_is_immediately_visible() {
    asupersync::test_utils::run_test(|| async {
        let _guard = TRACK_I_E2E_LOCK
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());

        let temp = tempdir().expect("tempdir");
        let fsqlite_db = temp.path().join("track_i_immediate_visibility.db");

        let conn = open_fsqlite(&fsqlite_db).await;
        assert!(
            conn.is_concurrent_mode_default(),
            "Track I tests must keep concurrent_mode_default enabled"
        );
        conn.execute("PRAGMA fsqlite.concurrent_mode = OFF")
            .await
            .expect("select serialized pager path");

        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .await
            .expect("create table");
        let fsqlite_journal_mode = conn
            .query("PRAGMA journal_mode")
            .await
            .expect("read fsqlite journal mode");
        assert_eq!(
            fsqlite_journal_mode[0].get(0),
            Some(&SqliteValue::Text("wal".into())),
            "FrankenSQLite writer must report WAL mode"
        );
        let live_c_observer = open_sqlite(&fsqlite_db);
        let c_journal_mode: String = live_c_observer
            .query_row("PRAGMA journal_mode", [], |row| row.get(0))
            .expect("read C SQLite journal mode");
        assert_eq!(
            c_journal_mode.to_ascii_lowercase(),
            "wal",
            "C SQLite observer must report WAL mode"
        );

        const INSERT_COUNT: i64 = 100;
        let mut fsqlite_observer = None;

        for rowid in 1..=INSERT_COUNT {
            conn.execute(&format!("INSERT INTO t VALUES ({rowid}, 'v{rowid}')"))
                .await
                .expect("autocommit insert");

            if fsqlite_observer.is_none() {
                fsqlite_observer = Some(open_fsqlite(&fsqlite_db).await);
            }
            let visible_count =
                fsqlite_count(fsqlite_observer.as_ref().unwrap(), "SELECT COUNT(*) FROM t").await;
            assert_eq!(
                visible_count, rowid,
                "successful autocommit INSERT must be visible to another FrankenSQLite connection before returning"
            );

            if rowid == 1 {
                let live_c_count = live_c_observer
                    .query_row("SELECT COUNT(*) FROM t", [], |row| row.get::<_, i64>(0));
                eprintln!(
                    "INFO bead_id={BEAD_ID} scenario=AUTOCOMMIT-VISIBILITY-BOUNDARY boundary=live_c_reader fsqlite_journal_mode=wal c_journal_mode={c_journal_mode} result={live_c_count:?}"
                );

                fsqlite_observer
                    .take()
                    .expect("fresh FrankenSQLite observer")
                    .close()
                    .await
                    .expect("close fresh FrankenSQLite observer before checkpoint");
                conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                    .await
                    .expect("checkpoint committed row");
                let post_checkpoint_count: i64 = live_c_observer
                    .query_row("SELECT COUNT(*) FROM t", [], |row| row.get(0))
                    .expect("C SQLite count after FrankenSQLite checkpoint");
                eprintln!(
                    "INFO bead_id={BEAD_ID} scenario=AUTOCOMMIT-VISIBILITY-BOUNDARY boundary=post_checkpoint_c_reader count={post_checkpoint_count}"
                );
                assert_eq!(
                    post_checkpoint_count, 1,
                    "C SQLite must see the committed row after an explicit FrankenSQLite checkpoint"
                );
            }
        }

        fsqlite_observer
            .take()
            .expect("FrankenSQLite observer")
            .close()
            .await
            .expect("close FrankenSQLite observer");
        drop(live_c_observer);
        conn.close().await.expect("close FrankenSQLite writer");

        let post_close_c_observer = open_sqlite(&fsqlite_db);
        let post_close_count: i64 = post_close_c_observer
            .query_row("SELECT COUNT(*) FROM t", [], |row| row.get(0))
            .expect("C SQLite count after FrankenSQLite close");
        eprintln!(
            "INFO bead_id={BEAD_ID} scenario=AUTOCOMMIT-VISIBILITY-BOUNDARY boundary=post_close_c_reader count={post_close_count}"
        );
        assert_eq!(
            post_close_count, INSERT_COUNT,
            "C SQLite must see every committed row after the FrankenSQLite writer closes"
        );

        eprintln!(
            "INFO bead_id={BEAD_ID} scenario=IMMEDIATE-AUTOCOMMIT-VISIBILITY inserts={INSERT_COUNT} replay_command={REPLAY_COMMAND}",
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
fn bd_iuvw4_track_i_connection_close_preserves_committed_writes() {
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
            let observer = open_sqlite(&fsqlite_db);
            let count: i64 = observer
                .query_row("SELECT COUNT(*) FROM t", [], |row| row.get(0))
                .expect("observe rows before writer close");
            assert_eq!(
                count, INSERT_COUNT,
                "all successful writes must already be committed before close"
            );
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
            "INFO bead_id={BEAD_ID} scenario=CLOSE-REOPEN-DURABILITY inserts={INSERT_COUNT} replay_command={REPLAY_COMMAND}"
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
        assert_eq!(
            profile.prepared_direct_insert_autocommit_executions, INSERT_COUNT as u64,
            "benchmark freshness: every measured FrankenSQLite INSERT must increment the direct autocommit execution counter"
        );

        eprintln!(
            "INFO bead_id={BEAD_ID} scenario=AUTOCOMMIT-10K inserts={} fsqlite_rows_per_sec={:.1} sqlite_rows_per_sec={:.1} direct_autocommit_execs={} commit_roundtrip_ns={} replay_command={REPLAY_COMMAND}",
            INSERT_COUNT,
            rows_per_sec(INSERT_COUNT, fsqlite_elapsed),
            rows_per_sec(INSERT_COUNT, sqlite_elapsed),
            profile.prepared_direct_insert_autocommit_executions,
            profile.commit_txn_roundtrip_time_ns,
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
        assert_eq!(
            profile.prepared_direct_insert_autocommit_executions, CYCLE_COUNT as u64,
            "profile freshness: every interleaved INSERT must increment the direct autocommit execution counter"
        );

        eprintln!(
            "INFO bead_id={BEAD_ID} scenario=INTERLEAVED cycles={} direct_autocommit_execs={} commit_roundtrip_ns={} replay_command={REPLAY_COMMAND}",
            CYCLE_COUNT,
            profile.prepared_direct_insert_autocommit_executions,
            profile.commit_txn_roundtrip_time_ns,
        );
    });
}
