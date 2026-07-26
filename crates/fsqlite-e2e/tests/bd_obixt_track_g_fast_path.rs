//! Track G fast-path regression coverage for `bd-obixt`.
#![recursion_limit = "512"]

use std::{path::Path, sync::Mutex};

use fsqlite_types::SqliteValue;
use fsqlite_vdbe::engine::{
    VdbeMetricsSnapshot, reset_vdbe_metrics, set_vdbe_metrics_enabled, vdbe_metrics_snapshot,
};
use tempfile::tempdir;

const BEAD_ID: &str = "bd-obixt";
const REPLAY_COMMAND: &str =
    "cargo test -p fsqlite-e2e --test bd_obixt_track_g_fast_path -- --nocapture --test-threads=1";

static TRACK_G_E2E_LOCK: Mutex<()> = Mutex::new(());

async fn capture_vdbe_metrics<T, Fut: std::future::Future<Output = T>>(
    f: impl FnOnce() -> Fut,
) -> (T, VdbeMetricsSnapshot) {
    set_vdbe_metrics_enabled(true);
    reset_vdbe_metrics();
    let result = f().await;
    let snapshot = vdbe_metrics_snapshot();
    reset_vdbe_metrics();
    set_vdbe_metrics_enabled(false);
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

async fn fetch_fsqlite_rows(conn: &fsqlite::Connection, table: &str) -> Vec<(i64, String)> {
    let sql = format!("SELECT id, val FROM {table} ORDER BY id");
    conn.query(&sql)
        .await
        .expect("query fsqlite rows")
        .into_iter()
        .map(|row| {
            let id = match row.get(0) {
                Some(SqliteValue::Integer(value)) => *value,
                other => panic!("expected INTEGER id, got {other:?}"),
            };
            let val = match row.get(1) {
                Some(SqliteValue::Text(value)) => value.to_string(),
                other => panic!("expected TEXT val, got {other:?}"),
            };
            (id, val)
        })
        .collect()
}

fn fetch_sqlite_rows(conn: &rusqlite::Connection, table: &str) -> Vec<(i64, String)> {
    let sql = format!("SELECT id, val FROM {table} ORDER BY id");
    let mut stmt = conn.prepare(&sql).expect("prepare sqlite select");
    stmt.query_map([], |row| {
        Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
    })
    .expect("query sqlite rows")
    .map(|row| row.expect("sqlite row"))
    .collect()
}

fn interleaved_rowids(limit: i64) -> Vec<i64> {
    let mut rowids = (1..=limit).step_by(2).collect::<Vec<_>>();
    rowids.extend((2..=limit).step_by(2));
    rowids
}

#[test]
fn bd_obixt_track_g_default_begin_sequential_inserts_keep_fast_path_hot() {
    let _guard = TRACK_G_E2E_LOCK.lock().unwrap();

    asupersync::test_utils::run_test(|| async {
        let temp = tempdir().expect("tempdir");
        let fsqlite_db = temp.path().join("track_g_default_begin_fsqlite.db");
        let sqlite_db = temp.path().join("track_g_default_begin_sqlite.db");

        let fconn = open_fsqlite(&fsqlite_db).await;
        let sconn = open_sqlite(&sqlite_db);

        assert!(
            fconn.is_concurrent_mode_default(),
            "Track G coverage must keep concurrent_mode_default enabled by default"
        );

        fconn
            .execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .await
            .expect("create fsqlite table");
        sconn
            .execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT);")
            .expect("create sqlite table");

        let (_result, metrics) = capture_vdbe_metrics(|| async {
            fconn.execute("BEGIN;").await.expect("fsqlite begin");
            for rowid in 1..=512_i64 {
                fconn
                    .execute(&format!("INSERT INTO t VALUES ({rowid}, 'v{rowid}')"))
                    .await
                    .expect("fsqlite insert");
            }
            fconn.execute("COMMIT;").await.expect("fsqlite commit");
        })
        .await;

        sconn.execute_batch("BEGIN;").expect("sqlite begin");
        for rowid in 1..=512_i64 {
            sconn
                .execute(
                    "INSERT INTO t VALUES (?1, ?2)",
                    rusqlite::params![rowid, format!("v{rowid}")],
                )
                .expect("sqlite insert");
        }
        sconn.execute_batch("COMMIT;").expect("sqlite commit");

        let fsqlite_rows = fetch_fsqlite_rows(&fconn, "t").await;
        let sqlite_rows = fetch_sqlite_rows(&sconn, "t");
        assert_eq!(
            fsqlite_rows, sqlite_rows,
            "default BEGIN sequential rowset mismatch"
        );
        assert!(
            metrics.insert_append_count >= 480,
            "default BEGIN sequential inserts should stay on the append path after the initial seed rows, got {metrics:?}"
        );
        assert!(
            metrics.insert_seek_count <= 16,
            "default BEGIN sequential inserts should avoid repeated existence seeks, got {metrics:?}"
        );
        assert_eq!(
            metrics.insert_append_hint_clear_count, 0,
            "default BEGIN sequential inserts should not clear the append hint, got {metrics:?}"
        );

        eprintln!(
            "INFO bead_id={BEAD_ID} scenario=TRACK-G-DEFAULT-BEGIN-SEQ append_count={} seek_count={} append_hint_clear_count={} replay_command={REPLAY_COMMAND}",
            metrics.insert_append_count,
            metrics.insert_seek_count,
            metrics.insert_append_hint_clear_count,
        );
    });
}

#[test]
fn bd_obixt_track_g_begin_concurrent_sequential_vs_interleaved_metrics() {
    let _guard = TRACK_G_E2E_LOCK.lock().unwrap();

    asupersync::test_utils::run_test(|| async {
        let sequential_rows: Vec<i64> = (1..=512).collect();
        let interleaved_rows = interleaved_rowids(512);

        let temp = tempdir().expect("tempdir");
        let seq_fsqlite_db = temp.path().join("track_g_seq_fsqlite.db");
        let seq_sqlite_db = temp.path().join("track_g_seq_sqlite.db");
        let gap_fsqlite_db = temp.path().join("track_g_gap_fsqlite.db");
        let gap_sqlite_db = temp.path().join("track_g_gap_sqlite.db");

        let seq_fconn = open_fsqlite(&seq_fsqlite_db).await;
        let seq_sconn = open_sqlite(&seq_sqlite_db);
        let gap_fconn = open_fsqlite(&gap_fsqlite_db).await;
        let gap_sconn = open_sqlite(&gap_sqlite_db);

        for conn in [&seq_fconn, &gap_fconn] {
            assert!(
                conn.is_concurrent_mode_default(),
                "Track G e2e coverage assumes concurrent_mode_default stays enabled"
            );
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
                .await
                .expect("create fsqlite table");
        }
        for conn in [&seq_sconn, &gap_sconn] {
            conn.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT);")
                .expect("create sqlite table");
        }

        let (_result, seq_metrics) = capture_vdbe_metrics(|| async {
            seq_fconn
                .execute("BEGIN CONCURRENT;")
                .await
                .expect("fsqlite sequential begin concurrent");
            for rowid in &sequential_rows {
                seq_fconn
                    .execute(&format!("INSERT INTO t VALUES ({rowid}, 'v{rowid}')"))
                    .await
                    .expect("fsqlite sequential insert");
            }
            seq_fconn
                .execute("COMMIT;")
                .await
                .expect("fsqlite sequential commit");
        })
        .await;

        seq_sconn
            .execute_batch("BEGIN;")
            .expect("sqlite sequential begin");
        for rowid in &sequential_rows {
            seq_sconn
                .execute(
                    "INSERT INTO t VALUES (?1, ?2)",
                    rusqlite::params![rowid, format!("v{rowid}")],
                )
                .expect("sqlite sequential insert");
        }
        seq_sconn
            .execute_batch("COMMIT;")
            .expect("sqlite sequential commit");

        let (_result, gap_metrics) = capture_vdbe_metrics(|| async {
            gap_fconn
                .execute("BEGIN CONCURRENT;")
                .await
                .expect("fsqlite interleaved begin concurrent");
            for rowid in &interleaved_rows {
                gap_fconn
                    .execute(&format!("INSERT INTO t VALUES ({rowid}, 'v{rowid}')"))
                    .await
                    .expect("fsqlite interleaved insert");
            }
            gap_fconn
                .execute("COMMIT;")
                .await
                .expect("fsqlite interleaved commit");
        })
        .await;

        gap_sconn
            .execute_batch("BEGIN;")
            .expect("sqlite interleaved begin");
        for rowid in &interleaved_rows {
            gap_sconn
                .execute(
                    "INSERT INTO t VALUES (?1, ?2)",
                    rusqlite::params![rowid, format!("v{rowid}")],
                )
                .expect("sqlite interleaved insert");
        }
        gap_sconn
            .execute_batch("COMMIT;")
            .expect("sqlite interleaved commit");

        let seq_fsqlite_rows = fetch_fsqlite_rows(&seq_fconn, "t").await;
        let seq_sqlite_rows = fetch_sqlite_rows(&seq_sconn, "t");
        let gap_fsqlite_rows = fetch_fsqlite_rows(&gap_fconn, "t").await;
        let gap_sqlite_rows = fetch_sqlite_rows(&gap_sconn, "t");

        assert_eq!(
            seq_fsqlite_rows, seq_sqlite_rows,
            "BEGIN CONCURRENT sequential rowset mismatch"
        );
        assert_eq!(
            gap_fsqlite_rows, gap_sqlite_rows,
            "BEGIN CONCURRENT interleaved rowset mismatch"
        );
        assert!(
            seq_metrics.insert_append_count > gap_metrics.insert_append_count,
            "sequential BEGIN CONCURRENT workload should stay on the append lane more often than the interleaved workload: seq={seq_metrics:?} gap={gap_metrics:?}"
        );
        assert!(
            gap_metrics.insert_seek_count > seq_metrics.insert_seek_count,
            "interleaved BEGIN CONCURRENT workload should force more existence seeks than the sequential workload: seq={seq_metrics:?} gap={gap_metrics:?}"
        );
        assert!(
            gap_metrics.insert_append_hint_clear_count >= seq_metrics.insert_append_hint_clear_count,
            "interleaved BEGIN CONCURRENT workload should not preserve the append hint better than the sequential workload: seq={seq_metrics:?} gap={gap_metrics:?}"
        );

        eprintln!(
            "INFO bead_id={BEAD_ID} scenario=TRACK-G-BEGIN-CONCURRENT-COMPARISON sequential_append_count={} sequential_seek_count={} interleaved_append_count={} interleaved_seek_count={} replay_command={REPLAY_COMMAND}",
            seq_metrics.insert_append_count,
            seq_metrics.insert_seek_count,
            gap_metrics.insert_append_count,
            gap_metrics.insert_seek_count,
        );
    });
}
