//! Track S register-value lifecycle and perf coverage for `bd-abgqx`.
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
use fsqlite_vdbe::engine::{
    VdbeMetricsSnapshot, reset_vdbe_metrics, set_vdbe_metrics_enabled, vdbe_metrics_snapshot,
};
use tempfile::tempdir;

const BEAD_ID: &str = "bd-abgqx";
const REPLAY_COMMAND: &str = "cargo test -p fsqlite-e2e --test bd_abgqx_track_s_register_values -- --nocapture --test-threads=1";
const INSERT_ROWS_FAST: i64 = 512;
const INSERT_ROWS_PERF: i64 = 10_000;

static TRACK_S_E2E_LOCK: Mutex<()> = Mutex::new(());

#[derive(Debug)]
struct TrackSMetricsSnapshot {
    hot_path: HotPathProfileSnapshot,
    vdbe: VdbeMetricsSnapshot,
}

async fn capture_track_s_metrics<T, Fut>(f: impl FnOnce() -> Fut) -> (T, TrackSMetricsSnapshot)
where
    Fut: std::future::Future<Output = T>,
{
    set_hot_path_profile_enabled(true);
    reset_hot_path_profile();
    set_vdbe_metrics_enabled(true);
    reset_vdbe_metrics();
    let result = f().await;
    let snapshot = TrackSMetricsSnapshot {
        hot_path: hot_path_profile_snapshot(),
        vdbe: vdbe_metrics_snapshot(),
    };
    reset_hot_path_profile();
    set_hot_path_profile_enabled(false);
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

async fn fetch_fsqlite_rows(conn: &fsqlite::Connection) -> Vec<(i64, String, i64)> {
    conn.query("SELECT id, val, score FROM reg_track ORDER BY id")
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
            let score = match row.get(2) {
                Some(SqliteValue::Integer(value)) => *value,
                other => panic!("expected INTEGER score, got {other:?}"),
            };
            (id, val, score)
        })
        .collect()
}

fn fetch_sqlite_rows(conn: &rusqlite::Connection) -> Vec<(i64, String, i64)> {
    let mut stmt = conn
        .prepare("SELECT id, val, score FROM reg_track ORDER BY id")
        .expect("prepare sqlite select");
    stmt.query_map([], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, i64>(2)?,
        ))
    })
    .expect("query sqlite rows")
    .map(|row| row.expect("sqlite row"))
    .collect()
}

fn rows_per_sec(rows: i64, elapsed: Duration) -> f64 {
    let secs = elapsed.as_secs_f64();
    if secs == 0.0 {
        return rows as f64;
    }
    rows as f64 / secs
}

fn estimated_metric_bytes_for_value(value: &SqliteValue) -> u64 {
    match value {
        SqliteValue::Null => 0,
        SqliteValue::Integer(_) | SqliteValue::Float(_) => {
            u64::try_from(std::mem::size_of::<SqliteValue>()).unwrap_or(u64::MAX)
        }
        SqliteValue::Text(text) => {
            u64::try_from(std::mem::size_of::<SqliteValue>().saturating_add(text.len()))
                .unwrap_or(u64::MAX)
        }
        SqliteValue::Blob(blob) => {
            u64::try_from(std::mem::size_of::<SqliteValue>().saturating_add(blob.len()))
                .unwrap_or(u64::MAX)
        }
    }
}

fn estimated_metric_bytes_for_row(values: &[SqliteValue]) -> u64 {
    values.iter().fold(0_u64, |acc, value| {
        acc.saturating_add(estimated_metric_bytes_for_value(value))
    })
}

#[test]
fn bd_abgqx_track_s_prepared_insert_select_keeps_metrics_heap_light() {
    let _guard = TRACK_S_E2E_LOCK.lock().unwrap();

    asupersync::test_utils::run_test(|| async {
        let temp = tempdir().expect("tempdir");
        let fsqlite_db = temp.path().join("track_s_register_values_fsqlite.db");
        let sqlite_db = temp.path().join("track_s_register_values_sqlite.db");

        let fconn = open_fsqlite(&fsqlite_db).await;
        let sconn = open_sqlite(&sqlite_db);

        assert!(
            fconn.is_concurrent_mode_default(),
            "Track S coverage must keep concurrent_mode_default enabled by default"
        );

        fconn
            .execute("CREATE TABLE reg_track (id INTEGER PRIMARY KEY, val TEXT NOT NULL, score INTEGER NOT NULL)")
            .await
            .expect("create fsqlite table");
        sconn
            .execute_batch(
                "CREATE TABLE reg_track (id INTEGER PRIMARY KEY, val TEXT NOT NULL, score INTEGER NOT NULL);",
            )
            .expect("create sqlite table");

        let insert_stmt = fconn
            .prepare("INSERT INTO reg_track VALUES (?1, ?2, ?3)")
            .await
            .expect("prepare fsqlite insert");
        let lookup_stmt = fconn
            .prepare("SELECT val, score FROM reg_track WHERE id = ?1")
            .await
            .expect("prepare fsqlite lookup");
        let mut sqlite_insert = sconn
            .prepare("INSERT INTO reg_track VALUES (?1, ?2, ?3)")
            .expect("prepare sqlite insert");
        let mut sqlite_lookup = sconn
            .prepare("SELECT val, score FROM reg_track WHERE id = ?1")
            .expect("prepare sqlite lookup");

        let probe_ids = [1_i64, 64, 128, 256, 512, 256, 128, 64, 1];
        let (_result, metrics) = capture_track_s_metrics(|| async {
            for rowid in 1..=INSERT_ROWS_FAST {
                let value = format!("v{rowid}");
                let score = rowid * 10;
                insert_stmt
                    .execute_with_params(&[
                        SqliteValue::Integer(rowid),
                        SqliteValue::Text(value.as_str().into()),
                        SqliteValue::Integer(score),
                    ])
                    .await
                    .expect("fsqlite insert");
            }

            for rowid in probe_ids {
                let row = lookup_stmt
                    .query_row_with_params(&[SqliteValue::Integer(rowid)])
                    .await
                    .expect("fsqlite lookup");
                assert_eq!(
                    row.values().to_vec(),
                    vec![
                        SqliteValue::Text(format!("v{rowid}").into()),
                        SqliteValue::Integer(rowid * 10),
                    ],
                    "prepared lookup should return the inserted row"
                );
            }
        })
        .await;

        for rowid in 1..=INSERT_ROWS_FAST {
            let value = format!("v{rowid}");
            let score = rowid * 10;
            sqlite_insert
                .execute(rusqlite::params![rowid, value, score])
                .expect("sqlite insert");
        }

        for rowid in probe_ids {
            let (val, score): (String, i64) = sqlite_lookup
                .query_row(rusqlite::params![rowid], |row| {
                    Ok((row.get(0)?, row.get(1)?))
                })
                .expect("sqlite lookup");
            assert_eq!(val, format!("v{rowid}"));
            assert_eq!(score, rowid * 10);
        }

        let fsqlite_rows = fetch_fsqlite_rows(&fconn).await;
        let sqlite_rows = fetch_sqlite_rows(&sconn);
        let expected_probe_metric_bytes = probe_ids
            .iter()
            .map(|rowid| {
                estimated_metric_bytes_for_row(&[
                    SqliteValue::Text(format!("v{rowid}").into()),
                    SqliteValue::Integer(rowid * 10),
                ])
            })
            .sum::<u64>();
        assert_eq!(
            fsqlite_rows, sqlite_rows,
            "prepared register rowset mismatch"
        );
        assert_eq!(
            metrics.hot_path.prepared_insert_fast_lane_hits, INSERT_ROWS_FAST as u64,
            "prepared Track S inserts should stay on the prepared fast lane"
        );
        assert_eq!(
            metrics.hot_path.prepared_direct_insert_executions, INSERT_ROWS_FAST as u64,
            "prepared Track S inserts should execute through the direct insert path"
        );
        assert_eq!(
            metrics.vdbe.make_record_calls_total, 0,
            "simple prepared Track S inserts should avoid the VDBE MakeRecord path entirely"
        );
        assert_eq!(
            metrics.vdbe.result_rows_total,
            probe_ids.len() as u64,
            "prepared lookups should surface one result row per probe"
        );
        assert_eq!(
            metrics.vdbe.result_value_heap_bytes_total, expected_probe_metric_bytes,
            "result-row metrics should match the inline Track S probe footprint estimate: {metrics:?}"
        );
        assert_eq!(
            metrics.vdbe.decoded_value_heap_bytes_total, expected_probe_metric_bytes,
            "decoded-value metrics should match the inline Track S probe footprint estimate: {metrics:?}"
        );
        assert!(
            metrics.vdbe.result_row_materialization_time_ns_total > 0,
            "prepared lookups should record result-row materialization timing: {metrics:?}"
        );

        eprintln!(
            "INFO bead_id={BEAD_ID} scenario=prepared_insert_select rows={} probes={} prepared_insert_fast_lane_hits={} prepared_direct_insert_executions={} make_record_calls_total={} result_rows_total={} decoded_value_heap_bytes_total={} result_value_heap_bytes_total={} result_row_materialization_time_ns_total={} replay_command={REPLAY_COMMAND}",
            INSERT_ROWS_FAST,
            probe_ids.len(),
            metrics.hot_path.prepared_insert_fast_lane_hits,
            metrics.hot_path.prepared_direct_insert_executions,
            metrics.vdbe.make_record_calls_total,
            metrics.vdbe.result_rows_total,
            metrics.vdbe.decoded_value_heap_bytes_total,
            metrics.vdbe.result_value_heap_bytes_total,
            metrics.vdbe.result_row_materialization_time_ns_total,
        );
    });
}

#[test]
#[ignore = "manual perf probe; run via rch when validating Track S register throughput"]
fn bd_abgqx_track_s_prepared_insert_10k_perf_probe_emits_metrics() {
    let _guard = TRACK_S_E2E_LOCK.lock().unwrap();

    asupersync::test_utils::run_test(|| async {
        let temp = tempdir().expect("tempdir");
        let fsqlite_db = temp.path().join("track_s_perf_fsqlite.db");
        let sqlite_db = temp.path().join("track_s_perf_sqlite.db");

        let fconn = open_fsqlite(&fsqlite_db).await;
        let sconn = open_sqlite(&sqlite_db);

        fconn
            .execute("CREATE TABLE reg_track (id INTEGER PRIMARY KEY, val TEXT NOT NULL, score INTEGER NOT NULL)")
            .await
            .expect("create fsqlite table");
        sconn
            .execute_batch(
                "CREATE TABLE reg_track (id INTEGER PRIMARY KEY, val TEXT NOT NULL, score INTEGER NOT NULL);",
            )
            .expect("create sqlite table");

        let insert_stmt = fconn
            .prepare("INSERT INTO reg_track VALUES (?1, ?2, ?3)")
            .await
            .expect("prepare fsqlite insert");
        let mut sqlite_insert = sconn
            .prepare("INSERT INTO reg_track VALUES (?1, ?2, ?3)")
            .expect("prepare sqlite insert");

        let fsqlite_start = Instant::now();
        let (_result, metrics) = capture_track_s_metrics(|| async {
            for rowid in 1..=INSERT_ROWS_PERF {
                let value = format!("v{rowid}");
                insert_stmt
                    .execute_with_params(&[
                        SqliteValue::Integer(rowid),
                        SqliteValue::Text(value.as_str().into()),
                        SqliteValue::Integer(rowid * 10),
                    ])
                    .await
                    .expect("fsqlite insert");
            }
        })
        .await;
        let fsqlite_elapsed = fsqlite_start.elapsed();

        let sqlite_start = Instant::now();
        for rowid in 1..=INSERT_ROWS_PERF {
            let value = format!("v{rowid}");
            sqlite_insert
                .execute(rusqlite::params![rowid, value, rowid * 10])
                .expect("sqlite insert");
        }
        let sqlite_elapsed = sqlite_start.elapsed();

        let fsqlite_rows = fetch_fsqlite_rows(&fconn).await;
        let sqlite_rows = fetch_sqlite_rows(&sconn);
        assert_eq!(fsqlite_rows, sqlite_rows, "10k prepared rowset mismatch");
        assert_eq!(
            metrics.hot_path.prepared_insert_fast_lane_hits, INSERT_ROWS_PERF as u64,
            "prepared 10k Track S probe should stay on the prepared fast lane"
        );
        assert_eq!(
            metrics.hot_path.prepared_direct_insert_executions, INSERT_ROWS_PERF as u64,
            "prepared 10k Track S probe should execute through the direct insert path"
        );
        assert_eq!(
            metrics.vdbe.make_record_calls_total, 0,
            "simple prepared 10k Track S probe should avoid the VDBE MakeRecord path entirely"
        );

        eprintln!(
            "INFO bead_id={BEAD_ID} scenario=prepared_insert_10k_perf_probe rows={} prepared_insert_fast_lane_hits={} prepared_direct_insert_executions={} make_record_calls_total={} make_record_blob_bytes_total={} fsqlite_rows_per_sec={:.1} sqlite_rows_per_sec={:.1} replay_command={REPLAY_COMMAND}",
            INSERT_ROWS_PERF,
            metrics.hot_path.prepared_insert_fast_lane_hits,
            metrics.hot_path.prepared_direct_insert_executions,
            metrics.vdbe.make_record_calls_total,
            metrics.vdbe.make_record_blob_bytes_total,
            rows_per_sec(INSERT_ROWS_PERF, fsqlite_elapsed),
            rows_per_sec(INSERT_ROWS_PERF, sqlite_elapsed),
        );
    });
}
