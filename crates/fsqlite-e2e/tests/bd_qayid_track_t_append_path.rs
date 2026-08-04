//! Track T append-path oracle and throughput evidence for `bd-qayid`.
#![recursion_limit = "512"]

use std::{
    env, fs,
    path::{Path, PathBuf},
    sync::Mutex,
    time::{Duration, Instant},
};

use fsqlite_types::SqliteValue;
use fsqlite_vdbe::engine::{
    VdbeMetricsSnapshot, reset_vdbe_metrics, set_vdbe_metrics_enabled, vdbe_metrics_snapshot,
};
use serde_json::json;
use tempfile::tempdir;

const BEAD_ID: &str = "bd-qayid";
const LOG_STANDARD_REF: &str = "AGENTS.md#cross-cutting-quality-contract";
const REPLAY_COMMAND: &str =
    "cargo test -p fsqlite-e2e --test bd_qayid_track_t_append_path -- --nocapture --test-threads=1";
const ORACLE_SEED: u64 = 0x71A2_1001;
const PERF_SEED: u64 = 0x71A2_1002;

static TRACK_T_E2E_LOCK: Mutex<()> = Mutex::new(());

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

fn explicit_row_insert_sql(table: &str, rowids: &[i64]) -> String {
    let values = rowids
        .iter()
        .map(|rowid| format!("({rowid}, 'v{rowid}')"))
        .collect::<Vec<_>>()
        .join(", ");
    format!("INSERT INTO {table} VALUES {values}")
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

fn rows_per_sec(rows: usize, elapsed: Duration) -> f64 {
    let secs = elapsed.as_secs_f64();
    if secs == 0.0 {
        return rows as f64;
    }
    rows as f64 / secs
}

fn write_artifact(path: &Path, payload: serde_json::Value) {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("create artifact dir");
    }
    let bytes = serde_json::to_vec_pretty(&payload).expect("serialize artifact");
    fs::write(path, bytes).expect("write artifact");
}

#[test]
fn bd_qayid_track_t_oracle_10k_sequential_append_matches_sqlite() {
    let _guard = TRACK_T_E2E_LOCK.lock().unwrap();

    asupersync::test_utils::run_test(|| async {
        let run_id = "bd-qayid-track-t-oracle";
        let trace_id = 0x71A2_1001_u64;
        let scenario_id = "TRACK-T-ORACLE-10K-SEQ";
        let rows: Vec<i64> = (1..=10_000).collect();

        let temp = tempdir().expect("tempdir");
        let fsqlite_db = temp.path().join("track_t_oracle_fsqlite.db");
        let sqlite_db = temp.path().join("track_t_oracle_sqlite.db");

        let fconn = open_fsqlite(&fsqlite_db).await;
        let sconn = open_sqlite(&sqlite_db);
        fconn
            .execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .await
            .expect("create fsqlite table");
        sconn
            .execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT);")
            .expect("create sqlite table");

        let insert_sql = explicit_row_insert_sql("t", &rows);

        let fsqlite_start = Instant::now();
        let (_result, metrics) = capture_vdbe_metrics(|| async {
            fconn.execute("BEGIN").await.expect("fsqlite begin");
            fconn
                .execute(&insert_sql)
                .await
                .expect("fsqlite bulk insert");
            fconn.execute("COMMIT").await.expect("fsqlite commit");
        })
        .await;
        let fsqlite_elapsed = fsqlite_start.elapsed();

        let sqlite_start = Instant::now();
        sconn.execute_batch("BEGIN;").expect("sqlite begin");
        sconn
            .execute_batch(&(insert_sql.clone() + ";"))
            .expect("sqlite bulk insert");
        sconn.execute_batch("COMMIT;").expect("sqlite commit");
        let sqlite_elapsed = sqlite_start.elapsed();

        let fsqlite_rows = fetch_fsqlite_rows(&fconn, "t").await;
        let sqlite_rows = fetch_sqlite_rows(&sconn, "t");
        assert_eq!(fsqlite_rows, sqlite_rows, "oracle rowset mismatch");

        assert!(
            metrics.insert_append_count >= 9_900,
            "sequential 10K insert should stay overwhelmingly append-driven, got {:?}",
            metrics
        );
        assert!(
            metrics.insert_seek_count <= 32,
            "sequential 10K insert should avoid repeated existence seeks, got {:?}",
            metrics
        );
        assert!(
            metrics.insert_append_hint_clear_count <= 4,
            "sequential 10K insert should not repeatedly clear the append hint, got {:?}",
            metrics
        );

        if let Ok(path) = env::var("FSQLITE_TRACK_T_E2E_ARTIFACT") {
            let artifact_path = PathBuf::from(path);
            write_artifact(
                &artifact_path,
                json!({
                    "bead_id": BEAD_ID,
                    "run_id": run_id,
                    "trace_id": trace_id,
                    "scenario_id": scenario_id,
                    "seed": ORACLE_SEED,
                    "log_standard_ref": LOG_STANDARD_REF,
                    "replay_command": REPLAY_COMMAND,
                    "overall_status": "pass",
                    "rows": rows.len(),
                    "fsqlite_elapsed_ms": fsqlite_elapsed.as_millis(),
                    "sqlite_elapsed_ms": sqlite_elapsed.as_millis(),
                    "fsqlite_rows_per_sec": rows_per_sec(rows.len(), fsqlite_elapsed),
                    "sqlite_rows_per_sec": rows_per_sec(rows.len(), sqlite_elapsed),
                    "vdbe_metrics": {
                        "append_count": metrics.insert_append_count,
                        "seek_count": metrics.insert_seek_count,
                        "append_hint_clear_count": metrics.insert_append_hint_clear_count,
                        "make_record_calls_total": metrics.make_record_calls_total
                    }
                }),
            );
            eprintln!(
                "DEBUG bead_id={BEAD_ID} run_id={run_id} trace_id={trace_id} scenario_id={scenario_id} seed={ORACLE_SEED} artifact_path={} replay_command={REPLAY_COMMAND}",
                artifact_path.display()
            );
        }

        eprintln!(
            "INFO bead_id={BEAD_ID} run_id={run_id} trace_id={trace_id} scenario_id={scenario_id} seed={ORACLE_SEED} rows={} append_count={} seek_count={} append_hint_clear_count={} make_record_calls_total={} fsqlite_rows_per_sec={:.1} sqlite_rows_per_sec={:.1} log_standard_ref={LOG_STANDARD_REF}",
            rows.len(),
            metrics.insert_append_count,
            metrics.insert_seek_count,
            metrics.insert_append_hint_clear_count,
            metrics.make_record_calls_total,
            rows_per_sec(rows.len(), fsqlite_elapsed),
            rows_per_sec(rows.len(), sqlite_elapsed),
        );
    });
}

#[test]
fn bd_qayid_track_t_oracle_begin_concurrent_seq_vs_interleaved_metrics() {
    let _guard = TRACK_T_E2E_LOCK.lock().unwrap();

    asupersync::test_utils::run_test(|| async {
        let rows: Vec<i64> = (1..=1_024).collect();
        let interleaved_rows = interleaved_rowids(1_024);

        let temp = tempdir().expect("tempdir");
        let seq_fsqlite_db = temp.path().join("track_t_seq_fsqlite.db");
        let seq_sqlite_db = temp.path().join("track_t_seq_sqlite.db");
        let gap_fsqlite_db = temp.path().join("track_t_gap_fsqlite.db");
        let gap_sqlite_db = temp.path().join("track_t_gap_sqlite.db");

        let seq_fconn = open_fsqlite(&seq_fsqlite_db).await;
        let seq_sconn = open_sqlite(&seq_sqlite_db);
        let gap_fconn = open_fsqlite(&gap_fsqlite_db).await;
        let gap_sconn = open_sqlite(&gap_sqlite_db);

        for conn in [&seq_fconn, &gap_fconn] {
            conn.execute("PRAGMA fsqlite.concurrent_mode=ON;")
                .await
                .expect("enable fsqlite concurrent mode");
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
                .await
                .expect("create fsqlite table");
        }
        for conn in [&seq_sconn, &gap_sconn] {
            conn.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT);")
                .expect("create sqlite table");
        }

        let seq_sql = explicit_row_insert_sql("t", &rows);
        let gap_sql = explicit_row_insert_sql("t", &interleaved_rows);

        let (_result, seq_metrics) = capture_vdbe_metrics(|| async {
            seq_fconn
                .execute("BEGIN CONCURRENT;")
                .await
                .expect("fsqlite sequential begin concurrent");
            seq_fconn
                .execute(&seq_sql)
                .await
                .expect("fsqlite sequential bulk insert");
            seq_fconn
                .execute("COMMIT;")
                .await
                .expect("fsqlite sequential commit");
        })
        .await;
        seq_sconn
            .execute_batch("BEGIN;")
            .expect("sqlite sequential begin");
        seq_sconn
            .execute_batch(&(seq_sql.clone() + ";"))
            .expect("sqlite sequential bulk insert");
        seq_sconn
            .execute_batch("COMMIT;")
            .expect("sqlite sequential commit");

        let (_result, gap_metrics) = capture_vdbe_metrics(|| async {
            gap_fconn
                .execute("BEGIN CONCURRENT;")
                .await
                .expect("fsqlite interleaved begin concurrent");
            gap_fconn
                .execute(&gap_sql)
                .await
                .expect("fsqlite interleaved bulk insert");
            gap_fconn
                .execute("COMMIT;")
                .await
                .expect("fsqlite interleaved commit");
        })
        .await;
        gap_sconn
            .execute_batch("BEGIN;")
            .expect("sqlite interleaved begin");
        gap_sconn
            .execute_batch(&(gap_sql.clone() + ";"))
            .expect("sqlite interleaved bulk insert");
        gap_sconn
            .execute_batch("COMMIT;")
            .expect("sqlite interleaved commit");

        let seq_fsqlite_rows = fetch_fsqlite_rows(&seq_fconn, "t").await;
        let seq_sqlite_rows = fetch_sqlite_rows(&seq_sconn, "t");
        let gap_fsqlite_rows = fetch_fsqlite_rows(&gap_fconn, "t").await;
        let gap_sqlite_rows = fetch_sqlite_rows(&gap_sconn, "t");
        assert_eq!(
            seq_fsqlite_rows, seq_sqlite_rows,
            "sequential BEGIN CONCURRENT rowset mismatch"
        );
        assert_eq!(
            gap_fsqlite_rows, gap_sqlite_rows,
            "interleaved BEGIN CONCURRENT rowset mismatch"
        );

        assert!(
            seq_metrics.insert_append_count > gap_metrics.insert_append_count,
            "sequential workload should stay on the append lane more often than the interleaved workload: seq={seq_metrics:?} gap={gap_metrics:?}"
        );
        assert!(
            gap_metrics.insert_seek_count > seq_metrics.insert_seek_count,
            "interleaved workload should force more existence seeks than the sequential workload: seq={seq_metrics:?} gap={gap_metrics:?}"
        );
    });
}

#[test]
#[ignore = "manual perf probe; run via rch when investigating Track T append throughput"]
fn bd_qayid_track_t_append_throughput_probe_emits_metrics() {
    let _guard = TRACK_T_E2E_LOCK.lock().unwrap();

    asupersync::test_utils::run_test(|| async {
        let seed = env::var("SEED")
            .ok()
            .and_then(|raw| raw.parse::<u64>().ok())
            .unwrap_or(PERF_SEED);
        let trace_id = env::var("TRACE_ID").unwrap_or_else(|_| seed.to_string());
        let run_id = env::var("RUN_ID").unwrap_or_else(|_| format!("{BEAD_ID}-track-t-throughput"));
        let scenario_id =
            env::var("SCENARIO_ID").unwrap_or_else(|_| "TRACK-T-APPEND-THROUGHPUT".to_owned());
        let sequential_rows: Vec<i64> = (1..=10_000).collect();
        let gapped_rows = interleaved_rowids(10_000);

        let temp = tempdir().expect("tempdir");
        let seq_db_path = temp.path().join("track_t_probe_seq.db");
        let gap_db_path = temp.path().join("track_t_probe_gap.db");
        let seq_conn = open_fsqlite(&seq_db_path).await;
        let gap_conn = open_fsqlite(&gap_db_path).await;
        seq_conn
            .execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .await
            .expect("create seq table");
        gap_conn
            .execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .await
            .expect("create gap table");

        let seq_sql = explicit_row_insert_sql("t", &sequential_rows);
        let gap_sql = explicit_row_insert_sql("t", &gapped_rows);

        let seq_start = Instant::now();
        let (_seq_result, seq_metrics) = capture_vdbe_metrics(|| async {
            seq_conn.execute(&seq_sql).await.expect("sequential insert");
        })
        .await;
        let seq_elapsed = seq_start.elapsed();

        let gap_start = Instant::now();
        let (_gap_result, gap_metrics) = capture_vdbe_metrics(|| async {
            gap_conn.execute(&gap_sql).await.expect("gapped insert");
        })
        .await;
        let gap_elapsed = gap_start.elapsed();

        let seq_count = fetch_fsqlite_rows(&seq_conn, "t").await.len();
        let gap_count = fetch_fsqlite_rows(&gap_conn, "t").await.len();
        assert_eq!(seq_count, sequential_rows.len());
        assert_eq!(gap_count, gapped_rows.len());
        assert!(
            seq_metrics.insert_append_count > gap_metrics.insert_append_count,
            "sequential insert should use the append path more often than the gapped variant"
        );
        assert!(
            gap_metrics.insert_seek_count > seq_metrics.insert_seek_count,
            "gapped insert should force more seeks than the sequential append shape"
        );

        if let Ok(path) = env::var("FSQLITE_TRACK_T_E2E_ARTIFACT") {
            let artifact_path = PathBuf::from(path);
            write_artifact(
                &artifact_path,
                json!({
                    "bead_id": BEAD_ID,
                    "run_id": run_id,
                    "trace_id": trace_id,
                    "scenario_id": scenario_id,
                    "seed": seed,
                    "log_standard_ref": LOG_STANDARD_REF,
                    "replay_command": REPLAY_COMMAND,
                    "overall_status": "pass",
                    "sequential": {
                        "rows": seq_count,
                        "elapsed_ms": seq_elapsed.as_millis(),
                        "rows_per_sec": rows_per_sec(seq_count, seq_elapsed),
                        "append_count": seq_metrics.insert_append_count,
                        "seek_count": seq_metrics.insert_seek_count,
                        "append_hint_clear_count": seq_metrics.insert_append_hint_clear_count,
                    },
                    "gapped": {
                        "rows": gap_count,
                        "elapsed_ms": gap_elapsed.as_millis(),
                        "rows_per_sec": rows_per_sec(gap_count, gap_elapsed),
                        "append_count": gap_metrics.insert_append_count,
                        "seek_count": gap_metrics.insert_seek_count,
                        "append_hint_clear_count": gap_metrics.insert_append_hint_clear_count,
                    }
                }),
            );
            eprintln!(
                "DEBUG bead_id={BEAD_ID} run_id={run_id} trace_id={trace_id} scenario_id={scenario_id} seed={seed} artifact_path={} replay_command={REPLAY_COMMAND}",
                artifact_path.display()
            );
        }

        eprintln!(
            "INFO bead_id={BEAD_ID} run_id={run_id} trace_id={trace_id} scenario_id={scenario_id} seed={seed} sequential_rows={} sequential_append_count={} sequential_seek_count={} sequential_append_hint_clear_count={} sequential_rows_per_sec={:.1} gapped_rows={} gapped_append_count={} gapped_seek_count={} gapped_append_hint_clear_count={} gapped_rows_per_sec={:.1} log_standard_ref={LOG_STANDARD_REF}",
            seq_count,
            seq_metrics.insert_append_count,
            seq_metrics.insert_seek_count,
            seq_metrics.insert_append_hint_clear_count,
            rows_per_sec(seq_count, seq_elapsed),
            gap_count,
            gap_metrics.insert_append_count,
            gap_metrics.insert_seek_count,
            gap_metrics.insert_append_hint_clear_count,
            rows_per_sec(gap_count, gap_elapsed),
        );
    });
}
