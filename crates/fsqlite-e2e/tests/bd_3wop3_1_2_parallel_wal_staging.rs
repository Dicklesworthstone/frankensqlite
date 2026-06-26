//! E2E verification for `bd-3wop3.1.2` lane-local WAL staging.

use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fsqlite_e2e::logging::init_logging;
use fsqlite_types::SqliteValue;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tempfile::tempdir;

const BEAD_ID: &str = "bd-3wop3.1.2";
const COMPATIBILITY_SELECTOR: &str = "wal_invariant,integrity_check,row_level";
const REPLAY_COMMAND: &str = "cargo test -p fsqlite-e2e --test bd_3wop3_1_2_parallel_wal_staging -- --nocapture --test-threads=1";
const CHILD_TEST_NAME: &str = "bd_3wop3_1_2_parallel_wal_staging_child_entrypoint";
const CHILD_MODE_ENV: &str = "FSQLITE_BD_3WOP3_1_2_MODE";
const CHILD_RUN_DIR_ENV: &str = "FSQLITE_BD_3WOP3_1_2_RUN_DIR";
const REPORT_ARTIFACT_ENV: &str = "FSQLITE_BD_3WOP3_1_2_ARTIFACT";
const RUN_ROOT_ENV: &str = "FSQLITE_BD_3WOP3_1_2_RUN_ROOT";

static E2E_LOCK: Mutex<()> = Mutex::new(());

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct FinalRow {
    table_name: String,
    id: i64,
    value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LaneRunSummary {
    bead_id: String,
    mode: String,
    run_id: String,
    scenario_id: String,
    compatibility_selector: String,
    replay_command: String,
    log_path: String,
    final_rows: Vec<FinalRow>,
    queue_submit_events: usize,
    flush_events: usize,
    lane_ids_seen: Vec<u16>,
    wave_lane_ids: Vec<Vec<u16>>,
    fallback_reasons: Vec<String>,
    shadow_verdicts: Vec<String>,
    control_modes: Vec<String>,
    required_fields: BTreeMap<String, bool>,
}

fn open_connection(path: &Path) -> fsqlite::Connection {
    let conn = fsqlite::Connection::open(path.to_str().expect("utf-8 db path"))
        .expect("open FrankenSQLite connection");
    conn.execute("PRAGMA journal_mode=WAL;")
        .expect("enable WAL mode");
    assert!(
        conn.is_concurrent_mode_default(),
        "bead_id={BEAD_ID} case=concurrent_mode_default_guard"
    );
    conn
}

fn insert_row(path: &Path, table_name: &'static str, id: i64, barrier: Arc<Barrier>) {
    let conn = open_connection(path);
    barrier.wait();
    conn.execute("BEGIN CONCURRENT;")
        .expect("begin concurrent transaction");
    conn.execute(&format!(
        "INSERT INTO {table_name} VALUES ({id}, '{table_name}-{id}')"
    ))
    .expect("insert row");
    conn.execute("COMMIT;").expect("commit transaction");
    conn.close().expect("close writer connection");
}

fn fetch_final_rows(path: &Path) -> Vec<FinalRow> {
    let conn = open_connection(path);
    let rows = conn
        .query(
            "SELECT table_name, id, value FROM (\
             SELECT 'a' AS table_name, id, value FROM a \
             UNION ALL \
             SELECT 'b' AS table_name, id, value FROM b\
         ) ORDER BY table_name, id",
        )
        .expect("query final rows")
        .into_iter()
        .map(|row| {
            let table_name = match row.get(0) {
                Some(SqliteValue::Text(value)) => value.to_string(),
                other => panic!("expected TEXT table_name, got {other:?}"),
            };
            let id = match row.get(1) {
                Some(SqliteValue::Integer(value)) => *value,
                other => panic!("expected INTEGER id, got {other:?}"),
            };
            let value = match row.get(2) {
                Some(SqliteValue::Text(value)) => value.to_string(),
                other => panic!("expected TEXT value, got {other:?}"),
            };
            FinalRow {
                table_name,
                id,
                value,
            }
        })
        .collect();
    conn.close().expect("close final row connection");
    rows
}

fn field_value<'a>(event: &'a Value, key: &str) -> Option<&'a Value> {
    event
        .get(key)
        .or_else(|| event.get("fields").and_then(|fields| fields.get(key)))
}

fn field_string(event: &Value, key: &str) -> Option<String> {
    field_value(event, key).map(|value| match value {
        Value::String(text) => text.clone(),
        other => other.to_string(),
    })
}

fn field_u16(event: &Value, key: &str) -> Option<u16> {
    field_value(event, key)
        .and_then(Value::as_u64)
        .and_then(|value| u16::try_from(value).ok())
}

fn stage_lane_ids_from_log(log_path: &Path) -> Vec<u16> {
    let Ok(log_text) = fs::read_to_string(log_path) else {
        return Vec::new();
    };

    let mut lane_ids = Vec::new();
    for line in log_text.lines().filter(|line| !line.trim().is_empty()) {
        let event: Value = serde_json::from_str(line).expect("parse json log event");
        let target = event
            .get("target")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if target != "fsqlite::wal::lane_staging" {
            continue;
        }
        if field_string(&event, "scenario_id").as_deref() != Some("parallel_wal_lane_stage") {
            continue;
        }
        if let Some(wal_lane_id) = field_u16(&event, "wal_lane_id") {
            lane_ids.push(wal_lane_id);
        }
    }

    lane_ids
}

fn wait_for_stage_lane_ids(log_path: &Path, expected_total: usize) -> Vec<u16> {
    for _ in 0..50 {
        let stage_lane_ids = stage_lane_ids_from_log(log_path);
        if stage_lane_ids.len() >= expected_total {
            return stage_lane_ids;
        }
        thread::sleep(Duration::from_millis(10));
    }

    let observed = stage_lane_ids_from_log(log_path);
    panic!(
        "bead_id={BEAD_ID} case=wave_stage_events_missing expected_at_least={expected_total} observed={}",
        observed.len()
    );
}

fn summarize_lane_logs(
    log_path: &Path,
    mode: &str,
    final_rows: Vec<FinalRow>,
    wave_lane_ids: Vec<Vec<u16>>,
) -> LaneRunSummary {
    let mut queue_submit_events = 0_usize;
    let mut flush_events = 0_usize;
    let mut lane_ids_seen = BTreeSet::new();
    let mut fallback_reasons = BTreeSet::new();
    let mut shadow_verdicts = BTreeSet::new();
    let mut control_modes = BTreeSet::new();
    let mut required_fields = BTreeMap::from([
        ("trace_id".to_owned(), false),
        ("run_id".to_owned(), false),
        ("scenario_id".to_owned(), false),
        ("batch_id".to_owned(), false),
        ("batch_membership".to_owned(), false),
        ("queue_delay_ns".to_owned(), false),
        ("target_wait_ns".to_owned(), false),
        ("max_wait_ns".to_owned(), false),
        ("fsync_boundary".to_owned(), false),
        ("ordering_phase".to_owned(), false),
        ("rollback_mode_active".to_owned(), false),
        ("wal_lane_id".to_owned(), false),
        ("lane_backlog".to_owned(), false),
        ("staged_frame_count".to_owned(), false),
        ("flush_trigger".to_owned(), false),
        ("control_mode".to_owned(), false),
        ("lane_policy_version".to_owned(), false),
        ("shadow_verdict".to_owned(), false),
        ("compatibility_selector".to_owned(), false),
        ("fallback_reason".to_owned(), false),
        ("elapsed_ns".to_owned(), false),
    ]);

    let log_text = fs::read_to_string(log_path).expect("read lane staging log");
    for line in log_text.lines().filter(|line| !line.trim().is_empty()) {
        let event: Value = serde_json::from_str(line).expect("parse json log event");
        let target = event
            .get("target")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if target != "fsqlite::wal::lane_staging" {
            continue;
        }

        for key in required_fields.keys().cloned().collect::<Vec<_>>() {
            if field_value(&event, &key).is_some() {
                required_fields.insert(key, true);
            }
        }

        if let Some(wal_lane_id) = field_u16(&event, "wal_lane_id") {
            lane_ids_seen.insert(wal_lane_id);
        }
        if let Some(control_mode) = field_string(&event, "control_mode") {
            control_modes.insert(control_mode);
        }
        if let Some(fallback_reason) = field_string(&event, "fallback_reason") {
            fallback_reasons.insert(fallback_reason);
        }
        if let Some(shadow_verdict) = field_string(&event, "shadow_verdict") {
            shadow_verdicts.insert(shadow_verdict);
        }
        match field_string(&event, "scenario_id").as_deref() {
            Some("parallel_wal_lane_stage") => queue_submit_events += 1,
            Some("parallel_wal_lane_flush") => flush_events += 1,
            _ => {}
        }
    }

    LaneRunSummary {
        bead_id: BEAD_ID.to_owned(),
        mode: mode.to_owned(),
        run_id: format!("{mode}-lane-staging"),
        scenario_id: "parallel_wal_lane_staging_e2e".to_owned(),
        compatibility_selector: COMPATIBILITY_SELECTOR.to_owned(),
        replay_command: REPLAY_COMMAND.to_owned(),
        log_path: log_path.display().to_string(),
        final_rows,
        queue_submit_events,
        flush_events,
        lane_ids_seen: lane_ids_seen.into_iter().collect(),
        wave_lane_ids,
        fallback_reasons: fallback_reasons.into_iter().collect(),
        shadow_verdicts: shadow_verdicts.into_iter().collect(),
        control_modes: control_modes.into_iter().collect(),
        required_fields,
    }
}

fn run_child_workload(run_dir: &Path, mode: &str) -> LaneRunSummary {
    let _guard = init_logging(run_dir, true).expect("initialize child logging");

    let db_path = run_dir.join("parallel_wal_staging.db");
    let log_path = run_dir.join("test.log.jsonl");
    {
        let conn = open_connection(&db_path);
        conn.execute("CREATE TABLE a (id INTEGER PRIMARY KEY, value TEXT)")
            .expect("create table a");
        conn.execute("CREATE TABLE b (id INTEGER PRIMARY KEY, value TEXT)")
            .expect("create table b");
        conn.close().expect("close setup connection");
    }

    let mut wave_lane_ids = Vec::new();
    for wave in 0..2_i64 {
        let stage_events_before_wave = stage_lane_ids_from_log(&log_path).len();
        let barrier = Arc::new(Barrier::new(3));
        let db_path_a = db_path.clone();
        let db_path_b = db_path.clone();
        let barrier_a = Arc::clone(&barrier);
        let barrier_b = Arc::clone(&barrier);

        let writer_a = thread::spawn(move || {
            insert_row(&db_path_a, "a", wave + 1, barrier_a);
        });
        let writer_b = thread::spawn(move || {
            insert_row(&db_path_b, "b", wave + 1, barrier_b);
        });

        barrier.wait();
        writer_a.join().expect("join writer_a");
        writer_b.join().expect("join writer_b");

        let mut observed_lane_ids =
            wait_for_stage_lane_ids(&log_path, stage_events_before_wave + 2)
                .into_iter()
                .skip(stage_events_before_wave)
                .collect::<Vec<_>>();
        observed_lane_ids.sort_unstable();
        wave_lane_ids.push(observed_lane_ids);
    }

    let final_rows = fetch_final_rows(&db_path);
    summarize_lane_logs(&log_path, mode, final_rows, wave_lane_ids)
}

fn read_summary(path: &Path) -> LaneRunSummary {
    serde_json::from_slice(&fs::read(path).expect("read child summary"))
        .expect("parse child summary")
}

fn run_root_for(label: &str) -> (PathBuf, Option<tempfile::TempDir>) {
    if let Ok(base) = env::var(RUN_ROOT_ENV) {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock after epoch")
            .as_nanos();
        let dir = PathBuf::from(base).join(format!("{label}-{nanos}"));
        fs::create_dir_all(&dir).expect("create persistent run dir");
        (dir, None)
    } else {
        let tmp = tempdir().expect("create temp run dir");
        let dir = tmp.path().join(label);
        fs::create_dir_all(&dir).expect("create temp mode run dir");
        (dir, Some(tmp))
    }
}

fn spawn_mode_run(
    label: &str,
    mode: &str,
    max_batch_bytes: Option<u64>,
    shadow_compare_per_mille: Option<u16>,
) -> LaneRunSummary {
    let (run_dir, _tmp_guard) = run_root_for(label);
    let summary_path = run_dir.join("summary.json");

    let mut command = Command::new(env::current_exe().expect("current test executable"));
    command
        .arg("--exact")
        .arg(CHILD_TEST_NAME)
        .arg("--ignored")
        .arg("--nocapture")
        .arg("--test-threads=1")
        .env("RUST_LOG", "trace")
        .env(CHILD_MODE_ENV, mode)
        .env(CHILD_RUN_DIR_ENV, &run_dir)
        .env("FSQLITE_PARALLEL_WAL_MODE", mode)
        .env("FSQLITE_PARALLEL_WAL_LANES", "2");
    if let Some(limit) = max_batch_bytes {
        command.env("FSQLITE_PARALLEL_WAL_MAX_BATCH_BYTES", limit.to_string());
    }
    if let Some(rate) = shadow_compare_per_mille {
        command.env(
            "FSQLITE_PARALLEL_WAL_SHADOW_COMPARE_PER_MILLE",
            rate.to_string(),
        );
    }

    let status = command.status().expect("spawn child lane-staging run");
    assert!(
        status.success(),
        "bead_id={BEAD_ID} case={label}_child_process_failed status={status}"
    );
    read_summary(&summary_path)
}

fn assert_required_fields(summary: &LaneRunSummary, case: &str) {
    let missing = summary
        .required_fields
        .iter()
        .filter_map(|(field, present)| (!present).then_some(field.clone()))
        .collect::<Vec<_>>();
    assert!(
        missing.is_empty(),
        "bead_id={BEAD_ID} case={case} missing required lane log fields: {missing:?}"
    );
}

fn expected_final_rows() -> Vec<FinalRow> {
    vec![
        FinalRow {
            table_name: "a".to_owned(),
            id: 1,
            value: "a-1".to_owned(),
        },
        FinalRow {
            table_name: "a".to_owned(),
            id: 2,
            value: "a-2".to_owned(),
        },
        FinalRow {
            table_name: "b".to_owned(),
            id: 1,
            value: "b-1".to_owned(),
        },
        FinalRow {
            table_name: "b".to_owned(),
            id: 2,
            value: "b-2".to_owned(),
        },
    ]
}

#[test]
fn bd_3wop3_1_2_parallel_wal_staging_control_modes_emit_lane_logs_and_preserve_rows() {
    let _guard = E2E_LOCK.lock().expect("lane staging e2e lock");

    let auto = spawn_mode_run("auto", "auto", None, None);
    let auto_sampled = spawn_mode_run("auto-sampled", "auto", None, Some(1_000));
    let conservative = spawn_mode_run("conservative", "conservative", None, None);
    let shadow_compare = spawn_mode_run("shadow_compare", "shadow_compare", None, None);

    let expected_rows = expected_final_rows();
    for (case, summary) in [
        ("auto", &auto),
        ("auto", &auto_sampled),
        ("conservative", &conservative),
        ("shadow_compare", &shadow_compare),
    ] {
        assert_eq!(
            summary.final_rows, expected_rows,
            "bead_id={BEAD_ID} case={case}_final_rows_match_expected"
        );
        assert_required_fields(summary, &format!("{case}_required_fields_present"));
        assert!(
            summary.queue_submit_events >= 4,
            "bead_id={BEAD_ID} case={case}_queue_submit_events expected >= 4, got {}",
            summary.queue_submit_events
        );
        assert!(
            summary.flush_events >= 1,
            "bead_id={BEAD_ID} case={case}_flush_events expected >= 1, got {}",
            summary.flush_events
        );
        assert!(
            summary.control_modes.iter().any(|observed| observed == case
                || (case == "shadow_compare" && observed == "shadow_compare")),
            "bead_id={BEAD_ID} case={case}_control_mode_logged observed={:?}",
            summary.control_modes
        );
    }

    assert_eq!(
        auto.final_rows, conservative.final_rows,
        "bead_id={BEAD_ID} case=auto_vs_conservative_row_level_equivalence"
    );
    assert_eq!(
        auto.final_rows, auto_sampled.final_rows,
        "bead_id={BEAD_ID} case=auto_vs_auto_sampled_row_level_equivalence"
    );
    assert_eq!(
        auto.final_rows, shadow_compare.final_rows,
        "bead_id={BEAD_ID} case=auto_vs_shadow_compare_row_level_equivalence"
    );
    assert_eq!(
        auto.lane_ids_seen,
        vec![0, 1],
        "bead_id={BEAD_ID} case=auto_lane_ids_cover_two_lanes observed={:?}",
        auto.lane_ids_seen
    );
    assert_eq!(
        conservative.lane_ids_seen,
        vec![0],
        "bead_id={BEAD_ID} case=conservative_collapses_to_single_lane observed={:?}",
        conservative.lane_ids_seen
    );
    assert_eq!(
        auto.wave_lane_ids,
        vec![vec![0, 1], vec![0, 1]],
        "bead_id={BEAD_ID} case=auto_reuses_lane_ids_after_worker_churn observed={:?}",
        auto.wave_lane_ids
    );
    assert_eq!(
        conservative.wave_lane_ids,
        vec![vec![0, 0], vec![0, 0]],
        "bead_id={BEAD_ID} case=conservative_routes_all_waves_through_single_lane observed={:?}",
        conservative.wave_lane_ids
    );
    assert!(
        conservative
            .fallback_reasons
            .iter()
            .any(|reason| reason == "operator_forced"),
        "bead_id={BEAD_ID} case=conservative_logs_operator_forced_fallback reasons={:?}",
        conservative.fallback_reasons
    );
    assert!(
        auto_sampled
            .shadow_verdicts
            .iter()
            .any(|verdict| verdict == "clean"),
        "bead_id={BEAD_ID} case=auto_sampled_shadow_compare_emits_clean_shadow_verdict verdicts={:?}",
        auto_sampled.shadow_verdicts
    );
    assert!(
        shadow_compare
            .shadow_verdicts
            .iter()
            .any(|verdict| verdict == "clean"),
        "bead_id={BEAD_ID} case=shadow_compare_emits_clean_shadow_verdict verdicts={:?}",
        shadow_compare.shadow_verdicts
    );

    if let Ok(path) = env::var(REPORT_ARTIFACT_ENV) {
        let artifact = serde_json::json!({
            "bead_id": BEAD_ID,
            "compatibility_selector": COMPATIBILITY_SELECTOR,
            "replay_command": REPLAY_COMMAND,
            "runs": {
                "auto": auto,
                "auto_sampled": auto_sampled,
                "conservative": conservative,
                "shadow_compare": shadow_compare,
            }
        });
        let artifact_path = PathBuf::from(path);
        if let Some(parent) = artifact_path.parent() {
            fs::create_dir_all(parent).expect("create report artifact parent");
        }
        fs::write(
            &artifact_path,
            serde_json::to_vec_pretty(&artifact).expect("serialize control-mode artifact"),
        )
        .expect("write control-mode artifact");
    }
}

#[test]
fn bd_3wop3_1_2_parallel_wal_staging_lane_overflow_falls_back_without_row_drift() {
    let _guard = E2E_LOCK.lock().expect("lane staging e2e lock");

    let baseline = spawn_mode_run("baseline-auto", "auto", None, None);
    let overflow = spawn_mode_run("overflow-auto", "auto", Some(1), None);

    assert_eq!(
        overflow.final_rows, baseline.final_rows,
        "bead_id={BEAD_ID} case=lane_overflow_row_level_equivalence"
    );
    assert_required_fields(&overflow, "lane_overflow_required_fields_present");
    assert!(
        overflow
            .fallback_reasons
            .iter()
            .any(|reason| reason == "lane_overflow"),
        "bead_id={BEAD_ID} case=lane_overflow_forces_logged_fallback reasons={:?}",
        overflow.fallback_reasons
    );
    assert!(
        overflow.control_modes.iter().any(|mode| mode == "auto"),
        "bead_id={BEAD_ID} case=lane_overflow_preserves_auto_control_mode logging={:?}",
        overflow.control_modes
    );
}

#[test]
#[ignore = "helper entrypoint for the parent test harness; not meant to run directly"]
fn bd_3wop3_1_2_parallel_wal_staging_child_entrypoint() {
    let run_dir = PathBuf::from(
        env::var(CHILD_RUN_DIR_ENV)
            .expect("child run dir env must be present for child entrypoint"),
    );
    fs::create_dir_all(&run_dir).expect("create child run dir");
    let mode =
        env::var(CHILD_MODE_ENV).expect("child mode env must be present for child entrypoint");
    let summary = run_child_workload(&run_dir, &mode);
    fs::write(
        run_dir.join("summary.json"),
        serde_json::to_vec_pretty(&summary).expect("serialize lane run summary"),
    )
    .expect("write lane run summary");
}
