//! Deterministic aggregate/window end-to-end parity checks for `bd-2wt.2`.

#![allow(clippy::too_many_lines)]
#![recursion_limit = "512"]

use std::{
    env, fs,
    path::PathBuf,
    sync::{Mutex, MutexGuard, OnceLock},
    time::Instant,
};

use fsqlite::Connection as FrankenConnection;
use fsqlite_core::connection::{
    hot_path_profile_enabled, hot_path_profile_snapshot, reset_hot_path_profile,
    set_hot_path_profile_enabled,
};
use fsqlite_e2e::comparison::SqlValue;
use serde_json::json;
use tempfile::tempdir;

const BEAD_ID: &str = "bd-2wt.2";
const LOG_STANDARD_REF: &str = "AGENTS.md#testing";
const DEFAULT_SEED: u64 = 2_204_112_001;
const REPLAY_COMMAND: &str = "cargo test -p fsqlite-e2e --test bd_2wt_2_aggregate_window_engine -- --nocapture --test-threads=1";
const ARTIFACT_ENV: &str = "FSQLITE_BD_2WT_2_E2E_ARTIFACT";
const MIN_WINDOW_PARTITIONS_TOTAL: u64 = 50;

struct QueryCase {
    name: &'static str,
    sql: &'static str,
}

const SETUP_STATEMENTS: &[&str] = &[
    "CREATE TABLE metrics(id INTEGER PRIMARY KEY, grp TEXT NOT NULL, dept TEXT NOT NULL, name TEXT NOT NULL, score INTEGER NOT NULL, sep TEXT NOT NULL)",
    "INSERT INTO metrics VALUES
        (1, 'A', 'eng', 'ada', 10, '|'),
        (2, 'A', 'eng', 'bea', 10, '|'),
        (3, 'A', 'eng', 'cy', 20, '|'),
        (4, 'B', 'ops', 'dia', 5, '/'),
        (5, 'B', 'ops', 'eli', 5, '/'),
        (6, 'B', 'ops', 'fay', 15, '/'),
        (7, 'C', 'eng', 'gia', 20, '|'),
        (8, 'C', 'ops', 'hal', 25, '/')",
];

const QUERY_CASES: &[QueryCase] = &[
    QueryCase {
        name: "grouped_aggregates_ordered_concat",
        sql: "SELECT dept,
                     COUNT(*) AS row_count,
                     COUNT(*) FILTER (WHERE score >= 10) AS scored_rows,
                     COUNT(DISTINCT grp) AS distinct_grps,
                     SUM(score) AS sum_score,
                     MIN(score) AS min_score,
                     MAX(score) AS max_score,
                     printf('%.2f', AVG(score)) AS avg_score,
                     GROUP_CONCAT(name, '|' ORDER BY score DESC, id) AS ordered_names,
                     STRING_AGG(name, '|' ORDER BY score DESC, id) AS ordered_names_alias
              FROM metrics
              GROUP BY dept
              ORDER BY dept",
    },
    QueryCase {
        name: "window_ranking_and_navigation",
        sql: "SELECT id,
                     dept,
                     name,
                     score,
                     ROW_NUMBER() OVER (PARTITION BY dept ORDER BY score DESC, id) AS rn,
                     RANK() OVER (PARTITION BY dept ORDER BY score DESC) AS rnk,
                     DENSE_RANK() OVER (PARTITION BY dept ORDER BY score DESC) AS dense_rnk,
                     NTILE(2) OVER (PARTITION BY dept ORDER BY score DESC, id) AS nt,
                     LAG(name, 1, '<NONE>') OVER (PARTITION BY dept ORDER BY score DESC, id) AS prev_name,
                     LEAD(name, 1, '<NONE>') OVER (PARTITION BY dept ORDER BY score DESC, id) AS next_name
              FROM metrics
              ORDER BY id",
    },
    QueryCase {
        name: "window_frames_exclude_filter_and_nth_value",
        sql: "SELECT id,
                     grp,
                     name,
                     score,
                     SUM(score) OVER (
                         PARTITION BY grp
                         ORDER BY score
                         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                     ) AS rows_sum,
                     SUM(score) OVER (
                         PARTITION BY grp
                         ORDER BY score
                         RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                     ) AS range_sum,
                     SUM(score) OVER (
                         PARTITION BY grp
                         ORDER BY score
                         GROUPS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                     ) AS groups_sum,
                     SUM(score) FILTER (WHERE score >= 10) OVER (
                         PARTITION BY grp
                         ORDER BY id
                         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                     ) AS filtered_rows_sum,
                     NTH_VALUE(name, 2) OVER (
                         PARTITION BY grp
                         ORDER BY score, id
                         ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                     ) AS second_name,
                     SUM(score) OVER (
                         PARTITION BY grp
                         ORDER BY score
                         ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                         EXCLUDE CURRENT ROW
                     ) AS excl_current
              FROM metrics
              ORDER BY id",
    },
];

#[derive(Debug)]
struct QuerySummary {
    pass: &'static str,
    name: &'static str,
    row_count: usize,
    first_row: Option<Vec<String>>,
}

#[derive(Debug)]
struct ScenarioOutcome {
    elapsed_ms: u128,
    window_func_partitions_total: u64,
    query_summaries: Vec<QuerySummary>,
}

struct HotPathProfileGuard {
    was_enabled: bool,
}

impl HotPathProfileGuard {
    fn new() -> Self {
        let was_enabled = hot_path_profile_enabled();
        reset_hot_path_profile();
        set_hot_path_profile_enabled(true);
        Self { was_enabled }
    }
}

impl Drop for HotPathProfileGuard {
    fn drop(&mut self) {
        reset_hot_path_profile();
        set_hot_path_profile_enabled(self.was_enabled);
    }
}

fn aggregate_window_e2e_serializer() -> MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn csqlite_query_values(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<SqlValue>> {
    let mut stmt = conn.prepare(sql).expect("csqlite prepare");
    let col_count = stmt.column_count();
    let rows = stmt
        .query_map([], |row| {
            let mut values = Vec::with_capacity(col_count);
            for idx in 0..col_count {
                let value: rusqlite::types::Value =
                    row.get(idx).unwrap_or(rusqlite::types::Value::Null);
                values.push(match value {
                    rusqlite::types::Value::Null => SqlValue::Null,
                    rusqlite::types::Value::Integer(v) => SqlValue::Integer(v),
                    rusqlite::types::Value::Real(v) => SqlValue::Real(v),
                    rusqlite::types::Value::Text(v) => SqlValue::Text(v),
                    rusqlite::types::Value::Blob(v) => SqlValue::Blob(v),
                });
            }
            Ok(values)
        })
        .expect("csqlite query_map");

    rows.collect::<Result<Vec<_>, _>>()
        .expect("csqlite collect rows")
}

async fn fsqlite_query_values(conn: &FrankenConnection, sql: &str) -> Vec<Vec<SqlValue>> {
    conn.query(sql)
        .await
        .expect("fsqlite query")
        .into_iter()
        .map(|row| {
            row.values()
                .iter()
                .map(|value| match value {
                    fsqlite_types::SqliteValue::Null => SqlValue::Null,
                    fsqlite_types::SqliteValue::Integer(v) => SqlValue::Integer(*v),
                    fsqlite_types::SqliteValue::Float(v) => SqlValue::Real(*v),
                    fsqlite_types::SqliteValue::Text(v) => SqlValue::Text(v.to_string()),
                    fsqlite_types::SqliteValue::Blob(v) => SqlValue::Blob(v.to_vec()),
                })
                .collect()
        })
        .collect()
}

fn sql_value_to_string(value: &SqlValue) -> String {
    match value {
        SqlValue::Null => "NULL".to_owned(),
        SqlValue::Integer(v) => v.to_string(),
        SqlValue::Real(v) => v.to_string(),
        SqlValue::Text(v) => v.clone(),
        SqlValue::Blob(v) => format!("{v:?}"),
    }
}

fn format_rows(rows: &[Vec<SqlValue>]) -> Vec<Vec<String>> {
    rows.iter()
        .map(|row| row.iter().map(sql_value_to_string).collect())
        .collect()
}

async fn execute_setup(fconn: &FrankenConnection, cconn: &rusqlite::Connection) {
    for sql in SETUP_STATEMENTS {
        cconn.execute(sql, []).expect("csqlite setup");
        fconn.execute(sql).await.expect("fsqlite setup");
    }
}

async fn compare_query_case(
    pass: &'static str,
    case: &QueryCase,
    run_id: &str,
    trace_id: &str,
    scenario_id: &str,
    fconn: &FrankenConnection,
    cconn: &rusqlite::Connection,
) -> QuerySummary {
    let c_rows = csqlite_query_values(cconn, case.sql);
    let f_rows = fsqlite_query_values(fconn, case.sql).await;
    assert_eq!(
        f_rows,
        c_rows,
        "bead_id={BEAD_ID} case=query_mismatch run_id={run_id} trace_id={trace_id} \
scenario_id={scenario_id} pass={pass} query_name={} sql={} fsqlite_rows={:?} csqlite_rows={:?}",
        case.name,
        case.sql,
        format_rows(&f_rows),
        format_rows(&c_rows)
    );

    QuerySummary {
        pass,
        name: case.name,
        row_count: f_rows.len(),
        first_row: f_rows
            .first()
            .map(|row| row.iter().map(sql_value_to_string).collect()),
    }
}

async fn run_aggregate_window_scenario(
    run_id: &str,
    trace_id: &str,
    scenario_id: &str,
) -> ScenarioOutcome {
    let _profile_guard = HotPathProfileGuard::new();
    let started = Instant::now();
    let temp_dir = tempdir().expect("create temp dir");
    let frank_path = temp_dir.path().join("frank.db");
    let sqlite_path = temp_dir.path().join("sqlite.db");

    let frank_path_str = frank_path.to_string_lossy().into_owned();
    let sqlite_path_str = sqlite_path.to_string_lossy().into_owned();

    let fconn = FrankenConnection::open(&frank_path_str)
        .await
        .expect("open fsqlite db");
    let cconn = rusqlite::Connection::open(&sqlite_path_str).expect("open csqlite db");
    execute_setup(&fconn, &cconn).await;

    let mut query_summaries = Vec::with_capacity(QUERY_CASES.len() * 2);
    for case in QUERY_CASES {
        query_summaries.push(
            compare_query_case(
                "initial",
                case,
                run_id,
                trace_id,
                scenario_id,
                &fconn,
                &cconn,
            )
            .await,
        );
    }

    drop(fconn);
    drop(cconn);

    let reopened_fconn = FrankenConnection::open(&frank_path_str)
        .await
        .expect("reopen fsqlite db");
    let reopened_cconn = rusqlite::Connection::open(&sqlite_path_str).expect("reopen csqlite db");
    for case in QUERY_CASES {
        query_summaries.push(
            compare_query_case(
                "reopen",
                case,
                run_id,
                trace_id,
                scenario_id,
                &reopened_fconn,
                &reopened_cconn,
            )
            .await,
        );
    }

    let profile = hot_path_profile_snapshot();
    assert!(
        profile.window_func_partitions_total >= MIN_WINDOW_PARTITIONS_TOTAL,
        "bead_id={BEAD_ID} case=window_partition_metric_too_small run_id={run_id} trace_id={trace_id} \
scenario_id={scenario_id} window_func_partitions_total={} minimum_required={MIN_WINDOW_PARTITIONS_TOTAL}",
        profile.window_func_partitions_total
    );

    ScenarioOutcome {
        elapsed_ms: started.elapsed().as_millis(),
        window_func_partitions_total: profile.window_func_partitions_total,
        query_summaries,
    }
}

fn maybe_write_artifact(
    run_id: &str,
    trace_id: &str,
    scenario_id: &str,
    outcome: &ScenarioOutcome,
    seed: u64,
) {
    let Ok(path) = env::var(ARTIFACT_ENV) else {
        return;
    };

    let artifact_path = PathBuf::from(path);
    if let Some(parent) = artifact_path.parent() {
        fs::create_dir_all(parent).expect("create artifact dir");
    }

    let query_summaries = outcome
        .query_summaries
        .iter()
        .map(|summary| {
            json!({
                "pass": summary.pass,
                "name": summary.name,
                "row_count": summary.row_count,
                "first_row": summary.first_row,
            })
        })
        .collect::<Vec<_>>();

    let artifact = json!({
        "bead_id": BEAD_ID,
        "run_id": run_id,
        "trace_id": trace_id,
        "scenario_id": scenario_id,
        "seed": seed,
        "elapsed_ms": outcome.elapsed_ms,
        "window_func_partitions_total": outcome.window_func_partitions_total,
        "query_summaries": query_summaries,
        "replay_command": REPLAY_COMMAND,
        "log_standard_ref": LOG_STANDARD_REF,
        "overall_status": "pass",
    });

    let payload = serde_json::to_vec_pretty(&artifact).expect("serialize artifact");
    fs::write(&artifact_path, payload).expect("write artifact");

    eprintln!(
        "DEBUG bead_id={BEAD_ID} run_id={run_id} trace_id={trace_id} scenario_id={scenario_id} \
seed={seed} artifact_path={} replay_command={REPLAY_COMMAND}",
        artifact_path.display()
    );
}

#[test]
fn bd_2wt_2_aggregate_window_engine_file_backed_parity() {
    let _serial = aggregate_window_e2e_serializer();
    asupersync::test_utils::run_test(|| async {
        let run_id = "bd-2wt.2-file-backed-parity";
        let trace_id = "2204112001";
        let scenario_id = "AGG-WINDOW-FILE-BACKED-PARITY";

        let outcome = run_aggregate_window_scenario(run_id, trace_id, scenario_id).await;

        eprintln!(
            "INFO bead_id={BEAD_ID} run_id={run_id} trace_id={trace_id} scenario_id={scenario_id} \
seed={DEFAULT_SEED} elapsed_ms={} window_func_partitions_total={} query_count={} log_standard_ref={LOG_STANDARD_REF}",
            outcome.elapsed_ms,
            outcome.window_func_partitions_total,
            outcome.query_summaries.len()
        );
    });
}

#[test]
fn bd_2wt_2_aggregate_window_engine_e2e_replay_emits_artifact() {
    let _serial = aggregate_window_e2e_serializer();
    asupersync::test_utils::run_test(|| async {
        let seed = env::var("SEED")
            .ok()
            .and_then(|raw| raw.parse::<u64>().ok())
            .unwrap_or(DEFAULT_SEED);
        let trace_id = env::var("TRACE_ID").unwrap_or_else(|_| seed.to_string());
        let run_id = env::var("RUN_ID").unwrap_or_else(|_| format!("{BEAD_ID}-seed-{seed}"));
        let scenario_id =
            env::var("SCENARIO_ID").unwrap_or_else(|_| "AGG-WINDOW-E2E-REPLAY".to_owned());

        let outcome = run_aggregate_window_scenario(&run_id, &trace_id, &scenario_id).await;
        maybe_write_artifact(&run_id, &trace_id, &scenario_id, &outcome, seed);

        eprintln!(
            "INFO bead_id={BEAD_ID} run_id={run_id} trace_id={trace_id} scenario_id={scenario_id} \
seed={seed} elapsed_ms={} window_func_partitions_total={} query_count={} log_standard_ref={LOG_STANDARD_REF}",
            outcome.elapsed_ms,
            outcome.window_func_partitions_total,
            outcome.query_summaries.len()
        );
    });
}
