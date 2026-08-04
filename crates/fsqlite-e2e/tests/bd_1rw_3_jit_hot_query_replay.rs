//! Deterministic JIT hot-query replay checks for `bd-1rw.3`.
#![recursion_limit = "512"]

use std::{
    collections::HashMap,
    env, fs,
    path::{Path, PathBuf},
    sync::Mutex,
};

use fsqlite_types::value::SqliteValue;
use rusqlite::types::{Value as RusqliteValue, ValueRef};
use serde_json::json;
use tempfile::tempdir;

const BEAD_ID: &str = "bd-1rw.3";
const LOG_STANDARD_REF: &str = "AGENTS.md#cross-cutting-quality-contract";
const DEFAULT_SEED: u64 = 1_003_202_603;
const REPLAY_COMMAND: &str = "cargo test -p fsqlite-e2e --test bd_1rw_3_jit_hot_query_replay -- --nocapture --test-threads=1";

static JIT_TEST_LOCK: Mutex<()> = Mutex::new(());

async fn scalar_i64(conn: &fsqlite::Connection, sql: &str) -> i64 {
    let row = conn.query_row(sql).await.expect("query_row");
    match row.get(0) {
        Some(SqliteValue::Integer(value)) => *value,
        other => panic!("expected integer scalar from '{sql}', got {other:?}"),
    }
}

async fn jit_stats(conn: &fsqlite::Connection) -> HashMap<String, i64> {
    let rows = conn
        .query("PRAGMA fsqlite.jit_stats;")
        .await
        .expect("jit_stats pragma");
    let mut map = HashMap::new();

    for row in rows {
        let key = match row.get(0) {
            Some(SqliteValue::Text(value)) => value.to_string(),
            other => panic!("expected TEXT key in jit_stats, got {other:?}"),
        };
        let value = match row.get(1) {
            Some(SqliteValue::Integer(value)) => *value,
            other => panic!("expected INTEGER value in jit_stats, got {other:?}"),
        };
        map.insert(key, value);
    }

    map
}

fn require_stat_at_least(stats: &HashMap<String, i64>, key: &str, min: i64) -> i64 {
    let value = *stats
        .get(key)
        .unwrap_or_else(|| panic!("missing jit stat '{key}'"));
    assert!(
        value >= min,
        "expected jit stat '{key}' >= {min}, got {value}"
    );
    value
}

fn merge_stats<I>(stats_sets: I) -> HashMap<String, i64>
where
    I: IntoIterator<Item = HashMap<String, i64>>,
{
    let mut merged = HashMap::new();
    for stats in stats_sets {
        for (key, value) in stats {
            *merged.entry(key).or_insert(0) += value;
        }
    }
    merged
}

async fn configure_jit(conn: &fsqlite::Connection, hot_threshold: i64, cache_capacity: i64) {
    conn.execute("PRAGMA fsqlite.jit_reset;")
        .await
        .expect("reset jit metrics");
    conn.execute("PRAGMA fsqlite.jit_enable = ON;")
        .await
        .expect("enable jit");
    conn.execute(&format!(
        "PRAGMA fsqlite.jit_hot_threshold = {hot_threshold};"
    ))
    .await
    .expect("set jit hot threshold");
    conn.execute(&format!(
        "PRAGMA fsqlite.jit_cache_capacity = {cache_capacity};"
    ))
    .await
    .expect("set jit cache capacity");
}

async fn disable_jit(conn: &fsqlite::Connection) {
    conn.execute("PRAGMA fsqlite.jit_enable = OFF;")
        .await
        .expect("disable jit");
}

fn sqlite_value_to_rusqlite(value: &SqliteValue) -> RusqliteValue {
    match value {
        SqliteValue::Null => RusqliteValue::Null,
        SqliteValue::Integer(value) => RusqliteValue::Integer(*value),
        SqliteValue::Float(value) => RusqliteValue::Real(*value),
        SqliteValue::Text(value) => RusqliteValue::Text(value.to_string()),
        SqliteValue::Blob(value) => RusqliteValue::Blob(value.to_vec()),
    }
}

fn rusqlite_value_to_sqlite(value: ValueRef<'_>) -> SqliteValue {
    match value {
        ValueRef::Null => SqliteValue::Null,
        ValueRef::Integer(value) => SqliteValue::Integer(value),
        ValueRef::Real(value) => SqliteValue::Float(value),
        ValueRef::Text(value) => {
            SqliteValue::Text(String::from_utf8(value.to_vec()).expect("utf8").into())
        }
        ValueRef::Blob(value) => SqliteValue::Blob(value.to_vec().into()),
    }
}

async fn collect_rows(conn: &fsqlite::Connection, sql: &str) -> Vec<Vec<SqliteValue>> {
    conn.query(sql)
        .await
        .expect("query rows")
        .into_iter()
        .map(|row| row.values().to_vec())
        .collect()
}

fn collect_rows_rusqlite(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<SqliteValue>> {
    let mut stmt = conn.prepare(sql).expect("prepare query");
    let column_count = stmt.column_count();
    let rows = stmt
        .query_map([], |row| {
            let mut values = Vec::with_capacity(column_count);
            for idx in 0..column_count {
                let value = row.get_ref(idx)?;
                values.push(rusqlite_value_to_sqlite(value));
            }
            Ok(values)
        })
        .expect("run query");
    rows.map(|row| row.expect("row decode")).collect()
}

struct SupportedHotLoopOutcome {
    row_count: i64,
    stats: HashMap<String, i64>,
}

async fn run_supported_hot_loop(iterations: usize) -> SupportedHotLoopOutcome {
    let tempdir = tempdir().expect("tempdir");
    let jit_path = tempdir.path().join("jit.db");
    let interpreter_path = tempdir.path().join("interpreter.db");
    let sqlite_path = tempdir.path().join("sqlite.db");

    let jit_path_str = jit_path.to_str().expect("jit path utf8");
    let interpreter_path_str = interpreter_path.to_str().expect("interpreter path utf8");
    let sqlite_path_str = sqlite_path.to_str().expect("sqlite path utf8");

    let interpreter_conn = fsqlite::Connection::open(interpreter_path_str)
        .await
        .expect("open interpreter db");
    let sqlite_conn = rusqlite::Connection::open(sqlite_path_str).expect("open sqlite db");

    disable_jit(&interpreter_conn).await;

    interpreter_conn
        .execute("CREATE TABLE jit_hot_query(v INTEGER, label TEXT);")
        .await
        .expect("create interpreter table");
    sqlite_conn
        .execute("CREATE TABLE jit_hot_query(v INTEGER, label TEXT);", [])
        .expect("create sqlite table");

    let interpreter_stmt = interpreter_conn
        .prepare("INSERT INTO jit_hot_query(v) VALUES (?1);")
        .await
        .expect("prepare interpreter insert");
    let mut sqlite_stmt = sqlite_conn
        .prepare("INSERT INTO jit_hot_query(v) VALUES (?1);")
        .expect("prepare sqlite insert");

    for idx in 0..iterations {
        let value = i64::try_from(idx).expect("iterations fit i64");
        let params = [SqliteValue::Integer(value)];
        assert_eq!(
            interpreter_stmt
                .execute_with_params(&params)
                .await
                .expect("interpreter insert"),
            1,
            "interpreter insert row count drifted",
        );
        let sqlite_params = params
            .iter()
            .map(sqlite_value_to_rusqlite)
            .collect::<Vec<_>>();
        assert_eq!(
            sqlite_stmt.execute(rusqlite::params_from_iter(sqlite_params)),
            Ok(1),
            "sqlite insert row count drifted",
        );
    }
    drop(interpreter_stmt);
    drop(sqlite_stmt);

    let interpreter_rows = collect_rows(
        &interpreter_conn,
        "SELECT v, label FROM jit_hot_query ORDER BY rowid;",
    )
    .await;
    let sqlite_rows = collect_rows_rusqlite(
        &sqlite_conn,
        "SELECT v, label FROM jit_hot_query ORDER BY rowid;",
    );
    assert_eq!(
        interpreter_rows, sqlite_rows,
        "interpreter INSERT results must match SQLite reference results",
    );

    let jit_conn = fsqlite::Connection::open(jit_path_str)
        .await
        .expect("open jit db");
    configure_jit(&jit_conn, 2, 16).await;
    jit_conn
        .execute("CREATE TABLE jit_hot_query(v INTEGER, label TEXT);")
        .await
        .expect("create jit table");
    let jit_stmt = jit_conn
        .prepare("INSERT INTO jit_hot_query(v) VALUES (?1);")
        .await
        .expect("prepare jit insert");
    for idx in 0..iterations {
        let value = i64::try_from(idx).expect("iterations fit i64");
        let params = [SqliteValue::Integer(value)];
        assert_eq!(
            jit_stmt
                .execute_with_params(&params)
                .await
                .expect("jit insert"),
            1,
            "jit insert row count drifted",
        );
    }
    drop(jit_stmt);

    let jit_rows = collect_rows(
        &jit_conn,
        "SELECT v, label FROM jit_hot_query ORDER BY rowid;",
    )
    .await;
    assert_eq!(
        jit_rows, interpreter_rows,
        "JIT INSERT results must match interpreter results",
    );
    assert_eq!(
        jit_rows, sqlite_rows,
        "JIT INSERT results must match SQLite reference results",
    );

    let row_count = scalar_i64(&jit_conn, "SELECT COUNT(*) FROM jit_hot_query;").await;
    assert_eq!(
        row_count,
        i64::try_from(iterations).expect("iterations fit i64"),
        "supported hot-loop row count drifted",
    );
    let stats = jit_stats(&jit_conn).await;

    SupportedHotLoopOutcome { row_count, stats }
}

async fn run_unsupported_hot_loop(conn: &fsqlite::Connection, iterations: usize) -> i64 {
    conn.execute("DROP TABLE IF EXISTS jit_fail_src;")
        .await
        .expect("drop temp table");
    conn.execute("CREATE TABLE jit_fail_src(v INTEGER NOT NULL);")
        .await
        .expect("create temp table");
    conn.execute("INSERT INTO jit_fail_src(v) VALUES (1), (2), (3);")
        .await
        .expect("seed rows");

    let stmt = conn
        .prepare("SELECT v FROM jit_fail_src WHERE v >= ?1 ORDER BY v;")
        .await
        .expect("prepare unsupported query");
    let mut total_rows = 0_i64;
    for _ in 0..iterations {
        let rows = stmt
            .query_with_params(&[SqliteValue::Integer(2)])
            .await
            .expect("run unsupported query");
        let values = rows
            .into_iter()
            .map(|row| row.get(0).cloned().expect("single-column row"))
            .collect::<Vec<_>>();
        assert_eq!(
            values,
            vec![SqliteValue::Integer(2), SqliteValue::Integer(3)],
            "fallback-path rows drifted",
        );
        total_rows += i64::try_from(values.len()).expect("row count fits i64");
    }

    assert_eq!(
        total_rows,
        i64::try_from(iterations).expect("iterations fit in i64") * 2,
        "fallback-path total rows drifted",
    );
    total_rows
}

struct ReplayArtifact<'a> {
    run_id: &'a str,
    trace_id: &'a str,
    scenario_id: &'a str,
    seed: u64,
    stats: &'a HashMap<String, i64>,
    supported_last: i64,
    unsupported_last: i64,
}

fn write_artifact(artifact_path: &Path, replay: &ReplayArtifact<'_>) {
    if let Some(parent) = artifact_path.parent() {
        fs::create_dir_all(parent).expect("create artifact dir");
    }

    let artifact = json!({
        "bead_id": BEAD_ID,
        "run_id": replay.run_id,
        "trace_id": replay.trace_id,
        "scenario_id": replay.scenario_id,
        "seed": replay.seed,
        "log_standard_ref": LOG_STANDARD_REF,
        "overall_status": "pass",
        "replay_command": REPLAY_COMMAND,
        "checks": {
            "supported_hot_loop_result": replay.supported_last,
            "unsupported_hot_loop_result": replay.unsupported_last,
            "jit_stats": replay.stats,
        }
    });

    let payload = serde_json::to_vec_pretty(&artifact).expect("serialize artifact");
    fs::write(artifact_path, payload).expect("write artifact");
}

#[test]
fn bd_1rw_3_jit_hot_query_matches_interpreter_semantics() {
    let _guard = JIT_TEST_LOCK.lock().unwrap();

    asupersync::test_utils::run_test(|| async {
        let run_id = "bd-1rw.3-hot-query";
        let trace_id = 1_003_202_631_u64;
        let scenario_id = "JIT-HOT-QUERY";

        let SupportedHotLoopOutcome {
            row_count: result,
            stats,
        } = run_supported_hot_loop(16).await;
        assert_eq!(result, 16);

        require_stat_at_least(&stats, "fsqlite_jit_compilations_total", 1);
        require_stat_at_least(&stats, "fsqlite_jit_cache_hits_total", 1);
        require_stat_at_least(&stats, "fsqlite_jit_cache_hit_ratio", 1);

        eprintln!(
            "INFO bead_id={BEAD_ID} run_id={run_id} trace_id={trace_id} scenario_id={scenario_id} seed={DEFAULT_SEED} result={} compilations={} cache_hits={} log_standard_ref={LOG_STANDARD_REF}",
            result,
            stats
                .get("fsqlite_jit_compilations_total")
                .copied()
                .unwrap_or_default(),
            stats
                .get("fsqlite_jit_cache_hits_total")
                .copied()
                .unwrap_or_default(),
        );
    });
}

#[test]
fn bd_1rw_3_jit_compile_failure_falls_back_cleanly() {
    let _guard = JIT_TEST_LOCK.lock().unwrap();

    asupersync::test_utils::run_test(|| async {
        let run_id = "bd-1rw.3-fallback";
        let trace_id = 1_003_202_632_u64;
        let scenario_id = "JIT-FALLBACK-UNSUPPORTED";

        let conn = fsqlite::Connection::open(":memory:")
            .await
            .expect("open connection");
        configure_jit(&conn, 1, 16).await;

        let result = run_unsupported_hot_loop(&conn, 8).await;
        assert_eq!(result, 16);

        let stats = jit_stats(&conn).await;
        require_stat_at_least(&stats, "fsqlite_jit_compile_failures_total", 1);
        require_stat_at_least(&stats, "fsqlite_jit_triggers_total", 1);

        eprintln!(
            "INFO bead_id={BEAD_ID} run_id={run_id} trace_id={trace_id} scenario_id={scenario_id} seed={DEFAULT_SEED} result={result} compile_failures={} triggers={} log_standard_ref={LOG_STANDARD_REF}",
            stats
                .get("fsqlite_jit_compile_failures_total")
                .copied()
                .unwrap_or_default(),
            stats
                .get("fsqlite_jit_triggers_total")
                .copied()
                .unwrap_or_default(),
        );
    });
}

#[test]
fn bd_1rw_3_jit_e2e_replay_emits_artifact() {
    let _guard = JIT_TEST_LOCK.lock().unwrap();

    asupersync::test_utils::run_test(|| async {
        let seed = env::var("SEED")
            .ok()
            .and_then(|raw| raw.parse::<u64>().ok())
            .unwrap_or(DEFAULT_SEED);
        let trace_id = env::var("TRACE_ID").unwrap_or_else(|_| seed.to_string());
        let run_id = env::var("RUN_ID").unwrap_or_else(|_| format!("{BEAD_ID}-seed-{seed}"));
        let scenario_id =
            env::var("SCENARIO_ID").unwrap_or_else(|_| "JIT-HOT-QUERY-E2E-REPLAY".to_owned());

        let conn = fsqlite::Connection::open(":memory:")
            .await
            .expect("open connection");
        configure_jit(&conn, 1, 16).await;
        let SupportedHotLoopOutcome {
            row_count: supported_last,
            stats: supported_stats,
        } = run_supported_hot_loop(16).await;
        let unsupported_last = run_unsupported_hot_loop(&conn, 8).await;
        let stats = merge_stats([supported_stats, jit_stats(&conn).await]);

        require_stat_at_least(&stats, "fsqlite_jit_compilations_total", 1);
        require_stat_at_least(&stats, "fsqlite_jit_cache_hits_total", 1);
        require_stat_at_least(&stats, "fsqlite_jit_compile_failures_total", 1);

        if let Ok(path) = env::var("FSQLITE_JIT_E2E_ARTIFACT") {
            let artifact_path = PathBuf::from(path);
            let replay = ReplayArtifact {
                run_id: &run_id,
                trace_id: &trace_id,
                scenario_id: &scenario_id,
                seed,
                stats: &stats,
                supported_last,
                unsupported_last,
            };
            write_artifact(&artifact_path, &replay);
            eprintln!(
                "DEBUG bead_id={BEAD_ID} run_id={run_id} trace_id={trace_id} scenario_id={scenario_id} seed={seed} artifact_path={} replay_command={REPLAY_COMMAND}",
                artifact_path.display()
            );
        }

        eprintln!(
            "INFO bead_id={BEAD_ID} run_id={run_id} trace_id={trace_id} scenario_id={scenario_id} seed={seed} supported_last={} unsupported_last={unsupported_last} compilations={} compile_failures={} cache_hits={} log_standard_ref={LOG_STANDARD_REF}",
            supported_last,
            stats
                .get("fsqlite_jit_compilations_total")
                .copied()
                .unwrap_or_default(),
            stats
                .get("fsqlite_jit_compile_failures_total")
                .copied()
                .unwrap_or_default(),
            stats
                .get("fsqlite_jit_cache_hits_total")
                .copied()
                .unwrap_or_default(),
        );
    });
}
