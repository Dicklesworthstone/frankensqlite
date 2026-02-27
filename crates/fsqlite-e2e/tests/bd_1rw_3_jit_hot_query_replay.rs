//! Deterministic JIT hot-query replay checks for `bd-1rw.3`.

use std::{
    collections::HashMap,
    env, fs,
    path::{Path, PathBuf},
};

use fsqlite_types::value::SqliteValue;
use serde_json::json;

const BEAD_ID: &str = "bd-1rw.3";
const LOG_STANDARD_REF: &str = "AGENTS.md#cross-cutting-quality-contract";
const DEFAULT_SEED: u64 = 1_003_202_603;
const REPLAY_COMMAND: &str = "cargo test -p fsqlite-e2e --test bd_1rw_3_jit_hot_query_replay -- --nocapture --test-threads=1";

fn scalar_i64(conn: &fsqlite::Connection, sql: &str) -> i64 {
    let row = conn.query_row(sql).expect("query_row");
    match row.get(0) {
        Some(SqliteValue::Integer(value)) => *value,
        other => panic!("expected integer scalar from '{sql}', got {other:?}"),
    }
}

fn jit_stats(conn: &fsqlite::Connection) -> HashMap<String, i64> {
    let rows = conn
        .query("PRAGMA fsqlite.jit_stats;")
        .expect("jit_stats pragma");
    let mut map = HashMap::new();

    for row in rows {
        let key = match row.get(0) {
            Some(SqliteValue::Text(value)) => value.clone(),
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

fn configure_jit(conn: &fsqlite::Connection, hot_threshold: i64, cache_capacity: i64) {
    conn.execute("PRAGMA fsqlite.jit_reset;")
        .expect("reset jit metrics");
    conn.execute("PRAGMA fsqlite.jit_enable = ON;")
        .expect("enable jit");
    conn.execute(&format!(
        "PRAGMA fsqlite.jit_hot_threshold = {hot_threshold};"
    ))
    .expect("set jit hot threshold");
    conn.execute(&format!(
        "PRAGMA fsqlite.jit_cache_capacity = {cache_capacity};"
    ))
    .expect("set jit cache capacity");
}

fn run_supported_hot_loop(conn: &fsqlite::Connection, iterations: usize) -> i64 {
    let mut last = 0_i64;
    for _ in 0..iterations {
        last = scalar_i64(conn, "SELECT 40 + 2;");
        assert_eq!(last, 42, "hot-loop result drifted");
    }
    last
}

fn run_unsupported_hot_loop(conn: &fsqlite::Connection, iterations: usize) -> i64 {
    conn.execute("DROP TABLE IF EXISTS jit_fail_src;")
        .expect("drop temp table");
    conn.execute("CREATE TABLE jit_fail_src(v INTEGER NOT NULL);")
        .expect("create temp table");

    for _ in 0..iterations {
        conn.execute("INSERT INTO jit_fail_src(v) VALUES (1);")
            .expect("insert row");
    }

    let row_count = scalar_i64(conn, "SELECT COUNT(*) FROM jit_fail_src;");
    assert_eq!(
        row_count,
        i64::try_from(iterations).expect("iterations fit in i64"),
        "fallback-path row count drifted"
    );
    row_count
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
    let run_id = "bd-1rw.3-hot-query";
    let trace_id = 1_003_202_631_u64;
    let scenario_id = "JIT-HOT-QUERY";

    let conn = fsqlite::Connection::open(":memory:").expect("open connection");
    configure_jit(&conn, 2, 16);

    let result = run_supported_hot_loop(&conn, 16);
    assert_eq!(result, 42);

    let stats = jit_stats(&conn);
    require_stat_at_least(&stats, "fsqlite_jit_compilations_total", 1);
    require_stat_at_least(&stats, "fsqlite_jit_cache_hits_total", 1);
    require_stat_at_least(&stats, "fsqlite_jit_cache_hit_ratio", 1);

    eprintln!(
        "INFO bead_id={BEAD_ID} run_id={run_id} trace_id={trace_id} scenario_id={scenario_id} seed={DEFAULT_SEED} result={result} compilations={} cache_hits={} log_standard_ref={LOG_STANDARD_REF}",
        stats
            .get("fsqlite_jit_compilations_total")
            .copied()
            .unwrap_or_default(),
        stats
            .get("fsqlite_jit_cache_hits_total")
            .copied()
            .unwrap_or_default(),
    );
}

#[test]
fn bd_1rw_3_jit_compile_failure_falls_back_cleanly() {
    let run_id = "bd-1rw.3-fallback";
    let trace_id = 1_003_202_632_u64;
    let scenario_id = "JIT-FALLBACK-UNSUPPORTED";

    let conn = fsqlite::Connection::open(":memory:").expect("open connection");
    configure_jit(&conn, 1, 16);

    let result = run_unsupported_hot_loop(&conn, 8);
    assert_eq!(result, 8);

    let stats = jit_stats(&conn);
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
}

#[test]
fn bd_1rw_3_jit_e2e_replay_emits_artifact() {
    let seed = env::var("SEED")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .unwrap_or(DEFAULT_SEED);
    let trace_id = env::var("TRACE_ID").unwrap_or_else(|_| seed.to_string());
    let run_id = env::var("RUN_ID").unwrap_or_else(|_| format!("{BEAD_ID}-seed-{seed}"));
    let scenario_id =
        env::var("SCENARIO_ID").unwrap_or_else(|_| "JIT-HOT-QUERY-E2E-REPLAY".to_owned());

    let conn = fsqlite::Connection::open(":memory:").expect("open connection");
    configure_jit(&conn, 2, 16);

    let supported_last = run_supported_hot_loop(&conn, 16);
    let unsupported_last = run_unsupported_hot_loop(&conn, 8);
    let stats = jit_stats(&conn);

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
        "INFO bead_id={BEAD_ID} run_id={run_id} trace_id={trace_id} scenario_id={scenario_id} seed={seed} supported_last={supported_last} unsupported_last={unsupported_last} compilations={} compile_failures={} cache_hits={} log_standard_ref={LOG_STANDARD_REF}",
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
}
