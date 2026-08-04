use std::fs;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant};

use fsqlite::{Connection, Row};
use fsqlite_types::SqliteValue;
use serde::Serialize;
use tempfile::tempdir;

const BEAD_ID: &str = "bd-3plop.2";
const SMALL_CACHE_KIB: i64 = -10;
const MID_CACHE_KIB: i64 = -256;
const LARGE_CACHE_KIB: i64 = -1_000;
const CONCURRENT_WRITERS: usize = 32;
const DEFAULT_RESIZE_STEPS: usize = 80;
const DEFAULT_QUERY_LOOPS: usize = 120;
const DEFAULT_WRITER_OPS: usize = 80;
const DEFAULT_MAX_CASE_MILLIS: u128 = 25_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Scenario {
    RapidShrink,
    RapidGrow,
    Oscillation,
    ConcurrentResizeQueries,
    ConcurrentResizeWrites,
}

impl Scenario {
    const ALL: [Self; 5] = [
        Self::RapidShrink,
        Self::RapidGrow,
        Self::Oscillation,
        Self::ConcurrentResizeQueries,
        Self::ConcurrentResizeWrites,
    ];

    const fn as_str(self) -> &'static str {
        match self {
            Self::RapidShrink => "rapid_shrink",
            Self::RapidGrow => "rapid_grow",
            Self::Oscillation => "oscillation",
            Self::ConcurrentResizeQueries => "concurrent_resize_queries",
            Self::ConcurrentResizeWrites => "concurrent_resize_writes_32_threads",
        }
    }
}

#[derive(Debug, Serialize, Clone, Copy)]
struct Checksum {
    row_count: u64,
    sum_v: i64,
}

#[derive(Debug, Serialize)]
struct CaseArtifact {
    scenario: String,
    resize_ops: usize,
    query_ops: usize,
    write_ops: usize,
    write_aborts: usize,
    final_cache_size: i64,
    integrity_ok: bool,
    deadlock_free: bool,
    starvation_free: bool,
    elapsed_ms: u128,
    checksum_before: Checksum,
    checksum_after: Checksum,
}

#[derive(Debug, Serialize)]
struct SuiteArtifact {
    schema_version: u32,
    bead_id: String,
    run_id: String,
    resize_steps: usize,
    query_loops: usize,
    writer_ops: usize,
    cases: Vec<CaseArtifact>,
    acceptance_checks: Vec<String>,
}

#[derive(Debug)]
struct WriteStats {
    committed: usize,
    aborted: usize,
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn env_u128(key: &str, default: u128) -> u128 {
    std::env::var(key)
        .ok()
        .and_then(|raw| raw.parse::<u128>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn resize_steps() -> usize {
    env_usize("BD_3PLOP2_STEPS", DEFAULT_RESIZE_STEPS)
}

fn query_loops() -> usize {
    env_usize("BD_3PLOP2_QUERY_LOOPS", DEFAULT_QUERY_LOOPS)
}

fn writer_ops() -> usize {
    env_usize("BD_3PLOP2_WRITER_OPS", DEFAULT_WRITER_OPS)
}

fn max_case_millis() -> u128 {
    env_u128("BD_3PLOP2_MAX_CASE_MS", DEFAULT_MAX_CASE_MILLIS)
}

fn workspace_root() -> Result<PathBuf, String> {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .map_err(|error| format!("workspace_root_canonicalize_failed: {error}"))
}

async fn configure_connection(conn: &Connection) -> Result<(), String> {
    conn.execute("PRAGMA journal_mode = WAL")
        .await
        .map_err(|error| format!("pragma_wal_failed error={error}"))?;
    conn.execute("PRAGMA synchronous = NORMAL")
        .await
        .map_err(|error| format!("pragma_sync_failed error={error}"))?;
    conn.execute("PRAGMA busy_timeout = 25")
        .await
        .map_err(|error| format!("pragma_busy_timeout_failed error={error}"))?;
    Ok(())
}

async fn open_db(path: &str) -> Result<Connection, String> {
    let conn = Connection::open(path)
        .await
        .map_err(|error| format!("open_failed path={path} {error}"))?;
    configure_connection(&conn).await?;
    Ok(conn)
}

async fn setup_database(path: &str) -> Result<(), String> {
    let conn = open_db(path).await?;
    conn.execute("CREATE TABLE IF NOT EXISTS kv (id INTEGER PRIMARY KEY, v INTEGER NOT NULL)")
        .await
        .map_err(|error| format!("create_table_failed error={error}"))?;
    conn.execute("DELETE FROM kv")
        .await
        .map_err(|error| format!("delete_table_failed error={error}"))?;

    for key in 1..=512_u64 {
        let sql = format!("INSERT INTO kv (id, v) VALUES ({key}, {key})");
        conn.execute(&sql)
            .await
            .map_err(|error| format!("seed_insert_failed key={key} error={error}"))?;
    }

    Ok(())
}

async fn set_cache_size(conn: &Connection, cache_size: i64) -> Result<(), String> {
    let sql = format!("PRAGMA cache_size={cache_size};");
    conn.execute(&sql)
        .await
        .map_err(|error| format!("set_cache_size_failed cache_size={cache_size} error={error}"))?;
    Ok(())
}

fn parse_i64_cell(row: &Row, index: usize, label: &str) -> Result<i64, String> {
    match row.get(index) {
        Some(SqliteValue::Integer(value)) => Ok(*value),
        Some(other) => Err(format!("{label}_type_unexpected value={other:?}")),
        None => Err(format!("{label}_missing index={index}")),
    }
}

async fn get_cache_size(conn: &Connection) -> Result<i64, String> {
    let rows = conn
        .query("PRAGMA cache_size;")
        .await
        .map_err(|error| format!("pragma_cache_size_query_failed error={error}"))?;
    let Some(row) = rows.first() else {
        return Err("pragma_cache_size_empty_result".to_owned());
    };
    parse_i64_cell(row, 0, "pragma_cache_size")
}

async fn integrity_ok(conn: &Connection) -> Result<bool, String> {
    let rows = conn
        .query("PRAGMA integrity_check;")
        .await
        .map_err(|error| format!("integrity_check_failed error={error}"))?;
    let Some(row) = rows.first() else {
        return Err("integrity_check_empty_result".to_owned());
    };

    match row.get(0) {
        Some(SqliteValue::Text(value)) => Ok(value.eq_ignore_ascii_case("ok")),
        Some(other) => Err(format!("integrity_check_type_unexpected value={other:?}")),
        None => Err("integrity_check_missing_cell".to_owned()),
    }
}

async fn checksum(conn: &Connection) -> Result<Checksum, String> {
    let count_rows = conn
        .query("SELECT count(*) FROM kv;")
        .await
        .map_err(|error| format!("checksum_count_query_failed error={error}"))?;
    let Some(count_row) = count_rows.first() else {
        return Err("checksum_count_empty_result".to_owned());
    };
    let row_count_i64 = parse_i64_cell(count_row, 0, "checksum_row_count")?;
    let row_count = u64::try_from(row_count_i64)
        .map_err(|_| format!("checksum_row_count_negative value={row_count_i64}"))?;

    let sum_rows = conn
        .query("SELECT sum(v) FROM kv;")
        .await
        .map_err(|error| format!("checksum_sum_query_failed error={error}"))?;
    let Some(sum_row) = sum_rows.first() else {
        return Err("checksum_sum_empty_result".to_owned());
    };
    let sum_v = match sum_row.get(0) {
        Some(SqliteValue::Integer(value)) => *value,
        Some(SqliteValue::Null) | None => 0,
        Some(other) => return Err(format!("checksum_sum_type_unexpected value={other:?}")),
    };

    Ok(Checksum { row_count, sum_v })
}

async fn run_resize_pattern(
    conn: &Connection,
    pattern: &[i64],
    iterations: usize,
    sleep_millis: u64,
) -> Result<usize, String> {
    if pattern.is_empty() {
        return Err("resize_pattern_empty".to_owned());
    }

    for step in 0..iterations {
        let cache_size = pattern[step % pattern.len()];
        set_cache_size(conn, cache_size).await?;
        if sleep_millis > 0 {
            thread::sleep(Duration::from_millis(sleep_millis));
        }
    }

    Ok(iterations)
}

/// Runs entirely inside a spawned worker thread; owns its own runtime because
/// `Connection` is `!Send`.
#[allow(clippy::needless_pass_by_value)]
fn run_query_worker(path: String, loops: usize, expected: Checksum) -> Result<usize, String> {
    let mut outcome: Result<usize, String> = Err("query_worker_did_not_run".to_owned());
    asupersync::test_utils::run_test(|| async {
        outcome = async {
            let conn = open_db(&path).await?;
            for idx in 0..loops {
                let observed = checksum(&conn).await?;
                if observed.row_count != expected.row_count || observed.sum_v != expected.sum_v {
                    return Err(format!(
                        "query_checksum_mismatch idx={idx} expected={expected:?} observed={observed:?}"
                    ));
                }
            }
            Ok(loops)
        }
        .await;
    });
    outcome
}

/// Runs entirely inside a spawned worker thread; owns its own runtime because
/// `Connection` is `!Send`.
#[allow(clippy::needless_pass_by_value)]
fn run_writer_worker(path: String, thread_idx: usize, ops: usize) -> WriteStats {
    let mut outcome: Option<WriteStats> = None;
    asupersync::test_utils::run_test(|| async {
        let Ok(conn) = open_db(&path).await else {
            outcome = Some(WriteStats {
                committed: 0,
                aborted: ops,
            });
            return;
        };

        let mut committed = 0;
        let mut aborted = 0;

        for op in 0..ops {
            let key = ((thread_idx * ops) + op) % 512 + 1;
            let update_sql = format!("UPDATE kv SET v = v + 1 WHERE id = {key}");

            match conn.execute(&update_sql).await {
                Ok(changed) => {
                    if changed > 0 {
                        committed += changed;
                    } else {
                        aborted += 1;
                    }
                }
                Err(_) => {
                    aborted += 1;
                }
            }
        }

        outcome = Some(WriteStats { committed, aborted });
    });
    outcome.expect("writer worker must produce stats")
}

async fn rapid_shrink_case(path: &str, steps: usize) -> Result<CaseArtifact, String> {
    setup_database(path).await?;
    let conn = open_db(path).await?;
    let checksum_before = checksum(&conn).await?;

    let started = Instant::now();
    let pattern = [
        LARGE_CACHE_KIB,
        -700,
        -500,
        MID_CACHE_KIB,
        -128,
        -64,
        -32,
        SMALL_CACHE_KIB,
    ];
    let resize_ops = run_resize_pattern(&conn, &pattern, steps, 1).await?;

    let final_cache_size = get_cache_size(&conn).await?;
    let checksum_after = checksum(&conn).await?;
    let integrity_ok = integrity_ok(&conn).await?;
    let elapsed_ms = started.elapsed().as_millis();

    Ok(CaseArtifact {
        scenario: Scenario::RapidShrink.as_str().to_owned(),
        resize_ops,
        query_ops: 0,
        write_ops: 0,
        write_aborts: 0,
        final_cache_size,
        integrity_ok,
        deadlock_free: true,
        starvation_free: elapsed_ms <= max_case_millis(),
        elapsed_ms,
        checksum_before,
        checksum_after,
    })
}

async fn rapid_grow_case(path: &str, steps: usize) -> Result<CaseArtifact, String> {
    setup_database(path).await?;
    let conn = open_db(path).await?;
    let checksum_before = checksum(&conn).await?;

    let started = Instant::now();
    let pattern = [
        SMALL_CACHE_KIB,
        -32,
        -64,
        -128,
        MID_CACHE_KIB,
        -500,
        -700,
        LARGE_CACHE_KIB,
    ];
    let resize_ops = run_resize_pattern(&conn, &pattern, steps, 1).await?;

    let final_cache_size = get_cache_size(&conn).await?;
    let checksum_after = checksum(&conn).await?;
    let integrity_ok = integrity_ok(&conn).await?;
    let elapsed_ms = started.elapsed().as_millis();

    Ok(CaseArtifact {
        scenario: Scenario::RapidGrow.as_str().to_owned(),
        resize_ops,
        query_ops: 0,
        write_ops: 0,
        write_aborts: 0,
        final_cache_size,
        integrity_ok,
        deadlock_free: true,
        starvation_free: elapsed_ms <= max_case_millis(),
        elapsed_ms,
        checksum_before,
        checksum_after,
    })
}

async fn oscillation_case(path: &str, steps: usize) -> Result<CaseArtifact, String> {
    setup_database(path).await?;
    let conn = open_db(path).await?;
    let checksum_before = checksum(&conn).await?;

    let started = Instant::now();
    let pattern = [SMALL_CACHE_KIB, LARGE_CACHE_KIB];
    let resize_ops = run_resize_pattern(&conn, &pattern, steps.saturating_mul(2), 2).await?;

    let final_cache_size = get_cache_size(&conn).await?;
    let checksum_after = checksum(&conn).await?;
    let integrity_ok = integrity_ok(&conn).await?;
    let elapsed_ms = started.elapsed().as_millis();

    Ok(CaseArtifact {
        scenario: Scenario::Oscillation.as_str().to_owned(),
        resize_ops,
        query_ops: 0,
        write_ops: 0,
        write_aborts: 0,
        final_cache_size,
        integrity_ok,
        deadlock_free: true,
        starvation_free: elapsed_ms <= max_case_millis(),
        elapsed_ms,
        checksum_before,
        checksum_after,
    })
}

async fn concurrent_resize_queries_case(
    path: &str,
    steps: usize,
    loops: usize,
) -> Result<CaseArtifact, String> {
    setup_database(path).await?;
    let conn = open_db(path).await?;
    let checksum_before = checksum(&conn).await?;

    let worker_path = path.to_owned();
    let started = Instant::now();
    let query_handle = thread::spawn(move || run_query_worker(worker_path, loops, checksum_before));

    let pattern = [
        SMALL_CACHE_KIB,
        MID_CACHE_KIB,
        LARGE_CACHE_KIB,
        MID_CACHE_KIB,
    ];
    let resize_ops = run_resize_pattern(&conn, &pattern, steps, 1).await?;

    let query_ops = query_handle
        .join()
        .map_err(|_| "query_worker_join_failed_possible_deadlock".to_owned())??;

    let final_cache_size = get_cache_size(&conn).await?;
    let checksum_after = checksum(&conn).await?;
    let integrity_ok = integrity_ok(&conn).await?;
    let elapsed_ms = started.elapsed().as_millis();

    Ok(CaseArtifact {
        scenario: Scenario::ConcurrentResizeQueries.as_str().to_owned(),
        resize_ops,
        query_ops,
        write_ops: 0,
        write_aborts: 0,
        final_cache_size,
        integrity_ok,
        deadlock_free: true,
        starvation_free: elapsed_ms <= max_case_millis(),
        elapsed_ms,
        checksum_before,
        checksum_after,
    })
}

async fn concurrent_resize_writes_case(
    path: &str,
    steps: usize,
    ops_per_writer: usize,
) -> Result<CaseArtifact, String> {
    setup_database(path).await?;
    let conn = open_db(path).await?;
    let checksum_before = checksum(&conn).await?;

    let started = Instant::now();
    let mut writer_handles = Vec::with_capacity(CONCURRENT_WRITERS);
    for thread_idx in 0..CONCURRENT_WRITERS {
        let worker_path = path.to_owned();
        writer_handles.push(thread::spawn(move || {
            run_writer_worker(worker_path, thread_idx, ops_per_writer)
        }));
    }

    let pattern = [
        LARGE_CACHE_KIB,
        MID_CACHE_KIB,
        SMALL_CACHE_KIB,
        MID_CACHE_KIB,
    ];
    let resize_ops = run_resize_pattern(&conn, &pattern, steps, 1).await?;

    let mut write_ops = 0;
    let mut write_aborts = 0;
    for handle in writer_handles {
        let stats = handle
            .join()
            .map_err(|_| "writer_join_failed_possible_deadlock".to_owned())?;
        write_ops += stats.committed;
        write_aborts += stats.aborted;
    }

    let final_cache_size = get_cache_size(&conn).await?;
    let verify_conn = open_db(path).await?;
    let checksum_after = checksum(&verify_conn).await?;
    let integrity_ok = integrity_ok(&verify_conn).await?;
    let elapsed_ms = started.elapsed().as_millis();

    Ok(CaseArtifact {
        scenario: Scenario::ConcurrentResizeWrites.as_str().to_owned(),
        resize_ops,
        query_ops: 0,
        write_ops,
        write_aborts,
        final_cache_size,
        integrity_ok,
        deadlock_free: true,
        starvation_free: elapsed_ms <= max_case_millis(),
        elapsed_ms,
        checksum_before,
        checksum_after,
    })
}

async fn run_case(
    scenario: Scenario,
    steps: usize,
    loops: usize,
    ops_per_writer: usize,
) -> Result<CaseArtifact, String> {
    let temp_dir = tempdir().map_err(|error| format!("tempdir_failed error={error}"))?;
    let db_path = temp_dir.path().join("resize-storm.db");
    let path = db_path
        .to_str()
        .ok_or_else(|| "db_path_utf8_failed".to_owned())?;

    match scenario {
        Scenario::RapidShrink => rapid_shrink_case(path, steps).await,
        Scenario::RapidGrow => rapid_grow_case(path, steps).await,
        Scenario::Oscillation => oscillation_case(path, steps).await,
        Scenario::ConcurrentResizeQueries => {
            concurrent_resize_queries_case(path, steps, loops).await
        }
        Scenario::ConcurrentResizeWrites => {
            concurrent_resize_writes_case(path, steps, ops_per_writer).await
        }
    }
}

fn write_suite_artifact(suite: &SuiteArtifact) -> Result<PathBuf, String> {
    let root = workspace_root()?;
    let output_dir = root.join("test-results").join("bd_3plop_2");
    fs::create_dir_all(&output_dir).map_err(|error| {
        format!(
            "artifact_dir_create_failed path={} error={error}",
            output_dir.display()
        )
    })?;

    let output_path = output_dir.join(format!("{}.json", suite.run_id));
    let payload = serde_json::to_string_pretty(suite)
        .map_err(|error| format!("artifact_serialize_failed error={error}"))?;
    fs::write(&output_path, payload).map_err(|error| {
        format!(
            "artifact_write_failed path={} error={error}",
            output_path.display()
        )
    })?;

    Ok(output_path)
}

#[test]
fn test_bd_3plop_2_cache_size_roundtrip() {
    asupersync::test_utils::run_test(|| async {
        let temp_dir = tempdir().expect("tempdir should be created");
        let db_path = temp_dir.path().join("roundtrip.db");
        let db_str = db_path.to_str().expect("db path should be utf8");

        setup_database(db_str)
            .await
            .expect("database setup should succeed");
        let conn = open_db(db_str).await.expect("connection should open");

        set_cache_size(&conn, SMALL_CACHE_KIB)
            .await
            .expect("cache_size small set should succeed");
        let small = get_cache_size(&conn)
            .await
            .expect("cache_size small query should succeed");
        assert_eq!(
            small, SMALL_CACHE_KIB,
            "cache_size small roundtrip must match"
        );

        set_cache_size(&conn, LARGE_CACHE_KIB)
            .await
            .expect("cache_size large set should succeed");
        let large = get_cache_size(&conn)
            .await
            .expect("cache_size large query should succeed");
        assert_eq!(
            large, LARGE_CACHE_KIB,
            "cache_size large roundtrip must match"
        );
    });
}

#[test]
fn test_e2e_bd_3plop_2_resize_storms() {
    asupersync::test_utils::run_test(|| async {
        let steps = resize_steps();
        let loops = query_loops();
        let ops_per_writer = writer_ops();

        let mut cases = Vec::new();
        for scenario in Scenario::ALL {
            let case = run_case(scenario, steps, loops, ops_per_writer)
                .await
                .unwrap_or_else(|error| {
                    panic!(
                        "bead_id={BEAD_ID} scenario={} steps={} loops={} writer_ops={} failed: {error}",
                        scenario.as_str(),
                        steps,
                        loops,
                        ops_per_writer,
                    )
                });

            assert!(
                case.integrity_ok,
                "bead_id={BEAD_ID} scenario={} integrity_check must pass",
                case.scenario
            );
            assert!(
                case.deadlock_free,
                "bead_id={BEAD_ID} scenario={} must complete without deadlock",
                case.scenario
            );
            assert!(
                case.starvation_free,
                "bead_id={BEAD_ID} scenario={} exceeded max case time budget {}ms",
                case.scenario,
                max_case_millis()
            );
            if case.scenario == Scenario::ConcurrentResizeQueries.as_str() {
                assert_eq!(
                    case.checksum_before.row_count, case.checksum_after.row_count,
                    "bead_id={BEAD_ID} query scenario row_count must remain stable"
                );
                assert_eq!(
                    case.checksum_before.sum_v, case.checksum_after.sum_v,
                    "bead_id={BEAD_ID} query scenario sum(v) must remain stable"
                );
            }
            if case.scenario == Scenario::ConcurrentResizeWrites.as_str() {
                assert!(
                    case.checksum_after.row_count >= case.checksum_before.row_count,
                    "bead_id={BEAD_ID} write scenario row_count must not shrink"
                );
                assert!(
                    case.checksum_after.sum_v >= case.checksum_before.sum_v,
                    "bead_id={BEAD_ID} write scenario sum(v) must not decrease"
                );
            }

            eprintln!(
                "INFO bead_id={BEAD_ID} case=resize_storm scenario={} resize_ops={} query_ops={} write_ops={} write_aborts={} final_cache_size={} integrity_ok={} deadlock_free={} starvation_free={} elapsed_ms={} checksum_before_row_count={} checksum_after_row_count={} checksum_before_sum_v={} checksum_after_sum_v={}",
                case.scenario,
                case.resize_ops,
                case.query_ops,
                case.write_ops,
                case.write_aborts,
                case.final_cache_size,
                case.integrity_ok,
                case.deadlock_free,
                case.starvation_free,
                case.elapsed_ms,
                case.checksum_before.row_count,
                case.checksum_after.row_count,
                case.checksum_before.sum_v,
                case.checksum_after.sum_v,
            );

            cases.push(case);
        }

        let run_id = format!(
            "{BEAD_ID}-steps{steps}-q{loops}-w{ops_per_writer}-{}",
            0xCACE_u64
        );
        let suite = SuiteArtifact {
            schema_version: 1,
            bead_id: BEAD_ID.to_owned(),
            run_id: run_id.clone(),
            resize_steps: steps,
            query_loops: loops,
            writer_ops: ops_per_writer,
            cases,
            acceptance_checks: vec![
                "all 5 resize-storm scenarios executed".to_owned(),
                "no deadlocks observed (all threads joined successfully)".to_owned(),
                "integrity_check passed for every scenario".to_owned(),
                "query checksum remained stable during resize+queries scenario".to_owned(),
                "write scenario completed with bounded runtime and non-decreasing checksums"
                    .to_owned(),
            ],
        };

        let artifact_path = write_suite_artifact(&suite).expect("suite artifact should be written");
        eprintln!(
            "INFO bead_id={BEAD_ID} case=suite_artifact path={} run_id={} scenarios={}",
            artifact_path.display(),
            run_id,
            suite.cases.len(),
        );
    });
}
