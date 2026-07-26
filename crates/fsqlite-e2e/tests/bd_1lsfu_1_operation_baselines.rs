//! Integration tests for operation baseline capture and regression detection.
//!
//! Bead: bd-1lsfu.1
//!
//! Tests that:
//! 1. All 9 operations can be measured via `measure_operation`.
//! 2. Baseline JSON roundtrips correctly.
//! 3. Regression detection works with configurable thresholds.
//! 4. Baselines can be captured for both FrankenSQLite and C SQLite.
#![recursion_limit = "512"]

use fsqlite_core::connection::{
    hot_path_profile_snapshot, reset_hot_path_profile, set_hot_path_profile_enabled,
};
use fsqlite_e2e::baseline::{
    BaselineReport, BenchmarkRecoveryMeasurement, BenchmarkRecoveryProbeId,
    DEFAULT_REGRESSION_THRESHOLD, LatencyStats, Operation, OperationBaseline, RegressionResult,
    evaluate_bd_wwqen_in_subquery_recovery, measure_operation, render_benchmark_recovery_markdown,
};
use fsqlite_types::SqliteValue;
use std::sync::{Mutex, OnceLock};

// ─── Async mirror of `measure_operation` ────────────────────────────────

/// Nearest-rank percentile on a sorted slice.
///
/// Mirrors the private `fsqlite_e2e::baseline::percentile` helper so that
/// [`measure_operation_async`] reports numbers identical to
/// [`measure_operation`].
fn nearest_rank_percentile(sorted: &[u64], pct: u32) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let last_index = sorted.len() - 1;
    let pct_usize = usize::try_from(pct).map_or(100, |value| value.min(100));
    let idx = pct_usize.saturating_mul(last_index).saturating_add(50) / 100;
    sorted[idx.min(sorted.len() - 1)]
}

/// Async mirror of [`measure_operation`].
///
/// `measure_operation` takes a synchronous `FnMut()`, which cannot contain
/// `.await`. This helper runs the identical warmup/measurement/percentile
/// pipeline over an async closure so that FrankenSQLite engine calls (which are
/// now `async`) can be measured with exactly the same statistics.
async fn measure_operation_async<F>(
    warmup: u32,
    iterations: u32,
    mut f: F,
) -> (LatencyStats, f64)
where
    F: std::ops::AsyncFnMut(),
{
    // Warmup phase.
    for _ in 0..warmup {
        f().await;
    }

    // Measurement phase.
    let mut samples_micros: Vec<u64> = Vec::with_capacity(iterations as usize);
    for _ in 0..iterations {
        let start = std::time::Instant::now();
        f().await;
        let elapsed = start.elapsed();
        let micros = u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX);
        samples_micros.push(micros);
    }

    samples_micros.sort_unstable();

    let len = samples_micros.len();
    let p50 = nearest_rank_percentile(&samples_micros, 50);
    let p95 = nearest_rank_percentile(&samples_micros, 95);
    let p99 = nearest_rank_percentile(&samples_micros, 99);
    let max = samples_micros.last().copied().unwrap_or(0);

    // Throughput: median ops/sec based on p50.
    let throughput = if p50 > 0 {
        1_000_000.0 / p50 as f64
    } else if len > 0 {
        // Sub-microsecond: estimate from total time.
        let total_micros: u64 = samples_micros.iter().sum();
        if total_micros > 0 {
            (len as f64) * 1_000_000.0 / total_micros as f64
        } else {
            f64::INFINITY
        }
    } else {
        0.0
    };

    (
        LatencyStats {
            p50_micros: p50,
            p95_micros: p95,
            p99_micros: p99,
            max_micros: max,
        },
        throughput,
    )
}

// ─── Baseline module unit integration tests ─────────────────────────────

#[test]
fn all_nine_operations_have_unique_names() {
    let ops = Operation::all();
    assert_eq!(ops.len(), 9);
    let mut names: Vec<&str> = ops.iter().map(Operation::display_name).collect();
    let orig_len = names.len();
    names.sort_unstable();
    names.dedup();
    assert_eq!(names.len(), orig_len, "duplicate operation display names");
}

#[test]
fn baseline_report_json_schema_version() {
    let report = BaselineReport::new("test");
    let json = report.to_pretty_json().unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(
        parsed["schema_version"],
        "fsqlite-e2e.operation_baseline.v1"
    );
    assert!(parsed["methodology"]["version"].is_string());
    assert!(parsed["environment"]["arch"].is_string());
}

#[test]
fn regression_threshold_default_is_ten_percent() {
    assert!((DEFAULT_REGRESSION_THRESHOLD - 0.10).abs() < f64::EPSILON);
}

#[test]
fn regression_check_empty_reports() {
    let old = BaselineReport::new("test");
    let current = BaselineReport::new("test");
    let results = old.check_regression(&current, DEFAULT_REGRESSION_THRESHOLD);
    assert!(results.is_empty());
}

#[test]
fn regression_check_missing_operation_in_current() {
    let mut old = BaselineReport::new("test");
    old.baselines.push(OperationBaseline {
        operation: Operation::PointLookup,
        engine: "frankensqlite".to_owned(),
        row_count: 1000,
        iterations: 100,
        warmup_iterations: 10,
        latency: LatencyStats {
            p50_micros: 50,
            p95_micros: 100,
            p99_micros: 200,
            max_micros: 500,
        },
        throughput_ops_per_sec: 20000.0,
    });

    let current = BaselineReport::new("test");
    let results = old.check_regression(&current, DEFAULT_REGRESSION_THRESHOLD);
    // Missing operation = no comparison = no regression.
    assert!(results.is_empty());
}

#[test]
fn regression_check_exact_match() {
    let baseline = OperationBaseline {
        operation: Operation::Aggregation,
        engine: "frankensqlite".to_owned(),
        row_count: 1000,
        iterations: 100,
        warmup_iterations: 10,
        latency: LatencyStats {
            p50_micros: 100,
            p95_micros: 200,
            p99_micros: 300,
            max_micros: 500,
        },
        throughput_ops_per_sec: 10000.0,
    };

    let mut old = BaselineReport::new("test");
    old.baselines.push(baseline.clone());
    let mut current = BaselineReport::new("test");
    current.baselines.push(baseline);

    let results = old.check_regression(&current, DEFAULT_REGRESSION_THRESHOLD);
    assert_eq!(results.len(), 1);
    assert!(!results[0].regressed);
    assert!((results[0].change_pct).abs() < 0.01);
}

#[test]
fn regression_result_summary_contains_key_info() {
    let result = RegressionResult {
        operation: Operation::BatchInsert,
        engine: "frankensqlite".to_owned(),
        baseline_p50_micros: 1000,
        current_p50_micros: 1200,
        change_pct: 20.0,
        regressed: true,
    };
    let summary = result.summary();
    assert!(summary.contains("REGRESSION"));
    assert!(summary.contains("batch_insert"));
    assert!(summary.contains("frankensqlite"));
    assert!(summary.contains("1000"));
    assert!(summary.contains("1200"));
}

// ─── Live operation measurement tests ───────────────────────────────────

#[test]
fn measure_noop_operation() {
    let (stats, throughput) = measure_operation(2, 10, || {
        // No-op: should be very fast.
    });
    // Stats should be valid (non-panicking, sane values).
    assert!(stats.p50_micros <= stats.p95_micros);
    assert!(stats.p95_micros <= stats.p99_micros);
    assert!(stats.p99_micros <= stats.max_micros);
    assert!(throughput > 0.0);
}

#[test]
fn measure_frankensqlite_point_lookup() {
    asupersync::test_utils::run_test(|| async {
        let conn = fsqlite::Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .await
            .unwrap();
        conn.execute("BEGIN").await.unwrap();
        for i in 0..100_i64 {
            conn.execute(&format!("INSERT INTO t VALUES ({i}, 'v{i}')"))
                .await
                .unwrap();
        }
        conn.execute("COMMIT").await.unwrap();

        let mut id = 1_i64;
        let (stats, throughput) = measure_operation_async(5, 50, async || {
            let rows = conn
                .query(&format!("SELECT * FROM t WHERE id = {id}"))
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);
            id = (id % 100) + 1;
        })
        .await;

        assert!(stats.p50_micros >= 1, "point lookup should take >= 1us");
        assert!(throughput > 0.0);
    });
}

#[test]
fn measure_frankensqlite_sequential_scan() {
    asupersync::test_utils::run_test(|| async {
        let conn = fsqlite::Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .await
            .unwrap();
        conn.execute("BEGIN").await.unwrap();
        for i in 0..200_i64 {
            conn.execute(&format!("INSERT INTO t VALUES ({i}, 'v{i}')"))
                .await
                .unwrap();
        }
        conn.execute("COMMIT").await.unwrap();

        let (stats, throughput) = measure_operation_async(3, 20, async || {
            let rows = conn.query("SELECT * FROM t").await.unwrap();
            assert_eq!(rows.len(), 200);
        })
        .await;

        assert!(stats.p50_micros >= 1);
        assert!(throughput > 0.0);
    });
}

#[test]
fn measure_csqlite_point_lookup() {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    conn.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT);")
        .unwrap();
    conn.execute_batch("BEGIN").unwrap();
    {
        let mut stmt = conn
            .prepare("INSERT INTO t VALUES (?1, ('v' || ?1))")
            .unwrap();
        for i in 0..100_i64 {
            stmt.execute(rusqlite::params![i]).unwrap();
        }
    }
    conn.execute_batch("COMMIT").unwrap();

    let mut stmt = conn.prepare("SELECT * FROM t WHERE id = ?1").unwrap();
    let mut id = 1_i64;
    let (stats, throughput) = measure_operation(5, 50, || {
        let rows: Vec<(i64,)> = stmt
            .query_map(rusqlite::params![id], |row| Ok((row.get(0).unwrap(),)))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(rows.len(), 1);
        id = (id % 100) + 1;
    });

    // p50 is u64, always >= 0; just confirm stats are valid.
    assert!(stats.p95_micros >= stats.p50_micros);
    assert!(throughput > 0.0);
}

// ─── Baseline save/load integration ─────────────────────────────────────

#[test]
fn save_load_roundtrip_with_all_operations() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("full_baseline.json");

    let mut report = BaselineReport::new("test");
    for op in Operation::all() {
        report.baselines.push(OperationBaseline {
            operation: op,
            engine: "frankensqlite".to_owned(),
            row_count: 1000,
            iterations: 100,
            warmup_iterations: 10,
            latency: LatencyStats {
                p50_micros: 100,
                p95_micros: 200,
                p99_micros: 300,
                max_micros: 500,
            },
            throughput_ops_per_sec: 10000.0,
        });
    }

    fsqlite_e2e::baseline::save_baseline(&report, &path).unwrap();
    let loaded = fsqlite_e2e::baseline::load_baseline(&path).unwrap();

    assert_eq!(loaded.baselines.len(), 9);
    for (i, op) in Operation::all().iter().enumerate() {
        assert_eq!(loaded.baselines[i].operation, *op);
    }
}

// ─── Full 9-operation baseline capture ──────────────────────────────────

#[test]
#[allow(clippy::too_many_lines)]
fn capture_all_nine_baselines_frankensqlite() {
    asupersync::test_utils::run_test(|| async {
        let conn = fsqlite::Connection::open(":memory:").await.unwrap();

        // Setup: create main table with 200 rows.
        conn.execute(
            "CREATE TABLE bench (id INTEGER PRIMARY KEY, name TEXT, category TEXT, score INTEGER)",
        )
        .await
        .unwrap();
        conn.execute("BEGIN").await.unwrap();
        for i in 1..=200_i64 {
            conn.execute(&format!(
                "INSERT INTO bench VALUES ({i}, 'name_{i}', 'cat_{}', {})",
                i % 10,
                i * 7,
            ))
            .await
            .unwrap();
        }
        conn.execute("COMMIT").await.unwrap();

        // Setup: create join table with 100 rows.
        conn.execute("CREATE TABLE bench2 (id INTEGER PRIMARY KEY, bench_id INTEGER, label TEXT)")
            .await
            .unwrap();
        conn.execute("BEGIN").await.unwrap();
        for i in 1..=100_i64 {
            conn.execute(&format!(
                "INSERT INTO bench2 VALUES ({i}, {}, 'label_{i}')",
                i * 2,
            ))
            .await
            .unwrap();
        }
        conn.execute("COMMIT").await.unwrap();

        let mut report = BaselineReport::new("test");
        let warmup = 3_u32;
        let iters = 20_u32;

        // 1. Sequential scan.
        let (lat, thr) = measure_operation_async(warmup, iters, async || {
            let rows = conn.query("SELECT * FROM bench").await.unwrap();
            assert_eq!(rows.len(), 200);
        })
        .await;
        report.baselines.push(OperationBaseline {
            operation: Operation::SequentialScan,
            engine: "frankensqlite".to_owned(),
            row_count: 200,
            iterations: iters,
            warmup_iterations: warmup,
            latency: lat,
            throughput_ops_per_sec: thr,
        });

        // 2. Point lookup.
        let mut id = 1_i64;
        let (lat, thr) = measure_operation_async(warmup, iters, async || {
            let rows = conn
                .query(&format!("SELECT * FROM bench WHERE id = {id}"))
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);
            id = (id % 200) + 1;
        })
        .await;
        report.baselines.push(OperationBaseline {
            operation: Operation::PointLookup,
            engine: "frankensqlite".to_owned(),
            row_count: 200,
            iterations: iters,
            warmup_iterations: warmup,
            latency: lat,
            throughput_ops_per_sec: thr,
        });

        // 3. Range scan.
        let (lat, thr) = measure_operation_async(warmup, iters, async || {
            let rows = conn
                .query("SELECT * FROM bench WHERE id >= 50 AND id < 100")
                .await
                .unwrap();
            assert_eq!(rows.len(), 50);
        })
        .await;
        report.baselines.push(OperationBaseline {
            operation: Operation::RangeScan,
            engine: "frankensqlite".to_owned(),
            row_count: 200,
            iterations: iters,
            warmup_iterations: warmup,
            latency: lat,
            throughput_ops_per_sec: thr,
        });

        // 4. Single-row insert (into a separate disposable table per measurement).
        let conn4 = fsqlite::Connection::open(":memory:").await.unwrap();
        conn4
            .execute("CREATE TABLE ins_test (id INTEGER PRIMARY KEY, val TEXT)")
            .await
            .unwrap();
        let mut insert_id = 1_i64;
        let (lat, thr) = measure_operation_async(warmup, iters, async || {
            conn4
                .execute(&format!(
                    "INSERT INTO ins_test VALUES ({insert_id}, 'val_{insert_id}')"
                ))
                .await
                .unwrap();
            insert_id += 1;
        })
        .await;
        report.baselines.push(OperationBaseline {
            operation: Operation::SingleRowInsert,
            engine: "frankensqlite".to_owned(),
            row_count: 0,
            iterations: iters,
            warmup_iterations: warmup,
            latency: lat,
            throughput_ops_per_sec: thr,
        });

        // 5. Batch insert.
        let (lat, thr) = measure_operation_async(warmup, iters, async || {
            let batch_conn = fsqlite::Connection::open(":memory:").await.unwrap();
            batch_conn
                .execute("CREATE TABLE batch_t (id INTEGER PRIMARY KEY, val TEXT)")
                .await
                .unwrap();
            batch_conn.execute("BEGIN").await.unwrap();
            for j in 1..=100_i64 {
                batch_conn
                    .execute(&format!("INSERT INTO batch_t VALUES ({j}, 'v{j}')"))
                    .await
                    .unwrap();
            }
            batch_conn.execute("COMMIT").await.unwrap();
        })
        .await;
        report.baselines.push(OperationBaseline {
            operation: Operation::BatchInsert,
            engine: "frankensqlite".to_owned(),
            row_count: 100,
            iterations: iters,
            warmup_iterations: warmup,
            latency: lat,
            throughput_ops_per_sec: thr,
        });

        // 6. Single-row update.
        let mut upd_id = 1_i64;
        let (lat, thr) = measure_operation_async(warmup, iters, async || {
            conn.execute(&format!(
                "UPDATE bench SET score = {} WHERE id = {upd_id}",
                upd_id * 13,
            ))
            .await
            .unwrap();
            upd_id = (upd_id % 200) + 1;
        })
        .await;
        report.baselines.push(OperationBaseline {
            operation: Operation::SingleRowUpdate,
            engine: "frankensqlite".to_owned(),
            row_count: 200,
            iterations: iters,
            warmup_iterations: warmup,
            latency: lat,
            throughput_ops_per_sec: thr,
        });

        // 7. Single-row delete (use a disposable table).
        let conn7 = fsqlite::Connection::open(":memory:").await.unwrap();
        conn7
            .execute("CREATE TABLE del_test (id INTEGER PRIMARY KEY, val TEXT)")
            .await
            .unwrap();
        for j in 1..=1000_i64 {
            conn7
                .execute(&format!("INSERT INTO del_test VALUES ({j}, 'v{j}')"))
                .await
                .unwrap();
        }
        let mut del_id = 1_i64;
        let (lat, thr) = measure_operation_async(warmup, iters, async || {
            conn7
                .execute(&format!("DELETE FROM del_test WHERE id = {del_id}"))
                .await
                .unwrap();
            del_id += 1;
        })
        .await;
        report.baselines.push(OperationBaseline {
            operation: Operation::SingleRowDelete,
            engine: "frankensqlite".to_owned(),
            row_count: 1000,
            iterations: iters,
            warmup_iterations: warmup,
            latency: lat,
            throughput_ops_per_sec: thr,
        });

        // 8. 2-way equi-join.
        let (lat, thr) = measure_operation_async(warmup, iters, async || {
            let rows = conn
                .query(
                    "SELECT bench.id, bench.name, bench2.label \
                     FROM bench INNER JOIN bench2 ON bench.id = bench2.bench_id",
                )
                .await
                .unwrap();
            assert!(!rows.is_empty());
        })
        .await;
        report.baselines.push(OperationBaseline {
            operation: Operation::TwoWayEquiJoin,
            engine: "frankensqlite".to_owned(),
            row_count: 200,
            iterations: iters,
            warmup_iterations: warmup,
            latency: lat,
            throughput_ops_per_sec: thr,
        });

        // 9. Aggregation.
        let (lat, thr) = measure_operation_async(warmup, iters, async || {
            let rows = conn
                .query("SELECT COUNT(*), SUM(score), AVG(score) FROM bench")
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);
        })
        .await;
        report.baselines.push(OperationBaseline {
            operation: Operation::Aggregation,
            engine: "frankensqlite".to_owned(),
            row_count: 200,
            iterations: iters,
            warmup_iterations: warmup,
            latency: lat,
            throughput_ops_per_sec: thr,
        });

        // Verify we captured all 9.
        assert_eq!(report.baselines.len(), 9);

        // Verify JSON roundtrip.
        let json = report.to_pretty_json().unwrap();
        let parsed = BaselineReport::from_json(&json).unwrap();
        assert_eq!(parsed.baselines.len(), 9);

        // Verify no regressions against self (should all be exact match).
        let results = report.check_regression(&parsed, DEFAULT_REGRESSION_THRESHOLD);
        for r in &results {
            assert!(
                !r.regressed,
                "self-comparison should not regress: {}",
                r.summary()
            );
        }
    });
}

// ─── Manual perf probes ────────────────────────────────────────────────

fn median_f64(mut values: Vec<f64>) -> f64 {
    values.sort_by(|left, right| left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal));
    values[values.len() / 2]
}

fn manual_hot_path_profile_mutex() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

/// Re-run the current metrics assertion as the only test in a child process.
///
/// Hot-path profiling is intentionally process-global. The profile mutex
/// serializes the tests which inspect those counters, but ordinary sibling
/// tests can still execute FrankenSQLite statements while profiling is
/// enabled and contaminate an exact-count snapshot. An exact one-test child
/// provides a genuinely private metrics process.
fn run_hot_path_test_in_isolated_process() -> bool {
    const CHILD_TEST_ENV: &str = "FSQLITE_OPERATION_BASELINE_HOT_PATH_TEST_CHILD";

    let current_thread = std::thread::current();
    let test_name = current_thread
        .name()
        .expect("the Rust test harness should name every test thread");
    if std::env::var(CHILD_TEST_ENV).as_deref() == Ok(test_name) {
        return false;
    }

    let status = std::process::Command::new(
        std::env::current_exe().expect("the current test executable should be available"),
    )
    .arg("--exact")
    .arg(test_name)
    .arg("--test-threads=1")
    .arg("--nocapture")
    .env(CHILD_TEST_ENV, test_name)
    .status()
    .expect("the isolated hot-path test process should start");

    assert!(
        status.success(),
        "isolated hot-path test {test_name} failed with {status}"
    );
    true
}

struct ManualHotPathProfileGuard {
    _guard: std::sync::MutexGuard<'static, ()>,
}

impl ManualHotPathProfileGuard {
    fn new() -> Self {
        let guard = manual_hot_path_profile_mutex().lock().unwrap();
        reset_hot_path_profile();
        set_hot_path_profile_enabled(true);
        Self { _guard: guard }
    }
}

impl Drop for ManualHotPathProfileGuard {
    fn drop(&mut self) {
        reset_hot_path_profile();
        set_hot_path_profile_enabled(false);
    }
}

#[derive(Debug)]
struct PrepareCacheProbeRun {
    rows_per_sec: f64,
    parse_cache_hits: u64,
    parse_cache_misses: u64,
    compiled_cache_hits: u64,
    compiled_cache_misses: u64,
    prepared_cache_hits: u64,
    prepared_cache_misses: u64,
}

#[derive(Debug)]
struct DecodeCacheProbeRun {
    rows_per_sec: f64,
    decode_cache_hits: u64,
    decode_cache_misses: u64,
    decode_cache_invalidations_position: u64,
    decode_cache_invalidations_write: u64,
    decode_cache_invalidations_pseudo: u64,
    record_decodes_per_row: f64,
}

fn opcode_total(profile: &fsqlite_core::connection::HotPathProfileSnapshot, opcode: &str) -> u64 {
    profile
        .vdbe
        .opcode_execution_totals
        .iter()
        .find(|entry| entry.opcode == opcode)
        .map_or(0, |entry| entry.total)
}

fn top_opcode_summary(
    profile: &fsqlite_core::connection::HotPathProfileSnapshot,
    limit: usize,
) -> String {
    let summary = profile
        .vdbe
        .opcode_execution_totals
        .iter()
        .take(limit)
        .map(|entry| format!("{}={}", entry.opcode, entry.total))
        .collect::<Vec<_>>();
    if summary.is_empty() {
        "(none)".to_owned()
    } else {
        summary.join(", ")
    }
}

fn log_manual_hot_path_profile(
    label: &str,
    wall: std::time::Duration,
    profile: &fsqlite_core::connection::HotPathProfileSnapshot,
) {
    eprintln!(
        concat!(
            "manual_hot_path_profile.{label} ",
            "wall_us={wall_us} execute_body_us={execute_body_us} ",
            "fast={fast} slow={slow} bg_status_us={bg_status_us} ",
            "opcodes_total={opcodes_total} stmt_us_total={stmt_us_total} ",
            "column_reads={column_reads} record_decodes={record_decodes} ",
            "decode_hits={decode_hits} decode_misses={decode_misses} ",
            "decoded_values={decoded_values} result_rows={result_rows} result_values={result_values} ",
            "open_read={open_read} rewind={rewind} seek_ge={seek_ge} rowid={rowid} ",
            "column={column} next={next} eq={eq} lt={lt} gt={gt} result_row={result_row} count={count} ",
            "top_opcodes=[{top_opcodes}]"
        ),
        label = label,
        wall_us = wall.as_micros(),
        execute_body_us = profile.execute_body_time_ns / 1_000,
        fast = profile.parser.fast_path_executions,
        slow = profile.parser.slow_path_executions,
        bg_status_us = profile.background_status_time_ns / 1_000,
        opcodes_total = profile.vdbe.opcodes_executed_total,
        stmt_us_total = profile.vdbe.statement_duration_us_total,
        column_reads = profile.vdbe.column_reads_total,
        record_decodes = profile.vdbe.record_decode_calls_total,
        decode_hits = profile.vdbe.decode_cache_hits_total,
        decode_misses = profile.vdbe.decode_cache_misses_total,
        decoded_values = profile.vdbe.decoded_values_total,
        result_rows = profile.vdbe.result_rows_total,
        result_values = profile.vdbe.result_values_total,
        open_read = opcode_total(profile, "OpenRead"),
        rewind = opcode_total(profile, "Rewind"),
        seek_ge = opcode_total(profile, "SeekGE"),
        rowid = opcode_total(profile, "Rowid"),
        column = opcode_total(profile, "Column"),
        next = opcode_total(profile, "Next"),
        eq = opcode_total(profile, "Eq"),
        lt = opcode_total(profile, "Lt"),
        gt = opcode_total(profile, "Gt"),
        result_row = opcode_total(profile, "ResultRow"),
        count = opcode_total(profile, "Count"),
        top_opcodes = top_opcode_summary(profile, 12),
    );
}

fn log_manual_insert_hot_path_profile(
    label: &str,
    wall: std::time::Duration,
    profile: &fsqlite_core::connection::HotPathProfileSnapshot,
) {
    eprintln!(
        concat!(
            "manual_insert_hot_path_profile.{label} ",
            "wall_us={wall_us} bg_status_us={bg_status_us} prepared_schema_refresh_us={prepared_schema_refresh_us} ",
            "begin_setup_us={begin_setup_us} execute_body_us={execute_body_us} ",
            "row_build_us={row_build_us} cursor_setup_us={cursor_setup_us} serialize_us={serialize_us} ",
            "btree_insert_us={btree_insert_us} memdb_apply_us={memdb_apply_us} direct_body_us={direct_body_us} ",
            "schema_validation_us={schema_validation_us} autocommit_begin_us={autocommit_begin_us} ",
            "change_tracking_us={change_tracking_us} autocommit_resolve_us={autocommit_resolve_us} ",
            "autocommit_wrapper_us={autocommit_wrapper_us} direct_autocommit_execs={direct_autocommit_execs} ",
            "commit_pre_txn_us={commit_pre_txn_us} commit_txn_roundtrip_us={commit_txn_roundtrip_us} ",
            "commit_finalize_seq_us={commit_finalize_seq_us} commit_handle_finalize_us={commit_handle_finalize_us} ",
            "commit_post_write_maintenance_us={commit_post_write_maintenance_us} finalize_post_publish_us={finalize_post_publish_us} ",
            "fast_execs={fast_execs} slow_execs={slow_execs} prepared_insert_fast_hits={prepared_insert_fast_hits} ",
            "prepared_insert_instrumented_hits={prepared_insert_instrumented_hits} cached_write_reuses={cached_write_reuses} ",
            "cached_write_parks={cached_write_parks} memdb_refreshes={memdb_refreshes} opcodes_total={opcodes_total}"
        ),
        label = label,
        wall_us = wall.as_micros(),
        bg_status_us = profile.background_status_time_ns / 1_000,
        prepared_schema_refresh_us = profile.prepared_schema_refresh_time_ns / 1_000,
        begin_setup_us = profile.begin_setup_time_ns / 1_000,
        execute_body_us = profile.execute_body_time_ns / 1_000,
        row_build_us = profile.prepared_direct_insert_row_build_time_ns / 1_000,
        cursor_setup_us = profile.prepared_direct_insert_cursor_setup_time_ns / 1_000,
        serialize_us = profile.prepared_direct_insert_serialize_time_ns / 1_000,
        btree_insert_us = profile.prepared_direct_insert_btree_insert_time_ns / 1_000,
        memdb_apply_us = profile.prepared_direct_insert_memdb_apply_time_ns / 1_000,
        direct_body_us = profile
            .prepared_direct_insert_row_build_time_ns
            .saturating_add(profile.prepared_direct_insert_cursor_setup_time_ns)
            .saturating_add(profile.prepared_direct_insert_serialize_time_ns)
            .saturating_add(profile.prepared_direct_insert_btree_insert_time_ns)
            .saturating_add(profile.prepared_direct_insert_memdb_apply_time_ns)
            / 1_000,
        schema_validation_us = profile.prepared_direct_insert_schema_validation_time_ns / 1_000,
        autocommit_begin_us = profile.prepared_direct_insert_autocommit_begin_time_ns / 1_000,
        change_tracking_us = profile.prepared_direct_insert_change_tracking_time_ns / 1_000,
        autocommit_resolve_us = profile.prepared_direct_insert_autocommit_resolve_time_ns / 1_000,
        autocommit_wrapper_us = profile
            .prepared_direct_insert_schema_validation_time_ns
            .saturating_add(profile.prepared_direct_insert_autocommit_begin_time_ns)
            .saturating_add(profile.prepared_direct_insert_change_tracking_time_ns)
            .saturating_add(profile.prepared_direct_insert_autocommit_resolve_time_ns)
            / 1_000,
        direct_autocommit_execs = profile.prepared_direct_insert_autocommit_executions,
        commit_pre_txn_us = profile.commit_pre_txn_time_ns / 1_000,
        commit_txn_roundtrip_us = profile.commit_txn_roundtrip_time_ns / 1_000,
        commit_finalize_seq_us = profile.commit_finalize_seq_time_ns / 1_000,
        commit_handle_finalize_us = profile.commit_handle_finalize_time_ns / 1_000,
        commit_post_write_maintenance_us = profile.commit_post_write_maintenance_time_ns / 1_000,
        finalize_post_publish_us = profile.finalize_post_publish_time_ns / 1_000,
        fast_execs = profile.parser.fast_path_executions,
        slow_execs = profile.parser.slow_path_executions,
        prepared_insert_fast_hits = profile.prepared_insert_fast_lane_hits,
        prepared_insert_instrumented_hits = profile.prepared_insert_instrumented_lane_hits,
        cached_write_reuses = profile.cached_write_txn_reuses,
        cached_write_parks = profile.cached_write_txn_parks,
        memdb_refreshes = profile.memdb_refresh_count,
        opcodes_total = profile.vdbe.opcodes_executed_total,
    );
}

fn apply_manual_probe_pragmas_csqlite(conn: &rusqlite::Connection) {
    conn.execute_batch(
        "PRAGMA page_size = 4096;\
         PRAGMA journal_mode = WAL;\
         PRAGMA synchronous = NORMAL;\
         PRAGMA cache_size = -64000;",
    )
    .ok();
}

async fn apply_manual_probe_pragmas_fsqlite(conn: &fsqlite::Connection) {
    for pragma in [
        "PRAGMA page_size = 4096;",
        "PRAGMA journal_mode = WAL;",
        "PRAGMA synchronous = NORMAL;",
        "PRAGMA cache_size = -64000;",
    ] {
        let _ = conn.execute(pragma).await;
    }
}

async fn setup_query_guard_bench(row_count: i64) -> fsqlite::Connection {
    let conn = fsqlite::Connection::open(":memory:").await.unwrap();
    apply_manual_probe_pragmas_fsqlite(&conn).await;
    conn.execute(
        "CREATE TABLE bench (\
             id INTEGER PRIMARY KEY,\
             name TEXT NOT NULL,\
             category TEXT NOT NULL,\
             score INTEGER NOT NULL\
         )",
    )
    .await
    .unwrap();
    conn.execute("BEGIN").await.unwrap();
    for i in 1..=row_count {
        conn.execute(&format!(
            "INSERT INTO bench VALUES ({i}, 'name_{i}', 'cat_{}', {})",
            i % 10,
            i * 7,
        ))
        .await
        .unwrap();
    }
    conn.execute("COMMIT").await.unwrap();
    conn
}

fn expected_bench_score_sum(row_count: i64) -> i64 {
    7 * row_count * (row_count + 1) / 2
}

async fn setup_subquery_guard_bench(row_count: i64) -> fsqlite::Connection {
    let conn = fsqlite::Connection::open(":memory:").await.unwrap();
    let category_count = (row_count / 20).max(5);
    apply_manual_probe_pragmas_fsqlite(&conn).await;
    conn.execute(
        "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL, category_id INTEGER)",
    )
    .await
    .unwrap();
    conn.execute("CREATE TABLE categories (id INTEGER PRIMARY KEY, name TEXT)")
        .await
        .unwrap();
    conn.execute("BEGIN").await.unwrap();
    for i in 1..=category_count {
        conn.execute(&format!("INSERT INTO categories VALUES ({i}, 'cat_{i}')"))
            .await
            .unwrap();
    }
    for i in 1..=row_count {
        let category_id = (i % category_count) + 1;
        let price = i as f64 * 3.14;
        conn.execute(&format!(
            "INSERT INTO products VALUES ({i}, 'prod_{i}', {price}, {category_id})"
        ))
        .await
        .unwrap();
    }
    conn.execute("COMMIT").await.unwrap();
    conn.execute("CREATE INDEX idx_prod_cat ON products(category_id)")
        .await
        .unwrap();
    conn
}

async fn run_fsqlite_prepare_cache_probe<'a, I>(sqls: I, row_count: i64) -> PrepareCacheProbeRun
where
    I: IntoIterator<Item = &'a str>,
{
    const CREATE_TABLE: &str =
        "CREATE TABLE bench (id INTEGER PRIMARY KEY, data TEXT, value REAL);";

    async fn apply_pragmas_fsqlite(conn: &fsqlite::Connection) {
        for pragma in [
            "PRAGMA page_size = 4096;",
            "PRAGMA journal_mode = WAL;",
            "PRAGMA synchronous = NORMAL;",
            "PRAGMA cache_size = -64000;",
        ] {
            let _ = conn.execute(pragma).await;
        }
    }

    let _profile_guard = ManualHotPathProfileGuard::new();
    let conn = fsqlite::Connection::open(":memory:").await.unwrap();
    apply_pragmas_fsqlite(&conn).await;
    conn.execute(CREATE_TABLE).await.unwrap();

    reset_hot_path_profile();
    let start = std::time::Instant::now();
    for (idx, sql) in sqls.into_iter().enumerate() {
        let stmt = conn.prepare(sql).await.unwrap();
        stmt.execute_with_params(&[SqliteValue::Integer(i64::try_from(idx).unwrap())])
            .await
            .unwrap();
    }
    let elapsed = start.elapsed();
    let profile = hot_path_profile_snapshot();

    let rows = conn.query("SELECT COUNT(*) FROM bench").await.unwrap();
    assert_eq!(rows[0].values()[0], SqliteValue::Integer(row_count));

    PrepareCacheProbeRun {
        rows_per_sec: row_count as f64 / elapsed.as_secs_f64(),
        parse_cache_hits: profile.parser.parse_cache_hits,
        parse_cache_misses: profile.parser.parse_cache_misses,
        compiled_cache_hits: profile.parser.compiled_cache_hits,
        compiled_cache_misses: profile.parser.compiled_cache_misses,
        prepared_cache_hits: profile.parser.prepared_cache_hits,
        prepared_cache_misses: profile.parser.prepared_cache_misses,
    }
}

async fn run_fsqlite_decode_cache_probe(
    sql: &str,
    iterations: usize,
    bench_rows: i64,
    expected_rows: usize,
) -> DecodeCacheProbeRun {
    const CREATE_TABLE: &str = "CREATE TABLE bench (id INTEGER PRIMARY KEY, data TEXT);";
    const INSERT_SQL: &str = "INSERT INTO bench VALUES (?1, ?2);";

    async fn apply_pragmas_fsqlite(conn: &fsqlite::Connection) {
        for pragma in [
            "PRAGMA page_size = 4096;",
            "PRAGMA journal_mode = WAL;",
            "PRAGMA synchronous = NORMAL;",
            "PRAGMA cache_size = -64000;",
        ] {
            let _ = conn.execute(pragma).await;
        }
    }

    let _profile_guard = ManualHotPathProfileGuard::new();
    let conn = fsqlite::Connection::open(":memory:").await.unwrap();
    apply_pragmas_fsqlite(&conn).await;
    conn.execute(CREATE_TABLE).await.unwrap();
    for id in 1..=bench_rows {
        conn.execute_with_params(
            INSERT_SQL,
            &[
                SqliteValue::Integer(id),
                SqliteValue::Text(format!("decode-cache-hot-row-{id}").into()),
            ],
        )
        .await
        .unwrap();
    }

    reset_hot_path_profile();
    let start = std::time::Instant::now();
    for _ in 0..iterations {
        let rows = conn.query(sql).await.unwrap();
        assert_eq!(
            rows.len(),
            expected_rows,
            "probe query should return the expected number of rows"
        );
    }
    let elapsed = start.elapsed();

    let profile = hot_path_profile_snapshot();
    DecodeCacheProbeRun {
        rows_per_sec: iterations as f64 / elapsed.as_secs_f64(),
        decode_cache_hits: profile.vdbe.decode_cache_hits_total,
        decode_cache_misses: profile.vdbe.decode_cache_misses_total,
        decode_cache_invalidations_position: profile.vdbe.decode_cache_invalidations_position_total,
        decode_cache_invalidations_write: profile.vdbe.decode_cache_invalidations_write_total,
        decode_cache_invalidations_pseudo: profile.vdbe.decode_cache_invalidations_pseudo_total,
        record_decodes_per_row: if profile.vdbe.result_rows_total == 0 {
            0.0
        } else {
            profile.vdbe.decode_cache_misses_total as f64 / profile.vdbe.result_rows_total as f64
        },
    }
}

#[test]
fn focused_read_guard_shapes_return_expected_results() {
    asupersync::test_utils::run_test(|| async {
        const ROW_COUNT: i64 = 10_000;
        const COUNT_RANGE_START: i64 = 2_500;
        const COUNT_RANGE_WIDTH: i64 = 50;
        const COUNT_RANGE_END: i64 = COUNT_RANGE_START + COUNT_RANGE_WIDTH;
        const UPDATED_SCORE: i64 = 1_400;
        const IN_SUBQUERY_EXPECTED_COUNT: i64 = 100;
        const EXISTS_SUBQUERY_EXPECTED_COUNT: i64 = ROW_COUNT / 2;
        const RECURSIVE_CTE_SUM: i64 = 500_500;
        const RECURSIVE_CTE_SQL: &str = "WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM cnt WHERE x < 1000) SELECT SUM(x) FROM cnt";

        let count_conn = setup_query_guard_bench(ROW_COUNT).await;
        let count_stmt = count_conn
            .prepare("SELECT COUNT(*) FROM bench")
            .await
            .unwrap();
        let count_range_stmt = count_conn
            .prepare("SELECT COUNT(*) FROM bench WHERE id >= ?1 AND id < ?2")
            .await
            .unwrap();
        let count_sum_stmt = count_conn
            .prepare("SELECT COUNT(*), SUM(score) FROM bench")
            .await
            .unwrap();

        let count_row = count_stmt.query_row().await.unwrap();
        assert_eq!(count_row.values()[0], SqliteValue::Integer(ROW_COUNT));
        let count_range_row = count_range_stmt
            .query_row_with_params(&[
                SqliteValue::Integer(COUNT_RANGE_START),
                SqliteValue::Integer(COUNT_RANGE_END),
            ])
            .await
            .unwrap();
        assert_eq!(
            count_range_row.values()[0],
            SqliteValue::Integer(COUNT_RANGE_WIDTH)
        );
        let count_sum_row = count_sum_stmt.query_row().await.unwrap();
        assert_eq!(count_sum_row.values()[0], SqliteValue::Integer(ROW_COUNT));
        assert_eq!(
            count_sum_row.values()[1],
            SqliteValue::Integer(expected_bench_score_sum(ROW_COUNT))
        );

        let inserted_id = ROW_COUNT + 1;
        let inserted_score = inserted_id * 7;
        count_conn
            .execute(&format!(
                "INSERT INTO bench VALUES ({inserted_id}, 'name_{inserted_id}', 'cat_{}', {inserted_score})",
                inserted_id % 10,
            ))
            .await
            .unwrap();
        count_conn
            .execute(&format!(
                "UPDATE bench SET score = {UPDATED_SCORE} WHERE id = 2"
            ))
            .await
            .unwrap();
        count_conn
            .execute("DELETE FROM bench WHERE id = 1")
            .await
            .unwrap();

        let post_write_count_row = count_stmt.query_row().await.unwrap();
        assert_eq!(
            post_write_count_row.values()[0],
            SqliteValue::Integer(ROW_COUNT)
        );
        let post_write_count_range_row = count_range_stmt
            .query_row_with_params(&[
                SqliteValue::Integer(COUNT_RANGE_START),
                SqliteValue::Integer(COUNT_RANGE_END),
            ])
            .await
            .unwrap();
        assert_eq!(
            post_write_count_range_row.values()[0],
            SqliteValue::Integer(COUNT_RANGE_WIDTH)
        );
        let post_write_count_sum_row = count_sum_stmt.query_row().await.unwrap();
        let expected_sum_after_writes =
            expected_bench_score_sum(ROW_COUNT) - 7 - 14 + UPDATED_SCORE + inserted_score;
        assert_eq!(
            post_write_count_sum_row.values()[0],
            SqliteValue::Integer(ROW_COUNT)
        );
        assert_eq!(
            post_write_count_sum_row.values()[1],
            SqliteValue::Integer(expected_sum_after_writes)
        );

        let category_count = (ROW_COUNT / 20).max(5);
        let subquery_conn = setup_subquery_guard_bench(ROW_COUNT).await;
        let exists_sql = format!(
            "SELECT COUNT(*) FROM products p WHERE EXISTS (SELECT 1 FROM categories c WHERE c.id = p.category_id AND c.id <= {})",
            category_count / 2
        );
        let exists_stmt = subquery_conn.prepare(&exists_sql).await.unwrap();
        let exists_row = exists_stmt.query_row().await.unwrap();
        assert_eq!(
            exists_row.values()[0],
            SqliteValue::Integer(EXISTS_SUBQUERY_EXPECTED_COUNT)
        );

        let in_stmt = subquery_conn
            .prepare(
                "SELECT COUNT(*) FROM products WHERE category_id IN (SELECT id FROM categories WHERE id <= 5)",
            )
            .await
            .unwrap();
        let in_row = in_stmt.query_row().await.unwrap();
        assert_eq!(
            in_row.values()[0],
            SqliteValue::Integer(IN_SUBQUERY_EXPECTED_COUNT)
        );

        let recursive_cte_conn = fsqlite::Connection::open(":memory:").await.unwrap();
        let recursive_cte_stmt = recursive_cte_conn.prepare(RECURSIVE_CTE_SQL).await.unwrap();
        let recursive_cte_row = recursive_cte_stmt.query_row().await.unwrap();
        assert_eq!(
            recursive_cte_row.values()[0],
            SqliteValue::Integer(RECURSIVE_CTE_SUM)
        );
    });
}

#[test]
fn prepared_insert_single_transaction_guard_shape_inserts_all_rows() {
    asupersync::test_utils::run_test(|| async {
        const ROW_COUNT: i64 = 1_000;
        const CREATE_TABLE: &str = "CREATE TABLE bench (id INTEGER PRIMARY KEY, name TEXT, email TEXT, score INTEGER, created TEXT)";
        const INSERT_SQL: &str = "INSERT INTO bench VALUES (?1, ('name_' || ?1), ('user_' || ?1 || '@test.com'), (?1 * 7), ('2026-01-' || ((?1 % 28) + 1)))";

        let conn = fsqlite::Connection::open(":memory:").await.unwrap();
        apply_manual_probe_pragmas_fsqlite(&conn).await;
        conn.execute(CREATE_TABLE).await.unwrap();
        conn.execute("BEGIN").await.unwrap();
        let stmt = conn.prepare(INSERT_SQL).await.unwrap();
        for i in 0..ROW_COUNT {
            conn.execute_prepared_with_params(&stmt, &[SqliteValue::Integer(i)])
                .await
                .unwrap();
        }
        conn.execute("COMMIT").await.unwrap();

        let rows = conn.query("SELECT COUNT(*) FROM bench").await.unwrap();
        assert_eq!(rows[0].values()[0], SqliteValue::Integer(ROW_COUNT));
    });
}

#[test]
#[ignore = "manual perf probe; run via rch when investigating COUNT(*)/IN/EXISTS/recursive CTE regressions"]
fn manual_perf_probe_read_guard_shapes_count_in_exists_recursive_cte() {
    asupersync::test_utils::run_test(|| async {
        const ROW_COUNT: i64 = 10_000;
        const WARMUP: u32 = 10;
        const ITERATIONS: u32 = 100;
        const COUNT_RANGE_START: i64 = 2_500;
        const COUNT_RANGE_WIDTH: i64 = 50;
        const COUNT_RANGE_END: i64 = COUNT_RANGE_START + COUNT_RANGE_WIDTH;
        const IN_SUBQUERY_EXPECTED_COUNT: i64 = 100;
        const EXISTS_SUBQUERY_EXPECTED_COUNT: i64 = ROW_COUNT / 2;
        const RECURSIVE_CTE_SUM: i64 = 500_500;
        const RECURSIVE_CTE_SQL: &str = "WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM cnt WHERE x < 1000) SELECT SUM(x) FROM cnt";

        let count_conn = setup_query_guard_bench(ROW_COUNT).await;
        let count_stmt = count_conn
            .prepare("SELECT COUNT(*) FROM bench")
            .await
            .unwrap();
        let count_range_stmt = count_conn
            .prepare("SELECT COUNT(*) FROM bench WHERE id >= ?1 AND id < ?2")
            .await
            .unwrap();
        let count_sum_stmt = count_conn
            .prepare("SELECT COUNT(*), SUM(score) FROM bench")
            .await
            .unwrap();
        let (count_stats, count_throughput) =
            measure_operation_async(WARMUP, ITERATIONS, async || {
                let row = count_stmt.query_row().await.unwrap();
                assert_eq!(row.values()[0], SqliteValue::Integer(ROW_COUNT));
            })
            .await;
        let (count_range_stats, count_range_throughput) =
            measure_operation_async(WARMUP, ITERATIONS, async || {
                let row = count_range_stmt
                    .query_row_with_params(&[
                        SqliteValue::Integer(COUNT_RANGE_START),
                        SqliteValue::Integer(COUNT_RANGE_END),
                    ])
                    .await
                    .unwrap();
                assert_eq!(row.values()[0], SqliteValue::Integer(COUNT_RANGE_WIDTH));
            })
            .await;
        let (count_sum_stats, count_sum_throughput) =
            measure_operation_async(WARMUP, ITERATIONS, async || {
                let row = count_sum_stmt.query_row().await.unwrap();
                assert_eq!(row.values()[0], SqliteValue::Integer(ROW_COUNT));
                assert_eq!(
                    row.values()[1],
                    SqliteValue::Integer(expected_bench_score_sum(ROW_COUNT))
                );
            })
            .await;

        let category_count = (ROW_COUNT / 20).max(5);
        let subquery_conn = setup_subquery_guard_bench(ROW_COUNT).await;
        let exists_sql = format!(
            "SELECT COUNT(*) FROM products p WHERE EXISTS (SELECT 1 FROM categories c WHERE c.id = p.category_id AND c.id <= {})",
            category_count / 2
        );
        let exists_stmt = subquery_conn.prepare(&exists_sql).await.unwrap();
        let (exists_stats, exists_throughput) =
            measure_operation_async(WARMUP, ITERATIONS, async || {
                let row = exists_stmt.query_row().await.unwrap();
                assert_eq!(
                    row.values()[0],
                    SqliteValue::Integer(EXISTS_SUBQUERY_EXPECTED_COUNT)
                );
            })
            .await;

        let in_stmt = subquery_conn
            .prepare(
                "SELECT COUNT(*) FROM products WHERE category_id IN (SELECT id FROM categories WHERE id <= 5)",
            )
            .await
            .unwrap();
        let (in_stats, in_throughput) = measure_operation_async(WARMUP, ITERATIONS, async || {
            let row = in_stmt.query_row().await.unwrap();
            assert_eq!(
                row.values()[0],
                SqliteValue::Integer(IN_SUBQUERY_EXPECTED_COUNT)
            );
        })
        .await;

        let recursive_cte_conn = fsqlite::Connection::open(":memory:").await.unwrap();
        let recursive_cte_stmt = recursive_cte_conn.prepare(RECURSIVE_CTE_SQL).await.unwrap();
        let (recursive_cte_stats, recursive_cte_throughput) =
            measure_operation_async(WARMUP, ITERATIONS, async || {
                let row = recursive_cte_stmt.query_row().await.unwrap();
                assert_eq!(row.values()[0], SqliteValue::Integer(RECURSIVE_CTE_SUM));
            })
            .await;

        eprintln!(
            "manual_perf_probe.read_guard_shapes.count_star p50_us={} p95_us={} throughput_ops_per_sec={count_throughput:.1}",
            count_stats.p50_micros, count_stats.p95_micros,
        );
        eprintln!(
            "manual_perf_probe.read_guard_shapes.count_range_50 p50_us={} p95_us={} throughput_ops_per_sec={count_range_throughput:.1}",
            count_range_stats.p50_micros, count_range_stats.p95_micros,
        );
        eprintln!(
            "manual_perf_probe.read_guard_shapes.count_sum_aggregate p50_us={} p95_us={} throughput_ops_per_sec={count_sum_throughput:.1}",
            count_sum_stats.p50_micros, count_sum_stats.p95_micros,
        );
        eprintln!(
            "manual_perf_probe.read_guard_shapes.exists_subquery p50_us={} p95_us={} throughput_ops_per_sec={exists_throughput:.1}",
            exists_stats.p50_micros, exists_stats.p95_micros,
        );
        eprintln!(
            "manual_perf_probe.read_guard_shapes.in_subquery p50_us={} p95_us={} throughput_ops_per_sec={in_throughput:.1}",
            in_stats.p50_micros, in_stats.p95_micros,
        );
        eprintln!(
            "manual_perf_probe.read_guard_shapes.recursive_cte p50_us={} p95_us={} throughput_ops_per_sec={recursive_cte_throughput:.1}",
            recursive_cte_stats.p50_micros, recursive_cte_stats.p95_micros,
        );
        let in_subquery_recovery = evaluate_bd_wwqen_in_subquery_recovery(
            "test",
            vec![BenchmarkRecoveryMeasurement::latency_probe(
                BenchmarkRecoveryProbeId::InSubquery10kLatency,
                ROW_COUNT as u64,
                in_stats.p50_micros,
                in_stats.p95_micros,
                in_throughput,
            )],
        );
        eprintln!(
            "{}",
            render_benchmark_recovery_markdown(&in_subquery_recovery)
        );
        eprintln!("{}", in_subquery_recovery.to_pretty_json().unwrap());

        assert!(count_throughput > 0.0);
        assert!(count_range_throughput > 0.0);
        assert!(count_sum_throughput > 0.0);
        assert!(exists_throughput > 0.0);
        assert!(in_throughput > 0.0);
        assert!(recursive_cte_throughput > 0.0);
    });
}

#[test]
#[ignore = "manual profile probe; run via rch when investigating COUNT(*)/IN/EXISTS runtime overhead at 100k rows"]
fn manual_hot_path_profile_read_guard_shapes_count_in_exists_100k() {
    asupersync::test_utils::run_test(|| async {
        const ROW_COUNT: i64 = 100_000;
        const COUNT_EXPECTED: i64 = ROW_COUNT;
        const COUNT_RANGE_START: i64 = 25_000;
        const COUNT_RANGE_WIDTH: i64 = 50;
        const COUNT_RANGE_END: i64 = COUNT_RANGE_START + COUNT_RANGE_WIDTH;
        const IN_SUBQUERY_EXPECTED_COUNT: i64 = 100;
        const EXISTS_SUBQUERY_EXPECTED_COUNT: i64 = ROW_COUNT / 2;

        let _profile_guard = ManualHotPathProfileGuard::new();

        let count_conn = setup_query_guard_bench(ROW_COUNT).await;
        let count_stmt = count_conn
            .prepare("SELECT COUNT(*) FROM bench")
            .await
            .unwrap();
        let count_range_stmt = count_conn
            .prepare("SELECT COUNT(*) FROM bench WHERE id >= ?1 AND id < ?2")
            .await
            .unwrap();
        let count_sum_stmt = count_conn
            .prepare("SELECT COUNT(*), SUM(score) FROM bench")
            .await
            .unwrap();
        let warm_count = count_stmt.query_row().await.unwrap();
        assert_eq!(warm_count.values()[0], SqliteValue::Integer(COUNT_EXPECTED));
        reset_hot_path_profile();
        let count_started = std::time::Instant::now();
        let count_row = count_stmt.query_row().await.unwrap();
        let count_wall = count_started.elapsed();
        assert_eq!(count_row.values()[0], SqliteValue::Integer(COUNT_EXPECTED));
        let count_profile = hot_path_profile_snapshot();
        log_manual_hot_path_profile("count_star_100k", count_wall, &count_profile);

        let warm_count_range = count_range_stmt
            .query_row_with_params(&[
                SqliteValue::Integer(COUNT_RANGE_START),
                SqliteValue::Integer(COUNT_RANGE_END),
            ])
            .await
            .unwrap();
        assert_eq!(
            warm_count_range.values()[0],
            SqliteValue::Integer(COUNT_RANGE_WIDTH)
        );
        reset_hot_path_profile();
        let count_range_started = std::time::Instant::now();
        let count_range_row = count_range_stmt
            .query_row_with_params(&[
                SqliteValue::Integer(COUNT_RANGE_START),
                SqliteValue::Integer(COUNT_RANGE_END),
            ])
            .await
            .unwrap();
        let count_range_wall = count_range_started.elapsed();
        assert_eq!(
            count_range_row.values()[0],
            SqliteValue::Integer(COUNT_RANGE_WIDTH)
        );
        let count_range_profile = hot_path_profile_snapshot();
        log_manual_hot_path_profile(
            "count_range_50_100k",
            count_range_wall,
            &count_range_profile,
        );

        let warm_count_sum = count_sum_stmt.query_row().await.unwrap();
        assert_eq!(
            warm_count_sum.values()[0],
            SqliteValue::Integer(COUNT_EXPECTED)
        );
        assert_eq!(
            warm_count_sum.values()[1],
            SqliteValue::Integer(expected_bench_score_sum(ROW_COUNT))
        );
        reset_hot_path_profile();
        let count_sum_started = std::time::Instant::now();
        let count_sum_row = count_sum_stmt.query_row().await.unwrap();
        let count_sum_wall = count_sum_started.elapsed();
        assert_eq!(
            count_sum_row.values()[0],
            SqliteValue::Integer(COUNT_EXPECTED)
        );
        assert_eq!(
            count_sum_row.values()[1],
            SqliteValue::Integer(expected_bench_score_sum(ROW_COUNT))
        );
        let count_sum_profile = hot_path_profile_snapshot();
        log_manual_hot_path_profile(
            "count_sum_aggregate_100k",
            count_sum_wall,
            &count_sum_profile,
        );

        let category_count = (ROW_COUNT / 20).max(5);
        let subquery_conn = setup_subquery_guard_bench(ROW_COUNT).await;

        let exists_sql = format!(
            "SELECT COUNT(*) FROM products p WHERE EXISTS (SELECT 1 FROM categories c WHERE c.id = p.category_id AND c.id <= {})",
            category_count / 2
        );
        let exists_stmt = subquery_conn.prepare(&exists_sql).await.unwrap();
        let warm_exists = exists_stmt.query_row().await.unwrap();
        assert_eq!(
            warm_exists.values()[0],
            SqliteValue::Integer(EXISTS_SUBQUERY_EXPECTED_COUNT)
        );
        reset_hot_path_profile();
        let exists_started = std::time::Instant::now();
        let exists_row = exists_stmt.query_row().await.unwrap();
        let exists_wall = exists_started.elapsed();
        assert_eq!(
            exists_row.values()[0],
            SqliteValue::Integer(EXISTS_SUBQUERY_EXPECTED_COUNT)
        );
        let exists_profile = hot_path_profile_snapshot();
        log_manual_hot_path_profile("exists_subquery_100k", exists_wall, &exists_profile);

        let in_stmt = subquery_conn
            .prepare(
                "SELECT COUNT(*) FROM products WHERE category_id IN (SELECT id FROM categories WHERE id <= 5)",
            )
            .await
            .unwrap();
        let warm_in = in_stmt.query_row().await.unwrap();
        assert_eq!(
            warm_in.values()[0],
            SqliteValue::Integer(IN_SUBQUERY_EXPECTED_COUNT)
        );
        reset_hot_path_profile();
        let in_started = std::time::Instant::now();
        let in_row = in_stmt.query_row().await.unwrap();
        let in_wall = in_started.elapsed();
        assert_eq!(
            in_row.values()[0],
            SqliteValue::Integer(IN_SUBQUERY_EXPECTED_COUNT)
        );
        let in_profile = hot_path_profile_snapshot();
        log_manual_hot_path_profile("in_subquery_100k", in_wall, &in_profile);
        let in_subquery_recovery = evaluate_bd_wwqen_in_subquery_recovery(
            "test",
            vec![BenchmarkRecoveryMeasurement::wall_time_probe(
                BenchmarkRecoveryProbeId::InSubquery100kWallTime,
                ROW_COUNT as u64,
                u64::try_from(in_wall.as_micros()).unwrap_or(u64::MAX),
            )],
        );
        eprintln!(
            "{}",
            render_benchmark_recovery_markdown(&in_subquery_recovery)
        );
        eprintln!("{}", in_subquery_recovery.to_pretty_json().unwrap());
    });
}

#[test]
#[ignore = "manual perf probe; run via rch when investigating bd-wwqen.4 future query_row fast-path cuts"]
fn manual_perf_probe_future_query_row_probe_shapes_100k() {
    asupersync::test_utils::run_test(|| async {
        const ROW_COUNT: i64 = 100_000;
        const WARMUP: u32 = 20;
        const ITERATIONS: u32 = 200;
        const UNIQUE_ID: i64 = 75_000;
        const EXPECTED_PROBE_EXECUTIONS: u64 = WARMUP as u64 + ITERATIONS as u64;

        let _profile_guard = ManualHotPathProfileGuard::new();
        let conn = setup_query_guard_bench(ROW_COUNT).await;
        conn.execute("CREATE UNIQUE INDEX idx_bench_name_probe ON bench(name)")
            .await
            .unwrap();

        let indexed_stmt = conn
            .prepare("SELECT * FROM bench WHERE name = ?1")
            .await
            .unwrap();
        let indexed_params = [SqliteValue::Text(format!("name_{UNIQUE_ID}").into())];
        let warm_indexed = indexed_stmt
            .query_row_with_params(&indexed_params)
            .await
            .unwrap();
        assert_eq!(warm_indexed.values()[0], SqliteValue::Integer(UNIQUE_ID));
        reset_hot_path_profile();
        let (indexed_stats, indexed_throughput) =
            measure_operation_async(WARMUP, ITERATIONS, async || {
                let row = indexed_stmt
                    .query_row_with_params(&indexed_params)
                    .await
                    .unwrap();
                assert_eq!(row.values()[0], SqliteValue::Integer(UNIQUE_ID));
            })
            .await;
        let indexed_profile = hot_path_profile_snapshot();
        eprintln!(
            concat!(
                "manual_perf_probe.future_query_row.indexed_equality_100k ",
                "p50_us={} p95_us={} throughput_ops_per_sec={:.1} ",
                "fast={} slow={} direct_indexed_hits={} result_rows={} result_values={}"
            ),
            indexed_stats.p50_micros,
            indexed_stats.p95_micros,
            indexed_throughput,
            indexed_profile.parser.fast_path_executions,
            indexed_profile.parser.slow_path_executions,
            indexed_profile.direct_indexed_equality_query_hits,
            indexed_profile.vdbe.result_rows_total,
            indexed_profile.vdbe.result_values_total,
        );
        assert_eq!(
            indexed_profile.direct_indexed_equality_query_hits, EXPECTED_PROBE_EXECUTIONS,
            "corrected indexed-equality probe should hit the B4 direct query_row path on every warmup + measured execution"
        );
        assert_eq!(
            indexed_profile.vdbe.result_rows_total, 0,
            "corrected indexed-equality probe should not materialize VDBE result rows"
        );
        assert_eq!(
            indexed_profile.vdbe.result_values_total, 0,
            "corrected indexed-equality probe should not materialize VDBE result values"
        );

        let range_stmt = conn
            .prepare("SELECT * FROM bench WHERE id >= ?1 AND id < ?2")
            .await
            .unwrap();
        let range_params = [
            SqliteValue::Integer(UNIQUE_ID),
            SqliteValue::Integer(UNIQUE_ID + 1),
        ];
        let warm_range = range_stmt
            .query_row_with_params(&range_params)
            .await
            .unwrap();
        assert_eq!(warm_range.values()[0], SqliteValue::Integer(UNIQUE_ID));
        reset_hot_path_profile();
        let (range_stats, range_throughput) =
            measure_operation_async(WARMUP, ITERATIONS, async || {
                let row = range_stmt
                    .query_row_with_params(&range_params)
                    .await
                    .unwrap();
                assert_eq!(row.values()[0], SqliteValue::Integer(UNIQUE_ID));
            })
            .await;
        let range_profile = hot_path_profile_snapshot();
        eprintln!(
            concat!(
                "manual_perf_probe.future_query_row.rowid_range_100k ",
                "p50_us={} p95_us={} throughput_ops_per_sec={:.1} ",
                "fast={} slow={} direct_rowid_range_hits={} result_rows={} result_values={}"
            ),
            range_stats.p50_micros,
            range_stats.p95_micros,
            range_throughput,
            range_profile.parser.fast_path_executions,
            range_profile.parser.slow_path_executions,
            range_profile.direct_rowid_range_query_hits,
            range_profile.vdbe.result_rows_total,
            range_profile.vdbe.result_values_total,
        );
        assert_eq!(
            range_profile.direct_rowid_range_query_hits, EXPECTED_PROBE_EXECUTIONS,
            "corrected rowid-range probe should hit the B4 direct query_row path on every warmup + measured execution"
        );
        assert_eq!(
            range_profile.vdbe.result_rows_total, 0,
            "corrected rowid-range probe should not materialize VDBE result rows"
        );
        assert_eq!(
            range_profile.vdbe.result_values_total, 0,
            "corrected rowid-range probe should not materialize VDBE result values"
        );

        assert!(indexed_throughput > 0.0);
        assert!(range_throughput > 0.0);
    });
}

#[test]
#[ignore = "manual perf probe; run via rch when investigating large prepared INSERT regressions"]
fn manual_perf_probe_large_prepared_insert_single_transaction_10k() {
    asupersync::test_utils::run_test(|| async {
        const ROW_COUNT: i64 = 10_000;
        const MEASURED_RUNS: usize = 3;
        const CREATE_TABLE: &str = "CREATE TABLE bench (id INTEGER PRIMARY KEY, name TEXT, email TEXT, score INTEGER, created TEXT);";
        const INSERT_SQL: &str = "INSERT INTO bench VALUES (?1, ('name_' || ?1), ('user_' || ?1 || '@test.com'), (?1 * 7), ('2026-01-' || ((?1 % 28) + 1)))";

        fn run_csqlite_once() -> f64 {
            let conn = rusqlite::Connection::open_in_memory().unwrap();
            apply_manual_probe_pragmas_csqlite(&conn);
            conn.execute_batch(CREATE_TABLE).unwrap();
            conn.execute_batch("BEGIN").unwrap();
            let start = std::time::Instant::now();
            let mut stmt = conn.prepare(INSERT_SQL).unwrap();
            for i in 0..ROW_COUNT {
                stmt.execute(rusqlite::params![i]).unwrap();
            }
            conn.execute_batch("COMMIT").unwrap();
            let elapsed = start.elapsed();
            let count: i64 = conn
                .query_row("SELECT COUNT(*) FROM bench", [], |row| row.get(0))
                .unwrap();
            assert_eq!(count, ROW_COUNT);
            ROW_COUNT as f64 / elapsed.as_secs_f64()
        }

        async fn run_fsqlite_once() -> f64 {
            let conn = fsqlite::Connection::open(":memory:").await.unwrap();
            apply_manual_probe_pragmas_fsqlite(&conn).await;
            conn.execute(CREATE_TABLE).await.unwrap();
            conn.execute("BEGIN").await.unwrap();
            let stmt = conn.prepare(INSERT_SQL).await.unwrap();
            let start = std::time::Instant::now();
            for i in 0..ROW_COUNT {
                conn.execute_prepared_with_params(&stmt, &[SqliteValue::Integer(i)])
                    .await
                    .unwrap();
            }
            conn.execute("COMMIT").await.unwrap();
            let elapsed = start.elapsed();
            let rows = conn.query("SELECT COUNT(*) FROM bench").await.unwrap();
            assert_eq!(rows[0].values()[0], SqliteValue::Integer(ROW_COUNT));
            ROW_COUNT as f64 / elapsed.as_secs_f64()
        }

        let csqlite_runs: Vec<f64> = (0..MEASURED_RUNS).map(|_| run_csqlite_once()).collect();
        let mut fsqlite_runs: Vec<f64> = Vec::with_capacity(MEASURED_RUNS);
        for _ in 0..MEASURED_RUNS {
            fsqlite_runs.push(run_fsqlite_once().await);
        }

        let csqlite_median = median_f64(csqlite_runs.clone());
        let fsqlite_median = median_f64(fsqlite_runs.clone());

        eprintln!(
            "manual_perf_probe.large_prepared_insert_single_txn_10k.csqlite.samples={csqlite_runs:?} median_rows_per_sec={csqlite_median:.1}"
        );
        eprintln!(
            "manual_perf_probe.large_prepared_insert_single_txn_10k.frankensqlite.samples={fsqlite_runs:?} median_rows_per_sec={fsqlite_median:.1} ratio_vs_csqlite={:.4}",
            fsqlite_median / csqlite_median
        );

        assert!(csqlite_median > 0.0);
        assert!(fsqlite_median > 0.0);
    });
}

#[test]
#[ignore = "manual profile probe; run via rch when investigating large prepared INSERT runtime overhead"]
fn manual_hot_path_profile_large_prepared_insert_single_transaction_10k() {
    asupersync::test_utils::run_test(|| async {
        const ROW_COUNT: i64 = 10_000;
        const CREATE_TABLE: &str = "CREATE TABLE bench (id INTEGER PRIMARY KEY, name TEXT, email TEXT, score INTEGER, created TEXT);";
        const INSERT_SQL: &str = "INSERT INTO bench VALUES (?1, ('name_' || ?1), ('user_' || ?1 || '@test.com'), (?1 * 7), ('2026-01-' || ((?1 % 28) + 1)))";

        let _profile_guard = ManualHotPathProfileGuard::new();
        let conn = fsqlite::Connection::open(":memory:").await.unwrap();
        apply_manual_probe_pragmas_fsqlite(&conn).await;
        conn.execute(CREATE_TABLE).await.unwrap();
        conn.execute("BEGIN").await.unwrap();
        let stmt = conn.prepare(INSERT_SQL).await.unwrap();

        reset_hot_path_profile();
        let started = std::time::Instant::now();
        for i in 0..ROW_COUNT {
            conn.execute_prepared_with_params(&stmt, &[SqliteValue::Integer(i)])
                .await
                .unwrap();
        }
        conn.execute("COMMIT").await.unwrap();
        let wall = started.elapsed();

        let rows = conn.query("SELECT COUNT(*) FROM bench").await.unwrap();
        assert_eq!(rows[0].values()[0], SqliteValue::Integer(ROW_COUNT));

        let profile = hot_path_profile_snapshot();
        log_manual_insert_hot_path_profile("large_prepared_insert_single_txn_10k", wall, &profile);
    });
}

#[test]
#[ignore = "manual hot path profile; run via rch when validating small_3col autocommit insert micro-cuts"]
fn manual_hot_path_profile_small_prepared_insert_autocommit_10k() {
    asupersync::test_utils::run_test(|| async {
        const ROW_COUNT: i64 = 10_000;
        const CREATE_TABLE: &str =
            "CREATE TABLE bench (id INTEGER PRIMARY KEY, name TEXT NOT NULL, value REAL NOT NULL);";
        const INSERT_SQL: &str = "INSERT INTO bench VALUES (?1, ('user_' || ?1), (?1 * 0.137))";

        let _profile_guard = ManualHotPathProfileGuard::new();
        let conn = fsqlite::Connection::open(":memory:").await.unwrap();
        apply_manual_probe_pragmas_fsqlite(&conn).await;
        conn.execute(CREATE_TABLE).await.unwrap();
        let stmt = conn.prepare(INSERT_SQL).await.unwrap();

        reset_hot_path_profile();
        let started = std::time::Instant::now();
        for i in 0..ROW_COUNT {
            conn.execute_prepared_with_params(&stmt, &[SqliteValue::Integer(i)])
                .await
                .unwrap();
        }
        let wall = started.elapsed();

        let rows = conn.query("SELECT COUNT(*) FROM bench").await.unwrap();
        assert_eq!(rows[0].values()[0], SqliteValue::Integer(ROW_COUNT));

        let profile = hot_path_profile_snapshot();
        log_manual_insert_hot_path_profile("small_prepared_insert_autocommit_10k", wall, &profile);
    });
}

#[test]
#[ignore = "manual perf probe; run via rch when investigating write throughput"]
fn manual_perf_probe_write_10k_autocommit_prepared_and_ad_hoc() {
    asupersync::test_utils::run_test(|| async {
        const ROW_COUNT: i64 = 10_000;
        const MEASURED_RUNS: usize = 3;
        const CREATE_TABLE: &str =
            "CREATE TABLE bench (id INTEGER PRIMARY KEY, data TEXT, value REAL);";
        const INSERT_SQL: &str = "INSERT INTO bench VALUES (?1, ('data_' || ?1), (?1 * 0.137));";

        fn run_csqlite_prepared_once() -> f64 {
            let conn = rusqlite::Connection::open_in_memory().unwrap();
            apply_manual_probe_pragmas_csqlite(&conn);
            conn.execute_batch(CREATE_TABLE).unwrap();
            let start = std::time::Instant::now();
            let mut stmt = conn.prepare(INSERT_SQL).unwrap();
            for i in 0..ROW_COUNT {
                stmt.execute(rusqlite::params![i]).unwrap();
            }
            let elapsed = start.elapsed();
            let count: i64 = conn
                .query_row("SELECT COUNT(*) FROM bench", [], |row| row.get(0))
                .unwrap();
            assert_eq!(count, ROW_COUNT);
            ROW_COUNT as f64 / elapsed.as_secs_f64()
        }

        fn run_csqlite_ad_hoc_once() -> f64 {
            let conn = rusqlite::Connection::open_in_memory().unwrap();
            apply_manual_probe_pragmas_csqlite(&conn);
            conn.execute_batch(CREATE_TABLE).unwrap();
            let start = std::time::Instant::now();
            for i in 0..ROW_COUNT {
                conn.execute(INSERT_SQL, rusqlite::params![i]).unwrap();
            }
            let elapsed = start.elapsed();
            let count: i64 = conn
                .query_row("SELECT COUNT(*) FROM bench", [], |row| row.get(0))
                .unwrap();
            assert_eq!(count, ROW_COUNT);
            ROW_COUNT as f64 / elapsed.as_secs_f64()
        }

        async fn run_fsqlite_prepared_once() -> f64 {
            let conn = fsqlite::Connection::open(":memory:").await.unwrap();
            apply_manual_probe_pragmas_fsqlite(&conn).await;
            conn.execute(CREATE_TABLE).await.unwrap();
            let stmt = conn.prepare(INSERT_SQL).await.unwrap();
            let start = std::time::Instant::now();
            for i in 0..ROW_COUNT {
                stmt.execute_with_params(&[fsqlite_types::value::SqliteValue::Integer(i)])
                    .await
                    .unwrap();
            }
            let elapsed = start.elapsed();
            let rows = conn.query("SELECT COUNT(*) FROM bench").await.unwrap();
            assert_eq!(
                rows[0].values()[0],
                fsqlite_types::value::SqliteValue::Integer(ROW_COUNT)
            );
            ROW_COUNT as f64 / elapsed.as_secs_f64()
        }

        async fn run_fsqlite_ad_hoc_once() -> f64 {
            let conn = fsqlite::Connection::open(":memory:").await.unwrap();
            apply_manual_probe_pragmas_fsqlite(&conn).await;
            conn.execute(CREATE_TABLE).await.unwrap();
            let start = std::time::Instant::now();
            for i in 0..ROW_COUNT {
                conn.execute_with_params(
                    INSERT_SQL,
                    &[fsqlite_types::value::SqliteValue::Integer(i)],
                )
                .await
                .unwrap();
            }
            let elapsed = start.elapsed();
            let rows = conn.query("SELECT COUNT(*) FROM bench").await.unwrap();
            assert_eq!(
                rows[0].values()[0],
                fsqlite_types::value::SqliteValue::Integer(ROW_COUNT)
            );
            ROW_COUNT as f64 / elapsed.as_secs_f64()
        }

        let csqlite_prepared: Vec<f64> = (0..MEASURED_RUNS)
            .map(|_| run_csqlite_prepared_once())
            .collect();
        let csqlite_ad_hoc: Vec<f64> = (0..MEASURED_RUNS)
            .map(|_| run_csqlite_ad_hoc_once())
            .collect();
        let mut fsqlite_prepared: Vec<f64> = Vec::with_capacity(MEASURED_RUNS);
        for _ in 0..MEASURED_RUNS {
            fsqlite_prepared.push(run_fsqlite_prepared_once().await);
        }
        let mut fsqlite_ad_hoc: Vec<f64> = Vec::with_capacity(MEASURED_RUNS);
        for _ in 0..MEASURED_RUNS {
            fsqlite_ad_hoc.push(run_fsqlite_ad_hoc_once().await);
        }

        let csqlite_prepared_median = median_f64(csqlite_prepared.clone());
        let csqlite_ad_hoc_median = median_f64(csqlite_ad_hoc.clone());
        let fsqlite_prepared_median = median_f64(fsqlite_prepared.clone());
        let fsqlite_ad_hoc_median = median_f64(fsqlite_ad_hoc.clone());

        eprintln!(
            "manual_perf_probe.write_10k_autocommit.csqlite_prepared.samples={csqlite_prepared:?} median_rows_per_sec={csqlite_prepared_median:.1}"
        );
        eprintln!(
            "manual_perf_probe.write_10k_autocommit.csqlite_ad_hoc.samples={csqlite_ad_hoc:?} median_rows_per_sec={csqlite_ad_hoc_median:.1}"
        );
        eprintln!(
            "manual_perf_probe.write_10k_autocommit.fsqlite_prepared.samples={fsqlite_prepared:?} median_rows_per_sec={fsqlite_prepared_median:.1} ratio_vs_csqlite={:.4}",
            fsqlite_prepared_median / csqlite_prepared_median
        );
        eprintln!(
            "manual_perf_probe.write_10k_autocommit.fsqlite_ad_hoc.samples={fsqlite_ad_hoc:?} median_rows_per_sec={fsqlite_ad_hoc_median:.1} ratio_vs_csqlite={:.4}",
            fsqlite_ad_hoc_median / csqlite_prepared_median
        );

        assert!(csqlite_prepared_median > 0.0);
        assert!(csqlite_ad_hoc_median > 0.0);
        assert!(fsqlite_prepared_median > 0.0);
        assert!(fsqlite_ad_hoc_median > 0.0);
    });
}

#[test]
#[ignore = "manual perf probe; run via rch when measuring UNIQUE secondary-index insert maintenance"]
fn manual_perf_probe_write_10k_autocommit_prepared_unique_email_index() {
    asupersync::test_utils::run_test(|| async {
        const ROW_COUNT: i64 = 10_000;
        const MEASURED_RUNS: usize = 3;
        const CREATE_TABLE: &str = "CREATE TABLE bench (id INTEGER PRIMARY KEY, name TEXT, email TEXT, score INTEGER, created TEXT);";
        const CREATE_INDEX: &str = "CREATE UNIQUE INDEX idx_bench_email_unique ON bench(email);";
        const INSERT_SQL: &str = "INSERT INTO bench VALUES (?1, ('name_' || ?1), ('user_' || ?1 || '@test.com'), (?1 * 7), ('2026-01-' || ((?1 % 28) + 1)))";

        fn run_csqlite_prepared_once() -> f64 {
            let conn = rusqlite::Connection::open_in_memory().unwrap();
            apply_manual_probe_pragmas_csqlite(&conn);
            conn.execute_batch(CREATE_TABLE).unwrap();
            conn.execute_batch(CREATE_INDEX).unwrap();
            let start = std::time::Instant::now();
            let mut stmt = conn.prepare(INSERT_SQL).unwrap();
            for i in 0..ROW_COUNT {
                stmt.execute(rusqlite::params![i]).unwrap();
            }
            let elapsed = start.elapsed();
            let count: i64 = conn
                .query_row("SELECT COUNT(*) FROM bench", [], |row| row.get(0))
                .unwrap();
            assert_eq!(count, ROW_COUNT);
            ROW_COUNT as f64 / elapsed.as_secs_f64()
        }

        async fn run_fsqlite_prepared_once() -> f64 {
            let conn = fsqlite::Connection::open(":memory:").await.unwrap();
            apply_manual_probe_pragmas_fsqlite(&conn).await;
            conn.execute(CREATE_TABLE).await.unwrap();
            conn.execute(CREATE_INDEX).await.unwrap();
            let stmt = conn.prepare(INSERT_SQL).await.unwrap();
            let start = std::time::Instant::now();
            for i in 0..ROW_COUNT {
                stmt.execute_with_params(&[fsqlite_types::value::SqliteValue::Integer(i)])
                    .await
                    .unwrap();
            }
            let elapsed = start.elapsed();
            let rows = conn.query("SELECT COUNT(*) FROM bench").await.unwrap();
            assert_eq!(
                rows[0].values()[0],
                fsqlite_types::value::SqliteValue::Integer(ROW_COUNT)
            );
            ROW_COUNT as f64 / elapsed.as_secs_f64()
        }

        let csqlite_prepared: Vec<f64> = (0..MEASURED_RUNS)
            .map(|_| run_csqlite_prepared_once())
            .collect();
        let mut fsqlite_prepared: Vec<f64> = Vec::with_capacity(MEASURED_RUNS);
        for _ in 0..MEASURED_RUNS {
            fsqlite_prepared.push(run_fsqlite_prepared_once().await);
        }

        let csqlite_prepared_median = median_f64(csqlite_prepared.clone());
        let fsqlite_prepared_median = median_f64(fsqlite_prepared.clone());

        eprintln!(
            "manual_perf_probe.write_10k_autocommit_prepared_unique_email_index.csqlite_prepared.samples={csqlite_prepared:?} median_rows_per_sec={csqlite_prepared_median:.1}"
        );
        eprintln!(
            "manual_perf_probe.write_10k_autocommit_prepared_unique_email_index.frankensqlite_prepared.samples={fsqlite_prepared:?} median_rows_per_sec={fsqlite_prepared_median:.1} ratio_vs_csqlite={:.4}",
            fsqlite_prepared_median / csqlite_prepared_median
        );

        assert!(csqlite_prepared_median > 0.0);
        assert!(fsqlite_prepared_median > 0.0);
    });
}

#[test]
#[ignore = "manual perf probe; run via rch when investigating repeated prepare reuse"]
fn manual_perf_probe_prepare_cache_reuse_vs_unique_sql_variants() {
    asupersync::test_utils::run_test(|| async {
    const ROW_COUNT: i64 = 10_000;
    const INSERT_SQL: &str = "INSERT INTO bench VALUES (?1, ('data_' || ?1), (?1 * 0.137))";

    let unique_sqls: Vec<String> = (0..ROW_COUNT)
        .map(|i| format!("{INSERT_SQL} -- prepare-cache-miss-{i}"))
        .collect();

    let reused = run_fsqlite_prepare_cache_probe(
        std::iter::repeat_n(INSERT_SQL, usize::try_from(ROW_COUNT).unwrap()),
        ROW_COUNT,
    )
    .await;
    let unique =
        run_fsqlite_prepare_cache_probe(unique_sqls.iter().map(String::as_str), ROW_COUNT).await;

    eprintln!(
        "manual_perf_probe.prepare_cache_reuse.reused rows_per_sec={:.1} parse_cache_hit={} parse_cache_miss={} compiled_cache_hit={} compiled_cache_miss={} prepared_cache_hit={} prepared_cache_miss={}",
        reused.rows_per_sec,
        reused.parse_cache_hits,
        reused.parse_cache_misses,
        reused.compiled_cache_hits,
        reused.compiled_cache_misses,
        reused.prepared_cache_hits,
        reused.prepared_cache_misses,
    );
    eprintln!(
        "manual_perf_probe.prepare_cache_reuse.unique_sql rows_per_sec={:.1} parse_cache_hit={} parse_cache_miss={} compiled_cache_hit={} compiled_cache_miss={} prepared_cache_hit={} prepared_cache_miss={}",
        unique.rows_per_sec,
        unique.parse_cache_hits,
        unique.parse_cache_misses,
        unique.compiled_cache_hits,
        unique.compiled_cache_misses,
        unique.prepared_cache_hits,
        unique.prepared_cache_misses,
    );
    eprintln!(
        "manual_perf_probe.prepare_cache_reuse.ratio reused_vs_unique={:.4}",
        reused.rows_per_sec / unique.rows_per_sec
    );

    assert!(reused.rows_per_sec > 0.0);
    assert!(unique.rows_per_sec > 0.0);
    assert_eq!(reused.prepared_cache_misses, 1);
    assert!(
        reused.prepared_cache_hits >= u64::try_from(ROW_COUNT - 1).unwrap(),
        "stable SQL should hit the prepared cache after the first prepare: {reused:?}"
    );
    assert_eq!(unique.prepared_cache_hits, 0);
    assert_eq!(
        unique.prepared_cache_misses,
        u64::try_from(ROW_COUNT).unwrap()
    );
    assert!(
        reused.rows_per_sec > unique.rows_per_sec,
        "prepared-cache reuse should outperform forced unique-SQL misses: reused={reused:?} unique={unique:?}"
    );
    });
}

#[test]
fn record_decode_cache_repeated_column_reads_reduce_record_decodes_per_row() {
    if run_hot_path_test_in_isolated_process() {
        return;
    }

    asupersync::test_utils::run_test(|| async {
        const ITERATIONS: usize = 2_000;
        const REPEATED_COLUMN_SQL: &str =
            "SELECT data, data, data, data, data FROM bench WHERE id = 1;";

        let repeated = run_fsqlite_decode_cache_probe(REPEATED_COLUMN_SQL, ITERATIONS, 1, 1).await;

        eprintln!(
            "record_decode_cache.repeated_column_reads rows_per_sec={:.1} decode_cache_hit={} decode_cache_miss={} invalidation_position={} invalidation_write={} invalidation_pseudo={} record_decodes_per_row={:.3}",
            repeated.rows_per_sec,
            repeated.decode_cache_hits,
            repeated.decode_cache_misses,
            repeated.decode_cache_invalidations_position,
            repeated.decode_cache_invalidations_write,
            repeated.decode_cache_invalidations_pseudo,
            repeated.record_decodes_per_row,
        );

        assert!(repeated.rows_per_sec > 0.0);
        assert!(
            repeated.decode_cache_hits > repeated.decode_cache_misses,
            "repeated-column query should produce more decode-cache hits than misses: {repeated:?}"
        );
        assert!(
            repeated.record_decodes_per_row <= 1.5,
            "repeated-column query should keep record decodes near one per returned row: {repeated:?}"
        );
        assert_eq!(repeated.decode_cache_invalidations_position, 0);
        assert_eq!(repeated.decode_cache_invalidations_write, 0);
        assert_eq!(repeated.decode_cache_invalidations_pseudo, 0);
    });
}

#[test]
fn record_decode_cache_multi_row_repeated_columns_hold_one_decode_per_row() {
    if run_hot_path_test_in_isolated_process() {
        return;
    }

    asupersync::test_utils::run_test(|| async {
        const ITERATIONS: usize = 250;
        const MULTI_ROW_SQL: &str =
            "SELECT id, data, data, data FROM bench WHERE id BETWEEN 1 AND 16 ORDER BY id;";

        let repeated = run_fsqlite_decode_cache_probe(MULTI_ROW_SQL, ITERATIONS, 32, 16).await;

        eprintln!(
            "record_decode_cache.multi_row_repeated_columns rows_per_sec={:.1} decode_cache_hit={} decode_cache_miss={} invalidation_position={} invalidation_write={} invalidation_pseudo={} record_decodes_per_row={:.3}",
            repeated.rows_per_sec,
            repeated.decode_cache_hits,
            repeated.decode_cache_misses,
            repeated.decode_cache_invalidations_position,
            repeated.decode_cache_invalidations_write,
            repeated.decode_cache_invalidations_pseudo,
            repeated.record_decodes_per_row,
        );

        assert!(repeated.rows_per_sec > 0.0);
        assert!(
            repeated.decode_cache_hits > repeated.decode_cache_misses,
            "multi-row repeated-column query should produce more decode-cache hits than misses: {repeated:?}"
        );
        assert!(
            repeated.record_decodes_per_row <= 1.1,
            "multi-row repeated-column query should stay near one decode miss per returned row: {repeated:?}"
        );
        // Some execution routes replace/drop the per-row decode scratch instead of
        // retaining a cursor cache and invalidating it on movement. The stable
        // contract for this probe is the per-row miss rate plus repeated-column
        // hits, not a specific invalidation telemetry path.
        assert_eq!(repeated.decode_cache_invalidations_write, 0);
        assert_eq!(repeated.decode_cache_invalidations_pseudo, 0);
    });
}
