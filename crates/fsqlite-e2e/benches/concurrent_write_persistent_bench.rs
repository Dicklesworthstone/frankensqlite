//! Benchmark: Real persistent concurrent-writer throughput.
//!
//! Bead: bd-l9k8e.8 (C8)
//!
//! THIS IS THE ONLY BENCHMARK THAT MATTERS.
//!
//! FrankenSQLite's thesis: page-level MVCC enables concurrent writers where
//! SQLite serializes them.  This benchmark measures:
//!
//! - N writer threads (2, 4, 8, 16)
//! - Each writer INSERTs into a DIFFERENT table (guaranteeing different pages)
//! - File-backed database with WAL mode
//! - Prepared statements on both sides
//! - `PRAGMA busy_timeout=0` on both engines so contention is measured by the
//!   harness rather than hidden inside engine-level sleeps
//!
//! Success criterion: FrankenSQLite shows >1.5x throughput over SQLite at N>=4
//! writers for non-conflicting workloads.  Theoretical improvement is Nx.
//!
//! Metrics captured:
//! - Wall-clock throughput (ops/sec) at each thread count
//! - Per-operation commit-stage latency histogram (p50, p99, max)
//! - Conflict/retry event count and affected-operation rate
//!
//! Optional machine-readable capture:
//! - Set `FSQLITE_PERSISTENT_PHASE_ATTRIBUTION_DIR=/path/to/dir`
//! - The benchmark writes `provenance.json` once, appends per-iteration
//!   records to `samples.jsonl`, and refreshes paired SQLite-vs-FrankenSQLite
//!   `component_comparison.{json,md}` artifacts without changing default
//!   stderr output

use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use fsqlite::{FrankenError, SqliteValue};
use fsqlite_e2e::persistent_phase_audit::{
    PERSISTENT_PHASE_COMPONENT_COMPARISON_SUITE_SCHEMA_V1, PersistentLatencySummary,
    PersistentMeasuredCommitSubBuckets, PersistentOperationTiming,
    PersistentOperationWallTimeAudit, PersistentPhaseComponentComparisonSuite,
    PersistentRetryStageCounts, build_measured_commit_sub_buckets, build_operation_wall_time_audit,
    build_persistent_phase_component_comparison_report, format_operation_wall_time_audit,
    persistent_latency_summary, render_persistent_phase_component_comparison_suite_markdown,
    sleep_with_accounting,
};
use fsqlite_wal::ConsolidationMetricsSnapshot;
use serde::{Deserialize, Serialize};

const ROWS_PER_THREAD: i64 = 1000;
/// Maximum retries before giving up on a transaction (applies to both engines).
///
/// With the deliberately zero SQLite busy timeout, 100 retries represented
/// only 10 ms of backoff. A preempted lock holder on a saturated benchmark host
/// could therefore make a healthy peer abort before it was scheduled again.
/// Ten seconds of bounded backoff preserves fail-fast behavior for a genuine
/// wedge without turning ordinary scheduler latency into a benchmark failure.
const MAX_TXN_RETRIES: u32 = 100_000;
const RETRY_BACKOFF: Duration = Duration::from_micros(100);
const PERSISTENT_PHASE_CAPTURE_DIR_ENV: &str = "FSQLITE_PERSISTENT_PHASE_ATTRIBUTION_DIR";
const PERSISTENT_PHASE_CAPTURE_PROVENANCE_SCHEMA_V1: &str =
    "fsqlite-e2e.persistent_phase_capture_provenance.v1";
const PERSISTENT_PHASE_CAPTURE_SAMPLE_SCHEMA_V3: &str =
    "fsqlite-e2e.persistent_phase_capture_sample.v3";
const SQLITE_ENGINE_ID: &str = "sqlite3";
const FSQLITE_ENGINE_ID: &str = "fsqlite_mvcc";

// ─── PRAGMA helpers ─────────────────────────────────────────────────────

fn run_fsqlite_pragma(conn: &fsqlite::Connection, pragma: &str) {
    fsqlite_e2e::block_on(conn.execute(pragma))
        .unwrap_or_else(|error| panic!("failed to execute benchmark pragma `{pragma}`: {error:?}"));
}

fn rollback_fsqlite(conn: &fsqlite::Connection, context: &str) {
    fsqlite_e2e::block_on(conn.execute("ROLLBACK")).unwrap_or_else(|error| {
        panic!("failed to roll back FrankenSQLite transaction after {context}: {error:?}")
    });
}

fn apply_setup_pragmas_fsqlite(conn: &fsqlite::Connection) {
    for pragma in [
        "PRAGMA page_size = 4096;",
        "PRAGMA journal_mode = WAL;",
        "PRAGMA synchronous = NORMAL;",
        "PRAGMA cache_size = -64000;",
        "PRAGMA busy_timeout = 0;",
        "PRAGMA fsqlite.concurrent_mode = ON;",
    ] {
        run_fsqlite_pragma(conn, pragma);
    }
}

fn apply_session_pragmas_fsqlite(conn: &fsqlite::Connection) {
    for pragma in [
        "PRAGMA journal_mode = WAL;",
        "PRAGMA synchronous = NORMAL;",
        "PRAGMA cache_size = -64000;",
        "PRAGMA busy_timeout = 0;",
        "PRAGMA fsqlite.concurrent_mode = ON;",
    ] {
        run_fsqlite_pragma(conn, pragma);
    }
}

fn is_retryable_fsqlite_error(error: &FrankenError) -> bool {
    matches!(
        error,
        FrankenError::Busy | FrankenError::BusyRecovery | FrankenError::BusySnapshot { .. }
    )
}

fn is_duplicate_insert_after_retry(error: &FrankenError) -> bool {
    // Check for proper constraint errors
    if matches!(
        error,
        FrankenError::PrimaryKeyViolation | FrankenError::UniqueViolation { .. }
    ) {
        return true;
    }
    // Also check for VDBE constraint errors (code 19) wrapped as Internal
    if let FrankenError::Internal(msg) = error {
        if msg.contains("code 19:") && msg.contains("PRIMARY KEY") {
            return true;
        }
        if msg.contains("code 19:") && msg.contains("UNIQUE") {
            return true;
        }
    }
    false
}

fn is_corruption_error(error: &FrankenError) -> bool {
    matches!(
        error,
        FrankenError::DatabaseCorrupt { .. } | FrankenError::WalCorrupt { .. }
    )
}

fn create_table_sql(table_id: usize) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS bench_{table_id} (id INTEGER PRIMARY KEY, name TEXT, score INTEGER);"
    )
}

fn insert_sql(table_id: usize) -> String {
    format!("INSERT INTO bench_{table_id} VALUES (?1, ('t' || ?1), (?1 * 7));")
}

fn criterion_config() -> Criterion {
    Criterion::default().configure_from_args()
}

#[derive(Debug, Clone, Serialize)]
struct PersistentBenchmarkMetrics {
    total_ops: u64,
    run_wall_ms: u64,
    throughput_ops_per_sec: f64,
    transaction_latency_us: PersistentLatencySummary,
    commit_latency_us: PersistentLatencySummary,
    contention_event_count: u64,
    contention_events_per_op: f64,
    operations_with_contention: u64,
    contention_operation_rate_percent: f64,
    operation_wall_time_audit: PersistentOperationWallTimeAudit,
}

#[derive(Debug, Clone, Serialize)]
struct PersistentPhaseCaptureProvenance {
    schema_version: &'static str,
    benchmark: &'static str,
    output_dir_env: &'static str,
    rows_per_thread: i64,
    max_txn_retries: u32,
    current_dir: String,
    current_exe: Option<String>,
    argv: Vec<String>,
    hostname: Option<String>,
    kernel_release: Option<String>,
    criterion_emission_scope: &'static str,
}

#[derive(Debug, Clone, Serialize)]
struct PersistentPhaseCaptureSample {
    schema_version: &'static str,
    timestamp_unix_ms: u64,
    benchmark_group: String,
    engine: &'static str,
    contention_label: &'static str,
    concurrency: usize,
    rows_per_thread: i64,
    total_rows: u64,
    metrics: PersistentBenchmarkMetrics,
    phase_metrics: Option<ConsolidationMetricsSnapshot>,
    phase_timing_report: Option<String>,
    flusher_lock_wait_fraction_basis_points: Option<u64>,
    lock_topology_limited: Option<bool>,
}

#[derive(Debug, Clone, Deserialize)]
struct PersistentPhaseCaptureArtifactMetrics {
    throughput_ops_per_sec: f64,
    operation_wall_time_audit: PersistentOperationWallTimeAudit,
}

#[derive(Debug, Clone, Deserialize)]
struct PersistentPhaseCaptureArtifactSample {
    timestamp_unix_ms: u64,
    benchmark_group: String,
    engine: String,
    concurrency: usize,
    rows_per_thread: i64,
    total_rows: u64,
    metrics: PersistentPhaseCaptureArtifactMetrics,
    flusher_lock_wait_fraction_basis_points: Option<u64>,
    lock_topology_limited: Option<bool>,
}

fn duration_ms_u64(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

#[allow(clippy::cast_precision_loss)]
fn ratio(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        return 0.0;
    }
    numerator as f64 / denominator as f64
}

#[allow(clippy::cast_precision_loss)]
fn throughput_ops_per_sec(total_ops: u64, run_wall: Duration) -> f64 {
    let seconds = run_wall.as_secs_f64();
    if seconds <= f64::EPSILON {
        return 0.0;
    }
    total_ops as f64 / seconds
}

fn collect_sorted_latencies(
    operation_timings: &[PersistentOperationTiming],
    bucket: impl Fn(&PersistentOperationTiming) -> Duration,
    skip_zero: bool,
) -> Vec<Duration> {
    let mut latencies: Vec<Duration> = operation_timings.iter().map(bucket).collect();
    if skip_zero {
        latencies.retain(|latency| !latency.is_zero());
    }
    latencies.sort();
    latencies
}

fn build_benchmark_metrics(
    total_ops: u64,
    run_wall: Duration,
    operation_timings: &[PersistentOperationTiming],
    retry_stage_counts: PersistentRetryStageCounts,
    measured_commit_sub_buckets: Option<PersistentMeasuredCommitSubBuckets>,
    contention_event_count: u64,
    operations_with_contention: u64,
) -> PersistentBenchmarkMetrics {
    let transaction_latencies =
        collect_sorted_latencies(operation_timings, |timing| timing.wall_time, false);
    let commit_latencies =
        collect_sorted_latencies(operation_timings, |timing| timing.commit_roundtrip, true);
    let operation_wall_time_audit = build_operation_wall_time_audit(
        operation_timings,
        retry_stage_counts,
        measured_commit_sub_buckets,
    );

    PersistentBenchmarkMetrics {
        total_ops,
        run_wall_ms: duration_ms_u64(run_wall),
        throughput_ops_per_sec: throughput_ops_per_sec(total_ops, run_wall),
        transaction_latency_us: persistent_latency_summary(&transaction_latencies),
        commit_latency_us: persistent_latency_summary(&commit_latencies),
        contention_event_count,
        contention_events_per_op: ratio(contention_event_count, total_ops),
        operations_with_contention,
        contention_operation_rate_percent: ratio(operations_with_contention, total_ops) * 100.0,
        operation_wall_time_audit,
    }
}

fn log_benchmark_metrics(
    engine_label: &str,
    n_threads: usize,
    contention_label: &str,
    metrics: &PersistentBenchmarkMetrics,
) {
    eprintln!(
        "[{engine_label} {n_threads}t] throughput={:.2} ops/s, txn_p50={}us, txn_p99={}us, commit_p50={}us, commit_p99={}us, {}_events={}, {}_events/op={:.3}, impacted_ops={}/{} ({:.2}%)",
        metrics.throughput_ops_per_sec,
        metrics.transaction_latency_us.p50_us,
        metrics.transaction_latency_us.p99_us,
        metrics.commit_latency_us.p50_us,
        metrics.commit_latency_us.p99_us,
        contention_label,
        metrics.contention_event_count,
        contention_label,
        metrics.contention_events_per_op,
        metrics.operations_with_contention,
        metrics.total_ops,
        metrics.contention_operation_rate_percent,
    );
}

fn persistent_phase_capture_dir() -> Option<PathBuf> {
    std::env::var_os(PERSISTENT_PHASE_CAPTURE_DIR_ENV)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

fn read_trimmed_file(path: &str) -> Option<String> {
    fs::read_to_string(path)
        .ok()
        .map(|contents| contents.trim().to_owned())
        .filter(|contents| !contents.is_empty())
}

fn persistent_phase_capture_provenance() -> PersistentPhaseCaptureProvenance {
    PersistentPhaseCaptureProvenance {
        schema_version: PERSISTENT_PHASE_CAPTURE_PROVENANCE_SCHEMA_V1,
        benchmark: "concurrent_write_persistent_bench",
        output_dir_env: PERSISTENT_PHASE_CAPTURE_DIR_ENV,
        rows_per_thread: ROWS_PER_THREAD,
        max_txn_retries: MAX_TXN_RETRIES,
        current_dir: std::env::current_dir()
            .map(|path| path.display().to_string())
            .unwrap_or_else(|_| ".".to_owned()),
        current_exe: std::env::current_exe()
            .ok()
            .map(|path| path.display().to_string()),
        argv: std::env::args_os()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect(),
        hostname: std::env::var("HOSTNAME")
            .ok()
            .filter(|hostname| !hostname.is_empty())
            .or_else(|| read_trimmed_file("/etc/hostname")),
        kernel_release: read_trimmed_file("/proc/sys/kernel/osrelease"),
        criterion_emission_scope: "every completed Criterion batched iteration appends one record; warmup and measurement phases are not distinguished by this harness",
    }
}

fn ensure_persistent_phase_capture_provenance(output_dir: &Path) -> std::io::Result<()> {
    fs::create_dir_all(output_dir)?;
    let provenance_path = output_dir.join("provenance.json");
    if provenance_path.exists() {
        return Ok(());
    }
    let payload = serde_json::to_string_pretty(&persistent_phase_capture_provenance())
        .map_err(std::io::Error::other)?;
    fs::write(provenance_path, payload.as_bytes())
}

fn unix_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| u64::try_from(duration.as_millis()).ok())
        .unwrap_or(0)
}

fn flusher_lock_wait_fraction_basis_points(metrics: &ConsolidationMetricsSnapshot) -> Option<u64> {
    let lock_wait_total = metrics.flusher_lock_wait_us_total();
    let wal_service_total = metrics.wal_service_us_total();
    let total = lock_wait_total.saturating_add(wal_service_total);
    (total > 0).then(|| lock_wait_total.saturating_mul(10_000) / total)
}

fn persistent_phase_base_group(benchmark_group: &str) -> &str {
    benchmark_group
        .split_once('/')
        .map_or(benchmark_group, |(base_group, _)| base_group)
}

fn refresh_persistent_phase_component_comparison_artifacts(
    output_dir: &Path,
) -> std::io::Result<()> {
    type ComparisonKey = (String, usize, i64, u64);

    let sample_path = output_dir.join("samples.jsonl");
    if !sample_path.exists() {
        return Ok(());
    }

    let sample_file = fs::File::open(&sample_path)?;
    let reader = BufReader::new(sample_file);
    let mut sqlite_samples: BTreeMap<ComparisonKey, PersistentPhaseCaptureArtifactSample> =
        BTreeMap::new();
    let mut fsqlite_samples: BTreeMap<ComparisonKey, PersistentPhaseCaptureArtifactSample> =
        BTreeMap::new();

    for (line_index, line_result) in reader.lines().enumerate() {
        let line = line_result?;
        if line.trim().is_empty() {
            continue;
        }

        let raw_value: serde_json::Value = serde_json::from_str(&line).map_err(|error| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "failed to parse {} line {} as JSON: {error}",
                    sample_path.display(),
                    line_index + 1
                ),
            )
        })?;
        let Some(schema_version) = raw_value
            .get("schema_version")
            .and_then(serde_json::Value::as_str)
        else {
            continue;
        };
        if schema_version != PERSISTENT_PHASE_CAPTURE_SAMPLE_SCHEMA_V3 {
            continue;
        }

        let sample: PersistentPhaseCaptureArtifactSample = serde_json::from_value(raw_value)
            .map_err(|error| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "failed to decode {} line {} as persistent phase sample: {error}",
                        sample_path.display(),
                        line_index + 1
                    ),
                )
            })?;
        let key = (
            persistent_phase_base_group(&sample.benchmark_group).to_owned(),
            sample.concurrency,
            sample.rows_per_thread,
            sample.total_rows,
        );
        let sample_map = match sample.engine.as_str() {
            SQLITE_ENGINE_ID => &mut sqlite_samples,
            FSQLITE_ENGINE_ID => &mut fsqlite_samples,
            _ => continue,
        };
        let should_replace = sample_map
            .get(&key)
            .is_none_or(|existing| sample.timestamp_unix_ms >= existing.timestamp_unix_ms);
        if should_replace {
            sample_map.insert(key, sample);
        }
    }

    let reports = sqlite_samples
        .into_iter()
        .filter_map(|(key, sqlite_sample)| {
            let fsqlite_sample = fsqlite_samples.get(&key)?;
            let (benchmark_group, concurrency, rows_per_thread, total_rows) = key;
            Some(build_persistent_phase_component_comparison_report(
                &benchmark_group,
                concurrency,
                rows_per_thread,
                total_rows,
                sqlite_sample.metrics.throughput_ops_per_sec,
                fsqlite_sample.metrics.throughput_ops_per_sec,
                &sqlite_sample.metrics.operation_wall_time_audit,
                &fsqlite_sample.metrics.operation_wall_time_audit,
                fsqlite_sample.flusher_lock_wait_fraction_basis_points,
                fsqlite_sample.lock_topology_limited,
            ))
        })
        .collect();

    let suite = PersistentPhaseComponentComparisonSuite {
        schema_version: PERSISTENT_PHASE_COMPONENT_COMPARISON_SUITE_SCHEMA_V1.to_owned(),
        reports,
    };
    let json_path = output_dir.join("component_comparison.json");
    let markdown_path = output_dir.join("component_comparison.md");
    let json_payload = serde_json::to_string_pretty(&suite).map_err(std::io::Error::other)?;
    let markdown_payload = render_persistent_phase_component_comparison_suite_markdown(&suite);
    fs::write(json_path, json_payload.as_bytes())?;
    fs::write(markdown_path, markdown_payload.as_bytes())
}

fn maybe_write_persistent_phase_capture(sample: &PersistentPhaseCaptureSample) {
    let Some(output_dir) = persistent_phase_capture_dir() else {
        return;
    };
    if let Err(error) = ensure_persistent_phase_capture_provenance(&output_dir) {
        eprintln!(
            "[persistent phase capture] failed to write provenance in {}: {error}",
            output_dir.display()
        );
        return;
    }
    let sample_path = output_dir.join("samples.jsonl");
    let encoded = match serde_json::to_string(sample) {
        Ok(encoded) => encoded,
        Err(error) => {
            eprintln!("[persistent phase capture] failed to serialize sample: {error}");
            return;
        }
    };
    let write_result = (|| -> std::io::Result<()> {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&sample_path)?;
        writeln!(file, "{encoded}")?;
        Ok(())
    })();
    if let Err(error) = write_result {
        eprintln!(
            "[persistent phase capture] failed to append {}: {error}",
            sample_path.display()
        );
        return;
    }
    if let Err(error) = refresh_persistent_phase_component_comparison_artifacts(&output_dir) {
        eprintln!(
            "[persistent phase capture] failed to refresh comparison artifacts in {}: {error}",
            output_dir.display()
        );
    }
}

// ─── C SQLite concurrent writers (file-backed WAL) ──────────────────────

// BENCH-META: engine=csqlite, lifecycle=prepared, storage=file, concurrency=concurrent
// BENCH-META: engine=frankensqlite, lifecycle=prepared, storage=file, concurrency=concurrent
fn bench_concurrent_csqlite_persistent(c: &mut Criterion, n_threads: usize, label: &str) {
    #[allow(clippy::cast_possible_wrap)]
    let total_rows = n_threads as u64 * ROWS_PER_THREAD as u64;
    let mut group = c.benchmark_group(label);
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(45));
    group.throughput(Throughput::Elements(total_rows));

    group.bench_function("csqlite_concurrent_persistent", |b| {
        b.iter_batched(
            || {
                let tmp = tempfile::NamedTempFile::new().unwrap();
                let path = tmp.path().to_str().unwrap().to_owned();
                {
                    let setup = rusqlite::Connection::open(&path).unwrap();
                    setup
                        .execute_batch(
                            "PRAGMA page_size = 4096;\
                             PRAGMA journal_mode = WAL;\
                             PRAGMA synchronous = NORMAL;\
                             PRAGMA cache_size = -64000;\
                             PRAGMA busy_timeout = 0;",
                        )
                        .unwrap();
                    // Create separate tables for each thread
                    for tid in 0..n_threads {
                        setup.execute_batch(&create_table_sql(tid)).unwrap();
                    }
                }
                let retry_count = Arc::new(AtomicU64::new(0));
                let operations_with_retries = Arc::new(AtomicU64::new(0));
                (tmp, path, retry_count, operations_with_retries)
            },
            |(_tmp, path, retry_count, operations_with_retries)| {
                let run_started = Instant::now();
                let barrier = Arc::new(Barrier::new(n_threads));
                let operation_timings: Arc<Vec<std::sync::Mutex<Vec<PersistentOperationTiming>>>> =
                    Arc::new(
                    (0..n_threads)
                        .map(|_| std::sync::Mutex::new(Vec::with_capacity(ROWS_PER_THREAD as usize)))
                        .collect(),
                    );
                let retry_stage_counts: Arc<
                    Vec<std::sync::Mutex<PersistentRetryStageCounts>>,
                > = Arc::new(
                    (0..n_threads)
                        .map(|_| std::sync::Mutex::new(PersistentRetryStageCounts::default()))
                        .collect(),
                );

                let handles: Vec<_> = (0..n_threads)
                    .map(|tid| {
                        let p = path.clone();
                        let bar = barrier.clone();
                        let retries = retry_count.clone();
                        let ops_with_retries = operations_with_retries.clone();
                        let op_timings = operation_timings.clone();
                        let per_thread_retry_stages = retry_stage_counts.clone();
                        thread::spawn(move || {
                            let conn = rusqlite::Connection::open(&p).unwrap();
                            conn.execute_batch(
                                "PRAGMA journal_mode=WAL;\
                                 PRAGMA synchronous=NORMAL;\
                                 PRAGMA cache_size=-64000;\
                                 PRAGMA busy_timeout=0;",
                            )
                            .unwrap();
                            let insert_stmt = insert_sql(tid);
                            let mut stmt = conn.prepare(&insert_stmt).unwrap();
                            bar.wait();

                            // Each row is its own transaction for realistic commit latency
                            for i in 0..ROWS_PER_THREAD {
                                let start = Instant::now();
                                let mut operation_timing = PersistentOperationTiming::default();
                                let mut begin_retries = 0u32;
                                loop {
                                    let begin_start = Instant::now();
                                    match conn.execute_batch("BEGIN IMMEDIATE") {
                                        Ok(()) => {
                                            operation_timing.begin_retry_handoff +=
                                                begin_start.elapsed();
                                            break;
                                        }
                                        Err(e) => {
                                            operation_timing.begin_retry_handoff +=
                                                begin_start.elapsed();
                                            let msg = e.to_string();
                                            if msg.contains("BUSY") || msg.contains("locked") {
                                                retries.fetch_add(1, Ordering::Relaxed);
                                                begin_retries += 1;
                                                {
                                                    let mut retry_counts =
                                                        per_thread_retry_stages[tid]
                                                            .lock()
                                                            .unwrap();
                                                    retry_counts.total_retries = retry_counts
                                                        .total_retries
                                                        .saturating_add(1);
                                                    retry_counts.begin_retries = retry_counts
                                                        .begin_retries
                                                        .saturating_add(1);
                                                }
                                                if begin_retries >= MAX_TXN_RETRIES {
                                                    panic!("BEGIN failed after {MAX_TXN_RETRIES} retries: {e}");
                                                }
                                                sleep_with_accounting(
                                                    &mut operation_timing,
                                                    RETRY_BACKOFF,
                                                );
                                            } else {
                                                panic!("BEGIN failed: {e}");
                                            }
                                        }
                                    }
                                }
                                let execute_start = Instant::now();
                                stmt.execute(rusqlite::params![i]).unwrap();
                                operation_timing.statement_execute_body += execute_start.elapsed();
                                let mut commit_retries = 0u32;
                                loop {
                                    let commit_start = Instant::now();
                                    match conn.execute_batch("COMMIT") {
                                        Ok(()) => {
                                            operation_timing.commit_roundtrip +=
                                                commit_start.elapsed();
                                            break;
                                        }
                                        Err(e) => {
                                            operation_timing.commit_roundtrip +=
                                                commit_start.elapsed();
                                            let msg = e.to_string();
                                            if msg.contains("BUSY") || msg.contains("locked") {
                                                retries.fetch_add(1, Ordering::Relaxed);
                                                commit_retries += 1;
                                                {
                                                    let mut retry_counts =
                                                        per_thread_retry_stages[tid]
                                                            .lock()
                                                            .unwrap();
                                                    retry_counts.total_retries = retry_counts
                                                        .total_retries
                                                        .saturating_add(1);
                                                    retry_counts.commit_retries = retry_counts
                                                        .commit_retries
                                                        .saturating_add(1);
                                                }
                                                if commit_retries >= MAX_TXN_RETRIES {
                                                    panic!("COMMIT failed after {MAX_TXN_RETRIES} retries: {e}");
                                                }
                                                sleep_with_accounting(
                                                    &mut operation_timing,
                                                    RETRY_BACKOFF,
                                                );
                                            } else {
                                                panic!("COMMIT failed: {e}");
                                            }
                                        }
                                    }
                                }
                                if begin_retries > 0 || commit_retries > 0 {
                                    ops_with_retries.fetch_add(1, Ordering::Relaxed);
                                }
                                operation_timing.wall_time = start.elapsed();
                                op_timings[tid].lock().unwrap().push(operation_timing);
                            }
                        })
                    })
                    .collect();

                for h in handles {
                    h.join().unwrap();
                }
                let run_wall = run_started.elapsed();

                // Report metrics
                let total_retries = retry_count.load(Ordering::Relaxed);
                let operations_with_retries = operations_with_retries.load(Ordering::Relaxed);
                let flattened_operation_timings: Vec<PersistentOperationTiming> = operation_timings
                    .iter()
                    .flat_map(|m| m.lock().unwrap().clone())
                    .collect();
                let retry_stage_counts = retry_stage_counts.iter().fold(
                    PersistentRetryStageCounts::default(),
                    |mut acc, counts| {
                        acc.merge(*counts.lock().unwrap());
                        acc
                    },
                );
                let metrics = build_benchmark_metrics(
                    total_rows,
                    run_wall,
                    &flattened_operation_timings,
                    retry_stage_counts,
                    None,
                    total_retries,
                    operations_with_retries,
                );

                log_benchmark_metrics("C SQLite", n_threads, "retry", &metrics);
                eprintln!(
                    "[C SQLite {n_threads}t wall audit] {}",
                    format_operation_wall_time_audit(&metrics.operation_wall_time_audit)
                );
                maybe_write_persistent_phase_capture(&PersistentPhaseCaptureSample {
                    schema_version: PERSISTENT_PHASE_CAPTURE_SAMPLE_SCHEMA_V3,
                    timestamp_unix_ms: unix_timestamp_ms(),
                    benchmark_group: format!("{label}/csqlite_concurrent_persistent"),
                    engine: SQLITE_ENGINE_ID,
                    contention_label: "retry",
                    concurrency: n_threads,
                    rows_per_thread: ROWS_PER_THREAD,
                    total_rows,
                    metrics,
                    phase_metrics: None,
                    phase_timing_report: None,
                    flusher_lock_wait_fraction_basis_points: None,
                    lock_topology_limited: None,
                });
            },
            criterion::BatchSize::LargeInput,
        );
    });

    // FrankenSQLite with real concurrent writers
    group.bench_function("frankensqlite_concurrent_persistent", |b| {
        b.iter_batched(
            || {
                let tmp = tempfile::NamedTempFile::new().unwrap();
                let path = tmp.path().to_str().unwrap().to_owned();
                {
                    // Setup: create tables using a single connection
                    let setup = fsqlite_e2e::block_on(fsqlite::Connection::open(&path))
                        .expect("open FrankenSQLite setup connection");
                    apply_setup_pragmas_fsqlite(&setup);
                    for tid in 0..n_threads {
                        fsqlite_e2e::block_on(setup.execute(&create_table_sql(tid)))
                            .expect("create FrankenSQLite benchmark table");
                    }
                }
                let conflict_count = Arc::new(AtomicU64::new(0));
                let operations_with_conflicts = Arc::new(AtomicU64::new(0));
                (tmp, path, conflict_count, operations_with_conflicts)
            },
            |(_tmp, path, conflict_count, operations_with_conflicts)| {
                let run_started = Instant::now();
                let barrier = Arc::new(Barrier::new(n_threads));
                let operation_timings: Arc<Vec<std::sync::Mutex<Vec<PersistentOperationTiming>>>> =
                    Arc::new(
                    (0..n_threads)
                        .map(|_| std::sync::Mutex::new(Vec::with_capacity(ROWS_PER_THREAD as usize)))
                        .collect(),
                    );
                let retry_stage_counts: Arc<
                    Vec<std::sync::Mutex<PersistentRetryStageCounts>>,
                > = Arc::new(
                    (0..n_threads)
                        .map(|_| std::sync::Mutex::new(PersistentRetryStageCounts::default()))
                        .collect(),
                );

                let handles: Vec<_> = (0..n_threads)
                    .map(|tid| {
                        let p = path.clone();
                        let bar = barrier.clone();
                        let conflicts = conflict_count.clone();
                        let ops_with_conflicts = operations_with_conflicts.clone();
                        let op_timings = operation_timings.clone();
                        let per_thread_retry_stages = retry_stage_counts.clone();
                        thread::spawn(move || {
                            let conn = fsqlite_e2e::block_on(fsqlite::Connection::open(&p))
                                .expect("open FrankenSQLite writer connection");
                            apply_session_pragmas_fsqlite(&conn);
                            let insert_stmt = insert_sql(tid);
                            let stmt = fsqlite_e2e::block_on(conn.prepare(&insert_stmt))
                                .expect("prepare FrankenSQLite INSERT");
                            bar.wait();

                            for i in 0..ROWS_PER_THREAD {
                                // Each thread writes to its own table, so row IDs can match
                                // the SQLite side exactly without cross-thread collisions.
                                let row_id = i;
                                let start = Instant::now();
                                let mut operation_timing = PersistentOperationTiming::default();
                                let mut retry_count = 0u32;

                                'txn: loop {
                                    // BEGIN CONCURRENT with retry
                                    loop {
                                        let begin_start = Instant::now();
                                        match fsqlite_e2e::block_on(
                                            conn.execute("BEGIN CONCURRENT"),
                                        ) {
                                            Ok(_) => {
                                                operation_timing.begin_retry_handoff +=
                                                    begin_start.elapsed();
                                                break;
                                            }
                                            Err(e) => {
                                                operation_timing.begin_retry_handoff +=
                                                    begin_start.elapsed();
                                                if is_retryable_fsqlite_error(&e) {
                                                    conflicts.fetch_add(1, Ordering::Relaxed);
                                                    retry_count += 1;
                                                    {
                                                        let mut retry_counts =
                                                            per_thread_retry_stages[tid]
                                                                .lock()
                                                                .unwrap();
                                                        retry_counts.total_retries = retry_counts
                                                            .total_retries
                                                            .saturating_add(1);
                                                        retry_counts.begin_retries = retry_counts
                                                            .begin_retries
                                                            .saturating_add(1);
                                                    }
                                                    if retry_count >= MAX_TXN_RETRIES {
                                                        panic!(
                                                            "BEGIN CONCURRENT failed after {MAX_TXN_RETRIES} retries: {e:?}"
                                                        );
                                                    }
                                                    sleep_with_accounting(
                                                        &mut operation_timing,
                                                        RETRY_BACKOFF,
                                                    );
                                                } else {
                                                    panic!("BEGIN CONCURRENT failed: {e:?}");
                                                }
                                            }
                                        }
                                    }

                                    // INSERT
                                    let execute_start = Instant::now();
                                    if let Err(e) = fsqlite_e2e::block_on(
                                        stmt.execute_with_params(&[SqliteValue::Integer(row_id)]),
                                    )
                                    {
                                        operation_timing.statement_execute_body +=
                                            execute_start.elapsed();
                                        if is_duplicate_insert_after_retry(&e) {
                                            // Row already exists (from previous retry that actually committed)
                                            {
                                                let mut retry_counts =
                                                    per_thread_retry_stages[tid]
                                                        .lock()
                                                        .unwrap();
                                                retry_counts.duplicate_after_retry_exits =
                                                    retry_counts
                                                        .duplicate_after_retry_exits
                                                        .saturating_add(1);
                                            }
                                            let rollback_start = Instant::now();
                                            rollback_fsqlite(
                                                &conn,
                                                "duplicate INSERT after retry",
                                            );
                                            operation_timing.rollback_cleanup +=
                                                rollback_start.elapsed();
                                            break 'txn;
                                        }
                                        if is_retryable_fsqlite_error(&e)
                                            || matches!(e, FrankenError::SerializationFailure { .. })
                                        {
                                            // Snapshot conflict — rollback and retry
                                            conflicts.fetch_add(1, Ordering::Relaxed);
                                            let rollback_start = Instant::now();
                                            rollback_fsqlite(&conn, "INSERT conflict");
                                            operation_timing.rollback_cleanup +=
                                                rollback_start.elapsed();
                                            retry_count += 1;
                                            {
                                                let mut retry_counts =
                                                    per_thread_retry_stages[tid]
                                                        .lock()
                                                        .unwrap();
                                                retry_counts.total_retries = retry_counts
                                                    .total_retries
                                                    .saturating_add(1);
                                                retry_counts.body_retries = retry_counts
                                                    .body_retries
                                                    .saturating_add(1);
                                            }
                                            if retry_count >= MAX_TXN_RETRIES {
                                                panic!("INSERT failed after {MAX_TXN_RETRIES} retries: {e:?}");
                                            }
                                            sleep_with_accounting(
                                                &mut operation_timing,
                                                RETRY_BACKOFF,
                                            );
                                            continue 'txn;
                                        }
                                        if is_corruption_error(&e) {
                                            let rollback_start = Instant::now();
                                            rollback_fsqlite(&conn, "corrupt INSERT");
                                            operation_timing.rollback_cleanup +=
                                                rollback_start.elapsed();
                                            panic!("CORRUPTION DETECTED: {e:?}");
                                        }
                                        panic!("INSERT failed: {e:?}");
                                    }
                                    operation_timing.statement_execute_body +=
                                        execute_start.elapsed();

                                    // COMMIT with retry
                                    let commit_start = Instant::now();
                                    match fsqlite_e2e::block_on(conn.execute("COMMIT")) {
                                        Ok(_) => {
                                            operation_timing.commit_roundtrip +=
                                                commit_start.elapsed();
                                            break 'txn;
                                        }
                                        Err(e) => {
                                            operation_timing.commit_roundtrip +=
                                                commit_start.elapsed();
                                            if is_retryable_fsqlite_error(&e)
                                                || matches!(e, FrankenError::SerializationFailure { .. })
                                            {
                                                conflicts.fetch_add(1, Ordering::Relaxed);
                                                let rollback_start = Instant::now();
                                                rollback_fsqlite(&conn, "COMMIT conflict");
                                                operation_timing.rollback_cleanup +=
                                                    rollback_start.elapsed();
                                                retry_count += 1;
                                                {
                                                    let mut retry_counts =
                                                        per_thread_retry_stages[tid]
                                                            .lock()
                                                            .unwrap();
                                                    retry_counts.total_retries = retry_counts
                                                        .total_retries
                                                        .saturating_add(1);
                                                    retry_counts.commit_retries = retry_counts
                                                        .commit_retries
                                                        .saturating_add(1);
                                                }
                                                if retry_count >= MAX_TXN_RETRIES {
                                                    panic!("COMMIT failed after {MAX_TXN_RETRIES} retries: {e:?}");
                                                }
                                                sleep_with_accounting(
                                                    &mut operation_timing,
                                                    RETRY_BACKOFF,
                                                );
                                                // Loop back to BEGIN CONCURRENT
                                            } else {
                                                panic!("COMMIT failed: {e:?}");
                                            }
                                        }
                                    }
                                }

                                if retry_count > 0 {
                                    ops_with_conflicts.fetch_add(1, Ordering::Relaxed);
                                }
                                operation_timing.wall_time = start.elapsed();
                                op_timings[tid].lock().unwrap().push(operation_timing);
                            }
                        })
                    })
                    .collect();

                for h in handles {
                    h.join().unwrap();
                }
                let run_wall = run_started.elapsed();

                // Report metrics
                let total_conflicts = conflict_count.load(Ordering::Relaxed);
                let operations_with_conflicts = operations_with_conflicts.load(Ordering::Relaxed);
                let flattened_operation_timings: Vec<PersistentOperationTiming> = operation_timings
                    .iter()
                    .flat_map(|m| m.lock().unwrap().clone())
                    .collect();
                let retry_stage_counts = retry_stage_counts.iter().fold(
                    PersistentRetryStageCounts::default(),
                    |mut acc, counts| {
                        acc.merge(*counts.lock().unwrap());
                        acc
                    },
                );

                // Print phase timing report from group commit metrics
                let metrics = fsqlite_wal::GLOBAL_CONSOLIDATION_METRICS.snapshot();
                let has_phase_metrics = metrics.total_commits() > 0;
                let measured_commit_sub_buckets = build_measured_commit_sub_buckets(&metrics);
                let benchmark_metrics = build_benchmark_metrics(
                    total_rows,
                    run_wall,
                    &flattened_operation_timings,
                    retry_stage_counts,
                    measured_commit_sub_buckets,
                    total_conflicts,
                    operations_with_conflicts,
                );
                log_benchmark_metrics("FrankenSQLite", n_threads, "conflict", &benchmark_metrics);
                let phase_timing_report = has_phase_metrics.then(|| metrics.phase_timing_report());
                if has_phase_metrics {
                    eprintln!(
                        "[FrankenSQLite {n_threads}t wal split] flusher_lock_wait_total={}us, wal_service_total={}us, wal_backend_lock_wait_p99={}us, wal_append_p99={}us, wal_sync_p99={}us, phase_b_p99={}us, lock_topology_limited={}, wakes={{notify:{}, timeout:{}, takeover:{}, failed_epoch:{}, busy_retry:{}}}",
                        metrics.flusher_lock_wait_us_total(),
                        metrics.wal_service_us_total(),
                        metrics.hist_wal_backend_lock_wait.p99,
                        metrics.hist_wal_append.p99,
                        metrics.hist_wal_sync.p99,
                        metrics.hist_phase_b.p99,
                        metrics.is_lock_topology_limited(),
                        metrics.wake_reasons.notify,
                        metrics.wake_reasons.timeout,
                        metrics.wake_reasons.flusher_takeover,
                        metrics.wake_reasons.failed_epoch,
                        metrics.wake_reasons.busy_retry,
                    );
                    eprintln!(
                        "[FrankenSQLite {n_threads}t phase timing]\n{}",
                        phase_timing_report
                            .as_deref()
                            .unwrap_or("phase timing unavailable")
                    );
                }
                eprintln!(
                    "[FrankenSQLite {n_threads}t wall audit] {}",
                    format_operation_wall_time_audit(
                        &benchmark_metrics.operation_wall_time_audit
                    )
                );
                maybe_write_persistent_phase_capture(&PersistentPhaseCaptureSample {
                    schema_version: PERSISTENT_PHASE_CAPTURE_SAMPLE_SCHEMA_V3,
                    timestamp_unix_ms: unix_timestamp_ms(),
                    benchmark_group: format!("{label}/frankensqlite_concurrent_persistent"),
                    engine: FSQLITE_ENGINE_ID,
                    contention_label: "conflict",
                    concurrency: n_threads,
                    rows_per_thread: ROWS_PER_THREAD,
                    total_rows,
                    metrics: benchmark_metrics,
                    phase_metrics: has_phase_metrics.then_some(metrics.clone()),
                    phase_timing_report,
                    flusher_lock_wait_fraction_basis_points:
                        flusher_lock_wait_fraction_basis_points(&metrics),
                    lock_topology_limited: has_phase_metrics
                        .then_some(metrics.is_lock_topology_limited()),
                });
                // Reset metrics for next iteration
                fsqlite_wal::GLOBAL_CONSOLIDATION_METRICS.reset();
            },
            criterion::BatchSize::LargeInput,
        );
    });

    group.finish();
}

fn bench_persistent_2t(c: &mut Criterion) {
    bench_concurrent_csqlite_persistent(c, 2, "persistent_concurrent_write_2t");
}

fn bench_persistent_4t(c: &mut Criterion) {
    bench_concurrent_csqlite_persistent(c, 4, "persistent_concurrent_write_4t");
}

fn bench_persistent_8t(c: &mut Criterion) {
    bench_concurrent_csqlite_persistent(c, 8, "persistent_concurrent_write_8t");
}

fn bench_persistent_16t(c: &mut Criterion) {
    bench_concurrent_csqlite_persistent(c, 16, "persistent_concurrent_write_16t");
}

criterion_group!(
    name = persistent_concurrent_write;
    config = criterion_config();
    targets = bench_persistent_2t, bench_persistent_4t, bench_persistent_8t, bench_persistent_16t
);
criterion_main!(persistent_concurrent_write);
