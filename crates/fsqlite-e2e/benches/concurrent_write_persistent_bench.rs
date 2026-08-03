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
//! - Citation capture is opt-in: setting the directory also requires the
//!   compile-time `FSQLITE_BENCH_BUILD_NONCE` identity. The benchmark writes
//!   `provenance.json` once, appends per-iteration
//!   records to `samples.jsonl`, and refreshes paired SQLite-vs-FrankenSQLite
//!   `component_comparison.{json,md}` artifacts without changing default
//!   stderr output

// bd-mnlk2 / bd-zavyn: the consolidated timed bodies await fsqlite-core's
// deliberately non-`Send`, deeply nested engine futures inside one runtime
// entry per transaction attempt; `future_not_send` and `large_futures`
// contradict that design (see fsqlite-core/src/lib.rs for the full rationale,
// including why boxing was rejected by the perf ledger).
#![allow(clippy::future_not_send)]
#![allow(clippy::large_futures)]

use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier, OnceLock};
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
use sha2::{Digest, Sha256};

const ROWS_PER_THREAD: i64 = 1000;
const PERSISTENT_BENCH_SYNCHRONOUS: &str = "NORMAL";
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
const PERSISTENT_PHASE_CAPTURE_PROVENANCE_SCHEMA_V2: &str =
    "fsqlite-e2e.persistent_phase_capture_provenance.v2";
const BENCH_BUILD_NONCE_ENV: &str = "FSQLITE_BENCH_BUILD_NONCE";
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

async fn apply_session_pragmas_fsqlite(conn: &fsqlite::Connection) {
    for pragma in [
        "PRAGMA journal_mode = WAL;",
        "PRAGMA synchronous = NORMAL;",
        "PRAGMA cache_size = -64000;",
        "PRAGMA busy_timeout = 0;",
        "PRAGMA fsqlite.concurrent_mode = ON;",
    ] {
        conn.execute(pragma).await.unwrap_or_else(|error| {
            panic!("failed to execute benchmark pragma `{pragma}`: {error:?}")
        });
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
    let criterion = Criterion::default().configure_from_args();
    persistent_phase_capture_dir().map_or(criterion, |capture_dir| {
        // criterion 0.8 takes `&Path` here, not an owned `PathBuf`.
        criterion.output_directory(&capture_dir.join("criterion_measurements"))
    })
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

#[derive(Debug, Clone, Deserialize, Serialize)]
struct PersistentPhaseCaptureProvenance {
    schema_version: String,
    benchmark: String,
    output_dir_env: String,
    rows_per_thread: i64,
    concurrency: usize,
    synchronous: String,
    max_txn_retries: u32,
    current_dir: String,
    current_exe: String,
    build_nonce: String,
    running_binary_sha256: String,
    argv: Vec<String>,
    hostname: Option<String>,
    kernel_release: Option<String>,
    criterion_emission_scope: String,
}

#[derive(Debug, Clone, Serialize)]
struct PersistentPhaseCaptureSample {
    schema_version: &'static str,
    timestamp_unix_ms: u64,
    benchmark_group: String,
    engine: &'static str,
    contention_label: &'static str,
    concurrency: usize,
    synchronous: &'static str,
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

fn require_lowercase_hex_64(value: &str, field: &str) -> std::io::Result<()> {
    if value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Ok(());
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        format!("{field} must be exactly 64 lowercase hexadecimal characters"),
    ))
}

fn compiled_build_nonce_from(value: Option<&str>) -> std::io::Result<String> {
    let nonce = value.ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("{BENCH_BUILD_NONCE_ENV} was absent while this benchmark was compiled"),
        )
    })?;
    require_lowercase_hex_64(nonce, BENCH_BUILD_NONCE_ENV)?;
    Ok(nonce.to_owned())
}

fn compiled_build_nonce() -> std::io::Result<String> {
    compiled_build_nonce_from(option_env!("FSQLITE_BENCH_BUILD_NONCE"))
}

fn sha256_file(path: &Path) -> std::io::Result<String> {
    let mut file = fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 8192];
    loop {
        let bytes_read = file.read(&mut buffer)?;
        if bytes_read == 0 {
            break;
        }
        hasher.update(&buffer[..bytes_read]);
    }
    // sha2 0.11 returns `hybrid_array::Array`, which does not implement
    // `LowerHex`; encode explicitly rather than relying on the 0.10 formatter.
    let digest = {
        use std::fmt::Write as _;

        let mut encoded = String::with_capacity(64);
        for byte in hasher.finalize() {
            let _ = write!(&mut encoded, "{byte:02x}");
        }
        encoded
    };
    require_lowercase_hex_64(&digest, "running binary SHA-256")?;
    Ok(digest)
}

fn running_executable_identity() -> std::io::Result<(String, String)> {
    static IDENTITY: OnceLock<std::io::Result<(String, String)>> = OnceLock::new();
    IDENTITY
        .get_or_init(|| {
            let executable = std::env::current_exe()?;
            let binary_sha256 = sha256_file(&executable)?;
            Ok((executable.display().to_string(), binary_sha256))
        })
        .as_ref()
        .map(Clone::clone)
        .map_err(|error| std::io::Error::new(error.kind(), error.to_string()))
}

fn persistent_phase_capture_provenance(
    concurrency: usize,
) -> std::io::Result<PersistentPhaseCaptureProvenance> {
    let (current_exe, running_binary_sha256) = running_executable_identity()?;
    Ok(PersistentPhaseCaptureProvenance {
        schema_version: PERSISTENT_PHASE_CAPTURE_PROVENANCE_SCHEMA_V2.to_owned(),
        benchmark: "concurrent_write_persistent_bench".to_owned(),
        output_dir_env: PERSISTENT_PHASE_CAPTURE_DIR_ENV.to_owned(),
        rows_per_thread: ROWS_PER_THREAD,
        concurrency,
        synchronous: PERSISTENT_BENCH_SYNCHRONOUS.to_owned(),
        max_txn_retries: MAX_TXN_RETRIES,
        current_dir: std::env::current_dir()
            .map(|path| path.display().to_string())
            .unwrap_or_else(|_| ".".to_owned()),
        current_exe,
        build_nonce: compiled_build_nonce()?,
        running_binary_sha256,
        argv: std::env::args_os()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect(),
        hostname: std::env::var("HOSTNAME")
            .ok()
            .filter(|hostname| !hostname.is_empty())
            .or_else(|| read_trimmed_file("/etc/hostname")),
        kernel_release: read_trimmed_file("/proc/sys/kernel/osrelease"),
        criterion_emission_scope: "every completed iteration sample is captured after its run_wall is fixed; all capture, logging, hashing, serialization, and IO are excluded from the Duration returned to Criterion; warmup and measurement phases are not distinguished by this harness".to_owned(),
    })
}

fn ensure_persistent_phase_capture_provenance(
    output_dir: &Path,
    concurrency: usize,
) -> std::io::Result<()> {
    fs::create_dir_all(output_dir)?;
    let provenance_path = output_dir.join("provenance.json");
    let current = persistent_phase_capture_provenance(concurrency)?;
    if provenance_path.exists() {
        let existing: PersistentPhaseCaptureProvenance =
            serde_json::from_slice(&fs::read(&provenance_path)?).map_err(|error| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "existing {} is not citation-grade provenance: {error}",
                        provenance_path.display()
                    ),
                )
            })?;
        if has_same_capture_identity(&existing, &current) {
            return Ok(());
        }
        return Err(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            format!(
                "existing {} belongs to a different build identity; refusing stale capture reuse",
                provenance_path.display()
            ),
        ));
    }
    let payload = serde_json::to_string_pretty(&current).map_err(std::io::Error::other)?;
    fs::write(provenance_path, payload.as_bytes())
}

fn has_same_capture_identity(
    existing: &PersistentPhaseCaptureProvenance,
    current: &PersistentPhaseCaptureProvenance,
) -> bool {
    existing.schema_version == PERSISTENT_PHASE_CAPTURE_PROVENANCE_SCHEMA_V2
        && existing.build_nonce == current.build_nonce
        && existing.concurrency == current.concurrency
        && existing.synchronous == current.synchronous
        && existing.current_exe == current.current_exe
        && existing.running_binary_sha256 == current.running_binary_sha256
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
    ensure_persistent_phase_capture_provenance(&output_dir, sample.concurrency).unwrap_or_else(
        |error| {
            panic!(
                "[persistent phase capture] citation provenance failed in {}: {error}",
                output_dir.display()
            )
        },
    );
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
    group.throughput(Throughput::Elements(total_rows));

    group.bench_function("csqlite_concurrent_persistent", |b| {
        // `iter_custom` is required here: Criterion must observe only the
        // concurrent-writer workload. Fixture creation, metric aggregation,
        // logging, and citation-artifact capture all perform filesystem,
        // locking, hashing, and allocation work that would otherwise be folded
        // into the reported per-iteration time.
        b.iter_custom(|iters| {
            let mut accumulated = Duration::ZERO;
            for _ in 0..iters {
                // ── setup: deliberately outside the timed region ──────────
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

                // ── timed region begins: concurrent workload only ─────────
                let run_started = Instant::now();
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
                // ── timed region ends: only run_wall is reported ──────────
                accumulated += run_wall;

                // Everything below is untimed: aggregation locks mutexes and
                // allocates, logging writes to stderr, and citation capture
                // hashes and writes files.
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
                    synchronous: PERSISTENT_BENCH_SYNCHRONOUS,
                    rows_per_thread: ROWS_PER_THREAD,
                    total_rows,
                    metrics,
                    phase_metrics: None,
                    phase_timing_report: None,
                    flusher_lock_wait_fraction_basis_points: None,
                    lock_topology_limited: None,
                });
                drop(tmp);
            }
            accumulated
        });
    });

    // FrankenSQLite with real concurrent writers
    group.bench_function("frankensqlite_concurrent_persistent", |b| {
        // See the C SQLite arm above: `iter_custom` keeps fixture creation,
        // aggregation, logging, and citation capture out of the reported time.
        b.iter_custom(|iters| {
            let mut accumulated = Duration::ZERO;
            for _ in 0..iters {
                // ── setup: deliberately outside the timed region ──────────
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

                // Discard consolidation metrics accumulated while creating the
                // fixture. Setup opens a connection and runs CREATE TABLE per
                // thread, which drives WAL consolidation; leaving those counts
                // in place would attribute setup DDL to the measured workload.
                fsqlite_wal::GLOBAL_CONSOLIDATION_METRICS.reset();

                // ── timed region begins: concurrent workload only ─────────
                let run_started = Instant::now();
                let handles: Vec<_> = (0..n_threads)
                    .map(|tid| {
                        let p = path.clone();
                        let bar = barrier.clone();
                        let conflicts = conflict_count.clone();
                        let ops_with_conflicts = operations_with_conflicts.clone();
                        let op_timings = operation_timings.clone();
                        let per_thread_retry_stages = retry_stage_counts.clone();
                        thread::spawn(move || {
                            // bd-mnlk2 / bd-zavyn: one runtime entry per timed sample.
                            let conn = fsqlite_e2e::block_on(async {
                                let conn = fsqlite::Connection::open(&p)
                                    .await
                                    .expect("open FrankenSQLite writer connection");
                                apply_session_pragmas_fsqlite(&conn).await;
                                conn
                            });
                            let insert_stmt = insert_sql(tid);
                            // `PreparedStatement` borrows the connection, so the
                            // prepare stays its own (per-thread, pre-loop) entry.
                            let stmt = fsqlite_e2e::block_on(conn.prepare(&insert_stmt))
                                .expect("prepare FrankenSQLite INSERT");
                            bar.wait();

                            // Stage-tagged outcome of one transaction attempt so
                            // retry classification, rollback, and backoff stay
                            // outside the entered runtime.
                            enum AttemptOutcome {
                                Committed,
                                BeginFailed(FrankenError),
                                InsertFailed(FrankenError),
                                CommitFailed(FrankenError),
                            }

                            for i in 0..ROWS_PER_THREAD {
                                // Each thread writes to its own table, so row IDs can match
                                // the SQLite side exactly without cross-thread collisions.
                                let row_id = i;
                                let start = Instant::now();
                                let mut operation_timing = PersistentOperationTiming::default();
                                let mut retry_count = 0u32;

                                'txn: loop {
                                    // bd-mnlk2 / bd-zavyn: one runtime entry per transaction attempt; backoff stays outside the runtime.
                                    let outcome = fsqlite_e2e::block_on(async {
                                        // BEGIN CONCURRENT
                                        let begin_start = Instant::now();
                                        let begin_result = conn.execute("BEGIN CONCURRENT").await;
                                        operation_timing.begin_retry_handoff +=
                                            begin_start.elapsed();
                                        if let Err(e) = begin_result {
                                            return AttemptOutcome::BeginFailed(e);
                                        }

                                        // INSERT
                                        let execute_start = Instant::now();
                                        let insert_result = stmt
                                            .execute_with_params(&[SqliteValue::Integer(row_id)])
                                            .await;
                                        operation_timing.statement_execute_body +=
                                            execute_start.elapsed();
                                        if let Err(e) = insert_result {
                                            return AttemptOutcome::InsertFailed(e);
                                        }

                                        // COMMIT
                                        let commit_start = Instant::now();
                                        let commit_result = conn.execute("COMMIT").await;
                                        operation_timing.commit_roundtrip +=
                                            commit_start.elapsed();
                                        match commit_result {
                                            Ok(_) => AttemptOutcome::Committed,
                                            Err(e) => AttemptOutcome::CommitFailed(e),
                                        }
                                    });

                                    match outcome {
                                        AttemptOutcome::Committed => break 'txn,
                                        AttemptOutcome::BeginFailed(e) => {
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
                                        AttemptOutcome::InsertFailed(e) => {
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
                                                || matches!(
                                                    e,
                                                    FrankenError::SerializationFailure { .. }
                                                )
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
                                                    panic!(
                                                        "INSERT failed after {MAX_TXN_RETRIES} retries: {e:?}"
                                                    );
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
                                        AttemptOutcome::CommitFailed(e) => {
                                            if is_retryable_fsqlite_error(&e)
                                                || matches!(
                                                    e,
                                                    FrankenError::SerializationFailure { .. }
                                                )
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
                                                    panic!(
                                                        "COMMIT failed after {MAX_TXN_RETRIES} retries: {e:?}"
                                                    );
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
                // ── timed region ends: only run_wall is reported ──────────
                accumulated += run_wall;

                // Everything below is untimed: aggregation locks mutexes and
                // allocates, logging writes to stderr, and citation capture
                // hashes and writes files.
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
                    synchronous: PERSISTENT_BENCH_SYNCHRONOUS,
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
                drop(tmp);
            }
            accumulated
        });
    });

    group.finish();
}

fn bench_persistent_1t(c: &mut Criterion) {
    bench_concurrent_csqlite_persistent(c, 1, "persistent_concurrent_write_1t");
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

#[cfg(test)]
mod tests {
    use super::{
        PersistentPhaseCaptureProvenance, compiled_build_nonce_from, has_same_capture_identity,
        require_lowercase_hex_64, sha256_file,
    };
    use std::io::Write;

    #[test]
    fn citation_nonce_requires_exact_lowercase_hex() {
        let valid = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        assert_eq!(compiled_build_nonce_from(Some(valid)).unwrap(), valid);
        assert!(compiled_build_nonce_from(None).is_err());
        assert!(compiled_build_nonce_from(Some("not-a-nonce")).is_err());
        assert!(
            require_lowercase_hex_64(
                "0123456789ABCDEF0123456789abcdef0123456789abcdef0123456789abcdef",
                "test nonce",
            )
            .is_err()
        );
    }

    #[test]
    fn running_binary_digest_is_sha256() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(b"abc").unwrap();
        assert_eq!(
            sha256_file(file.path()).unwrap(),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn citation_configuration_has_no_group_level_overrides() {
        // Scoped to the implementation: a whole-file search would match the
        // literals in this assertion and in the shell validator's counterpart.
        let source = include_str!("concurrent_write_persistent_bench.rs");
        let impl_slice = implementation_slice(source);
        assert!(!impl_slice.contains("group.sample_size("));
        assert!(!impl_slice.contains("group.measurement_time("));
    }

    /// The benchmark implementation, excluding this test module.
    ///
    /// Assertions below must not inspect their own prose: a bare search of the
    /// whole file would match the tokens named in these comments and messages.
    fn implementation_slice(source: &str) -> &str {
        let start = source
            .find("fn bench_concurrent_csqlite_persistent(")
            .expect("benchmark implementation function must exist");
        let end = source
            .find("fn bench_persistent_1t(")
            .expect("per-thread wrapper must exist");
        assert!(start < end, "implementation must precede the wrappers");
        &source[start..end]
    }

    /// Criterion must observe only the concurrent-writer workload.
    ///
    /// The batched form times the whole routine, which folds fixture teardown,
    /// metric aggregation, stderr logging, and citation-artifact hashing and
    /// writing into the reported per-iteration duration. Both engine arms must
    /// instead drive Criterion through a custom timing loop that accumulates
    /// nothing but the workload wall time.
    #[test]
    fn timed_region_accumulates_only_workload_wall_time() {
        let source = include_str!("concurrent_write_persistent_bench.rs");
        let impl_slice = implementation_slice(source);
        assert!(
            !impl_slice.contains(".iter_batched("),
            "batched timing includes post-workload capture work"
        );
        assert_eq!(
            impl_slice.matches("b.iter_custom(").count(),
            2,
            "both engine arms must drive Criterion through a custom timing loop"
        );
        assert_eq!(
            impl_slice.matches("accumulated += run_wall;").count(),
            2,
            "each arm must accumulate exactly the workload wall time"
        );
        assert_eq!(
            impl_slice
                .matches("let mut accumulated = Duration::ZERO;")
                .count(),
            2,
            "each arm returns only its accumulator to Criterion"
        );
    }

    /// Consolidation metrics must describe the workload, not the fixture.
    ///
    /// Setup opens a connection and runs `CREATE TABLE` per thread, which
    /// drives WAL consolidation. The FrankenSQLite arm must therefore reset the
    /// global snapshot after the fixture is built and before the clock starts.
    #[test]
    fn consolidation_metrics_reset_before_timed_region() {
        let source = include_str!("concurrent_write_persistent_bench.rs");
        let impl_slice = implementation_slice(source);
        let arm_start = impl_slice
            .find("\"frankensqlite_concurrent_persistent\"")
            .expect("FrankenSQLite arm must exist");
        let arm = &impl_slice[arm_start..];
        let reset = arm
            .find("GLOBAL_CONSOLIDATION_METRICS.reset();")
            .expect("arm must reset consolidation metrics");
        let run_started = arm
            .find("let run_started = Instant::now();")
            .expect("arm must start a timing clock");
        assert!(
            reset < run_started,
            "consolidation metrics must be reset before the timed region so \
             fixture DDL is not attributed to the measured workload"
        );
    }

    #[test]
    fn stale_capture_provenance_identity_is_rejected() {
        let current = PersistentPhaseCaptureProvenance {
            schema_version: "fsqlite-e2e.persistent_phase_capture_provenance.v2".to_owned(),
            benchmark: "concurrent_write_persistent_bench".to_owned(),
            output_dir_env: "FSQLITE_PERSISTENT_PHASE_ATTRIBUTION_DIR".to_owned(),
            rows_per_thread: 1000,
            concurrency: 8,
            synchronous: "NORMAL".to_owned(),
            max_txn_retries: 100,
            current_dir: "/worker/project".to_owned(),
            current_exe: "/worker/project/target/bench".to_owned(),
            build_nonce: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                .to_owned(),
            running_binary_sha256:
                "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad".to_owned(),
            argv: Vec::new(),
            hostname: Some("worker".to_owned()),
            kernel_release: Some("kernel".to_owned()),
            criterion_emission_scope: "measurement".to_owned(),
        };
        let decoded: PersistentPhaseCaptureProvenance =
            serde_json::from_slice(&serde_json::to_vec(&current).unwrap()).unwrap();
        assert!(has_same_capture_identity(&decoded, &current));

        let mut stale = decoded;
        stale.build_nonce =
            "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff".to_owned();
        assert!(!has_same_capture_identity(&stale, &current));
    }
}

criterion_group!(
    name = persistent_concurrent_write;
    config = criterion_config();
    targets = bench_persistent_1t, bench_persistent_2t, bench_persistent_4t, bench_persistent_8t, bench_persistent_16t
);
criterion_main!(persistent_concurrent_write);
