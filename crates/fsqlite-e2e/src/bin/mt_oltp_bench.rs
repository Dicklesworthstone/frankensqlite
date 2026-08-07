//! `mt-oltp-bench` — mixed-read-write OLTP benchmark for concurrent-mode claims.
//!
//! Validates the core FrankenSQLite MVCC promise: readers don't block under
//! write load. Seeds a shared file-backed DB, then runs R reader threads
//! alongside W writer threads concurrently. Measures:
//!   - Read latency (p50/p95/p99) under concurrent write load
//!   - Write throughput under concurrent read load
//!   - Per-thread fairness (Jain's fairness index)
//!   - Comparison: fsqlite vs C SQLite (rusqlite) under identical mixed load
//!
//! Configurable reader/writer thread mix. Each writer performs a fixed insert
//! budget while readers run closed-loop until the final writer finishes.
//!
//! ## CLI
//!
//! ```text
//! mt-oltp-bench [--seed-rows=5000] [--ops-per-thread=5000]
//!               [--readers=4] [--writers=2]
//!               [--iters=8] [--json-output=PATH]
//! ```
//! `--ops-per-thread` is the exact insert count for each writer. Readers run
//! continuously from the common start until the last writer finishes.

// bd-mnlk2 / bd-zavyn: the hoisted timed bodies await fsqlite-core's
// deliberately non-`Send`, deeply nested engine futures inside one runtime
// entry per sample; `future_not_send` and `large_futures` contradict that
// design (see comprehensive_bench.rs for the same rationale — boxing would
// put an allocation inside the timed window).
#![allow(clippy::future_not_send)]
#![allow(clippy::large_futures)]

use serde::Serialize;
use std::collections::HashSet;
use std::fmt::Write as _;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::{Arc, Condvar, Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const DEFAULT_SEED_ROWS: i64 = 5_000;
const DEFAULT_OPS_PER_THREAD: usize = 5_000;
const DEFAULT_READERS: usize = 4;
const DEFAULT_WRITERS: usize = 2;
const DEFAULT_ITERS: usize = 8;
const PAYLOAD_SIZE: usize = 64;
const ROWID_BASE_STRIDE: i64 = 1_000_000;
const MAX_RETRIES: usize = 512;
const RETRY_SLEEP: Duration = Duration::from_micros(100);
const SETUP_TIMEOUT: Duration = Duration::from_secs(60);
const WORK_TIMEOUT: Duration = Duration::from_secs(600);
const MAX_READER_OPS_PER_THREAD: u64 = 1_000_000;
const REPORT_SCHEMA: &str = "fsqlite-e2e.mt_oltp_bench_report.v2";
const ORDERING_POLICY: &str = "paired_alternating_abba_baab_blocks_of_8";
const BEAD_ID: &str = "bd-v39s2";

#[derive(Debug, Clone)]
struct Options {
    seed_rows: i64,
    ops_per_thread: usize,
    readers: usize,
    writers: usize,
    iters: usize,
    json_output: Option<PathBuf>,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            seed_rows: DEFAULT_SEED_ROWS,
            ops_per_thread: DEFAULT_OPS_PER_THREAD,
            readers: DEFAULT_READERS,
            writers: DEFAULT_WRITERS,
            iters: DEFAULT_ITERS,
            json_output: None,
        }
    }
}

fn print_usage_and_exit(code: i32) -> ! {
    eprintln!(
        "Usage: mt-oltp-bench [--seed-rows=N] [--ops-per-thread=N_WRITES] \
         [--readers=N] [--writers=N] [--iters=N] [--json-output=PATH]"
    );
    std::process::exit(code);
}

fn parse_opts() -> Options {
    let mut opts = Options::default();
    for arg in std::env::args().skip(1) {
        if arg == "--help" || arg == "-h" {
            print_usage_and_exit(0);
        }
        if let Some(v) = arg.strip_prefix("--seed-rows=") {
            opts.seed_rows = v.parse().unwrap_or_else(|_| {
                eprintln!("Bad --seed-rows: {v}");
                std::process::exit(2);
            });
        } else if let Some(v) = arg.strip_prefix("--ops-per-thread=") {
            opts.ops_per_thread = v.parse().unwrap_or_else(|_| {
                eprintln!("Bad --ops-per-thread: {v}");
                std::process::exit(2);
            });
        } else if let Some(v) = arg.strip_prefix("--readers=") {
            opts.readers = v.parse().unwrap_or_else(|_| {
                eprintln!("Bad --readers: {v}");
                std::process::exit(2);
            });
        } else if let Some(v) = arg.strip_prefix("--writers=") {
            opts.writers = v.parse().unwrap_or_else(|_| {
                eprintln!("Bad --writers: {v}");
                std::process::exit(2);
            });
        } else if let Some(v) = arg.strip_prefix("--iters=") {
            opts.iters = v.parse().unwrap_or_else(|_| {
                eprintln!("Bad --iters: {v}");
                std::process::exit(2);
            });
        } else if let Some(v) = arg.strip_prefix("--json-output=") {
            opts.json_output = Some(PathBuf::from(v));
        } else {
            eprintln!("Unknown option: {arg}");
            print_usage_and_exit(2);
        }
    }
    opts
}

// ─── Result types ───────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize)]
struct LatencyStats {
    count: u64,
    p50_us: f64,
    p95_us: f64,
    p99_us: f64,
    mean_us: f64,
}

#[derive(Debug, Clone, Serialize)]
struct ThreadReport {
    role: String,
    tid: usize,
    expected_ops: u64,
    completed_ops: u64,
    failed_ops: u64,
    elapsed_ms: f64,
    ops_per_sec: f64,
    latency: LatencyStats,
    settings: WorkerSettings,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct WorkerSettings {
    page_size_bytes: i64,
    journal_mode: String,
    synchronous: String,
    cache_size: i64,
    busy_timeout_ms: i64,
    wal_autocheckpoint_pages: i64,
    concurrent_mode: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct WorkReceipt {
    minimum_expected_reads: u64,
    completed_reads: u64,
    reads_with_verified_writer_call_interval_overlap: u64,
    expected_writes: u64,
    completed_writes: u64,
    expected_consumed_read_payload_bytes: u64,
    consumed_read_payload_bytes: u64,
    expected_database_rows_before: i64,
    observed_database_rows_before: i64,
    expected_database_id_sum_before: i64,
    observed_database_id_sum_before: i64,
    expected_database_payload_bytes_before: i64,
    observed_database_payload_bytes_before: i64,
    expected_matching_payload_rows_before: i64,
    observed_matching_payload_rows_before: i64,
    expected_database_rows_after: i64,
    observed_database_rows_after: i64,
    expected_database_id_sum_after: i64,
    observed_database_id_sum_after: i64,
    expected_database_payload_bytes_after: i64,
    observed_database_payload_bytes_after: i64,
    expected_matching_payload_rows_after: i64,
    observed_matching_payload_rows_after: i64,
    id_range_receipts_after: Vec<IdRangeReceipt>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct IdRangeReceipt {
    label: String,
    lower_inclusive: i64,
    upper_inclusive: i64,
    expected_rows: i64,
    observed_rows: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DatabaseState {
    rows: i64,
    id_sum: i64,
    payload_bytes: i64,
    matching_payload_rows: i64,
}

#[derive(Debug, Clone, Serialize)]
struct IterResult {
    readers: Vec<ThreadReport>,
    writers: Vec<ThreadReport>,
    wall_elapsed_ms: f64,
    reader_completion_elapsed_ms: f64,
    writer_completion_elapsed_ms: f64,
    both_roles_incomplete_elapsed_ms_from_common_start: f64,
    both_roles_incomplete_fraction_of_wall: f64,
    total_read_ops: u64,
    total_write_ops: u64,
    total_failed_writes: u64,
    aggregate_read_latency: LatencyStats,
    aggregate_write_latency: LatencyStats,
    read_ops_per_sec: f64,
    write_ops_per_sec: f64,
    reader_fairness_jain: f64,
    writer_fairness_jain: f64,
    work_receipt: WorkReceipt,
}

#[derive(Debug, Clone, Serialize)]
struct EngineReport {
    engine: String,
    iters: Vec<IterResult>,
    median_read_ops_per_sec: f64,
    median_write_ops_per_sec: f64,
    median_read_p50_us: f64,
    median_read_p95_us: f64,
    median_read_p99_us: f64,
}

#[derive(Debug, Clone, Serialize)]
struct BenchReport {
    schema_version: String,
    citable: bool,
    status: String,
    validation_limitations: Vec<String>,
    bead_id: String,
    timestamp_unix_ms: u64,
    seed_rows: i64,
    ops_per_thread: usize,
    num_readers: usize,
    num_writers: usize,
    iterations: usize,
    ordering_policy: String,
    workload_policy: String,
    ratio_aggregation: String,
    timing_scope: String,
    fsqlite: EngineReport,
    sqlite_reference: EngineReport,
    paired_iterations: Vec<PairedIterationRatio>,
    read_throughput_ratio: f64,
    write_throughput_ratio: f64,
    read_latency_p50_ratio: f64,
    read_latency_p95_ratio: f64,
}

#[derive(Debug, Clone, Serialize)]
struct PairedIterationRatio {
    sample_index: usize,
    order_block_index: usize,
    order_block_pattern: String,
    position_in_block: usize,
    execution_order: String,
    read_throughput_ratio: f64,
    write_throughput_ratio: f64,
    read_latency_p50_ratio: f64,
    read_latency_p95_ratio: f64,
}

// ─── Helpers ────────────────────────────────────────────────────────────

fn percentile(sorted: &[f64], pct: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    if sorted.len() == 1 {
        return sorted[0];
    }
    #[allow(clippy::cast_precision_loss)]
    let idx = pct * (sorted.len() - 1) as f64;
    let lo = idx.floor() as usize;
    let hi = idx.ceil() as usize;
    let frac = idx - lo as f64;
    if lo == hi {
        sorted[lo]
    } else {
        sorted[hi].mul_add(frac, sorted[lo] * (1.0 - frac))
    }
}

fn compute_latency_stats(mut latencies_ns: Vec<u64>) -> LatencyStats {
    let count = latencies_ns.len() as u64;
    if latencies_ns.is_empty() {
        return LatencyStats {
            count: 0,
            p50_us: 0.0,
            p95_us: 0.0,
            p99_us: 0.0,
            mean_us: 0.0,
        };
    }
    latencies_ns.sort_unstable();
    #[allow(clippy::cast_precision_loss)]
    let as_us: Vec<f64> = latencies_ns.iter().map(|ns| *ns as f64 / 1_000.0).collect();
    let mean_us = as_us.iter().sum::<f64>() / as_us.len() as f64;
    LatencyStats {
        count,
        p50_us: percentile(&as_us, 0.50),
        p95_us: percentile(&as_us, 0.95),
        p99_us: percentile(&as_us, 0.99),
        mean_us,
    }
}

fn jain_fairness(values: &[f64]) -> f64 {
    if values.is_empty() {
        return 1.0;
    }
    let n = values.len() as f64;
    let sum: f64 = values.iter().sum();
    let sum_sq: f64 = values.iter().map(|v| v * v).sum();
    if sum_sq <= 0.0 {
        return 1.0;
    }
    let numerator = sum * sum;
    let denominator = n * sum_sq;
    numerator / denominator
}

fn median_of(mut values: Vec<f64>) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    percentile(&values, 0.50)
}

fn make_payload() -> String {
    "x".repeat(PAYLOAD_SIZE)
}

fn lcg_next(state: &mut u64) -> u64 {
    *state = state
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1_442_695_040_888_963_407);
    *state
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn validate_options(opts: &Options) -> Result<(), String> {
    if opts.seed_rows <= 0 {
        return Err("--seed-rows must be greater than zero".to_owned());
    }
    if opts.ops_per_thread < 2 {
        return Err(
            "--ops-per-thread must be at least two so overlap qualification observes repeated timed writer-call intervals rather than a single scheduling boundary"
                .to_owned(),
        );
    }
    if opts.readers == 0 || opts.writers == 0 {
        return Err("mixed OLTP requires at least one reader and one writer".to_owned());
    }
    if opts.iters == 0 {
        return Err("--iters must be greater than zero".to_owned());
    }
    if !opts.iters.is_multiple_of(8) {
        return Err(
            "--iters must be a multiple of 8 for complete alternating ABBA/BAAB order blocks"
                .to_owned(),
        );
    }
    let rowid_base_stride = usize::try_from(ROWID_BASE_STRIDE)
        .map_err(|error| format!("row-id stride exceeds usize: {error}"))?;
    if opts.writers > 1 && opts.ops_per_thread > rowid_base_stride {
        return Err(format!(
            "--ops-per-thread must not exceed {ROWID_BASE_STRIDE}; larger values overlap writer row-id ranges"
        ));
    }
    if opts.writers > 0 {
        let last_writer = opts.writers - 1;
        let last_offset = opts.ops_per_thread - 1;
        i64::try_from(last_writer)
            .ok()
            .and_then(|writer| writer.checked_mul(ROWID_BASE_STRIDE))
            .and_then(|writer_base| opts.seed_rows.checked_add(1 + writer_base))
            .and_then(|base| {
                i64::try_from(last_offset)
                    .ok()
                    .and_then(|offset| base.checked_add(offset))
            })
            .ok_or_else(|| "writer row-id range exceeds SQLite INTEGER".to_owned())?;
    }
    Ok(())
}

fn normalized_synchronous(value: &str) -> Result<String, String> {
    match value.to_ascii_lowercase().as_str() {
        "0" | "off" => Ok("off".to_owned()),
        "1" | "normal" => Ok("normal".to_owned()),
        "2" | "full" => Ok("full".to_owned()),
        "3" | "extra" => Ok("extra".to_owned()),
        _ => Err(format!("unrecognized PRAGMA synchronous value `{value}`")),
    }
}

fn normalize_fsqlite_value(value: &fsqlite::SqliteValue) -> String {
    match value {
        fsqlite::SqliteValue::Null => "null".to_owned(),
        fsqlite::SqliteValue::Integer(value) => value.to_string(),
        fsqlite::SqliteValue::Float(value) => value.to_string(),
        fsqlite::SqliteValue::Text(value) => value.as_ref().to_ascii_lowercase(),
        fsqlite::SqliteValue::Blob(value) => format!("blob:{}", value.len()),
    }
}

fn query_fsqlite_scalar(conn: &fsqlite::Connection, sql: &str) -> Result<String, String> {
    let rows = fsqlite_e2e::block_on(conn.query(sql))
        .map_err(|error| format!("FrankenSQLite `{sql}` failed: {error}"))?;
    let row = rows
        .first()
        .ok_or_else(|| format!("FrankenSQLite `{sql}` returned no row"))?;
    if rows.len() != 1 {
        return Err(format!(
            "FrankenSQLite `{sql}` returned {} rows, expected one",
            rows.len()
        ));
    }
    row.get(0)
        .map(normalize_fsqlite_value)
        .ok_or_else(|| format!("FrankenSQLite `{sql}` returned no first column"))
}

fn query_rusqlite_scalar(conn: &rusqlite::Connection, sql: &str) -> Result<String, String> {
    conn.query_row(sql, [], |row| {
        let value = row.get_ref(0)?;
        Ok(match value {
            rusqlite::types::ValueRef::Null => "null".to_owned(),
            rusqlite::types::ValueRef::Integer(value) => value.to_string(),
            rusqlite::types::ValueRef::Real(value) => value.to_string(),
            rusqlite::types::ValueRef::Text(value) => {
                String::from_utf8_lossy(value).to_ascii_lowercase()
            }
            rusqlite::types::ValueRef::Blob(value) => format!("blob:{}", value.len()),
        })
    })
    .map_err(|error| format!("C SQLite `{sql}` failed: {error}"))
}

fn checkpoint_fsqlite_seed(conn: &fsqlite::Connection) -> Result<(), String> {
    let query_receipt = || {
        let rows = fsqlite_e2e::block_on(conn.query("PRAGMA wal_checkpoint(TRUNCATE);"))
            .map_err(|error| format!("FrankenSQLite seed checkpoint failed: {error}"))?;
        if rows.len() != 1 {
            return Err(format!(
                "FrankenSQLite seed checkpoint returned {} rows, expected one",
                rows.len()
            ));
        }
        Ok((
            rows[0].get(0).and_then(fsqlite::SqliteValue::as_integer),
            rows[0].get(1).and_then(fsqlite::SqliteValue::as_integer),
            rows[0].get(2).and_then(fsqlite::SqliteValue::as_integer),
        ))
    };
    let first = query_receipt()?;
    if first.0 != Some(0) {
        return Err(format!(
            "FrankenSQLite seed checkpoint reported busy state: {first:?}"
        ));
    }
    let empty_receipt = query_receipt()?;
    if empty_receipt != (Some(0), Some(0), Some(0)) {
        return Err(format!(
            "FrankenSQLite seed checkpoint did not reach an empty TRUNCATE state: first={first:?}, second={empty_receipt:?}"
        ));
    }
    Ok(())
}

fn checkpoint_rusqlite_seed(conn: &rusqlite::Connection) -> Result<(), String> {
    let query_receipt = || {
        conn.query_row("PRAGMA wal_checkpoint(TRUNCATE);", [], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
            ))
        })
        .map_err(|error| format!("C SQLite seed checkpoint failed: {error}"))
    };
    let first = query_receipt()?;
    if first.0 != 0 {
        return Err(format!(
            "C SQLite seed checkpoint reported busy state: {first:?}"
        ));
    }
    let empty_receipt = query_receipt()?;
    if empty_receipt != (0, 0, 0) {
        return Err(format!(
            "C SQLite seed checkpoint did not reach an empty TRUNCATE state: first={first:?}, second={empty_receipt:?}"
        ));
    }
    Ok(())
}

fn parse_worker_settings<F>(mut query: F, concurrent_mode: &str) -> Result<WorkerSettings, String>
where
    F: FnMut(&str) -> Result<String, String>,
{
    let page_size = query("PRAGMA page_size;")?;
    let journal_mode = query("PRAGMA journal_mode;")?;
    let synchronous = query("PRAGMA synchronous;")?;
    let cache_size = query("PRAGMA cache_size;")?;
    let busy_timeout = query("PRAGMA busy_timeout;")?;
    let wal_autocheckpoint = query("PRAGMA wal_autocheckpoint;")?;
    let settings = WorkerSettings {
        page_size_bytes: page_size
            .parse()
            .map_err(|error| format!("invalid PRAGMA page_size `{page_size}`: {error}"))?,
        journal_mode: journal_mode.to_ascii_lowercase(),
        synchronous: normalized_synchronous(&synchronous)?,
        cache_size: cache_size
            .parse()
            .map_err(|error| format!("invalid PRAGMA cache_size `{cache_size}`: {error}"))?,
        busy_timeout_ms: busy_timeout
            .parse()
            .map_err(|error| format!("invalid PRAGMA busy_timeout `{busy_timeout}`: {error}"))?,
        wal_autocheckpoint_pages: wal_autocheckpoint.parse().map_err(|error| {
            format!("invalid PRAGMA wal_autocheckpoint `{wal_autocheckpoint}`: {error}")
        })?,
        concurrent_mode: concurrent_mode.to_owned(),
    };
    let expected = WorkerSettings {
        page_size_bytes: 4_096,
        journal_mode: "wal".to_owned(),
        synchronous: "normal".to_owned(),
        cache_size: -64_000,
        busy_timeout_ms: 5_000,
        wal_autocheckpoint_pages: 1_000,
        concurrent_mode: concurrent_mode.to_owned(),
    };
    if settings != expected {
        return Err(format!(
            "worker settings mismatch: expected {expected:?}, observed {settings:?}"
        ));
    }
    Ok(settings)
}

fn configure_fsqlite_worker(conn: &fsqlite::Connection) -> Result<WorkerSettings, String> {
    for pragma in [
        "PRAGMA synchronous=NORMAL;",
        "PRAGMA cache_size=-64000;",
        "PRAGMA busy_timeout=5000;",
        "PRAGMA wal_autocheckpoint=1000;",
        "PRAGMA fsqlite.concurrent_mode=ON;",
    ] {
        fsqlite_e2e::block_on(conn.execute(pragma))
            .map_err(|error| format!("FrankenSQLite `{pragma}` failed: {error}"))?;
    }
    if !conn.is_concurrent_mode_default() {
        return Err("FrankenSQLite concurrent-writer mode is not enabled".to_owned());
    }
    let concurrent_mode = query_fsqlite_scalar(conn, "PRAGMA fsqlite.concurrent_mode;")?;
    if !matches!(concurrent_mode.as_str(), "1" | "true" | "on") {
        return Err(format!(
            "FrankenSQLite concurrent-mode readback was `{concurrent_mode}`, expected enabled"
        ));
    }
    parse_worker_settings(|sql| query_fsqlite_scalar(conn, sql), "fsqlite_mvcc_on")
}

fn configure_rusqlite_worker(conn: &rusqlite::Connection) -> Result<WorkerSettings, String> {
    conn.execute_batch(
        "PRAGMA synchronous=NORMAL;\
         PRAGMA cache_size=-64000;\
         PRAGMA busy_timeout=5000;\
         PRAGMA wal_autocheckpoint=1000;",
    )
    .map_err(|error| format!("C SQLite worker PRAGMAs failed: {error}"))?;
    parse_worker_settings(
        |sql| query_rusqlite_scalar(conn, sql),
        "sqlite_wal_single_writer",
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum WorkerRole {
    Reader,
    Writer,
}

impl WorkerRole {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Reader => "reader",
            Self::Writer => "writer",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct WorkerKey {
    role: WorkerRole,
    tid: usize,
}

#[derive(Debug)]
enum WorkerEvent {
    Ready(WorkerKey),
    Completed {
        key: WorkerKey,
        completed_ops: u64,
        finished_at: Instant,
    },
    Failed {
        key: WorkerKey,
        phase: &'static str,
        error: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StartState {
    Preparing,
    Run,
    Abort,
}

#[derive(Debug)]
struct StartGate {
    state: Mutex<StartState>,
    changed: Condvar,
}

impl StartGate {
    fn new() -> Self {
        Self {
            state: Mutex::new(StartState::Preparing),
            changed: Condvar::new(),
        }
    }

    fn wait(&self, key: WorkerKey) -> Result<(), String> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| "benchmark start gate was poisoned".to_owned())?;
        while *state == StartState::Preparing {
            state = self
                .changed
                .wait(state)
                .map_err(|_| "benchmark start gate was poisoned while waiting".to_owned())?;
        }
        match *state {
            StartState::Run => Ok(()),
            StartState::Abort => Err(format!(
                "{} worker {} aborted before timed work",
                key.role.as_str(),
                key.tid
            )),
            StartState::Preparing => unreachable!("preparing state is handled by wait loop"),
        }
    }

    fn set(&self, next: StartState) -> Result<(), String> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| "benchmark start gate was poisoned".to_owned())?;
        *state = next;
        self.changed.notify_all();
        Ok(())
    }

    fn ensure_running(&self, key: WorkerKey) -> Result<(), String> {
        let state = self
            .state
            .lock()
            .map_err(|_| "benchmark start gate was poisoned".to_owned())?;
        if *state == StartState::Run {
            Ok(())
        } else {
            Err(format!(
                "{} worker {} aborted during timed work",
                key.role.as_str(),
                key.tid
            ))
        }
    }
}

#[derive(Debug)]
struct WorkerOutcome {
    report: ThreadReport,
    latencies_ns: Vec<u64>,
    operation_intervals: Vec<TimedOperationInterval>,
    consumed_read_payload_bytes: u64,
    finished_at: Instant,
}

#[derive(Debug, Clone, Copy)]
struct TimedOperationInterval {
    started_at: Instant,
    finished_at: Instant,
}

impl TimedOperationInterval {
    fn overlaps(self, other: Self) -> bool {
        self.started_at < other.finished_at && other.started_at < self.finished_at
    }
}

#[derive(Debug, Clone, Copy)]
struct CoordinatedTiming {
    wall: Duration,
    reader_completion: Duration,
    writer_completion: Duration,
}

fn send_worker_event(
    sender: &mpsc::SyncSender<WorkerEvent>,
    event: WorkerEvent,
) -> Result<(), String> {
    sender
        .send(event)
        .map_err(|error| format!("benchmark coordinator stopped receiving events: {error}"))
}

fn wait_for_counter(
    gate: &StartGate,
    key: WorkerKey,
    counter: &AtomicUsize,
    expected: usize,
    description: &str,
) -> Result<(), String> {
    let deadline = Instant::now() + WORK_TIMEOUT;
    while counter.load(AtomicOrdering::Acquire) < expected {
        gate.ensure_running(key)?;
        if Instant::now() >= deadline {
            return Err(format!(
                "{} worker {} timed out waiting for {description}: observed {}, expected {expected}",
                key.role.as_str(),
                key.tid,
                counter.load(AtomicOrdering::Acquire),
            ));
        }
        thread::yield_now();
    }
    Ok(())
}

fn wait_for_active_writer_operation(
    gate: &StartGate,
    key: WorkerKey,
    active_writer_operations: &AtomicUsize,
    writers_remaining: &AtomicUsize,
) -> Result<bool, String> {
    let deadline = Instant::now() + WORK_TIMEOUT;
    loop {
        gate.ensure_running(key)?;
        if active_writer_operations.load(AtomicOrdering::Acquire) > 0 {
            return Ok(true);
        }
        if writers_remaining.load(AtomicOrdering::Acquire) == 0 {
            return Ok(false);
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "{} worker {} timed out waiting for an active timed writer call",
                key.role.as_str(),
                key.tid,
            ));
        }
        thread::yield_now();
    }
}

fn expected_worker_keys(num_readers: usize, num_writers: usize) -> HashSet<WorkerKey> {
    (0..num_readers)
        .map(|tid| WorkerKey {
            role: WorkerRole::Reader,
            tid,
        })
        .chain((0..num_writers).map(|tid| WorkerKey {
            role: WorkerRole::Writer,
            tid,
        }))
        .collect()
}

fn receive_until(
    receiver: &mpsc::Receiver<WorkerEvent>,
    deadline: Instant,
    phase: &str,
) -> Result<WorkerEvent, String> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err(format!("timed out waiting for {phase} worker receipts"));
    }
    receiver
        .recv_timeout(remaining)
        .map_err(|error| format!("failed waiting for {phase} worker receipt: {error}"))
}

fn coordinate_workers(
    expected_keys: &HashSet<WorkerKey>,
    receiver: &mpsc::Receiver<WorkerEvent>,
    gate: &StartGate,
    handles: Vec<thread::JoinHandle<Result<WorkerOutcome, String>>>,
) -> Result<(Vec<WorkerOutcome>, CoordinatedTiming), String> {
    let setup_deadline = Instant::now() + SETUP_TIMEOUT;
    let mut ready = HashSet::with_capacity(expected_keys.len());
    let mut setup_error = None;
    while ready.len() < expected_keys.len() {
        let event = match receive_until(receiver, setup_deadline, "setup") {
            Ok(event) => event,
            Err(error) => {
                setup_error = Some(error);
                break;
            }
        };
        match event {
            WorkerEvent::Ready(key) if expected_keys.contains(&key) && ready.insert(key) => {}
            WorkerEvent::Ready(key) if !expected_keys.contains(&key) => {
                setup_error = Some(format!("unexpected setup receipt from {key:?}"));
                break;
            }
            WorkerEvent::Ready(key) => {
                setup_error = Some(format!("duplicate setup receipt from {key:?}"));
                break;
            }
            WorkerEvent::Failed { key, phase, error } => {
                setup_error = Some(format!("{key:?} failed during {phase}: {error}"));
                break;
            }
            WorkerEvent::Completed { key, .. } => {
                setup_error = Some(format!("{key:?} completed before the timed start"));
                break;
            }
        }
    }
    if let Some(error) = setup_error {
        let _ = gate.set(StartState::Abort);
        for handle in handles {
            let _ = handle.join();
        }
        return Err(error);
    }

    let wall_started = Instant::now();
    gate.set(StartState::Run)?;
    let work_deadline = Instant::now() + WORK_TIMEOUT;
    let mut completed = HashSet::with_capacity(expected_keys.len());
    let mut latest_finished_at = wall_started;
    let mut latest_reader_finished_at = wall_started;
    let mut latest_writer_finished_at = wall_started;
    let mut first_error = None;
    while completed.len() < expected_keys.len() {
        let event = match receive_until(receiver, work_deadline, "completion") {
            Ok(event) => event,
            Err(error) => {
                first_error = Some(error);
                break;
            }
        };
        match event {
            WorkerEvent::Completed {
                key,
                completed_ops,
                finished_at,
            } if expected_keys.contains(&key) && completed.insert(key) => {
                if completed_ops == 0 {
                    first_error = Some(format!("{key:?} reported zero completed timed operations"));
                    break;
                }
                if finished_at < wall_started {
                    first_error = Some(format!(
                        "{key:?} reported a completion timestamp before the common start"
                    ));
                    break;
                }
                latest_finished_at = latest_finished_at.max(finished_at);
                match key.role {
                    WorkerRole::Reader => {
                        latest_reader_finished_at = latest_reader_finished_at.max(finished_at);
                    }
                    WorkerRole::Writer => {
                        latest_writer_finished_at = latest_writer_finished_at.max(finished_at);
                    }
                }
            }
            WorkerEvent::Completed { key, .. } if !expected_keys.contains(&key) => {
                first_error = Some(format!("unexpected completion from {key:?}"));
                break;
            }
            WorkerEvent::Completed { key, .. } => {
                first_error = Some(format!("duplicate completion from {key:?}"));
                break;
            }
            WorkerEvent::Failed { key, phase, error } => {
                first_error = Some(format!("{key:?} failed during {phase}: {error}"));
                break;
            }
            WorkerEvent::Ready(key) => {
                first_error = Some(format!("late setup receipt from {key:?}"));
                break;
            }
        }
    }
    let timing = CoordinatedTiming {
        wall: latest_finished_at.duration_since(wall_started),
        reader_completion: latest_reader_finished_at.duration_since(wall_started),
        writer_completion: latest_writer_finished_at.duration_since(wall_started),
    };
    if first_error.is_some() {
        let _ = gate.set(StartState::Abort);
    }

    let mut outcomes = Vec::with_capacity(handles.len());
    for handle in handles {
        match handle.join() {
            Ok(Ok(outcome)) => outcomes.push(outcome),
            Ok(Err(error)) => {
                first_error.get_or_insert(error);
            }
            Err(_) => {
                first_error.get_or_insert_with(|| "benchmark worker panicked".to_owned());
            }
        }
    }
    if let Some(error) = first_error {
        return Err(error);
    }
    if timing.reader_completion.is_zero() || timing.writer_completion.is_zero() {
        return Err("role completion timing produced a zero-duration window".to_owned());
    }
    Ok((outcomes, timing))
}

fn arithmetic_sum(first: i64, count: usize) -> Result<i64, String> {
    let first = i128::from(first);
    let count = i128::try_from(count).map_err(|_| "operation count exceeds i128".to_owned())?;
    let value = count
        .checked_mul(
            first
                .checked_mul(2)
                .and_then(|value| value.checked_add(count.saturating_sub(1)))
                .ok_or_else(|| "row-id sum overflow".to_owned())?,
        )
        .and_then(|value| value.checked_div(2))
        .ok_or_else(|| "row-id sum overflow".to_owned())?;
    i64::try_from(value).map_err(|_| "row-id sum exceeds SQLite INTEGER".to_owned())
}

fn expected_database_id_sum(
    seed_rows: i64,
    ops_per_thread: usize,
    num_writers: usize,
) -> Result<i64, String> {
    let seed_count = usize::try_from(seed_rows)
        .map_err(|error| format!("seed row count exceeds usize: {error}"))?;
    let mut sum = i128::from(arithmetic_sum(1, seed_count)?);
    for wid in 0..num_writers {
        let wid = i64::try_from(wid).map_err(|_| "writer index exceeds i64".to_owned())?;
        let first = seed_rows
            .checked_add(1)
            .and_then(|value| value.checked_add(wid.checked_mul(ROWID_BASE_STRIDE)?))
            .ok_or_else(|| "writer row-id range overflow".to_owned())?;
        sum = sum
            .checked_add(i128::from(arithmetic_sum(first, ops_per_thread)?))
            .ok_or_else(|| "database id sum overflow".to_owned())?;
    }
    i64::try_from(sum).map_err(|_| "database id sum exceeds SQLite INTEGER".to_owned())
}

fn expected_operation_count(threads: usize, ops_per_thread: usize) -> Result<u64, String> {
    u64::try_from(threads)
        .ok()
        .and_then(|threads| {
            u64::try_from(ops_per_thread)
                .ok()
                .and_then(|ops| threads.checked_mul(ops))
        })
        .ok_or_else(|| "expected operation count exceeds u64".to_owned())
}

fn expected_payload_bytes(rows: i64) -> Result<i64, String> {
    i64::try_from(PAYLOAD_SIZE)
        .ok()
        .and_then(|payload_size| rows.checked_mul(payload_size))
        .ok_or_else(|| "expected database payload bytes exceed SQLite INTEGER".to_owned())
}

fn expected_id_ranges(
    seed_rows: i64,
    ops_per_thread: usize,
    num_writers: usize,
) -> Result<Vec<(String, i64, i64, i64)>, String> {
    let mut ranges = Vec::with_capacity(num_writers.saturating_add(1));
    ranges.push(("seed".to_owned(), 1, seed_rows, seed_rows));
    let expected_writer_rows = i64::try_from(ops_per_thread)
        .map_err(|_| "writer operation count exceeds SQLite INTEGER".to_owned())?;
    for writer_index in 0..num_writers {
        let writer = i64::try_from(writer_index)
            .map_err(|_| "writer index exceeds SQLite INTEGER".to_owned())?;
        let lower = seed_rows
            .checked_add(1)
            .and_then(|value| value.checked_add(writer.checked_mul(ROWID_BASE_STRIDE)?))
            .ok_or_else(|| "writer row-id range overflow".to_owned())?;
        let upper = lower
            .checked_add(expected_writer_rows.saturating_sub(1))
            .ok_or_else(|| "writer row-id range overflow".to_owned())?;
        ranges.push((
            format!("writer_{writer_index}"),
            lower,
            upper,
            expected_writer_rows,
        ));
    }
    Ok(ranges)
}

fn query_fsqlite_id_range_receipts(
    conn: &fsqlite::Connection,
    seed_rows: i64,
    ops_per_thread: usize,
    num_writers: usize,
) -> Result<Vec<IdRangeReceipt>, String> {
    expected_id_ranges(seed_rows, ops_per_thread, num_writers)?
        .into_iter()
        .map(|(label, lower, upper, expected_rows)| {
            let row = fsqlite_e2e::block_on(conn.query_row_with_params(
                "SELECT COUNT(*) FROM bench WHERE id BETWEEN ?1 AND ?2",
                &[
                    fsqlite::SqliteValue::Integer(lower),
                    fsqlite::SqliteValue::Integer(upper),
                ],
            ))
            .map_err(|error| {
                format!(
                    "FrankenSQLite postflight range `{label}` [{lower}, {upper}] failed: {error}"
                )
            })?;
            let observed_rows = row
                .get(0)
                .and_then(fsqlite::SqliteValue::as_integer)
                .ok_or_else(|| {
                    format!("FrankenSQLite postflight range `{label}` COUNT was not an integer")
                })?;
            Ok(IdRangeReceipt {
                label,
                lower_inclusive: lower,
                upper_inclusive: upper,
                expected_rows,
                observed_rows,
            })
        })
        .collect()
}

fn query_rusqlite_id_range_receipts(
    conn: &rusqlite::Connection,
    seed_rows: i64,
    ops_per_thread: usize,
    num_writers: usize,
) -> Result<Vec<IdRangeReceipt>, String> {
    expected_id_ranges(seed_rows, ops_per_thread, num_writers)?
        .into_iter()
        .map(|(label, lower, upper, expected_rows)| {
            let observed_rows = conn
                .query_row(
                    "SELECT COUNT(*) FROM bench WHERE id BETWEEN ?1 AND ?2",
                    rusqlite::params![lower, upper],
                    |row| row.get(0),
                )
                .map_err(|error| {
                    format!(
                        "C SQLite postflight range `{label}` [{lower}, {upper}] failed: {error}"
                    )
                })?;
            Ok(IdRangeReceipt {
                label,
                lower_inclusive: lower,
                upper_inclusive: upper,
                expected_rows,
                observed_rows,
            })
        })
        .collect()
}

fn validate_work_receipt(receipt: &WorkReceipt) -> Result<(), String> {
    if receipt
        .minimum_expected_reads
        .saturating_add(receipt.expected_writes)
        == 0
    {
        return Err("work receipt contains zero expected operations".to_owned());
    }
    if receipt.completed_reads < receipt.minimum_expected_reads {
        return Err(format!(
            "read work mismatch: expected at least {}, completed {}",
            receipt.minimum_expected_reads, receipt.completed_reads
        ));
    }
    if receipt.reads_with_verified_writer_call_interval_overlap != receipt.completed_reads {
        return Err(format!(
            "read overlap mismatch: {} reads completed but only {} query intervals overlap a timed writer-call interval",
            receipt.completed_reads, receipt.reads_with_verified_writer_call_interval_overlap
        ));
    }
    if receipt.completed_writes != receipt.expected_writes {
        return Err(format!(
            "write work mismatch: expected {}, completed {}",
            receipt.expected_writes, receipt.completed_writes
        ));
    }
    if receipt.consumed_read_payload_bytes != receipt.expected_consumed_read_payload_bytes {
        return Err(format!(
            "read payload mismatch: expected {} bytes, consumed {}",
            receipt.expected_consumed_read_payload_bytes, receipt.consumed_read_payload_bytes
        ));
    }
    if receipt.observed_database_rows_before != receipt.expected_database_rows_before {
        return Err(format!(
            "seed row mismatch: expected {}, observed {}",
            receipt.expected_database_rows_before, receipt.observed_database_rows_before
        ));
    }
    if receipt.observed_database_id_sum_before != receipt.expected_database_id_sum_before {
        return Err(format!(
            "seed id-sum mismatch: expected {}, observed {}",
            receipt.expected_database_id_sum_before, receipt.observed_database_id_sum_before
        ));
    }
    if receipt.observed_database_payload_bytes_before
        != receipt.expected_database_payload_bytes_before
    {
        return Err(format!(
            "seed payload-byte mismatch: expected {}, observed {}",
            receipt.expected_database_payload_bytes_before,
            receipt.observed_database_payload_bytes_before
        ));
    }
    if receipt.observed_matching_payload_rows_before
        != receipt.expected_matching_payload_rows_before
    {
        return Err(format!(
            "seed payload-content mismatch: expected {} matching rows, observed {}",
            receipt.expected_matching_payload_rows_before,
            receipt.observed_matching_payload_rows_before
        ));
    }
    if receipt.observed_database_rows_after != receipt.expected_database_rows_after {
        return Err(format!(
            "postflight row mismatch: expected {}, observed {}",
            receipt.expected_database_rows_after, receipt.observed_database_rows_after
        ));
    }
    if receipt.observed_database_id_sum_after != receipt.expected_database_id_sum_after {
        return Err(format!(
            "postflight id-sum mismatch: expected {}, observed {}",
            receipt.expected_database_id_sum_after, receipt.observed_database_id_sum_after
        ));
    }
    if receipt.observed_database_payload_bytes_after
        != receipt.expected_database_payload_bytes_after
    {
        return Err(format!(
            "postflight payload-byte mismatch: expected {}, observed {}",
            receipt.expected_database_payload_bytes_after,
            receipt.observed_database_payload_bytes_after
        ));
    }
    if receipt.observed_matching_payload_rows_after != receipt.expected_matching_payload_rows_after
    {
        return Err(format!(
            "postflight payload-content mismatch: expected {} matching rows, observed {}",
            receipt.expected_matching_payload_rows_after,
            receipt.observed_matching_payload_rows_after
        ));
    }
    if receipt.id_range_receipts_after.is_empty() {
        return Err("postflight ID-range receipt set is empty".to_owned());
    }
    for range in &receipt.id_range_receipts_after {
        let range_width = range
            .upper_inclusive
            .checked_sub(range.lower_inclusive)
            .and_then(|width| width.checked_add(1));
        if range_width != Some(range.expected_rows) {
            return Err(format!(
                "postflight ID-range `{}` bounds [{}, {}] contain {:?} integers, expected {}",
                range.label,
                range.lower_inclusive,
                range.upper_inclusive,
                range_width,
                range.expected_rows
            ));
        }
        if range.observed_rows != range.expected_rows {
            return Err(format!(
                "postflight ID-range `{}` mismatch: expected {}, observed {}",
                range.label, range.expected_rows, range.observed_rows
            ));
        }
    }
    Ok(())
}

fn query_fsqlite_database_state(conn: &fsqlite::Connection) -> Result<DatabaseState, String> {
    let expected_payload = fsqlite::SqliteValue::Text(make_payload().into());
    let rows = fsqlite_e2e::block_on(conn.query_with_params(
        "SELECT COUNT(*), COALESCE(SUM(id), 0), \
         COALESCE(SUM(length(payload)), 0), \
         COALESCE(SUM(CASE WHEN payload = ?1 THEN 1 ELSE 0 END), 0) FROM bench",
        &[expected_payload],
    ))
    .map_err(|error| format!("FrankenSQLite postflight query failed: {error}"))?;
    if rows.len() != 1 {
        return Err(format!(
            "FrankenSQLite postflight returned {} rows, expected one",
            rows.len()
        ));
    }
    let count = rows[0]
        .get(0)
        .and_then(fsqlite::SqliteValue::as_integer)
        .ok_or_else(|| "FrankenSQLite postflight COUNT was not an integer".to_owned())?;
    let sum = rows[0]
        .get(1)
        .and_then(fsqlite::SqliteValue::as_integer)
        .ok_or_else(|| "FrankenSQLite postflight SUM was not an integer".to_owned())?;
    let payload_bytes = rows[0]
        .get(2)
        .and_then(fsqlite::SqliteValue::as_integer)
        .ok_or_else(|| "FrankenSQLite postflight payload-byte SUM was not an integer".to_owned())?;
    let matching_payload_rows = rows[0]
        .get(3)
        .and_then(fsqlite::SqliteValue::as_integer)
        .ok_or_else(|| {
            "FrankenSQLite postflight matching-payload SUM was not an integer".to_owned()
        })?;
    Ok(DatabaseState {
        rows: count,
        id_sum: sum,
        payload_bytes,
        matching_payload_rows,
    })
}

fn query_rusqlite_database_state(conn: &rusqlite::Connection) -> Result<DatabaseState, String> {
    let expected_payload = make_payload();
    conn.query_row(
        "SELECT COUNT(*), COALESCE(SUM(id), 0), \
         COALESCE(SUM(length(payload)), 0), \
         COALESCE(SUM(CASE WHEN payload = ?1 THEN 1 ELSE 0 END), 0) FROM bench",
        rusqlite::params![expected_payload],
        |row| {
            Ok(DatabaseState {
                rows: row.get(0)?,
                id_sum: row.get(1)?,
                payload_bytes: row.get(2)?,
                matching_payload_rows: row.get(3)?,
            })
        },
    )
    .map_err(|error| format!("C SQLite postflight query failed: {error}"))
}

async fn rollback_fsqlite(conn: &fsqlite::Connection, context: &str) -> Result<(), String> {
    conn.execute("ROLLBACK")
        .await
        .map(|_| ())
        .map_err(|error| format!("{context}; rollback failed: {error}"))
}

/// Outcome of a single write-transaction attempt (bd-mnlk2 / bd-zavyn).
enum WriteAttempt {
    Committed,
    /// A transient BEGIN/INSERT/COMMIT failure that already rolled back
    /// (where required) inside the runtime entry; the caller backs off
    /// outside the runtime and retries.
    Retry,
}

/// bd-mnlk2 / bd-zavyn: one runtime entry per transaction attempt. The
/// previous shape entered the harness runtime for every
/// BEGIN/INSERT/COMMIT/ROLLBACK (3+ entries of ~333 ns per written row,
/// FrankenSQLite side only), so the per-operation latency samples measured
/// the bridge as well as the engine. The retry backoff sleeps *outside* the
/// entered runtime (Gate 0 requirement: never hold a sync sleep inside a
/// current-thread runtime that owns engine progress).
fn execute_fsqlite_write(
    conn: &fsqlite::Connection,
    stmt: &fsqlite::PreparedStatement<'_>,
    id: i64,
    payload: &fsqlite::SqliteValue,
) -> Result<(), String> {
    for attempt in 1..=MAX_RETRIES {
        let outcome = fsqlite_e2e::block_on(async {
            match conn.execute("BEGIN CONCURRENT").await {
                Ok(_) => {}
                Err(error) if error.is_transient() && attempt < MAX_RETRIES => {
                    return Ok(WriteAttempt::Retry);
                }
                Err(error) => {
                    return Err(format!(
                        "BEGIN CONCURRENT failed for row {id} after {attempt} attempt(s): {error}"
                    ));
                }
            }

            match stmt
                .execute_with_params(&[fsqlite::SqliteValue::Integer(id), payload.clone()])
                .await
            {
                Ok(1) => {}
                Ok(affected) => {
                    rollback_fsqlite(
                        conn,
                        &format!("insert for row {id} affected {affected} rows, expected one"),
                    )
                    .await?;
                    return Err(format!(
                        "insert for row {id} affected {affected} rows, expected one"
                    ));
                }
                Err(error) if error.is_transient() && attempt < MAX_RETRIES => {
                    rollback_fsqlite(
                        conn,
                        &format!("transient insert failure for row {id}: {error}"),
                    )
                    .await?;
                    return Ok(WriteAttempt::Retry);
                }
                Err(error) => {
                    rollback_fsqlite(conn, &format!("insert failed for row {id}: {error}")).await?;
                    return Err(format!(
                        "insert failed for row {id} after {attempt} attempt(s): {error}"
                    ));
                }
            }

            match conn.execute("COMMIT").await {
                Ok(_) => Ok(WriteAttempt::Committed),
                Err(error) if error.is_transient() && attempt < MAX_RETRIES => {
                    rollback_fsqlite(
                        conn,
                        &format!("transient commit failure for row {id}: {error}"),
                    )
                    .await?;
                    Ok(WriteAttempt::Retry)
                }
                Err(error) => {
                    rollback_fsqlite(conn, &format!("commit failed for row {id}: {error}")).await?;
                    Err(format!(
                        "commit failed for row {id} after {attempt} attempt(s): {error}"
                    ))
                }
            }
        })?;

        match outcome {
            WriteAttempt::Committed => return Ok(()),
            WriteAttempt::Retry => thread::sleep(RETRY_SLEEP),
        }
    }
    Err(format!(
        "write for row {id} exhausted {MAX_RETRIES} attempts"
    ))
}

fn is_transient_rusqlite_error(error: &rusqlite::Error) -> bool {
    matches!(
        error.sqlite_error_code(),
        Some(rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked)
    )
}

fn execute_rusqlite_write(
    conn: &rusqlite::Connection,
    stmt: &mut rusqlite::Statement<'_>,
    id: i64,
    payload: &str,
) -> Result<(), String> {
    for attempt in 1..=MAX_RETRIES {
        match conn.execute_batch("BEGIN") {
            Ok(()) => {}
            Err(error) if is_transient_rusqlite_error(&error) && attempt < MAX_RETRIES => {
                thread::sleep(RETRY_SLEEP);
                continue;
            }
            Err(error) => {
                return Err(format!(
                    "C SQLite BEGIN failed for row {id} after {attempt} attempt(s): {error}"
                ));
            }
        }

        match stmt.execute(rusqlite::params![id, payload]) {
            Ok(1) => {}
            Ok(affected) => {
                conn.execute_batch("ROLLBACK").map_err(|error| {
                    format!(
                        "C SQLite insert for row {id} affected {affected} rows; rollback failed: {error}"
                    )
                })?;
                return Err(format!(
                    "C SQLite insert for row {id} affected {affected} rows, expected one"
                ));
            }
            Err(error) if is_transient_rusqlite_error(&error) && attempt < MAX_RETRIES => {
                conn.execute_batch("ROLLBACK").map_err(|rollback_error| {
                    format!(
                        "C SQLite transient insert failure for row {id}: {error}; rollback failed: {rollback_error}"
                    )
                })?;
                thread::sleep(RETRY_SLEEP);
                continue;
            }
            Err(error) => {
                conn.execute_batch("ROLLBACK").map_err(|rollback_error| {
                    format!(
                        "C SQLite insert failure for row {id}: {error}; rollback failed: {rollback_error}"
                    )
                })?;
                return Err(format!(
                    "C SQLite insert for row {id} failed after {attempt} attempt(s): {error}"
                ));
            }
        }

        match conn.execute_batch("COMMIT") {
            Ok(()) => return Ok(()),
            Err(error) if is_transient_rusqlite_error(&error) && attempt < MAX_RETRIES => {
                conn.execute_batch("ROLLBACK").map_err(|rollback_error| {
                    format!(
                        "C SQLite transient commit failure for row {id}: {error}; rollback failed: {rollback_error}"
                    )
                })?;
                thread::sleep(RETRY_SLEEP);
            }
            Err(error) => {
                conn.execute_batch("ROLLBACK").map_err(|rollback_error| {
                    format!(
                        "C SQLite commit failure for row {id}: {error}; rollback failed: {rollback_error}"
                    )
                })?;
                return Err(format!(
                    "C SQLite commit for row {id} failed after {attempt} attempt(s): {error}"
                ));
            }
        }
    }
    Err(format!(
        "C SQLite write for row {id} exhausted {MAX_RETRIES} attempts"
    ))
}

// ─── FrankenSQLite engine ───────────────────────────────────────────────

fn run_fsqlite_iter(
    seed_rows: i64,
    ops_per_thread: usize,
    num_readers: usize,
    num_writers: usize,
) -> Result<IterResult, String> {
    let temp_dir = tempfile::tempdir()
        .map_err(|error| format!("FrankenSQLite temp directory creation failed: {error}"))?;
    let path = temp_dir
        .path()
        .join("bench.db")
        .to_string_lossy()
        .into_owned();

    let expected_seed_sum = arithmetic_sum(
        1,
        usize::try_from(seed_rows)
            .map_err(|error| format!("seed row count cannot be represented as usize: {error}"))?,
    )?;
    let seed_database_state = {
        let conn = fsqlite_e2e::block_on(fsqlite::Connection::open(path.clone()))
            .map_err(|error| format!("FrankenSQLite seed open failed: {error}"))?;
        for pragma in [
            "PRAGMA page_size=4096;",
            "PRAGMA journal_mode=WAL;",
            "PRAGMA synchronous=NORMAL;",
            "PRAGMA cache_size=-64000;",
            "PRAGMA busy_timeout=5000;",
            "PRAGMA fsqlite.concurrent_mode=ON;",
        ] {
            fsqlite_e2e::block_on(conn.execute(pragma))
                .map_err(|error| format!("FrankenSQLite seed `{pragma}` failed: {error}"))?;
        }
        configure_fsqlite_worker(&conn)?;
        fsqlite_e2e::block_on(
            conn.execute("CREATE TABLE bench (id INTEGER PRIMARY KEY, payload TEXT NOT NULL)"),
        )
        .map_err(|error| format!("FrankenSQLite create table failed: {error}"))?;
        fsqlite_e2e::block_on(conn.execute("BEGIN CONCURRENT"))
            .map_err(|error| format!("FrankenSQLite seed BEGIN CONCURRENT failed: {error}"))?;
        let stmt =
            fsqlite_e2e::block_on(conn.prepare("INSERT INTO bench (id, payload) VALUES (?1, ?2)"))
                .map_err(|error| format!("FrankenSQLite seed prepare failed: {error}"))?;
        let payload = make_payload();
        for id in 1..=seed_rows {
            let affected = fsqlite_e2e::block_on(stmt.execute_with_params(&[
                fsqlite::SqliteValue::Integer(id),
                fsqlite::SqliteValue::Text(payload.clone().into()),
            ]))
            .map_err(|error| format!("FrankenSQLite seed insert {id} failed: {error}"))?;
            if affected != 1 {
                return Err(format!(
                    "FrankenSQLite seed insert {id} affected {affected} rows, expected one"
                ));
            }
        }
        fsqlite_e2e::block_on(conn.execute("COMMIT"))
            .map_err(|error| format!("FrankenSQLite seed COMMIT failed: {error}"))?;
        checkpoint_fsqlite_seed(&conn)?;
        let observed = query_fsqlite_database_state(&conn)?;
        let expected_seed_payload_bytes = expected_payload_bytes(seed_rows)?;
        if observed.rows != seed_rows
            || observed.id_sum != expected_seed_sum
            || observed.payload_bytes != expected_seed_payload_bytes
            || observed.matching_payload_rows != seed_rows
        {
            return Err(format!(
                "FrankenSQLite seed verification mismatch: rows {}/{seed_rows}, id sum {}/{expected_seed_sum}, payload bytes {}/{expected_seed_payload_bytes}, matching payload rows {}/{seed_rows}",
                observed.rows,
                observed.id_sum,
                observed.payload_bytes,
                observed.matching_payload_rows
            ));
        }
        observed
    };

    let total_threads = num_readers + num_writers;
    let path = Arc::new(path);
    let gate = Arc::new(StartGate::new());
    let writers_remaining = Arc::new(AtomicUsize::new(num_writers));
    let active_writer_operations = Arc::new(AtomicUsize::new(0));
    let readers_waiting_for_overlap = Arc::new(AtomicUsize::new(0));
    let (event_tx, event_rx) = mpsc::sync_channel(total_threads.saturating_mul(2).max(1));
    let mut handles = Vec::with_capacity(total_threads);

    for rid in 0..num_readers {
        let path = Arc::clone(&path);
        let gate = Arc::clone(&gate);
        let writers_remaining = Arc::clone(&writers_remaining);
        let active_writer_operations = Arc::clone(&active_writer_operations);
        let readers_waiting_for_overlap = Arc::clone(&readers_waiting_for_overlap);
        let event_tx = event_tx.clone();
        handles.push(thread::spawn(move || {
            let key = WorkerKey {
                role: WorkerRole::Reader,
                tid: rid,
            };
            let mut phase = "setup";
            let result = (|| -> Result<WorkerOutcome, String> {
                let conn =
                    fsqlite_e2e::block_on(fsqlite::Connection::open(path.as_str().to_owned()))
                        .map_err(|error| {
                            format!("FrankenSQLite reader {rid} open failed: {error}")
                        })?;
                let settings = configure_fsqlite_worker(&conn)?;
                let stmt =
                    fsqlite_e2e::block_on(conn.prepare("SELECT payload FROM bench WHERE id = ?1"))
                        .map_err(|error| {
                            format!("FrankenSQLite reader {rid} prepare failed: {error}")
                        })?;
                let mut latencies = Vec::with_capacity(ops_per_thread);
                let mut operation_intervals = Vec::with_capacity(ops_per_thread);
                let mut completed_ops = 0u64;
                let mut consumed_read_payload_bytes = 0u64;
                let mut rng_state = 0x0102_0304_u64 ^ (rid as u64).wrapping_mul(0x9e37);
                let expected_payload = make_payload();
                send_worker_event(&event_tx, WorkerEvent::Ready(key))?;
                gate.wait(key)?;
                phase = "timed work";
                let started = Instant::now();
                let previous =
                    readers_waiting_for_overlap.fetch_add(1, AtomicOrdering::AcqRel);
                if previous >= num_readers {
                    return Err(
                        "FrankenSQLite reader-overlap readiness counter overflowed".to_owned()
                    );
                }
                // bd-mnlk2 / bd-zavyn: the entire admission-gated read loop
                // runs inside ONE runtime entry, so each latency sample
                // brackets the engine query alone rather than a ~333 ns
                // bridge entry per read (the C SQLite reader arm pays no
                // bridge). The sync gate waits block this thread exactly as
                // they did outside the runtime: nothing else is scheduled on
                // this per-thread runtime.
                fsqlite_e2e::block_on(async {
                    while wait_for_active_writer_operation(
                        &gate,
                        key,
                        &active_writer_operations,
                        &writers_remaining,
                    )? {
                        gate.ensure_running(key)?;
                        if completed_ops >= MAX_READER_OPS_PER_THREAD {
                            return Err(format!(
                                "FrankenSQLite reader {rid} exceeded the fail-closed cap of {MAX_READER_OPS_PER_THREAD} operations while writers were still active"
                            ));
                        }
                        #[allow(clippy::cast_possible_wrap)]
                        let id = (lcg_next(&mut rng_state) % seed_rows as u64 + 1) as i64;
                        let operation_started = Instant::now();
                        let row = stmt
                            .query_row_with_params(&[fsqlite::SqliteValue::Integer(id)])
                            .await
                            .map_err(|error| {
                                format!(
                                    "FrankenSQLite reader {rid} query for row {id} failed: {error}"
                                )
                            })?;
                        let query_finished = Instant::now();
                        let payload = row
                            .get(0)
                            .and_then(fsqlite::SqliteValue::as_text)
                            .ok_or_else(|| {
                                format!(
                                    "FrankenSQLite reader {rid} row {id} payload was not TEXT"
                                )
                            })?;
                        if payload != expected_payload {
                            return Err(format!(
                                "FrankenSQLite reader {rid} row {id} payload mismatch"
                            ));
                        }
                        consumed_read_payload_bytes = consumed_read_payload_bytes
                            .checked_add(payload.len() as u64)
                            .ok_or_else(|| "read payload byte receipt overflow".to_owned())?;
                        let operation_finished = Instant::now();
                        completed_ops += 1;
                        latencies.push(
                            operation_finished
                                .duration_since(operation_started)
                                .as_nanos() as u64,
                        );
                        operation_intervals.push(TimedOperationInterval {
                            started_at: operation_started,
                            finished_at: query_finished,
                        });
                    }
                    Ok::<(), String>(())
                })?;
                let finished_at = Instant::now();
                let elapsed = finished_at.duration_since(started);
                #[allow(clippy::cast_precision_loss)]
                let ops_per_sec = completed_ops as f64 / elapsed.as_secs_f64();
                Ok(WorkerOutcome {
                    report: ThreadReport {
                        role: key.role.as_str().to_owned(),
                        tid: rid,
                        expected_ops: 1,
                        completed_ops,
                        failed_ops: u64::from(completed_ops == 0),
                        elapsed_ms: elapsed.as_secs_f64() * 1_000.0,
                        ops_per_sec,
                        latency: compute_latency_stats(latencies.clone()),
                        settings,
                    },
                    latencies_ns: latencies,
                    operation_intervals,
                    consumed_read_payload_bytes,
                    finished_at,
                })
            })();
            match result {
                Ok(outcome) => {
                    send_worker_event(
                        &event_tx,
                        WorkerEvent::Completed {
                            key,
                            completed_ops: outcome.report.completed_ops,
                            finished_at: outcome.finished_at,
                        },
                    )?;
                    Ok(outcome)
                }
                Err(error) => {
                    let _ = send_worker_event(
                        &event_tx,
                        WorkerEvent::Failed {
                            key,
                            phase,
                            error: error.clone(),
                        },
                    );
                    Err(error)
                }
            }
        }));
    }

    for wid in 0..num_writers {
        let path = Arc::clone(&path);
        let gate = Arc::clone(&gate);
        let writers_remaining = Arc::clone(&writers_remaining);
        let active_writer_operations = Arc::clone(&active_writer_operations);
        let readers_waiting_for_overlap = Arc::clone(&readers_waiting_for_overlap);
        let event_tx = event_tx.clone();
        handles.push(thread::spawn(move || {
            let key = WorkerKey {
                role: WorkerRole::Writer,
                tid: wid,
            };
            let mut phase = "setup";
            let result = (|| -> Result<WorkerOutcome, String> {
                let conn =
                    fsqlite_e2e::block_on(fsqlite::Connection::open(path.as_str().to_owned()))
                        .map_err(|error| {
                            format!("FrankenSQLite writer {wid} open failed: {error}")
                        })?;
                let settings = configure_fsqlite_worker(&conn)?;
                let stmt = fsqlite_e2e::block_on(
                    conn.prepare("INSERT INTO bench (id, payload) VALUES (?1, ?2)"),
                )
                .map_err(|error| format!("FrankenSQLite writer {wid} prepare failed: {error}"))?;
                let mut latencies = Vec::with_capacity(ops_per_thread);
                let mut operation_intervals = Vec::with_capacity(ops_per_thread);
                let mut completed_ops = 0u64;
                #[allow(clippy::cast_possible_wrap)]
                let base_id = seed_rows + 1 + (wid as i64 * ROWID_BASE_STRIDE);
                let payload = fsqlite::SqliteValue::Text(make_payload().into());
                send_worker_event(&event_tx, WorkerEvent::Ready(key))?;
                gate.wait(key)?;
                phase = "timed work";
                let started = Instant::now();
                wait_for_counter(
                    &gate,
                    key,
                    &readers_waiting_for_overlap,
                    num_readers,
                    "every reader to enter the active-writer admission loop",
                )?;
                for i in 0..ops_per_thread {
                    gate.ensure_running(key)?;
                    let id = base_id + i as i64;
                    let active_before =
                        active_writer_operations.fetch_add(1, AtomicOrdering::AcqRel);
                    if active_before >= num_writers {
                        active_writer_operations.fetch_sub(1, AtomicOrdering::AcqRel);
                        return Err(
                            "FrankenSQLite active-writer operation counter overflowed".to_owned()
                        );
                    }
                    let operation_started = Instant::now();
                    let write_result = execute_fsqlite_write(&conn, &stmt, id, &payload);
                    let operation_finished = Instant::now();
                    let active_before_decrement =
                        active_writer_operations.fetch_sub(1, AtomicOrdering::AcqRel);
                    if active_before_decrement == 0 {
                        return Err(
                            "FrankenSQLite active-writer operation counter underflowed".to_owned()
                        );
                    }
                    write_result?;
                    completed_ops += 1;
                    latencies.push(
                        operation_finished
                            .duration_since(operation_started)
                            .as_nanos() as u64,
                    );
                    operation_intervals.push(TimedOperationInterval {
                        started_at: operation_started,
                        finished_at: operation_finished,
                    });
                }
                let previous = writers_remaining.fetch_sub(1, AtomicOrdering::AcqRel);
                if previous == 0 {
                    return Err("FrankenSQLite writer countdown underflowed".to_owned());
                }
                let finished_at = Instant::now();
                let elapsed = finished_at.duration_since(started);
                #[allow(clippy::cast_precision_loss)]
                let ops_per_sec = completed_ops as f64 / elapsed.as_secs_f64();
                Ok(WorkerOutcome {
                    report: ThreadReport {
                        role: key.role.as_str().to_owned(),
                        tid: wid,
                        expected_ops: ops_per_thread as u64,
                        completed_ops,
                        failed_ops: (ops_per_thread as u64).saturating_sub(completed_ops),
                        elapsed_ms: elapsed.as_secs_f64() * 1_000.0,
                        ops_per_sec,
                        latency: compute_latency_stats(latencies.clone()),
                        settings,
                    },
                    latencies_ns: latencies,
                    operation_intervals,
                    consumed_read_payload_bytes: 0,
                    finished_at,
                })
            })();
            match result {
                Ok(outcome) => {
                    send_worker_event(
                        &event_tx,
                        WorkerEvent::Completed {
                            key,
                            completed_ops: outcome.report.completed_ops,
                            finished_at: outcome.finished_at,
                        },
                    )?;
                    Ok(outcome)
                }
                Err(error) => {
                    let _ = send_worker_event(
                        &event_tx,
                        WorkerEvent::Failed {
                            key,
                            phase,
                            error: error.clone(),
                        },
                    );
                    Err(error)
                }
            }
        }));
    }

    drop(event_tx);
    let keys = expected_worker_keys(num_readers, num_writers);
    let (outcomes, timing) = coordinate_workers(&keys, &event_rx, &gate, handles)?;
    let verifier = fsqlite_e2e::block_on(fsqlite::Connection::open(path.as_str().to_owned()))
        .map_err(|error| format!("FrankenSQLite verifier open failed: {error}"))?;
    configure_fsqlite_worker(&verifier)?;
    let database_state = query_fsqlite_database_state(&verifier)?;
    let id_range_receipts_after =
        query_fsqlite_id_range_receipts(&verifier, seed_rows, ops_per_thread, num_writers)?;
    build_iter_result(IterBuildInputs {
        outcomes,
        timing,
        seed_rows,
        ops_per_thread,
        num_readers,
        num_writers,
        seed_database_state,
        database_state,
        id_range_receipts_after,
    })
}

// ─── C SQLite (rusqlite) engine ─────────────────────────────────────────

fn run_rusqlite_iter(
    seed_rows: i64,
    ops_per_thread: usize,
    num_readers: usize,
    num_writers: usize,
) -> Result<IterResult, String> {
    let temp_dir = tempfile::tempdir()
        .map_err(|error| format!("C SQLite temp directory creation failed: {error}"))?;
    let path = temp_dir
        .path()
        .join("bench.db")
        .to_string_lossy()
        .into_owned();

    let expected_seed_sum = arithmetic_sum(
        1,
        usize::try_from(seed_rows)
            .map_err(|error| format!("seed row count cannot be represented as usize: {error}"))?,
    )?;
    let seed_database_state = {
        let conn = rusqlite::Connection::open(&path)
            .map_err(|error| format!("C SQLite seed open failed: {error}"))?;
        conn.execute_batch(
            "PRAGMA page_size=4096; \
             PRAGMA journal_mode=WAL; \
             PRAGMA synchronous=NORMAL; \
             PRAGMA cache_size=-64000; \
             PRAGMA busy_timeout=5000;",
        )
        .map_err(|error| format!("C SQLite seed PRAGMAs failed: {error}"))?;
        configure_rusqlite_worker(&conn)?;
        conn.execute_batch("CREATE TABLE bench (id INTEGER PRIMARY KEY, payload TEXT NOT NULL);")
            .map_err(|error| format!("C SQLite create table failed: {error}"))?;
        conn.execute_batch("BEGIN")
            .map_err(|error| format!("C SQLite seed BEGIN failed: {error}"))?;
        let mut stmt = conn
            .prepare("INSERT INTO bench (id, payload) VALUES (?1, ?2)")
            .map_err(|error| format!("C SQLite seed prepare failed: {error}"))?;
        let payload = make_payload();
        for id in 1..=seed_rows {
            let affected = stmt
                .execute(rusqlite::params![id, payload])
                .map_err(|error| format!("C SQLite seed insert {id} failed: {error}"))?;
            if affected != 1 {
                return Err(format!(
                    "C SQLite seed insert {id} affected {affected} rows, expected one"
                ));
            }
        }
        drop(stmt);
        conn.execute_batch("COMMIT")
            .map_err(|error| format!("C SQLite seed COMMIT failed: {error}"))?;
        checkpoint_rusqlite_seed(&conn)?;
        let observed = query_rusqlite_database_state(&conn)?;
        let expected_seed_payload_bytes = expected_payload_bytes(seed_rows)?;
        if observed.rows != seed_rows
            || observed.id_sum != expected_seed_sum
            || observed.payload_bytes != expected_seed_payload_bytes
            || observed.matching_payload_rows != seed_rows
        {
            return Err(format!(
                "C SQLite seed verification mismatch: rows {}/{seed_rows}, id sum {}/{expected_seed_sum}, payload bytes {}/{expected_seed_payload_bytes}, matching payload rows {}/{seed_rows}",
                observed.rows,
                observed.id_sum,
                observed.payload_bytes,
                observed.matching_payload_rows
            ));
        }
        observed
    };

    let total_threads = num_readers + num_writers;
    let path = Arc::new(path);
    let gate = Arc::new(StartGate::new());
    let writers_remaining = Arc::new(AtomicUsize::new(num_writers));
    let active_writer_operations = Arc::new(AtomicUsize::new(0));
    let readers_waiting_for_overlap = Arc::new(AtomicUsize::new(0));
    let (event_tx, event_rx) = mpsc::sync_channel(total_threads.saturating_mul(2).max(1));
    let mut handles = Vec::with_capacity(total_threads);

    for rid in 0..num_readers {
        let path = Arc::clone(&path);
        let gate = Arc::clone(&gate);
        let writers_remaining = Arc::clone(&writers_remaining);
        let active_writer_operations = Arc::clone(&active_writer_operations);
        let readers_waiting_for_overlap = Arc::clone(&readers_waiting_for_overlap);
        let event_tx = event_tx.clone();
        handles.push(thread::spawn(move || {
            let key = WorkerKey {
                role: WorkerRole::Reader,
                tid: rid,
            };
            let mut phase = "setup";
            let result = (|| -> Result<WorkerOutcome, String> {
                let conn = rusqlite::Connection::open(path.as_str())
                    .map_err(|error| format!("C SQLite reader {rid} open failed: {error}"))?;
                let settings = configure_rusqlite_worker(&conn)?;
                let mut stmt = conn
                    .prepare("SELECT payload FROM bench WHERE id = ?1")
                    .map_err(|error| format!("C SQLite reader {rid} prepare failed: {error}"))?;
                let mut latencies = Vec::with_capacity(ops_per_thread);
                let mut operation_intervals = Vec::with_capacity(ops_per_thread);
                let mut completed_ops = 0u64;
                let mut consumed_read_payload_bytes = 0u64;
                let mut rng_state = 0x0102_0304_u64 ^ (rid as u64).wrapping_mul(0x9e37);
                let expected_payload = make_payload();
                send_worker_event(&event_tx, WorkerEvent::Ready(key))?;
                gate.wait(key)?;
                phase = "timed work";

                let started = Instant::now();
                let previous =
                    readers_waiting_for_overlap.fetch_add(1, AtomicOrdering::AcqRel);
                if previous >= num_readers {
                    return Err("C SQLite reader-overlap readiness counter overflowed".to_owned());
                }
                #[allow(clippy::cast_possible_wrap)]
                while wait_for_active_writer_operation(
                    &gate,
                    key,
                    &active_writer_operations,
                    &writers_remaining,
                )? {
                    gate.ensure_running(key)?;
                    if completed_ops >= MAX_READER_OPS_PER_THREAD {
                        return Err(format!(
                            "C SQLite reader {rid} exceeded the fail-closed cap of {MAX_READER_OPS_PER_THREAD} operations while writers were still active"
                        ));
                    }
                    let id = (lcg_next(&mut rng_state) % seed_rows as u64 + 1) as i64;
                    let operation_started = Instant::now();
                    let payload: String = stmt
                        .query_row(rusqlite::params![id], |row| row.get(0))
                        .map_err(|error| {
                        format!("C SQLite reader {rid} query for row {id} failed: {error}")
                    })?;
                    let query_finished = Instant::now();
                    if payload != expected_payload {
                        return Err(format!("C SQLite reader {rid} row {id} payload mismatch"));
                    }
                    consumed_read_payload_bytes = consumed_read_payload_bytes
                        .checked_add(payload.len() as u64)
                        .ok_or_else(|| "read payload byte receipt overflow".to_owned())?;
                    let operation_finished = Instant::now();
                    completed_ops += 1;
                    latencies.push(
                        operation_finished
                            .duration_since(operation_started)
                            .as_nanos() as u64,
                    );
                    operation_intervals.push(TimedOperationInterval {
                        started_at: operation_started,
                        finished_at: query_finished,
                    });
                }
                let finished_at = Instant::now();
                let elapsed = finished_at.duration_since(started);
                #[allow(clippy::cast_precision_loss)]
                let ops_per_sec = completed_ops as f64 / elapsed.as_secs_f64();
                Ok(WorkerOutcome {
                    report: ThreadReport {
                        role: key.role.as_str().to_owned(),
                        tid: rid,
                        expected_ops: 1,
                        completed_ops,
                        failed_ops: u64::from(completed_ops == 0),
                        elapsed_ms: elapsed.as_secs_f64() * 1_000.0,
                        ops_per_sec,
                        latency: compute_latency_stats(latencies.clone()),
                        settings,
                    },
                    latencies_ns: latencies,
                    operation_intervals,
                    consumed_read_payload_bytes,
                    finished_at,
                })
            })();
            match result {
                Ok(outcome) => {
                    send_worker_event(
                        &event_tx,
                        WorkerEvent::Completed {
                            key,
                            completed_ops: outcome.report.completed_ops,
                            finished_at: outcome.finished_at,
                        },
                    )?;
                    Ok(outcome)
                }
                Err(error) => {
                    let _ = send_worker_event(
                        &event_tx,
                        WorkerEvent::Failed {
                            key,
                            phase,
                            error: error.clone(),
                        },
                    );
                    Err(error)
                }
            }
        }));
    }

    for wid in 0..num_writers {
        let path = Arc::clone(&path);
        let gate = Arc::clone(&gate);
        let writers_remaining = Arc::clone(&writers_remaining);
        let active_writer_operations = Arc::clone(&active_writer_operations);
        let readers_waiting_for_overlap = Arc::clone(&readers_waiting_for_overlap);
        let event_tx = event_tx.clone();
        handles.push(thread::spawn(move || {
            let key = WorkerKey {
                role: WorkerRole::Writer,
                tid: wid,
            };
            let mut phase = "setup";
            let result = (|| -> Result<WorkerOutcome, String> {
                let conn = rusqlite::Connection::open(path.as_str())
                    .map_err(|error| format!("C SQLite writer {wid} open failed: {error}"))?;
                let settings = configure_rusqlite_worker(&conn)?;
                let mut stmt = conn
                    .prepare("INSERT INTO bench (id, payload) VALUES (?1, ?2)")
                    .map_err(|error| format!("C SQLite writer {wid} prepare failed: {error}"))?;
                let mut latencies = Vec::with_capacity(ops_per_thread);
                let mut operation_intervals = Vec::with_capacity(ops_per_thread);
                let mut completed_ops = 0u64;
                #[allow(clippy::cast_possible_wrap)]
                let base_id = seed_rows + 1 + (wid as i64 * ROWID_BASE_STRIDE);
                let payload = make_payload();
                send_worker_event(&event_tx, WorkerEvent::Ready(key))?;
                gate.wait(key)?;
                phase = "timed work";

                let started = Instant::now();
                wait_for_counter(
                    &gate,
                    key,
                    &readers_waiting_for_overlap,
                    num_readers,
                    "every reader to enter the active-writer admission loop",
                )?;
                for i in 0..ops_per_thread {
                    gate.ensure_running(key)?;
                    let id = base_id + i as i64;
                    let active_before =
                        active_writer_operations.fetch_add(1, AtomicOrdering::AcqRel);
                    if active_before >= num_writers {
                        active_writer_operations.fetch_sub(1, AtomicOrdering::AcqRel);
                        return Err(
                            "C SQLite active-writer operation counter overflowed".to_owned()
                        );
                    }
                    let operation_started = Instant::now();
                    let write_result = execute_rusqlite_write(&conn, &mut stmt, id, &payload);
                    let operation_finished = Instant::now();
                    let active_before_decrement =
                        active_writer_operations.fetch_sub(1, AtomicOrdering::AcqRel);
                    if active_before_decrement == 0 {
                        return Err(
                            "C SQLite active-writer operation counter underflowed".to_owned()
                        );
                    }
                    write_result?;
                    completed_ops += 1;
                    latencies.push(
                        operation_finished
                            .duration_since(operation_started)
                            .as_nanos() as u64,
                    );
                    operation_intervals.push(TimedOperationInterval {
                        started_at: operation_started,
                        finished_at: operation_finished,
                    });
                }
                let previous = writers_remaining.fetch_sub(1, AtomicOrdering::AcqRel);
                if previous == 0 {
                    return Err("C SQLite writer countdown underflowed".to_owned());
                }
                let finished_at = Instant::now();
                let elapsed = finished_at.duration_since(started);
                #[allow(clippy::cast_precision_loss)]
                let ops_per_sec = completed_ops as f64 / elapsed.as_secs_f64();
                Ok(WorkerOutcome {
                    report: ThreadReport {
                        role: key.role.as_str().to_owned(),
                        tid: wid,
                        expected_ops: ops_per_thread as u64,
                        completed_ops,
                        failed_ops: (ops_per_thread as u64).saturating_sub(completed_ops),
                        elapsed_ms: elapsed.as_secs_f64() * 1_000.0,
                        ops_per_sec,
                        latency: compute_latency_stats(latencies.clone()),
                        settings,
                    },
                    latencies_ns: latencies,
                    operation_intervals,
                    consumed_read_payload_bytes: 0,
                    finished_at,
                })
            })();
            match result {
                Ok(outcome) => {
                    send_worker_event(
                        &event_tx,
                        WorkerEvent::Completed {
                            key,
                            completed_ops: outcome.report.completed_ops,
                            finished_at: outcome.finished_at,
                        },
                    )?;
                    Ok(outcome)
                }
                Err(error) => {
                    let _ = send_worker_event(
                        &event_tx,
                        WorkerEvent::Failed {
                            key,
                            phase,
                            error: error.clone(),
                        },
                    );
                    Err(error)
                }
            }
        }));
    }

    drop(event_tx);
    let keys = expected_worker_keys(num_readers, num_writers);
    let (outcomes, timing) = coordinate_workers(&keys, &event_rx, &gate, handles)?;
    let verifier = rusqlite::Connection::open(path.as_str())
        .map_err(|error| format!("C SQLite verifier open failed: {error}"))?;
    configure_rusqlite_worker(&verifier)?;
    let database_state = query_rusqlite_database_state(&verifier)?;
    let id_range_receipts_after =
        query_rusqlite_id_range_receipts(&verifier, seed_rows, ops_per_thread, num_writers)?;
    build_iter_result(IterBuildInputs {
        outcomes,
        timing,
        seed_rows,
        ops_per_thread,
        num_readers,
        num_writers,
        seed_database_state,
        database_state,
        id_range_receipts_after,
    })
}

// ─── Result aggregation ─────────────────────────────────────────────────

struct IterBuildInputs {
    outcomes: Vec<WorkerOutcome>,
    timing: CoordinatedTiming,
    seed_rows: i64,
    ops_per_thread: usize,
    num_readers: usize,
    num_writers: usize,
    seed_database_state: DatabaseState,
    database_state: DatabaseState,
    id_range_receipts_after: Vec<IdRangeReceipt>,
}

fn build_iter_result(inputs: IterBuildInputs) -> Result<IterResult, String> {
    let IterBuildInputs {
        outcomes,
        timing,
        seed_rows,
        ops_per_thread,
        num_readers,
        num_writers,
        seed_database_state,
        database_state,
        id_range_receipts_after,
    } = inputs;
    let mut reader_outcomes = Vec::with_capacity(num_readers);
    let mut writer_outcomes = Vec::with_capacity(num_writers);
    for outcome in outcomes {
        match outcome.report.role.as_str() {
            "reader" => reader_outcomes.push(outcome),
            "writer" => writer_outcomes.push(outcome),
            role => return Err(format!("unknown worker role `{role}`")),
        }
    }
    reader_outcomes.sort_by_key(|outcome| outcome.report.tid);
    writer_outcomes.sort_by_key(|outcome| outcome.report.tid);
    if reader_outcomes.len() != num_readers || writer_outcomes.len() != num_writers {
        return Err(format!(
            "worker outcome mismatch: expected {num_readers} readers/{num_writers} writers, observed {}/{}",
            reader_outcomes.len(),
            writer_outcomes.len()
        ));
    }

    let verified_overlap_reads = if num_writers == 0 {
        reader_outcomes
            .iter()
            .map(|outcome| outcome.report.completed_ops)
            .sum()
    } else {
        let writer_intervals = writer_outcomes
            .iter()
            .flat_map(|outcome| outcome.operation_intervals.iter().copied())
            .collect::<Vec<_>>();
        if writer_intervals.is_empty() {
            return Err("timed writer-call interval receipt set is empty".to_owned());
        }
        for outcome in &writer_outcomes {
            if outcome.operation_intervals.len() != outcome.latencies_ns.len()
                || outcome.operation_intervals.len() as u64 != outcome.report.completed_ops
            {
                return Err(format!(
                    "writer {} interval receipt mismatch: operations {}, latencies {}, intervals {}",
                    outcome.report.tid,
                    outcome.report.completed_ops,
                    outcome.latencies_ns.len(),
                    outcome.operation_intervals.len()
                ));
            }
        }
        let mut verified = 0u64;
        for outcome in &reader_outcomes {
            if outcome.operation_intervals.len() != outcome.latencies_ns.len()
                || outcome.operation_intervals.len() as u64 != outcome.report.completed_ops
            {
                return Err(format!(
                    "reader {} interval receipt mismatch: operations {}, latencies {}, intervals {}",
                    outcome.report.tid,
                    outcome.report.completed_ops,
                    outcome.latencies_ns.len(),
                    outcome.operation_intervals.len()
                ));
            }
            for (sample_index, interval) in outcome.operation_intervals.iter().enumerate() {
                if !writer_intervals
                    .iter()
                    .any(|writer_interval| interval.overlaps(*writer_interval))
                {
                    return Err(format!(
                        "reader {} sample {sample_index} has no actual interval overlap with any timed writer call",
                        outcome.report.tid
                    ));
                }
                verified = verified
                    .checked_add(1)
                    .ok_or_else(|| "verified read-overlap receipt overflow".to_owned())?;
            }
        }
        verified
    };

    let all_read_latencies: Vec<u64> = reader_outcomes
        .iter()
        .flat_map(|outcome| outcome.latencies_ns.iter().copied())
        .collect();
    let all_write_latencies: Vec<u64> = writer_outcomes
        .iter()
        .flat_map(|outcome| outcome.latencies_ns.iter().copied())
        .collect();
    let consumed_read_payload_bytes = reader_outcomes.iter().try_fold(0u64, |total, outcome| {
        total
            .checked_add(outcome.consumed_read_payload_bytes)
            .ok_or_else(|| "aggregate read payload byte receipt overflow".to_owned())
    })?;
    let readers: Vec<ThreadReport> = reader_outcomes
        .into_iter()
        .map(|outcome| outcome.report)
        .collect();
    let writers: Vec<ThreadReport> = writer_outcomes
        .into_iter()
        .map(|outcome| outcome.report)
        .collect();
    let wall_ms = timing.wall.as_secs_f64() * 1_000.0;
    let reader_completion_ms = timing.reader_completion.as_secs_f64() * 1_000.0;
    let writer_completion_ms = timing.writer_completion.as_secs_f64() * 1_000.0;
    let both_roles_incomplete_elapsed = timing.reader_completion.min(timing.writer_completion);
    let both_roles_incomplete_fraction =
        both_roles_incomplete_elapsed.as_secs_f64() / timing.wall.as_secs_f64();

    let total_read_ops: u64 = readers.iter().map(|r| r.completed_ops).sum();
    let total_write_ops: u64 = writers.iter().map(|w| w.completed_ops).sum();
    let total_failed: u64 = writers.iter().map(|w| w.failed_ops).sum();

    let reader_rates: Vec<f64> = readers.iter().map(|r| r.ops_per_sec).collect();
    let writer_rates: Vec<f64> = writers.iter().map(|w| w.ops_per_sec).collect();

    let minimum_expected_reads =
        u64::try_from(num_readers).map_err(|_| "reader count exceeds u64".to_owned())?;
    let expected_writes = expected_operation_count(num_writers, ops_per_thread)?;
    let payload_size =
        u64::try_from(PAYLOAD_SIZE).map_err(|_| "payload size exceeds u64".to_owned())?;
    let expected_consumed_read_payload_bytes = total_read_ops
        .checked_mul(payload_size)
        .ok_or_else(|| "expected read payload byte receipt overflow".to_owned())?;
    let expected_database_rows_after = i64::try_from(expected_writes)
        .ok()
        .and_then(|writes| seed_rows.checked_add(writes))
        .ok_or_else(|| "expected postflight row count exceeds SQLite INTEGER".to_owned())?;
    let expected_ranges = expected_id_ranges(seed_rows, ops_per_thread, num_writers)?;
    if id_range_receipts_after.len() != expected_ranges.len() {
        return Err(format!(
            "postflight ID-range receipt count mismatch: expected {}, observed {}",
            expected_ranges.len(),
            id_range_receipts_after.len()
        ));
    }
    for (receipt, (label, lower, upper, expected_rows)) in
        id_range_receipts_after.iter().zip(&expected_ranges)
    {
        if receipt.label.as_str() != label.as_str()
            || receipt.lower_inclusive != *lower
            || receipt.upper_inclusive != *upper
            || receipt.expected_rows != *expected_rows
        {
            return Err(format!(
                "postflight ID-range metadata mismatch: expected ({label}, {lower}, {upper}, {expected_rows}), observed {receipt:?}"
            ));
        }
    }
    let work_receipt = WorkReceipt {
        minimum_expected_reads,
        completed_reads: total_read_ops,
        reads_with_verified_writer_call_interval_overlap: verified_overlap_reads,
        expected_writes,
        completed_writes: total_write_ops,
        expected_consumed_read_payload_bytes,
        consumed_read_payload_bytes,
        expected_database_rows_before: seed_rows,
        observed_database_rows_before: seed_database_state.rows,
        expected_database_id_sum_before: arithmetic_sum(
            1,
            usize::try_from(seed_rows)
                .map_err(|error| format!("seed row count exceeds usize: {error}"))?,
        )?,
        observed_database_id_sum_before: seed_database_state.id_sum,
        expected_database_payload_bytes_before: expected_payload_bytes(seed_rows)?,
        observed_database_payload_bytes_before: seed_database_state.payload_bytes,
        expected_matching_payload_rows_before: seed_rows,
        observed_matching_payload_rows_before: seed_database_state.matching_payload_rows,
        expected_database_rows_after,
        observed_database_rows_after: database_state.rows,
        expected_database_id_sum_after: expected_database_id_sum(
            seed_rows,
            ops_per_thread,
            num_writers,
        )?,
        observed_database_id_sum_after: database_state.id_sum,
        expected_database_payload_bytes_after: expected_payload_bytes(
            expected_database_rows_after,
        )?,
        observed_database_payload_bytes_after: database_state.payload_bytes,
        expected_matching_payload_rows_after: expected_database_rows_after,
        observed_matching_payload_rows_after: database_state.matching_payload_rows,
        id_range_receipts_after,
    };
    validate_work_receipt(&work_receipt)?;
    for report in readers.iter().chain(&writers) {
        let operation_count_valid = if report.role == "reader" {
            report.completed_ops >= report.expected_ops
        } else {
            report.completed_ops == report.expected_ops
        };
        if !operation_count_valid
            || report.failed_ops != 0
            || report.latency.count != report.completed_ops
        {
            return Err(format!(
                "{} worker {} receipt mismatch: expected {}, completed {}, failed {}, latency samples {}",
                report.role,
                report.tid,
                report.expected_ops,
                report.completed_ops,
                report.failed_ops,
                report.latency.count
            ));
        }
    }

    #[allow(clippy::cast_precision_loss)]
    let read_ops_per_sec = total_read_ops as f64 / timing.reader_completion.as_secs_f64();
    #[allow(clippy::cast_precision_loss)]
    let write_ops_per_sec = total_write_ops as f64 / timing.writer_completion.as_secs_f64();

    Ok(IterResult {
        readers,
        writers,
        wall_elapsed_ms: wall_ms,
        reader_completion_elapsed_ms: reader_completion_ms,
        writer_completion_elapsed_ms: writer_completion_ms,
        both_roles_incomplete_elapsed_ms_from_common_start: both_roles_incomplete_elapsed
            .as_secs_f64()
            * 1_000.0,
        both_roles_incomplete_fraction_of_wall: both_roles_incomplete_fraction,
        total_read_ops,
        total_write_ops,
        total_failed_writes: total_failed,
        aggregate_read_latency: compute_latency_stats(all_read_latencies),
        aggregate_write_latency: compute_latency_stats(all_write_latencies),
        read_ops_per_sec,
        write_ops_per_sec,
        reader_fairness_jain: jain_fairness(&reader_rates),
        writer_fairness_jain: jain_fairness(&writer_rates),
        work_receipt,
    })
}

fn build_engine_report(engine: &str, iters: Vec<IterResult>) -> EngineReport {
    let read_rates: Vec<f64> = iters.iter().map(|i| i.read_ops_per_sec).collect();
    let write_rates: Vec<f64> = iters.iter().map(|i| i.write_ops_per_sec).collect();
    let read_p50s: Vec<f64> = iters
        .iter()
        .map(|i| i.aggregate_read_latency.p50_us)
        .collect();
    let read_p95s: Vec<f64> = iters
        .iter()
        .map(|i| i.aggregate_read_latency.p95_us)
        .collect();
    let read_p99s: Vec<f64> = iters
        .iter()
        .map(|i| i.aggregate_read_latency.p99_us)
        .collect();

    EngineReport {
        engine: engine.to_owned(),
        iters,
        median_read_ops_per_sec: median_of(read_rates),
        median_write_ops_per_sec: median_of(write_rates),
        median_read_p50_us: median_of(read_p50s),
        median_read_p95_us: median_of(read_p95s),
        median_read_p99_us: median_of(read_p99s),
    }
}

// ─── Printing ───────────────────────────────────────────────────────────

fn print_summary(report: &BenchReport) {
    let fs = &report.fsqlite;
    let cs = &report.sqlite_reference;

    let mut out = String::with_capacity(1024);
    let _ = writeln!(out, "\n[{BEAD_ID}] Mixed OLTP Benchmark Results");
    let _ = writeln!(
        out,
        "[{BEAD_ID}] !!! NON-CITABLE DIAGNOSTIC — NUMBERS ARE NOT RELEASE EVIDENCE !!!"
    );
    let _ = writeln!(out, "[{BEAD_ID}] Status: {}", report.status);
    for limitation in &report.validation_limitations {
        let _ = writeln!(out, "[{BEAD_ID}] Limitation: {limitation}");
    }
    let _ = writeln!(
        out,
        "[{BEAD_ID}] Config: {seed_rows} seed rows, {ops} writer ops/thread, {r}R/{w}W threads, {iters} iters",
        seed_rows = report.seed_rows,
        ops = report.ops_per_thread,
        r = report.num_readers,
        w = report.num_writers,
        iters = report.iterations,
    );
    let _ = writeln!(out);
    let _ = writeln!(
        out,
        "  {:>12} | {:>14} | {:>14} | {:>12} | {:>12} | {:>12}",
        "Engine", "Overlap reads/s", "Write ops/s", "Read p50 µs", "Read p95 µs", "Read p99 µs"
    );
    let _ = writeln!(
        out,
        "  {:-<12}-+-{:-<14}-+-{:-<14}-+-{:-<12}-+-{:-<12}-+-{:-<12}",
        "", "", "", "", "", ""
    );
    let _ = writeln!(
        out,
        "  {:>12} | {:>14.0} | {:>14.0} | {:>12.1} | {:>12.1} | {:>12.1}",
        "fsqlite",
        fs.median_read_ops_per_sec,
        fs.median_write_ops_per_sec,
        fs.median_read_p50_us,
        fs.median_read_p95_us,
        fs.median_read_p99_us,
    );
    let _ = writeln!(
        out,
        "  {:>12} | {:>14.0} | {:>14.0} | {:>12.1} | {:>12.1} | {:>12.1}",
        "C SQLite",
        cs.median_read_ops_per_sec,
        cs.median_write_ops_per_sec,
        cs.median_read_p50_us,
        cs.median_read_p95_us,
        cs.median_read_p99_us,
    );
    let _ = writeln!(out);
    let _ = writeln!(
        out,
        "  Overlap-qualified read completion-rate ratio: {:.2}x (fsqlite / C SQLite)",
        report.read_throughput_ratio
    );
    let _ = writeln!(
        out,
        "  Write throughput ratio: {:.2}x (fsqlite / C SQLite)",
        report.write_throughput_ratio
    );
    let _ = writeln!(
        out,
        "  Read p50 latency ratio: {:.2}x",
        report.read_latency_p50_ratio
    );
    let _ = writeln!(
        out,
        "  Read p95 latency ratio: {:.2}x",
        report.read_latency_p95_ratio
    );

    if let Some(last_iter) = fs.iters.last() {
        let _ = writeln!(
            out,
            "  Reader fairness (Jain): {:.4}",
            last_iter.reader_fairness_jain
        );
        let _ = writeln!(
            out,
            "  Writer fairness (Jain): {:.4}",
            last_iter.writer_fairness_jain
        );
    }

    eprint!("{out}");
}

// ─── Main ───────────────────────────────────────────────────────────────

fn ratio(numerator: f64, denominator: f64, metric: &str) -> Result<f64, String> {
    if !numerator.is_finite() || numerator <= 0.0 {
        return Err(format!(
            "{metric} ratio numerator must be finite and positive, got {numerator}"
        ));
    }
    if !denominator.is_finite() || denominator <= 0.0 {
        return Err(format!(
            "{metric} ratio denominator must be finite and positive, got {denominator}"
        ));
    }
    let value = numerator / denominator;
    if !value.is_finite() || value <= 0.0 {
        return Err(format!(
            "{metric} ratio must be finite and positive, got {value}"
        ));
    }
    Ok(value)
}

const fn fsqlite_runs_first(sample_index: usize) -> bool {
    let within_block = sample_index % 4;
    let abba_block = (sample_index / 4).is_multiple_of(2);
    if abba_block {
        matches!(within_block, 0 | 3)
    } else {
        matches!(within_block, 1 | 2)
    }
}

fn paired_iteration_ratio(
    sample_index: usize,
    execution_order: &str,
    fsqlite: &IterResult,
    sqlite_reference: &IterResult,
) -> Result<PairedIterationRatio, String> {
    let zero_based = sample_index.saturating_sub(1);
    Ok(PairedIterationRatio {
        sample_index,
        order_block_index: zero_based / 4,
        order_block_pattern: if (zero_based / 4).is_multiple_of(2) {
            "ABBA".to_owned()
        } else {
            "BAAB".to_owned()
        },
        position_in_block: zero_based % 4 + 1,
        execution_order: execution_order.to_owned(),
        read_throughput_ratio: ratio(
            fsqlite.read_ops_per_sec,
            sqlite_reference.read_ops_per_sec,
            "read throughput",
        )?,
        write_throughput_ratio: ratio(
            fsqlite.write_ops_per_sec,
            sqlite_reference.write_ops_per_sec,
            "write throughput",
        )?,
        read_latency_p50_ratio: ratio(
            fsqlite.aggregate_read_latency.p50_us,
            sqlite_reference.aggregate_read_latency.p50_us,
            "read latency p50",
        )?,
        read_latency_p95_ratio: ratio(
            fsqlite.aggregate_read_latency.p95_us,
            sqlite_reference.aggregate_read_latency.p95_us,
            "read latency p95",
        )?,
    })
}

fn run() -> Result<(), String> {
    let opts = parse_opts();
    validate_options(&opts)?;

    eprintln!(
        "[{BEAD_ID}] mt-oltp-bench: {r}R/{w}W, {seed} seed rows, {ops} writer ops/thread, {iters} iters",
        r = opts.readers,
        w = opts.writers,
        seed = opts.seed_rows,
        ops = opts.ops_per_thread,
        iters = opts.iters,
    );

    let mut fs_iters = Vec::with_capacity(opts.iters);
    let mut cs_iters = Vec::with_capacity(opts.iters);
    let mut paired_iterations = Vec::with_capacity(opts.iters);
    for i in 0..opts.iters {
        let fsqlite_first = fsqlite_runs_first(i);
        let execution_order = if fsqlite_first {
            "fsqlite_then_sqlite_reference"
        } else {
            "sqlite_reference_then_fsqlite"
        };
        eprintln!(
            "[{BEAD_ID}] Pair {}/{} ({execution_order})",
            i + 1,
            opts.iters
        );
        let run_fsqlite = || {
            eprint!("  FrankenSQLite... ");
            let result = run_fsqlite_iter(
                opts.seed_rows,
                opts.ops_per_thread,
                opts.readers,
                opts.writers,
            )?;
            eprintln!(
                "overlap-qualified reads={:.0}/s, writes={:.0}/s",
                result.read_ops_per_sec, result.write_ops_per_sec
            );
            Ok::<_, String>(result)
        };
        let run_csqlite = || {
            eprint!("  C SQLite... ");
            let result = run_rusqlite_iter(
                opts.seed_rows,
                opts.ops_per_thread,
                opts.readers,
                opts.writers,
            )?;
            eprintln!(
                "overlap-qualified reads={:.0}/s, writes={:.0}/s",
                result.read_ops_per_sec, result.write_ops_per_sec
            );
            Ok::<_, String>(result)
        };
        let (fs_result, cs_result) = if fsqlite_first {
            let fs_result = run_fsqlite()?;
            let cs_result = run_csqlite()?;
            (fs_result, cs_result)
        } else {
            let cs_result = run_csqlite()?;
            let fs_result = run_fsqlite()?;
            (fs_result, cs_result)
        };
        paired_iterations.push(paired_iteration_ratio(
            i + 1,
            execution_order,
            &fs_result,
            &cs_result,
        )?);
        fs_iters.push(fs_result);
        cs_iters.push(cs_result);
    }

    let fs_report = build_engine_report("fsqlite", fs_iters);
    let cs_report = build_engine_report("sqlite_reference", cs_iters);

    let read_ratio = median_of(
        paired_iterations
            .iter()
            .map(|pair| pair.read_throughput_ratio)
            .collect(),
    );
    let write_ratio = median_of(
        paired_iterations
            .iter()
            .map(|pair| pair.write_throughput_ratio)
            .collect(),
    );
    let p50_ratio = median_of(
        paired_iterations
            .iter()
            .map(|pair| pair.read_latency_p50_ratio)
            .collect(),
    );
    let p95_ratio = median_of(
        paired_iterations
            .iter()
            .map(|pair| pair.read_latency_p95_ratio)
            .collect(),
    );

    let report = BenchReport {
        schema_version: REPORT_SCHEMA.to_owned(),
        citable: false,
        status: "diagnostic_until_external_gate0_provenance_is_attached".to_owned(),
        validation_limitations: vec![
            "source commit, binary hash, exact build profile, and dependency graph are not embedded"
                .to_owned(),
            "the bundled C SQLite source/compiler command and native flags are not embedded"
                .to_owned(),
            "CPU topology, affinity, frequency policy, host interference, and temporary-filesystem identity are not embedded"
                .to_owned(),
            "worker timeout is cooperative between operations; a permanently hung database call can delay thread join"
                .to_owned(),
            "the report has no independently published JSON Schema validator".to_owned(),
        ],
        bead_id: BEAD_ID.to_owned(),
        timestamp_unix_ms: now_unix_ms(),
        seed_rows: opts.seed_rows,
        ops_per_thread: opts.ops_per_thread,
        num_readers: opts.readers,
        num_writers: opts.writers,
        iterations: opts.iters,
        ordering_policy: ORDERING_POLICY.to_owned(),
        workload_policy:
            "each writer executes exactly ops_per_thread committed inserts; after the common start every reader first registers that it is waiting for overlap, writers begin only after all readers register, and a reader starts a validated primary-key lookup only after observing at least one timed writer call active; readers wait across gaps between writer calls and stop when the final writer finishes; post-run interval receipts reject the entire iteration unless every reported query interval actually intersects a timed writer-call interval, so unloaded samples are never credited and a worker with no overlap-qualified lookup fails closed; readers have a cap of 1,000,000 credited lookups per thread, and an overlapping query may finish after the writer interval it intersects"
                .to_owned(),
        ratio_aggregation: "median_of_within-pair_fsqlite_over_sqlite_reference_ratios".to_owned(),
        timing_scope: "all worker setup, PRAGMAs, statement preparation, seed, an explicit verified TRUNCATE checkpoint, latency aggregation, completion-channel delay, joins, and postflight are excluded; the aggregate wall receipt runs from the common gate release to the latest worker completion and includes the initial reader-readiness rendezvous plus reader waits between active writer calls; the both_roles_incomplete fields are explicitly the common-start completion-window intersection, not operation overlap, while actual read/write-call overlap is proven from per-operation query-call/write-call intervals; read completion rate divides overlap-qualified validated reads by the common release-to-last-reader interval, and write completion rate divides exact writes by the common release-to-last-writer interval; per-operation read latency starts only after active-writer admission and includes query execution plus row type/value validation and payload consumption, while per-operation write latency includes explicit begin, prepared insert, commit, and contention retries"
            .to_owned(),
        fsqlite: fs_report,
        sqlite_reference: cs_report,
        paired_iterations,
        read_throughput_ratio: read_ratio,
        write_throughput_ratio: write_ratio,
        read_latency_p50_ratio: p50_ratio,
        read_latency_p95_ratio: p95_ratio,
    };

    print_summary(&report);

    if let Some(ref path) = opts.json_output {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|error| {
                format!(
                    "failed to create JSON output directory {}: {error}",
                    parent.display()
                )
            })?;
        }
        let json = serde_json::to_string_pretty(&report)
            .map_err(|error| format!("failed to serialize benchmark report: {error}"))?;
        std::fs::write(path, json)
            .map_err(|error| format!("failed to write JSON report {}: {error}", path.display()))?;
        eprintln!("[{BEAD_ID}] JSON written to {}", path.display());
    }
    Ok(())
}

fn main() {
    if let Err(error) = run() {
        eprintln!("[{BEAD_ID}] benchmark invalid: {error}");
        std::process::exit(1);
    }
}

// ─── Tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jain_fairness_equal_values() {
        assert!((jain_fairness(&[100.0, 100.0, 100.0, 100.0]) - 1.0).abs() < 1e-10);
    }

    #[test]
    fn jain_fairness_unequal_values() {
        let j = jain_fairness(&[100.0, 0.0]);
        assert!(j < 1.0);
        assert!(j > 0.0);
    }

    #[test]
    fn jain_fairness_single() {
        assert!((jain_fairness(&[42.0]) - 1.0).abs() < 1e-10);
    }

    #[test]
    fn jain_fairness_empty() {
        assert!((jain_fairness(&[]) - 1.0).abs() < 1e-10);
    }

    #[test]
    fn percentile_basics() {
        let data = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        assert!((percentile(&data, 0.0) - 1.0).abs() < 1e-10);
        assert!((percentile(&data, 0.5) - 3.0).abs() < 1e-10);
        assert!((percentile(&data, 1.0) - 5.0).abs() < 1e-10);
    }

    #[test]
    fn compute_latency_stats_empty() {
        let stats = compute_latency_stats(vec![]);
        assert_eq!(stats.count, 0);
        assert_eq!(stats.p50_us, 0.0);
    }

    #[test]
    fn compute_latency_stats_single() {
        let stats = compute_latency_stats(vec![5_000]);
        assert_eq!(stats.count, 1);
        assert!((stats.p50_us - 5.0).abs() < 0.01);
    }

    fn test_settings() -> WorkerSettings {
        WorkerSettings {
            page_size_bytes: 4_096,
            journal_mode: "wal".to_owned(),
            synchronous: "normal".to_owned(),
            cache_size: -64_000,
            busy_timeout_ms: 5_000,
            wal_autocheckpoint_pages: 1_000,
            concurrent_mode: "test".to_owned(),
        }
    }

    fn reader_outcome(tid: usize, latencies_ns: Vec<u64>) -> WorkerOutcome {
        let completed_ops = latencies_ns.len() as u64;
        WorkerOutcome {
            report: ThreadReport {
                role: "reader".to_owned(),
                tid,
                expected_ops: completed_ops,
                completed_ops,
                failed_ops: 0,
                elapsed_ms: 1.0,
                ops_per_sec: completed_ops as f64 * 1_000.0,
                latency: compute_latency_stats(latencies_ns.clone()),
                settings: test_settings(),
            },
            latencies_ns,
            operation_intervals: Vec::new(),
            consumed_read_payload_bytes: completed_ops * PAYLOAD_SIZE as u64,
            finished_at: Instant::now(),
        }
    }

    #[test]
    fn aggregate_latency_uses_real_operation_samples() {
        let result = build_iter_result(IterBuildInputs {
            outcomes: vec![
                reader_outcome(0, vec![1_000, 9_000]),
                reader_outcome(1, vec![4_000, 6_000]),
            ],
            timing: CoordinatedTiming {
                wall: Duration::from_millis(1),
                reader_completion: Duration::from_millis(1),
                writer_completion: Duration::from_millis(1),
            },
            seed_rows: 1,
            ops_per_thread: 2,
            num_readers: 2,
            num_writers: 0,
            seed_database_state: DatabaseState {
                rows: 1,
                id_sum: 1,
                payload_bytes: 64,
                matching_payload_rows: 1,
            },
            database_state: DatabaseState {
                rows: 1,
                id_sum: 1,
                payload_bytes: 64,
                matching_payload_rows: 1,
            },
            id_range_receipts_after: vec![IdRangeReceipt {
                label: "seed".to_owned(),
                lower_inclusive: 1,
                upper_inclusive: 1,
                expected_rows: 1,
                observed_rows: 1,
            }],
        })
        .expect("valid receipt");
        assert_eq!(result.aggregate_read_latency.count, 4);
        assert!(result.aggregate_read_latency.p99_us > 8.0);
        assert!((result.aggregate_read_latency.p50_us - 5.0).abs() < 0.01);
    }

    #[test]
    fn work_receipt_rejects_silent_read_drop_and_payload_corruption() {
        let mut receipt = WorkReceipt {
            minimum_expected_reads: 2,
            completed_reads: 1,
            reads_with_verified_writer_call_interval_overlap: 1,
            expected_writes: 0,
            completed_writes: 0,
            expected_consumed_read_payload_bytes: 128,
            consumed_read_payload_bytes: 64,
            expected_database_rows_before: 1,
            observed_database_rows_before: 1,
            expected_database_id_sum_before: 1,
            observed_database_id_sum_before: 1,
            expected_database_payload_bytes_before: 64,
            observed_database_payload_bytes_before: 64,
            expected_matching_payload_rows_before: 1,
            observed_matching_payload_rows_before: 1,
            expected_database_rows_after: 1,
            observed_database_rows_after: 1,
            expected_database_id_sum_after: 1,
            observed_database_id_sum_after: 1,
            expected_database_payload_bytes_after: 64,
            observed_database_payload_bytes_after: 64,
            expected_matching_payload_rows_after: 1,
            observed_matching_payload_rows_after: 1,
            id_range_receipts_after: vec![IdRangeReceipt {
                label: "seed".to_owned(),
                lower_inclusive: 1,
                upper_inclusive: 1,
                expected_rows: 1,
                observed_rows: 1,
            }],
        };
        let error = validate_work_receipt(&receipt).expect_err("silent drop must fail");
        assert!(error.contains("read work mismatch"));
        receipt.completed_reads = receipt.minimum_expected_reads;
        receipt.reads_with_verified_writer_call_interval_overlap = receipt.completed_reads;
        receipt.consumed_read_payload_bytes = receipt.expected_consumed_read_payload_bytes;
        receipt.reads_with_verified_writer_call_interval_overlap -= 1;
        let error = validate_work_receipt(&receipt).expect_err("unloaded read must fail");
        assert!(error.contains("read overlap mismatch"));
        receipt.reads_with_verified_writer_call_interval_overlap = receipt.completed_reads;
        receipt.observed_database_payload_bytes_after -= 1;
        let error = validate_work_receipt(&receipt).expect_err("payload corruption must fail");
        assert!(error.contains("postflight payload-byte mismatch"));
        receipt.observed_database_payload_bytes_after =
            receipt.expected_database_payload_bytes_after;
        receipt.observed_matching_payload_rows_after -= 1;
        let error =
            validate_work_receipt(&receipt).expect_err("same-length payload corruption must fail");
        assert!(error.contains("postflight payload-content mismatch"));
        receipt.observed_matching_payload_rows_after = receipt.expected_matching_payload_rows_after;
        receipt.id_range_receipts_after[0].observed_rows = 0;
        let error = validate_work_receipt(&receipt).expect_err("missing expected ID must fail");
        assert!(error.contains("postflight ID-range `seed` mismatch"));
    }

    #[test]
    fn options_reject_zero_work_and_overlapping_writer_ranges() {
        let mut opts = Options {
            seed_rows: 1,
            ops_per_thread: 0,
            readers: 1,
            writers: 0,
            iters: 1,
            json_output: None,
        };
        assert!(validate_options(&opts).is_err());
        opts.ops_per_thread = 1;
        opts.readers = 1;
        opts.writers = 1;
        opts.iters = 8;
        assert!(validate_options(&opts).is_err());
        opts.ops_per_thread = usize::try_from(ROWID_BASE_STRIDE).unwrap() + 1;
        opts.writers = 2;
        assert!(validate_options(&opts).is_err());

        opts.ops_per_thread = 2;
        opts.readers = 0;
        assert!(validate_options(&opts).is_err());
    }

    #[test]
    fn paired_order_is_complete_alternating_abba_baab_cycle() {
        let first_engines: Vec<&str> = (0..8)
            .map(|sample| {
                if fsqlite_runs_first(sample) {
                    "fsqlite"
                } else {
                    "sqlite_reference"
                }
            })
            .collect();
        assert_eq!(
            first_engines,
            [
                "fsqlite",
                "sqlite_reference",
                "sqlite_reference",
                "fsqlite",
                "sqlite_reference",
                "fsqlite",
                "fsqlite",
                "sqlite_reference",
            ]
        );
    }

    #[test]
    fn artifact_ratios_keep_full_precision() {
        let value = ratio(1.0, 3.0, "fixture").expect("valid ratio");
        assert_eq!(value, 1.0 / 3.0);
        assert_ne!(value, 0.33);
        assert!(ratio(1.0, 0.0, "fixture").is_err());
        assert!(ratio(f64::NAN, 1.0, "fixture").is_err());
    }

    #[test]
    fn tiny_real_runs_produce_exact_nonzero_receipts() {
        let fsqlite =
            run_fsqlite_iter(4, 16, 1, 1).expect("tiny FrankenSQLite benchmark must be valid");
        let csqlite =
            run_rusqlite_iter(4, 16, 1, 1).expect("tiny C SQLite benchmark must be valid");
        for result in [&fsqlite, &csqlite] {
            assert_eq!(result.work_receipt.minimum_expected_reads, 1);
            assert!(result.work_receipt.completed_reads >= 1);
            assert_eq!(
                result
                    .work_receipt
                    .reads_with_verified_writer_call_interval_overlap,
                result.work_receipt.completed_reads
            );
            assert_eq!(result.work_receipt.expected_writes, 16);
            assert_eq!(result.work_receipt.completed_writes, 16);
            assert_eq!(
                result.work_receipt.consumed_read_payload_bytes,
                result
                    .work_receipt
                    .completed_reads
                    .checked_mul(PAYLOAD_SIZE as u64)
                    .unwrap()
            );
            assert_eq!(
                result.work_receipt.consumed_read_payload_bytes,
                result.work_receipt.expected_consumed_read_payload_bytes
            );
            assert_eq!(result.work_receipt.observed_database_rows_after, 20);
            assert_eq!(
                result.work_receipt.observed_database_id_sum_after,
                result.work_receipt.expected_database_id_sum_after
            );
            assert_eq!(
                result.work_receipt.observed_database_payload_bytes_after,
                result.work_receipt.expected_database_payload_bytes_after
            );
            assert_eq!(result.work_receipt.id_range_receipts_after.len(), 2);
            assert!(
                result
                    .work_receipt
                    .id_range_receipts_after
                    .iter()
                    .all(|range| range.observed_rows == range.expected_rows)
            );
        }
    }

    #[test]
    fn median_of_values() {
        assert!((median_of(vec![3.0, 1.0, 2.0]) - 2.0).abs() < 1e-10);
        assert!((median_of(vec![1.0]) - 1.0).abs() < 1e-10);
        assert_eq!(median_of(vec![]), 0.0);
    }
}
