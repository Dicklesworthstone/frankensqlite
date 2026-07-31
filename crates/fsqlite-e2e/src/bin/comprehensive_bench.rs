#![recursion_limit = "256"]
// bd-h9o9r / bd-zavyn: the hoisted timed bodies await fsqlite-core's
// deliberately non-`Send`, deeply nested engine futures inside one runtime
// entry per sample; `future_not_send` and `large_futures` contradict that
// design (see fsqlite-core/src/lib.rs for the full rationale, including why
// boxing was rejected by the perf ledger).
#![allow(clippy::future_not_send)]
#![allow(clippy::large_futures)]

//! Comprehensive FrankenSQLite vs C SQLite benchmark.
//!
//! Measures insertion throughput across multiple dimensions:
//!
//! **Row counts:** 100, 1K, 10K, 100K
//! **Record sizes:** tiny (1 col), small (3 cols), medium (6 cols), large (10 cols with ~500B text)
//! **Transaction strategies:** autocommit, batched (1K per txn), single txn
//! **Concurrency:** single writer, 2/4/8 concurrent writers (C SQLite WAL vs FrankenSQLite MVCC)
//! **Read-after-write:** full scan, point lookup, range scan, COUNT(*), indexed lookup
//!
//! Usage:
//!   cargo run --profile release-perf -p fsqlite-e2e --bin comprehensive-bench
//!   cargo run --profile release-perf -p fsqlite-e2e --bin comprehensive-bench -- --quick
//!   cargo run --profile release-perf -p fsqlite-e2e --bin comprehensive-bench -- --filter insert

use std::collections::BTreeMap;
use std::io::{Read as _, Write as _};
use std::sync::{Arc, Condvar, Mutex, OnceLock, mpsc};
use std::time::{Duration, Instant, SystemTime};

use asupersync::runtime::{BlockingTaskHandle, Runtime, RuntimeBuilder};
use fsqlite_core::connection::{
    HotPathProfileSnapshot, hot_path_profile_enabled, hot_path_profile_snapshot,
    reset_hot_path_profile, set_hot_path_profile_enabled,
};
#[cfg(feature = "bridge-experiment")]
use rand::RngExt as _;
#[cfg(feature = "bridge-experiment")]
use rand::SeedableRng as _;
#[cfg(feature = "bridge-experiment")]
use rand::rngs::StdRng;
#[cfg(feature = "bridge-experiment")]
use rand::seq::SliceRandom as _;
use serde::Serialize;
use sha2::{Digest, Sha256};
use tracing_subscriber::prelude::*;

// ─── Configuration ─────────────────────────────────────────────────────

const WARMUP_ITERS: usize = 2;
const MIN_ITERS: usize = 3;
const MAX_ITERS: usize = 10;
const TARGET_DURATION: Duration = Duration::from_secs(5);

const ROW_COUNTS: &[usize] = &[100, 1_000, 10_000, 100_000];
const ROW_COUNTS_QUICK: &[usize] = &[100, 1_000, 10_000];

const CONCURRENT_THREAD_COUNTS: &[usize] = &[2, 4, 8];
const CONCURRENT_ROWS_PER_THREAD: usize = 1_000;
const CONCURRENT_RANGE_SIZE: i64 = 1_000_000;
const JSON_REPORT_SCHEMA_V6: &str = "fsqlite-e2e.comprehensive-bench-report.v6";
const BENCHMARK_PROVENANCE_SCHEMA_V3: &str = "fsqlite-e2e.benchmark-provenance.v3";
const CI_REGRESSION_GATE_SCHEMA_V2: &str = "fsqlite-e2e.comprehensive-bench-ci-regression-gate.v2";
const CI_REGRESSION_GATE_BEAD_ID: &str = "bd-m4tju";
const CI_REGRESSION_BASELINE_BEAD_ID: &str = "bd-0winn";
const CI_REGRESSION_BASELINE_AVG_RATIO: f64 = 2.74;
const CI_REGRESSION_GATE_THRESHOLD_SOURCE: &str =
    "bd-d4m5k rich scorecard: primary gate is per_category_weighted.score";
const CI_PRIMARY_SCORE_MAX_REGRESSION_PCT: f64 = 0.03;
const CI_GEOMEAN_MAX_REGRESSION_PCT: f64 = 0.05;
const CI_CATEGORY_GEOMEAN_MAX_REGRESSION_PCT: f64 = 0.10;
const CI_P90_MAX_REGRESSION_PCT: f64 = 0.15;
const CONCURRENT_WRITERS_SECTION_TITLE: &str =
    "Concurrent Writers — C SQLite WAL vs FrankenSQLite MVCC";
const DEFAULT_BENCH_PAGE_SIZE_BYTES: u32 = 4096;
#[cfg(feature = "bridge-experiment")]
const BRIDGE_REPORT_SCHEMA_V2: &str = "fsqlite-e2e.bridge-experiment.v2";
#[cfg(feature = "bridge-experiment")]
const BRIDGE_INSERT_SQL: &str = "INSERT INTO bridge_probe(id, value) VALUES (?1, ?2)";
#[cfg(feature = "bridge-experiment")]
const BRIDGE_EXACT_ORACLE_SQL: &str = "SELECT COUNT(*), COALESCE(SUM(value), 0), \
     COALESCE(SUM(CASE WHEN id = value AND id >= 0 AND id < ?1 THEN 1 ELSE 0 END), 0) \
     FROM bridge_probe";
#[cfg(feature = "bridge-experiment")]
const BRIDGE_ABSOLUTE_MAX_LOAD_1M: f64 = 1.0;
#[cfg(feature = "bridge-experiment")]
const BRIDGE_MAX_CPU_PRESSURE_SOME_AVG10: f64 = 1.0;
#[cfg(feature = "bridge-experiment")]
const BRIDGE_MAX_IO_PRESSURE_SOME_AVG60: f64 = 0.10;

// ─── Record size definitions ───────────────────────────────────────────

#[derive(Debug, Clone, Copy)]
enum RecordSize {
    Tiny,
    Small,
    Medium,
    Large,
}

impl RecordSize {
    const ALL: &[Self] = &[Self::Tiny, Self::Small, Self::Medium, Self::Large];

    const fn name(self) -> &'static str {
        match self {
            Self::Tiny => "tiny_1col",
            Self::Small => "small_3col",
            Self::Medium => "medium_6col",
            Self::Large => "large_10col",
        }
    }

    const fn description(self) -> &'static str {
        match self {
            Self::Tiny => "1 col (INTEGER PK only)",
            Self::Small => "3 cols (~30B: id, name, value)",
            Self::Medium => "6 cols (~180B: id, name, email, bio, category, score)",
            Self::Large => "10 cols (~600B: includes long text fields)",
        }
    }

    fn create_table_sql(self) -> &'static str {
        match self {
            Self::Tiny => "CREATE TABLE bench (id INTEGER PRIMARY KEY)",
            Self::Small => {
                "CREATE TABLE bench (id INTEGER PRIMARY KEY, name TEXT NOT NULL, value REAL NOT NULL)"
            }
            Self::Medium => {
                "CREATE TABLE bench (\
                id INTEGER PRIMARY KEY, \
                first_name TEXT NOT NULL, \
                last_name TEXT NOT NULL, \
                email TEXT NOT NULL, \
                bio TEXT NOT NULL, \
                score INTEGER NOT NULL\
            )"
            }
            Self::Large => {
                "CREATE TABLE bench (\
                id INTEGER PRIMARY KEY, \
                first_name TEXT NOT NULL, \
                last_name TEXT NOT NULL, \
                email TEXT NOT NULL, \
                department TEXT NOT NULL, \
                title TEXT NOT NULL, \
                bio TEXT NOT NULL, \
                address TEXT NOT NULL, \
                notes TEXT NOT NULL, \
                score INTEGER NOT NULL\
            )"
            }
        }
    }

    fn insert_sql_csqlite(self) -> &'static str {
        match self {
            Self::Tiny => "INSERT INTO bench VALUES (?1)",
            Self::Small => "INSERT INTO bench VALUES (?1, ('user_' || ?1), (?1 * 0.137))",
            Self::Medium => {
                "INSERT INTO bench VALUES (\
                ?1, \
                ('Alice_' || ?1), \
                ('Smith_' || ?1), \
                ('user' || ?1 || '@example.com'), \
                ('Bio text for user number ' || ?1 || '. This is a medium-length description that adds some realistic payload to each row in the database.'), \
                (?1 * 7)\
            )"
            }
            Self::Large => {
                "INSERT INTO bench VALUES (\
                ?1, \
                ('FirstName_' || ?1), \
                ('LastName_' || ?1), \
                ('employee' || ?1 || '@bigcorp.example.com'), \
                ('Engineering_Dept_' || (?1 % 20)), \
                ('Senior Software Engineer Level ' || (?1 % 5)), \
                ('This is the biography for employee number ' || ?1 || '. They have been working at the company for many years and have contributed to numerous projects across multiple teams. Their expertise spans distributed systems, database internals, and performance optimization. They are known for their thorough code reviews and mentorship of junior engineers.'), \
                (?1 || ' Technology Park, Building ' || (?1 % 50) || ', Suite ' || (?1 % 200) || ', Innovation City, CA 94000'), \
                ('Internal notes: Employee ' || ?1 || ' - Performance rating: Exceeds Expectations. Last review date: 2026-01-15. Next review: 2026-07-15. Skills: Rust, C++, SQL, distributed systems, leadership.'), \
                (?1 * 13)\
                )"
            }
        }
    }
}

// ─── Measurement infrastructure ────────────────────────────────────────

#[allow(dead_code)]
#[derive(Clone)]
struct Measurement {
    label: String,
    durations: Vec<Duration>,
    row_count: usize,
}

#[allow(dead_code)]
impl Measurement {
    fn mean(&self) -> Duration {
        let total: Duration = self.durations.iter().sum();
        total / u32::try_from(self.durations.len()).unwrap_or(1)
    }

    fn median(&self) -> Duration {
        let mut sorted: Vec<Duration> = self.durations.clone();
        sorted.sort();
        sorted[sorted.len() / 2]
    }

    fn min(&self) -> Duration {
        self.durations.iter().copied().min().unwrap_or_default()
    }

    fn stddev(&self) -> Duration {
        let mean = self.mean().as_nanos() as f64;
        let variance = self
            .durations
            .iter()
            .map(|d| {
                let diff = d.as_nanos() as f64 - mean;
                diff * diff
            })
            .sum::<f64>()
            / self.durations.len() as f64;
        Duration::from_nanos(variance.sqrt() as u64)
    }

    fn rows_per_sec(&self) -> f64 {
        let secs = self.median().as_secs_f64();
        if secs == 0.0 {
            return 0.0;
        }
        self.row_count as f64 / secs
    }

    fn us_per_row(&self) -> f64 {
        let us = self.median().as_secs_f64() * 1_000_000.0;
        if self.row_count == 0 {
            return 0.0;
        }
        us / self.row_count as f64
    }

    fn percentile(&self, pct: f64) -> Duration {
        let mut sorted: Vec<Duration> = self.durations.clone();
        sorted.sort();
        let idx = ((pct / 100.0) * (sorted.len() - 1) as f64).ceil() as usize;
        sorted[idx.min(sorted.len() - 1)]
    }

    fn p95(&self) -> Duration {
        self.percentile(95.0)
    }

    fn p99(&self) -> Duration {
        self.percentile(99.0)
    }

    fn cv_percent(&self) -> f64 {
        let mean_ns = self.mean().as_nanos() as f64;
        if mean_ns == 0.0 {
            return 0.0;
        }
        let stddev_ns = self.stddev().as_nanos() as f64;
        (stddev_ns / mean_ns) * 100.0
    }

    fn iter_count(&self) -> usize {
        self.durations.len()
    }
}

fn measure<F: FnMut()>(label: &str, row_count: usize, mut f: F) -> Measurement {
    // Warmup
    for w in 0..WARMUP_ITERS {
        eprint!("\r    [{label}] warmup {}/{WARMUP_ITERS}...", w + 1);
        f();
    }

    let mut durations = Vec::new();
    let mut total_elapsed = Duration::ZERO;

    for iter in 0..MAX_ITERS {
        eprint!(
            "\r    [{label}] iter {}/{MAX_ITERS} (total: {:.1}s)    ",
            iter + 1,
            total_elapsed.as_secs_f64()
        );
        let start = Instant::now();
        f();
        let elapsed = start.elapsed();
        durations.push(elapsed);
        total_elapsed += elapsed;

        if durations.len() >= MIN_ITERS && total_elapsed >= TARGET_DURATION {
            break;
        }
    }
    eprint!("\r{:80}\r", ""); // Clear progress line.

    Measurement {
        label: label.to_string(),
        durations,
        row_count,
    }
}

fn measure_with_teardown<F, T>(
    label: &str,
    row_count: usize,
    mut f: F,
    mut teardown: T,
) -> Measurement
where
    F: FnMut(),
    T: FnMut(),
{
    // Warmup
    for w in 0..WARMUP_ITERS {
        eprint!("\r    [{label}] warmup {}/{WARMUP_ITERS}...", w + 1);
        f();
        teardown();
    }

    let mut durations = Vec::new();
    let mut total_elapsed = Duration::ZERO;

    for iter in 0..MAX_ITERS {
        eprint!(
            "\r    [{label}] iter {}/{MAX_ITERS} (total: {:.1}s)    ",
            iter + 1,
            total_elapsed.as_secs_f64()
        );
        let start = Instant::now();
        f();
        let elapsed = start.elapsed();
        teardown();
        durations.push(elapsed);
        total_elapsed += elapsed;

        if durations.len() >= MIN_ITERS && total_elapsed >= TARGET_DURATION {
            break;
        }
    }
    eprint!("\r{:80}\r", ""); // Clear progress line.

    Measurement {
        label: label.to_string(),
        durations,
        row_count,
    }
}

struct ConcurrentSample {
    elapsed: Duration,
    readiness: JsonConcurrentSampleReadiness,
}

fn measure_concurrent<F>(
    label: &str,
    row_count: usize,
    mut sample: F,
) -> (Measurement, Vec<JsonConcurrentSampleReadiness>)
where
    F: FnMut(&str, usize) -> Result<ConcurrentSample, String>,
{
    let mut readiness = Vec::new();
    for warmup_index in 0..WARMUP_ITERS {
        eprint!(
            "\r    [{label}] warmup {}/{WARMUP_ITERS}...",
            warmup_index + 1
        );
        let outcome = sample("warmup", warmup_index)
            .unwrap_or_else(|error| panic!("{label} warmup {warmup_index} failed: {error}"));
        readiness.push(outcome.readiness);
    }

    let mut durations = Vec::new();
    let mut total_elapsed = Duration::ZERO;
    for iteration in 0..MAX_ITERS {
        eprint!(
            "\r    [{label}] iter {}/{MAX_ITERS} (total: {:.1}s)    ",
            iteration + 1,
            total_elapsed.as_secs_f64()
        );
        let outcome = sample("measured", iteration)
            .unwrap_or_else(|error| panic!("{label} iteration {iteration} failed: {error}"));
        total_elapsed += outcome.elapsed;
        durations.push(outcome.elapsed);
        readiness.push(outcome.readiness);
        if durations.len() >= MIN_ITERS && total_elapsed >= TARGET_DURATION {
            break;
        }
    }
    eprint!("\r{:80}\r", "");
    (
        Measurement {
            label: label.to_owned(),
            durations,
            row_count,
        },
        readiness,
    )
}

// ─── BusySnapshot / Busy retry helpers ─────────────────────────────────
//
// FrankenSQLite's MVCC can return `BusySnapshot` or `Busy` when a write
// races against another writer/snapshot. These are transient and must be
// retried with backoff, analogous to SQLITE_BUSY under WAL.  The bench
// harness uses a bounded exponential backoff so spurious contention on
// shared structures (e.g. the single-connection cache, the pager, or
// transient snapshot conflicts) does not turn into a hard panic.

/// Maximum number of retry attempts per mutation.
const BENCH_BUSY_MAX_RETRIES: u32 = 32;
/// Starting backoff in microseconds (doubles each attempt, capped at
/// ~100ms via the `min(10)` shift clamp).
const BENCH_BUSY_BACKOFF_US: u64 = 100;
/// Deterministic per-thread jitter window for concurrent writer retries.
const BENCH_BUSY_JITTER_US: u64 = 1_000;

fn is_busy_like(err: &fsqlite::FrankenError) -> bool {
    matches!(
        err,
        fsqlite::FrankenError::BusySnapshot { .. }
            | fsqlite::FrankenError::Busy
            | fsqlite::FrankenError::BusyRecovery
            | fsqlite::FrankenError::DatabaseLocked { .. }
            | fsqlite::FrankenError::LockFailed { .. }
    )
}

fn bench_busy_backoff_delay(attempt: u32, jitter_salt: u64) -> Duration {
    let shift = attempt.min(10);
    let base_us = BENCH_BUSY_BACKOFF_US << shift;
    let jitter_us = if jitter_salt == 0 {
        0
    } else {
        jitter_salt
            .wrapping_mul(37)
            .wrapping_add(u64::from(attempt).wrapping_mul(17))
            % BENCH_BUSY_JITTER_US
    };
    Duration::from_micros(base_us.saturating_add(jitter_us))
}

fn sleep_bench_busy_backoff(attempt: u32, jitter_salt: u64) {
    std::thread::sleep(bench_busy_backoff_delay(attempt, jitter_salt));
}

/// Retry `op` with bounded exponential backoff while it returns a
/// busy-like error.  Returns the last error if retries are exhausted.
fn retry_on_busy<T, F>(mut op: F) -> Result<T, fsqlite::FrankenError>
where
    F: FnMut() -> Result<T, fsqlite::FrankenError>,
{
    let mut attempt: u32 = 0;
    loop {
        match op() {
            Ok(v) => return Ok(v),
            Err(e) if is_busy_like(&e) && attempt < BENCH_BUSY_MAX_RETRIES => {
                sleep_bench_busy_backoff(attempt, 0);
                attempt += 1;
            }
            Err(e) => return Err(e),
        }
    }
}

/// `conn.execute(sql)` with BusySnapshot/Busy retry.
fn fs_execute(conn: &fsqlite::Connection, sql: &str) -> usize {
    retry_on_busy(|| fsqlite_e2e::block_on(conn.execute(sql)))
        .unwrap_or_else(|e| panic!("fsqlite execute failed after retries: {e} (sql={sql})"))
}

/// `conn.prepare(sql)` driven to completion on the crate-local runtime.
///
/// The storage stack is `async`, but this benchmark driver is a plain
/// synchronous `main` that also times `rusqlite` in the same process, so every
/// FrankenSQLite call crosses the boundary through `fsqlite_e2e::block_on`.
fn fs_prepare<'conn>(
    conn: &'conn fsqlite::Connection,
    sql: &str,
) -> fsqlite::PreparedStatement<'conn> {
    fsqlite_e2e::block_on(conn.prepare(sql))
        .unwrap_or_else(|e| panic!("fsqlite prepare failed: {e} (sql={sql})"))
}

/// `stmt.execute_with_params(params)` with BusySnapshot/Busy retry.
fn fs_stmt_execute_with_params(
    stmt: &fsqlite::PreparedStatement<'_>,
    params: &[fsqlite::SqliteValue],
) -> usize {
    retry_on_busy(|| fsqlite_e2e::block_on(stmt.execute_with_params(params))).unwrap_or_else(|e| {
        panic!("fsqlite prepared execute_with_params failed after retries: {e}")
    })
}

/// Async twin of [`retry_on_busy`] for bodies that run inside a single
/// hoisted runtime entry (bd-zavyn). Takes a future *factory* and rebuilds
/// the future on every attempt — a completed future must never be
/// re-polled. The backoff is a thread sleep: every caller is a
/// single-connection section (`:memory:` or a private file), where the
/// busy-like path is unreachable in practice; the multi-writer concurrent
/// section uses attempt-scoped runtime entries with its backoff outside
/// the runtime instead of this helper.
async fn retry_on_busy_async<T, F, Fut>(mut op: F) -> Result<T, fsqlite::FrankenError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, fsqlite::FrankenError>>,
{
    let mut attempt: u32 = 0;
    loop {
        match op().await {
            Ok(v) => return Ok(v),
            Err(e) if is_busy_like(&e) && attempt < BENCH_BUSY_MAX_RETRIES => {
                sleep_bench_busy_backoff(attempt, 0);
                attempt += 1;
            }
            Err(e) => return Err(e),
        }
    }
}

/// Async twin of [`fs_execute`] (bd-zavyn: the timed loops enter the
/// harness runtime once per sample and await instead of re-entering per
/// operation — the per-op entry cost ~333 ns was inflating every
/// FrankenSQLite write sample while the rusqlite arm paid nothing).
async fn fs_execute_async(conn: &fsqlite::Connection, sql: &str) -> usize {
    retry_on_busy_async(|| conn.execute(sql))
        .await
        .unwrap_or_else(|e| panic!("fsqlite execute failed after retries: {e} (sql={sql})"))
}

/// Async twin of [`fs_prepare`].
async fn fs_prepare_async<'conn>(
    conn: &'conn fsqlite::Connection,
    sql: &str,
) -> fsqlite::PreparedStatement<'conn> {
    conn.prepare(sql)
        .await
        .unwrap_or_else(|e| panic!("fsqlite prepare failed: {e} (sql={sql})"))
}

/// Async twin of [`fs_stmt_execute_with_params`].
async fn fs_stmt_execute_with_params_async(
    stmt: &fsqlite::PreparedStatement<'_>,
    params: &[fsqlite::SqliteValue],
) -> usize {
    retry_on_busy_async(|| stmt.execute_with_params(params))
        .await
        .unwrap_or_else(|e| {
            panic!("fsqlite prepared execute_with_params failed after retries: {e}")
        })
}

fn bench_env_flag(name: &str) -> bool {
    std::env::var(name)
        .map(|value| value == "1" || value.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

/// Matched durability for both engines in the concurrent-writer section.
///
/// NORMAL is the explicit default. FULL is available as a separate, labelled
/// experiment. Silently allowing C SQLite to inherit FULL while FrankenSQLite
/// receives NORMAL makes the comparison non-citable (bd-x5gzk).
///
/// Why this exists at all: `synchronous` is a PER-CONNECTION pragma, so the
/// setup connection's NORMAL never reaches the writer connections. A C SQLite
/// writer that never sets it inherits the compiled default
/// SQLITE_DEFAULT_SYNCHRONOUS=2 (FULL) and pays a real WAL fsync per commit,
/// while FrankenSQLite's NORMAL maps to WalCommitSyncPolicy::Deferred and pays
/// none. FULL is also the durability-serious comparison, and was used to probe
/// whether group-commit coalescing engages under a per-commit fsync: it does
/// not for this workload shape, because each writer issues exactly one commit
/// so commits never co-occur in the consolidator's FILLING window (bd-6hgad).
fn concurrent_sync_mode() -> &'static str {
    match std::env::var("FSQLITE_BENCH_CONCURRENT_SYNC") {
        Ok(value) if value.eq_ignore_ascii_case("normal") => "NORMAL",
        Ok(value) if value.eq_ignore_ascii_case("full") => "FULL",
        Ok(value) => panic!("FSQLITE_BENCH_CONCURRENT_SYNC must be NORMAL or FULL, got `{value}`"),
        Err(std::env::VarError::NotPresent) => "NORMAL",
        Err(error) => panic!("could not read FSQLITE_BENCH_CONCURRENT_SYNC: {error}"),
    }
}

fn collect_rusqlite_rows<P: rusqlite::Params>(
    stmt: &mut rusqlite::Statement<'_>,
    params: P,
) -> rusqlite::Result<Vec<Vec<rusqlite::types::Value>>> {
    let col_count = stmt.column_count();
    stmt.query_map(params, move |row| {
        let mut values = Vec::with_capacity(col_count);
        for idx in 0..col_count {
            let value = row.get(idx).unwrap_or(rusqlite::types::Value::Null);
            values.push(value);
        }
        Ok(values)
    })?
    .collect::<Result<Vec<_>, _>>()
}

fn fsqlite_integer(row: &fsqlite::Row, column: usize, context: &str) -> i64 {
    match row.get(column) {
        Some(fsqlite::SqliteValue::Integer(value)) => *value,
        value => panic!("{context}: expected INTEGER at column {column}, got {value:?}"),
    }
}

struct BenchTask<T> {
    handle: BlockingTaskHandle,
    result_rx: mpsc::Receiver<Result<T, String>>,
}

impl<T> BenchTask<T> {
    fn try_wait(self) -> Result<T, String> {
        self.handle.wait();
        match self.result_rx.recv() {
            Ok(result) => result,
            Err(error) => Err(format!(
                "benchmark worker exited without reporting a result: {error}"
            )),
        }
    }

    #[cfg(test)]
    fn wait(self) -> T {
        self.try_wait()
            .unwrap_or_else(|message| panic!("{message}"))
    }
}

fn spawn_bench_task<T, F>(runtime: &Runtime, task: F) -> BenchTask<T>
where
    T: Send + 'static,
    F: FnOnce() -> T + Send + 'static,
{
    let (result_tx, result_rx) = mpsc::channel();
    let handle = runtime
        .spawn_blocking(move || {
            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(task))
                .map_err(panic_payload_to_string);
            let _ = result_tx.send(outcome);
        })
        .expect("comprehensive benchmark runtime must configure a blocking pool");
    BenchTask { handle, result_rx }
}

fn panic_payload_to_string(payload: Box<dyn std::any::Any + Send>) -> String {
    match payload.downcast::<String>() {
        Ok(message) => *message,
        Err(payload) => match payload.downcast::<&'static str>() {
            Ok(message) => (*message).to_owned(),
            Err(_) => "non-string panic payload".to_owned(),
        },
    }
}

// ─── PRAGMA helpers ────────────────────────────────────────────────────

fn is_valid_benchmark_page_size(bytes: u32) -> bool {
    matches!(
        bytes,
        512 | 1024 | 2048 | 4096 | 8192 | 16384 | 32768 | 65536
    )
}

fn benchmark_page_size_bytes() -> u32 {
    static PAGE_SIZE_BYTES: OnceLock<u32> = OnceLock::new();
    *PAGE_SIZE_BYTES.get_or_init(|| {
        let Ok(raw_page_size) = std::env::var("FSQLITE_BENCH_PAGE_SIZE") else {
            return DEFAULT_BENCH_PAGE_SIZE_BYTES;
        };
        let page_size = raw_page_size
            .parse::<u32>()
            .expect("FSQLITE_BENCH_PAGE_SIZE must be an integer byte count");
        assert!(
            is_valid_benchmark_page_size(page_size),
            "FSQLITE_BENCH_PAGE_SIZE must be one of 512, 1024, 2048, 4096, 8192, 16384, 32768, or 65536"
        );
        page_size
    })
}

fn open_fsqlite_memory_connection_for_benchmark() -> fsqlite::Connection {
    let page_size = benchmark_page_size_bytes();
    if page_size == DEFAULT_BENCH_PAGE_SIZE_BYTES {
        fsqlite_e2e::block_on(fsqlite::Connection::open(":memory:")).unwrap()
    } else {
        fsqlite_e2e::block_on(fsqlite::Connection::open_with_page_size(
            ":memory:", page_size,
        ))
        .unwrap()
    }
}

/// Async twin of [`open_fsqlite_memory_connection_for_benchmark`] for timed
/// bodies that run inside one hoisted runtime entry (bd-zavyn).
async fn open_fsqlite_memory_connection_for_benchmark_async() -> fsqlite::Connection {
    let page_size = benchmark_page_size_bytes();
    if page_size == DEFAULT_BENCH_PAGE_SIZE_BYTES {
        fsqlite::Connection::open(":memory:").await.unwrap()
    } else {
        fsqlite::Connection::open_with_page_size(":memory:", page_size)
            .await
            .unwrap()
    }
}

fn apply_pragmas_csqlite(conn: &rusqlite::Connection) {
    conn.execute_batch(&format!(
        "PRAGMA page_size = {};\
         PRAGMA journal_mode = WAL;\
         PRAGMA synchronous = NORMAL;\
         PRAGMA cache_size = -64000;",
        benchmark_page_size_bytes()
    ))
    .unwrap_or_else(|error| panic!("failed to configure C SQLite benchmark connection: {error}"));
}

const FSQLITE_BENCHMARK_PRAGMAS: &[&str] = &[
    "PRAGMA journal_mode = WAL;",
    "PRAGMA synchronous = NORMAL;",
    "PRAGMA cache_size = -64000;",
    // Comprehensive benchmark workloads compare SQLite-compatible query and
    // write paths. They never issue `FOR SYSTEM_TIME` queries, so keep the
    // optional in-memory snapshot ring out of the hot path.
    "PRAGMA fsqlite_capture_time_travel_snapshots=false;",
];

fn apply_pragmas_fsqlite(conn: &fsqlite::Connection) {
    let page_size = format!("PRAGMA page_size = {};", benchmark_page_size_bytes());
    fsqlite_e2e::block_on(conn.execute(&page_size)).unwrap_or_else(|error| {
        panic!("failed to configure FrankenSQLite with `{page_size}`: {error}")
    });
    for pragma in FSQLITE_BENCHMARK_PRAGMAS {
        fsqlite_e2e::block_on(conn.execute(pragma)).unwrap_or_else(|error| {
            panic!("failed to configure FrankenSQLite with `{pragma}`: {error}")
        });
    }
    // Opt-in LAB_UNSAFE write-merge mode for A/B perf measurement of the
    // SSI e-process skip gate. The gate is safe to leave on: under the
    // benchmark's pivot-free workloads, SSI validation is the dominant
    // constant-time overhead per commit.
    if std::env::var("FSQLITE_BENCH_LAB_UNSAFE")
        .map(|s| s == "1" || s.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
    {
        fsqlite_e2e::block_on(conn.execute("PRAGMA fsqlite.write_merge = LAB_UNSAFE;"))
            .unwrap_or_else(|error| panic!("failed to enable LAB_UNSAFE write merge: {error}"));
        // Tight alpha so the gate opens reasonably fast on the short
        // benchmark runs. `alpha = 1e-3` matches the default.
        fsqlite_e2e::block_on(conn.execute("PRAGMA fsqlite.ssi_e_process_alpha = 0.001;"))
            .unwrap_or_else(|error| panic!("failed to set SSI e-process alpha: {error}"));
    }
}

/// Async twin of [`apply_pragmas_fsqlite`] for timed bodies that run inside
/// one hoisted runtime entry (bd-zavyn). Every PRAGMA result stays checked.
async fn apply_pragmas_fsqlite_async(conn: &fsqlite::Connection) {
    let page_size = format!("PRAGMA page_size = {};", benchmark_page_size_bytes());
    conn.execute(&page_size).await.unwrap_or_else(|error| {
        panic!("failed to configure FrankenSQLite with `{page_size}`: {error}")
    });
    for pragma in FSQLITE_BENCHMARK_PRAGMAS {
        conn.execute(pragma).await.unwrap_or_else(|error| {
            panic!("failed to configure FrankenSQLite with `{pragma}`: {error}")
        });
    }
    if std::env::var("FSQLITE_BENCH_LAB_UNSAFE")
        .map(|s| s == "1" || s.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
    {
        conn.execute("PRAGMA fsqlite.write_merge = LAB_UNSAFE;")
            .await
            .unwrap_or_else(|error| panic!("failed to enable LAB_UNSAFE write merge: {error}"));
        conn.execute("PRAGMA fsqlite.ssi_e_process_alpha = 0.001;")
            .await
            .unwrap_or_else(|error| panic!("failed to set SSI e-process alpha: {error}"));
    }
}

fn normalize_csqlite_value(value: rusqlite::types::ValueRef<'_>) -> String {
    match value {
        rusqlite::types::ValueRef::Null => "null".to_owned(),
        rusqlite::types::ValueRef::Integer(value) => value.to_string(),
        rusqlite::types::ValueRef::Real(value) => value.to_string(),
        rusqlite::types::ValueRef::Text(value) => {
            String::from_utf8_lossy(value).to_ascii_lowercase()
        }
        rusqlite::types::ValueRef::Blob(value) => format!("blob:{}", value.len()),
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

/// One-time cross-engine result-set oracle for multi-row benchmark queries
/// (bd-czzlp): both engines run `sql` once, outside the timed loops, and the
/// normalized results must match as multisets. Ordering is not compared
/// because these shapes carry no ORDER BY.
fn assert_result_set_oracle(
    cs_conn: &rusqlite::Connection,
    fs_conn: &fsqlite::Connection,
    sql: &str,
    context: &str,
) {
    let mut stmt = cs_conn
        .prepare(sql)
        .unwrap_or_else(|e| panic!("{context}: C SQLite prepare failed: {e}"));
    let col_count = stmt.column_count();
    let mut cs_rows: Vec<Vec<String>> = stmt
        .query_map([], |row| {
            let mut values = Vec::with_capacity(col_count);
            for idx in 0..col_count {
                values.push(normalize_csqlite_value(row.get_ref(idx)?));
            }
            Ok(values)
        })
        .unwrap_or_else(|e| panic!("{context}: C SQLite query failed: {e}"))
        .collect::<Result<Vec<_>, _>>()
        .unwrap_or_else(|e| panic!("{context}: C SQLite row decode failed: {e}"));
    let mut fs_rows: Vec<Vec<String>> = fsqlite_e2e::block_on(fs_conn.query(sql))
        .unwrap_or_else(|e| panic!("{context}: FrankenSQLite query failed: {e}"))
        .iter()
        .map(|row| row.values().iter().map(normalize_fsqlite_value).collect())
        .collect();
    assert_eq!(
        cs_rows.len(),
        fs_rows.len(),
        "{context}: row-count mismatch (C={}, F={})",
        cs_rows.len(),
        fs_rows.len()
    );
    cs_rows.sort_unstable();
    fs_rows.sort_unstable();
    assert_eq!(cs_rows, fs_rows, "{context}: result-set mismatch");
}

fn normalize_effective_pragma_value(pragma: &str, value: String) -> Result<String, String> {
    if !pragma.eq_ignore_ascii_case("synchronous") {
        return Ok(value);
    }
    normalized_synchronous(&value)
}

fn query_effective_csqlite_pragmas(
    conn: &rusqlite::Connection,
) -> Result<BTreeMap<String, String>, String> {
    let mut values = BTreeMap::new();
    for pragma in ["page_size", "journal_mode", "synchronous", "cache_size"] {
        let sql = format!("PRAGMA {pragma};");
        let value = conn
            .query_row(&sql, [], |row| row.get_ref(0).map(normalize_csqlite_value))
            .map_err(|error| format!("C SQLite `{sql}` failed: {error}"))?;
        let value = normalize_effective_pragma_value(pragma, value)
            .map_err(|error| format!("C SQLite `{sql}` returned invalid value: {error}"))?;
        values.insert(pragma.to_owned(), value);
    }
    Ok(values)
}

fn query_effective_fsqlite_pragmas(
    conn: &fsqlite::Connection,
) -> Result<BTreeMap<String, String>, String> {
    let mut values = BTreeMap::new();
    for pragma in ["page_size", "journal_mode", "synchronous", "cache_size"] {
        let sql = format!("PRAGMA {pragma};");
        let rows = fsqlite_e2e::block_on(conn.query(&sql))
            .map_err(|error| format!("FrankenSQLite `{sql}` failed: {error}"))?;
        let row = rows
            .first()
            .ok_or_else(|| format!("FrankenSQLite `{sql}` returned no row"))?;
        let value = row
            .get(0)
            .ok_or_else(|| format!("FrankenSQLite `{sql}` returned no first column"))?;
        let value = normalize_effective_pragma_value(pragma, normalize_fsqlite_value(value))
            .map_err(|error| format!("FrankenSQLite `{sql}` returned invalid value: {error}"))?;
        values.insert(pragma.to_owned(), value);
    }
    Ok(values)
}

fn record_durability_profile(
    profiles: &mut BTreeMap<String, BTreeMap<String, String>>,
    errors: &mut Vec<String>,
    name: &str,
    result: Result<BTreeMap<String, String>, String>,
) {
    match result {
        Ok(profile) => {
            profiles.insert(name.to_owned(), profile);
        }
        Err(error) => errors.push(error),
    }
}

fn capture_durability_identity() -> JsonDurabilityIdentity {
    let mut effective_profiles = BTreeMap::new();
    let mut validation_errors = Vec::new();

    let csqlite_memory = rusqlite::Connection::open_in_memory()
        .expect("durability certification must open C SQLite memory database");
    apply_pragmas_csqlite(&csqlite_memory);
    record_durability_profile(
        &mut effective_profiles,
        &mut validation_errors,
        "memory.csqlite",
        query_effective_csqlite_pragmas(&csqlite_memory),
    );

    let fsqlite_memory = open_fsqlite_memory_connection_for_benchmark();
    apply_pragmas_fsqlite(&fsqlite_memory);
    let memory_concurrent_mode_default = fsqlite_memory.is_concurrent_mode_default();
    record_durability_profile(
        &mut effective_profiles,
        &mut validation_errors,
        "memory.fsqlite",
        query_effective_fsqlite_pragmas(&fsqlite_memory),
    );

    let directory =
        tempfile::tempdir().expect("durability certification must create a temporary directory");
    let csqlite_path = directory.path().join("csqlite.db");
    let csqlite_file = rusqlite::Connection::open(&csqlite_path)
        .expect("durability certification must open C SQLite file database");
    apply_pragmas_csqlite(&csqlite_file);
    record_durability_profile(
        &mut effective_profiles,
        &mut validation_errors,
        "file.csqlite",
        query_effective_csqlite_pragmas(&csqlite_file),
    );

    let fsqlite_path = directory.path().join("fsqlite.db");
    let fsqlite_path = fsqlite_path
        .to_str()
        .expect("temporary benchmark path must be UTF-8");
    let fsqlite_file = fsqlite_e2e::block_on(fsqlite::Connection::open_with_page_size(
        fsqlite_path,
        benchmark_page_size_bytes(),
    ))
    .expect("durability certification must open FrankenSQLite file database");
    apply_pragmas_fsqlite(&fsqlite_file);
    let file_concurrent_mode_default = fsqlite_file.is_concurrent_mode_default();
    record_durability_profile(
        &mut effective_profiles,
        &mut validation_errors,
        "file.fsqlite",
        query_effective_fsqlite_pragmas(&fsqlite_file),
    );

    for kind in ["memory", "file"] {
        let csqlite = effective_profiles.get(&format!("{kind}.csqlite"));
        let fsqlite = effective_profiles.get(&format!("{kind}.fsqlite"));
        if let (Some(csqlite), Some(fsqlite)) = (csqlite, fsqlite)
            && csqlite != fsqlite
        {
            validation_errors.push(format!(
                "{kind} effective PRAGMAs differ: C SQLite={csqlite:?}, FrankenSQLite={fsqlite:?}"
            ));
        }
    }
    let concurrent_mode_default = memory_concurrent_mode_default && file_concurrent_mode_default;
    if !concurrent_mode_default {
        validation_errors.push(format!(
            "FrankenSQLite concurrent-writer mode is not default-on for every benchmark backend: \
             memory={memory_concurrent_mode_default}, file={file_concurrent_mode_default}"
        ));
    }

    let verified = effective_profiles.len() == 4;
    let matched = verified && validation_errors.is_empty();
    JsonDurabilityIdentity {
        page_size_bytes: benchmark_page_size_bytes(),
        default_synchronous: "NORMAL".to_owned(),
        concurrent_synchronous_modes: vec![concurrent_sync_mode().to_owned()],
        csqlite_pragmas: vec![
            format!("PRAGMA page_size = {};", benchmark_page_size_bytes()),
            "PRAGMA journal_mode = WAL;".to_owned(),
            "PRAGMA synchronous = NORMAL;".to_owned(),
            "PRAGMA cache_size = -64000;".to_owned(),
        ],
        fsqlite_pragmas: std::iter::once(format!(
            "PRAGMA page_size = {};",
            benchmark_page_size_bytes()
        ))
        .chain(
            FSQLITE_BENCHMARK_PRAGMAS
                .iter()
                .map(|pragma| (*pragma).to_owned()),
        )
        .collect(),
        concurrent_mode_default,
        verified,
        matched,
        validation_errors,
        effective_profiles,
    }
}

#[derive(Debug, Clone)]
struct FallbackDecisionLayer(Arc<Mutex<BTreeMap<String, u64>>>);

#[derive(Default)]
struct FallbackDecisionVisitor {
    decision_reason: Option<String>,
}

impl tracing::field::Visit for FallbackDecisionVisitor {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "decision_reason" {
            self.decision_reason = Some(value.to_owned());
        }
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "decision_reason" && self.decision_reason.is_none() {
            self.decision_reason = Some(format!("{value:?}").trim_matches('"').to_owned());
        }
    }
}

impl<S> tracing_subscriber::Layer<S> for FallbackDecisionLayer
where
    S: tracing::Subscriber,
{
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _context: tracing_subscriber::layer::Context<'_, S>,
    ) {
        if event.metadata().target() != "fsqlite.fallback_decision" {
            return;
        }
        let mut visitor = FallbackDecisionVisitor::default();
        event.record(&mut visitor);
        let reason = visitor
            .decision_reason
            .unwrap_or_else(|| "<missing-decision-reason>".to_owned());
        let mut counts = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *counts.entry(reason).or_insert(0) += 1;
    }
}

fn environment_flag_enabled(name: &str) -> bool {
    std::env::var(name)
        .map(|value| value == "1" || value.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

fn probe_execution_routing() -> JsonExecutionRouting {
    let previous_profile_enabled = hot_path_profile_enabled();
    set_hot_path_profile_enabled(true);
    reset_hot_path_profile();

    let conn = open_fsqlite_memory_connection_for_benchmark();
    apply_pragmas_fsqlite(&conn);
    fs_execute(
        &conn,
        "CREATE TABLE routing_probe(id INTEGER PRIMARY KEY, grp INTEGER, value TEXT)",
    );
    let insert = fs_prepare(
        &conn,
        "INSERT INTO routing_probe(id, grp, value) VALUES (?1, ?2, ?3)",
    );
    for id in 1_i64..=4 {
        fs_stmt_execute_with_params(
            &insert,
            &[
                fsqlite::SqliteValue::Integer(id),
                fsqlite::SqliteValue::Integer(id % 2),
                fsqlite::SqliteValue::Text(format!("value-{id}").into()),
            ],
        );
    }
    let update = fs_prepare(&conn, "UPDATE routing_probe SET value = ?2 WHERE id = ?1");
    fs_stmt_execute_with_params(
        &update,
        &[
            fsqlite::SqliteValue::Integer(1),
            fsqlite::SqliteValue::Text("updated".into()),
        ],
    );
    let delete = fs_prepare(&conn, "DELETE FROM routing_probe WHERE id = ?1");
    fs_stmt_execute_with_params(&delete, &[fsqlite::SqliteValue::Integer(4)]);

    let routing_decision_counts = Arc::new(Mutex::new(BTreeMap::new()));
    let subscriber = tracing_subscriber::registry()
        .with(
            tracing_subscriber::filter::Targets::new()
                .with_target("fsqlite.fallback_decision", tracing::Level::DEBUG),
        )
        .with(FallbackDecisionLayer(Arc::clone(&routing_decision_counts)));
    {
        let _subscriber_guard = tracing::subscriber::set_default(subscriber);
        let rows = fsqlite_e2e::block_on(
            conn.query("SELECT grp, GROUP_CONCAT(value) FROM routing_probe GROUP BY grp"),
        )
        .expect("routing certification GROUP BY query must succeed");
        std::hint::black_box(rows);
    }

    let profile = hot_path_profile_snapshot();
    set_hot_path_profile_enabled(previous_profile_enabled);
    let select_routing_decisions = routing_decision_counts
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    let timed_execution_instrumented = [
        "FSQLITE_BENCH_PROFILE_INSERT",
        "FSQLITE_BENCH_PROFILE_CONCURRENT",
        "FSQLITE_BENCH_PROFILE_DML",
        "FSQLITE_BENCH_PROFILE_IDX",
    ]
    .into_iter()
    .any(environment_flag_enabled);
    let prepared_dml_fallbacks = BTreeMap::from([
        (
            "returning".to_owned(),
            profile.prepared_update_delete_fallback_returning,
        ),
        (
            "sqlite_sequence".to_owned(),
            profile.prepared_update_delete_fallback_sqlite_sequence,
        ),
        (
            "without_rowid".to_owned(),
            profile.prepared_update_delete_fallback_without_rowid,
        ),
        (
            "live_vtab".to_owned(),
            profile.prepared_update_delete_fallback_live_vtab,
        ),
        (
            "trigger".to_owned(),
            profile.prepared_update_delete_fallback_trigger,
        ),
        (
            "foreign_key".to_owned(),
            profile.prepared_update_delete_fallback_foreign_key,
        ),
    ]);
    let mut probe_errors = Vec::new();
    if profile.prepared_insert_fast_lane_hits != 4 {
        probe_errors.push(format!(
            "routing probe expected 4 prepared INSERT fast-lane hits, observed {}",
            profile.prepared_insert_fast_lane_hits
        ));
    }
    if profile.prepared_insert_instrumented_lane_hits != 0 {
        probe_errors.push(format!(
            "routing probe observed {} instrumented prepared INSERT hits",
            profile.prepared_insert_instrumented_lane_hits
        ));
    }
    if profile.prepared_direct_insert_executions != 4 {
        probe_errors.push(format!(
            "routing probe expected 4 prepared direct INSERT executions, observed {}",
            profile.prepared_direct_insert_executions
        ));
    }
    if profile.prepared_update_delete_fast_lane_hits != 0 {
        probe_errors.push(format!(
            "routing probe observed {} prepared UPDATE/DELETE fused-lane hits; direct autocommit DML must return before that lane",
            profile.prepared_update_delete_fast_lane_hits
        ));
    }
    if profile.prepared_update_delete_instrumented_lane_hits != 0 {
        probe_errors.push(format!(
            "routing probe observed {} instrumented prepared UPDATE/DELETE hits",
            profile.prepared_update_delete_instrumented_lane_hits
        ));
    }
    if profile.prepared_direct_update_executions != 1 {
        probe_errors.push(format!(
            "routing probe expected 1 prepared direct UPDATE execution, observed {}",
            profile.prepared_direct_update_executions
        ));
    }
    if profile.prepared_direct_delete_executions != 1 {
        probe_errors.push(format!(
            "routing probe expected 1 prepared direct DELETE execution, observed {}",
            profile.prepared_direct_delete_executions
        ));
    }
    if profile.prepared_update_delete_dml_direct_handoff_runs != 0 {
        probe_errors.push(format!(
            "routing probe observed {} prepared UPDATE/DELETE VDBE direct-handoff runs",
            profile.prepared_update_delete_dml_direct_handoff_runs
        ));
    }
    if profile.prepared_table_dml_affected_only_runs != 0 {
        probe_errors.push(format!(
            "routing probe observed {} generic prepared affected-only DML runs",
            profile.prepared_table_dml_affected_only_runs
        ));
    }
    if prepared_dml_fallbacks.values().any(|count| *count != 0) {
        probe_errors.push(format!(
            "routing probe observed prepared DML fallbacks: {prepared_dml_fallbacks:?}"
        ));
    }
    let expected_routing_decisions = BTreeMap::from([
        ("group_by_fallback".to_owned(), 1_u64),
        ("valid_btree_page".to_owned(), 1_u64),
    ]);
    if select_routing_decisions != expected_routing_decisions {
        probe_errors.push(format!(
            "routing probe expected decisions {expected_routing_decisions:?}, observed {select_routing_decisions:?}"
        ));
    }
    if timed_execution_instrumented {
        probe_errors.push(
            "a FSQLITE_BENCH_PROFILE_* switch instruments timed FrankenSQLite execution".to_owned(),
        );
    }

    JsonExecutionRouting {
        probe_scope:
            "untimed surrogate INSERT/UPDATE/DELETE/GROUP BY probe; counters reset before the probe and excluded from score rows"
                .to_owned(),
        timed_routes_verified: false,
        limitations: vec![
            "probe health does not attest any scored statement, schema, prepared object, or dynamic dispatch decision"
                .to_owned(),
        ],
        timed_execution_instrumented,
        parser_fast_path_executions: profile.parser.fast_path_executions,
        parser_slow_path_executions: profile.parser.slow_path_executions,
        prepared_insert_fast_lane_hits: profile.prepared_insert_fast_lane_hits,
        prepared_insert_instrumented_lane_hits: profile.prepared_insert_instrumented_lane_hits,
        prepared_direct_insert_executions: profile.prepared_direct_insert_executions,
        prepared_update_delete_fast_lane_hits: profile.prepared_update_delete_fast_lane_hits,
        prepared_update_delete_instrumented_lane_hits: profile
            .prepared_update_delete_instrumented_lane_hits,
        prepared_direct_update_executions: profile.prepared_direct_update_executions,
        prepared_direct_delete_executions: profile.prepared_direct_delete_executions,
        prepared_update_delete_dml_direct_handoff_runs: profile
            .prepared_update_delete_dml_direct_handoff_runs,
        prepared_table_dml_affected_only_runs: profile.prepared_table_dml_affected_only_runs,
        prepared_dml_fallbacks,
        select_routing_decisions,
        probe_errors,
    }
}

// ─── Report formatting ────────────────────────────────────────────────

struct BenchReport {
    sections: Vec<ReportSection>,
}

struct ReportSection {
    title: String,
    description: String,
    rows: Vec<ReportRow>,
}

struct ReportRow {
    scenario: String,
    csqlite: Option<Measurement>,
    fsqlite: Option<Measurement>,
    fsqlite_concurrent_profile: Option<JsonFsqliteConcurrentProfile>,
    concurrent_readiness: Option<JsonConcurrentReadiness>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
struct CliOptions {
    quick: bool,
    filter: Option<String>,
    html_path: Option<String>,
    emit_html: bool,
    emit_timestamped_json: bool,
    json_out_path: Option<String>,
    json_stdout: bool,
    print_json_schema: bool,
    allow_unverified_provenance: bool,
    bridge_experiment: bool,
    bridge_samples: usize,
    bridge_operations: usize,
    bridge_seed: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum ScenarioCategory {
    ReadSingle,
    ReadAggregate,
    WriteSingle,
    WriteBulk,
    ConcurrentWriters,
    MixedOltp,
}

impl ScenarioCategory {
    const ALL: [Self; 6] = [
        Self::ReadSingle,
        Self::ReadAggregate,
        Self::WriteSingle,
        Self::WriteBulk,
        Self::ConcurrentWriters,
        Self::MixedOltp,
    ];

    const fn id(self) -> &'static str {
        match self {
            Self::ReadSingle => "read_single",
            Self::ReadAggregate => "read_aggregate",
            Self::WriteSingle => "write_single",
            Self::WriteBulk => "write_bulk",
            Self::ConcurrentWriters => "concurrent_writers",
            Self::MixedOltp => "mixed",
        }
    }

    const fn default_weight(self) -> f64 {
        match self {
            Self::ReadSingle => 0.35,
            Self::ReadAggregate => 0.15,
            Self::WriteSingle => 0.30,
            Self::WriteBulk => 0.10,
            Self::ConcurrentWriters | Self::MixedOltp => 0.05,
        }
    }
}

#[derive(Debug, Clone, Serialize, PartialEq)]
struct ReportSummaryStats {
    total_scenarios: usize,
    franken_faster: usize,
    comparable: usize,
    csqlite_faster: usize,
    avg_ratio: Option<f64>,
    average_ratio: Option<f64>,
    geomean_ratio: Option<f64>,
    median_ratio: Option<f64>,
    p90_ratio: Option<f64>,
    p99_ratio: Option<f64>,
    primary_metric: String,
    per_category: BTreeMap<String, JsonCategoryRatioStats>,
    per_category_weighted: JsonWeightedCategoryScore,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
struct DetectedEnvironment {
    os: Option<String>,
    arch: String,
    kernel_release: Option<String>,
    cpu_model: Option<String>,
    cpu_cores: Option<usize>,
    ram_gb: Option<f64>,
    active_toolchain: Option<String>,
    rust_version: Option<String>,
    cargo_version: Option<String>,
    git_commit_sha: Option<String>,
    git_branch: Option<String>,
    git_head_unix_ts: Option<u64>,
    git_dirty: Option<bool>,
    benchmark_binary_modified_unix_ts: Option<u64>,
    benchmark_binary_older_than_git_head: Option<bool>,
    build_profile: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
// These booleans are independent, serialized provenance receipts. Combining
// them into a state enum would make the evidence schema less precise.
#[allow(clippy::struct_excessive_bools)]
struct JsonBuildIdentity {
    workspace_root: String,
    git_commit_sha: String,
    git_branch: String,
    git_dirty: Option<bool>,
    tracked_workspace_inputs_watched: String,
    cargo_profile_family: String,
    selected_profile: String,
    declared_profile: String,
    build_nonce: String,
    opt_level: String,
    debuginfo: String,
    debug_assertions: bool,
    target: String,
    host: String,
    panic_strategy: String,
    panic_abort: bool,
    package_features: Vec<String>,
    encoded_rustflags_hex: String,
    encoded_rustflags_present: bool,
    profile_override_environment_hex: String,
    native_override_environment_hex: String,
    verbose_build_log_path: Option<String>,
    verbose_build_log_sha256: Option<String>,
    verbose_build_log_size_bytes: Option<u64>,
    verbose_build_log_verified: bool,
    profile_proof_scope: String,
    rustc_version: String,
    cargo_version: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct JsonRuntimeSourceIdentity {
    verification_root: String,
    git_commit_sha: Option<String>,
    git_branch: Option<String>,
    git_dirty: Option<bool>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct JsonTracingIdentity {
    rust_log: Option<String>,
    statement_debug_enabled: bool,
    statement_reuse_info_enabled: bool,
    fallback_decision_debug_enabled: bool,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct JsonDurabilityIdentity {
    page_size_bytes: u32,
    default_synchronous: String,
    concurrent_synchronous_modes: Vec<String>,
    csqlite_pragmas: Vec<String>,
    fsqlite_pragmas: Vec<String>,
    concurrent_mode_default: bool,
    verified: bool,
    matched: bool,
    validation_errors: Vec<String>,
    effective_profiles: BTreeMap<String, BTreeMap<String, String>>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct JsonExecutionRouting {
    probe_scope: String,
    timed_routes_verified: bool,
    limitations: Vec<String>,
    timed_execution_instrumented: bool,
    parser_fast_path_executions: u64,
    parser_slow_path_executions: u64,
    prepared_insert_fast_lane_hits: u64,
    prepared_insert_instrumented_lane_hits: u64,
    prepared_direct_insert_executions: u64,
    prepared_update_delete_fast_lane_hits: u64,
    prepared_update_delete_instrumented_lane_hits: u64,
    prepared_direct_update_executions: u64,
    prepared_direct_delete_executions: u64,
    prepared_update_delete_dml_direct_handoff_runs: u64,
    prepared_table_dml_affected_only_runs: u64,
    prepared_dml_fallbacks: BTreeMap<String, u64>,
    select_routing_decisions: BTreeMap<String, u64>,
    probe_errors: Vec<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct JsonBenchmarkProvenance {
    schema_version: String,
    citable: bool,
    status: String,
    validation_errors: Vec<String>,
    build: JsonBuildIdentity,
    runtime_source: JsonRuntimeSourceIdentity,
    working_directory: Option<String>,
    binary_path: Option<String>,
    binary_sha256: Option<String>,
    binary_size_bytes: Option<u64>,
    binary_modified_unix_ts: Option<u64>,
    binary_device_id: Option<u64>,
    binary_inode: Option<u64>,
    cargo_lock_sha256: Option<String>,
    cargo_feature_graph_sha256: Option<String>,
    cargo_feature_graph: Option<String>,
    cargo_feature_graph_command: String,
    command_line: Vec<String>,
    benchmark_environment: BTreeMap<String, String>,
    cpu_affinity: Option<String>,
    runtime_bridge: String,
    tracing: JsonTracingIdentity,
    durability: JsonDurabilityIdentity,
    execution_routing: JsonExecutionRouting,
}

#[cfg(feature = "bridge-experiment")]
#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
enum BridgeArm {
    PerOperationBlockOn,
    #[serde(rename = "inside_existing_runtime")]
    SingleRuntimeEntry,
    WorkerSyncFacade,
}

#[cfg(feature = "bridge-experiment")]
impl BridgeArm {
    const ALL: [Self; 3] = [
        Self::PerOperationBlockOn,
        Self::SingleRuntimeEntry,
        Self::WorkerSyncFacade,
    ];

    const fn id(self) -> &'static str {
        match self {
            Self::PerOperationBlockOn => "per_operation_block_on",
            Self::SingleRuntimeEntry => "inside_existing_runtime",
            Self::WorkerSyncFacade => "worker_sync_facade",
        }
    }
}

#[cfg(feature = "bridge-experiment")]
#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
enum BridgeWorkload {
    ReadyFuture,
    PreparedInsert,
    RawExecuteWithParams,
}

#[cfg(feature = "bridge-experiment")]
impl BridgeWorkload {
    const fn id(self) -> &'static str {
        match self {
            Self::ReadyFuture => "ready_future",
            Self::PreparedInsert => "prepared_insert",
            Self::RawExecuteWithParams => "raw_execute_with_params",
        }
    }
}

#[cfg(feature = "bridge-experiment")]
#[derive(Debug, Clone, Serialize, PartialEq)]
struct JsonBridgeSample {
    workload: BridgeWorkload,
    operation_count: usize,
    block_index: usize,
    order_slot: usize,
    arm: BridgeArm,
    elapsed_ns: u64,
    runtime_entries_total: usize,
    runtime_entries_inside_timed_region: usize,
    caller_future_completions_inside_timed_region: usize,
    engine_dml_future_calls_inside_timed_region: usize,
    worker_commands_total: usize,
    worker_commands_inside_timed_region: usize,
    worker_open_handshakes_total: usize,
    effective_settings: BTreeMap<String, String>,
    oracle_kind: String,
    checksum_count: i64,
    checksum_sum: i64,
    checksum_exact_rows: i64,
}

#[cfg(feature = "bridge-experiment")]
#[derive(Debug, Clone, Serialize, PartialEq)]
struct JsonBridgeArmStats {
    workload: BridgeWorkload,
    operation_count: usize,
    arm: BridgeArm,
    samples: usize,
    median_ns: f64,
    mean_ns: f64,
    p95_ns: f64,
    stddev_ns: f64,
    cv_pct: f64,
    median_ns_per_operation: f64,
}

#[cfg(feature = "bridge-experiment")]
#[derive(Debug, Clone, Serialize, PartialEq)]
struct JsonBridgePairedComparison {
    workload: BridgeWorkload,
    operation_count: usize,
    numerator: BridgeArm,
    denominator: BridgeArm,
    paired_blocks: usize,
    bootstrap_clusters: usize,
    median_ratio: f64,
    mean_ratio: f64,
    geomean_ratio: f64,
    bootstrap_mean_ratio_ci95_low: f64,
    bootstrap_mean_ratio_ci95_high: f64,
}

#[cfg(feature = "bridge-experiment")]
#[derive(Debug, Clone, Serialize, PartialEq)]
struct JsonBridgeReadyRegression {
    predictor: String,
    response: String,
    interpretation: String,
    points: usize,
    paired_blocks: usize,
    bootstrap_clusters: usize,
    intercept_ns: f64,
    slope_ns_per_additional_runtime_entry: f64,
    bootstrap_slope_ci95_low: f64,
    bootstrap_slope_ci95_high: f64,
    r_squared: f64,
}

#[cfg(feature = "bridge-experiment")]
#[derive(Debug, Clone, Serialize, PartialEq)]
struct JsonBridgeConfig {
    samples_per_arm: usize,
    raw_insert_operations: usize,
    ready_operation_counts: Vec<usize>,
    order_seed: u64,
    ordering_policy: String,
    warmup_policy: String,
    timed_region: String,
    arm_contracts: BTreeMap<String, String>,
    affinity_policy: String,
    max_load_average_1m: Option<f64>,
}

#[cfg(feature = "bridge-experiment")]
#[derive(Debug, Clone, Serialize, PartialEq)]
struct JsonBridgeHostState {
    captured_at_utc: String,
    load_average_1m: Option<f64>,
    load_average_5m: Option<f64>,
    load_average_15m: Option<f64>,
    available_parallelism: Option<usize>,
    cpu_affinity: Option<String>,
    selected_cpu_topology: BTreeMap<String, String>,
    scaling_governors: Vec<String>,
    energy_performance_preferences: Vec<String>,
    boost_controls: BTreeMap<String, String>,
    numa_nodes_online: Option<String>,
    memory_available_gb: Option<f64>,
    cpu_pressure_some_avg10: Option<f64>,
    io_pressure_some_avg60: Option<f64>,
    competing_processes: Vec<String>,
    competing_process_scan_error: Option<String>,
}

#[cfg(feature = "bridge-experiment")]
#[derive(Debug, Clone, Serialize, PartialEq)]
struct JsonBridgeReport {
    schema_version: String,
    generated_at_utc: String,
    provenance: JsonBenchmarkProvenance,
    environment: DetectedEnvironment,
    host_state_before: JsonBridgeHostState,
    host_state_checkpoints: Vec<JsonBridgeHostState>,
    host_state_after: JsonBridgeHostState,
    config: JsonBridgeConfig,
    raw_samples: Vec<JsonBridgeSample>,
    arm_statistics: Vec<JsonBridgeArmStats>,
    paired_comparisons: Vec<JsonBridgePairedComparison>,
    ready_runtime_entry_regression: JsonBridgeReadyRegression,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct JsonRunConfig {
    quick: bool,
    filter: Option<String>,
    warmup_iterations: usize,
    min_iterations: usize,
    max_iterations: usize,
    target_duration_secs: u64,
    row_counts: Vec<usize>,
    html_output_path: Option<String>,
    json_output_path: Option<String>,
    json_stdout: bool,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
struct JsonMeasurement {
    median_ms: f64,
    mean_ms: f64,
    min_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    stddev_ms: f64,
    cv_pct: f64,
    rows_per_sec: f64,
    us_per_row: f64,
    iterations: usize,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
struct JsonFsqliteConcurrentProfile {
    total_rows: usize,
    fsqlite_median_ms: f64,
    capture_scope: String,
    counters: BTreeMap<String, u64>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct JsonConcurrentWorkerSettings {
    page_size_bytes: u32,
    journal_mode: String,
    synchronous: String,
    cache_size: i64,
    busy_timeout_ms: u64,
    concurrent_mode: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct JsonConcurrentWorkerReceipt {
    worker_index: usize,
    setup_thread_id: String,
    postflight_thread_id: String,
    setup_cpu_affinity: String,
    postflight_cpu_affinity: String,
    completed_rows: usize,
    settings: JsonConcurrentWorkerSettings,
    settings_verified: bool,
    thread_identity_verified: bool,
    thread_affinity_verified: bool,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct JsonConcurrentSampleReadiness {
    phase: String,
    sample_index: usize,
    engine: String,
    expected_cpu_affinity: String,
    expected_workers: usize,
    expected_rows: usize,
    completed_rows: usize,
    database_rows: usize,
    expected_id_sum: i64,
    database_id_sum: i64,
    timed_scope: String,
    workers: Vec<JsonConcurrentWorkerReceipt>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct JsonConcurrentReadiness {
    csqlite_samples: Vec<JsonConcurrentSampleReadiness>,
    fsqlite_samples: Vec<JsonConcurrentSampleReadiness>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
struct JsonRow {
    scenario_id: String,
    scenario: String,
    category: String,
    csqlite: Option<JsonMeasurement>,
    fsqlite: Option<JsonMeasurement>,
    ratio_fsqlite_over_csqlite: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    fsqlite_concurrent_profile: Option<JsonFsqliteConcurrentProfile>,
    #[serde(skip_serializing_if = "Option::is_none")]
    concurrent_readiness: Option<JsonConcurrentReadiness>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
struct JsonCategoryRatioStats {
    n: usize,
    avg_ratio: Option<f64>,
    geomean_ratio: Option<f64>,
    median_ratio: Option<f64>,
    p90_ratio: Option<f64>,
    p99_ratio: Option<f64>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
struct JsonWeightedCategoryScore {
    primary: bool,
    score: Option<f64>,
    weights: BTreeMap<String, f64>,
    observed_weight: f64,
    missing_categories: Vec<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
struct JsonSection {
    section_id: String,
    title: String,
    description: String,
    rows: Vec<JsonRow>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
struct JsonBenchmarkReport {
    schema_version: String,
    generated_at_utc: String,
    total_elapsed_ms: u64,
    config: JsonRunConfig,
    environment: DetectedEnvironment,
    provenance: JsonBenchmarkProvenance,
    summary: ReportSummaryStats,
    ci_regression_gate: JsonCiRegressionGateDraft,
    sections: Vec<JsonSection>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
struct JsonCiRegressionGateDraft {
    schema_version: String,
    bead_id: String,
    depends_on_bead_id: String,
    status: String,
    eligible: bool,
    ineligibility_reasons: Vec<String>,
    evaluation_result: String,
    thresholds: JsonCiRegressionThresholdsDraft,
    observed: JsonCiRegressionObservedMetrics,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
struct JsonCiRegressionThresholdsDraft {
    avg_ratio_baseline: f64,
    avg_ratio_max: Option<f64>,
    mt_p95_ratio_max: Option<f64>,
    primary_score_max_regression_pct: f64,
    geomean_max_regression_pct: f64,
    per_category_geomean_max_regression_pct: f64,
    p90_max_regression_pct: f64,
    threshold_source: String,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
struct JsonCiRegressionObservedMetrics {
    avg_ratio: Option<f64>,
    primary_score: Option<f64>,
    geomean_ratio: Option<f64>,
    median_ratio: Option<f64>,
    p90_ratio: Option<f64>,
    max_mt_p95_ratio: Option<f64>,
    max_mt_p95_scenario_id: Option<String>,
}

fn compute_report_summary(report: &BenchReport) -> ReportSummaryStats {
    let mut franken_faster = 0_usize;
    let mut csqlite_faster = 0_usize;
    let mut comparable = 0_usize;
    let mut ratios = Vec::new();
    let mut category_ratios: BTreeMap<ScenarioCategory, Vec<f64>> = BTreeMap::new();

    for section in &report.sections {
        for row in &section.rows {
            if let Some(ratio) = row_ratio(row) {
                ratios.push(ratio);
                category_ratios
                    .entry(categorize_scenario(&section.title, &row.scenario))
                    .or_default()
                    .push(ratio);
                if ratio < 0.95 {
                    franken_faster += 1;
                } else if ratio > 1.05 {
                    csqlite_faster += 1;
                } else {
                    comparable += 1;
                }
            }
        }
    }

    let aggregate = ratio_stats(&ratios);
    let mut per_category = BTreeMap::new();
    for category in ScenarioCategory::ALL {
        let stats = ratio_stats(
            category_ratios
                .get(&category)
                .map_or(&[][..], Vec::as_slice),
        );
        per_category.insert(category.id().to_owned(), stats);
    }
    let per_category_weighted = weighted_category_score(&per_category);

    ReportSummaryStats {
        total_scenarios: ratios.len(),
        franken_faster,
        comparable,
        csqlite_faster,
        avg_ratio: aggregate.avg_ratio,
        average_ratio: aggregate.avg_ratio,
        geomean_ratio: aggregate.geomean_ratio,
        median_ratio: aggregate.median_ratio,
        p90_ratio: aggregate.p90_ratio,
        p99_ratio: aggregate.p99_ratio,
        primary_metric: "per_category_weighted.score".to_owned(),
        per_category,
        per_category_weighted,
    }
}

fn categorize_scenario(section_title: &str, scenario: &str) -> ScenarioCategory {
    let section = section_title.to_ascii_lowercase();
    let scenario = scenario.to_ascii_lowercase();

    if section.contains("concurrent writers") {
        return ScenarioCategory::ConcurrentWriters;
    }
    if section.contains("mixed oltp") {
        return ScenarioCategory::MixedOltp;
    }
    if section.contains("transaction strategy") {
        return if scenario.contains("autocommit") {
            ScenarioCategory::WriteSingle
        } else {
            ScenarioCategory::WriteBulk
        };
    }
    if section.contains("insert") || section.contains("record size") {
        return ScenarioCategory::WriteBulk;
    }
    if section.contains("update") || section.contains("delete") {
        return ScenarioCategory::WriteSingle;
    }
    if section.contains("join") || section.contains("subquery") || section.contains("cte") {
        return if scenario.contains("group")
            || scenario.contains("having")
            || scenario.contains("count")
            || scenario.contains("sum")
            || scenario.contains("exists")
            || scenario.contains(" in subquery")
            || scenario.contains("cte")
        {
            ScenarioCategory::ReadAggregate
        } else {
            ScenarioCategory::ReadSingle
        };
    }
    if section.contains("string") {
        return if scenario.contains("group_concat") {
            ScenarioCategory::ReadAggregate
        } else {
            ScenarioCategory::ReadSingle
        };
    }
    if section.contains("read") || section.contains("query") || section.contains("select") {
        return if scenario.contains("count")
            || scenario.contains("group")
            || scenario.contains("sum")
            || scenario.contains("aggregate")
        {
            ScenarioCategory::ReadAggregate
        } else {
            ScenarioCategory::ReadSingle
        };
    }

    ScenarioCategory::ReadSingle
}

fn ratio_stats(ratios: &[f64]) -> JsonCategoryRatioStats {
    let mut sorted: Vec<f64> = ratios
        .iter()
        .copied()
        .filter(|ratio| ratio.is_finite() && *ratio > 0.0)
        .collect();
    sorted.sort_by(f64::total_cmp);
    let n = sorted.len();
    if n == 0 {
        return JsonCategoryRatioStats {
            n: 0,
            avg_ratio: None,
            geomean_ratio: None,
            median_ratio: None,
            p90_ratio: None,
            p99_ratio: None,
        };
    }

    let sum = sorted.iter().sum::<f64>();
    let log_sum = sorted.iter().map(|ratio| ratio.ln()).sum::<f64>();
    JsonCategoryRatioStats {
        n,
        avg_ratio: Some(sum / n as f64),
        geomean_ratio: Some((log_sum / n as f64).exp()),
        median_ratio: percentile_ratio(&sorted, 50.0),
        p90_ratio: percentile_ratio(&sorted, 90.0),
        p99_ratio: percentile_ratio(&sorted, 99.0),
    }
}

fn percentile_ratio(sorted: &[f64], pct: f64) -> Option<f64> {
    if sorted.is_empty() {
        return None;
    }
    let idx = ((pct / 100.0) * (sorted.len() - 1) as f64).ceil() as usize;
    sorted.get(idx.min(sorted.len() - 1)).copied()
}

fn category_weights() -> BTreeMap<String, f64> {
    ScenarioCategory::ALL
        .into_iter()
        .map(|category| (category.id().to_owned(), category.default_weight()))
        .collect()
}

fn weighted_category_score(
    per_category: &BTreeMap<String, JsonCategoryRatioStats>,
) -> JsonWeightedCategoryScore {
    let weights = category_weights();
    let mut weighted_log_sum = 0.0_f64;
    let mut observed_weight = 0.0_f64;
    let mut missing_categories = Vec::new();

    for (category, weight) in &weights {
        match per_category
            .get(category)
            .and_then(|stats| stats.geomean_ratio)
        {
            Some(geomean) => {
                weighted_log_sum += geomean.ln() * weight;
                observed_weight += weight;
            }
            None => missing_categories.push(category.clone()),
        }
    }

    JsonWeightedCategoryScore {
        primary: true,
        score: (observed_weight > 0.0).then_some((weighted_log_sum / observed_weight).exp()),
        weights,
        observed_weight,
        missing_categories,
    }
}

fn row_ratio(row: &ReportRow) -> Option<f64> {
    let csqlite = row.csqlite.as_ref()?;
    let fsqlite = row.fsqlite.as_ref()?;
    let csqlite_nanos = csqlite.median().as_nanos();
    if csqlite_nanos == 0 {
        return None;
    }
    Some(fsqlite.median().as_nanos() as f64 / csqlite_nanos as f64)
}

fn row_p95_ratio(row: &ReportRow) -> Option<f64> {
    let csqlite = row.csqlite.as_ref()?;
    let fsqlite = row.fsqlite.as_ref()?;
    let csqlite_p95_nanos = csqlite.p95().as_nanos();
    if csqlite_p95_nanos == 0 {
        return None;
    }
    Some(fsqlite.p95().as_nanos() as f64 / csqlite_p95_nanos as f64)
}

fn max_multithread_p95_ratio(report: &BenchReport) -> (Option<f64>, Option<String>) {
    report
        .sections
        .iter()
        .filter(|section| section.title == CONCURRENT_WRITERS_SECTION_TITLE)
        .flat_map(|section| {
            let section_id = stable_slug(&section.title);
            section.rows.iter().filter_map(move |row| {
                row_p95_ratio(row).map(|ratio| {
                    let scenario_id = format!("{}__{}", section_id, stable_slug(&row.scenario));
                    (ratio, scenario_id)
                })
            })
        })
        .max_by(|(left, _), (right, _)| left.total_cmp(right))
        .map_or((None, None), |(ratio, scenario_id)| {
            (Some(ratio), Some(scenario_id))
        })
}

fn build_ci_regression_gate(
    report: &BenchReport,
    summary: &ReportSummaryStats,
    provenance: &JsonBenchmarkProvenance,
) -> JsonCiRegressionGateDraft {
    let (max_mt_p95_ratio, max_mt_p95_scenario_id) = max_multithread_p95_ratio(report);
    JsonCiRegressionGateDraft {
        schema_version: CI_REGRESSION_GATE_SCHEMA_V2.to_owned(),
        bead_id: CI_REGRESSION_GATE_BEAD_ID.to_owned(),
        depends_on_bead_id: CI_REGRESSION_BASELINE_BEAD_ID.to_owned(),
        status: if provenance.citable {
            "eligible_compatible_baseline_required".to_owned()
        } else {
            "ineligible".to_owned()
        },
        eligible: provenance.citable,
        ineligibility_reasons: provenance.validation_errors.clone(),
        evaluation_result: "not_evaluated".to_owned(),
        thresholds: JsonCiRegressionThresholdsDraft {
            avg_ratio_baseline: CI_REGRESSION_BASELINE_AVG_RATIO,
            avg_ratio_max: None,
            mt_p95_ratio_max: None,
            primary_score_max_regression_pct: CI_PRIMARY_SCORE_MAX_REGRESSION_PCT,
            geomean_max_regression_pct: CI_GEOMEAN_MAX_REGRESSION_PCT,
            per_category_geomean_max_regression_pct: CI_CATEGORY_GEOMEAN_MAX_REGRESSION_PCT,
            p90_max_regression_pct: CI_P90_MAX_REGRESSION_PCT,
            threshold_source: CI_REGRESSION_GATE_THRESHOLD_SOURCE.to_owned(),
        },
        observed: JsonCiRegressionObservedMetrics {
            avg_ratio: summary.avg_ratio,
            primary_score: summary.per_category_weighted.score,
            geomean_ratio: summary.geomean_ratio,
            median_ratio: summary.median_ratio,
            p90_ratio: summary.p90_ratio,
            max_mt_p95_ratio,
            max_mt_p95_scenario_id,
        },
    }
}

fn stable_slug(value: &str) -> String {
    let mut slug = String::with_capacity(value.len());
    let mut last_was_sep = false;
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() {
            slug.push(ch.to_ascii_lowercase());
            last_was_sep = false;
        } else if !last_was_sep && !slug.is_empty() {
            slug.push('-');
            last_was_sep = true;
        }
    }
    while slug.ends_with('-') {
        slug.pop();
    }
    if slug.is_empty() {
        "unnamed".to_owned()
    } else {
        slug
    }
}

fn duration_ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}

impl JsonMeasurement {
    fn from_measurement(measurement: &Measurement) -> Self {
        Self {
            median_ms: duration_ms(measurement.median()),
            mean_ms: duration_ms(measurement.mean()),
            min_ms: duration_ms(measurement.min()),
            p95_ms: duration_ms(measurement.p95()),
            p99_ms: duration_ms(measurement.p99()),
            stddev_ms: duration_ms(measurement.stddev()),
            cv_pct: measurement.cv_percent(),
            rows_per_sec: measurement.rows_per_sec(),
            us_per_row: measurement.us_per_row(),
            iterations: measurement.iter_count(),
        }
    }
}

fn command_stdout_at(
    current_dir: &std::path::Path,
    program: impl AsRef<std::ffi::OsStr>,
    args: &[&str],
) -> Option<String> {
    std::process::Command::new(program)
        .current_dir(current_dir)
        .args(args)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_owned())
        .filter(|stdout| !stdout.is_empty())
}

fn git_stdout_at(current_dir: &std::path::Path, args: &[&str]) -> Option<String> {
    command_stdout_at(current_dir, "git", args)
}

fn git_dirty_at(current_dir: &std::path::Path) -> Option<bool> {
    std::process::Command::new("git")
        .current_dir(current_dir)
        .args(["status", "--porcelain=v1", "--untracked-files=normal"])
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| !output.stdout.is_empty())
}

fn parse_build_bool(value: &str) -> Option<bool> {
    match value {
        "true" => Some(true),
        "false" => Some(false),
        _ => None,
    }
}

fn lowercase_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn sha256_bytes(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    lowercase_hex(digest.as_ref())
}

fn sha256_file(path: &std::path::Path) -> std::io::Result<String> {
    let mut file = std::fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    let digest = hasher.finalize();
    Ok(lowercase_hex(digest.as_ref()))
}

#[derive(Debug)]
struct BuildLogReceipt {
    path: Option<String>,
    sha256: Option<String>,
    size_bytes: Option<u64>,
    verified: bool,
}

fn split_posix_shell_words(command: &str) -> Option<Vec<String>> {
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum Quote {
        None,
        Single,
        Double,
    }

    let mut words = Vec::new();
    let mut word = String::new();
    let mut word_started = false;
    let mut quote = Quote::None;
    let mut chars = command.chars();
    while let Some(ch) = chars.next() {
        match quote {
            Quote::None => match ch {
                '\'' => {
                    quote = Quote::Single;
                    word_started = true;
                }
                '"' => {
                    quote = Quote::Double;
                    word_started = true;
                }
                '\\' => {
                    word.push(chars.next()?);
                    word_started = true;
                }
                ch if ch.is_whitespace() => {
                    if word_started {
                        words.push(std::mem::take(&mut word));
                        word_started = false;
                    }
                }
                _ => {
                    word.push(ch);
                    word_started = true;
                }
            },
            Quote::Single => {
                if ch == '\'' {
                    quote = Quote::None;
                } else {
                    word.push(ch);
                }
            }
            Quote::Double => match ch {
                '"' => quote = Quote::None,
                '\\' => {
                    word.push(chars.next()?);
                    word_started = true;
                }
                _ => word.push(ch),
            },
        }
    }
    if quote != Quote::None {
        return None;
    }
    if word_started {
        words.push(word);
    }
    Some(words)
}

fn rustc_command_tokens(line: &str) -> Option<Vec<String>> {
    let command = line.trim().strip_prefix("Running `")?.strip_suffix('`')?;
    let tokens = split_posix_shell_words(command)?;
    tokens
        .windows(2)
        .any(|pair| pair[0] == "--crate-name")
        .then_some(tokens)
}

fn rustc_crate_name(tokens: &[String]) -> Option<&str> {
    tokens
        .windows(2)
        .find(|pair| pair[0] == "--crate-name")
        .map(|pair| pair[1].as_str())
}

fn rustc_target(tokens: &[String]) -> Option<&str> {
    tokens
        .windows(2)
        .find(|pair| pair[0] == "--target")
        .map(|pair| pair[1].as_str())
}

fn rustc_log_line_for_crate<'a>(contents: &'a str, crate_name: &str) -> Option<&'a str> {
    let mut matches = contents.lines().filter(|line| {
        rustc_command_tokens(line)
            .as_deref()
            .and_then(rustc_crate_name)
            == Some(crate_name)
    });
    let only = matches.next()?;
    matches.next().is_none().then_some(only)
}

fn rustc_codegen_options(tokens: &[String]) -> Vec<&str> {
    let mut options = Vec::new();
    let mut index = 0;
    while index < tokens.len() {
        if tokens[index] == "-C" {
            if let Some(value) = tokens.get(index + 1) {
                options.push(value.as_str());
            }
            index += 2;
            continue;
        }
        if let Some(value) = tokens[index].strip_prefix("-C")
            && !value.is_empty()
        {
            options.push(value);
        }
        index += 1;
    }
    options
}

fn rustc_codegen_values<'a>(tokens: &'a [String], key: &str) -> Vec<Option<&'a str>> {
    rustc_codegen_options(tokens)
        .into_iter()
        .filter_map(|option| match option.split_once('=') {
            Some((observed_key, value)) if observed_key == key => Some(Some(value)),
            None if option == key => Some(None),
            _ => None,
        })
        .collect()
}

fn rustc_codegen_is_exact(tokens: &[String], key: &str, expected: Option<&str>) -> bool {
    rustc_codegen_values(tokens, key).as_slice() == [expected]
}

fn rustc_codegen_is_disabled_or_omitted(tokens: &[String], key: &str) -> bool {
    matches!(
        rustc_codegen_values(tokens, key).as_slice(),
        [] | [Some("off" | "no")]
    )
}

fn rustc_codegen_options_are_canonical_surface(tokens: &[String]) -> bool {
    const ALLOWED_KEYS: &[&str] = &[
        "codegen-units",
        "debuginfo",
        "debug-assertions",
        "embed-bitcode",
        "extra-filename",
        "linker-plugin-lto",
        "lto",
        "metadata",
        "opt-level",
        "overflow-checks",
        "panic",
        "rpath",
        "strip",
    ];
    rustc_codegen_options(tokens).into_iter().all(|option| {
        let key = option
            .split_once('=')
            .map_or(option, |(observed_key, _)| observed_key);
        ALLOWED_KEYS.contains(&key)
    })
}

fn rustc_has_no_unstable_overrides(tokens: &[String]) -> bool {
    !tokens.iter().any(|token| token.starts_with("-Z"))
}

fn rustc_invocation_is_direct(tokens: &[String]) -> bool {
    let Some(executable) = tokens
        .iter()
        .find(|token| !token.contains('=') && !token.starts_with("env"))
    else {
        return false;
    };
    std::path::Path::new(executable)
        .file_name()
        .and_then(std::ffi::OsStr::to_str)
        == Some("rustc")
}

fn rustc_target_invocation_is_canonical(
    tokens: &[String],
    profile: &str,
    final_binary: bool,
) -> bool {
    let expected_opt_level = match profile {
        "release-perf" => "3",
        "release" => "z",
        _ => return false,
    };
    if !rustc_invocation_is_direct(tokens)
        || !rustc_codegen_options_are_canonical_surface(tokens)
        || !rustc_has_no_unstable_overrides(tokens)
        || !rustc_codegen_is_exact(tokens, "opt-level", Some(expected_opt_level))
        || !rustc_codegen_is_exact(tokens, "codegen-units", Some("1"))
        || !rustc_codegen_is_disabled_or_omitted(tokens, "debug-assertions")
        || !rustc_codegen_is_disabled_or_omitted(tokens, "overflow-checks")
    {
        return false;
    }
    if final_binary {
        rustc_codegen_is_exact(tokens, "lto", None)
            && rustc_codegen_values(tokens, "linker-plugin-lto").is_empty()
            && rustc_codegen_is_exact(tokens, "panic", Some("abort"))
            && rustc_codegen_is_exact(tokens, "strip", Some("symbols"))
    } else {
        rustc_codegen_is_exact(tokens, "linker-plugin-lto", None)
            && rustc_codegen_values(tokens, "lto").is_empty()
    }
}

fn build_log_proves_profile(
    contents: &str,
    build_nonce: &str,
    profile: &str,
    target: &str,
) -> bool {
    let required_crates = [
        "asupersync",
        "fsqlite_error",
        "fsqlite_types",
        "fsqlite_observability",
        "fsqlite_ast",
        "fsqlite_parser",
        "fsqlite_planner",
        "fsqlite_func",
        "fsqlite_vfs",
        "fsqlite_wal",
        "fsqlite_pager",
        "fsqlite_mvcc",
        "fsqlite_btree",
        "fsqlite_vdbe",
        "fsqlite_ext_json",
        "fsqlite_ext_fts5",
        "fsqlite_ext_rtree",
        "fsqlite_core",
        "fsqlite",
        "fsqlite_e2e",
        "comprehensive_bench",
    ];
    let required_invocations_are_canonical = required_crates.iter().all(|crate_name| {
        rustc_log_line_for_crate(contents, crate_name)
            .and_then(rustc_command_tokens)
            .is_some_and(|tokens| {
                rustc_target(&tokens) == Some(target)
                    && rustc_target_invocation_is_canonical(
                        &tokens,
                        profile,
                        *crate_name == "comprehensive_bench",
                    )
            })
    });
    let target_invocations_are_canonical = contents
        .lines()
        .filter_map(rustc_command_tokens)
        .filter(|tokens| rustc_target(tokens) == Some(target))
        .all(|tokens| {
            rustc_target_invocation_is_canonical(
                &tokens,
                profile,
                rustc_crate_name(&tokens) == Some("comprehensive_bench"),
            )
        });
    let expected_nonce = format!("FSQLITE_BENCH_BUILD_NONCE={build_nonce}");
    let final_binary_is_nonce_bound = rustc_log_line_for_crate(contents, "comprehensive_bench")
        .and_then(rustc_command_tokens)
        .is_some_and(|tokens| tokens.iter().any(|token| token == &expected_nonce));
    let finished_profile = contents.contains(&format!("Finished `{profile}` profile"))
        || contents.contains(&format!("Finished '{profile}' profile"));
    required_invocations_are_canonical
        && target_invocations_are_canonical
        && final_binary_is_nonce_bound
        && finished_profile
}

fn capture_build_log_receipt(build_nonce: &str, profile: &str, target: &str) -> BuildLogReceipt {
    let Some(configured_path) = std::env::var_os("FSQLITE_BENCH_BUILD_LOG_PATH") else {
        return BuildLogReceipt {
            path: None,
            sha256: None,
            size_bytes: None,
            verified: false,
        };
    };
    let Ok(path) = std::fs::canonicalize(configured_path) else {
        return BuildLogReceipt {
            path: None,
            sha256: None,
            size_bytes: None,
            verified: false,
        };
    };
    let path_text = path.to_string_lossy().into_owned();
    let sha256 = sha256_file(&path).ok();
    let size_bytes = std::fs::metadata(&path).ok().map(|metadata| metadata.len());
    let verified = std::fs::read_to_string(&path)
        .is_ok_and(|contents| build_log_proves_profile(&contents, build_nonce, profile, target));
    BuildLogReceipt {
        path: Some(path_text),
        sha256,
        size_bytes,
        verified,
    }
}

fn binary_file_identity(metadata: &std::fs::Metadata) -> (Option<u64>, Option<u64>) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;
        (Some(metadata.dev()), Some(metadata.ino()))
    }
    #[cfg(not(unix))]
    {
        let _ = metadata;
        (None, None)
    }
}

fn cpu_affinity() -> Option<String> {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|status| {
            status.lines().find_map(|line| {
                line.strip_prefix("Cpus_allowed_list:")
                    .map(str::trim)
                    .filter(|cpus| !cpus.is_empty())
                    .map(str::to_owned)
            })
        })
}

fn capture_cargo_feature_graph(
    workspace_root: &std::path::Path,
    target: &str,
    package_features: &[String],
) -> Option<String> {
    let mut command = std::process::Command::new("cargo");
    command.current_dir(workspace_root).args([
        "tree",
        "--locked",
        "--offline",
        "-p",
        "fsqlite-e2e",
        "-e",
        "features,no-dev",
        "--no-default-features",
        "--target",
        target,
    ]);
    if !package_features.is_empty() {
        command.arg("--features").arg(package_features.join(","));
    }
    command
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_owned())
        .filter(|graph| !graph.is_empty())
}

fn decode_lower_hex(encoded: &str, field: &str) -> Result<Vec<u8>, String> {
    if encoded.len() % 2 != 0
        || encoded
            .bytes()
            .any(|byte| !byte.is_ascii_digit() && !(b'a'..=b'f').contains(&byte))
    {
        return Err(format!("{field} is not even-length lowercase hexadecimal"));
    }
    let decoded = encoded
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let digit = |byte: u8| -> u8 {
                match byte {
                    b'0'..=b'9' => byte - b'0',
                    b'a'..=b'f' => byte - b'a' + 10,
                    _ => unreachable!("hex alphabet checked above"),
                }
            };
            (digit(pair[0]) << 4) | digit(pair[1])
        })
        .collect();
    Ok(decoded)
}

fn decode_build_environment(encoded_hex: &str) -> Result<BTreeMap<String, String>, String> {
    let decoded = decode_lower_hex(encoded_hex, "build override environment")?;
    let decoded = String::from_utf8(decoded)
        .map_err(|_| "build override environment is not valid UTF-8".to_owned())?;
    if decoded.is_empty() {
        return Ok(BTreeMap::new());
    }

    let mut environment = BTreeMap::new();
    for assignment in decoded.split('\0') {
        let (name, value) = assignment.split_once('=').ok_or_else(|| {
            format!("build override environment entry has no `=` separator: `{assignment}`")
        })?;
        if name.is_empty() {
            return Err("build override environment contains an empty variable name".to_owned());
        }
        if environment
            .insert(name.to_owned(), value.to_owned())
            .is_some()
        {
            return Err(format!(
                "build override environment contains duplicate variable `{name}`"
            ));
        }
    }
    Ok(environment)
}

#[cfg(test)]
fn encode_build_environment(environment: &BTreeMap<String, String>) -> String {
    let assignments = environment
        .iter()
        .map(|(name, value)| format!("{name}={value}"))
        .collect::<Vec<_>>()
        .join("\0");
    lowercase_hex(assignments.as_bytes())
}

fn canonical_profile_environment(
    selected_profile: &str,
) -> Result<BTreeMap<String, String>, String> {
    let (prefix, opt_level) = match selected_profile {
        "release-perf" => ("CARGO_PROFILE_RELEASE_PERF", "3"),
        "release" => ("CARGO_PROFILE_RELEASE", "z"),
        other => {
            return Err(format!(
                "unsupported citable Cargo output profile `{other}`"
            ));
        }
    };
    Ok(BTreeMap::from([
        (
            "CARGO_BUILD_RUSTC_WORKSPACE_WRAPPER".to_owned(),
            String::new(),
        ),
        ("CARGO_BUILD_RUSTC_WRAPPER".to_owned(), String::new()),
        ("CARGO_BUILD_RUSTFLAGS".to_owned(), String::new()),
        ("RUSTC_WORKSPACE_WRAPPER".to_owned(), String::new()),
        ("RUSTC_WRAPPER".to_owned(), String::new()),
        (format!("{prefix}_CODEGEN_UNITS"), "1".to_owned()),
        (format!("{prefix}_DEBUG"), "false".to_owned()),
        (format!("{prefix}_DEBUG_ASSERTIONS"), "false".to_owned()),
        (format!("{prefix}_INCREMENTAL"), "false".to_owned()),
        (format!("{prefix}_LTO"), "true".to_owned()),
        (format!("{prefix}_OPT_LEVEL"), opt_level.to_owned()),
        (format!("{prefix}_OVERFLOW_CHECKS"), "false".to_owned()),
        (format!("{prefix}_PANIC"), "abort".to_owned()),
        (format!("{prefix}_RPATH"), "false".to_owned()),
        (format!("{prefix}_SPLIT_DEBUGINFO"), "off".to_owned()),
        (format!("{prefix}_STRIP"), "true".to_owned()),
    ]))
}

const fn compiled_panic_strategy() -> &'static str {
    if cfg!(panic = "abort") {
        "abort"
    } else {
        "unwind"
    }
}

fn canonical_native_environment() -> BTreeMap<String, String> {
    BTreeMap::from([(
        "LIBSQLITE3_FLAGS".to_owned(),
        "-DSQLITE_ENABLE_MATH_FUNCTIONS".to_owned(),
    )])
}

fn validate_build_identity(build: &JsonBuildIdentity) -> Vec<String> {
    let mut errors = Vec::new();
    if build.git_commit_sha.len() != 40
        || !build
            .git_commit_sha
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        errors.push("build Git SHA is absent or malformed".to_owned());
    }
    if build.git_dirty != Some(false) {
        errors.push("benchmark binary was built from a dirty or unknown tree".to_owned());
    }
    if build.tracked_workspace_inputs_watched != "complete" {
        errors.push("build script could not watch every tracked workspace input".to_owned());
    }
    if build.declared_profile == "unspecified" {
        errors.push(
            "build profile label missing; set FSQLITE_BENCH_PROFILE_NAME while building".to_owned(),
        );
    }
    if build.selected_profile != build.declared_profile {
        errors.push(format!(
            "declared profile `{}` does not match Cargo output profile `{}`",
            build.declared_profile, build.selected_profile
        ));
    }
    if build.cargo_profile_family != "release" {
        errors.push(format!(
            "citable benchmark profile must inherit Cargo release, got family `{}`",
            build.cargo_profile_family
        ));
    }
    match build.selected_profile.as_str() {
        "release-perf" if build.opt_level != "3" => errors.push(format!(
            "selected release-perf profile has effective opt-level {}",
            build.opt_level
        )),
        "release" if build.opt_level != "z" => errors.push(format!(
            "selected release profile has effective opt-level {}",
            build.opt_level
        )),
        "release-perf" | "release" => {}
        other => errors.push(format!(
            "unsupported citable Cargo output profile `{other}`"
        )),
    }
    if build.debuginfo != "false" {
        errors.push(format!(
            "citable profile must disable debuginfo, got `{}`",
            build.debuginfo
        ));
    }
    if build.debug_assertions {
        errors.push("citable profile compiled with debug assertions".to_owned());
    }
    if build.panic_strategy != "abort" || !build.panic_abort {
        errors.push(format!(
            "citable profile must compile panic=abort, got build `{}` and binary cfg panic_abort={}",
            build.panic_strategy, build.panic_abort
        ));
    }
    match decode_lower_hex(&build.encoded_rustflags_hex, "encoded rustflags") {
        Ok(flags) if flags.is_empty() => {}
        Ok(flags) => errors.push(format!(
            "citable builds require empty encoded rustflags, got `{}`",
            String::from_utf8_lossy(&flags).replace('\u{1f}', " ")
        )),
        Err(error) => errors.push(error),
    }
    if !build.encoded_rustflags_present {
        errors.push(
            "citable builds require explicitly present, empty CARGO_ENCODED_RUSTFLAGS to override Cargo-config rustflags"
                .to_owned(),
        );
    }
    match decode_build_environment(
        &build.profile_override_environment_hex,
    ) {
        Ok(actual) => match canonical_profile_environment(&build.selected_profile) {
            Ok(expected) if actual == expected => {}
            Ok(expected) => errors.push(format!(
                "build environment does not exactly force the canonical `{}` profile: expected {expected:?}, observed {actual:?}",
                build.selected_profile
            )),
            Err(error) => errors.push(error),
        },
        Err(error) => errors.push(error),
    }
    match decode_build_environment(&build.native_override_environment_hex) {
        Ok(environment) if environment == canonical_native_environment() => {}
        Ok(environment) => errors.push(format!(
            "citable builds require exactly the repository's canonical native build environment {:?}, got {environment:?}",
            canonical_native_environment()
        )),
        Err(error) => errors.push(error),
    }
    if build.build_nonce.len() != 64
        || !build
            .build_nonce
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        errors.push(format!(
            "citable builds require a 64-character lowercase hexadecimal build nonce, got `{}`",
            build.build_nonce
        ));
    }
    match (
        build.verbose_build_log_path.as_deref(),
        build.verbose_build_log_sha256.as_deref(),
        build.verbose_build_log_size_bytes,
        build.verbose_build_log_verified,
    ) {
        (Some(path), Some(sha256), Some(size), true)
            if std::path::Path::new(path).is_absolute()
                && size > 0
                && sha256.len() == 64
                && sha256.bytes().all(|byte| {
                    byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)
                }) => {}
        receipt => errors.push(format!(
            "citable builds require an absolute, nonempty, SHA-bound cargo -vv log whose explicit-target rustc invocations prove the nonce plus the required opt-level, codegen-units, debug-assertions, overflow-checks, LTO partition, and final panic/strip settings for the required Rust hot-path crate set and final benchmark binary; compiler identity and all other rustc flags remain outside this receipt's proof scope; observed {receipt:?}"
        )),
    }
    if build.target == "unknown" || build.host == "unknown" {
        errors.push("effective build target/host identity is incomplete".to_owned());
    }
    if build.rustc_version == "unknown" || build.cargo_version == "unknown" {
        errors.push("build toolchain identity is incomplete".to_owned());
    }
    errors
}

fn measurement_design_validation_errors(runtime_bridge: &str) -> Vec<String> {
    match runtime_bridge {
        // bd-zavyn: the timed FrankenSQLite bodies now enter the harness
        // runtime once per sample / transaction attempt instead of once per
        // operation, so samples measure the engine rather than ~333 ns of
        // bridge entry per op. The design remains non-citable for the same
        // reasons as before the hoist: pairing, work oracles, and host
        // gating are unchanged (Gate 0 owns those).
        "scenario_scoped_thread_local_block_on" | "per_operation_thread_local_block_on" => vec![
            "generic comprehensive measurements are diagnostic-only: engines run in unpaired C-first/FrankenSQLite-second adaptive blocks, scored rows lack complete work oracles, and host/topology state is not release-gated"
                .to_owned(),
            "generic C-reference measurements do not receipt-bind sqlite_source_id(), compile_options, the SQLite amalgamation/library hash, or the resolved native C compiler path/version/hash"
                .to_owned(),
        ],
        "three_arm_per_operation_inside_existing_runtime_worker_sync_facade" => vec![
            "the three-arm bridge is diagnostic-only until citable runs prove an isolated cgroup-v2 cpuset partition covering selected CPUs and online SMT siblings, full-dynticks coverage, disjoint effective IRQ affinities, per-thread affinity, and stable selected-policy frequency controls"
                .to_owned(),
            "the three-arm bridge has no fail-bounded watchdog around a wedged engine future or worker-facade call"
                .to_owned(),
        ],
        other => vec![format!(
            "unknown measurement design `{other}` cannot produce citable evidence"
        )],
    }
}

impl JsonBenchmarkProvenance {
    fn capture(command_line: Vec<String>, runtime_bridge: &str) -> Self {
        let build_workspace_root = env!("FSQLITE_BENCH_BUILD_WORKSPACE_ROOT").to_owned();
        let verification_root = std::env::var("FSQLITE_BENCH_SOURCE_ROOT")
            .unwrap_or_else(|_| build_workspace_root.clone());
        let verification_path = std::path::Path::new(&verification_root);
        let package_features = env!("FSQLITE_BENCH_BUILD_FEATURES")
            .split(',')
            .filter(|feature| !feature.is_empty())
            .map(str::to_owned)
            .collect::<Vec<_>>();
        let build_target = env!("FSQLITE_BENCH_BUILD_TARGET").to_owned();
        let runtime_git_sha = git_stdout_at(verification_path, &["rev-parse", "--verify", "HEAD"]);
        let runtime_git_branch = git_stdout_at(verification_path, &["branch", "--show-current"]);
        let runtime_git_dirty = git_dirty_at(verification_path);
        let binary_path = std::env::current_exe()
            .ok()
            .map(|path| path.to_string_lossy().into_owned());
        let binary_metadata = binary_path
            .as_deref()
            .and_then(|path| std::fs::metadata(path).ok());
        let binary_sha256 = binary_path
            .as_deref()
            .and_then(|path| sha256_file(std::path::Path::new(path)).ok());
        let binary_size_bytes = binary_metadata.as_ref().map(std::fs::Metadata::len);
        let binary_modified_unix_ts = binary_metadata
            .as_ref()
            .and_then(|metadata| metadata.modified().ok())
            .and_then(|modified| {
                modified
                    .duration_since(std::time::UNIX_EPOCH)
                    .ok()
                    .map(|duration| duration.as_secs())
            });
        let (binary_device_id, binary_inode) = binary_metadata
            .as_ref()
            .map(binary_file_identity)
            .unwrap_or((None, None));
        let cargo_lock_sha256 = sha256_file(&verification_path.join("Cargo.lock")).ok();
        let cargo_feature_graph =
            capture_cargo_feature_graph(verification_path, &build_target, &package_features);
        let cargo_feature_graph_sha256 = cargo_feature_graph
            .as_deref()
            .map(str::as_bytes)
            .map(sha256_bytes);
        let cargo_feature_graph_command = format!(
            "cargo tree --locked --offline -p fsqlite-e2e -e features,no-dev \
             --no-default-features --target {}{}",
            build_target,
            if package_features.is_empty() {
                String::new()
            } else {
                format!(" --features {}", package_features.join(","))
            }
        );
        let benchmark_environment = [
            "FSQLITE_BENCH_CONCURRENT_SYNC",
            "FSQLITE_BENCH_BUILD_LOG_PATH",
            "FSQLITE_BENCH_EXPECTED_CPU_AFFINITY",
            "FSQLITE_BENCH_LAB_UNSAFE",
            "FSQLITE_BENCH_MAX_LOAD_1M",
            "FSQLITE_BENCH_PAGE_SIZE",
            "FSQLITE_BENCH_PROFILE_CONCURRENT",
            "FSQLITE_BENCH_PROFILE_DML",
            "FSQLITE_BENCH_PROFILE_IDX",
            "FSQLITE_BENCH_PROFILE_IDX_ITERS",
            "FSQLITE_BENCH_PROFILE_INSERT",
            "FSQLITE_BENCH_SOURCE_ROOT",
            "RUST_LOG",
        ]
        .into_iter()
        .filter_map(|name| {
            std::env::var(name)
                .ok()
                .map(|value| (name.to_owned(), value))
        })
        .collect();

        let selected_profile = env!("FSQLITE_BENCH_BUILD_SELECTED_PROFILE").to_owned();
        let build_nonce = env!("FSQLITE_BENCH_BUILD_NONCE").to_owned();
        let build_log = capture_build_log_receipt(&build_nonce, &selected_profile, &build_target);
        let build = JsonBuildIdentity {
            workspace_root: build_workspace_root,
            git_commit_sha: env!("FSQLITE_BENCH_BUILD_GIT_SHA").to_owned(),
            git_branch: env!("FSQLITE_BENCH_BUILD_GIT_BRANCH").to_owned(),
            git_dirty: parse_build_bool(env!("FSQLITE_BENCH_BUILD_GIT_DIRTY")),
            tracked_workspace_inputs_watched: env!("FSQLITE_BENCH_BUILD_INPUT_TRACKING")
                .to_owned(),
            cargo_profile_family: env!("FSQLITE_BENCH_BUILD_PROFILE").to_owned(),
            selected_profile,
            declared_profile: env!("FSQLITE_BENCH_BUILD_PROFILE_LABEL").to_owned(),
            build_nonce,
            opt_level: env!("FSQLITE_BENCH_BUILD_OPT_LEVEL").to_owned(),
            debuginfo: env!("FSQLITE_BENCH_BUILD_DEBUG").to_owned(),
            debug_assertions: cfg!(debug_assertions),
            target: build_target,
            host: env!("FSQLITE_BENCH_BUILD_HOST").to_owned(),
            // Cargo build scripts are host executables and compile with unwind
            // even when the benchmark target uses panic=abort. Only the target
            // crate's cfg can attest the binary's effective panic strategy.
            panic_strategy: compiled_panic_strategy().to_owned(),
            panic_abort: cfg!(panic = "abort"),
            package_features,
            encoded_rustflags_hex: env!("FSQLITE_BENCH_BUILD_RUSTFLAGS_HEX").to_owned(),
            encoded_rustflags_present: parse_build_bool(env!(
                "FSQLITE_BENCH_BUILD_ENCODED_RUSTFLAGS_PRESENT"
            ))
            .unwrap_or(false),
            profile_override_environment_hex: env!(
                "FSQLITE_BENCH_BUILD_PROFILE_OVERRIDES_HEX"
            )
            .to_owned(),
            native_override_environment_hex: env!(
                "FSQLITE_BENCH_BUILD_NATIVE_OVERRIDES_HEX"
            )
            .to_owned(),
            verbose_build_log_path: build_log.path,
            verbose_build_log_sha256: build_log.sha256,
            verbose_build_log_size_bytes: build_log.size_bytes,
            verbose_build_log_verified: build_log.verified,
            profile_proof_scope:
                "selected profile bound from Cargo OUT_DIR; release fields forced through exact Cargo profile environment values; effective opt/debuginfo/panic cross-checked; explicitly present empty encoded rustflags and empty Cargo wrapper overrides required; the native override environment is constrained but the native compiler/source identity is not yet proven; tracked workspace inputs watched; a unique lowercase-hex build nonce binds the benchmark to a path- and SHA-bound cargo -vv log whose direct explicit-target rustc invocations prove only the required opt-level, codegen-units, debug-assertions, overflow-checks, LTO partition, and final panic/strip settings for the required Rust hot-path crate set and final benchmark binary, not compiler identity, other rustc flags, or every Cargo unit"
                    .to_owned(),
            rustc_version: env!("FSQLITE_BENCH_BUILD_RUSTC_VERSION").to_owned(),
            cargo_version: env!("FSQLITE_BENCH_BUILD_CARGO_VERSION").to_owned(),
        };
        let runtime_source = JsonRuntimeSourceIdentity {
            verification_root,
            git_commit_sha: runtime_git_sha,
            git_branch: runtime_git_branch,
            git_dirty: runtime_git_dirty,
        };
        let tracing = JsonTracingIdentity {
            rust_log: std::env::var("RUST_LOG").ok(),
            statement_debug_enabled: tracing::enabled!(
                target: "fsqlite.statement",
                tracing::Level::DEBUG
            ),
            statement_reuse_info_enabled: tracing::enabled!(
                target: "fsqlite.statement_reuse",
                tracing::Level::INFO
            ),
            fallback_decision_debug_enabled: tracing::enabled!(
                target: "fsqlite.fallback_decision",
                tracing::Level::DEBUG
            ),
        };
        let durability = capture_durability_identity();
        let execution_routing = probe_execution_routing();

        let mut validation_errors = validate_build_identity(&build);
        validation_errors.extend(measurement_design_validation_errors(runtime_bridge));
        if runtime_source.git_commit_sha.as_deref() != Some(build.git_commit_sha.as_str()) {
            validation_errors
                .push("verification checkout SHA does not match the binary build SHA".to_owned());
        }
        if runtime_source.git_dirty != Some(false) {
            validation_errors.push("verification checkout is dirty or unavailable".to_owned());
        }
        if binary_sha256.is_none() {
            validation_errors.push("benchmark binary SHA-256 could not be computed".to_owned());
        }
        if binary_size_bytes.is_none() || binary_modified_unix_ts.is_none() {
            validation_errors.push("benchmark binary metadata is incomplete".to_owned());
        }
        if cfg!(unix) && (binary_device_id.is_none() || binary_inode.is_none()) {
            validation_errors
                .push("benchmark binary device/inode identity is incomplete".to_owned());
        }
        if !cfg!(unix) {
            validation_errors.push(
                "citable benchmark artifacts currently require Unix device/inode identity"
                    .to_owned(),
            );
        }
        if cargo_lock_sha256.is_none() {
            validation_errors.push("Cargo.lock SHA-256 could not be computed".to_owned());
        }
        if cargo_feature_graph.is_none() {
            validation_errors.push(
                "runtime-resolved dependency graph could not be reconstructed from the verification checkout"
                    .to_owned(),
            );
        }
        if tracing.statement_debug_enabled
            || tracing.statement_reuse_info_enabled
            || tracing.fallback_decision_debug_enabled
        {
            validation_errors.push(
                "statement, statement-reuse, or fallback-decision tracing changes timed execution"
                    .to_owned(),
            );
        }
        if environment_flag_enabled("FSQLITE_BENCH_LAB_UNSAFE") {
            validation_errors.push(
                "FSQLITE_BENCH_LAB_UNSAFE is diagnostic-only and cannot produce a citable artifact"
                    .to_owned(),
            );
        }
        if !durability.verified || !durability.matched {
            validation_errors.extend(
                durability
                    .validation_errors
                    .iter()
                    .map(|error| format!("durability certification: {error}")),
            );
            if durability.validation_errors.is_empty() {
                validation_errors.push(
                    "effective durability settings were not fully verified and matched".to_owned(),
                );
            }
        }
        validation_errors.extend(
            execution_routing
                .probe_errors
                .iter()
                .map(|error| format!("execution-routing probe: {error}")),
        );
        let citable = validation_errors.is_empty();

        Self {
            schema_version: BENCHMARK_PROVENANCE_SCHEMA_V3.to_owned(),
            citable,
            status: if citable {
                "verified_citable".to_owned()
            } else {
                "unverified".to_owned()
            },
            validation_errors,
            build,
            runtime_source,
            working_directory: std::env::current_dir()
                .ok()
                .map(|path| path.to_string_lossy().into_owned()),
            binary_path,
            binary_sha256,
            binary_size_bytes,
            binary_modified_unix_ts,
            binary_device_id,
            binary_inode,
            cargo_lock_sha256,
            cargo_feature_graph_sha256,
            cargo_feature_graph,
            cargo_feature_graph_command,
            command_line,
            benchmark_environment,
            cpu_affinity: cpu_affinity(),
            runtime_bridge: runtime_bridge.to_owned(),
            tracing,
            durability,
            execution_routing,
        }
    }

    fn verify_runtime_source_unchanged(&mut self) {
        let verification_path = std::path::Path::new(&self.runtime_source.verification_root);
        let current_sha = git_stdout_at(verification_path, &["rev-parse", "--verify", "HEAD"]);
        let current_dirty = git_dirty_at(verification_path);
        if current_sha != self.runtime_source.git_commit_sha {
            self.validation_errors.push(
                "verification checkout SHA changed while the benchmark was running".to_owned(),
            );
        }
        if current_dirty != self.runtime_source.git_dirty || current_dirty != Some(false) {
            self.validation_errors
                .push("verification checkout dirty state changed or is no longer clean".to_owned());
        }
        match std::env::current_exe() {
            Ok(current_binary) => {
                let current_binary_path = current_binary.to_string_lossy();
                if self.binary_path.as_deref() != Some(current_binary_path.as_ref()) {
                    self.validation_errors.push(format!(
                        "benchmark executable path changed while running: start={:?}, end={current_binary_path}",
                        self.binary_path
                    ));
                }
                match sha256_file(&current_binary) {
                    Ok(current_sha256)
                        if self.binary_sha256.as_deref() == Some(current_sha256.as_str()) => {}
                    Ok(current_sha256) => self.validation_errors.push(format!(
                        "benchmark executable SHA-256 changed while running: start={:?}, end={current_sha256}",
                        self.binary_sha256
                    )),
                    Err(error) => self.validation_errors.push(format!(
                        "benchmark executable could not be rehashed after measurement: {error}"
                    )),
                }
                match std::fs::metadata(&current_binary) {
                    Ok(metadata) => {
                        let (device_id, inode) = binary_file_identity(&metadata);
                        if self.binary_size_bytes != Some(metadata.len())
                            || self.binary_device_id != device_id
                            || self.binary_inode != inode
                        {
                            self.validation_errors.push(format!(
                                "benchmark executable file identity changed while running: start=(size={:?}, device={:?}, inode={:?}), end=(size={}, device={device_id:?}, inode={inode:?})",
                                self.binary_size_bytes,
                                self.binary_device_id,
                                self.binary_inode,
                                metadata.len()
                            ));
                        }
                    }
                    Err(error) => self.validation_errors.push(format!(
                        "benchmark executable metadata could not be recaptured after measurement: {error}"
                    )),
                }
            }
            Err(error) => self.validation_errors.push(format!(
                "benchmark executable path could not be recaptured after measurement: {error}"
            )),
        }
        match self.build.verbose_build_log_path.as_deref() {
            Some(path) => {
                let path = std::path::Path::new(path);
                match sha256_file(path) {
                    Ok(current_sha256)
                        if self.build.verbose_build_log_sha256.as_deref()
                            == Some(current_sha256.as_str()) => {}
                    Ok(current_sha256) => self.validation_errors.push(format!(
                        "verbose build log SHA-256 changed while running: start={:?}, end={current_sha256}",
                        self.build.verbose_build_log_sha256
                    )),
                    Err(error) => self.validation_errors.push(format!(
                        "verbose build log could not be rehashed after measurement: {error}"
                    )),
                }
                match std::fs::metadata(path) {
                    Ok(metadata)
                        if self.build.verbose_build_log_size_bytes == Some(metadata.len()) => {}
                    Ok(metadata) => self.validation_errors.push(format!(
                        "verbose build log size changed while running: start={:?}, end={}",
                        self.build.verbose_build_log_size_bytes,
                        metadata.len()
                    )),
                    Err(error) => self.validation_errors.push(format!(
                        "verbose build log metadata could not be recaptured: {error}"
                    )),
                }
            }
            None => self
                .validation_errors
                .push("verbose build log path was absent at end of measurement".to_owned()),
        }
        self.refresh_validation_status();
    }

    #[cfg(feature = "bridge-experiment")]
    fn add_validation_error(&mut self, error: impl Into<String>) {
        self.validation_errors.push(error.into());
        self.refresh_validation_status();
    }

    fn refresh_validation_status(&mut self) {
        self.validation_errors.sort();
        self.validation_errors.dedup();
        self.citable = self.validation_errors.is_empty();
        self.status = if self.citable {
            "verified_citable".to_owned()
        } else {
            "unverified".to_owned()
        };
    }

    fn mark_explicit_override(&mut self) {
        if !self.citable {
            "unverified_explicit_override".clone_into(&mut self.status);
        }
    }
}

impl DetectedEnvironment {
    fn detect(provenance: &JsonBenchmarkProvenance) -> Self {
        fn command_stdout(program: &str, args: &[&str]) -> Option<String> {
            std::process::Command::new(program)
                .args(args)
                .output()
                .ok()
                .filter(|output| output.status.success())
                .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_owned())
                .filter(|stdout| !stdout.is_empty())
        }

        fn system_time_unix_secs(time: SystemTime) -> Option<u64> {
            time.duration_since(std::time::UNIX_EPOCH)
                .ok()
                .map(|duration| duration.as_secs())
        }

        let os = std::fs::read_to_string("/etc/os-release")
            .ok()
            .and_then(|os_release| {
                os_release.lines().find_map(|line| {
                    line.strip_prefix("PRETTY_NAME=")
                        .map(|pretty| pretty.trim_matches('"').to_owned())
                })
            });

        let (cpu_model, cpu_cores) =
            std::fs::read_to_string("/proc/cpuinfo")
                .ok()
                .map_or((None, None), |cpuinfo| {
                    let mut model = None;
                    let mut count = 0_usize;
                    for line in cpuinfo.lines() {
                        if line.starts_with("model name") {
                            if model.is_none() {
                                model = line.split(':').nth(1).map(|part| part.trim().to_owned());
                            }
                            count += 1;
                        }
                    }
                    (model, (count > 0).then_some(count))
                });

        let kernel_release = std::fs::read_to_string("/proc/sys/kernel/osrelease")
            .ok()
            .map(|release| release.trim().to_owned())
            .filter(|release| !release.is_empty());

        let ram_gb = std::fs::read_to_string("/proc/meminfo")
            .ok()
            .and_then(|meminfo| {
                meminfo.lines().find_map(|line| {
                    if !line.starts_with("MemTotal:") {
                        return None;
                    }
                    let kb_str: String = line.chars().filter(char::is_ascii_digit).collect();
                    kb_str.parse::<u64>().ok().map(|kb| kb as f64 / 1_048_576.0)
                })
            });

        let active_toolchain =
            command_stdout("rustup", &["show", "active-toolchain"]).or_else(|| {
                std::env::var("RUSTUP_TOOLCHAIN")
                    .ok()
                    .filter(|toolchain| !toolchain.is_empty())
            });
        let rust_version = command_stdout("rustc", &["--version"]);
        let cargo_version = command_stdout("cargo", &["--version"]);
        let git_commit_sha = Some(provenance.build.git_commit_sha.clone());
        let git_branch = Some(provenance.build.git_branch.clone());
        let git_head_unix_ts = git_stdout_at(
            std::path::Path::new(&provenance.runtime_source.verification_root),
            &["show", "-s", "--format=%ct", "HEAD"],
        )
        .and_then(|timestamp| timestamp.parse::<u64>().ok());
        let git_dirty = provenance.runtime_source.git_dirty;
        let benchmark_binary_modified_unix_ts = std::env::current_exe()
            .ok()
            .and_then(|exe| std::fs::metadata(exe).ok())
            .and_then(|metadata| metadata.modified().ok())
            .and_then(system_time_unix_secs);
        let benchmark_binary_older_than_git_head =
            match (benchmark_binary_modified_unix_ts, git_head_unix_ts) {
                (Some(binary_modified), Some(head_timestamp)) => {
                    Some(binary_modified < head_timestamp)
                }
                _ => None,
            };

        Self {
            os,
            arch: std::env::consts::ARCH.to_owned(),
            kernel_release,
            cpu_model,
            cpu_cores,
            ram_gb,
            active_toolchain,
            rust_version,
            cargo_version,
            git_commit_sha,
            git_branch,
            git_head_unix_ts,
            git_dirty,
            benchmark_binary_modified_unix_ts,
            benchmark_binary_older_than_git_head,
            build_profile: provenance.build.declared_profile.clone(),
        }
    }

    fn print(&self, to_stdout: bool) {
        if let Some(os) = &self.os {
            emit_line(to_stdout, format!("  OS: {os}"));
        }
        emit_line(to_stdout, format!("  Arch: {}", self.arch));
        if let Some(kernel_release) = &self.kernel_release {
            emit_line(to_stdout, format!("  Kernel: {kernel_release}"));
        }
        if let Some(cpu_model) = &self.cpu_model {
            match self.cpu_cores {
                Some(cpu_cores) => {
                    emit_line(to_stdout, format!("  CPU: {cpu_model} ({cpu_cores} cores)"));
                }
                None => emit_line(to_stdout, format!("  CPU: {cpu_model}")),
            }
        }
        if let Some(ram_gb) = self.ram_gb {
            emit_line(to_stdout, format!("  RAM: {ram_gb:.1} GB"));
        }
        if let Some(active_toolchain) = &self.active_toolchain {
            emit_line(to_stdout, format!("  Toolchain: {active_toolchain}"));
        }
        if let Some(rust_version) = &self.rust_version {
            emit_line(to_stdout, format!("  Rust: {rust_version}"));
        }
        if let Some(cargo_version) = &self.cargo_version {
            emit_line(to_stdout, format!("  Cargo: {cargo_version}"));
        }
        if let Some(git_commit_sha) = &self.git_commit_sha {
            match &self.git_branch {
                Some(git_branch) => {
                    emit_line(to_stdout, format!("  Git: {git_branch} @ {git_commit_sha}"));
                }
                None => emit_line(to_stdout, format!("  Git: {git_commit_sha}")),
            }
        }
        if self.git_dirty == Some(true) {
            emit_line(to_stdout, "  Git dirty: yes");
        }
        if let Some(modified) = self.benchmark_binary_modified_unix_ts {
            emit_line(
                to_stdout,
                format!("  Binary modified: {}", format_unix_utc(modified)),
            );
        }
        if self.benchmark_binary_older_than_git_head == Some(true) {
            emit_line(
                to_stdout,
                "  Warning: benchmark binary predates Git HEAD; rebuild before trusting results",
            );
        }
        emit_line(
            to_stdout,
            format!("  Build profile: {}", self.build_profile),
        );
    }
}

fn build_json_report(
    report: &BenchReport,
    total_elapsed: Duration,
    config: JsonRunConfig,
    environment: DetectedEnvironment,
    provenance: JsonBenchmarkProvenance,
) -> JsonBenchmarkReport {
    let summary = compute_report_summary(report);
    let sections = report
        .sections
        .iter()
        .map(|section| {
            let section_id = stable_slug(&section.title);
            let rows = section
                .rows
                .iter()
                .map(|row| JsonRow {
                    scenario_id: format!("{}__{}", section_id, stable_slug(&row.scenario)),
                    scenario: row.scenario.clone(),
                    category: categorize_scenario(&section.title, &row.scenario)
                        .id()
                        .to_owned(),
                    csqlite: row.csqlite.as_ref().map(JsonMeasurement::from_measurement),
                    fsqlite: row.fsqlite.as_ref().map(JsonMeasurement::from_measurement),
                    ratio_fsqlite_over_csqlite: row_ratio(row),
                    fsqlite_concurrent_profile: row.fsqlite_concurrent_profile.clone(),
                    concurrent_readiness: row.concurrent_readiness.clone(),
                })
                .collect();
            JsonSection {
                section_id,
                title: section.title.clone(),
                description: section.description.clone(),
                rows,
            }
        })
        .collect();

    JsonBenchmarkReport {
        schema_version: JSON_REPORT_SCHEMA_V6.to_owned(),
        generated_at_utc: chrono_stamp(),
        total_elapsed_ms: u64::try_from(total_elapsed.as_millis()).unwrap_or(u64::MAX),
        config,
        environment,
        ci_regression_gate: build_ci_regression_gate(report, &summary, &provenance),
        provenance,
        summary,
        sections,
    }
}

#[allow(clippy::too_many_lines)]
fn benchmark_json_schema() -> serde_json::Value {
    serde_json::json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": "https://frankensqlite.dev/schemas/fsqlite-e2e/comprehensive-bench-report.v6.json",
        "title": "FrankenSQLite comprehensive benchmark JSON report",
        "type": "object",
        "additionalProperties": false,
        "required": [
            "schema_version",
            "generated_at_utc",
            "total_elapsed_ms",
            "config",
            "environment",
            "provenance",
            "summary",
            "ci_regression_gate",
            "sections"
        ],
        "properties": {
            "schema_version": {
                "const": JSON_REPORT_SCHEMA_V6
            },
            "generated_at_utc": {
                "type": "string"
            },
            "total_elapsed_ms": {
                "type": "integer",
                "minimum": 0
            },
            "config": {
                "type": "object",
                "additionalProperties": false,
                "required": [
                    "quick", "filter", "warmup_iterations", "min_iterations",
                    "max_iterations", "target_duration_secs", "row_counts",
                    "html_output_path", "json_output_path", "json_stdout"
                ],
                "properties": {
                    "quick": {"type": "boolean"},
                    "filter": {"type": ["string", "null"]},
                    "warmup_iterations": {"type": "integer", "minimum": 0},
                    "min_iterations": {"type": "integer", "minimum": 1},
                    "max_iterations": {"type": "integer", "minimum": 1},
                    "target_duration_secs": {"type": "integer", "minimum": 0},
                    "row_counts": {
                        "type": "array",
                        "items": {"type": "integer", "minimum": 1}
                    },
                    "html_output_path": {"type": ["string", "null"]},
                    "json_output_path": {"type": ["string", "null"]},
                    "json_stdout": {"type": "boolean"}
                }
            },
            "environment": {
                "type": "object",
                "additionalProperties": false,
                "required": [
                    "os", "arch", "kernel_release", "cpu_model", "cpu_cores",
                    "ram_gb", "active_toolchain", "rust_version", "cargo_version",
                    "git_commit_sha", "git_branch", "git_head_unix_ts", "git_dirty",
                    "benchmark_binary_modified_unix_ts",
                    "benchmark_binary_older_than_git_head", "build_profile"
                ],
                "properties": {
                    "os": {"type": ["string", "null"]},
                    "arch": {"type": "string"},
                    "kernel_release": {"type": ["string", "null"]},
                    "cpu_model": {"type": ["string", "null"]},
                    "cpu_cores": {"type": ["integer", "null"], "minimum": 1},
                    "ram_gb": {"type": ["number", "null"]},
                    "active_toolchain": {"type": ["string", "null"]},
                    "rust_version": {"type": ["string", "null"]},
                    "cargo_version": {"type": ["string", "null"]},
                    "git_commit_sha": {"type": ["string", "null"]},
                    "git_branch": {"type": ["string", "null"]},
                    "git_head_unix_ts": {"type": ["integer", "null"]},
                    "git_dirty": {"type": ["boolean", "null"]},
                    "benchmark_binary_modified_unix_ts": {"type": ["integer", "null"]},
                    "benchmark_binary_older_than_git_head": {"type": ["boolean", "null"]},
                    "build_profile": {"type": "string"}
                }
            },
            "provenance": {
                "type": "object",
                "additionalProperties": false,
                "required": [
                    "schema_version",
                    "citable",
                    "status",
                    "validation_errors",
                    "build",
                    "runtime_source",
                    "working_directory",
                    "binary_path",
                    "binary_sha256",
                    "binary_size_bytes",
                    "binary_modified_unix_ts",
                    "binary_device_id",
                    "binary_inode",
                    "cargo_lock_sha256",
                    "cargo_feature_graph_sha256",
                    "cargo_feature_graph",
                    "cargo_feature_graph_command",
                    "command_line",
                    "benchmark_environment",
                    "cpu_affinity",
                    "runtime_bridge",
                    "tracing",
                    "durability",
                    "execution_routing"
                ],
                "properties": {
                    "schema_version": {"const": BENCHMARK_PROVENANCE_SCHEMA_V3},
                    "citable": {"const": false},
                    "status": {
                        "enum": [
                            "unverified",
                            "unverified_explicit_override"
                        ]
                    },
                    "validation_errors": {
                        "type": "array",
                        "minItems": 1,
                        "items": {"type": "string"}
                    },
                    "build": {
                        "type": "object",
                        "additionalProperties": false,
                        "required": [
                            "workspace_root", "git_commit_sha", "git_branch", "git_dirty",
                            "tracked_workspace_inputs_watched", "cargo_profile_family",
                            "selected_profile", "declared_profile", "opt_level",
                            "build_nonce", "debuginfo", "debug_assertions", "target", "host",
                            "panic_strategy", "panic_abort", "package_features",
                            "encoded_rustflags_hex", "encoded_rustflags_present",
                            "profile_override_environment_hex",
                            "native_override_environment_hex",
                            "verbose_build_log_path", "verbose_build_log_sha256",
                            "verbose_build_log_size_bytes",
                            "verbose_build_log_verified",
                            "profile_proof_scope", "rustc_version", "cargo_version"
                        ],
                        "properties": {
                            "workspace_root": {"type": "string"},
                            "git_commit_sha": {"type": "string"},
                            "git_branch": {"type": "string"},
                            "git_dirty": {"type": ["boolean", "null"]},
                            "tracked_workspace_inputs_watched": {"type": "string"},
                            "cargo_profile_family": {"type": "string"},
                            "selected_profile": {"type": "string"},
                            "declared_profile": {"type": "string"},
                            "build_nonce": {
                                "type": "string",
                                "minLength": 1
                            },
                            "opt_level": {"type": "string"},
                            "debuginfo": {"type": "string"},
                            "debug_assertions": {"type": "boolean"},
                            "target": {"type": "string"},
                            "host": {"type": "string"},
                            "panic_strategy": {"type": "string"},
                            "panic_abort": {"type": "boolean"},
                            "package_features": {
                                "type": "array",
                                "items": {"type": "string"}
                            },
                            "encoded_rustflags_hex": {
                                "type": "string",
                                "pattern": "^[0-9a-f]*$"
                            },
                            "encoded_rustflags_present": {"type": "boolean"},
                            "profile_override_environment_hex": {
                                "type": "string",
                                "pattern": "^[0-9a-f]*$"
                            },
                            "native_override_environment_hex": {
                                "type": "string",
                                "pattern": "^[0-9a-f]*$"
                            },
                            "verbose_build_log_path": {"type": ["string", "null"]},
                            "verbose_build_log_sha256": {
                                "type": ["string", "null"],
                                "pattern": "^[0-9a-f]{64}$"
                            },
                            "verbose_build_log_size_bytes": {
                                "type": ["integer", "null"],
                                "minimum": 0
                            },
                            "verbose_build_log_verified": {
                                "type": "boolean"
                            },
                            "profile_proof_scope": {"type": "string"},
                            "rustc_version": {"type": "string"},
                            "cargo_version": {"type": "string"}
                        }
                    },
                    "runtime_source": {
                        "type": "object",
                        "additionalProperties": false,
                        "required": [
                            "verification_root", "git_commit_sha", "git_branch", "git_dirty"
                        ],
                        "properties": {
                            "verification_root": {"type": "string"},
                            "git_commit_sha": {"type": ["string", "null"]},
                            "git_branch": {"type": ["string", "null"]},
                            "git_dirty": {"type": ["boolean", "null"]}
                        }
                    },
                    "working_directory": {"type": ["string", "null"]},
                    "binary_path": {"type": ["string", "null"]},
                    "binary_sha256": {"type": ["string", "null"]},
                    "binary_size_bytes": {"type": ["integer", "null"]},
                    "binary_modified_unix_ts": {"type": ["integer", "null"]},
                    "binary_device_id": {"type": ["integer", "null"]},
                    "binary_inode": {"type": ["integer", "null"]},
                    "cargo_lock_sha256": {"type": ["string", "null"]},
                    "cargo_feature_graph_sha256": {"type": ["string", "null"]},
                    "cargo_feature_graph": {"type": ["string", "null"]},
                    "cargo_feature_graph_command": {"type": "string"},
                    "command_line": {
                        "type": "array",
                        "items": {"type": "string"}
                    },
                    "benchmark_environment": {
                        "type": "object",
                        "additionalProperties": {"type": "string"}
                    },
                    "cpu_affinity": {"type": ["string", "null"]},
                    "runtime_bridge": {
                        "const": "scenario_scoped_thread_local_block_on"
                    },
                    "tracing": {
                        "type": "object",
                        "additionalProperties": false,
                        "required": [
                            "rust_log", "statement_debug_enabled",
                            "statement_reuse_info_enabled",
                            "fallback_decision_debug_enabled"
                        ],
                        "properties": {
                            "rust_log": {"type": ["string", "null"]},
                            "statement_debug_enabled": {"type": "boolean"},
                            "statement_reuse_info_enabled": {"type": "boolean"},
                            "fallback_decision_debug_enabled": {"type": "boolean"}
                        }
                    },
                    "durability": {
                        "type": "object",
                        "additionalProperties": false,
                        "required": [
                            "page_size_bytes", "default_synchronous",
                            "concurrent_synchronous_modes", "csqlite_pragmas",
                            "fsqlite_pragmas", "concurrent_mode_default", "verified",
                            "matched", "validation_errors", "effective_profiles"
                        ],
                        "properties": {
                            "page_size_bytes": {"type": "integer", "minimum": 512},
                            "default_synchronous": {"type": "string"},
                            "concurrent_synchronous_modes": {
                                "type": "array",
                                "items": {"type": "string"}
                            },
                            "csqlite_pragmas": {
                                "type": "array",
                                "items": {"type": "string"}
                            },
                            "fsqlite_pragmas": {
                                "type": "array",
                                "items": {"type": "string"}
                            },
                            "concurrent_mode_default": {"type": "boolean"},
                            "verified": {"type": "boolean"},
                            "matched": {"type": "boolean"},
                            "validation_errors": {
                                "type": "array",
                                "items": {"type": "string"}
                            },
                            "effective_profiles": {
                                "type": "object",
                                "additionalProperties": {
                                    "type": "object",
                                    "additionalProperties": {"type": "string"}
                                }
                            }
                        }
                    },
                    "execution_routing": {
                        "type": "object",
                        "additionalProperties": false,
                        "required": [
                            "probe_scope", "timed_routes_verified", "limitations",
                            "timed_execution_instrumented",
                            "parser_fast_path_executions", "parser_slow_path_executions",
                            "prepared_insert_fast_lane_hits",
                            "prepared_insert_instrumented_lane_hits",
                            "prepared_direct_insert_executions",
                            "prepared_update_delete_fast_lane_hits",
                            "prepared_update_delete_instrumented_lane_hits",
                            "prepared_direct_update_executions",
                            "prepared_direct_delete_executions",
                            "prepared_update_delete_dml_direct_handoff_runs",
                            "prepared_table_dml_affected_only_runs",
                            "prepared_dml_fallbacks", "select_routing_decisions",
                            "probe_errors"
                        ],
                        "properties": {
                            "probe_scope": {"type": "string"},
                            "timed_routes_verified": {"type": "boolean"},
                            "limitations": {
                                "type": "array",
                                "items": {"type": "string"}
                            },
                            "timed_execution_instrumented": {"type": "boolean"},
                            "parser_fast_path_executions": {"type": "integer", "minimum": 0},
                            "parser_slow_path_executions": {"type": "integer", "minimum": 0},
                            "prepared_insert_fast_lane_hits": {"type": "integer", "minimum": 0},
                            "prepared_insert_instrumented_lane_hits": {"type": "integer", "minimum": 0},
                            "prepared_direct_insert_executions": {"type": "integer", "minimum": 0},
                            "prepared_update_delete_fast_lane_hits": {"type": "integer", "minimum": 0},
                            "prepared_update_delete_instrumented_lane_hits": {"type": "integer", "minimum": 0},
                            "prepared_direct_update_executions": {"type": "integer", "minimum": 0},
                            "prepared_direct_delete_executions": {"type": "integer", "minimum": 0},
                            "prepared_update_delete_dml_direct_handoff_runs": {
                                "type": "integer",
                                "minimum": 0
                            },
                            "prepared_table_dml_affected_only_runs": {
                                "type": "integer",
                                "minimum": 0
                            },
                            "prepared_dml_fallbacks": {
                                "type": "object",
                                "additionalProperties": {"type": "integer", "minimum": 0}
                            },
                            "select_routing_decisions": {
                                "type": "object",
                                "additionalProperties": {"type": "integer", "minimum": 0}
                            },
                            "probe_errors": {
                                "type": "array",
                                "items": {"type": "string"}
                            }
                        }
                    }
                },
                "allOf": [
                    {
                        "if": {
                            "properties": {
                                "runtime_bridge": {
                                    "const": "scenario_scoped_thread_local_block_on"
                                }
                            },
                            "required": ["runtime_bridge"]
                        },
                        "then": {
                            "properties": {
                                "citable": {"const": false},
                                "status": {
                                    "enum": ["unverified", "unverified_explicit_override"]
                                }
                            }
                        }
                    },
                    {
                        "if": {
                            "properties": {"citable": {"const": true}},
                            "required": ["citable"]
                        },
                        "then": {
                            "properties": {
                                "status": {"const": "verified_citable"},
                                "validation_errors": {"maxItems": 0},
                                "build": {
                                    "properties": {
                                        "git_dirty": {"const": false},
                                        "git_commit_sha": {
                                            "pattern": "^[0-9a-f]{40}$"
                                        },
                                        "tracked_workspace_inputs_watched": {"const": "complete"},
                                        "cargo_profile_family": {"const": "release"},
                                        "declared_profile": {"enum": ["release", "release-perf"]},
                                        "build_nonce": {
                                            "type": "string",
                                            "pattern": "^[0-9a-f]{64}$"
                                        },
                                        "debug_assertions": {"const": false},
                                        "panic_strategy": {"const": "abort"},
                                        "panic_abort": {"const": true},
                                        "encoded_rustflags_hex": {"const": ""},
                                        "encoded_rustflags_present": {"const": true},
                                        "native_override_environment_hex": {
                                            "const": "4c494253514c495445335f464c4147533d2d4453514c4954455f454e41424c455f4d4154485f46554e4354494f4e53"
                                        },
                                        "profile_override_environment_hex": {
                                            "type": "string",
                                            "minLength": 2
                                        },
                                        "verbose_build_log_path": {
                                            "type": "string",
                                            "minLength": 1
                                        },
                                        "verbose_build_log_sha256": {
                                            "type": "string",
                                            "pattern": "^[0-9a-f]{64}$"
                                        },
                                        "verbose_build_log_size_bytes": {
                                            "type": "integer",
                                            "minimum": 1
                                        },
                                        "verbose_build_log_verified": {
                                            "const": true
                                        }
                                    }
                                },
                                "runtime_source": {
                                    "properties": {
                                        "git_commit_sha": {
                                            "type": "string",
                                            "pattern": "^[0-9a-f]{40}$"
                                        },
                                        "git_dirty": {"const": false}
                                    }
                                },
                                "binary_path": {"type": "string", "minLength": 1},
                                "binary_sha256": {
                                    "type": "string",
                                    "pattern": "^[0-9a-f]{64}$"
                                },
                                "binary_size_bytes": {"type": "integer", "minimum": 1},
                                "binary_modified_unix_ts": {"type": "integer", "minimum": 1},
                                "binary_device_id": {"type": "integer", "minimum": 0},
                                "binary_inode": {"type": "integer", "minimum": 1},
                                "cargo_lock_sha256": {
                                    "type": "string",
                                    "pattern": "^[0-9a-f]{64}$"
                                },
                                "cargo_feature_graph_sha256": {
                                    "type": "string",
                                    "pattern": "^[0-9a-f]{64}$"
                                },
                                "cargo_feature_graph": {"type": "string", "minLength": 1},
                                "tracing": {
                                    "properties": {
                                        "statement_debug_enabled": {"const": false},
                                        "statement_reuse_info_enabled": {"const": false},
                                        "fallback_decision_debug_enabled": {"const": false}
                                    }
                                },
                                "durability": {
                                    "properties": {
                                        "concurrent_mode_default": {"const": true},
                                        "verified": {"const": true},
                                        "matched": {"const": true},
                                        "validation_errors": {"maxItems": 0},
                                        "effective_profiles": {
                                            "required": [
                                                "memory.csqlite", "memory.fsqlite",
                                                "file.csqlite", "file.fsqlite"
                                            ],
                                            "minProperties": 4
                                        }
                                    }
                                },
                                "execution_routing": {
                                    "properties": {
                                        "timed_execution_instrumented": {"const": false},
                                        "probe_errors": {"maxItems": 0}
                                    }
                                }
                            },
                            "allOf": [
                                {
                                    "if": {
                                        "properties": {
                                            "build": {
                                                "properties": {
                                                    "declared_profile": {"const": "release-perf"}
                                                }
                                            }
                                        }
                                    },
                                    "then": {
                                        "properties": {
                                            "build": {
                                                "properties": {
                                                    "selected_profile": {"const": "release-perf"},
                                                    "opt_level": {"const": "3"},
                                                    "debuginfo": {"const": "false"}
                                                }
                                            }
                                        }
                                    }
                                },
                                {
                                    "if": {
                                        "properties": {
                                            "build": {
                                                "properties": {
                                                    "declared_profile": {"const": "release"}
                                                }
                                            }
                                        }
                                    },
                                    "then": {
                                        "properties": {
                                            "build": {
                                                "properties": {
                                                    "selected_profile": {"const": "release"},
                                                    "opt_level": {"const": "z"},
                                                    "debuginfo": {"const": "false"}
                                                }
                                            }
                                        }
                                    }
                                }
                            ]
                        }
                    },
                    {
                        "if": {
                            "properties": {"citable": {"const": false}},
                            "required": ["citable"]
                        },
                        "then": {
                            "properties": {
                                "status": {
                                    "enum": ["unverified", "unverified_explicit_override"]
                                }
                            }
                        }
                    }
                ]
            },
            "summary": {
                "type": "object",
                "additionalProperties": false,
                "required": [
                    "total_scenarios",
                    "franken_faster",
                    "comparable",
                    "csqlite_faster",
                    "avg_ratio",
                    "average_ratio",
                    "geomean_ratio",
                    "median_ratio",
                    "p90_ratio",
                    "p99_ratio",
                    "primary_metric",
                    "per_category",
                    "per_category_weighted"
                ],
                "properties": {
                    "total_scenarios": {"type": "integer", "minimum": 0},
                    "franken_faster": {"type": "integer", "minimum": 0},
                    "comparable": {"type": "integer", "minimum": 0},
                    "csqlite_faster": {"type": "integer", "minimum": 0},
                    "avg_ratio": {
                        "type": ["number", "null"],
                        "description": "Continuity metric only; not the primary score."
                    },
                    "average_ratio": {
                        "type": ["number", "null"],
                        "description": "Backward-compatible alias for avg_ratio; not the primary score."
                    },
                    "geomean_ratio": {"type": ["number", "null"]},
                    "median_ratio": {"type": ["number", "null"]},
                    "p90_ratio": {"type": ["number", "null"]},
                    "p99_ratio": {"type": ["number", "null"]},
                    "primary_metric": {"const": "per_category_weighted.score"},
                    "per_category": {
                        "type": "object",
                        "additionalProperties": false,
                        "required": ["read_single", "read_aggregate", "write_single", "write_bulk", "concurrent_writers", "mixed"],
                        "properties": {
                            "read_single": {"$ref": "#/$defs/category_stats"},
                            "read_aggregate": {"$ref": "#/$defs/category_stats"},
                            "write_single": {"$ref": "#/$defs/category_stats"},
                            "write_bulk": {"$ref": "#/$defs/category_stats"},
                            "concurrent_writers": {"$ref": "#/$defs/category_stats"},
                            "mixed": {"$ref": "#/$defs/category_stats"}
                        }
                    },
                    "per_category_weighted": {"$ref": "#/$defs/weighted_category_score"}
                }
            },
            "ci_regression_gate": {
                "type": "object",
                "additionalProperties": false,
                "required": [
                    "schema_version", "bead_id", "depends_on_bead_id", "status",
                    "eligible", "ineligibility_reasons", "evaluation_result",
                    "thresholds", "observed"
                ],
                "properties": {
                    "schema_version": {"const": CI_REGRESSION_GATE_SCHEMA_V2},
                    "bead_id": {"const": CI_REGRESSION_GATE_BEAD_ID},
                    "depends_on_bead_id": {"const": CI_REGRESSION_BASELINE_BEAD_ID},
                    "status": {
                        "enum": ["eligible_compatible_baseline_required", "ineligible"]
                    },
                    "eligible": {"type": "boolean"},
                    "ineligibility_reasons": {
                        "type": "array",
                        "items": {"type": "string"}
                    },
                    "evaluation_result": {"const": "not_evaluated"},
                    "thresholds": {
                        "type": "object",
                        "additionalProperties": false,
                        "required": [
                            "avg_ratio_baseline",
                            "avg_ratio_max",
                            "mt_p95_ratio_max",
                            "primary_score_max_regression_pct",
                            "geomean_max_regression_pct",
                            "per_category_geomean_max_regression_pct",
                            "p90_max_regression_pct",
                            "threshold_source"
                        ],
                        "properties": {
                            "avg_ratio_baseline": {"type": "number"},
                            "avg_ratio_max": {"type": ["number", "null"]},
                            "mt_p95_ratio_max": {"type": ["number", "null"]},
                            "primary_score_max_regression_pct": {"type": "number"},
                            "geomean_max_regression_pct": {"type": "number"},
                            "per_category_geomean_max_regression_pct": {"type": "number"},
                            "p90_max_regression_pct": {"type": "number"},
                            "threshold_source": {"type": "string"}
                        }
                    },
                    "observed": {
                        "type": "object",
                        "additionalProperties": false,
                        "required": [
                            "avg_ratio",
                            "primary_score",
                            "geomean_ratio",
                            "median_ratio",
                            "p90_ratio",
                            "max_mt_p95_ratio",
                            "max_mt_p95_scenario_id"
                        ],
                        "properties": {
                            "avg_ratio": {"type": ["number", "null"]},
                            "primary_score": {"type": ["number", "null"]},
                            "geomean_ratio": {"type": ["number", "null"]},
                            "median_ratio": {"type": ["number", "null"]},
                            "p90_ratio": {"type": ["number", "null"]},
                            "max_mt_p95_ratio": {
                                "type": ["number", "null"],
                                "description": "Worst fsqlite/csqlite p95 latency ratio among multithreaded concurrent-writer rows."
                            },
                            "max_mt_p95_scenario_id": {"type": ["string", "null"]}
                        }
                    }
                }
            },
            "sections": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["section_id", "title", "description", "rows"],
                    "properties": {
                        "section_id": {"type": "string"},
                        "title": {"type": "string"},
                        "description": {"type": "string"},
                        "rows": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "additionalProperties": false,
                                "required": ["scenario_id", "scenario", "category", "csqlite", "fsqlite", "ratio_fsqlite_over_csqlite"],
                                "properties": {
                                    "scenario_id": {"type": "string"},
                                    "scenario": {"type": "string"},
                                    "category": {"$ref": "#/$defs/scenario_category"},
                                    "csqlite": {"anyOf": [{"$ref": "#/$defs/measurement"}, {"type": "null"}]},
                                    "fsqlite": {"anyOf": [{"$ref": "#/$defs/measurement"}, {"type": "null"}]},
                                    "ratio_fsqlite_over_csqlite": {"type": ["number", "null"]},
                                    "fsqlite_concurrent_profile": {"$ref": "#/$defs/fsqlite_concurrent_profile"},
                                    "concurrent_readiness": {"$ref": "#/$defs/concurrent_readiness"}
                                }
                            }
                        }
                    }
                }
            }
        },
        "$defs": {
            "scenario_category": {
                "type": "string",
                "enum": ["read_single", "read_aggregate", "write_single", "write_bulk", "concurrent_writers", "mixed"]
            },
            "category_stats": {
                "type": "object",
                "additionalProperties": false,
                "required": ["n", "avg_ratio", "geomean_ratio", "median_ratio", "p90_ratio", "p99_ratio"],
                "properties": {
                    "n": {"type": "integer", "minimum": 0},
                    "avg_ratio": {"type": ["number", "null"]},
                    "geomean_ratio": {"type": ["number", "null"]},
                    "median_ratio": {"type": ["number", "null"]},
                    "p90_ratio": {"type": ["number", "null"]},
                    "p99_ratio": {"type": ["number", "null"]}
                }
            },
            "weighted_category_score": {
                "type": "object",
                "additionalProperties": false,
                "required": ["primary", "score", "weights", "observed_weight", "missing_categories"],
                "properties": {
                    "primary": {"const": true},
                    "score": {"type": ["number", "null"]},
                    "weights": {
                        "type": "object",
                        "additionalProperties": false,
                        "required": ["read_single", "read_aggregate", "write_single", "write_bulk", "concurrent_writers", "mixed"],
                        "properties": {
                            "read_single": {"type": "number"},
                            "read_aggregate": {"type": "number"},
                            "write_single": {"type": "number"},
                            "write_bulk": {"type": "number"},
                            "concurrent_writers": {"type": "number"},
                            "mixed": {"type": "number"}
                        }
                    },
                    "observed_weight": {"type": "number", "minimum": 0},
                    "missing_categories": {
                        "type": "array",
                        "items": {"$ref": "#/$defs/scenario_category"}
                    }
                }
            },
            "measurement": {
                "type": "object",
                "additionalProperties": false,
                "required": ["median_ms", "mean_ms", "min_ms", "p95_ms", "p99_ms", "stddev_ms", "cv_pct", "rows_per_sec", "us_per_row", "iterations"],
                "properties": {
                    "median_ms": {"type": "number", "minimum": 0},
                    "mean_ms": {"type": "number", "minimum": 0},
                    "min_ms": {"type": "number", "minimum": 0},
                    "p95_ms": {"type": "number", "minimum": 0},
                    "p99_ms": {"type": "number", "minimum": 0},
                    "stddev_ms": {"type": "number", "minimum": 0},
                    "cv_pct": {"type": "number", "minimum": 0},
                    "rows_per_sec": {"type": "number", "minimum": 0},
                    "us_per_row": {"type": "number", "minimum": 0},
                    "iterations": {"type": "integer", "minimum": 1}
                }
            },
            "fsqlite_concurrent_profile": {
                "type": "object",
                "additionalProperties": false,
                "required": ["total_rows", "fsqlite_median_ms", "capture_scope", "counters"],
                "properties": {
                    "total_rows": {"type": "integer", "minimum": 0},
                    "fsqlite_median_ms": {"type": "number", "minimum": 0},
                    "capture_scope": {"type": "string"},
                    "counters": {
                        "type": "object",
                        "description": "Stable counter names match the human concurrent_profile line when FSQLITE_BENCH_PROFILE_CONCURRENT=1.",
                        "additionalProperties": {"type": "integer", "minimum": 0}
                    }
                }
            },
            "concurrent_worker_settings": {
                "type": "object",
                "additionalProperties": false,
                "required": [
                    "page_size_bytes", "journal_mode", "synchronous",
                    "cache_size", "busy_timeout_ms", "concurrent_mode"
                ],
                "properties": {
                    "page_size_bytes": {"type": "integer", "minimum": 512},
                    "journal_mode": {"const": "wal"},
                    "synchronous": {"enum": ["normal", "full"]},
                    "cache_size": {"type": "integer"},
                    "busy_timeout_ms": {"type": "integer", "minimum": 1},
                    "concurrent_mode": {
                        "enum": ["sqlite_wal_single_writer", "fsqlite_mvcc_on"]
                    }
                }
            },
            "concurrent_worker_receipt": {
                "type": "object",
                "additionalProperties": false,
                "required": [
                    "worker_index", "setup_thread_id", "postflight_thread_id",
                    "setup_cpu_affinity", "postflight_cpu_affinity",
                    "completed_rows", "settings", "settings_verified",
                    "thread_identity_verified", "thread_affinity_verified"
                ],
                "properties": {
                    "worker_index": {"type": "integer", "minimum": 0},
                    "setup_thread_id": {"type": "string", "minLength": 1},
                    "postflight_thread_id": {"type": "string", "minLength": 1},
                    "setup_cpu_affinity": {"type": "string", "minLength": 1},
                    "postflight_cpu_affinity": {"type": "string", "minLength": 1},
                    "completed_rows": {"type": "integer", "minimum": 1},
                    "settings": {"$ref": "#/$defs/concurrent_worker_settings"},
                    "settings_verified": {"const": true},
                    "thread_identity_verified": {"const": true},
                    "thread_affinity_verified": {"const": true}
                }
            },
            "concurrent_sample_readiness": {
                "type": "object",
                "additionalProperties": false,
                "required": [
                    "phase", "sample_index", "engine", "expected_cpu_affinity",
                    "expected_workers", "expected_rows", "completed_rows",
                    "database_rows", "expected_id_sum", "database_id_sum",
                    "timed_scope", "workers"
                ],
                "properties": {
                    "phase": {"enum": ["warmup", "measured"]},
                    "sample_index": {"type": "integer", "minimum": 0},
                    "engine": {"enum": ["csqlite", "fsqlite"]},
                    "expected_cpu_affinity": {"type": "string", "minLength": 1},
                    "expected_workers": {"type": "integer", "minimum": 1},
                    "expected_rows": {"type": "integer", "minimum": 1},
                    "completed_rows": {"type": "integer", "minimum": 1},
                    "database_rows": {"type": "integer", "minimum": 1},
                    "expected_id_sum": {"type": "integer"},
                    "database_id_sum": {"type": "integer"},
                    "timed_scope": {"type": "string", "minLength": 1},
                    "workers": {
                        "type": "array",
                        "minItems": 1,
                        "items": {"$ref": "#/$defs/concurrent_worker_receipt"}
                    }
                }
            },
            "concurrent_readiness": {
                "type": "object",
                "additionalProperties": false,
                "required": ["csqlite_samples", "fsqlite_samples"],
                "properties": {
                    "csqlite_samples": {
                        "type": "array",
                        "minItems": 1,
                        "items": {"$ref": "#/$defs/concurrent_sample_readiness"}
                    },
                    "fsqlite_samples": {
                        "type": "array",
                        "minItems": 1,
                        "items": {"$ref": "#/$defs/concurrent_sample_readiness"}
                    }
                }
            }
        }
    })
}

#[cfg(feature = "bridge-experiment")]
#[allow(clippy::too_many_lines)]
fn bridge_json_schema() -> serde_json::Value {
    let comprehensive = benchmark_json_schema();
    let mut provenance = comprehensive["properties"]["provenance"].clone();
    provenance["properties"]["runtime_bridge"] = serde_json::json!({
        "const": "three_arm_per_operation_inside_existing_runtime_worker_sync_facade"
    });
    let environment = comprehensive["properties"]["environment"].clone();
    serde_json::json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": "https://frankensqlite.dev/schemas/fsqlite-e2e/bridge-experiment.v2.json",
        "title": "FrankenSQLite async bridge experiment report",
        "type": "object",
        "additionalProperties": false,
        "required": [
            "schema_version", "generated_at_utc", "provenance", "environment",
            "host_state_before", "host_state_checkpoints", "host_state_after",
            "config", "raw_samples",
            "arm_statistics", "paired_comparisons", "ready_runtime_entry_regression"
        ],
        "properties": {
            "schema_version": {"const": BRIDGE_REPORT_SCHEMA_V2},
            "generated_at_utc": {"type": "string"},
            "provenance": {
                "allOf": [
                    provenance,
                    {
                        "properties": {
                            "citable": {"const": false},
                            "status": {
                                "enum": [
                                    "unverified",
                                    "unverified_explicit_override"
                                ]
                            }
                        }
                    }
                ]
            },
            "environment": environment,
            "host_state_before": {"$ref": "#/$defs/host_state"},
            "host_state_checkpoints": {
                "type": "array",
                "minItems": 1,
                "items": {"$ref": "#/$defs/host_state"}
            },
            "host_state_after": {"$ref": "#/$defs/host_state"},
            "config": {"$ref": "#/$defs/config"},
            "raw_samples": {
                "type": "array",
                "minItems": 1,
                "items": {"$ref": "#/$defs/sample"}
            },
            "arm_statistics": {
                "type": "array",
                "minItems": 1,
                "items": {"$ref": "#/$defs/arm_statistics"}
            },
            "paired_comparisons": {
                "type": "array",
                "minItems": 1,
                "items": {"$ref": "#/$defs/paired_comparison"}
            },
            "ready_runtime_entry_regression": {"$ref": "#/$defs/ready_regression"}
        },
        "$defs": {
            "arm": {
                "enum": [
                    "per_operation_block_on",
                    "inside_existing_runtime",
                    "worker_sync_facade"
                ]
            },
            "workload": {
                "enum": [
                    "ready_future",
                    "prepared_insert",
                    "raw_execute_with_params"
                ]
            },
            "host_state": {
                "type": "object",
                "additionalProperties": false,
                "required": [
                    "captured_at_utc", "load_average_1m", "load_average_5m",
                    "load_average_15m", "available_parallelism", "cpu_affinity",
                    "selected_cpu_topology", "scaling_governors",
                    "energy_performance_preferences",
                    "boost_controls", "numa_nodes_online", "memory_available_gb",
                    "cpu_pressure_some_avg10", "io_pressure_some_avg60",
                    "competing_processes", "competing_process_scan_error"
                ],
                "properties": {
                    "captured_at_utc": {"type": "string"},
                    "load_average_1m": {"type": ["number", "null"], "minimum": 0},
                    "load_average_5m": {"type": ["number", "null"], "minimum": 0},
                    "load_average_15m": {"type": ["number", "null"], "minimum": 0},
                    "available_parallelism": {"type": ["integer", "null"], "minimum": 1},
                    "cpu_affinity": {"type": ["string", "null"]},
                    "selected_cpu_topology": {
                        "type": "object",
                        "minProperties": 0,
                        "additionalProperties": {"type": "string", "minLength": 1}
                    },
                    "scaling_governors": {
                        "type": "array",
                        "items": {"type": "string"}
                    },
                    "energy_performance_preferences": {
                        "type": "array",
                        "items": {"type": "string"}
                    },
                    "boost_controls": {
                        "type": "object",
                        "additionalProperties": {"type": "string"}
                    },
                    "numa_nodes_online": {"type": ["string", "null"]},
                    "memory_available_gb": {"type": ["number", "null"], "minimum": 0},
                    "cpu_pressure_some_avg10": {
                        "type": ["number", "null"],
                        "minimum": 0
                    },
                    "io_pressure_some_avg60": {
                        "type": ["number", "null"],
                        "minimum": 0
                    },
                    "competing_processes": {
                        "type": "array",
                        "items": {"type": "string", "minLength": 1}
                    },
                    "competing_process_scan_error": {"type": ["string", "null"]}
                }
            },
            "config": {
                "type": "object",
                "additionalProperties": false,
                "required": [
                    "samples_per_arm", "raw_insert_operations",
                    "ready_operation_counts", "order_seed", "ordering_policy",
                    "warmup_policy", "timed_region", "arm_contracts",
                    "affinity_policy", "max_load_average_1m"
                ],
                "properties": {
                    "samples_per_arm": {
                        "type": "integer",
                        "minimum": 48,
                        "multipleOf": 48
                    },
                    "raw_insert_operations": {"type": "integer", "minimum": 1},
                    "ready_operation_counts": {
                        "type": "array",
                        "minItems": 2,
                        "items": {"type": "integer", "minimum": 1}
                    },
                    "order_seed": {"type": "integer", "minimum": 0},
                    "ordering_policy": {"type": "string"},
                    "warmup_policy": {"type": "string"},
                    "timed_region": {"type": "string"},
                    "arm_contracts": {
                        "type": "object",
                        "additionalProperties": false,
                        "required": [
                            "per_operation_block_on",
                            "inside_existing_runtime",
                            "worker_sync_facade"
                        ],
                        "properties": {
                            "per_operation_block_on": {"type": "string"},
                            "inside_existing_runtime": {"type": "string"},
                            "worker_sync_facade": {"type": "string"}
                        }
                    },
                    "affinity_policy": {"type": "string"},
                    "max_load_average_1m": {
                        "type": ["number", "null"],
                        "minimum": 0,
                        "maximum": 1
                    }
                }
            },
            "sample": {
                "type": "object",
                "additionalProperties": false,
                "required": [
                    "workload", "operation_count", "block_index", "order_slot",
                    "arm", "elapsed_ns", "runtime_entries_total",
                    "runtime_entries_inside_timed_region",
                    "caller_future_completions_inside_timed_region",
                    "engine_dml_future_calls_inside_timed_region",
                    "worker_commands_total", "worker_commands_inside_timed_region",
                    "worker_open_handshakes_total", "effective_settings", "oracle_kind",
                    "checksum_count", "checksum_sum", "checksum_exact_rows"
                ],
                "properties": {
                    "workload": {"$ref": "#/$defs/workload"},
                    "operation_count": {"type": "integer", "minimum": 1},
                    "block_index": {"type": "integer", "minimum": 0},
                    "order_slot": {"type": "integer", "minimum": 0},
                    "arm": {"$ref": "#/$defs/arm"},
                    "elapsed_ns": {"type": "integer", "minimum": 0},
                    "runtime_entries_total": {"type": "integer", "minimum": 0},
                    "runtime_entries_inside_timed_region": {
                        "type": "integer", "minimum": 0
                    },
                    "caller_future_completions_inside_timed_region": {
                        "type": "integer", "minimum": 0
                    },
                    "engine_dml_future_calls_inside_timed_region": {
                        "type": "integer", "minimum": 0
                    },
                    "worker_commands_total": {"type": "integer", "minimum": 0},
                    "worker_commands_inside_timed_region": {
                        "type": "integer", "minimum": 0
                    },
                    "worker_open_handshakes_total": {
                        "type": "integer", "minimum": 0
                    },
                    "effective_settings": {
                        "type": "object",
                        "additionalProperties": {"type": "string"}
                    },
                    "oracle_kind": {"type": "string"},
                    "checksum_count": {"type": "integer"},
                    "checksum_sum": {"type": "integer"},
                    "checksum_exact_rows": {"type": "integer"}
                },
                "allOf": [
                    {
                        "if": {
                            "properties": {"workload": {"const": "ready_future"}},
                            "required": ["workload"]
                        },
                        "then": {
                            "properties": {
                                "effective_settings": {"maxProperties": 0}
                            }
                        },
                        "else": {
                            "properties": {
                                "effective_settings": {
                                    "required": [
                                        "page_size", "journal_mode", "synchronous",
                                        "cache_size", "concurrent_mode"
                                    ],
                                    "minProperties": 5,
                                    "maxProperties": 5
                                }
                            }
                        }
                    }
                ]
            },
            "arm_statistics": {
                "type": "object",
                "additionalProperties": false,
                "required": [
                    "workload", "operation_count", "arm", "samples",
                    "median_ns", "mean_ns", "p95_ns", "stddev_ns", "cv_pct",
                    "median_ns_per_operation"
                ],
                "properties": {
                    "workload": {"$ref": "#/$defs/workload"},
                    "operation_count": {"type": "integer", "minimum": 1},
                    "arm": {"$ref": "#/$defs/arm"},
                    "samples": {"type": "integer", "minimum": 1},
                    "median_ns": {"type": "number", "minimum": 0},
                    "mean_ns": {"type": "number", "minimum": 0},
                    "p95_ns": {"type": "number", "minimum": 0},
                    "stddev_ns": {"type": "number", "minimum": 0},
                    "cv_pct": {"type": "number", "minimum": 0},
                    "median_ns_per_operation": {"type": "number", "minimum": 0}
                }
            },
            "paired_comparison": {
                "type": "object",
                "additionalProperties": false,
                "required": [
                    "workload", "operation_count", "numerator", "denominator",
                    "paired_blocks", "bootstrap_clusters", "median_ratio", "mean_ratio", "geomean_ratio",
                    "bootstrap_mean_ratio_ci95_low",
                    "bootstrap_mean_ratio_ci95_high"
                ],
                "properties": {
                    "workload": {"$ref": "#/$defs/workload"},
                    "operation_count": {"type": "integer", "minimum": 1},
                    "numerator": {"$ref": "#/$defs/arm"},
                    "denominator": {"$ref": "#/$defs/arm"},
                    "paired_blocks": {"type": "integer", "minimum": 1},
                    "bootstrap_clusters": {"type": "integer", "minimum": 1},
                    "median_ratio": {"type": "number", "exclusiveMinimum": 0},
                    "mean_ratio": {"type": "number", "exclusiveMinimum": 0},
                    "geomean_ratio": {"type": "number", "exclusiveMinimum": 0},
                    "bootstrap_mean_ratio_ci95_low": {
                        "type": "number", "exclusiveMinimum": 0
                    },
                    "bootstrap_mean_ratio_ci95_high": {
                        "type": "number", "exclusiveMinimum": 0
                    }
                }
            },
            "ready_regression": {
                "type": "object",
                "additionalProperties": false,
                "required": [
                    "predictor", "response", "interpretation", "points",
                    "paired_blocks", "bootstrap_clusters", "intercept_ns",
                    "slope_ns_per_additional_runtime_entry",
                    "bootstrap_slope_ci95_low", "bootstrap_slope_ci95_high",
                    "r_squared"
                ],
                "properties": {
                    "predictor": {"type": "string"},
                    "response": {"type": "string"},
                    "interpretation": {"type": "string"},
                    "points": {"type": "integer", "minimum": 2},
                    "paired_blocks": {"type": "integer", "minimum": 2},
                    "bootstrap_clusters": {"type": "integer", "minimum": 1},
                    "intercept_ns": {"type": "number"},
                    "slope_ns_per_additional_runtime_entry": {"type": "number"},
                    "bootstrap_slope_ci95_low": {"type": "number"},
                    "bootstrap_slope_ci95_high": {"type": "number"},
                    "r_squared": {"type": "number"}
                }
            }
        }
    })
}

fn print_benchmark_json_schema() {
    match serde_json::to_string_pretty(&benchmark_json_schema()) {
        Ok(json) => println!("{json}"),
        Err(err) => {
            eprintln!("ERROR: Could not serialize benchmark JSON schema: {err}");
            std::process::exit(1);
        }
    }
}

#[cfg(feature = "bridge-experiment")]
fn print_bridge_json_schema() {
    match serde_json::to_string_pretty(&bridge_json_schema()) {
        Ok(json) => println!("{json}"),
        Err(err) => {
            eprintln!("ERROR: Could not serialize bridge JSON schema: {err}");
            std::process::exit(1);
        }
    }
}

fn section_filter_matches(filter_lower: Option<&str>, aliases: &[&str]) -> bool {
    match filter_lower {
        Some(filter) => aliases.iter().any(|alias| alias.contains(filter)),
        None => true,
    }
}

fn emit_line(to_stdout: bool, line: impl AsRef<str>) {
    if to_stdout {
        println!("{}", line.as_ref());
    } else {
        eprintln!("{}", line.as_ref());
    }
}

impl BenchReport {
    fn new() -> Self {
        Self {
            sections: Vec::new(),
        }
    }

    fn add_section(&mut self, title: &str, description: &str) -> &mut ReportSection {
        self.sections.push(ReportSection {
            title: title.to_string(),
            description: description.to_string(),
            rows: Vec::new(),
        });
        self.sections.last_mut().unwrap()
    }

    fn print(&self, total_elapsed: Duration, environment: &DetectedEnvironment) {
        println!("\n{}", "=".repeat(140));
        println!("  COMPREHENSIVE BENCHMARK: FrankenSQLite vs C SQLite");
        println!("  {}", chrono_stamp());
        environment.print(true);
        println!(
            "  Total benchmark time: {:.1}s",
            total_elapsed.as_secs_f64()
        );
        println!("{}\n", "=".repeat(140));

        for section in &self.sections {
            println!("\n## {}", section.title);
            if !section.description.is_empty() {
                println!("   {}\n", section.description);
            }

            // Header
            println!(
                "  {:<42} {:>12} {:>12} {:>12} {:>12} {:>16} {:>8} {:>8}",
                "Scenario",
                "C SQLite",
                "FrankenSQLite",
                "C rows/s",
                "F rows/s",
                "Ratio",
                "CV%(C)",
                "CV%(F)"
            );
            println!("  {}", "-".repeat(136));

            for row in &section.rows {
                let cs_time = row
                    .csqlite
                    .as_ref()
                    .map_or_else(|| "N/A".to_string(), |m| format_duration(m.median()));
                let fs_time = row
                    .fsqlite
                    .as_ref()
                    .map_or_else(|| "N/A".to_string(), |m| format_duration(m.median()));
                let cs_rps = row
                    .csqlite
                    .as_ref()
                    .map_or_else(|| "N/A".to_string(), |m| format_rps(m.rows_per_sec()));
                let fs_rps = row
                    .fsqlite
                    .as_ref()
                    .map_or_else(|| "N/A".to_string(), |m| format_rps(m.rows_per_sec()));
                let cs_cv = row
                    .csqlite
                    .as_ref()
                    .map_or_else(|| "N/A".to_string(), |m| format!("{:.1}%", m.cv_percent()));
                let fs_cv = row
                    .fsqlite
                    .as_ref()
                    .map_or_else(|| "N/A".to_string(), |m| format!("{:.1}%", m.cv_percent()));

                let ratio = match (&row.csqlite, &row.fsqlite) {
                    (Some(c), Some(f)) => {
                        let r = f.median().as_nanos() as f64 / c.median().as_nanos() as f64;
                        if r < 1.0 {
                            format!("{:.2}x \x1b[32mfaster\x1b[0m", 1.0 / r)
                        } else if r > 1.0 {
                            format!("{:.2}x \x1b[31mslower\x1b[0m", r)
                        } else {
                            "1.00x equal".to_string()
                        }
                    }
                    _ => "N/A".to_string(),
                };

                println!(
                    "  {:<42} {:>12} {:>12} {:>12} {:>12} {:>16} {:>8} {:>8}",
                    row.scenario, cs_time, fs_time, cs_rps, fs_rps, ratio, cs_cv, fs_cv
                );
            }
        }

        // Summary statistics
        println!("\n{}", "=".repeat(120));
        println!("  SUMMARY STATISTICS");
        println!("{}\n", "=".repeat(120));

        let summary = compute_report_summary(self);
        if let Some(avg_ratio) = summary.average_ratio {
            println!(
                "  Total scenarios: {}  |  FrankenSQLite faster: {}  |  Comparable: {}  |  C SQLite faster: {}",
                summary.total_scenarios,
                summary.franken_faster,
                summary.comparable,
                summary.csqlite_faster
            );
            println!(
                "  Average time ratio (FrankenSQLite / C SQLite): {:.2}x",
                avg_ratio
            );
        }

        println!();
    }

    fn write_html(&self, path: &str, provenance: &JsonBenchmarkProvenance) -> Result<(), String> {
        let mut html = String::with_capacity(32 * 1024);

        // Collect JSON data for charts.
        let mut sections_json = String::from("[");
        for (si, section) in self.sections.iter().enumerate() {
            if si > 0 {
                sections_json.push(',');
            }
            sections_json.push_str(&format!(
                r#"{{"title":{},"description":{},"rows":["#,
                json_string(&section.title),
                json_string(&section.description),
            ));
            for (ri, row) in section.rows.iter().enumerate() {
                if ri > 0 {
                    sections_json.push(',');
                }
                let cs_median_ns = row
                    .csqlite
                    .as_ref()
                    .map_or(0.0, |m| m.median().as_nanos() as f64);
                let fs_median_ns = row
                    .fsqlite
                    .as_ref()
                    .map_or(0.0, |m| m.median().as_nanos() as f64);
                let cs_rps = row.csqlite.as_ref().map_or(0.0, Measurement::rows_per_sec);
                let fs_rps = row.fsqlite.as_ref().map_or(0.0, Measurement::rows_per_sec);
                let cs_mean_ns = row
                    .csqlite
                    .as_ref()
                    .map_or(0.0, |m| m.mean().as_nanos() as f64);
                let fs_mean_ns = row
                    .fsqlite
                    .as_ref()
                    .map_or(0.0, |m| m.mean().as_nanos() as f64);
                let cs_min_ns = row
                    .csqlite
                    .as_ref()
                    .map_or(0.0, |m| m.min().as_nanos() as f64);
                let fs_min_ns = row
                    .fsqlite
                    .as_ref()
                    .map_or(0.0, |m| m.min().as_nanos() as f64);
                let cs_stddev_ns = row
                    .csqlite
                    .as_ref()
                    .map_or(0.0, |m| m.stddev().as_nanos() as f64);
                let fs_stddev_ns = row
                    .fsqlite
                    .as_ref()
                    .map_or(0.0, |m| m.stddev().as_nanos() as f64);
                let cs_iters = row.csqlite.as_ref().map_or(0, |m| m.durations.len());
                let fs_iters = row.fsqlite.as_ref().map_or(0, |m| m.durations.len());
                let cs_cv = row.csqlite.as_ref().map_or(0.0, Measurement::cv_percent);
                let fs_cv = row.fsqlite.as_ref().map_or(0.0, Measurement::cv_percent);
                let cs_p95_ns = row
                    .csqlite
                    .as_ref()
                    .map_or(0.0, |m| m.p95().as_nanos() as f64);
                let fs_p95_ns = row
                    .fsqlite
                    .as_ref()
                    .map_or(0.0, |m| m.p95().as_nanos() as f64);
                let ratio = if cs_median_ns > 0.0 {
                    fs_median_ns / cs_median_ns
                } else {
                    0.0
                };
                sections_json.push_str(&format!(
                    r#"{{"scenario":{},"cs_median_ns":{cs_median_ns},"fs_median_ns":{fs_median_ns},"cs_rps":{cs_rps},"fs_rps":{fs_rps},"cs_mean_ns":{cs_mean_ns},"fs_mean_ns":{fs_mean_ns},"cs_min_ns":{cs_min_ns},"fs_min_ns":{fs_min_ns},"cs_stddev_ns":{cs_stddev_ns},"fs_stddev_ns":{fs_stddev_ns},"cs_iters":{cs_iters},"fs_iters":{fs_iters},"cs_cv":{cs_cv:.1},"fs_cv":{fs_cv:.1},"cs_p95_ns":{cs_p95_ns},"fs_p95_ns":{fs_p95_ns},"ratio":{ratio}}}"#,
                    json_string(&row.scenario),
                ));
            }
            sections_json.push_str("]}");
        }
        sections_json.push(']');

        // Summary stats.
        let summary = compute_report_summary(self);
        let avg_ratio = summary.average_ratio.unwrap_or(1.0);
        let provenance_banner = if provenance.citable {
            r#"<section class="max-w-6xl mx-auto px-6 mt-6">
  <div class="rounded-xl border border-emerald-400 bg-emerald-950 p-5 text-emerald-100">
    <p class="text-xl font-extrabold">CITABLE PROVENANCE VERIFIED</p>
  </div>
</section>"#
                .to_owned()
        } else {
            let errors = provenance
                .validation_errors
                .iter()
                .map(|error| format!("<li>{}</li>", html_escape(error)))
                .collect::<String>();
            format!(
                r#"<section class="max-w-6xl mx-auto px-6 mt-6">
  <div class="rounded-xl border-4 border-red-400 bg-red-950 p-6 text-red-50" role="alert">
    <p class="text-3xl font-black">NON-CITABLE DIAGNOSTIC</p>
    <p class="mt-2 font-semibold">These numbers are not release evidence and must not support a performance claim.</p>
    <p class="mt-4 font-bold">Failed provenance checks:</p>
    <ul class="mt-2 list-disc space-y-1 pl-6">{errors}</ul>
  </div>
</section>"#
            )
        };

        html.push_str(&format!(
            r#"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>FrankenSQLite vs C SQLite — Benchmark Report</title>
<script src="https://cdn.tailwindcss.com"></script>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.7/dist/chart.umd.min.js"></script>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&family=JetBrains+Mono:wght@400;500;600&display=swap" rel="stylesheet">
<script>
tailwind.config = {{
  theme: {{
    extend: {{
      fontFamily: {{
        sans: ['Inter', 'system-ui', 'sans-serif'],
        mono: ['JetBrains Mono', 'monospace'],
      }},
    }},
  }},
}}
</script>
<style>
  body {{ font-family: 'Inter', system-ui, sans-serif; }}
  .gradient-bg {{ background: linear-gradient(135deg, #0f172a 0%, #1e293b 50%, #0f172a 100%); }}
  .card {{ background: rgba(30, 41, 59, 0.8); backdrop-filter: blur(12px); border: 1px solid rgba(148, 163, 184, 0.1); }}
  .glow {{ box-shadow: 0 0 40px rgba(59, 130, 246, 0.15); }}
  .stat-card {{ transition: transform 0.2s, box-shadow 0.2s; }}
  .stat-card:hover {{ transform: translateY(-2px); box-shadow: 0 8px 30px rgba(0,0,0,0.3); }}
  .faster {{ color: #34d399; }}
  .slower {{ color: #f87171; }}
  .equal {{ color: #94a3b8; }}
  .bar-cs {{ background: linear-gradient(90deg, #3b82f6, #60a5fa); }}
  .bar-fs {{ background: linear-gradient(90deg, #f59e0b, #fbbf24); }}
  table th {{ position: sticky; top: 0; z-index: 10; }}
  .section-nav a {{ transition: all 0.15s; }}
  .section-nav a:hover {{ background: rgba(59, 130, 246, 0.2); }}
  .section-nav a.active {{ background: rgba(59, 130, 246, 0.3); border-left-color: #3b82f6; }}
</style>
</head>
<body class="gradient-bg min-h-screen text-slate-200">

<!-- Hero Header -->
<header class="py-12 px-6 text-center border-b border-slate-700/50">
  <div class="max-w-5xl mx-auto">
    <div class="inline-flex items-center gap-2 px-4 py-1.5 rounded-full bg-blue-500/10 border border-blue-500/20 text-blue-400 text-sm font-medium mb-6">
      <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 10V3L4 14h7v7l9-11h-7z"/></svg>
      Performance Benchmark Report
    </div>
    <h1 class="text-4xl md:text-5xl font-extrabold bg-gradient-to-r from-white via-slate-200 to-slate-400 bg-clip-text text-transparent mb-4">
      FrankenSQLite vs C SQLite
    </h1>
    <p class="text-slate-400 text-lg max-w-2xl mx-auto">
      Comprehensive comparison across insertions, reads, concurrency, and mixed workloads.
      MVCC page-level versioning vs traditional WAL write lock.
    </p>
    <p class="text-slate-500 text-sm mt-4 font-mono">{}</p>
  </div>
</header>

{provenance_banner}

<!-- Summary Cards -->
<section class="max-w-6xl mx-auto px-6 -mt-6">
  <div class="grid grid-cols-2 md:grid-cols-4 gap-4">
    <div class="card rounded-xl p-5 stat-card glow">
      <div class="text-xs font-medium text-slate-400 uppercase tracking-wider mb-1">Total Scenarios</div>
      <div class="text-3xl font-bold text-white">{}</div>
    </div>
    <div class="card rounded-xl p-5 stat-card" style="box-shadow: 0 0 40px rgba(52,211,153,0.12);">
      <div class="text-xs font-medium text-slate-400 uppercase tracking-wider mb-1">FrankenSQLite Faster</div>
      <div class="text-3xl font-bold faster">{}</div>
    </div>
    <div class="card rounded-xl p-5 stat-card">
      <div class="text-xs font-medium text-slate-400 uppercase tracking-wider mb-1">Comparable</div>
      <div class="text-3xl font-bold equal">{}</div>
    </div>
    <div class="card rounded-xl p-5 stat-card" style="box-shadow: 0 0 40px rgba(248,113,113,0.10);">
      <div class="text-xs font-medium text-slate-400 uppercase tracking-wider mb-1">C SQLite Faster</div>
      <div class="text-3xl font-bold slower">{}</div>
    </div>
  </div>
  <div class="card rounded-xl p-5 mt-4 text-center">
    <span class="text-slate-400">Average time ratio (FrankenSQLite / C SQLite):</span>
    <span class="text-xl font-bold ml-2 {}">{avg_ratio:.2}x</span>
  </div>
</section>

<!-- Section Navigation -->
<nav class="section-nav max-w-6xl mx-auto px-6 mt-8">
  <div class="card rounded-xl p-4 flex flex-wrap gap-2" id="section-nav"></div>
</nav>

<!-- Benchmark Sections -->
<main class="max-w-6xl mx-auto px-6 py-8 space-y-10" id="sections-container"></main>

<!-- Footer -->
<footer class="border-t border-slate-700/50 py-8 text-center text-slate-500 text-sm">
  <p>Generated by <span class="text-slate-300 font-medium">comprehensive-bench</span> &mdash; FrankenSQLite E2E Benchmark Suite</p>
  <p class="mt-1">Clean-room Rust reimplementation of SQLite with MVCC page-level versioning</p>
</footer>

<script>
const DATA = {sections_json};

function fmtDuration(ns) {{
  if (ns === 0) return 'N/A';
  if (ns < 1e3) return ns.toFixed(0) + ' ns';
  if (ns < 1e6) return (ns / 1e3).toFixed(1) + ' \u00b5s';
  if (ns < 1e9) return (ns / 1e6).toFixed(2) + ' ms';
  return (ns / 1e9).toFixed(3) + ' s';
}}

function fmtRps(rps) {{
  if (rps === 0) return 'N/A';
  if (rps >= 1e6) return (rps / 1e6).toFixed(2) + 'M/s';
  if (rps >= 1e3) return (rps / 1e3).toFixed(1) + 'K/s';
  return rps.toFixed(0) + '/s';
}}

function ratioClass(r) {{
  if (r < 0.95) return 'faster';
  if (r > 1.05) return 'slower';
  return 'equal';
}}

function ratioText(r) {{
  if (r === 0) return 'N/A';
  if (r < 1.0) return (1/r).toFixed(2) + 'x faster';
  if (r > 1.0) return r.toFixed(2) + 'x slower';
  return '1.00x equal';
}}

// Build section navigation
const nav = document.getElementById('section-nav');
DATA.forEach((sec, i) => {{
  const a = document.createElement('a');
  a.href = '#section-' + i;
  a.textContent = sec.title.replace(/\u2014/g, '-').substring(0, 40) + (sec.title.length > 40 ? '...' : '');
  a.className = 'block px-3 py-1.5 rounded-lg text-sm text-slate-300 border-l-2 border-transparent hover:text-white cursor-pointer';
  nav.appendChild(a);
}});

// Build sections
const container = document.getElementById('sections-container');
DATA.forEach((sec, si) => {{
  const div = document.createElement('div');
  div.id = 'section-' + si;
  div.className = 'scroll-mt-24';

  // Only create chart for sections with paired data
  const hasChart = sec.rows.some(r => r.cs_median_ns > 0 && r.fs_median_ns > 0);
  const chartId = 'chart-' + si;

  let tableRows = '';
  sec.rows.forEach(r => {{
    const rc = ratioClass(r.ratio);
    const csCV = r.cs_cv !== undefined ? r.cs_cv.toFixed(1) + '%' : 'N/A';
    const fsCV = r.fs_cv !== undefined ? r.fs_cv.toFixed(1) + '%' : 'N/A';
    const csP95 = r.cs_p95_ns ? fmtDuration(r.cs_p95_ns) : 'N/A';
    const fsP95 = r.fs_p95_ns ? fmtDuration(r.fs_p95_ns) : 'N/A';
    tableRows += `<tr class="border-b border-slate-700/30 hover:bg-slate-700/20 transition-colors">
      <td class="py-3 px-4 text-sm font-medium text-slate-200">${{r.scenario}}</td>
      <td class="py-3 px-4 text-sm font-mono text-right text-blue-400" title="p95: ${{csP95}}">${{fmtDuration(r.cs_median_ns)}}</td>
      <td class="py-3 px-4 text-sm font-mono text-right text-amber-400" title="p95: ${{fsP95}}">${{fmtDuration(r.fs_median_ns)}}</td>
      <td class="py-3 px-4 text-sm font-mono text-right text-blue-300">${{fmtRps(r.cs_rps)}}</td>
      <td class="py-3 px-4 text-sm font-mono text-right text-amber-300">${{fmtRps(r.fs_rps)}}</td>
      <td class="py-3 px-4 text-sm font-mono text-right font-semibold ${{rc}}">${{ratioText(r.ratio)}}</td>
      <td class="py-3 px-4 text-sm font-mono text-right text-slate-500">${{csCV}} / ${{fsCV}}</td>
    </tr>`;
  }});

  div.innerHTML = `
    <div class="card rounded-2xl overflow-hidden glow">
      <div class="px-6 py-5 border-b border-slate-700/50">
        <h2 class="text-xl font-bold text-white">${{sec.title}}</h2>
        ${{sec.description ? '<p class="text-sm text-slate-400 mt-1">' + sec.description + '</p>' : ''}}
      </div>
      ${{hasChart ? '<div class="px-6 py-4 border-b border-slate-700/30"><canvas id="' + chartId + '" height="' + Math.max(60, sec.rows.length * 28) + '"></canvas></div>' : ''}}
      <div class="overflow-x-auto">
        <table class="w-full text-left">
          <thead>
            <tr class="bg-slate-800/80 text-xs font-semibold text-slate-400 uppercase tracking-wider">
              <th class="py-3 px-4">Scenario</th>
              <th class="py-3 px-4 text-right">C SQLite</th>
              <th class="py-3 px-4 text-right">FrankenSQLite</th>
              <th class="py-3 px-4 text-right">C rows/s</th>
              <th class="py-3 px-4 text-right">F rows/s</th>
              <th class="py-3 px-4 text-right">Ratio</th>
              <th class="py-3 px-4 text-right" title="Coefficient of Variation">CV% (C/F)</th>
            </tr>
          </thead>
          <tbody>${{tableRows}}</tbody>
        </table>
      </div>
    </div>`;

  container.appendChild(div);

  // Create horizontal bar chart
  if (hasChart) {{
    const ctx = document.getElementById(chartId).getContext('2d');
    const labels = sec.rows.filter(r => r.cs_median_ns > 0 && r.fs_median_ns > 0).map(r => {{
      const s = r.scenario;
      return s.length > 50 ? s.substring(0, 47) + '...' : s;
    }});
    const csData = sec.rows.filter(r => r.cs_median_ns > 0 && r.fs_median_ns > 0).map(r => r.cs_median_ns / 1e6);
    const fsData = sec.rows.filter(r => r.cs_median_ns > 0 && r.fs_median_ns > 0).map(r => r.fs_median_ns / 1e6);

    new Chart(ctx, {{
      type: 'bar',
      data: {{
        labels: labels,
        datasets: [
          {{
            label: 'C SQLite (ms)',
            data: csData,
            backgroundColor: 'rgba(59, 130, 246, 0.7)',
            borderColor: 'rgba(96, 165, 250, 1)',
            borderWidth: 1,
            borderRadius: 4,
          }},
          {{
            label: 'FrankenSQLite (ms)',
            data: fsData,
            backgroundColor: 'rgba(245, 158, 11, 0.7)',
            borderColor: 'rgba(251, 191, 36, 1)',
            borderWidth: 1,
            borderRadius: 4,
          }},
        ],
      }},
      options: {{
        indexAxis: 'y',
        responsive: true,
        maintainAspectRatio: false,
        plugins: {{
          legend: {{
            labels: {{ color: '#94a3b8', font: {{ family: 'Inter', size: 12 }} }},
          }},
          tooltip: {{
            callbacks: {{
              label: function(ctx) {{
                return ctx.dataset.label + ': ' + ctx.parsed.x.toFixed(2) + ' ms';
              }}
            }}
          }},
        }},
        scales: {{
          x: {{
            type: 'logarithmic',
            title: {{ display: true, text: 'Time (ms, log scale)', color: '#64748b' }},
            ticks: {{ color: '#64748b', font: {{ family: 'JetBrains Mono', size: 11 }} }},
            grid: {{ color: 'rgba(71, 85, 105, 0.3)' }},
          }},
          y: {{
            ticks: {{ color: '#94a3b8', font: {{ family: 'Inter', size: 11 }} }},
            grid: {{ display: false }},
          }},
        }},
      }},
    }});
  }}
}});

// Intersection observer for nav highlighting
const observer = new IntersectionObserver((entries) => {{
  entries.forEach(entry => {{
    if (entry.isIntersecting) {{
      const idx = entry.target.id.replace('section-', '');
      document.querySelectorAll('.section-nav a').forEach((a, i) => {{
        a.classList.toggle('active', i === parseInt(idx));
      }});
    }}
  }});
}}, {{ threshold: 0.3 }});
document.querySelectorAll('[id^="section-"]').forEach(el => observer.observe(el));
</script>
</body>
</html>"#,
            chrono_stamp(),
            if avg_ratio < 1.0 { "faster" } else { "slower" },
            summary.total_scenarios,
            summary.franken_faster,
            summary.comparable,
            summary.csqlite_faster,
        ));

        let provenance_json = serde_json::to_string(provenance)
            .map_err(|error| format!("could not serialize HTML provenance: {error}"))?
            .replace('<', "\\u003c")
            .replace('>', "\\u003e");
        let provenance_element = format!(
            r#"<script id="benchmark-provenance" type="application/json">{provenance_json}</script>
"#
        );
        let body_end = html
            .rfind("</body>")
            .ok_or_else(|| "generated HTML has no closing body element".to_owned())?;
        html.insert_str(body_end, &provenance_element);

        ensure_report_parent_dir(path, "HTML")?;
        let mut file = std::fs::File::create(path)
            .map_err(|error| format!("could not create HTML report at {path}: {error}"))?;
        file.write_all(html.as_bytes())
            .map_err(|error| format!("could not write HTML report at {path}: {error}"))?;
        eprintln!("HTML report written to: {path}");
        Ok(())
    }
}

fn html_escape(s: &str) -> String {
    let mut escaped = String::with_capacity(s.len());
    for ch in s.chars() {
        match ch {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '"' => escaped.push_str("&quot;"),
            '\'' => escaped.push_str("&#39;"),
            _ => escaped.push(ch),
        }
    }
    escaped
}

fn json_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for ch in s.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c < '\x20' => {
                out.push_str(&format!("\\u{:04x}", c as u32));
            }
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

impl ReportSection {
    fn add_row(
        &mut self,
        scenario: &str,
        csqlite: Option<Measurement>,
        fsqlite: Option<Measurement>,
    ) {
        self.add_row_with_fsqlite_concurrent_profile(scenario, csqlite, fsqlite, None);
    }

    fn add_row_with_fsqlite_concurrent_profile(
        &mut self,
        scenario: &str,
        csqlite: Option<Measurement>,
        fsqlite: Option<Measurement>,
        fsqlite_concurrent_profile: Option<JsonFsqliteConcurrentProfile>,
    ) {
        self.add_row_with_concurrent_details(
            scenario,
            csqlite,
            fsqlite,
            fsqlite_concurrent_profile,
            None,
        );
    }

    fn add_row_with_concurrent_details(
        &mut self,
        scenario: &str,
        csqlite: Option<Measurement>,
        fsqlite: Option<Measurement>,
        fsqlite_concurrent_profile: Option<JsonFsqliteConcurrentProfile>,
        concurrent_readiness: Option<JsonConcurrentReadiness>,
    ) {
        self.rows.push(ReportRow {
            scenario: scenario.to_string(),
            csqlite,
            fsqlite,
            fsqlite_concurrent_profile,
            concurrent_readiness,
        });
    }
}

fn format_duration(d: Duration) -> String {
    let nanos = d.as_nanos();
    if nanos < 1_000 {
        format!("{nanos} ns")
    } else if nanos < 1_000_000 {
        format!("{:.1} us", nanos as f64 / 1_000.0)
    } else if nanos < 1_000_000_000 {
        format!("{:.2} ms", nanos as f64 / 1_000_000.0)
    } else {
        format!("{:.3} s", nanos as f64 / 1_000_000_000.0)
    }
}

fn format_rps(rps: f64) -> String {
    if rps >= 1_000_000.0 {
        format!("{:.2}M/s", rps / 1_000_000.0)
    } else if rps >= 1_000.0 {
        format!("{:.1}K/s", rps / 1_000.0)
    } else {
        format!("{:.0}/s", rps)
    }
}

fn chrono_stamp() -> String {
    let now = SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    format_unix_utc(now)
}

fn format_unix_utc(timestamp: u64) -> String {
    // Convert unix timestamp to readable date.
    let days = timestamp / 86400;
    let secs_in_day = timestamp % 86400;
    let hours = secs_in_day / 3600;
    let mins = (secs_in_day % 3600) / 60;
    let secs = secs_in_day % 60;
    // Approximate year/month/day from days since epoch.
    let (year, month, day) = days_to_ymd(days);
    format!("{year}-{month:02}-{day:02} {hours:02}:{mins:02}:{secs:02} UTC")
}

fn days_to_ymd(days: u64) -> (u64, u64, u64) {
    // Simplified Gregorian calendar conversion.
    let mut y = 1970;
    let mut remaining = days;
    loop {
        let days_in_year = if y % 4 == 0 && (y % 100 != 0 || y % 400 == 0) {
            366
        } else {
            365
        };
        if remaining < days_in_year {
            break;
        }
        remaining -= days_in_year;
        y += 1;
    }
    let leap = y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
    let month_days: &[u64] = if leap {
        &[31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    } else {
        &[31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    };
    let mut m = 0;
    for &md in month_days {
        if remaining < md {
            break;
        }
        remaining -= md;
        m += 1;
    }
    (y, m + 1, remaining + 1)
}

fn timestamp_filename(base: &str, ext: &str) -> String {
    let now = SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let (y, m, d) = days_to_ymd(now / 86400);
    let h = (now % 86400) / 3600;
    let min = (now % 3600) / 60;
    format!("{base}_{y}{m:02}{d:02}_{h:02}{min:02}.{ext}")
}

fn print_usage() {
    eprintln!(
        "Usage:
  cargo run --profile release-perf -p fsqlite-e2e --bin comprehensive-bench
  cargo run --profile release-perf -p fsqlite-e2e --bin comprehensive-bench -- --quick
  cargo run --profile release-perf -p fsqlite-e2e --bin comprehensive-bench -- --filter insert
  cargo run --profile release-perf -p fsqlite-e2e --bin comprehensive-bench -- --json-out report.json --no-html --allow-unverified-provenance
  cargo run --profile release-perf -p fsqlite-e2e --bin comprehensive-bench -- --json-stdout --no-html --allow-unverified-provenance
  cargo run --profile release-perf -p fsqlite-e2e --bin comprehensive-bench -- --print-json-schema
  GATE0_TARGET=$(mktemp -d /data/tmp/fsqlite-gate0-target.XXXXXX)
  GATE0_RECEIPTS=$(mktemp -d /data/tmp/fsqlite-gate0-receipts.XXXXXX)
  GATE0_NONCE=$(od -An -N32 -tx1 /dev/urandom | tr -d ' \\n')
  GATE0_HOST=$(rustc -vV | sed -n 's/^host: //p')
  env -i HOME=\"$HOME\" USER=\"$USER\" LOGNAME=\"$USER\" PATH=\"$HOME/.cargo/bin:/usr/local/bin:/usr/bin:/bin\" LC_ALL=C CARGO_ENCODED_RUSTFLAGS= CARGO_BUILD_RUSTFLAGS= CARGO_BUILD_RUSTC_WRAPPER= CARGO_BUILD_RUSTC_WORKSPACE_WRAPPER= RUSTC_WRAPPER= RUSTC_WORKSPACE_WRAPPER= CARGO_PROFILE_RELEASE_PERF_CODEGEN_UNITS=1 CARGO_PROFILE_RELEASE_PERF_DEBUG=false CARGO_PROFILE_RELEASE_PERF_DEBUG_ASSERTIONS=false CARGO_PROFILE_RELEASE_PERF_INCREMENTAL=false CARGO_PROFILE_RELEASE_PERF_LTO=true CARGO_PROFILE_RELEASE_PERF_OPT_LEVEL=3 CARGO_PROFILE_RELEASE_PERF_OVERFLOW_CHECKS=false CARGO_PROFILE_RELEASE_PERF_PANIC=abort CARGO_PROFILE_RELEASE_PERF_RPATH=false CARGO_PROFILE_RELEASE_PERF_SPLIT_DEBUGINFO=off CARGO_PROFILE_RELEASE_PERF_STRIP=true LIBSQLITE3_FLAGS=-DSQLITE_ENABLE_MATH_FUNCTIONS FSQLITE_BENCH_PROFILE_NAME=release-perf FSQLITE_BENCH_BUILD_NONCE=\"$GATE0_NONCE\" cargo build --locked -vv --color never --message-format=json-render-diagnostics --target-dir \"$GATE0_TARGET\" --profile release-perf --target \"$GATE0_HOST\" -p fsqlite-e2e --features bridge-experiment --bin comprehensive-bench > \"$GATE0_RECEIPTS/gate0-build-events.jsonl\" 2> \"$GATE0_RECEIPTS/gate0-build-vv.log\"
  BENCH_BIN=$(jq -Rr 'fromjson? | select(.reason == \"compiler-artifact\" and .target.name == \"comprehensive-bench\" and (.target.kind | index(\"bin\"))) | .executable // empty' \"$GATE0_RECEIPTS/gate0-build-events.jsonl\" | tail -n 1)
  test -n \"$BENCH_BIN\" && FSQLITE_BENCH_BUILD_LOG_PATH=\"$GATE0_RECEIPTS/gate0-build-vv.log\" FSQLITE_BENCH_EXPECTED_CPU_AFFINITY=2-3 FSQLITE_BENCH_MAX_LOAD_1M=1 taskset -c 2,3 \"$BENCH_BIN\" --bridge-experiment --allow-unverified-provenance --json-out \"$GATE0_RECEIPTS/bridge-diagnostic.json\"

Flags:
  --quick              Run the reduced benchmark matrix.
  --filter <text>      Run only sections whose names match <text>.
  --html <path>        Write the HTML report to an explicit path.
  --no-html            Skip HTML report generation.
  --json               Write the JSON report to a timestamped file.
  --json-out <path>    Write the JSON report to an explicit path.
  --json-stdout        Emit only the structured JSON report to stdout.
  --print-json-schema  Emit the standardized benchmark JSON schema and exit.
  --allow-unverified-provenance
                       Emit explicitly non-citable artifacts when provenance validation fails.
  --bridge-experiment  Run the standalone three-arm async bridge experiment.
                       It remains diagnostic until isolated-cpuset/full-dynticks/IRQ
                       receipts are implemented; use --allow-unverified-provenance.
  --bridge-samples <n> Samples per bridge arm; multiple of 48 and at least 48
                       (default: 96).
  --bridge-operations <n>
                       Timed insert operations per sample (default: 1000).
  --bridge-seed <n>    Deterministic ABBA ordering/bootstrap seed.
  --help, -h           Show this help text."
    );
}

fn parse_cli_args(args: &[String]) -> Result<CliOptions, String> {
    let mut options = CliOptions {
        quick: false,
        filter: None,
        html_path: None,
        emit_html: false,
        emit_timestamped_json: false,
        json_out_path: None,
        json_stdout: false,
        print_json_schema: false,
        allow_unverified_provenance: false,
        bridge_experiment: false,
        bridge_samples: 96,
        bridge_operations: 1_000,
        bridge_seed: 0x4653_514c_4954_4530,
    };

    let mut index = 1;
    while index < args.len() {
        match args[index].as_str() {
            "--quick" => {
                options.quick = true;
                index += 1;
            }
            "--filter" => {
                let value = args
                    .get(index + 1)
                    .ok_or_else(|| "expected a value after --filter".to_owned())?;
                options.filter = Some(value.clone());
                index += 2;
            }
            "--html" => {
                let value = args
                    .get(index + 1)
                    .ok_or_else(|| "expected a path after --html".to_owned())?;
                options.html_path = Some(value.clone());
                options.emit_html = true;
                index += 2;
            }
            "--no-html" => {
                options.emit_html = false;
                index += 1;
            }
            "--json" => {
                options.emit_timestamped_json = true;
                index += 1;
            }
            "--json-out" => {
                let value = args
                    .get(index + 1)
                    .ok_or_else(|| "expected a path after --json-out".to_owned())?;
                options.json_out_path = Some(value.clone());
                index += 2;
            }
            "--json-stdout" => {
                options.json_stdout = true;
                index += 1;
            }
            "--print-json-schema" => {
                options.print_json_schema = true;
                index += 1;
            }
            "--allow-unverified-provenance" => {
                options.allow_unverified_provenance = true;
                index += 1;
            }
            "--bridge-experiment" => {
                options.bridge_experiment = true;
                options.emit_html = false;
                index += 1;
            }
            "--bridge-samples" => {
                let value = args
                    .get(index + 1)
                    .ok_or_else(|| "expected a value after --bridge-samples".to_owned())?;
                options.bridge_samples = value
                    .parse()
                    .map_err(|_| "--bridge-samples must be a positive integer".to_owned())?;
                index += 2;
            }
            "--bridge-operations" => {
                let value = args
                    .get(index + 1)
                    .ok_or_else(|| "expected a value after --bridge-operations".to_owned())?;
                options.bridge_operations = value
                    .parse()
                    .map_err(|_| "--bridge-operations must be a positive integer".to_owned())?;
                index += 2;
            }
            "--bridge-seed" => {
                let value = args
                    .get(index + 1)
                    .ok_or_else(|| "expected a value after --bridge-seed".to_owned())?;
                options.bridge_seed = value
                    .parse()
                    .map_err(|_| "--bridge-seed must be an unsigned integer".to_owned())?;
                index += 2;
            }
            unknown => {
                return Err(format!("unrecognized argument `{unknown}`"));
            }
        }
    }

    if options.bridge_experiment {
        if options.bridge_samples < 48 || options.bridge_samples % 48 != 0 {
            return Err("--bridge-samples must be a multiple of 48 and at least 48".to_owned());
        }
        if options.bridge_operations == 0 {
            return Err("--bridge-operations must be greater than zero".to_owned());
        }
        if options.quick || options.filter.is_some() || options.html_path.is_some() {
            return Err(
                "--bridge-experiment cannot be combined with --quick, --filter, or --html"
                    .to_owned(),
            );
        }
    }

    Ok(options)
}

fn print_run_banner(
    to_stdout: bool,
    options: &CliOptions,
    row_counts: &[usize],
    environment: &DetectedEnvironment,
    provenance: &JsonBenchmarkProvenance,
) {
    emit_line(to_stdout, format!("\n{}", "=".repeat(80)));
    emit_line(
        to_stdout,
        "  Comprehensive FrankenSQLite vs C SQLite Benchmark",
    );
    if !provenance.citable {
        emit_line(
            to_stdout,
            "  !!! NON-CITABLE DIAGNOSTIC — NUMBERS ARE NOT RELEASE EVIDENCE !!!",
        );
        for error in &provenance.validation_errors {
            emit_line(to_stdout, format!("  provenance: {error}"));
        }
    }
    emit_line(to_stdout, "=".repeat(80));
    environment.print(to_stdout);
    emit_line(
        to_stdout,
        format!("  Mode: {}", if options.quick { "quick" } else { "full" }),
    );
    emit_line(
        to_stdout,
        format!("  Row counts: {:?}", row_counts.iter().collect::<Vec<_>>()),
    );
    emit_line(
        to_stdout,
        format!(
            "  Measurement: {WARMUP_ITERS} warmup, {MIN_ITERS}-{MAX_ITERS} iters, target {:.0}s",
            TARGET_DURATION.as_secs_f64()
        ),
    );
    if let Some(filter) = &options.filter {
        emit_line(to_stdout, format!("  Filter: {filter}"));
    }
    emit_line(to_stdout, "=".repeat(80));
    emit_line(to_stdout, "");
}

// ─── Section 1: Insert throughput by row count ─────────────────────────

fn bench_insert_by_row_count(
    report: &mut BenchReport,
    row_counts: &[usize],
    record_size: RecordSize,
) {
    let section = report.add_section(
        &format!(
            "INSERTThroughput — Single Transaction — {}",
            record_size.name()
        ),
        &format!(
            "Record: {}. All rows inserted in a single BEGIN..COMMIT.",
            record_size.description()
        ),
    );
    let profile_insert_enabled = bench_env_flag("FSQLITE_BENCH_PROFILE_INSERT");

    for &count in row_counts {
        eprint!(
            "  Benchmarking single-txn insert {count} rows ({})... ",
            record_size.name()
        );

        let csqlite_m = {
            let insert_sql = record_size.insert_sql_csqlite();
            let create_sql = record_size.create_table_sql();
            measure(&format!("csqlite_{count}"), count, || {
                let conn = rusqlite::Connection::open_in_memory().unwrap();
                apply_pragmas_csqlite(&conn);
                conn.execute_batch(&format!("{create_sql};")).unwrap();
                conn.execute_batch("BEGIN").unwrap();
                let mut stmt = conn.prepare(insert_sql).unwrap();
                #[allow(clippy::cast_possible_wrap)]
                for i in 0..count as i64 {
                    stmt.execute(rusqlite::params![i]).unwrap();
                }
                conn.execute_batch("COMMIT").unwrap();
            })
        };

        let fsqlite_m = {
            let create_sql = record_size.create_table_sql();
            measure(&format!("fsqlite_{count}"), count, || {
                // bd-zavyn: one runtime entry per timed sample.
                fsqlite_e2e::block_on(async {
                    let conn = open_fsqlite_memory_connection_for_benchmark_async().await;
                    apply_pragmas_fsqlite_async(&conn).await;
                    fs_execute_async(&conn, create_sql).await;
                    fs_execute_async(&conn, "BEGIN").await;
                    #[allow(clippy::cast_possible_wrap)]
                    let stmt = fs_prepare_async(&conn, record_size.insert_sql_csqlite()).await;
                    for i in 0..count as i64 {
                        fs_stmt_execute_with_params_async(
                            &stmt,
                            &[fsqlite::SqliteValue::Integer(i)],
                        )
                        .await;
                    }
                    fs_execute_async(&conn, "COMMIT").await;
                });
            })
        };
        if profile_insert_enabled {
            profile_fsqlite_insert(record_size, count, "single_txn");
        }

        eprintln!(
            "C={} F={}",
            format_duration(csqlite_m.median()),
            format_duration(fsqlite_m.median()),
        );

        section.add_row(&format!("{count} rows"), Some(csqlite_m), Some(fsqlite_m));
    }
}

// ─── Section 2: Insert throughput by transaction strategy ──────────────

fn bench_insert_by_txn_strategy(report: &mut BenchReport, row_counts: &[usize]) {
    let section = report.add_section(
        "INSERTThroughput — Transaction Strategy Comparison (small_3col)",
        "Compares autocommit, batched (1K/txn), and single-txn strategies.",
    );

    let record_size = RecordSize::Small;
    let profile_insert_enabled = bench_env_flag("FSQLITE_BENCH_PROFILE_INSERT");

    for &count in row_counts {
        // Skip autocommit for large counts (too slow).
        let do_autocommit = count <= 10_000;
        let batch_size = 1000.min(count);

        // --- Autocommit ---
        if do_autocommit {
            eprint!("  Benchmarking autocommit {count} rows... ");

            let cs = {
                let insert_sql = record_size.insert_sql_csqlite();
                let create_sql = record_size.create_table_sql();
                measure(&format!("cs_auto_{count}"), count, || {
                    let conn = rusqlite::Connection::open_in_memory().unwrap();
                    apply_pragmas_csqlite(&conn);
                    conn.execute_batch(&format!("{create_sql};")).unwrap();
                    let mut stmt = conn.prepare(insert_sql).unwrap();
                    #[allow(clippy::cast_possible_wrap)]
                    for i in 0..count as i64 {
                        stmt.execute(rusqlite::params![i]).unwrap();
                    }
                })
            };

            let fs = {
                let create_sql = record_size.create_table_sql();
                measure(&format!("fs_auto_{count}"), count, || {
                    // bd-zavyn: one runtime entry per timed sample.
                    fsqlite_e2e::block_on(async {
                        let conn = open_fsqlite_memory_connection_for_benchmark_async().await;
                        apply_pragmas_fsqlite_async(&conn).await;
                        fs_execute_async(&conn, create_sql).await;
                        let stmt = fs_prepare_async(&conn, record_size.insert_sql_csqlite()).await;
                        #[allow(clippy::cast_possible_wrap)]
                        for i in 0..count as i64 {
                            fs_stmt_execute_with_params_async(
                                &stmt,
                                &[fsqlite::SqliteValue::Integer(i)],
                            )
                            .await;
                        }
                    });
                })
            };

            eprintln!(
                "C={} F={}",
                format_duration(cs.median()),
                format_duration(fs.median())
            );
            if profile_insert_enabled {
                profile_fsqlite_insert_with_strategy(
                    record_size,
                    count,
                    "txn_autocommit",
                    InsertProfileStrategy::Autocommit,
                );
            }
            section.add_row(&format!("{count} rows / autocommit"), Some(cs), Some(fs));
        }

        // --- Batched ---
        eprint!("  Benchmarking batched {count} rows ({batch_size}/txn)... ");

        let cs = {
            let insert_sql = record_size.insert_sql_csqlite();
            let create_sql = record_size.create_table_sql();
            measure(&format!("cs_batch_{count}"), count, || {
                let conn = rusqlite::Connection::open_in_memory().unwrap();
                apply_pragmas_csqlite(&conn);
                conn.execute_batch(&format!("{create_sql};")).unwrap();
                let mut stmt = conn.prepare(insert_sql).unwrap();
                let num_batches = count.div_ceil(batch_size);
                #[allow(clippy::cast_possible_wrap)]
                for batch in 0..num_batches {
                    conn.execute_batch("BEGIN").unwrap();
                    let start = (batch * batch_size) as i64;
                    let end = ((batch + 1) * batch_size).min(count) as i64;
                    for i in start..end {
                        stmt.execute(rusqlite::params![i]).unwrap();
                    }
                    conn.execute_batch("COMMIT").unwrap();
                }
            })
        };

        let fs = {
            let create_sql = record_size.create_table_sql();
            measure(&format!("fs_batch_{count}"), count, || {
                // bd-zavyn: one runtime entry per timed sample.
                fsqlite_e2e::block_on(async {
                    let conn = open_fsqlite_memory_connection_for_benchmark_async().await;
                    apply_pragmas_fsqlite_async(&conn).await;
                    fs_execute_async(&conn, create_sql).await;
                    let stmt = fs_prepare_async(&conn, record_size.insert_sql_csqlite()).await;
                    let num_batches = count.div_ceil(batch_size);
                    #[allow(clippy::cast_possible_wrap)]
                    for batch in 0..num_batches {
                        fs_execute_async(&conn, "BEGIN").await;
                        let start = (batch * batch_size) as i64;
                        let end = ((batch + 1) * batch_size).min(count) as i64;
                        for i in start..end {
                            fs_stmt_execute_with_params_async(
                                &stmt,
                                &[fsqlite::SqliteValue::Integer(i)],
                            )
                            .await;
                        }
                        fs_execute_async(&conn, "COMMIT").await;
                    }
                });
            })
        };

        eprintln!(
            "C={} F={}",
            format_duration(cs.median()),
            format_duration(fs.median())
        );
        if profile_insert_enabled {
            profile_fsqlite_insert_with_strategy(
                record_size,
                count,
                "txn_batched",
                InsertProfileStrategy::Batched { batch_size },
            );
        }
        section.add_row(
            &format!("{count} rows / batched ({batch_size}/txn)"),
            Some(cs),
            Some(fs),
        );

        // --- Single txn ---
        eprint!("  Benchmarking single-txn {count} rows... ");

        let cs = {
            let insert_sql = record_size.insert_sql_csqlite();
            let create_sql = record_size.create_table_sql();
            measure(&format!("cs_txn_{count}"), count, || {
                let conn = rusqlite::Connection::open_in_memory().unwrap();
                apply_pragmas_csqlite(&conn);
                conn.execute_batch(&format!("{create_sql};")).unwrap();
                conn.execute_batch("BEGIN").unwrap();
                let mut stmt = conn.prepare(insert_sql).unwrap();
                #[allow(clippy::cast_possible_wrap)]
                for i in 0..count as i64 {
                    stmt.execute(rusqlite::params![i]).unwrap();
                }
                conn.execute_batch("COMMIT").unwrap();
            })
        };

        let fs = {
            let create_sql = record_size.create_table_sql();
            measure(&format!("fs_txn_{count}"), count, || {
                // bd-zavyn: one runtime entry per timed sample.
                fsqlite_e2e::block_on(async {
                    let conn = open_fsqlite_memory_connection_for_benchmark_async().await;
                    apply_pragmas_fsqlite_async(&conn).await;
                    fs_execute_async(&conn, create_sql).await;
                    fs_execute_async(&conn, "BEGIN").await;
                    #[allow(clippy::cast_possible_wrap)]
                    let stmt = fs_prepare_async(&conn, record_size.insert_sql_csqlite()).await;
                    for i in 0..count as i64 {
                        fs_stmt_execute_with_params_async(
                            &stmt,
                            &[fsqlite::SqliteValue::Integer(i)],
                        )
                        .await;
                    }
                    fs_execute_async(&conn, "COMMIT").await;
                });
            })
        };

        eprintln!(
            "C={} F={}",
            format_duration(cs.median()),
            format_duration(fs.median())
        );
        if profile_insert_enabled {
            profile_fsqlite_insert_with_strategy(
                record_size,
                count,
                "txn_single",
                InsertProfileStrategy::SingleTxn,
            );
        }
        section.add_row(&format!("{count} rows / single txn"), Some(cs), Some(fs));
    }
}

// ─── Section 3: Insert throughput by record size ───────────────────────

fn bench_insert_by_record_size(report: &mut BenchReport) {
    let section = report.add_section(
        "INSERTThroughput — Record Size Comparison (10K rows, single txn)",
        "Fixed 10K rows in a single transaction, varying payload size.",
    );

    let count = 10_000_usize;
    let profile_insert_enabled = bench_env_flag("FSQLITE_BENCH_PROFILE_INSERT");

    for &record_size in RecordSize::ALL {
        eprint!(
            "  Benchmarking 10K rows record size {}... ",
            record_size.name()
        );

        let cs = {
            let insert_sql = record_size.insert_sql_csqlite();
            let create_sql = record_size.create_table_sql();
            measure(&format!("cs_{}", record_size.name()), count, || {
                let conn = rusqlite::Connection::open_in_memory().unwrap();
                apply_pragmas_csqlite(&conn);
                conn.execute_batch(&format!("{create_sql};")).unwrap();
                conn.execute_batch("BEGIN").unwrap();
                let mut stmt = conn.prepare(insert_sql).unwrap();
                #[allow(clippy::cast_possible_wrap)]
                for i in 0..count as i64 {
                    stmt.execute(rusqlite::params![i]).unwrap();
                }
                conn.execute_batch("COMMIT").unwrap();
            })
        };

        let fs = {
            let create_sql = record_size.create_table_sql();
            measure(&format!("fs_{}", record_size.name()), count, || {
                // bd-zavyn: one runtime entry per timed sample.
                fsqlite_e2e::block_on(async {
                    let conn = open_fsqlite_memory_connection_for_benchmark_async().await;
                    apply_pragmas_fsqlite_async(&conn).await;
                    fs_execute_async(&conn, create_sql).await;
                    fs_execute_async(&conn, "BEGIN").await;
                    #[allow(clippy::cast_possible_wrap)]
                    let stmt = fs_prepare_async(&conn, record_size.insert_sql_csqlite()).await;
                    for i in 0..count as i64 {
                        fs_stmt_execute_with_params_async(
                            &stmt,
                            &[fsqlite::SqliteValue::Integer(i)],
                        )
                        .await;
                    }
                    fs_execute_async(&conn, "COMMIT").await;
                });
            })
        };
        if profile_insert_enabled {
            profile_fsqlite_insert(record_size, count, "record_size");
        }

        eprintln!(
            "C={} F={}",
            format_duration(cs.median()),
            format_duration(fs.median())
        );
        section.add_row(
            &format!("{} — {}", record_size.name(), record_size.description()),
            Some(cs),
            Some(fs),
        );
    }
}

fn metric_delta(after: u64, before: u64) -> u64 {
    after.saturating_sub(before)
}

#[allow(clippy::too_many_lines)]
fn build_fsqlite_concurrent_profile(
    total_rows: usize,
    fs_median: Duration,
    profile: &HotPathProfileSnapshot,
    wal_frames: u64,
    wal_bytes: u64,
    wal_group_commits: u64,
    wal_group_commit_latency_us: u64,
) -> JsonFsqliteConcurrentProfile {
    let mvcc = &profile.vdbe.mvcc_write_path;
    let page_data = &profile.vdbe.page_data_motion;
    let mut counters = BTreeMap::new();
    macro_rules! counter {
        ($name:literal, $value:expr) => {
            counters.insert($name.to_owned(), $value);
        };
    }

    counter!("direct_insert", profile.prepared_direct_insert_executions);
    counter!("fast", profile.parser.fast_path_executions);
    counter!("slow", profile.parser.slow_path_executions);
    counter!("begin_ns", profile.begin_setup_time_ns);
    counter!("execute_body_ns", profile.execute_body_time_ns);
    counter!("direct_flush_calls", profile.direct_write_flush_calls);
    counter!("direct_flush_ns", profile.direct_write_flush_time_ns);
    counter!(
        "page_run_flushes",
        profile.prepared_direct_insert_page_run_flushes
    );
    counter!(
        "page_run_records",
        profile.prepared_direct_insert_page_run_records
    );
    counter!(
        "page_run_bytes",
        profile.prepared_direct_insert_page_run_bytes
    );
    counter!(
        "page_run_owned",
        profile.prepared_direct_insert_page_run_owned_flushes
    );
    counter!(
        "page_run_arena",
        profile.prepared_direct_insert_page_run_arena_flushes
    );
    counter!(
        "page_run_repeated",
        profile.prepared_direct_insert_page_run_repeated_flushes
    );
    counter!(
        "page_run_depth2",
        profile.prepared_direct_insert_page_run_depth2_bulk_append_hits
    );
    counter!(
        "row_build_ns",
        profile.prepared_direct_insert_row_build_time_ns
    );
    counter!(
        "cursor_setup_ns",
        profile.prepared_direct_insert_cursor_setup_time_ns
    );
    counter!(
        "serialize_ns",
        profile.prepared_direct_insert_serialize_time_ns
    );
    counter!(
        "btree_insert_ns",
        profile.prepared_direct_insert_btree_insert_time_ns
    );
    counter!(
        "schema_validation_ns",
        profile.prepared_direct_insert_schema_validation_time_ns
    );
    counter!(
        "change_tracking_ns",
        profile.prepared_direct_insert_change_tracking_time_ns
    );
    counter!("commit_pre_ns", profile.commit_pre_txn_time_ns);
    counter!("commit_roundtrip_ns", profile.commit_txn_roundtrip_time_ns);
    counter!("pager_commit_calls", profile.pager_commit.commit_calls);
    counter!("pager_phase_a_ns", profile.pager_commit.phase_a_time_ns);
    counter!("pager_wal_ns", profile.pager_commit.wal_commit_time_ns);
    counter!(
        "pager_mem_flush_ns",
        profile.pager_commit.memory_flush_time_ns
    );
    counter!(
        "pager_journal_ns",
        profile.pager_commit.journal_commit_time_ns
    );
    counter!(
        "pager_c_metadata_ns",
        profile.pager_commit.phase_c_metadata_time_ns
    );
    counter!("pager_file_size_ns", profile.pager_commit.file_size_time_ns);
    counter!("pager_unlock_ns", profile.pager_commit.unlock_time_ns);
    counter!("pager_publish_ns", profile.pager_commit.publish_time_ns);
    counter!(
        "pager_cache_finish_ns",
        profile.pager_commit.cache_finish_time_ns
    );
    counter!("commit_finalize_ns", profile.commit_finalize_seq_time_ns);
    counter!("commit_handle_ns", profile.commit_handle_finalize_time_ns);
    counter!(
        "post_write_ns",
        profile.commit_post_write_maintenance_time_ns
    );
    counter!("finalize_post_ns", profile.finalize_post_publish_time_ns);
    counter!(
        "concurrent_plan_attempts",
        profile.concurrent_commit_plan_attempts
    );
    counter!(
        "concurrent_plan_successes",
        profile.concurrent_commit_plan_successes
    );
    counter!(
        "concurrent_plan_errors",
        profile.concurrent_commit_plan_errors
    );
    counter!(
        "concurrent_plan_busy_snapshot_errors",
        profile.concurrent_commit_plan_busy_snapshot_errors
    );
    counter!(
        "concurrent_plan_pending_pages",
        profile.concurrent_commit_plan_pending_pages
    );
    counter!(
        "concurrent_plan_write_pages",
        profile.concurrent_commit_plan_write_pages
    );
    counter!(
        "concurrent_plan_held_lock_pages",
        profile.concurrent_commit_plan_held_lock_pages
    );
    counter!(
        "concurrent_plan_uncontended_fast_paths",
        profile.concurrent_commit_plan_uncontended_fast_paths
    );
    counter!(
        "concurrent_plan_candidate_free_fast_paths",
        profile.concurrent_commit_plan_candidate_free_fast_paths
    );
    counter!(
        "concurrent_plan_full_validations",
        profile.concurrent_commit_plan_full_validations
    );
    counter!("parser_multi_calls", profile.parser.parse_multi_calls);
    counter!("parser_cache_hits", profile.parser.parse_cache_hits);
    counter!("parser_cache_misses", profile.parser.parse_cache_misses);
    counter!("parser_parse_ns", profile.parser.parse_time_ns);
    counter!("parser_rewrite_ns", profile.parser.rewrite_time_ns);
    counter!("bg_checks", profile.background_status_checks);
    counter!("bg_ns", profile.background_status_time_ns);
    counter!("op_cx_bg_gates", profile.op_cx_background_gates);
    counter!(
        "dispatch_bg_gates",
        profile.statement_dispatch_background_gates
    );
    counter!("pager_pub_refreshes", profile.pager_publication_refreshes);
    counter!("commit_refreshes", profile.commit_refresh_count);
    counter!("prepared_lookup_ns", profile.prepared_lookup_time_ns);
    counter!("memdb_refresh", profile.memdb_refresh_count);
    counter!("cached_write_reuses", profile.cached_write_txn_reuses);
    counter!("cached_write_parks", profile.cached_write_txn_parks);
    counter!("page_pool_hits", profile.page_buffer_pool_hits);
    counter!("page_pool_misses", profile.page_buffer_pool_misses);
    counter!("vdbe_opcodes", profile.vdbe.opcodes_executed_total);
    counter!("vdbe_statements", profile.vdbe.statements_total);
    counter!(
        "vdbe_statement_us",
        profile.vdbe.statement_duration_us_total
    );
    counter!("vdbe_make_record", profile.vdbe.make_record_calls_total);
    counter!("mvcc_tier0", mvcc.tier0_already_owned_writes_total);
    counter!("mvcc_tier1", mvcc.tier1_first_touch_writes_total);
    counter!("mvcc_tier2", mvcc.tier2_commit_surface_writes_total);
    counter!("mvcc_page_lock_waits", mvcc.page_lock_waits_total);
    counter!("mvcc_page_lock_wait_ns", mvcc.page_lock_wait_time_ns_total);
    counter!("mvcc_busy_retries", mvcc.write_busy_retries_total);
    counter!("mvcc_busy_timeouts", mvcc.write_busy_timeouts_total);
    counter!("mvcc_stale_snapshot", mvcc.stale_snapshot_rejects_total);
    counter!("mvcc_page_one_tracks", mvcc.page_one_conflict_tracks_total);
    counter!(
        "mvcc_page_one_track_ns",
        mvcc.page_one_conflict_track_time_ns_total
    );
    counter!(
        "mvcc_pending_clears",
        mvcc.pending_commit_surface_clears_total
    );
    counter!(
        "mvcc_pending_clear_ns",
        mvcc.pending_commit_surface_clear_time_ns_total
    );
    counter!(
        "page_data_borrowed_norm",
        page_data.borrowed_write_normalization_calls_total
    );
    counter!(
        "page_data_borrowed_exact_copies",
        page_data.borrowed_exact_size_copies_total
    );
    counter!(
        "page_data_owned_norm",
        page_data.owned_write_normalization_calls_total
    );
    counter!(
        "page_data_owned_passthrough",
        page_data.owned_passthrough_total
    );
    counter!(
        "page_data_owned_zero_extends",
        page_data.owned_in_place_zero_extends_total
    );
    counter!(
        "page_data_owned_resized_copies",
        page_data.owned_resized_copies_total
    );
    counter!(
        "page_data_payload_bytes",
        page_data.normalized_payload_bytes_total
    );
    counter!(
        "page_data_zero_fill_bytes",
        page_data.normalized_zero_fill_bytes_total
    );
    counter!("wal_frames", wal_frames);
    counter!("wal_bytes", wal_bytes);
    counter!("wal_group_commits", wal_group_commits);
    counter!("wal_group_commit_latency_us", wal_group_commit_latency_us);

    JsonFsqliteConcurrentProfile {
        total_rows,
        fsqlite_median_ms: duration_ms(fs_median),
        capture_scope: "fsqlite arm aggregate across warmups and measured iterations".to_owned(),
        counters,
    }
}

#[allow(clippy::too_many_arguments)]
fn print_fsqlite_concurrent_profile(
    n_threads: usize,
    total_rows: usize,
    fs_median: Duration,
    profile: &HotPathProfileSnapshot,
    wal_frames: u64,
    wal_bytes: u64,
    wal_group_commits: u64,
    wal_group_commit_latency_us: u64,
) {
    let mvcc = &profile.vdbe.mvcc_write_path;
    let page_data = &profile.vdbe.page_data_motion;
    eprintln!(
        "    [fs_concurrent_{n_threads}t] concurrent_profile rows={total_rows} fs_median={} direct_insert={} fast={} slow={} begin_ns={} execute_body_ns={} direct_flush_calls={} direct_flush_ns={} page_run_flushes={} page_run_records={} page_run_bytes={} page_run_owned={} page_run_arena={} page_run_repeated={} page_run_depth2={} row_build_ns={} cursor_setup_ns={} serialize_ns={} btree_insert_ns={} schema_validation_ns={} change_tracking_ns={} commit_pre_ns={} commit_roundtrip_ns={} pager_commit_calls={} pager_phase_a_ns={} pager_wal_ns={} pager_mem_flush_ns={} pager_journal_ns={} pager_c_metadata_ns={} pager_file_size_ns={} pager_unlock_ns={} pager_publish_ns={} pager_cache_finish_ns={} commit_finalize_ns={} commit_handle_ns={} post_write_ns={} finalize_post_ns={} concurrent_plan_attempts={} concurrent_plan_successes={} concurrent_plan_errors={} concurrent_plan_busy_snapshot_errors={} concurrent_plan_pending_pages={} concurrent_plan_write_pages={} concurrent_plan_held_lock_pages={} concurrent_plan_uncontended_fast_paths={} concurrent_plan_candidate_free_fast_paths={} concurrent_plan_full_validations={} parser_multi_calls={} parser_cache_hits={} parser_cache_misses={} parser_parse_ns={} parser_rewrite_ns={} bg_checks={} bg_ns={} op_cx_bg_gates={} dispatch_bg_gates={} pager_pub_refreshes={} commit_refreshes={} prepared_lookup_ns={} memdb_refresh={} cached_write_reuses={} cached_write_parks={} page_pool_hits={} page_pool_misses={} vdbe_opcodes={} vdbe_statements={} vdbe_statement_us={} vdbe_make_record={} mvcc_tier0={} mvcc_tier1={} mvcc_tier2={} mvcc_page_lock_waits={} mvcc_page_lock_wait_ns={} mvcc_busy_retries={} mvcc_busy_timeouts={} mvcc_stale_snapshot={} mvcc_page_one_tracks={} mvcc_page_one_track_ns={} mvcc_pending_clears={} mvcc_pending_clear_ns={} page_data_borrowed_norm={} page_data_borrowed_exact_copies={} page_data_owned_norm={} page_data_owned_passthrough={} page_data_owned_zero_extends={} page_data_owned_resized_copies={} page_data_payload_bytes={} page_data_zero_fill_bytes={} wal_frames={} wal_bytes={} wal_group_commits={} wal_group_commit_latency_us={}",
        format_duration(fs_median),
        profile.prepared_direct_insert_executions,
        profile.parser.fast_path_executions,
        profile.parser.slow_path_executions,
        profile.begin_setup_time_ns,
        profile.execute_body_time_ns,
        profile.direct_write_flush_calls,
        profile.direct_write_flush_time_ns,
        profile.prepared_direct_insert_page_run_flushes,
        profile.prepared_direct_insert_page_run_records,
        profile.prepared_direct_insert_page_run_bytes,
        profile.prepared_direct_insert_page_run_owned_flushes,
        profile.prepared_direct_insert_page_run_arena_flushes,
        profile.prepared_direct_insert_page_run_repeated_flushes,
        profile.prepared_direct_insert_page_run_depth2_bulk_append_hits,
        profile.prepared_direct_insert_row_build_time_ns,
        profile.prepared_direct_insert_cursor_setup_time_ns,
        profile.prepared_direct_insert_serialize_time_ns,
        profile.prepared_direct_insert_btree_insert_time_ns,
        profile.prepared_direct_insert_schema_validation_time_ns,
        profile.prepared_direct_insert_change_tracking_time_ns,
        profile.commit_pre_txn_time_ns,
        profile.commit_txn_roundtrip_time_ns,
        profile.pager_commit.commit_calls,
        profile.pager_commit.phase_a_time_ns,
        profile.pager_commit.wal_commit_time_ns,
        profile.pager_commit.memory_flush_time_ns,
        profile.pager_commit.journal_commit_time_ns,
        profile.pager_commit.phase_c_metadata_time_ns,
        profile.pager_commit.file_size_time_ns,
        profile.pager_commit.unlock_time_ns,
        profile.pager_commit.publish_time_ns,
        profile.pager_commit.cache_finish_time_ns,
        profile.commit_finalize_seq_time_ns,
        profile.commit_handle_finalize_time_ns,
        profile.commit_post_write_maintenance_time_ns,
        profile.finalize_post_publish_time_ns,
        profile.concurrent_commit_plan_attempts,
        profile.concurrent_commit_plan_successes,
        profile.concurrent_commit_plan_errors,
        profile.concurrent_commit_plan_busy_snapshot_errors,
        profile.concurrent_commit_plan_pending_pages,
        profile.concurrent_commit_plan_write_pages,
        profile.concurrent_commit_plan_held_lock_pages,
        profile.concurrent_commit_plan_uncontended_fast_paths,
        profile.concurrent_commit_plan_candidate_free_fast_paths,
        profile.concurrent_commit_plan_full_validations,
        profile.parser.parse_multi_calls,
        profile.parser.parse_cache_hits,
        profile.parser.parse_cache_misses,
        profile.parser.parse_time_ns,
        profile.parser.rewrite_time_ns,
        profile.background_status_checks,
        profile.background_status_time_ns,
        profile.op_cx_background_gates,
        profile.statement_dispatch_background_gates,
        profile.pager_publication_refreshes,
        profile.commit_refresh_count,
        profile.prepared_lookup_time_ns,
        profile.memdb_refresh_count,
        profile.cached_write_txn_reuses,
        profile.cached_write_txn_parks,
        profile.page_buffer_pool_hits,
        profile.page_buffer_pool_misses,
        profile.vdbe.opcodes_executed_total,
        profile.vdbe.statements_total,
        profile.vdbe.statement_duration_us_total,
        profile.vdbe.make_record_calls_total,
        mvcc.tier0_already_owned_writes_total,
        mvcc.tier1_first_touch_writes_total,
        mvcc.tier2_commit_surface_writes_total,
        mvcc.page_lock_waits_total,
        mvcc.page_lock_wait_time_ns_total,
        mvcc.write_busy_retries_total,
        mvcc.write_busy_timeouts_total,
        mvcc.stale_snapshot_rejects_total,
        mvcc.page_one_conflict_tracks_total,
        mvcc.page_one_conflict_track_time_ns_total,
        mvcc.pending_commit_surface_clears_total,
        mvcc.pending_commit_surface_clear_time_ns_total,
        page_data.borrowed_write_normalization_calls_total,
        page_data.borrowed_exact_size_copies_total,
        page_data.owned_write_normalization_calls_total,
        page_data.owned_passthrough_total,
        page_data.owned_in_place_zero_extends_total,
        page_data.owned_resized_copies_total,
        page_data.normalized_payload_bytes_total,
        page_data.normalized_zero_fill_bytes_total,
        wal_frames,
        wal_bytes,
        wal_group_commits,
        wal_group_commit_latency_us,
    );
}

#[derive(Clone, Copy)]
enum InsertProfileStrategy {
    Autocommit,
    Batched { batch_size: usize },
    SingleTxn,
}

fn profile_fsqlite_insert(record_size: RecordSize, count: usize, label: &str) {
    profile_fsqlite_insert_with_strategy(
        record_size,
        count,
        label,
        InsertProfileStrategy::SingleTxn,
    );
}

fn profile_fsqlite_insert_with_strategy(
    record_size: RecordSize,
    count: usize,
    label: &str,
    strategy: InsertProfileStrategy,
) {
    let conn = open_fsqlite_memory_connection_for_benchmark();
    apply_pragmas_fsqlite(&conn);

    let previous_hot_path_profile_enabled = hot_path_profile_enabled();
    set_hot_path_profile_enabled(true);
    reset_hot_path_profile();

    let setup_start = Instant::now();
    fs_execute(&conn, record_size.create_table_sql());
    let setup_us = setup_start.elapsed().as_secs_f64() * 1_000_000.0;

    reset_hot_path_profile();
    let wal_before = fsqlite_wal::wal_telemetry_snapshot();

    let mut begin_us = if matches!(strategy, InsertProfileStrategy::SingleTxn) {
        let begin_start = Instant::now();
        fs_execute(&conn, "BEGIN");
        begin_start.elapsed().as_secs_f64() * 1_000_000.0
    } else {
        0.0
    };

    let prepare_start = Instant::now();
    let statement = fs_prepare(&conn, record_size.insert_sql_csqlite());
    let prepare_us = prepare_start.elapsed().as_secs_f64() * 1_000_000.0;

    let mut insert_us = 0.0;
    let mut commit_us = 0.0;
    #[allow(clippy::cast_possible_wrap)]
    match strategy {
        InsertProfileStrategy::Autocommit | InsertProfileStrategy::SingleTxn => {
            let insert_start = Instant::now();
            for i in 0..count as i64 {
                fs_stmt_execute_with_params(&statement, &[fsqlite::SqliteValue::Integer(i)]);
            }
            insert_us = insert_start.elapsed().as_secs_f64() * 1_000_000.0;
        }
        InsertProfileStrategy::Batched { batch_size } => {
            let batch_size = batch_size.max(1);
            let num_batches = count.div_ceil(batch_size);
            for batch in 0..num_batches {
                let begin_start = Instant::now();
                fs_execute(&conn, "BEGIN");
                begin_us = begin_start
                    .elapsed()
                    .as_secs_f64()
                    .mul_add(1_000_000.0, begin_us);

                let start = (batch * batch_size) as i64;
                let end = ((batch + 1) * batch_size).min(count) as i64;
                let insert_start = Instant::now();
                for i in start..end {
                    fs_stmt_execute_with_params(&statement, &[fsqlite::SqliteValue::Integer(i)]);
                }
                insert_us = insert_start
                    .elapsed()
                    .as_secs_f64()
                    .mul_add(1_000_000.0, insert_us);

                let commit_start = Instant::now();
                fs_execute(&conn, "COMMIT");
                commit_us = commit_start
                    .elapsed()
                    .as_secs_f64()
                    .mul_add(1_000_000.0, commit_us);
            }
        }
    }

    if matches!(strategy, InsertProfileStrategy::SingleTxn) {
        let commit_start = Instant::now();
        fs_execute(&conn, "COMMIT");
        commit_us = commit_start.elapsed().as_secs_f64() * 1_000_000.0;
    }

    let wal_after = fsqlite_wal::wal_telemetry_snapshot();
    let wal_frames = metric_delta(
        wal_after.wal.frames_written_total,
        wal_before.wal.frames_written_total,
    );
    let wal_bytes = metric_delta(
        wal_after.wal.bytes_written_total,
        wal_before.wal.bytes_written_total,
    );
    let wal_group_commits = metric_delta(
        wal_after.group_commit.group_commits_total,
        wal_before.group_commit.group_commits_total,
    );
    let wal_group_commit_size_sum = metric_delta(
        wal_after.group_commit.group_commit_size_sum,
        wal_before.group_commit.group_commit_size_sum,
    );
    let wal_group_commit_latency_us = metric_delta(
        wal_after.group_commit.commit_latency_us_total,
        wal_before.group_commit.commit_latency_us_total,
    );
    let commit_prepare_us = metric_delta(
        wal_after.consolidation.prepare_us_total,
        wal_before.consolidation.prepare_us_total,
    );
    let commit_batch_build_us = metric_delta(
        wal_after.consolidation.batch_build_us_total,
        wal_before.consolidation.batch_build_us_total,
    );
    let commit_conflict_snapshot_us = metric_delta(
        wal_after.consolidation.conflict_snapshot_us_total,
        wal_before.consolidation.conflict_snapshot_us_total,
    );
    let commit_lane_prepare_us = metric_delta(
        wal_after.consolidation.lane_prepare_us_total,
        wal_before.consolidation.lane_prepare_us_total,
    );
    let commit_consolidator_lock_wait_us = metric_delta(
        wal_after.consolidation.consolidator_lock_wait_us_total,
        wal_before.consolidation.consolidator_lock_wait_us_total,
    );
    let commit_consolidator_flushing_wait_us = metric_delta(
        wal_after.consolidation.consolidator_flushing_wait_us_total,
        wal_before.consolidation.consolidator_flushing_wait_us_total,
    );
    let commit_flusher_arrival_wait_us = metric_delta(
        wal_after.consolidation.flusher_arrival_wait_us_total,
        wal_before.consolidation.flusher_arrival_wait_us_total,
    );
    let commit_wal_backend_lock_wait_us = metric_delta(
        wal_after.consolidation.inner_lock_wait_us_total,
        wal_before.consolidation.inner_lock_wait_us_total,
    );
    let commit_exclusive_lock_us = metric_delta(
        wal_after.consolidation.exclusive_lock_us_total,
        wal_before.consolidation.exclusive_lock_us_total,
    );
    let commit_wal_append_us = metric_delta(
        wal_after.consolidation.wal_append_us_total,
        wal_before.consolidation.wal_append_us_total,
    );
    let commit_flush_frame_prep_us = metric_delta(
        wal_after.consolidation.flush_frame_prep_us_total,
        wal_before.consolidation.flush_frame_prep_us_total,
    );
    let commit_append_conflict_check_us = metric_delta(
        wal_after.consolidation.append_conflict_check_us_total,
        wal_before.consolidation.append_conflict_check_us_total,
    );
    let commit_append_frames_us = metric_delta(
        wal_after.consolidation.append_frames_us_total,
        wal_before.consolidation.append_frames_us_total,
    );
    let commit_wal_sync_us = metric_delta(
        wal_after.consolidation.wal_sync_us_total,
        wal_before.consolidation.wal_sync_us_total,
    );
    let commit_waiter_epoch_wait_us = metric_delta(
        wal_after.consolidation.waiter_epoch_wait_us_total,
        wal_before.consolidation.waiter_epoch_wait_us_total,
    );
    let commit_flusher_commits = metric_delta(
        wal_after.consolidation.flusher_commits,
        wal_before.consolidation.flusher_commits,
    );
    let commit_waiter_commits = metric_delta(
        wal_after.consolidation.waiter_commits,
        wal_before.consolidation.waiter_commits,
    );
    let commit_phase_a_us = metric_delta(
        wal_after.consolidation.commit_phase_a_us_total,
        wal_before.consolidation.commit_phase_a_us_total,
    );
    let commit_phase_b_us = metric_delta(
        wal_after.consolidation.commit_phase_b_us_total,
        wal_before.consolidation.commit_phase_b_us_total,
    );
    let commit_phase_c1_us = metric_delta(
        wal_after.consolidation.commit_phase_c1_us_total,
        wal_before.consolidation.commit_phase_c1_us_total,
    );
    let commit_phase_c2_us = metric_delta(
        wal_after.consolidation.commit_phase_c2_us_total,
        wal_before.consolidation.commit_phase_c2_us_total,
    );
    let commit_phase_count = metric_delta(
        wal_after.consolidation.commit_phase_count,
        wal_before.consolidation.commit_phase_count,
    );
    let commit_flusher_lock_wait_us = commit_consolidator_flushing_wait_us
        .saturating_add(commit_wal_backend_lock_wait_us)
        .saturating_add(commit_exclusive_lock_us);
    let commit_wal_service_us = commit_wal_append_us.saturating_add(commit_wal_sync_us);

    let profile = hot_path_profile_snapshot();
    set_hot_path_profile_enabled(previous_hot_path_profile_enabled);

    eprintln!(
        "    [fs_insert_{}_{}_{count}] insert_profile setup_us={setup_us:.1} begin_us={begin_us:.1} prepare_us={prepare_us:.1} insert_us={insert_us:.1} commit_us={commit_us:.1} rows={count} direct_insert={} fast={} slow={} schema_refreshes={} schema_refresh_ns={} begin_ns={} execute_body_ns={} direct_flush_calls={} direct_flush_ns={} page_run_flushes={} page_run_records={} page_run_bytes={} page_run_owned={} page_run_arena={} page_run_repeated={} page_run_empty_root={} page_run_depth2={} page_run_fallbacks={} page_run_fallback_rows={} commit_pre_ns={} commit_roundtrip_ns={} pager_commit_calls={} pager_phase_a_ns={} pager_wal_ns={} pager_mem_flush_ns={} pager_journal_ns={} pager_c_metadata_ns={} pager_file_size_ns={} pager_unlock_ns={} pager_publish_ns={} pager_cache_finish_ns={} commit_finalize_ns={} commit_handle_ns={} post_write_ns={} finalize_post_ns={} parser_multi_calls={} parser_cache_hits={} parser_cache_misses={} parser_parse_ns={} parser_rewrite_ns={} bg_checks={} bg_ns={} op_cx_bg_gates={} dispatch_bg_gates={} pager_pub_refreshes={} commit_refreshes={} prepared_lookup_ns={} memdb_refresh={} cached_write_reuses={} cached_write_parks={} page_pool_hits={} page_pool_misses={} row_build_ns={} preserialize_ns={} preserialize_cell_ns={} preserialize_eval_ns={} preserialize_affinity_ns={} preserialize_layout_ns={} preserialize_encode_ns={} row_value_build_ns={} cursor_setup_ns={} serialize_ns={} btree_insert_ns={} memdb_apply_ns={} schema_validation_ns={} autocommit_begin_ns={} autocommit_resolve_ns={} autocommit_executions={} change_tracking_ns={} record_parse_into={} record_decode_ns={} btree_payload_copy_calls={} btree_payload_copy_bytes={} btree_cell_assembly_calls={} btree_cell_assembly_bytes={} btree_leaf_payload_appends={} btree_leaf_payload_mutate_ns={} btree_leaf_payload_stage_ns={} btree_leaf_full_cell_appends={} btree_leaf_full_cell_mutate_ns={} btree_leaf_full_cell_stage_ns={} btree_quick_balance_attempts={} btree_quick_balance_hits={} btree_quick_balance_ns={} btree_local_split_attempts={} btree_local_split_hits={} btree_local_split_ns={} btree_nonroot_balance_calls={} btree_nonroot_balance_ns={} btree_bulk_group={}/{} btree_bulk_leaf_build={}/{} btree_bulk_leaf_write={}/{} btree_bulk_interior_build={}/{} btree_bulk_interior_write={}/{} btree_no_split_reuse_hits={} btree_conservative_reloads={} btree_page_header_rebuilds={} vdbe_opcodes={} vdbe_statements={} vdbe_make_record={} wal_frames={} wal_bytes={} wal_group_commits={} wal_group_commit_size_sum={} wal_group_commit_latency_us={} commit_prepare_us={} commit_batch_build_us={} commit_conflict_snapshot_us={} commit_lane_prepare_us={} commit_consolidator_lock_wait_us={} commit_consolidator_flushing_wait_us={} commit_flusher_arrival_wait_us={} commit_wal_backend_lock_wait_us={} commit_exclusive_lock_us={} commit_wal_append_us={} commit_flush_frame_prep_us={} commit_append_conflict_check_us={} commit_append_frames_us={} commit_wal_sync_us={} commit_waiter_epoch_wait_us={} commit_flusher_commits={} commit_waiter_commits={} commit_phase_a_us={} commit_phase_b_us={} commit_phase_c1_us={} commit_phase_c2_us={} commit_phase_count={} commit_flusher_lock_wait_us={} commit_wal_service_us={}",
        label,
        record_size.name(),
        profile.prepared_direct_insert_executions,
        profile.parser.fast_path_executions,
        profile.parser.slow_path_executions,
        profile.prepared_schema_refreshes,
        profile.prepared_schema_refresh_time_ns,
        profile.begin_setup_time_ns,
        profile.execute_body_time_ns,
        profile.direct_write_flush_calls,
        profile.direct_write_flush_time_ns,
        profile.prepared_direct_insert_page_run_flushes,
        profile.prepared_direct_insert_page_run_records,
        profile.prepared_direct_insert_page_run_bytes,
        profile.prepared_direct_insert_page_run_owned_flushes,
        profile.prepared_direct_insert_page_run_arena_flushes,
        profile.prepared_direct_insert_page_run_repeated_flushes,
        profile.prepared_direct_insert_page_run_empty_root_bulk_load_hits,
        profile.prepared_direct_insert_page_run_depth2_bulk_append_hits,
        profile.prepared_direct_insert_page_run_row_append_fallback_flushes,
        profile.prepared_direct_insert_page_run_row_append_fallback_rows,
        profile.commit_pre_txn_time_ns,
        profile.commit_txn_roundtrip_time_ns,
        profile.pager_commit.commit_calls,
        profile.pager_commit.phase_a_time_ns,
        profile.pager_commit.wal_commit_time_ns,
        profile.pager_commit.memory_flush_time_ns,
        profile.pager_commit.journal_commit_time_ns,
        profile.pager_commit.phase_c_metadata_time_ns,
        profile.pager_commit.file_size_time_ns,
        profile.pager_commit.unlock_time_ns,
        profile.pager_commit.publish_time_ns,
        profile.pager_commit.cache_finish_time_ns,
        profile.commit_finalize_seq_time_ns,
        profile.commit_handle_finalize_time_ns,
        profile.commit_post_write_maintenance_time_ns,
        profile.finalize_post_publish_time_ns,
        profile.parser.parse_multi_calls,
        profile.parser.parse_cache_hits,
        profile.parser.parse_cache_misses,
        profile.parser.parse_time_ns,
        profile.parser.rewrite_time_ns,
        profile.background_status_checks,
        profile.background_status_time_ns,
        profile.op_cx_background_gates,
        profile.statement_dispatch_background_gates,
        profile.pager_publication_refreshes,
        profile.commit_refresh_count,
        profile.prepared_lookup_time_ns,
        profile.memdb_refresh_count,
        profile.cached_write_txn_reuses,
        profile.cached_write_txn_parks,
        profile.page_buffer_pool_hits,
        profile.page_buffer_pool_misses,
        profile.prepared_direct_insert_row_build_time_ns,
        profile.prepared_direct_insert_preserialize_time_ns,
        profile.prepared_direct_insert_preserialize_cell_time_ns,
        profile.prepared_direct_insert_preserialize_eval_time_ns,
        profile.prepared_direct_insert_preserialize_affinity_time_ns,
        profile.prepared_direct_insert_preserialize_layout_time_ns,
        profile.prepared_direct_insert_preserialize_encode_time_ns,
        profile.prepared_direct_insert_row_value_build_time_ns,
        profile.prepared_direct_insert_cursor_setup_time_ns,
        profile.prepared_direct_insert_serialize_time_ns,
        profile.prepared_direct_insert_btree_insert_time_ns,
        profile.prepared_direct_insert_memdb_apply_time_ns,
        profile.prepared_direct_insert_schema_validation_time_ns,
        profile.prepared_direct_insert_autocommit_begin_time_ns,
        profile.prepared_direct_insert_autocommit_resolve_time_ns,
        profile.prepared_direct_insert_autocommit_executions,
        profile.prepared_direct_insert_change_tracking_time_ns,
        profile.record_decode.parse_record_into_calls,
        profile.record_decode.decode_time_ns,
        profile.btree_copy_kernels.local_payload_copy_calls,
        profile.btree_copy_kernels.local_payload_copy_bytes,
        profile.btree_copy_kernels.table_leaf_cell_assembly_calls,
        profile.btree_copy_kernels.table_leaf_cell_assembly_bytes,
        profile.btree_leaf_reuse.fast_table_leaf_payload_appends,
        profile
            .btree_leaf_reuse
            .fast_table_leaf_payload_mutate_time_ns,
        profile
            .btree_leaf_reuse
            .fast_table_leaf_payload_stage_time_ns,
        profile.btree_leaf_reuse.fast_table_leaf_full_cell_appends,
        profile
            .btree_leaf_reuse
            .fast_table_leaf_full_cell_mutate_time_ns,
        profile
            .btree_leaf_reuse
            .fast_table_leaf_full_cell_stage_time_ns,
        profile.btree_leaf_reuse.quick_balance_attempts,
        profile.btree_leaf_reuse.quick_balance_hits,
        profile.btree_leaf_reuse.quick_balance_time_ns,
        profile.btree_leaf_reuse.local_split_attempts,
        profile.btree_leaf_reuse.local_split_hits,
        profile.btree_leaf_reuse.local_split_time_ns,
        profile.btree_leaf_reuse.nonroot_balance_calls,
        profile.btree_leaf_reuse.nonroot_balance_time_ns,
        profile.btree_leaf_reuse.bulk_table_grouping_calls,
        profile.btree_leaf_reuse.bulk_table_grouping_time_ns,
        profile.btree_leaf_reuse.bulk_table_leaf_page_build_calls,
        profile.btree_leaf_reuse.bulk_table_leaf_page_build_time_ns,
        profile.btree_leaf_reuse.bulk_table_leaf_page_write_calls,
        profile.btree_leaf_reuse.bulk_table_leaf_page_write_time_ns,
        profile
            .btree_leaf_reuse
            .bulk_table_interior_page_build_calls,
        profile
            .btree_leaf_reuse
            .bulk_table_interior_page_build_time_ns,
        profile
            .btree_leaf_reuse
            .bulk_table_interior_page_write_calls,
        profile
            .btree_leaf_reuse
            .bulk_table_interior_page_write_time_ns,
        profile.btree_leaf_reuse.no_split_reuse_hits,
        profile.btree_leaf_reuse.conservative_reload_fallbacks,
        profile.btree_leaf_reuse.page_header_rebuild_count,
        profile.vdbe.opcodes_executed_total,
        profile.vdbe.statements_total,
        profile.vdbe.make_record_calls_total,
        wal_frames,
        wal_bytes,
        wal_group_commits,
        wal_group_commit_size_sum,
        wal_group_commit_latency_us,
        commit_prepare_us,
        commit_batch_build_us,
        commit_conflict_snapshot_us,
        commit_lane_prepare_us,
        commit_consolidator_lock_wait_us,
        commit_consolidator_flushing_wait_us,
        commit_flusher_arrival_wait_us,
        commit_wal_backend_lock_wait_us,
        commit_exclusive_lock_us,
        commit_wal_append_us,
        commit_flush_frame_prep_us,
        commit_append_conflict_check_us,
        commit_append_frames_us,
        commit_wal_sync_us,
        commit_waiter_epoch_wait_us,
        commit_flusher_commits,
        commit_waiter_commits,
        commit_phase_a_us,
        commit_phase_b_us,
        commit_phase_c1_us,
        commit_phase_c2_us,
        commit_phase_count,
        commit_flusher_lock_wait_us,
        commit_wal_service_us,
    );
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConcurrentGatePhase {
    Preparing,
    Run,
    Postflight,
    Abort,
}

struct ConcurrentSampleGate {
    phase: Mutex<ConcurrentGatePhase>,
    changed: Condvar,
}

impl ConcurrentSampleGate {
    fn new() -> Self {
        Self {
            phase: Mutex::new(ConcurrentGatePhase::Preparing),
            changed: Condvar::new(),
        }
    }

    fn transition(
        &self,
        expected: ConcurrentGatePhase,
        next: ConcurrentGatePhase,
    ) -> Result<(), String> {
        let mut phase = self
            .phase
            .lock()
            .map_err(|_| "concurrent sample gate mutex was poisoned".to_owned())?;
        if *phase != expected {
            return Err(format!(
                "concurrent sample gate expected {expected:?}, observed {phase:?}"
            ));
        }
        *phase = next;
        self.changed.notify_all();
        Ok(())
    }

    fn wait_for(&self, target: ConcurrentGatePhase) -> Result<(), String> {
        let mut phase = self
            .phase
            .lock()
            .map_err(|_| "concurrent sample gate mutex was poisoned".to_owned())?;
        loop {
            match *phase {
                ConcurrentGatePhase::Abort => {
                    return Err("concurrent sample was aborted".to_owned());
                }
                current if current == target => return Ok(()),
                ConcurrentGatePhase::Preparing | ConcurrentGatePhase::Run => {
                    phase = self
                        .changed
                        .wait(phase)
                        .map_err(|_| "concurrent sample gate mutex was poisoned".to_owned())?;
                }
                ConcurrentGatePhase::Postflight => {
                    return Err(format!(
                        "concurrent sample gate advanced to postflight before worker observed {target:?}"
                    ));
                }
            }
        }
    }

    fn abort(&self) {
        if let Ok(mut phase) = self.phase.lock() {
            *phase = ConcurrentGatePhase::Abort;
            self.changed.notify_all();
        }
    }
}

#[derive(Debug)]
struct ConcurrentWorkerSetup {
    worker_index: usize,
    thread_id: String,
    cpu_affinity: String,
    settings: JsonConcurrentWorkerSettings,
}

#[derive(Debug)]
struct ConcurrentWorkerPostflight {
    worker_index: usize,
    thread_id: String,
    cpu_affinity: String,
    settings: JsonConcurrentWorkerSettings,
}

#[derive(Debug)]
enum ConcurrentWorkerEvent {
    Setup {
        worker_index: usize,
        outcome: Result<ConcurrentWorkerSetup, String>,
    },
    Done {
        worker_index: usize,
        outcome: Result<usize, String>,
    },
    Postflight {
        worker_index: usize,
        outcome: Result<ConcurrentWorkerPostflight, String>,
    },
}

fn current_thread_identity() -> String {
    format!("{:?}", std::thread::current().id())
}

fn current_thread_cpu_affinity() -> Result<String, String> {
    std::fs::read_to_string("/proc/thread-self/status")
        .map_err(|error| format!("could not read /proc/thread-self/status: {error}"))?
        .lines()
        .find_map(|line| {
            line.strip_prefix("Cpus_allowed_list:")
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_owned)
        })
        .ok_or_else(|| "/proc/thread-self/status did not contain Cpus_allowed_list".to_owned())
}

fn concurrent_expected_cpu_affinity() -> Result<String, String> {
    let observed = cpu_affinity()
        .ok_or_else(|| "could not determine benchmark process CPU affinity".to_owned())?;
    if let Ok(expected) = std::env::var("FSQLITE_BENCH_EXPECTED_CPU_AFFINITY")
        && expected != observed
    {
        return Err(format!(
            "benchmark process CPU affinity mismatch: expected `{expected}`, observed `{observed}`"
        ));
    }
    Ok(observed)
}

fn normalized_synchronous(value: &str) -> Result<String, String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "0" | "off" => Ok("off".to_owned()),
        "1" | "normal" => Ok("normal".to_owned()),
        "2" | "full" => Ok("full".to_owned()),
        "3" | "extra" => Ok("extra".to_owned()),
        _ => Err(format!("unrecognized PRAGMA synchronous value `{value}`")),
    }
}

fn parse_worker_settings(
    profile: &BTreeMap<String, String>,
    concurrent_mode: &str,
) -> Result<JsonConcurrentWorkerSettings, String> {
    let value = |name: &str| {
        profile
            .get(name)
            .map(String::as_str)
            .ok_or_else(|| format!("worker PRAGMA profile omitted `{name}`"))
    };
    Ok(JsonConcurrentWorkerSettings {
        page_size_bytes: value("page_size")?
            .parse::<u32>()
            .map_err(|error| format!("invalid worker page_size: {error}"))?,
        journal_mode: value("journal_mode")?.to_ascii_lowercase(),
        synchronous: normalized_synchronous(value("synchronous")?)?,
        cache_size: value("cache_size")?
            .parse::<i64>()
            .map_err(|error| format!("invalid worker cache_size: {error}"))?,
        busy_timeout_ms: value("busy_timeout")?
            .parse::<u64>()
            .map_err(|error| format!("invalid worker busy_timeout: {error}"))?,
        concurrent_mode: concurrent_mode.to_owned(),
    })
}

fn expected_concurrent_worker_settings(concurrent_mode: &str) -> JsonConcurrentWorkerSettings {
    JsonConcurrentWorkerSettings {
        page_size_bytes: benchmark_page_size_bytes(),
        journal_mode: "wal".to_owned(),
        synchronous: concurrent_sync_mode().to_ascii_lowercase(),
        cache_size: -64_000,
        busy_timeout_ms: 5_000,
        concurrent_mode: concurrent_mode.to_owned(),
    }
}

fn verify_concurrent_worker_settings(
    engine: &str,
    worker_index: usize,
    actual: &JsonConcurrentWorkerSettings,
    concurrent_mode: &str,
) -> Result<(), String> {
    let expected = expected_concurrent_worker_settings(concurrent_mode);
    if actual != &expected {
        return Err(format!(
            "{engine} worker {worker_index} settings mismatch: expected {expected:?}, observed {actual:?}"
        ));
    }
    Ok(())
}

fn query_csqlite_concurrent_worker_settings(
    conn: &rusqlite::Connection,
) -> Result<JsonConcurrentWorkerSettings, String> {
    let mut profile = query_effective_csqlite_pragmas(conn)?;
    let busy_timeout = conn
        .query_row("PRAGMA busy_timeout;", [], |row| {
            row.get_ref(0).map(normalize_csqlite_value)
        })
        .map_err(|error| format!("C SQLite `PRAGMA busy_timeout` failed: {error}"))?;
    profile.insert("busy_timeout".to_owned(), busy_timeout);
    parse_worker_settings(&profile, "sqlite_wal_single_writer")
}

fn query_fsqlite_scalar(conn: &fsqlite::Connection, sql: &str) -> Result<String, String> {
    let rows = fsqlite_e2e::block_on(conn.query(sql))
        .map_err(|error| format!("FrankenSQLite `{sql}` failed: {error}"))?;
    let row = rows
        .first()
        .ok_or_else(|| format!("FrankenSQLite `{sql}` returned no row"))?;
    let value = row
        .get(0)
        .ok_or_else(|| format!("FrankenSQLite `{sql}` returned no first column"))?;
    Ok(normalize_fsqlite_value(value))
}

fn query_fsqlite_concurrent_worker_settings(
    conn: &fsqlite::Connection,
) -> Result<JsonConcurrentWorkerSettings, String> {
    if !conn.is_concurrent_mode_default() {
        return Err("FrankenSQLite concurrent-writer mode is not enabled".to_owned());
    }
    let mut profile = query_effective_fsqlite_pragmas(conn)?;
    profile.insert(
        "busy_timeout".to_owned(),
        query_fsqlite_scalar(conn, "PRAGMA busy_timeout;")?,
    );
    let pragma_mode = query_fsqlite_scalar(conn, "PRAGMA fsqlite.concurrent_mode;")?;
    if !matches!(pragma_mode.as_str(), "1" | "true" | "on") {
        return Err(format!(
            "FrankenSQLite concurrent-mode readback was `{pragma_mode}`, expected enabled"
        ));
    }
    parse_worker_settings(&profile, "fsqlite_mvcc_on")
}

fn configure_csqlite_concurrent_worker(
    conn: &rusqlite::Connection,
) -> Result<JsonConcurrentWorkerSettings, String> {
    conn.execute_batch(&format!(
        "PRAGMA page_size={};\
         PRAGMA journal_mode=WAL;\
         PRAGMA synchronous={};\
         PRAGMA cache_size=-64000;\
         PRAGMA busy_timeout=5000;",
        benchmark_page_size_bytes(),
        concurrent_sync_mode()
    ))
    .map_err(|error| format!("failed to configure C SQLite worker: {error}"))?;
    query_csqlite_concurrent_worker_settings(conn)
}

fn configure_fsqlite_concurrent_worker(
    conn: &fsqlite::Connection,
) -> Result<JsonConcurrentWorkerSettings, String> {
    for pragma in [
        format!("PRAGMA page_size={};", benchmark_page_size_bytes()),
        "PRAGMA journal_mode=WAL;".to_owned(),
        format!("PRAGMA synchronous={};", concurrent_sync_mode()),
        "PRAGMA cache_size=-64000;".to_owned(),
        "PRAGMA fsqlite_capture_time_travel_snapshots=false;".to_owned(),
        "PRAGMA fsqlite.concurrent_mode=ON;".to_owned(),
        "PRAGMA busy_timeout=5000;".to_owned(),
    ] {
        fsqlite_e2e::block_on(conn.execute(&pragma)).map_err(|error| {
            format!("failed to configure FrankenSQLite worker `{pragma}`: {error}")
        })?;
    }
    query_fsqlite_concurrent_worker_settings(conn)
}

fn send_concurrent_event(
    sender: &mpsc::SyncSender<ConcurrentWorkerEvent>,
    event: ConcurrentWorkerEvent,
) -> Result<(), String> {
    sender
        .send(event)
        .map_err(|error| format!("concurrent coordinator stopped receiving events: {error}"))
}

fn csqlite_concurrent_worker(
    path: &str,
    worker_index: usize,
    gate: &ConcurrentSampleGate,
    sender: &mpsc::SyncSender<ConcurrentWorkerEvent>,
) -> Result<(), String> {
    let setup = (|| {
        let conn = rusqlite::Connection::open(path)
            .map_err(|error| format!("C SQLite worker {worker_index} open failed: {error}"))?;
        let settings = configure_csqlite_concurrent_worker(&conn)?;
        verify_concurrent_worker_settings(
            "C SQLite",
            worker_index,
            &settings,
            "sqlite_wal_single_writer",
        )?;
        let setup = ConcurrentWorkerSetup {
            worker_index,
            thread_id: current_thread_identity(),
            cpu_affinity: current_thread_cpu_affinity()?,
            settings,
        };
        Ok::<_, String>((conn, setup))
    })();
    let (conn, setup) = match setup {
        Ok(setup) => setup,
        Err(error) => {
            send_concurrent_event(
                sender,
                ConcurrentWorkerEvent::Setup {
                    worker_index,
                    outcome: Err(error.clone()),
                },
            )?;
            return Err(error);
        }
    };
    send_concurrent_event(
        sender,
        ConcurrentWorkerEvent::Setup {
            worker_index,
            outcome: Ok(setup),
        },
    )?;
    gate.wait_for(ConcurrentGatePhase::Run)?;

    let transaction = (|| {
        conn.execute_batch("BEGIN")
            .map_err(|error| format!("C SQLite worker {worker_index} BEGIN failed: {error}"))?;
        #[allow(clippy::cast_possible_wrap)]
        let base = worker_index as i64 * CONCURRENT_RANGE_SIZE;
        {
            let mut statement = conn
                .prepare("INSERT INTO bench VALUES (?1, ('t' || ?1), (?1 * 7))")
                .map_err(|error| {
                    format!("C SQLite worker {worker_index} prepare failed: {error}")
                })?;
            #[allow(clippy::cast_possible_wrap)]
            for row_index in 0..CONCURRENT_ROWS_PER_THREAD as i64 {
                statement
                    .execute(rusqlite::params![base + row_index])
                    .map_err(|error| {
                        format!("C SQLite worker {worker_index} INSERT {row_index} failed: {error}")
                    })?;
            }
        }
        conn.execute_batch("COMMIT")
            .map_err(|error| format!("C SQLite worker {worker_index} COMMIT failed: {error}"))?;
        Ok::<_, String>(CONCURRENT_ROWS_PER_THREAD)
    })();
    match transaction {
        Ok(completed_rows) => send_concurrent_event(
            sender,
            ConcurrentWorkerEvent::Done {
                worker_index,
                outcome: Ok(completed_rows),
            },
        )?,
        Err(error) => {
            let _ = conn.execute_batch("ROLLBACK");
            send_concurrent_event(
                sender,
                ConcurrentWorkerEvent::Done {
                    worker_index,
                    outcome: Err(error.clone()),
                },
            )?;
            return Err(error);
        }
    }

    gate.wait_for(ConcurrentGatePhase::Postflight)?;
    let postflight = (|| {
        let settings = query_csqlite_concurrent_worker_settings(&conn)?;
        verify_concurrent_worker_settings(
            "C SQLite postflight",
            worker_index,
            &settings,
            "sqlite_wal_single_writer",
        )?;
        Ok::<_, String>(ConcurrentWorkerPostflight {
            worker_index,
            thread_id: current_thread_identity(),
            cpu_affinity: current_thread_cpu_affinity()?,
            settings,
        })
    })();
    let failed = postflight.as_ref().err().cloned();
    send_concurrent_event(
        sender,
        ConcurrentWorkerEvent::Postflight {
            worker_index,
            outcome: postflight,
        },
    )?;
    failed.map_or(Ok(()), Err)
}

/// Outcome of one concurrent-transaction attempt (bd-zavyn).
enum ConcurrentTxnAttempt {
    Committed,
    /// A transient BEGIN/INSERT/COMMIT failure that already rolled back
    /// (where required) inside the runtime entry; the caller backs off
    /// outside the runtime and retries.
    Retry,
}

/// bd-zavyn: one runtime entry per transaction attempt. The previous shape
/// entered the harness runtime for every BEGIN/prepare/row/COMMIT/ROLLBACK
/// (`CONCURRENT_ROWS_PER_THREAD + 2` entries per attempt per thread, all
/// inside the gate-released timed window, FrankenSQLite side only). The
/// transient-retry backoff sleeps *outside* the entered runtime (Gate 0
/// requirement: never hold a sync sleep inside a current-thread runtime
/// that owns engine progress).
fn execute_fsqlite_concurrent_transaction(
    conn: &fsqlite::Connection,
    worker_index: usize,
) -> Result<usize, String> {
    #[allow(clippy::cast_possible_wrap)]
    let base = worker_index as i64 * CONCURRENT_RANGE_SIZE;
    let mut retry_count = 0_u32;
    const TXN_MAX_RETRIES: u32 = 128;
    let jitter_salt = u64::try_from(worker_index).map_or(u64::MAX, |value| value.saturating_add(1));
    loop {
        let outcome = fsqlite_e2e::block_on(async {
            if let Err(error) = conn.execute("BEGIN CONCURRENT").await {
                if error.is_transient() && retry_count < TXN_MAX_RETRIES {
                    return Ok(ConcurrentTxnAttempt::Retry);
                }
                return Err(format!(
                    "FrankenSQLite worker {worker_index} BEGIN CONCURRENT failed after {retry_count} retries: {error}"
                ));
            }
            let statement = match conn
                .prepare("INSERT INTO bench VALUES (?1, ('t' || ?1), (?1 * 7))")
                .await
            {
                Ok(statement) => statement,
                Err(error) => {
                    let _ = conn.execute("ROLLBACK").await;
                    return Err(format!(
                        "FrankenSQLite worker {worker_index} prepare failed: {error}"
                    ));
                }
            };
            #[allow(clippy::cast_possible_wrap)]
            for row_index in 0..CONCURRENT_ROWS_PER_THREAD as i64 {
                match statement
                    .execute_with_params(&[fsqlite::SqliteValue::Integer(base + row_index)])
                    .await
                {
                    Ok(1) => {}
                    Ok(affected) => {
                        let _ = conn.execute("ROLLBACK").await;
                        return Err(format!(
                            "FrankenSQLite worker {worker_index} INSERT {row_index} affected {affected} rows"
                        ));
                    }
                    Err(error) if error.is_transient() && retry_count < TXN_MAX_RETRIES => {
                        conn.execute("ROLLBACK").await.map_err(|rollback_error| {
                            format!(
                                "FrankenSQLite worker {worker_index} rollback after INSERT retry failed: {rollback_error}"
                            )
                        })?;
                        return Ok(ConcurrentTxnAttempt::Retry);
                    }
                    Err(error) => {
                        let _ = conn.execute("ROLLBACK").await;
                        return Err(format!(
                            "FrankenSQLite worker {worker_index} INSERT {row_index} failed after {retry_count} retries: {error}"
                        ));
                    }
                }
            }
            match conn.execute("COMMIT").await {
                Ok(_) => Ok(ConcurrentTxnAttempt::Committed),
                Err(error) if error.is_transient() && retry_count < TXN_MAX_RETRIES => {
                    conn.execute("ROLLBACK").await.map_err(|rollback_error| {
                        format!(
                            "FrankenSQLite worker {worker_index} rollback after COMMIT retry failed: {rollback_error}"
                        )
                    })?;
                    Ok(ConcurrentTxnAttempt::Retry)
                }
                Err(error) => {
                    let _ = conn.execute("ROLLBACK").await;
                    Err(format!(
                        "FrankenSQLite worker {worker_index} COMMIT failed after {retry_count} retries: {error}"
                    ))
                }
            }
        })?;

        match outcome {
            ConcurrentTxnAttempt::Committed => return Ok(CONCURRENT_ROWS_PER_THREAD),
            ConcurrentTxnAttempt::Retry => {
                sleep_bench_busy_backoff(retry_count, jitter_salt);
                retry_count += 1;
            }
        }
    }
}

fn fsqlite_concurrent_worker(
    path: &str,
    worker_index: usize,
    gate: &ConcurrentSampleGate,
    sender: &mpsc::SyncSender<ConcurrentWorkerEvent>,
) -> Result<(), String> {
    let setup = (|| {
        let conn = fsqlite_e2e::block_on(fsqlite::Connection::open_with_page_size(
            path,
            benchmark_page_size_bytes(),
        ))
        .map_err(|error| format!("FrankenSQLite worker {worker_index} open failed: {error}"))?;
        let settings = configure_fsqlite_concurrent_worker(&conn)?;
        verify_concurrent_worker_settings(
            "FrankenSQLite",
            worker_index,
            &settings,
            "fsqlite_mvcc_on",
        )?;
        let setup = ConcurrentWorkerSetup {
            worker_index,
            thread_id: current_thread_identity(),
            cpu_affinity: current_thread_cpu_affinity()?,
            settings,
        };
        Ok::<_, String>((conn, setup))
    })();
    let (conn, setup) = match setup {
        Ok(setup) => setup,
        Err(error) => {
            send_concurrent_event(
                sender,
                ConcurrentWorkerEvent::Setup {
                    worker_index,
                    outcome: Err(error.clone()),
                },
            )?;
            return Err(error);
        }
    };
    send_concurrent_event(
        sender,
        ConcurrentWorkerEvent::Setup {
            worker_index,
            outcome: Ok(setup),
        },
    )?;
    gate.wait_for(ConcurrentGatePhase::Run)?;

    match execute_fsqlite_concurrent_transaction(&conn, worker_index) {
        Ok(completed_rows) => send_concurrent_event(
            sender,
            ConcurrentWorkerEvent::Done {
                worker_index,
                outcome: Ok(completed_rows),
            },
        )?,
        Err(error) => {
            send_concurrent_event(
                sender,
                ConcurrentWorkerEvent::Done {
                    worker_index,
                    outcome: Err(error.clone()),
                },
            )?;
            return Err(error);
        }
    }

    gate.wait_for(ConcurrentGatePhase::Postflight)?;
    let postflight = (|| {
        let settings = query_fsqlite_concurrent_worker_settings(&conn)?;
        verify_concurrent_worker_settings(
            "FrankenSQLite postflight",
            worker_index,
            &settings,
            "fsqlite_mvcc_on",
        )?;
        Ok::<_, String>(ConcurrentWorkerPostflight {
            worker_index,
            thread_id: current_thread_identity(),
            cpu_affinity: current_thread_cpu_affinity()?,
            settings,
        })
    })();
    let failed = postflight.as_ref().err().cloned();
    send_concurrent_event(
        sender,
        ConcurrentWorkerEvent::Postflight {
            worker_index,
            outcome: postflight,
        },
    )?;
    failed.map_or(Ok(()), Err)
}

const CONCURRENT_EVENT_TIMEOUT: Duration = Duration::from_secs(120);

fn receive_concurrent_setups(
    receiver: &mpsc::Receiver<ConcurrentWorkerEvent>,
    worker_count: usize,
) -> Result<BTreeMap<usize, ConcurrentWorkerSetup>, String> {
    let mut setups = BTreeMap::new();
    let deadline = Instant::now()
        .checked_add(CONCURRENT_EVENT_TIMEOUT)
        .ok_or_else(|| "worker setup deadline overflowed Instant".to_owned())?;
    for _ in 0..worker_count {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .ok_or_else(|| "timed out waiting for all worker setup receipts".to_owned())?;
        match receiver.recv_timeout(remaining) {
            Ok(ConcurrentWorkerEvent::Setup {
                worker_index,
                outcome,
            }) => {
                let setup = outcome?;
                if setup.worker_index != worker_index {
                    return Err(format!(
                        "setup event index {worker_index} carried worker {}",
                        setup.worker_index
                    ));
                }
                if setups.insert(worker_index, setup).is_some() {
                    return Err(format!("duplicate setup event for worker {worker_index}"));
                }
            }
            Ok(event) => return Err(format!("expected worker setup event, observed {event:?}")),
            Err(error) => return Err(format!("timed out waiting for worker setup: {error}")),
        }
    }
    Ok(setups)
}

fn receive_concurrent_completions(
    receiver: &mpsc::Receiver<ConcurrentWorkerEvent>,
    worker_count: usize,
) -> Result<BTreeMap<usize, usize>, String> {
    let mut completions = BTreeMap::new();
    let deadline = Instant::now()
        .checked_add(CONCURRENT_EVENT_TIMEOUT)
        .ok_or_else(|| "worker completion deadline overflowed Instant".to_owned())?;
    for _ in 0..worker_count {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .ok_or_else(|| "timed out waiting for all worker completion receipts".to_owned())?;
        match receiver.recv_timeout(remaining) {
            Ok(ConcurrentWorkerEvent::Done {
                worker_index,
                outcome,
            }) => {
                let completed_rows = outcome?;
                if completions.insert(worker_index, completed_rows).is_some() {
                    return Err(format!(
                        "duplicate completion event for worker {worker_index}"
                    ));
                }
            }
            Ok(event) => {
                return Err(format!(
                    "expected worker completion event, observed {event:?}"
                ));
            }
            Err(error) => return Err(format!("timed out waiting for worker completion: {error}")),
        }
    }
    Ok(completions)
}

fn receive_concurrent_postflights(
    receiver: &mpsc::Receiver<ConcurrentWorkerEvent>,
    worker_count: usize,
) -> Result<BTreeMap<usize, ConcurrentWorkerPostflight>, String> {
    let mut postflights = BTreeMap::new();
    let deadline = Instant::now()
        .checked_add(CONCURRENT_EVENT_TIMEOUT)
        .ok_or_else(|| "worker postflight deadline overflowed Instant".to_owned())?;
    for _ in 0..worker_count {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .ok_or_else(|| "timed out waiting for all worker postflight receipts".to_owned())?;
        match receiver.recv_timeout(remaining) {
            Ok(ConcurrentWorkerEvent::Postflight {
                worker_index,
                outcome,
            }) => {
                let postflight = outcome?;
                if postflight.worker_index != worker_index {
                    return Err(format!(
                        "postflight event index {worker_index} carried worker {}",
                        postflight.worker_index
                    ));
                }
                if postflights.insert(worker_index, postflight).is_some() {
                    return Err(format!(
                        "duplicate postflight event for worker {worker_index}"
                    ));
                }
            }
            Ok(event) => {
                return Err(format!(
                    "expected worker postflight event, observed {event:?}"
                ));
            }
            Err(error) => return Err(format!("timed out waiting for worker postflight: {error}")),
        }
    }
    Ok(postflights)
}

fn join_concurrent_workers(handles: Vec<BenchTask<Result<(), String>>>) -> Result<(), String> {
    let mut errors = Vec::new();
    for handle in handles {
        match handle.try_wait() {
            Ok(Ok(())) => {}
            Ok(Err(error)) | Err(error) => errors.push(error),
        }
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors.join("; "))
    }
}

fn abort_concurrent_sample(
    gate: &ConcurrentSampleGate,
    handles: Vec<BenchTask<Result<(), String>>>,
    error: String,
) -> String {
    gate.abort();
    match join_concurrent_workers(handles) {
        Ok(()) => error,
        Err(worker_error) => format!("{error}; worker shutdown: {worker_error}"),
    }
}

fn expected_concurrent_id_sum(worker_count: usize) -> Result<i64, String> {
    let rows = i128::try_from(CONCURRENT_ROWS_PER_THREAD)
        .map_err(|_| "row count exceeds i128".to_owned())?;
    let range = i128::from(CONCURRENT_RANGE_SIZE);
    let mut sum = 0_i128;
    for worker_index in 0..worker_count {
        let worker =
            i128::try_from(worker_index).map_err(|_| "worker index exceeds i128".to_owned())?;
        let base = worker
            .checked_mul(range)
            .ok_or_else(|| "concurrent row-id base overflowed i128".to_owned())?;
        let sequence = rows
            .checked_mul(rows.saturating_sub(1))
            .and_then(|value| value.checked_div(2))
            .ok_or_else(|| "concurrent row-id sequence sum overflowed i128".to_owned())?;
        sum = sum
            .checked_add(
                rows.checked_mul(base)
                    .and_then(|value| value.checked_add(sequence))
                    .ok_or_else(|| "concurrent worker row-id sum overflowed i128".to_owned())?,
            )
            .ok_or_else(|| "concurrent row-id sum overflowed i128".to_owned())?;
    }
    i64::try_from(sum).map_err(|_| "concurrent row-id sum exceeds i64".to_owned())
}

struct ConcurrentSampleContext<'a> {
    engine: &'a str,
    phase: &'a str,
    sample_index: usize,
    worker_count: usize,
    expected_cpu_affinity: String,
    gate: Arc<ConcurrentSampleGate>,
    receiver: mpsc::Receiver<ConcurrentWorkerEvent>,
    handles: Vec<BenchTask<Result<(), String>>>,
}

fn coordinate_concurrent_sample<F>(
    context: ConcurrentSampleContext<'_>,
    verify_database: F,
) -> Result<ConcurrentSample, String>
where
    F: FnOnce() -> Result<(usize, i64), String>,
{
    let ConcurrentSampleContext {
        engine,
        phase,
        sample_index,
        worker_count,
        expected_cpu_affinity,
        gate,
        receiver,
        handles,
    } = context;
    let setups = match receive_concurrent_setups(&receiver, worker_count) {
        Ok(setups) => setups,
        Err(error) => {
            return Err(abort_concurrent_sample(&gate, handles, error));
        }
    };
    let setup_indices = setups.keys().copied().collect::<Vec<_>>();
    let expected_indices = (0..worker_count).collect::<Vec<_>>();
    if setup_indices != expected_indices {
        return Err(abort_concurrent_sample(
            &gate,
            handles,
            format!(
                "{engine} setup worker indices mismatch: expected {expected_indices:?}, observed {setup_indices:?}"
            ),
        ));
    }
    let mut setup_thread_ids = std::collections::BTreeSet::new();
    for worker_index in 0..worker_count {
        let setup = setups
            .get(&worker_index)
            .expect("setup worker indices were validated");
        if setup.cpu_affinity != expected_cpu_affinity {
            return Err(abort_concurrent_sample(
                &gate,
                handles,
                format!(
                    "{engine} worker {worker_index} setup affinity mismatch: expected `{expected_cpu_affinity}`, observed `{}`",
                    setup.cpu_affinity
                ),
            ));
        }
        if !setup_thread_ids.insert(setup.thread_id.clone()) {
            return Err(abort_concurrent_sample(
                &gate,
                handles,
                format!(
                    "{engine} setup reused thread identity `{}` across workers",
                    setup.thread_id
                ),
            ));
        }
    }

    let start = Instant::now();
    if let Err(error) = gate.transition(ConcurrentGatePhase::Preparing, ConcurrentGatePhase::Run) {
        return Err(abort_concurrent_sample(&gate, handles, error));
    }
    let completions = match receive_concurrent_completions(&receiver, worker_count) {
        Ok(completions) => completions,
        Err(error) => {
            return Err(abort_concurrent_sample(&gate, handles, error));
        }
    };
    let elapsed = start.elapsed();
    if let Err(error) = gate.transition(ConcurrentGatePhase::Run, ConcurrentGatePhase::Postflight) {
        return Err(abort_concurrent_sample(&gate, handles, error));
    }
    let postflights = match receive_concurrent_postflights(&receiver, worker_count) {
        Ok(postflights) => postflights,
        Err(error) => {
            return Err(abort_concurrent_sample(&gate, handles, error));
        }
    };
    join_concurrent_workers(handles)?;

    let expected_rows = worker_count
        .checked_mul(CONCURRENT_ROWS_PER_THREAD)
        .ok_or_else(|| "concurrent expected row count overflowed usize".to_owned())?;
    let completed_rows = completions.values().try_fold(0_usize, |total, rows| {
        total
            .checked_add(*rows)
            .ok_or_else(|| "concurrent completed row count overflowed usize".to_owned())
    })?;
    let expected_id_sum = expected_concurrent_id_sum(worker_count)?;
    let (database_rows, database_id_sum) = verify_database()?;
    if completed_rows != expected_rows
        || database_rows != expected_rows
        || database_id_sum != expected_id_sum
    {
        return Err(format!(
            "{engine} work oracle mismatch: expected rows={expected_rows} id_sum={expected_id_sum}, \
             completed rows={completed_rows}, database rows={database_rows} id_sum={database_id_sum}"
        ));
    }

    let mut workers = Vec::with_capacity(worker_count);
    for worker_index in 0..worker_count {
        let setup = setups
            .get(&worker_index)
            .ok_or_else(|| format!("missing setup receipt for worker {worker_index}"))?;
        let postflight = postflights
            .get(&worker_index)
            .ok_or_else(|| format!("missing postflight receipt for worker {worker_index}"))?;
        let completed = completions
            .get(&worker_index)
            .copied()
            .ok_or_else(|| format!("missing completion receipt for worker {worker_index}"))?;
        if setup.settings != postflight.settings {
            return Err(format!(
                "{engine} worker {worker_index} settings changed during timed execution: setup={:?}, postflight={:?}",
                setup.settings, postflight.settings
            ));
        }
        let thread_identity_verified = setup.thread_id == postflight.thread_id;
        let thread_affinity_verified = setup.cpu_affinity == postflight.cpu_affinity
            && setup.cpu_affinity == expected_cpu_affinity;
        if !thread_identity_verified || !thread_affinity_verified {
            return Err(format!(
                "{engine} worker {worker_index} identity/affinity changed: setup thread={} affinity={}, postflight thread={} affinity={}",
                setup.thread_id, setup.cpu_affinity, postflight.thread_id, postflight.cpu_affinity
            ));
        }
        workers.push(JsonConcurrentWorkerReceipt {
            worker_index,
            setup_thread_id: setup.thread_id.clone(),
            postflight_thread_id: postflight.thread_id.clone(),
            setup_cpu_affinity: setup.cpu_affinity.clone(),
            postflight_cpu_affinity: postflight.cpu_affinity.clone(),
            completed_rows: completed,
            settings: setup.settings.clone(),
            settings_verified: true,
            thread_identity_verified,
            thread_affinity_verified,
        });
    }

    Ok(ConcurrentSample {
        elapsed,
        readiness: JsonConcurrentSampleReadiness {
            phase: phase.to_owned(),
            sample_index,
            engine: engine.to_owned(),
            expected_cpu_affinity,
            expected_workers: worker_count,
            expected_rows,
            completed_rows,
            database_rows,
            expected_id_sum,
            database_id_sum,
            timed_scope:
                "gate release through receipt of every worker's committed row count; runtime, file, schema, connection setup, PRAGMA verification, postflight verification, work oracle, and teardown excluded"
                    .to_owned(),
            workers,
        },
    })
}

fn run_csqlite_concurrent_sample(
    worker_count: usize,
    phase: &str,
    sample_index: usize,
) -> Result<ConcurrentSample, String> {
    let runtime = RuntimeBuilder::new()
        .blocking_threads(worker_count, worker_count)
        .build()
        .map_err(|error| format!("could not build C SQLite benchmark runtime: {error}"))?;
    let temporary = tempfile::NamedTempFile::new()
        .map_err(|error| format!("could not create C SQLite benchmark file: {error}"))?;
    let path = temporary
        .path()
        .to_str()
        .ok_or_else(|| "C SQLite benchmark path is not UTF-8".to_owned())?
        .to_owned();
    {
        let setup = rusqlite::Connection::open(&path)
            .map_err(|error| format!("C SQLite setup open failed: {error}"))?;
        configure_csqlite_concurrent_worker(&setup)?;
        setup
            .execute_batch(
                "CREATE TABLE bench \
                 (id INTEGER PRIMARY KEY, name TEXT, score INTEGER);",
            )
            .map_err(|error| format!("C SQLite setup schema failed: {error}"))?;
    }

    let expected_cpu_affinity = concurrent_expected_cpu_affinity()?;
    let gate = Arc::new(ConcurrentSampleGate::new());
    let capacity = worker_count.saturating_mul(3).max(1);
    let (sender, receiver) = mpsc::sync_channel(capacity);
    let handles = (0..worker_count)
        .map(|worker_index| {
            let path = path.clone();
            let gate = Arc::clone(&gate);
            let sender = sender.clone();
            spawn_bench_task(&runtime, move || {
                csqlite_concurrent_worker(&path, worker_index, &gate, &sender)
            })
        })
        .collect::<Vec<_>>();
    drop(sender);

    let verify_path = path.clone();
    coordinate_concurrent_sample(
        ConcurrentSampleContext {
            engine: "csqlite",
            phase,
            sample_index,
            worker_count,
            expected_cpu_affinity,
            gate,
            receiver,
            handles,
        },
        move || {
            let connection = rusqlite::Connection::open(&verify_path)
                .map_err(|error| format!("C SQLite oracle open failed: {error}"))?;
            let (count, sum) = connection
                .query_row(
                    "SELECT COUNT(*), COALESCE(SUM(id), 0) FROM bench",
                    [],
                    |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
                )
                .map_err(|error| format!("C SQLite work oracle failed: {error}"))?;
            let count = usize::try_from(count)
                .map_err(|_| "C SQLite work oracle count was negative".to_owned())?;
            Ok((count, sum))
        },
    )
}

fn run_fsqlite_concurrent_sample(
    worker_count: usize,
    phase: &str,
    sample_index: usize,
) -> Result<ConcurrentSample, String> {
    let runtime = RuntimeBuilder::new()
        .blocking_threads(worker_count, worker_count)
        .build()
        .map_err(|error| format!("could not build FrankenSQLite benchmark runtime: {error}"))?;
    let temporary = tempfile::NamedTempFile::new()
        .map_err(|error| format!("could not create FrankenSQLite benchmark file: {error}"))?;
    let path = temporary
        .path()
        .to_str()
        .ok_or_else(|| "FrankenSQLite benchmark path is not UTF-8".to_owned())?
        .to_owned();
    {
        let setup = fsqlite_e2e::block_on(fsqlite::Connection::open_with_page_size(
            &path,
            benchmark_page_size_bytes(),
        ))
        .map_err(|error| format!("FrankenSQLite setup open failed: {error}"))?;
        configure_fsqlite_concurrent_worker(&setup)?;
        fsqlite_e2e::block_on(
            setup.execute("CREATE TABLE bench (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)"),
        )
        .map_err(|error| format!("FrankenSQLite setup schema failed: {error}"))?;
    }

    let expected_cpu_affinity = concurrent_expected_cpu_affinity()?;
    let gate = Arc::new(ConcurrentSampleGate::new());
    let capacity = worker_count.saturating_mul(3).max(1);
    let (sender, receiver) = mpsc::sync_channel(capacity);
    let handles = (0..worker_count)
        .map(|worker_index| {
            let path = path.clone();
            let gate = Arc::clone(&gate);
            let sender = sender.clone();
            spawn_bench_task(&runtime, move || {
                fsqlite_concurrent_worker(&path, worker_index, &gate, &sender)
            })
        })
        .collect::<Vec<_>>();
    drop(sender);

    let verify_path = path.clone();
    coordinate_concurrent_sample(
        ConcurrentSampleContext {
            engine: "fsqlite",
            phase,
            sample_index,
            worker_count,
            expected_cpu_affinity,
            gate,
            receiver,
            handles,
        },
        move || {
            let connection = fsqlite_e2e::block_on(fsqlite::Connection::open_with_page_size(
                &verify_path,
                benchmark_page_size_bytes(),
            ))
            .map_err(|error| format!("FrankenSQLite oracle open failed: {error}"))?;
            let rows = fsqlite_e2e::block_on(
                connection.query("SELECT COUNT(*), COALESCE(SUM(id), 0) FROM bench"),
            )
            .map_err(|error| format!("FrankenSQLite work oracle failed: {error}"))?;
            let row = rows
                .first()
                .ok_or_else(|| "FrankenSQLite work oracle returned no row".to_owned())?;
            let count = usize::try_from(fsqlite_integer(
                row,
                0,
                "FrankenSQLite concurrent work oracle count",
            ))
            .map_err(|_| "FrankenSQLite work oracle count was negative".to_owned())?;
            let sum = fsqlite_integer(row, 1, "FrankenSQLite concurrent work oracle sum");
            Ok((count, sum))
        },
    )
}

// ─── Section 4: Concurrent writers ─────────────────────────────────────

fn bench_concurrent_writers(report: &mut BenchReport) {
    let section = report.add_section(
        CONCURRENT_WRITERS_SECTION_TITLE,
        &format!(
            "Each writer inserts {} rows into non-overlapping key ranges on the same \
             file-backed WAL database. Both engines spawn N OS threads each owning its \
             own connection, and both writer connections run at `synchronous=NORMAL` so \
             the two engines are compared at matched durability (set \
             `FSQLITE_BENCH_CONCURRENT_SYNC=full` to match them at FULL instead). \
             C SQLite uses WAL + busy_timeout, FrankenSQLite uses the \
             MVCC page-lock table via `PRAGMA fsqlite.concurrent_mode=ON` + \
             `BEGIN CONCURRENT`. Every scored worker connection proves its effective \
             PRAGMAs, thread identity, CPU affinity, completed-row count, and an untimed \
             database count/sum oracle. Setup and postflight work are outside the timer. \
             This mirrors the standalone `mt_mvcc_bench` harness. NOTE: this file-backed \
             section is disk-noise-bound on shared hosts (C medians have been observed \
             spreading 95-138 ms at 2 writers, CV up to 104%); cite `mt_mvcc_bench` for \
             concurrent-writer speed claims, not these rows (bd-x5gzk).",
            CONCURRENT_ROWS_PER_THREAD
        ),
    );
    let profile_concurrent_enabled = bench_env_flag("FSQLITE_BENCH_PROFILE_CONCURRENT");

    for &n_threads in CONCURRENT_THREAD_COUNTS {
        let total_rows = n_threads * CONCURRENT_ROWS_PER_THREAD;
        eprint!("  Benchmarking {n_threads} concurrent writers ({total_rows} total rows)... ");

        let (cs, csqlite_readiness) = measure_concurrent(
            &format!("cs_concurrent_{n_threads}t"),
            total_rows,
            |phase, sample_index| run_csqlite_concurrent_sample(n_threads, phase, sample_index),
        );
        let profile_scope = if profile_concurrent_enabled {
            let previous_hot_path_profile_enabled = hot_path_profile_enabled();
            set_hot_path_profile_enabled(true);
            reset_hot_path_profile();
            Some((
                previous_hot_path_profile_enabled,
                fsqlite_wal::wal_telemetry_snapshot(),
            ))
        } else {
            None
        };
        let (fs, fsqlite_readiness) = measure_concurrent(
            &format!("fs_concurrent_{n_threads}t"),
            total_rows,
            |phase, sample_index| run_fsqlite_concurrent_sample(n_threads, phase, sample_index),
        );
        let fsqlite_concurrent_profile =
            if let Some((previous_hot_path_profile_enabled, wal_before)) = profile_scope {
                let profile = hot_path_profile_snapshot();
                let wal_after = fsqlite_wal::wal_telemetry_snapshot();
                set_hot_path_profile_enabled(previous_hot_path_profile_enabled);
                let wal_frames = metric_delta(
                    wal_after.wal.frames_written_total,
                    wal_before.wal.frames_written_total,
                );
                let wal_bytes = metric_delta(
                    wal_after.wal.bytes_written_total,
                    wal_before.wal.bytes_written_total,
                );
                let wal_group_commits = metric_delta(
                    wal_after.group_commit.group_commits_total,
                    wal_before.group_commit.group_commits_total,
                );
                let wal_group_commit_latency_us = metric_delta(
                    wal_after.group_commit.commit_latency_us_total,
                    wal_before.group_commit.commit_latency_us_total,
                );
                let json_profile = build_fsqlite_concurrent_profile(
                    total_rows,
                    fs.median(),
                    &profile,
                    wal_frames,
                    wal_bytes,
                    wal_group_commits,
                    wal_group_commit_latency_us,
                );
                print_fsqlite_concurrent_profile(
                    n_threads,
                    total_rows,
                    fs.median(),
                    &profile,
                    wal_frames,
                    wal_bytes,
                    wal_group_commits,
                    wal_group_commit_latency_us,
                );
                Some(json_profile)
            } else {
                None
            };

        eprintln!(
            "C={} F={}",
            format_duration(cs.median()),
            format_duration(fs.median())
        );
        section.add_row_with_concurrent_details(
            &format!("{n_threads} writers x {CONCURRENT_ROWS_PER_THREAD} rows"),
            Some(cs),
            Some(fs),
            fsqlite_concurrent_profile,
            Some(JsonConcurrentReadiness {
                csqlite_samples: csqlite_readiness,
                fsqlite_samples: fsqlite_readiness,
            }),
        );
    }

    // Also benchmark C SQLite single-threaded for the same total work (baseline).
    let section = report.add_section(
        "Concurrent Writers — C SQLite Single-Thread Baseline",
        "Same total row count as concurrent tests, but single-threaded file-backed C SQLite.",
    );

    for &n_threads in CONCURRENT_THREAD_COUNTS {
        let total_rows = n_threads * CONCURRENT_ROWS_PER_THREAD;
        eprint!("  Benchmarking C SQLite single-thread baseline ({total_rows} rows)... ");

        let cs_single = measure(&format!("cs_single_{n_threads}t_equiv"), total_rows, || {
            let tmp = tempfile::NamedTempFile::new().unwrap();
            let path = tmp.path().to_str().unwrap().to_owned();
            let conn = rusqlite::Connection::open(&path).unwrap();
            conn.execute_batch(&format!(
                "PRAGMA page_size = {};\
                 PRAGMA journal_mode = WAL;\
                 PRAGMA synchronous = {};\
                 PRAGMA cache_size = -64000;\
                 CREATE TABLE bench (id INTEGER PRIMARY KEY, name TEXT, score INTEGER);",
                benchmark_page_size_bytes(),
                concurrent_sync_mode()
            ))
            .unwrap();

            conn.execute_batch("BEGIN").unwrap();
            let mut stmt = conn
                .prepare("INSERT INTO bench VALUES (?1, ('t' || ?1), (?1 * 7))")
                .unwrap();
            #[allow(clippy::cast_possible_wrap)]
            for i in 0..total_rows as i64 {
                stmt.execute(rusqlite::params![i]).unwrap();
            }
            conn.execute_batch("COMMIT").unwrap();
        });

        eprintln!("C_single={}", format_duration(cs_single.median()));
        section.add_row(
            &format!("C SQLite 1 thread / {total_rows} rows (baseline)"),
            Some(cs_single),
            None,
        );
    }
}

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod tests {
    use super::*;
    use std::cell::Cell;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn sample_measurement(label: &str, row_count: usize, durations_ms: &[u64]) -> Measurement {
        Measurement {
            label: label.to_owned(),
            durations: durations_ms
                .iter()
                .map(|ms| Duration::from_millis(*ms))
                .collect(),
            row_count,
        }
    }

    fn sample_fsqlite_concurrent_profile() -> JsonFsqliteConcurrentProfile {
        let mut counters = BTreeMap::new();
        counters.insert("mvcc_busy_retries".to_owned(), 7);
        counters.insert("mvcc_page_lock_waits".to_owned(), 3);
        counters.insert("mvcc_stale_snapshot".to_owned(), 2);
        counters.insert("wal_frames".to_owned(), 11);
        JsonFsqliteConcurrentProfile {
            total_rows: 2_000,
            fsqlite_median_ms: 12.5,
            capture_scope: "test aggregate".to_owned(),
            counters,
        }
    }

    fn sample_report() -> BenchReport {
        let mut report = BenchReport::new();
        let section = report.add_section(
            "Insert Throughput",
            "Sequential insert benchmarking for parser-stable JSON output.",
        );
        section.add_row(
            "100 rows / small record",
            Some(sample_measurement("csqlite", 100, &[1, 1, 2])),
            Some(sample_measurement("frankensqlite", 100, &[2, 2, 3])),
        );
        report
    }

    fn sample_provenance() -> JsonBenchmarkProvenance {
        JsonBenchmarkProvenance {
            schema_version: BENCHMARK_PROVENANCE_SCHEMA_V3.to_owned(),
            citable: false,
            status: "unverified".to_owned(),
            validation_errors: vec![
                "generic comprehensive measurements are diagnostic-only".to_owned(),
            ],
            build: JsonBuildIdentity {
                workspace_root: "/test/frankensqlite".to_owned(),
                git_commit_sha: "0123456789abcdef0123456789abcdef01234567".to_owned(),
                git_branch: "main".to_owned(),
                git_dirty: Some(false),
                tracked_workspace_inputs_watched: "complete".to_owned(),
                cargo_profile_family: "release".to_owned(),
                selected_profile: "release-perf".to_owned(),
                declared_profile: "release-perf".to_owned(),
                build_nonce: "34".repeat(32),
                opt_level: "3".to_owned(),
                debuginfo: "false".to_owned(),
                debug_assertions: false,
                target: "x86_64-unknown-linux-gnu".to_owned(),
                host: "x86_64-unknown-linux-gnu".to_owned(),
                panic_strategy: "abort".to_owned(),
                panic_abort: true,
                package_features: Vec::new(),
                encoded_rustflags_hex: String::new(),
                encoded_rustflags_present: true,
                profile_override_environment_hex: encode_build_environment(
                    &canonical_profile_environment("release-perf")
                        .expect("test profile is canonical"),
                ),
                native_override_environment_hex: encode_build_environment(
                    &canonical_native_environment(),
                ),
                verbose_build_log_path: Some("/test/cargo-build-vv.log".to_owned()),
                verbose_build_log_sha256: Some("12".repeat(32)),
                verbose_build_log_size_bytes: Some(1024),
                verbose_build_log_verified: true,
                profile_proof_scope: "test".to_owned(),
                rustc_version: "rustc test".to_owned(),
                cargo_version: "cargo test".to_owned(),
            },
            runtime_source: JsonRuntimeSourceIdentity {
                verification_root: "/test/frankensqlite".to_owned(),
                git_commit_sha: Some("0123456789abcdef0123456789abcdef01234567".to_owned()),
                git_branch: Some("main".to_owned()),
                git_dirty: Some(false),
            },
            working_directory: Some("/test/frankensqlite".to_owned()),
            binary_path: Some("/test/comprehensive-bench".to_owned()),
            binary_sha256: Some("ab".repeat(32)),
            binary_size_bytes: Some(1024),
            binary_modified_unix_ts: Some(1_700_000_001),
            binary_device_id: Some(8),
            binary_inode: Some(42),
            cargo_lock_sha256: Some("cd".repeat(32)),
            cargo_feature_graph_sha256: Some("ef".repeat(32)),
            cargo_feature_graph: Some("fsqlite-e2e v0.1.19".to_owned()),
            cargo_feature_graph_command:
                "cargo tree --locked --offline -p fsqlite-e2e -e features,no-dev".to_owned(),
            command_line: vec!["comprehensive-bench".to_owned(), "--quick".to_owned()],
            benchmark_environment: BTreeMap::new(),
            cpu_affinity: Some("0-7".to_owned()),
            runtime_bridge: "scenario_scoped_thread_local_block_on".to_owned(),
            tracing: JsonTracingIdentity {
                rust_log: None,
                statement_debug_enabled: false,
                statement_reuse_info_enabled: false,
                fallback_decision_debug_enabled: false,
            },
            durability: JsonDurabilityIdentity {
                page_size_bytes: 4096,
                default_synchronous: "NORMAL".to_owned(),
                concurrent_synchronous_modes: vec!["NORMAL".to_owned()],
                csqlite_pragmas: vec!["PRAGMA synchronous = NORMAL;".to_owned()],
                fsqlite_pragmas: vec!["PRAGMA synchronous = NORMAL;".to_owned()],
                concurrent_mode_default: true,
                verified: true,
                matched: true,
                validation_errors: Vec::new(),
                effective_profiles: BTreeMap::from([
                    (
                        "memory.csqlite".to_owned(),
                        BTreeMap::from([("synchronous".to_owned(), "normal".to_owned())]),
                    ),
                    (
                        "memory.fsqlite".to_owned(),
                        BTreeMap::from([("synchronous".to_owned(), "normal".to_owned())]),
                    ),
                    (
                        "file.csqlite".to_owned(),
                        BTreeMap::from([("synchronous".to_owned(), "normal".to_owned())]),
                    ),
                    (
                        "file.fsqlite".to_owned(),
                        BTreeMap::from([("synchronous".to_owned(), "normal".to_owned())]),
                    ),
                ]),
            },
            execution_routing: JsonExecutionRouting {
                probe_scope: "untimed test probe".to_owned(),
                timed_routes_verified: false,
                limitations: vec!["test".to_owned()],
                timed_execution_instrumented: false,
                parser_fast_path_executions: 1,
                parser_slow_path_executions: 1,
                prepared_insert_fast_lane_hits: 4,
                prepared_insert_instrumented_lane_hits: 0,
                prepared_direct_insert_executions: 4,
                prepared_update_delete_fast_lane_hits: 0,
                prepared_update_delete_instrumented_lane_hits: 0,
                prepared_direct_update_executions: 1,
                prepared_direct_delete_executions: 1,
                prepared_update_delete_dml_direct_handoff_runs: 0,
                prepared_table_dml_affected_only_runs: 0,
                prepared_dml_fallbacks: BTreeMap::new(),
                select_routing_decisions: BTreeMap::from([
                    ("group_by_fallback".to_owned(), 1),
                    ("valid_btree_page".to_owned(), 1),
                ]),
                probe_errors: Vec::new(),
            },
        }
    }

    #[test]
    fn benchmark_pragmas_disable_time_travel_capture() {
        assert!(
            FSQLITE_BENCHMARK_PRAGMAS.iter().any(|pragma| pragma
                .eq_ignore_ascii_case("PRAGMA fsqlite_capture_time_travel_snapshots=false;")),
            "comprehensive-bench should profile benchmark workloads, not optional time-travel snapshot cloning"
        );
    }

    #[test]
    fn effective_synchronous_pragma_values_have_one_canonical_representation() {
        for (left, right, expected) in [
            ("0", "OFF", "off"),
            ("1", "normal", "normal"),
            ("2", "FULL", "full"),
            ("3", "extra", "extra"),
        ] {
            assert_eq!(
                normalize_effective_pragma_value("synchronous", left.to_owned())
                    .expect("numeric synchronous value must normalize"),
                expected
            );
            assert_eq!(
                normalize_effective_pragma_value("SYNCHRONOUS", right.to_owned())
                    .expect("named synchronous value must normalize"),
                expected
            );
        }
        assert_eq!(
            normalize_effective_pragma_value("page_size", "4096".to_owned())
                .expect("unrelated pragma must pass through"),
            "4096"
        );
        assert!(
            normalize_effective_pragma_value("synchronous", "unknown".to_owned()).is_err(),
            "unknown durability values must fail closed"
        );
    }

    #[test]
    fn compiled_panic_strategy_matches_the_benchmark_target_cfg() {
        assert_eq!(
            compiled_panic_strategy(),
            if cfg!(panic = "abort") {
                "abort"
            } else {
                "unwind"
            }
        );
    }

    #[test]
    fn busy_backoff_delay_caps_and_jitters_deterministically() {
        assert_eq!(bench_busy_backoff_delay(0, 0), Duration::from_micros(100));
        assert_eq!(
            bench_busy_backoff_delay(10, 0),
            bench_busy_backoff_delay(11, 0),
            "base exponential backoff should cap at attempt 10",
        );

        let no_jitter = bench_busy_backoff_delay(3, 0);
        let jittered = bench_busy_backoff_delay(3, 5);
        assert!(jittered > no_jitter);
        assert!(
            jittered < no_jitter + Duration::from_micros(BENCH_BUSY_JITTER_US),
            "jitter should stay inside the configured window",
        );
        assert_eq!(
            bench_busy_backoff_delay(3, 5),
            jittered,
            "same attempt and salt should produce stable jitter",
        );
    }

    #[test]
    fn spawn_bench_task_runs_on_runtime_blocking_pool() {
        let runtime = RuntimeBuilder::new()
            .blocking_threads(1, 1)
            .build()
            .expect("benchmark runtime should build for test");
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_for_task = Arc::clone(&counter);

        let handle = spawn_bench_task(&runtime, move || {
            counter_for_task.fetch_add(1, Ordering::Relaxed);
        });

        handle.wait();
        assert_eq!(
            counter.load(Ordering::Relaxed),
            1,
            "benchmark task should run exactly once",
        );
    }

    #[test]
    fn spawn_bench_task_propagates_panics() {
        let runtime = RuntimeBuilder::new()
            .blocking_threads(1, 1)
            .build()
            .expect("benchmark runtime should build for test");
        let handle = spawn_bench_task(&runtime, || -> () {
            panic!("benchmark worker panic should surface");
        });

        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| handle.wait()))
            .expect_err("wait should propagate worker panic");
        let message = panic_payload_to_string(panic);
        assert!(
            message.contains("benchmark worker panic should surface"),
            "panic payload should mention original worker failure: {message}",
        );
    }

    #[test]
    fn concurrent_samples_prove_real_worker_settings_and_completed_work() {
        for (engine, sample) in [
            (
                "csqlite",
                run_csqlite_concurrent_sample(2, "measured", 0)
                    .expect("C SQLite concurrent sample should complete"),
            ),
            (
                "fsqlite",
                run_fsqlite_concurrent_sample(2, "measured", 0)
                    .expect("FrankenSQLite concurrent sample should complete"),
            ),
        ] {
            assert!(sample.elapsed > Duration::ZERO);
            let readiness = sample.readiness;
            assert_eq!(readiness.engine, engine);
            assert_eq!(readiness.expected_workers, 2);
            assert_eq!(readiness.expected_rows, 2 * CONCURRENT_ROWS_PER_THREAD);
            assert_eq!(readiness.completed_rows, readiness.expected_rows);
            assert_eq!(readiness.database_rows, readiness.expected_rows);
            assert_eq!(readiness.database_id_sum, readiness.expected_id_sum);
            assert_eq!(readiness.workers.len(), 2);
            for worker in readiness.workers {
                assert_eq!(worker.completed_rows, CONCURRENT_ROWS_PER_THREAD);
                assert!(worker.settings_verified);
                assert!(worker.thread_identity_verified);
                assert!(worker.thread_affinity_verified);
                assert_eq!(worker.setup_thread_id, worker.postflight_thread_id);
                assert_eq!(worker.setup_cpu_affinity, worker.postflight_cpu_affinity);
            }
        }
    }

    #[test]
    fn measure_with_teardown_runs_after_each_sample() {
        let timed_calls = Cell::new(0usize);
        let teardown_calls = Cell::new(0usize);

        let measurement = measure_with_teardown(
            "teardown-test",
            7,
            || timed_calls.set(timed_calls.get() + 1),
            || teardown_calls.set(teardown_calls.get() + 1),
        );

        let expected_calls = WARMUP_ITERS + MAX_ITERS;
        assert_eq!(timed_calls.get(), expected_calls);
        assert_eq!(teardown_calls.get(), expected_calls);
        assert_eq!(measurement.iter_count(), MAX_ITERS);
        assert_eq!(measurement.row_count, 7);
    }

    #[test]
    fn parse_cli_args_supports_machine_readable_flags() {
        let args = vec![
            "comprehensive-bench".to_owned(),
            "--quick".to_owned(),
            "--filter".to_owned(),
            "insert".to_owned(),
            "--json-out".to_owned(),
            "bench.json".to_owned(),
            "--json-stdout".to_owned(),
            "--no-html".to_owned(),
        ];

        let options = parse_cli_args(&args).expect("cli args should parse");

        assert_eq!(
            options,
            CliOptions {
                quick: true,
                filter: Some("insert".to_owned()),
                html_path: None,
                emit_html: false,
                emit_timestamped_json: false,
                json_out_path: Some("bench.json".to_owned()),
                json_stdout: true,
                print_json_schema: false,
                allow_unverified_provenance: false,
                bridge_experiment: false,
                bridge_samples: 96,
                bridge_operations: 1_000,
                bridge_seed: 0x4653_514c_4954_4530,
            }
        );
    }

    #[test]
    fn parse_cli_args_rejects_missing_filter_value() {
        let args = vec!["comprehensive-bench".to_owned(), "--filter".to_owned()];

        let error = parse_cli_args(&args).expect_err("missing filter value should error");
        assert!(
            error.contains("expected a value after --filter"),
            "unexpected error: {error}",
        );
    }

    #[test]
    fn section_filter_matches_update_delete_aliases() {
        let aliases = [
            "update",
            "delete",
            "update-delete",
            "update-delete-throughput",
            "update-deletethroughput",
            "update/delete",
            "dml",
        ];

        for filter in [
            "update",
            "delete",
            "update-delete",
            "update-delete-throughput",
            "update-deletethroughput",
            "update/delete",
            "dml",
        ] {
            assert!(
                section_filter_matches(Some(filter), &aliases),
                "filter {filter} should select the UPDATE/DELETE section",
            );
        }
        assert!(section_filter_matches(None, &aliases));
        assert!(!section_filter_matches(Some("insert"), &aliases));
    }

    #[test]
    fn scenario_categories_use_canonical_ids() {
        assert_eq!(ScenarioCategory::MixedOltp.id(), "mixed");
        assert_eq!(
            categorize_scenario("Mixed OLTP Workload at Scale", "5K ops (80r/20w)").id(),
            "mixed"
        );
        assert_eq!(
            categorize_scenario(
                "INSERTThroughput — Transaction Strategy Comparison (small_3col)",
                "100 rows / autocommit",
            )
            .id(),
            "write_single"
        );
        assert_eq!(
            categorize_scenario(
                "INSERTThroughput — Transaction Strategy Comparison (small_3col)",
                "1000 rows / batched (1000/txn)",
            )
            .id(),
            "write_bulk"
        );
        assert_eq!(
            categorize_scenario("Read-After-Write Query Performance", "100 rows / COUNT(*)").id(),
            "read_aggregate"
        );
        assert_eq!(
            categorize_scenario(
                "Read-After-Write Query Performance",
                "100 rows / point lookup (PK)",
            )
            .id(),
            "read_single"
        );
        assert_eq!(
            categorize_scenario(
                "JOIN Performance — Multi-Table Queries",
                "100 orders / INNER JOIN"
            )
            .id(),
            "read_single"
        );
        assert_eq!(
            categorize_scenario(
                "JOIN Performance — Multi-Table Queries",
                "100 orders / JOIN + GROUP BY",
            )
            .id(),
            "read_aggregate"
        );
    }

    #[test]
    fn weighted_category_score_uses_ratio_geomean_rollup() {
        let mut per_category = BTreeMap::new();
        for category in ScenarioCategory::ALL {
            per_category.insert(category.id().to_owned(), ratio_stats(&[]));
        }
        per_category.insert(
            "read_single".to_owned(),
            JsonCategoryRatioStats {
                n: 1,
                avg_ratio: Some(4.0),
                geomean_ratio: Some(4.0),
                median_ratio: Some(4.0),
                p90_ratio: Some(4.0),
                p99_ratio: Some(4.0),
            },
        );
        per_category.insert(
            "write_single".to_owned(),
            JsonCategoryRatioStats {
                n: 1,
                avg_ratio: Some(1.0),
                geomean_ratio: Some(1.0),
                median_ratio: Some(1.0),
                p90_ratio: Some(1.0),
                p99_ratio: Some(1.0),
            },
        );

        let score = weighted_category_score(&per_category);
        let expected = ((4.0_f64.ln() * 0.35) / 0.65).exp();

        assert!(
            (score.score.expect("score should exist") - expected).abs() < 1.0e-12,
            "weighted score should be a weighted geometric ratio rollup",
        );
        assert!((score.observed_weight - 0.65).abs() < 1.0e-12);
        assert!(score.missing_categories.contains(&"mixed".to_owned()));
    }

    #[test]
    fn build_json_report_uses_stable_ids_and_summary() {
        let report = sample_report();
        let json = build_json_report(
            &report,
            Duration::from_secs(2),
            JsonRunConfig {
                quick: true,
                filter: Some("insert".to_owned()),
                warmup_iterations: WARMUP_ITERS,
                min_iterations: MIN_ITERS,
                max_iterations: MAX_ITERS,
                target_duration_secs: TARGET_DURATION.as_secs(),
                row_counts: vec![100],
                html_output_path: Some("report.html".to_owned()),
                json_output_path: Some("report.json".to_owned()),
                json_stdout: false,
            },
            DetectedEnvironment {
                os: Some("TestOS".to_owned()),
                arch: "x86_64".to_owned(),
                kernel_release: Some("6.0.0-test".to_owned()),
                cpu_model: Some("Test CPU".to_owned()),
                cpu_cores: Some(8),
                ram_gb: Some(32.0),
                active_toolchain: Some("nightly-x86_64-unknown-linux-gnu".to_owned()),
                rust_version: Some("rustc test".to_owned()),
                cargo_version: Some("cargo test".to_owned()),
                git_commit_sha: Some("0123456789abcdef".to_owned()),
                git_branch: Some("main".to_owned()),
                git_head_unix_ts: Some(1_700_000_000),
                git_dirty: Some(false),
                benchmark_binary_modified_unix_ts: Some(1_700_000_001),
                benchmark_binary_older_than_git_head: Some(false),
                build_profile: "release-perf".to_owned(),
            },
            sample_provenance(),
        );

        assert_eq!(json.schema_version, JSON_REPORT_SCHEMA_V6);
        assert_eq!(json.environment.git_head_unix_ts, Some(1_700_000_000));
        assert_eq!(json.environment.git_dirty, Some(false));
        assert_eq!(
            json.environment.benchmark_binary_modified_unix_ts,
            Some(1_700_000_001)
        );
        assert_eq!(
            json.environment.benchmark_binary_older_than_git_head,
            Some(false)
        );
        assert_eq!(json.summary.total_scenarios, 1);
        assert_eq!(json.summary.primary_metric, "per_category_weighted.score");
        assert_eq!(json.summary.per_category["write_bulk"].n, 1);
        assert!(
            (json
                .summary
                .per_category_weighted
                .score
                .expect("primary score should exist")
                - json
                    .summary
                    .geomean_ratio
                    .expect("geomean ratio should exist"))
            .abs()
                < 1.0e-12
        );
        assert_eq!(
            json.ci_regression_gate.schema_version,
            CI_REGRESSION_GATE_SCHEMA_V2
        );
        assert_eq!(json.ci_regression_gate.bead_id, CI_REGRESSION_GATE_BEAD_ID);
        assert_eq!(
            json.ci_regression_gate.depends_on_bead_id,
            CI_REGRESSION_BASELINE_BEAD_ID
        );
        assert_eq!(
            json.ci_regression_gate.thresholds.avg_ratio_baseline,
            CI_REGRESSION_BASELINE_AVG_RATIO
        );
        assert_eq!(json.sections.len(), 1);
        assert_eq!(json.sections[0].section_id, "insert-throughput");
        assert_eq!(
            json.sections[0].rows[0].scenario_id,
            "insert-throughput__100-rows-small-record",
        );
        assert_eq!(json.sections[0].rows[0].category, "write_bulk");
        assert!(
            json.summary
                .average_ratio
                .expect("average ratio should exist for comparable row")
                > 1.0
        );
        assert!(
            json.sections[0].rows[0]
                .fsqlite_concurrent_profile
                .is_none()
        );

        let schema = benchmark_json_schema();
        assert!(
            jsonschema::draft202012::meta::is_valid(&schema),
            "the published V5 schema must itself be valid Draft 2020-12 JSON Schema"
        );
        let instance = serde_json::to_value(&json).expect("report should serialize");
        assert!(
            jsonschema::draft202012::is_valid(&schema, &instance),
            "a complete V5 report must validate against its published schema"
        );

        let mut contradictory_citable = instance.clone();
        contradictory_citable["provenance"]["citable"] = serde_json::Value::Bool(true);
        contradictory_citable["provenance"]["status"] =
            serde_json::Value::String("verified_citable".to_owned());
        contradictory_citable["provenance"]["validation_errors"] = serde_json::json!([]);
        assert!(
            !jsonschema::draft202012::is_valid(&schema, &contradictory_citable),
            "the generic V5 report cannot claim citable provenance"
        );

        let mut disabled_concurrency = instance.clone();
        disabled_concurrency["provenance"]["durability"]["concurrent_mode_default"] =
            serde_json::Value::Bool(false);
        assert!(
            jsonschema::draft202012::is_valid(&schema, &disabled_concurrency),
            "a diagnostic artifact must represent a failed concurrency check honestly"
        );

        let mut mismatched_profile = instance.clone();
        mismatched_profile["provenance"]["build"]["selected_profile"] =
            serde_json::Value::String("release".to_owned());
        assert!(
            jsonschema::draft202012::is_valid(&schema, &mismatched_profile),
            "a diagnostic artifact may preserve a mismatched profile receipt"
        );

        let mut wrong_design = instance.clone();
        wrong_design["provenance"]["runtime_bridge"] = serde_json::Value::String(
            "three_arm_per_operation_inside_existing_runtime_worker_sync_facade".to_owned(),
        );
        assert!(
            !jsonschema::draft202012::is_valid(&schema, &wrong_design),
            "the generic V5 schema must not accept a bridge-experiment provenance shape"
        );

        let mut diagnostic = instance.clone();
        diagnostic["provenance"]["citable"] = serde_json::Value::Bool(false);
        diagnostic["provenance"]["status"] = serde_json::Value::String("unverified".to_owned());
        diagnostic["provenance"]["validation_errors"] = serde_json::json!(["diagnostic fixture"]);
        diagnostic["provenance"]["durability"]["concurrent_mode_default"] =
            serde_json::Value::Bool(false);
        assert!(
            jsonschema::draft202012::is_valid(&schema, &diagnostic),
            "non-citable diagnostic artifacts should represent failed checks honestly"
        );

        let mut missing_build = instance.clone();
        missing_build["provenance"]
            .as_object_mut()
            .expect("provenance should be an object")
            .remove("build");
        assert!(
            !jsonschema::draft202012::is_valid(&schema, &missing_build),
            "the schema must reject missing provenance identity"
        );

        let mut unknown_field = instance;
        unknown_field
            .as_object_mut()
            .expect("report should be an object")
            .insert("unknown_gate0_field".to_owned(), serde_json::Value::Null);
        assert!(
            !jsonschema::draft202012::is_valid(&schema, &unknown_field),
            "the schema must reject unknown top-level fields"
        );
    }

    #[test]
    fn build_json_report_preserves_concurrent_profile_counters() {
        let mut report = BenchReport::new();
        let section = report.add_section(CONCURRENT_WRITERS_SECTION_TITLE, "test");
        section.add_row_with_fsqlite_concurrent_profile(
            "2 writers x 1000 rows",
            Some(sample_measurement("csqlite", 2_000, &[10, 10, 10])),
            Some(sample_measurement("frankensqlite", 2_000, &[12, 13, 14])),
            Some(sample_fsqlite_concurrent_profile()),
        );

        let json = build_json_report(
            &report,
            Duration::from_secs(1),
            JsonRunConfig {
                quick: true,
                filter: Some("concurrent".to_owned()),
                warmup_iterations: WARMUP_ITERS,
                min_iterations: MIN_ITERS,
                max_iterations: MAX_ITERS,
                target_duration_secs: TARGET_DURATION.as_secs(),
                row_counts: vec![100],
                html_output_path: None,
                json_output_path: Some("report.json".to_owned()),
                json_stdout: false,
            },
            DetectedEnvironment {
                os: None,
                arch: "x86_64".to_owned(),
                kernel_release: None,
                cpu_model: None,
                cpu_cores: Some(8),
                ram_gb: None,
                active_toolchain: None,
                rust_version: None,
                cargo_version: None,
                git_commit_sha: None,
                git_branch: Some("main".to_owned()),
                git_head_unix_ts: None,
                git_dirty: Some(false),
                benchmark_binary_modified_unix_ts: None,
                benchmark_binary_older_than_git_head: None,
                build_profile: "release-perf".to_owned(),
            },
            sample_provenance(),
        );

        let profile = json.sections[0].rows[0]
            .fsqlite_concurrent_profile
            .as_ref()
            .expect("concurrent profile should be attached to the row");
        assert_eq!(profile.total_rows, 2_000);
        assert_eq!(profile.counters["mvcc_busy_retries"], 7);
        assert_eq!(profile.counters["mvcc_page_lock_waits"], 3);

        let serialized = serde_json::to_value(&json).expect("report should serialize");
        assert_eq!(
            serialized["sections"][0]["rows"][0]["fsqlite_concurrent_profile"]["counters"]["mvcc_stale_snapshot"],
            2
        );
    }

    #[test]
    fn write_json_report_creates_parent_directories() {
        let report = sample_report();
        let json = build_json_report(
            &report,
            Duration::from_secs(1),
            JsonRunConfig {
                quick: true,
                filter: Some("insert".to_owned()),
                warmup_iterations: WARMUP_ITERS,
                min_iterations: MIN_ITERS,
                max_iterations: MAX_ITERS,
                target_duration_secs: TARGET_DURATION.as_secs(),
                row_counts: vec![100],
                html_output_path: None,
                json_output_path: None,
                json_stdout: false,
            },
            DetectedEnvironment {
                os: Some("TestOS".to_owned()),
                arch: "x86_64".to_owned(),
                kernel_release: None,
                cpu_model: None,
                cpu_cores: Some(8),
                ram_gb: None,
                active_toolchain: None,
                rust_version: None,
                cargo_version: None,
                git_commit_sha: None,
                git_branch: Some("main".to_owned()),
                git_head_unix_ts: None,
                git_dirty: Some(false),
                benchmark_binary_modified_unix_ts: None,
                benchmark_binary_older_than_git_head: None,
                build_profile: "release-perf".to_owned(),
            },
            sample_provenance(),
        );
        let temp = tempfile::tempdir().expect("temp directory should be created");
        let report_path = temp.path().join("nested").join("bench.json");
        let report_path = report_path
            .to_str()
            .expect("temp report path should be valid UTF-8");

        write_json_report(&json, report_path).expect("JSON report should be written");

        let written = std::fs::read_to_string(report_path).expect("JSON report should be written");
        assert!(
            written.contains(JSON_REPORT_SCHEMA_V6),
            "written JSON should include the benchmark schema version"
        );
    }

    #[test]
    fn report_writers_propagate_filesystem_errors_and_html_embeds_provenance() {
        let temp = tempfile::tempdir().expect("temp directory should be created");
        let blocker = temp.path().join("not-a-directory");
        std::fs::write(&blocker, b"file").expect("blocking file should be created");
        let blocked_json = blocker.join("report.json");
        let blocked_json = blocked_json
            .to_str()
            .expect("temp report path should be valid UTF-8");

        let report = sample_report();
        let provenance = sample_provenance();
        let json = build_json_report(
            &report,
            Duration::from_secs(1),
            JsonRunConfig {
                quick: true,
                filter: None,
                warmup_iterations: WARMUP_ITERS,
                min_iterations: MIN_ITERS,
                max_iterations: MAX_ITERS,
                target_duration_secs: TARGET_DURATION.as_secs(),
                row_counts: vec![100],
                html_output_path: None,
                json_output_path: Some(blocked_json.to_owned()),
                json_stdout: false,
            },
            DetectedEnvironment {
                os: None,
                arch: "x86_64".to_owned(),
                kernel_release: None,
                cpu_model: None,
                cpu_cores: Some(8),
                ram_gb: None,
                active_toolchain: None,
                rust_version: None,
                cargo_version: None,
                git_commit_sha: None,
                git_branch: Some("main".to_owned()),
                git_head_unix_ts: None,
                git_dirty: Some(false),
                benchmark_binary_modified_unix_ts: None,
                benchmark_binary_older_than_git_head: None,
                build_profile: "release-perf".to_owned(),
            },
            provenance.clone(),
        );
        assert!(
            write_json_report(&json, blocked_json).is_err(),
            "JSON output errors must propagate to the process boundary"
        );

        let html_path = temp.path().join("report.html");
        let html_path = html_path
            .to_str()
            .expect("temp report path should be valid UTF-8");
        report
            .write_html(html_path, &provenance)
            .expect("HTML report should be written");
        let html = std::fs::read_to_string(html_path).expect("HTML report should be readable");
        assert!(html.contains("benchmark-provenance"));
        assert!(html.contains(BENCHMARK_PROVENANCE_SCHEMA_V3));
        assert!(html.contains("NON-CITABLE DIAGNOSTIC"));
        assert!(html.contains("generic comprehensive measurements are diagnostic-only"));
    }

    #[test]
    fn sha256_fixture_is_stable() {
        assert_eq!(
            sha256_bytes(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn ci_regression_gate_tracks_multithread_p95_ratio() {
        let mut report = BenchReport::new();
        let section = report.add_section(CONCURRENT_WRITERS_SECTION_TITLE, "test");
        section.add_row(
            "2 writers x 1000 rows",
            Some(sample_measurement("csqlite", 2_000, &[10, 10, 10])),
            Some(sample_measurement("frankensqlite", 2_000, &[20, 25, 30])),
        );
        section.add_row(
            "8 writers x 1000 rows",
            Some(sample_measurement("csqlite", 8_000, &[10, 10, 10])),
            Some(sample_measurement("frankensqlite", 8_000, &[15, 15, 15])),
        );

        let json = build_json_report(
            &report,
            Duration::from_secs(1),
            JsonRunConfig {
                quick: true,
                filter: Some("concurrent".to_owned()),
                warmup_iterations: WARMUP_ITERS,
                min_iterations: MIN_ITERS,
                max_iterations: MAX_ITERS,
                target_duration_secs: TARGET_DURATION.as_secs(),
                row_counts: vec![100],
                html_output_path: None,
                json_output_path: Some("report.json".to_owned()),
                json_stdout: false,
            },
            DetectedEnvironment {
                os: None,
                arch: "x86_64".to_owned(),
                kernel_release: None,
                cpu_model: None,
                cpu_cores: Some(8),
                ram_gb: None,
                active_toolchain: None,
                rust_version: None,
                cargo_version: None,
                git_commit_sha: None,
                git_branch: Some("main".to_owned()),
                git_head_unix_ts: None,
                git_dirty: Some(false),
                benchmark_binary_modified_unix_ts: None,
                benchmark_binary_older_than_git_head: None,
                build_profile: "release-perf".to_owned(),
            },
            sample_provenance(),
        );

        assert_eq!(
            json.ci_regression_gate.observed.max_mt_p95_scenario_id,
            Some(
                "concurrent-writers-c-sqlite-wal-vs-frankensqlite-mvcc__2-writers-x-1000-rows"
                    .to_owned(),
            )
        );
        assert_eq!(json.ci_regression_gate.observed.max_mt_p95_ratio, Some(3.0));
        assert_eq!(
            json.ci_regression_gate.observed.primary_score,
            json.summary.per_category_weighted.score
        );
        assert_eq!(json.sections[0].rows[0].category, "concurrent_writers");
    }

    #[test]
    fn benchmark_json_schema_exposes_gate_metrics() {
        let schema = benchmark_json_schema();

        assert_eq!(
            schema["properties"]["schema_version"]["const"],
            JSON_REPORT_SCHEMA_V6
        );
        assert_eq!(
            schema["properties"]["ci_regression_gate"]["properties"]["bead_id"]["const"],
            CI_REGRESSION_GATE_BEAD_ID
        );
        assert_eq!(
            schema["properties"]["summary"]["properties"]["primary_metric"]["const"],
            "per_category_weighted.score"
        );
        assert_eq!(
            schema["properties"]["ci_regression_gate"]["properties"]["thresholds"]["properties"]["primary_score_max_regression_pct"]
                ["type"],
            "number"
        );
        assert_eq!(
            schema["properties"]["sections"]["items"]["properties"]["rows"]["items"]["properties"]
                ["category"]["$ref"],
            "#/$defs/scenario_category"
        );
        assert_eq!(
            schema["properties"]["sections"]["items"]["properties"]["rows"]["items"]["properties"]
                ["fsqlite_concurrent_profile"]["$ref"],
            "#/$defs/fsqlite_concurrent_profile"
        );
        assert_eq!(
            schema["properties"]["sections"]["items"]["properties"]["rows"]["items"]["properties"]
                ["concurrent_readiness"]["$ref"],
            "#/$defs/concurrent_readiness"
        );
        assert_eq!(
            schema["$defs"]["fsqlite_concurrent_profile"]["properties"]["counters"]["additionalProperties"]
                ["type"],
            "integer"
        );
        assert_eq!(schema["$defs"]["scenario_category"]["enum"][5], "mixed");
        assert_eq!(
            schema["properties"]["summary"]["properties"]["per_category"]["required"][5],
            "mixed"
        );
        assert_eq!(
            schema["properties"]["ci_regression_gate"]["properties"]["observed"]["properties"]["primary_score"]
                ["type"][0],
            "number"
        );
    }

    #[test]
    fn build_identity_validation_rejects_profile_misrepresentation() {
        let canonical = sample_provenance().build;
        assert_eq!(validate_build_identity(&canonical), Vec::<String>::new());
        let canonical_environment =
            canonical_profile_environment("release-perf").expect("test profile is canonical");
        assert_eq!(
            canonical_environment
                .get("RUSTC_WRAPPER")
                .map(String::as_str),
            Some("")
        );
        assert_eq!(
            canonical_environment
                .get("RUSTC_WORKSPACE_WRAPPER")
                .map(String::as_str),
            Some("")
        );

        let mut profile_mismatch = canonical.clone();
        profile_mismatch.declared_profile = "release".to_owned();
        assert!(
            validate_build_identity(&profile_mismatch)
                .iter()
                .any(|error| error.contains("does not match Cargo output profile"))
        );

        let mut wrong_opt = canonical.clone();
        wrong_opt.opt_level = "2".to_owned();
        assert!(
            validate_build_identity(&wrong_opt)
                .iter()
                .any(|error| error.contains("effective opt-level"))
        );

        let mut debug_assertions = canonical.clone();
        debug_assertions.debug_assertions = true;
        assert!(
            validate_build_identity(&debug_assertions)
                .iter()
                .any(|error| error.contains("debug assertions"))
        );

        let mut unwind = canonical.clone();
        unwind.panic_strategy = "unwind".to_owned();
        unwind.panic_abort = false;
        assert!(
            validate_build_identity(&unwind)
                .iter()
                .any(|error| error.contains("panic=abort"))
        );

        let mut rustflags_override = canonical.clone();
        rustflags_override.encoded_rustflags_hex = lowercase_hex(b"-Copt-level=0");
        assert!(
            validate_build_identity(&rustflags_override)
                .iter()
                .any(|error| error.contains("require empty encoded rustflags"))
        );

        let mut wrapper_environment = canonical_environment;
        wrapper_environment.insert("RUSTC_WRAPPER".to_owned(), "sccache".to_owned());
        let mut wrapper_override = canonical.clone();
        wrapper_override.profile_override_environment_hex =
            encode_build_environment(&wrapper_environment);
        assert!(
            validate_build_identity(&wrapper_override)
                .iter()
                .any(|error| error.contains("does not exactly force the canonical"))
        );

        let mut environment_override = canonical;
        environment_override.profile_override_environment_hex =
            lowercase_hex(b"CARGO_INCREMENTAL=1");
        assert!(
            validate_build_identity(&environment_override)
                .iter()
                .any(|error| error.contains("does not exactly force the canonical"))
        );
    }

    fn canonical_test_build_log(nonce: &str) -> String {
        let required_crates = [
            "asupersync",
            "fsqlite_error",
            "fsqlite_types",
            "fsqlite_observability",
            "fsqlite_ast",
            "fsqlite_parser",
            "fsqlite_planner",
            "fsqlite_func",
            "fsqlite_vfs",
            "fsqlite_wal",
            "fsqlite_pager",
            "fsqlite_mvcc",
            "fsqlite_btree",
            "fsqlite_vdbe",
            "fsqlite_ext_json",
            "fsqlite_ext_fts5",
            "fsqlite_ext_rtree",
            "fsqlite_core",
            "fsqlite",
            "fsqlite_e2e",
            "comprehensive_bench",
        ];
        let mut log = required_crates
            .iter()
            .map(|crate_name| {
                let nonce_environment = if *crate_name == "comprehensive_bench" {
                    format!("FSQLITE_BENCH_BUILD_NONCE={nonce} ")
                } else {
                    String::new()
                };
                let lto_and_binary_options = if *crate_name == "comprehensive_bench" {
                    "-C lto -C panic=abort -C strip=symbols"
                } else {
                    "-C linker-plugin-lto"
                };
                format!(
                    "Running `CARGO_PKG_DESCRIPTION='FrankenSQLite differential benchmark package' CARGO_PKG_NAME=fsqlite-e2e {nonce_environment}/opt/rust/bin/rustc --crate-name {crate_name} --target x86_64-unknown-linux-gnu -C opt-level=3 -C codegen-units=1 {lto_and_binary_options} --emit link`"
                )
            })
            .collect::<Vec<_>>()
            .join("\n");
        log.push_str("\nFinished `release-perf` profile [optimized] target(s) in 1.00s\n");
        log
    }

    #[test]
    fn verbose_build_log_proof_is_nonce_bound_and_rejects_ambiguous_invocations() {
        let nonce = "56".repeat(32);
        let canonical = canonical_test_build_log(&nonce);
        assert!(build_log_proves_profile(
            &canonical,
            &nonce,
            "release-perf",
            "x86_64-unknown-linux-gnu"
        ));

        assert!(!build_log_proves_profile(
            &canonical,
            &"78".repeat(32),
            "release-perf",
            "x86_64-unknown-linux-gnu"
        ));
        assert!(!build_log_proves_profile(
            &canonical,
            &nonce,
            "release",
            "x86_64-unknown-linux-gnu"
        ));

        let duplicated = format!(
            "{canonical}{}",
            canonical
                .lines()
                .find(|line| line.contains("--crate-name fsqlite_core"))
                .expect("canonical fixture includes fsqlite_core")
        );
        assert!(!build_log_proves_profile(
            &duplicated,
            &nonce,
            "release-perf",
            "x86_64-unknown-linux-gnu"
        ));

        let missing_codegen_flag = canonical.replace("-C codegen-units=1 ", "");
        assert!(!build_log_proves_profile(
            &missing_codegen_flag,
            &nonce,
            "release-perf",
            "x86_64-unknown-linux-gnu"
        ));

        let conflicting_opt_level =
            canonical.replace("-C opt-level=3", "-C opt-level=3 -C opt-level=0");
        assert!(!build_log_proves_profile(
            &conflicting_opt_level,
            &nonce,
            "release-perf",
            "x86_64-unknown-linux-gnu"
        ));

        let wrapped = canonical.replacen("rustc --crate-name", "sccache rustc --crate-name", 1);
        assert!(!build_log_proves_profile(
            &wrapped,
            &nonce,
            "release-perf",
            "x86_64-unknown-linux-gnu"
        ));

        assert_eq!(
            split_posix_shell_words(
                "A='multi word value' B=\"two words\" /opt/rust/bin/rustc --crate-name demo"
            ),
            Some(vec![
                "A=multi word value".to_owned(),
                "B=two words".to_owned(),
                "/opt/rust/bin/rustc".to_owned(),
                "--crate-name".to_owned(),
                "demo".to_owned(),
            ])
        );
        assert!(split_posix_shell_words("A='unterminated").is_none());
    }

    #[test]
    fn generic_comprehensive_measurement_design_is_never_citable() {
        assert!(
            measurement_design_validation_errors("scenario_scoped_thread_local_block_on")
                .iter()
                .any(|error| error.contains("diagnostic-only"))
        );
        assert!(
            measurement_design_validation_errors(
                "three_arm_per_operation_inside_existing_runtime_worker_sync_facade"
            )
            .iter()
            .any(|error| error.contains("isolated cgroup-v2 cpuset"))
        );
    }

    #[cfg(feature = "bridge-experiment")]
    fn bridge_test_sample(
        workload: BridgeWorkload,
        operation_count: usize,
        block_index: usize,
        arm: BridgeArm,
        elapsed_ns: u64,
    ) -> JsonBridgeSample {
        let checksum = bridge_expected_checksum(operation_count).expect("test checksum should fit");
        JsonBridgeSample {
            workload,
            operation_count,
            block_index,
            order_slot: 0,
            arm,
            elapsed_ns,
            runtime_entries_total: 0,
            runtime_entries_inside_timed_region: 0,
            caller_future_completions_inside_timed_region: 0,
            engine_dml_future_calls_inside_timed_region: 0,
            worker_commands_total: 0,
            worker_commands_inside_timed_region: 0,
            worker_open_handshakes_total: 0,
            effective_settings: if workload == BridgeWorkload::ReadyFuture {
                BTreeMap::new()
            } else {
                bridge_expected_effective_settings()
            },
            oracle_kind: "test_fixture".to_owned(),
            checksum_count: checksum.0,
            checksum_sum: checksum.1,
            checksum_exact_rows: i64::try_from(operation_count)
                .expect("test operation count should fit i64"),
        }
    }

    #[cfg(feature = "bridge-experiment")]
    #[test]
    fn bridge_json_schema_validates_complete_report_and_rejects_drift() {
        let host_state = JsonBridgeHostState {
            captured_at_utc: "2026-07-26T00:00:00Z".to_owned(),
            load_average_1m: Some(0.5),
            load_average_5m: Some(0.4),
            load_average_15m: Some(0.3),
            available_parallelism: Some(2),
            cpu_affinity: Some("2-3".to_owned()),
            selected_cpu_topology: BTreeMap::from([
                (
                    "cpu2".to_owned(),
                    "package=0,core=2,thread_siblings=2,66,numa=node0".to_owned(),
                ),
                (
                    "cpu3".to_owned(),
                    "package=0,core=3,thread_siblings=3,67,numa=node0".to_owned(),
                ),
            ]),
            scaling_governors: vec!["performance".to_owned()],
            energy_performance_preferences: vec!["performance".to_owned()],
            boost_controls: BTreeMap::from([("cpufreq.boost".to_owned(), "1".to_owned())]),
            numa_nodes_online: Some("0".to_owned()),
            memory_available_gb: Some(64.0),
            cpu_pressure_some_avg10: Some(0.0),
            io_pressure_some_avg60: Some(0.0),
            competing_processes: Vec::new(),
            competing_process_scan_error: None,
        };
        let sample = bridge_test_sample(
            BridgeWorkload::ReadyFuture,
            100,
            0,
            BridgeArm::PerOperationBlockOn,
            1_000,
        );
        let mut bridge_provenance = sample_provenance();
        bridge_provenance.citable = false;
        bridge_provenance.status = "unverified_explicit_override".to_owned();
        bridge_provenance.runtime_bridge =
            "three_arm_per_operation_inside_existing_runtime_worker_sync_facade".to_owned();
        bridge_provenance.validation_errors =
            vec!["test fixture models the diagnostic-only bridge contract".to_owned()];
        let report = JsonBridgeReport {
            schema_version: BRIDGE_REPORT_SCHEMA_V2.to_owned(),
            generated_at_utc: "2026-07-26T00:00:01Z".to_owned(),
            provenance: bridge_provenance,
            environment: DetectedEnvironment {
                os: Some("Linux".to_owned()),
                arch: "x86_64".to_owned(),
                kernel_release: Some("test".to_owned()),
                cpu_model: Some("test".to_owned()),
                cpu_cores: Some(2),
                ram_gb: Some(64.0),
                active_toolchain: Some("nightly".to_owned()),
                rust_version: Some("rustc test".to_owned()),
                cargo_version: Some("cargo test".to_owned()),
                git_commit_sha: Some("0123456789abcdef0123456789abcdef01234567".to_owned()),
                git_branch: Some("main".to_owned()),
                git_head_unix_ts: Some(1_700_000_000),
                git_dirty: Some(false),
                benchmark_binary_modified_unix_ts: Some(1_700_000_001),
                benchmark_binary_older_than_git_head: Some(false),
                build_profile: "release-perf".to_owned(),
            },
            host_state_before: host_state.clone(),
            host_state_checkpoints: vec![host_state.clone()],
            host_state_after: host_state,
            config: JsonBridgeConfig {
                samples_per_arm: 96,
                raw_insert_operations: 100,
                ready_operation_counts: vec![1, 10, 100, 1_000],
                order_seed: 7,
                ordering_policy: "test".to_owned(),
                warmup_policy: "test".to_owned(),
                timed_region: "test".to_owned(),
                arm_contracts: BTreeMap::from([
                    (
                        BridgeArm::PerOperationBlockOn.id().to_owned(),
                        "test".to_owned(),
                    ),
                    (
                        BridgeArm::SingleRuntimeEntry.id().to_owned(),
                        "test".to_owned(),
                    ),
                    (
                        BridgeArm::WorkerSyncFacade.id().to_owned(),
                        "test".to_owned(),
                    ),
                ]),
                affinity_policy: "test".to_owned(),
                max_load_average_1m: Some(1.0),
            },
            raw_samples: vec![sample],
            arm_statistics: vec![JsonBridgeArmStats {
                workload: BridgeWorkload::ReadyFuture,
                operation_count: 100,
                arm: BridgeArm::PerOperationBlockOn,
                samples: 1,
                median_ns: 1_000.0,
                mean_ns: 1_000.0,
                p95_ns: 1_000.0,
                stddev_ns: 0.0,
                cv_pct: 0.0,
                median_ns_per_operation: 10.0,
            }],
            paired_comparisons: vec![JsonBridgePairedComparison {
                workload: BridgeWorkload::ReadyFuture,
                operation_count: 100,
                numerator: BridgeArm::PerOperationBlockOn,
                denominator: BridgeArm::SingleRuntimeEntry,
                paired_blocks: 1,
                bootstrap_clusters: 1,
                median_ratio: 2.0,
                mean_ratio: 2.0,
                geomean_ratio: 2.0,
                bootstrap_mean_ratio_ci95_low: 1.9,
                bootstrap_mean_ratio_ci95_high: 2.1,
            }],
            ready_runtime_entry_regression: JsonBridgeReadyRegression {
                predictor: "test".to_owned(),
                response: "test".to_owned(),
                interpretation: "test".to_owned(),
                points: 8,
                paired_blocks: 2,
                bootstrap_clusters: 1,
                intercept_ns: 10.0,
                slope_ns_per_additional_runtime_entry: 5.0,
                bootstrap_slope_ci95_low: 4.0,
                bootstrap_slope_ci95_high: 6.0,
                r_squared: 1.0,
            },
        };
        let schema = bridge_json_schema();
        assert!(
            jsonschema::draft202012::meta::is_valid(&schema),
            "bridge schema must itself be valid Draft 2020-12"
        );
        let instance = serde_json::to_value(report).expect("bridge report should serialize");
        assert!(
            jsonschema::draft202012::is_valid(&schema, &instance),
            "complete bridge report must validate"
        );

        let mut missing_provenance = instance.clone();
        missing_provenance
            .as_object_mut()
            .unwrap()
            .remove("provenance");
        assert!(!jsonschema::draft202012::is_valid(
            &schema,
            &missing_provenance
        ));

        let mut unknown_field = instance;
        unknown_field
            .as_object_mut()
            .unwrap()
            .insert("unknown_bridge_field".to_owned(), serde_json::Value::Null);
        assert!(!jsonschema::draft202012::is_valid(&schema, &unknown_field));
    }

    #[cfg(feature = "bridge-experiment")]
    #[test]
    fn bridge_orders_are_mirrored_and_balanced() {
        let mut rng = StdRng::seed_from_u64(7);
        let two_arm_orders = bridge_two_arm_orders(100, &mut rng)
            .expect("complete complementary two-arm pairs should be valid");
        for pair in two_arm_orders.chunks_exact(2) {
            for order in pair {
                assert_eq!(order[0], order[3]);
                assert_eq!(order[1], order[2]);
                assert_ne!(order[0], order[1]);
            }
            assert_ne!(
                pair[0][0], pair[1][0],
                "adjacent blocks must be complementary ABBA/BAAB orders"
            );
        }
        assert!(bridge_two_arm_orders(99, &mut rng).is_err());

        let orders = bridge_three_arm_orders(6, &mut rng)
            .expect("two complete carryover cycles should be valid");
        let mut position_counts = BTreeMap::new();
        let mut predecessor_counts = BTreeMap::new();
        for order in &orders {
            assert_eq!(order[0], order[5]);
            assert_eq!(order[1], order[4]);
            assert_eq!(order[2], order[3]);
            for (position, arm) in order.iter().copied().enumerate() {
                *position_counts.entry((arm, position)).or_insert(0_usize) += 1;
            }
            for pair in order.windows(2) {
                *predecessor_counts
                    .entry((pair[0], pair[1]))
                    .or_insert(0_usize) += 1;
            }
        }
        for arm in BridgeArm::ALL {
            for position in 0..6 {
                assert_eq!(
                    position_counts[&(arm, position)],
                    2,
                    "{arm:?} should appear twice in position {position}"
                );
            }
            for successor in BridgeArm::ALL {
                assert_eq!(
                    predecessor_counts[&(arm, successor)],
                    if arm == successor { 2 } else { 4 },
                    "{arm:?} -> {successor:?} carryover should be exact"
                );
            }
        }
        assert!(bridge_three_arm_orders(5, &mut rng).is_err());
    }

    #[cfg(feature = "bridge-experiment")]
    #[test]
    fn bridge_percentile_interpolates_even_sample_median() {
        let sorted = [1.0, 3.0, 7.0, 9.0];
        assert!((bridge_percentile(&sorted, 50.0) - 5.0).abs() < f64::EPSILON);
    }

    #[cfg(feature = "bridge-experiment")]
    #[test]
    fn bridge_ready_count_orders_balance_position_and_predecessor() {
        let operation_counts = [1_usize, 10, 100, 1_000];
        let mut rng = StdRng::seed_from_u64(17);
        let orders = bridge_balanced_ready_count_orders(&operation_counts, 16, &mut rng)
            .expect("two complete Williams cycles should be valid");
        assert_eq!(orders.len(), 16);

        let mut position_counts = BTreeMap::new();
        let mut predecessor_counts = BTreeMap::new();
        for order in &orders {
            assert_eq!(order.len(), operation_counts.len());
            for (position, operation_count) in order.iter().copied().enumerate() {
                *position_counts
                    .entry((operation_count, position))
                    .or_insert(0_usize) += 1;
            }
            for pair in order.windows(2) {
                assert_ne!(pair[0], pair[1]);
                *predecessor_counts
                    .entry((pair[0], pair[1]))
                    .or_insert(0_usize) += 1;
            }
        }

        for operation_count in operation_counts {
            for position in 0..operation_counts.len() {
                assert_eq!(position_counts[&(operation_count, position)], 4);
            }
            for successor in operation_counts {
                if successor != operation_count {
                    assert_eq!(predecessor_counts[&(operation_count, successor)], 4);
                }
            }
        }

        assert!(
            bridge_balanced_ready_count_orders(&operation_counts, 10, &mut rng).is_err(),
            "an incomplete Williams cycle must fail instead of silently biasing positions"
        );
    }

    #[cfg(feature = "bridge-experiment")]
    #[test]
    fn bridge_affinity_parser_and_sample_policy_fail_closed() {
        assert_eq!(bridge_cpu_affinity_cardinality("2").unwrap(), 1);
        assert_eq!(bridge_cpu_affinity_cardinality("2-3").unwrap(), 2);
        assert_eq!(bridge_cpu_affinity_cardinality("2,4").unwrap(), 2);
        assert!(bridge_cpu_affinity_cardinality("").is_err());
        assert!(bridge_cpu_affinity_cardinality("3-2").is_err());
        assert!(bridge_cpu_affinity_cardinality("2-3,3-4").is_err());

        let valid = vec![
            "comprehensive-bench".to_owned(),
            "--bridge-experiment".to_owned(),
            "--bridge-samples".to_owned(),
            "48".to_owned(),
        ];
        assert_eq!(parse_cli_args(&valid).unwrap().bridge_samples, 48);

        for invalid in ["20", "40", "64", "96x"] {
            let args = vec![
                "comprehensive-bench".to_owned(),
                "--bridge-experiment".to_owned(),
                "--bridge-samples".to_owned(),
                invalid.to_owned(),
            ];
            assert!(parse_cli_args(&args).is_err());
        }
    }

    #[cfg(feature = "bridge-experiment")]
    #[test]
    fn bridge_competitor_classification_uses_full_process_identity() {
        assert!(
            !bridge_process_is_competitor("btrfs-cleaner", &[], None),
            "an idle kernel thread must not fail the run by mere existence"
        );
        assert!(
            bridge_process_is_competitor(
                "btrfs",
                &["/usr/bin/btrfs".to_owned(), "scrub".to_owned()],
                Some("0::/user.slice/test.scope")
            ),
            "a userspace Btrfs maintenance command must remain blocked"
        );
        assert!(
            !bridge_process_is_competitor(
                "cargo-io-enforc",
                &[
                    "/bin/bash".to_owned(),
                    "/home/ubuntu/.local/bin/cargo-io-enforcer".to_owned(),
                ],
                Some("0::/system.slice/cargo-io-enforcer.service")
            ),
            "the exact permanent build-priority monitor is not a Cargo build"
        );
        assert!(
            bridge_process_is_competitor(
                "cargo-io-enforc",
                &["/bin/bash".to_owned(), "/tmp/cargo-io-enforcer".to_owned()],
                Some("0::/user.slice/untrusted.scope")
            ),
            "a lookalike outside the exact service cgroup must fail closed"
        );
        assert!(bridge_process_is_competitor(
            "cargo",
            &["cargo".to_owned(), "build".to_owned()],
            Some("0::/user.slice/test.scope")
        ));
        assert!(bridge_process_is_competitor(
            "sbh",
            &["sbh".to_owned(), "daemon".to_owned()],
            Some("0::/user.slice/test.scope")
        ));
    }

    #[cfg(feature = "bridge-experiment")]
    #[test]
    fn bridge_paired_comparison_uses_block_means() {
        let mut samples = Vec::new();
        for block_index in 0..12 {
            samples.push(bridge_test_sample(
                BridgeWorkload::RawExecuteWithParams,
                100,
                block_index,
                BridgeArm::PerOperationBlockOn,
                200,
            ));
            samples.push(bridge_test_sample(
                BridgeWorkload::RawExecuteWithParams,
                100,
                block_index,
                BridgeArm::PerOperationBlockOn,
                220,
            ));
            samples.push(bridge_test_sample(
                BridgeWorkload::RawExecuteWithParams,
                100,
                block_index,
                BridgeArm::SingleRuntimeEntry,
                100,
            ));
            samples.push(bridge_test_sample(
                BridgeWorkload::RawExecuteWithParams,
                100,
                block_index,
                BridgeArm::SingleRuntimeEntry,
                110,
            ));
        }

        let comparison = bridge_paired_comparison(
            &samples,
            BridgeWorkload::RawExecuteWithParams,
            100,
            BridgeArm::PerOperationBlockOn,
            BridgeArm::SingleRuntimeEntry,
            3,
            99,
        )
        .expect("balanced blocks should compare");
        assert_eq!(comparison.paired_blocks, 12);
        assert_eq!(comparison.bootstrap_clusters, 4);
        assert!((comparison.median_ratio - 2.0).abs() < f64::EPSILON);
        assert!((comparison.mean_ratio - 2.0).abs() < f64::EPSILON);
        assert!((comparison.geomean_ratio - 2.0).abs() < f64::EPSILON);
        assert!((comparison.bootstrap_mean_ratio_ci95_low - 2.0).abs() < f64::EPSILON);
        assert!((comparison.bootstrap_mean_ratio_ci95_high - 2.0).abs() < f64::EPSILON);
    }

    #[cfg(feature = "bridge-experiment")]
    #[test]
    fn bridge_ready_regression_recovers_runtime_entry_slope() {
        let mut samples = Vec::new();
        for block_index in 0..16 {
            for operation_count in [1_usize, 10, 100, 1_000] {
                let baseline = 100_u64;
                let per_operation = baseline
                    + 5 * u64::try_from(operation_count.saturating_sub(1))
                        .expect("test count should fit");
                for _ in 0..2 {
                    samples.push(bridge_test_sample(
                        BridgeWorkload::ReadyFuture,
                        operation_count,
                        block_index,
                        BridgeArm::SingleRuntimeEntry,
                        baseline,
                    ));
                    samples.push(bridge_test_sample(
                        BridgeWorkload::ReadyFuture,
                        operation_count,
                        block_index,
                        BridgeArm::PerOperationBlockOn,
                        per_operation,
                    ));
                }
            }
        }

        let regression = bridge_ready_regression(&samples, 8, 123).expect("fixture should regress");
        assert_eq!(regression.points, 64);
        assert_eq!(regression.paired_blocks, 16);
        assert_eq!(regression.bootstrap_clusters, 2);
        assert!(regression.intercept_ns.abs() < 1.0e-9);
        assert!((regression.slope_ns_per_additional_runtime_entry - 5.0).abs() < 1.0e-9);
        assert!((regression.bootstrap_slope_ci95_low - 5.0).abs() < 1.0e-9);
        assert!((regression.bootstrap_slope_ci95_high - 5.0).abs() < 1.0e-9);
        assert!((regression.r_squared - 1.0).abs() < 1.0e-12);
    }

    #[cfg(feature = "bridge-experiment")]
    #[test]
    fn bridge_ready_samples_count_exact_runtime_entries_and_future_calls() {
        let per_operation =
            bridge_sample_ready_per_operation(3, 0, 0).expect("ready sample should run");
        assert_eq!(
            per_operation.runtime_entries_total, 4,
            "one untimed sentinel probe plus three timed entries"
        );
        assert_eq!(per_operation.runtime_entries_inside_timed_region, 3);
        assert_eq!(
            per_operation.caller_future_completions_inside_timed_region,
            3
        );
        assert_eq!(per_operation.engine_dml_future_calls_inside_timed_region, 0);
        assert_eq!(
            (per_operation.checksum_count, per_operation.checksum_sum),
            (3, 0)
        );
        assert_eq!(per_operation.checksum_exact_rows, 3);

        let runtime = RuntimeBuilder::current_thread()
            .build()
            .expect("test runtime should build");
        let single_runtime =
            bridge_sample_ready_single_runtime(&runtime, 3, 0, 1).expect("ready sample should run");
        assert_eq!(single_runtime.runtime_entries_total, 1);
        assert_eq!(single_runtime.runtime_entries_inside_timed_region, 0);
        assert_eq!(
            single_runtime.caller_future_completions_inside_timed_region,
            3
        );
        assert_eq!(
            single_runtime.engine_dml_future_calls_inside_timed_region,
            0
        );
        assert_eq!(
            (single_runtime.checksum_count, single_runtime.checksum_sum),
            (3, 0)
        );
        assert_eq!(single_runtime.checksum_exact_rows, 3);
    }

    #[cfg(feature = "bridge-experiment")]
    #[test]
    fn bridge_worker_sample_accounts_for_every_command_and_checks_rows() {
        let sample = bridge_sample_insert_worker(3, 7, 2).expect("worker sample should run");
        assert_eq!(sample.workload, BridgeWorkload::RawExecuteWithParams);
        assert_eq!(sample.arm, BridgeArm::WorkerSyncFacade);
        assert_eq!(sample.operation_count, 3);
        assert_eq!(sample.block_index, 7);
        assert_eq!(sample.order_slot, 2);
        assert_eq!(sample.runtime_entries_total, 0);
        assert_eq!(sample.runtime_entries_inside_timed_region, 0);
        assert_eq!(sample.caller_future_completions_inside_timed_region, 0);
        assert_eq!(sample.engine_dml_future_calls_inside_timed_region, 3);
        assert_eq!(sample.worker_commands_inside_timed_region, 3);
        assert_eq!(sample.worker_open_handshakes_total, 1);
        assert_eq!(
            sample.worker_commands_total,
            bridge_pragmas().len() + 5 + 7 + 3
        );
        assert_eq!(sample.oracle_kind, "untimed_exact_id_value_domain_query");
        assert_eq!((sample.checksum_count, sample.checksum_sum), (3, 3));
        assert_eq!(sample.checksum_exact_rows, 3);
    }
}

// ─── Section 5: Read-after-write performance ───────────────────────────

fn bench_read_after_write(report: &mut BenchReport, row_counts: &[usize]) {
    let section = report.add_section(
        "Read-After-Write Query Performance",
        "Insert N rows, then benchmark various SELECT patterns. Record: small_3col.",
    );

    let record_size = RecordSize::Small;

    for &count in row_counts {
        // Skip very large for query benchmarks.
        if count > 100_000 {
            continue;
        }

        eprint!("  Setting up {count} rows for read benchmarks... ");

        // Set up C SQLite.
        let cs_conn = {
            let conn = rusqlite::Connection::open_in_memory().unwrap();
            apply_pragmas_csqlite(&conn);
            let create_sql = record_size.create_table_sql();
            conn.execute_batch(&format!("{create_sql};")).unwrap();
            conn.execute_batch("BEGIN").unwrap();
            {
                let mut stmt = conn.prepare(record_size.insert_sql_csqlite()).unwrap();
                #[allow(clippy::cast_possible_wrap)]
                for i in 0..count as i64 {
                    stmt.execute(rusqlite::params![i]).unwrap();
                }
            }
            conn.execute_batch("COMMIT").unwrap();
            // Create secondary index.
            conn.execute_batch("CREATE INDEX idx_name ON bench(name);")
                .unwrap();
            conn
        };

        // Set up FrankenSQLite.
        let fs_conn = {
            let conn = open_fsqlite_memory_connection_for_benchmark();
            apply_pragmas_fsqlite(&conn);
            fs_execute(&conn, record_size.create_table_sql());
            fs_execute(&conn, "BEGIN");
            {
                let stmt = fs_prepare(&conn, record_size.insert_sql_csqlite());
                #[allow(clippy::cast_possible_wrap)]
                for i in 0..count as i64 {
                    fs_stmt_execute_with_params(&stmt, &[fsqlite::SqliteValue::Integer(i)]);
                }
            }
            fs_execute(&conn, "COMMIT");
            fs_execute(&conn, "CREATE INDEX idx_name ON bench(name)");
            conn
        };

        eprintln!("done.");

        // Full table scan.
        eprint!("    Full table scan... ");
        let cs = {
            let mut stmt = cs_conn.prepare("SELECT * FROM bench").unwrap();
            measure(&format!("cs_scan_{count}"), count, || {
                let _rows = collect_rusqlite_rows(&mut stmt, []).unwrap();
            })
        };
        let fs_stmt = fs_prepare(&fs_conn, "SELECT * FROM bench");
        let fs = measure(&format!("fs_scan_{count}"), count, || {
            let _rows = fsqlite_e2e::block_on(fs_stmt.query()).unwrap();
        });
        eprintln!(
            "C={} F={}",
            format_duration(cs.median()),
            format_duration(fs.median())
        );
        section.add_row(
            &format!("{count} rows / full table scan"),
            Some(cs),
            Some(fs),
        );

        // Point lookup by PK.
        eprint!("    Point lookup (PK)... ");
        let target_id = (count / 2) as i64;
        let cs = {
            let mut stmt = cs_conn
                .prepare("SELECT * FROM bench WHERE id = ?1")
                .unwrap();
            measure(&format!("cs_pk_{count}"), 1, || {
                let _rows = collect_rusqlite_rows(&mut stmt, rusqlite::params![target_id]).unwrap();
            })
        };
        let fs_stmt = fs_prepare(&fs_conn, "SELECT * FROM bench WHERE id = ?1");
        let fs = measure(&format!("fs_pk_{count}"), 1, || {
            let _row = fsqlite_e2e::block_on(
                fs_stmt.query_row_with_params(&[fsqlite::SqliteValue::Integer(target_id)]),
            )
            .unwrap();
        });
        eprintln!(
            "C={} F={}",
            format_duration(cs.median()),
            format_duration(fs.median())
        );
        section.add_row(
            &format!("{count} rows / point lookup (PK)"),
            Some(cs),
            Some(fs),
        );

        // Range scan (10% of table).
        let range_size = count / 10;
        let range_start = (count / 4) as i64;
        #[allow(clippy::cast_possible_wrap)]
        let range_end = range_start + range_size as i64;
        eprint!("    Range scan ({range_size} rows)... ");
        let cs = {
            let mut stmt = cs_conn
                .prepare("SELECT * FROM bench WHERE id >= ?1 AND id < ?2")
                .unwrap();
            measure(&format!("cs_range_{count}"), range_size, || {
                let _rows =
                    collect_rusqlite_rows(&mut stmt, rusqlite::params![range_start, range_end])
                        .unwrap();
            })
        };
        let fs_stmt = fs_prepare(&fs_conn, "SELECT * FROM bench WHERE id >= ?1 AND id < ?2");
        let fs = measure(&format!("fs_range_{count}"), range_size, || {
            let _rows = fsqlite_e2e::block_on(fs_stmt.query_with_params(&[
                fsqlite::SqliteValue::Integer(range_start),
                fsqlite::SqliteValue::Integer(range_end),
            ]))
            .unwrap();
        });
        eprintln!(
            "C={} F={}",
            format_duration(cs.median()),
            format_duration(fs.median())
        );
        section.add_row(
            &format!("{count} rows / range scan ({range_size} rows)"),
            Some(cs),
            Some(fs),
        );

        // COUNT(*)
        eprint!("    COUNT(*)... ");
        let cs = {
            let mut stmt = cs_conn.prepare("SELECT COUNT(*) FROM bench").unwrap();
            measure(&format!("cs_count_{count}"), 1, || {
                let _: i64 = stmt.query_row([], |r| r.get(0)).unwrap();
            })
        };
        let fs_stmt = fs_prepare(&fs_conn, "SELECT COUNT(*) FROM bench");
        let fs = measure(&format!("fs_count_{count}"), 1, || {
            let _row = fsqlite_e2e::block_on(fs_stmt.query_row()).unwrap();
        });
        eprintln!(
            "C={} F={}",
            format_duration(cs.median()),
            format_duration(fs.median())
        );
        section.add_row(&format!("{count} rows / COUNT(*)"), Some(cs), Some(fs));

        // Aggregate SUM + GROUP BY.
        eprint!("    SUM + GROUP BY... ");
        let cs = {
            // Group by integer division to get ~10 groups.
            #[allow(clippy::cast_possible_wrap)]
            let group_divisor = (count / 10).max(1) as i64;
            let sql = format!(
                "SELECT (id / {group_divisor}), SUM(value) FROM bench GROUP BY (id / {group_divisor})"
            );
            let mut stmt = cs_conn.prepare(&sql).unwrap();
            measure(&format!("cs_groupby_{count}"), count, || {
                let _rows = collect_rusqlite_rows(&mut stmt, []).unwrap();
            })
        };
        let fs = {
            #[allow(clippy::cast_possible_wrap)]
            let group_divisor = (count / 10).max(1) as i64;
            let sql = format!(
                "SELECT (id / {group_divisor}), SUM(value) FROM bench GROUP BY (id / {group_divisor})"
            );
            let stmt = fs_prepare(&fs_conn, &sql);
            measure(&format!("fs_groupby_{count}"), count, || {
                let _rows = fsqlite_e2e::block_on(stmt.query()).unwrap();
            })
        };
        eprintln!(
            "C={} F={}",
            format_duration(cs.median()),
            format_duration(fs.median())
        );
        section.add_row(
            &format!("{count} rows / SUM + GROUP BY (~10 groups)"),
            Some(cs),
            Some(fs),
        );

        // Indexed lookup on secondary index.
        eprint!("    Indexed lookup (secondary)... ");
        let target_name = format!("user_{target_id}");
        let cs = {
            let mut stmt = cs_conn
                .prepare("SELECT * FROM bench WHERE name = ?1")
                .unwrap();
            measure(&format!("cs_idx_{count}"), 1, || {
                let _rows =
                    collect_rusqlite_rows(&mut stmt, rusqlite::params![target_name.clone()])
                        .unwrap();
            })
        };
        let fs_stmt = fs_prepare(&fs_conn, "SELECT * FROM bench WHERE name = ?1");
        let target_name_param = [fsqlite::SqliteValue::Text(target_name.into())];
        let profile_idx_enabled = std::env::var("FSQLITE_BENCH_PROFILE_IDX")
            .map(|value| value == "1" || value.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        let previous_hot_path_profile_enabled = hot_path_profile_enabled();
        if profile_idx_enabled {
            set_hot_path_profile_enabled(true);
        }
        reset_hot_path_profile();
        let fs = measure(&format!("fs_idx_{count}"), 1, || {
            let _rows =
                fsqlite_e2e::block_on(fs_stmt.query_with_params(&target_name_param)).unwrap();
        });
        let fs_idx_profile = hot_path_profile_snapshot();
        if profile_idx_enabled {
            let profile_iters = std::env::var("FSQLITE_BENCH_PROFILE_IDX_ITERS")
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(50_000);
            reset_hot_path_profile();
            let start = Instant::now();
            let mut row_count = 0_usize;
            for _ in 0..profile_iters {
                let rows =
                    fsqlite_e2e::block_on(fs_stmt.query_with_params(&target_name_param)).unwrap();
                row_count = row_count.saturating_add(rows.len());
                std::hint::black_box(rows);
            }
            std::hint::black_box(row_count);
            let profile = hot_path_profile_snapshot();
            let ns_per_op =
                start.elapsed().as_secs_f64() * 1_000_000_000.0 / profile_iters.max(1) as f64;
            eprintln!(
                "    [fs_idx_{count}] profile direct_hits={} measured_direct_hits={} fast={} slow={} memdb_refresh={} cached_read_parks={} cached_read_reuses={} cached_write_reuses={} tight_loop_ns_per_op={ns_per_op:.2} iterations={profile_iters} rows_seen={row_count}",
                profile.direct_indexed_equality_query_hits,
                fs_idx_profile.direct_indexed_equality_query_hits,
                profile.parser.fast_path_executions,
                profile.parser.slow_path_executions,
                profile.memdb_refresh_count,
                profile.cached_read_snapshot_parks,
                profile.cached_read_snapshot_reuses,
                profile.cached_write_txn_reuses,
            );
            set_hot_path_profile_enabled(previous_hot_path_profile_enabled);
        }
        eprintln!(
            "C={} F={}",
            format_duration(cs.median()),
            format_duration(fs.median())
        );
        section.add_row(
            &format!("{count} rows / indexed lookup (secondary)"),
            Some(cs),
            Some(fs),
        );

        // ORDER BY + LIMIT.
        eprint!("    ORDER BY + LIMIT 20... ");
        let cs = {
            let mut stmt = cs_conn
                .prepare("SELECT * FROM bench ORDER BY value DESC LIMIT 20")
                .unwrap();
            measure(&format!("cs_order_{count}"), 20, || {
                let _rows = collect_rusqlite_rows(&mut stmt, []).unwrap();
            })
        };
        let fs_stmt = fs_prepare(&fs_conn, "SELECT * FROM bench ORDER BY value DESC LIMIT 20");
        let fs = measure(&format!("fs_order_{count}"), 20, || {
            let _rows = fsqlite_e2e::block_on(fs_stmt.query()).unwrap();
        });
        eprintln!(
            "C={} F={}",
            format_duration(cs.median()),
            format_duration(fs.median())
        );
        section.add_row(
            &format!("{count} rows / ORDER BY + LIMIT 20"),
            Some(cs),
            Some(fs),
        );
    }
}

// ─── Section 6: Update and delete throughput ───────────────────────────

fn profile_fsqlite_update_delete_dml(
    record_size: RecordSize,
    count: usize,
    mutation_count: usize,
    kind: &str,
) {
    let conn = open_fsqlite_memory_connection_for_benchmark();
    apply_pragmas_fsqlite(&conn);

    let setup_start = Instant::now();
    fs_execute(&conn, record_size.create_table_sql());
    fs_execute(&conn, "BEGIN");
    #[allow(clippy::cast_possible_wrap)]
    let insert = fs_prepare(&conn, record_size.insert_sql_csqlite());
    #[allow(clippy::cast_possible_wrap)]
    for i in 0..count as i64 {
        fs_stmt_execute_with_params(&insert, &[fsqlite::SqliteValue::Integer(i)]);
    }
    drop(insert);
    fs_execute(&conn, "COMMIT");
    let setup_us = setup_start.elapsed().as_secs_f64() * 1_000_000.0;

    let statement_sql = match kind {
        "update" => "UPDATE bench SET value = ?2 WHERE id = ?1",
        "delete" => "DELETE FROM bench WHERE id = ?1",
        _ => unreachable!("known DML profile kind"),
    };

    let previous_hot_path_profile_enabled = hot_path_profile_enabled();
    set_hot_path_profile_enabled(true);
    reset_hot_path_profile();

    let begin_start = Instant::now();
    fs_execute(&conn, "BEGIN");
    let begin_us = begin_start.elapsed().as_secs_f64() * 1_000_000.0;

    let prepare_start = Instant::now();
    let statement = fs_prepare(&conn, statement_sql);
    let prepare_us = prepare_start.elapsed().as_secs_f64() * 1_000_000.0;

    let mutate_start = Instant::now();
    #[allow(clippy::cast_possible_wrap)]
    for i in 0..mutation_count as i64 {
        let id = if kind == "update" { i * 10 } else { i * 20 };
        match kind {
            "update" => fs_stmt_execute_with_params(
                &statement,
                &[
                    fsqlite::SqliteValue::Integer(id),
                    fsqlite::SqliteValue::Float(999.99),
                ],
            ),
            "delete" => {
                fs_stmt_execute_with_params(&statement, &[fsqlite::SqliteValue::Integer(id)])
            }
            _ => unreachable!("known DML profile kind"),
        };
    }
    let mutate_us = mutate_start.elapsed().as_secs_f64() * 1_000_000.0;

    let commit_start = Instant::now();
    fs_execute(&conn, "COMMIT");
    let commit_us = commit_start.elapsed().as_secs_f64() * 1_000_000.0;

    let profile = hot_path_profile_snapshot();
    set_hot_path_profile_enabled(previous_hot_path_profile_enabled);

    eprintln!(
        "    [fs_{kind}_{count}] dml_profile setup_us={setup_us:.1} begin_us={begin_us:.1} prepare_us={prepare_us:.1} mutate_us={mutate_us:.1} commit_us={commit_us:.1} mutations={mutation_count} direct_update={} direct_delete={} update_leaf_start={}/{} update_leaf_start_ns={} update_leaf_active={}/{} update_leaf_miss={} update_leaf_active_ns={} update_leaf_flush={}/{} update_leaf_flush_ns={} delete_preflush_ns={} delete_rowid_ns={} delete_active_probe_ns={} delete_cursor_setup_ns={} delete_memdb_abandon={}/{} delete_memory_sync={}/{} delete_qf_ns={} delete_seek_ns={} delete_physical_ns={} delete_leaf_start={}/{} delete_leaf_start_ns={} delete_leaf_active={}/{} delete_leaf_miss={} delete_leaf_miss_shape={} delete_leaf_miss_staged={} delete_leaf_miss_out_of_leaf={} delete_leaf_miss_duplicate={} delete_leaf_miss_empty_leaf={} delete_leaf_miss_last_cell={} delete_leaf_miss_noncompact={} delete_leaf_miss_cell_shape={} delete_leaf_active_ns={} delete_leaf_flush={}/{} delete_leaf_flush_ns={} delete_leaf_materialize={}/{} delete_leaf_write={}/{} delete_leaf_search={}/{} delete_leaf_dupcheck={}/{} delete_leaf_compact={}/{} delete_leaf_cellparse={}/{} fast={} slow={} ud_fast_lane={} ud_instrumented_lane={} schema_refreshes={} schema_refresh_ns={} begin_ns={} execute_body_ns={} direct_flush_calls={} direct_flush_ns={} commit_pre_ns={} commit_roundtrip_ns={} pager_commit_calls={} pager_phase_a_ns={} pager_wal_ns={} pager_mem_flush_ns={} pager_journal_ns={} pager_c_metadata_ns={} pager_file_size_ns={} pager_unlock_ns={} pager_publish_ns={} pager_cache_finish_ns={} commit_finalize_ns={} commit_handle_ns={} post_write_ns={} finalize_post_ns={} parser_multi_calls={} parser_cache_hits={} parser_cache_misses={} parser_parse_ns={} parser_rewrite_ns={} bg_checks={} bg_ns={} op_cx_bg_gates={} dispatch_bg_gates={} pager_pub_refreshes={} commit_refreshes={} prepared_lookup_ns={} memdb_refresh={} cached_write_reuses={} cached_write_parks={} page_pool_hits={} page_pool_misses={} record_parse_into={} record_decode_ns={} btree_payload_copy_calls={} btree_payload_copy_bytes={} btree_cell_assembly_calls={} btree_cell_assembly_bytes={} vdbe_opcodes={} vdbe_statements={} vdbe_make_record={}",
        profile.prepared_direct_update_executions,
        profile.prepared_direct_delete_executions,
        profile.prepared_direct_update_leaf_patch_run_start_hits,
        profile.prepared_direct_update_leaf_patch_run_start_attempts,
        profile.prepared_direct_update_leaf_patch_run_start_time_ns,
        profile.prepared_direct_update_leaf_patch_run_active_hits,
        profile.prepared_direct_update_leaf_patch_run_active_attempts,
        profile.prepared_direct_update_leaf_patch_run_active_misses,
        profile.prepared_direct_update_leaf_patch_run_active_time_ns,
        profile.prepared_direct_update_leaf_patch_run_dirty_flushes,
        profile.prepared_direct_update_leaf_patch_run_flushes,
        profile.prepared_direct_update_leaf_patch_run_flush_time_ns,
        profile.prepared_direct_delete_preflush_time_ns,
        profile.prepared_direct_delete_rowid_lookup_time_ns,
        profile.prepared_direct_delete_active_leaf_probe_time_ns,
        profile.prepared_direct_delete_cursor_setup_time_ns,
        profile.prepared_direct_delete_memdb_abandon_calls,
        profile.prepared_direct_delete_memdb_abandon_time_ns,
        profile.prepared_direct_delete_memory_sync_calls,
        profile.prepared_direct_delete_memory_sync_time_ns,
        profile.prepared_direct_delete_qf_time_ns,
        profile.prepared_direct_delete_seek_time_ns,
        profile.prepared_direct_delete_physical_delete_time_ns,
        profile.prepared_direct_delete_leaf_run_start_hits,
        profile.prepared_direct_delete_leaf_run_start_attempts,
        profile.prepared_direct_delete_leaf_run_start_time_ns,
        profile.prepared_direct_delete_leaf_run_active_hits,
        profile.prepared_direct_delete_leaf_run_active_attempts,
        profile.prepared_direct_delete_leaf_run_active_misses,
        profile.prepared_direct_delete_leaf_run_active_miss_shape_mismatches,
        profile.prepared_direct_delete_leaf_run_active_miss_staged_runs,
        profile.prepared_direct_delete_leaf_run_active_miss_rowid_not_in_leaf,
        profile.prepared_direct_delete_leaf_run_active_miss_already_deleted,
        profile.prepared_direct_delete_leaf_run_active_miss_nonroot_would_empty_leaf,
        profile.prepared_direct_delete_leaf_run_active_miss_nonroot_last_cell,
        profile.prepared_direct_delete_leaf_run_active_miss_noncompact_cell_area,
        profile.prepared_direct_delete_leaf_run_active_miss_cell_shape_or_overflow,
        profile.prepared_direct_delete_leaf_run_active_time_ns,
        profile.prepared_direct_delete_leaf_run_dirty_flushes,
        profile.prepared_direct_delete_leaf_run_flushes,
        profile.prepared_direct_delete_leaf_run_flush_time_ns,
        profile.btree_leaf_reuse.delete_leaf_run_materialize_calls,
        profile.btree_leaf_reuse.delete_leaf_run_materialize_time_ns,
        profile.btree_leaf_reuse.delete_leaf_run_write_calls,
        profile.btree_leaf_reuse.delete_leaf_run_write_time_ns,
        profile.btree_leaf_reuse.delete_leaf_run_search_calls,
        profile.btree_leaf_reuse.delete_leaf_run_search_time_ns,
        profile
            .btree_leaf_reuse
            .delete_leaf_run_duplicate_check_calls,
        profile
            .btree_leaf_reuse
            .delete_leaf_run_duplicate_check_time_ns,
        profile.btree_leaf_reuse.delete_leaf_run_compact_check_calls,
        profile
            .btree_leaf_reuse
            .delete_leaf_run_compact_check_time_ns,
        profile.btree_leaf_reuse.delete_leaf_run_cell_parse_calls,
        profile.btree_leaf_reuse.delete_leaf_run_cell_parse_time_ns,
        profile.parser.fast_path_executions,
        profile.parser.slow_path_executions,
        profile.prepared_update_delete_fast_lane_hits,
        profile.prepared_update_delete_instrumented_lane_hits,
        profile.prepared_schema_refreshes,
        profile.prepared_schema_refresh_time_ns,
        profile.begin_setup_time_ns,
        profile.execute_body_time_ns,
        profile.direct_write_flush_calls,
        profile.direct_write_flush_time_ns,
        profile.commit_pre_txn_time_ns,
        profile.commit_txn_roundtrip_time_ns,
        profile.pager_commit.commit_calls,
        profile.pager_commit.phase_a_time_ns,
        profile.pager_commit.wal_commit_time_ns,
        profile.pager_commit.memory_flush_time_ns,
        profile.pager_commit.journal_commit_time_ns,
        profile.pager_commit.phase_c_metadata_time_ns,
        profile.pager_commit.file_size_time_ns,
        profile.pager_commit.unlock_time_ns,
        profile.pager_commit.publish_time_ns,
        profile.pager_commit.cache_finish_time_ns,
        profile.commit_finalize_seq_time_ns,
        profile.commit_handle_finalize_time_ns,
        profile.commit_post_write_maintenance_time_ns,
        profile.finalize_post_publish_time_ns,
        profile.parser.parse_multi_calls,
        profile.parser.parse_cache_hits,
        profile.parser.parse_cache_misses,
        profile.parser.parse_time_ns,
        profile.parser.rewrite_time_ns,
        profile.background_status_checks,
        profile.background_status_time_ns,
        profile.op_cx_background_gates,
        profile.statement_dispatch_background_gates,
        profile.pager_publication_refreshes,
        profile.commit_refresh_count,
        profile.prepared_lookup_time_ns,
        profile.memdb_refresh_count,
        profile.cached_write_txn_reuses,
        profile.cached_write_txn_parks,
        profile.page_buffer_pool_hits,
        profile.page_buffer_pool_misses,
        profile.record_decode.parse_record_into_calls,
        profile.record_decode.decode_time_ns,
        profile.btree_copy_kernels.local_payload_copy_calls,
        profile.btree_copy_kernels.local_payload_copy_bytes,
        profile.btree_copy_kernels.table_leaf_cell_assembly_calls,
        profile.btree_copy_kernels.table_leaf_cell_assembly_bytes,
        profile.vdbe.opcodes_executed_total,
        profile.vdbe.statements_total,
        profile.vdbe.make_record_calls_total,
    );
}

fn bench_update_delete(report: &mut BenchReport, row_counts: &[usize]) {
    let section = report.add_section(
        "UPDATE/DELETE Throughput",
        "Pre-populated table with N rows. Measures batch update (10% of rows) and batch delete (5% of rows).",
    );

    let record_size = RecordSize::Small;
    let profile_dml_enabled = bench_env_flag("FSQLITE_BENCH_PROFILE_DML");

    for &count in row_counts {
        if count > 100_000 {
            continue;
        }

        let update_count = count / 10;
        let delete_count = count / 20;

        // Batch update: update 10% of rows.
        eprint!("  Benchmarking update {update_count}/{count} rows... ");

        let cs = {
            let insert_sql = record_size.insert_sql_csqlite();
            let create_sql = record_size.create_table_sql();
            let conn = rusqlite::Connection::open_in_memory().unwrap();
            apply_pragmas_csqlite(&conn);
            conn.execute_batch(&format!("{create_sql};")).unwrap();
            conn.execute_batch("BEGIN").unwrap();
            let mut ins = conn.prepare(insert_sql).unwrap();
            #[allow(clippy::cast_possible_wrap)]
            for i in 0..count as i64 {
                ins.execute(rusqlite::params![i]).unwrap();
            }
            drop(ins);
            conn.execute_batch("COMMIT").unwrap();
            let mut upd = conn
                .prepare("UPDATE bench SET value = ?2 WHERE id = ?1")
                .unwrap();
            let mut reset = conn
                .prepare("UPDATE bench SET value = ?2 WHERE id = ?1")
                .unwrap();

            measure_with_teardown(
                &format!("cs_update_{count}"),
                update_count,
                || {
                    conn.execute_batch("BEGIN").unwrap();
                    #[allow(clippy::cast_possible_wrap)]
                    for i in 0..update_count as i64 {
                        let id = i * 10; // Every 10th row.
                        upd.execute(rusqlite::params![id, 999.99]).unwrap();
                    }
                    conn.execute_batch("COMMIT").unwrap();
                },
                || {
                    conn.execute_batch("BEGIN").unwrap();
                    #[allow(clippy::cast_possible_wrap)]
                    for i in 0..update_count as i64 {
                        let id = i * 10;
                        let original_value = f64::from(i32::try_from(id).unwrap()) * 0.137;
                        reset
                            .execute(rusqlite::params![id, original_value])
                            .unwrap();
                    }
                    conn.execute_batch("COMMIT").unwrap();
                },
            )
        };

        let fs = {
            let create_sql = record_size.create_table_sql();
            let conn = open_fsqlite_memory_connection_for_benchmark();
            apply_pragmas_fsqlite(&conn);
            fs_execute(&conn, create_sql);
            fs_execute(&conn, "BEGIN");
            #[allow(clippy::cast_possible_wrap)]
            let stmt = fs_prepare(&conn, record_size.insert_sql_csqlite());
            for i in 0..count as i64 {
                fs_stmt_execute_with_params(&stmt, &[fsqlite::SqliteValue::Integer(i)]);
            }
            drop(stmt);
            fs_execute(&conn, "COMMIT");
            let update = fs_prepare(&conn, "UPDATE bench SET value = ?2 WHERE id = ?1");
            let reset = fs_prepare(&conn, "UPDATE bench SET value = ?2 WHERE id = ?1");

            measure_with_teardown(
                &format!("fs_update_{count}"),
                update_count,
                || {
                    // bd-zavyn: one runtime entry per timed transaction.
                    fsqlite_e2e::block_on(async {
                        fs_execute_async(&conn, "BEGIN").await;
                        #[allow(clippy::cast_possible_wrap)]
                        for i in 0..update_count as i64 {
                            let id = i * 10;
                            fs_stmt_execute_with_params_async(
                                &update,
                                &[
                                    fsqlite::SqliteValue::Integer(id),
                                    fsqlite::SqliteValue::Float(999.99),
                                ],
                            )
                            .await;
                        }
                        fs_execute_async(&conn, "COMMIT").await;
                    });
                },
                || {
                    fs_execute(&conn, "BEGIN");
                    #[allow(clippy::cast_possible_wrap)]
                    for i in 0..update_count as i64 {
                        let id = i * 10;
                        let original_value = f64::from(i32::try_from(id).unwrap()) * 0.137;
                        fs_stmt_execute_with_params(
                            &reset,
                            &[
                                fsqlite::SqliteValue::Integer(id),
                                fsqlite::SqliteValue::Float(original_value),
                            ],
                        );
                    }
                    fs_execute(&conn, "COMMIT");
                },
            )
        };
        if profile_dml_enabled {
            profile_fsqlite_update_delete_dml(record_size, count, update_count, "update");
        }

        eprintln!(
            "C={} F={}",
            format_duration(cs.median()),
            format_duration(fs.median())
        );
        section.add_row(
            &format!("{count} rows / update {update_count} rows"),
            Some(cs),
            Some(fs),
        );

        // Batch delete: delete 5% of rows.
        eprint!("  Benchmarking delete {delete_count}/{count} rows... ");

        let cs = {
            let insert_sql = record_size.insert_sql_csqlite();
            let create_sql = record_size.create_table_sql();
            let conn = rusqlite::Connection::open_in_memory().unwrap();
            apply_pragmas_csqlite(&conn);
            conn.execute_batch(&format!("{create_sql};")).unwrap();
            conn.execute_batch("BEGIN").unwrap();
            let mut ins = conn.prepare(insert_sql).unwrap();
            #[allow(clippy::cast_possible_wrap)]
            for i in 0..count as i64 {
                ins.execute(rusqlite::params![i]).unwrap();
            }
            drop(ins);
            conn.execute_batch("COMMIT").unwrap();
            let mut del = conn.prepare("DELETE FROM bench WHERE id = ?1").unwrap();
            let mut restore = conn.prepare(insert_sql).unwrap();

            measure_with_teardown(
                &format!("cs_delete_{count}"),
                delete_count,
                || {
                    conn.execute_batch("BEGIN").unwrap();
                    #[allow(clippy::cast_possible_wrap)]
                    for i in 0..delete_count as i64 {
                        let id = i * 20; // Every 20th row.
                        del.execute(rusqlite::params![id]).unwrap();
                    }
                    conn.execute_batch("COMMIT").unwrap();
                },
                || {
                    conn.execute_batch("BEGIN").unwrap();
                    #[allow(clippy::cast_possible_wrap)]
                    for i in 0..delete_count as i64 {
                        let id = i * 20;
                        restore.execute(rusqlite::params![id]).unwrap();
                    }
                    conn.execute_batch("COMMIT").unwrap();
                },
            )
        };

        let fs = {
            let create_sql = record_size.create_table_sql();
            let conn = open_fsqlite_memory_connection_for_benchmark();
            apply_pragmas_fsqlite(&conn);
            fs_execute(&conn, create_sql);
            fs_execute(&conn, "BEGIN");
            #[allow(clippy::cast_possible_wrap)]
            let stmt = fs_prepare(&conn, record_size.insert_sql_csqlite());
            for i in 0..count as i64 {
                fs_stmt_execute_with_params(&stmt, &[fsqlite::SqliteValue::Integer(i)]);
            }
            drop(stmt);
            fs_execute(&conn, "COMMIT");
            let delete = fs_prepare(&conn, "DELETE FROM bench WHERE id = ?1");
            let restore = fs_prepare(&conn, record_size.insert_sql_csqlite());

            measure_with_teardown(
                &format!("fs_delete_{count}"),
                delete_count,
                || {
                    // bd-zavyn: one runtime entry per timed transaction.
                    fsqlite_e2e::block_on(async {
                        fs_execute_async(&conn, "BEGIN").await;
                        #[allow(clippy::cast_possible_wrap)]
                        for i in 0..delete_count as i64 {
                            let id = i * 20;
                            fs_stmt_execute_with_params_async(
                                &delete,
                                &[fsqlite::SqliteValue::Integer(id)],
                            )
                            .await;
                        }
                        fs_execute_async(&conn, "COMMIT").await;
                    });
                },
                || {
                    fs_execute(&conn, "BEGIN");
                    #[allow(clippy::cast_possible_wrap)]
                    for i in 0..delete_count as i64 {
                        let id = i * 20;
                        fs_stmt_execute_with_params(&restore, &[fsqlite::SqliteValue::Integer(id)]);
                    }
                    fs_execute(&conn, "COMMIT");
                },
            )
        };
        if profile_dml_enabled {
            profile_fsqlite_update_delete_dml(record_size, count, delete_count, "delete");
        }

        eprintln!(
            "C={} F={}",
            format_duration(cs.median()),
            format_duration(fs.median())
        );
        section.add_row(
            &format!("{count} rows / delete {delete_count} rows"),
            Some(cs),
            Some(fs),
        );
    }
}

// ─── Section 7: Mixed OLTP workload at scale ───────────────────────────

struct Rng64 {
    state: u64,
}

impl Rng64 {
    const fn new(seed: u64) -> Self {
        Self {
            state: if seed == 0 { 1 } else { seed },
        }
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }

    #[allow(clippy::cast_possible_truncation)]
    fn next_usize(&mut self, bound: usize) -> usize {
        (self.next_u64() % (bound as u64)) as usize
    }
}

fn bench_mixed_oltp(report: &mut BenchReport) {
    let section = report.add_section(
        "Mixed OLTP Workload (80% read / 20% write)",
        "Pre-seeded with 5K rows. Runs 5K operations with realistic distribution: \
         40% point lookups, 20% range scans, 20% aggregates, 15% inserts, 3% updates, 2% deletes.",
    );

    let ops = 5_000_usize;
    let seed_rows = 5_000_usize;

    eprint!("  Benchmarking mixed OLTP C SQLite... ");

    let cs = measure("cs_oltp", ops, || {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        apply_pragmas_csqlite(&conn);
        conn.execute_batch(
            "CREATE TABLE bench (id INTEGER PRIMARY KEY, name TEXT, score INTEGER);",
        )
        .unwrap();
        conn.execute_batch("BEGIN").unwrap();
        {
            let mut stmt = conn
                .prepare("INSERT INTO bench VALUES (?1, ('name_' || ?1), (?1 * 7))")
                .unwrap();
            #[allow(clippy::cast_possible_wrap)]
            for i in 1..=seed_rows as i64 {
                stmt.execute(rusqlite::params![i]).unwrap();
            }
        }
        conn.execute_batch("COMMIT").unwrap();

        let mut rng = Rng64::new(42);
        #[allow(clippy::cast_possible_wrap)]
        let mut next_id = seed_rows as i64 + 1;
        let mut select_pt = conn.prepare("SELECT * FROM bench WHERE id = ?1").unwrap();
        let mut select_range = conn
            .prepare("SELECT COUNT(*) FROM bench WHERE id >= ?1 AND id < ?2")
            .unwrap();
        let mut select_agg = conn
            .prepare("SELECT COUNT(*), SUM(score) FROM bench")
            .unwrap();
        let mut insert = conn
            .prepare("INSERT INTO bench VALUES (?1, ('name_' || ?1), (?1 * 7))")
            .unwrap();
        let mut update = conn
            .prepare("UPDATE bench SET score = ?2 WHERE id = ?1")
            .unwrap();
        let mut delete = conn.prepare("DELETE FROM bench WHERE id = ?1").unwrap();

        #[allow(clippy::cast_possible_wrap)]
        for _ in 0..ops {
            let roll = rng.next_usize(100);
            if roll < 40 {
                let id = (rng.next_usize(seed_rows) + 1) as i64;
                std::hint::black_box(
                    collect_rusqlite_rows(&mut select_pt, rusqlite::params![id]).unwrap(),
                );
            } else if roll < 60 {
                let start = (rng.next_usize(seed_rows.saturating_sub(50)) + 1) as i64;
                let count: i64 = select_range
                    .query_row(rusqlite::params![start, start + 50], |r| r.get(0))
                    .unwrap();
                std::hint::black_box(count);
            } else if roll < 80 {
                let aggregate: (i64, i64) = select_agg
                    .query_row([], |r| Ok((r.get(0).unwrap(), r.get(1).unwrap())))
                    .unwrap();
                std::hint::black_box(aggregate);
            } else if roll < 95 {
                std::hint::black_box(insert.execute(rusqlite::params![next_id]).unwrap());
                next_id += 1;
            } else if roll < 98 {
                let id = (rng.next_usize(seed_rows) + 1) as i64;
                std::hint::black_box(update.execute(rusqlite::params![id, id * 99]).unwrap());
            } else {
                let id = (rng.next_usize(seed_rows) + 1) as i64;
                std::hint::black_box(delete.execute(rusqlite::params![id]).unwrap());
            }
        }
        let final_state: (i64, i64) = conn
            .query_row(
                "SELECT COUNT(*), COALESCE(SUM(score), 0) FROM bench",
                [],
                |row| Ok((row.get(0).unwrap(), row.get(1).unwrap())),
            )
            .unwrap();
        std::hint::black_box(final_state);
    });

    eprintln!("C={}", format_duration(cs.median()));

    eprint!("  Benchmarking mixed OLTP FrankenSQLite... ");

    let fs = measure("fs_oltp", ops, || {
        // bd-zavyn: one runtime entry per timed sample. This body previously
        // re-entered the runtime once per seed row and once per operation
        // (~10k entries per sample) — the largest single instrument
        // distortion in the whole matrix.
        fsqlite_e2e::block_on(async {
            let conn = open_fsqlite_memory_connection_for_benchmark_async().await;
            apply_pragmas_fsqlite_async(&conn).await;
            fs_execute_async(
                &conn,
                "CREATE TABLE bench (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)",
            )
            .await;
            let seed_insert = fs_prepare_async(
                &conn,
                "INSERT INTO bench VALUES (?1, ('name_' || ?1), (?1 * 7))",
            )
            .await;
            fs_execute_async(&conn, "BEGIN").await;
            #[allow(clippy::cast_possible_wrap)]
            for i in 1..=seed_rows as i64 {
                fs_stmt_execute_with_params_async(
                    &seed_insert,
                    &[fsqlite::SqliteValue::Integer(i)],
                )
                .await;
            }
            fs_execute_async(&conn, "COMMIT").await;

            let mut rng = Rng64::new(42);
            #[allow(clippy::cast_possible_wrap)]
            let mut next_id = seed_rows as i64 + 1;
            let select_pt = fs_prepare_async(&conn, "SELECT * FROM bench WHERE id = ?1").await;
            let select_range = fs_prepare_async(
                &conn,
                "SELECT COUNT(*) FROM bench WHERE id >= ?1 AND id < ?2",
            )
            .await;
            let select_agg =
                fs_prepare_async(&conn, "SELECT COUNT(*), SUM(score) FROM bench").await;
            let insert = fs_prepare_async(
                &conn,
                "INSERT INTO bench VALUES (?1, ('name_' || ?1), (?1 * 7))",
            )
            .await;
            let update = fs_prepare_async(&conn, "UPDATE bench SET score = ?2 WHERE id = ?1").await;
            let delete = fs_prepare_async(&conn, "DELETE FROM bench WHERE id = ?1").await;

            #[allow(clippy::cast_possible_wrap)]
            for _ in 0..ops {
                let roll = rng.next_usize(100);
                if roll < 40 {
                    let id = (rng.next_usize(seed_rows) + 1) as i64;
                    std::hint::black_box(
                        select_pt
                            .query_with_params(&[fsqlite::SqliteValue::Integer(id)])
                            .await
                            .unwrap(),
                    );
                } else if roll < 60 {
                    let start = (rng.next_usize(seed_rows.saturating_sub(50)) + 1) as i64;
                    std::hint::black_box(
                        select_range
                            .query_row_with_params(&[
                                fsqlite::SqliteValue::Integer(start),
                                fsqlite::SqliteValue::Integer(start + 50),
                            ])
                            .await
                            .unwrap(),
                    );
                } else if roll < 80 {
                    std::hint::black_box(select_agg.query_row().await.unwrap());
                } else if roll < 95 {
                    std::hint::black_box(
                        insert
                            .execute_with_params(&[fsqlite::SqliteValue::Integer(next_id)])
                            .await
                            .unwrap(),
                    );
                    next_id += 1;
                } else if roll < 98 {
                    let id = (rng.next_usize(seed_rows) + 1) as i64;
                    std::hint::black_box(
                        update
                            .execute_with_params(&[
                                fsqlite::SqliteValue::Integer(id),
                                fsqlite::SqliteValue::Integer(id * 99),
                            ])
                            .await
                            .unwrap(),
                    );
                } else {
                    let id = (rng.next_usize(seed_rows) + 1) as i64;
                    std::hint::black_box(
                        delete
                            .execute_with_params(&[fsqlite::SqliteValue::Integer(id)])
                            .await
                            .unwrap(),
                    );
                }
            }
            let final_state =
                fs_prepare_async(&conn, "SELECT COUNT(*), COALESCE(SUM(score), 0) FROM bench")
                    .await
                    .query_row()
                    .await
                    .unwrap();
            std::hint::black_box(final_state);
        });
    });

    eprintln!("F={}", format_duration(fs.median()));

    section.add_row("5K ops (80r/20w) on 5K-row table", Some(cs), Some(fs));
}

// ─── Section 8: JOIN performance ────────────────────────────────────────

fn bench_join_performance(report: &mut BenchReport, row_counts: &[usize]) {
    let section = report.add_section(
        "JOIN Performance — Multi-Table Queries",
        "Two related tables (orders+customers). Measures INNER JOIN, LEFT JOIN, self-join, and JOIN with aggregation.",
    );

    for &count in row_counts {
        if count > 100_000 {
            continue;
        }

        let customer_count = count / 10; // 10x fewer customers than orders.
        let customer_count = customer_count.max(10);

        eprint!("  Setting up JOIN tables ({count} orders, {customer_count} customers)... ");

        // C SQLite setup + benchmarks.
        let cs_conn = {
            let conn = rusqlite::Connection::open_in_memory().unwrap();
            apply_pragmas_csqlite(&conn);
            conn.execute_batch(
                "CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT, region TEXT);\
                 CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, amount REAL, status TEXT);",
            ).unwrap();
            conn.execute_batch("BEGIN").unwrap();
            {
                let mut cstmt = conn.prepare("INSERT INTO customers VALUES (?1, ('cust_' || ?1), CASE ?1 % 4 WHEN 0 THEN 'North' WHEN 1 THEN 'South' WHEN 2 THEN 'East' ELSE 'West' END)").unwrap();
                #[allow(clippy::cast_possible_wrap)]
                for i in 1..=customer_count as i64 {
                    cstmt.execute(rusqlite::params![i]).unwrap();
                }
                let mut ostmt = conn.prepare("INSERT INTO orders VALUES (?1, ((?1 % ?2) + 1), (?1 * 9.99 / 100.0), CASE ?1 % 3 WHEN 0 THEN 'pending' WHEN 1 THEN 'shipped' ELSE 'delivered' END)").unwrap();
                #[allow(clippy::cast_possible_wrap)]
                for i in 1..=count as i64 {
                    ostmt
                        .execute(rusqlite::params![i, customer_count as i64])
                        .unwrap();
                }
            }
            conn.execute_batch("COMMIT").unwrap();
            conn.execute_batch("CREATE INDEX idx_orders_cust ON orders(customer_id);")
                .unwrap();
            conn
        };

        let fs_conn = {
            let conn = open_fsqlite_memory_connection_for_benchmark();
            apply_pragmas_fsqlite(&conn);
            fs_execute(
                &conn,
                "CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT, region TEXT)",
            );
            fs_execute(
                &conn,
                "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, amount REAL, status TEXT)",
            );
            fs_execute(&conn, "BEGIN");
            #[allow(clippy::cast_possible_wrap)]
            for i in 1..=customer_count as i64 {
                let region = match i % 4 {
                    0 => "North",
                    1 => "South",
                    2 => "East",
                    _ => "West",
                };
                fs_execute(
                    &conn,
                    &format!("INSERT INTO customers VALUES ({i}, 'cust_{i}', '{region}')"),
                );
            }
            #[allow(clippy::cast_possible_wrap)]
            for i in 1..=count as i64 {
                let cid = (i % customer_count as i64) + 1;
                let amount = i as f64 * 9.99 / 100.0;
                let status = match i % 3 {
                    0 => "pending",
                    1 => "shipped",
                    _ => "delivered",
                };
                fs_execute(
                    &conn,
                    &format!("INSERT INTO orders VALUES ({i}, {cid}, {amount}, '{status}')"),
                );
            }
            fs_execute(&conn, "COMMIT");
            fs_execute(&conn, "CREATE INDEX idx_orders_cust ON orders(customer_id)");
            conn
        };

        eprintln!("done.");

        // INNER JOIN.
        eprint!("    INNER JOIN... ");
        let cs = {
            let mut stmt = cs_conn.prepare("SELECT c.name, o.amount FROM customers c INNER JOIN orders o ON o.customer_id = c.id").unwrap();
            measure(&format!("cs_inner_join_{count}"), count, || {
                let _rows = collect_rusqlite_rows(&mut stmt, []).unwrap();
            })
        };
        let fs_stmt = fs_prepare(
            &fs_conn,
            "SELECT c.name, o.amount FROM customers c INNER JOIN orders o ON o.customer_id = c.id",
        );
        let fs = measure(&format!("fs_inner_join_{count}"), count, || {
            std::hint::black_box(fsqlite_e2e::block_on(fs_stmt.query()).unwrap());
        });
        eprintln!(
            "C={} F={}",
            format_duration(cs.median()),
            format_duration(fs.median())
        );
        section.add_row(&format!("{count} orders / INNER JOIN"), Some(cs), Some(fs));

        // LEFT JOIN.
        eprint!("    LEFT JOIN... ");
        let cs = {
            let mut stmt = cs_conn.prepare("SELECT c.name, o.amount FROM customers c LEFT JOIN orders o ON o.customer_id = c.id").unwrap();
            measure(&format!("cs_left_join_{count}"), count, || {
                let _rows = collect_rusqlite_rows(&mut stmt, []).unwrap();
            })
        };
        let fs_stmt = fs_prepare(
            &fs_conn,
            "SELECT c.name, o.amount FROM customers c LEFT JOIN orders o ON o.customer_id = c.id",
        );
        let fs = measure(&format!("fs_left_join_{count}"), count, || {
            std::hint::black_box(fsqlite_e2e::block_on(fs_stmt.query()).unwrap());
        });
        eprintln!(
            "C={} F={}",
            format_duration(cs.median()),
            format_duration(fs.median())
        );
        section.add_row(&format!("{count} orders / LEFT JOIN"), Some(cs), Some(fs));

        // JOIN + GROUP BY aggregate.
        eprint!("    JOIN + GROUP BY aggregate... ");
        let cs = {
            let mut stmt = cs_conn.prepare("SELECT c.name, COUNT(*), SUM(o.amount) FROM customers c JOIN orders o ON o.customer_id = c.id GROUP BY c.name").unwrap();
            measure(&format!("cs_join_agg_{count}"), customer_count, || {
                let _rows = collect_rusqlite_rows(&mut stmt, []).unwrap();
            })
        };
        let fs_stmt = fs_prepare(
            &fs_conn,
            "SELECT c.name, COUNT(*), SUM(o.amount) FROM customers c JOIN orders o ON o.customer_id = c.id GROUP BY c.name",
        );
        let fs = measure(&format!("fs_join_agg_{count}"), customer_count, || {
            std::hint::black_box(fsqlite_e2e::block_on(fs_stmt.query()).unwrap());
        });
        eprintln!(
            "C={} F={}",
            format_duration(cs.median()),
            format_duration(fs.median())
        );
        section.add_row(
            &format!("{count} orders / JOIN + GROUP BY"),
            Some(cs),
            Some(fs),
        );

        // JOIN + GROUP BY + HAVING.
        eprint!("    JOIN + GROUP BY + HAVING... ");
        let threshold = count as f64 * 0.05; // Customers with > 5% of orders.
        let cs = {
            let sql = format!(
                "SELECT c.name, COUNT(*) cnt FROM customers c JOIN orders o ON o.customer_id = c.id GROUP BY c.name HAVING cnt > {threshold}"
            );
            let mut stmt = cs_conn.prepare(&sql).unwrap();
            measure(&format!("cs_join_having_{count}"), customer_count, || {
                let _rows = collect_rusqlite_rows(&mut stmt, []).unwrap();
            })
        };
        let fs = {
            let sql = format!(
                "SELECT c.name, COUNT(*) cnt FROM customers c JOIN orders o ON o.customer_id = c.id GROUP BY c.name HAVING cnt > {threshold}"
            );
            let stmt = fs_prepare(&fs_conn, &sql);
            measure(&format!("fs_join_having_{count}"), customer_count, || {
                std::hint::black_box(fsqlite_e2e::block_on(stmt.query()).unwrap());
            })
        };
        eprintln!(
            "C={} F={}",
            format_duration(cs.median()),
            format_duration(fs.median())
        );
        section.add_row(
            &format!("{count} orders / JOIN + HAVING"),
            Some(cs),
            Some(fs),
        );
    }
}

// ─── Section 9: Subquery & CTE performance ──────────────────────────────

fn bench_subquery_cte(report: &mut BenchReport, row_counts: &[usize]) {
    let section = report.add_section(
        "Subquery & CTE Performance",
        "Measures scalar subqueries, EXISTS, IN subqueries, and recursive CTEs.",
    );

    for &count in row_counts {
        if count > 100_000 {
            continue;
        }

        eprint!("  Setting up subquery tables ({count} rows)... ");

        let cs_conn = {
            let conn = rusqlite::Connection::open_in_memory().unwrap();
            apply_pragmas_csqlite(&conn);
            conn.execute_batch(
                "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL, category_id INTEGER);\
                 CREATE TABLE categories (id INTEGER PRIMARY KEY, name TEXT);",
            ).unwrap();
            conn.execute_batch("BEGIN").unwrap();
            {
                let cat_count = (count / 20).max(5);
                let mut cstmt = conn
                    .prepare("INSERT INTO categories VALUES (?1, ('cat_' || ?1))")
                    .unwrap();
                #[allow(clippy::cast_possible_wrap)]
                for i in 1..=cat_count as i64 {
                    cstmt.execute(rusqlite::params![i]).unwrap();
                }
                let mut pstmt = conn.prepare("INSERT INTO products VALUES (?1, ('prod_' || ?1), (?1 * 3.14), ((?1 % ?2) + 1))").unwrap();
                #[allow(clippy::cast_possible_wrap)]
                for i in 1..=count as i64 {
                    pstmt
                        .execute(rusqlite::params![i, cat_count as i64])
                        .unwrap();
                }
            }
            conn.execute_batch("COMMIT").unwrap();
            conn.execute_batch("CREATE INDEX idx_prod_cat ON products(category_id);")
                .unwrap();
            conn
        };

        let cat_count = (count / 20).max(5);
        let fs_conn = {
            let conn = open_fsqlite_memory_connection_for_benchmark();
            apply_pragmas_fsqlite(&conn);
            fs_execute(
                &conn,
                "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL, category_id INTEGER)",
            );
            fs_execute(
                &conn,
                "CREATE TABLE categories (id INTEGER PRIMARY KEY, name TEXT)",
            );
            fs_execute(&conn, "BEGIN");
            #[allow(clippy::cast_possible_wrap)]
            for i in 1..=cat_count as i64 {
                fs_execute(
                    &conn,
                    &format!("INSERT INTO categories VALUES ({i}, 'cat_{i}')"),
                );
            }
            #[allow(clippy::cast_possible_wrap)]
            for i in 1..=count as i64 {
                let cid = (i % cat_count as i64) + 1;
                let price = i as f64 * 3.14;
                fs_execute(
                    &conn,
                    &format!("INSERT INTO products VALUES ({i}, 'prod_{i}', {price}, {cid})"),
                );
            }
            fs_execute(&conn, "COMMIT");
            fs_execute(&conn, "CREATE INDEX idx_prod_cat ON products(category_id)");
            conn
        };

        eprintln!("done.");

        // Scalar subquery in SELECT.
        eprint!("    Scalar subquery in SELECT... ");
        let scalar_sub_sql = "SELECT p.name, (SELECT c.name FROM categories c WHERE c.id = p.category_id) AS cat_name FROM products p LIMIT 100";
        assert_result_set_oracle(&cs_conn, &fs_conn, scalar_sub_sql, "scalar-subquery oracle");
        let cs = {
            let mut stmt = cs_conn.prepare(scalar_sub_sql).unwrap();
            measure(&format!("cs_scalar_sub_{count}"), 100, || {
                let _rows = collect_rusqlite_rows(&mut stmt, []).unwrap();
            })
        };
        let fs_stmt = fs_prepare(&fs_conn, scalar_sub_sql);
        let fs = measure(&format!("fs_scalar_sub_{count}"), 100, || {
            std::hint::black_box(fsqlite_e2e::block_on(fs_stmt.query()).unwrap());
        });
        eprintln!(
            "C={} F={}",
            format_duration(cs.median()),
            format_duration(fs.median())
        );
        section.add_row(
            &format!("{count} rows / scalar subquery (LIMIT 100)"),
            Some(cs),
            Some(fs),
        );

        // Parameter-varying EXISTS subquery. Varying the bound prevents a
        // one-entry exact-result cache from turning this into a warmed-result
        // lookup while C SQLite still executes the query.
        eprint!("    EXISTS subquery (parameter-varying)... ");
        let exists_sql = "SELECT COUNT(*) FROM products p WHERE EXISTS \
            (SELECT 1 FROM categories c \
             WHERE c.id = p.category_id AND c.id <= ?1)";
        #[allow(clippy::cast_possible_wrap)]
        let cat_count_i64 = cat_count as i64;
        let oracle_threshold = cat_count_i64.min(5);
        let expected_exists: i64 = cs_conn
            .query_row(exists_sql, rusqlite::params![oracle_threshold], |row| {
                row.get(0)
            })
            .unwrap();
        let exists_probe = fs_prepare(&fs_conn, exists_sql);
        let actual_exists = fsqlite_e2e::block_on(
            exists_probe.query_row_with_params(&[fsqlite::SqliteValue::Integer(oracle_threshold)]),
        )
        .unwrap();
        assert_eq!(
            fsqlite_integer(&actual_exists, 0, "EXISTS oracle"),
            expected_exists,
            "FrankenSQLite and C SQLite disagree on EXISTS benchmark oracle"
        );
        let cs = {
            let mut stmt = cs_conn.prepare(exists_sql).unwrap();
            let mut iteration = 0_i64;
            measure(&format!("cs_exists_{count}"), 1, || {
                let threshold = 1 + iteration % cat_count_i64;
                iteration += 1;
                let value: i64 = stmt
                    .query_row(rusqlite::params![threshold], |row| row.get(0))
                    .unwrap();
                std::hint::black_box(value);
            })
        };
        let fs = {
            let stmt = fs_prepare(&fs_conn, exists_sql);
            let mut iteration = 0_i64;
            measure(&format!("fs_exists_{count}"), 1, || {
                let threshold = 1 + iteration % cat_count_i64;
                iteration += 1;
                let row = fsqlite_e2e::block_on(
                    stmt.query_row_with_params(&[fsqlite::SqliteValue::Integer(threshold)]),
                )
                .unwrap();
                std::hint::black_box(fsqlite_integer(&row, 0, "EXISTS measurement"));
            })
        };
        eprintln!(
            "C={} F={}",
            format_duration(cs.median()),
            format_duration(fs.median())
        );
        section.add_row(
            &format!("{count} rows / EXISTS subquery (parameter-varying)"),
            Some(cs),
            Some(fs),
        );

        // Parameter-varying IN subquery. The previous constant `id <= 5`
        // shape measured FrankenSQLite's warmed exact-result cache after the
        // warmups, rather than general subquery execution (bd-czzlp).
        eprint!("    IN subquery (parameter-varying)... ");
        let in_sql = "SELECT COUNT(*) FROM products \
            WHERE category_id IN \
            (SELECT id FROM categories WHERE id <= ?1)";
        let expected_in: i64 = cs_conn
            .query_row(in_sql, rusqlite::params![oracle_threshold], |row| {
                row.get(0)
            })
            .unwrap();
        let in_probe = fs_prepare(&fs_conn, in_sql);
        let actual_in = fsqlite_e2e::block_on(
            in_probe.query_row_with_params(&[fsqlite::SqliteValue::Integer(oracle_threshold)]),
        )
        .unwrap();
        assert_eq!(
            fsqlite_integer(&actual_in, 0, "IN-subquery oracle"),
            expected_in,
            "FrankenSQLite and C SQLite disagree on IN-subquery benchmark oracle"
        );
        let cs = {
            let mut stmt = cs_conn.prepare(in_sql).unwrap();
            let mut iteration = 0_i64;
            measure(&format!("cs_in_sub_{count}"), 1, || {
                let threshold = 1 + iteration % cat_count_i64;
                iteration += 1;
                let value: i64 = stmt
                    .query_row(rusqlite::params![threshold], |row| row.get(0))
                    .unwrap();
                std::hint::black_box(value);
            })
        };
        let fs_stmt = fs_prepare(&fs_conn, in_sql);
        let mut fs_iteration = 0_i64;
        let fs = measure(&format!("fs_in_sub_{count}"), 1, || {
            let threshold = 1 + fs_iteration % cat_count_i64;
            fs_iteration += 1;
            let row = fsqlite_e2e::block_on(
                fs_stmt.query_row_with_params(&[fsqlite::SqliteValue::Integer(threshold)]),
            )
            .unwrap();
            std::hint::black_box(fsqlite_integer(&row, 0, "IN-subquery measurement"));
        });
        eprintln!(
            "C={} F={}",
            format_duration(cs.median()),
            format_duration(fs.median())
        );
        section.add_row(
            &format!("{count} rows / IN subquery (parameter-varying)"),
            Some(cs),
            Some(fs),
        );

        // CTE (non-recursive).
        eprint!("    CTE (non-recursive)... ");
        let cte_join_sql = "WITH top_cats AS (SELECT category_id, SUM(price) AS total FROM products GROUP BY category_id ORDER BY total DESC LIMIT 5) \
             SELECT p.name, p.price FROM products p JOIN top_cats tc ON p.category_id = tc.category_id";
        assert_result_set_oracle(&cs_conn, &fs_conn, cte_join_sql, "CTE+JOIN oracle");
        let cs = {
            let mut stmt = cs_conn.prepare(cte_join_sql).unwrap();
            measure(&format!("cs_cte_{count}"), count, || {
                let _rows = collect_rusqlite_rows(&mut stmt, []).unwrap();
            })
        };
        let fs_stmt = fs_prepare(&fs_conn, cte_join_sql);
        let fs = measure(&format!("fs_cte_{count}"), count, || {
            std::hint::black_box(fsqlite_e2e::block_on(fs_stmt.query()).unwrap());
        });
        eprintln!(
            "C={} F={}",
            format_duration(cs.median()),
            format_duration(fs.median())
        );
        section.add_row(&format!("{count} rows / CTE + JOIN"), Some(cs), Some(fs));
    }

    // This exact SUM shape is intentionally specialized by FrankenSQLite.
    // Keep it, but label it as such instead of presenting it as the general
    // recursive-CTE executor.
    const SPECIALIZED_RECURSIVE_CTE_SQL: &str = "WITH RECURSIVE cnt(x) AS \
         (SELECT 1 UNION ALL SELECT x+1 FROM cnt WHERE x < 1000) \
         SELECT SUM(x) FROM cnt";
    eprint!("    Recursive CTE specialized integer-series SUM... ");
    let cs = {
        let cs_conn = rusqlite::Connection::open_in_memory().unwrap();
        let mut stmt = cs_conn.prepare(SPECIALIZED_RECURSIVE_CTE_SQL).unwrap();
        measure("cs_recursive_cte", 1000, || {
            let value: i64 = stmt.query_row([], |row| row.get(0)).unwrap();
            assert_eq!(value, 500_500);
            std::hint::black_box(value);
        })
    };
    let fs = {
        let fs_conn = open_fsqlite_memory_connection_for_benchmark();
        let stmt = fs_prepare(&fs_conn, SPECIALIZED_RECURSIVE_CTE_SQL);
        measure("fs_recursive_cte", 1000, || {
            let row = fsqlite_e2e::block_on(stmt.query_row()).unwrap();
            let value = fsqlite_integer(&row, 0, "specialized recursive CTE");
            assert_eq!(value, 500_500);
            std::hint::black_box(value);
        })
    };
    eprintln!(
        "C={} F={}",
        format_duration(cs.median()),
        format_duration(fs.median())
    );
    section.add_row(
        "Recursive CTE specialized integer-series SUM (1..1000)",
        Some(cs),
        Some(fs),
    );

    // COUNT defeats the narrow SUM specialization and therefore measures the
    // general recursive frontier executor.
    const GENERAL_RECURSIVE_CTE_SQL: &str = "WITH RECURSIVE cnt(x) AS \
         (SELECT 1 UNION ALL SELECT x+1 FROM cnt WHERE x < 1000) \
         SELECT COUNT(*) FROM cnt";
    eprint!("    Recursive CTE general COUNT... ");
    let cs = {
        let cs_conn = rusqlite::Connection::open_in_memory().unwrap();
        let mut stmt = cs_conn.prepare(GENERAL_RECURSIVE_CTE_SQL).unwrap();
        measure("cs_recursive_cte_general", 1000, || {
            let value: i64 = stmt.query_row([], |row| row.get(0)).unwrap();
            assert_eq!(value, 1_000);
            std::hint::black_box(value);
        })
    };
    let fs = {
        let fs_conn = open_fsqlite_memory_connection_for_benchmark();
        let stmt = fs_prepare(&fs_conn, GENERAL_RECURSIVE_CTE_SQL);
        measure("fs_recursive_cte_general", 1000, || {
            let row = fsqlite_e2e::block_on(stmt.query_row()).unwrap();
            let value = fsqlite_integer(&row, 0, "general recursive CTE");
            assert_eq!(value, 1_000);
            std::hint::black_box(value);
        })
    };
    eprintln!(
        "C={} F={}",
        format_duration(cs.median()),
        format_duration(fs.median())
    );
    section.add_row("Recursive CTE general COUNT (1..1000)", Some(cs), Some(fs));
}

// ─── Section 10: String & LIKE performance ──────────────────────────────

fn bench_string_operations(report: &mut BenchReport, row_counts: &[usize]) {
    let section = report.add_section(
        "String & Pattern Matching Performance",
        "LIKE patterns, string functions, and text-heavy queries.",
    );

    for &count in row_counts {
        if count > 100_000 {
            continue;
        }

        eprint!("  Setting up string table ({count} rows)... ");

        let cs_conn = {
            let conn = rusqlite::Connection::open_in_memory().unwrap();
            apply_pragmas_csqlite(&conn);
            conn.execute_batch(
                "CREATE TABLE docs (id INTEGER PRIMARY KEY, title TEXT, body TEXT, tag TEXT);",
            )
            .unwrap();
            conn.execute_batch("BEGIN").unwrap();
            {
                let mut stmt = conn.prepare(
                    "INSERT INTO docs VALUES (?1, ('Document ' || ?1 || ': Important Analysis'), \
                     ('This is the body of document ' || ?1 || '. It contains various keywords like performance, benchmark, analysis, results, and optimization. \
                     The document is about testing and measuring throughput.'), \
                     CASE ?1 % 5 WHEN 0 THEN 'research' WHEN 1 THEN 'report' WHEN 2 THEN 'memo' WHEN 3 THEN 'analysis' ELSE 'draft' END)"
                ).unwrap();
                #[allow(clippy::cast_possible_wrap)]
                for i in 1..=count as i64 {
                    stmt.execute(rusqlite::params![i]).unwrap();
                }
            }
            conn.execute_batch("COMMIT").unwrap();
            conn
        };

        let fs_conn = {
            let conn = open_fsqlite_memory_connection_for_benchmark();
            apply_pragmas_fsqlite(&conn);
            fs_execute(
                &conn,
                "CREATE TABLE docs (id INTEGER PRIMARY KEY, title TEXT, body TEXT, tag TEXT)",
            );
            fs_execute(&conn, "BEGIN");
            #[allow(clippy::cast_possible_wrap)]
            for i in 1..=count as i64 {
                let tag = match i % 5 {
                    0 => "research",
                    1 => "report",
                    2 => "memo",
                    3 => "analysis",
                    _ => "draft",
                };
                fs_execute(
                    &conn,
                    &format!(
                        "INSERT INTO docs VALUES ({i}, 'Document {i}: Important Analysis', \
                         'This is the body of document {i}. It contains various keywords like performance, benchmark, analysis, results, and optimization. \
                         The document is about testing and measuring throughput.', '{tag}')"
                    ),
                );
            }
            fs_execute(&conn, "COMMIT");
            conn
        };

        eprintln!("done.");

        // LIKE with prefix pattern (sargable).
        eprint!("    LIKE prefix pattern... ");
        let cs = {
            let mut stmt = cs_conn
                .prepare("SELECT COUNT(*) FROM docs WHERE title LIKE 'Document 1%'")
                .unwrap();
            measure(&format!("cs_like_prefix_{count}"), 1, || {
                let value: i64 = stmt.query_row([], |r| r.get(0)).unwrap();
                std::hint::black_box(value);
            })
        };
        let fs_stmt = fs_prepare(
            &fs_conn,
            "SELECT COUNT(*) FROM docs WHERE title LIKE 'Document 1%'",
        );
        let fs = measure(&format!("fs_like_prefix_{count}"), 1, || {
            let row = fsqlite_e2e::block_on(fs_stmt.query_row()).unwrap();
            std::hint::black_box(fsqlite_integer(&row, 0, "LIKE prefix COUNT"));
        });
        eprintln!(
            "C={} F={}",
            format_duration(cs.median()),
            format_duration(fs.median())
        );
        section.add_row(
            &format!("{count} rows / LIKE 'prefix%'"),
            Some(cs),
            Some(fs),
        );

        // LIKE with wildcard (full scan).
        eprint!("    LIKE wildcard... ");
        let cs = {
            let mut stmt = cs_conn
                .prepare("SELECT COUNT(*) FROM docs WHERE body LIKE '%benchmark%'")
                .unwrap();
            measure(&format!("cs_like_wild_{count}"), 1, || {
                let value: i64 = stmt.query_row([], |r| r.get(0)).unwrap();
                std::hint::black_box(value);
            })
        };
        let fs_stmt = fs_prepare(
            &fs_conn,
            "SELECT COUNT(*) FROM docs WHERE body LIKE '%benchmark%'",
        );
        let fs = measure(&format!("fs_like_wild_{count}"), 1, || {
            let row = fsqlite_e2e::block_on(fs_stmt.query_row()).unwrap();
            std::hint::black_box(fsqlite_integer(&row, 0, "LIKE wildcard COUNT"));
        });
        eprintln!(
            "C={} F={}",
            format_duration(cs.median()),
            format_duration(fs.median())
        );
        section.add_row(
            &format!("{count} rows / LIKE '%wildcard%'"),
            Some(cs),
            Some(fs),
        );

        // String functions: LENGTH, UPPER, SUBSTR.
        eprint!("    String functions (LENGTH + UPPER + SUBSTR)... ");
        let cs = {
            let mut stmt = cs_conn
                .prepare("SELECT LENGTH(title), UPPER(tag), SUBSTR(body, 1, 50) FROM docs")
                .unwrap();
            measure(&format!("cs_str_funcs_{count}"), count, || {
                let rows = collect_rusqlite_rows(&mut stmt, []).unwrap();
                std::hint::black_box(rows);
            })
        };
        let fs_stmt = fs_prepare(
            &fs_conn,
            "SELECT LENGTH(title), UPPER(tag), SUBSTR(body, 1, 50) FROM docs",
        );
        let fs = measure(&format!("fs_str_funcs_{count}"), count, || {
            let rows = fsqlite_e2e::block_on(fs_stmt.query()).unwrap();
            std::hint::black_box(rows);
        });
        eprintln!(
            "C={} F={}",
            format_duration(cs.median()),
            format_duration(fs.median())
        );
        section.add_row(
            &format!("{count} rows / string functions"),
            Some(cs),
            Some(fs),
        );

        // GROUP_CONCAT.
        eprint!("    GROUP_CONCAT... ");
        let cs = {
            let mut stmt = cs_conn
                .prepare("SELECT tag, GROUP_CONCAT(id, ',') FROM docs GROUP BY tag")
                .unwrap();
            measure(&format!("cs_group_concat_{count}"), count, || {
                let rows = collect_rusqlite_rows(&mut stmt, []).unwrap();
                std::hint::black_box(rows);
            })
        };
        let fs_stmt = fs_prepare(
            &fs_conn,
            "SELECT tag, GROUP_CONCAT(id, ',') FROM docs GROUP BY tag",
        );
        let fs = measure(&format!("fs_group_concat_{count}"), count, || {
            let rows = fsqlite_e2e::block_on(fs_stmt.query()).unwrap();
            std::hint::black_box(rows);
        });
        eprintln!(
            "C={} F={}",
            format_duration(cs.median()),
            format_duration(fs.median())
        );
        section.add_row(&format!("{count} rows / GROUP_CONCAT"), Some(cs), Some(fs));
    }
}

// ─── Async bridge experiment ──────────────────────────────────────────

#[cfg(feature = "bridge-experiment")]
fn bridge_read_trimmed(path: impl AsRef<std::path::Path>) -> Option<String> {
    std::fs::read_to_string(path)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

#[cfg(feature = "bridge-experiment")]
fn bridge_cpufreq_values(file_name: &str) -> Vec<String> {
    let mut values = std::fs::read_dir("/sys/devices/system/cpu/cpufreq")
        .ok()
        .into_iter()
        .flat_map(|entries| entries.filter_map(Result::ok))
        .filter(|entry| entry.file_name().to_string_lossy().starts_with("policy"))
        .filter_map(|entry| bridge_read_trimmed(entry.path().join(file_name)))
        .collect::<Vec<_>>();
    values.sort();
    values.dedup();
    values
}

#[cfg(feature = "bridge-experiment")]
fn bridge_pressure_some_average(path: &str, field: &str) -> Option<f64> {
    bridge_read_trimmed(path)?
        .lines()
        .find(|line| line.starts_with("some "))
        .and_then(|line| {
            line.split_whitespace()
                .find_map(|value| value.strip_prefix(field))
        })
        .and_then(|value| value.parse::<f64>().ok())
        .filter(|value| value.is_finite() && *value >= 0.0)
}

#[cfg(feature = "bridge-experiment")]
fn bridge_cmdline_has_program(cmdline: &[String], program: &str) -> bool {
    cmdline.iter().any(|argument| {
        std::path::Path::new(argument)
            .file_name()
            .is_some_and(|name| name == program)
    })
}

#[cfg(feature = "bridge-experiment")]
fn bridge_process_is_competitor(comm: &str, cmdline: &[String], cgroup: Option<&str>) -> bool {
    // Kernel threads have an empty cmdline. Their mere presence is not host
    // activity: system-wide PSI and the checkpoints below still catch actual
    // Btrfs work, while a userspace `btrfs` command remains blocked.
    if cmdline.is_empty() {
        return false;
    }

    // This permanent system service is a lightweight build-priority monitor,
    // not a Cargo invocation. `/proc/<pid>/comm` truncates its name to
    // `cargo-io-enforc`, so require both the exact service cgroup and script
    // identity before exempting it; a real Cargo process still fails closed.
    let cargo_io_enforcer = bridge_cmdline_has_program(cmdline, "cargo-io-enforcer")
        && cgroup.is_some_and(|value| {
            value
                .lines()
                .any(|line| line == "0::/system.slice/cargo-io-enforcer.service")
        });
    if cargo_io_enforcer {
        return false;
    }

    let blocked = [
        "btrfs",
        "cargo",
        "cc1",
        "clang",
        "comprehensive-b",
        "fio",
        "fsqlite",
        "gcc",
        "hyperfine",
        "ld",
        "make",
        "mold",
        "mt_oltp_bench",
        "ninja",
        "rustc",
        "sbh",
        "sccache",
        "stress",
    ];
    blocked.iter().any(|prefix| comm.starts_with(prefix))
}

#[cfg(feature = "bridge-experiment")]
fn bridge_competing_processes() -> Result<Vec<String>, String> {
    let current_pid = std::process::id();
    let entries = std::fs::read_dir("/proc")
        .map_err(|error| format!("could not enumerate /proc: {error}"))?;
    let mut competitors = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|error| format!("could not inspect /proc entry: {error}"))?;
        let Some(pid) = entry
            .file_name()
            .to_str()
            .and_then(|name| name.parse::<u32>().ok())
        else {
            continue;
        };
        if pid == current_pid {
            continue;
        }
        let comm = match std::fs::read_to_string(entry.path().join("comm")) {
            Ok(comm) => comm.trim().to_owned(),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => {
                return Err(format!("could not read /proc/{pid}/comm: {error}"));
            }
        };
        let raw_cmdline = match std::fs::read(entry.path().join("cmdline")) {
            Ok(cmdline) => cmdline,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => {
                return Err(format!("could not read /proc/{pid}/cmdline: {error}"));
            }
        };
        let cmdline = raw_cmdline
            .split(|byte| *byte == 0)
            .filter(|argument| !argument.is_empty())
            .map(|argument| String::from_utf8_lossy(argument).into_owned())
            .collect::<Vec<_>>();
        let possible_cargo_io_enforcer = bridge_cmdline_has_program(&cmdline, "cargo-io-enforcer");
        let cgroup = if possible_cargo_io_enforcer {
            match std::fs::read_to_string(entry.path().join("cgroup")) {
                Ok(cgroup) => Some(cgroup),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => {
                    return Err(format!("could not read /proc/{pid}/cgroup: {error}"));
                }
            }
        } else {
            None
        };
        if bridge_process_is_competitor(&comm, &cmdline, cgroup.as_deref()) {
            competitors.push(format!("pid={pid},comm={comm}"));
        }
    }
    competitors.sort();
    Ok(competitors)
}

#[cfg(feature = "bridge-experiment")]
fn capture_bridge_host_state() -> JsonBridgeHostState {
    let load_average = bridge_read_trimmed("/proc/loadavg").and_then(|loadavg| {
        let values = loadavg
            .split_whitespace()
            .take(3)
            .map(str::parse::<f64>)
            .collect::<Result<Vec<_>, _>>()
            .ok()?;
        (values.len() == 3).then(|| (values[0], values[1], values[2]))
    });
    let memory_available_gb = bridge_read_trimmed("/proc/meminfo").and_then(|meminfo| {
        meminfo.lines().find_map(|line| {
            line.strip_prefix("MemAvailable:").and_then(|value| {
                value
                    .split_whitespace()
                    .next()
                    .and_then(|kilobytes| kilobytes.parse::<u64>().ok())
                    .map(|kilobytes| kilobytes as f64 / 1_048_576.0)
            })
        })
    });
    let boost_controls = [
        ("cpufreq.boost", "/sys/devices/system/cpu/cpufreq/boost"),
        (
            "intel_pstate.no_turbo",
            "/sys/devices/system/cpu/intel_pstate/no_turbo",
        ),
        (
            "amd_pstate.status",
            "/sys/devices/system/cpu/amd_pstate/status",
        ),
    ]
    .into_iter()
    .filter_map(|(name, path)| bridge_read_trimmed(path).map(|value| (name.to_owned(), value)))
    .collect();

    let affinity = cpu_affinity();
    let selected_cpu_topology = bridge_cpu_topology_receipt(affinity.as_deref());
    let (competing_processes, competing_process_scan_error) = match bridge_competing_processes() {
        Ok(processes) => (processes, None),
        Err(error) => (Vec::new(), Some(error)),
    };

    JsonBridgeHostState {
        captured_at_utc: chrono_stamp(),
        load_average_1m: load_average.map(|load| load.0),
        load_average_5m: load_average.map(|load| load.1),
        load_average_15m: load_average.map(|load| load.2),
        available_parallelism: std::thread::available_parallelism()
            .ok()
            .map(std::num::NonZero::get),
        cpu_affinity: affinity,
        selected_cpu_topology,
        scaling_governors: bridge_cpufreq_values("scaling_governor"),
        energy_performance_preferences: bridge_cpufreq_values("energy_performance_preference"),
        boost_controls,
        numa_nodes_online: bridge_read_trimmed("/sys/devices/system/node/online"),
        memory_available_gb,
        cpu_pressure_some_avg10: bridge_pressure_some_average("/proc/pressure/cpu", "avg10="),
        io_pressure_some_avg60: bridge_pressure_some_average("/proc/pressure/io", "avg60="),
        competing_processes,
        competing_process_scan_error,
    }
}

#[cfg(feature = "bridge-experiment")]
fn bridge_parse_cpu_affinity(value: &str) -> Result<Vec<usize>, String> {
    let mut cpus = Vec::new();
    let mut previous_end = None;
    for segment in value.split(',') {
        if segment.is_empty() {
            return Err("CPU affinity contains an empty segment".to_owned());
        }
        let (start, end) = match segment.split_once('-') {
            Some((start, end)) => (
                start
                    .parse::<usize>()
                    .map_err(|_| format!("invalid CPU affinity start `{start}`"))?,
                end.parse::<usize>()
                    .map_err(|_| format!("invalid CPU affinity end `{end}`"))?,
            ),
            None => {
                let cpu = segment
                    .parse::<usize>()
                    .map_err(|_| format!("invalid CPU affinity CPU `{segment}`"))?;
                (cpu, cpu)
            }
        };
        if start > end {
            return Err(format!(
                "CPU affinity range starts after it ends: `{segment}`"
            ));
        }
        if previous_end.is_some_and(|previous| start <= previous) {
            return Err("CPU affinity ranges overlap or are not increasing".to_owned());
        }
        cpus.extend(start..=end);
        previous_end = Some(end);
    }
    if cpus.is_empty() {
        Err("CPU affinity selects no CPUs".to_owned())
    } else {
        Ok(cpus)
    }
}

#[cfg(all(feature = "bridge-experiment", test))]
fn bridge_cpu_affinity_cardinality(value: &str) -> Result<usize, String> {
    bridge_parse_cpu_affinity(value).map(|cpus| cpus.len())
}

#[cfg(feature = "bridge-experiment")]
fn bridge_cpu_numa_node(cpu: usize) -> Result<String, String> {
    let cpu_path = std::path::PathBuf::from(format!("/sys/devices/system/cpu/cpu{cpu}"));
    let mut nodes = std::fs::read_dir(&cpu_path)
        .map_err(|error| format!("could not inspect topology for CPU {cpu}: {error}"))?
        .filter_map(Result::ok)
        .filter_map(|entry| entry.file_name().to_str().map(str::to_owned))
        .filter(|name| {
            name.strip_prefix("node").is_some_and(|suffix| {
                !suffix.is_empty() && suffix.bytes().all(|b| b.is_ascii_digit())
            })
        })
        .collect::<Vec<_>>();
    nodes.sort();
    match nodes.as_slice() {
        [node] => Ok(node.clone()),
        [] => Err(format!("CPU {cpu} exposes no NUMA-node topology")),
        _ => Err(format!("CPU {cpu} exposes multiple NUMA nodes: {nodes:?}")),
    }
}

#[cfg(feature = "bridge-experiment")]
fn bridge_cpu_topology(cpu: usize) -> Result<(String, String, String, String), String> {
    let topology = std::path::PathBuf::from(format!("/sys/devices/system/cpu/cpu{cpu}/topology"));
    let read = |name: &str| {
        bridge_read_trimmed(topology.join(name))
            .ok_or_else(|| format!("CPU {cpu} topology omits `{name}`"))
    };
    Ok((
        read("physical_package_id")?,
        read("core_id")?,
        read("thread_siblings_list")?,
        bridge_cpu_numa_node(cpu)?,
    ))
}

#[cfg(feature = "bridge-experiment")]
fn bridge_cpu_topology_receipt(affinity: Option<&str>) -> BTreeMap<String, String> {
    affinity
        .and_then(|value| bridge_parse_cpu_affinity(value).ok())
        .unwrap_or_default()
        .into_iter()
        .filter_map(|cpu| {
            bridge_cpu_topology(cpu)
                .ok()
                .map(|(package, core, siblings, node)| {
                    (
                        format!("cpu{cpu}"),
                        format!(
                            "package={package},core={core},thread_siblings={siblings},numa={node}"
                        ),
                    )
                })
        })
        .collect()
}

#[cfg(feature = "bridge-experiment")]
fn bridge_validate_cpu_topology(cpus: &[usize]) -> Result<(), String> {
    let mut physical_cores = std::collections::BTreeSet::new();
    let mut numa_nodes = std::collections::BTreeSet::new();
    for &cpu in cpus {
        let (package, core, siblings, node) = bridge_cpu_topology(cpu)?;
        if !physical_cores.insert((package, core)) {
            return Err(format!(
                "selected CPUs include SMT siblings on the same physical core; CPU {cpu} reports siblings `{siblings}`"
            ));
        }
        numa_nodes.insert(node);
    }
    if numa_nodes.len() != 1 {
        return Err(format!(
            "selected CPUs span NUMA nodes {numa_nodes:?}; citable bridge runs require one node"
        ));
    }
    Ok(())
}

#[cfg(feature = "bridge-experiment")]
fn bridge_max_load_average_1m() -> Result<f64, String> {
    let raw = std::env::var("FSQLITE_BENCH_MAX_LOAD_1M").map_err(|error| match error {
        std::env::VarError::NotPresent => {
            "citable bridge runs require FSQLITE_BENCH_MAX_LOAD_1M".to_owned()
        }
        other => format!("could not read FSQLITE_BENCH_MAX_LOAD_1M: {other}"),
    })?;
    let maximum = raw
        .parse::<f64>()
        .map_err(|_| format!("FSQLITE_BENCH_MAX_LOAD_1M must be a finite number, got `{raw}`"))?;
    if maximum.is_finite() && (0.0..=BRIDGE_ABSOLUTE_MAX_LOAD_1M).contains(&maximum) {
        Ok(maximum)
    } else {
        Err(format!(
            "FSQLITE_BENCH_MAX_LOAD_1M must be finite and between 0 and {BRIDGE_ABSOLUTE_MAX_LOAD_1M}, got `{raw}`"
        ))
    }
}

#[cfg(feature = "bridge-experiment")]
fn bridge_validate_host_state(
    provenance: &mut JsonBenchmarkProvenance,
    expected_affinity: Option<&str>,
    max_load_average_1m: Option<f64>,
    state: &JsonBridgeHostState,
    phase: &str,
) {
    match (expected_affinity, state.cpu_affinity.as_deref()) {
        (Some(expected), Some(observed)) if expected == observed => {
            match bridge_parse_cpu_affinity(observed) {
                Ok(cpus) if matches!(cpus.len(), 1 | 2) => {
                    if let Err(error) = bridge_validate_cpu_topology(&cpus) {
                        provenance.add_validation_error(format!(
                            "{phase} bridge CPU topology is invalid: {error}"
                        ));
                    }
                    if state.selected_cpu_topology.len() != cpus.len() {
                        provenance.add_validation_error(format!(
                            "{phase} bridge CPU topology receipt is incomplete: expected {} CPUs, captured {}",
                            cpus.len(),
                            state.selected_cpu_topology.len()
                        ));
                    }
                }
                Ok(cpus) => provenance.add_validation_error(format!(
                    "{phase} bridge CPU affinity selects {} CPUs; citable causal runs require exactly one or two",
                    cpus.len()
                )),
                Err(error) => provenance.add_validation_error(format!(
                    "{phase} bridge CPU affinity is invalid: {error}"
                )),
            }
        }
        (Some(expected), observed) => provenance.add_validation_error(format!(
            "{phase} bridge CPU affinity mismatch: expected `{expected}`, observed {observed:?}"
        )),
        (None, _) => provenance.add_validation_error(
            "citable bridge runs require FSQLITE_BENCH_EXPECTED_CPU_AFFINITY".to_owned(),
        ),
    }
    match (max_load_average_1m, state.load_average_1m) {
        (Some(maximum), Some(observed)) if observed <= maximum => {}
        (Some(maximum), Some(observed)) => provenance.add_validation_error(format!(
            "{phase} host load average {observed:.3} exceeds declared maximum {maximum:.3}"
        )),
        (Some(_), None) => provenance
            .add_validation_error(format!("{phase} host load average could not be captured")),
        (None, _) => {
            provenance.add_validation_error("citable bridge runs require FSQLITE_BENCH_MAX_LOAD_1M")
        }
    }
    match state.cpu_pressure_some_avg10 {
        Some(observed) if observed <= BRIDGE_MAX_CPU_PRESSURE_SOME_AVG10 => {}
        Some(observed) => provenance.add_validation_error(format!(
            "{phase} CPU pressure some avg10 {observed:.3} exceeds absolute maximum {BRIDGE_MAX_CPU_PRESSURE_SOME_AVG10:.3}"
        )),
        None => provenance
            .add_validation_error(format!("{phase} CPU pressure could not be captured")),
    }
    match state.io_pressure_some_avg60 {
        Some(observed) if observed <= BRIDGE_MAX_IO_PRESSURE_SOME_AVG60 => {}
        Some(observed) => provenance.add_validation_error(format!(
            "{phase} I/O pressure some avg60 {observed:.3} exceeds absolute maximum {BRIDGE_MAX_IO_PRESSURE_SOME_AVG60:.3}"
        )),
        None => provenance
            .add_validation_error(format!("{phase} I/O pressure could not be captured")),
    }
    if !state.competing_processes.is_empty() {
        provenance.add_validation_error(format!(
            "{phase} competing build, benchmark, or maintenance processes are active: {:?}",
            state.competing_processes
        ));
    }
    if let Some(error) = state.competing_process_scan_error.as_deref() {
        provenance.add_validation_error(format!(
            "{phase} competing-process scan was incomplete: {error}"
        ));
    }
    if state.scaling_governors.is_empty()
        || state
            .scaling_governors
            .iter()
            .any(|governor| governor != "performance")
    {
        provenance.add_validation_error(format!(
            "{phase} bridge CPUs are not uniformly governed by `performance`: {:?}",
            state.scaling_governors
        ));
    }
    if state
        .energy_performance_preferences
        .iter()
        .any(|preference| preference != "performance")
    {
        provenance.add_validation_error(format!(
            "{phase} bridge CPUs do not have uniform `performance` energy preference: {:?}",
            state.energy_performance_preferences
        ));
    }
    for (name, expected) in [
        ("cpufreq.boost", "1"),
        ("intel_pstate.no_turbo", "0"),
        ("amd_pstate.status", "active"),
    ] {
        if let Some(observed) = state.boost_controls.get(name)
            && observed != expected
        {
            provenance.add_validation_error(format!(
                "{phase} bridge boost control `{name}` is `{observed}`, expected `{expected}`"
            ));
        }
    }
}

#[cfg(feature = "bridge-experiment")]
fn bridge_validate_host_stability(
    provenance: &mut JsonBenchmarkProvenance,
    before: &JsonBridgeHostState,
    after: &JsonBridgeHostState,
) {
    for (name, changed) in [
        ("CPU affinity", before.cpu_affinity != after.cpu_affinity),
        (
            "selected CPU topology",
            before.selected_cpu_topology != after.selected_cpu_topology,
        ),
        (
            "CPU scaling governors",
            before.scaling_governors != after.scaling_governors,
        ),
        (
            "energy performance preferences",
            before.energy_performance_preferences != after.energy_performance_preferences,
        ),
        (
            "CPU boost controls",
            before.boost_controls != after.boost_controls,
        ),
        (
            "NUMA online set",
            before.numa_nodes_online != after.numa_nodes_online,
        ),
    ] {
        if changed {
            provenance.add_validation_error(format!(
                "bridge host state changed during measurement: {name}"
            ));
        }
    }
}

#[cfg(feature = "bridge-experiment")]
fn bridge_result<T>(result: Result<T, fsqlite::FrankenError>, context: &str) -> Result<T, String> {
    result.map_err(|error| format!("{context}: {error}"))
}

#[cfg(feature = "bridge-experiment")]
fn bridge_block_on<F>(runtime_entries: &mut usize, future: F) -> F::Output
where
    F: std::future::Future,
{
    *runtime_entries += 1;
    fsqlite_e2e::block_on(future)
}

#[cfg(feature = "bridge-experiment")]
fn bridge_elapsed_ns(elapsed: Duration) -> u64 {
    u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX)
}

#[cfg(feature = "bridge-experiment")]
fn bridge_expected_checksum(operation_count: usize) -> Result<(i64, i64), String> {
    let count = i64::try_from(operation_count)
        .map_err(|_| "bridge operation count exceeds i64::MAX".to_owned())?;
    let sum = i128::from(count)
        .checked_mul(i128::from(count.saturating_sub(1)))
        .and_then(|value| value.checked_div(2))
        .and_then(|value| i64::try_from(value).ok())
        .ok_or_else(|| "bridge checksum would overflow i64".to_owned())?;
    Ok((count, sum))
}

#[cfg(feature = "bridge-experiment")]
fn bridge_pragmas() -> Vec<String> {
    std::iter::once(format!(
        "PRAGMA page_size = {};",
        benchmark_page_size_bytes()
    ))
    .chain(
        FSQLITE_BENCHMARK_PRAGMAS
            .iter()
            .map(|pragma| (*pragma).to_owned()),
    )
    .chain(std::iter::once(
        "PRAGMA fsqlite.concurrent_mode=ON;".to_owned(),
    ))
    .collect()
}

#[cfg(feature = "bridge-experiment")]
fn bridge_expected_effective_settings() -> BTreeMap<String, String> {
    BTreeMap::from([
        (
            "page_size".to_owned(),
            benchmark_page_size_bytes().to_string(),
        ),
        ("journal_mode".to_owned(), "memory".to_owned()),
        ("synchronous".to_owned(), "normal".to_owned()),
        ("cache_size".to_owned(), "-64000".to_owned()),
        ("concurrent_mode".to_owned(), "on".to_owned()),
    ])
}

#[cfg(feature = "bridge-experiment")]
fn bridge_verify_effective_settings(
    mut profile: BTreeMap<String, String>,
    concurrent_mode: String,
    context: &str,
) -> Result<BTreeMap<String, String>, String> {
    let synchronous = profile
        .get("synchronous")
        .ok_or_else(|| format!("{context}: settings omitted synchronous"))
        .and_then(|value| normalized_synchronous(value))?;
    profile.insert("synchronous".to_owned(), synchronous);
    if !matches!(
        concurrent_mode.to_ascii_lowercase().as_str(),
        "1" | "true" | "on"
    ) {
        return Err(format!(
            "{context}: concurrent-mode readback was `{concurrent_mode}`, expected enabled"
        ));
    }
    profile.insert("concurrent_mode".to_owned(), "on".to_owned());
    let expected = bridge_expected_effective_settings();
    if profile != expected {
        return Err(format!(
            "{context}: expected effective settings {expected:?}, observed {profile:?}"
        ));
    }
    Ok(profile)
}

#[cfg(feature = "bridge-experiment")]
fn bridge_query_settings_per_operation(
    conn: &fsqlite::Connection,
    runtime_entries: &mut usize,
) -> Result<BTreeMap<String, String>, String> {
    let profile = query_effective_fsqlite_pragmas(conn)?;
    *runtime_entries = runtime_entries
        .checked_add(4)
        .ok_or_else(|| "per-operation settings runtime-entry count overflowed usize".to_owned())?;
    let concurrent_mode = query_fsqlite_scalar(conn, "PRAGMA fsqlite.concurrent_mode;")?;
    *runtime_entries = runtime_entries
        .checked_add(1)
        .ok_or_else(|| "per-operation settings runtime-entry count overflowed usize".to_owned())?;
    bridge_verify_effective_settings(profile, concurrent_mode, "per-operation arm")
}

#[cfg(feature = "bridge-experiment")]
fn bridge_setting_from_rows(rows: &[fsqlite::Row], sql: &str) -> Result<String, String> {
    let row = rows
        .first()
        .ok_or_else(|| format!("bridge `{sql}` returned no row"))?;
    row.get(0)
        .map(normalize_fsqlite_value)
        .ok_or_else(|| format!("bridge `{sql}` returned no first column"))
}

#[cfg(feature = "bridge-experiment")]
fn bridge_setting_from_row(row: &fsqlite::Row, sql: &str) -> Result<String, String> {
    row.get(0)
        .map(normalize_fsqlite_value)
        .ok_or_else(|| format!("bridge `{sql}` returned no first column"))
}

#[cfg(feature = "bridge-experiment")]
fn bridge_verify_affected_rows(affected: usize, context: &str) -> Result<(), String> {
    if affected == 1 {
        Ok(())
    } else {
        Err(format!(
            "{context}: expected one affected row, got {affected}"
        ))
    }
}

#[cfg(feature = "bridge-experiment")]
fn bridge_checksum_from_row(
    row: &fsqlite::Row,
    operation_count: usize,
    context: &str,
) -> Result<(i64, i64, i64), String> {
    let actual = (
        fsqlite_integer(row, 0, context),
        fsqlite_integer(row, 1, context),
        fsqlite_integer(row, 2, context),
    );
    let expected_checksum = bridge_expected_checksum(operation_count)?;
    let expected_exact_rows = i64::try_from(operation_count)
        .map_err(|_| format!("{context}: operation count exceeds i64::MAX"))?;
    let expected = (
        expected_checksum.0,
        expected_checksum.1,
        expected_exact_rows,
    );
    if actual == expected {
        Ok(actual)
    } else {
        Err(format!(
            "{context}: expected COUNT/SUM/exact-row receipt {expected:?}, got {actual:?}"
        ))
    }
}

#[cfg(feature = "bridge-experiment")]
fn bridge_sample_ready_per_operation(
    operation_count: usize,
    block_index: usize,
    order_slot: usize,
) -> Result<JsonBridgeSample, String> {
    let mut runtime_entries = 0_usize;
    let sentinel = bridge_block_on(&mut runtime_entries, std::future::ready(0x4653_514c_i64));
    if sentinel != 0x4653_514c {
        return Err(format!(
            "per-operation ready control preflight returned {sentinel}"
        ));
    }
    let entries_before_timing = runtime_entries;
    let start = Instant::now();
    for operation_index in 0..operation_count {
        let future = std::hint::black_box(std::future::ready(operation_index));
        let completion = fsqlite_e2e::block_on(future);
        let _ = std::hint::black_box(completion);
    }
    let elapsed = start.elapsed();
    runtime_entries = runtime_entries
        .checked_add(operation_count)
        .ok_or_else(|| "ready-future runtime-entry count overflowed usize".to_owned())?;
    let timed_runtime_entries = operation_count;
    debug_assert_eq!(
        runtime_entries.saturating_sub(entries_before_timing),
        timed_runtime_entries
    );
    let completion_count = i64::try_from(operation_count)
        .map_err(|_| "ready-future operation count exceeds i64::MAX".to_owned())?;

    Ok(JsonBridgeSample {
        workload: BridgeWorkload::ReadyFuture,
        operation_count,
        block_index,
        order_slot,
        arm: BridgeArm::PerOperationBlockOn,
        elapsed_ns: bridge_elapsed_ns(elapsed),
        runtime_entries_total: runtime_entries,
        runtime_entries_inside_timed_region: timed_runtime_entries,
        caller_future_completions_inside_timed_region: operation_count,
        engine_dml_future_calls_inside_timed_region: 0,
        worker_commands_total: 0,
        worker_commands_inside_timed_region: 0,
        worker_open_handshakes_total: 0,
        effective_settings: BTreeMap::new(),
        oracle_kind: "untimed_ready_sentinel_plus_control_flow_completion_count".to_owned(),
        checksum_count: completion_count,
        checksum_sum: 0,
        checksum_exact_rows: completion_count,
    })
}

#[cfg(feature = "bridge-experiment")]
fn bridge_sample_ready_single_runtime(
    runtime: &Runtime,
    operation_count: usize,
    block_index: usize,
    order_slot: usize,
) -> Result<JsonBridgeSample, String> {
    let elapsed = runtime.block_on(async {
        let sentinel = std::future::ready(0x4653_514c_i64).await;
        if sentinel != 0x4653_514c {
            return Err(format!(
                "existing-runtime ready control preflight returned {sentinel}"
            ));
        }
        let start = Instant::now();
        for operation_index in 0..operation_count {
            let future = std::hint::black_box(std::future::ready(operation_index));
            let completion = future.await;
            let _ = std::hint::black_box(completion);
        }
        Ok::<_, String>(start.elapsed())
    })?;
    let completion_count = i64::try_from(operation_count)
        .map_err(|_| "ready-future operation count exceeds i64::MAX".to_owned())?;

    Ok(JsonBridgeSample {
        workload: BridgeWorkload::ReadyFuture,
        operation_count,
        block_index,
        order_slot,
        arm: BridgeArm::SingleRuntimeEntry,
        elapsed_ns: bridge_elapsed_ns(elapsed),
        runtime_entries_total: 1,
        runtime_entries_inside_timed_region: 0,
        caller_future_completions_inside_timed_region: operation_count,
        engine_dml_future_calls_inside_timed_region: 0,
        worker_commands_total: 0,
        worker_commands_inside_timed_region: 0,
        worker_open_handshakes_total: 0,
        effective_settings: BTreeMap::new(),
        oracle_kind: "untimed_ready_sentinel_plus_control_flow_completion_count".to_owned(),
        checksum_count: completion_count,
        checksum_sum: 0,
        checksum_exact_rows: completion_count,
    })
}

#[cfg(feature = "bridge-experiment")]
fn bridge_sample_insert_per_operation(
    workload: BridgeWorkload,
    operation_count: usize,
    block_index: usize,
    order_slot: usize,
) -> Result<JsonBridgeSample, String> {
    if workload == BridgeWorkload::ReadyFuture {
        return Err("ready-future workload is not an insert workload".to_owned());
    }
    let mut runtime_entries = 0_usize;
    let conn = bridge_result(
        bridge_block_on(&mut runtime_entries, fsqlite::Connection::open(":memory:")),
        "per-operation arm open",
    )?;
    for pragma in bridge_pragmas() {
        bridge_result(
            bridge_block_on(&mut runtime_entries, conn.execute(&pragma)),
            &format!("per-operation arm configure `{pragma}`"),
        )?;
    }
    let effective_settings = bridge_query_settings_per_operation(&conn, &mut runtime_entries)?;
    bridge_result(
        bridge_block_on(
            &mut runtime_entries,
            conn.execute(
                "CREATE TABLE bridge_probe(\
                 id INTEGER PRIMARY KEY, value INTEGER NOT NULL)",
            ),
        ),
        "per-operation arm create schema",
    )?;
    bridge_result(
        bridge_block_on(&mut runtime_entries, conn.execute("BEGIN")),
        "per-operation arm begin",
    )?;

    let (elapsed, affected_total) = match workload {
        BridgeWorkload::PreparedInsert => {
            let statement = bridge_result(
                bridge_block_on(&mut runtime_entries, conn.prepare(BRIDGE_INSERT_SQL)),
                "per-operation arm prepare",
            )?;
            let warm_affected = bridge_result(
                bridge_block_on(
                    &mut runtime_entries,
                    statement.execute_with_params(&[
                        fsqlite::SqliteValue::Integer(-1),
                        fsqlite::SqliteValue::Integer(-1),
                    ]),
                ),
                "per-operation prepared warmup",
            )?;
            bridge_verify_affected_rows(warm_affected, "per-operation prepared warmup")?;
            let deleted = bridge_result(
                bridge_block_on(
                    &mut runtime_entries,
                    conn.execute("DELETE FROM bridge_probe WHERE id = -1"),
                ),
                "per-operation prepared warmup cleanup",
            )?;
            bridge_verify_affected_rows(deleted, "per-operation prepared warmup cleanup")?;

            let start = Instant::now();
            let mut affected_total = 0_usize;
            for value in 0..operation_count {
                let value = i64::try_from(value)
                    .map_err(|_| "prepared-insert value exceeds i64::MAX".to_owned())?;
                let affected = bridge_result(
                    fsqlite_e2e::block_on(statement.execute_with_params(&[
                        fsqlite::SqliteValue::Integer(value),
                        fsqlite::SqliteValue::Integer(value),
                    ])),
                    "per-operation prepared timed insert",
                )?;
                affected_total = affected_total.saturating_add(affected);
            }
            let elapsed = start.elapsed();
            drop(statement);
            (elapsed, affected_total)
        }
        BridgeWorkload::RawExecuteWithParams => {
            let warm_affected = bridge_result(
                bridge_block_on(
                    &mut runtime_entries,
                    conn.execute_with_params(
                        BRIDGE_INSERT_SQL,
                        &[
                            fsqlite::SqliteValue::Integer(-1),
                            fsqlite::SqliteValue::Integer(-1),
                        ],
                    ),
                ),
                "per-operation raw warmup",
            )?;
            bridge_verify_affected_rows(warm_affected, "per-operation raw warmup")?;
            let deleted = bridge_result(
                bridge_block_on(
                    &mut runtime_entries,
                    conn.execute("DELETE FROM bridge_probe WHERE id = -1"),
                ),
                "per-operation raw warmup cleanup",
            )?;
            bridge_verify_affected_rows(deleted, "per-operation raw warmup cleanup")?;

            let start = Instant::now();
            let mut affected_total = 0_usize;
            for value in 0..operation_count {
                let value = i64::try_from(value)
                    .map_err(|_| "raw-insert value exceeds i64::MAX".to_owned())?;
                let affected = bridge_result(
                    fsqlite_e2e::block_on(conn.execute_with_params(
                        BRIDGE_INSERT_SQL,
                        &[
                            fsqlite::SqliteValue::Integer(value),
                            fsqlite::SqliteValue::Integer(value),
                        ],
                    )),
                    "per-operation raw timed insert",
                )?;
                affected_total = affected_total.saturating_add(affected);
            }
            (start.elapsed(), affected_total)
        }
        BridgeWorkload::ReadyFuture => unreachable!(),
    };
    runtime_entries = runtime_entries
        .checked_add(operation_count)
        .ok_or_else(|| "per-operation runtime-entry count overflowed usize".to_owned())?;
    if affected_total != operation_count {
        return Err(format!(
            "per-operation {} timed inserts affected {affected_total} rows, expected {operation_count}",
            workload.id()
        ));
    }

    bridge_result(
        bridge_block_on(&mut runtime_entries, conn.execute("COMMIT")),
        "per-operation arm commit",
    )?;
    let checksum_row = bridge_result(
        bridge_block_on(
            &mut runtime_entries,
            conn.query_row_with_params(
                BRIDGE_EXACT_ORACLE_SQL,
                &[fsqlite::SqliteValue::Integer(
                    i64::try_from(operation_count)
                        .map_err(|_| "per-operation oracle count exceeds i64::MAX".to_owned())?,
                )],
            ),
        ),
        "per-operation arm checksum query",
    )?;
    let checksum =
        bridge_checksum_from_row(&checksum_row, operation_count, "per-operation arm checksum")?;
    bridge_result(
        bridge_block_on(&mut runtime_entries, conn.close()),
        "per-operation arm close",
    )?;

    Ok(JsonBridgeSample {
        workload,
        operation_count,
        block_index,
        order_slot,
        arm: BridgeArm::PerOperationBlockOn,
        elapsed_ns: bridge_elapsed_ns(elapsed),
        runtime_entries_total: runtime_entries,
        runtime_entries_inside_timed_region: operation_count,
        caller_future_completions_inside_timed_region: operation_count,
        engine_dml_future_calls_inside_timed_region: operation_count,
        worker_commands_total: 0,
        worker_commands_inside_timed_region: 0,
        worker_open_handshakes_total: 0,
        effective_settings,
        oracle_kind: "untimed_exact_id_value_domain_query".to_owned(),
        checksum_count: checksum.0,
        checksum_sum: checksum.1,
        checksum_exact_rows: checksum.2,
    })
}

#[cfg(feature = "bridge-experiment")]
// Boxing the timed per-operation awaits would add an allocation to the mechanism
// this diagnostic exists to measure, so retain the single enclosing future.
#[allow(clippy::large_futures)]
fn bridge_sample_insert_single_runtime(
    runtime: &Runtime,
    workload: BridgeWorkload,
    operation_count: usize,
    block_index: usize,
    order_slot: usize,
) -> Result<JsonBridgeSample, String> {
    if workload == BridgeWorkload::ReadyFuture {
        return Err("ready-future workload is not an insert workload".to_owned());
    }
    let (elapsed, checksum, effective_settings) = runtime.block_on(async {
        let conn = bridge_result(
            fsqlite::Connection::open(":memory:").await,
            "single-runtime arm open",
        )?;
        for pragma in bridge_pragmas() {
            bridge_result(
                conn.execute(&pragma).await,
                &format!("single-runtime arm configure `{pragma}`"),
            )?;
        }
        let mut effective_settings = BTreeMap::new();
        for pragma in ["page_size", "journal_mode", "synchronous", "cache_size"] {
            let sql = format!("PRAGMA {pragma};");
            let rows = bridge_result(
                conn.query(&sql).await,
                &format!("single-runtime arm read back `{sql}`"),
            )?;
            effective_settings.insert(
                pragma.to_owned(),
                bridge_setting_from_rows(&rows, &sql)?,
            );
        }
        let concurrent_sql = "PRAGMA fsqlite.concurrent_mode;";
        let concurrent_rows = bridge_result(
            conn.query(concurrent_sql).await,
            "single-runtime arm read back concurrent mode",
        )?;
        let concurrent_mode = bridge_setting_from_rows(&concurrent_rows, concurrent_sql)?;
        let effective_settings = bridge_verify_effective_settings(
            effective_settings,
            concurrent_mode,
            "single-runtime arm",
        )?;
        bridge_result(
            conn.execute(
                "CREATE TABLE bridge_probe(\
                 id INTEGER PRIMARY KEY, value INTEGER NOT NULL)",
            )
            .await,
            "single-runtime arm create schema",
        )?;
        bridge_result(conn.execute("BEGIN").await, "single-runtime arm begin")?;

        let (elapsed, affected_total) = match workload {
            BridgeWorkload::PreparedInsert => {
                let statement = bridge_result(
                    conn.prepare(BRIDGE_INSERT_SQL).await,
                    "single-runtime arm prepare",
                )?;
                let warm_affected = bridge_result(
                    statement
                        .execute_with_params(&[
                            fsqlite::SqliteValue::Integer(-1),
                            fsqlite::SqliteValue::Integer(-1),
                        ])
                        .await,
                    "single-runtime prepared warmup",
                )?;
                bridge_verify_affected_rows(warm_affected, "single-runtime prepared warmup")?;
                let deleted = bridge_result(
                    conn.execute("DELETE FROM bridge_probe WHERE id = -1")
                        .await,
                    "single-runtime prepared warmup cleanup",
                )?;
                bridge_verify_affected_rows(deleted, "single-runtime prepared warmup cleanup")?;

                let start = Instant::now();
                let mut affected_total = 0_usize;
                for value in 0..operation_count {
                    let value = i64::try_from(value)
                        .map_err(|_| "prepared-insert value exceeds i64::MAX".to_owned())?;
                    let affected = bridge_result(
                        statement
                            .execute_with_params(&[
                                fsqlite::SqliteValue::Integer(value),
                                fsqlite::SqliteValue::Integer(value),
                            ])
                            .await,
                        "single-runtime prepared timed insert",
                    )?;
                    affected_total = affected_total.saturating_add(affected);
                }
                let elapsed = start.elapsed();
                drop(statement);
                (elapsed, affected_total)
            }
            BridgeWorkload::RawExecuteWithParams => {
                let warm_affected = bridge_result(
                    conn.execute_with_params(
                        BRIDGE_INSERT_SQL,
                        &[
                            fsqlite::SqliteValue::Integer(-1),
                            fsqlite::SqliteValue::Integer(-1),
                        ],
                    )
                    .await,
                    "single-runtime raw warmup",
                )?;
                bridge_verify_affected_rows(warm_affected, "single-runtime raw warmup")?;
                let deleted = bridge_result(
                    conn.execute("DELETE FROM bridge_probe WHERE id = -1")
                        .await,
                    "single-runtime raw warmup cleanup",
                )?;
                bridge_verify_affected_rows(deleted, "single-runtime raw warmup cleanup")?;

                let start = Instant::now();
                let mut affected_total = 0_usize;
                for value in 0..operation_count {
                    let value = i64::try_from(value)
                        .map_err(|_| "raw-insert value exceeds i64::MAX".to_owned())?;
                    let affected = bridge_result(
                        conn.execute_with_params(
                            BRIDGE_INSERT_SQL,
                            &[
                                fsqlite::SqliteValue::Integer(value),
                                fsqlite::SqliteValue::Integer(value),
                            ],
                        )
                        .await,
                        "single-runtime raw timed insert",
                    )?;
                    affected_total = affected_total.saturating_add(affected);
                }
                (start.elapsed(), affected_total)
            }
            BridgeWorkload::ReadyFuture => unreachable!(),
        };
        if affected_total != operation_count {
            return Err(format!(
                "single-runtime {} timed inserts affected {affected_total} rows, expected {operation_count}",
                workload.id()
            ));
        }

        bridge_result(conn.execute("COMMIT").await, "single-runtime arm commit")?;
        let checksum_row = bridge_result(
            conn.query_row_with_params(
                BRIDGE_EXACT_ORACLE_SQL,
                &[fsqlite::SqliteValue::Integer(
                    i64::try_from(operation_count)
                        .map_err(|_| "single-runtime oracle count exceeds i64::MAX".to_owned())?,
                )],
            )
            .await,
            "single-runtime arm checksum query",
        )?;
        let checksum =
            bridge_checksum_from_row(&checksum_row, operation_count, "single-runtime checksum")?;
        bridge_result(conn.close().await, "single-runtime arm close")?;
        Ok::<_, String>((elapsed, checksum, effective_settings))
    })?;

    Ok(JsonBridgeSample {
        workload,
        operation_count,
        block_index,
        order_slot,
        arm: BridgeArm::SingleRuntimeEntry,
        elapsed_ns: bridge_elapsed_ns(elapsed),
        runtime_entries_total: 1,
        runtime_entries_inside_timed_region: 0,
        caller_future_completions_inside_timed_region: operation_count,
        engine_dml_future_calls_inside_timed_region: operation_count,
        worker_commands_total: 0,
        worker_commands_inside_timed_region: 0,
        worker_open_handshakes_total: 0,
        effective_settings,
        oracle_kind: "untimed_exact_id_value_domain_query".to_owned(),
        checksum_count: checksum.0,
        checksum_sum: checksum.1,
        checksum_exact_rows: checksum.2,
    })
}

#[cfg(feature = "bridge-experiment")]
fn bridge_worker_command<T>(
    command_count: &mut usize,
    result: Result<T, fsqlite::FrankenError>,
    context: &str,
) -> Result<T, String> {
    *command_count += 1;
    bridge_result(result, context)
}

#[cfg(feature = "bridge-experiment")]
fn bridge_sample_insert_worker(
    operation_count: usize,
    block_index: usize,
    order_slot: usize,
) -> Result<JsonBridgeSample, String> {
    let mut conn = bridge_result(
        fsqlite::AsyncConnection::open_sync(":memory:"),
        "worker arm open",
    )?;
    let mut worker_commands = 0_usize;
    for pragma in bridge_pragmas() {
        let result = conn.execute_sync(&pragma);
        bridge_worker_command(
            &mut worker_commands,
            result,
            &format!("worker arm configure `{pragma}`"),
        )?;
    }
    let mut effective_settings = BTreeMap::new();
    for pragma in ["page_size", "journal_mode", "synchronous", "cache_size"] {
        let sql = format!("PRAGMA {pragma};");
        let result = conn.query_row_sync(&sql);
        let row = bridge_worker_command(
            &mut worker_commands,
            result,
            &format!("worker arm read back `{sql}`"),
        )?;
        effective_settings.insert(pragma.to_owned(), bridge_setting_from_row(&row, &sql)?);
    }
    let concurrent_sql = "PRAGMA fsqlite.concurrent_mode;";
    let result = conn.query_row_sync(concurrent_sql);
    let row = bridge_worker_command(
        &mut worker_commands,
        result,
        "worker arm read back concurrent mode",
    )?;
    let effective_settings = bridge_verify_effective_settings(
        effective_settings,
        bridge_setting_from_row(&row, concurrent_sql)?,
        "worker arm",
    )?;
    let result = conn.execute_sync(
        "CREATE TABLE bridge_probe(\
         id INTEGER PRIMARY KEY, value INTEGER NOT NULL)",
    );
    bridge_worker_command(&mut worker_commands, result, "worker arm create schema")?;
    let result = conn.begin_transaction_sync();
    let mut transaction = bridge_worker_command(&mut worker_commands, result, "worker arm begin")?;

    let result = transaction.execute_with_params_sync(
        BRIDGE_INSERT_SQL,
        &[
            fsqlite::SqliteValue::Integer(-1),
            fsqlite::SqliteValue::Integer(-1),
        ],
    );
    let warm_affected = bridge_worker_command(&mut worker_commands, result, "worker raw warmup")?;
    bridge_verify_affected_rows(warm_affected, "worker raw warmup")?;
    let result = transaction.execute_sync("DELETE FROM bridge_probe WHERE id = -1");
    let deleted = bridge_worker_command(&mut worker_commands, result, "worker raw warmup cleanup")?;
    bridge_verify_affected_rows(deleted, "worker raw warmup cleanup")?;

    let start = Instant::now();
    let mut affected_total = 0_usize;
    for value in 0..operation_count {
        let value =
            i64::try_from(value).map_err(|_| "worker-insert value exceeds i64::MAX".to_owned())?;
        let result = transaction.execute_with_params_sync(
            BRIDGE_INSERT_SQL,
            &[
                fsqlite::SqliteValue::Integer(value),
                fsqlite::SqliteValue::Integer(value),
            ],
        );
        let affected = bridge_result(result, "worker raw timed insert")?;
        affected_total = affected_total.saturating_add(affected);
    }
    let elapsed = start.elapsed();
    worker_commands = worker_commands
        .checked_add(operation_count)
        .ok_or_else(|| "worker command count overflowed usize".to_owned())?;
    let timed_worker_commands = operation_count;
    if affected_total != operation_count {
        return Err(format!(
            "worker raw timed inserts affected {affected_total} rows, expected {operation_count}"
        ));
    }

    let result = transaction.commit_sync();
    bridge_worker_command(&mut worker_commands, result, "worker arm commit")?;
    drop(transaction);
    let result = conn.query_row_with_params_sync(
        BRIDGE_EXACT_ORACLE_SQL,
        &[fsqlite::SqliteValue::Integer(
            i64::try_from(operation_count)
                .map_err(|_| "worker oracle count exceeds i64::MAX".to_owned())?,
        )],
    );
    let checksum_row =
        bridge_worker_command(&mut worker_commands, result, "worker arm checksum query")?;
    let checksum = bridge_checksum_from_row(&checksum_row, operation_count, "worker checksum")?;
    let result = conn.close_sync();
    bridge_worker_command(&mut worker_commands, result, "worker arm close")?;

    Ok(JsonBridgeSample {
        workload: BridgeWorkload::RawExecuteWithParams,
        operation_count,
        block_index,
        order_slot,
        arm: BridgeArm::WorkerSyncFacade,
        elapsed_ns: bridge_elapsed_ns(elapsed),
        runtime_entries_total: 0,
        runtime_entries_inside_timed_region: 0,
        caller_future_completions_inside_timed_region: 0,
        engine_dml_future_calls_inside_timed_region: operation_count,
        worker_commands_total: worker_commands,
        worker_commands_inside_timed_region: timed_worker_commands,
        worker_open_handshakes_total: 1,
        effective_settings,
        oracle_kind: "untimed_exact_id_value_domain_query".to_owned(),
        checksum_count: checksum.0,
        checksum_sum: checksum.1,
        checksum_exact_rows: checksum.2,
    })
}

#[cfg(feature = "bridge-experiment")]
fn bridge_percentile(sorted: &[f64], percentile: f64) -> f64 {
    debug_assert!(!sorted.is_empty());
    if sorted.len() == 1 {
        return sorted[0];
    }
    let index = (percentile / 100.0) * (sorted.len() - 1) as f64;
    let lower = index.floor() as usize;
    let upper = index.ceil() as usize;
    let fraction = index - lower as f64;
    if lower == upper {
        sorted[lower]
    } else {
        sorted[upper].mul_add(fraction, sorted[lower] * (1.0 - fraction))
    }
}

#[cfg(feature = "bridge-experiment")]
fn bridge_arm_statistics(samples: &[JsonBridgeSample]) -> Vec<JsonBridgeArmStats> {
    let mut grouped: BTreeMap<(BridgeWorkload, usize, BridgeArm), Vec<f64>> = BTreeMap::new();
    for sample in samples {
        grouped
            .entry((sample.workload, sample.operation_count, sample.arm))
            .or_default()
            .push(sample.elapsed_ns as f64);
    }

    grouped
        .into_iter()
        .map(|((workload, operation_count, arm), mut values)| {
            values.sort_by(f64::total_cmp);
            let sample_count = values.len();
            let mean = values.iter().sum::<f64>() / sample_count as f64;
            let variance = values
                .iter()
                .map(|value| {
                    let delta = *value - mean;
                    delta * delta
                })
                .sum::<f64>()
                / sample_count as f64;
            let stddev = variance.sqrt();
            let median = bridge_percentile(&values, 50.0);
            JsonBridgeArmStats {
                workload,
                operation_count,
                arm,
                samples: sample_count,
                median_ns: median,
                mean_ns: mean,
                p95_ns: bridge_percentile(&values, 95.0),
                stddev_ns: stddev,
                cv_pct: if mean > 0.0 {
                    stddev / mean * 100.0
                } else {
                    0.0
                },
                median_ns_per_operation: median / operation_count.max(1) as f64,
            }
        })
        .collect()
}

#[cfg(feature = "bridge-experiment")]
fn bridge_bootstrap_mean_ci95(
    values: &[f64],
    cluster_width: usize,
    seed: u64,
) -> Result<(f64, f64, usize), String> {
    debug_assert!(!values.is_empty());
    if cluster_width == 0 || values.len() % cluster_width != 0 {
        return Err(format!(
            "bootstrap requires complete width-{cluster_width} clusters for {} values",
            values.len()
        ));
    }
    const RESAMPLES: usize = 10_000;
    let clusters = values.chunks_exact(cluster_width).collect::<Vec<_>>();
    let mut rng = StdRng::seed_from_u64(seed);
    let mut means = Vec::with_capacity(RESAMPLES);
    for _ in 0..RESAMPLES {
        let mut total = 0.0_f64;
        for _ in 0..clusters.len() {
            total += clusters[rng.random_range(0..clusters.len())]
                .iter()
                .sum::<f64>();
        }
        means.push(total / values.len() as f64);
    }
    means.sort_by(f64::total_cmp);
    Ok((
        bridge_percentile(&means, 2.5),
        bridge_percentile(&means, 97.5),
        clusters.len(),
    ))
}

#[cfg(feature = "bridge-experiment")]
fn bridge_paired_comparison(
    samples: &[JsonBridgeSample],
    workload: BridgeWorkload,
    operation_count: usize,
    numerator: BridgeArm,
    denominator: BridgeArm,
    bootstrap_cluster_width: usize,
    seed: u64,
) -> Result<JsonBridgePairedComparison, String> {
    let mut blocks: BTreeMap<usize, (Vec<f64>, Vec<f64>)> = BTreeMap::new();
    for sample in samples
        .iter()
        .filter(|sample| sample.workload == workload && sample.operation_count == operation_count)
    {
        let block = blocks.entry(sample.block_index).or_default();
        if sample.arm == numerator {
            block.0.push(sample.elapsed_ns as f64);
        } else if sample.arm == denominator {
            block.1.push(sample.elapsed_ns as f64);
        }
    }

    let mut ratios = Vec::with_capacity(blocks.len());
    for (block_index, (numerator_values, denominator_values)) in blocks {
        if numerator_values.len() != 2 || denominator_values.len() != 2 {
            return Err(format!(
                "{} operations={operation_count} block {block_index} has {} {} samples and {} {} samples; expected two per arm",
                workload.id(),
                numerator_values.len(),
                numerator.id(),
                denominator_values.len(),
                denominator.id()
            ));
        }
        let numerator_mean = numerator_values.iter().sum::<f64>() / 2.0;
        let denominator_mean = denominator_values.iter().sum::<f64>() / 2.0;
        for (arm, mean) in [
            (numerator.id(), numerator_mean),
            (denominator.id(), denominator_mean),
        ] {
            if !mean.is_finite() || mean <= 0.0 {
                return Err(format!(
                    "{} operations={operation_count} block {block_index} has invalid {arm} mean {mean}",
                    workload.id()
                ));
            }
        }
        ratios.push(numerator_mean / denominator_mean);
    }
    if ratios.is_empty() {
        return Err(format!(
            "no paired blocks for {} operations={operation_count} {} / {}",
            workload.id(),
            numerator.id(),
            denominator.id()
        ));
    }

    let mut sorted = ratios.clone();
    sorted.sort_by(f64::total_cmp);
    let mean_ratio = ratios.iter().sum::<f64>() / ratios.len() as f64;
    let geomean_ratio =
        (ratios.iter().map(|ratio| ratio.ln()).sum::<f64>() / ratios.len() as f64).exp();
    let (ci_low, ci_high, bootstrap_clusters) =
        bridge_bootstrap_mean_ci95(&ratios, bootstrap_cluster_width, seed)?;
    Ok(JsonBridgePairedComparison {
        workload,
        operation_count,
        numerator,
        denominator,
        paired_blocks: ratios.len(),
        bootstrap_clusters,
        median_ratio: bridge_percentile(&sorted, 50.0),
        mean_ratio,
        geomean_ratio,
        bootstrap_mean_ratio_ci95_low: ci_low,
        bootstrap_mean_ratio_ci95_high: ci_high,
    })
}

#[cfg(feature = "bridge-experiment")]
fn bridge_linear_fit(points: &[(f64, f64)]) -> Result<(f64, f64, f64), String> {
    if points.len() < 2 {
        return Err("linear fit requires at least two points".to_owned());
    }
    let mean_x = points.iter().map(|point| point.0).sum::<f64>() / points.len() as f64;
    let mean_y = points.iter().map(|point| point.1).sum::<f64>() / points.len() as f64;
    let covariance = points
        .iter()
        .map(|point| (point.0 - mean_x) * (point.1 - mean_y))
        .sum::<f64>();
    let variance_x = points
        .iter()
        .map(|point| {
            let delta = point.0 - mean_x;
            delta * delta
        })
        .sum::<f64>();
    if variance_x == 0.0 {
        return Err("linear fit predictor has zero variance".to_owned());
    }
    let slope = covariance / variance_x;
    let intercept = slope.mul_add(-mean_x, mean_y);
    let residual_sum_squares = points
        .iter()
        .map(|point| {
            let predicted = slope.mul_add(point.0, intercept);
            let residual = point.1 - predicted;
            residual * residual
        })
        .sum::<f64>();
    let total_sum_squares = points
        .iter()
        .map(|point| {
            let delta = point.1 - mean_y;
            delta * delta
        })
        .sum::<f64>();
    let r_squared = if total_sum_squares > 0.0 {
        1.0 - residual_sum_squares / total_sum_squares
    } else if residual_sum_squares == 0.0 {
        1.0
    } else {
        0.0
    };
    Ok((intercept, slope, r_squared))
}

#[cfg(feature = "bridge-experiment")]
fn bridge_ready_regression(
    samples: &[JsonBridgeSample],
    bootstrap_cluster_width: usize,
    seed: u64,
) -> Result<JsonBridgeReadyRegression, String> {
    let mut grouped: BTreeMap<(usize, usize), (Vec<f64>, Vec<f64>)> = BTreeMap::new();
    for sample in samples
        .iter()
        .filter(|sample| sample.workload == BridgeWorkload::ReadyFuture)
    {
        let pair = grouped
            .entry((sample.block_index, sample.operation_count))
            .or_default();
        match sample.arm {
            BridgeArm::PerOperationBlockOn => pair.0.push(sample.elapsed_ns as f64),
            BridgeArm::SingleRuntimeEntry => pair.1.push(sample.elapsed_ns as f64),
            BridgeArm::WorkerSyncFacade => {
                return Err("ready-future control unexpectedly contains worker samples".to_owned());
            }
        }
    }
    if grouped.is_empty() {
        return Err("ready-future regression has no paired samples".to_owned());
    }

    let mut points_by_block: BTreeMap<usize, Vec<(f64, f64)>> = BTreeMap::new();
    for ((block_index, operation_count), (per_operation, existing_runtime)) in grouped {
        if per_operation.len() != 2 || existing_runtime.len() != 2 {
            return Err(format!(
                "ready-future operations={operation_count} block {block_index} has {} per-operation and {} existing-runtime samples; expected two each",
                per_operation.len(),
                existing_runtime.len()
            ));
        }
        let per_operation_mean = per_operation.iter().sum::<f64>() / 2.0;
        let existing_runtime_mean = existing_runtime.iter().sum::<f64>() / 2.0;
        points_by_block.entry(block_index).or_default().push((
            operation_count.saturating_sub(1) as f64,
            per_operation_mean - existing_runtime_mean,
        ));
    }
    let expected_points_per_block = points_by_block.values().next().map_or(0, Vec::len);
    if expected_points_per_block < 2
        || points_by_block
            .values()
            .any(|points| points.len() != expected_points_per_block)
    {
        return Err(
            "ready-future regression blocks do not contain the same operation-count matrix"
                .to_owned(),
        );
    }

    let points = points_by_block
        .values()
        .flat_map(|block| block.iter().copied())
        .collect::<Vec<_>>();
    let (intercept, slope, r_squared) = bridge_linear_fit(&points)?;

    const RESAMPLES: usize = 10_000;
    let blocks = points_by_block.values().collect::<Vec<_>>();
    if bootstrap_cluster_width == 0 || blocks.len() % bootstrap_cluster_width != 0 {
        return Err(format!(
            "ready-future regression requires complete width-{bootstrap_cluster_width} Williams-cycle clusters, got {} blocks",
            blocks.len()
        ));
    }
    let bootstrap_clusters = blocks
        .chunks_exact(bootstrap_cluster_width)
        .collect::<Vec<_>>();
    let mut rng = StdRng::seed_from_u64(seed);
    let mut bootstrap_slopes = Vec::with_capacity(RESAMPLES);
    for _ in 0..RESAMPLES {
        let mut resampled_points =
            Vec::with_capacity(blocks.len().saturating_mul(expected_points_per_block));
        for _ in 0..bootstrap_clusters.len() {
            let cluster = bootstrap_clusters[rng.random_range(0..bootstrap_clusters.len())];
            for block in cluster {
                resampled_points.extend_from_slice(block);
            }
        }
        bootstrap_slopes.push(bridge_linear_fit(&resampled_points)?.1);
    }
    bootstrap_slopes.sort_by(f64::total_cmp);

    Ok(JsonBridgeReadyRegression {
        predictor: "additional per-operation Runtime::block_on entries (N - 1)".to_owned(),
        response:
            "paired block mean(per_operation_block_on) - mean(inside_existing_runtime) ns"
                .to_owned(),
        interpretation:
            "slope estimates each block_on entry beyond the first; intercept estimates the first timed block_on entry plus fixed arm difference"
                .to_owned(),
        points: points.len(),
        paired_blocks: blocks.len(),
        bootstrap_clusters: bootstrap_clusters.len(),
        intercept_ns: intercept,
        slope_ns_per_additional_runtime_entry: slope,
        bootstrap_slope_ci95_low: bridge_percentile(&bootstrap_slopes, 2.5),
        bootstrap_slope_ci95_high: bridge_percentile(&bootstrap_slopes, 97.5),
        r_squared,
    })
}

#[cfg(feature = "bridge-experiment")]
fn bridge_two_arm_orders(
    block_count: usize,
    rng: &mut StdRng,
) -> Result<Vec<[BridgeArm; 4]>, String> {
    if block_count == 0 || block_count % 2 != 0 {
        return Err(format!(
            "two-arm ordering requires complete complementary ABBA/BAAB block pairs, got {block_count} blocks"
        ));
    }
    let abba = [
        BridgeArm::PerOperationBlockOn,
        BridgeArm::SingleRuntimeEntry,
        BridgeArm::SingleRuntimeEntry,
        BridgeArm::PerOperationBlockOn,
    ];
    let baab = [
        BridgeArm::SingleRuntimeEntry,
        BridgeArm::PerOperationBlockOn,
        BridgeArm::PerOperationBlockOn,
        BridgeArm::SingleRuntimeEntry,
    ];
    let mut orders = Vec::with_capacity(block_count);
    while orders.len() < block_count {
        if rng.random::<bool>() {
            orders.extend([abba, baab]);
        } else {
            orders.extend([baab, abba]);
        }
    }
    Ok(orders)
}

#[cfg(feature = "bridge-experiment")]
fn bridge_three_arm_orders(
    block_count: usize,
    rng: &mut StdRng,
) -> Result<Vec<[BridgeArm; 6]>, String> {
    if block_count == 0 || block_count % 3 != 0 {
        return Err(format!(
            "three-arm ordering requires complete three-block carryover cycles, got {block_count} blocks"
        ));
    }
    let mut orders = Vec::with_capacity(block_count);
    while orders.len() < block_count {
        let mut labels = BridgeArm::ALL;
        labels.shuffle(rng);
        for rotation in 0..3 {
            let first = labels[rotation];
            let second = labels[(rotation + 1) % 3];
            let third = labels[(rotation + 2) % 3];
            orders.push([first, second, third, third, second, first]);
        }
    }
    Ok(orders)
}

#[cfg(feature = "bridge-experiment")]
fn bridge_balanced_ready_count_orders(
    operation_counts: &[usize],
    block_count: usize,
    rng: &mut StdRng,
) -> Result<Vec<Vec<usize>>, String> {
    if operation_counts.is_empty() {
        return Err("ready operation-count matrix must not be empty".to_owned());
    }
    let width = operation_counts.len();
    let cycle_blocks = width.saturating_mul(2);
    if block_count == 0 || block_count % cycle_blocks != 0 {
        return Err(format!(
            "ready count ordering requires a whole balanced Williams cycle: \
             block_count={block_count}, required multiple={cycle_blocks}"
        ));
    }
    let mut orders = Vec::with_capacity(block_count);
    while orders.len() < block_count {
        let mut labels = operation_counts.to_vec();
        labels.shuffle(rng);
        let base = (0..width)
            .map(|position| {
                if position == 0 {
                    0
                } else if position % 2 == 1 {
                    position.div_ceil(2)
                } else {
                    width - position / 2
                }
            })
            .collect::<Vec<_>>();
        for rotation in 0..width {
            let order = base
                .iter()
                .map(|index| labels[(index + rotation) % width])
                .collect::<Vec<_>>();
            orders.push(order.clone());
            if orders.len() == block_count {
                break;
            }
            orders.push(order.into_iter().rev().collect());
            if orders.len() == block_count {
                break;
            }
        }
    }
    Ok(orders)
}

#[cfg(feature = "bridge-experiment")]
fn bridge_collect_samples(
    runtime: &Runtime,
    options: &CliOptions,
    ready_operation_counts: &[usize],
) -> Result<(Vec<JsonBridgeSample>, Vec<JsonBridgeHostState>), String> {
    let mut rng = StdRng::seed_from_u64(options.bridge_seed);
    let block_count = options.bridge_samples / 2;
    let estimated = ready_operation_counts
        .len()
        .saturating_mul(options.bridge_samples)
        .saturating_mul(2)
        .saturating_add(options.bridge_samples.saturating_mul(5));
    let mut samples = Vec::with_capacity(estimated);
    let mut host_state_checkpoints = Vec::with_capacity(block_count.saturating_mul(3));

    eprintln!(
        "bridge ready-future control: operations={ready_operation_counts:?}, samples/arm/count={}",
        options.bridge_samples
    );
    let ready_count_orders =
        bridge_balanced_ready_count_orders(ready_operation_counts, block_count, &mut rng)?;
    let ready_arm_orders = bridge_two_arm_orders(block_count, &mut rng)?;
    for (block_index, ready_order) in ready_count_orders.iter().enumerate() {
        for (count_slot, &operation_count) in ready_order.iter().enumerate() {
            for (arm_slot, arm) in ready_arm_orders[block_index].into_iter().enumerate() {
                let order_slot = count_slot.saturating_mul(4).saturating_add(arm_slot);
                let sample = match arm {
                    BridgeArm::PerOperationBlockOn => {
                        bridge_sample_ready_per_operation(operation_count, block_index, order_slot)?
                    }
                    BridgeArm::SingleRuntimeEntry => bridge_sample_ready_single_runtime(
                        runtime,
                        operation_count,
                        block_index,
                        order_slot,
                    )?,
                    BridgeArm::WorkerSyncFacade => unreachable!(),
                };
                samples.push(sample);
            }
        }
        host_state_checkpoints.push(capture_bridge_host_state());
    }

    eprintln!(
        "bridge retained-prepared control: operations={}, samples/arm={}",
        options.bridge_operations, options.bridge_samples
    );
    let prepared_arm_orders = bridge_two_arm_orders(block_count, &mut rng)?;
    for (block_index, order) in prepared_arm_orders.iter().copied().enumerate() {
        for (order_slot, arm) in order.into_iter().enumerate() {
            let sample = match arm {
                BridgeArm::PerOperationBlockOn => bridge_sample_insert_per_operation(
                    BridgeWorkload::PreparedInsert,
                    options.bridge_operations,
                    block_index,
                    order_slot,
                )?,
                BridgeArm::SingleRuntimeEntry => bridge_sample_insert_single_runtime(
                    runtime,
                    BridgeWorkload::PreparedInsert,
                    options.bridge_operations,
                    block_index,
                    order_slot,
                )?,
                BridgeArm::WorkerSyncFacade => unreachable!(),
            };
            samples.push(sample);
        }
        host_state_checkpoints.push(capture_bridge_host_state());
    }

    eprintln!(
        "bridge common raw-DML path: operations={}, samples/arm={}",
        options.bridge_operations, options.bridge_samples
    );
    let three_arm_orders = bridge_three_arm_orders(block_count, &mut rng)?;
    for (block_index, order) in three_arm_orders.into_iter().enumerate() {
        for (order_slot, arm) in order.into_iter().enumerate() {
            let sample = match arm {
                BridgeArm::PerOperationBlockOn => bridge_sample_insert_per_operation(
                    BridgeWorkload::RawExecuteWithParams,
                    options.bridge_operations,
                    block_index,
                    order_slot,
                )?,
                BridgeArm::SingleRuntimeEntry => bridge_sample_insert_single_runtime(
                    runtime,
                    BridgeWorkload::RawExecuteWithParams,
                    options.bridge_operations,
                    block_index,
                    order_slot,
                )?,
                BridgeArm::WorkerSyncFacade => {
                    bridge_sample_insert_worker(options.bridge_operations, block_index, order_slot)?
                }
            };
            samples.push(sample);
        }
        host_state_checkpoints.push(capture_bridge_host_state());
    }

    Ok((samples, host_state_checkpoints))
}

#[cfg(feature = "bridge-experiment")]
fn bridge_build_comparisons(
    samples: &[JsonBridgeSample],
    ready_operation_counts: &[usize],
    operation_count: usize,
    seed: u64,
) -> Result<Vec<JsonBridgePairedComparison>, String> {
    let mut comparisons = Vec::new();
    let ready_bootstrap_cluster_width = ready_operation_counts.len().saturating_mul(2);
    for (index, &ready_count) in ready_operation_counts.iter().enumerate() {
        comparisons.push(bridge_paired_comparison(
            samples,
            BridgeWorkload::ReadyFuture,
            ready_count,
            BridgeArm::PerOperationBlockOn,
            BridgeArm::SingleRuntimeEntry,
            ready_bootstrap_cluster_width,
            seed ^ 0x1000 ^ index as u64,
        )?);
    }
    comparisons.push(bridge_paired_comparison(
        samples,
        BridgeWorkload::PreparedInsert,
        operation_count,
        BridgeArm::PerOperationBlockOn,
        BridgeArm::SingleRuntimeEntry,
        2,
        seed ^ 0x2000,
    )?);
    for (index, (numerator, denominator)) in [
        (
            BridgeArm::PerOperationBlockOn,
            BridgeArm::SingleRuntimeEntry,
        ),
        (BridgeArm::WorkerSyncFacade, BridgeArm::SingleRuntimeEntry),
        (BridgeArm::WorkerSyncFacade, BridgeArm::PerOperationBlockOn),
    ]
    .into_iter()
    .enumerate()
    {
        comparisons.push(bridge_paired_comparison(
            samples,
            BridgeWorkload::RawExecuteWithParams,
            operation_count,
            numerator,
            denominator,
            3,
            seed ^ 0x3000 ^ index as u64,
        )?);
    }
    Ok(comparisons)
}

#[cfg(feature = "bridge-experiment")]
fn run_bridge_experiment(args: &[String], options: &CliOptions) -> Result<(), String> {
    if benchmark_page_size_bytes() != DEFAULT_BENCH_PAGE_SIZE_BYTES {
        return Err(format!(
            "bridge experiment currently requires the default {}-byte page size so every arm opens identically",
            DEFAULT_BENCH_PAGE_SIZE_BYTES
        ));
    }
    bridge_expected_checksum(options.bridge_operations)?;

    // Warm the thread-local bridge once before any sample, then reuse one
    // separately constructed runtime for every single-entry sample.
    fsqlite_e2e::block_on(std::future::ready(()));
    let runtime = RuntimeBuilder::current_thread()
        .build()
        .map_err(|error| format!("could not build bridge experiment runtime: {error}"))?;

    // Fixed width keeps every allowed sample count on complete Williams
    // cycles, so count order and first-order carryover remain exactly
    // balanced rather than changing with the DML operation count.
    let ready_operation_counts = vec![1, 10, 100, 1_000];

    let mut provenance = JsonBenchmarkProvenance::capture(
        args.to_vec(),
        "three_arm_per_operation_inside_existing_runtime_worker_sync_facade",
    );
    if !provenance
        .build
        .package_features
        .iter()
        .any(|feature| feature == "bridge-experiment")
    {
        provenance.add_validation_error(
            "bridge artifact was not built with the fsqlite-e2e bridge-experiment feature",
        );
    }
    let expected_affinity = std::env::var("FSQLITE_BENCH_EXPECTED_CPU_AFFINITY").ok();
    let max_load_average_1m = match bridge_max_load_average_1m() {
        Ok(maximum) => Some(maximum),
        Err(error) => {
            provenance.add_validation_error(error);
            None
        }
    };
    let environment = DetectedEnvironment::detect(&provenance);
    let host_state_before = capture_bridge_host_state();
    bridge_validate_host_state(
        &mut provenance,
        expected_affinity.as_deref(),
        max_load_average_1m,
        &host_state_before,
        "pre-measurement",
    );
    if !provenance.citable {
        if options.allow_unverified_provenance {
            provenance.mark_explicit_override();
        } else {
            return Err(format!(
                "refusing to run bridge experiment with invalid provenance:\n  - {}",
                provenance.validation_errors.join("\n  - ")
            ));
        }
    }
    eprintln!(
        "!!! NON-CITABLE BRIDGE DIAGNOSTIC ({}) — NUMBERS ARE NOT RELEASE EVIDENCE !!!",
        provenance.status
    );
    for error in &provenance.validation_errors {
        eprintln!("  provenance: {error}");
    }

    let (samples, host_state_checkpoints) =
        bridge_collect_samples(&runtime, options, &ready_operation_counts)?;
    for (index, checkpoint) in host_state_checkpoints.iter().enumerate() {
        let phase = format!("measurement checkpoint {index}");
        bridge_validate_host_state(
            &mut provenance,
            expected_affinity.as_deref(),
            max_load_average_1m,
            checkpoint,
            &phase,
        );
        bridge_validate_host_stability(&mut provenance, &host_state_before, checkpoint);
    }
    let host_state_after = capture_bridge_host_state();
    bridge_validate_host_state(
        &mut provenance,
        expected_affinity.as_deref(),
        max_load_average_1m,
        &host_state_after,
        "post-measurement",
    );
    bridge_validate_host_stability(&mut provenance, &host_state_before, &host_state_after);
    let statistics = bridge_arm_statistics(&samples);
    let comparisons = bridge_build_comparisons(
        &samples,
        &ready_operation_counts,
        options.bridge_operations,
        options.bridge_seed,
    )?;
    let ready_regression = bridge_ready_regression(
        &samples,
        ready_operation_counts.len().saturating_mul(2),
        options.bridge_seed ^ 0x4000,
    )?;

    provenance.verify_runtime_source_unchanged();
    if !provenance.citable {
        if options.allow_unverified_provenance {
            provenance.mark_explicit_override();
        } else {
            return Err(format!(
                "source provenance changed during bridge experiment; no artifact emitted:\n  - {}",
                provenance.validation_errors.join("\n  - ")
            ));
        }
    }
    eprintln!(
        "!!! FINAL BRIDGE STATUS: NON-CITABLE DIAGNOSTIC ({}) — NUMBERS ARE NOT RELEASE EVIDENCE !!!",
        provenance.status
    );
    for error in &provenance.validation_errors {
        eprintln!("  provenance: {error}");
    }

    let report = JsonBridgeReport {
        schema_version: BRIDGE_REPORT_SCHEMA_V2.to_owned(),
        generated_at_utc: chrono_stamp(),
        provenance,
        environment,
        host_state_before,
        host_state_checkpoints,
        host_state_after,
        config: JsonBridgeConfig {
            samples_per_arm: options.bridge_samples,
            raw_insert_operations: options.bridge_operations,
            ready_operation_counts,
            order_seed: options.bridge_seed,
            ordering_policy:
                "seeded balanced Latin/Williams order with reversed pairs for ready operation counts and full eight-block Williams-cycle bootstrap clusters at the fixed four-count matrix; randomized complementary ABBA/BAAB two-block clusters with two-block bootstrap resampling for retained-prepared two-arm self-carryover balance; complete randomized three-block rotation cycles with mirrored ABC-CBA sequences and three-block bootstrap clusters for exact three-arm position and within-block first-order carryover balance; transitions across complete design clusters are randomized but not asserted exactly balanced"
                    .to_owned(),
            warmup_policy:
                "thread-local runtime prewarmed once; every database sample warms its exact DML path before timing"
                    .to_owned(),
            timed_region:
                "per-operation arm times N complete thread-local Runtime::block_on entries; existing-runtime arm times N awaits inside an already-entered runtime; worker arm times N complete public facade calls; open, PRAGMAs, schema, transaction begin/commit, checksum, and close excluded"
                    .to_owned(),
            arm_contracts: BTreeMap::from([
                (
                    BridgeArm::PerOperationBlockOn.id().to_owned(),
                    "one reused thread-local asupersync Runtime::block_on entry/exit per timed operation"
                        .to_owned(),
                ),
                (
                    BridgeArm::SingleRuntimeEntry.id().to_owned(),
                    "one Runtime::block_on surrounds the whole sample outside its timer; timed operations are ordinary awaits inside the existing runtime"
                        .to_owned(),
                ),
                (
                    BridgeArm::WorkerSyncFacade.id().to_owned(),
                    "public AsyncConnection synchronous facade: each timed call clones SQL and parameters, allocates a response channel, crosses the worker channel, schedules the worker, and drives the engine future with futures-lite::block_on"
                    .to_owned(),
                ),
            ]),
            affinity_policy:
                "expected affinity must exactly match /proc/self/status, select one or two distinct physical cores on one NUMA node, exclude SMT siblings, and preserve topology throughout the run"
                    .to_owned(),
            max_load_average_1m,
        },
        raw_samples: samples,
        arm_statistics: statistics,
        paired_comparisons: comparisons,
        ready_runtime_entry_regression: ready_regression,
    };

    let json_path = options
        .json_out_path
        .clone()
        .or_else(|| {
            options
                .emit_timestamped_json
                .then(|| timestamp_filename("bridge_report", "json"))
        })
        .or_else(|| (!options.json_stdout).then(|| timestamp_filename("bridge_report", "json")));
    if let Some(path) = json_path.as_deref() {
        write_json_report(&report, path)?;
    }
    if options.json_stdout {
        print_json_report(&report);
    }
    Ok(())
}

#[cfg(not(feature = "bridge-experiment"))]
fn run_bridge_experiment(_args: &[String], _options: &CliOptions) -> Result<(), String> {
    Err("--bridge-experiment requires rebuilding with `--features bridge-experiment`".to_owned())
}

// ─── Main ──────────────────────────────────────────────────────────────

fn write_json_report<T: Serialize>(report: &T, path: &str) -> Result<(), String> {
    let json = serde_json::to_string_pretty(report)
        .map_err(|error| format!("could not serialize JSON report: {error}"))?;
    ensure_report_parent_dir(path, "JSON")?;
    std::fs::write(path, format!("{json}\n"))
        .map_err(|error| format!("could not write JSON report at {path}: {error}"))?;
    eprintln!("JSON report written to: {path}");
    Ok(())
}

fn ensure_report_parent_dir(path: &str, report_kind: &str) -> Result<(), String> {
    let path = std::path::Path::new(path);
    let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    else {
        return Ok(());
    };

    std::fs::create_dir_all(parent).map_err(|error| {
        format!(
            "could not create {report_kind} report parent directory {}: {error}",
            parent.display()
        )
    })
}

fn print_json_report<T: Serialize>(report: &T) {
    match serde_json::to_string_pretty(report) {
        Ok(json) => println!("{json}"),
        Err(err) => {
            eprintln!("ERROR: Could not serialize JSON report: {err}");
            std::process::exit(1);
        }
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.iter().any(|arg| arg == "--help" || arg == "-h") {
        print_usage();
        return;
    }

    let options = match parse_cli_args(&args) {
        Ok(options) => options,
        Err(err) => {
            eprintln!("ERROR: {err}");
            print_usage();
            std::process::exit(2);
        }
    };
    // Without `bridge-experiment` the inner branch diverges (exit), which
    // makes the `else` look redundant to clippy under that cfg only; with
    // the feature it does not diverge, so the shape must stay.
    #[allow(clippy::redundant_else)]
    if options.print_json_schema {
        if options.bridge_experiment {
            #[cfg(feature = "bridge-experiment")]
            print_bridge_json_schema();
            #[cfg(not(feature = "bridge-experiment"))]
            {
                eprintln!(
                    "ERROR: bridge schema requires rebuilding with `--features bridge-experiment`"
                );
                std::process::exit(1);
            }
        } else {
            print_benchmark_json_schema();
        }
        return;
    }
    if options.bridge_experiment {
        if let Err(error) = run_bridge_experiment(&args, &options) {
            eprintln!("ERROR: {error}");
            std::process::exit(1);
        }
        return;
    }

    let row_counts = if options.quick {
        ROW_COUNTS_QUICK
    } else {
        ROW_COUNTS
    };
    let html_file = if options.emit_html {
        Some(
            options
                .html_path
                .clone()
                .unwrap_or_else(|| timestamp_filename("benchmark_report", "html")),
        )
    } else {
        None
    };
    let json_file = if let Some(path) = options.json_out_path.clone() {
        Some(path)
    } else if options.emit_timestamped_json {
        Some(timestamp_filename("benchmark_report", "json"))
    } else {
        None
    };
    let artifact_requested = html_file.is_some() || json_file.is_some() || options.json_stdout;
    let mut provenance =
        JsonBenchmarkProvenance::capture(args.clone(), "scenario_scoped_thread_local_block_on");
    if artifact_requested && !provenance.citable {
        if options.allow_unverified_provenance {
            provenance.mark_explicit_override();
        } else {
            eprintln!(
                "ERROR: artifact emission requires explicit diagnostic authorization because provenance is non-citable:"
            );
            for error in &provenance.validation_errors {
                eprintln!("  - {error}");
            }
            eprintln!(
                "Use --allow-unverified-provenance only for an explicitly non-citable diagnostic run."
            );
            std::process::exit(2);
        }
    }
    let filter_lower = options.filter.as_ref().map(|filter| filter.to_lowercase());

    let should_run =
        |name: &str| -> bool { section_filter_matches(filter_lower.as_deref(), &[name]) };
    let should_run_any =
        |aliases: &[&str]| -> bool { section_filter_matches(filter_lower.as_deref(), aliases) };

    let bench_start = Instant::now();
    let environment = DetectedEnvironment::detect(&provenance);
    print_run_banner(
        !options.json_stdout,
        &options,
        row_counts,
        &environment,
        &provenance,
    );

    let mut report = BenchReport::new();
    let total_sections = 10;
    let mut section_num = 0;

    // Section 1: Insert by row count across record sizes.
    if should_run("insert") {
        section_num += 1;
        eprintln!("\n[{section_num}/{total_sections}] INSERT throughput by row count");
        for &record_size in RecordSize::ALL {
            bench_insert_by_row_count(&mut report, row_counts, record_size);
        }
    }

    // Section 2: Transaction strategy comparison.
    if should_run("txn") || should_run("transaction") || should_run("insert") {
        section_num += 1;
        eprintln!("\n[{section_num}/{total_sections}] Transaction strategy comparison");
        bench_insert_by_txn_strategy(&mut report, row_counts);
    }

    // Section 3: Record size comparison.
    if should_run("record") || should_run("size") || should_run("insert") {
        section_num += 1;
        eprintln!("\n[{section_num}/{total_sections}] Record size comparison");
        bench_insert_by_record_size(&mut report);
    }

    // Section 4: Concurrent writers.
    if should_run("concurrent") || should_run("writer") {
        section_num += 1;
        eprintln!("\n[{section_num}/{total_sections}] Concurrent writers");
        bench_concurrent_writers(&mut report);
    }

    // Section 5: Read-after-write.
    if should_run("read") || should_run("query") || should_run("select") {
        section_num += 1;
        eprintln!("\n[{section_num}/{total_sections}] Read-after-write query performance");
        bench_read_after_write(&mut report, row_counts);
    }

    // Section 6: Update/delete.
    if should_run_any(&[
        "update",
        "delete",
        "update-delete",
        "update-delete-throughput",
        "update-deletethroughput",
        "update/delete",
        "dml",
    ]) {
        section_num += 1;
        eprintln!("\n[{section_num}/{total_sections}] UPDATE/DELETE throughput");
        bench_update_delete(&mut report, row_counts);
    }

    // Section 7: Mixed OLTP.
    if should_run("oltp") || should_run("mixed") {
        section_num += 1;
        eprintln!("\n[{section_num}/{total_sections}] Mixed OLTP workload");
        bench_mixed_oltp(&mut report);
    }

    // Section 8: JOIN performance.
    if should_run("join") || should_run("query") || should_run("select") {
        section_num += 1;
        eprintln!("\n[{section_num}/{total_sections}] JOIN performance");
        bench_join_performance(&mut report, row_counts);
    }

    // Section 9: Subquery & CTE.
    if should_run("subquery") || should_run("cte") || should_run("query") {
        section_num += 1;
        eprintln!("\n[{section_num}/{total_sections}] Subquery & CTE performance");
        bench_subquery_cte(&mut report, row_counts);
    }

    // Section 10: String operations.
    if should_run("string") || should_run("like") || should_run("pattern") {
        section_num += 1;
        eprintln!("\n[{section_num}/{total_sections}] String & pattern matching");
        bench_string_operations(&mut report, row_counts);
    }

    let total_elapsed = bench_start.elapsed();
    eprintln!(
        "\nBenchmark complete in {:.1}s. Generating reports...",
        total_elapsed.as_secs_f64()
    );

    if !options.json_stdout {
        report.print(total_elapsed, &environment);
    }
    provenance.verify_runtime_source_unchanged();
    if artifact_requested && !provenance.citable {
        if options.allow_unverified_provenance {
            provenance.mark_explicit_override();
        } else {
            eprintln!(
                "ERROR: source provenance changed during the benchmark; no artifact emitted:"
            );
            for error in &provenance.validation_errors {
                eprintln!("  - {error}");
            }
            std::process::exit(2);
        }
    }

    if let Some(path) = html_file.as_deref()
        && let Err(error) = report.write_html(path, &provenance)
    {
        eprintln!("ERROR: {error}");
        std::process::exit(1);
    }

    if json_file.is_some() || options.json_stdout {
        let json_report = build_json_report(
            &report,
            total_elapsed,
            JsonRunConfig {
                quick: options.quick,
                filter: options.filter.clone(),
                warmup_iterations: WARMUP_ITERS,
                min_iterations: MIN_ITERS,
                max_iterations: MAX_ITERS,
                target_duration_secs: TARGET_DURATION.as_secs(),
                row_counts: row_counts.to_vec(),
                html_output_path: html_file.clone(),
                json_output_path: json_file.clone(),
                json_stdout: options.json_stdout,
            },
            environment.clone(),
            provenance,
        );

        if let Some(path) = json_file.as_deref()
            && let Err(error) = write_json_report(&json_report, path)
        {
            eprintln!("ERROR: {error}");
            std::process::exit(1);
        }
        if options.json_stdout {
            print_json_report(&json_report);
        }
    }
}
