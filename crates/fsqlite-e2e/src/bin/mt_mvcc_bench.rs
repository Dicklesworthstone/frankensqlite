//! `mt-mvcc-bench` — real multi-threaded MVCC writer benchmark (IMPL-4a).
//!
//! Why this exists: it is the standalone scale harness for real multi-threaded
//! MVCC writer runs. It spawns N OS threads, each with its OWN
//! `Connection::open(path)` against the SAME shared file-backed database, so
//! the MVCC page-lock table, commit coordinator, and SSI validator are
//! exercised under real contention. The comprehensive benchmark now uses the
//! same one-connection-per-thread shape for its full-matrix concurrent rows;
//! this binary adds 16-thread coverage, separate-table mode, startup
//! diagnostics, and pass-over-pass history gates.
//!
//! For each thread count we measure:
//!   - FrankenSQLite file-backed database, one Connection per thread,
//!     `PRAGMA fsqlite.concurrent_mode=ON` + `BEGIN CONCURRENT`.
//!   - C SQLite (rusqlite) file-backed WAL, one Connection per thread,
//!     `journal_mode=WAL`, `synchronous=NORMAL`, `busy_timeout=5000`.
//!
//! By default, each thread inserts `--rows-per-thread` rows into the shared
//! table `bench(id INTEGER PRIMARY KEY, payload TEXT)` using disjoint rowid
//! ranges (`thread_id * 1_000_000 + i`) so there are no logical row conflicts
//! — only page-level contention on the table's btree. `--separate-tables`
//! gives each worker its own `bench_N` table to measure the no-shared-btree
//! concurrent writer shape.
//!
//! Output is a tab-separated table suitable for grepping / redirection:
//!
//! ```text
//! threads | fsqlite_wps | sqlite_wps | throughput_ratio | fsqlite_ms_p50 | ...
//!       1 | 12345       | 23456      | 0.53x            | 81.00          | ...
//! ```
//!
//! `throughput_ratio = fsqlite_wps / sqlite_wps`. Values above 1.0x mean
//! FrankenSQLite is faster than C SQLite WAL under equal multi-threaded load.
//! `time_ratio = fsqlite_batch_ms / sqlite_batch_ms`; lower is better.
//!
//! ## CLI
//!
//! ```text
//! mt-mvcc-bench [--rows-per-thread=1000] [--threads=1,2,4,8,16] [--iters=3]
//! [--json-output=PATH] [--summary-md=PATH]
//! [--separate-tables]
//! ```
//!
//! ## Caveats
//!
//! * `BEGIN CONCURRENT` requires `PRAGMA fsqlite.concurrent_mode=ON;` to be
//!   set on each per-thread connection (see
//!   `crates/fsqlite-harness/tests/bd_3plop_4_lock_contention_storms.rs`).
//!   If that PRAGMA fails on a given build, we fall back to plain `BEGIN`
//!   and print a warning (honest measurement over a fake win).
//! * We retry transient errors (`FrankenError::is_transient()`) by rolling back
//!   and reopening the whole transaction, up to `MAX_RETRIES`; hard row-level
//!   failures are counted in `failed_rows` and included in the report so you
//!   can tell when the numbers are bogus.
//! * Each iteration creates a fresh tempfile so there's no state carried
//!   across runs. `--iters=3` reports p50/p95/p99 across those 3 samples.

use std::collections::BTreeMap;
use std::sync::{Arc, Barrier, Condvar, Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant};
use std::{
    fmt::{Display, Write as _},
    fs,
    path::Path,
    path::PathBuf,
};

use serde::{Deserialize, Serialize};

// ─── Defaults ─────────────────────────────────────────────────────────────

const DEFAULT_ROWS_PER_THREAD: usize = 1_000;
const DEFAULT_THREADS: &[usize] = &[1, 2, 4, 8, 16];
const DEFAULT_ITERS: usize = 3;
const DEFAULT_HISTORY_JSON: &str = ".bench-history/mt-mvcc-bench.latest.json";
const DEFAULT_SEPARATE_TABLES_HISTORY_JSON: &str =
    ".bench-history/mt-mvcc-bench.separate-tables.latest.json";
const ROWID_BASE_STRIDE: i64 = 1_000_000;
const MAX_RETRIES: usize = 512;
const RETRY_SLEEP_MS: u64 = 1;
const MAX_RETRY_SLEEP_MS: u64 = 25;
const FSQLITE_RETRY_TIMEOUT: Duration = Duration::from_secs(5);
const SHARED_INSERT_SQL: &str = "INSERT INTO bench (id, payload) VALUES (?1, ?2)";
const STARTUP_COORDINATION_TIMEOUT: Duration = Duration::from_secs(5);
const PASS_OVER_PASS_SCHEMA_V1: &str = "fsqlite-e2e.mt_mvcc_bench.pass_over_pass.v1";
const PASS_OVER_PASS_MAX_RATIO_DROP_PCT: f64 = 5.0;
const REPORT_SCHEMA_V3: &str = "fsqlite-e2e.mt_mvcc_bench_report.v3";

// ─── CLI parsing (manual — no clap in workspace) ─────────────────────────

#[derive(Debug, Clone)]
struct Options {
    rows_per_thread: usize,
    threads: Vec<usize>,
    iters: usize,
    json_output: Option<PathBuf>,
    summary_md: Option<PathBuf>,
    history_json: PathBuf,
    apples_to_apples: bool,
    separate_tables: bool,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            rows_per_thread: DEFAULT_ROWS_PER_THREAD,
            threads: DEFAULT_THREADS.to_vec(),
            iters: DEFAULT_ITERS,
            json_output: None,
            summary_md: None,
            history_json: PathBuf::from(DEFAULT_HISTORY_JSON),
            apples_to_apples: false,
            separate_tables: false,
        }
    }
}

fn print_usage_and_exit(code: i32) -> ! {
    eprintln!(
        "usage: mt-mvcc-bench [--rows-per-thread=N] [--threads=N,N,...] [--iters=N] \\\n\
         [--json-output=PATH] [--summary-md=PATH] [--history-json=PATH] [--apples-to-apples] \\\n\
         [--separate-tables]\n\
         \n\
         defaults: --rows-per-thread={DEFAULT_ROWS_PER_THREAD} \
         --threads=1,2,4,8,16 --iters={DEFAULT_ITERS}\n\
         note: --apples-to-apples is a compatibility flag; this benchmark already\n\
         uses the prepared-statement/file-backed/shared-db path on both engines.\n\
         note: --rows-per-thread=0 reduces the run to shared-file worker open + synchronized start,\n\
         which is the minimal repro for the 13+ thread startup-open failure."
    );
    std::process::exit(code);
}

fn print_usage_error(message: impl Display) -> ! {
    eprintln!("{message}");
    print_usage_and_exit(2);
}

fn parse_thread_count(raw: &str) -> Result<usize, String> {
    let threads = raw
        .trim()
        .parse::<usize>()
        .map_err(|_| format!("invalid thread count in --threads: {raw}"))?;
    if threads == 0 {
        return Err("--threads values must be >= 1".to_owned());
    }
    Ok(threads)
}

fn parse_args() -> Options {
    let mut opts = Options::default();
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--apples-to-apples" {
            opts.apples_to_apples = true;
            continue;
        }
        if arg == "--separate-tables" {
            opts.separate_tables = true;
            continue;
        }
        let (key, val) = if let Some(eq) = arg.find('=') {
            (arg[..eq].to_owned(), arg[eq + 1..].to_owned())
        } else if arg == "--help" || arg == "-h" {
            print_usage_and_exit(0);
        } else {
            // Support space-separated form.
            let v = args.next().unwrap_or_else(|| {
                print_usage_error(format!("missing value for argument `{arg}`"))
            });
            (arg, v)
        };
        match key.as_str() {
            "--rows-per-thread" => {
                opts.rows_per_thread = val.parse().unwrap_or_else(|_| {
                    print_usage_error(format!("invalid --rows-per-thread: {val}"))
                });
            }
            "--threads" => {
                opts.threads = val
                    .split(',')
                    .map(|s| {
                        parse_thread_count(s).unwrap_or_else(|message| print_usage_error(message))
                    })
                    .collect();
                if opts.threads.is_empty() {
                    print_usage_error("--threads must contain at least one value");
                }
            }
            "--iters" => {
                opts.iters = val
                    .parse()
                    .unwrap_or_else(|_| print_usage_error(format!("invalid --iters: {val}")));
                if opts.iters == 0 {
                    print_usage_error("--iters must be >= 1");
                }
            }
            "--json-output" => {
                opts.json_output = Some(PathBuf::from(val));
            }
            "--summary-md" => {
                opts.summary_md = Some(PathBuf::from(val));
            }
            "--history-json" => {
                opts.history_json = PathBuf::from(val);
            }
            other => {
                eprintln!("unknown argument: {other}");
                print_usage_and_exit(2);
            }
        }
    }
    if opts.separate_tables && opts.history_json == Path::new(DEFAULT_HISTORY_JSON) {
        opts.history_json = PathBuf::from(DEFAULT_SEPARATE_TABLES_HISTORY_JSON);
    }
    opts
}

// ─── Reported per-config result ───────────────────────────────────────────

#[derive(Debug, Clone, Copy)]
struct RunResult {
    /// Wall-clock duration across threads (max of per-thread times), best of
    /// `iters` iterations.
    best_elapsed: Duration,
    /// Total rows written (across all threads) in the best iteration.
    total_rows: usize,
    /// Total rows that hit a hard failure after `MAX_RETRIES`.
    failed_rows: usize,
}

impl RunResult {
    fn writes_per_sec(&self) -> f64 {
        let secs = self.best_elapsed.as_secs_f64();
        if secs <= 0.0 {
            0.0
        } else {
            #[allow(clippy::cast_precision_loss)]
            let n = self.total_rows as f64;
            n / secs
        }
    }

    fn elapsed_ms(&self) -> f64 {
        self.best_elapsed.as_secs_f64() * 1_000.0
    }
}

#[derive(Debug, Clone)]
struct RunStats {
    samples: Vec<RunResult>,
}

impl RunStats {
    fn new(samples: Vec<RunResult>) -> Self {
        Self { samples }
    }

    fn total_failed_rows(&self) -> usize {
        self.samples.iter().map(|sample| sample.failed_rows).sum()
    }

    fn p50_writes_per_sec(&self) -> f64 {
        self.percentile_by(RunResult::writes_per_sec, 0.50)
    }

    fn p95_writes_per_sec(&self) -> f64 {
        self.percentile_by(RunResult::writes_per_sec, 0.95)
    }

    fn p99_writes_per_sec(&self) -> f64 {
        self.percentile_by(RunResult::writes_per_sec, 0.99)
    }

    fn p50_elapsed_ms(&self) -> f64 {
        self.percentile_by(RunResult::elapsed_ms, 0.50)
    }

    fn p95_elapsed_ms(&self) -> f64 {
        self.percentile_by(RunResult::elapsed_ms, 0.95)
    }

    fn p99_elapsed_ms(&self) -> f64 {
        self.percentile_by(RunResult::elapsed_ms, 0.99)
    }

    fn percentile_by(&self, value: fn(&RunResult) -> f64, percentile: f64) -> f64 {
        let values = self.samples.iter().map(value).collect();
        percentile_value(values, percentile)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct ThreadComparisonReport {
    threads: usize,
    fsqlite_wps_p50: f64,
    fsqlite_wps_p95: f64,
    fsqlite_wps_p99: f64,
    sqlite_wps_p50: f64,
    sqlite_wps_p95: f64,
    sqlite_wps_p99: f64,
    throughput_ratio: f64,
    fsqlite_ms_p50: f64,
    fsqlite_ms_p95: f64,
    fsqlite_ms_p99: f64,
    sqlite_ms_p50: f64,
    sqlite_ms_p95: f64,
    sqlite_ms_p99: f64,
    time_ratio: f64,
    fsqlite_failed_rows: usize,
    sqlite_failed_rows: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
struct MtMvccBenchReport {
    schema_version: &'static str,
    workload_shape: &'static str,
    rows_per_thread: usize,
    iterations: usize,
    thread_results: Vec<ThreadComparisonReport>,
    pass_over_pass_gate: PassOverPassGateReport,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
struct PassOverPassGateReport {
    schema_version: &'static str,
    history_json_path: String,
    threshold_ratio_drop_pct: f64,
    status: &'static str,
    previous_report_found: bool,
    regressions: Vec<RatioRegression>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
struct RatioRegression {
    threads: usize,
    previous_ratio: f64,
    current_ratio: f64,
    ratio_drop_pct: f64,
}

#[derive(Debug, Clone, Deserialize)]
struct HistoricalMtMvccBenchReport {
    workload_shape: Option<String>,
    rows_per_thread: Option<usize>,
    thread_results: Vec<ThreadComparisonReport>,
}

#[derive(Debug, Clone)]
struct FsqliteRetryBudget {
    attempts: usize,
    started: Instant,
}

impl FsqliteRetryBudget {
    fn new() -> Self {
        Self {
            attempts: 0,
            started: Instant::now(),
        }
    }

    const fn attempts(&self) -> usize {
        self.attempts
    }

    fn next_wait(&mut self, tid: usize) -> Option<Duration> {
        if self.attempts >= MAX_RETRIES || self.started.elapsed() >= FSQLITE_RETRY_TIMEOUT {
            return None;
        }
        self.attempts += 1;
        let exp_shift = (self.attempts / 8).min(5);
        let base_ms = RETRY_SLEEP_MS
            .saturating_mul(1_u64 << exp_shift)
            .min(MAX_RETRY_SLEEP_MS);
        let jitter_ms = ((tid as u64).wrapping_mul(7) + (self.attempts as u64).wrapping_mul(3)) % 5;
        Some(Duration::from_millis(base_ms.saturating_add(jitter_ms)))
    }
}

#[derive(Debug, Clone)]
struct StartupFailure {
    tid: usize,
    error: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StartupResultKind {
    Ready,
    Failed,
}

#[derive(Debug, Clone)]
struct StartupOutcome {
    tid: usize,
    kind: StartupResultKind,
    error: Option<String>,
}

#[derive(Debug, Default)]
struct StartupGateState {
    release: bool,
    abort: bool,
}

fn build_thread_report(
    threads: usize,
    fsqlite: &RunStats,
    sqlite: &RunStats,
) -> ThreadComparisonReport {
    let fsqlite_wps_p50 = fsqlite.p50_writes_per_sec();
    let sqlite_wps_p50 = sqlite.p50_writes_per_sec();
    let throughput_ratio = if sqlite_wps_p50 > 0.0 {
        fsqlite_wps_p50 / sqlite_wps_p50
    } else {
        0.0
    };
    let fsqlite_ms_p50 = fsqlite.p50_elapsed_ms();
    let sqlite_ms_p50 = sqlite.p50_elapsed_ms();
    let time_ratio = if sqlite_ms_p50 > 0.0 {
        fsqlite_ms_p50 / sqlite_ms_p50
    } else {
        0.0
    };

    ThreadComparisonReport {
        threads,
        fsqlite_wps_p50,
        fsqlite_wps_p95: fsqlite.p95_writes_per_sec(),
        fsqlite_wps_p99: fsqlite.p99_writes_per_sec(),
        sqlite_wps_p50,
        sqlite_wps_p95: sqlite.p95_writes_per_sec(),
        sqlite_wps_p99: sqlite.p99_writes_per_sec(),
        throughput_ratio,
        fsqlite_ms_p50,
        fsqlite_ms_p95: fsqlite.p95_elapsed_ms(),
        fsqlite_ms_p99: fsqlite.p99_elapsed_ms(),
        sqlite_ms_p50,
        sqlite_ms_p95: sqlite.p95_elapsed_ms(),
        sqlite_ms_p99: sqlite.p99_elapsed_ms(),
        time_ratio,
        fsqlite_failed_rows: fsqlite.total_failed_rows(),
        sqlite_failed_rows: sqlite.total_failed_rows(),
    }
}

fn ensure_parent_dir(path: &Path) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|error| format!("create parent directory {}: {error}", parent.display()))?;
    }
    Ok(())
}

fn render_markdown_summary(report: &MtMvccBenchReport) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "# mt-mvcc-bench Summary\n");
    let _ = writeln!(out, "- Workload shape: `{}`", report.workload_shape);
    let _ = writeln!(out, "- Rows per thread: `{}`", report.rows_per_thread);
    let _ = writeln!(out, "- Iterations: `{}`", report.iterations);
    let _ = writeln!(out, "- Schema: `{}`\n", report.schema_version);
    let gate = &report.pass_over_pass_gate;
    let _ = writeln!(
        out,
        "- Pass-over-pass gate: `{}` (threshold `{:.2}%`, history `{}`)",
        gate.status, gate.threshold_ratio_drop_pct, gate.history_json_path
    );
    if !gate.regressions.is_empty() {
        let _ = writeln!(out, "- Regressions:");
        for regression in &gate.regressions {
            let _ = writeln!(
                out,
                "  - {} threads: {:.2}x -> {:.2}x ({:.2}% drop)",
                regression.threads,
                regression.previous_ratio,
                regression.current_ratio,
                regression.ratio_drop_pct
            );
        }
    }
    let _ = writeln!(out);
    let _ = writeln!(
        out,
        "| Threads | fsqlite p50 wps | sqlite p50 wps | Throughput ratio | fsqlite p50 ms | sqlite p50 ms | Time ratio | fsqlite failed | sqlite failed |"
    );
    let _ = writeln!(
        out,
        "|---------|-----------------:|---------------:|-----------------:|---------------:|--------------:|-----------:|---------------:|--------------:|"
    );
    for row in &report.thread_results {
        let _ = writeln!(
            out,
            "| {} | {:.0} | {:.0} | {:.2}x | {:.2} | {:.2} | {:.2}x | {} | {} |",
            row.threads,
            row.fsqlite_wps_p50,
            row.sqlite_wps_p50,
            row.throughput_ratio,
            row.fsqlite_ms_p50,
            row.sqlite_ms_p50,
            row.time_ratio,
            row.fsqlite_failed_rows,
            row.sqlite_failed_rows
        );
    }
    out
}

fn write_json_report(path: &Path, report: &MtMvccBenchReport) -> Result<(), String> {
    ensure_parent_dir(path)?;
    let json = serde_json::to_vec_pretty(report)
        .map_err(|error| format!("serialize mt-mvcc bench report: {error}"))?;
    fs::write(path, json).map_err(|error| format!("write json report {}: {error}", path.display()))
}

fn write_markdown_summary(path: &Path, report: &MtMvccBenchReport) -> Result<(), String> {
    ensure_parent_dir(path)?;
    fs::write(path, render_markdown_summary(report))
        .map_err(|error| format!("write markdown summary {}: {error}", path.display()))
}

fn load_previous_report(path: &Path) -> Result<Option<HistoricalMtMvccBenchReport>, String> {
    if !path.exists() {
        return Ok(None);
    }
    let bytes = fs::read(path)
        .map_err(|error| format!("read history report {}: {error}", path.display()))?;
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|error| format!("parse history report {}: {error}", path.display()))
}

fn build_pass_over_pass_gate(
    history_json: &Path,
    previous: Option<&HistoricalMtMvccBenchReport>,
    current_rows: &[ThreadComparisonReport],
    current_workload_shape: &str,
    current_rows_per_thread: usize,
) -> PassOverPassGateReport {
    let previous = previous.filter(|previous| {
        previous
            .workload_shape
            .as_deref()
            .is_some_and(|shape| shape == current_workload_shape)
            && previous.rows_per_thread == Some(current_rows_per_thread)
    });
    let regressions = previous
        .map(|previous| {
            let previous_by_threads: BTreeMap<usize, f64> = previous
                .thread_results
                .iter()
                .map(|row| (row.threads, row.throughput_ratio))
                .collect();
            current_rows
                .iter()
                .filter_map(|row| {
                    let previous_ratio = *previous_by_threads.get(&row.threads)?;
                    if previous_ratio <= 0.0 || row.throughput_ratio >= previous_ratio {
                        return None;
                    }
                    let ratio_drop_pct =
                        ((previous_ratio - row.throughput_ratio) / previous_ratio) * 100.0;
                    (ratio_drop_pct > PASS_OVER_PASS_MAX_RATIO_DROP_PCT).then_some(
                        RatioRegression {
                            threads: row.threads,
                            previous_ratio,
                            current_ratio: row.throughput_ratio,
                            ratio_drop_pct,
                        },
                    )
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let status = if previous.is_none() {
        "no_prior_report"
    } else if regressions.is_empty() {
        "passed"
    } else {
        "failed"
    };
    PassOverPassGateReport {
        schema_version: PASS_OVER_PASS_SCHEMA_V1,
        history_json_path: history_json.display().to_string(),
        threshold_ratio_drop_pct: PASS_OVER_PASS_MAX_RATIO_DROP_PCT,
        status,
        previous_report_found: previous.is_some(),
        regressions,
    }
}

fn format_startup_failures(label: &str, failures: &[StartupFailure]) -> String {
    let details = failures
        .iter()
        .map(|failure| format!("t{}={}", failure.tid, failure.error))
        .collect::<Vec<_>>()
        .join(", ");
    format!("{label} startup failed before synchronized start: {details}")
}

fn workload_shape(separate_tables: bool) -> &'static str {
    if separate_tables {
        "separate_tables"
    } else {
        "shared_table"
    }
}

fn worker_table_count(threads: usize, separate_tables: bool) -> usize {
    if separate_tables { threads } else { 1 }
}

fn worker_table_name(tid: usize, separate_tables: bool) -> String {
    if separate_tables {
        format!("bench_{tid}")
    } else {
        "bench".to_owned()
    }
}

fn create_table_sql(table_name: &str) -> String {
    format!("CREATE TABLE IF NOT EXISTS {table_name} (id INTEGER PRIMARY KEY, payload TEXT);")
}

fn create_tables_sql(threads: usize, separate_tables: bool) -> String {
    let table_count = worker_table_count(threads, separate_tables);
    let mut sql = String::new();
    for tid in 0..table_count {
        let table_name = worker_table_name(tid, separate_tables);
        sql.push_str(&create_table_sql(&table_name));
    }
    sql
}

fn worker_insert_sql(tid: usize, separate_tables: bool) -> String {
    // SQL identifiers cannot be bound as parameters. The only dynamic
    // identifier here is generated from the zero-based worker index, not from
    // user text, so it cannot escape the `bench_N` table-name shape.
    if separate_tables {
        format!("INSERT INTO bench_{tid} (id, payload) VALUES (?1, ?2)")
    } else {
        SHARED_INSERT_SQL.to_owned()
    }
}

fn prepare_fsqlite_schema(path: &str, threads: usize, separate_tables: bool) -> Result<(), String> {
    let conn = fsqlite_e2e::block_on(fsqlite::Connection::open(path.to_owned()))
        .map_err(|error| format!("fsqlite open (init): {error}"))?;
    for pragma in [
        "PRAGMA page_size=4096;",
        "PRAGMA journal_mode=WAL;",
        "PRAGMA synchronous=NORMAL;",
        "PRAGMA cache_size=-64000;",
    ] {
        let _ = fsqlite_e2e::block_on(conn.execute(pragma));
    }
    for tid in 0..worker_table_count(threads, separate_tables) {
        let table_name = worker_table_name(tid, separate_tables);
        let create_sql = create_table_sql(&table_name);
        fsqlite_e2e::block_on(conn.execute(&create_sql))
            .map_err(|error| format!("create table {table_name}: {error}"))?;
    }
    Ok(())
}

#[allow(clippy::cast_precision_loss)]
fn percentile_value(mut values: Vec<f64>, percentile: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    values.sort_by(f64::total_cmp);
    if values.len() == 1 {
        return values[0];
    }
    let rank = percentile.clamp(0.0, 1.0) * (values.len() - 1) as f64;
    let lower = rank.floor() as usize;
    let upper = rank.ceil() as usize;
    if lower == upper {
        return values[lower];
    }
    let fraction = rank - lower as f64;
    (values[upper] - values[lower]).mul_add(fraction, values[lower])
}

// ─── FrankenSQLite workload ──────────────────────────────────────────────

fn open_fsqlite_worker(path: &str) -> Result<(fsqlite::Connection, bool), String> {
    let conn = fsqlite_e2e::block_on(fsqlite::Connection::open(path.to_owned()))
        .map_err(|error| format!("fsqlite open (worker): {error}"))?;
    // `synchronous` is PER-CONNECTION: `prepare_fsqlite_schema`'s NORMAL does not
    // carry to worker connections, so state it here. FrankenSQLite's default is
    // already NORMAL (`WalCommitSyncPolicy::Deferred`) and the rusqlite worker
    // sets NORMAL explicitly, so this is a no-op today — it pins the matched
    // durability the published concurrent-writer numbers depend on, rather than
    // leaving it to agree with C SQLite by coincidence of defaults. The same
    // omission on the C side of `comprehensive_bench::bench_concurrent_writers`
    // silently compared C-FULL against F-NORMAL for the life of that section
    // (bd-x5gzk); see docs/bench-methodology-concurrent-writers.md.
    let _ = fsqlite_e2e::block_on(conn.execute("PRAGMA synchronous=NORMAL;"));
    let concurrent_ok =
        fsqlite_e2e::block_on(conn.execute("PRAGMA fsqlite.concurrent_mode=ON;")).is_ok();
    let _ = fsqlite_e2e::block_on(conn.execute("PRAGMA busy_timeout=5000;"));
    Ok((conn, concurrent_ok))
}

fn run_fsqlite(
    threads: usize,
    rows_per_thread: usize,
    separate_tables: bool,
) -> Result<RunResult, String> {
    let tmp = tempfile::NamedTempFile::new().expect("tempfile");
    let path = tmp
        .path()
        .to_str()
        .expect("tempfile path is utf-8")
        .to_owned();

    prepare_fsqlite_schema(&path, threads, separate_tables)?;

    let path = Arc::new(path);
    let barrier = Arc::new(Barrier::new(threads));
    let startup_gate = Arc::new((Mutex::new(StartupGateState::default()), Condvar::new()));
    let (startup_tx, startup_rx) = mpsc::channel::<StartupOutcome>();
    let mut handles = Vec::with_capacity(threads);

    let t0 = Instant::now();
    for tid in 0..threads {
        let path = Arc::clone(&path);
        let barrier = Arc::clone(&barrier);
        let startup_gate = Arc::clone(&startup_gate);
        let startup_tx = startup_tx.clone();
        let handle = thread::spawn(move || -> Result<(Duration, usize), String> {
            // Each thread owns its own Connection (Connection: !Send + !Sync).
            let (conn, concurrent_ok) = match open_fsqlite_worker(path.as_str()) {
                Ok(worker) => {
                    let _ = startup_tx.send(StartupOutcome {
                        tid,
                        kind: StartupResultKind::Ready,
                        error: None,
                    });
                    worker
                }
                Err(error) => {
                    let _ = startup_tx.send(StartupOutcome {
                        tid,
                        kind: StartupResultKind::Failed,
                        error: Some(error.clone()),
                    });
                    return Err(error);
                }
            };

            let (gate_lock, gate_cv) = &*startup_gate;
            let mut gate_state = gate_lock
                .lock()
                .map_err(|_| "fsqlite startup gate poisoned".to_owned())?;
            while !gate_state.release && !gate_state.abort {
                gate_state = gate_cv
                    .wait(gate_state)
                    .map_err(|_| "fsqlite startup gate poisoned while waiting".to_owned())?;
            }
            if gate_state.abort {
                return Err(format!(
                    "fsqlite t{tid} startup aborted after peer open failure"
                ));
            }
            drop(gate_state);

            barrier.wait();
            let start = Instant::now();

            #[allow(clippy::cast_possible_wrap)]
            let base = if separate_tables {
                0
            } else {
                tid as i64 * ROWID_BASE_STRIDE
            };
            let mut failed = 0usize;

            // Prepare the INSERT once per transaction attempt; bind params per
            // iteration. This matches the rusqlite reference loop (L412-446
            // below) so both sides parse+plan the insert a single time and
            // the per-row cost is just bind + execute.
            //
            // Using `format!` per-iter on the fsqlite side was an
            // apples-to-oranges artifact that pinned `Lexer::tokenize_into`
            // at 2.53% self-time and drove 12%+ allocator churn on MT 8t
            // (2026-04-23 capture `fsqlite-t3b-validation-185110`).
            let insert_sql = worker_insert_sql(tid, separate_tables);

            // Single transaction spanning all rows; retry on transient
            // conflicts by rolling back and reopening the transaction.
            let mut retry_budget = FsqliteRetryBudget::new();
            'outer: loop {
                let begin_sql = if concurrent_ok {
                    "BEGIN CONCURRENT"
                } else {
                    "BEGIN"
                };
                if let Err(e) = fsqlite_e2e::block_on(conn.execute(begin_sql)) {
                    if e.is_transient()
                        && let Some(wait) = retry_budget.next_wait(tid)
                    {
                        thread::sleep(wait);
                        continue;
                    }
                    return Err(format!(
                        "[fsqlite t{tid}] BEGIN failed after {} retries: {e}",
                        retry_budget.attempts()
                    ));
                }

                let stmt = match fsqlite_e2e::block_on(conn.prepare(&insert_sql)) {
                    Ok(s) => s,
                    Err(e) => {
                        let _ = fsqlite_e2e::block_on(conn.execute("ROLLBACK"));
                        return Err(format!("[fsqlite t{tid}] prepare failed: {e}"));
                    }
                };

                #[allow(clippy::cast_possible_wrap)]
                for i in 0..rows_per_thread as i64 {
                    let id = base + i;
                    let payload = format!("tid{tid}_i{i}");
                    let params = [
                        fsqlite::SqliteValue::Integer(id),
                        fsqlite::SqliteValue::Text(payload.into()),
                    ];
                    match fsqlite_e2e::block_on(stmt.execute_with_params(&params)) {
                        Ok(_) => {}
                        Err(e) if e.is_transient() => {
                            let _ = fsqlite_e2e::block_on(conn.execute("ROLLBACK"));
                            if let Some(wait) = retry_budget.next_wait(tid) {
                                thread::sleep(wait);
                                continue 'outer;
                            }
                            return Err(format!(
                                "[fsqlite t{tid}] INSERT {id} exhausted retry budget after {} retries: {e}",
                                retry_budget.attempts()
                            ));
                        }
                        Err(e) => {
                            eprintln!("[fsqlite t{tid}] INSERT {id} failed: {e}");
                            failed += 1;
                        }
                    }
                }

                match fsqlite_e2e::block_on(conn.execute("COMMIT")) {
                    Ok(_) => break 'outer,
                    Err(e) if e.is_transient() => {
                        let _ = fsqlite_e2e::block_on(conn.execute("ROLLBACK"));
                        if let Some(wait) = retry_budget.next_wait(tid) {
                            thread::sleep(wait);
                            continue;
                        }
                        return Err(format!(
                            "[fsqlite t{tid}] COMMIT exhausted retry budget after {} retries: {e}",
                            retry_budget.attempts()
                        ));
                    }
                    Err(e) => {
                        let _ = fsqlite_e2e::block_on(conn.execute("ROLLBACK"));
                        return Err(format!("[fsqlite t{tid}] COMMIT failed: {e}"));
                    }
                }
            }

            Ok((start.elapsed(), failed))
        });
        handles.push(handle);
    }
    drop(startup_tx);

    let mut startup_failures = Vec::new();
    for _ in 0..threads {
        let outcome = startup_rx
            .recv_timeout(STARTUP_COORDINATION_TIMEOUT)
            .map_err(|error| {
                format!(
                    "fsqlite startup coordination timed out after {:?}: {error}",
                    STARTUP_COORDINATION_TIMEOUT
                )
            })?;
        if outcome.kind == StartupResultKind::Failed {
            startup_failures.push(StartupFailure {
                tid: outcome.tid,
                error: outcome
                    .error
                    .unwrap_or_else(|| "unknown startup failure".to_owned()),
            });
        }
    }

    {
        let (gate_lock, gate_cv) = &*startup_gate;
        let mut gate_state = gate_lock
            .lock()
            .map_err(|_| "fsqlite startup gate poisoned".to_owned())?;
        gate_state.release = startup_failures.is_empty();
        gate_state.abort = !startup_failures.is_empty();
        gate_cv.notify_all();
    }

    if !startup_failures.is_empty() {
        for handle in handles {
            let _ = handle.join();
        }
        return Err(format_startup_failures("fsqlite", &startup_failures));
    }

    let mut total_failed = 0usize;
    for (tid, h) in handles.into_iter().enumerate() {
        let (_d, failed) = h
            .join()
            .map_err(|_| format!("fsqlite worker t{tid} panicked"))??;
        total_failed += failed;
    }
    let elapsed = t0.elapsed();

    Ok(RunResult {
        best_elapsed: elapsed,
        total_rows: threads * rows_per_thread,
        failed_rows: total_failed,
    })
}

// ─── C SQLite (rusqlite) workload ────────────────────────────────────────

fn run_rusqlite(threads: usize, rows_per_thread: usize, separate_tables: bool) -> RunResult {
    let tmp = tempfile::NamedTempFile::new().expect("tempfile");
    let path = tmp
        .path()
        .to_str()
        .expect("tempfile path is utf-8")
        .to_owned();

    {
        let conn = rusqlite::Connection::open(&path).expect("rusqlite open (init)");
        let mut schema_sql = "PRAGMA page_size=4096;\
             PRAGMA journal_mode=WAL;\
             PRAGMA synchronous=NORMAL;\
             PRAGMA cache_size=-64000;"
            .to_owned();
        schema_sql.push_str(&create_tables_sql(threads, separate_tables));
        conn.execute_batch(&schema_sql).expect("init schema");
    }

    let path = Arc::new(path);
    let barrier = Arc::new(Barrier::new(threads));
    let mut handles = Vec::with_capacity(threads);

    let t0 = Instant::now();
    for tid in 0..threads {
        let path = Arc::clone(&path);
        let barrier = Arc::clone(&barrier);
        let handle = thread::spawn(move || -> usize {
            use rusqlite::OpenFlags;
            let flags = OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_NO_MUTEX;
            let conn = rusqlite::Connection::open_with_flags(path.as_str(), flags)
                .expect("rusqlite open (worker)");
            conn.execute_batch(
                "PRAGMA journal_mode=WAL;\
                 PRAGMA synchronous=NORMAL;\
                 PRAGMA busy_timeout=5000;",
            )
            .expect("worker pragmas");

            barrier.wait();

            #[allow(clippy::cast_possible_wrap)]
            let base = if separate_tables {
                0
            } else {
                tid as i64 * ROWID_BASE_STRIDE
            };
            let mut failed = 0usize;
            let insert_sql = worker_insert_sql(tid, separate_tables);

            conn.execute_batch("BEGIN").expect("BEGIN");
            {
                let mut stmt = conn.prepare(&insert_sql).expect("prepare");
                #[allow(clippy::cast_possible_wrap)]
                for i in 0..rows_per_thread as i64 {
                    let id = base + i;
                    let payload = format!("tid{tid}_i{i}");
                    let mut retry = 0usize;
                    loop {
                        match stmt.execute(rusqlite::params![id, &payload]) {
                            Ok(_) => break,
                            Err(e) => {
                                if retry < MAX_RETRIES
                                    && matches!(
                                        e.sqlite_error_code(),
                                        Some(
                                            rusqlite::ErrorCode::DatabaseBusy
                                                | rusqlite::ErrorCode::DatabaseLocked
                                        )
                                    )
                                {
                                    retry += 1;
                                    thread::sleep(Duration::from_millis(RETRY_SLEEP_MS));
                                    continue;
                                }
                                eprintln!("[sqlite t{tid}] INSERT {id} failed: {e}");
                                failed += 1;
                                break;
                            }
                        }
                    }
                }
            }
            // Retry COMMIT on Busy — WAL writer serialisation can race.
            let mut retry = 0usize;
            loop {
                match conn.execute_batch("COMMIT") {
                    Ok(()) => break,
                    Err(e) => {
                        if retry < MAX_RETRIES
                            && matches!(
                                e.sqlite_error_code(),
                                Some(
                                    rusqlite::ErrorCode::DatabaseBusy
                                        | rusqlite::ErrorCode::DatabaseLocked
                                )
                            )
                        {
                            retry += 1;
                            thread::sleep(Duration::from_millis(RETRY_SLEEP_MS));
                            continue;
                        }
                        eprintln!("[sqlite t{tid}] COMMIT failed: {e}");
                        let _ = conn.execute_batch("ROLLBACK");
                        failed += rows_per_thread;
                        break;
                    }
                }
            }

            failed
        });
        handles.push(handle);
    }

    let mut total_failed = 0usize;
    for h in handles {
        let failed = h.join().expect("thread join");
        total_failed += failed;
    }
    let elapsed = t0.elapsed();

    RunResult {
        best_elapsed: elapsed,
        total_rows: threads * rows_per_thread,
        failed_rows: total_failed,
    }
}

// ─── Driver ───────────────────────────────────────────────────────────────

fn collect_samples<F: FnMut() -> Result<RunResult, String>>(
    iters: usize,
    mut f: F,
) -> Result<RunStats, String> {
    let mut samples = Vec::with_capacity(iters);
    for _ in 0..iters {
        samples.push(f()?);
    }
    Ok(RunStats::new(samples))
}

#[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation)]
fn main() {
    if let Err(error) = run() {
        eprintln!("error: {error}");
        std::process::exit(1);
    }
}

#[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation)]
fn run() -> Result<(), String> {
    let opts = parse_args();

    eprintln!(
        "mt-mvcc-bench: rows_per_thread={} threads={:?} iters={} apples_to_apples={} separate_tables={}",
        opts.rows_per_thread, opts.threads, opts.iters, opts.apples_to_apples, opts.separate_tables,
    );

    println!(
        "threads | fsqlite_wps | sqlite_wps | throughput_ratio | fsqlite_wps_p95 | fsqlite_wps_p99 | sqlite_wps_p95 | sqlite_wps_p99 | fsqlite_ms_p50 | fsqlite_ms_p95 | fsqlite_ms_p99 | sqlite_ms_p50 | sqlite_ms_p95 | sqlite_ms_p99 | time_ratio | fsqlite_failed | sqlite_failed"
    );
    let mut thread_results = Vec::new();
    for &n in &opts.threads {
        if n == 0 {
            continue;
        }
        let fs = collect_samples(opts.iters, || {
            run_fsqlite(n, opts.rows_per_thread, opts.separate_tables)
        })?;
        let cs = collect_samples(opts.iters, || {
            Ok(run_rusqlite(n, opts.rows_per_thread, opts.separate_tables))
        })?;
        let report = build_thread_report(n, &fs, &cs);

        println!(
            "{n:>7} | {fs_wps:>11.0} | {cs_wps:>10.0} | {throughput_ratio:>16.2}x | {fs_wps_p95:>15.0} | {fs_wps_p99:>15.0} | {sqlite_wps_p95:>14.0} | {sqlite_wps_p99:>14.0} | {fs_ms_p50:>14.2} | {fs_ms_p95:>14.2} | {fs_ms_p99:>14.2} | {sqlite_ms_p50:>13.2} | {sqlite_ms_p95:>13.2} | {sqlite_ms_p99:>13.2} | {time_ratio:>10.2}x | {fs_failed:>14} | {sqlite_failed:>13}",
            fs_wps = report.fsqlite_wps_p50,
            cs_wps = report.sqlite_wps_p50,
            throughput_ratio = report.throughput_ratio,
            fs_wps_p95 = report.fsqlite_wps_p95,
            fs_wps_p99 = report.fsqlite_wps_p99,
            sqlite_wps_p95 = report.sqlite_wps_p95,
            sqlite_wps_p99 = report.sqlite_wps_p99,
            fs_ms_p50 = report.fsqlite_ms_p50,
            fs_ms_p95 = report.fsqlite_ms_p95,
            fs_ms_p99 = report.fsqlite_ms_p99,
            sqlite_ms_p50 = report.sqlite_ms_p50,
            sqlite_ms_p95 = report.sqlite_ms_p95,
            sqlite_ms_p99 = report.sqlite_ms_p99,
            time_ratio = report.time_ratio,
            fs_failed = report.fsqlite_failed_rows,
            sqlite_failed = report.sqlite_failed_rows
        );
        thread_results.push(report);
    }

    let previous_report = load_previous_report(&opts.history_json)?;
    let workload_shape = workload_shape(opts.separate_tables);
    let pass_over_pass_gate = build_pass_over_pass_gate(
        &opts.history_json,
        previous_report.as_ref(),
        &thread_results,
        workload_shape,
        opts.rows_per_thread,
    );

    let full_report = MtMvccBenchReport {
        schema_version: REPORT_SCHEMA_V3,
        workload_shape,
        rows_per_thread: opts.rows_per_thread,
        iterations: opts.iters,
        thread_results,
        pass_over_pass_gate,
    };

    if let Some(path) = opts.json_output.as_deref() {
        write_json_report(path, &full_report)?;
        eprintln!("mt-mvcc-bench: wrote json report {}", path.display());
    }
    if let Some(path) = opts.summary_md.as_deref() {
        write_markdown_summary(path, &full_report)?;
        eprintln!("mt-mvcc-bench: wrote markdown summary {}", path.display());
    }
    if full_report.pass_over_pass_gate.status != "failed" {
        write_json_report(&opts.history_json, &full_report)?;
        eprintln!(
            "mt-mvcc-bench: updated pass-over-pass history {}",
            opts.history_json.display()
        );
    }
    if !full_report.pass_over_pass_gate.regressions.is_empty() {
        return Err(format!(
            "pass-over-pass ratio gate failed: {}",
            full_report
                .pass_over_pass_gate
                .regressions
                .iter()
                .map(|regression| format!(
                    "{}t {:.2}x -> {:.2}x ({:.2}% drop)",
                    regression.threads,
                    regression.previous_ratio,
                    regression.current_ratio,
                    regression.ratio_drop_pct
                ))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_stats(elapsed_ms: u64, total_rows: usize, failed_rows: usize) -> RunStats {
        RunStats::new(vec![RunResult {
            best_elapsed: Duration::from_millis(elapsed_ms),
            total_rows,
            failed_rows,
        }])
    }

    #[test]
    fn thread_report_computes_expected_ratios() {
        let fsqlite = sample_stats(200, 1000, 3);
        let sqlite = sample_stats(100, 1000, 1);

        let report = build_thread_report(4, &fsqlite, &sqlite);

        assert_eq!(report.threads, 4);
        assert!((report.fsqlite_wps_p50 - 5000.0).abs() < 0.01);
        assert!((report.sqlite_wps_p50 - 10_000.0).abs() < 0.01);
        assert!((report.throughput_ratio - 0.5).abs() < 0.0001);
        assert!((report.time_ratio - 2.0).abs() < 0.0001);
        assert_eq!(report.fsqlite_failed_rows, 3);
        assert_eq!(report.sqlite_failed_rows, 1);
    }

    #[test]
    fn markdown_summary_renders_thread_rows() {
        let report = MtMvccBenchReport {
            schema_version: REPORT_SCHEMA_V3,
            workload_shape: "shared_table",
            rows_per_thread: 250,
            iterations: 1,
            thread_results: vec![ThreadComparisonReport {
                threads: 8,
                fsqlite_wps_p50: 6090.0,
                fsqlite_wps_p95: 6090.0,
                fsqlite_wps_p99: 6090.0,
                sqlite_wps_p50: 55_406.0,
                sqlite_wps_p95: 55_406.0,
                sqlite_wps_p99: 55_406.0,
                throughput_ratio: 0.11,
                fsqlite_ms_p50: 328.39,
                fsqlite_ms_p95: 328.39,
                fsqlite_ms_p99: 328.39,
                sqlite_ms_p50: 36.10,
                sqlite_ms_p95: 36.10,
                sqlite_ms_p99: 36.10,
                time_ratio: 9.10,
                fsqlite_failed_rows: 0,
                sqlite_failed_rows: 0,
            }],
            pass_over_pass_gate: PassOverPassGateReport {
                schema_version: PASS_OVER_PASS_SCHEMA_V1,
                history_json_path: DEFAULT_HISTORY_JSON.to_owned(),
                threshold_ratio_drop_pct: PASS_OVER_PASS_MAX_RATIO_DROP_PCT,
                status: "passed",
                previous_report_found: true,
                regressions: Vec::new(),
            },
        };

        let rendered = render_markdown_summary(&report);

        assert!(rendered.contains("# mt-mvcc-bench Summary"));
        assert!(rendered.contains("- Workload shape: `shared_table`"));
        assert!(rendered.contains("| 8 | 6090 | 55406 | 0.11x |"));
        assert!(rendered.contains("Pass-over-pass gate"));
    }

    #[test]
    fn separate_tables_schema_uses_one_table_per_worker() {
        let sql = create_tables_sql(3, true);

        assert!(sql.contains(
            "CREATE TABLE IF NOT EXISTS bench_0 (id INTEGER PRIMARY KEY, payload TEXT);"
        ));
        assert!(sql.contains(
            "CREATE TABLE IF NOT EXISTS bench_1 (id INTEGER PRIMARY KEY, payload TEXT);"
        ));
        assert!(sql.contains(
            "CREATE TABLE IF NOT EXISTS bench_2 (id INTEGER PRIMARY KEY, payload TEXT);"
        ));
        assert!(!sql.contains("CREATE TABLE IF NOT EXISTS bench ("));
    }

    #[test]
    fn shared_table_schema_uses_single_bench_table() {
        let sql = create_tables_sql(8, false);

        assert_eq!(sql.matches("CREATE TABLE IF NOT EXISTS bench ").count(), 1);
        assert!(
            sql.contains(
                "CREATE TABLE IF NOT EXISTS bench (id INTEGER PRIMARY KEY, payload TEXT);"
            )
        );
        assert!(!sql.contains("bench_0"));
    }

    #[test]
    fn worker_insert_sql_matches_workload_shape() {
        assert_eq!(
            worker_insert_sql(7, false),
            "INSERT INTO bench (id, payload) VALUES (?1, ?2)"
        );
        assert_eq!(
            worker_insert_sql(7, true),
            "INSERT INTO bench_7 (id, payload) VALUES (?1, ?2)"
        );
    }

    #[test]
    fn parse_thread_count_rejects_zero_worker_runs() {
        assert_eq!(parse_thread_count("16").unwrap(), 16);
        assert_eq!(parse_thread_count(" 4 ").unwrap(), 4);
        assert!(parse_thread_count("0").is_err());
        assert!(parse_thread_count("not-a-number").is_err());
    }

    #[test]
    fn pass_over_pass_gate_flags_ratio_drop_over_five_percent() {
        let previous = HistoricalMtMvccBenchReport {
            workload_shape: Some("shared_table".to_owned()),
            rows_per_thread: Some(1000),
            thread_results: vec![ThreadComparisonReport {
                threads: 8,
                fsqlite_wps_p50: 0.0,
                fsqlite_wps_p95: 0.0,
                fsqlite_wps_p99: 0.0,
                sqlite_wps_p50: 0.0,
                sqlite_wps_p95: 0.0,
                sqlite_wps_p99: 0.0,
                throughput_ratio: 0.50,
                fsqlite_ms_p50: 0.0,
                fsqlite_ms_p95: 0.0,
                fsqlite_ms_p99: 0.0,
                sqlite_ms_p50: 0.0,
                sqlite_ms_p95: 0.0,
                sqlite_ms_p99: 0.0,
                time_ratio: 0.0,
                fsqlite_failed_rows: 0,
                sqlite_failed_rows: 0,
            }],
        };
        let current = vec![ThreadComparisonReport {
            threads: 8,
            fsqlite_wps_p50: 0.0,
            fsqlite_wps_p95: 0.0,
            fsqlite_wps_p99: 0.0,
            sqlite_wps_p50: 0.0,
            sqlite_wps_p95: 0.0,
            sqlite_wps_p99: 0.0,
            throughput_ratio: 0.46,
            fsqlite_ms_p50: 0.0,
            fsqlite_ms_p95: 0.0,
            fsqlite_ms_p99: 0.0,
            sqlite_ms_p50: 0.0,
            sqlite_ms_p95: 0.0,
            sqlite_ms_p99: 0.0,
            time_ratio: 0.0,
            fsqlite_failed_rows: 0,
            sqlite_failed_rows: 0,
        }];

        let gate = build_pass_over_pass_gate(
            Path::new(DEFAULT_HISTORY_JSON),
            Some(&previous),
            &current,
            "shared_table",
            1000,
        );

        assert_eq!(gate.status, "failed");
        assert_eq!(gate.regressions.len(), 1);
        assert_eq!(gate.regressions[0].threads, 8);
        assert!((gate.regressions[0].ratio_drop_pct - 8.0).abs() < 1.0e-6);
    }

    #[test]
    fn pass_over_pass_gate_skips_without_prior_report() {
        let gate = build_pass_over_pass_gate(
            Path::new(DEFAULT_HISTORY_JSON),
            None,
            &[],
            "shared_table",
            1000,
        );

        assert_eq!(gate.status, "no_prior_report");
        assert!(gate.regressions.is_empty());
    }

    #[test]
    fn pass_over_pass_gate_skips_incompatible_history_shape() {
        let previous = HistoricalMtMvccBenchReport {
            workload_shape: Some("shared_table".to_owned()),
            rows_per_thread: Some(100),
            thread_results: vec![ThreadComparisonReport {
                threads: 16,
                fsqlite_wps_p50: 0.0,
                fsqlite_wps_p95: 0.0,
                fsqlite_wps_p99: 0.0,
                sqlite_wps_p50: 0.0,
                sqlite_wps_p95: 0.0,
                sqlite_wps_p99: 0.0,
                throughput_ratio: 30.0,
                fsqlite_ms_p50: 0.0,
                fsqlite_ms_p95: 0.0,
                fsqlite_ms_p99: 0.0,
                sqlite_ms_p50: 0.0,
                sqlite_ms_p95: 0.0,
                sqlite_ms_p99: 0.0,
                time_ratio: 0.0,
                fsqlite_failed_rows: 0,
                sqlite_failed_rows: 0,
            }],
        };
        let current = vec![ThreadComparisonReport {
            threads: 16,
            fsqlite_wps_p50: 0.0,
            fsqlite_wps_p95: 0.0,
            fsqlite_wps_p99: 0.0,
            sqlite_wps_p50: 0.0,
            sqlite_wps_p95: 0.0,
            sqlite_wps_p99: 0.0,
            throughput_ratio: 20.0,
            fsqlite_ms_p50: 0.0,
            fsqlite_ms_p95: 0.0,
            fsqlite_ms_p99: 0.0,
            sqlite_ms_p50: 0.0,
            sqlite_ms_p95: 0.0,
            sqlite_ms_p99: 0.0,
            time_ratio: 0.0,
            fsqlite_failed_rows: 0,
            sqlite_failed_rows: 0,
        }];

        let gate = build_pass_over_pass_gate(
            Path::new(DEFAULT_HISTORY_JSON),
            Some(&previous),
            &current,
            "shared_table",
            1000,
        );

        assert_eq!(gate.status, "no_prior_report");
        assert!(gate.regressions.is_empty());
    }

    #[test]
    fn retry_budget_allows_busy_timeout_scaled_retries() {
        let mut budget = FsqliteRetryBudget::new();
        let mut waits = Vec::new();
        for _ in 0..MAX_RETRIES {
            waits.push(
                budget
                    .next_wait(8)
                    .expect("budget should allow configured retry count"),
            );
        }

        assert_eq!(budget.attempts(), MAX_RETRIES);
        assert!(budget.next_wait(8).is_none());
        assert!(
            waits
                .iter()
                .all(|wait| wait.as_millis() <= u128::from(MAX_RETRY_SLEEP_MS + 4))
        );
    }
}
