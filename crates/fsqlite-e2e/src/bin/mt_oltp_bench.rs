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
//! Configurable ratios (e.g., 90/10 read/write, 50/50, pure-read, pure-write).
//!
//! ## CLI
//!
//! ```text
//! mt-oltp-bench [--seed-rows=5000] [--ops-per-thread=5000]
//!               [--readers=4] [--writers=2]
//!               [--iters=3] [--json-output=PATH]
//! ```

use serde::Serialize;
use std::fmt::Write as _;
use std::path::PathBuf;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const DEFAULT_SEED_ROWS: i64 = 5_000;
const DEFAULT_OPS_PER_THREAD: usize = 5_000;
const DEFAULT_READERS: usize = 4;
const DEFAULT_WRITERS: usize = 2;
const DEFAULT_ITERS: usize = 3;
const PAYLOAD_SIZE: usize = 64;
const ROWID_BASE_STRIDE: i64 = 1_000_000;
const MAX_RETRIES: usize = 512;
const REPORT_SCHEMA: &str = "fsqlite-e2e.mt_oltp_bench_report.v1";
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
        "Usage: mt-oltp-bench [--seed-rows=N] [--ops-per-thread=N] \
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
    ops: u64,
    failed_ops: u64,
    elapsed_ms: f64,
    ops_per_sec: f64,
    latency: LatencyStats,
}

#[derive(Debug, Clone, Serialize)]
struct IterResult {
    readers: Vec<ThreadReport>,
    writers: Vec<ThreadReport>,
    wall_elapsed_ms: f64,
    total_read_ops: u64,
    total_write_ops: u64,
    total_failed_writes: u64,
    aggregate_read_latency: LatencyStats,
    aggregate_write_latency: LatencyStats,
    read_ops_per_sec: f64,
    write_ops_per_sec: f64,
    reader_fairness_jain: f64,
    writer_fairness_jain: f64,
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
    bead_id: String,
    timestamp_unix_ms: u64,
    seed_rows: i64,
    ops_per_thread: usize,
    num_readers: usize,
    num_writers: usize,
    iterations: usize,
    fsqlite: EngineReport,
    sqlite_reference: EngineReport,
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

// ─── FrankenSQLite engine ───────────────────────────────────────────────

fn run_fsqlite_iter(
    seed_rows: i64,
    ops_per_thread: usize,
    num_readers: usize,
    num_writers: usize,
) -> IterResult {
    let tmp = tempfile::NamedTempFile::new().expect("tempfile");
    let path: String = tmp.path().to_string_lossy().into_owned();
    drop(tmp);

    {
        let conn = fsqlite::Connection::open(path.clone()).expect("fsqlite open (seed)");
        let _ = conn.execute("PRAGMA fsqlite.concurrent_mode=ON;");
        conn.execute("CREATE TABLE bench (id INTEGER PRIMARY KEY, payload TEXT)")
            .expect("create table");
        conn.execute("BEGIN").expect("begin");
        let stmt = conn
            .prepare("INSERT INTO bench (id, payload) VALUES (?1, ?2)")
            .expect("prepare insert");
        let payload = make_payload();
        for id in 1..=seed_rows {
            stmt.execute_with_params(&[
                fsqlite::SqliteValue::Integer(id),
                fsqlite::SqliteValue::Text(payload.clone().into()),
            ])
            .expect("seed insert");
        }
        conn.execute("COMMIT").expect("commit");
    }

    let total_threads = num_readers + num_writers;
    let path = Arc::new(path);
    let barrier = Arc::new(Barrier::new(total_threads));
    let mut handles = Vec::with_capacity(total_threads);

    let t0 = Instant::now();

    for rid in 0..num_readers {
        let path = Arc::clone(&path);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            let conn =
                fsqlite::Connection::open(path.as_str().to_owned()).expect("fsqlite open (reader)");
            let _ = conn.execute("PRAGMA fsqlite.concurrent_mode=ON;");
            let _ = conn.execute("PRAGMA busy_timeout=5000;");
            let stmt = conn
                .prepare("SELECT payload FROM bench WHERE id = ?1")
                .expect("prepare select");
            barrier.wait();

            let mut latencies = Vec::with_capacity(ops_per_thread);
            let mut rng_state = 0x0102_0304_u64 ^ (rid as u64).wrapping_mul(0x9e37);
            #[allow(clippy::cast_possible_wrap)]
            for _ in 0..ops_per_thread {
                let id = (lcg_next(&mut rng_state) % seed_rows as u64 + 1) as i64;
                let t = Instant::now();
                let _ = stmt.query_with_params(&[fsqlite::SqliteValue::Integer(id)]);
                latencies.push(t.elapsed().as_nanos() as u64);
            }

            let elapsed = t0.elapsed();
            let latency = compute_latency_stats(latencies);
            #[allow(clippy::cast_precision_loss)]
            let ops_f = ops_per_thread as f64;
            ThreadReport {
                role: "reader".to_owned(),
                tid: rid,
                ops: ops_per_thread as u64,
                failed_ops: 0,
                elapsed_ms: elapsed.as_secs_f64() * 1_000.0,
                ops_per_sec: ops_f / elapsed.as_secs_f64(),
                latency,
            }
        }));
    }

    for wid in 0..num_writers {
        let path = Arc::clone(&path);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            let conn =
                fsqlite::Connection::open(path.as_str().to_owned()).expect("fsqlite open (writer)");
            let concurrent_ok = conn.execute("PRAGMA fsqlite.concurrent_mode=ON;").is_ok();
            let _ = conn.execute("PRAGMA busy_timeout=5000;");

            barrier.wait();

            let mut latencies = Vec::with_capacity(ops_per_thread);
            let mut failed = 0u64;
            #[allow(clippy::cast_possible_wrap)]
            let base_id = seed_rows + 1 + (wid as i64 * ROWID_BASE_STRIDE);
            let payload = make_payload();

            for i in 0..ops_per_thread {
                let id = base_id + i as i64;
                let t = Instant::now();
                let begin_sql = if concurrent_ok {
                    "BEGIN CONCURRENT"
                } else {
                    "BEGIN"
                };
                if conn.execute(begin_sql).is_err() {
                    failed += 1;
                    latencies.push(t.elapsed().as_nanos() as u64);
                    continue;
                }
                let ok = conn
                    .execute(&format!(
                        "INSERT INTO bench (id, payload) VALUES ({id}, '{payload}')"
                    ))
                    .is_ok();
                if ok {
                    let mut committed = false;
                    for _retry in 0..MAX_RETRIES {
                        match conn.execute("COMMIT") {
                            Ok(_) => {
                                committed = true;
                                break;
                            }
                            Err(e) if e.is_transient() => {
                                thread::sleep(Duration::from_micros(100));
                            }
                            Err(_) => break,
                        }
                    }
                    if !committed {
                        let _ = conn.execute("ROLLBACK");
                        failed += 1;
                    }
                } else {
                    let _ = conn.execute("ROLLBACK");
                    failed += 1;
                }
                latencies.push(t.elapsed().as_nanos() as u64);
            }

            let elapsed = t0.elapsed();
            let latency = compute_latency_stats(latencies);
            #[allow(clippy::cast_precision_loss)]
            let successful = (ops_per_thread as u64).saturating_sub(failed);
            ThreadReport {
                role: "writer".to_owned(),
                tid: wid,
                ops: successful,
                failed_ops: failed,
                elapsed_ms: elapsed.as_secs_f64() * 1_000.0,
                ops_per_sec: successful as f64 / elapsed.as_secs_f64(),
                latency,
            }
        }));
    }

    let mut readers = Vec::new();
    let mut writers = Vec::new();
    for h in handles {
        let report = h.join().expect("thread join");
        if report.role == "reader" {
            readers.push(report);
        } else {
            writers.push(report);
        }
    }

    build_iter_result(readers, writers, t0.elapsed())
}

// ─── C SQLite (rusqlite) engine ─────────────────────────────────────────

fn run_rusqlite_iter(
    seed_rows: i64,
    ops_per_thread: usize,
    num_readers: usize,
    num_writers: usize,
) -> IterResult {
    let tmp = tempfile::NamedTempFile::new().expect("tempfile");
    let path: String = tmp.path().to_string_lossy().into_owned();
    drop(tmp);

    {
        let conn = rusqlite::Connection::open(&path).expect("sqlite open (seed)");
        conn.execute_batch(
            "PRAGMA journal_mode=WAL; \
             PRAGMA synchronous=NORMAL; \
             PRAGMA busy_timeout=5000;",
        )
        .expect("pragmas");
        conn.execute_batch("CREATE TABLE bench (id INTEGER PRIMARY KEY, payload TEXT);")
            .expect("create table");
        conn.execute_batch("BEGIN").expect("begin");
        let mut stmt = conn
            .prepare("INSERT INTO bench (id, payload) VALUES (?1, ?2)")
            .expect("prepare insert");
        let payload = make_payload();
        for id in 1..=seed_rows {
            stmt.execute(rusqlite::params![id, payload]).expect("seed");
        }
        drop(stmt);
        conn.execute_batch("COMMIT").expect("commit");
    }

    let total_threads = num_readers + num_writers;
    let path = Arc::new(path);
    let barrier = Arc::new(Barrier::new(total_threads));
    let mut handles = Vec::with_capacity(total_threads);

    let t0 = Instant::now();

    for rid in 0..num_readers {
        let path = Arc::clone(&path);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            let conn = rusqlite::Connection::open(path.as_str()).expect("sqlite open (reader)");
            conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;")
                .ok();
            let mut stmt = conn
                .prepare("SELECT payload FROM bench WHERE id = ?1")
                .expect("prepare select");
            barrier.wait();

            let mut latencies = Vec::with_capacity(ops_per_thread);
            let mut rng_state = 0x0102_0304_u64 ^ (rid as u64).wrapping_mul(0x9e37);
            #[allow(clippy::cast_possible_wrap)]
            for _ in 0..ops_per_thread {
                let id = (lcg_next(&mut rng_state) % seed_rows as u64 + 1) as i64;
                let t = Instant::now();
                let _: Option<String> = stmt.query_row(rusqlite::params![id], |r| r.get(0)).ok();
                latencies.push(t.elapsed().as_nanos() as u64);
            }

            let elapsed = t0.elapsed();
            let latency = compute_latency_stats(latencies);
            #[allow(clippy::cast_precision_loss)]
            let ops_f = ops_per_thread as f64;
            ThreadReport {
                role: "reader".to_owned(),
                tid: rid,
                ops: ops_per_thread as u64,
                failed_ops: 0,
                elapsed_ms: elapsed.as_secs_f64() * 1_000.0,
                ops_per_sec: ops_f / elapsed.as_secs_f64(),
                latency,
            }
        }));
    }

    for wid in 0..num_writers {
        let path = Arc::clone(&path);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            let conn = rusqlite::Connection::open(path.as_str()).expect("sqlite open (writer)");
            conn.execute_batch(
                "PRAGMA journal_mode=WAL; \
                 PRAGMA synchronous=NORMAL; \
                 PRAGMA busy_timeout=5000;",
            )
            .ok();

            barrier.wait();

            let mut latencies = Vec::with_capacity(ops_per_thread);
            let mut failed = 0u64;
            #[allow(clippy::cast_possible_wrap)]
            let base_id = seed_rows + 1 + (wid as i64 * ROWID_BASE_STRIDE);
            let payload = make_payload();

            for i in 0..ops_per_thread {
                let id = base_id + i as i64;
                let t = Instant::now();
                match conn.execute(
                    "INSERT INTO bench (id, payload) VALUES (?1, ?2)",
                    rusqlite::params![id, payload],
                ) {
                    Ok(_) => {}
                    Err(_) => {
                        failed += 1;
                    }
                }
                latencies.push(t.elapsed().as_nanos() as u64);
            }

            let elapsed = t0.elapsed();
            let latency = compute_latency_stats(latencies);
            #[allow(clippy::cast_precision_loss)]
            let successful = (ops_per_thread as u64).saturating_sub(failed);
            ThreadReport {
                role: "writer".to_owned(),
                tid: wid,
                ops: successful,
                failed_ops: failed,
                elapsed_ms: elapsed.as_secs_f64() * 1_000.0,
                ops_per_sec: successful as f64 / elapsed.as_secs_f64(),
                latency,
            }
        }));
    }

    let mut readers = Vec::new();
    let mut writers = Vec::new();
    for h in handles {
        let report = h.join().expect("thread join");
        if report.role == "reader" {
            readers.push(report);
        } else {
            writers.push(report);
        }
    }

    build_iter_result(readers, writers, t0.elapsed())
}

// ─── Result aggregation ─────────────────────────────────────────────────

fn build_iter_result(
    readers: Vec<ThreadReport>,
    writers: Vec<ThreadReport>,
    wall_elapsed: Duration,
) -> IterResult {
    let wall_ms = wall_elapsed.as_secs_f64() * 1_000.0;

    let total_read_ops: u64 = readers.iter().map(|r| r.ops).sum();
    let total_write_ops: u64 = writers.iter().map(|w| w.ops).sum();
    let total_failed: u64 = writers.iter().map(|w| w.failed_ops).sum();

    let all_read_latencies: Vec<u64> = readers
        .iter()
        .flat_map(|r| {
            std::iter::repeat_n(
                (r.latency.mean_us * 1_000.0) as u64,
                r.latency.count as usize,
            )
        })
        .collect();
    let all_write_latencies: Vec<u64> = writers
        .iter()
        .flat_map(|w| {
            std::iter::repeat_n(
                (w.latency.mean_us * 1_000.0) as u64,
                w.latency.count as usize,
            )
        })
        .collect();

    let reader_rates: Vec<f64> = readers.iter().map(|r| r.ops_per_sec).collect();
    let writer_rates: Vec<f64> = writers.iter().map(|w| w.ops_per_sec).collect();

    #[allow(clippy::cast_precision_loss)]
    let read_ops_per_sec = total_read_ops as f64 / wall_elapsed.as_secs_f64();
    #[allow(clippy::cast_precision_loss)]
    let write_ops_per_sec = total_write_ops as f64 / wall_elapsed.as_secs_f64();

    IterResult {
        readers,
        writers,
        wall_elapsed_ms: wall_ms,
        total_read_ops,
        total_write_ops,
        total_failed_writes: total_failed,
        aggregate_read_latency: compute_latency_stats(all_read_latencies),
        aggregate_write_latency: compute_latency_stats(all_write_latencies),
        read_ops_per_sec,
        write_ops_per_sec,
        reader_fairness_jain: jain_fairness(&reader_rates),
        writer_fairness_jain: jain_fairness(&writer_rates),
    }
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
        "[{BEAD_ID}] Config: {seed_rows} seed rows, {ops}/thread, {r}R/{w}W threads, {iters} iters",
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
        "Engine", "Read ops/s", "Write ops/s", "Read p50 µs", "Read p95 µs", "Read p99 µs"
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
        "  Read throughput ratio:  {:.2}x (fsqlite / C SQLite)",
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

fn main() {
    let opts = parse_opts();

    if opts.readers == 0 && opts.writers == 0 {
        eprintln!("Need at least 1 reader or writer");
        std::process::exit(2);
    }

    eprintln!(
        "[{BEAD_ID}] mt-oltp-bench: {r}R/{w}W, {seed} seed rows, {ops} ops/thread, {iters} iters",
        r = opts.readers,
        w = opts.writers,
        seed = opts.seed_rows,
        ops = opts.ops_per_thread,
        iters = opts.iters,
    );

    eprintln!("[{BEAD_ID}] Running FrankenSQLite...");
    let mut fs_iters = Vec::with_capacity(opts.iters);
    for i in 0..opts.iters {
        eprint!("  iter {}/{}... ", i + 1, opts.iters);
        let result = run_fsqlite_iter(
            opts.seed_rows,
            opts.ops_per_thread,
            opts.readers,
            opts.writers,
        );
        eprintln!(
            "read={:.0} ops/s, write={:.0} ops/s, failed={}",
            result.read_ops_per_sec, result.write_ops_per_sec, result.total_failed_writes
        );
        fs_iters.push(result);
    }

    eprintln!("[{BEAD_ID}] Running C SQLite (rusqlite)...");
    let mut cs_iters = Vec::with_capacity(opts.iters);
    for i in 0..opts.iters {
        eprint!("  iter {}/{}... ", i + 1, opts.iters);
        let result = run_rusqlite_iter(
            opts.seed_rows,
            opts.ops_per_thread,
            opts.readers,
            opts.writers,
        );
        eprintln!(
            "read={:.0} ops/s, write={:.0} ops/s, failed={}",
            result.read_ops_per_sec, result.write_ops_per_sec, result.total_failed_writes
        );
        cs_iters.push(result);
    }

    let fs_report = build_engine_report("fsqlite", fs_iters);
    let cs_report = build_engine_report("sqlite_reference", cs_iters);

    let read_ratio = if cs_report.median_read_ops_per_sec > 0.0 {
        fs_report.median_read_ops_per_sec / cs_report.median_read_ops_per_sec
    } else {
        0.0
    };
    let write_ratio = if cs_report.median_write_ops_per_sec > 0.0 {
        fs_report.median_write_ops_per_sec / cs_report.median_write_ops_per_sec
    } else {
        0.0
    };
    let p50_ratio = if cs_report.median_read_p50_us > 0.0 {
        fs_report.median_read_p50_us / cs_report.median_read_p50_us
    } else {
        0.0
    };
    let p95_ratio = if cs_report.median_read_p95_us > 0.0 {
        fs_report.median_read_p95_us / cs_report.median_read_p95_us
    } else {
        0.0
    };

    let report = BenchReport {
        schema_version: REPORT_SCHEMA.to_owned(),
        bead_id: BEAD_ID.to_owned(),
        timestamp_unix_ms: now_unix_ms(),
        seed_rows: opts.seed_rows,
        ops_per_thread: opts.ops_per_thread,
        num_readers: opts.readers,
        num_writers: opts.writers,
        iterations: opts.iters,
        fsqlite: fs_report,
        sqlite_reference: cs_report,
        read_throughput_ratio: (read_ratio * 100.0).round() / 100.0,
        write_throughput_ratio: (write_ratio * 100.0).round() / 100.0,
        read_latency_p50_ratio: (p50_ratio * 100.0).round() / 100.0,
        read_latency_p95_ratio: (p95_ratio * 100.0).round() / 100.0,
    };

    print_summary(&report);

    if let Some(ref path) = opts.json_output {
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let json = serde_json::to_string_pretty(&report).expect("serialize");
        std::fs::write(path, json).expect("write json");
        eprintln!("[{BEAD_ID}] JSON written to {}", path.display());
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

    #[test]
    fn build_iter_result_no_threads() {
        let result = build_iter_result(vec![], vec![], Duration::from_millis(100));
        assert_eq!(result.total_read_ops, 0);
        assert_eq!(result.total_write_ops, 0);
        assert!((result.reader_fairness_jain - 1.0).abs() < 1e-10);
    }

    #[test]
    fn median_of_values() {
        assert!((median_of(vec![3.0, 1.0, 2.0]) - 2.0).abs() < 1e-10);
        assert!((median_of(vec![1.0]) - 1.0).abs() < 1e-10);
        assert_eq!(median_of(vec![]), 0.0);
    }
}
