//! bd-wsw3p: Concurrent-write-only benchmark validation.
//!
//! Verifies that FrankenSQLite's page-level MVCC provides measurable throughput
//! improvement over C SQLite's serialized WAL_WRITE_LOCK at 4+ threads with
//! non-conflicting workloads (each thread writes to its own table).
//!
//! The production assertion follows the canonical `mt-mvcc-bench` transaction
//! shape: one retryable transaction spans all rows for each worker. The older
//! one-transaction-per-row shape remains as non-asserting diagnostic evidence.
//! The suite produces structured JSON artifacts to the temp directory, then
//! validates:
//! - Both engines produce correct row counts
//! - Median FrankenSQLite 4-thread throughput beats the C SQLite baseline in
//!   `release-perf`
//! - Structured output contains required fields

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use fsqlite::SqliteValue;
use serde::{Deserialize, Serialize};

const ROWS_PER_THREAD: i64 = 200;
const MAX_TXN_RETRIES: u32 = 100;
const RETRY_BACKOFF: Duration = Duration::from_micros(100);
const T4_SAMPLE_COUNT: usize = 11;
const T4_REPLAY_CMD: &str = "cargo test --profile release-perf -p fsqlite-e2e --test bd_wsw3p_concurrent_write_showcase t4_fsqlite_outperforms_csqlite_at_4_threads -- --nocapture --test-threads=1";

fn artifact_dir() -> PathBuf {
    let run_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock must be after Unix epoch")
        .as_nanos();
    let dir = std::env::temp_dir()
        .join("fsqlite-wsw3p-tests")
        .join(format!("run-{}-{run_id}", std::process::id()));
    fs::create_dir_all(&dir).expect("create artifact dir");
    dir
}

// ── Engine runners ──────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ThreadResult {
    thread_id: usize,
    rows_inserted: i64,
    wall_ms: u64,
    wall_ns: u64,
    retries: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchResult {
    engine: String,
    transaction_shape: String,
    n_threads: usize,
    rows_per_thread: i64,
    total_rows: i64,
    total_wall_ms: u64,
    throughput_ops_per_sec: f64,
    total_retries: u64,
    per_thread: Vec<ThreadResult>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TransactionShape {
    /// One transaction spans every row written by a worker. This is the
    /// canonical `mt-mvcc-bench --separate-tables` contract.
    PerWorker,
    /// Every row is its own transaction. This is retained only as diagnostic
    /// evidence for the small-transaction ceremony gap.
    PerRow,
}

impl TransactionShape {
    const fn as_str(self) -> &'static str {
        match self {
            Self::PerWorker => "per_worker",
            Self::PerRow => "per_row",
        }
    }
}

fn is_csqlite_busy(error: &rusqlite::Error) -> bool {
    matches!(
        error.sqlite_error_code(),
        Some(rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked)
    )
}

fn execute_csqlite_row(stmt: &mut rusqlite::Statement<'_>, row_id: i64, local_retries: &mut u64) {
    let mut attempts = 0u32;
    loop {
        match stmt.execute(rusqlite::params![row_id]) {
            Ok(_) => return,
            Err(error) => {
                if !is_csqlite_busy(&error) {
                    panic!("csqlite insert failed: {error}");
                }
                *local_retries += 1;
                attempts += 1;
                assert!(
                    attempts < MAX_TXN_RETRIES,
                    "csqlite insert failed after {MAX_TXN_RETRIES} retries: {error}"
                );
                thread::sleep(RETRY_BACKOFF);
            }
        }
    }
}

fn run_csqlite_concurrent(n_threads: usize, transaction_shape: TransactionShape) -> BenchResult {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let path = tmp.path().to_str().unwrap().to_owned();

    {
        let setup = rusqlite::Connection::open(&path).unwrap();
        setup
            .execute_batch(
                "PRAGMA page_size=4096; PRAGMA journal_mode=WAL; \
                 PRAGMA synchronous=NORMAL; PRAGMA cache_size=-64000;",
            )
            .unwrap();
        for tid in 0..n_threads {
            setup
                .execute_batch(&format!(
                    "CREATE TABLE bench_{tid} (id INTEGER PRIMARY KEY, name TEXT, score INTEGER);"
                ))
                .unwrap();
        }
    }

    let barrier = Arc::new(Barrier::new(n_threads));
    let retry_total = Arc::new(AtomicU64::new(0));

    let start = Instant::now();
    let handles: Vec<_> = (0..n_threads)
        .map(|tid| {
            let p = path.clone();
            let bar = barrier.clone();
            let retries = retry_total.clone();
            thread::spawn(move || {
                let conn = rusqlite::Connection::open(&p).unwrap();
                conn.execute_batch(
                    "PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; \
                     PRAGMA cache_size=-64000; PRAGMA busy_timeout=5000;",
                )
                .unwrap();
                bar.wait();

                let thread_start = Instant::now();
                let insert_sql =
                    format!("INSERT INTO bench_{tid} VALUES (?1, ('t' || ?1), (?1 * 7))");
                let mut stmt = conn.prepare(&insert_sql).unwrap();
                let mut local_retries: u64 = 0;

                match transaction_shape {
                    TransactionShape::PerWorker => {
                        conn.execute_batch("BEGIN").expect("csqlite BEGIN");
                        for i in 0..ROWS_PER_THREAD {
                            execute_csqlite_row(&mut stmt, i, &mut local_retries);
                        }

                        let mut attempts = 0u32;
                        loop {
                            match conn.execute_batch("COMMIT") {
                                Ok(()) => break,
                                Err(e) => {
                                    if !is_csqlite_busy(&e) {
                                        panic!("csqlite COMMIT failed: {e}");
                                    }
                                    local_retries += 1;
                                    attempts += 1;
                                    assert!(
                                        attempts < MAX_TXN_RETRIES,
                                        "csqlite COMMIT failed after {MAX_TXN_RETRIES} retries: {e}"
                                    );
                                    thread::sleep(RETRY_BACKOFF);
                                }
                            }
                        }
                    }
                    TransactionShape::PerRow => {
                        for i in 0..ROWS_PER_THREAD {
                            execute_csqlite_row(&mut stmt, i, &mut local_retries);
                        }
                    }
                }
                retries.fetch_add(local_retries, Ordering::Relaxed);
                let elapsed = thread_start.elapsed();
                ThreadResult {
                    thread_id: tid,
                    rows_inserted: ROWS_PER_THREAD,
                    wall_ms: elapsed.as_millis() as u64,
                    wall_ns: elapsed.as_nanos().try_into().unwrap_or(u64::MAX),
                    retries: local_retries,
                }
            })
        })
        .collect();

    let per_thread: Vec<ThreadResult> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    let total_wall = start.elapsed();
    let total_rows = n_threads as i64 * ROWS_PER_THREAD;
    #[allow(clippy::cast_precision_loss)]
    let throughput = total_rows as f64 / total_wall.as_secs_f64();

    {
        let verify = rusqlite::Connection::open(&path).unwrap();
        for tid in 0..n_threads {
            let count: i64 = verify
                .query_row(&format!("SELECT COUNT(*) FROM bench_{tid}"), [], |r| {
                    r.get(0)
                })
                .unwrap();
            assert_eq!(
                count, ROWS_PER_THREAD,
                "csqlite thread {tid} row count mismatch"
            );
        }
    }

    BenchResult {
        engine: "csqlite".to_owned(),
        transaction_shape: transaction_shape.as_str().to_owned(),
        n_threads,
        rows_per_thread: ROWS_PER_THREAD,
        total_rows,
        total_wall_ms: total_wall.as_millis() as u64,
        throughput_ops_per_sec: throughput,
        total_retries: retry_total.load(Ordering::Relaxed),
        per_thread,
    }
}

fn run_fsqlite_concurrent(n_threads: usize, transaction_shape: TransactionShape) -> BenchResult {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let path_str = tmp.path().to_str().unwrap().to_owned();

    {
        let conn = fsqlite::Connection::open(&path_str).unwrap();
        conn.execute("PRAGMA page_size = 4096;").unwrap();
        conn.execute("PRAGMA journal_mode = WAL;").unwrap();
        conn.execute("PRAGMA synchronous = NORMAL;").unwrap();
        conn.execute("PRAGMA cache_size = -64000;").unwrap();
        for tid in 0..n_threads {
            conn.execute(&format!(
                "CREATE TABLE bench_{tid} (id INTEGER PRIMARY KEY, name TEXT, score INTEGER);"
            ))
            .unwrap();
        }
    }

    let barrier = Arc::new(Barrier::new(n_threads));
    let retry_total = Arc::new(AtomicU64::new(0));

    let start = Instant::now();
    let handles: Vec<_> = (0..n_threads)
        .map(|tid| {
            let p = path_str.clone();
            let bar = barrier.clone();
            let retries = retry_total.clone();
            thread::spawn(move || {
                let conn = fsqlite::Connection::open(&p).unwrap();
                conn.execute("PRAGMA journal_mode = WAL;").unwrap();
                conn.execute("PRAGMA synchronous = NORMAL;").unwrap();
                conn.execute("PRAGMA cache_size = -64000;").unwrap();
                conn.execute("PRAGMA busy_timeout = 5000;").unwrap();
                conn.execute("PRAGMA fsqlite.concurrent_mode = ON;")
                    .unwrap();

                let insert_sql =
                    format!("INSERT INTO bench_{tid} VALUES (?1, ('t' || ?1), (?1 * 7));");
                let stmt = conn.prepare(&insert_sql).unwrap();
                bar.wait();

                let thread_start = Instant::now();
                let mut local_retries: u64 = 0;

                if transaction_shape == TransactionShape::PerWorker {
                    let mut attempts = 0u32;
                    'transaction: loop {
                        if let Err(_e) = conn.execute("BEGIN CONCURRENT") {
                            local_retries += 1;
                            attempts += 1;
                            if attempts >= MAX_TXN_RETRIES {
                                panic!("BEGIN CONCURRENT failed after {MAX_TXN_RETRIES} retries");
                            }
                            thread::sleep(RETRY_BACKOFF);
                            continue;
                        }

                        for i in 0..ROWS_PER_THREAD {
                            match stmt.execute_with_params(&[SqliteValue::Integer(i)]) {
                                Ok(_) => {}
                                Err(_e) => {
                                    let _ = conn.execute("ROLLBACK");
                                    local_retries += 1;
                                    attempts += 1;
                                    if attempts >= MAX_TXN_RETRIES {
                                        panic!("INSERT failed after {MAX_TXN_RETRIES} retries");
                                    }
                                    thread::sleep(RETRY_BACKOFF);
                                    continue 'transaction;
                                }
                            }
                        }

                        match conn.execute("COMMIT") {
                            Ok(_) => break 'transaction,
                            Err(_e) => {
                                let _ = conn.execute("ROLLBACK");
                                local_retries += 1;
                                attempts += 1;
                                if attempts >= MAX_TXN_RETRIES {
                                    panic!("COMMIT failed after {MAX_TXN_RETRIES} retries");
                                }
                                thread::sleep(RETRY_BACKOFF);
                            }
                        }
                    }
                } else {
                    for i in 0..ROWS_PER_THREAD {
                        let mut attempts = 0u32;
                        loop {
                            if let Err(_e) = conn.execute("BEGIN CONCURRENT") {
                                local_retries += 1;
                                attempts += 1;
                                if attempts >= MAX_TXN_RETRIES {
                                    panic!(
                                        "BEGIN CONCURRENT failed after {MAX_TXN_RETRIES} retries"
                                    );
                                }
                                thread::sleep(RETRY_BACKOFF);
                                continue;
                            }

                            match stmt.execute_with_params(&[SqliteValue::Integer(i)]) {
                                Ok(_) => {}
                                Err(_e) => {
                                    let _ = conn.execute("ROLLBACK");
                                    local_retries += 1;
                                    attempts += 1;
                                    if attempts >= MAX_TXN_RETRIES {
                                        panic!("INSERT failed after {MAX_TXN_RETRIES} retries");
                                    }
                                    thread::sleep(RETRY_BACKOFF);
                                    continue;
                                }
                            }

                            match conn.execute("COMMIT") {
                                Ok(_) => break,
                                Err(_e) => {
                                    let _ = conn.execute("ROLLBACK");
                                    local_retries += 1;
                                    attempts += 1;
                                    if attempts >= MAX_TXN_RETRIES {
                                        panic!("COMMIT failed after {MAX_TXN_RETRIES} retries");
                                    }
                                    thread::sleep(RETRY_BACKOFF);
                                }
                            }
                        }
                    }
                }

                retries.fetch_add(local_retries, Ordering::Relaxed);
                let elapsed = thread_start.elapsed();
                ThreadResult {
                    thread_id: tid,
                    rows_inserted: ROWS_PER_THREAD,
                    wall_ms: elapsed.as_millis() as u64,
                    wall_ns: elapsed.as_nanos().try_into().unwrap_or(u64::MAX),
                    retries: local_retries,
                }
            })
        })
        .collect();

    let per_thread: Vec<ThreadResult> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    let total_wall = start.elapsed();
    let total_rows = n_threads as i64 * ROWS_PER_THREAD;
    #[allow(clippy::cast_precision_loss)]
    let throughput = total_rows as f64 / total_wall.as_secs_f64();

    {
        let verify = rusqlite::Connection::open(tmp.path()).unwrap();
        for tid in 0..n_threads {
            let count: i64 = verify
                .query_row(&format!("SELECT COUNT(*) FROM bench_{tid}"), [], |r| {
                    r.get(0)
                })
                .unwrap();
            assert_eq!(
                count, ROWS_PER_THREAD,
                "fsqlite thread {tid} row count mismatch (rusqlite verification)"
            );
        }
    }

    BenchResult {
        engine: "fsqlite_mvcc".to_owned(),
        transaction_shape: transaction_shape.as_str().to_owned(),
        n_threads,
        rows_per_thread: ROWS_PER_THREAD,
        total_rows,
        total_wall_ms: total_wall.as_millis() as u64,
        throughput_ops_per_sec: throughput,
        total_retries: retry_total.load(Ordering::Relaxed),
        per_thread,
    }
}

// ── Structured JSON output ──────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize)]
struct ShowcaseReport {
    schema_version: String,
    bead_id: String,
    thread_counts: Vec<usize>,
    rows_per_thread: i64,
    results: Vec<BenchResult>,
    scaling_ratios: Vec<ScalingRatio>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ScalingRatio {
    n_threads: usize,
    transaction_shape: String,
    csqlite_throughput: f64,
    fsqlite_throughput: f64,
    ratio: f64,
}

fn write_report(dir: &Path, report: &ShowcaseReport) {
    let json = serde_json::to_string_pretty(report).expect("serialize report");
    fs::write(dir.join("concurrent_showcase.json"), json).expect("write report");
}

fn median(values: &mut [f64]) -> f64 {
    assert!(!values.is_empty(), "median requires at least one sample");
    values.sort_by(f64::total_cmp);
    values[values.len() / 2]
}

#[derive(Debug)]
struct PairedThroughputSample {
    sample: usize,
    order: &'static str,
    csqlite_ops_per_sec: f64,
    fsqlite_ops_per_sec: f64,
    ratio: f64,
}

// ── Tests ────────────────────────────────────────────────────────────────

#[test]
fn t1_csqlite_concurrent_writes_produce_correct_data() {
    let result = run_csqlite_concurrent(4, TransactionShape::PerWorker);
    assert_eq!(result.total_rows, 4 * ROWS_PER_THREAD);
    assert!(result.throughput_ops_per_sec > 0.0);
    assert_eq!(result.per_thread.len(), 4);
}

#[test]
fn t2_fsqlite_concurrent_writes_produce_correct_data() {
    let result = run_fsqlite_concurrent(4, TransactionShape::PerWorker);
    assert_eq!(result.total_rows, 4 * ROWS_PER_THREAD);
    assert!(result.throughput_ops_per_sec > 0.0);
    assert_eq!(result.per_thread.len(), 4);
}

#[test]
fn t3_structured_json_has_required_fields() {
    let dir = artifact_dir();
    let c_result = run_csqlite_concurrent(2, TransactionShape::PerWorker);
    let f_result = run_fsqlite_concurrent(2, TransactionShape::PerWorker);

    let report = ShowcaseReport {
        schema_version: "fsqlite-e2e.concurrent_showcase.v2".to_owned(),
        bead_id: "bd-wsw3p".to_owned(),
        thread_counts: vec![2],
        rows_per_thread: ROWS_PER_THREAD,
        results: vec![c_result, f_result],
        scaling_ratios: vec![],
    };
    write_report(&dir, &report);

    let raw = fs::read_to_string(dir.join("concurrent_showcase.json")).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&raw).unwrap();

    assert!(parsed["schema_version"].is_string());
    assert!(parsed["bead_id"].is_string());
    assert!(parsed["results"].is_array());
    let results = parsed["results"].as_array().unwrap();
    assert_eq!(results.len(), 2);

    for r in results {
        assert!(r["engine"].is_string());
        assert_eq!(r["transaction_shape"], "per_worker");
        assert!(r["n_threads"].is_number());
        assert!(r["throughput_ops_per_sec"].is_number());
        assert!(r["per_thread"].is_array());
        for t in r["per_thread"].as_array().unwrap() {
            assert!(t["thread_id"].is_number());
            assert!(t["rows_inserted"].is_number());
            assert!(t["wall_ms"].is_number());
            assert!(t["wall_ns"].is_number());
            assert!(t["retries"].is_number());
        }
    }
}

#[test]
fn median_uses_the_middle_order_statistic() {
    let mut values = [9.0, 1.0, 7.0, 3.0, 5.0];
    assert_eq!(median(&mut values), 5.0);
}

#[test]
#[cfg_attr(
    debug_assertions,
    ignore = "production performance gate; replay with cargo test --profile release-perf -p fsqlite-e2e --test bd_wsw3p_concurrent_write_showcase t4_fsqlite_outperforms_csqlite_at_4_threads -- --nocapture --test-threads=1"
)]
fn t4_fsqlite_outperforms_csqlite_at_4_threads() {
    // Warm both engines before collecting paired measurements. Alternate the
    // order within each pair so load and filesystem drift cannot
    // systematically benefit the engine that always runs second.
    let _ = run_fsqlite_concurrent(4, TransactionShape::PerWorker);
    let _ = run_csqlite_concurrent(4, TransactionShape::PerWorker);

    let mut samples = Vec::with_capacity(T4_SAMPLE_COUNT);
    let mut ratios = Vec::with_capacity(T4_SAMPLE_COUNT);
    let mut csqlite_throughputs = Vec::with_capacity(T4_SAMPLE_COUNT);
    let mut fsqlite_throughputs = Vec::with_capacity(T4_SAMPLE_COUNT);

    for sample in 0..T4_SAMPLE_COUNT {
        let (csqlite, fsqlite, order) = if sample % 2 == 0 {
            let fsqlite = run_fsqlite_concurrent(4, TransactionShape::PerWorker);
            let csqlite = run_csqlite_concurrent(4, TransactionShape::PerWorker);
            (csqlite, fsqlite, "fsqlite_first")
        } else {
            let csqlite = run_csqlite_concurrent(4, TransactionShape::PerWorker);
            let fsqlite = run_fsqlite_concurrent(4, TransactionShape::PerWorker);
            (csqlite, fsqlite, "csqlite_first")
        };
        let ratio =
            fsqlite.throughput_ops_per_sec / csqlite.throughput_ops_per_sec.max(f64::MIN_POSITIVE);
        csqlite_throughputs.push(csqlite.throughput_ops_per_sec);
        fsqlite_throughputs.push(fsqlite.throughput_ops_per_sec);
        ratios.push(ratio);
        samples.push(PairedThroughputSample {
            sample,
            order,
            csqlite_ops_per_sec: csqlite.throughput_ops_per_sec,
            fsqlite_ops_per_sec: fsqlite.throughput_ops_per_sec,
            ratio,
        });
    }

    let ratio_median = median(&mut ratios);
    let csqlite_median = median(&mut csqlite_throughputs);
    let fsqlite_median = median(&mut fsqlite_throughputs);
    let winning_samples = samples.iter().filter(|sample| sample.ratio > 1.0).count();

    eprintln!("canonical replay: {T4_REPLAY_CMD}");
    eprintln!(
        "4-thread per-worker-transaction median: csqlite={csqlite_median:.0} ops/s, \
         fsqlite={fsqlite_median:.0} ops/s, paired F/C={ratio_median:.2}x, \
         wins={winning_samples}/{T4_SAMPLE_COUNT}"
    );
    for sample in &samples {
        eprintln!(
            "sample {} ({}): csqlite={:.0} ops/s, fsqlite={:.0} ops/s, F/C={:.2}x",
            sample.sample,
            sample.order,
            sample.csqlite_ops_per_sec,
            sample.fsqlite_ops_per_sec,
            sample.ratio
        );
    }

    assert!(
        ratio_median > 1.0,
        "median FrankenSQLite 4-thread throughput must exceed C SQLite: \
         F/C={ratio_median:.2}x, wins={winning_samples}/{T4_SAMPLE_COUNT}, samples={samples:?}"
    );
}

#[test]
fn t5_showcase_4_8_produces_labeled_artifact_bundle() {
    let dir = artifact_dir();
    let thread_counts = vec![4, 8];

    let mut results = Vec::new();
    let mut scaling_ratios = Vec::new();

    for &n in &thread_counts {
        for transaction_shape in [TransactionShape::PerWorker, TransactionShape::PerRow] {
            let c = run_csqlite_concurrent(n, transaction_shape);
            let f = run_fsqlite_concurrent(n, transaction_shape);
            let ratio = f.throughput_ops_per_sec / c.throughput_ops_per_sec.max(1.0);
            scaling_ratios.push(ScalingRatio {
                n_threads: n,
                transaction_shape: transaction_shape.as_str().to_owned(),
                csqlite_throughput: c.throughput_ops_per_sec,
                fsqlite_throughput: f.throughput_ops_per_sec,
                ratio,
            });
            results.push(c);
            results.push(f);
        }
    }

    let report = ShowcaseReport {
        schema_version: "fsqlite-e2e.concurrent_showcase.v2".to_owned(),
        bead_id: "bd-wsw3p".to_owned(),
        thread_counts: thread_counts.clone(),
        rows_per_thread: ROWS_PER_THREAD,
        results,
        scaling_ratios: scaling_ratios.clone(),
    };
    write_report(&dir, &report);

    assert!(dir.join("concurrent_showcase.json").exists());
    assert!(
        !scaling_ratios.is_empty(),
        "must produce scaling ratio evidence"
    );

    eprintln!("\n=== Concurrent Write Showcase ===");
    for sr in &scaling_ratios {
        eprintln!(
            "  {}t / {}: csqlite={:.0} ops/s, fsqlite={:.0} ops/s, ratio={:.2}x{}",
            sr.n_threads,
            sr.transaction_shape,
            sr.csqlite_throughput,
            sr.fsqlite_throughput,
            sr.ratio,
            if sr.transaction_shape == TransactionShape::PerRow.as_str() {
                " (non-asserting diagnostic)"
            } else {
                ""
            }
        );
    }
    eprintln!("Artifacts: {}", dir.display());
}

#[test]
fn t6_rusqlite_verification_catches_data_on_fsqlite_db() {
    let result = run_fsqlite_concurrent(2, TransactionShape::PerWorker);
    assert_eq!(result.per_thread.len(), 2);
    for t in &result.per_thread {
        assert_eq!(t.rows_inserted, ROWS_PER_THREAD);
    }
}

#[test]
fn t7_each_thread_reports_nonzero_wall_time() {
    let result = run_fsqlite_concurrent(4, TransactionShape::PerWorker);
    for t in &result.per_thread {
        assert!(
            t.wall_ns > 0,
            "thread {} wall_ns should be > 0 for {} rows",
            t.thread_id,
            ROWS_PER_THREAD
        );
    }
}
