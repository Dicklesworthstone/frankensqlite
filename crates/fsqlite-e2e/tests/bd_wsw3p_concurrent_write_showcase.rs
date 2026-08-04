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
// These end-to-end tests deliberately hold a thread-local Connection across
// awaits and exercise the full unboxed engine future. Making those futures
// Send or boxing them would change the workload this benchmark measures.
#![allow(clippy::future_not_send, clippy::large_futures)]
#![recursion_limit = "512"]

use std::fmt::Write as _;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use fsqlite::SqliteValue;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const ROWS_PER_THREAD: i64 = 200;
const MAX_TXN_RETRIES: u32 = 100;
const RETRY_BACKOFF: Duration = Duration::from_micros(100);
const T4_SAMPLE_COUNT: usize = 11;
const T4_REPLAY_CMD: &str = "cargo test --locked --profile release-perf --package fsqlite-e2e --test bd_wsw3p_concurrent_write_showcase t4_fsqlite_outperforms_csqlite_at_4_threads -- --exact --ignored --nocapture --test-threads=1";

/// Writer count for the 16-thread cross-engine release gate.
const T16_THREADS: usize = 16;
/// Even, fixed paired-sample count so each engine runs first exactly half of
/// the time. The distribution-free lower bound below remains an order
/// statistic; the descriptive median is the mean of the two central ratios.
const T16_SAMPLE_COUNT: usize = 22;
/// One-sided error budget for the distribution-free median bound, as an exact
/// rational so the order statistic is derived by integer arithmetic only.
const T16_ALPHA_NUM: u128 = 5;
const T16_ALPHA_DEN: u128 = 100;
const T16_CONFIDENCE_PERCENT: u8 = 95;
/// Declared acceptance threshold: the lower confidence bound on the paired
/// FrankenSQLite-to-C-SQLite throughput ratio must exceed this value.
const T16_MIN_RATIO_LOWER_BOUND: f64 = 1.0;
/// The only build profile whose numbers may satisfy the run-for-release
/// contract. A debug or plain release build must fail closed.
const T16_REQUIRED_SELECTED_PROFILE: &str = "release-perf";
const T16_REPLAY_CMD: &str = "cargo test --locked --profile release-perf --package fsqlite-e2e --test bd_wsw3p_concurrent_write_showcase t16_fsqlite_outperforms_csqlite_at_16_threads -- --exact --ignored --nocapture --test-threads=1";

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

async fn run_fsqlite_concurrent(
    n_threads: usize,
    transaction_shape: TransactionShape,
) -> BenchResult {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let path_str = tmp.path().to_str().unwrap().to_owned();

    {
        let conn = fsqlite::Connection::open(&path_str).await.unwrap();
        conn.execute("PRAGMA page_size = 4096;").await.unwrap();
        conn.execute("PRAGMA journal_mode = WAL;").await.unwrap();
        conn.execute("PRAGMA synchronous = NORMAL;").await.unwrap();
        conn.execute("PRAGMA cache_size = -64000;").await.unwrap();
        for tid in 0..n_threads {
            conn.execute(&format!(
                "CREATE TABLE bench_{tid} (id INTEGER PRIMARY KEY, name TEXT, score INTEGER);"
            ))
            .await
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
                let mut thread_result: Option<ThreadResult> = None;
                asupersync::test_utils::run_test(|| async {
                    let conn = fsqlite::Connection::open(&p).await.unwrap();
                    conn.execute("PRAGMA journal_mode = WAL;").await.unwrap();
                    conn.execute("PRAGMA synchronous = NORMAL;").await.unwrap();
                    conn.execute("PRAGMA cache_size = -64000;").await.unwrap();
                    conn.execute("PRAGMA busy_timeout = 5000;").await.unwrap();
                    conn.execute("PRAGMA fsqlite.concurrent_mode = ON;")
                        .await
                        .unwrap();

                    let insert_sql =
                        format!("INSERT INTO bench_{tid} VALUES (?1, ('t' || ?1), (?1 * 7));");
                    let stmt = conn.prepare(&insert_sql).await.unwrap();
                    bar.wait();

                    let thread_start = Instant::now();
                    let mut local_retries: u64 = 0;

                    if transaction_shape == TransactionShape::PerWorker {
                        let mut attempts = 0u32;
                        'transaction: loop {
                            if let Err(_e) = conn.execute("BEGIN CONCURRENT").await {
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

                            for i in 0..ROWS_PER_THREAD {
                                match stmt.execute_with_params(&[SqliteValue::Integer(i)]).await {
                                    Ok(_) => {}
                                    Err(_e) => {
                                        drop(conn.execute("ROLLBACK").await);
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

                            match conn.execute("COMMIT").await {
                                Ok(_) => break 'transaction,
                                Err(_e) => {
                                    drop(conn.execute("ROLLBACK").await);
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
                                if let Err(_e) = conn.execute("BEGIN CONCURRENT").await {
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

                                match stmt.execute_with_params(&[SqliteValue::Integer(i)]).await {
                                    Ok(_) => {}
                                    Err(_e) => {
                                        drop(conn.execute("ROLLBACK").await);
                                        local_retries += 1;
                                        attempts += 1;
                                        if attempts >= MAX_TXN_RETRIES {
                                            panic!("INSERT failed after {MAX_TXN_RETRIES} retries");
                                        }
                                        thread::sleep(RETRY_BACKOFF);
                                        continue;
                                    }
                                }

                                match conn.execute("COMMIT").await {
                                    Ok(_) => break,
                                    Err(_e) => {
                                        drop(conn.execute("ROLLBACK").await);
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
                    thread_result = Some(ThreadResult {
                        thread_id: tid,
                        rows_inserted: ROWS_PER_THREAD,
                        wall_ms: elapsed.as_millis() as u64,
                        wall_ns: elapsed.as_nanos().try_into().unwrap_or(u64::MAX),
                        retries: local_retries,
                    });
                });
                thread_result.expect("fsqlite worker thread produced a result")
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
    let middle = values.len() / 2;
    if values.len() % 2 == 0 {
        f64::midpoint(values[middle - 1], values[middle])
    } else {
        values[middle]
    }
}

#[derive(Debug)]
struct PairedThroughputSample {
    sample: usize,
    order: &'static str,
    csqlite_ops_per_sec: f64,
    fsqlite_ops_per_sec: f64,
    ratio: f64,
}

/// One counterbalanced 16-thread pair, carrying the per-sample correctness
/// evidence alongside the throughput measurement. A sample is only admissible
/// when both engines committed exactly the expected row count.
#[derive(Debug)]
struct PairedCorrectnessSample {
    sample: usize,
    order: &'static str,
    csqlite_ops_per_sec: f64,
    fsqlite_ops_per_sec: f64,
    ratio: f64,
    csqlite_total_rows: i64,
    fsqlite_total_rows: i64,
    expected_total_rows: i64,
}

impl PairedCorrectnessSample {
    const fn rows_agree(&self) -> bool {
        self.csqlite_total_rows == self.expected_total_rows
            && self.fsqlite_total_rows == self.expected_total_rows
    }
}

/// One-based order statistic of the ascending paired ratios that is a
/// distribution-free lower confidence bound for the true median at confidence
/// `1 - alpha`.
///
/// This is the sign-test bound: the `k`-th smallest of `n` exchangeable
/// samples bounds the median from below with confidence
/// `1 - P(Binomial(n, 1/2) <= k - 1)`. The cumulative mass is accumulated as
/// exact integers scaled by `2^n`, so the returned index is deterministic and
/// free of floating-point drift. Returns `None` when no order statistic
/// achieves the requested confidence at this sample count.
fn median_lower_bound_order_statistic(n: usize, alpha_num: u128, alpha_den: u128) -> Option<usize> {
    assert!(n > 0 && n < 100, "sample count out of supported range");
    assert!(
        alpha_den > 0 && alpha_num < alpha_den,
        "alpha must be a proper fraction in [0, 1)"
    );
    let total: u128 = 1u128 << n;
    let mut cumulative: u128 = 0;
    let mut coefficient: u128 = 1;
    let mut best: Option<usize> = None;
    for i in 0..=n {
        cumulative += coefficient;
        if cumulative * alpha_den <= alpha_num * total {
            best = Some(i + 1);
        } else {
            break;
        }
        let n_u128 = u128::try_from(n).expect("supported sample count fits u128");
        let i_u128 = u128::try_from(i).expect("sample index fits u128");
        coefficient = coefficient * (n_u128 - i_u128) / (i_u128 + 1);
    }
    best
}

/// Absolute path and streaming SHA-256 digest of the test binary executing now.
///
/// The digest is computed in-process from the workspace `sha2` dependency so
/// identity is portable across platforms and needs no external tool. Fails
/// closed: a gate that cannot name the exact binary it measured is not
/// admissible evidence, so an unresolvable path or any read error panics
/// rather than degrading to an unidentified run.
fn running_binary_identity() -> (PathBuf, String) {
    let exe = std::env::current_exe().expect("running test binary path must be resolvable");
    let mut file = fs::File::open(&exe).unwrap_or_else(|error| {
        panic!(
            "running test binary {} must be readable for identity: {error}",
            exe.display()
        )
    });
    let mut hasher = Sha256::new();
    let mut buffer = vec![0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer).unwrap_or_else(|error| {
            panic!(
                "running test binary {} must stream cleanly for identity: {error}",
                exe.display()
            )
        });
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    let mut digest = String::with_capacity(64);
    for byte in hasher.finalize() {
        write!(&mut digest, "{byte:02x}").expect("writing to a String cannot fail");
    }
    assert_eq!(
        digest.len(),
        64,
        "running-binary SHA-256 must be 64 hexadecimal digits, found `{digest}`"
    );
    (exe, digest)
}

/// Machine identity of the host actually executing the measurement.
///
/// The Rust `HOST`/`TARGET` triples describe the build, not the machine, so a
/// bundle carrying only those cannot distinguish two runs on different hosts.
/// Prefer the process environment, then the standard Unix hostname file, then
/// the platform `hostname` command. Fails closed when none provides a non-empty
/// value, because an unattributable measurement is not admissible evidence.
fn runtime_machine_identity() -> String {
    for name in ["HOSTNAME", "COMPUTERNAME"] {
        if let Ok(value) = std::env::var(name) {
            let trimmed = value.trim();
            if !trimmed.is_empty() {
                return trimmed.to_owned();
            }
        }
    }
    if let Ok(value) = fs::read_to_string("/etc/hostname") {
        let trimmed = value.trim();
        if !trimmed.is_empty() {
            return trimmed.to_owned();
        }
    }
    if let Ok(output) = Command::new("hostname").output()
        && output.status.success()
        && let Ok(value) = String::from_utf8(output.stdout)
    {
        let trimmed = value.trim();
        if !trimmed.is_empty() {
            return trimmed.to_owned();
        }
    }
    panic!(
        "16-thread evidence requires runtime machine identity: HOSTNAME, \
         COMPUTERNAME, /etc/hostname, and the hostname command were unavailable \
         or empty"
    );
}

/// Assert that a build-provenance value is present and resolved.
fn require_resolved_provenance(name: &str, value: &str) {
    assert!(
        !value.is_empty() && value != "unknown",
        "16-thread evidence requires resolved build provenance: {name}=`{value}`"
    );
}

/// Assert that a hex-encoded provenance value is well formed. An empty value is
/// admissible and meaningful: it records that the corresponding environment was
/// genuinely unset at build time.
fn require_hex_provenance(name: &str, value: &str) {
    assert!(
        value.len() % 2 == 0 && value.chars().all(|c| c.is_ascii_hexdigit()),
        "build provenance {name} must be even-length hexadecimal, found `{value}`"
    );
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
    asupersync::test_utils::run_test(|| async {
        let result = run_fsqlite_concurrent(4, TransactionShape::PerWorker).await;
        assert_eq!(result.total_rows, 4 * ROWS_PER_THREAD);
        assert!(result.throughput_ops_per_sec > 0.0);
        assert_eq!(result.per_thread.len(), 4);
    });
}

#[test]
fn t3_structured_json_has_required_fields() {
    asupersync::test_utils::run_test(|| async {
        let dir = artifact_dir();
        let c_result = run_csqlite_concurrent(2, TransactionShape::PerWorker);
        let f_result = run_fsqlite_concurrent(2, TransactionShape::PerWorker).await;

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
    });
}

#[test]
fn median_handles_odd_and_even_sample_counts() {
    let mut odd = [9.0, 1.0, 7.0, 3.0, 5.0];
    assert_eq!(median(&mut odd), 5.0);

    let mut even = [9.0, 1.0, 7.0, 3.0];
    assert_eq!(median(&mut even), 5.0);
}

#[test]
#[ignore = "production performance gate; replay with cargo test --locked --profile release-perf --package fsqlite-e2e --test bd_wsw3p_concurrent_write_showcase t4_fsqlite_outperforms_csqlite_at_4_threads -- --exact --ignored --nocapture --test-threads=1"]
fn t4_fsqlite_outperforms_csqlite_at_4_threads() {
    // Warm both engines before collecting paired measurements. Alternate the
    // order within each pair so load and filesystem drift cannot
    // systematically benefit the engine that always runs second.
    asupersync::test_utils::run_test(|| async {
        drop(run_fsqlite_concurrent(4, TransactionShape::PerWorker).await);
        let _ = run_csqlite_concurrent(4, TransactionShape::PerWorker);

        let mut samples = Vec::with_capacity(T4_SAMPLE_COUNT);
        let mut ratios = Vec::with_capacity(T4_SAMPLE_COUNT);
        let mut csqlite_throughputs = Vec::with_capacity(T4_SAMPLE_COUNT);
        let mut fsqlite_throughputs = Vec::with_capacity(T4_SAMPLE_COUNT);

        for sample in 0..T4_SAMPLE_COUNT {
            let (csqlite, fsqlite, order) = if sample % 2 == 0 {
                let fsqlite = run_fsqlite_concurrent(4, TransactionShape::PerWorker).await;
                let csqlite = run_csqlite_concurrent(4, TransactionShape::PerWorker);
                (csqlite, fsqlite, "fsqlite_first")
            } else {
                let csqlite = run_csqlite_concurrent(4, TransactionShape::PerWorker);
                let fsqlite = run_fsqlite_concurrent(4, TransactionShape::PerWorker).await;
                (csqlite, fsqlite, "csqlite_first")
            };
            let ratio = fsqlite.throughput_ops_per_sec
                / csqlite.throughput_ops_per_sec.max(f64::MIN_POSITIVE);
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
    });
}

#[test]
fn t16_order_statistic_bound_is_the_sign_test_index() {
    // 22 samples, alpha = 5/100: cumulative Binomial(22, 1/2) mass through
    // i = 6 is 110056/4194304 < 0.05, and through i = 7 is 280600/4194304 > 0.05,
    // so the 7th smallest ratio is the lower bound.
    assert_eq!(
        median_lower_bound_order_statistic(22, 5, 100),
        Some(7),
        "sign-test order statistic must be derived, not assumed"
    );
    assert_eq!(median_lower_bound_order_statistic(1, 5, 100), None);
}

#[test]
#[ignore = "production 16-thread cross-engine performance gate; replay with cargo test --locked --profile release-perf --package fsqlite-e2e --test bd_wsw3p_concurrent_write_showcase t16_fsqlite_outperforms_csqlite_at_16_threads -- --exact --ignored --nocapture --test-threads=1"]
fn t16_fsqlite_outperforms_csqlite_at_16_threads() {
    // Identity first: a gate that cannot name the exact source and binary it
    // measured is not admissible evidence, so resolve provenance before any
    // measurement and fail closed when it is incomplete.
    let source_sha = env!("FSQLITE_BENCH_BUILD_GIT_SHA");
    let source_branch = env!("FSQLITE_BENCH_BUILD_GIT_BRANCH");
    let source_dirty = env!("FSQLITE_BENCH_BUILD_GIT_DIRTY");
    let build_features = env!("FSQLITE_BENCH_BUILD_FEATURES");
    let build_host = env!("FSQLITE_BENCH_BUILD_HOST");
    let build_target = env!("FSQLITE_BENCH_BUILD_TARGET");
    let build_profile = env!("FSQLITE_BENCH_BUILD_PROFILE");
    let build_selected_profile = env!("FSQLITE_BENCH_BUILD_SELECTED_PROFILE");
    let rustc_version = env!("FSQLITE_BENCH_BUILD_RUSTC_VERSION");
    let cargo_version = env!("FSQLITE_BENCH_BUILD_CARGO_VERSION");
    let rustflags_hex = env!("FSQLITE_BENCH_BUILD_RUSTFLAGS_HEX");
    let encoded_rustflags_present = env!("FSQLITE_BENCH_BUILD_ENCODED_RUSTFLAGS_PRESENT");
    let profile_overrides_hex = env!("FSQLITE_BENCH_BUILD_PROFILE_OVERRIDES_HEX");
    let native_overrides_hex = env!("FSQLITE_BENCH_BUILD_NATIVE_OVERRIDES_HEX");
    let feature_graph_sha256 = env!("FSQLITE_BENCH_BUILD_RESOLVED_DEPENDENCY_FEATURE_GRAPH_SHA256");
    let input_tracking = env!("FSQLITE_BENCH_BUILD_INPUT_TRACKING");

    assert_eq!(
        source_sha.len(),
        40,
        "current-source identity requires a full 40-digit commit, found `{source_sha}`"
    );
    assert!(
        source_sha.chars().all(|c| c.is_ascii_hexdigit()),
        "current-source identity must be hexadecimal, found `{source_sha}`"
    );
    assert_eq!(
        source_dirty, "false",
        "16-thread evidence requires a clean worktree at build time \
         (FSQLITE_BENCH_BUILD_GIT_DIRTY=`{source_dirty}`)"
    );
    require_resolved_provenance("FSQLITE_BENCH_BUILD_HOST", build_host);
    require_resolved_provenance("FSQLITE_BENCH_BUILD_TARGET", build_target);
    require_resolved_provenance("FSQLITE_BENCH_BUILD_PROFILE", build_profile);
    // The run_for_release contract is a release-perf contract. A debug or
    // plain release build must not be able to satisfy it, so pin the selected
    // profile exactly rather than merely requiring it to be resolved.
    assert_eq!(
        build_selected_profile, T16_REQUIRED_SELECTED_PROFILE,
        "16-thread evidence must come from a `{T16_REQUIRED_SELECTED_PROFILE}` build \
         (FSQLITE_BENCH_BUILD_SELECTED_PROFILE=`{build_selected_profile}`)"
    );
    require_resolved_provenance("FSQLITE_BENCH_BUILD_RUSTC_VERSION", rustc_version);
    require_resolved_provenance("FSQLITE_BENCH_BUILD_CARGO_VERSION", cargo_version);
    require_hex_provenance("FSQLITE_BENCH_BUILD_RUSTFLAGS_HEX", rustflags_hex);
    require_hex_provenance(
        "FSQLITE_BENCH_BUILD_PROFILE_OVERRIDES_HEX",
        profile_overrides_hex,
    );
    require_hex_provenance(
        "FSQLITE_BENCH_BUILD_NATIVE_OVERRIDES_HEX",
        native_overrides_hex,
    );
    assert!(
        matches!(encoded_rustflags_present, "true" | "false"),
        "FSQLITE_BENCH_BUILD_ENCODED_RUSTFLAGS_PRESENT must be a boolean flag, \
         found `{encoded_rustflags_present}`"
    );
    assert!(
        feature_graph_sha256.len() == 64
            && feature_graph_sha256
                .chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()),
        "resolved dependency feature-graph digest must be 64 lowercase \
         hexadecimal digits, found `{feature_graph_sha256}`"
    );
    assert_eq!(
        input_tracking, "complete",
        "16-thread evidence requires complete tracked-source rerun directives \
         (FSQLITE_BENCH_BUILD_INPUT_TRACKING=`{input_tracking}`)"
    );
    let (binary_path, binary_sha256) = running_binary_identity();
    let runtime_machine = runtime_machine_identity();

    let bound_index =
        median_lower_bound_order_statistic(T16_SAMPLE_COUNT, T16_ALPHA_NUM, T16_ALPHA_DEN)
            .expect("sample count must admit a distribution-free median bound");

    asupersync::test_utils::run_test(|| async move {
        let expected_total_rows =
            i64::try_from(T16_THREADS).expect("thread count fits i64") * ROWS_PER_THREAD;

        // Warm both engines before collecting paired measurements.
        drop(run_fsqlite_concurrent(T16_THREADS, TransactionShape::PerWorker).await);
        drop(run_csqlite_concurrent(
            T16_THREADS,
            TransactionShape::PerWorker,
        ));

        let mut samples = Vec::with_capacity(T16_SAMPLE_COUNT);
        let mut ratios = Vec::with_capacity(T16_SAMPLE_COUNT);

        for sample in 0..T16_SAMPLE_COUNT {
            // Alternate order within each pair so load and filesystem drift
            // cannot systematically benefit the engine that always runs second.
            let (csqlite, fsqlite, order) = if sample % 2 == 0 {
                let fsqlite =
                    run_fsqlite_concurrent(T16_THREADS, TransactionShape::PerWorker).await;
                let csqlite = run_csqlite_concurrent(T16_THREADS, TransactionShape::PerWorker);
                (csqlite, fsqlite, "fsqlite_first")
            } else {
                let csqlite = run_csqlite_concurrent(T16_THREADS, TransactionShape::PerWorker);
                let fsqlite =
                    run_fsqlite_concurrent(T16_THREADS, TransactionShape::PerWorker).await;
                (csqlite, fsqlite, "csqlite_first")
            };
            assert!(
                csqlite.throughput_ops_per_sec.is_finite() && csqlite.throughput_ops_per_sec > 0.0,
                "sample {sample} produced inadmissible C SQLite throughput: {}",
                csqlite.throughput_ops_per_sec
            );
            assert!(
                fsqlite.throughput_ops_per_sec.is_finite() && fsqlite.throughput_ops_per_sec > 0.0,
                "sample {sample} produced inadmissible FrankenSQLite throughput: {}",
                fsqlite.throughput_ops_per_sec
            );
            let ratio = fsqlite.throughput_ops_per_sec / csqlite.throughput_ops_per_sec;
            assert!(
                ratio.is_finite() && ratio > 0.0,
                "sample {sample} produced inadmissible F/C throughput ratio: {ratio}"
            );
            ratios.push(ratio);
            samples.push(PairedCorrectnessSample {
                sample,
                order,
                csqlite_ops_per_sec: csqlite.throughput_ops_per_sec,
                fsqlite_ops_per_sec: fsqlite.throughput_ops_per_sec,
                ratio,
                csqlite_total_rows: csqlite.total_rows,
                fsqlite_total_rows: fsqlite.total_rows,
                expected_total_rows,
            });
        }

        // Correctness gates the measurement: a throughput number taken from a
        // diverged database is not evidence, so every sample must agree with
        // the expected committed row count on both engines.
        for sample in &samples {
            assert!(
                sample.rows_agree(),
                "sample {} ({}) failed the per-sample correctness oracle: \
                 csqlite_rows={}, fsqlite_rows={}, expected={}",
                sample.sample,
                sample.order,
                sample.csqlite_total_rows,
                sample.fsqlite_total_rows,
                sample.expected_total_rows
            );
        }

        let mut sorted_ratios = ratios.clone();
        sorted_ratios.sort_by(f64::total_cmp);
        let ratio_lower_bound = sorted_ratios[bound_index - 1];
        let ratio_median = median(&mut ratios);
        let winning_samples = samples.iter().filter(|sample| sample.ratio > 1.0).count();
        eprintln!("canonical replay: {T16_REPLAY_CMD}");
        eprintln!(
            "provenance/source: sha={source_sha} branch={source_branch} \
             dirty={source_dirty} features={build_features} \
             input_tracking={input_tracking}"
        );
        eprintln!(
            "provenance/toolchain: host={build_host} target={build_target} \
             profile={build_profile} selected_profile={build_selected_profile} \
             rustc={rustc_version:?} cargo={cargo_version:?}"
        );
        eprintln!(
            "provenance/flags: rustflags_hex={rustflags_hex} \
             encoded_rustflags_present={encoded_rustflags_present} \
             profile_overrides_hex={profile_overrides_hex} \
             native_overrides_hex={native_overrides_hex} \
             feature_graph_sha256={feature_graph_sha256}"
        );
        eprintln!(
            "provenance/binary: path={} sha256={binary_sha256}",
            binary_path.display()
        );
        // Build triples describe the compiler's view; this names the machine
        // that produced these numbers. RCH transcript worker identity stays
        // separate outer evidence and is not asserted here.
        eprintln!("provenance/runtime: machine={runtime_machine}");
        eprintln!(
            "16-thread per-worker-transaction: median F/C={ratio_median:.4}x, \
             lower bound (order statistic {bound_index} of {T16_SAMPLE_COUNT}, \
             >={T16_CONFIDENCE_PERCENT}% confidence)={ratio_lower_bound:.4}x, \
             threshold={T16_MIN_RATIO_LOWER_BOUND:.4}x, \
             wins={winning_samples}/{T16_SAMPLE_COUNT}, \
             expected_rows_per_sample={expected_total_rows}"
        );
        for sample in &samples {
            eprintln!(
                "sample {} ({}): csqlite={:.0} ops/s, fsqlite={:.0} ops/s, \
                 F/C={:.4}x, rows={}/{}",
                sample.sample,
                sample.order,
                sample.csqlite_ops_per_sec,
                sample.fsqlite_ops_per_sec,
                sample.ratio,
                sample.csqlite_total_rows,
                sample.fsqlite_total_rows
            );
        }

        assert!(
            ratio_lower_bound > T16_MIN_RATIO_LOWER_BOUND,
            "the {T16_CONFIDENCE_PERCENT}% distribution-free lower bound on the paired \
             16-thread FrankenSQLite-to-C-SQLite throughput ratio must exceed \
             {T16_MIN_RATIO_LOWER_BOUND:.4}x: bound={ratio_lower_bound:.4}x, \
             median={ratio_median:.4}x, wins={winning_samples}/{T16_SAMPLE_COUNT}, \
             samples={samples:?}"
        );
    });
}

#[test]
fn t5_showcase_4_8_produces_labeled_artifact_bundle() {
    asupersync::test_utils::run_test(|| async {
        let dir = artifact_dir();
        let thread_counts = vec![4, 8];

        let mut results = Vec::new();
        let mut scaling_ratios = Vec::new();

        for &n in &thread_counts {
            for transaction_shape in [TransactionShape::PerWorker, TransactionShape::PerRow] {
                let c = run_csqlite_concurrent(n, transaction_shape);
                let f = run_fsqlite_concurrent(n, transaction_shape).await;
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
    });
}

#[test]
fn t6_rusqlite_verification_catches_data_on_fsqlite_db() {
    asupersync::test_utils::run_test(|| async {
        let result = run_fsqlite_concurrent(2, TransactionShape::PerWorker).await;
        assert_eq!(result.per_thread.len(), 2);
        for t in &result.per_thread {
            assert_eq!(t.rows_inserted, ROWS_PER_THREAD);
        }
    });
}

#[test]
fn t7_each_thread_reports_nonzero_wall_time() {
    asupersync::test_utils::run_test(|| async {
        let result = run_fsqlite_concurrent(4, TransactionShape::PerWorker).await;
        for t in &result.per_thread {
            assert!(
                t.wall_ns > 0,
                "thread {} wall_ns should be > 0 for {} rows",
                t.thread_id,
                ROWS_PER_THREAD
            );
        }
    });
}
