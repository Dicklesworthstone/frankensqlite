//! D-TEST: historical contention-elimination suite + placeholder gates (bd-3wop3.7).
//!
//! As of 2026-03-23 this file is **not** the canonical threshold authority for
//! the overlay scorecard. The truthful benchmark surface lives in:
//! - `crates/fsqlite-e2e/benches/concurrent_write_persistent_bench.rs`
//! - `artifacts/perf/2026-03-23-local/canonical_{mvcc,single_writer}.{md,jsonl}`
//! - the still-blocked governance lane (`bd-db300.1.7.4`, `bd-db300.7.9.1`,
//!   `bd-3wop3.1.5`) that owns final c1/4/8 and persistent 2/4/8/16 gate truth
//!
//! The ignored throughput gates in this file therefore remain historical
//! scaffolding only; they must not be read as current pass/fail policy.
//! Operators should use `scripts/capture_c1_evidence_pack.sh` for the c1 truth
//! surface and `scripts/capture_persistent_phase_pack.sh` for the persistent
//! 8t/16t truth surface and same-pack comparator provenance.
//!
//! ## Contention Tests
//! 1. test_no_global_locks_in_commit_fast_path
//! 2. test_parallel_wal_segments_independent (D1 dependency)
//! 3. test_page_cache_shard_distribution (D2 dependency)
//! 4. test_combiner_reduces_atomic_ops (D3 dependency)
//! 5. test_ebr_no_gc_pauses (D5 proof-surface blocker)
//! 6. test_scaling_curve
//!
//! ## Stress Tests
//! 7. test_64_thread_no_deadlock
//! 8. test_sustained_insert_p99_latency
//! 9. test_contention_under_version_churn
//!
//! ## Dependencies
//! - D1: Parallel WAL with per-thread log buffers
//! - D2: Sharded PageCache (128 partitions)
//! - D3: Flat Combining for commit sequencer
//! - D5: Epoch-Based Reclamation for MVCC GC
//!
//! Run the active deterministic keeper with:
//! ```sh
//! cargo test -p fsqlite-e2e --test bd_3wop3_7_contention_elimination test_page_cache_shard_distribution -- --exact
//! ```
//! Run each manual stress keeper by its exact name with `--ignored --exact`.
//! Do not run every ignored test as a group: the explicit release-blocker
//! stubs intentionally panic until their proof surfaces exist.
#![recursion_limit = "512"]

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};
use std::{env, process::Command};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Thread counts for scaling curve analysis.
const SCALING_THREAD_COUNTS: &[usize] = &[1, 2, 4, 8, 16];

/// Rows per thread for throughput tests.
const ROWS_PER_THREAD: u64 = 10_000;

/// Historical 8-thread placeholder gate from the pre-overlay contention file.
const HISTORICAL_PLACEHOLDER_8T_SPEEDUP: f64 = 1.5;

/// Historical 16-thread placeholder gate from the pre-overlay contention file.
const HISTORICAL_PLACEHOLDER_16T_SPEEDUP: f64 = 1.0;

/// A same-table concurrent insert can legitimately observe an advertised
/// concurrent-write contention result. Retry the idempotent `INSERT OR
/// REPLACE` against a fresh snapshot, but keep a finite budget so a stuck
/// writer remains a hard test failure.
const SUSTAINED_INSERT_MAX_RETRIES: u32 = 128;
const SUSTAINED_INSERT_RETRY_BASE_US: u64 = 25;
const SUSTAINED_INSERT_RETRY_CAP_US: u64 = 2_000;
const SUSTAINED_INSERT_RETRY_JITTER_US: u64 = 251;
const SUSTAINED_INSERT_WORKERS: usize = 4;
const SUSTAINED_INSERT_MIN_SUCCESSES_PER_WORKER: usize = 100;
const SUSTAINED_INSERT_MAX_RETRIES_PER_SUCCESS: u64 = 4;
const SUSTAINED_INSERT_P99_LIMIT_US: u64 = 10_000;
const SUSTAINED_INSERT_MAX_LATENCY_LIMIT_US: u64 = 500_000;
const SUSTAINED_INSERT_MIN_PROGRESS_RATIO_DENOMINATOR: usize = 4;

#[derive(Clone, Debug, Default)]
struct SustainedInsertWorkerMetrics {
    latencies_us: Vec<u64>,
    successful_ids: Vec<i64>,
    retry_attempts: u64,
}

fn sqlite_i64(value: u64) -> i64 {
    i64::try_from(value).expect("test workload values fit SQLite INTEGER")
}

fn is_expected_contention_error(error: &fsqlite::FrankenError) -> bool {
    matches!(
        error,
        fsqlite::FrankenError::Busy
            | fsqlite::FrankenError::BusyRecovery
            | fsqlite::FrankenError::BusySnapshot { .. }
            | fsqlite::FrankenError::DatabaseLocked { .. }
    )
}

fn is_sustained_insert_retryable_contention(error: &fsqlite::FrankenError) -> bool {
    matches!(
        error,
        fsqlite::FrankenError::Busy
            | fsqlite::FrankenError::BusyRecovery
            | fsqlite::FrankenError::BusySnapshot { .. }
            | fsqlite::FrankenError::DatabaseLocked { .. }
            | fsqlite::FrankenError::WriteConflict { .. }
            | fsqlite::FrankenError::SerializationFailure { .. }
    )
}

fn sustained_insert_retry_delay(attempt: u32, worker_id: u64) -> Duration {
    let shift = attempt.min(7);
    let base_us = (SUSTAINED_INSERT_RETRY_BASE_US << shift).min(SUSTAINED_INSERT_RETRY_CAP_US);
    let jitter_us = worker_id
        .wrapping_mul(37)
        .wrapping_add(u64::from(attempt).wrapping_mul(17))
        % SUSTAINED_INSERT_RETRY_JITTER_US;
    Duration::from_micros(base_us.saturating_add(jitter_us))
}

#[test]
fn sustained_insert_retry_delay_is_bounded_and_worker_staggered() {
    assert_eq!(
        sustained_insert_retry_delay(0, 0),
        Duration::from_micros(SUSTAINED_INSERT_RETRY_BASE_US)
    );
    assert_ne!(
        sustained_insert_retry_delay(3, 0),
        sustained_insert_retry_delay(3, 1)
    );
    assert!(
        sustained_insert_retry_delay(SUSTAINED_INSERT_MAX_RETRIES, 0)
            >= Duration::from_micros(SUSTAINED_INSERT_RETRY_CAP_US)
    );
    for attempt in 0..=SUSTAINED_INSERT_MAX_RETRIES {
        for worker_id in 0..SUSTAINED_INSERT_WORKERS {
            assert!(
                sustained_insert_retry_delay(
                    attempt,
                    u64::try_from(worker_id).expect("worker id fits u64"),
                )
                    <= Duration::from_micros(
                        SUSTAINED_INSERT_RETRY_CAP_US + SUSTAINED_INSERT_RETRY_JITTER_US - 1,
                    )
            );
        }
    }

    assert!(is_sustained_insert_retryable_contention(
        &fsqlite::FrankenError::WriteConflict { page: 1, holder: 2 }
    ));
    assert!(!is_sustained_insert_retryable_contention(
        &fsqlite::FrankenError::PageBufferCapacityExhausted {
            operation: "sustained_insert_keeper",
            page_size: 4_096,
            max_buffers: 8,
            total_buffers: 8,
            available_buffers: 0,
            cached_clean: 0,
            cached_dirty: 8,
            successful_evictions: 0,
        }
    ));
}

/// Run an ignored stress keeper in a child copy of this test binary so the
/// parent can enforce a real wall-clock deadline. Returning `true` means the
/// parent observed a successful child and should return from the test; the
/// child returns `false` and executes the workload body normally.
fn supervise_ignored_stress_test(test_name: &str, timeout: Duration) -> bool {
    const CHILD_TEST_ENV: &str = "FSQLITE_CONTENTION_KEEPER_CHILD";

    if env::var(CHILD_TEST_ENV).ok().as_deref() == Some(test_name) {
        return false;
    }

    let test_binary = env::current_exe().expect("resolve current test binary");
    let mut child = Command::new(test_binary)
        .args(["--exact", test_name, "--include-ignored", "--nocapture"])
        .env(CHILD_TEST_ENV, test_name)
        .spawn()
        .expect("spawn supervised stress-test child");
    let deadline = Instant::now() + timeout;

    loop {
        match child.try_wait().expect("poll stress-test child") {
            Some(status) => {
                assert!(
                    status.success(),
                    "supervised stress-test child {test_name} failed with {status}"
                );
                return true;
            }
            None if Instant::now() >= deadline => {
                let _ = child.kill();
                child.wait().expect("reap timed-out stress-test child");
                panic!("supervised stress test {test_name} exceeded {timeout:?}");
            }
            None => thread::sleep(Duration::from_millis(50)),
        }
    }
}

// ---------------------------------------------------------------------------
// Test result types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct ThroughputResult {
    ops_per_sec: f64,
}

// ---------------------------------------------------------------------------
// Helper: C SQLite baseline measurement
// ---------------------------------------------------------------------------

/// Measure C SQLite throughput at the given thread count.
///
/// Uses WAL mode with busy_timeout for write serialization.
fn measure_csqlite_throughput(thread_count: usize, rows_per_thread: u64) -> ThroughputResult {
    let tmp = tempfile::NamedTempFile::new().expect("tempfile");
    let path = tmp.path().to_str().unwrap().to_owned();

    // Setup database
    {
        let conn = rusqlite::Connection::open(&path).expect("open");
        conn.execute_batch(
            "PRAGMA page_size = 4096;
             PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;
             PRAGMA cache_size = -64000;
             CREATE TABLE bench (id INTEGER PRIMARY KEY, val INTEGER);",
        )
        .expect("setup");
    }

    let ready = Arc::new(Barrier::new(thread_count + 1));
    let start_gate = Arc::new(Barrier::new(thread_count + 1));
    let total_ops = Arc::new(AtomicU64::new(0));
    let worker_connections: Vec<_> = (0..thread_count)
        .map(|tid| {
            let conn = rusqlite::Connection::open(&path).expect("thread open");
            conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=10000;")
                .expect("pragma");
            (tid, conn)
        })
        .collect();

    let handles: Vec<_> = worker_connections
        .into_iter()
        .map(|(tid, conn)| {
            let worker_ready = Arc::clone(&ready);
            let worker_start_gate = Arc::clone(&start_gate);
            let ops = Arc::clone(&total_ops);
            let base = (tid as u64) * rows_per_thread * 2; // Non-overlapping ranges

            thread::spawn(move || {
                worker_ready.wait();
                worker_start_gate.wait();

                let mut local_ops = 0u64;
                for i in 0..rows_per_thread {
                    // Each insert is its own transaction for maximum contention
                    if conn
                        .execute(
                            "INSERT INTO bench VALUES (?1, ?2)",
                            rusqlite::params![sqlite_i64(base + i), sqlite_i64(i * 7)],
                        )
                        .is_ok()
                    {
                        local_ops += 1;
                    }
                }
                ops.fetch_add(local_ops, Ordering::Relaxed);
            })
        })
        .collect();

    ready.wait();
    let start = Instant::now();
    start_gate.wait();

    for h in handles {
        h.join().expect("join");
    }

    let elapsed = start.elapsed();
    let total = total_ops.load(Ordering::Relaxed);
    let expected_total = thread_count as u64 * rows_per_thread;
    assert_eq!(
        total, expected_total,
        "C SQLite control completed {total} of {expected_total} inserts"
    );
    let ops_per_sec = total as f64 / elapsed.as_secs_f64();

    ThroughputResult { ops_per_sec }
}

// ---------------------------------------------------------------------------
// Placeholder: FrankenSQLite throughput measurement
// ---------------------------------------------------------------------------

/// Measure the old FrankenSQLite placeholder control used by this file.
///
/// This is intentionally **not** a truthful concurrent-writer benchmark:
/// - it uses one in-memory connection,
/// - it runs sequentially,
/// - it bypasses the persistent-path harnesses that the 2026-03-23 overlay uses.
///
/// Keep this helper only so the historical ignored scaffolding compiles until
/// the blocked governance and matrix work replaces it with a real gate.
async fn measure_fsqlite_placeholder_sequential_control(
    thread_count: usize,
    rows_per_thread: u64,
) -> ThroughputResult {
    let conn = fsqlite::Connection::open(":memory:").await.expect("open");
    conn.execute("PRAGMA journal_mode = WAL")
        .await
        .expect("enable WAL for historical control");
    conn.execute("CREATE TABLE bench (id INTEGER PRIMARY KEY, val INTEGER)")
        .await
        .expect("create");

    let total_ops = thread_count as u64 * rows_per_thread;
    let start = Instant::now();

    for i in 0..total_ops {
        conn.execute_with_params(
            "INSERT INTO bench VALUES (?1, ?2)",
            &[
                fsqlite::SqliteValue::Integer(i as i64),
                fsqlite::SqliteValue::Integer((i * 7) as i64),
            ],
        )
        .await
        .expect("historical control insert");
    }

    let elapsed = start.elapsed();
    let ops_per_sec = total_ops as f64 / elapsed.as_secs_f64();
    conn.close()
        .await
        .expect("close historical control connection");

    ThroughputResult { ops_per_sec }
}

async fn create_fsqlite_file_backed_db(
    filename: &str,
    schema_sql: &str,
) -> (tempfile::TempDir, String) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join(filename).to_string_lossy().to_string();
    let conn = fsqlite::Connection::open(path.as_str())
        .await
        .expect("open setup db");
    conn.execute("PRAGMA journal_mode = WAL")
        .await
        .expect("enable WAL for setup connection");
    conn.execute("PRAGMA fsqlite.concurrent_mode = ON")
        .await
        .expect("enable concurrent-writer mode for setup connection");
    conn.execute(schema_sql).await.expect("create schema");
    conn.close().await.expect("close setup connection");
    (dir, path)
}

async fn open_fsqlite_worker(path: &str) -> fsqlite::Connection {
    let conn = fsqlite::Connection::open(path.to_owned())
        .await
        .expect("open worker db");
    conn.execute("PRAGMA fsqlite.concurrent_mode = ON")
        .await
        .expect("enable concurrent-writer mode for worker connection");
    conn
}

// ===========================================================================
// CONTENTION TESTS
// ===========================================================================

/// Test 1: Verify no global locks in the commit fast path.
///
/// Instruments the commit path to assert no global Mutex is acquired during
/// WAL frame writing. This is the foundational guarantee of D1.
///
/// D1 is implemented, but no exhaustive commit-path lock-acquisition counter
/// is currently exposed to this integration target.  A WAL-local hook would
/// not prove the absence of global locks elsewhere in commit processing.
#[test]
#[ignore = "release blocker: exhaustive commit-path global-lock instrumentation is not exposed"]
fn test_no_global_locks_in_commit_fast_path() {
    panic!(
        "test_no_global_locks_in_commit_fast_path: requires exhaustive commit-path lock-acquisition instrumentation, not a WAL-local proxy"
    );
}

/// Test 2: Verify WAL segment writes don't serialize.
///
/// Spawns N writer threads, each writing to their own WAL segment. Measures
/// that aggregate write bandwidth scales with thread count.
///
/// D1 is implemented, but this target does not yet expose a deterministic
/// receipt proving distinct segment ownership and overlap. Throughput alone is
/// hardware-sensitive and cannot establish independence.
#[test]
#[ignore = "release blocker: deterministic parallel-WAL segment ownership and overlap receipts are not exposed"]
fn test_parallel_wal_segments_independent() {
    panic!(
        "test_parallel_wal_segments_independent: requires distinct segment-ownership and observed-overlap receipts; a throughput ratio is not a correctness proof"
    );
}

/// Test 3: Verify pages distribute evenly across 128 shards.
///
/// Inserts the 2,560-page workload from the original D2 acceptance contract,
/// then inspects the cache's public shard-distribution diagnostics.  This is a
/// deterministic hash-balance keeper; file-backed cache concurrency is covered
/// separately by the pager crate's cache stress tests.
#[test]
fn test_page_cache_shard_distribution() {
    use fsqlite_pager::ShardedPageCache;
    use fsqlite_types::{PageNumber, PageSize};

    const PAGE_COUNT: usize = 2_560;
    const SHARD_COUNT: usize = 128;
    const MAX_COEFFICIENT_OF_VARIATION: f64 = 0.2;
    const MIN_PAGES_PER_SHARD: usize = 16;
    const MAX_PAGES_PER_SHARD: usize = 24;

    let raw_page_count = u32::try_from(PAGE_COUNT).expect("page count fits u32");
    let cache =
        ShardedPageCache::with_max_buffers_and_shards(PageSize::DEFAULT, PAGE_COUNT, SHARD_COUNT);
    for raw_page_number in 1..=raw_page_count {
        let page_number = PageNumber::new(raw_page_number).expect("page number is non-zero");
        cache
            .insert_fresh(page_number, |_| {})
            .expect("page cache has capacity for the acceptance workload");
    }

    let distribution = cache.shard_distribution();
    assert_eq!(distribution.len(), SHARD_COUNT);
    assert_eq!(distribution.iter().sum::<usize>(), PAGE_COUNT);
    assert!(
        distribution
            .iter()
            .all(|&pages| pages >= MIN_PAGES_PER_SHARD),
        "bd-3wop3.7: page-cache shard has fewer than {MIN_PAGES_PER_SHARD} pages (20% below the 20-page mean); distribution={distribution:?}"
    );
    assert!(
        distribution
            .iter()
            .all(|&pages| pages <= MAX_PAGES_PER_SHARD),
        "bd-3wop3.7: page-cache shard exceeds {MAX_PAGES_PER_SHARD} pages (20% above the 20-page mean); distribution={distribution:?}"
    );

    let mean = f64::from(raw_page_count)
        / f64::from(u32::try_from(SHARD_COUNT).expect("shard count fits u32"));
    let variance = distribution
        .iter()
        .map(|&pages| {
            let deviation =
                f64::from(u32::try_from(pages).expect("shard occupancy fits u32")) - mean;
            deviation * deviation
        })
        .sum::<f64>()
        / f64::from(u32::try_from(SHARD_COUNT).expect("shard count fits u32"));
    let coefficient_of_variation = variance.sqrt() / mean;

    assert!(
        coefficient_of_variation < MAX_COEFFICIENT_OF_VARIATION,
        "bd-3wop3.7: page-cache shard coefficient of variation {coefficient_of_variation:.3} exceeds {MAX_COEFFICIENT_OF_VARIATION:.3}; distribution={distribution:?}"
    );
}

/// Test 4: Verify flat combining reduces atomic operations.
///
/// Counts fetch_add calls during concurrent commits and asserts the count is
/// reduced by the combining factor (batching amortizes atomic ops).
///
/// D3 is implemented, but the current public API cannot deterministically
/// stage multiple pending requests before one caller combines them.  A barrier
/// before `alloc_commit_seq` is insufficient on a single-core scheduler, and
/// the available metrics are process-global rather than per combiner.
#[test]
#[ignore = "release blocker: deterministic staged-pending control and per-instance commit-combiner metrics are not exposed"]
fn test_combiner_reduces_atomic_ops() {
    panic!(
        "test_combiner_reduces_atomic_ops: production combining exists, but a deterministic reduction keeper requires staged pending-request control plus per-instance metrics"
    );
}

/// Test 5: require a deterministic receipt for GC-induced pause behavior.
///
/// Sustained writes alone cannot prove that collection occurred during the
/// measurement window, so this gate remains fail-closed until the public test
/// surface exposes GC-cycle and pause telemetry.
#[test]
#[ignore = "release blocker: deterministic GC-cycle and pause telemetry are not exposed"]
fn test_ebr_no_gc_pauses() {
    panic!(
        "test_ebr_no_gc_pauses: sustained writes do not prove that a GC cycle occurred; deterministic cycle and pause receipts are required"
    );
}

/// Manual sustained-insert latency smoke test.
///
/// This measures successful-operation p99 latency under concurrent inserts. It
/// deliberately makes no claim about EBR or GC because it cannot observe a
/// collection cycle.
#[test]
#[ignore = "manual timing-sensitive sustained-insert latency stress test"]
fn test_sustained_insert_p99_latency() {
    if supervise_ignored_stress_test("test_sustained_insert_p99_latency", Duration::from_secs(45)) {
        return;
    }

    use std::sync::atomic::AtomicBool;

    asupersync::test_utils::run_test(|| async {
        let (_dir, path) = create_fsqlite_file_backed_db(
            "sustained_insert_p99_latency.db",
            "CREATE TABLE bench (id INTEGER PRIMARY KEY, val TEXT)",
        )
        .await;
        let path = Arc::new(path);

        let stop = Arc::new(AtomicBool::new(false));
        let worker_metrics = Arc::new(std::sync::Mutex::new(vec![
            SustainedInsertWorkerMetrics::default();
            SUSTAINED_INSERT_WORKERS
        ]));
        let retry_exhaustions = Arc::new(AtomicU64::new(0));
        let retry_exhaustion_errors = Arc::new(std::sync::Mutex::new(Vec::new()));

        let barrier = Arc::new(Barrier::new(SUSTAINED_INSERT_WORKERS + 1));
        let handles: Vec<_> = (0..SUSTAINED_INSERT_WORKERS)
            .map(|tid| {
                let p = Arc::clone(&path);
                let s = Arc::clone(&stop);
                let m = Arc::clone(&worker_metrics);
                let e = Arc::clone(&retry_exhaustions);
                let ee = Arc::clone(&retry_exhaustion_errors);
                let b = Arc::clone(&barrier);

                thread::spawn(move || {
                    asupersync::test_utils::run_test(|| async {
                        let c = open_fsqlite_worker(p.as_str()).await;
                        b.wait();
                        let mut local_metrics = SustainedInsertWorkerMetrics {
                            latencies_us: Vec::with_capacity(10_000),
                            successful_ids: Vec::with_capacity(10_000),
                            retry_attempts: 0,
                        };
                        let worker_stride = i64::try_from(SUSTAINED_INSERT_WORKERS)
                            .expect("worker count fits i64");
                        let mut i = i64::try_from(tid).expect("worker id fits i64");

                        while !s.load(Ordering::Acquire) {
                            let op_start = Instant::now();
                            let mut attempt = 0_u32;
                            loop {
                                match c
                                    .execute_with_params(
                                        "INSERT OR REPLACE INTO bench VALUES (?1, ?2)",
                                        &[
                                            fsqlite::SqliteValue::Integer(i),
                                            fsqlite::SqliteValue::Text(format!("value_{i}").into()),
                                        ],
                                    )
                                    .await
                                {
                                    Ok(_) => {
                                        local_metrics
                                            .latencies_us
                                            .push(op_start.elapsed().as_micros() as u64);
                                        local_metrics.successful_ids.push(i);
                                        break;
                                    }
                                    Err(error)
                                        if is_sustained_insert_retryable_contention(&error)
                                            && attempt < SUSTAINED_INSERT_MAX_RETRIES =>
                                    {
                                        local_metrics.retry_attempts = local_metrics
                                            .retry_attempts
                                            .checked_add(1)
                                            .expect("sustained-insert retry count fits u64");
                                        thread::sleep(sustained_insert_retry_delay(
                                            attempt,
                                            u64::try_from(tid).expect("worker id fits u64"),
                                        ));
                                        attempt += 1;
                                    }
                                    Err(error)
                                        if is_sustained_insert_retryable_contention(&error) =>
                                    {
                                        e.fetch_add(1, Ordering::Relaxed);
                                        ee.lock().unwrap().push(format!("{error:?}"));
                                        break;
                                    }
                                    Err(error) => {
                                        panic!("unexpected sustained-insert failure: {error:?}");
                                    }
                                }
                            }
                            i = i
                                .checked_add(worker_stride)
                                .expect("sustained-insert row id fits i64");
                        }

                        m.lock().unwrap()[tid] = local_metrics;
                        c.close_without_checkpoint()
                            .await
                            .expect("close latency worker connection without checkpoint");
                    });
                })
            })
            .collect();

        // Run for 2 seconds (reduced for test speed)
        barrier.wait();
        thread::sleep(Duration::from_secs(2));
        stop.store(true, Ordering::Release);

        for h in handles {
            h.join().expect("join");
        }

        let exhausted = retry_exhaustions.load(Ordering::Relaxed);
        assert_eq!(
            exhausted,
            0,
            "bd-3wop3.7: sustained-insert workload exhausted its bounded retry budget: {:?}",
            retry_exhaustion_errors.lock().unwrap()
        );
        let mut worker_metrics = worker_metrics.lock().unwrap().clone();
        let mut worker_summaries = Vec::with_capacity(SUSTAINED_INSERT_WORKERS);
        let mut successful_operations = 0_usize;
        let mut retry_attempts = 0_u64;
        let mut worst_worker_p99_us = 0_u64;
        let mut maximum_latency_us = 0_u64;
        let mut minimum_worker_operations = usize::MAX;
        let mut maximum_worker_operations = 0_usize;
        let mut expected_rows = Vec::new();

        for (worker_id, metrics) in worker_metrics.iter_mut().enumerate() {
            assert_eq!(
                metrics.successful_ids.len(),
                metrics.latencies_us.len(),
                "bd-3wop3.7: worker {worker_id} must retain one row id per successful timed write"
            );
            expected_rows.extend(
                metrics
                    .successful_ids
                    .iter()
                    .map(|&id| (id, format!("value_{id}"))),
            );
            let samples = &mut metrics.latencies_us;
            samples.sort_unstable();
            assert!(
                samples.len() >= SUSTAINED_INSERT_MIN_SUCCESSES_PER_WORKER,
                "bd-3wop3.7: worker {worker_id} completed only {} writes; minimum is {}",
                samples.len(),
                SUSTAINED_INSERT_MIN_SUCCESSES_PER_WORKER
            );

            let p99_idx = samples
                .len()
                .saturating_mul(99)
                .div_ceil(100)
                .saturating_sub(1);
            let p99_us = samples.get(p99_idx).copied().unwrap_or(u64::MAX);
            let worker_max_us = samples.last().copied().unwrap_or(u64::MAX);
            assert!(
                p99_us < SUSTAINED_INSERT_P99_LIMIT_US,
                "bd-3wop3.7: worker {worker_id} sustained-insert p99 latency {:.2}ms exceeds {:.2}ms threshold",
                p99_us as f64 / 1_000.0,
                SUSTAINED_INSERT_P99_LIMIT_US as f64 / 1_000.0
            );
            assert!(
                worker_max_us < SUSTAINED_INSERT_MAX_LATENCY_LIMIT_US,
                "bd-3wop3.7: worker {worker_id} sustained-insert maximum latency {:.2}ms exceeds {:.2}ms threshold",
                worker_max_us as f64 / 1_000.0,
                SUSTAINED_INSERT_MAX_LATENCY_LIMIT_US as f64 / 1_000.0
            );

            let worker_retry_limit = u64::try_from(samples.len())
                .expect("worker operation count fits u64")
                .saturating_mul(SUSTAINED_INSERT_MAX_RETRIES_PER_SUCCESS);
            assert!(
                metrics.retry_attempts <= worker_retry_limit,
                "bd-3wop3.7: worker {worker_id} used {} retry attempts, exceeding its {worker_retry_limit}-attempt limit for {} successful writes",
                metrics.retry_attempts,
                samples.len()
            );

            successful_operations = successful_operations.saturating_add(samples.len());
            retry_attempts = retry_attempts.saturating_add(metrics.retry_attempts);
            worst_worker_p99_us = worst_worker_p99_us.max(p99_us);
            maximum_latency_us = maximum_latency_us.max(worker_max_us);
            minimum_worker_operations = minimum_worker_operations.min(samples.len());
            maximum_worker_operations = maximum_worker_operations.max(samples.len());
            worker_summaries.push((
                worker_id,
                samples.len(),
                metrics.retry_attempts,
                p99_us,
                worker_max_us,
            ));
        }

        assert!(
            minimum_worker_operations
                .saturating_mul(SUSTAINED_INSERT_MIN_PROGRESS_RATIO_DENOMINATOR)
                >= maximum_worker_operations,
            "bd-3wop3.7: slowest worker completed {minimum_worker_operations} writes while fastest completed {maximum_worker_operations}; slowest must achieve at least 1/{SUSTAINED_INSERT_MIN_PROGRESS_RATIO_DENOMINATOR} of fastest"
        );

        expected_rows.sort_unstable_by_key(|(id, _)| *id);
        let verifier = open_fsqlite_worker(&path).await;
        let actual_rows = verifier
            .query("SELECT id, val FROM bench ORDER BY id")
            .await
            .expect("read sustained-insert rows")
            .iter()
            .map(|row| match row.values() {
                [fsqlite::SqliteValue::Integer(id), fsqlite::SqliteValue::Text(value)] => {
                    (*id, value.to_string())
                }
                values => panic!("unexpected sustained-insert row shape: {values:?}"),
            })
            .collect::<Vec<_>>();
        verifier
            .close()
            .await
            .expect("close sustained-insert verifier");
        assert_eq!(
            actual_rows.len(),
            successful_operations,
            "bd-3wop3.7: persisted row count must equal successful timed operations"
        );
        assert_eq!(
            actual_rows, expected_rows,
            "bd-3wop3.7: persisted rows must exactly match every successful timed write"
        );

        println!(
            "[test_sustained_insert_p99_latency] {} ops, retries={}, retry_exhaustions={}, worst_worker_p99={:.2}ms, max={:.2}ms, workers={worker_summaries:?}",
            successful_operations,
            retry_attempts,
            exhausted,
            worst_worker_p99_us as f64 / 1_000.0,
            maximum_latency_us as f64 / 1_000.0
        );
    });
}

/// Test 6: Historical scaling-curve placeholder.
///
/// The real 2026-03-23 scaling story is owned by the canonical matrix and the
/// persistent benchmark harness, not by this file's sequential control.
#[test]
#[ignore = "stale placeholder; pending bd-3wop3.1.5, bd-db300.1.7.4, and bd-db300.7.9.1"]
fn test_scaling_curve() {
    panic!(
        "test_scaling_curve: stale placeholder gate; use scripts/capture_c1_evidence_pack.sh and scripts/capture_persistent_phase_pack.sh instead"
    );
}

// ===========================================================================
// REGRESSION GATES
// ===========================================================================

/// Regression gate: 8-thread throughput >= 1.5x C SQLite.
///
/// Historical note only: this function is blocked because the helper below is a
/// sequential in-memory control, not a truthful persistent concurrent benchmark.
#[test]
#[ignore = "stale placeholder; pending bd-3wop3.1.5, bd-db300.1.7.4, and bd-db300.7.9.1"]
fn test_8t_throughput_regression_gate() {
    panic!(
        "test_8t_throughput_regression_gate: historical {HISTORICAL_PLACEHOLDER_8T_SPEEDUP}x placeholder is non-authoritative; final 8t gate belongs to scripts/capture_persistent_phase_pack.sh with same-pack sqlite3 comparison"
    );
}

/// Regression gate: 16-thread throughput >= 1.0x C SQLite.
///
/// Historical note only: persistent 16-thread truth is part of the blocked
/// overlay contract and must not be inferred from this file's placeholder path.
#[test]
#[ignore = "stale placeholder; pending bd-3wop3.1.5, bd-db300.1.7.4, and bd-db300.7.9.1"]
fn test_16t_throughput_regression_gate() {
    panic!(
        "test_16t_throughput_regression_gate: historical {HISTORICAL_PLACEHOLDER_16T_SPEEDUP}x placeholder is non-authoritative; final persistent 16t gate belongs to scripts/capture_persistent_phase_pack.sh with phase-attribution evidence"
    );
}

// ===========================================================================
// STRESS TESTS
// ===========================================================================

/// Stress test: 64 threads, no deadlock within 60 seconds.
///
/// Spawns 64 writer threads with overlapping key ranges to maximize lock
/// contention. Asserts no deadlock occurs (all threads complete or timeout).
#[test]
#[ignore = "manual contention stress test"]
fn test_64_thread_no_deadlock() {
    if supervise_ignored_stress_test("test_64_thread_no_deadlock", Duration::from_secs(45)) {
        return;
    }

    // This stress test already exercises the current file-backed concurrent path.
    // It is separate from the historical placeholder throughput helper above.

    asupersync::test_utils::run_test(|| async {
        let (_dir, path) = create_fsqlite_file_backed_db(
            "64_thread_no_deadlock.db",
            "CREATE TABLE stress (id INTEGER PRIMARY KEY, val INTEGER)",
        )
        .await;
        let setup_conn = open_fsqlite_worker(&path).await;

        // Pre-populate some rows to enable updates
        for i in 0..100 {
            setup_conn
                .execute_with_params(
                    "INSERT INTO stress VALUES (?1, ?2)",
                    &[
                        fsqlite::SqliteValue::Integer(i),
                        fsqlite::SqliteValue::Integer(0),
                    ],
                )
                .await
                .expect("pre-populate contention row");
        }
        setup_conn
            .close()
            .await
            .expect("close deadlock-test setup connection");
        let path = Arc::new(path);

        let barrier = Arc::new(Barrier::new(64));
        let stop = Arc::new(AtomicBool::new(false));
        let completed = Arc::new(AtomicU64::new(0));
        let successful_ops = Arc::new(AtomicU64::new(0));
        let failed_ops = Arc::new(AtomicU64::new(0));
        let workers_with_progress = Arc::new(AtomicU64::new(0));

        let handles: Vec<_> = (0..64)
            .map(|tid| {
                let p = Arc::clone(&path);
                let b = Arc::clone(&barrier);
                let s = Arc::clone(&stop);
                let comp = Arc::clone(&completed);
                let successes = Arc::clone(&successful_ops);
                let failures = Arc::clone(&failed_ops);
                let progress = Arc::clone(&workers_with_progress);

                thread::spawn(move || {
                    asupersync::test_utils::run_test(|| async {
                        let c = open_fsqlite_worker(p.as_str()).await;
                        b.wait();
                        let mut ops = 0u64;
                        let mut local_successes = 0u64;
                        let mut local_failures = 0u64;

                        while !s.load(Ordering::Relaxed) && ops < 1000 {
                            // Update a random-ish row to create contention
                            let row_id = ((tid * 17 + ops as usize) % 100) as i64;
                            match c
                                .execute_with_params(
                                    "UPDATE stress SET val = val + 1 WHERE id = ?1",
                                    &[fsqlite::SqliteValue::Integer(row_id)],
                                )
                                .await
                            {
                                Ok(_) => local_successes += 1,
                                Err(error) if is_expected_contention_error(&error) => {
                                    local_failures += 1;
                                }
                                Err(error) => {
                                    panic!("unexpected contention-update failure: {error:?}");
                                }
                            }
                            ops += 1;
                        }

                        successes.fetch_add(local_successes, Ordering::Relaxed);
                        failures.fetch_add(local_failures, Ordering::Relaxed);
                        if local_successes > 0 {
                            progress.fetch_add(1, Ordering::Relaxed);
                        }
                        c.close_without_checkpoint()
                            .await
                            .expect("close deadlock-test worker connection without checkpoint");
                        comp.fetch_add(1, Ordering::Relaxed);
                    });
                })
            })
            .collect();

        // Give threads up to 30 seconds to complete
        let deadline = Instant::now() + Duration::from_secs(30);
        while Instant::now() < deadline && completed.load(Ordering::Relaxed) < 64 {
            thread::sleep(Duration::from_millis(100));
        }

        stop.store(true, Ordering::Release);

        // The child-process supervisor supplies the hard deadline; joining here
        // must still surface any worker panic.
        for h in handles {
            h.join().expect("join deadlock-test worker");
        }

        let final_completed = completed.load(Ordering::Relaxed);
        let final_successes = successful_ops.load(Ordering::Relaxed);
        let final_failures = failed_ops.load(Ordering::Relaxed);
        println!(
            "[test_64_thread_no_deadlock] {final_completed}/64 threads completed, {final_successes} successful updates, {final_failures} failed attempts"
        );

        assert_eq!(
            final_completed, 64,
            "bd-3wop3.7: deadlock detected - only {final_completed}/64 threads completed"
        );
        assert_eq!(
            workers_with_progress.load(Ordering::Relaxed),
            64,
            "bd-3wop3.7: at least one contention worker made no successful progress"
        );
    });
}

/// Stress test: high write pressure with repeated row replacement.
///
/// Runs continuous writes against a bounded key set to create version churn and
/// asserts a conservative successful-operation throughput floor. This test
/// cannot observe GC cycles and therefore makes no GC-specific claim.
#[test]
#[ignore = "manual timing-sensitive version-churn stress test"]
fn test_contention_under_version_churn() {
    if supervise_ignored_stress_test(
        "test_contention_under_version_churn",
        Duration::from_secs(45),
    ) {
        return;
    }

    asupersync::test_utils::run_test(|| async {
        let (_dir, path) = create_fsqlite_file_backed_db(
            "contention_under_version_churn.db",
            "CREATE TABLE version_churn (id INTEGER PRIMARY KEY, data BLOB)",
        )
        .await;
        let path = Arc::new(path);

        let stop = Arc::new(AtomicBool::new(false));
        let total_ops = Arc::new(AtomicU64::new(0));
        let total_failures = Arc::new(AtomicU64::new(0));
        let workers_with_progress = Arc::new(AtomicU64::new(0));

        let barrier = Arc::new(Barrier::new(5));
        let handles: Vec<_> = (0..4)
            .map(|tid| {
                let p = Arc::clone(&path);
                let b = Arc::clone(&barrier);
                let s = Arc::clone(&stop);
                let ops = Arc::clone(&total_ops);
                let failures = Arc::clone(&total_failures);
                let progress = Arc::clone(&workers_with_progress);

                thread::spawn(move || {
                    asupersync::test_utils::run_test(|| async {
                        let c = open_fsqlite_worker(p.as_str()).await;
                        b.wait();
                        let mut local_ops = 0u64;
                        let mut local_failures = 0u64;
                        let mut i = tid * 10_000_000;

                        // Create a blob that will stress memory allocation
                        let blob = vec![0xABu8; 1024];

                        while !s.load(Ordering::Relaxed) {
                            // Reuse a bounded key set to create version churn.
                            let row_id = (i % 1000) as i64; // Reuse 1000 row IDs
                            match c
                                .execute_with_params(
                                    "INSERT OR REPLACE INTO version_churn VALUES (?1, ?2)",
                                    &[
                                        fsqlite::SqliteValue::Integer(row_id),
                                        fsqlite::SqliteValue::Blob(blob.clone().into()),
                                    ],
                                )
                                .await
                            {
                                Ok(_) => local_ops += 1,
                                Err(error) if is_expected_contention_error(&error) => {
                                    local_failures += 1;
                                }
                                Err(error) => {
                                    panic!("unexpected version-churn write failure: {error:?}");
                                }
                            }
                            i += 1;
                        }

                        ops.fetch_add(local_ops, Ordering::Relaxed);
                        failures.fetch_add(local_failures, Ordering::Relaxed);
                        if local_ops > 0 {
                            progress.fetch_add(1, Ordering::Relaxed);
                        }
                        c.close_without_checkpoint()
                            .await
                            .expect("close version-churn worker connection without checkpoint");
                    });
                })
            })
            .collect();

        // Run for 3 seconds
        barrier.wait();
        let start = Instant::now();
        thread::sleep(Duration::from_secs(3));
        stop.store(true, Ordering::Release);

        for h in handles {
            h.join().expect("join");
        }

        let elapsed = start.elapsed();
        let total = total_ops.load(Ordering::Relaxed);
        let failures = total_failures.load(Ordering::Relaxed);
        let ops_per_sec = total as f64 / elapsed.as_secs_f64();

        println!(
            "[test_contention_under_version_churn] {} successful ops, {} failed attempts in {:.2}s = {:.0} successful ops/s",
            total,
            failures,
            elapsed.as_secs_f64(),
            ops_per_sec
        );

        assert_eq!(
            workers_with_progress.load(Ordering::Relaxed),
            4,
            "bd-3wop3.7: at least one version-churn worker made no successful progress"
        );

        // Assert minimum throughput (very conservative floor)
        // Under version churn, we should still achieve at least 1000 ops/s.
        assert!(
            ops_per_sec > 1000.0,
            "bd-3wop3.7: version-churn throughput collapsed ({ops_per_sec:.0} ops/s < 1000)"
        );
    });
}

// ===========================================================================
// SPLIT-LOCK COMMIT TESTS (D1-CRITICAL bd-3wop3.8)
// ===========================================================================

/// Test 7: Verify split-lock commit allows concurrent prepare phases.
///
/// The split-lock protocol separates commit into three phases:
/// - Phase A (prepare): Hold inner.lock(), collect write set
/// - Phase B (WAL I/O): Hold wal_backend.lock(), release inner.lock()
/// - Phase C (publish): Re-acquire inner.lock(), update db_size
///
/// This allows Thread B to start its prepare phase while Thread A does WAL I/O.
#[test]
#[ignore = "manual contention stress test"]
fn test_split_lock_commit_no_deadlock() {
    if supervise_ignored_stress_test(
        "test_split_lock_commit_no_deadlock",
        Duration::from_secs(45),
    ) {
        return;
    }

    // Test that multiple concurrent writers don't deadlock with the split-lock
    // protocol. With the old monolithic lock, this would cause severe contention.

    asupersync::test_utils::run_test(|| async {
        let (_dir, path) = create_fsqlite_file_backed_db(
            "split_lock_commit_no_deadlock.db",
            "CREATE TABLE split_lock_test (id INTEGER PRIMARY KEY, val INTEGER)",
        )
        .await;
        let path = Arc::new(path);

        let barrier = Arc::new(Barrier::new(8));
        let completed = Arc::new(AtomicU64::new(0));
        let total_ops = Arc::new(AtomicU64::new(0));

        let handles: Vec<_> = (0..8)
            .map(|tid| {
                let p = Arc::clone(&path);
                let b = Arc::clone(&barrier);
                let comp = Arc::clone(&completed);
                let ops = Arc::clone(&total_ops);
                let base = (tid as i64) * 10_000;

                thread::spawn(move || {
                    asupersync::test_utils::run_test(|| async {
                        let c = open_fsqlite_worker(p.as_str()).await;
                        b.wait();
                        let mut local_ops = 0u64;

                        // Each thread inserts 500 rows, each as its own transaction
                        // This maximizes commit contention
                        for i in 0..500 {
                            match c
                                .execute_with_params(
                                    "INSERT INTO split_lock_test VALUES (?1, ?2)",
                                    &[
                                        fsqlite::SqliteValue::Integer(base + i),
                                        fsqlite::SqliteValue::Integer(i * 7),
                                    ],
                                )
                                .await
                            {
                                Ok(_) => local_ops += 1,
                                Err(error) if is_expected_contention_error(&error) => {}
                                Err(error) => {
                                    panic!("unexpected split-lock insert failure: {error:?}");
                                }
                            }
                        }

                        ops.fetch_add(local_ops, Ordering::Relaxed);
                        c.close_without_checkpoint()
                            .await
                            .expect("close split-lock worker connection without checkpoint");
                        comp.fetch_add(1, Ordering::Relaxed);
                    });
                })
            })
            .collect();

        // The child-process supervisor supplies the hard deadline.
        for h in handles {
            h.join().expect("join");
        }

        let final_completed = completed.load(Ordering::Relaxed);
        let final_ops = total_ops.load(Ordering::Relaxed);

        println!(
            "[test_split_lock_commit_no_deadlock] {}/8 threads completed, {} total ops",
            final_completed, final_ops
        );

        assert_eq!(
            final_completed, 8,
            "bd-3wop3.8: split-lock deadlock - only {}/8 threads completed",
            final_completed
        );

        // All 8 threads × 500 ops = 4000 expected
        assert!(
            final_ops >= 3800,
            "bd-3wop3.8: too few operations completed ({} < 3800)",
            final_ops
        );
    });
}

/// Test 8: Verify split-lock commit throughput scales better than monolithic lock.
///
/// Measures commit throughput with increasing thread counts. With split-lock,
/// we expect better scaling because prepare phases can overlap with WAL I/O.
#[test]
#[ignore = "manual throughput benchmark"]
fn test_split_lock_commit_scaling() {
    if supervise_ignored_stress_test("test_split_lock_commit_scaling", Duration::from_secs(120)) {
        return;
    }

    // Measure throughput at 1, 2, 4, 8 threads and verify scaling isn't pathological.

    asupersync::test_utils::run_test(|| async {
        let mut results: Vec<(usize, f64)> = Vec::new();
        for &thread_count in &[1, 2, 4, 8] {
            let (_dir, path) = create_fsqlite_file_backed_db(
                &format!("split_lock_commit_scaling_{thread_count}.db"),
                "CREATE TABLE scaling_test (id INTEGER PRIMARY KEY, val INTEGER)",
            )
            .await;
            let path = Arc::new(path);

            let ready = Arc::new(Barrier::new(thread_count + 1));
            let start_gate = Arc::new(Barrier::new(thread_count + 1));
            let finish_gate = Arc::new(Barrier::new(thread_count + 1));
            let total_ops = Arc::new(AtomicU64::new(0));
            let ops_per_thread = 1000;

            let handles: Vec<_> = (0..thread_count)
                .map(|tid| {
                    let p = Arc::clone(&path);
                    let worker_ready = Arc::clone(&ready);
                    let worker_start = Arc::clone(&start_gate);
                    let worker_finish = Arc::clone(&finish_gate);
                    let ops = Arc::clone(&total_ops);
                    let base = (tid as i64) * (ops_per_thread as i64) * 2;

                    thread::spawn(move || {
                        asupersync::test_utils::run_test(|| async {
                            let c = open_fsqlite_worker(p.as_str()).await;
                            worker_ready.wait();
                            worker_start.wait();
                            let mut local_ops = 0u64;

                            for i in 0..ops_per_thread {
                                match c
                                    .execute_with_params(
                                        "INSERT INTO scaling_test VALUES (?1, ?2)",
                                        &[
                                            fsqlite::SqliteValue::Integer(base + i as i64),
                                            fsqlite::SqliteValue::Integer(i as i64),
                                        ],
                                    )
                                    .await
                                {
                                    Ok(_) => local_ops += 1,
                                    Err(error) if is_expected_contention_error(&error) => {}
                                    Err(error) => {
                                        panic!("unexpected scaling insert failure: {error:?}");
                                    }
                                }
                            }

                            ops.fetch_add(local_ops, Ordering::Relaxed);
                            worker_finish.wait();
                            c.close_without_checkpoint()
                                .await
                                .expect("close scaling worker connection without checkpoint");
                        });
                    })
                })
                .collect();

            ready.wait();
            let start = Instant::now();
            start_gate.wait();
            finish_gate.wait();
            let elapsed = start.elapsed();

            for h in handles {
                h.join().expect("join");
            }

            let total = total_ops.load(Ordering::Relaxed);
            let expected = u64::try_from(thread_count)
                .expect("thread count fits u64")
                .saturating_mul(u64::try_from(ops_per_thread).expect("operation count fits u64"));
            assert_eq!(
                total, expected,
                "bd-3wop3.8: scaling cell completed {total} of {expected} inserts"
            );
            let ops_per_sec = total as f64 / elapsed.as_secs_f64();

            results.push((thread_count, ops_per_sec));
        }

        println!("\n[test_split_lock_commit_scaling] Results:");
        for (threads, ops) in &results {
            println!("  {}t: {:.0} ops/s", threads, ops);
        }

        // Verify basic sanity: throughput at 4+ threads shouldn't collapse below 1-thread
        let single_thread_ops = results[0].1;
        let four_thread_ops = results[2].1;
        let eight_thread_ops = results[3].1;

        // With split-lock, 4t should be at least 50% of 1t (allowing for contention)
        // This is a conservative check - the goal is to catch pathological regression
        assert!(
            four_thread_ops > single_thread_ops * 0.5,
            "bd-3wop3.8: 4t throughput collapsed ({:.0} < {:.0} * 0.5)",
            four_thread_ops,
            single_thread_ops
        );

        // 8t should still be at least 30% of 1t (more contention expected)
        assert!(
            eight_thread_ops > single_thread_ops * 0.3,
            "bd-3wop3.8: 8t throughput collapsed ({:.0} < {:.0} * 0.3)",
            eight_thread_ops,
            single_thread_ops
        );
    });
}

/// Test 9: exercise concurrent commits carrying larger rows.
///
/// This is a large-row concurrency smoke test only. It neither injects a
/// deterministic WAL delay nor observes prepare/WAL overlap, so it must not be
/// used as proof of split-lock phase independence.
#[test]
#[ignore = "manual large-row concurrent-commit smoke test; does not prove prepare/WAL overlap"]
fn test_large_row_concurrent_commit_smoke() {
    if supervise_ignored_stress_test(
        "test_large_row_concurrent_commit_smoke",
        Duration::from_secs(30),
    ) {
        return;
    }

    asupersync::test_utils::run_test(|| async {
        let (_dir, path) = create_fsqlite_file_backed_db(
            "large_row_concurrent_commit_smoke.db",
            "CREATE TABLE large_row_test (id INTEGER PRIMARY KEY, data BLOB)",
        )
        .await;
        let path = Arc::new(path);

        let barrier = Arc::new(Barrier::new(4));
        let completed = Arc::new(AtomicU64::new(0));
        let total_ops = Arc::new(AtomicU64::new(0));

        let handles: Vec<_> = (0..4)
            .map(|tid| {
                let p = Arc::clone(&path);
                let b = Arc::clone(&barrier);
                let comp = Arc::clone(&completed);
                let ops = Arc::clone(&total_ops);

                thread::spawn(move || {
                    asupersync::test_utils::run_test(|| async {
                        let c = open_fsqlite_worker(p.as_str()).await;
                        b.wait();
                        let mut local_ops = 0u64;

                        // Write larger blobs to make WAL I/O more significant
                        let blob = vec![0xABu8; 4096]; // 4KB per row

                        for i in 0..100 {
                            let row_id = (tid * 1000 + i) as i64;
                            match c
                                .execute_with_params(
                                    "INSERT INTO large_row_test VALUES (?1, ?2)",
                                    &[
                                        fsqlite::SqliteValue::Integer(row_id),
                                        fsqlite::SqliteValue::Blob(blob.clone().into()),
                                    ],
                                )
                                .await
                            {
                                Ok(_) => local_ops += 1,
                                Err(error) if is_expected_contention_error(&error) => {}
                                Err(error) => {
                                    panic!("unexpected large-row insert failure: {error:?}");
                                }
                            }
                        }

                        ops.fetch_add(local_ops, Ordering::Relaxed);
                        c.close_without_checkpoint()
                            .await
                            .expect("close large-row worker connection without checkpoint");
                        comp.fetch_add(1, Ordering::Relaxed);
                    });
                })
            })
            .collect();

        for h in handles {
            h.join().expect("join");
        }

        let final_completed = completed.load(Ordering::Relaxed);
        let final_ops = total_ops.load(Ordering::Relaxed);

        println!(
            "[test_large_row_concurrent_commit_smoke] {}/4 threads, {} ops",
            final_completed, final_ops
        );

        assert_eq!(
            final_completed, 4,
            "bd-3wop3.8: large-row smoke completed only {}/4 threads",
            final_completed
        );

        // 4 threads × 100 ops = 400 expected
        assert!(
            final_ops >= 380,
            "bd-3wop3.8: too few successful large-row inserts ({} < 380)",
            final_ops
        );
    });
}

// ===========================================================================
// SCALING REPORT (manual run)
// ===========================================================================

/// Generate a scaling report comparing FrankenSQLite vs C SQLite at multiple
/// thread counts.
///
/// Run with: `cargo test -p fsqlite-e2e --test bd_3wop3_7_contention_elimination scaling_report -- --nocapture --ignored`
#[test]
#[ignore = "manual benchmark - run with --ignored"]
fn scaling_report() {
    if supervise_ignored_stress_test("scaling_report", Duration::from_secs(300)) {
        return;
    }

    asupersync::test_utils::run_test(|| async {
        println!("\n=== D-TEST Scaling Report (bd-3wop3.7) ===\n");
        println!("Thread | C SQLite ops/s | FS placeholder ops/s | Historical placeholder ratio");
        println!("-------|----------------|----------------------|----------------------------");

        for &threads in SCALING_THREAD_COUNTS {
            let csqlite = measure_csqlite_throughput(threads, ROWS_PER_THREAD / 10);
            let fsqlite =
                measure_fsqlite_placeholder_sequential_control(threads, ROWS_PER_THREAD / 10).await;
            let speedup = fsqlite.ops_per_sec / csqlite.ops_per_sec;

            println!(
                "{:>6} | {:>14.0} | {:>19.0} | {:>6.2}x",
                threads, csqlite.ops_per_sec, fsqlite.ops_per_sec, speedup
            );
        }

        println!(
            "\nNote: FrankenSQLite numbers here come from a historical sequential placeholder control, not the authoritative c1 or persistent 8t/16t scorecard surfaces. Use scripts/capture_c1_evidence_pack.sh and scripts/capture_persistent_phase_pack.sh for current truth."
        );
    });
}
