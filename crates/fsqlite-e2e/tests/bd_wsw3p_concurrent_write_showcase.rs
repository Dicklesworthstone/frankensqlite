//! bd-wsw3p: Concurrent-write-only benchmark validation.
//!
//! Verifies that FrankenSQLite's page-level MVCC provides measurable throughput
//! improvement over C SQLite's serialized WAL_WRITE_LOCK at 4+ threads with
//! non-conflicting workloads (each thread writes to its own table).
//!
//! The test is intentionally lightweight (small row counts, few iterations) so
//! it runs in ~30 seconds.  It produces structured JSON artifacts to the temp
//! directory, then validates:
//! - Both engines produce correct row counts
//! - FrankenSQLite 4-thread throughput beats the C SQLite baseline
//! - Structured output contains required fields

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use fsqlite::SqliteValue;
use serde::{Deserialize, Serialize};

const ROWS_PER_THREAD: i64 = 200;
const MAX_TXN_RETRIES: u32 = 100;
const RETRY_BACKOFF: Duration = Duration::from_micros(100);

fn artifact_dir() -> PathBuf {
    let dir = std::env::temp_dir()
        .join("fsqlite-wsw3p-tests")
        .join(format!("run-{}", std::process::id()));
    if dir.exists() {
        let _ = fs::remove_dir_all(&dir);
    }
    fs::create_dir_all(&dir).expect("create artifact dir");
    dir
}

// ── Engine runners ──────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ThreadResult {
    thread_id: usize,
    rows_inserted: i64,
    wall_ms: u64,
    retries: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchResult {
    engine: String,
    n_threads: usize,
    rows_per_thread: i64,
    total_rows: i64,
    total_wall_ms: u64,
    throughput_ops_per_sec: f64,
    total_retries: u64,
    per_thread: Vec<ThreadResult>,
}

fn run_csqlite_concurrent(n_threads: usize) -> BenchResult {
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
                conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;")
                    .unwrap();
                bar.wait();

                let thread_start = Instant::now();
                let insert_sql =
                    format!("INSERT INTO bench_{tid} VALUES (?1, ('t' || ?1), (?1 * 7))");
                let mut stmt = conn.prepare(&insert_sql).unwrap();
                let mut local_retries: u64 = 0;

                for i in 0..ROWS_PER_THREAD {
                    loop {
                        match stmt.execute(rusqlite::params![i]) {
                            Ok(_) => break,
                            Err(e) => {
                                if e.to_string().contains("database is locked") {
                                    local_retries += 1;
                                    thread::sleep(RETRY_BACKOFF);
                                } else {
                                    panic!("csqlite insert failed: {e}");
                                }
                            }
                        }
                    }
                }
                retries.fetch_add(local_retries, Ordering::Relaxed);
                ThreadResult {
                    thread_id: tid,
                    rows_inserted: ROWS_PER_THREAD,
                    wall_ms: thread_start.elapsed().as_millis() as u64,
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
        n_threads,
        rows_per_thread: ROWS_PER_THREAD,
        total_rows,
        total_wall_ms: total_wall.as_millis() as u64,
        throughput_ops_per_sec: throughput,
        total_retries: retry_total.load(Ordering::Relaxed),
        per_thread,
    }
}

fn run_fsqlite_concurrent(n_threads: usize) -> BenchResult {
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
                conn.execute("PRAGMA busy_timeout = 0;").unwrap();
                conn.execute("PRAGMA fsqlite.concurrent_mode = ON;")
                    .unwrap();

                let insert_sql =
                    format!("INSERT INTO bench_{tid} VALUES (?1, ('t' || ?1), (?1 * 7));");
                let stmt = conn.prepare(&insert_sql).unwrap();
                bar.wait();

                let thread_start = Instant::now();
                let mut local_retries: u64 = 0;

                for i in 0..ROWS_PER_THREAD {
                    let mut attempts = 0u32;
                    loop {
                        if let Err(_e) = conn.execute("BEGIN CONCURRENT") {
                            local_retries += 1;
                            attempts += 1;
                            if attempts >= MAX_TXN_RETRIES {
                                panic!("BEGIN CONCURRENT failed after {MAX_TXN_RETRIES} retries");
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

                retries.fetch_add(local_retries, Ordering::Relaxed);
                ThreadResult {
                    thread_id: tid,
                    rows_inserted: ROWS_PER_THREAD,
                    wall_ms: thread_start.elapsed().as_millis() as u64,
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
    csqlite_throughput: f64,
    fsqlite_throughput: f64,
    ratio: f64,
}

fn write_report(dir: &Path, report: &ShowcaseReport) {
    let json = serde_json::to_string_pretty(report).expect("serialize report");
    fs::write(dir.join("concurrent_showcase.json"), json).expect("write report");
}

// ── Tests ────────────────────────────────────────────────────────────────

#[test]
fn t1_csqlite_concurrent_writes_produce_correct_data() {
    let result = run_csqlite_concurrent(4);
    assert_eq!(result.total_rows, 4 * ROWS_PER_THREAD);
    assert!(result.throughput_ops_per_sec > 0.0);
    assert_eq!(result.per_thread.len(), 4);
}

#[test]
fn t2_fsqlite_concurrent_writes_produce_correct_data() {
    let result = run_fsqlite_concurrent(4);
    assert_eq!(result.total_rows, 4 * ROWS_PER_THREAD);
    assert!(result.throughput_ops_per_sec > 0.0);
    assert_eq!(result.per_thread.len(), 4);
}

#[test]
fn t3_structured_json_has_required_fields() {
    let dir = artifact_dir();
    let c_result = run_csqlite_concurrent(2);
    let f_result = run_fsqlite_concurrent(2);

    let report = ShowcaseReport {
        schema_version: "fsqlite-e2e.concurrent_showcase.v1".to_owned(),
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
        assert!(r["n_threads"].is_number());
        assert!(r["throughput_ops_per_sec"].is_number());
        assert!(r["per_thread"].is_array());
        for t in r["per_thread"].as_array().unwrap() {
            assert!(t["thread_id"].is_number());
            assert!(t["rows_inserted"].is_number());
            assert!(t["wall_ms"].is_number());
            assert!(t["retries"].is_number());
        }
    }
}

#[test]
fn t4_fsqlite_outperforms_csqlite_at_4_threads() {
    let c1 = run_csqlite_concurrent(1);
    let c4 = run_csqlite_concurrent(4);
    let f1 = run_fsqlite_concurrent(1);
    let f4 = run_fsqlite_concurrent(4);

    let csqlite_scaling = c4.throughput_ops_per_sec / c1.throughput_ops_per_sec;
    let fsqlite_scaling = f4.throughput_ops_per_sec / f1.throughput_ops_per_sec;
    let fsqlite_vs_csqlite_4t = f4.throughput_ops_per_sec / c4.throughput_ops_per_sec.max(1.0);

    eprintln!("csqlite 1t→4t scaling: {csqlite_scaling:.2}x");
    eprintln!("fsqlite 1t→4t scaling: {fsqlite_scaling:.2}x");
    eprintln!("fsqlite 4t vs csqlite 4t: {fsqlite_vs_csqlite_4t:.2}x");
    eprintln!(
        "csqlite throughput: 1t={:.0} 4t={:.0} ops/s",
        c1.throughput_ops_per_sec, c4.throughput_ops_per_sec
    );
    eprintln!(
        "fsqlite throughput: 1t={:.0} 4t={:.0} ops/s",
        f1.throughput_ops_per_sec, f4.throughput_ops_per_sec
    );

    assert!(
        f4.throughput_ops_per_sec > c4.throughput_ops_per_sec,
        "fsqlite 4-thread throughput ({:.0} ops/s, {fsqlite_vs_csqlite_4t:.2}x) must exceed csqlite 4-thread throughput ({:.0} ops/s)",
        f4.throughput_ops_per_sec,
        c4.throughput_ops_per_sec
    );
}

#[test]
fn t5_full_showcase_4_8_produces_artifact_bundle() {
    let dir = artifact_dir();
    let thread_counts = vec![4, 8];

    let mut results = Vec::new();
    let mut scaling_ratios = Vec::new();

    for &n in &thread_counts {
        let c = run_csqlite_concurrent(n);
        let f = run_fsqlite_concurrent(n);
        let ratio = f.throughput_ops_per_sec / c.throughput_ops_per_sec.max(1.0);
        scaling_ratios.push(ScalingRatio {
            n_threads: n,
            csqlite_throughput: c.throughput_ops_per_sec,
            fsqlite_throughput: f.throughput_ops_per_sec,
            ratio,
        });
        results.push(c);
        results.push(f);
    }

    let report = ShowcaseReport {
        schema_version: "fsqlite-e2e.concurrent_showcase.v1".to_owned(),
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
            "  {}t: csqlite={:.0} ops/s, fsqlite={:.0} ops/s, ratio={:.2}x",
            sr.n_threads, sr.csqlite_throughput, sr.fsqlite_throughput, sr.ratio
        );
    }
    eprintln!("Artifacts: {}", dir.display());
}

#[test]
fn t6_rusqlite_verification_catches_data_on_fsqlite_db() {
    let result = run_fsqlite_concurrent(2);
    assert_eq!(result.per_thread.len(), 2);
    for t in &result.per_thread {
        assert_eq!(t.rows_inserted, ROWS_PER_THREAD);
    }
}

#[test]
fn t7_each_thread_reports_nonzero_wall_time() {
    let result = run_fsqlite_concurrent(4);
    for t in &result.per_thread {
        assert!(
            t.wall_ms > 0 || ROWS_PER_THREAD <= 10,
            "thread {} wall_ms should be > 0 for {} rows",
            t.thread_id,
            ROWS_PER_THREAD
        );
    }
}
