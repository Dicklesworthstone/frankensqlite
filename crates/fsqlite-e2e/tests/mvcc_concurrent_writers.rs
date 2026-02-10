//! MVCC concurrent writers workload: disjoint partitions + controlled contention.
//!
//! Bead: bd-1w6k.4.3
//!
//! Exercises the concurrent write path at multiple concurrency levels to measure
//! MVCC scaling behaviour.  Two workload modes are used:
//!
//! 1. **Disjoint partitions** — each worker inserts into non-overlapping key
//!    ranges. Under MVCC this should scale nearly linearly because there is
//!    no page-level contention.
//!
//! 2. **Controlled contention** — all workers update the same 10 hot rows,
//!    forcing conflict detection, retry logic, and SSI abort paths.  Under
//!    stock SQLite (single `WAL_WRITE_LOCK`) contention serialises writers;
//!    under MVCC first-committer-wins (FCW) decides the winner.
//!
//! Each test runs the workload against C SQLite with **real concurrent threads**
//! (via `rusqlite` in WAL mode) and against FrankenSQLite **sequentially** (the
//! MVCC concurrent-writer path is not yet wired to persistence).
//!
//! Run the scaling report with:
//! ```sh
//! cargo test -p fsqlite-e2e --test mvcc_concurrent_writers -- --nocapture
//! ```

use std::fmt::Write as FmtWrite;
use std::path::Path;
use std::time::Instant;

use fsqlite_e2e::oplog::{
    preset_commutative_inserts_disjoint_keys, preset_hot_page_contention, OpLog,
};
use fsqlite_e2e::report::EngineRunReport;
use fsqlite_e2e::sqlite_executor::{SqliteExecConfig, run_oplog_sqlite};

// ── Configuration ────────────────────────────────────────────────────────

/// Concurrency levels tested for scaling analysis.
const CONCURRENCY_LEVELS: &[u16] = &[1, 2, 4, 8, 16];

/// Rows per worker for disjoint-partition mode.
const DISJOINT_ROWS_PER_WORKER: u32 = 500;

/// Rounds of contention for hot-page mode.
const CONTENTION_ROUNDS: u32 = 20;

/// Number of repetitions per configuration for statistical stability.
const REPETITIONS: usize = 3;

/// Deterministic seed for reproducibility.
const SEED: u64 = 42;

// ── Metric collection ────────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct RunMetrics {
    worker_count: u16,
    wall_time_ms: Vec<u64>,
    ops_per_sec: Vec<f64>,
    total_retries: Vec<u64>,
    total_aborts: Vec<u64>,
    ops_total: u64,
    errors: Vec<Option<String>>,
}

impl RunMetrics {
    fn new(worker_count: u16) -> Self {
        Self {
            worker_count,
            wall_time_ms: Vec::new(),
            ops_per_sec: Vec::new(),
            total_retries: Vec::new(),
            total_aborts: Vec::new(),
            ops_total: 0,
            errors: Vec::new(),
        }
    }

    fn record(&mut self, report: &EngineRunReport) {
        self.wall_time_ms.push(report.wall_time_ms);
        self.ops_per_sec.push(report.ops_per_sec);
        self.total_retries.push(report.retries);
        self.total_aborts.push(report.aborts);
        self.ops_total = report.ops_total;
        self.errors.push(report.error.clone());
    }

    fn median_wall_ms(&self) -> u64 {
        percentile_u64(&self.wall_time_ms, 50)
    }

    fn p50_ops_per_sec(&self) -> f64 {
        percentile_f64(&self.ops_per_sec, 50)
    }

    fn p95_ops_per_sec(&self) -> f64 {
        percentile_f64(&self.ops_per_sec, 95)
    }

    fn median_retries(&self) -> u64 {
        percentile_u64(&self.total_retries, 50)
    }

    fn median_aborts(&self) -> u64 {
        percentile_u64(&self.total_aborts, 50)
    }

    fn any_error(&self) -> bool {
        self.errors.iter().any(Option::is_some)
    }
}

fn percentile_u64(data: &[u64], pct: u32) -> u64 {
    if data.is_empty() {
        return 0;
    }
    let mut sorted = data.to_vec();
    sorted.sort_unstable();
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let idx = ((f64::from(pct) / 100.0) * (sorted.len() as f64 - 1.0)).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn percentile_f64(data: &[f64], pct: u32) -> f64 {
    if data.is_empty() {
        return 0.0;
    }
    let mut sorted = data.to_vec();
    sorted.sort_by(f64::total_cmp);
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let idx = ((f64::from(pct) / 100.0) * (sorted.len() as f64 - 1.0)).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

// ── Helpers ──────────────────────────────────────────────────────────────

/// Run a single oplog against C SQLite and return the report.
fn run_csqlite(oplog: &OpLog) -> EngineRunReport {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("bench.db");
    // Create empty DB file so rusqlite can open it.
    rusqlite::Connection::open(&db_path).unwrap();

    run_oplog_sqlite(&db_path, oplog, &SqliteExecConfig::default()).unwrap()
}

/// Run a single oplog against FrankenSQLite (sequential baseline).
fn run_fsqlite_sequential(oplog: &OpLog) -> EngineRunReport {
    let conn = fsqlite::Connection::open(":memory:").unwrap();
    let started = Instant::now();
    let mut ops_ok: u64 = 0;
    let mut error: Option<String> = None;

    for rec in &oplog.records {
        let sql = match &rec.kind {
            fsqlite_e2e::oplog::OpKind::Sql { statement } => statement.clone(),
            fsqlite_e2e::oplog::OpKind::Insert { table, key, values } => {
                let cols: Vec<String> = std::iter::once("id".to_owned())
                    .chain(values.iter().map(|(c, _)| c.clone()))
                    .collect();
                let vals: Vec<String> = std::iter::once(key.to_string())
                    .chain(values.iter().map(|(_, v)| format_val(v)))
                    .collect();
                format!(
                    "INSERT INTO \"{table}\" ({}) VALUES ({})",
                    cols.join(", "),
                    vals.join(", ")
                )
            }
            fsqlite_e2e::oplog::OpKind::Update { table, key, values } => {
                let sets: Vec<String> =
                    values.iter().map(|(c, v)| format!("{c}={}", format_val(v))).collect();
                format!("UPDATE \"{table}\" SET {} WHERE id={key}", sets.join(", "))
            }
            fsqlite_e2e::oplog::OpKind::Begin => "BEGIN".to_owned(),
            fsqlite_e2e::oplog::OpKind::Commit => "COMMIT".to_owned(),
            fsqlite_e2e::oplog::OpKind::Rollback => "ROLLBACK".to_owned(),
        };
        match conn.execute(&sql) {
            Ok(_) => ops_ok += 1,
            Err(e) => {
                if error.is_none() {
                    error = Some(format!("{e}"));
                }
            }
        }
    }

    let wall = started.elapsed();
    #[allow(clippy::cast_precision_loss)]
    let ops_per_sec = if wall.as_secs_f64() > 0.0 {
        ops_ok as f64 / wall.as_secs_f64()
    } else {
        0.0
    };

    EngineRunReport {
        wall_time_ms: u64::try_from(wall.as_millis()).unwrap_or(u64::MAX),
        ops_total: ops_ok,
        ops_per_sec,
        retries: 0,
        aborts: 0,
        correctness: fsqlite_e2e::report::CorrectnessReport {
            raw_sha256_match: None,
            dump_match: None,
            canonical_sha256_match: None,
            integrity_check_ok: None,
            raw_sha256: None,
            canonical_sha256: None,
            logical_sha256: None,
            notes: Some("mode=sequential (fsqlite baseline)".to_owned()),
        },
        latency_ms: None,
        error,
    }
}

/// Format a value string for SQL. Attempts integer, then float, then quoted string.
fn format_val(v: &str) -> String {
    if v.parse::<i64>().is_ok() || v.parse::<f64>().is_ok() {
        v.to_owned()
    } else {
        format!("'{}'", v.replace('\'', "''"))
    }
}

/// Generate and run the disjoint-partition workload at a specific concurrency level.
fn run_disjoint_scaling(worker_count: u16) -> RunMetrics {
    let mut metrics = RunMetrics::new(worker_count);
    for rep in 0..REPETITIONS {
        let oplog = preset_commutative_inserts_disjoint_keys(
            &format!("mvcc-disjoint-{worker_count}w-rep{rep}"),
            SEED + u64::from(rep as u32),
            worker_count,
            DISJOINT_ROWS_PER_WORKER,
        );
        let report = run_csqlite(&oplog);
        metrics.record(&report);
    }
    metrics
}

/// Generate and run the hot-page contention workload at a specific concurrency level.
fn run_contention_scaling(worker_count: u16) -> RunMetrics {
    let mut metrics = RunMetrics::new(worker_count);
    for rep in 0..REPETITIONS {
        let oplog = preset_hot_page_contention(
            &format!("mvcc-contention-{worker_count}w-rep{rep}"),
            SEED + u64::from(rep as u32),
            worker_count,
            CONTENTION_ROUNDS,
        );
        let report = run_csqlite(&oplog);
        metrics.record(&report);
    }
    metrics
}

/// Format a scaling report table.
fn format_scaling_table(mode: &str, results: &[RunMetrics], baseline_ops: f64) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "\n{'=':.>72}");
    let _ = writeln!(out, "  MVCC Scaling Report: {mode}");
    let _ = writeln!(out, "  Repetitions: {REPETITIONS}");
    let _ = writeln!(out, "{'=':.>72}");
    let _ = writeln!(
        out,
        "  {:>8} {:>12} {:>12} {:>10} {:>10} {:>8}",
        "Workers", "p50 ops/s", "p95 ops/s", "Speedup", "Retries", "Aborts"
    );
    let _ = writeln!(out, "  {:-<72}", "");

    for m in results {
        let p50 = m.p50_ops_per_sec();
        let p95 = m.p95_ops_per_sec();
        let speedup = if baseline_ops > 0.0 {
            p50 / baseline_ops
        } else {
            1.0
        };
        let _ = writeln!(
            out,
            "  {:>8} {:>12.1} {:>12.1} {:>9.2}x {:>10} {:>8}",
            m.worker_count,
            p50,
            p95,
            speedup,
            m.median_retries(),
            m.median_aborts()
        );
    }

    let _ = writeln!(out, "{'=':.>72}\n");
    out
}

// ── Individual tests: Disjoint partition mode ────────────────────────────

#[test]
fn mvcc_disjoint_1_worker() {
    let m = run_disjoint_scaling(1);
    assert!(!m.any_error(), "errors at 1 worker: {:?}", m.errors);
    assert!(m.p50_ops_per_sec() > 0.0);
}

#[test]
fn mvcc_disjoint_2_workers() {
    let m = run_disjoint_scaling(2);
    assert!(!m.any_error(), "errors at 2 workers: {:?}", m.errors);
    assert!(m.p50_ops_per_sec() > 0.0);
}

#[test]
fn mvcc_disjoint_4_workers() {
    let m = run_disjoint_scaling(4);
    assert!(!m.any_error(), "errors at 4 workers: {:?}", m.errors);
    assert!(m.p50_ops_per_sec() > 0.0);
}

#[test]
fn mvcc_disjoint_8_workers() {
    let m = run_disjoint_scaling(8);
    assert!(!m.any_error(), "errors at 8 workers: {:?}", m.errors);
    assert!(m.p50_ops_per_sec() > 0.0);
}

#[test]
fn mvcc_disjoint_16_workers() {
    let m = run_disjoint_scaling(16);
    assert!(!m.any_error(), "errors at 16 workers: {:?}", m.errors);
    assert!(m.p50_ops_per_sec() > 0.0);
}

// ── Individual tests: Controlled contention mode ─────────────────────────

#[test]
fn mvcc_contention_1_worker() {
    let m = run_contention_scaling(1);
    assert!(!m.any_error(), "errors at 1 worker: {:?}", m.errors);
}

#[test]
fn mvcc_contention_2_workers() {
    let m = run_contention_scaling(2);
    // Contention with 2+ workers may produce retries; that's expected.
    assert!(m.p50_ops_per_sec() > 0.0);
}

#[test]
fn mvcc_contention_4_workers() {
    let m = run_contention_scaling(4);
    assert!(m.p50_ops_per_sec() > 0.0);
}

#[test]
fn mvcc_contention_8_workers() {
    let m = run_contention_scaling(8);
    assert!(m.p50_ops_per_sec() > 0.0);
}

#[test]
fn mvcc_contention_16_workers() {
    let m = run_contention_scaling(16);
    assert!(m.p50_ops_per_sec() > 0.0);
}

// ── Scaling report (run with --nocapture) ────────────────────────────────

#[test]
fn mvcc_scaling_report_disjoint_partitions() {
    let results: Vec<RunMetrics> = CONCURRENCY_LEVELS
        .iter()
        .map(|&w| run_disjoint_scaling(w))
        .collect();

    let baseline_ops = results
        .first()
        .map_or(0.0, |m| m.p50_ops_per_sec());

    let table = format_scaling_table("Disjoint Partitions (zero contention)", &results, baseline_ops);
    println!("{table}");

    // Verify no fatal errors at any level.
    for m in &results {
        assert!(!m.any_error(), "errors at {} workers: {:?}", m.worker_count, m.errors);
    }
}

#[test]
fn mvcc_scaling_report_hot_page_contention() {
    let results: Vec<RunMetrics> = CONCURRENCY_LEVELS
        .iter()
        .map(|&w| run_contention_scaling(w))
        .collect();

    let baseline_ops = results
        .first()
        .map_or(0.0, |m| m.p50_ops_per_sec());

    let table = format_scaling_table("Hot-Page Contention (SSI conflict)", &results, baseline_ops);
    println!("{table}");

    // Contention mode: verify retry counts increase with worker count (expected).
    if results.len() >= 2 {
        let retries_1 = results[0].median_retries();
        let retries_max = results.last().map_or(0, RunMetrics::median_retries);
        // With 16 workers hitting the same rows, retries should be >= single-worker.
        assert!(
            retries_max >= retries_1,
            "expected retries to increase with contention: 1w={retries_1}, max_w={retries_max}"
        );
    }
}

// ── FrankenSQLite baseline comparison ────────────────────────────────────

#[test]
fn mvcc_fsqlite_baseline_disjoint() {
    // Run the same disjoint workload on FrankenSQLite (sequential) as a baseline.
    let oplog = preset_commutative_inserts_disjoint_keys(
        "mvcc-baseline-disjoint",
        SEED,
        4,
        DISJOINT_ROWS_PER_WORKER,
    );
    let report = run_fsqlite_sequential(&oplog);
    assert!(report.error.is_none(), "fsqlite error: {:?}", report.error);
    assert!(report.ops_total > 0, "should have executed operations");
    println!(
        "FrankenSQLite baseline (4w sequential): {} ops, {:.1} ops/s, {}ms",
        report.ops_total, report.ops_per_sec, report.wall_time_ms
    );
}

#[test]
fn mvcc_fsqlite_baseline_contention() {
    let oplog = preset_hot_page_contention("mvcc-baseline-contention", SEED, 4, CONTENTION_ROUNDS);
    let report = run_fsqlite_sequential(&oplog);
    // Sequential execution has no contention, so should succeed.
    assert!(report.error.is_none(), "fsqlite error: {:?}", report.error);
    assert!(report.ops_total > 0, "should have executed operations");
    println!(
        "FrankenSQLite baseline (4w contention, sequential): {} ops, {:.1} ops/s, {}ms",
        report.ops_total, report.ops_per_sec, report.wall_time_ms
    );
}

// ── Combined scaling comparison ──────────────────────────────────────────

#[test]
fn mvcc_combined_scaling_comparison() {
    let mut report = String::new();
    let _ = writeln!(report, "\n{'#':.>72}");
    let _ = writeln!(report, "  MVCC Concurrent Writers — Full Scaling Report");
    let _ = writeln!(report, "  Bead: bd-1w6k.4.3");
    let _ = writeln!(report, "{'#':.>72}");

    // ── Disjoint partition mode ──
    let disjoint: Vec<RunMetrics> = CONCURRENCY_LEVELS
        .iter()
        .map(|&w| run_disjoint_scaling(w))
        .collect();
    let base_disjoint = disjoint.first().map_or(0.0, |m| m.p50_ops_per_sec());
    let _ = writeln!(
        report,
        "{}",
        format_scaling_table("Disjoint Partitions", &disjoint, base_disjoint)
    );

    // ── Contention mode ──
    let contention: Vec<RunMetrics> = CONCURRENCY_LEVELS
        .iter()
        .map(|&w| run_contention_scaling(w))
        .collect();
    let base_contention = contention.first().map_or(0.0, |m| m.p50_ops_per_sec());
    let _ = writeln!(
        report,
        "{}",
        format_scaling_table("Hot-Page Contention", &contention, base_contention)
    );

    // ── FrankenSQLite baseline ──
    let _ = writeln!(report, "  FrankenSQLite Sequential Baselines:");
    let _ = writeln!(report, "  {:-<72}", "");

    let fs_disjoint = {
        let oplog = preset_commutative_inserts_disjoint_keys("baseline-d", SEED, 4, DISJOINT_ROWS_PER_WORKER);
        run_fsqlite_sequential(&oplog)
    };
    let _ = writeln!(
        report,
        "  Disjoint (4w seq):   {:>8} ops  {:>10.1} ops/s  {:>6}ms",
        fs_disjoint.ops_total, fs_disjoint.ops_per_sec, fs_disjoint.wall_time_ms
    );

    let fs_contention = {
        let oplog = preset_hot_page_contention("baseline-c", SEED, 4, CONTENTION_ROUNDS);
        run_fsqlite_sequential(&oplog)
    };
    let _ = writeln!(
        report,
        "  Contention (4w seq): {:>8} ops  {:>10.1} ops/s  {:>6}ms",
        fs_contention.ops_total, fs_contention.ops_per_sec, fs_contention.wall_time_ms
    );

    let _ = writeln!(report, "\n{'#':.>72}");
    println!("{report}");
}
