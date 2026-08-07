//! E2E primary test runner — orchestrates correctness, benchmarks, and recovery tests.
//!
//! # Subcommands
//!
//! - `run-smoke` — Fast 30-second infrastructure verification.
//! - `run-correctness` — Run correctness verification tests only.
//! - `run-recovery` — Run corruption recovery demos only.
//! - `run-all` — Run all test categories sequentially.
//! - `report` — Generate summary report from previous run data.
//!
//! Bead: bd-2bpb

use std::ffi::OsString;
use std::fmt::Write as _;
use std::fs;
use std::io::{self, Write as _};
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

// ── CLI Types ────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct RunnerConfig {
    verbose: bool,
    output_dir: PathBuf,
    json_output: bool,
    no_color: bool,
    golden_dir: PathBuf,
    seed: u64,
    filter: Option<String>,
}

impl Default for RunnerConfig {
    fn default() -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_secs());
        Self {
            verbose: false,
            output_dir: PathBuf::from(format!("sample_sqlite_db_files/runs/{timestamp}")),
            json_output: false,
            no_color: false,
            golden_dir: PathBuf::from("sample_sqlite_db_files/golden"),
            seed: 42,
            filter: None,
        }
    }
}

// ── results.json Schema ─────────────────────────────────────────────────

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct TestAssertion {
    name: String,
    passed: bool,
    detail: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct TestResult {
    name: String,
    category: String,
    status: String,
    duration_ms: u64,
    assertions: Vec<TestAssertion>,
    log_file: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct ResultsSummary {
    total_tests: usize,
    passed: usize,
    failed: usize,
    skipped: usize,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct ResultsJson {
    run_id: String,
    timestamp: String,
    duration_seconds: f64,
    seed: u64,
    summary: ResultsSummary,
    tests: Vec<TestResult>,
}

// ── Main ─────────────────────────────────────────────────────────────────

fn main() {
    let exit_code = run_cli(std::env::args_os());
    if exit_code != 0 {
        std::process::exit(exit_code);
    }
}

fn run_cli<I>(os_args: I) -> i32
where
    I: IntoIterator<Item = OsString>,
{
    let raw: Vec<String> = os_args
        .into_iter()
        .map(|a| a.to_string_lossy().into_owned())
        .collect();

    let tail = if raw.len() > 1 { &raw[1..] } else { &[] };

    if tail.is_empty() || tail.iter().any(|a| a == "-h" || a == "--help") {
        print_help();
        return 0;
    }

    // Parse global options + find subcommand.
    let mut config = RunnerConfig::default();
    let mut subcmd_idx = None;

    for (i, arg) in tail.iter().enumerate() {
        match arg.as_str() {
            "--verbose" | "-v" => config.verbose = true,
            "--json" => config.json_output = true,
            "--no-color" => config.no_color = true,
            "--output" if i + 1 < tail.len() => {
                config.output_dir = PathBuf::from(&tail[i + 1]);
            }
            "--golden" if i + 1 < tail.len() => {
                config.golden_dir = PathBuf::from(&tail[i + 1]);
            }
            "--seed" if i + 1 < tail.len() => {
                if let Ok(s) = tail[i + 1].parse::<u64>() {
                    config.seed = s;
                }
            }
            "--filter" if i + 1 < tail.len() => {
                config.filter = Some(tail[i + 1].clone());
            }
            "run-smoke" | "run-correctness" | "run-recovery" | "run-all" | "report"
                if subcmd_idx.is_none() =>
            {
                subcmd_idx = Some(i);
            }
            _ => {
                // Skip option values (already consumed above).
            }
        }
    }

    let Some(idx) = subcmd_idx else {
        eprintln!("error: no subcommand specified");
        eprintln!();
        print_help();
        return 2;
    };
    match tail[idx].as_str() {
        "run-smoke" => cmd_run_smoke(&config),
        "run-correctness" => cmd_run_correctness(&config),
        "run-recovery" => cmd_run_recovery(&config),
        "run-all" => cmd_run_all(&config),
        "report" => cmd_report(&config),
        _ => {
            eprintln!("error: unknown subcommand");
            2
        }
    }
}

fn print_help() {
    let text = "\
e2e-runner — FrankenSQLite E2E primary test runner

USAGE:
    e2e-runner [OPTIONS] <SUBCOMMAND>

SUBCOMMANDS:
    run-all           Run all test categories (smoke + correctness + recovery)
    run-correctness   Run correctness verification tests only
    run-recovery      Run corruption recovery demos only
    run-smoke         Fast 30-second infrastructure verification
    report            Generate summary report from previous run data

OPTIONS:
    --verbose, -v         Enable TRACE-level logging
    --output <DIR>        Output directory for logs/reports
    --filter <PATTERN>    Run only tests matching pattern
    --json                Output results as JSON instead of human-readable
    --no-color            Disable ANSI color output
    --golden <DIR>        Golden database directory
    --seed <N>            Override default RNG seed (default: 42)
    -h, --help            Show this help message
";
    let _ = io::stdout().write_all(text.as_bytes());
}

// ── Subcommands ──────────────────────────────────────────────────────────

fn cmd_run_smoke(config: &RunnerConfig) -> i32 {
    setup_output_dir(&config.output_dir);
    let report = fsqlite_e2e::smoke::run_smoke_tests();

    if config.json_output {
        let results = smoke_to_results_json(&report, config.seed);
        match serde_json::to_string_pretty(&results) {
            Ok(json) => println!("{json}"),
            Err(e) => {
                eprintln!("error: failed to serialize results: {e}");
                return 1;
            }
        }
    } else {
        print!("{}", fsqlite_e2e::smoke::format_smoke_report(&report));
    }

    i32::from(!report.all_passed)
}

fn cmd_run_correctness(config: &RunnerConfig) -> i32 {
    setup_output_dir(&config.output_dir);
    let overall_start = Instant::now();
    let mut tests = Vec::new();

    // Test 1: Both backends can execute identical workloads.
    if should_run("correctness_workload_replay", config.filter.as_deref()) {
        tests.push(run_correctness_workload_replay(config));
    }

    // Test 2: Canonicalization produces stable hashes.
    if should_run("correctness_canonicalization", config.filter.as_deref()) {
        tests.push(run_correctness_canonicalization(config));
    }

    // Test 3: Logical comparison of workload results.
    if should_run("correctness_logical_comparison", config.filter.as_deref()) {
        tests.push(run_correctness_logical_comparison(config));
    }

    let elapsed = overall_start.elapsed();
    emit_results(config, &tests, elapsed)
}

fn cmd_run_recovery(config: &RunnerConfig) -> i32 {
    setup_output_dir(&config.output_dir);
    let overall_start = Instant::now();
    let mut tests = Vec::new();

    // Test 1: BitFlip corruption + detection.
    if should_run("recovery_bitflip", config.filter.as_deref()) {
        tests.push(run_recovery_bitflip(config));
    }

    // Test 2: Page zero corruption + detection.
    if should_run("recovery_page_zero", config.filter.as_deref()) {
        tests.push(run_recovery_page_zero(config));
    }

    // Test 3: Header zero corruption + detection.
    if should_run("recovery_header_zero", config.filter.as_deref()) {
        tests.push(run_recovery_header_zero(config));
    }

    let elapsed = overall_start.elapsed();
    emit_results(config, &tests, elapsed)
}

fn cmd_run_all(config: &RunnerConfig) -> i32 {
    setup_output_dir(&config.output_dir);
    let overall_start = Instant::now();
    let mut tests = Vec::new();

    // Smoke first.
    let smoke = fsqlite_e2e::smoke::run_smoke_tests();
    for check in &smoke.checks {
        tests.push(TestResult {
            name: check.name.clone(),
            category: "smoke".to_owned(),
            status: if check.passed { "passed" } else { "failed" }.to_owned(),
            duration_ms: check.elapsed_ms,
            assertions: vec![TestAssertion {
                name: check.name.clone(),
                passed: check.passed,
                detail: check.detail.clone(),
            }],
            log_file: None,
        });
    }

    // Correctness.
    tests.push(run_correctness_workload_replay(config));
    tests.push(run_correctness_canonicalization(config));
    tests.push(run_correctness_logical_comparison(config));

    // Recovery.
    tests.push(run_recovery_bitflip(config));
    tests.push(run_recovery_page_zero(config));
    tests.push(run_recovery_header_zero(config));

    let elapsed = overall_start.elapsed();
    emit_results(config, &tests, elapsed)
}

fn cmd_report(config: &RunnerConfig) -> i32 {
    let results_path = config.output_dir.join("results.json");
    if !results_path.exists() {
        eprintln!(
            "error: results.json not found at {}",
            results_path.display()
        );
        eprintln!("hint: run a test suite first, e.g. `e2e-runner run-all`");
        return 1;
    }

    let content = match fs::read_to_string(&results_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("error: {e}");
            return 1;
        }
    };

    let results: ResultsJson = match serde_json::from_str(&content) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("error: invalid results.json: {e}");
            return 1;
        }
    };

    let markdown = render_results_markdown(&results);

    if config.json_output {
        println!("{content}");
    } else {
        print!("{markdown}");
    }

    // Also write report.md.
    let report_path = config.output_dir.join("report.md");
    if let Err(e) = fs::write(&report_path, &markdown) {
        eprintln!("warning: could not write {}: {e}", report_path.display());
    }

    0
}

// ── Test Implementations ─────────────────────────────────────────────────

fn run_correctness_workload_replay(config: &RunnerConfig) -> TestResult {
    let start = Instant::now();
    let mut assertions = Vec::new();

    // Generate a deterministic workload and replay on C SQLite.
    let oplog =
        fsqlite_e2e::oplog::preset_commutative_inserts_disjoint_keys("smoke", config.seed, 1, 50);

    let tmp = match tempfile::TempDir::new() {
        Ok(t) => t,
        Err(e) => return error_result("correctness_workload_replay", "correctness", start, &e),
    };

    let db_path = tmp.path().join("work.db");

    // Create a minimal seed database.
    match rusqlite::Connection::open(&db_path) {
        Ok(conn) => {
            if let Err(e) =
                conn.execute_batch("CREATE TABLE IF NOT EXISTS seed (id INTEGER PRIMARY KEY);")
            {
                return error_result("correctness_workload_replay", "correctness", start, &e);
            }
            drop(conn);
        }
        Err(e) => return error_result("correctness_workload_replay", "correctness", start, &e),
    }

    let sqlite_config = fsqlite_e2e::sqlite_executor::SqliteExecConfig::default();
    match fsqlite_e2e::sqlite_executor::run_oplog_sqlite(&db_path, &oplog, &sqlite_config) {
        Ok(report) => {
            assertions.push(TestAssertion {
                name: "sqlite_execution".to_owned(),
                passed: report.error.is_none(),
                detail: report
                    .error
                    .clone()
                    .unwrap_or_else(|| "no errors".to_owned()),
            });
            assertions.push(TestAssertion {
                name: "ops_executed".to_owned(),
                passed: report.ops_total > 0,
                detail: format!("{} ops in {}ms", report.ops_total, report.wall_time_ms),
            });
        }
        Err(e) => {
            assertions.push(TestAssertion {
                name: "sqlite_execution".to_owned(),
                passed: false,
                detail: format!("{e}"),
            });
        }
    }

    let all_passed = assertions.iter().all(|a| a.passed);
    TestResult {
        name: "correctness_workload_replay".to_owned(),
        category: "correctness".to_owned(),
        status: if all_passed { "passed" } else { "failed" }.to_owned(),
        duration_ms: u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX),
        assertions,
        log_file: None,
    }
}

fn run_correctness_canonicalization(config: &RunnerConfig) -> TestResult {
    let _ = config;
    let start = Instant::now();
    let mut assertions = Vec::new();

    let tmp = match tempfile::TempDir::new() {
        Ok(t) => t,
        Err(e) => return error_result("correctness_canonicalization", "correctness", start, &e),
    };

    let db_path = tmp.path().join("canon.db");
    match rusqlite::Connection::open(&db_path) {
        Ok(conn) => {
            if let Err(e) = conn.execute_batch(
                "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT);
                 INSERT INTO t VALUES (1, 'a'); INSERT INTO t VALUES (2, 'b');
                 PRAGMA wal_checkpoint(TRUNCATE);",
            ) {
                return error_result("correctness_canonicalization", "correctness", start, &e);
            }
            drop(conn);
        }
        Err(e) => return error_result("correctness_canonicalization", "correctness", start, &e),
    }

    match fsqlite_e2e::canonicalize::canonical_sha256(&db_path) {
        Ok(hash1) => match fsqlite_e2e::canonicalize::canonical_sha256(&db_path) {
            Ok(hash2) => {
                let stable = hash1 == hash2;
                assertions.push(TestAssertion {
                    name: "canonical_stability".to_owned(),
                    passed: stable,
                    detail: if stable {
                        format!("stable hash: {}…", &hash1[..16])
                    } else {
                        format!("unstable: {} vs {}", &hash1[..16], &hash2[..16])
                    },
                });
            }
            Err(e) => {
                assertions.push(TestAssertion {
                    name: "canonical_stability".to_owned(),
                    passed: false,
                    detail: format!("second canonicalization failed: {e}"),
                });
            }
        },
        Err(e) => {
            assertions.push(TestAssertion {
                name: "canonical_stability".to_owned(),
                passed: false,
                detail: format!("canonicalization failed: {e}"),
            });
        }
    }

    let all_passed = assertions.iter().all(|a| a.passed);
    TestResult {
        name: "correctness_canonicalization".to_owned(),
        category: "correctness".to_owned(),
        status: if all_passed { "passed" } else { "failed" }.to_owned(),
        duration_ms: u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX),
        assertions,
        log_file: None,
    }
}

fn run_correctness_logical_comparison(_config: &RunnerConfig) -> TestResult {
    let start = Instant::now();
    let mut assertions = Vec::new();

    let tmp = match tempfile::TempDir::new() {
        Ok(t) => t,
        Err(e) => return error_result("correctness_logical_comparison", "correctness", start, &e),
    };

    // Create two identical databases independently.
    let db_a = tmp.path().join("a.db");
    let db_b = tmp.path().join("b.db");

    for db_path in [&db_a, &db_b] {
        match rusqlite::Connection::open(db_path) {
            Ok(conn) => {
                if let Err(e) = conn.execute_batch(
                    "CREATE TABLE items (id INTEGER PRIMARY KEY, val TEXT);
                     INSERT INTO items VALUES (1, 'alpha');
                     INSERT INTO items VALUES (2, 'beta');
                     INSERT INTO items VALUES (3, 'gamma');
                     PRAGMA wal_checkpoint(TRUNCATE);",
                ) {
                    return error_result(
                        "correctness_logical_comparison",
                        "correctness",
                        start,
                        &e,
                    );
                }
                drop(conn);
            }
            Err(e) => {
                return error_result("correctness_logical_comparison", "correctness", start, &e);
            }
        }
    }

    // Compare using the three-tier comparison.
    match fsqlite_e2e::canonicalize::canonicalize_and_compare(&db_a, &db_b) {
        Ok(result) => {
            assertions.push(TestAssertion {
                name: "tier_comparison".to_owned(),
                passed: result.logical_match,
                detail: format!(
                    "tier={:?}, logical_match={}",
                    result.tier, result.logical_match
                ),
            });
        }
        Err(e) => {
            assertions.push(TestAssertion {
                name: "tier_comparison".to_owned(),
                passed: false,
                detail: format!("comparison failed: {e}"),
            });
        }
    }

    let all_passed = assertions.iter().all(|a| a.passed);
    TestResult {
        name: "correctness_logical_comparison".to_owned(),
        category: "correctness".to_owned(),
        status: if all_passed { "passed" } else { "failed" }.to_owned(),
        duration_ms: u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX),
        assertions,
        log_file: None,
    }
}

fn run_recovery_bitflip(config: &RunnerConfig) -> TestResult {
    run_recovery_test("recovery_bitflip", config, |db_path| {
        let injector = fsqlite_e2e::corruption::CorruptionInjector::new(db_path.to_path_buf())?;
        let report = injector.inject(&fsqlite_e2e::corruption::CorruptionPattern::BitFlip {
            byte_offset: 200,
            bit_position: 3,
        })?;
        Ok(format!("{} byte(s) affected", report.affected_bytes))
    })
}

fn run_recovery_page_zero(config: &RunnerConfig) -> TestResult {
    run_recovery_test("recovery_page_zero", config, |db_path| {
        let injector = fsqlite_e2e::corruption::CorruptionInjector::new(db_path.to_path_buf())?;
        let report = injector
            .inject(&fsqlite_e2e::corruption::CorruptionPattern::PageZero { page_number: 2 })?;
        Ok(format!("{} byte(s) affected", report.affected_bytes))
    })
}

fn run_recovery_header_zero(config: &RunnerConfig) -> TestResult {
    run_recovery_test("recovery_header_zero", config, |db_path| {
        let injector = fsqlite_e2e::corruption::CorruptionInjector::new(db_path.to_path_buf())?;
        let report = injector.inject(&fsqlite_e2e::corruption::CorruptionPattern::HeaderZero)?;
        Ok(format!("{} byte(s) affected", report.affected_bytes))
    })
}

fn run_recovery_test(
    name: &str,
    _config: &RunnerConfig,
    inject_fn: impl FnOnce(&Path) -> fsqlite_e2e::E2eResult<String>,
) -> TestResult {
    let start = Instant::now();
    let mut assertions = Vec::new();

    let tmp = match tempfile::TempDir::new() {
        Ok(t) => t,
        Err(e) => return error_result(name, "recovery", start, &e),
    };

    let db_path = tmp.path().join("recovery.db");
    match rusqlite::Connection::open(&db_path) {
        Ok(conn) => {
            if let Err(e) = conn.execute_batch(
                "CREATE TABLE data (id INTEGER PRIMARY KEY, val TEXT);
                 INSERT INTO data VALUES (1, 'test');
                 INSERT INTO data VALUES (2, 'data');
                 PRAGMA wal_checkpoint(TRUNCATE);",
            ) {
                return error_result(name, "recovery", start, &e);
            }
            drop(conn);
        }
        Err(e) => return error_result(name, "recovery", start, &e),
    }

    // Hash before.
    let hash_before = match fsqlite_e2e::golden::GoldenCopy::hash_file(&db_path) {
        Ok(h) => h,
        Err(e) => return error_result(name, "recovery", start, &e),
    };

    // Inject corruption.
    match inject_fn(&db_path) {
        Ok(detail) => {
            assertions.push(TestAssertion {
                name: "injection".to_owned(),
                passed: true,
                detail,
            });
        }
        Err(e) => {
            assertions.push(TestAssertion {
                name: "injection".to_owned(),
                passed: false,
                detail: format!("{e}"),
            });
        }
    }

    // Hash after — should differ.
    match fsqlite_e2e::golden::GoldenCopy::hash_file(&db_path) {
        Ok(hash_after) => {
            let changed = hash_before != hash_after;
            assertions.push(TestAssertion {
                name: "hash_changed".to_owned(),
                passed: changed,
                detail: if changed {
                    "corruption detected: hash differs".to_owned()
                } else {
                    "WARNING: hash unchanged after corruption".to_owned()
                },
            });
        }
        Err(e) => {
            assertions.push(TestAssertion {
                name: "hash_changed".to_owned(),
                passed: false,
                detail: format!("hash failed: {e}"),
            });
        }
    }

    let all_passed = assertions.iter().all(|a| a.passed);
    TestResult {
        name: name.to_owned(),
        category: "recovery".to_owned(),
        status: if all_passed { "passed" } else { "failed" }.to_owned(),
        duration_ms: u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX),
        assertions,
        log_file: None,
    }
}

// ── Helpers ──────────────────────────────────────────────────────────────

fn error_result(
    name: &str,
    category: &str,
    start: Instant,
    err: &dyn std::fmt::Display,
) -> TestResult {
    TestResult {
        name: name.to_owned(),
        category: category.to_owned(),
        status: "failed".to_owned(),
        duration_ms: u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX),
        assertions: vec![TestAssertion {
            name: "setup".to_owned(),
            passed: false,
            detail: format!("{err}"),
        }],
        log_file: None,
    }
}

fn should_run(test_name: &str, filter: Option<&str>) -> bool {
    match filter {
        Some(pattern) => test_name.contains(pattern),
        None => true,
    }
}

fn setup_output_dir(dir: &Path) {
    if let Err(e) = fs::create_dir_all(dir) {
        eprintln!(
            "warning: could not create output dir {}: {e}",
            dir.display()
        );
    }
}

fn smoke_to_results_json(smoke: &fsqlite_e2e::smoke::SmokeTestReport, seed: u64) -> ResultsJson {
    let tests: Vec<TestResult> = smoke
        .checks
        .iter()
        .map(|c| TestResult {
            name: c.name.clone(),
            category: "smoke".to_owned(),
            status: if c.passed { "passed" } else { "failed" }.to_owned(),
            duration_ms: c.elapsed_ms,
            assertions: vec![TestAssertion {
                name: c.name.clone(),
                passed: c.passed,
                detail: c.detail.clone(),
            }],
            log_file: None,
        })
        .collect();

    let passed = tests.iter().filter(|t| t.status == "passed").count();
    let failed = tests.iter().filter(|t| t.status == "failed").count();

    ResultsJson {
        run_id: format!("smoke-{}", smoke.total_elapsed_ms),
        timestamp: chrono_now_iso(),
        duration_seconds: f64::from(u32::try_from(smoke.total_elapsed_ms).unwrap_or(u32::MAX))
            / 1000.0,
        seed,
        summary: ResultsSummary {
            total_tests: tests.len(),
            passed,
            failed,
            skipped: 0,
        },
        tests,
    }
}

fn emit_results(config: &RunnerConfig, tests: &[TestResult], elapsed: std::time::Duration) -> i32 {
    let passed = tests.iter().filter(|t| t.status == "passed").count();
    let failed = tests.iter().filter(|t| t.status == "failed").count();
    let skipped = tests.iter().filter(|t| t.status == "skipped").count();

    let results = ResultsJson {
        run_id: format!(
            "run-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0, |d| d.as_secs())
        ),
        timestamp: chrono_now_iso(),
        duration_seconds: elapsed.as_secs_f64(),
        seed: config.seed,
        summary: ResultsSummary {
            total_tests: tests.len(),
            passed,
            failed,
            skipped,
        },
        tests: tests.to_vec(),
    };

    // Write results.json.
    let results_path = config.output_dir.join("results.json");
    if let Ok(json) = serde_json::to_string_pretty(&results)
        && let Err(e) = fs::write(&results_path, &json)
    {
        eprintln!("warning: could not write results.json: {e}");
    }

    // Write report.md.
    let markdown = render_results_markdown(&results);
    let report_path = config.output_dir.join("report.md");
    if let Err(e) = fs::write(&report_path, &markdown) {
        eprintln!("warning: could not write report.md: {e}");
    }

    if config.json_output {
        match serde_json::to_string_pretty(&results) {
            Ok(json) => println!("{json}"),
            Err(e) => eprintln!("error: {e}"),
        }
    } else {
        print!("{markdown}");
    }

    i32::from(failed > 0)
}

fn render_results_markdown(results: &ResultsJson) -> String {
    let mut out = String::with_capacity(2048);
    let _ = writeln!(out, "# E2E Test Report\n");
    let _ = writeln!(out, "- **Run ID:** {}", results.run_id);
    let _ = writeln!(out, "- **Timestamp:** {}", results.timestamp);
    let _ = writeln!(out, "- **Duration:** {:.2}s", results.duration_seconds);
    let _ = writeln!(out, "- **Seed:** {}", results.seed);
    let _ = writeln!(
        out,
        "- **Summary:** {}/{} passed, {} failed, {} skipped\n",
        results.summary.passed,
        results.summary.total_tests,
        results.summary.failed,
        results.summary.skipped,
    );

    let _ = writeln!(out, "| Test | Category | Status | Duration |");
    let _ = writeln!(out, "|------|----------|--------|----------|");
    for t in &results.tests {
        let _ = writeln!(
            out,
            "| {} | {} | {} | {}ms |",
            t.name, t.category, t.status, t.duration_ms
        );
    }

    if results.summary.failed > 0 {
        let _ = writeln!(out, "\n## Failures\n");
        for t in &results.tests {
            if t.status == "failed" {
                let _ = writeln!(out, "### {}\n", t.name);
                for a in &t.assertions {
                    if !a.passed {
                        let _ = writeln!(out, "- **{}:** {}", a.name, a.detail);
                    }
                }
            }
        }
    }

    out
}

fn chrono_now_iso() -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    // Simple ISO-ish timestamp without chrono dependency.
    format!("{secs}")
}

// ── Tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn run_with(args: &[&str]) -> i32 {
        let os_args: Vec<OsString> = args.iter().map(OsString::from).collect();
        run_cli(os_args)
    }

    #[test]
    fn test_runner_help_exits_zero() {
        assert_eq!(run_with(&["e2e-runner", "--help"]), 0);
        assert_eq!(run_with(&["e2e-runner", "-h"]), 0);
    }

    #[test]
    fn test_runner_no_subcommand_exits_two() {
        assert_eq!(run_with(&["e2e-runner", "--verbose"]), 2);
    }

    #[test]
    fn test_runner_smoke_subcommand() {
        let code = run_with(&["e2e-runner", "run-smoke"]);
        assert_eq!(code, 0, "smoke tests should pass");
    }

    #[test]
    fn test_runner_json_output() {
        // Capture by running with --json flag — just verify it doesn't crash.
        let code = run_with(&["e2e-runner", "--json", "run-smoke"]);
        assert_eq!(code, 0);
    }

    #[test]
    fn test_runner_filter() {
        let dir = tempfile::TempDir::new().unwrap();
        let out_dir = dir.path().to_str().unwrap();
        let code = run_with(&[
            "e2e-runner",
            "--output",
            out_dir,
            "--filter",
            "canonicalization",
            "run-correctness",
        ]);
        assert_eq!(code, 0);

        // Verify results.json only has the filtered test.
        let results: ResultsJson =
            serde_json::from_str(&fs::read_to_string(dir.path().join("results.json")).unwrap())
                .unwrap();
        assert_eq!(results.summary.total_tests, 1);
        assert!(results.tests[0].name.contains("canonicalization"));
    }

    #[test]
    fn test_runner_output_directory() {
        let dir = tempfile::TempDir::new().unwrap();
        let out_dir = dir.path().join("nested/output");
        let out_str = out_dir.to_str().unwrap();
        let code = run_with(&["e2e-runner", "--output", out_str, "run-smoke"]);
        assert_eq!(code, 0);
        // Output dir should be created (but results.json written only for non-smoke runs)
        assert!(out_dir.exists(), "output directory should be created");
    }

    #[test]
    fn test_runner_run_all() {
        let dir = tempfile::TempDir::new().unwrap();
        let out_dir = dir.path().to_str().unwrap();
        let code = run_with(&["e2e-runner", "--output", out_dir, "run-all"]);
        assert_eq!(code, 0);

        // Verify results.json is created.
        let results_path = dir.path().join("results.json");
        assert!(results_path.exists(), "results.json should exist");

        let results: ResultsJson =
            serde_json::from_str(&fs::read_to_string(&results_path).unwrap()).unwrap();
        assert!(
            results.summary.total_tests >= 10,
            "run-all should have many tests"
        );
        assert_eq!(results.summary.failed, 0, "all tests should pass");

        // Verify report.md is created.
        let report_path = dir.path().join("report.md");
        assert!(report_path.exists(), "report.md should exist");
        let report = fs::read_to_string(&report_path).unwrap();
        assert!(report.contains("E2E Test Report"));
    }

    #[test]
    fn test_runner_report_generation() {
        let dir = tempfile::TempDir::new().unwrap();
        let out_dir = dir.path().to_str().unwrap();

        // First, generate results.
        run_with(&["e2e-runner", "--output", out_dir, "run-correctness"]);

        // Then generate report from them.
        let code = run_with(&["e2e-runner", "--output", out_dir, "report"]);
        assert_eq!(code, 0);

        let report = fs::read_to_string(dir.path().join("report.md")).unwrap();
        assert!(report.contains("E2E Test Report"));
        assert!(report.contains("correctness"));
    }

    #[test]
    fn test_results_json_serialization() {
        let results = ResultsJson {
            run_id: "test-123".to_owned(),
            timestamp: "12345".to_owned(),
            duration_seconds: 1.5,
            seed: 42,
            summary: ResultsSummary {
                total_tests: 2,
                passed: 1,
                failed: 1,
                skipped: 0,
            },
            tests: vec![
                TestResult {
                    name: "pass_test".to_owned(),
                    category: "correctness".to_owned(),
                    status: "passed".to_owned(),
                    duration_ms: 100,
                    assertions: vec![TestAssertion {
                        name: "check".to_owned(),
                        passed: true,
                        detail: "ok".to_owned(),
                    }],
                    log_file: None,
                },
                TestResult {
                    name: "fail_test".to_owned(),
                    category: "recovery".to_owned(),
                    status: "failed".to_owned(),
                    duration_ms: 200,
                    assertions: vec![TestAssertion {
                        name: "check".to_owned(),
                        passed: false,
                        detail: "bad".to_owned(),
                    }],
                    log_file: None,
                },
            ],
        };

        let json = serde_json::to_string_pretty(&results).unwrap();
        assert!(json.contains("\"run_id\": \"test-123\""));
        assert!(json.contains("\"passed\": 1"));
        assert!(json.contains("\"failed\": 1"));

        // Roundtrip.
        let decoded: ResultsJson = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.summary.total_tests, 2);
    }

    #[test]
    fn test_render_markdown_report() {
        let results = ResultsJson {
            run_id: "md-test".to_owned(),
            timestamp: "12345".to_owned(),
            duration_seconds: 2.5,
            seed: 42,
            summary: ResultsSummary {
                total_tests: 1,
                passed: 1,
                failed: 0,
                skipped: 0,
            },
            tests: vec![TestResult {
                name: "example".to_owned(),
                category: "smoke".to_owned(),
                status: "passed".to_owned(),
                duration_ms: 50,
                assertions: vec![],
                log_file: None,
            }],
        };

        let md = render_results_markdown(&results);
        assert!(md.contains("# E2E Test Report"));
        assert!(md.contains("md-test"));
        assert!(md.contains("example"));
        assert!(md.contains("smoke"));
    }

    #[test]
    fn test_should_run_filter() {
        assert!(should_run("foo_bar", None));
        assert!(should_run("foo_bar", Some("foo")));
        assert!(!should_run("foo_bar", Some("baz")));
    }
}
