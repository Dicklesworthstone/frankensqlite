//! Fast smoke test: 30-second infrastructure verification before full E2E.
//!
//! Bead: bd-dcjy
//!
//! Verifies that all critical E2E infrastructure is functional:
//! 1. Golden copy integrity (rusqlite can open and integrity-check)
//! 2. FrankenSQLite backend (in-memory CREATE/INSERT/SELECT)
//! 3. C SQLite / rusqlite backend (in-memory CREATE/INSERT/SELECT)
//! 4. Workload generator determinism (same seed → same OpLog)
//! 5. Corruption injector (inject + detect change)
//! 6. Canonicalization pipeline (VACUUM INTO + stable SHA-256)
//! 7. Logging pipeline (JSON-lines output + structured fields)
//!
//! Target: all checks complete in <30 seconds total.

use std::path::Path;

use crate::{E2eError, E2eResult};

/// Outcome of a single smoke check.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SmokeCheckResult {
    /// Machine-readable name of the check.
    pub name: String,
    /// Whether the check passed.
    pub passed: bool,
    /// Elapsed wall-clock time in milliseconds.
    pub elapsed_ms: u64,
    /// Human-readable detail (success message or error description).
    pub detail: String,
}

/// Aggregate outcome of the full smoke test suite.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SmokeTestReport {
    /// Individual check results in execution order.
    pub checks: Vec<SmokeCheckResult>,
    /// Total wall-clock time for all checks in milliseconds.
    pub total_elapsed_ms: u64,
    /// Whether every check passed.
    pub all_passed: bool,
}

/// Run a single check, capturing timing and converting errors to a result.
fn run_check(name: &str, f: impl FnOnce() -> E2eResult<String>) -> SmokeCheckResult {
    let start = std::time::Instant::now();
    let outcome = f();
    let elapsed = start.elapsed();
    let elapsed_ms = u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX);

    match outcome {
        Ok(detail) => SmokeCheckResult {
            name: name.to_owned(),
            passed: true,
            elapsed_ms,
            detail,
        },
        Err(e) => SmokeCheckResult {
            name: name.to_owned(),
            passed: false,
            elapsed_ms,
            detail: format!("{e}"),
        },
    }
}

/// Execute the full smoke test suite.
///
/// Each check is independent and runs sequentially.  Returns a
/// [`SmokeTestReport`] summarizing all results.
#[must_use]
pub fn run_smoke_tests() -> SmokeTestReport {
    let overall_start = std::time::Instant::now();

    let checks = vec![
        run_check("golden_copy_integrity", check_golden_copies),
        run_check("frankensqlite_backend", check_frankensqlite),
        run_check("csqlite_backend", check_csqlite),
        run_check("workload_determinism", check_workload_determinism),
        run_check("corruption_injector", check_corruption_injector),
        run_check("canonicalization_pipeline", check_canonicalization),
        run_check("logging_pipeline", check_logging),
    ];

    let total_elapsed_ms = u64::try_from(overall_start.elapsed().as_millis()).unwrap_or(u64::MAX);
    let all_passed = checks.iter().all(|c| c.passed);

    SmokeTestReport {
        checks,
        total_elapsed_ms,
        all_passed,
    }
}

// ── Check 1: Golden Copy Integrity ──────────────────────────────────────

fn check_golden_copies() -> E2eResult<String> {
    let golden_dir = Path::new(crate::golden::GOLDEN_DIR_RELATIVE);

    if !golden_dir.exists() {
        // No golden directory is acceptable in CI / fresh checkouts.
        return Ok(
            "golden directory not found — skipped (acceptable for fresh checkouts)".to_owned(),
        );
    }

    let files = crate::golden::discover_compatibility_baseline_golden_files(golden_dir)?;
    if files.is_empty() {
        return Ok(
            "golden directory exists but contains no clean compatibility baselines — skipped"
                .to_owned(),
        );
    }

    let reports = crate::golden::validate_compatibility_baseline_golden(golden_dir)?;
    let failed: Vec<_> = reports.iter().filter(|r| !r.integrity_ok).collect();

    if failed.is_empty() {
        Ok(format!(
            "{} compatibility-baseline golden database(s) verified, all integrity checks passed",
            reports.len()
        ))
    } else {
        let names: Vec<_> = failed.iter().map(|r| r.name.as_str()).collect();
        Err(E2eError::Divergence(format!(
            "{} of {} golden databases failed integrity check: {}",
            failed.len(),
            reports.len(),
            names.join(", ")
        )))
    }
}

// ── Check 2: FrankenSQLite Backend ──────────────────────────────────────

fn check_frankensqlite() -> E2eResult<String> {
    let conn = crate::block_on(fsqlite::Connection::open(":memory:"))
        .map_err(|e| E2eError::Fsqlite(format!("open: {e}")))?;

    crate::block_on(conn.execute("CREATE TABLE smoke_test (id INTEGER PRIMARY KEY, val TEXT);"))
        .map_err(|e| E2eError::Fsqlite(format!("create: {e}")))?;

    crate::block_on(conn.execute("INSERT INTO smoke_test VALUES (1, 'hello');"))
        .map_err(|e| E2eError::Fsqlite(format!("insert: {e}")))?;

    let rows = crate::block_on(conn.query("SELECT id, val FROM smoke_test;"))
        .map_err(|e| E2eError::Fsqlite(format!("select: {e}")))?;

    if rows.is_empty() {
        return Err(E2eError::Fsqlite(
            "SELECT returned 0 rows, expected 1".to_owned(),
        ));
    }

    Ok("FrankenSQLite: CREATE/INSERT/SELECT verified (1 row)".to_owned())
}

// ── Check 3: C SQLite / rusqlite Backend ────────────────────────────────

fn check_csqlite() -> E2eResult<String> {
    let conn = rusqlite::Connection::open_in_memory()?;

    conn.execute(
        "CREATE TABLE smoke_test (id INTEGER PRIMARY KEY, val TEXT)",
        [],
    )?;
    conn.execute("INSERT INTO smoke_test VALUES (1, 'hello')", [])?;

    let count: i64 = conn.query_row("SELECT count(*) FROM smoke_test", [], |r| r.get(0))?;

    if count != 1 {
        return Err(E2eError::Divergence(format!("expected 1 row, got {count}")));
    }

    Ok("C SQLite (rusqlite): CREATE/INSERT/SELECT verified (1 row)".to_owned())
}

// ── Check 4: Workload Generator Determinism ─────────────────────────────

fn check_workload_determinism() -> E2eResult<String> {
    use crate::workload::{WorkloadConfig, WorkloadGenerator};

    let config = WorkloadConfig {
        fixture_id: "smoke_test".to_owned(),
        seed: 42,
        num_operations: 100,
        ..WorkloadConfig::default()
    };

    let run1 = WorkloadGenerator::new(config.clone()).generate();
    let run2 = WorkloadGenerator::new(config).generate();

    // OpLog doesn't derive PartialEq, so compare via JSON serialization.
    let json1 = serde_json::to_string(&run1)
        .map_err(|e| E2eError::Io(std::io::Error::other(format!("serialize run1: {e}"))))?;
    let json2 = serde_json::to_string(&run2)
        .map_err(|e| E2eError::Io(std::io::Error::other(format!("serialize run2: {e}"))))?;

    if json1 != json2 {
        return Err(E2eError::Divergence(
            "workload generator not deterministic: same seed produced different OpLogs".to_owned(),
        ));
    }

    Ok(format!(
        "workload determinism verified: seed=42, {} records identical across 2 runs",
        run1.records.len()
    ))
}

// ── Check 5: Corruption Injector + Detection ────────────────────────────

fn check_corruption_injector() -> E2eResult<String> {
    use crate::corruption::{CorruptionInjector, CorruptionPattern};

    let tmp = tempfile::TempDir::new()?;
    let db_path = tmp.path().join("smoke.db");

    // Create a small test database via rusqlite.
    let conn = rusqlite::Connection::open(&db_path)?;
    conn.execute_batch(
        "CREATE TABLE items (id INTEGER PRIMARY KEY, data TEXT);
         INSERT INTO items VALUES (1, 'test data for smoke check');
         PRAGMA wal_checkpoint(TRUNCATE);",
    )?;
    drop(conn);

    // Hash before corruption.
    let hash_before = crate::golden::GoldenCopy::hash_file(&db_path)?;

    // Inject a bit-flip.
    let injector = CorruptionInjector::new(db_path.clone())?;
    let report = injector.inject(&CorruptionPattern::BitFlip {
        byte_offset: 200,
        bit_position: 3,
    })?;

    // Hash after corruption.
    let hash_after = crate::golden::GoldenCopy::hash_file(&db_path)?;

    if hash_before == hash_after {
        return Err(E2eError::Divergence(
            "corruption injector had no effect: hashes identical before and after".to_owned(),
        ));
    }

    Ok(format!(
        "corruption injector verified: BitFlip at byte 200, {} byte(s) affected, hash changed",
        report.affected_bytes
    ))
}

// ── Check 6: Canonicalization Pipeline ──────────────────────────────────

fn check_canonicalization() -> E2eResult<String> {
    let tmp = tempfile::TempDir::new()?;
    let source = tmp.path().join("source.db");

    // Create a test database.
    let conn = rusqlite::Connection::open(&source)?;
    conn.execute_batch(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT);
         INSERT INTO t VALUES (1, 'alpha');
         INSERT INTO t VALUES (2, 'beta');
         PRAGMA wal_checkpoint(TRUNCATE);",
    )?;
    drop(conn);

    // Canonicalize twice and verify stable hash.
    let hash1 = crate::canonicalize::canonical_sha256(&source)?;
    let hash2 = crate::canonicalize::canonical_sha256(&source)?;

    if hash1 != hash2 {
        return Err(E2eError::Divergence(format!(
            "canonicalization not stable: hash1={}, hash2={}",
            &hash1[..16],
            &hash2[..16],
        )));
    }

    Ok(format!(
        "canonicalization pipeline verified: stable SHA-256 = {}…",
        &hash1[..16]
    ))
}

// ── Check 7: Logging Pipeline ───────────────────────────────────────────

fn check_logging() -> E2eResult<String> {
    use tracing_subscriber::layer::SubscriberExt;

    let tmp = tempfile::TempDir::new()?;
    let log_path = tmp.path().join("smoke.log.jsonl");

    // Create a scoped subscriber writing JSON to a file.
    let file = std::fs::File::create(&log_path)?;
    let writer = std::sync::Arc::new(std::sync::Mutex::new(file));

    let json_layer = tracing_subscriber::fmt::layer()
        .json()
        .with_writer(SyncWriter(writer))
        .with_target(true);

    let subscriber = tracing_subscriber::registry().with(json_layer);

    tracing::subscriber::with_default(subscriber, || {
        tracing::info!(
            check = "smoke",
            backend = "test",
            "logging pipeline smoke check"
        );
    });

    // Verify the output is valid JSON with expected fields.
    let content = std::fs::read_to_string(&log_path)?;
    if content.trim().is_empty() {
        return Err(E2eError::Io(std::io::Error::other(
            "log file is empty after writing a test event",
        )));
    }

    for line in content.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let parsed: serde_json::Value = serde_json::from_str(line).map_err(|e| {
            E2eError::Io(std::io::Error::other(format!("invalid JSON in log: {e}")))
        })?;
        if parsed.get("level").is_none() {
            return Err(E2eError::Io(std::io::Error::other(
                "log event missing 'level' field",
            )));
        }
    }

    Ok("logging pipeline verified: JSON-lines output with structured fields".to_owned())
}

/// A `MakeWriter` backed by a shared `Arc<Mutex<File>>`.
#[derive(Clone)]
struct SyncWriter(std::sync::Arc<std::sync::Mutex<std::fs::File>>);

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for SyncWriter {
    type Writer = SyncWriterGuard<'a>;

    fn make_writer(&'a self) -> Self::Writer {
        SyncWriterGuard {
            guard: self.0.lock().expect("log mutex poisoned"),
        }
    }
}

struct SyncWriterGuard<'a> {
    guard: std::sync::MutexGuard<'a, std::fs::File>,
}

impl std::io::Write for SyncWriterGuard<'_> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        std::io::Write::write(&mut *self.guard, buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        std::io::Write::flush(&mut *self.guard)
    }
}

// ── Convenience: Print Report ───────────────────────────────────────────

/// Format a [`SmokeTestReport`] as a human-readable summary string.
#[must_use]
pub fn format_smoke_report(report: &SmokeTestReport) -> String {
    use std::fmt::Write;

    let mut out = String::new();
    let _ = writeln!(out, "=== Smoke Test Results ===\n");

    for check in &report.checks {
        let status = if check.passed { "PASS" } else { "FAIL" };
        let _ = writeln!(
            out,
            "  [{status}] {name} ({elapsed}ms)",
            name = check.name,
            elapsed = check.elapsed_ms,
        );
        let _ = writeln!(out, "         {}", check.detail);
    }

    let _ = writeln!(out);
    let verdict = if report.all_passed {
        "ALL CHECKS PASSED"
    } else {
        "SOME CHECKS FAILED"
    };
    let _ = writeln!(
        out,
        "{verdict} — {passed}/{total} in {elapsed}ms",
        passed = report.checks.iter().filter(|c| c.passed).count(),
        total = report.checks.len(),
        elapsed = report.total_elapsed_ms,
    );

    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn smoke_csqlite_backend_passes() {
        let result = run_check("csqlite_backend", check_csqlite);
        assert!(result.passed, "csqlite check failed: {}", result.detail);
    }

    #[test]
    fn smoke_frankensqlite_backend_passes() {
        let result = run_check("frankensqlite_backend", check_frankensqlite);
        assert!(
            result.passed,
            "frankensqlite check failed: {}",
            result.detail
        );
    }

    #[test]
    fn smoke_workload_determinism_passes() {
        let result = run_check("workload_determinism", check_workload_determinism);
        assert!(
            result.passed,
            "workload determinism check failed: {}",
            result.detail
        );
    }

    #[test]
    fn smoke_corruption_injector_passes() {
        let result = run_check("corruption_injector", check_corruption_injector);
        assert!(
            result.passed,
            "corruption injector check failed: {}",
            result.detail
        );
    }

    #[test]
    fn smoke_canonicalization_passes() {
        let result = run_check("canonicalization_pipeline", check_canonicalization);
        assert!(
            result.passed,
            "canonicalization check failed: {}",
            result.detail
        );
    }

    #[test]
    fn smoke_logging_pipeline_passes() {
        let result = run_check("logging_pipeline", check_logging);
        assert!(result.passed, "logging check failed: {}", result.detail);
    }

    #[test]
    fn smoke_full_suite_report_structure() {
        let report = run_smoke_tests();
        assert_eq!(report.checks.len(), 7, "expected 7 smoke checks");
        assert!(
            report.total_elapsed_ms < 30_000,
            "smoke tests should complete in <30 seconds, took {}ms",
            report.total_elapsed_ms
        );

        // Verify the report formatter produces output.
        let formatted = format_smoke_report(&report);
        assert!(formatted.contains("Smoke Test Results"));
        assert!(formatted.contains("csqlite_backend"));
        assert!(formatted.contains("frankensqlite_backend"));
    }

    #[test]
    fn smoke_check_result_serializes() {
        let result = SmokeCheckResult {
            name: "test".to_owned(),
            passed: true,
            elapsed_ms: 42,
            detail: "ok".to_owned(),
        };
        let json = serde_json::to_string(&result).unwrap();
        assert!(json.contains("\"name\":\"test\""));

        let report = SmokeTestReport {
            checks: vec![result],
            total_elapsed_ms: 42,
            all_passed: true,
        };
        let json = serde_json::to_string(&report).unwrap();
        assert!(json.contains("\"all_passed\":true"));
    }
}
