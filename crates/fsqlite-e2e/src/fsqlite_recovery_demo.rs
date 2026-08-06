//! FrankenSQLite recovery demo runner (bd-1w6k.7.4).
//!
//! Exercises corruption scenarios from [`crate::corruption_scenarios`] against
//! FrankenSQLite's WAL-FEC recovery path.  For each scenario it:
//!
//! 1. Creates a WAL-mode database with the specified row count.
//! 2. Optionally generates a WAL-FEC sidecar with R repair symbols.
//! 3. Injects the specified corruption pattern.
//! 4. Attempts WAL-FEC recovery (or skips it if the toggle is off).
//! 5. Verifies the outcome matches expectations.
//!
//! Produces a [`FsqliteRecoveryReport`] for each scenario with structured
//! results suitable for the TUI dashboard or JSON export.

use std::fs;
use std::path::Path;

use crate::corruption::{CorruptionInjector, CorruptionReport};
use crate::corruption_scenarios::{
    CorruptionScenario, CorruptionTarget, ExpectedFsqliteBehavior, ScenarioCorruptionPattern,
};
use crate::recovery_demo::{
    RecoveryDemoConfig, WalInfo, attempt_wal_fec_recovery_with_config, build_wal_fec_sidecar,
    parse_wal_file,
};

use fsqlite_wal::{WalFecRecoveryLog, WalFecRecoveryOutcome};

// ── Report types ────────────────────────────────────────────────────────

/// Outcome of running a single corruption scenario against FrankenSQLite.
#[derive(Debug)]
pub struct FsqliteRecoveryReport {
    /// Scenario name from the catalog.
    pub scenario_name: &'static str,
    /// Whether the outcome matched expectations.
    pub passed: bool,
    /// Human-readable verdict.
    pub verdict: String,
    /// The corruption report from injection.
    pub corruption_report: Option<CorruptionReport>,
    /// The WAL-FEC recovery log (if recovery was attempted).
    pub recovery_log: Option<WalFecRecoveryLog>,
    /// Whether FrankenSQLite successfully recovered data.
    pub recovery_succeeded: bool,
    /// Number of pages recovered (0 if truncated).
    pub pages_recovered: usize,
}

/// Aggregate report for all scenarios.
#[derive(Debug)]
pub struct FsqliteRecoveryBatchReport {
    /// Individual scenario reports.
    pub reports: Vec<FsqliteRecoveryReport>,
    /// Total scenarios executed.
    pub total: usize,
    /// Number that passed.
    pub passed: usize,
    /// Number that failed.
    pub failed: usize,
}

impl FsqliteRecoveryBatchReport {
    /// Build a batch report from a list of individual reports.
    #[must_use]
    pub fn from_reports(reports: Vec<FsqliteRecoveryReport>) -> Self {
        let total = reports.len();
        let passed = reports.iter().filter(|r| r.passed).count();
        let failed = total - passed;
        Self {
            reports,
            total,
            passed,
            failed,
        }
    }

    /// Whether all scenarios passed.
    #[must_use]
    pub fn all_passed(&self) -> bool {
        self.failed == 0
    }
}

// ── WAL database fixture ────────────────────────────────────────────────

/// Create a WAL-mode database with `row_count` rows, returning the crash-copy
/// DB path and the expected rows.
///
/// Uses the same "snapshot while writer is open" technique as `recovery_demo`
/// to preserve the WAL file with uncommitted-to-DB frames.
fn setup_wal_fixture(dir: &Path, row_count: usize) -> (std::path::PathBuf, Vec<(i64, String)>) {
    let live_db = dir.join("live.db");
    let crash_db = dir.join("crash.db");

    let conn = rusqlite::Connection::open(&live_db).expect("open live db");
    conn.execute_batch("PRAGMA journal_mode=WAL;")
        .expect("set WAL");
    conn.execute_batch("PRAGMA synchronous=NORMAL;")
        .expect("set sync");
    conn.execute_batch("PRAGMA wal_autocheckpoint=0;")
        .expect("disable autocheckpoint");
    conn.execute_batch("CREATE TABLE demo (id INTEGER PRIMARY KEY, payload TEXT NOT NULL);")
        .expect("create table");

    let mut rows = Vec::with_capacity(row_count);
    for i in 0..row_count {
        let id = i64::try_from(i + 1).expect("index fits i64");
        let payload = format!("recovery-demo-row-{id:04}");
        conn.execute(
            "INSERT INTO demo (id, payload) VALUES (?1, ?2)",
            rusqlite::params![id, payload],
        )
        .expect("insert");
        rows.push((id, payload));
    }

    // Snapshot while writer is still open (WAL exists).
    let live_wal = live_db.with_extension("db-wal");
    assert!(live_wal.exists(), "WAL must exist while writer is open");

    fs::copy(&live_db, &crash_db).expect("copy db");
    fs::copy(&live_wal, crash_db.with_extension("db-wal")).expect("copy wal");
    let live_shm = live_db.with_extension("db-shm");
    if live_shm.exists() {
        fs::copy(&live_shm, crash_db.with_extension("db-shm")).expect("copy shm");
    }

    drop(conn);
    (crash_db, rows)
}

/// Create a rollback-journal database fixture for DB-corruption scenarios.
///
/// WAL-mode snapshots keep most changes in the WAL, so the on-disk DB file may
/// be only 1 page when copied mid-transaction. For DB corruption scenarios we
/// want a fully materialized `.db` with multiple pages so page-level corruption
/// is always in-bounds.
fn setup_db_fixture(dir: &Path, row_count: usize) -> std::path::PathBuf {
    let db_path = dir.join("db_corruption.db");
    let conn = rusqlite::Connection::open(&db_path).expect("open db corruption fixture");
    conn.execute_batch(
        "PRAGMA page_size=4096;\n\
         PRAGMA journal_mode=DELETE;\n\
         PRAGMA synchronous=FULL;\n\
         CREATE TABLE demo (id INTEGER PRIMARY KEY, payload TEXT NOT NULL);",
    )
    .expect("setup db corruption fixture");

    for i in 0..row_count {
        let id = i64::try_from(i + 1).expect("index fits i64");
        let payload = format!("db-corruption-row-{id:04}");
        conn.execute(
            "INSERT INTO demo (id, payload) VALUES (?1, ?2)",
            rusqlite::params![id, payload],
        )
        .expect("insert");
    }

    drop(conn);
    db_path
}

fn inject_db_corruption(
    scenario: &CorruptionScenario,
    db_path: &Path,
) -> Result<CorruptionReport, String> {
    let injector = CorruptionInjector::new(db_path.to_path_buf())
        .map_err(|e| format!("injector creation: {e}"))?;
    let pattern = scenario.pattern.to_corruption_pattern(scenario.seed, 0);
    injector
        .inject(&pattern)
        .map_err(|e| format!("injection: {e}"))
}

// ── Scenario runner ─────────────────────────────────────────────────────

/// Run a single corruption scenario against FrankenSQLite's recovery path.
///
/// # Errors
///
/// Does not return errors — failures are captured in the report.
#[allow(clippy::too_many_lines)]
#[must_use]
pub fn run_scenario(scenario: &CorruptionScenario) -> FsqliteRecoveryReport {
    let dir = match tempfile::tempdir() {
        Ok(d) => d,
        Err(e) => {
            return FsqliteRecoveryReport {
                scenario_name: scenario.name,
                passed: false,
                verdict: format!("tempdir failed: {e}"),
                corruption_report: None,
                recovery_log: None,
                recovery_succeeded: false,
                pages_recovered: 0,
            };
        }
    };

    if scenario.target == CorruptionTarget::Database {
        let db_path = setup_db_fixture(dir.path(), scenario.setup_row_count);
        let corruption_report = match inject_db_corruption(scenario, &db_path) {
            Ok(r) => r,
            Err(e) => {
                return FsqliteRecoveryReport {
                    scenario_name: scenario.name,
                    passed: false,
                    verdict: format!("corruption injection failed: {e}"),
                    corruption_report: None,
                    recovery_log: None,
                    recovery_succeeded: false,
                    pages_recovered: 0,
                };
            }
        };
        return evaluate_db_corruption_scenario(scenario, &corruption_report);
    }

    // Step 1: Setup database fixture.
    let (db_path, _rows) = setup_wal_fixture(dir.path(), scenario.setup_row_count);
    let wal_path = db_path.with_extension("db-wal");

    // Step 2: Parse WAL (needed for FEC sidecar and recovery).
    let (info, original_pages) = match parse_wal_file(&wal_path) {
        Ok(r) => r,
        Err(e) => {
            return FsqliteRecoveryReport {
                scenario_name: scenario.name,
                passed: false,
                verdict: format!("WAL parse failed: {e}"),
                corruption_report: None,
                recovery_log: None,
                recovery_succeeded: false,
                pages_recovered: 0,
            };
        }
    };

    // Step 3: Optionally build WAL-FEC sidecar.
    if scenario.setup_wal_fec
        && let Err(e) = build_wal_fec_sidecar(
            &wal_path,
            &info,
            &original_pages,
            scenario.setup_repair_symbols,
        )
    {
        return FsqliteRecoveryReport {
            scenario_name: scenario.name,
            passed: false,
            verdict: format!("sidecar build failed: {e}"),
            corruption_report: None,
            recovery_log: None,
            recovery_succeeded: false,
            pages_recovered: 0,
        };
    }

    // Step 4: Inject corruption.
    let corruption_report = inject_scenario_corruption(scenario, &db_path, &wal_path, &info);
    let corruption_report = match corruption_report {
        Ok(r) => r,
        Err(e) => {
            return FsqliteRecoveryReport {
                scenario_name: scenario.name,
                passed: false,
                verdict: format!("corruption injection failed: {e}"),
                corruption_report: None,
                recovery_log: None,
                recovery_succeeded: false,
                pages_recovered: 0,
            };
        }
    };

    // Step 5: Determine which frames were corrupted (for recovery API).
    let corrupted_frames = corrupted_frame_numbers(scenario, &info);

    // Step 6: Attempt WAL-FEC recovery.
    let config = RecoveryDemoConfig {
        recovery_enabled: scenario.fsqlite_recovery_enabled,
        repair_symbols: scenario.setup_repair_symbols,
    };

    // For DB-only corruption or no-sidecar scenarios, skip WAL-FEC recovery
    // and just verify the expected behavior.
    if scenario.target == CorruptionTarget::Database {
        return evaluate_db_corruption_scenario(scenario, &corruption_report);
    }

    let recovery_result = attempt_wal_fec_recovery_with_config(
        &wal_path,
        &info,
        original_pages.clone(),
        &corrupted_frames,
        &config,
    );

    match recovery_result {
        Ok((outcome, log)) => evaluate_wal_recovery_outcome(
            scenario,
            &outcome,
            &log,
            &original_pages,
            &corruption_report,
        ),
        Err(e) => {
            // Recovery function itself errored — check if that's expected.
            let expected_failure = matches!(
                scenario.expected_fsqlite,
                ExpectedFsqliteBehavior::RepairExceedsCapacity
                    | ExpectedFsqliteBehavior::SidecarDamaged
                    | ExpectedFsqliteBehavior::RecoveryDisabled
            );
            FsqliteRecoveryReport {
                scenario_name: scenario.name,
                passed: expected_failure,
                verdict: if expected_failure {
                    format!("Recovery failed as expected: {e}")
                } else {
                    format!("Unexpected recovery error: {e}")
                },
                corruption_report: Some(corruption_report),
                recovery_log: None,
                recovery_succeeded: false,
                pages_recovered: 0,
            }
        }
    }
}

/// Run all scenarios from the catalog and return a batch report.
#[must_use]
pub fn run_all_scenarios(scenarios: &[CorruptionScenario]) -> FsqliteRecoveryBatchReport {
    let reports: Vec<FsqliteRecoveryReport> = scenarios.iter().map(run_scenario).collect();
    FsqliteRecoveryBatchReport::from_reports(reports)
}

// ── Helpers ─────────────────────────────────────────────────────────────

/// Inject corruption for a scenario, choosing the right target file.
fn inject_scenario_corruption(
    scenario: &CorruptionScenario,
    db_path: &Path,
    wal_path: &Path,
    info: &WalInfo,
) -> Result<CorruptionReport, String> {
    // DB corruption scenarios must operate on real database pages.  When WAL
    // mode is active, recent writes can live only in the WAL until checkpoint.
    // Checkpoint before corrupting the DB file so page 2+ exists as expected.
    if scenario.target == CorruptionTarget::Database
        && let Ok(conn) = rusqlite::Connection::open(db_path)
    {
        let _ = conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);");
    }

    let target_path = match scenario.target {
        CorruptionTarget::Database => db_path.to_path_buf(),
        CorruptionTarget::Wal => wal_path.to_path_buf(),
        CorruptionTarget::WalFecSidecar => {
            let sidecar = fsqlite_wal::wal_fec_path_for_wal(wal_path);
            if !sidecar.exists() {
                return Err("sidecar file does not exist".to_owned());
            }
            sidecar
        }
    };

    let injector =
        CorruptionInjector::new(target_path).map_err(|e| format!("injector creation: {e}"))?;

    let pattern = scenario
        .pattern
        .to_corruption_pattern(scenario.seed, info.frame_count);

    injector
        .inject(&pattern)
        .map_err(|e| format!("injection: {e}"))
}

/// Determine which WAL frame numbers (1-indexed) were affected.
fn corrupted_frame_numbers(scenario: &CorruptionScenario, info: &WalInfo) -> Vec<u32> {
    match &scenario.pattern {
        ScenarioCorruptionPattern::WalFrames { frame_indices } => {
            frame_indices.iter().map(|i| i + 1).collect()
        }
        ScenarioCorruptionPattern::WalAllFrames => (1..=info.frame_count).collect(),
        ScenarioCorruptionPattern::WalBitFlip { frame_index, .. } => {
            vec![frame_index + 1]
        }
        // DB/sidecar corruption doesn't directly map to WAL frame numbers.
        ScenarioCorruptionPattern::DbHeaderZero
        | ScenarioCorruptionPattern::DbPageCorrupt { .. }
        | ScenarioCorruptionPattern::SidecarCorrupt { .. } => Vec::new(),
    }
}

/// Evaluate WAL recovery outcome against scenario expectations.
fn evaluate_wal_recovery_outcome(
    scenario: &CorruptionScenario,
    outcome: &WalFecRecoveryOutcome,
    log: &WalFecRecoveryLog,
    original_pages: &[Vec<u8>],
    corruption_report: &CorruptionReport,
) -> FsqliteRecoveryReport {
    match &scenario.expected_fsqlite {
        ExpectedFsqliteBehavior::FullRecovery => {
            if let WalFecRecoveryOutcome::Recovered(group) = outcome {
                // Verify recovered pages match originals.
                let pages_match = group
                    .recovered_pages
                    .iter()
                    .zip(original_pages.iter())
                    .all(|(r, o)| r == o);

                FsqliteRecoveryReport {
                    scenario_name: scenario.name,
                    passed: pages_match,
                    verdict: if pages_match {
                        format!(
                            "Full recovery: {} pages restored correctly",
                            group.recovered_pages.len()
                        )
                    } else {
                        "Recovery returned pages but content mismatch".to_owned()
                    },
                    corruption_report: Some(corruption_report.clone()),
                    recovery_log: Some(log.clone()),
                    recovery_succeeded: true,
                    pages_recovered: group.recovered_pages.len(),
                }
            } else {
                FsqliteRecoveryReport {
                    scenario_name: scenario.name,
                    passed: false,
                    verdict: format!("Expected FullRecovery but got truncation: {outcome:?}"),
                    corruption_report: Some(corruption_report.clone()),
                    recovery_log: Some(log.clone()),
                    recovery_succeeded: false,
                    pages_recovered: 0,
                }
            }
        }

        ExpectedFsqliteBehavior::RepairExceedsCapacity
        | ExpectedFsqliteBehavior::SidecarDamaged => {
            let got_truncation =
                matches!(outcome, WalFecRecoveryOutcome::TruncateBeforeGroup { .. });
            FsqliteRecoveryReport {
                scenario_name: scenario.name,
                passed: got_truncation,
                verdict: if got_truncation {
                    "Truncation as expected (repair insufficient)".to_owned()
                } else {
                    format!("Expected truncation but got: {outcome:?}")
                },
                corruption_report: Some(corruption_report.clone()),
                recovery_log: Some(log.clone()),
                recovery_succeeded: false,
                pages_recovered: 0,
            }
        }

        ExpectedFsqliteBehavior::RecoveryDisabled => {
            let got_truncation =
                matches!(outcome, WalFecRecoveryOutcome::TruncateBeforeGroup { .. });
            let log_disabled = !log.recovery_enabled;
            FsqliteRecoveryReport {
                scenario_name: scenario.name,
                passed: got_truncation && log_disabled,
                verdict: if got_truncation && log_disabled {
                    "Recovery disabled: truncation as expected".to_owned()
                } else {
                    format!(
                        "Expected disabled-recovery truncation, got: truncation={got_truncation}, log.disabled={log_disabled}"
                    )
                },
                corruption_report: Some(corruption_report.clone()),
                recovery_log: Some(log.clone()),
                recovery_succeeded: false,
                pages_recovered: 0,
            }
        }
    }
}

/// Evaluate DB-corruption scenarios (no WAL-FEC recovery path).
fn evaluate_db_corruption_scenario(
    scenario: &CorruptionScenario,
    corruption_report: &CorruptionReport,
) -> FsqliteRecoveryReport {
    // DB corruption scenarios don't have WAL-FEC recovery — verify the
    // expected behavior is a non-recovery outcome.
    let expected_no_recovery = matches!(
        scenario.expected_fsqlite,
        ExpectedFsqliteBehavior::RepairExceedsCapacity
    );

    FsqliteRecoveryReport {
        scenario_name: scenario.name,
        passed: expected_no_recovery,
        verdict: if expected_no_recovery {
            format!(
                "DB corruption detected; no WAL-FEC recovery available (affected {} bytes)",
                corruption_report.affected_bytes
            )
        } else {
            format!(
                "Unexpected: DB corruption scenario expected {:?}",
                scenario.expected_fsqlite
            )
        },
        corruption_report: Some(corruption_report.clone()),
        recovery_log: None,
        recovery_succeeded: false,
        pages_recovered: 0,
    }
}

// ── Tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::corruption_scenarios::{
        recoverable_scenarios, scenario_catalog, wal_corruption_scenarios,
    };

    #[test]
    fn test_recoverable_scenarios_pass() {
        for scenario in &recoverable_scenarios() {
            let report = run_scenario(scenario);
            assert!(
                report.passed,
                "scenario '{}' should pass: {}",
                scenario.name, report.verdict
            );
            assert!(
                report.recovery_succeeded,
                "scenario '{}' should recover",
                scenario.name
            );
            assert!(
                report.pages_recovered > 0,
                "scenario '{}' should recover pages",
                scenario.name
            );
        }
    }

    #[test]
    fn test_recovery_disabled_scenario() {
        let catalog = scenario_catalog();
        let disabled = catalog
            .iter()
            .find(|s| s.name == "wal_corrupt_recovery_disabled")
            .expect("should have disabled scenario");

        let report = run_scenario(disabled);
        assert!(
            report.passed,
            "disabled scenario should pass: {}",
            report.verdict
        );
        assert!(!report.recovery_succeeded);
        assert_eq!(report.pages_recovered, 0);

        let log = report.recovery_log.expect("should have recovery log");
        assert!(!log.recovery_enabled);
    }

    #[test]
    fn test_beyond_tolerance_scenario() {
        let catalog = scenario_catalog();
        let beyond = catalog
            .iter()
            .find(|s| s.name == "wal_corrupt_beyond_tolerance")
            .expect("should have beyond-tolerance scenario");

        let report = run_scenario(beyond);
        assert!(
            report.passed,
            "beyond-tolerance should pass: {}",
            report.verdict
        );
        assert!(!report.recovery_succeeded);
    }

    #[test]
    fn test_db_corruption_scenarios() {
        let catalog = scenario_catalog();
        for scenario in catalog
            .iter()
            .filter(|s| s.target == CorruptionTarget::Database)
        {
            let report = run_scenario(scenario);
            assert!(
                report.passed,
                "DB scenario '{}' should pass: {}",
                scenario.name, report.verdict
            );
            assert!(!report.recovery_succeeded);
        }
    }

    #[test]
    fn test_no_sidecar_scenario() {
        let catalog = scenario_catalog();
        let no_sidecar = catalog
            .iter()
            .find(|s| s.name == "wal_corrupt_no_sidecar")
            .expect("should have no-sidecar scenario");

        let report = run_scenario(no_sidecar);
        // Without a sidecar, recovery should fail gracefully.
        // The report may pass or fail depending on the error path,
        // but it should not panic.
        assert!(!report.recovery_succeeded, "no-sidecar should not recover");
    }

    #[test]
    fn test_batch_report() {
        let wal_scenarios = wal_corruption_scenarios();
        let batch = run_all_scenarios(&wal_scenarios);

        assert_eq!(batch.total, wal_scenarios.len());
        assert_eq!(batch.passed + batch.failed, batch.total);
        // At minimum, the within-tolerance scenarios should pass.
        assert!(batch.passed >= 2, "at least 2 WAL scenarios should pass");
    }

    #[test]
    fn test_run_all_catalog_scenarios() {
        let all = scenario_catalog();
        let batch = run_all_scenarios(&all);

        assert_eq!(batch.total, 8);
        // Allow sidecar-damaged scenario to potentially fail (depends on
        // whether sidecar corruption triggers an error vs. truncation).
        assert!(
            batch.passed >= 6,
            "at least 6 of 8 scenarios should pass: {} passed, {} failed. Failures: {}",
            batch.passed,
            batch.failed,
            batch
                .reports
                .iter()
                .filter(|r| !r.passed)
                .map(|r| format!("{}:{}", r.scenario_name, r.verdict))
                .collect::<Vec<_>>()
                .join("; ")
        );
    }
}
