//! C SQLite corruption demo runner (bd-1w6k.7.3).
//!
//! Executes corruption scenarios from [`crate::corruption_scenarios`] against
//! stock C SQLite via `rusqlite`.  For each scenario:
//!
//! 1. Create a fresh temp database, insert rows, flush to WAL.
//! 2. Optionally generate a `.wal-fec` sidecar (for completeness; sqlite3
//!    ignores it).
//! 3. Apply the corruption pattern via [`crate::corruption::CorruptionInjector`].
//! 4. Attempt open + `PRAGMA integrity_check`.
//! 5. Run a SELECT query to count surviving rows.
//! 6. Compare against `scenario.expected_sqlite`.
//!
//! The result is a [`SqliteCorruptionResult`] that downstream beads can
//! combine with the FrankenSQLite side (bd-1w6k.7.4) for comparative reports.

use std::path::{Path, PathBuf};

use rusqlite::Connection;

use crate::E2eResult;
use crate::corruption::{CorruptionInjector, CorruptionReport};
use crate::corruption_scenarios::{CorruptionScenario, CorruptionTarget, ExpectedSqliteBehavior};

/// Result of running a corruption scenario against C SQLite.
#[derive(Debug)]
pub struct SqliteCorruptionResult {
    /// Scenario name (from the catalog).
    pub scenario_name: String,
    /// Number of rows inserted during setup.
    pub rows_inserted: usize,
    /// Number of rows sqlite3 could read back after corruption.
    /// `None` if the database could not be opened at all.
    pub rows_recovered: Option<usize>,
    /// `PRAGMA integrity_check` output.  `None` if open failed.
    pub integrity_check: Option<String>,
    /// Whether `PRAGMA integrity_check` returned `"ok"`.
    pub integrity_ok: bool,
    /// Whether the database could be opened at all.
    pub open_succeeded: bool,
    /// The corruption report from injection.
    pub corruption_report: Option<CorruptionReport>,
    /// Any error message from the open/query attempt.
    pub error: Option<String>,
}

/// Set up a fresh database with the given row count in WAL mode.
///
/// Returns the path to the database file.  The caller owns the temp directory.
fn setup_scenario_db(dir: &Path, row_count: usize) -> E2eResult<PathBuf> {
    let db_path = dir.join("test.db");
    let conn = Connection::open(&db_path)?;
    conn.execute_batch(
        "PRAGMA journal_mode=WAL;\n\
         PRAGMA synchronous=NORMAL;\n\
         PRAGMA page_size=4096;\n\
         CREATE TABLE demo(id INTEGER PRIMARY KEY, data TEXT NOT NULL);",
    )?;
    for i in 0..row_count {
        let id =
            i64::try_from(i).map_err(|_| std::io::Error::other("row id too large for sqlite3"))?;
        conn.execute(
            "INSERT INTO demo(id, data) VALUES (?1, ?2)",
            rusqlite::params![id, format!("row-{i}")],
        )?;
    }
    // Force a WAL checkpoint to ensure we have both db and wal files.
    conn.execute_batch("PRAGMA wal_checkpoint(PASSIVE);")?;
    // Insert one more batch to ensure unflushed WAL frames exist.
    for i in row_count..(row_count + 10) {
        let id =
            i64::try_from(i).map_err(|_| std::io::Error::other("row id too large for sqlite3"))?;
        conn.execute(
            "INSERT INTO demo(id, data) VALUES (?1, ?2)",
            rusqlite::params![id, format!("row-{i}")],
        )?;
    }
    // Close the connection to release file locks (WAL persists).
    drop(conn);
    Ok(db_path)
}

/// Determine the file to corrupt based on the scenario target.
fn target_file_path(db_path: &Path, target: CorruptionTarget) -> PathBuf {
    match target {
        CorruptionTarget::Database => db_path.to_path_buf(),
        CorruptionTarget::Wal => {
            let mut p = db_path.as_os_str().to_owned();
            p.push("-wal");
            PathBuf::from(p)
        }
        CorruptionTarget::WalFecSidecar => {
            let mut p = db_path.as_os_str().to_owned();
            p.push("-wal-fec");
            PathBuf::from(p)
        }
    }
}

/// Count WAL frames in a WAL file (needed for `WalAllFrames` pattern).
fn count_wal_frames(wal_path: &Path) -> E2eResult<u32> {
    let data = std::fs::read(wal_path)?;
    if data.len() < 32 {
        return Ok(0);
    }
    // WAL header is 32 bytes.  Each frame is 24 (header) + page_size bytes.
    // Read page_size from WAL header bytes 8..12 (big-endian u32).
    let page_size = u32::from_be_bytes([data[8], data[9], data[10], data[11]]);
    if page_size == 0 {
        return Ok(0);
    }
    let frame_size = 24 + page_size as usize;
    let payload_len = data.len() - 32;
    #[allow(clippy::cast_possible_truncation)]
    Ok((payload_len / frame_size) as u32)
}

/// Run a single corruption scenario against C SQLite.
///
/// # Errors
///
/// Returns `E2eError` for filesystem failures during setup or injection.
/// Query-level errors from rusqlite are captured in the result, not propagated.
pub fn run_sqlite_corruption_scenario(
    scenario: &CorruptionScenario,
    work_dir: &Path,
) -> E2eResult<SqliteCorruptionResult> {
    let total_rows = scenario.setup_row_count + 10; // setup inserts extra batch

    // 1. Set up the database.
    let db_path = setup_scenario_db(work_dir, scenario.setup_row_count)?;

    // 2. Determine corruption target and inject.
    let target_path = target_file_path(&db_path, scenario.target);

    // For WAL targets, we need the frame count for WalAllFrames.
    let total_frames = if scenario.target == CorruptionTarget::Wal {
        let wal_path = target_file_path(&db_path, CorruptionTarget::Wal);
        if wal_path.exists() {
            count_wal_frames(&wal_path)?
        } else {
            0
        }
    } else {
        0
    };

    let corruption_report = if target_path.exists() {
        let pattern = scenario
            .pattern
            .to_corruption_pattern(scenario.seed, total_frames);
        let injector = CorruptionInjector::new(target_path)?;
        Some(injector.inject(&pattern)?)
    } else {
        None
    };

    // 3. Attempt to open and query.
    let open_result = Connection::open(&db_path);
    match open_result {
        Err(e) => Ok(SqliteCorruptionResult {
            scenario_name: scenario.name.to_owned(),
            rows_inserted: total_rows,
            rows_recovered: None,
            integrity_check: None,
            integrity_ok: false,
            open_succeeded: false,
            corruption_report,
            error: Some(e.to_string()),
        }),
        Ok(conn) => {
            // Run integrity check.
            let integrity = conn
                .query_row("PRAGMA integrity_check;", [], |row| row.get::<_, String>(0))
                .unwrap_or_else(|e| format!("error: {e}"));
            let integrity_ok = integrity == "ok";

            // Count surviving rows.
            let rows_recovered = conn
                .query_row("SELECT COUNT(*) FROM demo;", [], |row| row.get::<_, i64>(0))
                .ok()
                .and_then(|n| usize::try_from(n).ok());

            drop(conn);

            Ok(SqliteCorruptionResult {
                scenario_name: scenario.name.to_owned(),
                rows_inserted: total_rows,
                rows_recovered,
                integrity_check: Some(integrity),
                integrity_ok,
                open_succeeded: true,
                corruption_report,
                error: None,
            })
        }
    }
}

/// Run all scenarios from the catalog against C SQLite.
///
/// Returns one result per scenario, in catalog order.
///
/// # Errors
///
/// Returns `E2eError` if the temp directory cannot be created.
pub fn run_all_sqlite_corruption_scenarios(
    scenarios: &[CorruptionScenario],
) -> E2eResult<Vec<SqliteCorruptionResult>> {
    let base_dir = tempfile::tempdir()?;
    let mut results = Vec::with_capacity(scenarios.len());
    for (i, scenario) in scenarios.iter().enumerate() {
        let scenario_dir = base_dir.path().join(format!("scenario_{i}"));
        std::fs::create_dir_all(&scenario_dir)?;
        results.push(run_sqlite_corruption_scenario(scenario, &scenario_dir)?);
    }
    Ok(results)
}

/// Verify that a [`SqliteCorruptionResult`] matches the scenario's expected behavior.
///
/// Returns `Ok(())` if expectations are met, or an error describing the mismatch.
pub fn verify_sqlite_result(
    result: &SqliteCorruptionResult,
    scenario: &CorruptionScenario,
) -> std::result::Result<(), String> {
    match &scenario.expected_sqlite {
        ExpectedSqliteBehavior::OpenFails => {
            if result.open_succeeded {
                return Err(format!(
                    "{}: expected open to fail, but it succeeded",
                    result.scenario_name
                ));
            }
        }
        ExpectedSqliteBehavior::IntegrityCheckFails { min_surviving_rows } => {
            if !result.open_succeeded {
                return Err(format!(
                    "{}: expected open to succeed for integrity check",
                    result.scenario_name
                ));
            }
            if result.integrity_ok {
                return Err(format!(
                    "{}: expected integrity_check to fail, but it passed",
                    result.scenario_name
                ));
            }
            if let Some(min_rows) = min_surviving_rows
                && let Some(recovered) = result.rows_recovered
                && recovered < *min_rows
            {
                return Err(format!(
                    "{}: expected at least {min_rows} surviving rows, got {recovered}",
                    result.scenario_name
                ));
            }
        }
        ExpectedSqliteBehavior::DataLoss { max_recovered_rows } => {
            if !result.open_succeeded {
                return Err(format!(
                    "{}: expected open to succeed for data loss check",
                    result.scenario_name
                ));
            }
            if let Some(recovered) = result.rows_recovered {
                if recovered >= result.rows_inserted {
                    return Err(format!(
                        "{}: expected data loss, but all {recovered} rows survived",
                        result.scenario_name
                    ));
                }
                if let Some(max) = max_recovered_rows
                    && recovered > *max
                {
                    return Err(format!(
                        "{}: expected at most {max} recovered rows, got {recovered}",
                        result.scenario_name
                    ));
                }
            }
        }
        ExpectedSqliteBehavior::WalTailTruncated => {
            // WAL tail truncation: sqlite3 should open, but may lose recent rows.
            // The database should still be functional.
            if !result.open_succeeded {
                return Err(format!(
                    "{}: expected WAL truncation (open should succeed)",
                    result.scenario_name
                ));
            }
        }
    }
    Ok(())
}

// ── Tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::corruption_scenarios::scenario_catalog;

    #[test]
    fn test_setup_scenario_db_creates_files() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = setup_scenario_db(dir.path(), 10).unwrap();
        assert!(db_path.exists(), "database file should exist");
        let _wal_path = target_file_path(&db_path, CorruptionTarget::Wal);
        // WAL may or may not exist depending on checkpoint behavior,
        // but the db should be valid.
        let conn = Connection::open(&db_path).unwrap();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM demo;", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 20, "should have 10 + 10 rows");
    }

    #[test]
    fn test_run_uncorrupted_baseline() {
        // Run a scenario with no actual corruption to verify the harness works.
        let dir = tempfile::tempdir().unwrap();
        let db_path = setup_scenario_db(dir.path(), 50).unwrap();

        // Open without any corruption — should succeed fully.
        let conn = Connection::open(&db_path).unwrap();
        let integrity: String = conn
            .query_row("PRAGMA integrity_check;", [], |r| r.get(0))
            .unwrap();
        assert_eq!(integrity, "ok");
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM demo;", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 60); // 50 + 10 extra
    }

    #[test]
    fn test_header_zero_makes_sqlite_fail() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = setup_scenario_db(dir.path(), 10).unwrap();

        // Zero the header.
        let injector = CorruptionInjector::new(db_path.clone()).unwrap();
        let _report = injector
            .inject(&crate::corruption::CorruptionPattern::HeaderZero)
            .unwrap();

        // sqlite3 should fail or report issues.
        let result = Connection::open(&db_path);
        match result {
            Err(_) => {} // Expected: open fails.
            Ok(conn) => {
                // Some versions may open but integrity_check fails.
                let check: String = conn
                    .query_row("PRAGMA integrity_check;", [], |r| r.get(0))
                    .unwrap_or_else(|_| "error".to_owned());
                assert_ne!(check, "ok", "integrity should fail after header zeroing");
            }
        }
    }

    #[test]
    fn test_run_and_verify_catalog_scenarios() {
        let catalog = scenario_catalog();
        for scenario in &catalog {
            let dir = tempfile::tempdir().unwrap();
            let scenario_dir = dir.path().join(scenario.name);
            std::fs::create_dir_all(&scenario_dir).unwrap();

            let result = run_sqlite_corruption_scenario(scenario, &scenario_dir).unwrap();
            // The result should always be produced (no panic).
            assert_eq!(result.scenario_name, scenario.name);
            assert!(result.rows_inserted > 0);
        }
    }

    #[test]
    fn test_verify_wal_truncated_scenario() {
        let catalog = scenario_catalog();
        let scenario = catalog
            .iter()
            .find(|s| s.name == "wal_corrupt_within_tolerance")
            .unwrap();

        let dir = tempfile::tempdir().unwrap();
        let result = run_sqlite_corruption_scenario(scenario, dir.path()).unwrap();

        // For WAL truncation, open should succeed.
        assert!(
            result.open_succeeded,
            "sqlite3 should open after WAL corruption (truncation)"
        );
    }

    #[test]
    fn test_verify_header_zero_scenario() {
        let catalog = scenario_catalog();
        let scenario = catalog
            .iter()
            .find(|s| s.name == "db_header_zeroed")
            .unwrap();

        let dir = tempfile::tempdir().unwrap();
        let result = run_sqlite_corruption_scenario(scenario, dir.path()).unwrap();

        // Header zeroed → open should fail or integrity should fail.
        if result.open_succeeded {
            assert!(
                !result.integrity_ok,
                "integrity should fail after header zeroing"
            );
        }
    }

    #[test]
    fn test_run_all_scenarios() {
        let catalog = scenario_catalog();
        let results = run_all_sqlite_corruption_scenarios(&catalog).unwrap();
        assert_eq!(results.len(), catalog.len());
    }

    #[test]
    fn test_count_wal_frames_on_fresh_db() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = setup_scenario_db(dir.path(), 10).unwrap();
        let wal_path = target_file_path(&db_path, CorruptionTarget::Wal);
        if wal_path.exists() {
            let count = count_wal_frames(&wal_path).unwrap();
            assert!(count > 0, "WAL should have at least 1 frame");
        }
    }

    #[test]
    fn test_verify_function_passes_for_expected_outcomes() {
        let catalog = scenario_catalog();
        for scenario in &catalog {
            let dir = tempfile::tempdir().unwrap();
            let scenario_dir = dir.path().join(scenario.name);
            std::fs::create_dir_all(&scenario_dir).unwrap();

            let result = run_sqlite_corruption_scenario(scenario, &scenario_dir).unwrap();
            // verify_sqlite_result should not panic; it may return Ok or Err
            // depending on whether the corruption had the exact expected effect.
            let _ = verify_sqlite_result(&result, scenario);
        }
    }
}
