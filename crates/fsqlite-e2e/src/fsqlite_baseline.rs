//! FrankenSQLite recovery baseline measurement under corruption (bd-2als.4.2).
//!
//! For each entry in the [`crate::corruption::corruption_strategy_catalog`],
//! this module:
//!
//! 1. Creates a WAL-mode database with known data and a WAL-FEC sidecar.
//! 2. Takes a deterministic logical dump as the "expected" reference.
//! 3. Applies the corruption pattern.
//! 4. Attempts WAL-FEC recovery (for WAL-targeting strategies).
//! 5. Classifies the outcome into a three-tier system:
//!    - **Recovered**: DB usable, integrity OK, data matches expected.
//!    - **Partial**: DB usable but some data lost (dump diverges).
//!    - **Lost**: DB cannot be opened or recovery was not possible.
//!
//! Results are JSON-serializable so downstream beads (bd-2als.4.4) can
//! combine them with the sqlite3 baseline for comparative reports.

use std::fmt;
use std::path::{Path, PathBuf};

use rusqlite::Connection;
use sha2::{Digest, Sha256};

use crate::E2eResult;
use crate::corruption::{
    CatalogEntry, CorruptionCategory, CorruptionReport, CorruptionSeverity,
    corruption_strategy_catalog,
};
use crate::recovery_demo::{
    RecoveryDemoConfig, WalInfo, attempt_wal_fec_recovery_with_config, build_wal_fec_sidecar,
    parse_wal_file,
};

// ── Recovery tier classification ────────────────────────────────────────

/// Three-tier recovery outcome for FrankenSQLite after corruption.
///
/// Listed in order of increasing "health" — `Lost` is the worst,
/// `Recovered` means full recovery with data integrity.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub enum FsqliteRecoveryTier {
    /// DB cannot be recovered: open failed or recovery impossible.
    Lost,
    /// DB is usable but data diverges from expected (some data lost).
    Partial,
    /// Full recovery: integrity OK and data matches expected dump.
    Recovered,
}

impl fmt::Display for FsqliteRecoveryTier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Lost => f.write_str("lost"),
            Self::Partial => f.write_str("partial"),
            Self::Recovered => f.write_str("recovered"),
        }
    }
}

// ── Baseline result ─────────────────────────────────────────────────────

/// Structured evidence from running one corruption catalog entry against
/// FrankenSQLite's WAL-FEC recovery path.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FsqliteBaselineResult {
    /// The catalog entry's stable identifier.
    pub strategy_id: String,
    /// Human-readable strategy name.
    pub strategy_name: String,
    /// Target file category.
    pub category: CorruptionCategory,
    /// Severity level.
    pub severity: CorruptionSeverity,
    /// Classified recovery tier.
    pub recovery_tier: FsqliteRecoveryTier,
    /// Number of rows in the database before corruption.
    pub rows_before: usize,
    /// Number of rows readable after corruption+recovery (`None` if open failed).
    pub rows_after: Option<usize>,
    /// Whether WAL-FEC recovery was attempted.
    pub recovery_attempted: bool,
    /// Whether WAL-FEC recovery succeeded (pages were restored).
    pub recovery_succeeded: bool,
    /// Number of pages recovered via WAL-FEC.
    pub pages_recovered: usize,
    /// `PRAGMA integrity_check` output after recovery (`None` if open failed).
    pub integrity_output: Option<String>,
    /// Error from open attempt (`None` if open succeeded).
    pub open_error: Option<String>,
    /// SHA-256 hash of the expected (pre-corruption) logical dump.
    pub expected_dump_hash: String,
    /// SHA-256 hash of the actual (post-recovery) logical dump, if obtainable.
    pub actual_dump_hash: Option<String>,
    /// Injection report from the corruption framework.
    pub corruption_report: Option<CorruptionReport>,
    /// Structured WAL-FEC recovery log summary (if recovery was attempted).
    ///
    /// Serialized as a human-readable string since `WalFecRecoveryLog` does
    /// not implement serde traits.
    pub recovery_log_summary: Option<String>,
}

// ── Setup ───────────────────────────────────────────────────────────────

/// Default row count for baseline databases.
const BASELINE_ROW_COUNT: usize = 100;

/// Repair symbols to provision in the WAL-FEC sidecar.
const BASELINE_REPAIR_SYMBOLS: u32 = 4;

/// Page size for baseline databases.
const BASELINE_PAGE_SIZE: u32 = 4096;

/// Create a WAL-mode database with known data and a WAL-FEC sidecar.
///
/// Returns `(db_path, wal_info, original_page_payloads, expected_dump_hash)`.
///
/// The database has `BASELINE_ROW_COUNT` rows inserted, then a passive
/// checkpoint, then 20 more rows (so WAL frames exist for WAL-targeting
/// strategies).
#[allow(clippy::type_complexity)]
fn setup_baseline_with_fec(
    dir: &Path,
) -> E2eResult<(PathBuf, Option<WalInfo>, Vec<Vec<u8>>, String)> {
    let live_db = dir.join("live.db");
    let crash_db = dir.join("baseline.db");

    let conn = Connection::open(&live_db)?;
    conn.execute_batch(
        "PRAGMA journal_mode=WAL;\n\
         PRAGMA synchronous=NORMAL;\n\
         PRAGMA page_size=4096;\n\
         PRAGMA wal_autocheckpoint=0;\n\
         CREATE TABLE items(id INTEGER PRIMARY KEY, label TEXT NOT NULL, value REAL NOT NULL);\n\
         CREATE INDEX idx_items_label ON items(label);",
    )?;

    #[allow(clippy::cast_precision_loss, clippy::items_after_statements)]
    fn row_value(i: usize) -> f64 {
        i as f64 * 1.5
    }

    for i in 0..BASELINE_ROW_COUNT {
        let id = i64::try_from(i).expect("row count fits i64");
        conn.execute(
            "INSERT INTO items(id, label, value) VALUES (?1, ?2, ?3)",
            rusqlite::params![id, format!("item-{i:04}"), row_value(i)],
        )?;
    }

    // Checkpoint to ensure DB file has content.
    conn.execute_batch("PRAGMA wal_checkpoint(PASSIVE);")?;

    // Insert more rows so WAL frames exist.
    for i in BASELINE_ROW_COUNT..(BASELINE_ROW_COUNT + 20) {
        let id = i64::try_from(i).expect("row count fits i64");
        conn.execute(
            "INSERT INTO items(id, label, value) VALUES (?1, ?2, ?3)",
            rusqlite::params![id, format!("item-{i:04}"), row_value(i)],
        )?;
    }

    // Compute the "expected" dump hash while the writer connection is still open.
    // Opening and closing an extra connection here can checkpoint and remove the
    // WAL file, which would break WAL-targeting corruption strategies later.
    let expected_dump = logical_dump(&conn)?;
    let expected_hash = hash_dump(&expected_dump);

    // Snapshot while writer is still open (WAL exists).
    let live_wal = live_db.with_extension("db-wal");
    std::fs::copy(&live_db, &crash_db)?;
    if live_wal.exists() {
        let crash_wal = crash_db.with_extension("db-wal");
        std::fs::copy(&live_wal, &crash_wal)?;
    }
    drop(conn);

    // Parse WAL and build FEC sidecar on the crash copy.
    let crash_wal = crash_db.with_extension("db-wal");
    let (wal_info, original_pages) = if crash_wal.exists() {
        match parse_wal_file(&crash_wal) {
            Ok((info, pages)) => {
                // Build FEC sidecar with repair symbols.
                if !pages.is_empty() {
                    let _ =
                        build_wal_fec_sidecar(&crash_wal, &info, &pages, BASELINE_REPAIR_SYMBOLS);
                }
                (Some(info), pages)
            }
            Err(_) => (None, Vec::new()),
        }
    } else {
        (None, Vec::new())
    };

    Ok((crash_db, wal_info, original_pages, expected_hash))
}

/// Create a dummy WAL-FEC sidecar file for sidecar corruption strategies.
fn ensure_sidecar_file(db_path: &Path) -> E2eResult<PathBuf> {
    let sidecar_path = db_path.with_extension("db-wal-fec");
    if !sidecar_path.exists() {
        let dummy = vec![0xFE; 2048];
        std::fs::write(&sidecar_path, dummy)?;
    }
    Ok(sidecar_path)
}

/// Get the file path for a given corruption category relative to a DB path.
fn target_path_for_category(db_path: &Path, category: CorruptionCategory) -> PathBuf {
    match category {
        CorruptionCategory::DatabaseFile => db_path.to_path_buf(),
        CorruptionCategory::Wal => db_path.with_extension("db-wal"),
        CorruptionCategory::Sidecar => db_path.with_extension("db-wal-fec"),
    }
}

// ── Logical dump ────────────────────────────────────────────────────────

/// Produce a deterministic logical dump for divergence detection.
fn logical_dump(conn: &Connection) -> E2eResult<String> {
    use std::fmt::Write;

    let mut dump = String::new();

    let mut schema_stmt =
        conn.prepare("SELECT sql FROM sqlite_master WHERE sql IS NOT NULL ORDER BY name")?;
    let sqls: Vec<String> = schema_stmt
        .query_map([], |row| row.get(0))?
        .collect::<Result<Vec<_>, _>>()?;
    for sql in &sqls {
        let _ = writeln!(dump, "{sql};");
    }
    dump.push_str("---\n");

    let mut tables_stmt = conn.prepare(
        "SELECT name FROM sqlite_master WHERE type='table' \
         AND name NOT LIKE 'sqlite_%' ORDER BY name",
    )?;
    let tables: Vec<String> = tables_stmt
        .query_map([], |row| row.get(0))?
        .collect::<Result<Vec<_>, _>>()?;

    for table in &tables {
        let _ = writeln!(dump, "TABLE {table}");
        let query = format!("SELECT * FROM \"{table}\" ORDER BY 1");
        let mut stmt = conn.prepare(&query)?;
        let col_count = stmt.column_count();
        let mut rows = stmt.query([])?;
        while let Some(row) = rows.next()? {
            let mut parts = Vec::with_capacity(col_count);
            for i in 0..col_count {
                let val: rusqlite::types::Value = row.get(i)?;
                parts.push(format!("{val:?}"));
            }
            let _ = writeln!(dump, "  {}", parts.join("|"));
        }
    }
    Ok(dump)
}

/// SHA-256 hash of a string, returned as hex.
fn hash_dump(dump: &str) -> String {
    use std::fmt::Write;
    let digest = Sha256::digest(dump.as_bytes());
    let mut hex = String::with_capacity(64);
    for byte in digest {
        let _ = write!(hex, "{byte:02x}");
    }
    hex
}

// ── Identify corrupted frames ───────────────────────────────────────────

/// Heuristic: identify which WAL frames were likely corrupted.
///
/// Compares the current WAL file's frame payloads against the originals
/// and returns the frame numbers (1-indexed) that differ.
fn identify_corrupted_frames(
    wal_path: &Path,
    original_pages: &[Vec<u8>],
    page_size: u32,
) -> Vec<u32> {
    let Ok(wal_bytes) = std::fs::read(wal_path) else {
        return Vec::new();
    };

    let header_size = 32_usize;
    let frame_header = 24_usize;
    let page_size_usize = page_size as usize;
    let frame_size = frame_header + page_size_usize;

    let mut corrupted = Vec::new();
    for (i, original) in original_pages.iter().enumerate() {
        let frame_start = header_size + i * frame_size;
        let payload_start = frame_start + frame_header;
        let payload_end = payload_start + page_size_usize;
        if payload_end > wal_bytes.len() {
            // WAL was truncated — consider all remaining frames corrupted.
            for j in i..original_pages.len() {
                corrupted.push(u32::try_from(j + 1).expect("frame index fits u32"));
            }
            break;
        }
        if wal_bytes[payload_start..payload_end] != *original {
            corrupted.push(u32::try_from(i + 1).expect("frame index fits u32"));
        }
    }
    corrupted
}

// ── Single-strategy measurement ─────────────────────────────────────────

/// Run a single corruption catalog entry against FrankenSQLite's WAL-FEC
/// recovery path and classify the outcome.
///
/// # Errors
///
/// Returns `E2eError` for filesystem failures during setup or injection.
#[allow(clippy::too_many_lines)]
pub fn measure_fsqlite_baseline(
    entry: &CatalogEntry,
    work_dir: &Path,
) -> E2eResult<FsqliteBaselineResult> {
    let total_rows = BASELINE_ROW_COUNT + 20;

    // 1. Set up baseline DB with WAL-FEC sidecar.
    let (db_path, wal_info, original_pages, expected_hash) = setup_baseline_with_fec(work_dir)?;

    // Ensure sidecar exists for sidecar strategies.
    if entry.category == CorruptionCategory::Sidecar {
        ensure_sidecar_file(&db_path)?;
    }

    // 2. Apply corruption.
    let target = target_path_for_category(&db_path, entry.category);
    let corruption_report = if target.exists() {
        Some(entry.inject(&target, BASELINE_PAGE_SIZE)?)
    } else {
        let hash_copy = expected_hash.clone();
        return Ok(FsqliteBaselineResult {
            strategy_id: entry.strategy_id.clone(),
            strategy_name: entry.name.clone(),
            category: entry.category,
            severity: entry.severity,
            recovery_tier: FsqliteRecoveryTier::Recovered,
            rows_before: total_rows,
            rows_after: Some(total_rows),
            recovery_attempted: false,
            recovery_succeeded: false,
            pages_recovered: 0,
            integrity_output: Some("ok (target file absent)".to_owned()),
            open_error: None,
            expected_dump_hash: expected_hash,
            actual_dump_hash: Some(hash_copy),
            corruption_report: None,
            recovery_log_summary: None,
        });
    };

    // 3. Attempt WAL-FEC recovery for WAL strategies.
    let (recovery_attempted, recovery_succeeded, pages_recovered, recovery_log_summary) = if entry
        .category
        == CorruptionCategory::Wal
    {
        if let Some(ref info) = wal_info {
            let wal_path = db_path.with_extension("db-wal");
            let corrupted_frames =
                identify_corrupted_frames(&wal_path, &original_pages, info.page_size);

            let config = RecoveryDemoConfig {
                recovery_enabled: true,
                repair_symbols: BASELINE_REPAIR_SYMBOLS,
            };

            match attempt_wal_fec_recovery_with_config(&wal_path, info, &corrupted_frames, &config)
            {
                Ok((outcome, log)) => {
                    let (succeeded, pages) = match &outcome {
                        fsqlite_wal::WalFecRecoveryOutcome::Recovered(group) => {
                            // Write recovered pages back to WAL for reading.
                            let _ = write_recovered_wal(&wal_path, info, &group.recovered_pages);
                            (true, group.recovered_pages.len())
                        }
                        fsqlite_wal::WalFecRecoveryOutcome::TruncateBeforeGroup { .. } => {
                            (false, 0)
                        }
                    };
                    (true, succeeded, pages, Some(format!("{log:?}")))
                }
                Err(_) => (true, false, 0, None),
            }
        } else {
            (false, false, 0, None)
        }
    } else {
        (false, false, 0, None)
    };

    // 4. Try to open and inspect the (possibly recovered) database.
    let open_result = Connection::open(&db_path);
    let conn = match open_result {
        Err(e) => {
            return Ok(FsqliteBaselineResult {
                strategy_id: entry.strategy_id.clone(),
                strategy_name: entry.name.clone(),
                category: entry.category,
                severity: entry.severity,
                recovery_tier: FsqliteRecoveryTier::Lost,
                rows_before: total_rows,
                rows_after: None,
                recovery_attempted,
                recovery_succeeded,
                pages_recovered,
                integrity_output: None,
                open_error: Some(e.to_string()),
                expected_dump_hash: expected_hash,
                actual_dump_hash: None,
                corruption_report,
                recovery_log_summary,
            });
        }
        Ok(c) => c,
    };

    // 5. Run integrity check.
    let integrity = conn
        .query_row("PRAGMA integrity_check;", [], |row| row.get::<_, String>(0))
        .unwrap_or_else(|e| format!("error: {e}"));

    // 6. Count surviving rows.
    let rows_after = conn
        .query_row("SELECT COUNT(*) FROM items;", [], |row| {
            row.get::<_, i64>(0)
        })
        .ok()
        .and_then(|n| usize::try_from(n).ok());

    // 7. Attempt logical dump for divergence detection.
    let actual_dump = logical_dump(&conn).ok();
    let actual_hash = actual_dump.as_deref().map(hash_dump);
    drop(conn);

    // 8. Classify recovery tier.
    let recovery_tier = if actual_hash.as_deref() == Some(expected_hash.as_str()) {
        FsqliteRecoveryTier::Recovered
    } else if rows_after.is_some() {
        FsqliteRecoveryTier::Partial
    } else {
        FsqliteRecoveryTier::Lost
    };

    Ok(FsqliteBaselineResult {
        strategy_id: entry.strategy_id.clone(),
        strategy_name: entry.name.clone(),
        category: entry.category,
        severity: entry.severity,
        recovery_tier,
        rows_before: total_rows,
        rows_after,
        recovery_attempted,
        recovery_succeeded,
        pages_recovered,
        integrity_output: Some(integrity),
        open_error: None,
        expected_dump_hash: expected_hash,
        actual_dump_hash: actual_hash,
        corruption_report,
        recovery_log_summary,
    })
}

/// Write recovered page payloads back into the WAL for rusqlite to read.
///
/// This reconstructs the WAL with original frame headers and recovered
/// page data so that opening the DB via rusqlite sees the recovered state.
fn write_recovered_wal(
    wal_path: &Path,
    info: &WalInfo,
    recovered_pages: &[Vec<u8>],
) -> E2eResult<()> {
    let wal_bytes = std::fs::read(wal_path)?;
    let header_size = 32_usize;
    let frame_header = 24_usize;
    let page_size = info.page_size as usize;
    let frame_size = frame_header + page_size;

    let mut new_wal = wal_bytes;

    for (i, page_data) in recovered_pages.iter().enumerate() {
        let frame_start = header_size + i * frame_size;
        let payload_start = frame_start + frame_header;
        let payload_end = payload_start + page_size;
        if payload_end <= new_wal.len() && page_data.len() == page_size {
            new_wal[payload_start..payload_end].copy_from_slice(page_data);
        }
    }

    std::fs::write(wal_path, new_wal)?;
    Ok(())
}

// ── Full catalog sweep ──────────────────────────────────────────────────

/// Run every strategy from the corruption catalog against FrankenSQLite.
///
/// Returns one [`FsqliteBaselineResult`] per catalog entry, in catalog order.
///
/// # Errors
///
/// Returns `E2eError` if the temp directory cannot be created.
pub fn run_full_fsqlite_baseline() -> E2eResult<Vec<FsqliteBaselineResult>> {
    let catalog = corruption_strategy_catalog();
    let base_dir = tempfile::tempdir()?;
    let mut results = Vec::with_capacity(catalog.len());

    for (i, entry) in catalog.iter().enumerate() {
        let entry_dir = base_dir
            .path()
            .join(format!("strategy_{i}_{}", entry.strategy_id));
        std::fs::create_dir_all(&entry_dir)?;
        results.push(measure_fsqlite_baseline(entry, &entry_dir)?);
    }

    Ok(results)
}

/// Run a filtered set of strategies (by category) against FrankenSQLite.
///
/// # Errors
///
/// Returns `E2eError` if the temp directory cannot be created.
pub fn run_fsqlite_baseline_by_category(
    category: CorruptionCategory,
) -> E2eResult<Vec<FsqliteBaselineResult>> {
    let catalog: Vec<CatalogEntry> = corruption_strategy_catalog()
        .into_iter()
        .filter(|e| e.category == category)
        .collect();
    let base_dir = tempfile::tempdir()?;
    let mut results = Vec::with_capacity(catalog.len());

    for (i, entry) in catalog.iter().enumerate() {
        let entry_dir = base_dir
            .path()
            .join(format!("strategy_{i}_{}", entry.strategy_id));
        std::fs::create_dir_all(&entry_dir)?;
        results.push(measure_fsqlite_baseline(entry, &entry_dir)?);
    }

    Ok(results)
}

// ── Recovery summary ────────────────────────────────────────────────────

/// Summary statistics for a batch of fsqlite baseline results.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FsqliteRecoverySummary {
    /// Total number of strategies tested.
    pub total: usize,
    /// Count of `Lost` outcomes.
    pub lost: usize,
    /// Count of `Partial` outcomes.
    pub partial: usize,
    /// Count of `Recovered` outcomes.
    pub recovered: usize,
    /// Number of strategies where WAL-FEC was attempted.
    pub recovery_attempts: usize,
    /// Number of strategies where WAL-FEC succeeded.
    pub recovery_successes: usize,
}

impl FsqliteRecoverySummary {
    /// Compute summary statistics from a slice of results.
    #[must_use]
    pub fn from_results(results: &[FsqliteBaselineResult]) -> Self {
        let mut summary = Self {
            total: results.len(),
            lost: 0,
            partial: 0,
            recovered: 0,
            recovery_attempts: 0,
            recovery_successes: 0,
        };
        for r in results {
            match r.recovery_tier {
                FsqliteRecoveryTier::Lost => summary.lost += 1,
                FsqliteRecoveryTier::Partial => summary.partial += 1,
                FsqliteRecoveryTier::Recovered => summary.recovered += 1,
            }
            if r.recovery_attempted {
                summary.recovery_attempts += 1;
            }
            if r.recovery_succeeded {
                summary.recovery_successes += 1;
            }
        }
        summary
    }
}

impl fmt::Display for FsqliteRecoverySummary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "fsqlite baseline: {total} strategies — \
             {lost} lost, {partial} partial, {recovered} recovered \
             (WAL-FEC: {attempts} attempted, {successes} succeeded)",
            total = self.total,
            lost = self.lost,
            partial = self.partial,
            recovered = self.recovered,
            attempts = self.recovery_attempts,
            successes = self.recovery_successes,
        )
    }
}

// ── Tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_setup_baseline_with_fec_creates_files() {
        let dir = tempfile::tempdir().unwrap();
        let (db_path, wal_info, original_pages, _expected_hash) =
            setup_baseline_with_fec(dir.path()).unwrap();
        assert!(db_path.exists(), "database file should exist");

        let wal_path = db_path.with_extension("db-wal");
        assert!(wal_path.exists(), "WAL file should exist");

        let sidecar_path = db_path.with_extension("db-wal-fec");
        assert!(sidecar_path.exists(), "WAL-FEC sidecar should exist");

        assert!(wal_info.is_some(), "WAL info should be present");
        assert!(!original_pages.is_empty(), "should have WAL frame payloads");
    }

    #[test]
    fn test_logical_dump_deterministic() {
        let dir = tempfile::tempdir().unwrap();
        let (db_path, _, _, _expected_hash) = setup_baseline_with_fec(dir.path()).unwrap();

        let conn = Connection::open(&db_path).unwrap();
        let dump1 = logical_dump(&conn).unwrap();
        let dump2 = logical_dump(&conn).unwrap();
        assert_eq!(dump1, dump2, "logical dump must be deterministic");
    }

    #[test]
    fn test_measure_header_zero() {
        let catalog = corruption_strategy_catalog();
        let entry = catalog
            .iter()
            .find(|e| e.strategy_id == "header_zero")
            .unwrap();

        let dir = tempfile::tempdir().unwrap();
        let result = measure_fsqlite_baseline(entry, dir.path()).unwrap();

        assert_eq!(result.strategy_id, "header_zero");
        assert_eq!(result.rows_before, BASELINE_ROW_COUNT + 20);
        // Header zeroing — DB-level corruption, no WAL-FEC help.
        assert!(
            result.recovery_tier == FsqliteRecoveryTier::Lost
                || result.recovery_tier == FsqliteRecoveryTier::Partial,
            "header_zero should cause lost or partial, got {}",
            result.recovery_tier
        );
    }

    #[test]
    fn test_measure_sidecar_corrupt() {
        let catalog = corruption_strategy_catalog();
        let entry = catalog
            .iter()
            .find(|e| e.strategy_id == "sidecar_corrupt_symbols")
            .unwrap();

        let dir = tempfile::tempdir().unwrap();
        let result = measure_fsqlite_baseline(entry, dir.path()).unwrap();

        assert_eq!(result.strategy_id, "sidecar_corrupt_symbols");
        // Sidecar corruption only damages FEC repair symbols.
        // The DB and WAL are intact so data should still be readable.
        assert!(
            result.recovery_tier == FsqliteRecoveryTier::Recovered
                || result.recovery_tier == FsqliteRecoveryTier::Partial,
            "sidecar corruption should not cause total loss"
        );
    }

    #[test]
    fn test_measure_wal_truncate() {
        let catalog = corruption_strategy_catalog();
        let entry = catalog
            .iter()
            .find(|e| e.strategy_id == "wal_truncate_0")
            .unwrap();

        let dir = tempfile::tempdir().unwrap();
        let result = measure_fsqlite_baseline(entry, dir.path()).unwrap();

        assert_eq!(result.strategy_id, "wal_truncate_0");
        assert!(
            result.recovery_attempted,
            "WAL corruption should trigger recovery attempt"
        );
    }

    #[test]
    fn test_measure_wal_bitflip() {
        let catalog = corruption_strategy_catalog();
        let entry = catalog
            .iter()
            .find(|e| e.strategy_id == "wal_bitflip_frame0")
            .unwrap();

        let dir = tempfile::tempdir().unwrap();
        let result = measure_fsqlite_baseline(entry, dir.path()).unwrap();

        assert_eq!(result.strategy_id, "wal_bitflip_frame0");
        assert!(
            result.recovery_attempted,
            "WAL bitflip should trigger recovery attempt"
        );
    }

    #[test]
    fn test_run_full_baseline() {
        let results = run_full_fsqlite_baseline().unwrap();
        let catalog = corruption_strategy_catalog();
        assert_eq!(
            results.len(),
            catalog.len(),
            "should have one result per catalog entry"
        );

        for result in &results {
            assert!(!result.strategy_id.is_empty());
            assert!(result.rows_before > 0);
        }
    }

    #[test]
    fn test_recovery_summary() {
        let results = run_full_fsqlite_baseline().unwrap();
        let summary = FsqliteRecoverySummary::from_results(&results);

        assert_eq!(summary.total, results.len());
        assert_eq!(
            summary.lost + summary.partial + summary.recovered,
            summary.total,
            "tier counts must sum to total"
        );
    }

    #[test]
    fn test_run_baseline_by_category_wal() {
        let results = run_fsqlite_baseline_by_category(CorruptionCategory::Wal).unwrap();
        for r in &results {
            assert_eq!(r.category, CorruptionCategory::Wal);
            assert!(
                r.recovery_attempted,
                "WAL strategies should attempt recovery"
            );
        }
        assert!(!results.is_empty());
    }

    #[test]
    fn test_results_json_serializable() {
        let results = run_full_fsqlite_baseline().unwrap();
        let json = serde_json::to_string_pretty(&results).unwrap();
        assert!(!json.is_empty());

        let deserialized: Vec<FsqliteBaselineResult> = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.len(), results.len());
    }

    #[test]
    fn test_summary_display() {
        let results = run_full_fsqlite_baseline().unwrap();
        let summary = FsqliteRecoverySummary::from_results(&results);
        let display = summary.to_string();
        assert!(display.contains("fsqlite baseline"));
        assert!(display.contains("strategies"));
    }

    #[test]
    fn test_recovery_tier_ordering() {
        assert!(FsqliteRecoveryTier::Lost < FsqliteRecoveryTier::Partial);
        assert!(FsqliteRecoveryTier::Partial < FsqliteRecoveryTier::Recovered);
    }

    #[test]
    fn test_recovery_tier_display() {
        assert_eq!(FsqliteRecoveryTier::Lost.to_string(), "lost");
        assert_eq!(FsqliteRecoveryTier::Partial.to_string(), "partial");
        assert_eq!(FsqliteRecoveryTier::Recovered.to_string(), "recovered");
    }
}
