//! Golden copy management — load, hash, and compare database snapshots.

use std::path::{Path, PathBuf};

use std::fmt::Write;

use fsqlite_vfs::host_fs;
use sha2::{Digest, Sha256};

use crate::{E2eError, E2eResult};

// ─── Golden directory discovery ────────────────────────────────────────

/// Default path to the golden database directory, relative to the workspace root.
pub const GOLDEN_DIR_RELATIVE: &str = "sample_sqlite_db_files/golden";

/// Discover all `.db` files in the given directory.
///
/// Returns a sorted list of paths for deterministic ordering.
///
/// # Errors
///
/// Returns `E2eError::Io` if the directory cannot be read.
pub fn discover_golden_files(dir: &Path) -> E2eResult<Vec<PathBuf>> {
    let mut files = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().is_some_and(|ext| ext == "db") && path.is_file() {
            files.push(path);
        }
    }
    files.sort();
    Ok(files)
}

/// Result of validating a single golden database file.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct IntegrityReport {
    /// Database file name (stem only).
    pub name: String,
    /// Whether `PRAGMA integrity_check` returned "ok".
    pub integrity_ok: bool,
    /// Page count from `PRAGMA page_count`.
    pub page_count: i64,
    /// Number of objects in `sqlite_master` (tables, views, triggers, indexes).
    pub master_count: i64,
    /// Raw integrity check result string (first line).
    pub integrity_result: String,
}

/// Validate a single golden database file.
///
/// Opens the file read-only via rusqlite and checks:
/// 1. `PRAGMA integrity_check` returns "ok"
/// 2. `PRAGMA page_count` > 0
/// 3. At least one object in `sqlite_master`
///
/// # Errors
///
/// Returns `E2eError::Rusqlite` on connection or query errors.
pub fn validate_golden_integrity(path: &Path) -> E2eResult<IntegrityReport> {
    let name = path.file_stem().map_or_else(
        || "unknown".to_owned(),
        |s| s.to_string_lossy().into_owned(),
    );

    let conn = rusqlite::Connection::open_with_flags(
        path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;

    let integrity_result: String =
        conn.query_row("PRAGMA integrity_check", [], |row| row.get(0))?;
    let integrity_ok = integrity_result == "ok";

    let page_count: i64 = conn.query_row("PRAGMA page_count", [], |row| row.get(0))?;

    let master_count: i64 =
        conn.query_row("SELECT count(*) FROM sqlite_master", [], |row| row.get(0))?;

    Ok(IntegrityReport {
        name,
        integrity_ok,
        page_count,
        master_count,
        integrity_result,
    })
}

/// Validate all golden database files in a directory.
///
/// Returns a vec of reports. Fails fast on any I/O or connection error,
/// but does NOT fail on integrity check failures — the caller should
/// inspect the returned reports.
///
/// # Errors
///
/// Returns `E2eError::Io` if the directory cannot be read, or
/// `E2eError::Rusqlite` if a database cannot be opened.
pub fn validate_all_golden(dir: &Path) -> E2eResult<Vec<IntegrityReport>> {
    let files = discover_golden_files(dir)?;
    let mut reports = Vec::with_capacity(files.len());
    for path in &files {
        reports.push(validate_golden_integrity(path)?);
    }
    Ok(reports)
}

/// Metadata about a golden database file.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DbMetadata {
    /// Number of tables in the database.
    pub table_count: usize,
    /// Total number of rows across all tables.
    pub row_count: usize,
    /// `SQLite` page size.
    pub page_size: u32,
}

/// A golden database snapshot used as a reference during testing.
#[derive(Debug, Clone)]
pub struct GoldenCopy {
    /// Human-readable name for this golden copy.
    pub name: String,
    /// Path to the golden database file.
    pub path: PathBuf,
    /// Expected SHA-256 hex digest.
    pub sha256: String,
    /// Structural metadata.
    pub metadata: DbMetadata,
}

impl GoldenCopy {
    /// Compute the SHA-256 hex digest of a file at `path`.
    ///
    /// # Errors
    ///
    /// Returns `E2eError::Io` if the file cannot be read.
    pub fn hash_file(path: &Path) -> E2eResult<String> {
        let bytes = host_fs::read(path).map_err(|e| std::io::Error::other(e.to_string()))?;
        let mut hasher = Sha256::new();
        hasher.update(&bytes);
        let digest = hasher.finalize();
        let mut hex = String::with_capacity(64);
        for byte in digest {
            let _ = write!(hex, "{byte:02x}");
        }
        Ok(hex)
    }

    /// Verify that the file at `path` matches the expected hash.
    ///
    /// # Errors
    ///
    /// Returns `E2eError::HashMismatch` on mismatch, or `E2eError::Io` on
    /// read failure.
    pub fn verify_hash(&self, path: &Path) -> E2eResult<()> {
        let actual = Self::hash_file(path)?;
        if actual == self.sha256 {
            Ok(())
        } else {
            Err(E2eError::HashMismatch {
                expected: self.sha256.clone(),
                actual,
            })
        }
    }
}

// ─── Three-tier recovery verification (bd-391r) ──────────────────────

/// Verification tier indicating the strength of a recovery proof.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VerificationTier {
    /// SHA-256 match: cryptographic proof of bit-for-bit recovery.
    Tier1Sha256,
    /// Logical equivalence: schema + rows match despite layout differences.
    Tier2Logical,
    /// Data completeness: row counts and spot-checks match.
    Tier3Completeness,
}

/// Outcome of a single verification tier.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TierOutcome {
    pub tier: VerificationTier,
    pub passed: bool,
    pub detail: String,
}

/// Result of three-tier recovery verification comparing a golden copy
/// against a repaired database.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RecoveryVerificationResult {
    pub golden_sha256: String,
    pub repaired_sha256: String,
    pub tier1: TierOutcome,
    pub tier2: TierOutcome,
    pub tier3: TierOutcome,
    /// Highest tier that passed (strongest proof achieved).
    pub highest_tier: Option<VerificationTier>,
}

/// Collect table names from a rusqlite connection (user tables only).
fn list_user_tables(conn: &rusqlite::Connection) -> E2eResult<Vec<String>> {
    let mut stmt = conn.prepare(
        "SELECT name FROM sqlite_master WHERE type='table' \
         AND name NOT LIKE 'sqlite_%' ORDER BY name",
    )?;
    let tables: Vec<String> = stmt
        .query_map([], |row| row.get(0))?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(tables)
}

/// Build a deterministic logical dump: schema DDL + sorted rows per table.
fn logical_dump(conn: &rusqlite::Connection) -> E2eResult<String> {
    let mut dump = String::new();

    // Schema: all DDL from sqlite_master, sorted by name.
    let mut schema_stmt =
        conn.prepare("SELECT sql FROM sqlite_master WHERE sql IS NOT NULL ORDER BY name")?;
    let sqls: Vec<String> = schema_stmt
        .query_map([], |row| row.get(0))?
        .collect::<Result<Vec<_>, _>>()?;
    for sql in &sqls {
        let _ = writeln!(dump, "{sql};");
    }
    dump.push_str("---\n");

    // Data: per table, SELECT * ORDER BY first column for stable ordering.
    let tables = list_user_tables(conn)?;
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

/// Count total rows across all user tables.
fn total_row_count(conn: &rusqlite::Connection) -> E2eResult<usize> {
    let tables = list_user_tables(conn)?;
    let mut total: usize = 0;
    for table in &tables {
        let count: i64 = conn.query_row(&format!("SELECT count(*) FROM \"{table}\""), [], |r| {
            r.get(0)
        })?;
        total += usize::try_from(count).unwrap_or(0);
    }
    Ok(total)
}

/// Perform three-tier recovery verification comparing a golden copy
/// against a repaired database.
///
/// **Tier 1 (SHA-256 match)**: Compare raw file hashes. If they match,
/// recovery is cryptographically proven correct.
///
/// **Tier 2 (Logical equivalence)**: Compare deterministic dumps of
/// schema + all rows sorted by primary key. Catches cases where page
/// layout differs but data is identical.
///
/// **Tier 3 (Data completeness)**: Compare total row counts and run
/// `PRAGMA integrity_check` on both. The minimum bar for partial
/// recovery.
///
/// # Errors
///
/// Returns `E2eError::Io` if files cannot be read, or
/// `E2eError::Rusqlite` if databases cannot be opened.
pub fn verify_recovery(
    golden_path: &Path,
    repaired_path: &Path,
) -> E2eResult<RecoveryVerificationResult> {
    let golden_sha256 = GoldenCopy::hash_file(golden_path)?;
    let repaired_sha256 = GoldenCopy::hash_file(repaired_path)?;

    // Tier 1: SHA-256 match.
    let tier1_passed = golden_sha256 == repaired_sha256;
    let tier1 = TierOutcome {
        tier: VerificationTier::Tier1Sha256,
        passed: tier1_passed,
        detail: if tier1_passed {
            "SHA-256 match: bit-for-bit recovery confirmed.".to_owned()
        } else {
            format!(
                "SHA-256 mismatch: golden={}, repaired={}.",
                &golden_sha256[..16],
                &repaired_sha256[..16],
            )
        },
    };

    // Open both databases read-only for Tiers 2 and 3.
    let flags =
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX;
    let golden_conn = rusqlite::Connection::open_with_flags(golden_path, flags)?;
    let repaired_conn = rusqlite::Connection::open_with_flags(repaired_path, flags)?;

    // Tier 2: Logical equivalence.
    let golden_dump = logical_dump(&golden_conn)?;
    let repaired_dump = logical_dump(&repaired_conn)?;
    let tier2_passed = golden_dump == repaired_dump;
    let tier2 = TierOutcome {
        tier: VerificationTier::Tier2Logical,
        passed: tier2_passed,
        detail: if tier2_passed {
            "Logical equivalence: schema and all rows match.".to_owned()
        } else {
            "Logical mismatch: schema or row data differs.".to_owned()
        },
    };

    // Tier 3: Data completeness (row counts + integrity check).
    let golden_rows = total_row_count(&golden_conn)?;
    let repaired_rows = total_row_count(&repaired_conn)?;
    let golden_integrity: String =
        golden_conn.query_row("PRAGMA integrity_check", [], |r| r.get(0))?;
    let repaired_integrity: String =
        repaired_conn.query_row("PRAGMA integrity_check", [], |r| r.get(0))?;

    let rows_match = golden_rows == repaired_rows;
    let both_integrity_ok = golden_integrity == "ok" && repaired_integrity == "ok";
    let tier3_passed = rows_match && both_integrity_ok;
    let tier3 = TierOutcome {
        tier: VerificationTier::Tier3Completeness,
        passed: tier3_passed,
        detail: format!(
            "rows: golden={golden_rows}, repaired={repaired_rows} ({}); \
             integrity: golden={golden_integrity}, repaired={repaired_integrity}",
            if rows_match { "match" } else { "MISMATCH" },
        ),
    };

    // Highest tier that passed.
    let highest_tier = if tier1_passed {
        Some(VerificationTier::Tier1Sha256)
    } else if tier2_passed {
        Some(VerificationTier::Tier2Logical)
    } else if tier3_passed {
        Some(VerificationTier::Tier3Completeness)
    } else {
        None
    };

    Ok(RecoveryVerificationResult {
        golden_sha256,
        repaired_sha256,
        tier1,
        tier2,
        tier3,
        highest_tier,
    })
}

// ─── Canonicalization pipeline (bd-1w6k.5.2) ─────────────────────────
//
// The canonical implementation lives in `crate::canonicalize`.  Re-exports
// below provide backward-compatible access from this module.

/// Re-export: result of canonicalizing a database file.
pub use crate::canonicalize::CanonicalResult;

/// Produce a deterministic canonical copy of a database via `VACUUM INTO`
/// with fixed PRAGMAs, then return its SHA-256 hash.
///
/// Delegates to [`crate::canonicalize::canonicalize`].
///
/// # Errors
///
/// Returns `E2eError::Rusqlite` on connection, PRAGMA, or VACUUM errors,
/// or `E2eError::Io` if the resulting file cannot be hashed.
pub fn canonicalize_database(source: &Path, dest: &Path) -> E2eResult<CanonicalResult> {
    crate::canonicalize::canonicalize(source, dest)
}

/// Convenience: canonicalize a database to a temporary file and return only
/// the SHA-256 hash.
///
/// Delegates to [`crate::canonicalize::canonical_sha256`].
///
/// # Errors
///
/// Propagates errors from [`crate::canonicalize::canonicalize`].
pub fn canonical_sha256(source: &Path) -> E2eResult<String> {
    crate::canonicalize::canonical_sha256(source)
}

// ─── SHA-256 hashing + mismatch reporting (bd-1w6k.5.3) ──────────────

/// Compute a full [`CorrectnessReport`] for a database file.
///
/// Fills in all three SHA-256 tiers (raw, canonical, logical) and the
/// integrity check result.  If a particular tier cannot be computed (e.g.
/// canonicalization fails), it is recorded as `None` with a note rather
/// than propagating the error.
///
/// # Errors
///
/// Returns `E2eError::Io` if the file cannot be read at all, or
/// `E2eError::Rusqlite` if the database cannot be opened.
pub fn compute_correctness_report(db_path: &Path) -> E2eResult<crate::report::CorrectnessReport> {
    use sha2::{Digest, Sha256};

    let mut notes = Vec::<String>::new();

    // Raw SHA-256 of the on-disk file.
    let raw_sha256 = GoldenCopy::hash_file(db_path).ok();
    if raw_sha256.is_none() {
        notes.push("raw_sha256: file read failed".to_owned());
    }

    // Open the database for integrity check and logical dump.
    let flags =
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX;
    let conn = rusqlite::Connection::open_with_flags(db_path, flags)?;

    // Integrity check.
    let integrity_check_ok = conn
        .query_row("PRAGMA integrity_check", [], |r| r.get::<_, String>(0))
        .ok()
        .map(|s| s == "ok");

    // Logical SHA-256.
    let logical_sha256 = logical_dump(&conn).ok().map(|dump| {
        let digest = Sha256::digest(dump.as_bytes());
        let mut hex = String::with_capacity(64);
        for byte in digest {
            let _ = write!(hex, "{byte:02x}");
        }
        hex
    });

    drop(conn);

    // Canonical SHA-256 (via VACUUM INTO).
    let canonical_sha256 = match crate::canonicalize::canonical_sha256(db_path) {
        Ok(h) => Some(h),
        Err(e) => {
            notes.push(format!("canonical_sha256: {e}"));
            None
        }
    };

    let notes_str = if notes.is_empty() {
        None
    } else {
        Some(notes.join("; "))
    };

    Ok(crate::report::CorrectnessReport {
        raw_sha256_match: None,
        dump_match: None,
        canonical_sha256_match: None,
        integrity_check_ok,
        raw_sha256,
        canonical_sha256,
        logical_sha256,
        notes: notes_str,
    })
}

/// Actionable mismatch diagnostic produced when two databases differ.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MismatchDiagnostic {
    /// Which tier triggered the mismatch (or `None` if insufficient data).
    pub failed_tier: String,
    /// Paths of the compared databases.
    pub path_a: PathBuf,
    pub path_b: PathBuf,
    /// The full comparison report with verdict and tier details.
    pub comparison: crate::report::ComparisonReport,
    /// Quick triage hints to help narrow down the cause.
    pub triage_hints: Vec<String>,
}

/// Collect triage hints for a potential mismatch between two database files.
fn collect_triage_hints(path_a: &Path, path_b: &Path) -> Vec<String> {
    let mut hints = Vec::new();

    // WAL sidecar check.
    let wal_a = path_a.with_extension("db-wal");
    let wal_b = path_b.with_extension("db-wal");
    if wal_a.exists() {
        hints.push(format!("WAL sidecar present: {}", wal_a.display()));
    }
    if wal_b.exists() {
        hints.push(format!("WAL sidecar present: {}", wal_b.display()));
    }

    // SHM sidecar check.
    let shm_a = path_a.with_extension("db-shm");
    let shm_b = path_b.with_extension("db-shm");
    if shm_a.exists() {
        hints.push(format!("SHM sidecar present: {}", shm_a.display()));
    }
    if shm_b.exists() {
        hints.push(format!("SHM sidecar present: {}", shm_b.display()));
    }

    // Journal sidecar check.
    let journal_a = path_a.with_extension("db-journal");
    let journal_b = path_b.with_extension("db-journal");
    if journal_a.exists() {
        hints.push(format!("journal sidecar present: {}", journal_a.display()));
    }
    if journal_b.exists() {
        hints.push(format!("journal sidecar present: {}", journal_b.display()));
    }

    // PRAGMA comparison hints.
    let flags =
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX;
    if let (Ok(ca), Ok(cb)) = (
        rusqlite::Connection::open_with_flags(path_a, flags),
        rusqlite::Connection::open_with_flags(path_b, flags),
    ) {
        // page_size mismatch.
        if let (Ok(ps_a), Ok(ps_b)) = (
            ca.query_row("PRAGMA page_size", [], |r| r.get::<_, i64>(0)),
            cb.query_row("PRAGMA page_size", [], |r| r.get::<_, i64>(0)),
        ) {
            if ps_a != ps_b {
                hints.push(format!("page_size mismatch: {ps_a} vs {ps_b}"));
            }
        }
        // auto_vacuum mismatch.
        if let (Ok(av_a), Ok(av_b)) = (
            ca.query_row("PRAGMA auto_vacuum", [], |r| r.get::<_, i64>(0)),
            cb.query_row("PRAGMA auto_vacuum", [], |r| r.get::<_, i64>(0)),
        ) {
            if av_a != av_b {
                hints.push(format!("auto_vacuum mismatch: {av_a} vs {av_b}"));
            }
        }
        // journal_mode mismatch.
        if let (Ok(jm_a), Ok(jm_b)) = (
            ca.query_row("PRAGMA journal_mode", [], |r| r.get::<_, String>(0)),
            cb.query_row("PRAGMA journal_mode", [], |r| r.get::<_, String>(0)),
        ) {
            if jm_a != jm_b {
                hints.push(format!("journal_mode mismatch: {jm_a} vs {jm_b}"));
            }
        }
    }

    if hints.is_empty() {
        hints.push("no obvious triage hints found — investigate logical dumps".to_owned());
    }

    hints
}

/// Compare two databases end-to-end using all three equality tiers.
///
/// Returns a [`crate::report::ComparisonReport`] with the verdict, and an
/// optional [`MismatchDiagnostic`] when the verdict is not `Match`.
///
/// # Errors
///
/// Returns `E2eError::Rusqlite` or `E2eError::Io` if either database
/// cannot be opened or read.
pub fn verify_databases(
    path_a: &Path,
    path_b: &Path,
) -> E2eResult<(crate::report::ComparisonReport, Option<MismatchDiagnostic>)> {
    let cr_a = compute_correctness_report(path_a)?;
    let cr_b = compute_correctness_report(path_b)?;
    let comparison = crate::report::ComparisonReport::derive(&cr_a, &cr_b);

    let diagnostic = if matches!(comparison.verdict, crate::report::ComparisonVerdict::Match) {
        None
    } else {
        let failed_tier = match comparison.verdict {
            crate::report::ComparisonVerdict::Mismatch => {
                if comparison.tiers.canonical_sha256_match == Some(false) {
                    "canonical_sha256".to_owned()
                } else if comparison.tiers.logical_match == Some(false) {
                    "logical".to_owned()
                } else {
                    "unknown".to_owned()
                }
            }
            _ => "insufficient_data".to_owned(),
        };

        let triage_hints = collect_triage_hints(path_a, path_b);

        Some(MismatchDiagnostic {
            failed_tier,
            path_a: path_a.to_path_buf(),
            path_b: path_b.to_path_buf(),
            comparison: comparison.clone(),
            triage_hints,
        })
    };

    Ok((comparison, diagnostic))
}

/// Format a [`MismatchDiagnostic`] as a human-readable report string.
#[must_use]
pub fn format_mismatch_diagnostic(diag: &MismatchDiagnostic) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "=== MISMATCH DETECTED ===");
    let _ = writeln!(out, "Failed tier: {}", diag.failed_tier);
    let _ = writeln!(out, "Verdict: {:?}", diag.comparison.verdict);
    let _ = writeln!(out, "Explanation: {}", diag.comparison.explanation);
    let _ = writeln!(out);
    let _ = writeln!(out, "Paths:");
    let _ = writeln!(out, "  A: {}", diag.path_a.display());
    let _ = writeln!(out, "  B: {}", diag.path_b.display());
    let _ = writeln!(out);
    let _ = writeln!(out, "Tier details:");
    let _ = writeln!(
        out,
        "  raw_sha256_match:       {:?}",
        diag.comparison.tiers.raw_sha256_match
    );
    let _ = writeln!(
        out,
        "  canonical_sha256_match: {:?}",
        diag.comparison.tiers.canonical_sha256_match
    );
    let _ = writeln!(
        out,
        "  logical_match:          {:?}",
        diag.comparison.tiers.logical_match
    );
    let _ = writeln!(out);
    let _ = writeln!(out, "Triage hints:");
    for hint in &diag.triage_hints {
        let _ = writeln!(out, "  - {hint}");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_file_deterministic() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        std::fs::write(&path, b"hello world").unwrap();

        let h1 = GoldenCopy::hash_file(&path).unwrap();
        let h2 = GoldenCopy::hash_file(&path).unwrap();
        assert_eq!(h1, h2, "hashing the same file must be deterministic");
        // Known SHA-256 of "hello world"
        assert_eq!(
            h1,
            "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        );
    }

    /// Helper: create a test SQLite database with known data.
    fn create_test_db(path: &Path, rows: usize) {
        let conn = rusqlite::Connection::open(path).unwrap();
        conn.execute_batch("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, value REAL);")
            .unwrap();
        for i in 1..=rows {
            let id = i64::try_from(i).expect("row id must fit i64");
            conn.execute(
                "INSERT INTO items VALUES (?1, ?2, ?3)",
                rusqlite::params![id, format!("item_{i}"), i as f64 * 1.5],
            )
            .unwrap();
        }
        // Checkpoint WAL to main file for clean comparison.
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .unwrap();
    }

    #[test]
    fn verify_recovery_tier1_identical_databases() {
        let dir = tempfile::tempdir().unwrap();
        let golden = dir.path().join("golden.db");
        let repaired = dir.path().join("repaired.db");

        create_test_db(&golden, 100);
        std::fs::copy(&golden, &repaired).unwrap();

        let result = verify_recovery(&golden, &repaired).unwrap();
        assert!(
            result.tier1.passed,
            "Tier 1 should pass for identical files"
        );
        assert!(
            result.tier2.passed,
            "Tier 2 should pass for identical files"
        );
        assert!(
            result.tier3.passed,
            "Tier 3 should pass for identical files"
        );
        assert_eq!(result.highest_tier, Some(VerificationTier::Tier1Sha256));
    }

    #[test]
    fn verify_recovery_tier2_logically_equivalent() {
        let dir = tempfile::tempdir().unwrap();
        let golden = dir.path().join("golden.db");
        let repaired = dir.path().join("repaired.db");

        // Create identical content in two separate databases.
        create_test_db(&golden, 50);
        create_test_db(&repaired, 50);

        let result = verify_recovery(&golden, &repaired).unwrap();
        // SHA-256 may or may not match depending on SQLite internals,
        // but logical content should match.
        assert!(result.tier2.passed, "Tier 2 should pass for same content");
        assert!(result.tier3.passed, "Tier 3 should pass for same content");
    }

    #[test]
    fn verify_recovery_tier3_row_count_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let golden = dir.path().join("golden.db");
        let repaired = dir.path().join("repaired.db");

        create_test_db(&golden, 100);
        create_test_db(&repaired, 50);

        let result = verify_recovery(&golden, &repaired).unwrap();
        assert!(!result.tier1.passed, "Tier 1 should fail");
        assert!(!result.tier2.passed, "Tier 2 should fail");
        assert!(
            !result.tier3.passed,
            "Tier 3 should fail: different row counts"
        );
        assert_eq!(result.highest_tier, None);
    }

    #[test]
    fn verify_recovery_highest_tier_is_correct() {
        let dir = tempfile::tempdir().unwrap();
        let golden = dir.path().join("golden.db");
        let repaired = dir.path().join("repaired.db");

        create_test_db(&golden, 100);
        std::fs::copy(&golden, &repaired).unwrap();

        let result = verify_recovery(&golden, &repaired).unwrap();
        // Identical files => Tier 1 is highest.
        assert_eq!(result.highest_tier, Some(VerificationTier::Tier1Sha256));
    }

    // ─── Canonicalization tests ───────────────────────────────────────

    #[test]
    fn canonicalize_produces_stable_hash() {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("source.db");
        create_test_db(&source, 200);

        let dest1 = dir.path().join("canon1.db");
        let dest2 = dir.path().join("canon2.db");

        let r1 = canonicalize_database(&source, &dest1).unwrap();
        let r2 = canonicalize_database(&source, &dest2).unwrap();

        assert_eq!(
            r1.sha256, r2.sha256,
            "canonical hash must be stable across runs"
        );
        assert!(r1.size_bytes > 0, "canonical must have content");
    }

    #[test]
    fn canonicalize_same_content_different_sources_match() {
        let dir = tempfile::tempdir().unwrap();
        let src_a = dir.path().join("a.db");
        let src_b = dir.path().join("b.db");

        // Create two separate databases with identical content.
        create_test_db(&src_a, 100);
        create_test_db(&src_b, 100);

        let hash_a = canonical_sha256(&src_a).unwrap();
        let hash_b = canonical_sha256(&src_b).unwrap();

        assert_eq!(
            hash_a, hash_b,
            "databases with identical logical content must produce identical canonical hashes"
        );
    }

    #[test]
    fn canonicalize_different_content_differ() {
        let dir = tempfile::tempdir().unwrap();
        let small = dir.path().join("small.db");
        let large = dir.path().join("large.db");

        create_test_db(&small, 10);
        create_test_db(&large, 500);

        let hash_small = canonical_sha256(&small).unwrap();
        let hash_large = canonical_sha256(&large).unwrap();

        assert_ne!(
            hash_small, hash_large,
            "databases with different content must produce different hashes"
        );
    }

    #[test]
    fn canonicalize_does_not_modify_source() {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("source.db");
        create_test_db(&source, 50);

        let hash_before = GoldenCopy::hash_file(&source).unwrap();
        let _ = canonical_sha256(&source).unwrap();
        let hash_after = GoldenCopy::hash_file(&source).unwrap();

        assert_eq!(
            hash_before, hash_after,
            "canonicalization must never modify the source file"
        );
    }

    // ─── sha256 hashing + mismatch reporting tests (bd-1w6k.5.3) ─────

    #[test]
    fn correctness_report_populates_all_tiers() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("test.db");
        create_test_db(&db, 50);

        let report = compute_correctness_report(&db).unwrap();
        assert!(report.raw_sha256.is_some(), "raw_sha256 must be populated");
        assert!(
            report.canonical_sha256.is_some(),
            "canonical_sha256 must be populated"
        );
        assert!(
            report.logical_sha256.is_some(),
            "logical_sha256 must be populated"
        );
        assert_eq!(
            report.integrity_check_ok,
            Some(true),
            "integrity_check must pass"
        );
        assert!(report.notes.is_none(), "no error notes expected");
    }

    #[test]
    fn verify_databases_match_for_identical_content() {
        let dir = tempfile::tempdir().unwrap();
        let a = dir.path().join("a.db");
        let b = dir.path().join("b.db");

        create_test_db(&a, 100);
        create_test_db(&b, 100);

        let (comparison, diagnostic) = verify_databases(&a, &b).unwrap();
        assert!(
            matches!(comparison.verdict, crate::report::ComparisonVerdict::Match),
            "identical content should match: {:?}",
            comparison.explanation
        );
        assert!(diagnostic.is_none(), "no diagnostic for matching databases");
    }

    #[test]
    fn verify_databases_mismatch_produces_diagnostic() {
        let dir = tempfile::tempdir().unwrap();
        let a = dir.path().join("a.db");
        let b = dir.path().join("b.db");

        create_test_db(&a, 100);
        create_test_db(&b, 50);

        let (comparison, diagnostic) = verify_databases(&a, &b).unwrap();
        assert!(
            matches!(
                comparison.verdict,
                crate::report::ComparisonVerdict::Mismatch
            ),
            "different content should mismatch"
        );
        let diag = diagnostic.expect("diagnostic must be present on mismatch");
        assert!(
            !diag.triage_hints.is_empty(),
            "triage hints must be present"
        );
        assert!(!diag.failed_tier.is_empty(), "failed_tier must be set");
    }

    #[test]
    fn mismatch_diagnostic_format_is_human_readable() {
        let dir = tempfile::tempdir().unwrap();
        let a = dir.path().join("a.db");
        let b = dir.path().join("b.db");

        create_test_db(&a, 100);
        create_test_db(&b, 50);

        let (_, diagnostic) = verify_databases(&a, &b).unwrap();
        let diag = diagnostic.unwrap();
        let text = format_mismatch_diagnostic(&diag);
        assert!(text.contains("MISMATCH DETECTED"));
        assert!(text.contains("Failed tier:"));
        assert!(text.contains("Triage hints:"));
        assert!(text.contains("Paths:"));
    }

    #[test]
    fn triage_hints_detect_wal_sidecar() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("test.db");
        create_test_db(&db, 10);

        // Create a fake WAL sidecar.
        let wal = db.with_extension("db-wal");
        std::fs::write(&wal, b"fake wal").unwrap();

        let dummy = dir.path().join("other.db");
        create_test_db(&dummy, 10);

        let hints = collect_triage_hints(&db, &dummy);
        assert!(
            hints.iter().any(|h| h.contains("WAL sidecar present")),
            "should detect WAL sidecar: {hints:?}"
        );
    }

    #[test]
    fn correctness_report_is_deterministic() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("test.db");
        create_test_db(&db, 30);

        let r1 = compute_correctness_report(&db).unwrap();
        let r2 = compute_correctness_report(&db).unwrap();

        assert_eq!(r1.raw_sha256, r2.raw_sha256);
        assert_eq!(r1.canonical_sha256, r2.canonical_sha256);
        assert_eq!(r1.logical_sha256, r2.logical_sha256);
    }
}
