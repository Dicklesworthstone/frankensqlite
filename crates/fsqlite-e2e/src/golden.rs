//! Golden copy management — load, hash, and compare database snapshots.

use std::path::{Path, PathBuf};

use std::fmt::Write as _;

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
    /// SQLite page size.
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
}
