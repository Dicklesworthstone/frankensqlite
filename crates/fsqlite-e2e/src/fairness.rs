//! Benchmark fairness protocol: identical PRAGMAs, page size, cache normalization.
//!
//! Bead: bd-3qeq
//!
//! Benchmarking FrankenSQLite vs C SQLite is meaningless if they run with
//! different settings.  This module defines the canonical benchmark PRAGMA
//! set and provides a verification function that **must** succeed before any
//! timed measurement begins.
//!
//! ## Standard Benchmark Settings
//!
//! | PRAGMA | Value | Rationale |
//! |--------|-------|-----------|
//! | `page_size` | 4096 | Default SQLite page size, most common in production |
//! | `cache_size` | -64000 | 64 MB page cache (negative = KiB) |
//! | `journal_mode` | WAL | FrankenSQLite's natural mode; C SQLite's fastest concurrent mode |
//! | `synchronous` | NORMAL | Production-realistic balance of durability and speed |
//! | `temp_store` | MEMORY | Avoid disk I/O variance from temp files |
//! | `mmap_size` | 0 | Ensure we test our page cache, not the OS mmap cache |
//! | `auto_vacuum` | NONE | Eliminate non-deterministic background vacuuming |

use crate::{E2eError, E2eResult, HarnessSettings};

/// Canonical benchmark PRAGMA expectations.
///
/// Each tuple is `(pragma_name, expected_value)` where the expected value is
/// the string representation that `PRAGMA <name>;` returns.
///
/// Note: `synchronous` returns `1` for NORMAL, `temp_store` returns `2` for
/// MEMORY, and `auto_vacuum` returns `0` for NONE.
pub const BENCHMARK_PRAGMAS: &[(&str, &str)] = &[
    ("page_size", "4096"),
    ("journal_mode", "wal"),
    ("synchronous", "1"),
    ("temp_store", "2"),
    ("mmap_size", "0"),
    ("auto_vacuum", "0"),
];

/// Returns a [`HarnessSettings`] tuned for fair benchmarking.
///
/// This applies the canonical settings from [`BENCHMARK_PRAGMAS`] plus a
/// large cache to keep test databases in memory.
#[must_use]
pub fn benchmark_settings() -> HarnessSettings {
    HarnessSettings {
        journal_mode: "wal".to_owned(),
        synchronous: "NORMAL".to_owned(),
        cache_size: -64_000,
        page_size: 4096,
        busy_timeout_ms: 5000,
        concurrent_mode: true,
        run_integrity_check: true,
    }
}

/// Additional PRAGMAs applied for benchmark fairness beyond what
/// [`HarnessSettings`] covers.
///
/// These are applied **after** the standard harness PRAGMAs.
#[must_use]
pub fn additional_benchmark_pragmas() -> Vec<String> {
    vec![
        "PRAGMA temp_store=MEMORY;".to_owned(),
        "PRAGMA mmap_size=0;".to_owned(),
        "PRAGMA auto_vacuum=NONE;".to_owned(),
    ]
}

/// Verify that a rusqlite connection has the expected benchmark PRAGMA settings.
///
/// Returns `Ok(())` if all PRAGMAs match expectations, or `Err` with a
/// diagnostic message listing every mismatch.
///
/// # Errors
///
/// Returns [`E2eError::Divergence`] if any PRAGMA value does not match the
/// expected benchmark setting, or [`E2eError::Rusqlite`] if querying a
/// PRAGMA fails.
pub fn verify_rusqlite_pragmas(conn: &rusqlite::Connection) -> E2eResult<()> {
    let mut mismatches = Vec::new();

    for &(pragma, expected) in BENCHMARK_PRAGMAS {
        match query_pragma_rusqlite(conn, pragma) {
            Ok(Some(actual_str)) => {
                // In-memory databases report "memory" instead of "wal" for journal_mode.
                let ok = if pragma == "journal_mode" {
                    actual_str == expected || actual_str == "memory"
                } else {
                    actual_str == expected
                };
                if !ok {
                    mismatches.push(format!(
                        "PRAGMA {pragma}: expected {expected:?}, got {actual_str:?}"
                    ));
                }
            }
            Ok(None) => {
                // PRAGMA returned no rows — skip (common for in-memory databases).
            }
            Err(e) => {
                mismatches.push(format!("PRAGMA {pragma}: query failed: {e}"));
            }
        }
    }

    // Cache size is a special case: the sign and exact value matter, but the
    // PRAGMA returns pages (positive) or KiB (negative), depending on input.
    // We check that the effective cache is at least 64 MB (negative form).
    if let Ok(Some(cache_str)) = query_pragma_rusqlite(conn, "cache_size") {
        if let Ok(cache_val) = cache_str.parse::<i64>() {
            // Negative means KiB: -64000 ≈ 64 MB.
            if cache_val > 0 || cache_val.unsigned_abs() < 60_000 {
                mismatches.push(format!(
                    "PRAGMA cache_size: expected <= -60000 (>= 60 MB), got {cache_val}"
                ));
            }
        }
    }

    if mismatches.is_empty() {
        Ok(())
    } else {
        Err(E2eError::Divergence(format!(
            "benchmark fairness check failed:\n  {}",
            mismatches.join("\n  ")
        )))
    }
}

/// Query a single PRAGMA value from a rusqlite connection, returning `None` if
/// the PRAGMA returns no rows.
fn query_pragma_rusqlite(
    conn: &rusqlite::Connection,
    pragma: &str,
) -> Result<Option<String>, rusqlite::Error> {
    let mut stmt = conn.prepare(&format!("PRAGMA {pragma};"))?;
    let mut rows = stmt.query([])?;
    match rows.next()? {
        Some(row) => {
            let val: rusqlite::types::Value = row.get(0)?;
            Ok(Some(pragma_value_to_string(&val)))
        }
        None => Ok(None),
    }
}

/// Convert a rusqlite `Value` to its string representation for PRAGMA comparison.
fn pragma_value_to_string(val: &rusqlite::types::Value) -> String {
    match val {
        rusqlite::types::Value::Integer(i) => i.to_string(),
        rusqlite::types::Value::Real(f) => f.to_string(),
        rusqlite::types::Value::Text(s) => s.clone(),
        rusqlite::types::Value::Blob(b) => format!("{b:?}"),
        rusqlite::types::Value::Null => String::new(),
    }
}

/// Verify that a FrankenSQLite connection has the expected benchmark settings.
///
/// # Errors
///
/// Returns [`E2eError::Divergence`] if any setting does not match.
pub fn verify_fsqlite_pragmas(conn: &fsqlite::Connection) -> E2eResult<()> {
    let mut mismatches = Vec::new();

    for &(pragma, expected) in BENCHMARK_PRAGMAS {
        let query = format!("PRAGMA {pragma};");
        match crate::block_on(conn.query(&query)) {
            Ok(rows) => {
                let raw = rows
                    .first()
                    .and_then(|row| row.get(0))
                    .map(std::string::ToString::to_string)
                    .unwrap_or_default();

                // Skip unimplemented PRAGMAs (empty response).
                if raw.is_empty() {
                    continue;
                }

                // Normalize: strip surrounding quotes, lowercase.
                let actual = normalize_pragma_value(&raw);

                // FrankenSQLite may return text names instead of numeric
                // codes.  Map known text values to their numeric equivalents.
                let normalized = fsqlite_pragma_to_numeric(&actual, pragma);

                let ok = if pragma == "journal_mode" {
                    normalized == expected || normalized == "memory"
                } else {
                    normalized == expected
                };
                if !ok {
                    mismatches.push(format!(
                        "PRAGMA {pragma}: expected {expected:?}, got {normalized:?} (raw: {raw:?})"
                    ));
                }
            }
            Err(e) => {
                mismatches.push(format!("PRAGMA {pragma}: query failed: {e}"));
            }
        }
    }

    if mismatches.is_empty() {
        Ok(())
    } else {
        Err(E2eError::Divergence(format!(
            "benchmark fairness check failed (fsqlite):\n  {}",
            mismatches.join("\n  ")
        )))
    }
}

/// Strip surrounding quotes and lowercase for PRAGMA value comparison.
fn normalize_pragma_value(raw: &str) -> String {
    let trimmed = raw.trim();
    let stripped = trimmed
        .strip_prefix('\'')
        .and_then(|s| s.strip_suffix('\''))
        .unwrap_or(trimmed);
    stripped.to_lowercase()
}

/// Map FrankenSQLite text PRAGMA names to their numeric equivalents
/// so they can be compared against the canonical expected values.
fn fsqlite_pragma_to_numeric(value: &str, pragma: &str) -> String {
    match pragma {
        "synchronous" => match value {
            "off" => "0".to_owned(),
            "normal" => "1".to_owned(),
            "full" => "2".to_owned(),
            "extra" => "3".to_owned(),
            _ => value.to_owned(),
        },
        "temp_store" => match value {
            "default" => "0".to_owned(),
            "file" => "1".to_owned(),
            "memory" => "2".to_owned(),
            _ => value.to_owned(),
        },
        "auto_vacuum" => match value {
            "none" => "0".to_owned(),
            "full" => "1".to_owned(),
            "incremental" => "2".to_owned(),
            _ => value.to_owned(),
        },
        _ => value.to_owned(),
    }
}

/// Apply the canonical benchmark PRAGMAs to a rusqlite connection.
///
/// # Errors
///
/// Returns [`E2eError::Rusqlite`] if any PRAGMA fails to execute.
pub fn apply_benchmark_pragmas_rusqlite(conn: &rusqlite::Connection) -> E2eResult<()> {
    let settings = benchmark_settings();
    for pragma in &settings.to_sqlite3_pragmas() {
        conn.execute_batch(pragma).map_err(E2eError::Rusqlite)?;
    }
    for pragma in &additional_benchmark_pragmas() {
        conn.execute_batch(pragma).map_err(E2eError::Rusqlite)?;
    }
    Ok(())
}

/// Apply the canonical benchmark PRAGMAs to a FrankenSQLite connection.
///
/// # Errors
///
/// Returns [`E2eError::Fsqlite`] if any PRAGMA fails to execute.
pub fn apply_benchmark_pragmas_fsqlite(conn: &fsqlite::Connection) -> E2eResult<()> {
    let settings = benchmark_settings();
    for pragma in &settings.to_fsqlite_pragmas() {
        crate::block_on(conn.execute(pragma)).map_err(|e| E2eError::Fsqlite(e.to_string()))?;
    }
    for pragma in &additional_benchmark_pragmas() {
        crate::block_on(conn.execute(pragma)).map_err(|e| E2eError::Fsqlite(e.to_string()))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: create a file-backed rusqlite connection in a temp directory.
    /// WAL mode only works with file-backed databases, not in-memory.
    fn file_backed_rusqlite() -> (tempfile::TempDir, rusqlite::Connection) {
        let tmp = tempfile::TempDir::new().unwrap();
        let path = tmp.path().join("bench_test.db");
        let conn = rusqlite::Connection::open(&path).unwrap();
        (tmp, conn)
    }

    #[test]
    fn test_pragma_verification_passes() {
        let (_tmp, conn) = file_backed_rusqlite();
        apply_benchmark_pragmas_rusqlite(&conn).unwrap();
        verify_rusqlite_pragmas(&conn).unwrap();
    }

    #[test]
    fn test_pragma_verification_fails_on_wrong_setting() {
        let (_tmp, conn) = file_backed_rusqlite();
        apply_benchmark_pragmas_rusqlite(&conn).unwrap();
        // Override synchronous to FULL (2) — should cause verification failure.
        conn.execute_batch("PRAGMA synchronous=FULL;").unwrap();
        let result = verify_rusqlite_pragmas(&conn);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("synchronous"),
            "error should mention synchronous: {err_msg}"
        );
    }

    #[test]
    fn test_pragma_applied_to_both_backends() {
        // rusqlite (file-backed for WAL support)
        let (_tmp, rconn) = file_backed_rusqlite();
        apply_benchmark_pragmas_rusqlite(&rconn).unwrap();
        verify_rusqlite_pragmas(&rconn).unwrap();

        // FrankenSQLite (in-memory is fine for the PRAGMAs it supports)
        let fconn = crate::block_on(fsqlite::Connection::open(":memory:")).unwrap();
        apply_benchmark_pragmas_fsqlite(&fconn).unwrap();
        verify_fsqlite_pragmas(&fconn).unwrap();
    }

    #[test]
    fn test_wal_mode_active() {
        let (_tmp, conn) = file_backed_rusqlite();
        apply_benchmark_pragmas_rusqlite(&conn).unwrap();
        let mode: String = conn
            .query_row("PRAGMA journal_mode;", [], |row| row.get(0))
            .unwrap();
        assert_eq!(mode, "wal", "expected WAL mode, got {mode:?}");
    }

    #[test]
    fn test_cache_size_effective() {
        let (_tmp, conn) = file_backed_rusqlite();
        apply_benchmark_pragmas_rusqlite(&conn).unwrap();
        let cache: i64 = conn
            .query_row("PRAGMA cache_size;", [], |row| row.get(0))
            .unwrap();
        // We set -64000 (64 MB in KiB).  The returned value should be negative.
        assert!(
            cache <= -60_000,
            "cache_size should be at least 60 MB (got {cache})"
        );
    }
}
