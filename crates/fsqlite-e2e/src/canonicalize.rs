//! Database canonicalization pipeline for deterministic SHA-256 hashing.
//!
//! Beads: bd-1w6k.5.2, bd-1opl
//!
//! Produces a canonical database file whose SHA-256 is stable across repeated
//! runs for identical logical content.  The pipeline:
//!
//! 1. Checkpoint the WAL (`PRAGMA wal_checkpoint(TRUNCATE)`)
//! 2. Normalize PRAGMAs (`page_size`, `auto_vacuum = NONE`)
//! 3. `VACUUM INTO <canonical_path>` to produce a defragmented, single-file copy
//! 4. SHA-256 hash the canonical file
//!
//! ## Layered Comparison (bd-1opl, bd-sfzqn)
//!
//! For cross-engine comparison, independent evidence is recorded for:
//!
//! - raw database-file SHA-256;
//! - canonical `VACUUM INTO` SHA-256;
//! - type-preserving row-level equality;
//! - weaker row-count, spot-check, and integrity evidence.

use std::cmp::Ordering;
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

use crate::{E2eError, E2eResult};

/// Result of canonicalizing a database file.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CanonicalResult {
    /// Path to the canonical output file.
    pub canonical_path: PathBuf,
    /// SHA-256 hex digest of the canonical file.
    pub sha256: String,
    /// Size of the canonical file in bytes.
    pub size_bytes: u64,
}

/// Canonicalize a `SQLite` database file for deterministic hashing.
///
/// The source database is opened read-only (via rusqlite), its WAL is
/// checkpointed, and the result is `VACUUM INTO` a new file at `output_path`.
/// The output file's SHA-256 is then computed and returned.
///
/// Fixed PRAGMAs applied before `VACUUM INTO`:
/// - `page_size = 4096` (the `SQLite` default, ensuring layout stability)
/// - `auto_vacuum = 0` (OFF — avoids non-deterministic page relocation)
///
/// # Errors
///
/// Returns `E2eError::Rusqlite` for database errors, `E2eError::Io` for
/// filesystem errors.
///
/// # Safety / immutability
///
/// The source database is opened with `SQLITE_OPEN_READ_ONLY`.
/// WAL checkpointing is best-effort (may silently fail on a read-only handle).
/// **Never pass a golden database path directly** — always operate on a working
/// copy.
pub fn canonicalize(source: &Path, output_path: &Path) -> E2eResult<CanonicalResult> {
    let flags =
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX;
    let conn = rusqlite::Connection::open_with_flags(source, flags)?;

    // Checkpoint the WAL to fold all WAL frames back into the main database.
    // TRUNCATE mode also removes the WAL file afterward.  Best-effort: may
    // fail on a read-only connection, which is acceptable.
    let _ = conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);");

    // Fixed PRAGMAs for deterministic output.
    conn.execute_batch("PRAGMA page_size = 4096;")?;
    conn.execute_batch("PRAGMA auto_vacuum = 0;")?;

    // Remove dest if it exists so VACUUM INTO doesn't fail.
    if output_path.exists() {
        std::fs::remove_file(output_path)?;
    }

    // VACUUM INTO creates a fresh, defragmented database file at output_path.
    // The resulting file has:
    //   - No freelist pages
    //   - Contiguous page allocation
    //   - Deterministic page layout for the same logical content
    let output_str = output_path
        .to_str()
        .ok_or_else(|| E2eError::Io(std::io::Error::other("output path is not valid UTF-8")))?;

    conn.execute("VACUUM INTO ?1", [output_str])?;
    drop(conn);

    // Compute SHA-256 of the canonical file.
    let canonical_bytes = std::fs::read(output_path)?;
    let sha256 = sha256_hex(&canonical_bytes);
    let size_bytes = u64::try_from(canonical_bytes.len()).unwrap_or(0);

    Ok(CanonicalResult {
        canonical_path: output_path.to_path_buf(),
        sha256,
        size_bytes,
    })
}

/// Canonicalize a database and return only the SHA-256 hash.
///
/// Convenience wrapper that creates a temporary canonical file, hashes it,
/// and cleans up.
///
/// # Errors
///
/// Returns errors from [`canonicalize`].
pub fn canonical_sha256(source: &Path) -> E2eResult<String> {
    let tmp_dir = tempfile::TempDir::new()?;
    let output = tmp_dir.path().join("canonical.db");
    let result = canonicalize(source, &output)?;
    Ok(result.sha256)
}

/// Compare two databases by canonicalizing both and comparing SHA-256 hashes.
///
/// Returns `(sha256_a, sha256_b, matched)`.
///
/// # Errors
///
/// Returns errors from [`canonicalize`].
pub fn compare_canonical(db_a: &Path, db_b: &Path) -> E2eResult<(String, String, bool)> {
    let tmp_dir = tempfile::TempDir::new()?;
    let out_a = tmp_dir.path().join("canonical_a.db");
    let out_b = tmp_dir.path().join("canonical_b.db");

    let result_a = canonicalize(db_a, &out_a)?;
    let result_b = canonicalize(db_b, &out_b)?;

    let matched = result_a.sha256 == result_b.sha256;
    Ok((result_a.sha256, result_b.sha256, matched))
}

/// Compute SHA-256 hex digest of arbitrary bytes.
fn sha256_hex(data: &[u8]) -> String {
    use std::fmt::Write as _;
    let digest = Sha256::digest(data);
    let mut hex = String::with_capacity(64);
    for byte in digest {
        let _ = write!(hex, "{byte:02x}");
    }
    hex
}

// ─── Three-Tier Comparison (bd-1opl) ─────────────────────────────────────

/// Which comparison tier produced the result.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ComparisonTier {
    /// Original standalone database files have identical SHA-256 digests.
    RawIdentical,
    /// SHA-256 of canonical `VACUUM INTO` output matches byte-for-byte.
    CanonicalMatch,
    /// Row-level logical comparison matches across all tables.
    LogicalMatch,
    /// Row counts and spot-check rows match; weaker than logical equality.
    DataComplete,
}

impl std::fmt::Display for ComparisonTier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RawIdentical => write!(f, "Raw Identical (standalone file SHA-256)"),
            Self::CanonicalMatch => write!(f, "Canonical Match (VACUUM INTO SHA-256)"),
            Self::LogicalMatch => write!(f, "Logical Match (typed row-level)"),
            Self::DataComplete => write!(f, "Data Complete (counts + spot checks)"),
        }
    }
}

/// Result of a three-tier cross-engine database comparison.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TieredComparisonResult {
    /// The highest tier that matched.
    pub tier: ComparisonTier,
    /// SHA-256 of the first raw database file.
    pub raw_sha256_a: Option<String>,
    /// SHA-256 of the second raw database file.
    pub raw_sha256_b: Option<String>,
    /// Whether the original database files matched byte-for-byte.
    pub raw_match: Option<bool>,
    /// SHA-256 of the first canonical `VACUUM INTO` database, when attempted.
    pub canonical_sha256_a: Option<String>,
    /// SHA-256 of the second canonical `VACUUM INTO` database, when attempted.
    pub canonical_sha256_b: Option<String>,
    /// Whether canonical database files matched.
    pub canonical_match: bool,
    /// Whether the type-preserving logical row comparison matched.
    pub logical_match: bool,
    /// Whether weaker row-count, spot-check, and integrity evidence matched.
    pub data_complete: bool,
    /// Human-readable description of how the result was determined.
    pub detail: String,
}

/// Compare two on-disk databases using independent, layered evidence.
///
/// Raw file hashes are always recorded. The comparator then attempts canonical
/// `VACUUM INTO` hashes, type-preserving logical rows, and finally weaker data
/// completeness checks. A mismatch at one layer continues to the next so the
/// result describes the strongest authority that actually matched.
///
/// Both paths are opened via rusqlite (read-only) so this works for any
/// SQLite-compatible database file, regardless of which engine produced it.
///
/// # Errors
///
/// Returns `E2eError` on I/O or database errors that prevent even the final
/// data-completeness comparison.
pub fn canonicalize_and_compare(db_a: &Path, db_b: &Path) -> E2eResult<TieredComparisonResult> {
    let raw_sha256_a = file_sha256(db_a)?;
    let raw_sha256_b = file_sha256(db_b)?;
    let raw_match = if raw_file_is_standalone(db_a)? && raw_file_is_standalone(db_b)? {
        Some(raw_sha256_a == raw_sha256_b)
    } else {
        None
    };

    // --- Canonical VACUUM INTO + SHA-256 ---
    let canonical_attempt = match try_tier1(db_a, db_b) {
        Ok(result) if result.canonical_match => {
            return Ok(with_raw_evidence(
                result,
                raw_sha256_a,
                raw_sha256_b,
                raw_match,
            ));
        }
        Ok(result) => Some(result),
        Err(e) => {
            tracing::info!(error = %e, "canonical comparison failed, falling back to logical comparison");
            None
        }
    };

    // --- Type-preserving logical row-level comparison ---
    let logical_attempt = match try_tier2(db_a, db_b) {
        Ok(mut result) => {
            carry_canonical_evidence(&mut result, canonical_attempt.as_ref());
            if result.logical_match {
                return Ok(with_raw_evidence(
                    result,
                    raw_sha256_a,
                    raw_sha256_b,
                    raw_match,
                ));
            }
            Some(result)
        }
        Err(e) => {
            tracing::info!(error = %e, "logical comparison failed, falling back to data-completeness checks");
            None
        }
    };

    // --- Weaker data-completeness evidence ---
    let mut result = try_tier3(db_a, db_b)?;
    carry_canonical_evidence(&mut result, canonical_attempt.as_ref());
    if let Some(logical) = logical_attempt {
        result.logical_match = logical.logical_match;
        result.detail = format!("{}; {}", logical.detail, result.detail);
    }
    Ok(with_raw_evidence(
        result,
        raw_sha256_a,
        raw_sha256_b,
        raw_match,
    ))
}

fn file_sha256(path: &Path) -> E2eResult<String> {
    use std::io::Read as _;

    let mut file = std::fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0_u8; 64 * 1024].into_boxed_slice();
    loop {
        let bytes_read = file.read(&mut buffer)?;
        if bytes_read == 0 {
            break;
        }
        hasher.update(&buffer[..bytes_read]);
    }

    use std::fmt::Write as _;
    let mut hex = String::with_capacity(64);
    for byte in hasher.finalize() {
        let _ = write!(hex, "{byte:02x}");
    }
    Ok(hex)
}

fn raw_file_is_standalone(path: &Path) -> E2eResult<bool> {
    for suffix in ["-wal", "-journal"] {
        let mut sidecar = path.as_os_str().to_owned();
        sidecar.push(suffix);
        match std::fs::metadata(PathBuf::from(sidecar)) {
            Ok(metadata) if metadata.len() > 0 => return Ok(false),
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(true)
}

fn with_raw_evidence(
    mut result: TieredComparisonResult,
    raw_sha256_a: String,
    raw_sha256_b: String,
    raw_match: Option<bool>,
) -> TieredComparisonResult {
    result.raw_match = raw_match;
    result.raw_sha256_a = Some(raw_sha256_a);
    result.raw_sha256_b = Some(raw_sha256_b);
    if raw_match == Some(true) {
        result.tier = ComparisonTier::RawIdentical;
        result.detail = format!("Raw PASS: standalone file SHA-256 match; {}", result.detail);
    }
    result
}

fn carry_canonical_evidence(
    result: &mut TieredComparisonResult,
    canonical_attempt: Option<&TieredComparisonResult>,
) {
    if let Some(canonical) = canonical_attempt {
        result
            .canonical_sha256_a
            .clone_from(&canonical.canonical_sha256_a);
        result
            .canonical_sha256_b
            .clone_from(&canonical.canonical_sha256_b);
        result.canonical_match = canonical.canonical_match;
    }
}

/// Canonical layer: VACUUM INTO both databases, compare SHA-256 hashes.
fn try_tier1(db_a: &Path, db_b: &Path) -> E2eResult<TieredComparisonResult> {
    let tmp_dir = tempfile::TempDir::new()?;
    let out_a = tmp_dir.path().join("canonical_a.db");
    let out_b = tmp_dir.path().join("canonical_b.db");

    let result_a = canonicalize(db_a, &out_a)?;
    let result_b = canonicalize(db_b, &out_b)?;

    let canonical_match = result_a.sha256 == result_b.sha256;

    Ok(TieredComparisonResult {
        tier: ComparisonTier::CanonicalMatch,
        raw_sha256_a: None,
        raw_sha256_b: None,
        raw_match: None,
        canonical_sha256_a: Some(result_a.sha256.clone()),
        canonical_sha256_b: Some(result_b.sha256.clone()),
        canonical_match,
        logical_match: false,
        data_complete: false,
        detail: if canonical_match {
            format!("Canonical PASS: SHA-256 match ({})", &result_a.sha256[..16])
        } else {
            format!(
                "Canonical FAIL: SHA-256 mismatch (a={}, b={})",
                &result_a.sha256[..16],
                &result_b.sha256[..16]
            )
        },
    })
}

/// Open a database read-only via rusqlite.
fn open_readonly(path: &Path) -> E2eResult<rusqlite::Connection> {
    let flags =
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX;
    Ok(rusqlite::Connection::open_with_flags(path, flags)?)
}

/// List user tables (excluding `sqlite_*` internal tables), sorted by name.
fn list_user_tables(conn: &rusqlite::Connection) -> E2eResult<Vec<String>> {
    let mut stmt = conn.prepare(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
    )?;
    let names: Vec<String> = stmt
        .query_map([], |row| row.get(0))?
        .collect::<Result<Vec<String>, _>>()?;
    Ok(names)
}

/// Get the full user-defined schema catalog in deterministic order.
fn schema_sql(conn: &rusqlite::Connection) -> E2eResult<Vec<(String, String, String, String)>> {
    let mut stmt = conn.prepare(
        "SELECT type, name, tbl_name, sql
         FROM sqlite_master
         WHERE name NOT LIKE 'sqlite_%' AND sql IS NOT NULL
         ORDER BY type, name",
    )?;
    let rows = stmt
        .query_map([], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows)
}

fn quoted_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

/// Get row count for a table.
fn row_count(conn: &rusqlite::Connection, table: &str) -> E2eResult<i64> {
    let table = quoted_identifier(table);
    let count: i64 = conn.query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))?;
    Ok(count)
}

fn storage_class_rank(value: &rusqlite::types::Value) -> u8 {
    match value {
        rusqlite::types::Value::Null => 0,
        rusqlite::types::Value::Integer(_) => 1,
        rusqlite::types::Value::Real(_) => 2,
        rusqlite::types::Value::Text(_) => 3,
        rusqlite::types::Value::Blob(_) => 4,
    }
}

fn compare_values(left: &rusqlite::types::Value, right: &rusqlite::types::Value) -> Ordering {
    match (left, right) {
        (rusqlite::types::Value::Null, rusqlite::types::Value::Null) => Ordering::Equal,
        (rusqlite::types::Value::Integer(left), rusqlite::types::Value::Integer(right)) => {
            left.cmp(right)
        }
        (rusqlite::types::Value::Real(left), rusqlite::types::Value::Real(right)) => {
            left.total_cmp(right)
        }
        (rusqlite::types::Value::Text(left), rusqlite::types::Value::Text(right)) => {
            left.cmp(right)
        }
        (rusqlite::types::Value::Blob(left), rusqlite::types::Value::Blob(right)) => {
            left.cmp(right)
        }
        _ => storage_class_rank(left).cmp(&storage_class_rank(right)),
    }
}

fn compare_rows(left: &[rusqlite::types::Value], right: &[rusqlite::types::Value]) -> Ordering {
    left.iter()
        .zip(right)
        .map(|(left, right)| compare_values(left, right))
        .find(|ordering| *ordering != Ordering::Equal)
        .unwrap_or_else(|| left.len().cmp(&right.len()))
}

/// Fetch every row and sort the typed values independently of physical rowids.
fn fetch_all_rows_sorted(
    conn: &rusqlite::Connection,
    table: &str,
) -> E2eResult<Vec<Vec<rusqlite::types::Value>>> {
    let mut stmt = conn.prepare(&format!("SELECT * FROM {}", quoted_identifier(table)))?;
    let col_count = stmt.column_count();
    let mut rows: Vec<Vec<rusqlite::types::Value>> = stmt
        .query_map([], |row| {
            let mut vals = Vec::with_capacity(col_count);
            for i in 0..col_count {
                vals.push(row.get(i)?);
            }
            Ok(vals)
        })?
        .collect::<Result<Vec<_>, _>>()?;
    rows.sort_by(|left, right| compare_rows(left, right));
    Ok(rows)
}

/// Logical layer: type-preserving row comparison across all tables.
fn try_tier2(db_a: &Path, db_b: &Path) -> E2eResult<TieredComparisonResult> {
    let conn_a = open_readonly(db_a)?;
    let conn_b = open_readonly(db_b)?;

    // Compare schemas first.
    let schema_a = schema_sql(&conn_a)?;
    let schema_b = schema_sql(&conn_b)?;

    if schema_a != schema_b {
        return Ok(TieredComparisonResult {
            tier: ComparisonTier::LogicalMatch,
            raw_sha256_a: None,
            raw_sha256_b: None,
            raw_match: None,
            canonical_sha256_a: None,
            canonical_sha256_b: None,
            canonical_match: false,
            logical_match: false,
            data_complete: false,
            detail: "Logical FAIL: schema mismatch".to_owned(),
        });
    }

    let tables = list_user_tables(&conn_a)?;

    // Compare every row in every table.
    for table in &tables {
        let rows_a = fetch_all_rows_sorted(&conn_a, table)?;
        let rows_b = fetch_all_rows_sorted(&conn_b, table)?;

        if rows_a != rows_b {
            return Ok(TieredComparisonResult {
                tier: ComparisonTier::LogicalMatch,
                raw_sha256_a: None,
                raw_sha256_b: None,
                raw_match: None,
                canonical_sha256_a: None,
                canonical_sha256_b: None,
                canonical_match: false,
                logical_match: false,
                data_complete: false,
                detail: format!(
                    "Logical FAIL: row mismatch in table \"{table}\" (a={} rows, b={} rows)",
                    rows_a.len(),
                    rows_b.len()
                ),
            });
        }
    }

    Ok(TieredComparisonResult {
        tier: ComparisonTier::LogicalMatch,
        raw_sha256_a: None,
        raw_sha256_b: None,
        raw_match: None,
        canonical_sha256_a: None,
        canonical_sha256_b: None,
        canonical_match: false,
        logical_match: true,
        data_complete: false,
        detail: format!(
            "Logical PASS: all {} table(s) match row-by-row",
            tables.len()
        ),
    })
}

/// Data-completeness layer: row counts, spot checks, and integrity checks.
fn try_tier3(db_a: &Path, db_b: &Path) -> E2eResult<TieredComparisonResult> {
    let conn_a = open_readonly(db_a)?;
    let conn_b = open_readonly(db_b)?;

    let tables_a = list_user_tables(&conn_a)?;
    let tables_b = list_user_tables(&conn_b)?;

    // Table list must match.
    if tables_a != tables_b {
        return Ok(TieredComparisonResult {
            tier: ComparisonTier::DataComplete,
            raw_sha256_a: None,
            raw_sha256_b: None,
            raw_match: None,
            canonical_sha256_a: None,
            canonical_sha256_b: None,
            canonical_match: false,
            logical_match: false,
            data_complete: false,
            detail: format!("Data FAIL: table list mismatch (a={tables_a:?}, b={tables_b:?})"),
        });
    }

    // Check row counts for each table.
    let mut all_counts_match = true;
    let mut detail_parts = Vec::new();

    for table in &tables_a {
        let count_a = row_count(&conn_a, table)?;
        let count_b = row_count(&conn_b, table)?;

        if count_a != count_b {
            all_counts_match = false;
            detail_parts.push(format!(
                "\"{table}\": count mismatch (a={count_a}, b={count_b})"
            ));
        }
    }

    if !all_counts_match {
        return Ok(TieredComparisonResult {
            tier: ComparisonTier::DataComplete,
            raw_sha256_a: None,
            raw_sha256_b: None,
            raw_match: None,
            canonical_sha256_a: None,
            canonical_sha256_b: None,
            canonical_match: false,
            logical_match: false,
            data_complete: false,
            detail: format!("Data FAIL: {}", detail_parts.join("; ")),
        });
    }

    // Spot checks: first 10 and last 10 rows of each table.
    let mut spot_checks_pass = true;
    for table in &tables_a {
        let count = row_count(&conn_a, table)?;
        if count == 0 {
            continue;
        }

        // First 10 rows.
        let first_a = spot_check_rows(&conn_a, table, "ASC", 10)?;
        let first_b = spot_check_rows(&conn_b, table, "ASC", 10)?;
        if first_a != first_b {
            spot_checks_pass = false;
            detail_parts.push(format!("\"{table}\": first-10 spot check mismatch"));
        }

        // Last 10 rows.
        let last_a = spot_check_rows(&conn_a, table, "DESC", 10)?;
        let last_b = spot_check_rows(&conn_b, table, "DESC", 10)?;
        if last_a != last_b {
            spot_checks_pass = false;
            detail_parts.push(format!("\"{table}\": last-10 spot check mismatch"));
        }
    }

    // Integrity check on both.
    let integrity_a = run_integrity_check(&conn_a);
    let integrity_b = run_integrity_check(&conn_b);

    let integrity_ok = integrity_a && integrity_b;
    if !integrity_ok {
        detail_parts.push(format!(
            "integrity_check: a={}, b={}",
            if integrity_a { "ok" } else { "FAIL" },
            if integrity_b { "ok" } else { "FAIL" },
        ));
    }

    let data_complete = all_counts_match && spot_checks_pass && integrity_ok;

    Ok(TieredComparisonResult {
        tier: ComparisonTier::DataComplete,
        raw_sha256_a: None,
        raw_sha256_b: None,
        raw_match: None,
        canonical_sha256_a: None,
        canonical_sha256_b: None,
        canonical_match: false,
        logical_match: false,
        data_complete,
        detail: if data_complete {
            format!(
                "Data PASS: {} table(s), counts match, spot checks pass, integrity ok",
                tables_a.len()
            )
        } else {
            format!("Data FAIL: {}", detail_parts.join("; "))
        },
    })
}

/// Fetch a limited number of rows for spot-check comparison.
fn spot_check_rows(
    conn: &rusqlite::Connection,
    table: &str,
    order: &str,
    limit: usize,
) -> E2eResult<Vec<Vec<rusqlite::types::Value>>> {
    let table = quoted_identifier(table);
    let col_count = conn
        .prepare(&format!("SELECT * FROM {table}"))?
        .column_count();
    let ordering = (1..=col_count)
        .map(|position| format!("{position} {order}"))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!("SELECT * FROM {table} ORDER BY {ordering} LIMIT {limit}");

    let mut stmt = conn.prepare(&sql)?;
    let rows: Vec<Vec<rusqlite::types::Value>> = stmt
        .query_map([], |row| {
            let mut vals = Vec::with_capacity(col_count);
            for i in 0..col_count {
                vals.push(row.get(i)?);
            }
            Ok(vals)
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows)
}

/// Run `PRAGMA integrity_check` and return whether it passes.
fn run_integrity_check(conn: &rusqlite::Connection) -> bool {
    conn.query_row("PRAGMA integrity_check", [], |row| row.get::<_, String>(0))
        .is_ok_and(|result| result == "ok")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonicalize_produces_stable_hash() {
        let tmp = tempfile::TempDir::new().unwrap();
        let db_path = tmp.path().join("test.db");

        // Create a database with some data.
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.execute_batch(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);
             INSERT INTO t VALUES (1, 'hello');
             INSERT INTO t VALUES (2, 'world');",
        )
        .unwrap();
        drop(conn);

        // Canonicalize twice — hashes must be identical.
        let out1 = tmp.path().join("canon1.db");
        let out2 = tmp.path().join("canon2.db");

        let r1 = canonicalize(&db_path, &out1).unwrap();
        let r2 = canonicalize(&db_path, &out2).unwrap();

        assert_eq!(r1.sha256, r2.sha256, "canonical hashes should be stable");
        assert!(!r1.sha256.is_empty());
        assert!(r1.size_bytes > 0);
    }

    #[test]
    fn different_data_produces_different_hash() {
        let tmp = tempfile::TempDir::new().unwrap();

        let db_a = tmp.path().join("a.db");
        let db_b = tmp.path().join("b.db");

        let conn_a = rusqlite::Connection::open(&db_a).unwrap();
        conn_a
            .execute_batch(
                "CREATE TABLE t (id INTEGER PRIMARY KEY);
                 INSERT INTO t VALUES (1);",
            )
            .unwrap();
        drop(conn_a);

        let conn_b = rusqlite::Connection::open(&db_b).unwrap();
        conn_b
            .execute_batch(
                "CREATE TABLE t (id INTEGER PRIMARY KEY);
                 INSERT INTO t VALUES (1);
                 INSERT INTO t VALUES (2);",
            )
            .unwrap();
        drop(conn_b);

        let (sha_a, sha_b, matched) = compare_canonical(&db_a, &db_b).unwrap();
        assert!(!matched, "different data should have different hashes");
        assert_ne!(sha_a, sha_b);
    }

    #[test]
    fn same_data_different_insertion_order_produces_same_hash() {
        let tmp = tempfile::TempDir::new().unwrap();

        let db_a = tmp.path().join("a.db");
        let db_b = tmp.path().join("b.db");

        // Insert in order 1,2,3
        let conn_a = rusqlite::Connection::open(&db_a).unwrap();
        conn_a
            .execute_batch(
                "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);
                 INSERT INTO t VALUES (1, 'a');
                 INSERT INTO t VALUES (2, 'b');
                 INSERT INTO t VALUES (3, 'c');",
            )
            .unwrap();
        drop(conn_a);

        // Insert in order 3,1,2
        let conn_b = rusqlite::Connection::open(&db_b).unwrap();
        conn_b
            .execute_batch(
                "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);
                 INSERT INTO t VALUES (3, 'c');
                 INSERT INTO t VALUES (1, 'a');
                 INSERT INTO t VALUES (2, 'b');",
            )
            .unwrap();
        drop(conn_b);

        let (sha_a, sha_b, matched) = compare_canonical(&db_a, &db_b).unwrap();
        assert!(
            matched,
            "same logical data should produce same canonical hash\n  a={sha_a}\n  b={sha_b}"
        );
    }

    #[test]
    fn canonical_sha256_convenience_works() {
        let tmp = tempfile::TempDir::new().unwrap();
        let db_path = tmp.path().join("test.db");

        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.execute_batch(
            "CREATE TABLE t (id INTEGER PRIMARY KEY);
             INSERT INTO t VALUES (1);",
        )
        .unwrap();
        drop(conn);

        let hash = canonical_sha256(&db_path).unwrap();
        assert_eq!(hash.len(), 64, "SHA-256 hex should be 64 chars");

        // Running again should give the same result.
        let hash2 = canonical_sha256(&db_path).unwrap();
        assert_eq!(hash, hash2);
    }

    #[test]
    fn canonicalize_accepts_quote_in_output_path() {
        let tmp = tempfile::TempDir::new().unwrap();
        let source = tmp.path().join("source.db");
        let output = tmp.path().join("canonical'copy.db");
        create_db(
            &source,
            "CREATE TABLE t(v TEXT); INSERT INTO t VALUES ('ok');",
        );

        let result = canonicalize(&source, &output).unwrap();
        assert_eq!(result.canonical_path, output);
        assert!(output.is_file());
    }

    #[test]
    fn canonicalize_handles_wal_mode() {
        let tmp = tempfile::TempDir::new().unwrap();
        let db_path = tmp.path().join("wal_test.db");

        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
        conn.execute_batch(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);
             INSERT INTO t VALUES (1, 'hello');
             INSERT INTO t VALUES (2, 'world');",
        )
        .unwrap();
        // Leave connection open so WAL is active.
        drop(conn);

        let out = tmp.path().join("canon.db");
        let result = canonicalize(&db_path, &out).unwrap();
        assert!(!result.sha256.is_empty());
        assert!(result.size_bytes > 0);

        // The WAL should have been checkpointed.
        let wal_path = db_path.with_extension("db-wal");
        if wal_path.exists() {
            let wal_size = std::fs::metadata(&wal_path).unwrap().len();
            // After TRUNCATE checkpoint, WAL should be 0 bytes or removed.
            assert_eq!(wal_size, 0, "WAL should be truncated after checkpoint");
        }
    }

    // ─── Layered Comparison Tests (bd-1opl, bd-sfzqn) ───────────────────

    /// Helper: create a database at `path` and run `sql` inside it.
    fn create_db(path: &Path, sql: &str) {
        let conn = rusqlite::Connection::open(path).unwrap();
        conn.execute_batch(sql).unwrap();
        drop(conn);
    }

    #[test]
    fn test_identical_databases_preserve_raw_and_canonical_evidence() {
        let tmp = tempfile::TempDir::new().unwrap();
        let db_a = tmp.path().join("a.db");
        let db_b = tmp.path().join("b.db");

        let sql = "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);
                    INSERT INTO t VALUES (1, 'hello');
                    INSERT INTO t VALUES (2, 'world');
                    INSERT INTO t VALUES (3, 'test');";
        create_db(&db_a, sql);
        create_db(&db_b, sql);

        let result = canonicalize_and_compare(&db_a, &db_b).unwrap();
        assert_eq!(result.tier, ComparisonTier::RawIdentical);
        assert_eq!(result.raw_match, Some(true));
        assert!(result.canonical_match);
        assert!(result.raw_sha256_a.is_some());
        assert_eq!(result.raw_sha256_a, result.raw_sha256_b);
        assert_eq!(result.canonical_sha256_a, result.canonical_sha256_b);
    }

    #[test]
    fn test_different_insert_order_tier1_match() {
        let tmp = tempfile::TempDir::new().unwrap();
        let db_a = tmp.path().join("a.db");
        let db_b = tmp.path().join("b.db");

        create_db(
            &db_a,
            "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);
             INSERT INTO t VALUES (1, 'a');
             INSERT INTO t VALUES (2, 'b');
             INSERT INTO t VALUES (3, 'c');",
        );
        create_db(
            &db_b,
            "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);
             INSERT INTO t VALUES (3, 'c');
             INSERT INTO t VALUES (1, 'a');
             INSERT INTO t VALUES (2, 'b');",
        );

        let result = canonicalize_and_compare(&db_a, &db_b).unwrap();
        assert_eq!(result.tier, ComparisonTier::CanonicalMatch);
        assert!(result.canonical_match);
    }

    #[test]
    fn canonical_match_does_not_claim_raw_file_equality() {
        let tmp = tempfile::TempDir::new().unwrap();
        let db_a = tmp.path().join("page-1024.db");
        let db_b = tmp.path().join("page-4096.db");

        create_db(
            &db_a,
            "PRAGMA page_size=1024;
             CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);
             INSERT INTO t VALUES (1, 'same');",
        );
        create_db(
            &db_b,
            "PRAGMA page_size=4096;
             CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);
             INSERT INTO t VALUES (1, 'same');",
        );

        let result = canonicalize_and_compare(&db_a, &db_b).unwrap();
        assert_eq!(result.raw_match, Some(false));
        assert!(result.canonical_match);
        assert_ne!(result.raw_sha256_a, result.raw_sha256_b);
        assert_eq!(result.canonical_sha256_a, result.canonical_sha256_b);
    }

    #[test]
    fn nonempty_journal_sidecar_makes_raw_authority_unavailable() {
        let tmp = tempfile::TempDir::new().unwrap();
        let db = tmp.path().join("sidecar.db");
        create_db(&db, "CREATE TABLE t(id INTEGER PRIMARY KEY);");
        assert!(raw_file_is_standalone(&db).unwrap());

        let mut journal = db.as_os_str().to_owned();
        journal.push("-journal");
        std::fs::write(PathBuf::from(journal), [1_u8]).unwrap();
        assert!(!raw_file_is_standalone(&db).unwrap());
    }

    #[test]
    fn test_with_deletes_tier1_match() {
        let tmp = tempfile::TempDir::new().unwrap();
        let db_a = tmp.path().join("a.db");
        let db_b = tmp.path().join("b.db");

        // Both insert 1..5 then delete 2,4 — same logical content.
        let sql = "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);
                    INSERT INTO t VALUES (1, 'a');
                    INSERT INTO t VALUES (2, 'b');
                    INSERT INTO t VALUES (3, 'c');
                    INSERT INTO t VALUES (4, 'd');
                    INSERT INTO t VALUES (5, 'e');
                    DELETE FROM t WHERE id IN (2, 4);";
        create_db(&db_a, sql);
        create_db(&db_b, sql);

        let result = canonicalize_and_compare(&db_a, &db_b).unwrap();
        assert_eq!(result.tier, ComparisonTier::RawIdentical);
        assert!(result.canonical_match);
    }

    #[test]
    fn test_tier2_fallback() {
        // Tier 2 (logical match) is tested by directly calling try_tier2
        // on databases with identical logical content.
        let tmp = tempfile::TempDir::new().unwrap();
        let db_a = tmp.path().join("a.db");
        let db_b = tmp.path().join("b.db");

        create_db(
            &db_a,
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER);
             INSERT INTO users VALUES (1, 'Alice', 30);
             INSERT INTO users VALUES (2, 'Bob', 25);",
        );
        create_db(
            &db_b,
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER);
             INSERT INTO users VALUES (2, 'Bob', 25);
             INSERT INTO users VALUES (1, 'Alice', 30);",
        );

        let result = try_tier2(&db_a, &db_b).unwrap();
        assert_eq!(result.tier, ComparisonTier::LogicalMatch);
        assert!(result.logical_match);
        assert!(!result.data_complete);
        assert!(result.detail.contains("PASS"));
    }

    #[test]
    fn logical_comparison_preserves_sqlite_storage_classes() {
        let tmp = tempfile::TempDir::new().unwrap();
        let db_a = tmp.path().join("null.db");
        let db_b = tmp.path().join("text.db");

        create_db(&db_a, "CREATE TABLE t(v); INSERT INTO t VALUES (NULL);");
        create_db(&db_b, "CREATE TABLE t(v); INSERT INTO t VALUES ('NULL');");

        let result = try_tier2(&db_a, &db_b).unwrap();
        assert!(!result.logical_match, "NULL must not equal text 'NULL'");
        assert!(result.detail.contains("row mismatch"));
    }

    #[test]
    fn logical_comparison_ignores_physical_rowid_order() {
        let tmp = tempfile::TempDir::new().unwrap();
        let db_a = tmp.path().join("forward.db");
        let db_b = tmp.path().join("reverse.db");

        create_db(
            &db_a,
            "CREATE TABLE t(v TEXT); INSERT INTO t VALUES ('a'), ('b');",
        );
        create_db(
            &db_b,
            "CREATE TABLE t(v TEXT); INSERT INTO t VALUES ('b'), ('a');",
        );

        let result = try_tier2(&db_a, &db_b).unwrap();
        assert!(result.logical_match);
    }

    #[test]
    fn logical_comparison_checks_non_table_schema_objects() {
        let tmp = tempfile::TempDir::new().unwrap();
        let db_a = tmp.path().join("indexed.db");
        let db_b = tmp.path().join("plain.db");

        create_db(&db_a, "CREATE TABLE t(v TEXT); CREATE INDEX t_v ON t(v);");
        create_db(&db_b, "CREATE TABLE t(v TEXT);");

        let result = try_tier2(&db_a, &db_b).unwrap();
        assert!(!result.logical_match);
        assert!(result.detail.contains("schema mismatch"));
    }

    #[test]
    fn logical_comparison_quotes_table_identifiers() {
        let tmp = tempfile::TempDir::new().unwrap();
        let db_a = tmp.path().join("quoted-a.db");
        let db_b = tmp.path().join("quoted-b.db");
        let sql = "CREATE TABLE \"odd\"\"name\"(v TEXT); \
                   INSERT INTO \"odd\"\"name\" VALUES ('ok');";
        create_db(&db_a, sql);
        create_db(&db_b, sql);

        let result = try_tier2(&db_a, &db_b).unwrap();
        assert!(result.logical_match);
    }

    #[test]
    fn test_tier3_fallback() {
        // Tier 3 verifies row counts and spot checks even when
        // full row comparison might not be available.
        let tmp = tempfile::TempDir::new().unwrap();
        let db_a = tmp.path().join("a.db");
        let db_b = tmp.path().join("b.db");

        create_db(
            &db_a,
            "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);
             INSERT INTO t VALUES (1, 'hello');
             INSERT INTO t VALUES (2, 'world');",
        );
        create_db(
            &db_b,
            "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);
             INSERT INTO t VALUES (1, 'hello');
             INSERT INTO t VALUES (2, 'world');",
        );

        let result = try_tier3(&db_a, &db_b).unwrap();
        assert_eq!(result.tier, ComparisonTier::DataComplete);
        assert!(result.data_complete);
        assert!(result.detail.contains("PASS"));

        // Now test a mismatch: different row counts.
        let db_c = tmp.path().join("c.db");
        create_db(
            &db_c,
            "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);
             INSERT INTO t VALUES (1, 'hello');",
        );

        let mismatch = try_tier3(&db_a, &db_c).unwrap();
        assert!(!mismatch.data_complete);
        assert!(mismatch.detail.contains("FAIL"));
    }

    #[test]
    fn test_empty_database_match() {
        let tmp = tempfile::TempDir::new().unwrap();
        let db_a = tmp.path().join("a.db");
        let db_b = tmp.path().join("b.db");

        // Empty databases (only sqlite_master).
        create_db(&db_a, "SELECT 1;");
        create_db(&db_b, "SELECT 1;");

        let result = canonicalize_and_compare(&db_a, &db_b).unwrap();
        assert!(result.canonical_match || result.logical_match || result.data_complete);

        // Also test with matching empty tables.
        let db_c = tmp.path().join("c.db");
        let db_d = tmp.path().join("d.db");
        create_db(&db_c, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);");
        create_db(&db_d, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);");

        let result2 = canonicalize_and_compare(&db_c, &db_d).unwrap();
        assert_eq!(result2.tier, ComparisonTier::RawIdentical);
        assert!(result2.canonical_match);
    }

    #[test]
    fn test_schema_only_match() {
        let tmp = tempfile::TempDir::new().unwrap();
        let db_a = tmp.path().join("a.db");
        let db_b = tmp.path().join("b.db");

        // Tables exist but have no rows.
        let sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
                    CREATE TABLE orders (id INTEGER PRIMARY KEY, amount REAL);";
        create_db(&db_a, sql);
        create_db(&db_b, sql);

        let result = canonicalize_and_compare(&db_a, &db_b).unwrap();
        assert_eq!(result.tier, ComparisonTier::RawIdentical);
        assert!(result.canonical_match);
    }

    #[test]
    fn test_null_handling() {
        let tmp = tempfile::TempDir::new().unwrap();
        let db_a = tmp.path().join("a.db");
        let db_b = tmp.path().join("b.db");

        // Databases with NULL values in various positions.
        let sql = "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, score REAL);
                    INSERT INTO t VALUES (1, NULL, 3.14);
                    INSERT INTO t VALUES (2, 'test', NULL);
                    INSERT INTO t VALUES (3, NULL, NULL);";
        create_db(&db_a, sql);
        create_db(&db_b, sql);

        // Canonical comparison should work.
        let result = canonicalize_and_compare(&db_a, &db_b).unwrap();
        assert_eq!(result.tier, ComparisonTier::RawIdentical);
        assert!(result.canonical_match);

        // Also verify logical comparison handles NULLs correctly.
        let tier2 = try_tier2(&db_a, &db_b).unwrap();
        assert!(tier2.logical_match);

        // And weaker data-completeness checks.
        let tier3 = try_tier3(&db_a, &db_b).unwrap();
        assert!(tier3.data_complete);
    }
}
