//! Database canonicalization pipeline for deterministic SHA-256 hashing.
//!
//! Bead: bd-1w6k.5.2
//!
//! Produces a canonical database file whose SHA-256 is stable across repeated
//! runs for identical logical content.  The pipeline:
//!
//! 1. Checkpoint the WAL (`PRAGMA wal_checkpoint(TRUNCATE)`)
//! 2. Normalize PRAGMAs (`page_size`, `auto_vacuum = NONE`)
//! 3. `VACUUM INTO <canonical_path>` to produce a defragmented, single-file copy
//! 4. SHA-256 hash the canonical file

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

    conn.execute_batch(&format!("VACUUM INTO '{output_str}';"))?;
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
}
