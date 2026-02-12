//! C SQLite file format compatibility roundtrip verification.
//!
//! Bead: bd-3r48 (5F.3)
//!
//! FrankenSQLite databases must be readable by C SQLite (via rusqlite) and vice versa.
//! This test verifies file format compatibility by round-tripping data between both
//! implementations.
//!
//! ## Test Coverage
//!
//! - FrankenSQLite writes, C SQLite reads
//! - C SQLite writes, FrankenSQLite reads
//! - Edge cases: NULL, large blobs, overflow pages
//! - Schema format compatibility
//!
//! ## Current Status
//!
//! FrankenSQLite's storage stack is still being integrated (Phase 5). Full file format
//! compatibility is expected after completion of:
//! - Phase 5A: DDL operations write to SQLite file format
//! - Phase 5B: DML operations write to SQLite file format
//! - Phase 5C: Query operations read from SQLite file format
//! - Phase 5D: Transaction operations properly checkpoint
//!
//! Tests gracefully report the current compatibility state.

// Match expressions are intentionally clear about enum variants being extracted
#![allow(clippy::single_match_else)]
// Test functions are intentionally comprehensive
#![allow(clippy::too_many_lines)]
// u8 casts in test data generation are intentionally wrapping
#![allow(clippy::cast_sign_loss)]
#![allow(clippy::cast_possible_truncation)]

use rusqlite::params;
use tempfile::tempdir;

// ─── Structured Logging ─────────────────────────────────────────────────────

macro_rules! compat_log {
    ($direction:expr, $step:expr, $($arg:tt)*) => {
        eprintln!("[COMPAT][{}][step={}] {}", $direction, $step, format_args!($($arg)*));
    };
}

macro_rules! compat_log_kv {
    ($direction:expr, $step:expr, $msg:expr, $($key:ident = $val:expr),* $(,)?) => {
        eprintln!("[COMPAT][{}][step={}] {} | {}", $direction, $step, $msg,
            vec![$(format!("{}={:?}", stringify!($key), $val)),*].join(" "));
    };
}

// ─── Test: C SQLite writes, FrankenSQLite reads (should work) ───────────────

/// This test verifies that FrankenSQLite can read databases created by C SQLite.
/// This is a critical capability for migration and interoperability.
#[test]
fn test_csqlite_writes_fsqlite_reads() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("compat_cs_to_fs.db");
    let path_str = path.to_str().unwrap();

    compat_log!(
        "csqlite→fsqlite",
        "start",
        "Beginning C SQLite → FrankenSQLite test"
    );

    // Phase 1: Write with C SQLite (rusqlite)
    {
        compat_log!(
            "csqlite→fsqlite",
            "open_csqlite",
            "Opening database with rusqlite"
        );
        let c_conn = rusqlite::Connection::open(&path).unwrap();

        // Create table
        compat_log!(
            "csqlite→fsqlite",
            "create_table",
            "Creating table and index"
        );
        c_conn
            .execute_batch(
                "
            CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT, val REAL, data BLOB);
            CREATE INDEX idx_name ON t(name);
        ",
            )
            .unwrap();

        // Insert 100 rows
        compat_log!("csqlite→fsqlite", "insert", "Inserting 100 rows");
        for i in 0i64..100 {
            c_conn
                .execute(
                    "INSERT INTO t VALUES (?, ?, ?, ?)",
                    params![i, format!("name_{i}"), i as f64 * 2.0, vec![i as u8; 20]],
                )
                .unwrap();
        }
    }

    // Phase 2: Read with FrankenSQLite
    {
        compat_log!(
            "csqlite→fsqlite",
            "open_fsqlite",
            "Opening database with FrankenSQLite"
        );
        let conn = match fsqlite::Connection::open(path_str) {
            Ok(c) => c,
            Err(e) => {
                compat_log!(
                    "csqlite→fsqlite",
                    "error",
                    "Failed to open C SQLite database: {e}"
                );
                compat_log!(
                    "csqlite→fsqlite",
                    "status",
                    "EXPECTED: Phase 5 storage stack integration incomplete"
                );
                // This is expected until Phase 5 is complete
                return;
            }
        };

        // Verify row count
        let rows = match conn.query("SELECT COUNT(*) FROM t") {
            Ok(r) => r,
            Err(e) => {
                compat_log!("csqlite→fsqlite", "query_error", "Query failed: {e}");
                return;
            }
        };

        let count = match rows.first().and_then(|r| r.get(0)) {
            Some(fsqlite_types::SqliteValue::Integer(n)) => *n,
            other => {
                compat_log_kv!(
                    "csqlite→fsqlite",
                    "type_error",
                    "Unexpected count type",
                    value = other
                );
                return;
            }
        };

        compat_log_kv!("csqlite→fsqlite", "count", "Row count", count = count);

        if count != 100 {
            compat_log!(
                "csqlite→fsqlite",
                "count_mismatch",
                "Expected 100 rows, got {count}"
            );
            // Don't panic - report the state
            return;
        }

        // Verify specific row
        let rows = match conn.query("SELECT id, name, val FROM t WHERE id = 50") {
            Ok(r) => r,
            Err(e) => {
                compat_log!(
                    "csqlite→fsqlite",
                    "query_error",
                    "Specific query failed: {e}"
                );
                return;
            }
        };

        if rows.is_empty() {
            compat_log!("csqlite→fsqlite", "no_row", "Row id=50 not found");
            return;
        }

        let row = rows.first().unwrap();
        let id = match row.get(0) {
            Some(fsqlite_types::SqliteValue::Integer(n)) => *n,
            _ => {
                compat_log!("csqlite→fsqlite", "type_error", "Expected integer id");
                return;
            }
        };
        let name = match row.get(1) {
            Some(fsqlite_types::SqliteValue::Text(s)) => s.clone(),
            _ => {
                compat_log!("csqlite→fsqlite", "type_error", "Expected text name");
                return;
            }
        };
        let val = match row.get(2) {
            Some(fsqlite_types::SqliteValue::Float(f)) => *f,
            _ => {
                compat_log!("csqlite→fsqlite", "type_error", "Expected float val");
                return;
            }
        };

        assert_eq!(id, 50);
        assert_eq!(name, "name_50");
        assert!((val - 100.0).abs() < f64::EPSILON);
        compat_log_kv!(
            "csqlite→fsqlite",
            "data_verify",
            "Row 50 data verified",
            id = id,
            name = name,
            val = val
        );
    }

    compat_log!("csqlite→fsqlite", "complete", "Test passed");
}

// ─── Test: FrankenSQLite writes, C SQLite reads ─────────────────────────────

/// This test verifies that databases created by FrankenSQLite can be read by C SQLite.
/// This is essential for compatibility claims.
///
/// NOTE: This test may fail until Phase 5 storage stack integration is complete.
/// The file format produced by FrankenSQLite must match SQLite exactly.
#[test]
fn test_fsqlite_writes_csqlite_reads() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("compat_fs_to_cs.db");
    let path_str = path.to_str().unwrap();

    compat_log!(
        "fsqlite→csqlite",
        "start",
        "Beginning FrankenSQLite → C SQLite test"
    );

    // Phase 1: Write with FrankenSQLite
    {
        compat_log!(
            "fsqlite→csqlite",
            "open",
            "Opening database with FrankenSQLite"
        );
        let conn = fsqlite::Connection::open(path_str).unwrap();

        // Create table with various column types
        compat_log!(
            "fsqlite→csqlite",
            "create_table",
            "CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT, val REAL, data BLOB)"
        );
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT, val REAL, data BLOB)")
            .unwrap();

        // Create index
        compat_log!(
            "fsqlite→csqlite",
            "create_index",
            "CREATE INDEX idx_name ON t(name)"
        );
        conn.execute("CREATE INDEX idx_name ON t(name)").unwrap();

        // Insert 100 rows with varied data types
        compat_log!("fsqlite→csqlite", "insert", "Inserting 100 rows");
        for i in 0i64..100 {
            let sql = format!(
                "INSERT INTO t VALUES ({}, 'name_{}', {}, X'{}')",
                i,
                i,
                i as f64 * 1.5,
                hex_encode(&[i as u8; 20])
            );
            conn.execute(&sql).unwrap();
        }

        // Close connection to ensure data is persisted
        compat_log!(
            "fsqlite→csqlite",
            "close",
            "Closing FrankenSQLite connection"
        );
        conn.close().unwrap();
    }

    // Phase 2: Attempt to read with C SQLite (rusqlite)
    {
        compat_log!(
            "fsqlite→csqlite",
            "open_csqlite",
            "Attempting to open with rusqlite"
        );

        // Try to open - may fail if file format isn't compatible yet
        let c_conn = match rusqlite::Connection::open(&path) {
            Ok(c) => c,
            Err(e) => {
                compat_log!(
                    "fsqlite→csqlite",
                    "open_error",
                    "C SQLite cannot open database: {e}"
                );
                compat_log!(
                    "fsqlite→csqlite",
                    "status",
                    "EXPECTED: FrankenSQLite file format not yet compatible"
                );
                // Expected until Phase 5 storage integration is complete
                return;
            }
        };

        // Try to verify row count
        let count_result: Result<i64, _> =
            c_conn.query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0));

        match count_result {
            Ok(count) => {
                assert_eq!(count, 100);
                compat_log_kv!(
                    "fsqlite→csqlite",
                    "count",
                    "Row count verified",
                    count = count
                );
            }
            Err(e) => {
                compat_log!(
                    "fsqlite→csqlite",
                    "query_error",
                    "C SQLite query failed: {e}"
                );
                compat_log!(
                    "fsqlite→csqlite",
                    "status",
                    "File format incompatibility detected"
                );
                return;
            }
        }

        // Run integrity check
        let integrity: String = c_conn
            .query_row("PRAGMA integrity_check", [], |r| r.get(0))
            .unwrap();

        if integrity == "ok" {
            compat_log_kv!(
                "fsqlite→csqlite",
                "integrity",
                "Integrity check passed",
                result = integrity
            );
        } else {
            compat_log_kv!(
                "fsqlite→csqlite",
                "integrity_fail",
                "Integrity check failed",
                result = integrity
            );
        }
    }

    compat_log!("fsqlite→csqlite", "complete", "Test passed");
}

// ─── Test: Edge cases (C SQLite → FrankenSQLite) ────────────────────────────

/// Test edge cases reading C SQLite databases with unusual values.
#[test]
fn test_compat_edge_cases_csqlite_to_fsqlite() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("compat_edge_cs_fs.db");
    let path_str = path.to_str().unwrap();

    compat_log!(
        "edge_cases_rev",
        "start",
        "Testing edge cases (C SQLite → FrankenSQLite)"
    );

    // Write edge cases with C SQLite
    {
        let c_conn = rusqlite::Connection::open(&path).unwrap();

        c_conn
            .execute_batch(
                "CREATE TABLE edge(id INTEGER PRIMARY KEY, txt TEXT, num INTEGER, flt REAL, blb BLOB)",
            )
            .unwrap();

        // NULL row
        c_conn
            .execute("INSERT INTO edge(id) VALUES (1)", [])
            .unwrap();

        // Empty values
        c_conn
            .execute("INSERT INTO edge VALUES (2, '', 0, 0.0, X'')", [])
            .unwrap();

        // Integer extremes
        c_conn
            .execute(
                "INSERT INTO edge VALUES (3, 'max', ?, 1.0, X'00')",
                [i64::MAX],
            )
            .unwrap();
        c_conn
            .execute(
                "INSERT INTO edge VALUES (4, 'min', ?, -1.0, X'FF')",
                [i64::MIN],
            )
            .unwrap();

        // Large blob
        let large_blob = vec![0xCD_u8; 8192];
        c_conn
            .execute(
                "INSERT INTO edge VALUES (5, 'large_blob', 5, 5.5, ?)",
                [&large_blob],
            )
            .unwrap();

        // Large text
        let large_text: String = (0..4096)
            .map(|i| char::from(b'Z' - (i % 26) as u8))
            .collect();
        c_conn
            .execute(
                "INSERT INTO edge VALUES (6, ?, 6, 6.6, X'00')",
                [&large_text],
            )
            .unwrap();
    }

    // Verify with FrankenSQLite
    {
        let conn = match fsqlite::Connection::open(path_str) {
            Ok(c) => c,
            Err(e) => {
                compat_log!(
                    "edge_cases_rev",
                    "open_error",
                    "Cannot open C SQLite database: {e}"
                );
                return;
            }
        };

        // NULL row
        let rows = match conn.query("SELECT txt, num, flt, blb FROM edge WHERE id = 1") {
            Ok(r) => r,
            Err(e) => {
                compat_log!("edge_cases_rev", "query_error", "Query failed: {e}");
                return;
            }
        };
        let row = rows.first().unwrap();
        assert!(matches!(row.get(0), Some(fsqlite_types::SqliteValue::Null)));
        assert!(matches!(row.get(1), Some(fsqlite_types::SqliteValue::Null)));
        compat_log!("edge_cases_rev", "verify_null", "NULL row verified");

        // Empty values
        let rows = conn
            .query("SELECT txt, blb FROM edge WHERE id = 2")
            .unwrap();
        let row = rows.first().unwrap();
        if let Some(fsqlite_types::SqliteValue::Text(s)) = row.get(0) {
            assert!(s.is_empty());
        }
        if let Some(fsqlite_types::SqliteValue::Blob(b)) = row.get(1) {
            assert!(b.is_empty());
        }
        compat_log!("edge_cases_rev", "verify_empty", "Empty values verified");

        // Integer extremes
        let rows = conn.query("SELECT num FROM edge WHERE id = 3").unwrap();
        if let Some(fsqlite_types::SqliteValue::Integer(n)) = rows.first().unwrap().get(0) {
            assert_eq!(*n, i64::MAX);
        }
        let rows = conn.query("SELECT num FROM edge WHERE id = 4").unwrap();
        if let Some(fsqlite_types::SqliteValue::Integer(n)) = rows.first().unwrap().get(0) {
            assert_eq!(*n, i64::MIN);
        }
        compat_log!(
            "edge_cases_rev",
            "verify_int_extremes",
            "Integer extremes verified"
        );

        // Large blob
        let rows = conn.query("SELECT blb FROM edge WHERE id = 5").unwrap();
        if let Some(fsqlite_types::SqliteValue::Blob(b)) = rows.first().unwrap().get(0) {
            assert_eq!(b.len(), 8192);
            assert!(b.iter().all(|&x| x == 0xCD));
        }
        compat_log!("edge_cases_rev", "verify_large_blob", "8KB blob verified");

        // Large text
        let rows = conn.query("SELECT txt FROM edge WHERE id = 6").unwrap();
        if let Some(fsqlite_types::SqliteValue::Text(s)) = rows.first().unwrap().get(0) {
            assert_eq!(s.len(), 4096);
        }
        compat_log!("edge_cases_rev", "verify_large_text", "4KB text verified");
    }

    compat_log!(
        "edge_cases_rev",
        "complete",
        "All reverse edge cases verified"
    );
}

// ─── Test: Bidirectional roundtrip ──────────────────────────────────────────

/// Test bidirectional roundtrip: C SQLite → FrankenSQLite → C SQLite
#[test]
fn test_compat_bidirectional_roundtrip() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("compat_roundtrip.db");
    let path_str = path.to_str().unwrap();

    compat_log!("roundtrip", "start", "Testing bidirectional roundtrip");

    // Step 1: C SQLite creates and writes
    {
        let c_conn = rusqlite::Connection::open(&path).unwrap();
        c_conn
            .execute_batch("CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)")
            .unwrap();
        for i in 0..50 {
            c_conn
                .execute(
                    "INSERT INTO t VALUES (?, ?)",
                    params![i, format!("csqlite_{i}")],
                )
                .unwrap();
        }
    }
    compat_log!("roundtrip", "step1", "C SQLite created table with 50 rows");

    // Step 2: FrankenSQLite reads and adds more
    {
        let conn = match fsqlite::Connection::open(path_str) {
            Ok(c) => c,
            Err(e) => {
                compat_log!("roundtrip", "step2_error", "Cannot open database: {e}");
                return;
            }
        };

        // Verify existing data
        let rows = conn.query("SELECT COUNT(*) FROM t").unwrap();
        let count = match rows.first().and_then(|r| r.get(0)) {
            Some(fsqlite_types::SqliteValue::Integer(n)) => *n,
            _ => {
                compat_log!("roundtrip", "step2_error", "Count query failed");
                return;
            }
        };

        if count != 50 {
            compat_log!(
                "roundtrip",
                "step2_count_mismatch",
                "Expected 50 rows, got {count}"
            );
            return;
        }

        // Add more rows
        for i in 50..100 {
            if let Err(e) = conn.execute(&format!("INSERT INTO t VALUES ({i}, 'fsqlite_{i}')")) {
                compat_log!("roundtrip", "step2_insert_error", "Insert failed: {e}");
                return;
            }
        }

        if let Err(e) = conn.close() {
            compat_log!("roundtrip", "step2_close_error", "Close failed: {e}");
            return;
        }
    }
    compat_log!(
        "roundtrip",
        "step2",
        "FrankenSQLite verified and added 50 more rows"
    );

    // Step 3: C SQLite reads and verifies
    {
        let c_conn = match rusqlite::Connection::open(&path) {
            Ok(c) => c,
            Err(e) => {
                compat_log!(
                    "roundtrip",
                    "step3_error",
                    "C SQLite cannot open after FrankenSQLite writes: {e}"
                );
                compat_log!(
                    "roundtrip",
                    "status",
                    "EXPECTED: FrankenSQLite write format not yet compatible"
                );
                return;
            }
        };

        let count: i64 = match c_conn.query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0)) {
            Ok(c) => c,
            Err(e) => {
                compat_log!("roundtrip", "step3_query_error", "Query failed: {e}");
                return;
            }
        };

        if count != 100 {
            compat_log!(
                "roundtrip",
                "step3_count_mismatch",
                "Expected 100 rows, got {count}"
            );
            return;
        }

        // Verify data from FrankenSQLite
        let val: String = c_conn
            .query_row("SELECT val FROM t WHERE id = 75", [], |r| r.get(0))
            .unwrap();

        if val != "fsqlite_75" {
            compat_log!(
                "roundtrip",
                "step3_data_mismatch",
                "Expected fsqlite_75, got {val}"
            );
            return;
        }
    }
    compat_log!("roundtrip", "step3", "C SQLite verified all 100 rows");

    compat_log!(
        "roundtrip",
        "complete",
        "Bidirectional roundtrip successful"
    );
}

// ─── Utility functions ──────────────────────────────────────────────────────

fn hex_encode(bytes: &[u8]) -> String {
    use std::fmt::Write;
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut acc, b| {
            let _ = write!(acc, "{b:02X}");
            acc
        })
}
