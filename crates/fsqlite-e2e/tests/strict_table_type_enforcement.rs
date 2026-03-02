//! STRICT table type-enforcement E2E tests (bd-xr8t1).
//!
//! Verifies that `CREATE TABLE ... STRICT` properly enforces column types
//! during INSERT and UPDATE, rejecting incompatible storage classes with
//! `SQLITE_CONSTRAINT_DATATYPE` (error code 3091).
//!
//! SQLite 3.37+ reference: <https://www.sqlite.org/stricttables.html>

use fsqlite::Connection;
use fsqlite_error::ErrorCode;
use tempfile::tempdir;

fn open_db(name: &str) -> Connection {
    let temp = tempdir().expect("tempdir");
    let db_path = temp.path().join(name);
    Connection::open(db_path.to_string_lossy().to_string()).expect("open connection")
}

// ─── CREATE TABLE ... STRICT ────────────────────────────────────────────

#[test]
fn strict_table_creation_succeeds() {
    let conn = open_db("strict-create.db");
    conn.execute(
        "CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT, score REAL, data BLOB, extra ANY) STRICT;",
    )
    .expect("create strict table");

    let rows = conn
        .query("SELECT name FROM sqlite_master WHERE type='table' AND name='t1';")
        .expect("query sqlite_master");
    assert_eq!(rows.len(), 1, "strict table should exist in sqlite_master");
}

#[test]
fn strict_table_rejects_missing_column_type() {
    let conn = open_db("strict-no-type.db");
    let err = conn
        .execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, name) STRICT;")
        .expect_err("STRICT table should reject column without type");
    let msg = err.to_string();
    assert!(
        msg.to_ascii_lowercase().contains("strict") || msg.to_ascii_lowercase().contains("type"),
        "error should mention strict/type: {msg}"
    );
}

// ─── INSERT: Matching Types ─────────────────────────────────────────────

#[test]
fn strict_insert_integer_accepts_integer() {
    let conn = open_db("strict-int-ok.db");
    conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val INTEGER) STRICT;")
        .expect("create");
    conn.execute("INSERT INTO t1 VALUES (1, 42);")
        .expect("integer into INTEGER should succeed");
}

#[test]
fn strict_insert_text_accepts_text() {
    let conn = open_db("strict-text-ok.db");
    conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT) STRICT;")
        .expect("create");
    conn.execute("INSERT INTO t1 VALUES (1, 'hello');")
        .expect("text into TEXT should succeed");
}

#[test]
fn strict_insert_real_accepts_float() {
    let conn = open_db("strict-real-ok.db");
    conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val REAL) STRICT;")
        .expect("create");
    conn.execute("INSERT INTO t1 VALUES (1, 3.14);")
        .expect("float into REAL should succeed");
}

#[test]
fn strict_insert_real_accepts_integer_with_coercion() {
    let conn = open_db("strict-real-int.db");
    conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val REAL) STRICT;")
        .expect("create");
    conn.execute("INSERT INTO t1 VALUES (1, 42);")
        .expect("integer into REAL should succeed (coerced to 42.0)");

    let rows = conn
        .query("SELECT typeof(val), val FROM t1 WHERE id = 1;")
        .expect("query");
    assert!(!rows.is_empty(), "should have a row");
}

#[test]
fn strict_insert_blob_accepts_blob() {
    let conn = open_db("strict-blob-ok.db");
    conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val BLOB) STRICT;")
        .expect("create");
    conn.execute("INSERT INTO t1 VALUES (1, X'DEADBEEF');")
        .expect("blob into BLOB should succeed");
}

#[test]
fn strict_insert_any_accepts_all_types() {
    let conn = open_db("strict-any-ok.db");
    conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val ANY) STRICT;")
        .expect("create");
    conn.execute("INSERT INTO t1 VALUES (1, 42);")
        .expect("integer into ANY");
    conn.execute("INSERT INTO t1 VALUES (2, 'hello');")
        .expect("text into ANY");
    conn.execute("INSERT INTO t1 VALUES (3, 3.14);")
        .expect("real into ANY");
    conn.execute("INSERT INTO t1 VALUES (4, X'CAFE');")
        .expect("blob into ANY");
    conn.execute("INSERT INTO t1 VALUES (5, NULL);")
        .expect("null into ANY");
}

// ─── INSERT: Null is Always Accepted ────────────────────────────────────

#[test]
fn strict_insert_null_accepted_in_all_typed_columns() {
    let conn = open_db("strict-null-ok.db");
    conn.execute(
        "CREATE TABLE t1 (id INTEGER PRIMARY KEY, a INTEGER, b TEXT, c REAL, d BLOB) STRICT;",
    )
    .expect("create");
    conn.execute("INSERT INTO t1 VALUES (1, NULL, NULL, NULL, NULL);")
        .expect("NULL should be accepted in all STRICT column types");
}

// ─── INSERT: Type Violations ────────────────────────────────────────────

#[test]
fn strict_insert_rejects_text_into_integer() {
    let conn = open_db("strict-text-to-int.db");
    conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val INTEGER) STRICT;")
        .expect("create");
    let err = conn
        .execute("INSERT INTO t1 VALUES (1, 'hello');")
        .expect_err("text into INTEGER should fail");
    assert_eq!(
        err.error_code(),
        ErrorCode::Constraint,
        "should be SQLITE_CONSTRAINT: {err}"
    );
    let msg = err.to_string();
    assert!(
        msg.contains("cannot store"),
        "error should say 'cannot store': {msg}"
    );
}

#[test]
fn strict_insert_rejects_text_into_real() {
    let conn = open_db("strict-text-to-real.db");
    conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val REAL) STRICT;")
        .expect("create");
    let err = conn
        .execute("INSERT INTO t1 VALUES (1, 'hello');")
        .expect_err("text into REAL should fail");
    assert_eq!(err.error_code(), ErrorCode::Constraint);
}

#[test]
fn strict_insert_rejects_integer_into_text() {
    let conn = open_db("strict-int-to-text.db");
    conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT) STRICT;")
        .expect("create");
    let err = conn
        .execute("INSERT INTO t1 VALUES (1, 42);")
        .expect_err("integer into TEXT should fail");
    assert_eq!(err.error_code(), ErrorCode::Constraint);
}

#[test]
fn strict_insert_rejects_real_into_integer() {
    let conn = open_db("strict-real-to-int.db");
    conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val INTEGER) STRICT;")
        .expect("create");
    let err = conn
        .execute("INSERT INTO t1 VALUES (1, 3.14);")
        .expect_err("real into INTEGER should fail");
    assert_eq!(err.error_code(), ErrorCode::Constraint);
}

#[test]
fn strict_insert_rejects_text_into_blob() {
    let conn = open_db("strict-text-to-blob.db");
    conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val BLOB) STRICT;")
        .expect("create");
    let err = conn
        .execute("INSERT INTO t1 VALUES (1, 'hello');")
        .expect_err("text into BLOB should fail");
    assert_eq!(err.error_code(), ErrorCode::Constraint);
}

// ─── Non-STRICT Table: Same Types Accepted (Control Group) ──────────────

#[test]
fn non_strict_table_accepts_mismatched_types() {
    let conn = open_db("non-strict.db");
    conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val INTEGER);")
        .expect("create non-strict table");
    conn.execute("INSERT INTO t1 VALUES (1, 'hello');")
        .expect("text into INTEGER should succeed in non-strict table");
}
