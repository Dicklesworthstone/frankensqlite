//! STRICT table type-enforcement E2E tests (bd-xr8t1).
//!
//! Verifies that `CREATE TABLE ... STRICT` properly enforces column types
//! during INSERT and UPDATE, rejecting incompatible storage classes with
//! `SQLITE_CONSTRAINT_DATATYPE` (error code 3091).
//!
//! SQLite 3.37+ reference: <https://www.sqlite.org/stricttables.html>

use fsqlite::Connection;
use fsqlite_error::ErrorCode;
use std::ops::Deref;
use tempfile::{TempDir, tempdir};

struct TestConnection {
    _temp: TempDir,
    conn: Connection,
}

impl Deref for TestConnection {
    type Target = Connection;

    fn deref(&self) -> &Self::Target {
        &self.conn
    }
}

async fn open_db(name: &str) -> TestConnection {
    let temp = tempdir().expect("tempdir");
    let db_path = temp.path().join(name);
    let conn = Connection::open(db_path.to_string_lossy().to_string())
        .await
        .expect("open connection");
    TestConnection { _temp: temp, conn }
}

// ─── CREATE TABLE ... STRICT ────────────────────────────────────────────

#[test]
fn strict_table_creation_succeeds() {
    asupersync::test_utils::run_test(|| async {
        let conn = open_db("strict-create.db").await;
        conn.execute(
            "CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT, score REAL, data BLOB, extra ANY) STRICT;",
        )
        .await
        .expect("create strict table");

        let rows = conn
            .query("SELECT name FROM sqlite_master WHERE type='table' AND name='t1';")
            .await
            .expect("query sqlite_master");
        assert_eq!(rows.len(), 1, "strict table should exist in sqlite_master");
    });
}

#[test]
fn strict_table_rejects_missing_column_type() {
    asupersync::test_utils::run_test(|| async {
        let conn = open_db("strict-no-type.db").await;
        let err = conn
            .execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, name) STRICT;")
            .await
            .expect_err("STRICT table should reject column without type");
        let msg = err.to_string();
        assert!(
            msg.to_ascii_lowercase().contains("strict")
                || msg.to_ascii_lowercase().contains("type"),
            "error should mention strict/type: {msg}"
        );
    });
}

// ─── INSERT: Matching Types ─────────────────────────────────────────────

#[test]
fn strict_insert_integer_accepts_integer() {
    asupersync::test_utils::run_test(|| async {
        let conn = open_db("strict-int-ok.db").await;
        conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val INTEGER) STRICT;")
            .await
            .expect("create");
        conn.execute("INSERT INTO t1 VALUES (1, 42);")
            .await
            .expect("integer into INTEGER should succeed");
    });
}

#[test]
fn strict_insert_text_accepts_text() {
    asupersync::test_utils::run_test(|| async {
        let conn = open_db("strict-text-ok.db").await;
        conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT) STRICT;")
            .await
            .expect("create");
        conn.execute("INSERT INTO t1 VALUES (1, 'hello');")
            .await
            .expect("text into TEXT should succeed");
    });
}

#[test]
fn strict_insert_real_accepts_float() {
    asupersync::test_utils::run_test(|| async {
        let conn = open_db("strict-real-ok.db").await;
        conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val REAL) STRICT;")
            .await
            .expect("create");
        conn.execute("INSERT INTO t1 VALUES (1, 3.14);")
            .await
            .expect("float into REAL should succeed");
    });
}

#[test]
fn strict_insert_real_accepts_integer_with_coercion() {
    asupersync::test_utils::run_test(|| async {
        let conn = open_db("strict-real-int.db").await;
        conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val REAL) STRICT;")
            .await
            .expect("create");
        conn.execute("INSERT INTO t1 VALUES (1, 42);")
            .await
            .expect("integer into REAL should succeed (coerced to 42.0)");

        let rows = conn
            .query("SELECT typeof(val), val FROM t1 WHERE id = 1;")
            .await
            .expect("query");
        assert!(!rows.is_empty(), "should have a row");
    });
}

#[test]
fn strict_insert_blob_accepts_blob() {
    asupersync::test_utils::run_test(|| async {
        let conn = open_db("strict-blob-ok.db").await;
        conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val BLOB) STRICT;")
            .await
            .expect("create");
        conn.execute("INSERT INTO t1 VALUES (1, X'DEADBEEF');")
            .await
            .expect("blob into BLOB should succeed");
    });
}

#[test]
fn strict_insert_any_accepts_all_types() {
    asupersync::test_utils::run_test(|| async {
        let conn = open_db("strict-any-ok.db").await;
        conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val ANY) STRICT;")
            .await
            .expect("create");
        conn.execute("INSERT INTO t1 VALUES (1, 42);")
            .await
            .expect("integer into ANY");
        conn.execute("INSERT INTO t1 VALUES (2, 'hello');")
            .await
            .expect("text into ANY");
        conn.execute("INSERT INTO t1 VALUES (3, 3.14);")
            .await
            .expect("real into ANY");
        conn.execute("INSERT INTO t1 VALUES (4, X'CAFE');")
            .await
            .expect("blob into ANY");
        conn.execute("INSERT INTO t1 VALUES (5, NULL);")
            .await
            .expect("null into ANY");
    });
}

// ─── INSERT: Null is Always Accepted ────────────────────────────────────

#[test]
fn strict_insert_null_accepted_in_all_typed_columns() {
    asupersync::test_utils::run_test(|| async {
        let conn = open_db("strict-null-ok.db").await;
        conn.execute(
            "CREATE TABLE t1 (id INTEGER PRIMARY KEY, a INTEGER, b TEXT, c REAL, d BLOB) STRICT;",
        )
        .await
        .expect("create");
        conn.execute("INSERT INTO t1 VALUES (1, NULL, NULL, NULL, NULL);")
            .await
            .expect("NULL should be accepted in all STRICT column types");
    });
}

// ─── INSERT: Type Violations ────────────────────────────────────────────

#[test]
fn strict_insert_rejects_text_into_integer() {
    asupersync::test_utils::run_test(|| async {
        let conn = open_db("strict-text-to-int.db").await;
        conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val INTEGER) STRICT;")
            .await
            .expect("create");
        let err = conn
            .execute("INSERT INTO t1 VALUES (1, 'hello');")
            .await
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
    });
}

#[test]
fn strict_insert_rejects_text_into_real() {
    asupersync::test_utils::run_test(|| async {
        let conn = open_db("strict-text-to-real.db").await;
        conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val REAL) STRICT;")
            .await
            .expect("create");
        let err = conn
            .execute("INSERT INTO t1 VALUES (1, 'hello');")
            .await
            .expect_err("text into REAL should fail");
        assert_eq!(err.error_code(), ErrorCode::Constraint);
    });
}

#[test]
fn strict_insert_accepts_integer_into_text_gh272() {
    asupersync::test_utils::run_test(|| async {
        let conn = open_db("strict-int-to-text.db").await;
        conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT) STRICT;")
            .await
            .expect("create");
        // GH #272: TEXT affinity converts INTEGER to its text form in a STRICT
        // TEXT column (stock sqlite3 stores it as text '42'), rather than the
        // former "cannot store INTEGER value in TEXT column" error.
        conn.execute("INSERT INTO t1 VALUES (1, 42);")
            .await
            .expect("integer coerces to TEXT in a STRICT TEXT column");
        let rows = conn
            .query("SELECT 1 FROM t1 WHERE id = 1 AND typeof(val) = 'text' AND val = '42';")
            .await
            .expect("query");
        assert!(!rows.is_empty(), "integer should store as text '42'");
    });
}

#[test]
fn strict_insert_rejects_real_into_integer() {
    asupersync::test_utils::run_test(|| async {
        let conn = open_db("strict-real-to-int.db").await;
        conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val INTEGER) STRICT;")
            .await
            .expect("create");
        let err = conn
            .execute("INSERT INTO t1 VALUES (1, 3.14);")
            .await
            .expect_err("real into INTEGER should fail");
        assert_eq!(err.error_code(), ErrorCode::Constraint);
    });
}

#[test]
fn strict_insert_rejects_text_into_blob() {
    asupersync::test_utils::run_test(|| async {
        let conn = open_db("strict-text-to-blob.db").await;
        conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val BLOB) STRICT;")
            .await
            .expect("create");
        let err = conn
            .execute("INSERT INTO t1 VALUES (1, 'hello');")
            .await
            .expect_err("text into BLOB should fail");
        assert_eq!(err.error_code(), ErrorCode::Constraint);
    });
}

// ─── Non-STRICT Table: Same Types Accepted (Control Group) ──────────────

#[test]
fn non_strict_table_accepts_mismatched_types() {
    asupersync::test_utils::run_test(|| async {
        let conn = open_db("non-strict.db").await;
        conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val INTEGER);")
            .await
            .expect("create non-strict table");
        conn.execute("INSERT INTO t1 VALUES (1, 'hello');")
            .await
            .expect("text into INTEGER should succeed in non-strict table");
    });
}
