//! bd-8rjci (parity): a read-only handle must refuse a write-class statement
//! at the execute() boundary regardless of whether it would change any rows.
//!
//! Stock SQLite (verified vs sqlite3 3.51.0) raises SQLITE_READONLY — code 8,
//! "attempt to write a readonly database" — for EVERY write on a `mode=ro`
//! connection, including a no-op whose WHERE can never match:
//!
//! ```text
//! $ sqlite3 "file:oracle.db?mode=ro" "DELETE FROM t WHERE 1=0;"
//! Error: stepping, attempt to write a readonly database (8)
//! $ sqlite3 "file:oracle.db?mode=ro" "UPDATE t SET v='z' WHERE 1=0;"
//! Error: stepping, attempt to write a readonly database (8)
//! $ sqlite3 "file:oracle.db?mode=ro" "DELETE FROM t WHERE id=1;"
//! Error: stepping, attempt to write a readonly database (8)
//! ```
//!
//! Previously fsqlite admitted the no-op forms: the precompiled/deferred DML
//! fast lanes short-circuit a no-op UPDATE/DELETE (0 rows match) before ever
//! acquiring a write transaction, so the pager-level read-only check never
//! fired and `execute()` wrongly returned `Ok(0)` instead of `Err(ReadOnly)`.
//! A row-changing write already errored (it reaches the write path). The fix
//! adds the read-only guard to the MAIN-targeted DML fast lanes, matching the
//! dispatcher's existing guard.

use fsqlite_core::connection::Connection;
use fsqlite_error::{ErrorCode, FrankenError};
use fsqlite_types::value::SqliteValue;

/// Seed a database file with a small table, then return a fresh read-only
/// (`is_readonly() == true`) connection to it. `open_schema_only` opens a
/// read-only pager — the same door the VACUUM-INTO keeper uses.
async fn seed_then_open_readonly(dir: &std::path::Path) -> Connection {
    let path = dir.join("src.db").to_string_lossy().into_owned();
    {
        let conn = Connection::open(&path).await.expect("create source");
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);")
            .await
            .expect("create table");
        conn.execute("INSERT INTO t(v) VALUES ('a'), ('b');")
            .await
            .expect("seed rows");
        conn.close().await.expect("close writer");
    }
    Connection::open_schema_only(&path)
        .await
        .expect("open read-only source")
}

fn assert_stock_readonly(err: &FrankenError, label: &str) {
    assert!(
        matches!(err, FrankenError::ReadOnly),
        "{label}: expected FrankenError::ReadOnly, got {err:?}"
    );
    // Exact stock parity: code 8 + verbatim message.
    assert_eq!(
        err.error_code(),
        ErrorCode::ReadOnly,
        "{label}: expected ErrorCode::ReadOnly"
    );
    assert_eq!(err.error_code() as i32, 8, "{label}: SQLITE_READONLY is 8");
    assert_eq!(
        err.to_string(),
        "attempt to write a readonly database",
        "{label}: message must match stock verbatim"
    );
}

/// The bug: a no-op `DELETE ... WHERE 1=0` on a read-only handle must be
/// refused with SQLITE_READONLY, not silently admitted as `Ok(0)`.
#[test]
fn bd_8rjci_noop_delete_on_readonly_is_refused() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("temp dir");
        let ro = seed_then_open_readonly(dir.path()).await;

        let err = ro
            .execute("DELETE FROM t WHERE 1=0;")
            .await
            .expect_err("no-op DELETE on a read-only handle must fail SQLITE_READONLY");
        assert_stock_readonly(&err, "no-op DELETE");

        ro.close().await.expect("close read-only");
    });
}

/// The same defect on the UPDATE fast lane: a no-op `UPDATE ... WHERE 1=0`.
#[test]
fn bd_8rjci_noop_update_on_readonly_is_refused() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("temp dir");
        let ro = seed_then_open_readonly(dir.path()).await;

        let err = ro
            .execute("UPDATE t SET v='z' WHERE 1=0;")
            .await
            .expect_err("no-op UPDATE on a read-only handle must fail SQLITE_READONLY");
        assert_stock_readonly(&err, "no-op UPDATE");

        ro.close().await.expect("close read-only");
    });
}

/// A row-changing write must still be refused (regression guard for the path
/// that already worked, so the fix does not accidentally narrow it).
#[test]
fn bd_8rjci_row_changing_write_on_readonly_still_refused() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("temp dir");
        let ro = seed_then_open_readonly(dir.path()).await;

        let del = ro
            .execute("DELETE FROM t WHERE id=1;")
            .await
            .expect_err("row-changing DELETE on a read-only handle must fail SQLITE_READONLY");
        assert_stock_readonly(&del, "row-changing DELETE");

        let upd = ro
            .execute("UPDATE t SET v='z' WHERE id=2;")
            .await
            .expect_err("row-changing UPDATE on a read-only handle must fail SQLITE_READONLY");
        assert_stock_readonly(&upd, "row-changing UPDATE");

        ro.close().await.expect("close read-only");
    });
}

/// Control: the identical no-op statements on a WRITABLE handle stay `Ok(0)` —
/// the fix must not turn a legitimate no-op write into an error.
#[test]
fn bd_8rjci_noop_write_on_writable_handle_is_ok_zero() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("rw.db").to_string_lossy().into_owned();
        let conn = Connection::open(&path).await.expect("open writable");
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);")
            .await
            .expect("create table");
        conn.execute("INSERT INTO t(v) VALUES ('a'), ('b');")
            .await
            .expect("seed rows");

        let deleted = conn
            .execute("DELETE FROM t WHERE 1=0;")
            .await
            .expect("no-op DELETE on a writable handle succeeds");
        assert_eq!(deleted, 0, "no-op DELETE changes no rows");

        let updated = conn
            .execute("UPDATE t SET v='z' WHERE 1=0;")
            .await
            .expect("no-op UPDATE on a writable handle succeeds");
        assert_eq!(updated, 0, "no-op UPDATE changes no rows");

        // Rows are untouched.
        let rows = conn
            .query("SELECT COUNT(*) FROM t;")
            .await
            .expect("count rows");
        assert_eq!(rows[0].values()[0], SqliteValue::Integer(2));

        conn.close().await.expect("close writable");
    });
}
