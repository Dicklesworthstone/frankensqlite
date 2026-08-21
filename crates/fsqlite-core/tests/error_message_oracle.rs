//! Error-message parity keeper: frank's error text must match stock SQLite
//! (3.53) VERBATIM for the common error conditions below. A differential probe
//! (frank vs rusqlite) confirmed these match; asserted here against the exact
//! expected string so a regression (e.g. an `Internal("internal error: …")`
//! wrapping, or reworded text) is caught.
//!
//! Scope note (bd-ttof2): NOT asserted here — cases where stock appends
//! " in <SQL> at offset N" (no-such-column in SELECT, no-such-function,
//! wrong-arg-count, ambiguous-column, table-already-exists), and the open bugs
//! (INTEGER-PK dup wording, INSERT column-count `not implemented:` prefix,
//! aggregate-in-WHERE not rejected). Those are tracked in bd-ttof2.

use fsqlite_core::connection::Connection;

/// Run `setup` then `sql`; assert `sql` fails with EXACTLY `expected` message.
async fn err_is(setup: &[&str], sql: &str, expected: &str) {
    let f = Connection::open(":memory:").await.unwrap();
    let _ = f.execute("PRAGMA foreign_keys=ON").await;
    for s in setup {
        let _ = f.execute(s).await;
    }
    match f.execute(sql).await {
        Ok(_) => panic!("expected an error for `{sql}`, but it succeeded"),
        Err(e) => assert_eq!(e.to_string(), expected, "sql=`{sql}`"),
    }
}

#[test]
fn constraint_violations() {
    asupersync::test_utils::run_test(|| async {
        err_is(&["CREATE TABLE t(x INT UNIQUE)", "INSERT INTO t VALUES (1)"],
               "INSERT INTO t VALUES (1)", "UNIQUE constraint failed: t.x").await;
        err_is(&["CREATE TABLE t(x INT NOT NULL)"],
               "INSERT INTO t VALUES (NULL)", "NOT NULL constraint failed: t.x").await;
        err_is(&["CREATE TABLE t(x INT CHECK (x > 0))"],
               "INSERT INTO t VALUES (-1)", "CHECK constraint failed: x > 0").await;
        err_is(&["CREATE TABLE t(x INT CONSTRAINT pos CHECK (x > 0))"],
               "INSERT INTO t VALUES (-1)", "CHECK constraint failed: pos").await;
        err_is(&["CREATE TABLE p(id INT PRIMARY KEY)", "CREATE TABLE c(pid INT REFERENCES p(id))"],
               "INSERT INTO c VALUES (99)", "FOREIGN KEY constraint failed").await;
    });
}

#[test]
fn strict_type_errors() {
    asupersync::test_utils::run_test(|| async {
        err_is(&["CREATE TABLE t(x INTEGER) STRICT"],
               "INSERT INTO t VALUES ('notanint')", "cannot store TEXT value in INTEGER column t.x").await;
    });
}

#[test]
fn name_resolution_and_txn_errors() {
    asupersync::test_utils::run_test(|| async {
        err_is(&[], "SELECT * FROM nonexistent", "no such table: nonexistent").await;
        // bd-ttof2 fix: an unknown INSERT column now matches stock verbatim
        // (was Internal("internal error: column '…' not found in table '…'")).
        err_is(&["CREATE TABLE t(a INT)"],
               "INSERT INTO t(nope) VALUES (1)", "table t has no column named nope").await;
        err_is(&[], "RELEASE nonexistent_sp", "no such savepoint: nonexistent_sp").await;
        err_is(&[], "COMMIT", "cannot commit - no transaction is active").await;
    });
}
