#![recursion_limit = "512"]

//! bd-ih8ak (owner decision 2026-11-12): path-identity probes run once per
//! transaction, at its begin and at its commit, instead of before every file
//! operation. These keepers pin what must still hold:
//! - a database file replaced at its path between two autocommit statements
//!   fails the next statement at its begin: a read-only SELECT has no commit
//!   probe, so only the begin probe can refuse it;
//! - one replaced inside an explicit transaction fails that transaction's
//!   COMMIT before anything is appended to the WAL.

#![cfg(all(feature = "native", unix))]

use std::path::{Path, PathBuf};

use fsqlite_core::connection::Connection;
use fsqlite_error::FrankenError;

/// A valid database the connection has never seen, to put at its path.
fn stranger_database(dir: &Path) -> PathBuf {
    let path = dir.join("stranger.db");
    let conn = rusqlite::Connection::open(&path).expect("open stranger");
    conn.execute_batch("CREATE TABLE other(y); INSERT INTO other VALUES (42);")
        .expect("fill stranger");
    drop(conn);
    path
}

/// Move the database aside and put a copy of `stranger` at its path: a new
/// file at the stable path, which the connection's binding never bound. The
/// `-wal` path still names the connection's own WAL.
fn swap_database_file(db: &Path, aside: &Path, stranger: &Path) {
    std::fs::rename(db, aside).expect("move the database aside");
    std::fs::copy(stranger, db).expect("put the stranger at the path");
}

fn wal_len(db: &Path) -> u64 {
    let mut wal = db.as_os_str().to_owned();
    wal.push("-wal");
    std::fs::metadata(PathBuf::from(wal)).map_or(0, |metadata| metadata.len())
}

fn assert_refused_as_replaced<T: std::fmt::Debug>(result: Result<T, FrankenError>, what: &str) {
    match result {
        Err(FrankenError::CannotOpen { .. }) => {}
        other => panic!("{what} must be refused as a replaced database, got {other:?}"),
    }
}

#[test]
fn a_database_replaced_between_autocommit_statements_fails_the_next_statement() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = dir.path().join("swap.db");
        let stranger = stranger_database(dir.path());
        let conn = Connection::open(db.to_str().expect("utf-8 path"))
            .await
            .expect("open");
        conn.execute("PRAGMA journal_mode=WAL").await.expect("wal");
        conn.execute("CREATE TABLE t(x)").await.expect("ddl");
        conn.execute("INSERT INTO t VALUES (1)")
            .await
            .expect("insert");
        let rows = conn.query("SELECT count(*) FROM t").await.expect("count");
        assert_eq!(rows.len(), 1, "the table reads before the swap");

        swap_database_file(&db, &dir.path().join("aside.db"), &stranger);
        let wal_before = wal_len(&db);

        assert_refused_as_replaced(
            conn.query("SELECT count(*) FROM t").await,
            "an autocommit SELECT after the swap",
        );
        assert_refused_as_replaced(
            conn.execute("INSERT INTO t VALUES (2)").await,
            "an autocommit INSERT after the swap",
        );
        assert_eq!(
            wal_len(&db),
            wal_before,
            "the refused INSERT appended nothing to the connection's WAL"
        );
    });
}

#[test]
fn a_database_replaced_inside_a_transaction_fails_its_commit_before_appending() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = dir.path().join("swap_txn.db");
        let stranger = stranger_database(dir.path());
        let conn = Connection::open(db.to_str().expect("utf-8 path"))
            .await
            .expect("open");
        conn.execute("PRAGMA journal_mode=WAL").await.expect("wal");
        conn.execute("CREATE TABLE t(x)").await.expect("ddl");
        conn.execute("INSERT INTO t VALUES (1)")
            .await
            .expect("insert");

        conn.execute("BEGIN").await.expect("begin");
        conn.execute("INSERT INTO t VALUES (2)")
            .await
            .expect("insert inside the transaction");
        swap_database_file(&db, &dir.path().join("aside_txn.db"), &stranger);
        let wal_before = wal_len(&db);

        let commit = conn.execute("COMMIT").await;
        assert!(
            commit.is_err(),
            "COMMIT after the database was replaced inside the transaction must fail: {commit:?}"
        );
        assert_eq!(
            wal_len(&db),
            wal_before,
            "the refused COMMIT appended nothing to the connection's WAL"
        );
        let _ = conn.execute("ROLLBACK").await;
    });
}
