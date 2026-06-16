//! Isolation probes for the bd-rjc sequential single-account increment bug.
//!
//! `connection::tests::test_sequential_single_account_increment_oracle_probe_bd_rjc`
//! drives a longer sequential autocommit `UPDATE t SET v = v + 1` workload on
//! one file-backed connection and expects every increment to become durable.
//! These shorter probes split that scenario into its two independent variables
//! so a regression can be attributed precisely:
//!
//! * [`autocommit_increments_persist_without_external_oracle`] runs the pure
//!   FrankenSQLite path (no second SQLite engine ever touches the file). This is
//!   the invariant FrankenSQLite owns end-to-end: every autocommit increment
//!   must become durable.
//! * [`autocommit_increments_survive_interleaved_rusqlite_oracle`] reproduces the
//!   exact failing-test shape, opening a `rusqlite` connection mid-loop (as the
//!   oracle assertions do). Real SQLite can checkpoint and remove/reset the
//!   path-visible `-wal` while a live FrankenSQLite writer still owns the old
//!   file descriptor. The writer must recover through the current sidecar path
//!   before appending, or every later commit lands on the detached WAL handle.

use fsqlite_core::connection::Connection;
use fsqlite_types::SqliteValue;

const TXNS: i64 = 64;

fn read_value(conn: &Connection) -> Option<i64> {
    conn.query_row("SELECT v FROM t WHERE id = 1;")
        .ok()
        .and_then(|row| row.get(0).cloned())
        .and_then(|value| match value {
            SqliteValue::Integer(n) => Some(n),
            _ => None,
        })
}

#[test]
fn autocommit_increments_persist_without_external_oracle() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir
        .path()
        .join("seq_autocommit_no_oracle.db")
        .to_string_lossy()
        .into_owned();

    let conn = Connection::open(&db).unwrap();
    conn.execute("PRAGMA busy_timeout=5000;").unwrap();
    conn.execute("PRAGMA fsqlite.concurrent_mode=ON;").unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER NOT NULL);")
        .unwrap();
    conn.execute("INSERT INTO t (id, v) VALUES (1, 0);")
        .unwrap();

    for step in 0..TXNS {
        assert_eq!(
            conn.execute("UPDATE t SET v = v + 1 WHERE id = 1;")
                .unwrap(),
            1,
            "autocommit increment {step} should affect exactly one row"
        );
        assert_eq!(
            read_value(&conn),
            Some(step + 1),
            "autocommit increment {step} must be visible to the writing connection"
        );
    }
    drop(conn);

    let verifier = Connection::open(&db).unwrap();
    assert_eq!(
        read_value(&verifier),
        Some(TXNS),
        "every autocommit increment must be durable for a fresh connection"
    );
}

#[test]
fn autocommit_increments_persist_without_reads_or_external_oracle() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir
        .path()
        .join("seq_autocommit_no_reads.db")
        .to_string_lossy()
        .into_owned();

    let conn = Connection::open(&db).unwrap();
    conn.execute("PRAGMA busy_timeout=5000;").unwrap();
    conn.execute("PRAGMA fsqlite.concurrent_mode=ON;").unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER NOT NULL);")
        .unwrap();
    conn.execute("INSERT INTO t (id, v) VALUES (1, 0);")
        .unwrap();

    for step in 0..TXNS {
        assert_eq!(
            conn.execute("UPDATE t SET v = v + 1 WHERE id = 1;")
                .unwrap(),
            1,
            "autocommit increment {step} should affect exactly one row"
        );
    }
    drop(conn);

    let verifier = Connection::open(&db).unwrap();
    assert_eq!(
        read_value(&verifier),
        Some(TXNS),
        "every autocommit increment must be durable without read-boundary refreshes"
    );
}

#[test]
fn autocommit_increments_survive_interleaved_rusqlite_oracle() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir
        .path()
        .join("seq_autocommit_rusqlite_oracle.db")
        .to_string_lossy()
        .into_owned();

    let conn = Connection::open(&db).unwrap();
    conn.execute("PRAGMA busy_timeout=5000;").unwrap();
    conn.execute("PRAGMA fsqlite.concurrent_mode=ON;").unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER NOT NULL);")
        .unwrap();
    conn.execute("INSERT INTO t (id, v) VALUES (1, 0);")
        .unwrap();

    for step in 0..TXNS {
        assert_eq!(
            conn.execute("UPDATE t SET v = v + 1 WHERE id = 1;")
                .unwrap(),
            1,
            "autocommit increment {step} should affect exactly one row"
        );
        if step == 0 {
            // Mirror the failing oracle test: a second SQLite engine reads the
            // file mid-loop. On close it may checkpoint and remove/reset the
            // path-visible WAL sidecar behind the live FrankenSQLite handle.
            let oracle = rusqlite::Connection::open(&db).unwrap();
            let value: i64 = oracle
                .query_row("SELECT v FROM t WHERE id = 1;", [], |row| row.get(0))
                .unwrap();
            assert_eq!(
                value, 1,
                "oracle should observe the first committed increment"
            );
        }
    }
    drop(conn);

    let verifier = Connection::open(&db).unwrap();
    assert_eq!(
        read_value(&verifier),
        Some(TXNS),
        "autocommit increments must remain durable even after an external SQLite \
         connection opened (and possibly reset the WAL) mid-loop"
    );
}
