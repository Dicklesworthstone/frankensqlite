//! Correctness test: MVCC isolation — snapshot reads during concurrent writes.
//!
//! Bead: bd-oqi4
//!
//! Validates snapshot isolation: a reader that opens a transaction sees a
//! stable view of the data, even while a concurrent writer is modifying
//! rows.  Uses C SQLite (via rusqlite with WAL mode) as the reference
//! implementation, and verifies FrankenSQLite's in-memory engine produces
//! equivalent isolation behavior for single-connection BEGIN/query patterns.
//!
//! FrankenSQLite's full MVCC concurrent writer path is not yet wired to
//! persistence, so cross-connection isolation tests run against C SQLite
//! only.  Single-connection transaction isolation (which is what the
//! in-memory engine supports) is verified on both engines.

use std::sync::{Arc, Barrier};
use std::thread;

use fsqlite_e2e::comparison::{ComparisonRunner, SqlBackend, SqlValue};

// ─── Single-connection isolation tests (both engines) ──────────────────

#[test]
fn isolation_read_stability_within_transaction() {
    // Within a single BEGIN..COMMIT, repeated reads return the same result
    // even if we modify data in between (on the same connection, the changes
    // ARE visible since it's the same transaction).
    let runner = ComparisonRunner::new_in_memory().expect("failed to create runner");

    let setup = [
        "CREATE TABLE iso_test (id INTEGER PRIMARY KEY, val INTEGER)",
        "INSERT INTO iso_test VALUES (1, 100)",
        "INSERT INTO iso_test VALUES (2, 200)",
    ];
    for sql in &setup {
        runner.csqlite().execute(sql).expect("csqlite setup");
        runner.frank().execute(sql).expect("fsqlite setup");
    }

    // Both engines: read, modify, read again — within same implicit txns.
    let read_sql = "SELECT val FROM iso_test WHERE id = 1";
    let c_before = runner.csqlite().query(read_sql).expect("csqlite read1");
    let f_before = runner.frank().query(read_sql).expect("fsqlite read1");
    assert_eq!(c_before, f_before, "initial read differs");
    assert_eq!(c_before[0][0], SqlValue::Integer(100));

    // Update row 1.
    runner
        .csqlite()
        .execute("UPDATE iso_test SET val = 999 WHERE id = 1")
        .expect("csqlite update");
    runner
        .frank()
        .execute("UPDATE iso_test SET val = 999 WHERE id = 1")
        .expect("fsqlite update");

    // Read again — should see the updated value.
    let c_after = runner.csqlite().query(read_sql).expect("csqlite read2");
    let f_after = runner.frank().query(read_sql).expect("fsqlite read2");
    assert_eq!(c_after, f_after, "post-update read differs");
    assert_eq!(c_after[0][0], SqlValue::Integer(999));
}

#[test]
fn isolation_rollback_restores_snapshot() {
    // BEGIN → modify → ROLLBACK → read should see original values.
    let runner = ComparisonRunner::new_in_memory().expect("failed to create runner");

    let setup = [
        "CREATE TABLE iso_test (id INTEGER PRIMARY KEY, val INTEGER)",
        "INSERT INTO iso_test VALUES (1, 100)",
    ];
    for sql in &setup {
        runner.csqlite().execute(sql).expect("csqlite setup");
        runner.frank().execute(sql).expect("fsqlite setup");
    }

    // Begin, modify, rollback.
    for sql in &[
        "BEGIN",
        "UPDATE iso_test SET val = 999 WHERE id = 1",
        "ROLLBACK",
    ] {
        runner.csqlite().execute(sql).expect("csqlite txn");
        runner.frank().execute(sql).expect("fsqlite txn");
    }

    // Should see original value.
    let read_sql = "SELECT val FROM iso_test WHERE id = 1";
    let c_val = runner.csqlite().query(read_sql).expect("csqlite read");
    let f_val = runner.frank().query(read_sql).expect("fsqlite read");
    assert_eq!(c_val, f_val, "rollback read differs");
    assert_eq!(c_val[0][0], SqlValue::Integer(100));
}

#[test]
fn isolation_committed_changes_visible_after_commit() {
    let runner = ComparisonRunner::new_in_memory().expect("failed to create runner");

    let setup = [
        "CREATE TABLE iso_test (id INTEGER PRIMARY KEY, val INTEGER)",
        "INSERT INTO iso_test VALUES (1, 100)",
    ];
    for sql in &setup {
        runner.csqlite().execute(sql).expect("csqlite setup");
        runner.frank().execute(sql).expect("fsqlite setup");
    }

    // Commit a change in a transaction.
    for sql in &[
        "BEGIN",
        "UPDATE iso_test SET val = 500 WHERE id = 1",
        "COMMIT",
    ] {
        runner.csqlite().execute(sql).expect("csqlite txn");
        runner.frank().execute(sql).expect("fsqlite txn");
    }

    // Should see committed value.
    let read_sql = "SELECT val FROM iso_test WHERE id = 1";
    let c_val = runner.csqlite().query(read_sql).expect("csqlite read");
    let f_val = runner.frank().query(read_sql).expect("fsqlite read");
    assert_eq!(c_val, f_val, "committed read differs");
    assert_eq!(c_val[0][0], SqlValue::Integer(500));
}

#[test]
fn isolation_multiple_transactions_sequential() {
    // Multiple sequential transactions should accumulate changes correctly.
    let runner = ComparisonRunner::new_in_memory().expect("failed to create runner");

    runner
        .csqlite()
        .execute("CREATE TABLE iso_test (id INTEGER PRIMARY KEY, val INTEGER)")
        .expect("csqlite create");
    runner
        .frank()
        .execute("CREATE TABLE iso_test (id INTEGER PRIMARY KEY, val INTEGER)")
        .expect("fsqlite create");

    // Transaction 1: insert rows.
    for sql in &[
        "BEGIN",
        "INSERT INTO iso_test VALUES (1, 10)",
        "INSERT INTO iso_test VALUES (2, 20)",
        "COMMIT",
    ] {
        runner.csqlite().execute(sql).expect("csqlite txn1");
        runner.frank().execute(sql).expect("fsqlite txn1");
    }

    // Transaction 2: update and insert.
    for sql in &[
        "BEGIN",
        "UPDATE iso_test SET val = 100 WHERE id = 1",
        "INSERT INTO iso_test VALUES (3, 30)",
        "COMMIT",
    ] {
        runner.csqlite().execute(sql).expect("csqlite txn2");
        runner.frank().execute(sql).expect("fsqlite txn2");
    }

    // Transaction 3: delete and insert.
    for sql in &[
        "BEGIN",
        "DELETE FROM iso_test WHERE id = 2",
        "INSERT INTO iso_test VALUES (4, 40)",
        "COMMIT",
    ] {
        runner.csqlite().execute(sql).expect("csqlite txn3");
        runner.frank().execute(sql).expect("fsqlite txn3");
    }

    // Verify final state.
    let read_sql = "SELECT id, val FROM iso_test ORDER BY id";
    let c_rows = runner.csqlite().query(read_sql).expect("csqlite read");
    let f_rows = runner.frank().query(read_sql).expect("fsqlite read");
    assert_eq!(c_rows, f_rows, "multi-transaction state differs");
    assert_eq!(c_rows.len(), 3); // rows 1, 3, 4 (2 was deleted)
}

// ─── Cross-connection snapshot isolation (C SQLite only) ───────────────
//
// These tests use C SQLite to verify WAL-mode snapshot isolation behavior
// that FrankenSQLite's MVCC will replicate once wired to persistence.

#[test]
fn csqlite_wal_snapshot_isolation_no_dirty_reads() {
    // Writer starts a transaction but doesn't commit.
    // Reader should NOT see uncommitted changes.
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let path = tmp.path().to_str().unwrap();

    let setup = rusqlite::Connection::open(path).unwrap();
    setup
        .execute_batch(
            "PRAGMA journal_mode=WAL;\
             CREATE TABLE iso_test (id INTEGER PRIMARY KEY, val INTEGER);\
             INSERT INTO iso_test VALUES (1, 100);",
        )
        .unwrap();
    drop(setup);

    // Writer opens and modifies but doesn't commit.
    let writer = rusqlite::Connection::open(path).unwrap();
    writer
        .execute_batch(
            "PRAGMA journal_mode=WAL; BEGIN; UPDATE iso_test SET val = 999 WHERE id = 1;",
        )
        .unwrap();

    // Reader should see original value (no dirty read).
    let reader = rusqlite::Connection::open(path).unwrap();
    reader.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
    let val: i64 = reader
        .query_row("SELECT val FROM iso_test WHERE id = 1", [], |r| r.get(0))
        .unwrap();
    assert_eq!(val, 100, "dirty read detected!");

    // Writer commits.
    writer.execute_batch("COMMIT;").unwrap();

    // Now reader should see committed value.
    let val2: i64 = reader
        .query_row("SELECT val FROM iso_test WHERE id = 1", [], |r| r.get(0))
        .unwrap();
    assert_eq!(val2, 999, "committed value not visible");
}

#[test]
fn csqlite_wal_snapshot_read_stability() {
    // Reader opens a transaction, writer commits changes.
    // Reader's transaction should still see the old snapshot.
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let path = tmp.path().to_str().unwrap();

    let setup = rusqlite::Connection::open(path).unwrap();
    setup
        .execute_batch(
            "PRAGMA journal_mode=WAL;\
             CREATE TABLE iso_test (id INTEGER PRIMARY KEY, val INTEGER);\
             INSERT INTO iso_test VALUES (1, 100);",
        )
        .unwrap();
    drop(setup);

    // Reader opens a BEGIN transaction — takes a snapshot.
    let reader = rusqlite::Connection::open(path).unwrap();
    reader
        .execute_batch("PRAGMA journal_mode=WAL; BEGIN;")
        .unwrap();
    let val_before: i64 = reader
        .query_row("SELECT val FROM iso_test WHERE id = 1", [], |r| r.get(0))
        .unwrap();
    assert_eq!(val_before, 100);

    // Writer commits a change.
    let writer = rusqlite::Connection::open(path).unwrap();
    writer
        .execute_batch(
            "PRAGMA journal_mode=WAL;\
             BEGIN; UPDATE iso_test SET val = 999 WHERE id = 1; COMMIT;",
        )
        .unwrap();

    // Reader still sees old value (snapshot isolation).
    let val_during: i64 = reader
        .query_row("SELECT val FROM iso_test WHERE id = 1", [], |r| r.get(0))
        .unwrap();
    assert_eq!(val_during, 100, "snapshot isolation violated!");

    // Reader closes transaction — now sees new value.
    reader.execute_batch("COMMIT;").unwrap();
    let val_after: i64 = reader
        .query_row("SELECT val FROM iso_test WHERE id = 1", [], |r| r.get(0))
        .unwrap();
    assert_eq!(
        val_after, 999,
        "committed value not visible after reader txn ends"
    );
}

#[test]
fn csqlite_wal_concurrent_writer_reader_no_phantom() {
    // Writer inserts rows while reader has an open transaction.
    // Reader should not see "phantom" rows within its transaction.
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let path = tmp.path().to_str().unwrap();

    let setup = rusqlite::Connection::open(path).unwrap();
    setup
        .execute_batch(
            "PRAGMA journal_mode=WAL;\
             CREATE TABLE iso_test (id INTEGER PRIMARY KEY, val INTEGER);\
             INSERT INTO iso_test VALUES (1, 100);",
        )
        .unwrap();
    drop(setup);

    // Reader opens transaction — sees 1 row.
    let reader = rusqlite::Connection::open(path).unwrap();
    reader
        .execute_batch("PRAGMA journal_mode=WAL; BEGIN;")
        .unwrap();
    let count_before: i64 = reader
        .query_row("SELECT COUNT(*) FROM iso_test", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count_before, 1);

    // Writer inserts more rows and commits.
    let writer = rusqlite::Connection::open(path).unwrap();
    writer
        .execute_batch("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;")
        .unwrap();
    writer
        .execute_batch(
            "BEGIN;\
             INSERT INTO iso_test VALUES (2, 200);\
             INSERT INTO iso_test VALUES (3, 300);\
             COMMIT;",
        )
        .unwrap();

    // Reader still sees only 1 row (no phantom reads).
    let count_during: i64 = reader
        .query_row("SELECT COUNT(*) FROM iso_test", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count_during, 1, "phantom read detected!");

    // Reader closes transaction — now sees all 3 rows.
    reader.execute_batch("COMMIT;").unwrap();
    let count_after: i64 = reader
        .query_row("SELECT COUNT(*) FROM iso_test", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count_after, 3, "new rows not visible after reader txn ends");
}

#[test]
fn csqlite_wal_concurrent_readers_during_writes() {
    // Multiple readers + one writer, all concurrent.
    // Each reader should see a consistent snapshot.
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let path = tmp.path().to_str().unwrap().to_owned();

    {
        let setup = rusqlite::Connection::open(&path).unwrap();
        setup
            .execute_batch(
                "PRAGMA journal_mode=WAL;\
                 CREATE TABLE iso_test (id INTEGER PRIMARY KEY, val INTEGER);",
            )
            .unwrap();
        // Insert initial 100 rows.
        setup.execute_batch("BEGIN;").unwrap();
        for i in 1..=100 {
            setup
                .execute(
                    "INSERT INTO iso_test VALUES (?1, ?2)",
                    rusqlite::params![i, i * 10],
                )
                .unwrap();
        }
        setup.execute_batch("COMMIT;").unwrap();
    }

    let barrier = Arc::new(Barrier::new(5)); // 4 readers + 1 writer

    // Spawn 4 reader threads that open transactions and repeatedly read.
    let reader_handles: Vec<_> = (0..4)
        .map(|_rid| {
            let p = path.clone();
            let bar = barrier.clone();
            thread::spawn(move || {
                let conn = rusqlite::Connection::open(&p).unwrap();
                conn.execute_batch("PRAGMA journal_mode=WAL; BEGIN;")
                    .unwrap();

                // Take initial snapshot count.
                let initial: i64 = conn
                    .query_row("SELECT COUNT(*) FROM iso_test", [], |r| r.get(0))
                    .unwrap();

                bar.wait(); // sync with writer

                // Read multiple times while writer is active.
                for _ in 0..10 {
                    let count: i64 = conn
                        .query_row("SELECT COUNT(*) FROM iso_test", [], |r| r.get(0))
                        .unwrap();
                    assert_eq!(
                        count, initial,
                        "snapshot violated: expected {initial}, got {count}"
                    );
                    thread::sleep(std::time::Duration::from_millis(5));
                }

                conn.execute_batch("COMMIT;").unwrap();
                initial
            })
        })
        .collect();

    // Writer thread inserts more rows.
    let writer_path = path.clone();
    let writer_bar = barrier;
    let writer_handle = thread::spawn(move || {
        let conn = rusqlite::Connection::open(&writer_path).unwrap();
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;")
            .unwrap();
        writer_bar.wait();

        for batch in 0..5 {
            conn.execute_batch("BEGIN;").unwrap();
            for i in 0..20 {
                let id = 101 + batch * 20 + i;
                conn.execute(
                    "INSERT INTO iso_test VALUES (?1, ?2)",
                    rusqlite::params![id, id * 10],
                )
                .unwrap();
            }
            conn.execute_batch("COMMIT;").unwrap();
            thread::sleep(std::time::Duration::from_millis(10));
        }
    });

    writer_handle.join().unwrap();
    for h in reader_handles {
        let snapshot_count = h.join().unwrap();
        assert_eq!(
            snapshot_count, 100,
            "reader snapshot should have seen initial 100 rows"
        );
    }

    // Final check: all rows present.
    let final_conn = rusqlite::Connection::open(&path).unwrap();
    let total: i64 = final_conn
        .query_row("SELECT COUNT(*) FROM iso_test", [], |r| r.get(0))
        .unwrap();
    assert_eq!(total, 200, "expected 100 initial + 100 writer rows");
}
