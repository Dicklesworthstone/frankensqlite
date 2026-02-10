//! Correctness test: concurrent multi-thread writes with logical equivalence.
//!
//! Bead: bd-244z
//!
//! Spawns multiple writer threads on C SQLite (via rusqlite, using a temp
//! file with WAL mode), then verifies FrankenSQLite produces the same
//! logical result when executing the same operations sequentially.
//!
//! FrankenSQLite's MVCC concurrent writer path is not yet wired to the
//! persistence layer, so this test validates **logical equivalence**: the
//! final set of rows produced by the same set of operations must be
//! identical regardless of execution order or concurrency model.
//!
//! For C SQLite, true multi-threaded concurrent writes are exercised
//! (each thread opens its own connection to a shared WAL-mode file).

use std::sync::{Arc, Barrier};
use std::thread;

// ─── Helpers ───────────────────────────────────────────────────────────

/// Query all rows from the test table, returning sorted by id.
fn query_sorted(conn: &rusqlite::Connection) -> Vec<(i64, String, i64)> {
    let mut stmt = conn
        .prepare("SELECT id, name, val FROM concurrent_test ORDER BY id")
        .unwrap();
    stmt.query_map([], |row| {
        Ok((
            row.get::<_, i64>(0).unwrap(),
            row.get::<_, String>(1).unwrap(),
            row.get::<_, i64>(2).unwrap(),
        ))
    })
    .unwrap()
    .collect::<Result<Vec<_>, _>>()
    .unwrap()
}

/// Query all rows from the FrankenSQLite test table.
fn frank_query_sorted(conn: &fsqlite::Connection) -> Vec<(i64, String, i64)> {
    let rows = conn
        .query("SELECT id, name, val FROM concurrent_test ORDER BY id")
        .unwrap();
    rows.iter()
        .map(|r| {
            let vals = r.values();
            let id = match &vals[0] {
                fsqlite_types::value::SqliteValue::Integer(i) => *i,
                other => {
                    assert!(
                        matches!(other, fsqlite_types::value::SqliteValue::Integer(_)),
                        "expected Integer for id, got {other:?}"
                    );
                    0
                }
            };
            let name = match &vals[1] {
                fsqlite_types::value::SqliteValue::Text(s) => s.clone(),
                other => {
                    assert!(
                        matches!(other, fsqlite_types::value::SqliteValue::Text(_)),
                        "expected Text for name, got {other:?}"
                    );
                    String::new()
                }
            };
            let val = match &vals[2] {
                fsqlite_types::value::SqliteValue::Integer(i) => *i,
                other => {
                    assert!(
                        matches!(other, fsqlite_types::value::SqliteValue::Integer(_)),
                        "expected Integer for val, got {other:?}"
                    );
                    0
                }
            };
            (id, name, val)
        })
        .collect()
}

/// Generate INSERT statements for a thread's non-overlapping key range.
fn gen_thread_inserts(thread_id: usize, count: usize, range_size: usize) -> Vec<String> {
    let base = thread_id * range_size;
    (0..count)
        .map(|i| {
            let id = base + i;
            let name = format!("t{thread_id}_row{i}");
            #[allow(clippy::cast_possible_wrap)]
            let val = (id * 7 + 13) as i64;
            format!("INSERT INTO concurrent_test VALUES ({id}, '{name}', {val})")
        })
        .collect()
}

// ─── Tests ─────────────────────────────────────────────────────────────

#[test]
fn concurrent_writes_2_threads_disjoint_keys() {
    concurrent_writes_n_threads(2, 500);
}

#[test]
fn concurrent_writes_4_threads_disjoint_keys() {
    concurrent_writes_n_threads(4, 250);
}

#[test]
fn concurrent_writes_8_threads_disjoint_keys() {
    concurrent_writes_n_threads(8, 125);
}

fn concurrent_writes_n_threads(n_threads: usize, ops_per_thread: usize) {
    let range_size = 10_000; // non-overlapping ranges
    let total_expected = n_threads * ops_per_thread;

    // ── C SQLite: true concurrent writes via temp file ──
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let db_path = tmp.path().to_str().unwrap().to_owned();

    {
        let setup = rusqlite::Connection::open(&db_path).unwrap();
        setup
            .execute_batch(
                "PRAGMA journal_mode=WAL;\
                 CREATE TABLE concurrent_test (id INTEGER PRIMARY KEY, name TEXT, val INTEGER);",
            )
            .unwrap();
    }

    let barrier = Arc::new(Barrier::new(n_threads));
    let handles: Vec<_> = (0..n_threads)
        .map(|tid| {
            let path = db_path.clone();
            let bar = barrier.clone();
            let stmts = gen_thread_inserts(tid, ops_per_thread, range_size);
            thread::spawn(move || {
                let conn = rusqlite::Connection::open(&path).unwrap();
                conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;")
                    .unwrap();
                bar.wait();
                conn.execute_batch("BEGIN;").unwrap();
                for sql in &stmts {
                    conn.execute(sql, []).unwrap();
                }
                conn.execute_batch("COMMIT;").unwrap();
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    let csqlite_conn = rusqlite::Connection::open(&db_path).unwrap();
    let csqlite_rows = query_sorted(&csqlite_conn);
    assert_eq!(
        csqlite_rows.len(),
        total_expected,
        "C SQLite row count mismatch"
    );

    // ── FrankenSQLite: sequential execution (same operations) ──
    let frank = fsqlite::Connection::open(":memory:").unwrap();
    frank
        .execute("CREATE TABLE concurrent_test (id INTEGER PRIMARY KEY, name TEXT, val INTEGER)")
        .unwrap();

    for tid in 0..n_threads {
        let stmts = gen_thread_inserts(tid, ops_per_thread, range_size);
        frank.execute("BEGIN").unwrap();
        for sql in &stmts {
            frank.execute(sql).unwrap();
        }
        frank.execute("COMMIT").unwrap();
    }

    let frank_rows = frank_query_sorted(&frank);
    assert_eq!(
        frank_rows.len(),
        total_expected,
        "FrankenSQLite row count mismatch"
    );

    // ── Compare logical equivalence ──
    assert_eq!(
        csqlite_rows,
        frank_rows,
        "logical equivalence failed: {n_threads} threads x {ops_per_thread} ops\n  \
         csqlite has {} rows, fsqlite has {} rows",
        csqlite_rows.len(),
        frank_rows.len()
    );
}

#[test]
fn concurrent_writes_verify_no_data_loss() {
    // 4 threads, 200 ops each, verify every single row is present.
    let n_threads = 4;
    let ops_per_thread = 200;
    let range_size = 10_000;

    let tmp = tempfile::NamedTempFile::new().unwrap();
    let db_path = tmp.path().to_str().unwrap().to_owned();

    {
        let setup = rusqlite::Connection::open(&db_path).unwrap();
        setup
            .execute_batch(
                "PRAGMA journal_mode=WAL;\
                 CREATE TABLE concurrent_test (id INTEGER PRIMARY KEY, name TEXT, val INTEGER);",
            )
            .unwrap();
    }

    let barrier = Arc::new(Barrier::new(n_threads));
    let handles: Vec<_> = (0..n_threads)
        .map(|tid| {
            let path = db_path.clone();
            let bar = barrier.clone();
            let stmts = gen_thread_inserts(tid, ops_per_thread, range_size);
            thread::spawn(move || -> rusqlite::Result<()> {
                let conn = rusqlite::Connection::open(&path)?;
                conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;")?;
                bar.wait();
                for sql in &stmts {
                    loop {
                        match conn.execute(sql, []) {
                            Ok(_) => break,
                            Err(e) if e.to_string().contains("database is locked") => {
                                thread::sleep(std::time::Duration::from_millis(1));
                            }
                            Err(e) => return Err(e),
                        }
                    }
                }
                Ok(())
            })
        })
        .collect();

    for h in handles {
        let r = h.join().unwrap();
        assert!(r.is_ok(), "worker thread error: {r:?}");
    }

    let conn = rusqlite::Connection::open(&db_path).unwrap();
    let rows = query_sorted(&conn);

    // Verify every expected row is present.
    for tid in 0..n_threads {
        for i in 0..ops_per_thread {
            #[allow(clippy::cast_possible_wrap)]
            let expected_id = (tid * range_size + i) as i64;
            assert!(
                rows.iter().any(|(id, _, _)| *id == expected_id),
                "missing row id={expected_id} (thread={tid}, offset={i})"
            );
        }
    }

    assert_eq!(rows.len(), n_threads * ops_per_thread);
}
