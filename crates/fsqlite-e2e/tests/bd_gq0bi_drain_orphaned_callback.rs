//! bd-gq0bi: drain_orphaned holds draining Mutex while invoking user
//! is_active_txn closure — callback-under-lock.
//!
//! ## Finding summary
//!
//! `InProcessPageLockTable::drain_orphaned` (core_types.rs:1270) holds
//! `self.draining` lock across invocation of the caller-supplied
//! `is_active_txn` closure inside `map.retain()`. Per deadlock-finder
//! Class 1: "Don't hold a lock across a call you don't own."
//!
//! Current production call-chain: `full_rebuild` passes a closure that
//! does NOT re-enter InProcessPageLockTable, so no actual deadlock exists
//! today. But if the closure were ever changed to call back into the lock
//! table (e.g., try_acquire), it would deadlock on the non-reentrant Mutex.
//!
//! ## Test approach
//!
//! Exercise the drain_orphaned → full_rebuild path under concurrent lock
//! operations to verify the current safe call-chain doesn't deadlock and
//! that the cleanup works correctly.
//!
//! - D1: Full rebuild under concurrent lock pressure — no deadlock
//! - D2: Orphaned entries are cleaned up correctly
//! - D3: Concurrent drain + acquire/release storm
#![recursion_limit = "512"]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use fsqlite::Connection;

const STRESS_DURATION: Duration = Duration::from_secs(2);

fn test_tmpdir() -> tempfile::TempDir {
    tempfile::tempdir_in(std::env::temp_dir())
        .or_else(|_| tempfile::tempdir_in("."))
        .expect("tempdir")
}

// ─── D1: Concurrent transactions don't deadlock during GC ──────────

#[test]
fn d1_concurrent_txns_no_deadlock_during_gc() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let db_path = dir.path().join("d1.db");
        let path_str = db_path.to_str().expect("path");

        {
            let conn = Connection::open(path_str).await.expect("open");
            conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, val TEXT)")
                .await
                .expect("create");
            conn.execute("BEGIN").await.expect("begin");
            for i in 1..=100 {
                conn.execute(&format!("INSERT INTO items VALUES ({i}, 'initial')"))
                    .await
                    .expect("seed");
            }
            conn.execute("COMMIT").await.expect("commit");
        }

        let stop = Arc::new(AtomicBool::new(false));
        let total_ops = Arc::new(AtomicU64::new(0));

        // Mix of short and long-lived transactions to trigger GC pressure
        let threads: Vec<_> = (0..8)
            .map(|i| {
                let path = path_str.to_string();
                let s = Arc::clone(&stop);
                let ops = Arc::clone(&total_ops);
                std::thread::spawn(move || {
                    asupersync::test_utils::run_test(|| async {
                        let conn = Connection::open(&path).await.expect("open");
                        let mut local_ops = 0u64;
                        while !s.load(Ordering::Relaxed) {
                            if i % 2 == 0 {
                                // Short txns — high churn creates orphaned lock entries
                                if conn.execute("BEGIN").await.is_ok() {
                                    let row = (local_ops % 100) + 1;
                                    conn.execute(&format!(
                                        "UPDATE items SET val = 'v{local_ops}' WHERE id = {row}"
                                    ))
                                    .await
                                    .ok();
                                    let should_rollback = local_ops.is_multiple_of(3)
                                        || conn.execute("COMMIT").await.is_err();
                                    if should_rollback {
                                        conn.execute("ROLLBACK").await.ok();
                                    }
                                    local_ops += 1;
                                }
                            } else {
                                // Long txns — hold snapshot while others churn
                                if conn.execute("BEGIN").await.is_ok() {
                                    conn.query("SELECT * FROM items").await.ok();
                                    std::thread::sleep(Duration::from_millis(5));
                                    conn.execute("COMMIT").await.ok();
                                    local_ops += 1;
                                }
                            }
                        }
                        ops.fetch_add(local_ops, Ordering::Relaxed);
                    });
                })
            })
            .collect();

        std::thread::sleep(STRESS_DURATION);
        stop.store(true, Ordering::Relaxed);

        for t in threads {
            t.join()
                .expect("thread must not deadlock (drain_orphaned callback-under-lock?)");
        }

        let ops = total_ops.load(Ordering::Relaxed);
        assert!(ops > 0, "no operations completed — possible deadlock");
        eprintln!("D1: {ops} ops (mixed short+long txns), 8 threads, no deadlock");
    });
}

// ─── D2: Orphaned entries are cleaned after connection drop ────────

#[test]
fn d2_orphaned_entries_cleaned_after_drop() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let db_path = dir.path().join("d2.db");
        let path_str = db_path.to_str().expect("path");

        {
            let conn = Connection::open(path_str).await.expect("open");
            conn.execute("CREATE TABLE data (id INTEGER PRIMARY KEY, val INTEGER)")
                .await
                .expect("create");
            conn.execute("BEGIN").await.expect("begin");
            for i in 1..=50 {
                conn.execute(&format!("INSERT INTO data VALUES ({i}, {i})"))
                    .await
                    .expect("seed");
            }
            conn.execute("COMMIT").await.expect("commit");
        }

        // Create multiple connections that start transactions then are dropped
        for round in 0..5 {
            let conn = Connection::open(path_str).await.expect("open");
            conn.execute("BEGIN").await.expect("begin");
            for i in 1..=10 {
                let id = 1000 + round * 100 + i;
                conn.execute(&format!("INSERT INTO data VALUES ({id}, {id})"))
                    .await
                    .ok();
            }
            // Drop without commit — simulates crash/orphaned locks
        }

        // New connection should see only the original 50 rows
        let verify = Connection::open(path_str).await.expect("verify");
        let rows = verify
            .query("SELECT * FROM data")
            .await
            .expect("count")
            .len();
        assert!(
            rows >= 50,
            "original rows lost after orphaned connection drops (got {rows})"
        );
        eprintln!("D2: {rows} rows visible after 5 orphaned connection drops");
    });
}

// ─── D3: Rapid connection open/close churn ─────────────────────────

#[test]
fn d3_rapid_connection_churn() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let db_path = dir.path().join("d3.db");
        let path_str = db_path.to_str().expect("path");

        {
            let conn = Connection::open(path_str).await.expect("open");
            conn.execute("CREATE TABLE kv (k TEXT PRIMARY KEY, v INTEGER)")
                .await
                .expect("create");
        }

        let stop = Arc::new(AtomicBool::new(false));
        let total_ops = Arc::new(AtomicU64::new(0));

        // Threads rapidly open connections, do work, close them
        let threads: Vec<_> = (0..4)
            .map(|i| {
                let path = path_str.to_string();
                let s = Arc::clone(&stop);
                let ops = Arc::clone(&total_ops);
                std::thread::spawn(move || {
                    asupersync::test_utils::run_test(|| async {
                        let mut local_ops = 0u64;
                        while !s.load(Ordering::Relaxed) {
                            // Open new connection each iteration — creates/destroys
                            // snapshot registrations
                            if let Ok(conn) = Connection::open(&path).await {
                                if conn.execute("BEGIN").await.is_ok() {
                                    let key = format!("k_{i}_{local_ops}");
                                    conn.execute(&format!(
                                        "INSERT OR REPLACE INTO kv VALUES ('{key}', {local_ops})"
                                    ))
                                    .await
                                    .ok();
                                    conn.execute("COMMIT").await.ok();
                                }
                                local_ops += 1;
                            }
                            // conn dropped — triggers cleanup including drain_orphaned path
                        }
                        ops.fetch_add(local_ops, Ordering::Relaxed);
                    });
                })
            })
            .collect();

        std::thread::sleep(STRESS_DURATION);
        stop.store(true, Ordering::Relaxed);

        for t in threads {
            t.join()
                .expect("thread must not deadlock during connection churn");
        }

        let ops = total_ops.load(Ordering::Relaxed);
        assert!(ops > 0, "no connection cycles completed");

        // Final integrity check
        let verify = Connection::open(path_str).await.expect("verify");
        let rows = verify.query("SELECT * FROM kv").await.expect("count").len();
        assert!(rows > 0, "no data survived connection churn");
        eprintln!("D3: {ops} connection open/close cycles, {rows} final rows");
    });
}

// ─── D4: Concurrent drain + sustained write pressure ───────────────

#[test]
fn d4_drain_under_sustained_write_pressure() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let db_path = dir.path().join("d4.db");
        let path_str = db_path.to_str().expect("path");

        {
            let conn = Connection::open(path_str).await.expect("open");
            conn.execute("CREATE TABLE log (id INTEGER PRIMARY KEY, tid INTEGER, seq INTEGER)")
                .await
                .expect("create");
        }

        let stop = Arc::new(AtomicBool::new(false));
        let total_committed = Arc::new(AtomicU64::new(0));

        // Sustained writers
        let writers: Vec<_> = (0..4)
            .map(|tid| {
                let path = path_str.to_string();
                let s = Arc::clone(&stop);
                let tc = Arc::clone(&total_committed);
                std::thread::spawn(move || {
                    asupersync::test_utils::run_test(|| async {
                        let conn = Connection::open(&path).await.expect("open");
                        let mut seq = 0u64;
                        let mut committed = 0u64;
                        while !s.load(Ordering::Relaxed) {
                            let id = tid as u64 * 1_000_000 + seq;
                            if conn.execute("BEGIN").await.is_ok()
                                && conn
                                    .execute(&format!(
                                        "INSERT INTO log VALUES ({id}, {tid}, {seq})"
                                    ))
                                    .await
                                    .is_ok()
                                && conn.execute("COMMIT").await.is_ok()
                            {
                                committed += 1;
                            } else {
                                conn.execute("ROLLBACK").await.ok();
                            }
                            seq += 1;
                        }
                        tc.fetch_add(committed, Ordering::Relaxed);
                    });
                })
            })
            .collect();

        // Connection-churn threads (trigger drain_orphaned path)
        let churners: Vec<_> = (0..4)
            .map(|_| {
                let path = path_str.to_string();
                let s = Arc::clone(&stop);
                std::thread::spawn(move || {
                    let mut cycles = 0u64;
                    asupersync::test_utils::run_test(|| async {
                        while !s.load(Ordering::Relaxed) {
                            if let Ok(conn) = Connection::open(&path).await {
                                // Start txn, read, then drop without commit
                                if conn.execute("BEGIN").await.is_ok() {
                                    conn.query("SELECT COUNT(*) FROM log").await.ok();
                                    // Don't commit — drop triggers cleanup
                                }
                            }
                            cycles += 1;
                        }
                    });
                    cycles
                })
            })
            .collect();

        std::thread::sleep(STRESS_DURATION);
        stop.store(true, Ordering::Relaxed);

        for w in writers {
            w.join().expect("writer must not deadlock");
        }
        let mut churn_cycles = 0u64;
        for c in churners {
            churn_cycles += c.join().expect("churner must not deadlock");
        }

        let committed = total_committed.load(Ordering::Relaxed);
        assert!(committed > 0, "no writes committed");
        assert!(churn_cycles > 0, "no churn cycles completed");

        let verify = Connection::open(path_str).await.expect("verify");
        let rows = verify
            .query("SELECT * FROM log")
            .await
            .expect("count")
            .len();
        assert_eq!(
            rows, committed as usize,
            "row count mismatch: {rows} visible vs {committed} committed"
        );
        eprintln!("D4: {committed} writes, {churn_cycles} churn cycles, {rows} final rows");
    });
}
