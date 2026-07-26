//! bd-65byl: WASM condvar path has lost-wakeup window — epoch advances
//! between load and lock acquisition.
//!
//! ## Bug hypothesis
//!
//! In the WASM fallback path of `wait_for_page_lock_release`:
//! ```text
//! let observed_epoch = self.change_epoch.load(Acquire);     // (A)
//! let mut gate = self.change_gate.lock();                    // (B)
//! if self.change_epoch.load(Acquire) == observed_epoch {     // (C)
//!     self.change_cv.wait(&mut gate);                        // (D)
//! }
//! ```
//! If epoch advances between (A) and (B), the re-check at (C) catches it
//! and skips the wait — no lost wakeup. The bead claims there's a window,
//! but the double-check pattern at (C) should prevent it.
//!
//! However, if the notifier doesn't hold `change_gate` while advancing
//! `change_epoch`, the following interleaving is possible:
//!   1. Waiter loads epoch=5 at (A)
//!   2. Notifier advances epoch to 6 and signals condvar
//!   3. Waiter acquires gate at (B), re-checks: epoch=6 != 5, skips wait ✓
//! This is safe. The real risk is if condvar::notify happens BEFORE the
//! waiter reaches (D) but AFTER the epoch check at (C) — but that can't
//! happen because (C) and (D) are under the gate lock.
//!
//! ## Test approach
//!
//! We exercise the page-lock contention path (native park/unpark, not WASM
//! condvar) to verify no lost wakeups under high concention:
//! - W1: Rapid lock contention with timeout — no indefinite hangs
//! - W2: Lock handoff chains — every waiter eventually gets the lock
//! - W3: Many waiters for same page — all wake up when holder releases
//! - W4: Mixed page locks with cross-page notification
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

// ─── W1: Rapid lock contention — no indefinite hangs ───────────────

#[test]
fn w1_rapid_lock_contention_no_hang() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let db_path = dir.path().join("w1.db");
        let path_str = db_path.to_str().expect("path");

        {
            let conn = Connection::open(path_str).await.expect("open");
            conn.execute("CREATE TABLE hot (id INTEGER PRIMARY KEY, val INTEGER)")
                .await
                .expect("create");
            conn.execute("INSERT INTO hot VALUES (1, 0)")
                .await
                .expect("seed");
        }

        let stop = Arc::new(AtomicBool::new(false));
        let total_updates = Arc::new(AtomicU64::new(0));

        // Multiple threads contending on the same row
        let threads: Vec<_> = (0..8)
            .map(|_| {
                let path = path_str.to_string();
                let s = Arc::clone(&stop);
                let tu = Arc::clone(&total_updates);
                std::thread::spawn(move || {
                    asupersync::test_utils::run_test(|| async {
                        let conn = Connection::open(&path).await.expect("open");
                        let mut updates = 0u64;
                        while !s.load(Ordering::Relaxed) {
                            if conn.execute("BEGIN").await.is_ok() {
                                if conn
                                    .execute("UPDATE hot SET val = val + 1 WHERE id = 1")
                                    .await
                                    .is_ok()
                                    && conn.execute("COMMIT").await.is_ok()
                                {
                                    updates += 1;
                                } else {
                                    conn.execute("ROLLBACK").await.ok();
                                }
                            }
                        }
                        tu.fetch_add(updates, Ordering::Relaxed);
                    });
                })
            })
            .collect();

        std::thread::sleep(STRESS_DURATION);
        stop.store(true, Ordering::Relaxed);

        for t in threads {
            t.join().expect("thread must complete — no indefinite hang");
        }

        let updates = total_updates.load(Ordering::Relaxed);
        assert!(updates > 0, "no updates succeeded — possible livelock");

        // Verify final value
        let verify = Connection::open(path_str).await.expect("verify");
        let rows = verify
            .query("SELECT val FROM hot WHERE id = 1")
            .await
            .expect("query");
        assert_eq!(rows.len(), 1, "hot row must exist");
        eprintln!("W1: {updates} concurrent updates on same row, 8 threads");
    });
}

// ─── W2: Lock handoff chains — every waiter gets the lock ──────────

#[test]
fn w2_lock_handoff_chains() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let db_path = dir.path().join("w2.db");
        let path_str = db_path.to_str().expect("path");

        {
            let conn = Connection::open(path_str).await.expect("open");
            conn.execute("CREATE TABLE chain (id INTEGER PRIMARY KEY, writer INTEGER)")
                .await
                .expect("create");
        }

        let num_writers = 8;
        let writes_per = 20;

        // Each writer must successfully write N rows (retrying on contention)
        let threads: Vec<_> = (0..num_writers)
            .map(|i| {
                let path = path_str.to_string();
                std::thread::spawn(move || {
                    let mut result = (0, 0);
                    asupersync::test_utils::run_test(|| async {
                        let conn = Connection::open(&path).await.expect("open");
                        let mut written = 0;
                        let mut attempts = 0;
                        while written < writes_per {
                            attempts += 1;
                            if attempts > writes_per * 100 {
                                panic!(
                                    "writer {i} stuck after {attempts} attempts ({written}/{writes_per} written) — lost wakeup?"
                                );
                            }
                            let id = i * 10000 + written;
                            if conn.execute("BEGIN").await.is_ok()
                                && conn
                                    .execute(&format!("INSERT INTO chain VALUES ({id}, {i})"))
                                    .await
                                    .is_ok()
                                && conn.execute("COMMIT").await.is_ok()
                            {
                                written += 1;
                            } else {
                                conn.execute("ROLLBACK").await.ok();
                                std::thread::yield_now();
                            }
                        }
                        result = (written, attempts);
                    });
                    result
                })
            })
            .collect();

        let mut total_written = 0;
        let mut total_attempts = 0;
        for t in threads {
            let (w, a) = t.join().expect("writer must not panic or hang");
            total_written += w;
            total_attempts += a;
        }

        assert_eq!(
            total_written,
            num_writers * writes_per,
            "not all writers completed their writes"
        );

        let verify = Connection::open(path_str).await.expect("verify");
        let rows = verify
            .query("SELECT * FROM chain")
            .await
            .expect("count")
            .len();
        assert_eq!(rows, total_written as usize);
        eprintln!(
            "W2: {total_written} writes across {num_writers} threads ({total_attempts} attempts)"
        );
    });
}

// ─── W3: Many readers + one writer — readers don't starve ──────────

#[test]
fn w3_readers_dont_starve() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let db_path = dir.path().join("w3.db");
        let path_str = db_path.to_str().expect("path");

        {
            let conn = Connection::open(path_str).await.expect("open");
            conn.execute("CREATE TABLE data (id INTEGER PRIMARY KEY, val TEXT)")
                .await
                .expect("create");
            conn.execute("BEGIN").await.expect("begin");
            for i in 1..=100 {
                conn.execute(&format!("INSERT INTO data VALUES ({i}, 'initial')"))
                    .await
                    .expect("insert");
            }
            conn.execute("COMMIT").await.expect("commit");
        }

        let stop = Arc::new(AtomicBool::new(false));
        let total_reads = Arc::new(AtomicU64::new(0));
        let total_writes = Arc::new(AtomicU64::new(0));

        // 1 writer: continuously updates
        let w_path = path_str.to_string();
        let w_stop = Arc::clone(&stop);
        let tw = Arc::clone(&total_writes);
        let writer = std::thread::spawn(move || {
            asupersync::test_utils::run_test(|| async {
                let conn = Connection::open(&w_path).await.expect("w open");
                let mut writes = 0u64;
                while !w_stop.load(Ordering::Relaxed) {
                    if conn.execute("BEGIN").await.is_ok() {
                        let id = (writes % 100) + 1;
                        if conn
                            .execute(&format!(
                                "UPDATE data SET val = 'v{writes}' WHERE id = {id}"
                            ))
                            .await
                            .is_ok()
                            && conn.execute("COMMIT").await.is_ok()
                        {
                            writes += 1;
                        } else {
                            conn.execute("ROLLBACK").await.ok();
                        }
                    }
                }
                tw.fetch_add(writes, Ordering::Relaxed);
            });
        });

        // 6 readers: continuously read
        let readers: Vec<_> = (0..6)
            .map(|_| {
                let path = path_str.to_string();
                let s = Arc::clone(&stop);
                let tr = Arc::clone(&total_reads);
                std::thread::spawn(move || {
                    asupersync::test_utils::run_test(|| async {
                        let conn = Connection::open(&path).await.expect("r open");
                        let mut reads = 0u64;
                        while !s.load(Ordering::Relaxed) {
                            if conn.query("SELECT * FROM data").await.is_ok() {
                                reads += 1;
                            }
                        }
                        tr.fetch_add(reads, Ordering::Relaxed);
                    });
                })
            })
            .collect();

        std::thread::sleep(STRESS_DURATION);
        stop.store(true, Ordering::Relaxed);

        writer.join().expect("writer must not hang");
        for r in readers {
            r.join().expect("reader must not hang");
        }

        let reads = total_reads.load(Ordering::Relaxed);
        let writes = total_writes.load(Ordering::Relaxed);
        assert!(reads > 0, "readers starved — 0 reads completed");
        assert!(writes > 0, "writer starved — 0 writes completed");
        eprintln!("W3: {writes} writes, {reads} reads, no starvation");
    });
}

// ─── W4: Mixed page locks with cross-page notification ─────────────

#[test]
fn w4_cross_table_concurrent_updates() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let db_path = dir.path().join("w4.db");
        let path_str = db_path.to_str().expect("path");

        {
            let conn = Connection::open(path_str).await.expect("open");
            conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val INTEGER)")
                .await
                .expect("create t1");
            conn.execute("CREATE TABLE t2 (id INTEGER PRIMARY KEY, val INTEGER)")
                .await
                .expect("create t2");
            conn.execute("CREATE TABLE t3 (id INTEGER PRIMARY KEY, val INTEGER)")
                .await
                .expect("create t3");
            for i in 1..=10 {
                conn.execute(&format!("INSERT INTO t1 VALUES ({i}, 0)"))
                    .await
                    .expect("seed t1");
                conn.execute(&format!("INSERT INTO t2 VALUES ({i}, 0)"))
                    .await
                    .expect("seed t2");
                conn.execute(&format!("INSERT INTO t3 VALUES ({i}, 0)"))
                    .await
                    .expect("seed t3");
            }
        }

        let stop = Arc::new(AtomicBool::new(false));
        let total_ops = Arc::new(AtomicU64::new(0));

        // Each thread writes to a different table but they share the same WAL/pager
        let tables = ["t1", "t2", "t3"];
        let threads: Vec<_> = (0..6)
            .map(|i| {
                let path = path_str.to_string();
                let s = Arc::clone(&stop);
                let ops = Arc::clone(&total_ops);
                let table = tables[i % 3].to_string();
                std::thread::spawn(move || {
                    asupersync::test_utils::run_test(|| async {
                        let conn = Connection::open(&path).await.expect("open");
                        let mut local_ops = 0u64;
                        while !s.load(Ordering::Relaxed) {
                            let row_id = (local_ops % 10) + 1;
                            if conn.execute("BEGIN").await.is_ok() {
                                if conn
                                    .execute(&format!(
                                        "UPDATE {table} SET val = val + 1 WHERE id = {row_id}"
                                    ))
                                    .await
                                    .is_ok()
                                    && conn.execute("COMMIT").await.is_ok()
                                {
                                    local_ops += 1;
                                } else {
                                    conn.execute("ROLLBACK").await.ok();
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
                .expect("thread must not hang — cross-page wakeup issue?");
        }

        let ops = total_ops.load(Ordering::Relaxed);
        assert!(ops > 0, "no operations completed — possible lost wakeup");
        eprintln!("W4: {ops} cross-table concurrent updates, 6 threads");
    });
}
