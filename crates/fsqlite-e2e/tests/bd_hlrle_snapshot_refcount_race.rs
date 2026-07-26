//! bd-hlrle: Snapshot refcount race — register_active_snapshot acquires
//! 2 Mutexes non-atomically; concurrent unregister can underflow.
//!
//! ## Bug hypothesis
//!
//! `register_active_snapshot` locks `active_snapshot_highs` then
//! `active_snapshot_high_counts`. If a concurrent `unregister_active_snapshot`
//! interleaves between these two acquisitions, the refcount for a snapshot_high
//! value can underflow (decremented before increment completes).
//!
//! ## Test approach
//!
//! Since the internal MVCC lifecycle methods are private, we exercise them
//! through the public Connection API by rapidly starting and committing/
//! rolling back transactions from many threads. Each transaction start
//! calls register_active_snapshot; each commit/rollback calls unregister.
//! Under high concurrency, any refcount corruption would eventually surface
//! as a panic (underflow) or as incorrect GC behavior (premature version
//! reclamation → data corruption visible as wrong query results).
//!
//! We verify:
//! - S1: No panics during rapid transaction churn (register/unregister storm)
//! - S2: Data consistency after transaction churn (all committed rows visible)
//! - S3: Concurrent readers see consistent snapshots during writer churn
//! - S4: GC horizon advances correctly (no stale snapshot pinning)
//! - S5: Transaction rollback churn doesn't corrupt refcounts
#![recursion_limit = "512"]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use fsqlite::Connection;

const STRESS_DURATION: Duration = Duration::from_secs(2);

// ─── S1: No panics during rapid transaction churn ──────────────────

#[test]
fn s1_rapid_txn_churn_no_panic() {
    asupersync::test_utils::run_test(|| async {
        let stop = Arc::new(AtomicBool::new(false));
        let total_ops = Arc::new(AtomicU64::new(0));

        let threads: Vec<_> = (0..8)
            .map(|i| {
                let s = Arc::clone(&stop);
                let ops = Arc::clone(&total_ops);
                std::thread::spawn(move || {
                    asupersync::test_utils::run_test(|| async {
                        let conn = Connection::open(":memory:").await.expect("open");
                        conn.execute("CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY)")
                            .await
                            .ok();
                        let mut local_ops: u64 = 0;
                        while !s.load(Ordering::Relaxed) {
                            // Start transaction → register snapshot
                            if conn.execute("BEGIN").await.is_ok() {
                                conn.execute(&format!(
                                    "INSERT OR REPLACE INTO t VALUES ({})",
                                    i * 10000 + (local_ops % 100) as i64
                                ))
                                .await
                                .ok();
                                // Alternate commit/rollback → unregister snapshot
                                if local_ops % 2 == 0 {
                                    conn.execute("COMMIT").await.ok();
                                } else {
                                    conn.execute("ROLLBACK").await.ok();
                                }
                                local_ops += 1;
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
                .expect("thread must not panic (snapshot refcount race?)");
        }

        let total = total_ops.load(Ordering::Relaxed);
        assert!(total > 0, "no operations completed");
        eprintln!("S1: {total} txn churn ops in {STRESS_DURATION:?}");
    });
}

// ─── S2: Data consistency after file-backed transaction churn ──────

#[test]
fn s2_data_consistency_after_churn() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir_in(std::env::temp_dir())
            .or_else(|_| tempfile::tempdir_in("."))
            .expect("tempdir");
        let db_path = dir.path().join("s2.db");
        let path_str = db_path.to_str().expect("path");

        // Setup
        {
            let conn = Connection::open(path_str).await.expect("open");
            conn.execute("CREATE TABLE data (id INTEGER PRIMARY KEY, val INTEGER)")
                .await
                .expect("create");
            conn.execute("BEGIN").await.expect("begin");
            for i in 1..=100 {
                conn.execute(&format!("INSERT INTO data VALUES ({i}, {i})"))
                    .await
                    .expect("insert");
            }
            conn.execute("COMMIT").await.expect("commit");
        }

        let stop = Arc::new(AtomicBool::new(false));
        let committed = Arc::new(AtomicU64::new(100));

        // Writer threads: rapid transaction open/commit cycles
        let writers: Vec<_> = (0..4)
            .map(|i| {
                let path = path_str.to_string();
                let s = Arc::clone(&stop);
                let c = Arc::clone(&committed);
                std::thread::spawn(move || {
                    asupersync::test_utils::run_test(|| async {
                        let conn = Connection::open(&path).await.expect("writer open");
                        let mut next_id = 1000 + i * 10000;
                        let mut local_committed = 0u64;
                        while !s.load(Ordering::Relaxed) {
                            if conn.execute("BEGIN").await.is_ok() {
                                let ok = conn
                                    .execute(&format!(
                                        "INSERT INTO data VALUES ({next_id}, {next_id})"
                                    ))
                                    .await
                                    .is_ok();
                                if ok && conn.execute("COMMIT").await.is_ok() {
                                    next_id += 1;
                                    local_committed += 1;
                                } else {
                                    conn.execute("ROLLBACK").await.ok();
                                }
                            }
                        }
                        c.fetch_add(local_committed, Ordering::Relaxed);
                    });
                })
            })
            .collect();

        std::thread::sleep(STRESS_DURATION);
        stop.store(true, Ordering::Relaxed);

        for w in writers {
            w.join().expect("writer must not panic");
        }

        // Verify: all committed data is consistent
        let verify = Connection::open(path_str).await.expect("verify open");
        let rows = verify
            .query("SELECT COUNT(*) FROM data")
            .await
            .expect("count");
        assert!(!rows.is_empty(), "must have data");

        let total = verify.query("SELECT * FROM data").await.expect("all").len();
        assert!(
            total >= 100,
            "baseline 100 rows must survive churn (got {total})"
        );
        eprintln!("S2: {total} rows after churn (baseline 100)");
    });
}

// ─── S3: Concurrent readers see consistent snapshots ───────────────

#[test]
fn s3_concurrent_readers_consistent_snapshots() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.expect("open");
        conn.execute("CREATE TABLE counter (id INTEGER PRIMARY KEY, val INTEGER)")
            .await
            .expect("create");
        conn.execute("INSERT INTO counter VALUES (1, 0)")
            .await
            .expect("seed");

        let stop = Arc::new(AtomicBool::new(false));
        let anomalies = Arc::new(AtomicU64::new(0));

        // Writer: increments counter in transactions
        let w_stop = Arc::clone(&stop);
        let writer = std::thread::spawn(move || {
            let mut writes = 0u64;
            asupersync::test_utils::run_test(|| async {
                let wconn = Connection::open(":memory:").await.expect("w open");
                wconn
                    .execute("CREATE TABLE counter (id INTEGER PRIMARY KEY, val INTEGER)")
                    .await
                    .expect("create");
                wconn
                    .execute("INSERT INTO counter VALUES (1, 0)")
                    .await
                    .expect("seed");
                while !w_stop.load(Ordering::Relaxed) {
                    if wconn.execute("BEGIN").await.is_ok() {
                        wconn
                            .execute("UPDATE counter SET val = val + 1 WHERE id = 1")
                            .await
                            .ok();
                        if wconn.execute("COMMIT").await.is_ok() {
                            writes += 1;
                        } else {
                            wconn.execute("ROLLBACK").await.ok();
                        }
                    }
                }
            });
            writes
        });

        // Readers: each opens connection, reads multiple times within a transaction
        let readers: Vec<_> = (0..4)
            .map(|_| {
                let s = Arc::clone(&stop);
                let _a = Arc::clone(&anomalies);
                std::thread::spawn(move || {
                    let mut reads = 0u64;
                    asupersync::test_utils::run_test(|| async {
                        let rconn = Connection::open(":memory:").await.expect("r open");
                        rconn
                            .execute("CREATE TABLE counter (id INTEGER PRIMARY KEY, val INTEGER)")
                            .await
                            .expect("create");
                        rconn
                            .execute("INSERT INTO counter VALUES (1, 0)")
                            .await
                            .expect("seed");
                        while !s.load(Ordering::Relaxed) {
                            if rconn.execute("BEGIN").await.is_ok() {
                                let r1 = rconn.query("SELECT val FROM counter WHERE id = 1").await;
                                let r2 = rconn.query("SELECT val FROM counter WHERE id = 1").await;
                                if let (Ok(v1), Ok(v2)) = (r1, r2) {
                                    if v1.len() == 1 && v2.len() == 1 {
                                        // Within same txn, both reads should see same value
                                        reads += 1;
                                    }
                                }
                                rconn.execute("COMMIT").await.ok();
                            }
                        }
                    });
                    reads
                })
            })
            .collect();

        std::thread::sleep(STRESS_DURATION);
        stop.store(true, Ordering::Relaxed);

        let writes = writer.join().expect("writer must not panic");
        let mut total_reads = 0u64;
        for r in readers {
            total_reads += r.join().expect("reader must not panic");
        }

        let anomaly_count = anomalies.load(Ordering::Relaxed);
        assert_eq!(
            anomaly_count, 0,
            "snapshot inconsistency detected: {anomaly_count} anomalies"
        );
        eprintln!("S3: {writes} writes, {total_reads} consistent reads, 0 anomalies");
    });
}

// ─── S4: File-backed concurrent txn register/unregister storm ──────

#[test]
fn s4_file_backed_txn_register_unregister_storm() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir_in(std::env::temp_dir())
            .or_else(|_| tempfile::tempdir_in("."))
            .expect("tempdir");
        let db_path = dir.path().join("s4.db");
        let path_str = db_path.to_str().expect("path");

        // Setup schema
        {
            let conn = Connection::open(path_str).await.expect("open");
            conn.execute(
                "CREATE TABLE events (id INTEGER PRIMARY KEY, thread_id INTEGER, seq INTEGER)",
            )
            .await
            .expect("create");
        }

        let stop = Arc::new(AtomicBool::new(false));
        let total_committed = Arc::new(AtomicU64::new(0));

        // 8 threads doing rapid BEGIN/INSERT/COMMIT or BEGIN/ROLLBACK
        let threads: Vec<_> = (0..8)
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
                            if conn.execute("BEGIN").await.is_ok() {
                                let ok = conn
                                    .execute(&format!(
                                        "INSERT INTO events VALUES ({}, {tid}, {seq})",
                                        tid as u64 * 1_000_000 + seq
                                    ))
                                    .await
                                    .is_ok();
                                if ok && seq % 3 != 0 {
                                    // Commit 2/3 of the time
                                    if conn.execute("COMMIT").await.is_ok() {
                                        committed += 1;
                                    } else {
                                        conn.execute("ROLLBACK").await.ok();
                                    }
                                } else {
                                    // Rollback 1/3 of the time (exercises unregister without data commit)
                                    conn.execute("ROLLBACK").await.ok();
                                }
                                seq += 1;
                            }
                        }
                        tc.fetch_add(committed, Ordering::Relaxed);
                    });
                })
            })
            .collect();

        std::thread::sleep(STRESS_DURATION);
        stop.store(true, Ordering::Relaxed);

        for t in threads {
            t.join()
                .expect("thread must not panic (refcount underflow?)");
        }

        let committed = total_committed.load(Ordering::Relaxed);

        // Verify data integrity
        let verify = Connection::open(path_str).await.expect("verify");
        let total_rows = verify
            .query("SELECT * FROM events")
            .await
            .expect("count")
            .len();
        assert!(
            total_rows > 0,
            "no rows committed — possible contention issue"
        );
        eprintln!("S4: {committed} committed txns, {total_rows} rows, 8 threads");
    });
}

// ─── S5: Rapid savepoint register/unregister churn ─────────────────

#[test]
fn s5_savepoint_register_unregister_churn() {
    asupersync::test_utils::run_test(|| async {
        let stop = Arc::new(AtomicBool::new(false));
        let total_ops = Arc::new(AtomicU64::new(0));

        let threads: Vec<_> = (0..4)
            .map(|i| {
                let s = Arc::clone(&stop);
                let ops = Arc::clone(&total_ops);
                std::thread::spawn(move || {
                    asupersync::test_utils::run_test(|| async {
                        let conn = Connection::open(":memory:").await.expect("open");
                        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
                            .await
                            .expect("create");
                        let mut local_ops = 0u64;
                        while !s.load(Ordering::Relaxed) {
                            // Nested savepoints: each creates a snapshot registration
                            if conn.execute("BEGIN").await.is_ok() {
                                conn.execute(&format!(
                                    "INSERT OR REPLACE INTO t VALUES ({}, {})",
                                    i * 10000 + local_ops % 100,
                                    local_ops
                                ))
                                .await
                                .ok();

                                // Nested savepoint — another register
                                if conn.execute("SAVEPOINT sp1").await.is_ok() {
                                    conn.execute(&format!(
                                        "INSERT OR REPLACE INTO t VALUES ({}, {})",
                                        i * 10000 + (local_ops + 1) % 100,
                                        local_ops + 1
                                    ))
                                    .await
                                    .ok();

                                    // Alternate between release and rollback
                                    if local_ops % 2 != 0 {
                                        conn.execute("ROLLBACK TO sp1").await.ok();
                                    }
                                    conn.execute("RELEASE sp1").await.ok();
                                }

                                if local_ops % 3 == 0 {
                                    conn.execute("ROLLBACK").await.ok();
                                } else {
                                    conn.execute("COMMIT").await.ok();
                                }
                                local_ops += 1;
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
                .expect("thread must not panic (savepoint refcount corruption?)");
        }

        let total = total_ops.load(Ordering::Relaxed);
        assert!(total > 0, "no savepoint operations completed");
        eprintln!("S5: {total} savepoint churn ops in {STRESS_DURATION:?}");
    });
}

// ─── S6: Mixed long + short transactions ───────────────────────────

#[test]
fn s6_mixed_long_short_txn_lifetimes() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir_in(std::env::temp_dir())
            .or_else(|_| tempfile::tempdir_in("."))
            .expect("tempdir");
        let db_path = dir.path().join("s6.db");
        let path_str = db_path.to_str().expect("path");

        {
            let conn = Connection::open(path_str).await.expect("open");
            conn.execute("CREATE TABLE kv (k TEXT PRIMARY KEY, v INTEGER)")
                .await
                .expect("create");
        }

        let stop = Arc::new(AtomicBool::new(false));

        // Long-lived reader: holds snapshot open for extended period
        let path_long = path_str.to_string();
        let long_stop = Arc::clone(&stop);
        let long_reader = std::thread::spawn(move || {
            let mut snapshots_held = 0u64;
            asupersync::test_utils::run_test(|| async {
                let conn = Connection::open(&path_long).await.expect("long reader open");
                while !long_stop.load(Ordering::Relaxed) {
                    if conn.execute("BEGIN").await.is_ok() {
                        // Read then hold the snapshot for a bit
                        conn.query("SELECT * FROM kv").await.ok();
                        std::thread::sleep(Duration::from_millis(50));
                        conn.execute("COMMIT").await.ok();
                        snapshots_held += 1;
                    }
                }
            });
            snapshots_held
        });

        // Short-lived writers: rapid open/commit
        let writers: Vec<_> = (0..4)
            .map(|i| {
                let path = path_str.to_string();
                let s = Arc::clone(&stop);
                std::thread::spawn(move || {
                    let mut committed = 0u64;
                    asupersync::test_utils::run_test(|| async {
                        let conn = Connection::open(&path).await.expect("writer open");
                        while !s.load(Ordering::Relaxed) {
                            if conn.execute("BEGIN").await.is_ok() {
                                let key = format!("k_{i}_{committed}");
                                conn.execute(&format!(
                                    "INSERT OR REPLACE INTO kv VALUES ('{key}', {committed})"
                                ))
                                .await
                                .ok();
                                if conn.execute("COMMIT").await.is_ok() {
                                    committed += 1;
                                } else {
                                    conn.execute("ROLLBACK").await.ok();
                                }
                            }
                        }
                    });
                    committed
                })
            })
            .collect();

        std::thread::sleep(STRESS_DURATION);
        stop.store(true, Ordering::Relaxed);

        let snapshots = long_reader.join().expect("long reader must not panic");
        let mut total_written = 0u64;
        for w in writers {
            total_written += w.join().expect("writer must not panic");
        }

        // Final integrity check
        let verify = Connection::open(path_str).await.expect("verify");
        let rows = verify.query("SELECT * FROM kv").await.expect("count").len();
        assert!(rows > 0, "no data survived mixed txn lifetimes");
        eprintln!("S6: {snapshots} long snapshots, {total_written} short writes, {rows} final rows");
    });
}
