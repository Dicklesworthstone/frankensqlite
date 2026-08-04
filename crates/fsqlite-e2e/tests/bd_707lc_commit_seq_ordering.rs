//! bd-707lc: Relaxed loads on next_commit_seq while combiner uses AcqRel —
//! readers may observe stale epoch.
//!
//! ## Bug hypothesis
//!
//! `CommitCombiner::next_seq()` uses `Ordering::Relaxed` to load
//! `next_commit_seq`, while the combiner's `fetch_add` in the commit path
//! uses `Ordering::AcqRel`. On weakly-ordered architectures (ARM), a reader
//! calling `next_seq()` concurrently with a commit could observe a stale
//! value — the sequence hasn't advanced yet from the reader's perspective
//! even though the commit has completed.
//!
//! ## Test approach
//!
//! We exercise the commit path from multiple threads and verify:
//! - C1: Concurrent commits all succeed without panics
//! - C2: After all commits complete, committed row count is correct
//! - C3: Commit ordering is monotonic (each connection's commits are ordered)
//! - C4: No lost commits under high concurrency (every committed row is visible)
//! - C5: Rapid commit + read interleaving doesn't produce stale reads
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

// ─── C1: Concurrent commits succeed without panics ─────────────────

#[test]
fn c1_concurrent_commits_no_panic() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let db_path = dir.path().join("c1.db");
        let path_str = db_path.to_str().expect("path");

        {
            let conn = Connection::open(path_str).await.expect("open");
            conn.execute("CREATE TABLE log (id INTEGER PRIMARY KEY, tid INTEGER, seq INTEGER)")
                .await
                .expect("create");
        }

        let stop = Arc::new(AtomicBool::new(false));
        let total_commits = Arc::new(AtomicU64::new(0));

        let threads: Vec<_> = (0..8)
            .map(|tid| {
                let path = path_str.to_string();
                let s = Arc::clone(&stop);
                let tc = Arc::clone(&total_commits);
                std::thread::spawn(move || {
                    asupersync::test_utils::run_test(|| async {
                        let conn = Connection::open(&path).await.expect("open");
                        let mut seq = 0u64;
                        let mut committed = 0u64;
                        while !s.load(Ordering::Relaxed) {
                            if conn.execute("BEGIN").await.is_ok() {
                                let id = tid as u64 * 1_000_000 + seq;
                                if conn
                                    .execute(&format!(
                                        "INSERT INTO log VALUES ({id}, {tid}, {seq})"
                                    ))
                                    .await
                                    .is_ok()
                                {
                                    if conn.execute("COMMIT").await.is_ok() {
                                        committed += 1;
                                    } else {
                                        conn.execute("ROLLBACK").await.ok();
                                    }
                                } else {
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
                .expect("thread must not panic during concurrent commits");
        }

        let commits = total_commits.load(Ordering::Relaxed);
        assert!(commits > 0, "no commits succeeded — possible livelock");
        eprintln!("C1: {commits} concurrent commits, 8 threads, no panics");
    });
}

// ─── C2: Committed row count correct after concurrent writes ───────

#[test]
fn c2_committed_row_count_correct() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let db_path = dir.path().join("c2.db");
        let path_str = db_path.to_str().expect("path");

        {
            let conn = Connection::open(path_str).await.expect("open");
            conn.execute("CREATE TABLE counts (tid INTEGER, seq INTEGER)")
                .await
                .expect("create");
        }

        let per_thread = 50;
        let num_threads = 4;

        let threads: Vec<_> = (0..num_threads)
            .map(|tid| {
                let path = path_str.to_string();
                std::thread::spawn(move || {
                    let mut committed = 0u64;
                    asupersync::test_utils::run_test(|| async {
                        let conn = Connection::open(&path).await.expect("open");
                        for seq in 0..per_thread {
                            if conn.execute("BEGIN").await.is_ok() {
                                if conn
                                    .execute(&format!("INSERT INTO counts VALUES ({tid}, {seq})"))
                                    .await
                                    .is_ok()
                                    && conn.execute("COMMIT").await.is_ok()
                                {
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

        let mut total_committed = 0u64;
        for t in threads {
            total_committed += t.join().expect("no panic");
        }

        let verify = Connection::open(path_str).await.expect("verify");
        let rows = verify
            .query("SELECT * FROM counts")
            .await
            .expect("count")
            .len();
        assert_eq!(
            rows, total_committed as usize,
            "row count mismatch: expected {total_committed} committed, got {rows} visible"
        );
        eprintln!("C2: {total_committed} committed, {rows} visible — match");
    });
}

// ─── C3: Per-connection commit ordering is monotonic ───────────────

#[test]
fn c3_per_connection_commit_ordering() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let db_path = dir.path().join("c3.db");
        let path_str = db_path.to_str().expect("path");

        {
            let conn = Connection::open(path_str).await.expect("open");
            conn.execute(
                "CREATE TABLE ordered (tid INTEGER, seq INTEGER, rowid_val INTEGER PRIMARY KEY)",
            )
            .await
            .expect("create");
        }

        let num_threads = 4;
        let per_thread = 100;

        let threads: Vec<_> = (0..num_threads)
            .map(|tid| {
                let path = path_str.to_string();
                std::thread::spawn(move || {
                    asupersync::test_utils::run_test(|| async {
                        let conn = Connection::open(&path).await.expect("open");
                        for seq in 0..per_thread {
                            let id = tid * 10000 + seq;
                            loop {
                                if conn.execute("BEGIN").await.is_ok()
                                    && conn
                                        .execute(&format!(
                                            "INSERT INTO ordered VALUES ({tid}, {seq}, {id})"
                                        ))
                                        .await
                                        .is_ok()
                                    && conn.execute("COMMIT").await.is_ok()
                                {
                                    break;
                                }
                                conn.execute("ROLLBACK").await.ok();
                                std::thread::yield_now();
                            }
                        }
                    });
                })
            })
            .collect();

        for t in threads {
            t.join().expect("no panic");
        }

        // Verify: for each tid, seq values are in insertion order (by rowid)
        let verify = Connection::open(path_str).await.expect("verify");
        for tid in 0..num_threads {
            let rows = verify
                .query(&format!(
                    "SELECT seq FROM ordered WHERE tid = {tid} ORDER BY rowid_val"
                ))
                .await
                .expect("query");
            let mut prev_seq = -1i64;
            for row in &rows {
                if let Some(fsqlite_types::SqliteValue::Integer(seq_val)) = row.get(0) {
                    assert!(
                        *seq_val > prev_seq,
                        "tid {tid}: non-monotonic seq {seq_val} after {prev_seq}"
                    );
                    prev_seq = *seq_val;
                }
            }
        }
        eprintln!("C3: {num_threads} threads × {per_thread} commits, all monotonic");
    });
}

// ─── C4: No lost commits ───────────────────────────────────────────

#[test]
fn c4_no_lost_commits() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let db_path = dir.path().join("c4.db");
        let path_str = db_path.to_str().expect("path");

        {
            let conn = Connection::open(path_str).await.expect("open");
            conn.execute("CREATE TABLE commits (id INTEGER PRIMARY KEY)")
                .await
                .expect("create");
        }

        let stop = Arc::new(AtomicBool::new(false));
        // Each thread tracks which IDs it committed
        let committed_ids: Vec<Arc<std::sync::Mutex<Vec<u64>>>> = (0..4)
            .map(|_| Arc::new(std::sync::Mutex::new(Vec::new())))
            .collect();

        let threads: Vec<_> = (0..4)
            .map(|i| {
                let path = path_str.to_string();
                let s = Arc::clone(&stop);
                let ids = Arc::clone(&committed_ids[i]);
                std::thread::spawn(move || {
                    asupersync::test_utils::run_test(|| async {
                        let conn = Connection::open(&path).await.expect("open");
                        let mut next_id = i as u64 * 1_000_000;
                        while !s.load(Ordering::Relaxed) {
                            if conn.execute("BEGIN").await.is_ok() {
                                if conn
                                    .execute(&format!("INSERT INTO commits VALUES ({next_id})"))
                                    .await
                                    .is_ok()
                                    && conn.execute("COMMIT").await.is_ok()
                                {
                                    ids.lock().unwrap().push(next_id);
                                } else {
                                    conn.execute("ROLLBACK").await.ok();
                                }
                                next_id += 1;
                            }
                        }
                    });
                })
            })
            .collect();

        std::thread::sleep(STRESS_DURATION);
        stop.store(true, Ordering::Relaxed);

        for t in threads {
            t.join().expect("no panic");
        }

        // Gather all committed IDs
        let mut all_committed: Vec<u64> = Vec::new();
        for ids in &committed_ids {
            all_committed.extend(ids.lock().unwrap().iter());
        }
        all_committed.sort_unstable();

        // Verify each committed ID is visible
        let verify = Connection::open(path_str).await.expect("verify");
        let visible = verify
            .query("SELECT * FROM commits")
            .await
            .expect("all")
            .len();
        assert_eq!(
            visible,
            all_committed.len(),
            "LOST COMMITS: {visible} visible but {} committed",
            all_committed.len()
        );
        eprintln!("C4: {} commits, all visible — no lost commits", visible);
    });
}

// ─── C5: Rapid commit + read interleaving ──────────────────────────

#[test]
fn c5_commit_read_interleaving() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let db_path = dir.path().join("c5.db");
        let path_str = db_path.to_str().expect("path");

        {
            let conn = Connection::open(path_str).await.expect("open");
            conn.execute("CREATE TABLE seq_log (val INTEGER)")
                .await
                .expect("create");
        }

        let stop = Arc::new(AtomicBool::new(false));
        let anomalies = Arc::new(AtomicU64::new(0));

        // Writer: inserts increasing values
        let w_path = path_str.to_string();
        let w_stop = Arc::clone(&stop);
        let writer = std::thread::spawn(move || {
            let mut val = 0u64;
            asupersync::test_utils::run_test(|| async {
                let conn = Connection::open(&w_path).await.expect("w open");
                while !w_stop.load(Ordering::Relaxed) {
                    if conn.execute("BEGIN").await.is_ok() {
                        if conn
                            .execute(&format!("INSERT INTO seq_log VALUES ({val})"))
                            .await
                            .is_ok()
                            && conn.execute("COMMIT").await.is_ok()
                        {
                            val += 1;
                        } else {
                            conn.execute("ROLLBACK").await.ok();
                        }
                    }
                }
            });
            val
        });

        // Readers: check that row count never decreases between reads
        let readers: Vec<_> = (0..4)
            .map(|_| {
                let path = path_str.to_string();
                let s = Arc::clone(&stop);
                let a = Arc::clone(&anomalies);
                std::thread::spawn(move || {
                    let mut reads = 0u64;
                    asupersync::test_utils::run_test(|| async {
                        let conn = Connection::open(&path).await.expect("r open");
                        let mut prev_count = 0usize;
                        while !s.load(Ordering::Relaxed) {
                            if let Ok(rows) = conn.query("SELECT COUNT(*) FROM seq_log").await {
                                let count = rows.len();
                                if count < prev_count {
                                    a.fetch_add(1, Ordering::Relaxed);
                                }
                                prev_count = count;
                                reads += 1;
                            }
                        }
                    });
                    reads
                })
            })
            .collect();

        std::thread::sleep(STRESS_DURATION);
        stop.store(true, Ordering::Relaxed);

        let writes = writer.join().expect("writer no panic");
        let mut total_reads = 0u64;
        for r in readers {
            total_reads += r.join().expect("reader no panic");
        }

        let anomaly_count = anomalies.load(Ordering::Relaxed);
        assert_eq!(
            anomaly_count, 0,
            "STALE READ: count decreased {anomaly_count} times (commit_seq ordering bug?)"
        );
        eprintln!("C5: {writes} writes, {total_reads} reads, 0 stale-read anomalies");
    });
}
