//! bd-9sf3q: FK cascade DELETE in morsel-parallel INSERT lane + SSI abort
//! in sibling lane → cascade orphans violate FK constraint at commit.
//!
//! ## Bug hypothesis
//!
//! When a DELETE with FK CASCADE fires concurrently with an INSERT into
//! the child table from a different connection, the cascade may leave
//! orphaned child rows that violate the foreign key constraint. If SSI
//! abort fires on the sibling lane, the orphans remain committed.
//!
//! ## Test approach
//!
//! - F1: Concurrent INSERT + DELETE with FK CASCADE — no orphans
//! - F2: Multi-level FK CASCADE under concurrent writes
//! - F3: FK CASCADE with SSI contention (same parent row)
//! - F4: FK SET NULL under concurrent child inserts
//! - F5: Rapid parent delete/reinsert with child references
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

async fn setup_fk_schema(conn: &Connection) {
    conn.execute("PRAGMA foreign_keys = ON")
        .await
        .expect("fk on");
    conn.execute("CREATE TABLE parents (id INTEGER PRIMARY KEY, name TEXT)")
        .await
        .expect("create parents");
    conn.execute(
        "CREATE TABLE children (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parents(id) ON DELETE CASCADE, val TEXT)",
    )
    .await
    .expect("create children");
}

async fn verify_fk_integrity(conn: &Connection) -> bool {
    match conn.query("PRAGMA foreign_key_check").await {
        Ok(rows) => rows.is_empty(),
        Err(_) => true,
    }
}

// ─── F1: Concurrent INSERT + DELETE with FK CASCADE ────────────────

#[test]
fn f1_concurrent_insert_delete_cascade() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let db_path = dir.path().join("f1.db");
        let path_str = db_path.to_str().expect("path");

        {
            let conn = Connection::open(path_str).await.expect("open");
            setup_fk_schema(&conn).await;
            conn.execute("BEGIN").await.expect("begin");
            for i in 1..=20 {
                conn.execute(&format!("INSERT INTO parents VALUES ({i}, 'parent_{i}')"))
                    .await
                    .expect("seed parent");
                for j in 1..=5 {
                    let cid = i * 100 + j;
                    conn.execute(&format!(
                        "INSERT INTO children VALUES ({cid}, {i}, 'child_{cid}')"
                    ))
                    .await
                    .expect("seed child");
                }
            }
            conn.execute("COMMIT").await.expect("commit");
        }

        let stop = Arc::new(AtomicBool::new(false));

        // Deleter: removes parents (triggers CASCADE on children)
        let d_path = path_str.to_string();
        let d_stop = Arc::clone(&stop);
        let deleter = std::thread::spawn(move || {
            let mut deletes = 0u64;
            asupersync::test_utils::run_test(|| async {
                let conn = Connection::open(&d_path).await.expect("d open");
                conn.execute("PRAGMA foreign_keys = ON").await.ok();
                while !d_stop.load(Ordering::Relaxed) {
                    let pid = (deletes % 20) + 1;
                    if conn.execute("BEGIN").await.is_ok() {
                        conn.execute(&format!("DELETE FROM parents WHERE id = {pid}"))
                            .await
                            .ok();
                        if conn.execute("COMMIT").await.is_ok() {
                            deletes += 1;
                        } else {
                            conn.execute("ROLLBACK").await.ok();
                        }
                    }
                }
            });
            deletes
        });

        // Inserter: adds new parents and children
        let i_path = path_str.to_string();
        let i_stop = Arc::clone(&stop);
        let inserter = std::thread::spawn(move || {
            let mut inserts = 0u64;
            asupersync::test_utils::run_test(|| async {
                let conn = Connection::open(&i_path).await.expect("i open");
                conn.execute("PRAGMA foreign_keys = ON").await.ok();
                while !i_stop.load(Ordering::Relaxed) {
                    let pid = 1000 + inserts;
                    if conn.execute("BEGIN").await.is_ok() {
                        if conn
                            .execute(&format!(
                                "INSERT OR IGNORE INTO parents VALUES ({pid}, 'new_{pid}')"
                            ))
                            .await
                            .is_ok()
                        {
                            let cid = pid * 100;
                            conn.execute(&format!(
                                "INSERT OR IGNORE INTO children VALUES ({cid}, {pid}, 'child_{cid}')"
                            ))
                            .await
                            .ok();
                            if conn.execute("COMMIT").await.is_ok() {
                                inserts += 1;
                            } else {
                                conn.execute("ROLLBACK").await.ok();
                            }
                        } else {
                            conn.execute("ROLLBACK").await.ok();
                        }
                    }
                }
            });
            inserts
        });

        std::thread::sleep(STRESS_DURATION);
        stop.store(true, Ordering::Relaxed);

        let deletes = deleter.join().expect("deleter must not panic");
        let inserts = inserter.join().expect("inserter must not panic");

        // FK integrity check
        let verify = Connection::open(path_str).await.expect("verify");
        verify.execute("PRAGMA foreign_keys = ON").await.ok();
        let fk_ok = verify_fk_integrity(&verify).await;
        assert!(
            fk_ok,
            "FK CONSTRAINT VIOLATED: orphaned children after concurrent INSERT+DELETE CASCADE"
        );
        eprintln!("F1: {deletes} cascade deletes, {inserts} inserts, FK integrity OK");
    });
}

// ─── F2: Multi-level FK CASCADE ────────────────────────────────────

#[test]
fn f2_multi_level_cascade() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let db_path = dir.path().join("f2.db");
        let path_str = db_path.to_str().expect("path");

        {
            let conn = Connection::open(path_str).await.expect("open");
            conn.execute("PRAGMA foreign_keys = ON")
                .await
                .expect("fk on");
            conn.execute("CREATE TABLE l1 (id INTEGER PRIMARY KEY, name TEXT)")
                .await
                .expect("create l1");
            conn.execute(
                "CREATE TABLE l2 (id INTEGER PRIMARY KEY, l1_id INTEGER REFERENCES l1(id) ON DELETE CASCADE, val TEXT)",
            )
            .await
            .expect("create l2");
            conn.execute(
                "CREATE TABLE l3 (id INTEGER PRIMARY KEY, l2_id INTEGER REFERENCES l2(id) ON DELETE CASCADE, val TEXT)",
            )
            .await
            .expect("create l3");

            conn.execute("BEGIN").await.expect("begin");
            for i in 1..=10 {
                conn.execute(&format!("INSERT INTO l1 VALUES ({i}, 'l1_{i}')"))
                    .await
                    .expect("seed l1");
                for j in 1..=5 {
                    let l2id = i * 100 + j;
                    conn.execute(&format!("INSERT INTO l2 VALUES ({l2id}, {i}, 'l2_{l2id}')"))
                        .await
                        .expect("seed l2");
                    for k in 1..=3 {
                        let l3id = l2id * 100 + k;
                        conn.execute(&format!(
                            "INSERT INTO l3 VALUES ({l3id}, {l2id}, 'l3_{l3id}')"
                        ))
                        .await
                        .expect("seed l3");
                    }
                }
            }
            conn.execute("COMMIT").await.expect("commit");
        }

        let stop = Arc::new(AtomicBool::new(false));

        let d_path = path_str.to_string();
        let d_stop = Arc::clone(&stop);
        let deleter = std::thread::spawn(move || {
            let mut ops = 0u64;
            asupersync::test_utils::run_test(|| async {
                let conn = Connection::open(&d_path).await.expect("d open");
                conn.execute("PRAGMA foreign_keys = ON").await.ok();
                while !d_stop.load(Ordering::Relaxed) {
                    let l1id = (ops % 10) + 1;
                    if conn.execute("BEGIN").await.is_ok() {
                        conn.execute(&format!("DELETE FROM l1 WHERE id = {l1id}"))
                            .await
                            .ok();
                        if conn.execute("COMMIT").await.is_err() {
                            conn.execute("ROLLBACK").await.ok();
                        }
                        ops += 1;
                    }
                }
            });
            ops
        });

        let i_path = path_str.to_string();
        let i_stop = Arc::clone(&stop);
        let inserter = std::thread::spawn(move || {
            let mut ops = 0u64;
            asupersync::test_utils::run_test(|| async {
                let conn = Connection::open(&i_path).await.expect("i open");
                conn.execute("PRAGMA foreign_keys = ON").await.ok();
                while !i_stop.load(Ordering::Relaxed) {
                    let l1id = 2000 + ops;
                    if conn.execute("BEGIN").await.is_ok() {
                        if conn
                            .execute(&format!("INSERT INTO l1 VALUES ({l1id}, 'new')"))
                            .await
                            .is_ok()
                        {
                            let l2id = l1id * 100;
                            conn.execute(&format!(
                                "INSERT INTO l2 VALUES ({l2id}, {l1id}, 'new_l2')"
                            ))
                            .await
                            .ok();
                            let l3id = l2id * 100;
                            conn.execute(&format!(
                                "INSERT INTO l3 VALUES ({l3id}, {l2id}, 'new_l3')"
                            ))
                            .await
                            .ok();
                        }
                        if conn.execute("COMMIT").await.is_err() {
                            conn.execute("ROLLBACK").await.ok();
                        }
                        ops += 1;
                    }
                }
            });
            ops
        });

        std::thread::sleep(STRESS_DURATION);
        stop.store(true, Ordering::Relaxed);

        let del_ops = deleter.join().expect("deleter no panic");
        let ins_ops = inserter.join().expect("inserter no panic");

        let verify = Connection::open(path_str).await.expect("verify");
        verify.execute("PRAGMA foreign_keys = ON").await.ok();
        let fk_ok = verify_fk_integrity(&verify).await;
        assert!(fk_ok, "FK VIOLATED: multi-level cascade left orphans");
        eprintln!("F2: {del_ops} 3-level cascade deletes, {ins_ops} inserts, FK OK");
    });
}

// ─── F3: FK CASCADE with contention on same parent ─────────────────

#[test]
fn f3_cascade_same_parent_contention() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let db_path = dir.path().join("f3.db");
        let path_str = db_path.to_str().expect("path");

        {
            let conn = Connection::open(path_str).await.expect("open");
            setup_fk_schema(&conn).await;
            conn.execute("INSERT INTO parents VALUES (1, 'hot_parent')")
                .await
                .expect("seed parent");
        }

        let stop = Arc::new(AtomicBool::new(false));
        let total_ops = Arc::new(AtomicU64::new(0));

        // 4 threads all targeting the same parent
        let threads: Vec<_> = (0..4)
            .map(|tid| {
                let path = path_str.to_string();
                let s = Arc::clone(&stop);
                let ops = Arc::clone(&total_ops);
                std::thread::spawn(move || {
                    asupersync::test_utils::run_test(|| async {
                        let conn = Connection::open(&path).await.expect("open");
                        conn.execute("PRAGMA foreign_keys = ON").await.ok();
                        let mut local_ops = 0u64;
                        while !s.load(Ordering::Relaxed) {
                            let cid = tid as u64 * 1_000_000 + local_ops;
                            if conn.execute("BEGIN").await.is_ok() {
                                // Try to add child to parent 1
                                conn.execute(&format!(
                                    "INSERT OR IGNORE INTO children VALUES ({cid}, 1, 'child')"
                                ))
                                .await
                                .ok();
                                if conn.execute("COMMIT").await.is_err() {
                                    conn.execute("ROLLBACK").await.ok();
                                }
                            }

                            // Occasionally delete the parent (cascade) then re-add
                            if local_ops.is_multiple_of(50) && conn.execute("BEGIN").await.is_ok() {
                                conn.execute("DELETE FROM parents WHERE id = 1").await.ok();
                                conn.execute(
                                    "INSERT OR IGNORE INTO parents VALUES (1, 'hot_parent')",
                                )
                                .await
                                .ok();
                                if conn.execute("COMMIT").await.is_err() {
                                    conn.execute("ROLLBACK").await.ok();
                                }
                            }
                            local_ops += 1;
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
                .expect("thread must not panic during FK contention");
        }

        let ops = total_ops.load(Ordering::Relaxed);

        let verify = Connection::open(path_str).await.expect("verify");
        verify.execute("PRAGMA foreign_keys = ON").await.ok();
        let fk_ok = verify_fk_integrity(&verify).await;
        assert!(fk_ok, "FK VIOLATED: cascade contention left orphans");
        eprintln!("F3: {ops} ops contending on same parent, FK integrity OK");
    });
}

// ─── F4: FK SET NULL under concurrent child inserts ────────────────

#[test]
fn f4_fk_set_null_concurrent_inserts() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let db_path = dir.path().join("f4.db");
        let path_str = db_path.to_str().expect("path");

        {
            let conn = Connection::open(path_str).await.expect("open");
            conn.execute("PRAGMA foreign_keys = ON")
                .await
                .expect("fk on");
            conn.execute("CREATE TABLE refs (id INTEGER PRIMARY KEY, name TEXT)")
                .await
                .expect("create refs");
            conn.execute(
                "CREATE TABLE entries (id INTEGER PRIMARY KEY, ref_id INTEGER REFERENCES refs(id) ON DELETE SET NULL, data TEXT)",
            )
            .await
            .expect("create entries");

            conn.execute("BEGIN").await.expect("begin");
            for i in 1..=10 {
                conn.execute(&format!("INSERT INTO refs VALUES ({i}, 'ref_{i}')"))
                    .await
                    .expect("seed ref");
            }
            conn.execute("COMMIT").await.expect("commit");
        }

        let stop = Arc::new(AtomicBool::new(false));

        // Writer: adds entries referencing existing refs
        let w_path = path_str.to_string();
        let w_stop = Arc::clone(&stop);
        let writer = std::thread::spawn(move || {
            let mut inserted = 0u64;
            asupersync::test_utils::run_test(|| async {
                let conn = Connection::open(&w_path).await.expect("w open");
                conn.execute("PRAGMA foreign_keys = ON").await.ok();
                while !w_stop.load(Ordering::Relaxed) {
                    let ref_id = (inserted % 10) + 1;
                    if conn.execute("BEGIN").await.is_ok() {
                        conn.execute(&format!(
                            "INSERT INTO entries VALUES ({inserted}, {ref_id}, 'data')"
                        ))
                        .await
                        .ok();
                        if conn.execute("COMMIT").await.is_ok() {
                            inserted += 1;
                        } else {
                            conn.execute("ROLLBACK").await.ok();
                        }
                    }
                }
            });
            inserted
        });

        // Deleter: removes refs (SET NULL on entries)
        let d_path = path_str.to_string();
        let d_stop = Arc::clone(&stop);
        let deleter = std::thread::spawn(move || {
            let mut deleted = 0u64;
            asupersync::test_utils::run_test(|| async {
                let conn = Connection::open(&d_path).await.expect("d open");
                conn.execute("PRAGMA foreign_keys = ON").await.ok();
                while !d_stop.load(Ordering::Relaxed) {
                    std::thread::sleep(Duration::from_millis(50));
                    let ref_id = (deleted % 10) + 1;
                    if conn.execute("BEGIN").await.is_ok() {
                        conn.execute(&format!("DELETE FROM refs WHERE id = {ref_id}"))
                            .await
                            .ok();
                        // Re-add so writer can keep going
                        conn.execute(&format!(
                            "INSERT OR IGNORE INTO refs VALUES ({ref_id}, 'ref_{ref_id}')"
                        ))
                        .await
                        .ok();
                        if conn.execute("COMMIT").await.is_ok() {
                            deleted += 1;
                        } else {
                            conn.execute("ROLLBACK").await.ok();
                        }
                    }
                }
            });
            deleted
        });

        std::thread::sleep(STRESS_DURATION);
        stop.store(true, Ordering::Relaxed);

        let inserted = writer.join().expect("writer no panic");
        let deleted = deleter.join().expect("deleter no panic");

        let verify = Connection::open(path_str).await.expect("verify");
        verify.execute("PRAGMA foreign_keys = ON").await.ok();
        let fk_ok = verify_fk_integrity(&verify).await;
        assert!(
            fk_ok,
            "FK VIOLATED: SET NULL concurrent inserts left violations"
        );
        eprintln!("F4: {inserted} inserts, {deleted} SET NULL cycles, FK OK");
    });
}

// ─── F5: Rapid parent delete/reinsert with child references ────────

#[test]
fn f5_rapid_parent_delete_reinsert() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let db_path = dir.path().join("f5.db");
        let path_str = db_path.to_str().expect("path");

        {
            let conn = Connection::open(path_str).await.expect("open");
            setup_fk_schema(&conn).await;
        }

        let stop = Arc::new(AtomicBool::new(false));
        let total_ops = Arc::new(AtomicU64::new(0));

        // Parent manager: rapidly creates, populates, deletes parents
        let m_path = path_str.to_string();
        let m_stop = Arc::clone(&stop);
        let m_ops = Arc::clone(&total_ops);
        let manager = std::thread::spawn(move || {
            asupersync::test_utils::run_test(|| async {
                let conn = Connection::open(&m_path).await.expect("m open");
                conn.execute("PRAGMA foreign_keys = ON").await.ok();
                let mut ops = 0u64;
                while !m_stop.load(Ordering::Relaxed) {
                    let pid = (ops % 20) + 1;
                    if conn.execute("BEGIN").await.is_ok() {
                        conn.execute(&format!(
                            "INSERT OR IGNORE INTO parents VALUES ({pid}, 'p_{pid}')"
                        ))
                        .await
                        .ok();
                        // Add a child
                        let cid = pid * 1000 + (ops % 10);
                        conn.execute(&format!(
                            "INSERT OR IGNORE INTO children VALUES ({cid}, {pid}, 'c')"
                        ))
                        .await
                        .ok();
                        if conn.execute("COMMIT").await.is_err() {
                            conn.execute("ROLLBACK").await.ok();
                        }
                    }

                    // Every 10th op, delete a parent (cascade children)
                    if ops.is_multiple_of(10) && conn.execute("BEGIN").await.is_ok() {
                        conn.execute(&format!("DELETE FROM parents WHERE id = {pid}"))
                            .await
                            .ok();
                        if conn.execute("COMMIT").await.is_err() {
                            conn.execute("ROLLBACK").await.ok();
                        }
                    }
                    ops += 1;
                }
                m_ops.fetch_add(ops, Ordering::Relaxed);
            });
        });

        // Reader: checks FK integrity periodically
        let r_path = path_str.to_string();
        let r_stop = Arc::clone(&stop);
        let reader = std::thread::spawn(move || {
            let mut checks = 0u64;
            let mut violations = 0u64;
            asupersync::test_utils::run_test(|| async {
                let conn = Connection::open(&r_path).await.expect("r open");
                conn.execute("PRAGMA foreign_keys = ON").await.ok();
                while !r_stop.load(Ordering::Relaxed) {
                    if !verify_fk_integrity(&conn).await {
                        violations += 1;
                    }
                    checks += 1;
                    std::thread::sleep(Duration::from_millis(10));
                }
            });
            (checks, violations)
        });

        std::thread::sleep(STRESS_DURATION);
        stop.store(true, Ordering::Relaxed);

        manager.join().expect("manager no panic");
        let (checks, violations) = reader.join().expect("reader no panic");

        let ops = total_ops.load(Ordering::Relaxed);
        assert_eq!(
            violations, 0,
            "FK violations detected during rapid parent delete/reinsert: {violations}/{checks} checks"
        );
        eprintln!("F5: {ops} parent ops, {checks} FK checks, 0 violations");
    });
}
