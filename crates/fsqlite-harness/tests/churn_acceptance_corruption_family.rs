//! bd-kqari: acceptance-scale churn keeper for the closed bd-0shxy corruption
//! family (index/table desync, cursor is_table flag errors, 0x00 page flags).
//!
//! The family had two mechanisms, both fixed and both exercised here:
//! - INTRA-TXN savepoint re-grant (savepoint quarantine fix): statement
//!   retries inside `BEGIN CONCURRENT` under heavy same-bucket contention
//!   trigger statement-savepoint rollbacks with live allocations.
//! - COMMITTED-FREELIST resurrection (gh302 append-gate guard, 05144b4f4):
//!   DELETE traffic creates committed free pages that concurrent peers then
//!   re-allocate; the original minimal repro had NO deletes, so this keeper
//!   is strictly stronger on the resurrection axis.
//!
//! Scale defaults stay CI-viable; FSQLITE_CHURN_SCALE=full selects the
//! acceptance-scale variant for dedicated runs (bd-kqari's 1M-op gate).
//!
//! Verdict is the bd-0shxy differential oracle: engine COUNT via a fresh
//! connection, the rusqlite C oracle on the same file, and a clean
//! integrity_check must all agree with the arithmetic expectation.

use fsqlite::Connection;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;

fn scale() -> (usize, usize) {
    match std::env::var("FSQLITE_CHURN_SCALE").as_deref() {
        Ok("full") => (32, 31_250), // 1M ops: bd-kqari acceptance gate
        Ok("large") => (32, 1_000),
        // CI keeper default: 2400 ops. The first cut (24x300) exceeded 15
        // minutes under load — the resurrection guard's freelist-publication
        // serialization makes delete-heavy churn expensive; the scaled runs
        // are what FSQLITE_CHURN_SCALE=large/full are for.
        _ => (16, 150),
    }
}

#[test]
fn churn_acceptance_corruption_family() {
    asupersync::test_utils::run_test(|| async {
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("churn_acceptance.db");
        let db = db_path.to_string_lossy().into_owned();

        let setup = Connection::open(&db).await.unwrap();
        setup
            .execute_batch(
                "PRAGMA journal_mode=WAL;
                 PRAGMA busy_timeout=10000;
                 CREATE TABLE items (id INTEGER PRIMARY KEY, category INTEGER, data TEXT);
                 CREATE INDEX idx_category ON items(category);",
            )
            .await
            .unwrap();

        let (n_threads, ops_per_thread) = scale();
        let barrier = Arc::new(Barrier::new(n_threads));
        // Signed running expectation: +1 per surviving insert, deletes
        // subtract exactly what they removed (reported by changes()).
        let expected_rows = Arc::new(AtomicI64::new(0));

        let mut handles = vec![];
        for tid in 0..n_threads {
            let bar = barrier.clone();
            let path = db.clone();
            let expected = expected_rows.clone();
            handles.push(thread::spawn(move || -> Result<(), String> {
                let mut outcome: Result<(), String> = Ok(());
                asupersync::test_utils::run_test(|| async {
                    outcome = async {
                        let conn = Connection::open(&path).await.map_err(|e| format!("{e:?}"))?;
                        conn.execute_batch("PRAGMA busy_timeout=10000;")
                            .await
                            .map_err(|e| format!("{e:?}"))?;
                        bar.wait();

                        for i in 0..ops_per_thread {
                            let id = (tid * 10_000_000 + i) as i64;
                            // All writers hammer one index bucket (max split
                            // contention); every 8th op is a ranged DELETE to
                            // feed the committed freelist (resurrection axis).
                            let delete_op = i % 8 == 7;
                            let sql = if delete_op {
                                // Point-delete ONE of this thread's own older
                                // rows by primary key: feeds the committed
                                // freelist (resurrection axis) with a minimal
                                // conflict surface. The first cut used ranged
                                // deletes over the hammered bucket and
                                // livelocked in cross-thread retry storms.
                                format!(
                                    "DELETE FROM items WHERE id = {}",
                                    (tid * 10_000_000 + i.saturating_sub(7)) as i64,
                                )
                            } else {
                                format!(
                                    "INSERT INTO items (id, category, data) VALUES ({id}, 42, 'churn_{tid}_{i}')"
                                )
                            };

                            loop {
                                if conn.in_transaction() {
                                    match conn.execute("ROLLBACK;").await {
                                        Ok(_) => {}
                                        Err(_err) if !conn.in_transaction() => {}
                                        Err(err) => {
                                            return Err(format!(
                                                "thread={tid} op={i} stale-txn clear failed: {err}"
                                            ));
                                        }
                                    }
                                }
                                match conn.execute("BEGIN CONCURRENT;").await {
                                    Ok(_) => {}
                                    Err(err) if err.is_transient() => {
                                        thread::sleep(std::time::Duration::from_millis(1));
                                        continue;
                                    }
                                    Err(err) => {
                                        return Err(format!(
                                            "thread={tid} op={i} begin failed: {err}"
                                        ));
                                    }
                                }
                                let changes = match conn.execute(&sql).await {
                                    Ok(changes) => changes,
                                    Err(err) if err.is_transient() => {
                                        match conn.execute("ROLLBACK;").await {
                                            Ok(_) => {}
                                            Err(_r) if !conn.in_transaction() => {}
                                            Err(r) => {
                                                return Err(format!(
                                                    "thread={tid} op={i} rollback failed: {r}"
                                                ));
                                            }
                                        }
                                        thread::sleep(std::time::Duration::from_millis(1));
                                        continue;
                                    }
                                    Err(err) => {
                                        drop(conn.execute("ROLLBACK;").await);
                                        return Err(format!(
                                            "thread={tid} op={i} statement failed: {err}"
                                        ));
                                    }
                                };
                                match conn.execute("COMMIT;").await {
                                    Ok(_) => {
                                        if delete_op {
                                            expected.fetch_sub(changes as i64, Ordering::Relaxed);
                                        } else {
                                            if changes != 1 {
                                                return Err(format!(
                                                    "thread={tid} op={i} insert changed {changes}"
                                                ));
                                            }
                                            expected.fetch_add(1, Ordering::Relaxed);
                                        }
                                        break;
                                    }
                                    Err(err) if err.is_transient() => {
                                        match conn.execute("ROLLBACK;").await {
                                            Ok(_) => {}
                                            Err(_r) if !conn.in_transaction() => {}
                                            Err(r) => {
                                                return Err(format!(
                                                    "thread={tid} op={i} commit-rollback failed: {r}"
                                                ));
                                            }
                                        }
                                        thread::sleep(std::time::Duration::from_millis(1));
                                        continue;
                                    }
                                    Err(err) => {
                                        drop(conn.execute("ROLLBACK;").await);
                                        return Err(format!(
                                            "thread={tid} op={i} commit failed: {err}"
                                        ));
                                    }
                                }
                            }
                        }
                        Ok(())
                    }
                    .await;
                });
                outcome
            }));
        }

        for h in handles {
            let res = h.join().unwrap();
            assert!(res.is_ok(), "Thread failed: {:?}", res);
        }

        let expected = expected_rows.load(Ordering::SeqCst);

        // bd-0shxy differential oracle: three independent readers must agree.
        let fresh = Connection::open(&db).await.unwrap();
        let rows = fresh.query("SELECT COUNT(*) FROM items").await.unwrap();
        let engine_total = match &rows[0].values()[0] {
            fsqlite_types::SqliteValue::Integer(i) => *i,
            other => panic!("engine COUNT returned {other:?}"),
        };
        let rows = fresh
            .query("SELECT COUNT(*) FROM items INDEXED BY idx_category WHERE category = 42")
            .await
            .unwrap();
        let index_total = match &rows[0].values()[0] {
            fsqlite_types::SqliteValue::Integer(i) => *i,
            other => panic!("index COUNT returned {other:?}"),
        };
        let integrity = fresh.query("PRAGMA integrity_check;").await.unwrap();
        let integrity_text = format!("{:?}", integrity[0].values());
        fresh.close().await.unwrap();
        setup.close().await.unwrap();

        let oracle = rusqlite::Connection::open(&db_path).unwrap();
        let oracle_total: i64 = oracle
            .query_row("SELECT COUNT(*) FROM items", [], |r| r.get(0))
            .expect("rusqlite oracle must be able to read the produced file");
        let oracle_integrity: String = oracle
            .query_row("PRAGMA integrity_check;", [], |r| r.get(0))
            .expect("rusqlite integrity_check must run");

        assert!(
            integrity_text.contains("\"ok\"") || integrity_text.contains("Text(\"ok\")"),
            "engine integrity_check must be clean, got {integrity_text}"
        );
        assert_eq!(
            oracle_integrity, "ok",
            "C-oracle integrity_check must be clean"
        );
        assert_eq!(
            engine_total, expected,
            "engine table COUNT diverged from the arithmetic expectation"
        );
        assert_eq!(
            index_total, expected,
            "index COUNT diverged (index/table desync — bd-0shxy family)"
        );
        assert_eq!(
            oracle_total, expected,
            "C-oracle COUNT diverged from the arithmetic expectation"
        );
    });
}
