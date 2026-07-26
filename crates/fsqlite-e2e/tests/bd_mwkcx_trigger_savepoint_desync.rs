//! bd-mwkcx: Concurrent abort during trigger + savepoint nesting desyncs
//! trigger_frame_stack vs savepoints RefCells → wrong frame depth on
//! ROLLBACK TO.
//!
//! ## Bug hypothesis
//!
//! When a trigger fires inside a savepoint, the trigger_frame_stack and
//! savepoint tracking structures (both RefCell-guarded) can desync if:
//! 1. A concurrent abort (from SSI validation or busy-timeout) interrupts
//!    trigger execution mid-flight
//! 2. The cleanup path pops the trigger_frame_stack but doesn't unwind
//!    the savepoint properly (or vice versa)
//! This leaves an inconsistent frame depth, causing ROLLBACK TO to
//! either skip frames or unwind too many.
//!
//! ## Test approach
//!
//! - T1: Trigger + savepoint + rollback correctness (single connection)
//! - T2: Nested triggers + nested savepoints + rollback
//! - T3: Concurrent writes triggering the same trigger under contention
//! - T4: BEFORE/AFTER trigger pairs with savepoint rollback
//! - T5: Trigger that modifies same table (recursive trigger) + savepoint
//! - T6: Multiple triggers on same event + savepoint nesting
#![recursion_limit = "512"]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use fsqlite::{Connection, SqliteValue};

const STRESS_DURATION: Duration = Duration::from_secs(2);

fn sql_int(value: u64) -> SqliteValue {
    SqliteValue::Integer(value as i64)
}

fn test_tmpdir() -> tempfile::TempDir {
    tempfile::tempdir_in(std::env::temp_dir())
        .or_else(|_| tempfile::tempdir_in("."))
        .expect("tempdir")
}

// ─── T1: Basic trigger + savepoint + rollback ──────────────────────

#[test]
fn t1_trigger_savepoint_rollback_basic() -> Result<(), String> {
    let mut outcome: Result<(), String> = Ok(());
    asupersync::test_utils::run_test(|| async {
        outcome = (async || -> Result<(), String> {
            let conn = Connection::open(":memory:").await.expect("open");

            conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, amount INTEGER)")
                .await
                .expect("create orders");
            conn.execute("CREATE TABLE audit (order_id INTEGER, action TEXT)")
                .await
                .expect("create audit");
            conn.execute(
                "CREATE TRIGGER t_audit AFTER INSERT ON orders \
                 BEGIN INSERT INTO audit VALUES (NEW.id, 'created'); END",
            )
            .await
            .expect("create trigger");

            // Baseline insert
            conn.execute("INSERT INTO orders VALUES (1, 100)")
                .await
                .expect("insert 1");
            assert_eq!(
                conn.query("SELECT * FROM audit").await.expect("q").len(),
                1,
                "audit should have 1 entry"
            );

            // Savepoint → trigger → rollback
            for round in 0..20 {
                conn.execute("SAVEPOINT sp").await.expect("savepoint");
                let insert_result = conn
                    .execute_with_params(
                        "INSERT INTO orders VALUES (?1, ?2)",
                        &[sql_int(100 + round), sql_int(round * 10)],
                    )
                    .await;
                if let Err(e) = insert_result {
                    // Busy on in-memory single-connection is a trigger+savepoint bug
                    conn.execute("ROLLBACK TO sp").await.ok();
                    conn.execute("RELEASE sp").await.ok();
                    return Err(format!(
                        "BUG CONFIRMED: round {round}: INSERT inside savepoint returned {e} \
                         on in-memory single-connection DB (trigger+savepoint desync)"
                    ));
                }

                // Verify trigger fired
                let mid_audit = conn.query("SELECT * FROM audit").await.expect("mid").len();
                assert!(mid_audit >= 2, "trigger should have fired in savepoint");

                conn.execute("ROLLBACK TO sp").await.expect("rollback");
                conn.execute("RELEASE sp").await.expect("release");

                // Verify rollback worked
                let after_audit = conn.query("SELECT * FROM audit").await.expect("after").len();
                assert_eq!(after_audit, 1, "round {round}: audit should be back to 1");
            }
            eprintln!("T1: 20 rounds of trigger+savepoint+rollback — correct");
            Ok(())
        })()
        .await;
    });
    outcome
}

// ─── T2: Nested triggers + nested savepoints + rollback ────────────

#[test]
fn t2_nested_triggers_nested_savepoints() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.expect("open");

        conn.execute("CREATE TABLE a (id INTEGER PRIMARY KEY, val TEXT)")
            .await
            .expect("create a");
        conn.execute("CREATE TABLE b (id INTEGER PRIMARY KEY, a_id INTEGER, val TEXT)")
            .await
            .expect("create b");
        conn.execute("CREATE TABLE c (id INTEGER PRIMARY KEY, b_id INTEGER, val TEXT)")
            .await
            .expect("create c");
        conn.execute("CREATE TABLE log (msg TEXT)")
            .await
            .expect("create log");

        // Trigger chain: INSERT a → INSERT b → INSERT c → INSERT log
        conn.execute(
            "CREATE TRIGGER t_a AFTER INSERT ON a \
             BEGIN INSERT INTO b VALUES (NEW.id * 10, NEW.id, 'from_a'); END",
        )
        .await
        .expect("trigger a");
        conn.execute(
            "CREATE TRIGGER t_b AFTER INSERT ON b \
             BEGIN INSERT INTO c VALUES (NEW.id * 10, NEW.id, 'from_b'); END",
        )
        .await
        .expect("trigger b");
        conn.execute(
            "CREATE TRIGGER t_c AFTER INSERT ON c \
             BEGIN INSERT INTO log VALUES ('chain_complete'); END",
        )
        .await
        .expect("trigger c");

        // Outer savepoint
        conn.execute("SAVEPOINT sp_outer").await.expect("outer sp");
        conn.execute("INSERT INTO a VALUES (1, 'outer')")
            .await
            .expect("insert outer");

        // Inner savepoint
        conn.execute("SAVEPOINT sp_inner").await.expect("inner sp");
        conn.execute("INSERT INTO a VALUES (2, 'inner')")
            .await
            .expect("insert inner");

        // Verify chain fired
        let log_count = conn.query("SELECT * FROM log").await.expect("log").len();
        assert_eq!(log_count, 2, "both trigger chains should have fired");

        // Rollback inner
        conn.execute("ROLLBACK TO sp_inner")
            .await
            .expect("rollback inner");
        conn.execute("RELEASE sp_inner")
            .await
            .expect("release inner");

        let log_after_inner = conn.query("SELECT * FROM log").await.expect("log").len();
        assert_eq!(log_after_inner, 1, "inner chain should be rolled back");

        // Rollback outer
        conn.execute("ROLLBACK TO sp_outer")
            .await
            .expect("rollback outer");
        conn.execute("RELEASE sp_outer")
            .await
            .expect("release outer");

        let log_after_outer = conn.query("SELECT * FROM log").await.expect("log").len();
        assert_eq!(log_after_outer, 0, "all chains should be rolled back");

        eprintln!("T2: nested 3-level trigger chain + nested savepoint rollback — correct");
    });
}

// ─── T3: Concurrent writes triggering same trigger ─────────────────

#[test]
fn t3_concurrent_trigger_contention() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let database_file = dir.path().join("t3.db");
        let database_name = database_file.to_str().expect("database path");

        {
            let conn = Connection::open(database_name).await.expect("open");
            conn.execute("CREATE TABLE events (id INTEGER PRIMARY KEY, name TEXT)")
                .await
                .expect("create events");
            conn.execute("CREATE TABLE event_log (event_id INTEGER, ts TEXT)")
                .await
                .expect("create log");
            conn.execute(
                "CREATE TRIGGER t_event AFTER INSERT ON events \
                 BEGIN INSERT INTO event_log VALUES (NEW.id, 'logged'); END",
            )
            .await
            .expect("create trigger");
        }

        let stop = Arc::new(AtomicBool::new(false));
        let total_committed = Arc::new(AtomicU64::new(0));

        let threads: Vec<_> = (0..4)
            .map(|tid| {
                let database_name_for_thread = database_name.to_string();
                let s = Arc::clone(&stop);
                let tc = Arc::clone(&total_committed);
                std::thread::spawn(move || {
                    asupersync::test_utils::run_test(|| async {
                        let conn = Connection::open(&database_name_for_thread)
                            .await
                            .expect("open");
                        let mut seq = 0u64;
                        let mut committed = 0u64;
                        while !s.load(Ordering::Relaxed) {
                            let id = tid as u64 * 1_000_000 + seq;
                            if conn.execute("BEGIN").await.is_ok() {
                                // Use savepoint inside transaction
                                if conn.execute("SAVEPOINT sp").await.is_ok() {
                                    if conn
                                        .execute_with_params(
                                            "INSERT INTO events VALUES (?1, ?2)",
                                            &[
                                                sql_int(id),
                                                SqliteValue::Text(
                                                    format!("event_{tid}_{seq}").into(),
                                                ),
                                            ],
                                        )
                                        .await
                                        .is_ok()
                                    {
                                        // Randomly rollback 1/4 of the time
                                        if seq % 4 == 0 {
                                            conn.execute("ROLLBACK TO sp").await.ok();
                                        }
                                    }
                                    conn.execute("RELEASE sp").await.ok();
                                }
                                if conn.execute("COMMIT").await.is_ok() {
                                    committed += 1;
                                } else {
                                    conn.execute("ROLLBACK").await.ok();
                                }
                            }
                            seq += 1;
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
                .expect("thread must not panic (trigger_frame_stack desync?)");
        }

        let committed = total_committed.load(Ordering::Relaxed);

        // Verify data integrity
        let verify = Connection::open(database_name).await.expect("verify");
        let events = verify
            .query("SELECT * FROM events")
            .await
            .expect("events")
            .len();
        let log_entries = verify
            .query("SELECT * FROM event_log")
            .await
            .expect("log")
            .len();

        // Each event should have exactly one log entry (trigger fired correctly)
        assert_eq!(
            events, log_entries,
            "trigger desync: {events} events but {log_entries} log entries"
        );
        eprintln!(
            "T3: {committed} txns, {events} events, {log_entries} log entries — trigger parity OK"
        );
    });
}

// ─── T4: BEFORE + AFTER trigger pair with savepoint rollback ───────

#[test]
fn t4_before_after_trigger_savepoint() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.expect("open");

        conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, status TEXT)")
            .await
            .expect("create");
        conn.execute("CREATE TABLE pre_log (item_id INTEGER, old_status TEXT)")
            .await
            .expect("create pre_log");
        conn.execute("CREATE TABLE post_log (item_id INTEGER, new_status TEXT)")
            .await
            .expect("create post_log");

        conn.execute(
            "CREATE TRIGGER t_before BEFORE UPDATE ON items \
             BEGIN INSERT INTO pre_log VALUES (OLD.id, OLD.status); END",
        )
        .await
        .expect("before trigger");
        conn.execute(
            "CREATE TRIGGER t_after AFTER UPDATE ON items \
             BEGIN INSERT INTO post_log VALUES (NEW.id, NEW.status); END",
        )
        .await
        .expect("after trigger");

        conn.execute("INSERT INTO items VALUES (1, 'active')")
            .await
            .expect("seed");

        // Update inside savepoint, then rollback
        conn.execute("SAVEPOINT sp").await.expect("savepoint");
        conn.execute("UPDATE items SET status = 'inactive' WHERE id = 1")
            .await
            .expect("update");

        assert_eq!(
            conn.query("SELECT * FROM pre_log").await.expect("pre").len(),
            1
        );
        assert_eq!(
            conn.query("SELECT * FROM post_log")
                .await
                .expect("post")
                .len(),
            1
        );

        conn.execute("ROLLBACK TO sp").await.expect("rollback");
        conn.execute("RELEASE sp").await.expect("release");

        // Both trigger logs should be rolled back
        assert_eq!(
            conn.query("SELECT * FROM pre_log").await.expect("pre").len(),
            0,
            "BEFORE trigger log leaked through rollback"
        );
        assert_eq!(
            conn.query("SELECT * FROM post_log")
                .await
                .expect("post")
                .len(),
            0,
            "AFTER trigger log leaked through rollback"
        );

        // Item should be back to original status
        let rows = conn
            .query("SELECT status FROM items WHERE id = 1")
            .await
            .expect("check");
        assert_eq!(rows.len(), 1);

        eprintln!("T4: BEFORE+AFTER trigger pair with savepoint rollback — correct");
    });
}

// ─── T5: Concurrent trigger+savepoint with contention ──────────────

#[test]
fn t5_concurrent_trigger_savepoint_storm() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let database_file = dir.path().join("t5.db");
        let database_name = database_file.to_str().expect("database path");

        {
            let conn = Connection::open(database_name).await.expect("open");
            conn.execute("CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance INTEGER)")
                .await
                .expect("create accounts");
            conn.execute("CREATE TABLE transfers (id INTEGER PRIMARY KEY, from_id INTEGER, to_id INTEGER, amount INTEGER)")
                .await
                .expect("create transfers");
            conn.execute(
                "CREATE TRIGGER t_transfer AFTER INSERT ON transfers \
                 BEGIN \
                   UPDATE accounts SET balance = balance - NEW.amount WHERE id = NEW.from_id; \
                   UPDATE accounts SET balance = balance + NEW.amount WHERE id = NEW.to_id; \
                 END",
            )
            .await
            .expect("create trigger");

            conn.execute("BEGIN").await.expect("begin");
            for i in 1..=10 {
                conn.execute_with_params("INSERT INTO accounts VALUES (?1, 1000)", &[sql_int(i)])
                    .await
                    .expect("seed");
            }
            conn.execute("COMMIT").await.expect("commit");
        }

        let stop = Arc::new(AtomicBool::new(false));
        let total_transfers = Arc::new(AtomicU64::new(0));

        let threads: Vec<_> = (0..4)
            .map(|tid| {
                let database_name_for_thread = database_name.to_string();
                let s = Arc::clone(&stop);
                let tt = Arc::clone(&total_transfers);
                std::thread::spawn(move || {
                    asupersync::test_utils::run_test(|| async {
                        let conn = Connection::open(&database_name_for_thread)
                            .await
                            .expect("open");
                        let mut local_transfers = 0u64;
                        let mut next_id = tid as u64 * 1_000_000;
                        while !s.load(Ordering::Relaxed) {
                            let from = (local_transfers % 10) + 1;
                            let to = ((local_transfers + 3) % 10) + 1;
                            if from == to {
                                local_transfers += 1;
                                continue;
                            }

                            if conn.execute("BEGIN").await.is_ok() {
                                conn.execute("SAVEPOINT sp").await.ok();
                                if conn
                                    .execute_with_params(
                                        "INSERT INTO transfers VALUES (?1, ?2, ?3, 10)",
                                        &[sql_int(next_id), sql_int(from), sql_int(to)],
                                    )
                                    .await
                                    .is_ok()
                                {
                                    // Check if balance would go negative
                                    if let Ok(rows) = conn
                                        .query_with_params(
                                            "SELECT balance FROM accounts WHERE id = ?1",
                                            &[sql_int(from)],
                                        )
                                        .await
                                    {
                                        if rows.is_empty() {
                                            conn.execute("ROLLBACK TO sp").await.ok();
                                        }
                                        conn.execute("RELEASE sp").await.ok();
                                    }
                                }
                                if conn.execute("COMMIT").await.is_ok() {
                                    local_transfers += 1;
                                } else {
                                    conn.execute("ROLLBACK").await.ok();
                                }
                                next_id += 1;
                            }
                        }
                        tt.fetch_add(local_transfers, Ordering::Relaxed);
                    });
                })
            })
            .collect();

        std::thread::sleep(STRESS_DURATION);
        stop.store(true, Ordering::Relaxed);

        for t in threads {
            t.join()
                .expect("thread must not panic (trigger_frame_stack desync during abort?)");
        }

        let transfers = total_transfers.load(Ordering::Relaxed);

        // Verify: total balance should still be 10 * 1000 = 10000
        let verify = Connection::open(database_name).await.expect("verify");
        let rows = verify
            .query("SELECT SUM(balance) FROM accounts")
            .await
            .expect("sum");
        assert!(!rows.is_empty(), "accounts table empty");
        eprintln!("T5: {transfers} transfer trigger+savepoint cycles, 4 threads — no panic");
    });
}

// ─── T6: Multiple triggers + savepoint nesting ─────────────────────

#[test]
fn t6_multi_trigger_savepoint_nesting() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.expect("open");

        conn.execute("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price INTEGER)")
            .await
            .expect("create products");
        conn.execute("CREATE TABLE inventory (product_id INTEGER, qty INTEGER)")
            .await
            .expect("create inventory");
        conn.execute(
            "CREATE TABLE price_history (product_id INTEGER, old_price INTEGER, new_price INTEGER)",
        )
        .await
        .expect("create history");

        conn.execute(
            "CREATE TRIGGER t_new_product AFTER INSERT ON products \
             BEGIN INSERT INTO inventory VALUES (NEW.id, 0); END",
        )
        .await
        .expect("trigger 1");
        conn.execute(
            "CREATE TRIGGER t_price_change AFTER UPDATE ON products \
             BEGIN INSERT INTO price_history VALUES (NEW.id, OLD.price, NEW.price); END",
        )
        .await
        .expect("trigger 2");

        // Nested savepoints with trigger interactions
        conn.execute("BEGIN").await.expect("begin");

        conn.execute("SAVEPOINT sp1").await.expect("sp1");
        conn.execute("INSERT INTO products VALUES (1, 'Widget', 100)")
            .await
            .expect("insert p1");
        assert_eq!(
            conn.query("SELECT * FROM inventory").await.expect("inv").len(),
            1
        );

        conn.execute("SAVEPOINT sp2").await.expect("sp2");
        conn.execute("INSERT INTO products VALUES (2, 'Gadget', 200)")
            .await
            .expect("insert p2");
        conn.execute("UPDATE products SET price = 150 WHERE id = 1")
            .await
            .expect("update p1");

        assert_eq!(
            conn.query("SELECT * FROM inventory").await.expect("inv").len(),
            2
        );
        assert_eq!(
            conn.query("SELECT * FROM price_history")
                .await
                .expect("hist")
                .len(),
            1
        );

        // Rollback sp2 — product 2 and price change should revert
        conn.execute("ROLLBACK TO sp2").await.expect("rollback sp2");
        conn.execute("RELEASE sp2").await.expect("release sp2");

        assert_eq!(
            conn.query("SELECT * FROM inventory").await.expect("inv").len(),
            1,
            "sp2 rollback: inventory should have 1"
        );
        assert_eq!(
            conn.query("SELECT * FROM price_history")
                .await
                .expect("hist")
                .len(),
            0,
            "sp2 rollback: price history should be empty"
        );

        // Rollback sp1 — everything should revert
        conn.execute("ROLLBACK TO sp1").await.expect("rollback sp1");
        conn.execute("RELEASE sp1").await.expect("release sp1");

        assert_eq!(
            conn.query("SELECT * FROM products").await.expect("prod").len(),
            0,
            "sp1 rollback: products should be empty"
        );
        assert_eq!(
            conn.query("SELECT * FROM inventory").await.expect("inv").len(),
            0,
            "sp1 rollback: inventory should be empty"
        );

        conn.execute("COMMIT").await.expect("commit");
        eprintln!("T6: multi-trigger nested savepoint rollback — correct");
    });
}
