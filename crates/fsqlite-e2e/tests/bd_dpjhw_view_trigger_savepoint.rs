//! bd-dpjhw: VIEW materialization not transaction-aware in TRIGGERS +
//! SAVEPOINT ROLLBACK.
//!
//! When a trigger modifies data visible through a VIEW, and the enclosing
//! SAVEPOINT is rolled back, the VIEW's materialized state should reflect the
//! rollback. If VIEW materialization is not transaction-aware, the aborted
//! trigger's side effects leak into subsequent SELECTs.
//!
//! ## Minimal repro
//!
//! 1. CREATE TABLE t + CREATE VIEW v AS SELECT ... FROM t
//! 2. CREATE TRIGGER that INSERTs into t
//! 3. SAVEPOINT sp1
//! 4. INSERT that fires the trigger (adds rows to t, visible via v)
//! 5. ROLLBACK TO sp1
//! 6. SELECT * FROM v → should see pre-trigger state, not aborted state
#![recursion_limit = "512"]

use fsqlite::Connection;

async fn open_conn() -> Connection {
    Connection::open(":memory:").await.expect("open :memory:")
}

fn csqlite_conn() -> rusqlite::Connection {
    rusqlite::Connection::open_in_memory().expect("csqlite open")
}

fn csqlite_query_count(conn: &rusqlite::Connection, sql: &str) -> usize {
    let mut stmt = conn.prepare(sql).expect("csqlite prepare");
    stmt.query_map([], |_| Ok(()))
        .expect("csqlite query")
        .count()
}

// ─── V1: Basic VIEW + trigger + SAVEPOINT ROLLBACK ──────────────────

#[test]
fn v1_view_after_trigger_savepoint_rollback() {
    asupersync::test_utils::run_test(|| async {
        let conn = open_conn().await;

        conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
            .await
            .expect("create items");
        conn.execute("CREATE TABLE item_log (item_id INTEGER, action TEXT)")
            .await
            .expect("create item_log");
        conn.execute("CREATE VIEW v_log AS SELECT item_id, action FROM item_log")
            .await
            .expect("create view");
        conn.execute(
            "CREATE TRIGGER t_log AFTER INSERT ON items \
             BEGIN INSERT INTO item_log VALUES (NEW.id, 'created'); END",
        )
        .await
        .expect("create trigger");

        // Seed baseline
        conn.execute("INSERT INTO items VALUES (1, 'baseline')")
            .await
            .expect("seed");

        let before = conn
            .query("SELECT * FROM v_log")
            .await
            .expect("before count");
        let before_count = before.len();
        assert_eq!(before_count, 1, "baseline: 1 log entry");

        // SAVEPOINT → INSERT (fires trigger) → ROLLBACK
        conn.execute("SAVEPOINT sp1").await.expect("savepoint");
        conn.execute("INSERT INTO items VALUES (2, 'rolled-back')")
            .await
            .expect("insert in savepoint");

        // Mid-savepoint: view should show 2 entries
        let mid = conn.query("SELECT * FROM v_log").await.expect("mid count");
        assert_eq!(mid.len(), 2, "mid-savepoint: 2 log entries");

        conn.execute("ROLLBACK TO sp1").await.expect("rollback");
        conn.execute("RELEASE sp1").await.expect("release");

        // After rollback: view must show only 1 entry (the baseline)
        let after = conn
            .query("SELECT * FROM v_log")
            .await
            .expect("after count");
        assert_eq!(
            after.len(),
            before_count,
            "BUG: VIEW shows {} rows after ROLLBACK, expected {} (trigger state leaked)",
            after.len(),
            before_count
        );
    });
}

// ─── V2: Oracle parity — compare with C SQLite ─────────────────────

#[test]
fn v2_view_trigger_savepoint_oracle_parity() {
    asupersync::test_utils::run_test(|| async {
        // C SQLite (rusqlite) as oracle
        let c = csqlite_conn();
        c.execute_batch(
            "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT);
         CREATE TABLE item_log (item_id INTEGER, action TEXT);
         CREATE VIEW v_log AS SELECT item_id, action FROM item_log;
         CREATE TRIGGER t_log AFTER INSERT ON items
           BEGIN INSERT INTO item_log VALUES (NEW.id, 'created'); END;
         INSERT INTO items VALUES (1, 'baseline');",
        )
        .expect("csqlite setup");

        let c_before = csqlite_query_count(&c, "SELECT * FROM v_log");

        c.execute_batch(
            "SAVEPOINT sp1;
         INSERT INTO items VALUES (2, 'rolled-back');
         ROLLBACK TO sp1;
         RELEASE sp1;",
        )
        .expect("csqlite savepoint cycle");

        let c_after = csqlite_query_count(&c, "SELECT * FROM v_log");
        assert_eq!(
            c_before, c_after,
            "csqlite oracle: view count must not change"
        );

        // FrankenSQLite
        let f = open_conn().await;
        f.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
            .await
            .expect("f create items");
        f.execute("CREATE TABLE item_log (item_id INTEGER, action TEXT)")
            .await
            .expect("f create item_log");
        f.execute("CREATE VIEW v_log AS SELECT item_id, action FROM item_log")
            .await
            .expect("f create view");
        f.execute(
            "CREATE TRIGGER t_log AFTER INSERT ON items \
             BEGIN INSERT INTO item_log VALUES (NEW.id, 'created'); END",
        )
        .await
        .expect("f create trigger");
        f.execute("INSERT INTO items VALUES (1, 'baseline')")
            .await
            .expect("f seed");

        let _f_before = f
            .query("SELECT * FROM v_log")
            .await
            .expect("f before")
            .len();

        f.execute("SAVEPOINT sp1").await.expect("f savepoint");
        f.execute("INSERT INTO items VALUES (2, 'rolled-back')")
            .await
            .expect("f insert");
        f.execute("ROLLBACK TO sp1").await.expect("f rollback");
        f.execute("RELEASE sp1").await.expect("f release");

        let f_after = f.query("SELECT * FROM v_log").await.expect("f after").len();

        assert_eq!(
            f_after, c_after,
            "PARITY BUG: fsqlite view shows {} rows, csqlite shows {} after SAVEPOINT ROLLBACK",
            f_after, c_after
        );
    });
}

// ─── V3: Nested savepoints with trigger + view ─────────────────────

#[test]
fn v3_nested_savepoint_trigger_view() {
    asupersync::test_utils::run_test(|| async {
        let conn = open_conn().await;

        conn.execute("CREATE TABLE data (id INTEGER PRIMARY KEY, val INTEGER)")
            .await
            .expect("create");
        conn.execute("CREATE TABLE audit (data_id INTEGER)")
            .await
            .expect("create audit");
        conn.execute("CREATE VIEW v_audit AS SELECT data_id FROM audit")
            .await
            .expect("create view");
        conn.execute(
            "CREATE TRIGGER t_audit AFTER INSERT ON data \
             BEGIN INSERT INTO audit VALUES (NEW.id); END",
        )
        .await
        .expect("create trigger");

        conn.execute("INSERT INTO data VALUES (1, 100)")
            .await
            .expect("seed");
        let baseline = conn.query("SELECT * FROM v_audit").await.expect("q").len();
        assert_eq!(baseline, 1);

        // Outer savepoint (avoid 'outer' — parser treats it as reserved keyword)
        conn.execute("SAVEPOINT sp_outer").await.expect("outer sp");
        conn.execute("INSERT INTO data VALUES (2, 200)")
            .await
            .expect("insert outer");

        // Inner savepoint — commit this one
        conn.execute("SAVEPOINT sp_inner").await.expect("inner sp");
        conn.execute("INSERT INTO data VALUES (3, 300)")
            .await
            .expect("insert inner");
        conn.execute("RELEASE sp_inner")
            .await
            .expect("release inner");

        // View should show 3 entries (baseline + outer + inner)
        let mid = conn.query("SELECT * FROM v_audit").await.expect("mid").len();
        assert_eq!(mid, 3);

        // Rollback outer — both outer and inner inserts should revert
        conn.execute("ROLLBACK TO sp_outer")
            .await
            .expect("rollback outer");
        conn.execute("RELEASE sp_outer")
            .await
            .expect("release outer");

        let after = conn
            .query("SELECT * FROM v_audit")
            .await
            .expect("after")
            .len();
        assert_eq!(
            after, baseline,
            "BUG: nested savepoint rollback didn't revert view (got {after}, expected {baseline})"
        );
    });
}

// ─── V4: UPDATE trigger + view + rollback ───────────────────────────

#[test]
fn v4_update_trigger_view_rollback() {
    asupersync::test_utils::run_test(|| async {
        let conn = open_conn().await;

        conn.execute("CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance INTEGER)")
            .await
            .expect("create");
        conn.execute(
            "CREATE TABLE balance_history (acct_id INTEGER, old_bal INTEGER, new_bal INTEGER)",
        )
        .await
        .expect("create history");
        conn.execute(
            "CREATE VIEW v_history AS SELECT acct_id, old_bal, new_bal FROM balance_history",
        )
        .await
        .expect("create view");
        conn.execute(
            "CREATE TRIGGER t_history AFTER UPDATE ON accounts \
             BEGIN INSERT INTO balance_history VALUES (NEW.id, OLD.balance, NEW.balance); END",
        )
        .await
        .expect("create trigger");

        conn.execute("INSERT INTO accounts VALUES (1, 1000)")
            .await
            .expect("seed");

        let before = conn.query("SELECT * FROM v_history").await.expect("q").len();
        assert_eq!(before, 0, "no history before any update");

        conn.execute("SAVEPOINT sp").await.expect("savepoint");
        conn.execute("UPDATE accounts SET balance = 500 WHERE id = 1")
            .await
            .expect("update");

        let mid = conn
            .query("SELECT * FROM v_history")
            .await
            .expect("mid")
            .len();
        assert_eq!(mid, 1, "one history entry after update");

        conn.execute("ROLLBACK TO sp").await.expect("rollback");
        conn.execute("RELEASE sp").await.expect("release");

        let after = conn
            .query("SELECT * FROM v_history")
            .await
            .expect("after")
            .len();
        assert_eq!(
            after, before,
            "BUG: UPDATE trigger history leaked through SAVEPOINT ROLLBACK via VIEW (got {after})"
        );

        // Also verify the base table was rolled back
        let balance = conn
            .query("SELECT balance FROM accounts WHERE id = 1")
            .await
            .expect("balance");
        assert_eq!(balance.len(), 1);
    });
}

// ─── V5: DELETE trigger + view + rollback ───────────────────────────

#[test]
fn v5_delete_trigger_view_rollback() {
    asupersync::test_utils::run_test(|| async {
        let conn = open_conn().await;

        conn.execute("CREATE TABLE records (id INTEGER PRIMARY KEY, data TEXT)")
            .await
            .expect("create");
        conn.execute("CREATE TABLE trash (rec_id INTEGER, deleted_data TEXT)")
            .await
            .expect("create trash");
        conn.execute("CREATE VIEW v_trash AS SELECT rec_id, deleted_data FROM trash")
            .await
            .expect("create view");
        conn.execute(
            "CREATE TRIGGER t_trash AFTER DELETE ON records \
             BEGIN INSERT INTO trash VALUES (OLD.id, OLD.data); END",
        )
        .await
        .expect("create trigger");

        conn.execute("INSERT INTO records VALUES (1, 'keep me')")
            .await
            .expect("seed");

        conn.execute("SAVEPOINT sp").await.expect("savepoint");
        conn.execute("DELETE FROM records WHERE id = 1")
            .await
            .expect("delete");

        let mid_trash = conn.query("SELECT * FROM v_trash").await.expect("mid").len();
        assert_eq!(mid_trash, 1, "trash has one entry mid-savepoint");

        conn.execute("ROLLBACK TO sp").await.expect("rollback");
        conn.execute("RELEASE sp").await.expect("release");

        // After rollback: record should be back, trash should be empty
        let records = conn
            .query("SELECT * FROM records")
            .await
            .expect("records")
            .len();
        assert_eq!(records, 1, "record must be restored after rollback");

        let trash = conn
            .query("SELECT * FROM v_trash")
            .await
            .expect("trash")
            .len();
        assert_eq!(
            trash, 0,
            "BUG: DELETE trigger trash leaked through SAVEPOINT ROLLBACK via VIEW (got {trash})"
        );
    });
}

// ─── V6: Multiple triggers on same INSERT + view + rollback ─────────

#[test]
fn v6_multi_trigger_view_rollback() {
    asupersync::test_utils::run_test(|| async {
        let conn = open_conn().await;

        conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, amount INTEGER)")
            .await
            .expect("create orders");
        conn.execute("CREATE TABLE order_log (order_id INTEGER, phase TEXT)")
            .await
            .expect("create log");
        conn.execute("CREATE TABLE stats (total_orders INTEGER)")
            .await
            .expect("create stats");
        conn.execute("INSERT INTO stats VALUES (0)")
            .await
            .expect("seed stats");

        conn.execute("CREATE VIEW v_log AS SELECT order_id, phase FROM order_log")
            .await
            .expect("create view");

        // Two triggers on the same table
        conn.execute(
            "CREATE TRIGGER t_log AFTER INSERT ON orders \
             BEGIN INSERT INTO order_log VALUES (NEW.id, 'placed'); END",
        )
        .await
        .expect("create log trigger");
        conn.execute(
            "CREATE TRIGGER t_stats AFTER INSERT ON orders \
             BEGIN UPDATE stats SET total_orders = total_orders + 1; END",
        )
        .await
        .expect("create stats trigger");

        conn.execute("SAVEPOINT sp").await.expect("savepoint");
        conn.execute("INSERT INTO orders VALUES (1, 100)")
            .await
            .expect("insert");

        let mid_log = conn
            .query("SELECT * FROM v_log")
            .await
            .expect("mid log")
            .len();
        assert_eq!(mid_log, 1);

        conn.execute("ROLLBACK TO sp").await.expect("rollback");
        conn.execute("RELEASE sp").await.expect("release");

        let after_log = conn
            .query("SELECT * FROM v_log")
            .await
            .expect("after log")
            .len();
        assert_eq!(
            after_log, 0,
            "BUG: multi-trigger log leaked through ROLLBACK via VIEW (got {after_log})"
        );

        let stats = conn
            .query("SELECT total_orders FROM stats")
            .await
            .expect("stats");
        assert_eq!(stats.len(), 1);
    });
}

// ─── V7: View with JOIN across trigger-modified tables + rollback ───
// CONFIRMED BUG: trigger side-effects leak through VIEW JOIN after
// SAVEPOINT ROLLBACK. The view shows 2 rows instead of 1.

#[test]
#[ignore = "bd-dpjhw: VIEW JOIN shows aborted trigger state after SAVEPOINT ROLLBACK"]
fn v7_view_join_trigger_rollback() {
    asupersync::test_utils::run_test(|| async {
        let conn = open_conn().await;

        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
            .await
            .expect("create users");
        conn.execute("CREATE TABLE events (id INTEGER PRIMARY KEY, user_id INTEGER, action TEXT)")
            .await
            .expect("create events");
        conn.execute(
            "CREATE VIEW v_user_events AS \
             SELECT u.name, e.action FROM users u JOIN events e ON u.id = e.user_id",
        )
        .await
        .expect("create view");

        conn.execute(
            "CREATE TRIGGER t_welcome AFTER INSERT ON users \
             BEGIN INSERT INTO events (user_id, action) VALUES (NEW.id, 'welcome'); END",
        )
        .await
        .expect("create trigger");

        // Seed a user outside savepoint
        conn.execute("INSERT INTO users VALUES (1, 'alice')")
            .await
            .expect("seed");
        let baseline = conn
            .query("SELECT * FROM v_user_events")
            .await
            .expect("baseline")
            .len();
        assert_eq!(baseline, 1, "alice has welcome event");

        // Add user in savepoint then rollback
        conn.execute("SAVEPOINT sp").await.expect("savepoint");
        conn.execute("INSERT INTO users VALUES (2, 'bob')")
            .await
            .expect("insert bob");

        let mid = conn
            .query("SELECT * FROM v_user_events")
            .await
            .expect("mid")
            .len();
        assert_eq!(mid, 2, "two welcome events mid-savepoint");

        conn.execute("ROLLBACK TO sp").await.expect("rollback");
        conn.execute("RELEASE sp").await.expect("release");

        let after = conn
            .query("SELECT * FROM v_user_events")
            .await
            .expect("after")
            .len();
        assert_eq!(
            after, baseline,
            "BUG: VIEW JOIN shows {} rows after rollback, expected {} (trigger state leaked)",
            after, baseline
        );
    });
}
