//! Count-semantics validation for the bd-zywqc.11.1 hot-path metric wiring.
//!
//! The registry and `/metrics` endpoint are unit-tested in fsqlite-observability;
//! this pins the ENGINE side — that `commits_total` counts each durable commit
//! exactly once (never double-counting a busy retry) and that rollbacks and
//! read-only statements never touch it. Coverage:
//!   * explicit `COMMIT` transactions (the original bd-zywqc.11.1 wiring), and
//!   * single-statement autocommit writes, which reach durability through a
//!     separate, branchy resolver (bd-zywqc.11.1.3). Autocommit writes are the
//!     common case; before .3 they were silently uncounted, so `commits_total`
//!     undercounted real commit throughput.
//!
//! `commits_total` is a process-GLOBAL counter, so the two scenarios live in one
//! `#[test]` and run sequentially: cargo runs `#[test]` fns on parallel threads,
//! and a sibling test committing concurrently would leak into these exact-delta
//! assertions. One test in this dedicated binary means the deltas are the only
//! mutations of the counter in-flight.

use fsqlite_core::connection::Connection;
use fsqlite_observability::metrics::{global, metrics_disabled};

#[test]
fn commits_total_counts_durable_commits_and_ignores_rollbacks_reads_and_failures() {
    // Counters are no-ops when the subsystem is disabled; the assertions only
    // hold when metrics are on (the default), so skip cleanly otherwise.
    if metrics_disabled() {
        return;
    }
    asupersync::test_utils::run_test(|| async {
        // ---- Scenario 1: explicit COMMIT / ROLLBACK -------------------------
        let conn = Connection::open(":memory:").await.expect("open");
        conn.execute("CREATE TABLE t (n INTEGER);")
            .await
            .expect("create table");

        // Snapshot AFTER open + the autocommit CREATE so the delta reflects only
        // the explicit commits below.
        let before_explicit = global().commits_total.get();

        const COMMITS: u64 = 20;
        for i in 0..COMMITS {
            conn.execute("BEGIN;").await.expect("begin");
            conn.execute(&format!("INSERT INTO t VALUES ({i});"))
                .await
                .expect("insert");
            conn.execute("COMMIT;").await.expect("commit");
        }
        let after_commits = global().commits_total.get();

        const ROLLBACKS: u64 = 7;
        for i in 0..ROLLBACKS {
            conn.execute("BEGIN;").await.expect("begin");
            conn.execute(&format!("INSERT INTO t VALUES ({i});"))
                .await
                .expect("insert");
            conn.execute("ROLLBACK;").await.expect("rollback");
        }
        let after_rollbacks = global().commits_total.get();

        assert_eq!(
            after_commits - before_explicit,
            COMMITS,
            "each successful explicit COMMIT must count exactly once (no double-count, no miss)"
        );
        assert_eq!(
            after_rollbacks, after_commits,
            "ROLLBACK must not increment commits_total"
        );
        drop(conn);

        // ---- Scenario 2: single-statement autocommit writes (bd-zywqc.11.1.3)
        // These reach durability through the autocommit resolver
        // (`commit_and_retain` fast path / normal `txn.commit()` tail) and the
        // retained-batch flush, NOT the explicit-COMMIT path above.
        let conn = Connection::open(":memory:").await.expect("open");
        // Retained autocommit batching (default ON) legitimately coalesces many
        // parked writes into a single durable flush — one commit, not N. Disable
        // it so every write below is its own durable commit and the one-per-write
        // count semantics are exercised directly.
        conn.execute("PRAGMA fsqlite.autocommit_retain = OFF;")
            .await
            .expect("disable autocommit batching");
        conn.execute("CREATE TABLE u (id INTEGER PRIMARY KEY, n INTEGER);")
            .await
            .expect("create table");

        // Snapshot AFTER open + the DDL autocommit (itself a counted write) so the
        // deltas below reflect only the statements we drive.
        let before_auto = global().commits_total.get();

        // Read-only autocommit statements must never increment commits_total.
        const READS: u64 = 5;
        for _ in 0..READS {
            let _ = conn.query("SELECT count(*) FROM u;").await.expect("select");
        }
        assert_eq!(
            global().commits_total.get(),
            before_auto,
            "read-only autocommit statements must not increment commits_total"
        );

        // Each successful single-statement autocommit write counts exactly once.
        const WRITES: u64 = 20;
        for i in 0..WRITES {
            conn.execute(&format!("INSERT INTO u VALUES ({i}, {i});"))
                .await
                .expect("insert");
        }
        let after_writes = global().commits_total.get();
        assert_eq!(
            after_writes - before_auto,
            WRITES,
            "each successful autocommit write must count exactly once (no miss, no double-count)"
        );

        // A failed autocommit write (duplicate PRIMARY KEY) rolls back and must
        // not increment commits_total.
        const FAILS: u64 = 7;
        for _ in 0..FAILS {
            conn.execute("INSERT INTO u VALUES (0, 999);")
                .await
                .expect_err("duplicate PRIMARY KEY must fail");
        }
        assert_eq!(
            global().commits_total.get(),
            after_writes,
            "a failed autocommit statement must not increment commits_total"
        );
    });
}
