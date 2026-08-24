//! Count-semantics validation for the bd-zywqc.11.1 hot-path metric wiring.
//!
//! The registry and `/metrics` endpoint are unit-tested in fsqlite-observability;
//! this pins the ENGINE side — that a successful explicit COMMIT increments
//! `commits_total` exactly once (never double-counting a busy retry) and a
//! ROLLBACK never does. `commits_total` is scoped to explicit COMMIT
//! transactions; single-statement autocommit commits reach durability through a
//! separate, branchy resolver and are intentionally NOT counted here (documented
//! follow-up on the bead, alongside page_lock/history).

use fsqlite_core::connection::Connection;
use fsqlite_observability::metrics::{global, metrics_disabled};

#[test]
fn commits_total_counts_each_explicit_commit_once_and_ignores_rollback() {
    // Counters are no-ops when the subsystem is disabled; the assertion only
    // holds when metrics are on (the default), so skip cleanly otherwise.
    if metrics_disabled() {
        return;
    }
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.expect("open");
        conn.execute("CREATE TABLE t (n INTEGER);")
            .await
            .expect("create table");

        // Snapshot AFTER open + the autocommit CREATE (which this metric does not
        // count) so the delta reflects only the explicit commits below.
        let before = global().commits_total.get();

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
            after_commits - before,
            COMMITS,
            "each successful explicit COMMIT must count exactly once (no double-count, no miss)"
        );
        assert_eq!(
            after_rollbacks, after_commits,
            "ROLLBACK must not increment commits_total"
        );
    });
}
