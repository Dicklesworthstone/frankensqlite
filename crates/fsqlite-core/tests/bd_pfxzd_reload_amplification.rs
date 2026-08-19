//! bd-pfxzd (GH#368) regression guard: the per-write in-txn memdb refresh must
//! take the schema-cookie fast path (bd-ixf69), so N single-row INSERTs inside
//! ONE explicit transaction do NOT each trigger a full `sqlite_master` scan.
//!
//! GH#368 measured 618x whole-database read amplification on published 0.3.5,
//! where the per-write refresh rescanned `sqlite_master` on every statement.
//! bd-ixf69 (db1cf5eb0) enabled the schema-cookie fast path at that site
//! (`allow_dirty_schema_only_fast_path=true`), making the per-statement refresh
//! O(1). This test locks that in by counting `memdb_txn_schema_full_scans`: it
//! must stay a small constant, NEVER proportional to the statement count. bd-dsxu2
//! reverted this same fast path once for a since-fixed FK reason; this keeper (plus
//! the issue110 FK cache tests) is what stops a third round-trip.

use std::sync::{Mutex, MutexGuard};

use fsqlite_core::connection::{
    Connection, hot_path_profile_enabled, hot_path_profile_snapshot, reset_hot_path_profile,
    set_hot_path_profile_enabled,
};

static PROFILE_LOCK: Mutex<()> = Mutex::new(());

struct ProfileGuard {
    _lock: MutexGuard<'static, ()>,
    previous_enabled: bool,
}

impl ProfileGuard {
    fn new() -> Self {
        let lock = PROFILE_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let previous_enabled = hot_path_profile_enabled();
        set_hot_path_profile_enabled(true);
        reset_hot_path_profile();
        Self {
            _lock: lock,
            previous_enabled,
        }
    }
}

impl Drop for ProfileGuard {
    fn drop(&mut self) {
        reset_hot_path_profile();
        set_hot_path_profile_enabled(self.previous_enabled);
    }
}

#[test]
fn bd_pfxzd_in_txn_inserts_are_o1_schema_scans() {
    asupersync::test_utils::run_test(|| async {
        let _guard = ProfileGuard::new();
        let conn = Connection::open(":memory:").await.unwrap();

        // A multi-object schema so a per-statement full sqlite_master scan would be
        // a real, countable cost — and so the fast path is meaningful.
        for i in 0..12 {
            conn.execute(&format!(
                "CREATE TABLE t{i}(a INTEGER PRIMARY KEY, b TEXT);"
            ))
            .await
            .unwrap();
        }

        // Measure ONLY the in-txn write phase: reset after the schema is built.
        reset_hot_path_profile();

        const N: u64 = 60;
        conn.execute("BEGIN;").await.unwrap();
        for i in 0..N {
            conn.execute(&format!("INSERT INTO t0(b) VALUES ('row{i}');"))
                .await
                .unwrap();
        }
        conn.execute("COMMIT;").await.unwrap();

        let full_scans = hot_path_profile_snapshot().memdb_txn_schema_full_scans;

        // O(1): the schema-cookie fast path (bd-ixf69) means the per-write refresh
        // never rescans sqlite_master, so only a small constant of full scans can
        // occur (begin/commit boundaries), NEVER ~N. A regression to O(N) reads
        // like a hang from outside (GH#368). N/4 conservatively separates the two
        // populations (constant vs linear) without being flaky on boundary scans.
        assert!(
            full_scans < N / 4,
            "N={N} in-txn INSERTs triggered {full_scans} full sqlite_master scans — \
             the schema-cookie fast path regressed toward O(N) (bd-pfxzd / GH#368; \
             cf. the bd-dsxu2 revert). Expected a small constant."
        );

        // Sanity: the counters are actually wired (profiling on, snapshot works).
        assert!(
            hot_path_profile_enabled(),
            "profiling must be enabled for this measurement"
        );
    });
}
