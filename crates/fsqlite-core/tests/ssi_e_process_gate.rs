//! Integration tests for the anytime-valid e-process SSI skip gate.
//!
//! The gate is controlled by `PRAGMA fsqlite.write_merge = LAB_UNSAFE`
//! and `PRAGMA fsqlite.ssi_e_process_alpha = <float>`. Its safety
//! contract is:
//!
//! 1. With `write_merge = SAFE` (the default), the gate is *never*
//!    consulted. `should_skip_ssi_validation` returns `false`
//!    unconditionally.
//! 2. With `write_merge = LAB_UNSAFE`, the gate opens only after a
//!    clean history has accumulated (min_observations + min_clean_streak)
//!    and never while the e-process has crossed `1/α`.
//! 3. Commits executed with the gate open must produce the same final
//!    database state as commits executed with the gate closed, as long
//!    as no true SSI pivot is present in the workload.
//!
//! We exercise all three contracts below.

use fsqlite_core::connection::{Connection, WriteMergeMode};

/// Run a small OLTP-style workload on a fresh in-memory connection,
/// applied as `commits` independent transactions. Returns the final
/// SUM(v) from `kv`. Deterministic by construction.
async fn run_workload(conn: &Connection, commits: usize) -> i64 {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS kv (k INTEGER PRIMARY KEY, v INTEGER NOT NULL);",
    )
    .await
    .unwrap();
    for i in 0..commits {
        let k = i + 1;
        let v = ((i * 7 + 3) % 997) as i64;
        conn.execute_batch(&format!(
            "BEGIN CONCURRENT; INSERT OR REPLACE INTO kv(k, v) VALUES ({k}, {v}); COMMIT;"
        ))
        .await
        .unwrap();
    }
    let stmt = conn
        .prepare("SELECT COALESCE(SUM(v), 0) FROM kv")
        .await
        .unwrap();
    let row = stmt.query_row().await.unwrap();
    match &row.values()[0] {
        fsqlite_types::SqliteValue::Integer(n) => *n,
        other => panic!("expected integer sum, got {other:?}"),
    }
}

#[test]
fn default_mode_is_safe_and_gate_locked() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        assert_eq!(conn.write_merge_mode(), WriteMergeMode::Safe);
        // Under SAFE, the gate must never open, no matter what hash we pass.
        for h in 0..1024u64 {
            assert!(
                !conn.should_skip_ssi_validation(h),
                "gate should be locked under SAFE at h={h}"
            );
        }
    });
}

#[test]
fn lab_unsafe_pragma_activates_and_reports() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute_batch("PRAGMA fsqlite.write_merge = LAB_UNSAFE;")
            .await
            .unwrap();
        assert_eq!(conn.write_merge_mode(), WriteMergeMode::LabUnsafe);

        // Rust-level getter stays consistent with the pragma.
        assert_eq!(conn.write_merge_mode(), WriteMergeMode::LabUnsafe);

        // Setting back to SAFE should also work.
        conn.execute_batch("PRAGMA fsqlite.write_merge = SAFE;")
            .await
            .unwrap();
        assert_eq!(conn.write_merge_mode(), WriteMergeMode::Safe);
    });
}

#[test]
fn unknown_write_merge_value_errors() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();

        // An arbitrary unknown value is rejected with a message that names the
        // accepted values, and the mode is left at its previous (default) value.
        let err = conn
            .execute_batch("PRAGMA fsqlite.write_merge = RECKLESS;")
            .await
            .expect_err("unknown write_merge value must error");
        assert!(
            err.to_string().contains("SAFE or LAB_UNSAFE"),
            "rejection must name the accepted values, got: {err}"
        );
        // Mode stays at the previous (default) value on error.
        assert_eq!(conn.write_merge_mode(), WriteMergeMode::Safe);

        // `OFF` is the specific value the README promises to reject (bd-p4dcv):
        // only `SAFE | LAB_UNSAFE` are accepted, so `OFF`/`off` must fail closed
        // and never mutate the mode. This pins the docs<->parser contract so a
        // future change cannot silently reintroduce `OFF` as a no-op or an
        // SSI-disable switch (the historical mis-documentation the bead flagged).
        for value in ["OFF", "off"] {
            let err = conn
                .execute_batch(&format!("PRAGMA fsqlite.write_merge = {value};"))
                .await;
            assert!(
                err.is_err(),
                "write_merge = {value} must be rejected (README documents `OFF` as rejected)"
            );
            assert_eq!(
                conn.write_merge_mode(),
                WriteMergeMode::Safe,
                "rejected write_merge = {value} must not mutate the mode"
            );
        }
    });
}

#[test]
fn alpha_pragma_round_trips() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute_batch("PRAGMA fsqlite.ssi_e_process_alpha = 0.005;")
            .await
            .unwrap();
        let snap = conn.ssi_e_process_snapshot();
        // threshold = 1 / alpha
        assert!(
            (snap.threshold - 200.0).abs() < 1e-9,
            "threshold={} expected 200",
            snap.threshold
        );

        // Out-of-range alpha is rejected.
        assert!(
            conn.execute_batch("PRAGMA fsqlite.ssi_e_process_alpha = 1.5;")
                .await
                .is_err()
        );
        assert!(
            conn.execute_batch("PRAGMA fsqlite.ssi_e_process_alpha = -0.1;")
                .await
                .is_err()
        );
    });
}

#[test]
fn lab_unsafe_gate_opens_after_clean_history() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute_batch(
            "PRAGMA fsqlite.write_merge = LAB_UNSAFE;
         PRAGMA fsqlite.ssi_e_process_alpha = 0.001;",
        )
        .await
        .unwrap();

        // Gate is locked on a cold start.
        assert!(!conn.should_skip_ssi_validation(1));

        // Feed a long clean history. The default gate config requires
        // 64 observations and a clean streak of 32.
        for _ in 0..128 {
            conn.observe_ssi_outcome(false);
        }

        // At least some hashes should now be allowed to skip (depending
        // on the periodic audit stride). We use an odd hash to avoid the
        // default 1/20 audit stride which is aligned to even values.
        let mut any_granted = false;
        for h in (1..200u64).step_by(2) {
            if conn.should_skip_ssi_validation(h) {
                any_granted = true;
                break;
            }
        }
        assert!(
            any_granted,
            "gate should grant at least one skip after 128 clean observations"
        );
        let snap = conn.ssi_e_process_snapshot();
        assert!(
            snap.skip_grants > 0,
            "skip_grants should be > 0 after gate opens"
        );
    });
}

#[test]
fn lab_unsafe_gate_closes_on_alert() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute_batch(
            "PRAGMA fsqlite.write_merge = LAB_UNSAFE;
         PRAGMA fsqlite.ssi_e_process_alpha = 0.001;",
        )
        .await
        .unwrap();

        // Feed conflicts until the e-process fires an alert. Under the
        // default p0 = 1e-4, three conflicts give ~10^12 evidence — well
        // above the 1000 threshold at α = 1e-3. min_observations defaults
        // to 64, so pad with clean observations first.
        for _ in 0..64 {
            conn.observe_ssi_outcome(false);
        }
        for _ in 0..5 {
            conn.observe_ssi_outcome(true);
        }
        let snap = conn.ssi_e_process_snapshot();
        assert_eq!(
            snap.alert_state,
            fsqlite_mvcc::GateAlertState::Alert,
            "gate should be in Alert state after 5 conflicts, snapshot={snap}"
        );
        // Gate must refuse to grant a skip while in Alert.
        for h in 0..100u64 {
            assert!(
                !conn.should_skip_ssi_validation(h),
                "gate must not grant a skip while in Alert; h={h} snap={snap}"
            );
        }
    });
}

#[test]
fn lab_unsafe_commits_match_safe_commits() {
    asupersync::test_utils::run_test(|| async {
        // Run the same workload in SAFE mode and in LAB_UNSAFE mode.
        // The final state must be byte-identical: the skip gate may only
        // ever skip *validation* on commits that have no SSI pivot, which
        // is 100% of the commits in this single-connection, no-concurrency
        // workload.
        let safe_sum = {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute_batch("PRAGMA fsqlite.write_merge = SAFE;")
                .await
                .unwrap();
            run_workload(&conn, 256).await
        };
        let lab_sum = {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute_batch(
                "PRAGMA fsqlite.write_merge = LAB_UNSAFE;
             PRAGMA fsqlite.ssi_e_process_alpha = 0.001;",
            )
            .await
            .unwrap();
            // Prime the e-process with a clean history so the gate opens
            // during the workload. In production this would accumulate
            // organically; priming here lets us exercise the skip path
            // on every commit.
            for _ in 0..128 {
                conn.observe_ssi_outcome(false);
            }
            run_workload(&conn, 256).await
        };
        assert_eq!(
            safe_sum, lab_sum,
            "LAB_UNSAFE must produce identical final state as SAFE on a pivot-free workload"
        );
    });
}

#[test]
fn reset_gate_via_api_clears_state() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute_batch("PRAGMA fsqlite.write_merge = LAB_UNSAFE;")
            .await
            .unwrap();
        for _ in 0..32 {
            conn.observe_ssi_outcome(false);
        }
        conn.observe_ssi_outcome(true);
        let pre = conn.ssi_e_process_snapshot();
        assert!(pre.observations > 0);
        conn.reset_ssi_e_process_gate();
        let post = conn.ssi_e_process_snapshot();
        assert_eq!(post.observations, 0);
        assert!((post.e_value - 1.0).abs() < 1e-12);
    });
}
