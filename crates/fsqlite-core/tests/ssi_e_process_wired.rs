//! Integration tests for the SSI e-process gate wired into the real
//! commit path. These tests cover the "does it actually skip?" behaviour
//! (separate from the API-level contract covered in
//! `ssi_e_process_gate.rs`).
//!
//! Safety contract under test:
//!
//! 1. Under `write_merge = SAFE`, the gate is never consulted and the
//!    commit path runs full SSI validation on every concurrent commit.
//! 2. Under `write_merge = LAB_UNSAFE`, the gate eventually opens on a
//!    pivot-free workload and grants at least some skips. The final DB
//!    state matches a SAFE-mode run of the same workload byte-for-byte.
//! 3. An adversarial workload that injects a true page-level write-write
//!    conflict must NOT cross the e-process threshold; FCW (first-
//!    committer-wins) catches the conflict regardless of SSI skip.

use fsqlite_core::connection::{Connection, WriteMergeMode};

/// Only commits that actually select the SSI preparer may train its gate.
/// The policy is captured at BEGIN, so changing the PRAGMA inside a transaction
/// must not relabel an FCW-only commit as an observed clean SSI validation.
#[test]
fn begin_time_fcw_policy_does_not_train_the_ssi_gate() {
    asupersync::test_utils::run_test(|| async {
        for mode in ["SAFE", "LAB_UNSAFE"] {
            let conn = Connection::open(":memory:").await.unwrap();
            assert!(conn.is_concurrent_mode_default());
            conn.execute_batch(
                "CREATE TABLE policy_gate(id INTEGER PRIMARY KEY, value INTEGER);
                 INSERT INTO policy_gate VALUES(1,0);",
            )
            .await
            .unwrap();
            conn.execute(&format!("PRAGMA fsqlite.write_merge={mode};"))
                .await
                .unwrap();
            conn.reset_ssi_e_process_gate();
            let before = conn.ssi_e_process_snapshot();
            assert_eq!(before.observations, 0);

            conn.execute_batch(
                "PRAGMA fsqlite.serializable=OFF;
                 BEGIN;
                 UPDATE policy_gate SET value=value+1 WHERE id=1;
                 PRAGMA fsqlite.serializable=ON;
                 COMMIT;",
            )
            .await
            .unwrap();
            let fcw = conn.ssi_e_process_snapshot();
            assert_eq!(
                fcw.observations, before.observations,
                "{mode}: FCW-only validation must not fabricate an SSI observation"
            );
            assert_eq!(fcw.clean_streak, before.clean_streak);
            assert_eq!(fcw.e_value.to_bits(), before.e_value.to_bits());
            assert_eq!(
                fcw.skip_consultations, before.skip_consultations,
                "{mode}: BEGIN-time FCW-only policy must not consult the SSI audit gate"
            );
            assert_eq!(fcw.skip_grants, before.skip_grants);

            conn.execute_batch(
                "BEGIN;
                 UPDATE policy_gate SET value=value+1 WHERE id=1;
                 PRAGMA fsqlite.serializable=OFF;
                 COMMIT;",
            )
            .await
            .unwrap();
            let ssi = conn.ssi_e_process_snapshot();
            assert_eq!(ssi.observations, before.observations + 1);
            assert_eq!(ssi.clean_streak, before.clean_streak + 1);
            assert_eq!(ssi.skip_grants, 0, "an untrained gate cannot skip validation");
            assert_eq!(
                conn.query("SELECT value FROM policy_gate WHERE id=1;")
                    .await
                    .unwrap()[0]
                    .values(),
                &[fsqlite_types::SqliteValue::Integer(2)]
            );
            eprintln!(
                "bead_id=bd-6hdwo.14 event=begin_policy_ssi_training_verified \
                 mode={mode} fcw_observations={} ssi_observations={}",
                fcw.observations, ssi.observations
            );
            conn.close().await.unwrap();
        }
    });
}

/// Deterministic workload: N serial `BEGIN CONCURRENT` transactions on
/// disjoint keys. Returns `SUM(v) FROM kv`.
async fn run_pivot_free_workload(conn: &Connection, commits: usize) -> i64 {
    conn.execute_batch("CREATE TABLE IF NOT EXISTS kv (k INTEGER PRIMARY KEY, v INTEGER);")
        .await
        .unwrap();
    for i in 0..commits {
        let k = i + 1;
        let v = ((i * 131 + 7) % 10_007) as i64;
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

/// LAB_UNSAFE + a long pivot-free workload: the gate must open (clean
/// streak past threshold, e-value in `Clear/Watching` state, at least
/// some `should_skip_ssi` grants). The final DB state must equal the
/// SAFE-mode run.
#[test]
fn lab_unsafe_wired_commit_path_opens_gate_and_matches_safe() {
    asupersync::test_utils::run_test(|| async {
        let commits = 512;

        // SAFE baseline.
        let safe_sum = {
            let conn = Connection::open(":memory:").await.unwrap();
            assert_eq!(conn.write_merge_mode(), WriteMergeMode::Safe);
            let sum = run_pivot_free_workload(&conn, commits).await;
            // Under SAFE, the gate must never open regardless of outcomes
            // auto-fed by the commit path.
            let snap = conn.ssi_e_process_snapshot();
            assert_eq!(
                snap.skip_grants, 0,
                "SAFE must never grant a skip; snap={snap}"
            );
            sum
        };

        // LAB_UNSAFE: rely on the wired commit path to feed observations.
        let (lab_sum, lab_snap) = {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute_batch(
                "PRAGMA fsqlite.write_merge = LAB_UNSAFE;
             PRAGMA fsqlite.ssi_e_process_alpha = 0.001;",
            )
            .await
            .unwrap();
            assert_eq!(conn.write_merge_mode(), WriteMergeMode::LabUnsafe);
            let sum = run_pivot_free_workload(&conn, commits).await;
            (sum, conn.ssi_e_process_snapshot())
        };

        assert_eq!(
            safe_sum, lab_sum,
            "LAB_UNSAFE must produce identical aggregate as SAFE on a pivot-free workload"
        );

        // The wired commit path must have fed real observations.
        assert!(
            lab_snap.observations > 0,
            "LAB_UNSAFE commit path must auto-feed the e-process; snap={lab_snap}"
        );
        // Under a clean workload, the e-process must stay below threshold.
        assert!(
            !matches!(lab_snap.alert_state, fsqlite_mvcc::GateAlertState::Alert),
            "clean workload must not trip the gate to Alert; snap={lab_snap}"
        );
        // Some commits should be audit-sampled (even when the gate wants to
        // skip, `periodic_sample_rate` forces a real observation fraction).
        // After enough commits we require at least SOME skip grants, unless
        // the audit stride perfectly aligned with our session-id/commit-seq
        // mix (extremely unlikely at commits = 512).
        assert!(
            lab_snap.skip_consultations > 0,
            "LAB_UNSAFE commit path must consult the gate; snap={lab_snap}"
        );
        assert!(
            lab_snap.skip_grants > 0,
            "the real pivot-free workload must actually skip SSI; snap={lab_snap}"
        );
    });
}

/// Two real connections attempt the same page concurrently. The loser must
/// be rejected by page admission or FCW, and that rejection cannot invent
/// an SSI observation. Verify the winner's exact row after both close.
#[test]
fn lab_unsafe_same_page_conflict_does_not_train_gate() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("actual-same-page-conflict.db");
        let path = path.to_str().unwrap();
        let first = Connection::open(path).await.unwrap();
        first
            .execute_batch(
                "PRAGMA journal_mode=WAL;
                 CREATE TABLE kv(k INTEGER PRIMARY KEY, v INTEGER);
                 INSERT INTO kv VALUES(1,10);",
            )
            .await
            .unwrap();
        let second = Connection::open(path).await.unwrap();
        for conn in [&first, &second] {
            assert!(conn.is_concurrent_mode_default());
            conn.execute("PRAGMA fsqlite.write_merge=LAB_UNSAFE;")
                .await
                .unwrap();
            conn.reset_ssi_e_process_gate();
            conn.execute("BEGIN;").await.unwrap();
        }
        let before = second.ssi_e_process_snapshot();
        first.execute("UPDATE kv SET v=11 WHERE k=1;").await.unwrap();
        let second_write = second.execute("UPDATE kv SET v=99 WHERE k=1;").await;
        first.execute("COMMIT;").await.unwrap();
        let (phase, error) = match second_write {
            Ok(_) => (
                "commit",
                second.execute("COMMIT;").await.expect_err("both same-page writers cannot commit"),
            ),
            Err(error) => ("write", error),
        };
        assert!(error.is_transient(), "expected retryable {phase} conflict, got {error:?}");
        let after = second.ssi_e_process_snapshot();
        assert_eq!(after.observations, before.observations, "{phase} conflict did not discover SSI edges");
        assert_eq!(after.clean_streak, before.clean_streak);
        assert_eq!(after.e_value.to_bits(), before.e_value.to_bits());
        assert_eq!(after.skip_grants, before.skip_grants);
        second.execute("ROLLBACK;").await.unwrap();
        first.close().await.unwrap();
        second.close().await.unwrap();
        let reopened = Connection::open(path).await.unwrap();
        assert_eq!(reopened.query("SELECT k,v FROM kv;").await.unwrap()[0].values(), &[fsqlite_types::SqliteValue::Integer(1), fsqlite_types::SqliteValue::Integer(11)]);
        eprintln!("bead_id=bd-6hdwo.14 event=actual_same_page_conflict phase={phase} error={error:?} observations_before={} observations_after={}", before.observations, after.observations);
        reopened.close().await.unwrap();
    });
}

/// Feeding a synthetic conflict observation stream via the Rust API
/// must still force the gate into Alert and keep the commit path safe
/// (skipping is disallowed under Alert). This pins the adversarial
/// contract: conflicts push the e-value above `1/α` regardless of how
/// the observation was sourced.
#[test]
fn synthetic_conflict_stream_traps_gate_in_alert_and_disables_skip() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute_batch(
            "PRAGMA fsqlite.write_merge = LAB_UNSAFE;
         PRAGMA fsqlite.ssi_e_process_alpha = 0.001;",
        )
        .await
        .unwrap();

        // Pad to `min_observations`.
        for _ in 0..64 {
            conn.observe_ssi_outcome(false);
        }
        // Now inject a conflict burst — at the default p0 = 1e-4 each
        // conflict contributes ln(50) ≈ 3.9 to log_e. Five of them = 19.6
        // which is well above ln(1000) ≈ 6.9.
        for _ in 0..5 {
            conn.observe_ssi_outcome(true);
        }
        let snap = conn.ssi_e_process_snapshot();
        assert_eq!(
            snap.alert_state,
            fsqlite_mvcc::GateAlertState::Alert,
            "5 conflicts must fire the gate; snap={snap}"
        );
        // `should_skip_ssi_validation` must refuse to grant under Alert.
        for h in 0..64u64 {
            assert!(
                !conn.should_skip_ssi_validation(h),
                "skip must be forbidden under Alert; h={h}"
            );
        }
        // And the wired commit path must still execute fine under Alert.
        conn.execute_batch("CREATE TABLE adv (k INTEGER PRIMARY KEY);")
            .await
            .unwrap();
        for i in 0..16 {
            conn.execute_batch(&format!(
                "BEGIN CONCURRENT; INSERT INTO adv(k) VALUES ({i}); COMMIT;"
            ))
            .await
            .unwrap();
        }
        let after = conn.ssi_e_process_snapshot();
        assert_eq!(
            after.skip_grants, snap.skip_grants,
            "no skips may be granted while in Alert; before={snap} after={after}"
        );
    });
}
