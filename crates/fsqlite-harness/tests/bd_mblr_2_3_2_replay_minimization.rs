//! E2E contract test for `bd-mblr.2.3.2` failure replay/minimization harness.
//!
//! Validates:
//! - deterministic replay package construction from seed/profile/artifact bundle,
//! - minimization that preserves invariant violation,
//! - before/after evidence and replay recipe serialization.

use fsqlite_harness::differential_v2::{
    NormalizedValue, Outcome, StatementDivergence, StmtOutcome,
};
use fsqlite_harness::mismatch_minimizer::{MinimizerConfig, Subsystem};
use fsqlite_harness::replay_harness::{
    build_replay_minimization_package, DriftDetectorConfig, ReplayConfig, ReplayEntryResult,
    ReplayMinimizationPackage, ReplayMinimizationRequest, ReplaySession,
};

const BEAD_ID: &str = "bd-mblr.2.3.2";

fn make_entry(id: &str, outcome: Outcome) -> ReplayEntryResult {
    ReplayEntryResult {
        entry_id: id.to_owned(),
        outcome,
        statements_total: 10,
        statements_mismatched: if outcome == Outcome::Divergence { 1 } else { 0 },
        mismatch_rate: if outcome == Outcome::Divergence {
            0.1
        } else {
            0.0
        },
        subsystem: if outcome == Outcome::Divergence {
            Some(Subsystem::Vdbe)
        } else {
            None
        },
        artifact_id: format!("artifact-{id}"),
    }
}

fn make_divergence(index: usize, sql: &str) -> StatementDivergence {
    StatementDivergence {
        index,
        sql: sql.to_owned(),
        csqlite_outcome: StmtOutcome::Rows(vec![vec![NormalizedValue::Integer(1)]]),
        fsqlite_outcome: StmtOutcome::Rows(vec![vec![NormalizedValue::Integer(2)]]),
    }
}

#[test]
fn test_bd_mblr_2_3_2_replay_minimization_package_contract() {
    let config = ReplayConfig {
        drift_config: DriftDetectorConfig {
            window_size: 2,
            warmup_windows: 1,
            sensitivity_threshold: 0.5,
            ..DriftDetectorConfig::default()
        },
        base_seed: 20260213,
        ..ReplayConfig::default()
    };
    let mut session = ReplaySession::new(config);
    session.record_entry(make_entry("e1", Outcome::Pass));
    session.record_entry(make_entry("e2", Outcome::Divergence));
    let summary = session.finalize();

    let schema = vec!["CREATE TABLE t(a INTEGER PRIMARY KEY);".to_owned()];
    let workload = vec![
        "INSERT INTO t VALUES (1);".to_owned(),
        "INSERT INTO t VALUES (42);".to_owned(),
        "UPDATE t SET a = a + 1 WHERE a = 1;".to_owned(),
        "SELECT a FROM t WHERE a = 42;".to_owned(),
        "DELETE FROM t WHERE a = 99;".to_owned(),
    ];
    let minimizer = MinimizerConfig::default();
    let repro = |_: &[String], candidate: &[String]| {
        let failing_index = candidate
            .iter()
            .position(|stmt| stmt.contains("SELECT a FROM t WHERE a = 42"))?;
        Some(vec![make_divergence(
            failing_index,
            "SELECT a FROM t WHERE a = 42",
        )])
    };

    let request = ReplayMinimizationRequest {
        run_id: "run-bd-mblr-2-3-2",
        fault_profile_id: "fault-profile-replay-min",
        artifact_bundle_id: "artifact-bundle-replay-min",
        schema: &schema,
        workload: &workload,
        minimizer_config: &minimizer,
    };
    let package = build_replay_minimization_package(&summary, &request, &repro)
        .expect("bead_id=bd-mblr.2.3.2 package should be produced");

    assert_eq!(package.run_id, "run-bd-mblr-2-3-2");
    assert_eq!(package.base_seed, 20260213);
    assert_eq!(
        package.evidence.before.evidence_hash, summary.summary_hash,
        "bead_id={BEAD_ID} case=before_hash",
    );
    assert!(
        package.evidence.after.statement_count < package.evidence.before.statement_count,
        "bead_id={BEAD_ID} case=statement_reduction before={} after={}",
        package.evidence.before.statement_count,
        package.evidence.after.statement_count,
    );
    assert!(
        package.evidence.invariant_violation_preserved,
        "bead_id={BEAD_ID} case=violation_preserved",
    );
    assert!(
        package
            .replay_recipe
            .contains("--artifact-bundle artifact-bundle-replay-min"),
        "bead_id={BEAD_ID} case=replay_recipe_has_artifact_bundle",
    );

    let json = package
        .to_json()
        .expect("bead_id=bd-mblr.2.3.2 package serialize");
    let decoded = ReplayMinimizationPackage::from_json(&json)
        .expect("bead_id=bd-mblr.2.3.2 package deserialize");
    assert_eq!(decoded.schema_version, package.schema_version);
    assert_eq!(decoded.run_id, package.run_id);
    assert_eq!(decoded.base_seed, package.base_seed);
    assert_eq!(
        decoded.minimal_reproduction.signature.hash,
        package.minimal_reproduction.signature.hash
    );
    assert_eq!(
        decoded.evidence, package.evidence,
        "bead_id={BEAD_ID} case=json_roundtrip",
    );
}

#[test]
fn test_bd_mblr_2_3_2_package_requires_metadata() {
    let config = ReplayConfig::default();
    let mut session = ReplaySession::new(config);
    session.record_entry(make_entry("e1", Outcome::Divergence));
    let summary = session.finalize();

    let schema = vec!["CREATE TABLE t(a INTEGER PRIMARY KEY);".to_owned()];
    let workload = vec!["SELECT a FROM t WHERE a = 42;".to_owned()];
    let minimizer = MinimizerConfig::default();
    let repro = |_: &[String], candidate: &[String]| {
        candidate
            .iter()
            .position(|stmt| stmt.contains("SELECT a FROM t WHERE a = 42"))
            .map(|idx| vec![make_divergence(idx, "SELECT a FROM t WHERE a = 42")])
    };

    let request = ReplayMinimizationRequest {
        run_id: "",
        fault_profile_id: "fault-profile",
        artifact_bundle_id: "artifact-bundle",
        schema: &schema,
        workload: &workload,
        minimizer_config: &minimizer,
    };
    let package = build_replay_minimization_package(&summary, &request, &repro);
    assert!(
        package.is_none(),
        "bead_id={BEAD_ID} case=missing_run_id_rejected",
    );
}
