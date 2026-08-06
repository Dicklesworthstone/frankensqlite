use std::collections::BTreeMap;

use fsqlite_harness::failure_bundle::{EnvironmentInfo, ExecutionLaneEvidence};
use fsqlite_harness::mismatch_minimizer::{
    HISTORY_REDUCTION_SNAPSHOT_KEY, HistoryReductionCase, HistoryReductionKind,
    HistoryReductionObservation, HistoryReductionResult, TypedReductionConfig,
    TypedReductionStatus, minimize_history_case,
};
use fsqlite_harness::replay_harness::{
    SerializabilityReplayArtifact, replay_serializability_oracle,
};
use fsqlite_harness::serializability_oracle::{
    AnomalyKind, BeginMode, HistoryEvent, HistoryOperation, HistoryValue, HistoryWorkload,
    OracleVerdict, ScheduleProvenance, SerializabilityBundleContext,
    TRANSACTION_HISTORY_SCHEMA_VERSION, TransactionHistory, build_serializability_failure_bundle,
    check_history, validate_serializability_failure_bundle,
};
use proptest::prelude::*;

const RUN_ID: &str = "run-history-reduction";
const TRACE_ID: &str = "trace-history-reduction";
const SCENARIO_ID: &str = "TURSO-HISTORY-REDUCTION";

fn event(
    event_id: u64,
    logical_time: u64,
    process_id: &str,
    transaction_id: Option<&str>,
    operation: HistoryOperation,
) -> HistoryEvent {
    HistoryEvent {
        event_id,
        logical_time,
        process_id: process_id.to_owned(),
        connection_id: transaction_id.unwrap_or("control").to_owned(),
        transaction_id: transaction_id.map(str::to_owned),
        operation,
    }
}

fn reduction_case(auxiliary_count: usize) -> HistoryReductionCase {
    let mut final_state = BTreeMap::new();
    final_state.insert("x".to_owned(), HistoryValue::Integer(1));
    final_state.insert("y".to_owned(), HistoryValue::Integer(1));
    let mut history = TransactionHistory {
        schema_version: TRANSACTION_HISTORY_SCHEMA_VERSION.to_owned(),
        run_id: RUN_ID.to_owned(),
        trace_id: TRACE_ID.to_owned(),
        scenario_id: SCENARIO_ID.to_owned(),
        seed: 19,
        engine_git_sha: "0123456789abcdef".to_owned(),
        engine_dirty: false,
        workload: HistoryWorkload::WriteSkew { minimum_sum: 1 },
        schedule: ScheduleProvenance::observation_only("synthetic-history-reducer-fixture"),
        execution_lane_evidence: vec![ExecutionLaneEvidence::semantic_only(
            TRACE_ID,
            RUN_ID,
            SCENARIO_ID,
            "transaction-history-reduction",
        )],
        concurrent_mode_enabled: true,
        reopen_concurrent_mode_enabled: Some(true),
        initial_state: BTreeMap::new(),
        final_state,
        final_state_sha256: String::new(),
        events: vec![
            event(
                0,
                0,
                "worker-main",
                Some("t1"),
                HistoryOperation::Begin {
                    mode: BeginMode::Concurrent,
                },
            ),
            event(
                1,
                1,
                "worker-main",
                Some("t2"),
                HistoryOperation::Begin {
                    mode: BeginMode::Concurrent,
                },
            ),
            event(
                2,
                2,
                "worker-main",
                Some("t1"),
                HistoryOperation::Read {
                    key: "y".to_owned(),
                    value: HistoryValue::Integer(0),
                    version: None,
                    source_transaction_id: None,
                },
            ),
            event(
                3,
                3,
                "worker-main",
                Some("t2"),
                HistoryOperation::Read {
                    key: "x".to_owned(),
                    value: HistoryValue::Integer(0),
                    version: None,
                    source_transaction_id: None,
                },
            ),
            event(
                4,
                4,
                "worker-main",
                Some("t1"),
                HistoryOperation::Read {
                    key: "unrelated-main".to_owned(),
                    value: HistoryValue::Null,
                    version: None,
                    source_transaction_id: None,
                },
            ),
            event(
                5,
                5,
                "worker-main",
                Some("t1"),
                HistoryOperation::Write {
                    key: "x".to_owned(),
                    value: HistoryValue::Integer(1),
                    page_number: Some(2),
                },
            ),
            event(
                6,
                6,
                "worker-main",
                Some("t2"),
                HistoryOperation::Write {
                    key: "y".to_owned(),
                    value: HistoryValue::Integer(1),
                    page_number: Some(3),
                },
            ),
            event(7, 7, "worker-main", Some("t1"), HistoryOperation::Commit),
            event(8, 8, "worker-main", Some("t2"), HistoryOperation::Commit),
            event(
                9,
                9,
                "worker-main",
                Some("t-noise-main"),
                HistoryOperation::Begin {
                    mode: BeginMode::Concurrent,
                },
            ),
            event(
                10,
                10,
                "worker-main",
                Some("t-noise-main"),
                HistoryOperation::Read {
                    key: "unrelated-main".to_owned(),
                    value: HistoryValue::Null,
                    version: None,
                    source_transaction_id: None,
                },
            ),
            event(
                11,
                11,
                "worker-main",
                Some("t-noise-main"),
                HistoryOperation::Commit,
            ),
            event(
                12,
                12,
                "worker-noise",
                Some("t-noise"),
                HistoryOperation::Begin {
                    mode: BeginMode::Concurrent,
                },
            ),
            event(
                13,
                13,
                "worker-noise",
                Some("t-noise"),
                HistoryOperation::Read {
                    key: "unrelated".to_owned(),
                    value: HistoryValue::Null,
                    version: None,
                    source_transaction_id: None,
                },
            ),
            event(
                14,
                14,
                "worker-noise",
                Some("t-noise"),
                HistoryOperation::Commit,
            ),
            event(
                15,
                15,
                "worker-main",
                None,
                HistoryOperation::Checkpoint {
                    mode: "passive".to_owned(),
                },
            ),
            event(
                16,
                16,
                "worker-main",
                None,
                HistoryOperation::Crash {
                    crash_id: "crash-1".to_owned(),
                },
            ),
            event(
                17,
                17,
                "worker-main",
                None,
                HistoryOperation::Restart {
                    crash_id: "crash-1".to_owned(),
                },
            ),
        ],
    };
    history.refresh_final_state_hash();

    HistoryReductionCase {
        history,
        schedule_events: (0..auxiliary_count)
            .map(|index| format!("schedule-{index}"))
            .collect(),
        yield_choices: (0..auxiliary_count)
            .map(|index| format!("yield-{index}"))
            .collect(),
        observed_fields: (0..auxiliary_count)
            .map(|index| (format!("field-{index}"), format!("value-{index}")))
            .collect(),
    }
}

fn verify_exact_failure(
    case: &HistoryReductionCase,
) -> Result<HistoryReductionObservation, String> {
    let report = case.validate()?;
    let witness = report
        .minimal_witness
        .as_ref()
        .ok_or_else(|| "expected a stable serializability witness".to_owned())?;
    let signature = format!(
        "{:?}:{}:{}",
        witness.kind,
        witness.transaction_ids.join(","),
        witness.keys.join(",")
    );
    HistoryReductionObservation::from_case(case, signature)
}

#[test]
fn semantic_history_reduction_preserves_public_witness_and_oracle_replay() {
    let case = reduction_case(2);
    let config = TypedReductionConfig {
        max_attempts: 256,
        cancel_after_attempts: None,
    };
    let first = minimize_history_case(&case, &config, &verify_exact_failure)
        .expect("reduce deterministic history");
    let second = minimize_history_case(&case, &config, &verify_exact_failure)
        .expect("repeat deterministic history reduction");

    assert_eq!(first.status, TypedReductionStatus::Complete);
    assert_eq!(
        first.to_json().expect("encode first"),
        second.to_json().expect("encode second")
    );
    assert_eq!(first.stats.original.transactions, 4);
    assert_eq!(first.stats.minimized.transactions, 2);
    assert!(first.stats.minimized.operations < first.stats.original.operations);
    assert_eq!(first.stats.minimized.schedule_events, 0);
    assert_eq!(first.stats.minimized.yield_choices, 0);
    assert_eq!(first.stats.minimized.checkpoints, 0);
    assert_eq!(first.stats.minimized.crash_points, 0);
    assert_eq!(first.stats.minimized.observed_fields, 0);
    assert_eq!(first.observation.verdict, OracleVerdict::Rejected);
    assert_eq!(
        first
            .observation
            .minimal_witness
            .as_ref()
            .map(|witness| witness.kind),
        Some(AnomalyKind::G2Cycle)
    );
    for kind in [
        HistoryReductionKind::Transaction,
        HistoryReductionKind::Worker,
        HistoryReductionKind::Operation,
        HistoryReductionKind::Checkpoint,
        HistoryReductionKind::CrashPoint,
        HistoryReductionKind::ScheduleEvent,
        HistoryReductionKind::YieldChoice,
        HistoryReductionKind::ObservedField,
    ] {
        assert!(
            first
                .trace
                .iter()
                .any(|attempt| attempt.kind == kind && attempt.accepted),
            "no accepted {kind:?} reduction"
        );
    }

    let irreducible = minimize_history_case(&first.minimized, &config, &verify_exact_failure)
        .expect("re-run minimized history to a fixed point");
    assert_eq!(irreducible.status, TypedReductionStatus::Complete);
    assert_eq!(irreducible.minimized, first.minimized);
    assert_eq!(irreducible.stats.accepted_candidates, 0);

    let report = check_history(&first.minimized.history).expect("check minimized history");
    let mut bundle = build_serializability_failure_bundle(
        &first.minimized.history,
        &report,
        SerializabilityBundleContext {
            bundle_id: "fb-history-reduction-1".to_owned(),
            created_at: "2026-08-06T00:00:00Z".to_owned(),
            test_name: "history_reduction_public_replay".to_owned(),
            script_path: Some(
                "crates/fsqlite-harness/tests/bd_turso_test_adaptation_zu081_19_history_reduction.rs"
                    .to_owned(),
            ),
            repro_command: "cargo test -p fsqlite-harness --test bd_turso_test_adaptation_zu081_19_history_reduction"
                .to_owned(),
            environment: EnvironmentInfo::new("0123456789abcdef", "nightly", "test"),
        },
    )
    .expect("build canonical serializability bundle");
    let mut conflicting_bundle = bundle.clone();
    conflicting_bundle.state_snapshots.insert(
        HISTORY_REDUCTION_SNAPSHOT_KEY.to_owned(),
        "different canonical evidence".to_owned(),
    );
    conflicting_bundle.content_hash = conflicting_bundle.deterministic_bundle_hash();
    assert_eq!(
        first.attach_to_failure_bundle(&mut conflicting_bundle),
        Err("history reduction bundle already contains different canonical evidence".to_owned())
    );
    assert_eq!(
        conflicting_bundle
            .state_snapshots
            .get(HISTORY_REDUCTION_SNAPSHOT_KEY)
            .map(String::as_str),
        Some("different canonical evidence")
    );
    first
        .attach_to_failure_bundle(&mut bundle)
        .expect("attach reduction to canonical bundle");
    first
        .attach_to_failure_bundle(&mut bundle)
        .expect("reattaching identical canonical evidence is idempotent");
    assert!(
        bundle
            .state_snapshots
            .contains_key(HISTORY_REDUCTION_SNAPSHOT_KEY)
    );
    let (bundled_history, bundled_report) =
        validate_serializability_failure_bundle(&bundle).expect("validate augmented bundle");
    assert_eq!(bundled_history, first.minimized.history);
    assert_eq!(bundled_report, report);

    let replay =
        SerializabilityReplayArtifact::from_history(first.minimized.history.clone(), Some(bundle))
            .expect("build canonical replay artifact");
    let encoded_replay = replay.to_json().expect("encode replay artifact");
    let decoded_replay = SerializabilityReplayArtifact::from_json_strict(&encoded_replay)
        .expect("decode replay artifact");
    assert_eq!(
        replay_serializability_oracle(&decoded_replay).expect("replay public oracle"),
        report
    );

    let encoded_reduction = first.to_json().expect("encode reduction");
    assert!(
        HistoryReductionResult::from_json_strict(&encoded_reduction[..encoded_reduction.len() / 2])
            .is_err()
    );
    let mut lane_drift = first.clone();
    lane_drift.observation.required_lanes.clear();
    lane_drift.content_hash = lane_drift.deterministic_hash();
    assert!(lane_drift.validate().is_err());
}

#[test]
fn cancellation_and_budget_exhaustion_return_valid_partial_reductions() {
    let case = reduction_case(1);
    let cancelled = minimize_history_case(
        &case,
        &TypedReductionConfig {
            max_attempts: 32,
            cancel_after_attempts: Some(0),
        },
        &verify_exact_failure,
    )
    .expect("cancelled reduction remains valid");
    assert_eq!(cancelled.status, TypedReductionStatus::Cancelled);
    assert!(cancelled.trace.is_empty());
    assert_eq!(cancelled.minimized, case);

    let exhausted = minimize_history_case(
        &case,
        &TypedReductionConfig {
            max_attempts: 1,
            cancel_after_attempts: None,
        },
        &verify_exact_failure,
    )
    .expect("budget-limited reduction remains valid");
    assert_eq!(exhausted.status, TypedReductionStatus::BudgetExhausted);
    assert_eq!(exhausted.trace.len(), 1);
    assert!(exhausted.to_json().is_ok());

    let mut malformed = case;
    malformed.schedule_events = vec!["duplicate".to_owned(), "duplicate".to_owned()];
    assert!(
        minimize_history_case(
            &malformed,
            &TypedReductionConfig::default(),
            &verify_exact_failure
        )
        .is_err()
    );
}

#[test]
fn deterministic_schedule_claim_requires_execution_backed_replay_adapter() {
    let mut case = reduction_case(0);
    case.history.schedule = ScheduleProvenance::deterministic(
        "synthetic-history-reducer-fixture",
        "unexecuted-schedule",
        "a".repeat(64),
        "cargo test -p fsqlite-harness --test bd_turso_test_adaptation_zu081_19_history_reduction",
    );
    assert_eq!(
        minimize_history_case(
            &case,
            &TypedReductionConfig::default(),
            &verify_exact_failure,
        ),
        Err(
            "history reduction deterministic schedules require an execution-backed replay adapter"
                .to_owned()
        )
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]

    #[test]
    fn auxiliary_dimensions_reduce_monotonically(auxiliary_count in 0usize..6) {
        let case = reduction_case(auxiliary_count);
        let result = minimize_history_case(
            &case,
            &TypedReductionConfig {
                max_attempts: 256,
                cancel_after_attempts: None,
            },
            &verify_exact_failure,
        )
        .map_err(proptest::test_runner::TestCaseError::fail)?;
        prop_assert!(result.stats.minimized.operations <= result.stats.original.operations);
        prop_assert_eq!(result.stats.minimized.schedule_events, 0);
        prop_assert_eq!(result.stats.minimized.yield_choices, 0);
        prop_assert_eq!(result.stats.minimized.observed_fields, 0);
        prop_assert_eq!(
            result.observation,
            verify_exact_failure(&result.minimized)
                .map_err(proptest::test_runner::TestCaseError::fail)?
        );
    }
}
