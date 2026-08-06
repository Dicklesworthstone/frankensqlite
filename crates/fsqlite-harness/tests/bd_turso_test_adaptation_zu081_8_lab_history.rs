//! bd-turso-test-adaptation-zu081.8: production SQL histories under LabRuntime.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard};

use asupersync::lab::{LabConfig, LabRuntime};
use asupersync::runtime::yield_now;
use asupersync::types::Budget;
use fsqlite::{AsyncConnection, ConnectionEnv, IoPollStrategy, Row, RuntimeConfig, SqliteValue};
use fsqlite_harness::failure_bundle::{ExecutionLaneEvidence, ObservedExecutionLane};
use fsqlite_harness::serializability_oracle::{
    BeginMode, HistoryEvent, HistoryOperation, HistoryValue, HistoryWorkload, OracleVerdict,
    ScheduleControl, ScheduleProvenance, SerializabilityReport, TransactionHistory, check_history,
};
use fsqlite_harness::test_inventory::ExecutionLane;
use fsqlite_types::cx::Cx as FsqliteCx;
use sha2::{Digest, Sha256};

const BEAD_ID: &str = "bd-turso-test-adaptation-zu081.8";
const TRACE_ID: &str = "turso-lab-history-trace";
const PROCESS_ID: &str = "lab-process-0";
const CANCEL_BUDGET_SCENARIO_ID: &str = "bd-zu081-8-cancel-budget";
const CANCEL_BUDGET_SEED: u64 = 9_001;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LabHistoryCase {
    DisjointWriters,
    SameRowConflict,
}

impl LabHistoryCase {
    const fn scenario_id(self) -> &'static str {
        match self {
            Self::DisjointWriters => "bd-zu081-8-disjoint-writers",
            Self::SameRowConflict => "bd-zu081-8-same-row-conflict",
        }
    }

    const fn run_id(self) -> &'static str {
        match self {
            Self::DisjointWriters => "bd-zu081-8-disjoint-seed-5150",
            Self::SameRowConflict => "bd-zu081-8-conflict-seed-5151",
        }
    }

    const fn seed(self) -> u64 {
        match self {
            Self::DisjointWriters => 5_150,
            Self::SameRowConflict => 5_151,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SqlOperationRecord {
    process_id: String,
    connection_id: String,
    transaction_id: Option<String>,
    operation: HistoryOperation,
}

#[derive(Debug, Default)]
struct EventRecorder {
    records: Vec<SqlOperationRecord>,
}

impl EventRecorder {
    fn record(
        &mut self,
        connection_id: impl Into<String>,
        transaction_id: Option<&str>,
        operation: HistoryOperation,
    ) {
        self.records.push(SqlOperationRecord {
            process_id: PROCESS_ID.to_owned(),
            connection_id: connection_id.into(),
            transaction_id: transaction_id.map(str::to_owned),
            operation,
        });
    }

    fn into_history_events(self) -> Vec<HistoryEvent> {
        self.records
            .into_iter()
            .enumerate()
            .map(|(index, record)| {
                let id = u64::try_from(index).expect("history event index fits in u64");
                HistoryEvent {
                    event_id: id,
                    logical_time: id,
                    process_id: record.process_id,
                    connection_id: record.connection_id,
                    transaction_id: record.transaction_id,
                    operation: record.operation,
                }
            })
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TxnOutcome {
    transaction_id: String,
    committed: bool,
    transient_conflict: bool,
    concurrent_mode_enabled: bool,
}

#[derive(Debug, Clone, Copy)]
struct DisjointWriterPlan {
    connection_id: &'static str,
    transaction_id: &'static str,
    table: &'static str,
    value: i64,
    decision_id: u64,
}

#[derive(Debug, Clone)]
struct LabHistoryArtifact {
    history: TransactionHistory,
    report: SerializabilityReport,
    history_json: String,
    report_json: String,
}

fn lock_recorder(recorder: &Arc<Mutex<EventRecorder>>) -> MutexGuard<'_, EventRecorder> {
    recorder
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn lock_outcomes(outcomes: &Arc<Mutex<Vec<TxnOutcome>>>) -> MutexGuard<'_, Vec<TxnOutcome>> {
    outcomes
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn runtime_config() -> RuntimeConfig {
    // LabRuntime drives the SQL futures synchronously; use production blocking
    // file I/O so no ambient io_uring driver task can escape its scheduler.
    RuntimeConfig {
        worker_threads: 1,
        io_poll_strategy: IoPollStrategy::Blocking,
    }
}

fn sha256_hex(bytes: impl AsRef<[u8]>) -> String {
    fsqlite_harness::bytes_to_lower_hex(Sha256::digest(bytes))
}

fn engine_git_sha() -> String {
    option_env!("FSQLITE_TEST_ENGINE_GIT_SHA")
        .or(option_env!("GITHUB_SHA"))
        .unwrap_or("rch-source-snapshot-git-metadata-unavailable")
        .to_owned()
}

fn engine_dirty() -> bool {
    option_env!("FSQLITE_TEST_ENGINE_DIRTY").is_some_and(|value| value == "1" || value == "true")
}

fn fsqlite_cx(case: LabHistoryCase, decision_id: u64) -> FsqliteCx {
    let native = asupersync::Cx::current().expect("LabRuntime task must install a current Cx");
    let cx = FsqliteCx::new().with_trace_context(case.seed(), decision_id, 0);
    cx.set_native_cx(native);
    cx
}

fn connection_env(cx: &FsqliteCx) -> ConnectionEnv {
    ConnectionEnv::new_with_root_cx(runtime_config(), cx)
}

async fn open_lab_connection(
    case: LabHistoryCase,
    decision_id: u64,
    path: &str,
) -> Result<(AsyncConnection, FsqliteCx), String> {
    let cx = fsqlite_cx(case, decision_id);
    let conn = AsyncConnection::open_with_env(&cx, path.to_owned(), connection_env(&cx))
        .await
        .map_err(|error| format!("open production async connection: {error}"))?;
    Ok((conn, cx))
}

fn open_sync_connection(_case: LabHistoryCase, _decision_id: u64, path: &str) -> AsyncConnection {
    AsyncConnection::open_sync(path.to_owned())
        .expect("sync production async connection should open")
}

fn sqlite_integer(row: &Row, column: usize) -> Result<i64, String> {
    let value = row
        .get(column)
        .ok_or_else(|| format!("column {column} should exist"))?;
    match value {
        SqliteValue::Integer(value) => Ok(*value),
        value => Err(format!("expected integer SQLite value, got {value:?}")),
    }
}

async fn concurrent_mode_enabled(conn: &AsyncConnection, cx: &FsqliteCx) -> Result<bool, String> {
    let rows = conn
        .query(cx, "PRAGMA concurrent_mode;")
        .await
        .map_err(|error| format!("query concurrent mode: {error}"))?;
    let row = rows
        .first()
        .ok_or_else(|| "PRAGMA concurrent_mode returned no rows".to_owned())?;
    Ok(sqlite_integer(row, 0)? == 1)
}

fn setup_disjoint_database(path: &str) {
    let mut conn = open_sync_connection(LabHistoryCase::DisjointWriters, 10, path);
    conn.execute_batch_sync(
        "CREATE TABLE writer_a(id INTEGER PRIMARY KEY, value INTEGER);
         CREATE TABLE writer_b(id INTEGER PRIMARY KEY, value INTEGER);",
    )
    .expect("setup disjoint writer schema");
    conn.close_sync()
        .expect("setup disjoint connection should close");
}

fn setup_conflict_database(path: &str) {
    let mut conn = open_sync_connection(LabHistoryCase::SameRowConflict, 20, path);
    conn.execute_batch_sync(
        "CREATE TABLE conflict_register(id INTEGER PRIMARY KEY, value INTEGER);
         INSERT INTO conflict_register VALUES (1, 0);",
    )
    .expect("setup conflict schema");
    conn.close_sync()
        .expect("setup conflict connection should close");
}

fn verify_disjoint_final_state(path: &str) -> BTreeMap<String, HistoryValue> {
    let mut conn = open_sync_connection(LabHistoryCase::DisjointWriters, 30, path);
    let mode = conn
        .query_sync("PRAGMA concurrent_mode;")
        .expect("query reopen concurrent mode");
    assert_eq!(
        sqlite_integer(mode.first().expect("concurrent mode row"), 0).expect("integer mode value"),
        1,
        "reopened verification connection must keep concurrent mode enabled"
    );
    let a = conn
        .query_sync("SELECT value FROM writer_a WHERE id = 1;")
        .expect("query writer_a final value");
    let b = conn
        .query_sync("SELECT value FROM writer_b WHERE id = 1;")
        .expect("query writer_b final value");
    conn.close_sync()
        .expect("verify disjoint connection should close");

    BTreeMap::from([
        (
            "writer_a/1".to_owned(),
            HistoryValue::Integer(
                sqlite_integer(a.first().expect("writer_a final row"), 0)
                    .expect("writer_a integer value"),
            ),
        ),
        (
            "writer_b/1".to_owned(),
            HistoryValue::Integer(
                sqlite_integer(b.first().expect("writer_b final row"), 0)
                    .expect("writer_b integer value"),
            ),
        ),
    ])
}

fn verify_conflict_final_state(path: &str) -> BTreeMap<String, HistoryValue> {
    let mut conn = open_sync_connection(LabHistoryCase::SameRowConflict, 40, path);
    let mode = conn
        .query_sync("PRAGMA concurrent_mode;")
        .expect("query reopen concurrent mode");
    assert_eq!(
        sqlite_integer(mode.first().expect("concurrent mode row"), 0).expect("integer mode value"),
        1,
        "reopened conflict verification connection must keep concurrent mode enabled"
    );
    let rows = conn
        .query_sync("SELECT value FROM conflict_register WHERE id = 1;")
        .expect("query conflict final value");
    conn.close_sync()
        .expect("verify conflict connection should close");

    BTreeMap::from([(
        "conflict_register/1".to_owned(),
        HistoryValue::Integer(
            sqlite_integer(rows.first().expect("conflict final row"), 0)
                .expect("conflict integer value"),
        ),
    )])
}

async fn run_disjoint_writer(
    path: String,
    plan: DisjointWriterPlan,
    recorder: Arc<Mutex<EventRecorder>>,
    outcomes: Arc<Mutex<Vec<TxnOutcome>>>,
) {
    let (mut conn, cx) =
        open_lab_connection(LabHistoryCase::DisjointWriters, plan.decision_id, &path)
            .await
            .expect("open disjoint lab connection");
    let mode_enabled = concurrent_mode_enabled(&conn, &cx)
        .await
        .expect("query disjoint concurrent mode");
    assert!(mode_enabled, "concurrent mode must start enabled");

    conn.execute(&cx, "BEGIN;")
        .await
        .expect("begin disjoint transaction");
    lock_recorder(&recorder).record(
        plan.connection_id,
        Some(plan.transaction_id),
        HistoryOperation::Begin {
            mode: BeginMode::Concurrent,
        },
    );
    yield_now().await;

    conn.execute(
        &cx,
        &format!("INSERT INTO {} VALUES (1, {});", plan.table, plan.value),
    )
    .await
    .expect("insert disjoint value");
    lock_recorder(&recorder).record(
        plan.connection_id,
        Some(plan.transaction_id),
        HistoryOperation::Write {
            key: format!("{}/1", plan.table),
            value: HistoryValue::Integer(plan.value),
            page_number: None,
        },
    );
    yield_now().await;

    conn.execute(&cx, "COMMIT;")
        .await
        .expect("commit disjoint transaction");
    lock_recorder(&recorder).record(
        plan.connection_id,
        Some(plan.transaction_id),
        HistoryOperation::Commit,
    );
    lock_outcomes(&outcomes).push(TxnOutcome {
        transaction_id: plan.transaction_id.to_owned(),
        committed: true,
        transient_conflict: false,
        concurrent_mode_enabled: mode_enabled,
    });
    conn.close_sync()
        .expect("disjoint lab connection should close");
}

async fn run_same_row_conflict(
    path: String,
    recorder: Arc<Mutex<EventRecorder>>,
    outcomes: Arc<Mutex<Vec<TxnOutcome>>>,
) {
    let (mut winner, winner_cx) = open_lab_connection(LabHistoryCase::SameRowConflict, 51, &path)
        .await
        .expect("open conflict winner");
    let (mut loser, loser_cx) = open_lab_connection(LabHistoryCase::SameRowConflict, 52, &path)
        .await
        .expect("open conflict loser");
    let winner_mode = concurrent_mode_enabled(&winner, &winner_cx)
        .await
        .expect("query winner concurrent mode");
    let loser_mode = concurrent_mode_enabled(&loser, &loser_cx)
        .await
        .expect("query loser concurrent mode");
    assert!(
        winner_mode && loser_mode,
        "both conflict connections need concurrent default"
    );

    winner
        .execute(&winner_cx, "BEGIN;")
        .await
        .expect("winner begin");
    lock_recorder(&recorder).record(
        "conflict-winner",
        Some("txn-winner"),
        HistoryOperation::Begin {
            mode: BeginMode::Concurrent,
        },
    );
    loser
        .execute(&loser_cx, "BEGIN;")
        .await
        .expect("loser begin");
    lock_recorder(&recorder).record(
        "conflict-loser",
        Some("txn-loser"),
        HistoryOperation::Begin {
            mode: BeginMode::Concurrent,
        },
    );
    yield_now().await;

    let winner_read = winner
        .query(
            &winner_cx,
            "SELECT value FROM conflict_register WHERE id = 1;",
        )
        .await
        .expect("winner read");
    lock_recorder(&recorder).record(
        "conflict-winner",
        Some("txn-winner"),
        HistoryOperation::Read {
            key: "conflict_register/1".to_owned(),
            value: HistoryValue::Integer(
                sqlite_integer(winner_read.first().expect("winner read row"), 0)
                    .expect("winner integer value"),
            ),
            version: Some("initial".to_owned()),
            source_transaction_id: None,
        },
    );
    let loser_read = loser
        .query(
            &loser_cx,
            "SELECT value FROM conflict_register WHERE id = 1;",
        )
        .await
        .expect("loser read");
    lock_recorder(&recorder).record(
        "conflict-loser",
        Some("txn-loser"),
        HistoryOperation::Read {
            key: "conflict_register/1".to_owned(),
            value: HistoryValue::Integer(
                sqlite_integer(loser_read.first().expect("loser read row"), 0)
                    .expect("loser integer value"),
            ),
            version: Some("initial".to_owned()),
            source_transaction_id: None,
        },
    );
    yield_now().await;

    winner
        .execute(
            &winner_cx,
            "UPDATE conflict_register SET value = 100 WHERE id = 1;",
        )
        .await
        .expect("winner update");
    lock_recorder(&recorder).record(
        "conflict-winner",
        Some("txn-winner"),
        HistoryOperation::Write {
            key: "conflict_register/1".to_owned(),
            value: HistoryValue::Integer(100),
            page_number: None,
        },
    );
    winner
        .execute(&winner_cx, "COMMIT;")
        .await
        .expect("winner commit");
    lock_recorder(&recorder).record(
        "conflict-winner",
        Some("txn-winner"),
        HistoryOperation::Commit,
    );
    lock_outcomes(&outcomes).push(TxnOutcome {
        transaction_id: "txn-winner".to_owned(),
        committed: true,
        transient_conflict: false,
        concurrent_mode_enabled: winner_mode,
    });
    yield_now().await;

    let update_result = loser
        .execute(
            &loser_cx,
            "UPDATE conflict_register SET value = 200 WHERE id = 1;",
        )
        .await;
    let conflict = match update_result {
        Ok(_) => loser
            .execute(&loser_cx, "COMMIT;")
            .await
            .expect_err("loser commit must fail after same-row conflict"),
        Err(error) => error,
    };
    assert!(
        conflict.is_transient(),
        "same-row loser must fail with a transient FCW conflict, got {conflict}"
    );
    lock_recorder(&recorder).record(
        "conflict-loser",
        Some("txn-loser"),
        HistoryOperation::Conflict {
            reason: "first_committer_wins_transient".to_owned(),
        },
    );
    let _ = loser.execute(&loser_cx, "ROLLBACK;").await;
    lock_recorder(&recorder).record(
        "conflict-loser",
        Some("txn-loser"),
        HistoryOperation::Rollback {
            reason: "fcw_conflict_retry_required".to_owned(),
        },
    );
    lock_outcomes(&outcomes).push(TxnOutcome {
        transaction_id: "txn-loser".to_owned(),
        committed: false,
        transient_conflict: true,
        concurrent_mode_enabled: loser_mode,
    });

    winner
        .close_sync()
        .expect("winner lab connection should close");
    loser
        .close_sync()
        .expect("loser lab connection should close");
}

fn schedule_provenance(
    case: LabHistoryCase,
    report: &asupersync::lab::runtime::LabRunReport,
) -> ScheduleProvenance {
    deterministic_schedule_provenance(
        case.scenario_id(),
        case.seed(),
        report,
        case_test_name(case),
        "production-async-connection",
    )
}

fn cancellation_schedule_provenance(
    report: &asupersync::lab::runtime::LabRunReport,
) -> ScheduleProvenance {
    deterministic_schedule_provenance(
        CANCEL_BUDGET_SCENARIO_ID,
        CANCEL_BUDGET_SEED,
        report,
        "cancellation_and_budget_exhaustion_are_explicit_history_outcomes",
        "cx-cancellation",
    )
}

fn deterministic_schedule_provenance(
    scenario_id: &str,
    seed: u64,
    report: &asupersync::lab::runtime::LabRunReport,
    test_name: &str,
    source_suffix: &str,
) -> ScheduleProvenance {
    let schedule_id = format!(
        "{}:seed={}:steps={}:trace={}:schedule={}",
        scenario_id,
        seed,
        report.steps_total,
        report.trace_fingerprint,
        report.trace_certificate.schedule_hash
    );
    let schedule_sha256 = sha256_hex(schedule_id.as_bytes());
    ScheduleProvenance::deterministic(
        format!("{BEAD_ID}:asupersync-lab-runtime:{source_suffix}"),
        schedule_id,
        schedule_sha256,
        format!(
            "FSQLITE_TEST_ENGINE_GIT_SHA=<sha> RCH_REQUIRE_REMOTE=1 RCH_NO_SELF_HEALING=1 rch exec -- cargo test --locked -p fsqlite-harness --test bd_turso_test_adaptation_zu081_8_lab_history {} -- --exact --test-threads=1",
            test_name
        ),
    )
}

fn case_test_name(case: LabHistoryCase) -> &'static str {
    match case {
        LabHistoryCase::DisjointWriters => {
            "production_lab_history_disjoint_writers_is_deterministic_and_replayable"
        }
        LabHistoryCase::SameRowConflict => {
            "production_lab_history_same_row_conflict_matches_fcw_abort"
        }
    }
}

fn lane_evidence(case: LabHistoryCase) -> ExecutionLaneEvidence {
    ExecutionLaneEvidence::from_observations(
        ExecutionLane::MvccRequired,
        vec![
            ObservedExecutionLane::SqlResult,
            ObservedExecutionLane::Planner,
            ObservedExecutionLane::Vdbe,
            ObservedExecutionLane::PagerBacked,
            ObservedExecutionLane::Mvcc,
        ],
        TRACE_ID,
        case.run_id(),
        case.scenario_id(),
        "sql-transaction-history",
        "file",
        "production-async-connection",
        "fsqlite:async-worker:lab-runtime",
        Vec::new(),
        true,
    )
}

fn build_history(
    case: LabHistoryCase,
    report: &asupersync::lab::runtime::LabRunReport,
    events: Vec<HistoryEvent>,
    final_state: BTreeMap<String, HistoryValue>,
) -> TransactionHistory {
    let mut history = TransactionHistory {
        schema_version: fsqlite_harness::serializability_oracle::TRANSACTION_HISTORY_SCHEMA_VERSION
            .to_owned(),
        run_id: case.run_id().to_owned(),
        trace_id: TRACE_ID.to_owned(),
        scenario_id: case.scenario_id().to_owned(),
        seed: case.seed(),
        engine_git_sha: engine_git_sha(),
        engine_dirty: engine_dirty(),
        workload: HistoryWorkload::Register,
        schedule: schedule_provenance(case, report),
        execution_lane_evidence: vec![lane_evidence(case)],
        concurrent_mode_enabled: true,
        reopen_concurrent_mode_enabled: Some(true),
        initial_state: match case {
            LabHistoryCase::DisjointWriters => BTreeMap::new(),
            LabHistoryCase::SameRowConflict => {
                BTreeMap::from([("conflict_register/1".to_owned(), HistoryValue::Integer(0))])
            }
        },
        final_state,
        final_state_sha256: String::new(),
        events,
    };
    history.refresh_final_state_hash();
    history
}

fn build_cancelled_history(
    report: &asupersync::lab::runtime::LabRunReport,
    events: Vec<HistoryEvent>,
) -> TransactionHistory {
    let mut history = TransactionHistory {
        schema_version: fsqlite_harness::serializability_oracle::TRANSACTION_HISTORY_SCHEMA_VERSION
            .to_owned(),
        run_id: CANCEL_BUDGET_SCENARIO_ID.to_owned(),
        trace_id: TRACE_ID.to_owned(),
        scenario_id: CANCEL_BUDGET_SCENARIO_ID.to_owned(),
        seed: CANCEL_BUDGET_SEED,
        engine_git_sha: engine_git_sha(),
        engine_dirty: engine_dirty(),
        workload: HistoryWorkload::Register,
        schedule: cancellation_schedule_provenance(report),
        execution_lane_evidence: vec![ExecutionLaneEvidence::from_observations(
            ExecutionLane::SqlResultOnly,
            vec![ObservedExecutionLane::SqlResult],
            TRACE_ID,
            CANCEL_BUDGET_SCENARIO_ID,
            CANCEL_BUDGET_SCENARIO_ID,
            "cancelled-sql-history",
            "file",
            "lab-runtime-cancellation",
            "fsqlite:cx-cancel-observed",
            Vec::new(),
            true,
        )],
        concurrent_mode_enabled: true,
        reopen_concurrent_mode_enabled: Some(true),
        initial_state: BTreeMap::new(),
        final_state: BTreeMap::new(),
        final_state_sha256: String::new(),
        events,
    };
    history.refresh_final_state_hash();
    history
}

fn run_lab_case(case: LabHistoryCase) -> LabHistoryArtifact {
    let dir = tempfile::tempdir().expect("tempdir should be created");
    let db_path = dir
        .path()
        .join(format!("{}.db", case.scenario_id()))
        .to_string_lossy()
        .into_owned();
    match case {
        LabHistoryCase::DisjointWriters => setup_disjoint_database(&db_path),
        LabHistoryCase::SameRowConflict => setup_conflict_database(&db_path),
    }

    let recorder = Arc::new(Mutex::new(EventRecorder::default()));
    let outcomes = Arc::new(Mutex::new(Vec::new()));
    let mut runtime = LabRuntime::new(
        LabConfig::new(case.seed())
            .worker_count(2)
            .max_steps(50_000),
    );
    let root = runtime.state.create_root_region(Budget::INFINITE);

    match case {
        LabHistoryCase::DisjointWriters => {
            let (task_a, _) = runtime
                .state
                .create_task(
                    root,
                    Budget::INFINITE,
                    run_disjoint_writer(
                        db_path.clone(),
                        DisjointWriterPlan {
                            connection_id: "writer-a",
                            transaction_id: "txn-a",
                            table: "writer_a",
                            value: 10,
                            decision_id: 41,
                        },
                        Arc::clone(&recorder),
                        Arc::clone(&outcomes),
                    ),
                )
                .expect("spawn disjoint writer A");
            let (task_b, _) = runtime
                .state
                .create_task(
                    root,
                    Budget::INFINITE,
                    run_disjoint_writer(
                        db_path.clone(),
                        DisjointWriterPlan {
                            connection_id: "writer-b",
                            transaction_id: "txn-b",
                            table: "writer_b",
                            value: 20,
                            decision_id: 42,
                        },
                        Arc::clone(&recorder),
                        Arc::clone(&outcomes),
                    ),
                )
                .expect("spawn disjoint writer B");
            let mut scheduler = runtime.scheduler.lock();
            scheduler.schedule(task_a, 0);
            scheduler.schedule(task_b, 0);
        }
        LabHistoryCase::SameRowConflict => {
            let (task, _) = runtime
                .state
                .create_task(
                    root,
                    Budget::INFINITE,
                    run_same_row_conflict(
                        db_path.clone(),
                        Arc::clone(&recorder),
                        Arc::clone(&outcomes),
                    ),
                )
                .expect("spawn same-row conflict history");
            let mut scheduler = runtime.scheduler.lock();
            scheduler.schedule(task, 0);
        }
    }

    let report = runtime.run_until_quiescent_with_report();
    assert!(
        report.lab_test_passed(),
        "LabRuntime production history run failed: {:?}",
        report.to_json()
    );

    let outcomes = lock_outcomes(&outcomes).clone();
    assert!(
        outcomes
            .iter()
            .all(|outcome| outcome.concurrent_mode_enabled),
        "all production connections must prove concurrent mode is enabled: {outcomes:?}"
    );
    match case {
        LabHistoryCase::DisjointWriters => {
            assert_eq!(
                outcomes.iter().filter(|outcome| outcome.committed).count(),
                2,
                "both disjoint writers must commit"
            );
        }
        LabHistoryCase::SameRowConflict => {
            assert!(
                outcomes
                    .iter()
                    .any(|outcome| outcome.transaction_id == "txn-loser"
                        && !outcome.committed
                        && outcome.transient_conflict),
                "same-row loser must be a transient FCW abort: {outcomes:?}"
            );
        }
    }

    let events = std::mem::take(&mut lock_recorder(&recorder).records);
    let final_state = match case {
        LabHistoryCase::DisjointWriters => verify_disjoint_final_state(&db_path),
        LabHistoryCase::SameRowConflict => verify_conflict_final_state(&db_path),
    };
    let history = build_history(
        case,
        &report,
        EventRecorder { records: events }.into_history_events(),
        final_state,
    );
    let report = check_history(&history).expect("serializability oracle should accept history");
    let history_json = history
        .to_json()
        .expect("history should encode as strict JSON");
    let report_json = report
        .to_json(&history)
        .expect("report should encode as strict JSON");
    LabHistoryArtifact {
        history,
        report,
        history_json,
        report_json,
    }
}

#[test]
fn production_lab_history_disjoint_writers_is_deterministic_and_replayable() {
    asupersync::test_utils::run_test(|| async {
        let first = run_lab_case(LabHistoryCase::DisjointWriters);
        let second = run_lab_case(LabHistoryCase::DisjointWriters);

        assert_eq!(first.history_json, second.history_json);
        assert_eq!(first.report_json, second.report_json);
        assert_eq!(
            first.history.schedule.schedule_sha256,
            second.history.schedule.schedule_sha256
        );
        assert_eq!(
            first.history.final_state_sha256,
            second.history.final_state_sha256
        );
        assert_eq!(
            first.history.execution_lane_evidence,
            second.history.execution_lane_evidence
        );
        assert!(first.history.schedule.deterministic_replay_claim());
        assert_eq!(first.report.verdict, OracleVerdict::Serializable);
        assert_eq!(
            first.history.final_state.get("writer_a/1"),
            Some(&HistoryValue::Integer(10))
        );
        assert_eq!(
            first.history.final_state.get("writer_b/1"),
            Some(&HistoryValue::Integer(20))
        );
    });
}

#[test]
fn production_lab_history_same_row_conflict_matches_fcw_abort() {
    asupersync::test_utils::run_test(|| async {
        let artifact = run_lab_case(LabHistoryCase::SameRowConflict);

        assert!(artifact.history.schedule.deterministic_replay_claim());
        assert_eq!(artifact.report.verdict, OracleVerdict::Serializable);
        assert_eq!(
            artifact.history.final_state.get("conflict_register/1"),
            Some(&HistoryValue::Integer(100))
        );
        assert!(
            artifact.history.events.iter().any(|event| matches!(
                event.operation,
                HistoryOperation::Conflict { ref reason }
                    if reason == "first_committer_wins_transient"
            )),
            "history must record the FCW conflict"
        );
    });
}

#[test]
fn deterministic_history_artifacts_fail_closed_for_corruption_and_observation_only() {
    asupersync::test_utils::run_test(|| async {
        let artifact = run_lab_case(LabHistoryCase::SameRowConflict);
        let midpoint = artifact.history_json.len() / 2;
        assert!(
            TransactionHistory::from_json_strict(&artifact.history_json[..midpoint]).is_err(),
            "truncated history artifacts must fail closed"
        );

        let mut smuggled = artifact.history.clone();
        smuggled.schedule.control = ScheduleControl::ObservationOnly;
        assert!(
            smuggled.to_json().is_err(),
            "observation-only histories cannot retain deterministic replay fields"
        );
    });
}

#[test]
fn cancellation_and_budget_exhaustion_are_explicit_history_outcomes() {
    let mut runtime = LabRuntime::new(
        LabConfig::new(CANCEL_BUDGET_SEED)
            .worker_count(1)
            .max_steps(1),
    );
    let root = runtime.state.create_root_region(Budget::INFINITE);
    let recorder = Arc::new(Mutex::new(EventRecorder::default()));
    let task_recorder = Arc::clone(&recorder);
    let (task, _) = runtime
        .state
        .create_task(root, Budget::INFINITE, async move {
            let native = asupersync::Cx::current().expect("LabRuntime task must install a Cx");
            native.cancel_with(asupersync::CancelKind::User, Some("bd-zu081-8-cancel"));
            let cx = FsqliteCx::new().with_trace_context(CANCEL_BUDGET_SEED, 1, 0);
            cx.set_native_cx(native);
            assert!(
                cx.checkpoint().is_err(),
                "FrankenSQLite Cx must observe LabRuntime cancellation"
            );
            lock_recorder(&task_recorder).record(
                "cancelled-connection",
                Some("txn-cancelled"),
                HistoryOperation::Begin {
                    mode: BeginMode::Concurrent,
                },
            );
            lock_recorder(&task_recorder).record(
                "cancelled-connection",
                Some("txn-cancelled"),
                HistoryOperation::Cancel {
                    reason: "lab-runtime-user-cancel".to_owned(),
                },
            );
            lock_recorder(&task_recorder).record(
                "budgeted-connection",
                Some("txn-timeout"),
                HistoryOperation::Begin {
                    mode: BeginMode::Concurrent,
                },
            );
            lock_recorder(&task_recorder).record(
                "budgeted-connection",
                Some("txn-timeout"),
                HistoryOperation::Timeout { budget_ms: 1 },
            );
        })
        .expect("spawn cancellation task");
    runtime.scheduler.lock().schedule(task, 0);
    let report = runtime.run_until_quiescent_with_report();
    assert!(
        report.steps_total <= 1,
        "max_steps(1) must surface the exact bounded schedule budget: {:?}",
        report.to_json()
    );

    let history = build_cancelled_history(
        &report,
        EventRecorder {
            records: std::mem::take(&mut lock_recorder(&recorder).records),
        }
        .into_history_events(),
    );
    let oracle = check_history(&history).expect("cancelled history must be explicit");
    assert_eq!(oracle.verdict, OracleVerdict::Serializable);
    assert_eq!(
        oracle.excluded_transactions.cancelled,
        vec!["txn-cancelled"]
    );
    assert_eq!(oracle.excluded_transactions.timed_out, vec!["txn-timeout"]);
}
