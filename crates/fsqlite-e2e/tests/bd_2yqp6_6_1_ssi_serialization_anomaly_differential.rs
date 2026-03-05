//! Deterministic SSI serialization anomaly differential suite.
//!
//! Bead: bd-2yqp6.6.1
//!
//! This suite executes deterministic two-transaction schedules against both
//! FrankenSQLite and C SQLite (rusqlite oracle) and validates:
//! - non-serializable schedules are not silently accepted,
//! - conflict/abort semantics are measured and surfaced,
//! - replay payloads carry seed/trace metadata and conform to schema.

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

use fsqlite_error::FrankenError;
use fsqlite_types::value::SqliteValue;
use rusqlite::ffi::ErrorCode;
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tempfile::tempdir;

const BEAD_ID: &str = "bd-2yqp6.6.1";
const LOG_STANDARD_REF: &str = "AGENTS.md#cross-cutting-quality-contract";
const REPLAY_COMMAND: &str = "cargo test -p fsqlite-e2e --test bd_2yqp6_6_1_ssi_serialization_anomaly_differential -- --nocapture --test-threads=1";
const DEFAULT_SEEDS: [u64; 2] = [0x2A51_6601_0000_0001, 0x2A51_6601_0000_0002];

#[derive(Debug, Clone, Copy)]
enum EngineKind {
    Fsqlite,
    Sqlite,
}

impl EngineKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Fsqlite => "fsqlite",
            Self::Sqlite => "sqlite3",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScenarioKind {
    WriteSkewGuard,
    DisjointWrites,
    HotRowConflict,
}

impl ScenarioKind {
    const fn scenario_id(self) -> &'static str {
        match self {
            Self::WriteSkewGuard => "SSI-WRITE-SKEW-GUARD",
            Self::DisjointWrites => "SSI-DISJOINT-WRITES",
            Self::HotRowConflict => "SSI-HOT-ROW-CONFLICT",
        }
    }

    const fn scenario_name(self) -> &'static str {
        match self {
            Self::WriteSkewGuard => "write_skew_guard",
            Self::DisjointWrites => "disjoint_writes",
            Self::HotRowConflict => "hot_row_conflict",
        }
    }

    fn tx_plan(self, tx_index: usize) -> TxPlan {
        match (self, tx_index) {
            (Self::WriteSkewGuard, 0) => TxPlan {
                read_key: 2,
                write_key: 1,
            },
            (Self::WriteSkewGuard, 1) => TxPlan {
                read_key: 1,
                write_key: 2,
            },
            (Self::DisjointWrites, 0) => TxPlan {
                read_key: 1,
                write_key: 1,
            },
            (Self::DisjointWrites, 1) => TxPlan {
                read_key: 2,
                write_key: 2,
            },
            (Self::HotRowConflict, 0) | (Self::HotRowConflict, 1) => TxPlan {
                read_key: 1,
                write_key: 1,
            },
            _ => panic!("invalid tx index: {tx_index}"),
        }
    }

    const fn initial_rows(self) -> [i64; 2] {
        match self {
            Self::WriteSkewGuard | Self::DisjointWrites => [1, 1],
            Self::HotRowConflict => [0, 0],
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct TxPlan {
    read_key: i64,
    write_key: i64,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum TxnOutcome {
    Committed,
    Busy,
    BusySnapshot,
    Locked,
    OtherError,
}

impl TxnOutcome {
    const fn is_abort_like(self) -> bool {
        !matches!(self, Self::Committed)
    }
}

#[derive(Debug, Clone, Serialize)]
struct TxnTrace {
    tx_index: usize,
    read_key: i64,
    write_key: i64,
    observed_value: Option<i64>,
    planned_write: bool,
    write_attempted: bool,
    start_order: u64,
    commit_order: Option<u64>,
    outcome: TxnOutcome,
    error_extended_code: Option<i32>,
    error_message: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct SchedulerPlan {
    seed: u64,
    commit_order: [usize; 2],
}

impl SchedulerPlan {
    fn from_seed(seed: u64) -> Self {
        if seed & 1 == 0 {
            Self {
                seed,
                commit_order: [0, 1],
            }
        } else {
            Self {
                seed,
                commit_order: [1, 0],
            }
        }
    }
}

#[derive(Debug, Clone)]
struct TxnRuntime {
    trace: TxnTrace,
    read_set: BTreeSet<i64>,
    write_set: BTreeSet<i64>,
    open: bool,
}

#[derive(Debug, Clone)]
struct EngineRun {
    engine: EngineKind,
    traces: [TxnTrace; 2],
    committed_for_graph: Vec<CommittedTxn>,
    final_rows: HashMap<i64, i64>,
    elapsed_ms: u64,
}

impl EngineRun {
    fn committed_count(&self) -> usize {
        self.traces
            .iter()
            .filter(|trace| trace.outcome == TxnOutcome::Committed)
            .count()
    }

    fn abort_count(&self) -> usize {
        self.traces
            .iter()
            .filter(|trace| trace.outcome.is_abort_like())
            .count()
    }

    fn first_failure_diagnostic(&self) -> Option<String> {
        self.traces
            .iter()
            .find_map(|trace| trace.error_message.clone())
    }
}

#[derive(Debug, Clone)]
struct CommittedTxn {
    start_order: u64,
    commit_order: u64,
    read_set: BTreeSet<i64>,
    write_set: BTreeSet<i64>,
}

#[test]
fn bead_metadata_constants_are_stable_for_replay() {
    assert_eq!(BEAD_ID, "bd-2yqp6.6.1");
    assert_eq!(LOG_STANDARD_REF, "AGENTS.md#cross-cutting-quality-contract");
    assert_eq!(
        REPLAY_COMMAND,
        "cargo test -p fsqlite-e2e --test bd_2yqp6_6_1_ssi_serialization_anomaly_differential -- --nocapture --test-threads=1"
    );
}

#[test]
fn serialization_checker_flags_rw_cycle_and_accepts_disjoint() {
    let cycle = vec![
        CommittedTxn {
            start_order: 1,
            commit_order: 10,
            read_set: BTreeSet::from([2]),
            write_set: BTreeSet::from([1]),
        },
        CommittedTxn {
            start_order: 2,
            commit_order: 11,
            read_set: BTreeSet::from([1]),
            write_set: BTreeSet::from([2]),
        },
    ];
    assert!(
        detect_cycle(&cycle).is_some(),
        "rw anti-dependency cycle should be detected"
    );

    let acyclic = vec![
        CommittedTxn {
            start_order: 1,
            commit_order: 2,
            read_set: BTreeSet::from([1]),
            write_set: BTreeSet::from([1]),
        },
        CommittedTxn {
            start_order: 3,
            commit_order: 4,
            read_set: BTreeSet::from([2]),
            write_set: BTreeSet::from([2]),
        },
    ];
    assert!(
        detect_cycle(&acyclic).is_none(),
        "disjoint writes should remain acyclic"
    );
}

#[test]
fn bd_2yqp6_6_1_ssi_serialization_anomaly_differential_matrix() {
    let scenarios = [
        ScenarioKind::WriteSkewGuard,
        ScenarioKind::DisjointWrites,
        ScenarioKind::HotRowConflict,
    ];

    let mut run_records = Vec::new();
    let mut first_failure: Option<String> = None;

    for scenario in scenarios {
        for seed in DEFAULT_SEEDS {
            let scheduler = SchedulerPlan::from_seed(seed);
            let run_id = format!("{BEAD_ID}-{}-{seed:016x}", scenario.scenario_name());
            let trace_id = format!("trace-{run_id}");

            let oracle = run_engine_scenario(EngineKind::Sqlite, scenario, &scheduler);
            let candidate = run_engine_scenario(EngineKind::Fsqlite, scenario, &scheduler);

            let oracle_cycle = detect_cycle(&oracle.committed_for_graph).is_some();
            let candidate_cycle = detect_cycle(&candidate.committed_for_graph).is_some();

            let mut checks = Vec::new();

            if oracle_cycle {
                checks.push("sqlite oracle produced serialization cycle".to_owned());
            }
            if candidate_cycle {
                checks.push("fsqlite accepted non-serializable committed schedule".to_owned());
            }

            if scenario == ScenarioKind::WriteSkewGuard {
                let final_sum = row_sum(&candidate.final_rows);
                if final_sum < 1 {
                    checks.push(format!(
                        "write-skew guard violated: final_sum={final_sum}, expected >= 1"
                    ));
                }
                if candidate.abort_count() == 0 {
                    checks.push(
                        "write-skew scenario did not surface abort/error semantics".to_owned(),
                    );
                }
            }

            if scenario == ScenarioKind::DisjointWrites && candidate.committed_count() == 0 {
                checks.push("disjoint scenario made no forward progress".to_owned());
            }

            if scenario == ScenarioKind::HotRowConflict && candidate.abort_count() == 0 {
                checks
                    .push("hot-row contention did not report conflict/abort semantics".to_owned());
            }

            let status = if checks.is_empty() { "pass" } else { "fail" };
            let first_failure_for_record = checks.first().cloned();
            if first_failure.is_none() {
                first_failure = first_failure_for_record.clone();
            }

            let record = json!({
                "bead_id": BEAD_ID,
                "trace_id": trace_id,
                "run_id": run_id,
                "scenario_id": scenario.scenario_id(),
                "seed": seed,
                "scheduler": {
                    "seed": scheduler.seed,
                    "commit_order": scheduler.commit_order,
                },
                "timing_ms": {
                    "sqlite3": oracle.elapsed_ms,
                    "fsqlite": candidate.elapsed_ms,
                },
                "outcome": status,
                "first_failure": first_failure_for_record,
                "checks": {
                    "non_serializable_schedule_rejected": !candidate_cycle,
                    "oracle_cycle_free": !oracle_cycle,
                    "abort_semantics_measured": candidate.abort_count() > 0 || scenario == ScenarioKind::DisjointWrites,
                    "disjoint_no_false_abort": scenario != ScenarioKind::DisjointWrites || candidate.committed_count() > 0,
                },
                "sqlite3": summarize_engine_run(&oracle),
                "fsqlite": summarize_engine_run(&candidate),
                "log_standard_ref": LOG_STANDARD_REF,
                "replay_command": REPLAY_COMMAND,
            });

            assert_outcome_schema_valid(&record);
            println!("SCENARIO_OUTCOME:{record}");
            run_records.push(record);
        }
    }

    maybe_write_artifact(&run_records);

    assert!(
        first_failure.is_none(),
        "{BEAD_ID} differential matrix failed: {}",
        first_failure.unwrap_or_else(|| "unknown failure".to_owned())
    );
}

fn run_engine_scenario(
    engine: EngineKind,
    scenario: ScenarioKind,
    scheduler: &SchedulerPlan,
) -> EngineRun {
    let tmp = tempdir().expect("tempdir");
    let db_path = tmp.path().join(format!(
        "{}-{}-{:016x}.db",
        engine.as_str(),
        scenario.scenario_name(),
        scheduler.seed
    ));

    match engine {
        EngineKind::Fsqlite => run_fsqlite(db_path.as_path(), scenario, scheduler),
        EngineKind::Sqlite => run_sqlite(db_path.as_path(), scenario, scheduler),
    }
}

fn run_fsqlite(db_path: &Path, scenario: ScenarioKind, scheduler: &SchedulerPlan) -> EngineRun {
    initialize_fsqlite_db(db_path, scenario);

    let start_time = Instant::now();
    let mut conn_a = open_fsqlite_worker(db_path);
    let mut conn_b = open_fsqlite_worker(db_path);

    let mut runtimes = [
        begin_and_prepare_fsqlite_txn(&mut conn_a, 0, scenario),
        begin_and_prepare_fsqlite_txn(&mut conn_b, 1, scenario),
    ];

    commit_in_order_fsqlite(
        &mut conn_a,
        &mut conn_b,
        &mut runtimes,
        scheduler.commit_order,
    );

    let committed_for_graph = committed_txns_from_runtime(&runtimes);
    let elapsed_ms = duration_to_ms(start_time.elapsed().as_millis());

    EngineRun {
        engine: EngineKind::Fsqlite,
        traces: [runtimes[0].trace.clone(), runtimes[1].trace.clone()],
        committed_for_graph,
        final_rows: read_rows_with_sqlite(db_path),
        elapsed_ms,
    }
}

fn run_sqlite(db_path: &Path, scenario: ScenarioKind, scheduler: &SchedulerPlan) -> EngineRun {
    initialize_sqlite_db(db_path, scenario);

    let start_time = Instant::now();
    let mut conn_a = open_sqlite_worker(db_path);
    let mut conn_b = open_sqlite_worker(db_path);

    let mut runtimes = [
        begin_and_prepare_sqlite_txn(&mut conn_a, 0, scenario),
        begin_and_prepare_sqlite_txn(&mut conn_b, 1, scenario),
    ];

    commit_in_order_sqlite(
        &mut conn_a,
        &mut conn_b,
        &mut runtimes,
        scheduler.commit_order,
    );

    let committed_for_graph = committed_txns_from_runtime(&runtimes);
    let elapsed_ms = duration_to_ms(start_time.elapsed().as_millis());

    EngineRun {
        engine: EngineKind::Sqlite,
        traces: [runtimes[0].trace.clone(), runtimes[1].trace.clone()],
        committed_for_graph,
        final_rows: read_rows_with_sqlite(db_path),
        elapsed_ms,
    }
}

fn initialize_fsqlite_db(path: &Path, scenario: ScenarioKind) {
    let path_str = path.to_str().expect("utf8 path");
    let conn = fsqlite::Connection::open(path_str).expect("open setup fsqlite connection");
    conn.execute("PRAGMA journal_mode=WAL;")
        .expect("set WAL mode");
    conn.execute("PRAGMA busy_timeout=0;")
        .expect("set busy timeout");
    conn.execute("PRAGMA fsqlite.concurrent_mode=ON;")
        .expect("enable concurrent mode");
    conn.execute("CREATE TABLE guard (id INTEGER PRIMARY KEY, v INTEGER NOT NULL);")
        .expect("create guard table");

    let [left, right] = scenario.initial_rows();
    conn.execute(&format!("INSERT INTO guard (id, v) VALUES (1, {left});"))
        .expect("insert row 1");
    conn.execute(&format!("INSERT INTO guard (id, v) VALUES (2, {right});"))
        .expect("insert row 2");
}

fn initialize_sqlite_db(path: &Path, scenario: ScenarioKind) {
    let conn = rusqlite::Connection::open(path).expect("open setup sqlite connection");
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=0;")
        .expect("configure sqlite setup connection");
    conn.execute_batch("CREATE TABLE guard (id INTEGER PRIMARY KEY, v INTEGER NOT NULL);")
        .expect("create guard table");

    let [left, right] = scenario.initial_rows();
    conn.execute(
        "INSERT INTO guard (id, v) VALUES (?1, ?2)",
        rusqlite::params![1_i64, left],
    )
    .expect("insert row 1");
    conn.execute(
        "INSERT INTO guard (id, v) VALUES (?1, ?2)",
        rusqlite::params![2_i64, right],
    )
    .expect("insert row 2");
}

fn open_fsqlite_worker(path: &Path) -> fsqlite::Connection {
    let path_str = path.to_str().expect("utf8 path");
    let conn = fsqlite::Connection::open(path_str).expect("open fsqlite worker");
    conn.execute("PRAGMA busy_timeout=0;")
        .expect("set fsqlite worker busy_timeout");
    conn.execute("PRAGMA fsqlite.concurrent_mode=ON;")
        .expect("enable fsqlite worker concurrent mode");
    conn
}

fn open_sqlite_worker(path: &Path) -> rusqlite::Connection {
    let conn = rusqlite::Connection::open(path).expect("open sqlite worker");
    conn.execute_batch("PRAGMA busy_timeout=0;")
        .expect("set sqlite worker busy timeout");
    conn
}

fn begin_and_prepare_fsqlite_txn(
    conn: &mut fsqlite::Connection,
    tx_index: usize,
    scenario: ScenarioKind,
) -> TxnRuntime {
    let plan = scenario.tx_plan(tx_index);
    let start_order = u64::try_from(tx_index + 1).expect("tx index fits in u64");

    let mut runtime = TxnRuntime {
        trace: TxnTrace {
            tx_index,
            read_key: plan.read_key,
            write_key: plan.write_key,
            observed_value: None,
            planned_write: false,
            write_attempted: false,
            start_order,
            commit_order: None,
            outcome: TxnOutcome::OtherError,
            error_extended_code: None,
            error_message: None,
        },
        read_set: BTreeSet::new(),
        write_set: BTreeSet::new(),
        open: false,
    };

    if let Err(err) = conn.execute("BEGIN CONCURRENT;") {
        apply_fsqlite_error(&mut runtime, err);
        return runtime;
    }

    runtime.open = true;
    if let Some(seq) = conn.current_concurrent_snapshot_seq() {
        runtime.trace.start_order = seq;
    }

    let read_sql = format!("SELECT v FROM guard WHERE id = {};", plan.read_key);
    let observed = match conn.query_row(read_sql.as_str()) {
        Ok(row) => match extract_int_fsqlite(&row, 0) {
            Ok(v) => v,
            Err(err) => {
                let _ = conn.execute("ROLLBACK;");
                runtime.open = false;
                apply_fsqlite_error(&mut runtime, err);
                return runtime;
            }
        },
        Err(err) => {
            let _ = conn.execute("ROLLBACK;");
            runtime.open = false;
            apply_fsqlite_error(&mut runtime, err);
            return runtime;
        }
    };
    runtime.trace.observed_value = Some(observed);
    runtime.read_set.insert(plan.read_key);

    let planned_write = should_write(scenario, observed);
    runtime.trace.planned_write = planned_write;

    if planned_write {
        runtime.trace.write_attempted = true;
        let write_sql = write_statement(scenario, plan.write_key);
        if let Err(err) = conn.execute(write_sql.as_str()) {
            let _ = conn.execute("ROLLBACK;");
            runtime.open = false;
            apply_fsqlite_error(&mut runtime, err);
            return runtime;
        }
        runtime.write_set.insert(plan.write_key);
    }

    runtime
}

fn begin_and_prepare_sqlite_txn(
    conn: &mut rusqlite::Connection,
    tx_index: usize,
    scenario: ScenarioKind,
) -> TxnRuntime {
    let plan = scenario.tx_plan(tx_index);
    let start_order = u64::try_from(tx_index + 1).expect("tx index fits in u64");

    let mut runtime = TxnRuntime {
        trace: TxnTrace {
            tx_index,
            read_key: plan.read_key,
            write_key: plan.write_key,
            observed_value: None,
            planned_write: false,
            write_attempted: false,
            start_order,
            commit_order: None,
            outcome: TxnOutcome::OtherError,
            error_extended_code: None,
            error_message: None,
        },
        read_set: BTreeSet::new(),
        write_set: BTreeSet::new(),
        open: false,
    };

    if let Err(err) = conn.execute_batch("BEGIN;") {
        apply_sqlite_error(&mut runtime, err);
        return runtime;
    }

    runtime.open = true;

    let read_sql = format!("SELECT v FROM guard WHERE id = {};", plan.read_key);
    let observed_result: rusqlite::Result<i64> =
        conn.query_row(read_sql.as_str(), [], |row| row.get::<_, i64>(0));
    let observed = match observed_result {
        Ok(v) => v,
        Err(err) => {
            let _ = conn.execute_batch("ROLLBACK;");
            runtime.open = false;
            apply_sqlite_error(&mut runtime, err);
            return runtime;
        }
    };

    runtime.trace.observed_value = Some(observed);
    runtime.read_set.insert(plan.read_key);

    let planned_write = should_write(scenario, observed);
    runtime.trace.planned_write = planned_write;

    if planned_write {
        runtime.trace.write_attempted = true;
        let write_sql = write_statement(scenario, plan.write_key);
        if let Err(err) = conn.execute_batch(write_sql.as_str()) {
            let _ = conn.execute_batch("ROLLBACK;");
            runtime.open = false;
            apply_sqlite_error(&mut runtime, err);
            return runtime;
        }
        runtime.write_set.insert(plan.write_key);
    }

    runtime
}

fn commit_in_order_fsqlite(
    conn_a: &mut fsqlite::Connection,
    conn_b: &mut fsqlite::Connection,
    runtimes: &mut [TxnRuntime; 2],
    commit_order: [usize; 2],
) {
    let mut commit_seq = 1_u64;
    for tx_index in commit_order {
        let runtime = &mut runtimes[tx_index];
        if !runtime.open {
            continue;
        }

        if tx_index == 0 {
            commit_single_fsqlite_txn(conn_a, runtime, commit_seq);
        } else {
            commit_single_fsqlite_txn(conn_b, runtime, commit_seq);
        }
        commit_seq = commit_seq.saturating_add(1);
    }
}

fn commit_in_order_sqlite(
    conn_a: &mut rusqlite::Connection,
    conn_b: &mut rusqlite::Connection,
    runtimes: &mut [TxnRuntime; 2],
    commit_order: [usize; 2],
) {
    let mut commit_seq = 1_u64;
    for tx_index in commit_order {
        let runtime = &mut runtimes[tx_index];
        if !runtime.open {
            continue;
        }

        if tx_index == 0 {
            commit_single_sqlite_txn(conn_a, runtime, commit_seq);
        } else {
            commit_single_sqlite_txn(conn_b, runtime, commit_seq);
        }
        commit_seq = commit_seq.saturating_add(1);
    }
}

fn commit_single_fsqlite_txn(
    conn: &mut fsqlite::Connection,
    runtime: &mut TxnRuntime,
    commit_seq: u64,
) {
    match conn.execute("COMMIT;") {
        Ok(_) => {
            runtime.trace.outcome = TxnOutcome::Committed;
            runtime.trace.commit_order = conn.last_local_commit_seq().or(Some(commit_seq));
            runtime.open = false;
        }
        Err(err) => {
            let _ = conn.execute("ROLLBACK;");
            runtime.open = false;
            apply_fsqlite_error(runtime, err);
        }
    }
}

fn commit_single_sqlite_txn(
    conn: &mut rusqlite::Connection,
    runtime: &mut TxnRuntime,
    commit_seq: u64,
) {
    match conn.execute_batch("COMMIT;") {
        Ok(_) => {
            runtime.trace.outcome = TxnOutcome::Committed;
            runtime.trace.commit_order = Some(commit_seq);
            runtime.open = false;
        }
        Err(err) => {
            let _ = conn.execute_batch("ROLLBACK;");
            runtime.open = false;
            apply_sqlite_error(runtime, err);
        }
    }
}

fn apply_fsqlite_error(runtime: &mut TxnRuntime, err: FrankenError) {
    let (outcome, extended_code) = classify_fsqlite_error(&err);
    runtime.trace.outcome = outcome;
    runtime.trace.error_extended_code = Some(extended_code);
    runtime.trace.error_message = Some(err.to_string());
}

fn apply_sqlite_error(runtime: &mut TxnRuntime, err: rusqlite::Error) {
    let (outcome, extended_code) = classify_sqlite_error(&err);
    runtime.trace.outcome = outcome;
    runtime.trace.error_extended_code = extended_code;
    runtime.trace.error_message = Some(err.to_string());
}

fn classify_fsqlite_error(err: &FrankenError) -> (TxnOutcome, i32) {
    let code = err.extended_error_code();
    let outcome = match err {
        FrankenError::BusySnapshot { .. } => TxnOutcome::BusySnapshot,
        FrankenError::Busy | FrankenError::BusyRecovery => TxnOutcome::Busy,
        FrankenError::DatabaseLocked { .. } | FrankenError::LockFailed { .. } => TxnOutcome::Locked,
        _ => TxnOutcome::OtherError,
    };
    (outcome, code)
}

fn classify_sqlite_error(err: &rusqlite::Error) -> (TxnOutcome, Option<i32>) {
    match err {
        rusqlite::Error::SqliteFailure(code, _) => {
            let outcome = match code.code {
                ErrorCode::DatabaseBusy => {
                    if code.extended_code == 517 {
                        TxnOutcome::BusySnapshot
                    } else {
                        TxnOutcome::Busy
                    }
                }
                ErrorCode::DatabaseLocked => TxnOutcome::Locked,
                _ => TxnOutcome::OtherError,
            };
            (outcome, Some(code.extended_code))
        }
        _ => (TxnOutcome::OtherError, None),
    }
}

fn extract_int_fsqlite(row: &fsqlite::Row, index: usize) -> Result<i64, FrankenError> {
    match row.get(index) {
        Some(SqliteValue::Integer(value)) => Ok(*value),
        Some(other) => Err(FrankenError::Internal(format!(
            "expected integer column at index {index}, got {other:?}"
        ))),
        None => Err(FrankenError::Internal(format!(
            "missing column at index {index}"
        ))),
    }
}

fn should_write(scenario: ScenarioKind, observed: i64) -> bool {
    match scenario {
        ScenarioKind::WriteSkewGuard => observed > 0,
        ScenarioKind::DisjointWrites | ScenarioKind::HotRowConflict => true,
    }
}

fn write_statement(scenario: ScenarioKind, key: i64) -> String {
    match scenario {
        ScenarioKind::WriteSkewGuard => format!("UPDATE guard SET v = 0 WHERE id = {key};"),
        ScenarioKind::DisjointWrites => format!("UPDATE guard SET v = 2 WHERE id = {key};"),
        ScenarioKind::HotRowConflict => format!("UPDATE guard SET v = v + 1 WHERE id = {key};"),
    }
}

fn committed_txns_from_runtime(runtimes: &[TxnRuntime; 2]) -> Vec<CommittedTxn> {
    runtimes
        .iter()
        .filter_map(|runtime| {
            (runtime.trace.outcome == TxnOutcome::Committed).then_some(CommittedTxn {
                start_order: runtime.trace.start_order,
                commit_order: runtime.trace.commit_order.unwrap_or_default(),
                read_set: runtime.read_set.clone(),
                write_set: runtime.write_set.clone(),
            })
        })
        .collect()
}

fn read_rows_with_sqlite(db_path: &Path) -> HashMap<i64, i64> {
    let conn = rusqlite::Connection::open(db_path).expect("open verifier sqlite connection");
    let mut stmt = conn
        .prepare("SELECT id, v FROM guard ORDER BY id")
        .expect("prepare verifier query");
    let rows = stmt
        .query_map([], |row| {
            let id: i64 = row.get(0)?;
            let value: i64 = row.get(1)?;
            Ok((id, value))
        })
        .expect("execute verifier query");

    let mut map = HashMap::new();
    for row in rows {
        let (id, value) = row.expect("decode verifier row");
        map.insert(id, value);
    }
    map
}

fn row_sum(rows: &HashMap<i64, i64>) -> i64 {
    rows.values().copied().sum()
}

fn summarize_engine_run(run: &EngineRun) -> Value {
    json!({
        "engine": run.engine.as_str(),
        "committed": run.committed_count(),
        "aborted": run.abort_count(),
        "final_rows": {
            "1": run.final_rows.get(&1).copied(),
            "2": run.final_rows.get(&2).copied(),
        },
        "transactions": run.traces,
        "first_failure": run.first_failure_diagnostic(),
    })
}

fn outcome_schema() -> Value {
    json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "required": [
            "bead_id",
            "trace_id",
            "run_id",
            "scenario_id",
            "seed",
            "scheduler",
            "timing_ms",
            "outcome",
            "checks",
            "sqlite3",
            "fsqlite",
            "log_standard_ref",
            "replay_command"
        ],
        "properties": {
            "bead_id": { "type": "string" },
            "trace_id": { "type": "string" },
            "run_id": { "type": "string" },
            "scenario_id": { "type": "string" },
            "seed": { "type": "integer", "minimum": 1 },
            "scheduler": {
                "type": "object",
                "required": ["seed", "commit_order"],
                "properties": {
                    "seed": { "type": "integer", "minimum": 1 },
                    "commit_order": {
                        "type": "array",
                        "items": { "type": "integer" },
                        "minItems": 2,
                        "maxItems": 2
                    }
                }
            },
            "timing_ms": {
                "type": "object",
                "required": ["sqlite3", "fsqlite"],
                "properties": {
                    "sqlite3": { "type": "integer", "minimum": 0 },
                    "fsqlite": { "type": "integer", "minimum": 0 }
                }
            },
            "outcome": { "type": "string", "enum": ["pass", "fail"] },
            "first_failure": { "type": ["string", "null"] },
            "checks": {
                "type": "object",
                "required": [
                    "non_serializable_schedule_rejected",
                    "oracle_cycle_free",
                    "abort_semantics_measured",
                    "disjoint_no_false_abort"
                ],
                "properties": {
                    "non_serializable_schedule_rejected": { "type": "boolean" },
                    "oracle_cycle_free": { "type": "boolean" },
                    "abort_semantics_measured": { "type": "boolean" },
                    "disjoint_no_false_abort": { "type": "boolean" }
                }
            },
            "sqlite3": { "type": "object" },
            "fsqlite": { "type": "object" },
            "log_standard_ref": { "type": "string" },
            "replay_command": { "type": "string" }
        }
    })
}

fn assert_outcome_schema_valid(record: &Value) {
    let schema = outcome_schema();
    let validator = jsonschema::options()
        .with_draft(jsonschema::Draft::Draft202012)
        .build(&schema)
        .expect("build scenario outcome schema validator");

    let errors: Vec<String> = validator
        .iter_errors(record)
        .map(|err| err.to_string())
        .collect();
    assert!(
        errors.is_empty(),
        "scenario outcome failed schema validation:\n- {}",
        errors.join("\n- ")
    );
}

fn maybe_write_artifact(run_records: &[Value]) {
    let Ok(path) = env::var("FSQLITE_SSI_ANOMALY_ARTIFACT") else {
        return;
    };

    let artifact_path = PathBuf::from(path);
    if let Some(parent) = artifact_path.parent() {
        fs::create_dir_all(parent).expect("create artifact parent directory");
    }

    let overall_status = if run_records.iter().all(|record| {
        record
            .get("outcome")
            .and_then(Value::as_str)
            .is_some_and(|status| status == "pass")
    }) {
        "pass"
    } else {
        "fail"
    };

    let artifact = json!({
        "bead_id": BEAD_ID,
        "overall_status": overall_status,
        "run_count": run_records.len(),
        "log_standard_ref": LOG_STANDARD_REF,
        "replay_command": REPLAY_COMMAND,
        "runs": run_records,
    });

    let payload = serde_json::to_vec_pretty(&artifact).expect("serialize artifact payload");
    let digest = Sha256::digest(&payload);
    let hash = format!("{digest:x}");

    fs::write(&artifact_path, payload).expect("write differential artifact");

    eprintln!(
        "DEBUG bead_id={BEAD_ID} artifact_path={} sha256={} replay_command={REPLAY_COMMAND}",
        artifact_path.display(),
        hash
    );
}

fn duration_to_ms(ms: u128) -> u64 {
    u64::try_from(ms).unwrap_or(u64::MAX)
}

fn detect_cycle(txns: &[CommittedTxn]) -> Option<Vec<usize>> {
    let node_count = txns.len();
    if node_count <= 1 {
        return None;
    }

    let mut edges: Vec<BTreeSet<usize>> = vec![BTreeSet::new(); node_count];
    let mut indegree = vec![0_usize; node_count];

    for left_idx in 0..node_count {
        for right_idx in (left_idx + 1)..node_count {
            let left = &txns[left_idx];
            let right = &txns[right_idx];

            let mut add_edge = |from: usize, to: usize| {
                if edges[from].insert(to) {
                    indegree[to] += 1;
                }
            };

            if intersects(&left.write_set, &right.write_set) {
                if left.commit_order <= right.commit_order {
                    add_edge(left_idx, right_idx);
                } else {
                    add_edge(right_idx, left_idx);
                }
            }

            if intersects(&left.write_set, &right.read_set) {
                orient_read_write_conflict(left, right, left_idx, right_idx, &mut add_edge);
            }
            if intersects(&right.write_set, &left.read_set) {
                orient_read_write_conflict(right, left, right_idx, left_idx, &mut add_edge);
            }
        }
    }

    let mut queue = VecDeque::new();
    for (idx, degree) in indegree.iter().enumerate() {
        if *degree == 0 {
            queue.push_back(idx);
        }
    }

    let mut visited = 0_usize;
    while let Some(node) = queue.pop_front() {
        visited += 1;
        for &next in &edges[node] {
            indegree[next] = indegree[next].saturating_sub(1);
            if indegree[next] == 0 {
                queue.push_back(next);
            }
        }
    }

    if visited == node_count {
        return None;
    }

    let mut state = vec![0_u8; node_count];
    let mut stack = Vec::new();
    for node in 0..node_count {
        if indegree[node] == 0 || state[node] != 0 {
            continue;
        }
        if let Some(cycle) = dfs_cycle(node, &edges, &indegree, &mut state, &mut stack) {
            return Some(cycle);
        }
    }

    Some(Vec::new())
}

fn dfs_cycle(
    node: usize,
    edges: &[BTreeSet<usize>],
    indegree: &[usize],
    state: &mut [u8],
    stack: &mut Vec<usize>,
) -> Option<Vec<usize>> {
    state[node] = 1;
    stack.push(node);
    for &next in &edges[node] {
        if indegree[next] == 0 {
            continue;
        }
        if state[next] == 0 {
            if let Some(cycle) = dfs_cycle(next, edges, indegree, state, stack) {
                return Some(cycle);
            }
        } else if state[next] == 1 {
            let start = stack
                .iter()
                .position(|&value| value == next)
                .expect("cycle node must be present in DFS stack");
            let mut cycle = stack[start..].to_vec();
            cycle.push(next);
            return Some(cycle);
        }
    }
    stack.pop();
    state[node] = 2;
    None
}

fn orient_read_write_conflict(
    writer: &CommittedTxn,
    reader: &CommittedTxn,
    writer_idx: usize,
    reader_idx: usize,
    add_edge: &mut impl FnMut(usize, usize),
) {
    if writer.commit_order <= reader.start_order {
        add_edge(writer_idx, reader_idx);
    } else if reader.commit_order <= writer.start_order {
        add_edge(reader_idx, writer_idx);
    } else {
        add_edge(reader_idx, writer_idx);
    }
}

fn intersects(left: &BTreeSet<i64>, right: &BTreeSet<i64>) -> bool {
    left.iter().any(|item| right.contains(item))
}
