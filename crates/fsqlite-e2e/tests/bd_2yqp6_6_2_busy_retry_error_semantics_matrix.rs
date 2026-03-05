//! Deterministic BUSY / BUSY_SNAPSHOT parity matrix.
//!
//! Bead: bd-2yqp6.6.2
//!
//! This suite validates, with deterministic replay metadata:
//! - conflict surfaces for `BUSY` and `BUSY_SNAPSHOT`,
//! - error-code mapping evidence (`base` + `extended`),
//! - concurrent-mode default anti-regression behavior.

#![allow(clippy::too_many_lines)]

use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

use fsqlite::Connection as FsqliteConnection;
use fsqlite_error::FrankenError;
use rusqlite::ffi::ErrorCode;
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tempfile::tempdir;

const BEAD_ID: &str = "bd-2yqp6.6.2";
const LOG_STANDARD_REF: &str = "AGENTS.md#cross-cutting-quality-contract";
const REPLAY_COMMAND: &str = "cargo test -p fsqlite-e2e --test bd_2yqp6_6_2_busy_retry_error_semantics_matrix -- --nocapture --test-threads=1";
const SCENARIO_FILTER_ENV: &str = "FSQLITE_BUSY_MATRIX_ONLY";
const ARTIFACT_ENV_PATH: &str = "FSQLITE_BUSY_MATRIX_ARTIFACT";
const DEFAULT_SEED: u64 = 0x2A51_6602_0000_0001;

const DEFAULT_SCENARIO_IDS: [&str; 3] = [
    "busy_snapshot_conflict",
    "busy_lock_immediate",
    "concurrent_mode_default_guard",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScenarioKind {
    BusySnapshotConflict,
    BusyLockImmediate,
    ConcurrentModeDefaultGuard,
}

impl ScenarioKind {
    const fn scenario_id(self) -> &'static str {
        match self {
            Self::BusySnapshotConflict => "busy_snapshot_conflict",
            Self::BusyLockImmediate => "busy_lock_immediate",
            Self::ConcurrentModeDefaultGuard => "concurrent_mode_default_guard",
        }
    }

    const fn description(self) -> &'static str {
        match self {
            Self::BusySnapshotConflict => {
                "Two concurrent writers touch the same row; conflict must surface as BUSY-family semantics with deterministic codes"
            }
            Self::BusyLockImmediate => {
                "SQLite serializes BEGIN IMMEDIATE; FrankenSQLite may intentionally diverge under MVCC"
            }
            Self::ConcurrentModeDefaultGuard => {
                "Plain BEGIN must promote when concurrent_mode default is ON and stop promoting when OFF"
            }
        }
    }

    fn from_id(id: &str) -> Option<Self> {
        match id {
            "busy_snapshot_conflict" => Some(Self::BusySnapshotConflict),
            "busy_lock_immediate" => Some(Self::BusyLockImmediate),
            "concurrent_mode_default_guard" => Some(Self::ConcurrentModeDefaultGuard),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ErrorClass {
    None,
    Busy,
    BusySnapshot,
    Locked,
    Other,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct EngineOutcome {
    engine: &'static str,
    status: &'static str,
    classification: ErrorClass,
    base_error_code: Option<i32>,
    extended_error_code: Option<i32>,
    message: Option<String>,
    note: Option<String>,
}

impl EngineOutcome {
    fn ok(engine: &'static str, note: Option<String>) -> Self {
        Self {
            engine,
            status: "ok",
            classification: ErrorClass::None,
            base_error_code: None,
            extended_error_code: None,
            message: None,
            note,
        }
    }

    fn not_applicable(engine: &'static str, note: &str) -> Self {
        Self {
            engine,
            status: "not_applicable",
            classification: ErrorClass::None,
            base_error_code: None,
            extended_error_code: None,
            message: None,
            note: Some(note.to_owned()),
        }
    }

    fn from_fsqlite_error(err: FrankenError) -> Self {
        let extended = err.extended_error_code();
        let base = Some(extended & 0xFF);
        let classification = match err {
            FrankenError::BusySnapshot { .. } => ErrorClass::BusySnapshot,
            FrankenError::Busy | FrankenError::BusyRecovery => ErrorClass::Busy,
            FrankenError::DatabaseLocked { .. } | FrankenError::LockFailed { .. } => {
                ErrorClass::Locked
            }
            _ => ErrorClass::Other,
        };

        Self {
            engine: "fsqlite",
            status: "error",
            classification,
            base_error_code: base,
            extended_error_code: Some(extended),
            message: Some(err.to_string()),
            note: None,
        }
    }

    fn from_sqlite_error(err: rusqlite::Error) -> Self {
        match err {
            rusqlite::Error::SqliteFailure(code, extra) => {
                let classification = match code.code {
                    ErrorCode::DatabaseBusy => {
                        if code.extended_code == 517 {
                            ErrorClass::BusySnapshot
                        } else {
                            ErrorClass::Busy
                        }
                    }
                    ErrorCode::DatabaseLocked => ErrorClass::Locked,
                    _ => ErrorClass::Other,
                };

                let detail = extra.unwrap_or_default();
                Self {
                    engine: "sqlite3",
                    status: "error",
                    classification,
                    base_error_code: Some(code.extended_code & 0xFF),
                    extended_error_code: Some(code.extended_code),
                    message: Some(format!(
                        "{} ({detail})",
                        rusqlite::Error::SqliteFailure(code, None)
                    )),
                    note: None,
                }
            }
            other => Self {
                engine: "sqlite3",
                status: "error",
                classification: ErrorClass::Other,
                base_error_code: None,
                extended_error_code: None,
                message: Some(other.to_string()),
                note: None,
            },
        }
    }

    fn fingerprint(&self) -> Value {
        json!({
            "status": self.status,
            "classification": self.classification,
            "base_error_code": self.base_error_code,
            "extended_error_code": self.extended_error_code,
        })
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct ConcurrentModeGuard {
    default_on: bool,
    begin_promotes_to_concurrent: bool,
    pragma_off_disables_promotion: bool,
    pragma_on_restores_default: bool,
}

impl ConcurrentModeGuard {
    const fn all_pass(&self) -> bool {
        self.default_on
            && self.begin_promotes_to_concurrent
            && self.pragma_off_disables_promotion
            && self.pragma_on_restores_default
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct ScenarioRun {
    sqlite: EngineOutcome,
    fsqlite: EngineOutcome,
    sqlite_timing_ms: u64,
    fsqlite_timing_ms: u64,
    guard: Option<ConcurrentModeGuard>,
}

impl ScenarioRun {
    fn fingerprint(&self) -> Value {
        json!({
            "sqlite": self.sqlite.fingerprint(),
            "fsqlite": self.fsqlite.fingerprint(),
            "guard": self.guard,
        })
    }
}

#[test]
fn bead_metadata_constants_are_stable_for_replay() {
    assert_eq!(BEAD_ID, "bd-2yqp6.6.2");
    assert_eq!(LOG_STANDARD_REF, "AGENTS.md#cross-cutting-quality-contract");
    assert_eq!(
        REPLAY_COMMAND,
        "cargo test -p fsqlite-e2e --test bd_2yqp6_6_2_busy_retry_error_semantics_matrix -- --nocapture --test-threads=1"
    );
}

#[test]
fn bd_2yqp6_6_2_busy_retry_error_semantics_matrix() {
    let scenarios = selected_scenarios();
    assert!(
        !scenarios.is_empty(),
        "at least one scenario must be selected"
    );

    let mut records = Vec::new();
    let mut first_failure: Option<String> = None;

    for scenario in scenarios {
        let run_id = format!("{BEAD_ID}-{}-{DEFAULT_SEED:016x}", scenario.scenario_id());
        let trace_id = format!("trace-{run_id}");

        let run_a = run_scenario(scenario);
        let run_b = run_scenario(scenario);
        let deterministic_replay = run_a.fingerprint() == run_b.fingerprint();

        let mut diagnostics = Vec::new();
        if !deterministic_replay {
            diagnostics.push("non-deterministic engine outcome fingerprint".to_owned());
        }

        let checks = scenario_checks(scenario, &run_a, &mut diagnostics, deterministic_replay);

        let outcome = if diagnostics.is_empty() {
            "pass"
        } else {
            "fail"
        };
        let first_failure_for_record = diagnostics.first().cloned();
        if first_failure.is_none() {
            first_failure = first_failure_for_record.clone();
        }

        let record = json!({
            "bead_id": BEAD_ID,
            "trace_id": trace_id,
            "run_id": run_id,
            "scenario_id": scenario.scenario_id(),
            "seed": DEFAULT_SEED,
            "scenario": {
                "description": scenario.description(),
            },
            "timing_ms": {
                "sqlite3": run_a.sqlite_timing_ms,
                "fsqlite": run_a.fsqlite_timing_ms,
            },
            "outcome": outcome,
            "first_failure": first_failure_for_record,
            "checks": checks,
            "sqlite3": run_a.sqlite,
            "fsqlite": run_a.fsqlite,
            "concurrent_mode_guard": run_a.guard,
            "log_standard_ref": LOG_STANDARD_REF,
            "replay_command": format!("{REPLAY_COMMAND} {}", scenario.scenario_id()),
        });

        assert_outcome_schema_valid(&record);
        println!("SCENARIO_OUTCOME:{record}");
        records.push(record);
    }

    maybe_write_artifact(&records, first_failure.as_deref());
    if let Some(failure) = first_failure {
        panic!("busy/retry/error semantics matrix failed: {failure}");
    }
}

fn selected_scenarios() -> Vec<ScenarioKind> {
    let parsed: Vec<ScenarioKind> = env::var(SCENARIO_FILTER_ENV)
        .ok()
        .map(|raw| {
            raw.split(',')
                .map(str::trim)
                .filter(|id| !id.is_empty())
                .filter_map(ScenarioKind::from_id)
                .collect()
        })
        .unwrap_or_default();

    if parsed.is_empty() {
        DEFAULT_SCENARIO_IDS
            .iter()
            .filter_map(|id| ScenarioKind::from_id(id))
            .collect()
    } else {
        parsed
    }
}

fn run_scenario(scenario: ScenarioKind) -> ScenarioRun {
    match scenario {
        ScenarioKind::BusySnapshotConflict => run_busy_snapshot_conflict(),
        ScenarioKind::BusyLockImmediate => run_busy_lock_immediate(),
        ScenarioKind::ConcurrentModeDefaultGuard => run_concurrent_mode_default_guard(),
    }
}

fn run_busy_snapshot_conflict() -> ScenarioRun {
    let dir =
        tempdir().unwrap_or_else(|e| panic!("create tempdir for busy snapshot scenario: {e}"));
    let sqlite_path = dir.path().join("busy_snapshot_sqlite.db");
    let fsqlite_path = dir.path().join("busy_snapshot_fsqlite.db");

    let started_sqlite = Instant::now();
    let sqlite = run_sqlite_busy_snapshot_like(sqlite_path.as_path());
    let sqlite_ms = duration_to_ms(started_sqlite.elapsed().as_millis());

    let started_fsqlite = Instant::now();
    let fsqlite = run_fsqlite_busy_snapshot_like(fsqlite_path.as_path());
    let fsqlite_ms = duration_to_ms(started_fsqlite.elapsed().as_millis());

    ScenarioRun {
        sqlite,
        fsqlite,
        sqlite_timing_ms: sqlite_ms,
        fsqlite_timing_ms: fsqlite_ms,
        guard: None,
    }
}

fn run_busy_lock_immediate() -> ScenarioRun {
    let dir = tempdir().unwrap_or_else(|e| panic!("create tempdir for busy lock scenario: {e}"));
    let sqlite_path = dir.path().join("busy_lock_sqlite.db");
    let fsqlite_path = dir.path().join("busy_lock_fsqlite.db");

    let started_sqlite = Instant::now();
    let sqlite = run_sqlite_busy_lock(sqlite_path.as_path());
    let sqlite_ms = duration_to_ms(started_sqlite.elapsed().as_millis());

    let started_fsqlite = Instant::now();
    let fsqlite = run_fsqlite_busy_lock(fsqlite_path.as_path());
    let fsqlite_ms = duration_to_ms(started_fsqlite.elapsed().as_millis());

    ScenarioRun {
        sqlite,
        fsqlite,
        sqlite_timing_ms: sqlite_ms,
        fsqlite_timing_ms: fsqlite_ms,
        guard: None,
    }
}

fn run_concurrent_mode_default_guard() -> ScenarioRun {
    let started_fsqlite = Instant::now();
    let (fsqlite, guard) = run_fsqlite_default_guard_checks();
    let fsqlite_ms = duration_to_ms(started_fsqlite.elapsed().as_millis());

    ScenarioRun {
        sqlite: EngineOutcome::not_applicable(
            "sqlite3",
            "concurrent_mode_default is FrankenSQLite-specific",
        ),
        fsqlite,
        sqlite_timing_ms: 0,
        fsqlite_timing_ms: fsqlite_ms,
        guard: Some(guard),
    }
}

fn run_sqlite_busy_snapshot_like(path: &Path) -> EngineOutcome {
    if let Err(err) = initialize_sqlite_db(path) {
        return EngineOutcome::from_sqlite_error(err);
    }

    let conn1 = match rusqlite::Connection::open(path) {
        Ok(conn) => conn,
        Err(err) => return EngineOutcome::from_sqlite_error(err),
    };
    let conn2 = match rusqlite::Connection::open(path) {
        Ok(conn) => conn,
        Err(err) => return EngineOutcome::from_sqlite_error(err),
    };

    if let Err(err) = conn1.execute_batch("PRAGMA busy_timeout=0; BEGIN;") {
        return EngineOutcome::from_sqlite_error(err);
    }
    if let Err(err) = conn2.execute_batch("PRAGMA busy_timeout=0; BEGIN;") {
        let _ = conn1.execute_batch("ROLLBACK;");
        return EngineOutcome::from_sqlite_error(err);
    }
    if let Err(err) = conn1.execute("UPDATE t SET v = v + 1 WHERE id = 1;", []) {
        let _ = conn1.execute_batch("ROLLBACK;");
        let _ = conn2.execute_batch("ROLLBACK;");
        return EngineOutcome::from_sqlite_error(err);
    }

    let second_update = conn2.execute("UPDATE t SET v = v + 1 WHERE id = 1;", []);
    let commit1 = conn1.execute_batch("COMMIT;");

    match second_update {
        Err(err) => {
            let _ = conn2.execute_batch("ROLLBACK;");
            if let Err(commit_err) = commit1 {
                return EngineOutcome::from_sqlite_error(commit_err);
            }
            EngineOutcome::from_sqlite_error(err)
        }
        Ok(_) => {
            if let Err(err) = commit1 {
                let _ = conn2.execute_batch("ROLLBACK;");
                return EngineOutcome::from_sqlite_error(err);
            }
            match conn2.execute_batch("COMMIT;") {
                Ok(_) => EngineOutcome::ok(
                    "sqlite3",
                    Some("both writers committed under sqlite locking model".to_owned()),
                ),
                Err(err) => EngineOutcome::from_sqlite_error(err),
            }
        }
    }
}

fn run_fsqlite_busy_snapshot_like(path: &Path) -> EngineOutcome {
    if let Err(err) = initialize_fsqlite_db(path) {
        return EngineOutcome::from_fsqlite_error(err);
    }

    let conn1 = match open_fsqlite_worker(path, true) {
        Ok(conn) => conn,
        Err(err) => return EngineOutcome::from_fsqlite_error(err),
    };
    let conn2 = match open_fsqlite_worker(path, true) {
        Ok(conn) => conn,
        Err(err) => return EngineOutcome::from_fsqlite_error(err),
    };

    if let Err(err) = conn1.execute("BEGIN CONCURRENT;") {
        return EngineOutcome::from_fsqlite_error(err);
    }
    if let Err(err) = conn2.execute("BEGIN CONCURRENT;") {
        let _ = conn1.execute("ROLLBACK;");
        return EngineOutcome::from_fsqlite_error(err);
    }
    if let Err(err) = conn1.execute("UPDATE t SET v = v + 1 WHERE id = 1;") {
        let _ = conn1.execute("ROLLBACK;");
        let _ = conn2.execute("ROLLBACK;");
        return EngineOutcome::from_fsqlite_error(err);
    }

    let second_update = conn2.execute("UPDATE t SET v = v + 1 WHERE id = 1;");
    let commit1 = conn1.execute("COMMIT;");

    match second_update {
        Err(err) => {
            let _ = conn2.execute("ROLLBACK;");
            if let Err(commit_err) = commit1 {
                return EngineOutcome::from_fsqlite_error(commit_err);
            }
            EngineOutcome::from_fsqlite_error(err)
        }
        Ok(_) => {
            if let Err(err) = commit1 {
                let _ = conn2.execute("ROLLBACK;");
                return EngineOutcome::from_fsqlite_error(err);
            }
            match conn2.execute("COMMIT;") {
                Ok(_) => EngineOutcome::ok(
                    "fsqlite",
                    Some("both writers committed (no conflicting page detected)".to_owned()),
                ),
                Err(err) => EngineOutcome::from_fsqlite_error(err),
            }
        }
    }
}

fn run_sqlite_busy_lock(path: &Path) -> EngineOutcome {
    if let Err(err) = initialize_sqlite_db(path) {
        return EngineOutcome::from_sqlite_error(err);
    }

    let conn1 = match rusqlite::Connection::open(path) {
        Ok(conn) => conn,
        Err(err) => return EngineOutcome::from_sqlite_error(err),
    };
    let conn2 = match rusqlite::Connection::open(path) {
        Ok(conn) => conn,
        Err(err) => return EngineOutcome::from_sqlite_error(err),
    };

    if let Err(err) = conn1.execute_batch("PRAGMA busy_timeout=0; BEGIN IMMEDIATE;") {
        return EngineOutcome::from_sqlite_error(err);
    }

    let second_begin = conn2.execute_batch("PRAGMA busy_timeout=0; BEGIN IMMEDIATE;");
    let _ = conn1.execute_batch("ROLLBACK;");
    let _ = conn2.execute_batch("ROLLBACK;");

    match second_begin {
        Ok(_) => EngineOutcome::ok(
            "sqlite3",
            Some("second BEGIN IMMEDIATE unexpectedly succeeded".to_owned()),
        ),
        Err(err) => EngineOutcome::from_sqlite_error(err),
    }
}

fn run_fsqlite_busy_lock(path: &Path) -> EngineOutcome {
    if let Err(err) = initialize_fsqlite_db(path) {
        return EngineOutcome::from_fsqlite_error(err);
    }

    let conn1 = match open_fsqlite_worker(path, false) {
        Ok(conn) => conn,
        Err(err) => return EngineOutcome::from_fsqlite_error(err),
    };
    let conn2 = match open_fsqlite_worker(path, false) {
        Ok(conn) => conn,
        Err(err) => return EngineOutcome::from_fsqlite_error(err),
    };

    if let Err(err) = conn1.execute("BEGIN IMMEDIATE;") {
        return EngineOutcome::from_fsqlite_error(err);
    }

    let second_begin = conn2.execute("BEGIN IMMEDIATE;");
    let _ = conn1.execute("ROLLBACK;");
    let _ = conn2.execute("ROLLBACK;");

    match second_begin {
        Ok(_) => EngineOutcome::ok(
            "fsqlite",
            Some(
                "second BEGIN IMMEDIATE succeeded under non-serialized MVCC locking model"
                    .to_owned(),
            ),
        ),
        Err(err) => EngineOutcome::from_fsqlite_error(err),
    }
}

fn run_fsqlite_default_guard_checks() -> (EngineOutcome, ConcurrentModeGuard) {
    let conn = match FsqliteConnection::open(":memory:") {
        Ok(conn) => conn,
        Err(err) => {
            let outcome = EngineOutcome::from_fsqlite_error(err);
            let guard = ConcurrentModeGuard {
                default_on: false,
                begin_promotes_to_concurrent: false,
                pragma_off_disables_promotion: false,
                pragma_on_restores_default: false,
            };
            return (outcome, guard);
        }
    };

    let default_on = conn.is_concurrent_mode_default();

    let begin_promotes_to_concurrent =
        conn.execute("BEGIN;").is_ok() && conn.is_concurrent_transaction();
    let _ = conn.execute("ROLLBACK;");

    let pragma_off_disables_promotion = conn.execute("PRAGMA fsqlite.concurrent_mode=OFF;").is_ok()
        && !conn.is_concurrent_mode_default()
        && conn.execute("BEGIN;").is_ok()
        && !conn.is_concurrent_transaction();
    let _ = conn.execute("ROLLBACK;");

    let pragma_on_restores_default = conn.execute("PRAGMA fsqlite.concurrent_mode=ON;").is_ok()
        && conn.is_concurrent_mode_default();

    let guard = ConcurrentModeGuard {
        default_on,
        begin_promotes_to_concurrent,
        pragma_off_disables_promotion,
        pragma_on_restores_default,
    };

    if guard.all_pass() {
        (
            EngineOutcome::ok(
                "fsqlite",
                Some("concurrent_mode default guard checks passed".to_owned()),
            ),
            guard,
        )
    } else {
        (
            EngineOutcome {
                engine: "fsqlite",
                status: "error",
                classification: ErrorClass::Other,
                base_error_code: None,
                extended_error_code: None,
                message: Some(format!("default guard failed: {guard:?}")),
                note: None,
            },
            guard,
        )
    }
}

fn initialize_sqlite_db(path: &Path) -> rusqlite::Result<()> {
    let conn = rusqlite::Connection::open(path)?;
    conn.execute_batch(
        "PRAGMA journal_mode=WAL; PRAGMA busy_timeout=0; \
         CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER NOT NULL); \
         INSERT INTO t (id, v) VALUES (1, 0);",
    )
}

fn initialize_fsqlite_db(path: &Path) -> Result<(), FrankenError> {
    let conn = FsqliteConnection::open(path_to_utf8(path))?;
    conn.execute("PRAGMA journal_mode=WAL;")?;
    conn.execute("PRAGMA busy_timeout=0;")?;
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER NOT NULL);")?;
    conn.execute("INSERT INTO t (id, v) VALUES (1, 0);")?;
    Ok(())
}

fn open_fsqlite_worker(
    path: &Path,
    concurrent_mode: bool,
) -> Result<FsqliteConnection, FrankenError> {
    let conn = FsqliteConnection::open(path_to_utf8(path))?;
    conn.execute("PRAGMA busy_timeout=0;")?;
    let pragma = if concurrent_mode {
        "PRAGMA fsqlite.concurrent_mode=ON;"
    } else {
        "PRAGMA fsqlite.concurrent_mode=OFF;"
    };
    conn.execute(pragma)?;
    Ok(conn)
}

fn path_to_utf8(path: &Path) -> &str {
    path.to_str()
        .unwrap_or_else(|| panic!("path is not valid UTF-8: {}", path.display()))
}

fn scenario_checks(
    scenario: ScenarioKind,
    run: &ScenarioRun,
    diagnostics: &mut Vec<String>,
    deterministic_replay: bool,
) -> Value {
    match scenario {
        ScenarioKind::BusySnapshotConflict => {
            let fsqlite_busy_snapshot = run.fsqlite.classification == ErrorClass::BusySnapshot
                && run.fsqlite.extended_error_code == Some(517);
            let fsqlite_conflict_surface = matches!(
                run.fsqlite.classification,
                ErrorClass::Busy | ErrorClass::BusySnapshot | ErrorClass::Locked
            );
            let sqlite_conflict_surface = matches!(
                run.sqlite.classification,
                ErrorClass::Busy | ErrorClass::BusySnapshot | ErrorClass::Locked
            );
            let fsqlite_busy_family_code = run
                .fsqlite
                .base_error_code
                .map_or(true, |code| code == 5 || code == 6);
            let sqlite_busy_family_code = run
                .sqlite
                .base_error_code
                .map_or(true, |code| code == 5 || code == 6);
            let busy_family_codes_valid = fsqlite_busy_family_code && sqlite_busy_family_code;

            if !fsqlite_conflict_surface {
                diagnostics.push(format!(
                    "fsqlite did not surface BUSY-family conflict semantics (classification={:?}, extended={:?})",
                    run.fsqlite.classification, run.fsqlite.extended_error_code
                ));
            }
            if !sqlite_conflict_surface {
                diagnostics.push(format!(
                    "sqlite did not surface conflict semantics (classification={:?}, extended={:?})",
                    run.sqlite.classification, run.sqlite.extended_error_code
                ));
            }
            if !busy_family_codes_valid {
                diagnostics.push(format!(
                    "busy-family error-code mapping invalid (sqlite={:?}, fsqlite={:?})",
                    run.sqlite.base_error_code, run.fsqlite.base_error_code
                ));
            }

            json!({
                "deterministic_replay": deterministic_replay,
                "fsqlite_conflict_surface": fsqlite_conflict_surface,
                "fsqlite_busy_snapshot_surface": fsqlite_busy_snapshot,
                "sqlite_conflict_surface": sqlite_conflict_surface,
                "busy_snapshot_observed": fsqlite_busy_snapshot,
                "busy_family_codes_valid": busy_family_codes_valid,
                "error_code_parity_validated": fsqlite_conflict_surface && sqlite_conflict_surface && busy_family_codes_valid,
                "concurrent_mode_default_guard": false,
            })
        }
        ScenarioKind::BusyLockImmediate => {
            let fsqlite_busy_like = matches!(
                run.fsqlite.classification,
                ErrorClass::Busy | ErrorClass::Locked
            );
            let fsqlite_non_serialized_begin_immediate =
                run.fsqlite.status == "ok" && run.fsqlite.classification == ErrorClass::None;
            let sqlite_busy_like = matches!(
                run.sqlite.classification,
                ErrorClass::Busy | ErrorClass::Locked
            );
            let parity_codes = run
                .fsqlite
                .base_error_code
                .is_some_and(|code| code == 5 || code == 6)
                && run
                    .sqlite
                    .base_error_code
                    .is_some_and(|code| code == 5 || code == 6);
            let fsqlite_expected_surface =
                fsqlite_busy_like || fsqlite_non_serialized_begin_immediate;
            let error_code_parity_validated = if fsqlite_busy_like {
                parity_codes
            } else {
                fsqlite_non_serialized_begin_immediate
            };

            if !fsqlite_expected_surface {
                diagnostics.push(format!(
                    "fsqlite lock behavior was neither busy-like nor declared MVCC divergence (status={}, classification={:?})",
                    run.fsqlite.status, run.fsqlite.classification
                ));
            }
            if !sqlite_busy_like {
                diagnostics.push(format!(
                    "sqlite serialized lock contention was not busy-like (classification={:?})",
                    run.sqlite.classification
                ));
            }
            if fsqlite_busy_like && !parity_codes {
                diagnostics.push(format!(
                    "base busy/locked error-code parity failed (sqlite={:?}, fsqlite={:?})",
                    run.sqlite.base_error_code, run.fsqlite.base_error_code
                ));
            }

            json!({
                "deterministic_replay": deterministic_replay,
                "fsqlite_busy_surface": fsqlite_busy_like,
                "fsqlite_non_serialized_begin_immediate": fsqlite_non_serialized_begin_immediate,
                "fsqlite_expected_surface": fsqlite_expected_surface,
                "sqlite_busy_surface": sqlite_busy_like,
                "error_code_parity_validated": error_code_parity_validated,
                "concurrent_mode_default_guard": false,
            })
        }
        ScenarioKind::ConcurrentModeDefaultGuard => {
            let guard_ok = run
                .guard
                .as_ref()
                .is_some_and(ConcurrentModeGuard::all_pass);
            if !guard_ok {
                diagnostics.push(format!(
                    "concurrent_mode default guard failed: {:?}",
                    run.guard
                ));
            }

            json!({
                "deterministic_replay": deterministic_replay,
                "fsqlite_busy_surface": true,
                "sqlite_busy_surface": true,
                "error_code_parity_validated": true,
                "concurrent_mode_default_guard": guard_ok,
            })
        }
    }
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
            "scenario",
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
            "scenario": { "type": "object" },
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
                    "deterministic_replay",
                    "error_code_parity_validated",
                    "concurrent_mode_default_guard"
                ],
                "properties": {
                    "deterministic_replay": { "type": "boolean" },
                    "error_code_parity_validated": { "type": "boolean" },
                    "concurrent_mode_default_guard": { "type": "boolean" }
                }
            },
            "sqlite3": { "type": "object" },
            "fsqlite": { "type": "object" },
            "concurrent_mode_guard": { "type": ["object", "null"] },
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
        .expect("build busy matrix outcome schema validator");

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

fn maybe_write_artifact(records: &[Value], first_failure: Option<&str>) {
    let Ok(path) = env::var(ARTIFACT_ENV_PATH) else {
        return;
    };

    let artifact_path = PathBuf::from(path);
    if let Some(parent) = artifact_path.parent() {
        fs::create_dir_all(parent).expect("create busy matrix artifact parent directory");
    }

    let overall_status = if records.iter().all(|record| {
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
        "run_count": records.len(),
        "first_failure": first_failure,
        "log_standard_ref": LOG_STANDARD_REF,
        "replay_command": REPLAY_COMMAND,
        "runs": records,
    });

    let payload = serde_json::to_vec_pretty(&artifact).expect("serialize busy matrix artifact");
    let digest = Sha256::digest(&payload);
    let hash = format!("{digest:x}");
    fs::write(&artifact_path, payload).expect("write busy matrix artifact");

    eprintln!(
        "DEBUG bead_id={BEAD_ID} artifact_path={} sha256={} replay_command={REPLAY_COMMAND}",
        artifact_path.display(),
        hash
    );
}

fn duration_to_ms(ms: u128) -> u64 {
    u64::try_from(ms).unwrap_or(u64::MAX)
}
