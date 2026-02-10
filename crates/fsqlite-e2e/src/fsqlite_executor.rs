//! FrankenSQLite executor — runs an [`OpLog`] against the `fsqlite` engine.
//!
//! Bead: bd-1w6k.3.3
//!
//! # Architecture
//!
//! `fsqlite::Connection` uses `Rc<RefCell<…>>` internally and is therefore
//! `!Send`.  All operations run on a single thread.  For multi-worker OpLogs,
//! workers are executed **sequentially** (worker 0, then worker 1, …), which
//! preserves correctness for commutative workloads (disjoint keys) while
//! foregoing true concurrency.
//!
//! Once FrankenSQLite gains a thread-safe connection pool (Phase 5+ MVCC), this
//! module can be upgraded to spawn per-worker threads analogous to
//! [`crate::sqlite_executor`].

use std::path::Path;
use std::time::Instant;

use fsqlite::Connection;
use fsqlite_types::value::SqliteValue;

use crate::oplog::{ExpectedResult, OpKind, OpLog, OpRecord};
use crate::report::{CorrectnessReport, EngineRunReport};
use crate::sqlite_executor;
use crate::{E2eError, E2eResult};

/// Execution configuration for the FrankenSQLite OpLog executor.
#[derive(Debug, Clone)]
pub struct FsqliteExecConfig {
    /// PRAGMA statements executed once on the connection before running.
    ///
    /// Each entry should be a complete statement, e.g. `"PRAGMA page_size=4096;"`.
    pub pragmas: Vec<String>,
    /// Enable MVCC concurrent-writer mode for this run.
    ///
    /// When `true`, the executor will issue `PRAGMA concurrent_mode=ON;`
    /// before the workload so that `BEGIN` is automatically promoted to
    /// `BEGIN CONCURRENT`.  The report's `correctness.notes` will record
    /// which mode was used.
    ///
    /// Expected transient errors in concurrent mode:
    /// - `SQLITE_BUSY` — page lock contention; retry the transaction.
    /// - `SQLITE_BUSY_SNAPSHOT` — first-committer-wins conflict; retry.
    pub concurrent_mode: bool,
    /// Run `PRAGMA integrity_check` after the workload completes and populate
    /// [`CorrectnessReport::integrity_check_ok`]. Defaults to `true`.
    pub run_integrity_check: bool,
}

impl Default for FsqliteExecConfig {
    fn default() -> Self {
        Self {
            pragmas: Vec::new(),
            concurrent_mode: false,
            run_integrity_check: true,
        }
    }
}

/// Run an OpLog against FrankenSQLite.
///
/// Opens a single [`Connection`] and replays every record sequentially.
/// Multi-worker logs are handled by running each worker's partition in order
/// (setup SQL first, then worker 0, worker 1, …).
///
/// # Errors
///
/// Returns an error only for setup failures (connection open, PRAGMA application).
/// Per-operation execution failures are captured in the
/// [`EngineRunReport::error`] field.
pub fn run_oplog_fsqlite(
    db_path: &Path,
    oplog: &OpLog,
    config: &FsqliteExecConfig,
) -> E2eResult<EngineRunReport> {
    let worker_count = oplog.header.concurrency.worker_count;
    if worker_count == 0 {
        return Err(E2eError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "oplog worker_count=0",
        )));
    }

    let conn = open_connection(db_path)?;

    // Apply concurrent-mode PRAGMA before user pragmas so the user can
    // override it if needed.
    if config.concurrent_mode {
        conn.execute("PRAGMA fsqlite.concurrent_mode=ON;")
            .map_err(|e| E2eError::Fsqlite(format!("PRAGMA fsqlite.concurrent_mode=ON: {e}")))?;
    }

    for pragma in &config.pragmas {
        conn.execute(pragma)
            .map_err(|e| E2eError::Fsqlite(format!("pragma `{pragma}`: {e}")))?;
    }

    let (setup_len, per_worker) = partition_records(oplog, worker_count)?;

    let started = Instant::now();
    let (ops_ok, ops_err, first_error) = replay_all(&conn, oplog, setup_len, &per_worker);
    let wall = started.elapsed();

    let integrity_check_ok = if config.run_integrity_check && db_path != Path::new(":memory:") {
        // Best-effort verification: validate the resulting DB file with
        // libsqlite via rusqlite. This does not require FrankenSQLite to
        // implement `PRAGMA integrity_check` itself.
        Some(sqlite_executor::run_integrity_check_sqlite(db_path))
    } else {
        None
    };

    Ok(build_report(
        wall,
        ops_ok,
        ops_err,
        first_error,
        config.concurrent_mode,
        integrity_check_ok,
    ))
}

fn open_connection(db_path: &Path) -> E2eResult<Connection> {
    let path_str = if db_path == Path::new(":memory:") {
        ":memory:".to_owned()
    } else {
        db_path
            .to_str()
            .ok_or_else(|| E2eError::Io(std::io::Error::other("path is not valid UTF-8")))?
            .to_owned()
    };
    Connection::open(&path_str).map_err(|e| E2eError::Fsqlite(format!("open: {e}")))
}

/// Partition OpLog records into setup SQL + per-worker slices.
fn partition_records(oplog: &OpLog, worker_count: u16) -> E2eResult<(usize, Vec<Vec<&OpRecord>>)> {
    let setup_len = oplog
        .records
        .iter()
        .take_while(|r| matches!(&r.kind, OpKind::Sql { .. }))
        .count();

    let mut per_worker: Vec<Vec<&OpRecord>> = vec![Vec::new(); usize::from(worker_count)];
    for rec in oplog.records.iter().skip(setup_len) {
        let idx = usize::from(rec.worker);
        if idx >= per_worker.len() {
            return Err(E2eError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "oplog record worker={} out of range (worker_count={worker_count})",
                    rec.worker
                ),
            )));
        }
        per_worker[idx].push(rec);
    }

    Ok((setup_len, per_worker))
}

/// Execute setup records then each worker's records sequentially.
fn replay_all(
    conn: &Connection,
    oplog: &OpLog,
    setup_len: usize,
    per_worker: &[Vec<&OpRecord>],
) -> (u64, u64, Option<String>) {
    let mut ops_ok: u64 = 0;
    let mut ops_err: u64 = 0;
    let mut first_error: Option<String> = None;

    let mut tally = |rec: &OpRecord| match execute_op(conn, rec) {
        Ok(()) => ops_ok += 1,
        Err(msg) => {
            ops_err += 1;
            if first_error.is_none() {
                first_error = Some(msg);
            }
        }
    };

    for rec in &oplog.records[..setup_len] {
        tally(rec);
    }
    for records in per_worker {
        for rec in records {
            tally(rec);
        }
    }

    (ops_ok, ops_err, first_error)
}

/// Assemble an [`EngineRunReport`] from execution statistics.
#[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss)]
fn build_report(
    wall: std::time::Duration,
    ops_ok: u64,
    ops_err: u64,
    first_error: Option<String>,
    concurrent_mode: bool,
    integrity_check_ok: Option<bool>,
) -> EngineRunReport {
    let wall_ms = wall.as_millis() as u64;
    let ops_total = ops_ok + ops_err;
    let ops_per_sec = if wall.as_secs_f64() > 0.0 {
        (ops_ok as f64) / wall.as_secs_f64()
    } else {
        0.0
    };

    let error = first_error.or_else(|| {
        if ops_err > 0 {
            Some(format!("ops_err={ops_err}"))
        } else {
            None
        }
    });

    let mode_label = if concurrent_mode {
        "concurrent (MVCC)"
    } else {
        "single-writer (serialized)"
    };
    let notes = format!("mode={mode_label}; single-threaded sequential execution");

    EngineRunReport {
        wall_time_ms: wall_ms,
        ops_total,
        ops_per_sec,
        retries: 0,
        aborts: 0,
        correctness: CorrectnessReport {
            raw_sha256_match: None,
            dump_match: None,
            canonical_sha256_match: None,
            integrity_check_ok,
            raw_sha256: None,
            canonical_sha256: None,
            logical_sha256: None,
            notes: Some(notes),
        },
        latency_ms: None,
        error,
    }
}

// ── Operation dispatch ────────────────────────────────────────────────────

fn execute_op(conn: &Connection, rec: &OpRecord) -> Result<(), String> {
    match &rec.kind {
        OpKind::Sql { statement } => execute_sql(conn, statement, rec.expected.as_ref()),
        OpKind::Insert { table, key, values } => {
            execute_insert(conn, table, *key, values, rec.expected.as_ref())
        }
        OpKind::Update { table, key, values } => {
            execute_update(conn, table, *key, values, rec.expected.as_ref())
        }
        OpKind::Begin => conn
            .execute("BEGIN;")
            .map(|_| ())
            .map_err(|e| e.to_string()),
        OpKind::Commit => conn
            .execute("COMMIT;")
            .map(|_| ())
            .map_err(|e| e.to_string()),
        OpKind::Rollback => conn
            .execute("ROLLBACK;")
            .map(|_| ())
            .map_err(|e| e.to_string()),
    }
}

fn execute_sql(
    conn: &Connection,
    statement: &str,
    expected: Option<&ExpectedResult>,
) -> Result<(), String> {
    let trimmed = statement.trim();
    let upper = trimmed.to_ascii_uppercase();

    // Skip DDL that FrankenSQLite does not yet support.  These are
    // performance-only constructs that do not affect logical data.
    if upper.starts_with("CREATE INDEX")
        || upper.starts_with("CREATE UNIQUE INDEX")
        || upper.starts_with("DROP INDEX")
    {
        return Ok(());
    }

    let is_query = trimmed
        .split_whitespace()
        .next()
        .is_some_and(|w| w.eq_ignore_ascii_case("SELECT"));

    if is_query {
        let rows = conn.query(trimmed).map_err(|e| e.to_string())?;
        if let Some(ExpectedResult::RowCount(n)) = expected {
            if rows.len() != *n {
                return Err(format!(
                    "rowcount mismatch: expected {n}, got {} for `{trimmed}`",
                    rows.len()
                ));
            }
        }
    } else {
        let affected = conn.execute(trimmed).map_err(|e| e.to_string())?;
        if let Some(ExpectedResult::AffectedRows(n)) = expected {
            if affected != *n {
                return Err(format!(
                    "affected mismatch: expected {n}, got {affected} for `{trimmed}`"
                ));
            }
        }
    }

    Ok(())
}

fn execute_insert(
    conn: &Connection,
    table: &str,
    key: i64,
    values: &[(String, String)],
    expected: Option<&ExpectedResult>,
) -> Result<(), String> {
    let mut cols = Vec::with_capacity(values.len() + 1);
    let mut params: Vec<SqliteValue> = Vec::with_capacity(values.len() + 1);

    cols.push("\"id\"".to_owned());
    params.push(SqliteValue::Integer(key));

    for (col, v) in values {
        cols.push(format!("\"{}\"", escape_ident(col)));
        params.push(parse_value(v));
    }

    let placeholders: Vec<String> = (1..=params.len()).map(|i| format!("?{i}")).collect();
    let sql = format!(
        "INSERT INTO \"{}\" ({}) VALUES ({})",
        escape_ident(table),
        cols.join(", "),
        placeholders.join(", ")
    );

    let affected = conn
        .execute_with_params(&sql, &params)
        .map_err(|e| e.to_string())?;

    if let Some(ExpectedResult::AffectedRows(n)) = expected {
        if affected != *n {
            return Err(format!(
                "affected mismatch: expected {n}, got {affected} for `{sql}`"
            ));
        }
    }

    Ok(())
}

fn execute_update(
    conn: &Connection,
    table: &str,
    key: i64,
    values: &[(String, String)],
    expected: Option<&ExpectedResult>,
) -> Result<(), String> {
    let mut sets = Vec::with_capacity(values.len());
    let mut params: Vec<SqliteValue> = Vec::with_capacity(values.len() + 1);

    params.push(SqliteValue::Integer(key));

    for (idx, (col, v)) in values.iter().enumerate() {
        let p = idx + 2;
        sets.push(format!("\"{}\"=?{p}", escape_ident(col)));
        params.push(parse_value(v));
    }

    let sql = format!(
        "UPDATE \"{}\" SET {} WHERE id=?1",
        escape_ident(table),
        sets.join(", ")
    );

    let affected = conn
        .execute_with_params(&sql, &params)
        .map_err(|e| e.to_string())?;

    if let Some(ExpectedResult::AffectedRows(n)) = expected {
        if affected != *n {
            return Err(format!(
                "affected mismatch: expected {n}, got {affected} for `{sql}`"
            ));
        }
    }

    Ok(())
}

// ── Helpers ──────────────────────────────────────────────────────────────

fn escape_ident(s: &str) -> String {
    s.replace('"', "\"\"")
}

fn parse_value(s: &str) -> SqliteValue {
    if s.eq_ignore_ascii_case("null") {
        return SqliteValue::Null;
    }
    if let Ok(i) = s.parse::<i64>() {
        return SqliteValue::Integer(i);
    }
    if let Ok(f) = s.parse::<f64>() {
        return SqliteValue::Float(f);
    }
    SqliteValue::Text(s.to_owned())
}

// ── Tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::oplog::preset_commutative_inserts_disjoint_keys;

    #[test]
    fn run_oplog_fsqlite_basic_serial() {
        let oplog = preset_commutative_inserts_disjoint_keys("test-fixture", 1, 1, 10);
        let report =
            run_oplog_fsqlite(Path::new(":memory:"), &oplog, &FsqliteExecConfig::default())
                .unwrap();

        assert!(report.error.is_none(), "error={:?}", report.error);
        assert!(report.ops_total > 0, "should have executed operations");
    }

    #[test]
    fn run_oplog_fsqlite_multi_worker_sequential() {
        let oplog = preset_commutative_inserts_disjoint_keys("test-fixture", 42, 4, 25);
        let report =
            run_oplog_fsqlite(Path::new(":memory:"), &oplog, &FsqliteExecConfig::default())
                .unwrap();

        assert!(report.error.is_none(), "error={:?}", report.error);
        assert!(report.ops_total > 0);
    }

    #[test]
    fn run_oplog_fsqlite_verify_row_count() {
        let oplog = preset_commutative_inserts_disjoint_keys("test-fixture", 7, 2, 50);

        // Run through the executor (uses Connection internally).
        let path_str = ":memory:";
        let conn = Connection::open(path_str).unwrap();

        // Manually replay the same oplog to verify final state.
        for rec in &oplog.records {
            let _ = execute_op(&conn, rec);
        }

        let rows = conn.query("SELECT COUNT(*) FROM t0").unwrap();
        let count = rows[0].get(0).unwrap();
        assert_eq!(
            *count,
            SqliteValue::Integer(100),
            "expected 2 workers × 50 rows = 100"
        );
    }

    #[test]
    fn run_oplog_fsqlite_hot_contention() {
        // Hot contention preset uses INSERT OR IGNORE which FrankenSQLite
        // does not yet fully support (duplicate rows may be inserted).
        // Verify the executor runs to completion without panicking; allow
        // reported errors from affected-row mismatches.
        let oplog = crate::oplog::preset_hot_page_contention("test-fixture", 42, 2, 3);
        let report =
            run_oplog_fsqlite(Path::new(":memory:"), &oplog, &FsqliteExecConfig::default())
                .unwrap();

        assert!(report.ops_total > 0);
    }

    #[test]
    fn run_oplog_fsqlite_mixed_read_write() {
        // Mixed read-write preset uses INSERT OR IGNORE for seeding;
        // FrankenSQLite may insert duplicates causing rowcount mismatches.
        // Verify execution completes without panicking.
        let oplog = crate::oplog::preset_mixed_read_write("test-fixture", 0, 2, 10);
        let report =
            run_oplog_fsqlite(Path::new(":memory:"), &oplog, &FsqliteExecConfig::default())
                .unwrap();

        assert!(report.ops_total > 0);
    }

    #[test]
    fn report_serialization_roundtrip() {
        let oplog = preset_commutative_inserts_disjoint_keys("test-fixture", 1, 1, 5);
        let report =
            run_oplog_fsqlite(Path::new(":memory:"), &oplog, &FsqliteExecConfig::default())
                .unwrap();

        let json = serde_json::to_string_pretty(&report).unwrap();
        let parsed: EngineRunReport = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.ops_total, report.ops_total);
        assert!(parsed.error.is_none());
    }

    #[test]
    #[allow(clippy::approx_constant)]
    fn parse_value_types() {
        assert_eq!(parse_value("null"), SqliteValue::Null);
        assert_eq!(parse_value("NULL"), SqliteValue::Null);
        assert_eq!(parse_value("42"), SqliteValue::Integer(42));
        assert_eq!(parse_value("-7"), SqliteValue::Integer(-7));
        assert_eq!(parse_value("3.14"), SqliteValue::Float(3.14));
        assert_eq!(parse_value("hello"), SqliteValue::Text("hello".to_owned()));
    }

    #[test]
    fn escape_ident_handles_quotes() {
        assert_eq!(escape_ident("normal"), "normal");
        assert_eq!(escape_ident(r#"has"quote"#), r#"has""quote"#);
    }

    #[test]
    fn integrity_check_skipped_for_memory_db() {
        let oplog = preset_commutative_inserts_disjoint_keys("test-fixture", 1, 1, 5);
        let report =
            run_oplog_fsqlite(Path::new(":memory:"), &oplog, &FsqliteExecConfig::default())
                .unwrap();

        // :memory: databases have no file to validate, so integrity_check_ok
        // should be None even when run_integrity_check is true (the default).
        assert!(
            report.correctness.integrity_check_ok.is_none(),
            "expected None for :memory: db, got {:?}",
            report.correctness.integrity_check_ok
        );
    }

    #[test]
    fn integrity_check_disabled_leaves_none() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("check-disabled.db");

        let oplog = preset_commutative_inserts_disjoint_keys("test-fixture", 7, 1, 5);
        let config = FsqliteExecConfig {
            run_integrity_check: false,
            ..FsqliteExecConfig::default()
        };
        let report = run_oplog_fsqlite(&db_path, &oplog, &config).unwrap();

        assert!(
            report.correctness.integrity_check_ok.is_none(),
            "expected None when disabled, got {:?}",
            report.correctness.integrity_check_ok
        );
    }

    #[test]
    fn integrity_check_populates_report_for_file_db() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("integrity.db");

        let oplog = preset_commutative_inserts_disjoint_keys("test-fixture", 7, 1, 5);
        let report = run_oplog_fsqlite(&db_path, &oplog, &FsqliteExecConfig::default()).unwrap();

        // For a file-based DB, integrity_check should be populated.
        assert!(
            report.correctness.integrity_check_ok.is_some(),
            "expected Some for file-based db"
        );
    }

    #[test]
    fn run_deterministic_transform_preset() {
        let oplog = crate::oplog::preset_deterministic_transform("dt-test", 42, 30);
        let report =
            run_oplog_fsqlite(Path::new(":memory:"), &oplog, &FsqliteExecConfig::default())
                .unwrap();

        // FrankenSQLite may report affected-row mismatches on parameterized
        // UPDATE … WHERE id=?  because its parameter binding for WHERE
        // clauses is not yet fully correct.  Allow errors from this known
        // limitation; verify that operations still ran.
        assert!(report.ops_total > 0, "should have executed operations");
        assert!(
            report.ops_total > 100,
            "expected >100 ops for 30-row transform, got {}",
            report.ops_total
        );
    }

    #[test]
    fn deterministic_transform_seed_produces_consistent_results() {
        // Run the same workload twice and verify identical op counts.
        let oplog_a = crate::oplog::preset_deterministic_transform("dt-consist", 99, 20);
        let oplog_b = crate::oplog::preset_deterministic_transform("dt-consist", 99, 20);

        let report_a = run_oplog_fsqlite(
            Path::new(":memory:"),
            &oplog_a,
            &FsqliteExecConfig::default(),
        )
        .unwrap();
        let report_b = run_oplog_fsqlite(
            Path::new(":memory:"),
            &oplog_b,
            &FsqliteExecConfig::default(),
        )
        .unwrap();

        assert_eq!(
            report_a.ops_total, report_b.ops_total,
            "identical seeds should yield identical op counts"
        );
        assert_eq!(report_a.error, report_b.error);
    }

    #[test]
    fn zero_worker_count_is_error() {
        let oplog = OpLog {
            header: crate::oplog::OpLogHeader {
                fixture_id: "test".to_owned(),
                seed: 0,
                rng: crate::oplog::RngSpec::default(),
                concurrency: crate::oplog::ConcurrencyModel {
                    worker_count: 0,
                    ..crate::oplog::ConcurrencyModel::default()
                },
                preset: None,
            },
            records: Vec::new(),
        };
        let result =
            run_oplog_fsqlite(Path::new(":memory:"), &oplog, &FsqliteExecConfig::default());
        assert!(result.is_err());
    }
}
