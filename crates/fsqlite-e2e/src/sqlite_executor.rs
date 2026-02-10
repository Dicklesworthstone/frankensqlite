//! SQLite (C SQLite) executor for OpLogs.
//!
//! This module executes an [`crate::oplog::OpLog`] against stock SQLite via
//! `rusqlite` (bundled libsqlite3) with configurable concurrency and
//! retry-on-busy instrumentation.
//!
//! The design goal is *reproducible differential testing*: given the same OpLog
//! and fixture DB file, both the FrankenSQLite executor and this executor
//! should apply identical logical effects. Physical file layout is handled by
//! separate canonicalization steps (VACUUM/exports) in other beads.

use std::path::Path;
use std::sync::Barrier;
use std::time::{Duration, Instant};

use rusqlite::ffi::ErrorCode;
use rusqlite::types::Value;
use rusqlite::{Connection, Transaction, params_from_iter};

use crate::oplog::{ExpectedResult, OpKind, OpLog, OpRecord};
use crate::report::{CorrectnessReport, EngineRunReport};
use crate::{E2eError, E2eResult};

/// Execution configuration for the C SQLite (rusqlite) OpLog executor.
#[derive(Debug, Clone)]
pub struct SqliteExecConfig {
    /// PRAGMA statements executed once per worker connection before running.
    ///
    /// Each entry should be a complete statement, e.g. `"PRAGMA journal_mode=WAL;"`.
    pub pragmas: Vec<String>,

    /// Maximum number of retries for a single transaction batch when SQLite
    /// returns `SQLITE_BUSY` / `SQLITE_LOCKED`.
    pub max_busy_retries: u32,

    /// Base backoff applied after each busy retry.
    pub busy_backoff: Duration,

    /// Maximum backoff cap.
    pub busy_backoff_max: Duration,

    /// Run `PRAGMA integrity_check` after the workload completes and populate
    /// [`CorrectnessReport::integrity_check_ok`].  Defaults to `true`.
    pub run_integrity_check: bool,
}

impl Default for SqliteExecConfig {
    fn default() -> Self {
        Self {
            pragmas: vec![
                "PRAGMA journal_mode=WAL;".to_owned(),
                "PRAGMA synchronous=NORMAL;".to_owned(),
                // We want *instrumented* retries, so keep SQLite's internal
                // busy handler effectively disabled.
                "PRAGMA busy_timeout=0;".to_owned(),
                "PRAGMA temp_store=MEMORY;".to_owned(),
            ],
            max_busy_retries: 10_000,
            busy_backoff: Duration::from_millis(1),
            busy_backoff_max: Duration::from_millis(250),
            run_integrity_check: true,
        }
    }
}

#[derive(Debug, Clone)]
struct WorkerStats {
    ops_ok: u64,
    ops_err: u64,
    retries: u64,
    aborts: u64,
    error: Option<String>,
}

/// Run an OpLog against C SQLite with concurrent worker threads.
///
/// - Uses one rusqlite `Connection` per worker (opened inside the worker thread).
/// - Retries whole transaction batches on `SQLITE_BUSY` / `SQLITE_LOCKED`.
///
/// # Errors
///
/// Returns an error only for setup failures (e.g. cannot open DB). Per-worker
/// execution failures are returned in the [`EngineRunReport::error`] field.
#[allow(clippy::too_many_lines)]
pub fn run_oplog_sqlite(
    db_path: &Path,
    oplog: &OpLog,
    config: &SqliteExecConfig,
) -> E2eResult<EngineRunReport> {
    let worker_count = oplog.header.concurrency.worker_count;
    if worker_count == 0 {
        return Err(E2eError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "oplog worker_count=0",
        )));
    }

    // Extract leading SQL-only records as global setup (DDL/PRAGMAs).
    let setup_len = oplog
        .records
        .iter()
        .take_while(|r| matches!(&r.kind, OpKind::Sql { .. }))
        .count();

    // Run setup SQL on a single connection first.
    if setup_len > 0 {
        let setup_conn = Connection::open(db_path)?;
        apply_pragmas(&setup_conn, &config.pragmas)?;
        for rec in &oplog.records[..setup_len] {
            if let OpKind::Sql { statement } = &rec.kind {
                setup_conn.execute_batch(statement)?;
            }
        }
    }

    // Partition remaining records by worker.
    let mut per_worker: Vec<Vec<OpRecord>> = vec![Vec::new(); usize::from(worker_count)];
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
        per_worker[idx].push(rec.clone());
    }

    let barrier = Barrier::new(usize::from(worker_count));
    let started = Instant::now();

    let worker_stats: Vec<WorkerStats> = std::thread::scope(|s| {
        let mut joins = Vec::with_capacity(usize::from(worker_count));
        for w in 0..worker_count {
            let records = per_worker[usize::from(w)].clone();
            let cfg = config.clone();
            let db = db_path.to_path_buf();
            let barrier_ref = &barrier;
            joins.push(s.spawn(move || run_worker(&db, w, &records, barrier_ref, &cfg)));
        }

        joins
            .into_iter()
            .map(|j| {
                j.join().unwrap_or_else(|_| WorkerStats {
                    ops_ok: 0,
                    ops_err: 0,
                    retries: 0,
                    aborts: 0,
                    error: Some("worker thread panicked".to_owned()),
                })
            })
            .collect()
    });

    let wall = started.elapsed();
    let wall_ms = duration_to_u64_ms(wall);
    let wall_secs = wall.as_secs_f64();

    let ops_ok = worker_stats.iter().map(|s| s.ops_ok).sum::<u64>();
    let ops_err = worker_stats.iter().map(|s| s.ops_err).sum::<u64>();
    let retries = worker_stats.iter().map(|s| s.retries).sum::<u64>();
    let aborts = worker_stats.iter().map(|s| s.aborts).sum::<u64>();

    let error = worker_stats
        .iter()
        .find_map(|s| s.error.clone())
        .or_else(|| {
            if ops_err > 0 {
                Some(format!("ops_err={ops_err}"))
            } else {
                None
            }
        });

    let ops_total = ops_ok + ops_err;
    #[allow(clippy::cast_precision_loss)]
    let ops_per_sec = if wall_secs > 0.0 {
        (ops_ok as f64) / wall_secs
    } else {
        0.0
    };

    let integrity_check_ok = if config.run_integrity_check {
        Some(run_integrity_check_sqlite(db_path))
    } else {
        None
    };

    Ok(EngineRunReport {
        wall_time_ms: wall_ms,
        ops_total,
        ops_per_sec,
        retries,
        aborts,
        correctness: CorrectnessReport {
            raw_sha256_match: None,
            dump_match: None,
            canonical_sha256_match: None,
            integrity_check_ok,
            raw_sha256: None,
            canonical_sha256: None,
            logical_sha256: None,
            notes: None,
        },
        latency_ms: None,
        error,
    })
}

/// Run `PRAGMA integrity_check` on the database at `db_path`.
///
/// Opens a fresh connection, checkpoints the WAL, then executes the pragma.
/// Returns `true` if the only result row is the string `"ok"`.
/// Returns `false` on any integrity error or if the connection/query fails.
pub fn run_integrity_check_sqlite(db_path: &Path) -> bool {
    let Ok(conn) = Connection::open(db_path) else {
        return false;
    };
    // Checkpoint WAL before checking so the integrity check covers all data.
    let _ = conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);");

    let result: Result<String, _> =
        conn.pragma_query_value(None, "integrity_check", |row| row.get(0));
    matches!(result, Ok(ref msg) if msg == "ok")
}

fn run_worker(
    db_path: &Path,
    worker_id: u16,
    records: &[OpRecord],
    barrier: &Barrier,
    config: &SqliteExecConfig,
) -> WorkerStats {
    let mut stats = WorkerStats {
        ops_ok: 0,
        ops_err: 0,
        retries: 0,
        aborts: 0,
        error: None,
    };

    let mut conn = match Connection::open(db_path) {
        Ok(c) => c,
        Err(e) => {
            stats.error = Some(format!("worker {worker_id} open failed: {e}"));
            return stats;
        }
    };

    if let Err(e) = apply_pragmas(&conn, &config.pragmas) {
        stats.error = Some(format!("worker {worker_id} pragmas failed: {e}"));
        return stats;
    }

    let batches = split_into_batches(records);

    // Try to align thread start to increase contention realism.
    barrier.wait();

    for batch in batches {
        if stats.error.is_some() {
            break;
        }

        let mut attempt: u32 = 0;
        loop {
            match execute_batch(&mut conn, &batch) {
                Ok((ok, err)) => {
                    stats.ops_ok += ok;
                    stats.ops_err += err;
                    break;
                }
                Err(BatchError::Busy(msg)) => {
                    stats.retries += 1;
                    stats.aborts += 1;
                    attempt = attempt.saturating_add(1);
                    if attempt > config.max_busy_retries {
                        stats.error = Some(format!(
                            "worker {worker_id}: exceeded max_busy_retries={} (last={msg})",
                            config.max_busy_retries
                        ));
                        break;
                    }
                    std::thread::sleep(backoff_duration(config, attempt));
                }
                Err(BatchError::Fatal(msg)) => {
                    stats.error = Some(format!("worker {worker_id}: {msg}"));
                    break;
                }
            }
        }
    }

    stats
}

fn apply_pragmas(conn: &Connection, pragmas: &[String]) -> Result<(), rusqlite::Error> {
    for p in pragmas {
        conn.execute_batch(p)?;
    }
    Ok(())
}

#[derive(Debug, Clone)]
struct Batch {
    ops: Vec<OpRecord>,
    commit: bool,
}

fn split_into_batches(records: &[OpRecord]) -> Vec<Batch> {
    let mut out = Vec::new();
    let mut in_txn = false;
    let mut current = Vec::new();

    for rec in records {
        match rec.kind {
            OpKind::Begin => {
                // Flush any prior autocommit ops.
                if !current.is_empty() {
                    out.push(Batch {
                        ops: std::mem::take(&mut current),
                        commit: true,
                    });
                }
                in_txn = true;
            }
            OpKind::Commit => {
                out.push(Batch {
                    ops: std::mem::take(&mut current),
                    commit: true,
                });
                in_txn = false;
            }
            OpKind::Rollback => {
                out.push(Batch {
                    ops: std::mem::take(&mut current),
                    commit: false,
                });
                in_txn = false;
            }
            _ => {
                current.push(rec.clone());
                if !in_txn && !current.is_empty() {
                    // Autocommit mode: one op per batch.
                    out.push(Batch {
                        ops: std::mem::take(&mut current),
                        commit: true,
                    });
                }
            }
        }
    }

    if !current.is_empty() {
        out.push(Batch {
            ops: current,
            commit: true,
        });
    }

    out
}

#[derive(Debug)]
enum BatchError {
    Busy(String),
    Fatal(String),
}

fn execute_batch(conn: &mut Connection, batch: &Batch) -> Result<(u64, u64), BatchError> {
    let tx = conn
        .transaction()
        .map_err(|e| classify_rusqlite_error(&e))?;

    let mut ok: u64 = 0;

    for op in &batch.ops {
        match execute_op(&tx, op) {
            Ok(()) => ok = ok.saturating_add(1),
            Err(OpError::Busy(msg)) => return Err(BatchError::Busy(msg)),
            Err(OpError::Fatal(msg)) => return Err(BatchError::Fatal(msg)),
        }
    }

    if batch.commit {
        tx.commit().map_err(|e| classify_rusqlite_error(&e))?;
    } else {
        // Rollback by dropping the transaction.
        drop(tx);
    }

    Ok((ok, 0))
}

#[derive(Debug)]
enum OpError {
    Busy(String),
    Fatal(String),
}

fn execute_op(tx: &Transaction<'_>, op: &OpRecord) -> Result<(), OpError> {
    match &op.kind {
        OpKind::Sql { statement } => execute_sql_stmt(tx, statement, op.expected.as_ref()),
        OpKind::Insert { table, key, values } => {
            execute_structured_insert(tx, table, *key, values, op.expected.as_ref())
        }
        OpKind::Update { table, key, values } => {
            execute_structured_update(tx, table, *key, values, op.expected.as_ref())
        }
        OpKind::Begin | OpKind::Commit | OpKind::Rollback => Ok(()),
    }
}

fn execute_sql_stmt(
    tx: &Transaction<'_>,
    statement: &str,
    expected: Option<&ExpectedResult>,
) -> Result<(), OpError> {
    let trimmed = statement.trim();
    let is_query = trimmed
        .split_whitespace()
        .next()
        .is_some_and(|w| w.eq_ignore_ascii_case("SELECT"));

    if is_query {
        match query_row_count(tx, trimmed) {
            Ok(rc) => {
                if let Some(ExpectedResult::RowCount(n)) = expected {
                    if rc != *n {
                        return Err(OpError::Fatal(format!(
                            "rowcount mismatch: expected {n}, got {rc} for `{trimmed}`"
                        )));
                    }
                }
                Ok(())
            }
            Err(e) => Err(classify_rusqlite_error_as_op(&e)),
        }
    } else {
        match tx.execute(trimmed, []) {
            Ok(affected) => {
                if let Some(ExpectedResult::AffectedRows(n)) = expected {
                    if affected != *n {
                        return Err(OpError::Fatal(format!(
                            "affected mismatch: expected {n}, got {affected} for `{trimmed}`"
                        )));
                    }
                }
                Ok(())
            }
            Err(e) => Err(classify_rusqlite_error_as_op(&e)),
        }
    }
}

fn execute_structured_insert(
    tx: &Transaction<'_>,
    table: &str,
    key: i64,
    values: &[(String, String)],
    expected: Option<&ExpectedResult>,
) -> Result<(), OpError> {
    let mut cols = Vec::with_capacity(values.len() + 1);
    let mut params: Vec<Value> = Vec::with_capacity(values.len() + 1);

    cols.push("\"id\"".to_owned());
    params.push(Value::Integer(key));

    for (col, v) in values {
        cols.push(format!("\"{}\"", escape_ident(col)));
        params.push(parse_sql_value(v));
    }

    let placeholders: Vec<String> = (1..=params.len()).map(|i| format!("?{i}")).collect();
    let sql = format!(
        "INSERT INTO \"{}\" ({}) VALUES ({})",
        escape_ident(table),
        cols.join(", "),
        placeholders.join(", ")
    );

    match tx.execute(&sql, params_from_iter(params)) {
        Ok(affected) => {
            if let Some(ExpectedResult::AffectedRows(n)) = expected {
                if affected != *n {
                    return Err(OpError::Fatal(format!(
                        "affected mismatch: expected {n}, got {affected} for `{sql}`"
                    )));
                }
            }
            Ok(())
        }
        Err(e) => Err(classify_rusqlite_error_as_op(&e)),
    }
}

fn execute_structured_update(
    tx: &Transaction<'_>,
    table: &str,
    key: i64,
    values: &[(String, String)],
    expected: Option<&ExpectedResult>,
) -> Result<(), OpError> {
    let mut sets = Vec::with_capacity(values.len());
    let mut params: Vec<Value> = Vec::with_capacity(values.len() + 1);

    // Param 1 is key.
    params.push(Value::Integer(key));

    for (idx, (col, v)) in values.iter().enumerate() {
        // Params start at ?2 for SET values.
        let p = idx + 2;
        sets.push(format!("\"{}\"=?{p}", escape_ident(col)));
        params.push(parse_sql_value(v));
    }

    let sql = format!(
        "UPDATE \"{}\" SET {} WHERE id=?1",
        escape_ident(table),
        sets.join(", ")
    );

    match tx.execute(&sql, params_from_iter(params)) {
        Ok(affected) => {
            if let Some(ExpectedResult::AffectedRows(n)) = expected {
                if affected != *n {
                    return Err(OpError::Fatal(format!(
                        "affected mismatch: expected {n}, got {affected} for `{sql}`"
                    )));
                }
            }
            Ok(())
        }
        Err(e) => Err(classify_rusqlite_error_as_op(&e)),
    }
}

fn query_row_count(tx: &Transaction<'_>, sql: &str) -> Result<usize, rusqlite::Error> {
    let mut stmt = tx.prepare(sql)?;
    let mut rows = stmt.query([])?;
    let mut count: usize = 0;
    while rows.next()?.is_some() {
        count = count.saturating_add(1);
    }
    Ok(count)
}

fn escape_ident(s: &str) -> String {
    s.replace('"', "\"\"")
}

fn parse_sql_value(s: &str) -> Value {
    if s.eq_ignore_ascii_case("null") {
        return Value::Null;
    }

    // Try integer first, then float, then text.
    if let Ok(i) = s.parse::<i64>() {
        return Value::Integer(i);
    }
    if let Ok(f) = s.parse::<f64>() {
        return Value::Real(f);
    }

    Value::Text(s.to_owned())
}

fn classify_rusqlite_error(err: &rusqlite::Error) -> BatchError {
    let code = err.sqlite_error_code();
    if matches!(
        code,
        Some(ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked)
    ) {
        BatchError::Busy(err.to_string())
    } else {
        BatchError::Fatal(err.to_string())
    }
}

fn classify_rusqlite_error_as_op(err: &rusqlite::Error) -> OpError {
    let code = err.sqlite_error_code();
    if matches!(
        code,
        Some(ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked)
    ) {
        OpError::Busy(err.to_string())
    } else {
        OpError::Fatal(err.to_string())
    }
}

fn backoff_duration(config: &SqliteExecConfig, attempt: u32) -> Duration {
    // Exponential backoff with cap.
    let shift = attempt.min(31);
    let base_ms = duration_to_u64_ms(config.busy_backoff);
    let max_ms = duration_to_u64_ms(config.busy_backoff_max);
    let factor_ms = 1_u64 << shift;
    let raw = base_ms.saturating_mul(factor_ms);
    Duration::from_millis(raw.min(max_ms))
}

fn duration_to_u64_ms(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::oplog::preset_commutative_inserts_disjoint_keys;

    #[test]
    fn test_run_oplog_sqlite_basic_concurrent() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        // Start from an empty DB file.
        Connection::open(&db_path).unwrap();

        let oplog = preset_commutative_inserts_disjoint_keys("test-fixture", 1, 4, 25);
        let report = run_oplog_sqlite(&db_path, &oplog, &SqliteExecConfig::default()).unwrap();
        assert!(report.error.is_none(), "error={:?}", report.error);
        assert!(report.ops_total > 0);

        // Verify row count is as expected.
        let conn = Connection::open(&db_path).unwrap();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM t0", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 100);
    }

    #[test]
    fn integrity_check_populates_report() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("integrity.db");
        Connection::open(&db_path).unwrap();

        let oplog = preset_commutative_inserts_disjoint_keys("test-fixture", 99, 1, 10);
        let report = run_oplog_sqlite(&db_path, &oplog, &SqliteExecConfig::default()).unwrap();

        assert_eq!(
            report.correctness.integrity_check_ok,
            Some(true),
            "healthy database should pass integrity_check"
        );
    }

    #[test]
    fn integrity_check_disabled_leaves_none() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("no_ic.db");
        Connection::open(&db_path).unwrap();

        let oplog = preset_commutative_inserts_disjoint_keys("test-fixture", 7, 1, 5);
        let config = SqliteExecConfig {
            run_integrity_check: false,
            ..SqliteExecConfig::default()
        };
        let report = run_oplog_sqlite(&db_path, &oplog, &config).unwrap();

        assert_eq!(
            report.correctness.integrity_check_ok, None,
            "integrity_check_ok should be None when disabled"
        );
    }

    #[test]
    fn run_deterministic_transform_sqlite() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("dt_sqlite.db");

        let oplog = crate::oplog::preset_deterministic_transform("dt-sqlite", 42, 30);
        let report = run_oplog_sqlite(&db_path, &oplog, &SqliteExecConfig::default()).unwrap();

        assert!(report.error.is_none(), "error={:?}", report.error);
        assert!(report.ops_total > 100);
        assert_eq!(
            report.correctness.integrity_check_ok,
            Some(true),
            "DB should pass integrity_check after deterministic transform"
        );
    }

    #[test]
    fn run_integrity_check_on_healthy_db() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("healthy.db");
        let conn = Connection::open(&db_path).unwrap();
        conn.execute_batch("CREATE TABLE t(x); INSERT INTO t VALUES(1);")
            .unwrap();
        drop(conn);

        assert!(
            run_integrity_check_sqlite(&db_path),
            "healthy database should pass"
        );
    }
}
