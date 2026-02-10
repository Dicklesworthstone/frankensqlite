//! Reference executor — runs an [`OpLog`] against stock `sqlite3` CLI.
//!
//! # Architecture
//!
//! One OS process per worker, each holding an open `sqlite3` connection to
//! the target database.  Operations are fed via per-worker `.sql` scripts
//! and executed concurrently.  PRAGMA `busy_timeout` handles lock contention;
//! errors are captured from stderr for the structured run report.
//!
//! # Concurrency modes
//!
//! - **free**: all worker scripts run concurrently with no coordination.
//! - **deterministic** / **barrier**: currently treated as *free* at the CLI
//!   level.  True deterministic ordering requires tighter process control
//!   (see [`Sqlite3Executor::run`] for details).

use std::collections::HashMap;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::time::Instant;

use serde::{Deserialize, Serialize};

use crate::oplog::{OpKind, OpLog, OpRecord};
use crate::{E2eError, E2eResult};

// ── Configuration ──────────────────────────────────────────────────────

/// Configuration for a `sqlite3` executor run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutorConfig {
    /// Path to the `sqlite3` binary.
    pub sqlite3_bin: String,
    /// Journal mode PRAGMA value (e.g. `"wal"`, `"delete"`).
    pub journal_mode: String,
    /// Synchronous PRAGMA value (e.g. `"NORMAL"`, `"FULL"`).
    pub synchronous: String,
    /// Busy timeout in milliseconds.
    pub busy_timeout_ms: u32,
    /// Scratch directory for per-worker `.sql` scripts.  If `None`, a tempdir
    /// is created automatically.
    pub scratch_dir: Option<PathBuf>,
}

impl Default for ExecutorConfig {
    fn default() -> Self {
        Self {
            sqlite3_bin: "sqlite3".to_owned(),
            journal_mode: "wal".to_owned(),
            synchronous: "NORMAL".to_owned(),
            busy_timeout_ms: 5000,
            scratch_dir: None,
        }
    }
}

// ── Run report ─────────────────────────────────────────────────────────

/// Structured JSON report produced by a run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunReport {
    /// Database path that was tested.
    pub db_path: String,
    /// OpLog preset name (if any).
    pub preset: Option<String>,
    /// Number of workers.
    pub worker_count: u16,
    /// Per-worker results.
    pub workers: Vec<WorkerReport>,
    /// Total wall-clock duration of the run.
    pub total_duration_ms: u64,
    /// Overall success: true if every worker exited 0.
    pub success: bool,
}

/// Report for a single worker process.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerReport {
    /// Worker index (0-based).
    pub worker_id: u16,
    /// Number of SQL statements fed to this worker.
    pub statement_count: usize,
    /// sqlite3 process exit code.
    pub exit_code: i32,
    /// Wall-clock duration for this worker.
    pub duration_ms: u64,
    /// Number of `SQLITE_BUSY` errors detected in stderr.
    pub busy_count: usize,
    /// Number of other errors detected in stderr.
    pub error_count: usize,
    /// Raw stderr output (truncated to 4 KiB for the report).
    pub stderr_snippet: String,
}

// ── Executor ───────────────────────────────────────────────────────────

/// Executes an [`OpLog`] against the `sqlite3` CLI.
pub struct Sqlite3Executor {
    config: ExecutorConfig,
}

impl Sqlite3Executor {
    /// Create an executor with the given configuration.
    #[must_use]
    pub fn new(config: ExecutorConfig) -> Self {
        Self { config }
    }

    /// Create an executor with default settings.
    #[must_use]
    pub fn with_defaults() -> Self {
        Self::new(ExecutorConfig::default())
    }

    /// Run the given `OpLog` against `db_path`.
    ///
    /// Spawns one `sqlite3` child process per worker, feeds each a `.sql`
    /// script, and collects exit codes + stderr for the report.
    ///
    /// # Errors
    ///
    /// Returns `E2eError::Io` on process spawn or temp-file failures.
    #[allow(clippy::too_many_lines, clippy::cast_possible_truncation)]
    pub fn run(&self, oplog: &OpLog, db_path: &Path) -> E2eResult<RunReport> {
        let worker_count = oplog.header.concurrency.worker_count;

        // Split records by worker.
        let mut worker_ops: HashMap<u16, Vec<&OpRecord>> = HashMap::new();
        for rec in &oplog.records {
            worker_ops.entry(rec.worker).or_default().push(rec);
        }

        // Determine scratch dir.
        let _tempdir;
        let scratch = if let Some(ref dir) = self.config.scratch_dir {
            std::fs::create_dir_all(dir)?;
            dir.clone()
        } else {
            let td = tempfile::tempdir()?;
            let p = td.path().to_path_buf();
            _tempdir = td;
            p
        };

        // Generate per-worker SQL scripts.
        let mut worker_scripts: Vec<(u16, PathBuf, usize)> = Vec::new();
        for w in 0..worker_count {
            let ops = worker_ops.get(&w).map_or(&[][..], |v| v.as_slice());
            let sql = self.generate_worker_sql(ops);
            let stmt_count = ops.len();
            let script_path = scratch.join(format!("worker_{w}.sql"));
            std::fs::write(&script_path, sql.as_bytes())?;
            worker_scripts.push((w, script_path, stmt_count));
        }

        // Run worker 0 first (schema setup), then remaining workers concurrently.
        let start = Instant::now();
        let mut workers: Vec<WorkerReport> = Vec::with_capacity(worker_scripts.len());

        // Phase 1: worker 0 runs alone to establish schema.
        if let Some(&(w, ref script_path, stmt_count)) = worker_scripts.first() {
            let worker_start = Instant::now();
            let output = std::process::Command::new(&self.config.sqlite3_bin)
                .arg(db_path)
                .stdin(std::process::Stdio::from(std::fs::File::open(script_path)?))
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .output()
                .map_err(|e| {
                    E2eError::Io(std::io::Error::new(
                        e.kind(),
                        format!("failed to spawn sqlite3 for worker {w}: {e}"),
                    ))
                })?;
            workers.push(Self::collect_worker_report(
                w,
                stmt_count,
                &output,
                &worker_start,
            ));
        }

        // Phase 2: workers 1..N run concurrently.
        let mut handles: Vec<(u16, usize, std::process::Child, Instant)> = Vec::new();

        for (w, script_path, stmt_count) in worker_scripts.iter().skip(1) {
            let child = std::process::Command::new(&self.config.sqlite3_bin)
                .arg(db_path)
                .stdin(std::process::Stdio::from(std::fs::File::open(script_path)?))
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .spawn()
                .map_err(|e| {
                    E2eError::Io(std::io::Error::new(
                        e.kind(),
                        format!("failed to spawn sqlite3 for worker {w}: {e}"),
                    ))
                })?;
            handles.push((*w, *stmt_count, child, Instant::now()));
        }

        // Collect concurrent results.
        for (w, stmt_count, child, worker_start) in handles {
            let output = child.wait_with_output()?;
            workers.push(Self::collect_worker_report(
                w,
                stmt_count,
                &output,
                &worker_start,
            ));
        }

        let total_duration = start.elapsed();
        let success = workers.iter().all(|w| w.exit_code == 0);

        Ok(RunReport {
            db_path: db_path.to_string_lossy().into_owned(),
            preset: oplog.header.preset.clone(),
            worker_count,
            workers,
            total_duration_ms: total_duration.as_millis() as u64,
            success,
        })
    }

    /// Build a [`WorkerReport`] from a completed `sqlite3` process.
    #[allow(clippy::cast_possible_truncation)]
    fn collect_worker_report(
        worker_id: u16,
        stmt_count: usize,
        output: &std::process::Output,
        start: &Instant,
    ) -> WorkerReport {
        let duration = start.elapsed();
        let exit_code = output.status.code().unwrap_or(-1);
        let stderr_full = String::from_utf8_lossy(&output.stderr).into_owned();
        let busy_count = count_pattern(&stderr_full, "database is locked")
            + count_pattern(&stderr_full, "SQLITE_BUSY");
        let error_count = count_pattern(&stderr_full, "Error:")
            + count_pattern(&stderr_full, "error:")
            + count_pattern(&stderr_full, "Runtime error");
        WorkerReport {
            worker_id,
            statement_count: stmt_count,
            exit_code,
            duration_ms: duration.as_millis() as u64,
            busy_count,
            error_count,
            stderr_snippet: truncate_string(&stderr_full, 4096),
        }
    }

    /// Generate the SQL script for a single worker.
    fn generate_worker_sql(&self, ops: &[&OpRecord]) -> String {
        let mut sql = String::with_capacity(ops.len() * 80);

        // Preamble: PRAGMAs.
        // Set busy_timeout first so any subsequent PRAGMAs that need locks
        // (e.g. journal_mode) will wait instead of failing immediately.
        let _ = writeln!(
            sql,
            ".bail on\nPRAGMA busy_timeout={};\nPRAGMA journal_mode={};\nPRAGMA synchronous={};",
            self.config.busy_timeout_ms, self.config.journal_mode, self.config.synchronous,
        );

        for op in ops {
            match &op.kind {
                OpKind::Sql { statement } => {
                    sql.push_str(statement);
                    if !statement.ends_with(';') {
                        sql.push(';');
                    }
                    sql.push('\n');
                }
                OpKind::Insert { table, key, values } => {
                    let cols: Vec<&str> = values.iter().map(|(c, _)| c.as_str()).collect();
                    let vals: Vec<String> = values.iter().map(|(_, v)| quote_value(v)).collect();
                    let _ = writeln!(
                        sql,
                        "INSERT INTO \"{table}\" (id, {cols}) VALUES ({key}, {vals});",
                        cols = cols.join(", "),
                        vals = vals.join(", "),
                    );
                }
                OpKind::Update { table, key, values } => {
                    let sets: Vec<String> = values
                        .iter()
                        .map(|(c, v)| format!("\"{c}\" = {}", quote_value(v)))
                        .collect();
                    let _ = writeln!(
                        sql,
                        "UPDATE \"{table}\" SET {sets} WHERE id = {key};",
                        sets = sets.join(", "),
                    );
                }
                OpKind::Begin => sql.push_str("BEGIN;\n"),
                OpKind::Commit => sql.push_str("COMMIT;\n"),
                OpKind::Rollback => sql.push_str("ROLLBACK;\n"),
            }
        }

        sql
    }
}

// ── Helpers ─────────────────────────────────────────────────────────────

/// Quote a value for insertion into SQL.  Attempts to detect numeric values
/// and pass them unquoted; everything else is single-quoted with escaping.
fn quote_value(v: &str) -> String {
    if v.parse::<i64>().is_ok() || v.parse::<f64>().is_ok() {
        v.to_owned()
    } else {
        format!("'{}'", v.replace('\'', "''"))
    }
}

/// Count non-overlapping occurrences of `pattern` in `haystack`.
fn count_pattern(haystack: &str, pattern: &str) -> usize {
    haystack.matches(pattern).count()
}

/// Truncate a string to at most `max_bytes` bytes (UTF-8 safe).
fn truncate_string(s: &str, max_bytes: usize) -> String {
    if s.len() <= max_bytes {
        return s.to_owned();
    }
    // Walk back from max_bytes to find a char boundary.
    let mut end = max_bytes;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    let mut truncated = s[..end].to_owned();
    truncated.push_str("...[truncated]");
    truncated
}

// ── Tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::oplog;

    #[test]
    fn test_quote_value_integer() {
        assert_eq!(quote_value("42"), "42");
        assert_eq!(quote_value("-7"), "-7");
    }

    #[test]
    fn test_quote_value_float() {
        assert_eq!(quote_value("3.14"), "3.14");
    }

    #[test]
    fn test_quote_value_string() {
        assert_eq!(quote_value("hello"), "'hello'");
        assert_eq!(quote_value("it's"), "'it''s'");
    }

    #[test]
    fn test_count_pattern() {
        assert_eq!(count_pattern("foo bar foo baz foo", "foo"), 3);
        assert_eq!(count_pattern("no match here", "xyz"), 0);
    }

    #[test]
    fn test_truncate_string() {
        assert_eq!(truncate_string("short", 100), "short");
        let long = "a".repeat(5000);
        let truncated = truncate_string(&long, 100);
        assert!(truncated.len() < 120);
        assert!(truncated.ends_with("...[truncated]"));
    }

    #[test]
    fn test_generate_worker_sql_preamble() {
        let executor = Sqlite3Executor::with_defaults();
        let sql = executor.generate_worker_sql(&[]);
        assert!(sql.contains("PRAGMA journal_mode=wal"));
        assert!(sql.contains("PRAGMA synchronous=NORMAL"));
        assert!(sql.contains("PRAGMA busy_timeout=5000"));
        assert!(sql.contains(".bail on"));
    }

    #[test]
    fn test_generate_worker_sql_insert() {
        let executor = Sqlite3Executor::with_defaults();
        let rec = OpRecord {
            op_id: 0,
            worker: 0,
            kind: OpKind::Insert {
                table: "t1".to_owned(),
                key: 42,
                values: vec![
                    ("name".to_owned(), "test".to_owned()),
                    ("count".to_owned(), "7".to_owned()),
                ],
            },
            expected: None,
        };
        let sql = executor.generate_worker_sql(&[&rec]);
        assert!(sql.contains(r#"INSERT INTO "t1" (id, name, count) VALUES (42, 'test', 7);"#));
    }

    #[test]
    fn test_generate_worker_sql_update() {
        let executor = Sqlite3Executor::with_defaults();
        let rec = OpRecord {
            op_id: 0,
            worker: 0,
            kind: OpKind::Update {
                table: "hot".to_owned(),
                key: 5,
                values: vec![("counter".to_owned(), "99".to_owned())],
            },
            expected: None,
        };
        let sql = executor.generate_worker_sql(&[&rec]);
        assert!(sql.contains(r#"UPDATE "hot" SET "counter" = 99 WHERE id = 5;"#));
    }

    #[test]
    fn test_generate_worker_sql_begin_commit() {
        let executor = Sqlite3Executor::with_defaults();
        let ops = [
            OpRecord {
                op_id: 0,
                worker: 0,
                kind: OpKind::Begin,
                expected: None,
            },
            OpRecord {
                op_id: 1,
                worker: 0,
                kind: OpKind::Sql {
                    statement: "SELECT 1".to_owned(),
                },
                expected: None,
            },
            OpRecord {
                op_id: 2,
                worker: 0,
                kind: OpKind::Commit,
                expected: None,
            },
        ];
        let refs: Vec<&OpRecord> = ops.iter().collect();
        let sql = executor.generate_worker_sql(&refs);
        assert!(sql.contains("BEGIN;"));
        assert!(sql.contains("SELECT 1;"));
        assert!(sql.contains("COMMIT;"));
    }

    #[test]
    fn test_run_single_worker_serial() {
        // Skip if sqlite3 is not available.
        if std::process::Command::new("sqlite3")
            .arg("--version")
            .output()
            .is_err()
        {
            eprintln!("SKIP: sqlite3 not found on PATH");
            return;
        }

        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        let log = oplog::preset_commutative_inserts_disjoint_keys("test", 42, 1, 5);

        let executor = Sqlite3Executor::with_defaults();
        let report = executor.run(&log, &db_path).unwrap();

        assert!(report.success, "single-worker run should succeed");
        assert_eq!(report.worker_count, 1);
        assert_eq!(report.workers.len(), 1);
        assert_eq!(report.workers[0].exit_code, 0);
        assert_eq!(report.workers[0].busy_count, 0);

        // Verify the database has the expected rows.
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM t0", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 5, "should have 5 rows from 1 worker × 5 rows");
    }

    #[test]
    fn test_run_concurrent_workers() {
        if std::process::Command::new("sqlite3")
            .arg("--version")
            .output()
            .is_err()
        {
            eprintln!("SKIP: sqlite3 not found on PATH");
            return;
        }

        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("concurrent.db");

        // 8 workers with 10 rows each (disjoint keys — no conflict).
        let log = oplog::preset_commutative_inserts_disjoint_keys("test", 7, 8, 10);

        let executor = Sqlite3Executor::with_defaults();
        let report = executor.run(&log, &db_path).unwrap();

        assert!(
            report.success,
            "8-worker disjoint-key run should succeed: {:?}",
            report
                .workers
                .iter()
                .map(|w| (&w.stderr_snippet, w.exit_code))
                .collect::<Vec<_>>()
        );
        assert_eq!(report.worker_count, 8);
        assert_eq!(report.workers.len(), 8);

        // Verify total row count.
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM t0", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 80, "should have 80 rows from 8 workers × 10 rows");
    }

    #[test]
    fn test_run_hot_contention() {
        if std::process::Command::new("sqlite3")
            .arg("--version")
            .output()
            .is_err()
        {
            eprintln!("SKIP: sqlite3 not found on PATH");
            return;
        }

        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("contention.db");

        // 4 workers contending on 10 hot rows for 3 rounds.
        let log = oplog::preset_hot_page_contention("test", 42, 4, 3);

        let executor = Sqlite3Executor::with_defaults();
        let report = executor.run(&log, &db_path).unwrap();

        // With .bail on and busy_timeout=5000, the contention should
        // resolve within the timeout.
        assert!(
            report.success,
            "hot contention should succeed with busy_timeout: {:?}",
            report
                .workers
                .iter()
                .map(|w| (&w.stderr_snippet, w.exit_code))
                .collect::<Vec<_>>()
        );

        // Verify the hot table exists and has exactly 10 rows.
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM hot", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 10, "hot table should have exactly 10 rows");
    }

    #[test]
    fn test_report_serialization() {
        let report = RunReport {
            db_path: "/tmp/test.db".to_owned(),
            preset: Some("test_preset".to_owned()),
            worker_count: 2,
            workers: vec![
                WorkerReport {
                    worker_id: 0,
                    statement_count: 10,
                    exit_code: 0,
                    duration_ms: 50,
                    busy_count: 0,
                    error_count: 0,
                    stderr_snippet: String::new(),
                },
                WorkerReport {
                    worker_id: 1,
                    statement_count: 10,
                    exit_code: 0,
                    duration_ms: 55,
                    busy_count: 2,
                    error_count: 0,
                    stderr_snippet: "database is locked\ndatabase is locked\n".to_owned(),
                },
            ],
            total_duration_ms: 60,
            success: true,
        };

        let json = serde_json::to_string_pretty(&report).unwrap();
        let parsed: RunReport = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.worker_count, 2);
        assert_eq!(parsed.workers[1].busy_count, 2);
        assert!(parsed.success);
    }
}
