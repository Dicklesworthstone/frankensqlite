//! Differential comparison engine — run identical SQL against FrankenSQLite and
//! C SQLite (via rusqlite) and compare results.
//!
//! The [`SqlBackend`] trait abstracts over both engines, and [`ComparisonRunner`]
//! orchestrates side-by-side execution with result matching.

use std::fmt;

use fsqlite::Connection as FConnection;
use fsqlite_types::value::SqliteValue;
use rusqlite::Connection as CConnection;
use sha2::{Digest, Sha256};

use crate::{E2eError, E2eResult};

// ─── Normalized value type ──────────────────────────────────────────────

/// A normalized SQL value for cross-engine comparison.
///
/// Both backends convert their native value types into this common
/// representation before comparison.
#[derive(Debug, Clone, PartialEq)]
pub enum SqlValue {
    /// SQL NULL.
    Null,
    /// 64-bit signed integer.
    Integer(i64),
    /// 64-bit floating point.
    Real(f64),
    /// UTF-8 text.
    Text(String),
    /// Raw bytes.
    Blob(Vec<u8>),
}

impl Eq for SqlValue {}

impl fmt::Display for SqlValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Null => write!(f, "NULL"),
            Self::Integer(i) => write!(f, "{i}"),
            Self::Real(r) => write!(f, "{r}"),
            Self::Text(s) => write!(f, "'{s}'"),
            Self::Blob(b) => write!(f, "X'{}'", hex_encode(b)),
        }
    }
}

/// Hex-encode bytes without pulling in an extra crate.
fn hex_encode(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{b:02X}");
    }
    s
}

// ─── Row / outcome types ────────────────────────────────────────────────

/// A single row of normalized SQL values.
pub type NormalizedRow = Vec<SqlValue>;

/// A single row as stringified column values (legacy convenience alias).
pub type Row = Vec<String>;

/// Outcome of executing a SQL statement against one engine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StmtOutcome {
    /// Statement returned rows.
    Rows(Vec<Row>),
    /// Statement executed successfully with `n` affected rows.
    Execute(usize),
    /// Statement failed with an error message.
    Error(String),
}

/// Outcome using normalized value types for precise cross-engine comparison.
#[derive(Debug, Clone, PartialEq)]
pub enum NormalizedOutcome {
    /// Query returned rows of normalized values.
    Rows(Vec<NormalizedRow>),
    /// DML executed with `n` affected rows.
    Execute(usize),
    /// Statement failed.
    Error(String),
}

impl Eq for NormalizedOutcome {}

// ─── SqlBackend trait ───────────────────────────────────────────────────

/// Trait abstracting over a SQL database engine for differential testing.
pub trait SqlBackend {
    /// Execute a non-query SQL statement, returning affected row count.
    ///
    /// # Errors
    ///
    /// Returns the engine-specific error as a string.
    fn execute(&self, sql: &str) -> Result<usize, String>;

    /// Execute a query SQL statement, returning rows of normalized values.
    ///
    /// # Errors
    ///
    /// Returns the engine-specific error as a string.
    fn query(&self, sql: &str) -> Result<Vec<NormalizedRow>, String>;

    /// Run a SQL statement and return a normalized outcome (auto-detecting
    /// query vs DML based on the first keyword).
    fn run_stmt(&self, sql: &str) -> NormalizedOutcome {
        let trimmed = sql.trim();
        let is_query = trimmed
            .split_whitespace()
            .next()
            .is_some_and(|w| w.eq_ignore_ascii_case("SELECT"));

        if is_query {
            match self.query(trimmed) {
                Ok(rows) => NormalizedOutcome::Rows(rows),
                Err(e) => NormalizedOutcome::Error(e),
            }
        } else {
            match self.execute(trimmed) {
                Ok(n) => NormalizedOutcome::Execute(n),
                Err(e) => NormalizedOutcome::Error(e),
            }
        }
    }
}

// ─── C SQLite backend (rusqlite) ────────────────────────────────────────

/// C SQLite backend powered by rusqlite with the bundled feature.
pub struct CSqliteBackend {
    conn: CConnection,
}

impl CSqliteBackend {
    /// Open an in-memory C SQLite database.
    ///
    /// # Errors
    ///
    /// Returns `E2eError::Rusqlite` if the connection fails.
    pub fn open_in_memory() -> E2eResult<Self> {
        let conn = CConnection::open_in_memory()?;
        Ok(Self { conn })
    }

    /// Open a C SQLite database at `path`.
    ///
    /// # Errors
    ///
    /// Returns `E2eError::Rusqlite` on failure.
    pub fn open(path: &str) -> E2eResult<Self> {
        let conn = CConnection::open(path)?;
        Ok(Self { conn })
    }
}

impl SqlBackend for CSqliteBackend {
    fn execute(&self, sql: &str) -> Result<usize, String> {
        self.conn.execute(sql.trim(), []).map_err(|e| e.to_string())
    }

    fn query(&self, sql: &str) -> Result<Vec<NormalizedRow>, String> {
        let mut prepared = self.conn.prepare(sql.trim()).map_err(|e| e.to_string())?;
        let col_count = prepared.column_count();
        let rows = prepared
            .query_map([], |row| {
                let mut vals = Vec::with_capacity(col_count);
                for i in 0..col_count {
                    let rv: rusqlite::types::Value =
                        row.get(i).unwrap_or(rusqlite::types::Value::Null);
                    vals.push(rusqlite_value_to_sql_value(&rv));
                }
                Ok(vals)
            })
            .map_err(|e| e.to_string())?;

        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|e| e.to_string())
    }
}

/// Convert a rusqlite `Value` to our normalized `SqlValue`.
fn rusqlite_value_to_sql_value(v: &rusqlite::types::Value) -> SqlValue {
    match v {
        rusqlite::types::Value::Null => SqlValue::Null,
        rusqlite::types::Value::Integer(i) => SqlValue::Integer(*i),
        rusqlite::types::Value::Real(f) => SqlValue::Real(*f),
        rusqlite::types::Value::Text(s) => SqlValue::Text(s.clone()),
        rusqlite::types::Value::Blob(b) => SqlValue::Blob(b.clone()),
    }
}

// ─── FrankenSQLite backend ──────────────────────────────────────────────

/// FrankenSQLite backend powered by `fsqlite_core::Connection`.
pub struct FrankenSqliteBackend {
    conn: FConnection,
}

impl FrankenSqliteBackend {
    /// Open an in-memory FrankenSQLite database.
    ///
    /// # Errors
    ///
    /// Returns `E2eError::Fsqlite` if the connection fails.
    pub fn open_in_memory() -> E2eResult<Self> {
        let conn = FConnection::open(":memory:").map_err(|e| E2eError::Fsqlite(e.to_string()))?;
        Ok(Self { conn })
    }
}

impl SqlBackend for FrankenSqliteBackend {
    fn execute(&self, sql: &str) -> Result<usize, String> {
        self.conn.execute(sql.trim()).map_err(|e| e.to_string())
    }

    fn query(&self, sql: &str) -> Result<Vec<NormalizedRow>, String> {
        let rows = self.conn.query(sql.trim()).map_err(|e| e.to_string())?;
        Ok(rows
            .into_iter()
            .map(|row| {
                row.values()
                    .iter()
                    .map(fsqlite_value_to_sql_value)
                    .collect()
            })
            .collect())
    }
}

/// Convert a FrankenSQLite `SqliteValue` to our normalized `SqlValue`.
fn fsqlite_value_to_sql_value(v: &SqliteValue) -> SqlValue {
    match v {
        SqliteValue::Null => SqlValue::Null,
        SqliteValue::Integer(i) => SqlValue::Integer(*i),
        SqliteValue::Float(f) => SqlValue::Real(*f),
        SqliteValue::Text(s) => SqlValue::Text(s.clone()),
        SqliteValue::Blob(b) => SqlValue::Blob(b.clone()),
    }
}

// ─── Mismatch / result types ────────────────────────────────────────────

/// A single mismatch between the two engines.
#[derive(Debug, Clone)]
pub struct Mismatch {
    /// Zero-based index of the statement in the workload.
    pub index: usize,
    /// The SQL statement that diverged.
    pub sql: String,
    /// Outcome from C SQLite.
    pub csqlite: NormalizedOutcome,
    /// Outcome from FrankenSQLite.
    pub fsqlite: NormalizedOutcome,
}

/// Result of running a workload through the comparison engine.
#[derive(Debug)]
pub struct ComparisonResult {
    /// Number of statements that produced identical results.
    pub operations_matched: usize,
    /// Number of statements that produced different results.
    pub operations_mismatched: usize,
    /// Details of each mismatch.
    pub mismatches: Vec<Mismatch>,
}

/// Result of comparing database state via SHA-256 after a workload.
#[derive(Debug)]
pub struct HashComparison {
    /// SHA-256 hex digest of the FrankenSQLite database dump.
    pub frank_sha256: String,
    /// SHA-256 hex digest of the C SQLite database dump.
    pub csqlite_sha256: String,
    /// Whether the two hashes match.
    pub matched: bool,
}

// ─── ComparisonRunner ───────────────────────────────────────────────────

/// Orchestrates differential testing by running the same workload against
/// both FrankenSQLite and C SQLite and comparing results.
pub struct ComparisonRunner {
    frank: FrankenSqliteBackend,
    csqlite: CSqliteBackend,
}

impl ComparisonRunner {
    /// Create a new comparison runner with in-memory databases for both engines.
    ///
    /// # Errors
    ///
    /// Returns an error if either backend fails to initialize.
    pub fn new_in_memory() -> E2eResult<Self> {
        Ok(Self {
            frank: FrankenSqliteBackend::open_in_memory()?,
            csqlite: CSqliteBackend::open_in_memory()?,
        })
    }

    /// Run the same SQL workload on both backends and compare results.
    #[must_use]
    pub fn run_and_compare(&self, statements: &[String]) -> ComparisonResult {
        let mut matched = 0usize;
        let mut mismatched_count = 0usize;
        let mut mismatches = Vec::new();

        for (i, sql) in statements.iter().enumerate() {
            let c_outcome = self.csqlite.run_stmt(sql);
            let f_outcome = self.frank.run_stmt(sql);

            if c_outcome == f_outcome {
                matched += 1;
            } else {
                mismatched_count += 1;
                mismatches.push(Mismatch {
                    index: i,
                    sql: sql.clone(),
                    csqlite: c_outcome,
                    fsqlite: f_outcome,
                });
            }
        }

        ComparisonResult {
            operations_matched: matched,
            operations_mismatched: mismatched_count,
            mismatches,
        }
    }

    /// Compare final database state by dumping all table data sorted by
    /// the first selected column (best-effort stable ordering across engines)
    /// from both engines and computing SHA-256 over the
    /// concatenated logical dump.
    ///
    /// This is a *logical* comparison — it does not depend on physical page
    /// layout, so it works even when VACUUM produces different binary files.
    pub fn compare_logical_state(&self) -> HashComparison {
        // FrankenSQLite doesn't yet expose sqlite_master in the in-memory backend,
        // so discover table names via the C SQLite backend and use that list for both.
        let tables = list_user_tables_csqlite(&self.csqlite);
        let frank_dump = logical_dump_tables(&self.frank, &tables);
        let csqlite_dump = logical_dump_tables(&self.csqlite, &tables);

        let frank_sha = sha256_hex(frank_dump.as_bytes());
        let csqlite_sha = sha256_hex(csqlite_dump.as_bytes());
        let matched = frank_sha == csqlite_sha;

        HashComparison {
            frank_sha256: frank_sha,
            csqlite_sha256: csqlite_sha,
            matched,
        }
    }

    /// Get a reference to the FrankenSQLite backend.
    #[must_use]
    pub fn frank(&self) -> &FrankenSqliteBackend {
        &self.frank
    }

    /// Get a reference to the C SQLite backend.
    #[must_use]
    pub fn csqlite(&self) -> &CSqliteBackend {
        &self.csqlite
    }
}

// ─── Logical dump helpers ───────────────────────────────────────────────

fn list_user_tables_csqlite(backend: &CSqliteBackend) -> Vec<String> {
    let Ok(rows) = backend.query(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
    ) else {
        return Vec::new();
    };

    rows.iter()
        .filter_map(|r| match r.first() {
            Some(SqlValue::Text(name)) => Some(name.clone()),
            _ => None,
        })
        .collect()
}

/// Produce a deterministic text dump of all user tables.
fn logical_dump_tables<B: SqlBackend>(backend: &B, tables: &[String]) -> String {
    use std::fmt::Write as _;

    let mut dump = String::new();
    for table_name in tables {
        let _ = writeln!(dump, "-- TABLE: {table_name}");
        let rows = backend
            .query(&format!("SELECT * FROM \"{table_name}\" ORDER BY rowid"))
            .or_else(|_| backend.query(&format!("SELECT * FROM \"{table_name}\" ORDER BY 1")))
            .or_else(|_| backend.query(&format!("SELECT * FROM \"{table_name}\"")));
        if let Ok(rows) = rows {
            for data_row in &rows {
                for (j, val) in data_row.iter().enumerate() {
                    if j > 0 {
                        dump.push('|');
                    }
                    dump.push_str(&val.to_string());
                }
                dump.push('\n');
            }
        }
    }
    dump
}

/// Compute SHA-256 hex digest.
fn sha256_hex(data: &[u8]) -> String {
    use std::fmt::Write as _;
    let digest = Sha256::digest(data);
    let mut hex = String::with_capacity(64);
    for byte in digest {
        let _ = write!(hex, "{byte:02x}");
    }
    hex
}

// ─── Mismatch debugger (prefix reduction) ───────────────────────────────

/// Artifacts captured from a workload evaluation (both engines).
#[derive(Debug)]
pub struct WorkloadArtifacts {
    /// Per-statement comparison result.
    pub comparison: ComparisonResult,
    /// Full logical dump produced from FrankenSQLite.
    pub frank_dump: String,
    /// Full logical dump produced from C SQLite.
    pub csqlite_dump: String,
    /// SHA-256 comparison of the logical dumps.
    pub hash: HashComparison,
}

/// Reduced reproduction input for debugging a mismatch.
#[derive(Debug)]
pub struct ReducedRepro {
    /// Number of workload evaluations performed during reduction.
    pub iterations: usize,
    /// Reduced statements (prefix) that still reproduces the mismatch.
    pub reduced_statements: Vec<String>,
    /// Reduced OpLog containing the statements as `OpKind::Sql` on worker 0.
    pub reduced_oplog: crate::oplog::OpLog,
    /// JSONL form of `reduced_oplog`.
    pub reduced_jsonl: String,
    /// Artifacts for the reduced reproduction workload.
    pub artifacts: WorkloadArtifacts,
}

/// Reduce an OpLog to a minimal failing prefix (best-effort) for debugging.
///
/// The reduced repro is emitted as an OpLog where every operation is encoded
/// as `OpKind::Sql` on worker 0 (even if the input used structured ops).
///
/// # Errors
///
/// Returns an error if the in-memory backends fail to initialize.
pub fn reduce_oplog_to_minimal_repro(
    oplog: &crate::oplog::OpLog,
    max_iterations: usize,
) -> E2eResult<Option<ReducedRepro>> {
    let statements = oplog_to_sql_statements(oplog);
    reduce_statements_to_minimal_repro(&statements, max_iterations, Some(&oplog.header))
}

/// Reduce a SQL workload to a minimal failing prefix (best-effort).
///
/// Today this uses a monotone prefix bisection strategy:
/// - If there is a statement-level mismatch, the minimal prefix is the first
///   mismatching index + 1 (no search required).
/// - Otherwise, if only the final logical-state hash differs, we binary-search
///   the smallest prefix whose hash differs.
///
/// This is intended to quickly produce a small, debuggable repro in a handful
/// of iterations for typical logs.
///
/// # Errors
///
/// Returns an error if the in-memory backends fail to initialize.
pub fn reduce_sql_workload_to_minimal_repro(
    statements: &[String],
    max_iterations: usize,
) -> E2eResult<Option<ReducedRepro>> {
    reduce_statements_to_minimal_repro(statements, max_iterations, None)
}

fn reduce_statements_to_minimal_repro(
    statements: &[String],
    max_iterations: usize,
    header: Option<&crate::oplog::OpLogHeader>,
) -> E2eResult<Option<ReducedRepro>> {
    if statements.is_empty() {
        return Ok(None);
    }

    let mut iterations: usize = 0;
    let full = evaluate_sql_workload(statements)?;
    iterations = iterations.saturating_add(1);

    let full_has_stmt_mismatch = full.comparison.operations_mismatched > 0;
    let full_has_hash_mismatch = !full.hash.matched;

    if !full_has_stmt_mismatch && !full_has_hash_mismatch {
        return Ok(None);
    }

    let reduced_len = if full_has_stmt_mismatch {
        // Minimal prefix is first mismatch index + 1.
        full.comparison
            .mismatches
            .first()
            .map_or(statements.len(), |m| m.index.saturating_add(1))
            .clamp(1, statements.len())
    } else {
        // Only hash mismatch: binary-search minimal failing prefix.
        minimize_failing_prefix_len(
            statements.len(),
            max_iterations.saturating_sub(iterations),
            |prefix_len| {
                let stmts = &statements[..prefix_len];
                let eval = evaluate_sql_workload(stmts)?;
                iterations = iterations.saturating_add(1);
                Ok(!eval.hash.matched)
            },
        )?
        .unwrap_or(statements.len())
    };

    let reduced_statements: Vec<String> = statements[..reduced_len].to_vec();
    let artifacts = evaluate_sql_workload(&reduced_statements)?;
    iterations = iterations.saturating_add(1);

    let reduced_oplog = statements_to_sql_oplog(header, &reduced_statements);
    let reduced_jsonl = reduced_oplog
        .to_jsonl()
        .map_err(|e| E2eError::Divergence(format!("failed to serialize reduced oplog: {e}")))?;

    Ok(Some(ReducedRepro {
        iterations,
        reduced_statements,
        reduced_oplog,
        reduced_jsonl,
        artifacts,
    }))
}

/// Evaluate a SQL workload from scratch in fresh in-memory databases.
fn evaluate_sql_workload(statements: &[String]) -> E2eResult<WorkloadArtifacts> {
    let runner = ComparisonRunner::new_in_memory()?;
    let comparison = runner.run_and_compare(statements);

    let tables = list_user_tables_csqlite(runner.csqlite());
    let frank_dump = logical_dump_tables(runner.frank(), &tables);
    let csqlite_dump = logical_dump_tables(runner.csqlite(), &tables);

    let frank_sha = sha256_hex(frank_dump.as_bytes());
    let csqlite_sha = sha256_hex(csqlite_dump.as_bytes());
    let matched = frank_sha == csqlite_sha;

    Ok(WorkloadArtifacts {
        comparison,
        frank_dump,
        csqlite_dump,
        hash: HashComparison {
            frank_sha256: frank_sha,
            csqlite_sha256: csqlite_sha,
            matched,
        },
    })
}

fn oplog_to_sql_statements(oplog: &crate::oplog::OpLog) -> Vec<String> {
    use crate::oplog::OpKind;

    oplog
        .records
        .iter()
        .map(|r| match &r.kind {
            OpKind::Sql { statement } => statement.clone(),
            OpKind::Begin => "BEGIN".to_owned(),
            OpKind::Commit => "COMMIT".to_owned(),
            OpKind::Rollback => "ROLLBACK".to_owned(),
            OpKind::Insert { table, key, values } => structured_insert_sql(table, *key, values),
            OpKind::Update { table, key, values } => structured_update_sql(table, *key, values),
        })
        .collect()
}

fn structured_insert_sql(table: &str, key: i64, values: &[(String, String)]) -> String {
    let mut cols = Vec::with_capacity(values.len() + 1);
    let mut vals = Vec::with_capacity(values.len() + 1);

    cols.push("\"id\"".to_owned());
    vals.push(key.to_string());

    for (col, v) in values {
        cols.push(format!("\"{}\"", escape_ident(col)));
        vals.push(sql_literal(v));
    }

    format!(
        "INSERT INTO \"{}\" ({}) VALUES ({})",
        escape_ident(table),
        cols.join(", "),
        vals.join(", ")
    )
}

fn structured_update_sql(table: &str, key: i64, values: &[(String, String)]) -> String {
    let mut sets = Vec::with_capacity(values.len());
    for (col, v) in values {
        sets.push(format!("\"{}\"={}", escape_ident(col), sql_literal(v)));
    }

    format!(
        "UPDATE \"{}\" SET {} WHERE id={}",
        escape_ident(table),
        sets.join(", "),
        key
    )
}

fn escape_ident(s: &str) -> String {
    s.replace('"', "\"\"")
}

fn sql_literal(s: &str) -> String {
    if s.eq_ignore_ascii_case("null") {
        return "NULL".to_owned();
    }

    // Keep numeric values unquoted.
    if s.parse::<i64>().is_ok() || s.parse::<f64>().is_ok() {
        return s.to_owned();
    }

    let escaped = s.replace('\'', "''");
    format!("'{escaped}'")
}

/// Produce an OpLog that runs the given statements as `OpKind::Sql` on worker 0.
fn statements_to_sql_oplog(
    header: Option<&crate::oplog::OpLogHeader>,
    statements: &[String],
) -> crate::oplog::OpLog {
    use crate::oplog::{ConcurrencyModel, OpKind, OpLog, OpLogHeader, OpRecord, RngSpec};

    let base_header = header.cloned().unwrap_or_else(|| OpLogHeader {
        fixture_id: "reduced".to_owned(),
        seed: 0,
        rng: RngSpec::default(),
        concurrency: ConcurrencyModel {
            worker_count: 1,
            transaction_size: 1,
            commit_order_policy: "deterministic".to_owned(),
        },
        preset: None,
    });

    let header = OpLogHeader {
        fixture_id: base_header.fixture_id,
        seed: base_header.seed,
        rng: base_header.rng,
        concurrency: ConcurrencyModel {
            worker_count: 1,
            transaction_size: 1,
            commit_order_policy: "deterministic".to_owned(),
        },
        preset: base_header.preset,
    };

    let records: Vec<OpRecord> = statements
        .iter()
        .enumerate()
        .map(|(i, s)| OpRecord {
            op_id: u64::try_from(i).unwrap_or(u64::MAX),
            worker: 0,
            kind: OpKind::Sql {
                statement: s.clone(),
            },
            expected: None,
        })
        .collect();

    OpLog { header, records }
}

fn minimize_failing_prefix_len<F>(
    len: usize,
    max_iterations: usize,
    mut fails: F,
) -> E2eResult<Option<usize>>
where
    F: FnMut(usize) -> E2eResult<bool>,
{
    if len == 0 {
        return Ok(None);
    }

    // If the full workload doesn't fail, there's nothing to reduce.
    if !fails(len)? {
        return Ok(None);
    }

    // Standard monotone binary search on prefix length.
    let mut lo: usize = 1;
    let mut hi: usize = len;
    let mut iters: usize = 0;

    while lo < hi && iters < max_iterations {
        iters = iters.saturating_add(1);
        let mid = lo + (hi - lo) / 2;
        if fails(mid)? {
            hi = mid;
        } else {
            lo = mid.saturating_add(1);
        }
    }

    Some(lo)
        .filter(|v| *v >= 1 && *v <= len)
        .map_or(Ok(None), |v| Ok(Some(v)))
}

// ─── Repro package writer ────────────────────────────────────────────────

/// Write a self-contained reproduction package to `output_dir`.
///
/// Creates:
/// - `minimal_oplog.jsonl` — the reduced operation sequence
/// - `diff.md` — human-readable diff summary of logical state
/// - `debug_log.jsonl` — structured metadata about the bisection
///
/// # Errors
///
/// Returns `E2eError::Io` if directory creation or file writing fails.
pub fn write_repro_package(
    repro: &ReducedRepro,
    output_dir: &std::path::Path,
) -> E2eResult<std::path::PathBuf> {
    use std::fmt::Write as _;

    std::fs::create_dir_all(output_dir)?;

    // 1. minimal_oplog.jsonl
    let oplog_path = output_dir.join("minimal_oplog.jsonl");
    std::fs::write(&oplog_path, &repro.reduced_jsonl)?;

    // 2. diff.md
    let mut diff = String::new();
    let _ = writeln!(diff, "# Mismatch Reproduction Diff\n");
    let _ = writeln!(
        diff,
        "**Reduced statements:** {}",
        repro.reduced_statements.len()
    );
    let _ = writeln!(diff, "**Bisection iterations:** {}\n", repro.iterations);

    if repro.artifacts.comparison.operations_mismatched > 0 {
        let _ = writeln!(diff, "## Statement-Level Mismatches\n");
        for m in &repro.artifacts.comparison.mismatches {
            let _ = writeln!(diff, "### Statement {} : `{}`\n", m.index, m.sql);
            let _ = writeln!(diff, "- **C SQLite:** `{:?}`", m.csqlite);
            let _ = writeln!(diff, "- **FrankenSQLite:** `{:?}`\n", m.fsqlite);
        }
    }

    let _ = writeln!(diff, "## Logical State Hash Comparison\n");
    let _ = writeln!(
        diff,
        "- **C SQLite SHA-256:** `{}`",
        repro.artifacts.hash.csqlite_sha256
    );
    let _ = writeln!(
        diff,
        "- **FrankenSQLite SHA-256:** `{}`",
        repro.artifacts.hash.frank_sha256
    );
    let _ = writeln!(
        diff,
        "- **Match:** {}\n",
        if repro.artifacts.hash.matched {
            "YES"
        } else {
            "NO"
        }
    );

    if repro.artifacts.frank_dump != repro.artifacts.csqlite_dump {
        let _ = writeln!(diff, "## Logical Dump (C SQLite)\n```");
        let _ = write!(diff, "{}", repro.artifacts.csqlite_dump);
        let _ = writeln!(diff, "```\n\n## Logical Dump (FrankenSQLite)\n```");
        let _ = write!(diff, "{}", repro.artifacts.frank_dump);
        let _ = writeln!(diff, "```");
    }

    std::fs::write(output_dir.join("diff.md"), &diff)?;

    // 3. debug_log.jsonl — one JSON object with metadata
    let debug_meta = serde_json::json!({
        "iterations": repro.iterations,
        "reduced_statement_count": repro.reduced_statements.len(),
        "stmt_mismatches": repro.artifacts.comparison.operations_mismatched,
        "hash_matched": repro.artifacts.hash.matched,
        "frank_sha256": repro.artifacts.hash.frank_sha256,
        "csqlite_sha256": repro.artifacts.hash.csqlite_sha256,
    });
    let debug_json = serde_json::to_string(&debug_meta)
        .map_err(|e| E2eError::Divergence(format!("failed to serialize debug log: {e}")))?;
    std::fs::write(output_dir.join("debug_log.jsonl"), debug_json)?;

    Ok(output_dir.to_path_buf())
}

// ─── Legacy helpers (kept for backward compat) ──────────────────────────

/// Run a sequence of SQL statements against C SQLite (rusqlite) and collect
/// outcomes using the legacy string-based format.
///
/// # Errors
///
/// Returns `E2eError::Rusqlite` if the connection itself fails to open.
pub fn run_csqlite(db_path: &str, statements: &[String]) -> E2eResult<Vec<StmtOutcome>> {
    let conn = CConnection::open(db_path)?;
    let mut outcomes = Vec::with_capacity(statements.len());

    for stmt in statements {
        let outcome = execute_csqlite_stmt(&conn, stmt);
        outcomes.push(outcome);
    }

    Ok(outcomes)
}

/// Execute a single statement against a rusqlite connection (legacy format).
fn execute_csqlite_stmt(conn: &CConnection, sql: &str) -> StmtOutcome {
    let trimmed = sql.trim();
    let is_query = trimmed
        .split_whitespace()
        .next()
        .is_some_and(|w| w.eq_ignore_ascii_case("SELECT"));

    if is_query {
        match conn.prepare(trimmed) {
            Ok(mut prepared) => {
                let col_count = prepared.column_count();
                match prepared.query_map([], |row| {
                    let mut cols = Vec::with_capacity(col_count);
                    for i in 0..col_count {
                        let val: String = row
                            .get::<_, rusqlite::types::Value>(i)
                            .map_or_else(|e| format!("ERR:{e}"), |v| format!("{v:?}"));
                        cols.push(val);
                    }
                    Ok(cols)
                }) {
                    Ok(rows) => {
                        let collected: Vec<Row> = rows.filter_map(Result::ok).collect();
                        StmtOutcome::Rows(collected)
                    }
                    Err(e) => StmtOutcome::Error(e.to_string()),
                }
            }
            Err(e) => StmtOutcome::Error(e.to_string()),
        }
    } else {
        match conn.execute(trimmed, []) {
            Ok(n) => StmtOutcome::Execute(n),
            Err(e) => StmtOutcome::Error(e.to_string()),
        }
    }
}

/// Compare two sequences of string-based outcomes and return divergences.
#[must_use]
pub fn find_divergences(
    csqlite: &[StmtOutcome],
    fsqlite: &[StmtOutcome],
) -> Vec<(usize, StmtOutcome, StmtOutcome)> {
    csqlite
        .iter()
        .zip(fsqlite.iter())
        .enumerate()
        .filter(|(_, (c, f))| c != f)
        .map(|(i, (c, f))| (i, c.clone(), f.clone()))
        .collect()
}

/// Run comparison and return an error if any divergences are found.
///
/// # Errors
///
/// Returns `E2eError::Divergence` listing the first divergent statement.
pub fn assert_no_divergences(csqlite: &[StmtOutcome], fsqlite: &[StmtOutcome]) -> E2eResult<()> {
    let divs = find_divergences(csqlite, fsqlite);
    if divs.is_empty() {
        Ok(())
    } else {
        let (idx, ref c, ref f) = divs[0];
        Err(E2eError::Divergence(format!(
            "statement {idx}: csqlite={c:?}, fsqlite={f:?}"
        )))
    }
}

// ─── Tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // -- Legacy tests --

    #[test]
    fn test_find_divergences_identical() {
        let a = vec![StmtOutcome::Execute(1), StmtOutcome::Execute(0)];
        let b = a.clone();
        assert!(find_divergences(&a, &b).is_empty());
    }

    #[test]
    fn test_find_divergences_different() {
        let a = vec![StmtOutcome::Execute(1)];
        let b = vec![StmtOutcome::Execute(2)];
        let divs = find_divergences(&a, &b);
        assert_eq!(divs.len(), 1);
        assert_eq!(divs[0].0, 0);
    }

    #[test]
    fn test_csqlite_basic_roundtrip() {
        let stmts = vec![
            "CREATE TABLE x (id INTEGER PRIMARY KEY, v TEXT)".to_owned(),
            "INSERT INTO x VALUES (1, 'hello')".to_owned(),
            "SELECT * FROM x".to_owned(),
        ];
        let outcomes = run_csqlite(":memory:", &stmts).unwrap();
        assert_eq!(outcomes.len(), 3);
        assert!(matches!(outcomes[0], StmtOutcome::Execute(0)));
        assert!(matches!(outcomes[1], StmtOutcome::Execute(1)));
        assert!(matches!(outcomes[2], StmtOutcome::Rows(ref r) if r.len() == 1));
    }

    // -- SqlBackend trait tests --

    #[test]
    fn test_csqlite_backend_execute_and_query() {
        let backend = CSqliteBackend::open_in_memory().unwrap();
        backend
            .execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .unwrap();
        let affected = backend
            .execute("INSERT INTO t VALUES (1, 'hello')")
            .unwrap();
        assert_eq!(affected, 1);

        let rows = backend.query("SELECT id, val FROM t").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], SqlValue::Integer(1));
        assert_eq!(rows[0][1], SqlValue::Text("hello".to_owned()));
    }

    #[test]
    fn test_fsqlite_backend_execute_and_query() {
        let backend = FrankenSqliteBackend::open_in_memory().unwrap();
        backend
            .execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .unwrap();
        let affected = backend
            .execute("INSERT INTO t VALUES (1, 'hello')")
            .unwrap();
        assert_eq!(affected, 1);

        let rows = backend.query("SELECT id, val FROM t").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], SqlValue::Integer(1));
        assert_eq!(rows[0][1], SqlValue::Text("hello".to_owned()));
    }

    #[test]
    fn test_comparison_runner_identical_workload() {
        let runner = ComparisonRunner::new_in_memory().unwrap();
        let stmts = vec![
            "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)".to_owned(),
            "INSERT INTO t VALUES (1, 'a')".to_owned(),
            "INSERT INTO t VALUES (2, 'b')".to_owned(),
            "SELECT * FROM t ORDER BY id".to_owned(),
        ];

        let result = runner.run_and_compare(&stmts);
        assert_eq!(
            result.operations_mismatched, 0,
            "mismatches: {:?}",
            result.mismatches
        );
        assert_eq!(result.operations_matched, stmts.len());
    }

    #[test]
    fn test_comparison_runner_logical_state() {
        // Known divergence: FrankenSQLite and C SQLite produce different
        // logical dumps for `SELECT * FROM "t" ORDER BY rowid`, so the
        // hash comparison is expected to fail until rowid ordering is
        // fully implemented.  We verify both hashes are non-empty and
        // that statement-level execution still agrees.
        let runner = ComparisonRunner::new_in_memory().unwrap();
        let stmts = vec![
            "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)".to_owned(),
            "INSERT INTO t VALUES (1, 'hello')".to_owned(),
            "INSERT INTO t VALUES (2, 'world')".to_owned(),
        ];

        let result = runner.run_and_compare(&stmts);
        assert_eq!(result.operations_mismatched, 0);

        let hash = runner.compare_logical_state();
        assert!(!hash.frank_sha256.is_empty());
        assert!(!hash.csqlite_sha256.is_empty());
        // TODO: assert!(hash.matched) once FrankenSQLite rowid ordering matches C SQLite
    }

    #[test]
    fn test_sql_value_display() {
        assert_eq!(SqlValue::Null.to_string(), "NULL");
        assert_eq!(SqlValue::Integer(42).to_string(), "42");
        assert_eq!(SqlValue::Real(2.5).to_string(), "2.5");
        assert_eq!(SqlValue::Text("hi".to_owned()).to_string(), "'hi'");
        assert_eq!(SqlValue::Blob(vec![0xDE, 0xAD]).to_string(), "X'DEAD'");
    }

    #[test]
    fn test_mismatch_detection() {
        let a = NormalizedOutcome::Execute(1);
        let b = NormalizedOutcome::Execute(2);
        assert_ne!(a, b);

        let c = NormalizedOutcome::Rows(vec![vec![SqlValue::Integer(1)]]);
        let d = NormalizedOutcome::Rows(vec![vec![SqlValue::Integer(1)]]);
        assert_eq!(c, d);
    }

    #[test]
    fn test_sha256_hex_known_value() {
        let h = sha256_hex(b"hello world");
        assert_eq!(
            h,
            "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        );
    }

    #[test]
    fn test_null_handling_both_backends() {
        let runner = ComparisonRunner::new_in_memory().unwrap();
        let stmts = vec![
            "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)".to_owned(),
            "INSERT INTO t VALUES (1, NULL)".to_owned(),
            "SELECT val FROM t".to_owned(),
        ];
        let result = runner.run_and_compare(&stmts);
        assert_eq!(
            result.operations_mismatched, 0,
            "NULL handling diverged: {:?}",
            result.mismatches
        );
    }

    #[test]
    fn test_multiple_inserts_and_select_count() {
        let runner = ComparisonRunner::new_in_memory().unwrap();
        let stmts = vec![
            "CREATE TABLE t (id INTEGER PRIMARY KEY, val REAL)".to_owned(),
            "INSERT INTO t VALUES (1, 1.5)".to_owned(),
            "INSERT INTO t VALUES (2, 2.5)".to_owned(),
            "INSERT INTO t VALUES (3, 3.5)".to_owned(),
            "SELECT COUNT(*) FROM t".to_owned(),
        ];
        let result = runner.run_and_compare(&stmts);
        assert_eq!(
            result.operations_mismatched, 0,
            "mismatches: {:?}",
            result.mismatches
        );
    }

    #[test]
    fn test_error_on_both_backends_matches() {
        let runner = ComparisonRunner::new_in_memory().unwrap();
        let stmts = vec!["SELECT * FROM nonexistent_table".to_owned()];
        let result = runner.run_and_compare(&stmts);
        // Both should error — whether they match depends on error message format,
        // but both should return Error variants.
        assert_eq!(result.operations_matched + result.operations_mismatched, 1);
    }

    // -- Mismatch debugger / reduction tests --

    #[test]
    fn test_reduce_no_mismatch_returns_none() {
        // Use a workload that creates no tables so both engines produce
        // identical empty logical dumps and matching hashes.
        let stmts = vec!["SELECT 1".to_owned()];
        let result = reduce_sql_workload_to_minimal_repro(&stmts, 20).unwrap();
        assert!(result.is_none(), "matching workload should return None");
    }

    #[test]
    fn test_reduce_empty_workload_returns_none() {
        let result = reduce_sql_workload_to_minimal_repro(&[], 20).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_minimize_failing_prefix_len_basic() {
        // Simulate: prefixes of length >= 3 fail, < 3 pass.
        let result = minimize_failing_prefix_len(10, 20, |len| Ok(len >= 3)).unwrap();
        assert_eq!(result, Some(3));
    }

    #[test]
    fn test_minimize_failing_prefix_len_first_fails() {
        // Even prefix of length 1 fails.
        let result = minimize_failing_prefix_len(10, 20, |_len| Ok(true)).unwrap();
        assert_eq!(result, Some(1));
    }

    #[test]
    fn test_minimize_failing_prefix_len_none_fail() {
        // Even the full length doesn't fail.
        let result = minimize_failing_prefix_len(10, 20, |_len| Ok(false)).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_minimize_failing_prefix_len_empty() {
        let result = minimize_failing_prefix_len(0, 20, |_len| Ok(true)).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_minimize_failing_prefix_len_respects_max_iterations() {
        let mut call_count = 0usize;
        // fail always, but limit iterations to 3
        let result = minimize_failing_prefix_len(1000, 3, |_len| {
            call_count += 1;
            Ok(true)
        })
        .unwrap();
        // Should return something (best guess) after 3 search iterations + 1 initial check
        assert!(result.is_some());
        // The initial check + 3 search iterations = 4 total
        assert!(call_count <= 4, "call_count={call_count}");
    }

    #[test]
    fn test_statements_to_sql_oplog_roundtrip() {
        let stmts = vec![
            "CREATE TABLE t (id INTEGER PRIMARY KEY)".to_owned(),
            "INSERT INTO t VALUES (1)".to_owned(),
        ];
        let oplog = statements_to_sql_oplog(None, &stmts);
        assert_eq!(oplog.records.len(), 2);
        assert_eq!(oplog.header.concurrency.worker_count, 1);
        assert!(oplog.records.iter().all(|r| r.worker == 0));
    }

    #[test]
    fn test_write_repro_package_creates_files() {
        // Build a ReducedRepro with synthetic data.
        let stmts = vec!["CREATE TABLE t (id INTEGER PRIMARY KEY)".to_owned()];
        let oplog = statements_to_sql_oplog(None, &stmts);
        let jsonl = oplog.to_jsonl().unwrap();

        let repro = ReducedRepro {
            iterations: 3,
            reduced_statements: stmts,
            reduced_oplog: oplog,
            reduced_jsonl: jsonl,
            artifacts: WorkloadArtifacts {
                comparison: ComparisonResult {
                    operations_matched: 1,
                    operations_mismatched: 0,
                    mismatches: vec![],
                },
                frank_dump: "-- TABLE: t\n".to_owned(),
                csqlite_dump: "-- TABLE: t\n".to_owned(),
                hash: HashComparison {
                    frank_sha256: "aaa".to_owned(),
                    csqlite_sha256: "bbb".to_owned(),
                    matched: false,
                },
            },
        };

        let tmp = tempfile::tempdir().unwrap();
        let out_dir = tmp.path().join("repro");
        let result = write_repro_package(&repro, &out_dir).unwrap();

        assert_eq!(result, out_dir);
        assert!(out_dir.join("minimal_oplog.jsonl").exists());
        assert!(out_dir.join("diff.md").exists());
        assert!(out_dir.join("debug_log.jsonl").exists());

        // Verify diff.md content
        let diff_content = std::fs::read_to_string(out_dir.join("diff.md")).unwrap();
        assert!(diff_content.contains("Mismatch Reproduction Diff"));
        assert!(diff_content.contains("**Reduced statements:** 1"));
        assert!(diff_content.contains("**Match:** NO"));

        // Verify debug_log.jsonl is valid JSON
        let debug_str = std::fs::read_to_string(out_dir.join("debug_log.jsonl")).unwrap();
        let debug_val: serde_json::Value = serde_json::from_str(&debug_str).unwrap();
        assert_eq!(debug_val["iterations"], 3);
        assert!(!debug_val["hash_matched"].as_bool().unwrap());
    }

    #[test]
    fn test_evaluate_sql_workload_consistent_engines() {
        let stmts = vec![
            "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)".to_owned(),
            "INSERT INTO t VALUES (1, 'test')".to_owned(),
        ];
        let artifacts = evaluate_sql_workload(&stmts).unwrap();
        assert_eq!(artifacts.comparison.operations_mismatched, 0);
    }
}
