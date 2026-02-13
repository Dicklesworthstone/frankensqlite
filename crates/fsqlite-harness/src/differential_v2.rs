//! Oracle Differential Harness V2 — reproducible execution envelopes (bd-1dp9.1.2).
//!
//! This module defines the **execution envelope** format: a self-describing,
//! deterministic specification of a differential test run.  Given identical
//! envelope contents, any conformant executor MUST produce identical normalized
//! artifact IDs.
//!
//! # Architecture
//!
//! ```text
//! ExecutionEnvelope → (fsqlite, csqlite) → DifferentialResult → ArtifactBundle
//! ```
//!
//! The envelope captures:
//! - Query/input seeds for RNG reproducibility
//! - Engine version strings
//! - PRAGMA configuration
//! - Schema setup SQL
//! - Workload SQL statements
//! - Output canonicalization rules
//!
//! The artifact ID is the SHA-256 of the envelope's canonical JSON representation,
//! guaranteeing that the same logical input always maps to the same identifier
//! regardless of serialization whitespace or field ordering differences.

use std::fmt;
use std::fmt::Write as _;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Bead identifier for log correlation.
const BEAD_ID: &str = "bd-1dp9.1.2";

/// Current envelope format version.
pub const FORMAT_VERSION: u32 = 1;

// ─── Execution Envelope ──────────────────────────────────────────────────

/// A self-describing, reproducible specification of a differential test run.
///
/// The envelope is the single source of truth for "what was tested".  Two runs
/// with identical envelopes MUST produce identical normalized results.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExecutionEnvelope {
    /// Schema version for forward compatibility.
    pub format_version: u32,
    /// Unique run identifier for log correlation (not part of the artifact ID).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    /// Base seed for deterministic RNG derivation.
    pub seed: u64,
    /// Engine version metadata.
    pub engines: EngineVersions,
    /// PRAGMA configuration applied to both engines before the workload.
    pub pragmas: PragmaConfig,
    /// Schema setup SQL (CREATE TABLE, CREATE INDEX, etc.) executed before
    /// the workload.  Order matters.
    pub schema: Vec<String>,
    /// The workload: an ordered sequence of SQL statements to execute
    /// against both engines.
    pub workload: Vec<String>,
    /// Rules governing how outputs are normalized before comparison.
    pub canonicalization: CanonicalizationRules,
}

/// Engine version strings for reproducibility pinning.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EngineVersions {
    /// FrankenSQLite crate version (from Cargo.toml).
    pub fsqlite: String,
    /// C SQLite version (from rusqlite bundled library).
    pub csqlite: String,
}

/// PRAGMA configuration applied to both engines before a run.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PragmaConfig {
    /// Journal mode: `"wal"`, `"delete"`, `"memory"`, etc.
    pub journal_mode: String,
    /// Synchronous level: `"OFF"`, `"NORMAL"`, `"FULL"`.
    pub synchronous: String,
    /// Page cache size (negative = KiB, positive = pages).
    pub cache_size: i64,
    /// Database page size in bytes.
    pub page_size: u32,
}

impl Default for PragmaConfig {
    fn default() -> Self {
        Self {
            journal_mode: "wal".to_owned(),
            synchronous: "NORMAL".to_owned(),
            cache_size: -2000,
            page_size: 4096,
        }
    }
}

impl PragmaConfig {
    /// Emit the PRAGMA statements for a C SQLite (rusqlite) connection.
    #[must_use]
    pub fn to_pragma_sql(&self) -> Vec<String> {
        vec![
            format!("PRAGMA journal_mode={};", self.journal_mode),
            format!("PRAGMA synchronous={};", self.synchronous),
            format!("PRAGMA cache_size={};", self.cache_size),
            format!("PRAGMA page_size={};", self.page_size),
        ]
    }
}

/// Rules governing output normalization during comparison.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CanonicalizationRules {
    /// Relative tolerance for floating-point comparison.
    /// Stored as a string to avoid floating-point non-determinism in hashing.
    pub float_tolerance: String,
    /// Compare unordered result sets as sorted multisets.
    pub unordered_results_as_multiset: bool,
    /// Match errors by category rather than exact message text.
    pub error_match_by_category: bool,
    /// Normalize whitespace in text values.
    pub normalize_whitespace: bool,
}

impl Default for CanonicalizationRules {
    fn default() -> Self {
        Self {
            float_tolerance: "1e-12".to_owned(),
            unordered_results_as_multiset: true,
            error_match_by_category: true,
            normalize_whitespace: true,
        }
    }
}

impl ExecutionEnvelope {
    /// Compute the deterministic artifact ID for this envelope.
    ///
    /// The artifact ID is the SHA-256 of a canonical JSON representation that
    /// excludes the mutable `run_id` field but includes all semantically
    /// significant fields.
    ///
    /// # Invariant
    ///
    /// Two envelopes that differ only in `run_id` MUST produce the same artifact ID.
    #[must_use]
    pub fn artifact_id(&self) -> String {
        // Create a copy without run_id for canonical hashing
        let canonical = CanonicalEnvelope {
            format_version: self.format_version,
            seed: self.seed,
            engines: &self.engines,
            pragmas: &self.pragmas,
            schema: &self.schema,
            workload: &self.workload,
            canonicalization: &self.canonicalization,
        };
        let json = serde_json::to_string(&canonical).expect("envelope serialization must not fail");
        sha256_hex(json.as_bytes())
    }

    /// Builder for creating envelopes with sensible defaults.
    #[must_use]
    pub fn builder(seed: u64) -> EnvelopeBuilder {
        EnvelopeBuilder {
            seed,
            run_id: None,
            engines: EngineVersions {
                fsqlite: env!("CARGO_PKG_VERSION").to_owned(),
                csqlite: String::new(),
            },
            pragmas: PragmaConfig::default(),
            schema: Vec::new(),
            workload: Vec::new(),
            canonicalization: CanonicalizationRules::default(),
        }
    }
}

/// Canonical form for hashing — excludes run_id.
#[derive(Serialize)]
struct CanonicalEnvelope<'a> {
    format_version: u32,
    seed: u64,
    engines: &'a EngineVersions,
    pragmas: &'a PragmaConfig,
    schema: &'a [String],
    workload: &'a [String],
    canonicalization: &'a CanonicalizationRules,
}

/// Fluent builder for `ExecutionEnvelope`.
pub struct EnvelopeBuilder {
    seed: u64,
    run_id: Option<String>,
    engines: EngineVersions,
    pragmas: PragmaConfig,
    schema: Vec<String>,
    workload: Vec<String>,
    canonicalization: CanonicalizationRules,
}

impl EnvelopeBuilder {
    /// Set the run identifier (optional, for log correlation only).
    #[must_use]
    pub fn run_id(mut self, id: impl Into<String>) -> Self {
        self.run_id = Some(id.into());
        self
    }

    /// Set engine versions.
    #[must_use]
    pub fn engines(mut self, fsqlite: impl Into<String>, csqlite: impl Into<String>) -> Self {
        self.engines.fsqlite = fsqlite.into();
        self.engines.csqlite = csqlite.into();
        self
    }

    /// Set PRAGMA configuration.
    #[must_use]
    pub fn pragmas(mut self, pragmas: PragmaConfig) -> Self {
        self.pragmas = pragmas;
        self
    }

    /// Add schema setup SQL statements.
    #[must_use]
    pub fn schema(mut self, stmts: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.schema.extend(stmts.into_iter().map(Into::into));
        self
    }

    /// Add workload SQL statements.
    #[must_use]
    pub fn workload(mut self, stmts: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.workload.extend(stmts.into_iter().map(Into::into));
        self
    }

    /// Set canonicalization rules.
    #[must_use]
    pub fn canonicalization(mut self, rules: CanonicalizationRules) -> Self {
        self.canonicalization = rules;
        self
    }

    /// Build the envelope.
    #[must_use]
    pub fn build(self) -> ExecutionEnvelope {
        ExecutionEnvelope {
            format_version: FORMAT_VERSION,
            run_id: self.run_id,
            seed: self.seed,
            engines: self.engines,
            pragmas: self.pragmas,
            schema: self.schema,
            workload: self.workload,
            canonicalization: self.canonicalization,
        }
    }
}

// ─── Differential Result ─────────────────────────────────────────────────

/// Normalized SQL value for cross-engine comparison.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum NormalizedValue {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

impl Eq for NormalizedValue {}

impl fmt::Display for NormalizedValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Null => write!(f, "NULL"),
            Self::Integer(i) => write!(f, "{i}"),
            Self::Real(r) => write!(f, "{r}"),
            Self::Text(s) => write!(f, "'{s}'"),
            Self::Blob(b) => {
                write!(f, "X'")?;
                for byte in b {
                    write!(f, "{byte:02X}")?;
                }
                write!(f, "'")
            }
        }
    }
}

/// Outcome of executing a single SQL statement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StmtOutcome {
    /// Query returned rows.
    Rows(Vec<Vec<NormalizedValue>>),
    /// DML executed with N affected rows.
    Execute(usize),
    /// Statement failed.
    Error(String),
}

/// A single statement-level divergence between engines.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatementDivergence {
    /// Zero-based index in the combined (schema + workload) sequence.
    pub index: usize,
    /// The SQL statement that diverged.
    pub sql: String,
    /// C SQLite outcome.
    pub csqlite_outcome: StmtOutcome,
    /// FrankenSQLite outcome.
    pub fsqlite_outcome: StmtOutcome,
}

/// Hashes of all artifacts produced by a differential run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArtifactHashes {
    /// SHA-256 of the envelope (deterministic input ID).
    pub envelope_id: String,
    /// SHA-256 of the serialized result (deterministic output ID).
    pub result_hash: String,
    /// SHA-256 of the concatenated workload SQL.
    pub workload_hash: String,
}

/// Complete result of a differential harness v2 run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DifferentialResult {
    /// Bead ID for log correlation.
    pub bead_id: String,
    /// The envelope that was executed (input specification).
    pub envelope: ExecutionEnvelope,
    /// Total statements executed (schema + workload).
    pub statements_total: usize,
    /// Number of statements with matching outcomes.
    pub statements_matched: usize,
    /// Number of statements with divergent outcomes.
    pub statements_mismatched: usize,
    /// Index of first divergence (if any).
    pub first_divergence_index: Option<usize>,
    /// All divergences.
    pub divergences: Vec<StatementDivergence>,
    /// Logical state hash from FrankenSQLite.
    pub logical_state_hash_fsqlite: String,
    /// Logical state hash from C SQLite.
    pub logical_state_hash_csqlite: String,
    /// Whether the final logical database states match.
    pub logical_state_matched: bool,
    /// Deterministic artifact hashes.
    pub artifact_hashes: ArtifactHashes,
    /// Overall outcome.
    pub outcome: Outcome,
}

/// Overall outcome of a differential run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Outcome {
    /// All statements matched and logical states are identical.
    Pass,
    /// At least one statement diverged or logical states differ.
    Divergence,
    /// An infrastructure error prevented comparison.
    Error,
}

impl fmt::Display for Outcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pass => write!(f, "pass"),
            Self::Divergence => write!(f, "divergence"),
            Self::Error => write!(f, "error"),
        }
    }
}

impl DifferentialResult {
    /// Compute the result hash (SHA-256 of the deterministic portion of the result).
    ///
    /// This hash is over the statement outcomes and logical state hashes,
    /// ensuring that identical runs produce identical result hashes.
    #[must_use]
    pub fn compute_result_hash(&self) -> String {
        let hashable = ResultHashable {
            statements_total: self.statements_total,
            statements_matched: self.statements_matched,
            statements_mismatched: self.statements_mismatched,
            first_divergence_index: self.first_divergence_index,
            logical_state_hash_fsqlite: &self.logical_state_hash_fsqlite,
            logical_state_hash_csqlite: &self.logical_state_hash_csqlite,
            logical_state_matched: self.logical_state_matched,
            outcome: self.outcome,
        };
        let json =
            serde_json::to_string(&hashable).expect("result hash serialization must not fail");
        sha256_hex(json.as_bytes())
    }
}

#[derive(Serialize)]
struct ResultHashable<'a> {
    statements_total: usize,
    statements_matched: usize,
    statements_mismatched: usize,
    first_divergence_index: Option<usize>,
    logical_state_hash_fsqlite: &'a str,
    logical_state_hash_csqlite: &'a str,
    logical_state_matched: bool,
    outcome: Outcome,
}

// ─── Execution Engine (trait-based for testability) ──────────────────────

/// Trait for a SQL execution backend (implemented by both fsqlite and rusqlite).
pub trait SqlExecutor {
    /// Execute a non-query statement, returning affected row count.
    ///
    /// # Errors
    ///
    /// Returns the error message as a string.
    fn execute(&self, sql: &str) -> Result<usize, String>;

    /// Execute a query, returning normalized rows.
    ///
    /// # Errors
    ///
    /// Returns the error message as a string.
    fn query(&self, sql: &str) -> Result<Vec<Vec<NormalizedValue>>, String>;

    /// Run a statement (auto-detecting query vs DML).
    fn run_stmt(&self, sql: &str) -> StmtOutcome {
        let trimmed = sql.trim();
        let is_query = trimmed.split_whitespace().next().is_some_and(|w| {
            w.eq_ignore_ascii_case("SELECT")
                || w.eq_ignore_ascii_case("PRAGMA")
                || w.eq_ignore_ascii_case("EXPLAIN")
        });

        if is_query {
            match self.query(trimmed) {
                Ok(rows) => StmtOutcome::Rows(rows),
                Err(e) => StmtOutcome::Error(e),
            }
        } else {
            match self.execute(trimmed) {
                Ok(n) => StmtOutcome::Execute(n),
                Err(e) => StmtOutcome::Error(e),
            }
        }
    }

    /// Produce a deterministic logical dump of all user tables.
    fn logical_dump(&self) -> String {
        let tables = match self.query(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
        ) {
            Ok(rows) => rows
                .into_iter()
                .filter_map(|r| match r.into_iter().next() {
                    Some(NormalizedValue::Text(name)) => Some(name),
                    _ => None,
                })
                .collect::<Vec<_>>(),
            Err(_) => Vec::new(),
        };

        let mut dump = String::new();
        for table in &tables {
            let _ = writeln!(dump, "-- TABLE: {table}");
            let rows = self
                .query(&format!("SELECT * FROM \"{table}\" ORDER BY rowid"))
                .or_else(|_| self.query(&format!("SELECT * FROM \"{table}\" ORDER BY 1")))
                .or_else(|_| self.query(&format!("SELECT * FROM \"{table}\"")));
            if let Ok(rows) = rows {
                for row in &rows {
                    for (j, val) in row.iter().enumerate() {
                        if j > 0 {
                            dump.push('|');
                        }
                        let _ = write!(dump, "{val}");
                    }
                    dump.push('\n');
                }
            }
        }
        dump
    }
}

/// FrankenSQLite executor wrapping `fsqlite::Connection`.
pub struct FsqliteExecutor {
    conn: fsqlite::Connection,
}

impl FsqliteExecutor {
    /// Open an in-memory FrankenSQLite database.
    ///
    /// # Errors
    ///
    /// Returns an error string if the connection fails.
    pub fn open_in_memory() -> Result<Self, String> {
        let conn = fsqlite::Connection::open(":memory:").map_err(|e| e.to_string())?;
        Ok(Self { conn })
    }
}

impl SqlExecutor for FsqliteExecutor {
    fn execute(&self, sql: &str) -> Result<usize, String> {
        self.conn.execute(sql.trim()).map_err(|e| e.to_string())
    }

    fn query(&self, sql: &str) -> Result<Vec<Vec<NormalizedValue>>, String> {
        let rows = self.conn.query(sql.trim()).map_err(|e| e.to_string())?;
        Ok(rows
            .into_iter()
            .map(|row| {
                row.values()
                    .iter()
                    .map(|v| match v {
                        fsqlite_types::value::SqliteValue::Null => NormalizedValue::Null,
                        fsqlite_types::value::SqliteValue::Integer(i) => {
                            NormalizedValue::Integer(*i)
                        }
                        fsqlite_types::value::SqliteValue::Float(f) => NormalizedValue::Real(*f),
                        fsqlite_types::value::SqliteValue::Text(s) => {
                            NormalizedValue::Text(s.clone())
                        }
                        fsqlite_types::value::SqliteValue::Blob(b) => {
                            NormalizedValue::Blob(b.clone())
                        }
                    })
                    .collect()
            })
            .collect())
    }
}

// ─── Harness Runner ──────────────────────────────────────────────────────

/// Run a differential test from an execution envelope.
///
/// This is the main entry point: given an envelope and two executors,
/// it runs the schema + workload on both and produces a `DifferentialResult`.
pub fn run_differential<F: SqlExecutor, C: SqlExecutor>(
    envelope: &ExecutionEnvelope,
    fsqlite_exec: &F,
    csqlite_exec: &C,
) -> DifferentialResult {
    // Apply PRAGMAs to both engines (ignore errors — some PRAGMAs return rows).
    for pragma in &envelope.pragmas.to_pragma_sql() {
        let _ = fsqlite_exec.run_stmt(pragma);
        let _ = csqlite_exec.run_stmt(pragma);
    }

    // Collect schema + workload into a single ordered sequence.
    let statements: Vec<&str> = envelope
        .schema
        .iter()
        .chain(envelope.workload.iter())
        .map(String::as_str)
        .collect();

    let mut matched = 0usize;
    let mut mismatched = 0usize;
    let mut divergences = Vec::new();
    let mut first_divergence_index: Option<usize> = None;

    for (i, &sql) in statements.iter().enumerate() {
        let f_out = fsqlite_exec.run_stmt(sql);
        let c_out = csqlite_exec.run_stmt(sql);

        if outcomes_match(&f_out, &c_out, &envelope.canonicalization) {
            matched += 1;
        } else {
            mismatched += 1;
            if first_divergence_index.is_none() {
                first_divergence_index = Some(i);
            }
            divergences.push(StatementDivergence {
                index: i,
                sql: sql.to_owned(),
                csqlite_outcome: c_out,
                fsqlite_outcome: f_out,
            });
        }
    }

    // Compare logical state.
    let f_dump = fsqlite_exec.logical_dump();
    let c_dump = csqlite_exec.logical_dump();
    let f_hash = sha256_hex(f_dump.as_bytes());
    let c_hash = sha256_hex(c_dump.as_bytes());
    let state_matched = f_hash == c_hash;

    let envelope_id = envelope.artifact_id();
    let workload_hash = {
        let combined: String = statements.join("\n");
        sha256_hex(combined.as_bytes())
    };

    let outcome = if mismatched == 0 && state_matched {
        Outcome::Pass
    } else {
        Outcome::Divergence
    };

    let mut result = DifferentialResult {
        bead_id: BEAD_ID.to_owned(),
        envelope: envelope.clone(),
        statements_total: statements.len(),
        statements_matched: matched,
        statements_mismatched: mismatched,
        first_divergence_index,
        divergences,
        logical_state_hash_fsqlite: f_hash,
        logical_state_hash_csqlite: c_hash,
        logical_state_matched: state_matched,
        artifact_hashes: ArtifactHashes {
            envelope_id,
            result_hash: String::new(),
            workload_hash,
        },
        outcome,
    };

    result.artifact_hashes.result_hash = result.compute_result_hash();
    result
}

// ─── Outcome comparison with canonicalization ────────────────────────────

fn outcomes_match(
    fsqlite: &StmtOutcome,
    csqlite: &StmtOutcome,
    rules: &CanonicalizationRules,
) -> bool {
    match (fsqlite, csqlite) {
        (StmtOutcome::Execute(a), StmtOutcome::Execute(b)) => a == b,
        (StmtOutcome::Rows(a), StmtOutcome::Rows(b)) => rows_match(a, b, rules),
        (StmtOutcome::Error(_), StmtOutcome::Error(_)) if rules.error_match_by_category => {
            // Both errored — match by category (both failed = match).
            true
        }
        (StmtOutcome::Error(a), StmtOutcome::Error(b)) => a == b,
        _ => false,
    }
}

fn rows_match(
    a: &[Vec<NormalizedValue>],
    b: &[Vec<NormalizedValue>],
    rules: &CanonicalizationRules,
) -> bool {
    if a.len() != b.len() {
        return false;
    }

    if rules.unordered_results_as_multiset {
        let mut a_sorted = a.to_vec();
        let mut b_sorted = b.to_vec();
        let key = |row: &[NormalizedValue]| -> String {
            row.iter()
                .map(|v| format!("{v}"))
                .collect::<Vec<_>>()
                .join("|")
        };
        a_sorted.sort_by(|x, y| key(x).cmp(&key(y)));
        b_sorted.sort_by(|x, y| key(x).cmp(&key(y)));
        a_sorted
            .iter()
            .zip(b_sorted.iter())
            .all(|(ra, rb)| row_values_match(ra, rb, rules))
    } else {
        a.iter()
            .zip(b.iter())
            .all(|(ra, rb)| row_values_match(ra, rb, rules))
    }
}

fn row_values_match(
    a: &[NormalizedValue],
    b: &[NormalizedValue],
    rules: &CanonicalizationRules,
) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter()
        .zip(b.iter())
        .all(|(va, vb)| value_match(va, vb, rules))
}

fn value_match(a: &NormalizedValue, b: &NormalizedValue, rules: &CanonicalizationRules) -> bool {
    match (a, b) {
        (NormalizedValue::Null, NormalizedValue::Null) => true,
        (NormalizedValue::Integer(x), NormalizedValue::Integer(y)) => x == y,
        (NormalizedValue::Real(x), NormalizedValue::Real(y)) => {
            let tol: f64 = rules.float_tolerance.parse().unwrap_or(1e-12);
            floats_match(*x, *y, tol)
        }
        (NormalizedValue::Text(x), NormalizedValue::Text(y)) => {
            if rules.normalize_whitespace {
                normalize_ws(x) == normalize_ws(y)
            } else {
                x == y
            }
        }
        (NormalizedValue::Blob(x), NormalizedValue::Blob(y)) => x == y,
        // Cross-type: integer and real that represent the same value.
        (NormalizedValue::Integer(i), NormalizedValue::Real(f))
        | (NormalizedValue::Real(f), NormalizedValue::Integer(i)) => {
            #[allow(clippy::cast_precision_loss)]
            let fi = *i as f64;
            let tol: f64 = rules.float_tolerance.parse().unwrap_or(1e-12);
            floats_match(fi, *f, tol)
        }
        _ => false,
    }
}

fn floats_match(a: f64, b: f64, tolerance: f64) -> bool {
    if a == b {
        return true;
    }
    let denom = a.abs().max(b.abs());
    if denom == 0.0 {
        return (a - b).abs() < tolerance;
    }
    ((a - b).abs() / denom) < tolerance
}

fn normalize_ws(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

// ─── Helpers ─────────────────────────────────────────────────────────────

/// Compute SHA-256 hex digest.
fn sha256_hex(data: &[u8]) -> String {
    let digest = Sha256::digest(data);
    let mut hex = String::with_capacity(64);
    for byte in digest {
        let _ = write!(hex, "{byte:02x}");
    }
    hex
}
