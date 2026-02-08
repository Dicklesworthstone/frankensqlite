//! Golden-file test oracle harness for C sqlite3 behavioral parity (§1.1, bd-1daa).
//!
//! Compares FrankenSQLite execution results against C SQLite 3.52.0 output.
//! Any intentional divergence must be annotated with a machine-readable rationale
//! and spec section reference; unannotated divergences fail CI.
//!
//! # Architecture
//!
//! ```text
//! JSON Fixture → OracleRunner → (C sqlite3 output, FrankenSQLite output) → Comparator → Report
//! ```
//!
//! # Normalization Rules (§17.7)
//!
//! - Unordered result sets compared as sorted multisets
//! - Float tolerance: 1e-12 relative
//! - Error codes: match by category, not exact integer

use std::fmt;
use std::fmt::Write as _;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::process::Stdio;

use serde::{Deserialize, Serialize};
use tracing::{debug, error, info, warn};

use fsqlite_error::{FrankenError, Result};

/// Bead identifier for log correlation.
const BEAD_ID: &str = "bd-1daa";

/// Default float comparison tolerance (relative).
const FLOAT_TOLERANCE: f64 = 1e-12;

/// Required C SQLite version prefix for oracle pinning (INV-ORACLE-VERSION-PINNED).
const ORACLE_VERSION_PREFIX: &str = "3.";

// ---------------------------------------------------------------------------
// Fixture format (JSON)
// ---------------------------------------------------------------------------

/// A single test fixture defining SQL to execute and expected behavior.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TestFixture {
    /// Unique identifier for this fixture.
    pub id: String,
    /// Human-readable description.
    #[serde(default)]
    pub description: String,
    /// Sequence of operations to execute.
    pub ops: Vec<FixtureOp>,
    /// Which FrankenSQLite modes this fixture applies to.
    #[serde(default = "default_modes")]
    pub fsqlite_modes: Vec<FsqliteMode>,
    /// Intentional divergence annotation, if any.
    #[serde(default)]
    pub divergence: Option<DivergenceAnnotation>,
}

/// An operation within a fixture.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op")]
pub enum FixtureOp {
    /// Open a database (in-memory or file-backed).
    #[serde(rename = "open")]
    Open {
        /// Database path (`:memory:` for in-memory).
        #[serde(default = "default_memory_db")]
        path: String,
    },
    /// Execute a statement (no result rows expected).
    #[serde(rename = "exec")]
    Exec {
        /// SQL statement to execute.
        sql: String,
        /// Expected error category, if the statement should fail.
        #[serde(default)]
        expect_error: Option<ErrorCategory>,
    },
    /// Execute a query and check results.
    #[serde(rename = "query")]
    Query {
        /// SQL query to execute.
        sql: String,
        /// Expected result rows.
        #[serde(default)]
        expect: QueryExpectation,
    },
}

/// Expected query results.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct QueryExpectation {
    /// Expected column names.
    #[serde(default)]
    pub columns: Vec<String>,
    /// Expected row values (each row is a vec of string representations).
    #[serde(default)]
    pub rows: Vec<Vec<String>>,
    /// Expected row count (if rows are not checked individually).
    #[serde(default)]
    pub row_count: Option<usize>,
    /// Whether row order matters.
    #[serde(default)]
    pub ordered: bool,
    /// Expected error category, if the query should fail.
    #[serde(default)]
    pub expect_error: Option<ErrorCategory>,
}

/// Error categories for coarse-grained comparison (§17.7).
///
/// We compare error *categories*, not exact codes or messages,
/// since FrankenSQLite may produce different message text.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ErrorCategory {
    Error,
    Busy,
    Locked,
    Constraint,
    Corrupt,
    Schema,
    IoErr,
    Auth,
    Abort,
    ReadOnly,
    CantOpen,
    Full,
    Range,
    NotADb,
}

impl ErrorCategory {
    /// Classify a C sqlite3 error string into a category.
    #[must_use]
    pub fn from_sqlite_error(msg: &str) -> Self {
        let upper = msg.to_uppercase();
        if upper.contains("CONSTRAINT") {
            return Self::Constraint;
        }
        if upper.contains("BUSY") {
            return Self::Busy;
        }
        if upper.contains("LOCKED") {
            return Self::Locked;
        }
        if upper.contains("CORRUPT") || upper.contains("MALFORMED") {
            return Self::Corrupt;
        }
        if upper.contains("SCHEMA") {
            return Self::Schema;
        }
        if upper.contains("READONLY") || upper.contains("READ-ONLY") {
            return Self::ReadOnly;
        }
        if upper.contains("UNABLE TO OPEN") || upper.contains("CANNOT OPEN") {
            return Self::CantOpen;
        }
        if upper.contains("AUTHORIZATION") {
            return Self::Auth;
        }
        if upper.contains("ABORT") {
            return Self::Abort;
        }
        if upper.contains("FULL") {
            return Self::Full;
        }
        if upper.contains("NOT A DATABASE") {
            return Self::NotADb;
        }
        Self::Error
    }

    /// Classify a `FrankenError` into a category.
    #[must_use]
    pub fn from_franken_error(err: &FrankenError) -> Self {
        use fsqlite_error::ErrorCode;
        match err.error_code() {
            ErrorCode::Busy => Self::Busy,
            ErrorCode::Locked => Self::Locked,
            ErrorCode::Constraint => Self::Constraint,
            ErrorCode::Corrupt => Self::Corrupt,
            ErrorCode::Schema => Self::Schema,
            ErrorCode::IoErr => Self::IoErr,
            ErrorCode::Auth => Self::Auth,
            ErrorCode::Abort => Self::Abort,
            ErrorCode::ReadOnly => Self::ReadOnly,
            ErrorCode::CantOpen => Self::CantOpen,
            ErrorCode::Full => Self::Full,
            ErrorCode::Range => Self::Range,
            ErrorCode::NotADb => Self::NotADb,
            _ => Self::Error,
        }
    }
}

impl fmt::Display for ErrorCategory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Error => "ERROR",
            Self::Busy => "BUSY",
            Self::Locked => "LOCKED",
            Self::Constraint => "CONSTRAINT",
            Self::Corrupt => "CORRUPT",
            Self::Schema => "SCHEMA",
            Self::IoErr => "IOERR",
            Self::Auth => "AUTH",
            Self::Abort => "ABORT",
            Self::ReadOnly => "READONLY",
            Self::CantOpen => "CANTOPEN",
            Self::Full => "FULL",
            Self::Range => "RANGE",
            Self::NotADb => "NOTADB",
        })
    }
}

/// FrankenSQLite operating mode for mode-specific fixtures.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FsqliteMode {
    Compatibility,
    Native,
}

/// Machine-readable annotation for an intentional behavioral divergence.
///
/// Every divergence must have a rationale and spec reference.
/// Unannotated divergences fail CI (INV-DIVERGENCE-DOCUMENTED).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DivergenceAnnotation {
    /// Human-readable description of what differs.
    pub description: String,
    /// Why we intentionally diverge.
    pub rationale: String,
    /// Spec section reference (e.g., "§2.4: MVCC provides stronger isolation").
    pub spec_ref: String,
}

fn default_modes() -> Vec<FsqliteMode> {
    vec![FsqliteMode::Compatibility, FsqliteMode::Native]
}

fn default_memory_db() -> String {
    ":memory:".to_string()
}

// ---------------------------------------------------------------------------
// Oracle result types
// ---------------------------------------------------------------------------

/// Result of executing a single operation against an oracle or FrankenSQLite.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum OpResult {
    /// Operation succeeded with no result rows (exec/open).
    Ok,
    /// Query returned rows.
    Rows {
        columns: Vec<String>,
        rows: Vec<Vec<String>>,
    },
    /// Operation failed with an error.
    Error {
        category: ErrorCategory,
        message: String,
    },
}

/// Full result of running a fixture.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FixtureResult {
    pub fixture_id: String,
    pub op_results: Vec<OpResult>,
}

// ---------------------------------------------------------------------------
// Normalization
// ---------------------------------------------------------------------------

/// Normalize a result value string for comparison.
///
/// Applies float tolerance and whitespace normalization.
#[must_use]
pub fn normalize_value(value: &str) -> String {
    let trimmed = value.trim();
    // Try to parse as float for tolerance-based comparison.
    if let Ok(f) = trimmed.parse::<f64>() {
        if f.is_nan() {
            return "NaN".to_string();
        }
        if f.is_infinite() {
            return if f.is_sign_positive() {
                "Inf".to_string()
            } else {
                "-Inf".to_string()
            };
        }
        // Round to eliminate floating-point noise beyond tolerance.
        return format!("{f:.15}");
    }
    // For non-float values, normalize NULL representation.
    if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("null") {
        return "NULL".to_string();
    }
    trimmed.to_string()
}

/// Compare two float strings within relative tolerance.
#[must_use]
pub fn floats_match(a: &str, b: &str) -> bool {
    let (Ok(fa), Ok(fb)) = (a.trim().parse::<f64>(), b.trim().parse::<f64>()) else {
        return false;
    };
    let diff = (fa - fb).abs();
    // Clamp the scale to >= 1.0 so near-zero values compare sensibly without
    // strict float equality.
    let scale = fa.abs().max(fb.abs()).max(1.0);
    (diff / scale) < FLOAT_TOLERANCE
}

/// Normalize rows as a sorted multiset for unordered comparison.
#[must_use]
pub fn normalize_rows_as_multiset(rows: &[Vec<String>]) -> Vec<Vec<String>> {
    let mut normalized: Vec<Vec<String>> = rows
        .iter()
        .map(|row| row.iter().map(|v| normalize_value(v)).collect())
        .collect();
    normalized.sort();
    normalized
}

// ---------------------------------------------------------------------------
// Comparison engine
// ---------------------------------------------------------------------------

/// Outcome of comparing oracle and FrankenSQLite results for a single operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompareOutcome {
    /// Results match (possibly after normalization).
    Match,
    /// Results differ.
    Mismatch { detail: String },
}

/// Compare two `OpResult` values with normalization.
#[must_use]
pub fn compare_op_results(oracle: &OpResult, franken: &OpResult, ordered: bool) -> CompareOutcome {
    match (oracle, franken) {
        (OpResult::Ok, OpResult::Ok) => CompareOutcome::Match,
        (
            OpResult::Error {
                category: cat_o, ..
            },
            OpResult::Error {
                category: cat_f, ..
            },
        ) => {
            if cat_o == cat_f {
                CompareOutcome::Match
            } else {
                CompareOutcome::Mismatch {
                    detail: format!("error category mismatch: oracle={cat_o}, franken={cat_f}"),
                }
            }
        }
        (
            OpResult::Rows {
                columns: cols_o,
                rows: rows_o,
            },
            OpResult::Rows {
                columns: cols_f,
                rows: rows_f,
            },
        ) => compare_query_results(cols_o, rows_o, cols_f, rows_f, ordered),
        _ => CompareOutcome::Mismatch {
            detail: format!(
                "result kind mismatch: oracle={}, franken={}",
                op_result_kind(oracle),
                op_result_kind(franken)
            ),
        },
    }
}

fn op_result_kind(result: &OpResult) -> &'static str {
    match result {
        OpResult::Ok => "ok",
        OpResult::Rows { .. } => "rows",
        OpResult::Error { .. } => "error",
    }
}

fn compare_query_results(
    cols_o: &[String],
    rows_o: &[Vec<String>],
    cols_f: &[String],
    rows_f: &[Vec<String>],
    ordered: bool,
) -> CompareOutcome {
    // Column count must match (skip if either side has no column metadata).
    if !cols_o.is_empty() && !cols_f.is_empty() && cols_o.len() != cols_f.len() {
        return CompareOutcome::Mismatch {
            detail: format!(
                "column count mismatch: oracle={}, franken={}",
                cols_o.len(),
                cols_f.len()
            ),
        };
    }
    // Row count must match.
    if rows_o.len() != rows_f.len() {
        return CompareOutcome::Mismatch {
            detail: format!(
                "row count mismatch: oracle={}, franken={}",
                rows_o.len(),
                rows_f.len()
            ),
        };
    }

    let (norm_o, norm_f) = if ordered {
        (
            rows_o
                .iter()
                .map(|r| r.iter().map(|v| normalize_value(v)).collect::<Vec<_>>())
                .collect::<Vec<_>>(),
            rows_f
                .iter()
                .map(|r| r.iter().map(|v| normalize_value(v)).collect::<Vec<_>>())
                .collect::<Vec<_>>(),
        )
    } else {
        (
            normalize_rows_as_multiset(rows_o),
            normalize_rows_as_multiset(rows_f),
        )
    };

    for (i, (row_o, row_f)) in norm_o.iter().zip(norm_f.iter()).enumerate() {
        for (j, (val_o, val_f)) in row_o.iter().zip(row_f.iter()).enumerate() {
            if val_o != val_f && !floats_match(val_o, val_f) {
                return CompareOutcome::Mismatch {
                    detail: format!(
                        "value mismatch at row {i}, col {j}: oracle={val_o:?}, franken={val_f:?}"
                    ),
                };
            }
        }
    }
    CompareOutcome::Match
}

// ---------------------------------------------------------------------------
// C sqlite3 oracle execution
// ---------------------------------------------------------------------------

/// Locates the C sqlite3 binary on the system.
///
/// Returns the path if found and version is acceptable.
pub fn find_sqlite3_binary() -> Result<PathBuf> {
    let candidates = [
        "/usr/bin/sqlite3",
        "/usr/local/bin/sqlite3",
        "/opt/homebrew/bin/sqlite3",
    ];
    for path_str in candidates {
        let path = PathBuf::from(path_str);
        if path.is_file() {
            return Ok(path);
        }
    }
    Err(FrankenError::Internal(
        "C sqlite3 binary not found on system".to_string(),
    ))
}

/// Verify the oracle binary version starts with the expected prefix.
pub fn verify_oracle_version(sqlite3_path: &Path) -> Result<String> {
    let output = Command::new(sqlite3_path)
        .arg("--version")
        .output()
        .map_err(|err| {
            FrankenError::Internal(format!("failed to execute sqlite3 --version: {err}"))
        })?;

    let version_str = String::from_utf8_lossy(&output.stdout).trim().to_string();
    info!(
        bead_id = BEAD_ID,
        binary = %sqlite3_path.display(),
        version = %version_str,
        "oracle version check"
    );

    if !version_str.starts_with(ORACLE_VERSION_PREFIX) {
        return Err(FrankenError::Internal(format!(
            "oracle version mismatch: expected prefix '{ORACLE_VERSION_PREFIX}', got '{version_str}'"
        )));
    }

    Ok(version_str)
}

/// Execute SQL statements against the C sqlite3 oracle and capture output.
///
/// Runs all statements in a single sqlite3 process via stdin so that
/// `:memory:` databases persist across statements. Uses sentinel markers
/// to delimit per-statement output.
#[allow(clippy::too_many_lines)]
pub fn run_sqlite3_oracle(
    sqlite3_path: &Path,
    db_path: &str,
    sql_statements: &[&str],
) -> Result<Vec<OpResult>> {
    let db_arg = if db_path == ":memory:" {
        ":memory:".to_string()
    } else {
        db_path.to_string()
    };

    // Build a single script that delimits each statement's output with sentinels.
    // We use `.print` to emit markers that we can parse afterwards.
    let sentinel_prefix = "__FSQLITE_ORACLE_SENTINEL__";
    let mut script = String::new();
    let mut stmt_types = Vec::new(); // (idx, is_query, sql_text)
    // Track script line numbers so stderr errors ("near line N:") can be
    // mapped back to the correct statement index.
    let mut line_number = 1_usize;
    let mut sql_line_to_idx: Vec<(usize, usize)> = Vec::new();

    for (idx, &sql) in sql_statements.iter().enumerate() {
        let trimmed = sql.trim();
        if trimmed.is_empty() {
            continue;
        }
        let is_query = trimmed.split_ascii_whitespace().next().is_some_and(|kw| {
            kw.eq_ignore_ascii_case("SELECT")
                || kw.eq_ignore_ascii_case("PRAGMA")
                || kw.eq_ignore_ascii_case("EXPLAIN")
                || kw.eq_ignore_ascii_case("VALUES")
        });

        // Emit start sentinel (one script line).
        let _ = writeln!(script, ".print {sentinel_prefix}START_{idx}");
        line_number += 1;

        if is_query {
            let _ = writeln!(script, ".mode json");
            line_number += 1;
        }
        // Record which script line this SQL statement occupies.
        sql_line_to_idx.push((line_number, idx));
        // Ensure SQL ends with semicolon so sqlite3 CLI doesn't treat the
        // next `.print` sentinel as a continuation of the SQL statement.
        if trimmed.ends_with(';') {
            let _ = writeln!(script, "{trimmed}");
        } else {
            let _ = writeln!(script, "{trimmed};");
        }
        line_number += 1;
        // Emit end sentinel (one script line).
        let _ = writeln!(script, ".print {sentinel_prefix}END_{idx}");
        line_number += 1;

        stmt_types.push((idx, is_query, trimmed.to_string()));

        debug!(
            bead_id = BEAD_ID,
            sql = %trimmed,
            idx = idx,
            is_query = is_query,
            "oracle queuing SQL"
        );
    }

    let mut child = Command::new(sqlite3_path)
        .arg(&db_arg)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|err| FrankenError::Internal(format!("failed to spawn sqlite3: {err}")))?;

    if let Some(ref mut stdin) = child.stdin {
        stdin.write_all(script.as_bytes()).map_err(|err| {
            FrankenError::Internal(format!("failed to write to sqlite3 stdin: {err}"))
        })?;
    }
    drop(child.stdin.take());

    let output = child
        .wait_with_output()
        .map_err(|err| FrankenError::Internal(format!("sqlite3 process failed: {err}")))?;

    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();

    // Parse stdout into per-statement sections using sentinels.
    let mut results = Vec::new();
    for (idx, is_query, _sql_text) in &stmt_types {
        let start_marker = format!("{sentinel_prefix}START_{idx}");
        let end_marker = format!("{sentinel_prefix}END_{idx}");

        // Extract the section between start and end markers.
        let section = extract_section(&stdout, &start_marker, &end_marker);

        if !section.is_empty() && *is_query {
            if let Some((columns, rows)) = parse_json_rows(&section) {
                results.push(OpResult::Rows { columns, rows });
            } else {
                // Non-JSON output — try line-based.
                let rows: Vec<Vec<String>> = section
                    .lines()
                    .filter(|l| !l.trim().is_empty())
                    .map(|line| vec![line.to_string()])
                    .collect();
                if rows.is_empty() {
                    results.push(OpResult::Rows {
                        columns: Vec::new(),
                        rows: Vec::new(),
                    });
                } else {
                    results.push(OpResult::Rows {
                        columns: vec!["result".to_string()],
                        rows,
                    });
                }
            }
        } else if !*is_query {
            results.push(OpResult::Ok);
        } else {
            results.push(OpResult::Rows {
                columns: Vec::new(),
                rows: Vec::new(),
            });
        }
    }

    // If there were errors in stderr, patch the relevant results using
    // line-number mapping to identify which statement each error belongs to.
    if !stderr.trim().is_empty() {
        patch_errors_from_stderr(&mut results, &stderr, &sql_line_to_idx);
    }

    Ok(results)
}

/// Extract text between two sentinel markers.
fn extract_section(text: &str, start: &str, end: &str) -> String {
    let start_pos = text.find(start).map(|p| p + start.len());
    let end_pos = text.find(end);
    match (start_pos, end_pos) {
        (Some(s), Some(e)) if s <= e => text[s..e].trim().to_string(),
        _ => String::new(),
    }
}

/// Parse JSON array of objects from sqlite3 `.mode json` output.
fn parse_json_rows(text: &str) -> Option<(Vec<String>, Vec<Vec<String>>)> {
    let trimmed = text.trim();
    if trimmed.is_empty() || trimmed == "[]" {
        return Some((Vec::new(), Vec::new()));
    }
    // Parse as Value (not BTreeMap) to preserve sqlite3's column order.
    let arr: Vec<serde_json::Value> = serde_json::from_str(trimmed).ok()?;
    let columns: Vec<String> = arr
        .first()
        .and_then(|first| first.as_object().map(|obj| obj.keys().cloned().collect()))?;
    let rows: Vec<Vec<String>> = arr
        .iter()
        .filter_map(|row| {
            let obj = row.as_object()?;
            Some(
                columns
                    .iter()
                    .map(|col| match obj.get(col) {
                        Some(serde_json::Value::Null) | None => "NULL".to_string(),
                        Some(serde_json::Value::String(s)) => s.clone(),
                        Some(v) => v.to_string(),
                    })
                    .collect(),
            )
        })
        .collect();
    Some((columns, rows))
}

/// Patch `OpResult::Ok` entries with errors detected in stderr.
///
/// sqlite3 CLI stderr errors include "near line N:" where N is the script
/// line number. We match each error to its source statement using the
/// `sql_line_to_idx` mapping built during script generation.
fn patch_errors_from_stderr(
    results: &mut [OpResult],
    stderr: &str,
    sql_line_to_idx: &[(usize, usize)],
) {
    // Parse "near line N:" from each stderr error line to find the script line.
    let line_re_prefix = "near line ";
    for err_line in stderr.lines().filter(|l| !l.trim().is_empty()) {
        // Extract line number from patterns like "Runtime error near line 8:"
        // or "Error: near line 5:".
        let stmt_idx = if let Some(pos) = err_line.find(line_re_prefix) {
            let after = &err_line[pos + line_re_prefix.len()..];
            let num_str: String = after.chars().take_while(char::is_ascii_digit).collect();
            if let Ok(script_line) = num_str.parse::<usize>() {
                // Find the statement index whose script line is closest to
                // (and <= ) the reported error line.
                sql_line_to_idx
                    .iter()
                    .filter(|(line, _)| *line <= script_line)
                    .max_by_key(|(line, _)| *line)
                    .map(|(_, idx)| *idx)
            } else {
                None
            }
        } else {
            None
        };

        if let Some(idx) = stmt_idx {
            if idx < results.len() {
                let category = ErrorCategory::from_sqlite_error(err_line);
                debug!(
                    bead_id = BEAD_ID,
                    stmt_idx = idx,
                    category = %category,
                    err = %err_line.trim(),
                    "patching oracle result with stderr error (line-number match)"
                );
                results[idx] = OpResult::Error {
                    category,
                    message: err_line.trim().to_string(),
                };
            }
        } else {
            // Fallback: no line number found — assign to first Ok result.
            if let Some((i, result)) = results
                .iter_mut()
                .enumerate()
                .find(|(_, r)| matches!(r, OpResult::Ok))
            {
                let category = ErrorCategory::from_sqlite_error(err_line);
                debug!(
                    bead_id = BEAD_ID,
                    stmt_idx = i,
                    category = %category,
                    err = %err_line.trim(),
                    "patching oracle result with stderr error (fallback)"
                );
                *result = OpResult::Error {
                    category,
                    message: err_line.trim().to_string(),
                };
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Fixture runner + harness
// ---------------------------------------------------------------------------

/// Report from running a single fixture through the oracle harness.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FixtureReport {
    /// Fixture identifier.
    pub fixture_id: String,
    /// Whether the fixture passed (all ops match or annotated divergence).
    pub passed: bool,
    /// Per-operation comparison outcomes.
    pub outcomes: Vec<CompareOutcome>,
    /// If an annotated divergence was triggered, its details.
    pub divergence: Option<DivergenceAnnotation>,
    /// Detailed diff for any mismatches (for CI reporting).
    #[serde(default)]
    pub diffs: Vec<String>,
}

/// Load a fixture from a JSON file.
pub fn load_fixture(path: &Path) -> Result<TestFixture> {
    let bytes = std::fs::read(path).map_err(|err| {
        FrankenError::Internal(format!("failed to read fixture {}: {err}", path.display()))
    })?;
    let fixture: TestFixture = serde_json::from_slice(&bytes).map_err(|err| {
        FrankenError::Internal(format!("failed to parse fixture {}: {err}", path.display()))
    })?;
    Ok(fixture)
}

/// Load all fixtures from a directory.
pub fn load_fixtures_from_dir(dir: &Path) -> Result<Vec<TestFixture>> {
    let mut fixtures = Vec::new();
    if !dir.is_dir() {
        return Err(FrankenError::Internal(format!(
            "fixture directory does not exist: {}",
            dir.display()
        )));
    }
    let mut entries: Vec<_> = std::fs::read_dir(dir)
        .map_err(|err| FrankenError::Internal(format!("failed to read fixture directory: {err}")))?
        .filter_map(std::result::Result::ok)
        .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "json"))
        .collect();
    entries.sort_by_key(std::fs::DirEntry::path);
    for entry in entries {
        fixtures.push(load_fixture(&entry.path())?);
    }
    info!(
        bead_id = BEAD_ID,
        dir = %dir.display(),
        count = fixtures.len(),
        "loaded fixtures"
    );
    Ok(fixtures)
}

/// Run a fixture against the C sqlite3 oracle only (for golden output generation).
pub fn run_fixture_oracle_only(
    sqlite3_path: &Path,
    fixture: &TestFixture,
) -> Result<Vec<OpResult>> {
    let sql_stmts: Vec<&str> = fixture
        .ops
        .iter()
        .filter_map(|op| match op {
            FixtureOp::Exec { sql, .. } | FixtureOp::Query { sql, .. } => Some(sql.as_str()),
            FixtureOp::Open { .. } => None,
        })
        .collect();

    let db_path = fixture
        .ops
        .iter()
        .find_map(|op| {
            if let FixtureOp::Open { path } = op {
                Some(path.as_str())
            } else {
                None
            }
        })
        .unwrap_or(":memory:");

    run_sqlite3_oracle(sqlite3_path, db_path, &sql_stmts)
}

/// Compare oracle results against expected values in a fixture.
///
/// Returns a `FixtureReport` with pass/fail status.
#[allow(clippy::too_many_lines)]
pub fn compare_fixture_against_oracle(
    fixture: &TestFixture,
    oracle_results: &[OpResult],
) -> FixtureReport {
    let sql_ops: Vec<&FixtureOp> = fixture
        .ops
        .iter()
        .filter(|op| !matches!(op, FixtureOp::Open { .. }))
        .collect();

    let mut outcomes = Vec::new();
    let mut diffs = Vec::new();
    let mut all_match = true;

    for (i, (op, oracle_result)) in sql_ops.iter().zip(oracle_results.iter()).enumerate() {
        let outcome = match op {
            FixtureOp::Exec {
                sql,
                expect_error: Some(expected_cat),
            } => match oracle_result {
                OpResult::Error { category, .. } if category == expected_cat => {
                    CompareOutcome::Match
                }
                _ => CompareOutcome::Mismatch {
                    detail: format!(
                        "op {i}: expected error {expected_cat} for '{sql}', got {oracle_result:?}"
                    ),
                },
            },
            FixtureOp::Exec {
                expect_error: None, ..
            } => match oracle_result {
                OpResult::Ok => CompareOutcome::Match,
                _ => CompareOutcome::Mismatch {
                    detail: format!("op {i}: expected success, got {oracle_result:?}"),
                },
            },
            FixtureOp::Query { sql, expect } => {
                if let Some(expected_cat) = &expect.expect_error {
                    match oracle_result {
                        OpResult::Error { category, .. } if category == expected_cat => {
                            CompareOutcome::Match
                        }
                        _ => CompareOutcome::Mismatch {
                            detail: format!(
                                "op {i}: expected error {expected_cat} for '{sql}', got {oracle_result:?}"
                            ),
                        },
                    }
                } else if let OpResult::Rows { rows, .. } = oracle_result {
                    if !expect.rows.is_empty() {
                        let expected = OpResult::Rows {
                            columns: expect.columns.clone(),
                            rows: expect.rows.clone(),
                        };
                        compare_op_results(&expected, oracle_result, expect.ordered)
                    } else if let Some(expected_count) = expect.row_count {
                        if rows.len() == expected_count {
                            CompareOutcome::Match
                        } else {
                            CompareOutcome::Mismatch {
                                detail: format!(
                                    "op {i}: expected {expected_count} rows, got {}",
                                    rows.len()
                                ),
                            }
                        }
                    } else {
                        CompareOutcome::Match
                    }
                } else {
                    CompareOutcome::Mismatch {
                        detail: format!(
                            "op {i}: expected query results for '{sql}', got {oracle_result:?}"
                        ),
                    }
                }
            }
            FixtureOp::Open { .. } => CompareOutcome::Match,
        };

        if let CompareOutcome::Mismatch { ref detail } = outcome {
            all_match = false;
            diffs.push(detail.clone());
            warn!(
                bead_id = BEAD_ID,
                fixture_id = %fixture.id,
                op_index = i,
                detail = %detail,
                "fixture comparison mismatch"
            );
        }
        outcomes.push(outcome);
    }

    // Annotated divergences turn mismatches into passes.
    let passed = if all_match {
        true
    } else if fixture.divergence.is_some() {
        info!(
            bead_id = BEAD_ID,
            fixture_id = %fixture.id,
            "mismatch accepted due to divergence annotation"
        );
        true
    } else {
        error!(
            bead_id = BEAD_ID,
            fixture_id = %fixture.id,
            diffs = ?diffs,
            "UNANNOTATED divergence — this blocks CI"
        );
        false
    };

    FixtureReport {
        fixture_id: fixture.id.clone(),
        passed,
        outcomes,
        divergence: fixture.divergence.clone(),
        diffs,
    }
}

/// Aggregate report from running an entire fixture suite.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SuiteReport {
    /// Total fixtures executed.
    pub total: usize,
    /// Fixtures that passed.
    pub passed: usize,
    /// Fixtures that failed (unannotated divergences).
    pub failed: usize,
    /// Fixtures skipped due to mode filtering.
    pub skipped: usize,
    /// Annotated divergences encountered.
    pub divergences: usize,
    /// Individual fixture reports.
    pub reports: Vec<FixtureReport>,
}

impl SuiteReport {
    /// Whether the entire suite passed (for CI gating).
    #[must_use]
    pub fn all_passed(&self) -> bool {
        self.failed == 0
    }
}

/// Run a full fixture suite against the C sqlite3 oracle.
///
/// Applies mode filtering: fixtures not applicable to the given mode are skipped.
pub fn run_suite(
    sqlite3_path: &Path,
    fixtures: &[TestFixture],
    mode: FsqliteMode,
) -> Result<SuiteReport> {
    let mut reports = Vec::new();
    let mut passed = 0_usize;
    let mut failed = 0_usize;
    let mut skipped = 0_usize;
    let mut divergences = 0_usize;

    info!(
        bead_id = BEAD_ID,
        mode = ?mode,
        fixture_count = fixtures.len(),
        "starting oracle suite run"
    );

    for fixture in fixtures {
        if !fixture.fsqlite_modes.contains(&mode) {
            debug!(
                bead_id = BEAD_ID,
                fixture_id = %fixture.id,
                required_modes = ?fixture.fsqlite_modes,
                current_mode = ?mode,
                "skipping fixture due to mode filter"
            );
            skipped += 1;
            continue;
        }

        let oracle_results = run_fixture_oracle_only(sqlite3_path, fixture)?;
        let report = compare_fixture_against_oracle(fixture, &oracle_results);

        if report.passed {
            passed += 1;
        } else {
            failed += 1;
        }
        if report.divergence.is_some() {
            divergences += 1;
        }
        reports.push(report);
    }

    let total = passed + failed + skipped;
    info!(
        bead_id = BEAD_ID,
        total = total,
        passed = passed,
        failed = failed,
        skipped = skipped,
        divergences = divergences,
        "oracle suite run complete"
    );

    Ok(SuiteReport {
        total,
        passed,
        failed,
        skipped,
        divergences,
        reports,
    })
}

// ---------------------------------------------------------------------------
// SLT (SQLite Logic Test) ingestion
// ---------------------------------------------------------------------------

/// A parsed SLT (SQLite Logic Test) entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SltEntry {
    /// "statement" or "query"
    pub kind: SltKind,
    /// SQL text
    pub sql: String,
    /// Expected result type string (e.g., "ok", "error", column types like "III")
    pub result_type: String,
    /// Expected output lines
    pub expected: Vec<String>,
}

/// SLT entry kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SltKind {
    Statement,
    Query,
    Halt,
}

/// Parse an SLT file into entries.
///
/// SLT format is line-oriented:
/// ```text
/// statement ok
/// CREATE TABLE t1(a INTEGER, b TEXT)
///
/// query III nosort
/// SELECT 1, 2, 3
/// ----
/// 1|2|3
/// ```
pub fn parse_slt(content: &str) -> Vec<SltEntry> {
    let mut entries = Vec::new();
    let lines: Vec<&str> = content.lines().collect();
    let mut i = 0;

    while i < lines.len() {
        let line = lines[i].trim();

        if line.starts_with("statement") {
            let result_type = line
                .strip_prefix("statement")
                .unwrap_or("")
                .trim()
                .to_string();
            i += 1;
            let mut sql_lines = Vec::new();
            while i < lines.len() && !lines[i].trim().is_empty() {
                sql_lines.push(lines[i]);
                i += 1;
            }
            entries.push(SltEntry {
                kind: SltKind::Statement,
                sql: sql_lines.join("\n"),
                result_type,
                expected: Vec::new(),
            });
        } else if line.starts_with("query") {
            let result_type = line.strip_prefix("query").unwrap_or("").trim().to_string();
            i += 1;
            let mut sql_lines = Vec::new();
            while i < lines.len() && lines[i].trim() != "----" && !lines[i].trim().is_empty() {
                sql_lines.push(lines[i]);
                i += 1;
            }
            // Skip the "----" separator
            if i < lines.len() && lines[i].trim() == "----" {
                i += 1;
            }
            // Read expected output
            let mut expected = Vec::new();
            while i < lines.len() && !lines[i].trim().is_empty() {
                expected.push(lines[i].to_string());
                i += 1;
            }
            entries.push(SltEntry {
                kind: SltKind::Query,
                sql: sql_lines.join("\n"),
                result_type,
                expected,
            });
        } else if line.starts_with("halt") {
            entries.push(SltEntry {
                kind: SltKind::Halt,
                sql: String::new(),
                result_type: String::new(),
                expected: Vec::new(),
            });
            break;
        } else {
            i += 1;
        }
    }

    entries
}

/// Convert SLT entries into a `TestFixture`.
#[must_use]
pub fn slt_entries_to_fixture(entries: &[SltEntry], fixture_id: &str) -> TestFixture {
    let mut ops = vec![FixtureOp::Open {
        path: ":memory:".to_string(),
    }];

    for entry in entries {
        match entry.kind {
            SltKind::Statement => {
                let expect_error = if entry.result_type.contains("error") {
                    Some(ErrorCategory::Error)
                } else {
                    None
                };
                ops.push(FixtureOp::Exec {
                    sql: entry.sql.clone(),
                    expect_error,
                });
            }
            SltKind::Query => {
                let expect = if entry.expected.is_empty() {
                    QueryExpectation::default()
                } else {
                    let rows: Vec<Vec<String>> = entry
                        .expected
                        .iter()
                        .map(|line| line.split('|').map(String::from).collect())
                        .collect();
                    QueryExpectation {
                        rows,
                        ordered: entry.result_type.contains("rowsort")
                            || entry.result_type.contains("nosort"),
                        ..QueryExpectation::default()
                    }
                };
                ops.push(FixtureOp::Query {
                    sql: entry.sql.clone(),
                    expect,
                });
            }
            SltKind::Halt => break,
        }
    }

    TestFixture {
        id: fixture_id.to_string(),
        description: format!("Converted from SLT: {fixture_id}"),
        ops,
        fsqlite_modes: vec![FsqliteMode::Compatibility, FsqliteMode::Native],
        divergence: None,
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_BEAD_ID: &str = "bd-1daa";

    #[test]
    fn test_oracle_comparison_exact_match() {
        let oracle = OpResult::Rows {
            columns: vec!["a".into(), "b".into()],
            rows: vec![vec!["1".into(), "hello".into()]],
        };
        let franken = OpResult::Rows {
            columns: vec!["a".into(), "b".into()],
            rows: vec![vec!["1".into(), "hello".into()]],
        };
        assert_eq!(
            compare_op_results(&oracle, &franken, true),
            CompareOutcome::Match,
            "bead_id={TEST_BEAD_ID} exact match should pass"
        );
    }

    #[test]
    fn test_oracle_comparison_float_tolerance() {
        // Values within 1e-12 relative tolerance should match.
        assert!(
            floats_match("0.333333333333333", "0.333333333333334"),
            "bead_id={TEST_BEAD_ID} floats within tolerance should match"
        );
        // Values outside tolerance should not match.
        assert!(
            !floats_match("1.0", "2.0"),
            "bead_id={TEST_BEAD_ID} floats outside tolerance should not match"
        );
        // Exact equality always matches.
        assert!(
            floats_match("42.0", "42.0"),
            "bead_id={TEST_BEAD_ID} exact float equality"
        );
    }

    #[test]
    fn test_oracle_comparison_unordered_multiset() {
        let oracle = OpResult::Rows {
            columns: vec!["x".into()],
            rows: vec![vec!["3".into()], vec!["1".into()], vec!["2".into()]],
        };
        let franken = OpResult::Rows {
            columns: vec!["x".into()],
            rows: vec![vec!["1".into()], vec!["2".into()], vec!["3".into()]],
        };
        // Unordered comparison (ordered=false) should match.
        assert_eq!(
            compare_op_results(&oracle, &franken, false),
            CompareOutcome::Match,
            "bead_id={TEST_BEAD_ID} unordered multiset comparison"
        );
        // Ordered comparison should NOT match.
        assert!(
            matches!(
                compare_op_results(&oracle, &franken, true),
                CompareOutcome::Mismatch { .. }
            ),
            "bead_id={TEST_BEAD_ID} ordered comparison should fail for different order"
        );
    }

    #[test]
    fn test_oracle_comparison_error_code_match() {
        let oracle = OpResult::Error {
            category: ErrorCategory::Constraint,
            message: "UNIQUE constraint failed: users.email".into(),
        };
        let franken = OpResult::Error {
            category: ErrorCategory::Constraint,
            message: "UNIQUE violation: users.email".into(),
        };
        assert_eq!(
            compare_op_results(&oracle, &franken, true),
            CompareOutcome::Match,
            "bead_id={TEST_BEAD_ID} error category match (messages may differ)"
        );

        // Different categories should NOT match.
        let franken_wrong = OpResult::Error {
            category: ErrorCategory::Error,
            message: "some error".into(),
        };
        assert!(
            matches!(
                compare_op_results(&oracle, &franken_wrong, true),
                CompareOutcome::Mismatch { .. }
            ),
            "bead_id={TEST_BEAD_ID} different error categories should mismatch"
        );
    }

    #[test]
    fn test_divergence_annotation_required() {
        let fixture = TestFixture {
            id: "test_divergence".into(),
            description: "test divergence detection".into(),
            ops: vec![
                FixtureOp::Open {
                    path: ":memory:".into(),
                },
                FixtureOp::Query {
                    sql: "SELECT 1".into(),
                    expect: QueryExpectation {
                        rows: vec![vec!["999".into()]],
                        ordered: true,
                        ..QueryExpectation::default()
                    },
                },
            ],
            fsqlite_modes: vec![FsqliteMode::Compatibility],
            divergence: None,
        };

        // Oracle returns "1" but fixture expects "999" — mismatch without annotation.
        let oracle_results = vec![OpResult::Rows {
            columns: vec!["1".into()],
            rows: vec![vec!["1".into()]],
        }];
        let report = compare_fixture_against_oracle(&fixture, &oracle_results);
        assert!(
            !report.passed,
            "bead_id={TEST_BEAD_ID} unannotated divergence must fail CI"
        );
        assert!(
            !report.diffs.is_empty(),
            "bead_id={TEST_BEAD_ID} diffs must be reported for unannotated divergence"
        );

        // Same fixture WITH divergence annotation should pass.
        let annotated = TestFixture {
            divergence: Some(DivergenceAnnotation {
                description: "intentional: MVCC returns different result".into(),
                rationale: "MVCC provides stronger isolation".into(),
                spec_ref: "§2.4".into(),
            }),
            ..fixture
        };
        let report = compare_fixture_against_oracle(&annotated, &oracle_results);
        assert!(
            report.passed,
            "bead_id={TEST_BEAD_ID} annotated divergence should pass"
        );
    }

    #[test]
    fn test_slt_ingestion_basic() {
        let slt_content = "\
statement ok
CREATE TABLE t1(a INTEGER, b TEXT)

statement ok
INSERT INTO t1 VALUES(1, 'hello')

query IT nosort
SELECT a, b FROM t1
----
1|hello

statement ok
INSERT INTO t1 VALUES(2, 'world')

query I nosort
SELECT a FROM t1 ORDER BY a
----
1
2
";
        let entries = parse_slt(slt_content);
        assert_eq!(
            entries.len(),
            5,
            "bead_id={TEST_BEAD_ID} expected 5 SLT entries"
        );
        assert_eq!(entries[0].kind, SltKind::Statement);
        assert!(entries[0].sql.contains("CREATE TABLE"));
        assert_eq!(entries[2].kind, SltKind::Query);
        assert_eq!(entries[2].expected.len(), 1);
        assert_eq!(entries[2].expected[0], "1|hello");

        // Convert to fixture.
        let fixture = slt_entries_to_fixture(&entries, "test_slt");
        assert_eq!(fixture.id, "test_slt");
        // 1 open + 5 sql ops = 6 total
        assert_eq!(
            fixture.ops.len(),
            6,
            "bead_id={TEST_BEAD_ID} expected 6 fixture ops (1 open + 5 sql)"
        );
    }

    #[test]
    fn test_fixture_mode_filtering() {
        let compat_only = TestFixture {
            id: "compat_only".into(),
            description: String::new(),
            ops: vec![FixtureOp::Open {
                path: ":memory:".into(),
            }],
            fsqlite_modes: vec![FsqliteMode::Compatibility],
            divergence: None,
        };
        assert!(
            compat_only
                .fsqlite_modes
                .contains(&FsqliteMode::Compatibility),
            "bead_id={TEST_BEAD_ID} fixture should include compatibility mode"
        );
        assert!(
            !compat_only.fsqlite_modes.contains(&FsqliteMode::Native),
            "bead_id={TEST_BEAD_ID} fixture should not include native mode"
        );
    }

    #[test]
    fn test_error_category_classification() {
        assert_eq!(
            ErrorCategory::from_sqlite_error("UNIQUE constraint failed: t.x"),
            ErrorCategory::Constraint
        );
        assert_eq!(
            ErrorCategory::from_sqlite_error("database is locked"),
            ErrorCategory::Locked
        );
        assert_eq!(
            ErrorCategory::from_sqlite_error("database or disk is full"),
            ErrorCategory::Full
        );
        assert_eq!(
            ErrorCategory::from_sqlite_error("database table is locked"),
            ErrorCategory::Locked
        );
        assert_eq!(
            ErrorCategory::from_sqlite_error("near \"FOO\": syntax error"),
            ErrorCategory::Error
        );
    }

    #[test]
    fn test_normalize_value() {
        assert_eq!(normalize_value("  hello  "), "hello");
        assert_eq!(normalize_value("NULL"), "NULL");
        assert_eq!(normalize_value("null"), "NULL");
        assert_eq!(normalize_value(""), "NULL");
    }

    #[test]
    fn test_fixture_roundtrip_json() {
        let fixture = TestFixture {
            id: "roundtrip_test".into(),
            description: "test JSON roundtrip".into(),
            ops: vec![
                FixtureOp::Open {
                    path: ":memory:".into(),
                },
                FixtureOp::Exec {
                    sql: "CREATE TABLE t(a INT)".into(),
                    expect_error: None,
                },
                FixtureOp::Query {
                    sql: "SELECT * FROM t".into(),
                    expect: QueryExpectation {
                        columns: vec!["a".into()],
                        rows: Vec::new(),
                        row_count: Some(0),
                        ordered: false,
                        expect_error: None,
                    },
                },
            ],
            fsqlite_modes: vec![FsqliteMode::Compatibility, FsqliteMode::Native],
            divergence: None,
        };

        let json = serde_json::to_string_pretty(&fixture).expect("serialize");
        let deserialized: TestFixture = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(
            fixture, deserialized,
            "bead_id={TEST_BEAD_ID} fixture JSON roundtrip must be lossless"
        );
    }

    #[test]
    fn test_find_sqlite3_binary() {
        // This test may fail in environments without sqlite3 installed.
        let result = find_sqlite3_binary();
        if let Ok(path) = &result {
            assert!(
                path.is_file(),
                "bead_id={TEST_BEAD_ID} sqlite3 binary should exist at {path:?}"
            );
        }
    }

    #[test]
    fn test_oracle_execution_simple() {
        let Ok(sqlite3_path) = find_sqlite3_binary() else {
            eprintln!("skipping: sqlite3 binary not found");
            return;
        };

        let results = run_sqlite3_oracle(
            &sqlite3_path,
            ":memory:",
            &[
                "CREATE TABLE t(a INTEGER, b TEXT)",
                "INSERT INTO t VALUES(1, 'hello')",
                "SELECT a, b FROM t",
            ],
        )
        .expect("oracle execution should succeed");

        assert_eq!(
            results.len(),
            3,
            "bead_id={TEST_BEAD_ID} expected 3 results"
        );
        assert_eq!(
            results[0],
            OpResult::Ok,
            "bead_id={TEST_BEAD_ID} CREATE TABLE"
        );
        assert_eq!(results[1], OpResult::Ok, "bead_id={TEST_BEAD_ID} INSERT");
        if let OpResult::Rows { ref rows, .. } = results[2] {
            assert_eq!(
                rows.len(),
                1,
                "bead_id={TEST_BEAD_ID} SELECT should return 1 row"
            );
            assert_eq!(
                rows[0].len(),
                2,
                "bead_id={TEST_BEAD_ID} SELECT should return 2 columns"
            );
        } else {
            panic!(
                "bead_id={TEST_BEAD_ID} SELECT should return rows, got {:?}",
                results[2]
            );
        }
    }

    #[test]
    fn test_oracle_error_detection() {
        let Ok(sqlite3_path) = find_sqlite3_binary() else {
            eprintln!("skipping: sqlite3 binary not found");
            return;
        };

        let results = run_sqlite3_oracle(
            &sqlite3_path,
            ":memory:",
            &[
                "CREATE TABLE t(a INTEGER UNIQUE)",
                "INSERT INTO t VALUES(1)",
                "INSERT INTO t VALUES(1)", // UNIQUE violation
            ],
        )
        .expect("oracle execution should succeed");

        assert_eq!(results.len(), 3);
        // Third statement should be a constraint error.
        if let OpResult::Error { category, .. } = &results[2] {
            assert_eq!(
                *category,
                ErrorCategory::Constraint,
                "bead_id={TEST_BEAD_ID} UNIQUE violation should be Constraint category"
            );
        } else {
            panic!(
                "bead_id={TEST_BEAD_ID} expected error for duplicate insert, got {:?}",
                results[2]
            );
        }
    }

    #[test]
    fn test_suite_report_all_passed() {
        let report = SuiteReport {
            total: 3,
            passed: 2,
            failed: 0,
            skipped: 1,
            divergences: 0,
            reports: Vec::new(),
        };
        assert!(report.all_passed());
    }

    #[test]
    fn test_suite_report_has_failures() {
        let report = SuiteReport {
            total: 3,
            passed: 1,
            failed: 1,
            skipped: 1,
            divergences: 0,
            reports: Vec::new(),
        };
        assert!(!report.all_passed());
    }

    #[test]
    fn test_e2e_conformance_fixtures_pass_oracle() {
        let Ok(sqlite3_path) = find_sqlite3_binary() else {
            eprintln!("skipping: sqlite3 binary not found");
            return;
        };

        let fixture_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("conformance");
        if !fixture_dir.is_dir() {
            eprintln!("skipping: conformance directory not found");
            return;
        }

        let fixtures =
            load_fixtures_from_dir(&fixture_dir).expect("should load conformance fixtures");
        assert!(
            fixtures.len() >= 10,
            "bead_id={TEST_BEAD_ID} expected at least 10 conformance fixtures, got {}",
            fixtures.len()
        );

        let report = run_suite(&sqlite3_path, &fixtures, FsqliteMode::Compatibility)
            .expect("suite should run");
        assert!(
            report.all_passed(),
            "bead_id={TEST_BEAD_ID} conformance suite failed: {} passed, {} failed, diffs: {:?}",
            report.passed,
            report.failed,
            report
                .reports
                .iter()
                .filter(|r| !r.passed)
                .flat_map(|r| r.diffs.iter())
                .collect::<Vec<_>>()
        );
    }
}
