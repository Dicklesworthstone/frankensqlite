//! Automatic mismatch minimizer and canonical signature pipeline (bd-1dp9.2.3).
//!
//! Given a failing differential test (a [`DifferentialResult`] or
//! [`MetamorphicTestCase`] that diverges), this module:
//!
//! 1. **Minimizes** the workload to the smallest subset of SQL statements
//!    that still reproduces the divergence (delta debugging).
//! 2. **Extracts a canonical signature** from the minimal reproduction to
//!    enable deduplication of repeated failures.
//! 3. **Classifies** and **triages** minimized failures by subsystem and
//!    severity.
//!
//! # Minimization Strategy
//!
//! The minimizer uses a binary-search delta-debugging algorithm:
//!
//! 1. **Binary partition**: split the workload in half and test each half.
//! 2. **Recursive narrowing**: if one half still fails, recurse into it.
//! 3. **1-minimal**: try removing each remaining statement individually.
//! 4. **Schema preservation**: schema setup statements are never removed
//!    (they define the tables/indexes required by the workload).
//!
//! # Canonical Signatures
//!
//! A [`MismatchSignature`] is a content-addressed fingerprint of a minimal
//! divergence. It captures:
//! - The mismatch classification
//! - The subsystem attribution (parser, planner, VDBE, storage, etc.)
//! - A hash of the minimal SQL + schema
//!
//! Signatures enable deduplication: if two failures produce the same
//! signature, they are the same root-cause bug.
//!
//! # Determinism
//!
//! All operations are deterministic given the same input. Hashes use
//! SHA-256 truncated to 16 hex characters for readability.

use std::collections::BTreeSet;
use std::fmt;
use std::fmt::Write as _;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::differential_v2::StatementDivergence;
use crate::metamorphic::MismatchClassification;

/// Bead identifier for log correlation.
#[allow(dead_code)]
const BEAD_ID: &str = "bd-1dp9.2.3";

/// Schema version for the minimizer output format.
pub const MINIMIZER_SCHEMA_VERSION: u32 = 1;

// ===========================================================================
// Subsystem Attribution
// ===========================================================================

/// Likely subsystem responsible for a divergence.
///
/// Attribution is heuristic-based: the minimizer inspects the failing SQL
/// and divergence pattern to guess which FrankenSQLite subsystem is at fault.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Subsystem {
    /// SQL parser or tokenizer.
    Parser,
    /// Name resolver or schema lookup.
    Resolver,
    /// Query planner or optimizer.
    Planner,
    /// VDBE bytecode compiler or VM execution.
    Vdbe,
    /// B-tree or page-level storage.
    Storage,
    /// WAL, pager, or checkpoint logic.
    Wal,
    /// MVCC version chain or conflict detection.
    Mvcc,
    /// Built-in function implementation.
    Functions,
    /// Extension module (FTS, JSON, R-tree, etc.).
    Extension,
    /// Type system, affinity, or collation.
    TypeSystem,
    /// PRAGMA handling.
    Pragma,
    /// Unknown or cross-cutting.
    Unknown,
}

impl fmt::Display for Subsystem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Parser => write!(f, "parser"),
            Self::Resolver => write!(f, "resolver"),
            Self::Planner => write!(f, "planner"),
            Self::Vdbe => write!(f, "vdbe"),
            Self::Storage => write!(f, "storage"),
            Self::Wal => write!(f, "wal"),
            Self::Mvcc => write!(f, "mvcc"),
            Self::Functions => write!(f, "functions"),
            Self::Extension => write!(f, "extension"),
            Self::TypeSystem => write!(f, "type_system"),
            Self::Pragma => write!(f, "pragma"),
            Self::Unknown => write!(f, "unknown"),
        }
    }
}

// ===========================================================================
// Canonical Signature
// ===========================================================================

/// A content-addressed fingerprint of a minimal divergence.
///
/// Two failures with the same signature are considered duplicates of the
/// same root-cause bug. The signature is stable across runs as long as
/// the minimized SQL and classification are identical.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct MismatchSignature {
    /// Truncated SHA-256 of the canonical minimal reproduction.
    pub hash: String,
    /// Mismatch classification.
    pub classification: MismatchClassification,
    /// Attributed subsystem.
    pub subsystem: Subsystem,
    /// Number of statements in the minimal reproduction.
    pub minimal_statement_count: usize,
    /// First diverging SQL statement (for human readability).
    pub first_diverging_sql: String,
}

impl MismatchSignature {
    /// Compute a signature from a minimal reproduction.
    #[must_use]
    pub fn compute(
        schema: &[String],
        minimal_workload: &[String],
        classification: &MismatchClassification,
        subsystem: Subsystem,
        first_divergence: Option<&StatementDivergence>,
    ) -> Self {
        // Build canonical content for hashing.
        let mut hasher = Sha256::new();
        hasher.update(b"sig-v1:");
        hasher.update(classification.to_string().as_bytes());
        hasher.update(b":");
        hasher.update(subsystem.to_string().as_bytes());
        hasher.update(b":");
        for stmt in schema {
            hasher.update(stmt.as_bytes());
            hasher.update(b"\n");
        }
        hasher.update(b"---workload---\n");
        for stmt in minimal_workload {
            hasher.update(stmt.as_bytes());
            hasher.update(b"\n");
        }
        let digest = hasher.finalize();
        let hash = hex_encode_truncated(&digest, 16);

        let first_diverging_sql = first_divergence.map(|d| d.sql.clone()).unwrap_or_default();

        Self {
            hash,
            classification: classification.clone(),
            subsystem,
            minimal_statement_count: minimal_workload.len(),
            first_diverging_sql,
        }
    }
}

impl fmt::Display for MismatchSignature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "SIG-{}/{}/{}stmts",
            self.hash, self.subsystem, self.minimal_statement_count
        )
    }
}

// ===========================================================================
// Minimized Reproduction
// ===========================================================================

/// A minimal reproduction of a differential failure.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MinimalReproduction {
    /// Schema version.
    pub schema_version: u32,
    /// Canonical signature for deduplication.
    pub signature: MismatchSignature,
    /// Original envelope seed.
    pub original_seed: u64,
    /// Schema setup SQL (preserved from original).
    pub schema: Vec<String>,
    /// Minimal workload that reproduces the divergence.
    pub minimal_workload: Vec<String>,
    /// Original workload size.
    pub original_workload_size: usize,
    /// Reduction ratio: `1 - (minimal / original)`.
    pub reduction_ratio: f64,
    /// Index of the first diverging statement in the minimal workload.
    pub first_divergence_index: Option<usize>,
    /// The statement divergences in the minimal reproduction.
    pub divergences: Vec<StatementDivergence>,
    /// Reproduction command.
    pub repro_command: String,
}

impl MinimalReproduction {
    /// Serialize to deterministic JSON.
    ///
    /// # Errors
    ///
    /// Returns `Err` if serialization fails.
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }

    /// Deserialize from JSON.
    ///
    /// # Errors
    ///
    /// Returns `Err` if the JSON is malformed.
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }
}

// ===========================================================================
// Minimizer Configuration
// ===========================================================================

/// Configuration for the mismatch minimizer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MinimizerConfig {
    /// Maximum number of delta-debugging iterations before giving up.
    pub max_iterations: usize,
    /// Whether to attempt 1-minimal reduction (try removing each statement).
    pub one_minimal: bool,
    /// Maximum workload size to attempt minimization on.
    /// Larger workloads skip straight to signature extraction.
    pub max_workload_size: usize,
}

impl Default for MinimizerConfig {
    fn default() -> Self {
        Self {
            max_iterations: 100,
            one_minimal: true,
            max_workload_size: 1000,
        }
    }
}

// ===========================================================================
// Subsystem Attribution Heuristics
// ===========================================================================

/// Attribute a divergence to a likely subsystem based on SQL content and
/// divergence pattern.
#[must_use]
#[allow(clippy::too_many_lines)]
pub fn attribute_subsystem(
    divergences: &[StatementDivergence],
    schema: &[String],
    workload: &[String],
) -> Subsystem {
    // Collect all SQL for keyword analysis.
    let all_sql: String = schema
        .iter()
        .chain(workload.iter())
        .map(String::as_str)
        .collect::<Vec<_>>()
        .join(" ");
    let all_upper = all_sql.to_uppercase();

    // Check for diverging SQL content.
    let diverging_sql: String = divergences
        .iter()
        .map(|d| d.sql.as_str())
        .collect::<Vec<_>>()
        .join(" ");
    let div_upper = diverging_sql.to_uppercase();

    // Extension signals.
    if div_upper.contains("JSON") || div_upper.contains("JSON_") {
        return Subsystem::Extension;
    }
    if div_upper.contains("FTS") || div_upper.contains("MATCH") {
        return Subsystem::Extension;
    }
    if div_upper.contains("RTREE") || div_upper.contains("GEOPOLY") {
        return Subsystem::Extension;
    }

    // Window functions -> VDBE (check before general functions since
    // window functions like ROW_NUMBER also appear in the function list).
    if div_upper.contains("OVER(") || div_upper.contains("OVER (") {
        return Subsystem::Vdbe;
    }

    // Function signals.
    let function_keywords = [
        "ABS(",
        "AVG(",
        "COUNT(",
        "GROUP_CONCAT(",
        "LENGTH(",
        "LOWER(",
        "MAX(",
        "MIN(",
        "SUM(",
        "TOTAL(",
        "TYPEOF(",
        "UNICODE(",
        "ZEROBLOB(",
        "SUBSTR(",
        "REPLACE(",
        "TRIM(",
        "ROUND(",
        "RANDOM(",
        "INSTR(",
        "COALESCE(",
        "IFNULL(",
        "NULLIF(",
        "IIF(",
        "LIKELIHOOD(",
        "LIKELY(",
        "UNLIKELY(",
        "ROW_NUMBER(",
        "RANK(",
        "DENSE_RANK(",
        "NTILE(",
        "LAG(",
        "LEAD(",
        "FIRST_VALUE(",
        "LAST_VALUE(",
        "NTH_VALUE(",
        "CUME_DIST(",
        "PERCENT_RANK(",
    ];
    if function_keywords.iter().any(|kw| div_upper.contains(kw)) {
        return Subsystem::Functions;
    }

    // PRAGMA signals.
    if div_upper.contains("PRAGMA") {
        return Subsystem::Pragma;
    }

    // Type system signals.
    if div_upper.contains("CAST(") || div_upper.contains("TYPEOF(") {
        return Subsystem::TypeSystem;
    }
    if div_upper.contains("COLLATE") {
        return Subsystem::TypeSystem;
    }

    // Storage/WAL signals.
    if all_upper.contains("JOURNAL_MODE") || all_upper.contains("WAL") {
        return Subsystem::Wal;
    }
    if all_upper.contains("VACUUM") || all_upper.contains("INTEGRITY_CHECK") {
        return Subsystem::Storage;
    }

    // Planner signals (index hints, EXPLAIN).
    if div_upper.contains("EXPLAIN") || div_upper.contains("INDEXED BY") {
        return Subsystem::Planner;
    }

    // Complex query structure -> likely planner/VDBE.
    if div_upper.contains("JOIN")
        || div_upper.contains("UNION")
        || div_upper.contains("INTERSECT")
        || div_upper.contains("EXCEPT")
        || div_upper.contains("WITH RECURSIVE")
    {
        return Subsystem::Planner;
    }

    // Simple DML -> VDBE.
    if div_upper.contains("INSERT") || div_upper.contains("UPDATE") || div_upper.contains("DELETE")
    {
        return Subsystem::Vdbe;
    }

    // Simple SELECT -> VDBE.
    if div_upper.contains("SELECT") {
        return Subsystem::Vdbe;
    }

    Subsystem::Unknown
}

// ===========================================================================
// Delta Debugging Minimizer
// ===========================================================================

/// Test function signature for the minimizer.
///
/// Given a schema and workload, returns `true` if the divergence is
/// still reproducible.
pub type ReproducibilityTest = dyn Fn(&[String], &[String]) -> Option<Vec<StatementDivergence>>;

/// Minimize a differential failure to its smallest reproducing workload.
///
/// The `test_fn` is called with `(schema, candidate_workload)` and should
/// return `Some(divergences)` if the failure reproduces, or `None` if it
/// does not.
///
/// Returns `None` if the failure cannot be reproduced even with the full
/// workload, or if the workload is empty.
pub fn minimize_workload(
    schema: &[String],
    workload: &[String],
    config: &MinimizerConfig,
    test_fn: &ReproducibilityTest,
) -> Option<MinimalReproduction> {
    if workload.is_empty() {
        return None;
    }

    // Verify the full workload reproduces.
    let original_divergences = test_fn(schema, workload)?;

    if workload.len() > config.max_workload_size {
        // Too large for delta debugging; just extract signature.
        return Some(build_reproduction(
            schema,
            workload,
            workload,
            &original_divergences,
        ));
    }

    let mut current = workload.to_vec();
    let mut iterations = 0;

    // Phase 1: Binary partition reduction.
    let mut granularity = 2;
    while granularity <= current.len() && iterations < config.max_iterations {
        let chunk_size = current.len().div_ceil(granularity);
        let mut reduced = false;

        for chunk_idx in 0..granularity {
            let start = chunk_idx * chunk_size;
            let end = (start + chunk_size).min(current.len());

            // Try removing this chunk.
            let mut candidate: Vec<String> = Vec::with_capacity(current.len() - (end - start));
            candidate.extend_from_slice(&current[..start]);
            candidate.extend_from_slice(&current[end..]);

            if candidate.is_empty() {
                continue;
            }

            iterations += 1;
            if let Some(_divs) = test_fn(schema, &candidate) {
                current = candidate;
                reduced = true;
                break;
            }
        }

        if reduced {
            // Reset granularity to try larger chunks again.
            granularity = 2;
        } else {
            granularity *= 2;
        }
    }

    // Phase 2: 1-minimal reduction (try removing each statement).
    if config.one_minimal {
        let mut i = 0;
        while i < current.len() && iterations < config.max_iterations {
            let mut candidate = current.clone();
            candidate.remove(i);

            if candidate.is_empty() {
                i += 1;
                continue;
            }

            iterations += 1;
            if test_fn(schema, &candidate).is_some() {
                current = candidate;
                // Don't increment i — the next statement moved into position i.
            } else {
                i += 1;
            }
        }
    }

    // Final verification.
    let final_divergences = test_fn(schema, &current)?;

    Some(build_reproduction(
        schema,
        workload,
        &current,
        &final_divergences,
    ))
}

/// Build a `MinimalReproduction` from the minimized workload.
fn build_reproduction(
    schema: &[String],
    original_workload: &[String],
    minimal_workload: &[String],
    divergences: &[StatementDivergence],
) -> MinimalReproduction {
    let subsystem = attribute_subsystem(divergences, schema, minimal_workload);

    let classification = if divergences.is_empty() {
        MismatchClassification::TrueDivergence {
            description: "empty divergence list".to_owned(),
        }
    } else {
        // Use the first divergence for classification.
        MismatchClassification::TrueDivergence {
            description: format!(
                "statement {} diverged: {}",
                divergences[0].index, divergences[0].sql
            ),
        }
    };

    let signature = MismatchSignature::compute(
        schema,
        minimal_workload,
        &classification,
        subsystem,
        divergences.first(),
    );

    #[allow(clippy::cast_precision_loss)]
    let reduction_ratio = if original_workload.is_empty() {
        0.0
    } else {
        1.0 - (minimal_workload.len() as f64 / original_workload.len() as f64)
    };

    let first_divergence_index = divergences.first().map(|d| d.index);

    let repro_command = format!(
        "# Minimal reproduction ({} statements from original {}):\n{}",
        minimal_workload.len(),
        original_workload.len(),
        minimal_workload.join("\n")
    );

    MinimalReproduction {
        schema_version: MINIMIZER_SCHEMA_VERSION,
        signature,
        original_seed: 0, // Caller should set this.
        schema: schema.to_vec(),
        minimal_workload: minimal_workload.to_vec(),
        original_workload_size: original_workload.len(),
        reduction_ratio,
        first_divergence_index,
        divergences: divergences.to_vec(),
        repro_command,
    }
}

// ===========================================================================
// Deduplication
// ===========================================================================

/// A collection of minimized reproductions, deduplicated by signature.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DeduplicatedFailures {
    /// Unique failures keyed by signature hash.
    pub unique_failures: Vec<MinimalReproduction>,
    /// Total failures before deduplication.
    pub total_before_dedup: usize,
    /// Duplicate count per signature hash.
    pub duplicate_counts: Vec<(String, usize)>,
}

/// Deduplicate a collection of minimized reproductions by signature.
#[must_use]
pub fn deduplicate(reproductions: &[MinimalReproduction]) -> DeduplicatedFailures {
    let mut seen: BTreeSet<String> = BTreeSet::new();
    let mut unique = Vec::new();
    let mut counts: std::collections::BTreeMap<String, usize> = std::collections::BTreeMap::new();

    for repro in reproductions {
        *counts.entry(repro.signature.hash.clone()).or_insert(0) += 1;
        if seen.insert(repro.signature.hash.clone()) {
            unique.push(repro.clone());
        }
    }

    // Sort by triage priority (most actionable first).
    unique.sort_by(|a, b| {
        a.signature
            .classification
            .triage_priority()
            .cmp(&b.signature.classification.triage_priority())
            .then_with(|| a.signature.hash.cmp(&b.signature.hash))
    });

    let duplicate_counts: Vec<(String, usize)> =
        counts.into_iter().filter(|(_, count)| *count > 1).collect();

    DeduplicatedFailures {
        unique_failures: unique,
        total_before_dedup: reproductions.len(),
        duplicate_counts,
    }
}

// ===========================================================================
// Helpers
// ===========================================================================

/// Encode bytes as hex, truncated to `max_chars` characters.
fn hex_encode_truncated(bytes: &[u8], max_chars: usize) -> String {
    let mut s = String::with_capacity(max_chars);
    for byte in bytes {
        if s.len() >= max_chars {
            break;
        }
        let _ = write!(s, "{byte:02x}");
    }
    s.truncate(max_chars);
    s
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::differential_v2::{NormalizedValue, StmtOutcome};

    fn make_divergence(index: usize, sql: &str) -> StatementDivergence {
        StatementDivergence {
            index,
            sql: sql.to_owned(),
            csqlite_outcome: StmtOutcome::Rows(vec![vec![NormalizedValue::Integer(1)]]),
            fsqlite_outcome: StmtOutcome::Rows(vec![vec![NormalizedValue::Integer(2)]]),
        }
    }

    // --- Signature ---

    #[test]
    fn test_signature_deterministic() {
        let schema = vec!["CREATE TABLE t(a INTEGER);".to_owned()];
        let workload = vec!["SELECT a FROM t;".to_owned()];
        let classification = MismatchClassification::TrueDivergence {
            description: "test".to_owned(),
        };

        let sig1 =
            MismatchSignature::compute(&schema, &workload, &classification, Subsystem::Vdbe, None);
        let sig2 =
            MismatchSignature::compute(&schema, &workload, &classification, Subsystem::Vdbe, None);

        assert_eq!(sig1.hash, sig2.hash);
    }

    #[test]
    fn test_signature_differs_on_classification() {
        let schema = vec!["CREATE TABLE t(a INTEGER);".to_owned()];
        let workload = vec!["SELECT a FROM t;".to_owned()];

        let sig1 = MismatchSignature::compute(
            &schema,
            &workload,
            &MismatchClassification::TrueDivergence {
                description: "test".to_owned(),
            },
            Subsystem::Vdbe,
            None,
        );
        let sig2 = MismatchSignature::compute(
            &schema,
            &workload,
            &MismatchClassification::OrderDependentDifference,
            Subsystem::Vdbe,
            None,
        );

        assert_ne!(sig1.hash, sig2.hash);
    }

    #[test]
    fn test_signature_differs_on_subsystem() {
        let schema = vec!["CREATE TABLE t(a INTEGER);".to_owned()];
        let workload = vec!["SELECT a FROM t;".to_owned()];
        let classification = MismatchClassification::TrueDivergence {
            description: "test".to_owned(),
        };

        let sig1 =
            MismatchSignature::compute(&schema, &workload, &classification, Subsystem::Vdbe, None);
        let sig2 = MismatchSignature::compute(
            &schema,
            &workload,
            &classification,
            Subsystem::Parser,
            None,
        );

        assert_ne!(sig1.hash, sig2.hash);
    }

    #[test]
    fn test_signature_display() {
        let sig = MismatchSignature {
            hash: "abcdef0123456789".to_owned(),
            classification: MismatchClassification::TrueDivergence {
                description: "test".to_owned(),
            },
            subsystem: Subsystem::Vdbe,
            minimal_statement_count: 3,
            first_diverging_sql: "SELECT 1".to_owned(),
        };

        let display = sig.to_string();
        assert!(display.contains("SIG-"));
        assert!(display.contains("vdbe"));
        assert!(display.contains("3stmts"));
    }

    // --- Subsystem attribution ---

    #[test]
    fn test_attribute_json_to_extension() {
        let divs = vec![make_divergence(
            0,
            "SELECT json_extract(data, '$.a') FROM t",
        )];
        let subsystem = attribute_subsystem(
            &divs,
            &[],
            &["SELECT json_extract(data, '$.a') FROM t".to_owned()],
        );
        assert_eq!(subsystem, Subsystem::Extension);
    }

    #[test]
    fn test_attribute_pragma_to_pragma() {
        let divs = vec![make_divergence(0, "PRAGMA table_info(t)")];
        let subsystem = attribute_subsystem(&divs, &[], &["PRAGMA table_info(t)".to_owned()]);
        assert_eq!(subsystem, Subsystem::Pragma);
    }

    #[test]
    fn test_attribute_window_to_vdbe() {
        let divs = vec![make_divergence(
            0,
            "SELECT row_number() OVER (ORDER BY a) FROM t",
        )];
        let subsystem = attribute_subsystem(
            &divs,
            &[],
            &["SELECT row_number() OVER (ORDER BY a) FROM t".to_owned()],
        );
        assert_eq!(subsystem, Subsystem::Vdbe);
    }

    #[test]
    fn test_attribute_join_to_planner() {
        let divs = vec![make_divergence(
            0,
            "SELECT * FROM t1 JOIN t2 ON t1.a = t2.b",
        )];
        let subsystem = attribute_subsystem(
            &divs,
            &[],
            &["SELECT * FROM t1 JOIN t2 ON t1.a = t2.b".to_owned()],
        );
        assert_eq!(subsystem, Subsystem::Planner);
    }

    #[test]
    fn test_attribute_function_to_functions() {
        let divs = vec![make_divergence(0, "SELECT ABS(-5)")];
        let subsystem = attribute_subsystem(&divs, &[], &["SELECT ABS(-5)".to_owned()]);
        assert_eq!(subsystem, Subsystem::Functions);
    }

    #[test]
    fn test_attribute_cast_to_type_system() {
        let divs = vec![make_divergence(0, "SELECT CAST(42 AS TEXT)")];
        let subsystem = attribute_subsystem(&divs, &[], &["SELECT CAST(42 AS TEXT)".to_owned()]);
        assert_eq!(subsystem, Subsystem::TypeSystem);
    }

    // --- Delta debugging ---

    #[test]
    fn test_minimize_single_statement() {
        let schema = vec!["CREATE TABLE t(a INTEGER);".to_owned()];
        let workload = vec!["SELECT a FROM t;".to_owned()];
        let config = MinimizerConfig::default();

        let result = minimize_workload(&schema, &workload, &config, &|_s, w| {
            if w.iter().any(|s| s.contains("SELECT")) {
                Some(vec![make_divergence(0, "SELECT a FROM t;")])
            } else {
                None
            }
        });

        let repro = result.expect("should produce reproduction");
        assert_eq!(repro.minimal_workload.len(), 1);
        assert!(repro.minimal_workload[0].contains("SELECT"));
    }

    #[test]
    fn test_minimize_removes_non_contributing() {
        let schema = vec!["CREATE TABLE t(a INTEGER);".to_owned()];
        let workload = vec![
            "INSERT INTO t VALUES(1);".to_owned(),
            "INSERT INTO t VALUES(2);".to_owned(),
            "INSERT INTO t VALUES(3);".to_owned(),
            "SELECT a FROM t;".to_owned(), // Only this diverges
        ];
        let config = MinimizerConfig::default();

        let result = minimize_workload(&schema, &workload, &config, &|_s, w| {
            // Only diverge if the SELECT is present
            if w.iter().any(|s| s.contains("SELECT")) {
                Some(vec![make_divergence(w.len() - 1, "SELECT a FROM t;")])
            } else {
                None
            }
        });

        let repro = result.expect("should produce reproduction");
        assert!(
            repro.minimal_workload.len() < workload.len(),
            "should reduce workload from {} statements",
            workload.len()
        );
        assert!(repro.reduction_ratio > 0.0);
    }

    #[test]
    fn test_minimize_preserves_required_statements() {
        let schema = vec!["CREATE TABLE t(a INTEGER);".to_owned()];
        let workload = vec![
            "INSERT INTO t VALUES(1);".to_owned(),
            "INSERT INTO t VALUES(2);".to_owned(),
            "SELECT a FROM t WHERE a = 2;".to_owned(),
        ];
        let config = MinimizerConfig::default();

        let result = minimize_workload(&schema, &workload, &config, &|_s, w| {
            // Need both the INSERT(2) and the SELECT to diverge
            let has_insert_2 = w.iter().any(|s| s.contains("VALUES(2)"));
            let has_select = w.iter().any(|s| s.contains("SELECT"));
            if has_insert_2 && has_select {
                Some(vec![make_divergence(2, "SELECT a FROM t WHERE a = 2;")])
            } else {
                None
            }
        });

        let repro = result.expect("should produce reproduction");
        assert!(repro.minimal_workload.len() >= 2);
        assert!(
            repro
                .minimal_workload
                .iter()
                .any(|s| s.contains("VALUES(2)"))
        );
        assert!(repro.minimal_workload.iter().any(|s| s.contains("SELECT")));
    }

    #[test]
    fn test_minimize_empty_workload() {
        let schema = vec!["CREATE TABLE t(a INTEGER);".to_owned()];
        let workload: Vec<String> = vec![];
        let config = MinimizerConfig::default();

        let result = minimize_workload(&schema, &workload, &config, &|_, _| None);
        assert!(result.is_none());
    }

    #[test]
    fn test_minimize_no_reproduction() {
        let schema = vec!["CREATE TABLE t(a INTEGER);".to_owned()];
        let workload = vec!["SELECT 1;".to_owned()];
        let config = MinimizerConfig::default();

        let result = minimize_workload(&schema, &workload, &config, &|_, _| None);
        assert!(result.is_none());
    }

    // --- Deduplication ---

    #[test]
    fn test_deduplicate_identical_signatures() {
        let schema = vec!["CREATE TABLE t(a);".to_owned()];
        let workload = vec!["SELECT a FROM t;".to_owned()];
        let divs = vec![make_divergence(0, "SELECT a FROM t;")];

        let repro1 = build_reproduction(&schema, &workload, &workload, &divs);
        let repro2 = build_reproduction(&schema, &workload, &workload, &divs);

        let deduped = deduplicate(&[repro1, repro2]);
        assert_eq!(deduped.unique_failures.len(), 1);
        assert_eq!(deduped.total_before_dedup, 2);
        assert_eq!(deduped.duplicate_counts.len(), 1);
        assert_eq!(deduped.duplicate_counts[0].1, 2);
    }

    #[test]
    fn test_deduplicate_different_signatures() {
        let schema = vec!["CREATE TABLE t(a);".to_owned()];
        let workload1 = vec!["SELECT a FROM t;".to_owned()];
        let workload2 = vec!["SELECT a + 1 FROM t;".to_owned()];
        let divs1 = vec![make_divergence(0, "SELECT a FROM t;")];
        let divs2 = vec![make_divergence(0, "SELECT a + 1 FROM t;")];

        let repro1 = build_reproduction(&schema, &workload1, &workload1, &divs1);
        let repro2 = build_reproduction(&schema, &workload2, &workload2, &divs2);

        let deduped = deduplicate(&[repro1, repro2]);
        assert_eq!(deduped.unique_failures.len(), 2);
        assert_eq!(deduped.total_before_dedup, 2);
        assert!(deduped.duplicate_counts.is_empty());
    }

    // --- JSON round-trip ---

    #[test]
    fn test_reproduction_json_roundtrip() {
        let schema = vec!["CREATE TABLE t(a);".to_owned()];
        let workload = vec!["SELECT a FROM t;".to_owned()];
        let divs = vec![make_divergence(0, "SELECT a FROM t;")];

        let repro = build_reproduction(&schema, &workload, &workload, &divs);
        let json = repro.to_json().expect("serialize");
        let restored = MinimalReproduction::from_json(&json).expect("deserialize");

        assert_eq!(restored.signature.hash, repro.signature.hash);
        assert_eq!(restored.minimal_workload, repro.minimal_workload);
    }

    // --- Config defaults ---

    #[test]
    fn test_minimizer_config_defaults() {
        let config = MinimizerConfig::default();
        assert_eq!(config.max_iterations, 100);
        assert!(config.one_minimal);
        assert_eq!(config.max_workload_size, 1000);
    }
}
