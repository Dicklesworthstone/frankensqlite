//! Parser/resolver/PRAGMA semantic closure wave (bd-1dp9.3.2).
//!
//! Structured test infrastructure for identifying and closing semantic gaps
//! in the parser, name resolver, and PRAGMA handling pipeline stages.
//!
//! # Closure Wave Pattern
//!
//! A "closure wave" systematically:
//! 1. Enumerates expected behaviors for a pipeline stage
//! 2. Tests each behavior against both C SQLite and FrankenSQLite
//! 3. Records gaps as structured `ClosureCase` entries
//! 4. Produces a coverage report showing closure progress
//!
//! # Coverage Domains
//!
//! - **Parser**: statement types, expression precedence, error recovery
//! - **Resolver**: column resolution, scope chaining, ambiguity detection
//! - **PRAGMA**: pragma dispatch, argument validation, result format

use std::collections::BTreeMap;
use std::fmt;
use std::fmt::Write as _;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::semantic_gap_map::PipelineStage;

/// Bead identifier for log correlation.
#[allow(dead_code)]
const BEAD_ID: &str = "bd-1dp9.3.2";

/// Schema version.
pub const CLOSURE_WAVE_SCHEMA_VERSION: u32 = 1;

// ===========================================================================
// Closure Case
// ===========================================================================

/// Domain of a closure case.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum ClosureDomain {
    /// Parser: tokenization and AST production.
    Parser,
    /// Resolver: name resolution and scope handling.
    Resolver,
    /// PRAGMA: pragma dispatch and semantics.
    Pragma,
}

impl ClosureDomain {
    /// Map to pipeline stage.
    #[must_use]
    pub const fn pipeline_stage(self) -> PipelineStage {
        match self {
            Self::Parser => PipelineStage::Parse,
            Self::Resolver => PipelineStage::Resolve,
            Self::Pragma => PipelineStage::CrossCutting,
        }
    }

    /// All domains.
    pub const ALL: [Self; 3] = [Self::Parser, Self::Resolver, Self::Pragma];
}

impl fmt::Display for ClosureDomain {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Parser => write!(f, "parser"),
            Self::Resolver => write!(f, "resolver"),
            Self::Pragma => write!(f, "pragma"),
        }
    }
}

/// Outcome of running a closure case.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ClosureOutcome {
    /// Both engines produce matching results.
    Pass,
    /// Engines produce different results.
    Fail,
    /// Case was skipped (e.g., unsupported by test harness).
    Skip,
    /// Case could not be executed (infrastructure error).
    Error,
}

impl fmt::Display for ClosureOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pass => write!(f, "pass"),
            Self::Fail => write!(f, "fail"),
            Self::Skip => write!(f, "skip"),
            Self::Error => write!(f, "error"),
        }
    }
}

/// A single closure test case.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClosureCase {
    /// Unique case identifier (e.g., "PARSE-001").
    pub case_id: String,
    /// Domain this case belongs to.
    pub domain: ClosureDomain,
    /// Human-readable title.
    pub title: String,
    /// SQL to test (schema + workload).
    pub schema_sql: Vec<String>,
    /// SQL workload to execute after schema.
    pub workload_sql: Vec<String>,
    /// Expected behavior description.
    pub expected_behavior: String,
    /// Outcome of the test.
    pub outcome: ClosureOutcome,
    /// Explanation of failure (if outcome is Fail).
    pub failure_detail: Option<String>,
    /// Feature IDs this case covers.
    pub feature_tags: Vec<String>,
    /// Spec section reference.
    pub spec_reference: Option<String>,
}

// ===========================================================================
// Closure Wave Registry
// ===========================================================================

/// Registry of closure cases organized by domain.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ClosureWaveRegistry {
    /// All registered cases.
    pub cases: Vec<ClosureCase>,
}

impl ClosureWaveRegistry {
    /// Create a new empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a case to the registry.
    pub fn add(&mut self, case: ClosureCase) {
        self.cases.push(case);
    }

    /// Get cases for a specific domain.
    #[must_use]
    pub fn cases_for_domain(&self, domain: ClosureDomain) -> Vec<&ClosureCase> {
        self.cases.iter().filter(|c| c.domain == domain).collect()
    }

    /// Count outcomes by type.
    #[must_use]
    pub fn outcome_counts(&self) -> BTreeMap<String, usize> {
        let mut counts = BTreeMap::new();
        for case in &self.cases {
            *counts.entry(case.outcome.to_string()).or_insert(0) += 1;
        }
        counts
    }

    /// Generate a coverage report.
    #[must_use]
    pub fn coverage_report(&self) -> ClosureCoverageReport {
        let mut by_domain: BTreeMap<String, DomainCoverage> = BTreeMap::new();

        for domain in ClosureDomain::ALL {
            let domain_cases = self.cases_for_domain(domain);
            let total = domain_cases.len();
            let passed = domain_cases
                .iter()
                .filter(|c| c.outcome == ClosureOutcome::Pass)
                .count();
            let failed = domain_cases
                .iter()
                .filter(|c| c.outcome == ClosureOutcome::Fail)
                .count();
            let skipped = domain_cases
                .iter()
                .filter(|c| c.outcome == ClosureOutcome::Skip)
                .count();

            #[allow(clippy::cast_precision_loss)]
            let closure_rate = if total == 0 {
                0.0
            } else {
                passed as f64 / total as f64
            };

            by_domain.insert(
                domain.to_string(),
                DomainCoverage {
                    total,
                    passed,
                    failed,
                    skipped,
                    closure_rate,
                },
            );
        }

        let total = self.cases.len();
        let total_passed = self
            .cases
            .iter()
            .filter(|c| c.outcome == ClosureOutcome::Pass)
            .count();

        #[allow(clippy::cast_precision_loss)]
        let overall_closure_rate = if total == 0 {
            0.0
        } else {
            total_passed as f64 / total as f64
        };

        // Compute deterministic hash.
        let mut hasher = Sha256::new();
        hasher.update(b"closure-wave-v1:");
        for case in &self.cases {
            hasher.update(case.case_id.as_bytes());
            hasher.update(b":");
            hasher.update(case.outcome.to_string().as_bytes());
            hasher.update(b"\n");
        }
        let digest = hasher.finalize();
        let mut report_hash = String::with_capacity(16);
        for byte in &digest[..8] {
            let _ = write!(report_hash, "{byte:02x}");
        }

        ClosureCoverageReport {
            schema_version: CLOSURE_WAVE_SCHEMA_VERSION,
            report_hash,
            total_cases: total,
            total_passed,
            overall_closure_rate,
            by_domain,
        }
    }
}

/// Coverage statistics for a single domain.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DomainCoverage {
    /// Total cases.
    pub total: usize,
    /// Cases that passed.
    pub passed: usize,
    /// Cases that failed.
    pub failed: usize,
    /// Cases that were skipped.
    pub skipped: usize,
    /// Closure rate (passed / total).
    pub closure_rate: f64,
}

/// Coverage report for the entire closure wave.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClosureCoverageReport {
    /// Schema version.
    pub schema_version: u32,
    /// Deterministic report hash.
    pub report_hash: String,
    /// Total cases across all domains.
    pub total_cases: usize,
    /// Total passing cases.
    pub total_passed: usize,
    /// Overall closure rate.
    pub overall_closure_rate: f64,
    /// Coverage per domain.
    pub by_domain: BTreeMap<String, DomainCoverage>,
}

impl ClosureCoverageReport {
    /// Serialize to JSON.
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

    /// Human-readable summary line.
    #[must_use]
    pub fn summary_line(&self) -> String {
        format!(
            "Closure wave: {}/{} cases passed ({:.1}%)",
            self.total_passed,
            self.total_cases,
            self.overall_closure_rate * 100.0,
        )
    }
}

// ===========================================================================
// Built-in Closure Case Catalog
// ===========================================================================

/// Build the canonical closure case catalog for parser/resolver/PRAGMA.
///
/// These cases represent the expected SQL behaviors that must match between
/// C SQLite and FrankenSQLite. Each case is initially `Skip` until executed.
#[must_use]
pub fn build_canonical_catalog() -> ClosureWaveRegistry {
    let mut registry = ClosureWaveRegistry::new();

    // --- Parser Cases ---
    add_parser_cases(&mut registry);

    // --- Resolver Cases ---
    add_resolver_cases(&mut registry);

    // --- PRAGMA Cases ---
    add_pragma_cases(&mut registry);

    registry
}

#[allow(clippy::too_many_lines)]
fn add_parser_cases(reg: &mut ClosureWaveRegistry) {
    // Basic statement types.
    let parser_cases = [
        (
            "PARSE-001",
            "SELECT literal",
            vec![],
            vec!["SELECT 1;"],
            "Returns integer 1",
        ),
        (
            "PARSE-002",
            "SELECT with alias",
            vec![],
            vec!["SELECT 1 AS x;"],
            "Returns column named x",
        ),
        (
            "PARSE-003",
            "INSERT VALUES",
            vec!["CREATE TABLE t(a);"],
            vec!["INSERT INTO t VALUES(1);"],
            "Inserts one row",
        ),
        (
            "PARSE-004",
            "UPDATE SET",
            vec!["CREATE TABLE t(a);", "INSERT INTO t VALUES(1);"],
            vec!["UPDATE t SET a = 2;"],
            "Updates row",
        ),
        (
            "PARSE-005",
            "DELETE FROM",
            vec!["CREATE TABLE t(a);", "INSERT INTO t VALUES(1);"],
            vec!["DELETE FROM t;"],
            "Deletes all rows",
        ),
        (
            "PARSE-006",
            "CREATE TABLE IF NOT EXISTS",
            vec![],
            vec!["CREATE TABLE IF NOT EXISTS t(a);"],
            "Creates table or no-op",
        ),
        (
            "PARSE-007",
            "DROP TABLE IF EXISTS",
            vec![],
            vec!["DROP TABLE IF EXISTS t;"],
            "Drops table or no-op",
        ),
        (
            "PARSE-008",
            "CREATE INDEX",
            vec!["CREATE TABLE t(a, b);"],
            vec!["CREATE INDEX idx ON t(a);"],
            "Creates index",
        ),
        (
            "PARSE-009",
            "Nested parenthesized expression",
            vec![],
            vec!["SELECT ((1 + 2) * 3);"],
            "Returns 9",
        ),
        (
            "PARSE-010",
            "CASE WHEN expression",
            vec![],
            vec!["SELECT CASE WHEN 1=1 THEN 'yes' ELSE 'no' END;"],
            "Returns yes",
        ),
        (
            "PARSE-011",
            "BETWEEN expression",
            vec![],
            vec!["SELECT 5 BETWEEN 1 AND 10;"],
            "Returns 1 (true)",
        ),
        (
            "PARSE-012",
            "IN expression with list",
            vec![],
            vec!["SELECT 3 IN (1, 2, 3);"],
            "Returns 1 (true)",
        ),
        (
            "PARSE-013",
            "LIKE expression",
            vec!["CREATE TABLE t(s TEXT);", "INSERT INTO t VALUES('hello');"],
            vec!["SELECT s FROM t WHERE s LIKE 'hel%';"],
            "Returns hello",
        ),
        (
            "PARSE-014",
            "GLOB expression",
            vec!["CREATE TABLE t(s TEXT);", "INSERT INTO t VALUES('hello');"],
            vec!["SELECT s FROM t WHERE s GLOB 'hel*';"],
            "Returns hello",
        ),
        (
            "PARSE-015",
            "Compound SELECT UNION",
            vec![],
            vec!["SELECT 1 UNION SELECT 2;"],
            "Returns 1 and 2",
        ),
        (
            "PARSE-016",
            "Compound SELECT UNION ALL",
            vec![],
            vec!["SELECT 1 UNION ALL SELECT 1;"],
            "Returns 1 twice",
        ),
        (
            "PARSE-017",
            "Compound SELECT INTERSECT",
            vec![],
            vec!["SELECT 1 INTERSECT SELECT 1;"],
            "Returns 1",
        ),
        (
            "PARSE-018",
            "Compound SELECT EXCEPT",
            vec![],
            vec!["SELECT 1 EXCEPT SELECT 2;"],
            "Returns 1",
        ),
        (
            "PARSE-019",
            "Subquery in FROM",
            vec![],
            vec!["SELECT * FROM (SELECT 1 AS a);"],
            "Returns a=1",
        ),
        (
            "PARSE-020",
            "Correlated subquery in WHERE",
            vec![
                "CREATE TABLE t(a);",
                "INSERT INTO t VALUES(1);",
                "INSERT INTO t VALUES(2);",
            ],
            vec!["SELECT a FROM t WHERE a > (SELECT MIN(a) FROM t);"],
            "Returns 2",
        ),
    ];

    for (id, title, schema, workload, expected) in parser_cases {
        reg.add(ClosureCase {
            case_id: id.to_owned(),
            domain: ClosureDomain::Parser,
            title: title.to_owned(),
            schema_sql: schema.iter().map(|s| (*s).to_owned()).collect(),
            workload_sql: workload.iter().map(|s| (*s).to_owned()).collect(),
            expected_behavior: expected.to_owned(),
            outcome: ClosureOutcome::Skip,
            failure_detail: None,
            feature_tags: vec!["F-SQL".to_owned()],
            spec_reference: None,
        });
    }
}

#[allow(clippy::too_many_lines)]
fn add_resolver_cases(reg: &mut ClosureWaveRegistry) {
    let resolver_cases = [
        (
            "RESOLVE-001",
            "Simple column reference",
            vec!["CREATE TABLE t(a, b);", "INSERT INTO t VALUES(1, 2);"],
            vec!["SELECT a FROM t;"],
            "Resolves column a",
        ),
        (
            "RESOLVE-002",
            "Table-qualified column",
            vec!["CREATE TABLE t(a);", "INSERT INTO t VALUES(1);"],
            vec!["SELECT t.a FROM t;"],
            "Resolves t.a",
        ),
        (
            "RESOLVE-003",
            "Alias in SELECT",
            vec!["CREATE TABLE t(a);", "INSERT INTO t VALUES(1);"],
            vec!["SELECT a AS x FROM t;"],
            "Column named x",
        ),
        (
            "RESOLVE-004",
            "Table alias",
            vec!["CREATE TABLE t(a);", "INSERT INTO t VALUES(1);"],
            vec!["SELECT t1.a FROM t AS t1;"],
            "Resolves via alias",
        ),
        (
            "RESOLVE-005",
            "JOIN column resolution",
            vec![
                "CREATE TABLE t1(a);",
                "CREATE TABLE t2(b);",
                "INSERT INTO t1 VALUES(1);",
                "INSERT INTO t2 VALUES(2);",
            ],
            vec!["SELECT t1.a, t2.b FROM t1, t2;"],
            "Resolves both columns",
        ),
        (
            "RESOLVE-006",
            "Ambiguous column error",
            vec!["CREATE TABLE t1(a);", "CREATE TABLE t2(a);"],
            vec!["SELECT a FROM t1, t2;"],
            "Error: ambiguous column",
        ),
        (
            "RESOLVE-007",
            "No such table error",
            vec![],
            vec!["SELECT a FROM nonexistent;"],
            "Error: no such table",
        ),
        (
            "RESOLVE-008",
            "No such column error",
            vec!["CREATE TABLE t(a);"],
            vec!["SELECT z FROM t;"],
            "Error: no such column",
        ),
        (
            "RESOLVE-009",
            "Subquery scope isolation",
            vec!["CREATE TABLE t(a);", "INSERT INTO t VALUES(1);"],
            vec!["SELECT (SELECT a FROM t) AS sub;"],
            "Resolves inner scope",
        ),
        (
            "RESOLVE-010",
            "ORDER BY column index",
            vec![
                "CREATE TABLE t(a, b);",
                "INSERT INTO t VALUES(2, 1);",
                "INSERT INTO t VALUES(1, 2);",
            ],
            vec!["SELECT a, b FROM t ORDER BY 1;"],
            "Orders by first column",
        ),
        (
            "RESOLVE-011",
            "GROUP BY column alias",
            vec![
                "CREATE TABLE t(a, b);",
                "INSERT INTO t VALUES(1, 10);",
                "INSERT INTO t VALUES(1, 20);",
            ],
            vec!["SELECT a, SUM(b) AS total FROM t GROUP BY a;"],
            "Groups by column a",
        ),
        (
            "RESOLVE-012",
            "HAVING with aggregate",
            vec![
                "CREATE TABLE t(a, b);",
                "INSERT INTO t VALUES(1, 10);",
                "INSERT INTO t VALUES(2, 20);",
            ],
            vec!["SELECT a, SUM(b) FROM t GROUP BY a HAVING SUM(b) > 15;"],
            "Filters groups",
        ),
        (
            "RESOLVE-013",
            "Star expansion",
            vec!["CREATE TABLE t(a, b, c);", "INSERT INTO t VALUES(1, 2, 3);"],
            vec!["SELECT * FROM t;"],
            "Expands to a, b, c",
        ),
        (
            "RESOLVE-014",
            "Table.star expansion",
            vec![
                "CREATE TABLE t1(a);",
                "CREATE TABLE t2(b);",
                "INSERT INTO t1 VALUES(1);",
                "INSERT INTO t2 VALUES(2);",
            ],
            vec!["SELECT t1.* FROM t1, t2;"],
            "Expands t1 columns only",
        ),
        (
            "RESOLVE-015",
            "EXISTS subquery",
            vec!["CREATE TABLE t(a);", "INSERT INTO t VALUES(1);"],
            vec!["SELECT EXISTS(SELECT 1 FROM t WHERE a=1);"],
            "Returns 1",
        ),
    ];

    for (id, title, schema, workload, expected) in resolver_cases {
        reg.add(ClosureCase {
            case_id: id.to_owned(),
            domain: ClosureDomain::Resolver,
            title: title.to_owned(),
            schema_sql: schema.iter().map(|s| (*s).to_owned()).collect(),
            workload_sql: workload.iter().map(|s| (*s).to_owned()).collect(),
            expected_behavior: expected.to_owned(),
            outcome: ClosureOutcome::Skip,
            failure_detail: None,
            feature_tags: vec!["F-SQL".to_owned()],
            spec_reference: None,
        });
    }
}

#[allow(clippy::too_many_lines)]
fn add_pragma_cases(reg: &mut ClosureWaveRegistry) {
    let pragma_cases = [
        (
            "PRAGMA-001",
            "PRAGMA table_info",
            vec!["CREATE TABLE t(a INTEGER, b TEXT);"],
            vec!["PRAGMA table_info(t);"],
            "Returns column metadata",
        ),
        (
            "PRAGMA-002",
            "PRAGMA database_list",
            vec![],
            vec!["PRAGMA database_list;"],
            "Returns attached databases",
        ),
        (
            "PRAGMA-003",
            "PRAGMA compile_options",
            vec![],
            vec!["PRAGMA compile_options;"],
            "Returns compile-time options",
        ),
        (
            "PRAGMA-004",
            "PRAGMA journal_mode query",
            vec![],
            vec!["PRAGMA journal_mode;"],
            "Returns current journal mode",
        ),
        (
            "PRAGMA-005",
            "PRAGMA encoding",
            vec![],
            vec!["PRAGMA encoding;"],
            "Returns text encoding",
        ),
        (
            "PRAGMA-006",
            "PRAGMA page_size query",
            vec![],
            vec!["PRAGMA page_size;"],
            "Returns current page size",
        ),
        (
            "PRAGMA-007",
            "PRAGMA cache_size query",
            vec![],
            vec!["PRAGMA cache_size;"],
            "Returns cache size",
        ),
        (
            "PRAGMA-008",
            "PRAGMA integrity_check",
            vec!["CREATE TABLE t(a);", "INSERT INTO t VALUES(1);"],
            vec!["PRAGMA integrity_check;"],
            "Returns ok",
        ),
        (
            "PRAGMA-009",
            "PRAGMA foreign_keys",
            vec![],
            vec!["PRAGMA foreign_keys;"],
            "Returns foreign key enforcement state",
        ),
        (
            "PRAGMA-010",
            "PRAGMA user_version",
            vec![],
            vec!["PRAGMA user_version;"],
            "Returns user version number",
        ),
        (
            "PRAGMA-011",
            "PRAGMA application_id",
            vec![],
            vec!["PRAGMA application_id;"],
            "Returns application ID",
        ),
        (
            "PRAGMA-012",
            "PRAGMA index_list",
            vec!["CREATE TABLE t(a);", "CREATE INDEX idx ON t(a);"],
            vec!["PRAGMA index_list(t);"],
            "Returns index metadata",
        ),
        (
            "PRAGMA-013",
            "PRAGMA index_info",
            vec!["CREATE TABLE t(a, b);", "CREATE INDEX idx ON t(a, b);"],
            vec!["PRAGMA index_info(idx);"],
            "Returns indexed columns",
        ),
        (
            "PRAGMA-014",
            "PRAGMA quick_check",
            vec!["CREATE TABLE t(a);"],
            vec!["PRAGMA quick_check;"],
            "Returns ok",
        ),
        (
            "PRAGMA-015",
            "PRAGMA collation_list",
            vec![],
            vec!["PRAGMA collation_list;"],
            "Returns available collations",
        ),
    ];

    for (id, title, schema, workload, expected) in pragma_cases {
        reg.add(ClosureCase {
            case_id: id.to_owned(),
            domain: ClosureDomain::Pragma,
            title: title.to_owned(),
            schema_sql: schema.iter().map(|s| (*s).to_owned()).collect(),
            workload_sql: workload.iter().map(|s| (*s).to_owned()).collect(),
            expected_behavior: expected.to_owned(),
            outcome: ClosureOutcome::Skip,
            failure_detail: None,
            feature_tags: vec!["F-PRAGMA".to_owned()],
            spec_reference: None,
        });
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_canonical_catalog_size() {
        let catalog = build_canonical_catalog();
        // 20 parser + 15 resolver + 15 pragma = 50
        assert_eq!(catalog.cases.len(), 50);
    }

    #[test]
    fn test_domain_filtering() {
        let catalog = build_canonical_catalog();
        let parser_cases = catalog.cases_for_domain(ClosureDomain::Parser);
        assert_eq!(parser_cases.len(), 20);

        let resolver_cases = catalog.cases_for_domain(ClosureDomain::Resolver);
        assert_eq!(resolver_cases.len(), 15);

        let pragma_cases = catalog.cases_for_domain(ClosureDomain::Pragma);
        assert_eq!(pragma_cases.len(), 15);
    }

    #[test]
    fn test_all_cases_initially_skip() {
        let catalog = build_canonical_catalog();
        assert!(
            catalog
                .cases
                .iter()
                .all(|c| c.outcome == ClosureOutcome::Skip)
        );
    }

    #[test]
    fn test_coverage_report_all_skip() {
        let catalog = build_canonical_catalog();
        let report = catalog.coverage_report();

        assert_eq!(report.total_cases, 50);
        assert_eq!(report.total_passed, 0);
        assert!((report.overall_closure_rate).abs() < f64::EPSILON);
    }

    #[test]
    fn test_coverage_report_with_passes() {
        let mut catalog = build_canonical_catalog();
        // Mark first 5 as passing.
        for case in catalog.cases.iter_mut().take(5) {
            case.outcome = ClosureOutcome::Pass;
        }

        let report = catalog.coverage_report();
        assert_eq!(report.total_passed, 5);
        assert!((report.overall_closure_rate - 0.1).abs() < 0.001);
    }

    #[test]
    fn test_coverage_report_per_domain() {
        let mut catalog = build_canonical_catalog();
        // Mark all parser cases as passing.
        for case in &mut catalog.cases {
            if case.domain == ClosureDomain::Parser {
                case.outcome = ClosureOutcome::Pass;
            }
        }

        let report = catalog.coverage_report();
        let parser_cov = &report.by_domain["parser"];
        assert_eq!(parser_cov.passed, 20);
        assert!((parser_cov.closure_rate - 1.0).abs() < f64::EPSILON);

        let resolver_cov = &report.by_domain["resolver"];
        assert_eq!(resolver_cov.passed, 0);
    }

    #[test]
    fn test_coverage_report_json_roundtrip() {
        let catalog = build_canonical_catalog();
        let report = catalog.coverage_report();
        let json = report.to_json().expect("serialize");
        let restored = ClosureCoverageReport::from_json(&json).expect("deserialize");

        assert_eq!(restored.total_cases, report.total_cases);
        assert_eq!(restored.report_hash, report.report_hash);
    }

    #[test]
    fn test_coverage_report_hash_deterministic() {
        let r1 = build_canonical_catalog().coverage_report();
        let r2 = build_canonical_catalog().coverage_report();
        assert_eq!(r1.report_hash, r2.report_hash);
    }

    #[test]
    fn test_summary_line() {
        let mut catalog = build_canonical_catalog();
        for case in catalog.cases.iter_mut().take(10) {
            case.outcome = ClosureOutcome::Pass;
        }
        let report = catalog.coverage_report();
        let line = report.summary_line();
        assert!(line.contains("10/50"));
        assert!(line.contains("20.0%"));
    }

    #[test]
    fn test_outcome_counts() {
        let mut catalog = ClosureWaveRegistry::new();
        catalog.add(ClosureCase {
            case_id: "T-001".to_owned(),
            domain: ClosureDomain::Parser,
            title: "test".to_owned(),
            schema_sql: vec![],
            workload_sql: vec!["SELECT 1;".to_owned()],
            expected_behavior: "returns 1".to_owned(),
            outcome: ClosureOutcome::Pass,
            failure_detail: None,
            feature_tags: vec![],
            spec_reference: None,
        });
        catalog.add(ClosureCase {
            case_id: "T-002".to_owned(),
            domain: ClosureDomain::Parser,
            title: "test2".to_owned(),
            schema_sql: vec![],
            workload_sql: vec!["SELECT 2;".to_owned()],
            expected_behavior: "returns 2".to_owned(),
            outcome: ClosureOutcome::Fail,
            failure_detail: Some("wrong result".to_owned()),
            feature_tags: vec![],
            spec_reference: None,
        });

        let counts = catalog.outcome_counts();
        assert_eq!(counts["pass"], 1);
        assert_eq!(counts["fail"], 1);
    }

    #[test]
    fn test_domain_display() {
        assert_eq!(ClosureDomain::Parser.to_string(), "parser");
        assert_eq!(ClosureDomain::Resolver.to_string(), "resolver");
        assert_eq!(ClosureDomain::Pragma.to_string(), "pragma");
    }

    #[test]
    fn test_outcome_display() {
        assert_eq!(ClosureOutcome::Pass.to_string(), "pass");
        assert_eq!(ClosureOutcome::Fail.to_string(), "fail");
        assert_eq!(ClosureOutcome::Skip.to_string(), "skip");
        assert_eq!(ClosureOutcome::Error.to_string(), "error");
    }

    #[test]
    fn test_domain_pipeline_stage() {
        assert_eq!(ClosureDomain::Parser.pipeline_stage(), PipelineStage::Parse);
        assert_eq!(
            ClosureDomain::Resolver.pipeline_stage(),
            PipelineStage::Resolve
        );
        assert_eq!(
            ClosureDomain::Pragma.pipeline_stage(),
            PipelineStage::CrossCutting
        );
    }

    #[test]
    fn test_case_ids_unique() {
        let catalog = build_canonical_catalog();
        let ids: Vec<&str> = catalog.cases.iter().map(|c| c.case_id.as_str()).collect();
        let unique: std::collections::BTreeSet<&str> = ids.iter().copied().collect();
        assert_eq!(ids.len(), unique.len(), "duplicate case IDs detected");
    }

    #[test]
    fn test_all_cases_have_workload() {
        let catalog = build_canonical_catalog();
        for case in &catalog.cases {
            assert!(
                !case.workload_sql.is_empty(),
                "case {} has empty workload",
                case.case_id
            );
        }
    }

    #[test]
    fn test_empty_registry_coverage() {
        let registry = ClosureWaveRegistry::new();
        let report = registry.coverage_report();
        assert_eq!(report.total_cases, 0);
        assert!((report.overall_closure_rate).abs() < f64::EPSILON);
    }
}
