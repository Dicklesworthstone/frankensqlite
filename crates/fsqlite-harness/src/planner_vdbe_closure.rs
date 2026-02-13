//! Planner + VDBE opcode semantic closure wave (bd-1dp9.3.3).
//!
//! Structured test infrastructure for identifying and closing behavioral
//! deltas in the query planner and VDBE execution engine.
//!
//! # Closure Wave Pattern
//!
//! 1. Enumerate expected behaviors for planner decisions and VDBE semantics
//! 2. Test each behavior against both C SQLite and FrankenSQLite
//! 3. Record gaps as structured `PlannerVdbeCase` entries
//! 4. Produce a coverage report showing closure progress
//!
//! # Coverage Domains
//!
//! - **Planner**: access path selection, join ordering, cost model, aggregate
//!   planning, ORDER BY optimization, subquery handling
//! - **Vdbe**: opcode semantics, expression evaluation, NULL propagation,
//!   cursor lifecycle, sorting, aggregates, type affinity

use std::collections::BTreeMap;
use std::fmt;
use std::fmt::Write as _;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::closure_wave::ClosureOutcome;
use crate::semantic_gap_map::PipelineStage;

/// Bead identifier for log correlation.
#[allow(dead_code)]
const BEAD_ID: &str = "bd-1dp9.3.3";

/// Schema version.
pub const PLANNER_VDBE_SCHEMA_VERSION: u32 = 1;

// ===========================================================================
// Domain
// ===========================================================================

/// Domain of a planner/VDBE closure case.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum PlannerVdbeDomain {
    /// Planner: access path selection, join ordering, cost model.
    Planner,
    /// VDBE: opcode semantics, expression evaluation, execution.
    Vdbe,
}

impl PlannerVdbeDomain {
    /// Map to pipeline stage.
    #[must_use]
    pub const fn pipeline_stage(self) -> PipelineStage {
        match self {
            Self::Planner => PipelineStage::Plan,
            Self::Vdbe => PipelineStage::Execute,
        }
    }

    /// All domains.
    pub const ALL: [Self; 2] = [Self::Planner, Self::Vdbe];
}

impl fmt::Display for PlannerVdbeDomain {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Planner => write!(f, "planner"),
            Self::Vdbe => write!(f, "vdbe"),
        }
    }
}

// ===========================================================================
// Case
// ===========================================================================

/// A single planner/VDBE closure test case.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlannerVdbeCase {
    /// Unique case identifier (e.g., "PLAN-001", "VDBE-001").
    pub case_id: String,
    /// Domain this case belongs to.
    pub domain: PlannerVdbeDomain,
    /// Human-readable title.
    pub title: String,
    /// SQL to set up the schema.
    pub schema_sql: Vec<String>,
    /// SQL workload to execute after schema.
    pub workload_sql: Vec<String>,
    /// Expected behavior description.
    pub expected_behavior: String,
    /// Outcome of the test.
    pub outcome: ClosureOutcome,
    /// Explanation of failure (if outcome is Fail).
    pub failure_detail: Option<String>,
    /// Feature tags for cross-referencing.
    pub feature_tags: Vec<String>,
    /// Spec section reference.
    pub spec_reference: Option<String>,
}

// ===========================================================================
// Registry
// ===========================================================================

/// Registry of planner/VDBE closure cases.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PlannerVdbeRegistry {
    /// All registered cases.
    pub cases: Vec<PlannerVdbeCase>,
}

impl PlannerVdbeRegistry {
    /// Create a new empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a case to the registry.
    pub fn add(&mut self, case: PlannerVdbeCase) {
        self.cases.push(case);
    }

    /// Get cases for a specific domain.
    #[must_use]
    pub fn cases_for_domain(&self, domain: PlannerVdbeDomain) -> Vec<&PlannerVdbeCase> {
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
    #[allow(clippy::cast_precision_loss)]
    pub fn coverage_report(&self) -> PlannerVdbeCoverageReport {
        let mut by_domain: BTreeMap<String, PlannerVdbeDomainCoverage> = BTreeMap::new();

        for domain in PlannerVdbeDomain::ALL {
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

            let closure_rate = if total == 0 {
                0.0
            } else {
                passed as f64 / total as f64
            };

            by_domain.insert(
                domain.to_string(),
                PlannerVdbeDomainCoverage {
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

        let overall_closure_rate = if total == 0 {
            0.0
        } else {
            total_passed as f64 / total as f64
        };

        // Deterministic hash.
        let mut hasher = Sha256::new();
        hasher.update(b"planner-vdbe-closure-v1:");
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

        PlannerVdbeCoverageReport {
            schema_version: PLANNER_VDBE_SCHEMA_VERSION,
            report_hash,
            total_cases: total,
            total_passed,
            overall_closure_rate,
            by_domain,
        }
    }
}

// ===========================================================================
// Coverage Report
// ===========================================================================

/// Coverage statistics for a single domain.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlannerVdbeDomainCoverage {
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

/// Coverage report for the planner/VDBE closure wave.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlannerVdbeCoverageReport {
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
    pub by_domain: BTreeMap<String, PlannerVdbeDomainCoverage>,
}

impl PlannerVdbeCoverageReport {
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
            "Planner+VDBE closure: {}/{} cases passed ({:.1}%)",
            self.total_passed,
            self.total_cases,
            self.overall_closure_rate * 100.0,
        )
    }
}

// ===========================================================================
// Canonical Catalog
// ===========================================================================

/// Build the canonical closure case catalog for planner + VDBE.
///
/// These cases represent expected SQL behaviors where planner decisions
/// and VDBE opcode execution must match between C SQLite and FrankenSQLite.
/// Each case is initially `Skip` until executed against both engines.
#[must_use]
pub fn build_planner_vdbe_catalog() -> PlannerVdbeRegistry {
    let mut registry = PlannerVdbeRegistry::new();
    add_planner_cases(&mut registry);
    add_vdbe_cases(&mut registry);
    registry
}

// ---------------------------------------------------------------------------
// Planner cases (20)
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_lines)]
fn add_planner_cases(reg: &mut PlannerVdbeRegistry) {
    let schema_t = vec![
        "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT, c REAL);".to_owned(),
        "INSERT INTO t VALUES(1,'x',1.0),(2,'y',2.0),(3,'z',3.0),(4,'w',4.0),(5,'v',5.0);"
            .to_owned(),
    ];
    let schema_idx = {
        let mut s = schema_t.clone();
        s.push("CREATE INDEX idx_b ON t(b);".to_owned());
        s
    };
    let schema_two = vec![
        "CREATE TABLE t1(a INTEGER PRIMARY KEY, b TEXT);".to_owned(),
        "CREATE TABLE t2(x INTEGER PRIMARY KEY, y TEXT, a_ref INTEGER);".to_owned(),
        "INSERT INTO t1 VALUES(1,'p'),(2,'q'),(3,'r');".to_owned(),
        "INSERT INTO t2 VALUES(10,'s',1),(20,'t',2),(30,'u',3);".to_owned(),
    ];
    let schema_three = vec![
        "CREATE TABLE a(id INTEGER PRIMARY KEY, val TEXT);".to_owned(),
        "CREATE TABLE b(id INTEGER PRIMARY KEY, a_id INTEGER, val TEXT);".to_owned(),
        "CREATE TABLE c(id INTEGER PRIMARY KEY, b_id INTEGER, val TEXT);".to_owned(),
        "INSERT INTO a VALUES(1,'a1'),(2,'a2');".to_owned(),
        "INSERT INTO b VALUES(1,1,'b1'),(2,1,'b2'),(3,2,'b3');".to_owned(),
        "INSERT INTO c VALUES(1,1,'c1'),(2,2,'c2'),(3,3,'c3');".to_owned(),
    ];

    let planner_cases: Vec<(&str, &str, Vec<String>, Vec<String>, &str, Vec<&str>)> = vec![
        // Access path selection
        (
            "PLAN-001",
            "Full table scan (no WHERE)",
            schema_t.clone(),
            vec!["SELECT * FROM t;".to_owned()],
            "Returns all 5 rows",
            vec!["F-PLANNER"],
        ),
        (
            "PLAN-002",
            "Rowid equality lookup",
            schema_t.clone(),
            vec!["SELECT * FROM t WHERE a = 3;".to_owned()],
            "Returns single row via rowid",
            vec!["F-PLANNER"],
        ),
        (
            "PLAN-003",
            "Index equality scan",
            schema_idx.clone(),
            vec!["SELECT * FROM t WHERE b = 'y';".to_owned()],
            "Uses index idx_b for equality",
            vec!["F-PLANNER"],
        ),
        (
            "PLAN-004",
            "Index range scan",
            schema_idx.clone(),
            vec!["SELECT * FROM t WHERE b > 'v' ORDER BY b;".to_owned()],
            "Uses index range scan",
            vec!["F-PLANNER"],
        ),
        (
            "PLAN-005",
            "Full scan with non-indexed WHERE",
            schema_t.clone(),
            vec!["SELECT * FROM t WHERE c > 2.5;".to_owned()],
            "Full scan with filter, returns rows where c>2.5",
            vec!["F-PLANNER"],
        ),
        // Join ordering
        (
            "PLAN-006",
            "Two-table equijoin",
            schema_two.clone(),
            vec!["SELECT t1.b, t2.y FROM t1 JOIN t2 ON t1.a = t2.a_ref;".to_owned()],
            "Returns 3 rows from equijoin",
            vec!["F-PLANNER"],
        ),
        (
            "PLAN-007",
            "Three-table chain join",
            schema_three.clone(),
            vec!["SELECT a.val, b.val, c.val FROM a JOIN b ON a.id=b.a_id JOIN c ON b.id=c.b_id;".to_owned()],
            "Returns rows from chain join a→b→c",
            vec!["F-PLANNER"],
        ),
        (
            "PLAN-008",
            "CROSS JOIN preserves order",
            vec![
                "CREATE TABLE s1(x);".to_owned(),
                "CREATE TABLE s2(y);".to_owned(),
                "INSERT INTO s1 VALUES(1),(2);".to_owned(),
                "INSERT INTO s2 VALUES('a'),('b');".to_owned(),
            ],
            vec!["SELECT * FROM s1 CROSS JOIN s2;".to_owned()],
            "Cartesian product in declaration order",
            vec!["F-PLANNER"],
        ),
        (
            "PLAN-009",
            "Self-join with aliases",
            schema_t.clone(),
            vec!["SELECT t1.a, t2.a FROM t AS t1, t AS t2 WHERE t1.a < t2.a AND t1.a <= 2 AND t2.a <= 3;".to_owned()],
            "Returns pairs where t1.a < t2.a",
            vec!["F-PLANNER"],
        ),
        (
            "PLAN-010",
            "LEFT JOIN preserves unmatched left rows",
            schema_two.clone(),
            vec![
                "INSERT INTO t1 VALUES(99,'orphan');".to_owned(),
                "SELECT t1.b, t2.y FROM t1 LEFT JOIN t2 ON t1.a = t2.a_ref ORDER BY t1.a;".to_owned(),
            ],
            "Orphan row has NULL for t2.y",
            vec!["F-PLANNER"],
        ),
        // ORDER BY planning
        (
            "PLAN-011",
            "ORDER BY indexed column",
            schema_idx.clone(),
            vec!["SELECT b FROM t ORDER BY b;".to_owned()],
            "Returns b values in sorted order",
            vec!["F-PLANNER"],
        ),
        (
            "PLAN-012",
            "ORDER BY DESC",
            schema_t.clone(),
            vec!["SELECT a FROM t ORDER BY a DESC;".to_owned()],
            "Returns rowids 5,4,3,2,1",
            vec!["F-PLANNER"],
        ),
        (
            "PLAN-013",
            "ORDER BY with LIMIT",
            schema_t.clone(),
            vec!["SELECT a FROM t ORDER BY a LIMIT 3;".to_owned()],
            "Returns first 3 rowids",
            vec!["F-PLANNER"],
        ),
        (
            "PLAN-014",
            "ORDER BY with LIMIT and OFFSET",
            schema_t.clone(),
            vec!["SELECT a FROM t ORDER BY a LIMIT 2 OFFSET 2;".to_owned()],
            "Returns rowids 3,4",
            vec!["F-PLANNER"],
        ),
        // Aggregate planning
        (
            "PLAN-015",
            "Simple aggregate without GROUP BY",
            schema_t.clone(),
            vec!["SELECT COUNT(*), SUM(c), AVG(c) FROM t;".to_owned()],
            "Returns (5, 15.0, 3.0)",
            vec!["F-PLANNER"],
        ),
        (
            "PLAN-016",
            "GROUP BY single column",
            vec![
                "CREATE TABLE g(cat TEXT, val INTEGER);".to_owned(),
                "INSERT INTO g VALUES('a',10),('b',20),('a',30),('b',40);".to_owned(),
            ],
            vec!["SELECT cat, SUM(val) FROM g GROUP BY cat ORDER BY cat;".to_owned()],
            "Returns a:40, b:60",
            vec!["F-PLANNER"],
        ),
        (
            "PLAN-017",
            "GROUP BY with HAVING",
            vec![
                "CREATE TABLE g(cat TEXT, val INTEGER);".to_owned(),
                "INSERT INTO g VALUES('a',10),('b',20),('a',30),('b',40);".to_owned(),
            ],
            vec!["SELECT cat, SUM(val) AS s FROM g GROUP BY cat HAVING s > 50;".to_owned()],
            "Returns only b:60",
            vec!["F-PLANNER"],
        ),
        // Subquery / compound
        (
            "PLAN-018",
            "Subquery in FROM",
            schema_t.clone(),
            vec!["SELECT sub.m FROM (SELECT MAX(c) AS m FROM t) AS sub;".to_owned()],
            "Returns 5.0",
            vec!["F-PLANNER"],
        ),
        (
            "PLAN-019",
            "EXISTS subquery",
            schema_two.clone(),
            vec!["SELECT t1.b FROM t1 WHERE EXISTS(SELECT 1 FROM t2 WHERE t2.a_ref = t1.a);".to_owned()],
            "Returns rows with matching t2 entry",
            vec!["F-PLANNER"],
        ),
        (
            "PLAN-020",
            "UNION with ORDER BY",
            vec![],
            vec!["SELECT 3 AS v UNION SELECT 1 UNION SELECT 2 ORDER BY v;".to_owned()],
            "Returns 1,2,3 in order",
            vec!["F-PLANNER"],
        ),
    ];

    for (id, title, schema, workload, expected, tags) in planner_cases {
        reg.add(PlannerVdbeCase {
            case_id: id.to_owned(),
            domain: PlannerVdbeDomain::Planner,
            title: title.to_owned(),
            schema_sql: schema,
            workload_sql: workload,
            expected_behavior: expected.to_owned(),
            outcome: ClosureOutcome::Skip,
            failure_detail: None,
            feature_tags: tags.iter().copied().map(ToOwned::to_owned).collect(),
            spec_reference: None,
        });
    }
}

// ---------------------------------------------------------------------------
// VDBE cases (30)
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_lines)]
fn add_vdbe_cases(reg: &mut PlannerVdbeRegistry) {
    let schema_t = vec![
        "CREATE TABLE t(a INTEGER, b TEXT, c REAL);".to_owned(),
        "INSERT INTO t VALUES(1,'hello',1.5);".to_owned(),
        "INSERT INTO t VALUES(2,'world',2.5);".to_owned(),
        "INSERT INTO t VALUES(3,NULL,NULL);".to_owned(),
    ];

    let vdbe_cases: Vec<(&str, &str, Vec<String>, Vec<String>, &str, Vec<&str>)> = vec![
        // Expression evaluation
        (
            "VDBE-001",
            "Integer arithmetic",
            vec![],
            vec!["SELECT 10 + 3, 10 - 3, 10 * 3, 10 / 3, 10 % 3;".to_owned()],
            "Returns 13, 7, 30, 3, 1",
            vec!["F-VDBE"],
        ),
        (
            "VDBE-002",
            "Real arithmetic",
            vec![],
            vec!["SELECT 10.0 / 3.0;".to_owned()],
            "Returns ~3.333...",
            vec!["F-VDBE"],
        ),
        (
            "VDBE-003",
            "String concatenation",
            vec![],
            vec!["SELECT 'hello' || ' ' || 'world';".to_owned()],
            "Returns 'hello world'",
            vec!["F-VDBE"],
        ),
        (
            "VDBE-004",
            "NULL propagation in arithmetic",
            vec![],
            vec!["SELECT 1 + NULL, NULL * 5, NULL || 'x';".to_owned()],
            "All return NULL",
            vec!["F-VDBE"],
        ),
        (
            "VDBE-005",
            "Comparison operators",
            vec![],
            vec!["SELECT 1 < 2, 2 <= 2, 3 > 2, 3 >= 3, 1 = 1, 1 != 2;".to_owned()],
            "All return 1 (true)",
            vec!["F-VDBE"],
        ),
        (
            "VDBE-006",
            "NULL in comparisons",
            vec![],
            vec!["SELECT NULL = NULL, NULL != NULL, NULL < 1, NULL > 1;".to_owned()],
            "All return NULL",
            vec!["F-VDBE"],
        ),
        (
            "VDBE-007",
            "IS NULL / IS NOT NULL",
            vec![],
            vec!["SELECT NULL IS NULL, 1 IS NULL, NULL IS NOT NULL, 1 IS NOT NULL;".to_owned()],
            "Returns 1, 0, 0, 1",
            vec!["F-VDBE"],
        ),
        (
            "VDBE-008",
            "Three-valued logic (AND/OR with NULL)",
            vec![],
            vec!["SELECT NULL AND 0, NULL AND 1, NULL OR 1, NULL OR 0;".to_owned()],
            "Returns 0, NULL, 1, NULL",
            vec!["F-VDBE"],
        ),
        (
            "VDBE-009",
            "CASE WHEN expression",
            vec![],
            vec!["SELECT CASE WHEN 1=1 THEN 'yes' WHEN 1=2 THEN 'no' ELSE 'other' END;".to_owned()],
            "Returns 'yes'",
            vec!["F-VDBE"],
        ),
        (
            "VDBE-010",
            "COALESCE with NULLs",
            vec![],
            vec!["SELECT COALESCE(NULL, NULL, 3, 4);".to_owned()],
            "Returns 3 (first non-NULL)",
            vec!["F-VDBE"],
        ),
        (
            "VDBE-011",
            "NULLIF expression",
            vec![],
            vec!["SELECT NULLIF(1, 1), NULLIF(1, 2);".to_owned()],
            "Returns NULL, 1",
            vec!["F-VDBE"],
        ),
        (
            "VDBE-012",
            "IIF expression",
            vec![],
            vec!["SELECT IIF(1, 'true', 'false'), IIF(0, 'true', 'false');".to_owned()],
            "Returns 'true', 'false'",
            vec!["F-VDBE"],
        ),
        // Type affinity
        (
            "VDBE-013",
            "Type affinity: integer stored in TEXT column",
            vec![
                "CREATE TABLE ta(x TEXT);".to_owned(),
                "INSERT INTO ta VALUES(42);".to_owned(),
            ],
            vec!["SELECT typeof(x), x FROM ta;".to_owned()],
            "Returns text, '42'",
            vec!["F-VDBE"],
        ),
        (
            "VDBE-014",
            "CAST expressions",
            vec![],
            vec!["SELECT CAST('123' AS INTEGER), CAST(3.14 AS TEXT), CAST(42 AS REAL);".to_owned()],
            "Returns 123, '3.14', 42.0",
            vec!["F-VDBE"],
        ),
        // Cursor and iteration
        (
            "VDBE-015",
            "Basic cursor iteration",
            schema_t.clone(),
            vec!["SELECT a, b FROM t ORDER BY a;".to_owned()],
            "Returns 3 rows in rowid order",
            vec!["F-VDBE"],
        ),
        (
            "VDBE-016",
            "Empty table iteration",
            vec!["CREATE TABLE empty(x);".to_owned()],
            vec!["SELECT * FROM empty;".to_owned()],
            "Returns 0 rows",
            vec!["F-VDBE"],
        ),
        (
            "VDBE-017",
            "Index seek and scan",
            vec![
                "CREATE TABLE t(a INTEGER, b TEXT);".to_owned(),
                "CREATE INDEX idx ON t(a);".to_owned(),
                "INSERT INTO t VALUES(1,'x'),(2,'y'),(3,'z');".to_owned(),
            ],
            vec!["SELECT b FROM t WHERE a >= 2 ORDER BY a;".to_owned()],
            "Returns y, z via index range",
            vec!["F-VDBE"],
        ),
        // Aggregates
        (
            "VDBE-018",
            "COUNT(*) vs COUNT(col) with NULLs",
            schema_t.clone(),
            vec!["SELECT COUNT(*), COUNT(b), COUNT(c) FROM t;".to_owned()],
            "COUNT(*) = 3, COUNT(b) = 2, COUNT(c) = 2",
            vec!["F-VDBE"],
        ),
        (
            "VDBE-019",
            "SUM/AVG with NULLs",
            schema_t.clone(),
            vec!["SELECT SUM(a), AVG(c) FROM t;".to_owned()],
            "SUM = 6, AVG = 2.0 (NULLs excluded)",
            vec!["F-VDBE"],
        ),
        (
            "VDBE-020",
            "MIN/MAX with mixed types",
            vec![
                "CREATE TABLE m(v);".to_owned(),
                "INSERT INTO m VALUES(3),(1),('z'),('a'),(NULL);".to_owned(),
            ],
            vec!["SELECT MIN(v), MAX(v) FROM m;".to_owned()],
            "MIN = 1 (integer), MAX = 'z' (text sort)",
            vec!["F-VDBE"],
        ),
        (
            "VDBE-021",
            "Aggregate on empty set",
            vec!["CREATE TABLE empty(x INTEGER);".to_owned()],
            vec!["SELECT COUNT(*), SUM(x), AVG(x), MIN(x), MAX(x) FROM empty;".to_owned()],
            "COUNT=0, rest are NULL",
            vec!["F-VDBE"],
        ),
        (
            "VDBE-022",
            "GROUP_CONCAT",
            vec![
                "CREATE TABLE g(cat TEXT, val TEXT);".to_owned(),
                "INSERT INTO g VALUES('a','1'),('a','2'),('b','3');".to_owned(),
            ],
            vec!["SELECT cat, GROUP_CONCAT(val, ',') FROM g GROUP BY cat ORDER BY cat;".to_owned()],
            "Returns a:'1,2', b:'3'",
            vec!["F-VDBE"],
        ),
        // Sorting
        (
            "VDBE-023",
            "Multi-column ORDER BY",
            vec![
                "CREATE TABLE s(a INTEGER, b INTEGER);".to_owned(),
                "INSERT INTO s VALUES(1,2),(1,1),(2,1),(2,2);".to_owned(),
            ],
            vec!["SELECT a, b FROM s ORDER BY a ASC, b DESC;".to_owned()],
            "Returns (1,2),(1,1),(2,2),(2,1)",
            vec!["F-VDBE"],
        ),
        (
            "VDBE-024",
            "DISTINCT elimination",
            vec![],
            vec!["SELECT DISTINCT 1 UNION ALL SELECT 1 UNION ALL SELECT 2;".to_owned()],
            "Returns 1 and 2 (no duplicates after DISTINCT on compound)",
            vec!["F-VDBE"],
        ),
        // Data modification opcodes
        (
            "VDBE-025",
            "INSERT and read back",
            vec!["CREATE TABLE ins(a INTEGER PRIMARY KEY, b TEXT);".to_owned()],
            vec![
                "INSERT INTO ins VALUES(1,'one'),(2,'two'),(3,'three');".to_owned(),
                "SELECT * FROM ins ORDER BY a;".to_owned(),
            ],
            "Returns 3 rows in order",
            vec!["F-VDBE"],
        ),
        (
            "VDBE-026",
            "UPDATE with WHERE filter",
            vec![
                "CREATE TABLE u(a INTEGER PRIMARY KEY, b TEXT);".to_owned(),
                "INSERT INTO u VALUES(1,'old'),(2,'old'),(3,'keep');".to_owned(),
            ],
            vec![
                "UPDATE u SET b = 'new' WHERE a <= 2;".to_owned(),
                "SELECT a, b FROM u ORDER BY a;".to_owned(),
            ],
            "Rows 1,2 have 'new', row 3 has 'keep'",
            vec!["F-VDBE"],
        ),
        (
            "VDBE-027",
            "DELETE with WHERE filter",
            vec![
                "CREATE TABLE d(a INTEGER PRIMARY KEY, b TEXT);".to_owned(),
                "INSERT INTO d VALUES(1,'x'),(2,'y'),(3,'z');".to_owned(),
            ],
            vec![
                "DELETE FROM d WHERE a = 2;".to_owned(),
                "SELECT * FROM d ORDER BY a;".to_owned(),
            ],
            "Returns rows 1 and 3 only",
            vec!["F-VDBE"],
        ),
        (
            "VDBE-028",
            "INSERT OR REPLACE conflict resolution",
            vec![
                "CREATE TABLE r(a INTEGER PRIMARY KEY, b TEXT);".to_owned(),
                "INSERT INTO r VALUES(1,'original');".to_owned(),
            ],
            vec![
                "INSERT OR REPLACE INTO r VALUES(1,'replaced');".to_owned(),
                "SELECT b FROM r WHERE a = 1;".to_owned(),
            ],
            "Returns 'replaced'",
            vec!["F-VDBE"],
        ),
        // Bitwise and special ops
        (
            "VDBE-029",
            "Bitwise operations",
            vec![],
            vec!["SELECT 12 & 10, 12 | 10, ~0, 1 << 4, 16 >> 2;".to_owned()],
            "Returns 8, 14, -1, 16, 4",
            vec!["F-VDBE"],
        ),
        (
            "VDBE-030",
            "BETWEEN and IN operators",
            vec![],
            vec!["SELECT 5 BETWEEN 1 AND 10, 5 IN (1, 3, 5, 7);".to_owned()],
            "Returns 1, 1 (both true)",
            vec!["F-VDBE"],
        ),
    ];

    for (id, title, schema, workload, expected, tags) in vdbe_cases {
        reg.add(PlannerVdbeCase {
            case_id: id.to_owned(),
            domain: PlannerVdbeDomain::Vdbe,
            title: title.to_owned(),
            schema_sql: schema,
            workload_sql: workload,
            expected_behavior: expected.to_owned(),
            outcome: ClosureOutcome::Skip,
            failure_detail: None,
            feature_tags: tags.iter().copied().map(ToOwned::to_owned).collect(),
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
    fn test_catalog_size() {
        let catalog = build_planner_vdbe_catalog();
        // 20 planner + 30 VDBE = 50
        assert_eq!(catalog.cases.len(), 50);
    }

    #[test]
    fn test_domain_filtering() {
        let catalog = build_planner_vdbe_catalog();
        let planner = catalog.cases_for_domain(PlannerVdbeDomain::Planner);
        assert_eq!(planner.len(), 20);

        let vdbe = catalog.cases_for_domain(PlannerVdbeDomain::Vdbe);
        assert_eq!(vdbe.len(), 30);
    }

    #[test]
    fn test_all_initially_skip() {
        let catalog = build_planner_vdbe_catalog();
        assert!(
            catalog
                .cases
                .iter()
                .all(|c| c.outcome == ClosureOutcome::Skip)
        );
    }

    #[test]
    fn test_coverage_report_all_skip() {
        let catalog = build_planner_vdbe_catalog();
        let report = catalog.coverage_report();

        assert_eq!(report.total_cases, 50);
        assert_eq!(report.total_passed, 0);
        assert!(report.overall_closure_rate.abs() < f64::EPSILON);
    }

    #[test]
    fn test_coverage_report_with_passes() {
        let mut catalog = build_planner_vdbe_catalog();
        for case in catalog.cases.iter_mut().take(10) {
            case.outcome = ClosureOutcome::Pass;
        }

        let report = catalog.coverage_report();
        assert_eq!(report.total_passed, 10);
        assert!((report.overall_closure_rate - 0.2).abs() < 0.001);
    }

    #[test]
    fn test_coverage_per_domain() {
        let mut catalog = build_planner_vdbe_catalog();
        for case in &mut catalog.cases {
            if case.domain == PlannerVdbeDomain::Planner {
                case.outcome = ClosureOutcome::Pass;
            }
        }

        let report = catalog.coverage_report();
        let planner_cov = &report.by_domain["planner"];
        assert_eq!(planner_cov.passed, 20);
        assert!((planner_cov.closure_rate - 1.0).abs() < f64::EPSILON);

        let vdbe_cov = &report.by_domain["vdbe"];
        assert_eq!(vdbe_cov.passed, 0);
    }

    #[test]
    fn test_json_roundtrip() {
        let catalog = build_planner_vdbe_catalog();
        let report = catalog.coverage_report();
        let json = report.to_json().expect("serialize");
        let restored = PlannerVdbeCoverageReport::from_json(&json).expect("deserialize");

        assert_eq!(restored.total_cases, report.total_cases);
        assert_eq!(restored.report_hash, report.report_hash);
    }

    #[test]
    fn test_hash_deterministic() {
        let r1 = build_planner_vdbe_catalog().coverage_report();
        let r2 = build_planner_vdbe_catalog().coverage_report();
        assert_eq!(r1.report_hash, r2.report_hash);
    }

    #[test]
    fn test_summary_line() {
        let mut catalog = build_planner_vdbe_catalog();
        for case in catalog.cases.iter_mut().take(25) {
            case.outcome = ClosureOutcome::Pass;
        }
        let report = catalog.coverage_report();
        let line = report.summary_line();
        assert!(line.contains("25/50"));
        assert!(line.contains("50.0%"));
    }

    #[test]
    fn test_outcome_counts() {
        let mut registry = PlannerVdbeRegistry::new();
        registry.add(PlannerVdbeCase {
            case_id: "T-001".to_owned(),
            domain: PlannerVdbeDomain::Planner,
            title: "test".to_owned(),
            schema_sql: vec![],
            workload_sql: vec!["SELECT 1;".to_owned()],
            expected_behavior: "returns 1".to_owned(),
            outcome: ClosureOutcome::Pass,
            failure_detail: None,
            feature_tags: vec![],
            spec_reference: None,
        });
        registry.add(PlannerVdbeCase {
            case_id: "T-002".to_owned(),
            domain: PlannerVdbeDomain::Vdbe,
            title: "test2".to_owned(),
            schema_sql: vec![],
            workload_sql: vec!["SELECT 2;".to_owned()],
            expected_behavior: "returns 2".to_owned(),
            outcome: ClosureOutcome::Fail,
            failure_detail: Some("wrong result".to_owned()),
            feature_tags: vec![],
            spec_reference: None,
        });

        let counts = registry.outcome_counts();
        assert_eq!(counts["pass"], 1);
        assert_eq!(counts["fail"], 1);
    }

    #[test]
    fn test_domain_display() {
        assert_eq!(PlannerVdbeDomain::Planner.to_string(), "planner");
        assert_eq!(PlannerVdbeDomain::Vdbe.to_string(), "vdbe");
    }

    #[test]
    fn test_domain_pipeline_stage() {
        assert_eq!(
            PlannerVdbeDomain::Planner.pipeline_stage(),
            PipelineStage::Plan
        );
        assert_eq!(
            PlannerVdbeDomain::Vdbe.pipeline_stage(),
            PipelineStage::Execute
        );
    }

    #[test]
    fn test_case_ids_unique() {
        let catalog = build_planner_vdbe_catalog();
        let ids: Vec<&str> = catalog.cases.iter().map(|c| c.case_id.as_str()).collect();
        let unique: std::collections::BTreeSet<&str> = ids.iter().copied().collect();
        assert_eq!(ids.len(), unique.len(), "duplicate case IDs detected");
    }

    #[test]
    fn test_all_cases_have_workload() {
        let catalog = build_planner_vdbe_catalog();
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
        let registry = PlannerVdbeRegistry::new();
        let report = registry.coverage_report();
        assert_eq!(report.total_cases, 0);
        assert!(report.overall_closure_rate.abs() < f64::EPSILON);
    }

    #[test]
    fn test_planner_case_ids_prefixed() {
        let catalog = build_planner_vdbe_catalog();
        for case in catalog.cases_for_domain(PlannerVdbeDomain::Planner) {
            assert!(
                case.case_id.starts_with("PLAN-"),
                "planner case {} missing PLAN- prefix",
                case.case_id
            );
        }
    }

    #[test]
    fn test_vdbe_case_ids_prefixed() {
        let catalog = build_planner_vdbe_catalog();
        for case in catalog.cases_for_domain(PlannerVdbeDomain::Vdbe) {
            assert!(
                case.case_id.starts_with("VDBE-"),
                "VDBE case {} missing VDBE- prefix",
                case.case_id
            );
        }
    }

    #[test]
    fn test_schema_version() {
        let catalog = build_planner_vdbe_catalog();
        let report = catalog.coverage_report();
        assert_eq!(report.schema_version, PLANNER_VDBE_SCHEMA_VERSION);
    }
}
