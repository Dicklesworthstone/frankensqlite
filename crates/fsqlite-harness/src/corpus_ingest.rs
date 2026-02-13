//! Upstream SQLite corpus ingestion and normalization (bd-1dp9.2.1).
//!
//! Ingests SLT/TCL-derived test cases, classifies them by taxonomy family,
//! wraps them with deterministic seed metadata, and produces coverage reports
//! showing bucket fill percentages and missing families.
//!
//! # Architecture
//!
//! ```text
//! SLT/SQL input → classify_family() → CorpusEntry → CorpusManifest → CoverageReport
//! ```
//!
//! The classification heuristic maps SQL content to the taxonomy families
//! defined in `parity_taxonomy.toml` (bd-1dp9.1.1).

use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fmt::Write as _;
use std::path::Path;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Bead identifier for log correlation.
const BEAD_ID: &str = "bd-1dp9.2.1";

/// Default seed base for corpus entries (same as FRANKEN_SEED from e2e).
pub const CORPUS_SEED_BASE: u64 = 0x0046_5241_4E4B_454E;

// ─── Taxonomy Family ─────────────────────────────────────────────────────

/// Feature families matching the parity taxonomy (parity_taxonomy.toml).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Family {
    /// SQL grammar: SELECT, INSERT, UPDATE, DELETE, DDL, expressions.
    SQL,
    /// Transactions: BEGIN, COMMIT, ROLLBACK, SAVEPOINT, isolation.
    TXN,
    /// Built-in functions: scalar, aggregate, date/time, math, window.
    FUN,
    /// Virtual database engine: VDBE opcodes, EXPLAIN output.
    VDB,
    /// Query planner: JOIN strategies, index usage, CTEs, subqueries.
    PLN,
    /// PRAGMAs and configuration.
    PGM,
    /// Extensions: FTS, JSON, R-tree, session, ICU.
    EXT,
    /// CLI commands and API behavior.
    CLI,
}

impl Family {
    /// All valid family variants.
    pub const ALL: [Self; 8] = [
        Self::SQL,
        Self::TXN,
        Self::FUN,
        Self::VDB,
        Self::PLN,
        Self::PGM,
        Self::EXT,
        Self::CLI,
    ];

    /// Convert from string (case-insensitive).
    #[must_use]
    pub fn from_str_opt(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "SQL" => Some(Self::SQL),
            "TXN" => Some(Self::TXN),
            "FUN" => Some(Self::FUN),
            "VDB" => Some(Self::VDB),
            "PLN" => Some(Self::PLN),
            "PGM" => Some(Self::PGM),
            "EXT" => Some(Self::EXT),
            "CLI" => Some(Self::CLI),
            _ => None,
        }
    }
}

impl fmt::Display for Family {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SQL => write!(f, "SQL"),
            Self::TXN => write!(f, "TXN"),
            Self::FUN => write!(f, "FUN"),
            Self::VDB => write!(f, "VDB"),
            Self::PLN => write!(f, "PLN"),
            Self::PGM => write!(f, "PGM"),
            Self::EXT => write!(f, "EXT"),
            Self::CLI => write!(f, "CLI"),
        }
    }
}

// ─── Corpus Entry ────────────────────────────────────────────────────────

/// Source of a corpus entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CorpusSource {
    /// SQLite Logic Test file.
    Slt { file: String },
    /// TCL-derived test.
    Tcl { file: String },
    /// Conformance fixture (JSON).
    Fixture { file: String },
    /// Manually authored test case.
    Custom { author: String },
    /// Metamorphic/generated test.
    Generated { generator: String, seed: u64 },
}

/// Rationale for skipping a corpus entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkipRationale {
    /// Why this entry is skipped.
    pub reason: String,
    /// Which exclusion ID from the taxonomy this relates to (if any).
    pub exclusion_id: Option<String>,
}

/// A single test case in the normalized corpus.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CorpusEntry {
    /// Unique entry identifier (deterministic from content hash).
    pub id: String,
    /// Primary taxonomy family classification.
    pub family: Family,
    /// Secondary families (for tests spanning multiple areas).
    pub secondary_families: Vec<Family>,
    /// Source provenance.
    pub source: CorpusSource,
    /// SQL statements comprising this test case.
    pub statements: Vec<String>,
    /// Deterministic seed for reproducibility.
    pub seed: u64,
    /// Skip rationale (if this entry should be excluded from scoring).
    pub skip: Option<SkipRationale>,
    /// Taxonomy feature IDs this entry exercises.
    pub taxonomy_features: Vec<String>,
    /// Human-readable description.
    pub description: String,
}

impl CorpusEntry {
    /// Compute the deterministic content hash for this entry.
    #[must_use]
    pub fn content_hash(&self) -> String {
        let hashable = serde_json::json!({
            "family": self.family.to_string(),
            "statements": self.statements,
            "seed": self.seed,
        });
        let json = hashable.to_string();
        sha256_hex(json.as_bytes())
    }
}

// ─── Corpus Manifest ─────────────────────────────────────────────────────

/// Complete normalized corpus with metadata and coverage analysis.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CorpusManifest {
    /// Bead ID for traceability.
    pub bead_id: String,
    /// Manifest version.
    pub version: u32,
    /// Base seed used for entry seed derivation.
    pub base_seed: u64,
    /// All corpus entries.
    pub entries: Vec<CorpusEntry>,
    /// Coverage analysis.
    pub coverage: CoverageReport,
}

/// Coverage report showing fill percentages per family.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoverageReport {
    /// Total corpus entries.
    pub total_entries: usize,
    /// Entries that are not skipped.
    pub active_entries: usize,
    /// Skipped entries.
    pub skipped_entries: usize,
    /// Per-family coverage.
    pub by_family: BTreeMap<String, FamilyCoverage>,
    /// Families with zero entries.
    pub missing_families: Vec<String>,
    /// Families below the minimum threshold (< 5 entries).
    pub underrepresented_families: Vec<String>,
}

/// Coverage metrics for a single family.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FamilyCoverage {
    /// Number of active entries in this family.
    pub entry_count: usize,
    /// Fill percentage (entries / expected minimum).
    pub fill_pct: f64,
    /// Taxonomy feature IDs covered by at least one entry.
    pub features_covered: BTreeSet<String>,
    /// Taxonomy feature IDs not covered by any entry.
    pub features_missing: Vec<String>,
}

// ─── Family Classification ───────────────────────────────────────────────

/// Minimum expected entries per family for "adequate" coverage.
const FAMILY_MINIMUMS: &[(Family, usize)] = &[
    (Family::SQL, 30),
    (Family::TXN, 10),
    (Family::FUN, 15),
    (Family::VDB, 5),
    (Family::PLN, 10),
    (Family::PGM, 8),
    (Family::EXT, 5),
    (Family::CLI, 3),
];

/// Classify a set of SQL statements into a primary taxonomy family.
///
/// Uses keyword and pattern analysis to determine the most relevant family.
/// Returns the primary family and any secondary families.
#[must_use]
#[allow(clippy::too_many_lines)]
pub fn classify_family(statements: &[String]) -> (Family, Vec<Family>) {
    let combined = statements
        .iter()
        .map(|s| s.to_uppercase())
        .collect::<Vec<_>>()
        .join(" ");

    let mut scores: BTreeMap<Family, u32> = BTreeMap::new();

    // Extension patterns (check first — highest specificity).
    if has_any_pattern(
        &combined,
        &["FTS3", "FTS4", "FTS5", "MATCH", "RANK(", "BM25("],
    ) {
        *scores.entry(Family::EXT).or_default() += 10;
    }
    if has_any_pattern(&combined, &["JSON(", "JSON_", "->", "->>"]) {
        *scores.entry(Family::EXT).or_default() += 10;
    }
    if has_any_pattern(&combined, &["RTREE", "R_TREE"]) {
        *scores.entry(Family::EXT).or_default() += 10;
    }

    // PRAGMA patterns.
    if has_any_pattern(&combined, &["PRAGMA "]) {
        *scores.entry(Family::PGM).or_default() += 8;
    }

    // VDBE patterns.
    if has_any_pattern(&combined, &["EXPLAIN ", "EXPLAIN QUERY PLAN"]) {
        *scores.entry(Family::VDB).or_default() += 8;
    }
    if has_any_pattern(&combined, &["VACUUM"]) {
        *scores.entry(Family::VDB).or_default() += 5;
    }

    // Transaction patterns.
    if has_any_pattern(
        &combined,
        &[
            "BEGIN",
            "COMMIT",
            "ROLLBACK",
            "SAVEPOINT",
            "RELEASE",
            "BEGIN CONCURRENT",
            "BEGIN IMMEDIATE",
            "BEGIN EXCLUSIVE",
        ],
    ) {
        *scores.entry(Family::TXN).or_default() += 8;
    }

    // Function patterns.
    if has_any_pattern(
        &combined,
        &[
            "ABS(",
            "MAX(",
            "MIN(",
            "SUM(",
            "AVG(",
            "COUNT(",
            "LENGTH(",
            "SUBSTR(",
            "UPPER(",
            "LOWER(",
            "TRIM(",
            "ROUND(",
            "RANDOM(",
            "TYPEOF(",
            "COALESCE(",
            "NULLIF(",
            "REPLACE(",
            "INSTR(",
            "HEX(",
            "ZEROBLOB(",
            "DATE(",
            "TIME(",
            "DATETIME(",
            "STRFTIME(",
            "JULIANDAY(",
            "TOTAL(",
            "GROUP_CONCAT(",
            "UNICODE(",
            "PRINTF(",
            "CHAR(",
            "LIKELIHOOD(",
            "IIF(",
            "GLOB(",
            "LIKE(",
        ],
    ) {
        *scores.entry(Family::FUN).or_default() += 6;
    }

    // Window function patterns.
    if has_any_pattern(
        &combined,
        &[
            "OVER(",
            "OVER (",
            "PARTITION BY",
            "ROW_NUMBER(",
            "RANK(",
            "DENSE_RANK(",
            "NTILE(",
            "LAG(",
            "LEAD(",
            "FIRST_VALUE(",
            "LAST_VALUE(",
            "NTH_VALUE(",
        ],
    ) {
        *scores.entry(Family::FUN).or_default() += 4;
        *scores.entry(Family::PLN).or_default() += 2;
    }

    // Planner patterns (complex queries).
    if has_any_pattern(
        &combined,
        &[
            "JOIN",
            "LEFT JOIN",
            "RIGHT JOIN",
            "CROSS JOIN",
            "NATURAL JOIN",
            "USING(",
        ],
    ) {
        *scores.entry(Family::PLN).or_default() += 4;
    }
    if has_any_pattern(&combined, &["WITH ", "WITH RECURSIVE"]) {
        *scores.entry(Family::PLN).or_default() += 5;
    }
    if combined.matches("SELECT").count() > 1 {
        // Subqueries → planner territory.
        *scores.entry(Family::PLN).or_default() += 3;
    }
    if has_any_pattern(&combined, &["CREATE INDEX", "INDEXED BY", "NOT INDEXED"]) {
        *scores.entry(Family::PLN).or_default() += 3;
    }

    // SQL grammar patterns (broad).
    if has_any_pattern(
        &combined,
        &[
            "CREATE TABLE",
            "ALTER TABLE",
            "DROP TABLE",
            "CREATE VIEW",
            "CREATE TRIGGER",
            "INSERT",
            "UPDATE",
            "DELETE",
            "SELECT",
        ],
    ) {
        *scores.entry(Family::SQL).or_default() += 3;
    }
    if has_any_pattern(
        &combined,
        &[
            "CASE WHEN",
            "CAST(",
            "BETWEEN",
            "IN (",
            "EXISTS(",
            "UNION",
            "INTERSECT",
            "EXCEPT",
            "COLLATE",
            "GROUP BY",
            "HAVING",
            "LIMIT",
            "OFFSET",
            "ORDER BY",
            "DISTINCT",
            "ON CONFLICT",
        ],
    ) {
        *scores.entry(Family::SQL).or_default() += 2;
    }

    // Determine primary and secondary families.
    let mut sorted: Vec<_> = scores.into_iter().collect();
    sorted.sort_by_key(|item| Reverse(item.1));

    let primary = sorted.first().map_or(Family::SQL, |(fam, _)| *fam);

    let secondary: Vec<Family> = sorted
        .iter()
        .skip(1)
        .filter(|(_, score)| *score >= 3)
        .map(|(fam, _)| *fam)
        .collect();

    (primary, secondary)
}

fn has_any_pattern(haystack: &str, patterns: &[&str]) -> bool {
    patterns.iter().any(|p| haystack.contains(p))
}

// ─── Seed Derivation ─────────────────────────────────────────────────────

/// Derive a deterministic seed for a corpus entry from the base seed and entry index.
#[inline]
#[must_use]
pub const fn derive_entry_seed(base_seed: u64, entry_index: u32) -> u64 {
    base_seed ^ ((entry_index as u64).wrapping_mul(0x517C_C1B7_2722_0A95))
}

// ─── Corpus Builder ──────────────────────────────────────────────────────

/// Builder for constructing a normalized corpus.
pub struct CorpusBuilder {
    base_seed: u64,
    entries: Vec<CorpusEntry>,
    taxonomy_features_by_family: BTreeMap<String, Vec<String>>,
}

impl CorpusBuilder {
    /// Create a new builder with the given base seed.
    #[must_use]
    pub fn new(base_seed: u64) -> Self {
        Self {
            base_seed,
            entries: Vec::new(),
            taxonomy_features_by_family: BTreeMap::new(),
        }
    }

    /// Register taxonomy features for coverage tracking.
    ///
    /// Features are keyed by family name (e.g., "SQL") and contain
    /// feature IDs (e.g., "F-SQL.1").
    pub fn register_taxonomy_features(
        &mut self,
        family: &str,
        features: impl IntoIterator<Item = impl Into<String>>,
    ) {
        self.taxonomy_features_by_family
            .entry(family.to_owned())
            .or_default()
            .extend(features.into_iter().map(Into::into));
    }

    /// Add SQL statements as a corpus entry with automatic classification.
    pub fn add_statements(
        &mut self,
        statements: impl IntoIterator<Item = impl Into<String>>,
        source: CorpusSource,
        description: impl Into<String>,
    ) {
        let stmts: Vec<String> = statements.into_iter().map(Into::into).collect();
        let (family, secondary) = classify_family(&stmts);
        let index = u32::try_from(self.entries.len()).unwrap_or(u32::MAX);
        let seed = derive_entry_seed(self.base_seed, index);
        let id = format!("corpus-{family}-{index:04}");

        self.entries.push(CorpusEntry {
            id,
            family,
            secondary_families: secondary,
            source,
            statements: stmts,
            seed,
            skip: None,
            taxonomy_features: Vec::new(),
            description: description.into(),
        });
    }

    /// Add a pre-classified corpus entry.
    pub fn add_entry(&mut self, mut entry: CorpusEntry) {
        if entry.seed == 0 {
            let index = u32::try_from(self.entries.len()).unwrap_or(u32::MAX);
            entry.seed = derive_entry_seed(self.base_seed, index);
        }
        self.entries.push(entry);
    }

    /// Add SQL statements with explicit family override.
    pub fn add_with_family(
        &mut self,
        family: Family,
        statements: impl IntoIterator<Item = impl Into<String>>,
        source: CorpusSource,
        description: impl Into<String>,
    ) {
        let stmts: Vec<String> = statements.into_iter().map(Into::into).collect();
        let (_, secondary) = classify_family(&stmts);
        let index = u32::try_from(self.entries.len()).unwrap_or(u32::MAX);
        let seed = derive_entry_seed(self.base_seed, index);
        let id = format!("corpus-{family}-{index:04}");

        self.entries.push(CorpusEntry {
            id,
            family,
            secondary_families: secondary,
            source,
            statements: stmts,
            seed,
            skip: None,
            taxonomy_features: Vec::new(),
            description: description.into(),
        });
    }

    /// Mark the last entry as skipped.
    pub fn skip_last(&mut self, reason: impl Into<String>, exclusion_id: Option<String>) {
        if let Some(entry) = self.entries.last_mut() {
            entry.skip = Some(SkipRationale {
                reason: reason.into(),
                exclusion_id,
            });
        }
    }

    /// Link the last entry to taxonomy feature IDs.
    pub fn link_features(&mut self, feature_ids: impl IntoIterator<Item = impl Into<String>>) {
        if let Some(entry) = self.entries.last_mut() {
            entry
                .taxonomy_features
                .extend(feature_ids.into_iter().map(Into::into));
        }
    }

    /// Build the corpus manifest with coverage analysis.
    #[must_use]
    pub fn build(self) -> CorpusManifest {
        let coverage = compute_coverage(&self.entries, &self.taxonomy_features_by_family);
        CorpusManifest {
            bead_id: BEAD_ID.to_owned(),
            version: 1,
            base_seed: self.base_seed,
            entries: self.entries,
            coverage,
        }
    }
}

// ─── Coverage Computation ────────────────────────────────────────────────

fn compute_coverage(
    entries: &[CorpusEntry],
    taxonomy_features: &BTreeMap<String, Vec<String>>,
) -> CoverageReport {
    let active: Vec<_> = entries.iter().filter(|e| e.skip.is_none()).collect();
    let skipped = entries.len() - active.len();

    let mut by_family: BTreeMap<String, FamilyCoverage> = BTreeMap::new();

    for &(fam, min) in FAMILY_MINIMUMS {
        let fam_str = fam.to_string();
        let fam_entries: Vec<_> = active.iter().filter(|e| e.family == fam).collect();

        let features_covered: BTreeSet<String> = fam_entries
            .iter()
            .flat_map(|e| e.taxonomy_features.iter().cloned())
            .collect();

        let all_features = taxonomy_features.get(&fam_str).cloned().unwrap_or_default();

        let features_missing: Vec<String> = all_features
            .iter()
            .filter(|f| !features_covered.contains(f.as_str()))
            .cloned()
            .collect();

        let count = fam_entries.len();
        let fill = if min > 0 {
            (count as f64 / min as f64) * 100.0
        } else if count > 0 {
            100.0
        } else {
            0.0
        };

        by_family.insert(
            fam_str,
            FamilyCoverage {
                entry_count: count,
                fill_pct: (fill * 100.0).round() / 100.0,
                features_covered,
                features_missing,
            },
        );
    }

    let missing: Vec<String> = Family::ALL
        .iter()
        .filter(|f| {
            by_family
                .get(&f.to_string())
                .map_or(true, |c| c.entry_count == 0)
        })
        .map(|f| f.to_string())
        .collect();

    let underrepresented: Vec<String> = by_family
        .iter()
        .filter(|(_, cov)| cov.entry_count > 0 && cov.fill_pct < 100.0)
        .map(|(name, _)| name.clone())
        .collect();

    CoverageReport {
        total_entries: entries.len(),
        active_entries: active.len(),
        skipped_entries: skipped,
        by_family,
        missing_families: missing,
        underrepresented_families: underrepresented,
    }
}

// ─── Conformance Fixture Ingestion ───────────────────────────────────────

/// Ingest conformance fixture JSON files from a directory into corpus entries.
///
/// # Errors
///
/// Returns an error if a fixture file cannot be read or parsed.
pub fn ingest_conformance_fixtures(
    dir: &Path,
    builder: &mut CorpusBuilder,
) -> Result<usize, String> {
    let mut count = 0;
    let entries = std::fs::read_dir(dir)
        .map_err(|e| format!("failed to read conformance dir {}: {e}", dir.display()))?;

    let mut files: Vec<_> = entries
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "json"))
        .collect();
    files.sort_by_key(|e| e.path());

    for entry in files {
        let path = entry.path();
        let content = std::fs::read_to_string(&path)
            .map_err(|e| format!("failed to read {}: {e}", path.display()))?;

        let fixture: serde_json::Value = serde_json::from_str(&content)
            .map_err(|e| format!("failed to parse {}: {e}", path.display()))?;

        let ops = fixture["ops"].as_array().unwrap_or(&Vec::new()).clone();
        let statements: Vec<String> = ops
            .iter()
            .filter_map(|op| op["sql"].as_str().map(String::from))
            .collect();

        if statements.is_empty() {
            continue;
        }

        let fixture_id = fixture["id"].as_str().unwrap_or("unknown").to_owned();
        let description = fixture["description"].as_str().unwrap_or("").to_owned();

        builder.add_statements(
            statements,
            CorpusSource::Fixture {
                file: path
                    .file_name()
                    .unwrap_or_default()
                    .to_string_lossy()
                    .to_string(),
            },
            format!("{fixture_id}: {description}"),
        );
        count += 1;
    }

    Ok(count)
}

// ─── Built-in Seed Corpus ────────────────────────────────────────────────

/// Generate a baseline seed corpus covering all families with minimal test cases.
///
/// This provides the "minimum viable corpus" for each taxonomy family,
/// ensuring no family has zero coverage.
#[allow(clippy::too_many_lines)]
pub fn generate_seed_corpus(builder: &mut CorpusBuilder) {
    let src = |name: &str| CorpusSource::Custom {
        author: format!("seed_corpus/{name}"),
    };

    // ── SQL Family ──────────────────────────────────────────────────────
    builder.add_with_family(
        Family::SQL,
        ["CREATE TABLE t1(id INTEGER PRIMARY KEY, name TEXT, age INTEGER)"],
        src("sql_create_table"),
        "DDL: CREATE TABLE with column types",
    );
    builder.link_features(["F-SQL.1"]);

    builder.add_with_family(
        Family::SQL,
        [
            "CREATE TABLE t2(x INTEGER PRIMARY KEY, y TEXT)",
            "INSERT INTO t2 VALUES(1, 'a')",
            "INSERT INTO t2 VALUES(2, 'b')",
            "SELECT * FROM t2 ORDER BY x",
        ],
        src("sql_insert_select"),
        "DML: INSERT and SELECT with ORDER BY",
    );
    builder.link_features(["F-SQL.2", "F-SQL.3"]);

    builder.add_with_family(
        Family::SQL,
        [
            "CREATE TABLE t3(id INTEGER PRIMARY KEY, val INTEGER)",
            "INSERT INTO t3 VALUES(1, 10)",
            "UPDATE t3 SET val = 20 WHERE id = 1",
            "SELECT val FROM t3 WHERE id = 1",
        ],
        src("sql_update"),
        "DML: UPDATE with WHERE clause",
    );
    builder.link_features(["F-SQL.4"]);

    builder.add_with_family(
        Family::SQL,
        [
            "CREATE TABLE t4(id INTEGER PRIMARY KEY, val TEXT)",
            "INSERT INTO t4 VALUES(1, 'keep')",
            "INSERT INTO t4 VALUES(2, 'remove')",
            "DELETE FROM t4 WHERE id = 2",
            "SELECT COUNT(*) FROM t4",
        ],
        src("sql_delete"),
        "DML: DELETE with WHERE clause",
    );
    builder.link_features(["F-SQL.5"]);

    builder.add_with_family(
        Family::SQL,
        [
            "CREATE TABLE t5(id INTEGER, name TEXT, UNIQUE(name))",
            "INSERT INTO t5 VALUES(1, 'alice')",
            "INSERT OR IGNORE INTO t5 VALUES(2, 'alice')",
            "SELECT COUNT(*) FROM t5",
        ],
        src("sql_constraints"),
        "DDL: UNIQUE constraint and ON CONFLICT",
    );
    builder.link_features(["F-SQL.8"]);

    builder.add_with_family(
        Family::SQL,
        [
            "SELECT CASE WHEN 1 > 0 THEN 'yes' ELSE 'no' END",
            "SELECT CAST(42 AS TEXT)",
            "SELECT 1 IN (1, 2, 3)",
            "SELECT 5 BETWEEN 1 AND 10",
        ],
        src("sql_expressions"),
        "Expressions: CASE, CAST, IN, BETWEEN",
    );
    builder.link_features(["F-SQL.10", "F-SQL.11"]);

    builder.add_with_family(
        Family::SQL,
        [
            "CREATE TABLE u1(id INTEGER PRIMARY KEY, v TEXT)",
            "CREATE TABLE u2(id INTEGER PRIMARY KEY, v TEXT)",
            "INSERT INTO u1 VALUES(1, 'a')",
            "INSERT INTO u2 VALUES(1, 'b')",
            "SELECT * FROM u1 UNION SELECT * FROM u2",
            "SELECT * FROM u1 INTERSECT SELECT * FROM u2",
            "SELECT * FROM u1 EXCEPT SELECT * FROM u2",
        ],
        src("sql_compound"),
        "Compound queries: UNION, INTERSECT, EXCEPT",
    );
    builder.link_features(["F-SQL.14"]);

    builder.add_with_family(
        Family::SQL,
        [
            "CREATE TABLE n1(id INTEGER PRIMARY KEY, val TEXT DEFAULT NULL)",
            "INSERT INTO n1(id) VALUES(1)",
            "SELECT val IS NULL FROM n1",
            "SELECT COALESCE(val, 'default') FROM n1",
        ],
        src("sql_null_handling"),
        "NULL semantics: IS NULL, COALESCE",
    );
    builder.link_features(["F-SQL.9"]);

    // ── TXN Family ──────────────────────────────────────────────────────
    builder.add_with_family(
        Family::TXN,
        [
            "CREATE TABLE tx1(id INTEGER PRIMARY KEY, v INTEGER)",
            "INSERT INTO tx1 VALUES(1, 10)",
        ],
        src("txn_autocommit"),
        "Transaction: autocommit mode",
    );
    builder.link_features(["F-TXN.1"]);

    builder.add_with_family(
        Family::TXN,
        [
            "CREATE TABLE tx2(id INTEGER PRIMARY KEY, v INTEGER)",
            "INSERT INTO tx2 VALUES(1, 0)",
        ],
        src("txn_isolation"),
        "Transaction: basic isolation",
    );
    builder.link_features(["F-TXN.3"]);

    // ── FUN Family ──────────────────────────────────────────────────────
    builder.add_with_family(
        Family::FUN,
        [
            "SELECT ABS(-42)",
            "SELECT LENGTH('hello')",
            "SELECT UPPER('hello')",
            "SELECT LOWER('HELLO')",
            "SELECT TYPEOF(42)",
            "SELECT TYPEOF(3.14)",
            "SELECT TYPEOF('text')",
            "SELECT TYPEOF(NULL)",
        ],
        src("fun_scalar"),
        "Scalar functions: ABS, LENGTH, UPPER, LOWER, TYPEOF",
    );
    builder.link_features(["F-FUN.1"]);

    builder.add_with_family(
        Family::FUN,
        [
            "CREATE TABLE agg1(id INTEGER, val INTEGER)",
            "INSERT INTO agg1 VALUES(1, 10)",
            "INSERT INTO agg1 VALUES(2, 20)",
            "INSERT INTO agg1 VALUES(3, 30)",
            "SELECT COUNT(*), SUM(val), AVG(val), MIN(val), MAX(val) FROM agg1",
        ],
        src("fun_aggregate"),
        "Aggregate functions: COUNT, SUM, AVG, MIN, MAX",
    );
    builder.link_features(["F-FUN.5"]);

    builder.add_with_family(
        Family::FUN,
        [
            "SELECT SUBSTR('hello world', 1, 5)",
            "SELECT REPLACE('hello', 'l', 'r')",
            "SELECT TRIM('  hello  ')",
            "SELECT INSTR('hello', 'ell')",
            "SELECT HEX(X'DEADBEEF')",
        ],
        src("fun_string"),
        "String functions: SUBSTR, REPLACE, TRIM, INSTR, HEX",
    );
    builder.link_features(["F-FUN.2"]);

    builder.add_with_family(
        Family::FUN,
        [
            "SELECT ROUND(3.14159, 2)",
            "SELECT ABS(-99)",
            "SELECT MAX(1, 2, 3)",
            "SELECT MIN(1, 2, 3)",
        ],
        src("fun_math"),
        "Math functions: ROUND, ABS, MAX, MIN",
    );
    builder.link_features(["F-FUN.3"]);

    // ── VDB Family ──────────────────────────────────────────────────────
    builder.add_with_family(
        Family::VDB,
        [
            "CREATE TABLE v1(id INTEGER PRIMARY KEY, val TEXT)",
            "EXPLAIN SELECT * FROM v1 WHERE id = 1",
        ],
        src("vdb_explain"),
        "VDBE: EXPLAIN output",
    );
    builder.link_features(["F-VDB.1"]);

    // ── PLN Family ──────────────────────────────────────────────────────
    builder.add_with_family(
        Family::PLN,
        [
            "CREATE TABLE p1(id INTEGER PRIMARY KEY, name TEXT)",
            "CREATE TABLE p2(id INTEGER PRIMARY KEY, p1_id INTEGER, value TEXT)",
            "INSERT INTO p1 VALUES(1, 'a')",
            "INSERT INTO p2 VALUES(1, 1, 'x')",
            "SELECT p1.name, p2.value FROM p1 JOIN p2 ON p1.id = p2.p1_id",
        ],
        src("pln_join"),
        "Planner: basic JOIN",
    );
    builder.link_features(["F-PLN.1"]);

    builder.add_with_family(
        Family::PLN,
        [
            "CREATE TABLE p3(id INTEGER PRIMARY KEY, val INTEGER)",
            "CREATE INDEX idx_p3_val ON p3(val)",
            "INSERT INTO p3 VALUES(1, 100)",
            "INSERT INTO p3 VALUES(2, 200)",
            "SELECT * FROM p3 WHERE val > 150",
        ],
        src("pln_index"),
        "Planner: index usage",
    );
    builder.link_features(["F-PLN.3"]);

    builder.add_with_family(
        Family::PLN,
        ["WITH cte AS (SELECT 1 AS n UNION ALL SELECT n+1 FROM cte WHERE n < 5) SELECT * FROM cte"],
        src("pln_cte"),
        "Planner: recursive CTE",
    );
    builder.link_features(["F-PLN.5"]);

    // ── PGM Family ──────────────────────────────────────────────────────
    builder.add_with_family(
        Family::PGM,
        ["PRAGMA table_info('sqlite_master')"],
        src("pgm_table_info"),
        "PRAGMA: table_info",
    );
    builder.link_features(["F-PGM.1"]);

    builder.add_with_family(
        Family::PGM,
        [
            "PRAGMA page_size",
            "PRAGMA journal_mode",
            "PRAGMA cache_size",
        ],
        src("pgm_read_settings"),
        "PRAGMA: read configuration",
    );
    builder.link_features(["F-PGM.2"]);

    // ── EXT Family ──────────────────────────────────────────────────────
    builder.add_with_family(
        Family::EXT,
        ["SELECT JSON('{\"a\":1,\"b\":2}')"],
        src("ext_json"),
        "Extension: JSON functions",
    );
    builder.link_features(["F-EXT.2"]);

    // ── CLI Family ──────────────────────────────────────────────────────
    builder.add_with_family(
        Family::CLI,
        ["SELECT sqlite_version()"],
        src("cli_version"),
        "CLI/API: sqlite_version()",
    );
    builder.link_features(["F-CLI.1"]);
}

// ─── Helpers ─────────────────────────────────────────────────────────────

fn sha256_hex(data: &[u8]) -> String {
    let digest = Sha256::digest(data);
    let mut hex = String::with_capacity(64);
    for byte in digest {
        let _ = write!(hex, "{byte:02x}");
    }
    hex
}
