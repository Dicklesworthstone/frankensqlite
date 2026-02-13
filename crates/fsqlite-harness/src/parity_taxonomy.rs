//! Canonical feature universe and weighted parity taxonomy (bd-1dp9.1.1).
//!
//! Defines the complete declared parity surface for FrankenSQLite against
//! SQLite 3.52.0: SQL grammar, planner semantics, VDBE opcodes,
//! transaction/storage behavior, PRAGMAs, extensions, built-in functions,
//! CLI/API facets, and file format details.
//!
//! # Design
//!
//! Each [`Feature`] has a unique [`FeatureId`], a human-readable title, a
//! weighted importance score, its current parity status, and an optional
//! exclusion rationale for intentional divergences. The weight scheme is
//! normalised so that `sum(weights) == 1.0` within each [`FeatureCategory`].
//!
//! The taxonomy is the single source of truth consumed by:
//! - The parity score engine (bd-1dp9.1.3)
//! - The oracle differential harness (bd-1dp9.1.2)
//! - The unit matrix expansion (bd-1dp9.7.1)
//! - The execution-wave staffing lanes (bd-1dp9.9.1)
//!
//! # Determinism
//!
//! [`FeatureUniverse::features()`] returns features in a deterministic order
//! (sorted by [`FeatureId`]). All scoring arithmetic uses `f64` with no
//! platform-dependent rounding; scores are truncated to 6 decimal places via
//! [`truncate_score`] for cross-platform reproducibility.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use serde::{Deserialize, Serialize};

/// Bead identifier for log correlation.
#[allow(dead_code)]
const BEAD_ID: &str = "bd-1dp9.1.1";

/// Schema version for migration compatibility.
pub const TAXONOMY_SCHEMA_VERSION: u32 = 1;

// ---------------------------------------------------------------------------
// Feature identification
// ---------------------------------------------------------------------------

/// A unique, hierarchical feature identifier.
///
/// Format: `F-{CATEGORY}-{SEQ}` where `CATEGORY` is a short mnemonic and
/// `SEQ` is a zero-padded three-digit sequence number.
///
/// Examples: `F-SQL-001`, `F-VDBE-042`, `F-PRAGMA-003`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct FeatureId(pub String);

impl FeatureId {
    /// Create a new feature ID from category prefix and sequence number.
    #[must_use]
    pub fn new(category_prefix: &str, seq: u16) -> Self {
        Self(format!("F-{category_prefix}-{seq:03}"))
    }
}

impl fmt::Display for FeatureId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

// ---------------------------------------------------------------------------
// Feature categories
// ---------------------------------------------------------------------------

/// Top-level feature category for grouping and independent weight normalisation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum FeatureCategory {
    /// SQL grammar: statement types, clauses, expression operators.
    SqlGrammar,
    /// VDBE opcodes: bytecode VM instruction coverage.
    VdbeOpcodes,
    /// Storage and transaction semantics: WAL, pager, B-tree, locking.
    StorageTransaction,
    /// PRAGMA support: compile-time and runtime configuration.
    Pragma,
    /// Built-in scalar, aggregate, and window functions.
    BuiltinFunctions,
    /// Extension modules: FTS, JSON, R-tree, Session, ICU, misc.
    Extensions,
    /// Type system: affinity, coercion, collation, encoding.
    TypeSystem,
    /// File format: header, page layout, record encoding, overflow.
    FileFormat,
    /// Public API and CLI: connection lifecycle, prepared statements, CLI.
    ApiCli,
}

impl FeatureCategory {
    /// All categories in canonical order.
    pub const ALL: [Self; 9] = [
        Self::SqlGrammar,
        Self::VdbeOpcodes,
        Self::StorageTransaction,
        Self::Pragma,
        Self::BuiltinFunctions,
        Self::Extensions,
        Self::TypeSystem,
        Self::FileFormat,
        Self::ApiCli,
    ];

    /// Short prefix string used in [`FeatureId`] construction.
    #[must_use]
    pub const fn prefix(self) -> &'static str {
        match self {
            Self::SqlGrammar => "SQL",
            Self::VdbeOpcodes => "VDBE",
            Self::StorageTransaction => "STOR",
            Self::Pragma => "PRAGMA",
            Self::BuiltinFunctions => "FUNC",
            Self::Extensions => "EXT",
            Self::TypeSystem => "TYPE",
            Self::FileFormat => "FMT",
            Self::ApiCli => "API",
        }
    }

    /// Human-readable display name.
    #[must_use]
    pub const fn display_name(self) -> &'static str {
        match self {
            Self::SqlGrammar => "SQL Grammar",
            Self::VdbeOpcodes => "VDBE Opcodes",
            Self::StorageTransaction => "Storage & Transactions",
            Self::Pragma => "PRAGMAs",
            Self::BuiltinFunctions => "Built-in Functions",
            Self::Extensions => "Extensions",
            Self::TypeSystem => "Type System",
            Self::FileFormat => "File Format",
            Self::ApiCli => "API & CLI",
        }
    }

    /// Category-level weight in the global score (sums to 1.0).
    ///
    /// Weights reflect relative importance to SQLite behavioural parity:
    /// - SQL grammar and storage are highest because they define core behaviour.
    /// - VDBE opcodes are critical but indirectly tested through SQL execution.
    /// - Extensions are lower because they are optional in many deployments.
    #[must_use]
    pub const fn global_weight(self) -> f64 {
        match self {
            Self::SqlGrammar => 0.25,
            Self::StorageTransaction => 0.20,
            Self::VdbeOpcodes => 0.15,
            Self::BuiltinFunctions => 0.10,
            Self::TypeSystem => 0.08,
            Self::FileFormat => 0.07,
            Self::Pragma | Self::Extensions | Self::ApiCli => 0.05,
        }
    }
}

impl fmt::Display for FeatureCategory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.display_name())
    }
}

// ---------------------------------------------------------------------------
// Parity status
// ---------------------------------------------------------------------------

/// Current parity status of a feature.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ParityStatus {
    /// Feature is fully implemented and passes differential tests.
    Passing,
    /// Feature is partially implemented (some edge cases fail).
    Partial,
    /// Feature is not yet implemented.
    Missing,
    /// Feature is intentionally excluded with documented rationale.
    Excluded,
}

impl ParityStatus {
    /// Numeric contribution to parity score.
    ///
    /// - `Passing` → 1.0
    /// - `Partial` → 0.5 (conservative; exact partial coverage is refined by
    ///   the score engine in bd-1dp9.1.3 using sub-feature decomposition).
    /// - `Missing` → 0.0
    /// - `Excluded` → removed from denominator (does not affect score).
    #[must_use]
    pub const fn score_contribution(self) -> Option<f64> {
        match self {
            Self::Passing => Some(1.0),
            Self::Partial => Some(0.5),
            Self::Missing => Some(0.0),
            Self::Excluded => None,
        }
    }
}

impl fmt::Display for ParityStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Passing => f.write_str("passing"),
            Self::Partial => f.write_str("partial"),
            Self::Missing => f.write_str("missing"),
            Self::Excluded => f.write_str("excluded"),
        }
    }
}

// ---------------------------------------------------------------------------
// Exclusion rationale
// ---------------------------------------------------------------------------

/// Reason for intentionally excluding a feature from the parity surface.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ExclusionRationale {
    /// Why this feature is excluded.
    pub reason: String,
    /// Spec section or upstream reference.
    pub reference: String,
}

// ---------------------------------------------------------------------------
// Observability mapping
// ---------------------------------------------------------------------------

/// Maps a feature to the test/oracle infrastructure that validates it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObservabilityMapping {
    /// Bead IDs of related test beads (e.g. `["bd-1daa", "bd-2832"]`).
    pub bead_ids: Vec<String>,
    /// Oracle fixture IDs that exercise this feature.
    pub fixture_ids: Vec<String>,
    /// Crate-level test module paths (e.g. `"fsqlite-parser::tests::select"`).
    pub test_modules: Vec<String>,
}

// ---------------------------------------------------------------------------
// Feature definition
// ---------------------------------------------------------------------------

/// A single entry in the parity feature universe.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Feature {
    /// Unique feature identifier.
    pub id: FeatureId,
    /// Human-readable title.
    pub title: String,
    /// Detailed description of expected SQLite behaviour.
    pub description: String,
    /// Top-level category.
    pub category: FeatureCategory,
    /// Intra-category weight (relative; normalised to sum=1.0 within category).
    pub weight: f64,
    /// Current parity status.
    pub status: ParityStatus,
    /// Exclusion rationale (only present when `status == Excluded`).
    pub exclusion: Option<ExclusionRationale>,
    /// Observability mapping.
    pub observability: ObservabilityMapping,
    /// Tags for cross-cutting queries (e.g. `["concurrency", "wal"]`).
    pub tags: BTreeSet<String>,
}

// ---------------------------------------------------------------------------
// Feature universe
// ---------------------------------------------------------------------------

/// The complete declared parity surface.
///
/// Features are stored in a `BTreeMap` keyed by [`FeatureId`] for
/// deterministic iteration order.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureUniverse {
    /// Schema version for forward-compatible migrations.
    pub schema_version: u32,
    /// Target SQLite version for parity comparison.
    pub target_sqlite_version: String,
    /// All features in the universe.
    pub features: BTreeMap<FeatureId, Feature>,
}

/// Truncate an `f64` to 6 decimal places for cross-platform determinism.
#[must_use]
pub fn truncate_score(val: f64) -> f64 {
    (val * 1_000_000.0).trunc() / 1_000_000.0
}

/// Parity score result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParityScore {
    /// Global weighted score in [0.0, 1.0].
    pub global_score: f64,
    /// Per-category scores.
    pub category_scores: BTreeMap<String, CategoryScore>,
    /// Count of features by status.
    pub status_counts: StatusCounts,
}

/// Per-category parity score.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CategoryScore {
    pub category: String,
    pub score: f64,
    pub weight: f64,
    pub feature_count: usize,
    pub passing_count: usize,
    pub partial_count: usize,
    pub missing_count: usize,
    pub excluded_count: usize,
}

/// Global status counts.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StatusCounts {
    pub total: usize,
    pub passing: usize,
    pub partial: usize,
    pub missing: usize,
    pub excluded: usize,
}

impl FeatureUniverse {
    /// Compute the weighted parity score.
    ///
    /// Global score = `sum(category_weight * category_score)` where each
    /// category score is the weight-normalised sum of feature contributions,
    /// excluding `Excluded` features from the denominator.
    #[must_use]
    pub fn compute_score(&self) -> ParityScore {
        let mut category_scores = BTreeMap::new();
        let mut status_counts = StatusCounts::default();

        for cat in FeatureCategory::ALL {
            let features_in_cat: Vec<&Feature> = self
                .features
                .values()
                .filter(|f| f.category == cat)
                .collect();

            let mut passing = 0_usize;
            let mut partial = 0_usize;
            let mut missing = 0_usize;
            let mut excluded = 0_usize;
            let mut weighted_sum = 0.0_f64;
            let mut weight_denom = 0.0_f64;

            for feat in &features_in_cat {
                match feat.status {
                    ParityStatus::Passing => passing += 1,
                    ParityStatus::Partial => partial += 1,
                    ParityStatus::Missing => missing += 1,
                    ParityStatus::Excluded => excluded += 1,
                }
                if let Some(contrib) = feat.status.score_contribution() {
                    weighted_sum += feat.weight * contrib;
                    weight_denom += feat.weight;
                }
            }

            let cat_score = if weight_denom > 0.0 {
                truncate_score(weighted_sum / weight_denom)
            } else {
                0.0
            };

            status_counts.total += features_in_cat.len();
            status_counts.passing += passing;
            status_counts.partial += partial;
            status_counts.missing += missing;
            status_counts.excluded += excluded;

            category_scores.insert(
                cat.display_name().to_owned(),
                CategoryScore {
                    category: cat.display_name().to_owned(),
                    score: cat_score,
                    weight: cat.global_weight(),
                    feature_count: features_in_cat.len(),
                    passing_count: passing,
                    partial_count: partial,
                    missing_count: missing,
                    excluded_count: excluded,
                },
            );
        }

        let global_score: f64 = category_scores
            .values()
            .map(|cs| cs.weight * cs.score)
            .sum();

        ParityScore {
            global_score: truncate_score(global_score),
            category_scores,
            status_counts,
        }
    }

    /// Return features sorted deterministically by ID.
    #[must_use]
    pub fn sorted_features(&self) -> Vec<&Feature> {
        self.features.values().collect()
    }

    /// Return features filtered by category.
    #[must_use]
    pub fn features_by_category(&self, cat: FeatureCategory) -> Vec<&Feature> {
        self.features
            .values()
            .filter(|f| f.category == cat)
            .collect()
    }

    /// Return features filtered by status.
    #[must_use]
    pub fn features_by_status(&self, status: ParityStatus) -> Vec<&Feature> {
        self.features
            .values()
            .filter(|f| f.status == status)
            .collect()
    }

    /// Return features matching any of the given tags.
    #[must_use]
    pub fn features_by_tag(&self, tag: &str) -> Vec<&Feature> {
        self.features
            .values()
            .filter(|f| f.tags.contains(tag))
            .collect()
    }

    /// Validate taxonomy invariants. Returns a list of violations.
    #[must_use]
    pub fn validate(&self) -> Vec<String> {
        let mut violations = Vec::new();

        // INV-TAX-1: All feature IDs must be unique (guaranteed by BTreeMap).
        // INV-TAX-2: Excluded features must have exclusion rationale.
        for feat in self.features.values() {
            if feat.status == ParityStatus::Excluded && feat.exclusion.is_none() {
                violations.push(format!(
                    "{}: excluded feature missing exclusion rationale",
                    feat.id
                ));
            }
        }

        // INV-TAX-3: Weights must be positive within each category.
        for feat in self.features.values() {
            if feat.weight <= 0.0 {
                violations.push(format!(
                    "{}: weight must be positive, got {}",
                    feat.id, feat.weight
                ));
            }
        }

        // INV-TAX-4: Category global weights must sum to 1.0.
        let weight_sum: f64 = FeatureCategory::ALL.iter().map(|c| c.global_weight()).sum();
        if (weight_sum - 1.0).abs() > 1e-9 {
            violations.push(format!(
                "category global weights sum to {weight_sum}, expected 1.0"
            ));
        }

        // INV-TAX-5: Every category must have at least one feature.
        for cat in FeatureCategory::ALL {
            let count = self.features.values().filter(|f| f.category == cat).count();
            if count == 0 {
                violations.push(format!("category {cat} has zero features"));
            }
        }

        violations
    }

    /// Serialise to deterministic JSON (sorted keys).
    ///
    /// # Errors
    ///
    /// Returns `Err` if serialisation fails.
    pub fn to_json(&self) -> std::result::Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }

    /// Deserialise from JSON.
    ///
    /// # Errors
    ///
    /// Returns `Err` if the JSON is malformed or schema version mismatches.
    pub fn from_json(json: &str) -> std::result::Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }
}

// ---------------------------------------------------------------------------
// Builder helpers
// ---------------------------------------------------------------------------

/// Helper to construct features with less boilerplate.
struct FeatureBuilder {
    category: FeatureCategory,
    seq: u16,
    features: Vec<Feature>,
}

impl FeatureBuilder {
    fn new(category: FeatureCategory) -> Self {
        Self {
            category,
            seq: 0,
            features: Vec::new(),
        }
    }

    fn add(
        &mut self,
        title: &str,
        description: &str,
        weight: f64,
        status: ParityStatus,
        tags: &[&str],
    ) -> &mut Self {
        self.seq += 1;
        self.features.push(Feature {
            id: FeatureId::new(self.category.prefix(), self.seq),
            title: title.to_owned(),
            description: description.to_owned(),
            category: self.category,
            weight,
            status,
            exclusion: None,
            observability: ObservabilityMapping::default(),
            tags: tags.iter().map(|&s| s.to_owned()).collect(),
        });
        self
    }

    #[allow(dead_code)]
    fn add_excluded(
        &mut self,
        title: &str,
        description: &str,
        weight: f64,
        reason: &str,
        reference: &str,
        tags: &[&str],
    ) -> &mut Self {
        self.seq += 1;
        self.features.push(Feature {
            id: FeatureId::new(self.category.prefix(), self.seq),
            title: title.to_owned(),
            description: description.to_owned(),
            category: self.category,
            weight,
            status: ParityStatus::Excluded,
            exclusion: Some(ExclusionRationale {
                reason: reason.to_owned(),
                reference: reference.to_owned(),
            }),
            observability: ObservabilityMapping::default(),
            tags: tags.iter().map(|&s| s.to_owned()).collect(),
        });
        self
    }

    fn build(self) -> Vec<Feature> {
        self.features
    }
}

// ---------------------------------------------------------------------------
// Canonical universe construction
// ---------------------------------------------------------------------------

/// Build the canonical feature universe for SQLite 3.52.0 parity.
///
/// This is the single source of truth for what "100% parity" means.
#[must_use]
pub fn build_canonical_universe() -> FeatureUniverse {
    let mut features = BTreeMap::new();

    let all_features: Vec<Feature> = [
        build_sql_grammar_features(),
        build_vdbe_opcode_features(),
        build_storage_transaction_features(),
        build_pragma_features(),
        build_builtin_function_features(),
        build_extension_features(),
        build_type_system_features(),
        build_file_format_features(),
        build_api_cli_features(),
    ]
    .into_iter()
    .flatten()
    .collect();

    for feat in all_features {
        features.insert(feat.id.clone(), feat);
    }

    FeatureUniverse {
        schema_version: TAXONOMY_SCHEMA_VERSION,
        target_sqlite_version: "3.52.0".to_owned(),
        features,
    }
}

// ---------------------------------------------------------------------------
// SQL Grammar features
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_lines)]
fn build_sql_grammar_features() -> Vec<Feature> {
    let mut b = FeatureBuilder::new(FeatureCategory::SqlGrammar);

    // DML statements
    b.add(
        "SELECT basic",
        "Basic SELECT with column list, aliases, DISTINCT",
        3.0,
        ParityStatus::Passing,
        &["dml", "select"],
    );
    b.add(
        "SELECT with WHERE",
        "WHERE clause with comparison, logical, and IS NULL operators",
        3.0,
        ParityStatus::Passing,
        &["dml", "select"],
    );
    b.add(
        "SELECT with JOIN",
        "INNER, LEFT, CROSS, NATURAL joins with ON/USING",
        3.0,
        ParityStatus::Passing,
        &["dml", "select", "join"],
    );
    b.add(
        "SELECT with GROUP BY",
        "GROUP BY with aggregate functions and HAVING",
        2.5,
        ParityStatus::Passing,
        &["dml", "select", "aggregate"],
    );
    b.add(
        "SELECT with ORDER BY",
        "ORDER BY with ASC/DESC, NULLS FIRST/LAST, collation",
        2.5,
        ParityStatus::Passing,
        &["dml", "select"],
    );
    b.add(
        "SELECT with LIMIT/OFFSET",
        "LIMIT and OFFSET clauses",
        2.0,
        ParityStatus::Passing,
        &["dml", "select"],
    );
    b.add(
        "SELECT subqueries",
        "Scalar subqueries, EXISTS, IN (subquery), FROM (subquery)",
        2.5,
        ParityStatus::Passing,
        &["dml", "select", "subquery"],
    );
    b.add(
        "SELECT compound",
        "UNION, UNION ALL, INTERSECT, EXCEPT with ORDER BY",
        2.0,
        ParityStatus::Passing,
        &["dml", "select", "compound"],
    );
    b.add(
        "SELECT CTE",
        "WITH clause: non-recursive and recursive CTEs",
        2.0,
        ParityStatus::Passing,
        &["dml", "select", "cte"],
    );
    b.add(
        "SELECT window functions",
        "OVER clause, PARTITION BY, ORDER BY, frame specs",
        2.0,
        ParityStatus::Passing,
        &["dml", "select", "window"],
    );
    b.add(
        "INSERT basic",
        "INSERT INTO ... VALUES, INSERT INTO ... SELECT",
        3.0,
        ParityStatus::Passing,
        &["dml", "insert"],
    );
    b.add(
        "INSERT OR conflict",
        "INSERT OR REPLACE/IGNORE/ABORT/ROLLBACK/FAIL",
        2.0,
        ParityStatus::Passing,
        &["dml", "insert", "conflict"],
    );
    b.add(
        "INSERT RETURNING",
        "RETURNING clause on INSERT",
        1.5,
        ParityStatus::Missing,
        &["dml", "insert", "returning"],
    );
    b.add(
        "UPDATE basic",
        "UPDATE ... SET ... WHERE",
        3.0,
        ParityStatus::Passing,
        &["dml", "update"],
    );
    b.add(
        "UPDATE with FROM",
        "UPDATE ... SET ... FROM ... WHERE (SQLite 3.33+)",
        1.5,
        ParityStatus::Missing,
        &["dml", "update"],
    );
    b.add(
        "UPDATE OR conflict",
        "UPDATE OR REPLACE/IGNORE/ABORT/ROLLBACK/FAIL",
        1.5,
        ParityStatus::Passing,
        &["dml", "update", "conflict"],
    );
    b.add(
        "DELETE basic",
        "DELETE FROM ... WHERE",
        3.0,
        ParityStatus::Passing,
        &["dml", "delete"],
    );
    b.add(
        "DELETE with LIMIT",
        "DELETE ... ORDER BY ... LIMIT",
        1.0,
        ParityStatus::Missing,
        &["dml", "delete"],
    );
    b.add(
        "REPLACE",
        "REPLACE INTO statement",
        1.5,
        ParityStatus::Passing,
        &["dml", "replace"],
    );
    b.add(
        "UPSERT",
        "INSERT ... ON CONFLICT DO UPDATE/NOTHING",
        1.5,
        ParityStatus::Missing,
        &["dml", "upsert"],
    );

    // DDL statements
    b.add(
        "CREATE TABLE",
        "CREATE TABLE with columns, constraints, WITHOUT ROWID, STRICT",
        3.0,
        ParityStatus::Passing,
        &["ddl", "table"],
    );
    b.add(
        "CREATE TABLE AS",
        "CREATE TABLE ... AS SELECT",
        1.5,
        ParityStatus::Missing,
        &["ddl", "table"],
    );
    b.add(
        "CREATE INDEX",
        "CREATE INDEX / CREATE UNIQUE INDEX",
        2.5,
        ParityStatus::Passing,
        &["ddl", "index"],
    );
    b.add(
        "CREATE VIEW",
        "CREATE VIEW ... AS SELECT",
        2.0,
        ParityStatus::Passing,
        &["ddl", "view"],
    );
    b.add(
        "CREATE TRIGGER",
        "CREATE TRIGGER BEFORE/AFTER/INSTEAD OF INSERT/UPDATE/DELETE",
        2.0,
        ParityStatus::Passing,
        &["ddl", "trigger"],
    );
    b.add(
        "CREATE VIRTUAL TABLE",
        "CREATE VIRTUAL TABLE ... USING module",
        1.5,
        ParityStatus::Partial,
        &["ddl", "vtab"],
    );
    b.add(
        "DROP TABLE/INDEX/VIEW/TRIGGER",
        "DROP IF EXISTS for all object types",
        2.0,
        ParityStatus::Passing,
        &["ddl", "drop"],
    );
    b.add(
        "ALTER TABLE",
        "ADD COLUMN, RENAME COLUMN, RENAME TABLE, DROP COLUMN",
        2.0,
        ParityStatus::Partial,
        &["ddl", "alter"],
    );

    // Transaction control
    b.add(
        "BEGIN/COMMIT/ROLLBACK",
        "Transaction control with DEFERRED/IMMEDIATE/EXCLUSIVE",
        3.0,
        ParityStatus::Passing,
        &["txn"],
    );
    b.add(
        "SAVEPOINT/RELEASE/ROLLBACK TO",
        "Nested savepoint support",
        2.0,
        ParityStatus::Passing,
        &["txn", "savepoint"],
    );
    b.add(
        "BEGIN CONCURRENT",
        "FrankenSQLite MVCC concurrent writer mode (extension)",
        2.0,
        ParityStatus::Passing,
        &["txn", "concurrent"],
    );

    // Database operations
    b.add(
        "ATTACH/DETACH",
        "Attach and detach additional database files",
        1.5,
        ParityStatus::Partial,
        &["database"],
    );
    b.add(
        "VACUUM",
        "Database compaction and defragmentation",
        1.5,
        ParityStatus::Partial,
        &["database", "vacuum"],
    );
    b.add(
        "REINDEX",
        "Rebuild indexes",
        1.0,
        ParityStatus::Passing,
        &["database"],
    );
    b.add(
        "ANALYZE",
        "Collect statistics for the query planner",
        1.0,
        ParityStatus::Partial,
        &["database", "planner"],
    );

    // Expression operators
    b.add(
        "Arithmetic operators",
        "+, -, *, /, % with proper type coercion",
        2.0,
        ParityStatus::Passing,
        &["expr", "arithmetic"],
    );
    b.add(
        "Comparison operators",
        "=, !=, <, >, <=, >=, IS, IS NOT",
        2.0,
        ParityStatus::Passing,
        &["expr", "comparison"],
    );
    b.add(
        "Logical operators",
        "AND, OR, NOT with three-valued logic",
        2.0,
        ParityStatus::Passing,
        &["expr", "logic"],
    );
    b.add(
        "String operators",
        "|| (concat), LIKE, GLOB, COLLATE",
        2.0,
        ParityStatus::Passing,
        &["expr", "string"],
    );
    b.add(
        "Bitwise operators",
        "&, |, ~, <<, >>",
        1.5,
        ParityStatus::Passing,
        &["expr", "bitwise"],
    );
    b.add(
        "BETWEEN/IN/CASE",
        "BETWEEN, IN (list/subquery), CASE WHEN",
        2.0,
        ParityStatus::Passing,
        &["expr"],
    );
    b.add(
        "CAST expressions",
        "CAST(expr AS type)",
        1.5,
        ParityStatus::Passing,
        &["expr", "cast"],
    );
    b.add(
        "EXISTS/NOT EXISTS",
        "Correlated and uncorrelated EXISTS subqueries",
        1.5,
        ParityStatus::Passing,
        &["expr", "subquery"],
    );
    b.add(
        "Aggregate expressions",
        "COUNT, SUM, AVG, MIN, MAX, GROUP_CONCAT, TOTAL",
        2.0,
        ParityStatus::Passing,
        &["expr", "aggregate"],
    );
    b.add(
        "Window expressions",
        "ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, etc.",
        1.5,
        ParityStatus::Passing,
        &["expr", "window"],
    );

    // Meta
    b.add(
        "EXPLAIN",
        "EXPLAIN and EXPLAIN QUERY PLAN",
        1.0,
        ParityStatus::Partial,
        &["meta"],
    );
    b.add(
        "PRAGMA statement",
        "PRAGMA name / PRAGMA name = value syntax",
        1.5,
        ParityStatus::Passing,
        &["meta", "pragma"],
    );

    b.build()
}

// ---------------------------------------------------------------------------
// VDBE Opcode features
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_lines)]
fn build_vdbe_opcode_features() -> Vec<Feature> {
    let mut b = FeatureBuilder::new(FeatureCategory::VdbeOpcodes);

    // Group opcodes by functional area rather than listing all 191 individually.
    // Each group gets a weight proportional to its importance.
    b.add(
        "Control flow opcodes",
        "Goto, Gosub, Return, InitCoroutine, EndCoroutine, Yield, Halt, HaltIfNull",
        2.0,
        ParityStatus::Passing,
        &["vdbe", "control"],
    );
    b.add(
        "Constants and values",
        "Integer, Int64, Real, String8, String, Null, SoftNull, Blob, Variable",
        2.0,
        ParityStatus::Passing,
        &["vdbe", "constants"],
    );
    b.add(
        "Register operations",
        "Move, Copy, SCopy, IntCopy, BeginSubrtn",
        1.5,
        ParityStatus::Passing,
        &["vdbe", "register"],
    );
    b.add(
        "Arithmetic opcodes",
        "Add, Subtract, Multiply, Divide, Remainder, Concat, AddImm",
        2.0,
        ParityStatus::Passing,
        &["vdbe", "arithmetic"],
    );
    b.add(
        "Comparison opcodes",
        "Eq, Ne, Lt, Le, Gt, Ge, ElseEq, Compare, Permutation",
        2.0,
        ParityStatus::Passing,
        &["vdbe", "comparison"],
    );
    b.add("Branching opcodes", "Jump, If, IfNot, IsNull, NotNull, IsType, Once, And, Or, Not, BitNot, ZeroOrNull, IfNullRow, IsTrue", 2.0, ParityStatus::Passing, &["vdbe", "branch"]);
    b.add(
        "Type conversion opcodes",
        "MustBeInt, RealAffinity, Cast, Affinity, TypeCheck",
        1.5,
        ParityStatus::Passing,
        &["vdbe", "type"],
    );
    b.add(
        "Column and record opcodes",
        "Column, MakeRecord, Offset, Count, ResultRow",
        2.0,
        ParityStatus::Passing,
        &["vdbe", "record"],
    );
    b.add(
        "Transaction opcodes",
        "Transaction, Savepoint, AutoCommit, ReadCookie, SetCookie",
        2.0,
        ParityStatus::Passing,
        &["vdbe", "txn"],
    );
    b.add("Cursor open/close opcodes", "OpenRead, OpenWrite, OpenDup, OpenEphemeral, OpenAutoindex, SorterOpen, OpenPseudo, ReopenIdx, Close, ColumnsUsed, SequenceTest", 2.0, ParityStatus::Passing, &["vdbe", "cursor"]);
    b.add(
        "Seek opcodes",
        "SeekLT, SeekLE, SeekGE, SeekGT, SeekScan, SeekHit, SeekRowid, IfNotOpen, SeekEnd",
        2.0,
        ParityStatus::Passing,
        &["vdbe", "seek"],
    );
    b.add(
        "Index lookup opcodes",
        "Found, NotFound, NoConflict, IfNoHope, NotExists",
        2.0,
        ParityStatus::Passing,
        &["vdbe", "index"],
    );
    b.add(
        "Insert/Delete opcodes",
        "Insert, Delete, RowCell, ResetCount, NewRowid, Sequence",
        2.0,
        ParityStatus::Passing,
        &["vdbe", "write"],
    );
    b.add(
        "Navigation opcodes",
        "Rewind, Next, Prev, Last, Sort, SorterSort, SorterNext, IfEmpty, IfSizeBetween",
        2.0,
        ParityStatus::Passing,
        &["vdbe", "navigate"],
    );
    b.add("Index DML opcodes", "IdxInsert, IdxDelete, SorterInsert, SorterCompare, SorterData, DeferredSeek, IdxRowid, FinishSeek, IdxLE, IdxGT, IdxLT, IdxGE", 2.0, ParityStatus::Passing, &["vdbe", "index"]);
    b.add(
        "Row data opcodes",
        "RowData, Rowid, NullRow",
        1.5,
        ParityStatus::Passing,
        &["vdbe", "data"],
    );
    b.add("DDL opcodes", "Destroy, Clear, ResetSorter, CreateBtree, SqlExec, ParseSchema, LoadAnalysis, DropTable, DropIndex, DropTrigger", 1.5, ParityStatus::Passing, &["vdbe", "ddl"]);
    b.add(
        "Aggregate opcodes",
        "AggStep, AggStep1, AggValue, AggFinal, AggInverse",
        2.0,
        ParityStatus::Passing,
        &["vdbe", "aggregate"],
    );
    b.add(
        "Function opcodes",
        "PureFunc, Function",
        2.0,
        ParityStatus::Passing,
        &["vdbe", "function"],
    );
    b.add(
        "Integrity check opcodes",
        "IntegrityCk",
        1.0,
        ParityStatus::Passing,
        &["vdbe", "integrity"],
    );
    b.add(
        "RowSet opcodes",
        "RowSetAdd, RowSetRead, RowSetTest",
        1.0,
        ParityStatus::Passing,
        &["vdbe", "rowset"],
    );
    b.add(
        "Trigger/Program opcodes",
        "Program, Param, FkCheck, FkCounter, FkIfZero",
        1.5,
        ParityStatus::Passing,
        &["vdbe", "trigger"],
    );
    b.add(
        "WAL/Journal opcodes",
        "Checkpoint, JournalMode, Vacuum, IncrVacuum",
        1.5,
        ParityStatus::Passing,
        &["vdbe", "wal"],
    );
    b.add("Virtual table opcodes", "VBegin, VCreate, VDestroy, VOpen, VFilter, VColumn, VNext, VRename, VUpdate, VCheck, VInitIn", 1.5, ParityStatus::Partial, &["vdbe", "vtab"]);
    b.add(
        "Memory/counter opcodes",
        "MemMax, IfPos, OffsetLimit, IfNotZero, DecrJumpZero",
        1.0,
        ParityStatus::Passing,
        &["vdbe", "counter"],
    );
    b.add(
        "Locking/Expire opcodes",
        "Expire, CursorLock, CursorUnlock, TableLock",
        1.0,
        ParityStatus::Passing,
        &["vdbe", "lock"],
    );
    b.add(
        "Bloom filter opcodes",
        "FilterAdd, Filter",
        1.0,
        ParityStatus::Passing,
        &["vdbe", "bloom"],
    );
    b.add(
        "Subtype opcodes",
        "ClrSubtype, GetSubtype, SetSubtype",
        1.0,
        ParityStatus::Passing,
        &["vdbe", "subtype"],
    );
    b.add(
        "Page count opcodes",
        "Pagecount, MaxPgcnt",
        1.0,
        ParityStatus::Passing,
        &["vdbe", "page"],
    );
    b.add(
        "Trace/Init opcodes",
        "Trace, Init, Noop, CursorHint, Abortable, ReleaseReg",
        0.5,
        ParityStatus::Passing,
        &["vdbe", "debug"],
    );
    b.add(
        "Collation opcodes",
        "CollSeq",
        1.0,
        ParityStatus::Passing,
        &["vdbe", "collation"],
    );
    b.add(
        "Bitwise opcodes",
        "BitAnd, BitOr, ShiftLeft, ShiftRight",
        1.0,
        ParityStatus::Passing,
        &["vdbe", "bitwise"],
    );

    b.build()
}

// ---------------------------------------------------------------------------
// Storage & Transaction features
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_lines)]
fn build_storage_transaction_features() -> Vec<Feature> {
    let mut b = FeatureBuilder::new(FeatureCategory::StorageTransaction);

    // Pager
    b.add(
        "Page cache with ARC eviction",
        "Adaptive Replacement Cache for page buffer management",
        2.0,
        ParityStatus::Passing,
        &["pager", "cache"],
    );
    b.add(
        "Dirty page write-back",
        "Ordered write-back of modified pages to database file",
        2.0,
        ParityStatus::Passing,
        &["pager", "writeback"],
    );
    b.add(
        "Rollback journal",
        "Legacy rollback journal for crash recovery",
        1.5,
        ParityStatus::Passing,
        &["pager", "journal"],
    );

    // WAL
    b.add(
        "WAL mode",
        "Write-ahead log with concurrent readers during writes",
        3.0,
        ParityStatus::Passing,
        &["wal"],
    );
    b.add(
        "WAL checkpoint",
        "PASSIVE, FULL, RESTART, TRUNCATE checkpoint modes",
        2.0,
        ParityStatus::Passing,
        &["wal", "checkpoint"],
    );
    b.add(
        "WAL recovery",
        "Crash recovery by replaying WAL frames",
        2.5,
        ParityStatus::Passing,
        &["wal", "recovery"],
    );
    b.add(
        "WAL index (SHM)",
        "Shared-memory WAL index for multi-process read concurrency",
        2.0,
        ParityStatus::Passing,
        &["wal", "shm"],
    );
    b.add(
        "WAL FEC repair symbols",
        "RaptorQ-based forward error correction for torn write healing",
        2.0,
        ParityStatus::Passing,
        &["wal", "fec", "raptorq"],
    );

    // B-tree
    b.add(
        "B-tree table pages",
        "B+tree leaf/interior table page format",
        3.0,
        ParityStatus::Passing,
        &["btree", "table"],
    );
    b.add(
        "B-tree index pages",
        "B-tree leaf/interior index page format",
        2.5,
        ParityStatus::Passing,
        &["btree", "index"],
    );
    b.add(
        "B-tree page splitting",
        "Median-based page splits with parent promotion",
        2.0,
        ParityStatus::Passing,
        &["btree", "split"],
    );
    b.add(
        "B-tree page merging",
        "Underfull page merging with sibling rebalancing",
        2.0,
        ParityStatus::Passing,
        &["btree", "merge"],
    );
    b.add(
        "Overflow pages",
        "Large record spill to linked overflow page chains",
        1.5,
        ParityStatus::Passing,
        &["btree", "overflow"],
    );
    b.add(
        "Freelist management",
        "Trunk/leaf freelist for page recycling",
        1.5,
        ParityStatus::Passing,
        &["btree", "freelist"],
    );
    b.add(
        "Cursor navigation",
        "move_to, next, prev, insert, delete operations",
        2.5,
        ParityStatus::Passing,
        &["btree", "cursor"],
    );

    // Transaction semantics
    b.add(
        "DEFERRED transactions",
        "Deferred lock acquisition (default mode)",
        2.0,
        ParityStatus::Passing,
        &["txn", "deferred"],
    );
    b.add(
        "IMMEDIATE transactions",
        "Immediate RESERVED lock on BEGIN",
        2.0,
        ParityStatus::Passing,
        &["txn", "immediate"],
    );
    b.add(
        "EXCLUSIVE transactions",
        "Exclusive lock blocking all other connections",
        2.0,
        ParityStatus::Passing,
        &["txn", "exclusive"],
    );
    b.add(
        "Autocommit wrapping",
        "Implicit transaction per statement when not in explicit txn",
        2.0,
        ParityStatus::Passing,
        &["txn", "autocommit"],
    );

    // MVCC (FrankenSQLite extension)
    b.add(
        "MVCC page-level locking",
        "Per-page exclusive write locks for concurrent writers",
        3.0,
        ParityStatus::Passing,
        &["mvcc", "concurrent"],
    );
    b.add(
        "MVCC snapshot isolation",
        "Consistent snapshots via monotonic CommitSeq",
        2.5,
        ParityStatus::Passing,
        &["mvcc", "snapshot"],
    );
    b.add(
        "SSI validation",
        "Serializable Snapshot Isolation preventing write skew",
        2.5,
        ParityStatus::Passing,
        &["mvcc", "ssi"],
    );
    b.add(
        "First-committer-wins",
        "Conflict resolution: first writer to commit a page wins",
        2.0,
        ParityStatus::Passing,
        &["mvcc", "fcw"],
    );
    b.add(
        "Safe write merging",
        "Intent replay and structured page patches for commuting conflicts",
        2.0,
        ParityStatus::Passing,
        &["mvcc", "merge"],
    );
    b.add(
        "MVCC garbage collection",
        "Epoch-based reclamation of old page versions",
        2.0,
        ParityStatus::Passing,
        &["mvcc", "gc"],
    );

    // VFS
    b.add(
        "Unix VFS",
        "POSIX file I/O with advisory locking",
        2.0,
        ParityStatus::Passing,
        &["vfs", "unix"],
    );
    b.add(
        "Memory VFS",
        "In-memory database backend",
        2.0,
        ParityStatus::Passing,
        &["vfs", "memory"],
    );

    b.build()
}

// ---------------------------------------------------------------------------
// PRAGMA features
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_lines)]
fn build_pragma_features() -> Vec<Feature> {
    let mut b = FeatureBuilder::new(FeatureCategory::Pragma);

    b.add(
        "journal_mode",
        "Read/set journal mode (DELETE, TRUNCATE, PERSIST, MEMORY, WAL, OFF)",
        3.0,
        ParityStatus::Passing,
        &["pragma", "journal"],
    );
    b.add(
        "cache_size",
        "Set page cache size",
        2.0,
        ParityStatus::Passing,
        &["pragma", "cache"],
    );
    b.add(
        "page_size",
        "Read/set database page size",
        2.0,
        ParityStatus::Passing,
        &["pragma", "page"],
    );
    b.add(
        "synchronous",
        "Set sync mode (OFF, NORMAL, FULL, EXTRA)",
        2.0,
        ParityStatus::Passing,
        &["pragma", "sync"],
    );
    b.add(
        "foreign_keys",
        "Enable/disable foreign key enforcement",
        2.5,
        ParityStatus::Passing,
        &["pragma", "fk"],
    );
    b.add(
        "wal_checkpoint",
        "Trigger WAL checkpoint",
        2.0,
        ParityStatus::Passing,
        &["pragma", "wal"],
    );
    b.add(
        "integrity_check",
        "Run database integrity verification",
        2.0,
        ParityStatus::Passing,
        &["pragma", "integrity"],
    );
    b.add(
        "auto_vacuum",
        "Set auto-vacuum mode (NONE, FULL, INCREMENTAL)",
        1.5,
        ParityStatus::Partial,
        &["pragma", "vacuum"],
    );
    b.add(
        "encoding",
        "Read/set text encoding (UTF-8, UTF-16le, UTF-16be)",
        1.5,
        ParityStatus::Passing,
        &["pragma", "encoding"],
    );
    b.add(
        "table_info",
        "Query table column metadata",
        2.0,
        ParityStatus::Passing,
        &["pragma", "schema"],
    );
    b.add(
        "index_list / index_info",
        "Query index metadata",
        1.5,
        ParityStatus::Passing,
        &["pragma", "schema"],
    );
    b.add(
        "database_list",
        "List attached databases",
        1.0,
        ParityStatus::Passing,
        &["pragma", "database"],
    );
    b.add(
        "compile_options",
        "List compile-time options",
        1.0,
        ParityStatus::Partial,
        &["pragma", "meta"],
    );
    b.add(
        "user_version / schema_version",
        "Read/write user and schema version numbers",
        1.5,
        ParityStatus::Passing,
        &["pragma", "version"],
    );
    b.add(
        "busy_timeout",
        "Set busy-wait timeout for lock contention",
        1.5,
        ParityStatus::Partial,
        &["pragma", "busy"],
    );
    b.add(
        "temp_store",
        "Configure temporary storage location",
        1.0,
        ParityStatus::Partial,
        &["pragma", "temp"],
    );
    b.add(
        "mmap_size",
        "Set memory-mapped I/O size",
        1.0,
        ParityStatus::Missing,
        &["pragma", "mmap"],
    );
    b.add(
        "locking_mode",
        "Set NORMAL or EXCLUSIVE locking mode",
        1.0,
        ParityStatus::Partial,
        &["pragma", "lock"],
    );
    b.add(
        "recursive_triggers",
        "Enable recursive trigger execution",
        1.0,
        ParityStatus::Partial,
        &["pragma", "trigger"],
    );
    b.add(
        "secure_delete",
        "Enable secure deletion of data",
        1.0,
        ParityStatus::Missing,
        &["pragma", "security"],
    );
    b.add(
        "case_sensitive_like",
        "Enable case-sensitive LIKE operator",
        1.0,
        ParityStatus::Partial,
        &["pragma", "like"],
    );
    b.add(
        "quick_check",
        "Fast integrity check variant",
        1.5,
        ParityStatus::Partial,
        &["pragma", "integrity"],
    );

    b.build()
}

// ---------------------------------------------------------------------------
// Built-in function features
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_lines)]
fn build_builtin_function_features() -> Vec<Feature> {
    let mut b = FeatureBuilder::new(FeatureCategory::BuiltinFunctions);

    // Core scalar functions
    b.add(
        "abs()",
        "Absolute value",
        1.0,
        ParityStatus::Passing,
        &["func", "scalar", "math"],
    );
    b.add(
        "length() / octet_length()",
        "String/blob length in chars/bytes",
        1.0,
        ParityStatus::Passing,
        &["func", "scalar", "string"],
    );
    b.add(
        "typeof()",
        "Runtime type name",
        1.0,
        ParityStatus::Passing,
        &["func", "scalar", "type"],
    );
    b.add(
        "upper() / lower()",
        "Case conversion",
        1.0,
        ParityStatus::Passing,
        &["func", "scalar", "string"],
    );
    b.add(
        "hex() / unhex()",
        "Hex encoding/decoding",
        1.0,
        ParityStatus::Passing,
        &["func", "scalar", "encoding"],
    );
    b.add(
        "quote()",
        "SQL literal quoting",
        1.0,
        ParityStatus::Passing,
        &["func", "scalar"],
    );
    b.add(
        "nullif() / ifnull() / coalesce()",
        "NULL handling functions",
        1.5,
        ParityStatus::Passing,
        &["func", "scalar", "null"],
    );
    b.add(
        "printf() / format()",
        "Formatted string output",
        1.0,
        ParityStatus::Passing,
        &["func", "scalar", "string"],
    );
    b.add(
        "instr()",
        "Substring position",
        1.0,
        ParityStatus::Passing,
        &["func", "scalar", "string"],
    );
    b.add(
        "trim() / ltrim() / rtrim()",
        "Whitespace/char trimming",
        1.0,
        ParityStatus::Passing,
        &["func", "scalar", "string"],
    );
    b.add(
        "replace()",
        "String replacement",
        1.0,
        ParityStatus::Passing,
        &["func", "scalar", "string"],
    );
    b.add(
        "substr() / substring()",
        "Substring extraction",
        1.0,
        ParityStatus::Passing,
        &["func", "scalar", "string"],
    );
    b.add(
        "unicode() / char()",
        "Unicode codepoint conversion",
        1.0,
        ParityStatus::Passing,
        &["func", "scalar", "string"],
    );
    b.add(
        "zeroblob() / randomblob()",
        "Blob generation",
        1.0,
        ParityStatus::Passing,
        &["func", "scalar", "blob"],
    );
    b.add(
        "random()",
        "Random integer generation",
        0.5,
        ParityStatus::Passing,
        &["func", "scalar", "random"],
    );
    b.add(
        "round()",
        "Rounding to decimal places",
        1.0,
        ParityStatus::Passing,
        &["func", "scalar", "math"],
    );
    b.add(
        "sign()",
        "Sign function (-1, 0, 1)",
        0.5,
        ParityStatus::Passing,
        &["func", "scalar", "math"],
    );
    b.add(
        "iif()",
        "Inline if/then/else",
        1.0,
        ParityStatus::Passing,
        &["func", "scalar"],
    );
    b.add(
        "concat() / concat_ws()",
        "String concatenation (3.44+)",
        1.0,
        ParityStatus::Passing,
        &["func", "scalar", "string"],
    );
    b.add(
        "likely() / unlikely()",
        "Planner hints",
        0.5,
        ParityStatus::Passing,
        &["func", "scalar", "planner"],
    );
    b.add(
        "sqlite_version()",
        "Version string",
        0.5,
        ParityStatus::Passing,
        &["func", "scalar", "meta"],
    );
    b.add(
        "changes() / total_changes()",
        "Row change counters",
        1.0,
        ParityStatus::Passing,
        &["func", "scalar", "meta"],
    );
    b.add(
        "last_insert_rowid()",
        "Last inserted row ID",
        1.0,
        ParityStatus::Passing,
        &["func", "scalar", "meta"],
    );
    b.add(
        "glob() / like()",
        "Pattern matching functions",
        1.0,
        ParityStatus::Passing,
        &["func", "scalar", "pattern"],
    );
    b.add(
        "soundex()",
        "Phonetic encoding",
        0.5,
        ParityStatus::Passing,
        &["func", "scalar", "string"],
    );
    b.add(
        "min() / max() scalar",
        "Scalar min/max of arguments",
        1.0,
        ParityStatus::Passing,
        &["func", "scalar"],
    );
    b.add(
        "load_extension()",
        "Dynamic extension loading",
        1.0,
        ParityStatus::Missing,
        &["func", "scalar", "ext"],
    );

    // Aggregate functions
    b.add(
        "count()",
        "Row count aggregate (count(*) and count(expr))",
        2.0,
        ParityStatus::Passing,
        &["func", "aggregate"],
    );
    b.add(
        "sum() / total()",
        "Numeric sum aggregate",
        1.5,
        ParityStatus::Passing,
        &["func", "aggregate"],
    );
    b.add(
        "avg()",
        "Average aggregate",
        1.5,
        ParityStatus::Passing,
        &["func", "aggregate"],
    );
    b.add(
        "min() / max() aggregate",
        "Min/max aggregate",
        1.5,
        ParityStatus::Passing,
        &["func", "aggregate"],
    );
    b.add(
        "group_concat()",
        "String aggregation with separator",
        1.5,
        ParityStatus::Passing,
        &["func", "aggregate"],
    );

    // Window functions
    b.add(
        "row_number()",
        "Sequential row number",
        1.5,
        ParityStatus::Passing,
        &["func", "window"],
    );
    b.add(
        "rank() / dense_rank()",
        "Ranking with/without gaps",
        1.5,
        ParityStatus::Passing,
        &["func", "window"],
    );
    b.add(
        "ntile()",
        "Distribute rows into N buckets",
        1.0,
        ParityStatus::Passing,
        &["func", "window"],
    );
    b.add(
        "lag() / lead()",
        "Access rows relative to current",
        1.5,
        ParityStatus::Passing,
        &["func", "window"],
    );
    b.add(
        "first_value() / last_value() / nth_value()",
        "Positional value access",
        1.5,
        ParityStatus::Passing,
        &["func", "window"],
    );
    b.add(
        "cume_dist() / percent_rank()",
        "Cumulative distribution",
        1.0,
        ParityStatus::Passing,
        &["func", "window"],
    );

    // Date/time functions
    b.add(
        "date() / time() / datetime()",
        "Date and time formatting",
        1.5,
        ParityStatus::Passing,
        &["func", "datetime"],
    );
    b.add(
        "julianday()",
        "Julian day number",
        1.0,
        ParityStatus::Passing,
        &["func", "datetime"],
    );
    b.add(
        "strftime()",
        "Formatted date/time output",
        1.5,
        ParityStatus::Passing,
        &["func", "datetime"],
    );
    b.add(
        "unixepoch()",
        "Unix epoch seconds",
        1.0,
        ParityStatus::Passing,
        &["func", "datetime"],
    );
    b.add(
        "timediff()",
        "Time difference (3.43+)",
        1.0,
        ParityStatus::Passing,
        &["func", "datetime"],
    );

    // Math functions (SQLite 3.35+)
    b.add(
        "Math: ceil/floor/trunc",
        "Ceiling, floor, truncation",
        1.0,
        ParityStatus::Passing,
        &["func", "math"],
    );
    b.add(
        "Math: log/log2/log10/ln",
        "Logarithmic functions",
        1.0,
        ParityStatus::Passing,
        &["func", "math"],
    );
    b.add(
        "Math: exp/pow/sqrt",
        "Exponential, power, square root",
        1.0,
        ParityStatus::Passing,
        &["func", "math"],
    );
    b.add(
        "Math: sin/cos/tan/asin/acos/atan/atan2",
        "Trigonometric functions",
        1.0,
        ParityStatus::Passing,
        &["func", "math"],
    );
    b.add(
        "Math: pi/radians/degrees",
        "Constants and conversions",
        0.5,
        ParityStatus::Passing,
        &["func", "math"],
    );
    b.add(
        "Math: mod",
        "Modulo function",
        0.5,
        ParityStatus::Passing,
        &["func", "math"],
    );

    b.build()
}

// ---------------------------------------------------------------------------
// Extension features
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_lines)]
fn build_extension_features() -> Vec<Feature> {
    let mut b = FeatureBuilder::new(FeatureCategory::Extensions);

    // FTS3/FTS4
    b.add(
        "FTS3 basic",
        "Full-text search v3: MATCH queries, ranking",
        2.0,
        ParityStatus::Partial,
        &["ext", "fts3"],
    );
    b.add(
        "FTS3 tokenizers",
        "Built-in tokenizers: simple, porter, unicode61",
        1.5,
        ParityStatus::Partial,
        &["ext", "fts3", "tokenizer"],
    );
    b.add(
        "FTS4 enhancements",
        "FTS4 content tables, languageid, prefix search",
        1.5,
        ParityStatus::Partial,
        &["ext", "fts4"],
    );

    // FTS5
    b.add(
        "FTS5 basic",
        "Full-text search v5: MATCH, BM25 ranking",
        2.0,
        ParityStatus::Partial,
        &["ext", "fts5"],
    );
    b.add(
        "FTS5 auxiliary functions",
        "highlight(), snippet(), bm25()",
        1.5,
        ParityStatus::Partial,
        &["ext", "fts5"],
    );
    b.add(
        "FTS5 tokenizers",
        "Built-in and custom tokenizers",
        1.0,
        ParityStatus::Partial,
        &["ext", "fts5", "tokenizer"],
    );

    // JSON
    b.add(
        "json()",
        "JSON validation and canonicalization",
        2.0,
        ParityStatus::Passing,
        &["ext", "json"],
    );
    b.add(
        "json_extract() / json_type()",
        "Value extraction and type queries",
        2.0,
        ParityStatus::Passing,
        &["ext", "json"],
    );
    b.add(
        "json_set() / json_insert() / json_replace() / json_remove()",
        "JSON mutation functions",
        1.5,
        ParityStatus::Passing,
        &["ext", "json"],
    );
    b.add(
        "json_each() / json_tree()",
        "Table-valued JSON decomposition",
        1.5,
        ParityStatus::Passing,
        &["ext", "json"],
    );
    b.add(
        "json_group_array() / json_group_object()",
        "JSON aggregation",
        1.0,
        ParityStatus::Passing,
        &["ext", "json"],
    );
    b.add(
        "-> and ->> operators",
        "JSON shorthand extraction operators (3.38+)",
        1.5,
        ParityStatus::Missing,
        &["ext", "json"],
    );

    // R-tree
    b.add(
        "R-tree basic",
        "Spatial indexing with rtree virtual table",
        1.5,
        ParityStatus::Partial,
        &["ext", "rtree"],
    );
    b.add(
        "R-tree queries",
        "Range and containment queries",
        1.0,
        ParityStatus::Partial,
        &["ext", "rtree"],
    );
    b.add(
        "Geopoly",
        "Polygon-based spatial queries",
        1.0,
        ParityStatus::Missing,
        &["ext", "rtree", "geopoly"],
    );

    // Session
    b.add(
        "Session changeset",
        "Record and apply changesets",
        1.5,
        ParityStatus::Partial,
        &["ext", "session"],
    );
    b.add(
        "Session patchset",
        "Compact patchset format",
        1.0,
        ParityStatus::Partial,
        &["ext", "session"],
    );
    b.add(
        "Session conflict handling",
        "Conflict handlers for changeset application",
        1.0,
        ParityStatus::Partial,
        &["ext", "session"],
    );

    // ICU
    b.add(
        "ICU collation",
        "Unicode-aware collation sequences",
        1.5,
        ParityStatus::Partial,
        &["ext", "icu"],
    );
    b.add(
        "ICU LIKE/REGEXP",
        "Unicode-aware pattern matching",
        1.0,
        ParityStatus::Missing,
        &["ext", "icu"],
    );

    // Misc
    b.add(
        "generate_series()",
        "Table-valued function for number sequences",
        1.5,
        ParityStatus::Passing,
        &["ext", "misc"],
    );
    b.add(
        "dbstat virtual table",
        "Page-level statistics",
        1.0,
        ParityStatus::Partial,
        &["ext", "misc"],
    );
    b.add(
        "dbpage virtual table",
        "Direct page access",
        1.0,
        ParityStatus::Partial,
        &["ext", "misc"],
    );
    b.add(
        "carray()",
        "C-array binding virtual table",
        0.5,
        ParityStatus::Missing,
        &["ext", "misc"],
    );

    b.build()
}

// ---------------------------------------------------------------------------
// Type system features
// ---------------------------------------------------------------------------

fn build_type_system_features() -> Vec<Feature> {
    let mut b = FeatureBuilder::new(FeatureCategory::TypeSystem);

    b.add(
        "Type affinity rules",
        "Column type affinity: INTEGER, TEXT, BLOB, REAL, NUMERIC",
        3.0,
        ParityStatus::Passing,
        &["type", "affinity"],
    );
    b.add(
        "Type coercion",
        "Implicit type coercion in comparisons and operations",
        2.5,
        ParityStatus::Passing,
        &["type", "coercion"],
    );
    b.add(
        "NULL semantics",
        "Three-valued logic, NULL propagation, IS NULL/IS NOT NULL",
        2.5,
        ParityStatus::Passing,
        &["type", "null"],
    );
    b.add(
        "Integer storage classes",
        "1/2/3/4/6/8-byte integer encoding",
        2.0,
        ParityStatus::Passing,
        &["type", "integer"],
    );
    b.add(
        "Real (IEEE 754)",
        "8-byte big-endian IEEE 754 floating point",
        2.0,
        ParityStatus::Passing,
        &["type", "real"],
    );
    b.add(
        "Text encoding",
        "UTF-8, UTF-16le, UTF-16be with BOM handling",
        2.0,
        ParityStatus::Passing,
        &["type", "encoding"],
    );
    b.add(
        "Blob handling",
        "Binary data storage and comparison",
        1.5,
        ParityStatus::Passing,
        &["type", "blob"],
    );
    b.add(
        "Collation sequences",
        "BINARY, NOCASE, RTRIM built-in collations",
        2.0,
        ParityStatus::Passing,
        &["type", "collation"],
    );
    b.add(
        "STRICT tables",
        "STRICT keyword enforcing column types at insert/update",
        1.5,
        ParityStatus::Partial,
        &["type", "strict"],
    );
    b.add(
        "WITHOUT ROWID tables",
        "Primary key as storage key, no implicit rowid",
        1.5,
        ParityStatus::Partial,
        &["type", "rowid"],
    );
    b.add(
        "Serial type encoding",
        "Variable-length record header type codes",
        2.0,
        ParityStatus::Passing,
        &["type", "serial"],
    );

    b.build()
}

// ---------------------------------------------------------------------------
// File format features
// ---------------------------------------------------------------------------

fn build_file_format_features() -> Vec<Feature> {
    let mut b = FeatureBuilder::new(FeatureCategory::FileFormat);

    b.add(
        "100-byte database header",
        "Magic string, page size, schema format, freelist info",
        3.0,
        ParityStatus::Passing,
        &["format", "header"],
    );
    b.add(
        "B-tree page headers",
        "Page type flag, freeblock pointer, cell count, content area offset",
        2.5,
        ParityStatus::Passing,
        &["format", "btree"],
    );
    b.add(
        "Cell pointer array",
        "Sorted array of 2-byte cell offsets per page",
        2.0,
        ParityStatus::Passing,
        &["format", "cell"],
    );
    b.add(
        "Record format",
        "Type-length header + column values encoding",
        2.5,
        ParityStatus::Passing,
        &["format", "record"],
    );
    b.add(
        "Varint encoding",
        "Huffman-coded variable-length integers (1-9 bytes)",
        2.0,
        ParityStatus::Passing,
        &["format", "varint"],
    );
    b.add(
        "Overflow page chains",
        "4-byte next-page pointers for large records",
        1.5,
        ParityStatus::Passing,
        &["format", "overflow"],
    );
    b.add(
        "Freelist format",
        "Trunk pages with leaf page arrays",
        1.5,
        ParityStatus::Passing,
        &["format", "freelist"],
    );
    b.add(
        "WAL frame format",
        "32-byte frame header + page data",
        2.0,
        ParityStatus::Passing,
        &["format", "wal"],
    );
    b.add(
        "WAL index (SHM) format",
        "Shared memory header + hash tables for frame lookup",
        2.0,
        ParityStatus::Passing,
        &["format", "wal", "shm"],
    );
    b.add(
        "Pointer map pages",
        "Auto-vacuum pointer map for page relocations",
        1.0,
        ParityStatus::Partial,
        &["format", "ptrmap"],
    );
    b.add(
        "Schema table (sqlite_schema)",
        "Root page 1 schema table format",
        2.0,
        ParityStatus::Passing,
        &["format", "schema"],
    );
    b.add(
        "File change counter",
        "Header bytes 24-27 change counter for cache invalidation",
        1.5,
        ParityStatus::Passing,
        &["format", "counter"],
    );

    b.build()
}

// ---------------------------------------------------------------------------
// API & CLI features
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_lines)]
fn build_api_cli_features() -> Vec<Feature> {
    let mut b = FeatureBuilder::new(FeatureCategory::ApiCli);

    // Connection API
    b.add(
        "Connection::open()",
        "Open database by path (file or :memory:)",
        3.0,
        ParityStatus::Passing,
        &["api", "connection"],
    );
    b.add(
        "Connection::execute()",
        "Execute a statement without results",
        3.0,
        ParityStatus::Passing,
        &["api", "execute"],
    );
    b.add(
        "Connection::query()",
        "Execute a query and iterate results",
        3.0,
        ParityStatus::Passing,
        &["api", "query"],
    );
    b.add(
        "Prepared statements",
        "Prepare, bind, step, reset, finalize lifecycle",
        2.5,
        ParityStatus::Passing,
        &["api", "prepared"],
    );
    b.add(
        "Parameter binding",
        "Positional (?N) and named (:name, @name, $name) parameters",
        2.0,
        ParityStatus::Passing,
        &["api", "bind"],
    );
    b.add(
        "Error reporting",
        "Error codes, messages, and extended error codes",
        2.0,
        ParityStatus::Passing,
        &["api", "error"],
    );
    b.add(
        "Custom functions",
        "Register scalar and aggregate functions from Rust",
        1.5,
        ParityStatus::Passing,
        &["api", "udf"],
    );
    b.add(
        "Multi-statement execution",
        "Execute semicolon-separated SQL batch",
        1.5,
        ParityStatus::Passing,
        &["api", "batch"],
    );
    b.add(
        "Authorizer callback",
        "Authorize/deny SQL operations before execution",
        1.0,
        ParityStatus::Partial,
        &["api", "authorizer"],
    );
    b.add(
        "Busy handler",
        "Callback or timeout for lock contention",
        1.5,
        ParityStatus::Partial,
        &["api", "busy"],
    );
    b.add(
        "Backup API",
        "Online database backup to another connection",
        1.5,
        ParityStatus::Missing,
        &["api", "backup"],
    );

    // CLI
    b.add(
        "CLI REPL",
        "Interactive shell with command history",
        1.5,
        ParityStatus::Passing,
        &["cli", "repl"],
    );
    b.add(
        "CLI dot-commands",
        ".tables, .schema, .mode, .headers, .import, .dump, etc.",
        1.5,
        ParityStatus::Partial,
        &["cli", "dotcmd"],
    );
    b.add(
        "CLI output modes",
        "Column, CSV, JSON, table, list output formats",
        1.0,
        ParityStatus::Partial,
        &["cli", "output"],
    );
    b.add(
        "CLI batch mode",
        "Non-interactive SQL file execution",
        1.0,
        ParityStatus::Passing,
        &["cli", "batch"],
    );

    b.build()
}

// ---------------------------------------------------------------------------
// Reviewer checklist
// ---------------------------------------------------------------------------

/// Reviewer checklist for taxonomy completeness audits.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReviewerChecklist {
    /// All items in the checklist.
    pub items: Vec<ChecklistItem>,
}

/// A single checklist item.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChecklistItem {
    /// Category being checked.
    pub category: String,
    /// What to verify.
    pub question: String,
    /// Whether the check passes.
    pub passes: bool,
    /// Notes from the reviewer.
    pub notes: String,
}

/// Build the default reviewer checklist for taxonomy validation.
#[must_use]
pub fn build_reviewer_checklist(universe: &FeatureUniverse) -> ReviewerChecklist {
    let score = universe.compute_score();
    let violations = universe.validate();

    let mut items = vec![
        ChecklistItem {
            category: "Schema".to_owned(),
            question: "Schema version is set and non-zero?".to_owned(),
            passes: universe.schema_version > 0,
            notes: format!("v{}", universe.schema_version),
        },
        ChecklistItem {
            category: "Schema".to_owned(),
            question: "Target SQLite version is specified?".to_owned(),
            passes: !universe.target_sqlite_version.is_empty(),
            notes: universe.target_sqlite_version.clone(),
        },
        ChecklistItem {
            category: "Coverage".to_owned(),
            question: "All 9 feature categories have features?".to_owned(),
            passes: FeatureCategory::ALL
                .iter()
                .all(|c| !universe.features_by_category(*c).is_empty()),
            notes: format!("{} total features", universe.features.len()),
        },
        ChecklistItem {
            category: "Coverage".to_owned(),
            question: "Category global weights sum to 1.0?".to_owned(),
            passes: {
                let s: f64 = FeatureCategory::ALL.iter().map(|c| c.global_weight()).sum();
                (s - 1.0).abs() < 1e-9
            },
            notes: String::new(),
        },
        ChecklistItem {
            category: "Invariants".to_owned(),
            question: "No taxonomy validation violations?".to_owned(),
            passes: violations.is_empty(),
            notes: if violations.is_empty() {
                "clean".to_owned()
            } else {
                violations.join("; ")
            },
        },
        ChecklistItem {
            category: "Scoring".to_owned(),
            question: "Global parity score is computable?".to_owned(),
            passes: score.global_score.is_finite(),
            notes: format!("{:.4}", score.global_score),
        },
        ChecklistItem {
            category: "Exclusions".to_owned(),
            question: "All excluded features have rationale?".to_owned(),
            passes: universe
                .features_by_status(ParityStatus::Excluded)
                .iter()
                .all(|f| f.exclusion.is_some()),
            notes: format!("{} excluded features", score.status_counts.excluded),
        },
    ];

    // Per-category coverage items.
    for cat in FeatureCategory::ALL {
        let feats = universe.features_by_category(cat);
        items.push(ChecklistItem {
            category: "Category Coverage".to_owned(),
            question: format!("{} has sufficient features?", cat.display_name()),
            passes: feats.len() >= 3,
            notes: format!("{} features", feats.len()),
        });
    }

    ReviewerChecklist { items }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_BEAD: &str = "bd-1dp9.1.1";

    #[test]
    fn taxonomy_builds_without_panic() {
        let universe = build_canonical_universe();
        assert!(
            !universe.features.is_empty(),
            "[{TEST_BEAD}] universe must contain features"
        );
    }

    #[test]
    fn taxonomy_schema_version_set() {
        let universe = build_canonical_universe();
        assert_eq!(
            universe.schema_version, TAXONOMY_SCHEMA_VERSION,
            "[{TEST_BEAD}] schema version must match constant"
        );
    }

    #[test]
    fn taxonomy_target_version_is_sqlite_3() {
        let universe = build_canonical_universe();
        assert!(
            universe.target_sqlite_version.starts_with("3."),
            "[{TEST_BEAD}] target must be SQLite 3.x, got {}",
            universe.target_sqlite_version
        );
    }

    #[test]
    fn taxonomy_all_categories_populated() {
        let universe = build_canonical_universe();
        for cat in FeatureCategory::ALL {
            let count = universe.features_by_category(cat).len();
            assert!(count > 0, "[{TEST_BEAD}] category {cat} has 0 features");
        }
    }

    #[test]
    fn taxonomy_feature_ids_unique() {
        let universe = build_canonical_universe();
        // BTreeMap guarantees unique keys, but verify via count.
        let all_ids: Vec<_> = universe.features.keys().collect();
        let unique: BTreeSet<_> = all_ids.iter().collect();
        assert_eq!(
            all_ids.len(),
            unique.len(),
            "[{TEST_BEAD}] duplicate feature IDs detected"
        );
    }

    #[test]
    fn taxonomy_weights_positive() {
        let universe = build_canonical_universe();
        for feat in universe.features.values() {
            assert!(
                feat.weight > 0.0,
                "[{TEST_BEAD}] feature {} has non-positive weight {}",
                feat.id,
                feat.weight
            );
        }
    }

    #[test]
    fn taxonomy_category_global_weights_sum_to_one() {
        let sum: f64 = FeatureCategory::ALL.iter().map(|c| c.global_weight()).sum();
        assert!(
            (sum - 1.0).abs() < 1e-9,
            "[{TEST_BEAD}] category global weights sum to {sum}, expected 1.0"
        );
    }

    #[test]
    fn taxonomy_excluded_features_have_rationale() {
        let universe = build_canonical_universe();
        for feat in universe.features_by_status(ParityStatus::Excluded) {
            assert!(
                feat.exclusion.is_some(),
                "[{TEST_BEAD}] excluded feature {} missing rationale",
                feat.id
            );
        }
    }

    #[test]
    fn taxonomy_validation_passes() {
        let universe = build_canonical_universe();
        let violations = universe.validate();
        assert!(
            violations.is_empty(),
            "[{TEST_BEAD}] taxonomy validation failed: {:?}",
            violations
        );
    }

    #[test]
    fn taxonomy_score_computable() {
        let universe = build_canonical_universe();
        let score = universe.compute_score();
        assert!(
            score.global_score >= 0.0 && score.global_score <= 1.0,
            "[{TEST_BEAD}] global score out of range: {}",
            score.global_score
        );
        assert!(
            score.global_score.is_finite(),
            "[{TEST_BEAD}] global score is not finite"
        );
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn taxonomy_score_deterministic() {
        let u1 = build_canonical_universe();
        let u2 = build_canonical_universe();
        let s1 = u1.compute_score();
        let s2 = u2.compute_score();
        assert_eq!(
            s1.global_score, s2.global_score,
            "[{TEST_BEAD}] score must be deterministic across builds"
        );
    }

    #[test]
    fn taxonomy_per_category_scores_in_range() {
        let universe = build_canonical_universe();
        let score = universe.compute_score();
        for (name, cs) in &score.category_scores {
            assert!(
                cs.score >= 0.0 && cs.score <= 1.0,
                "[{TEST_BEAD}] category {name} score out of range: {}",
                cs.score
            );
        }
    }

    #[test]
    fn taxonomy_status_counts_consistent() {
        let universe = build_canonical_universe();
        let score = universe.compute_score();
        let sc = &score.status_counts;
        assert_eq!(
            sc.total,
            sc.passing + sc.partial + sc.missing + sc.excluded,
            "[{TEST_BEAD}] status counts do not sum to total"
        );
        assert_eq!(
            sc.total,
            universe.features.len(),
            "[{TEST_BEAD}] total count mismatch"
        );
    }

    #[test]
    fn taxonomy_json_roundtrip() {
        let universe = build_canonical_universe();
        let json = universe.to_json().expect("serialisation must succeed");
        let restored = FeatureUniverse::from_json(&json).expect("deserialisation must succeed");
        assert_eq!(
            universe.features.len(),
            restored.features.len(),
            "[{TEST_BEAD}] feature count mismatch after JSON roundtrip"
        );
        assert_eq!(
            universe.schema_version, restored.schema_version,
            "[{TEST_BEAD}] schema version mismatch after JSON roundtrip"
        );
    }

    #[test]
    fn taxonomy_filter_by_tag() {
        let universe = build_canonical_universe();
        let mvcc_features = universe.features_by_tag("mvcc");
        assert!(
            !mvcc_features.is_empty(),
            "[{TEST_BEAD}] no features tagged 'mvcc'"
        );
        for feat in &mvcc_features {
            assert!(
                feat.tags.contains("mvcc"),
                "[{TEST_BEAD}] feature {} in mvcc results but missing tag",
                feat.id
            );
        }
    }

    #[test]
    fn taxonomy_reviewer_checklist_all_pass() {
        let universe = build_canonical_universe();
        let checklist = build_reviewer_checklist(&universe);
        for item in &checklist.items {
            assert!(
                item.passes,
                "[{TEST_BEAD}] checklist item failed: {} — {}{}",
                item.category,
                item.question,
                if item.notes.is_empty() {
                    String::new()
                } else {
                    format!(" ({})", item.notes)
                }
            );
        }
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn taxonomy_truncate_score_determinism() {
        // Verify cross-platform truncation consistency.
        assert_eq!(truncate_score(0.123_456_789), 0.123_456);
        assert_eq!(truncate_score(1.0), 1.0);
        assert_eq!(truncate_score(0.0), 0.0);
        assert_eq!(truncate_score(0.999_999_999), 0.999_999);
    }

    #[test]
    fn taxonomy_concurrent_mode_default_on() {
        // Verify the taxonomy correctly marks concurrent writer features as Passing.
        let universe = build_canonical_universe();
        let concurrent_features = universe.features_by_tag("concurrent");
        assert!(
            !concurrent_features.is_empty(),
            "[{TEST_BEAD}] must have concurrent-tagged features"
        );
        for feat in &concurrent_features {
            assert_eq!(
                feat.status,
                ParityStatus::Passing,
                "[{TEST_BEAD}] concurrent feature {} must be Passing, got {}",
                feat.id,
                feat.status
            );
        }
    }

    #[test]
    fn taxonomy_minimum_feature_count() {
        // Sanity check: the SQLite surface is large. We should have 100+ features.
        let universe = build_canonical_universe();
        assert!(
            universe.features.len() >= 100,
            "[{TEST_BEAD}] expected 100+ features, got {}",
            universe.features.len()
        );
    }
}
