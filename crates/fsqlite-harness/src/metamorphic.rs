//! Metamorphic SQL generator and property suites (bd-1dp9.2.2, bd-mblr.7.1.1).
//!
//! Metamorphic testing generates semantically equivalent SQL rewrites from
//! existing corpus entries. If the oracle (C SQLite) and FrankenSQLite
//! agree on the original query but disagree on the rewrite, we have found
//! a parity gap.
//!
//! # Transforms
//!
//! Each [`MetamorphicTransform`] has:
//! - A unique name for triage tags
//! - A soundness proof sketch explaining why the rewrite preserves semantics
//! - Taxonomy feature IDs that the transform exercises
//! - A [`TransformFamily`] classifying its rewrite strategy
//! - An [`EquivalenceExpectation`] defining what "same result" means
//! - An `apply` method that rewrites SQL statements
//!
//! # Transform Families
//!
//! Transforms are grouped into families by what part of the SQL they modify:
//! - [`TransformFamily::Predicate`] — WHERE/HAVING clause rewrites
//! - [`TransformFamily::Projection`] — SELECT column list rewrites
//! - [`TransformFamily::Structural`] — query structure changes (subquery, UNION)
//! - [`TransformFamily::Literal`] — literal value/type rewrites
//!
//! # Mismatch Classification
//!
//! When oracle and subject disagree on a metamorphic test case, the
//! [`MismatchClassification`] enum categorizes the divergence for triage.
//!
//! # Determinism
//!
//! All transforms are deterministic given the same input and seed. Seeds
//! are derived from [`derive_entry_seed`](crate::corpus_ingest::derive_entry_seed)
//! to ensure reproducibility. The [`SeedContract`] formalizes this guarantee.

use std::collections::BTreeSet;
use std::fmt;

use serde::{Deserialize, Serialize};

use crate::corpus_ingest::{CorpusEntry, CorpusSource, Family, derive_entry_seed};

/// Bead identifier for log correlation.
#[allow(dead_code)]
const BEAD_ID: &str = "bd-1dp9.2.2";

/// Bead identifier for the workload grammar / oracle contract additions.
#[allow(dead_code)]
const BEAD_ID_GRAMMAR: &str = "bd-mblr.7.1.1";

// ===========================================================================
// Transform Families (bd-mblr.7.1.1)
// ===========================================================================

/// Semantic family grouping for metamorphic transforms.
///
/// Each family identifies *what* part of the SQL a transform modifies.
/// Families enable stratified coverage analysis: a well-covered corpus
/// should exercise transforms from every family.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum TransformFamily {
    /// Transforms that modify WHERE/HAVING predicates without changing
    /// the projected columns or query structure.
    Predicate,
    /// Transforms that wrap or rewrite SELECT-list expressions without
    /// altering the filter or query structure.
    Projection,
    /// Transforms that change the overall query structure (wrapping in
    /// a subquery, adding compound operators like INTERSECT).
    Structural,
    /// Transforms that rewrite literal values or their type annotations
    /// without changing semantics (e.g., `42` → `CAST(42 AS INTEGER)`).
    Literal,
}

impl TransformFamily {
    /// All valid family variants for iteration.
    pub const ALL: [Self; 4] = [
        Self::Predicate,
        Self::Projection,
        Self::Structural,
        Self::Literal,
    ];
}

impl fmt::Display for TransformFamily {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Predicate => write!(f, "predicate"),
            Self::Projection => write!(f, "projection"),
            Self::Structural => write!(f, "structural"),
            Self::Literal => write!(f, "literal"),
        }
    }
}

// ===========================================================================
// Equivalence Expectations (bd-mblr.7.1.1)
// ===========================================================================

/// Defines what "semantically equivalent" means for a particular transform.
///
/// Different transforms produce different equivalence guarantees. A
/// `SubqueryWrap` preserves the multiset of rows but not necessarily
/// their order, while a `TautologicalPredicate` preserves exact row
/// order because it does not change the query plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum EquivalenceExpectation {
    /// Exact row-by-row match including order. The rewritten query must
    /// produce identical rows in the same sequence.
    ExactRowMatch,
    /// Same multiset of rows (duplicates count, order irrelevant). This
    /// is the correct expectation when the rewrite may change the query
    /// plan and thus the implicit row ordering.
    MultisetEquivalence,
    /// Same set of distinct rows (duplicates and order irrelevant). Used
    /// when the transform may affect duplicate elimination (e.g.,
    /// `INTERSECT` implicitly applies `DISTINCT`).
    SetEquivalence,
    /// Values are equivalent after SQLite type affinity coercion. Used
    /// when the transform changes the type annotation but not the
    /// underlying value (e.g., `CAST(42 AS INTEGER)`).
    TypeCoercionEquivalent,
}

impl fmt::Display for EquivalenceExpectation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ExactRowMatch => write!(f, "exact_row_match"),
            Self::MultisetEquivalence => write!(f, "multiset_equivalence"),
            Self::SetEquivalence => write!(f, "set_equivalence"),
            Self::TypeCoercionEquivalent => write!(f, "type_coercion_equivalent"),
        }
    }
}

// ===========================================================================
// Mismatch Classification (bd-mblr.7.1.1)
// ===========================================================================

/// Classification of result mismatches between oracle and subject.
///
/// When a metamorphic test case produces different results from the oracle
/// (C SQLite) and the subject (FrankenSQLite), the mismatch is classified
/// to guide triage priority and resolution strategy.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum MismatchClassification {
    /// Real semantic divergence: the subject produces incorrect results.
    /// This is a true bug that must be fixed.
    TrueDivergence {
        /// Brief description of the nature of the divergence.
        description: String,
    },
    /// Row ordering differs but the multiset of rows matches. This is
    /// acceptable when the query has no ORDER BY clause, since SQLite
    /// does not guarantee row order without it.
    OrderDependentDifference,
    /// Type representation differs (e.g., `"1"` vs `1`) but the values
    /// are equivalent under SQLite type affinity rules.
    TypeAffinityDifference,
    /// NULL handling edge case: one engine treats an expression as NULL
    /// while the other produces a non-NULL value, or vice versa.
    NullHandlingDifference,
    /// Floating-point precision difference within acceptable epsilon.
    /// The string stores the epsilon as a decimal (e.g., `"0.001"`).
    FloatingPointDifference {
        /// Maximum observed epsilon between values, as string for Hash/Eq.
        max_epsilon_str: String,
    },
    /// Infrastructure or tool issue, not a real semantic divergence.
    /// Examples: timeout, memory limit, missing extension.
    FalsePositive {
        /// Reason this is a false positive.
        reason: String,
    },
}

impl fmt::Display for MismatchClassification {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TrueDivergence { description } => {
                write!(f, "true_divergence: {description}")
            }
            Self::OrderDependentDifference => write!(f, "order_dependent_difference"),
            Self::TypeAffinityDifference => write!(f, "type_affinity_difference"),
            Self::NullHandlingDifference => write!(f, "null_handling_difference"),
            Self::FloatingPointDifference { max_epsilon_str } => {
                write!(f, "floating_point_difference(eps={max_epsilon_str})")
            }
            Self::FalsePositive { reason } => write!(f, "false_positive: {reason}"),
        }
    }
}

impl MismatchClassification {
    /// Whether this classification represents a real bug that needs fixing.
    #[must_use]
    pub fn is_actionable(&self) -> bool {
        matches!(self, Self::TrueDivergence { .. })
    }

    /// Triage priority (lower = more urgent). True divergences are highest
    /// priority; false positives are lowest.
    #[must_use]
    pub fn triage_priority(&self) -> u8 {
        match self {
            Self::TrueDivergence { .. } => 0,
            Self::NullHandlingDifference => 1,
            Self::TypeAffinityDifference => 2,
            Self::FloatingPointDifference { .. } => 3,
            Self::OrderDependentDifference => 4,
            Self::FalsePositive { .. } => 5,
        }
    }
}

/// Classify a mismatch given the equivalence expectation and the observed
/// result difference.
///
/// This is a rule-based classifier. The `oracle_rows` and `subject_rows`
/// are string representations of result rows for comparison.
#[must_use]
pub fn classify_mismatch(
    expectation: EquivalenceExpectation,
    oracle_rows: &[Vec<String>],
    subject_rows: &[Vec<String>],
) -> MismatchClassification {
    // Exact match — no mismatch (caller should check before calling).
    if oracle_rows == subject_rows {
        return MismatchClassification::FalsePositive {
            reason: "results are identical".to_owned(),
        };
    }

    // Check for order-only difference.
    let mut oracle_sorted = oracle_rows.to_vec();
    let mut subject_sorted = subject_rows.to_vec();
    oracle_sorted.sort();
    subject_sorted.sort();

    if oracle_sorted == subject_sorted {
        return if expectation == EquivalenceExpectation::ExactRowMatch {
            // Caller expected exact order but got multiset match.
            MismatchClassification::OrderDependentDifference
        } else {
            // Multiset/set equivalence — this is acceptable.
            MismatchClassification::FalsePositive {
                reason: "multiset match under non-exact expectation".to_owned(),
            }
        };
    }

    // Check for set equivalence (dedup then compare).
    let oracle_set: BTreeSet<&Vec<String>> = oracle_rows.iter().collect();
    let subject_set: BTreeSet<&Vec<String>> = subject_rows.iter().collect();

    if oracle_set == subject_set && expectation == EquivalenceExpectation::SetEquivalence {
        return MismatchClassification::FalsePositive {
            reason: "set match under set equivalence expectation".to_owned(),
        };
    }

    // Check for NULL handling differences (one side has NULL where other doesn't).
    let has_null_diff = oracle_rows.iter().zip(subject_rows.iter()).any(|(o, s)| {
        o.iter()
            .zip(s.iter())
            .any(|(ov, sv)| (ov.is_empty() || ov == "NULL") != (sv.is_empty() || sv == "NULL"))
    });
    if has_null_diff && oracle_rows.len() == subject_rows.len() {
        return MismatchClassification::NullHandlingDifference;
    }

    // Check for type affinity differences (numeric string vs integer).
    if oracle_rows.len() == subject_rows.len()
        && expectation == EquivalenceExpectation::TypeCoercionEquivalent
    {
        let all_affinity_match = oracle_rows.iter().zip(subject_rows.iter()).all(|(o, s)| {
            o.len() == s.len()
                && o.iter()
                    .zip(s.iter())
                    .all(|(ov, sv)| values_affinity_equal(ov, sv))
        });
        if all_affinity_match {
            return MismatchClassification::TypeAffinityDifference;
        }
    }

    // Default: true divergence.
    MismatchClassification::TrueDivergence {
        description: format!(
            "oracle returned {} rows, subject returned {} rows",
            oracle_rows.len(),
            subject_rows.len()
        ),
    }
}

/// Check if two string values are equivalent under SQLite type affinity.
fn values_affinity_equal(a: &str, b: &str) -> bool {
    if a == b {
        return true;
    }
    // Try numeric comparison.
    if let (Ok(fa), Ok(fb)) = (a.parse::<f64>(), b.parse::<f64>()) {
        return (fa - fb).abs() < f64::EPSILON;
    }
    false
}

// ===========================================================================
// Seed Contract (bd-mblr.7.1.1)
// ===========================================================================

/// Formalizes the deterministic seed contract for metamorphic testing.
///
/// The seed contract guarantees that given the same `base_seed`, corpus
/// entry index, and transform index, the generated test case is identical
/// across runs and platforms.
///
/// # Derivation
///
/// Seeds are derived using [`derive_entry_seed`] from `corpus_ingest`:
/// `case_seed = derive_entry_seed(base_seed, entry_idx * 1000 + tx_idx)`.
///
/// # Replay
///
/// Given a `MetamorphicTestCase`, the replay command reconstructs the
/// exact same test case by re-running `generate_metamorphic_corpus` with
/// the recorded seed and filtering by case ID.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SeedContract {
    /// Base seed for corpus generation.
    pub base_seed: u64,
    /// Per-case derived seed.
    pub case_seed: u64,
    /// Entry index in the corpus.
    pub entry_idx: usize,
    /// Transform index in the registry.
    pub transform_idx: usize,
}

impl SeedContract {
    /// Create a new seed contract for a specific case.
    #[must_use]
    pub fn new(base_seed: u64, entry_idx: usize, transform_idx: usize) -> Self {
        #[allow(clippy::cast_possible_truncation)]
        let case_seed = derive_entry_seed(base_seed, (entry_idx * 1000 + transform_idx) as u32);
        Self {
            base_seed,
            case_seed,
            entry_idx,
            transform_idx,
        }
    }

    /// Validate that a test case's seed matches this contract.
    #[must_use]
    pub fn validates(&self, test_case: &MetamorphicTestCase) -> bool {
        test_case.seed == self.case_seed
    }

    /// Replay command to reproduce this specific case.
    #[must_use]
    pub fn replay_command(&self, case_id: &str) -> String {
        format!("cargo test -p fsqlite-harness -- metamorphic --nocapture 2>&1 | grep '{case_id}'")
    }
}

/// Oracle result contract: captures expected behavior for a test case.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OracleContract {
    /// The test case this contract applies to.
    pub case_id: String,
    /// Transform family.
    pub family: TransformFamily,
    /// Expected equivalence type.
    pub equivalence: EquivalenceExpectation,
    /// Seed contract for reproducibility.
    pub seed: SeedContract,
}

// ---------------------------------------------------------------------------
// Transform trait
// ---------------------------------------------------------------------------

/// A metamorphic transform that produces semantically equivalent SQL.
pub trait MetamorphicTransform: fmt::Debug {
    /// Transform name for triage tags.
    fn name(&self) -> &str;

    /// Brief proof sketch explaining why the rewrite preserves semantics.
    fn soundness_sketch(&self) -> &str;

    /// Taxonomy feature IDs this transform exercises.
    fn feature_tags(&self) -> Vec<&str>;

    /// Semantic family this transform belongs to (bd-mblr.7.1.1).
    fn family(&self) -> TransformFamily;

    /// What kind of result equivalence this transform guarantees (bd-mblr.7.1.1).
    fn equivalence_expectation(&self) -> EquivalenceExpectation;

    /// Apply the transform to a single SQL statement.
    ///
    /// Returns `Some(rewritten_sql)` if the transform is applicable,
    /// `None` if the statement is not a valid target for this transform.
    fn apply_one(&self, sql: &str, seed: u64) -> Option<String>;
}

// ---------------------------------------------------------------------------
// Transform registry
// ---------------------------------------------------------------------------

/// Registry of all available metamorphic transforms.
#[derive(Debug)]
pub struct TransformRegistry {
    transforms: Vec<Box<dyn MetamorphicTransform>>,
}

impl Default for TransformRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl TransformRegistry {
    /// Create a registry with all built-in transforms.
    #[must_use]
    pub fn new() -> Self {
        Self {
            transforms: vec![
                Box::new(SubqueryWrap),
                Box::new(TautologicalPredicate),
                Box::new(DoubleNegation),
                Box::new(CoalesceIdentity),
                Box::new(UnionSelfIntersect),
                Box::new(CastLiteralIdentity),
                Box::new(ExpressionCommute),
                Box::new(NullCoalesce),
            ],
        }
    }

    /// All registered transforms.
    #[must_use]
    pub fn transforms(&self) -> &[Box<dyn MetamorphicTransform>] {
        &self.transforms
    }

    /// Look up a transform by name.
    #[must_use]
    pub fn by_name(&self, name: &str) -> Option<&dyn MetamorphicTransform> {
        self.transforms
            .iter()
            .find(|t| t.name() == name)
            .map(|b| b.as_ref())
    }
}

// ---------------------------------------------------------------------------
// Test case
// ---------------------------------------------------------------------------

/// A metamorphic test case: original + rewritten SQL with metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetamorphicTestCase {
    /// Unique identifier (content-based).
    pub id: String,
    /// Original SQL statements.
    pub original: Vec<String>,
    /// Rewritten SQL statements.
    pub transformed: Vec<String>,
    /// Transform name that produced this case.
    pub transform_name: String,
    /// Chain of transforms for composed rewrites.
    pub transform_chain: Vec<String>,
    /// Deterministic seed.
    pub seed: u64,
    /// Taxonomy family of the original.
    pub family: Family,
    /// Feature tags from the transform.
    pub feature_tags: Vec<String>,
    /// Source corpus entry ID (provenance).
    pub source_entry_id: String,
    /// Transform family classification (bd-mblr.7.1.1).
    pub transform_family: TransformFamily,
    /// Expected equivalence type for oracle comparison (bd-mblr.7.1.1).
    pub equivalence: EquivalenceExpectation,
}

// ---------------------------------------------------------------------------
// Generation
// ---------------------------------------------------------------------------

/// Generate metamorphic test cases from corpus entries.
///
/// For each entry, tries each applicable transform and produces a test case.
/// Returns at most `max_per_entry` cases per corpus entry.
#[must_use]
pub fn generate_metamorphic_corpus(
    entries: &[CorpusEntry],
    registry: &TransformRegistry,
    base_seed: u64,
    max_per_entry: usize,
) -> Vec<MetamorphicTestCase> {
    let mut cases = Vec::new();

    for (entry_idx, entry) in entries.iter().enumerate() {
        if entry.skip.is_some() {
            continue;
        }

        let mut entry_cases = 0_usize;

        for (tx_idx, transform) in registry.transforms().iter().enumerate() {
            if entry_cases >= max_per_entry {
                break;
            }

            #[allow(clippy::cast_possible_truncation)]
            let case_seed = derive_entry_seed(base_seed, (entry_idx * 1000 + tx_idx) as u32);

            // Try to transform each statement; collect successful rewrites.
            let mut transformed = Vec::new();
            let mut any_changed = false;

            for stmt in &entry.statements {
                if let Some(rewritten) = transform.apply_one(stmt, case_seed) {
                    any_changed = true;
                    transformed.push(rewritten);
                } else {
                    transformed.push(stmt.clone());
                }
            }

            if !any_changed {
                continue;
            }

            let id = format!(
                "mm-{}-{}-{:08x}",
                entry.id,
                transform.name(),
                case_seed & 0xFFFF_FFFF
            );

            cases.push(MetamorphicTestCase {
                id,
                original: entry.statements.clone(),
                transformed,
                transform_name: transform.name().to_owned(),
                transform_chain: vec![transform.name().to_owned()],
                seed: case_seed,
                family: entry.family,
                feature_tags: transform
                    .feature_tags()
                    .iter()
                    .map(|s| (*s).to_owned())
                    .collect(),
                source_entry_id: entry.id.clone(),
                transform_family: transform.family(),
                equivalence: transform.equivalence_expectation(),
            });

            entry_cases += 1;
        }
    }

    cases
}

/// Compose two transforms into a chained test case.
///
/// Returns `None` if neither transform applies.
#[must_use]
pub fn compose_transforms(
    sql: &[String],
    first: &dyn MetamorphicTransform,
    second: &dyn MetamorphicTransform,
    seed: u64,
) -> Option<Vec<String>> {
    let mut result = Vec::new();
    let mut any_changed = false;

    for stmt in sql {
        let step1 = first.apply_one(stmt, seed).unwrap_or_else(|| stmt.clone());
        let step2 = second
            .apply_one(&step1, seed.wrapping_add(1))
            .unwrap_or(step1);

        if &step2 != stmt {
            any_changed = true;
        }
        result.push(step2);
    }

    if any_changed { Some(result) } else { None }
}

/// Coverage report: which transforms applied to which families.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MetamorphicCoverage {
    /// Total test cases generated.
    pub total_cases: usize,
    /// Cases per transform name.
    pub by_transform: std::collections::BTreeMap<String, usize>,
    /// Cases per family.
    pub by_family: std::collections::BTreeMap<String, usize>,
    /// Feature tags covered.
    pub feature_tags: BTreeSet<String>,
    /// Cases per transform family (bd-mblr.7.1.1).
    pub by_transform_family: std::collections::BTreeMap<String, usize>,
    /// Cases per equivalence expectation (bd-mblr.7.1.1).
    pub by_equivalence: std::collections::BTreeMap<String, usize>,
}

/// Compute coverage statistics from generated test cases.
#[must_use]
pub fn compute_coverage(cases: &[MetamorphicTestCase]) -> MetamorphicCoverage {
    let mut cov = MetamorphicCoverage {
        total_cases: cases.len(),
        ..Default::default()
    };

    for case in cases {
        *cov.by_transform
            .entry(case.transform_name.clone())
            .or_insert(0) += 1;
        *cov.by_family.entry(case.family.to_string()).or_insert(0) += 1;
        for tag in &case.feature_tags {
            cov.feature_tags.insert(tag.clone());
        }
        *cov.by_transform_family
            .entry(case.transform_family.to_string())
            .or_insert(0) += 1;
        *cov.by_equivalence
            .entry(case.equivalence.to_string())
            .or_insert(0) += 1;
    }

    cov
}

/// Helper to create a [`CorpusEntry`] from a metamorphic test case for
/// integration with the corpus builder.
#[must_use]
pub fn test_case_to_entry(case: &MetamorphicTestCase) -> CorpusEntry {
    CorpusEntry {
        id: case.id.clone(),
        family: case.family,
        secondary_families: Vec::new(),
        source: CorpusSource::Generated {
            generator: format!("metamorphic:{}", case.transform_name),
            seed: case.seed,
        },
        statements: case.transformed.clone(),
        seed: case.seed,
        skip: None,
        taxonomy_features: case.feature_tags.clone(),
        description: format!(
            "Metamorphic rewrite ({}) of {}",
            case.transform_name, case.source_entry_id
        ),
    }
}

// ===========================================================================
// Built-in transforms
// ===========================================================================

// ---------------------------------------------------------------------------
// 1. SubqueryWrap
// ---------------------------------------------------------------------------

/// Wrap a SELECT statement in a derived table: `SELECT * FROM (original) AS _sub`.
///
/// # Soundness
///
/// A derived table (subquery in FROM) preserves the result set of the inner
/// query. The outer `SELECT *` projects all columns unchanged. This is
/// semantically equivalent for any SELECT that does not use ORDER BY or LIMIT,
/// since SQLite does not guarantee row order without ORDER BY.
///
/// For SELECTs with ORDER BY or LIMIT, wrapping may change semantics because
/// ORDER BY applies to the inner subquery but SQLite may not preserve it in
/// the outer query. We therefore only apply this to SELECTs without trailing
/// ORDER BY/LIMIT.
#[derive(Debug)]
pub struct SubqueryWrap;

impl MetamorphicTransform for SubqueryWrap {
    fn name(&self) -> &str {
        "subquery_wrap"
    }

    fn soundness_sketch(&self) -> &str {
        "SELECT * FROM (Q) AS _sub = Q for any Q without ORDER BY/LIMIT. \
         The derived table preserves the multiset of rows."
    }

    fn feature_tags(&self) -> Vec<&str> {
        vec!["F-SQL.7", "F-SQL.9"]
    }

    fn family(&self) -> TransformFamily {
        TransformFamily::Structural
    }

    fn equivalence_expectation(&self) -> EquivalenceExpectation {
        EquivalenceExpectation::MultisetEquivalence
    }

    fn apply_one(&self, sql: &str, _seed: u64) -> Option<String> {
        let upper = sql.trim().to_uppercase();
        if !upper.starts_with("SELECT") {
            return None;
        }
        // Skip if ORDER BY or LIMIT present (semantics may change).
        if upper.contains("ORDER BY") || upper.contains(" LIMIT ") || upper.ends_with("LIMIT") {
            return None;
        }
        Some(format!("SELECT * FROM ({sql}) AS _sub"))
    }
}

// ---------------------------------------------------------------------------
// 2. TautologicalPredicate
// ---------------------------------------------------------------------------

/// Add a tautological predicate: `WHERE 1=1` or `AND 1=1`.
///
/// # Soundness
///
/// The expression `1=1` evaluates to TRUE for all rows. Adding it to a
/// WHERE clause with AND does not change the filter. For queries without a
/// WHERE clause, `WHERE 1=1` selects all rows (the default).
#[derive(Debug)]
pub struct TautologicalPredicate;

impl MetamorphicTransform for TautologicalPredicate {
    fn name(&self) -> &str {
        "tautological_predicate"
    }

    fn soundness_sketch(&self) -> &str {
        "WHERE (P AND 1=1) = WHERE P, since 1=1 is TRUE for all rows. \
         WHERE 1=1 selects all rows."
    }

    fn feature_tags(&self) -> Vec<&str> {
        vec!["F-SQL.2", "F-SQL.39"]
    }

    fn family(&self) -> TransformFamily {
        TransformFamily::Predicate
    }

    fn equivalence_expectation(&self) -> EquivalenceExpectation {
        EquivalenceExpectation::ExactRowMatch
    }

    fn apply_one(&self, sql: &str, _seed: u64) -> Option<String> {
        let upper = sql.trim().to_uppercase();
        if !upper.starts_with("SELECT") {
            return None;
        }

        let trimmed = sql.trim();

        // Find WHERE clause position (case-insensitive).
        if let Some(where_pos) = find_keyword_position(trimmed, "WHERE") {
            // Insert ` AND 1=1` after the WHERE clause conditions.
            // We need to find the end of the WHERE clause (before GROUP BY,
            // ORDER BY, LIMIT, HAVING, or end of string).
            let after_where = &trimmed[where_pos + 5..];
            if let Some(end_pos) = find_clause_boundary(after_where) {
                let insert_pos = where_pos + 5 + end_pos;
                Some(format!(
                    "{} AND 1=1{}",
                    &trimmed[..insert_pos],
                    &trimmed[insert_pos..]
                ))
            } else {
                Some(format!("{trimmed} AND 1=1"))
            }
        } else {
            // No WHERE clause — find the right place to insert one.
            // Insert before GROUP BY, ORDER BY, LIMIT, HAVING, or at end.
            if let Some(pos) =
                find_first_clause(trimmed, &["GROUP BY", "ORDER BY", "LIMIT", "HAVING"])
            {
                Some(format!(
                    "{} WHERE 1=1 {}",
                    trimmed[..pos].trim_end(),
                    &trimmed[pos..]
                ))
            } else {
                Some(format!("{trimmed} WHERE 1=1"))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// 3. DoubleNegation
// ---------------------------------------------------------------------------

/// Wrap WHERE condition in `NOT(NOT(...))`.
///
/// # Soundness
///
/// In SQL's three-valued logic:
/// - `NOT(NOT(TRUE))` = TRUE
/// - `NOT(NOT(FALSE))` = FALSE
/// - `NOT(NOT(NULL))` = NULL
///
/// Since WHERE treats NULL as FALSE, a row filtered by `WHERE P` is also
/// filtered by `WHERE NOT(NOT(P))`: both exclude exactly the NULL and FALSE
/// rows.
#[derive(Debug)]
pub struct DoubleNegation;

impl MetamorphicTransform for DoubleNegation {
    fn name(&self) -> &str {
        "double_negation"
    }

    fn soundness_sketch(&self) -> &str {
        "NOT(NOT(P)) has the same truth value as P in SQL's three-valued \
         logic. NULL maps to NULL under double negation, and WHERE treats \
         both NULL and FALSE as exclusion."
    }

    fn feature_tags(&self) -> Vec<&str> {
        vec!["F-SQL.39", "F-SQL.43"]
    }

    fn family(&self) -> TransformFamily {
        TransformFamily::Predicate
    }

    fn equivalence_expectation(&self) -> EquivalenceExpectation {
        EquivalenceExpectation::ExactRowMatch
    }

    fn apply_one(&self, sql: &str, _seed: u64) -> Option<String> {
        let trimmed = sql.trim();
        let upper = trimmed.to_uppercase();
        if !upper.starts_with("SELECT") {
            return None;
        }

        let where_pos = find_keyword_position(trimmed, "WHERE")?;
        let condition_start = where_pos + 5;
        let after_where = &trimmed[condition_start..];

        // Find where the condition ends.
        let condition_end = find_clause_boundary(after_where).unwrap_or(after_where.len());
        let condition = after_where[..condition_end].trim();

        if condition.is_empty() {
            return None;
        }

        let rest = &after_where[condition_end..];
        Some(format!(
            "{}WHERE NOT(NOT({condition})){rest}",
            &trimmed[..where_pos]
        ))
    }
}

// ---------------------------------------------------------------------------
// 4. CoalesceIdentity
// ---------------------------------------------------------------------------

/// Wrap result expressions in `COALESCE(expr, expr)`.
///
/// # Soundness
///
/// `COALESCE(x, x)` returns the first non-NULL argument. Since both
/// arguments are the same expression `x`:
/// - If `x` is non-NULL, returns `x`
/// - If `x` is NULL, returns `x` (NULL)
///
/// Therefore `COALESCE(x, x) = x` for all `x`.
#[derive(Debug)]
pub struct CoalesceIdentity;

impl MetamorphicTransform for CoalesceIdentity {
    fn name(&self) -> &str {
        "coalesce_identity"
    }

    fn soundness_sketch(&self) -> &str {
        "COALESCE(x, x) = x for all x. If x is non-NULL, returns first \
         non-NULL (x). If x is NULL, both args are NULL, returns NULL."
    }

    fn feature_tags(&self) -> Vec<&str> {
        vec!["F-FUNC.7"]
    }

    fn family(&self) -> TransformFamily {
        TransformFamily::Projection
    }

    fn equivalence_expectation(&self) -> EquivalenceExpectation {
        EquivalenceExpectation::ExactRowMatch
    }

    fn apply_one(&self, sql: &str, _seed: u64) -> Option<String> {
        let trimmed = sql.trim();
        let upper = trimmed.to_uppercase();
        if !upper.starts_with("SELECT") {
            return None;
        }

        // Find the column list between SELECT and FROM.
        let select_end = if upper.starts_with("SELECT DISTINCT ") {
            16
        } else if upper.starts_with("SELECT ALL ") {
            11
        } else {
            7
        };

        let from_pos = find_keyword_position(trimmed, "FROM")?;
        if from_pos <= select_end {
            return None;
        }

        let columns_str = trimmed[select_end..from_pos].trim();
        if columns_str == "*" || columns_str.is_empty() {
            return None;
        }

        // Split columns by comma (simple split, doesn't handle nested parens).
        let columns = split_top_level_commas(columns_str);
        if columns.is_empty() {
            return None;
        }

        let wrapped: Vec<String> = columns
            .iter()
            .map(|col| {
                let col = col.trim();
                // Don't wrap if it already has an alias (AS keyword).
                if col.to_uppercase().contains(" AS ") {
                    // Extract expression and alias.
                    if let Some(as_pos) = find_keyword_position(col, "AS") {
                        let expr = col[..as_pos].trim();
                        let alias = col[as_pos + 2..].trim();
                        format!("COALESCE({expr}, {expr}) AS {alias}")
                    } else {
                        format!("COALESCE({col}, {col})")
                    }
                } else {
                    format!("COALESCE({col}, {col})")
                }
            })
            .collect();

        Some(format!(
            "{}{} {}",
            &trimmed[..select_end],
            wrapped.join(", "),
            &trimmed[from_pos..]
        ))
    }
}

// ---------------------------------------------------------------------------
// 5. UnionSelfIntersect
// ---------------------------------------------------------------------------

/// `Q INTERSECT Q` is equivalent to `Q`.
///
/// # Soundness
///
/// For any query Q returning multiset R, `Q INTERSECT Q` returns the
/// set-intersection of R with itself, which is the set of distinct rows
/// in R. This is equivalent to `SELECT DISTINCT` over Q.
///
/// To preserve exact multiset semantics, we use `Q INTERSECT ALL Q` when
/// available, but SQLite does not support INTERSECT ALL. Therefore this
/// transform is only sound for queries whose results already have unique
/// rows (i.e., queries with DISTINCT or queries on tables with UNIQUE/PK
/// constraints).
///
/// We conservatively only apply this to SELECT DISTINCT queries.
#[derive(Debug)]
pub struct UnionSelfIntersect;

impl MetamorphicTransform for UnionSelfIntersect {
    fn name(&self) -> &str {
        "union_self_intersect"
    }

    fn soundness_sketch(&self) -> &str {
        "Q INTERSECT Q = SELECT DISTINCT of Q's results. Applied only to \
         SELECT DISTINCT queries where the result is already a set."
    }

    fn feature_tags(&self) -> Vec<&str> {
        vec!["F-SQL.8"]
    }

    fn family(&self) -> TransformFamily {
        TransformFamily::Structural
    }

    fn equivalence_expectation(&self) -> EquivalenceExpectation {
        EquivalenceExpectation::SetEquivalence
    }

    fn apply_one(&self, sql: &str, _seed: u64) -> Option<String> {
        let trimmed = sql.trim();
        let upper = trimmed.to_uppercase();

        // Only apply to SELECT DISTINCT (result is already a set).
        if !upper.starts_with("SELECT DISTINCT") {
            return None;
        }
        // Don't apply if already a compound query.
        if upper.contains(" UNION ") || upper.contains(" INTERSECT ") || upper.contains(" EXCEPT ")
        {
            return None;
        }

        Some(format!("{trimmed} INTERSECT {trimmed}"))
    }
}

// ---------------------------------------------------------------------------
// 6. CastLiteralIdentity
// ---------------------------------------------------------------------------

/// Replace integer literals with `CAST(n AS INTEGER)`.
///
/// # Soundness
///
/// `CAST(42 AS INTEGER) = 42` since 42 is already an integer. The CAST
/// is an identity operation when the value matches the target type.
#[derive(Debug)]
pub struct CastLiteralIdentity;

impl MetamorphicTransform for CastLiteralIdentity {
    fn name(&self) -> &str {
        "cast_literal_identity"
    }

    fn soundness_sketch(&self) -> &str {
        "CAST(n AS INTEGER) = n when n is an integer literal. The type \
         affinity is already INTEGER, so CAST is a no-op."
    }

    fn feature_tags(&self) -> Vec<&str> {
        vec!["F-SQL.45"]
    }

    fn family(&self) -> TransformFamily {
        TransformFamily::Literal
    }

    fn equivalence_expectation(&self) -> EquivalenceExpectation {
        EquivalenceExpectation::TypeCoercionEquivalent
    }

    fn apply_one(&self, sql: &str, seed: u64) -> Option<String> {
        let trimmed = sql.trim();
        let upper = trimmed.to_uppercase();
        if !upper.starts_with("SELECT") && !upper.starts_with("INSERT") {
            return None;
        }

        // Find standalone integer literals and wrap the first one in CAST.
        // Use seed to pick which literal to wrap (for variety).
        let mut literals: Vec<(usize, usize)> = Vec::new();
        let bytes = trimmed.as_bytes();
        let mut i = 0;
        while i < bytes.len() {
            if bytes[i].is_ascii_digit() {
                let start = i;
                while i < bytes.len() && bytes[i].is_ascii_digit() {
                    i += 1;
                }
                // Ensure it's a standalone literal (not part of an identifier).
                let before_ok = start == 0
                    || !bytes[start - 1].is_ascii_alphanumeric() && bytes[start - 1] != b'_';
                let after_ok =
                    i >= bytes.len() || !bytes[i].is_ascii_alphanumeric() && bytes[i] != b'_';
                if before_ok && after_ok && i - start <= 10 {
                    literals.push((start, i));
                }
            } else {
                i += 1;
            }
        }

        if literals.is_empty() {
            return None;
        }

        // Pick one literal using the seed.
        #[allow(clippy::cast_possible_truncation)]
        let pick = (seed as usize) % literals.len();
        let (start, end) = literals[pick];
        let literal = &trimmed[start..end];

        Some(format!(
            "{}CAST({literal} AS INTEGER){}",
            &trimmed[..start],
            &trimmed[end..]
        ))
    }
}

// ---------------------------------------------------------------------------
// 7. ExpressionCommute
// ---------------------------------------------------------------------------

/// Commute a simple binary comparison: `a = b` -> `b = a`.
///
/// # Soundness
///
/// The `=` operator is commutative: `a = b` iff `b = a`. Similarly,
/// `a != b` iff `b != a`. We only commute `=` and `!=` which are
/// symmetric relations.
#[derive(Debug)]
pub struct ExpressionCommute;

impl MetamorphicTransform for ExpressionCommute {
    fn name(&self) -> &str {
        "expression_commute"
    }

    fn soundness_sketch(&self) -> &str {
        "Equality (=) and inequality (!=, <>) are symmetric relations. \
         a = b iff b = a. a != b iff b != a."
    }

    fn feature_tags(&self) -> Vec<&str> {
        vec!["F-SQL.38"]
    }

    fn family(&self) -> TransformFamily {
        TransformFamily::Predicate
    }

    fn equivalence_expectation(&self) -> EquivalenceExpectation {
        EquivalenceExpectation::ExactRowMatch
    }

    fn apply_one(&self, sql: &str, seed: u64) -> Option<String> {
        let trimmed = sql.trim();
        let upper = trimmed.to_uppercase();
        if !upper.starts_with("SELECT")
            && !upper.starts_with("UPDATE")
            && !upper.starts_with("DELETE")
        {
            return None;
        }

        // Look for WHERE clause and find simple comparisons.
        let where_pos = find_keyword_position(trimmed, "WHERE")?;
        let after_where = &trimmed[where_pos + 5..];
        let condition_end = find_clause_boundary(after_where).unwrap_or(after_where.len());
        let condition = &after_where[..condition_end];

        // Find `a = b` or `a != b` or `a <> b` patterns.
        // Simple approach: find the first `=` that's not `!=`, `<=`, `>=`.
        let comparisons = find_simple_comparisons(condition);
        if comparisons.is_empty() {
            return None;
        }

        // Pick one comparison to commute.
        #[allow(clippy::cast_possible_truncation)]
        let pick = (seed as usize) % comparisons.len();
        let (left, op, right, start, end) = comparisons[pick].clone();

        let commuted_condition = format!(
            "{}{right} {op} {left}{}",
            &condition[..start],
            &condition[end..]
        );

        Some(format!(
            "{}WHERE{commuted_condition}{}",
            &trimmed[..where_pos],
            &after_where[condition_end..]
        ))
    }
}

// ---------------------------------------------------------------------------
// 8. NullCoalesce
// ---------------------------------------------------------------------------

/// Wrap expressions in `COALESCE(expr, NULL)`.
///
/// # Soundness
///
/// `COALESCE(x, NULL)` returns x if x is not NULL, else NULL. Since the
/// fallback is NULL (same as if x were NULL), this is equivalent to x.
///
/// More precisely: `COALESCE(x, NULL) = x` for all x, because:
/// - x non-NULL: first non-NULL arg is x -> returns x
/// - x NULL: both args are NULL -> returns NULL (= x)
#[derive(Debug)]
pub struct NullCoalesce;

impl MetamorphicTransform for NullCoalesce {
    fn name(&self) -> &str {
        "null_coalesce"
    }

    fn soundness_sketch(&self) -> &str {
        "COALESCE(x, NULL) = x. If x is non-NULL, returns x (first non-NULL). \
         If x is NULL, returns NULL (no non-NULL args found)."
    }

    fn feature_tags(&self) -> Vec<&str> {
        vec!["F-FUNC.7", "F-TYPE.3"]
    }

    fn family(&self) -> TransformFamily {
        TransformFamily::Projection
    }

    fn equivalence_expectation(&self) -> EquivalenceExpectation {
        EquivalenceExpectation::ExactRowMatch
    }

    fn apply_one(&self, sql: &str, _seed: u64) -> Option<String> {
        let trimmed = sql.trim();
        let upper = trimmed.to_uppercase();
        if !upper.starts_with("SELECT") {
            return None;
        }

        // Find the column list between SELECT and FROM.
        let select_end = if upper.starts_with("SELECT DISTINCT ") {
            16
        } else if upper.starts_with("SELECT ALL ") {
            11
        } else {
            7
        };

        let from_pos = find_keyword_position(trimmed, "FROM")?;
        if from_pos <= select_end {
            return None;
        }

        let columns_str = trimmed[select_end..from_pos].trim();
        if columns_str == "*" || columns_str.is_empty() {
            return None;
        }

        let columns = split_top_level_commas(columns_str);
        if columns.is_empty() {
            return None;
        }

        let wrapped: Vec<String> = columns
            .iter()
            .map(|col| {
                let col = col.trim();
                if col.to_uppercase().contains(" AS ") {
                    if let Some(as_pos) = find_keyword_position(col, "AS") {
                        let expr = col[..as_pos].trim();
                        let alias = col[as_pos + 2..].trim();
                        format!("COALESCE({expr}, NULL) AS {alias}")
                    } else {
                        format!("COALESCE({col}, NULL)")
                    }
                } else {
                    format!("COALESCE({col}, NULL)")
                }
            })
            .collect();

        Some(format!(
            "{}{} {}",
            &trimmed[..select_end],
            wrapped.join(", "),
            &trimmed[from_pos..]
        ))
    }
}

// ===========================================================================
// SQL string utilities
// ===========================================================================

/// Find the byte position of a keyword in SQL (case-insensitive, word boundary).
fn find_keyword_position(sql: &str, keyword: &str) -> Option<usize> {
    let upper = sql.to_uppercase();
    let kw_upper = keyword.to_uppercase();
    let kw_len = kw_upper.len();

    let mut search_from = 0;
    while let Some(pos) = upper[search_from..].find(&kw_upper) {
        let abs_pos = search_from + pos;
        let before_ok = abs_pos == 0 || !sql.as_bytes()[abs_pos - 1].is_ascii_alphanumeric();
        let after_pos = abs_pos + kw_len;
        let after_ok = after_pos >= sql.len() || !sql.as_bytes()[after_pos].is_ascii_alphanumeric();

        // Skip keywords inside string literals or parentheses.
        if before_ok && after_ok && !inside_string_literal(sql, abs_pos) {
            return Some(abs_pos);
        }
        search_from = abs_pos + 1;
    }
    None
}

/// Check if a position is inside a string literal (single quotes).
fn inside_string_literal(sql: &str, pos: usize) -> bool {
    let mut in_string = false;
    for (i, ch) in sql.char_indices() {
        if i >= pos {
            break;
        }
        if ch == '\'' {
            in_string = !in_string;
        }
    }
    in_string
}

/// Find the end of a WHERE condition (position of GROUP BY, ORDER BY,
/// LIMIT, HAVING, or end of string).
fn find_clause_boundary(after_where: &str) -> Option<usize> {
    find_first_clause(
        after_where,
        &["GROUP BY", "ORDER BY", "LIMIT", "HAVING", "WINDOW"],
    )
}

/// Find the position of the first matching keyword from a list.
fn find_first_clause(sql: &str, keywords: &[&str]) -> Option<usize> {
    let mut earliest: Option<usize> = None;
    for kw in keywords {
        if let Some(pos) = find_keyword_position(sql, kw) {
            earliest = Some(earliest.map_or(pos, |e: usize| e.min(pos)));
        }
    }
    earliest
}

/// Split a column list by commas, respecting parenthesis nesting.
fn split_top_level_commas(s: &str) -> Vec<String> {
    let mut result = Vec::new();
    let mut current = String::new();
    let mut depth = 0_i32;
    let mut in_string = false;

    for ch in s.chars() {
        if ch == '\'' && !in_string {
            in_string = true;
            current.push(ch);
        } else if ch == '\'' && in_string {
            in_string = false;
            current.push(ch);
        } else if in_string {
            current.push(ch);
        } else if ch == '(' {
            depth += 1;
            current.push(ch);
        } else if ch == ')' {
            depth -= 1;
            current.push(ch);
        } else if ch == ',' && depth == 0 {
            result.push(current.trim().to_owned());
            current.clear();
        } else {
            current.push(ch);
        }
    }

    let trimmed = current.trim().to_owned();
    if !trimmed.is_empty() {
        result.push(trimmed);
    }
    result
}

/// Find simple `lhs = rhs` or `lhs != rhs` comparisons in a condition string.
///
/// Returns vec of (left_expr, operator, right_expr, start_offset, end_offset).
fn find_simple_comparisons(condition: &str) -> Vec<(String, String, String, usize, usize)> {
    let mut results = Vec::new();
    let bytes = condition.as_bytes();
    let len = bytes.len();

    let mut i = 0;
    while i < len {
        // Skip string literals.
        if bytes[i] == b'\'' {
            i += 1;
            while i < len && bytes[i] != b'\'' {
                i += 1;
            }
            i += 1;
            continue;
        }

        // Look for `=` (not `!=`, `<=`, `>=`, `<>`)
        if bytes[i] == b'='
            && (i == 0 || (bytes[i - 1] != b'!' && bytes[i - 1] != b'<' && bytes[i - 1] != b'>'))
        {
            if i + 1 < len && bytes[i + 1] == b'=' {
                // `==` — skip
                i += 2;
                continue;
            }
            // Found `=`. Extract left and right operands.
            let left_end = i;
            let right_start = i + 1;

            // Scan left for the operand (simple: take the last token).
            let left = extract_operand_left(condition, left_end);
            let right = extract_operand_right(condition, right_start);

            if let (Some((l, l_start)), Some((r, r_end))) = (left, right) {
                results.push((l, "=".to_owned(), r, l_start, r_end));
            }
        }

        i += 1;
    }

    results
}

/// Extract the left operand of a comparison (simple token scanning).
fn extract_operand_left(s: &str, end: usize) -> Option<(String, usize)> {
    let before = s[..end].trim_end();
    if before.is_empty() {
        return None;
    }

    // Take the last "word" (identifier, number, or closing paren expression).
    let bytes = before.as_bytes();
    let mut i = bytes.len();
    if bytes[i - 1] == b')' {
        // Match the opening paren.
        let mut depth = 1;
        i -= 1;
        while i > 0 && depth > 0 {
            i -= 1;
            if bytes[i] == b')' {
                depth += 1;
            } else if bytes[i] == b'(' {
                depth -= 1;
            }
        }
    } else {
        while i > 0
            && (bytes[i - 1].is_ascii_alphanumeric()
                || bytes[i - 1] == b'_'
                || bytes[i - 1] == b'.')
        {
            i -= 1;
        }
    }

    let operand = before[i..].trim().to_owned();
    if operand.is_empty() {
        return None;
    }
    Some((operand, i))
}

/// Extract the right operand of a comparison (simple token scanning).
fn extract_operand_right(s: &str, start: usize) -> Option<(String, usize)> {
    let after = s[start..].trim_start();
    let offset = start + (s[start..].len() - after.len());

    if after.is_empty() {
        return None;
    }

    let bytes = after.as_bytes();
    let mut i = 0;
    if bytes[0] == b'(' {
        // Match the closing paren.
        let mut depth = 1;
        i = 1;
        while i < bytes.len() && depth > 0 {
            if bytes[i] == b'(' {
                depth += 1;
            } else if bytes[i] == b')' {
                depth -= 1;
            }
            i += 1;
        }
    } else if bytes[0] == b'\'' {
        // String literal.
        i = 1;
        while i < bytes.len() && bytes[i] != b'\'' {
            i += 1;
        }
        if i < bytes.len() {
            i += 1;
        }
    } else {
        while i < bytes.len()
            && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_' || bytes[i] == b'.')
        {
            i += 1;
        }
    }

    let operand = after[..i].trim().to_owned();
    if operand.is_empty() {
        return None;
    }
    Some((operand, offset + i))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // --- String utilities ---

    #[test]
    fn find_keyword_basic() {
        assert_eq!(find_keyword_position("SELECT x FROM t", "FROM"), Some(9));
    }

    #[test]
    fn find_keyword_case_insensitive() {
        assert_eq!(find_keyword_position("select x from t", "FROM"), Some(9));
    }

    #[test]
    fn find_keyword_not_in_string() {
        assert_eq!(
            find_keyword_position("SELECT 'FROM' FROM t", "FROM"),
            Some(14)
        );
    }

    #[test]
    fn find_keyword_absent() {
        assert_eq!(find_keyword_position("SELECT 1", "FROM"), None);
    }

    #[test]
    fn find_keyword_not_substring() {
        // "INFORMED" contains "FROM" but it's not a keyword.
        assert_eq!(find_keyword_position("SELECT INFORMED", "FROM"), None);
    }

    #[test]
    fn split_commas_simple() {
        let cols = split_top_level_commas("a, b, c");
        assert_eq!(cols, vec!["a", "b", "c"]);
    }

    #[test]
    fn split_commas_with_parens() {
        let cols = split_top_level_commas("a, COUNT(b, c), d");
        assert_eq!(cols, vec!["a", "COUNT(b, c)", "d"]);
    }

    #[test]
    fn split_commas_with_strings() {
        let cols = split_top_level_commas("a, 'hello, world', b");
        assert_eq!(cols, vec!["a", "'hello, world'", "b"]);
    }

    // --- SubqueryWrap ---

    #[test]
    fn subquery_wrap_basic() {
        let t = SubqueryWrap;
        let result = t.apply_one("SELECT a, b FROM t", 42);
        assert_eq!(
            result.as_deref(),
            Some("SELECT * FROM (SELECT a, b FROM t) AS _sub")
        );
    }

    #[test]
    fn subquery_wrap_skips_order_by() {
        let t = SubqueryWrap;
        assert!(t.apply_one("SELECT a FROM t ORDER BY a", 42).is_none());
    }

    #[test]
    fn subquery_wrap_skips_non_select() {
        let t = SubqueryWrap;
        assert!(t.apply_one("INSERT INTO t VALUES (1)", 42).is_none());
    }

    // --- TautologicalPredicate ---

    #[test]
    fn tautological_no_where() {
        let t = TautologicalPredicate;
        let result = t.apply_one("SELECT a FROM t", 42);
        assert_eq!(result.as_deref(), Some("SELECT a FROM t WHERE 1=1"));
    }

    #[test]
    fn tautological_with_where() {
        let t = TautologicalPredicate;
        let result = t.apply_one("SELECT a FROM t WHERE x > 5", 42);
        assert_eq!(
            result.as_deref(),
            Some("SELECT a FROM t WHERE x > 5 AND 1=1")
        );
    }

    #[test]
    fn tautological_with_group_by() {
        let t = TautologicalPredicate;
        let result = t.apply_one("SELECT a FROM t GROUP BY a", 42);
        assert_eq!(
            result.as_deref(),
            Some("SELECT a FROM t WHERE 1=1 GROUP BY a")
        );
    }

    // --- DoubleNegation ---

    #[test]
    fn double_negation_basic() {
        let t = DoubleNegation;
        let result = t.apply_one("SELECT a FROM t WHERE x > 5", 42);
        assert_eq!(
            result.as_deref(),
            Some("SELECT a FROM t WHERE NOT(NOT(x > 5))")
        );
    }

    #[test]
    fn double_negation_no_where() {
        let t = DoubleNegation;
        assert!(t.apply_one("SELECT a FROM t", 42).is_none());
    }

    // --- CoalesceIdentity ---

    #[test]
    fn coalesce_identity_basic() {
        let t = CoalesceIdentity;
        let result = t.apply_one("SELECT a, b FROM t", 42);
        assert_eq!(
            result.as_deref(),
            Some("SELECT COALESCE(a, a), COALESCE(b, b) FROM t")
        );
    }

    #[test]
    fn coalesce_identity_star() {
        let t = CoalesceIdentity;
        assert!(t.apply_one("SELECT * FROM t", 42).is_none());
    }

    #[test]
    fn coalesce_identity_with_alias() {
        let t = CoalesceIdentity;
        let result = t.apply_one("SELECT a AS x FROM t", 42);
        assert_eq!(result.as_deref(), Some("SELECT COALESCE(a, a) AS x FROM t"));
    }

    // --- UnionSelfIntersect ---

    #[test]
    fn union_self_intersect_distinct() {
        let t = UnionSelfIntersect;
        let result = t.apply_one("SELECT DISTINCT a FROM t", 42);
        assert_eq!(
            result.as_deref(),
            Some("SELECT DISTINCT a FROM t INTERSECT SELECT DISTINCT a FROM t")
        );
    }

    #[test]
    fn union_self_intersect_non_distinct() {
        let t = UnionSelfIntersect;
        assert!(t.apply_one("SELECT a FROM t", 42).is_none());
    }

    // --- CastLiteralIdentity ---

    #[test]
    fn cast_literal_basic() {
        let t = CastLiteralIdentity;
        let result = t.apply_one("SELECT 42 FROM t", 0);
        assert_eq!(result.as_deref(), Some("SELECT CAST(42 AS INTEGER) FROM t"));
    }

    #[test]
    fn cast_literal_no_literals() {
        let t = CastLiteralIdentity;
        assert!(t.apply_one("SELECT a FROM t", 0).is_none());
    }

    // --- ExpressionCommute ---

    #[test]
    fn expression_commute_basic() {
        let t = ExpressionCommute;
        let result = t.apply_one("SELECT a FROM t WHERE x = 5", 0);
        assert!(result.is_some());
        let r = result.unwrap();
        assert!(r.contains("5 = x") || r.contains("5 =x"), "got: {r}");
    }

    #[test]
    fn expression_commute_no_where() {
        let t = ExpressionCommute;
        assert!(t.apply_one("SELECT a FROM t", 0).is_none());
    }

    // --- NullCoalesce ---

    #[test]
    fn null_coalesce_basic() {
        let t = NullCoalesce;
        let result = t.apply_one("SELECT a, b FROM t", 42);
        assert_eq!(
            result.as_deref(),
            Some("SELECT COALESCE(a, NULL), COALESCE(b, NULL) FROM t")
        );
    }

    // --- Registry ---

    #[test]
    fn registry_has_all_transforms() {
        let reg = TransformRegistry::new();
        assert_eq!(reg.transforms().len(), 8);
    }

    #[test]
    fn registry_lookup_by_name() {
        let reg = TransformRegistry::new();
        assert!(reg.by_name("subquery_wrap").is_some());
        assert!(reg.by_name("nonexistent").is_none());
    }

    #[test]
    fn all_transforms_have_soundness_sketch() {
        let reg = TransformRegistry::new();
        for t in reg.transforms() {
            assert!(
                !t.soundness_sketch().is_empty(),
                "transform {} missing soundness sketch",
                t.name()
            );
        }
    }

    #[test]
    fn all_transforms_have_feature_tags() {
        let reg = TransformRegistry::new();
        for t in reg.transforms() {
            assert!(
                !t.feature_tags().is_empty(),
                "transform {} missing feature tags",
                t.name()
            );
        }
    }

    // --- Transform Families (bd-mblr.7.1.1) ---

    #[test]
    fn all_transform_families_represented() {
        let reg = TransformRegistry::new();
        let families: BTreeSet<TransformFamily> =
            reg.transforms().iter().map(|t| t.family()).collect();

        for family in TransformFamily::ALL {
            assert!(
                families.contains(&family),
                "no transform in family {family:?}"
            );
        }
    }

    #[test]
    fn predicate_family_contains_expected_transforms() {
        let reg = TransformRegistry::new();
        let predicate_names: Vec<&str> = reg
            .transforms()
            .iter()
            .filter(|t| t.family() == TransformFamily::Predicate)
            .map(|t| t.name())
            .collect();

        assert!(predicate_names.contains(&"tautological_predicate"));
        assert!(predicate_names.contains(&"double_negation"));
        assert!(predicate_names.contains(&"expression_commute"));
    }

    #[test]
    fn projection_family_contains_expected_transforms() {
        let reg = TransformRegistry::new();
        let proj_names: Vec<&str> = reg
            .transforms()
            .iter()
            .filter(|t| t.family() == TransformFamily::Projection)
            .map(|t| t.name())
            .collect();

        assert!(proj_names.contains(&"coalesce_identity"));
        assert!(proj_names.contains(&"null_coalesce"));
    }

    #[test]
    fn structural_family_contains_expected_transforms() {
        let reg = TransformRegistry::new();
        let struct_names: Vec<&str> = reg
            .transforms()
            .iter()
            .filter(|t| t.family() == TransformFamily::Structural)
            .map(|t| t.name())
            .collect();

        assert!(struct_names.contains(&"subquery_wrap"));
        assert!(struct_names.contains(&"union_self_intersect"));
    }

    #[test]
    fn literal_family_contains_expected_transforms() {
        let reg = TransformRegistry::new();
        let lit_names: Vec<&str> = reg
            .transforms()
            .iter()
            .filter(|t| t.family() == TransformFamily::Literal)
            .map(|t| t.name())
            .collect();

        assert!(lit_names.contains(&"cast_literal_identity"));
    }

    #[test]
    fn transform_family_display() {
        assert_eq!(TransformFamily::Predicate.to_string(), "predicate");
        assert_eq!(TransformFamily::Projection.to_string(), "projection");
        assert_eq!(TransformFamily::Structural.to_string(), "structural");
        assert_eq!(TransformFamily::Literal.to_string(), "literal");
    }

    // --- Equivalence Expectations (bd-mblr.7.1.1) ---

    #[test]
    fn all_transforms_have_equivalence_expectations() {
        let reg = TransformRegistry::new();
        for t in reg.transforms() {
            // Just verify it doesn't panic and returns a valid variant.
            let _ = t.equivalence_expectation();
        }
    }

    #[test]
    fn structural_transforms_allow_reordering() {
        let reg = TransformRegistry::new();
        let wrap = reg.by_name("subquery_wrap").unwrap();
        assert_eq!(
            wrap.equivalence_expectation(),
            EquivalenceExpectation::MultisetEquivalence
        );
    }

    #[test]
    fn intersect_uses_set_equivalence() {
        let reg = TransformRegistry::new();
        let intersect = reg.by_name("union_self_intersect").unwrap();
        assert_eq!(
            intersect.equivalence_expectation(),
            EquivalenceExpectation::SetEquivalence
        );
    }

    #[test]
    fn predicate_transforms_preserve_order() {
        let reg = TransformRegistry::new();
        for t in reg.transforms() {
            if t.family() == TransformFamily::Predicate {
                assert_eq!(
                    t.equivalence_expectation(),
                    EquivalenceExpectation::ExactRowMatch,
                    "predicate transform {} should preserve row order",
                    t.name()
                );
            }
        }
    }

    #[test]
    fn cast_literal_uses_type_coercion() {
        let reg = TransformRegistry::new();
        let cast = reg.by_name("cast_literal_identity").unwrap();
        assert_eq!(
            cast.equivalence_expectation(),
            EquivalenceExpectation::TypeCoercionEquivalent
        );
    }

    #[test]
    fn equivalence_display() {
        assert_eq!(
            EquivalenceExpectation::ExactRowMatch.to_string(),
            "exact_row_match"
        );
        assert_eq!(
            EquivalenceExpectation::MultisetEquivalence.to_string(),
            "multiset_equivalence"
        );
        assert_eq!(
            EquivalenceExpectation::SetEquivalence.to_string(),
            "set_equivalence"
        );
        assert_eq!(
            EquivalenceExpectation::TypeCoercionEquivalent.to_string(),
            "type_coercion_equivalent"
        );
    }

    // --- Mismatch Classification (bd-mblr.7.1.1) ---

    #[test]
    fn classify_identical_results() {
        let rows = vec![vec!["1".to_owned(), "alice".to_owned()]];
        let result = classify_mismatch(EquivalenceExpectation::ExactRowMatch, &rows, &rows);
        assert!(!result.is_actionable());
    }

    #[test]
    fn classify_order_dependent_difference() {
        let oracle = vec![vec!["1".to_owned()], vec!["2".to_owned()]];
        let subject = vec![vec!["2".to_owned()], vec!["1".to_owned()]];
        let result = classify_mismatch(EquivalenceExpectation::ExactRowMatch, &oracle, &subject);
        assert_eq!(result, MismatchClassification::OrderDependentDifference);
        assert!(!result.is_actionable());
    }

    #[test]
    fn classify_order_diff_under_multiset_is_false_positive() {
        let oracle = vec![vec!["1".to_owned()], vec!["2".to_owned()]];
        let subject = vec![vec!["2".to_owned()], vec!["1".to_owned()]];
        let result = classify_mismatch(
            EquivalenceExpectation::MultisetEquivalence,
            &oracle,
            &subject,
        );
        assert!(!result.is_actionable());
    }

    #[test]
    fn classify_true_divergence() {
        let oracle = vec![vec!["1".to_owned()]];
        let subject = vec![vec!["1".to_owned()], vec!["2".to_owned()]];
        let result = classify_mismatch(EquivalenceExpectation::ExactRowMatch, &oracle, &subject);
        assert!(result.is_actionable());
        assert_eq!(result.triage_priority(), 0);
    }

    #[test]
    fn classify_null_handling_difference() {
        let oracle = vec![vec!["NULL".to_owned()]];
        let subject = vec![vec!["0".to_owned()]];
        let result = classify_mismatch(EquivalenceExpectation::ExactRowMatch, &oracle, &subject);
        assert_eq!(result, MismatchClassification::NullHandlingDifference);
    }

    #[test]
    fn classify_type_affinity_difference() {
        let oracle = vec![vec!["42".to_owned()]];
        let subject = vec![vec!["42.0".to_owned()]];
        let result = classify_mismatch(
            EquivalenceExpectation::TypeCoercionEquivalent,
            &oracle,
            &subject,
        );
        assert_eq!(result, MismatchClassification::TypeAffinityDifference);
    }

    #[test]
    fn mismatch_triage_priority_ordering() {
        let true_div = MismatchClassification::TrueDivergence {
            description: "test".to_owned(),
        };
        let null_diff = MismatchClassification::NullHandlingDifference;
        let type_diff = MismatchClassification::TypeAffinityDifference;
        let fp_diff = MismatchClassification::FloatingPointDifference {
            max_epsilon_str: "0.001".to_owned(),
        };
        let order_diff = MismatchClassification::OrderDependentDifference;
        let false_pos = MismatchClassification::FalsePositive {
            reason: "test".to_owned(),
        };

        assert!(true_div.triage_priority() < null_diff.triage_priority());
        assert!(null_diff.triage_priority() < type_diff.triage_priority());
        assert!(type_diff.triage_priority() < fp_diff.triage_priority());
        assert!(fp_diff.triage_priority() < order_diff.triage_priority());
        assert!(order_diff.triage_priority() < false_pos.triage_priority());
    }

    #[test]
    fn mismatch_display_formats() {
        let d = MismatchClassification::TrueDivergence {
            description: "row count mismatch".to_owned(),
        };
        assert!(d.to_string().contains("true_divergence"));

        let fp = MismatchClassification::FalsePositive {
            reason: "timeout".to_owned(),
        };
        assert!(fp.to_string().contains("false_positive"));
    }

    // --- Seed Contract (bd-mblr.7.1.1) ---

    #[test]
    fn seed_contract_deterministic() {
        let c1 = SeedContract::new(0x4652_414E_4B45_4E, 0, 0);
        let c2 = SeedContract::new(0x4652_414E_4B45_4E, 0, 0);
        assert_eq!(c1.case_seed, c2.case_seed);
    }

    #[test]
    fn seed_contract_varies_with_entry_idx() {
        let c1 = SeedContract::new(42, 0, 0);
        let c2 = SeedContract::new(42, 1, 0);
        assert_ne!(c1.case_seed, c2.case_seed);
    }

    #[test]
    fn seed_contract_varies_with_transform_idx() {
        let c1 = SeedContract::new(42, 0, 0);
        let c2 = SeedContract::new(42, 0, 1);
        assert_ne!(c1.case_seed, c2.case_seed);
    }

    #[test]
    fn seed_contract_replay_command() {
        let c = SeedContract::new(42, 0, 0);
        let cmd = c.replay_command("mm-test-001");
        assert!(cmd.contains("metamorphic"));
        assert!(cmd.contains("mm-test-001"));
    }

    // --- Oracle Contract (bd-mblr.7.1.1) ---

    #[test]
    fn oracle_contract_roundtrip() {
        let contract = OracleContract {
            case_id: "mm-test-001".to_owned(),
            family: TransformFamily::Predicate,
            equivalence: EquivalenceExpectation::ExactRowMatch,
            seed: SeedContract::new(42, 0, 0),
        };
        let json = serde_json::to_string(&contract).unwrap();
        let deser: OracleContract = serde_json::from_str(&json).unwrap();
        assert_eq!(deser.case_id, contract.case_id);
        assert_eq!(deser.family, contract.family);
        assert_eq!(deser.equivalence, contract.equivalence);
        assert_eq!(deser.seed.case_seed, contract.seed.case_seed);
    }

    // --- Coverage with new fields (bd-mblr.7.1.1) ---

    #[test]
    fn coverage_includes_transform_families() {
        use crate::corpus_ingest::{CORPUS_SEED_BASE, CorpusBuilder, generate_seed_corpus};

        let mut builder = CorpusBuilder::new(CORPUS_SEED_BASE);
        generate_seed_corpus(&mut builder);
        let manifest = builder.build();

        let reg = TransformRegistry::new();
        let cases = generate_metamorphic_corpus(&manifest.entries, &reg, CORPUS_SEED_BASE, 8);
        let coverage = compute_coverage(&cases);

        assert!(
            !coverage.by_transform_family.is_empty(),
            "coverage should include transform family stats"
        );
        assert!(
            !coverage.by_equivalence.is_empty(),
            "coverage should include equivalence stats"
        );
    }
}
