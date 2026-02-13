//! Parity invariant catalog and proof-obligation mapping (bd-1dp9.8.1).
//!
//! Maps every parity claim from the [`parity_taxonomy`](crate::parity_taxonomy) feature
//! universe to explicit invariants, assumptions, and executable checks.  Each
//! release claim references test/proof IDs and data artifacts.
//!
//! # Architecture
//!
//! ```text
//!   FeatureUniverse (bd-1dp9.1.1)
//!        │
//!        ▼
//!   InvariantCatalog  ◄──  this module
//!        │
//!        ├── ParityInvariant (1:N per Feature)
//!        │       ├── invariant_id, statement, assumptions
//!        │       └── proof_obligations[]
//!        │              ├── ProofObligation { kind, evidence_ref, status }
//!        │              └── ArtifactRef { path, hash, schema_version }
//!        │
//!        ├── validate()            → Vec<Violation>
//!        ├── release_traceability()→ ReleaseTraceabilityReport
//!        └── stats()               → CatalogStats
//! ```
//!
//! # Downstream Consumers
//!
//! - **bd-1dp9.8.2**: Anytime-valid parity drift monitors (e-process + BOCPD)
//! - **bd-1dp9.8.3**: Conformal confidence gates + Bayesian prioritiser
//! - **bd-1dp9.8.4**: Release certificate generator with evidence ledger
//! - **bd-1dp9.8.5**: Adversarial counterexample search
//!
//! # Determinism
//!
//! Invariant IDs are assigned deterministically via category prefix and
//! monotonic sequence.  The catalog is sorted by invariant ID for stable
//! iteration order.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use serde::{Deserialize, Serialize};

use crate::parity_taxonomy::{FeatureCategory, FeatureId};

/// Bead identifier for log correlation.
#[allow(dead_code)]
const BEAD_ID: &str = "bd-1dp9.8.1";

/// Schema version for migration compatibility.
pub const CATALOG_SCHEMA_VERSION: u32 = 1;

// ---------------------------------------------------------------------------
// Invariant identification
// ---------------------------------------------------------------------------

/// Unique invariant identifier.
///
/// Format: `PAR-{CATEGORY}-{SEQ}` where `CATEGORY` matches the taxonomy
/// prefix and `SEQ` is a zero-padded three-digit sequence number.
///
/// Examples: `PAR-SQL-001`, `PAR-MVCC-003`, `PAR-EXT-002`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct InvariantId(pub String);

impl InvariantId {
    /// Create a new invariant ID from category prefix and sequence number.
    #[must_use]
    pub fn new(category_prefix: &str, seq: u16) -> Self {
        Self(format!("PAR-{category_prefix}-{seq:03}"))
    }
}

impl fmt::Display for InvariantId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

// ---------------------------------------------------------------------------
// Proof obligation types
// ---------------------------------------------------------------------------

/// The kind of evidence that satisfies a proof obligation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ProofKind {
    /// Deterministic unit test with concrete inputs and assertions.
    UnitTest,
    /// End-to-end scenario test exercising the full stack.
    E2eTest,
    /// Property-based / fuzzing test (proptest or similar).
    PropertyTest,
    /// Anytime-valid e-process statistical monitor.
    EProcessMonitor,
    /// Differential oracle comparing against reference SQLite.
    DifferentialOracle,
    /// Manual code review with documented rationale.
    CodeReview,
    /// Formal model check (e.g., TLA+ or Rust const-assertion).
    FormalModel,
}

impl fmt::Display for ProofKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnitTest => f.write_str("unit_test"),
            Self::E2eTest => f.write_str("e2e_test"),
            Self::PropertyTest => f.write_str("property_test"),
            Self::EProcessMonitor => f.write_str("e_process_monitor"),
            Self::DifferentialOracle => f.write_str("differential_oracle"),
            Self::CodeReview => f.write_str("code_review"),
            Self::FormalModel => f.write_str("formal_model"),
        }
    }
}

/// Current verification status of a proof obligation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ObligationStatus {
    /// Obligation is fully satisfied with passing evidence.
    Verified,
    /// Obligation has been started but evidence is incomplete.
    Partial,
    /// Obligation has not yet been addressed.
    Pending,
    /// Obligation was explicitly waived with documented rationale.
    Waived,
}

impl ObligationStatus {
    /// Whether this status counts as satisfied for release gating.
    #[must_use]
    pub const fn is_satisfied(self) -> bool {
        matches!(self, Self::Verified | Self::Waived)
    }
}

impl fmt::Display for ObligationStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Verified => f.write_str("verified"),
            Self::Partial => f.write_str("partial"),
            Self::Pending => f.write_str("pending"),
            Self::Waived => f.write_str("waived"),
        }
    }
}

// ---------------------------------------------------------------------------
// Artifact reference
// ---------------------------------------------------------------------------

/// Reference to a data artifact produced by an executable check.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactRef {
    /// Logical path or identifier (e.g., test function name, log file pattern).
    pub path: String,
    /// Optional artifact hash for integrity verification.
    pub hash: Option<String>,
    /// Schema version of the artifact format.
    pub schema_version: Option<String>,
}

// ---------------------------------------------------------------------------
// Proof obligation
// ---------------------------------------------------------------------------

/// A single proof obligation binding an invariant to executable evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProofObligation {
    /// What kind of proof this obligation requires.
    pub kind: ProofKind,
    /// Current verification status.
    pub status: ObligationStatus,
    /// Crate containing the test/check.
    pub crate_name: String,
    /// Module or test function path (e.g., `fsqlite_core::tests::select_basic`).
    pub test_path: String,
    /// Human-readable description of what is being checked.
    pub description: String,
    /// References to data artifacts produced by this check.
    pub artifacts: Vec<ArtifactRef>,
    /// Waiver rationale (required when status is `Waived`).
    pub waiver_rationale: Option<String>,
    /// Related bead IDs for traceability.
    pub related_beads: Vec<String>,
}

// ---------------------------------------------------------------------------
// Parity invariant
// ---------------------------------------------------------------------------

/// A single parity invariant: a formal claim about behavioural equivalence
/// between FrankenSQLite and SQLite 3.52.0 for a specific feature.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParityInvariant {
    /// Unique invariant identifier.
    pub id: InvariantId,
    /// Feature ID from the taxonomy that this invariant covers.
    pub feature_id: FeatureId,
    /// Feature category (denormalised for efficient grouping).
    pub category: FeatureCategory,
    /// Formal invariant statement.
    ///
    /// Should be phrased as a testable claim, e.g.:
    /// "For all valid SELECT ... WHERE queries, FrankenSQLite returns
    /// identical result sets to SQLite 3.52.0 under the same schema."
    pub statement: String,
    /// Assumptions under which the invariant holds.
    ///
    /// E.g., "Database uses WAL journal mode", "No concurrent writers".
    pub assumptions: Vec<String>,
    /// Proof obligations linking this invariant to executable evidence.
    pub obligations: Vec<ProofObligation>,
    /// Tags for cross-cutting queries.
    pub tags: BTreeSet<String>,
    /// Spec section references.
    pub spec_refs: Vec<String>,
}

impl ParityInvariant {
    /// Whether all proof obligations are satisfied.
    #[must_use]
    pub fn is_fully_verified(&self) -> bool {
        !self.obligations.is_empty() && self.obligations.iter().all(|o| o.status.is_satisfied())
    }

    /// Count of obligations by status.
    #[must_use]
    pub fn obligation_summary(&self) -> ObligationSummary {
        let mut summary = ObligationSummary::default();
        for o in &self.obligations {
            match o.status {
                ObligationStatus::Verified => summary.verified += 1,
                ObligationStatus::Partial => summary.partial += 1,
                ObligationStatus::Pending => summary.pending += 1,
                ObligationStatus::Waived => summary.waived += 1,
            }
        }
        summary.total = self.obligations.len();
        summary
    }
}

/// Summary counts of proof obligation statuses.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObligationSummary {
    pub total: usize,
    pub verified: usize,
    pub partial: usize,
    pub pending: usize,
    pub waived: usize,
}

// ---------------------------------------------------------------------------
// Invariant catalog
// ---------------------------------------------------------------------------

/// The complete parity invariant catalog.
///
/// Invariants are stored in a `BTreeMap` keyed by [`InvariantId`] for
/// deterministic iteration order.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvariantCatalog {
    /// Schema version for forward-compatible migrations.
    pub schema_version: u32,
    /// All invariants in the catalog.
    pub invariants: BTreeMap<InvariantId, ParityInvariant>,
}

/// A single catalog validation violation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Violation {
    /// Invariant ID (or empty for catalog-level violations).
    pub invariant_id: Option<InvariantId>,
    /// Violation rule name.
    pub rule: String,
    /// Human-readable message.
    pub message: String,
}

impl fmt::Display for Violation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(id) = &self.invariant_id {
            write!(f, "[{}] {}: {}", id, self.rule, self.message)
        } else {
            write!(f, "[catalog] {}: {}", self.rule, self.message)
        }
    }
}

/// Catalog-level statistics.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogStats {
    /// Total number of invariants.
    pub total_invariants: usize,
    /// Number of fully verified invariants.
    pub verified_invariants: usize,
    /// Number of partially verified invariants.
    pub partial_invariants: usize,
    /// Number of pending invariants.
    pub pending_invariants: usize,
    /// Total number of proof obligations across all invariants.
    pub total_obligations: usize,
    /// Number of satisfied obligations (verified + waived).
    pub satisfied_obligations: usize,
    /// Number of distinct features covered.
    pub features_covered: usize,
    /// Number of distinct categories covered.
    pub categories_covered: usize,
    /// Per-category invariant counts.
    pub per_category: BTreeMap<String, usize>,
}

/// Release traceability report: maps every invariant to its evidence chain.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReleaseTraceabilityReport {
    /// Schema version.
    pub schema_version: u32,
    /// Per-invariant traceability entries.
    pub entries: Vec<TraceabilityEntry>,
    /// Overall verification percentage.
    pub verification_pct: f64,
    /// Whether all invariants are satisfied for release.
    pub release_ready: bool,
}

/// A single entry in the release traceability report.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceabilityEntry {
    /// Invariant ID.
    pub invariant_id: InvariantId,
    /// Feature ID.
    pub feature_id: FeatureId,
    /// Category display name.
    pub category: String,
    /// Invariant statement.
    pub statement: String,
    /// Whether fully verified.
    pub verified: bool,
    /// Proof kind → status mapping.
    pub proof_summary: Vec<ProofSummaryEntry>,
    /// Artifact references.
    pub artifact_refs: Vec<String>,
}

/// Summary of a single proof obligation in a traceability entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProofSummaryEntry {
    pub kind: ProofKind,
    pub status: ObligationStatus,
    pub test_path: String,
}

impl InvariantCatalog {
    /// Validate catalog structural invariants.
    ///
    /// # Rules
    ///
    /// - `CAT-VAL-1`: All invariant IDs must be unique (guaranteed by `BTreeMap`).
    /// - `CAT-VAL-2`: Every invariant must have at least one proof obligation.
    /// - `CAT-VAL-3`: Waived obligations must have a waiver rationale.
    /// - `CAT-VAL-4`: Every category must have at least one invariant.
    /// - `CAT-VAL-5`: Invariant statements must be non-empty.
    /// - `CAT-VAL-6`: Feature IDs must be non-empty.
    /// - `CAT-VAL-7`: Test paths in obligations must be non-empty.
    #[must_use]
    pub fn validate(&self) -> Vec<Violation> {
        let mut violations = Vec::new();

        // CAT-VAL-2: Every invariant must have at least one proof obligation.
        for inv in self.invariants.values() {
            if inv.obligations.is_empty() {
                violations.push(Violation {
                    invariant_id: Some(inv.id.clone()),
                    rule: "CAT-VAL-2".to_owned(),
                    message: "invariant has no proof obligations".to_owned(),
                });
            }
        }

        // CAT-VAL-3: Waived obligations must have waiver rationale.
        for inv in self.invariants.values() {
            for (i, obl) in inv.obligations.iter().enumerate() {
                if obl.status == ObligationStatus::Waived && obl.waiver_rationale.is_none() {
                    violations.push(Violation {
                        invariant_id: Some(inv.id.clone()),
                        rule: "CAT-VAL-3".to_owned(),
                        message: format!("obligation[{i}] is waived but has no rationale"),
                    });
                }
            }
        }

        // CAT-VAL-4: Every category must have at least one invariant.
        for cat in FeatureCategory::ALL {
            let count = self
                .invariants
                .values()
                .filter(|inv| inv.category == cat)
                .count();
            if count == 0 {
                violations.push(Violation {
                    invariant_id: None,
                    rule: "CAT-VAL-4".to_owned(),
                    message: format!("category {cat} has no invariants"),
                });
            }
        }

        // CAT-VAL-5: Invariant statements must be non-empty.
        for inv in self.invariants.values() {
            if inv.statement.trim().is_empty() {
                violations.push(Violation {
                    invariant_id: Some(inv.id.clone()),
                    rule: "CAT-VAL-5".to_owned(),
                    message: "invariant statement is empty".to_owned(),
                });
            }
        }

        // CAT-VAL-6: Feature IDs must be non-empty.
        for inv in self.invariants.values() {
            if inv.feature_id.0.trim().is_empty() {
                violations.push(Violation {
                    invariant_id: Some(inv.id.clone()),
                    rule: "CAT-VAL-6".to_owned(),
                    message: "feature_id is empty".to_owned(),
                });
            }
        }

        // CAT-VAL-7: Test paths in obligations must be non-empty.
        for inv in self.invariants.values() {
            for (i, obl) in inv.obligations.iter().enumerate() {
                if obl.test_path.trim().is_empty() {
                    violations.push(Violation {
                        invariant_id: Some(inv.id.clone()),
                        rule: "CAT-VAL-7".to_owned(),
                        message: format!("obligation[{i}] has empty test_path"),
                    });
                }
            }
        }

        violations
    }

    /// Compute catalog-level statistics.
    #[must_use]
    pub fn stats(&self) -> CatalogStats {
        let mut stats = CatalogStats::default();
        let mut feature_ids = BTreeSet::new();
        let mut categories = BTreeSet::new();

        for inv in self.invariants.values() {
            stats.total_invariants += 1;

            if inv.is_fully_verified() {
                stats.verified_invariants += 1;
            } else if inv
                .obligations
                .iter()
                .any(|o| o.status == ObligationStatus::Verified)
            {
                stats.partial_invariants += 1;
            } else {
                stats.pending_invariants += 1;
            }

            stats.total_obligations += inv.obligations.len();
            stats.satisfied_obligations += inv
                .obligations
                .iter()
                .filter(|o| o.status.is_satisfied())
                .count();

            feature_ids.insert(&inv.feature_id);
            categories.insert(inv.category);

            *stats
                .per_category
                .entry(inv.category.display_name().to_owned())
                .or_insert(0) += 1;
        }

        stats.features_covered = feature_ids.len();
        stats.categories_covered = categories.len();
        stats
    }

    /// Generate a release traceability report.
    #[must_use]
    pub fn release_traceability(&self) -> ReleaseTraceabilityReport {
        let mut entries = Vec::new();

        for inv in self.invariants.values() {
            let proof_summary: Vec<ProofSummaryEntry> = inv
                .obligations
                .iter()
                .map(|o| ProofSummaryEntry {
                    kind: o.kind,
                    status: o.status,
                    test_path: o.test_path.clone(),
                })
                .collect();

            let artifact_refs: Vec<String> = inv
                .obligations
                .iter()
                .flat_map(|o| o.artifacts.iter().map(|a| a.path.clone()))
                .collect();

            entries.push(TraceabilityEntry {
                invariant_id: inv.id.clone(),
                feature_id: inv.feature_id.clone(),
                category: inv.category.display_name().to_owned(),
                statement: inv.statement.clone(),
                verified: inv.is_fully_verified(),
                proof_summary,
                artifact_refs,
            });
        }

        let total = entries.len();
        let verified = entries.iter().filter(|e| e.verified).count();
        #[allow(clippy::cast_precision_loss)]
        let verification_pct = if total > 0 {
            (verified as f64 / total as f64) * 100.0
        } else {
            0.0
        };
        let release_ready = total > 0 && verified == total;

        ReleaseTraceabilityReport {
            schema_version: CATALOG_SCHEMA_VERSION,
            entries,
            verification_pct,
            release_ready,
        }
    }

    /// Return invariants filtered by category.
    #[must_use]
    pub fn by_category(&self, cat: FeatureCategory) -> Vec<&ParityInvariant> {
        self.invariants
            .values()
            .filter(|inv| inv.category == cat)
            .collect()
    }

    /// Return invariants filtered by feature ID.
    #[must_use]
    pub fn by_feature(&self, feature_id: &FeatureId) -> Vec<&ParityInvariant> {
        self.invariants
            .values()
            .filter(|inv| &inv.feature_id == feature_id)
            .collect()
    }

    /// Return invariants matching any of the given tags.
    #[must_use]
    pub fn by_tag(&self, tag: &str) -> Vec<&ParityInvariant> {
        self.invariants
            .values()
            .filter(|inv| inv.tags.contains(tag))
            .collect()
    }

    /// Return invariants that are not yet fully verified.
    #[must_use]
    pub fn unverified(&self) -> Vec<&ParityInvariant> {
        self.invariants
            .values()
            .filter(|inv| !inv.is_fully_verified())
            .collect()
    }

    /// Serialise to deterministic JSON.
    ///
    /// # Errors
    ///
    /// Returns `Err` if serialisation fails.
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }

    /// Deserialise from JSON.
    ///
    /// # Errors
    ///
    /// Returns `Err` if the JSON is malformed.
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }
}

// ---------------------------------------------------------------------------
// Canonical catalog builder
// ---------------------------------------------------------------------------

/// Helper for building invariants within a category.
struct InvariantBuilder {
    category: FeatureCategory,
    prefix: &'static str,
    seq: u16,
    invariants: Vec<ParityInvariant>,
}

impl InvariantBuilder {
    fn new(category: FeatureCategory) -> Self {
        Self {
            category,
            prefix: category.prefix(),
            seq: 0,
            invariants: Vec::new(),
        }
    }

    fn add(
        &mut self,
        feature_id: &str,
        statement: &str,
        assumptions: &[&str],
        obligations: Vec<ProofObligation>,
        tags: &[&str],
        spec_refs: &[&str],
    ) -> &mut Self {
        self.seq += 1;
        self.invariants.push(ParityInvariant {
            id: InvariantId::new(self.prefix, self.seq),
            feature_id: FeatureId(feature_id.to_owned()),
            category: self.category,
            statement: statement.to_owned(),
            assumptions: assumptions.iter().map(|&s| s.to_owned()).collect(),
            obligations,
            tags: tags.iter().map(|&s| s.to_owned()).collect(),
            spec_refs: spec_refs.iter().map(|&s| s.to_owned()).collect(),
        });
        self
    }

    fn build(self) -> Vec<ParityInvariant> {
        self.invariants
    }
}

/// Create a verified unit-test proof obligation.
fn unit_obligation(
    crate_name: &str,
    test_path: &str,
    description: &str,
    beads: &[&str],
) -> ProofObligation {
    ProofObligation {
        kind: ProofKind::UnitTest,
        status: ObligationStatus::Verified,
        crate_name: crate_name.to_owned(),
        test_path: test_path.to_owned(),
        description: description.to_owned(),
        artifacts: Vec::new(),
        waiver_rationale: None,
        related_beads: beads.iter().map(|&s| s.to_owned()).collect(),
    }
}

/// Create a verified differential-oracle proof obligation.
fn differential_obligation(
    crate_name: &str,
    test_path: &str,
    description: &str,
    beads: &[&str],
) -> ProofObligation {
    ProofObligation {
        kind: ProofKind::DifferentialOracle,
        status: ObligationStatus::Verified,
        crate_name: crate_name.to_owned(),
        test_path: test_path.to_owned(),
        description: description.to_owned(),
        artifacts: Vec::new(),
        waiver_rationale: None,
        related_beads: beads.iter().map(|&s| s.to_owned()).collect(),
    }
}

/// Create a verified E2E proof obligation.
fn e2e_obligation(
    crate_name: &str,
    test_path: &str,
    description: &str,
    beads: &[&str],
) -> ProofObligation {
    ProofObligation {
        kind: ProofKind::E2eTest,
        status: ObligationStatus::Verified,
        crate_name: crate_name.to_owned(),
        test_path: test_path.to_owned(),
        description: description.to_owned(),
        artifacts: Vec::new(),
        waiver_rationale: None,
        related_beads: beads.iter().map(|&s| s.to_owned()).collect(),
    }
}

/// Create a verified e-process monitor obligation.
fn eprocess_obligation(
    crate_name: &str,
    test_path: &str,
    description: &str,
    beads: &[&str],
) -> ProofObligation {
    ProofObligation {
        kind: ProofKind::EProcessMonitor,
        status: ObligationStatus::Verified,
        crate_name: crate_name.to_owned(),
        test_path: test_path.to_owned(),
        description: description.to_owned(),
        artifacts: Vec::new(),
        waiver_rationale: None,
        related_beads: beads.iter().map(|&s| s.to_owned()).collect(),
    }
}

/// Create a verified property-test obligation.
fn property_obligation(
    crate_name: &str,
    test_path: &str,
    description: &str,
    beads: &[&str],
) -> ProofObligation {
    ProofObligation {
        kind: ProofKind::PropertyTest,
        status: ObligationStatus::Verified,
        crate_name: crate_name.to_owned(),
        test_path: test_path.to_owned(),
        description: description.to_owned(),
        artifacts: Vec::new(),
        waiver_rationale: None,
        related_beads: beads.iter().map(|&s| s.to_owned()).collect(),
    }
}

/// Create a pending obligation placeholder.
fn pending_obligation(
    kind: ProofKind,
    crate_name: &str,
    test_path: &str,
    description: &str,
    beads: &[&str],
) -> ProofObligation {
    ProofObligation {
        kind,
        status: ObligationStatus::Pending,
        crate_name: crate_name.to_owned(),
        test_path: test_path.to_owned(),
        description: description.to_owned(),
        artifacts: Vec::new(),
        waiver_rationale: None,
        related_beads: beads.iter().map(|&s| s.to_owned()).collect(),
    }
}

/// Build the canonical parity invariant catalog.
///
/// This is the single source of truth for all parity proof obligations.
/// Every feature in the taxonomy has at least one invariant with at
/// least one proof obligation.
#[must_use]
#[allow(clippy::too_many_lines)]
pub fn build_canonical_catalog() -> InvariantCatalog {
    let mut invariants = BTreeMap::new();

    let all_invariants: Vec<ParityInvariant> = [
        build_sql_grammar_invariants(),
        build_vdbe_invariants(),
        build_storage_transaction_invariants(),
        build_pragma_invariants(),
        build_builtin_function_invariants(),
        build_extension_invariants(),
        build_type_system_invariants(),
        build_file_format_invariants(),
        build_api_cli_invariants(),
    ]
    .into_iter()
    .flatten()
    .collect();

    for inv in all_invariants {
        invariants.insert(inv.id.clone(), inv);
    }

    InvariantCatalog {
        schema_version: CATALOG_SCHEMA_VERSION,
        invariants,
    }
}

// ---------------------------------------------------------------------------
// Per-category invariant builders
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_lines)]
fn build_sql_grammar_invariants() -> Vec<ParityInvariant> {
    let mut b = InvariantBuilder::new(FeatureCategory::SqlGrammar);

    b.add(
        "F-SQL-001",
        "For all valid SELECT queries with column lists, aliases, and DISTINCT, FrankenSQLite returns identical result sets to SQLite 3.52.0 under the same schema and data.",
        &["WAL journal mode", "UTF-8 encoding"],
        vec![
            unit_obligation("fsqlite-core", "fsqlite_core::query::test_select_basic", "Basic SELECT column list verification", &["bd-1ik"]),
            differential_obligation("fsqlite-e2e", "fsqlite_e2e::correctness::select_basic", "Differential comparison of SELECT results", &["bd-1dp9.1.2"]),
        ],
        &["dml", "select"],
        &["spec:§10.1"],
    );
    b.add(
        "F-SQL-002",
        "WHERE clause evaluation with comparison, logical, and IS NULL operators produces identical row filtering to SQLite 3.52.0.",
        &["WAL journal mode"],
        vec![
            unit_obligation("fsqlite-core", "fsqlite_core::query::test_select_where", "WHERE clause operator semantics", &["bd-1ik"]),
            unit_obligation("fsqlite-core", "fsqlite_core::query::test_query_with_params_table_where_two_anonymous_placeholders_bind_in_order", "Parameterised WHERE binding order", &["bd-1ik"]),
        ],
        &["dml", "select", "where"],
        &["spec:§10.1"],
    );
    b.add(
        "F-SQL-003",
        "JOIN operations (INNER, LEFT, CROSS, NATURAL) with ON/USING produce identical result sets to SQLite 3.52.0.",
        &["WAL journal mode"],
        vec![
            unit_obligation("fsqlite-core", "fsqlite_core::query::test_select_join", "JOIN result set correctness", &["bd-1ik"]),
        ],
        &["dml", "select", "join"],
        &["spec:§10.1"],
    );
    b.add(
        "F-SQL-004",
        "GROUP BY with aggregate functions and HAVING produces identical grouped results to SQLite 3.52.0.",
        &["WAL journal mode"],
        vec![
            unit_obligation("fsqlite-core", "fsqlite_core::query::test_select_group_by", "GROUP BY aggregate correctness", &["bd-1ik"]),
        ],
        &["dml", "select", "aggregate"],
        &["spec:§10.1"],
    );
    b.add(
        "F-SQL-005",
        "ORDER BY with ASC/DESC and collation produces identical row ordering to SQLite 3.52.0.",
        &["WAL journal mode"],
        vec![unit_obligation(
            "fsqlite-core",
            "fsqlite_core::query::test_select_order_by",
            "ORDER BY directional sorting",
            &["bd-1ik"],
        )],
        &["dml", "select"],
        &["spec:§10.1"],
    );
    b.add(
        "F-SQL-006",
        "LIMIT and OFFSET produce identical result windowing to SQLite 3.52.0.",
        &["WAL journal mode"],
        vec![unit_obligation(
            "fsqlite-core",
            "fsqlite_core::query::test_select_limit_offset",
            "LIMIT/OFFSET windowing",
            &["bd-1ik"],
        )],
        &["dml", "select"],
        &["spec:§10.1"],
    );
    b.add(
        "F-SQL-007",
        "Subqueries (scalar, EXISTS, IN, FROM) produce identical results to SQLite 3.52.0.",
        &["WAL journal mode"],
        vec![unit_obligation(
            "fsqlite-core",
            "fsqlite_core::query::test_select_subquery",
            "Subquery evaluation correctness",
            &["bd-1ik"],
        )],
        &["dml", "select", "subquery"],
        &["spec:§10.1"],
    );
    b.add(
        "F-SQL-008",
        "Compound queries (UNION, UNION ALL, INTERSECT, EXCEPT) produce identical result sets to SQLite 3.52.0.",
        &["WAL journal mode"],
        vec![
            unit_obligation("fsqlite-core", "fsqlite_core::query::test_select_compound", "Compound SELECT set operations", &["bd-1ik"]),
        ],
        &["dml", "select", "compound"],
        &["spec:§10.1"],
    );
    b.add(
        "F-SQL-009",
        "Common Table Expressions (recursive and non-recursive) produce identical results to SQLite 3.52.0.",
        &["WAL journal mode"],
        vec![
            unit_obligation("fsqlite-core", "fsqlite_core::query::test_select_cte", "CTE evaluation correctness", &["bd-1ik"]),
        ],
        &["dml", "select", "cte"],
        &["spec:§10.1"],
    );
    b.add(
        "F-SQL-010",
        "Window functions (ROW_NUMBER, RANK, LAG, LEAD, etc.) with OVER clauses produce identical results to SQLite 3.52.0.",
        &["WAL journal mode"],
        vec![
            unit_obligation("fsqlite-core", "fsqlite_core::query::test_select_window", "Window function evaluation", &["bd-1ik"]),
        ],
        &["dml", "select", "window"],
        &["spec:§10.1"],
    );
    b.add(
        "F-SQL-011",
        "INSERT INTO ... VALUES and INSERT INTO ... SELECT produce identical database state to SQLite 3.52.0.",
        &["WAL journal mode"],
        vec![
            unit_obligation("fsqlite-core", "fsqlite_core::mutation::test_insert_basic", "INSERT row insertion correctness", &["bd-1ik"]),
            unit_obligation("fsqlite-vdbe", "fsqlite_vdbe::codegen::test_codegen_insert_values", "INSERT VDBE codegen", &["bd-1ik"]),
        ],
        &["dml", "insert"],
        &["spec:§10.2"],
    );
    b.add(
        "F-SQL-012",
        "INSERT OR REPLACE/IGNORE/ABORT/ROLLBACK/FAIL conflict resolution matches SQLite 3.52.0 semantics.",
        &["WAL journal mode"],
        vec![
            unit_obligation("fsqlite-core", "fsqlite_core::mutation::test_insert_conflict", "INSERT conflict resolution", &["bd-1ik"]),
        ],
        &["dml", "insert", "conflict"],
        &["spec:§10.2"],
    );
    b.add(
        "F-SQL-013",
        "INSERT RETURNING clause returns identical values to SQLite 3.52.0.",
        &["WAL journal mode"],
        vec![pending_obligation(
            ProofKind::UnitTest,
            "fsqlite-core",
            "fsqlite_core::mutation::test_insert_returning",
            "INSERT RETURNING clause",
            &["bd-1dp9.3"],
        )],
        &["dml", "insert", "returning"],
        &["spec:§10.2"],
    );
    b.add(
        "F-SQL-014",
        "UPDATE ... SET ... WHERE produces identical row modifications to SQLite 3.52.0.",
        &["WAL journal mode"],
        vec![unit_obligation(
            "fsqlite-core",
            "fsqlite_core::mutation::test_update_basic",
            "UPDATE row modification",
            &["bd-1ik"],
        )],
        &["dml", "update"],
        &["spec:§10.2"],
    );
    b.add(
        "F-SQL-015",
        "UPDATE ... FROM syntax produces identical results to SQLite 3.52.0 (3.33+).",
        &["WAL journal mode"],
        vec![pending_obligation(
            ProofKind::UnitTest,
            "fsqlite-core",
            "fsqlite_core::mutation::test_update_from",
            "UPDATE FROM clause",
            &["bd-1dp9.3"],
        )],
        &["dml", "update"],
        &["spec:§10.2"],
    );
    b.add(
        "F-SQL-016",
        "UPDATE OR conflict resolution matches SQLite 3.52.0 semantics.",
        &["WAL journal mode"],
        vec![unit_obligation(
            "fsqlite-core",
            "fsqlite_core::mutation::test_update_conflict",
            "UPDATE conflict resolution",
            &["bd-1ik"],
        )],
        &["dml", "update", "conflict"],
        &["spec:§10.2"],
    );
    b.add(
        "F-SQL-017",
        "DELETE FROM ... WHERE produces identical row deletions to SQLite 3.52.0.",
        &["WAL journal mode"],
        vec![unit_obligation(
            "fsqlite-core",
            "fsqlite_core::mutation::test_delete_basic",
            "DELETE row removal",
            &["bd-1ik"],
        )],
        &["dml", "delete"],
        &["spec:§10.2"],
    );
    b.add(
        "F-SQL-018",
        "DELETE with ORDER BY and LIMIT produces identical results to SQLite 3.52.0.",
        &["WAL journal mode"],
        vec![pending_obligation(
            ProofKind::UnitTest,
            "fsqlite-core",
            "fsqlite_core::mutation::test_delete_limit",
            "DELETE with LIMIT",
            &["bd-1dp9.3"],
        )],
        &["dml", "delete"],
        &["spec:§10.2"],
    );
    b.add(
        "F-SQL-019",
        "REPLACE INTO produces identical semantics to DELETE + INSERT on conflict.",
        &["WAL journal mode"],
        vec![unit_obligation(
            "fsqlite-core",
            "fsqlite_core::mutation::test_replace",
            "REPLACE semantics",
            &["bd-1ik"],
        )],
        &["dml", "replace"],
        &["spec:§10.2"],
    );
    b.add(
        "F-SQL-020",
        "UPSERT (INSERT ... ON CONFLICT DO UPDATE/NOTHING) matches SQLite 3.52.0.",
        &["WAL journal mode"],
        vec![pending_obligation(
            ProofKind::UnitTest,
            "fsqlite-core",
            "fsqlite_core::mutation::test_upsert",
            "UPSERT semantics",
            &["bd-1dp9.3"],
        )],
        &["dml", "upsert"],
        &["spec:§10.2"],
    );
    b.add(
        "F-SQL-021",
        "CREATE TABLE with columns, constraints, WITHOUT ROWID, and STRICT produces identical schema to SQLite 3.52.0.",
        &["WAL journal mode"],
        vec![
            unit_obligation("fsqlite-core", "fsqlite_core::schema::test_create_table", "CREATE TABLE DDL", &["bd-1ik"]),
        ],
        &["ddl", "table"],
        &["spec:§10.3"],
    );
    b.add(
        "F-SQL-022",
        "CREATE TABLE AS SELECT produces identical schema and data to SQLite 3.52.0.",
        &["WAL journal mode"],
        vec![pending_obligation(
            ProofKind::UnitTest,
            "fsqlite-core",
            "fsqlite_core::schema::test_ctas",
            "CREATE TABLE AS SELECT",
            &["bd-1dp9.3"],
        )],
        &["ddl", "table"],
        &["spec:§10.3"],
    );
    b.add(
        "F-SQL-023",
        "CREATE INDEX / CREATE UNIQUE INDEX produces identical index structure to SQLite 3.52.0.",
        &["WAL journal mode"],
        vec![
            unit_obligation(
                "fsqlite-core",
                "fsqlite_core::schema::test_create_index",
                "CREATE INDEX DDL",
                &["bd-1ik"],
            ),
            unit_obligation(
                "fsqlite-vdbe",
                "fsqlite_vdbe::codegen::test_codegen_select_with_index",
                "Index-based query execution",
                &["bd-1ik"],
            ),
        ],
        &["ddl", "index"],
        &["spec:§10.3"],
    );
    b.add(
        "F-SQL-024",
        "CREATE VIEW produces identical view definitions to SQLite 3.52.0.",
        &["WAL journal mode"],
        vec![unit_obligation(
            "fsqlite-core",
            "fsqlite_core::schema::test_create_view",
            "CREATE VIEW DDL",
            &["bd-1ik"],
        )],
        &["ddl", "view"],
        &["spec:§10.3"],
    );
    b.add(
        "F-SQL-025",
        "CREATE TRIGGER with BEFORE/AFTER/INSTEAD OF semantics matches SQLite 3.52.0.",
        &["WAL journal mode"],
        vec![unit_obligation(
            "fsqlite-core",
            "fsqlite_core::schema::test_create_trigger",
            "CREATE TRIGGER DDL",
            &["bd-1ik"],
        )],
        &["ddl", "trigger"],
        &["spec:§10.3"],
    );
    b.add(
        "F-SQL-026",
        "CREATE VIRTUAL TABLE using registered modules matches SQLite 3.52.0.",
        &["WAL journal mode", "Required extensions loaded"],
        vec![unit_obligation(
            "fsqlite-core",
            "fsqlite_core::schema::test_create_vtab",
            "CREATE VIRTUAL TABLE DDL",
            &["bd-3c7"],
        )],
        &["ddl", "vtab"],
        &["spec:§14"],
    );
    b.add(
        "F-SQL-027",
        "DROP TABLE/INDEX/VIEW/TRIGGER IF EXISTS matches SQLite 3.52.0 behaviour.",
        &["WAL journal mode"],
        vec![unit_obligation(
            "fsqlite-core",
            "fsqlite_core::schema::test_drop",
            "DROP IF EXISTS DDL",
            &["bd-1ik"],
        )],
        &["ddl", "drop"],
        &["spec:§10.3"],
    );
    b.add(
        "F-SQL-028",
        "ALTER TABLE (ADD COLUMN, RENAME COLUMN, RENAME TABLE, DROP COLUMN) matches SQLite 3.52.0.",
        &["WAL journal mode"],
        vec![unit_obligation(
            "fsqlite-core",
            "fsqlite_core::schema::test_alter_table",
            "ALTER TABLE DDL",
            &["bd-1ik"],
        )],
        &["ddl", "alter"],
        &["spec:§10.3"],
    );
    b.add(
        "F-SQL-029",
        "BEGIN/COMMIT/ROLLBACK with DEFERRED/IMMEDIATE/EXCLUSIVE matches SQLite 3.52.0 lock semantics.",
        &[],
        vec![
            unit_obligation("fsqlite-core", "fsqlite_core::txn::test_begin_commit_rollback", "Transaction control flow", &["bd-1ik"]),
            e2e_obligation("fsqlite-e2e", "fsqlite_e2e::correctness_transactions", "E2E transaction lifecycle", &["bd-1dp9.4"]),
        ],
        &["txn"],
        &["spec:§4.1"],
    );
    b.add(
        "F-SQL-030",
        "SAVEPOINT/RELEASE/ROLLBACK TO nested savepoint semantics match SQLite 3.52.0.",
        &[],
        vec![unit_obligation(
            "fsqlite-core",
            "fsqlite_core::txn::test_savepoint",
            "Savepoint nesting",
            &["bd-1ik"],
        )],
        &["txn", "savepoint"],
        &["spec:§4.1"],
    );
    b.add(
        "F-SQL-031",
        "BEGIN CONCURRENT MVCC mode provides serialisable concurrent writers without data loss.",
        &["MVCC enabled"],
        vec![
            unit_obligation(
                "fsqlite-core",
                "fsqlite_core::txn::test_begin_concurrent",
                "Concurrent writer mode",
                &["bd-1ik"],
            ),
            eprocess_obligation(
                "fsqlite-harness",
                "fsqlite_harness::eprocess::monitor_inv1_monotonicity",
                "MVCC INV-1 monitoring",
                &["bd-3go.3"],
            ),
        ],
        &["txn", "concurrent", "mvcc"],
        &["spec:§4.2"],
    );
    b.add(
        "F-SQL-032",
        "ATTACH/DETACH produces identical multi-database behaviour to SQLite 3.52.0.",
        &["WAL journal mode"],
        vec![unit_obligation(
            "fsqlite-core",
            "fsqlite_core::database::test_attach_detach",
            "ATTACH/DETACH lifecycle",
            &["bd-1ik"],
        )],
        &["database"],
        &["spec:§10.3"],
    );
    b.add(
        "F-SQL-033",
        "VACUUM compaction and defragmentation produces identical database state to SQLite 3.52.0.",
        &["WAL journal mode"],
        vec![unit_obligation(
            "fsqlite-core",
            "fsqlite_core::database::test_vacuum",
            "VACUUM compaction",
            &["bd-1ik"],
        )],
        &["database", "vacuum"],
        &["spec:§10.3"],
    );
    b.add(
        "F-SQL-034",
        "REINDEX rebuilds indexes identically to SQLite 3.52.0.",
        &["WAL journal mode"],
        vec![unit_obligation(
            "fsqlite-core",
            "fsqlite_core::database::test_reindex",
            "REINDEX rebuild",
            &["bd-1ik"],
        )],
        &["database"],
        &["spec:§10.3"],
    );
    b.add(
        "F-SQL-035",
        "ANALYZE statistics collection matches SQLite 3.52.0 for query planner decisions.",
        &["WAL journal mode"],
        vec![unit_obligation(
            "fsqlite-core",
            "fsqlite_core::database::test_analyze",
            "ANALYZE statistics",
            &["bd-1ik"],
        )],
        &["database", "planner"],
        &["spec:§10.3"],
    );
    b.add(
        "F-SQL-036",
        "Arithmetic operators (+, -, *, /, %) with type coercion match SQLite 3.52.0.",
        &[],
        vec![unit_obligation(
            "fsqlite-core",
            "fsqlite_core::expr::test_arithmetic",
            "Arithmetic operator evaluation",
            &["bd-1ik"],
        )],
        &["expr", "arithmetic"],
        &["spec:§10.1"],
    );
    b.add(
        "F-SQL-037",
        "Comparison operators (=, !=, <, >, <=, >=, IS, IS NOT) match SQLite 3.52.0.",
        &[],
        vec![unit_obligation(
            "fsqlite-core",
            "fsqlite_core::expr::test_comparison",
            "Comparison operator evaluation",
            &["bd-1ik"],
        )],
        &["expr", "comparison"],
        &["spec:§10.1"],
    );
    b.add(
        "F-SQL-038",
        "Logical operators (AND, OR, NOT) with three-valued logic match SQLite 3.52.0.",
        &[],
        vec![unit_obligation(
            "fsqlite-core",
            "fsqlite_core::expr::test_logical",
            "Logical operator three-valued logic",
            &["bd-1ik"],
        )],
        &["expr", "logic"],
        &["spec:§10.1"],
    );
    b.add(
        "F-SQL-039",
        "String operators (||, LIKE, GLOB, COLLATE) match SQLite 3.52.0.",
        &[],
        vec![unit_obligation(
            "fsqlite-core",
            "fsqlite_core::expr::test_string_ops",
            "String operator evaluation",
            &["bd-1ik"],
        )],
        &["expr", "string"],
        &["spec:§10.1"],
    );
    b.add(
        "F-SQL-040",
        "Bitwise operators (&, |, ~, <<, >>) match SQLite 3.52.0.",
        &[],
        vec![unit_obligation(
            "fsqlite-core",
            "fsqlite_core::expr::test_bitwise",
            "Bitwise operator evaluation",
            &["bd-1ik"],
        )],
        &["expr", "bitwise"],
        &["spec:§10.1"],
    );
    b.add(
        "F-SQL-041",
        "BETWEEN, IN (list/subquery), and CASE WHEN expressions match SQLite 3.52.0.",
        &[],
        vec![unit_obligation(
            "fsqlite-core",
            "fsqlite_core::expr::test_between_in_case",
            "BETWEEN/IN/CASE evaluation",
            &["bd-1ik"],
        )],
        &["expr"],
        &["spec:§10.1"],
    );
    b.add(
        "F-SQL-042",
        "CAST expressions produce identical type conversions to SQLite 3.52.0.",
        &[],
        vec![unit_obligation(
            "fsqlite-core",
            "fsqlite_core::expr::test_cast",
            "CAST type conversion",
            &["bd-1ik"],
        )],
        &["expr", "cast"],
        &["spec:§10.1"],
    );
    b.add(
        "F-SQL-043",
        "EXISTS/NOT EXISTS subqueries produce identical boolean results to SQLite 3.52.0.",
        &[],
        vec![unit_obligation(
            "fsqlite-core",
            "fsqlite_core::expr::test_exists",
            "EXISTS subquery evaluation",
            &["bd-1ik"],
        )],
        &["expr", "subquery"],
        &["spec:§10.1"],
    );
    b.add(
        "F-SQL-044",
        "Aggregate expressions (COUNT, SUM, AVG, MIN, MAX, GROUP_CONCAT, TOTAL) match SQLite 3.52.0.",
        &[],
        vec![
            unit_obligation("fsqlite-core", "fsqlite_core::expr::test_aggregate", "Aggregate function evaluation", &["bd-9y1"]),
        ],
        &["expr", "aggregate"],
        &["spec:§13"],
    );
    b.add(
        "F-SQL-045",
        "Window function expressions (ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD) match SQLite 3.52.0.",
        &[],
        vec![
            unit_obligation("fsqlite-core", "fsqlite_core::expr::test_window_expr", "Window expression evaluation", &["bd-9y1"]),
        ],
        &["expr", "window"],
        &["spec:§13"],
    );
    b.add(
        "F-SQL-046",
        "EXPLAIN and EXPLAIN QUERY PLAN output is structurally compatible with SQLite 3.52.0.",
        &["WAL journal mode"],
        vec![unit_obligation(
            "fsqlite-core",
            "fsqlite_core::explain::test_explain",
            "EXPLAIN output structure",
            &["bd-1ik"],
        )],
        &["meta"],
        &["spec:§10.4"],
    );
    b.add(
        "F-SQL-047",
        "PRAGMA name / PRAGMA name = value syntax is parsed and executed identically to SQLite 3.52.0.",
        &[],
        vec![
            unit_obligation("fsqlite-core", "fsqlite_core::pragma::test_pragma_syntax", "PRAGMA syntax parsing", &["bd-1ik"]),
        ],
        &["meta", "pragma"],
        &["spec:§12"],
    );

    b.build()
}

#[allow(clippy::too_many_lines)]
fn build_vdbe_invariants() -> Vec<ParityInvariant> {
    let mut b = InvariantBuilder::new(FeatureCategory::VdbeOpcodes);

    b.add(
        "F-VDBE-001",
        "Control flow opcodes (Goto, Gosub, Return, Halt, etc.) produce identical program counter transitions to SQLite 3.52.0.",
        &["Valid VDBE program"],
        vec![
            unit_obligation("fsqlite-vdbe", "fsqlite_vdbe::exec::test_control_flow", "Control flow opcode execution", &["bd-1ik"]),
        ],
        &["vdbe", "control"],
        &["spec:§11.1"],
    );
    b.add(
        "F-VDBE-002",
        "Constant and value loading opcodes produce identical register contents to SQLite 3.52.0.",
        &["Valid VDBE program"],
        vec![unit_obligation(
            "fsqlite-vdbe",
            "fsqlite_vdbe::exec::test_constants",
            "Constant loading opcodes",
            &["bd-1ik"],
        )],
        &["vdbe", "constants"],
        &["spec:§11.1"],
    );
    b.add(
        "F-VDBE-003",
        "Register manipulation opcodes (Move, Copy, SCopy) produce identical register states to SQLite 3.52.0.",
        &["Valid VDBE program"],
        vec![
            unit_obligation("fsqlite-vdbe", "fsqlite_vdbe::exec::test_register_ops", "Register manipulation opcodes", &["bd-1ik"]),
        ],
        &["vdbe", "register"],
        &["spec:§11.1"],
    );
    b.add(
        "F-VDBE-004",
        "VDBE arithmetic opcodes produce identical results with proper type coercion to SQLite 3.52.0.",
        &["Valid VDBE program"],
        vec![
            unit_obligation("fsqlite-vdbe", "fsqlite_vdbe::exec::test_arithmetic", "VDBE arithmetic evaluation", &["bd-1ik"]),
        ],
        &["vdbe", "arithmetic"],
        &["spec:§11.1"],
    );
    b.add(
        "F-VDBE-005",
        "VDBE comparison opcodes produce identical branching decisions to SQLite 3.52.0.",
        &["Valid VDBE program"],
        vec![unit_obligation(
            "fsqlite-vdbe",
            "fsqlite_vdbe::exec::test_comparison",
            "VDBE comparison evaluation",
            &["bd-1ik"],
        )],
        &["vdbe", "comparison"],
        &["spec:§11.1"],
    );
    b.add(
        "F-VDBE-006",
        "VDBE branching opcodes (If, IfNot, IsNull, IsType, etc.) produce identical control flow to SQLite 3.52.0.",
        &["Valid VDBE program"],
        vec![
            unit_obligation("fsqlite-vdbe", "fsqlite_vdbe::exec::test_branching", "VDBE branching logic", &["bd-1ik"]),
        ],
        &["vdbe", "branch"],
        &["spec:§11.1"],
    );
    b.add(
        "F-VDBE-007",
        "Type conversion opcodes produce identical type affinity results to SQLite 3.52.0.",
        &["Valid VDBE program"],
        vec![unit_obligation(
            "fsqlite-vdbe",
            "fsqlite_vdbe::exec::test_type_conversion",
            "Type conversion opcodes",
            &["bd-1ik"],
        )],
        &["vdbe", "type"],
        &["spec:§11.1"],
    );
    b.add(
        "F-VDBE-008",
        "Column and record opcodes produce identical record encoding/decoding to SQLite 3.52.0.",
        &["Valid VDBE program"],
        vec![unit_obligation(
            "fsqlite-vdbe",
            "fsqlite_vdbe::exec::test_column_record",
            "Column/record opcodes",
            &["bd-1ik"],
        )],
        &["vdbe", "record"],
        &["spec:§11.1"],
    );
    b.add(
        "F-VDBE-009",
        "Transaction opcodes produce identical transaction state transitions to SQLite 3.52.0.",
        &["Valid VDBE program"],
        vec![unit_obligation(
            "fsqlite-vdbe",
            "fsqlite_vdbe::exec::test_transaction_ops",
            "Transaction opcode execution",
            &["bd-1ik"],
        )],
        &["vdbe", "txn"],
        &["spec:§11.1"],
    );
    b.add(
        "F-VDBE-010",
        "Cursor lifecycle opcodes produce identical cursor states and B-tree interactions to SQLite 3.52.0.",
        &["Valid VDBE program", "Valid B-tree"],
        vec![
            unit_obligation("fsqlite-vdbe", "fsqlite_vdbe::exec::test_cursor_lifecycle", "Cursor open/close opcodes", &["bd-1ik"]),
        ],
        &["vdbe", "cursor"],
        &["spec:§11.2"],
    );
    b.add(
        "F-VDBE-011",
        "Seek opcodes produce identical cursor positioning to SQLite 3.52.0.",
        &["Valid VDBE program", "Valid B-tree"],
        vec![unit_obligation(
            "fsqlite-vdbe",
            "fsqlite_vdbe::exec::test_seek_ops",
            "Seek opcode cursor positioning",
            &["bd-1ik"],
        )],
        &["vdbe", "seek"],
        &["spec:§11.2"],
    );
    b.add(
        "F-VDBE-012",
        "Index lookup opcodes (Found, NotFound, NoConflict) produce identical results to SQLite 3.52.0.",
        &["Valid VDBE program", "Valid index"],
        vec![
            unit_obligation("fsqlite-vdbe", "fsqlite_vdbe::exec::test_index_lookup", "Index lookup opcode correctness", &["bd-1ik"]),
        ],
        &["vdbe", "index"],
        &["spec:§11.2"],
    );
    // Covering remaining VDBE opcode groups with combined invariants
    b.add(
        "F-VDBE-013",
        "Insert, Delete, NewRowid, and write opcodes produce identical B-tree mutations to SQLite 3.52.0.",
        &["Valid VDBE program", "Valid B-tree"],
        vec![
            unit_obligation("fsqlite-vdbe", "fsqlite_vdbe::exec::test_write_ops", "Write opcode B-tree mutation", &["bd-1ik"]),
        ],
        &["vdbe", "write"],
        &["spec:§11.2"],
    );
    b.add(
        "F-VDBE-014",
        "Navigation opcodes (Rewind, Next, Prev, Last, Sort, Sorter*) produce identical traversal sequences to SQLite 3.52.0.",
        &["Valid VDBE program", "Valid B-tree"],
        vec![
            unit_obligation("fsqlite-vdbe", "fsqlite_vdbe::exec::test_navigation", "Navigation opcode traversal", &["bd-1ik"]),
        ],
        &["vdbe", "navigate"],
        &["spec:§11.2"],
    );
    b.add(
        "F-VDBE-015",
        "Index DML opcodes produce identical index modifications to SQLite 3.52.0.",
        &["Valid VDBE program", "Valid index"],
        vec![unit_obligation(
            "fsqlite-vdbe",
            "fsqlite_vdbe::exec::test_index_dml",
            "Index DML opcode correctness",
            &["bd-1ik"],
        )],
        &["vdbe", "index"],
        &["spec:§11.2"],
    );
    b.add(
        "F-VDBE-016",
        "Row data extraction opcodes produce identical data retrieval to SQLite 3.52.0.",
        &["Valid VDBE program"],
        vec![unit_obligation(
            "fsqlite-vdbe",
            "fsqlite_vdbe::exec::test_row_data",
            "Row data extraction opcodes",
            &["bd-1ik"],
        )],
        &["vdbe", "data"],
        &["spec:§11.1"],
    );
    b.add(
        "F-VDBE-017",
        "DDL opcodes (Destroy, Clear, CreateBtree, etc.) produce identical schema changes to SQLite 3.52.0.",
        &["Valid VDBE program"],
        vec![
            unit_obligation("fsqlite-vdbe", "fsqlite_vdbe::exec::test_ddl_ops", "DDL opcode execution", &["bd-1ik"]),
        ],
        &["vdbe", "ddl"],
        &["spec:§11.2"],
    );
    b.add(
        "F-VDBE-018",
        "Aggregate opcodes produce identical accumulator states to SQLite 3.52.0.",
        &["Valid VDBE program"],
        vec![unit_obligation(
            "fsqlite-vdbe",
            "fsqlite_vdbe::exec::test_aggregate_ops",
            "Aggregate opcode accumulation",
            &["bd-9y1"],
        )],
        &["vdbe", "aggregate"],
        &["spec:§11.1"],
    );
    b.add(
        "F-VDBE-019",
        "Function call opcodes (PureFunc, Function) produce identical invocations to SQLite 3.52.0.",
        &["Valid VDBE program"],
        vec![
            unit_obligation("fsqlite-vdbe", "fsqlite_vdbe::exec::test_function_ops", "Function call opcodes", &["bd-9y1"]),
        ],
        &["vdbe", "function"],
        &["spec:§11.1"],
    );
    b.add(
        "F-VDBE-020",
        "IntegrityCk opcode produces identical integrity check results to SQLite 3.52.0.",
        &["Valid VDBE program"],
        vec![unit_obligation(
            "fsqlite-vdbe",
            "fsqlite_vdbe::exec::test_integrity_check",
            "IntegrityCk opcode",
            &["bd-1ik"],
        )],
        &["vdbe", "integrity"],
        &["spec:§11.2"],
    );
    // Remaining VDBE groups
    b.add(
        "F-VDBE-021",
        "RowSet, Trigger/Program, WAL/Journal, Virtual Table, Memory/Counter, Locking/Expire, Bloom filter, Subtype, Page count, Trace/Init, Collation, and Bitwise opcodes all produce identical behaviour to SQLite 3.52.0.",
        &["Valid VDBE program"],
        vec![
            unit_obligation("fsqlite-vdbe", "fsqlite_vdbe::exec::test_misc_opcodes", "Miscellaneous opcode coverage", &["bd-1ik"]),
        ],
        &["vdbe"],
        &["spec:§11"],
    );

    b.build()
}

#[allow(clippy::too_many_lines)]
fn build_storage_transaction_invariants() -> Vec<ParityInvariant> {
    let mut b = InvariantBuilder::new(FeatureCategory::StorageTransaction);

    // Pager
    b.add(
        "F-STOR-001",
        "ARC page cache eviction produces identical cache behaviour to SQLite 3.52.0 under equivalent workloads.",
        &["Same cache_size PRAGMA"],
        vec![
            unit_obligation("fsqlite-pager", "fsqlite_pager::cache::test_arc_eviction", "ARC eviction policy", &["bd-7pu"]),
        ],
        &["pager", "cache"],
        &["spec:§6"],
    );
    b.add(
        "F-STOR-002",
        "Dirty page write-back ordering preserves crash-safety invariants and matches SQLite 3.52.0 commit semantics.",
        &["WAL journal mode"],
        vec![
            unit_obligation("fsqlite-pager", "fsqlite_pager::writeback::test_dirty_writeback", "Dirty page ordering", &["bd-7pu"]),
        ],
        &["pager", "writeback"],
        &["spec:§6"],
    );
    b.add(
        "F-STOR-003",
        "Rollback journal provides crash recovery semantics matching SQLite 3.52.0.",
        &["DELETE journal mode"],
        vec![unit_obligation(
            "fsqlite-pager",
            "fsqlite_pager::journal::test_rollback_journal",
            "Rollback journal mechanics",
            &["bd-7pu"],
        )],
        &["pager", "journal"],
        &["spec:§6"],
    );
    // WAL
    b.add(
        "F-STOR-004",
        "WAL mode provides concurrent read-during-write semantics matching SQLite 3.52.0.",
        &["WAL journal mode"],
        vec![
            unit_obligation(
                "fsqlite-wal",
                "fsqlite_wal::wal::test_wal_concurrent_read",
                "WAL concurrent reads",
                &["bd-3t3"],
            ),
            e2e_obligation(
                "fsqlite-e2e",
                "fsqlite_e2e::correctness_transactions::test_concurrent_reads",
                "E2E WAL concurrency",
                &["bd-1dp9.4"],
            ),
        ],
        &["wal"],
        &["spec:§5"],
    );
    b.add(
        "F-STOR-005",
        "WAL checkpoint modes (PASSIVE, FULL, RESTART, TRUNCATE) match SQLite 3.52.0 behaviour.",
        &["WAL journal mode"],
        vec![unit_obligation(
            "fsqlite-wal",
            "fsqlite_wal::checkpoint::test_checkpoint_modes",
            "Checkpoint mode semantics",
            &["bd-3t3"],
        )],
        &["wal", "checkpoint"],
        &["spec:§5.3"],
    );
    b.add(
        "F-STOR-006",
        "WAL crash recovery by replaying frames produces identical database state to SQLite 3.52.0.",
        &["WAL journal mode"],
        vec![
            unit_obligation("fsqlite-wal", "fsqlite_wal::recovery::test_wal_recovery", "WAL frame replay", &["bd-3t3"]),
        ],
        &["wal", "recovery"],
        &["spec:§5.4"],
    );
    b.add(
        "F-STOR-007",
        "WAL index (SHM) provides correct multi-process read concurrency.",
        &["WAL journal mode"],
        vec![unit_obligation(
            "fsqlite-wal",
            "fsqlite_wal::shm::test_wal_index",
            "WAL SHM index operations",
            &["bd-3t3"],
        )],
        &["wal", "shm"],
        &["spec:§5.2"],
    );
    b.add(
        "F-STOR-008",
        "RaptorQ-based FEC repair symbols heal torn writes with provable coverage.",
        &["WAL journal mode", "FEC enabled"],
        vec![
            unit_obligation(
                "fsqlite-harness",
                "fsqlite_harness::gf256_verification::test_e2e_raptorq_roundtrip_uses_gf256_tables",
                "RaptorQ encode/decode roundtrip",
                &["bd-1hi.1"],
            ),
            property_obligation(
                "fsqlite-harness",
                "fsqlite_harness::gf256_verification::prop_gf256_mul_associative",
                "GF(256) algebraic properties",
                &["bd-1hi.1"],
            ),
        ],
        &["wal", "fec", "raptorq"],
        &["spec:§3.5.12"],
    );
    // B-tree
    b.add(
        "F-STOR-009",
        "B+tree table page format matches SQLite 3.52.0 cell layout and navigation.",
        &[],
        vec![unit_obligation(
            "fsqlite-btree",
            "fsqlite_btree::page::test_table_page_format",
            "Table page cell layout",
            &["bd-7pu"],
        )],
        &["btree", "table"],
        &["spec:§7"],
    );
    b.add(
        "F-STOR-010",
        "B-tree index page format matches SQLite 3.52.0 key ordering.",
        &[],
        vec![unit_obligation(
            "fsqlite-btree",
            "fsqlite_btree::page::test_index_page_format",
            "Index page key layout",
            &["bd-7pu"],
        )],
        &["btree", "index"],
        &["spec:§7"],
    );
    b.add(
        "F-STOR-011",
        "B-tree page splitting preserves ordering and balance properties.",
        &[],
        vec![unit_obligation(
            "fsqlite-btree",
            "fsqlite_btree::split::test_page_split",
            "Page split correctness",
            &["bd-7pu"],
        )],
        &["btree", "split"],
        &["spec:§7"],
    );
    b.add(
        "F-STOR-012",
        "B-tree page merging preserves ordering and minimum-fill invariants.",
        &[],
        vec![unit_obligation(
            "fsqlite-btree",
            "fsqlite_btree::merge::test_page_merge",
            "Page merge correctness",
            &["bd-7pu"],
        )],
        &["btree", "merge"],
        &["spec:§7"],
    );
    b.add(
        "F-STOR-013",
        "Overflow page chains correctly spill and reconstruct large records.",
        &[],
        vec![unit_obligation(
            "fsqlite-btree",
            "fsqlite_btree::overflow::test_overflow",
            "Overflow chain handling",
            &["bd-7pu"],
        )],
        &["btree", "overflow"],
        &["spec:§7"],
    );
    b.add(
        "F-STOR-014",
        "Freelist management correctly recycles pages via trunk/leaf chains.",
        &[],
        vec![unit_obligation(
            "fsqlite-btree",
            "fsqlite_btree::freelist::test_freelist",
            "Freelist page recycling",
            &["bd-7pu"],
        )],
        &["btree", "freelist"],
        &["spec:§7"],
    );
    b.add(
        "F-STOR-015",
        "Cursor operations (move_to, next, prev, insert, delete) produce identical traversal to SQLite 3.52.0.",
        &["Valid B-tree"],
        vec![
            unit_obligation("fsqlite-btree", "fsqlite_btree::cursor::test_cursor_navigation", "Cursor navigation correctness", &["bd-7pu"]),
        ],
        &["btree", "cursor"],
        &["spec:§7"],
    );
    // Transaction semantics
    b.add(
        "F-STOR-016",
        "DEFERRED transaction lock acquisition matches SQLite 3.52.0.",
        &[],
        vec![unit_obligation(
            "fsqlite-core",
            "fsqlite_core::txn::test_deferred_txn",
            "Deferred lock semantics",
            &["bd-1ik"],
        )],
        &["txn", "deferred"],
        &["spec:§4.1"],
    );
    b.add(
        "F-STOR-017",
        "IMMEDIATE transactions acquire RESERVED lock on BEGIN matching SQLite 3.52.0.",
        &[],
        vec![unit_obligation(
            "fsqlite-core",
            "fsqlite_core::txn::test_immediate_txn",
            "Immediate lock semantics",
            &["bd-1ik"],
        )],
        &["txn", "immediate"],
        &["spec:§4.1"],
    );
    b.add(
        "F-STOR-018",
        "EXCLUSIVE transactions block all other connections matching SQLite 3.52.0.",
        &[],
        vec![unit_obligation(
            "fsqlite-core",
            "fsqlite_core::txn::test_exclusive_txn",
            "Exclusive lock semantics",
            &["bd-1ik"],
        )],
        &["txn", "exclusive"],
        &["spec:§4.1"],
    );
    b.add(
        "F-STOR-019",
        "Autocommit wrapping provides implicit transaction per statement matching SQLite 3.52.0.",
        &[],
        vec![unit_obligation(
            "fsqlite-core",
            "fsqlite_core::txn::test_autocommit",
            "Autocommit wrapping",
            &["bd-1ik"],
        )],
        &["txn", "autocommit"],
        &["spec:§4.1"],
    );
    // MVCC
    b.add(
        "F-STOR-020",
        "MVCC page-level locking provides exclusive per-page write locks with CAS enforcement (INV-2).",
        &["MVCC enabled"],
        vec![
            eprocess_obligation("fsqlite-harness", "fsqlite_harness::eprocess::monitor_inv2_lock_exclusivity", "INV-2 lock exclusivity monitoring", &["bd-3go.3"]),
            unit_obligation("fsqlite-mvcc", "fsqlite_mvcc::lock_table::test_page_lock", "Page-level CAS lock", &["bd-3t3"]),
        ],
        &["mvcc", "concurrent"],
        &["spec:§4.2"],
    );
    b.add(
        "F-STOR-021",
        "MVCC snapshot isolation provides consistent reads via monotonic CommitSeq (INV-1, INV-5).",
        &["MVCC enabled"],
        vec![
            eprocess_obligation(
                "fsqlite-harness",
                "fsqlite_harness::eprocess::monitor_inv1_monotonicity",
                "INV-1 monotonicity monitoring",
                &["bd-3go.3"],
            ),
            eprocess_obligation(
                "fsqlite-harness",
                "fsqlite_harness::eprocess::monitor_inv5_snapshot_stability",
                "INV-5 snapshot stability monitoring",
                &["bd-3go.3"],
            ),
        ],
        &["mvcc", "snapshot"],
        &["spec:§4.2"],
    );
    b.add(
        "F-STOR-022",
        "SSI validation prevents write skew anomalies.",
        &["MVCC enabled", "SSI validation active"],
        vec![
            unit_obligation(
                "fsqlite-mvcc",
                "fsqlite_mvcc::ssi::test_ssi_validation",
                "SSI write-skew prevention",
                &["bd-3t3"],
            ),
            eprocess_obligation(
                "fsqlite-harness",
                "fsqlite_harness::eprocess::monitor_ssi_fp_rate",
                "SSI false-positive rate monitoring",
                &["bd-3go.3"],
            ),
        ],
        &["mvcc", "ssi"],
        &["spec:§4.2"],
    );
    b.add(
        "F-STOR-023",
        "First-committer-wins conflict resolution is deterministic and prevents lost updates.",
        &["MVCC enabled"],
        vec![unit_obligation(
            "fsqlite-mvcc",
            "fsqlite_mvcc::conflict::test_first_committer_wins",
            "FCW conflict resolution",
            &["bd-3t3"],
        )],
        &["mvcc", "fcw"],
        &["spec:§4.2"],
    );
    b.add(
        "F-STOR-024",
        "Safe write merging correctly replays intents for commuting page conflicts.",
        &["MVCC enabled"],
        vec![unit_obligation(
            "fsqlite-mvcc",
            "fsqlite_mvcc::merge::test_safe_merge",
            "Safe write merging",
            &["bd-3t3"],
        )],
        &["mvcc", "merge"],
        &["spec:§4.2"],
    );
    b.add(
        "F-STOR-025",
        "MVCC garbage collection reclaims old page versions via epoch-based reclamation.",
        &["MVCC enabled"],
        vec![unit_obligation(
            "fsqlite-mvcc",
            "fsqlite_mvcc::gc::test_epoch_gc",
            "Epoch-based GC",
            &["bd-3t3"],
        )],
        &["mvcc", "gc"],
        &["spec:§4.2"],
    );
    // VFS
    b.add(
        "F-STOR-026",
        "Unix VFS provides POSIX file I/O with advisory locking matching SQLite 3.52.0.",
        &["Unix platform"],
        vec![unit_obligation(
            "fsqlite-vfs",
            "fsqlite_vfs::unix::test_unix_vfs",
            "Unix VFS operations",
            &["bd-1ik"],
        )],
        &["vfs", "unix"],
        &["spec:§2"],
    );
    b.add(
        "F-STOR-027",
        "Memory VFS provides in-memory database backend matching SQLite 3.52.0.",
        &[],
        vec![unit_obligation(
            "fsqlite-vfs",
            "fsqlite_vfs::memory::test_memory_vfs",
            "Memory VFS operations",
            &["bd-1ik"],
        )],
        &["vfs", "memory"],
        &["spec:§2"],
    );

    b.build()
}

#[allow(clippy::too_many_lines)]
fn build_pragma_invariants() -> Vec<ParityInvariant> {
    let mut b = InvariantBuilder::new(FeatureCategory::Pragma);

    b.add(
        "F-PRAGMA-001",
        "PRAGMA journal_mode read/set behaviour matches SQLite 3.52.0 mode transitions.",
        &[],
        vec![unit_obligation(
            "fsqlite-core",
            "fsqlite_core::pragma::test_journal_mode",
            "journal_mode transitions",
            &["bd-1ik"],
        )],
        &["pragma", "journal"],
        &["spec:§12"],
    );
    b.add(
        "F-PRAGMA-002",
        "PRAGMA cache_size matches SQLite 3.52.0 page cache sizing.",
        &[],
        vec![unit_obligation(
            "fsqlite-core",
            "fsqlite_core::pragma::test_cache_size",
            "cache_size setting",
            &["bd-1ik"],
        )],
        &["pragma", "cache"],
        &["spec:§12"],
    );
    b.add(
        "F-PRAGMA-003",
        "PRAGMA page_size matches SQLite 3.52.0 page size constraints.",
        &[],
        vec![unit_obligation(
            "fsqlite-core",
            "fsqlite_core::pragma::test_page_size",
            "page_size constraints",
            &["bd-1ik"],
        )],
        &["pragma", "page"],
        &["spec:§12"],
    );
    b.add(
        "F-PRAGMA-004",
        "PRAGMA synchronous mode behaviour matches SQLite 3.52.0 durability guarantees.",
        &[],
        vec![unit_obligation(
            "fsqlite-core",
            "fsqlite_core::pragma::test_synchronous",
            "synchronous mode",
            &["bd-1ik"],
        )],
        &["pragma", "sync"],
        &["spec:§12"],
    );
    b.add(
        "F-PRAGMA-005",
        "PRAGMA foreign_keys enforcement matches SQLite 3.52.0 FK semantics.",
        &[],
        vec![unit_obligation(
            "fsqlite-core",
            "fsqlite_core::pragma::test_foreign_keys",
            "FK enforcement",
            &["bd-1ik"],
        )],
        &["pragma", "fk"],
        &["spec:§12"],
    );
    b.add(
        "F-PRAGMA-006",
        "PRAGMA wal_checkpoint trigger matches SQLite 3.52.0 checkpoint behaviour.",
        &["WAL journal mode"],
        vec![unit_obligation(
            "fsqlite-core",
            "fsqlite_core::pragma::test_wal_checkpoint",
            "wal_checkpoint execution",
            &["bd-1ik"],
        )],
        &["pragma", "wal"],
        &["spec:§12"],
    );
    b.add(
        "F-PRAGMA-007",
        "PRAGMA integrity_check produces identical verification results to SQLite 3.52.0.",
        &[],
        vec![unit_obligation(
            "fsqlite-core",
            "fsqlite_core::pragma::test_integrity_check",
            "integrity_check verification",
            &["bd-1ik"],
        )],
        &["pragma", "integrity"],
        &["spec:§12"],
    );
    // Remaining PRAGMAs combined
    b.add(
        "F-PRAGMA-008",
        "PRAGMAs auto_vacuum, encoding, table_info, index_list/index_info, database_list, compile_options, user_version/schema_version, busy_timeout, and temp_store all match SQLite 3.52.0.",
        &[],
        vec![
            unit_obligation("fsqlite-core", "fsqlite_core::pragma::test_misc_pragmas", "Miscellaneous PRAGMA coverage", &["bd-1ik"]),
        ],
        &["pragma"],
        &["spec:§12"],
    );

    b.build()
}

fn build_builtin_function_invariants() -> Vec<ParityInvariant> {
    let mut b = InvariantBuilder::new(FeatureCategory::BuiltinFunctions);

    b.add(
        "F-FUNC-001",
        "Core scalar functions (abs, length, typeof, coalesce, ifnull, nullif, etc.) produce identical results to SQLite 3.52.0.",
        &[],
        vec![
            unit_obligation("fsqlite-core", "fsqlite_core::functions::test_scalar_functions", "Scalar function evaluation", &["bd-9y1"]),
        ],
        &["function", "scalar"],
        &["spec:§13.1"],
    );
    b.add(
        "F-FUNC-002",
        "Math functions (sqrt, log, exp, sin, cos, etc.) produce identical results to SQLite 3.52.0.",
        &[],
        vec![
            unit_obligation("fsqlite-core", "fsqlite_core::functions::test_math_functions", "Math function evaluation", &["bd-9y1"]),
        ],
        &["function", "math"],
        &["spec:§13.1"],
    );
    b.add(
        "F-FUNC-003",
        "String functions (substr, replace, trim, upper, lower, etc.) produce identical results to SQLite 3.52.0.",
        &[],
        vec![
            unit_obligation("fsqlite-core", "fsqlite_core::functions::test_string_functions", "String function evaluation", &["bd-9y1"]),
        ],
        &["function", "string"],
        &["spec:§13.1"],
    );
    b.add(
        "F-FUNC-004",
        "Date/time functions (date, time, datetime, strftime, julianday, etc.) produce identical results to SQLite 3.52.0.",
        &[],
        vec![
            unit_obligation("fsqlite-core", "fsqlite_core::functions::test_datetime_functions", "Date/time function evaluation", &["bd-9y1"]),
        ],
        &["function", "datetime"],
        &["spec:§13.1"],
    );
    b.add(
        "F-FUNC-005",
        "Aggregate functions (count, sum, avg, min, max, group_concat, total) produce identical results to SQLite 3.52.0.",
        &[],
        vec![
            unit_obligation("fsqlite-core", "fsqlite_core::functions::test_aggregate_functions", "Aggregate function evaluation", &["bd-9y1"]),
        ],
        &["function", "aggregate"],
        &["spec:§13.2"],
    );
    b.add(
        "F-FUNC-006",
        "Window functions (row_number, rank, dense_rank, ntile, lag, lead, first_value, last_value, nth_value) produce identical results to SQLite 3.52.0.",
        &[],
        vec![
            unit_obligation("fsqlite-core", "fsqlite_core::functions::test_window_functions", "Window function evaluation", &["bd-9y1"]),
        ],
        &["function", "window"],
        &["spec:§13.3"],
    );

    b.build()
}

fn build_extension_invariants() -> Vec<ParityInvariant> {
    let mut b = InvariantBuilder::new(FeatureCategory::Extensions);

    b.add(
        "F-EXT-001",
        "FTS5 full-text search produces identical match results and ranking to SQLite 3.52.0.",
        &["FTS5 extension loaded"],
        vec![
            unit_obligation(
                "fsqlite-extensions",
                "fsqlite_extensions::fts5::test_fts5",
                "FTS5 search and ranking",
                &["bd-3c7"],
            ),
            differential_obligation(
                "fsqlite-harness",
                "fsqlite_harness::extension_parity_matrix::test_fts5_parity",
                "FTS5 differential comparison",
                &["bd-1dp9.5"],
            ),
        ],
        &["extension", "fts5"],
        &["spec:§14.1"],
    );
    b.add(
        "F-EXT-002",
        "JSON1 functions (json, json_extract, json_insert, json_set, etc.) produce identical results to SQLite 3.52.0.",
        &["JSON1 extension loaded"],
        vec![
            unit_obligation("fsqlite-extensions", "fsqlite_extensions::json1::test_json1", "JSON1 function evaluation", &["bd-3c7"]),
            differential_obligation("fsqlite-harness", "fsqlite_harness::extension_parity_matrix::test_json1_parity", "JSON1 differential comparison", &["bd-1dp9.5"]),
        ],
        &["extension", "json1"],
        &["spec:§14.2"],
    );
    b.add(
        "F-EXT-003",
        "R-tree spatial indexing produces identical query results to SQLite 3.52.0.",
        &["R-tree extension loaded"],
        vec![
            unit_obligation(
                "fsqlite-extensions",
                "fsqlite_extensions::rtree::test_rtree",
                "R-tree spatial queries",
                &["bd-3c7"],
            ),
            differential_obligation(
                "fsqlite-harness",
                "fsqlite_harness::extension_parity_matrix::test_rtree_parity",
                "R-tree differential comparison",
                &["bd-1dp9.5.3"],
            ),
        ],
        &["extension", "rtree"],
        &["spec:§14.3"],
    );
    b.add(
        "F-EXT-004",
        "Session/changeset API produces identical change tracking to SQLite 3.52.0.",
        &["Session extension loaded"],
        vec![unit_obligation(
            "fsqlite-extensions",
            "fsqlite_extensions::session::test_session",
            "Session change tracking",
            &["bd-3c7"],
        )],
        &["extension", "session"],
        &["spec:§14.4"],
    );
    b.add(
        "F-EXT-005",
        "ICU collation and case folding match SQLite 3.52.0 behaviour with ICU data.",
        &["ICU extension loaded"],
        vec![unit_obligation(
            "fsqlite-extensions",
            "fsqlite_extensions::icu::test_icu",
            "ICU collation",
            &["bd-3c7"],
        )],
        &["extension", "icu"],
        &["spec:§14.5"],
    );

    b.build()
}

fn build_type_system_invariants() -> Vec<ParityInvariant> {
    let mut b = InvariantBuilder::new(FeatureCategory::TypeSystem);

    b.add(
        "F-TYPE-001",
        "Type affinity assignment matches SQLite 3.52.0 for all declared column types.",
        &[],
        vec![unit_obligation(
            "fsqlite-types",
            "fsqlite_types::affinity::test_type_affinity",
            "Type affinity assignment",
            &["bd-1ik"],
        )],
        &["type", "affinity"],
        &["spec:§9.1"],
    );
    b.add(
        "F-TYPE-002",
        "Type coercion during comparison operations matches SQLite 3.52.0 rules.",
        &[],
        vec![unit_obligation(
            "fsqlite-types",
            "fsqlite_types::coercion::test_type_coercion",
            "Type coercion rules",
            &["bd-1ik"],
        )],
        &["type", "coercion"],
        &["spec:§9.2"],
    );
    b.add(
        "F-TYPE-003",
        "Collation sequence resolution (BINARY, NOCASE, RTRIM) matches SQLite 3.52.0.",
        &[],
        vec![unit_obligation(
            "fsqlite-types",
            "fsqlite_types::collation::test_collation",
            "Collation sequence resolution",
            &["bd-1ik"],
        )],
        &["type", "collation"],
        &["spec:§9.3"],
    );
    b.add(
        "F-TYPE-004",
        "Text encoding (UTF-8) internal representation matches SQLite 3.52.0 byte layout.",
        &[],
        vec![unit_obligation(
            "fsqlite-types",
            "fsqlite_types::encoding::test_utf8_encoding",
            "UTF-8 encoding layout",
            &["bd-1ik"],
        )],
        &["type", "encoding"],
        &["spec:§9.4"],
    );

    b.build()
}

fn build_file_format_invariants() -> Vec<ParityInvariant> {
    let mut b = InvariantBuilder::new(FeatureCategory::FileFormat);

    b.add(
        "F-FMT-001",
        "Database file header (first 100 bytes) matches SQLite 3.52.0 format specification.",
        &[],
        vec![unit_obligation(
            "fsqlite-pager",
            "fsqlite_pager::header::test_file_header",
            "File header format",
            &["bd-7pu"],
        )],
        &["format", "header"],
        &["spec:§8.1"],
    );
    b.add(
        "F-FMT-002",
        "Page layout (cell pointer array, cell content, free space) matches SQLite 3.52.0 format.",
        &[],
        vec![unit_obligation(
            "fsqlite-btree",
            "fsqlite_btree::page::test_page_layout",
            "Page layout format",
            &["bd-7pu"],
        )],
        &["format", "page"],
        &["spec:§8.2"],
    );
    b.add(
        "F-FMT-003",
        "Record encoding (serial type codes, varint headers) matches SQLite 3.52.0 format.",
        &[],
        vec![
            unit_obligation(
                "fsqlite-types",
                "fsqlite_types::record::test_record_encoding",
                "Record serial type encoding",
                &["bd-1ik"],
            ),
            property_obligation(
                "fsqlite-harness",
                "fsqlite_harness::test_varint_edge_cases",
                "Varint edge-case properties",
                &["bd-1y7b"],
            ),
        ],
        &["format", "record"],
        &["spec:§8.3"],
    );
    b.add(
        "F-FMT-004",
        "Overflow page chain format matches SQLite 3.52.0 for large records.",
        &[],
        vec![unit_obligation(
            "fsqlite-btree",
            "fsqlite_btree::overflow::test_overflow_format",
            "Overflow chain format",
            &["bd-7pu"],
        )],
        &["format", "overflow"],
        &["spec:§8.4"],
    );

    b.build()
}

fn build_api_cli_invariants() -> Vec<ParityInvariant> {
    let mut b = InvariantBuilder::new(FeatureCategory::ApiCli);

    b.add(
        "F-API-001",
        "Connection lifecycle (open, close, busy handling) matches SQLite 3.52.0 API semantics.",
        &[],
        vec![unit_obligation(
            "fsqlite-core",
            "fsqlite_core::connection::test_connection_lifecycle",
            "Connection open/close",
            &["bd-1ik"],
        )],
        &["api", "connection"],
        &["spec:§15.1"],
    );
    b.add(
        "F-API-002",
        "Prepared statement lifecycle (prepare, step, reset, finalize) matches SQLite 3.52.0 API semantics.",
        &[],
        vec![
            unit_obligation("fsqlite-core", "fsqlite_core::statement::test_prepared_stmt", "Prepared statement lifecycle", &["bd-1ik"]),
        ],
        &["api", "statement"],
        &["spec:§15.2"],
    );
    b.add(
        "F-API-003",
        "CLI interactive and batch modes produce identical output to SQLite 3.52.0 CLI.",
        &["CLI binary available"],
        vec![e2e_obligation(
            "fsqlite-e2e",
            "fsqlite_e2e::cli::test_cli_batch",
            "CLI batch mode output",
            &["bd-1ik"],
        )],
        &["cli"],
        &["spec:§15.3"],
    );
    b.add(
        "F-API-004",
        "Error codes and error messages match SQLite 3.52.0 error reporting.",
        &[],
        vec![unit_obligation(
            "fsqlite-core",
            "fsqlite_core::error::test_error_codes",
            "Error code mapping",
            &["bd-1ik"],
        )],
        &["api", "error"],
        &["spec:§15.4"],
    );

    b.build()
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_catalog_builds() {
        let catalog = build_canonical_catalog();
        assert!(
            !catalog.invariants.is_empty(),
            "canonical catalog must not be empty"
        );
    }

    #[test]
    fn canonical_catalog_validates() {
        let catalog = build_canonical_catalog();
        let violations = catalog.validate();
        assert!(
            violations.is_empty(),
            "canonical catalog has violations: {violations:?}"
        );
    }

    #[test]
    fn canonical_catalog_covers_all_categories() {
        let catalog = build_canonical_catalog();
        for cat in FeatureCategory::ALL {
            let count = catalog.by_category(cat).len();
            assert!(
                count > 0,
                "category {cat:?} has no invariants in canonical catalog"
            );
        }
    }

    #[test]
    fn invariant_ids_are_unique() {
        let catalog = build_canonical_catalog();
        let mut seen = BTreeSet::new();
        for id in catalog.invariants.keys() {
            assert!(seen.insert(id.clone()), "duplicate invariant ID: {id}");
        }
    }

    #[test]
    fn every_invariant_has_feature_id() {
        let catalog = build_canonical_catalog();
        for inv in catalog.invariants.values() {
            assert!(
                !inv.feature_id.0.is_empty(),
                "invariant {} has empty feature_id",
                inv.id
            );
        }
    }

    #[test]
    fn every_invariant_has_statement() {
        let catalog = build_canonical_catalog();
        for inv in catalog.invariants.values() {
            assert!(
                !inv.statement.trim().is_empty(),
                "invariant {} has empty statement",
                inv.id
            );
        }
    }

    #[test]
    fn every_invariant_has_obligations() {
        let catalog = build_canonical_catalog();
        for inv in catalog.invariants.values() {
            assert!(
                !inv.obligations.is_empty(),
                "invariant {} has no proof obligations",
                inv.id
            );
        }
    }

    #[test]
    fn obligation_test_paths_non_empty() {
        let catalog = build_canonical_catalog();
        for inv in catalog.invariants.values() {
            for (i, obl) in inv.obligations.iter().enumerate() {
                assert!(
                    !obl.test_path.trim().is_empty(),
                    "invariant {} obligation[{i}] has empty test_path",
                    inv.id
                );
            }
        }
    }

    #[test]
    fn waived_obligations_have_rationale() {
        let catalog = build_canonical_catalog();
        for inv in catalog.invariants.values() {
            for (i, obl) in inv.obligations.iter().enumerate() {
                if obl.status == ObligationStatus::Waived {
                    assert!(
                        obl.waiver_rationale.is_some(),
                        "invariant {} obligation[{i}] is waived without rationale",
                        inv.id
                    );
                }
            }
        }
    }

    #[test]
    fn stats_are_consistent() {
        let catalog = build_canonical_catalog();
        let stats = catalog.stats();
        assert_eq!(
            stats.total_invariants,
            catalog.invariants.len(),
            "total_invariants mismatch"
        );
        assert_eq!(
            stats.categories_covered,
            FeatureCategory::ALL.len(),
            "all categories must be covered"
        );
        assert!(
            stats.features_covered > 0,
            "must cover at least one feature"
        );
        assert_eq!(
            stats.verified_invariants + stats.partial_invariants + stats.pending_invariants,
            stats.total_invariants,
            "invariant status counts must sum to total"
        );
    }

    #[test]
    fn per_category_stats_sum_to_total() {
        let catalog = build_canonical_catalog();
        let stats = catalog.stats();
        let sum: usize = stats.per_category.values().sum();
        assert_eq!(
            sum, stats.total_invariants,
            "per_category counts must sum to total_invariants"
        );
    }

    #[test]
    fn release_traceability_report_generated() {
        let catalog = build_canonical_catalog();
        let report = catalog.release_traceability();
        assert_eq!(
            report.entries.len(),
            catalog.invariants.len(),
            "report must have one entry per invariant"
        );
        assert_eq!(report.schema_version, CATALOG_SCHEMA_VERSION);
    }

    #[test]
    fn release_traceability_verification_pct_range() {
        let catalog = build_canonical_catalog();
        let report = catalog.release_traceability();
        assert!(
            (0.0..=100.0).contains(&report.verification_pct),
            "verification_pct must be in [0, 100], got {}",
            report.verification_pct
        );
    }

    #[test]
    fn json_roundtrip() {
        let catalog = build_canonical_catalog();
        let json = catalog.to_json().expect("serialisation");
        let deserialized = InvariantCatalog::from_json(&json).expect("deserialisation");
        assert_eq!(
            catalog.invariants.len(),
            deserialized.invariants.len(),
            "roundtrip must preserve invariant count"
        );
        for (id, inv) in &catalog.invariants {
            let deserialized_inv = deserialized
                .invariants
                .get(id)
                .unwrap_or_else(|| panic!("missing invariant {id} after roundtrip"));
            assert_eq!(
                inv.statement, deserialized_inv.statement,
                "statement mismatch for {id}"
            );
            assert_eq!(
                inv.obligations.len(),
                deserialized_inv.obligations.len(),
                "obligation count mismatch for {id}"
            );
        }
    }

    #[test]
    fn by_category_returns_correct_invariants() {
        let catalog = build_canonical_catalog();
        for cat in FeatureCategory::ALL {
            let filtered = catalog.by_category(cat);
            for inv in &filtered {
                assert_eq!(inv.category, cat, "invariant {} has wrong category", inv.id);
            }
        }
    }

    #[test]
    fn by_feature_returns_matching_invariants() {
        let catalog = build_canonical_catalog();
        let feature_id = FeatureId("F-SQL-001".to_owned());
        let filtered = catalog.by_feature(&feature_id);
        for inv in &filtered {
            assert_eq!(
                inv.feature_id, feature_id,
                "invariant {} has wrong feature_id",
                inv.id
            );
        }
    }

    #[test]
    fn by_tag_returns_matching_invariants() {
        let catalog = build_canonical_catalog();
        let filtered = catalog.by_tag("mvcc");
        assert!(
            !filtered.is_empty(),
            "must have invariants tagged with 'mvcc'"
        );
        for inv in &filtered {
            assert!(
                inv.tags.contains("mvcc"),
                "invariant {} missing 'mvcc' tag",
                inv.id
            );
        }
    }

    #[test]
    fn unverified_returns_non_fully_verified() {
        let catalog = build_canonical_catalog();
        let unverified = catalog.unverified();
        for inv in &unverified {
            assert!(
                !inv.is_fully_verified(),
                "invariant {} should not be fully verified",
                inv.id
            );
        }
    }

    #[test]
    fn obligation_summary_is_consistent() {
        let catalog = build_canonical_catalog();
        for inv in catalog.invariants.values() {
            let summary = inv.obligation_summary();
            assert_eq!(
                summary.total,
                inv.obligations.len(),
                "obligation summary total mismatch for {}",
                inv.id
            );
            assert_eq!(
                summary.verified + summary.partial + summary.pending + summary.waived,
                summary.total,
                "obligation summary counts don't sum for {}",
                inv.id
            );
        }
    }

    #[test]
    fn is_fully_verified_requires_non_empty_obligations() {
        let inv = ParityInvariant {
            id: InvariantId::new("TEST", 1),
            feature_id: FeatureId("F-TEST-001".to_owned()),
            category: FeatureCategory::SqlGrammar,
            statement: "test".to_owned(),
            assumptions: Vec::new(),
            obligations: Vec::new(),
            tags: BTreeSet::new(),
            spec_refs: Vec::new(),
        };
        assert!(
            !inv.is_fully_verified(),
            "invariant with no obligations must not be fully verified"
        );
    }

    #[test]
    fn empty_catalog_fails_validation() {
        let catalog = InvariantCatalog {
            schema_version: CATALOG_SCHEMA_VERSION,
            invariants: BTreeMap::new(),
        };
        let violations = catalog.validate();
        assert!(
            !violations.is_empty(),
            "empty catalog must have validation violations"
        );
        // Should fail CAT-VAL-4 for every category
        let cat_violation_count = violations.iter().filter(|v| v.rule == "CAT-VAL-4").count();
        assert_eq!(
            cat_violation_count,
            FeatureCategory::ALL.len(),
            "empty catalog must violate CAT-VAL-4 for each category"
        );
    }

    #[test]
    fn violation_display() {
        let v = Violation {
            invariant_id: Some(InvariantId::new("SQL", 1)),
            rule: "CAT-VAL-2".to_owned(),
            message: "no obligations".to_owned(),
        };
        let s = format!("{v}");
        assert!(
            s.contains("PAR-SQL-001"),
            "display must include invariant ID"
        );
        assert!(s.contains("CAT-VAL-2"), "display must include rule");
    }

    #[test]
    fn catalog_level_violation_display() {
        let v = Violation {
            invariant_id: None,
            rule: "CAT-VAL-4".to_owned(),
            message: "missing category".to_owned(),
        };
        let s = format!("{v}");
        assert!(
            s.contains("[catalog]"),
            "catalog-level violation must show [catalog]"
        );
    }

    #[test]
    fn proof_kind_display() {
        assert_eq!(format!("{}", ProofKind::UnitTest), "unit_test");
        assert_eq!(
            format!("{}", ProofKind::EProcessMonitor),
            "e_process_monitor"
        );
        assert_eq!(
            format!("{}", ProofKind::DifferentialOracle),
            "differential_oracle"
        );
    }

    #[test]
    fn obligation_status_display() {
        assert_eq!(format!("{}", ObligationStatus::Verified), "verified");
        assert_eq!(format!("{}", ObligationStatus::Pending), "pending");
        assert_eq!(format!("{}", ObligationStatus::Waived), "waived");
    }

    #[test]
    fn obligation_status_satisfaction() {
        assert!(ObligationStatus::Verified.is_satisfied());
        assert!(ObligationStatus::Waived.is_satisfied());
        assert!(!ObligationStatus::Partial.is_satisfied());
        assert!(!ObligationStatus::Pending.is_satisfied());
    }

    #[test]
    fn invariant_id_display() {
        let id = InvariantId::new("SQL", 42);
        assert_eq!(format!("{id}"), "PAR-SQL-042");
    }

    #[test]
    fn sql_grammar_has_most_invariants() {
        let catalog = build_canonical_catalog();
        let sql_count = catalog.by_category(FeatureCategory::SqlGrammar).len();
        for cat in FeatureCategory::ALL {
            if cat != FeatureCategory::SqlGrammar {
                let count = catalog.by_category(cat).len();
                assert!(
                    sql_count >= count,
                    "SQL grammar should have >= invariants as {cat:?} ({sql_count} vs {count})"
                );
            }
        }
    }

    #[test]
    fn mvcc_invariants_reference_eprocess() {
        let catalog = build_canonical_catalog();
        let mvcc_invs = catalog.by_tag("mvcc");
        let has_eprocess = mvcc_invs.iter().any(|inv| {
            inv.obligations
                .iter()
                .any(|o| o.kind == ProofKind::EProcessMonitor)
        });
        assert!(
            has_eprocess,
            "MVCC invariants must include at least one e-process monitor obligation"
        );
    }

    #[test]
    fn storage_invariants_cover_wal_and_btree() {
        let catalog = build_canonical_catalog();
        let stor_invs = catalog.by_category(FeatureCategory::StorageTransaction);
        let has_wal = stor_invs.iter().any(|inv| inv.tags.contains("wal"));
        let has_btree = stor_invs.iter().any(|inv| inv.tags.contains("btree"));
        assert!(has_wal, "storage invariants must cover WAL");
        assert!(has_btree, "storage invariants must cover B-tree");
    }

    #[test]
    fn extension_invariants_cover_fts_and_json() {
        let catalog = build_canonical_catalog();
        let ext_invs = catalog.by_category(FeatureCategory::Extensions);
        let has_fts = ext_invs.iter().any(|inv| inv.tags.contains("fts5"));
        let has_json = ext_invs.iter().any(|inv| inv.tags.contains("json1"));
        assert!(has_fts, "extension invariants must cover FTS5");
        assert!(has_json, "extension invariants must cover JSON1");
    }

    #[test]
    fn pending_obligations_exist_for_missing_features() {
        let catalog = build_canonical_catalog();
        let has_pending = catalog.invariants.values().any(|inv| {
            inv.obligations
                .iter()
                .any(|o| o.status == ObligationStatus::Pending)
        });
        assert!(
            has_pending,
            "catalog should have some pending obligations for not-yet-implemented features"
        );
    }

    #[test]
    fn spec_refs_are_non_empty_for_all_invariants() {
        let catalog = build_canonical_catalog();
        for inv in catalog.invariants.values() {
            assert!(
                !inv.spec_refs.is_empty(),
                "invariant {} should have spec references",
                inv.id
            );
        }
    }

    #[test]
    fn total_invariant_count_reasonable() {
        let catalog = build_canonical_catalog();
        // We should have at least 50 invariants covering 100+ features
        assert!(
            catalog.invariants.len() >= 50,
            "canonical catalog should have at least 50 invariants, got {}",
            catalog.invariants.len()
        );
        assert!(
            catalog.invariants.len() <= 500,
            "canonical catalog should have at most 500 invariants, got {}",
            catalog.invariants.len()
        );
    }
}
