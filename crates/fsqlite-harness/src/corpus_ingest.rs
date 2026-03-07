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
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::mismatch_minimizer::{minimize_workload, MinimizerConfig, ReproducibilityTest};
use crate::oracle::{parse_slt, SltKind};

/// Bead identifier for log correlation.
const BEAD_ID: &str = "bd-1dp9.2.1";
/// Bead identifier for user repro intake/minimization pipeline.
const USER_REPRO_BEAD_ID: &str = "bd-2yqp6.3.5";

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
    /// User-reported mismatch intake reduced to deterministic replay form.
    UserRepro {
        /// Deterministic fixture identifier.
        fixture_id: String,
        /// Trace identifier for structured diagnostics.
        trace_id: String,
        /// Run identifier for structured diagnostics.
        run_id: String,
        /// Scenario identifier for structured diagnostics.
        scenario_id: String,
        /// Original workload statement count before minimization.
        original_statement_count: usize,
        /// Minimized workload statement count after minimization.
        minimized_statement_count: usize,
    },
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

/// Intake request for converting a user-reported mismatch into a deterministic
/// minimized corpus fixture.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserReproIntakeRequest {
    /// Human-readable title for this user report.
    pub title: String,
    /// Optional free-form details from the reporter.
    pub details: String,
    /// Trace identifier required by structured log contracts.
    pub trace_id: String,
    /// Run identifier required by structured log contracts.
    pub run_id: String,
    /// Scenario identifier required by structured log contracts.
    pub scenario_id: String,
    /// Deterministic seed for replay/minimization.
    pub seed: u64,
    /// Optional reporter handle or source reference.
    pub reporter: Option<String>,
    /// Schema/setup SQL needed before replay workload execution.
    pub schema: Vec<String>,
    /// Original failing workload SQL statements.
    pub workload: Vec<String>,
    /// Optional taxonomy feature IDs to attach for coverage accounting.
    pub taxonomy_features: Vec<String>,
}

/// Deterministic minimized fixture artifact emitted by user repro intake.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UserReproFixtureArtifact {
    /// Schema version for this artifact payload.
    pub schema_version: u32,
    /// Owning bead id.
    pub bead_id: String,
    /// Deterministic fixture id used for file naming/dedupe.
    pub fixture_id: String,
    /// Trace identifier carried through replay metadata.
    pub trace_id: String,
    /// Run identifier carried through replay metadata.
    pub run_id: String,
    /// Scenario identifier carried through replay metadata.
    pub scenario_id: String,
    /// Deterministic seed for replay.
    pub seed: u64,
    /// Signature hash produced by mismatch minimization.
    pub signature_hash: String,
    /// Classification text emitted by mismatch minimizer.
    pub classification: String,
    /// Attributed subsystem text emitted by mismatch minimizer.
    pub subsystem: String,
    /// Original failing workload size (without schema setup statements).
    pub original_statement_count: usize,
    /// Minimized workload size (without schema setup statements).
    pub minimized_statement_count: usize,
    /// Reduction ratio in [0.0, 1.0].
    pub reduction_ratio: f64,
    /// Full replay SQL for original failing behavior (schema + workload).
    pub original_replay_sql: Vec<String>,
    /// Full replay SQL for minimized failing behavior (schema + workload).
    pub minimized_replay_sql: Vec<String>,
    /// Single-line command reference for original replay.
    pub original_replay_command: String,
    /// Single-line command reference for minimized replay.
    pub minimized_replay_command: String,
    /// Divergence count captured by the minimizer on minimized replay.
    pub divergence_count: usize,
    /// First divergence index from minimized replay (if available).
    pub first_divergence_index: Option<usize>,
}

/// Result from user repro intake pipeline.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UserReproIntakeReport {
    /// Deterministic corpus entry identifier created by intake.
    pub entry_id: String,
    /// Primary family used for coverage accounting.
    pub family: Family,
    /// Emitted minimized fixture artifact.
    pub artifact: UserReproFixtureArtifact,
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
    /// Active entry counts by source type.
    pub by_source: BTreeMap<String, usize>,
    /// Count of active entries ingested from user repro pipeline.
    pub user_repro_entries: usize,
    /// Total minimized statement count across user repro entries.
    pub user_repro_minimized_statements: usize,
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

/// Intake a user-reported mismatch and append a minimized deterministic corpus
/// entry, preserving both original and minimized replay scripts.
///
/// # Errors
///
/// Returns an error when request metadata is incomplete or the minimizer cannot
/// reproduce/reduce the provided workload.
pub fn intake_user_repro_fixture(
    builder: &mut CorpusBuilder,
    request: &UserReproIntakeRequest,
    minimizer_config: &MinimizerConfig,
    test_fn: &ReproducibilityTest,
) -> Result<UserReproIntakeReport, String> {
    validate_user_repro_request(request)?;

    let minimal = minimize_workload(
        &request.schema,
        &request.workload,
        minimizer_config,
        test_fn,
    )
    .ok_or_else(|| "unable to minimize user repro: workload is not reproducible".to_owned())?;

    let fixture_id = deterministic_user_repro_fixture_id(request, &minimal.signature.hash);
    let original_replay_sql = compose_replay_sql(&request.schema, &request.workload);
    let minimized_replay_sql = compose_replay_sql(&request.schema, &minimal.minimal_workload);

    let original_replay_command = format!(
        "RUN_ID='{}' TRACE_ID='{}' SCENARIO_ID='{}' SEED={} bash scripts/verify_corpus_ingest.sh --json",
        request.run_id, request.trace_id, request.scenario_id, request.seed
    );
    let minimized_replay_command = format!(
        "RUN_ID='{}' TRACE_ID='{}' SCENARIO_ID='{}' SEED={} bash scripts/verify_corpus_ingest.sh --json",
        request.run_id, request.trace_id, request.scenario_id, request.seed
    );

    let (family, _) = classify_family(&compose_replay_sql(
        &request.schema,
        &minimal.minimal_workload,
    ));

    let entry_index = u32::try_from(builder.entries.len()).unwrap_or(u32::MAX);
    let entry_id = format!("corpus-{family}-{entry_index:04}");
    let description = format!("user-repro {}: {}", fixture_id, request.title);

    builder.add_with_family(
        family,
        minimized_replay_sql.clone(),
        CorpusSource::UserRepro {
            fixture_id: fixture_id.clone(),
            trace_id: request.trace_id.clone(),
            run_id: request.run_id.clone(),
            scenario_id: request.scenario_id.clone(),
            original_statement_count: request.workload.len(),
            minimized_statement_count: minimal.minimal_workload.len(),
        },
        description,
    );
    if !request.taxonomy_features.is_empty() {
        builder.link_features(request.taxonomy_features.clone());
    }

    let artifact = UserReproFixtureArtifact {
        schema_version: 1,
        bead_id: USER_REPRO_BEAD_ID.to_owned(),
        fixture_id,
        trace_id: request.trace_id.clone(),
        run_id: request.run_id.clone(),
        scenario_id: request.scenario_id.clone(),
        seed: request.seed,
        signature_hash: minimal.signature.hash.clone(),
        classification: minimal.signature.classification.to_string(),
        subsystem: minimal.signature.subsystem.to_string(),
        original_statement_count: request.workload.len(),
        minimized_statement_count: minimal.minimal_workload.len(),
        reduction_ratio: minimal.reduction_ratio,
        original_replay_sql,
        minimized_replay_sql,
        original_replay_command,
        minimized_replay_command,
        divergence_count: minimal.divergences.len(),
        first_divergence_index: minimal.first_divergence_index,
    };

    Ok(UserReproIntakeReport {
        entry_id,
        family,
        artifact,
    })
}

/// Render a deterministic fixture JSON payload for a minimized user repro.
///
/// The payload is ingestible by [`ingest_conformance_fixtures_with_report`].
///
/// # Errors
///
/// Returns an error if serialization fails.
pub fn render_user_repro_fixture_json(
    artifact: &UserReproFixtureArtifact,
) -> Result<String, String> {
    let payload = serde_json::json!({
        "id": artifact.fixture_id,
        "description": format!(
            "user repro minimized fixture ({})",
            artifact.signature_hash
        ),
        "ops": artifact
            .minimized_replay_sql
            .iter()
            .map(|sql| serde_json::json!({ "sql": sql }))
            .collect::<Vec<_>>(),
        "metadata": {
            "schema_version": artifact.schema_version,
            "bead_id": artifact.bead_id,
            "trace_id": artifact.trace_id,
            "run_id": artifact.run_id,
            "scenario_id": artifact.scenario_id,
            "seed": artifact.seed,
            "signature_hash": artifact.signature_hash,
            "classification": artifact.classification,
            "subsystem": artifact.subsystem,
            "original_statement_count": artifact.original_statement_count,
            "minimized_statement_count": artifact.minimized_statement_count,
            "reduction_ratio": artifact.reduction_ratio,
            "original_replay_sql": artifact.original_replay_sql,
            "minimized_replay_sql": artifact.minimized_replay_sql,
            "original_replay_command": artifact.original_replay_command,
            "minimized_replay_command": artifact.minimized_replay_command,
            "divergence_count": artifact.divergence_count,
            "first_divergence_index": artifact.first_divergence_index,
        }
    });

    serde_json::to_string_pretty(&payload)
        .map_err(|error| format!("failed to serialize user repro fixture: {error}"))
}

/// Write a deterministic user repro fixture JSON file to `output_dir`.
///
/// # Errors
///
/// Returns an error when directory creation, serialization, or writing fails.
pub fn write_user_repro_fixture(
    output_dir: &Path,
    artifact: &UserReproFixtureArtifact,
) -> Result<std::path::PathBuf, String> {
    std::fs::create_dir_all(output_dir).map_err(|error| {
        format!(
            "failed to create user repro output dir {}: {error}",
            output_dir.display()
        )
    })?;

    let fixture_json = render_user_repro_fixture_json(artifact)?;
    let path = output_dir.join(format!("{}.json", artifact.fixture_id));
    std::fs::write(&path, fixture_json).map_err(|error| {
        format!(
            "failed to write user repro fixture {}: {error}",
            path.display()
        )
    })?;
    Ok(path)
}

fn validate_user_repro_request(request: &UserReproIntakeRequest) -> Result<(), String> {
    if request.title.trim().is_empty() {
        return Err("user repro request title cannot be empty".to_owned());
    }
    if request.trace_id.trim().is_empty() {
        return Err("user repro request trace_id cannot be empty".to_owned());
    }
    if request.run_id.trim().is_empty() {
        return Err("user repro request run_id cannot be empty".to_owned());
    }
    if request.scenario_id.trim().is_empty() {
        return Err("user repro request scenario_id cannot be empty".to_owned());
    }
    if request.workload.is_empty() {
        return Err("user repro request workload cannot be empty".to_owned());
    }
    Ok(())
}

fn compose_replay_sql(schema: &[String], workload: &[String]) -> Vec<String> {
    schema
        .iter()
        .chain(workload.iter())
        .map(|sql| sql.trim().to_owned())
        .filter(|sql| !sql.is_empty())
        .collect()
}

fn deterministic_user_repro_fixture_id(
    request: &UserReproIntakeRequest,
    signature_hash: &str,
) -> String {
    let canonical = serde_json::json!({
        "trace_id": request.trace_id,
        "run_id": request.run_id,
        "scenario_id": request.scenario_id,
        "seed": request.seed,
        "schema": request.schema,
        "workload": request.workload,
        "signature_hash": signature_hash,
    });
    let digest = sha256_hex(canonical.to_string().as_bytes());
    format!("user-repro-{}", &digest[..16])
}

// ─── Coverage Computation ────────────────────────────────────────────────

fn compute_coverage(
    entries: &[CorpusEntry],
    taxonomy_features: &BTreeMap<String, Vec<String>>,
) -> CoverageReport {
    let active: Vec<_> = entries.iter().filter(|e| e.skip.is_none()).collect();
    let skipped = entries.len() - active.len();
    let mut by_source: BTreeMap<String, usize> = BTreeMap::new();
    let mut user_repro_entries = 0usize;
    let mut user_repro_minimized_statements = 0usize;

    for entry in &active {
        let source_name = source_name(&entry.source).to_owned();
        *by_source.entry(source_name).or_insert(0) += 1;
        if let CorpusSource::UserRepro {
            minimized_statement_count,
            ..
        } = &entry.source
        {
            user_repro_entries += 1;
            user_repro_minimized_statements += *minimized_statement_count;
        }
    }

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
                .is_none_or(|c| c.entry_count == 0)
        })
        .map(std::string::ToString::to_string)
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
        by_source,
        user_repro_entries,
        user_repro_minimized_statements,
        missing_families: missing,
        underrepresented_families: underrepresented,
    }
}

fn source_name(source: &CorpusSource) -> &'static str {
    match source {
        CorpusSource::Slt { .. } => "slt",
        CorpusSource::Tcl { .. } => "tcl",
        CorpusSource::Fixture { .. } => "fixture",
        CorpusSource::Custom { .. } => "custom",
        CorpusSource::Generated { .. } => "generated",
        CorpusSource::UserRepro { .. } => "user_repro",
    }
}

// ─── Conformance Fixture Ingestion ───────────────────────────────────────

/// Why a fixture JSON file did not produce corpus entries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FixtureSkipDetail {
    /// Fixture file name.
    pub file: String,
    /// Human-readable reason for the skip.
    pub reason: String,
}

/// Summary report for conformance fixture ingestion.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FixtureIngestReport {
    /// Number of fixture JSON files discovered in the directory.
    pub fixture_json_files_seen: usize,
    /// Number of fixture files that produced at least one corpus entry.
    pub fixture_entries_ingested: usize,
    /// Total SQL statements extracted from fixture files.
    pub sql_statements_ingested: usize,
    /// Fixture files that were skipped and why.
    pub skipped_files: Vec<FixtureSkipDetail>,
}

/// Ingest conformance fixture JSON files from a directory into corpus entries.
///
/// # Errors
///
/// Returns an error if a fixture file cannot be read or parsed.
pub fn ingest_conformance_fixtures_with_report(
    dir: &Path,
    builder: &mut CorpusBuilder,
) -> Result<FixtureIngestReport, String> {
    let mut report = FixtureIngestReport::default();
    let entries = std::fs::read_dir(dir)
        .map_err(|e| format!("failed to read conformance dir {}: {e}", dir.display()))?;

    let mut files: Vec<_> = entries
        .filter_map(std::result::Result::ok)
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "json"))
        .collect();
    files.sort_by_key(std::fs::DirEntry::path);
    report.fixture_json_files_seen = files.len();

    for entry in files {
        let path = entry.path();
        let file_name = path
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();
        let content = std::fs::read_to_string(&path)
            .map_err(|e| format!("failed to read {}: {e}", path.display()))?;

        let fixture: serde_json::Value = serde_json::from_str(&content)
            .map_err(|e| format!("failed to parse {}: {e}", path.display()))?;

        let ops = fixture.get("ops").and_then(serde_json::Value::as_array);
        let statements: Vec<String> = ops
            .map_or(&[][..], Vec::as_slice)
            .iter()
            .filter_map(|op| op.get("sql").and_then(serde_json::Value::as_str))
            .map(str::to_owned)
            .collect();

        if statements.is_empty() {
            let reason = if ops.is_none() {
                "missing ops array".to_owned()
            } else {
                "no ops[].sql statements found".to_owned()
            };
            report.skipped_files.push(FixtureSkipDetail {
                file: file_name,
                reason,
            });
            continue;
        }

        let fixture_id = fixture["id"].as_str().unwrap_or("unknown").to_owned();
        let description = fixture["description"].as_str().unwrap_or("").to_owned();
        report.sql_statements_ingested += statements.len();

        builder.add_statements(
            statements,
            CorpusSource::Fixture { file: file_name },
            format!("{fixture_id}: {description}"),
        );
        report.fixture_entries_ingested += 1;
    }

    Ok(report)
}

/// Ingest conformance fixture JSON files and return number of ingested fixtures.
///
/// # Errors
///
/// Returns an error if a fixture file cannot be read or parsed.
pub fn ingest_conformance_fixtures(
    dir: &Path,
    builder: &mut CorpusBuilder,
) -> Result<usize, String> {
    let report = ingest_conformance_fixtures_with_report(dir, builder)?;
    Ok(report.fixture_entries_ingested)
}

// ─── SQLLogicTest Ingestion ─────────────────────────────────────────────

/// Why an SLT file did not produce corpus entries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SltSkipDetail {
    /// SLT file name.
    pub file: String,
    /// Human-readable reason for the skip.
    pub reason: String,
}

/// Summary report for SQLLogicTest ingestion.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SltIngestReport {
    /// Number of `.slt`-style files discovered.
    pub slt_files_seen: usize,
    /// Number of parsed SLT entries that produced SQL statements.
    pub slt_entries_ingested: usize,
    /// Total SQL statements extracted from SLT entries.
    pub sql_statements_ingested: usize,
    /// SLT files that were skipped and why.
    pub skipped_files: Vec<SltSkipDetail>,
}

/// Ingest SQLLogicTest (`.slt`/`.sqllogictest`/`.test`) files into corpus entries.
///
/// # Errors
///
/// Returns an error if an SLT file cannot be read.
pub fn ingest_slt_files_with_report(
    dir: &Path,
    builder: &mut CorpusBuilder,
) -> Result<SltIngestReport, String> {
    let mut report = SltIngestReport::default();
    let files = collect_slt_suite_files(dir)?;
    report.slt_files_seen = files.len();

    for path in files {
        let file_name = path
            .strip_prefix(dir)
            .unwrap_or(path.as_path())
            .to_string_lossy()
            .to_string();
        let content = std::fs::read_to_string(&path)
            .map_err(|error| format!("failed to read {}: {error}", path.display()))?;

        let parsed_entries = parse_slt(&content);
        if parsed_entries.is_empty() {
            report.skipped_files.push(SltSkipDetail {
                file: file_name,
                reason: "no SLT entries parsed".to_owned(),
            });
            continue;
        }

        let mut statements = Vec::new();
        let mut ingestable_entries = 0_usize;
        for parsed in parsed_entries {
            if matches!(parsed.kind, SltKind::Statement | SltKind::Query) {
                let normalized = normalize_slt_sql(&parsed.sql);
                if !normalized.is_empty() {
                    statements.push(normalized);
                    ingestable_entries += 1;
                }
            }
        }

        if statements.is_empty() {
            report.skipped_files.push(SltSkipDetail {
                file: file_name,
                reason: "parsed SLT entries contained no SQL statements".to_owned(),
            });
            continue;
        }

        report.slt_entries_ingested += ingestable_entries;
        report.sql_statements_ingested += statements.len();
        let statement_count = statements.len();

        builder.add_statements(
            statements,
            CorpusSource::Slt {
                file: file_name.clone(),
            },
            format!("SLT file {file_name} ({statement_count} statements)"),
        );
    }

    Ok(report)
}

/// Ingest SQLLogicTest files and return number of SQL statements ingested.
///
/// # Errors
///
/// Returns an error if an SLT file cannot be read.
pub fn ingest_slt_files(dir: &Path, builder: &mut CorpusBuilder) -> Result<usize, String> {
    let report = ingest_slt_files_with_report(dir, builder)?;
    Ok(report.sql_statements_ingested)
}

fn normalize_slt_sql(sql: &str) -> String {
    sql.lines()
        .map(str::trim_end)
        .collect::<Vec<_>>()
        .join("\n")
        .trim()
        .to_owned()
}

fn collect_slt_suite_files(dir: &Path) -> Result<Vec<PathBuf>, String> {
    let mut files = Vec::new();
    collect_slt_suite_files_recursive(dir, &mut files)?;
    files.sort();
    Ok(files)
}

fn collect_slt_suite_files_recursive(dir: &Path, files: &mut Vec<PathBuf>) -> Result<(), String> {
    let entries = std::fs::read_dir(dir)
        .map_err(|error| format!("failed to read slt dir {}: {error}", dir.display()))?;
    let mut children = Vec::new();
    for entry_result in entries {
        let entry = entry_result
            .map_err(|error| format!("failed to read slt dir entry {}: {error}", dir.display()))?;
        children.push(entry);
    }
    children.sort_by_key(std::fs::DirEntry::path);

    for entry in children {
        let file_type = entry
            .file_type()
            .map_err(|error| format!("failed to inspect {}: {error}", entry.path().display()))?;
        let path = entry.path();
        if file_type.is_dir() {
            collect_slt_suite_files_recursive(&path, files)?;
            continue;
        }
        if file_type.is_file() && is_supported_slt_file(&path) {
            files.push(path);
        }
    }

    Ok(())
}

fn is_supported_slt_file(path: &Path) -> bool {
    path.extension()
        .and_then(std::ffi::OsStr::to_str)
        .is_some_and(|ext| {
            ext.eq_ignore_ascii_case("slt")
                || ext.eq_ignore_ascii_case("sqllogictest")
                || ext.eq_ignore_ascii_case("test")
        })
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
