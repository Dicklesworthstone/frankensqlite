//! Metamorphic differential runner with deterministic mismatch reduction
//! (bd-mblr.7.1.2).
//!
//! Integrates three existing pipelines into a single execution flow:
//!
//! 1. **Metamorphic grammar** ([`crate::metamorphic`]) generates semantically
//!    equivalent SQL rewrites from corpus entries.
//! 2. **Differential V2 harness** ([`crate::differential_v2`]) executes both
//!    original and transformed variants against FrankenSQLite and C SQLite,
//!    reporting per-statement divergences.
//! 3. **Mismatch minimizer** ([`crate::mismatch_minimizer`]) reduces divergent
//!    workloads to minimal reproductions, computes canonical signatures, and
//!    deduplicates failures.
//!
//! # Usage
//!
//! ```ignore
//! let config = RunConfig::default();
//! let report = run_metamorphic_differential(
//!     &corpus_entries,
//!     &config,
//!     || FsqliteExecutor::open_in_memory(),
//!     || CsqliteExecutor::open_in_memory(),
//! )?;
//! assert_eq!(report.diverged, 0, "no divergences");
//! ```
//!
//! # Determinism
//!
//! All operations are deterministic given the same corpus, seed, and executor
//! factories. The report's `data_hash` fingerprints the input corpus for
//! traceability.

use std::fmt::Write as _;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::corpus_ingest::CorpusEntry;
use crate::differential_v2::{
    CanonicalizationRules, DifferentialResult, ExecutionEnvelope, Outcome, PragmaConfig,
    SqlExecutor, StatementDivergence,
};
use crate::metamorphic::{
    EquivalenceExpectation, MetamorphicTestCase, MismatchClassification, TransformRegistry,
    generate_metamorphic_corpus,
};
use crate::mismatch_minimizer::{
    DeduplicatedFailures, MinimalReproduction, MinimizerConfig, attribute_subsystem, deduplicate,
    minimize_workload,
};

/// Bead identifier for log correlation.
#[allow(dead_code)]
const BEAD_ID: &str = "bd-mblr.7.1.2";
const DEFAULT_BASE_SEED: u64 = u64::from_be_bytes(*b"\0FRANKEN");

// ===========================================================================
// Configuration
// ===========================================================================

/// Configuration for a metamorphic differential run campaign.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunConfig {
    /// Base seed for deterministic RNG derivation.
    pub base_seed: u64,
    /// Maximum metamorphic test cases generated per corpus entry.
    pub max_cases_per_entry: usize,
    /// PRAGMA configuration applied to both engines.
    pub pragmas: PragmaConfig,
    /// Canonicalization rules for output comparison.
    pub canonicalization: CanonicalizationRules,
    /// Mismatch minimizer configuration.
    pub minimizer: MinimizerConfig,
    /// Whether to minimize divergent workloads (expensive but produces
    /// actionable reproductions).
    pub enable_minimization: bool,
    /// Maximum workload statements before skipping V2 envelope reduction
    /// (the mismatch_minimizer's own reduction is still attempted).
    pub max_envelope_reduction_size: usize,
}

impl Default for RunConfig {
    fn default() -> Self {
        Self {
            base_seed: DEFAULT_BASE_SEED,
            max_cases_per_entry: 8,
            pragmas: PragmaConfig::default(),
            canonicalization: CanonicalizationRules::default(),
            minimizer: MinimizerConfig::default(),
            enable_minimization: true,
            max_envelope_reduction_size: 500,
        }
    }
}

// ===========================================================================
// Per-Case Result
// ===========================================================================

/// Result for a single metamorphic test case.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CaseResult {
    /// Test case identifier.
    pub case_id: String,
    /// Transform name that generated this case.
    pub transform_name: String,
    /// Expected equivalence type.
    pub equivalence: EquivalenceExpectation,
    /// Whether the original SQL matched between engines.
    pub original_passed: bool,
    /// Whether the transformed SQL matched between engines.
    pub transformed_passed: bool,
    /// Mismatch classification (only set when a divergence is detected).
    pub classification: Option<MismatchClassification>,
    /// Minimal reproduction (only set when minimization succeeds).
    pub minimal_reproduction: Option<MinimalReproduction>,
    /// Divergence variant: which comparison failed.
    pub divergence_source: Option<DivergenceSource>,
}

/// Which comparison produced the divergence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DivergenceSource {
    /// The original SQL diverged between engines (a real parity bug).
    Original,
    /// The transformed SQL diverged between engines.
    Transformed,
    /// Original and transformed agreed internally but their results differ
    /// (the transform is not semantics-preserving for this input).
    CrossVariant,
}

// ===========================================================================
// Run Report
// ===========================================================================

/// Structured evidence report from a metamorphic differential run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DifferentialRunReport {
    /// Bead identifier.
    pub bead_id: String,
    /// SHA-256 fingerprint of the input corpus for traceability.
    pub data_hash: String,
    /// Base seed used.
    pub base_seed: u64,
    /// Total metamorphic test cases generated.
    pub total_cases: usize,
    /// Cases that passed (both variants matched).
    pub passed: usize,
    /// Cases that diverged.
    pub diverged: usize,
    /// Cases skipped (generation produced no transformable cases).
    pub skipped: usize,
    /// Per-case results (only divergent cases included to save space).
    pub divergent_cases: Vec<CaseResult>,
    /// Deduplicated failure signatures.
    pub deduplicated: DeduplicatedFailures,
    /// Coverage summary by transform family and equivalence type.
    pub coverage_summary: CoverageSummary,
}

/// Summary of which transforms and equivalence types were exercised.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CoverageSummary {
    /// Count of cases per transform name.
    pub by_transform: Vec<(String, usize)>,
    /// Count of cases per equivalence expectation.
    pub by_equivalence: Vec<(String, usize)>,
    /// Count of divergences per transform name.
    pub divergences_by_transform: Vec<(String, usize)>,
}

// ===========================================================================
// Runner
// ===========================================================================

/// Run the full metamorphic differential pipeline.
///
/// For each corpus entry, generates metamorphic variants, executes both
/// original and transformed SQL against the two engines, and minimizes
/// any divergences into canonical signatures.
///
/// # Errors
///
/// Returns `Err` if executor construction fails.
#[allow(clippy::too_many_lines)]
pub fn run_metamorphic_differential<FFactory, CFactory, F, C>(
    entries: &[CorpusEntry],
    config: &RunConfig,
    make_fsqlite: FFactory,
    make_reference_sqlite: CFactory,
) -> Result<DifferentialRunReport, String>
where
    FFactory: Fn() -> Result<F, String> + Clone + 'static,
    CFactory: Fn() -> Result<C, String> + Clone + 'static,
    F: SqlExecutor,
    C: SqlExecutor,
{
    let registry = TransformRegistry::new();
    let cases = generate_metamorphic_corpus(
        entries,
        &registry,
        config.base_seed,
        config.max_cases_per_entry,
    );

    let data_hash = compute_corpus_hash(entries);

    let mut passed = 0usize;
    let mut diverged = 0usize;
    let mut divergent_cases = Vec::new();
    let mut all_reproductions: Vec<MinimalReproduction> = Vec::new();

    // Coverage tracking.
    let mut transform_counts: std::collections::BTreeMap<String, usize> =
        std::collections::BTreeMap::new();
    let mut equivalence_counts: std::collections::BTreeMap<String, usize> =
        std::collections::BTreeMap::new();
    let mut divergence_transform_counts: std::collections::BTreeMap<String, usize> =
        std::collections::BTreeMap::new();

    for case in &cases {
        *transform_counts
            .entry(case.transform_name.clone())
            .or_insert(0) += 1;
        *equivalence_counts
            .entry(case.equivalence.to_string())
            .or_insert(0) += 1;

        match run_single_case(
            case,
            config,
            make_fsqlite.clone(),
            make_reference_sqlite.clone(),
        )? {
            SingleCaseOutcome::Passed => {
                passed += 1;
            }
            SingleCaseOutcome::Diverged(result) => {
                diverged += 1;
                *divergence_transform_counts
                    .entry(case.transform_name.clone())
                    .or_insert(0) += 1;
                if let Some(ref repro) = result.minimal_reproduction {
                    all_reproductions.push(repro.clone());
                }
                divergent_cases.push(*result);
            }
        }
    }

    let deduplicated = deduplicate(&all_reproductions);

    let coverage_summary = CoverageSummary {
        by_transform: transform_counts.into_iter().collect(),
        by_equivalence: equivalence_counts.into_iter().collect(),
        divergences_by_transform: divergence_transform_counts.into_iter().collect(),
    };

    Ok(DifferentialRunReport {
        bead_id: BEAD_ID.to_owned(),
        data_hash,
        base_seed: config.base_seed,
        total_cases: cases.len(),
        passed,
        diverged,
        skipped: 0,
        divergent_cases,
        deduplicated,
        coverage_summary,
    })
}

// ===========================================================================
// Single-Case Execution
// ===========================================================================

enum SingleCaseOutcome {
    Passed,
    Diverged(Box<CaseResult>),
}

/// Execute a single metamorphic test case against both engines.
#[allow(clippy::similar_names)]
fn run_single_case<FFactory, CFactory, F, C>(
    case: &MetamorphicTestCase,
    config: &RunConfig,
    make_fsqlite: FFactory,
    make_csqlite: CFactory,
) -> Result<SingleCaseOutcome, String>
where
    FFactory: Fn() -> Result<F, String> + Clone + 'static,
    CFactory: Fn() -> Result<C, String> + Clone + 'static,
    F: SqlExecutor,
    C: SqlExecutor,
{
    // Separate schema statements (CREATE/INSERT) from query workload.
    let (schema, workload) = partition_schema_and_workload(&case.original);
    let (tx_schema, tx_workload) = partition_schema_and_workload(&case.transformed);

    // --- Run original variant ---
    let original_envelope = build_envelope(
        &schema,
        &workload,
        case.seed,
        &config.pragmas,
        &config.canonicalization,
    );
    let original_result = {
        let f = make_fsqlite()?;
        let c = make_csqlite()?;
        crate::differential_v2::run_differential(&original_envelope, &f, &c)
    };

    let original_passed = !has_divergence(&original_result);

    // --- Run transformed variant ---
    let transformed_envelope = build_envelope(
        &tx_schema,
        &tx_workload,
        case.seed,
        &config.pragmas,
        &config.canonicalization,
    );
    let transformed_result = {
        let f = make_fsqlite()?;
        let c = make_csqlite()?;
        crate::differential_v2::run_differential(&transformed_envelope, &f, &c)
    };

    let transformed_passed = !has_divergence(&transformed_result);

    // --- Determine divergence ---
    if original_passed && transformed_passed {
        return Ok(SingleCaseOutcome::Passed);
    }

    let (divergence_source, failing_envelope, failing_result) = if original_passed {
        (
            DivergenceSource::Transformed,
            &transformed_envelope,
            &transformed_result,
        )
    } else {
        (
            DivergenceSource::Original,
            &original_envelope,
            &original_result,
        )
    };

    // Classify the mismatch using metamorphic classification.
    let classification = classify_divergence(failing_result, case);

    // Minimize if enabled and workload is small enough.
    let minimal_reproduction = if config.enable_minimization
        && failing_envelope.workload.len() <= config.max_envelope_reduction_size
    {
        try_minimize(
            failing_envelope,
            &classification,
            case.seed,
            config,
            make_fsqlite,
            make_csqlite,
        )
    } else {
        None
    };

    Ok(SingleCaseOutcome::Diverged(Box::new(CaseResult {
        case_id: case.id.clone(),
        transform_name: case.transform_name.clone(),
        equivalence: case.equivalence,
        original_passed,
        transformed_passed,
        classification: Some(classification),
        minimal_reproduction,
        divergence_source: Some(divergence_source),
    })))
}

// ===========================================================================
// Minimization Integration
// ===========================================================================

/// Attempt to minimize a failing workload and extract a canonical signature.
fn try_minimize<FFactory, CFactory, F, C>(
    envelope: &ExecutionEnvelope,
    classification: &MismatchClassification,
    seed: u64,
    config: &RunConfig,
    make_fsqlite: FFactory,
    make_reference_sqlite: CFactory,
) -> Option<MinimalReproduction>
where
    FFactory: Fn() -> Result<F, String> + Clone + 'static,
    CFactory: Fn() -> Result<C, String> + Clone + 'static,
    F: SqlExecutor,
    C: SqlExecutor,
{
    let pragmas = config.pragmas.clone();
    let canonicalization = config.canonicalization.clone();

    // Use the mismatch_minimizer's delta-debugging.
    let test_fn = move |schema: &[String],
                        workload: &[String]|
          -> Option<Vec<StatementDivergence>> {
        let probe_envelope = build_envelope(schema, workload, seed, &pragmas, &canonicalization);
        let f = make_fsqlite().ok()?;
        let c = make_reference_sqlite().ok()?;
        let result = crate::differential_v2::run_differential(&probe_envelope, &f, &c);
        if has_divergence(&result) {
            Some(result.divergences)
        } else {
            None
        }
    };

    let mut repro = minimize_workload(
        &envelope.schema,
        &envelope.workload,
        &config.minimizer,
        &test_fn,
    );

    // Enrich with seed and override classification if metamorphic analysis
    // produced a more specific classification.
    if let Some(ref mut r) = repro {
        r.original_seed = seed;
        if !matches!(
            classification,
            MismatchClassification::TrueDivergence { .. }
        ) {
            // Re-compute signature with the metamorphic classification.
            let subsystem = attribute_subsystem(&r.divergences, &r.schema, &r.minimal_workload);
            r.signature = crate::mismatch_minimizer::MismatchSignature::compute(
                &r.schema,
                &r.minimal_workload,
                classification,
                subsystem,
                r.divergences.first(),
            );
        }
    }

    repro
}

// ===========================================================================
// Helpers
// ===========================================================================

/// Build an execution envelope from schema, workload, and config.
fn build_envelope(
    schema: &[String],
    workload: &[String],
    seed: u64,
    pragmas: &PragmaConfig,
    canonicalization: &CanonicalizationRules,
) -> ExecutionEnvelope {
    ExecutionEnvelope::builder(seed)
        .pragmas(pragmas.clone())
        .schema(schema.to_vec())
        .workload(workload.to_vec())
        .canonicalization(canonicalization.clone())
        .build()
}

/// Partition SQL statements into schema (DDL/DML setup) and workload (queries).
///
/// Schema statements are CREATE TABLE/INDEX/VIEW, INSERT (for setup data),
/// and any PRAGMA. Everything else is workload.
fn partition_schema_and_workload(statements: &[String]) -> (Vec<String>, Vec<String>) {
    let mut schema = Vec::new();
    let mut workload = Vec::new();

    for stmt in statements {
        let upper = stmt.trim().to_uppercase();
        if upper.starts_with("CREATE ")
            || upper.starts_with("INSERT ")
            || upper.starts_with("PRAGMA ")
        {
            schema.push(stmt.clone());
        } else {
            workload.push(stmt.clone());
        }
    }

    // If everything is schema (e.g., INSERT-only test), treat all as workload
    // so the differential runner can compare outcomes.
    if workload.is_empty() && !schema.is_empty() {
        workload = schema;
        schema = Vec::new();
    }

    (schema, workload)
}

/// Classify a divergence using the metamorphic mismatch classification.
fn classify_divergence(
    result: &DifferentialResult,
    case: &MetamorphicTestCase,
) -> MismatchClassification {
    if result.divergences.is_empty() && !result.logical_state_matched {
        return MismatchClassification::TrueDivergence {
            description: format!(
                "logical state hash mismatch (transform: {})",
                case.transform_name
            ),
        };
    }

    for div in &result.divergences {
        // Use the metamorphic classify_mismatch if we have row data.
        let classified = classify_from_divergence(div, case.equivalence);
        if !matches!(classified, MismatchClassification::FalsePositive { .. }) {
            return classified;
        }
    }

    MismatchClassification::TrueDivergence {
        description: format!(
            "{} statement(s) diverged (transform: {})",
            result.statements_mismatched, case.transform_name
        ),
    }
}

/// Classify a single statement divergence using metamorphic rules.
fn classify_from_divergence(
    div: &StatementDivergence,
    equivalence: EquivalenceExpectation,
) -> MismatchClassification {
    use crate::differential_v2::StmtOutcome;

    match (&div.fsqlite_outcome, &div.csqlite_outcome) {
        (StmtOutcome::Error(a), StmtOutcome::Error(b)) => {
            // Both errored but with different messages — usually not actionable.
            MismatchClassification::FalsePositive {
                reason: format!("both errored: fsqlite={a}, csqlite={b}"),
            }
        }
        (StmtOutcome::Rows(f_rows), StmtOutcome::Rows(c_rows)) => {
            // Check if this is an order-dependent difference.
            if f_rows.len() == c_rows.len() {
                let mut f_sorted = f_rows.clone();
                let mut c_sorted = c_rows.clone();
                let key = |row: &[crate::differential_v2::NormalizedValue]| -> String {
                    row.iter()
                        .map(|v| format!("{v}"))
                        .collect::<Vec<_>>()
                        .join("|")
                };
                f_sorted.sort_by_key(|r| key(r));
                c_sorted.sort_by_key(|r| key(r));

                if f_sorted == c_sorted {
                    return match equivalence {
                        EquivalenceExpectation::ExactRowMatch => {
                            MismatchClassification::OrderDependentDifference
                        }
                        _ => MismatchClassification::FalsePositive {
                            reason: "multiset-equivalent under relaxed equivalence".to_owned(),
                        },
                    };
                }
            }
            MismatchClassification::TrueDivergence {
                description: format!("row content mismatch: {}", div.sql),
            }
        }
        _ => MismatchClassification::TrueDivergence {
            description: format!("outcome type mismatch: {}", div.sql),
        },
    }
}

/// Check whether a differential result has any divergence.
fn has_divergence(result: &DifferentialResult) -> bool {
    matches!(result.outcome, Outcome::Divergence | Outcome::Error)
        || result.statements_mismatched > 0
        || !result.logical_state_matched
}

/// Compute a SHA-256 fingerprint of the corpus entries for traceability.
fn compute_corpus_hash(entries: &[CorpusEntry]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"corpus-v1:");
    for entry in entries {
        hasher.update(entry.id.as_bytes());
        hasher.update(b":");
        for stmt in &entry.statements {
            hasher.update(stmt.as_bytes());
            hasher.update(b"\n");
        }
        hasher.update(b"---\n");
    }
    let digest = hasher.finalize();
    let mut hex = String::with_capacity(64);
    for byte in digest {
        let _ = write!(hex, "{byte:02x}");
    }
    hex
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::corpus_ingest::{CorpusSource, Family};

    /// Build a minimal corpus entry for testing.
    fn make_entry(id: &str, statements: Vec<&str>) -> CorpusEntry {
        CorpusEntry {
            id: id.to_owned(),
            family: Family::SQL,
            secondary_families: Vec::new(),
            source: CorpusSource::Custom {
                author: "test".to_owned(),
            },
            statements: statements.into_iter().map(String::from).collect(),
            seed: 42,
            skip: None,
            taxonomy_features: Vec::new(),
            description: String::new(),
        }
    }

    /// Stub executor that records/returns canned data.
    #[derive(Clone)]
    struct StubExecutor {
        results: std::collections::HashMap<String, crate::differential_v2::StmtOutcome>,
    }

    impl StubExecutor {
        fn new() -> Self {
            Self {
                results: std::collections::HashMap::new(),
            }
        }

        fn with_default() -> Self {
            Self::new()
        }
    }

    impl SqlExecutor for StubExecutor {
        fn execute(&self, sql: &str) -> Result<usize, String> {
            if let Some(crate::differential_v2::StmtOutcome::Execute(n)) =
                self.results.get(sql.trim())
            {
                Ok(*n)
            } else {
                Ok(0)
            }
        }

        fn query(
            &self,
            sql: &str,
        ) -> Result<Vec<Vec<crate::differential_v2::NormalizedValue>>, String> {
            if let Some(crate::differential_v2::StmtOutcome::Rows(rows)) =
                self.results.get(sql.trim())
            {
                Ok(rows.clone())
            } else {
                Ok(Vec::new())
            }
        }

        fn engine_identity(&self) -> crate::differential_v2::EngineIdentity {
            crate::differential_v2::EngineIdentity::CSqliteOracle
        }
    }

    #[test]
    fn test_run_config_default() {
        let config = RunConfig::default();
        assert_eq!(config.base_seed, DEFAULT_BASE_SEED);
        assert_eq!(config.max_cases_per_entry, 8);
        assert!(config.enable_minimization);
    }

    #[test]
    fn test_partition_schema_and_workload() {
        let stmts = vec![
            "CREATE TABLE t(a INTEGER, b TEXT)".to_owned(),
            "INSERT INTO t VALUES(1, 'hello')".to_owned(),
            "SELECT * FROM t".to_owned(),
            "SELECT a + 1 FROM t WHERE b = 'hello'".to_owned(),
        ];
        let (schema, workload) = partition_schema_and_workload(&stmts);
        assert_eq!(schema.len(), 2);
        assert_eq!(workload.len(), 2);
        assert!(schema[0].starts_with("CREATE"));
        assert!(schema[1].starts_with("INSERT"));
        assert!(workload[0].starts_with("SELECT"));
    }

    #[test]
    fn test_partition_all_schema_becomes_workload() {
        let stmts = vec![
            "INSERT INTO t VALUES(1)".to_owned(),
            "INSERT INTO t VALUES(2)".to_owned(),
        ];
        let (schema, workload) = partition_schema_and_workload(&stmts);
        assert!(schema.is_empty());
        assert_eq!(workload.len(), 2);
    }

    #[test]
    fn test_compute_corpus_hash_deterministic() {
        let entries = vec![make_entry("e1", vec!["SELECT 1"])];
        let h1 = compute_corpus_hash(&entries);
        let h2 = compute_corpus_hash(&entries);
        assert_eq!(h1, h2);
    }

    #[test]
    fn test_compute_corpus_hash_varies_with_content() {
        let e1 = vec![make_entry("e1", vec!["SELECT 1"])];
        let e2 = vec![make_entry("e1", vec!["SELECT 2"])];
        assert_ne!(compute_corpus_hash(&e1), compute_corpus_hash(&e2));
    }

    #[test]
    fn test_run_metamorphic_differential_empty_corpus() {
        let config = RunConfig::default();
        let report = run_metamorphic_differential(
            &[],
            &config,
            || Ok(StubExecutor::with_default()),
            || Ok(StubExecutor::with_default()),
        )
        .expect("empty corpus should succeed");

        assert_eq!(report.total_cases, 0);
        assert_eq!(report.passed, 0);
        assert_eq!(report.diverged, 0);
    }

    #[test]
    fn test_run_with_matching_stubs() {
        // Both engines return identical results → all pass.
        let entries = vec![make_entry(
            "basic",
            vec![
                "CREATE TABLE t(a INTEGER, b TEXT)",
                "INSERT INTO t VALUES(1, 'hello')",
                "SELECT * FROM t",
            ],
        )];
        let config = RunConfig {
            max_cases_per_entry: 2,
            enable_minimization: false,
            ..RunConfig::default()
        };

        let report = run_metamorphic_differential(
            &entries,
            &config,
            || Ok(StubExecutor::with_default()),
            || Ok(StubExecutor::with_default()),
        )
        .expect("matching stubs should succeed");

        // With matching stub executors, no divergences.
        assert_eq!(report.diverged, 0);
        assert!(report.divergent_cases.is_empty());
    }

    #[test]
    fn test_classify_from_divergence_order_difference() {
        use crate::differential_v2::NormalizedValue;

        let div = StatementDivergence {
            index: 0,
            sql: "SELECT * FROM t".to_owned(),
            fsqlite_outcome: crate::differential_v2::StmtOutcome::Rows(vec![
                vec![NormalizedValue::Integer(2)],
                vec![NormalizedValue::Integer(1)],
            ]),
            csqlite_outcome: crate::differential_v2::StmtOutcome::Rows(vec![
                vec![NormalizedValue::Integer(1)],
                vec![NormalizedValue::Integer(2)],
            ]),
        };

        let classified = classify_from_divergence(&div, EquivalenceExpectation::ExactRowMatch);
        assert!(matches!(
            classified,
            MismatchClassification::OrderDependentDifference
        ));

        // With multiset equivalence, this should be a false positive.
        let classified_multiset =
            classify_from_divergence(&div, EquivalenceExpectation::MultisetEquivalence);
        assert!(matches!(
            classified_multiset,
            MismatchClassification::FalsePositive { .. }
        ));
    }

    #[test]
    fn test_classify_from_divergence_true_divergence() {
        use crate::differential_v2::NormalizedValue;

        let div = StatementDivergence {
            index: 0,
            sql: "SELECT count(*) FROM t".to_owned(),
            fsqlite_outcome: crate::differential_v2::StmtOutcome::Rows(vec![vec![
                NormalizedValue::Integer(5),
            ]]),
            csqlite_outcome: crate::differential_v2::StmtOutcome::Rows(vec![vec![
                NormalizedValue::Integer(3),
            ]]),
        };

        let classified = classify_from_divergence(&div, EquivalenceExpectation::ExactRowMatch);
        assert!(matches!(
            classified,
            MismatchClassification::TrueDivergence { .. }
        ));
    }

    #[test]
    fn test_classify_from_divergence_both_errors() {
        let div = StatementDivergence {
            index: 0,
            sql: "SELECT bad_func()".to_owned(),
            fsqlite_outcome: crate::differential_v2::StmtOutcome::Error(
                "no such function".to_owned(),
            ),
            csqlite_outcome: crate::differential_v2::StmtOutcome::Error(
                "no such function: bad_func".to_owned(),
            ),
        };

        let classified = classify_from_divergence(&div, EquivalenceExpectation::ExactRowMatch);
        assert!(matches!(
            classified,
            MismatchClassification::FalsePositive { .. }
        ));
    }

    #[test]
    fn test_divergence_source_variants() {
        // Ensure all variants are representable and serializable.
        let sources = [
            DivergenceSource::Original,
            DivergenceSource::Transformed,
            DivergenceSource::CrossVariant,
        ];
        for source in &sources {
            let json = serde_json::to_string(source).expect("serialize");
            let _: DivergenceSource = serde_json::from_str(&json).expect("deserialize");
        }
    }

    #[test]
    fn test_coverage_summary_structure() {
        let summary = CoverageSummary::default();
        assert!(summary.by_transform.is_empty());
        assert!(summary.by_equivalence.is_empty());
        assert!(summary.divergences_by_transform.is_empty());
    }

    #[test]
    fn test_build_envelope_roundtrip() {
        let schema = vec!["CREATE TABLE t(a INT)".to_owned()];
        let workload = vec!["SELECT * FROM t".to_owned()];
        let envelope = build_envelope(
            &schema,
            &workload,
            42,
            &PragmaConfig::default(),
            &CanonicalizationRules::default(),
        );
        assert_eq!(envelope.seed, 42);
        assert_eq!(envelope.schema.len(), 1);
        assert_eq!(envelope.workload.len(), 1);
    }

    #[test]
    fn test_try_minimize_preserves_non_true_divergence_classification() {
        use crate::differential_v2::{NormalizedValue, StmtOutcome};

        let mut fsqlite = StubExecutor::new();
        fsqlite.results.insert(
            "SELECT 1".to_owned(),
            StmtOutcome::Rows(vec![vec![NormalizedValue::Integer(1)]]),
        );

        let mut csqlite = StubExecutor::new();
        csqlite.results.insert(
            "SELECT 1".to_owned(),
            StmtOutcome::Rows(vec![vec![NormalizedValue::Integer(2)]]),
        );

        let config = RunConfig {
            minimizer: crate::mismatch_minimizer::MinimizerConfig {
                max_iterations: 16,
                one_minimal: true,
                max_workload_size: 16,
            },
            ..RunConfig::default()
        };

        let envelope = build_envelope(
            &["CREATE TABLE t(a INTEGER)".to_owned()],
            &["SELECT 1".to_owned()],
            42,
            &config.pragmas,
            &config.canonicalization,
        );

        let classification = MismatchClassification::OrderDependentDifference;
        let minimized = try_minimize(
            &envelope,
            &classification,
            42,
            &config,
            move || Ok(fsqlite.clone()),
            move || Ok(csqlite.clone()),
        )
        .expect("divergence should produce minimal reproduction");

        assert_eq!(minimized.signature.classification, classification);
    }
}
