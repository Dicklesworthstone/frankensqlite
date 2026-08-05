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

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::corpus_ingest::{CorpusEntry, CorpusSource, Family};
use crate::differential_v2::{
    CanonicalizationRules, DifferentialResult, ExecutionEnvelope, Outcome, PragmaConfig,
    ResultOrdering, SqlExecutor, StatementDivergence,
};
use crate::failure_bundle::{
    EnvironmentInfo, ExecutionLaneEvidence, FailureBundle, FailureBundleBuilder, FailureInfo,
    FailureType, FirstDivergence, ReproducibilityInfo, ScenarioInfo, TypedDifferentialEvidence,
    TypedEngineProvenance,
};
use crate::metamorphic::{
    EquivalenceExpectation, MetamorphicTestCase, MismatchClassification, TransformRegistry,
    generate_metamorphic_corpus,
};
use crate::mismatch_minimizer::{
    DeduplicatedFailures, MinimalReproduction, MinimizerConfig, TypedReductionConfig,
    TypedReductionObservation, TypedReductionResult, attribute_subsystem, deduplicate,
    minimize_typed_statements, minimize_workload,
};
use crate::test_inventory::ExecutionLane;
use crate::typed_sql_generator::{
    GENERATOR_SCHEMA_VERSION, GENERATOR_VERSION, GeneratedCase, GeneratedStatement,
    Statement as GeneratedAstStatement, StatementRole, derive_seed,
    validate_canonical_profile_evidence,
};

/// Bead identifier for log correlation.
#[allow(dead_code)]
const BEAD_ID: &str = "bd-mblr.7.1.2";
const DEFAULT_BASE_SEED: u64 = u64::from_be_bytes(*b"\0FRANKEN");
const PASSING_REPLAY_SAMPLE_LIMIT: usize = 3;
/// Stable schema for typed generator-to-differential adapter artifacts.
pub const TYPED_DIFFERENTIAL_SCHEMA_VERSION: &str = "fsqlite.typed-differential.v1";
/// Adapter implementation version included in replay evidence.
pub const TYPED_DIFFERENTIAL_ADAPTER_VERSION: &str = "1.0.0";
/// Fixed presubmit cases per profile required by the Phase-1 pilot.
pub const TYPED_PRESUBMIT_CASES_PER_PROFILE: u32 = 100;
/// Bounded nightly cases per profile.
pub const TYPED_NIGHTLY_CASES_PER_PROFILE: u32 = 10_000;

/// Campaign tier with a fixed, reviewable seed budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TypedCampaignTier {
    Presubmit,
    Nightly,
}

impl TypedCampaignTier {
    #[must_use]
    pub const fn default_case_count(self) -> u32 {
        match self {
            Self::Presubmit => TYPED_PRESUBMIT_CASES_PER_PROFILE,
            Self::Nightly => TYPED_NIGHTLY_CASES_PER_PROFILE,
        }
    }
}

/// Contiguous deterministic seed range before CI sharding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TypedSeedRange {
    pub tier: TypedCampaignTier,
    pub start: u64,
    pub count: u32,
}

impl TypedSeedRange {
    #[must_use]
    pub const fn for_tier(tier: TypedCampaignTier, start: u64) -> Self {
        Self {
            tier,
            start,
            count: tier.default_case_count(),
        }
    }

    /// Validate non-empty, non-overflowing range bounds.
    pub fn validate(self) -> Result<(), TypedAdapterError> {
        if self.count == 0 {
            return Err(TypedAdapterError::invalid(
                "seed_range.count",
                "seed range must contain at least one seed",
            ));
        }
        self.start
            .checked_add(u64::from(self.count - 1))
            .ok_or_else(|| TypedAdapterError::invalid("seed_range", "seed range overflows u64"))?;
        Ok(())
    }

    /// Materialize the range in stable ascending order.
    pub fn seeds(self) -> Result<Vec<u64>, TypedAdapterError> {
        self.validate()?;
        Ok((0..self.count)
            .map(|offset| self.start + u64::from(offset))
            .collect())
    }
}

/// Interleaved shard of one seed range. Shards are disjoint and their union is
/// exactly the source range.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TypedSeedShard {
    pub range: TypedSeedRange,
    pub shard_index: u32,
    pub shard_count: u32,
}

impl TypedSeedShard {
    pub fn validate(self) -> Result<(), TypedAdapterError> {
        self.range.validate()?;
        if self.shard_count == 0 || self.shard_index >= self.shard_count {
            return Err(TypedAdapterError::invalid(
                "seed_shard",
                format!(
                    "shard_index={} must be less than non-zero shard_count={}",
                    self.shard_index, self.shard_count
                ),
            ));
        }
        Ok(())
    }

    pub fn seeds(self) -> Result<Vec<u64>, TypedAdapterError> {
        self.validate()?;
        Ok((self.shard_index..self.range.count)
            .step_by(usize::try_from(self.shard_count).unwrap_or(usize::MAX))
            .map(|offset| self.range.start + u64::from(offset))
            .collect())
    }
}

/// Prove a complete shard set has neither gaps nor overlap.
pub fn validate_typed_seed_shards(
    range: TypedSeedRange,
    shards: &[TypedSeedShard],
) -> Result<(), TypedAdapterError> {
    range.validate()?;
    if shards.is_empty() {
        return Err(TypedAdapterError::invalid(
            "seed_shards",
            "at least one shard is required",
        ));
    }
    let expected_count = shards[0].shard_count;
    if usize::try_from(expected_count).ok() != Some(shards.len()) {
        return Err(TypedAdapterError::invalid(
            "seed_shards",
            "complete shard set must contain exactly shard_count entries",
        ));
    }
    let mut indexes = BTreeSet::new();
    let mut observed = BTreeSet::new();
    for shard in shards {
        shard.validate()?;
        if shard.range != range || shard.shard_count != expected_count {
            return Err(TypedAdapterError::invalid(
                "seed_shards",
                "all shards must reference the same range and shard count",
            ));
        }
        if !indexes.insert(shard.shard_index) {
            return Err(TypedAdapterError::invalid(
                "seed_shards",
                "duplicate shard index",
            ));
        }
        for seed in shard.seeds()? {
            if !observed.insert(seed) {
                return Err(TypedAdapterError::invalid(
                    "seed_shards",
                    "seed assigned to more than one shard",
                ));
            }
        }
    }
    let expected = range.seeds()?.into_iter().collect::<BTreeSet<_>>();
    if observed != expected {
        return Err(TypedAdapterError::invalid(
            "seed_shards",
            "shard union does not exactly cover the source range",
        ));
    }
    Ok(())
}

/// Stable seed derivation retained for every generated statement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TypedSeedLineage {
    pub ordinal: u32,
    pub seed_path: String,
    pub derived_seed: u64,
}

/// Canonical adapter artifact joining generator, comparator, replay, and
/// corpus provenance without introducing another case format.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TypedDifferentialCase {
    pub schema_version: String,
    pub adapter_version: String,
    pub case_id: String,
    pub run_id: String,
    pub trace_id: String,
    pub scenario_id: String,
    pub profile_sha256: String,
    pub feature_ids: Vec<String>,
    pub required_lanes: Vec<ExecutionLane>,
    pub ordering: Vec<ResultOrdering>,
    pub seed_lineage: Vec<TypedSeedLineage>,
    pub generated: GeneratedCase,
    pub envelope: ExecutionEnvelope,
    pub content_hash: String,
}

impl TypedDifferentialCase {
    #[must_use]
    pub fn deterministic_hash(&self) -> String {
        let mut canonical = self.clone();
        canonical.content_hash.clear();
        let encoded = serde_json::to_vec(&canonical)
            .expect("typed differential case serialization must succeed");
        sha256_hex(&encoded)
    }

    pub fn validate(&self) -> Result<(), TypedAdapterError> {
        if self.schema_version != TYPED_DIFFERENTIAL_SCHEMA_VERSION
            || self.adapter_version != TYPED_DIFFERENTIAL_ADAPTER_VERSION
        {
            return Err(TypedAdapterError::invalid(
                "typed_case.schema",
                "typed adapter schema/version mismatch",
            ));
        }
        let evidence = self
            .generated
            .canonical_profile_evidence
            .as_ref()
            .ok_or_else(|| {
                TypedAdapterError::unsupported(
                    "typed_case.profile",
                    "typed differential execution requires canonical profile evidence",
                )
            })?;
        validate_canonical_profile_evidence(evidence)
            .map_err(|error| TypedAdapterError::invalid(error.constraint, error.message))?;
        let projection = project_generated_case(&self.generated)?;
        if self.profile_sha256 != evidence.profile_sha256
            || self.required_lanes != evidence.required_lanes
            || self.case_id != projection.case_id
            || self.trace_id != self.generated.trace_hash
            || self.feature_ids != projection.feature_ids
            || self.ordering != projection.ordering
            || self.seed_lineage != projection.seed_lineage
            || self.envelope.schema != projection.schema
            || self.envelope.workload != projection.workload
            || self.envelope.seed != self.generated.root_seed
            || self.envelope.run_id.as_deref() != Some(self.run_id.as_str())
            || self.envelope.scenario_id != self.scenario_id
            || self.envelope.statement_ordering != self.ordering
        {
            return Err(TypedAdapterError::invalid(
                "typed_case.provenance",
                "generator, profile, lane, ordering, seed, or envelope provenance drifted",
            ));
        }
        let envelope_errors = self.envelope.validate_parity_contract();
        if !envelope_errors.is_empty() {
            return Err(TypedAdapterError::invalid(
                "typed_case.envelope",
                envelope_errors.join("; "),
            ));
        }
        if self.content_hash != self.deterministic_hash() {
            return Err(TypedAdapterError::invalid(
                "typed_case.content_hash",
                "typed case content hash mismatch",
            ));
        }
        Ok(())
    }

    pub fn to_canonical_json(&self) -> Result<String, TypedAdapterError> {
        self.validate()?;
        serde_json::to_string_pretty(self)
            .map_err(|error| TypedAdapterError::artifact("typed_case_json", error.to_string()))
    }

    pub fn from_json_strict(json: &str) -> Result<Self, TypedAdapterError> {
        let case: Self = serde_json::from_str(json).map_err(|error| {
            TypedAdapterError::artifact("typed_case_json", format!("decode failed: {error}"))
        })?;
        case.validate()?;
        Ok(case)
    }
}

/// Typed adapter error category; unsupported input, cancellation, timeout, and
/// artifact failures remain distinct from invalid generator evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TypedAdapterErrorKind {
    InvalidInput,
    Unsupported,
    Cancelled,
    Timeout,
    LaneViolation,
    Artifact,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TypedAdapterError {
    pub kind: TypedAdapterErrorKind,
    pub constraint: String,
    pub message: String,
}

impl TypedAdapterError {
    pub(crate) fn new(
        kind: TypedAdapterErrorKind,
        constraint: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            kind,
            constraint: constraint.into(),
            message: message.into(),
        }
    }

    pub(crate) fn invalid(constraint: impl Into<String>, message: impl Into<String>) -> Self {
        Self::new(TypedAdapterErrorKind::InvalidInput, constraint, message)
    }

    pub(crate) fn unsupported(constraint: impl Into<String>, message: impl Into<String>) -> Self {
        Self::new(TypedAdapterErrorKind::Unsupported, constraint, message)
    }

    pub(crate) fn artifact(constraint: impl Into<String>, message: impl Into<String>) -> Self {
        Self::new(TypedAdapterErrorKind::Artifact, constraint, message)
    }
}

fn typed_statement_ordering(statement: &GeneratedStatement) -> ResultOrdering {
    match &statement.ast {
        GeneratedAstStatement::Select { select } if select.order_by.is_empty() => {
            ResultOrdering::UnorderedMultiset
        }
        GeneratedAstStatement::Select { .. } => ResultOrdering::Ordered,
        _ => ResultOrdering::NotApplicable,
    }
}

struct TypedCaseProjection {
    case_id: String,
    schema: Vec<String>,
    workload: Vec<String>,
    ordering: Vec<ResultOrdering>,
    feature_ids: Vec<String>,
    seed_lineage: Vec<TypedSeedLineage>,
}

fn project_generated_case(
    generated: &GeneratedCase,
) -> Result<TypedCaseProjection, TypedAdapterError> {
    if generated.schema_version != GENERATOR_SCHEMA_VERSION
        || generated.generator_version != GENERATOR_VERSION
    {
        return Err(TypedAdapterError::unsupported(
            "typed_case.generator_version",
            "generated case schema/version is unsupported",
        ));
    }
    let evidence = generated
        .canonical_profile_evidence
        .as_ref()
        .ok_or_else(|| {
            TypedAdapterError::unsupported(
                "typed_case.profile",
                "typed differential execution requires canonical profile evidence",
            )
        })?;
    validate_canonical_profile_evidence(evidence)
        .map_err(|error| TypedAdapterError::invalid(error.constraint, error.message))?;
    if generated.profile_name != evidence.profile_name
        || generated.profile_version != evidence.profile_version
    {
        return Err(TypedAdapterError::invalid(
            "typed_case.profile_identity",
            "generated profile name/version does not match canonical evidence",
        ));
    }
    if generated.statements.is_empty() {
        return Err(TypedAdapterError::invalid(
            "typed_case.statements",
            "generated case contains no statements",
        ));
    }

    let sql_script = generated
        .statements
        .iter()
        .map(|statement| statement.sql.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    let trace_json = serde_json::to_string(&generated.trace).map_err(|error| {
        TypedAdapterError::artifact("typed_case.trace", format!("encode failed: {error}"))
    })?;
    let schema_json = serde_json::to_string(&generated.final_schema).map_err(|error| {
        TypedAdapterError::artifact("typed_case.schema", format!("encode failed: {error}"))
    })?;
    if generated.sql_hash != sha256_hex(sql_script.as_bytes())
        || generated.trace_hash != sha256_hex(trace_json.as_bytes())
        || generated.schema_hash != sha256_hex(schema_json.as_bytes())
    {
        return Err(TypedAdapterError::invalid(
            "typed_case.generator_hashes",
            "generated SQL, trace, or schema hash does not match its payload",
        ));
    }

    let mut schema = Vec::new();
    let mut workload = Vec::new();
    let mut ordering = Vec::with_capacity(generated.statements.len());
    let mut feature_ids = BTreeSet::new();
    let mut seed_lineage = Vec::with_capacity(generated.statements.len());
    let mut subject_seen = false;
    for statement in &generated.statements {
        if statement.sql != statement.ast.to_sql() {
            return Err(TypedAdapterError::invalid(
                "typed_case.statement_sql",
                format!(
                    "statement ordinal {} SQL does not match its typed AST",
                    statement.ordinal
                ),
            ));
        }
        if statement.seed_path.trim().is_empty()
            || statement.derived_seed != derive_seed(generated.root_seed, &statement.seed_path)
        {
            return Err(TypedAdapterError::invalid(
                "typed_case.seed_lineage",
                format!(
                    "statement ordinal {} has invalid seed lineage",
                    statement.ordinal
                ),
            ));
        }
        let feature_id = statement.profile_feature_id.as_deref().ok_or_else(|| {
            TypedAdapterError::invalid(
                "typed_case.feature_id",
                format!(
                    "statement ordinal {} lacks canonical feature lineage",
                    statement.ordinal
                ),
            )
        })?;
        if !evidence.bindings.iter().any(|binding| {
            binding.role == statement.role
                && binding.construct == statement.construct
                && binding.feature_id == feature_id
        }) {
            return Err(TypedAdapterError::invalid(
                "typed_case.feature_binding",
                format!(
                    "statement ordinal {} has no matching canonical feature binding",
                    statement.ordinal
                ),
            ));
        }
        feature_ids.insert(feature_id.to_owned());
        ordering.push(typed_statement_ordering(statement));
        seed_lineage.push(TypedSeedLineage {
            ordinal: statement.ordinal,
            seed_path: statement.seed_path.clone(),
            derived_seed: statement.derived_seed,
        });
        match statement.role {
            StatementRole::Setup if subject_seen => {
                return Err(TypedAdapterError::invalid(
                    "typed_case.statement_order",
                    "setup statement appeared after the first subject statement",
                ));
            }
            StatementRole::Setup => schema.push(statement.sql.clone()),
            StatementRole::Subject => {
                subject_seen = true;
                workload.push(statement.sql.clone());
            }
        }
    }
    if workload.is_empty() {
        return Err(TypedAdapterError::invalid(
            "typed_case.workload",
            "canonical case must contain at least one subject statement",
        ));
    }
    let sql_hash_prefix = generated.sql_hash.get(..12).ok_or_else(|| {
        TypedAdapterError::invalid(
            "typed_case.sql_hash",
            "generated SQL hash is shorter than 12 bytes",
        )
    })?;
    Ok(TypedCaseProjection {
        case_id: format!(
            "typed-{}-{:016x}-{sql_hash_prefix}",
            generated.profile_name.replace('_', "-"),
            generated.root_seed,
        ),
        schema,
        workload,
        ordering,
        feature_ids: feature_ids.into_iter().collect(),
        seed_lineage,
    })
}

/// Adapt one canonical generated case into the existing strict differential
/// envelope without reparsing or heuristically repartitioning its SQL.
pub fn adapt_generated_case(
    generated: GeneratedCase,
    run_id: impl Into<String>,
    scenario_id: impl Into<String>,
) -> Result<TypedDifferentialCase, TypedAdapterError> {
    let run_id = run_id.into();
    let scenario_id = scenario_id.into();
    if run_id.trim().is_empty() || scenario_id.trim().is_empty() {
        return Err(TypedAdapterError::invalid(
            "typed_case.identity",
            "run_id and scenario_id must be non-empty",
        ));
    }
    let evidence = generated
        .canonical_profile_evidence
        .as_ref()
        .ok_or_else(|| {
            TypedAdapterError::unsupported(
                "typed_case.profile",
                "bootstrap/non-canonical generated cases cannot enter parity campaigns",
            )
        })?;
    validate_canonical_profile_evidence(evidence)
        .map_err(|error| TypedAdapterError::invalid(error.constraint, error.message))?;
    let projection = project_generated_case(&generated)?;

    let envelope = ExecutionEnvelope::builder(generated.root_seed)
        .run_id(run_id.clone())
        .scenario_id(scenario_id.clone())
        .schema(projection.schema)
        .workload(projection.workload)
        .statement_ordering(projection.ordering.clone())
        .build();
    let mut case = TypedDifferentialCase {
        schema_version: TYPED_DIFFERENTIAL_SCHEMA_VERSION.to_owned(),
        adapter_version: TYPED_DIFFERENTIAL_ADAPTER_VERSION.to_owned(),
        case_id: projection.case_id,
        run_id,
        trace_id: generated.trace_hash.clone(),
        scenario_id,
        profile_sha256: evidence.profile_sha256.clone(),
        feature_ids: projection.feature_ids,
        required_lanes: evidence.required_lanes.clone(),
        ordering: projection.ordering,
        seed_lineage: projection.seed_lineage,
        generated,
        envelope,
        content_hash: String::new(),
    };
    case.content_hash = case.deterministic_hash();
    case.validate()?;
    Ok(case)
}

/// Convert a typed adapter artifact into the existing normalized corpus.
#[must_use]
pub fn typed_case_to_corpus_entry(case: &TypedDifferentialCase) -> CorpusEntry {
    let statements = case
        .envelope
        .schema
        .iter()
        .chain(case.envelope.workload.iter())
        .cloned()
        .collect();
    CorpusEntry {
        id: case.case_id.clone(),
        family: Family::SQL,
        secondary_families: Vec::new(),
        source: CorpusSource::TypedGenerated {
            generator_version: case.generated.generator_version.clone(),
            profile_name: case.generated.profile_name.clone(),
            profile_sha256: case.profile_sha256.clone(),
            trace_hash: case.generated.trace_hash.clone(),
            case_hash: case.content_hash.clone(),
            seed: case.generated.root_seed,
            promotion: None,
        },
        statements,
        seed: case.generated.root_seed,
        skip: None,
        taxonomy_features: case.feature_ids.clone(),
        description: format!(
            "canonical typed differential case for profile {}",
            case.generated.profile_name
        ),
    }
}

/// One public-path typed differential result with lane evidence.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TypedDifferentialRun {
    pub schema_version: String,
    pub case_id: String,
    pub case_hash: String,
    pub profile_sha256: String,
    pub feature_ids: Vec<String>,
    pub required_lanes: Vec<ExecutionLane>,
    pub lane_evidence: Vec<ExecutionLaneEvidence>,
    pub result: DifferentialResult,
}

fn validate_typed_lane_evidence(
    case: &TypedDifferentialCase,
    lane_evidence: &[ExecutionLaneEvidence],
) -> Result<(), TypedAdapterError> {
    for evidence in lane_evidence {
        let errors = evidence.validate();
        if !errors.is_empty() {
            return Err(TypedAdapterError::new(
                TypedAdapterErrorKind::LaneViolation,
                "typed_case.lane_evidence",
                errors.join("; "),
            ));
        }
        if evidence.run_id != case.run_id
            || evidence.trace_id != case.trace_id
            || evidence.scenario_id != case.scenario_id
        {
            return Err(TypedAdapterError::new(
                TypedAdapterErrorKind::LaneViolation,
                "typed_case.lane_evidence",
                "lane evidence identity does not match the typed case",
            ));
        }
    }
    for required in &case.required_lanes {
        if !lane_evidence
            .iter()
            .any(|evidence| evidence.required_lane == *required && evidence.requirement_satisfied)
        {
            return Err(TypedAdapterError::new(
                TypedAdapterErrorKind::LaneViolation,
                "typed_case.required_lanes",
                format!(
                    "required lane {} was not positively observed",
                    required.label()
                ),
            ));
        }
    }
    Ok(())
}

/// Build honest semantic-lane evidence for supported-core/read-only/DML
/// campaigns. Stronger profiles must supply observations from their existing
/// instrumented E2E lane and fail closed here otherwise.
pub fn semantic_lane_evidence(
    case: &TypedDifferentialCase,
) -> Result<Vec<ExecutionLaneEvidence>, TypedAdapterError> {
    if case.required_lanes != [ExecutionLane::SqlResultOnly] {
        return Err(TypedAdapterError::unsupported(
            "typed_case.required_lanes",
            "semantic adapter cannot certify pager/planner/VDBE/MVCC/recovery lanes",
        ));
    }
    Ok(vec![ExecutionLaneEvidence::semantic_only(
        case.trace_id.clone(),
        case.run_id.clone(),
        case.scenario_id.clone(),
        "typed_sql_case",
    )])
}

/// Run one adapted case through the existing strict comparator.
pub fn run_typed_differential_case<F: SqlExecutor, C: SqlExecutor>(
    case: &TypedDifferentialCase,
    lane_evidence: Vec<ExecutionLaneEvidence>,
    fsqlite: &F,
    csqlite: &C,
) -> Result<TypedDifferentialRun, TypedAdapterError> {
    case.validate()?;
    validate_typed_lane_evidence(case, &lane_evidence)?;
    let result = crate::differential_v2::run_differential(&case.envelope, fsqlite, csqlite);
    if result.outcome != Outcome::Error && result.comparisons.len() != result.statements_total {
        return Err(TypedAdapterError::invalid(
            "typed_case.comparisons",
            "strict differential result omitted statement outcome evidence",
        ));
    }
    tracing::info!(
        bead_id = "bd-turso-test-adaptation-zu081.5",
        run_id = %case.run_id,
        trace_id = %case.trace_id,
        scenario_id = %case.scenario_id,
        seed = case.generated.root_seed,
        profile = %case.generated.profile_name,
        profile_sha256 = %case.profile_sha256,
        feature_count = case.feature_ids.len(),
        lane_required = %case.required_lanes.iter().map(|lane| lane.label()).collect::<Vec<_>>().join(","),
        outcome = %result.outcome,
        mismatched = result.statements_mismatched,
        "typed differential case completed"
    );
    Ok(TypedDifferentialRun {
        schema_version: TYPED_DIFFERENTIAL_SCHEMA_VERSION.to_owned(),
        case_id: case.case_id.clone(),
        case_hash: case.content_hash.clone(),
        profile_sha256: case.profile_sha256.clone(),
        feature_ids: case.feature_ids.clone(),
        required_lanes: case.required_lanes.clone(),
        lane_evidence,
        result,
    })
}

/// Execute with explicit orchestration state so cancellation and timeout are
/// not collapsed into generic SQL errors.
pub fn run_typed_differential_case_with_control<F: SqlExecutor, C: SqlExecutor>(
    case: &TypedDifferentialCase,
    lane_evidence: Vec<ExecutionLaneEvidence>,
    cancelled: bool,
    timed_out: bool,
    fsqlite: &F,
    csqlite: &C,
) -> Result<TypedDifferentialRun, TypedAdapterError> {
    if cancelled {
        return Err(TypedAdapterError::new(
            TypedAdapterErrorKind::Cancelled,
            "typed_case.execution",
            "typed differential execution cancelled before dispatch",
        ));
    }
    if timed_out {
        return Err(TypedAdapterError::new(
            TypedAdapterErrorKind::Timeout,
            "typed_case.execution",
            "typed differential execution exceeded its orchestration budget",
        ));
    }
    run_typed_differential_case(case, lane_evidence, fsqlite, csqlite)
}

/// Build the canonical failure bundle for one typed differential divergence.
///
/// The bundle embeds the exact generated case and differential result, while
/// its typed evidence binds engine identities, canonical contract hashes,
/// seed lineage, ordering policy, required lanes, and the result hash.
pub fn build_typed_failure_bundle(
    case: &TypedDifferentialCase,
    run: &TypedDifferentialRun,
    created_at: &str,
    repro_command: &str,
    environment: EnvironmentInfo,
    subject: TypedEngineProvenance,
    reference: TypedEngineProvenance,
) -> Result<FailureBundle, TypedAdapterError> {
    case.validate()?;
    let run_provenance = (
        run.case_id.as_str(),
        run.case_hash.as_str(),
        run.profile_sha256.as_str(),
        run.feature_ids.as_slice(),
        run.required_lanes.as_slice(),
    );
    let case_provenance = (
        case.case_id.as_str(),
        case.content_hash.as_str(),
        case.profile_sha256.as_str(),
        case.feature_ids.as_slice(),
        case.required_lanes.as_slice(),
    );
    if run_provenance != case_provenance {
        return Err(TypedAdapterError::invalid(
            "typed_bundle.run",
            "typed run provenance does not match the generated case",
        ));
    }
    if run.result.outcome != Outcome::Divergence || run.result.divergences.is_empty() {
        return Err(TypedAdapterError::invalid(
            "typed_bundle.outcome",
            "failure bundles require a statement-level differential divergence",
        ));
    }
    validate_typed_lane_evidence(case, &run.lane_evidence)?;
    let primary_evidence = case
        .required_lanes
        .first()
        .and_then(|required| {
            run.lane_evidence.iter().find(|evidence| {
                evidence.required_lane == *required && evidence.requirement_satisfied
            })
        })
        .cloned()
        .ok_or_else(|| {
            TypedAdapterError::new(
                TypedAdapterErrorKind::LaneViolation,
                "typed_bundle.execution_lane",
                "no positively observed primary execution lane is available",
            )
        })?;
    let profile = case
        .generated
        .canonical_profile_evidence
        .as_ref()
        .ok_or_else(|| {
            TypedAdapterError::unsupported(
                "typed_bundle.profile",
                "canonical profile evidence is required for failure publication",
            )
        })?;
    let contract_sha256 = BTreeMap::from([
        (
            "feature_universe_ledger".to_owned(),
            profile.feature_ledger_sha256.clone(),
        ),
        (
            "parity_taxonomy".to_owned(),
            profile.parity_taxonomy_sha256.clone(),
        ),
        (
            "sqlite_version_contract".to_owned(),
            profile.version_contract_sha256.clone(),
        ),
        (
            "supported_surface_matrix".to_owned(),
            profile.surface_matrix_sha256.clone(),
        ),
    ]);
    let first = &run.result.divergences[0];
    let expected = serde_json::to_string(&first.csqlite_outcome)
        .map_err(|error| TypedAdapterError::artifact("typed_bundle.expected", error.to_string()))?;
    let actual = serde_json::to_string(&first.fsqlite_outcome)
        .map_err(|error| TypedAdapterError::artifact("typed_bundle.actual", error.to_string()))?;
    let typed_case_json = case.to_canonical_json()?;
    let result_json = serde_json::to_string_pretty(&run.result)
        .map_err(|error| TypedAdapterError::artifact("typed_bundle.result", error.to_string()))?;
    let phase = if first.index < case.envelope.schema.len() {
        "schema"
    } else {
        "workload"
    };
    let typed_evidence = TypedDifferentialEvidence {
        schema_version: "fsqlite.typed-differential-evidence.v1".to_owned(),
        adapter_version: case.adapter_version.clone(),
        generator_version: case.generated.generator_version.clone(),
        profile_name: case.generated.profile_name.clone(),
        profile_version: case.generated.profile_version.clone(),
        profile_sha256: case.profile_sha256.clone(),
        case_sha256: case.content_hash.clone(),
        contract_sha256,
        feature_ids: case.feature_ids.clone(),
        required_lanes: case.required_lanes.clone(),
        ordering: case.ordering.clone(),
        seed_lineage: case
            .seed_lineage
            .iter()
            .map(|seed| {
                format!(
                    "ordinal={};path={};seed={}",
                    seed.ordinal, seed.seed_path, seed.derived_seed
                )
            })
            .collect(),
        subject,
        reference,
        differential_result_sha256: run.result.artifact_hashes.result_hash.clone(),
    };
    let suffix = run
        .result
        .artifact_hashes
        .result_hash
        .get(..12)
        .ok_or_else(|| {
            TypedAdapterError::artifact(
                "typed_bundle.result_hash",
                "differential result hash is shorter than 12 bytes",
            )
        })?;
    FailureBundleBuilder::new()
        .bundle_id(&format!("fb-{}-{suffix}", case.run_id))
        .created_at(created_at)
        .run_id(&case.run_id)
        .execution_lane_evidence(primary_evidence)
        .scenario(ScenarioInfo {
            scenario_id: case.scenario_id.clone(),
            bead_id: "bd-turso-test-adaptation-zu081.5".to_owned(),
            test_name: case.case_id.clone(),
            script_path: Some(
                "crates/fsqlite-harness/src/bin/differential_manifest_runner.rs".to_owned(),
            ),
        })
        .failure(FailureInfo {
            failure_type: FailureType::Divergence,
            message: format!("typed differential divergence at statement {}", first.index),
            expected: Some(expected),
            actual: Some(actual),
            diff: None,
            invariant: Some("strict C SQLite differential parity".to_owned()),
            first_divergence: Some(FirstDivergence {
                operation_index: u64::try_from(first.index).unwrap_or(u64::MAX),
                sql: Some(first.sql.clone()),
                phase: Some(phase.to_owned()),
            }),
        })
        .reproducibility(ReproducibilityInfo {
            seed: Some(case.generated.root_seed),
            fixture_id: Some(case.case_id.clone()),
            schedule_fingerprint: None,
            repro_command: repro_command.to_owned(),
            storage_mode: Some("in-memory".to_owned()),
            concurrency_mode: Some("concurrent-writers".to_owned()),
        })
        .environment(environment)
        .state_snapshot("typed_case_json", &typed_case_json)
        .state_snapshot("differential_result_json", &result_json)
        .typed_differential(typed_evidence)
        .triage_tag("typed-differential")
        .triage_tag("turso-test-adaptation")
        .build()
        .map_err(|error| TypedAdapterError::artifact("typed_bundle", error))
}

/// Minimize a typed divergence using a fresh subject and reference executor
/// for every candidate workload.
pub fn minimize_typed_divergence<F, C, FF, CF>(
    case: &TypedDifferentialCase,
    config: &MinimizerConfig,
    repro_command: &str,
    fsqlite_factory: FF,
    csqlite_factory: CF,
) -> Result<MinimalReproduction, TypedAdapterError>
where
    F: SqlExecutor,
    C: SqlExecutor,
    FF: Fn() -> Result<F, String> + 'static,
    CF: Fn() -> Result<C, String> + 'static,
{
    case.validate()?;
    let schema = case.envelope.schema.clone();
    let source_workload = case.envelope.workload.clone();
    let schema_ordering = case.ordering[..case.envelope.schema.len()].to_vec();
    let workload_ordering = case.ordering[case.envelope.schema.len()..].to_vec();
    let seed = case.envelope.seed;
    let root_seed = case.generated.root_seed;
    let run_id = case.run_id.clone();
    let scenario_id = case.scenario_id.clone();
    let engines = case.envelope.engines.clone();
    let pragmas = case.envelope.pragmas.clone();
    let canonicalization = case.envelope.canonicalization.clone();
    let source_workload_for_test = source_workload.clone();
    let test = move |schema: &[String], workload: &[String]| {
        let fsqlite = fsqlite_factory().ok()?;
        let csqlite = csqlite_factory().ok()?;
        let mut next_index = 0;
        let mut candidate_ordering = schema_ordering.clone();
        for statement in workload {
            let relative = source_workload_for_test[next_index..]
                .iter()
                .position(|candidate| candidate == statement)?;
            next_index += relative;
            candidate_ordering.push(workload_ordering[next_index]);
            next_index += 1;
        }
        let envelope = ExecutionEnvelope::builder(seed)
            .run_id(run_id.clone())
            .scenario_id(scenario_id.clone())
            .engines(engines.fsqlite.clone(), engines.csqlite.clone())
            .engine_identities(
                engines.subject_identity.clone(),
                engines.reference_identity.clone(),
            )
            .pragmas(pragmas.clone())
            .schema(schema.iter().cloned())
            .workload(workload.iter().cloned())
            .canonicalization(canonicalization.clone())
            .statement_ordering(candidate_ordering)
            .build();
        let result = crate::differential_v2::run_differential(&envelope, &fsqlite, &csqlite);
        (result.outcome == Outcome::Divergence).then_some(result.divergences)
    };
    let mut minimal =
        minimize_workload(&schema, &source_workload, config, &test).ok_or_else(|| {
            TypedAdapterError::invalid(
                "typed_minimizer.reproduction",
                "full typed workload did not reproduce a statement divergence",
            )
        })?;
    if repro_command.contains('\n')
        || repro_command.contains('\r')
        || repro_command.trim().is_empty()
    {
        return Err(TypedAdapterError::invalid(
            "typed_minimizer.repro_command",
            "reproduction command must be non-empty and single-line",
        ));
    }
    minimal.original_seed = root_seed;
    repro_command.clone_into(&mut minimal.repro_command);
    Ok(minimal)
}

/// Replayable result of reducing one typed differential divergence.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StructuredTypedMinimization {
    pub schema_version: String,
    pub original_case_hash: String,
    pub minimized_case: TypedDifferentialCase,
    pub original_result_sha256: String,
    pub minimized_result_sha256: String,
    pub mismatch_signature: String,
    pub reduction: TypedReductionResult,
    pub repro_command: String,
    pub content_hash: String,
}

impl StructuredTypedMinimization {
    #[must_use]
    pub fn deterministic_hash(&self) -> String {
        let mut canonical = self.clone();
        canonical.content_hash.clear();
        let bytes = serde_json::to_vec(&canonical)
            .expect("structured typed minimization serialization must succeed");
        sha256_hex(&bytes)
    }

    pub fn validate(&self) -> Result<(), TypedAdapterError> {
        if self.schema_version != "fsqlite.structured-typed-minimization.v1" {
            return Err(TypedAdapterError::artifact(
                "typed_reduction.schema_version",
                "structured typed minimization schema is unsupported",
            ));
        }
        self.minimized_case.validate()?;
        self.reduction
            .validate()
            .map_err(|error| TypedAdapterError::artifact("typed_reduction.result", error))?;
        if !is_sha256_hex_64(&self.original_case_hash)
            || !is_sha256_hex_64(&self.original_result_sha256)
            || !is_sha256_hex_64(&self.minimized_result_sha256)
        {
            return Err(TypedAdapterError::artifact(
                "typed_reduction.hashes",
                "case and result identities must be lowercase SHA-256 values",
            ));
        }
        if self.mismatch_signature != self.reduction.observation.mismatch_signature
            || self.minimized_case.required_lanes != self.reduction.observation.required_lanes
            || self.minimized_case.generated.statements != self.reduction.minimized_statements
        {
            return Err(TypedAdapterError::artifact(
                "typed_reduction.provenance",
                "minimized case, mismatch signature, lane, or AST provenance drifted",
            ));
        }
        validate_repro_command(&self.repro_command)?;
        if self.content_hash != self.deterministic_hash() {
            return Err(TypedAdapterError::artifact(
                "typed_reduction.content_hash",
                "structured typed minimization content hash mismatch",
            ));
        }
        Ok(())
    }

    pub fn to_json(&self) -> Result<String, TypedAdapterError> {
        self.validate()?;
        serde_json::to_string_pretty(self)
            .map_err(|error| TypedAdapterError::artifact("typed_reduction.json", error.to_string()))
    }

    pub fn from_json_strict(json: &str) -> Result<Self, TypedAdapterError> {
        let package: Self = serde_json::from_str(json).map_err(|error| {
            TypedAdapterError::artifact("typed_reduction.json", format!("decode failed: {error}"))
        })?;
        package.validate()?;
        Ok(package)
    }
}

fn validate_repro_command(repro_command: &str) -> Result<(), TypedAdapterError> {
    if repro_command.contains('\n')
        || repro_command.contains('\r')
        || repro_command.trim().is_empty()
    {
        return Err(TypedAdapterError::invalid(
            "typed_minimizer.repro_command",
            "reproduction command must be non-empty and single-line",
        ));
    }
    Ok(())
}

fn mismatch_witness_sha256(result: &DifferentialResult) -> Result<String, TypedAdapterError> {
    #[derive(Serialize)]
    struct Witness<'a> {
        outcome: Outcome,
        divergences: Vec<(
            &'a crate::differential_v2::StmtOutcome,
            &'a crate::differential_v2::StmtOutcome,
        )>,
    }

    if result.outcome != Outcome::Divergence || result.divergences.is_empty() {
        return Err(TypedAdapterError::invalid(
            "typed_reduction.witness",
            "structured reduction requires a statement-level divergence",
        ));
    }
    let witness = Witness {
        outcome: result.outcome,
        divergences: result
            .divergences
            .iter()
            .map(|divergence| (&divergence.csqlite_outcome, &divergence.fsqlite_outcome))
            .collect(),
    };
    let bytes = serde_json::to_vec(&witness).map_err(|error| {
        TypedAdapterError::artifact("typed_reduction.witness", error.to_string())
    })?;
    Ok(sha256_hex(&bytes))
}

fn rebuild_reduced_typed_case(
    source: &TypedDifferentialCase,
    statements: &[GeneratedStatement],
) -> Result<TypedDifferentialCase, TypedAdapterError> {
    let generated = source
        .generated
        .rebuild_with_statements(statements.to_vec())
        .map_err(|error| {
            TypedAdapterError::invalid(
                "typed_reduction.generated_case",
                format!("reduced generator AST is invalid: {error:?}"),
            )
        })?;
    let mut reduced =
        adapt_generated_case(generated, source.run_id.clone(), source.scenario_id.clone())?;
    reduced
        .envelope
        .engines
        .clone_from(&source.envelope.engines);
    reduced
        .envelope
        .pragmas
        .clone_from(&source.envelope.pragmas);
    reduced
        .envelope
        .canonicalization
        .clone_from(&source.envelope.canonicalization);
    reduced.content_hash = reduced.deterministic_hash();
    reduced.validate()?;
    Ok(reduced)
}

/// Reduce a typed case through fresh public executors for every candidate.
/// Exact result/error witness and required-lane identity are mandatory.
pub fn minimize_typed_divergence_structured<F, C, FF, CF, LF>(
    case: &TypedDifferentialCase,
    config: &TypedReductionConfig,
    repro_command: &str,
    fsqlite_factory: FF,
    csqlite_factory: CF,
    lane_evidence_factory: LF,
) -> Result<(StructuredTypedMinimization, TypedDifferentialRun), TypedAdapterError>
where
    F: SqlExecutor,
    C: SqlExecutor,
    FF: Fn() -> Result<F, String>,
    CF: Fn() -> Result<C, String>,
    LF: Fn(&TypedDifferentialCase) -> Result<Vec<ExecutionLaneEvidence>, TypedAdapterError>,
{
    case.validate()?;
    validate_repro_command(repro_command)?;
    let run_candidate = |statements: &[GeneratedStatement]| {
        let candidate = rebuild_reduced_typed_case(case, statements)?;
        let lane_evidence = lane_evidence_factory(&candidate)?;
        let fsqlite = fsqlite_factory().map_err(|error| {
            TypedAdapterError::artifact("typed_reduction.subject_factory", error)
        })?;
        let csqlite = csqlite_factory().map_err(|error| {
            TypedAdapterError::artifact("typed_reduction.reference_factory", error)
        })?;
        let run = run_typed_differential_case(&candidate, lane_evidence, &fsqlite, &csqlite)?;
        let signature = mismatch_witness_sha256(&run.result)?;
        Ok::<_, TypedAdapterError>((candidate, run, signature))
    };

    let (_, original_run, original_signature) = run_candidate(&case.generated.statements)?;
    let test = |statements: &[GeneratedStatement]| {
        let (candidate, _, mismatch_signature) = run_candidate(statements)
            .map_err(|error| format!("{}: {}", error.constraint, error.message))?;
        Ok(TypedReductionObservation {
            mismatch_signature,
            required_lanes: candidate.required_lanes,
        })
    };
    let reduction = minimize_typed_statements(&case.generated.statements, config, &test)
        .map_err(|error| TypedAdapterError::invalid("typed_reduction.minimizer", error))?;
    if reduction.observation.mismatch_signature != original_signature
        || reduction.observation.required_lanes != case.required_lanes
    {
        return Err(TypedAdapterError::invalid(
            "typed_reduction.original_witness",
            "full-case witness changed between required verification passes",
        ));
    }
    let (minimized_case, minimized_run, minimized_signature) =
        run_candidate(&reduction.minimized_statements)?;
    if minimized_signature != original_signature {
        return Err(TypedAdapterError::invalid(
            "typed_reduction.final_witness",
            "final minimized witness differs from the original",
        ));
    }

    let mut package = StructuredTypedMinimization {
        schema_version: "fsqlite.structured-typed-minimization.v1".to_owned(),
        original_case_hash: case.content_hash.clone(),
        minimized_case,
        original_result_sha256: original_run.result.artifact_hashes.result_hash,
        minimized_result_sha256: minimized_run.result.artifact_hashes.result_hash.clone(),
        mismatch_signature: original_signature,
        reduction,
        repro_command: repro_command.to_owned(),
        content_hash: String::new(),
    };
    package.content_hash = package.deterministic_hash();
    package.validate()?;
    tracing::info!(
        bead_id = "bd-turso-test-adaptation-zu081.6",
        run_id = %case.run_id,
        trace_id = %case.trace_id,
        scenario_id = %case.scenario_id,
        original_statements = package.reduction.stats.original_statements,
        minimized_statements = package.reduction.stats.minimized_statements,
        attempts = package.reduction.stats.attempts,
        accepted = package.reduction.stats.accepted_candidates,
        mismatch_signature = %package.mismatch_signature,
        required_lanes = %package.minimized_case.required_lanes.iter().map(|lane| lane.label()).collect::<Vec<_>>().join(","),
        complete = package.reduction.status.is_complete(),
        "structured typed differential reduction completed"
    );
    Ok((package, minimized_run))
}

/// Add original/minimized AST and deterministic reduction evidence to the
/// existing canonical typed failure bundle.
pub fn build_typed_reduction_failure_bundle(
    original_case: &TypedDifferentialCase,
    package: &StructuredTypedMinimization,
    minimized_run: &TypedDifferentialRun,
    created_at: &str,
    environment: EnvironmentInfo,
    subject: TypedEngineProvenance,
    reference: TypedEngineProvenance,
) -> Result<FailureBundle, TypedAdapterError> {
    original_case.validate()?;
    package.validate()?;
    if original_case.content_hash != package.original_case_hash
        || original_case.generated.statements != package.reduction.original_statements
    {
        return Err(TypedAdapterError::artifact(
            "typed_reduction.original_case",
            "original case does not match reduction provenance",
        ));
    }
    let mut bundle = build_typed_failure_bundle(
        &package.minimized_case,
        minimized_run,
        created_at,
        &package.repro_command,
        environment,
        subject,
        reference,
    )?;
    let original_json = original_case.to_canonical_json()?;
    let minimized_json = package.minimized_case.to_canonical_json()?;
    let reduction_json = package.to_json()?;
    bundle
        .state_snapshots
        .insert("original_typed_case_json".to_owned(), original_json);
    bundle
        .state_snapshots
        .insert("minimized_typed_case_json".to_owned(), minimized_json);
    bundle
        .state_snapshots
        .insert("typed_reduction_json".to_owned(), reduction_json);
    bundle.scenario.bead_id = "bd-turso-test-adaptation-zu081.6".to_owned();
    bundle.triage_tags.push("structured-reduction".to_owned());
    bundle.content_hash = bundle.deterministic_bundle_hash();
    let errors = bundle.validate();
    if !errors.is_empty() {
        return Err(TypedAdapterError::artifact(
            "typed_reduction.bundle",
            errors.join("; "),
        ));
    }
    Ok(bundle)
}

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
    /// Deterministic sample of passing cases for replay evidence.
    pub sampled_passing_cases: Vec<PassingCaseSample>,
    /// Cases skipped (generation produced no transformable cases).
    pub skipped: usize,
    /// Per-case results (only divergent cases included to save space).
    pub divergent_cases: Vec<CaseResult>,
    /// Deduplicated failure signatures.
    pub deduplicated: DeduplicatedFailures,
    /// Coverage summary by transform family and equivalence type.
    pub coverage_summary: CoverageSummary,
}

/// Deterministic sampled passing-case metadata for replay evidence.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PassingCaseSample {
    /// Test case identifier.
    pub case_id: String,
    /// Transform name that generated this case.
    pub transform_name: String,
    /// Deterministic case seed.
    pub seed: u64,
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
    let mut sampled_passing_cases = Vec::new();
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
                if sampled_passing_cases.len() < PASSING_REPLAY_SAMPLE_LIMIT {
                    sampled_passing_cases.push(PassingCaseSample {
                        case_id: case.id.clone(),
                        transform_name: case.transform_name.clone(),
                        seed: case.seed,
                    });
                }
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
        sampled_passing_cases,
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

fn sha256_hex(data: &[u8]) -> String {
    let digest = Sha256::digest(data);
    let mut hex = String::with_capacity(64);
    for byte in digest {
        let _ = write!(hex, "{byte:02x}");
    }
    hex
}

fn is_sha256_hex_64(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::corpus_ingest::{CorpusBuilder, CorpusSource, Family};
    use crate::differential_v2::{CsqliteExecutor, FsqliteExecutor, NormalizedValue, StmtOutcome};
    use crate::replay_harness::{
        TypedDifferentialReplayArtifact, promote_typed_divergence, replay_typed_differential,
    };
    use crate::typed_sql_generator::{
        GenerationBudget, GeneratorConfig, NamedGeneratorProfile, derive_named_profile,
        generate_case,
    };
    use proptest::prelude::*;

    fn workspace_root() -> std::path::PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(std::path::Path::parent)
            .expect("harness crate must live under workspace/crates")
            .to_path_buf()
    }

    fn typed_case(kind: NamedGeneratorProfile, seed: u64) -> TypedDifferentialCase {
        let profile = derive_named_profile(&workspace_root(), kind).expect("derive profile");
        let requested_statements =
            u32::try_from(profile.setup.len() + 8).expect("bounded statement count");
        let generated = generate_case(GeneratorConfig {
            root_seed: seed,
            requested_statements,
            profile,
            budget: GenerationBudget::default(),
        })
        .expect("generate canonical case");
        adapt_generated_case(generated, "typed-run", "TURSO-TYPED-5").expect("adapt canonical case")
    }

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
        identity: crate::differential_v2::EngineIdentity,
    }

    impl StubExecutor {
        fn new(identity: crate::differential_v2::EngineIdentity) -> Self {
            Self {
                results: std::collections::HashMap::new(),
                identity,
            }
        }

        fn fsqlite_stub() -> Self {
            Self::new(crate::differential_v2::EngineIdentity::FrankenSqlite)
        }

        fn csqlite_stub() -> Self {
            Self::new(crate::differential_v2::EngineIdentity::CSqliteOracle)
        }

        fn with_result(mut self, sql: &str, outcome: StmtOutcome) -> Self {
            self.results.insert(sql.trim().to_owned(), outcome);
            self
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
            self.identity
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
    fn typed_seed_shards_are_stable_disjoint_and_complete() {
        let range = TypedSeedRange::for_tier(TypedCampaignTier::Presubmit, 4_200);
        let shards = (0..7)
            .map(|shard_index| TypedSeedShard {
                range,
                shard_index,
                shard_count: 7,
            })
            .collect::<Vec<_>>();
        validate_typed_seed_shards(range, &shards).expect("valid complete shard set");
        assert_eq!(
            shards
                .iter()
                .map(|shard| shard.seeds().unwrap().len())
                .sum::<usize>(),
            usize::try_from(TYPED_PRESUBMIT_CASES_PER_PROFILE).unwrap()
        );
        assert_eq!(shards[0].seeds().unwrap(), shards[0].seeds().unwrap());

        let mut duplicate = shards.clone();
        duplicate[1].shard_index = 0;
        assert_eq!(
            validate_typed_seed_shards(range, &duplicate)
                .unwrap_err()
                .constraint,
            "seed_shards"
        );
    }

    proptest! {
        #[test]
        fn typed_seed_shards_partition_arbitrary_bounded_ranges(
            start in 0_u64..=u64::MAX - 512,
            count in 1_u32..=512,
            shard_count in 1_u32..=16,
        ) {
            let range = TypedSeedRange {
                tier: TypedCampaignTier::Presubmit,
                start,
                count,
            };
            let shards = (0..shard_count)
                .map(|shard_index| TypedSeedShard {
                    range,
                    shard_index,
                    shard_count,
                })
                .collect::<Vec<_>>();
            prop_assert!(validate_typed_seed_shards(range, &shards).is_ok());
            let union = shards
                .iter()
                .flat_map(|shard| shard.seeds().expect("valid shard"))
                .collect::<BTreeSet<_>>();
            prop_assert_eq!(union, range.seeds().expect("valid range").into_iter().collect());
        }
    }

    #[test]
    fn typed_adapter_preserves_roles_ordering_lineage_and_corpus_metadata() {
        let case = typed_case(NamedGeneratorProfile::ReadOnly, 0xD1FF_E2E0_2026_0804);
        assert_eq!(
            case.envelope.schema.len() + case.envelope.workload.len(),
            case.generated.statements.len()
        );
        assert_eq!(case.ordering.len(), case.generated.statements.len());
        assert_eq!(case.seed_lineage.len(), case.generated.statements.len());
        assert!(
            case.generated
                .statements
                .iter()
                .zip(&case.ordering)
                .all(|(statement, ordering)| *ordering == typed_statement_ordering(statement))
        );
        let json = case.to_canonical_json().expect("encode typed case");
        let decoded = TypedDifferentialCase::from_json_strict(&json).expect("decode typed case");
        assert_eq!(decoded, case);

        let left = typed_case_to_corpus_entry(&case);
        let right = typed_case_to_corpus_entry(&case);
        assert_eq!(left.content_hash(), right.content_hash());
        assert_eq!(
            serde_json::to_string(&left.source).unwrap(),
            serde_json::to_string(&right.source).unwrap()
        );
        assert_eq!(left.taxonomy_features, case.feature_ids);
    }

    #[test]
    fn typed_adapter_runs_public_engines_and_keeps_control_states_explicit() {
        let case = typed_case(NamedGeneratorProfile::ReadOnly, 0xD1FF_E2E0_2026_0804);
        let fsqlite = FsqliteExecutor::open_in_memory().expect("open FrankenSQLite");
        let csqlite = CsqliteExecutor::open_in_memory().expect("open C SQLite");
        let lane_evidence = semantic_lane_evidence(&case).expect("semantic lane evidence");
        let report = run_typed_differential_case(&case, lane_evidence.clone(), &fsqlite, &csqlite)
            .expect("run typed differential");
        assert_eq!(
            report.result.outcome,
            Outcome::Pass,
            "{:#?}",
            report.result.divergences
        );
        assert_eq!(
            report.result.comparisons.len(),
            report.result.statements_total
        );
        assert!(report.result.comparisons.iter().all(|row| row.matched));

        assert_eq!(
            run_typed_differential_case_with_control(
                &case,
                lane_evidence.clone(),
                true,
                false,
                &fsqlite,
                &csqlite,
            )
            .unwrap_err()
            .kind,
            TypedAdapterErrorKind::Cancelled
        );
        assert_eq!(
            run_typed_differential_case_with_control(
                &case,
                lane_evidence,
                false,
                true,
                &fsqlite,
                &csqlite,
            )
            .unwrap_err()
            .kind,
            TypedAdapterErrorKind::Timeout
        );
    }

    #[test]
    fn typed_adapter_fails_closed_on_unobserved_stronger_lane_and_corruption() {
        let planner = typed_case(NamedGeneratorProfile::Planner, 77);
        assert_eq!(
            semantic_lane_evidence(&planner).unwrap_err().kind,
            TypedAdapterErrorKind::Unsupported
        );
        let fallback_decision = crate::failure_bundle::FallbackDecisionEvidence {
            statement_kind: "select".to_owned(),
            fallback_boundary: "conn.select.with_clause_materialization".to_owned(),
            decision_reason: "with_clause_materialization".to_owned(),
            decision_outcome: "allowed_compatibility_fallback".to_owned(),
            source_touchpoint: "typed-adapter-negative-test".to_owned(),
            first_failure_diagnostic: "statement_kind=select; fallback_boundary=conn.select.with_clause_materialization; source_touchpoint=typed-adapter-negative-test; decision_reason=with_clause_materialization".to_owned(),
            occurrences: 1,
        };
        let forced_fallback = ExecutionLaneEvidence::from_observations(
            ExecutionLane::PlannerRequired,
            vec![
                crate::failure_bundle::ObservedExecutionLane::Planner,
                crate::failure_bundle::ObservedExecutionLane::CompatibilityFallback,
            ],
            planner.trace_id.clone(),
            planner.run_id.clone(),
            planner.scenario_id.clone(),
            "select",
            "memory",
            "fallback_allowed",
            "memory:fallback_allowed",
            vec![fallback_decision],
            true,
        );
        assert!(forced_fallback.validate().is_empty());
        assert!(!forced_fallback.requirement_satisfied);
        assert_eq!(
            run_typed_differential_case(
                &planner,
                vec![forced_fallback],
                &StubExecutor::fsqlite_stub(),
                &StubExecutor::csqlite_stub(),
            )
            .unwrap_err()
            .kind,
            TypedAdapterErrorKind::LaneViolation
        );

        let case = typed_case(NamedGeneratorProfile::Dml, 88);
        let mut json = case.to_canonical_json().unwrap();
        json.truncate(json.len() / 2);
        assert_eq!(
            TypedDifferentialCase::from_json_strict(&json)
                .unwrap_err()
                .kind,
            TypedAdapterErrorKind::Artifact
        );
        let mut corrupted = case.clone();
        corrupted.profile_sha256 = "0".repeat(64);
        assert_eq!(
            corrupted.validate().unwrap_err().constraint,
            "typed_case.provenance"
        );

        let mut cross_link_drift = case.clone();
        cross_link_drift.envelope.schema[0].push(' ');
        cross_link_drift.content_hash = cross_link_drift.deterministic_hash();
        assert_eq!(
            cross_link_drift.validate().unwrap_err().constraint,
            "typed_case.provenance"
        );

        let mut ast_sql_drift = case;
        ast_sql_drift.generated.statements[0].sql.push(' ');
        let sql_script = ast_sql_drift
            .generated
            .statements
            .iter()
            .map(|statement| statement.sql.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        ast_sql_drift.generated.sql_hash = sha256_hex(sql_script.as_bytes());
        ast_sql_drift.content_hash = ast_sql_drift.deterministic_hash();
        assert_eq!(
            ast_sql_drift.validate().unwrap_err().constraint,
            "typed_case.statement_sql"
        );
    }

    #[test]
    fn typed_divergence_bundles_replays_minimizes_and_promotes_fail_closed() {
        let case = typed_case(NamedGeneratorProfile::ReadOnly, 0xBADD_1FF5_2026_0804);
        let divergent_sql = case.envelope.workload[0].clone();
        let fsqlite = StubExecutor::fsqlite_stub().with_result(
            &divergent_sql,
            StmtOutcome::Rows(vec![vec![NormalizedValue::Integer(1)]]),
        );
        let csqlite = StubExecutor::csqlite_stub().with_result(
            &divergent_sql,
            StmtOutcome::Rows(vec![vec![NormalizedValue::Integer(2)]]),
        );
        let run = run_typed_differential_case(
            &case,
            semantic_lane_evidence(&case).expect("semantic evidence"),
            &fsqlite,
            &csqlite,
        )
        .expect("synthetic typed divergence");
        assert_eq!(run.result.outcome, Outcome::Divergence);

        let bundle = build_typed_failure_bundle(
            &case,
            &run,
            "2026-08-04T00:00:00Z",
            "cargo run -p fsqlite-harness --bin differential_manifest_runner -- --typed-replay artifact.json",
            EnvironmentInfo::new("candidate-sha", "nightly", "test-platform"),
            TypedEngineProvenance {
                identity: "frankensqlite".to_owned(),
                version: case.envelope.engines.fsqlite.clone(),
                git_sha: "candidate-sha".to_owned(),
                dirty: false,
            },
            TypedEngineProvenance {
                identity: "csqlite-oracle".to_owned(),
                version: case.envelope.engines.csqlite.clone(),
                git_sha: "rusqlite-bundled".to_owned(),
                dirty: false,
            },
        )
        .expect("canonical typed bundle");
        let bundle_json = bundle.to_json().expect("bundle JSON");
        assert_eq!(
            FailureBundle::from_json_strict(&bundle_json).expect("strict bundle"),
            bundle
        );
        let mut truncated_bundle = bundle_json;
        truncated_bundle.truncate(truncated_bundle.len() / 2);
        assert!(FailureBundle::from_json_strict(&truncated_bundle).is_err());

        let artifact = TypedDifferentialReplayArtifact::from_run(case.clone(), &run, Some(bundle))
            .expect("typed replay artifact");
        let artifact_json = artifact.to_json().expect("replay JSON");
        let decoded = TypedDifferentialReplayArtifact::from_json_strict(&artifact_json)
            .expect("strict replay artifact");
        let (_, verification) =
            replay_typed_differential(&decoded, &fsqlite, &csqlite).expect("exact replay");
        assert!(verification.exact_match);

        let mut truncated_artifact = artifact_json;
        truncated_artifact.truncate(truncated_artifact.len() / 2);
        assert!(TypedDifferentialReplayArtifact::from_json_strict(&truncated_artifact).is_err());
        let mut corrupted = decoded.clone();
        corrupted.expected_result_sha256 = "0".repeat(64);
        assert!(corrupted.validate().is_err());
        let mut missing_lane_proof = decoded.clone();
        missing_lane_proof.lane_evidence.clear();
        assert!(missing_lane_proof.validate().is_err());

        let fsqlite_factory_value = fsqlite.clone();
        let csqlite_factory_value = csqlite.clone();
        let minimal = minimize_typed_divergence(
            &case,
            &MinimizerConfig::default(),
            "cargo run -p fsqlite-harness --bin differential_manifest_runner -- --typed-replay artifact.json",
            move || Ok(fsqlite_factory_value.clone()),
            move || Ok(csqlite_factory_value.clone()),
        )
        .expect("typed minimizer handoff");
        assert_eq!(minimal.original_seed, case.generated.root_seed);
        assert!(!minimal.minimal_workload.is_empty());
        assert!(!minimal.divergences.is_empty());

        let mut builder = CorpusBuilder::new(case.generated.root_seed);
        assert!(
            promote_typed_divergence(&mut builder, &decoded, &verification, &minimal, "").is_err()
        );
        let promotion = promote_typed_divergence(
            &mut builder,
            &decoded,
            &verification,
            &minimal,
            "reviewer@example.invalid",
        )
        .expect("reviewed promotion");
        let manifest = builder.build();
        assert_eq!(manifest.entries.len(), 1);
        assert_eq!(manifest.entries[0].id, promotion.entry_id);
        assert_eq!(
            manifest.entries[0].content_hash(),
            promotion.entry_content_sha256
        );
        assert!(matches!(
            &manifest.entries[0].source,
            CorpusSource::TypedGenerated {
                promotion: Some(_),
                ..
            }
        ));
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
            || Ok(StubExecutor::fsqlite_stub()),
            || Ok(StubExecutor::csqlite_stub()),
        )
        .expect("empty corpus should succeed");

        assert_eq!(report.total_cases, 0);
        assert_eq!(report.passed, 0);
        assert_eq!(report.diverged, 0);
        assert!(report.sampled_passing_cases.is_empty());
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
            || Ok(StubExecutor::fsqlite_stub()),
            || Ok(StubExecutor::csqlite_stub()),
        )
        .expect("matching stubs should succeed");

        // With matching stub executors, no divergences.
        assert_eq!(report.diverged, 0);
        assert!(report.divergent_cases.is_empty());
        assert!(!report.sampled_passing_cases.is_empty());
        assert!(
            report.sampled_passing_cases.len() <= PASSING_REPLAY_SAMPLE_LIMIT,
            "passing sample size must be bounded"
        );
    }

    #[test]
    fn test_passing_case_sampling_is_deterministic() {
        let entries = vec![make_entry(
            "sampling",
            vec![
                "CREATE TABLE t(a INTEGER, b TEXT)",
                "INSERT INTO t VALUES(1, 'hello')",
                "SELECT * FROM t",
            ],
        )];
        let config = RunConfig {
            max_cases_per_entry: 4,
            enable_minimization: false,
            ..RunConfig::default()
        };

        let report_a = run_metamorphic_differential(
            &entries,
            &config,
            || Ok(StubExecutor::fsqlite_stub()),
            || Ok(StubExecutor::csqlite_stub()),
        )
        .expect("first deterministic run should succeed");
        let report_b = run_metamorphic_differential(
            &entries,
            &config,
            || Ok(StubExecutor::fsqlite_stub()),
            || Ok(StubExecutor::csqlite_stub()),
        )
        .expect("second deterministic run should succeed");

        assert_eq!(
            report_a.sampled_passing_cases, report_b.sampled_passing_cases,
            "passing replay samples must be deterministic for identical seeds and corpus"
        );
        assert_eq!(
            report_a.sampled_passing_cases.len(),
            report_a.passed.min(PASSING_REPLAY_SAMPLE_LIMIT),
            "sample count should be bounded by pass count and sample limit"
        );
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

        let mut fsqlite = StubExecutor::fsqlite_stub();
        fsqlite.results.insert(
            "SELECT 1".to_owned(),
            StmtOutcome::Rows(vec![vec![NormalizedValue::Integer(1)]]),
        );

        let mut csqlite = StubExecutor::csqlite_stub();
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
