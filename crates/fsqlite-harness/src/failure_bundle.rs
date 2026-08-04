//! Failure artifact bundling and triage UX (bd-mblr.4.4).
//!
//! Provides a unified failure bundle schema for E2E tests, collecting all
//! context needed to triage and reproduce a test failure:
//! - **Failure classification**: assertion, panic, divergence, timeout, SSI conflict.
//! - **Reproducibility**: seed, fixture ID, schedule fingerprint, repro command.
//! - **State snapshots**: artifact hashes, DB page previews, WAL state.
//! - **Diff hints**: expected vs. actual, first-divergence markers.
//! - **Environment**: git SHA, toolchain, platform, feature flags.
//!
//! # Schema Version
//!
//! The current bundle schema version is `2.0.0`. Bundles are self-describing:
//! every serialized bundle includes the schema version so readers can reject
//! unknown schemas and require an explicit migration.
//!
//! # Adoption Checklist
//!
//! See [`build_e2e_adoption_checklist`] for the E2E-specific adoption items
//! (E-1 through E-10) that complement the unit-test diagnostics contract from
//! [`crate::test_diagnostics`].

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::LazyLock;

use crate::test_inventory::ExecutionLane;

#[allow(dead_code)]
const BEAD_ID: &str = "bd-mblr.4.4";

/// Schema version for the failure bundle format.
pub const BUNDLE_SCHEMA_VERSION: &str = "2.0.0";
/// Schema version for execution-lane evidence embedded in reports and bundles.
pub const EXECUTION_LANE_EVIDENCE_SCHEMA_VERSION: &str = "fsqlite.execution_lane_evidence.v1";
const FIRST_FAILURE_DIAGNOSTIC_POINTER: &str = "/failure/first_divergence";

/// Runtime components whose participation was positively observed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObservedExecutionLane {
    /// The public SQL path produced a result or classified error.
    SqlResult,
    /// The run performed pager-backed storage work.
    PagerBacked,
    /// The query planner participated in compilation.
    Planner,
    /// VDBE bytecode executed.
    Vdbe,
    /// Concurrent page-level MVCC commit planning executed.
    Mvcc,
    /// WAL recovery executed.
    Recovery,
    /// A compatibility fallback was attempted, allowed, or denied.
    CompatibilityFallback,
}

impl ObservedExecutionLane {
    /// Stable serialized/test-facing identifier for this observation.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::SqlResult => "sql_result",
            Self::PagerBacked => "pager_backed",
            Self::Planner => "planner",
            Self::Vdbe => "vdbe",
            Self::Mvcc => "mvcc",
            Self::Recovery => "recovery",
            Self::CompatibilityFallback => "compatibility_fallback",
        }
    }
}

/// One decision emitted by the existing `fsqlite.fallback_decision` contract.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FallbackDecisionEvidence {
    pub statement_kind: String,
    pub fallback_boundary: String,
    pub decision_reason: String,
    pub decision_outcome: String,
    pub source_touchpoint: String,
    pub first_failure_diagnostic: String,
    pub occurrences: u64,
}

impl FallbackDecisionEvidence {
    #[must_use]
    fn canonical_first_failure_diagnostic(&self) -> String {
        format!(
            "statement_kind={}; fallback_boundary={}; source_touchpoint={}; decision_reason={}",
            self.statement_kind,
            self.fallback_boundary,
            self.source_touchpoint,
            self.decision_reason,
        )
    }
}

#[derive(Debug, Deserialize)]
struct FallbackBoundaryInventory {
    allowed_decision_outcomes: Vec<String>,
    boundary: Vec<FallbackBoundaryInventoryRow>,
}

#[derive(Debug, Deserialize)]
struct FallbackBoundaryInventoryRow {
    boundary_id: String,
    statement_kind: String,
    decision_reason: String,
}

static CANONICAL_FALLBACK_BOUNDARY_INVENTORY: LazyLock<Result<FallbackBoundaryInventory, String>> =
    LazyLock::new(|| {
        toml::from_str(include_str!(
            "../../../docs/contracts/fallback_boundary_inventory.toml"
        ))
        .map_err(|error| error.to_string())
    });

/// Fail-closed proof that a run exercised its declared execution requirement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionLaneEvidence {
    pub schema_version: String,
    pub required_lane: ExecutionLane,
    pub observed_lanes: Vec<ObservedExecutionLane>,
    pub trace_id: String,
    pub run_id: String,
    pub scenario_id: String,
    pub statement_kind: String,
    pub backend_kind: String,
    pub backend_mode: String,
    pub backend_identity: String,
    pub fallback_decisions: Vec<FallbackDecisionEvidence>,
    pub fallback_capture_complete: bool,
    pub requirement_satisfied: bool,
    pub first_failure_diagnostic: Option<String>,
}

impl ExecutionLaneEvidence {
    /// Construct canonical evidence and compute its satisfaction result.
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn from_observations(
        required_lane: ExecutionLane,
        mut observed_lanes: Vec<ObservedExecutionLane>,
        trace_id: impl Into<String>,
        run_id: impl Into<String>,
        scenario_id: impl Into<String>,
        statement_kind: impl Into<String>,
        backend_kind: impl Into<String>,
        backend_mode: impl Into<String>,
        backend_identity: impl Into<String>,
        mut fallback_decisions: Vec<FallbackDecisionEvidence>,
        fallback_capture_complete: bool,
    ) -> Self {
        observed_lanes.sort_unstable();
        fallback_decisions.sort_unstable();
        let mut evidence = Self {
            schema_version: EXECUTION_LANE_EVIDENCE_SCHEMA_VERSION.to_owned(),
            required_lane,
            observed_lanes,
            trace_id: trace_id.into(),
            run_id: run_id.into(),
            scenario_id: scenario_id.into(),
            statement_kind: statement_kind.into(),
            backend_kind: backend_kind.into(),
            backend_mode: backend_mode.into(),
            backend_identity: backend_identity.into(),
            fallback_decisions,
            fallback_capture_complete,
            requirement_satisfied: false,
            first_failure_diagnostic: None,
        };
        evidence.requirement_satisfied = evidence.computed_requirement_satisfied();
        if !evidence.requirement_satisfied {
            evidence.first_failure_diagnostic = Some(evidence.mismatch_diagnostic());
        }
        evidence
    }

    /// Minimal evidence for a semantic-only failure bundle outside an engine report.
    #[must_use]
    pub fn semantic_only(
        trace_id: impl Into<String>,
        run_id: impl Into<String>,
        scenario_id: impl Into<String>,
        statement_kind: impl Into<String>,
    ) -> Self {
        Self::from_observations(
            ExecutionLane::SqlResultOnly,
            vec![ObservedExecutionLane::SqlResult],
            trace_id,
            run_id,
            scenario_id,
            statement_kind,
            "unreported",
            "unreported",
            "unreported:unreported",
            Vec::new(),
            true,
        )
    }

    #[must_use]
    fn required_observation(&self) -> ObservedExecutionLane {
        match self.required_lane {
            ExecutionLane::SqlResultOnly => ObservedExecutionLane::SqlResult,
            ExecutionLane::PagerBackedRequired => ObservedExecutionLane::PagerBacked,
            ExecutionLane::PlannerRequired => ObservedExecutionLane::Planner,
            ExecutionLane::VdbeRequired => ObservedExecutionLane::Vdbe,
            ExecutionLane::MvccRequired => ObservedExecutionLane::Mvcc,
            ExecutionLane::RecoveryRequired => ObservedExecutionLane::Recovery,
        }
    }

    #[must_use]
    fn is_fallback_sensitive(&self) -> bool {
        self.required_lane != ExecutionLane::SqlResultOnly
    }

    #[must_use]
    fn computed_requirement_satisfied(&self) -> bool {
        let observed = self.observed_lanes.contains(&self.required_observation());
        let fallback_seen = self
            .observed_lanes
            .contains(&ObservedExecutionLane::CompatibilityFallback)
            || !self.fallback_decisions.is_empty();
        let backend_certifying = !self.is_fallback_sensitive()
            || (self.backend_kind != "memory" && self.backend_mode != "fallback_allowed");
        self.fallback_capture_complete
            && observed
            && backend_certifying
            && !(self.is_fallback_sensitive() && fallback_seen)
    }

    #[must_use]
    fn mismatch_diagnostic(&self) -> String {
        let observed = self
            .observed_lanes
            .iter()
            .map(|lane| lane.label())
            .collect::<Vec<_>>()
            .join(",");
        let fallback_reason = self.fallback_decisions.first().map_or_else(
            || {
                if self.fallback_capture_complete {
                    "none"
                } else {
                    "capture_incomplete"
                }
            },
            |decision| decision.decision_reason.as_str(),
        );
        format!(
            "execution_lane_mismatch: run_id={}; trace_id={}; scenario_id={}; required_lane={}; observed_lanes={observed}; backend_identity={}; fallback_reason={fallback_reason}",
            self.run_id,
            self.trace_id,
            self.scenario_id,
            self.required_lane.label(),
            self.backend_identity,
        )
    }

    /// Validate structural integrity and recompute the fail-closed outcome.
    #[must_use]
    pub fn validate(&self) -> Vec<String> {
        let mut errors = Vec::new();
        if self.schema_version != EXECUTION_LANE_EVIDENCE_SCHEMA_VERSION {
            errors.push(format!(
                "execution_lane.schema_version must equal {EXECUTION_LANE_EVIDENCE_SCHEMA_VERSION}"
            ));
        }
        for (field, value) in [
            ("trace_id", self.trace_id.as_str()),
            ("run_id", self.run_id.as_str()),
            ("scenario_id", self.scenario_id.as_str()),
            ("statement_kind", self.statement_kind.as_str()),
            ("backend_kind", self.backend_kind.as_str()),
            ("backend_mode", self.backend_mode.as_str()),
            ("backend_identity", self.backend_identity.as_str()),
        ] {
            if value.trim().is_empty() {
                errors.push(format!("execution_lane.{field} is empty"));
            }
        }
        if self.backend_identity != format!("{}:{}", self.backend_kind, self.backend_mode) {
            errors.push(
                "execution_lane.backend_identity conflicts with backend kind/mode".to_owned(),
            );
        }
        if self.observed_lanes.is_empty() {
            errors.push("execution_lane.observed_lanes is empty".to_owned());
        }
        if !self
            .observed_lanes
            .windows(2)
            .all(|pair| pair[0] <= pair[1])
        {
            errors.push("execution_lane.observed_lanes is not canonically sorted".to_owned());
        }
        if self
            .observed_lanes
            .windows(2)
            .any(|pair| pair[0] == pair[1])
        {
            errors.push("execution_lane.observed_lanes contains duplicates".to_owned());
        }

        let fallback_observed = self
            .observed_lanes
            .contains(&ObservedExecutionLane::CompatibilityFallback);
        let fallback_evidence_present = !self.fallback_decisions.is_empty();
        if fallback_observed != fallback_evidence_present {
            errors.push(
                "execution_lane compatibility-fallback observation conflicts with fallback decisions"
                    .to_owned(),
            );
        }
        if !self
            .fallback_decisions
            .windows(2)
            .all(|pair| pair[0] <= pair[1])
        {
            errors.push("execution_lane.fallback_decisions is not canonically sorted".to_owned());
        }
        if self
            .fallback_decisions
            .windows(2)
            .any(|pair| pair[0] == pair[1])
        {
            errors.push("execution_lane.fallback_decisions contains duplicates".to_owned());
        }
        for (index, decision) in self.fallback_decisions.iter().enumerate() {
            for (field, value) in [
                ("statement_kind", decision.statement_kind.as_str()),
                ("fallback_boundary", decision.fallback_boundary.as_str()),
                ("decision_reason", decision.decision_reason.as_str()),
                ("decision_outcome", decision.decision_outcome.as_str()),
                ("source_touchpoint", decision.source_touchpoint.as_str()),
                (
                    "first_failure_diagnostic",
                    decision.first_failure_diagnostic.as_str(),
                ),
            ] {
                if value.trim().is_empty() {
                    errors.push(format!(
                        "execution_lane.fallback_decisions[{index}].{field} is empty"
                    ));
                }
            }
            if decision.occurrences == 0 {
                errors.push(format!(
                    "execution_lane.fallback_decisions[{index}].occurrences is zero"
                ));
            }
            if decision.first_failure_diagnostic != decision.canonical_first_failure_diagnostic() {
                errors.push(format!(
                    "execution_lane.fallback_decisions[{index}].first_failure_diagnostic conflicts with structured fields"
                ));
            }
        }
        errors.extend(self.validate_canonical_fallback_registry());

        let computed = self.computed_requirement_satisfied();
        if self.requirement_satisfied != computed {
            errors.push("execution_lane.requirement_satisfied is inconsistent".to_owned());
        }
        match (computed, self.first_failure_diagnostic.as_deref()) {
            (true, Some(_)) => errors.push(
                "execution_lane.first_failure_diagnostic conflicts with satisfied requirement"
                    .to_owned(),
            ),
            (false, None) => errors.push(
                "execution_lane.first_failure_diagnostic missing for lane mismatch".to_owned(),
            ),
            (false, Some(diagnostic)) if diagnostic != self.mismatch_diagnostic() => {
                errors.push(
                    "execution_lane.first_failure_diagnostic does not match canonical mismatch"
                        .to_owned(),
                );
            }
            _ => {}
        }
        errors
    }

    /// Validate observed fallback tuples against the canonical registry.
    #[must_use]
    pub fn validate_canonical_fallback_registry(&self) -> Vec<String> {
        if self.fallback_decisions.is_empty() {
            return Vec::new();
        }
        let inventory = match CANONICAL_FALLBACK_BOUNDARY_INVENTORY.as_ref() {
            Ok(inventory) => inventory,
            Err(error) => {
                return vec![format!(
                    "execution_lane canonical fallback registry is invalid: {error}"
                )];
            }
        };
        if inventory.boundary.is_empty() {
            return vec!["execution_lane canonical fallback registry is empty".to_owned()];
        }
        self.fallback_decisions
            .iter()
            .filter_map(|decision| {
                let fallback_boundary = decision.fallback_boundary.as_str();
                let boundary_registered = inventory.boundary.iter().any(|row| {
                    row.boundary_id == fallback_boundary
                        && row.statement_kind == decision.statement_kind
                        && row.decision_reason == decision.decision_reason
                });
                let outcome_registered = inventory
                    .allowed_decision_outcomes
                    .contains(&decision.decision_outcome);
                (!(boundary_registered && outcome_registered)).then(|| {
                    format!(
                        "execution_lane fallback tuple is absent from canonical registry: boundary={} statement={} reason={} outcome={}",
                        decision.fallback_boundary,
                        decision.statement_kind,
                        decision.decision_reason,
                        decision.decision_outcome,
                    )
                })
            })
            .collect()
    }

    /// Emit one bounded aggregate diagnostic for this run.
    pub fn emit_diagnostic(&self) {
        let observed_lanes = self
            .observed_lanes
            .iter()
            .map(|lane| lane.label())
            .collect::<Vec<_>>()
            .join(",");
        let lane_count = |lane| u8::from(self.observed_lanes.contains(&lane));
        let fallback_reason = self.fallback_decisions.first().map_or_else(
            || {
                if self.fallback_capture_complete {
                    "none"
                } else {
                    "capture_incomplete"
                }
            },
            |decision| decision.decision_reason.as_str(),
        );
        let fallback = self.fallback_decisions.first();
        let fallback_decision_count = self
            .fallback_decisions
            .iter()
            .fold(0_u64, |count, decision| {
                count.saturating_add(decision.occurrences)
            });
        if self.requirement_satisfied {
            tracing::info!(
                target: "fsqlite.execution_lane",
                run_id = %self.run_id,
                trace_id = %self.trace_id,
                scenario_id = %self.scenario_id,
                statement_kind = %self.statement_kind,
                required_lane = self.required_lane.label(),
                observed_lanes = %observed_lanes,
                observed_lane_count = self.observed_lanes.len(),
                sql_result_count = lane_count(ObservedExecutionLane::SqlResult),
                pager_backed_count = lane_count(ObservedExecutionLane::PagerBacked),
                planner_count = lane_count(ObservedExecutionLane::Planner),
                vdbe_count = lane_count(ObservedExecutionLane::Vdbe),
                mvcc_count = lane_count(ObservedExecutionLane::Mvcc),
                recovery_count = lane_count(ObservedExecutionLane::Recovery),
                compatibility_fallback_count = lane_count(ObservedExecutionLane::CompatibilityFallback),
                fallback_decision_count,
                fallback_unique_decision_count = self.fallback_decisions.len(),
                fallback_capture_complete = self.fallback_capture_complete,
                backend_identity = %self.backend_identity,
                backend_mode = %self.backend_mode,
                fallback_boundary = fallback.map_or("none", |value| value.fallback_boundary.as_str()),
                fallback_reason,
                fallback_outcome = fallback.map_or("none", |value| value.decision_outcome.as_str()),
                first_failure_diag = "none",
                "execution-lane evidence aggregate"
            );
        } else {
            tracing::error!(
                target: "fsqlite.execution_lane",
                run_id = %self.run_id,
                trace_id = %self.trace_id,
                scenario_id = %self.scenario_id,
                statement_kind = %self.statement_kind,
                required_lane = self.required_lane.label(),
                observed_lanes = %observed_lanes,
                observed_lane_count = self.observed_lanes.len(),
                sql_result_count = lane_count(ObservedExecutionLane::SqlResult),
                pager_backed_count = lane_count(ObservedExecutionLane::PagerBacked),
                planner_count = lane_count(ObservedExecutionLane::Planner),
                vdbe_count = lane_count(ObservedExecutionLane::Vdbe),
                mvcc_count = lane_count(ObservedExecutionLane::Mvcc),
                recovery_count = lane_count(ObservedExecutionLane::Recovery),
                compatibility_fallback_count = lane_count(ObservedExecutionLane::CompatibilityFallback),
                fallback_decision_count,
                fallback_unique_decision_count = self.fallback_decisions.len(),
                fallback_capture_complete = self.fallback_capture_complete,
                backend_identity = %self.backend_identity,
                backend_mode = %self.backend_mode,
                fallback_boundary = fallback.map_or("none", |value| value.fallback_boundary.as_str()),
                fallback_reason,
                fallback_outcome = fallback.map_or("none", |value| value.decision_outcome.as_str()),
                first_failure_diag = %self.first_failure_diagnostic.as_deref().unwrap_or("none"),
                "execution-lane requirement mismatch"
            );
        }
    }
}

// ─── Failure Types ──────────────────────────────────────────────────────

/// Classification of the failure mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum FailureType {
    /// A test assertion failed (expected != actual).
    Assertion,
    /// An unexpected panic occurred.
    Panic,
    /// Differential divergence between fsqlite and reference (rusqlite).
    Divergence,
    /// Test exceeded its timeout budget.
    Timeout,
    /// SSI serialization conflict during concurrent execution.
    SsiConflict,
    /// MVCC version chain corruption or invariant violation.
    MvccInvariant,
    /// WAL/checkpoint/recovery failure.
    WalRecovery,
    /// File format or compatibility issue.
    FileFormat,
    /// Extension-specific failure (JSON, FTS, R-tree, etc.).
    Extension,
    /// Unclassified or unknown failure mode.
    Other,
}

impl FailureType {
    /// Human-readable label for display.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::Assertion => "assertion",
            Self::Panic => "panic",
            Self::Divergence => "divergence",
            Self::Timeout => "timeout",
            Self::SsiConflict => "ssi-conflict",
            Self::MvccInvariant => "mvcc-invariant",
            Self::WalRecovery => "wal-recovery",
            Self::FileFormat => "file-format",
            Self::Extension => "extension",
            Self::Other => "other",
        }
    }
}

impl std::fmt::Display for FailureType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.label())
    }
}

// ─── Environment Info ───────────────────────────────────────────────────

/// Environment metadata captured at failure time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnvironmentInfo {
    /// Git commit SHA (short or full).
    pub git_sha: String,
    /// Rust toolchain version (e.g., `nightly-2026-02-10`).
    pub toolchain: String,
    /// Platform triple (e.g., `x86_64-unknown-linux-gnu`).
    pub platform: String,
    /// Cargo feature flags active during the build.
    pub feature_flags: Vec<String>,
    /// Additional environment key-value pairs.
    pub extra: BTreeMap<String, String>,
}

impl EnvironmentInfo {
    /// Create a minimal environment info with required fields.
    #[must_use]
    pub fn new(git_sha: &str, toolchain: &str, platform: &str) -> Self {
        Self {
            git_sha: git_sha.to_owned(),
            toolchain: toolchain.to_owned(),
            platform: platform.to_owned(),
            feature_flags: Vec::new(),
            extra: BTreeMap::new(),
        }
    }
}

// ─── Scenario Info ──────────────────────────────────────────────────────

/// Scenario metadata linking the failure to the traceability matrix.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScenarioInfo {
    /// Scenario ID from the traceability matrix (e.g., `MVCC-3`).
    pub scenario_id: String,
    /// Bead ID owning this test.
    pub bead_id: String,
    /// Human-readable test name or path.
    pub test_name: String,
    /// Script path (workspace-relative) that produced this failure.
    pub script_path: Option<String>,
}

// ─── Reproducibility Info ───────────────────────────────────────────────

/// Information needed to reproduce the failure.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReproducibilityInfo {
    /// Deterministic seed used for this run.
    pub seed: Option<u64>,
    /// Fixture ID used (if applicable).
    pub fixture_id: Option<String>,
    /// Schedule fingerprint for concurrent tests.
    pub schedule_fingerprint: Option<String>,
    /// Exact command to reproduce this failure.
    pub repro_command: String,
    /// Storage mode (in-memory, file-backed, WAL, rollback-journal).
    pub storage_mode: Option<String>,
    /// Concurrency mode (sequential, concurrent-writers, MVCC, SSI).
    pub concurrency_mode: Option<String>,
}

// ─── Failure Info ───────────────────────────────────────────────────────

/// Details about the failure itself.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FailureInfo {
    /// Classification of the failure mode.
    pub failure_type: FailureType,
    /// The failure message (panic message, assertion text, etc.).
    pub message: String,
    /// Expected value (for assertion/divergence failures).
    pub expected: Option<String>,
    /// Actual value (for assertion/divergence failures).
    pub actual: Option<String>,
    /// Line-based diff between expected and actual (if applicable).
    pub diff: Option<String>,
    /// Spec invariant reference (e.g., `INV-1`, `§5.3.2`).
    pub invariant: Option<String>,
    /// First-divergence marker (for differential tests).
    pub first_divergence: Option<FirstDivergence>,
}

/// Marker for the first point of divergence in differential testing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FirstDivergence {
    /// The SQL statement or operation index where divergence was first detected.
    pub operation_index: u64,
    /// The SQL statement that produced the divergence.
    pub sql: Option<String>,
    /// Phase when the divergence was detected.
    pub phase: Option<String>,
}

// ─── Artifact Info ──────────────────────────────────────────────────────

/// A collected artifact (log file, DB snapshot, etc.).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactEntry {
    /// Descriptive label for the artifact (e.g., `db-snapshot`, `wal-log`).
    pub label: String,
    /// Path relative to the bundle root.
    pub path: String,
    /// SHA-256 hash of the artifact contents.
    pub sha256: String,
    /// Size in bytes.
    pub size_bytes: u64,
}

// ─── Failure Bundle ─────────────────────────────────────────────────────

/// A complete failure triage package.
///
/// Contains all context needed to understand, reproduce, and fix a test
/// failure. Designed for both human operators and automated triage tools.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FailureBundle {
    /// Schema version for direct, fail-closed schema evolution.
    pub schema_version: String,
    /// Unique bundle identifier (format: `fb-{run_id}-{seq}`).
    pub bundle_id: String,
    /// ISO 8601 timestamp (UTC) when the bundle was created.
    pub created_at: String,
    /// Run ID correlating with E2E log events.
    pub run_id: String,
    /// Required and positively observed engine execution paths.
    pub execution_lane_evidence: ExecutionLaneEvidence,
    /// Scenario and test metadata.
    pub scenario: ScenarioInfo,
    /// Failure details.
    pub failure: FailureInfo,
    /// Reproducibility data.
    pub reproducibility: ReproducibilityInfo,
    /// Environment at failure time.
    pub environment: EnvironmentInfo,
    /// Collected artifacts (snapshots, logs, diffs).
    pub artifacts: Vec<ArtifactEntry>,
    /// State snapshots as key-value pairs (compact representations).
    pub state_snapshots: BTreeMap<String, String>,
    /// Triage tags for automated classification.
    pub triage_tags: Vec<String>,
    /// Deterministic content hash over canonical payload (excludes human labels).
    pub content_hash: String,
}

impl FailureBundle {
    /// JSON pointer linking to first-failure diagnostics when available.
    #[must_use]
    pub fn first_failure_diagnostics_pointer(&self) -> Option<&'static str> {
        self.failure
            .first_divergence
            .as_ref()
            .map(|_| FIRST_FAILURE_DIAGNOSTIC_POINTER)
    }

    /// Deterministic hash over canonical replay-relevant bundle content.
    ///
    /// Human labels (`bundle_id`, `created_at`) are intentionally excluded so
    /// logically identical failures share the same identity hash.
    #[must_use]
    pub fn deterministic_bundle_hash(&self) -> String {
        #[derive(Serialize)]
        struct CanonicalReproducibility<'a> {
            seed: Option<u64>,
            fixture_id: &'a Option<String>,
            schedule_fingerprint: &'a Option<String>,
            repro_command: &'a str,
            storage_mode: &'a Option<String>,
            concurrency_mode: &'a Option<String>,
        }

        #[derive(Serialize)]
        struct CanonicalFailureBundle<'a> {
            schema_version: &'a str,
            run_id: &'a str,
            execution_lane_evidence: &'a ExecutionLaneEvidence,
            scenario: &'a ScenarioInfo,
            failure: &'a FailureInfo,
            first_failure_diagnostics_pointer: Option<&'a str>,
            reproducibility: CanonicalReproducibility<'a>,
            environment: &'a EnvironmentInfo,
            artifacts: &'a [ArtifactEntry],
            state_snapshots: &'a BTreeMap<String, String>,
            triage_tags: &'a [String],
        }

        let canonical = CanonicalFailureBundle {
            schema_version: &self.schema_version,
            run_id: &self.run_id,
            execution_lane_evidence: &self.execution_lane_evidence,
            scenario: &self.scenario,
            failure: &self.failure,
            first_failure_diagnostics_pointer: self.first_failure_diagnostics_pointer(),
            reproducibility: CanonicalReproducibility {
                seed: self.reproducibility.seed,
                fixture_id: &self.reproducibility.fixture_id,
                schedule_fingerprint: &self.reproducibility.schedule_fingerprint,
                repro_command: self.reproducibility.repro_command.as_str(),
                storage_mode: &self.reproducibility.storage_mode,
                concurrency_mode: &self.reproducibility.concurrency_mode,
            },
            environment: &self.environment,
            artifacts: &self.artifacts,
            state_snapshots: &self.state_snapshots,
            triage_tags: &self.triage_tags,
        };
        let payload = serde_json::to_string(&canonical)
            .expect("failure bundle canonical serialization must not fail");
        sha256_hex(payload.as_bytes())
    }

    /// Validate that all required fields are populated.
    ///
    /// Returns a list of validation errors (empty if valid).
    #[must_use]
    pub fn validate(&self) -> Vec<String> {
        let mut errors = Vec::new();

        if self.schema_version.trim().is_empty() {
            errors.push("schema_version is empty".to_owned());
        }
        if self.bundle_id.trim().is_empty() {
            errors.push("bundle_id is empty".to_owned());
        }
        if self.created_at.trim().is_empty() {
            errors.push("created_at is empty".to_owned());
        }
        if self.run_id.trim().is_empty() {
            errors.push("run_id is empty".to_owned());
        }
        errors.extend(self.execution_lane_evidence.validate());
        if self.execution_lane_evidence.run_id != self.run_id {
            errors.push("execution_lane.run_id does not match bundle run_id".to_owned());
        }
        if self.execution_lane_evidence.scenario_id != self.scenario.scenario_id {
            errors.push(
                "execution_lane.scenario_id does not match bundle scenario.scenario_id".to_owned(),
            );
        }
        if self.scenario.scenario_id.trim().is_empty() {
            errors.push("scenario.scenario_id is empty".to_owned());
        }
        if self.scenario.bead_id.trim().is_empty() {
            errors.push("scenario.bead_id is empty".to_owned());
        }
        if self.scenario.test_name.trim().is_empty() {
            errors.push("scenario.test_name is empty".to_owned());
        }
        if self
            .scenario
            .script_path
            .as_ref()
            .is_some_and(|path| path.trim().is_empty())
        {
            errors.push("scenario.script_path must be non-empty when present".to_owned());
        }
        if self.failure.message.trim().is_empty() {
            errors.push("failure.message is empty".to_owned());
        }
        if normalize_one_command(&self.reproducibility.repro_command).is_none() {
            errors.push(
                "reproducibility.repro_command must be non-empty, single-line command".to_owned(),
            );
        }
        if self.environment.git_sha.trim().is_empty() {
            errors.push("environment.git_sha is empty".to_owned());
        }
        if !is_sha256_hex_64(&self.content_hash) {
            errors.push("content_hash must be 64 lowercase hex chars".to_owned());
        }
        if self.content_hash != self.deterministic_bundle_hash() {
            errors.push("content_hash does not match canonical bundle payload".to_owned());
        }

        // Validate artifact entries
        for (i, art) in self.artifacts.iter().enumerate() {
            if art.label.trim().is_empty() {
                errors.push(format!("artifacts[{i}].label is empty"));
            }
            if art.path.trim().is_empty() {
                errors.push(format!("artifacts[{i}].path is empty"));
            }
            if art.sha256.trim().is_empty() {
                errors.push(format!("artifacts[{i}].sha256 is empty"));
            }
        }

        errors
    }

    /// Compact one-line summary for log output and index listings.
    #[must_use]
    pub fn summary_line(&self) -> String {
        let seed_str = self
            .reproducibility
            .seed
            .map_or_else(String::new, |s| format!(" seed=0x{s:016X}"));
        format!(
            "[{}] {} {} ({}){seed_str}",
            self.failure.failure_type.label(),
            self.scenario.scenario_id,
            self.scenario.test_name,
            self.bundle_id,
        )
    }
}

// ─── Builder ────────────────────────────────────────────────────────────

/// Fluent builder for constructing failure bundles.
///
/// Ensures all required fields are set before building, and fills in
/// defaults for optional fields.
pub struct FailureBundleBuilder {
    bundle_id: Option<String>,
    created_at: Option<String>,
    run_id: Option<String>,
    execution_lane_evidence: Option<ExecutionLaneEvidence>,
    scenario: Option<ScenarioInfo>,
    failure: Option<FailureInfo>,
    reproducibility: Option<ReproducibilityInfo>,
    environment: Option<EnvironmentInfo>,
    artifacts: Vec<ArtifactEntry>,
    state_snapshots: BTreeMap<String, String>,
    triage_tags: Vec<String>,
}

impl FailureBundleBuilder {
    /// Create a new builder.
    #[must_use]
    pub fn new() -> Self {
        Self {
            bundle_id: None,
            created_at: None,
            run_id: None,
            execution_lane_evidence: None,
            scenario: None,
            failure: None,
            reproducibility: None,
            environment: None,
            artifacts: Vec::new(),
            state_snapshots: BTreeMap::new(),
            triage_tags: Vec::new(),
        }
    }

    /// Set the bundle ID.
    #[must_use]
    pub fn bundle_id(mut self, id: &str) -> Self {
        self.bundle_id = Some(id.to_owned());
        self
    }

    /// Set the creation timestamp (ISO 8601 UTC).
    #[must_use]
    pub fn created_at(mut self, ts: &str) -> Self {
        self.created_at = Some(ts.to_owned());
        self
    }

    /// Set the run ID for log correlation.
    #[must_use]
    pub fn run_id(mut self, rid: &str) -> Self {
        self.run_id = Some(rid.to_owned());
        self
    }

    /// Set fail-closed execution-lane evidence for this failure.
    #[must_use]
    pub fn execution_lane_evidence(mut self, evidence: ExecutionLaneEvidence) -> Self {
        self.execution_lane_evidence = Some(evidence);
        self
    }

    /// Set scenario metadata.
    #[must_use]
    pub fn scenario(mut self, scenario: ScenarioInfo) -> Self {
        self.scenario = Some(scenario);
        self
    }

    /// Set failure details.
    #[must_use]
    pub fn failure(mut self, failure: FailureInfo) -> Self {
        self.failure = Some(failure);
        self
    }

    /// Set reproducibility data.
    #[must_use]
    pub fn reproducibility(mut self, repro: ReproducibilityInfo) -> Self {
        self.reproducibility = Some(repro);
        self
    }

    /// Set environment metadata.
    #[must_use]
    pub fn environment(mut self, env: EnvironmentInfo) -> Self {
        self.environment = Some(env);
        self
    }

    /// Add an artifact entry.
    #[must_use]
    pub fn artifact(mut self, entry: ArtifactEntry) -> Self {
        self.artifacts.push(entry);
        self
    }

    /// Add a state snapshot key-value pair.
    #[must_use]
    pub fn state_snapshot(mut self, key: &str, value: &str) -> Self {
        self.state_snapshots
            .insert(key.to_owned(), value.to_owned());
        self
    }

    /// Add a triage tag.
    #[must_use]
    pub fn triage_tag(mut self, tag: &str) -> Self {
        self.triage_tags.push(tag.to_owned());
        self
    }

    /// Build the failure bundle, returning an error if required fields are missing.
    ///
    /// # Errors
    ///
    /// Returns `Err` with a description of the missing field(s).
    pub fn build(self) -> Result<FailureBundle, String> {
        let bundle_id = self.bundle_id.ok_or("bundle_id is required")?;
        let created_at = self.created_at.ok_or("created_at is required")?;
        let run_id = self.run_id.ok_or("run_id is required")?;
        let execution_lane_evidence = self
            .execution_lane_evidence
            .ok_or("execution_lane_evidence is required")?;
        let scenario = self.scenario.ok_or("scenario is required")?;
        let failure = self.failure.ok_or("failure is required")?;
        let mut reproducibility = self.reproducibility.ok_or("reproducibility is required")?;
        reproducibility.repro_command = normalize_one_command(&reproducibility.repro_command)
            .ok_or(
            "reproducibility.repro_command is required and must be non-empty, single-line command",
        )?;
        let environment = self.environment.ok_or("environment is required")?;

        let mut bundle = FailureBundle {
            schema_version: BUNDLE_SCHEMA_VERSION.to_owned(),
            bundle_id,
            created_at,
            run_id,
            execution_lane_evidence,
            scenario,
            failure,
            reproducibility,
            environment,
            artifacts: self.artifacts,
            state_snapshots: self.state_snapshots,
            triage_tags: self.triage_tags,
            content_hash: String::new(),
        };
        bundle.content_hash = bundle.deterministic_bundle_hash();
        let errors = bundle.validate();
        if errors.is_empty() {
            Ok(bundle)
        } else {
            Err(format!("invalid failure bundle: {}", errors.join("; ")))
        }
    }
}

impl Default for FailureBundleBuilder {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Convenience Constructors ───────────────────────────────────────────

/// Build a failure bundle for a simple assertion failure.
///
/// # Errors
///
/// Returns `Err` if any required field is empty.
#[allow(clippy::too_many_arguments)]
pub fn bundle_assertion_failure(
    bundle_id: &str,
    run_id: &str,
    created_at: &str,
    bead_id: &str,
    test_name: &str,
    scenario_id: &str,
    expected: &str,
    actual: &str,
    repro_command: &str,
    env: EnvironmentInfo,
) -> Result<FailureBundle, String> {
    use crate::test_diagnostics::simple_diff;

    let diff = simple_diff(expected, actual);

    FailureBundleBuilder::new()
        .bundle_id(bundle_id)
        .run_id(run_id)
        .execution_lane_evidence(ExecutionLaneEvidence::semantic_only(
            "unreported",
            run_id,
            scenario_id,
            "assertion",
        ))
        .created_at(created_at)
        .scenario(ScenarioInfo {
            scenario_id: scenario_id.to_owned(),
            bead_id: bead_id.to_owned(),
            test_name: test_name.to_owned(),
            script_path: None,
        })
        .failure(FailureInfo {
            failure_type: FailureType::Assertion,
            message: "assertion failed: expected != actual".to_owned(),
            expected: Some(expected.to_owned()),
            actual: Some(actual.to_owned()),
            diff,
            invariant: None,
            first_divergence: None,
        })
        .reproducibility(ReproducibilityInfo {
            seed: None,
            fixture_id: None,
            schedule_fingerprint: None,
            repro_command: repro_command.to_owned(),
            storage_mode: None,
            concurrency_mode: None,
        })
        .environment(env)
        .triage_tag("assertion")
        .build()
}

/// Build a failure bundle for a differential divergence.
///
/// # Errors
///
/// Returns `Err` if any required field is empty.
#[allow(clippy::too_many_arguments)]
pub fn bundle_divergence_failure(
    bundle_id: &str,
    run_id: &str,
    created_at: &str,
    bead_id: &str,
    test_name: &str,
    scenario_id: &str,
    divergence: FirstDivergence,
    expected: &str,
    actual: &str,
    seed: u64,
    repro_command: &str,
    env: EnvironmentInfo,
) -> Result<FailureBundle, String> {
    use crate::test_diagnostics::simple_diff;

    let diff = simple_diff(expected, actual);

    FailureBundleBuilder::new()
        .bundle_id(bundle_id)
        .run_id(run_id)
        .execution_lane_evidence(ExecutionLaneEvidence::semantic_only(
            "unreported",
            run_id,
            scenario_id,
            "differential",
        ))
        .created_at(created_at)
        .scenario(ScenarioInfo {
            scenario_id: scenario_id.to_owned(),
            bead_id: bead_id.to_owned(),
            test_name: test_name.to_owned(),
            script_path: None,
        })
        .failure(FailureInfo {
            failure_type: FailureType::Divergence,
            message: format!("divergence at operation {}", divergence.operation_index),
            expected: Some(expected.to_owned()),
            actual: Some(actual.to_owned()),
            diff,
            invariant: None,
            first_divergence: Some(divergence),
        })
        .reproducibility(ReproducibilityInfo {
            seed: Some(seed),
            fixture_id: None,
            schedule_fingerprint: None,
            repro_command: repro_command.to_owned(),
            storage_mode: None,
            concurrency_mode: None,
        })
        .environment(env)
        .triage_tag("divergence")
        .triage_tag("differential")
        .build()
}

// ─── Triage Index ───────────────────────────────────────────────────────

/// In-memory index over a collection of failure bundles.
///
/// Supports queries by scenario ID, bead ID, seed, failure type, and
/// triage tag for rapid operator navigation.
#[derive(Debug, Clone, Default)]
pub struct BundleIndex {
    bundles: Vec<FailureBundle>,
}

impl BundleIndex {
    /// Create a new empty index.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a bundle to the index.
    pub fn insert(&mut self, bundle: FailureBundle) {
        self.bundles.push(bundle);
    }

    /// Total number of bundles.
    #[must_use]
    pub fn len(&self) -> usize {
        self.bundles.len()
    }

    /// Whether the index is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.bundles.is_empty()
    }

    /// All bundles in insertion order.
    #[must_use]
    pub fn all(&self) -> &[FailureBundle] {
        &self.bundles
    }

    /// Find bundles by scenario ID.
    #[must_use]
    pub fn by_scenario(&self, scenario_id: &str) -> Vec<&FailureBundle> {
        self.bundles
            .iter()
            .filter(|b| b.scenario.scenario_id == scenario_id)
            .collect()
    }

    /// Find bundles by bead ID.
    #[must_use]
    pub fn by_bead(&self, bead_id: &str) -> Vec<&FailureBundle> {
        self.bundles
            .iter()
            .filter(|b| b.scenario.bead_id == bead_id)
            .collect()
    }

    /// Find bundles by seed value.
    #[must_use]
    pub fn by_seed(&self, seed: u64) -> Vec<&FailureBundle> {
        self.bundles
            .iter()
            .filter(|b| b.reproducibility.seed == Some(seed))
            .collect()
    }

    /// Find bundles by failure type.
    #[must_use]
    pub fn by_failure_type(&self, ft: FailureType) -> Vec<&FailureBundle> {
        self.bundles
            .iter()
            .filter(|b| b.failure.failure_type == ft)
            .collect()
    }

    /// Find bundles that have a specific triage tag.
    #[must_use]
    pub fn by_triage_tag(&self, tag: &str) -> Vec<&FailureBundle> {
        self.bundles
            .iter()
            .filter(|b| b.triage_tags.iter().any(|t| t == tag))
            .collect()
    }

    /// Produce a per-failure-type summary of the index contents.
    #[must_use]
    pub fn type_summary(&self) -> BTreeMap<String, usize> {
        let mut counts = BTreeMap::new();
        for bundle in &self.bundles {
            *counts
                .entry(bundle.failure.failure_type.label().to_owned())
                .or_insert(0) += 1;
        }
        counts
    }

    /// Produce a per-scenario summary of the index contents.
    #[must_use]
    pub fn scenario_summary(&self) -> BTreeMap<String, usize> {
        let mut counts = BTreeMap::new();
        for bundle in &self.bundles {
            *counts
                .entry(bundle.scenario.scenario_id.clone())
                .or_insert(0) += 1;
        }
        counts
    }

    /// Render a compact triage report for operator review.
    #[must_use]
    pub fn render_triage_report(&self) -> String {
        use std::fmt::Write;

        if self.bundles.is_empty() {
            return "Triage Report: 0 failures (all clear)".to_owned();
        }

        let mut out = String::new();
        let _ = writeln!(out, "Triage Report: {} failure(s)", self.bundles.len());
        let _ = writeln!(out, "---");

        // Type breakdown
        let type_counts = self.type_summary();
        let _ = writeln!(out, "By type:");
        for (ty, count) in &type_counts {
            let _ = writeln!(out, "  {ty}: {count}");
        }

        // Scenario breakdown
        let scenario_counts = self.scenario_summary();
        let _ = writeln!(out, "By scenario:");
        for (sid, count) in &scenario_counts {
            let _ = writeln!(out, "  {sid}: {count}");
        }

        let _ = writeln!(out, "---");
        let _ = writeln!(out, "Details:");
        for (i, bundle) in self.bundles.iter().enumerate() {
            let _ = writeln!(out, "  {}. {}", i + 1, bundle.summary_line());
        }

        out
    }
}

// ─── E2E Adoption Checklist ─────────────────────────────────────────────

/// An item in the E2E failure bundling adoption checklist.
#[derive(Debug, Clone)]
pub struct E2eAdoptionItem {
    /// Short identifier (E-1 through E-10).
    pub id: String,
    /// What E2E test code should do.
    pub requirement: String,
    /// Example of compliant usage.
    pub example: String,
}

/// Build the E2E failure bundling adoption checklist.
///
/// Complements the unit-test diagnostics contract (D-1 through D-8) with
/// E2E-specific requirements for structured failure output.
#[must_use]
#[allow(clippy::literal_string_with_formatting_args)]
pub fn build_e2e_adoption_checklist() -> Vec<E2eAdoptionItem> {
    vec![
        E2eAdoptionItem {
            id: "E-1".to_owned(),
            requirement: "Every E2E test emits a run_id in log events for cross-correlation"
                .to_owned(),
            example: r#"let run_id = format!("{bead_id}-{ts}-{pid}");"#.to_owned(),
        },
        E2eAdoptionItem {
            id: "E-2".to_owned(),
            requirement: "On failure, create a FailureBundle with scenario_id and bead_id"
                .to_owned(),
            example: r#"let bundle = FailureBundleBuilder::new()
    .scenario(ScenarioInfo { scenario_id: "MVCC-3".into(), ... })
    .build()?;"#
                .to_owned(),
        },
        E2eAdoptionItem {
            id: "E-3".to_owned(),
            requirement: "Include seed and fixture_id in reproducibility for all deterministic tests"
                .to_owned(),
            example: r".reproducibility(ReproducibilityInfo { seed: Some(0xCAFE), .. })"
                .to_owned(),
        },
        E2eAdoptionItem {
            id: "E-4".to_owned(),
            requirement: "Include a repro_command that exactly reproduces the failure".to_owned(),
            example: r#"repro_command: "cargo test -p fsqlite-e2e -- mvcc_3 --exact --nocapture".into()"#
                .to_owned(),
        },
        E2eAdoptionItem {
            id: "E-5".to_owned(),
            requirement: "Differential tests record FirstDivergence with operation index and SQL"
                .to_owned(),
            example: r#"first_divergence: Some(FirstDivergence { operation_index: 42, sql: Some("SELECT ...".into()), phase: Some("execute".into()) })"#.to_owned(),
        },
        E2eAdoptionItem {
            id: "E-6".to_owned(),
            requirement: "Capture environment metadata (git_sha, toolchain, platform) in every bundle".to_owned(),
            example: r#"EnvironmentInfo::new("abc1234", "nightly-2026-02-10", "x86_64-unknown-linux-gnu")"#.to_owned(),
        },
        E2eAdoptionItem {
            id: "E-7".to_owned(),
            requirement: "Hash and register all collected artifacts (DB snapshots, WAL files, logs)".to_owned(),
            example: r#".artifact(ArtifactEntry { label: "db-snapshot".into(), path: "bundle/test.db".into(), sha256: hash, size_bytes: 4096 })"#.to_owned(),
        },
        E2eAdoptionItem {
            id: "E-8".to_owned(),
            requirement: "Add triage_tags for automated classification (e.g., 'assertion', 'divergence', 'flaky')".to_owned(),
            example: r#".triage_tag("divergence").triage_tag("differential")"#.to_owned(),
        },
        E2eAdoptionItem {
            id: "E-9".to_owned(),
            requirement: "Use state_snapshots for compact page/WAL/MVCC state at failure time".to_owned(),
            example: r#".state_snapshot("wal_frame_count", "42").state_snapshot("page_header", "[0D 00 ...]")"#.to_owned(),
        },
        E2eAdoptionItem {
            id: "E-10".to_owned(),
            requirement: "Validate every bundle with .validate() before persisting or indexing".to_owned(),
            example: r#"let errors = bundle.validate();
assert!(errors.is_empty(), "bundle validation failed: {errors:?}");"#.to_owned(),
        },
    ]
}

fn normalize_one_command(command: &str) -> Option<String> {
    let normalized = command.trim();
    if normalized.is_empty() || normalized.contains('\n') || normalized.contains('\r') {
        return None;
    }
    Some(normalized.to_owned())
}

fn is_sha256_hex_64(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn sha256_hex(data: &[u8]) -> String {
    let digest = Sha256::digest(data);
    let mut hex = String::with_capacity(64);
    for byte in digest {
        let _ = write!(hex, "{byte:02x}");
    }
    hex
}

// ─── Tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    use proptest::prelude::*;

    fn sample_env() -> EnvironmentInfo {
        EnvironmentInfo::new("abc1234", "nightly-2026-02-10", "x86_64-unknown-linux-gnu")
    }

    fn sample_scenario() -> ScenarioInfo {
        ScenarioInfo {
            scenario_id: "MVCC-3".to_owned(),
            bead_id: "bd-test".to_owned(),
            test_name: "test_mvcc_concurrent_write".to_owned(),
            script_path: Some("crates/fsqlite-e2e/tests/mvcc.rs".to_owned()),
        }
    }

    fn sample_failure() -> FailureInfo {
        FailureInfo {
            failure_type: FailureType::Assertion,
            message: "assertion failed: row count mismatch".to_owned(),
            expected: Some("10".to_owned()),
            actual: Some("9".to_owned()),
            diff: None,
            invariant: Some("INV-MVCC-1".to_owned()),
            first_divergence: None,
        }
    }

    fn sample_repro() -> ReproducibilityInfo {
        ReproducibilityInfo {
            seed: Some(0xDEAD_BEEF),
            fixture_id: Some("concurrent-writes-10".to_owned()),
            schedule_fingerprint: Some("sha256:abc123".to_owned()),
            repro_command:
                "cargo test -p fsqlite-e2e -- test_mvcc_concurrent_write --exact --nocapture"
                    .to_owned(),
            storage_mode: Some("file-backed".to_owned()),
            concurrency_mode: Some("concurrent-writers".to_owned()),
        }
    }

    fn sample_lane(run_id: &str, scenario_id: &str) -> ExecutionLaneEvidence {
        ExecutionLaneEvidence::semantic_only("trace-test", run_id, scenario_id, "test")
    }

    fn lane_evidence(
        required: ExecutionLane,
        observed: Vec<ObservedExecutionLane>,
    ) -> ExecutionLaneEvidence {
        ExecutionLaneEvidence::from_observations(
            required,
            observed,
            "trace-test",
            "run-test",
            "scenario-test",
            "test",
            "unix",
            "parity_cert_strict",
            "unix:parity_cert_strict",
            Vec::new(),
            true,
        )
    }

    fn sample_fallback() -> FallbackDecisionEvidence {
        FallbackDecisionEvidence {
            statement_kind: "select".to_owned(),
            fallback_boundary: "conn.select.group_by_fallback".to_owned(),
            decision_reason: "group_by_fallback".to_owned(),
            decision_outcome: "denied".to_owned(),
            source_touchpoint: "execute_statement_dispatch".to_owned(),
            first_failure_diagnostic: "statement_kind=select; fallback_boundary=conn.select.group_by_fallback; source_touchpoint=execute_statement_dispatch; decision_reason=group_by_fallback".to_owned(),
            occurrences: 1,
        }
    }

    fn sample_bundle_with_labels(bundle_id: &str, created_at: &str) -> FailureBundle {
        FailureBundleBuilder::new()
            .bundle_id(bundle_id)
            .created_at(created_at)
            .run_id("bd-test-20260213-1234")
            .execution_lane_evidence(sample_lane("bd-test-20260213-1234", "MVCC-3"))
            .scenario(sample_scenario())
            .failure(sample_failure())
            .reproducibility(sample_repro())
            .environment(sample_env())
            .artifact(ArtifactEntry {
                label: "db-snapshot".to_owned(),
                path: "bundle/test.db".to_owned(),
                sha256: "sha256:deadbeef".to_owned(),
                size_bytes: 4096,
            })
            .state_snapshot("wal_frames", "12")
            .state_snapshot("page_count", "5")
            .triage_tag("assertion")
            .triage_tag("mvcc")
            .build()
            .expect("sample bundle should build")
    }

    fn sample_bundle() -> FailureBundle {
        sample_bundle_with_labels("fb-run001-1", "2026-02-13T06:00:00Z")
    }

    // ── Schema version ──────────────────────────────────────────────

    #[test]
    fn schema_version_is_semver() {
        let parts: Vec<&str> = BUNDLE_SCHEMA_VERSION.split('.').collect();
        assert_eq!(
            parts.len(),
            3,
            "bead_id={BEAD_ID} case=schema_version_semver"
        );
        for p in parts {
            assert!(
                p.parse::<u32>().is_ok(),
                "bead_id={BEAD_ID} case=schema_version_numeric part={p}"
            );
        }
    }

    #[test]
    fn execution_lane_match_matrix_is_fail_closed() {
        let requirements = [
            ExecutionLane::SqlResultOnly,
            ExecutionLane::PagerBackedRequired,
            ExecutionLane::PlannerRequired,
            ExecutionLane::VdbeRequired,
            ExecutionLane::MvccRequired,
            ExecutionLane::RecoveryRequired,
        ];
        let observations = [
            ObservedExecutionLane::SqlResult,
            ObservedExecutionLane::PagerBacked,
            ObservedExecutionLane::Planner,
            ObservedExecutionLane::Vdbe,
            ObservedExecutionLane::Mvcc,
            ObservedExecutionLane::Recovery,
        ];

        for (required_index, required) in requirements.into_iter().enumerate() {
            for (observed_index, observed) in observations.into_iter().enumerate() {
                let evidence = lane_evidence(required, vec![observed]);
                assert_eq!(
                    evidence.requirement_satisfied,
                    required_index == observed_index,
                    "required={} observed={}",
                    required.label(),
                    observed.label(),
                );
                assert!(evidence.validate().is_empty(), "{evidence:?}");
            }
        }
    }

    #[test]
    fn storage_requirements_reject_any_compatibility_fallback() {
        for required in [
            ExecutionLane::PagerBackedRequired,
            ExecutionLane::PlannerRequired,
            ExecutionLane::VdbeRequired,
            ExecutionLane::MvccRequired,
            ExecutionLane::RecoveryRequired,
        ] {
            let mut evidence = ExecutionLaneEvidence::from_observations(
                required,
                vec![
                    match required {
                        ExecutionLane::PagerBackedRequired => ObservedExecutionLane::PagerBacked,
                        ExecutionLane::PlannerRequired => ObservedExecutionLane::Planner,
                        ExecutionLane::VdbeRequired => ObservedExecutionLane::Vdbe,
                        ExecutionLane::MvccRequired => ObservedExecutionLane::Mvcc,
                        ExecutionLane::RecoveryRequired => ObservedExecutionLane::Recovery,
                        ExecutionLane::SqlResultOnly => unreachable!(),
                    },
                    ObservedExecutionLane::CompatibilityFallback,
                ],
                "trace-test",
                "run-test",
                "scenario-test",
                "test",
                "unix",
                "parity_cert_strict",
                "unix:parity_cert_strict",
                vec![sample_fallback()],
                true,
            );
            assert!(
                !evidence.requirement_satisfied,
                "required={}",
                required.label()
            );
            assert!(evidence.validate().is_empty(), "{evidence:?}");
            evidence.first_failure_diagnostic = None;
            assert!(
                evidence
                    .validate()
                    .iter()
                    .any(|error| error.contains("first_failure_diagnostic missing"))
            );
        }
    }

    #[test]
    fn execution_lane_validation_rejects_duplicates_conflicts_and_registry_drift() {
        let mut duplicate = lane_evidence(
            ExecutionLane::VdbeRequired,
            vec![ObservedExecutionLane::Vdbe, ObservedExecutionLane::Vdbe],
        );
        assert!(
            duplicate
                .validate()
                .iter()
                .any(|error| error.contains("duplicates"))
        );

        duplicate.observed_lanes = vec![ObservedExecutionLane::CompatibilityFallback];
        assert!(
            duplicate
                .validate()
                .iter()
                .any(|error| error.contains("conflicts with fallback decisions"))
        );

        let fallback = ExecutionLaneEvidence::from_observations(
            ExecutionLane::SqlResultOnly,
            vec![
                ObservedExecutionLane::SqlResult,
                ObservedExecutionLane::CompatibilityFallback,
            ],
            "trace-test",
            "run-test",
            "scenario-test",
            "select",
            "unix",
            "fallback_allowed",
            "unix:fallback_allowed",
            vec![sample_fallback()],
            true,
        );
        assert!(fallback.validate().is_empty());
        let mut allowed = fallback.clone();
        allowed.fallback_decisions[0].decision_outcome =
            "allowed_compatibility_fallback".to_owned();
        assert!(allowed.validate().is_empty());
        let mut drifted = fallback;
        drifted.fallback_decisions[0].decision_outcome = "unknown_outcome".to_owned();
        assert!(
            drifted
                .validate_canonical_fallback_registry()
                .iter()
                .any(|error| error.contains("group_by_fallback"))
        );

        let mut corrupted_diagnostic = allowed.clone();
        corrupted_diagnostic.fallback_decisions[0].first_failure_diagnostic =
            "statement_kind=delete".to_owned();
        assert!(
            corrupted_diagnostic
                .validate()
                .iter()
                .any(|error| error.contains("conflicts with structured fields"))
        );

        let mut zero_occurrences = allowed;
        zero_occurrences.fallback_decisions[0].occurrences = 0;
        assert!(
            zero_occurrences
                .validate()
                .iter()
                .any(|error| error.contains("occurrences is zero"))
        );
    }

    #[test]
    fn incomplete_fallback_capture_is_a_valid_fail_closed_mismatch() {
        let evidence = ExecutionLaneEvidence::from_observations(
            ExecutionLane::PagerBackedRequired,
            vec![ObservedExecutionLane::PagerBacked],
            "trace-test",
            "run-test",
            "scenario-test",
            "select",
            "unix",
            "parity_cert_strict",
            "unix:parity_cert_strict",
            Vec::new(),
            false,
        );

        assert!(!evidence.requirement_satisfied);
        assert!(evidence.validate().is_empty(), "{evidence:?}");
        assert!(
            evidence
                .first_failure_diagnostic
                .as_deref()
                .is_some_and(|diagnostic| diagnostic.contains("fallback_reason=capture_incomplete")),
            "{evidence:?}"
        );
    }

    #[test]
    fn execution_lane_fallback_reasons_bind_to_canonical_inventory() {
        let inventory: toml::Value = toml::from_str(include_str!(
            "../../../docs/contracts/fallback_boundary_inventory.toml"
        ))
        .expect("parse canonical fallback inventory");
        let allowed = inventory
            .get("boundary")
            .and_then(toml::Value::as_array)
            .expect("fallback inventory boundary rows")
            .iter()
            .filter_map(|row| row.get("decision_reason"))
            .filter_map(toml::Value::as_str)
            .collect::<BTreeSet<_>>();
        let evidence = ExecutionLaneEvidence::from_observations(
            ExecutionLane::SqlResultOnly,
            vec![
                ObservedExecutionLane::SqlResult,
                ObservedExecutionLane::CompatibilityFallback,
            ],
            "trace-test",
            "run-test",
            "scenario-test",
            "select",
            "unix",
            "fallback_allowed",
            "unix:fallback_allowed",
            vec![sample_fallback()],
            true,
        );

        assert!(!allowed.is_empty());
        assert!(allowed.contains("group_by_fallback"));
        assert!(evidence.validate_canonical_fallback_registry().is_empty());
    }

    #[test]
    fn execution_lane_serialization_is_stable_and_unknown_values_fail() {
        let evidence = lane_evidence(
            ExecutionLane::PlannerRequired,
            vec![
                ObservedExecutionLane::Vdbe,
                ObservedExecutionLane::Planner,
                ObservedExecutionLane::SqlResult,
            ],
        );
        let first = serde_json::to_string(&evidence).expect("serialize evidence");
        let decoded: ExecutionLaneEvidence =
            serde_json::from_str(&first).expect("deserialize evidence");
        let second = serde_json::to_string(&decoded).expect("reserialize evidence");
        assert_eq!(first, second);
        assert!(decoded.validate().is_empty());

        let unknown_required = first.replace("planner_required", "unknown_required");
        assert!(serde_json::from_str::<ExecutionLaneEvidence>(&unknown_required).is_err());
        let unknown_observed = first.replace("\"planner\"", "\"unknown_observed\"");
        assert!(serde_json::from_str::<ExecutionLaneEvidence>(&unknown_observed).is_err());
        let unknown_field = first.replacen('{', "{\"unknown\":true,", 1);
        assert!(serde_json::from_str::<ExecutionLaneEvidence>(&unknown_field).is_err());
    }

    proptest! {
        #[test]
        fn execution_lane_property_recomputes_requirement(
            required_index in 0_usize..6,
            observed_bits in 0_u8..64,
            fallback_capture_complete in any::<bool>(),
        ) {
            let requirements = [
                ExecutionLane::SqlResultOnly,
                ExecutionLane::PagerBackedRequired,
                ExecutionLane::PlannerRequired,
                ExecutionLane::VdbeRequired,
                ExecutionLane::MvccRequired,
                ExecutionLane::RecoveryRequired,
            ];
            let observations = [
                ObservedExecutionLane::SqlResult,
                ObservedExecutionLane::PagerBacked,
                ObservedExecutionLane::Planner,
                ObservedExecutionLane::Vdbe,
                ObservedExecutionLane::Mvcc,
                ObservedExecutionLane::Recovery,
            ];
            let observed = observations
                .into_iter()
                .enumerate()
                .filter_map(|(index, lane)| ((observed_bits & (1 << index)) != 0).then_some(lane))
                .collect::<Vec<_>>();
            let evidence = ExecutionLaneEvidence::from_observations(
                requirements[required_index],
                observed,
                "trace-property",
                "run-property",
                "scenario-property",
                "test",
                "unix",
                "parity_cert_strict",
                "unix:parity_cert_strict",
                Vec::new(),
                fallback_capture_complete,
            );
            prop_assert_eq!(
                evidence.requirement_satisfied,
                fallback_capture_complete && (observed_bits & (1 << required_index)) != 0
            );
            let errors = evidence.validate();
            if observed_bits == 0 {
                prop_assert!(
                    errors.iter().any(|error| error.contains("observed_lanes is empty")),
                    "{evidence:?} errors={errors:?}"
                );
            } else {
                prop_assert!(errors.is_empty(), "{evidence:?} errors={errors:?}");
            }
        }
    }

    // ── Builder ─────────────────────────────────────────────────────

    #[test]
    fn builder_produces_valid_bundle() {
        let bundle = sample_bundle();
        let errors = bundle.validate();
        assert!(
            errors.is_empty(),
            "bead_id={BEAD_ID} case=builder_valid errors={errors:?}"
        );
    }

    #[test]
    fn builder_rejects_missing_execution_lane_evidence() {
        let error = FailureBundleBuilder::new()
            .bundle_id("fb-run001-missing-lane")
            .created_at("2026-02-13T06:00:00Z")
            .run_id("bd-test-20260213-1234")
            .scenario(sample_scenario())
            .failure(sample_failure())
            .reproducibility(sample_repro())
            .environment(sample_env())
            .build()
            .expect_err("missing execution-lane evidence must fail closed");
        assert_eq!(error, "execution_lane_evidence is required");
    }

    #[test]
    fn builder_rejects_corrupted_execution_lane_evidence() {
        let mut evidence = sample_lane("bd-test-20260213-1234", "MVCC-3");
        evidence.requirement_satisfied = false;
        let error = FailureBundleBuilder::new()
            .bundle_id("fb-run001-corrupt-lane")
            .created_at("2026-02-13T06:00:00Z")
            .run_id("bd-test-20260213-1234")
            .execution_lane_evidence(evidence)
            .scenario(sample_scenario())
            .failure(sample_failure())
            .reproducibility(sample_repro())
            .environment(sample_env())
            .build()
            .expect_err("corrupted execution-lane evidence must fail closed");
        assert!(error.contains("execution_lane.requirement_satisfied is inconsistent"));
    }

    #[test]
    fn execution_lane_evidence_guard_unit_contract() {
        execution_lane_match_matrix_is_fail_closed();
        storage_requirements_reject_any_compatibility_fallback();
        execution_lane_validation_rejects_duplicates_conflicts_and_registry_drift();
        incomplete_fallback_capture_is_a_valid_fail_closed_mismatch();
        execution_lane_fallback_reasons_bind_to_canonical_inventory();
        execution_lane_serialization_is_stable_and_unknown_values_fail();
        execution_lane_property_recomputes_requirement();
        builder_rejects_missing_execution_lane_evidence();
        builder_rejects_corrupted_execution_lane_evidence();
    }

    #[test]
    fn builder_rejects_missing_bundle_id() {
        let result = FailureBundleBuilder::new()
            .created_at("2026-02-13T06:00:00Z")
            .run_id("run1")
            .execution_lane_evidence(sample_lane("run1", "MVCC-3"))
            .scenario(sample_scenario())
            .failure(sample_failure())
            .reproducibility(sample_repro())
            .environment(sample_env())
            .build();
        assert!(result.is_err(), "bead_id={BEAD_ID} case=builder_missing_id");
    }

    #[test]
    fn builder_rejects_missing_scenario() {
        let result = FailureBundleBuilder::new()
            .bundle_id("fb-1")
            .created_at("2026-02-13T06:00:00Z")
            .run_id("run1")
            .execution_lane_evidence(sample_lane("run1", "MVCC-3"))
            .failure(sample_failure())
            .reproducibility(sample_repro())
            .environment(sample_env())
            .build();
        assert!(
            result.is_err(),
            "bead_id={BEAD_ID} case=builder_missing_scenario"
        );
    }

    #[test]
    fn builder_rejects_missing_failure() {
        let result = FailureBundleBuilder::new()
            .bundle_id("fb-1")
            .created_at("2026-02-13T06:00:00Z")
            .run_id("run1")
            .execution_lane_evidence(sample_lane("run1", "MVCC-3"))
            .scenario(sample_scenario())
            .reproducibility(sample_repro())
            .environment(sample_env())
            .build();
        assert!(
            result.is_err(),
            "bead_id={BEAD_ID} case=builder_missing_failure"
        );
    }

    #[test]
    fn builder_rejects_missing_environment() {
        let result = FailureBundleBuilder::new()
            .bundle_id("fb-1")
            .created_at("2026-02-13T06:00:00Z")
            .run_id("run1")
            .execution_lane_evidence(sample_lane("run1", "MVCC-3"))
            .scenario(sample_scenario())
            .failure(sample_failure())
            .reproducibility(sample_repro())
            .build();
        assert!(
            result.is_err(),
            "bead_id={BEAD_ID} case=builder_missing_env"
        );
    }

    #[test]
    fn builder_rejects_multiline_repro_command() {
        let result = FailureBundleBuilder::new()
            .bundle_id("fb-1")
            .created_at("2026-02-13T06:00:00Z")
            .run_id("run1")
            .execution_lane_evidence(sample_lane("run1", "MVCC-3"))
            .scenario(sample_scenario())
            .failure(sample_failure())
            .reproducibility(ReproducibilityInfo {
                seed: Some(1),
                fixture_id: None,
                schedule_fingerprint: None,
                repro_command: "cargo test\n-- --nocapture".to_owned(),
                storage_mode: None,
                concurrency_mode: None,
            })
            .environment(sample_env())
            .build();
        let error = result.expect_err("multiline repro command should fail");
        assert!(
            error.contains("single-line"),
            "bead_id={BEAD_ID} case=builder_multiline_repro_error error={error}"
        );
    }

    // ── Validation ──────────────────────────────────────────────────

    #[test]
    fn validate_catches_empty_bundle_id() {
        let mut bundle = sample_bundle();
        bundle.bundle_id = String::new();
        let errors = bundle.validate();
        assert!(
            errors.iter().any(|e| e.contains("bundle_id")),
            "bead_id={BEAD_ID} case=validate_empty_bundle_id errors={errors:?}"
        );
    }

    #[test]
    fn validate_catches_empty_scenario_id() {
        let mut bundle = sample_bundle();
        bundle.scenario.scenario_id = String::new();
        let errors = bundle.validate();
        assert!(
            errors.iter().any(|e| e.contains("scenario_id")),
            "bead_id={BEAD_ID} case=validate_empty_scenario_id errors={errors:?}"
        );
    }

    #[test]
    fn validate_catches_empty_artifact_label() {
        let mut bundle = sample_bundle();
        bundle.artifacts[0].label = String::new();
        let errors = bundle.validate();
        assert!(
            errors.iter().any(|e| e.contains("label")),
            "bead_id={BEAD_ID} case=validate_empty_artifact_label errors={errors:?}"
        );
    }

    #[test]
    fn validate_catches_multiline_repro_command() {
        let mut bundle = sample_bundle();
        bundle.reproducibility.repro_command = "cargo test\n-- --nocapture".to_owned();
        let errors = bundle.validate();
        assert!(
            errors.iter().any(|e| e.contains("single-line")),
            "bead_id={BEAD_ID} case=validate_multiline_repro errors={errors:?}"
        );
    }

    #[test]
    fn validate_catches_content_hash_mismatch() {
        let mut bundle = sample_bundle();
        bundle.content_hash = "0".repeat(64);
        let errors = bundle.validate();
        assert!(
            errors
                .iter()
                .any(|e| e.contains("content_hash does not match canonical bundle payload")),
            "bead_id={BEAD_ID} case=validate_content_hash_mismatch errors={errors:?}"
        );
    }

    #[test]
    fn deterministic_content_hash_stable_across_human_labels() {
        let bundle_a = sample_bundle_with_labels("fb-run001-1", "2026-02-13T06:00:00Z");
        let bundle_b = sample_bundle_with_labels("fb-run999-9", "2030-01-01T00:00:00Z");

        assert_eq!(
            bundle_a.content_hash, bundle_b.content_hash,
            "bead_id={BEAD_ID} case=content_hash_stable_labels"
        );
        assert_eq!(
            bundle_a.deterministic_bundle_hash(),
            bundle_b.deterministic_bundle_hash(),
            "bead_id={BEAD_ID} case=content_hash_stable_labels_recompute"
        );
    }

    #[test]
    fn deterministic_content_hash_includes_first_failure_linkage() {
        let without_first_failure = sample_bundle();
        let without_hash = without_first_failure.deterministic_bundle_hash();

        let mut with_first_failure = sample_bundle();
        with_first_failure.failure.first_divergence = Some(FirstDivergence {
            operation_index: 12,
            sql: Some("SELECT * FROM t".to_owned()),
            phase: Some("execute".to_owned()),
        });
        let with_hash = with_first_failure.deterministic_bundle_hash();

        assert_ne!(
            without_hash, with_hash,
            "bead_id={BEAD_ID} case=content_hash_first_failure_linkage"
        );
        assert_eq!(
            with_first_failure.first_failure_diagnostics_pointer(),
            Some(FIRST_FAILURE_DIAGNOSTIC_POINTER),
            "bead_id={BEAD_ID} case=first_failure_pointer"
        );
    }

    // ── JSON round-trip ─────────────────────────────────────────────

    #[test]
    fn json_roundtrip_preserves_all_fields() {
        let bundle = sample_bundle();
        let json =
            serde_json::to_string_pretty(&bundle).expect("bead_id=bd-mblr.4.4 case=json_serialize");
        let decoded: FailureBundle =
            serde_json::from_str(&json).expect("bead_id=bd-mblr.4.4 case=json_deserialize");
        assert_eq!(bundle, decoded, "bead_id={BEAD_ID} case=json_roundtrip_eq");
    }

    #[test]
    fn json_includes_schema_version() {
        let bundle = sample_bundle();
        let json = serde_json::to_string(&bundle).expect("serialize");
        assert!(
            json.contains(BUNDLE_SCHEMA_VERSION),
            "bead_id={BEAD_ID} case=json_schema_version"
        );
    }

    // ── Failure types ───────────────────────────────────────────────

    #[test]
    fn failure_type_labels_are_unique() {
        let types = [
            FailureType::Assertion,
            FailureType::Panic,
            FailureType::Divergence,
            FailureType::Timeout,
            FailureType::SsiConflict,
            FailureType::MvccInvariant,
            FailureType::WalRecovery,
            FailureType::FileFormat,
            FailureType::Extension,
            FailureType::Other,
        ];
        let labels: std::collections::HashSet<&str> = types.iter().map(|t| t.label()).collect();
        assert_eq!(
            labels.len(),
            types.len(),
            "bead_id={BEAD_ID} case=failure_type_unique_labels"
        );
    }

    #[test]
    fn failure_type_display_matches_label() {
        let ft = FailureType::Divergence;
        assert_eq!(
            format!("{ft}"),
            ft.label(),
            "bead_id={BEAD_ID} case=failure_type_display"
        );
    }

    // ── Convenience constructors ────────────────────────────────────

    #[test]
    fn bundle_assertion_failure_creates_valid_bundle() {
        let bundle = bundle_assertion_failure(
            "fb-assert-1",
            "run-001",
            "2026-02-13T06:00:00Z",
            "bd-test",
            "test_row_count",
            "SQL-5",
            "10 rows",
            "9 rows",
            "cargo test -- test_row_count --exact",
            sample_env(),
        )
        .expect("bead_id=bd-mblr.4.4 case=assertion_builder");

        assert_eq!(bundle.failure.failure_type, FailureType::Assertion);
        assert_eq!(bundle.failure.expected.as_deref(), Some("10 rows"));
        assert_eq!(bundle.failure.actual.as_deref(), Some("9 rows"));
        let errors = bundle.validate();
        assert!(
            errors.is_empty(),
            "bead_id={BEAD_ID} case=assertion_valid errors={errors:?}"
        );
    }

    #[test]
    fn bundle_divergence_failure_creates_valid_bundle() {
        let div = FirstDivergence {
            operation_index: 42,
            sql: Some("SELECT count(*) FROM t1".to_owned()),
            phase: Some("execute".to_owned()),
        };
        let bundle = bundle_divergence_failure(
            "fb-div-1",
            "run-002",
            "2026-02-13T06:00:00Z",
            "bd-test",
            "test_diff_select",
            "SQL-10",
            div,
            "42",
            "41",
            0xCAFE_BABE,
            "cargo test -- test_diff_select --exact",
            sample_env(),
        )
        .expect("bead_id=bd-mblr.4.4 case=divergence_builder");

        assert_eq!(bundle.failure.failure_type, FailureType::Divergence);
        assert!(bundle.failure.first_divergence.is_some());
        assert_eq!(bundle.reproducibility.seed, Some(0xCAFE_BABE));
        let errors = bundle.validate();
        assert!(
            errors.is_empty(),
            "bead_id={BEAD_ID} case=divergence_valid errors={errors:?}"
        );
    }

    // ── Summary line ────────────────────────────────────────────────

    #[test]
    fn summary_line_includes_key_fields() {
        let bundle = sample_bundle();
        let line = bundle.summary_line();
        assert!(
            line.contains("assertion"),
            "bead_id={BEAD_ID} case=summary_type"
        );
        assert!(
            line.contains("MVCC-3"),
            "bead_id={BEAD_ID} case=summary_scenario"
        );
        assert!(
            line.contains("fb-run001-1"),
            "bead_id={BEAD_ID} case=summary_bundle_id"
        );
        assert!(
            line.contains("seed=0x"),
            "bead_id={BEAD_ID} case=summary_seed"
        );
    }

    #[test]
    fn summary_line_omits_seed_when_none() {
        let mut bundle = sample_bundle();
        bundle.reproducibility.seed = None;
        let line = bundle.summary_line();
        assert!(
            !line.contains("seed="),
            "bead_id={BEAD_ID} case=summary_no_seed"
        );
    }

    // ── BundleIndex ─────────────────────────────────────────────────

    #[test]
    fn index_empty_by_default() {
        let idx = BundleIndex::new();
        assert!(idx.is_empty(), "bead_id={BEAD_ID} case=index_empty");
        assert_eq!(idx.len(), 0, "bead_id={BEAD_ID} case=index_len_zero");
    }

    #[test]
    fn index_insert_and_query() {
        let mut idx = BundleIndex::new();
        idx.insert(sample_bundle());
        assert_eq!(idx.len(), 1, "bead_id={BEAD_ID} case=index_len_one");

        let by_scenario = idx.by_scenario("MVCC-3");
        assert_eq!(
            by_scenario.len(),
            1,
            "bead_id={BEAD_ID} case=index_by_scenario"
        );

        let by_bead = idx.by_bead("bd-test");
        assert_eq!(by_bead.len(), 1, "bead_id={BEAD_ID} case=index_by_bead");

        let by_seed = idx.by_seed(0xDEAD_BEEF);
        assert_eq!(by_seed.len(), 1, "bead_id={BEAD_ID} case=index_by_seed");

        let by_type = idx.by_failure_type(FailureType::Assertion);
        assert_eq!(by_type.len(), 1, "bead_id={BEAD_ID} case=index_by_type");

        let by_tag = idx.by_triage_tag("mvcc");
        assert_eq!(by_tag.len(), 1, "bead_id={BEAD_ID} case=index_by_tag");
    }

    #[test]
    fn index_query_no_match() {
        let mut idx = BundleIndex::new();
        idx.insert(sample_bundle());

        assert!(
            idx.by_scenario("NONEXISTENT").is_empty(),
            "bead_id={BEAD_ID} case=index_no_match_scenario"
        );
        assert!(
            idx.by_seed(0x1234).is_empty(),
            "bead_id={BEAD_ID} case=index_no_match_seed"
        );
        assert!(
            idx.by_failure_type(FailureType::Timeout).is_empty(),
            "bead_id={BEAD_ID} case=index_no_match_type"
        );
    }

    #[test]
    fn index_multi_bundle_queries() {
        let mut idx = BundleIndex::new();

        // First bundle: MVCC assertion
        idx.insert(sample_bundle());

        // Second bundle: SQL divergence
        let sql_bundle = FailureBundleBuilder::new()
            .bundle_id("fb-run001-2")
            .created_at("2026-02-13T06:01:00Z")
            .run_id("bd-test-20260213-1234")
            .execution_lane_evidence(sample_lane("bd-test-20260213-1234", "SQL-5"))
            .scenario(ScenarioInfo {
                scenario_id: "SQL-5".to_owned(),
                bead_id: "bd-sql".to_owned(),
                test_name: "test_select_divergence".to_owned(),
                script_path: None,
            })
            .failure(FailureInfo {
                failure_type: FailureType::Divergence,
                message: "result set differs".to_owned(),
                expected: None,
                actual: None,
                diff: None,
                invariant: None,
                first_divergence: Some(FirstDivergence {
                    operation_index: 7,
                    sql: Some("SELECT * FROM t1".to_owned()),
                    phase: None,
                }),
            })
            .reproducibility(ReproducibilityInfo {
                seed: Some(0xCAFE),
                fixture_id: None,
                schedule_fingerprint: None,
                repro_command: "cargo test -- test_select_divergence --exact".to_owned(),
                storage_mode: None,
                concurrency_mode: None,
            })
            .environment(sample_env())
            .triage_tag("divergence")
            .build()
            .expect("sql bundle");
        idx.insert(sql_bundle);

        // Third bundle: another MVCC assertion
        let mvcc2 = FailureBundleBuilder::new()
            .bundle_id("fb-run001-3")
            .created_at("2026-02-13T06:02:00Z")
            .run_id("bd-test-20260213-1234")
            .execution_lane_evidence(sample_lane("bd-test-20260213-1234", "MVCC-3"))
            .scenario(ScenarioInfo {
                scenario_id: "MVCC-3".to_owned(),
                bead_id: "bd-test".to_owned(),
                test_name: "test_mvcc_isolation".to_owned(),
                script_path: None,
            })
            .failure(sample_failure())
            .reproducibility(ReproducibilityInfo {
                seed: Some(0xBEEF),
                fixture_id: None,
                schedule_fingerprint: None,
                repro_command: "cargo test -- test_mvcc_isolation --exact".to_owned(),
                storage_mode: None,
                concurrency_mode: None,
            })
            .environment(sample_env())
            .triage_tag("assertion")
            .build()
            .expect("mvcc2 bundle");
        idx.insert(mvcc2);

        assert_eq!(idx.len(), 3, "bead_id={BEAD_ID} case=multi_total");
        assert_eq!(
            idx.by_scenario("MVCC-3").len(),
            2,
            "bead_id={BEAD_ID} case=multi_mvcc3"
        );
        assert_eq!(
            idx.by_scenario("SQL-5").len(),
            1,
            "bead_id={BEAD_ID} case=multi_sql5"
        );
        assert_eq!(
            idx.by_failure_type(FailureType::Assertion).len(),
            2,
            "bead_id={BEAD_ID} case=multi_assertions"
        );
        assert_eq!(
            idx.by_failure_type(FailureType::Divergence).len(),
            1,
            "bead_id={BEAD_ID} case=multi_divergences"
        );
    }

    // ── Type summary ────────────────────────────────────────────────

    #[test]
    fn type_summary_counts() {
        let mut idx = BundleIndex::new();
        idx.insert(sample_bundle());

        let mut bundle2 = sample_bundle();
        bundle2.bundle_id = "fb-run001-2".to_owned();
        bundle2.failure.failure_type = FailureType::Timeout;
        idx.insert(bundle2);

        let summary = idx.type_summary();
        assert_eq!(
            summary.get("assertion"),
            Some(&1),
            "bead_id={BEAD_ID} case=type_summary_assertion"
        );
        assert_eq!(
            summary.get("timeout"),
            Some(&1),
            "bead_id={BEAD_ID} case=type_summary_timeout"
        );
    }

    // ── Scenario summary ────────────────────────────────────────────

    #[test]
    fn scenario_summary_counts() {
        let mut idx = BundleIndex::new();
        idx.insert(sample_bundle());

        let mut bundle2 = sample_bundle();
        bundle2.bundle_id = "fb-run001-2".to_owned();
        bundle2.scenario.scenario_id = "WAL-1".to_owned();
        idx.insert(bundle2);

        let summary = idx.scenario_summary();
        assert_eq!(
            summary.get("MVCC-3"),
            Some(&1),
            "bead_id={BEAD_ID} case=scenario_summary_mvcc"
        );
        assert_eq!(
            summary.get("WAL-1"),
            Some(&1),
            "bead_id={BEAD_ID} case=scenario_summary_wal"
        );
    }

    // ── Triage report ───────────────────────────────────────────────

    #[test]
    fn triage_report_empty() {
        let idx = BundleIndex::new();
        let report = idx.render_triage_report();
        assert!(
            report.contains("0 failures"),
            "bead_id={BEAD_ID} case=triage_empty"
        );
    }

    #[test]
    fn triage_report_with_bundles() {
        let mut idx = BundleIndex::new();
        idx.insert(sample_bundle());
        let report = idx.render_triage_report();
        assert!(
            report.contains("1 failure"),
            "bead_id={BEAD_ID} case=triage_count"
        );
        assert!(
            report.contains("By type:"),
            "bead_id={BEAD_ID} case=triage_type_section"
        );
        assert!(
            report.contains("By scenario:"),
            "bead_id={BEAD_ID} case=triage_scenario_section"
        );
        assert!(
            report.contains("MVCC-3"),
            "bead_id={BEAD_ID} case=triage_scenario_id"
        );
    }

    // ── EnvironmentInfo ─────────────────────────────────────────────

    #[test]
    fn environment_info_new_sets_required_fields() {
        let env = EnvironmentInfo::new("sha", "toolchain", "platform");
        assert_eq!(env.git_sha, "sha", "bead_id={BEAD_ID} case=env_sha");
        assert_eq!(
            env.toolchain, "toolchain",
            "bead_id={BEAD_ID} case=env_toolchain"
        );
        assert_eq!(
            env.platform, "platform",
            "bead_id={BEAD_ID} case=env_platform"
        );
        assert!(
            env.feature_flags.is_empty(),
            "bead_id={BEAD_ID} case=env_flags_empty"
        );
        assert!(
            env.extra.is_empty(),
            "bead_id={BEAD_ID} case=env_extra_empty"
        );
    }

    // ── Adoption checklist ──────────────────────────────────────────

    #[test]
    fn adoption_checklist_has_ten_items() {
        let checklist = build_e2e_adoption_checklist();
        assert_eq!(
            checklist.len(),
            10,
            "bead_id={BEAD_ID} case=checklist_count"
        );
    }

    #[test]
    fn adoption_checklist_ids_sequential() {
        let checklist = build_e2e_adoption_checklist();
        for (i, item) in checklist.iter().enumerate() {
            let expected_id = format!("E-{}", i + 1);
            assert_eq!(
                item.id, expected_id,
                "bead_id={BEAD_ID} case=checklist_id_{expected_id}"
            );
        }
    }

    #[test]
    fn adoption_checklist_items_nonempty() {
        let checklist = build_e2e_adoption_checklist();
        for item in &checklist {
            assert!(
                !item.requirement.is_empty(),
                "bead_id={BEAD_ID} case=checklist_req_{}",
                item.id
            );
            assert!(
                !item.example.is_empty(),
                "bead_id={BEAD_ID} case=checklist_ex_{}",
                item.id
            );
        }
    }
}
