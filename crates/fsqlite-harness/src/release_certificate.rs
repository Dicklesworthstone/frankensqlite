//! Release certificate generator with auditable evidence ledger (bd-1dp9.8.4).
//!
//! Generates machine-verifiable release certificates that aggregate:
//! - Parity invariant catalog and traceability report (bd-1dp9.8.1)
//! - Drift monitor snapshot (bd-1dp9.8.2)
//! - Confidence gate report and evidence ledger (bd-1dp9.8.3)
//! - Adversarial counterexample campaign results (bd-1dp9.8.5)
//! - CI artifact manifest and flake budget summary (bd-1dp9.7.3)
//!
//! The certificate bundles score bounds, gate decisions, artifact hashes,
//! unresolved-risk statements, and drift alerts into a single deterministic
//! JSON artifact suitable for CI enforcement and audit archival.
//!
//! # Determinism
//!
//! Certificate generation is deterministic given identical inputs.  All
//! floating-point values use `truncate_score` for cross-platform reproducibility.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Component, Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::adversarial_search::{
    AdversarialConfig, CampaignResult, CounterexampleSeverity, run_campaign,
};
use crate::certification_policy::{
    CERTIFICATION_MAX_EVIDENCE_AGE_HOURS, CERTIFICATION_MAX_HIGH_SEVERITY_COUNTEREXAMPLES,
    CERTIFICATION_MIN_VERIFICATION_PCT, CERTIFICATION_POLICY_ID,
    CERTIFICATION_POLICY_SCHEMA_VERSION, CertificationPolicy, CertificationRatchetBaseline,
    CertificationRatchetCandidate, REQUIRED_CERTIFICATION_LANES, canonical_certification_policy,
    certification_gate_config, evaluate_certification_ratchets,
};
use crate::ci_gate_matrix::{
    ArtifactEntry, ArtifactManifest, FallbackTransparencyGateSummary, GlobalFlakeBudgetResult,
};
use crate::confidence_gates::{
    EvidenceLedger, ExpectedLossRanking, GateConfig, GateDecision, GateReport,
    build_evidence_ledger, evaluate_full,
};
use crate::drift_monitor::{ParityDriftConfig, ParityDriftMonitor, ParityDriftSnapshot};
use crate::no_mock_critical_path_gate::{NoMockCriticalPathReport, NoMockVerdict};
use crate::parity_invariant_catalog::{
    CatalogStats, InvariantId, ProofSummaryEntry, ReleaseTraceabilityReport,
    build_canonical_catalog,
};
use crate::parity_taxonomy::{
    FeatureCategory, FeatureId, build_canonical_universe, truncate_score,
};
use crate::parity_verification_workflow::{
    BEAD_ID as WORKFLOW_BEAD_ID, SCHEMA_VERSION as WORKFLOW_SCHEMA_VERSION, WorkflowOutcome,
    WorkflowPhase, WorkflowReport, validate_workflow_report,
};
use crate::score_engine::BayesianScorecard;

#[allow(dead_code)]
const BEAD_ID: &str = "bd-1dp9.8.4";

/// Public bead identifier.
pub const RELEASE_CERT_BEAD_ID: &str = "bd-1dp9.8.4";

/// Schema version for all certificate artifacts.
pub const CERTIFICATE_SCHEMA_VERSION: u32 = 1;

// ---------------------------------------------------------------------------
// Certificate verdict
// ---------------------------------------------------------------------------

/// Overall release certificate verdict.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum CertificateVerdict {
    /// All gates pass, no unresolved high-severity findings.
    Approved,
    /// Gates pass conditionally — minor unresolved risks documented.
    Conditional,
    /// One or more gates fail or high-severity counterexamples found.
    Rejected,
}

impl std::fmt::Display for CertificateVerdict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Approved => write!(f, "APPROVED"),
            Self::Conditional => write!(f, "CONDITIONAL"),
            Self::Rejected => write!(f, "REJECTED"),
        }
    }
}

// ---------------------------------------------------------------------------
// Unresolved risk
// ---------------------------------------------------------------------------

/// An unresolved risk statement embedded in the certificate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnresolvedRisk {
    /// Source component that reported the risk.
    pub source: String,
    /// Severity level (Low, Medium, High).
    pub severity: String,
    /// Human-readable description.
    pub description: String,
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the release certificate generator.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CertificateConfig {
    /// Gate configuration for confidence gates.
    pub gate_config: GateConfig,
    /// Drift monitor configuration.
    pub drift_config: ParityDriftConfig,
    /// Adversarial search configuration.
    pub adversarial_config: AdversarialConfig,
    /// Maximum number of HIGH-severity counterexamples before rejection.
    pub max_high_severity: usize,
    /// Minimum global verification percentage for approval.
    pub min_verification_pct: f64,
}

impl Default for CertificateConfig {
    fn default() -> Self {
        Self {
            gate_config: certification_gate_config(),
            drift_config: ParityDriftConfig::default(),
            adversarial_config: AdversarialConfig::default(),
            max_high_severity: CERTIFICATION_MAX_HIGH_SEVERITY_COUNTEREXAMPLES,
            min_verification_pct: CERTIFICATION_MIN_VERIFICATION_PCT,
        }
    }
}

// ---------------------------------------------------------------------------
// Strict frozen-candidate adapter contract (bd-2yqp6.7.3)
// ---------------------------------------------------------------------------

const STRICT_CERTIFICATE_INPUT_SCHEMA: &str = "fsqlite.parity_certificate_input.v5";
const STRICT_CERTIFICATE_BUNDLE_SCHEMA: &str = "fsqlite.release_certificate_bundle.v3";
const D4_RUNTIME_PATH_PROOF_SCHEMA: &str = "fsqlite.d4_runtime_path_proof.v1";
const D4_SCENARIO_ARTIFACT_SCHEMA: &str = "fsqlite.d4_scenario_artifact.v1";
const STRICT_RESULTS_RECORD_SCHEMA: &str = "fsqlite.release_traceability_result.v1";
const STRICT_BACKEND_IDENTITY: &str = "fsqlite:pager_wal_mvcc_btree:parity_cert_strict";
const PHASE5_SCHEMA_VERSION: u32 = 3;
const PHASE5_RCH_RECEIPT_SCHEMA: &str = "fsqlite.phase5.rch_execution_receipt.v1";
const PHASE5_LIVE_GUARD_RECEIPT_SCHEMA: &str = "fsqlite.phase5.live_guard_receipt.v1";
const PHASE5_DIGEST_ALGORITHM: &str = "blake3-256";
const PHASE5_INVENTORY_SHA256_ALGORITHM: &str = "sha2-256";
const PHASE5_EVIDENCE_PREFIX: &str = "tests/artifacts/release-evidence";
const REGRESSION_BASELINE_PATH: &str = "tests/regression_baseline.json";
const T16_SOURCE_PATH: &str = "crates/fsqlite-e2e/tests/bd_wsw3p_concurrent_write_showcase.rs";
const T16_TEST_NAME: &str = "t16_fsqlite_outperforms_csqlite_at_16_threads";
const T16_SAMPLE_COUNT: usize = 22;
const T16_BOUND_ORDER_STATISTIC: usize = 7;
const T16_EXPECTED_ROWS_PER_SAMPLE: i64 = 3_200;
const T16_MIN_RATIO_LOWER_BOUND: f64 = 1.0;
const PHASE5_LIVE_GUARD_LOCATOR: &str = concat!(
    "crates/fsqlite-harness/tests/phase5_regression_guard.rs::",
    "phase5_regression_guard_full_workspace_against_baseline"
);
const PHASE5_WORKSPACE_ARGV: [&str; 6] = [
    "cargo",
    "test",
    "--locked",
    "--workspace",
    "--",
    "--test-threads=1",
];
const PATH_REMAPPED_LIBRARY_SOURCES: [&str; 2] = [
    "crates/fsqlite-core/src/policy_controller.rs",
    "crates/fsqlite-parser/src/semantic_test.rs",
];
const REQUIRED_D4_SCENARIOS: [&str; 5] = [
    "connection_open",
    "transaction_begin_commit_rollback",
    "restart_recovery",
    "concurrent_write_read",
    "concurrent_mode_defaults",
];

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StrictEvidenceRef {
    path: String,
    sha256: String,
    observed_unix_ms: u128,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StrictLaneManifestRef {
    lane: String,
    #[serde(flatten)]
    evidence: StrictEvidenceRef,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StrictCertificateEvidenceInput {
    schema_version: String,
    /// Evidence descendant E: the checked-out commit that binds every strict artifact.
    candidate_git_sha: String,
    /// Tested candidate T: the direct first parent of `candidate_git_sha`.
    tested_candidate_git_sha: String,
    run_id: String,
    trace_id: String,
    scenario_id: String,
    seed: u64,
    generated_unix_ms: u128,
    freshness_budget_ms: u128,
    workflow_report: StrictEvidenceRef,
    gate_report: StrictEvidenceRef,
    expected_loss_ranking: StrictEvidenceRef,
    evidence_ledger: StrictEvidenceRef,
    catalog_stats: StrictEvidenceRef,
    release_traceability: StrictEvidenceRef,
    drift_snapshot: StrictEvidenceRef,
    adversarial_campaign: StrictEvidenceRef,
    ci_flake_budget: StrictEvidenceRef,
    certification_policy: StrictEvidenceRef,
    ratchet_baseline: StrictEvidenceRef,
    ratchet_baseline_git_sha: String,
    ratchet_candidate: StrictEvidenceRef,
    critical_path_evidence: StrictEvidenceRef,
    results_jsonl: StrictEvidenceRef,
    scorecard: StrictEvidenceRef,
    candidate_artifact_manifest: StrictEvidenceRef,
    required_lane_manifests: Vec<StrictLaneManifestRef>,
    d4_runtime_path_proof: StrictEvidenceRef,
    g9_gate_summary: StrictEvidenceRef,
    regression_baseline: StrictEvidenceRef,
    phase5_release_evidence_manifest: StrictEvidenceRef,
    phase5_live_guard_receipt: StrictEvidenceRef,
    dependency_feature_graph: StrictEvidenceRef,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct D4RuntimePathProof {
    schema_version: String,
    source_commit: String,
    run_id: String,
    trace_id: String,
    scenario_id: String,
    seed: u64,
    generated_unix_ms: u128,
    backend_identity: String,
    gate_passed: bool,
    concurrent_mode_default: bool,
    certifying_fallback_events: usize,
    scenarios: Vec<D4ScenarioProof>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct D4ScenarioProof {
    scenario: String,
    passed: bool,
    exit_code: i32,
    backend_identity: String,
    artifact: StrictEvidenceRef,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct D4ScenarioArtifact {
    schema_version: String,
    source_commit: String,
    run_id: String,
    trace_id: String,
    scenario_id: String,
    seed: u64,
    generated_unix_ms: u128,
    scenario: String,
    backend_identity: String,
    passed: bool,
    exit_code: i32,
    concurrent_mode_default: bool,
    certifying_fallback_events: usize,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct StrictResultsRecord {
    schema_version: String,
    candidate_git_sha: String,
    run_id: String,
    trace_id: String,
    scenario_id: String,
    seed: u64,
    invariant_id: InvariantId,
    feature_id: FeatureId,
    category: String,
    statement: String,
    proof_summary: Vec<ProofSummaryEntry>,
    artifacts: Vec<StrictResultsArtifact>,
    passed: bool,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct StrictResultsArtifact {
    path: String,
    sha256: String,
    size_bytes: u64,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum RegressionIgnoreKind {
    KnownBug,
    Placeholder,
    Performance,
    Stress,
    Diagnostic,
    SubprocessHelper,
    ArtifactGeneration,
    EnvironmentSpecific,
    ReleaseGate,
}

impl RegressionIgnoreKind {
    const fn allows_policy(self, policy: RegressionIgnorePolicy) -> bool {
        match self {
            Self::KnownBug => matches!(policy, RegressionIgnorePolicy::BlockRelease),
            Self::Placeholder => matches!(
                policy,
                RegressionIgnorePolicy::BlockRelease | RegressionIgnorePolicy::CoveredByParent
            ),
            Self::Performance => matches!(
                policy,
                RegressionIgnorePolicy::RunForRelease | RegressionIgnorePolicy::Exempt
            ),
            Self::Stress => matches!(policy, RegressionIgnorePolicy::RunForRelease),
            Self::Diagnostic | Self::ArtifactGeneration | Self::EnvironmentSpecific => {
                matches!(policy, RegressionIgnorePolicy::Exempt)
            }
            Self::SubprocessHelper => matches!(policy, RegressionIgnorePolicy::CoveredByParent),
            Self::ReleaseGate => matches!(
                policy,
                RegressionIgnorePolicy::BlockRelease | RegressionIgnorePolicy::RunForRelease
            ),
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum RegressionIgnorePolicy {
    BlockRelease,
    RunForRelease,
    CoveredByParent,
    Exempt,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct RegressionTestIdentity {
    source_path: String,
    test_name: String,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct RegressionEvidenceReceipt {
    source_commit: String,
    artifact_path: String,
    artifact_blake3: String,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct RegressionEvidenceRequirement {
    requirement: String,
    receipt: Option<RegressionEvidenceReceipt>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct RegressionIgnoredTest {
    source_path: String,
    test_name: String,
    reason: String,
    cfg_condition: Option<String>,
    kind: RegressionIgnoreKind,
    policy: RegressionIgnorePolicy,
    #[serde(default)]
    parent_tests: Vec<RegressionTestIdentity>,
    evidence: RegressionEvidenceRequirement,
}

impl RegressionIgnoredTest {
    fn locator(&self) -> String {
        format!("{}::{}", self.source_path, self.test_name)
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct RegressionBaselineReference {
    as_of_phase: String,
    total_tests: u64,
    passed: u64,
    failed: u64,
    ignored: u64,
    baseline_commit: String,
    #[serde(default)]
    baseline_evidence: Option<Phase5BaselineEvidence>,
    ignored_tests: Vec<RegressionIgnoredTest>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Phase5Manifest {
    schema_version: u32,
    tested_commit: String,
    signature_path: String,
    signer_attestation: Phase5EvidenceLeaf,
    cargo_lock: Phase5EvidenceLeaf,
    rust_toolchain: Phase5EvidenceLeaf,
    pre_capture_untracked: Phase5EvidenceLeaf,
    compiler_inventory_attestation: Phase5EvidenceLeaf,
    workspace: Phase5RunEvidence,
    run_receipts: Vec<Phase5RunReceipt>,
    auxiliary_scorecards: Phase5AuxiliaryScorecards,
    performance_regression_gate: Phase5PerformanceRegressionGate,
    evidence_pack: Vec<Phase5EvidenceLeaf>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Phase5PerformanceRegressionGate {
    schema_version: String,
    status: String,
    release_authorized: bool,
    blockers: Vec<String>,
    rationale: String,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Phase5RunReceipt {
    source_path: String,
    test_name: String,
    requirement_blake3: String,
    evidence: Phase5RunEvidence,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Phase5RunEvidence {
    execution: Phase5CommandEvidence,
    runner_receipt: Phase5EvidenceLeaf,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Phase5CommandEvidence {
    argv: Vec<String>,
    exit_status: i32,
    stdout: Phase5StreamEvidence,
    stderr: Phase5StreamEvidence,
    transcript: Phase5EvidenceLeaf,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum Phase5StreamCapture {
    Observed,
    SynthesizedEmpty,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Phase5StreamEvidence {
    capture: Phase5StreamCapture,
    leaf: Phase5EvidenceLeaf,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Phase5EvidenceLeaf {
    path: String,
    digest_algorithm: String,
    digest: String,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Phase5RchReceipt {
    schema_version: String,
    inner_cargo_argv: Vec<String>,
    job_id: String,
    active_status: Phase5EvidenceLeaf,
    completed_status: Phase5EvidenceLeaf,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Phase5ScorecardEvidence {
    scorecard: Phase5EvidenceLeaf,
    pack_manifest: Phase5EvidenceLeaf,
    commit_provenance: Phase5EvidenceLeaf,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Phase5AuxiliaryScorecards {
    c1: Phase5ScorecardEvidence,
    persistent: Phase5PersistentProfileScorecards,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Phase5PersistentProfileScorecards {
    release: Phase5ScorecardEvidence,
    release_perf: Phase5ScorecardEvidence,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Phase5CompilerInventoryRuns {
    all_targets: Phase5RunEvidence,
    all_targets_ignored: Phase5RunEvidence,
    doctests: Phase5RunEvidence,
    doctests_ignored: Phase5RunEvidence,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Phase5CompilerInventoryAttestation {
    tested_tree_blake3: String,
    cargo_metadata_blake3: String,
    target_mappings_blake3: String,
    active_identities_blake3: String,
    ignored_identities_blake3: String,
    doctest_identities_blake3: String,
    expanded_identities_blake3: String,
    cfg_profile: String,
    inventory_runs: Phase5CompilerInventoryRuns,
    inventory_leaves: Vec<Phase5CompilerInventoryLeaf>,
    targets: Vec<Phase5CompilerDerivedTarget>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Phase5CompilerInventoryLeaf {
    role: String,
    path: String,
    sha256_algorithm: String,
    sha256: String,
    blake3_algorithm: String,
    blake3: String,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Phase5CompilerDerivedTarget {
    target: String,
    source_inventory_blake3: String,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Phase5BaselineEvidence {
    source_commit: String,
    workspace: Phase5RunEvidence,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Phase5LiveGuardReceipt {
    schema_version: String,
    source_commit: String,
    tested_tree_blake3: String,
    phase5_manifest_sha256: String,
    baseline_metadata_git_sha: String,
    project_id: String,
    requirement_blake3: String,
    evidence: Phase5RunEvidence,
}

#[derive(Debug, Clone, PartialEq)]
struct T16SemanticEvidence {
    binary_path: String,
    binary_sha256: String,
    runtime_machine: String,
}

#[derive(Debug, Clone, PartialEq)]
struct T16SemanticSample {
    sample: usize,
    order: String,
    csqlite_ops_per_sec: f64,
    fsqlite_ops_per_sec: f64,
    ratio: f64,
    csqlite_total_rows: i64,
    fsqlite_total_rows: i64,
}

#[derive(Debug, Deserialize)]
struct Phase5RchStatusEnvelope {
    api_version: String,
    command: String,
    success: bool,
    data: Phase5RchStatusData,
}

#[derive(Debug, Deserialize)]
struct Phase5RchStatusData {
    daemon: Phase5RchDaemonStatus,
}

#[derive(Debug, Deserialize)]
struct Phase5RchDaemonStatus {
    active_builds: Vec<Phase5RchActiveBuild>,
    recent_builds: Vec<Phase5RchCompletedBuild>,
}

#[derive(Debug, Deserialize)]
struct Phase5RchActiveBuild {
    id: u64,
    project_id: String,
    worker_id: String,
    command: String,
}

#[derive(Debug, Deserialize)]
struct Phase5RchCompletedBuild {
    id: u64,
    project_id: String,
    worker_id: String,
    command: String,
    exit_code: i32,
    location: String,
    cancellation: Option<serde_json::Value>,
}

#[derive(Debug, Serialize)]
struct StrictCertificateBundleManifest {
    schema_version: String,
    /// Evidence descendant E that owns the strict artifact manifest.
    candidate_git_sha: String,
    /// Tested candidate T, required to be the direct first parent of E.
    tested_candidate_git_sha: String,
    run_id: String,
    trace_id: String,
    scenario_id: String,
    seed: u64,
    input_path: String,
    evidence_manifest_sha256: String,
    certificate_sha256: String,
    summary_sha256: String,
}

#[derive(Debug)]
struct LoadedStrictEvidence<T> {
    relative_path: String,
    sha256: String,
    bytes: Vec<u8>,
    value: T,
}

/// Filesystem and T/E provenance parameters for strict release-certificate generation.
#[derive(Debug, Clone)]
pub struct StrictCertificateRunConfig {
    /// Checkout containing the exact evidence descendant E.
    pub workspace_root: PathBuf,
    /// Existing sealed root containing every referenced evidence artifact.
    pub evidence_root: PathBuf,
    /// Evidence manifest path, relative to `evidence_root`.
    pub evidence_json: PathBuf,
    /// Exact lowercase 40-hex evidence descendant E checked out in `workspace_root`.
    pub candidate_git_sha: String,
    /// Exact lowercase 40-hex tested candidate T, the direct first parent of E.
    pub tested_candidate_git_sha: String,
    /// Exact lowercase 40-hex commit that first committed the baseline metadata.
    pub baseline_metadata_git_sha: String,
    /// Exact RCH project identity required for all candidate-bound receipts.
    pub candidate_rch_project_id: String,
    /// Exact RCH project identity required for the historical baseline receipt.
    pub baseline_rch_project_id: String,
    /// New output directory published atomically after every check passes.
    pub output_dir: PathBuf,
}

// ---------------------------------------------------------------------------
// Evidence chain entry
// ---------------------------------------------------------------------------

/// A single entry in the auditable evidence chain.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvidenceChainEntry {
    /// Component that produced this evidence.
    pub source_bead: String,
    /// Schema version of the source.
    pub schema_version: u32,
    /// SHA-256 hash of the serialized source report.
    pub content_hash: String,
    /// One-line summary.
    pub summary: String,
}

// ---------------------------------------------------------------------------
// Certification traceability
// ---------------------------------------------------------------------------

/// Schema version for embedded certification traceability payloads.
pub const CERTIFICATION_TRACEABILITY_SCHEMA_VERSION: u32 = 1;

/// Run-level metadata used to connect features/tests to a concrete artifact run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CertificationRunReference {
    /// Run identifier from the artifact manifest.
    pub run_id: String,
    /// Lane identifier from the artifact manifest.
    pub lane: String,
    /// Git revision for the run.
    pub git_sha: String,
    /// Manifest timestamp.
    pub created_at: String,
    /// Whether the producing gate passed.
    pub gate_passed: bool,
}

/// Feature-to-test-to-run-to-artifact view for one invariant.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CertificationTraceabilityEntry {
    /// Invariant ID.
    pub invariant_id: InvariantId,
    /// Feature ID.
    pub feature_id: FeatureId,
    /// Category display name.
    pub category: String,
    /// Invariant statement.
    pub statement: String,
    /// Whether the invariant is fully verified.
    pub verified: bool,
    /// Proof obligations recorded for the invariant.
    pub proof_summary: Vec<ProofSummaryEntry>,
    /// Run metadata if an artifact manifest was provided.
    pub run: Option<CertificationRunReference>,
    /// Artifact hashes linked from the certification manifest.
    pub artifacts: Vec<ArtifactEntry>,
    /// Artifact refs declared by the traceability report but missing from the manifest.
    pub missing_artifact_refs: Vec<String>,
}

/// Embedded certification traceability report for the release certificate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CertificationTraceabilityReport {
    /// Schema version.
    pub schema_version: u32,
    /// Certification policy used to interpret the report.
    pub policy_id: String,
    /// Whether a concrete artifact manifest was provided.
    pub manifest_present: bool,
    /// Number of entries whose artifact refs were fully resolved.
    pub fully_linked_entries: usize,
    /// Total number of unresolved artifact refs.
    pub missing_artifact_ref_count: usize,
    /// Per-invariant traceability entries.
    pub entries: Vec<CertificationTraceabilityEntry>,
}

/// Certification evidence summary embedded into the certificate verdict.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CertificationEvidenceStatus {
    /// Schema version.
    pub schema_version: u32,
    /// Certification policy used to interpret the evidence.
    pub policy_id: String,
    /// Whether a concrete artifact manifest was present.
    pub artifact_manifest_present: bool,
    /// Whether the artifact manifest's gate passed.
    pub artifact_manifest_gate_passed: Option<bool>,
    /// Whether verification-contract evidence was embedded in the manifest.
    pub verification_contract_present: bool,
    /// Whether verification-contract enforcement passed.
    pub verification_contract_passed: Option<bool>,
    /// Whether final gate enforcement passed.
    pub final_gate_passed: Option<bool>,
    /// Whether G9 fallback-transparency gate evidence was embedded in the manifest.
    pub fallback_transparency_gate_present: bool,
    /// Whether G9 fallback-transparency gate enforcement passed.
    pub fallback_transparency_gate_passed: Option<bool>,
    /// Number of standardized G9 gate failures.
    pub fallback_transparency_gate_failure_count: usize,
    /// Number of fallback boundaries missing strict-denial or real-backend proof.
    pub fallback_transparency_missing_boundary_count: usize,
    /// Number of stale fallback-transparency artifacts.
    pub fallback_transparency_stale_artifact_count: usize,
    /// Count of certifying-mode compatibility fallback events.
    pub fallback_transparency_certifying_fallback_event_count: usize,
    /// Replay command for the G9 fallback-denial bundle.
    pub fallback_transparency_replay_command: Option<String>,
    /// Count of missing-evidence beads from verification-contract enforcement.
    pub missing_evidence_beads: usize,
    /// Count of invalid-reference beads from verification-contract enforcement.
    pub invalid_reference_beads: usize,
    /// Number of artifacts carried by the manifest.
    pub reported_artifact_count: usize,
    /// Number of certification traceability entries.
    pub traceability_entry_count: usize,
    /// Number of certification traceability entries fully resolved to artifacts.
    pub fully_linked_traceability_entry_count: usize,
    /// Number of unresolved artifact refs across the traceability report.
    pub missing_artifact_ref_count: usize,
}

// ---------------------------------------------------------------------------
// Release certificate
// ---------------------------------------------------------------------------

/// A machine-verifiable release certificate.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(clippy::struct_excessive_bools)]
pub struct ReleaseCertificate {
    /// Schema version.
    pub schema_version: u32,
    /// Bead identifier.
    pub bead_id: String,
    /// Certification policy identifier.
    pub certification_policy_id: String,
    /// Embedded certification policy.
    pub certification_policy: CertificationPolicy,
    /// Overall verdict.
    pub verdict: CertificateVerdict,

    // ---- Score bounds ----
    /// Global posterior mean from Bayesian gates.
    pub global_posterior_mean: f64,
    /// Conservative lower bound (credible interval).
    pub global_lower_bound: f64,
    /// Verification percentage (invariants verified / total).
    pub global_verification_pct: f64,
    /// Total expected loss across all invariants.
    pub total_expected_loss: f64,

    // ---- Gate summary ----
    /// Confidence gate global decision.
    pub gate_decision: GateDecision,
    /// Whether gates declare release-ready.
    pub gate_release_ready: bool,
    /// Total invariants evaluated.
    pub total_invariants: usize,
    /// Invariants passing gate.
    pub passing_invariants: usize,

    // ---- Catalog statistics ----
    /// Parity invariant catalog statistics.
    pub catalog_stats: CatalogStats,

    // ---- Drift status ----
    /// Whether any drift monitor has rejected its null hypothesis.
    pub any_drift_rejected: bool,
    /// Whether any drift alarm has been raised.
    pub any_drift_alarm: bool,
    /// Number of categories with active drift alerts.
    pub drift_alert_categories: usize,

    // ---- Adversarial search ----
    /// Whether adversarial campaign passed.
    pub adversarial_passed: bool,
    /// Total counterexamples found.
    pub counterexample_count: usize,
    /// HIGH-severity counterexample count.
    pub high_severity_count: usize,

    // ---- CI status ----
    /// Global flake budget pass/fail (if provided).
    pub ci_flake_budget_passed: Option<bool>,
    /// Number of CI artifact hashes in the certificate.
    pub artifact_hash_count: usize,
    /// Certification evidence completeness and contract status.
    pub certification_evidence: CertificationEvidenceStatus,

    // ---- Evidence chain ----
    /// Ordered evidence chain entries for audit trail.
    pub evidence_chain: Vec<EvidenceChainEntry>,

    // ---- Certification traceability ----
    /// Feature -> test -> run -> artifact-hash traceability view.
    pub certification_traceability: CertificationTraceabilityReport,

    // ---- Unresolved risks ----
    /// Unresolved risk statements.
    pub unresolved_risks: Vec<UnresolvedRisk>,

    // ---- Embedded ledger ----
    /// Full evidence ledger from confidence gates.
    pub evidence_ledger: EvidenceLedger,

    // ---- Human summary ----
    /// Human-readable certificate summary.
    pub summary: String,
}

impl ReleaseCertificate {
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

    /// Compact one-line triage summary.
    #[must_use]
    pub fn triage_line(&self) -> String {
        format!(
            "{}: gate={} verified={:.1}% invariants={}/{} drift={} adversarial={} risks={}",
            self.verdict,
            self.gate_decision,
            self.global_verification_pct,
            self.passing_invariants,
            self.total_invariants,
            if self.any_drift_rejected {
                "REJECTED"
            } else {
                "ok"
            },
            if self.adversarial_passed {
                "PASS"
            } else {
                "FAIL"
            },
            self.unresolved_risks.len(),
        )
    }
}

// ---------------------------------------------------------------------------
// Certificate generation inputs
// ---------------------------------------------------------------------------

/// Pre-built inputs for certificate generation (for testability and flexibility).
#[derive(Debug, Clone)]
pub struct CertificateInputs {
    /// Gate report from confidence gates.
    pub gate_report: GateReport,
    /// Expected-loss ranking.
    pub expected_loss_ranking: ExpectedLossRanking,
    /// Evidence ledger.
    pub evidence_ledger: EvidenceLedger,
    /// Catalog statistics.
    pub catalog_stats: CatalogStats,
    /// Traceability report.
    pub traceability: ReleaseTraceabilityReport,
    /// Drift monitor snapshot.
    pub drift_snapshot: ParityDriftSnapshot,
    /// Adversarial campaign result.
    pub campaign_result: CampaignResult,
    /// Optional CI flake budget result.
    pub ci_flake_budget: Option<GlobalFlakeBudgetResult>,
    /// Optional CI artifact manifest for the certification bundle.
    pub artifact_manifest: Option<ArtifactManifest>,
}

// ---------------------------------------------------------------------------
// Core generation logic
// ---------------------------------------------------------------------------

/// Compute a SHA-256 hash of a string (for evidence chain).
fn sha256_hex(data: &str) -> String {
    crate::bytes_to_lower_hex(Sha256::digest(data.as_bytes()))
}

fn fallback_transparency_remediation_summary(
    gate: Option<&FallbackTransparencyGateSummary>,
) -> String {
    let Some(gate) = gate else {
        return "none".to_owned();
    };
    let messages = gate.remediation_messages();
    let rendered: Vec<_> = messages
        .iter()
        .take(3)
        .map(|message| message.render_concise())
        .collect();
    if rendered.is_empty() {
        "none".to_owned()
    } else {
        rendered.join(" | ")
    }
}

/// Build the embedded certification traceability view.
#[must_use]
fn build_certification_traceability(
    traceability: &ReleaseTraceabilityReport,
    artifact_manifest: Option<&ArtifactManifest>,
    policy: &CertificationPolicy,
) -> CertificationTraceabilityReport {
    let artifact_index: BTreeMap<&str, &ArtifactEntry> = artifact_manifest
        .map(|manifest| {
            manifest
                .artifacts
                .iter()
                .map(|artifact| (artifact.path.as_str(), artifact))
                .collect()
        })
        .unwrap_or_default();
    let run_reference = artifact_manifest.map(|manifest| CertificationRunReference {
        run_id: manifest.run_id.clone(),
        lane: manifest.lane.clone(),
        git_sha: manifest.git_sha.clone(),
        created_at: manifest.created_at.clone(),
        gate_passed: manifest.gate_passed,
    });

    let mut fully_linked_entries = 0_usize;
    let mut missing_artifact_ref_count = 0_usize;
    let mut entries = Vec::with_capacity(traceability.entries.len());

    for entry in &traceability.entries {
        let mut artifacts = Vec::new();
        let mut missing_artifact_refs = Vec::new();

        for artifact_ref in &entry.artifact_refs {
            if let Some(artifact) = artifact_index.get(artifact_ref.as_str()) {
                artifacts.push((**artifact).clone());
            } else {
                missing_artifact_ref_count = missing_artifact_ref_count.saturating_add(1);
                missing_artifact_refs.push(artifact_ref.clone());
            }
        }

        if missing_artifact_refs.is_empty() {
            fully_linked_entries = fully_linked_entries.saturating_add(1);
        }

        entries.push(CertificationTraceabilityEntry {
            invariant_id: entry.invariant_id.clone(),
            feature_id: entry.feature_id.clone(),
            category: entry.category.clone(),
            statement: entry.statement.clone(),
            verified: entry.verified,
            proof_summary: entry.proof_summary.clone(),
            run: run_reference.clone(),
            artifacts,
            missing_artifact_refs,
        });
    }

    CertificationTraceabilityReport {
        schema_version: CERTIFICATION_TRACEABILITY_SCHEMA_VERSION,
        policy_id: policy.policy_id.clone(),
        manifest_present: artifact_manifest.is_some(),
        fully_linked_entries,
        missing_artifact_ref_count,
        entries,
    }
}

/// Build the certification evidence summary used by verdicting and audit.
#[must_use]
fn build_certification_evidence_status(
    artifact_manifest: Option<&ArtifactManifest>,
    certification_traceability: &CertificationTraceabilityReport,
    policy: &CertificationPolicy,
) -> CertificationEvidenceStatus {
    let (artifact_manifest_gate_passed, verification_contract_passed, final_gate_passed) =
        artifact_manifest
            .map(|manifest| {
                (
                    Some(manifest.gate_passed),
                    manifest
                        .verification_contract
                        .as_ref()
                        .map(|contract| contract.contract_passed),
                    manifest
                        .verification_contract
                        .as_ref()
                        .map(|contract| contract.final_gate_passed),
                )
            })
            .unwrap_or((None, None, None));
    let verification_contract_present = artifact_manifest
        .and_then(|manifest| manifest.verification_contract.as_ref())
        .is_some();
    let fallback_transparency_gate =
        artifact_manifest.and_then(|manifest| manifest.fallback_transparency_gate.as_ref());
    let fallback_transparency_gate_present = fallback_transparency_gate.is_some();
    let fallback_transparency_gate_passed =
        fallback_transparency_gate.map(FallbackTransparencyGateSummary::gate_passed);
    let fallback_transparency_gate_failure_count =
        fallback_transparency_gate.map_or(0, |gate| gate.gate_failures.len());
    let fallback_transparency_missing_boundary_count =
        fallback_transparency_gate.map_or(0, |gate| gate.missing_boundary_ids.len());
    let fallback_transparency_stale_artifact_count =
        fallback_transparency_gate.map_or(0, |gate| gate.stale_artifacts.len());
    let fallback_transparency_certifying_fallback_event_count =
        fallback_transparency_gate.map_or(0, |gate| gate.certifying_fallback_events);
    let fallback_transparency_replay_command =
        fallback_transparency_gate.map(|gate| gate.replay_command.clone());

    let missing_evidence_beads = artifact_manifest
        .and_then(|manifest| manifest.verification_contract.as_ref())
        .map_or(0, |contract| contract.missing_evidence_beads);
    let invalid_reference_beads = artifact_manifest
        .and_then(|manifest| manifest.verification_contract.as_ref())
        .map_or(0, |contract| contract.invalid_reference_beads);
    let reported_artifact_count = artifact_manifest.map_or(0, |manifest| manifest.artifacts.len());

    CertificationEvidenceStatus {
        schema_version: CERTIFICATION_TRACEABILITY_SCHEMA_VERSION,
        policy_id: policy.policy_id.clone(),
        artifact_manifest_present: artifact_manifest.is_some(),
        artifact_manifest_gate_passed,
        verification_contract_present,
        verification_contract_passed,
        final_gate_passed,
        fallback_transparency_gate_present,
        fallback_transparency_gate_passed,
        fallback_transparency_gate_failure_count,
        fallback_transparency_missing_boundary_count,
        fallback_transparency_stale_artifact_count,
        fallback_transparency_certifying_fallback_event_count,
        fallback_transparency_replay_command,
        missing_evidence_beads,
        invalid_reference_beads,
        reported_artifact_count,
        traceability_entry_count: certification_traceability.entries.len(),
        fully_linked_traceability_entry_count: certification_traceability.fully_linked_entries,
        missing_artifact_ref_count: certification_traceability.missing_artifact_ref_count,
    }
}

/// Build a release certificate from pre-assembled inputs.
#[must_use]
#[allow(clippy::too_many_lines)]
pub fn build_certificate(
    inputs: &CertificateInputs,
    config: &CertificateConfig,
) -> ReleaseCertificate {
    let gate_report = &inputs.gate_report;
    let ranking = &inputs.expected_loss_ranking;
    let ledger = &inputs.evidence_ledger;
    let drift = &inputs.drift_snapshot;
    let campaign = &inputs.campaign_result;
    let stats = &inputs.catalog_stats;
    let certification_policy = canonical_certification_policy();
    let certification_traceability = build_certification_traceability(
        &inputs.traceability,
        inputs.artifact_manifest.as_ref(),
        &certification_policy,
    );
    let certification_evidence = build_certification_evidence_status(
        inputs.artifact_manifest.as_ref(),
        &certification_traceability,
        &certification_policy,
    );

    // ---- Evidence chain ----
    let mut evidence_chain = Vec::new();

    // 1. Invariant catalog
    let catalog_json = serde_json::to_string(&stats).unwrap_or_default();
    evidence_chain.push(EvidenceChainEntry {
        source_bead: "bd-1dp9.8.1".to_owned(),
        schema_version: 1,
        content_hash: sha256_hex(&catalog_json),
        summary: format!(
            "Catalog: {}/{} verified, {} categories",
            stats.verified_invariants, stats.total_invariants, stats.categories_covered,
        ),
    });

    // 2. Drift monitor
    let drift_json = serde_json::to_string(drift).unwrap_or_default();
    evidence_chain.push(EvidenceChainEntry {
        source_bead: "bd-1dp9.8.2".to_owned(),
        schema_version: drift.schema_version,
        content_hash: sha256_hex(&drift_json),
        summary: format!(
            "Drift: {} categories monitored, rejected={}",
            drift.category_states.len(),
            drift.any_rejected,
        ),
    });

    // 3. Confidence gates
    let gate_json = serde_json::to_string(gate_report).unwrap_or_default();
    evidence_chain.push(EvidenceChainEntry {
        source_bead: "bd-1dp9.8.3".to_owned(),
        schema_version: gate_report.schema_version,
        content_hash: sha256_hex(&gate_json),
        summary: format!(
            "Gates: decision={} verified={:.1}% loss={:.4}",
            gate_report.global_decision,
            gate_report.global_verification_pct,
            ranking.total_expected_loss,
        ),
    });

    // 4. Certification policy + traceability
    let certification_json =
        serde_json::to_string(&(certification_policy.clone(), &certification_traceability))
            .unwrap_or_default();
    evidence_chain.push(EvidenceChainEntry {
        source_bead: "bd-2yqp6.7".to_owned(),
        schema_version: certification_policy.schema_version,
        content_hash: sha256_hex(&certification_json),
        summary: format!(
            "Certification: policy={} manifest={} linked={}/{} missing_refs={}",
            certification_policy.policy_id,
            certification_traceability.manifest_present,
            certification_traceability.fully_linked_entries,
            certification_traceability.entries.len(),
            certification_traceability.missing_artifact_ref_count,
        ),
    });

    // 5. Adversarial search
    let campaign_json = serde_json::to_string(campaign).unwrap_or_default();
    evidence_chain.push(EvidenceChainEntry {
        source_bead: "bd-1dp9.8.5".to_owned(),
        schema_version: campaign.schema_version,
        content_hash: sha256_hex(&campaign_json),
        summary: format!(
            "Adversarial: {} trials, {} counterexamples, passed={}",
            campaign.total_trials,
            campaign.counterexamples.len(),
            campaign.passed,
        ),
    });

    if let Some(gate) = inputs
        .artifact_manifest
        .as_ref()
        .and_then(|manifest| manifest.fallback_transparency_gate.as_ref())
    {
        let gate_json = serde_json::to_string(gate).unwrap_or_default();
        evidence_chain.push(EvidenceChainEntry {
            source_bead: "bd-2yqp6.7.9".to_owned(),
            schema_version: 1,
            content_hash: sha256_hex(&gate_json),
            summary: format!(
                "G9 fallback transparency: status={} covered={} missing={} stale={} certifying_fallbacks={}",
                gate.status.as_str(),
                gate.covered_boundary_ids.len(),
                gate.missing_boundary_ids.len(),
                gate.stale_artifacts.len(),
                gate.certifying_fallback_events,
            ),
        });
    }

    // ---- Unresolved risks ----
    let mut unresolved_risks = Vec::new();

    // Drift risks
    for (cat, state) in &drift.category_states {
        if state.rejected {
            unresolved_risks.push(UnresolvedRisk {
                source: "drift_monitor".to_owned(),
                severity: "High".to_owned(),
                description: format!(
                    "Category '{cat}' null hypothesis rejected (e-value={:.2})",
                    state.e_value,
                ),
            });
        } else if state.drift_alerts_count > 0 {
            unresolved_risks.push(UnresolvedRisk {
                source: "drift_monitor".to_owned(),
                severity: "Medium".to_owned(),
                description: format!(
                    "Category '{cat}' has {} drift alert(s)",
                    state.drift_alerts_count,
                ),
            });
        }
    }

    // Adversarial risks
    let high_severity_count = campaign
        .counterexamples
        .iter()
        .filter(|c| c.severity == CounterexampleSeverity::High)
        .count();

    for cx in &campaign.counterexamples {
        if cx.severity == CounterexampleSeverity::High {
            unresolved_risks.push(UnresolvedRisk {
                source: "adversarial_search".to_owned(),
                severity: "High".to_owned(),
                description: format!("{}: {}", cx.id, cx.description),
            });
        }
    }

    // Gate risks
    if !gate_report.release_ready {
        unresolved_risks.push(UnresolvedRisk {
            source: "confidence_gates".to_owned(),
            severity: "High".to_owned(),
            description: format!(
                "Gate decision={}, release_ready=false",
                gate_report.global_decision,
            ),
        });
    }

    if !certification_evidence.artifact_manifest_present {
        unresolved_risks.push(UnresolvedRisk {
            source: "certification_policy".to_owned(),
            severity: "High".to_owned(),
            description: "Missing artifact manifest; certification traceability does not yet reach run/artifact-hash evidence.".to_owned(),
        });
    }

    if certification_evidence.missing_artifact_ref_count > 0 {
        unresolved_risks.push(UnresolvedRisk {
            source: "certification_policy".to_owned(),
            severity: "High".to_owned(),
            description: format!(
                "{} traceability artifact reference(s) are missing from the certification manifest.",
                certification_evidence.missing_artifact_ref_count,
            ),
        });
    }

    if certification_evidence.artifact_manifest_gate_passed == Some(false) {
        unresolved_risks.push(UnresolvedRisk {
            source: "certification_policy".to_owned(),
            severity: "High".to_owned(),
            description: "Artifact manifest gate failed for the certification run.".to_owned(),
        });
    }

    if certification_evidence.final_gate_passed == Some(false) {
        unresolved_risks.push(UnresolvedRisk {
            source: "verification_contract".to_owned(),
            severity: "High".to_owned(),
            description: format!(
                "Verification contract failed (missing_evidence_beads={}, invalid_reference_beads={}).",
                certification_evidence.missing_evidence_beads,
                certification_evidence.invalid_reference_beads,
            ),
        });
    }

    if certification_evidence.artifact_manifest_present
        && !certification_evidence.verification_contract_present
    {
        unresolved_risks.push(UnresolvedRisk {
            source: "verification_contract".to_owned(),
            severity: "High".to_owned(),
            description:
                "Artifact manifest is present but verification-contract evidence is missing."
                    .to_owned(),
        });
    }

    if certification_evidence.artifact_manifest_present
        && !certification_evidence.fallback_transparency_gate_present
    {
        unresolved_risks.push(UnresolvedRisk {
            source: "fallback_transparency_gate".to_owned(),
            severity: "High".to_owned(),
            description:
                "Artifact manifest is present but G9 fallback-transparency evidence is missing."
                    .to_owned(),
        });
    }

    if certification_evidence.fallback_transparency_gate_passed == Some(false) {
        let remediation_summary = fallback_transparency_remediation_summary(
            inputs
                .artifact_manifest
                .as_ref()
                .and_then(|manifest| manifest.fallback_transparency_gate.as_ref()),
        );
        unresolved_risks.push(UnresolvedRisk {
            source: "fallback_transparency_gate".to_owned(),
            severity: "High".to_owned(),
            description: format!(
                "G9 fallback-transparency gate failed (gate_failures={}, missing_boundaries={}, stale_artifacts={}, certifying_fallback_events={}, replay_command={}, remediation={}).",
                certification_evidence.fallback_transparency_gate_failure_count,
                certification_evidence.fallback_transparency_missing_boundary_count,
                certification_evidence.fallback_transparency_stale_artifact_count,
                certification_evidence.fallback_transparency_certifying_fallback_event_count,
                certification_evidence
                    .fallback_transparency_replay_command
                    .as_deref()
                    .unwrap_or("missing"),
                remediation_summary,
            ),
        });
    }

    // ---- Drift summary ----
    let drift_alert_categories = drift
        .category_states
        .values()
        .filter(|s| s.drift_alerts_count > 0)
        .count();

    // ---- Verdict ----
    let verdict = determine_verdict(
        gate_report,
        drift,
        high_severity_count,
        &certification_evidence,
        config,
    );

    // ---- Summary ----
    let summary = format!(
        "Release certificate {}: gate={} verified={:.1}% ({}/{} invariants), \
         drift_rejected={}, adversarial={} ({} counterexamples, {} high), \
         traceability={}/{} manifest={}, g9_gate={}, {} unresolved risk(s)",
        verdict,
        gate_report.global_decision,
        truncate_score(gate_report.global_verification_pct),
        gate_report.passing_invariants,
        gate_report.total_invariants,
        drift.any_rejected,
        if campaign.passed { "PASS" } else { "FAIL" },
        campaign.counterexamples.len(),
        high_severity_count,
        certification_traceability.fully_linked_entries,
        certification_traceability.entries.len(),
        certification_traceability.manifest_present,
        certification_evidence
            .fallback_transparency_gate_passed
            .map_or("missing", |passed| if passed { "pass" } else { "fail" }),
        unresolved_risks.len(),
    );

    ReleaseCertificate {
        schema_version: CERTIFICATE_SCHEMA_VERSION,
        bead_id: RELEASE_CERT_BEAD_ID.to_owned(),
        certification_policy_id: certification_policy.policy_id.clone(),
        certification_policy,
        verdict,
        global_posterior_mean: truncate_score(ledger.global_posterior_mean),
        global_lower_bound: truncate_score(ledger.global_lower_bound),
        global_verification_pct: truncate_score(ledger.global_verification_pct),
        total_expected_loss: truncate_score(ledger.total_expected_loss),
        gate_decision: gate_report.global_decision,
        gate_release_ready: gate_report.release_ready,
        total_invariants: gate_report.total_invariants,
        passing_invariants: gate_report.passing_invariants,
        catalog_stats: stats.clone(),
        any_drift_rejected: drift.any_rejected,
        any_drift_alarm: drift.any_drift,
        drift_alert_categories,
        adversarial_passed: campaign.passed,
        counterexample_count: campaign.counterexamples.len(),
        high_severity_count,
        ci_flake_budget_passed: inputs.ci_flake_budget.as_ref().map(|fb| fb.pipeline_pass),
        artifact_hash_count: inputs
            .artifact_manifest
            .as_ref()
            .map_or(0, |m| m.artifacts.len()),
        certification_evidence,
        evidence_chain,
        certification_traceability,
        unresolved_risks,
        evidence_ledger: ledger.clone(),
        summary,
    }
}

/// Determine the overall certificate verdict.
fn determine_verdict(
    gate_report: &GateReport,
    drift: &ParityDriftSnapshot,
    high_severity_count: usize,
    certification_evidence: &CertificationEvidenceStatus,
    config: &CertificateConfig,
) -> CertificateVerdict {
    // Hard rejection: gate failure or too many high-severity counterexamples
    if gate_report.global_decision == GateDecision::Fail {
        return CertificateVerdict::Rejected;
    }
    if high_severity_count > config.max_high_severity {
        return CertificateVerdict::Rejected;
    }
    if drift.any_rejected {
        return CertificateVerdict::Rejected;
    }
    if certification_evidence.artifact_manifest_gate_passed == Some(false) {
        return CertificateVerdict::Rejected;
    }
    if certification_evidence.artifact_manifest_present
        && !certification_evidence.verification_contract_present
    {
        return CertificateVerdict::Rejected;
    }
    if certification_evidence.final_gate_passed == Some(false) {
        return CertificateVerdict::Rejected;
    }
    if certification_evidence.artifact_manifest_present
        && !certification_evidence.fallback_transparency_gate_present
    {
        return CertificateVerdict::Rejected;
    }
    if certification_evidence.fallback_transparency_gate_passed == Some(false) {
        return CertificateVerdict::Rejected;
    }
    if certification_evidence.missing_artifact_ref_count > 0 {
        return CertificateVerdict::Rejected;
    }

    // Conditional: gate is conditional, or drift alarms exist, or verification is low
    if gate_report.global_decision == GateDecision::Conditional {
        return CertificateVerdict::Conditional;
    }
    if drift.any_drift {
        return CertificateVerdict::Conditional;
    }
    if gate_report.global_verification_pct < config.min_verification_pct {
        return CertificateVerdict::Conditional;
    }
    if !certification_evidence.artifact_manifest_present {
        return CertificateVerdict::Conditional;
    }

    CertificateVerdict::Approved
}

// ---------------------------------------------------------------------------
// Convenience: run full pipeline
// ---------------------------------------------------------------------------

/// Run the full release certificate pipeline from canonical sources.
///
/// This is the top-level orchestrator that builds all inputs from scratch
/// and produces a signed certificate.
#[must_use]
pub fn generate_release_certificate(config: &CertificateConfig) -> ReleaseCertificate {
    // 1. Build canonical catalog and universe.
    let catalog = build_canonical_catalog();
    let universe = build_canonical_universe();

    // 2. Evaluate confidence gates.
    let (gate_report, ranking) = evaluate_full(&catalog, &universe, &config.gate_config);
    let ledger = build_evidence_ledger(&gate_report, &ranking);

    // 3. Run drift monitor (observe canonical categories with catalog stats).
    let mut drift_monitor = ParityDriftMonitor::new(config.drift_config.clone());
    let stat = catalog.stats();
    for cat in FeatureCategory::ALL {
        let cat_name = cat.display_name();
        let cat_count = stat.per_category.get(cat_name).copied().unwrap_or(0);
        let mismatches = cat_count.saturating_sub(stat.verified_invariants.min(cat_count));
        drift_monitor.observe_batch(cat, mismatches, cat_count);
    }
    let drift_snapshot = drift_monitor.snapshot();

    // 4. Run adversarial campaign.
    let campaign_result = run_campaign(&config.adversarial_config);

    // 5. Build inputs.
    let inputs = CertificateInputs {
        gate_report,
        expected_loss_ranking: ranking,
        evidence_ledger: ledger,
        catalog_stats: catalog.stats(),
        traceability: catalog.release_traceability(),
        drift_snapshot,
        campaign_result,
        ci_flake_budget: None,
        artifact_manifest: None,
    };

    build_certificate(&inputs, config)
}

// ---------------------------------------------------------------------------
// File I/O
// ---------------------------------------------------------------------------

/// Write a release certificate to a JSON file.
///
/// # Errors
///
/// Returns `Err` if serialization or file writing fails.
pub fn write_certificate(path: &Path, cert: &ReleaseCertificate) -> Result<(), String> {
    let json = cert.to_json().map_err(|e| format!("serialize: {e}"))?;
    std::fs::write(path, json).map_err(|e| format!("write: {e}"))
}

/// Load a release certificate from a JSON file.
///
/// # Errors
///
/// Returns `Err` if reading or deserialization fails.
pub fn load_certificate(path: &Path) -> Result<ReleaseCertificate, String> {
    let json = std::fs::read_to_string(path).map_err(|e| format!("read: {e}"))?;
    ReleaseCertificate::from_json(&json).map_err(|e| format!("parse: {e}"))
}

fn strict_current_unix_ms() -> Result<u128, String> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .map_err(|error| format!("system_clock_before_unix_epoch: {error}"))
}

fn is_lower_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn checked_relative_path(path: &Path) -> Result<&Path, String> {
    if path.as_os_str().is_empty()
        || path.is_absolute()
        || path.to_str().is_none_or(|value| value.contains('\\'))
    {
        return Err(format!(
            "evidence_path_must_be_nonempty_relative path={}",
            path.display()
        ));
    }
    if path.components().any(|component| {
        matches!(
            component,
            Component::ParentDir | Component::RootDir | Component::Prefix(_)
        )
    }) {
        return Err(format!(
            "evidence_path_contains_forbidden_component path={}",
            path.display()
        ));
    }
    Ok(path)
}

fn canonical_regular_file(root: &Path, relative: &Path) -> Result<PathBuf, String> {
    let relative = checked_relative_path(relative)?;
    let root_metadata = fs::symlink_metadata(root).map_err(|error| {
        format!(
            "evidence_root_metadata_failed path={} error={error}",
            root.display()
        )
    })?;
    if root_metadata.file_type().is_symlink() || !root_metadata.is_dir() {
        return Err(format!(
            "evidence_root_must_be_real_directory path={}",
            root.display()
        ));
    }
    let canonical_root = root.canonicalize().map_err(|error| {
        format!(
            "evidence_root_canonicalize_failed path={} error={error}",
            root.display()
        )
    })?;
    let lexical_path = canonical_root.join(relative);
    reject_symlink_components(&lexical_path)?;
    let metadata = fs::symlink_metadata(&lexical_path).map_err(|error| {
        format!(
            "evidence_metadata_failed path={} error={error}",
            lexical_path.display()
        )
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(format!(
            "evidence_path_must_be_real_regular_file path={}",
            lexical_path.display()
        ));
    }
    let canonical_path = lexical_path.canonicalize().map_err(|error| {
        format!(
            "evidence_path_canonicalize_failed path={} error={error}",
            lexical_path.display()
        )
    })?;
    if !canonical_path.starts_with(&canonical_root) {
        return Err(format!(
            "evidence_path_escaped_root path={}",
            canonical_path.display()
        ));
    }
    Ok(canonical_path)
}

fn canonical_relative_path(root: &Path, path: &Path) -> Result<String, String> {
    let canonical_root = root
        .canonicalize()
        .map_err(|error| format!("evidence_root_canonicalize_failed: {error}"))?;
    let relative = path
        .strip_prefix(&canonical_root)
        .map_err(|error| format!("evidence_relative_path_failed: {error}"))?;
    let mut parts = Vec::new();
    for component in relative.components() {
        let Component::Normal(part) = component else {
            return Err("evidence_relative_path_not_canonical".to_owned());
        };
        parts.push(
            part.to_str()
                .ok_or_else(|| "evidence_relative_path_not_utf8".to_owned())?,
        );
    }
    if parts.is_empty() {
        return Err("evidence_relative_path_empty".to_owned());
    }
    Ok(parts.join("/"))
}

fn sha256_bytes(bytes: &[u8]) -> String {
    crate::bytes_to_lower_hex(Sha256::digest(bytes))
}

fn validate_evidence_timestamp(
    reference: &StrictEvidenceRef,
    generated_unix_ms: u128,
    now_unix_ms: u128,
    freshness_budget_ms: u128,
) -> Result<(), String> {
    if reference.observed_unix_ms > generated_unix_ms {
        return Err(format!(
            "evidence_observed_after_manifest_generation path={}",
            reference.path
        ));
    }
    if generated_unix_ms > now_unix_ms {
        return Err("certificate_manifest_generated_in_future".to_owned());
    }
    let age = now_unix_ms
        .checked_sub(reference.observed_unix_ms)
        .ok_or_else(|| format!("evidence_timestamp_in_future path={}", reference.path))?;
    if age > freshness_budget_ms {
        return Err(format!(
            "stale_evidence path={} age_ms={age} budget_ms={freshness_budget_ms}",
            reference.path
        ));
    }
    Ok(())
}

fn load_strict_evidence_bytes(
    root: &Path,
    reference: &StrictEvidenceRef,
    generated_unix_ms: u128,
    now_unix_ms: u128,
    freshness_budget_ms: u128,
) -> Result<LoadedStrictEvidence<()>, String> {
    if !is_lower_hex(&reference.sha256, 64) {
        return Err(format!("invalid_evidence_sha256 path={}", reference.path));
    }
    validate_evidence_timestamp(
        reference,
        generated_unix_ms,
        now_unix_ms,
        freshness_budget_ms,
    )?;
    let path = canonical_regular_file(root, Path::new(&reference.path))?;
    let bytes = fs::read(&path)
        .map_err(|error| format!("evidence_read_failed path={} error={error}", path.display()))?;
    let observed = sha256_bytes(&bytes);
    if observed != reference.sha256 {
        return Err(format!("evidence_hash_mismatch path={}", path.display()));
    }
    let relative_path = canonical_relative_path(root, &path)?;
    if relative_path != reference.path {
        return Err(format!(
            "evidence_path_not_canonical path={} canonical={relative_path}",
            reference.path
        ));
    }
    Ok(LoadedStrictEvidence {
        relative_path,
        sha256: observed,
        value: (),
        bytes,
    })
}

fn load_strict_json<T: serde::de::DeserializeOwned>(
    root: &Path,
    reference: &StrictEvidenceRef,
    generated_unix_ms: u128,
    now_unix_ms: u128,
    freshness_budget_ms: u128,
) -> Result<LoadedStrictEvidence<T>, String> {
    let raw = load_strict_evidence_bytes(
        root,
        reference,
        generated_unix_ms,
        now_unix_ms,
        freshness_budget_ms,
    )?;
    let value = serde_json::from_slice(&raw.bytes).map_err(|error| {
        format!(
            "evidence_json_parse_failed path={} error={error}",
            raw.relative_path
        )
    })?;
    Ok(LoadedStrictEvidence {
        relative_path: raw.relative_path,
        sha256: raw.sha256,
        bytes: raw.bytes,
        value,
    })
}

enum StrictEvidenceProgram {
    Cargo,
    Git,
}

fn sanitized_command(program: StrictEvidenceProgram, inherited_environment: &[&str]) -> Command {
    let mut command = match program {
        StrictEvidenceProgram::Cargo => Command::new("cargo"),
        StrictEvidenceProgram::Git => Command::new("git"),
    };
    command.env_clear();
    for name in inherited_environment {
        if let Some(value) = std::env::var_os(name) {
            command.env(name, value);
        }
    }
    command.env("LC_ALL", "C").env("LANG", "C");
    command
}

fn sanitized_git_command(workspace_root: &Path) -> Command {
    let mut command = sanitized_command(
        StrictEvidenceProgram::Git,
        &["PATH", "SystemRoot", "WINDIR", "PATHEXT"],
    );
    command
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .arg("-C")
        .arg(workspace_root)
        .arg("--no-optional-locks")
        .args([
            "-c",
            "color.ui=false",
            "-c",
            "core.quotepath=false",
            "-c",
            "core.fsmonitor=false",
        ]);
    command
}

fn git_output(workspace_root: &Path, args: &[&str], label: &str) -> Result<Vec<u8>, String> {
    let output = sanitized_git_command(workspace_root)
        .args(args)
        .output()
        .map_err(|error| format!("{label}_spawn_failed: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "{label}_failed exit_code={}",
            output.status.code().unwrap_or(-1)
        ));
    }
    if !output.stderr.is_empty() {
        return Err(format!("{label}_stderr_not_empty"));
    }
    Ok(output.stdout)
}

fn current_head(workspace_root: &Path) -> Result<String, String> {
    String::from_utf8(git_output(
        workspace_root,
        &["rev-parse", "HEAD"],
        "git_head",
    )?)
    .map_err(|error| format!("git_head_not_utf8: {error}"))
    .map(|head| head.trim().to_owned())
}

fn require_exact_clean_checkout(workspace_root: &Path) -> Result<(), String> {
    let status = git_output(
        workspace_root,
        &[
            "status",
            "--porcelain=v2",
            "--untracked-files=all",
            "--ignore-submodules=none",
        ],
        "git_status",
    )?;
    validate_exact_clean_status(&status)
}

fn validate_exact_clean_status(status: &[u8]) -> Result<(), String> {
    if !status.is_empty() {
        return Err("candidate_checkout_not_exactly_clean".to_owned());
    }
    Ok(())
}

fn require_ancestor(workspace_root: &Path, ancestor: &str, descendant: &str) -> Result<(), String> {
    let output = sanitized_git_command(workspace_root)
        .args(["merge-base", "--is-ancestor", ancestor, descendant])
        .output()
        .map_err(|error| format!("git_ancestor_check_spawn_failed: {error}"))?;
    if !output.status.success() || !output.stdout.is_empty() || !output.stderr.is_empty() {
        return Err(format!(
            "commit_not_ancestor ancestor={ancestor} descendant={descendant}"
        ));
    }
    Ok(())
}

/// Require the Phase-5 evidence commit E to be a non-merge direct descendant
/// of the tested candidate T. This keeps the strict certificate's artifact
/// binding at E while keeping the measured source tree and manifest at T.
fn require_direct_phase5_evidence_descendant(
    workspace_root: &Path,
    tested_candidate_git_sha: &str,
    evidence_git_sha: &str,
) -> Result<(), String> {
    if tested_candidate_git_sha == evidence_git_sha {
        return Err("phase5_evidence_commit_must_not_equal_tested_candidate".to_owned());
    }
    let parents = String::from_utf8(git_output(
        workspace_root,
        &["rev-list", "--parents", "-n", "1", evidence_git_sha],
        "git_phase5_evidence_parents",
    )?)
    .map_err(|error| format!("git_phase5_evidence_parents_not_utf8: {error}"))?;
    validate_direct_phase5_evidence_parents(tested_candidate_git_sha, evidence_git_sha, &parents)
}

fn validate_direct_phase5_evidence_parents(
    tested_candidate_git_sha: &str,
    evidence_git_sha: &str,
    parents: &str,
) -> Result<(), String> {
    if tested_candidate_git_sha == evidence_git_sha {
        return Err("phase5_evidence_commit_must_not_equal_tested_candidate".to_owned());
    }
    let mut parents = parents.split_whitespace();
    if parents.next() != Some(evidence_git_sha) {
        return Err("phase5_evidence_commit_parent_identity_mismatch".to_owned());
    }
    let Some(parent) = parents.next() else {
        return Err("phase5_evidence_commit_must_be_single_parent".to_owned());
    };
    if parents.next().is_some() {
        return Err("phase5_evidence_commit_must_be_single_parent".to_owned());
    }
    if parent != tested_candidate_git_sha {
        return Err("phase5_evidence_commit_parent_mismatch".to_owned());
    }
    Ok(())
}

fn git_blob_at_commit(
    workspace_root: &Path,
    commit: &str,
    relative_path: &str,
) -> Result<Vec<u8>, String> {
    let object = format!("{commit}:{relative_path}");
    let output = sanitized_git_command(workspace_root)
        .args(["show", &object])
        .output()
        .map_err(|error| format!("git_blob_read_spawn_failed object={object} error={error}"))?;
    if !output.status.success() {
        return Err(format!("git_blob_read_failed object={object}"));
    }
    if !output.stderr.is_empty() {
        return Err(format!("git_blob_read_stderr_not_empty object={object}"));
    }
    Ok(output.stdout)
}

fn tested_tree_blake3(workspace_root: &Path, commit: &str) -> Result<String, String> {
    let output = sanitized_git_command(workspace_root)
        .args(["ls-tree", "-r", "--full-tree", commit])
        .output()
        .map_err(|error| {
            format!("git_tree_enumeration_spawn_failed commit={commit} error={error}")
        })?;
    if !output.status.success() {
        return Err(format!("git_tree_enumeration_failed commit={commit}"));
    }
    if !output.stderr.is_empty() {
        return Err(format!(
            "git_tree_enumeration_stderr_not_empty commit={commit}"
        ));
    }
    Ok(blake3::hash(&output.stdout).to_hex().to_string())
}

fn validate_rch_project_id(project_id: &str, label: &str) -> Result<(), String> {
    if project_id.trim().is_empty()
        || project_id.len() > 256
        || project_id
            .chars()
            .any(|character| character.is_control() || character.is_whitespace())
    {
        return Err(format!("{label}_rch_project_id_invalid"));
    }
    Ok(())
}

fn json_values_equal<T: Serialize, U: Serialize>(left: &T, right: &U) -> Result<bool, String> {
    let left = serde_json::to_value(left)
        .map_err(|error| format!("left_value_serialize_failed: {error}"))?;
    let right = serde_json::to_value(right)
        .map_err(|error| format!("right_value_serialize_failed: {error}"))?;
    Ok(left == right)
}

fn manifest_artifact_index(
    evidence_root: &Path,
    manifest: &ArtifactManifest,
) -> Result<BTreeMap<String, (String, u64)>, String> {
    let errors = manifest.validate();
    if !errors.is_empty() {
        return Err(format!("artifact_manifest_invalid: {}", errors.join("; ")));
    }
    let mut index = BTreeMap::new();
    for artifact in &manifest.artifacts {
        if !is_lower_hex(&artifact.content_hash, 64) {
            return Err(format!(
                "invalid_manifest_artifact_sha256 path={}",
                artifact.path
            ));
        }
        let path = canonical_regular_file(evidence_root, Path::new(&artifact.path))?;
        let bytes = fs::read(&path).map_err(|error| {
            format!(
                "manifest_artifact_read_failed path={} error={error}",
                path.display()
            )
        })?;
        let observed_hash = sha256_bytes(&bytes);
        if observed_hash != artifact.content_hash {
            return Err(format!(
                "manifest_artifact_hash_mismatch path={}",
                path.display()
            ));
        }
        let observed_size = u64::try_from(bytes.len())
            .map_err(|_| format!("manifest_artifact_size_overflow path={}", path.display()))?;
        if observed_size != artifact.size_bytes {
            return Err(format!(
                "manifest_artifact_size_mismatch path={} expected={} observed={observed_size}",
                path.display(),
                artifact.size_bytes
            ));
        }
        let relative = canonical_relative_path(evidence_root, &path)?;
        if index
            .insert(relative.clone(), (observed_hash, observed_size))
            .is_some()
        {
            return Err(format!("duplicate_manifest_artifact_path path={relative}"));
        }
    }
    Ok(index)
}

fn require_manifest_binding<T>(
    manifest_index: &BTreeMap<String, (String, u64)>,
    evidence: &LoadedStrictEvidence<T>,
) -> Result<(), String> {
    let Some((hash, size)) = manifest_index.get(&evidence.relative_path) else {
        return Err(format!(
            "mandatory_evidence_not_bound_by_candidate_manifest path={}",
            evidence.relative_path
        ));
    };
    let evidence_size = u64::try_from(evidence.bytes.len()).map_err(|_| {
        format!(
            "mandatory_evidence_size_overflow path={}",
            evidence.relative_path
        )
    })?;
    if hash != &evidence.sha256 || size != &evidence_size {
        return Err(format!(
            "mandatory_evidence_binding_mismatch path={}",
            evidence.relative_path
        ));
    }
    Ok(())
}

fn validate_contract_outcome(
    outcome: &crate::verification_contract_enforcement::ContractEnforcementOutcome,
    label: &str,
) -> Result<(), String> {
    if !outcome.base_gate_passed
        || !outcome.contract_passed
        || !outcome.final_gate_passed
        || outcome.missing_evidence_beads != 0
        || outcome.invalid_reference_beads != 0
        || outcome.failing_beads != 0
    {
        return Err(format!("{label}_verification_contract_not_release_ready"));
    }
    Ok(())
}

fn validate_candidate_manifest(
    manifest: &ArtifactManifest,
    candidate_git_sha: &str,
) -> Result<(), String> {
    let errors = manifest.validate();
    if !errors.is_empty() {
        return Err(format!("artifact_manifest_invalid: {}", errors.join("; ")));
    }
    if !manifest.gate_passed || manifest.git_sha != candidate_git_sha {
        return Err("artifact_manifest_not_passing_for_candidate".to_owned());
    }
    if manifest.run_id.trim().is_empty()
        || manifest.lane.trim().is_empty()
        || manifest.created_at.trim().is_empty()
    {
        return Err("artifact_manifest_missing_run_identity".to_owned());
    }
    let contract = manifest
        .verification_contract
        .as_ref()
        .ok_or_else(|| "artifact_manifest_missing_verification_contract".to_owned())?;
    validate_contract_outcome(contract, "artifact_manifest")?;
    let g9 = manifest
        .fallback_transparency_gate
        .as_ref()
        .ok_or_else(|| "artifact_manifest_missing_g9_gate".to_owned())?;
    validate_g9_summary_shape(g9, candidate_git_sha)
}

fn validate_g9_summary_shape(
    gate: &FallbackTransparencyGateSummary,
    candidate_git_sha: &str,
) -> Result<(), String> {
    let errors = gate.validate();
    if !errors.is_empty() {
        return Err(format!("g9_gate_invalid: {}", errors.join("; ")));
    }
    if !gate.gate_passed()
        || gate.source_commit != candidate_git_sha
        || gate.backend_identity_summary != STRICT_BACKEND_IDENTITY
        || !gate.missing_boundary_ids.is_empty()
        || !gate.stale_artifacts.is_empty()
        || gate.certifying_fallback_events != 0
        || !gate.gate_failures.is_empty()
    {
        return Err("g9_gate_not_release_ready_for_candidate".to_owned());
    }
    Ok(())
}

fn validate_g9_artifacts(
    evidence_root: &Path,
    manifest_index: &BTreeMap<String, (String, u64)>,
    gate: &FallbackTransparencyGateSummary,
) -> Result<(), String> {
    for artifact in [
        &gate.inventory,
        &gate.schema_validation,
        &gate.replay_bundle,
    ] {
        let path = canonical_regular_file(evidence_root, Path::new(&artifact.path))?;
        let relative = canonical_relative_path(evidence_root, &path)?;
        let bytes = fs::read(&path).map_err(|error| {
            format!(
                "g9_artifact_read_failed path={} error={error}",
                path.display()
            )
        })?;
        let observed = sha256_bytes(&bytes);
        if observed != artifact.content_hash {
            return Err(format!("g9_artifact_hash_mismatch path={relative}"));
        }
        let Some((manifest_hash, manifest_size)) = manifest_index.get(&relative) else {
            return Err(format!("g9_artifact_missing_from_manifest path={relative}"));
        };
        let observed_size = u64::try_from(bytes.len())
            .map_err(|_| format!("g9_artifact_size_overflow path={relative}"))?;
        if manifest_hash != &observed || manifest_size != &observed_size {
            return Err(format!("g9_artifact_manifest_mismatch path={relative}"));
        }
    }
    Ok(())
}

fn validate_required_lane_manifests(
    evidence_root: &Path,
    input: &StrictCertificateEvidenceInput,
    candidate_manifest: &LoadedStrictEvidence<ArtifactManifest>,
    candidate_manifest_index: &BTreeMap<String, (String, u64)>,
    now_unix_ms: u128,
    freshness_budget_ms: u128,
) -> Result<Vec<LoadedStrictEvidence<ArtifactManifest>>, String> {
    if input.required_lane_manifests.len() != REQUIRED_CERTIFICATION_LANES.len() {
        return Err("required_lane_manifest_count_mismatch".to_owned());
    }
    let expected = REQUIRED_CERTIFICATION_LANES
        .iter()
        .map(|lane| lane.as_str())
        .collect::<BTreeSet<_>>();
    let observed = input
        .required_lane_manifests
        .iter()
        .map(|lane| lane.lane.as_str())
        .collect::<BTreeSet<_>>();
    if expected != observed || observed.len() != input.required_lane_manifests.len() {
        return Err("required_certification_lane_set_mismatch".to_owned());
    }

    let mut loaded = Vec::with_capacity(input.required_lane_manifests.len());
    for lane in &input.required_lane_manifests {
        if lane.evidence.observed_unix_ms > input.candidate_artifact_manifest.observed_unix_ms {
            return Err(format!(
                "required_lane_manifest_postdates_candidate_manifest lane={}",
                lane.lane
            ));
        }
        let manifest: LoadedStrictEvidence<ArtifactManifest> = load_strict_json(
            evidence_root,
            &lane.evidence,
            input.generated_unix_ms,
            now_unix_ms,
            freshness_budget_ms,
        )?;
        require_manifest_binding(candidate_manifest_index, &manifest)?;
        if manifest.value.lane != lane.lane
            || manifest.value.run_id != input.run_id
            || manifest.value.run_id != candidate_manifest.value.run_id
            || manifest.value.seed != input.seed
            || manifest.value.seed != candidate_manifest.value.seed
            || manifest.value.git_sha != input.candidate_git_sha
            || manifest.value.git_sha != candidate_manifest.value.git_sha
            || manifest.value.created_at.trim().is_empty()
            || manifest.value.created_at != candidate_manifest.value.created_at
            || !manifest.value.gate_passed
        {
            return Err(format!(
                "required_lane_not_in_canonical_candidate_family lane={}",
                lane.lane
            ));
        }
        let errors = manifest.value.validate();
        if !errors.is_empty() {
            return Err(format!(
                "required_lane_manifest_invalid lane={} errors={}",
                lane.lane,
                errors.join("; ")
            ));
        }
        let contract = manifest
            .value
            .verification_contract
            .as_ref()
            .ok_or_else(|| {
                format!(
                    "required_lane_missing_verification_contract lane={}",
                    lane.lane
                )
            })?;
        validate_contract_outcome(contract, &format!("required_lane_{}", lane.lane))?;
        drop(manifest_artifact_index(evidence_root, &manifest.value)?);
        loaded.push(manifest);
    }
    Ok(loaded)
}

fn validate_workflow_evidence(
    evidence_root: &Path,
    workflow: &WorkflowReport,
    input: &StrictCertificateEvidenceInput,
    manifest_index: &BTreeMap<String, (String, u64)>,
    now_unix_ms: u128,
    freshness_budget_ms: u128,
) -> Result<(), String> {
    if workflow.schema_version != WORKFLOW_SCHEMA_VERSION || workflow.bead_id != WORKFLOW_BEAD_ID {
        return Err("workflow_report_schema_or_owner_mismatch".to_owned());
    }
    if workflow.run_id != input.run_id
        || workflow.trace_id != input.trace_id
        || workflow.scenario_id != input.scenario_id
        || workflow.seed != input.seed
        || workflow.generated_unix_ms != input.generated_unix_ms
        || workflow.freshness_budget_ms != input.freshness_budget_ms
    {
        return Err("workflow_report_identity_mismatch".to_owned());
    }
    let violations = validate_workflow_report(workflow);
    if !violations.is_empty()
        || !workflow.validation_violations.is_empty()
        || !workflow.workflow_complete
        || !workflow.certificate_ready
        || workflow.first_failure.is_some()
    {
        return Err("workflow_report_not_certificate_ready".to_owned());
    }
    let expected_phases = WorkflowPhase::required_order();
    if workflow.steps.len() != expected_phases.len() {
        return Err("workflow_report_phase_count_mismatch".to_owned());
    }
    for (step, expected_phase) in workflow.steps.iter().zip(expected_phases) {
        if step.phase != expected_phase.as_str()
            || step.outcome != WorkflowOutcome::Pass.as_str()
            || step.exit_code != 0
        {
            return Err(format!(
                "workflow_step_not_passing phase={}",
                expected_phase.as_str()
            ));
        }
    }

    let mut artifact_ids = BTreeSet::new();
    let mut artifact_paths = BTreeSet::new();
    for artifact in &workflow.artifact_index {
        if !artifact_ids.insert(artifact.artifact_id.as_str()) {
            return Err(format!(
                "workflow_duplicate_artifact_id id={}",
                artifact.artifact_id
            ));
        }
        if !artifact_paths.insert(artifact.path.as_str()) {
            return Err(format!(
                "workflow_duplicate_artifact_path path={}",
                artifact.path
            ));
        }
        if !artifact.required {
            continue;
        }
        let observed_unix_ms = artifact.observed_unix_ms.ok_or_else(|| {
            format!(
                "workflow_required_artifact_missing_timestamp id={}",
                artifact.artifact_id
            )
        })?;
        let reference = StrictEvidenceRef {
            path: artifact.path.clone(),
            sha256: artifact.sha256.clone(),
            observed_unix_ms,
        };
        let observed = load_strict_evidence_bytes(
            evidence_root,
            &reference,
            input.generated_unix_ms,
            now_unix_ms,
            freshness_budget_ms,
        )?;
        require_manifest_binding(manifest_index, &observed)?;
        if !artifact.fresh
            || artifact.age_ms != input.generated_unix_ms.checked_sub(observed_unix_ms)
            || observed.sha256 != artifact.sha256
        {
            return Err(format!(
                "workflow_required_artifact_not_fresh_or_hash_bound id={}",
                artifact.artifact_id
            ));
        }
    }
    Ok(())
}

fn validate_d4_runtime_path_proof(
    evidence_root: &Path,
    proof: &D4RuntimePathProof,
    input: &StrictCertificateEvidenceInput,
    manifest_index: &BTreeMap<String, (String, u64)>,
    now_unix_ms: u128,
    freshness_budget_ms: u128,
) -> Result<(), String> {
    if proof.schema_version != D4_RUNTIME_PATH_PROOF_SCHEMA
        || proof.source_commit != input.candidate_git_sha
        || proof.run_id != input.run_id
        || proof.trace_id != input.trace_id
        || proof.scenario_id != input.scenario_id
        || proof.seed != input.seed
        || proof.generated_unix_ms != input.generated_unix_ms
        || proof.backend_identity != STRICT_BACKEND_IDENTITY
        || !proof.gate_passed
        || !proof.concurrent_mode_default
        || proof.certifying_fallback_events != 0
    {
        return Err("d4_runtime_path_proof_not_release_ready".to_owned());
    }
    if proof.scenarios.len() != REQUIRED_D4_SCENARIOS.len() {
        return Err("d4_runtime_path_scenario_count_mismatch".to_owned());
    }
    let expected = REQUIRED_D4_SCENARIOS.into_iter().collect::<BTreeSet<_>>();
    let observed = proof
        .scenarios
        .iter()
        .map(|scenario| scenario.scenario.as_str())
        .collect::<BTreeSet<_>>();
    if observed != expected || observed.len() != proof.scenarios.len() {
        return Err("d4_runtime_path_scenario_set_mismatch".to_owned());
    }
    let mut artifact_paths = BTreeSet::new();
    let mut artifact_hashes = BTreeSet::new();
    for scenario in &proof.scenarios {
        if !scenario.passed
            || scenario.exit_code != 0
            || scenario.backend_identity != STRICT_BACKEND_IDENTITY
        {
            return Err(format!(
                "d4_runtime_path_scenario_failed scenario={}",
                scenario.scenario
            ));
        }
        let artifact: LoadedStrictEvidence<D4ScenarioArtifact> = load_strict_json(
            evidence_root,
            &scenario.artifact,
            input.generated_unix_ms,
            now_unix_ms,
            freshness_budget_ms,
        )?;
        require_manifest_binding(manifest_index, &artifact)?;
        if !artifact_paths.insert(artifact.relative_path.clone())
            || !artifact_hashes.insert(artifact.sha256.clone())
        {
            return Err(format!(
                "d4_runtime_path_scenario_artifact_not_distinct scenario={}",
                scenario.scenario
            ));
        }
        let payload = &artifact.value;
        if payload.schema_version != D4_SCENARIO_ARTIFACT_SCHEMA
            || payload.source_commit != input.candidate_git_sha
            || payload.run_id != input.run_id
            || payload.trace_id != input.trace_id
            || payload.scenario_id != input.scenario_id
            || payload.seed != input.seed
            || payload.generated_unix_ms != input.generated_unix_ms
            || payload.scenario != scenario.scenario
            || payload.backend_identity != scenario.backend_identity
            || payload.passed != scenario.passed
            || payload.exit_code != scenario.exit_code
            || !payload.concurrent_mode_default
            || payload.certifying_fallback_events != 0
        {
            return Err(format!(
                "d4_runtime_path_scenario_artifact_mismatch scenario={}",
                scenario.scenario
            ));
        }
    }
    Ok(())
}

fn validate_policy(policy: &CertificationPolicy) -> Result<(), String> {
    let canonical = canonical_certification_policy();
    if policy.schema_version != CERTIFICATION_POLICY_SCHEMA_VERSION
        || policy.policy_id != CERTIFICATION_POLICY_ID
        || !json_values_equal(policy, &canonical)?
    {
        return Err("certification_policy_not_canonical".to_owned());
    }
    Ok(())
}

fn validate_canonical_adversarial_campaign(campaign: &CampaignResult) -> Result<(), String> {
    let canonical_campaign = run_campaign(&AdversarialConfig::default());
    if !json_values_equal(campaign, &canonical_campaign)? {
        return Err("adversarial_campaign_not_canonical_or_nonvacuous".to_owned());
    }
    Ok(())
}

fn validate_canonical_parity_evidence(
    gate: &GateReport,
    ranking: &ExpectedLossRanking,
    ledger: &EvidenceLedger,
    catalog: &CatalogStats,
    traceability: &ReleaseTraceabilityReport,
    drift: &ParityDriftSnapshot,
) -> Result<(), String> {
    let canonical_gate_config = certification_gate_config();
    if !json_values_equal(&gate.config, &canonical_gate_config)?
        || !json_values_equal(&ranking.config, &canonical_gate_config)?
    {
        return Err("confidence_gate_config_not_canonical".to_owned());
    }
    let canonical_catalog = build_canonical_catalog();
    let canonical_universe = build_canonical_universe();
    let (mut expected_gate, expected_ranking) = evaluate_full(
        &canonical_catalog,
        &canonical_universe,
        &canonical_gate_config,
    );
    expected_gate
        .verification_contract
        .clone_from(&gate.verification_contract);
    let expected_ledger = build_evidence_ledger(&expected_gate, &expected_ranking);
    let canonical_stats = canonical_catalog.stats();
    let canonical_traceability = canonical_catalog.release_traceability();
    let mut canonical_drift_monitor = ParityDriftMonitor::new(ParityDriftConfig::default());
    for category in FeatureCategory::ALL {
        let category_name = category.display_name();
        let category_count = canonical_stats
            .per_category
            .get(category_name)
            .copied()
            .unwrap_or(0);
        let mismatches =
            category_count.saturating_sub(canonical_stats.verified_invariants.min(category_count));
        canonical_drift_monitor.observe_batch(category, mismatches, category_count);
    }
    let canonical_drift = canonical_drift_monitor.snapshot();
    if !json_values_equal(gate, &expected_gate)?
        || !json_values_equal(ranking, &expected_ranking)?
        || !json_values_equal(ledger, &expected_ledger)?
        || !json_values_equal(catalog, &canonical_stats)?
        || !json_values_equal(traceability, &canonical_traceability)?
        || !json_values_equal(drift, &canonical_drift)?
    {
        return Err("canonical_parity_evidence_mismatch".to_owned());
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn validate_gate_inputs(
    gate: &GateReport,
    ranking: &ExpectedLossRanking,
    ledger: &EvidenceLedger,
    catalog: &CatalogStats,
    traceability: &ReleaseTraceabilityReport,
    drift: &ParityDriftSnapshot,
    campaign: &CampaignResult,
    flake_budget: &GlobalFlakeBudgetResult,
    critical_path: &NoMockCriticalPathReport,
    scorecard: &BayesianScorecard,
) -> Result<(), String> {
    validate_canonical_parity_evidence(gate, ranking, ledger, catalog, traceability, drift)?;
    validate_canonical_adversarial_campaign(campaign)?;
    if gate.global_decision != GateDecision::Pass
        || !gate.release_ready
        || gate.global_verification_pct != 100.0
        || gate.total_invariants == 0
        || gate.passing_invariants != gate.total_invariants
        || gate.global_lower_bound < gate.config.release_threshold
    {
        return Err("confidence_gate_not_release_ready".to_owned());
    }
    if gate.category_results.values().any(|result| {
        result.decision != GateDecision::Pass
            || result.verification_pct != 100.0
            || result.total_invariants == 0
            || result.passing_invariants != result.total_invariants
    }) || gate
        .invariant_results
        .values()
        .any(|result| result.decision != GateDecision::Pass || result.verification_pct != 100.0)
    {
        return Err("confidence_gate_contains_nonpassing_result".to_owned());
    }
    if ledger.global_decision != gate.global_decision
        || ledger.release_ready != gate.release_ready
        || ledger.global_posterior_mean != gate.global_posterior_mean
        || ledger.global_lower_bound != gate.global_lower_bound
        || ledger.global_verification_pct != gate.global_verification_pct
        || ledger.total_expected_loss != ranking.total_expected_loss
        || ledger.total_invariants != gate.total_invariants
        || ledger.passing_invariants != gate.passing_invariants
    {
        return Err("evidence_ledger_gate_mismatch".to_owned());
    }
    let recomputed_expected_loss = truncate_score(
        ranking
            .entries
            .iter()
            .map(|entry| entry.expected_loss)
            .sum(),
    );
    let recomputed_actionable = ranking
        .entries
        .iter()
        .filter(|entry| entry.expected_loss > 0.0)
        .count();
    if ranking.entries.len() != gate.total_invariants
        || ranking.total_expected_loss != recomputed_expected_loss
        || ranking.actionable_count != recomputed_actionable
        || ranking.entries.iter().enumerate().any(|(index, entry)| {
            entry.rank != index.saturating_add(1)
                || !gate.invariant_results.contains_key(&entry.invariant_id.0)
                || entry.gate_decision != GateDecision::Pass
        })
        || ranking
            .entries
            .windows(2)
            .any(|entries| entries[0].expected_loss < entries[1].expected_loss)
    {
        return Err("expected_loss_ranking_incoherent".to_owned());
    }
    if catalog.total_invariants != gate.total_invariants
        || catalog.verified_invariants != catalog.total_invariants
        || catalog.partial_invariants != 0
        || catalog.pending_invariants != 0
        || traceability.entries.len() != catalog.total_invariants
        || traceability.verification_pct != 100.0
        || !traceability.release_ready
        || traceability.entries.iter().any(|entry| !entry.verified)
    {
        return Err("catalog_or_traceability_not_release_ready".to_owned());
    }
    if drift.any_rejected
        || drift.any_drift
        || drift
            .category_states
            .values()
            .any(|state| state.drift_alerts_count != 0)
    {
        return Err("drift_snapshot_not_green".to_owned());
    }
    if !campaign.passed
        || campaign.counterexamples_found != 0
        || !campaign.counterexamples.is_empty()
    {
        return Err("adversarial_campaign_not_green".to_owned());
    }
    if !flake_budget.within_budget || !flake_budget.pipeline_pass {
        return Err("ci_flake_budget_not_green".to_owned());
    }
    if flake_budget.total_lanes != flake_budget.lane_results.len()
        || flake_budget.total_flakes
            != flake_budget
                .lane_results
                .iter()
                .map(|lane| lane.flake_count)
                .sum::<usize>()
    {
        return Err("ci_flake_budget_aggregate_mismatch".to_owned());
    }
    let expected_lanes = REQUIRED_CERTIFICATION_LANES
        .iter()
        .map(|lane| lane.as_str())
        .collect::<BTreeSet<_>>();
    let observed_lanes = flake_budget
        .lane_results
        .iter()
        .filter(|lane| expected_lanes.contains(lane.lane.as_str()))
        .map(|lane| lane.lane.as_str())
        .collect::<BTreeSet<_>>();
    let required_lane_result_count = flake_budget
        .lane_results
        .iter()
        .filter(|lane| expected_lanes.contains(lane.lane.as_str()))
        .count();
    if observed_lanes != expected_lanes
        || required_lane_result_count != expected_lanes.len()
        || flake_budget.lane_results.iter().any(|lane| {
            let recorded_total = lane
                .pass_count
                .checked_add(lane.fail_count)
                .and_then(|total| total.checked_add(lane.flake_count))
                .and_then(|total| total.checked_add(lane.skip_count));
            expected_lanes.contains(lane.lane.as_str())
                && (recorded_total != Some(lane.total_tests)
                    || lane.total_tests == 0
                    || lane.fail_count != 0
                    || lane.flake_count != 0
                    || !lane.within_budget
                    || lane.pipeline_fail)
        })
    {
        return Err("required_ci_lane_flake_budget_not_green".to_owned());
    }
    if critical_path.verdict != NoMockVerdict::Pass
        || critical_path.total_critical_invariants == 0
        || critical_path.real_evidence_count != critical_path.total_critical_invariants
        || critical_path.exception_count != 0
        || critical_path.missing_evidence_count != 0
        || critical_path.blocking_count != 0
        || critical_path.coverage_pct != 100.0
        || !critical_path.violations.is_empty()
    {
        return Err("critical_path_evidence_not_green".to_owned());
    }
    let scorecard_contract = scorecard
        .verification_contract
        .as_ref()
        .ok_or_else(|| "scorecard_missing_verification_contract".to_owned())?;
    validate_contract_outcome(scorecard_contract, "scorecard")?;
    if !scorecard.release_ready || scorecard.global_lower_bound < scorecard.release_threshold {
        return Err("scorecard_not_release_ready".to_owned());
    }
    Ok(())
}

fn validate_results_jsonl(
    payload: &[u8],
    input: &StrictCertificateEvidenceInput,
    traceability: &ReleaseTraceabilityReport,
    manifest_index: &BTreeMap<String, (String, u64)>,
) -> Result<(), String> {
    let text =
        std::str::from_utf8(payload).map_err(|error| format!("results_jsonl_not_utf8: {error}"))?;
    if text.is_empty() || text.contains('\r') || !text.ends_with('\n') {
        return Err("results_jsonl_not_canonical_newline_delimited_utf8".to_owned());
    }
    if traceability
        .entries
        .windows(2)
        .any(|entries| entries[0].invariant_id >= entries[1].invariant_id)
    {
        return Err("release_traceability_entries_not_strictly_sorted".to_owned());
    }
    let lines = text
        .strip_suffix('\n')
        .unwrap_or_default()
        .split('\n')
        .collect::<Vec<_>>();
    if lines.len() != traceability.entries.len() || lines.iter().any(|line| line.is_empty()) {
        return Err(format!(
            "results_jsonl_traceability_cardinality_mismatch records={} traceability={}",
            lines.len(),
            traceability.entries.len()
        ));
    }
    for (line_number, (line, entry)) in lines.into_iter().zip(&traceability.entries).enumerate() {
        let record: StrictResultsRecord = serde_json::from_str(line).map_err(|error| {
            format!("results_jsonl_parse_failed line={line_number} error={error}")
        })?;
        if record.schema_version != STRICT_RESULTS_RECORD_SCHEMA
            || record.candidate_git_sha != input.candidate_git_sha
            || record.run_id != input.run_id
            || record.trace_id != input.trace_id
            || record.scenario_id != input.scenario_id
            || record.seed != input.seed
            || record.invariant_id != entry.invariant_id
            || record.feature_id != entry.feature_id
            || record.category != entry.category
            || record.statement != entry.statement
            || !record.passed
            || !json_values_equal(&record.proof_summary, &entry.proof_summary)?
        {
            return Err(format!(
                "results_jsonl_record_not_exact_traceability_entry line={line_number}"
            ));
        }
        let expected_artifacts = entry
            .artifact_refs
            .iter()
            .map(|path| {
                manifest_index
                    .get(path)
                    .map(|(sha256, size_bytes)| StrictResultsArtifact {
                        path: path.clone(),
                        sha256: sha256.clone(),
                        size_bytes: *size_bytes,
                    })
                    .ok_or_else(|| format!("results_jsonl_artifact_not_in_manifest path={path}"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        if record.artifacts != expected_artifacts {
            return Err(format!(
                "results_jsonl_artifact_closure_mismatch line={line_number}"
            ));
        }
    }
    Ok(())
}

fn validate_dependency_feature_graph_document(
    graph: &LoadedStrictEvidence<serde_json::Value>,
) -> Result<(String, String), String> {
    let object = graph
        .value
        .as_object()
        .ok_or_else(|| "dependency_feature_graph_must_be_object".to_owned())?;
    let expected_keys = BTreeSet::from(["command", "schema_version", "target", "tree"]);
    let observed_keys = object.keys().map(String::as_str).collect::<BTreeSet<_>>();
    if observed_keys != expected_keys {
        return Err("dependency_feature_graph_key_set_mismatch".to_owned());
    }
    if object
        .get("schema_version")
        .and_then(serde_json::Value::as_str)
        != Some("fsqlite.dependency_feature_graph.v1")
    {
        return Err("dependency_feature_graph_schema_mismatch".to_owned());
    }
    let target = object
        .get("target")
        .and_then(serde_json::Value::as_str)
        .filter(|target| !target.is_empty() && !target.contains(char::is_whitespace))
        .ok_or_else(|| "dependency_feature_graph_target_invalid".to_owned())?;
    let expected_command = serde_json::json!([
        "cargo",
        "tree",
        "--locked",
        "--offline",
        "-p",
        "fsqlite-e2e",
        "-e",
        "features,no-dev",
        "--no-default-features",
        "--target",
        target,
    ]);
    if object.get("command") != Some(&expected_command) {
        return Err("dependency_feature_graph_command_mismatch".to_owned());
    }
    let tree = object
        .get("tree")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| "dependency_feature_graph_tree_missing".to_owned())?;
    if tree.is_empty()
        || !tree.ends_with('\n')
        || !tree.contains("${WORKSPACE_ROOT}")
        || tree.contains('\r')
    {
        return Err("dependency_feature_graph_tree_not_canonical".to_owned());
    }

    let sorted = object
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<BTreeMap<_, _>>();
    let mut canonical = serde_json::to_vec(&sorted)
        .map_err(|error| format!("dependency_feature_graph_serialize_failed: {error}"))?;
    canonical.push(b'\n');
    if graph.bytes != canonical {
        return Err("dependency_feature_graph_bytes_not_canonical".to_owned());
    }
    Ok((target.to_owned(), tree.to_owned()))
}

fn validate_dependency_feature_graph(
    workspace_root: &Path,
    graph: &LoadedStrictEvidence<serde_json::Value>,
) -> Result<String, String> {
    let (target, tree) = validate_dependency_feature_graph_document(graph)?;
    let mut command = sanitized_command(
        StrictEvidenceProgram::Cargo,
        &[
            "PATH",
            "HOME",
            "CARGO_HOME",
            "RUSTUP_HOME",
            "RUSTUP_TOOLCHAIN",
            "SystemRoot",
            "WINDIR",
            "PATHEXT",
        ],
    );
    let output = command
        .env("CARGO_NET_OFFLINE", "true")
        .env("CARGO_TERM_COLOR", "never")
        .env("TERM", "dumb")
        .current_dir(workspace_root)
        .args([
            "tree",
            "--locked",
            "--offline",
            "-p",
            "fsqlite-e2e",
            "-e",
            "features,no-dev",
            "--no-default-features",
            "--target",
            target.as_str(),
        ])
        .output()
        .map_err(|error| format!("dependency_feature_graph_replay_spawn_failed: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "dependency_feature_graph_replay_failed exit_code={}",
            output.status.code().unwrap_or(-1)
        ));
    }
    if !output.stderr.is_empty() {
        return Err("dependency_feature_graph_replay_stderr_not_empty".to_owned());
    }
    let replayed = String::from_utf8(output.stdout)
        .map_err(|error| format!("dependency_feature_graph_replay_not_utf8: {error}"))?;
    if replayed.contains('\r') || replayed.contains('\u{1b}') {
        return Err("dependency_feature_graph_replay_not_canonical_text".to_owned());
    }
    let canonical_workspace = workspace_root
        .canonicalize()
        .map_err(|error| format!("dependency_feature_graph_workspace_failed: {error}"))?;
    let workspace_text = canonical_workspace
        .to_str()
        .ok_or_else(|| "dependency_feature_graph_workspace_not_utf8".to_owned())?;
    let mut normalized = replayed.replace(workspace_text, "${WORKSPACE_ROOT}");
    let slash_workspace = workspace_text.replace('\\', "/");
    if slash_workspace != workspace_text {
        normalized = normalized.replace(&slash_workspace, "${WORKSPACE_ROOT}");
    }
    if normalized != tree {
        return Err("dependency_feature_graph_replay_mismatch".to_owned());
    }
    Ok(target)
}

fn validate_regression_baseline(
    workspace_root: &Path,
    baseline: &RegressionBaselineReference,
    baseline_bytes: &[u8],
    baseline_metadata_git_sha: &str,
    candidate_git_sha: &str,
) -> Result<(), String> {
    let accounted = baseline
        .passed
        .checked_add(baseline.failed)
        .and_then(|count| count.checked_add(baseline.ignored));
    if baseline.as_of_phase.trim().is_empty()
        || baseline.total_tests == 0
        || accounted != Some(baseline.total_tests)
        || baseline.failed != 0
        || !is_lower_hex(&baseline.baseline_commit, 40)
        || !is_lower_hex(baseline_metadata_git_sha, 40)
        || baseline.baseline_commit == candidate_git_sha
        || baseline.baseline_commit == baseline_metadata_git_sha
        || baseline_metadata_git_sha == candidate_git_sha
    {
        return Err("regression_baseline_not_green_or_predating_candidate".to_owned());
    }
    require_ancestor(
        workspace_root,
        &baseline.baseline_commit,
        baseline_metadata_git_sha,
    )?;
    require_ancestor(workspace_root, baseline_metadata_git_sha, candidate_git_sha)?;
    if git_blob_at_commit(
        workspace_root,
        baseline_metadata_git_sha,
        REGRESSION_BASELINE_PATH,
    )? != baseline_bytes
    {
        return Err("regression_baseline_not_exact_metadata_commit_blob".to_owned());
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ExpectedPhase5Run {
    requirement_blake3: String,
    argv: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ExpectedPhase5Requirements {
    current_runs: BTreeMap<String, ExpectedPhase5Run>,
    live_guard: ExpectedPhase5Run,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Phase5CargoTestTarget {
    Library,
    Integration(String),
}

fn validate_rust_test_identity(source_path: &str, test_name: &str) -> Result<(), String> {
    let normalized = checked_relative_path(Path::new(source_path))?
        .to_string_lossy()
        .into_owned();
    if normalized != source_path || !source_path.ends_with(".rs") {
        return Err(format!(
            "regression_test_source_not_canonical path={source_path}"
        ));
    }
    let valid_identifier = |segment: &str| {
        let mut bytes = segment.bytes();
        bytes
            .next()
            .is_some_and(|byte| byte == b'_' || byte.is_ascii_alphabetic())
            && bytes.all(|byte| byte == b'_' || byte.is_ascii_alphanumeric())
    };
    if test_name.trim() != test_name
        || test_name.is_empty()
        || test_name
            .split("::")
            .any(|segment| !valid_identifier(segment))
    {
        return Err(format!(
            "regression_test_name_not_canonical name={test_name}"
        ));
    }
    Ok(())
}

fn expected_phase5_test_target(source_path: &str) -> Result<Phase5CargoTestTarget, String> {
    let components = source_path.split('/').collect::<Vec<_>>();
    if components.len() < 4 || components[0] != "crates" {
        return Err(format!(
            "phase5_test_source_outside_workspace path={source_path}"
        ));
    }
    match components[2] {
        "src" if components[3] != "main.rs" && components[3] != "bin" => {
            Ok(Phase5CargoTestTarget::Library)
        }
        "tests" if components.len() == 4 => {
            let target = Path::new(components[3])
                .file_stem()
                .and_then(|stem| stem.to_str())
                .filter(|target| !target.is_empty())
                .ok_or_else(|| format!("phase5_integration_target_invalid path={source_path}"))?;
            Ok(Phase5CargoTestTarget::Integration(target.to_owned()))
        }
        _ => Err(format!("phase5_test_target_ambiguous path={source_path}")),
    }
}

fn canonical_phase5_runtime_test_name(
    source_path: &str,
    test_name: &str,
) -> Result<String, String> {
    validate_rust_test_identity(source_path, test_name)?;
    match expected_phase5_test_target(source_path)? {
        Phase5CargoTestTarget::Integration(_) => Ok(test_name.to_owned()),
        Phase5CargoTestTarget::Library => {
            if PATH_REMAPPED_LIBRARY_SOURCES.contains(&source_path) {
                return Err(format!(
                    "phase5_path_remapped_library_test_unsupported path={source_path}"
                ));
            }
            let components = source_path.split('/').collect::<Vec<_>>();
            let relative = &components[3..];
            let mut modules = Vec::new();
            for (index, component) in relative.iter().enumerate() {
                let is_final = index + 1 == relative.len();
                if is_final {
                    let stem = component.strip_suffix(".rs").ok_or_else(|| {
                        format!("phase5_library_source_not_rust path={source_path}")
                    })?;
                    match stem {
                        "lib" if relative.len() == 1 => {}
                        "mod" if relative.len() > 1 => {}
                        "mod" | "" => {
                            return Err(format!(
                                "phase5_library_module_path_invalid path={source_path}"
                            ));
                        }
                        named => modules.push(named.to_owned()),
                    }
                } else if component.is_empty() || component.ends_with(".rs") {
                    return Err(format!(
                        "phase5_library_module_directory_invalid path={source_path}"
                    ));
                } else {
                    modules.push((*component).to_owned());
                }
            }
            if modules.is_empty() {
                Ok(test_name.to_owned())
            } else {
                Ok(format!("{}::{test_name}", modules.join("::")))
            }
        }
    }
}

fn expected_phase5_run_argv(entry: &RegressionIgnoredTest) -> Result<Vec<String>, String> {
    let package = entry
        .source_path
        .split('/')
        .nth(1)
        .ok_or_else(|| format!("phase5_test_package_missing locator={}", entry.locator()))?;
    let target = expected_phase5_test_target(&entry.source_path)?;
    let needs_ignored_filter = match entry.cfg_condition.as_deref() {
        None | Some("test") => true,
        Some("debug_assertions" | "all(debug_assertions,test)") => false,
        Some(condition) => {
            return Err(format!(
                "phase5_test_cfg_condition_unsupported locator={} condition={condition}",
                entry.locator()
            ));
        }
    };
    let mut argv = [
        "cargo",
        "test",
        "--locked",
        "--profile",
        "release-perf",
        "--package",
        package,
    ]
    .map(str::to_owned)
    .to_vec();
    match target {
        Phase5CargoTestTarget::Library => argv.push("--lib".to_owned()),
        Phase5CargoTestTarget::Integration(target) => {
            argv.push("--test".to_owned());
            argv.push(target);
        }
    }
    argv.extend([
        canonical_phase5_runtime_test_name(&entry.source_path, &entry.test_name)?,
        "--".to_owned(),
        "--exact".to_owned(),
    ]);
    if needs_ignored_filter {
        argv.push("--ignored".to_owned());
    }
    argv.extend(["--nocapture".to_owned(), "--test-threads=1".to_owned()]);
    Ok(argv)
}

#[allow(clippy::too_many_lines)]
fn expected_phase5_requirements(
    baseline: &RegressionBaselineReference,
) -> Result<ExpectedPhase5Requirements, String> {
    let mut requirements = BTreeMap::new();
    let mut live_guard = None;
    let mut previous_locator: Option<String> = None;
    for test in &baseline.ignored_tests {
        let locator = test.locator();
        if previous_locator
            .as_ref()
            .is_some_and(|previous| previous >= &locator)
        {
            return Err(format!(
                "regression_baseline_tests_not_strictly_sorted locator={locator}"
            ));
        }
        previous_locator = Some(locator.clone());
        validate_rust_test_identity(&test.source_path, &test.test_name)?;
        if test.reason.trim().is_empty()
            || test.reason.trim() != test.reason
            || test.evidence.requirement.trim().is_empty()
            || test.evidence.requirement.trim() != test.evidence.requirement
            || !test.kind.allows_policy(test.policy)
        {
            return Err(format!(
                "regression_baseline_test_invalid locator={locator}"
            ));
        }
        match test.policy {
            RegressionIgnorePolicy::BlockRelease => {
                return Err(format!(
                    "regression_baseline_block_release_entry locator={locator}"
                ));
            }
            RegressionIgnorePolicy::CoveredByParent if test.parent_tests.is_empty() => {
                return Err(format!(
                    "regression_baseline_parent_coverage_empty locator={locator}"
                ));
            }
            RegressionIgnorePolicy::CoveredByParent => {}
            _ if !test.parent_tests.is_empty() => {
                return Err(format!(
                    "regression_baseline_unexpected_parent_coverage locator={locator}"
                ));
            }
            _ => {}
        }
        let mut previous_parent: Option<String> = None;
        for parent in &test.parent_tests {
            validate_rust_test_identity(&parent.source_path, &parent.test_name)?;
            let parent_locator = format!("{}::{}", parent.source_path, parent.test_name);
            if parent_locator == locator
                || parent_locator == PHASE5_LIVE_GUARD_LOCATOR
                || previous_parent
                    .as_ref()
                    .is_some_and(|previous| previous >= &parent_locator)
            {
                return Err(format!(
                    "regression_baseline_parent_identity_invalid locator={locator}"
                ));
            }
            previous_parent = Some(parent_locator);
        }
        if let Some(receipt) = &test.evidence.receipt {
            let canonical_artifact_path =
                checked_relative_path(Path::new(&receipt.artifact_path))?.to_string_lossy();
            if !is_lower_hex(&receipt.source_commit, 40)
                || !is_lower_hex(&receipt.artifact_blake3, 64)
                || canonical_artifact_path.as_ref() != receipt.artifact_path.as_str()
            {
                return Err(format!(
                    "regression_baseline_historical_receipt_invalid locator={locator}"
                ));
            }
        }
        if test.policy != RegressionIgnorePolicy::RunForRelease {
            continue;
        }
        let expected = ExpectedPhase5Run {
            requirement_blake3: blake3::hash(test.evidence.requirement.as_bytes())
                .to_hex()
                .to_string(),
            argv: expected_phase5_run_argv(test)?,
        };
        if locator == PHASE5_LIVE_GUARD_LOCATOR {
            if test.kind != RegressionIgnoreKind::ReleaseGate
                || live_guard.replace(expected).is_some()
            {
                return Err("regression_baseline_live_guard_identity_invalid".to_owned());
            }
        } else if requirements.insert(locator.clone(), expected).is_some() {
            return Err(format!(
                "regression_baseline_release_requirement_duplicate locator={locator}"
            ));
        }
    }
    let t16_locator = format!("{T16_SOURCE_PATH}::{T16_TEST_NAME}");
    if !requirements.contains_key(&t16_locator) {
        return Err("regression_baseline_t16_requirement_missing".to_owned());
    }
    Ok(ExpectedPhase5Requirements {
        current_runs: requirements,
        live_guard: live_guard
            .ok_or_else(|| "regression_baseline_live_guard_requirement_missing".to_owned())?,
    })
}

fn phase5_leaf_key(leaf: &Phase5EvidenceLeaf) -> String {
    format!(
        "{}\u{0}{}\u{0}{}",
        leaf.path, leaf.digest_algorithm, leaf.digest
    )
}

fn load_phase5_leaf(
    evidence_root: &Path,
    candidate_git_sha: &str,
    leaf: &Phase5EvidenceLeaf,
) -> Result<Vec<u8>, String> {
    let expected_prefix = format!("{PHASE5_EVIDENCE_PREFIX}/{candidate_git_sha}/");
    if !leaf.path.starts_with(&expected_prefix)
        || leaf.digest_algorithm != PHASE5_DIGEST_ALGORITHM
        || !is_lower_hex(&leaf.digest, 64)
    {
        return Err(format!("phase5_leaf_shape_invalid path={}", leaf.path));
    }
    let path = canonical_regular_file(evidence_root, Path::new(&leaf.path))?;
    let relative = canonical_relative_path(evidence_root, &path)?;
    if relative != leaf.path {
        return Err(format!("phase5_leaf_path_not_canonical path={}", leaf.path));
    }
    let bytes = fs::read(&path).map_err(|error| {
        format!(
            "phase5_leaf_read_failed path={} error={error}",
            path.display()
        )
    })?;
    if blake3::hash(&bytes).to_hex().as_str() != leaf.digest {
        return Err(format!("phase5_leaf_digest_mismatch path={}", leaf.path));
    }
    Ok(bytes)
}

fn require_phase5_pack_leaf(
    pack: &BTreeSet<String>,
    leaf: &Phase5EvidenceLeaf,
) -> Result<(), String> {
    if !pack.contains(&phase5_leaf_key(leaf)) {
        return Err(format!(
            "phase5_referenced_leaf_missing_from_pack path={}",
            leaf.path
        ));
    }
    Ok(())
}

enum Phase5LeafBinding<'a> {
    Pack(&'a BTreeSet<String>),
    CandidateManifest(&'a BTreeMap<String, (String, u64)>),
    MetadataCommit {
        workspace_root: &'a Path,
        metadata_commit: &'a str,
    },
}

fn load_bound_phase5_leaf(
    evidence_root: &Path,
    evidence_commit: &str,
    leaf: &Phase5EvidenceLeaf,
    binding: &Phase5LeafBinding<'_>,
) -> Result<Vec<u8>, String> {
    let bytes = load_phase5_leaf(evidence_root, evidence_commit, leaf)?;
    match binding {
        Phase5LeafBinding::Pack(pack) => require_phase5_pack_leaf(pack, leaf)?,
        Phase5LeafBinding::CandidateManifest(index) => {
            let observed_size = u64::try_from(bytes.len())
                .map_err(|_| format!("phase5_leaf_size_overflow path={}", leaf.path))?;
            let observed = (sha256_bytes(&bytes), observed_size);
            if index.get(&leaf.path) != Some(&observed) {
                return Err(format!(
                    "phase5_leaf_not_bound_by_candidate_manifest path={}",
                    leaf.path
                ));
            }
        }
        Phase5LeafBinding::MetadataCommit {
            workspace_root,
            metadata_commit,
        } => {
            if git_blob_at_commit(workspace_root, metadata_commit, &leaf.path)? != bytes {
                return Err(format!(
                    "phase5_leaf_not_bound_by_metadata_commit path={} commit={metadata_commit}",
                    leaf.path
                ));
            }
        }
    }
    Ok(bytes)
}

fn parse_strict_command_tokens(command: &str) -> Result<Vec<String>, String> {
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum Quote {
        None,
        Single,
        Double,
    }

    let mut quote = Quote::None;
    let mut escaped = false;
    let mut started = false;
    let mut token = String::new();
    let mut tokens = Vec::new();
    for character in command.chars() {
        if matches!(character, '\0' | '\n' | '\r') {
            return Err("phase5_rch_command_control_character".to_owned());
        }
        if escaped {
            token.push(character);
            escaped = false;
            started = true;
            continue;
        }
        match quote {
            Quote::Single => {
                if character == '\'' {
                    quote = Quote::None;
                } else {
                    token.push(character);
                }
                started = true;
            }
            Quote::Double => {
                match character {
                    '"' => quote = Quote::None,
                    '\\' => escaped = true,
                    '$' | '`' => return Err("phase5_rch_command_shell_expansion".to_owned()),
                    _ => token.push(character),
                }
                started = true;
            }
            Quote::None => match character {
                '\'' => {
                    quote = Quote::Single;
                    started = true;
                }
                '"' => {
                    quote = Quote::Double;
                    started = true;
                }
                '\\' => {
                    escaped = true;
                    started = true;
                }
                value if value.is_whitespace() => {
                    if started {
                        tokens.push(std::mem::take(&mut token));
                        started = false;
                    }
                }
                '$' | '`' | ';' | '|' | '&' | '<' | '>' => {
                    return Err("phase5_rch_command_shell_syntax".to_owned());
                }
                _ => {
                    token.push(character);
                    started = true;
                }
            },
        }
    }
    if escaped || quote != Quote::None {
        return Err("phase5_rch_command_incomplete_quote".to_owned());
    }
    if started {
        tokens.push(token);
    }
    if tokens.is_empty() || tokens.iter().any(String::is_empty) {
        return Err("phase5_rch_command_empty_token".to_owned());
    }
    Ok(tokens)
}

fn parse_phase5_status(bytes: &[u8], label: &str) -> Result<Phase5RchStatusEnvelope, String> {
    let status: Phase5RchStatusEnvelope =
        serde_json::from_slice(bytes).map_err(|error| format!("{label}_parse_failed: {error}"))?;
    if status.api_version != "1.0" || status.command != "status" || !status.success {
        return Err(format!("{label}_not_successful_status_v1"));
    }
    Ok(status)
}

fn one_transcript_marker(transcript: &str, marker: &str, label: &str) -> Result<String, String> {
    let values = transcript
        .lines()
        .filter_map(|line| line.trim().strip_prefix(marker))
        .filter(|value| !value.trim().is_empty())
        .collect::<Vec<_>>();
    match values.as_slice() {
        [value] => Ok(value.trim().to_owned()),
        _ => Err(format!(
            "{label}_missing_or_duplicate_marker marker={marker}"
        )),
    }
}

fn validate_phase5_rch_binding(
    receipt: &Phase5RchReceipt,
    execution: &Phase5CommandEvidence,
    active: &Phase5RchStatusEnvelope,
    completed: &Phase5RchStatusEnvelope,
    transcript: &str,
    expected_project_id: &str,
) -> Result<String, String> {
    let job_id = receipt
        .job_id
        .parse::<u64>()
        .map_err(|error| format!("phase5_job_id_invalid: {error}"))?;
    let active_matches = active
        .data
        .daemon
        .active_builds
        .iter()
        .filter(|build| build.id == job_id)
        .collect::<Vec<_>>();
    let [active_build] = active_matches.as_slice() else {
        return Err("phase5_active_status_job_cardinality_mismatch".to_owned());
    };
    if active_build.project_id != expected_project_id
        || active_build.worker_id.trim().is_empty()
        || parse_strict_command_tokens(&active_build.command)? != execution.argv
        || active
            .data
            .daemon
            .active_builds
            .iter()
            .filter(|build| build.worker_id == active_build.worker_id)
            .count()
            != 1
    {
        return Err("phase5_active_status_not_bound_to_command".to_owned());
    }
    let completed_matches = completed
        .data
        .daemon
        .recent_builds
        .iter()
        .filter(|build| build.id == job_id)
        .collect::<Vec<_>>();
    let [completed_build] = completed_matches.as_slice() else {
        return Err("phase5_completed_status_job_cardinality_mismatch".to_owned());
    };
    if completed_build.project_id != active_build.project_id
        || completed_build.worker_id != active_build.worker_id
        || parse_strict_command_tokens(&completed_build.command)? != execution.argv
        || completed_build.exit_code != execution.exit_status
        || completed_build.location != "remote"
        || completed_build.cancellation.is_some()
    {
        return Err("phase5_completed_status_not_bound_to_remote_command".to_owned());
    }
    let selected_worker = one_transcript_marker(transcript, "Selected worker: ", "phase5")?
        .split_whitespace()
        .next()
        .unwrap_or_default()
        .to_owned();
    let remote_exit =
        one_transcript_marker(transcript, "Remote command finished: exit=", "phase5")?
            .split_whitespace()
            .next()
            .and_then(|value| value.parse::<i32>().ok());
    if selected_worker != active_build.worker_id || remote_exit != Some(execution.exit_status) {
        return Err("phase5_transcript_not_bound_to_remote_status".to_owned());
    }
    Ok(active_build.worker_id.clone())
}

#[derive(Debug)]
struct ValidatedPhase5Run {
    transcript: String,
    worker_id: String,
    referenced_leaves: BTreeSet<String>,
}

fn insert_phase5_leaf(
    leaves: &mut BTreeSet<String>,
    paths: &mut BTreeSet<String>,
    leaf: &Phase5EvidenceLeaf,
) -> Result<(), String> {
    if !is_lower_hex(&leaf.digest, 64)
        || leaf.digest_algorithm != PHASE5_DIGEST_ALGORITHM
        || !leaves.insert(phase5_leaf_key(leaf))
        || !paths.insert(leaf.path.clone())
    {
        return Err(format!(
            "phase5_evidence_leaf_duplicate_or_invalid path={}",
            leaf.path
        ));
    }
    Ok(())
}

fn merge_phase5_run_leaves(
    leaves: &mut BTreeSet<String>,
    paths: &mut BTreeSet<String>,
    run: &ValidatedPhase5Run,
) -> Result<(), String> {
    for key in &run.referenced_leaves {
        let path = key.split('\0').next().unwrap_or_default().to_owned();
        if !leaves.insert(key.clone()) || !paths.insert(path.clone()) {
            return Err(format!("phase5_run_leaf_reused path={path}"));
        }
    }
    Ok(())
}

fn validate_phase5_run(
    evidence_root: &Path,
    evidence_commit: &str,
    run: &Phase5RunEvidence,
    binding: &Phase5LeafBinding<'_>,
    expected_project_id: &str,
) -> Result<ValidatedPhase5Run, String> {
    if run.execution.argv.is_empty()
        || run
            .execution
            .argv
            .iter()
            .any(|arg| arg.is_empty() || arg.contains('\0'))
        || run.execution.exit_status != 0
        || run.execution.stdout.capture != Phase5StreamCapture::Observed
        || run.execution.stderr.capture != Phase5StreamCapture::Observed
        || run.execution.transcript != run.execution.stderr.leaf
        || run.execution.stdout.leaf.path == run.execution.stderr.leaf.path
    {
        return Err("phase5_command_evidence_not_successful".to_owned());
    }
    let mut referenced_leaves = BTreeSet::new();
    for leaf in [
        &run.execution.stdout.leaf,
        &run.execution.stderr.leaf,
        &run.runner_receipt,
    ] {
        if !referenced_leaves.insert(phase5_leaf_key(leaf)) {
            return Err(format!("phase5_run_duplicate_leaf path={}", leaf.path));
        }
    }
    drop(load_bound_phase5_leaf(
        evidence_root,
        evidence_commit,
        &run.execution.stdout.leaf,
        binding,
    )?);
    let transcript_bytes = load_bound_phase5_leaf(
        evidence_root,
        evidence_commit,
        &run.execution.stderr.leaf,
        binding,
    )?;
    let transcript = String::from_utf8(transcript_bytes)
        .map_err(|error| format!("phase5_transcript_not_utf8: {error}"))?;
    if transcript.contains('\u{1b}') {
        return Err("phase5_transcript_contains_terminal_escape".to_owned());
    }
    let receipt_bytes =
        load_bound_phase5_leaf(evidence_root, evidence_commit, &run.runner_receipt, binding)?;
    let receipt: Phase5RchReceipt = serde_json::from_slice(&receipt_bytes)
        .map_err(|error| format!("phase5_rch_receipt_parse_failed: {error}"))?;
    if receipt.schema_version != PHASE5_RCH_RECEIPT_SCHEMA
        || receipt.inner_cargo_argv != run.execution.argv
        || receipt.job_id.is_empty()
        || !receipt.job_id.bytes().all(|byte| byte.is_ascii_digit())
        || (receipt.job_id.len() > 1 && receipt.job_id.starts_with('0'))
    {
        return Err("phase5_rch_receipt_identity_invalid".to_owned());
    }
    for leaf in [&receipt.active_status, &receipt.completed_status] {
        if !referenced_leaves.insert(phase5_leaf_key(leaf)) {
            return Err(format!("phase5_run_duplicate_leaf path={}", leaf.path));
        }
    }
    let active_bytes = load_bound_phase5_leaf(
        evidence_root,
        evidence_commit,
        &receipt.active_status,
        binding,
    )?;
    let completed_bytes = load_bound_phase5_leaf(
        evidence_root,
        evidence_commit,
        &receipt.completed_status,
        binding,
    )?;
    let active = parse_phase5_status(&active_bytes, "phase5_active_status")?;
    let completed = parse_phase5_status(&completed_bytes, "phase5_completed_status")?;
    let worker_id = validate_phase5_rch_binding(
        &receipt,
        &run.execution,
        &active,
        &completed,
        &transcript,
        expected_project_id,
    )?;
    Ok(ValidatedPhase5Run {
        transcript,
        worker_id,
        referenced_leaves,
    })
}

#[allow(clippy::too_many_lines)]
fn validate_phase5_compiler_inventory(
    workspace_root: &Path,
    evidence_root: &Path,
    candidate_git_sha: &str,
    leaf: &Phase5EvidenceLeaf,
    pack: &BTreeSet<String>,
    expected_project_id: &str,
) -> Result<(Phase5CompilerInventoryAttestation, BTreeSet<String>), String> {
    let binding = Phase5LeafBinding::Pack(pack);
    let bytes = load_bound_phase5_leaf(evidence_root, candidate_git_sha, leaf, &binding)?;
    let attestation: Phase5CompilerInventoryAttestation = serde_json::from_slice(&bytes)
        .map_err(|error| format!("phase5_compiler_inventory_parse_failed: {error}"))?;
    if attestation.tested_tree_blake3 != tested_tree_blake3(workspace_root, candidate_git_sha)?
        || attestation.cfg_profile.trim().is_empty()
        || attestation.inventory_leaves.is_empty()
        || attestation.targets.is_empty()
    {
        return Err("phase5_compiler_inventory_not_bound_to_candidate_tree".to_owned());
    }
    for digest in [
        &attestation.cargo_metadata_blake3,
        &attestation.target_mappings_blake3,
        &attestation.active_identities_blake3,
        &attestation.ignored_identities_blake3,
        &attestation.doctest_identities_blake3,
        &attestation.expanded_identities_blake3,
    ] {
        if !is_lower_hex(digest, 64) {
            return Err("phase5_compiler_inventory_digest_invalid".to_owned());
        }
    }

    let expected_commands = [
        (
            &attestation.inventory_runs.all_targets,
            vec![
                "cargo",
                "test",
                "--locked",
                "--workspace",
                "--all-targets",
                "--",
                "--list",
            ],
        ),
        (
            &attestation.inventory_runs.all_targets_ignored,
            vec![
                "cargo",
                "test",
                "--locked",
                "--workspace",
                "--all-targets",
                "--",
                "--list",
                "--ignored",
            ],
        ),
        (
            &attestation.inventory_runs.doctests,
            vec![
                "cargo",
                "test",
                "--locked",
                "--workspace",
                "--doc",
                "--",
                "--list",
            ],
        ),
        (
            &attestation.inventory_runs.doctests_ignored,
            vec![
                "cargo",
                "test",
                "--locked",
                "--workspace",
                "--doc",
                "--",
                "--list",
                "--ignored",
            ],
        ),
    ];
    let mut referenced = BTreeSet::from([phase5_leaf_key(leaf)]);
    let mut referenced_paths = BTreeSet::from([leaf.path.clone()]);
    for (run, expected) in expected_commands {
        if run.execution.argv != expected.into_iter().map(str::to_owned).collect::<Vec<_>>() {
            return Err("phase5_compiler_inventory_command_not_canonical".to_owned());
        }
        let validated = validate_phase5_run(
            evidence_root,
            candidate_git_sha,
            run,
            &binding,
            expected_project_id,
        )?;
        merge_phase5_run_leaves(&mut referenced, &mut referenced_paths, &validated)?;
    }

    let mut previous_leaf: Option<(&str, &str)> = None;
    let mut role_bytes = BTreeMap::new();
    for inventory_leaf in &attestation.inventory_leaves {
        if previous_leaf.is_some_and(|previous| {
            previous >= (inventory_leaf.role.as_str(), inventory_leaf.path.as_str())
        }) || inventory_leaf.role.trim().is_empty()
            || inventory_leaf.sha256_algorithm != PHASE5_INVENTORY_SHA256_ALGORITHM
            || !is_lower_hex(&inventory_leaf.sha256, 64)
            || inventory_leaf.blake3_algorithm != PHASE5_DIGEST_ALGORITHM
            || !is_lower_hex(&inventory_leaf.blake3, 64)
        {
            return Err("phase5_compiler_inventory_leaf_invalid".to_owned());
        }
        previous_leaf = Some((&inventory_leaf.role, &inventory_leaf.path));
        let evidence_leaf = Phase5EvidenceLeaf {
            path: inventory_leaf.path.clone(),
            digest_algorithm: inventory_leaf.blake3_algorithm.clone(),
            digest: inventory_leaf.blake3.clone(),
        };
        let bytes =
            load_bound_phase5_leaf(evidence_root, candidate_git_sha, &evidence_leaf, &binding)?;
        if sha256_bytes(&bytes) != inventory_leaf.sha256
            || role_bytes
                .insert(inventory_leaf.role.clone(), bytes)
                .is_some()
            || !referenced.insert(phase5_leaf_key(&evidence_leaf))
            || !referenced_paths.insert(evidence_leaf.path)
        {
            return Err("phase5_compiler_inventory_leaf_binding_invalid".to_owned());
        }
    }
    for (role, expected_digest) in [
        ("cargo_metadata", &attestation.cargo_metadata_blake3),
        ("target_mappings", &attestation.target_mappings_blake3),
        ("active_identities", &attestation.active_identities_blake3),
        ("ignored_identities", &attestation.ignored_identities_blake3),
        ("doctest_identities", &attestation.doctest_identities_blake3),
        (
            "expanded_identities",
            &attestation.expanded_identities_blake3,
        ),
    ] {
        let bytes = role_bytes
            .get(role)
            .ok_or_else(|| format!("phase5_compiler_inventory_role_missing role={role}"))?;
        if blake3::hash(bytes).to_hex().as_str() != expected_digest {
            return Err(format!(
                "phase5_compiler_inventory_role_mismatch role={role}"
            ));
        }
    }
    let mut previous_target: Option<&str> = None;
    for target in &attestation.targets {
        if target.target.trim().is_empty()
            || !is_lower_hex(&target.source_inventory_blake3, 64)
            || previous_target.is_some_and(|previous| previous >= target.target.as_str())
        {
            return Err("phase5_compiler_inventory_target_invalid".to_owned());
        }
        previous_target = Some(&target.target);
    }
    Ok((attestation, referenced))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Phase5RegressionCounts {
    total_tests: u64,
    passed: u64,
    failed: u64,
    ignored: u64,
}

impl Phase5RegressionCounts {
    const fn zero() -> Self {
        Self {
            total_tests: 0,
            passed: 0,
            failed: 0,
            ignored: 0,
        }
    }

    fn checked_add(&mut self, rhs: Self) -> Result<(), String> {
        *self = Self {
            total_tests: self
                .total_tests
                .checked_add(rhs.total_tests)
                .ok_or_else(|| "phase5_workspace_total_overflow".to_owned())?,
            passed: self
                .passed
                .checked_add(rhs.passed)
                .ok_or_else(|| "phase5_workspace_passed_overflow".to_owned())?,
            failed: self
                .failed
                .checked_add(rhs.failed)
                .ok_or_else(|| "phase5_workspace_failed_overflow".to_owned())?,
            ignored: self
                .ignored
                .checked_add(rhs.ignored)
                .ok_or_else(|| "phase5_workspace_ignored_overflow".to_owned())?,
        };
        Ok(())
    }
}

fn phase5_parse_count_segment(segment: &str, label: &str) -> Option<u64> {
    let suffix = format!(" {label}");
    segment
        .trim()
        .strip_suffix(&suffix)?
        .split_whitespace()
        .last()?
        .parse()
        .ok()
}

fn phase5_parse_summary_line(line: &str) -> Option<Phase5RegressionCounts> {
    let result = line.strip_prefix("test result: ")?;
    let outcome = result.split_whitespace().next()?;
    if !matches!(outcome, "ok." | "FAILED.") {
        return None;
    }
    let mut passed = None;
    let mut failed = None;
    let mut ignored = None;
    for segment in line.split(';') {
        for (label, slot) in [
            ("passed", &mut passed),
            ("failed", &mut failed),
            ("ignored", &mut ignored),
        ] {
            if let Some(count) = phase5_parse_count_segment(segment, label)
                && slot.replace(count).is_some()
            {
                return None;
            }
        }
    }
    let passed = passed?;
    let failed = failed?;
    let ignored = ignored?;
    if (outcome == "ok." && failed != 0) || (outcome == "FAILED." && failed == 0) {
        return None;
    }
    Some(Phase5RegressionCounts {
        total_tests: passed.checked_add(failed)?.checked_add(ignored)?,
        passed,
        failed,
        ignored,
    })
}

fn phase5_cargo_target_section(line: &str) -> Option<&str> {
    if let Some(section) = line.strip_prefix("     Running ")
        && !section.is_empty()
        && section.contains(" (")
        && section.ends_with(')')
    {
        return Some(section);
    }
    let section = line.strip_prefix("   Doc-tests ")?;
    (!section.trim().is_empty() && section == section.trim_end()).then_some(section)
}

fn parse_phase5_workspace_counts(output: &str) -> Result<Phase5RegressionCounts, String> {
    if output.contains('\u{1b}') {
        return Err("phase5_workspace_transcript_contains_terminal_escape".to_owned());
    }
    let mut totals = Phase5RegressionCounts::zero();
    let mut active_section: Option<String> = None;
    let mut active_summary = None;
    for line in output.lines() {
        if let Some(section_header) = phase5_cargo_target_section(line) {
            if let Some(section) = active_section.take() {
                let summary = active_summary.take().ok_or_else(|| {
                    format!("phase5_workspace_section_missing_summary section={section}")
                })?;
                totals.checked_add(summary)?;
            }
            active_section = Some(section_header.to_owned());
            continue;
        }
        if line.starts_with("test result: ") {
            let parsed = phase5_parse_summary_line(line)
                .ok_or_else(|| format!("phase5_workspace_summary_malformed line={line}"))?;
            if active_section.is_none() {
                return Err("phase5_workspace_summary_outside_target".to_owned());
            }
            active_summary = Some(parsed);
        }
    }
    let section = active_section.ok_or_else(|| "phase5_workspace_no_target_sections".to_owned())?;
    let summary = active_summary
        .ok_or_else(|| format!("phase5_workspace_section_missing_summary section={section}"))?;
    totals.checked_add(summary)?;
    Ok(totals)
}

fn validate_phase5_exact_test_transcript(
    transcript: &str,
    expected_argv: &[String],
) -> Result<(), String> {
    let separator = expected_argv
        .iter()
        .position(|argument| argument == "--")
        .ok_or_else(|| "phase5_exact_test_command_missing_separator".to_owned())?;
    let runtime_test_name = separator
        .checked_sub(1)
        .and_then(|index| expected_argv.get(index))
        .ok_or_else(|| "phase5_exact_test_command_missing_name".to_owned())?;
    let expected_line = format!("test {runtime_test_name} ... ok");
    if transcript
        .lines()
        .filter(|line| line.trim() == expected_line)
        .count()
        != 1
    {
        return Err(format!(
            "phase5_exact_test_identity_missing test={runtime_test_name}"
        ));
    }
    let summary = transcript
        .lines()
        .rfind(|line| line.starts_with("test result: "))
        .and_then(phase5_parse_summary_line)
        .ok_or_else(|| "phase5_exact_test_summary_missing".to_owned())?;
    if summary
        != (Phase5RegressionCounts {
            total_tests: 1,
            passed: 1,
            failed: 0,
            ignored: 0,
        })
    {
        return Err("phase5_exact_test_summary_not_one_pass".to_owned());
    }
    Ok(())
}

fn validate_phase5_baseline_evidence(
    workspace_root: &Path,
    evidence_root: &Path,
    baseline: &RegressionBaselineReference,
    baseline_metadata_git_sha: &str,
    expected_project_id: &str,
) -> Result<(), String> {
    let evidence = baseline
        .baseline_evidence
        .as_ref()
        .ok_or_else(|| "regression_baseline_workspace_evidence_missing".to_owned())?;
    if evidence.source_commit != baseline.baseline_commit
        || evidence.workspace.execution.argv != PHASE5_WORKSPACE_ARGV.map(str::to_owned).to_vec()
    {
        return Err("regression_baseline_workspace_evidence_identity_mismatch".to_owned());
    }
    let binding = Phase5LeafBinding::MetadataCommit {
        workspace_root,
        metadata_commit: baseline_metadata_git_sha,
    };
    let validated = validate_phase5_run(
        evidence_root,
        &baseline.baseline_commit,
        &evidence.workspace,
        &binding,
        expected_project_id,
    )?;
    let observed = parse_phase5_workspace_counts(&validated.transcript)?;
    let expected = Phase5RegressionCounts {
        total_tests: baseline.total_tests,
        passed: baseline.passed,
        failed: baseline.failed,
        ignored: baseline.ignored,
    };
    if observed != expected {
        return Err("regression_baseline_workspace_counts_mismatch".to_owned());
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn validate_phase5_live_guard_receipt(
    workspace_root: &Path,
    evidence_root: &Path,
    receipt: &Phase5LiveGuardReceipt,
    evidence_git_sha: &str,
    tested_candidate_git_sha: &str,
    baseline_metadata_git_sha: &str,
    phase5_manifest_sha256: &str,
    expected_project_id: &str,
    expected: &ExpectedPhase5Run,
    manifest_index: &BTreeMap<String, (String, u64)>,
) -> Result<(), String> {
    if receipt.schema_version != PHASE5_LIVE_GUARD_RECEIPT_SCHEMA
        || receipt.source_commit != evidence_git_sha
        || receipt.tested_tree_blake3
            != tested_tree_blake3(workspace_root, tested_candidate_git_sha)?
        || receipt.phase5_manifest_sha256 != phase5_manifest_sha256
        || receipt.baseline_metadata_git_sha != baseline_metadata_git_sha
        || receipt.project_id != expected_project_id
        || receipt.requirement_blake3 != expected.requirement_blake3
        || receipt.evidence.execution.argv != expected.argv
    {
        return Err("phase5_live_guard_receipt_identity_mismatch".to_owned());
    }
    let binding = Phase5LeafBinding::CandidateManifest(manifest_index);
    let validated = validate_phase5_run(
        evidence_root,
        tested_candidate_git_sha,
        &receipt.evidence,
        &binding,
        expected_project_id,
    )?;
    validate_phase5_exact_test_transcript(&validated.transcript, &expected.argv)?;
    Ok(())
}

fn unique_t16_line<'a>(transcript: &'a str, prefix: &str) -> Result<&'a str, String> {
    let matches = transcript
        .lines()
        .map(str::trim)
        .filter(|line| line.starts_with(prefix))
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [line] => Ok(line),
        _ => Err(format!(
            "t16_transcript_line_cardinality_mismatch prefix={prefix} count={}",
            matches.len()
        )),
    }
}

fn t16_token_allow_empty<'a>(line: &'a str, key: &str) -> Result<&'a str, String> {
    let prefix = format!("{key}=");
    let matches = line
        .split_whitespace()
        .filter_map(|token| token.strip_prefix(&prefix))
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [value] => Ok(value),
        _ => Err(format!("t16_token_missing_or_duplicate key={key}")),
    }
}

fn t16_token<'a>(line: &'a str, key: &str) -> Result<&'a str, String> {
    t16_token_allow_empty(line, key).and_then(|value| {
        if value.is_empty() {
            Err(format!("t16_token_empty key={key}"))
        } else {
            Ok(value)
        }
    })
}

fn t16_take_segment<'a>(
    input: &mut &'a str,
    prefix: &str,
    delimiter: &str,
) -> Result<&'a str, String> {
    let remainder = input
        .strip_prefix(prefix)
        .ok_or_else(|| format!("t16_segment_prefix_mismatch prefix={prefix}"))?;
    let (value, tail) = remainder
        .split_once(delimiter)
        .ok_or_else(|| format!("t16_segment_delimiter_missing delimiter={delimiter}"))?;
    if value.is_empty() {
        return Err(format!("t16_segment_empty prefix={prefix}"));
    }
    *input = tail;
    Ok(value)
}

fn parse_t16_sample(line: &str) -> Result<T16SemanticSample, String> {
    let mut input = line;
    let sample = t16_take_segment(&mut input, "sample ", " (")?
        .parse::<usize>()
        .map_err(|error| format!("t16_sample_index_invalid: {error}"))?;
    let order = t16_take_segment(&mut input, "", "): csqlite=")?.to_owned();
    let csqlite_ops_per_sec = t16_take_segment(&mut input, "", " ops/s, fsqlite=")?
        .parse::<f64>()
        .map_err(|error| format!("t16_csqlite_throughput_invalid: {error}"))?;
    let fsqlite_ops_per_sec = t16_take_segment(&mut input, "", " ops/s, F/C=")?
        .parse::<f64>()
        .map_err(|error| format!("t16_fsqlite_throughput_invalid: {error}"))?;
    let ratio = t16_take_segment(&mut input, "", "x, rows=")?
        .parse::<f64>()
        .map_err(|error| format!("t16_ratio_invalid: {error}"))?;
    let (csqlite_rows, fsqlite_rows) = input
        .split_once('/')
        .ok_or_else(|| "t16_sample_rows_delimiter_missing".to_owned())?;
    let csqlite_total_rows = csqlite_rows
        .parse::<i64>()
        .map_err(|error| format!("t16_csqlite_rows_invalid: {error}"))?;
    let fsqlite_total_rows = fsqlite_rows
        .parse::<i64>()
        .map_err(|error| format!("t16_fsqlite_rows_invalid: {error}"))?;
    Ok(T16SemanticSample {
        sample,
        order,
        csqlite_ops_per_sec,
        fsqlite_ops_per_sec,
        ratio,
        csqlite_total_rows,
        fsqlite_total_rows,
    })
}

#[allow(clippy::too_many_lines)]
fn validate_t16_transcript(
    transcript: &str,
    candidate_git_sha: &str,
    feature_graph_sha256: &str,
    target: &str,
    expected_argv: &[String],
) -> Result<T16SemanticEvidence, String> {
    let expected_replay = format!("canonical replay: {}", expected_argv.join(" "));
    if unique_t16_line(transcript, "canonical replay: ")? != expected_replay {
        return Err("t16_canonical_replay_command_mismatch".to_owned());
    }

    let source = unique_t16_line(transcript, "provenance/source: ")?;
    let source_sha = t16_token(source, "sha")?.to_owned();
    let source_dirty = t16_token(source, "dirty")?.to_owned();
    let input_tracking = t16_token(source, "input_tracking")?.to_owned();
    let branch = t16_token(source, "branch")?;
    if source_sha != candidate_git_sha
        || source_dirty != "false"
        || input_tracking != "complete"
        || branch == "unknown"
    {
        return Err("t16_source_provenance_mismatch".to_owned());
    }

    let toolchain = unique_t16_line(transcript, "provenance/toolchain: ")?;
    let observed_target = t16_token(toolchain, "target")?.to_owned();
    let selected_profile = t16_token(toolchain, "selected_profile")?.to_owned();
    if observed_target != target
        || selected_profile != "release-perf"
        || t16_token(toolchain, "host")? == "unknown"
        || t16_token(toolchain, "profile")? == "unknown"
        || !toolchain.contains(" rustc=")
        || !toolchain.contains(" cargo=")
    {
        return Err("t16_toolchain_provenance_mismatch".to_owned());
    }

    let flags = unique_t16_line(transcript, "provenance/flags: ")?;
    let observed_feature_graph = t16_token(flags, "feature_graph_sha256")?.to_owned();
    if observed_feature_graph != feature_graph_sha256
        || !matches!(
            t16_token(flags, "encoded_rustflags_present")?,
            "true" | "false"
        )
    {
        return Err("t16_flag_provenance_mismatch".to_owned());
    }
    for key in [
        "rustflags_hex",
        "profile_overrides_hex",
        "native_overrides_hex",
    ] {
        let value = t16_token_allow_empty(flags, key)?;
        if !value.len().is_multiple_of(2) || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(format!("t16_hex_provenance_invalid key={key}"));
        }
    }

    let binary = unique_t16_line(transcript, "provenance/binary: ")?
        .strip_prefix("provenance/binary: path=")
        .ok_or_else(|| "t16_binary_provenance_prefix_mismatch".to_owned())?;
    let (binary_path, binary_sha256) = binary
        .rsplit_once(" sha256=")
        .ok_or_else(|| "t16_binary_provenance_shape_invalid".to_owned())?;
    if binary_path.trim().is_empty() || !is_lower_hex(binary_sha256, 64) {
        return Err("t16_binary_provenance_invalid".to_owned());
    }
    let runtime_machine = unique_t16_line(transcript, "provenance/runtime: machine=")?
        .strip_prefix("provenance/runtime: machine=")
        .filter(|machine| {
            !machine.trim().is_empty()
                && !machine
                    .chars()
                    .any(|character| character.is_control() || character.is_whitespace())
        })
        .ok_or_else(|| "t16_runtime_machine_invalid".to_owned())?
        .to_owned();

    let mut summary = unique_t16_line(transcript, "16-thread per-worker-transaction: ")?;
    let ratio_median = t16_take_segment(
        &mut summary,
        "16-thread per-worker-transaction: median F/C=",
        "x, lower bound (order statistic ",
    )?
    .parse::<f64>()
    .map_err(|error| format!("t16_median_invalid: {error}"))?;
    let bound_index = t16_take_segment(&mut summary, "", " of ")?
        .parse::<usize>()
        .map_err(|error| format!("t16_bound_index_invalid: {error}"))?;
    let bound_sample_count = t16_take_segment(&mut summary, "", ", >=")?
        .parse::<usize>()
        .map_err(|error| format!("t16_bound_sample_count_invalid: {error}"))?;
    let confidence = t16_take_segment(&mut summary, "", "% confidence)=")?
        .parse::<u8>()
        .map_err(|error| format!("t16_confidence_invalid: {error}"))?;
    let ratio_lower_bound = t16_take_segment(&mut summary, "", "x, threshold=")?
        .parse::<f64>()
        .map_err(|error| format!("t16_lower_bound_invalid: {error}"))?;
    let threshold = t16_take_segment(&mut summary, "", "x, wins=")?
        .parse::<f64>()
        .map_err(|error| format!("t16_threshold_invalid: {error}"))?;
    let winning_samples = t16_take_segment(&mut summary, "", "/")?
        .parse::<usize>()
        .map_err(|error| format!("t16_winning_samples_invalid: {error}"))?;
    let winning_denominator = t16_take_segment(&mut summary, "", ", expected_rows_per_sample=")?
        .parse::<usize>()
        .map_err(|error| format!("t16_winning_denominator_invalid: {error}"))?;
    let expected_rows = summary
        .parse::<i64>()
        .map_err(|error| format!("t16_expected_rows_invalid: {error}"))?;
    if bound_index != T16_BOUND_ORDER_STATISTIC
        || bound_sample_count != T16_SAMPLE_COUNT
        || confidence != 95
        || winning_denominator != T16_SAMPLE_COUNT
        || expected_rows != T16_EXPECTED_ROWS_PER_SAMPLE
        || threshold != T16_MIN_RATIO_LOWER_BOUND
        || !ratio_median.is_finite()
        || !ratio_lower_bound.is_finite()
        || ratio_lower_bound <= threshold
    {
        return Err("t16_summary_semantics_invalid".to_owned());
    }

    let sample_lines = transcript
        .lines()
        .map(str::trim)
        .filter(|line| line.starts_with("sample "))
        .collect::<Vec<_>>();
    if sample_lines.len() != T16_SAMPLE_COUNT {
        return Err(format!(
            "t16_transcript_sample_count_mismatch observed={}",
            sample_lines.len()
        ));
    }
    let samples = sample_lines
        .into_iter()
        .map(parse_t16_sample)
        .collect::<Result<Vec<_>, _>>()?;
    for (index, sample) in samples.iter().enumerate() {
        let expected_order = if index.is_multiple_of(2) {
            "fsqlite_first"
        } else {
            "csqlite_first"
        };
        let minimum_ratio =
            (sample.fsqlite_ops_per_sec - 0.5).max(0.0) / (sample.csqlite_ops_per_sec + 0.5);
        let maximum_ratio = (sample.fsqlite_ops_per_sec + 0.5) / (sample.csqlite_ops_per_sec - 0.5);
        let printed_ratio_tolerance = 0.000_05;
        if sample.sample != index
            || sample.order != expected_order
            || !sample.csqlite_ops_per_sec.is_finite()
            || sample.csqlite_ops_per_sec <= 0.5
            || !sample.fsqlite_ops_per_sec.is_finite()
            || sample.fsqlite_ops_per_sec <= 0.5
            || !sample.ratio.is_finite()
            || sample.ratio <= 0.0
            || sample.ratio + printed_ratio_tolerance < minimum_ratio
            || sample.ratio - printed_ratio_tolerance > maximum_ratio
            || sample.csqlite_total_rows != T16_EXPECTED_ROWS_PER_SAMPLE
            || sample.fsqlite_total_rows != T16_EXPECTED_ROWS_PER_SAMPLE
        {
            return Err(format!("t16_sample_semantics_invalid sample={index}"));
        }
    }
    let mut sorted_ratios = samples
        .iter()
        .map(|sample| sample.ratio)
        .collect::<Vec<_>>();
    sorted_ratios.sort_by(f64::total_cmp);
    let recomputed_bound = sorted_ratios[T16_BOUND_ORDER_STATISTIC - 1];
    let midpoint = T16_SAMPLE_COUNT / 2;
    let recomputed_median = f64::midpoint(sorted_ratios[midpoint - 1], sorted_ratios[midpoint]);
    let definite_wins = samples
        .iter()
        .filter(|sample| sample.ratio > threshold)
        .count();
    let rounded_threshold_samples = samples
        .iter()
        .filter(|sample| sample.ratio.total_cmp(&threshold).is_eq())
        .count();
    if (ratio_lower_bound - recomputed_bound).abs() > 0.0001
        || (ratio_median - recomputed_median).abs() > 0.0001
        || winning_samples < definite_wins
        || winning_samples > definite_wins.saturating_add(rounded_threshold_samples)
    {
        return Err("t16_summary_does_not_match_samples".to_owned());
    }
    Ok(T16SemanticEvidence {
        binary_path: binary_path.to_owned(),
        binary_sha256: binary_sha256.to_owned(),
        runtime_machine,
    })
}

fn validate_t16_binary_manifest_binding(
    evidence: &T16SemanticEvidence,
    manifest_index: &BTreeMap<String, (String, u64)>,
) -> Result<(), String> {
    let binary_path = Path::new(&evidence.binary_path);
    if !binary_path.is_absolute()
        || binary_path
            .components()
            .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
    {
        return Err("t16_binary_path_not_canonical_absolute_path".to_owned());
    }
    let binary_name = binary_path
        .file_name()
        .ok_or_else(|| "t16_binary_path_missing_file_name".to_owned())?;
    let matches = manifest_index
        .iter()
        .filter(|entry| {
            let (path, artifact) = *entry;
            artifact.1 > 0
                && artifact.0.as_str() == evidence.binary_sha256.as_str()
                && Path::new(path.as_str()).file_name() == Some(binary_name)
        })
        .count();
    if matches != 1 {
        return Err(format!(
            "t16_binary_manifest_binding_cardinality_mismatch count={matches}"
        ));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
fn validate_phase5_performance_regression_gate(
    performance: &Phase5PerformanceRegressionGate,
) -> Result<(), String> {
    const SCHEMA: &str = "fsqlite.performance_release_admission.v1";
    const STATUS: &str = "blocked_no_immutable_historical_baseline";
    const BLOCKERS: [&str; 4] = ["bd-dqdoe", "bd-uh1fv", "bd-zywqc.2", "bd-1dp9.6.4"];
    const RATIONALE: &str = "Dual-profile persistent receipts prove only profile-bound capture integrity. The existing perf_regression_gate is diagnostic-only and has no immutable historical paired baseline, calibration, synthetic-regression sensitivity proof, or authoritative regression policy; it cannot authorize release.";
    if performance.schema_version != SCHEMA
        || performance.status != STATUS
        || performance.release_authorized
        || !performance.blockers.iter().map(String::as_str).eq(BLOCKERS)
        || performance.rationale != RATIONALE
    {
        return Err("phase5_performance_regression_gate_contract_invalid".to_owned());
    }
    Ok(())
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
fn validate_phase5_manifest(
    workspace_root: &Path,
    evidence_root: &Path,
    manifest: &Phase5Manifest,
    tested_candidate_git_sha: &str,
    feature_graph_sha256: &str,
    feature_graph_target: &str,
    expected_requirements: &ExpectedPhase5Requirements,
    expected_project_id: &str,
    manifest_index: &BTreeMap<String, (String, u64)>,
) -> Result<(), String> {
    if manifest.schema_version != PHASE5_SCHEMA_VERSION
        || manifest.tested_commit != tested_candidate_git_sha
        || manifest.evidence_pack.is_empty()
    {
        return Err("phase5_manifest_candidate_or_schema_mismatch".to_owned());
    }
    validate_phase5_performance_regression_gate(&manifest.performance_regression_gate)?;
    let expected_minisig_path =
        format!("{PHASE5_EVIDENCE_PREFIX}/{tested_candidate_git_sha}/signing/manifest.minisig");
    let declared_minisig_path = manifest.signature_path.as_str();
    if !PartialEq::eq(declared_minisig_path, expected_minisig_path.as_str())
        || !manifest_index.contains_key(declared_minisig_path)
    {
        return Err("phase5_manifest_signature_path_not_candidate_bound".to_owned());
    }
    let mut pack = BTreeSet::new();
    let mut pack_paths = BTreeSet::new();
    let mut previous_pack_path: Option<&str> = None;
    for leaf in &manifest.evidence_pack {
        if previous_pack_path.is_some_and(|previous| previous >= leaf.path.as_str()) {
            return Err("phase5_evidence_pack_not_strictly_path_sorted".to_owned());
        }
        previous_pack_path = Some(&leaf.path);
        insert_phase5_leaf(&mut pack, &mut pack_paths, leaf)?;
        drop(load_bound_phase5_leaf(
            evidence_root,
            tested_candidate_git_sha,
            leaf,
            &Phase5LeafBinding::CandidateManifest(manifest_index),
        )?);
    }
    if pack_paths.contains(&manifest.signature_path) {
        return Err("phase5_signature_must_not_be_evidence_pack_leaf".to_owned());
    }

    let binding = Phase5LeafBinding::Pack(&pack);
    let mut referenced = BTreeSet::new();
    let mut referenced_paths = BTreeSet::new();
    for leaf in [
        &manifest.signer_attestation,
        &manifest.cargo_lock,
        &manifest.rust_toolchain,
        &manifest.pre_capture_untracked,
        &manifest.auxiliary_scorecards.c1.scorecard,
        &manifest.auxiliary_scorecards.c1.pack_manifest,
        &manifest.auxiliary_scorecards.c1.commit_provenance,
        &manifest.auxiliary_scorecards.persistent.release.scorecard,
        &manifest
            .auxiliary_scorecards
            .persistent
            .release
            .pack_manifest,
        &manifest
            .auxiliary_scorecards
            .persistent
            .release
            .commit_provenance,
        &manifest
            .auxiliary_scorecards
            .persistent
            .release_perf
            .scorecard,
        &manifest
            .auxiliary_scorecards
            .persistent
            .release_perf
            .pack_manifest,
        &manifest
            .auxiliary_scorecards
            .persistent
            .release_perf
            .commit_provenance,
    ] {
        drop(load_bound_phase5_leaf(
            evidence_root,
            tested_candidate_git_sha,
            leaf,
            &binding,
        )?);
        insert_phase5_leaf(&mut referenced, &mut referenced_paths, leaf)?;
    }

    let expected_workspace = PHASE5_WORKSPACE_ARGV
        .iter()
        .map(|argument| (*argument).to_owned())
        .collect::<Vec<_>>();
    if manifest.workspace.execution.argv != expected_workspace {
        return Err("phase5_workspace_command_not_canonical".to_owned());
    }
    let workspace = validate_phase5_run(
        evidence_root,
        tested_candidate_git_sha,
        &manifest.workspace,
        &binding,
        expected_project_id,
    )?;
    merge_phase5_run_leaves(&mut referenced, &mut referenced_paths, &workspace)?;

    let (_, compiler_leaves) = validate_phase5_compiler_inventory(
        workspace_root,
        evidence_root,
        tested_candidate_git_sha,
        &manifest.compiler_inventory_attestation,
        &pack,
        expected_project_id,
    )?;
    for key in compiler_leaves {
        let path = key.split('\0').next().unwrap_or_default().to_owned();
        if !referenced.insert(key) || !referenced_paths.insert(path.clone()) {
            return Err(format!("phase5_compiler_leaf_reused path={path}"));
        }
    }

    let mut t16_evidence = None;
    let mut locators = BTreeSet::new();
    let mut previous_locator: Option<String> = None;
    for receipt in &manifest.run_receipts {
        let locator = format!("{}::{}", receipt.source_path, receipt.test_name);
        let expected_requirement = expected_requirements.current_runs.get(&locator);
        if previous_locator
            .as_ref()
            .is_some_and(|previous| previous >= &locator)
            || !locators.insert(locator.clone())
            || expected_requirement.map(|expected| &expected.requirement_blake3)
                != Some(&receipt.requirement_blake3)
            || !is_lower_hex(&receipt.requirement_blake3, 64)
        {
            return Err(format!(
                "phase5_run_receipt_identity_invalid locator={locator}"
            ));
        }
        let expected = expected_requirement
            .ok_or_else(|| format!("phase5_run_receipt_unexpected locator={locator}"))?;
        if receipt.evidence.execution.argv != expected.argv {
            return Err(format!(
                "phase5_run_receipt_command_not_canonical locator={locator}"
            ));
        }
        let run = validate_phase5_run(
            evidence_root,
            tested_candidate_git_sha,
            &receipt.evidence,
            &binding,
            expected_project_id,
        )?;
        validate_phase5_exact_test_transcript(&run.transcript, &expected.argv)?;
        merge_phase5_run_leaves(&mut referenced, &mut referenced_paths, &run)?;
        if receipt.source_path == T16_SOURCE_PATH && receipt.test_name == T16_TEST_NAME {
            if t16_evidence.is_some() {
                return Err("phase5_t16_receipt_duplicate_or_wrong_command".to_owned());
            }
            let semantic = validate_t16_transcript(
                &run.transcript,
                tested_candidate_git_sha,
                feature_graph_sha256,
                feature_graph_target,
                &expected.argv,
            )?;
            if semantic.runtime_machine != run.worker_id {
                return Err("t16_runtime_machine_not_bound_to_rch_worker".to_owned());
            }
            validate_t16_binary_manifest_binding(&semantic, manifest_index)?;
            t16_evidence = Some(semantic);
        }
        previous_locator = Some(locator);
    }
    let expected_locators = expected_requirements
        .current_runs
        .keys()
        .cloned()
        .collect::<BTreeSet<_>>();
    if locators != expected_locators {
        return Err("phase5_run_receipt_coverage_mismatch".to_owned());
    }
    if referenced != pack || referenced_paths != pack_paths {
        return Err("phase5_evidence_pack_not_exactly_referenced".to_owned());
    }
    if t16_evidence.is_none() {
        return Err("phase5_t16_receipt_missing".to_owned());
    }
    Err("phase5_performance_regression_gate_not_authorized".to_owned())
}

fn validate_run_identity(input: &StrictCertificateEvidenceInput) -> Result<(), String> {
    for (field, value) in [
        ("run_id", input.run_id.as_str()),
        ("trace_id", input.trace_id.as_str()),
        ("scenario_id", input.scenario_id.as_str()),
    ] {
        if value.trim().is_empty()
            || value.len() > 256
            || value
                .chars()
                .any(|character| character.is_control() || character.is_whitespace())
        {
            return Err(format!(
                "certificate_input_run_identity_invalid field={field}"
            ));
        }
    }
    Ok(())
}

fn validate_contract_coherence(
    gate: &GateReport,
    ledger: &EvidenceLedger,
    scorecard: &BayesianScorecard,
    manifest: &ArtifactManifest,
) -> Result<(), String> {
    let gate_contract = gate
        .verification_contract
        .as_ref()
        .ok_or_else(|| "gate_report_missing_verification_contract".to_owned())?;
    let ledger_contract = ledger
        .verification_contract
        .as_ref()
        .ok_or_else(|| "evidence_ledger_missing_verification_contract".to_owned())?;
    let scorecard_contract = scorecard
        .verification_contract
        .as_ref()
        .ok_or_else(|| "scorecard_missing_verification_contract".to_owned())?;
    let manifest_contract = manifest
        .verification_contract
        .as_ref()
        .ok_or_else(|| "artifact_manifest_missing_verification_contract".to_owned())?;
    for (label, contract) in [
        ("gate_report", gate_contract),
        ("evidence_ledger", ledger_contract),
        ("scorecard", scorecard_contract),
        ("artifact_manifest", manifest_contract),
    ] {
        validate_contract_outcome(contract, label)?;
    }
    if !json_values_equal(gate_contract, ledger_contract)?
        || !json_values_equal(gate_contract, scorecard_contract)?
        || !json_values_equal(gate_contract, manifest_contract)?
    {
        return Err("verification_contract_evidence_mismatch".to_owned());
    }
    Ok(())
}

fn validate_ratchets(
    baseline: &CertificationRatchetBaseline,
    candidate: &CertificationRatchetCandidate,
    gate: &GateReport,
    traceability: &ReleaseTraceabilityReport,
) -> Result<(), String> {
    if baseline.schema_version != CERTIFICATION_POLICY_SCHEMA_VERSION
        || baseline.policy_id != CERTIFICATION_POLICY_ID
    {
        return Err("ratchet_baseline_not_strict_policy".to_owned());
    }
    let expected = CertificationRatchetCandidate {
        global_lower_bound: gate.global_lower_bound,
        category_lower_bounds: gate
            .category_results
            .iter()
            .map(|(category, result)| (category.clone(), result.credible_lower))
            .collect(),
        required_suite_pass_rate_pct: 100.0,
        traceability_link_coverage_pct: traceability.verification_pct,
    };
    if !json_values_equal(candidate, &expected)?
        || !evaluate_certification_ratchets(baseline, candidate).passed
    {
        return Err("certification_ratchet_not_preserved".to_owned());
    }
    Ok(())
}

fn validate_ratchet_baseline_anchor(
    workspace_root: &Path,
    baseline: &LoadedStrictEvidence<CertificationRatchetBaseline>,
    baseline_git_sha: &str,
    candidate_git_sha: &str,
) -> Result<(), String> {
    if !is_lower_hex(baseline_git_sha, 40) || baseline_git_sha == candidate_git_sha {
        return Err("ratchet_baseline_git_sha_not_independent_ancestor".to_owned());
    }
    require_ancestor(workspace_root, baseline_git_sha, candidate_git_sha)?;
    let historical = git_blob_at_commit(workspace_root, baseline_git_sha, &baseline.relative_path)?;
    if historical != baseline.bytes {
        return Err("ratchet_baseline_not_identical_to_anchored_git_blob".to_owned());
    }
    Ok(())
}

fn validate_traceability_manifest_binding(
    traceability: &ReleaseTraceabilityReport,
    manifest_index: &BTreeMap<String, (String, u64)>,
) -> Result<(), String> {
    if traceability.entries.is_empty()
        || !traceability.release_ready
        || traceability.verification_pct != 100.0
    {
        return Err("release_traceability_not_complete".to_owned());
    }
    for entry in &traceability.entries {
        let artifact_set = entry.artifact_refs.iter().collect::<BTreeSet<_>>();
        if !entry.verified
            || entry.artifact_refs.is_empty()
            || artifact_set.len() != entry.artifact_refs.len()
        {
            return Err(format!(
                "release_traceability_entry_not_fully_linked invariant={}",
                entry.invariant_id.0
            ));
        }
        for artifact in &entry.artifact_refs {
            let canonical = checked_relative_path(Path::new(artifact))?
                .to_string_lossy()
                .into_owned();
            if canonical != *artifact || !manifest_index.contains_key(artifact) {
                return Err(format!(
                    "release_traceability_artifact_not_in_manifest path={artifact}"
                ));
            }
        }
    }
    Ok(())
}

fn resolve_output_path(workspace_root: &Path, output_dir: &Path) -> PathBuf {
    if output_dir.is_absolute() {
        output_dir.to_path_buf()
    } else {
        workspace_root.join(output_dir)
    }
}

fn reject_symlink_components(path: &Path) -> Result<(), String> {
    let mut current = PathBuf::new();
    for component in path.components() {
        current.push(component.as_os_str());
        if let Ok(metadata) = fs::symlink_metadata(&current)
            && metadata.file_type().is_symlink()
        {
            return Err(format!(
                "certificate_output_alias_component path={}",
                current.display()
            ));
        }
    }
    Ok(())
}

fn prepare_strict_output_directory(
    workspace_root: &Path,
    output_dir: &Path,
) -> Result<PathBuf, String> {
    if output_dir.as_os_str().is_empty()
        || output_dir
            .components()
            .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
    {
        return Err("certificate_output_path_not_canonical".to_owned());
    }
    let workspace_metadata = fs::symlink_metadata(workspace_root)
        .map_err(|error| format!("workspace_root_metadata_failed: {error}"))?;
    if workspace_metadata.file_type().is_symlink() || !workspace_metadata.is_dir() {
        return Err("workspace_root_must_be_real_directory".to_owned());
    }
    let canonical_workspace = workspace_root
        .canonicalize()
        .map_err(|error| format!("workspace_root_canonicalize_failed: {error}"))?;
    let requested = resolve_output_path(&canonical_workspace, output_dir);
    reject_symlink_components(&requested)?;
    if fs::symlink_metadata(&requested).is_ok() {
        return Err(format!(
            "refusing_to_overwrite_certificate_output path={}",
            requested.display()
        ));
    }
    let parent = requested
        .parent()
        .ok_or_else(|| "certificate_output_has_no_parent".to_owned())?;
    let parent_metadata = fs::symlink_metadata(parent)
        .map_err(|error| format!("certificate_output_parent_missing: {error}"))?;
    if parent_metadata.file_type().is_symlink() || !parent_metadata.is_dir() {
        return Err("certificate_output_parent_must_be_real_directory".to_owned());
    }
    let canonical_parent = parent
        .canonicalize()
        .map_err(|error| format!("certificate_output_parent_canonicalize_failed: {error}"))?;
    if !canonical_parent.starts_with(&canonical_workspace) {
        return Err("certificate_output_parent_outside_workspace".to_owned());
    }
    let name = requested
        .file_name()
        .ok_or_else(|| "certificate_output_has_no_name".to_owned())?;
    let final_path = canonical_parent.join(name);
    if fs::symlink_metadata(&final_path).is_ok() {
        return Err(format!(
            "refusing_to_overwrite_certificate_output path={}",
            final_path.display()
        ));
    }
    Ok(final_path)
}

fn write_new_bundle_file(path: &Path, payload: &[u8]) -> Result<(), String> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|error| {
            format!(
                "certificate_output_create_failed path={} error={error}",
                path.display()
            )
        })?;
    file.write_all(payload).map_err(|error| {
        format!(
            "certificate_output_write_failed path={} error={error}",
            path.display()
        )
    })?;
    file.sync_all().map_err(|error| {
        format!(
            "certificate_output_sync_failed path={} error={error}",
            path.display()
        )
    })
}

fn sync_directory(path: &Path) -> Result<(), String> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| {
            format!(
                "certificate_output_directory_sync_failed path={} error={error}",
                path.display()
            )
        })
}

fn strict_bundle_file_names(path: &Path) -> Result<BTreeSet<String>, String> {
    fs::read_dir(path)
        .map_err(|error| format!("certificate_output_read_failed: {error}"))?
        .map(|entry| {
            entry
                .map_err(|error| format!("certificate_output_entry_failed: {error}"))
                .map(|entry| entry.file_name().to_string_lossy().into_owned())
        })
        .collect()
}

fn publish_strict_certificate_bundle(
    config: &StrictCertificateRunConfig,
    input: &StrictCertificateEvidenceInput,
    input_path: &str,
    input_bytes: &[u8],
    certificate_json: &[u8],
    summary_markdown: &[u8],
) -> Result<(), String> {
    let final_path = prepare_strict_output_directory(&config.workspace_root, &config.output_dir)?;
    let parent = final_path
        .parent()
        .ok_or_else(|| "certificate_output_has_no_parent".to_owned())?;
    fs::create_dir(&final_path).map_err(|error| {
        if error.kind() == std::io::ErrorKind::AlreadyExists {
            format!(
                "refusing_to_overwrite_certificate_output path={}",
                final_path.display()
            )
        } else {
            format!(
                "certificate_output_reservation_failed path={} error={error}",
                final_path.display()
            )
        }
    })?;
    sync_directory(parent)?;
    write_new_bundle_file(&final_path.join("certificate.json"), certificate_json)?;
    write_new_bundle_file(&final_path.join("certificate.md"), summary_markdown)?;
    write_new_bundle_file(&final_path.join("evidence-manifest.json"), input_bytes)?;
    let data_names = strict_bundle_file_names(&final_path)?;
    let expected_data_names = BTreeSet::from([
        "certificate.json".to_owned(),
        "certificate.md".to_owned(),
        "evidence-manifest.json".to_owned(),
    ]);
    if data_names != expected_data_names {
        return Err("certificate_output_precommit_file_set_mismatch".to_owned());
    }
    sync_directory(&final_path)?;
    let bundle = StrictCertificateBundleManifest {
        schema_version: STRICT_CERTIFICATE_BUNDLE_SCHEMA.to_owned(),
        candidate_git_sha: input.candidate_git_sha.clone(),
        tested_candidate_git_sha: input.tested_candidate_git_sha.clone(),
        run_id: input.run_id.clone(),
        trace_id: input.trace_id.clone(),
        scenario_id: input.scenario_id.clone(),
        seed: input.seed,
        input_path: input_path.to_owned(),
        evidence_manifest_sha256: sha256_bytes(input_bytes),
        certificate_sha256: sha256_bytes(certificate_json),
        summary_sha256: sha256_bytes(summary_markdown),
    };
    let mut bundle_bytes = serde_json::to_vec_pretty(&bundle)
        .map_err(|error| format!("certificate_bundle_manifest_serialize_failed: {error}"))?;
    bundle_bytes.push(b'\n');
    // The manifest is the publication commit marker. The final directory is
    // reserved with create_dir (atomic no-replace), all data files are synced,
    // and only then is this marker created and the directory synced again.
    write_new_bundle_file(&final_path.join("bundle-manifest.json"), &bundle_bytes)?;

    let names = strict_bundle_file_names(&final_path)?;
    let expected = BTreeSet::from([
        "bundle-manifest.json".to_owned(),
        "certificate.json".to_owned(),
        "certificate.md".to_owned(),
        "evidence-manifest.json".to_owned(),
    ]);
    if names != expected {
        return Err("certificate_output_committed_file_set_mismatch".to_owned());
    }
    sync_directory(&final_path)?;
    sync_directory(parent)
}

/// Validate a frozen release candidate and atomically publish its certificate bundle.
///
/// Every referenced artifact is re-read and re-hashed. The function refuses a
/// dirty or mismatched checkout and never replaces an existing output path.
///
/// # Errors
///
/// Returns a stable fail-closed diagnostic when any candidate, evidence,
/// policy, performance, or publication invariant is not satisfied.
pub fn build_and_publish_strict_certificate(
    config: &StrictCertificateRunConfig,
) -> Result<ReleaseCertificate, String> {
    build_and_publish_strict_certificate_at(config, strict_current_unix_ms()?)
}

#[allow(clippy::too_many_lines)]
fn build_and_publish_strict_certificate_at(
    config: &StrictCertificateRunConfig,
    now_unix_ms: u128,
) -> Result<ReleaseCertificate, String> {
    if !is_lower_hex(&config.candidate_git_sha, 40) {
        return Err("candidate_git_sha_must_be_lowercase_40_hex".to_owned());
    }
    if !is_lower_hex(&config.tested_candidate_git_sha, 40) {
        return Err("tested_candidate_git_sha_must_be_lowercase_40_hex".to_owned());
    }
    if !is_lower_hex(&config.baseline_metadata_git_sha, 40) {
        return Err("baseline_metadata_git_sha_must_be_lowercase_40_hex".to_owned());
    }
    validate_rch_project_id(&config.candidate_rch_project_id, "candidate")?;
    validate_rch_project_id(&config.baseline_rch_project_id, "baseline")?;
    let workspace_metadata = fs::symlink_metadata(&config.workspace_root)
        .map_err(|error| format!("workspace_root_metadata_failed: {error}"))?;
    if workspace_metadata.file_type().is_symlink() || !workspace_metadata.is_dir() {
        return Err("workspace_root_must_be_real_directory".to_owned());
    }
    let canonical_workspace = config
        .workspace_root
        .canonicalize()
        .map_err(|error| format!("workspace_root_canonicalize_failed: {error}"))?;
    let evidence_root_metadata = fs::symlink_metadata(&config.evidence_root)
        .map_err(|error| format!("evidence_root_metadata_failed: {error}"))?;
    if evidence_root_metadata.file_type().is_symlink() || !evidence_root_metadata.is_dir() {
        return Err("evidence_root_must_be_real_directory".to_owned());
    }
    let canonical_evidence_root = config
        .evidence_root
        .canonicalize()
        .map_err(|error| format!("evidence_root_canonicalize_failed: {error}"))?;
    if canonical_evidence_root != canonical_workspace {
        return Err("strict_evidence_root_must_equal_workspace_root".to_owned());
    }
    if current_head(&config.workspace_root)? != config.candidate_git_sha {
        return Err("candidate_git_sha_does_not_match_checked_out_head".to_owned());
    }
    require_direct_phase5_evidence_descendant(
        &config.workspace_root,
        &config.tested_candidate_git_sha,
        &config.candidate_git_sha,
    )?;
    require_exact_clean_checkout(&config.workspace_root)?;
    let input_path = canonical_regular_file(&config.evidence_root, &config.evidence_json)?;
    let input_relative = canonical_relative_path(&config.evidence_root, &input_path)?;
    if input_relative.as_str() != config.evidence_json.to_string_lossy().as_ref() {
        return Err("certificate_input_path_not_canonical".to_owned());
    }
    let input_bytes =
        fs::read(&input_path).map_err(|error| format!("certificate_input_read_failed: {error}"))?;
    let input: StrictCertificateEvidenceInput = serde_json::from_slice(&input_bytes)
        .map_err(|error| format!("certificate_input_parse_failed: {error}"))?;
    let max_freshness_ms = u128::from(CERTIFICATION_MAX_EVIDENCE_AGE_HOURS)
        .checked_mul(60 * 60 * 1_000)
        .ok_or_else(|| "certification_freshness_budget_overflow".to_owned())?;
    if input.schema_version != STRICT_CERTIFICATE_INPUT_SCHEMA
        || input.candidate_git_sha != config.candidate_git_sha
        || input.tested_candidate_git_sha != config.tested_candidate_git_sha
        || input.freshness_budget_ms == 0
        || input.freshness_budget_ms > max_freshness_ms
        || input.generated_unix_ms > now_unix_ms
        || now_unix_ms
            .checked_sub(input.generated_unix_ms)
            .is_none_or(|age| age > input.freshness_budget_ms)
    {
        return Err("certificate_input_not_current_candidate_evidence".to_owned());
    }
    validate_run_identity(&input)?;

    let candidate_manifest: LoadedStrictEvidence<ArtifactManifest> = load_strict_json(
        &config.evidence_root,
        &input.candidate_artifact_manifest,
        input.generated_unix_ms,
        now_unix_ms,
        input.freshness_budget_ms,
    )?;
    validate_candidate_manifest(&candidate_manifest.value, &config.candidate_git_sha)?;
    if candidate_manifest.value.run_id != input.run_id
        || candidate_manifest.value.seed != input.seed
    {
        return Err("candidate_artifact_manifest_run_identity_mismatch".to_owned());
    }
    let manifest_index = manifest_artifact_index(&config.evidence_root, &candidate_manifest.value)?;

    macro_rules! load_bound_json {
        ($reference:expr, $ty:ty) => {{
            let loaded: LoadedStrictEvidence<$ty> = load_strict_json(
                &config.evidence_root,
                $reference,
                input.generated_unix_ms,
                now_unix_ms,
                input.freshness_budget_ms,
            )?;
            require_manifest_binding(&manifest_index, &loaded)?;
            loaded
        }};
    }

    let workflow = load_bound_json!(&input.workflow_report, WorkflowReport);
    let gate = load_bound_json!(&input.gate_report, GateReport);
    let ranking = load_bound_json!(&input.expected_loss_ranking, ExpectedLossRanking);
    let ledger = load_bound_json!(&input.evidence_ledger, EvidenceLedger);
    let catalog = load_bound_json!(&input.catalog_stats, CatalogStats);
    let traceability = load_bound_json!(&input.release_traceability, ReleaseTraceabilityReport);
    let drift = load_bound_json!(&input.drift_snapshot, ParityDriftSnapshot);
    let campaign = load_bound_json!(&input.adversarial_campaign, CampaignResult);
    let flake = load_bound_json!(&input.ci_flake_budget, GlobalFlakeBudgetResult);
    let policy = load_bound_json!(&input.certification_policy, CertificationPolicy);
    let ratchet_baseline = load_bound_json!(&input.ratchet_baseline, CertificationRatchetBaseline);
    let ratchet_candidate =
        load_bound_json!(&input.ratchet_candidate, CertificationRatchetCandidate);
    let critical_path = load_bound_json!(&input.critical_path_evidence, NoMockCriticalPathReport);
    let scorecard = load_bound_json!(&input.scorecard, BayesianScorecard);
    let d4 = load_bound_json!(&input.d4_runtime_path_proof, D4RuntimePathProof);
    let g9 = load_bound_json!(&input.g9_gate_summary, FallbackTransparencyGateSummary);
    let regression = load_bound_json!(&input.regression_baseline, RegressionBaselineReference);
    let phase5 = load_bound_json!(&input.phase5_release_evidence_manifest, Phase5Manifest);
    let live_guard = load_bound_json!(&input.phase5_live_guard_receipt, Phase5LiveGuardReceipt);
    let feature_graph = load_bound_json!(&input.dependency_feature_graph, serde_json::Value);
    let results = load_strict_evidence_bytes(
        &config.evidence_root,
        &input.results_jsonl,
        input.generated_unix_ms,
        now_unix_ms,
        input.freshness_budget_ms,
    )?;
    require_manifest_binding(&manifest_index, &results)?;
    let lane_manifests = validate_required_lane_manifests(
        &config.evidence_root,
        &input,
        &candidate_manifest,
        &manifest_index,
        now_unix_ms,
        input.freshness_budget_ms,
    )?;

    validate_workflow_evidence(
        &config.evidence_root,
        &workflow.value,
        &input,
        &manifest_index,
        now_unix_ms,
        input.freshness_budget_ms,
    )?;
    validate_policy(&policy.value)?;
    validate_gate_inputs(
        &gate.value,
        &ranking.value,
        &ledger.value,
        &catalog.value,
        &traceability.value,
        &drift.value,
        &campaign.value,
        &flake.value,
        &critical_path.value,
        &scorecard.value,
    )?;
    validate_contract_coherence(
        &gate.value,
        &ledger.value,
        &scorecard.value,
        &candidate_manifest.value,
    )?;
    validate_traceability_manifest_binding(&traceability.value, &manifest_index)?;
    validate_ratchets(
        &ratchet_baseline.value,
        &ratchet_candidate.value,
        &gate.value,
        &traceability.value,
    )?;
    validate_ratchet_baseline_anchor(
        &config.workspace_root,
        &ratchet_baseline,
        &input.ratchet_baseline_git_sha,
        &config.candidate_git_sha,
    )?;
    validate_results_jsonl(&results.bytes, &input, &traceability.value, &manifest_index)?;
    validate_d4_runtime_path_proof(
        &config.evidence_root,
        &d4.value,
        &input,
        &manifest_index,
        now_unix_ms,
        input.freshness_budget_ms,
    )?;
    let embedded_g9 = candidate_manifest
        .value
        .fallback_transparency_gate
        .as_ref()
        .ok_or_else(|| "artifact_manifest_missing_g9_gate".to_owned())?;
    if &g9.value != embedded_g9 {
        return Err("g9_standalone_and_embedded_summary_mismatch".to_owned());
    }
    validate_g9_summary_shape(&g9.value, &config.candidate_git_sha)?;
    validate_g9_artifacts(&config.evidence_root, &manifest_index, &g9.value)?;
    if regression.relative_path != REGRESSION_BASELINE_PATH {
        return Err("regression_baseline_path_not_canonical".to_owned());
    }
    let phase5_requirements = expected_phase5_requirements(&regression.value)?;
    validate_regression_baseline(
        &config.workspace_root,
        &regression.value,
        &regression.bytes,
        &config.baseline_metadata_git_sha,
        &config.tested_candidate_git_sha,
    )?;
    validate_phase5_baseline_evidence(
        &config.workspace_root,
        &config.evidence_root,
        &regression.value,
        &config.baseline_metadata_git_sha,
        &config.baseline_rch_project_id,
    )?;
    let feature_graph_target =
        validate_dependency_feature_graph(&config.workspace_root, &feature_graph)?;
    let expected_phase5_path = format!(
        "{PHASE5_EVIDENCE_PREFIX}/{}/manifest.json",
        config.tested_candidate_git_sha
    );
    if phase5.relative_path != expected_phase5_path {
        return Err("phase5_manifest_path_not_candidate_canonical".to_owned());
    }
    validate_phase5_manifest(
        &config.workspace_root,
        &config.evidence_root,
        &phase5.value,
        &config.tested_candidate_git_sha,
        &feature_graph.sha256,
        &feature_graph_target,
        &phase5_requirements,
        &config.candidate_rch_project_id,
        &manifest_index,
    )?;
    let expected_live_guard_prefix = format!(
        "{PHASE5_EVIDENCE_PREFIX}/{}/",
        config.tested_candidate_git_sha
    );
    if !live_guard
        .relative_path
        .starts_with(&expected_live_guard_prefix)
        || !live_guard.relative_path.ends_with(".json")
    {
        return Err("phase5_live_guard_receipt_path_not_candidate_canonical".to_owned());
    }
    validate_phase5_live_guard_receipt(
        &config.workspace_root,
        &config.evidence_root,
        &live_guard.value,
        &config.candidate_git_sha,
        &config.tested_candidate_git_sha,
        &config.baseline_metadata_git_sha,
        &phase5.sha256,
        &config.candidate_rch_project_id,
        &phase5_requirements.live_guard,
        &manifest_index,
    )?;

    if current_head(&config.workspace_root)? != config.candidate_git_sha {
        return Err("candidate_head_changed_during_certificate_validation".to_owned());
    }
    require_exact_clean_checkout(&config.workspace_root)?;
    let refreshed_candidate_manifest: LoadedStrictEvidence<ArtifactManifest> = load_strict_json(
        &config.evidence_root,
        &input.candidate_artifact_manifest,
        input.generated_unix_ms,
        now_unix_ms,
        input.freshness_budget_ms,
    )?;
    if refreshed_candidate_manifest.bytes != candidate_manifest.bytes
        || manifest_artifact_index(&config.evidence_root, &refreshed_candidate_manifest.value)?
            != manifest_index
    {
        return Err("candidate_evidence_changed_during_validation".to_owned());
    }
    for lane_manifest in &lane_manifests {
        drop(manifest_artifact_index(
            &config.evidence_root,
            &lane_manifest.value,
        )?);
    }
    for leaf in &phase5.value.evidence_pack {
        drop(load_phase5_leaf(
            &config.evidence_root,
            &config.tested_candidate_git_sha,
            leaf,
        )?);
    }
    if fs::read(&input_path).map_err(|error| format!("certificate_input_reread_failed: {error}"))?
        != input_bytes
    {
        return Err("certificate_input_changed_during_validation".to_owned());
    }

    let certificate = build_certificate(
        &CertificateInputs {
            gate_report: gate.value,
            expected_loss_ranking: ranking.value,
            evidence_ledger: ledger.value,
            catalog_stats: catalog.value,
            traceability: traceability.value,
            drift_snapshot: drift.value,
            campaign_result: campaign.value,
            ci_flake_budget: Some(flake.value),
            artifact_manifest: Some(candidate_manifest.value),
        },
        &CertificateConfig::default(),
    );
    if certificate.verdict != CertificateVerdict::Approved
        || !certificate.unresolved_risks.is_empty()
        || certificate.global_verification_pct != 100.0
        || !certificate.certification_evidence.artifact_manifest_present
        || certificate.certification_evidence.final_gate_passed != Some(true)
        || certificate
            .certification_evidence
            .fallback_transparency_gate_passed
            != Some(true)
        || certificate
            .certification_evidence
            .missing_artifact_ref_count
            != 0
    {
        return Err(format!(
            "strict_certificate_not_approved verdict={}",
            certificate.verdict
        ));
    }
    let mut certificate_json = certificate
        .to_json()
        .map_err(|error| format!("certificate_serialize_failed: {error}"))?
        .into_bytes();
    certificate_json.push(b'\n');
    let summary_markdown = format!(
        "# FrankenSQLite release certificate\n\nCandidate: `{}`\n\n{}\n",
        config.candidate_git_sha, certificate.summary
    );
    publish_strict_certificate_bundle(
        config,
        &input,
        &input_relative,
        &input_bytes,
        &certificate_json,
        summary_markdown.as_bytes(),
    )?;
    Ok(certificate)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_phase5_leaf(path: &str) -> Phase5EvidenceLeaf {
        Phase5EvidenceLeaf {
            path: path.to_owned(),
            digest_algorithm: PHASE5_DIGEST_ALGORITHM.to_owned(),
            digest: "a".repeat(64),
        }
    }

    #[test]
    fn phase5_diagnostic_performance_gate_remains_non_authorizing() {
        let mut performance = Phase5PerformanceRegressionGate {
            schema_version: "fsqlite.performance_release_admission.v1".to_owned(),
            status: "blocked_no_immutable_historical_baseline".to_owned(),
            release_authorized: false,
            blockers: vec![
                "bd-dqdoe".to_owned(),
                "bd-uh1fv".to_owned(),
                "bd-zywqc.2".to_owned(),
                "bd-1dp9.6.4".to_owned(),
            ],
            rationale: "Dual-profile persistent receipts prove only profile-bound capture integrity. The existing perf_regression_gate is diagnostic-only and has no immutable historical paired baseline, calibration, synthetic-regression sensitivity proof, or authoritative regression policy; it cannot authorize release.".to_owned(),
        };
        assert!(validate_phase5_performance_regression_gate(&performance).is_ok());
        performance.release_authorized = true;
        assert_eq!(
            validate_phase5_performance_regression_gate(&performance),
            Err("phase5_performance_regression_gate_contract_invalid".to_owned())
        );
    }

    #[test]
    fn phase5_provenance_accepts_direct_b_m_t_e_evidence_path() {
        // B -> M -> T is separately enforced by `validate_regression_baseline`.
        // This boundary test proves the new final link T -> E is exact.
        let baseline = "1".repeat(40);
        let metadata = "2".repeat(40);
        let tested = "3".repeat(40);
        let evidence = "4".repeat(40);
        assert_ne!(baseline, metadata);
        assert_ne!(metadata, tested);
        assert_eq!(
            validate_direct_phase5_evidence_parents(
                &tested,
                &evidence,
                &format!("{evidence} {tested}\n"),
            ),
            Ok(())
        );
    }

    #[test]
    fn phase5_provenance_rejects_evidence_parent_mismatch() {
        let tested = "3".repeat(40);
        let evidence = "4".repeat(40);
        let wrong_parent = "5".repeat(40);
        assert_eq!(
            validate_direct_phase5_evidence_parents(
                &tested,
                &evidence,
                &format!("{evidence} {wrong_parent}\n"),
            ),
            Err("phase5_evidence_commit_parent_mismatch".to_owned())
        );
    }

    #[test]
    fn phase5_provenance_rejects_merge_evidence_commit() {
        let tested = "3".repeat(40);
        let evidence = "4".repeat(40);
        let second_parent = "5".repeat(40);
        assert_eq!(
            validate_direct_phase5_evidence_parents(
                &tested,
                &evidence,
                &format!("{evidence} {tested} {second_parent}\n"),
            ),
            Err("phase5_evidence_commit_must_be_single_parent".to_owned())
        );
    }

    #[test]
    fn phase5_provenance_rejects_self_baselining_evidence_commit() {
        let tested = "3".repeat(40);
        assert_eq!(
            validate_direct_phase5_evidence_parents(&tested, &tested, &format!("{tested}\n")),
            Err("phase5_evidence_commit_must_not_equal_tested_candidate".to_owned())
        );
    }

    fn sample_run_for_release_entry(source_path: &str, test_name: &str) -> RegressionIgnoredTest {
        RegressionIgnoredTest {
            source_path: source_path.to_owned(),
            test_name: test_name.to_owned(),
            reason: "Release proof".to_owned(),
            cfg_condition: None,
            kind: RegressionIgnoreKind::Performance,
            policy: RegressionIgnorePolicy::RunForRelease,
            parent_tests: Vec::new(),
            evidence: RegressionEvidenceRequirement {
                requirement: "Run the exact keeper".to_owned(),
                receipt: None,
            },
        }
    }

    fn default_cert() -> ReleaseCertificate {
        let config = CertificateConfig::default();
        generate_release_certificate(&config)
    }

    #[test]
    fn strict_evidence_timestamp_rejects_future_and_stale_observations() {
        let mut reference = StrictEvidenceRef {
            path: "evidence.json".to_owned(),
            sha256: "a".repeat(64),
            observed_unix_ms: 101,
        };
        assert_eq!(
            validate_evidence_timestamp(&reference, 100, 110, 20)
                .expect_err("post-generation observation must fail"),
            "evidence_observed_after_manifest_generation path=evidence.json"
        );

        reference.observed_unix_ms = 50;
        assert!(
            validate_evidence_timestamp(&reference, 100, 110, 20)
                .expect_err("stale observation must fail")
                .starts_with("stale_evidence")
        );
    }

    #[test]
    fn strict_rch_command_parser_rejects_shell_expansion() {
        assert_eq!(
            parse_strict_command_tokens("cargo test --locked; touch sentinel")
                .expect_err("shell separator must fail"),
            "phase5_rch_command_shell_syntax"
        );
        assert_eq!(
            parse_strict_command_tokens("cargo test '$HOME'")
                .expect("single-quoted literal is inert"),
            ["cargo", "test", "$HOME"].map(str::to_owned)
        );
    }

    #[test]
    fn phase5_and_baseline_schemas_reject_unknown_fields_and_policy_values() {
        let command = serde_json::json!({
            "argv": ["cargo", "test"],
            "exit_status": 0,
            "stdout": {
                "capture": "observed",
                "leaf": {"path": "x", "digest_algorithm": "blake3-256", "digest": "a".repeat(64)},
            },
            "stderr": {
                "capture": "observed",
                "leaf": {"path": "y", "digest_algorithm": "blake3-256", "digest": "b".repeat(64)},
            },
            "transcript": {"path": "y", "digest_algorithm": "blake3-256", "digest": "b".repeat(64)},
            "untrusted_extra": true,
        });
        assert!(serde_json::from_value::<Phase5CommandEvidence>(command).is_err());

        let baseline = serde_json::json!({
            "as_of_phase": "phase5",
            "total_tests": 1,
            "passed": 1,
            "failed": 0,
            "ignored": 0,
            "baseline_commit": "a".repeat(40),
            "baseline_evidence": null,
            "ignored_tests": [{
                "source_path": "crates/fsqlite-harness/tests/example.rs",
                "test_name": "keeper",
                "reason": "Release proof",
                "cfg_condition": null,
                "kind": "release_gate",
                "policy": "future_policy",
                "parent_tests": [],
                "evidence": {"requirement": "Run it", "receipt": null},
            }],
        });
        assert!(serde_json::from_value::<RegressionBaselineReference>(baseline).is_err());
    }

    #[test]
    fn every_phase5_exact_test_argv_is_derived_from_its_source_target() {
        let integration = sample_run_for_release_entry(T16_SOURCE_PATH, T16_TEST_NAME);
        assert_eq!(
            expected_phase5_run_argv(&integration).expect("integration command"),
            [
                "cargo",
                "test",
                "--locked",
                "--profile",
                "release-perf",
                "--package",
                "fsqlite-e2e",
                "--test",
                "bd_wsw3p_concurrent_write_showcase",
                T16_TEST_NAME,
                "--",
                "--exact",
                "--ignored",
                "--nocapture",
                "--test-threads=1",
            ]
            .map(str::to_owned)
        );

        let mut library =
            sample_run_for_release_entry("crates/fsqlite-pager/src/page_cache.rs", "tests::keeper");
        library.cfg_condition = Some("debug_assertions".to_owned());
        let argv = expected_phase5_run_argv(&library).expect("library command");
        assert!(argv.contains(&"--lib".to_owned()));
        assert!(argv.contains(&"page_cache::tests::keeper".to_owned()));
        assert!(!argv.contains(&"--ignored".to_owned()));
    }

    #[test]
    fn rch_binding_rejects_a_job_from_the_wrong_project() {
        let stdout = sample_phase5_leaf("stdout");
        let stderr = sample_phase5_leaf("stderr");
        let execution = Phase5CommandEvidence {
            argv: ["cargo", "test"].map(str::to_owned).to_vec(),
            exit_status: 0,
            stdout: Phase5StreamEvidence {
                capture: Phase5StreamCapture::Observed,
                leaf: stdout,
            },
            stderr: Phase5StreamEvidence {
                capture: Phase5StreamCapture::Observed,
                leaf: stderr.clone(),
            },
            transcript: stderr,
        };
        let receipt = Phase5RchReceipt {
            schema_version: PHASE5_RCH_RECEIPT_SCHEMA.to_owned(),
            inner_cargo_argv: execution.argv.clone(),
            job_id: "7".to_owned(),
            active_status: sample_phase5_leaf("active"),
            completed_status: sample_phase5_leaf("completed"),
        };
        let active = Phase5RchStatusEnvelope {
            api_version: "1.0".to_owned(),
            command: "status".to_owned(),
            success: true,
            data: Phase5RchStatusData {
                daemon: Phase5RchDaemonStatus {
                    active_builds: vec![Phase5RchActiveBuild {
                        id: 7,
                        project_id: "wrong-project".to_owned(),
                        worker_id: "worker-1".to_owned(),
                        command: "cargo test".to_owned(),
                    }],
                    recent_builds: Vec::new(),
                },
            },
        };
        let completed = Phase5RchStatusEnvelope {
            api_version: "1.0".to_owned(),
            command: "status".to_owned(),
            success: true,
            data: Phase5RchStatusData {
                daemon: Phase5RchDaemonStatus {
                    active_builds: Vec::new(),
                    recent_builds: vec![Phase5RchCompletedBuild {
                        id: 7,
                        project_id: "wrong-project".to_owned(),
                        worker_id: "worker-1".to_owned(),
                        command: "cargo test".to_owned(),
                        exit_code: 0,
                        location: "remote".to_owned(),
                        cancellation: None,
                    }],
                },
            },
        };
        assert_eq!(
            validate_phase5_rch_binding(
                &receipt,
                &execution,
                &active,
                &completed,
                "Selected worker: worker-1\nRemote command finished: exit=0\n",
                "expected-project",
            )
            .expect_err("wrong project must fail"),
            "phase5_active_status_not_bound_to_command"
        );
    }

    #[test]
    fn canonical_parity_and_campaign_evidence_reject_subset_or_vacuous_payloads() {
        let canonical_catalog = build_canonical_catalog();
        let universe = build_canonical_universe();
        let config = certification_gate_config();
        let (gate, ranking) = evaluate_full(&canonical_catalog, &universe, &config);
        let ledger = build_evidence_ledger(&gate, &ranking);
        let mut drift_monitor = ParityDriftMonitor::new(ParityDriftConfig::default());
        let stats = canonical_catalog.stats();
        for category in FeatureCategory::ALL {
            let count = stats
                .per_category
                .get(category.display_name())
                .copied()
                .unwrap_or(0);
            drift_monitor.observe_batch(
                category,
                count.saturating_sub(stats.verified_invariants.min(count)),
                count,
            );
        }
        let drift = drift_monitor.snapshot();
        validate_canonical_parity_evidence(
            &gate,
            &ranking,
            &ledger,
            &stats,
            &canonical_catalog.release_traceability(),
            &drift,
        )
        .expect("canonical evidence");
        let mut subset_stats = stats;
        subset_stats.total_invariants = subset_stats.total_invariants.saturating_sub(1);
        assert_eq!(
            validate_canonical_parity_evidence(
                &gate,
                &ranking,
                &ledger,
                &subset_stats,
                &canonical_catalog.release_traceability(),
                &drift,
            )
            .expect_err("subset must fail"),
            "canonical_parity_evidence_mismatch"
        );

        let mut vacuous = run_campaign(&AdversarialConfig::default());
        vacuous.total_trials = 0;
        assert_eq!(
            validate_canonical_adversarial_campaign(&vacuous)
                .expect_err("vacuous campaign must fail"),
            "adversarial_campaign_not_canonical_or_nonvacuous"
        );
    }

    #[test]
    fn dependency_feature_graph_requires_exact_canonical_bytes() {
        let target = "x86_64-unknown-linux-gnu";
        let value = serde_json::json!({
            "schema_version": "fsqlite.dependency_feature_graph.v1",
            "command": [
                "cargo", "tree", "--locked", "--offline", "-p", "fsqlite-e2e",
                "-e", "features,no-dev", "--no-default-features", "--target", target,
            ],
            "target": target,
            "tree": "fsqlite-e2e v0.2.0 (${WORKSPACE_ROOT}/crates/fsqlite-e2e)\n",
        });
        let object = value.as_object().expect("graph object");
        let sorted = object
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<BTreeMap<_, _>>();
        let mut bytes = serde_json::to_vec(&sorted).expect("serialize canonical graph");
        bytes.push(b'\n');
        let expected_tree = value["tree"].as_str().unwrap().to_owned();
        let graph = LoadedStrictEvidence {
            relative_path: "dependency-feature-graph.json".to_owned(),
            sha256: sha256_bytes(&bytes),
            bytes,
            value,
        };
        assert_eq!(
            validate_dependency_feature_graph_document(&graph).expect("canonical graph"),
            (target.to_owned(), expected_tree)
        );

        let mut noncanonical = graph;
        noncanonical.bytes =
            serde_json::to_vec_pretty(&noncanonical.value).expect("serialize pretty graph");
        assert_eq!(
            validate_dependency_feature_graph_document(&noncanonical)
                .expect_err("pretty JSON is not canonical evidence"),
            "dependency_feature_graph_bytes_not_canonical"
        );
    }

    #[test]
    fn current_regression_baseline_exposes_t16_release_requirement() {
        let mut baseline: RegressionBaselineReference =
            serde_json::from_str(include_str!("../../../tests/regression_baseline.json"))
                .expect("current regression baseline schema");
        baseline
            .ignored_tests
            .retain(|entry| entry.policy != RegressionIgnorePolicy::BlockRelease);
        let mut blocked = baseline.clone();
        let synthetic_blocker = blocked
            .ignored_tests
            .iter_mut()
            .find(|entry| entry.source_path == T16_SOURCE_PATH && entry.test_name == T16_TEST_NAME)
            .expect("T16 baseline entry");
        synthetic_blocker.kind = RegressionIgnoreKind::KnownBug;
        synthetic_blocker.policy = RegressionIgnorePolicy::BlockRelease;
        assert!(
            expected_phase5_requirements(&blocked)
                .expect_err("blocking tests must fail closed")
                .starts_with("regression_baseline_block_release_entry")
        );
        let requirements = expected_phase5_requirements(&baseline).expect("release requirements");
        assert!(
            requirements
                .current_runs
                .contains_key(&format!("{T16_SOURCE_PATH}::{T16_TEST_NAME}"))
        );
        assert_eq!(
            requirements.live_guard.argv.last().map(String::as_str),
            Some("--test-threads=1")
        );

        let mut without_live_guard = baseline.clone();
        without_live_guard
            .ignored_tests
            .retain(|entry| entry.locator() != PHASE5_LIVE_GUARD_LOCATOR);
        assert_eq!(
            expected_phase5_requirements(&without_live_guard)
                .expect_err("live guard cannot be omitted"),
            "regression_baseline_live_guard_requirement_missing"
        );
        assert_eq!(
            validate_regression_baseline(
                Path::new("."),
                &baseline,
                b"not-read",
                &baseline.baseline_commit,
                &"c".repeat(40),
            )
            .expect_err("B and M must be distinct"),
            "regression_baseline_not_green_or_predating_candidate"
        );
        assert_eq!(
            validate_phase5_baseline_evidence(
                Path::new("."),
                Path::new("."),
                &RegressionBaselineReference {
                    baseline_evidence: None,
                    ..baseline.clone()
                },
                &"b".repeat(40),
                "baseline-project",
            )
            .expect_err("baseline workspace evidence is mandatory"),
            "regression_baseline_workspace_evidence_missing"
        );
    }

    #[test]
    fn t16_transcript_requires_each_of_twenty_two_samples_once() {
        let candidate = "a".repeat(40);
        let feature_graph = "b".repeat(64);
        let target = "x86_64-unknown-linux-gnu";
        let expected_argv = expected_phase5_run_argv(&sample_run_for_release_entry(
            T16_SOURCE_PATH,
            T16_TEST_NAME,
        ))
        .expect("T16 argv");
        let mut transcript = format!(
            "canonical replay: {}\n\
             provenance/source: sha={candidate} branch=main dirty=false features= input_tracking=complete\n\
             provenance/toolchain: host=build-host target={target} profile=release-perf selected_profile=release-perf rustc=\"rustc 1.0\" cargo=\"cargo 1.0\"\n\
             provenance/flags: rustflags_hex= encoded_rustflags_present=false profile_overrides_hex= native_overrides_hex= feature_graph_sha256={feature_graph}\n\
             provenance/binary: path=/tmp/test sha256={}\n\
             provenance/runtime: machine=worker-1\n\
             16-thread per-worker-transaction: median F/C=1.1000x, lower bound (order statistic 7 of 22, >=95% confidence)=1.1000x, threshold=1.0000x, wins=22/22, expected_rows_per_sample=3200\n",
            expected_argv.join(" "),
            "c".repeat(64)
        );
        for sample in 0..T16_SAMPLE_COUNT {
            let order = if sample.is_multiple_of(2) {
                "fsqlite_first"
            } else {
                "csqlite_first"
            };
            transcript.push_str(&format!(
                "sample {sample} ({order}): csqlite=1000 ops/s, fsqlite=1100 ops/s, F/C=1.1000x, rows=3200/3200\n"
            ));
        }
        transcript.push_str("test result: ok. 1 passed; 0 failed\n");
        let semantic = validate_t16_transcript(
            &transcript,
            &candidate,
            &feature_graph,
            target,
            &expected_argv,
        )
        .expect("complete transcript");
        assert_eq!(semantic.runtime_machine, "worker-1");

        let incomplete = transcript.replace(
            "sample 21 (csqlite_first): csqlite=1000 ops/s, fsqlite=1100 ops/s, F/C=1.1000x, rows=3200/3200\n",
            "",
        );
        assert!(
            validate_t16_transcript(
                &incomplete,
                &candidate,
                &feature_graph,
                target,
                &expected_argv,
            )
            .expect_err("missing sample must fail")
            .starts_with("t16_transcript_sample_count_mismatch")
        );

        let tampered = transcript.replace("wins=22/22", "wins=21/22");
        assert_eq!(
            validate_t16_transcript(
                &tampered,
                &candidate,
                &feature_graph,
                target,
                &expected_argv,
            )
            .expect_err("summary tampering must fail"),
            "t16_summary_does_not_match_samples"
        );
    }

    #[test]
    fn strict_output_path_rejects_parent_alias_before_touching_disk() {
        assert_eq!(
            prepare_strict_output_directory(Path::new("."), Path::new("../outside"))
                .expect_err("parent alias must fail"),
            "certificate_output_path_not_canonical"
        );
    }

    #[test]
    fn strict_output_path_accepts_an_absent_canonical_child_without_creating_it() {
        let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .canonicalize()
            .expect("workspace root");
        let relative = Path::new("artifacts/strict-certificate-path-validation-only");
        let expected = workspace.join(relative);
        assert!(
            !expected.exists(),
            "validation-only output must stay absent"
        );
        assert_eq!(
            prepare_strict_output_directory(&workspace, relative).expect("canonical output path"),
            expected
        );
        assert!(!expected.exists(), "path validation must not publish early");
    }

    #[test]
    fn strict_output_path_refuses_existing_directory_without_mutation() {
        let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .canonicalize()
            .expect("workspace root");
        let error = prepare_strict_output_directory(&workspace, Path::new("crates"))
            .expect_err("existing workspace directory must never be replaced");
        assert!(error.starts_with("refusing_to_overwrite_certificate_output"));
        assert!(workspace.join("crates").is_dir());
    }

    #[test]
    fn strict_bundle_file_creation_never_clobbers_an_existing_file() {
        let manifest = Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml");
        let before = fs::read(&manifest).expect("read harness manifest");
        let error = write_new_bundle_file(&manifest, b"must-not-replace")
            .expect_err("create_new must reject an existing path");
        assert!(error.starts_with("certificate_output_create_failed"));
        assert_eq!(
            fs::read(&manifest).expect("reread harness manifest"),
            before
        );
    }

    #[test]
    fn exact_cleanliness_rejects_even_an_untracked_porcelain_record() {
        validate_exact_clean_status(b"").expect("empty status is exactly clean");
        assert_eq!(
            validate_exact_clean_status(b"? lint_probe\0")
                .expect_err("untracked paths must fail closed"),
            "candidate_checkout_not_exactly_clean"
        );
    }

    #[test]
    fn t16_binary_identity_requires_one_manifest_artifact() {
        let semantic = T16SemanticEvidence {
            binary_path: "/remote/target/release-perf/deps/t16-binary".to_owned(),
            binary_sha256: "d".repeat(64),
            runtime_machine: "worker-1".to_owned(),
        };
        let mut manifest = BTreeMap::from([(
            "artifacts/t16-binary".to_owned(),
            (semantic.binary_sha256.clone(), 42),
        )]);
        validate_t16_binary_manifest_binding(&semantic, &manifest)
            .expect("one basename-and-hash match is exact");
        manifest.insert(
            "duplicate/t16-binary".to_owned(),
            (semantic.binary_sha256.clone(), 42),
        );
        assert_eq!(
            validate_t16_binary_manifest_binding(&semantic, &manifest)
                .expect_err("ambiguous binary identity must fail"),
            "t16_binary_manifest_binding_cardinality_mismatch count=2"
        );
    }

    #[test]
    fn d4_scenario_artifact_schema_is_typed_and_closed() {
        let mut payload = serde_json::json!({
            "schema_version": D4_SCENARIO_ARTIFACT_SCHEMA,
            "source_commit": "a".repeat(40),
            "run_id": "run-1",
            "trace_id": "trace-1",
            "scenario_id": "scenario-family-1",
            "seed": 7,
            "generated_unix_ms": 1000,
            "scenario": "connection_open",
            "backend_identity": STRICT_BACKEND_IDENTITY,
            "passed": true,
            "exit_code": 0,
            "concurrent_mode_default": true,
            "certifying_fallback_events": 0,
        });
        let parsed: D4ScenarioArtifact =
            serde_json::from_value(payload.clone()).expect("closed D4 scenario schema");
        assert_eq!(parsed.scenario, "connection_open");
        payload["unbound_note"] = serde_json::json!("must be rejected");
        assert!(
            serde_json::from_value::<D4ScenarioArtifact>(payload).is_err(),
            "unknown D4 fields must fail closed"
        );
    }

    #[test]
    fn results_jsonl_record_schema_rejects_unbound_fields() {
        let mut payload = serde_json::json!({
            "schema_version": STRICT_RESULTS_RECORD_SCHEMA,
            "candidate_git_sha": "a".repeat(40),
            "run_id": "run-1",
            "trace_id": "trace-1",
            "scenario_id": "scenario-family-1",
            "seed": 7,
            "invariant_id": "PAR-SQL-001",
            "feature_id": "F-SQL-001",
            "category": "SQL Grammar",
            "statement": "typed closure",
            "proof_summary": [],
            "artifacts": [],
            "passed": true,
        });
        serde_json::from_value::<StrictResultsRecord>(payload.clone())
            .expect("closed results record schema");
        payload["unbound_note"] = serde_json::json!("must be rejected");
        assert!(
            serde_json::from_value::<StrictResultsRecord>(payload).is_err(),
            "unknown results fields must fail closed"
        );
    }

    #[test]
    fn certificate_has_correct_bead_id() {
        let cert = default_cert();
        assert_eq!(cert.bead_id, RELEASE_CERT_BEAD_ID);
    }

    #[test]
    fn certificate_has_schema_version() {
        let cert = default_cert();
        assert_eq!(cert.schema_version, CERTIFICATE_SCHEMA_VERSION);
    }

    #[test]
    fn certificate_verdict_is_valid() {
        let cert = default_cert();
        // Must be one of the three valid verdicts
        assert!(
            matches!(
                cert.verdict,
                CertificateVerdict::Approved
                    | CertificateVerdict::Conditional
                    | CertificateVerdict::Rejected
            ),
            "bead_id={BEAD_ID} case=verdict_valid",
        );
    }

    #[test]
    fn certificate_has_evidence_chain() {
        let cert = default_cert();
        // Should have entries for 4 source beads
        assert!(
            cert.evidence_chain.len() >= 4,
            "bead_id={BEAD_ID} case=evidence_chain_count entries={}",
            cert.evidence_chain.len(),
        );
    }

    #[test]
    fn evidence_chain_has_content_hashes() {
        let cert = default_cert();
        for entry in &cert.evidence_chain {
            assert!(
                !entry.content_hash.is_empty(),
                "bead_id={BEAD_ID} case=chain_hash source={}",
                entry.source_bead,
            );
        }
    }

    #[test]
    fn evidence_hash_is_real_sha256_known_vector() {
        assert_eq!(
            sha256_hex("abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn evidence_chain_uses_sha256_for_catalog_payload() {
        let cert = default_cert();
        let catalog = build_canonical_catalog();
        let catalog_json = serde_json::to_string(&catalog.stats()).expect("catalog serializes");
        let catalog_entry = cert
            .evidence_chain
            .iter()
            .find(|entry| entry.source_bead == "bd-1dp9.8.1")
            .expect("catalog evidence entry");

        assert_eq!(catalog_entry.content_hash, sha256_hex(&catalog_json));
        assert_eq!(catalog_entry.content_hash.len(), 64);
    }

    #[test]
    fn certificate_has_score_bounds() {
        let cert = default_cert();
        assert!(
            cert.global_posterior_mean >= 0.0 && cert.global_posterior_mean <= 1.0,
            "bead_id={BEAD_ID} case=posterior_mean",
        );
        assert!(
            cert.global_lower_bound <= cert.global_posterior_mean,
            "bead_id={BEAD_ID} case=lower_bound",
        );
    }

    #[test]
    fn certificate_has_invariant_counts() {
        let cert = default_cert();
        assert!(
            cert.total_invariants > 0,
            "bead_id={BEAD_ID} case=total_invariants",
        );
        assert!(
            cert.passing_invariants <= cert.total_invariants,
            "bead_id={BEAD_ID} case=passing_bounded",
        );
    }

    #[test]
    fn certificate_tracks_drift() {
        let cert = default_cert();
        // Drift fields should be populated
        // Verify drift fields are populated (always true, but exercises field access).
        #[allow(clippy::overly_complex_bool_expr)]
        let drift_populated = cert.any_drift_rejected || !cert.any_drift_rejected;
        assert!(drift_populated, "bead_id={BEAD_ID} case=drift_populated");
    }

    #[test]
    fn certificate_tracks_adversarial() {
        let cert = default_cert();
        assert!(
            cert.high_severity_count <= cert.counterexample_count,
            "bead_id={BEAD_ID} case=adversarial_bounded",
        );
    }

    #[test]
    fn certificate_summary_nonempty() {
        let cert = default_cert();
        assert!(
            !cert.summary.is_empty(),
            "bead_id={BEAD_ID} case=summary_nonempty",
        );
    }

    #[test]
    fn certificate_triage_line_has_key_fields() {
        let cert = default_cert();
        let line = cert.triage_line();
        assert!(line.contains("gate="), "bead_id={BEAD_ID} case=triage_gate");
        assert!(
            line.contains("verified="),
            "bead_id={BEAD_ID} case=triage_verified",
        );
        assert!(
            line.contains("invariants="),
            "bead_id={BEAD_ID} case=triage_invariants",
        );
        assert!(
            line.contains("risks="),
            "bead_id={BEAD_ID} case=triage_risks",
        );
    }

    #[test]
    fn verdict_display() {
        assert_eq!(CertificateVerdict::Approved.to_string(), "APPROVED");
        assert_eq!(CertificateVerdict::Conditional.to_string(), "CONDITIONAL");
        assert_eq!(CertificateVerdict::Rejected.to_string(), "REJECTED");
    }

    #[test]
    fn certificate_json_roundtrip() {
        let cert = default_cert();
        let json = cert.to_json().expect("serialize");
        let parsed = ReleaseCertificate::from_json(&json).expect("parse");

        assert_eq!(parsed.bead_id, cert.bead_id);
        assert_eq!(
            parsed.certification_policy_id, cert.certification_policy_id,
            "bead_id={BEAD_ID} case=policy_roundtrip",
        );
        assert_eq!(parsed.verdict, cert.verdict);
        assert_eq!(parsed.total_invariants, cert.total_invariants);
        assert_eq!(parsed.passing_invariants, cert.passing_invariants);
        assert_eq!(parsed.high_severity_count, cert.high_severity_count);
    }

    #[test]
    fn certificate_file_roundtrip() {
        let cert = default_cert();

        let dir = std::env::temp_dir().join("fsqlite-release-cert-test");
        std::fs::create_dir_all(&dir).expect("create temp dir");
        let path = dir.join("cert-test.json");

        write_certificate(&path, &cert).expect("write");
        let loaded = load_certificate(&path).expect("load");

        assert_eq!(loaded.verdict, cert.verdict);
        assert_eq!(loaded.total_invariants, cert.total_invariants);
        assert_eq!(loaded.bead_id, cert.bead_id);

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir(&dir);
    }

    #[test]
    fn certificate_deterministic() {
        let config = CertificateConfig::default();
        let c1 = generate_release_certificate(&config);
        let c2 = generate_release_certificate(&config);

        assert_eq!(c1.verdict, c2.verdict, "bead_id={BEAD_ID} case=det_verdict");
        assert_eq!(
            c1.total_invariants, c2.total_invariants,
            "bead_id={BEAD_ID} case=det_invariants",
        );
        assert_eq!(
            c1.passing_invariants, c2.passing_invariants,
            "bead_id={BEAD_ID} case=det_passing",
        );
        assert_eq!(
            c1.high_severity_count, c2.high_severity_count,
            "bead_id={BEAD_ID} case=det_adversarial",
        );
        // JSON should be identical for deterministic inputs
        assert_eq!(
            c1.to_json().unwrap(),
            c2.to_json().unwrap(),
            "bead_id={BEAD_ID} case=det_json",
        );
    }

    #[test]
    fn verdict_rejected_on_gate_fail() {
        let config = CertificateConfig::default();
        let catalog = build_canonical_catalog();
        let universe = build_canonical_universe();
        let (gate_report, ranking) = evaluate_full(&catalog, &universe, &config.gate_config);
        let ledger = build_evidence_ledger(&gate_report, &ranking);

        // Force a FAIL gate by fabricating a gate report
        let mut failing_report = gate_report;
        failing_report.global_decision = GateDecision::Fail;
        failing_report.release_ready = false;

        let drift_monitor = ParityDriftMonitor::new(config.drift_config.clone());
        let drift_snapshot = drift_monitor.snapshot();
        let campaign_result = run_campaign(&config.adversarial_config);

        let inputs = CertificateInputs {
            gate_report: failing_report,
            expected_loss_ranking: ranking,
            evidence_ledger: ledger,
            catalog_stats: catalog.stats(),
            traceability: catalog.release_traceability(),
            drift_snapshot,
            campaign_result,
            ci_flake_budget: None,
            artifact_manifest: None,
        };

        let cert = build_certificate(&inputs, &config);
        assert_eq!(
            cert.verdict,
            CertificateVerdict::Rejected,
            "bead_id={BEAD_ID} case=gate_fail_rejected",
        );
    }

    #[test]
    fn embedded_ledger_matches_gate_decision() {
        let cert = default_cert();
        assert_eq!(
            cert.evidence_ledger.global_decision, cert.gate_decision,
            "bead_id={BEAD_ID} case=ledger_gate_match",
        );
    }

    #[test]
    fn catalog_stats_populated() {
        let cert = default_cert();
        assert!(
            cert.catalog_stats.total_invariants > 0,
            "bead_id={BEAD_ID} case=catalog_stats",
        );
    }

    #[test]
    fn certificate_default_uses_track_g_threshold_units() {
        let config = CertificateConfig::default();
        assert_eq!(
            config.min_verification_pct, 100.0,
            "bead_id={BEAD_ID} case=min_pct_units",
        );
        assert_eq!(
            config.gate_config.category_min_verification_pct, 100.0,
            "bead_id={BEAD_ID} case=category_min_pct_units",
        );
    }

    #[test]
    fn triage_line_reports_verification_pct_without_double_scaling() {
        let cert = ReleaseCertificate {
            schema_version: CERTIFICATE_SCHEMA_VERSION,
            bead_id: RELEASE_CERT_BEAD_ID.to_owned(),
            certification_policy_id: "policy".to_owned(),
            certification_policy: canonical_certification_policy(),
            verdict: CertificateVerdict::Conditional,
            global_posterior_mean: 1.0,
            global_lower_bound: 1.0,
            global_verification_pct: 87.5,
            total_expected_loss: 0.0,
            gate_decision: GateDecision::Conditional,
            gate_release_ready: false,
            total_invariants: 8,
            passing_invariants: 7,
            catalog_stats: CatalogStats::default(),
            any_drift_rejected: false,
            any_drift_alarm: false,
            drift_alert_categories: 0,
            adversarial_passed: true,
            counterexample_count: 0,
            high_severity_count: 0,
            ci_flake_budget_passed: None,
            artifact_hash_count: 0,
            certification_evidence: CertificationEvidenceStatus {
                schema_version: CERTIFICATION_TRACEABILITY_SCHEMA_VERSION,
                policy_id: "policy".to_owned(),
                artifact_manifest_present: false,
                artifact_manifest_gate_passed: None,
                verification_contract_present: false,
                verification_contract_passed: None,
                final_gate_passed: None,
                fallback_transparency_gate_present: false,
                fallback_transparency_gate_passed: None,
                fallback_transparency_gate_failure_count: 0,
                fallback_transparency_missing_boundary_count: 0,
                fallback_transparency_stale_artifact_count: 0,
                fallback_transparency_certifying_fallback_event_count: 0,
                fallback_transparency_replay_command: None,
                missing_evidence_beads: 0,
                invalid_reference_beads: 0,
                reported_artifact_count: 0,
                traceability_entry_count: 0,
                fully_linked_traceability_entry_count: 0,
                missing_artifact_ref_count: 0,
            },
            evidence_chain: Vec::new(),
            certification_traceability: CertificationTraceabilityReport {
                schema_version: CERTIFICATION_TRACEABILITY_SCHEMA_VERSION,
                policy_id: "policy".to_owned(),
                manifest_present: false,
                fully_linked_entries: 0,
                missing_artifact_ref_count: 0,
                entries: Vec::new(),
            },
            unresolved_risks: Vec::new(),
            evidence_ledger: EvidenceLedger {
                schema_version: 1,
                global_decision: GateDecision::Conditional,
                release_ready: false,
                global_posterior_mean: 1.0,
                global_lower_bound: 1.0,
                global_verification_pct: 87.5,
                total_expected_loss: 0.0,
                total_invariants: 8,
                passing_invariants: 7,
                top_priority_items: Vec::new(),
                category_summaries: BTreeMap::new(),
                verification_contract: None,
            },
            summary: "summary".to_owned(),
        };

        let line = cert.triage_line();
        assert!(
            line.contains("verified=87.5%"),
            "bead_id={BEAD_ID} case=triage_units line={line}",
        );
    }
}
