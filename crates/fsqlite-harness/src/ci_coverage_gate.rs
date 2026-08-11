//! CI coverage gate workflow and threshold checker (bd-mblr.3.1.1).
//!
//! Computes coverage metrics from the unit matrix and no-mock evidence map,
//! then enforces approved SLO thresholds with actionable failure output.
//!
//! # Architecture
//!
//! The gate pulls data from:
//! 1. [`UnitMatrix`] — per-category test counts, fill percentages, invariant counts
//! 2. [`NoMockEvidenceMap`] — real-component evidence for critical-path invariants
//! 3. [`CoverageThresholds`] — SLO thresholds from bd-mblr.1.4.1
//!
//! It produces a [`CoverageGateReport`] with:
//! - Per-category pass/fail results
//! - Global coverage verdict
//! - Actionable annotations for every failing threshold
//! - Machine-readable JSON and human-readable markdown output
//!
//! # Upstream Dependencies
//!
//! - [`unit_matrix`](crate::unit_matrix) (bd-1dp9.7.1)
//! - [`no_mock_evidence`](crate::no_mock_evidence) (bd-mblr.6.7)
//! - [`parity_taxonomy`](crate::parity_taxonomy) (bd-1dp9.1.1)
//! - SLO policy from bd-mblr.1.4.1
//!
//! # Downstream Consumers
//!
//! - **bd-mblr.7.9.2**: Lane selection engine uses gate results
//! - **bd-mblr.3.3**: Flake budget and quarantine workflow
//!
//! The same module owns the Turso adaptation cross-phase scorecard because it
//! extends this gate's existing coverage and CI policy rather than introducing
//! a second campaign runner or artifact format.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fmt::Write as FmtWrite;

use serde::{Deserialize, Serialize};

use crate::no_mock_evidence::{NoMockEvidenceMap, build_evidence_map};
use crate::parity_taxonomy::FeatureCategory;
use crate::unit_matrix::{UnitMatrix, build_canonical_matrix};

/// Bead identifier for log correlation.
#[allow(dead_code)]
const BEAD_ID: &str = "bd-mblr.3.1.1";

/// Schema version for report compatibility.
pub const COVERAGE_GATE_SCHEMA_VERSION: &str = "1.0.0";

/// Schema emitted by the Turso campaign promotion scorecard.
pub const TURSO_CAMPAIGN_SCORECARD_SCHEMA_VERSION: &str = "2.0.0";

const TURSO_NATIVE_LANES: [&str; 6] = [
    "bd-turso-test-adaptation-zu081.5",
    "bd-turso-test-adaptation-zu081.6",
    "bd-turso-test-adaptation-zu081.8",
    "bd-turso-test-adaptation-zu081.9",
    "bd-turso-test-adaptation-zu081.19",
    "bd-turso-test-adaptation-zu081.20",
];

const TURSO_OPTIONAL_DECISIONS: [&str; 7] = [
    "bd-turso-test-adaptation-zu081.10",
    "bd-turso-test-adaptation-zu081.11",
    "bd-turso-test-adaptation-zu081.12",
    "bd-turso-test-adaptation-zu081.13",
    "bd-turso-test-adaptation-zu081.14",
    "bd-turso-test-adaptation-zu081.15",
    "bd-turso-test-adaptation-zu081.16",
];

const MAX_CAMPAIGN_SUMMARY_LANES: usize = 16;
const MAX_CAMPAIGN_SUMMARY_LANE_ID_BYTES: usize = 96;
const MAX_PRESUBMIT_SEEDS_PER_PROFILE: u64 = 100;
const MAX_OPTIONAL_RATIONALE_BYTES: usize = 1_024;

/// Resource tier represented by a campaign receipt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CampaignTier {
    /// Fixed-seed change gate, bounded to ten minutes.
    Presubmit,
    /// Scheduled expansion, bounded to one hour per lane.
    Nightly,
    /// Explicitly budgeted operator or release campaign.
    Manual,
}

impl CampaignTier {
    const fn maximum_budget_seconds(self) -> Option<u64> {
        match self {
            Self::Presubmit => Some(600),
            Self::Nightly => Some(3_600),
            Self::Manual => None,
        }
    }

    const fn minimum_retention_days(self) -> u32 {
        match self {
            Self::Presubmit => 7,
            Self::Nightly => 30,
            Self::Manual => 90,
        }
    }
}

/// Highest tier whose retained receipts are required by this scorecard.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CampaignPromotionStage {
    /// Require only presubmit evidence.
    Presubmit,
    /// Require presubmit and nightly evidence.
    Nightly,
    /// Require presubmit, nightly, and manual/release evidence.
    Release,
}

impl CampaignPromotionStage {
    const fn required_tiers(self) -> &'static [CampaignTier] {
        match self {
            Self::Presubmit => &[CampaignTier::Presubmit],
            Self::Nightly => &[CampaignTier::Presubmit, CampaignTier::Nightly],
            Self::Release => &[
                CampaignTier::Presubmit,
                CampaignTier::Nightly,
                CampaignTier::Manual,
            ],
        }
    }
}

/// Portfolio decision for an external or optional campaign.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CampaignDisposition {
    /// The lane is admitted and therefore gates CI.
    Adopted,
    /// The lane is deferred behind explicit re-entry conditions.
    Deferred,
    /// The lane is rejected for this campaign.
    Rejected,
}

/// Completion state reported by one shard.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CampaignRunStatus {
    /// The configured seed range completed.
    Completed,
    /// The run was cancelled.
    Cancelled,
    /// The configured budget was consumed before exploration completed.
    BudgetExhausted,
    /// The runner stopped with unexplored work for another reason.
    IncompleteExploration,
}

/// Scheduling claim made by a campaign receipt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HistoryAccounting {
    /// Execution was controlled and is reproducible from the schedule artifact.
    Deterministic,
    /// The history is useful evidence, but OS scheduling was not controlled.
    ObservationOnly,
    /// The lane does not produce transaction histories.
    NotApplicable,
}

/// Required outcome accounting for a campaign shard.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CampaignOutcomeCounts {
    pub generated: u64,
    pub executed: u64,
    pub unsupported: u64,
    pub invalid: u64,
    pub skipped: u64,
    pub timed_out: u64,
    pub mismatched: u64,
    pub crashed: u64,
    pub reduced: u64,
    pub promoted: u64,
}

impl CampaignOutcomeCounts {
    fn terminal_total(self) -> Option<u64> {
        self.executed
            .checked_add(self.unsupported)?
            .checked_add(self.invalid)?
            .checked_add(self.skipped)?
            .checked_add(self.timed_out)
    }

    fn checked_add(self, other: Self) -> Option<Self> {
        Some(Self {
            generated: self.generated.checked_add(other.generated)?,
            executed: self.executed.checked_add(other.executed)?,
            unsupported: self.unsupported.checked_add(other.unsupported)?,
            invalid: self.invalid.checked_add(other.invalid)?,
            skipped: self.skipped.checked_add(other.skipped)?,
            timed_out: self.timed_out.checked_add(other.timed_out)?,
            mismatched: self.mismatched.checked_add(other.mismatched)?,
            crashed: self.crashed.checked_add(other.crashed)?,
            reduced: self.reduced.checked_add(other.reduced)?,
            promoted: self.promoted.checked_add(other.promoted)?,
        })
    }
}

/// One half-open deterministic seed interval.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CampaignSeedShard {
    pub index: u32,
    pub count: u32,
    pub start_seed: u64,
    pub end_seed_exclusive: u64,
}

/// Coverage dimensions represented by a shard artifact.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CampaignCoverageDimensions {
    pub feature_ids: Vec<String>,
    pub constructs: Vec<String>,
    pub execution_lanes: Vec<String>,
    pub fault_kinds: Vec<String>,
    pub concurrency_workloads: Vec<String>,
    pub reducer_families: Vec<String>,
}

/// Retained, hash-addressed evidence produced by a run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CampaignArtifactEvidence {
    pub path: String,
    pub sha256: String,
    pub retention_days: u32,
}

/// Evidence receipt for one lane/tier/shard.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CampaignRunEvidence {
    pub lane_id: String,
    pub tier: CampaignTier,
    pub shard: CampaignSeedShard,
    pub expected_seed_count: u64,
    pub budget_seconds: u64,
    pub elapsed_seconds: u64,
    pub status: CampaignRunStatus,
    pub outcomes: CampaignOutcomeCounts,
    pub coverage: CampaignCoverageDimensions,
    pub history_accounting: HistoryAccounting,
    pub required_lane_evidence_verified: bool,
    pub public_replay_verified: bool,
    pub replay_command: String,
    pub artifacts: Vec<CampaignArtifactEvidence>,
}

/// Decision record for one optional/external child.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OptionalCampaignDecision {
    pub bead_id: String,
    pub disposition: CampaignDisposition,
    pub admitted: bool,
    pub rationale: String,
}

/// Repository-wide invariant represented by a retained command receipt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CampaignGlobalGate {
    CanonicalContractDriftGuard,
    ConcurrentWriterDefaults,
    NoTokioDependency,
    TargetAccounting,
    DocumentationProvenance,
    WorkspaceFormat,
    WorkspaceCheck,
    WorkspaceClippy,
    WorkspaceTests,
}

const CAMPAIGN_GLOBAL_GATES: [CampaignGlobalGate; 9] = [
    CampaignGlobalGate::CanonicalContractDriftGuard,
    CampaignGlobalGate::ConcurrentWriterDefaults,
    CampaignGlobalGate::NoTokioDependency,
    CampaignGlobalGate::TargetAccounting,
    CampaignGlobalGate::DocumentationProvenance,
    CampaignGlobalGate::WorkspaceFormat,
    CampaignGlobalGate::WorkspaceCheck,
    CampaignGlobalGate::WorkspaceClippy,
    CampaignGlobalGate::WorkspaceTests,
];

/// Command and artifact proving one repository-wide invariant.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CampaignGlobalGateReceipt {
    pub gate: CampaignGlobalGate,
    pub passed: bool,
    pub command: String,
    pub artifact: CampaignArtifactEvidence,
}

/// Baseline used to reject silent skip/unsupported growth.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CampaignDriftControl {
    pub baseline_unsupported: u64,
    pub baseline_skipped: u64,
    pub linked_contract_decision: Option<String>,
}

/// Complete input consumed by the Turso promotion gate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TursoCampaignGateInput {
    pub promotion_stage: CampaignPromotionStage,
    pub workflow_id: String,
    pub run_id: String,
    pub build_id: String,
    pub engine_sha: String,
    pub engine_dirty: bool,
    pub contract_hash: String,
    pub profile_hash: String,
    pub global_gate_receipts: Vec<CampaignGlobalGateReceipt>,
    pub drift: CampaignDriftControl,
    pub optional_decisions: Vec<OptionalCampaignDecision>,
    pub runs: Vec<CampaignRunEvidence>,
}

impl TursoCampaignGateInput {
    /// Deserialize a fail-closed campaign input.
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }
}

/// Final promotion decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CampaignPromotionOutcome {
    Promote,
    Hold,
}

/// One actionable campaign-gate failure.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CampaignGateDiagnostic {
    pub code: String,
    pub lane_id: Option<String>,
    pub detail: String,
}

/// Bounded per-lane accounting retained in the scorecard.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CampaignLaneSummary {
    pub lane_id: String,
    pub run_count: usize,
    pub outcomes: CampaignOutcomeCounts,
    pub deterministic_history_runs: usize,
    pub observation_only_history_runs: usize,
}

/// Machine-readable promotion scorecard.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TursoCampaignScorecard {
    pub schema_version: String,
    pub promotion_stage: CampaignPromotionStage,
    pub outcome: CampaignPromotionOutcome,
    pub workflow_id: String,
    pub run_id: String,
    pub build_id: String,
    pub engine_sha: String,
    pub engine_dirty: bool,
    pub contract_hash: String,
    pub profile_hash: String,
    pub global_gate_receipts: Vec<CampaignGlobalGateReceipt>,
    pub drift: CampaignDriftControl,
    pub optional_decisions: Vec<OptionalCampaignDecision>,
    pub runs: Vec<CampaignRunEvidence>,
    pub totals: CampaignOutcomeCounts,
    pub lane_summaries: Vec<CampaignLaneSummary>,
    pub diagnostics: Vec<CampaignGateDiagnostic>,
}

impl TursoCampaignScorecard {
    /// Serialize the complete scorecard artifact.
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }

    /// Deserialize a scorecard artifact.
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }

    /// Render one bounded line per lane plus one final outcome line.
    #[must_use]
    pub fn render_bounded_summary(&self) -> String {
        let mut output = String::new();
        for lane in self.lane_summaries.iter().take(MAX_CAMPAIGN_SUMMARY_LANES) {
            let lane_errors = self
                .diagnostics
                .iter()
                .filter(|diagnostic| diagnostic.lane_id.as_deref() == Some(lane.lane_id.as_str()))
                .count();
            let lane_id = bounded_summary_field(&lane.lane_id, MAX_CAMPAIGN_SUMMARY_LANE_ID_BYTES);
            let _ = writeln!(
                output,
                "lane={} runs={} generated={} executed={} errors={lane_errors}",
                lane_id, lane.run_count, lane.outcomes.generated, lane.outcomes.executed
            );
        }
        if self.lane_summaries.len() > MAX_CAMPAIGN_SUMMARY_LANES {
            let _ = writeln!(
                output,
                "lanes_omitted={}",
                self.lane_summaries.len() - MAX_CAMPAIGN_SUMMARY_LANES
            );
        }
        let global_errors = self
            .diagnostics
            .iter()
            .filter(|diagnostic| diagnostic.lane_id.is_none())
            .count();
        let error_codes = self
            .diagnostics
            .iter()
            .map(|diagnostic| diagnostic.code.as_str())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>()
            .join(",");
        let _ = writeln!(
            output,
            "campaign={:?} stage={:?} global_errors={global_errors} error_codes={error_codes}",
            self.outcome, self.promotion_stage,
        );
        output
    }
}

fn campaign_diagnostic(
    diagnostics: &mut Vec<CampaignGateDiagnostic>,
    code: &str,
    lane_id: Option<&str>,
    detail: impl Into<String>,
) {
    diagnostics.push(CampaignGateDiagnostic {
        code: code.to_owned(),
        lane_id: lane_id.map(str::to_owned),
        detail: detail.into(),
    });
}

fn is_nonzero_lower_hex(value: &str, expected_len: usize) -> bool {
    value.len() == expected_len
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        && value.bytes().any(|byte| byte != b'0')
}

fn bounded_summary_field(value: &str, maximum_bytes: usize) -> String {
    let mut output = String::with_capacity(value.len().min(maximum_bytes));
    for character in value.chars() {
        let character = if character.is_whitespace() || character.is_control() {
            '_'
        } else {
            character
        };
        if output.len() + character.len_utf8() > maximum_bytes {
            break;
        }
        output.push(character);
    }
    output
}

fn validate_campaign_run(run: &CampaignRunEvidence, diagnostics: &mut Vec<CampaignGateDiagnostic>) {
    let lane_id = run.lane_id.as_str();
    if run.budget_seconds == 0 {
        campaign_diagnostic(
            diagnostics,
            "budget_zero",
            Some(lane_id),
            "a campaign budget must be explicit and non-zero",
        );
    }
    if let Some(maximum) = run.tier.maximum_budget_seconds()
        && run.budget_seconds > maximum
    {
        campaign_diagnostic(
            diagnostics,
            "tier_budget_exceeded",
            Some(lane_id),
            format!(
                "tier={:?} budget={} maximum={maximum}",
                run.tier, run.budget_seconds
            ),
        );
    }
    if run.elapsed_seconds > run.budget_seconds {
        campaign_diagnostic(
            diagnostics,
            "elapsed_exceeds_budget",
            Some(lane_id),
            format!(
                "elapsed={} budget={}",
                run.elapsed_seconds, run.budget_seconds
            ),
        );
    }
    if run.status != CampaignRunStatus::Completed {
        campaign_diagnostic(
            diagnostics,
            "campaign_incomplete",
            Some(lane_id),
            format!("status={:?}", run.status),
        );
    }

    match run.outcomes.terminal_total() {
        Some(terminal_total) if terminal_total == run.outcomes.generated => {}
        Some(terminal_total) => campaign_diagnostic(
            diagnostics,
            "outcome_count_imbalance",
            Some(lane_id),
            format!(
                "generated={} terminal_total={terminal_total}",
                run.outcomes.generated
            ),
        ),
        None => campaign_diagnostic(
            diagnostics,
            "outcome_count_overflow",
            Some(lane_id),
            "terminal outcome count overflowed u64",
        ),
    }
    if run.outcomes.timed_out > 0 {
        campaign_diagnostic(
            diagnostics,
            "timeout_is_not_pass",
            Some(lane_id),
            format!("timed_out={}", run.outcomes.timed_out),
        );
    }
    if run.outcomes.mismatched > 0 || run.outcomes.crashed > 0 {
        campaign_diagnostic(
            diagnostics,
            "semantic_or_crash_failure_present",
            Some(lane_id),
            format!(
                "mismatched={} crashed={}",
                run.outcomes.mismatched, run.outcomes.crashed
            ),
        );
    }

    match run.outcomes.mismatched.checked_add(run.outcomes.crashed) {
        Some(failures) => {
            if failures > run.outcomes.executed {
                campaign_diagnostic(
                    diagnostics,
                    "failure_subcount_exceeds_executed",
                    Some(lane_id),
                    format!("failures={failures} executed={}", run.outcomes.executed),
                );
            }
            if run.outcomes.reduced > failures {
                campaign_diagnostic(
                    diagnostics,
                    "reduced_subcount_exceeds_failures",
                    Some(lane_id),
                    format!("reduced={} failures={failures}", run.outcomes.reduced),
                );
            }
        }
        None => campaign_diagnostic(
            diagnostics,
            "failure_subcount_overflow",
            Some(lane_id),
            "mismatch and crash count overflowed u64",
        ),
    }
    if run.outcomes.promoted > run.outcomes.reduced {
        campaign_diagnostic(
            diagnostics,
            "promoted_subcount_exceeds_reduced",
            Some(lane_id),
            format!(
                "promoted={} reduced={}",
                run.outcomes.promoted, run.outcomes.reduced
            ),
        );
    }

    if !run.required_lane_evidence_verified {
        campaign_diagnostic(
            diagnostics,
            "required_lane_evidence_missing",
            Some(lane_id),
            "the public runner did not verify required execution lanes",
        );
    }
    if !run.public_replay_verified || run.replay_command.trim().is_empty() {
        campaign_diagnostic(
            diagnostics,
            "public_replay_missing",
            Some(lane_id),
            "a verified non-empty public replay command is required",
        );
    }
    if run.artifacts.is_empty() {
        campaign_diagnostic(
            diagnostics,
            "artifact_missing",
            Some(lane_id),
            "at least one retained artifact is required",
        );
    }
    for artifact in &run.artifacts {
        if artifact.path.trim().is_empty() || !is_nonzero_lower_hex(&artifact.sha256, 64) {
            campaign_diagnostic(
                diagnostics,
                "artifact_provenance_invalid",
                Some(lane_id),
                format!("path={} sha256={}", artifact.path, artifact.sha256),
            );
        }
        let minimum_retention = run.tier.minimum_retention_days();
        if artifact.retention_days < minimum_retention {
            campaign_diagnostic(
                diagnostics,
                "artifact_retention_too_short",
                Some(lane_id),
                format!(
                    "path={} retention_days={} minimum={minimum_retention}",
                    artifact.path, artifact.retention_days
                ),
            );
        }
    }

    let coverage_dimensions = [
        ("feature", &run.coverage.feature_ids),
        ("construct", &run.coverage.constructs),
        ("execution_lane", &run.coverage.execution_lanes),
        ("fault", &run.coverage.fault_kinds),
        ("concurrency_workload", &run.coverage.concurrency_workloads),
        ("reducer_family", &run.coverage.reducer_families),
    ];
    if coverage_dimensions
        .iter()
        .all(|(_, values)| values.is_empty())
    {
        campaign_diagnostic(
            diagnostics,
            "run_coverage_missing",
            Some(lane_id),
            "a gating run must report at least one coverage dimension",
        );
    }
    for (dimension, values) in coverage_dimensions {
        if values.iter().any(|value| value.trim().is_empty()) {
            campaign_diagnostic(
                diagnostics,
                "coverage_dimension_value_invalid",
                Some(lane_id),
                format!("dimension={dimension}"),
            );
        }
    }

    let required_dimensions: &[(&str, &[String])] = match run.lane_id.as_str() {
        "bd-turso-test-adaptation-zu081.5" => &[
            ("feature", &run.coverage.feature_ids),
            ("construct", &run.coverage.constructs),
            ("execution_lane", &run.coverage.execution_lanes),
        ],
        "bd-turso-test-adaptation-zu081.6" | "bd-turso-test-adaptation-zu081.19" => {
            &[("reducer_family", &run.coverage.reducer_families)]
        }
        "bd-turso-test-adaptation-zu081.8" => &[
            ("execution_lane", &run.coverage.execution_lanes),
            ("concurrency_workload", &run.coverage.concurrency_workloads),
        ],
        "bd-turso-test-adaptation-zu081.9" => &[
            ("fault", &run.coverage.fault_kinds),
            ("concurrency_workload", &run.coverage.concurrency_workloads),
        ],
        "bd-turso-test-adaptation-zu081.20" => &[
            ("construct", &run.coverage.constructs),
            ("concurrency_workload", &run.coverage.concurrency_workloads),
        ],
        _ => &[],
    };
    for (dimension, values) in required_dimensions {
        if values.is_empty() {
            campaign_diagnostic(
                diagnostics,
                "required_lane_coverage_missing",
                Some(lane_id),
                format!("dimension={dimension}"),
            );
        }
    }

    if run.lane_id == "bd-turso-test-adaptation-zu081.8"
        && run.history_accounting != HistoryAccounting::Deterministic
    {
        campaign_diagnostic(
            diagnostics,
            "production_history_not_deterministic",
            Some(lane_id),
            "the LabRuntime production-history lane must carry deterministic schedule evidence",
        );
    }
    if run.lane_id == "bd-turso-test-adaptation-zu081.19"
        && run.history_accounting != HistoryAccounting::Deterministic
    {
        campaign_diagnostic(
            diagnostics,
            "history_reduction_not_deterministic",
            Some(lane_id),
            "history/schedule reduction must preserve deterministic schedule evidence",
        );
    }
    if run.lane_id == "bd-turso-test-adaptation-zu081.9"
        && run.history_accounting != HistoryAccounting::ObservationOnly
    {
        campaign_diagnostic(
            diagnostics,
            "multiprocess_history_misclassified",
            Some(lane_id),
            "the OS-scheduled multiprocess lane must remain observation-only",
        );
    }
}

fn validate_seed_group(
    lane_id: &str,
    tier: CampaignTier,
    runs: &[&CampaignRunEvidence],
    diagnostics: &mut Vec<CampaignGateDiagnostic>,
) {
    let Some(first) = runs.first() else {
        return;
    };
    if first.expected_seed_count == 0 || first.shard.count == 0 {
        campaign_diagnostic(
            diagnostics,
            "seed_shard_configuration_invalid",
            Some(lane_id),
            format!(
                "tier={tier:?} expected_seed_count={} shard_count={}",
                first.expected_seed_count, first.shard.count
            ),
        );
        return;
    }

    let expected_seed_count = first.expected_seed_count;
    let shard_count = first.shard.count;
    if tier == CampaignTier::Presubmit && expected_seed_count > MAX_PRESUBMIT_SEEDS_PER_PROFILE {
        campaign_diagnostic(
            diagnostics,
            "presubmit_seed_budget_exceeded",
            Some(lane_id),
            format!(
                "expected_seed_count={expected_seed_count} maximum={MAX_PRESUBMIT_SEEDS_PER_PROFILE}"
            ),
        );
    }
    let mut ordered = runs.to_vec();
    ordered.sort_by_key(|run| (run.shard.start_seed, run.shard.index));
    let Some(first_ordered) = ordered.first() else {
        return;
    };
    let mut indexes = BTreeSet::new();
    let mut cursor = first_ordered.shard.start_seed;
    let mut covered_seed_count = 0_u64;

    for run in ordered {
        if run.expected_seed_count != expected_seed_count || run.shard.count != shard_count {
            campaign_diagnostic(
                diagnostics,
                "seed_shard_configuration_mismatch",
                Some(lane_id),
                format!("tier={tier:?} shard_index={}", run.shard.index),
            );
        }
        if run.shard.index >= run.shard.count || !indexes.insert(run.shard.index) {
            campaign_diagnostic(
                diagnostics,
                "seed_shard_index_invalid",
                Some(lane_id),
                format!(
                    "tier={tier:?} shard_index={} shard_count={}",
                    run.shard.index, run.shard.count
                ),
            );
        }
        if run.shard.start_seed != cursor || run.shard.end_seed_exclusive <= run.shard.start_seed {
            campaign_diagnostic(
                diagnostics,
                "seed_shard_gap_or_overlap",
                Some(lane_id),
                format!(
                    "tier={tier:?} expected_start={cursor} actual_range={}..{}",
                    run.shard.start_seed, run.shard.end_seed_exclusive
                ),
            );
        }
        if let Some(shard_seed_count) = run
            .shard
            .end_seed_exclusive
            .checked_sub(run.shard.start_seed)
        {
            if run.outcomes.generated != shard_seed_count {
                campaign_diagnostic(
                    diagnostics,
                    "seed_shard_outcome_count_mismatch",
                    Some(lane_id),
                    format!(
                        "tier={tier:?} shard_index={} seed_count={shard_seed_count} generated={}",
                        run.shard.index, run.outcomes.generated
                    ),
                );
            }
            if let Some(sum) = covered_seed_count.checked_add(shard_seed_count) {
                covered_seed_count = sum;
            } else {
                campaign_diagnostic(
                    diagnostics,
                    "seed_shard_coverage_overflow",
                    Some(lane_id),
                    format!("tier={tier:?}"),
                );
            }
        }
        cursor = run.shard.end_seed_exclusive;
    }

    if u32::try_from(indexes.len()) != Ok(shard_count) || covered_seed_count != expected_seed_count
    {
        campaign_diagnostic(
            diagnostics,
            "seed_shard_coverage_incomplete",
            Some(lane_id),
            format!(
                "tier={tier:?} indexes={}/{} covered_seeds={covered_seed_count}/{expected_seed_count}",
                indexes.len(),
                shard_count
            ),
        );
    }
}

/// Evaluate retained Turso campaign receipts against the cross-phase promotion contract.
#[must_use]
#[allow(clippy::too_many_lines)]
pub fn evaluate_turso_campaign_gate(input: &TursoCampaignGateInput) -> TursoCampaignScorecard {
    let mut diagnostics = Vec::new();

    for (field, value) in [
        ("workflow_id", input.workflow_id.as_str()),
        ("run_id", input.run_id.as_str()),
        ("build_id", input.build_id.as_str()),
    ] {
        if value.trim().is_empty() {
            campaign_diagnostic(
                &mut diagnostics,
                "provenance_field_missing",
                None,
                format!("field={field}"),
            );
        }
    }
    if !is_nonzero_lower_hex(&input.engine_sha, 40) {
        campaign_diagnostic(
            &mut diagnostics,
            "engine_sha_invalid",
            None,
            format!("engine_sha={}", input.engine_sha),
        );
    }
    if input.engine_dirty {
        campaign_diagnostic(
            &mut diagnostics,
            "engine_dirty",
            None,
            "promotion evidence must be bound to a clean engine snapshot",
        );
    }
    for (field, value) in [
        ("contract_hash", input.contract_hash.as_str()),
        ("profile_hash", input.profile_hash.as_str()),
    ] {
        if !is_nonzero_lower_hex(value, 64) {
            campaign_diagnostic(
                &mut diagnostics,
                "provenance_hash_invalid",
                None,
                format!("field={field} value={value}"),
            );
        }
    }

    let required_retention_days = input
        .promotion_stage
        .required_tiers()
        .last()
        .map_or(7, |tier| tier.minimum_retention_days());
    let mut global_gate_receipts = BTreeMap::new();
    for receipt in &input.global_gate_receipts {
        if global_gate_receipts.insert(receipt.gate, receipt).is_some() {
            campaign_diagnostic(
                &mut diagnostics,
                "global_gate_receipt_duplicate",
                None,
                format!("gate={:?}", receipt.gate),
            );
        }
        if !receipt.passed {
            campaign_diagnostic(
                &mut diagnostics,
                "global_gate_failed",
                None,
                format!("gate={:?}", receipt.gate),
            );
        }
        if receipt.command.trim().is_empty()
            || receipt.artifact.path.trim().is_empty()
            || !is_nonzero_lower_hex(&receipt.artifact.sha256, 64)
        {
            campaign_diagnostic(
                &mut diagnostics,
                "global_gate_provenance_invalid",
                None,
                format!("gate={:?}", receipt.gate),
            );
        }
        if receipt.artifact.retention_days < required_retention_days {
            campaign_diagnostic(
                &mut diagnostics,
                "global_gate_retention_too_short",
                None,
                format!(
                    "gate={:?} retention_days={} minimum={required_retention_days}",
                    receipt.gate, receipt.artifact.retention_days
                ),
            );
        }
    }
    for gate in CAMPAIGN_GLOBAL_GATES {
        if !global_gate_receipts.contains_key(&gate) {
            campaign_diagnostic(
                &mut diagnostics,
                "global_gate_receipt_missing",
                None,
                format!("gate={gate:?}"),
            );
        }
    }

    let mut optional_by_id = BTreeMap::new();
    for decision in &input.optional_decisions {
        if optional_by_id
            .insert(decision.bead_id.as_str(), decision)
            .is_some()
        {
            campaign_diagnostic(
                &mut diagnostics,
                "optional_decision_duplicate",
                Some(&decision.bead_id),
                "optional/external decision appears more than once",
            );
        }
        if !TURSO_OPTIONAL_DECISIONS.contains(&decision.bead_id.as_str()) {
            campaign_diagnostic(
                &mut diagnostics,
                "optional_decision_unknown",
                Some(&decision.bead_id),
                "decision is not a Turso optional/external child",
            );
        }
        if decision.rationale.trim().is_empty() {
            campaign_diagnostic(
                &mut diagnostics,
                "optional_decision_rationale_missing",
                Some(&decision.bead_id),
                "every disposition requires a bounded rationale",
            );
        } else if decision.rationale.len() > MAX_OPTIONAL_RATIONALE_BYTES {
            campaign_diagnostic(
                &mut diagnostics,
                "optional_decision_rationale_too_long",
                Some(&decision.bead_id),
                format!(
                    "rationale_bytes={} maximum={MAX_OPTIONAL_RATIONALE_BYTES}",
                    decision.rationale.len()
                ),
            );
        }
        let admission_is_consistent = match decision.disposition {
            CampaignDisposition::Adopted => decision.admitted,
            CampaignDisposition::Deferred | CampaignDisposition::Rejected => !decision.admitted,
        };
        if !admission_is_consistent {
            campaign_diagnostic(
                &mut diagnostics,
                "optional_decision_admission_invalid",
                Some(&decision.bead_id),
                format!(
                    "disposition={:?} admitted={}",
                    decision.disposition, decision.admitted
                ),
            );
        }
    }
    for expected in TURSO_OPTIONAL_DECISIONS {
        if !optional_by_id.contains_key(expected) {
            campaign_diagnostic(
                &mut diagnostics,
                "optional_decision_missing",
                Some(expected),
                "scorecard must list every optional/external child",
            );
        }
    }

    let mut gating_lanes = TURSO_NATIVE_LANES
        .into_iter()
        .map(str::to_owned)
        .collect::<BTreeSet<_>>();
    for decision in &input.optional_decisions {
        if decision.disposition == CampaignDisposition::Adopted && decision.admitted {
            gating_lanes.insert(decision.bead_id.clone());
        }
    }

    let mut sorted_runs = input.runs.clone();
    sorted_runs.sort_by(|left, right| {
        (left.lane_id.as_str(), left.tier, left.shard.index).cmp(&(
            right.lane_id.as_str(),
            right.tier,
            right.shard.index,
        ))
    });
    let mut grouped = BTreeMap::<(String, CampaignTier), Vec<&CampaignRunEvidence>>::new();
    let mut totals = CampaignOutcomeCounts::default();
    let mut coverage_dimensions: [BTreeSet<String>; 6] = std::array::from_fn(|_| BTreeSet::new());
    let mut lane_accumulators =
        BTreeMap::<String, (usize, CampaignOutcomeCounts, usize, usize)>::new();

    for run in &sorted_runs {
        if !input.promotion_stage.required_tiers().contains(&run.tier) {
            campaign_diagnostic(
                &mut diagnostics,
                "unexpected_tier_receipt",
                Some(&run.lane_id),
                format!(
                    "stage={:?} does not admit tier={:?}",
                    input.promotion_stage, run.tier
                ),
            );
        }
        if !gating_lanes.contains(&run.lane_id) {
            campaign_diagnostic(
                &mut diagnostics,
                "non_gating_lane_receipt",
                Some(&run.lane_id),
                "deferred, rejected, conditional, or unknown lanes cannot supply gate evidence",
            );
        }
        validate_campaign_run(run, &mut diagnostics);
        grouped
            .entry((run.lane_id.clone(), run.tier))
            .or_default()
            .push(run);

        if let Some(sum) = totals.checked_add(run.outcomes) {
            totals = sum;
        } else {
            campaign_diagnostic(
                &mut diagnostics,
                "campaign_total_overflow",
                Some(&run.lane_id),
                "aggregate campaign counts overflowed u64",
            );
        }

        for value in &run.coverage.feature_ids {
            coverage_dimensions[0].insert(value.clone());
        }
        for value in &run.coverage.constructs {
            coverage_dimensions[1].insert(value.clone());
        }
        for value in &run.coverage.execution_lanes {
            coverage_dimensions[2].insert(value.clone());
        }
        for value in &run.coverage.fault_kinds {
            coverage_dimensions[3].insert(value.clone());
        }
        for value in &run.coverage.concurrency_workloads {
            coverage_dimensions[4].insert(value.clone());
        }
        for value in &run.coverage.reducer_families {
            coverage_dimensions[5].insert(value.clone());
        }

        let accumulator = lane_accumulators
            .entry(run.lane_id.clone())
            .or_insert_with(|| (0, CampaignOutcomeCounts::default(), 0, 0));
        accumulator.0 += 1;
        if let Some(sum) = accumulator.1.checked_add(run.outcomes) {
            accumulator.1 = sum;
        }
        match run.history_accounting {
            HistoryAccounting::Deterministic => accumulator.2 += 1,
            HistoryAccounting::ObservationOnly => accumulator.3 += 1,
            HistoryAccounting::NotApplicable => {}
        }
    }

    for lane_id in &gating_lanes {
        for tier in input.promotion_stage.required_tiers() {
            let key = (lane_id.clone(), *tier);
            match grouped.get(&key) {
                Some(group) => validate_seed_group(lane_id, *tier, group, &mut diagnostics),
                None => campaign_diagnostic(
                    &mut diagnostics,
                    "required_lane_tier_missing",
                    Some(lane_id),
                    format!("tier={tier:?}"),
                ),
            }
        }
    }

    for (name, values) in [
        ("feature", &coverage_dimensions[0]),
        ("construct", &coverage_dimensions[1]),
        ("execution_lane", &coverage_dimensions[2]),
        ("fault", &coverage_dimensions[3]),
        ("concurrency_workload", &coverage_dimensions[4]),
        ("reducer_family", &coverage_dimensions[5]),
    ] {
        if values.is_empty() {
            campaign_diagnostic(
                &mut diagnostics,
                "coverage_dimension_missing",
                None,
                format!("dimension={name}"),
            );
        }
    }

    let drift_decision = input
        .drift
        .linked_contract_decision
        .as_deref()
        .filter(|decision| decision.starts_with("bd-") && decision.len() > 3);
    if totals.unsupported > input.drift.baseline_unsupported && drift_decision.is_none() {
        campaign_diagnostic(
            &mut diagnostics,
            "unsupported_count_drift",
            None,
            format!(
                "baseline={} current={}",
                input.drift.baseline_unsupported, totals.unsupported
            ),
        );
    }
    if totals.skipped > input.drift.baseline_skipped && drift_decision.is_none() {
        campaign_diagnostic(
            &mut diagnostics,
            "skipped_count_drift",
            None,
            format!(
                "baseline={} current={}",
                input.drift.baseline_skipped, totals.skipped
            ),
        );
    }

    let lane_summaries = lane_accumulators
        .into_iter()
        .map(
            |(
                lane_id,
                (run_count, outcomes, deterministic_history_runs, observation_only_history_runs),
            )| CampaignLaneSummary {
                lane_id,
                run_count,
                outcomes,
                deterministic_history_runs,
                observation_only_history_runs,
            },
        )
        .collect();
    let mut optional_decisions = input.optional_decisions.clone();
    optional_decisions.sort_by(|left, right| left.bead_id.cmp(&right.bead_id));
    let mut retained_global_gate_receipts = input.global_gate_receipts.clone();
    retained_global_gate_receipts.sort_by_key(|receipt| receipt.gate);
    let outcome = if diagnostics.is_empty() {
        CampaignPromotionOutcome::Promote
    } else {
        CampaignPromotionOutcome::Hold
    };

    TursoCampaignScorecard {
        schema_version: TURSO_CAMPAIGN_SCORECARD_SCHEMA_VERSION.to_owned(),
        promotion_stage: input.promotion_stage,
        outcome,
        workflow_id: input.workflow_id.clone(),
        run_id: input.run_id.clone(),
        build_id: input.build_id.clone(),
        engine_sha: input.engine_sha.clone(),
        engine_dirty: input.engine_dirty,
        contract_hash: input.contract_hash.clone(),
        profile_hash: input.profile_hash.clone(),
        global_gate_receipts: retained_global_gate_receipts,
        drift: input.drift.clone(),
        optional_decisions,
        runs: sorted_runs,
        totals,
        lane_summaries,
        diagnostics,
    }
}

// ---------------------------------------------------------------------------
// Threshold configuration
// ---------------------------------------------------------------------------

/// SLO threshold configuration for the coverage gate.
///
/// Thresholds are expressed as fractions (0.0–1.0). Each threshold is
/// checked independently and produces a separate annotation on failure.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoverageThresholds {
    /// Minimum required global fill percentage across all categories.
    pub global_fill_pct_min: f64,

    /// Minimum required fill percentage for any individual category.
    pub category_fill_pct_min: f64,

    /// Minimum required non-mock evidence coverage percentage.
    pub evidence_coverage_pct_min: f64,

    /// Minimum required number of invariants per category.
    pub min_invariants_per_category: usize,

    /// Minimum required number of property-based tests globally.
    pub min_property_tests_global: usize,

    /// Categories that are critical: failure here is always blocking.
    pub critical_categories: Vec<FeatureCategory>,
}

impl Default for CoverageThresholds {
    fn default() -> Self {
        Self {
            global_fill_pct_min: 0.60,
            category_fill_pct_min: 0.30,
            evidence_coverage_pct_min: 0.50,
            min_invariants_per_category: 2,
            min_property_tests_global: 5,
            critical_categories: vec![
                FeatureCategory::SqlGrammar,
                FeatureCategory::VdbeOpcodes,
                FeatureCategory::StorageTransaction,
            ],
        }
    }
}

impl CoverageThresholds {
    /// Strict thresholds for release readiness.
    #[must_use]
    pub fn strict() -> Self {
        Self {
            global_fill_pct_min: 0.80,
            category_fill_pct_min: 0.50,
            evidence_coverage_pct_min: 0.70,
            min_invariants_per_category: 5,
            min_property_tests_global: 10,
            critical_categories: vec![
                FeatureCategory::SqlGrammar,
                FeatureCategory::VdbeOpcodes,
                FeatureCategory::StorageTransaction,
                FeatureCategory::BuiltinFunctions,
            ],
        }
    }

    /// Lenient thresholds for early development.
    #[must_use]
    pub fn lenient() -> Self {
        Self {
            global_fill_pct_min: 0.30,
            category_fill_pct_min: 0.10,
            evidence_coverage_pct_min: 0.20,
            min_invariants_per_category: 1,
            min_property_tests_global: 1,
            critical_categories: vec![FeatureCategory::StorageTransaction],
        }
    }
}

// ---------------------------------------------------------------------------
// Gate violations
// ---------------------------------------------------------------------------

/// Severity of a coverage gate violation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ViolationSeverity {
    /// Informational: logged but does not block.
    Info,
    /// Warning: logged and tracked but does not block.
    Warning,
    /// Blocking: prevents merge / release.
    Blocking,
}

impl fmt::Display for ViolationSeverity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Info => write!(f, "info"),
            Self::Warning => write!(f, "warning"),
            Self::Blocking => write!(f, "blocking"),
        }
    }
}

/// A single threshold violation detected by the coverage gate.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoverageViolation {
    /// Which threshold was breached.
    pub check: String,
    /// Severity of the violation.
    pub severity: ViolationSeverity,
    /// Category this violation relates to (if per-category).
    pub category: Option<String>,
    /// Observed value (human-readable).
    pub observed: String,
    /// Required threshold (human-readable).
    pub required: String,
    /// Actionable remediation instruction.
    pub remediation: String,
}

// ---------------------------------------------------------------------------
// Per-category result
// ---------------------------------------------------------------------------

/// Coverage result for a single taxonomy category.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CategoryCoverageResult {
    /// Feature category.
    pub category: FeatureCategory,
    /// Display name.
    pub display_name: String,
    /// Fill percentage (from unit matrix).
    pub fill_pct: f64,
    /// Number of tests in this category.
    pub test_count: usize,
    /// Number of invariants covered.
    pub invariant_count: usize,
    /// Number of property-based tests.
    pub property_test_count: usize,
    /// Whether this category is critical.
    pub is_critical: bool,
    /// Whether this category passes all thresholds.
    pub passes: bool,
    /// Missing coverage areas.
    pub missing_areas: Vec<String>,
}

// ---------------------------------------------------------------------------
// Gate report
// ---------------------------------------------------------------------------

/// Overall verdict of the coverage gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CoverageVerdict {
    /// All thresholds met.
    Pass,
    /// One or more blocking violations.
    Fail,
    /// No blocking violations but warnings present.
    PassWithWarnings,
}

impl fmt::Display for CoverageVerdict {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pass => write!(f, "PASS"),
            Self::Fail => write!(f, "FAIL"),
            Self::PassWithWarnings => write!(f, "PASS_WITH_WARNINGS"),
        }
    }
}

/// Complete report produced by the coverage gate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoverageGateReport {
    /// Schema version.
    pub schema_version: String,
    /// Bead ID.
    pub bead_id: String,
    /// Overall verdict.
    pub verdict: CoverageVerdict,
    /// Total tests in the matrix.
    pub total_tests: usize,
    /// Total invariants across all tests.
    pub total_invariants: usize,
    /// Global fill percentage (weighted average).
    pub global_fill_pct: f64,
    /// Evidence coverage percentage (from no-mock map).
    pub evidence_coverage_pct: f64,
    /// Total property-based tests.
    pub property_test_count: usize,
    /// Per-category results.
    pub categories: Vec<CategoryCoverageResult>,
    /// All violations found.
    pub violations: Vec<CoverageViolation>,
    /// Blocking violation count.
    pub blocking_count: usize,
    /// Warning count.
    pub warning_count: usize,
    /// Human-readable summary.
    pub summary: String,
}

impl CoverageGateReport {
    /// Serialize to deterministic JSON.
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

    /// Render a human-readable markdown summary.
    #[must_use]
    pub fn render_summary(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(out, "# Coverage Gate Report");
        let _ = writeln!(out);
        let _ = writeln!(out, "**Verdict**: {}", self.verdict);
        let _ = writeln!(
            out,
            "**Global Fill**: {:.1}% | **Evidence Coverage**: {:.1}%",
            self.global_fill_pct * 100.0,
            self.evidence_coverage_pct * 100.0,
        );
        let _ = writeln!(
            out,
            "**Tests**: {} | **Invariants**: {} | **Property Tests**: {}",
            self.total_tests, self.total_invariants, self.property_test_count,
        );
        let _ = writeln!(out);

        // Category table.
        let _ = writeln!(out, "## Per-Category Results");
        let _ = writeln!(out);
        let _ = writeln!(
            out,
            "| Category | Fill % | Tests | Invariants | Critical | Status |"
        );
        let _ = writeln!(
            out,
            "|----------|--------|-------|------------|----------|--------|"
        );
        for cat in &self.categories {
            let status = if cat.passes { "PASS" } else { "FAIL" };
            let crit = if cat.is_critical { "yes" } else { "no" };
            let _ = writeln!(
                out,
                "| {} | {:.1}% | {} | {} | {} | {} |",
                cat.display_name,
                cat.fill_pct * 100.0,
                cat.test_count,
                cat.invariant_count,
                crit,
                status,
            );
        }
        let _ = writeln!(out);

        if !self.violations.is_empty() {
            let _ = writeln!(
                out,
                "## Violations ({} blocking, {} warnings)",
                self.blocking_count, self.warning_count
            );
            let _ = writeln!(out);
            for v in &self.violations {
                let cat_str = v.category.as_deref().unwrap_or("global");
                let _ = writeln!(
                    out,
                    "- **[{}]** `{}` ({cat_str}): observed={}, required={}",
                    v.severity, v.check, v.observed, v.required,
                );
                let _ = writeln!(out, "  - Remediation: {}", v.remediation);
            }
        }

        out
    }
}

// ---------------------------------------------------------------------------
// Gate evaluation
// ---------------------------------------------------------------------------

/// Run the coverage gate against the canonical matrix and evidence map.
///
/// Returns a complete [`CoverageGateReport`] with per-category results
/// and actionable violations.
#[must_use]
pub fn evaluate_coverage_gate(thresholds: &CoverageThresholds) -> CoverageGateReport {
    let matrix = build_canonical_matrix();
    let evidence_map = build_evidence_map();
    evaluate_coverage_gate_with(thresholds, &matrix, &evidence_map)
}

/// Run the coverage gate against provided matrix and evidence map.
///
/// This variant is useful for testing with synthetic data.
#[must_use]
#[allow(clippy::too_many_lines)]
pub fn evaluate_coverage_gate_with(
    thresholds: &CoverageThresholds,
    matrix: &UnitMatrix,
    evidence_map: &NoMockEvidenceMap,
) -> CoverageGateReport {
    let mut violations = Vec::new();
    let mut categories = Vec::new();

    // Compute global metrics.
    let total_tests = matrix.tests.len();
    let total_invariants: usize = matrix.tests.iter().map(|t| t.invariants.len()).sum();
    let property_test_count: usize = matrix.tests.iter().filter(|t| t.property_based).count();

    // Weighted global fill percentage.
    let global_fill_pct = if matrix.coverage.is_empty() {
        0.0
    } else {
        let total_weight: usize = matrix.coverage.iter().map(|b| b.test_count.max(1)).sum();
        let weighted_sum: f64 = matrix
            .coverage
            .iter()
            .map(|b| b.fill_pct * (b.test_count.max(1) as f64))
            .sum();
        if total_weight == 0 {
            0.0
        } else {
            weighted_sum / total_weight as f64
        }
    };

    let evidence_coverage_pct = evidence_map.stats.coverage_pct;

    // Check global fill threshold.
    if global_fill_pct < thresholds.global_fill_pct_min {
        violations.push(CoverageViolation {
            check: "global_fill_pct".to_owned(),
            severity: ViolationSeverity::Blocking,
            category: None,
            observed: format!("{:.1}%", global_fill_pct * 100.0),
            required: format!("{:.1}%", thresholds.global_fill_pct_min * 100.0),
            remediation: "Add more unit tests to increase overall coverage fill percentage."
                .to_owned(),
        });
    }

    // Check evidence coverage threshold.
    if evidence_coverage_pct < thresholds.evidence_coverage_pct_min {
        violations.push(CoverageViolation {
            check: "evidence_coverage_pct".to_owned(),
            severity: ViolationSeverity::Blocking,
            category: None,
            observed: format!("{:.1}%", evidence_coverage_pct * 100.0),
            required: format!("{:.1}%", thresholds.evidence_coverage_pct_min * 100.0),
            remediation:
                "Add no-mock evidence entries for critical invariants in no_mock_evidence.rs."
                    .to_owned(),
        });
    }

    // Check property test count.
    if property_test_count < thresholds.min_property_tests_global {
        violations.push(CoverageViolation {
            check: "min_property_tests_global".to_owned(),
            severity: ViolationSeverity::Warning,
            category: None,
            observed: format!("{property_test_count}"),
            required: format!("{}", thresholds.min_property_tests_global),
            remediation: "Add property-based tests (proptest) to improve coverage breadth."
                .to_owned(),
        });
    }

    // Per-category evaluation.
    for bucket in &matrix.coverage {
        let is_critical = thresholds.critical_categories.contains(&bucket.category);

        let mut cat_passes = true;

        // Check category fill percentage.
        if bucket.fill_pct < thresholds.category_fill_pct_min {
            let severity = if is_critical {
                ViolationSeverity::Blocking
            } else {
                ViolationSeverity::Warning
            };
            violations.push(CoverageViolation {
                check: "category_fill_pct".to_owned(),
                severity,
                category: Some(bucket.category.display_name().to_owned()),
                observed: format!("{:.1}%", bucket.fill_pct * 100.0),
                required: format!("{:.1}%", thresholds.category_fill_pct_min * 100.0),
                remediation: format!(
                    "Add tests for category '{}'. Missing areas: {}",
                    bucket.category.display_name(),
                    if bucket.missing_coverage.is_empty() {
                        "(none listed)".to_owned()
                    } else {
                        bucket.missing_coverage.join(", ")
                    },
                ),
            });
            if severity == ViolationSeverity::Blocking {
                cat_passes = false;
            }
        }

        // Check invariant count.
        if bucket.invariant_count < thresholds.min_invariants_per_category {
            let severity = if is_critical {
                ViolationSeverity::Blocking
            } else {
                ViolationSeverity::Info
            };
            violations.push(CoverageViolation {
                check: "min_invariants_per_category".to_owned(),
                severity,
                category: Some(bucket.category.display_name().to_owned()),
                observed: format!("{}", bucket.invariant_count),
                required: format!("{}", thresholds.min_invariants_per_category),
                remediation: format!(
                    "Add more invariant assertions to tests in category '{}'.",
                    bucket.category.display_name(),
                ),
            });
            if severity == ViolationSeverity::Blocking {
                cat_passes = false;
            }
        }

        categories.push(CategoryCoverageResult {
            category: bucket.category,
            display_name: bucket.category.display_name().to_owned(),
            fill_pct: bucket.fill_pct,
            test_count: bucket.test_count,
            invariant_count: bucket.invariant_count,
            property_test_count: bucket.property_test_count,
            is_critical,
            passes: cat_passes,
            missing_areas: bucket.missing_coverage.clone(),
        });
    }

    let blocking_count = violations
        .iter()
        .filter(|v| v.severity == ViolationSeverity::Blocking)
        .count();
    let warning_count = violations
        .iter()
        .filter(|v| v.severity == ViolationSeverity::Warning)
        .count();

    let verdict = if blocking_count > 0 {
        CoverageVerdict::Fail
    } else if warning_count > 0 {
        CoverageVerdict::PassWithWarnings
    } else {
        CoverageVerdict::Pass
    };

    let summary = format!(
        "Coverage gate {verdict}: {total_tests} tests, {total_invariants} invariants, \
         global fill {:.1}%, evidence {:.1}%, {blocking_count} blocking, {warning_count} warnings",
        global_fill_pct * 100.0,
        evidence_coverage_pct * 100.0,
    );

    CoverageGateReport {
        schema_version: COVERAGE_GATE_SCHEMA_VERSION.to_owned(),
        bead_id: BEAD_ID.to_owned(),
        verdict,
        total_tests,
        total_invariants,
        global_fill_pct,
        evidence_coverage_pct,
        property_test_count,
        categories,
        violations,
        blocking_count,
        warning_count,
        summary,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn run_default_gate() -> CoverageGateReport {
        evaluate_coverage_gate(&CoverageThresholds::default())
    }

    fn run_lenient_gate() -> CoverageGateReport {
        evaluate_coverage_gate(&CoverageThresholds::lenient())
    }

    #[test]
    fn default_thresholds_reasonable() {
        let t = CoverageThresholds::default();
        assert!(t.global_fill_pct_min > 0.0 && t.global_fill_pct_min <= 1.0);
        assert!(t.category_fill_pct_min > 0.0 && t.category_fill_pct_min <= 1.0);
        assert!(t.evidence_coverage_pct_min > 0.0 && t.evidence_coverage_pct_min <= 1.0);
        assert!(t.min_invariants_per_category > 0);
        assert!(t.min_property_tests_global > 0);
        assert!(!t.critical_categories.is_empty());
    }

    #[test]
    fn strict_thresholds_higher_than_default() {
        let d = CoverageThresholds::default();
        let s = CoverageThresholds::strict();
        assert!(s.global_fill_pct_min >= d.global_fill_pct_min);
        assert!(s.category_fill_pct_min >= d.category_fill_pct_min);
        assert!(s.evidence_coverage_pct_min >= d.evidence_coverage_pct_min);
        assert!(s.min_invariants_per_category >= d.min_invariants_per_category);
        assert!(s.critical_categories.len() >= d.critical_categories.len());
    }

    #[test]
    fn lenient_thresholds_lower_than_default() {
        let d = CoverageThresholds::default();
        let l = CoverageThresholds::lenient();
        assert!(l.global_fill_pct_min <= d.global_fill_pct_min);
        assert!(l.category_fill_pct_min <= d.category_fill_pct_min);
        assert!(l.evidence_coverage_pct_min <= d.evidence_coverage_pct_min);
    }

    #[test]
    fn gate_report_has_all_categories() {
        let report = run_default_gate();
        assert_eq!(report.categories.len(), FeatureCategory::ALL.len());
    }

    #[test]
    fn gate_report_global_metrics_non_negative() {
        let report = run_default_gate();
        assert!(report.global_fill_pct >= 0.0);
        assert!(report.evidence_coverage_pct >= 0.0);
        assert!(report.total_tests > 0);
        assert!(report.total_invariants > 0);
    }

    #[test]
    fn gate_report_json_roundtrip() {
        let report = run_default_gate();
        let json = report.to_json().expect("serialize");
        let restored = CoverageGateReport::from_json(&json).expect("deserialize");
        assert_eq!(restored.verdict, report.verdict);
        assert_eq!(restored.total_tests, report.total_tests);
        assert_eq!(restored.blocking_count, report.blocking_count);
        assert_eq!(restored.categories.len(), report.categories.len());
    }

    #[test]
    fn verdict_display() {
        assert_eq!(format!("{}", CoverageVerdict::Pass), "PASS");
        assert_eq!(format!("{}", CoverageVerdict::Fail), "FAIL");
        assert_eq!(
            format!("{}", CoverageVerdict::PassWithWarnings),
            "PASS_WITH_WARNINGS"
        );
    }

    #[test]
    fn severity_display() {
        assert_eq!(format!("{}", ViolationSeverity::Info), "info");
        assert_eq!(format!("{}", ViolationSeverity::Warning), "warning");
        assert_eq!(format!("{}", ViolationSeverity::Blocking), "blocking");
    }

    #[test]
    fn severity_ordering() {
        assert!(ViolationSeverity::Info < ViolationSeverity::Warning);
        assert!(ViolationSeverity::Warning < ViolationSeverity::Blocking);
    }

    #[test]
    fn summary_contains_verdict() {
        let report = run_default_gate();
        assert!(
            report.summary.contains("PASS") || report.summary.contains("FAIL"),
            "summary should contain verdict: {}",
            report.summary,
        );
    }

    #[test]
    fn render_summary_contains_category_table() {
        let report = run_default_gate();
        let md = report.render_summary();
        assert!(
            md.contains("Per-Category Results"),
            "missing category section"
        );
        assert!(md.contains("Fill %"), "missing table header");
        // Check at least one category appears.
        assert!(
            md.contains("SQL Grammar") || md.contains("VDBE Opcodes"),
            "no category names in summary",
        );
    }

    #[test]
    fn lenient_gate_passes_canonical_data() {
        // The canonical matrix/evidence should pass lenient thresholds.
        let report = run_lenient_gate();
        assert_eq!(
            report.verdict,
            CoverageVerdict::Pass,
            "lenient gate should pass: violations={:?}",
            report
                .violations
                .iter()
                .filter(|v| v.severity == ViolationSeverity::Blocking)
                .map(|v| format!(
                    "{}: {} (obs={}, req={})",
                    v.check,
                    v.category.as_deref().unwrap_or("global"),
                    v.observed,
                    v.required
                ))
                .collect::<Vec<_>>(),
        );
    }

    #[test]
    fn critical_category_failure_is_blocking() {
        // With thresholds that set fill_pct_min very high, critical category
        // failures should be blocking.
        let t = CoverageThresholds {
            category_fill_pct_min: 0.999,
            ..CoverageThresholds::default()
        };
        let report = evaluate_coverage_gate(&t);
        let blocking_cats: Vec<_> = report
            .violations
            .iter()
            .filter(|v| v.severity == ViolationSeverity::Blocking && v.check == "category_fill_pct")
            .filter_map(|v| v.category.as_deref())
            .collect();
        // At least one critical category should be blocking.
        let critical_names: Vec<_> = t
            .critical_categories
            .iter()
            .map(|c| c.display_name())
            .collect();
        let any_critical_blocked = blocking_cats
            .iter()
            .any(|name| critical_names.contains(name));
        assert!(
            any_critical_blocked,
            "expected at least one critical category to be blocking",
        );
    }

    #[test]
    fn non_critical_category_failure_is_warning() {
        // Extensions is not in default critical categories; its failure
        // should be a warning, not blocking.
        let t = CoverageThresholds {
            category_fill_pct_min: 0.999,
            ..CoverageThresholds::default()
        };
        let report = evaluate_coverage_gate(&t);
        let ext_violations: Vec<_> = report
            .violations
            .iter()
            .filter(|v| {
                v.category.as_deref() == Some("Extensions") && v.check == "category_fill_pct"
            })
            .collect();
        for v in &ext_violations {
            assert_eq!(
                v.severity,
                ViolationSeverity::Warning,
                "Extensions category failure should be warning, not {:?}",
                v.severity,
            );
        }
    }

    #[test]
    fn zero_threshold_always_passes() {
        let t = CoverageThresholds {
            global_fill_pct_min: 0.0,
            category_fill_pct_min: 0.0,
            evidence_coverage_pct_min: 0.0,
            min_invariants_per_category: 0,
            min_property_tests_global: 0,
            critical_categories: vec![],
        };
        let report = evaluate_coverage_gate(&t);
        assert_eq!(report.blocking_count, 0);
        assert!(
            report.verdict == CoverageVerdict::Pass
                || report.verdict == CoverageVerdict::PassWithWarnings,
        );
    }

    #[test]
    fn category_results_have_display_names() {
        let report = run_default_gate();
        for cat in &report.categories {
            assert!(
                !cat.display_name.is_empty(),
                "empty display name for {:?}",
                cat.category
            );
        }
    }

    #[test]
    fn violations_have_remediation() {
        let report = run_default_gate();
        for v in &report.violations {
            assert!(
                !v.remediation.is_empty(),
                "violation {:?} missing remediation",
                v.check
            );
        }
    }

    #[test]
    fn gate_is_deterministic() {
        let r1 = run_default_gate();
        let r2 = run_default_gate();
        assert_eq!(r1.verdict, r2.verdict);
        assert_eq!(r1.total_tests, r2.total_tests);
        assert_eq!(r1.total_invariants, r2.total_invariants);
        assert_eq!(r1.blocking_count, r2.blocking_count);
        assert_eq!(r1.categories.len(), r2.categories.len());
    }

    #[test]
    fn schema_version_set() {
        let report = run_default_gate();
        assert_eq!(report.schema_version, COVERAGE_GATE_SCHEMA_VERSION);
    }

    #[test]
    fn bead_id_set() {
        let report = run_default_gate();
        assert_eq!(report.bead_id, BEAD_ID);
    }

    fn campaign_optional_decisions() -> Vec<OptionalCampaignDecision> {
        TURSO_OPTIONAL_DECISIONS
            .into_iter()
            .map(|bead_id| OptionalCampaignDecision {
                bead_id: bead_id.to_owned(),
                disposition: CampaignDisposition::Deferred,
                admitted: false,
                rationale: format!("bounded decision for {bead_id}"),
            })
            .collect()
    }

    fn campaign_run(lane_id: &str, tier: CampaignTier) -> CampaignRunEvidence {
        let history_accounting = match lane_id {
            "bd-turso-test-adaptation-zu081.8" | "bd-turso-test-adaptation-zu081.19" => {
                HistoryAccounting::Deterministic
            }
            "bd-turso-test-adaptation-zu081.9" => HistoryAccounting::ObservationOnly,
            _ => HistoryAccounting::NotApplicable,
        };
        let budget_seconds = match tier {
            CampaignTier::Presubmit => 600,
            CampaignTier::Nightly => 3_600,
            CampaignTier::Manual => 7_200,
        };
        CampaignRunEvidence {
            lane_id: lane_id.to_owned(),
            tier,
            shard: CampaignSeedShard {
                index: 0,
                count: 1,
                start_seed: 0,
                end_seed_exclusive: 100,
            },
            expected_seed_count: 100,
            budget_seconds,
            elapsed_seconds: budget_seconds / 2,
            status: CampaignRunStatus::Completed,
            outcomes: CampaignOutcomeCounts {
                generated: 100,
                executed: 100,
                ..CampaignOutcomeCounts::default()
            },
            coverage: CampaignCoverageDimensions {
                feature_ids: vec!["SURF-SQL-CORE-001".to_owned()],
                constructs: vec!["transaction".to_owned()],
                execution_lanes: vec!["pager_backed_required".to_owned()],
                fault_kinds: (lane_id == "bd-turso-test-adaptation-zu081.9")
                    .then(|| "process_kill".to_owned())
                    .into_iter()
                    .collect(),
                concurrency_workloads: [
                    "bd-turso-test-adaptation-zu081.8",
                    "bd-turso-test-adaptation-zu081.9",
                    "bd-turso-test-adaptation-zu081.20",
                ]
                .contains(&lane_id)
                .then(|| "bank_transfer".to_owned())
                .into_iter()
                .collect(),
                reducer_families: [
                    "bd-turso-test-adaptation-zu081.6",
                    "bd-turso-test-adaptation-zu081.19",
                    "bd-turso-test-adaptation-zu081.20",
                ]
                .contains(&lane_id)
                .then(|| "canonical_minimizer".to_owned())
                .into_iter()
                .collect(),
            },
            history_accounting,
            required_lane_evidence_verified: true,
            public_replay_verified: true,
            replay_command: format!("cargo test -p fsqlite-harness {lane_id}"),
            artifacts: vec![CampaignArtifactEvidence {
                path: format!("artifacts/{lane_id}/{tier:?}.json"),
                sha256: "b".repeat(64),
                retention_days: tier.minimum_retention_days(),
            }],
        }
    }

    fn passing_campaign_input(stage: CampaignPromotionStage) -> TursoCampaignGateInput {
        let runs = TURSO_NATIVE_LANES
            .into_iter()
            .flat_map(|lane_id| {
                stage
                    .required_tiers()
                    .iter()
                    .map(move |tier| campaign_run(lane_id, *tier))
            })
            .collect();
        TursoCampaignGateInput {
            promotion_stage: stage,
            workflow_id: "verification-gates.yml".to_owned(),
            run_id: "run-42".to_owned(),
            build_id: "build-42".to_owned(),
            engine_sha: "a".repeat(40),
            engine_dirty: false,
            contract_hash: "c".repeat(64),
            profile_hash: "d".repeat(64),
            global_gate_receipts: CAMPAIGN_GLOBAL_GATES
                .into_iter()
                .map(|gate| CampaignGlobalGateReceipt {
                    gate,
                    passed: true,
                    command: format!("verify {gate:?}"),
                    artifact: CampaignArtifactEvidence {
                        path: format!("artifacts/global/{gate:?}.log"),
                        sha256: "e".repeat(64),
                        retention_days: stage
                            .required_tiers()
                            .last()
                            .map_or(7, |tier| tier.minimum_retention_days()),
                    },
                })
                .collect(),
            drift: CampaignDriftControl {
                baseline_unsupported: 0,
                baseline_skipped: 0,
                linked_contract_decision: None,
            },
            optional_decisions: campaign_optional_decisions(),
            runs,
        }
    }

    fn campaign_diagnostic_codes(scorecard: &TursoCampaignScorecard) -> Vec<&str> {
        scorecard
            .diagnostics
            .iter()
            .map(|diagnostic| diagnostic.code.as_str())
            .collect()
    }

    #[test]
    fn turso_release_scorecard_promotes_complete_retained_evidence() {
        let input = passing_campaign_input(CampaignPromotionStage::Release);
        let scorecard = evaluate_turso_campaign_gate(&input);
        assert_eq!(scorecard.outcome, CampaignPromotionOutcome::Promote);
        assert!(scorecard.diagnostics.is_empty());
        assert_eq!(scorecard.lane_summaries.len(), TURSO_NATIVE_LANES.len());
        assert_eq!(
            scorecard.optional_decisions.len(),
            TURSO_OPTIONAL_DECISIONS.len()
        );
        assert_eq!(scorecard.totals.generated, 1_800);

        let json = scorecard.to_json().expect("serialize scorecard");
        let restored = TursoCampaignScorecard::from_json(&json).expect("restore scorecard");
        assert_eq!(restored, scorecard);
        assert_eq!(
            scorecard.render_bounded_summary().lines().count(),
            scorecard.lane_summaries.len() + 1
        );
    }

    #[test]
    fn turso_scorecard_rejects_count_imbalance_and_timeout_as_pass() {
        let mut input = passing_campaign_input(CampaignPromotionStage::Presubmit);
        input.runs[0].outcomes.executed = 98;
        input.runs[0].outcomes.timed_out = 1;
        let scorecard = evaluate_turso_campaign_gate(&input);
        let codes = campaign_diagnostic_codes(&scorecard);
        assert_eq!(scorecard.outcome, CampaignPromotionOutcome::Hold);
        assert!(codes.contains(&"outcome_count_imbalance"));
        assert!(codes.contains(&"timeout_is_not_pass"));
    }

    #[test]
    fn turso_scorecard_accepts_nonzero_seed_range_origins() {
        let mut input = passing_campaign_input(CampaignPromotionStage::Presubmit);
        input.runs[0].shard.start_seed = 4_200;
        input.runs[0].shard.end_seed_exclusive = 4_300;
        let scorecard = evaluate_turso_campaign_gate(&input);
        assert_eq!(scorecard.outcome, CampaignPromotionOutcome::Promote);
    }

    #[test]
    fn turso_scorecard_rejects_seed_gaps_and_count_mismatch() {
        let mut input = passing_campaign_input(CampaignPromotionStage::Presubmit);
        let mut second_shard = input.runs[0].clone();
        input.runs[0].shard = CampaignSeedShard {
            index: 0,
            count: 2,
            start_seed: 4_200,
            end_seed_exclusive: 4_250,
        };
        input.runs[0].outcomes.generated = 50;
        input.runs[0].outcomes.executed = 50;
        second_shard.shard = CampaignSeedShard {
            index: 1,
            count: 2,
            start_seed: 4_251,
            end_seed_exclusive: 4_301,
        };
        second_shard.outcomes.generated = 49;
        second_shard.outcomes.executed = 49;
        input.runs.push(second_shard);
        let scorecard = evaluate_turso_campaign_gate(&input);
        let codes = campaign_diagnostic_codes(&scorecard);
        assert!(codes.contains(&"seed_shard_gap_or_overlap"));
        assert!(codes.contains(&"seed_shard_outcome_count_mismatch"));
    }

    #[test]
    fn turso_scorecard_enforces_presubmit_seed_budget() {
        let mut input = passing_campaign_input(CampaignPromotionStage::Presubmit);
        input.runs[0].expected_seed_count = MAX_PRESUBMIT_SEEDS_PER_PROFILE + 1;
        input.runs[0].shard.end_seed_exclusive = MAX_PRESUBMIT_SEEDS_PER_PROFILE + 1;
        input.runs[0].outcomes.generated = MAX_PRESUBMIT_SEEDS_PER_PROFILE + 1;
        input.runs[0].outcomes.executed = MAX_PRESUBMIT_SEEDS_PER_PROFILE + 1;

        let scorecard = evaluate_turso_campaign_gate(&input);
        assert_eq!(scorecard.outcome, CampaignPromotionOutcome::Hold);
        assert!(campaign_diagnostic_codes(&scorecard).contains(&"presubmit_seed_budget_exceeded"));
    }

    #[test]
    fn turso_scorecard_rejects_placeholder_hashes_and_future_tiers() {
        let mut input = passing_campaign_input(CampaignPromotionStage::Presubmit);
        input.engine_sha = "0".repeat(40);
        input.contract_hash = "0".repeat(64);
        input.runs[0].artifacts[0].sha256 = "0".repeat(64);
        input.global_gate_receipts[0].artifact.sha256 = "0".repeat(64);
        input.runs.push(campaign_run(
            "bd-turso-test-adaptation-zu081.5",
            CampaignTier::Nightly,
        ));

        let scorecard = evaluate_turso_campaign_gate(&input);
        let codes = campaign_diagnostic_codes(&scorecard);
        assert_eq!(scorecard.outcome, CampaignPromotionOutcome::Hold);
        assert!(codes.contains(&"engine_sha_invalid"));
        assert!(codes.contains(&"provenance_hash_invalid"));
        assert!(codes.contains(&"artifact_provenance_invalid"));
        assert!(codes.contains(&"global_gate_provenance_invalid"));
        assert!(codes.contains(&"unexpected_tier_receipt"));
    }

    #[test]
    fn turso_scorecard_requires_linked_decision_for_skip_drift() {
        let mut input = passing_campaign_input(CampaignPromotionStage::Presubmit);
        input.runs[0].outcomes.executed = 98;
        input.runs[0].outcomes.unsupported = 1;
        input.runs[0].outcomes.skipped = 1;
        let scorecard = evaluate_turso_campaign_gate(&input);
        let codes = campaign_diagnostic_codes(&scorecard);
        assert!(codes.contains(&"unsupported_count_drift"));
        assert!(codes.contains(&"skipped_count_drift"));

        input.drift.linked_contract_decision = Some("bd-contract-decision".to_owned());
        let approved = evaluate_turso_campaign_gate(&input);
        assert_eq!(approved.outcome, CampaignPromotionOutcome::Promote);
    }

    #[test]
    fn turso_scorecard_never_promotes_cancelled_or_incomplete_exploration() {
        for status in [
            CampaignRunStatus::Cancelled,
            CampaignRunStatus::BudgetExhausted,
            CampaignRunStatus::IncompleteExploration,
        ] {
            let mut input = passing_campaign_input(CampaignPromotionStage::Presubmit);
            input.runs[0].status = status;
            let scorecard = evaluate_turso_campaign_gate(&input);
            assert_eq!(scorecard.outcome, CampaignPromotionOutcome::Hold);
            assert!(campaign_diagnostic_codes(&scorecard).contains(&"campaign_incomplete"));
        }
    }

    #[test]
    fn turso_scorecard_only_gates_admitted_optional_lanes() {
        let mut input = passing_campaign_input(CampaignPromotionStage::Presubmit);
        let deferred = evaluate_turso_campaign_gate(&input);
        assert_eq!(deferred.outcome, CampaignPromotionOutcome::Promote);

        input.optional_decisions[0].disposition = CampaignDisposition::Adopted;
        input.optional_decisions[0].admitted = true;
        let adopted_without_evidence = evaluate_turso_campaign_gate(&input);
        assert_eq!(
            adopted_without_evidence.outcome,
            CampaignPromotionOutcome::Hold
        );
        assert!(
            campaign_diagnostic_codes(&adopted_without_evidence)
                .contains(&"required_lane_tier_missing")
        );
    }

    #[test]
    fn turso_scorecard_bounds_optional_decision_rationales() {
        let mut input = passing_campaign_input(CampaignPromotionStage::Presubmit);
        input.optional_decisions[0].rationale = "x".repeat(MAX_OPTIONAL_RATIONALE_BYTES + 1);

        let scorecard = evaluate_turso_campaign_gate(&input);
        assert_eq!(scorecard.outcome, CampaignPromotionOutcome::Hold);
        assert!(
            campaign_diagnostic_codes(&scorecard).contains(&"optional_decision_rationale_too_long")
        );
    }

    #[test]
    fn turso_scorecard_requires_artifact_replay_and_global_invariants() {
        let mut input = passing_campaign_input(CampaignPromotionStage::Presubmit);
        input
            .global_gate_receipts
            .iter_mut()
            .find(|receipt| receipt.gate == CampaignGlobalGate::NoTokioDependency)
            .expect("no-Tokio gate receipt")
            .passed = false;
        input
            .global_gate_receipts
            .iter_mut()
            .find(|receipt| receipt.gate == CampaignGlobalGate::ConcurrentWriterDefaults)
            .expect("concurrent-default gate receipt")
            .passed = false;
        input.runs[0].public_replay_verified = false;
        input.runs[0].artifacts[0].retention_days = 0;
        let scorecard = evaluate_turso_campaign_gate(&input);
        let codes = campaign_diagnostic_codes(&scorecard);
        assert!(codes.contains(&"global_gate_failed"));
        assert!(codes.contains(&"public_replay_missing"));
        assert!(codes.contains(&"artifact_retention_too_short"));
    }

    #[test]
    fn turso_scorecard_separates_deterministic_and_observation_only_histories() {
        let mut input = passing_campaign_input(CampaignPromotionStage::Presubmit);
        let deterministic = input
            .runs
            .iter_mut()
            .find(|run| run.lane_id == "bd-turso-test-adaptation-zu081.8")
            .expect("deterministic history run");
        deterministic.history_accounting = HistoryAccounting::ObservationOnly;
        let multiprocess = input
            .runs
            .iter_mut()
            .find(|run| run.lane_id == "bd-turso-test-adaptation-zu081.9")
            .expect("multiprocess history run");
        multiprocess.history_accounting = HistoryAccounting::Deterministic;
        let history_reduction = input
            .runs
            .iter_mut()
            .find(|run| run.lane_id == "bd-turso-test-adaptation-zu081.19")
            .expect("deterministic history reduction run");
        history_reduction.history_accounting = HistoryAccounting::ObservationOnly;

        let scorecard = evaluate_turso_campaign_gate(&input);
        let codes = campaign_diagnostic_codes(&scorecard);
        assert!(codes.contains(&"production_history_not_deterministic"));
        assert!(codes.contains(&"multiprocess_history_misclassified"));
        assert!(codes.contains(&"history_reduction_not_deterministic"));
    }

    #[test]
    fn turso_scorecard_rejects_blank_and_missing_lane_coverage() {
        let mut input = passing_campaign_input(CampaignPromotionStage::Presubmit);
        let sql_reducer = input
            .runs
            .iter_mut()
            .find(|run| run.lane_id == "bd-turso-test-adaptation-zu081.6")
            .expect("SQL reducer run");
        sql_reducer.coverage.reducer_families.clear();
        let operation_plan = input
            .runs
            .iter_mut()
            .find(|run| run.lane_id == "bd-turso-test-adaptation-zu081.20")
            .expect("operation-plan run");
        operation_plan.coverage.constructs = vec!["   ".to_owned()];

        let scorecard = evaluate_turso_campaign_gate(&input);
        let codes = campaign_diagnostic_codes(&scorecard);
        assert_eq!(scorecard.outcome, CampaignPromotionOutcome::Hold);
        assert!(codes.contains(&"required_lane_coverage_missing"));
        assert!(codes.contains(&"coverage_dimension_value_invalid"));
    }

    #[test]
    fn turso_scorecard_summary_is_bounded_for_unknown_lane_spam() {
        let mut input = passing_campaign_input(CampaignPromotionStage::Presubmit);
        input.runs.push(campaign_run(
            &format!("aaaaaaaa-\n\r\t{}\u{2028}", "界".repeat(100)),
            CampaignTier::Presubmit,
        ));
        for index in 0..20 {
            let mut run = campaign_run(
                "unknown-lane-with-an-intentionally-overlong-identifier-that-must-not-expand-info-output-without-bound",
                CampaignTier::Presubmit,
            );
            run.lane_id.push_str(&format!("-{index:02}"));
            input.runs.push(run);
        }

        let scorecard = evaluate_turso_campaign_gate(&input);
        let summary = scorecard.render_bounded_summary();
        assert!(summary.lines().count() <= MAX_CAMPAIGN_SUMMARY_LANES + 2);
        assert!(summary.contains("lanes_omitted="));
        assert!(summary.lines().all(|line| line.len() < 240));
        assert!(
            !summary
                .chars()
                .any(|character| matches!(character, '\r' | '\t' | '\u{2028}'))
        );
    }
}
