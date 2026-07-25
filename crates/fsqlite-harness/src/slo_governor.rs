//! Shadow-mode Swarm SLO resource governor policy engine.
//!
//! The policy in this module is intentionally pure: callers provide a sampled
//! evidence vector, and the evaluator returns an explainable decision without
//! mutating engine state. Enforced rollout and signal adapters live in later
//! beads.

#![allow(clippy::struct_excessive_bools)]

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

/// Owning bead for the shadow policy engine.
pub const SWARM_SLO_POLICY_BEAD_ID: &str = "bd-swarm-slo-resource-governor-qb256.1";
/// Stable shadow policy identifier used in evidence artifacts.
pub const SWARM_SLO_POLICY_ID: &str = "swarm-slo-shadow.v1";
/// Machine-readable schema version for decisions emitted by this module.
pub const SWARM_SLO_DECISION_SCHEMA_VERSION: u32 = 1;
/// Owning bead for operator-facing SLO governor output.
pub const SWARM_SLO_OPERATOR_BEAD_ID: &str = "bd-swarm-slo-resource-governor-qb256.5";
/// Machine-readable schema version for operator reports emitted by this module.
pub const SWARM_SLO_OPERATOR_REPORT_SCHEMA_VERSION: u32 = 1;
/// Owning bead for enforced-mode rollout gating.
pub const SWARM_SLO_ROLLOUT_BEAD_ID: &str = "bd-swarm-slo-resource-governor-qb256.3";
/// Machine-readable schema version for enforced-mode rollout reports.
pub const SWARM_SLO_ENFORCEMENT_ROLLOUT_SCHEMA_VERSION: u32 = 1;
/// Stable rollout gate identifier used in evidence artifacts.
pub const SWARM_SLO_ENFORCEMENT_ROLLOUT_POLICY_ID: &str = "swarm-slo-enforcement-rollout.v1";

const PER_MILLE: u64 = 1_000;

/// Source of the sampled governor input.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SwarmSloSampleSource {
    /// Deterministic replay artifact or scorecard.
    Replay,
    /// Live harness run inside the test workspace.
    LiveHarness,
    /// Future production telemetry path.
    Production,
}

/// Control mode requested by the caller.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SwarmSloControlMode {
    /// Observe only. Decisions are recommendations.
    Shadow,
    /// Enforced mode is a later rollout bead. The policy can still label input.
    Enforced,
}

/// GC escalation tier copied from the E4.3 guardrail design.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SwarmSloGcTier {
    /// Normal timer-driven cleanup.
    Normal,
    /// More frequent background cleanup.
    Elevated,
    /// Cleanup after every commit.
    Urgent,
    /// Inline cleanup before commit.
    Critical,
}

/// Helper lane affected by a budget-shrink decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SwarmSloHelperLane {
    /// Replay/evidence lane.
    Evidence,
    /// Invalidation/coalescing lane.
    Invalidation,
    /// Agent build/test proof lane.
    AgentProof,
}

/// Recommendation emitted by the shadow policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum SwarmSloAction {
    /// Allow work to proceed normally.
    Admit,
    /// Park briefly before admitting work.
    Defer {
        /// Maximum park budget in microseconds.
        park_budget_us: u32,
    },
    /// Fail closed with backpressure.
    ApplyBackpressure,
    /// Reduce optional helper work in one lane.
    ShrinkHelperBudget {
        /// Lane whose helper budget should be reduced.
        target_lane: SwarmSloHelperLane,
    },
    /// Recommend safe mode. This does not disable concurrent-writer defaults.
    ForceSafeMode,
    /// Promote GC into the critical path for the current pressure event.
    TriggerEmergencyGc,
    /// Trigger a checkpoint to reduce WAL pressure.
    TriggerCheckpoint,
}

/// Missing or degraded signal annotations carried with every decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SwarmSloDegradedSignal {
    /// Replay scorecards do not yet expose p99.9 latency.
    MissingP999,
    /// Publish-window occupancy or p99 was unavailable.
    MissingPublishWindow,
    /// Live metrics were unavailable.
    MissingLiveMetrics,
    /// Sample age exceeded the stale-metrics threshold.
    StaleMetrics,
    /// Input came from replay only, not live telemetry.
    ReplayOnlyInput,
    /// Input labels were redacted for privacy.
    PrivacyRedactedInput,
}

/// Sampled evidence vector for one governor decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SwarmSloGovernorInput {
    /// Stable run identifier.
    pub run_id: String,
    /// Trace identifier.
    pub trace_id: String,
    /// Scenario identifier.
    pub scenario_id: String,
    /// Backend label, such as `frankensqlite_concurrent`.
    pub backend: String,
    /// Resource profile identifier.
    pub profile_id: String,
    /// Sample source.
    pub sample_source: SwarmSloSampleSource,
    /// Requested control mode.
    pub control_mode: SwarmSloControlMode,
    /// Optional artifact path for replay-derived input.
    pub artifact_path: Option<String>,
    /// Optional artifact hash for replay-derived input.
    pub artifact_hash: Option<String>,
    /// First replay or live diagnostic, or `none` when absent.
    pub first_failure_diag: String,
    /// Sample timestamp in milliseconds since caller-defined epoch.
    pub sample_ts_ms: u64,
    /// Sample age in milliseconds.
    pub sample_age_ms: u64,
    /// Sample window in milliseconds.
    pub sample_window_ms: u64,
    /// Actor count.
    pub actor_count: u32,
    /// Connection count.
    pub connection_count: u32,
    /// Writer count.
    pub writer_count: u32,
    /// Reader count.
    pub reader_count: u32,
    /// Statement count.
    pub statement_count: u64,
    /// Transaction count.
    pub transaction_count: u64,
    /// Concurrency level.
    pub concurrency_level: u32,
    /// Replay schedule seed or fingerprint.
    pub schedule_fingerprint: String,
    /// Available CPU cores.
    pub available_cores: u32,
    /// Configured helper threads.
    pub configured_helper_threads: u32,
    /// Active helper threads.
    pub active_helper_threads: u32,
    /// Memory limit for the resource profile.
    pub memory_limit_bytes: u64,
    /// Observed memory high-water mark.
    pub memory_high_water_bytes: u64,
    /// Page-cache footprint.
    pub page_cache_bytes: u64,
    /// CPU utilization in per-mille units.
    pub cpu_utilization_per_mille: u16,
    /// Active concurrent writers.
    pub active_writers: u32,
    /// Writers in the publish window, when measured.
    pub publish_window_occupancy: Option<u32>,
    /// Publish-window p99 in nanoseconds, when measured.
    pub publish_window_p99_ns: Option<u64>,
    /// Commit retry count.
    pub retry_count: u64,
    /// Retry rate in per-mille units.
    pub retry_rate_per_mille: u16,
    /// Abort count.
    pub abort_count: u64,
    /// Abort rate in per-mille units.
    pub abort_rate_per_mille: u16,
    /// Evidence queue depth.
    pub evidence_queue_depth: u32,
    /// Evidence queue drops in the sample window.
    pub evidence_queue_drops: u32,
    /// Evidence worker count.
    pub evidence_worker_count: u32,
    /// Wakeup queue depth.
    pub wakeup_queue_depth: u32,
    /// Wakeup-to-run p95 in nanoseconds.
    pub wakeup_to_run_p95_ns: Option<u64>,
    /// Wakeup-to-run p99 in nanoseconds.
    pub wakeup_to_run_p99_ns: Option<u64>,
    /// Maximum MVCC version-chain depth.
    pub max_chain_depth: u32,
    /// GC tier.
    pub gc_tier: SwarmSloGcTier,
    /// Whether inline GC is active.
    pub gc_inline_active: bool,
    /// WAL frames pending checkpoint.
    pub wal_frames_pending_checkpoint: u32,
    /// Whether a checkpoint is active.
    pub checkpoint_active: bool,
    /// Invalidation queue depth.
    pub invalidation_queue_depth: u32,
    /// Inline invalidation fallback count.
    pub invalidation_fallback_count: u32,
    /// Build/test saturation in per-mille units for proof lanes.
    pub build_or_test_saturation_per_mille: u16,
    /// Whether the operator path detected wedge risk.
    pub agent_wedge_risk: bool,
    /// User-visible p50 in nanoseconds.
    pub latency_p50_ns: u64,
    /// User-visible p95 in nanoseconds.
    pub latency_p95_ns: u64,
    /// User-visible p99 in nanoseconds.
    pub latency_p99_ns: u64,
    /// User-visible p99.9 in nanoseconds, when measured.
    pub latency_p999_ns: Option<u64>,
    /// Whether input labels were privacy-redacted.
    pub privacy_redacted: bool,
    /// Whether concurrent-writer default was observed as enabled.
    pub concurrent_mode_default_observed: bool,
    /// Degraded signals already known by the adapter.
    pub degraded_signals: Vec<SwarmSloDegradedSignal>,
}

impl SwarmSloGovernorInput {
    /// Build a healthy replay sample for deterministic tests and smoke adapters.
    pub fn healthy_replay(
        run_id: impl Into<String>,
        trace_id: impl Into<String>,
        scenario_id: impl Into<String>,
    ) -> Self {
        Self {
            run_id: run_id.into(),
            trace_id: trace_id.into(),
            scenario_id: scenario_id.into(),
            backend: "frankensqlite_concurrent".to_owned(),
            profile_id: "high_capacity_server".to_owned(),
            sample_source: SwarmSloSampleSource::Replay,
            control_mode: SwarmSloControlMode::Shadow,
            artifact_path: Some(
                "crates/fsqlite-harness/conformance/agent_swarm_trace_sanitized_golden.json"
                    .to_owned(),
            ),
            artifact_hash: None,
            first_failure_diag: "none".to_owned(),
            sample_ts_ms: 0,
            sample_age_ms: 0,
            sample_window_ms: 1_000,
            actor_count: 8,
            connection_count: 8,
            writer_count: 4,
            reader_count: 4,
            statement_count: 1_000,
            transaction_count: 100,
            concurrency_level: 8,
            schedule_fingerprint: "deterministic-smoke".to_owned(),
            available_cores: 64,
            configured_helper_threads: 3,
            active_helper_threads: 2,
            memory_limit_bytes: 256 * 1024 * 1024 * 1024,
            memory_high_water_bytes: 8 * 1024 * 1024,
            page_cache_bytes: 64 * 1024 * 1024,
            cpu_utilization_per_mille: 250,
            active_writers: 4,
            publish_window_occupancy: Some(1),
            publish_window_p99_ns: Some(1_000_000),
            retry_count: 0,
            retry_rate_per_mille: 0,
            abort_count: 0,
            abort_rate_per_mille: 0,
            evidence_queue_depth: 4,
            evidence_queue_drops: 0,
            evidence_worker_count: 1,
            wakeup_queue_depth: 0,
            wakeup_to_run_p95_ns: Some(100_000),
            wakeup_to_run_p99_ns: Some(150_000),
            max_chain_depth: 8,
            gc_tier: SwarmSloGcTier::Normal,
            gc_inline_active: false,
            wal_frames_pending_checkpoint: 128,
            checkpoint_active: false,
            invalidation_queue_depth: 0,
            invalidation_fallback_count: 0,
            build_or_test_saturation_per_mille: 0,
            agent_wedge_risk: false,
            latency_p50_ns: 1_000_000,
            latency_p95_ns: 2_000_000,
            latency_p99_ns: 4_000_000,
            latency_p999_ns: None,
            privacy_redacted: false,
            concurrent_mode_default_observed: true,
            degraded_signals: Vec::new(),
        }
    }
}

/// Tunable thresholds for the shadow policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SwarmSloGovernorConfig {
    /// Writer saturation threshold in per-mille units.
    pub writer_saturation_threshold_per_mille: u64,
    /// Publish-window contention threshold in per-mille units.
    pub publish_contention_threshold_per_mille: u64,
    /// p99/p50 tail-stress threshold in per-mille units.
    pub tail_stress_threshold_per_mille: u64,
    /// Retry-rate threshold in per-mille units.
    pub retry_rate_threshold_per_mille: u16,
    /// Abort-rate threshold in per-mille units.
    pub abort_rate_threshold_per_mille: u16,
    /// Evidence drops above this threshold shrink the evidence budget.
    pub evidence_drop_threshold: u32,
    /// Invalidation fallbacks above this threshold shrink invalidation budget.
    pub invalidation_fallback_threshold: u32,
    /// Chain depth above this threshold triggers emergency GC.
    pub max_chain_depth_critical: u32,
    /// WAL frames above this threshold trigger checkpoint if none is active.
    pub wal_frames_restart_threshold: u32,
    /// User p99 above this threshold enters safe mode.
    pub safe_mode_p99_threshold_ns: u64,
    /// Inline-GC publish p99 above this threshold enters safe mode.
    pub safe_mode_publish_window_threshold_ns: u64,
    /// Build/test saturation threshold for proof-lane helper shrink.
    pub build_test_saturation_threshold_per_mille: u16,
    /// Metrics older than this are marked stale.
    pub stale_metrics_after_ms: u64,
    /// Consecutive healthy samples required to exit safe mode.
    pub recovery_healthy_decisions: u32,
}

impl Default for SwarmSloGovernorConfig {
    fn default() -> Self {
        Self {
            writer_saturation_threshold_per_mille: 950,
            publish_contention_threshold_per_mille: 500,
            tail_stress_threshold_per_mille: 15_000,
            retry_rate_threshold_per_mille: 300,
            abort_rate_threshold_per_mille: 200,
            evidence_drop_threshold: 100,
            invalidation_fallback_threshold: 10,
            max_chain_depth_critical: 256,
            wal_frames_restart_threshold: 10_000,
            safe_mode_p99_threshold_ns: 100_000_000,
            safe_mode_publish_window_threshold_ns: 10_000_000,
            build_test_saturation_threshold_per_mille: 900,
            stale_metrics_after_ms: 5_000,
            recovery_healthy_decisions: 3,
        }
    }
}

/// Stateful hysteresis for safe-mode exit.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SwarmSloGovernorState {
    next_decision_id: u64,
    safe_mode_active: bool,
    healthy_safe_mode_exit_samples: u32,
}

impl SwarmSloGovernorState {
    /// Whether safe mode is currently active.
    pub const fn safe_mode_active(&self) -> bool {
        self.safe_mode_active
    }

    /// Next monotonic decision identifier.
    pub const fn next_decision_id(&self) -> u64 {
        self.next_decision_id
    }
}

/// Derived pressure signals copied into each decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SwarmSloDerivedSignals {
    /// Writer saturation in per-mille units.
    pub writer_saturation_per_mille: u64,
    /// Publish-window contention in per-mille units, when available.
    pub publish_contention_per_mille: Option<u64>,
    /// p99/p50 tail stress in per-mille units.
    pub tail_stress_per_mille: u64,
}

/// Explainable shadow governor decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SwarmSloDecision {
    /// Decision schema version.
    pub schema_version: u32,
    /// Owning bead identifier.
    pub bead_id: String,
    /// Policy version.
    pub policy_id: String,
    /// Monotonic decision identifier.
    pub decision_id: u64,
    /// Matched guardrail or Swarm SLO rule.
    pub guardrail_id: String,
    /// Recommended action.
    pub action: SwarmSloAction,
    /// Caller-requested control mode.
    pub control_mode: SwarmSloControlMode,
    /// Full evidence snapshot used for the decision.
    pub evidence: SwarmSloGovernorInput,
    /// Derived pressure signals.
    pub derived: SwarmSloDerivedSignals,
    /// Decision counterfactual.
    pub counterfactual: String,
    /// Regret/risk explanation for the chosen action.
    pub regret: String,
    /// Degraded signals observed or inferred for the decision.
    pub degraded_signals: BTreeSet<SwarmSloDegradedSignal>,
    /// Observed concurrent-mode default at decision time.
    pub concurrent_mode_default_observed: bool,
    /// Safe-mode state after applying the decision.
    pub safe_mode_active_after: bool,
}

/// Status of one operator-visible SLO budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SwarmSloOperatorBudgetStatus {
    /// The metric is comfortably inside its configured limit.
    Within,
    /// The metric is inside the limit but close enough to deserve attention.
    NearLimit,
    /// The metric exceeded its configured limit.
    OverLimit,
    /// The metric could not be measured in the current sample.
    Unmeasured,
}

/// Fallback risk level surfaced to operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SwarmSloFallbackPathRisk {
    /// No fallback or degraded signal was observed.
    NoneObserved,
    /// No compatibility fallback was observed, but signal quality is degraded.
    DegradedSignalsOnly,
    /// A compatibility fallback or first-failure diagnostic was observed.
    CompatibilityFallbackObserved,
    /// The concurrent-writer default invariant was not observed.
    ConcurrentDefaultBroken,
}

/// One SLO budget row in the operator report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SwarmSloOperatorBudget {
    /// Stable metric name.
    pub metric: String,
    /// Observed value, or `None` if the metric was not measured.
    pub observed: Option<u64>,
    /// Configured limit for this metric.
    pub limit: u64,
    /// Unit for both `observed` and `limit`.
    pub unit: String,
    /// Budget status.
    pub status: SwarmSloOperatorBudgetStatus,
    /// Operator-facing explanation.
    pub guidance: String,
}

/// Evidence identity copied into operator output.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SwarmSloOperatorEvidenceContext {
    /// Stable run identifier.
    pub run_id: String,
    /// Trace identifier.
    pub trace_id: String,
    /// Scenario identifier.
    pub scenario_id: String,
    /// Backend label.
    pub backend: String,
    /// Resource profile identifier.
    pub profile_id: String,
    /// Sample source.
    pub sample_source: SwarmSloSampleSource,
    /// Optional artifact path.
    pub artifact_path: Option<String>,
    /// Optional artifact hash.
    pub artifact_hash: Option<String>,
    /// First failure diagnostic, or `none`.
    pub first_failure_diag: String,
}

/// Deterministic operator-facing report for one SLO governor decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SwarmSloOperatorReport {
    /// Report schema version.
    pub schema_version: u32,
    /// Owning bead identifier.
    pub bead_id: String,
    /// Policy version that produced the decision.
    pub policy_id: String,
    /// Decision identifier from the policy engine.
    pub decision_id: u64,
    /// Matched guardrail.
    pub guardrail_id: String,
    /// Caller-requested control mode.
    pub control_mode: SwarmSloControlMode,
    /// Recommended action.
    pub action: SwarmSloAction,
    /// Short action summary for CLIs and doctor output.
    pub action_summary: String,
    /// Evidence identity.
    pub evidence: SwarmSloOperatorEvidenceContext,
    /// Current SLO budget rows.
    pub budgets: Vec<SwarmSloOperatorBudget>,
    /// Active recommendations and caveats.
    pub active_recommendations: Vec<String>,
    /// Degraded signal-quality annotations.
    pub degraded_signal_quality: Vec<SwarmSloDegradedSignal>,
    /// Fallback-path risk classification.
    pub fallback_path_risk: SwarmSloFallbackPathRisk,
    /// Compatibility fallback summary.
    pub compatibility_fallback_summary: String,
    /// Operator kill-switch guidance.
    pub kill_switches: Vec<String>,
    /// Safe-mode guidance.
    pub safe_mode_guidance: String,
    /// Guardrail against uncited performance claims.
    pub measurement_guardrail: String,
}

/// Strictness knobs for enforced-mode rollout gating.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SwarmSloEnforcementRolloutConfig {
    /// Require a caller-supplied opt-in before allowing enforcement.
    pub require_explicit_opt_in: bool,
    /// Require live or production telemetry rather than replay-only evidence.
    pub require_live_metrics: bool,
    /// Require publish-window occupancy and latency measurements.
    pub require_publish_window_metrics: bool,
    /// Require p99.9 telemetry before enforcement.
    pub require_p999_latency: bool,
    /// Require proof-pack hash evidence.
    pub require_proof_pack_hash: bool,
    /// Require a dated benchmark artifact path and commit for performance claims.
    pub require_benchmark_artifact: bool,
    /// Allow degraded telemetry to reach enforcement.
    pub allow_degraded_telemetry: bool,
    /// Allow compatibility fallback observations to reach enforcement.
    pub allow_compatibility_fallback: bool,
    /// Forbid any SQLite-style serialized-writer fallback request.
    pub forbid_serialized_writer_fallback: bool,
}

impl Default for SwarmSloEnforcementRolloutConfig {
    fn default() -> Self {
        Self {
            require_explicit_opt_in: true,
            require_live_metrics: true,
            require_publish_window_metrics: true,
            require_p999_latency: true,
            require_proof_pack_hash: true,
            require_benchmark_artifact: true,
            allow_degraded_telemetry: false,
            allow_compatibility_fallback: false,
            forbid_serialized_writer_fallback: true,
        }
    }
}

/// Artifact evidence required before promoting SLO governor enforcement.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SwarmSloEnforcementRolloutEvidence {
    /// Hash of the replay/stress proof pack.
    pub proof_pack_hash: Option<String>,
    /// Whether governor-off and governor-shadow evidence was compared.
    pub governor_off_shadow_compared: bool,
    /// Dated benchmark artifact path supporting any performance claim.
    pub benchmark_artifact_path: Option<String>,
    /// Commit used for the benchmark artifact.
    pub benchmark_artifact_commit: Option<String>,
    /// Date attached to the benchmark artifact.
    pub benchmark_artifact_date: Option<String>,
    /// CI-sized smoke command for reproducing the rollout evidence.
    pub smoke_command: Option<String>,
    /// Heavy replay command, which must use `rch`.
    pub heavy_rch_command: Option<String>,
}

/// Caller intent and operator switches for one enforcement gate evaluation.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SwarmSloEnforcementRolloutRequest {
    /// Explicit operator opt-in to enforced mode.
    pub explicit_opt_in: bool,
    /// Operator kill switch forcing non-enforcement.
    pub operator_kill_switch_active: bool,
    /// Whether a caller requested SQLite-style serialized writer fallback.
    pub serialized_writer_fallback_requested: bool,
    /// Evidence bundle for this rollout decision.
    pub evidence: SwarmSloEnforcementRolloutEvidence,
}

/// Final rollout gate verdict.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SwarmSloEnforcementRolloutVerdict {
    /// Enforced mode may proceed for this evidence snapshot.
    Enforce,
    /// Keep the policy in shadow mode and continue collecting evidence.
    DowngradeToShadow,
    /// Do not enforce until hard blockers are removed.
    Blocked,
}

/// Machine-readable reason for a rollout block or shadow downgrade.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SwarmSloEnforcementRolloutReason {
    /// Enforced mode was not requested on the decision input.
    EnforcedModeNotRequested,
    /// The operator did not explicitly opt in.
    ExplicitOptInMissing,
    /// A kill switch is active.
    KillSwitchActive,
    /// The concurrent-writer default invariant was not observed.
    ConcurrentDefaultNotObserved,
    /// SQLite-style serialized writer fallback was requested.
    SerializedWriterFallbackRequested,
    /// Operator report and decision identity did not match.
    OperatorReportMismatch,
    /// Proof-pack hash was absent or not a SHA-256 hex string.
    ProofPackHashMissing,
    /// Governor-off and governor-shadow evidence was not compared.
    GovernorOffShadowComparisonMissing,
    /// Benchmark artifact path was missing.
    BenchmarkArtifactMissing,
    /// Benchmark artifact commit was missing.
    BenchmarkArtifactCommitMissing,
    /// Benchmark artifact date was missing.
    BenchmarkArtifactDateMissing,
    /// Live metrics were missing.
    MissingLiveMetrics,
    /// Metrics were stale.
    StaleMetrics,
    /// Evidence was replay-only.
    ReplayOnlyInput,
    /// Publish-window measurements were missing.
    MissingPublishWindow,
    /// p99.9 latency was missing.
    MissingP999,
    /// Labels or values were privacy redacted.
    PrivacyRedactedInput,
    /// A compatibility fallback was observed.
    CompatibilityFallbackObserved,
}

/// Deterministic report explaining an enforced-mode rollout gate decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SwarmSloEnforcementRolloutReport {
    /// Report schema version.
    pub schema_version: u32,
    /// Owning bead identifier.
    pub bead_id: String,
    /// Rollout policy identifier.
    pub rollout_policy_id: String,
    /// Shadow policy identifier that produced the decision.
    pub policy_id: String,
    /// Decision identifier from the governor.
    pub decision_id: u64,
    /// Matched guardrail from the governor.
    pub guardrail_id: String,
    /// Requested control mode from the decision.
    pub requested_control_mode: SwarmSloControlMode,
    /// Effective control mode after rollout gating.
    pub effective_control_mode: SwarmSloControlMode,
    /// Gate verdict.
    pub verdict: SwarmSloEnforcementRolloutVerdict,
    /// Recommended governor action.
    pub action: SwarmSloAction,
    /// Hard blockers that must be removed before enforcement.
    pub blockers: Vec<SwarmSloEnforcementRolloutReason>,
    /// Softer evidence gaps that downgrade enforcement to shadow mode.
    pub downgrade_reasons: Vec<SwarmSloEnforcementRolloutReason>,
    /// Whether the concurrent-writer default was observed as enabled.
    pub concurrent_mode_default_observed: bool,
    /// Whether SQLite-style serialized writer fallback was requested.
    pub serialized_writer_fallback_requested: bool,
    /// Whether an operator kill switch was active.
    pub operator_kill_switch_active: bool,
    /// Proof-pack hash used by the gate.
    pub proof_pack_hash: Option<String>,
    /// Benchmark artifact path used by the gate.
    pub benchmark_artifact_path: Option<String>,
    /// Benchmark artifact commit used by the gate.
    pub benchmark_artifact_commit: Option<String>,
    /// Benchmark artifact date used by the gate.
    pub benchmark_artifact_date: Option<String>,
    /// Smoke command for reproducing the gate evidence.
    pub smoke_command: Option<String>,
    /// Heavy command for reproducing the gate evidence through `rch`.
    pub heavy_rch_command: Option<String>,
    /// Operator-visible kill switches copied from the operator report.
    pub kill_switches: Vec<String>,
    /// Guardrail against uncited performance claims.
    pub measurement_guardrail: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RuleMatch {
    guardrail_id: &'static str,
    action: SwarmSloAction,
    counterfactual: &'static str,
    regret: &'static str,
}

/// Evaluate one sample without preserving hysteresis between calls.
pub fn evaluate_swarm_slo_once(input: &SwarmSloGovernorInput) -> SwarmSloDecision {
    let mut state = SwarmSloGovernorState::default();
    evaluate_swarm_slo(input, &SwarmSloGovernorConfig::default(), &mut state)
}

/// Evaluate one sample with explicit config and state.
pub fn evaluate_swarm_slo(
    input: &SwarmSloGovernorInput,
    config: &SwarmSloGovernorConfig,
    state: &mut SwarmSloGovernorState,
) -> SwarmSloDecision {
    let decision_id = state.next_decision_id;
    state.next_decision_id = state.next_decision_id.saturating_add(1);

    let degraded_signals = normalize_degraded_signals(input, config);
    let derived = derive_signals(input);
    let raw_match = match_rule(input, config, &derived);
    let selected = apply_safe_mode_hysteresis(raw_match, state, input, config);

    SwarmSloDecision {
        schema_version: SWARM_SLO_DECISION_SCHEMA_VERSION,
        bead_id: SWARM_SLO_POLICY_BEAD_ID.to_owned(),
        policy_id: SWARM_SLO_POLICY_ID.to_owned(),
        decision_id,
        guardrail_id: selected.guardrail_id.to_owned(),
        action: selected.action,
        control_mode: input.control_mode,
        evidence: input.clone(),
        derived,
        counterfactual: selected.counterfactual.to_owned(),
        regret: selected.regret.to_owned(),
        degraded_signals,
        concurrent_mode_default_observed: input.concurrent_mode_default_observed,
        safe_mode_active_after: state.safe_mode_active,
    }
}

/// Build deterministic operator output from an evaluated governor decision.
#[must_use]
pub fn build_swarm_slo_operator_report(
    decision: &SwarmSloDecision,
    config: &SwarmSloGovernorConfig,
) -> SwarmSloOperatorReport {
    SwarmSloOperatorReport {
        schema_version: SWARM_SLO_OPERATOR_REPORT_SCHEMA_VERSION,
        bead_id: SWARM_SLO_OPERATOR_BEAD_ID.to_owned(),
        policy_id: decision.policy_id.clone(),
        decision_id: decision.decision_id,
        guardrail_id: decision.guardrail_id.clone(),
        control_mode: decision.control_mode,
        action: decision.action,
        action_summary: operator_action_summary(decision.action),
        evidence: operator_evidence_context(decision),
        budgets: operator_budgets(decision, config),
        active_recommendations: active_operator_recommendations(decision),
        degraded_signal_quality: decision.degraded_signals.iter().copied().collect(),
        fallback_path_risk: fallback_path_risk(decision),
        compatibility_fallback_summary: compatibility_fallback_summary(decision),
        kill_switches: operator_kill_switches(decision),
        safe_mode_guidance: safe_mode_guidance(decision, config),
        measurement_guardrail: "This report only describes observed budgets and guardrails; it does not claim performance improvement without benchmark artifacts.".to_owned(),
    }
}

/// Gate a shadow-policy decision before allowing enforced-mode rollout.
#[must_use]
pub fn gate_swarm_slo_enforcement_rollout(
    decision: &SwarmSloDecision,
    operator_report: &SwarmSloOperatorReport,
    request: &SwarmSloEnforcementRolloutRequest,
    config: &SwarmSloEnforcementRolloutConfig,
) -> SwarmSloEnforcementRolloutReport {
    let blockers = rollout_blockers(decision, operator_report, request, config);
    let downgrade_reasons = rollout_downgrade_reasons(decision, operator_report, config);
    let verdict = rollout_verdict(&blockers, &downgrade_reasons);
    let effective_control_mode = if verdict == SwarmSloEnforcementRolloutVerdict::Enforce {
        SwarmSloControlMode::Enforced
    } else {
        SwarmSloControlMode::Shadow
    };

    SwarmSloEnforcementRolloutReport {
        schema_version: SWARM_SLO_ENFORCEMENT_ROLLOUT_SCHEMA_VERSION,
        bead_id: SWARM_SLO_ROLLOUT_BEAD_ID.to_owned(),
        rollout_policy_id: SWARM_SLO_ENFORCEMENT_ROLLOUT_POLICY_ID.to_owned(),
        policy_id: decision.policy_id.clone(),
        decision_id: decision.decision_id,
        guardrail_id: decision.guardrail_id.clone(),
        requested_control_mode: decision.control_mode,
        effective_control_mode,
        verdict,
        action: decision.action,
        blockers,
        downgrade_reasons,
        concurrent_mode_default_observed: decision.concurrent_mode_default_observed,
        serialized_writer_fallback_requested: request.serialized_writer_fallback_requested,
        operator_kill_switch_active: request.operator_kill_switch_active,
        proof_pack_hash: request.evidence.proof_pack_hash.clone(),
        benchmark_artifact_path: request.evidence.benchmark_artifact_path.clone(),
        benchmark_artifact_commit: request.evidence.benchmark_artifact_commit.clone(),
        benchmark_artifact_date: request.evidence.benchmark_artifact_date.clone(),
        smoke_command: request.evidence.smoke_command.clone(),
        heavy_rch_command: request.evidence.heavy_rch_command.clone(),
        kill_switches: operator_report.kill_switches.clone(),
        measurement_guardrail: "Enforced rollout is allowed only with explicit opt-in, current non-degraded telemetry, concurrent-writer-default evidence, no serialized-writer fallback, and dated benchmark/proof-pack artifacts."
            .to_owned(),
    }
}

fn rollout_blockers(
    decision: &SwarmSloDecision,
    operator_report: &SwarmSloOperatorReport,
    request: &SwarmSloEnforcementRolloutRequest,
    config: &SwarmSloEnforcementRolloutConfig,
) -> Vec<SwarmSloEnforcementRolloutReason> {
    let mut blockers = Vec::new();

    if config.require_explicit_opt_in && !request.explicit_opt_in {
        blockers.push(SwarmSloEnforcementRolloutReason::ExplicitOptInMissing);
    }

    if request.operator_kill_switch_active {
        blockers.push(SwarmSloEnforcementRolloutReason::KillSwitchActive);
    }

    if !decision.concurrent_mode_default_observed {
        blockers.push(SwarmSloEnforcementRolloutReason::ConcurrentDefaultNotObserved);
    }

    if config.forbid_serialized_writer_fallback && request.serialized_writer_fallback_requested {
        blockers.push(SwarmSloEnforcementRolloutReason::SerializedWriterFallbackRequested);
    }

    if operator_report_mismatches_decision(operator_report, decision) {
        blockers.push(SwarmSloEnforcementRolloutReason::OperatorReportMismatch);
    }

    if config.require_proof_pack_hash
        && !request
            .evidence
            .proof_pack_hash
            .as_deref()
            .is_some_and(looks_like_sha256_hex)
    {
        blockers.push(SwarmSloEnforcementRolloutReason::ProofPackHashMissing);
    }

    if !request.evidence.governor_off_shadow_compared {
        blockers.push(SwarmSloEnforcementRolloutReason::GovernorOffShadowComparisonMissing);
    }

    if config.require_benchmark_artifact {
        if string_option_is_blank(request.evidence.benchmark_artifact_path.as_deref()) {
            blockers.push(SwarmSloEnforcementRolloutReason::BenchmarkArtifactMissing);
        }
        if string_option_is_blank(request.evidence.benchmark_artifact_commit.as_deref()) {
            blockers.push(SwarmSloEnforcementRolloutReason::BenchmarkArtifactCommitMissing);
        }
        if string_option_is_blank(request.evidence.benchmark_artifact_date.as_deref()) {
            blockers.push(SwarmSloEnforcementRolloutReason::BenchmarkArtifactDateMissing);
        }
    }

    blockers
}

fn rollout_downgrade_reasons(
    decision: &SwarmSloDecision,
    operator_report: &SwarmSloOperatorReport,
    config: &SwarmSloEnforcementRolloutConfig,
) -> Vec<SwarmSloEnforcementRolloutReason> {
    let mut reasons = Vec::new();

    if decision.control_mode != SwarmSloControlMode::Enforced {
        reasons.push(SwarmSloEnforcementRolloutReason::EnforcedModeNotRequested);
    }

    if !config.allow_degraded_telemetry {
        push_degraded_rollout_reasons(decision, config, &mut reasons);
    }

    if !config.allow_compatibility_fallback
        && operator_report.fallback_path_risk
            == SwarmSloFallbackPathRisk::CompatibilityFallbackObserved
    {
        reasons.push(SwarmSloEnforcementRolloutReason::CompatibilityFallbackObserved);
    }

    reasons
}

fn rollout_verdict(
    blockers: &[SwarmSloEnforcementRolloutReason],
    downgrade_reasons: &[SwarmSloEnforcementRolloutReason],
) -> SwarmSloEnforcementRolloutVerdict {
    if !blockers.is_empty() {
        SwarmSloEnforcementRolloutVerdict::Blocked
    } else if !downgrade_reasons.is_empty() {
        SwarmSloEnforcementRolloutVerdict::DowngradeToShadow
    } else {
        SwarmSloEnforcementRolloutVerdict::Enforce
    }
}

fn push_degraded_rollout_reasons(
    decision: &SwarmSloDecision,
    config: &SwarmSloEnforcementRolloutConfig,
    reasons: &mut Vec<SwarmSloEnforcementRolloutReason>,
) {
    if config.require_live_metrics {
        push_if_degraded(
            decision,
            SwarmSloDegradedSignal::MissingLiveMetrics,
            SwarmSloEnforcementRolloutReason::MissingLiveMetrics,
            reasons,
        );
        push_if_degraded(
            decision,
            SwarmSloDegradedSignal::ReplayOnlyInput,
            SwarmSloEnforcementRolloutReason::ReplayOnlyInput,
            reasons,
        );
    }

    if config.require_publish_window_metrics {
        push_if_degraded(
            decision,
            SwarmSloDegradedSignal::MissingPublishWindow,
            SwarmSloEnforcementRolloutReason::MissingPublishWindow,
            reasons,
        );
    }

    if config.require_p999_latency {
        push_if_degraded(
            decision,
            SwarmSloDegradedSignal::MissingP999,
            SwarmSloEnforcementRolloutReason::MissingP999,
            reasons,
        );
    }

    push_if_degraded(
        decision,
        SwarmSloDegradedSignal::StaleMetrics,
        SwarmSloEnforcementRolloutReason::StaleMetrics,
        reasons,
    );
    push_if_degraded(
        decision,
        SwarmSloDegradedSignal::PrivacyRedactedInput,
        SwarmSloEnforcementRolloutReason::PrivacyRedactedInput,
        reasons,
    );
}

fn push_if_degraded(
    decision: &SwarmSloDecision,
    signal: SwarmSloDegradedSignal,
    reason: SwarmSloEnforcementRolloutReason,
    reasons: &mut Vec<SwarmSloEnforcementRolloutReason>,
) {
    if decision.degraded_signals.contains(&signal) {
        reasons.push(reason);
    }
}

fn operator_report_mismatches_decision(
    operator_report: &SwarmSloOperatorReport,
    decision: &SwarmSloDecision,
) -> bool {
    operator_report.policy_id != decision.policy_id
        || operator_report.decision_id != decision.decision_id
        || operator_report.guardrail_id != decision.guardrail_id
        || operator_report.control_mode != decision.control_mode
        || operator_report.action != decision.action
}

fn looks_like_sha256_hex(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn string_option_is_blank(value: Option<&str>) -> bool {
    value.is_none_or(|inner| inner.trim().is_empty())
}

fn operator_evidence_context(decision: &SwarmSloDecision) -> SwarmSloOperatorEvidenceContext {
    let evidence = &decision.evidence;

    SwarmSloOperatorEvidenceContext {
        run_id: evidence.run_id.clone(),
        trace_id: evidence.trace_id.clone(),
        scenario_id: evidence.scenario_id.clone(),
        backend: evidence.backend.clone(),
        profile_id: evidence.profile_id.clone(),
        sample_source: evidence.sample_source,
        artifact_path: evidence.artifact_path.clone(),
        artifact_hash: evidence.artifact_hash.clone(),
        first_failure_diag: evidence.first_failure_diag.clone(),
    }
}

fn operator_budgets(
    decision: &SwarmSloDecision,
    config: &SwarmSloGovernorConfig,
) -> Vec<SwarmSloOperatorBudget> {
    let evidence = &decision.evidence;
    let derived = &decision.derived;

    vec![
        operator_budget(
            "writer_saturation_per_mille",
            Some(derived.writer_saturation_per_mille),
            config.writer_saturation_threshold_per_mille,
            "per_mille",
            "active writers per available core",
        ),
        operator_budget(
            "publish_contention_per_mille",
            derived.publish_contention_per_mille,
            config.publish_contention_threshold_per_mille,
            "per_mille",
            "publish-window occupancy per available core",
        ),
        operator_budget(
            "tail_stress_per_mille",
            Some(derived.tail_stress_per_mille),
            config.tail_stress_threshold_per_mille,
            "per_mille",
            "p99 latency divided by p50 latency",
        ),
        operator_budget(
            "retry_rate_per_mille",
            Some(u64::from(evidence.retry_rate_per_mille)),
            u64::from(config.retry_rate_threshold_per_mille),
            "per_mille",
            "commit retry rate in the sample window",
        ),
        operator_budget(
            "abort_rate_per_mille",
            Some(u64::from(evidence.abort_rate_per_mille)),
            u64::from(config.abort_rate_threshold_per_mille),
            "per_mille",
            "transaction abort rate in the sample window",
        ),
        operator_budget(
            "evidence_queue_drops",
            Some(u64::from(evidence.evidence_queue_drops)),
            u64::from(config.evidence_drop_threshold),
            "count",
            "dropped evidence samples",
        ),
        operator_budget(
            "invalidation_fallback_count",
            Some(u64::from(evidence.invalidation_fallback_count)),
            u64::from(config.invalidation_fallback_threshold),
            "count",
            "inline invalidation compatibility fallbacks",
        ),
        operator_budget(
            "max_chain_depth",
            Some(u64::from(evidence.max_chain_depth)),
            u64::from(config.max_chain_depth_critical),
            "versions",
            "maximum MVCC version-chain depth",
        ),
        operator_budget(
            "wal_frames_pending_checkpoint",
            Some(u64::from(evidence.wal_frames_pending_checkpoint)),
            u64::from(config.wal_frames_restart_threshold),
            "frames",
            "WAL frames pending checkpoint",
        ),
        operator_budget(
            "latency_p99_ns",
            Some(evidence.latency_p99_ns),
            config.safe_mode_p99_threshold_ns,
            "ns",
            "user-visible p99 latency",
        ),
        operator_budget(
            "build_or_test_saturation_per_mille",
            Some(u64::from(evidence.build_or_test_saturation_per_mille)),
            u64::from(config.build_test_saturation_threshold_per_mille),
            "per_mille",
            "agent proof/build/test saturation",
        ),
    ]
}

fn operator_budget(
    metric: &str,
    observed: Option<u64>,
    limit: u64,
    unit: &str,
    guidance: &str,
) -> SwarmSloOperatorBudget {
    SwarmSloOperatorBudget {
        metric: metric.to_owned(),
        observed,
        limit,
        unit: unit.to_owned(),
        status: operator_budget_status(observed, limit),
        guidance: guidance.to_owned(),
    }
}

fn operator_budget_status(observed: Option<u64>, limit: u64) -> SwarmSloOperatorBudgetStatus {
    let Some(observed) = observed else {
        return SwarmSloOperatorBudgetStatus::Unmeasured;
    };

    if observed > limit {
        SwarmSloOperatorBudgetStatus::OverLimit
    } else if observed.saturating_mul(10) >= limit.saturating_mul(9) {
        SwarmSloOperatorBudgetStatus::NearLimit
    } else {
        SwarmSloOperatorBudgetStatus::Within
    }
}

fn active_operator_recommendations(decision: &SwarmSloDecision) -> Vec<String> {
    let mut recommendations = vec![
        operator_action_summary(decision.action),
        format!(
            "guardrail {} matched; {}",
            decision.guardrail_id, decision.counterfactual
        ),
        format!("operator regret: {}", decision.regret),
    ];

    if !decision.concurrent_mode_default_observed {
        recommendations.push(
            "stop rollout and inspect concurrent-mode defaults before trusting governor output"
                .to_owned(),
        );
    }

    if !decision.degraded_signals.is_empty() {
        recommendations.push(
            "treat this as degraded signal quality; do not promote to enforced mode from this sample alone"
                .to_owned(),
        );
    }

    if decision.control_mode == SwarmSloControlMode::Enforced {
        recommendations.push(
            "enforced mode was requested, but this operator report only explains the decision"
                .to_owned(),
        );
    }

    recommendations
}

fn operator_action_summary(action: SwarmSloAction) -> String {
    match action {
        SwarmSloAction::Admit => "admit foreground work and continue shadow monitoring".to_owned(),
        SwarmSloAction::Defer { park_budget_us } => {
            format!("defer admission for up to {park_budget_us}us")
        }
        SwarmSloAction::ApplyBackpressure => {
            "apply fail-closed backpressure until the guardrail clears".to_owned()
        }
        SwarmSloAction::ShrinkHelperBudget { target_lane } => {
            format!(
                "shrink optional {} helper work before foreground work",
                helper_lane_name(target_lane)
            )
        }
        SwarmSloAction::ForceSafeMode => {
            "enter or hold safe mode without disabling concurrent writers".to_owned()
        }
        SwarmSloAction::TriggerEmergencyGc => {
            "trigger emergency GC for the current pressure event".to_owned()
        }
        SwarmSloAction::TriggerCheckpoint => {
            "trigger checkpoint pressure relief if no checkpoint is active".to_owned()
        }
    }
}

const fn helper_lane_name(lane: SwarmSloHelperLane) -> &'static str {
    match lane {
        SwarmSloHelperLane::Evidence => "evidence",
        SwarmSloHelperLane::Invalidation => "invalidation",
        SwarmSloHelperLane::AgentProof => "agent-proof",
    }
}

fn fallback_path_risk(decision: &SwarmSloDecision) -> SwarmSloFallbackPathRisk {
    let evidence = &decision.evidence;

    if !decision.concurrent_mode_default_observed {
        SwarmSloFallbackPathRisk::ConcurrentDefaultBroken
    } else if evidence.invalidation_fallback_count > 0
        || first_failure_diag_is_present(&evidence.first_failure_diag)
    {
        SwarmSloFallbackPathRisk::CompatibilityFallbackObserved
    } else if decision.degraded_signals.is_empty() {
        SwarmSloFallbackPathRisk::NoneObserved
    } else {
        SwarmSloFallbackPathRisk::DegradedSignalsOnly
    }
}

fn compatibility_fallback_summary(decision: &SwarmSloDecision) -> String {
    let evidence = &decision.evidence;

    if evidence.invalidation_fallback_count > 0 {
        return format!(
            "{} inline invalidation fallback event(s) observed; compatibility fallback is visible in this report",
            evidence.invalidation_fallback_count
        );
    }

    if first_failure_diag_is_present(&evidence.first_failure_diag) {
        return format!(
            "first-failure diagnostic is present and must be reviewed before rollout: {}",
            evidence.first_failure_diag
        );
    }

    if decision.degraded_signals.is_empty() {
        "no compatibility fallback or degraded signal was observed in this sample".to_owned()
    } else {
        "no compatibility fallback event was observed, but degraded telemetry is listed explicitly"
            .to_owned()
    }
}

fn first_failure_diag_is_present(diag: &str) -> bool {
    let trimmed = diag.trim();
    !trimmed.is_empty() && trimmed != "none"
}

fn operator_kill_switches(decision: &SwarmSloDecision) -> Vec<String> {
    let mut switches = vec![
        "shadow_mode_only: keep control_mode=shadow so actions remain recommendations".to_owned(),
        "safe_mode: suppress optional helper pressure while concurrent-writer defaults stay enabled"
            .to_owned(),
        "helper_budget_shrink: reduce evidence, invalidation, or agent-proof helper lanes before foreground work"
            .to_owned(),
    ];

    if matches!(
        decision.action,
        SwarmSloAction::ApplyBackpressure | SwarmSloAction::ForceSafeMode
    ) {
        switches.push(
            "admission_backpressure: fail closed instead of admitting unbounded pressure"
                .to_owned(),
        );
    }

    switches
}

fn safe_mode_guidance(decision: &SwarmSloDecision, config: &SwarmSloGovernorConfig) -> String {
    if !decision.concurrent_mode_default_observed {
        return "do not use safe mode as a compatibility shim; restore the concurrent-writer default before rollout"
            .to_owned();
    }

    if decision.action == SwarmSloAction::ForceSafeMode || decision.safe_mode_active_after {
        return format!(
            "enter or hold safe mode until {} consecutive healthy samples; keep concurrent-writer mode enabled",
            config.recovery_healthy_decisions
        );
    }

    "safe mode is not required for this decision; keep concurrent-writer mode enabled and leave enforcement gated on proof artifacts"
        .to_owned()
}

fn normalize_degraded_signals(
    input: &SwarmSloGovernorInput,
    config: &SwarmSloGovernorConfig,
) -> BTreeSet<SwarmSloDegradedSignal> {
    let mut signals = input
        .degraded_signals
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();

    if input.latency_p999_ns.is_none() {
        signals.insert(SwarmSloDegradedSignal::MissingP999);
    }

    if input.publish_window_occupancy.is_none() || input.publish_window_p99_ns.is_none() {
        signals.insert(SwarmSloDegradedSignal::MissingPublishWindow);
    }

    if input.sample_window_ms == 0 {
        signals.insert(SwarmSloDegradedSignal::MissingLiveMetrics);
    }

    if input.sample_age_ms > config.stale_metrics_after_ms {
        signals.insert(SwarmSloDegradedSignal::StaleMetrics);
    }

    if input.sample_source == SwarmSloSampleSource::Replay {
        signals.insert(SwarmSloDegradedSignal::ReplayOnlyInput);
    }

    if input.privacy_redacted {
        signals.insert(SwarmSloDegradedSignal::PrivacyRedactedInput);
    }

    signals
}

fn derive_signals(input: &SwarmSloGovernorInput) -> SwarmSloDerivedSignals {
    SwarmSloDerivedSignals {
        writer_saturation_per_mille: ratio_per_mille(
            u64::from(input.active_writers),
            u64::from(input.available_cores.max(1)),
        ),
        publish_contention_per_mille: input.publish_window_occupancy.map(|occupancy| {
            ratio_per_mille(
                u64::from(occupancy),
                u64::from(input.available_cores.max(1)),
            )
        }),
        tail_stress_per_mille: if input.latency_p50_ns == 0 {
            PER_MILLE
        } else {
            ratio_per_mille(input.latency_p99_ns, input.latency_p50_ns)
        },
    }
}

fn ratio_per_mille(numerator: u64, denominator: u64) -> u64 {
    numerator.saturating_mul(PER_MILLE) / denominator.max(1)
}

fn match_rule(
    input: &SwarmSloGovernorInput,
    config: &SwarmSloGovernorConfig,
    derived: &SwarmSloDerivedSignals,
) -> RuleMatch {
    if !input.concurrent_mode_default_observed {
        return rule(
            "G0_CONCURRENT_DEFAULT_OFF",
            SwarmSloAction::ApplyBackpressure,
            "pressure rules would run only after the concurrent-writer default is observed",
            "failing closed may reject useful work, but evaluating under a broken core invariant is worse",
        );
    }

    if input.max_chain_depth > config.max_chain_depth_critical {
        return rule(
            "G1",
            SwarmSloAction::TriggerEmergencyGc,
            "without emergency GC, version chains may keep growing",
            "inline GC adds latency, but bounds memory and visibility-chain pressure",
        );
    }

    if input.wal_frames_pending_checkpoint > config.wal_frames_restart_threshold
        && !input.checkpoint_active
    {
        return rule(
            "G2",
            SwarmSloAction::TriggerCheckpoint,
            "without checkpoint pressure relief, WAL scan and recovery cost can keep growing",
            "checkpoint work can contend with writers, but it reduces WAL pressure",
        );
    }

    if derived.writer_saturation_per_mille > config.writer_saturation_threshold_per_mille {
        return rule(
            "G3",
            SwarmSloAction::ApplyBackpressure,
            "additional admissions would increase writer-lane saturation",
            "backpressure may surface SQLITE_BUSY earlier, but avoids unbounded writer contention",
        );
    }

    if derived
        .publish_contention_per_mille
        .is_some_and(|contention| contention > config.publish_contention_threshold_per_mille)
    {
        return rule(
            "G4",
            SwarmSloAction::Defer {
                park_budget_us: 500,
            },
            "without a short defer, contenders keep colliding in the publish window",
            "parking adds small admission latency, but reduces collision probability",
        );
    }

    if derived.tail_stress_per_mille > config.tail_stress_threshold_per_mille {
        return rule(
            "G5",
            SwarmSloAction::Defer {
                park_budget_us: 1_000,
            },
            "without admission delay, pathological tail pressure can persist",
            "defer slows new work, but lets queues drain before p99 grows further",
        );
    }

    if input.retry_rate_per_mille > config.retry_rate_threshold_per_mille {
        return rule(
            "G6",
            SwarmSloAction::Defer {
                park_budget_us: 200,
            },
            "without retry dampening, compute is wasted on repeated attempts",
            "brief defer may lower peak throughput, but reduces retry churn",
        );
    }

    if input.abort_rate_per_mille > config.abort_rate_threshold_per_mille {
        return rule(
            "G7",
            SwarmSloAction::Defer {
                park_budget_us: 500,
            },
            "without abort dampening, hot-page contention or SSI pressure persists",
            "brief defer may delay independent work, but gives competing transactions time to finish",
        );
    }

    if input.evidence_queue_drops > config.evidence_drop_threshold {
        return rule(
            "G8",
            SwarmSloAction::ShrinkHelperBudget {
                target_lane: SwarmSloHelperLane::Evidence,
            },
            "without reducing evidence detail, the evidence lane keeps dropping samples",
            "lower evidence fidelity is worse for diagnostics, but preserves foreground work",
        );
    }

    if input.invalidation_fallback_count > config.invalidation_fallback_threshold {
        return rule(
            "G9",
            SwarmSloAction::ShrinkHelperBudget {
                target_lane: SwarmSloHelperLane::Invalidation,
            },
            "without invalidation budget shrink, inline fallbacks can keep increasing",
            "coarser invalidation may reduce freshness, but prevents fallback storms",
        );
    }

    if input.build_or_test_saturation_per_mille >= config.build_test_saturation_threshold_per_mille
        || input.agent_wedge_risk
    {
        return rule(
            "S1",
            SwarmSloAction::ShrinkHelperBudget {
                target_lane: SwarmSloHelperLane::AgentProof,
            },
            "without proof-lane shrink, agent builds and tests can starve useful database work",
            "slowing proof lanes delays feedback, but avoids wedging the shared host",
        );
    }

    if input.latency_p99_ns > config.safe_mode_p99_threshold_ns {
        return rule(
            "G10",
            SwarmSloAction::ForceSafeMode,
            "without safe mode, user-visible p99 is already above the hard threshold",
            "safe mode removes scheduling uncertainty, but reduces optional offload capacity",
        );
    }

    if input.gc_inline_active
        && input
            .publish_window_p99_ns
            .is_some_and(|p99| p99 > config.safe_mode_publish_window_threshold_ns)
    {
        return rule(
            "G11",
            SwarmSloAction::ForceSafeMode,
            "without safe mode, inline GC can dominate the publish window",
            "safe mode can reduce helper concurrency, but keeps critical work inline and bounded",
        );
    }

    rule(
        "G12",
        SwarmSloAction::Admit,
        "pressure rules would have delayed or throttled work",
        "admitting preserves throughput when sampled pressure is healthy",
    )
}

fn apply_safe_mode_hysteresis(
    raw_match: RuleMatch,
    state: &mut SwarmSloGovernorState,
    input: &SwarmSloGovernorInput,
    config: &SwarmSloGovernorConfig,
) -> RuleMatch {
    if raw_match.action == SwarmSloAction::ForceSafeMode {
        state.safe_mode_active = true;
        state.healthy_safe_mode_exit_samples = 0;
        return raw_match;
    }

    if !state.safe_mode_active {
        state.healthy_safe_mode_exit_samples = 0;
        return raw_match;
    }

    if safe_mode_exit_sample_is_healthy(raw_match.action, input, config) {
        state.healthy_safe_mode_exit_samples =
            state.healthy_safe_mode_exit_samples.saturating_add(1);

        if state.healthy_safe_mode_exit_samples >= config.recovery_healthy_decisions {
            state.safe_mode_active = false;
            state.healthy_safe_mode_exit_samples = 0;
            return raw_match;
        }
    } else {
        state.healthy_safe_mode_exit_samples = 0;
    }

    rule(
        "G_SAFE_HOLD",
        SwarmSloAction::ForceSafeMode,
        "raw pressure no longer requires immediate safe mode, but recovery holdoff has not elapsed",
        "holding safe mode protects against flapping at the cost of temporary helper suppression",
    )
}

fn safe_mode_exit_sample_is_healthy(
    raw_action: SwarmSloAction,
    input: &SwarmSloGovernorInput,
    config: &SwarmSloGovernorConfig,
) -> bool {
    raw_action == SwarmSloAction::Admit
        && input.latency_p99_ns < config.safe_mode_p99_threshold_ns / 2
        && !input.gc_inline_active
        && derive_signals(input).tail_stress_per_mille < config.tail_stress_threshold_per_mille / 2
}

const fn rule(
    guardrail_id: &'static str,
    action: SwarmSloAction,
    counterfactual: &'static str,
    regret: &'static str,
) -> RuleMatch {
    RuleMatch {
        guardrail_id,
        action,
        counterfactual,
        regret,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn healthy_input() -> SwarmSloGovernorInput {
        SwarmSloGovernorInput::healthy_replay(
            "run-slo-policy-test",
            "trace-slo-policy-test",
            "scenario-slo-policy-test",
        )
    }

    fn enforced_live_input() -> SwarmSloGovernorInput {
        let mut input = healthy_input();
        input.sample_source = SwarmSloSampleSource::LiveHarness;
        input.control_mode = SwarmSloControlMode::Enforced;
        input.artifact_path = Some("tests/artifacts/swarm-slo/live-sample.json".to_owned());
        input.artifact_hash =
            Some("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned());
        input.latency_p999_ns = Some(5_000_000);
        input.degraded_signals.clear();
        input.sample_age_ms = 0;
        input.sample_window_ms = 1_000;
        input.publish_window_occupancy = Some(1);
        input.publish_window_p99_ns = Some(1_000_000);
        input.first_failure_diag = "none".to_owned();
        input.invalidation_fallback_count = 0;
        input.concurrent_mode_default_observed = true;
        input.privacy_redacted = false;
        input
    }

    fn full_rollout_request() -> SwarmSloEnforcementRolloutRequest {
        SwarmSloEnforcementRolloutRequest {
            explicit_opt_in: true,
            operator_kill_switch_active: false,
            serialized_writer_fallback_requested: false,
            evidence: SwarmSloEnforcementRolloutEvidence {
                proof_pack_hash: Some(
                    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
                ),
                governor_off_shadow_compared: true,
                benchmark_artifact_path: Some(
                    "tests/artifacts/swarm-slo/full-quick-2026-05-24/report.json".to_owned(),
                ),
                benchmark_artifact_commit: Some("edc3193c".to_owned()),
                benchmark_artifact_date: Some("2026-05-24".to_owned()),
                smoke_command: Some("cargo test -p fsqlite-harness --lib slo_governor".to_owned()),
                heavy_rch_command: Some(
                    "timeout 1200 rch exec -- cargo test -p fsqlite-harness --lib slo_governor"
                        .to_owned(),
                ),
            },
        }
    }

    #[test]
    fn healthy_replay_admits_and_records_degraded_replay_signals() {
        let decision = evaluate_swarm_slo_once(&healthy_input());

        assert_eq!(decision.guardrail_id, "G12");
        assert_eq!(decision.action, SwarmSloAction::Admit);
        assert_eq!(decision.decision_id, 0);
        assert!(decision.concurrent_mode_default_observed);
        assert!(
            decision
                .degraded_signals
                .contains(&SwarmSloDegradedSignal::MissingP999)
        );
        assert!(
            decision
                .degraded_signals
                .contains(&SwarmSloDegradedSignal::ReplayOnlyInput)
        );
    }

    #[test]
    fn emergency_gc_has_priority_over_writer_backpressure() {
        let mut input = healthy_input();
        input.max_chain_depth = 257;
        input.active_writers = 128;

        let decision = evaluate_swarm_slo_once(&input);

        assert_eq!(decision.guardrail_id, "G1");
        assert_eq!(decision.action, SwarmSloAction::TriggerEmergencyGc);
    }

    #[test]
    fn writer_saturation_applies_backpressure() {
        let mut input = healthy_input();
        input.available_cores = 64;
        input.active_writers = 64;

        let decision = evaluate_swarm_slo_once(&input);

        assert_eq!(decision.guardrail_id, "G3");
        assert_eq!(decision.action, SwarmSloAction::ApplyBackpressure);
        assert_eq!(decision.derived.writer_saturation_per_mille, 1_000);
    }

    #[test]
    fn publish_contention_defers_admission() {
        let mut input = healthy_input();
        input.available_cores = 64;
        input.publish_window_occupancy = Some(40);

        let decision = evaluate_swarm_slo_once(&input);

        assert_eq!(decision.guardrail_id, "G4");
        assert_eq!(
            decision.action,
            SwarmSloAction::Defer {
                park_budget_us: 500
            }
        );
    }

    #[test]
    fn retry_and_abort_pressure_are_distinct_defer_rules() {
        let mut retry_input = healthy_input();
        retry_input.retry_rate_per_mille = 301;
        let retry_decision = evaluate_swarm_slo_once(&retry_input);
        assert_eq!(retry_decision.guardrail_id, "G6");
        assert_eq!(
            retry_decision.action,
            SwarmSloAction::Defer {
                park_budget_us: 200
            }
        );

        let mut abort_input = healthy_input();
        abort_input.abort_rate_per_mille = 201;
        let abort_decision = evaluate_swarm_slo_once(&abort_input);
        assert_eq!(abort_decision.guardrail_id, "G7");
        assert_eq!(
            abort_decision.action,
            SwarmSloAction::Defer {
                park_budget_us: 500
            }
        );
    }

    #[test]
    fn missing_publish_window_is_degraded_without_fabricating_g11() {
        let mut input = healthy_input();
        input.gc_inline_active = true;
        input.publish_window_occupancy = None;
        input.publish_window_p99_ns = None;

        let decision = evaluate_swarm_slo_once(&input);

        assert_eq!(decision.guardrail_id, "G12");
        assert_eq!(decision.action, SwarmSloAction::Admit);
        assert!(
            decision
                .degraded_signals
                .contains(&SwarmSloDegradedSignal::MissingPublishWindow)
        );
    }

    #[test]
    fn proof_lane_saturation_shrinks_agent_helper_budget() {
        let mut input = healthy_input();
        input.build_or_test_saturation_per_mille = 900;

        let decision = evaluate_swarm_slo_once(&input);

        assert_eq!(decision.guardrail_id, "S1");
        assert_eq!(
            decision.action,
            SwarmSloAction::ShrinkHelperBudget {
                target_lane: SwarmSloHelperLane::AgentProof
            }
        );
    }

    #[test]
    fn safe_mode_exit_requires_sustained_healthy_samples() {
        let config = SwarmSloGovernorConfig {
            recovery_healthy_decisions: 3,
            ..SwarmSloGovernorConfig::default()
        };
        let mut state = SwarmSloGovernorState::default();
        let mut unsafe_input = healthy_input();
        unsafe_input.latency_p50_ns = 20_000_000;
        unsafe_input.latency_p99_ns = 150_000_000;

        let enter = evaluate_swarm_slo(&unsafe_input, &config, &mut state);
        assert_eq!(enter.guardrail_id, "G10");
        assert!(state.safe_mode_active());

        let healthy = healthy_input();
        let hold1 = evaluate_swarm_slo(&healthy, &config, &mut state);
        let hold2 = evaluate_swarm_slo(&healthy, &config, &mut state);
        let exit = evaluate_swarm_slo(&healthy, &config, &mut state);

        assert_eq!(hold1.guardrail_id, "G_SAFE_HOLD");
        assert_eq!(hold2.guardrail_id, "G_SAFE_HOLD");
        assert_eq!(exit.guardrail_id, "G12");
        assert!(!state.safe_mode_active());
        assert_eq!(exit.decision_id, 3);
    }

    #[test]
    fn concurrent_mode_default_guard_fails_closed_without_mutating_connection()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut outcome: Result<(), Box<dyn std::error::Error>> = Ok(());
        asupersync::test_utils::run_test(|| async {
            outcome = async {
                let conn = fsqlite_core::connection::Connection::open(":memory:").await?;
                assert!(conn.is_concurrent_mode_default());

                let mut input = healthy_input();
                input.concurrent_mode_default_observed = conn.is_concurrent_mode_default();
                input.latency_p50_ns = 20_000_000;
                input.latency_p99_ns = 150_000_000;
                let safe_mode_decision = evaluate_swarm_slo_once(&input);
                assert_eq!(safe_mode_decision.action, SwarmSloAction::ForceSafeMode);
                assert!(
                    conn.is_concurrent_mode_default(),
                    "shadow policy must not disable concurrent-writer default"
                );

                input.concurrent_mode_default_observed = false;
                input.latency_p99_ns = 4_000_000;
                let fail_closed_decision = evaluate_swarm_slo_once(&input);
                assert_eq!(
                    fail_closed_decision.guardrail_id,
                    "G0_CONCURRENT_DEFAULT_OFF"
                );
                assert_eq!(
                    fail_closed_decision.action,
                    SwarmSloAction::ApplyBackpressure
                );

                Ok(())
            }
            .await;
        });
        outcome
    }

    #[test]
    fn decisions_are_json_serializable_for_evidence_artifacts()
    -> Result<(), Box<dyn std::error::Error>> {
        let decision = evaluate_swarm_slo_once(&healthy_input());
        let encoded = serde_json::to_string(&decision)?;

        assert!(encoded.contains(SWARM_SLO_POLICY_ID));
        assert!(encoded.contains("swarm-slo-shadow.v1"));

        Ok(())
    }

    #[test]
    fn operator_report_lists_budgets_guidance_and_no_perf_claim() {
        let config = SwarmSloGovernorConfig::default();
        let decision = evaluate_swarm_slo_once(&healthy_input());
        let report = build_swarm_slo_operator_report(&decision, &config);

        assert_eq!(
            report.schema_version,
            SWARM_SLO_OPERATOR_REPORT_SCHEMA_VERSION
        );
        assert_eq!(report.bead_id, SWARM_SLO_OPERATOR_BEAD_ID);
        assert_eq!(report.action, SwarmSloAction::Admit);
        assert!(report.action_summary.contains("admit foreground work"));
        assert!(
            report
                .budgets
                .iter()
                .any(|budget| budget.metric == "writer_saturation_per_mille"
                    && budget.status == SwarmSloOperatorBudgetStatus::Within)
        );
        assert!(
            report
                .kill_switches
                .iter()
                .any(|switch| switch.contains("shadow_mode_only"))
        );
        assert!(
            report
                .measurement_guardrail
                .contains("does not claim performance improvement")
        );
        assert_eq!(
            report.fallback_path_risk,
            SwarmSloFallbackPathRisk::DegradedSignalsOnly
        );
        assert!(
            report
                .degraded_signal_quality
                .contains(&SwarmSloDegradedSignal::MissingP999)
        );
        assert!(
            report
                .degraded_signal_quality
                .contains(&SwarmSloDegradedSignal::ReplayOnlyInput)
        );
    }

    #[test]
    fn operator_report_surfaces_fallback_and_safe_mode_without_disabling_concurrency() {
        let config = SwarmSloGovernorConfig::default();
        let mut input = healthy_input();
        input.latency_p50_ns = 10_000_000;
        input.latency_p99_ns = config.safe_mode_p99_threshold_ns + 1;
        input.invalidation_fallback_count = 1;
        input.first_failure_diag =
            "fallback path used after invalidation queue pressure".to_owned();

        let decision = evaluate_swarm_slo_once(&input);
        let report = build_swarm_slo_operator_report(&decision, &config);

        assert_eq!(report.action, SwarmSloAction::ForceSafeMode);
        assert_eq!(
            report.fallback_path_risk,
            SwarmSloFallbackPathRisk::CompatibilityFallbackObserved
        );
        assert!(
            report
                .compatibility_fallback_summary
                .contains("inline invalidation fallback")
        );
        assert!(
            report
                .safe_mode_guidance
                .contains("keep concurrent-writer mode enabled")
        );
        assert!(
            report
                .kill_switches
                .iter()
                .any(|switch| switch.contains("admission_backpressure"))
        );
    }

    #[test]
    fn operator_report_marks_missing_publish_budget_unmeasured() {
        let config = SwarmSloGovernorConfig::default();
        let mut input = healthy_input();
        input.publish_window_occupancy = None;
        input.publish_window_p99_ns = None;

        let decision = evaluate_swarm_slo_once(&input);
        let report = build_swarm_slo_operator_report(&decision, &config);

        assert!(report.budgets.iter().any(|budget| {
            budget.metric == "publish_contention_per_mille"
                && budget.status == SwarmSloOperatorBudgetStatus::Unmeasured
        }));
        assert!(
            report
                .degraded_signal_quality
                .contains(&SwarmSloDegradedSignal::MissingPublishWindow)
        );
    }

    #[test]
    fn enforcement_rollout_gate_allows_only_opted_in_current_artifact_backed_evidence() {
        let governor_config = SwarmSloGovernorConfig::default();
        let rollout_config = SwarmSloEnforcementRolloutConfig::default();
        let input = enforced_live_input();
        let decision = evaluate_swarm_slo_once(&input);
        let operator_report = build_swarm_slo_operator_report(&decision, &governor_config);
        let request = full_rollout_request();

        let report = gate_swarm_slo_enforcement_rollout(
            &decision,
            &operator_report,
            &request,
            &rollout_config,
        );

        assert_eq!(report.verdict, SwarmSloEnforcementRolloutVerdict::Enforce);
        assert_eq!(report.effective_control_mode, SwarmSloControlMode::Enforced);
        assert!(report.blockers.is_empty());
        assert!(report.downgrade_reasons.is_empty());
        assert!(report.concurrent_mode_default_observed);
        assert!(!report.serialized_writer_fallback_requested);
        assert!(
            report
                .measurement_guardrail
                .contains("dated benchmark/proof-pack artifacts")
        );
        assert!(
            report
                .heavy_rch_command
                .as_deref()
                .is_some_and(|command| command.contains("rch exec"))
        );
    }

    #[test]
    fn enforcement_rollout_gate_downgrades_missing_or_stale_telemetry_to_shadow() {
        let governor_config = SwarmSloGovernorConfig::default();
        let rollout_config = SwarmSloEnforcementRolloutConfig::default();
        let mut input = enforced_live_input();
        input.sample_age_ms = governor_config.stale_metrics_after_ms + 1;
        input.publish_window_occupancy = None;
        input.publish_window_p99_ns = None;
        input.latency_p999_ns = None;
        let decision = evaluate_swarm_slo(
            &input,
            &governor_config,
            &mut SwarmSloGovernorState::default(),
        );
        let operator_report = build_swarm_slo_operator_report(&decision, &governor_config);
        let request = full_rollout_request();

        let report = gate_swarm_slo_enforcement_rollout(
            &decision,
            &operator_report,
            &request,
            &rollout_config,
        );

        assert_eq!(
            report.verdict,
            SwarmSloEnforcementRolloutVerdict::DowngradeToShadow
        );
        assert_eq!(report.effective_control_mode, SwarmSloControlMode::Shadow);
        assert!(
            report
                .downgrade_reasons
                .contains(&SwarmSloEnforcementRolloutReason::StaleMetrics)
        );
        assert!(
            report
                .downgrade_reasons
                .contains(&SwarmSloEnforcementRolloutReason::MissingPublishWindow)
        );
        assert!(
            report
                .downgrade_reasons
                .contains(&SwarmSloEnforcementRolloutReason::MissingP999)
        );
    }

    #[test]
    fn enforcement_rollout_gate_blocks_kill_switch_serialized_fallback_and_broken_default() {
        let governor_config = SwarmSloGovernorConfig::default();
        let rollout_config = SwarmSloEnforcementRolloutConfig::default();
        let mut input = enforced_live_input();
        input.concurrent_mode_default_observed = false;
        let decision = evaluate_swarm_slo_once(&input);
        let operator_report = build_swarm_slo_operator_report(&decision, &governor_config);
        let mut request = full_rollout_request();
        request.explicit_opt_in = false;
        request.operator_kill_switch_active = true;
        request.serialized_writer_fallback_requested = true;

        let report = gate_swarm_slo_enforcement_rollout(
            &decision,
            &operator_report,
            &request,
            &rollout_config,
        );

        assert_eq!(report.verdict, SwarmSloEnforcementRolloutVerdict::Blocked);
        assert_eq!(report.effective_control_mode, SwarmSloControlMode::Shadow);
        assert!(
            report
                .blockers
                .contains(&SwarmSloEnforcementRolloutReason::ExplicitOptInMissing)
        );
        assert!(
            report
                .blockers
                .contains(&SwarmSloEnforcementRolloutReason::KillSwitchActive)
        );
        assert!(
            report
                .blockers
                .contains(&SwarmSloEnforcementRolloutReason::ConcurrentDefaultNotObserved)
        );
        assert!(
            report
                .blockers
                .contains(&SwarmSloEnforcementRolloutReason::SerializedWriterFallbackRequested)
        );
    }

    #[test]
    fn enforcement_rollout_gate_blocks_uncited_performance_claims() {
        let governor_config = SwarmSloGovernorConfig::default();
        let rollout_config = SwarmSloEnforcementRolloutConfig::default();
        let input = enforced_live_input();
        let decision = evaluate_swarm_slo_once(&input);
        let operator_report = build_swarm_slo_operator_report(&decision, &governor_config);
        let request = SwarmSloEnforcementRolloutRequest {
            explicit_opt_in: true,
            ..SwarmSloEnforcementRolloutRequest::default()
        };

        let report = gate_swarm_slo_enforcement_rollout(
            &decision,
            &operator_report,
            &request,
            &rollout_config,
        );

        assert_eq!(report.verdict, SwarmSloEnforcementRolloutVerdict::Blocked);
        assert!(
            report
                .blockers
                .contains(&SwarmSloEnforcementRolloutReason::ProofPackHashMissing)
        );
        assert!(
            report
                .blockers
                .contains(&SwarmSloEnforcementRolloutReason::GovernorOffShadowComparisonMissing)
        );
        assert!(
            report
                .blockers
                .contains(&SwarmSloEnforcementRolloutReason::BenchmarkArtifactMissing)
        );
        assert!(
            report
                .blockers
                .contains(&SwarmSloEnforcementRolloutReason::BenchmarkArtifactCommitMissing)
        );
        assert!(
            report
                .blockers
                .contains(&SwarmSloEnforcementRolloutReason::BenchmarkArtifactDateMissing)
        );
    }
}
