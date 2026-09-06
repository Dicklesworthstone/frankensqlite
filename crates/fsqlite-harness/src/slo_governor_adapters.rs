//! Replay and live-harness signal adapters for the Swarm SLO governor.
//!
//! The adapters in this module normalize existing harness artifacts into the
//! single [`SwarmSloGovernorInput`] schema. They are intentionally private to
//! the harness crate surface and do not expose a competing production metrics
//! endpoint; production export belongs to the `bd-zywqc.11` metrics work.

#![allow(clippy::struct_excessive_bools)]

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::agent_swarm_trace::{
    AgentSwarmBackendResourceScorecard, AgentSwarmBackendSummary, AgentSwarmCoordinationMetrics,
    AgentSwarmEvidenceManifest, AgentSwarmReplayBackend, AgentSwarmReplayReport,
    AgentSwarmReplaySchedule, AgentSwarmResourceProfileId, AgentSwarmResourceScorecard,
    AgentSwarmStatementReplay,
};
use crate::slo_governor::{
    SWARM_SLO_POLICY_ID, SwarmSloAction, SwarmSloControlMode, SwarmSloDegradedSignal,
    SwarmSloFallbackPathRisk, SwarmSloGcTier, SwarmSloGovernorConfig, SwarmSloGovernorInput,
    SwarmSloGovernorState, SwarmSloOperatorBudget, SwarmSloOperatorReport, SwarmSloSampleSource,
    build_swarm_slo_operator_report, evaluate_swarm_slo,
};

/// Owning bead for the replay/live signal adapter slice.
pub const SWARM_SLO_ADAPTER_BEAD_ID: &str = "bd-swarm-slo-resource-governor-qb256.4";
/// Owning bead for replay/stress proof-pack artifacts.
pub const SWARM_SLO_PROOF_PACK_BEAD_ID: &str = "bd-swarm-slo-resource-governor-qb256.6";
/// Owning bead for the coordination E2E proof-pack runbook.
pub const AGENT_SWARM_COORDINATION_PROOF_PACK_BEAD_ID: &str =
    "bd-agent-swarm-coordination-transparency-8jr6u.10";
/// Machine-readable schema version for SLO replay/stress proof packs.
pub const SWARM_SLO_PROOF_PACK_SCHEMA_VERSION: u32 = 1;
/// Stable proof-pack implementation version.
pub const SWARM_SLO_PROOF_PACK_VERSION: &str = "swarm-slo-proof-pack.v1";

/// Configuration applied while adapting deterministic replay scorecards.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SwarmSloReplayAdapterConfig {
    /// Control mode attached to the resulting governor input.
    pub control_mode: SwarmSloControlMode,
    /// Sample timestamp in milliseconds since caller-defined epoch.
    pub sample_ts_ms: u64,
    /// Sample age in milliseconds.
    pub sample_age_ms: u64,
    /// Replay adapters leave this at zero to mark live metrics unavailable.
    pub sample_window_ms: u64,
    /// Artifact path copied from the evidence manifest when present.
    pub artifact_path: Option<String>,
    /// Artifact hash copied from the evidence manifest when present.
    pub artifact_hash: Option<String>,
    /// Configured helper threads, when known by the caller.
    pub configured_helper_threads: u32,
    /// Active helper threads, when known by the caller.
    pub active_helper_threads: u32,
    /// Evidence workers, when known by the caller.
    pub evidence_worker_count: u32,
}

impl Default for SwarmSloReplayAdapterConfig {
    fn default() -> Self {
        Self {
            control_mode: SwarmSloControlMode::Shadow,
            sample_ts_ms: 0,
            sample_age_ms: 0,
            sample_window_ms: 0,
            artifact_path: None,
            artifact_hash: None,
            configured_helper_threads: 0,
            active_helper_threads: 0,
            evidence_worker_count: 0,
        }
    }
}

impl SwarmSloReplayAdapterConfig {
    /// Copy replay artifact identity from an existing evidence manifest.
    pub fn with_evidence_manifest(mut self, manifest: &AgentSwarmEvidenceManifest) -> Self {
        self.artifact_path = Some(manifest.trace_artifact_path.clone());
        self.artifact_hash = Some(manifest.artifact_hash.clone());
        self
    }
}

/// Failure while translating a replay artifact into governor input.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SwarmSloAdapterError {
    /// Replay report did not contain the requested backend.
    ReplayBackendMissing {
        /// Requested backend.
        backend: AgentSwarmReplayBackend,
    },
    /// Resource scorecard did not contain the requested backend.
    ScorecardBackendMissing {
        /// Requested backend.
        backend: AgentSwarmReplayBackend,
    },
    /// Report and scorecard identity fields disagree.
    MismatchedReplayIdentity {
        /// Identity field name.
        field: &'static str,
        /// Value from the replay report.
        report_value: String,
        /// Value from the resource scorecard.
        scorecard_value: String,
    },
    /// Evidence manifest identity did not match the replay report.
    MismatchedEvidenceManifestIdentity {
        /// Identity field name.
        field: &'static str,
        /// Value from the replay report.
        report_value: String,
        /// Value from the evidence manifest.
        manifest_value: String,
    },
}

impl fmt::Display for SwarmSloAdapterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ReplayBackendMissing { backend } => {
                write!(f, "replay report is missing backend {backend:?}")
            }
            Self::ScorecardBackendMissing { backend } => {
                write!(f, "resource scorecard is missing backend {backend:?}")
            }
            Self::MismatchedReplayIdentity {
                field,
                report_value,
                scorecard_value,
            } => write!(
                f,
                "replay report {field}={report_value:?} does not match scorecard {field}={scorecard_value:?}",
            ),
            Self::MismatchedEvidenceManifestIdentity {
                field,
                report_value,
                manifest_value,
            } => write!(
                f,
                "replay report {field}={report_value:?} does not match evidence manifest {field}={manifest_value:?}",
            ),
        }
    }
}

impl std::error::Error for SwarmSloAdapterError {}

/// Resource profile and replay commands attached to a proof pack.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SwarmSloProofPackProfile {
    /// Resource profile label.
    pub profile_id: String,
    /// Profile-visible CPU cores.
    pub available_cores: u32,
    /// Profile memory limit.
    pub memory_limit_bytes: u64,
    /// Profile page-cache limit.
    pub page_cache_limit_bytes: u64,
    /// Maximum statement count intended for local smoke runs.
    pub local_statement_limit: usize,
    /// One-command CI-sized smoke replay.
    pub smoke_command: String,
    /// CPU-heavy replay command that must be offloaded through `rch`.
    pub heavy_rch_command: String,
}

/// Raw replay/scorecard metrics with the governor off.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SwarmSloGovernorOffMetrics {
    /// Statements replayed on this backend.
    pub statement_count: usize,
    /// Transactions represented by the replay trace.
    pub transaction_count: usize,
    /// User-visible p50 in nanoseconds.
    pub latency_p50_ns: u64,
    /// User-visible p95 in nanoseconds.
    pub latency_p95_ns: u64,
    /// User-visible p99 in nanoseconds.
    pub latency_p99_ns: u64,
    /// Backend throughput in statements/second multiplied by 1000.
    pub throughput_statements_per_second_x1000: u64,
    /// Commit retry count.
    pub retry_count: u64,
    /// Retry rate in per-mille units.
    pub retry_rate_per_mille: u64,
    /// Abort/error count.
    pub abort_count: usize,
    /// Abort/error rate in per-mille units.
    pub abort_rate_per_mille: u64,
    /// Expected-result mismatches observed during replay.
    pub expected_mismatch_count: usize,
    /// Conflict/error classes observed during replay.
    pub conflict_classes: BTreeMap<String, usize>,
    /// Coordination bridge metrics copied from the replay scorecard.
    pub coordination_metrics: AgentSwarmCoordinationMetrics,
    /// Deterministic replay memory high-water estimate.
    pub memory_high_water_bytes: u64,
    /// Profile memory limit.
    pub memory_limit_bytes: u64,
    /// Profile page-cache footprint estimate.
    pub page_cache_bytes: u64,
    /// First failure diagnostic copied from the evidence bundle.
    pub first_failure_diag: String,
}

/// Shadow-mode SLO governor decision attached to a proof row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SwarmSloShadowProof {
    /// Requested control mode.
    pub control_mode: SwarmSloControlMode,
    /// Matched guardrail.
    pub guardrail_id: String,
    /// Recommended shadow action.
    pub action: SwarmSloAction,
    /// Short action summary.
    pub action_summary: String,
    /// Fallback risk classification from operator output.
    pub fallback_path_risk: SwarmSloFallbackPathRisk,
    /// Operator budget rows.
    pub budgets: Vec<SwarmSloOperatorBudget>,
    /// Full deterministic operator report.
    pub operator_report: SwarmSloOperatorReport,
}

/// One backend row in the replay/stress proof pack.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SwarmSloProofPackBackend {
    /// Backend role.
    pub backend: AgentSwarmReplayBackend,
    /// Whether this backend is the FrankenSQLite concurrent-writer default path.
    pub concurrent_writer_default: bool,
    /// Raw replay metrics with the governor off.
    pub governor_off: SwarmSloGovernorOffMetrics,
    /// Shadow decision, present only for the FrankenSQLite concurrent-default backend.
    pub governor_shadow: Option<SwarmSloShadowProof>,
    /// Why shadow policy was not evaluated for this backend.
    pub shadow_skip_reason: Option<String>,
}

/// One operator-facing proof point in the final coordination runbook.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SwarmSloProofPackRunbookCheck {
    /// Coordination surface covered by this check.
    pub surface: String,
    /// Proof-pack field or metric that backs the check.
    pub evidence_field: String,
    /// Count observed in the FrankenSQLite concurrent-default replay row,
    /// or `None` when the observation is unavailable.
    pub observed_count: Option<u64>,
    /// SQL inspection an operator can run on the database or trace tables.
    pub operator_query: String,
}

/// Machine-readable runbook bundled with the final coordination proof pack.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SwarmSloProofPackRunbook {
    /// Bead that owns this runbook.
    pub bead_id: String,
    /// Coordination coverage checks for the deterministic replay scenario.
    pub scenario_coverage: Vec<SwarmSloProofPackRunbookCheck>,
    /// Artifact paths a future agent or operator should inspect.
    pub artifact_paths: Vec<String>,
    /// Commands that reproduce the focused proof run.
    pub proof_commands: Vec<String>,
    /// Beads/BV graph-health commands required at closeout.
    pub graph_health_commands: Vec<String>,
    /// Query shape for detecting hot worker ranges.
    pub hot_range_detection: String,
    /// Query shape for detecting stale or expired leases.
    pub stuck_lease_detection: String,
    /// Query shape for detecting fallback-heavy statements.
    pub fallback_detection: String,
    /// Query shape for detecting resource-governor pressure.
    pub resource_pressure_detection: String,
    /// Explicitly unmeasured claims or follow-up boundaries.
    pub known_limitations: Vec<String>,
}

/// Machine-readable proof pack for SLO governor replay and stress evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SwarmSloReplayStressProofPack {
    /// Proof-pack schema version.
    pub schema_version: u32,
    /// Owning bead identifier.
    pub bead_id: String,
    /// Proof-pack implementation version.
    pub proof_pack_version: String,
    /// Stable hash of this proof pack.
    pub proof_pack_hash: String,
    /// Shadow policy version evaluated by this proof pack.
    pub policy_id: String,
    /// Trace identifier.
    pub trace_id: String,
    /// Run identifier.
    pub run_id: String,
    /// Scenario identifier.
    pub scenario_id: String,
    /// Replay seed.
    pub replay_seed: u64,
    /// Replay schedule fingerprint.
    pub schedule_fingerprint: String,
    /// Resource profile and replay commands.
    pub profile: SwarmSloProofPackProfile,
    /// Evidence manifest hash.
    pub evidence_manifest_hash: String,
    /// Full trace artifact path.
    pub trace_artifact_path: String,
    /// Full trace artifact hash.
    pub trace_artifact_hash: String,
    /// Replay command used to produce the evidence manifest.
    pub replay_command: String,
    /// First failure diagnostic for the proof pack.
    pub first_failure_diag: String,
    /// Backend proof rows.
    pub backend_proofs: Vec<SwarmSloProofPackBackend>,
    /// Operator runbook for interpreting the coordination proof scenario.
    pub operator_runbook: SwarmSloProofPackRunbook,
    /// Guardrail against uncited performance claims.
    pub measurement_guardrail: String,
}

/// Adapt a replay report plus resource scorecard into one governor input row.
pub fn adapt_replay_scorecard_to_swarm_slo_input(
    report: &AgentSwarmReplayReport,
    scorecard: &AgentSwarmResourceScorecard,
    backend: AgentSwarmReplayBackend,
    config: &SwarmSloReplayAdapterConfig,
) -> Result<SwarmSloGovernorInput, SwarmSloAdapterError> {
    validate_replay_identity(report, scorecard)?;

    let replay_backend = report
        .backends
        .iter()
        .find(|candidate| candidate.identity.backend == backend)
        .ok_or(SwarmSloAdapterError::ReplayBackendMissing { backend })?;
    let scorecard_backend = scorecard
        .backends
        .iter()
        .find(|candidate| candidate.backend == backend)
        .ok_or(SwarmSloAdapterError::ScorecardBackendMissing { backend })?;

    let actor_count = capped_u32(scorecard_backend.per_actor_statement_count.len());
    let connection_count = unique_count_u32(
        replay_backend
            .statements
            .iter()
            .map(|statement| statement.connection_id.as_str()),
    );
    let writer_count = writer_actor_count(&replay_backend.statements);
    let reader_count = actor_count.saturating_sub(writer_count);
    let concurrency_level = actor_count.max(connection_count).max(unique_count_u32(
        replay_backend
            .statements
            .iter()
            .map(|statement| statement.concurrency_group.as_str()),
    ));

    let coordination_metrics = &scorecard_backend.coordination_metrics;

    Ok(SwarmSloGovernorInput {
        run_id: report.run_id.clone(),
        trace_id: report.trace_id.clone(),
        scenario_id: report.scenario_id.clone(),
        backend: backend.as_str().to_owned(),
        profile_id: resource_profile_id_label(scorecard.profile.profile_id).to_owned(),
        sample_source: SwarmSloSampleSource::Replay,
        control_mode: config.control_mode,
        artifact_path: config.artifact_path.clone(),
        artifact_hash: config.artifact_hash.clone(),
        first_failure_diag: scorecard.first_failure_diag.clone(),
        sample_ts_ms: config.sample_ts_ms,
        sample_age_ms: config.sample_age_ms,
        sample_window_ms: config.sample_window_ms,
        actor_count,
        connection_count,
        writer_count,
        reader_count,
        statement_count: usize_to_u64(replay_backend.summary.statements_total),
        transaction_count: usize_to_u64(report.transaction_count),
        concurrency_level,
        schedule_fingerprint: format!(
            "seed={};schedule={}",
            report.seed,
            replay_schedule_label(report.schedule),
        ),
        available_cores: scorecard_backend.core_count,
        configured_helper_threads: config.configured_helper_threads,
        active_helper_threads: config.active_helper_threads,
        memory_limit_bytes: scorecard_backend.memory_limit_bytes,
        memory_high_water_bytes: scorecard_backend.memory_high_water_bytes,
        page_cache_bytes: scorecard_backend.page_cache_footprint_bytes,
        cpu_utilization_per_mille: scorecard_backend.cpu_utilization_per_mille,
        active_writers: writer_count,
        publish_window_occupancy: None,
        publish_window_p99_ns: None,
        retry_count: replay_backend.summary.retry_count,
        retry_rate_per_mille: capped_u16(scorecard_backend.retry_rate_per_mille),
        abort_count: usize_to_u64(replay_backend.summary.abort_count),
        abort_rate_per_mille: capped_u16(scorecard_backend.abort_rate_per_mille),
        evidence_queue_depth: capped_u32_from_u64(
            coordination_metrics
                .queue_claim_count
                .saturating_sub(coordination_metrics.queue_release_count),
        ),
        evidence_queue_drops: 0,
        evidence_worker_count: config.evidence_worker_count,
        wakeup_queue_depth: 0,
        wakeup_to_run_p95_ns: None,
        wakeup_to_run_p99_ns: None,
        max_chain_depth: 0,
        gc_tier: SwarmSloGcTier::Normal,
        gc_inline_active: false,
        wal_frames_pending_checkpoint: 0,
        checkpoint_active: false,
        invalidation_queue_depth: 0,
        invalidation_fallback_count: capped_u32_from_u64(
            coordination_metrics.fallback_reason_count,
        ),
        build_or_test_saturation_per_mille: coordination_metrics
            .fairness_resource_pressure_per_mille,
        agent_wedge_risk: coordination_metrics.coordination_correctness_per_mille < 800
            || !coordination_metrics.privacy_scrubber_preserved,
        latency_p50_ns: scorecard_backend.latency_p50_ns,
        latency_p95_ns: scorecard_backend.latency_p95_ns,
        latency_p99_ns: scorecard_backend.latency_p99_ns,
        latency_p999_ns: None,
        privacy_redacted: true,
        concurrent_mode_default_observed: scorecard_backend.concurrent_writer_default,
        degraded_signals: replay_degraded_signals(),
    })
}

/// Measured live-harness sample before normalization into governor input.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SwarmSloLiveHarnessSample {
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
    /// Requested control mode.
    pub control_mode: SwarmSloControlMode,
    /// First live diagnostic, or `none` when absent.
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
    /// Schedule or workload fingerprint.
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
    /// Degraded signals already known by the live adapter.
    pub degraded_signals: Vec<SwarmSloDegradedSignal>,
}

/// Adapt a measured live-harness sample into the shared governor input schema.
pub fn adapt_live_harness_sample_to_swarm_slo_input(
    sample: SwarmSloLiveHarnessSample,
) -> SwarmSloGovernorInput {
    SwarmSloGovernorInput {
        run_id: sample.run_id,
        trace_id: sample.trace_id,
        scenario_id: sample.scenario_id,
        backend: sample.backend,
        profile_id: sample.profile_id,
        sample_source: SwarmSloSampleSource::LiveHarness,
        control_mode: sample.control_mode,
        artifact_path: None,
        artifact_hash: None,
        first_failure_diag: sample.first_failure_diag,
        sample_ts_ms: sample.sample_ts_ms,
        sample_age_ms: sample.sample_age_ms,
        sample_window_ms: sample.sample_window_ms,
        actor_count: sample.actor_count,
        connection_count: sample.connection_count,
        writer_count: sample.writer_count,
        reader_count: sample.reader_count,
        statement_count: sample.statement_count,
        transaction_count: sample.transaction_count,
        concurrency_level: sample.concurrency_level,
        schedule_fingerprint: sample.schedule_fingerprint,
        available_cores: sample.available_cores,
        configured_helper_threads: sample.configured_helper_threads,
        active_helper_threads: sample.active_helper_threads,
        memory_limit_bytes: sample.memory_limit_bytes,
        memory_high_water_bytes: sample.memory_high_water_bytes,
        page_cache_bytes: sample.page_cache_bytes,
        cpu_utilization_per_mille: sample.cpu_utilization_per_mille,
        active_writers: sample.active_writers,
        publish_window_occupancy: sample.publish_window_occupancy,
        publish_window_p99_ns: sample.publish_window_p99_ns,
        retry_count: sample.retry_count,
        retry_rate_per_mille: sample.retry_rate_per_mille,
        abort_count: sample.abort_count,
        abort_rate_per_mille: sample.abort_rate_per_mille,
        evidence_queue_depth: sample.evidence_queue_depth,
        evidence_queue_drops: sample.evidence_queue_drops,
        evidence_worker_count: sample.evidence_worker_count,
        wakeup_queue_depth: sample.wakeup_queue_depth,
        wakeup_to_run_p95_ns: sample.wakeup_to_run_p95_ns,
        wakeup_to_run_p99_ns: sample.wakeup_to_run_p99_ns,
        max_chain_depth: sample.max_chain_depth,
        gc_tier: sample.gc_tier,
        gc_inline_active: sample.gc_inline_active,
        wal_frames_pending_checkpoint: sample.wal_frames_pending_checkpoint,
        checkpoint_active: sample.checkpoint_active,
        invalidation_queue_depth: sample.invalidation_queue_depth,
        invalidation_fallback_count: sample.invalidation_fallback_count,
        build_or_test_saturation_per_mille: sample.build_or_test_saturation_per_mille,
        agent_wedge_risk: sample.agent_wedge_risk,
        latency_p50_ns: sample.latency_p50_ns,
        latency_p95_ns: sample.latency_p95_ns,
        latency_p99_ns: sample.latency_p99_ns,
        latency_p999_ns: sample.latency_p999_ns,
        privacy_redacted: sample.privacy_redacted,
        concurrent_mode_default_observed: sample.concurrent_mode_default_observed,
        degraded_signals: sample.degraded_signals,
    }
}

/// Build a deterministic proof pack from replay, scorecard, and evidence-manifest artifacts.
pub fn build_swarm_slo_replay_stress_proof_pack(
    report: &AgentSwarmReplayReport,
    scorecard: &AgentSwarmResourceScorecard,
    manifest: &AgentSwarmEvidenceManifest,
    adapter_config: &SwarmSloReplayAdapterConfig,
    governor_config: &SwarmSloGovernorConfig,
) -> Result<SwarmSloReplayStressProofPack, SwarmSloAdapterError> {
    validate_replay_identity(report, scorecard)?;
    validate_manifest_identity(report, manifest)?;

    let backend_proofs = scorecard
        .backends
        .iter()
        .map(|scorecard_backend| {
            let replay_backend = report
                .backends
                .iter()
                .find(|candidate| candidate.identity.backend == scorecard_backend.backend)
                .ok_or(SwarmSloAdapterError::ReplayBackendMissing {
                    backend: scorecard_backend.backend,
                })?;
            let governor_off = governor_off_metrics(
                &replay_backend.summary,
                scorecard_backend,
                report.transaction_count,
                &scorecard.first_failure_diag,
            );
            let (governor_shadow, shadow_skip_reason) = if scorecard_backend
                .concurrent_writer_default
            {
                let input = adapt_replay_scorecard_to_swarm_slo_input(
                    report,
                    scorecard,
                    scorecard_backend.backend,
                    adapter_config,
                )?;
                let mut state = SwarmSloGovernorState::default();
                let decision = evaluate_swarm_slo(&input, governor_config, &mut state);
                let operator_report = build_swarm_slo_operator_report(&decision, governor_config);
                (
                    Some(SwarmSloShadowProof {
                        control_mode: decision.control_mode,
                        guardrail_id: decision.guardrail_id,
                        action: decision.action,
                        action_summary: operator_report.action_summary.clone(),
                        fallback_path_risk: operator_report.fallback_path_risk,
                        budgets: operator_report.budgets.clone(),
                        operator_report,
                    }),
                    None,
                )
            } else {
                (
                    None,
                    Some(
                        "shadow governor is evaluated only for FrankenSQLite concurrent-default evidence"
                            .to_owned(),
                    ),
                )
            };

            Ok(SwarmSloProofPackBackend {
                backend: scorecard_backend.backend,
                concurrent_writer_default: scorecard_backend.concurrent_writer_default,
                governor_off,
                governor_shadow,
                shadow_skip_reason,
            })
        })
        .collect::<Result<Vec<_>, SwarmSloAdapterError>>()?;
    let operator_runbook = proof_pack_operator_runbook(report, manifest, &backend_proofs);

    let mut proof_pack = SwarmSloReplayStressProofPack {
        schema_version: SWARM_SLO_PROOF_PACK_SCHEMA_VERSION,
        bead_id: SWARM_SLO_PROOF_PACK_BEAD_ID.to_owned(),
        proof_pack_version: SWARM_SLO_PROOF_PACK_VERSION.to_owned(),
        proof_pack_hash: String::new(),
        policy_id: SWARM_SLO_POLICY_ID.to_owned(),
        trace_id: report.trace_id.clone(),
        run_id: report.run_id.clone(),
        scenario_id: report.scenario_id.clone(),
        replay_seed: report.seed,
        schedule_fingerprint: replay_schedule_label(report.schedule).to_owned(),
        profile: proof_pack_profile(report, scorecard),
        evidence_manifest_hash: manifest.artifact_hash.clone(),
        trace_artifact_path: manifest.trace_artifact_path.clone(),
        trace_artifact_hash: manifest.trace_artifact_hash.clone(),
        replay_command: manifest.replay_command.clone(),
        first_failure_diag: scorecard.first_failure_diag.clone(),
        backend_proofs,
        operator_runbook,
        measurement_guardrail: "This proof pack compares governor-off replay metrics with shadow governor decisions; README performance claims still require dated benchmark artifact paths and commits.".to_owned(),
    };
    proof_pack.proof_pack_hash = proof_pack_hash(&proof_pack);
    log_swarm_slo_replay_stress_proof_pack(&proof_pack);
    Ok(proof_pack)
}

fn validate_replay_identity(
    report: &AgentSwarmReplayReport,
    scorecard: &AgentSwarmResourceScorecard,
) -> Result<(), SwarmSloAdapterError> {
    replay_identity_field("run_id", &report.run_id, &scorecard.run_id)?;
    replay_identity_field("trace_id", &report.trace_id, &scorecard.trace_id)?;
    replay_identity_field("scenario_id", &report.scenario_id, &scorecard.scenario_id)
}

fn validate_manifest_identity(
    report: &AgentSwarmReplayReport,
    manifest: &AgentSwarmEvidenceManifest,
) -> Result<(), SwarmSloAdapterError> {
    manifest_identity_field("run_id", &report.run_id, &manifest.run_id)?;
    manifest_identity_field("trace_id", &report.trace_id, &manifest.trace_id)?;
    manifest_identity_field("scenario_id", &report.scenario_id, &manifest.scenario_id)
}

fn replay_identity_field(
    field: &'static str,
    report_value: &str,
    scorecard_value: &str,
) -> Result<(), SwarmSloAdapterError> {
    if report_value == scorecard_value {
        Ok(())
    } else {
        Err(SwarmSloAdapterError::MismatchedReplayIdentity {
            field,
            report_value: report_value.to_owned(),
            scorecard_value: scorecard_value.to_owned(),
        })
    }
}

fn manifest_identity_field(
    field: &'static str,
    report_value: &str,
    manifest_value: &str,
) -> Result<(), SwarmSloAdapterError> {
    if report_value == manifest_value {
        Ok(())
    } else {
        Err(SwarmSloAdapterError::MismatchedEvidenceManifestIdentity {
            field,
            report_value: report_value.to_owned(),
            manifest_value: manifest_value.to_owned(),
        })
    }
}

fn proof_pack_profile(
    report: &AgentSwarmReplayReport,
    scorecard: &AgentSwarmResourceScorecard,
) -> SwarmSloProofPackProfile {
    SwarmSloProofPackProfile {
        profile_id: resource_profile_id_label(scorecard.profile.profile_id).to_owned(),
        available_cores: scorecard.profile.core_count,
        memory_limit_bytes: scorecard.profile.memory_limit_bytes,
        page_cache_limit_bytes: scorecard.profile.page_cache_limit_bytes,
        local_statement_limit: scorecard.profile.local_statement_limit,
        smoke_command: report.commands.smoke_command.clone(),
        heavy_rch_command: scorecard.profile.heavy_rch_command.clone(),
    }
}

fn proof_pack_operator_runbook(
    report: &AgentSwarmReplayReport,
    manifest: &AgentSwarmEvidenceManifest,
    backend_proofs: &[SwarmSloProofPackBackend],
) -> SwarmSloProofPackRunbook {
    let metrics = backend_proofs
        .iter()
        .find(|proof| proof.backend == AgentSwarmReplayBackend::FrankenSqliteConcurrent)
        .map(|proof| &proof.governor_off.coordination_metrics);
    let mut artifact_paths = vec![
        manifest.trace_artifact_path.clone(),
        format!(
            "tests/artifacts/agent-swarm-replay/{}-{}-slo-proof-pack.json",
            report.trace_id, report.run_id
        ),
    ];
    if let Some(path) = manifest.minimized_trace_artifact_path.as_ref() {
        artifact_paths.push(path.clone());
    }

    SwarmSloProofPackRunbook {
        bead_id: AGENT_SWARM_COORDINATION_PROOF_PACK_BEAD_ID.to_owned(),
        scenario_coverage: proof_pack_runbook_checks(metrics),
        artifact_paths,
        proof_commands: vec![
            "cargo test -p fsqlite-harness --lib coordination_e2e_proof_pack_covers_operator_runbook -- --nocapture".to_owned(),
            "timeout 900 rch exec -- env CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/data/tmp/frankensqlite-target-agent-swarm-proof-pack cargo test -p fsqlite-harness --lib coordination_e2e_proof_pack_covers_operator_runbook -- --nocapture".to_owned(),
        ],
        graph_health_commands: vec![
            "br dep cycles --json".to_owned(),
            "bv --robot-insights".to_owned(),
            "bv --robot-triage".to_owned(),
        ],
        hot_range_detection: "SELECT worker_range_start, worker_range_end, range_imbalance_reason, COUNT(*) AS statement_count FROM fsqlite_worker_ranges GROUP BY worker_range_start, worker_range_end, range_imbalance_reason ORDER BY statement_count DESC".to_owned(),
        stuck_lease_detection: "SELECT lease_owner, lease_generation, lease_expires_ms FROM fsqlite_lease WHERE lease_expires_ms <= :now_ms ORDER BY lease_expires_ms".to_owned(),
        fallback_detection: "PRAGMA fsqlite.fallback_events".to_owned(),
        resource_pressure_detection: "SELECT resource_governor_decision, COUNT(*) AS statement_count FROM fsqlite_coordination_events WHERE resource_governor_decision IS NOT NULL GROUP BY resource_governor_decision ORDER BY statement_count DESC".to_owned(),
        known_limitations: vec![
            "The deterministic replay proves artifact wiring and scoring semantics; it does not make a README performance claim.".to_owned(),
            "p999 latency and publish-window pressure remain live-harness measurements, not deterministic replay measurements.".to_owned(),
            "The coordination SQL catalog is still a contract surface until parser/planner/VDBE implementation beads ship it natively.".to_owned(),
            "Fallback counts come from new engine events around each replayed statement. The ring can evict events; per-statement evidence reports drops. Unavailable EXPLAIN CONCURRENCY and governor observations are serialized as null.".to_owned(),
        ],
    }
}

fn proof_pack_runbook_checks(
    metrics: Option<&AgentSwarmCoordinationMetrics>,
) -> Vec<SwarmSloProofPackRunbookCheck> {
    let count = |field: fn(&AgentSwarmCoordinationMetrics) -> u64| metrics.map(field);
    vec![
        SwarmSloProofPackRunbookCheck {
            surface: "queue_claim".to_owned(),
            evidence_field: "coordination_metrics.queue_claim_count".to_owned(),
            observed_count: count(|metrics| metrics.queue_claim_count),
            operator_query: "SELECT queue_claim_id, queue_release_reason FROM fsqlite_queue WHERE queue_claim_id IS NOT NULL".to_owned(),
        },
        SwarmSloProofPackRunbookCheck {
            surface: "queue_release".to_owned(),
            evidence_field: "coordination_metrics.queue_release_count".to_owned(),
            observed_count: count(|metrics| metrics.queue_release_count),
            operator_query: "SELECT queue_claim_id, queue_release_reason FROM fsqlite_queue WHERE queue_release_reason IS NOT NULL".to_owned(),
        },
        SwarmSloProofPackRunbookCheck {
            surface: "lease_expiration".to_owned(),
            evidence_field: "coordination_metrics.lease_expiration_count".to_owned(),
            observed_count: count(|metrics| metrics.lease_expiration_count),
            operator_query: "SELECT lease_owner, lease_generation, lease_expires_ms FROM fsqlite_lease WHERE lease_expires_ms <= :now_ms".to_owned(),
        },
        SwarmSloProofPackRunbookCheck {
            surface: "worker_range".to_owned(),
            evidence_field: "coordination_metrics.range_allocation_count".to_owned(),
            observed_count: count(|metrics| metrics.range_allocation_count),
            operator_query: "SELECT worker_range_start, worker_range_end, range_imbalance_reason FROM fsqlite_worker_ranges".to_owned(),
        },
        SwarmSloProofPackRunbookCheck {
            surface: "contention_diagnostics".to_owned(),
            evidence_field: "coordination_metrics.explain_concurrency_row_count".to_owned(),
            observed_count: metrics.and_then(|metrics| metrics.explain_concurrency_row_count),
            operator_query: "SELECT explain_concurrency_reason FROM fsqlite_coordination_events WHERE explain_concurrency_reason IS NOT NULL".to_owned(),
        },
        SwarmSloProofPackRunbookCheck {
            surface: "fallback_reporting".to_owned(),
            evidence_field: "coordination_metrics.fallback_reason_count".to_owned(),
            observed_count: count(|metrics| metrics.fallback_reason_count),
            operator_query: "PRAGMA fsqlite.fallback_events".to_owned(),
        },
        SwarmSloProofPackRunbookCheck {
            surface: "resource_governor_scoring".to_owned(),
            evidence_field: "coordination_metrics.resource_governor_decision_count".to_owned(),
            observed_count: metrics.and_then(|metrics| metrics.resource_governor_decision_count),
            operator_query: "SELECT resource_governor_decision, COUNT(*) FROM fsqlite_coordination_events WHERE resource_governor_decision IS NOT NULL GROUP BY resource_governor_decision".to_owned(),
        },
    ]
}

fn governor_off_metrics(
    summary: &AgentSwarmBackendSummary,
    scorecard: &AgentSwarmBackendResourceScorecard,
    transaction_count: usize,
    first_failure_diag: &str,
) -> SwarmSloGovernorOffMetrics {
    SwarmSloGovernorOffMetrics {
        statement_count: summary.statements_total,
        transaction_count,
        latency_p50_ns: summary.latency_p50_ns,
        latency_p95_ns: summary.latency_p95_ns,
        latency_p99_ns: summary.latency_p99_ns,
        throughput_statements_per_second_x1000: summary.throughput_statements_per_second_x1000,
        retry_count: summary.retry_count,
        retry_rate_per_mille: scorecard.retry_rate_per_mille,
        abort_count: summary.abort_count,
        abort_rate_per_mille: scorecard.abort_rate_per_mille,
        expected_mismatch_count: summary.expected_mismatch_count,
        conflict_classes: summary.conflict_classes.clone(),
        coordination_metrics: scorecard.coordination_metrics.clone(),
        memory_high_water_bytes: scorecard.memory_high_water_bytes,
        memory_limit_bytes: scorecard.memory_limit_bytes,
        page_cache_bytes: scorecard.page_cache_footprint_bytes,
        first_failure_diag: first_failure_diag.to_owned(),
    }
}

fn proof_pack_hash(proof_pack: &SwarmSloReplayStressProofPack) -> String {
    #[derive(Serialize)]
    struct ProofPackDigest<'a> {
        schema_version: u32,
        bead_id: &'a str,
        proof_pack_version: &'a str,
        policy_id: &'a str,
        trace_id: &'a str,
        run_id: &'a str,
        scenario_id: &'a str,
        replay_seed: u64,
        schedule_fingerprint: &'a str,
        profile: &'a SwarmSloProofPackProfile,
        evidence_manifest_hash: &'a str,
        trace_artifact_path: &'a str,
        trace_artifact_hash: &'a str,
        replay_command: &'a str,
        first_failure_diag: &'a str,
        backend_proofs: &'a [SwarmSloProofPackBackend],
        operator_runbook: &'a SwarmSloProofPackRunbook,
        measurement_guardrail: &'a str,
    }

    stable_json_hash(&ProofPackDigest {
        schema_version: proof_pack.schema_version,
        bead_id: &proof_pack.bead_id,
        proof_pack_version: &proof_pack.proof_pack_version,
        policy_id: &proof_pack.policy_id,
        trace_id: &proof_pack.trace_id,
        run_id: &proof_pack.run_id,
        scenario_id: &proof_pack.scenario_id,
        replay_seed: proof_pack.replay_seed,
        schedule_fingerprint: &proof_pack.schedule_fingerprint,
        profile: &proof_pack.profile,
        evidence_manifest_hash: &proof_pack.evidence_manifest_hash,
        trace_artifact_path: &proof_pack.trace_artifact_path,
        trace_artifact_hash: &proof_pack.trace_artifact_hash,
        replay_command: &proof_pack.replay_command,
        first_failure_diag: &proof_pack.first_failure_diag,
        backend_proofs: &proof_pack.backend_proofs,
        operator_runbook: &proof_pack.operator_runbook,
        measurement_guardrail: &proof_pack.measurement_guardrail,
    })
}

fn stable_json_hash<T>(value: &T) -> String
where
    T: Serialize,
{
    serde_json::to_vec(value).map_or_else(
        |error| sha256_hex(error.to_string().as_bytes()),
        |bytes| sha256_hex(&bytes),
    )
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(out, "{byte:02x}");
    }
    out
}

fn log_swarm_slo_replay_stress_proof_pack(proof_pack: &SwarmSloReplayStressProofPack) {
    for backend in &proof_pack.backend_proofs {
        let (shadow_guardrail, shadow_action) = backend.governor_shadow.as_ref().map_or_else(
            || ("not_applicable".to_owned(), "not_applicable".to_owned()),
            |shadow| (shadow.guardrail_id.clone(), format!("{:?}", shadow.action)),
        );
        tracing::info!(
            target: "fsqlite.slo_governor.proof_pack",
            trace_id = %proof_pack.trace_id,
            run_id = %proof_pack.run_id,
            scenario_id = %proof_pack.scenario_id,
            proof_pack_hash = %proof_pack.proof_pack_hash,
            backend = backend.backend.as_str(),
            profile_id = %proof_pack.profile.profile_id,
            p50_ns = backend.governor_off.latency_p50_ns,
            p95_ns = backend.governor_off.latency_p95_ns,
            p99_ns = backend.governor_off.latency_p99_ns,
            throughput_statements_per_second_x1000 = backend.governor_off.throughput_statements_per_second_x1000,
            retry_count = backend.governor_off.retry_count,
            retry_rate_per_mille = backend.governor_off.retry_rate_per_mille,
            abort_count = backend.governor_off.abort_count,
            abort_rate_per_mille = backend.governor_off.abort_rate_per_mille,
            queue_claim_count = backend.governor_off.coordination_metrics.queue_claim_count,
            lease_expiration_count = backend.governor_off.coordination_metrics.lease_expiration_count,
            range_allocation_count = backend.governor_off.coordination_metrics.range_allocation_count,
            fallback_reason_count = backend.governor_off.coordination_metrics.fallback_reason_count,
            coordination_correctness = backend.governor_off.coordination_metrics.coordination_correctness_per_mille,
            conflict_transparency = backend.governor_off.coordination_metrics.conflict_transparency_per_mille,
            memory_high_water_bytes = backend.governor_off.memory_high_water_bytes,
            governor_shadow_guardrail = %shadow_guardrail,
            governor_shadow_action = %shadow_action,
            first_failure_diag = %backend.governor_off.first_failure_diag,
            heavy_rch_command = %proof_pack.profile.heavy_rch_command,
            runbook_check_count = proof_pack.operator_runbook.scenario_coverage.len(),
            runbook_artifact_count = proof_pack.operator_runbook.artifact_paths.len(),
            "swarm slo replay stress proof pack",
        );
    }
}

fn writer_actor_count(statements: &[AgentSwarmStatementReplay]) -> u32 {
    let mut writers = BTreeSet::new();
    for statement in statements {
        if statement_is_write_work(statement) {
            writers.insert(statement.actor_id.as_str());
        }
    }
    capped_u32(writers.len())
}

fn statement_is_write_work(statement: &AgentSwarmStatementReplay) -> bool {
    let keyword = first_sql_keyword(&statement.materialized_sql);
    WRITE_KEYWORDS
        .iter()
        .any(|candidate| keyword.eq_ignore_ascii_case(candidate))
}

fn first_sql_keyword(sql: &str) -> &str {
    sql.trim_start()
        .split(|c: char| !c.is_ascii_alphabetic())
        .next()
        .unwrap_or_default()
}

const WRITE_KEYWORDS: &[&str] = &[
    "INSERT", "UPDATE", "DELETE", "REPLACE", "CREATE", "DROP", "ALTER", "VACUUM", "PRAGMA",
];

fn replay_degraded_signals() -> Vec<SwarmSloDegradedSignal> {
    vec![
        SwarmSloDegradedSignal::MissingP999,
        SwarmSloDegradedSignal::MissingPublishWindow,
        SwarmSloDegradedSignal::MissingLiveMetrics,
        SwarmSloDegradedSignal::ReplayOnlyInput,
        SwarmSloDegradedSignal::PrivacyRedactedInput,
    ]
}

fn unique_count_u32<'a>(values: impl Iterator<Item = &'a str>) -> u32 {
    capped_u32(values.collect::<BTreeSet<_>>().len())
}

fn capped_u32(value: usize) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

fn capped_u32_from_u64(value: u64) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

fn capped_u16(value: u64) -> u16 {
    u16::try_from(value).unwrap_or(u16::MAX)
}

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

const fn resource_profile_id_label(profile_id: AgentSwarmResourceProfileId) -> &'static str {
    match profile_id {
        AgentSwarmResourceProfileId::LocalSmoke => "local_smoke",
        AgentSwarmResourceProfileId::Workstation => "workstation",
        AgentSwarmResourceProfileId::HighCapacityServer => "high_capacity_server",
    }
}

const fn replay_schedule_label(schedule: AgentSwarmReplaySchedule) -> &'static str {
    match schedule {
        AgentSwarmReplaySchedule::TraceOrder => "trace_order",
        AgentSwarmReplaySchedule::ConcurrencyGroupThenOrder => "concurrency_group_then_order",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent_swarm_trace::{
        AGENT_SWARM_COORDINATION_BRIDGE_VERSION, AgentSwarmEvidenceManifestConfig,
        AgentSwarmReplayConfig, AgentSwarmResourceProfile, AgentSwarmResourceScorecardConfig,
        AgentSwarmTrace, ExpectedResultClass, FIRST_FAILURE_DIAGNOSTIC_ABSENT, RawTraceStatement,
        RowCountClass, StatementSource, TraceMetadata, TraceStatement, TransactionBoundary,
        build_agent_swarm_evidence_manifest, load_agent_swarm_trace_json, replay_agent_swarm_trace,
        score_agent_swarm_resource_envelope,
    };
    use crate::slo_governor::{SwarmSloAction, evaluate_swarm_slo_once};

    const GOLDEN_TRACE_JSON: &str =
        include_str!("../conformance/agent_swarm_trace_sanitized_golden.json");
    const GOLDEN_TRACE_PATH: &str =
        "crates/fsqlite-harness/conformance/agent_swarm_trace_sanitized_golden.json";

    #[test]
    fn golden_replay_adapts_without_fabricating_live_only_signals()
    -> Result<(), Box<dyn std::error::Error>> {
        let trace = load_agent_swarm_trace_json(GOLDEN_TRACE_JSON)?;
        let replay_config = AgentSwarmReplayConfig::smoke(0x51_0A_DA_7A);
        let report = replay_agent_swarm_trace(&trace, &replay_config)?;
        let scorecard_config =
            AgentSwarmResourceScorecardConfig::new(AgentSwarmResourceProfile::local_smoke());
        let scorecard = score_agent_swarm_resource_envelope(&report, &scorecard_config);
        let manifest_config = AgentSwarmEvidenceManifestConfig::new(
            GOLDEN_TRACE_PATH,
            replay_config.commands.smoke_command.clone(),
        )
        .without_regression_proposals();
        let manifest = build_agent_swarm_evidence_manifest(&report, &manifest_config);
        let adapter_config =
            SwarmSloReplayAdapterConfig::default().with_evidence_manifest(&manifest);

        let input = adapt_replay_scorecard_to_swarm_slo_input(
            &report,
            &scorecard,
            AgentSwarmReplayBackend::FrankenSqliteConcurrent,
            &adapter_config,
        )?;

        assert_eq!(input.sample_source, SwarmSloSampleSource::Replay);
        assert_eq!(input.trace_id, trace.trace_id);
        assert_eq!(input.run_id, trace.run_id);
        assert_eq!(input.scenario_id, trace.scenario_id);
        assert_eq!(input.artifact_path.as_deref(), Some(GOLDEN_TRACE_PATH));
        assert_eq!(input.first_failure_diag, scorecard.first_failure_diag);
        assert!(
            input
                .first_failure_diag
                .contains("topology_error: connection conn-b"),
        );
        assert_eq!(input.latency_p999_ns, None);
        assert_eq!(input.publish_window_occupancy, None);
        assert_eq!(input.publish_window_p99_ns, None);
        assert!(input.concurrent_mode_default_observed);
        assert!(
            input
                .degraded_signals
                .contains(&SwarmSloDegradedSignal::MissingP999),
        );

        let decision = evaluate_swarm_slo_once(&input);
        assert_ne!(decision.guardrail_id, "G11");
        assert!(
            decision
                .degraded_signals
                .contains(&SwarmSloDegradedSignal::MissingPublishWindow),
        );
        assert!(
            decision
                .degraded_signals
                .contains(&SwarmSloDegradedSignal::ReplayOnlyInput),
        );

        Ok(())
    }

    #[test]
    fn live_harness_sample_preserves_measured_publish_and_p999_signals() {
        let sample = healthy_live_sample();
        let input = adapt_live_harness_sample_to_swarm_slo_input(sample);

        assert_eq!(input.sample_source, SwarmSloSampleSource::LiveHarness);
        assert_eq!(input.publish_window_occupancy, Some(2));
        assert_eq!(input.publish_window_p99_ns, Some(700_000));
        assert_eq!(input.latency_p999_ns, Some(4_000_000));
        assert_eq!(input.first_failure_diag, FIRST_FAILURE_DIAGNOSTIC_ABSENT);

        let decision = evaluate_swarm_slo_once(&input);
        assert_eq!(decision.action, SwarmSloAction::Admit);
        assert!(
            !decision
                .degraded_signals
                .contains(&SwarmSloDegradedSignal::MissingP999),
        );
        assert!(
            !decision
                .degraded_signals
                .contains(&SwarmSloDegradedSignal::MissingPublishWindow),
        );
    }

    #[test]
    fn missing_scorecard_backend_is_reported() -> Result<(), Box<dyn std::error::Error>> {
        let trace = load_agent_swarm_trace_json(GOLDEN_TRACE_JSON)?;
        let replay_config = AgentSwarmReplayConfig::smoke(0xB4_CE);
        let report = replay_agent_swarm_trace(&trace, &replay_config)?;
        let scorecard_config =
            AgentSwarmResourceScorecardConfig::new(AgentSwarmResourceProfile::local_smoke());
        let mut scorecard = score_agent_swarm_resource_envelope(&report, &scorecard_config);
        scorecard
            .backends
            .retain(|backend| backend.backend != AgentSwarmReplayBackend::CSqliteOracle);

        let Err(error) = adapt_replay_scorecard_to_swarm_slo_input(
            &report,
            &scorecard,
            AgentSwarmReplayBackend::CSqliteOracle,
            &SwarmSloReplayAdapterConfig::default(),
        ) else {
            return Err(std::io::Error::other("missing backend was accepted").into());
        };

        assert_eq!(
            error,
            SwarmSloAdapterError::ScorecardBackendMissing {
                backend: AgentSwarmReplayBackend::CSqliteOracle,
            },
        );

        Ok(())
    }

    #[test]
    fn coordination_e2e_proof_pack_covers_operator_runbook()
    -> Result<(), Box<dyn std::error::Error>> {
        let trace = coordination_e2e_trace()?;
        let replay_config =
            AgentSwarmReplayConfig::smoke(0xC0_0D_10).with_schema_sql(coordination_e2e_schema());
        let report = replay_agent_swarm_trace(&trace, &replay_config)?;
        assert!(
            report.first_failure.is_none(),
            "coordination proof replay should be clean: {:?}",
            report.first_failure
        );

        let scorecard_config = AgentSwarmResourceScorecardConfig::new(
            AgentSwarmResourceProfile::high_capacity_server(),
        );
        let scorecard = score_agent_swarm_resource_envelope(&report, &scorecard_config);
        let manifest_config = AgentSwarmEvidenceManifestConfig::new(
            "tests/artifacts/agent-swarm-replay/coordination-e2e-trace.json",
            replay_config.commands.smoke_command.clone(),
        )
        .with_trace_artifact_hash("coordination-e2e-trace-hash-stable-under-privacy-scrubbing")
        .without_regression_proposals();
        let manifest = build_agent_swarm_evidence_manifest(&report, &manifest_config);
        let adapter_config = SwarmSloReplayAdapterConfig {
            sample_ts_ms: 1_779_724_800_000,
            sample_age_ms: 0,
            configured_helper_threads: 64,
            active_helper_threads: 16,
            evidence_worker_count: 8,
            ..SwarmSloReplayAdapterConfig::default()
        }
        .with_evidence_manifest(&manifest);

        let proof_pack = build_swarm_slo_replay_stress_proof_pack(
            &report,
            &scorecard,
            &manifest,
            &adapter_config,
            &SwarmSloGovernorConfig::default(),
        )?;

        let subject_proof = proof_pack
            .backend_proofs
            .iter()
            .find(|proof| proof.backend == AgentSwarmReplayBackend::FrankenSqliteConcurrent)
            .ok_or_else(|| std::io::Error::other("missing subject proof"))?;
        let metrics = &subject_proof.governor_off.coordination_metrics;
        assert!(metrics.queue_claim_count > 0);
        assert!(metrics.queue_release_count > 0);
        assert!(metrics.lease_expiration_count > 0);
        assert!(metrics.range_allocation_count > 0);
        assert_eq!(metrics.explain_concurrency_row_count, None);
        assert_eq!(metrics.fallback_reason_count, 1);
        assert_eq!(metrics.resource_governor_decision_count, None);
        let subject = report
            .backends
            .iter()
            .find(|backend| {
                backend.identity.backend == AgentSwarmReplayBackend::FrankenSqliteConcurrent
            })
            .ok_or_else(|| std::io::Error::other("missing subject replay"))?;
        for statement in &subject.statements {
            let evidence = statement
                .fallback_evidence
                .as_ref()
                .expect("real executor evidence");
            assert_eq!(evidence.dropped_events, 0);
            if statement.logical_order == 7 {
                assert_eq!(evidence.events.len(), 1);
                let event = &evidence.events[0];
                assert_eq!(event.decision_reason, "with_clause_materialization");
                assert_eq!(event.decision_outcome, "allowed_compatibility_fallback");
                assert_eq!(event.storage_backend, "memory");
                assert!(event.event_id > 0);
                assert!(event.statement_id > 0);
            } else {
                assert!(
                    evidence.events.is_empty(),
                    "synthetic rows, labels and later reads are not fallback events: {statement:?}"
                );
            }
        }
        let oracle = report
            .backends
            .iter()
            .find(|backend| backend.identity.backend == AgentSwarmReplayBackend::CSqliteOracle)
            .ok_or_else(|| std::io::Error::other("missing oracle replay"))?;
        assert!(
            oracle
                .statements
                .iter()
                .all(|statement| statement.fallback_evidence.is_none())
        );
        assert!(subject_proof.governor_shadow.is_some());

        assert_eq!(
            proof_pack.operator_runbook.bead_id,
            AGENT_SWARM_COORDINATION_PROOF_PACK_BEAD_ID
        );
        assert!(
            proof_pack
                .operator_runbook
                .scenario_coverage
                .iter()
                .all(|check| match check.surface.as_str() {
                    "contention_diagnostics" | "resource_governor_scoring" =>
                        check.observed_count.is_none(),
                    _ => check.observed_count.is_some_and(|count| count > 0),
                }),
            "runbook coverage missing observed counts: {:?}",
            proof_pack.operator_runbook.scenario_coverage
        );
        assert!(
            proof_pack
                .operator_runbook
                .graph_health_commands
                .iter()
                .any(|command| command == "br dep cycles --json")
        );
        assert!(
            proof_pack
                .operator_runbook
                .graph_health_commands
                .iter()
                .any(|command| command == "bv --robot-insights")
        );
        assert!(
            proof_pack
                .operator_runbook
                .proof_commands
                .iter()
                .any(|command| {
                    command.contains("rch exec")
                        && command.contains("coordination_e2e_proof_pack_covers_operator_runbook")
                })
        );
        assert!(
            proof_pack
                .operator_runbook
                .artifact_paths
                .iter()
                .any(|path| {
                    path == "tests/artifacts/agent-swarm-replay/coordination-e2e-trace.json"
                })
        );
        assert!(
            proof_pack
                .operator_runbook
                .hot_range_detection
                .contains("range_imbalance_reason")
        );
        assert!(
            proof_pack
                .operator_runbook
                .stuck_lease_detection
                .contains("lease_expires_ms")
        );
        assert!(
            proof_pack
                .operator_runbook
                .fallback_detection
                .contains("PRAGMA fsqlite.fallback_events")
        );
        assert!(
            proof_pack
                .operator_runbook
                .resource_pressure_detection
                .contains("resource_governor_decision")
        );
        assert!(
            proof_pack
                .operator_runbook
                .known_limitations
                .iter()
                .any(|limitation| limitation.contains("does not make a README performance claim"))
        );

        let encoded = serde_json::to_string(&proof_pack)?;
        assert!(encoded.contains("operator_runbook"));
        assert!(encoded.contains("queue_release"));
        assert!(encoded.contains("resource_governor_scoring"));
        assert!(encoded.contains("bv --robot-insights"));

        Ok(())
    }

    #[test]
    fn replay_stress_proof_pack_compares_off_metrics_with_shadow_decision()
    -> Result<(), Box<dyn std::error::Error>> {
        let trace = load_agent_swarm_trace_json(GOLDEN_TRACE_JSON)?;
        let replay_config = AgentSwarmReplayConfig::smoke(0x5EED_5EED);
        let report = replay_agent_swarm_trace(&trace, &replay_config)?;
        let scorecard_config = AgentSwarmResourceScorecardConfig::new(
            AgentSwarmResourceProfile::high_capacity_server(),
        );
        let scorecard = score_agent_swarm_resource_envelope(&report, &scorecard_config);
        let manifest_config = AgentSwarmEvidenceManifestConfig::new(
            GOLDEN_TRACE_PATH,
            replay_config.commands.smoke_command.clone(),
        )
        .without_regression_proposals();
        let manifest = build_agent_swarm_evidence_manifest(&report, &manifest_config);
        let adapter_config = SwarmSloReplayAdapterConfig {
            sample_ts_ms: 1_779_543_600_000,
            sample_age_ms: 0,
            configured_helper_threads: 4,
            active_helper_threads: 2,
            evidence_worker_count: 1,
            ..SwarmSloReplayAdapterConfig::default()
        }
        .with_evidence_manifest(&manifest);

        let proof_pack = build_swarm_slo_replay_stress_proof_pack(
            &report,
            &scorecard,
            &manifest,
            &adapter_config,
            &SwarmSloGovernorConfig::default(),
        )?;

        assert_eq!(
            proof_pack.schema_version,
            SWARM_SLO_PROOF_PACK_SCHEMA_VERSION
        );
        assert_eq!(proof_pack.bead_id, SWARM_SLO_PROOF_PACK_BEAD_ID);
        assert_eq!(proof_pack.policy_id, SWARM_SLO_POLICY_ID);
        assert_eq!(proof_pack.profile.available_cores, 64);
        assert_eq!(
            proof_pack.profile.memory_limit_bytes,
            256 * 1024 * 1024 * 1024
        );
        assert!(proof_pack.profile.smoke_command.contains("cargo test"));
        assert!(proof_pack.profile.heavy_rch_command.contains("rch exec"));
        assert_eq!(proof_pack.backend_proofs.len(), 2);
        assert_eq!(proof_pack.proof_pack_hash.len(), 64);
        assert!(
            proof_pack
                .proof_pack_hash
                .chars()
                .all(|ch| ch.is_ascii_hexdigit())
        );

        let subject_report = report
            .backends
            .iter()
            .find(|backend| {
                backend.identity.backend == AgentSwarmReplayBackend::FrankenSqliteConcurrent
            })
            .ok_or_else(|| std::io::Error::other("missing subject report"))?;
        let subject_scorecard = scorecard
            .backends
            .iter()
            .find(|backend| backend.backend == AgentSwarmReplayBackend::FrankenSqliteConcurrent)
            .ok_or_else(|| std::io::Error::other("missing subject scorecard"))?;
        let subject_proof = proof_pack
            .backend_proofs
            .iter()
            .find(|proof| proof.backend == AgentSwarmReplayBackend::FrankenSqliteConcurrent)
            .ok_or_else(|| std::io::Error::other("missing subject proof"))?;

        assert!(subject_proof.concurrent_writer_default);
        assert_eq!(
            subject_proof.governor_off.statement_count,
            subject_report.summary.statements_total
        );
        assert_eq!(
            subject_proof.governor_off.latency_p50_ns,
            subject_report.summary.latency_p50_ns
        );
        assert_eq!(
            subject_proof.governor_off.latency_p95_ns,
            subject_report.summary.latency_p95_ns
        );
        assert_eq!(
            subject_proof.governor_off.latency_p99_ns,
            subject_report.summary.latency_p99_ns
        );
        assert_eq!(
            subject_proof
                .governor_off
                .throughput_statements_per_second_x1000,
            subject_report
                .summary
                .throughput_statements_per_second_x1000
        );
        assert_eq!(
            subject_proof.governor_off.retry_rate_per_mille,
            subject_scorecard.retry_rate_per_mille
        );
        assert_eq!(
            subject_proof.governor_off.abort_rate_per_mille,
            subject_scorecard.abort_rate_per_mille
        );
        assert_eq!(
            subject_proof.governor_off.memory_high_water_bytes,
            subject_scorecard.memory_high_water_bytes
        );
        assert_eq!(
            subject_proof
                .governor_off
                .coordination_metrics
                .bridge_version,
            AGENT_SWARM_COORDINATION_BRIDGE_VERSION
        );
        assert_eq!(
            subject_proof.governor_off.coordination_metrics,
            subject_scorecard.coordination_metrics
        );
        assert!(
            subject_proof
                .governor_off
                .coordination_metrics
                .trace_fields
                .iter()
                .any(|field| field == "resource_governor_decision")
        );

        let shadow = subject_proof
            .governor_shadow
            .as_ref()
            .ok_or_else(|| std::io::Error::other("missing subject shadow proof"))?;
        assert_eq!(shadow.control_mode, SwarmSloControlMode::Shadow);
        assert!(
            shadow
                .operator_report
                .evidence
                .artifact_path
                .as_deref()
                .is_some_and(|path| path == GOLDEN_TRACE_PATH)
        );
        assert!(shadow.budgets.iter().any(|budget| {
            budget.metric == "latency_p99_ns"
                && budget.observed == Some(subject_report.summary.latency_p99_ns)
        }));
        assert!(
            shadow
                .operator_report
                .measurement_guardrail
                .contains("does not claim performance improvement")
        );

        let oracle_proof = proof_pack
            .backend_proofs
            .iter()
            .find(|proof| proof.backend == AgentSwarmReplayBackend::CSqliteOracle)
            .ok_or_else(|| std::io::Error::other("missing oracle proof"))?;
        assert!(!oracle_proof.concurrent_writer_default);
        assert!(oracle_proof.governor_shadow.is_none());
        assert!(
            oracle_proof
                .shadow_skip_reason
                .as_deref()
                .is_some_and(|reason| reason.contains("FrankenSQLite concurrent-default"))
        );

        let encoded = serde_json::to_string(&proof_pack)?;
        assert!(encoded.contains("governor_off"));
        assert!(encoded.contains("governor_shadow"));
        assert!(encoded.contains("coordination_metrics"));
        assert!(encoded.contains("operator_runbook"));
        assert!(encoded.contains("heavy_rch_command"));
        assert!(encoded.contains("README performance claims still require"));

        Ok(())
    }

    fn coordination_e2e_trace() -> Result<AgentSwarmTrace, Box<dyn std::error::Error>> {
        let metadata = TraceMetadata::new(
            "inline-harness",
            AGENT_SWARM_COORDINATION_PROOF_PACK_BEAD_ID,
        )?;
        let statements = vec![
            coordination_e2e_statement(
                0,
                "worker-a",
                "queue-claim",
                "UPDATE fsqlite_queue SET claim_state = claim_state + 1, queue_claim_id = 'claim-a' WHERE status_code = 0",
            )?,
            coordination_e2e_statement(
                1,
                "worker-a",
                "queue-release",
                "UPDATE fsqlite_queue SET status_code = 1, queue_release_reason = 'completed' WHERE status_code = 0",
            )?,
            coordination_e2e_statement(
                2,
                "worker-b",
                "expired-lease-takeover",
                "UPDATE fsqlite_lease SET lease_owner = 'worker-b', lease_generation = lease_generation + 1 WHERE lease_expires_ms <= 100",
            )?,
            coordination_e2e_statement(
                3,
                "worker-c",
                "range-imbalance-split",
                "INSERT INTO fsqlite_worker_ranges(worker_range_start, worker_range_end, range_imbalance_reason) VALUES (0, 64, 'split')",
            )?,
            coordination_e2e_statement(
                4,
                "worker-d",
                "explain-concurrency-conflict",
                "SELECT explain_concurrency_reason FROM fsqlite_coordination_events WHERE event_id = 0",
            )?,
            coordination_e2e_statement(
                5,
                "worker-d",
                "compatibility-fallback",
                "SELECT fallback_reason FROM fsqlite_coordination_events WHERE fallback_reason IS NOT NULL",
            )?,
            coordination_e2e_statement(
                6,
                "worker-e",
                "resource-governor-shadow",
                "SELECT resource_governor_decision FROM fsqlite_coordination_events WHERE resource_governor_decision IS NOT NULL",
            )?,
            coordination_e2e_statement(
                7,
                "worker-e",
                "ordinary-query",
                "WITH source(value) AS (SELECT 7) SELECT value FROM source",
            )?,
            coordination_e2e_statement(
                8,
                "worker-e",
                "compatibility-fallback",
                "SELECT fallback_reason FROM fsqlite_coordination_events WHERE event_id = 0",
            )?,
        ];

        Ok(AgentSwarmTrace::new(
            "trace-coordination-e2e-proof-pack",
            "run-coordination-e2e-proof-pack",
            "coordination-e2e-proof-pack",
            metadata,
            statements,
        )?)
    }

    fn coordination_e2e_statement(
        logical_order: u64,
        actor_id: &'static str,
        workload_phase: &'static str,
        sql: &'static str,
    ) -> Result<TraceStatement, Box<dyn std::error::Error>> {
        Ok(TraceStatement::from_raw(RawTraceStatement {
            logical_order,
            logical_timestamp: None,
            actor_id,
            connection_id: actor_id,
            transaction_id: None,
            transaction_boundary: TransactionBoundary::None,
            concurrency_group: "coordination-e2e-proof",
            workload_phase,
            sql,
            expected_result_class: ExpectedResultClass::Success,
            row_count_class: RowCountClass::Unknown,
            error_class: None,
            source: StatementSource::new(
                "inline-harness",
                format!("coordination-e2e-{logical_order}"),
            )?,
        })?)
    }

    fn coordination_e2e_schema() -> Vec<String> {
        vec![
            "CREATE TABLE fsqlite_queue(id INTEGER PRIMARY KEY, status_code INTEGER NOT NULL, claim_state INTEGER NOT NULL, queue_claim_id TEXT, queue_release_reason TEXT)".to_owned(),
            "INSERT INTO fsqlite_queue(id, status_code, claim_state, queue_claim_id, queue_release_reason) VALUES (0, 0, 0, NULL, NULL)".to_owned(),
            "CREATE TABLE fsqlite_lease(id INTEGER PRIMARY KEY, lease_owner TEXT NOT NULL, lease_generation INTEGER NOT NULL, lease_expires_ms INTEGER NOT NULL)".to_owned(),
            "INSERT INTO fsqlite_lease(id, lease_owner, lease_generation, lease_expires_ms) VALUES (0, 'redacted-text', 0, 0)".to_owned(),
            "CREATE TABLE fsqlite_worker_ranges(worker_range_start INTEGER NOT NULL, worker_range_end INTEGER NOT NULL, range_imbalance_reason TEXT)".to_owned(),
            "CREATE TABLE fsqlite_coordination_events(event_id INTEGER PRIMARY KEY, explain_concurrency_reason TEXT, fallback_reason TEXT, resource_governor_decision TEXT)".to_owned(),
            "INSERT INTO fsqlite_coordination_events(event_id, explain_concurrency_reason, fallback_reason, resource_governor_decision) VALUES (0, 'hot_page_predicted', 'compatibility_fallback', 'shed_optional_work')".to_owned(),
        ]
    }

    fn healthy_live_sample() -> SwarmSloLiveHarnessSample {
        SwarmSloLiveHarnessSample {
            run_id: "run-live".to_owned(),
            trace_id: "trace-live".to_owned(),
            scenario_id: "scenario-live".to_owned(),
            backend: AgentSwarmReplayBackend::FrankenSqliteConcurrent
                .as_str()
                .to_owned(),
            profile_id: "local_smoke".to_owned(),
            control_mode: SwarmSloControlMode::Shadow,
            first_failure_diag: FIRST_FAILURE_DIAGNOSTIC_ABSENT.to_owned(),
            sample_ts_ms: 1_000,
            sample_age_ms: 100,
            sample_window_ms: 1_000,
            actor_count: 4,
            connection_count: 4,
            writer_count: 2,
            reader_count: 2,
            statement_count: 100,
            transaction_count: 20,
            concurrency_level: 4,
            schedule_fingerprint: "live-smoke".to_owned(),
            available_cores: 16,
            configured_helper_threads: 3,
            active_helper_threads: 2,
            memory_limit_bytes: 32 * 1024 * 1024,
            memory_high_water_bytes: 4 * 1024 * 1024,
            page_cache_bytes: 2 * 1024 * 1024,
            cpu_utilization_per_mille: 150,
            active_writers: 2,
            publish_window_occupancy: Some(2),
            publish_window_p99_ns: Some(700_000),
            retry_count: 0,
            retry_rate_per_mille: 0,
            abort_count: 0,
            abort_rate_per_mille: 0,
            evidence_queue_depth: 1,
            evidence_queue_drops: 0,
            evidence_worker_count: 1,
            wakeup_queue_depth: 0,
            wakeup_to_run_p95_ns: Some(80_000),
            wakeup_to_run_p99_ns: Some(120_000),
            max_chain_depth: 4,
            gc_tier: SwarmSloGcTier::Normal,
            gc_inline_active: false,
            wal_frames_pending_checkpoint: 32,
            checkpoint_active: false,
            invalidation_queue_depth: 0,
            invalidation_fallback_count: 0,
            build_or_test_saturation_per_mille: 0,
            agent_wedge_risk: false,
            latency_p50_ns: 1_000_000,
            latency_p95_ns: 2_000_000,
            latency_p99_ns: 3_000_000,
            latency_p999_ns: Some(4_000_000),
            privacy_redacted: false,
            concurrent_mode_default_observed: true,
            degraded_signals: Vec::new(),
        }
    }
}
