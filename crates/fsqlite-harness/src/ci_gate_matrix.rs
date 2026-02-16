//! CI gate matrix, artifact publication, flake budgets, and auto-bisect hooks (bd-1dp9.7.3).
//!
//! Provides the programmatic contracts for wiring CI to run unit/e2e/differential/perf gates
//! with artifact publishing, deterministic retries, flake budget policies, and auto-bisect
//! hooks.
//!
//! # Flake Budget
//!
//! Each CI lane has a configurable flake budget (maximum allowed flake rate). A test result
//! is classified as a "flake" when it fails intermittently — succeeds on retry with the same
//! seed. The [`FlakeBudgetPolicy`] enforces per-lane and global flake rate limits.
//!
//! # Auto-Bisect Hooks
//!
//! When a regression is detected (gate failure that is not a flake), the [`AutoBisectHook`]
//! produces a [`BisectRequest`] with the commit range, deterministic replay seed, and
//! failing gate identifier. CI can consume this to trigger automated bisection.
//!
//! # Artifact Publication
//!
//! The [`ArtifactManifest`] defines the structured output contract for CI artifacts.
//! Each gate run produces a manifest entry with checksums, paths, and metadata.

use std::collections::BTreeMap;
use std::fmt::Write as FmtWrite;

use serde::{Deserialize, Serialize};

use crate::verification_contract_enforcement::ContractEnforcementOutcome;

#[allow(dead_code)]
const BEAD_ID: &str = "bd-1dp9.7.3";

// ---- CI Lane Definitions ----

/// Classification of CI gate lanes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum CiLane {
    /// Unit tests across workspace crates.
    Unit,
    /// End-to-end differential tests (fsqlite vs C SQLite).
    E2eDifferential,
    /// Correctness scenario tests (deterministic seeds).
    E2eCorrectness,
    /// Recovery and crash scenario tests.
    E2eRecovery,
    /// Performance regression detection.
    Performance,
    /// Schema/log validation gates.
    SchemaValidation,
    /// Scenario coverage drift enforcement.
    CoverageDrift,
}

impl CiLane {
    /// All defined CI lanes.
    pub const ALL: [Self; 7] = [
        Self::Unit,
        Self::E2eDifferential,
        Self::E2eCorrectness,
        Self::E2eRecovery,
        Self::Performance,
        Self::SchemaValidation,
        Self::CoverageDrift,
    ];

    /// Stable string identifier for this lane.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unit => "unit",
            Self::E2eDifferential => "e2e-differential",
            Self::E2eCorrectness => "e2e-correctness",
            Self::E2eRecovery => "e2e-recovery",
            Self::Performance => "performance",
            Self::SchemaValidation => "schema-validation",
            Self::CoverageDrift => "coverage-drift",
        }
    }

    /// Whether this lane supports deterministic retry (flake detection).
    #[must_use]
    pub const fn supports_retry(self) -> bool {
        match self {
            Self::Unit | Self::E2eDifferential | Self::E2eCorrectness | Self::E2eRecovery => true,
            Self::Performance | Self::SchemaValidation | Self::CoverageDrift => false,
        }
    }
}

// ---- Flake Budget Policy ----

/// Escalation level derived from observed flake rate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FlakeEscalationLevel {
    /// Below warning threshold.
    None,
    /// At or above warning threshold but below critical threshold.
    Warn,
    /// At or above critical threshold.
    Critical,
}

impl FlakeEscalationLevel {
    /// Stable string label for reports and structured logs.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Warn => "warn",
            Self::Critical => "critical",
        }
    }
}

/// Configuration for flake budget enforcement on a single CI lane.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LaneFlakeBudget {
    /// Maximum allowed flake rate (0.0 = no flakes allowed, 1.0 = all flakes allowed).
    pub max_flake_rate: f64,
    /// Warning threshold for escalation.
    #[serde(default = "default_warn_flake_rate")]
    pub warn_flake_rate: f64,
    /// Critical threshold for escalation.
    #[serde(default = "default_critical_flake_rate")]
    pub critical_flake_rate: f64,
    /// Maximum number of retries for flake detection.
    pub max_retries: u32,
    /// Whether flakes in this lane are blocking (fail the pipeline) or advisory.
    pub blocking: bool,
}

#[must_use]
const fn default_warn_flake_rate() -> f64 {
    0.03
}

#[must_use]
const fn default_critical_flake_rate() -> f64 {
    0.08
}

impl LaneFlakeBudget {
    /// Default budget: 5% flake rate, 2 retries, blocking.
    #[must_use]
    pub fn default_strict() -> Self {
        Self {
            max_flake_rate: 0.05,
            warn_flake_rate: 0.03,
            critical_flake_rate: 0.08,
            max_retries: 2,
            blocking: true,
        }
    }

    /// Relaxed budget for performance lanes (higher variance expected).
    #[must_use]
    pub fn default_relaxed() -> Self {
        Self {
            max_flake_rate: 0.10,
            warn_flake_rate: 0.07,
            critical_flake_rate: 0.15,
            max_retries: 3,
            blocking: false,
        }
    }

    /// Determine the escalation level for the observed flake rate.
    #[must_use]
    pub fn escalation_level(&self, flake_rate: f64) -> FlakeEscalationLevel {
        if flake_rate >= self.critical_flake_rate {
            FlakeEscalationLevel::Critical
        } else if flake_rate >= self.warn_flake_rate {
            FlakeEscalationLevel::Warn
        } else {
            FlakeEscalationLevel::None
        }
    }
}

/// Global flake budget policy across all CI lanes.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FlakeBudgetPolicy {
    /// Per-lane budgets. Lanes not in this map use the default budget.
    pub lane_budgets: BTreeMap<String, LaneFlakeBudget>,
    /// Global maximum flake rate across all lanes combined.
    pub global_max_flake_rate: f64,
    /// Schema version for policy serialization.
    pub schema_version: String,
}

impl FlakeBudgetPolicy {
    /// Build the canonical flake budget policy.
    #[must_use]
    pub fn canonical() -> Self {
        let mut lane_budgets = BTreeMap::new();
        for lane in CiLane::ALL {
            let budget = match lane {
                CiLane::Performance => LaneFlakeBudget::default_relaxed(),
                _ => LaneFlakeBudget::default_strict(),
            };
            lane_budgets.insert(lane.as_str().to_owned(), budget);
        }
        Self {
            lane_budgets,
            global_max_flake_rate: 0.05,
            schema_version: "1.0.0".to_owned(),
        }
    }

    /// Look up the budget for a lane. Returns the default strict budget if not configured.
    #[must_use]
    pub fn budget_for(&self, lane: CiLane) -> LaneFlakeBudget {
        self.lane_budgets
            .get(lane.as_str())
            .cloned()
            .unwrap_or_else(LaneFlakeBudget::default_strict)
    }
}

/// Outcome of a single test execution (pass, fail, or flake).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TestOutcome {
    /// Test passed on first attempt.
    Pass,
    /// Test failed consistently across all retry attempts.
    Fail,
    /// Test failed initially but passed on retry (intermittent failure).
    Flake,
    /// Test was skipped.
    Skip,
}

/// Result of evaluating flake budget for a lane.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FlakeBudgetResult {
    pub lane: String,
    pub total_tests: usize,
    pub pass_count: usize,
    pub fail_count: usize,
    pub flake_count: usize,
    pub skip_count: usize,
    pub flake_rate: f64,
    pub budget_max_flake_rate: f64,
    pub escalation_warn_flake_rate: f64,
    pub escalation_critical_flake_rate: f64,
    pub escalation_level: FlakeEscalationLevel,
    pub within_budget: bool,
    pub blocking: bool,
    /// Whether this lane's result should fail the overall pipeline.
    pub pipeline_fail: bool,
}

/// Evaluate the flake budget for a lane given test outcomes.
#[must_use]
pub fn evaluate_flake_budget(
    lane: CiLane,
    outcomes: &[TestOutcome],
    policy: &FlakeBudgetPolicy,
) -> FlakeBudgetResult {
    let budget = policy.budget_for(lane);
    let total = outcomes.len();
    let pass_count = outcomes.iter().filter(|o| **o == TestOutcome::Pass).count();
    let fail_count = outcomes.iter().filter(|o| **o == TestOutcome::Fail).count();
    let flake_count = outcomes
        .iter()
        .filter(|o| **o == TestOutcome::Flake)
        .count();
    let skip_count = outcomes.iter().filter(|o| **o == TestOutcome::Skip).count();

    let executed = total - skip_count;
    let flake_rate = if executed > 0 {
        flake_count as f64 / executed as f64
    } else {
        0.0
    };

    let within_budget = flake_rate <= budget.max_flake_rate;
    let escalation_level = budget.escalation_level(flake_rate);
    let pipeline_fail = (!within_budget && budget.blocking) || fail_count > 0;

    FlakeBudgetResult {
        lane: lane.as_str().to_owned(),
        total_tests: total,
        pass_count,
        fail_count,
        flake_count,
        skip_count,
        flake_rate,
        budget_max_flake_rate: budget.max_flake_rate,
        escalation_warn_flake_rate: budget.warn_flake_rate,
        escalation_critical_flake_rate: budget.critical_flake_rate,
        escalation_level,
        within_budget,
        blocking: budget.blocking,
        pipeline_fail,
    }
}

/// Evaluate the global flake budget across all lanes.
#[must_use]
pub fn evaluate_global_flake_budget(
    lane_results: &[FlakeBudgetResult],
    policy: &FlakeBudgetPolicy,
) -> GlobalFlakeBudgetResult {
    let total_executed: usize = lane_results
        .iter()
        .map(|r| r.total_tests - r.skip_count)
        .sum();
    let total_flakes: usize = lane_results.iter().map(|r| r.flake_count).sum();
    let global_flake_rate = if total_executed > 0 {
        total_flakes as f64 / total_executed as f64
    } else {
        0.0
    };
    let within_budget = global_flake_rate <= policy.global_max_flake_rate;
    let any_lane_failed = lane_results.iter().any(|r| r.pipeline_fail);

    GlobalFlakeBudgetResult {
        total_lanes: lane_results.len(),
        total_executed,
        total_flakes,
        global_flake_rate,
        global_max_flake_rate: policy.global_max_flake_rate,
        within_budget,
        pipeline_pass: within_budget && !any_lane_failed,
        lane_results: lane_results.to_vec(),
    }
}

/// Global flake budget evaluation across all CI lanes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalFlakeBudgetResult {
    pub total_lanes: usize,
    pub total_executed: usize,
    pub total_flakes: usize,
    pub global_flake_rate: f64,
    pub global_max_flake_rate: f64,
    pub within_budget: bool,
    pub pipeline_pass: bool,
    pub lane_results: Vec<FlakeBudgetResult>,
}

impl GlobalFlakeBudgetResult {
    /// Render a human-readable summary.
    #[must_use]
    pub fn render_summary(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(
            out,
            "CI Flake Budget Report (bd-1dp9.7.3)\n\
             Global: {}/{} flakes ({:.1}%, budget: {:.1}%) — {}\n\
             Pipeline: {}",
            self.total_flakes,
            self.total_executed,
            self.global_flake_rate * 100.0,
            self.global_max_flake_rate * 100.0,
            if self.within_budget {
                "WITHIN BUDGET"
            } else {
                "OVER BUDGET"
            },
            if self.pipeline_pass { "PASS" } else { "FAIL" },
        );
        for lane in &self.lane_results {
            let status = if lane.pipeline_fail {
                "FAIL"
            } else if !lane.within_budget {
                "WARN"
            } else {
                "OK"
            };
            let _ = writeln!(
                out,
                "  [{status}] {}: {}/{} flakes ({:.1}%, budget: {:.1}%) | {} fail | {}",
                lane.lane,
                lane.flake_count,
                lane.total_tests - lane.skip_count,
                lane.flake_rate * 100.0,
                lane.budget_max_flake_rate * 100.0,
                lane.fail_count,
                if lane.blocking {
                    "blocking"
                } else {
                    "advisory"
                },
            );
            let _ = writeln!(
                out,
                "      escalation={} (warn: {:.1}%, critical: {:.1}%)",
                lane.escalation_level.as_str(),
                lane.escalation_warn_flake_rate * 100.0,
                lane.escalation_critical_flake_rate * 100.0,
            );
        }
        out
    }
}

// ---- Retry and Quarantine Policy (bd-mblr.3.3) ----

/// Failure class used by retry policy to avoid masking regressions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RetryFailureClass {
    /// Deterministic correctness failure; must not be treated as flake.
    CorrectnessRegression,
    /// Infrastructure/transient failure (timeouts, ephemeral network issues).
    InfrastructureTransient,
    /// Persistent infrastructure failure (infra outage, invalid environment).
    InfrastructurePersistent,
}

/// Retry policy contract for CI gates.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RetryPolicy {
    /// Retries allowed for transient infrastructure failures.
    pub max_transient_retries: u32,
    /// Retries allowed for persistent infrastructure failures.
    pub max_persistent_retries: u32,
    /// Whether correctness failures are ever retryable.
    pub allow_correctness_retry: bool,
    /// Schema version for serialized policy artifacts.
    pub schema_version: String,
}

impl RetryPolicy {
    /// Canonical retry policy for CI gates.
    #[must_use]
    pub fn canonical() -> Self {
        Self {
            max_transient_retries: 2,
            max_persistent_retries: 1,
            allow_correctness_retry: false,
            schema_version: "1.0.0".to_owned(),
        }
    }
}

/// Decision returned by retry-policy evaluation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RetryDecision {
    /// Whether another retry attempt is allowed.
    pub allow_retry: bool,
    /// Whether this failure path is eligible to be counted as a flake.
    pub classify_as_flake: bool,
    /// Whether the failure is terminal for pipeline purposes.
    pub hard_failure: bool,
    /// Human-readable reason.
    pub reason: String,
}

/// Evaluate retry behavior for a lane/failure class combination.
///
/// `attempt` is zero-based (0 = first failure observation before any retry).
#[must_use]
pub fn evaluate_retry_decision(
    lane: CiLane,
    failure_class: RetryFailureClass,
    attempt: u32,
    recovered_on_retry: bool,
    policy: &RetryPolicy,
) -> RetryDecision {
    if matches!(failure_class, RetryFailureClass::CorrectnessRegression) {
        return RetryDecision {
            allow_retry: policy.allow_correctness_retry && lane.supports_retry(),
            classify_as_flake: false,
            hard_failure: !policy.allow_correctness_retry,
            reason: "correctness regression is non-flaky and must not be masked".to_owned(),
        };
    }

    if recovered_on_retry && matches!(failure_class, RetryFailureClass::InfrastructureTransient) {
        return RetryDecision {
            allow_retry: false,
            classify_as_flake: true,
            hard_failure: false,
            reason: "transient infrastructure failure recovered on retry".to_owned(),
        };
    }

    if !lane.supports_retry() {
        return RetryDecision {
            allow_retry: false,
            classify_as_flake: false,
            hard_failure: true,
            reason: "lane does not allow retry; failure is terminal".to_owned(),
        };
    }

    match failure_class {
        RetryFailureClass::InfrastructureTransient => {
            let allow_retry = attempt < policy.max_transient_retries;
            RetryDecision {
                allow_retry,
                classify_as_flake: false,
                hard_failure: !allow_retry,
                reason: if allow_retry {
                    "transient infrastructure failure eligible for retry".to_owned()
                } else {
                    "transient retry budget exhausted".to_owned()
                },
            }
        }
        RetryFailureClass::InfrastructurePersistent => {
            let allow_retry = attempt < policy.max_persistent_retries;
            RetryDecision {
                allow_retry,
                classify_as_flake: false,
                hard_failure: !allow_retry,
                reason: if allow_retry {
                    "persistent infrastructure failure granted final retry".to_owned()
                } else {
                    "persistent infrastructure failure exhausted retry budget".to_owned()
                },
            }
        }
        RetryFailureClass::CorrectnessRegression => unreachable!(),
    }
}

/// Quarantine policy for temporarily waiving flake-budget failures.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct QuarantinePolicy {
    /// Whether quarantine is enabled.
    pub enabled: bool,
    /// Maximum run-count TTL for quarantine entries.
    pub max_expires_after_runs: u32,
    /// Require an explicit owner.
    pub require_owner: bool,
    /// Require a follow-up bead/issue linkage.
    pub require_follow_up_issue: bool,
    /// Schema version for serialized policy artifacts.
    pub schema_version: String,
}

impl QuarantinePolicy {
    /// Canonical quarantine policy for flake workflow control.
    #[must_use]
    pub fn canonical() -> Self {
        Self {
            enabled: true,
            max_expires_after_runs: 5,
            require_owner: true,
            require_follow_up_issue: true,
            schema_version: "1.0.0".to_owned(),
        }
    }
}

/// Quarantine request ticket attached to CI lane decisions.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct QuarantineTicket {
    pub lane: String,
    pub gate_id: String,
    pub owner: String,
    pub follow_up_issue: String,
    pub reason: String,
    /// TTL in CI runs before expiry.
    pub expires_after_runs: u32,
}

/// Quarantine evaluation decision.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct QuarantineDecision {
    pub approved: bool,
    pub effective_pipeline_fail: bool,
    pub reasons: Vec<String>,
}

/// Evaluate whether a quarantine ticket can waive a lane-level pipeline failure.
#[must_use]
pub fn evaluate_quarantine_ticket(
    lane_result: &FlakeBudgetResult,
    ticket: &QuarantineTicket,
    policy: &QuarantinePolicy,
) -> QuarantineDecision {
    let mut reasons = Vec::new();

    if !policy.enabled {
        reasons.push("quarantine policy disabled".to_owned());
    }
    if ticket.lane != lane_result.lane {
        reasons.push("ticket lane does not match lane result".to_owned());
    }
    if ticket.gate_id.trim().is_empty() {
        reasons.push("gate_id is required".to_owned());
    }
    if ticket.reason.trim().is_empty() {
        reasons.push("quarantine reason is required".to_owned());
    }
    if policy.require_owner && ticket.owner.trim().is_empty() {
        reasons.push("owner is required".to_owned());
    }
    if policy.require_follow_up_issue && ticket.follow_up_issue.trim().is_empty() {
        reasons.push("follow_up_issue is required".to_owned());
    }
    if ticket.expires_after_runs == 0 || ticket.expires_after_runs > policy.max_expires_after_runs {
        reasons.push(format!(
            "expires_after_runs must be between 1 and {}",
            policy.max_expires_after_runs
        ));
    }
    if lane_result.fail_count > 0 {
        reasons.push("quarantine cannot mask hard failures".to_owned());
    }
    if lane_result.within_budget {
        reasons.push("lane is already within budget; quarantine not applicable".to_owned());
    }
    if matches!(lane_result.escalation_level, FlakeEscalationLevel::Critical) {
        reasons
            .push("critical escalation requires immediate remediation, not quarantine".to_owned());
    }
    if !lane_result.blocking {
        reasons.push("advisory lanes do not require quarantine waivers".to_owned());
    }

    let approved = reasons.is_empty();
    QuarantineDecision {
        approved,
        effective_pipeline_fail: if approved {
            false
        } else {
            lane_result.pipeline_fail
        },
        reasons,
    }
}

// ---- Auto-Bisect Hooks ----

/// Trigger condition for auto-bisect.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BisectTrigger {
    /// A gate that previously passed now fails consistently (not a flake).
    GateRegression,
    /// Performance regression detected above the critical threshold.
    PerformanceRegression,
    /// Drift detector flagged a regime shift.
    DriftShiftDetected,
}

/// A request for automated bisection, produced by the auto-bisect hook.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BisectRequest {
    /// Unique identifier for this bisect request.
    pub request_id: String,
    /// What triggered the bisect.
    pub trigger: BisectTrigger,
    /// The CI lane where the regression was detected.
    pub lane: String,
    /// The failing gate or test identifier.
    pub failing_gate: String,
    /// Git SHA of the known-good commit (last passing).
    pub good_commit: String,
    /// Git SHA of the known-bad commit (first failing).
    pub bad_commit: String,
    /// Deterministic seed for replay.
    pub replay_seed: u64,
    /// Command to reproduce the failure.
    pub replay_command: String,
    /// Expected exit code for "pass" (typically 0).
    pub expected_exit_code: i32,
    /// ISO 8601 timestamp of when the bisect was requested.
    pub requested_at: String,
    /// Human-readable description of the regression.
    pub description: String,
}

/// Configuration for the auto-bisect hook.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AutoBisectConfig {
    /// Whether auto-bisect is enabled.
    pub enabled: bool,
    /// Maximum number of bisect steps before giving up.
    pub max_steps: u32,
    /// Timeout per bisect step in seconds.
    pub step_timeout_secs: u64,
    /// Maximum concurrently running bisect jobs across all lanes.
    pub max_concurrent_runs: u32,
    /// Maximum concurrently running bisect jobs for a single lane.
    pub max_concurrent_per_lane: u32,
    /// Maximum queued bisect jobs waiting for worker capacity.
    pub max_pending_runs: u32,
    /// Lanes eligible for auto-bisect.
    pub eligible_lanes: Vec<String>,
}

impl AutoBisectConfig {
    /// Default configuration: enabled, 20 steps max, 300s timeout.
    #[must_use]
    pub fn default_config() -> Self {
        Self {
            enabled: true,
            max_steps: 20,
            step_timeout_secs: 300,
            max_concurrent_runs: 4,
            max_concurrent_per_lane: 2,
            max_pending_runs: 16,
            eligible_lanes: CiLane::ALL
                .iter()
                .filter(|l| l.supports_retry())
                .map(|l| l.as_str().to_owned())
                .collect(),
        }
    }
}

/// Runtime queue/concurrency snapshot used when deciding whether a bisect can
/// be dispatched now or must be deferred.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct BisectDispatchContext {
    /// Number of active bisect runs across all lanes.
    pub active_runs: u32,
    /// Number of active bisect runs for the candidate lane.
    pub active_for_lane: u32,
    /// Number of queued bisect runs waiting for capacity.
    pub pending_runs: u32,
}

impl BisectDispatchContext {
    /// Empty context: no active or pending jobs.
    #[must_use]
    pub const fn idle() -> Self {
        Self {
            active_runs: 0,
            active_for_lane: 0,
            pending_runs: 0,
        }
    }
}

/// Outcome of evaluating dispatch policy for an auto-bisect trigger.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BisectDispatchStatus {
    /// Trigger accepted; request may be enqueued.
    Enqueued,
    /// Auto-bisect is disabled globally.
    SkippedDisabled,
    /// Lane is not eligible for auto-bisect.
    SkippedIneligibleLane,
    /// No hard failures detected, so no bisect is needed.
    SkippedNoRegression,
    /// Global active bisect cap reached.
    SkippedGlobalConcurrencyCap,
    /// Per-lane active bisect cap reached.
    SkippedLaneConcurrencyCap,
    /// Pending queue cap reached.
    SkippedPendingCap,
}

impl BisectDispatchStatus {
    /// Whether this status represents an accepted dispatch.
    #[must_use]
    pub const fn is_enqueued(self) -> bool {
        matches!(self, Self::Enqueued)
    }
}

/// Detailed dispatch policy decision for operator/CI diagnostics.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BisectDispatchDecision {
    /// Decision status.
    pub status: BisectDispatchStatus,
    /// Trigger chosen when status is `Enqueued`.
    pub trigger: Option<BisectTrigger>,
    /// Human-readable policy reason.
    pub reason: String,
    /// Queue position if enqueued.
    pub queue_position: Option<u32>,
}

/// Evaluate auto-bisect trigger and bounded-concurrency dispatch policy.
#[must_use]
pub fn evaluate_bisect_dispatch(
    lane_result: &FlakeBudgetResult,
    config: &AutoBisectConfig,
    context: BisectDispatchContext,
) -> BisectDispatchDecision {
    if !config.enabled {
        return BisectDispatchDecision {
            status: BisectDispatchStatus::SkippedDisabled,
            trigger: None,
            reason: "auto-bisect disabled".to_owned(),
            queue_position: None,
        };
    }
    if !config.eligible_lanes.contains(&lane_result.lane) {
        return BisectDispatchDecision {
            status: BisectDispatchStatus::SkippedIneligibleLane,
            trigger: None,
            reason: format!("lane '{}' is not auto-bisect eligible", lane_result.lane),
            queue_position: None,
        };
    }
    if lane_result.fail_count == 0 {
        return BisectDispatchDecision {
            status: BisectDispatchStatus::SkippedNoRegression,
            trigger: None,
            reason: "no hard failures observed".to_owned(),
            queue_position: None,
        };
    }
    if context.active_runs >= config.max_concurrent_runs {
        return BisectDispatchDecision {
            status: BisectDispatchStatus::SkippedGlobalConcurrencyCap,
            trigger: None,
            reason: format!(
                "global bisect concurrency cap reached: active={} cap={}",
                context.active_runs, config.max_concurrent_runs
            ),
            queue_position: None,
        };
    }
    if context.active_for_lane >= config.max_concurrent_per_lane {
        return BisectDispatchDecision {
            status: BisectDispatchStatus::SkippedLaneConcurrencyCap,
            trigger: None,
            reason: format!(
                "lane bisect concurrency cap reached: lane_active={} lane_cap={}",
                context.active_for_lane, config.max_concurrent_per_lane
            ),
            queue_position: None,
        };
    }
    if context.pending_runs >= config.max_pending_runs {
        return BisectDispatchDecision {
            status: BisectDispatchStatus::SkippedPendingCap,
            trigger: None,
            reason: format!(
                "pending bisect queue cap reached: pending={} cap={}",
                context.pending_runs, config.max_pending_runs
            ),
            queue_position: None,
        };
    }

    BisectDispatchDecision {
        status: BisectDispatchStatus::Enqueued,
        trigger: Some(BisectTrigger::GateRegression),
        reason: "regression accepted for auto-bisect dispatch".to_owned(),
        queue_position: Some(context.pending_runs.saturating_add(1)),
    }
}

/// Build a bisect request from a detected regression.
#[must_use]
#[allow(clippy::too_many_arguments)]
pub fn build_bisect_request(
    trigger: BisectTrigger,
    lane: CiLane,
    failing_gate: &str,
    good_commit: &str,
    bad_commit: &str,
    replay_seed: u64,
    replay_command: &str,
    description: &str,
) -> BisectRequest {
    let request_id = format!(
        "bisect-{}-{}-{replay_seed}",
        lane.as_str(),
        &bad_commit[..8.min(bad_commit.len())],
    );

    BisectRequest {
        request_id,
        trigger,
        lane: lane.as_str().to_owned(),
        failing_gate: failing_gate.to_owned(),
        good_commit: good_commit.to_owned(),
        bad_commit: bad_commit.to_owned(),
        replay_seed,
        replay_command: replay_command.to_owned(),
        expected_exit_code: 0,
        requested_at: "2026-02-13T09:00:00Z".to_owned(), // placeholder for deterministic tests
        description: description.to_owned(),
    }
}

/// Evaluate whether a bisect should be triggered given gate results and config.
#[must_use]
pub fn should_trigger_bisect(
    lane_result: &FlakeBudgetResult,
    config: &AutoBisectConfig,
) -> Option<BisectTrigger> {
    let decision = evaluate_bisect_dispatch(lane_result, config, BisectDispatchContext::idle());
    if decision.status.is_enqueued() {
        decision.trigger
    } else {
        None
    }
}

/// Operator-visible outcome from running an automated bisect.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BisectExecutionOutcome {
    /// Run completed with a likely culprit range.
    Success,
    /// Run ended with uncertain verdict (e.g., flakiness).
    Uncertain,
    /// Run cancelled by operator action.
    Cancelled,
    /// Run exceeded configured timeout budget.
    Timeout,
}

/// Structured telemetry for a bisect run summary.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BisectRunTelemetry {
    /// Correlation identifiers required by logging contract.
    pub trace_id: String,
    pub run_id: String,
    pub scenario_id: String,
    /// Queue and execution timing.
    pub queue_wait_ms: u64,
    pub execution_ms: u64,
    /// Number of bisect candidate steps attempted.
    pub step_count: u32,
}

/// Operator/CI summary for a completed/terminated bisect run.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BisectResultSummary {
    /// Schema version for summary payload.
    pub schema_version: String,
    /// Request correlation.
    pub request_id: String,
    pub lane: String,
    /// Terminal run outcome.
    pub outcome: BisectExecutionOutcome,
    /// Likely culprit commit range (inclusive lower, inclusive upper).
    pub likely_culprit_start: String,
    pub likely_culprit_end: String,
    /// Confidence in the inferred culprit range [0.0, 1.0].
    pub confidence: f64,
    /// Linked replay artifacts useful for debugging.
    pub replay_artifacts: Vec<String>,
    /// Actionable operator context lines.
    pub actionable_context: Vec<String>,
    /// Structured telemetry contract.
    pub telemetry: BisectRunTelemetry,
}

impl BisectResultSummary {
    /// Validate summary completeness and schema constraints.
    #[must_use]
    pub fn validate(&self) -> Vec<String> {
        let mut errors = Vec::new();
        if self.schema_version.is_empty() {
            errors.push("summary.schema_version is empty".to_owned());
        }
        if self.request_id.is_empty() {
            errors.push("summary.request_id is empty".to_owned());
        }
        if self.lane.is_empty() {
            errors.push("summary.lane is empty".to_owned());
        }
        if self.likely_culprit_start.is_empty() {
            errors.push("summary.likely_culprit_start is empty".to_owned());
        }
        if self.likely_culprit_end.is_empty() {
            errors.push("summary.likely_culprit_end is empty".to_owned());
        }
        if !(0.0..=1.0).contains(&self.confidence) {
            errors.push(format!(
                "summary.confidence must be within [0,1], got {}",
                self.confidence
            ));
        }
        if self.telemetry.trace_id.is_empty() {
            errors.push("summary.telemetry.trace_id is empty".to_owned());
        }
        if self.telemetry.run_id.is_empty() {
            errors.push("summary.telemetry.run_id is empty".to_owned());
        }
        if self.telemetry.scenario_id.is_empty() {
            errors.push("summary.telemetry.scenario_id is empty".to_owned());
        }
        if self.replay_artifacts.is_empty() {
            errors.push("summary.replay_artifacts is empty".to_owned());
        }
        errors
    }

    /// Render operator-facing one-page summary.
    #[must_use]
    pub fn render_summary(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(
            out,
            "Bisect Result: lane={} request={} outcome={:?}",
            self.lane, self.request_id, self.outcome
        );
        let _ = writeln!(
            out,
            "  Culprit range: {}..{} | confidence={:.2}",
            self.likely_culprit_start, self.likely_culprit_end, self.confidence
        );
        let _ = writeln!(
            out,
            "  trace_id={} run_id={} scenario_id={} queue_wait_ms={} execution_ms={} steps={}",
            self.telemetry.trace_id,
            self.telemetry.run_id,
            self.telemetry.scenario_id,
            self.telemetry.queue_wait_ms,
            self.telemetry.execution_ms,
            self.telemetry.step_count,
        );
        if !self.replay_artifacts.is_empty() {
            let _ = writeln!(
                out,
                "  Replay artifacts: {}",
                self.replay_artifacts.join(", ")
            );
        }
        for line in &self.actionable_context {
            let _ = writeln!(out, "  Context: {line}");
        }
        out
    }
}

/// Build a structured bisect result summary.
#[must_use]
#[allow(clippy::too_many_arguments)]
pub fn build_bisect_result_summary(
    request: &BisectRequest,
    outcome: BisectExecutionOutcome,
    likely_culprit_start: &str,
    likely_culprit_end: &str,
    confidence: f64,
    replay_artifacts: Vec<String>,
    telemetry: BisectRunTelemetry,
    actionable_context: Vec<String>,
) -> BisectResultSummary {
    BisectResultSummary {
        schema_version: "1.0.0".to_owned(),
        request_id: request.request_id.clone(),
        lane: request.lane.clone(),
        outcome,
        likely_culprit_start: likely_culprit_start.to_owned(),
        likely_culprit_end: likely_culprit_end.to_owned(),
        confidence,
        replay_artifacts,
        actionable_context,
        telemetry,
    }
}

// ---- Artifact Publication ----

/// A single artifact produced by a CI gate run.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArtifactEntry {
    /// Artifact kind (log, report, manifest, database, trace).
    pub kind: ArtifactKind,
    /// Relative path within the artifact output directory.
    pub path: String,
    /// SHA-256 content hash (64 hex chars).
    pub content_hash: String,
    /// Size in bytes.
    pub size_bytes: u64,
    /// Human-readable description.
    pub description: String,
}

/// Classification of artifact types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum ArtifactKind {
    /// Structured log file (JSONL events).
    Log,
    /// Gate/validation report (JSON).
    Report,
    /// Execution manifest (JSON).
    Manifest,
    /// Test database file.
    Database,
    /// Execution trace (for replay).
    Trace,
    /// Performance benchmark data.
    Benchmark,
}

/// Complete artifact manifest for a CI gate run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArtifactManifest {
    pub schema_version: String,
    pub bead_id: String,
    pub run_id: String,
    pub lane: String,
    pub git_sha: String,
    pub seed: u64,
    pub created_at: String,
    pub artifacts: Vec<ArtifactEntry>,
    /// Whether the gate passed.
    pub gate_passed: bool,
    /// Bisect request if regression detected.
    pub bisect_request: Option<BisectRequest>,
    /// Optional terminal bisect result summary for operator/CI publication.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bisect_result_summary: Option<BisectResultSummary>,
    /// Verification contract enforcement payload (bd-1dp9.7.7).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verification_contract: Option<ContractEnforcementOutcome>,
}

impl ArtifactManifest {
    /// Attach a bisect result summary to this artifact manifest.
    #[must_use]
    pub fn with_bisect_result_summary(mut self, summary: BisectResultSummary) -> Self {
        self.bisect_result_summary = Some(summary);
        self
    }

    /// Validate the manifest for completeness.
    #[must_use]
    pub fn validate(&self) -> Vec<String> {
        let mut errors = Vec::new();
        if self.run_id.is_empty() {
            errors.push("run_id must not be empty".to_owned());
        }
        if self.lane.is_empty() {
            errors.push("lane must not be empty".to_owned());
        }
        if self.git_sha.is_empty() {
            errors.push("git_sha must not be empty".to_owned());
        }
        for (i, artifact) in self.artifacts.iter().enumerate() {
            if artifact.path.is_empty() {
                errors.push(format!("artifact[{i}].path must not be empty"));
            }
            if artifact.content_hash.len() != 64 {
                errors.push(format!(
                    "artifact[{i}].content_hash must be 64 hex chars, got {}",
                    artifact.content_hash.len(),
                ));
            }
        }
        if let Some(contract) = &self.verification_contract {
            if contract.final_gate_passed != self.gate_passed {
                errors.push(format!(
                    "verification_contract.final_gate_passed={} must match gate_passed={}",
                    contract.final_gate_passed, self.gate_passed
                ));
            }
        }
        if let Some(summary) = &self.bisect_result_summary {
            for error in summary.validate() {
                errors.push(format!("bisect_result_summary.{error}"));
            }
        }
        errors
    }

    /// Render a human-readable summary of the manifest.
    #[must_use]
    pub fn render_summary(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(
            out,
            "Artifact Manifest: {} ({})\n\
             Run: {} | Seed: {} | Git: {}\n\
             Gate: {} | Artifacts: {}",
            self.lane,
            self.bead_id,
            self.run_id,
            self.seed,
            self.git_sha,
            if self.gate_passed { "PASS" } else { "FAIL" },
            self.artifacts.len(),
        );
        for artifact in &self.artifacts {
            let _ = writeln!(
                out,
                "  [{:?}] {} ({} bytes) — {}",
                artifact.kind, artifact.path, artifact.size_bytes, artifact.description,
            );
        }
        if let Some(ref bisect) = self.bisect_request {
            let _ = writeln!(
                out,
                "  Bisect requested: {} -> {} ({})",
                bisect.good_commit, bisect.bad_commit, bisect.description,
            );
        }
        if let Some(ref summary) = self.bisect_result_summary {
            let _ = writeln!(
                out,
                "  Bisect result: outcome={:?} range={}..{} confidence={:.2}",
                summary.outcome,
                summary.likely_culprit_start,
                summary.likely_culprit_end,
                summary.confidence,
            );
            let _ = writeln!(
                out,
                "    trace_id={} run_id={} scenario_id={} execution_ms={}",
                summary.telemetry.trace_id,
                summary.telemetry.run_id,
                summary.telemetry.scenario_id,
                summary.telemetry.execution_ms,
            );
            if !summary.replay_artifacts.is_empty() {
                let _ = writeln!(
                    out,
                    "    replay_artifacts={}",
                    summary.replay_artifacts.join(", "),
                );
            }
        }
        if let Some(ref contract) = self.verification_contract {
            let _ = writeln!(
                out,
                "  Verification contract: {} ({})",
                if contract.contract_passed {
                    "PASS"
                } else {
                    "FAIL"
                },
                contract.disposition,
            );
            let _ = writeln!(
                out,
                "    failing_beads={} missing_evidence_beads={} invalid_reference_beads={}",
                contract.failing_beads,
                contract.missing_evidence_beads,
                contract.invalid_reference_beads,
            );
        }
        out
    }
}

/// Build an artifact manifest for a completed gate run.
#[must_use]
pub fn build_artifact_manifest(
    lane: CiLane,
    run_id: &str,
    git_sha: &str,
    seed: u64,
    gate_passed: bool,
    artifacts: Vec<ArtifactEntry>,
    bisect_request: Option<BisectRequest>,
) -> ArtifactManifest {
    build_artifact_manifest_with_contract(
        lane,
        run_id,
        git_sha,
        seed,
        gate_passed,
        artifacts,
        bisect_request,
        None,
    )
}

/// Build an artifact manifest for a completed gate run with optional
/// verification-contract enforcement payload.
#[must_use]
#[allow(clippy::too_many_arguments)]
pub fn build_artifact_manifest_with_contract(
    lane: CiLane,
    run_id: &str,
    git_sha: &str,
    seed: u64,
    base_gate_passed: bool,
    artifacts: Vec<ArtifactEntry>,
    bisect_request: Option<BisectRequest>,
    verification_contract: Option<ContractEnforcementOutcome>,
) -> ArtifactManifest {
    let gate_passed = verification_contract
        .as_ref()
        .map_or(base_gate_passed, |contract| contract.final_gate_passed);

    ArtifactManifest {
        schema_version: "1.0.0".to_owned(),
        bead_id: BEAD_ID.to_owned(),
        run_id: run_id.to_owned(),
        lane: lane.as_str().to_owned(),
        git_sha: git_sha.to_owned(),
        seed,
        created_at: "2026-02-13T09:00:00Z".to_owned(),
        artifacts,
        gate_passed,
        bisect_request,
        bisect_result_summary: None,
        verification_contract,
    }
}

// ---- Tests ----

#[cfg(test)]
mod tests {
    use super::*;
    use crate::verification_contract_enforcement::{
        BeadContractVerdict, ContractBeadStatus, ContractEnforcementOutcome, EnforcementDisposition,
    };

    fn synthetic_contract_outcome(
        base_gate_passed: bool,
        contract_passed: bool,
    ) -> ContractEnforcementOutcome {
        let status = if contract_passed {
            ContractBeadStatus::Pass
        } else {
            ContractBeadStatus::FailMissingEvidence
        };
        let failing_beads = if contract_passed { 0 } else { 1 };
        let missing_evidence_beads = if contract_passed { 0 } else { 1 };
        let disposition = match (base_gate_passed, contract_passed) {
            (true, true) => EnforcementDisposition::Allowed,
            (false, true) => EnforcementDisposition::BlockedByBaseGate,
            (true, false) => EnforcementDisposition::BlockedByContract,
            (false, false) => EnforcementDisposition::BlockedByBoth,
        };

        ContractEnforcementOutcome {
            schema_version: 1,
            bead_id: "bd-1dp9.7.7".to_owned(),
            base_gate_passed,
            contract_passed,
            final_gate_passed: base_gate_passed && contract_passed,
            disposition,
            total_beads: 1,
            failing_beads,
            missing_evidence_beads,
            invalid_reference_beads: 0,
            bead_verdicts: vec![BeadContractVerdict {
                bead_id: "bd-1dp9.7.7".to_owned(),
                status,
                missing_evidence_count: missing_evidence_beads,
                invalid_reference_count: 0,
                details: Vec::new(),
            }],
        }
    }

    // ---- Flake Budget Tests ----

    #[test]
    fn canonical_policy_covers_all_lanes() {
        let policy = FlakeBudgetPolicy::canonical();
        for lane in CiLane::ALL {
            assert!(
                policy.lane_budgets.contains_key(lane.as_str()),
                "canonical policy missing lane '{}'",
                lane.as_str(),
            );
        }
    }

    #[test]
    fn flake_budget_all_pass() {
        let policy = FlakeBudgetPolicy::canonical();
        let outcomes = vec![TestOutcome::Pass; 100];
        let result = evaluate_flake_budget(CiLane::Unit, &outcomes, &policy);
        assert!(result.within_budget);
        assert!(!result.pipeline_fail);
        assert_eq!(result.flake_count, 0);
        assert_eq!(result.flake_rate, 0.0);
    }

    #[test]
    fn flake_budget_within_threshold() {
        let policy = FlakeBudgetPolicy::canonical();
        // 4 flakes out of 100 = 4% < 5% budget
        let mut outcomes = vec![TestOutcome::Pass; 96];
        outcomes.extend(vec![TestOutcome::Flake; 4]);
        let result = evaluate_flake_budget(CiLane::Unit, &outcomes, &policy);
        assert!(result.within_budget);
        assert!(!result.pipeline_fail);
        assert_eq!(result.flake_count, 4);
    }

    #[test]
    fn flake_budget_over_threshold_blocking() {
        let policy = FlakeBudgetPolicy::canonical();
        // 10 flakes out of 100 = 10% > 5% budget, blocking lane
        let mut outcomes = vec![TestOutcome::Pass; 90];
        outcomes.extend(vec![TestOutcome::Flake; 10]);
        let result = evaluate_flake_budget(CiLane::Unit, &outcomes, &policy);
        assert!(!result.within_budget);
        assert!(result.pipeline_fail, "blocking lane should fail pipeline");
    }

    #[test]
    fn flake_budget_over_threshold_advisory() {
        let policy = FlakeBudgetPolicy::canonical();
        // Performance lane is advisory — over budget but no pipeline fail
        let mut outcomes = vec![TestOutcome::Pass; 80];
        outcomes.extend(vec![TestOutcome::Flake; 20]);
        let result = evaluate_flake_budget(CiLane::Performance, &outcomes, &policy);
        assert!(!result.within_budget);
        assert!(
            !result.pipeline_fail,
            "advisory lane should not fail pipeline"
        );
    }

    #[test]
    fn flake_budget_escalation_thresholds() {
        let policy = FlakeBudgetPolicy::canonical();

        // 3% flake rate should trigger warning for strict lanes.
        let mut warn_outcomes = vec![TestOutcome::Pass; 97];
        warn_outcomes.extend(vec![TestOutcome::Flake; 3]);
        let warn_result = evaluate_flake_budget(CiLane::Unit, &warn_outcomes, &policy);
        assert_eq!(warn_result.escalation_level, FlakeEscalationLevel::Warn);

        // 9% flake rate should trigger critical escalation for strict lanes.
        let mut critical_outcomes = vec![TestOutcome::Pass; 91];
        critical_outcomes.extend(vec![TestOutcome::Flake; 9]);
        let critical_result = evaluate_flake_budget(CiLane::Unit, &critical_outcomes, &policy);
        assert_eq!(
            critical_result.escalation_level,
            FlakeEscalationLevel::Critical
        );
    }

    #[test]
    fn flake_budget_hard_failure_always_fails() {
        let policy = FlakeBudgetPolicy::canonical();
        // Even one hard failure should fail the pipeline
        let outcomes = vec![TestOutcome::Pass, TestOutcome::Fail, TestOutcome::Pass];
        let result = evaluate_flake_budget(CiLane::Unit, &outcomes, &policy);
        assert!(result.pipeline_fail, "hard failure must fail pipeline");
        assert_eq!(result.fail_count, 1);
    }

    #[test]
    fn flake_budget_skips_excluded_from_rate() {
        let policy = FlakeBudgetPolicy::canonical();
        // 50 pass + 50 skip + 3 flake = 3/50 = 6% > 5%
        let mut outcomes = vec![TestOutcome::Pass; 50];
        outcomes.extend(vec![TestOutcome::Skip; 50]);
        outcomes.extend(vec![TestOutcome::Flake; 3]);
        let result = evaluate_flake_budget(CiLane::Unit, &outcomes, &policy);
        // 3 flakes out of 53 executed (50 pass + 3 flake) = 5.66% > 5%
        assert!(!result.within_budget);
    }

    #[test]
    fn flake_budget_empty_stream() {
        let policy = FlakeBudgetPolicy::canonical();
        let result = evaluate_flake_budget(CiLane::Unit, &[], &policy);
        assert!(result.within_budget);
        assert!(!result.pipeline_fail);
        assert_eq!(result.flake_rate, 0.0);
    }

    #[test]
    fn global_flake_budget_aggregates_lanes() {
        let policy = FlakeBudgetPolicy::canonical();
        let lane_results = vec![
            evaluate_flake_budget(CiLane::Unit, &[TestOutcome::Pass; 100], &policy),
            evaluate_flake_budget(
                CiLane::E2eCorrectness,
                &{
                    let mut v = vec![TestOutcome::Pass; 98];
                    v.extend(vec![TestOutcome::Flake; 2]);
                    v
                },
                &policy,
            ),
        ];
        let global = evaluate_global_flake_budget(&lane_results, &policy);
        assert!(global.pipeline_pass);
        assert_eq!(global.total_flakes, 2);
        assert_eq!(global.total_executed, 200);
    }

    #[test]
    fn global_flake_budget_fails_if_lane_fails() {
        let policy = FlakeBudgetPolicy::canonical();
        let lane_results = vec![
            evaluate_flake_budget(CiLane::Unit, &[TestOutcome::Fail], &policy),
            evaluate_flake_budget(CiLane::E2eCorrectness, &[TestOutcome::Pass; 100], &policy),
        ];
        let global = evaluate_global_flake_budget(&lane_results, &policy);
        assert!(!global.pipeline_pass, "should fail if any lane fails");
    }

    #[test]
    fn global_flake_budget_render_summary() {
        let policy = FlakeBudgetPolicy::canonical();
        let lane_results = vec![evaluate_flake_budget(
            CiLane::Unit,
            &[TestOutcome::Pass; 50],
            &policy,
        )];
        let global = evaluate_global_flake_budget(&lane_results, &policy);
        let summary = global.render_summary();
        assert!(summary.contains("CI Flake Budget Report"));
        assert!(summary.contains("PASS"));
    }

    // ---- Retry and Quarantine Tests (bd-mblr.3.3) ----

    #[test]
    fn retry_policy_correctness_regression_is_not_flake() {
        let policy = RetryPolicy::canonical();
        let decision = evaluate_retry_decision(
            CiLane::Unit,
            RetryFailureClass::CorrectnessRegression,
            0,
            false,
            &policy,
        );
        assert!(!decision.allow_retry);
        assert!(!decision.classify_as_flake);
        assert!(decision.hard_failure);
    }

    #[test]
    fn retry_policy_transient_recovery_classifies_flake() {
        let policy = RetryPolicy::canonical();
        let decision = evaluate_retry_decision(
            CiLane::E2eCorrectness,
            RetryFailureClass::InfrastructureTransient,
            1,
            true,
            &policy,
        );
        assert!(!decision.allow_retry);
        assert!(decision.classify_as_flake);
        assert!(!decision.hard_failure);
    }

    #[test]
    fn quarantine_requires_owner_follow_up_and_expiry() {
        let policy = FlakeBudgetPolicy::canonical();
        let mut outcomes = vec![TestOutcome::Pass; 88];
        outcomes.extend(vec![TestOutcome::Flake; 12]);
        let lane_result = evaluate_flake_budget(CiLane::Unit, &outcomes, &policy);
        assert!(lane_result.pipeline_fail);

        let quarantine_policy = QuarantinePolicy::canonical();
        let ticket = QuarantineTicket {
            lane: "unit".to_owned(),
            gate_id: "unit-gate".to_owned(),
            owner: String::new(),
            follow_up_issue: String::new(),
            reason: "temporary infra incident".to_owned(),
            expires_after_runs: 0,
        };
        let decision = evaluate_quarantine_ticket(&lane_result, &ticket, &quarantine_policy);
        assert!(!decision.approved);
        assert!(decision.effective_pipeline_fail);
        assert!(
            decision
                .reasons
                .iter()
                .any(|reason| reason.contains("owner is required"))
        );
        assert!(
            decision
                .reasons
                .iter()
                .any(|reason| reason.contains("follow_up_issue is required"))
        );
    }

    #[test]
    fn quarantine_cannot_mask_hard_failures() {
        let policy = FlakeBudgetPolicy::canonical();
        let outcomes = vec![TestOutcome::Pass, TestOutcome::Fail];
        let lane_result = evaluate_flake_budget(CiLane::Unit, &outcomes, &policy);
        assert!(lane_result.pipeline_fail);

        let ticket = QuarantineTicket {
            lane: "unit".to_owned(),
            gate_id: "unit-gate".to_owned(),
            owner: "MaroonCanyon".to_owned(),
            follow_up_issue: "bd-mblr.3.3".to_owned(),
            reason: "trying to waive correctness regression".to_owned(),
            expires_after_runs: 2,
        };
        let decision =
            evaluate_quarantine_ticket(&lane_result, &ticket, &QuarantinePolicy::canonical());
        assert!(!decision.approved);
        assert!(decision.effective_pipeline_fail);
        assert!(
            decision
                .reasons
                .iter()
                .any(|reason| reason.contains("cannot mask hard failures"))
        );
    }

    #[test]
    fn quarantine_allows_flake_only_lane_with_valid_ticket() {
        let policy = FlakeBudgetPolicy::canonical();
        let mut outcomes = vec![TestOutcome::Pass; 94];
        outcomes.extend(vec![TestOutcome::Flake; 6]);
        let lane_result = evaluate_flake_budget(CiLane::Unit, &outcomes, &policy);
        assert!(lane_result.pipeline_fail);
        assert_eq!(lane_result.escalation_level, FlakeEscalationLevel::Warn);

        let ticket = QuarantineTicket {
            lane: "unit".to_owned(),
            gate_id: "unit-gate".to_owned(),
            owner: "MaroonCanyon".to_owned(),
            follow_up_issue: "bd-mblr.3.3".to_owned(),
            reason: "known transient runner instability".to_owned(),
            expires_after_runs: 2,
        };
        let decision =
            evaluate_quarantine_ticket(&lane_result, &ticket, &QuarantinePolicy::canonical());
        assert!(decision.approved);
        assert!(!decision.effective_pipeline_fail);
        assert!(decision.reasons.is_empty());
    }

    // ---- Auto-Bisect Tests ----

    #[test]
    fn bisect_trigger_on_gate_failure() {
        let config = AutoBisectConfig::default_config();
        let lane_result = FlakeBudgetResult {
            lane: "unit".to_owned(),
            total_tests: 100,
            pass_count: 99,
            fail_count: 1,
            flake_count: 0,
            skip_count: 0,
            flake_rate: 0.0,
            budget_max_flake_rate: 0.05,
            escalation_warn_flake_rate: 0.03,
            escalation_critical_flake_rate: 0.08,
            escalation_level: FlakeEscalationLevel::None,
            within_budget: true,
            blocking: true,
            pipeline_fail: true,
        };
        let trigger = should_trigger_bisect(&lane_result, &config);
        assert_eq!(trigger, Some(BisectTrigger::GateRegression));
    }

    #[test]
    fn bisect_dispatch_enqueued_when_capacity_available() {
        let config = AutoBisectConfig::default_config();
        let lane_result = FlakeBudgetResult {
            lane: "unit".to_owned(),
            total_tests: 10,
            pass_count: 9,
            fail_count: 1,
            flake_count: 0,
            skip_count: 0,
            flake_rate: 0.0,
            budget_max_flake_rate: 0.05,
            escalation_warn_flake_rate: 0.03,
            escalation_critical_flake_rate: 0.08,
            escalation_level: FlakeEscalationLevel::None,
            within_budget: true,
            blocking: true,
            pipeline_fail: true,
        };
        let decision = evaluate_bisect_dispatch(
            &lane_result,
            &config,
            BisectDispatchContext {
                active_runs: 1,
                active_for_lane: 0,
                pending_runs: 2,
            },
        );
        assert_eq!(decision.status, BisectDispatchStatus::Enqueued);
        assert_eq!(decision.trigger, Some(BisectTrigger::GateRegression));
        assert_eq!(decision.queue_position, Some(3));
    }

    #[test]
    fn bisect_dispatch_skips_on_global_capacity_cap() {
        let mut config = AutoBisectConfig::default_config();
        config.max_concurrent_runs = 1;
        let lane_result = FlakeBudgetResult {
            lane: "unit".to_owned(),
            total_tests: 10,
            pass_count: 9,
            fail_count: 1,
            flake_count: 0,
            skip_count: 0,
            flake_rate: 0.0,
            budget_max_flake_rate: 0.05,
            escalation_warn_flake_rate: 0.03,
            escalation_critical_flake_rate: 0.08,
            escalation_level: FlakeEscalationLevel::None,
            within_budget: true,
            blocking: true,
            pipeline_fail: true,
        };
        let decision = evaluate_bisect_dispatch(
            &lane_result,
            &config,
            BisectDispatchContext {
                active_runs: 1,
                active_for_lane: 0,
                pending_runs: 0,
            },
        );
        assert_eq!(
            decision.status,
            BisectDispatchStatus::SkippedGlobalConcurrencyCap
        );
        assert!(decision.trigger.is_none());
    }

    #[test]
    fn no_bisect_when_disabled() {
        let mut config = AutoBisectConfig::default_config();
        config.enabled = false;
        let lane_result = FlakeBudgetResult {
            lane: "unit".to_owned(),
            total_tests: 100,
            pass_count: 99,
            fail_count: 1,
            flake_count: 0,
            skip_count: 0,
            flake_rate: 0.0,
            budget_max_flake_rate: 0.05,
            escalation_warn_flake_rate: 0.03,
            escalation_critical_flake_rate: 0.08,
            escalation_level: FlakeEscalationLevel::None,
            within_budget: true,
            blocking: true,
            pipeline_fail: true,
        };
        assert_eq!(should_trigger_bisect(&lane_result, &config), None);
    }

    #[test]
    fn no_bisect_for_ineligible_lane() {
        let config = AutoBisectConfig::default_config();
        let lane_result = FlakeBudgetResult {
            lane: "custom-lane".to_owned(),
            total_tests: 10,
            pass_count: 9,
            fail_count: 1,
            flake_count: 0,
            skip_count: 0,
            flake_rate: 0.0,
            budget_max_flake_rate: 0.05,
            escalation_warn_flake_rate: 0.03,
            escalation_critical_flake_rate: 0.08,
            escalation_level: FlakeEscalationLevel::None,
            within_budget: true,
            blocking: true,
            pipeline_fail: true,
        };
        assert_eq!(should_trigger_bisect(&lane_result, &config), None);
    }

    #[test]
    fn no_bisect_when_all_pass() {
        let config = AutoBisectConfig::default_config();
        let lane_result = FlakeBudgetResult {
            lane: "unit".to_owned(),
            total_tests: 100,
            pass_count: 100,
            fail_count: 0,
            flake_count: 0,
            skip_count: 0,
            flake_rate: 0.0,
            budget_max_flake_rate: 0.05,
            escalation_warn_flake_rate: 0.03,
            escalation_critical_flake_rate: 0.08,
            escalation_level: FlakeEscalationLevel::None,
            within_budget: true,
            blocking: true,
            pipeline_fail: false,
        };
        assert_eq!(should_trigger_bisect(&lane_result, &config), None);
    }

    #[test]
    fn bisect_request_construction() {
        let request = build_bisect_request(
            BisectTrigger::GateRegression,
            CiLane::Unit,
            "test_btree_split_merge",
            "abc12340000",
            "def56780000",
            42,
            "cargo test -p fsqlite-btree -- test_btree_split_merge",
            "B-tree split/merge regression after refactor",
        );
        assert!(request.request_id.contains("unit"));
        assert_eq!(request.trigger, BisectTrigger::GateRegression);
        assert_eq!(request.good_commit, "abc12340000");
        assert_eq!(request.bad_commit, "def56780000");
        assert_eq!(request.replay_seed, 42);
    }

    #[test]
    fn bisect_request_json_roundtrip() {
        let request = build_bisect_request(
            BisectTrigger::PerformanceRegression,
            CiLane::Performance,
            "perf_write_contention",
            "aaa",
            "bbb",
            999,
            "cargo bench -- perf_write_contention",
            "Write contention benchmark regressed 30%",
        );
        let json = serde_json::to_string_pretty(&request).unwrap();
        let deserialized: BisectRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, request);
    }

    #[test]
    fn bisect_result_summary_validate_and_render() {
        let request = build_bisect_request(
            BisectTrigger::GateRegression,
            CiLane::E2eCorrectness,
            "scenario_17",
            "good-sha",
            "bad-sha",
            77,
            "cargo test -p fsqlite-harness -- scenario_17",
            "synthetic regression",
        );
        let summary = build_bisect_result_summary(
            &request,
            BisectExecutionOutcome::Success,
            "good-sha",
            "bad-sha",
            0.92,
            vec![
                "artifacts/bisect/report.json".to_owned(),
                "artifacts/bisect/replay.jsonl".to_owned(),
            ],
            BisectRunTelemetry {
                trace_id: "trace-123".to_owned(),
                run_id: "run-123".to_owned(),
                scenario_id: "scenario_17".to_owned(),
                queue_wait_ms: 15,
                execution_ms: 1820,
                step_count: 4,
            },
            vec!["first failing midpoint was bad-sha".to_owned()],
        );
        let errors = summary.validate();
        assert!(errors.is_empty(), "summary should validate: {errors:?}");
        let rendered = summary.render_summary();
        assert!(rendered.contains("confidence=0.92"));
        assert!(rendered.contains("trace_id=trace-123"));
        assert!(rendered.contains("replay.jsonl"));
    }

    // ---- Artifact Manifest Tests ----

    #[test]
    fn artifact_manifest_validation_pass() {
        let manifest = build_artifact_manifest(
            CiLane::Unit,
            "bd-1dp9.7.3-20260213T090000Z-42",
            "abc1234",
            42,
            true,
            vec![ArtifactEntry {
                kind: ArtifactKind::Report,
                path: "reports/unit-gate.json".to_owned(),
                content_hash: "a".repeat(64),
                size_bytes: 1024,
                description: "Unit gate report".to_owned(),
            }],
            None,
        );
        let errors = manifest.validate();
        assert!(errors.is_empty(), "valid manifest: {errors:?}");
    }

    #[test]
    fn artifact_manifest_validation_catches_empty_fields() {
        let manifest = ArtifactManifest {
            schema_version: "1.0.0".to_owned(),
            bead_id: BEAD_ID.to_owned(),
            run_id: String::new(),  // invalid
            lane: String::new(),    // invalid
            git_sha: String::new(), // invalid
            seed: 0,
            created_at: "2026-02-13T09:00:00Z".to_owned(),
            artifacts: vec![ArtifactEntry {
                kind: ArtifactKind::Log,
                path: String::new(),              // invalid
                content_hash: "short".to_owned(), // invalid
                size_bytes: 0,
                description: "test".to_owned(),
            }],
            gate_passed: false,
            bisect_request: None,
            bisect_result_summary: None,
            verification_contract: None,
        };
        let errors = manifest.validate();
        assert!(
            errors.len() >= 4,
            "should catch multiple errors: {errors:?}"
        );
    }

    #[test]
    fn artifact_manifest_with_contract_blocks_gate_when_contract_fails() {
        let manifest = build_artifact_manifest_with_contract(
            CiLane::Unit,
            "run-contract-block",
            "abc123",
            42,
            true,
            Vec::new(),
            None,
            Some(synthetic_contract_outcome(true, false)),
        );

        assert!(
            !manifest.gate_passed,
            "contract enforcement should block final gate pass"
        );
        assert!(manifest.verification_contract.is_some());
        let summary = manifest.render_summary();
        assert!(summary.contains("Verification contract: FAIL"));
        assert!(summary.contains("blocked_by_contract"));
    }

    #[test]
    fn artifact_manifest_validate_flags_contract_mismatch() {
        let mut manifest = build_artifact_manifest_with_contract(
            CiLane::Unit,
            "run-contract-mismatch",
            "abc123",
            42,
            true,
            Vec::new(),
            None,
            Some(synthetic_contract_outcome(true, false)),
        );

        manifest.gate_passed = true;
        let errors = manifest.validate();
        assert!(
            errors
                .iter()
                .any(|error| { error.contains("verification_contract.final_gate_passed") })
        );
    }

    #[test]
    fn artifact_manifest_with_bisect_request() {
        let bisect = build_bisect_request(
            BisectTrigger::GateRegression,
            CiLane::E2eDifferential,
            "correctness_mvcc_isolation",
            "good123",
            "bad456",
            42,
            "cargo test -p fsqlite-e2e -- correctness_mvcc_isolation",
            "MVCC isolation regression",
        );
        let manifest = build_artifact_manifest(
            CiLane::E2eDifferential,
            "run-1",
            "bad456",
            42,
            false,
            Vec::new(),
            Some(bisect),
        );
        assert!(!manifest.gate_passed);
        assert!(manifest.bisect_request.is_some());
        let summary = manifest.render_summary();
        assert!(summary.contains("Bisect requested"));
    }

    #[test]
    fn artifact_manifest_json_roundtrip() {
        let manifest = build_artifact_manifest(
            CiLane::E2eCorrectness,
            "run-42",
            "sha256",
            42,
            true,
            vec![
                ArtifactEntry {
                    kind: ArtifactKind::Log,
                    path: "logs/events.jsonl".to_owned(),
                    content_hash: "b".repeat(64),
                    size_bytes: 2048,
                    description: "Event log".to_owned(),
                },
                ArtifactEntry {
                    kind: ArtifactKind::Report,
                    path: "reports/gate.json".to_owned(),
                    content_hash: "c".repeat(64),
                    size_bytes: 512,
                    description: "Gate report".to_owned(),
                },
            ],
            None,
        );
        let json = serde_json::to_string_pretty(&manifest).unwrap();
        let deserialized: ArtifactManifest = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.artifacts.len(), 2);
        assert_eq!(deserialized.gate_passed, true);
    }

    #[test]
    fn artifact_manifest_render_summary() {
        let manifest = build_artifact_manifest(
            CiLane::Unit,
            "test-run",
            "abc",
            42,
            true,
            vec![ArtifactEntry {
                kind: ArtifactKind::Report,
                path: "report.json".to_owned(),
                content_hash: "d".repeat(64),
                size_bytes: 100,
                description: "Test report".to_owned(),
            }],
            None,
        );
        let summary = manifest.render_summary();
        assert!(summary.contains("Artifact Manifest"));
        assert!(summary.contains("PASS"));
        assert!(summary.contains("report.json"));
    }

    // ---- CI Lane Tests ----

    #[test]
    fn all_lanes_have_unique_names() {
        let mut seen = std::collections::BTreeSet::new();
        for lane in CiLane::ALL {
            assert!(
                seen.insert(lane.as_str()),
                "duplicate lane name: {}",
                lane.as_str(),
            );
        }
    }

    #[test]
    fn retry_support_matches_lane_type() {
        // Functional test lanes support retry
        assert!(CiLane::Unit.supports_retry());
        assert!(CiLane::E2eDifferential.supports_retry());
        assert!(CiLane::E2eCorrectness.supports_retry());
        assert!(CiLane::E2eRecovery.supports_retry());
        // Non-functional lanes do not
        assert!(!CiLane::Performance.supports_retry());
        assert!(!CiLane::SchemaValidation.supports_retry());
        assert!(!CiLane::CoverageDrift.supports_retry());
    }

    // ---- Integration: Flake + Bisect + Artifact Pipeline ----

    #[test]
    fn full_pipeline_pass_no_bisect() {
        let policy = FlakeBudgetPolicy::canonical();
        let config = AutoBisectConfig::default_config();

        let outcomes = vec![TestOutcome::Pass; 100];
        let result = evaluate_flake_budget(CiLane::Unit, &outcomes, &policy);

        // No bisect needed
        assert_eq!(should_trigger_bisect(&result, &config), None);

        // Build artifact manifest
        let manifest = build_artifact_manifest(
            CiLane::Unit,
            "run-pass",
            "goodsha",
            42,
            !result.pipeline_fail,
            vec![ArtifactEntry {
                kind: ArtifactKind::Report,
                path: "reports/unit.json".to_owned(),
                content_hash: "e".repeat(64),
                size_bytes: 256,
                description: "Unit gate report".to_owned(),
            }],
            None,
        );
        assert!(manifest.gate_passed);
        assert!(manifest.bisect_request.is_none());
    }

    #[test]
    fn full_pipeline_fail_triggers_bisect() {
        let policy = FlakeBudgetPolicy::canonical();
        let config = AutoBisectConfig::default_config();

        let outcomes = vec![TestOutcome::Pass, TestOutcome::Fail, TestOutcome::Pass];
        let result = evaluate_flake_budget(CiLane::E2eCorrectness, &outcomes, &policy);

        // Bisect should trigger
        let trigger = should_trigger_bisect(&result, &config);
        assert_eq!(trigger, Some(BisectTrigger::GateRegression));

        // Build bisect request
        let bisect = build_bisect_request(
            trigger.unwrap(),
            CiLane::E2eCorrectness,
            "correctness_test_42",
            "last-good-sha",
            "current-bad-sha",
            42,
            "cargo test -p fsqlite-e2e -- correctness_test_42",
            "Correctness test regression",
        );

        // Build artifact manifest with bisect
        let manifest = build_artifact_manifest(
            CiLane::E2eCorrectness,
            "run-fail",
            "current-bad-sha",
            42,
            false,
            Vec::new(),
            Some(bisect),
        );
        assert!(!manifest.gate_passed);
        assert!(manifest.bisect_request.is_some());
    }

    #[test]
    fn full_pipeline_determinism() {
        let policy = FlakeBudgetPolicy::canonical();
        let outcomes = vec![TestOutcome::Pass; 50];

        let r1 = evaluate_flake_budget(CiLane::Unit, &outcomes, &policy);
        let r2 = evaluate_flake_budget(CiLane::Unit, &outcomes, &policy);

        let json1 = serde_json::to_string(&r1).unwrap();
        let json2 = serde_json::to_string(&r2).unwrap();
        assert_eq!(json1, json2, "pipeline must be deterministic");
    }
}
