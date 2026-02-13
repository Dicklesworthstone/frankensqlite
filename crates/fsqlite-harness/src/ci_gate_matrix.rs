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

/// Configuration for flake budget enforcement on a single CI lane.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LaneFlakeBudget {
    /// Maximum allowed flake rate (0.0 = no flakes allowed, 1.0 = all flakes allowed).
    pub max_flake_rate: f64,
    /// Maximum number of retries for flake detection.
    pub max_retries: u32,
    /// Whether flakes in this lane are blocking (fail the pipeline) or advisory.
    pub blocking: bool,
}

impl LaneFlakeBudget {
    /// Default budget: 5% flake rate, 2 retries, blocking.
    #[must_use]
    pub fn default_strict() -> Self {
        Self {
            max_flake_rate: 0.05,
            max_retries: 2,
            blocking: true,
        }
    }

    /// Relaxed budget for performance lanes (higher variance expected).
    #[must_use]
    pub fn default_relaxed() -> Self {
        Self {
            max_flake_rate: 0.10,
            max_retries: 3,
            blocking: false,
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
    let flake_count = outcomes.iter().filter(|o| **o == TestOutcome::Flake).count();
    let skip_count = outcomes.iter().filter(|o| **o == TestOutcome::Skip).count();

    let executed = total - skip_count;
    let flake_rate = if executed > 0 {
        flake_count as f64 / executed as f64
    } else {
        0.0
    };

    let within_budget = flake_rate <= budget.max_flake_rate;
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
                if lane.blocking { "blocking" } else { "advisory" },
            );
        }
        out
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
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AutoBisectConfig {
    /// Whether auto-bisect is enabled.
    pub enabled: bool,
    /// Maximum number of bisect steps before giving up.
    pub max_steps: u32,
    /// Timeout per bisect step in seconds.
    pub step_timeout_secs: u64,
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
            eligible_lanes: CiLane::ALL
                .iter()
                .filter(|l| l.supports_retry())
                .map(|l| l.as_str().to_owned())
                .collect(),
        }
    }
}

/// Build a bisect request from a detected regression.
#[must_use]
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
    if !config.enabled {
        return None;
    }
    if !config.eligible_lanes.contains(&lane_result.lane) {
        return None;
    }
    // Trigger on consistent failures (not flakes)
    if lane_result.fail_count > 0 {
        return Some(BisectTrigger::GateRegression);
    }
    None
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
}

impl ArtifactManifest {
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
    }
}

// ---- Tests ----

#[cfg(test)]
mod tests {
    use super::*;

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
        assert!(!result.pipeline_fail, "advisory lane should not fail pipeline");
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
            evaluate_flake_budget(
                CiLane::Unit,
                &[TestOutcome::Pass; 100],
                &policy,
            ),
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
            evaluate_flake_budget(
                CiLane::Unit,
                &[TestOutcome::Fail],
                &policy,
            ),
            evaluate_flake_budget(
                CiLane::E2eCorrectness,
                &[TestOutcome::Pass; 100],
                &policy,
            ),
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
            within_budget: true,
            blocking: true,
            pipeline_fail: true,
        };
        let trigger = should_trigger_bisect(&lane_result, &config);
        assert_eq!(trigger, Some(BisectTrigger::GateRegression));
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
            run_id: String::new(), // invalid
            lane: String::new(),   // invalid
            git_sha: String::new(), // invalid
            seed: 0,
            created_at: "2026-02-13T09:00:00Z".to_owned(),
            artifacts: vec![ArtifactEntry {
                kind: ArtifactKind::Log,
                path: String::new(), // invalid
                content_hash: "short".to_owned(), // invalid
                size_bytes: 0,
                description: "test".to_owned(),
            }],
            gate_passed: false,
            bisect_request: None,
        };
        let errors = manifest.validate();
        assert!(errors.len() >= 4, "should catch multiple errors: {errors:?}");
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
