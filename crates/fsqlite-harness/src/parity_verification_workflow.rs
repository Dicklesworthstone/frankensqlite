//! One-command parity verification workflow and artifact navigator (bd-2yqp6.7.8).
//!
//! This module is the user-facing orchestration layer over the narrower Track G
//! gates. The gates still own their domain-specific artifacts; this layer proves
//! they ran in the expected order and publishes a deterministic index that tells
//! a user what passed, what failed first, and which artifacts/replay commands to
//! inspect.

use serde::{Deserialize, Serialize};

/// Owning bead identifier.
pub const BEAD_ID: &str = "bd-2yqp6.7.8";
/// Workflow report schema version.
pub const SCHEMA_VERSION: &str = "fsqlite.parity_verification_workflow.v1";
/// Default freshness budget: 24 hours.
pub const DEFAULT_FRESHNESS_BUDGET_MS: u128 = 24 * 60 * 60 * 1_000;

const REQUIRED_ARTIFACT_ROLES: [&str; 9] = [
    "oracle_preflight_json",
    "differential_manifest_json",
    "parity_evidence_matrix_json",
    "parity_status_json",
    "parity_status_markdown",
    "fallback_boundary_inventory_toml",
    "fallback_decision_schema_json",
    "fallback_denial_replay_json",
    "g9_gate_summary_json",
];

/// Ordered workflow phases for the user command.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowPhase {
    /// Validate C SQLite oracle, fixtures, and subject/reference identity.
    PreflightDoctor,
    /// Run the required differential/conformance lane.
    DifferentialCi,
    /// Enforce Track G quality-contract evidence classes.
    ParityEvidenceMatrix,
    /// Publish the parity status report and evidence freshness dashboard.
    ParityStatusReport,
    /// Validate final certificate readiness from the prior artifacts.
    CertificateReadiness,
}

impl WorkflowPhase {
    /// Deterministic phase order for certification.
    #[must_use]
    pub const fn required_order() -> [Self; 5] {
        [
            Self::PreflightDoctor,
            Self::DifferentialCi,
            Self::ParityEvidenceMatrix,
            Self::ParityStatusReport,
            Self::CertificateReadiness,
        ]
    }

    /// Stable phase identifier.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PreflightDoctor => "preflight_doctor",
            Self::DifferentialCi => "differential_ci",
            Self::ParityEvidenceMatrix => "parity_evidence_matrix",
            Self::ParityStatusReport => "parity_status_report",
            Self::CertificateReadiness => "certificate_readiness",
        }
    }

    /// Zero-based phase order index.
    #[must_use]
    pub const fn order_index(self) -> usize {
        match self {
            Self::PreflightDoctor => 0,
            Self::DifferentialCi => 1,
            Self::ParityEvidenceMatrix => 2,
            Self::ParityStatusReport => 3,
            Self::CertificateReadiness => 4,
        }
    }
}

/// Terminal outcome for a workflow phase.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowOutcome {
    /// The phase completed successfully.
    Pass,
    /// The phase completed and failed its gate.
    Fail,
    /// The phase was intentionally skipped; never certifying.
    Skipped,
}

impl WorkflowOutcome {
    /// Stable outcome identifier.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pass => "pass",
            Self::Fail => "fail",
            Self::Skipped => "skipped",
        }
    }
}

/// Human/actionable diagnostic for the first failed phase.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkflowFirstFailure {
    /// Phase that failed first.
    pub phase: WorkflowPhase,
    /// Short human-readable summary.
    pub summary: String,
    /// JSON pointer or artifact-local pointer for the failure detail.
    pub diagnostic_json_pointer: String,
    /// Artifact ids that contain the evidence.
    pub artifact_ids: Vec<String>,
    /// Deterministic commands to rerun or remediate the failure.
    pub remediation_commands: Vec<String>,
}

/// Input step captured by the one-command wrapper.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkflowStepInput {
    /// Phase identifier.
    pub phase: WorkflowPhase,
    /// Command executed for this phase.
    pub command: String,
    /// Terminal outcome.
    pub outcome: WorkflowOutcome,
    /// Process exit code observed by the wrapper.
    pub exit_code: i32,
    /// Start timestamp in Unix milliseconds.
    pub started_unix_ms: u128,
    /// Finish timestamp in Unix milliseconds.
    pub finished_unix_ms: u128,
    /// Artifact ids produced or consumed by this phase.
    pub artifact_ids: Vec<String>,
    /// First-failure diagnostic when this phase failed.
    pub first_failure: Option<WorkflowFirstFailure>,
}

/// Artifact input captured by the one-command wrapper.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkflowArtifactInput {
    /// Stable id used by step references.
    pub artifact_id: String,
    /// User-facing artifact role.
    pub role: String,
    /// Path or URI for the artifact.
    pub path: String,
    /// SHA-256 payload hash.
    pub sha256: String,
    /// Command that regenerates this artifact.
    pub replay_command: String,
    /// Observation timestamp in Unix milliseconds.
    pub observed_unix_ms: Option<u128>,
    /// Whether the artifact is required for a certifying workflow.
    pub required: bool,
}

/// Raw workflow input consumed by the runner.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkflowInput {
    /// Run identifier.
    pub run_id: String,
    /// Trace identifier.
    pub trace_id: String,
    /// Scenario identifier.
    pub scenario_id: String,
    /// Deterministic seed.
    pub seed: u64,
    /// Report generation timestamp in Unix milliseconds.
    pub generated_unix_ms: u128,
    /// Freshness budget in milliseconds.
    pub freshness_budget_ms: u128,
    /// Ordered workflow step observations.
    pub steps: Vec<WorkflowStepInput>,
    /// Artifact observations for the navigator.
    pub artifacts: Vec<WorkflowArtifactInput>,
}

/// Published step state.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkflowStepReport {
    /// Phase identifier.
    pub phase: String,
    /// Expected phase order.
    pub order_index: usize,
    /// Command executed.
    pub command: String,
    /// Terminal outcome.
    pub outcome: String,
    /// Process exit code.
    pub exit_code: i32,
    /// Duration in milliseconds when timestamps are well-ordered.
    pub duration_ms: Option<u128>,
    /// Artifact ids linked to this step.
    pub artifact_ids: Vec<String>,
    /// First-failure diagnostic attached to this step.
    pub first_failure: Option<WorkflowFirstFailure>,
}

/// Published artifact index row.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArtifactIndexEntry {
    /// Stable id used by step references.
    pub artifact_id: String,
    /// User-facing artifact role.
    pub role: String,
    /// Path or URI.
    pub path: String,
    /// SHA-256 payload hash.
    pub sha256: String,
    /// Command that regenerates the artifact.
    pub replay_command: String,
    /// Observation timestamp.
    pub observed_unix_ms: Option<u128>,
    /// Age relative to report generation.
    pub age_ms: Option<u128>,
    /// Whether the artifact is within the freshness budget.
    pub fresh: bool,
    /// Whether the artifact is required for certification.
    pub required: bool,
}

/// Validation violation for workflow publication.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkflowViolation {
    /// Stable violation code.
    pub code: String,
    /// Human detail.
    pub detail: String,
}

/// Machine-readable workflow report.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkflowReport {
    /// Workflow schema version.
    pub schema_version: String,
    /// Owning bead id.
    pub bead_id: String,
    /// Run identifier.
    pub run_id: String,
    /// Trace identifier.
    pub trace_id: String,
    /// Scenario identifier.
    pub scenario_id: String,
    /// Deterministic seed.
    pub seed: u64,
    /// Report generation timestamp.
    pub generated_unix_ms: u128,
    /// Freshness budget in milliseconds.
    pub freshness_budget_ms: u128,
    /// Ordered phase observations.
    pub steps: Vec<WorkflowStepReport>,
    /// Navigable artifact index.
    pub artifact_index: Vec<ArtifactIndexEntry>,
    /// First failed phase, if any.
    pub first_failure: Option<WorkflowFirstFailure>,
    /// Suggested next commands.
    pub next_steps: Vec<String>,
    /// Validation violations.
    pub validation_violations: Vec<WorkflowViolation>,
    /// Whether every required phase and artifact is certifying.
    pub workflow_complete: bool,
    /// Whether the workflow is ready to feed the final parity certificate bead.
    pub certificate_ready: bool,
    /// Compact human summary.
    pub summary: String,
}

/// Build the published workflow report from wrapper observations.
#[must_use]
pub fn build_workflow_report(input: WorkflowInput) -> WorkflowReport {
    let steps = input
        .steps
        .iter()
        .map(build_step_report)
        .collect::<Vec<_>>();
    let artifact_index = input
        .artifacts
        .iter()
        .map(|artifact| build_artifact_index_entry(artifact, &input))
        .collect::<Vec<_>>();
    let first_failure = first_failure(&input.steps);
    let mut report = WorkflowReport {
        schema_version: SCHEMA_VERSION.to_owned(),
        bead_id: BEAD_ID.to_owned(),
        run_id: input.run_id,
        trace_id: input.trace_id,
        scenario_id: input.scenario_id,
        seed: input.seed,
        generated_unix_ms: input.generated_unix_ms,
        freshness_budget_ms: input.freshness_budget_ms,
        steps,
        artifact_index,
        first_failure,
        next_steps: Vec::new(),
        validation_violations: Vec::new(),
        workflow_complete: false,
        certificate_ready: false,
        summary: String::new(),
    };

    report.validation_violations = validate_workflow_report(&report);
    report.workflow_complete = report.validation_violations.is_empty();
    report.certificate_ready = report.workflow_complete
        && report.steps.last().is_some_and(|step| {
            step.phase == WorkflowPhase::CertificateReadiness.as_str()
                && step.outcome == WorkflowOutcome::Pass.as_str()
        });
    report.next_steps = next_steps(&report);
    report.summary = workflow_summary(&report);
    report
}

/// Validate workflow completeness.
#[must_use]
pub fn validate_workflow_report(report: &WorkflowReport) -> Vec<WorkflowViolation> {
    let mut violations = Vec::new();
    validate_metadata(report, &mut violations);
    validate_phase_order(report, &mut violations);
    validate_step_contracts(report, &mut violations);
    validate_artifacts(report, &mut violations);
    violations
}

/// Render a deterministic Markdown navigator.
#[must_use]
pub fn render_workflow_markdown(report: &WorkflowReport) -> String {
    let mut output = String::new();
    output.push_str("# FrankenSQLite Parity Verification Workflow\n\n");
    output.push_str(&format!("bead_id: `{}`\n", report.bead_id));
    output.push_str(&format!("schema_version: `{}`\n", report.schema_version));
    output.push_str(&format!("run_id: `{}`\n", report.run_id));
    output.push_str(&format!("trace_id: `{}`\n", report.trace_id));
    output.push_str(&format!("scenario_id: `{}`\n", report.scenario_id));
    output.push_str(&format!(
        "workflow_complete: `{}`\n",
        report.workflow_complete
    ));
    output.push_str(&format!(
        "certificate_ready: `{}`\n\n",
        report.certificate_ready
    ));

    output.push_str("## Ordered Gates\n\n");
    for step in &report.steps {
        output.push_str(&format!(
            "- `{}` outcome=`{}` exit_code=`{}` duration_ms=`{}` command=`{}`\n",
            step.phase,
            step.outcome,
            step.exit_code,
            step.duration_ms
                .map_or_else(|| "invalid".to_owned(), |duration| duration.to_string()),
            step.command,
        ));
    }
    output.push('\n');

    output.push_str("## Artifact Index\n\n");
    for artifact in &report.artifact_index {
        output.push_str(&format!(
            "- `{}` role=`{}` fresh=`{}` required=`{}` sha256=`{}` path=`{}` replay=`{}`\n",
            artifact.artifact_id,
            artifact.role,
            artifact.fresh,
            artifact.required,
            artifact.sha256,
            artifact.path,
            artifact.replay_command,
        ));
    }
    output.push('\n');

    output.push_str("## First Failure\n\n");
    if let Some(failure) = &report.first_failure {
        output.push_str(&format!(
            "- phase: `{}`\n- diagnostic_json_pointer: `{}`\n- summary: {}\n",
            failure.phase.as_str(),
            failure.diagnostic_json_pointer,
            failure.summary,
        ));
        output.push_str("- artifacts:\n");
        for artifact_id in &failure.artifact_ids {
            output.push_str(&format!("  - `{artifact_id}`\n"));
        }
        output.push_str("- remediation_commands:\n");
        for command in &failure.remediation_commands {
            output.push_str(&format!("  - `{command}`\n"));
        }
    } else {
        output.push_str("- none\n");
    }
    output.push('\n');

    output.push_str("## Validation\n\n");
    if report.validation_violations.is_empty() {
        output.push_str("- no validation violations\n");
    } else {
        for violation in &report.validation_violations {
            output.push_str(&format!("- `{}`: {}\n", violation.code, violation.detail));
        }
    }

    output.push_str("\n## Next Steps\n\n");
    for command in &report.next_steps {
        output.push_str(&format!("- `{command}`\n"));
    }
    output
}

fn build_step_report(input: &WorkflowStepInput) -> WorkflowStepReport {
    WorkflowStepReport {
        phase: input.phase.as_str().to_owned(),
        order_index: input.phase.order_index(),
        command: input.command.clone(),
        outcome: input.outcome.as_str().to_owned(),
        exit_code: input.exit_code,
        duration_ms: input.finished_unix_ms.checked_sub(input.started_unix_ms),
        artifact_ids: input.artifact_ids.clone(),
        first_failure: input.first_failure.clone(),
    }
}

fn build_artifact_index_entry(
    artifact: &WorkflowArtifactInput,
    input: &WorkflowInput,
) -> ArtifactIndexEntry {
    let age_ms = artifact
        .observed_unix_ms
        .and_then(|observed| input.generated_unix_ms.checked_sub(observed));
    let fresh = age_ms.is_some_and(|age| age <= input.freshness_budget_ms);
    ArtifactIndexEntry {
        artifact_id: artifact.artifact_id.clone(),
        role: artifact.role.clone(),
        path: artifact.path.clone(),
        sha256: artifact.sha256.clone(),
        replay_command: artifact.replay_command.clone(),
        observed_unix_ms: artifact.observed_unix_ms,
        age_ms,
        fresh,
        required: artifact.required,
    }
}

fn first_failure(steps: &[WorkflowStepInput]) -> Option<WorkflowFirstFailure> {
    steps
        .iter()
        .find(|step| step.outcome != WorkflowOutcome::Pass)
        .map(|step| {
            step.first_failure
                .clone()
                .unwrap_or_else(|| WorkflowFirstFailure {
                    phase: step.phase,
                    summary: format!(
                        "{} failed without a structured first-failure diagnostic",
                        step.phase.as_str()
                    ),
                    diagnostic_json_pointer: "/steps".to_owned(),
                    artifact_ids: step.artifact_ids.clone(),
                    remediation_commands: vec![format!("rerun: {}", step.command)],
                })
        })
}

fn validate_metadata(report: &WorkflowReport, violations: &mut Vec<WorkflowViolation>) {
    for (field, value) in [
        ("run_id", report.run_id.as_str()),
        ("trace_id", report.trace_id.as_str()),
        ("scenario_id", report.scenario_id.as_str()),
    ] {
        if value.trim().is_empty() {
            violations.push(violation(
                "missing_workflow_metadata",
                format!("{field} must be non-empty"),
            ));
        }
    }
    if report.freshness_budget_ms == 0 {
        violations.push(violation(
            "invalid_freshness_budget",
            "freshness_budget_ms must be positive",
        ));
    }
}

fn validate_phase_order(report: &WorkflowReport, violations: &mut Vec<WorkflowViolation>) {
    let expected = WorkflowPhase::required_order();
    if report.steps.len() != expected.len() {
        violations.push(violation(
            "workflow_phase_count_mismatch",
            format!(
                "expected {} phases, observed {}",
                expected.len(),
                report.steps.len()
            ),
        ));
    }

    for expected_phase in expected {
        let count = report
            .steps
            .iter()
            .filter(|step| step.phase == expected_phase.as_str())
            .count();
        match count {
            0 => violations.push(violation(
                "missing_workflow_phase",
                format!("missing required phase {}", expected_phase.as_str()),
            )),
            1 => {}
            _ => violations.push(violation(
                "duplicate_workflow_phase",
                format!("phase {} appears {count} times", expected_phase.as_str()),
            )),
        }
    }

    for (index, step) in report.steps.iter().enumerate() {
        if let Some(expected_phase) = expected.get(index)
            && step.phase != expected_phase.as_str()
        {
            violations.push(violation(
                "workflow_phase_order_mismatch",
                format!(
                    "phase index {index} expected {} but observed {}",
                    expected_phase.as_str(),
                    step.phase
                ),
            ));
        }
    }
}

fn validate_step_contracts(report: &WorkflowReport, violations: &mut Vec<WorkflowViolation>) {
    for step in &report.steps {
        if step.command.trim().is_empty() {
            violations.push(violation(
                "missing_step_command",
                format!("phase {} is missing replay command", step.phase),
            ));
        }
        if step.duration_ms.is_none() {
            violations.push(violation(
                "invalid_step_timing",
                format!("phase {} finished before it started", step.phase),
            ));
        }
        if step.outcome != WorkflowOutcome::Pass.as_str() {
            violations.push(violation(
                "workflow_gate_failed",
                format!(
                    "phase {} ended with outcome={} exit_code={}",
                    step.phase, step.outcome, step.exit_code
                ),
            ));
            if step.first_failure.is_none() {
                violations.push(violation(
                    "missing_first_failure",
                    format!("phase {} failed without first_failure", step.phase),
                ));
            }
        }
    }
}

fn validate_artifacts(report: &WorkflowReport, violations: &mut Vec<WorkflowViolation>) {
    for role in REQUIRED_ARTIFACT_ROLES {
        if !report
            .artifact_index
            .iter()
            .any(|artifact| artifact.role == role)
        {
            violations.push(violation(
                "missing_required_artifact_role",
                format!("missing required artifact role {role}"),
            ));
        }
    }

    for artifact in &report.artifact_index {
        if artifact.artifact_id.trim().is_empty() || artifact.path.trim().is_empty() {
            violations.push(violation(
                "invalid_artifact_identity",
                format!("artifact role {} has an empty id or path", artifact.role),
            ));
        }
        if artifact.replay_command.trim().is_empty() {
            violations.push(violation(
                "missing_artifact_replay",
                format!(
                    "artifact {} is missing replay command",
                    artifact.artifact_id
                ),
            ));
        }
        if !is_sha256_hex(&artifact.sha256) {
            violations.push(violation(
                "invalid_artifact_hash",
                format!("artifact {} has invalid sha256", artifact.artifact_id),
            ));
        }
        if artifact.required && !artifact.fresh {
            violations.push(violation(
                "stale_required_artifact",
                format!(
                    "artifact {} role={} is missing freshness or outside budget",
                    artifact.artifact_id, artifact.role
                ),
            ));
        }
    }

    for step in &report.steps {
        for artifact_id in &step.artifact_ids {
            if !report
                .artifact_index
                .iter()
                .any(|artifact| &artifact.artifact_id == artifact_id)
            {
                violations.push(violation(
                    "unknown_step_artifact",
                    format!(
                        "phase {} references unknown artifact {artifact_id}",
                        step.phase
                    ),
                ));
            }
        }
    }

    if let Some(failure) = &report.first_failure {
        if failure.remediation_commands.is_empty() {
            violations.push(violation(
                "missing_failure_remediation",
                "first_failure remediation_commands must be non-empty",
            ));
        }
        for artifact_id in &failure.artifact_ids {
            if !report
                .artifact_index
                .iter()
                .any(|artifact| &artifact.artifact_id == artifact_id)
            {
                violations.push(violation(
                    "unknown_failure_artifact",
                    format!("first_failure references unknown artifact {artifact_id}"),
                ));
            }
        }
    }
}

fn next_steps(report: &WorkflowReport) -> Vec<String> {
    if let Some(failure) = &report.first_failure {
        return failure.remediation_commands.clone();
    }
    let stale_artifacts = report
        .artifact_index
        .iter()
        .filter(|artifact| artifact.required && !artifact.fresh)
        .map(|artifact| artifact.replay_command.clone())
        .collect::<Vec<_>>();
    if !stale_artifacts.is_empty() {
        return stale_artifacts;
    }
    vec![
        "br show bd-2yqp6.7.3 --json".to_owned(),
        "cargo run -p fsqlite-harness --bin parity_verification_workflow_runner -- --help"
            .to_owned(),
    ]
}

fn workflow_summary(report: &WorkflowReport) -> String {
    if report.certificate_ready {
        return "parity verification workflow is complete and certificate-ready".to_owned();
    }
    if let Some(failure) = &report.first_failure {
        return format!(
            "parity verification blocked at {}: {}",
            failure.phase.as_str(),
            failure.summary
        );
    }
    if let Some(violation) = report.validation_violations.first() {
        return format!(
            "parity verification blocked by {}: {}",
            violation.code, violation.detail
        );
    }
    "parity verification workflow is incomplete".to_owned()
}

fn violation(code: &str, detail: impl Into<String>) -> WorkflowViolation {
    WorkflowViolation {
        code: code.to_owned(),
        detail: detail.into(),
    }
}

fn is_sha256_hex(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}
