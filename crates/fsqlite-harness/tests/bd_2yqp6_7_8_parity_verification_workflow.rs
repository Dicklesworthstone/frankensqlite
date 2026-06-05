use fsqlite_harness::parity_verification_workflow::{
    DEFAULT_FRESHNESS_BUDGET_MS, WorkflowArtifactInput, WorkflowFirstFailure, WorkflowInput,
    WorkflowOutcome, WorkflowPhase, WorkflowStepInput, build_workflow_report,
    render_workflow_markdown,
};

const GENERATED_MS: u128 = 1_700_000_000_000;
const HASH: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

fn passing_input() -> WorkflowInput {
    let phases = WorkflowPhase::required_order();
    WorkflowInput {
        run_id: "bd-2yqp6.7.8-test".to_owned(),
        trace_id: "trace-bd-2yqp6.7.8-test".to_owned(),
        scenario_id: "PARITY-WORKFLOW-G8".to_owned(),
        seed: 7_258,
        generated_unix_ms: GENERATED_MS,
        freshness_budget_ms: DEFAULT_FRESHNESS_BUDGET_MS,
        steps: phases
            .into_iter()
            .enumerate()
            .map(|(index, phase)| WorkflowStepInput {
                phase,
                command: format!("run {}", phase.as_str()),
                outcome: WorkflowOutcome::Pass,
                exit_code: 0,
                started_unix_ms: GENERATED_MS + u128::try_from(index).expect("index fits"),
                finished_unix_ms: GENERATED_MS + u128::try_from(index + 1).expect("index fits"),
                artifact_ids: artifact_ids_for_phase(phase),
                first_failure: None,
            })
            .collect(),
        artifacts: vec![
            artifact("oracle_preflight", "oracle_preflight_json"),
            artifact("differential_manifest", "differential_manifest_json"),
            artifact("parity_evidence_matrix", "parity_evidence_matrix_json"),
            artifact("parity_status_json", "parity_status_json"),
            artifact("parity_status_markdown", "parity_status_markdown"),
            artifact(
                "fallback_boundary_inventory",
                "fallback_boundary_inventory_toml",
            ),
            artifact("fallback_decision_schema", "fallback_decision_schema_json"),
            artifact("fallback_denial_replay", "fallback_denial_replay_json"),
            artifact("g9_gate_summary", "g9_gate_summary_json"),
            artifact("events", "workflow_events_jsonl"),
        ],
    }
}

fn artifact_ids_for_phase(phase: WorkflowPhase) -> Vec<String> {
    match phase {
        WorkflowPhase::PreflightDoctor => vec!["oracle_preflight".to_owned()],
        WorkflowPhase::DifferentialCi => vec!["differential_manifest".to_owned()],
        WorkflowPhase::ParityEvidenceMatrix => vec!["parity_evidence_matrix".to_owned()],
        WorkflowPhase::ParityStatusReport => {
            vec![
                "parity_status_json".to_owned(),
                "parity_status_markdown".to_owned(),
            ]
        }
        WorkflowPhase::CertificateReadiness => vec![
            "oracle_preflight".to_owned(),
            "differential_manifest".to_owned(),
            "parity_evidence_matrix".to_owned(),
            "parity_status_json".to_owned(),
            "fallback_boundary_inventory".to_owned(),
            "fallback_decision_schema".to_owned(),
            "fallback_denial_replay".to_owned(),
            "g9_gate_summary".to_owned(),
        ],
    }
}

fn artifact(artifact_id: &str, role: &str) -> WorkflowArtifactInput {
    WorkflowArtifactInput {
        artifact_id: artifact_id.to_owned(),
        role: role.to_owned(),
        path: format!("artifacts/bd-2yqp6.7.8/{artifact_id}.json"),
        sha256: HASH.to_owned(),
        replay_command: format!("replay {artifact_id}"),
        observed_unix_ms: Some(GENERATED_MS),
        required: role != "workflow_events_jsonl",
    }
}

fn failure(phase: WorkflowPhase) -> WorkflowFirstFailure {
    WorkflowFirstFailure {
        phase,
        summary: format!("{} failed", phase.as_str()),
        diagnostic_json_pointer: "/steps/0".to_owned(),
        artifact_ids: artifact_ids_for_phase(phase),
        remediation_commands: vec![format!("rerun {}", phase.as_str())],
    }
}

#[test]
fn successful_workflow_is_certificate_ready() {
    let report = build_workflow_report(passing_input());

    assert!(
        report.workflow_complete,
        "expected complete workflow: {:?}",
        report.validation_violations
    );
    assert!(report.certificate_ready);
    assert!(report.first_failure.is_none());
    assert_eq!(report.artifact_index.len(), 10);

    let markdown = render_workflow_markdown(&report);
    assert!(markdown.contains("## Ordered Gates"));
    assert!(markdown.contains("## Artifact Index"));
    assert!(markdown.contains("certificate_ready: `true`"));
}

#[test]
fn preflight_failure_blocks_with_actionable_first_failure() {
    let mut input = passing_input();
    input.steps[0].outcome = WorkflowOutcome::Fail;
    input.steps[0].exit_code = 1;
    input.steps[0].first_failure = Some(failure(WorkflowPhase::PreflightDoctor));

    let report = build_workflow_report(input);

    assert!(!report.workflow_complete);
    assert!(!report.certificate_ready);
    let first_failure = report.first_failure.expect("first failure is surfaced");
    assert_eq!(first_failure.phase, WorkflowPhase::PreflightDoctor);
    assert_eq!(report.next_steps, vec!["rerun preflight_doctor".to_owned()]);
    assert!(
        report
            .validation_violations
            .iter()
            .any(|violation| violation.code == "workflow_gate_failed")
    );
}

#[test]
fn evidence_gate_failure_blocks_before_status_publication() {
    let mut input = passing_input();
    input.steps[2].outcome = WorkflowOutcome::Fail;
    input.steps[2].exit_code = 1;
    input.steps[2].first_failure = Some(failure(WorkflowPhase::ParityEvidenceMatrix));

    let report = build_workflow_report(input);

    assert!(!report.workflow_complete);
    let first_failure = report.first_failure.expect("first failure is surfaced");
    assert_eq!(first_failure.phase, WorkflowPhase::ParityEvidenceMatrix);
    assert_eq!(
        first_failure.artifact_ids,
        vec!["parity_evidence_matrix".to_owned()]
    );
}

#[test]
fn stale_required_evidence_blocks_certification() {
    let mut input = passing_input();
    input.freshness_budget_ms = 10;
    input.artifacts[1].observed_unix_ms = Some(GENERATED_MS - 11);

    let report = build_workflow_report(input);

    assert!(!report.workflow_complete);
    assert!(!report.certificate_ready);
    assert!(
        report
            .validation_violations
            .iter()
            .any(|violation| violation.code == "stale_required_artifact")
    );
}

#[test]
fn out_of_order_phases_are_not_certifying() {
    let mut input = passing_input();
    input.steps.swap(1, 2);

    let report = build_workflow_report(input);

    assert!(!report.workflow_complete);
    assert!(
        report
            .validation_violations
            .iter()
            .any(|violation| violation.code == "workflow_phase_order_mismatch")
    );
}
