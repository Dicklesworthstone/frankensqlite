use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use fsqlite_harness::e2e_traceability::{
    ConcurrencyMode, InvocationContract, ScriptEntry, ScriptKind, StorageMode, TraceabilityMatrix,
};
use fsqlite_harness::parity_evidence_matrix::{EvidenceViolationKind, ParityEvidenceReport};

fn gate_binary_path() -> &'static Path {
    Path::new(env!("CARGO_BIN_EXE_parity_evidence_matrix_gate"))
}

fn write_minimal_issues_jsonl(workspace_root: &Path) -> PathBuf {
    let beads_dir = workspace_root.join(".beads");
    fs::create_dir_all(&beads_dir).expect("create .beads directory");

    let issues_path = beads_dir.join("issues.jsonl");
    let payload = r#"{"id":"bd-1dp9.7.5","issue_type":"task"}"#;
    fs::write(&issues_path, payload).expect("write issues.jsonl");
    issues_path
}

fn write_traceability_override_with_invalid_log_schema(workspace_root: &Path) -> PathBuf {
    let scripts_dir = workspace_root.join("scripts");
    fs::create_dir_all(&scripts_dir).expect("create scripts directory");
    fs::write(
        scripts_dir.join("verify_invalid_reference.sh"),
        "#!/usr/bin/env bash\nexit 0\n",
    )
    .expect("write synthetic script");

    let traceability = TraceabilityMatrix {
        schema_version: "1.0.0".to_owned(),
        bead_id: "bd-mblr.4.5.1".to_owned(),
        scripts: vec![ScriptEntry {
            path: "scripts/verify_invalid_reference.sh".to_owned(),
            kind: ScriptKind::ShellUtility,
            bead_id: Some("bd-1dp9.7.5".to_owned()),
            description: "synthetic invalid-log-reference scenario".to_owned(),
            invocation: InvocationContract {
                command: "bash scripts/verify_invalid_reference.sh".to_owned(),
                env_vars: Vec::new(),
                json_output: true,
                timeout_secs: Some(30),
            },
            scenario_ids: vec!["INFRA-7001".to_owned()],
            storage_modes: vec![StorageMode::InMemory],
            concurrency_modes: vec![ConcurrencyMode::Sequential],
            artifact_paths: vec!["artifacts/invalid-reference.json".to_owned()],
            log_schema_version: Some("1.bad.0".to_owned()),
        }],
        gaps: Vec::new(),
    };

    let override_path = workspace_root.join("traceability_override.json");
    let payload = serde_json::to_string_pretty(&traceability).expect("serialize traceability");
    fs::write(&override_path, payload).expect("write traceability override");
    override_path
}

#[test]
fn test_gate_binary_detects_missing_evidence_for_required_beads() {
    let temp_dir = tempfile::tempdir().expect("create temporary workspace");
    let workspace_root = temp_dir.path();
    let _issues_path = write_minimal_issues_jsonl(workspace_root);

    let output = Command::new(gate_binary_path())
        .arg("--workspace-root")
        .arg(workspace_root)
        .output()
        .expect("run parity_evidence_matrix_gate");

    assert_eq!(
        output.status.code(),
        Some(1),
        "expected non-zero exit code when evidence is missing"
    );

    let stdout = String::from_utf8(output.stdout).expect("stdout should be utf-8");
    let report: ParityEvidenceReport =
        serde_json::from_str(&stdout).expect("report should be valid json");

    assert!(!report.summary.overall_pass);
    assert!(report.summary.violation_count > 0);
    assert!(report.violations.iter().any(|violation| violation.kind
        == EvidenceViolationKind::MissingUnitEvidence
        || violation.kind == EvidenceViolationKind::MissingE2eEvidence
        || violation.kind == EvidenceViolationKind::MissingLogEvidence));
}

#[test]
fn test_gate_binary_detects_invalid_log_reference_from_traceability_override() {
    let temp_dir = tempfile::tempdir().expect("create temporary workspace");
    let workspace_root = temp_dir.path();
    let _issues_path = write_minimal_issues_jsonl(workspace_root);
    let override_path = write_traceability_override_with_invalid_log_schema(workspace_root);

    let output = Command::new(gate_binary_path())
        .arg("--workspace-root")
        .arg(workspace_root)
        .arg("--traceability-override")
        .arg(&override_path)
        .output()
        .expect("run parity_evidence_matrix_gate with traceability override");

    assert_eq!(
        output.status.code(),
        Some(1),
        "expected non-zero exit code for invalid log reference"
    );

    let stdout = String::from_utf8(output.stdout).expect("stdout should be utf-8");
    let report: ParityEvidenceReport =
        serde_json::from_str(&stdout).expect("report should be valid json");

    assert!(
        report
            .violations
            .iter()
            .any(|violation| violation.kind == EvidenceViolationKind::InvalidLogReference),
        "expected invalid log reference violation in report"
    );
}
