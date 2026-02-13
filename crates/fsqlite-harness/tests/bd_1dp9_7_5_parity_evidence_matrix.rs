use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

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
