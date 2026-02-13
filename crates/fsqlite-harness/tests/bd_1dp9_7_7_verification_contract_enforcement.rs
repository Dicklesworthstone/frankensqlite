//! E2E tests for bd-1dp9.7.7 verification contract enforcement integration.
//!
//! Verifies enforcement wiring across:
//! - CI artifact manifests (ci_gate_matrix),
//! - scorecard release decisions (score_engine),
//! - release handoff evidence ledgers (confidence_gates).

use std::fs;

use fsqlite_harness::ci_gate_matrix::{
    ArtifactEntry, ArtifactKind, CiLane, build_artifact_manifest_with_contract,
};
use fsqlite_harness::confidence_gates::{
    apply_contract_outcome_to_gate_report, build_evidence_ledger, evaluate_full,
};
use fsqlite_harness::parity_evidence_matrix::{
    EvidenceSummary, EvidenceViolation, EvidenceViolationKind, ParityEvidenceReport,
    ParityEvidenceRow,
};
use fsqlite_harness::parity_invariant_catalog::build_canonical_catalog;
use fsqlite_harness::parity_taxonomy::build_canonical_universe;
use fsqlite_harness::score_engine::{ScoreEngineConfig, compute_bayesian_scorecard_with_contract};
use fsqlite_harness::verification_contract_enforcement::{
    ContractBeadStatus, EnforcementDisposition, classify_parity_evidence_report,
    enforce_gate_decision, render_contract_enforcement_logs,
};

const BEAD_ID: &str = "bd-1dp9.7.7";
const SEED: u64 = 20260213;

fn synthetic_parity_report(violations: Vec<EvidenceViolation>) -> ParityEvidenceReport {
    ParityEvidenceReport {
        schema_version: 1,
        bead_id: "bd-1dp9.7.5".to_owned(),
        generated_unix_ms: 0,
        workspace_root: ".".to_owned(),
        rows: vec![ParityEvidenceRow {
            bead_id: "bd-1dp9.7.7".to_owned(),
            unit_test_ids: vec!["UT-1".to_owned()],
            e2e_script_paths: vec!["scripts/test.sh".to_owned()],
            log_schema_refs: vec!["scripts/test.sh@1.0.0".to_owned()],
        }],
        summary: EvidenceSummary {
            required_bead_count: 1,
            row_count: 1,
            violation_count: violations.len(),
            overall_pass: violations.is_empty(),
        },
        violations,
    }
}

fn missing_violation() -> EvidenceViolation {
    EvidenceViolation {
        bead_id: "bd-1dp9.7.7".to_owned(),
        kind: EvidenceViolationKind::MissingUnitEvidence,
        detail: "no unit evidence linked".to_owned(),
    }
}

fn invalid_violation() -> EvidenceViolation {
    EvidenceViolation {
        bead_id: "bd-1dp9.7.7".to_owned(),
        kind: EvidenceViolationKind::InvalidLogReference,
        detail: "invalid schema version".to_owned(),
    }
}

#[test]
fn e2e_ci_manifest_blocked_for_missing_and_invalid_evidence() {
    for (name, violation, expected_status) in [
        (
            "missing",
            missing_violation(),
            ContractBeadStatus::FailMissingEvidence,
        ),
        (
            "invalid",
            invalid_violation(),
            ContractBeadStatus::FailInvalidReferences,
        ),
    ] {
        let contract = classify_parity_evidence_report(&synthetic_parity_report(vec![violation]));
        let enforcement = enforce_gate_decision(true, &contract);
        let manifest = build_artifact_manifest_with_contract(
            CiLane::Unit,
            &format!("{BEAD_ID}-{name}-{SEED}"),
            "abc123",
            SEED,
            true,
            vec![ArtifactEntry {
                kind: ArtifactKind::Report,
                path: "reports/unit.json".to_owned(),
                content_hash: "a".repeat(64),
                size_bytes: 32,
                description: "unit gate report".to_owned(),
            }],
            None,
            Some(enforcement.clone()),
        );

        assert!(!manifest.gate_passed);
        let contract = manifest
            .verification_contract
            .as_ref()
            .expect("manifest should carry contract payload");
        assert_eq!(
            contract.disposition,
            EnforcementDisposition::BlockedByContract
        );
        assert_eq!(
            contract.bead_verdicts[0].status, expected_status,
            "unexpected bead status for scenario {name}"
        );

        let logs = render_contract_enforcement_logs(contract);
        assert!(logs.iter().any(
            |line| line.contains("event=verification_contract_enforcement")
                && line.contains("disposition=blocked_by_contract")
        ));
        assert!(logs.iter().any(|line| {
            line.contains("event=contract_bead_verdict")
                && line.contains("contract_bead_id=bd-1dp9.7.7")
        }));
    }
}

#[test]
fn e2e_scorecard_release_blocked_when_contract_missing() {
    let temp_dir = tempfile::tempdir().expect("create temporary workspace");
    let beads_dir = temp_dir.path().join(".beads");
    fs::create_dir_all(&beads_dir).expect("create .beads directory");
    fs::write(
        beads_dir.join("issues.jsonl"),
        r#"{"id":"bd-1dp9.7.7","issue_type":"task"}"#,
    )
    .expect("write issues.jsonl");

    let universe = build_canonical_universe();
    let config = ScoreEngineConfig {
        release_threshold: 0.0,
        ..Default::default()
    };
    let scorecard = compute_bayesian_scorecard_with_contract(temp_dir.path(), &universe, &config)
        .expect("compute scorecard with contract");

    let contract = scorecard
        .verification_contract
        .as_ref()
        .expect("scorecard should include contract payload");
    assert!(contract.base_gate_passed);
    assert!(!contract.contract_passed);
    assert_eq!(
        contract.disposition,
        EnforcementDisposition::BlockedByContract
    );
    assert!(!scorecard.release_ready);
}

#[test]
fn e2e_release_handoff_ledger_carries_contract_verdict() {
    let catalog = build_canonical_catalog();
    let universe = build_canonical_universe();
    let (report, ranking) = evaluate_full(&catalog, &universe, &Default::default());
    let contract =
        classify_parity_evidence_report(&synthetic_parity_report(vec![missing_violation()]));
    let enforcement = enforce_gate_decision(true, &contract);
    let enforced_report = apply_contract_outcome_to_gate_report(report, enforcement.clone());
    let ledger = build_evidence_ledger(&enforced_report, &ranking);

    let ledger_contract = ledger
        .verification_contract
        .as_ref()
        .expect("ledger should include contract payload");
    assert_eq!(
        ledger_contract.disposition,
        EnforcementDisposition::BlockedByContract
    );
    assert!(!ledger.release_ready);

    let logs = render_contract_enforcement_logs(ledger_contract);
    assert!(
        logs.first()
            .expect("summary line")
            .contains("event=verification_contract_enforcement")
    );
}
