//! Verification-contract enforcement bridge (bd-1dp9.7.7).
//!
//! Integrates parity evidence validation (bd-1dp9.7.5) into gate outcomes by:
//! - classifying missing/invalid evidence failures per bead,
//! - producing deterministic bead-level verdicts,
//! - enforcing final pass/fail decisions for CI/scorecard/release gates.

use std::collections::BTreeMap;
use std::fmt;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::parity_evidence_matrix::{
    EvidenceViolation, EvidenceViolationKind, ParityEvidenceReport,
    generate_workspace_parity_evidence_report,
};

/// Bead identifier for this enforcement bridge.
pub const BEAD_ID: &str = "bd-1dp9.7.7";
/// Schema version for machine-readable enforcement payloads.
pub const CONTRACT_ENFORCEMENT_SCHEMA_VERSION: u32 = 1;

/// Per-bead verdict for the verification contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContractBeadStatus {
    Pass,
    FailMissingEvidence,
    FailInvalidReferences,
    FailMixed,
}

impl ContractBeadStatus {
    #[must_use]
    pub const fn is_pass(self) -> bool {
        matches!(self, Self::Pass)
    }
}

impl fmt::Display for ContractBeadStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            Self::Pass => "pass",
            Self::FailMissingEvidence => "fail_missing_evidence",
            Self::FailInvalidReferences => "fail_invalid_references",
            Self::FailMixed => "fail_mixed",
        };
        f.write_str(value)
    }
}

/// Deterministic contract verdict for one bead.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeadContractVerdict {
    pub bead_id: String,
    pub status: ContractBeadStatus,
    pub missing_evidence_count: usize,
    pub invalid_reference_count: usize,
    pub details: Vec<String>,
}

/// Classification report for parity verification contract validity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VerificationContractReport {
    pub schema_version: u32,
    pub bead_id: String,
    pub generated_unix_ms: u128,
    pub workspace_root: String,
    pub total_beads: usize,
    pub passing_beads: usize,
    pub failing_beads: usize,
    pub missing_evidence_beads: usize,
    pub invalid_reference_beads: usize,
    pub overall_pass: bool,
    pub bead_verdicts: Vec<BeadContractVerdict>,
    pub violations: Vec<EvidenceViolation>,
}

/// Final enforcement disposition when combining base gate + contract gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EnforcementDisposition {
    Allowed,
    BlockedByBaseGate,
    BlockedByContract,
    BlockedByBoth,
}

impl fmt::Display for EnforcementDisposition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            Self::Allowed => "allowed",
            Self::BlockedByBaseGate => "blocked_by_base_gate",
            Self::BlockedByContract => "blocked_by_contract",
            Self::BlockedByBoth => "blocked_by_both",
        };
        f.write_str(value)
    }
}

/// Enforcement result used by CI manifests, scorecards, and release ledgers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContractEnforcementOutcome {
    pub schema_version: u32,
    pub bead_id: String,
    pub base_gate_passed: bool,
    pub contract_passed: bool,
    pub final_gate_passed: bool,
    pub disposition: EnforcementDisposition,
    pub total_beads: usize,
    pub failing_beads: usize,
    pub missing_evidence_beads: usize,
    pub invalid_reference_beads: usize,
    pub bead_verdicts: Vec<BeadContractVerdict>,
}

#[derive(Default)]
struct BeadCounters {
    missing_evidence_count: usize,
    invalid_reference_count: usize,
    details: Vec<String>,
}

/// Build a classification report from a parity evidence report.
#[must_use]
pub fn classify_parity_evidence_report(
    parity_report: &ParityEvidenceReport,
) -> VerificationContractReport {
    let mut by_bead: BTreeMap<String, BeadCounters> = BTreeMap::new();

    for row in &parity_report.rows {
        by_bead.entry(row.bead_id.clone()).or_default();
    }

    for violation in &parity_report.violations {
        let counters = by_bead.entry(violation.bead_id.clone()).or_default();
        if is_missing_violation(violation.kind) {
            counters.missing_evidence_count += 1;
        } else if is_invalid_violation(violation.kind) {
            counters.invalid_reference_count += 1;
        }
        counters.details.push(format!(
            "kind={} detail={}",
            violation.kind, violation.detail
        ));
    }

    let mut bead_verdicts = Vec::with_capacity(by_bead.len());
    let mut missing_evidence_beads = 0_usize;
    let mut invalid_reference_beads = 0_usize;

    for (bead_id, counters) in by_bead {
        let status = classify_bead_status(
            counters.missing_evidence_count,
            counters.invalid_reference_count,
        );
        if matches!(
            status,
            ContractBeadStatus::FailMissingEvidence | ContractBeadStatus::FailMixed
        ) {
            missing_evidence_beads += 1;
        }
        if matches!(
            status,
            ContractBeadStatus::FailInvalidReferences | ContractBeadStatus::FailMixed
        ) {
            invalid_reference_beads += 1;
        }

        bead_verdicts.push(BeadContractVerdict {
            bead_id,
            status,
            missing_evidence_count: counters.missing_evidence_count,
            invalid_reference_count: counters.invalid_reference_count,
            details: counters.details,
        });
    }

    let total_beads = bead_verdicts.len();
    let failing_beads = bead_verdicts
        .iter()
        .filter(|verdict| !verdict.status.is_pass())
        .count();
    let passing_beads = total_beads.saturating_sub(failing_beads);
    let overall_pass = failing_beads == 0;

    VerificationContractReport {
        schema_version: CONTRACT_ENFORCEMENT_SCHEMA_VERSION,
        bead_id: BEAD_ID.to_owned(),
        generated_unix_ms: unix_time_ms(),
        workspace_root: parity_report.workspace_root.clone(),
        total_beads,
        passing_beads,
        failing_beads,
        missing_evidence_beads,
        invalid_reference_beads,
        overall_pass,
        bead_verdicts,
        violations: parity_report.violations.clone(),
    }
}

/// Evaluate the workspace verification contract from canonical inventories.
///
/// # Errors
///
/// Returns `Err` when the workspace report cannot be generated.
pub fn evaluate_workspace_verification_contract(
    workspace_root: &Path,
) -> Result<VerificationContractReport, String> {
    let parity_report = generate_workspace_parity_evidence_report(workspace_root)?;
    Ok(classify_parity_evidence_report(&parity_report))
}

/// Combine a base gate pass/fail with verification-contract validity.
#[must_use]
pub fn enforce_gate_decision(
    base_gate_passed: bool,
    contract: &VerificationContractReport,
) -> ContractEnforcementOutcome {
    let contract_passed = contract.overall_pass;
    let final_gate_passed = base_gate_passed && contract_passed;
    let disposition = match (base_gate_passed, contract_passed) {
        (true, true) => EnforcementDisposition::Allowed,
        (false, true) => EnforcementDisposition::BlockedByBaseGate,
        (true, false) => EnforcementDisposition::BlockedByContract,
        (false, false) => EnforcementDisposition::BlockedByBoth,
    };

    ContractEnforcementOutcome {
        schema_version: CONTRACT_ENFORCEMENT_SCHEMA_VERSION,
        bead_id: BEAD_ID.to_owned(),
        base_gate_passed,
        contract_passed,
        final_gate_passed,
        disposition,
        total_beads: contract.total_beads,
        failing_beads: contract.failing_beads,
        missing_evidence_beads: contract.missing_evidence_beads,
        invalid_reference_beads: contract.invalid_reference_beads,
        bead_verdicts: contract.bead_verdicts.clone(),
    }
}

/// Render deterministic structured log lines for contract enforcement results.
#[must_use]
pub fn render_contract_enforcement_logs(outcome: &ContractEnforcementOutcome) -> Vec<String> {
    let mut lines = Vec::with_capacity(1 + outcome.bead_verdicts.len());
    lines.push(format!(
        "bead_id={} event=verification_contract_enforcement disposition={} base_gate_passed={} contract_passed={} final_gate_passed={} total_beads={} failing_beads={} missing_evidence_beads={} invalid_reference_beads={}",
        BEAD_ID,
        outcome.disposition,
        outcome.base_gate_passed,
        outcome.contract_passed,
        outcome.final_gate_passed,
        outcome.total_beads,
        outcome.failing_beads,
        outcome.missing_evidence_beads,
        outcome.invalid_reference_beads,
    ));

    for verdict in &outcome.bead_verdicts {
        lines.push(format!(
            "bead_id={} event=contract_bead_verdict contract_bead_id={} status={} missing_evidence_count={} invalid_reference_count={} detail_count={}",
            BEAD_ID,
            verdict.bead_id,
            verdict.status,
            verdict.missing_evidence_count,
            verdict.invalid_reference_count,
            verdict.details.len(),
        ));
    }

    lines
}

fn is_missing_violation(kind: EvidenceViolationKind) -> bool {
    matches!(
        kind,
        EvidenceViolationKind::MissingUnitEvidence
            | EvidenceViolationKind::MissingE2eEvidence
            | EvidenceViolationKind::MissingLogEvidence
    )
}

fn is_invalid_violation(kind: EvidenceViolationKind) -> bool {
    matches!(
        kind,
        EvidenceViolationKind::InvalidE2eReference | EvidenceViolationKind::InvalidLogReference
    )
}

fn classify_bead_status(missing_count: usize, invalid_count: usize) -> ContractBeadStatus {
    match (missing_count > 0, invalid_count > 0) {
        (false, false) => ContractBeadStatus::Pass,
        (true, false) => ContractBeadStatus::FailMissingEvidence,
        (false, true) => ContractBeadStatus::FailInvalidReferences,
        (true, true) => ContractBeadStatus::FailMixed,
    }
}

fn unix_time_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parity_evidence_matrix::{EvidenceSummary, ParityEvidenceReport, ParityEvidenceRow};

    fn synthetic_report(violations: Vec<EvidenceViolation>) -> ParityEvidenceReport {
        let rows = vec![
            ParityEvidenceRow {
                bead_id: "bd-1dp9.7.7".to_owned(),
                unit_test_ids: vec!["UT-1".to_owned()],
                e2e_script_paths: vec!["scripts/test.sh".to_owned()],
                log_schema_refs: vec!["scripts/test.sh@1.0.0".to_owned()],
            },
            ParityEvidenceRow {
                bead_id: "bd-1dp9.7.8".to_owned(),
                unit_test_ids: vec!["UT-2".to_owned()],
                e2e_script_paths: vec!["scripts/test2.sh".to_owned()],
                log_schema_refs: vec!["scripts/test2.sh@1.0.0".to_owned()],
            },
        ];
        let overall_pass = violations.is_empty();
        ParityEvidenceReport {
            schema_version: 1,
            bead_id: "bd-1dp9.7.5".to_owned(),
            generated_unix_ms: 0,
            workspace_root: ".".to_owned(),
            rows,
            summary: EvidenceSummary {
                required_bead_count: 2,
                row_count: 2,
                violation_count: violations.len(),
                overall_pass,
            },
            violations,
        }
    }

    fn missing_violation(bead_id: &str) -> EvidenceViolation {
        EvidenceViolation {
            bead_id: bead_id.to_owned(),
            kind: EvidenceViolationKind::MissingLogEvidence,
            detail: "missing log evidence".to_owned(),
        }
    }

    fn invalid_violation(bead_id: &str) -> EvidenceViolation {
        EvidenceViolation {
            bead_id: bead_id.to_owned(),
            kind: EvidenceViolationKind::InvalidLogReference,
            detail: "invalid log reference".to_owned(),
        }
    }

    fn verdict_for<'a>(
        report: &'a VerificationContractReport,
        bead_id: &str,
    ) -> &'a BeadContractVerdict {
        report
            .bead_verdicts
            .iter()
            .find(|verdict| verdict.bead_id == bead_id)
            .expect("bead verdict should exist")
    }

    #[test]
    fn classify_report_marks_missing_evidence_failures() {
        let parity = synthetic_report(vec![missing_violation("bd-1dp9.7.7")]);
        let report = classify_parity_evidence_report(&parity);

        assert!(!report.overall_pass);
        assert_eq!(report.failing_beads, 1);
        assert_eq!(report.missing_evidence_beads, 1);
        assert_eq!(report.invalid_reference_beads, 0);
        assert_eq!(
            verdict_for(&report, "bd-1dp9.7.7").status,
            ContractBeadStatus::FailMissingEvidence
        );
    }

    #[test]
    fn classify_report_marks_invalid_reference_failures() {
        let parity = synthetic_report(vec![invalid_violation("bd-1dp9.7.7")]);
        let report = classify_parity_evidence_report(&parity);

        assert!(!report.overall_pass);
        assert_eq!(report.failing_beads, 1);
        assert_eq!(report.missing_evidence_beads, 0);
        assert_eq!(report.invalid_reference_beads, 1);
        assert_eq!(
            verdict_for(&report, "bd-1dp9.7.7").status,
            ContractBeadStatus::FailInvalidReferences
        );
    }

    #[test]
    fn classify_report_marks_mixed_failures() {
        let parity = synthetic_report(vec![
            missing_violation("bd-1dp9.7.7"),
            invalid_violation("bd-1dp9.7.7"),
        ]);
        let report = classify_parity_evidence_report(&parity);

        assert!(!report.overall_pass);
        assert_eq!(report.failing_beads, 1);
        assert_eq!(report.missing_evidence_beads, 1);
        assert_eq!(report.invalid_reference_beads, 1);
        assert_eq!(
            verdict_for(&report, "bd-1dp9.7.7").status,
            ContractBeadStatus::FailMixed
        );
    }

    #[test]
    fn classify_report_marks_pass_when_no_violations() {
        let parity = synthetic_report(Vec::new());
        let report = classify_parity_evidence_report(&parity);

        assert!(report.overall_pass);
        assert_eq!(report.failing_beads, 0);
        assert_eq!(report.passing_beads, 2);
        assert!(
            report
                .bead_verdicts
                .iter()
                .all(|verdict| verdict.status == ContractBeadStatus::Pass)
        );
    }

    #[test]
    fn enforcement_disposition_matrix_is_correct() {
        let pass_report = classify_parity_evidence_report(&synthetic_report(Vec::new()));
        let fail_report =
            classify_parity_evidence_report(&synthetic_report(vec![missing_violation(
                "bd-1dp9.7.7",
            )]));

        let allowed = enforce_gate_decision(true, &pass_report);
        assert!(allowed.final_gate_passed);
        assert_eq!(allowed.disposition, EnforcementDisposition::Allowed);

        let blocked_by_contract = enforce_gate_decision(true, &fail_report);
        assert!(!blocked_by_contract.final_gate_passed);
        assert_eq!(
            blocked_by_contract.disposition,
            EnforcementDisposition::BlockedByContract
        );

        let blocked_by_base = enforce_gate_decision(false, &pass_report);
        assert!(!blocked_by_base.final_gate_passed);
        assert_eq!(
            blocked_by_base.disposition,
            EnforcementDisposition::BlockedByBaseGate
        );

        let blocked_by_both = enforce_gate_decision(false, &fail_report);
        assert!(!blocked_by_both.final_gate_passed);
        assert_eq!(
            blocked_by_both.disposition,
            EnforcementDisposition::BlockedByBoth
        );
    }

    #[test]
    fn structured_logs_include_summary_and_bead_verdict_lines() {
        let fail_report =
            classify_parity_evidence_report(&synthetic_report(vec![invalid_violation(
                "bd-1dp9.7.7",
            )]));
        let outcome = enforce_gate_decision(true, &fail_report);
        let logs = render_contract_enforcement_logs(&outcome);

        assert!(!logs.is_empty());
        assert!(
            logs[0].contains("event=verification_contract_enforcement"),
            "summary log line should contain enforcement event"
        );
        assert!(
            logs.iter()
                .any(|line| line.contains("event=contract_bead_verdict")
                    && line.contains("contract_bead_id=bd-1dp9.7.7")
                    && line.contains("status=fail_invalid_references"))
        );
    }
}
