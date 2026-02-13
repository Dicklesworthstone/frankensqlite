//! Parity evidence contract matrix and validation gate (bd-1dp9.7.5).
//!
//! This module builds a machine-readable matrix mapping parity closure beads to
//! required verification artifacts across three layers:
//! - unit test evidence,
//! - e2e script evidence,
//! - structured log schema evidence.
//!
//! It then validates completeness and reference integrity so missing evidence is
//! reported with bead-scoped diagnostics.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::e2e_traceability::TraceabilityMatrix;
use crate::unit_matrix::UnitMatrix;

/// Bead identifier for this contract gate.
pub const BEAD_ID: &str = "bd-1dp9.7.5";
/// Schema version for matrix report serialization.
pub const EVIDENCE_MATRIX_SCHEMA_VERSION: u32 = 1;

/// Per-bead evidence references across required layers.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ParityEvidenceRow {
    /// Parity closure bead ID.
    pub bead_id: String,
    /// Unit test IDs linked to this bead.
    pub unit_test_ids: Vec<String>,
    /// E2E script paths linked to this bead.
    pub e2e_script_paths: Vec<String>,
    /// Structured log schema references (`<script_path>@<schema_version>`).
    pub log_schema_refs: Vec<String>,
}

/// Violation class emitted by matrix validation.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceViolationKind {
    MissingUnitEvidence,
    MissingE2eEvidence,
    MissingLogEvidence,
    InvalidE2eReference,
    InvalidLogReference,
}

impl fmt::Display for EvidenceViolationKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            Self::MissingUnitEvidence => "missing_unit_evidence",
            Self::MissingE2eEvidence => "missing_e2e_evidence",
            Self::MissingLogEvidence => "missing_log_evidence",
            Self::InvalidE2eReference => "invalid_e2e_reference",
            Self::InvalidLogReference => "invalid_log_reference",
        };
        f.write_str(value)
    }
}

/// One validation violation with bead-scoped detail.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EvidenceViolation {
    pub bead_id: String,
    pub kind: EvidenceViolationKind,
    pub detail: String,
}

/// Summary counters for matrix reporting.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EvidenceSummary {
    pub required_bead_count: usize,
    pub row_count: usize,
    pub violation_count: usize,
    pub overall_pass: bool,
}

/// Full parity evidence report consumed by scripts and gate binaries.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ParityEvidenceReport {
    pub schema_version: u32,
    pub bead_id: String,
    pub generated_unix_ms: u128,
    pub workspace_root: String,
    pub rows: Vec<ParityEvidenceRow>,
    pub violations: Vec<EvidenceViolation>,
    pub summary: EvidenceSummary,
}

#[derive(Debug, Clone, Deserialize)]
struct IssueJsonlRow {
    id: String,
    issue_type: String,
}

/// Load parity closure bead IDs from `.beads/issues.jsonl`.
///
/// Includes `bd-1dp9.*` items with type `task|feature|bug`.
pub fn load_parity_closure_bead_ids(path: &Path) -> Result<Vec<String>, String> {
    let payload = std::fs::read_to_string(path).map_err(|error| {
        format!(
            "issues_jsonl_read_failed path={} error={error}",
            path.display()
        )
    })?;

    let mut bead_ids = BTreeSet::new();
    for (line_index, line) in payload.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }

        let row: IssueJsonlRow = serde_json::from_str(line).map_err(|error| {
            format!(
                "issues_jsonl_parse_failed path={} line={} error={error}",
                path.display(),
                line_index + 1
            )
        })?;

        if is_parity_closure_issue(&row) {
            bead_ids.insert(row.id);
        }
    }

    Ok(bead_ids.into_iter().collect())
}

fn is_parity_closure_issue(row: &IssueJsonlRow) -> bool {
    row.id.starts_with("bd-1dp9.") && matches!(row.issue_type.as_str(), "task" | "feature" | "bug")
}

/// Build evidence rows from canonical unit/e2e inventories.
#[must_use]
pub fn build_parity_evidence_rows(
    required_bead_ids: &[String],
    unit_matrix: &UnitMatrix,
    traceability: &TraceabilityMatrix,
) -> Vec<ParityEvidenceRow> {
    let unit_refs_by_bead = collect_unit_refs_by_bead(unit_matrix);
    let e2e_refs_by_bead = collect_e2e_refs_by_bead(traceability);
    let log_refs_by_bead = collect_log_refs_by_bead(traceability);

    let mut rows = Vec::with_capacity(required_bead_ids.len());
    for bead_id in required_bead_ids {
        let unit_test_ids = unit_refs_by_bead
            .get(bead_id)
            .map_or_else(Vec::new, |items| items.iter().cloned().collect());
        let e2e_script_paths = e2e_refs_by_bead
            .get(bead_id)
            .map_or_else(Vec::new, |items| items.iter().cloned().collect());
        let log_schema_refs = log_refs_by_bead
            .get(bead_id)
            .map_or_else(Vec::new, |items| items.iter().cloned().collect());

        rows.push(ParityEvidenceRow {
            bead_id: bead_id.clone(),
            unit_test_ids,
            e2e_script_paths,
            log_schema_refs,
        });
    }

    rows
}

fn collect_unit_refs_by_bead(unit_matrix: &UnitMatrix) -> BTreeMap<String, BTreeSet<String>> {
    let mut refs_by_bead: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for test in &unit_matrix.tests {
        for bead_id in &test.failure_diagnostics.related_beads {
            refs_by_bead
                .entry(bead_id.clone())
                .or_default()
                .insert(test.test_id.clone());
        }
    }
    refs_by_bead
}

fn collect_e2e_refs_by_bead(
    traceability: &TraceabilityMatrix,
) -> BTreeMap<String, BTreeSet<String>> {
    let mut refs_by_bead: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for script in &traceability.scripts {
        if let Some(bead_id) = &script.bead_id {
            refs_by_bead
                .entry(bead_id.clone())
                .or_default()
                .insert(script.path.clone());
        }
    }
    refs_by_bead
}

fn collect_log_refs_by_bead(
    traceability: &TraceabilityMatrix,
) -> BTreeMap<String, BTreeSet<String>> {
    let mut refs_by_bead: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for script in &traceability.scripts {
        let Some(bead_id) = &script.bead_id else {
            continue;
        };
        let Some(version) = &script.log_schema_version else {
            continue;
        };

        refs_by_bead
            .entry(bead_id.clone())
            .or_default()
            .insert(format!("{}@{}", script.path, version));
    }
    refs_by_bead
}

/// Validate evidence rows for completeness and reference integrity.
#[must_use]
pub fn validate_parity_evidence_rows(
    rows: &[ParityEvidenceRow],
    workspace_root: &Path,
) -> Vec<EvidenceViolation> {
    let mut violations = Vec::new();

    for row in rows {
        if row.unit_test_ids.is_empty() {
            violations.push(EvidenceViolation {
                bead_id: row.bead_id.clone(),
                kind: EvidenceViolationKind::MissingUnitEvidence,
                detail: "no unit test IDs linked".to_owned(),
            });
        }
        if row.e2e_script_paths.is_empty() {
            violations.push(EvidenceViolation {
                bead_id: row.bead_id.clone(),
                kind: EvidenceViolationKind::MissingE2eEvidence,
                detail: "no e2e script paths linked".to_owned(),
            });
        }
        if row.log_schema_refs.is_empty() {
            violations.push(EvidenceViolation {
                bead_id: row.bead_id.clone(),
                kind: EvidenceViolationKind::MissingLogEvidence,
                detail: "no log schema references linked".to_owned(),
            });
        }

        for script_path in &row.e2e_script_paths {
            let candidate_path = workspace_root.join(script_path);
            if !candidate_path.is_file() {
                violations.push(EvidenceViolation {
                    bead_id: row.bead_id.clone(),
                    kind: EvidenceViolationKind::InvalidE2eReference,
                    detail: format!("missing script path: {script_path}"),
                });
            }
        }

        for reference in &row.log_schema_refs {
            let Some((script_path, schema_version)) = reference.rsplit_once('@') else {
                violations.push(EvidenceViolation {
                    bead_id: row.bead_id.clone(),
                    kind: EvidenceViolationKind::InvalidLogReference,
                    detail: format!("log reference missing @ separator: {reference}"),
                });
                continue;
            };

            if !row.e2e_script_paths.iter().any(|path| path == script_path) {
                violations.push(EvidenceViolation {
                    bead_id: row.bead_id.clone(),
                    kind: EvidenceViolationKind::InvalidLogReference,
                    detail: format!(
                        "log reference script not linked as e2e evidence: {script_path}"
                    ),
                });
            }

            if !is_semver(schema_version) {
                violations.push(EvidenceViolation {
                    bead_id: row.bead_id.clone(),
                    kind: EvidenceViolationKind::InvalidLogReference,
                    detail: format!("invalid log schema version: {schema_version}"),
                });
            }
        }
    }

    violations
}

fn is_semver(value: &str) -> bool {
    let mut parts = value.split('.');
    let Some(major) = parts.next() else {
        return false;
    };
    let Some(minor) = parts.next() else {
        return false;
    };
    let Some(patch) = parts.next() else {
        return false;
    };
    if parts.next().is_some() {
        return false;
    }

    [major, minor, patch]
        .iter()
        .all(|part| !part.is_empty() && part.chars().all(|ch| ch.is_ascii_digit()))
}

/// Build a complete report from provided inventories and required bead list.
#[must_use]
pub fn build_parity_evidence_report(
    workspace_root: &Path,
    required_bead_ids: &[String],
    unit_matrix: &UnitMatrix,
    traceability: &TraceabilityMatrix,
) -> ParityEvidenceReport {
    let rows = build_parity_evidence_rows(required_bead_ids, unit_matrix, traceability);
    let violations = validate_parity_evidence_rows(&rows, workspace_root);
    let summary = EvidenceSummary {
        required_bead_count: required_bead_ids.len(),
        row_count: rows.len(),
        violation_count: violations.len(),
        overall_pass: violations.is_empty(),
    };

    ParityEvidenceReport {
        schema_version: EVIDENCE_MATRIX_SCHEMA_VERSION,
        bead_id: BEAD_ID.to_owned(),
        generated_unix_ms: unix_time_ms(),
        workspace_root: workspace_root.display().to_string(),
        rows,
        violations,
        summary,
    }
}

/// Build the report from canonical harness inventories and workspace beads data.
pub fn generate_workspace_parity_evidence_report(
    workspace_root: &Path,
) -> Result<ParityEvidenceReport, String> {
    let issues_path = workspace_root.join(".beads/issues.jsonl");
    let required_bead_ids = load_parity_closure_bead_ids(&issues_path)?;
    let unit_matrix = crate::unit_matrix::build_canonical_matrix();
    let traceability = crate::e2e_traceability::build_canonical_inventory();

    Ok(build_parity_evidence_report(
        workspace_root,
        &required_bead_ids,
        &unit_matrix,
        &traceability,
    ))
}

/// Render violations as deterministic single-line diagnostics.
#[must_use]
pub fn render_violation_diagnostics(report: &ParityEvidenceReport) -> Vec<String> {
    report
        .violations
        .iter()
        .map(|violation| {
            format!(
                "bead_id={} kind={} detail={}",
                violation.bead_id, violation.kind, violation.detail
            )
        })
        .collect()
}

fn unix_time_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    use crate::e2e_traceability::{
        ConcurrencyMode, InvocationContract, ScriptEntry, ScriptKind, StorageMode,
        TraceabilityMatrix,
    };
    use crate::parity_taxonomy::FeatureCategory;
    use crate::unit_matrix::{BucketCoverage, FailureDiagnostics, UnitMatrix, UnitTestEntry};

    fn minimal_unit_matrix(bead_id: &str) -> UnitMatrix {
        UnitMatrix {
            schema_version: "1.0.0".to_owned(),
            bead_id: "bd-1dp9.7.1".to_owned(),
            root_seed: 42,
            tests: vec![UnitTestEntry {
                test_id: "UT-PARITY-001".to_owned(),
                category: FeatureCategory::SqlGrammar,
                crate_name: "fsqlite-harness".to_owned(),
                module_path: "parity_evidence_matrix::tests".to_owned(),
                description: "synthetic unit evidence".to_owned(),
                invariants: vec!["matrix_is_complete".to_owned()],
                seed: 9_001,
                property_based: false,
                failure_diagnostics: FailureDiagnostics {
                    dump_targets: vec!["rows".to_owned()],
                    log_spans: vec!["parity.evidence".to_owned()],
                    related_beads: vec![bead_id.to_owned()],
                },
            }],
            coverage: vec![BucketCoverage {
                category: FeatureCategory::SqlGrammar,
                test_count: 1,
                invariant_count: 1,
                property_test_count: 0,
                contributing_crates: vec!["fsqlite-harness".to_owned()],
                missing_coverage: Vec::new(),
                fill_pct: 1.0,
            }],
        }
    }

    fn minimal_traceability(
        bead_id: &str,
        script_path: &str,
        schema_version: &str,
    ) -> TraceabilityMatrix {
        TraceabilityMatrix {
            schema_version: "1.0.0".to_owned(),
            bead_id: "bd-mblr.4.5.1".to_owned(),
            scripts: vec![ScriptEntry {
                path: script_path.to_owned(),
                kind: ScriptKind::ShellUtility,
                bead_id: Some(bead_id.to_owned()),
                description: "synthetic e2e evidence".to_owned(),
                invocation: InvocationContract {
                    command: "bash scripts/synthetic.sh".to_owned(),
                    env_vars: Vec::new(),
                    json_output: true,
                    timeout_secs: Some(60),
                },
                scenario_ids: vec!["INFRA-9001".to_owned()],
                storage_modes: vec![StorageMode::InMemory],
                concurrency_modes: vec![ConcurrencyMode::Sequential],
                artifact_paths: vec!["artifacts/synthetic.json".to_owned()],
                log_schema_version: Some(schema_version.to_owned()),
            }],
            gaps: Vec::new(),
        }
    }

    #[test]
    fn load_parity_closure_bead_ids_filters_expected_rows() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let jsonl_path = temp_dir.path().join("issues.jsonl");
        let payload = [
            r#"{"id":"bd-1dp9.7.5","issue_type":"task"}"#,
            r#"{"id":"bd-1dp9.9","issue_type":"feature"}"#,
            r#"{"id":"bd-1dp9","issue_type":"epic"}"#,
            r#"{"id":"bd-xyz.1","issue_type":"task"}"#,
        ]
        .join("\n");
        std::fs::write(&jsonl_path, payload).expect("write jsonl");

        let bead_ids = load_parity_closure_bead_ids(&jsonl_path).expect("load bead ids");
        assert_eq!(
            bead_ids,
            vec!["bd-1dp9.7.5".to_owned(), "bd-1dp9.9".to_owned()]
        );
    }

    #[test]
    fn build_rows_collects_unit_e2e_and_log_refs() {
        let required = vec!["bd-1dp9.7.5".to_owned()];
        let unit_matrix = minimal_unit_matrix("bd-1dp9.7.5");
        let traceability = minimal_traceability(
            "bd-1dp9.7.5",
            "scripts/verify_parity_evidence_matrix.sh",
            "1.0.0",
        );

        let rows = build_parity_evidence_rows(&required, &unit_matrix, &traceability);
        assert_eq!(rows.len(), 1);

        let row = &rows[0];
        assert_eq!(row.bead_id, "bd-1dp9.7.5");
        assert_eq!(row.unit_test_ids, vec!["UT-PARITY-001".to_owned()]);
        assert_eq!(
            row.e2e_script_paths,
            vec!["scripts/verify_parity_evidence_matrix.sh".to_owned()]
        );
        assert_eq!(
            row.log_schema_refs,
            vec!["scripts/verify_parity_evidence_matrix.sh@1.0.0".to_owned()]
        );
    }

    #[test]
    fn validate_rows_reports_missing_and_invalid_references() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let row = ParityEvidenceRow {
            bead_id: "bd-1dp9.7.5".to_owned(),
            unit_test_ids: Vec::new(),
            e2e_script_paths: vec!["scripts/missing.sh".to_owned()],
            log_schema_refs: vec!["scripts/other.sh@invalid".to_owned()],
        };

        let violations = validate_parity_evidence_rows(&[row], temp_dir.path());

        assert!(
            violations
                .iter()
                .any(|violation| violation.kind == EvidenceViolationKind::MissingUnitEvidence)
        );
        assert!(
            violations
                .iter()
                .any(|violation| violation.kind == EvidenceViolationKind::InvalidE2eReference)
        );
        assert!(
            violations
                .iter()
                .any(|violation| violation.kind == EvidenceViolationKind::InvalidLogReference)
        );
    }

    #[test]
    fn semver_parser_accepts_three_numeric_segments_only() {
        assert!(is_semver("1.0.0"));
        assert!(is_semver("0.12.7"));
        assert!(!is_semver("1.0"));
        assert!(!is_semver("1.0.0-beta"));
        assert!(!is_semver("v1.0.0"));
    }

    #[test]
    fn build_report_marks_failure_when_violations_present() {
        let required = vec!["bd-1dp9.7.5".to_owned()];
        let unit_matrix = UnitMatrix {
            schema_version: "1.0.0".to_owned(),
            bead_id: "bd-1dp9.7.1".to_owned(),
            root_seed: 1,
            tests: Vec::new(),
            coverage: Vec::new(),
        };
        let traceability = TraceabilityMatrix {
            schema_version: "1.0.0".to_owned(),
            bead_id: "bd-mblr.4.5.1".to_owned(),
            scripts: Vec::new(),
            gaps: Vec::new(),
        };

        let report =
            build_parity_evidence_report(Path::new("."), &required, &unit_matrix, &traceability);

        assert_eq!(report.summary.required_bead_count, 1);
        assert!(!report.summary.overall_pass);
        assert_eq!(report.summary.violation_count, report.violations.len());
        assert!(!render_violation_diagnostics(&report).is_empty());
    }

    #[test]
    fn jsonl_parser_rejects_invalid_line_payload() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let jsonl_path = temp_dir.path().join("issues.jsonl");
        std::fs::write(&jsonl_path, "{not-valid-json}\n").expect("write jsonl");

        let error = load_parity_closure_bead_ids(&jsonl_path).expect_err("expected parser failure");
        assert!(error.contains("issues_jsonl_parse_failed"));
    }

    #[test]
    fn builder_handles_unknown_beads_with_empty_refs() {
        let required = vec!["bd-1dp9.7.5".to_owned(), "bd-1dp9.7.999".to_owned()];
        let unit_matrix = minimal_unit_matrix("bd-1dp9.7.5");
        let traceability = minimal_traceability(
            "bd-1dp9.7.5",
            "scripts/verify_parity_evidence_matrix.sh",
            "1.0.0",
        );

        let rows = build_parity_evidence_rows(&required, &unit_matrix, &traceability);
        let rows_by_bead: BTreeMap<_, _> = rows
            .into_iter()
            .map(|row| (row.bead_id.clone(), row))
            .collect();

        let known = rows_by_bead
            .get("bd-1dp9.7.5")
            .expect("known bead row should exist");
        assert!(!known.unit_test_ids.is_empty());

        let unknown = rows_by_bead
            .get("bd-1dp9.7.999")
            .expect("unknown bead row should exist");
        assert!(unknown.unit_test_ids.is_empty());
        assert!(unknown.e2e_script_paths.is_empty());
        assert!(unknown.log_schema_refs.is_empty());
    }
}
