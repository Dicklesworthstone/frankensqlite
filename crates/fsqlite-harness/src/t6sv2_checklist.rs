//! Observability-program checklist and evidence validator (`bd-t6sv2.16`).
//!
//! This module builds a living checklist for `bd-t6sv2.*` child beads and
//! validates three required evidence lanes per bead:
//! - unit/property test linkage,
//! - deterministic E2E script linkage,
//! - structured log-schema linkage.
//!
//! The report is deterministic and machine-readable so CI and operators can
//! triage missing or stale evidence quickly.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::e2e_traceability::TraceabilityMatrix;
use crate::unit_matrix::UnitMatrix;

/// Owning bead for this checklist gate.
pub const BEAD_ID: &str = "bd-t6sv2.16";
/// JSON schema version for serialized reports.
pub const CHECKLIST_SCHEMA_VERSION: u32 = 1;

/// Missing-evidence categories tracked per bead.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum ChecklistMissingKind {
    UnitEvidence,
    E2eEvidence,
    LogEvidence,
}

impl ChecklistMissingKind {
    #[must_use]
    pub const fn remediation(self) -> &'static str {
        match self {
            Self::UnitEvidence => {
                "Add deterministic unit/property tests and link bead ID in failure diagnostics."
            }
            Self::E2eEvidence => {
                "Add at least one deterministic E2E script mapping with replay-capable invocation."
            }
            Self::LogEvidence => {
                "Add structured log schema reference (`script@semver`) for the linked E2E script."
            }
        }
    }
}

/// Validation violation categories.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum ChecklistViolationKind {
    MissingUnitEvidence,
    MissingE2eEvidence,
    MissingLogEvidence,
    InvalidE2eReference,
    InvalidLogReference,
}

impl fmt::Display for ChecklistViolationKind {
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

/// One bead row in the checklist report.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct T6sv2ChecklistRow {
    pub bead_id: String,
    pub title: String,
    pub status: String,
    pub priority: i64,
    pub owner: String,
    pub unit_test_ids: Vec<String>,
    pub e2e_script_paths: Vec<String>,
    pub log_schema_refs: Vec<String>,
    pub missing: Vec<ChecklistMissingKind>,
    pub stale_links: Vec<String>,
    pub triage_command: String,
    pub remediation: Vec<String>,
}

/// One validation violation with owner attribution.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct T6sv2ChecklistViolation {
    pub bead_id: String,
    pub owner: String,
    pub kind: ChecklistViolationKind,
    pub detail: String,
    pub triage_command: String,
}

/// Checklist summary counters.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct T6sv2ChecklistSummary {
    pub child_count: usize,
    pub open_count: usize,
    pub in_progress_count: usize,
    pub closed_count: usize,
    pub other_status_count: usize,
    pub missing_unit_count: usize,
    pub missing_e2e_count: usize,
    pub missing_log_count: usize,
    pub stale_link_count: usize,
    pub violation_count: usize,
    pub overall_pass: bool,
}

/// Full serialized checklist report.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct T6sv2ChecklistReport {
    pub schema_version: u32,
    pub bead_id: String,
    pub generated_unix_ms: u128,
    pub workspace_root: String,
    pub rows: Vec<T6sv2ChecklistRow>,
    pub violations: Vec<T6sv2ChecklistViolation>,
    pub summary: T6sv2ChecklistSummary,
}

impl T6sv2ChecklistReport {
    /// Render a deterministic Markdown summary for operator triage.
    #[must_use]
    pub fn render_markdown(&self) -> String {
        let mut out = String::new();
        out.push_str("# t6sv2 Evidence Checklist Report\n\n");
        out.push_str(&format!("- bead_id: `{}`\n", self.bead_id));
        out.push_str(&format!("- overall_pass: `{}`\n", self.summary.overall_pass));
        out.push_str(&format!("- child_count: `{}`\n", self.summary.child_count));
        out.push_str(&format!("- open_count: `{}`\n", self.summary.open_count));
        out.push_str(&format!(
            "- in_progress_count: `{}`\n",
            self.summary.in_progress_count
        ));
        out.push_str(&format!("- closed_count: `{}`\n", self.summary.closed_count));
        out.push_str(&format!(
            "- missing_unit_count: `{}`\n",
            self.summary.missing_unit_count
        ));
        out.push_str(&format!(
            "- missing_e2e_count: `{}`\n",
            self.summary.missing_e2e_count
        ));
        out.push_str(&format!(
            "- missing_log_count: `{}`\n",
            self.summary.missing_log_count
        ));
        out.push_str(&format!(
            "- stale_link_count: `{}`\n",
            self.summary.stale_link_count
        ));
        out.push_str(&format!(
            "- violation_count: `{}`\n\n",
            self.summary.violation_count
        ));

        out.push_str("## Rows\n\n");
        out.push_str("| Bead | Status | Owner | Missing | Stale Links |\n");
        out.push_str("|---|---|---|---|---:|\n");
        for row in &self.rows {
            let missing = if row.missing.is_empty() {
                "none".to_owned()
            } else {
                row.missing
                    .iter()
                    .map(|kind| format!("{kind:?}"))
                    .collect::<Vec<_>>()
                    .join(",")
            };
            out.push_str(&format!(
                "| `{}` | {} | {} | {} | {} |\n",
                row.bead_id,
                row.status,
                row.owner,
                missing,
                row.stale_links.len()
            ));
        }

        if self.violations.is_empty() {
            out.push_str("\n## Violations\n\nNone.\n");
        } else {
            out.push_str("\n## Violations\n\n");
            for violation in &self.violations {
                out.push_str(&format!(
                    "- `{}` owner={} kind={} detail={} cmd=`{}`\n",
                    violation.bead_id,
                    violation.owner,
                    violation.kind,
                    violation.detail,
                    violation.triage_command
                ));
            }
        }

        out
    }
}

#[derive(Debug, Clone)]
struct ChecklistIssue {
    id: String,
    title: String,
    status: String,
    priority: i64,
    owner: String,
}

#[derive(Debug, Deserialize)]
struct RawIssue {
    #[serde(default)]
    id: String,
    #[serde(default)]
    title: String,
    #[serde(default)]
    status: String,
    #[serde(default, rename = "issue_type")]
    issue_type: String,
    #[serde(default)]
    priority: Option<i64>,
    #[serde(default)]
    owner: Option<String>,
    #[serde(default)]
    assignee: Option<String>,
    #[serde(default)]
    created_by: String,
}

/// Generate checklist report from workspace canonical inventories.
///
/// # Errors
///
/// Returns an error when required files cannot be read or parsed.
pub fn generate_workspace_t6sv2_checklist_report(
    workspace_root: &Path,
) -> Result<T6sv2ChecklistReport, String> {
    let issues_path = workspace_root.join(".beads/issues.jsonl");
    let unit_matrix = crate::unit_matrix::build_canonical_matrix();
    let traceability = crate::e2e_traceability::build_canonical_inventory();
    generate_t6sv2_checklist_report(
        workspace_root,
        &issues_path,
        &unit_matrix,
        &traceability,
        None,
    )
}

/// Generate checklist report from explicit issues path and inventories.
///
/// # Errors
///
/// Returns an error when issues cannot be loaded or parsed.
pub fn generate_t6sv2_checklist_report(
    workspace_root: &Path,
    issues_path: &Path,
    unit_matrix: &UnitMatrix,
    traceability: &TraceabilityMatrix,
    generated_unix_ms: Option<u128>,
) -> Result<T6sv2ChecklistReport, String> {
    let issues = load_t6sv2_children(issues_path)?;
    let rows = build_rows(workspace_root, &issues, unit_matrix, traceability);
    let violations = collect_violations(&rows);
    let summary = build_summary(&rows, &violations);

    Ok(T6sv2ChecklistReport {
        schema_version: CHECKLIST_SCHEMA_VERSION,
        bead_id: BEAD_ID.to_owned(),
        generated_unix_ms: generated_unix_ms.unwrap_or_else(default_generated_unix_ms),
        workspace_root: workspace_root.display().to_string(),
        rows,
        violations,
        summary,
    })
}

/// Render deterministic single-line diagnostics.
#[must_use]
pub fn render_violation_diagnostics(report: &T6sv2ChecklistReport) -> Vec<String> {
    report
        .violations
        .iter()
        .map(|violation| {
            format!(
                "bead_id={} owner={} kind={} detail={}",
                violation.bead_id, violation.owner, violation.kind, violation.detail
            )
        })
        .collect()
}

fn default_generated_unix_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis())
}

fn load_t6sv2_children(path: &Path) -> Result<Vec<ChecklistIssue>, String> {
    let payload = std::fs::read_to_string(path)
        .map_err(|error| format!("issues_read_failed path={} error={error}", path.display()))?;
    let mut issues = Vec::new();

    for (line_index, line) in payload.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let raw: RawIssue = serde_json::from_str(line).map_err(|error| {
            format!(
                "issues_parse_failed path={} line={} error={error}",
                path.display(),
                line_index.saturating_add(1)
            )
        })?;
        if !is_t6sv2_child(&raw.id) || !is_child_kind(&raw.issue_type) {
            continue;
        }
        let owner = resolve_owner(raw.owner, raw.assignee, raw.created_by);
        issues.push(ChecklistIssue {
            id: raw.id.trim().to_owned(),
            title: raw.title.trim().to_owned(),
            status: raw.status.trim().to_ascii_lowercase(),
            priority: raw.priority.unwrap_or(99),
            owner,
        });
    }

    issues.sort_by(|left, right| left.id.cmp(&right.id));
    Ok(issues)
}

fn is_t6sv2_child(issue_id: &str) -> bool {
    if !issue_id.starts_with("bd-t6sv2.") {
        return false;
    }
    let Some((_, suffix)) = issue_id.rsplit_once('.') else {
        return false;
    };
    !suffix.is_empty() && suffix.chars().all(|ch| ch.is_ascii_digit())
}

fn is_child_kind(issue_type: &str) -> bool {
    matches!(issue_type.trim().to_ascii_lowercase().as_str(), "task" | "feature" | "bug")
}

fn resolve_owner(owner: Option<String>, assignee: Option<String>, created_by: String) -> String {
    [owner, assignee, Some(created_by)]
        .into_iter()
        .flatten()
        .map(|value| value.trim().to_owned())
        .find(|value| !value.is_empty())
        .unwrap_or_else(|| String::from("unassigned"))
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
        let Some(bead_id) = &script.bead_id else {
            continue;
        };
        refs_by_bead
            .entry(bead_id.clone())
            .or_default()
            .insert(script.path.clone());
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

fn build_rows(
    workspace_root: &Path,
    issues: &[ChecklistIssue],
    unit_matrix: &UnitMatrix,
    traceability: &TraceabilityMatrix,
) -> Vec<T6sv2ChecklistRow> {
    let unit_refs = collect_unit_refs_by_bead(unit_matrix);
    let e2e_refs = collect_e2e_refs_by_bead(traceability);
    let log_refs = collect_log_refs_by_bead(traceability);

    let mut rows = Vec::with_capacity(issues.len());
    for issue in issues {
        let unit_test_ids = unit_refs
            .get(&issue.id)
            .map_or_else(Vec::new, |items| items.iter().cloned().collect());
        let e2e_script_paths = e2e_refs
            .get(&issue.id)
            .map_or_else(Vec::new, |items| items.iter().cloned().collect());
        let log_schema_refs = log_refs
            .get(&issue.id)
            .map_or_else(Vec::new, |items| items.iter().cloned().collect());

        let mut missing = Vec::new();
        if unit_test_ids.is_empty() {
            missing.push(ChecklistMissingKind::UnitEvidence);
        }
        if e2e_script_paths.is_empty() {
            missing.push(ChecklistMissingKind::E2eEvidence);
        }
        if log_schema_refs.is_empty() {
            missing.push(ChecklistMissingKind::LogEvidence);
        }

        let stale_links = validate_references(workspace_root, &e2e_script_paths, &log_schema_refs);
        let mut remediation = missing
            .iter()
            .map(|kind| kind.remediation().to_owned())
            .collect::<Vec<_>>();
        if !stale_links.is_empty() {
            remediation.push(
                "Fix stale evidence links or remove invalid references so checklist rows are actionable."
                    .to_owned(),
            );
        }

        rows.push(T6sv2ChecklistRow {
            bead_id: issue.id.clone(),
            title: issue.title.clone(),
            status: issue.status.clone(),
            priority: issue.priority,
            owner: issue.owner.clone(),
            unit_test_ids,
            e2e_script_paths,
            log_schema_refs,
            missing,
            stale_links,
            triage_command: format!("br show {} --json", issue.id),
            remediation,
        });
    }
    rows
}

fn validate_references(
    workspace_root: &Path,
    e2e_script_paths: &[String],
    log_schema_refs: &[String],
) -> Vec<String> {
    let mut stale = BTreeSet::new();

    for script_path in e2e_script_paths {
        let candidate = workspace_root.join(script_path);
        if !candidate.is_file() {
            stale.insert(format!("missing e2e script path: {script_path}"));
        }
    }

    for reference in log_schema_refs {
        let Some((script_path, version)) = reference.rsplit_once('@') else {
            stale.insert(format!("invalid log reference format: {reference}"));
            continue;
        };
        if !e2e_script_paths.iter().any(|path| path == script_path) {
            stale.insert(format!(
                "log reference script not linked as e2e evidence: {script_path}"
            ));
        }
        if !is_semver(version) {
            stale.insert(format!("invalid log schema version: {version}"));
        }
    }

    stale.into_iter().collect()
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

fn collect_violations(rows: &[T6sv2ChecklistRow]) -> Vec<T6sv2ChecklistViolation> {
    let mut violations = Vec::new();

    for row in rows {
        let triage_command = row.triage_command.clone();
        if row
            .missing
            .iter()
            .any(|kind| *kind == ChecklistMissingKind::UnitEvidence)
        {
            violations.push(T6sv2ChecklistViolation {
                bead_id: row.bead_id.clone(),
                owner: row.owner.clone(),
                kind: ChecklistViolationKind::MissingUnitEvidence,
                detail: String::from("no linked unit/property evidence"),
                triage_command: triage_command.clone(),
            });
        }
        if row
            .missing
            .iter()
            .any(|kind| *kind == ChecklistMissingKind::E2eEvidence)
        {
            violations.push(T6sv2ChecklistViolation {
                bead_id: row.bead_id.clone(),
                owner: row.owner.clone(),
                kind: ChecklistViolationKind::MissingE2eEvidence,
                detail: String::from("no linked deterministic e2e evidence"),
                triage_command: triage_command.clone(),
            });
        }
        if row
            .missing
            .iter()
            .any(|kind| *kind == ChecklistMissingKind::LogEvidence)
        {
            violations.push(T6sv2ChecklistViolation {
                bead_id: row.bead_id.clone(),
                owner: row.owner.clone(),
                kind: ChecklistViolationKind::MissingLogEvidence,
                detail: String::from("no linked structured log-schema evidence"),
                triage_command: triage_command.clone(),
            });
        }

        for detail in &row.stale_links {
            let kind = if detail.starts_with("missing e2e script path:") {
                ChecklistViolationKind::InvalidE2eReference
            } else {
                ChecklistViolationKind::InvalidLogReference
            };
            violations.push(T6sv2ChecklistViolation {
                bead_id: row.bead_id.clone(),
                owner: row.owner.clone(),
                kind,
                detail: detail.clone(),
                triage_command: triage_command.clone(),
            });
        }
    }

    violations.sort_by(|left, right| {
        left.bead_id
            .cmp(&right.bead_id)
            .then_with(|| left.kind.cmp(&right.kind))
            .then_with(|| left.detail.cmp(&right.detail))
    });
    violations
}

fn build_summary(
    rows: &[T6sv2ChecklistRow],
    violations: &[T6sv2ChecklistViolation],
) -> T6sv2ChecklistSummary {
    let mut open_count = 0_usize;
    let mut in_progress_count = 0_usize;
    let mut closed_count = 0_usize;
    let mut other_status_count = 0_usize;
    let mut missing_unit_count = 0_usize;
    let mut missing_e2e_count = 0_usize;
    let mut missing_log_count = 0_usize;
    let mut stale_link_count = 0_usize;

    for row in rows {
        match row.status.as_str() {
            "open" => open_count = open_count.saturating_add(1),
            "in_progress" => in_progress_count = in_progress_count.saturating_add(1),
            "closed" => closed_count = closed_count.saturating_add(1),
            _ => other_status_count = other_status_count.saturating_add(1),
        }
        if row
            .missing
            .iter()
            .any(|kind| *kind == ChecklistMissingKind::UnitEvidence)
        {
            missing_unit_count = missing_unit_count.saturating_add(1);
        }
        if row
            .missing
            .iter()
            .any(|kind| *kind == ChecklistMissingKind::E2eEvidence)
        {
            missing_e2e_count = missing_e2e_count.saturating_add(1);
        }
        if row
            .missing
            .iter()
            .any(|kind| *kind == ChecklistMissingKind::LogEvidence)
        {
            missing_log_count = missing_log_count.saturating_add(1);
        }
        stale_link_count = stale_link_count.saturating_add(row.stale_links.len());
    }

    T6sv2ChecklistSummary {
        child_count: rows.len(),
        open_count,
        in_progress_count,
        closed_count,
        other_status_count,
        missing_unit_count,
        missing_e2e_count,
        missing_log_count,
        stale_link_count,
        violation_count: violations.len(),
        overall_pass: violations.is_empty(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    use crate::e2e_traceability::{
        ConcurrencyMode, InvocationContract, ScriptEntry, ScriptKind, StorageMode,
    };
    use crate::parity_taxonomy::FeatureCategory;
    use crate::unit_matrix::{BucketCoverage, FailureDiagnostics, UnitTestEntry};

    fn write_jsonl(path: &Path, rows: &[serde_json::Value]) {
        let payload = rows
            .iter()
            .map(|row| serde_json::to_string(row).expect("serialize row"))
            .collect::<Vec<_>>()
            .join("\n");
        std::fs::write(path, format!("{payload}\n")).expect("write jsonl");
    }

    fn minimal_unit_matrix(bead_id: &str) -> UnitMatrix {
        UnitMatrix {
            schema_version: String::from("1.0.0"),
            bead_id: String::from("bd-test"),
            root_seed: 11,
            tests: vec![UnitTestEntry {
                test_id: String::from("UT-T6SV2-001"),
                category: FeatureCategory::SqlGrammar,
                crate_name: String::from("fsqlite-harness"),
                module_path: String::from("t6sv2_checklist::tests"),
                description: String::from("synthetic"),
                invariants: vec![String::from("synthetic_invariant")],
                seed: 42,
                property_based: false,
                failure_diagnostics: FailureDiagnostics {
                    dump_targets: vec![String::from("rows")],
                    log_spans: vec![String::from("checklist")],
                    related_beads: vec![bead_id.to_owned()],
                },
            }],
            coverage: vec![BucketCoverage {
                category: FeatureCategory::SqlGrammar,
                test_count: 1,
                invariant_count: 1,
                property_test_count: 0,
                contributing_crates: vec![String::from("fsqlite-harness")],
                missing_coverage: Vec::new(),
                fill_pct: 1.0,
            }],
        }
    }

    fn minimal_traceability(
        bead_id: &str,
        script_path: &str,
        schema_version: Option<&str>,
    ) -> TraceabilityMatrix {
        TraceabilityMatrix {
            schema_version: String::from("1.0.0"),
            bead_id: String::from("bd-test"),
            scripts: vec![ScriptEntry {
                path: script_path.to_owned(),
                kind: ScriptKind::ShellUtility,
                bead_id: Some(bead_id.to_owned()),
                description: String::from("synthetic"),
                invocation: InvocationContract {
                    command: String::from("bash scripts/test.sh"),
                    env_vars: Vec::new(),
                    json_output: true,
                    timeout_secs: Some(60),
                },
                scenario_ids: vec![String::from("OBS-100")],
                storage_modes: vec![StorageMode::InMemory],
                concurrency_modes: vec![ConcurrencyMode::Sequential],
                artifact_paths: vec![String::from("artifacts/synthetic.json")],
                log_schema_version: schema_version.map(str::to_owned),
            }],
            gaps: Vec::new(),
        }
    }

    #[test]
    fn load_children_filters_and_resolves_owner() {
        let temp = tempdir().expect("tempdir");
        let issues_path = temp.path().join("issues.jsonl");
        write_jsonl(
            &issues_path,
            &[
                serde_json::json!({"id":"bd-t6sv2","issue_type":"epic"}),
                serde_json::json!({"id":"bd-t6sv2.4","title":"A","status":"open","issue_type":"task","priority":1,"owner":"OwnerA","created_by":"alice"}),
                serde_json::json!({"id":"bd-t6sv2.5","title":"B","status":"in_progress","issue_type":"feature","priority":2,"assignee":"OwnerB","created_by":"bob"}),
                serde_json::json!({"id":"bd-t6sv2.6","title":"C","status":"closed","issue_type":"bug","priority":3,"created_by":"carol"}),
                serde_json::json!({"id":"bd-else.1","issue_type":"task"}),
            ],
        );

        let issues = load_t6sv2_children(&issues_path).expect("load children");
        assert_eq!(issues.len(), 3);
        assert_eq!(issues[0].id, "bd-t6sv2.4");
        assert_eq!(issues[0].owner, "OwnerA");
        assert_eq!(issues[1].id, "bd-t6sv2.5");
        assert_eq!(issues[1].owner, "OwnerB");
        assert_eq!(issues[2].id, "bd-t6sv2.6");
        assert_eq!(issues[2].owner, "carol");
    }

    #[test]
    fn report_rollup_counts_missing_and_statuses() {
        let temp = tempdir().expect("tempdir");
        let workspace = temp.path();
        let script_dir = workspace.join("scripts");
        std::fs::create_dir_all(&script_dir).expect("create scripts dir");
        let script_path = script_dir.join("verify_t6sv2_4.sh");
        std::fs::write(&script_path, "#!/usr/bin/env bash\n").expect("write script");

        let issues_path = workspace.join("issues.jsonl");
        write_jsonl(
            &issues_path,
            &[
                serde_json::json!({"id":"bd-t6sv2.4","title":"A","status":"open","issue_type":"task","priority":1,"owner":"ops-a","created_by":"ops-a"}),
                serde_json::json!({"id":"bd-t6sv2.5","title":"B","status":"in_progress","issue_type":"task","priority":1,"owner":"ops-b","created_by":"ops-b"}),
                serde_json::json!({"id":"bd-t6sv2.6","title":"C","status":"closed","issue_type":"task","priority":1,"owner":"ops-c","created_by":"ops-c"}),
            ],
        );

        let unit_matrix = minimal_unit_matrix("bd-t6sv2.4");
        let traceability = minimal_traceability("bd-t6sv2.4", "scripts/verify_t6sv2_4.sh", Some("1.0.0"));
        let report = generate_t6sv2_checklist_report(
            workspace,
            &issues_path,
            &unit_matrix,
            &traceability,
            Some(123),
        )
        .expect("generate report");

        assert_eq!(report.summary.child_count, 3);
        assert_eq!(report.summary.open_count, 1);
        assert_eq!(report.summary.in_progress_count, 1);
        assert_eq!(report.summary.closed_count, 1);
        assert_eq!(report.summary.missing_unit_count, 2);
        assert_eq!(report.summary.missing_e2e_count, 2);
        assert_eq!(report.summary.missing_log_count, 2);
        assert!(!report.summary.overall_pass);

        let row_four = report
            .rows
            .iter()
            .find(|row| row.bead_id == "bd-t6sv2.4")
            .expect("row for bd-t6sv2.4");
        assert!(row_four.missing.is_empty());
        assert!(row_four.stale_links.is_empty());
    }

    #[test]
    fn stale_reference_violation_has_owner_attribution() {
        let temp = tempdir().expect("tempdir");
        let workspace = temp.path();
        let issues_path = workspace.join("issues.jsonl");
        write_jsonl(
            &issues_path,
            &[serde_json::json!({
                "id":"bd-t6sv2.6",
                "title":"C",
                "status":"open",
                "issue_type":"task",
                "priority":1,
                "owner":"ops-c",
                "created_by":"ops-c"
            })],
        );

        let unit_matrix = minimal_unit_matrix("bd-t6sv2.6");
        let traceability = minimal_traceability("bd-t6sv2.6", "scripts/missing.sh", Some("bad"));
        let report = generate_t6sv2_checklist_report(
            workspace,
            &issues_path,
            &unit_matrix,
            &traceability,
            Some(99),
        )
        .expect("generate report");

        assert!(
            report
                .violations
                .iter()
                .any(|violation| violation.owner == "ops-c"
                    && violation.kind == ChecklistViolationKind::InvalidE2eReference)
        );
        assert!(
            report
                .violations
                .iter()
                .any(|violation| violation.owner == "ops-c"
                    && violation.kind == ChecklistViolationKind::InvalidLogReference)
        );
    }

    #[test]
    fn deterministic_output_for_fixed_timestamp() {
        let temp = tempdir().expect("tempdir");
        let workspace = temp.path();
        let script_dir = workspace.join("scripts");
        std::fs::create_dir_all(&script_dir).expect("create scripts dir");
        let script_path = script_dir.join("verify_t6sv2_4.sh");
        std::fs::write(&script_path, "#!/usr/bin/env bash\n").expect("write script");

        let issues_path = workspace.join("issues.jsonl");
        write_jsonl(
            &issues_path,
            &[serde_json::json!({
                "id":"bd-t6sv2.4",
                "title":"A",
                "status":"open",
                "issue_type":"task",
                "priority":1,
                "owner":"ops-a",
                "created_by":"ops-a"
            })],
        );

        let unit_matrix = minimal_unit_matrix("bd-t6sv2.4");
        let traceability = minimal_traceability("bd-t6sv2.4", "scripts/verify_t6sv2_4.sh", Some("1.0.0"));

        let report_a = generate_t6sv2_checklist_report(
            workspace,
            &issues_path,
            &unit_matrix,
            &traceability,
            Some(777),
        )
        .expect("report a");
        let report_b = generate_t6sv2_checklist_report(
            workspace,
            &issues_path,
            &unit_matrix,
            &traceability,
            Some(777),
        )
        .expect("report b");

        let json_a = serde_json::to_string(&report_a).expect("serialize a");
        let json_b = serde_json::to_string(&report_b).expect("serialize b");
        assert_eq!(json_a, json_b);
        assert!(render_violation_diagnostics(&report_a).is_empty());
    }
}
