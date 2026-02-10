use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use proptest::prelude::proptest;
use proptest::test_runner::TestCaseError;
use serde::Deserialize;

const BEAD_ID: &str = "bd-3kp.3";
const ISSUES_JSONL: &str = ".beads/issues.jsonl";
const LOG_STANDARD_REF: &str = "bd-1fpm";
const REQUIRED_SCRIPT_MARKER: &str = "e2e/future_work_report.sh";
const UNIT_TEST_IDS: [&str; 5] = [
    "test_bd_3kp_3_unit_compliance_gate",
    "prop_bd_3kp_3_structure_compliance",
    "test_future_work_entry_schema",
    "test_future_work_dependency_bead_ids_resolve",
    "test_future_work_not_startable_before_prereqs",
];
const E2E_TEST_IDS: [&str; 3] = [
    "e2e_future_work_report_generation",
    "e2e_future_work_prereq_gate",
    "test_e2e_bd_3kp_3_compliance",
];
const LOG_LEVEL_MARKERS: [&str; 4] = ["DEBUG", "INFO", "WARN", "ERROR"];
const REQUIRED_TOKENS: [&str; 13] = [
    "test_bd_3kp_3_unit_compliance_gate",
    "prop_bd_3kp_3_structure_compliance",
    "test_future_work_entry_schema",
    "test_future_work_dependency_bead_ids_resolve",
    "test_future_work_not_startable_before_prereqs",
    "e2e_future_work_report_generation",
    "e2e_future_work_prereq_gate",
    "test_e2e_bd_3kp_3_compliance",
    "DEBUG",
    "INFO",
    "WARN",
    "ERROR",
    "bd-1fpm",
];
const EXPECTED_SECTION_IDS: [&str; 9] = [
    "§21.2", "§21.3", "§21.4", "§21.5", "§21.6", "§21.7", "§21.8", "§21.9", "§21.10",
];

#[derive(Debug, PartialEq, Eq)]
#[allow(clippy::struct_field_names)]
struct ComplianceEvaluation {
    missing_unit_ids: Vec<&'static str>,
    missing_e2e_ids: Vec<&'static str>,
    missing_log_levels: Vec<&'static str>,
    missing_log_standard_ref: bool,
}

impl ComplianceEvaluation {
    fn is_compliant(&self) -> bool {
        self.missing_unit_ids.is_empty()
            && self.missing_e2e_ids.is_empty()
            && self.missing_log_levels.is_empty()
            && !self.missing_log_standard_ref
    }
}

#[derive(Debug, Clone)]
struct FutureSection {
    id: String,
    body: String,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(clippy::struct_field_names)]
struct IssueDependency {
    depends_on_id: String,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(clippy::struct_field_names)]
struct IssueComment {
    text: String,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(clippy::struct_field_names)]
struct IssueRecord {
    id: String,
    #[serde(default)]
    title: String,
    #[serde(default)]
    status: String,
    #[serde(default)]
    description: String,
    #[serde(default)]
    dependencies: Vec<IssueDependency>,
    #[serde(default)]
    comments: Vec<IssueComment>,
}

#[derive(Debug, Clone)]
#[allow(clippy::struct_field_names)]
struct StartGateFailure {
    section_id: String,
    blockers: Vec<String>,
}

fn workspace_root() -> Result<PathBuf, String> {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .map_err(|error| format!("workspace_root_canonicalize_failed: {error}"))
}

fn load_all_issues() -> Result<Vec<IssueRecord>, String> {
    let issues_path = workspace_root()?.join(ISSUES_JSONL);
    let raw = fs::read_to_string(&issues_path).map_err(|error| {
        format!(
            "issues_jsonl_read_failed path={} error={error}",
            issues_path.display()
        )
    })?;

    raw.lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            serde_json::from_str::<IssueRecord>(line)
                .map_err(|error| format!("issues_jsonl_parse_failed error={error} line={line}"))
        })
        .collect::<Result<Vec<_>, _>>()
}

fn load_issue_record(issue_id: &str) -> Result<IssueRecord, String> {
    load_all_issues()?
        .into_iter()
        .find(|issue| issue.id == issue_id)
        .ok_or_else(|| format!("bead_id={issue_id} not_found_in={ISSUES_JSONL}"))
}

fn canonical_description(issue: &IssueRecord) -> String {
    let mut canonical = issue.description.clone();
    for comment in &issue.comments {
        canonical.push_str("\n\n");
        canonical.push_str(&comment.text);
    }
    canonical
}

fn tokenize(text: &str) -> impl Iterator<Item = &str> {
    text.split(|ch: char| !(ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' || ch == '.'))
}

fn contains_token(text: &str, needle: &str) -> bool {
    tokenize(text).any(|token| token == needle)
}

fn evaluate_description(description: &str) -> ComplianceEvaluation {
    let missing_unit_ids = UNIT_TEST_IDS
        .into_iter()
        .filter(|id| !contains_token(description, id))
        .collect::<Vec<_>>();

    let missing_e2e_ids = E2E_TEST_IDS
        .into_iter()
        .filter(|id| !contains_token(description, id))
        .collect::<Vec<_>>();

    let missing_log_levels = LOG_LEVEL_MARKERS
        .into_iter()
        .filter(|level| !description.contains(level))
        .collect::<Vec<_>>();

    ComplianceEvaluation {
        missing_unit_ids,
        missing_e2e_ids,
        missing_log_levels,
        missing_log_standard_ref: !description.contains(LOG_STANDARD_REF),
    }
}

fn parse_future_section_id(line: &str) -> Option<String> {
    let trimmed = line.trim_start();
    let rest = trimmed.strip_prefix("§21.")?;
    let digits = rest
        .chars()
        .take_while(char::is_ascii_digit)
        .collect::<String>();
    if digits.is_empty() {
        return None;
    }
    let value = digits.parse::<u8>().ok()?;
    if !(2..=10).contains(&value) {
        return None;
    }
    Some(format!("§21.{value}"))
}

fn parse_future_sections(description: &str) -> Vec<FutureSection> {
    let mut sections = Vec::new();
    let mut current_id: Option<String> = None;
    let mut current_body = String::new();

    for line in description.lines() {
        if let Some(section_id) = parse_future_section_id(line) {
            if let Some(previous_id) = current_id.take() {
                sections.push(FutureSection {
                    id: previous_id,
                    body: current_body.trim().to_owned(),
                });
                current_body.clear();
            }
            current_id = Some(section_id);
            current_body.push_str(line);
            current_body.push('\n');
            continue;
        }

        if current_id.is_some() {
            current_body.push_str(line);
            current_body.push('\n');
        }
    }

    if let Some(last_id) = current_id {
        sections.push(FutureSection {
            id: last_id,
            body: current_body.trim().to_owned(),
        });
    }

    sections
}

fn check_future_work_entry_schema(description: &str) -> Result<(), String> {
    let sections = parse_future_sections(description);
    let ids = sections
        .iter()
        .map(|section| section.id.as_str())
        .collect::<Vec<_>>();

    if ids != EXPECTED_SECTION_IDS {
        return Err(format!(
            "bead_id={BEAD_ID} case=future_work_sections_mismatch expected={EXPECTED_SECTION_IDS:?} actual={ids:?}"
        ));
    }

    let missing_schema_fields = sections
        .iter()
        .filter_map(|section| {
            let has_scope = section.body.contains("What it is:");
            let has_motivation = section.body.contains("Why it's deferred:");
            let has_non_goals = section.body.contains("Why it's deferred:");
            let has_dependencies = section.body.contains("Dependencies on current work:");
            let has_prerequisites = section
                .body
                .contains("What needs to be true before starting:");
            let has_proof_obligations = section
                .body
                .contains("Key implementation details from spec:")
                || section.body.contains("Key implementation notes from spec:");

            (!has_scope
                || !has_motivation
                || !has_non_goals
                || !has_dependencies
                || !has_prerequisites
                || !has_proof_obligations)
                .then_some(section.id.clone())
        })
        .collect::<Vec<_>>();

    if !missing_schema_fields.is_empty() {
        return Err(format!(
            "bead_id={BEAD_ID} case=future_work_schema_missing section_ids={missing_schema_fields:?}"
        ));
    }

    Ok(())
}

fn extract_bead_ids(text: &str) -> BTreeSet<String> {
    tokenize(text)
        .filter(|token| token.starts_with("bd-"))
        .map(str::to_owned)
        .collect::<BTreeSet<_>>()
}

fn check_future_work_dependency_bead_ids_resolve(
    issue: &IssueRecord,
    description: &str,
) -> Result<(), String> {
    let all_ids = load_all_issues()?
        .into_iter()
        .map(|entry| entry.id)
        .collect::<BTreeSet<_>>();

    let mut referenced_ids = extract_bead_ids(description);
    for dependency in &issue.dependencies {
        referenced_ids.insert(dependency.depends_on_id.clone());
    }

    let unresolved = referenced_ids
        .into_iter()
        .filter(|id| !all_ids.contains(id))
        .collect::<Vec<_>>();

    if !unresolved.is_empty() {
        return Err(format!(
            "bead_id={BEAD_ID} case=dependency_bead_ids_unresolved ids={unresolved:?}"
        ));
    }

    Ok(())
}

fn parse_phase_numbers(text: &str) -> BTreeSet<u8> {
    let tokens = text.split_whitespace().collect::<Vec<_>>();
    let mut phases = BTreeSet::new();

    for pair in tokens.windows(2) {
        if pair[0] != "Phase" {
            continue;
        }
        let digits = pair[1]
            .chars()
            .take_while(char::is_ascii_digit)
            .collect::<String>();
        if let Ok(phase) = digits.parse::<u8>() {
            phases.insert(phase);
        }
    }

    phases
}

fn check_future_work_not_startable_before_prereqs(
    issue: &IssueRecord,
    description: &str,
) -> Result<Vec<StartGateFailure>, String> {
    let sections = parse_future_sections(description);
    let issues = load_all_issues()?;
    let mut failures = Vec::new();

    for section in sections {
        let phases = parse_phase_numbers(&section.body);
        if phases.is_empty() {
            continue;
        }

        let mut blockers = Vec::new();
        for phase in phases {
            let marker = format!("§16 Phase {phase}");
            let matching = issues
                .iter()
                .filter(|candidate| candidate.title.contains(&marker))
                .collect::<Vec<_>>();

            if matching.is_empty() {
                blockers.push(format!("phase_{phase}:no_phase_bead_found"));
                continue;
            }

            let open = matching
                .iter()
                .filter(|candidate| candidate.status != "closed")
                .map(|candidate| format!("phase_{phase}:{}:{}", candidate.id, candidate.status))
                .collect::<Vec<_>>();

            blockers.extend(open);
        }

        if !blockers.is_empty() {
            failures.push(StartGateFailure {
                section_id: section.id,
                blockers,
            });
        }
    }

    if failures.is_empty() {
        return Err(format!(
            "bead_id={BEAD_ID} case=start_gate_expected_blockers issue_status={}",
            issue.status
        ));
    }

    Ok(failures)
}

fn synthetic_compliant_description() -> String {
    let mut description = String::from("## Unit Test Requirements\n");
    for id in UNIT_TEST_IDS {
        description.push_str("- ");
        description.push_str(id);
        description.push('\n');
    }
    description.push('\n');

    description.push_str("## E2E Test\n");
    for id in E2E_TEST_IDS {
        description.push_str("- ");
        description.push_str(id);
        description.push('\n');
    }
    description.push('\n');

    description.push_str("## Logging Requirements\n");
    description.push_str("- DEBUG: stage-level progress\n");
    description.push_str("- INFO: summary counters\n");
    description.push_str("- WARN: degraded-mode conditions\n");
    description.push_str("- ERROR: terminal diagnostics\n");
    description.push_str("- Reference: bd-1fpm\n");

    description
}

#[test]
fn test_future_work_entry_schema() -> Result<(), String> {
    let issue = load_issue_record(BEAD_ID)?;
    check_future_work_entry_schema(&canonical_description(&issue))
}

#[test]
fn test_future_work_dependency_bead_ids_resolve() -> Result<(), String> {
    let issue = load_issue_record(BEAD_ID)?;
    check_future_work_dependency_bead_ids_resolve(&issue, &canonical_description(&issue))
}

#[test]
fn test_future_work_not_startable_before_prereqs() -> Result<(), String> {
    let issue = load_issue_record(BEAD_ID)?;
    let failures =
        check_future_work_not_startable_before_prereqs(&issue, &canonical_description(&issue))?;

    let starts_without_blockers = failures
        .iter()
        .filter(|failure| failure.blockers.is_empty())
        .map(|failure| failure.section_id.clone())
        .collect::<Vec<_>>();

    if !starts_without_blockers.is_empty() {
        return Err(format!(
            "bead_id={BEAD_ID} case=start_gate_unexpectedly_passed sections={starts_without_blockers:?}"
        ));
    }

    Ok(())
}

#[test]
fn test_bd_3kp_3_unit_compliance_gate() -> Result<(), String> {
    let issue = load_issue_record(BEAD_ID)?;
    let description = canonical_description(&issue);
    let evaluation = evaluate_description(&description);

    if !evaluation.missing_unit_ids.is_empty() {
        return Err(format!(
            "bead_id={BEAD_ID} case=unit_ids_missing missing={:?}",
            evaluation.missing_unit_ids
        ));
    }
    if !evaluation.missing_e2e_ids.is_empty() {
        return Err(format!(
            "bead_id={BEAD_ID} case=e2e_ids_missing missing={:?}",
            evaluation.missing_e2e_ids
        ));
    }
    if !evaluation.missing_log_levels.is_empty() {
        return Err(format!(
            "bead_id={BEAD_ID} case=logging_levels_missing missing={:?}",
            evaluation.missing_log_levels
        ));
    }
    if evaluation.missing_log_standard_ref {
        return Err(format!(
            "bead_id={BEAD_ID} case=logging_standard_missing expected_ref={LOG_STANDARD_REF}"
        ));
    }

    check_future_work_entry_schema(&description)?;
    check_future_work_dependency_bead_ids_resolve(&issue, &description)?;
    let _ = check_future_work_not_startable_before_prereqs(&issue, &description)?;

    Ok(())
}

proptest! {
    #[test]
    fn prop_bd_3kp_3_structure_compliance(missing_index in 0usize..REQUIRED_TOKENS.len()) {
        let mut synthetic = synthetic_compliant_description();
        synthetic = synthetic.replacen(REQUIRED_TOKENS[missing_index], "", 1);
        let evaluation = evaluate_description(&synthetic);

        if evaluation.is_compliant() {
            return Err(TestCaseError::fail(format!(
                "bead_id={} case=structure_compliance expected_non_compliant missing_index={} marker={}",
                BEAD_ID,
                missing_index,
                REQUIRED_TOKENS[missing_index],
            )));
        }
    }
}

#[test]
#[allow(clippy::too_many_lines)]
fn test_e2e_bd_3kp_3_compliance() -> Result<(), String> {
    let issue = load_issue_record(BEAD_ID)?;
    let description = canonical_description(&issue);
    let evaluation = evaluate_description(&description);

    eprintln!("DEBUG bead_id={BEAD_ID} case=e2e_start expected_script={REQUIRED_SCRIPT_MARKER}");

    let schema_result = check_future_work_entry_schema(&description);
    let dependency_result = check_future_work_dependency_bead_ids_resolve(&issue, &description);
    let start_gate_result = check_future_work_not_startable_before_prereqs(&issue, &description);

    eprintln!(
        "INFO bead_id={BEAD_ID} case=e2e_summary missing_unit_ids={} missing_e2e_ids={} missing_log_levels={} missing_log_standard_ref={}",
        evaluation.missing_unit_ids.len(),
        evaluation.missing_e2e_ids.len(),
        evaluation.missing_log_levels.len(),
        evaluation.missing_log_standard_ref,
    );

    for id in &evaluation.missing_unit_ids {
        eprintln!("WARN bead_id={BEAD_ID} case=missing_unit_id id={id}");
    }
    for id in &evaluation.missing_e2e_ids {
        eprintln!("WARN bead_id={BEAD_ID} case=missing_e2e_id id={id}");
    }
    for level in &evaluation.missing_log_levels {
        eprintln!("WARN bead_id={BEAD_ID} case=missing_log_level level={level}");
    }
    if evaluation.missing_log_standard_ref {
        eprintln!(
            "ERROR bead_id={BEAD_ID} case=missing_log_standard_ref expected={LOG_STANDARD_REF}"
        );
    }

    if let Err(error) = &schema_result {
        eprintln!("WARN bead_id={BEAD_ID} case=schema_validation_failure detail={error}");
    }
    if let Err(error) = &dependency_result {
        eprintln!("WARN bead_id={BEAD_ID} case=dependency_validation_failure detail={error}");
    }
    if let Err(error) = &start_gate_result {
        eprintln!("WARN bead_id={BEAD_ID} case=start_gate_validation_failure detail={error}");
    }

    let root = workspace_root()?;
    let script = root.join(REQUIRED_SCRIPT_MARKER);
    if !script.is_file() {
        return Err(format!(
            "bead_id={BEAD_ID} case=e2e_script_missing path={}",
            script.display()
        ));
    }

    let report_output = Command::new("bash")
        .arg(script.as_os_str())
        .current_dir(&root)
        .output()
        .map_err(|error| {
            format!(
                "bead_id={BEAD_ID} case=e2e_script_exec_failed path={} error={error}",
                script.display()
            )
        })?;

    let report_stdout = String::from_utf8_lossy(&report_output.stdout);
    let report_stderr = String::from_utf8_lossy(&report_output.stderr);
    if !report_stderr.trim().is_empty() {
        eprintln!("WARN bead_id={BEAD_ID} case=e2e_script_stderr text={report_stderr}");
    }
    if !report_output.status.success() {
        return Err(format!(
            "bead_id={BEAD_ID} case=e2e_script_nonzero_exit status={} stdout={report_stdout} stderr={report_stderr}",
            report_output.status
        ));
    }
    if !report_stdout.contains("case=summary") || !report_stdout.contains("trace_id=") {
        return Err(format!(
            "bead_id={BEAD_ID} case=e2e_script_missing_summary_or_trace output={report_stdout}"
        ));
    }

    let prereq_gate_output = Command::new("bash")
        .arg(script.as_os_str())
        .current_dir(&root)
        .env("FUTURE_WORK_PROMOTION_TARGET", "§21.2")
        .output()
        .map_err(|error| {
            format!(
                "bead_id={BEAD_ID} case=e2e_prereq_gate_exec_failed path={} error={error}",
                script.display()
            )
        })?;

    let prereq_stdout = String::from_utf8_lossy(&prereq_gate_output.stdout);
    if prereq_gate_output.status.success() {
        // Prerequisites are met: promotion is allowed.  Verify the script
        // emitted the expected "promotion_allowed" marker.
        if !prereq_stdout.contains("case=promotion_allowed") {
            return Err(format!(
                "bead_id={BEAD_ID} case=e2e_prereq_gate_success_missing_marker output={prereq_stdout}"
            ));
        }
    } else {
        // Prerequisites not yet met: promotion is blocked.
        if !prereq_stdout.contains("case=promotion_blocked") {
            return Err(format!(
                "bead_id={BEAD_ID} case=e2e_prereq_gate_missing_block_reason output={prereq_stdout}"
            ));
        }
    }

    if !evaluation.is_compliant()
        || schema_result.is_err()
        || dependency_result.is_err()
        || start_gate_result.is_err()
    {
        return Err(format!(
            "bead_id={BEAD_ID} case=e2e_compliance_failure evaluation={evaluation:?} schema_result={schema_result:?} dependency_result={dependency_result:?} start_gate_result={start_gate_result:?}"
        ));
    }

    Ok(())
}

#[test]
fn test_e2e_bd_3kp_3() -> Result<(), String> {
    test_e2e_bd_3kp_3_compliance()
}
