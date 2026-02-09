use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use proptest::prelude::proptest;
use proptest::test_runner::TestCaseError;
use serde_json::Value;

const BEAD_ID: &str = "bd-3kp.2";
const ISSUES_JSONL: &str = ".beads/issues.jsonl";
const UNIT_TEST_IDS: [&str; 5] = [
    "test_bd_3kp_2_unit_compliance_gate",
    "prop_bd_3kp_2_structure_compliance",
    "test_risks_have_unique_ids",
    "test_each_high_priority_risk_has_signal",
    "test_each_risk_has_mitigation_pointer",
];
const E2E_TEST_IDS: [&str; 2] = ["test_e2e_bd_3kp_2", "test_e2e_bd_3kp_2_compliance"];
const LOG_LEVEL_MARKERS: [&str; 4] = ["DEBUG", "INFO", "WARN", "ERROR"];
const LOG_STANDARD_REF: &str = "bd-1fpm";
const REQUIRED_SCRIPT_MARKER: &str = "e2e/risk_register_report.sh";
const REQUIRED_TOKENS: [&str; 13] = [
    "test_bd_3kp_2_unit_compliance_gate",
    "prop_bd_3kp_2_structure_compliance",
    "test_risks_have_unique_ids",
    "test_each_high_priority_risk_has_signal",
    "test_each_risk_has_mitigation_pointer",
    "test_e2e_bd_3kp_2",
    "test_e2e_bd_3kp_2_compliance",
    "DEBUG",
    "INFO",
    "WARN",
    "ERROR",
    "bd-1fpm",
    "e2e/risk_register_report.sh",
];
const EXPECTED_RISK_IDS: [&str; 8] = ["R1", "R2", "R3", "R4", "R5", "R6", "R7", "R8"];
const HIGH_PRIORITY_RISK_IDS: [&str; 5] = ["R1", "R2", "R5", "R7", "R8"];

#[derive(Debug, PartialEq, Eq)]
#[allow(clippy::struct_field_names)]
struct ComplianceEvaluation {
    missing_unit_ids: Vec<&'static str>,
    missing_e2e_ids: Vec<&'static str>,
    missing_log_levels: Vec<&'static str>,
    missing_log_standard_ref: bool,
    missing_script_marker: bool,
}

impl ComplianceEvaluation {
    fn is_compliant(&self) -> bool {
        self.missing_unit_ids.is_empty()
            && self.missing_e2e_ids.is_empty()
            && self.missing_log_levels.is_empty()
            && !self.missing_log_standard_ref
            && !self.missing_script_marker
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RiskSection {
    id: String,
    body: String,
}

fn workspace_root() -> Result<PathBuf, String> {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .map_err(|error| format!("workspace_root_canonicalize_failed: {error}"))
}

fn load_issue_description(issue_id: &str) -> Result<String, String> {
    let issues_path = workspace_root()?.join(ISSUES_JSONL);
    let raw = fs::read_to_string(&issues_path).map_err(|error| {
        format!(
            "issues_jsonl_read_failed path={} error={error}",
            issues_path.display()
        )
    })?;

    for line in raw.lines().filter(|line| !line.trim().is_empty()) {
        let value: Value = serde_json::from_str(line)
            .map_err(|error| format!("issues_jsonl_parse_failed error={error} line={line}"))?;
        if value.get("id").and_then(Value::as_str) == Some(issue_id) {
            let mut canonical = value
                .get("description")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned();

            if let Some(comments) = value.get("comments").and_then(Value::as_array) {
                for comment in comments {
                    if let Some(text) = comment.get("text").and_then(Value::as_str) {
                        canonical.push_str("\n\n");
                        canonical.push_str(text);
                    }
                }
            }

            return Ok(canonical);
        }
    }

    Err(format!("bead_id={issue_id} not_found_in={ISSUES_JSONL}"))
}

fn contains_identifier(text: &str, expected_marker: &str) -> bool {
    text.split(|ch: char| !(ch.is_ascii_alphanumeric() || ch == '_' || ch == '.'))
        .any(|token| token == expected_marker)
}

fn evaluate_description(description: &str) -> ComplianceEvaluation {
    let missing_unit_ids = UNIT_TEST_IDS
        .into_iter()
        .filter(|id| !contains_identifier(description, id))
        .collect::<Vec<_>>();

    let missing_e2e_ids = E2E_TEST_IDS
        .into_iter()
        .filter(|id| !contains_identifier(description, id))
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
        missing_script_marker: !description.contains(REQUIRED_SCRIPT_MARKER),
    }
}

fn parse_risk_id(line: &str) -> Option<&str> {
    let trimmed = line.trim_start();
    if !trimmed.starts_with('R') {
        return None;
    }

    let rest = &trimmed[1..];
    let digits_len = rest.chars().take_while(char::is_ascii_digit).count();
    if digits_len == 0 {
        return None;
    }

    let after_digits = rest.chars().nth(digits_len)?;
    if after_digits != '.' {
        return None;
    }

    Some(&trimmed[..=digits_len])
}

fn parse_risk_sections(description: &str) -> Vec<RiskSection> {
    let mut sections = Vec::new();
    let mut current_id: Option<String> = None;
    let mut current_body = String::new();

    for line in description.lines() {
        if let Some(risk_id) = parse_risk_id(line) {
            if let Some(id) = current_id.take() {
                sections.push(RiskSection {
                    id,
                    body: current_body.trim().to_owned(),
                });
                current_body.clear();
            }
            current_id = Some(risk_id.to_owned());
            current_body.push_str(line);
            current_body.push('\n');
            continue;
        }

        if current_id.is_some() {
            current_body.push_str(line);
            current_body.push('\n');
        }
    }

    if let Some(id) = current_id {
        sections.push(RiskSection {
            id,
            body: current_body.trim().to_owned(),
        });
    }

    sections
}

fn check_risks_have_unique_ids(description: &str) -> Result<(), String> {
    let sections = parse_risk_sections(description);
    let ids = sections
        .iter()
        .map(|section| section.id.clone())
        .collect::<Vec<_>>();

    let mut counts = BTreeMap::<String, usize>::new();
    for id in &ids {
        *counts.entry(id.clone()).or_default() += 1;
    }

    let duplicates = counts
        .iter()
        .filter_map(|(id, count)| (*count > 1).then_some(id.clone()))
        .collect::<Vec<_>>();

    if !duplicates.is_empty() {
        return Err(format!(
            "bead_id={BEAD_ID} case=risk_ids_not_unique duplicates={duplicates:?}"
        ));
    }

    let actual = ids.into_iter().collect::<BTreeSet<_>>();
    let expected = EXPECTED_RISK_IDS
        .into_iter()
        .map(str::to_owned)
        .collect::<BTreeSet<_>>();

    if actual != expected {
        return Err(format!(
            "bead_id={BEAD_ID} case=risk_ids_mismatch expected={expected:?} actual={actual:?}"
        ));
    }

    Ok(())
}

fn check_each_high_priority_risk_has_signal(description: &str) -> Result<(), String> {
    let sections = parse_risk_sections(description)
        .into_iter()
        .filter(|section| HIGH_PRIORITY_RISK_IDS.contains(&section.id.as_str()))
        .collect::<Vec<_>>();

    let missing_signal = sections
        .iter()
        .filter_map(|section| {
            let has_monitoring = section.body.contains("Monitoring approach");
            let has_trigger_conditions = section.body.contains("Trigger conditions");
            let has_test_signal =
                section.body.contains("Test requirements") || section.body.contains("test_");
            (!has_monitoring || !has_trigger_conditions || !has_test_signal)
                .then_some(section.id.clone())
        })
        .collect::<Vec<_>>();

    if !missing_signal.is_empty() {
        return Err(format!(
            "bead_id={BEAD_ID} case=high_priority_risk_missing_signal risks={missing_signal:?}"
        ));
    }

    Ok(())
}

fn check_each_risk_has_mitigation_pointer(description: &str) -> Result<(), String> {
    let sections = parse_risk_sections(description);
    let missing_pointers = sections
        .iter()
        .filter_map(|section| {
            let has_mitigation_heading = section.body.contains("Mitigation strategies");
            let has_structured_mitigation_entries =
                section.body.contains("\n1.") || section.body.contains("\n1. ");
            let has_pointer = section.body.contains("Section ")
                || section.body.contains('§')
                || section.body.contains("Phase ")
                || section.body.contains('Q')
                || section.body.contains("from spec");
            (!has_mitigation_heading || (!has_pointer && !has_structured_mitigation_entries))
                .then_some(section.id.clone())
        })
        .collect::<Vec<_>>();

    if !missing_pointers.is_empty() {
        return Err(format!(
            "bead_id={BEAD_ID} case=risk_missing_mitigation_pointer risks={missing_pointers:?}"
        ));
    }

    Ok(())
}

#[test]
fn test_risks_have_unique_ids() -> Result<(), String> {
    let description = load_issue_description(BEAD_ID)?;
    check_risks_have_unique_ids(&description)
}

#[test]
fn test_each_high_priority_risk_has_signal() -> Result<(), String> {
    let description = load_issue_description(BEAD_ID)?;
    check_each_high_priority_risk_has_signal(&description)
}

#[test]
fn test_each_risk_has_mitigation_pointer() -> Result<(), String> {
    let description = load_issue_description(BEAD_ID)?;
    check_each_risk_has_mitigation_pointer(&description)
}

#[test]
fn test_bd_3kp_2_unit_compliance_gate() -> Result<(), String> {
    let description = load_issue_description(BEAD_ID)?;
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
    if evaluation.missing_script_marker {
        return Err(format!(
            "bead_id={BEAD_ID} case=missing_script_marker expected={REQUIRED_SCRIPT_MARKER}"
        ));
    }

    check_risks_have_unique_ids(&description)?;
    check_each_high_priority_risk_has_signal(&description)?;
    check_each_risk_has_mitigation_pointer(&description)?;

    Ok(())
}

proptest! {
    #[test]
    fn prop_bd_3kp_2_structure_compliance(missing_index in 0usize..REQUIRED_TOKENS.len()) {
        let mut synthetic = format!(
            "## Unit Test Requirements\n- {}\n- {}\n- {}\n- {}\n- {}\n\n## E2E Test\n- {}\n- {}\n- {}\n\n## Logging Requirements\n- DEBUG: stage progress\n- INFO: summary\n- WARN: degraded mode\n- ERROR: terminal failure\n- Reference: {}\n",
            UNIT_TEST_IDS[0],
            UNIT_TEST_IDS[1],
            UNIT_TEST_IDS[2],
            UNIT_TEST_IDS[3],
            UNIT_TEST_IDS[4],
            E2E_TEST_IDS[0],
            E2E_TEST_IDS[1],
            REQUIRED_SCRIPT_MARKER,
            LOG_STANDARD_REF,
        );

        synthetic = synthetic.replacen(REQUIRED_TOKENS[missing_index], "", 1);
        let evaluation = evaluate_description(&synthetic);
        if evaluation.is_compliant() {
            return Err(TestCaseError::fail(format!(
                "bead_id={} case=structure_compliance expected_non_compliant missing_index={} missing_marker={}",
                BEAD_ID,
                missing_index,
                REQUIRED_TOKENS[missing_index]
            )));
        }
    }
}

#[test]
fn test_e2e_bd_3kp_2_compliance() -> Result<(), String> {
    let description = load_issue_description(BEAD_ID)?;
    let evaluation = evaluate_description(&description);

    eprintln!("DEBUG bead_id={BEAD_ID} case=e2e_start expected_script={REQUIRED_SCRIPT_MARKER}");

    let mut risk_errors = Vec::new();
    if let Err(error) = check_risks_have_unique_ids(&description) {
        risk_errors.push(error);
    }
    if let Err(error) = check_each_high_priority_risk_has_signal(&description) {
        risk_errors.push(error);
    }
    if let Err(error) = check_each_risk_has_mitigation_pointer(&description) {
        risk_errors.push(error);
    }

    eprintln!(
        "INFO bead_id={BEAD_ID} case=e2e_summary missing_unit_ids={} missing_e2e_ids={} missing_log_levels={} missing_log_standard_ref={} missing_script_marker={} risk_errors={}",
        evaluation.missing_unit_ids.len(),
        evaluation.missing_e2e_ids.len(),
        evaluation.missing_log_levels.len(),
        evaluation.missing_log_standard_ref,
        evaluation.missing_script_marker,
        risk_errors.len()
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
    if evaluation.missing_script_marker {
        eprintln!(
            "ERROR bead_id={BEAD_ID} case=missing_script_marker expected={REQUIRED_SCRIPT_MARKER}"
        );
    }
    for error in &risk_errors {
        eprintln!("WARN bead_id={BEAD_ID} case=risk_validation_failure detail={error}");
    }

    let root = workspace_root()?;
    let script = root.join(REQUIRED_SCRIPT_MARKER);
    if !script.is_file() {
        return Err(format!(
            "bead_id={BEAD_ID} case=e2e_script_missing path={}",
            script.display()
        ));
    }

    let output = Command::new("bash")
        .arg(script.as_os_str())
        .current_dir(&root)
        .output()
        .map_err(|error| {
            format!(
                "bead_id={BEAD_ID} case=e2e_script_exec_failed path={} error={error}",
                script.display()
            )
        })?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    eprintln!(
        "DEBUG bead_id={BEAD_ID} case=e2e_script_status code={}",
        output.status.code().unwrap_or(-1)
    );
    if !stderr.trim().is_empty() {
        eprintln!("WARN bead_id={BEAD_ID} case=e2e_script_stderr text={stderr}");
    }

    if !output.status.success() {
        eprintln!(
            "ERROR bead_id={BEAD_ID} case=e2e_script_failure stdout={stdout} stderr={stderr}"
        );
        return Err(format!(
            "bead_id={BEAD_ID} case=e2e_script_nonzero_exit status={}",
            output.status
        ));
    }

    if !stdout.contains("case=summary") {
        return Err(format!(
            "bead_id={BEAD_ID} case=e2e_script_missing_summary output={stdout}"
        ));
    }

    if !evaluation.is_compliant() || !risk_errors.is_empty() {
        return Err(format!(
            "bead_id={BEAD_ID} case=e2e_compliance_failure evaluation={evaluation:?} risk_errors={risk_errors:?}"
        ));
    }

    Ok(())
}

#[test]
fn test_e2e_bd_3kp_2() -> Result<(), String> {
    test_e2e_bd_3kp_2_compliance()
}
