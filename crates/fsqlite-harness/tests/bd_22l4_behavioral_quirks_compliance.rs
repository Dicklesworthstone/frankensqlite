use std::fs;
use std::path::{Path, PathBuf};

use proptest::prelude::proptest;
use proptest::test_runner::TestCaseError;
use serde_json::Value;

const BEAD_ID: &str = "bd-22l4";
const ISSUES_JSONL: &str = ".beads/issues.jsonl";
const UNIT_TEST_IDS: [&str; 2] = [
    "test_bd_22l4_unit_compliance_gate",
    "prop_bd_22l4_structure_compliance",
];
const BEHAVIOR_QUIRK_TEST_IDS: [&str; 14] = [
    "test_type_affinity_advisory",
    "test_strict_table_type_enforcement",
    "test_null_unique_multiple",
    "test_order_by_compound_first_select",
    "test_integer_overflow_promotes_real",
    "test_sum_overflow_error",
    "test_autoincrement_no_reuse",
    "test_rowid_reuse_without_autoincrement",
    "test_max_rowid_random_fallback",
    "test_like_ascii_case_insensitive",
    "test_like_unicode_case_sensitive",
    "test_empty_string_not_null",
    "test_nondeterministic_reevaluated",
    "test_deterministic_factored",
];
const E2E_TEST_IDS: [&str; 2] = ["test_e2e_bd_22l4", "test_e2e_bd_22l4_compliance"];
const LOG_LEVEL_MARKERS: [&str; 4] = ["DEBUG", "INFO", "WARN", "ERROR"];
const LOG_STANDARD_REF: &str = "bd-1fpm";
const REQUIRED_TOKENS: [&str; 23] = [
    "test_bd_22l4_unit_compliance_gate",
    "prop_bd_22l4_structure_compliance",
    "test_e2e_bd_22l4",
    "test_e2e_bd_22l4_compliance",
    "test_type_affinity_advisory",
    "test_strict_table_type_enforcement",
    "test_null_unique_multiple",
    "test_order_by_compound_first_select",
    "test_integer_overflow_promotes_real",
    "test_sum_overflow_error",
    "test_autoincrement_no_reuse",
    "test_rowid_reuse_without_autoincrement",
    "test_max_rowid_random_fallback",
    "test_like_ascii_case_insensitive",
    "test_like_unicode_case_sensitive",
    "test_empty_string_not_null",
    "test_nondeterministic_reevaluated",
    "test_deterministic_factored",
    "DEBUG",
    "INFO",
    "WARN",
    "ERROR",
    "bd-1fpm",
];

#[derive(Debug, PartialEq, Eq)]
#[allow(clippy::struct_field_names)]
struct ComplianceEvaluation {
    missing_unit_ids: Vec<&'static str>,
    missing_behavior_quirk_ids: Vec<&'static str>,
    missing_e2e_ids: Vec<&'static str>,
    missing_log_levels: Vec<&'static str>,
    missing_log_standard_ref: bool,
}

impl ComplianceEvaluation {
    fn is_compliant(&self) -> bool {
        self.missing_unit_ids.is_empty()
            && self.missing_behavior_quirk_ids.is_empty()
            && self.missing_e2e_ids.is_empty()
            && self.missing_log_levels.is_empty()
            && !self.missing_log_standard_ref
    }
}

fn workspace_root() -> Result<PathBuf, String> {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .map_err(|error| format!("workspace_root_canonicalize_failed: {error}"))
}

fn load_issue_description(issue_id: &str) -> Result<String, String> {
    let issues_path = workspace_root()?.join(ISSUES_JSONL);
    let raw = fs::read_to_string(&issues_path)
        .map_err(|error| format!("issues_jsonl_read_failed path={issues_path:?} error={error}"))?;

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

fn is_identifier_char(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

fn contains_identifier(text: &str, needle: &str) -> bool {
    text.match_indices(needle).any(|(start, _)| {
        let end = start + needle.len();
        let bytes = text.as_bytes();

        let before_ok = start == 0 || !is_identifier_char(bytes[start - 1]);
        let after_ok = end == bytes.len() || !is_identifier_char(bytes[end]);

        before_ok && after_ok
    })
}

fn evaluate_description(description: &str) -> ComplianceEvaluation {
    let missing_unit_ids = UNIT_TEST_IDS
        .into_iter()
        .filter(|id| !contains_identifier(description, id))
        .collect::<Vec<_>>();

    let missing_behavior_quirk_ids = BEHAVIOR_QUIRK_TEST_IDS
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
        missing_behavior_quirk_ids,
        missing_e2e_ids,
        missing_log_levels,
        missing_log_standard_ref: !description.contains(LOG_STANDARD_REF),
    }
}

#[test]
fn test_bd_22l4_unit_compliance_gate() -> Result<(), String> {
    let description = load_issue_description(BEAD_ID)?;
    let evaluation = evaluate_description(&description);

    if !evaluation.missing_unit_ids.is_empty() {
        return Err(format!(
            "bead_id={BEAD_ID} case=unit_ids_missing missing={:?}",
            evaluation.missing_unit_ids
        ));
    }
    if !evaluation.missing_behavior_quirk_ids.is_empty() {
        return Err(format!(
            "bead_id={BEAD_ID} case=behavior_quirk_ids_missing missing={:?}",
            evaluation.missing_behavior_quirk_ids
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

    Ok(())
}

proptest! {
    #[test]
    fn prop_bd_22l4_structure_compliance(missing_index in 0usize..REQUIRED_TOKENS.len()) {
        let mut synthetic = format!(
            "## Unit Test Requirements\n- {}\n- {}\n- {}\n- {}\n- {}\n- {}\n- {}\n- {}\n- {}\n- {}\n- {}\n- {}\n- {}\n- {}\n- {}\n- {}\n\n## E2E Test\n- {}\n- {}\n\n## Logging Requirements\n- DEBUG: stage progress\n- INFO: summary\n- WARN: degraded mode\n- ERROR: terminal failure\n- Reference: {}\n",
            UNIT_TEST_IDS[0],
            UNIT_TEST_IDS[1],
            BEHAVIOR_QUIRK_TEST_IDS[0],
            BEHAVIOR_QUIRK_TEST_IDS[1],
            BEHAVIOR_QUIRK_TEST_IDS[2],
            BEHAVIOR_QUIRK_TEST_IDS[3],
            BEHAVIOR_QUIRK_TEST_IDS[4],
            BEHAVIOR_QUIRK_TEST_IDS[5],
            BEHAVIOR_QUIRK_TEST_IDS[6],
            BEHAVIOR_QUIRK_TEST_IDS[7],
            BEHAVIOR_QUIRK_TEST_IDS[8],
            BEHAVIOR_QUIRK_TEST_IDS[9],
            BEHAVIOR_QUIRK_TEST_IDS[10],
            BEHAVIOR_QUIRK_TEST_IDS[11],
            BEHAVIOR_QUIRK_TEST_IDS[12],
            BEHAVIOR_QUIRK_TEST_IDS[13],
            E2E_TEST_IDS[0],
            E2E_TEST_IDS[1],
            LOG_STANDARD_REF,
        );

        synthetic = synthetic.replacen(REQUIRED_TOKENS[missing_index], "", 1);
        let evaluation = evaluate_description(&synthetic);
        if evaluation.is_compliant() {
            return Err(TestCaseError::fail(format!(
                "bead_id={} case=structure_compliance expected_non_compliant missing_index={} marker={}",
                BEAD_ID,
                missing_index,
                REQUIRED_TOKENS[missing_index]
            )));
        }
    }
}

#[test]
fn test_e2e_bd_22l4_compliance() -> Result<(), String> {
    let description = load_issue_description(BEAD_ID)?;
    let evaluation = evaluate_description(&description);

    eprintln!(
        "DEBUG bead_id={BEAD_ID} case=e2e_start issue_file={} required_token_count={}",
        ISSUES_JSONL,
        REQUIRED_TOKENS.len()
    );

    eprintln!(
        "INFO bead_id={BEAD_ID} case=e2e_summary missing_unit_ids={} missing_behavior_quirk_ids={} missing_e2e_ids={} missing_log_levels={} missing_log_standard_ref={}",
        evaluation.missing_unit_ids.len(),
        evaluation.missing_behavior_quirk_ids.len(),
        evaluation.missing_e2e_ids.len(),
        evaluation.missing_log_levels.len(),
        evaluation.missing_log_standard_ref
    );

    for id in &evaluation.missing_unit_ids {
        eprintln!("WARN bead_id={BEAD_ID} case=missing_unit_id id={id}");
    }
    for id in &evaluation.missing_behavior_quirk_ids {
        eprintln!("WARN bead_id={BEAD_ID} case=missing_behavior_quirk_id id={id}");
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

    if !evaluation.is_compliant() {
        return Err(format!(
            "bead_id={BEAD_ID} case=e2e_compliance_failure evaluation={evaluation:?}"
        ));
    }

    Ok(())
}

#[test]
fn test_e2e_bd_22l4() -> Result<(), String> {
    test_e2e_bd_22l4_compliance()
}
