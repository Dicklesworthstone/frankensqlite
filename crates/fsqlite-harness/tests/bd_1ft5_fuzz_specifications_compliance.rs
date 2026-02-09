use std::fs;
use std::path::{Path, PathBuf};

use proptest::prelude::proptest;
use proptest::test_runner::TestCaseError;
use serde_json::Value;

const BEAD_ID: &str = "bd-1ft5";
const ISSUES_JSONL: &str = ".beads/issues.jsonl";
const UNIT_TEST_IDS: [&str; 2] = [
    "test_bd_1ft5_unit_compliance_gate",
    "prop_bd_1ft5_structure_compliance",
];
const FUZZ_TARGET_IDS: [&str; 9] = [
    "fuzz_sql_parser_raw_bytes",
    "fuzz_sql_parser_grammar_based",
    "fuzz_record_decoder",
    "fuzz_btree_page_decoder",
    "fuzz_wal_frame_decoder",
    "fuzz_json_parser",
    "fuzz_raptorq_decoder",
    "test_fuzz_sql_parser_known_crashes",
    "test_fuzz_grammar_integrity_check_after_execution",
];
const FUZZ_TEST_IDS: [&str; 12] = [
    "test_fuzz_sql_parser_utf8",
    "test_fuzz_sql_parser_non_utf8",
    "test_fuzz_sql_parser_no_infinite_loop",
    "test_fuzz_grammar_select",
    "test_fuzz_grammar_insert",
    "test_fuzz_grammar_integrity_check",
    "test_fuzz_record_decoder_no_panic",
    "test_fuzz_btree_page_decoder_no_panic",
    "test_fuzz_wal_frame_decoder_no_panic",
    "test_fuzz_json_parser_no_panic",
    "test_fuzz_raptorq_decoder_no_corruption",
    "test_fuzz_raptorq_decoder_bitflip_detection",
];
const E2E_TEST_IDS: [&str; 2] = ["test_e2e_bd_1ft5", "test_e2e_bd_1ft5_compliance"];
const FUZZ_MARKERS: [&str; 11] = [
    "cargo-fuzz",
    "libfuzzer",
    "arbitrary",
    "FuzzStatement",
    "PRAGMA integrity_check",
    "decode_record",
    "json_valid",
    "silent corruption",
    "must not panic",
    "must not loop",
    "1 million iterations",
];
const LOG_LEVEL_MARKERS: [&str; 4] = ["DEBUG", "INFO", "WARN", "ERROR"];
const LOG_DETAIL_MARKERS: [&str; 4] = ["target", "cases", "crashes", "duration_ms"];
const LOG_STANDARD_REF: &str = "bd-1fpm";
const REQUIRED_TOKENS: [&str; 45] = [
    "test_bd_1ft5_unit_compliance_gate",
    "prop_bd_1ft5_structure_compliance",
    "fuzz_sql_parser_raw_bytes",
    "fuzz_sql_parser_grammar_based",
    "fuzz_record_decoder",
    "fuzz_btree_page_decoder",
    "fuzz_wal_frame_decoder",
    "fuzz_json_parser",
    "fuzz_raptorq_decoder",
    "test_fuzz_sql_parser_known_crashes",
    "test_fuzz_grammar_integrity_check_after_execution",
    "test_fuzz_sql_parser_utf8",
    "test_fuzz_sql_parser_non_utf8",
    "test_fuzz_sql_parser_no_infinite_loop",
    "test_fuzz_grammar_select",
    "test_fuzz_grammar_insert",
    "test_fuzz_grammar_integrity_check",
    "test_fuzz_record_decoder_no_panic",
    "test_fuzz_btree_page_decoder_no_panic",
    "test_fuzz_wal_frame_decoder_no_panic",
    "test_fuzz_json_parser_no_panic",
    "test_fuzz_raptorq_decoder_no_corruption",
    "test_fuzz_raptorq_decoder_bitflip_detection",
    "test_e2e_bd_1ft5",
    "test_e2e_bd_1ft5_compliance",
    "cargo-fuzz",
    "libfuzzer",
    "arbitrary",
    "FuzzStatement",
    "PRAGMA integrity_check",
    "decode_record",
    "json_valid",
    "silent corruption",
    "must not panic",
    "must not loop",
    "1 million iterations",
    "DEBUG",
    "INFO",
    "WARN",
    "ERROR",
    "target",
    "cases",
    "crashes",
    "duration_ms",
    "bd-1fpm",
];

#[derive(Debug, PartialEq, Eq)]
#[allow(clippy::struct_field_names)]
struct ComplianceEvaluation {
    missing_unit_ids: Vec<&'static str>,
    missing_fuzz_target_ids: Vec<&'static str>,
    missing_fuzz_test_ids: Vec<&'static str>,
    missing_e2e_ids: Vec<&'static str>,
    missing_fuzz_markers: Vec<&'static str>,
    missing_log_levels: Vec<&'static str>,
    missing_log_details: Vec<&'static str>,
    missing_log_standard_ref: bool,
}

impl ComplianceEvaluation {
    fn is_compliant(&self) -> bool {
        self.missing_unit_ids.is_empty()
            && self.missing_fuzz_target_ids.is_empty()
            && self.missing_fuzz_test_ids.is_empty()
            && self.missing_e2e_ids.is_empty()
            && self.missing_fuzz_markers.is_empty()
            && self.missing_log_levels.is_empty()
            && self.missing_log_details.is_empty()
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

    let missing_fuzz_target_ids = FUZZ_TARGET_IDS
        .into_iter()
        .filter(|id| !contains_identifier(description, id))
        .collect::<Vec<_>>();

    let missing_fuzz_test_ids = FUZZ_TEST_IDS
        .into_iter()
        .filter(|id| !contains_identifier(description, id))
        .collect::<Vec<_>>();

    let missing_e2e_ids = E2E_TEST_IDS
        .into_iter()
        .filter(|id| !contains_identifier(description, id))
        .collect::<Vec<_>>();

    let missing_fuzz_markers = FUZZ_MARKERS
        .into_iter()
        .filter(|marker| !description.contains(marker))
        .collect::<Vec<_>>();

    let missing_log_levels = LOG_LEVEL_MARKERS
        .into_iter()
        .filter(|level| !description.contains(level))
        .collect::<Vec<_>>();

    let missing_log_details = LOG_DETAIL_MARKERS
        .into_iter()
        .filter(|marker| !description.contains(marker))
        .collect::<Vec<_>>();

    ComplianceEvaluation {
        missing_unit_ids,
        missing_fuzz_target_ids,
        missing_fuzz_test_ids,
        missing_e2e_ids,
        missing_fuzz_markers,
        missing_log_levels,
        missing_log_details,
        missing_log_standard_ref: !description.contains(LOG_STANDARD_REF),
    }
}

fn synthetic_compliant_description() -> String {
    let mut text = String::from("## Unit Test Requirements\n");

    for id in UNIT_TEST_IDS {
        text.push_str("- ");
        text.push_str(id);
        text.push('\n');
    }
    for id in FUZZ_TARGET_IDS {
        text.push_str("- ");
        text.push_str(id);
        text.push('\n');
    }
    for id in FUZZ_TEST_IDS {
        text.push_str("- ");
        text.push_str(id);
        text.push('\n');
    }

    text.push_str("\n## E2E Test\n");
    for id in E2E_TEST_IDS {
        text.push_str("- ");
        text.push_str(id);
        text.push('\n');
    }

    text.push_str("\n## Fuzz Markers\n");
    for marker in FUZZ_MARKERS {
        text.push_str("- ");
        text.push_str(marker);
        text.push('\n');
    }

    text.push_str("\n## Logging Requirements\n");
    text.push_str("- DEBUG: stage progress\n");
    text.push_str("- INFO: fuzz run summary includes target, cases, crashes, duration_ms\n");
    text.push_str("- WARN: degraded mode\n");
    text.push_str("- ERROR: crash seed and minimized input artifact path\n");
    text.push_str("- Reference: ");
    text.push_str(LOG_STANDARD_REF);
    text.push('\n');

    text
}

#[test]
fn test_bd_1ft5_unit_compliance_gate() -> Result<(), String> {
    let description = load_issue_description(BEAD_ID)?;
    let evaluation = evaluate_description(&description);

    if !evaluation.missing_unit_ids.is_empty() {
        return Err(format!(
            "bead_id={BEAD_ID} case=unit_ids_missing missing={:?}",
            evaluation.missing_unit_ids
        ));
    }
    if !evaluation.missing_fuzz_target_ids.is_empty() {
        return Err(format!(
            "bead_id={BEAD_ID} case=fuzz_target_ids_missing missing={:?}",
            evaluation.missing_fuzz_target_ids
        ));
    }
    if !evaluation.missing_fuzz_test_ids.is_empty() {
        return Err(format!(
            "bead_id={BEAD_ID} case=fuzz_test_ids_missing missing={:?}",
            evaluation.missing_fuzz_test_ids
        ));
    }
    if !evaluation.missing_e2e_ids.is_empty() {
        return Err(format!(
            "bead_id={BEAD_ID} case=e2e_ids_missing missing={:?}",
            evaluation.missing_e2e_ids
        ));
    }
    if !evaluation.missing_fuzz_markers.is_empty() {
        return Err(format!(
            "bead_id={BEAD_ID} case=fuzz_markers_missing missing={:?}",
            evaluation.missing_fuzz_markers
        ));
    }
    if !evaluation.missing_log_levels.is_empty() {
        return Err(format!(
            "bead_id={BEAD_ID} case=logging_levels_missing missing={:?}",
            evaluation.missing_log_levels
        ));
    }
    if !evaluation.missing_log_details.is_empty() {
        return Err(format!(
            "bead_id={BEAD_ID} case=logging_detail_markers_missing missing={:?}",
            evaluation.missing_log_details
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
    fn prop_bd_1ft5_structure_compliance(missing_index in 0usize..REQUIRED_TOKENS.len()) {
        let mut synthetic = synthetic_compliant_description();
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
fn test_e2e_bd_1ft5_compliance() -> Result<(), String> {
    let description = load_issue_description(BEAD_ID)?;
    let evaluation = evaluate_description(&description);

    eprintln!(
        "DEBUG bead_id={BEAD_ID} case=e2e_start issue_file={} required_token_count={}",
        ISSUES_JSONL,
        REQUIRED_TOKENS.len()
    );

    eprintln!(
        "INFO bead_id={BEAD_ID} case=e2e_summary missing_unit_ids={} missing_fuzz_target_ids={} missing_fuzz_test_ids={} missing_e2e_ids={} missing_fuzz_markers={} missing_log_levels={} missing_log_details={} missing_log_standard_ref={}",
        evaluation.missing_unit_ids.len(),
        evaluation.missing_fuzz_target_ids.len(),
        evaluation.missing_fuzz_test_ids.len(),
        evaluation.missing_e2e_ids.len(),
        evaluation.missing_fuzz_markers.len(),
        evaluation.missing_log_levels.len(),
        evaluation.missing_log_details.len(),
        evaluation.missing_log_standard_ref
    );

    for id in &evaluation.missing_unit_ids {
        eprintln!("WARN bead_id={BEAD_ID} case=missing_unit_id id={id}");
    }
    for id in &evaluation.missing_fuzz_target_ids {
        eprintln!("WARN bead_id={BEAD_ID} case=missing_fuzz_target_id id={id}");
    }
    for id in &evaluation.missing_fuzz_test_ids {
        eprintln!("WARN bead_id={BEAD_ID} case=missing_fuzz_test_id id={id}");
    }
    for id in &evaluation.missing_e2e_ids {
        eprintln!("WARN bead_id={BEAD_ID} case=missing_e2e_id id={id}");
    }
    for marker in &evaluation.missing_fuzz_markers {
        eprintln!("WARN bead_id={BEAD_ID} case=missing_fuzz_marker marker={marker}");
    }
    for level in &evaluation.missing_log_levels {
        eprintln!("WARN bead_id={BEAD_ID} case=missing_log_level level={level}");
    }
    for marker in &evaluation.missing_log_details {
        eprintln!("WARN bead_id={BEAD_ID} case=missing_log_detail_marker marker={marker}");
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
fn test_e2e_bd_1ft5() -> Result<(), String> {
    test_e2e_bd_1ft5_compliance()
}
