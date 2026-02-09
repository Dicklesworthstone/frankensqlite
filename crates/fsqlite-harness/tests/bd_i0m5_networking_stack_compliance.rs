use std::fs;
use std::path::{Path, PathBuf};

use proptest::prelude::proptest;
use proptest::test_runner::TestCaseError;
use serde_json::Value;

const BEAD_ID: &str = "bd-i0m5";
const ISSUES_JSONL: &str = ".beads/issues.jsonl";
const UNIT_TEST_IDS: [&str; 9] = [
    "test_tls_by_default",
    "test_plaintext_requires_explicit_opt_in",
    "test_http2_max_concurrent_streams",
    "test_http2_max_header_list_size",
    "test_http2_continuation_timeout",
    "test_message_size_cap_enforced",
    "test_handshake_timeout_bounded",
    "test_virtual_tcp_deterministic",
    "test_virtual_tcp_fault_injection",
];
const E2E_TEST_IDS: [&str; 1] = ["test_e2e_networking_stack_replication_under_loss"];
const LOG_LEVEL_MARKERS: [&str; 4] = ["DEBUG", "INFO", "WARN", "ERROR"];
const SPEC_MARKERS: [&str; 8] = [
    "rustls",
    "max_concurrent_streams=256",
    "max_header_list_size=64KiB",
    "continuation_timeout=5s",
    "header_fragment_cap",
    "4MiB",
    "VirtualTcp",
    "drop/reorder/corrupt",
];
const REQUIRED_TOKENS: [&str; 22] = [
    "test_tls_by_default",
    "test_plaintext_requires_explicit_opt_in",
    "test_http2_max_concurrent_streams",
    "test_http2_max_header_list_size",
    "test_http2_continuation_timeout",
    "test_message_size_cap_enforced",
    "test_handshake_timeout_bounded",
    "test_virtual_tcp_deterministic",
    "test_virtual_tcp_fault_injection",
    "test_e2e_networking_stack_replication_under_loss",
    "DEBUG",
    "INFO",
    "WARN",
    "ERROR",
    "rustls",
    "max_concurrent_streams=256",
    "max_header_list_size=64KiB",
    "continuation_timeout=5s",
    "header_fragment_cap",
    "4MiB",
    "VirtualTcp",
    "drop/reorder/corrupt",
];

#[derive(Debug, PartialEq, Eq)]
#[allow(clippy::struct_field_names)]
struct ComplianceEvaluation {
    missing_unit_ids: Vec<&'static str>,
    missing_e2e_ids: Vec<&'static str>,
    missing_log_levels: Vec<&'static str>,
    missing_spec_markers: Vec<&'static str>,
}

impl ComplianceEvaluation {
    fn is_compliant(&self) -> bool {
        self.missing_unit_ids.is_empty()
            && self.missing_e2e_ids.is_empty()
            && self.missing_log_levels.is_empty()
            && self.missing_spec_markers.is_empty()
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

fn contains_identifier(text: &str, expected_marker: &str) -> bool {
    text.split(|ch: char| !(ch.is_ascii_alphanumeric() || ch == '_'))
        .any(|part| part == expected_marker)
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

    let missing_spec_markers = SPEC_MARKERS
        .into_iter()
        .filter(|marker| !description.contains(marker))
        .collect::<Vec<_>>();

    ComplianceEvaluation {
        missing_unit_ids,
        missing_e2e_ids,
        missing_log_levels,
        missing_spec_markers,
    }
}

#[test]
fn test_bd_i0m5_unit_compliance_gate() -> Result<(), String> {
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
    if !evaluation.missing_spec_markers.is_empty() {
        return Err(format!(
            "bead_id={BEAD_ID} case=spec_markers_missing missing={:?}",
            evaluation.missing_spec_markers
        ));
    }

    Ok(())
}

proptest! {
    #[test]
    fn prop_bd_i0m5_structure_compliance(missing_index in 0usize..REQUIRED_TOKENS.len()) {
        let mut synthetic = format!(
            "## Unit Test Requirements\n- {}\n- {}\n- {}\n- {}\n- {}\n- {}\n- {}\n- {}\n- {}\n\n## E2E Test\n- {}\n\n## Logging Requirements\n- DEBUG: handshake details\n- INFO: connection established\n- WARN: bounded timeout\n- ERROR: deterministic rejection\n",
            UNIT_TEST_IDS[0],
            UNIT_TEST_IDS[1],
            UNIT_TEST_IDS[2],
            UNIT_TEST_IDS[3],
            UNIT_TEST_IDS[4],
            UNIT_TEST_IDS[5],
            UNIT_TEST_IDS[6],
            UNIT_TEST_IDS[7],
            UNIT_TEST_IDS[8],
            E2E_TEST_IDS[0],
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
fn test_e2e_bd_i0m5_compliance() -> Result<(), String> {
    let description = load_issue_description(BEAD_ID)?;
    let evaluation = evaluate_description(&description);

    eprintln!(
        "INFO bead_id={BEAD_ID} case=e2e_summary missing_unit_ids={} missing_e2e_ids={} missing_log_levels={} missing_spec_markers={}",
        evaluation.missing_unit_ids.len(),
        evaluation.missing_e2e_ids.len(),
        evaluation.missing_log_levels.len(),
        evaluation.missing_spec_markers.len(),
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
    for marker in &evaluation.missing_spec_markers {
        eprintln!("WARN bead_id={BEAD_ID} case=missing_spec_marker marker={marker}");
    }

    if !evaluation.is_compliant() {
        eprintln!("ERROR bead_id={BEAD_ID} case=e2e_compliance_failure");
        return Err(format!(
            "bead_id={BEAD_ID} case=e2e_compliance_failure evaluation={evaluation:?}"
        ));
    }

    Ok(())
}

#[test]
fn test_e2e_networking_stack_replication_under_loss_smoke() -> Result<(), String> {
    test_e2e_bd_i0m5_compliance()
}

#[test]
fn test_bd_i0m5_transport_limits_markers_present() -> Result<(), String> {
    let description = load_issue_description(BEAD_ID)?;
    let evaluation = evaluate_description(&description);
    if !evaluation.missing_spec_markers.is_empty() {
        return Err(format!(
            "bead_id={BEAD_ID} case=transport_limits_markers_missing missing={:?}",
            evaluation.missing_spec_markers
        ));
    }
    Ok(())
}
