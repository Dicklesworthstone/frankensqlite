use std::fs;
use std::path::{Path, PathBuf};

use proptest::prelude::proptest;
use proptest::test_runner::TestCaseError;
use serde_json::Value;

const BEAD_ID: &str = "bd-148q";
const ISSUES_JSONL: &str = ".beads/issues.jsonl";
const UNIT_TEST_IDS: [&str; 2] = [
    "test_bd_148q_unit_compliance_gate",
    "prop_bd_148q_structure_compliance",
];
const RESILIENCE_TEST_IDS: [&str; 11] = [
    "test_bulkhead_limits_concurrent_work",
    "test_governor_budget_exhaustion_degrades_gracefully",
    "test_circuit_breaker_opens_on_threshold",
    "test_retry_with_exponential_backoff_and_jitter",
    "test_hedge_first_ok_cancels_loser",
    "test_pipeline_backpressure",
    "test_bracket_cleanup_under_cancellation",
    "test_rate_limit_caps_background_work",
    "test_losers_drain_invariant",
    "test_governor_default_from_available_parallelism",
    "test_e2e_resilience_combinators_with_fault_injection",
];
const E2E_TEST_IDS: [&str; 1] = ["test_e2e_bd_148q_compliance"];
const RESILIENCE_MARKERS: [&str; 11] = [
    "INV-LOSERS-DRAIN",
    "INV-NO-OBLIGATION-LEAKS",
    "pipeline",
    "bulkhead",
    "governor",
    "rate_limit",
    "retry",
    "circuit_breaker",
    "hedge",
    "bracket",
    "available_parallelism",
];
const GOVERNANCE_MARKERS: [&str; 2] = ["fsqlite.bg_cpu_max", "fsqlite.remote_max_in_flight"];
const LOG_LEVEL_MARKERS: [&str; 4] = ["DEBUG", "INFO", "WARN", "ERROR"];
const LOG_STANDARD_REF: &str = "bd-1fpm";
const REQUIRED_TOKENS: [&str; 33] = [
    "test_bd_148q_unit_compliance_gate",
    "prop_bd_148q_structure_compliance",
    "test_bulkhead_limits_concurrent_work",
    "test_governor_budget_exhaustion_degrades_gracefully",
    "test_circuit_breaker_opens_on_threshold",
    "test_retry_with_exponential_backoff_and_jitter",
    "test_hedge_first_ok_cancels_loser",
    "test_pipeline_backpressure",
    "test_bracket_cleanup_under_cancellation",
    "test_rate_limit_caps_background_work",
    "test_losers_drain_invariant",
    "test_governor_default_from_available_parallelism",
    "test_e2e_resilience_combinators_with_fault_injection",
    "test_e2e_bd_148q_compliance",
    "INV-LOSERS-DRAIN",
    "INV-NO-OBLIGATION-LEAKS",
    "pipeline",
    "bulkhead",
    "governor",
    "rate_limit",
    "retry",
    "circuit_breaker",
    "hedge",
    "bracket",
    "available_parallelism",
    "fsqlite.bg_cpu_max",
    "fsqlite.remote_max_in_flight",
    "DEBUG",
    "INFO",
    "WARN",
    "ERROR",
    "bd-1fpm",
    "test_e2e_bd_148q_compliance",
];

#[derive(Debug, PartialEq, Eq)]
#[allow(clippy::struct_field_names)]
struct ComplianceEvaluation {
    missing_unit_ids: Vec<&'static str>,
    missing_resilience_test_ids: Vec<&'static str>,
    missing_e2e_ids: Vec<&'static str>,
    missing_resilience_markers: Vec<&'static str>,
    missing_governance_markers: Vec<&'static str>,
    missing_log_levels: Vec<&'static str>,
    missing_log_standard_ref: bool,
}

impl ComplianceEvaluation {
    fn is_compliant(&self) -> bool {
        self.missing_unit_ids.is_empty()
            && self.missing_resilience_test_ids.is_empty()
            && self.missing_e2e_ids.is_empty()
            && self.missing_resilience_markers.is_empty()
            && self.missing_governance_markers.is_empty()
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

    let missing_resilience_test_ids = RESILIENCE_TEST_IDS
        .into_iter()
        .filter(|id| !contains_identifier(description, id))
        .collect::<Vec<_>>();

    let missing_e2e_ids = E2E_TEST_IDS
        .into_iter()
        .filter(|id| !contains_identifier(description, id))
        .collect::<Vec<_>>();

    let missing_resilience_markers = RESILIENCE_MARKERS
        .into_iter()
        .filter(|marker| !description.contains(marker))
        .collect::<Vec<_>>();

    let missing_governance_markers = GOVERNANCE_MARKERS
        .into_iter()
        .filter(|marker| !description.contains(marker))
        .collect::<Vec<_>>();

    let missing_log_levels = LOG_LEVEL_MARKERS
        .into_iter()
        .filter(|level| !description.contains(level))
        .collect::<Vec<_>>();

    ComplianceEvaluation {
        missing_unit_ids,
        missing_resilience_test_ids,
        missing_e2e_ids,
        missing_resilience_markers,
        missing_governance_markers,
        missing_log_levels,
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
    for id in RESILIENCE_TEST_IDS {
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

    text.push_str("\n## Resilience Markers\n");
    for marker in RESILIENCE_MARKERS {
        text.push_str("- ");
        text.push_str(marker);
        text.push('\n');
    }

    text.push_str("\n## Governance Markers\n");
    for marker in GOVERNANCE_MARKERS {
        text.push_str("- ");
        text.push_str(marker);
        text.push('\n');
    }

    text.push_str("\n## Logging Requirements\n");
    text.push_str("- DEBUG: stage progress\n");
    text.push_str("- INFO: completion summary\n");
    text.push_str("- WARN: degraded behavior\n");
    text.push_str("- ERROR: reproducible failure details\n");
    text.push_str("- Reference: ");
    text.push_str(LOG_STANDARD_REF);
    text.push('\n');

    text
}

#[test]
fn test_bd_148q_unit_compliance_gate() -> Result<(), String> {
    let description = load_issue_description(BEAD_ID)?;
    let evaluation = evaluate_description(&description);

    if !evaluation.missing_unit_ids.is_empty() {
        return Err(format!(
            "bead_id={BEAD_ID} case=unit_ids_missing missing={:?}",
            evaluation.missing_unit_ids
        ));
    }
    if !evaluation.missing_resilience_test_ids.is_empty() {
        return Err(format!(
            "bead_id={BEAD_ID} case=resilience_test_ids_missing missing={:?}",
            evaluation.missing_resilience_test_ids
        ));
    }
    if !evaluation.missing_e2e_ids.is_empty() {
        return Err(format!(
            "bead_id={BEAD_ID} case=e2e_ids_missing missing={:?}",
            evaluation.missing_e2e_ids
        ));
    }
    if !evaluation.missing_resilience_markers.is_empty() {
        return Err(format!(
            "bead_id={BEAD_ID} case=resilience_markers_missing missing={:?}",
            evaluation.missing_resilience_markers
        ));
    }
    if !evaluation.missing_governance_markers.is_empty() {
        return Err(format!(
            "bead_id={BEAD_ID} case=governance_markers_missing missing={:?}",
            evaluation.missing_governance_markers
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
    fn prop_bd_148q_structure_compliance(missing_index in 0usize..REQUIRED_TOKENS.len()) {
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
fn test_e2e_bd_148q_compliance() -> Result<(), String> {
    let description = load_issue_description(BEAD_ID)?;
    let evaluation = evaluate_description(&description);

    eprintln!(
        "DEBUG bead_id={BEAD_ID} case=e2e_start issue_file={} required_token_count={}",
        ISSUES_JSONL,
        REQUIRED_TOKENS.len()
    );

    eprintln!(
        "INFO bead_id={BEAD_ID} case=e2e_summary missing_unit_ids={} missing_resilience_test_ids={} missing_e2e_ids={} missing_resilience_markers={} missing_governance_markers={} missing_log_levels={} missing_log_standard_ref={}",
        evaluation.missing_unit_ids.len(),
        evaluation.missing_resilience_test_ids.len(),
        evaluation.missing_e2e_ids.len(),
        evaluation.missing_resilience_markers.len(),
        evaluation.missing_governance_markers.len(),
        evaluation.missing_log_levels.len(),
        evaluation.missing_log_standard_ref
    );

    for id in &evaluation.missing_unit_ids {
        eprintln!("WARN bead_id={BEAD_ID} case=missing_unit_id id={id}");
    }
    for id in &evaluation.missing_resilience_test_ids {
        eprintln!("WARN bead_id={BEAD_ID} case=missing_resilience_test_id id={id}");
    }
    for id in &evaluation.missing_e2e_ids {
        eprintln!("WARN bead_id={BEAD_ID} case=missing_e2e_id id={id}");
    }
    for marker in &evaluation.missing_resilience_markers {
        eprintln!("WARN bead_id={BEAD_ID} case=missing_resilience_marker marker={marker}");
    }
    for marker in &evaluation.missing_governance_markers {
        eprintln!("WARN bead_id={BEAD_ID} case=missing_governance_marker marker={marker}");
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
fn test_e2e_bd_148q() -> Result<(), String> {
    test_e2e_bd_148q_compliance()
}
