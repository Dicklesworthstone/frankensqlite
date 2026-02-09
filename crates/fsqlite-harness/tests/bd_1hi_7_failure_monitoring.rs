use std::fs;
use std::path::{Path, PathBuf};

use fsqlite_core::repair_symbols::{
    DecodeAttempt, DecodeObjectType, FailureRateMonitor, MonitorLogLevel,
};
use proptest::prelude::*;
use serde_json::Value;

const BEAD_ID: &str = "bd-1hi.7";
const ISSUES_JSONL: &str = ".beads/issues.jsonl";
const UNIT_TEST_IDS: [&str; 2] = [
    "test_bd_1hi_7_unit_compliance_gate",
    "prop_bd_1hi_7_structure_compliance",
];
const E2E_TEST_IDS: [&str; 3] = [
    "test_e2e_bd_1hi_7_compliance",
    "test_e2e_bd_1hi_7",
    "test_e2e_monitoring_under_corruption",
];
const LOG_LEVEL_MARKERS: [&str; 4] = ["DEBUG", "INFO", "WARN", "ERROR"];
const LOG_STANDARD_REF: &str = "bd-1fpm";
const REQUIRED_TOKENS: [&str; 10] = [
    "test_bd_1hi_7_unit_compliance_gate",
    "prop_bd_1hi_7_structure_compliance",
    "test_e2e_bd_1hi_7_compliance",
    "test_e2e_bd_1hi_7",
    "test_e2e_monitoring_under_corruption",
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

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("workspace root should be canonicalizable")
}

fn load_issue_description(issue_id: &str) -> String {
    let issues_path = workspace_root().join(ISSUES_JSONL);
    let raw = fs::read_to_string(&issues_path).expect("issues.jsonl should be readable");

    for line in raw.lines().filter(|line| !line.trim().is_empty()) {
        let value: Value = serde_json::from_str(line).expect("issues line should be valid JSON");
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

            return canonical;
        }
    }

    panic!("bead_id={issue_id} not found in {ISSUES_JSONL}");
}

fn evaluate_description(description: &str) -> ComplianceEvaluation {
    let missing_unit_ids = UNIT_TEST_IDS
        .into_iter()
        .filter(|id| !description.contains(id))
        .collect::<Vec<_>>();

    let missing_e2e_ids = E2E_TEST_IDS
        .into_iter()
        .filter(|id| !description.contains(id))
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

#[test]
fn test_bd_1hi_7_unit_compliance_gate() {
    let description = load_issue_description(BEAD_ID);
    let evaluation = evaluate_description(&description);

    assert!(
        evaluation.missing_unit_ids.is_empty(),
        "bead_id={BEAD_ID} case=unit_ids_missing missing={:?}",
        evaluation.missing_unit_ids
    );
    assert!(
        evaluation.missing_e2e_ids.is_empty(),
        "bead_id={BEAD_ID} case=e2e_ids_missing missing={:?}",
        evaluation.missing_e2e_ids
    );
    assert!(
        evaluation.missing_log_levels.is_empty(),
        "bead_id={BEAD_ID} case=logging_levels_missing missing={:?}",
        evaluation.missing_log_levels
    );
    assert!(
        !evaluation.missing_log_standard_ref,
        "bead_id={BEAD_ID} case=logging_standard_missing expected_ref={LOG_STANDARD_REF}"
    );
}

proptest! {
    #[test]
    fn prop_bd_1hi_7_structure_compliance(missing_index in 0usize..REQUIRED_TOKENS.len()) {
        let mut synthetic = format!(
            "## Unit Test Requirements\n- {}\n- {}\n\n## E2E Test\n- {}\n- {}\n- {}\n\n## Logging Requirements\n- DEBUG: stage progress\n- INFO: summary\n- WARN: degraded mode\n- ERROR: terminal failure\n- Reference: {}\n",
            UNIT_TEST_IDS[0],
            UNIT_TEST_IDS[1],
            E2E_TEST_IDS[0],
            E2E_TEST_IDS[1],
            E2E_TEST_IDS[2],
            LOG_STANDARD_REF,
        );

        synthetic = synthetic.replacen(REQUIRED_TOKENS[missing_index], "", 1);
        let evaluation = evaluate_description(&synthetic);
        prop_assert!(
            !evaluation.is_compliant(),
            "bead_id={} case=structure_compliance expected_non_compliant missing_index={} token={}",
            BEAD_ID,
            missing_index,
            REQUIRED_TOKENS[missing_index]
        );
    }
}

#[test]
fn test_e2e_bd_1hi_7_compliance() {
    let description = load_issue_description(BEAD_ID);
    let evaluation = evaluate_description(&description);

    eprintln!(
        "INFO bead_id={BEAD_ID} case=e2e_summary missing_unit_ids={} missing_e2e_ids={} missing_log_levels={} missing_log_standard_ref={}",
        evaluation.missing_unit_ids.len(),
        evaluation.missing_e2e_ids.len(),
        evaluation.missing_log_levels.len(),
        evaluation.missing_log_standard_ref
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

    assert!(
        evaluation.is_compliant(),
        "bead_id={BEAD_ID} case=e2e_compliance_failure evaluation={evaluation:?}"
    );
}

#[test]
fn test_e2e_bd_1hi_7() {
    test_e2e_bd_1hi_7_compliance();
}

#[test]
#[allow(clippy::too_many_lines)]
fn test_e2e_monitoring_under_corruption() {
    let mut monitor = FailureRateMonitor::new();
    let mut first_warn_idx = None;
    let mut first_info_idx = None;
    let mut first_error_idx = None;
    let unrecoverable_index = 420usize;

    for step in 0..600usize {
        let success = if step < 300 {
            true
        } else if step < unrecoverable_index {
            step % 11 != 0
        } else {
            step % 3 != 0
        };

        let update = monitor.update(DecodeAttempt::new(
            100,
            102,
            4096,
            success,
            250,
            DecodeObjectType::WalCommitGroup,
        ));

        for event in update.events {
            match event.level {
                MonitorLogLevel::Debug => {
                    eprintln!(
                        "DEBUG bead_id={BEAD_ID} case=monitor_event step={step} attempts={} failures={} p_upper={} null_rate={} e_value={}",
                        event.attempts,
                        event.failures,
                        event.p_upper,
                        event.null_rate,
                        event.e_value
                    );
                }
                MonitorLogLevel::Warn => {
                    if first_warn_idx.is_none() {
                        first_warn_idx = Some(step);
                    }
                    eprintln!(
                        "WARN bead_id={BEAD_ID} case=monitor_warn step={step} attempts={} failures={} p_upper={} null_rate={} e_value={}",
                        event.attempts,
                        event.failures,
                        event.p_upper,
                        event.null_rate,
                        event.e_value
                    );
                }
                MonitorLogLevel::Info => {
                    if first_info_idx.is_none() {
                        first_info_idx = Some(step);
                    }
                    eprintln!(
                        "INFO bead_id={BEAD_ID} case=monitor_alert step={step} attempts={} failures={} p_upper={} null_rate={} e_value={}",
                        event.attempts,
                        event.failures,
                        event.p_upper,
                        event.null_rate,
                        event.e_value
                    );
                }
                MonitorLogLevel::Error => {
                    if first_error_idx.is_none() {
                        first_error_idx = Some(step);
                    }
                    eprintln!(
                        "ERROR bead_id={BEAD_ID} case=monitor_error step={step} attempts={} failures={} p_upper={} null_rate={} e_value={}",
                        event.attempts,
                        event.failures,
                        event.p_upper,
                        event.null_rate,
                        event.e_value
                    );
                }
            }
        }
    }

    assert!(
        first_warn_idx.is_some(),
        "bead_id={BEAD_ID} case=missing_warn_event"
    );
    assert!(
        first_info_idx.is_some(),
        "bead_id={BEAD_ID} case=missing_info_alert"
    );
    assert!(
        first_error_idx.is_some(),
        "bead_id={BEAD_ID} case=missing_error_event"
    );

    let first_info = first_info_idx.expect("checked is_some");
    assert!(
        first_info < unrecoverable_index,
        "bead_id={BEAD_ID} case=late_detection alert_index={first_info} unrecoverable_index={unrecoverable_index}"
    );

    let redundancy_bump = monitor.recommended_redundancy_bump(DecodeAttempt::new(
        100,
        102,
        4096,
        false,
        250,
        DecodeObjectType::WalCommitGroup,
    ));
    assert!(
        redundancy_bump >= 1,
        "bead_id={BEAD_ID} case=adaptive_redundancy_not_engaged bump={redundancy_bump}"
    );
}
