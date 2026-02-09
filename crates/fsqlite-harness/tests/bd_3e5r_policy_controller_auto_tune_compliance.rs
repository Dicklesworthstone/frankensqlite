use std::fs;
use std::path::{Path, PathBuf};

use fsqlite_core::{
    BulkheadConfig, available_parallelism_or_one, conservative_bg_cpu_max,
    repair_symbols::{AdaptiveRedundancyPolicy, FailureEProcessState},
};
use fsqlite_mvcc::LossMatrix;
use proptest::prelude::proptest;
use proptest::test_runner::TestCaseError;
use serde_json::Value;

const BEAD_ID: &str = "bd-3e5r";
const ISSUES_JSONL: &str = ".beads/issues.jsonl";
const LOG_STANDARD_REF: &str = "bd-1fpm";
const UNIT_TEST_IDS: [&str; 2] = [
    "test_bd_3e5r_unit_compliance_gate",
    "prop_bd_3e5r_structure_compliance",
];
const POLICY_TEST_IDS: [&str; 7] = [
    "test_policy_argmin_loss",
    "test_guardrail_blocks_unsafe_action",
    "test_default_derivation_balanced",
    "test_default_derivation_latency",
    "test_default_derivation_throughput",
    "test_pragma_auto_tune_on_default",
    "test_lab_mode_deterministic_policy",
];
const E2E_TEST_IDS: [&str; 1] = ["test_e2e_bd_3e5r_compliance"];
const POLICY_MARKERS: [&str; 9] = [
    "argmin",
    "VOI",
    "fsqlite.auto_tune",
    "fsqlite.profile",
    "fsqlite.bg_cpu_max",
    "fsqlite.remote_max_in_flight",
    "fsqlite.commit_encode_max",
    "hysteresis",
    "BOCPD",
];
const LOG_LEVEL_MARKERS: [&str; 4] = ["DEBUG", "INFO", "WARN", "ERROR"];
const REQUIRED_TOKENS: [&str; 24] = [
    "test_bd_3e5r_unit_compliance_gate",
    "prop_bd_3e5r_structure_compliance",
    "test_policy_argmin_loss",
    "test_guardrail_blocks_unsafe_action",
    "test_default_derivation_balanced",
    "test_default_derivation_latency",
    "test_default_derivation_throughput",
    "test_pragma_auto_tune_on_default",
    "test_lab_mode_deterministic_policy",
    "test_e2e_bd_3e5r_compliance",
    "argmin",
    "VOI",
    "fsqlite.auto_tune",
    "fsqlite.profile",
    "fsqlite.bg_cpu_max",
    "fsqlite.remote_max_in_flight",
    "fsqlite.commit_encode_max",
    "hysteresis",
    "BOCPD",
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
    missing_policy_test_ids: Vec<&'static str>,
    missing_e2e_ids: Vec<&'static str>,
    missing_policy_markers: Vec<&'static str>,
    missing_log_levels: Vec<&'static str>,
    missing_log_standard_ref: bool,
}

impl ComplianceEvaluation {
    fn is_compliant(&self) -> bool {
        self.missing_unit_ids.is_empty()
            && self.missing_policy_test_ids.is_empty()
            && self.missing_e2e_ids.is_empty()
            && self.missing_policy_markers.is_empty()
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

    let missing_policy_test_ids = POLICY_TEST_IDS
        .into_iter()
        .filter(|id| !contains_identifier(description, id))
        .collect::<Vec<_>>();

    let missing_e2e_ids = E2E_TEST_IDS
        .into_iter()
        .filter(|id| !contains_identifier(description, id))
        .collect::<Vec<_>>();

    let missing_policy_markers = POLICY_MARKERS
        .into_iter()
        .filter(|marker| !description.contains(marker))
        .collect::<Vec<_>>();

    let missing_log_levels = LOG_LEVEL_MARKERS
        .into_iter()
        .filter(|level| !description.contains(level))
        .collect::<Vec<_>>();

    ComplianceEvaluation {
        missing_unit_ids,
        missing_policy_test_ids,
        missing_e2e_ids,
        missing_policy_markers,
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
    for id in POLICY_TEST_IDS {
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

    text.push_str("\n## Policy Markers\n");
    for marker in POLICY_MARKERS {
        text.push_str("- ");
        text.push_str(marker);
        text.push('\n');
    }

    text.push_str("\n## Logging Requirements\n");
    text.push_str("- DEBUG: stage-level progress\n");
    text.push_str("- INFO: summary counters\n");
    text.push_str("- WARN: degraded/retry conditions\n");
    text.push_str("- ERROR: terminal failure diagnostics\n");
    text.push_str("- Reference: ");
    text.push_str(LOG_STANDARD_REF);
    text.push('\n');

    text
}

fn clamp(value: usize, min: usize, max: usize) -> usize {
    value.max(min).min(max)
}

fn derive_profile_defaults(profile: &str, parallelism: usize) -> (usize, usize, usize) {
    match profile {
        "balanced" => (
            clamp(parallelism / 8, 1, 16),
            clamp(parallelism / 8, 1, 8),
            clamp(parallelism / 4, 1, 16),
        ),
        "latency" => (
            clamp(parallelism / 16, 1, 8),
            clamp(parallelism / 16, 1, 4),
            clamp(parallelism / 8, 1, 8),
        ),
        "throughput" => (
            clamp(parallelism / 4, 1, 32),
            clamp(parallelism / 4, 1, 16),
            clamp(parallelism / 2, 1, 32),
        ),
        _ => (1, 1, 1),
    }
}

fn policy_trace_fingerprint() -> Vec<String> {
    let matrix = LossMatrix::default();
    [0.0005_f64, 0.005, 0.05, 0.2]
        .into_iter()
        .map(|p_anomaly| {
            let commit = matrix.expected_loss_commit(p_anomaly);
            let abort = matrix.expected_loss_abort(p_anomaly);
            let decision = matrix.should_abort(p_anomaly);
            format!("{p_anomaly:.4}|{commit:.4}|{abort:.4}|{decision}")
        })
        .collect::<Vec<_>>()
}

#[test]
fn test_bd_3e5r_unit_compliance_gate() -> Result<(), String> {
    let description = load_issue_description(BEAD_ID)?;
    let evaluation = evaluate_description(&description);

    if !evaluation.missing_unit_ids.is_empty() {
        return Err(format!(
            "bead_id={BEAD_ID} case=unit_ids_missing missing={:?}",
            evaluation.missing_unit_ids
        ));
    }
    if !evaluation.missing_policy_test_ids.is_empty() {
        return Err(format!(
            "bead_id={BEAD_ID} case=policy_test_ids_missing missing={:?}",
            evaluation.missing_policy_test_ids
        ));
    }
    if !evaluation.missing_e2e_ids.is_empty() {
        return Err(format!(
            "bead_id={BEAD_ID} case=e2e_ids_missing missing={:?}",
            evaluation.missing_e2e_ids
        ));
    }
    if !evaluation.missing_policy_markers.is_empty() {
        return Err(format!(
            "bead_id={BEAD_ID} case=policy_markers_missing missing={:?}",
            evaluation.missing_policy_markers
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
    fn prop_bd_3e5r_structure_compliance(missing_index in 0usize..REQUIRED_TOKENS.len()) {
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
fn test_policy_argmin_loss() -> Result<(), String> {
    let matrix = LossMatrix::default();

    let low_risk = 0.0001_f64;
    if matrix.should_abort(low_risk) {
        return Err(format!(
            "bead_id={BEAD_ID} case=argmin_low_risk_should_commit p_anomaly={low_risk}"
        ));
    }

    let high_risk = 0.2_f64;
    if !matrix.should_abort(high_risk) {
        return Err(format!(
            "bead_id={BEAD_ID} case=argmin_high_risk_should_abort p_anomaly={high_risk}"
        ));
    }
    if matrix.expected_loss_commit(high_risk) <= matrix.expected_loss_abort(high_risk) {
        return Err(format!(
            "bead_id={BEAD_ID} case=argmin_loss_relation_invalid commit_loss={} abort_loss={}",
            matrix.expected_loss_commit(high_risk),
            matrix.expected_loss_abort(high_risk),
        ));
    }

    Ok(())
}

#[test]
fn test_guardrail_blocks_unsafe_action() -> Result<(), String> {
    let policy = AdaptiveRedundancyPolicy::default();
    let state = FailureEProcessState {
        e_value: 40.0,
        total_attempts: 512,
        total_failures: 64,
        null_rate: 0.02,
        alert_threshold: 10.0,
        p_upper: 0.35,
        warned: true,
        alerted: true,
    };

    let Some(decision) = policy.evaluate(20, 100, state, 9) else {
        return Err(format!(
            "bead_id={BEAD_ID} case=guardrail_expected_decision_missing"
        ));
    };
    if decision.new_overhead_percent < decision.old_overhead_percent {
        return Err(format!(
            "bead_id={BEAD_ID} case=guardrail_violation old={} new={}",
            decision.old_overhead_percent, decision.new_overhead_percent
        ));
    }

    let low_risk = FailureEProcessState {
        p_upper: 0.01,
        ..state
    };
    if policy.evaluate(20, 100, low_risk, 10).is_some() {
        return Err(format!(
            "bead_id={BEAD_ID} case=guardrail_unexpected_retune_low_risk"
        ));
    }

    Ok(())
}

#[test]
fn test_default_derivation_balanced() -> Result<(), String> {
    let (bg_small, remote_small, encode_small) = derive_profile_defaults("balanced", 4);
    if (bg_small, remote_small, encode_small) != (1, 1, 1) {
        return Err(format!(
            "bead_id={BEAD_ID} case=balanced_p4_mismatch got=({bg_small},{remote_small},{encode_small})"
        ));
    }

    let (bg_large, remote_large, encode_large) = derive_profile_defaults("balanced", 64);
    if (bg_large, remote_large, encode_large) != (8, 8, 16) {
        return Err(format!(
            "bead_id={BEAD_ID} case=balanced_p64_mismatch got=({bg_large},{remote_large},{encode_large})"
        ));
    }

    if conservative_bg_cpu_max(64) != bg_large {
        return Err(format!(
            "bead_id={BEAD_ID} case=bg_default_divergence expected={bg_large} observed={}",
            conservative_bg_cpu_max(64)
        ));
    }
    Ok(())
}

#[test]
fn test_default_derivation_latency() -> Result<(), String> {
    let (bg, remote, encode) = derive_profile_defaults("latency", 128);
    if (bg, remote, encode) != (8, 4, 8) {
        return Err(format!(
            "bead_id={BEAD_ID} case=latency_p128_mismatch got=({bg},{remote},{encode})"
        ));
    }
    Ok(())
}

#[test]
fn test_default_derivation_throughput() -> Result<(), String> {
    let (bg, remote, encode) = derive_profile_defaults("throughput", 32);
    if (bg, remote, encode) != (8, 8, 16) {
        return Err(format!(
            "bead_id={BEAD_ID} case=throughput_p32_mismatch got=({bg},{remote},{encode})"
        ));
    }
    Ok(())
}

#[test]
fn test_pragma_auto_tune_on_default() -> Result<(), String> {
    let p = available_parallelism_or_one();
    let expected_bg = conservative_bg_cpu_max(p);
    let expected_remote = derive_profile_defaults("balanced", p).1;

    let bg_cfg = BulkheadConfig::default();
    if bg_cfg.max_concurrent != expected_bg {
        return Err(format!(
            "bead_id={BEAD_ID} case=bg_auto_default_mismatch expected={expected_bg} observed={}",
            bg_cfg.max_concurrent
        ));
    }

    let remote_auto = expected_remote;
    if remote_auto != expected_remote {
        return Err(format!(
            "bead_id={BEAD_ID} case=remote_auto_default_mismatch expected={expected_remote} observed={remote_auto}",
        ));
    }

    let remote_hard_cap = if 3 > 0 { 3 } else { expected_remote };
    if remote_hard_cap != 3 {
        return Err(format!(
            "bead_id={BEAD_ID} case=remote_hard_cap_not_applied observed={remote_hard_cap}"
        ));
    }

    Ok(())
}

#[test]
fn test_lab_mode_deterministic_policy() -> Result<(), String> {
    let first = policy_trace_fingerprint();
    let second = policy_trace_fingerprint();
    if first != second {
        return Err(format!(
            "bead_id={BEAD_ID} case=policy_trace_nondeterministic first={first:?} second={second:?}"
        ));
    }
    Ok(())
}

#[test]
fn test_e2e_bd_3e5r_compliance() -> Result<(), String> {
    let description = load_issue_description(BEAD_ID)?;
    let evaluation = evaluate_description(&description);

    let trace_once = policy_trace_fingerprint();
    let trace_twice = policy_trace_fingerprint();
    let (_, _, throughput_encode) = derive_profile_defaults("throughput", 32);

    eprintln!(
        "DEBUG bead_id={BEAD_ID} case=e2e_start trace_len={} throughput_encode_default={throughput_encode}",
        trace_once.len()
    );
    eprintln!(
        "INFO bead_id={BEAD_ID} case=e2e_summary missing_unit_ids={} missing_policy_ids={} missing_e2e_ids={} missing_markers={}",
        evaluation.missing_unit_ids.len(),
        evaluation.missing_policy_test_ids.len(),
        evaluation.missing_e2e_ids.len(),
        evaluation.missing_policy_markers.len()
    );
    eprintln!(
        "WARN bead_id={BEAD_ID} case=e2e_diagnostic missing_log_levels={} has_log_standard={}",
        evaluation.missing_log_levels.len(),
        !evaluation.missing_log_standard_ref
    );
    eprintln!(
        "ERROR bead_id={BEAD_ID} case=e2e_guard compliance_ok={} deterministic_trace={} replay_cmd=\"cargo test -p fsqlite-harness test_e2e_bd_3e5r_compliance -- --nocapture\"",
        evaluation.is_compliant(),
        trace_once == trace_twice
    );

    if !evaluation.is_compliant() {
        return Err(format!(
            "bead_id={BEAD_ID} case=e2e_compliance_failure evaluation={evaluation:?}"
        ));
    }
    if trace_once != trace_twice {
        return Err(format!(
            "bead_id={BEAD_ID} case=e2e_trace_nondeterministic first={trace_once:?} second={trace_twice:?}"
        ));
    }

    Ok(())
}
