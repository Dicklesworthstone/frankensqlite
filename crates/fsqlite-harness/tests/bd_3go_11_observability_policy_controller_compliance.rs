use std::fs;
use std::path::{Path, PathBuf};

use fsqlite_mvcc::{
    AmsEvidenceLedger, AmsWindowCollector, AmsWindowCollectorConfig, DEFAULT_AMS_R,
    DEFAULT_HEAVY_HITTER_K,
};
use fsqlite_wal::recovery_compaction::{CompactionMdpState, CompactionPolicy};
use proptest::prelude::proptest;
use proptest::test_runner::TestCaseError;
use serde_json::Value;

const BEAD_ID: &str = "bd-3go.11";
const ISSUES_JSONL: &str = ".beads/issues.jsonl";
const LOG_STANDARD_REF: &str = "bd-1fpm";
const UNIT_TEST_IDS: [&str; 7] = [
    "test_bd_3go_11_unit_compliance_gate",
    "prop_bd_3go_11_structure_compliance",
    "test_evidence_entry_has_required_fields",
    "test_evidence_entry_deterministic_ordering",
    "test_commit_ledger_includes_contention_state",
    "test_ledger_bounded_size",
    "test_policy_controller_deterministic_in_lab",
];
const E2E_TEST_IDS: [&str; 2] = ["test_e2e_bd_3go_11_compliance", "test_e2e_bd_3go_11"];
const LOG_LEVEL_MARKERS: [&str; 4] = ["DEBUG", "INFO", "WARN", "ERROR"];
const SPEC_MARKERS: [&str; 12] = [
    "Task inspector",
    "Evidence ledger",
    "PolicyController",
    "argmin E[L(a, state) | evidence]",
    "fsqlite.auto_tune",
    "fsqlite.profile",
    "fsqlite.bg_cpu_max",
    "remote_max_in_flight",
    "commit_encode_max",
    "hysteresis",
    "BOCPD",
    "ASUPERSYNC_TEST_ARTIFACTS_DIR",
];
const REQUIRED_TOKENS: [&str; 26] = [
    "test_bd_3go_11_unit_compliance_gate",
    "prop_bd_3go_11_structure_compliance",
    "test_evidence_entry_has_required_fields",
    "test_evidence_entry_deterministic_ordering",
    "test_commit_ledger_includes_contention_state",
    "test_ledger_bounded_size",
    "test_policy_controller_deterministic_in_lab",
    "test_e2e_bd_3go_11_compliance",
    "test_e2e_bd_3go_11",
    "DEBUG",
    "INFO",
    "WARN",
    "ERROR",
    "bd-1fpm",
    "Task inspector",
    "Evidence ledger",
    "PolicyController",
    "argmin E[L(a, state) | evidence]",
    "fsqlite.auto_tune",
    "fsqlite.profile",
    "fsqlite.bg_cpu_max",
    "remote_max_in_flight",
    "commit_encode_max",
    "hysteresis",
    "BOCPD",
    "ASUPERSYNC_TEST_ARTIFACTS_DIR",
];
const POLICY_TRACE_START_NS: u64 = 1_700_000_000_000_000_000;

#[derive(Debug, PartialEq, Eq)]
#[allow(clippy::struct_field_names)]
struct ComplianceEvaluation {
    missing_unit_ids: Vec<&'static str>,
    missing_e2e_ids: Vec<&'static str>,
    missing_log_levels: Vec<&'static str>,
    missing_log_standard_ref: bool,
    missing_spec_markers: Vec<&'static str>,
}

impl ComplianceEvaluation {
    fn is_compliant(&self) -> bool {
        self.missing_unit_ids.is_empty()
            && self.missing_e2e_ids.is_empty()
            && self.missing_log_levels.is_empty()
            && !self.missing_log_standard_ref
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

fn load_source(relative_path: &str) -> Result<String, String> {
    let path = workspace_root()?.join(relative_path);
    fs::read_to_string(&path).map_err(|error| {
        format!(
            "source_read_failed path={} error={error}",
            path.as_path().display()
        )
    })
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
        missing_log_standard_ref: !description.contains(LOG_STANDARD_REF),
        missing_spec_markers,
    }
}

fn synthetic_compliant_description() -> String {
    let mut text = String::from("## Unit Test Requirements\n");
    for id in UNIT_TEST_IDS {
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

    text.push_str("\n## Logging Requirements\n");
    text.push_str("- DEBUG: stage-level progress\n");
    text.push_str("- INFO: summary counters\n");
    text.push_str("- WARN: degraded-mode/retry conditions\n");
    text.push_str("- ERROR: terminal failure diagnostics\n");
    text.push_str("- Reference: ");
    text.push_str(LOG_STANDARD_REF);
    text.push('\n');

    text.push_str("\n## Spec Markers\n");
    for marker in SPEC_MARKERS {
        text.push_str("- ");
        text.push_str(marker);
        text.push('\n');
    }

    text
}

fn build_ams_evidence_ledger() -> AmsEvidenceLedger {
    let config = AmsWindowCollectorConfig {
        r: DEFAULT_AMS_R,
        db_epoch: 7,
        regime_id: 11,
        window_width_ticks: 32,
        track_exact_m2: true,
        track_heavy_hitters: true,
        heavy_hitter_k: DEFAULT_HEAVY_HITTER_K,
        estimate_zipf: true,
    };
    let mut collector = AmsWindowCollector::new(config, 0);

    for tick in 0_u64..96 {
        let write_set = [tick % 11, (tick.saturating_mul(5) + 1) % 17, 42];
        let _closed_window = collector.observe_commit_attempt(tick, &write_set);
    }

    collector.force_flush(96).to_evidence_ledger()
}

fn policy_trace_fingerprint() -> Vec<String> {
    let mut policy = CompactionPolicy::new();
    let states = [
        (
            CompactionMdpState {
                space_amp_bucket: 0,
                read_regime: 0,
                write_regime: 0,
                compaction_debt: 0,
            },
            "idle workload stays deferred",
        ),
        (
            CompactionMdpState {
                space_amp_bucket: 2,
                read_regime: 1,
                write_regime: 0,
                compaction_debt: 1,
            },
            "space amplification elevated",
        ),
        (
            CompactionMdpState {
                space_amp_bucket: 3,
                read_regime: 2,
                write_regime: 2,
                compaction_debt: 2,
            },
            "BOCPD regime shift under heavy writes",
        ),
    ];

    for (index, (state, reason)) in states.iter().enumerate() {
        let timestamp_ns = POLICY_TRACE_START_NS + index as u64;
        let action = policy.recommend(state);
        policy.record_decision(timestamp_ns, *state, action, reason);
    }

    policy
        .evidence_ledger()
        .iter()
        .map(|entry| {
            format!(
                "{}|{}|{}|{}|{}|{:?}|{}",
                entry.timestamp_ns,
                entry.state.space_amp_bucket,
                entry.state.read_regime,
                entry.state.write_regime,
                entry.state.compaction_debt,
                entry.action,
                entry.reason
            )
        })
        .collect::<Vec<_>>()
}

#[test]
fn test_bd_3go_11_unit_compliance_gate() -> Result<(), String> {
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
    fn prop_bd_3go_11_structure_compliance(missing_index in 0usize..REQUIRED_TOKENS.len()) {
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
fn test_evidence_entry_has_required_fields() -> Result<(), String> {
    let mut policy = CompactionPolicy::new();
    let state = CompactionMdpState {
        space_amp_bucket: 2,
        read_regime: 1,
        write_regime: 0,
        compaction_debt: 1,
    };
    let action = policy.recommend(&state);
    policy.record_decision(
        POLICY_TRACE_START_NS,
        state,
        action,
        "structured-policy-evidence",
    );

    let Some(entry) = policy.evidence_ledger().first() else {
        return Err("bead_id=bd-3go.11 case=evidence_entry_missing".to_owned());
    };

    if entry.timestamp_ns != POLICY_TRACE_START_NS {
        return Err(format!(
            "bead_id={BEAD_ID} case=evidence_timestamp_mismatch expected={} observed={}",
            POLICY_TRACE_START_NS, entry.timestamp_ns
        ));
    }
    if entry.reason.trim().is_empty() {
        return Err(format!("bead_id={BEAD_ID} case=evidence_reason_empty"));
    }

    let conflict_model_src = load_source("crates/fsqlite-mvcc/src/conflict_model.rs")?;
    let required_fields = [
        "pub regime_id: u64",
        "pub m2_hat: Option<f64>",
        "pub p_eff_hat: f64",
    ];
    for field in required_fields {
        if !conflict_model_src.contains(field) {
            return Err(format!(
                "bead_id={BEAD_ID} case=contention_field_missing field={field}"
            ));
        }
    }

    Ok(())
}

#[test]
fn test_evidence_entry_deterministic_ordering() -> Result<(), String> {
    let first = build_ams_evidence_ledger();
    let second = build_ams_evidence_ledger();
    if first != second {
        return Err(format!(
            "bead_id={BEAD_ID} case=nondeterministic_ledger first={first:?} second={second:?}"
        ));
    }

    for pair in first.heavy_hitters.windows(2) {
        let left = &pair[0];
        let right = &pair[1];
        let sorted = left.count_hat > right.count_hat
            || (left.count_hat == right.count_hat && left.pgno <= right.pgno);
        if !sorted {
            return Err(format!(
                "bead_id={BEAD_ID} case=heavy_hitter_order_violation left={left:?} right={right:?}"
            ));
        }
    }

    Ok(())
}

#[test]
fn test_commit_ledger_includes_contention_state() -> Result<(), String> {
    let ledger = build_ams_evidence_ledger();
    if ledger.regime_id != 11 {
        return Err(format!(
            "bead_id={BEAD_ID} case=regime_id_mismatch expected=11 observed={}",
            ledger.regime_id
        ));
    }
    if ledger.m2_hat.is_none() {
        return Err(format!(
            "bead_id={BEAD_ID} case=missing_m2_hat ledger={ledger:?}"
        ));
    }
    if !ledger.p_eff_hat.is_finite() {
        return Err(format!(
            "bead_id={BEAD_ID} case=invalid_p_eff_hat p_eff_hat={}",
            ledger.p_eff_hat
        ));
    }
    if ledger.heavy_hitters.is_empty() {
        return Err(format!(
            "bead_id={BEAD_ID} case=missing_heavy_hitters ledger={ledger:?}"
        ));
    }

    let repair_symbols_src = load_source("crates/fsqlite-core/src/repair_symbols.rs")?;
    let required_markers = [
        "record_overhead_retune_with_context",
        "regime_id",
        "p_upper",
    ];
    for marker in required_markers {
        if !repair_symbols_src.contains(marker) {
            return Err(format!(
                "bead_id={BEAD_ID} case=repair_symbols_marker_missing marker={marker}"
            ));
        }
    }

    Ok(())
}

#[test]
fn test_ledger_bounded_size() -> Result<(), String> {
    let config = AmsWindowCollectorConfig {
        r: DEFAULT_AMS_R,
        db_epoch: 99,
        regime_id: 3,
        window_width_ticks: 64,
        track_exact_m2: false,
        track_heavy_hitters: true,
        heavy_hitter_k: 32,
        estimate_zipf: false,
    };
    let mut collector = AmsWindowCollector::new(config, 0);
    for tick in 0_u64..10_000 {
        let write_set = [tick % 97, tick % 31, tick % 17];
        let _closed_window = collector.observe_commit_attempt(tick, &write_set);
    }
    let ledger = collector.force_flush(10_000).to_evidence_ledger();

    if ledger.heavy_hitters.len() > 32 {
        return Err(format!(
            "bead_id={BEAD_ID} case=heavy_hitter_bound_exceeded len={} bound=32",
            ledger.heavy_hitters.len()
        ));
    }

    Ok(())
}

#[test]
fn test_policy_controller_deterministic_in_lab() -> Result<(), String> {
    let first = policy_trace_fingerprint();
    let second = policy_trace_fingerprint();
    if first != second {
        return Err(format!(
            "bead_id={BEAD_ID} case=nondeterministic_policy_trace first={first:?} second={second:?}"
        ));
    }
    Ok(())
}

#[test]
fn test_e2e_bd_3go_11_compliance() -> Result<(), String> {
    let description = load_issue_description(BEAD_ID)?;
    let evaluation = evaluate_description(&description);

    let ledger_once = build_ams_evidence_ledger();
    let ledger_twice = build_ams_evidence_ledger();
    let policy_once = policy_trace_fingerprint();
    let policy_twice = policy_trace_fingerprint();

    eprintln!(
        "DEBUG bead_id={BEAD_ID} case=e2e_start heavy_hitters={} policy_events={} deterministic_ledger={}",
        ledger_once.heavy_hitters.len(),
        policy_once.len(),
        ledger_once == ledger_twice
    );
    eprintln!(
        "INFO bead_id={BEAD_ID} case=e2e_summary missing_unit_ids={} missing_e2e_ids={} missing_logs={} missing_spec_markers={}",
        evaluation.missing_unit_ids.len(),
        evaluation.missing_e2e_ids.len(),
        evaluation.missing_log_levels.len(),
        evaluation.missing_spec_markers.len()
    );
    eprintln!(
        "WARN bead_id={BEAD_ID} case=e2e_diagnostic policy_trace_deterministic={} log_standard_present={}",
        policy_once == policy_twice,
        !evaluation.missing_log_standard_ref
    );
    eprintln!(
        "ERROR bead_id={BEAD_ID} case=e2e_guard compliance_ok={} policy_events={} replay_cmd=\"cargo test -p fsqlite-harness test_e2e_bd_3go_11 -- --nocapture\"",
        evaluation.is_compliant(),
        policy_once.len()
    );

    if !evaluation.is_compliant() {
        return Err(format!(
            "bead_id={BEAD_ID} case=e2e_compliance_failure evaluation={evaluation:?}"
        ));
    }
    if ledger_once != ledger_twice {
        return Err(format!(
            "bead_id={BEAD_ID} case=e2e_nondeterministic_ledger first={ledger_once:?} second={ledger_twice:?}"
        ));
    }
    if policy_once != policy_twice {
        return Err(format!(
            "bead_id={BEAD_ID} case=e2e_nondeterministic_policy first={policy_once:?} second={policy_twice:?}"
        ));
    }

    Ok(())
}

#[test]
fn test_e2e_bd_3go_11() -> Result<(), String> {
    test_e2e_bd_3go_11_compliance()
}
