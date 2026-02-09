use std::fs;
use std::path::{Path, PathBuf};

use fsqlite_mvcc::{
    InstrumentationCounters, effective_w_index_multiplier, effective_w_leaf_split,
    effective_w_root_split, p_abort_attempt, p_drift, tps_estimate,
};
use proptest::prelude::proptest;
use proptest::test_runner::TestCaseError;
use serde_json::Value;

const BEAD_ID: &str = "bd-25q8";
const ISSUES_JSONL: &str = ".beads/issues.jsonl";
const LOG_STANDARD_REF: &str = "bd-1fpm";
const UNIT_TEST_IDS: [&str; 2] = [
    "test_bd_25q8_unit_compliance_gate",
    "prop_bd_25q8_structure_compliance",
];
const E2E_TEST_IDS: [&str; 1] = ["test_e2e_bd_25q8_compliance"];
const LOG_LEVEL_MARKERS: [&str; 4] = ["DEBUG", "INFO", "WARN", "ERROR"];
const REQUIRED_TOKENS: [&str; 8] = [
    "test_bd_25q8_unit_compliance_gate",
    "prop_bd_25q8_structure_compliance",
    "test_e2e_bd_25q8_compliance",
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
    }
}

#[test]
fn test_bd_25q8_unit_compliance_gate() -> Result<(), String> {
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

    Ok(())
}

proptest! {
    #[test]
    fn prop_bd_25q8_structure_compliance(missing_index in 0usize..REQUIRED_TOKENS.len()) {
        let mut synthetic = format!(
            "## Unit Test Requirements\n- {}\n- {}\n\n## E2E Test\n- {}\n\n## Logging Requirements\n- DEBUG: stage progress\n- INFO: summary\n- WARN: degraded mode\n- ERROR: terminal failure\n- Reference: {}\n",
            UNIT_TEST_IDS[0],
            UNIT_TEST_IDS[1],
            E2E_TEST_IDS[0],
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

// ---------------------------------------------------------------------------
// Functional smoke tests for §18.5-18.8 API
// ---------------------------------------------------------------------------

#[test]
fn test_btree_hotspot_effective_w_smoke() {
    // Leaf split: base=1, split=3 → 1-1+3 = 3
    assert_eq!(effective_w_leaf_split(1, 3), 3);
    // Root split: base=2, split=5 → 2-1+5 = 6
    assert_eq!(effective_w_root_split(2, 5), 6);
    // Index multiplier: base=2, K=3, split=1 → 2*(1+3)*1 = 8
    assert_eq!(effective_w_index_multiplier(2, 3, 1), 8);
}

#[test]
fn test_p_drift_and_tps_smoke() {
    let pd = p_drift(4, 0.01);
    assert!(pd > 0.0 && pd < 1.0, "p_drift in (0,1): {pd}");

    let pa = p_abort_attempt(pd, 0.5);
    assert!(pa >= 0.0 && pa <= pd, "p_abort <= p_drift: {pa} <= {pd}");

    let tps = tps_estimate(4, pa, 0.005);
    assert!(tps > 0.0, "tps positive: {tps}");
}

#[test]
fn test_instrumentation_roundtrip_smoke() {
    let mut counters = InstrumentationCounters::default();
    counters.record_commit(5, 3);
    counters.record_commit(10, 3);
    counters.record_conflict();
    counters.record_merge_rebase();

    let m2 = counters.pages_per_commit_m2().expect("should have commits");
    assert!(m2 > 0.0, "m2 > 0: {m2}");

    let f = counters.f_merge().expect("should have conflicts");
    assert!((f - 1.0).abs() < 1e-9, "f_merge=1.0 when all merged: {f}");
}

#[test]
#[allow(clippy::cast_precision_loss)]
fn test_end_to_end_conflict_model_chain() {
    // End-to-end: compute effective W → M2 → p_drift → P_abort → TPS
    let n: u64 = 8;
    let base_w: u64 = 5;
    let total_pages: f64 = 100_000.0;

    // With index multiplier (K=3, no split)
    let eff_w = effective_w_index_multiplier(base_w, 3, 1);
    assert_eq!(eff_w, 20);

    // Uniform M2 = W²/P
    #[allow(clippy::cast_precision_loss)]
    let m2 = (eff_w * eff_w) as f64 / total_pages;

    // p_drift
    let pd = p_drift(n, m2);
    assert!(pd > 0.0 && pd < 1.0);

    // P_abort with 50% merge success
    let pa = p_abort_attempt(pd, 0.50);

    // TPS
    let tps = tps_estimate(n, pa, 0.002);
    assert!(tps > 0.0);

    eprintln!(
        "INFO bead_id={BEAD_ID} case=e2e_chain eff_w={eff_w} m2={m2:.6} p_drift={pd:.6} p_abort={pa:.6} tps={tps:.1}"
    );
}

#[test]
fn test_e2e_bd_25q8_compliance() -> Result<(), String> {
    let description = load_issue_description(BEAD_ID)?;
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

    if !evaluation.is_compliant() {
        return Err(format!(
            "bead_id={BEAD_ID} case=e2e_compliance_failure evaluation={evaluation:?}"
        ));
    }

    Ok(())
}
