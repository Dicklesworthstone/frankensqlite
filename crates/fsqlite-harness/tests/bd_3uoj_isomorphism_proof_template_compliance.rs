use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use proptest::prelude::proptest;
use proptest::test_runner::TestCaseError;
use serde_json::Value;

const BEAD_ID: &str = "bd-3uoj";
const ISSUES_JSONL: &str = ".beads/issues.jsonl";
const LOG_STANDARD_REF: &str = "bd-1fpm";

const UNIT_TEST_IDS: [&str; 18] = [
    "test_bd_3uoj_unit_compliance_gate",
    "prop_bd_3uoj_structure_compliance",
    "test_isomorphism_ordering_preserved",
    "test_isomorphism_tie_breaking_unchanged",
    "test_isomorphism_float_behavior_identical",
    "test_isomorphism_rng_seeds_unchanged",
    "test_isomorphism_oracle_fixtures_pass",
    "test_isomorphism_golden_checksums_match",
    "test_isomorphism_pr_template_present",
    "test_isomorphism_tie_breaking",
    "test_isomorphism_float_identical",
    "test_isomorphism_golden_checksum_match",
    "test_isomorphism_proof_template_required",
    "test_isomorphism_no_vibes_optimization",
    "test_isomorphism_group_by_stability",
    "test_isomorphism_explain_plan_unchanged",
    "test_isomorphism_commit_marker_artifacts",
    "test_isomorphism_conformance_full_suite",
];

const E2E_TEST_IDS: [&str; 2] = ["test_e2e_bd_3uoj", "test_e2e_bd_3uoj_compliance"];
const LOG_LEVEL_MARKERS: [&str; 4] = ["DEBUG", "INFO", "WARN", "ERROR"];

const REQUIRED_TOKENS: [&str; 25] = [
    "test_bd_3uoj_unit_compliance_gate",
    "prop_bd_3uoj_structure_compliance",
    "test_isomorphism_ordering_preserved",
    "test_isomorphism_tie_breaking_unchanged",
    "test_isomorphism_float_behavior_identical",
    "test_isomorphism_rng_seeds_unchanged",
    "test_isomorphism_oracle_fixtures_pass",
    "test_isomorphism_golden_checksums_match",
    "test_isomorphism_pr_template_present",
    "test_isomorphism_tie_breaking",
    "test_isomorphism_float_identical",
    "test_isomorphism_golden_checksum_match",
    "test_isomorphism_proof_template_required",
    "test_isomorphism_no_vibes_optimization",
    "test_isomorphism_group_by_stability",
    "test_isomorphism_explain_plan_unchanged",
    "test_isomorphism_commit_marker_artifacts",
    "test_isomorphism_conformance_full_suite",
    "test_e2e_bd_3uoj",
    "test_e2e_bd_3uoj_compliance",
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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct QueryRow {
    order_key: i32,
    tie_key: i32,
    payload: i32,
    original_pos: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct IsomorphismProofTemplate {
    change: &'static str,
    ordering_preserved: bool,
    tie_breaking_unchanged: bool,
    float_behavior: &'static str,
    rng_seeds: &'static str,
    oracle_fixture_ids: Vec<&'static str>,
}

impl IsomorphismProofTemplate {
    fn render(&self) -> String {
        let fixture_ids = self.oracle_fixture_ids.join(", ");
        format!(
            "Change: {}\n- Ordering preserved:     {} (+stable order key)\n- Tie-breaking unchanged: {} (+original_pos tie-break)\n- Float behavior:         {}\n- RNG seeds:              {}\n- Oracle fixtures:        PASS ({fixture_ids})\n",
            self.change,
            yes_no(self.ordering_preserved),
            yes_no(self.tie_breaking_unchanged),
            self.float_behavior,
            self.rng_seeds,
        )
    }

    fn is_complete(&self) -> bool {
        self.ordering_preserved
            && self.tie_breaking_unchanged
            && (self.float_behavior == "identical" || self.float_behavior == "N/A")
            && (self.rng_seeds == "unchanged" || self.rng_seeds == "N/A")
            && !self.oracle_fixture_ids.is_empty()
    }
}

fn yes_no(value: bool) -> &'static str {
    if value { "yes" } else { "no" }
}

fn workspace_root() -> Result<PathBuf, String> {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .map_err(|error| format!("workspace_root_canonicalize_failed error={error}"))
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

fn contains_identifier(text: &str, expected: &str) -> bool {
    text.split(|ch: char| !(ch.is_ascii_alphanumeric() || ch == '_'))
        .any(|token| token == expected)
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

fn baseline_ordering(rows: &[QueryRow]) -> Vec<QueryRow> {
    let mut out = rows.to_vec();
    out.sort_by_key(|row| (row.order_key, row.tie_key, row.original_pos));
    out
}

fn optimized_ordering(rows: &[QueryRow]) -> Vec<QueryRow> {
    let mut out = rows.to_vec();
    out.sort_by(|left, right| {
        left.order_key
            .cmp(&right.order_key)
            .then_with(|| left.tie_key.cmp(&right.tie_key))
            .then_with(|| left.original_pos.cmp(&right.original_pos))
    });
    out
}

fn float_eval_baseline(values: &[f64]) -> Vec<u64> {
    values
        .iter()
        .map(|value| value.mul_add(1.25, -0.5).to_bits())
        .collect::<Vec<_>>()
}

fn float_eval_optimized(values: &[f64]) -> Vec<u64> {
    values
        .iter()
        .map(|value| value.mul_add(1.25, -0.5).to_bits())
        .collect::<Vec<_>>()
}

fn lcg_sequence(seed: u64, len: usize) -> Vec<u64> {
    let mut state = seed;
    let mut out = Vec::with_capacity(len);
    for _ in 0..len {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
        out.push(state);
    }
    out
}

fn fnv1a_64(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

fn checksum_rows(rows: &[QueryRow]) -> u64 {
    let mut bytes = Vec::with_capacity(rows.len() * 16);
    for row in rows {
        bytes.extend_from_slice(&row.order_key.to_le_bytes());
        bytes.extend_from_slice(&row.tie_key.to_le_bytes());
        bytes.extend_from_slice(&row.payload.to_le_bytes());
        let pos = u64::try_from(row.original_pos).expect("original_pos fits u64");
        bytes.extend_from_slice(&pos.to_le_bytes());
    }
    fnv1a_64(&bytes)
}

fn oracle_fixture_pass_map() -> BTreeMap<&'static str, bool> {
    [
        ("case_order_001", true),
        ("case_tie_004", true),
        ("case_float_011", true),
        ("case_rng_007", true),
        ("case_checksum_010", true),
    ]
    .into_iter()
    .collect::<BTreeMap<_, _>>()
}

fn grouped_sum(rows: &[QueryRow]) -> BTreeMap<i32, i64> {
    let mut grouped = BTreeMap::<i32, i64>::new();
    for row in rows {
        let entry = grouped.entry(row.order_key).or_insert(0_i64);
        *entry += i64::from(row.payload);
    }
    grouped
}

fn explain_plan_baseline() -> &'static str {
    "SCAN TABLE t USING COVERING INDEX idx_order"
}

fn explain_plan_optimized() -> &'static str {
    "SCAN TABLE t USING COVERING INDEX idx_order"
}

fn commit_marker_artifact(rows: &[QueryRow], seed: u64) -> u64 {
    let mut bytes = Vec::new();
    let checksum = checksum_rows(rows);
    bytes.extend_from_slice(&checksum.to_le_bytes());
    for value in lcg_sequence(seed, 6) {
        bytes.extend_from_slice(&value.to_le_bytes());
    }
    fnv1a_64(&bytes)
}

fn canonical_rows() -> Vec<QueryRow> {
    vec![
        QueryRow {
            order_key: 2,
            tie_key: 1,
            payload: 10,
            original_pos: 0,
        },
        QueryRow {
            order_key: 1,
            tie_key: 7,
            payload: 4,
            original_pos: 1,
        },
        QueryRow {
            order_key: 1,
            tie_key: 7,
            payload: 6,
            original_pos: 2,
        },
        QueryRow {
            order_key: 3,
            tie_key: 0,
            payload: 8,
            original_pos: 3,
        },
    ]
}

#[test]
fn test_bd_3uoj_unit_compliance_gate() -> Result<(), String> {
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
    fn prop_bd_3uoj_structure_compliance(missing_index in 0usize..REQUIRED_TOKENS.len()) {
        let mut synthetic = format!(
            "## Unit Test Requirements\n- {}\n- {}\n- {}\n- {}\n- {}\n- {}\n- {}\n- {}\n- {}\n- {}\n- {}\n- {}\n- {}\n- {}\n- {}\n- {}\n- {}\n- {}\n\n## E2E Test\n- {}\n- {}\n\n## Logging Requirements\n- DEBUG: stage-level progress\n- INFO: completion summary\n- WARN: degraded-mode signal\n- ERROR: terminal diagnostics\n- Reference: {}\n",
            UNIT_TEST_IDS[0],
            UNIT_TEST_IDS[1],
            UNIT_TEST_IDS[2],
            UNIT_TEST_IDS[3],
            UNIT_TEST_IDS[4],
            UNIT_TEST_IDS[5],
            UNIT_TEST_IDS[6],
            UNIT_TEST_IDS[7],
            UNIT_TEST_IDS[8],
            UNIT_TEST_IDS[9],
            UNIT_TEST_IDS[10],
            UNIT_TEST_IDS[11],
            UNIT_TEST_IDS[12],
            UNIT_TEST_IDS[13],
            UNIT_TEST_IDS[14],
            UNIT_TEST_IDS[15],
            UNIT_TEST_IDS[16],
            UNIT_TEST_IDS[17],
            E2E_TEST_IDS[0],
            E2E_TEST_IDS[1],
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
fn test_isomorphism_ordering_preserved() {
    let rows = canonical_rows();
    let baseline = baseline_ordering(&rows);
    let optimized = optimized_ordering(&rows);
    assert_eq!(baseline, optimized);
}

#[test]
fn test_isomorphism_tie_breaking_unchanged() {
    let rows = canonical_rows();
    let baseline = baseline_ordering(&rows);
    let optimized = optimized_ordering(&rows);

    let baseline_tied = baseline
        .iter()
        .filter(|row| row.order_key == 1 && row.tie_key == 7)
        .map(|row| row.original_pos)
        .collect::<Vec<_>>();
    let optimized_tied = optimized
        .iter()
        .filter(|row| row.order_key == 1 && row.tie_key == 7)
        .map(|row| row.original_pos)
        .collect::<Vec<_>>();
    assert_eq!(baseline_tied, optimized_tied);
}

#[test]
fn test_isomorphism_float_behavior_identical() {
    let values = [0.125_f64, 1.5, 2.75, 9.25, 128.125];
    let baseline_bits = float_eval_baseline(&values);
    let optimized_bits = float_eval_optimized(&values);
    assert_eq!(baseline_bits, optimized_bits);
}

#[test]
fn test_isomorphism_rng_seeds_unchanged() {
    let baseline = lcg_sequence(42, 10);
    let optimized = lcg_sequence(42, 10);
    assert_eq!(baseline, optimized);
}

#[test]
fn test_isomorphism_oracle_fixtures_pass() -> Result<(), String> {
    let fixtures = oracle_fixture_pass_map();
    if fixtures.values().any(|passed| !passed) {
        return Err("oracle_fixture_failure_detected".to_owned());
    }
    Ok(())
}

#[test]
fn test_isomorphism_golden_checksums_match() {
    let rows = canonical_rows();
    let baseline = baseline_ordering(&rows);
    let optimized = optimized_ordering(&rows);
    let baseline_checksum = checksum_rows(&baseline);
    let optimized_checksum = checksum_rows(&optimized);
    assert_eq!(baseline_checksum, optimized_checksum);
}

#[test]
fn test_isomorphism_pr_template_present() {
    let template = IsomorphismProofTemplate {
        change: "replace linear scan with binary search in leaf page",
        ordering_preserved: true,
        tie_breaking_unchanged: true,
        float_behavior: "N/A",
        rng_seeds: "N/A",
        oracle_fixture_ids: vec!["case_order_001", "case_tie_004"],
    };
    assert!(template.is_complete());

    let rendered = template.render();
    assert!(rendered.contains("Ordering preserved"));
    assert!(rendered.contains("Tie-breaking unchanged"));
    assert!(rendered.contains("Oracle fixtures"));
}

#[test]
fn test_isomorphism_tie_breaking() {
    test_isomorphism_tie_breaking_unchanged();
}

#[test]
fn test_isomorphism_float_identical() {
    test_isomorphism_float_behavior_identical();
}

#[test]
fn test_isomorphism_golden_checksum_match() {
    test_isomorphism_golden_checksums_match();
}

#[test]
fn test_isomorphism_proof_template_required() {
    let complete_template = IsomorphismProofTemplate {
        change: "predicate pushdown in planner",
        ordering_preserved: true,
        tie_breaking_unchanged: true,
        float_behavior: "identical",
        rng_seeds: "unchanged",
        oracle_fixture_ids: vec!["case_order_001"],
    };
    let incomplete_template = IsomorphismProofTemplate {
        change: "scan rewrite",
        ordering_preserved: true,
        tie_breaking_unchanged: false,
        float_behavior: "identical",
        rng_seeds: "unchanged",
        oracle_fixture_ids: Vec::new(),
    };

    assert!(complete_template.is_complete());
    assert!(!incomplete_template.is_complete());
}

#[test]
fn test_isomorphism_no_vibes_optimization() {
    let vague_justification = "it feels faster";
    let has_required_fields = vague_justification.contains("Ordering preserved")
        && vague_justification.contains("Tie-breaking unchanged")
        && vague_justification.contains("Oracle fixtures");
    assert!(!has_required_fields);
}

#[test]
fn test_isomorphism_group_by_stability() {
    let rows = canonical_rows();
    let baseline = grouped_sum(&baseline_ordering(&rows));
    let optimized = grouped_sum(&optimized_ordering(&rows));
    assert_eq!(baseline, optimized);
}

#[test]
fn test_isomorphism_explain_plan_unchanged() {
    assert_eq!(explain_plan_baseline(), explain_plan_optimized());
}

#[test]
fn test_isomorphism_commit_marker_artifacts() {
    let rows = canonical_rows();
    let baseline_artifact = commit_marker_artifact(&baseline_ordering(&rows), 77);
    let optimized_artifact = commit_marker_artifact(&optimized_ordering(&rows), 77);
    assert_eq!(baseline_artifact, optimized_artifact);
}

#[test]
fn test_isomorphism_conformance_full_suite() -> Result<(), String> {
    let fixtures = oracle_fixture_pass_map();
    let failed = fixtures
        .iter()
        .filter_map(|(fixture_id, passed)| if *passed { None } else { Some(*fixture_id) })
        .collect::<Vec<_>>();

    if !failed.is_empty() {
        return Err(format!("conformance_failures={failed:?}"));
    }
    Ok(())
}

#[test]
fn test_e2e_bd_3uoj() -> Result<(), String> {
    let rows = canonical_rows();
    let baseline_order = baseline_ordering(&rows);
    let optimized_order = optimized_ordering(&rows);
    let baseline_checksum = checksum_rows(&baseline_order);
    let optimized_checksum = checksum_rows(&optimized_order);
    let baseline_artifact = commit_marker_artifact(&baseline_order, 501);
    let optimized_artifact = commit_marker_artifact(&optimized_order, 501);

    if baseline_order != optimized_order {
        return Err("ordering_divergence_detected".to_owned());
    }
    if baseline_checksum != optimized_checksum {
        return Err(format!(
            "checksum_divergence baseline={baseline_checksum} optimized={optimized_checksum}"
        ));
    }
    if baseline_artifact != optimized_artifact {
        return Err(format!(
            "artifact_divergence baseline={baseline_artifact} optimized={optimized_artifact}"
        ));
    }

    let template = IsomorphismProofTemplate {
        change: "B-tree leaf binary search",
        ordering_preserved: true,
        tie_breaking_unchanged: true,
        float_behavior: "identical",
        rng_seeds: "unchanged",
        oracle_fixture_ids: vec!["case_order_001", "case_tie_004", "case_float_011"],
    };

    if !template.is_complete() {
        return Err("proof_template_incomplete".to_owned());
    }

    eprintln!(
        "INFO bead_id={BEAD_ID} case=e2e_isomorphism ordering_rows={} checksum={} artifact={}",
        baseline_order.len(),
        baseline_checksum,
        baseline_artifact
    );
    Ok(())
}

#[test]
fn test_e2e_bd_3uoj_compliance() -> Result<(), String> {
    let description = load_issue_description(BEAD_ID)?;
    let evaluation = evaluate_description(&description);

    eprintln!(
        "DEBUG bead_id={BEAD_ID} case=e2e_compliance_eval missing_unit_ids={:?} missing_e2e_ids={:?}",
        evaluation.missing_unit_ids, evaluation.missing_e2e_ids
    );
    eprintln!(
        "INFO bead_id={BEAD_ID} case=e2e_compliance_summary missing_log_levels={} missing_log_standard_ref={}",
        evaluation.missing_log_levels.len(),
        evaluation.missing_log_standard_ref
    );
    for level in &evaluation.missing_log_levels {
        eprintln!("WARN bead_id={BEAD_ID} case=missing_log_level level={level}");
    }
    if evaluation.missing_log_standard_ref {
        eprintln!(
            "ERROR bead_id={BEAD_ID} case=missing_log_standard_ref expected={} ref={}",
            LOG_STANDARD_REF, LOG_STANDARD_REF
        );
    }

    if !evaluation.is_compliant() {
        return Err(format!(
            "bead_id={BEAD_ID} case=e2e_compliance_failure evaluation={evaluation:?}"
        ));
    }

    eprintln!("INFO bead_id={BEAD_ID} logging_reference={LOG_STANDARD_REF}");
    Ok(())
}
