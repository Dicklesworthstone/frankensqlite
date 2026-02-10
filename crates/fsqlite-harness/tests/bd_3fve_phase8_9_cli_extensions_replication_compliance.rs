use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use proptest::prelude::proptest;
use proptest::test_runner::TestCaseError;
use serde_json::{Value, json};

const BEAD_ID: &str = "bd-3fve";
const ISSUES_JSONL: &str = ".beads/issues.jsonl";
const UNIT_TEST_IDS: [&str; 2] = [
    "test_bd_3fve_unit_compliance_gate",
    "prop_bd_3fve_structure_compliance",
];
const PHASE8_9_TEST_IDS: [&str; 22] = [
    "test_json1_all_functions",
    "test_json1_jsonb_roundtrip",
    "test_json_each_tree",
    "test_fts5_tokenize_100k",
    "test_fts5_highlight_snippet",
    "test_fts5_prefix_queries",
    "test_fts3_matchinfo",
    "test_rtree_2d_100k",
    "test_rtree_custom_geometry",
    "test_session_changeset",
    "test_icu_collation",
    "test_generate_series_perf",
    "test_cli_dot_commands",
    "test_cli_output_modes",
    "test_cli_tab_completion",
    "test_cli_syntax_highlighting",
    "test_conformance_100pct_parity",
    "test_conformance_slt_ingestion",
    "test_benchmark_single_writer_3x",
    "test_benchmark_multiwriter_scaling",
    "test_replication_10pct_loss",
    "test_snapshot_shipping_raptorq",
];
const E2E_TEST_IDS: [&str; 2] = ["test_e2e_bd_3fve", "test_e2e_bd_3fve_compliance"];
const PHASE8_9_REQUIRED_MARKERS: [&str; 16] = [
    "fsqlite-json",
    "fsqlite-fts5",
    "fsqlite-fts3",
    "fsqlite-rtree",
    "fsqlite-session",
    "fsqlite-icu",
    "fsqlite-misc",
    "fsqlite-cli",
    "fsqlite-harness",
    "fsqlite-replication",
    ".tables",
    ".schema",
    ".mode",
    ".headers",
    ".import",
    ".dump",
];
const LOG_LEVEL_MARKERS: [&str; 4] = ["DEBUG", "INFO", "WARN", "ERROR"];
const LOG_STANDARD_REF: &str = "bd-1fpm";
const REQUIRED_TOKENS: &[&str] = &[
    "test_bd_3fve_unit_compliance_gate",
    "prop_bd_3fve_structure_compliance",
    "test_json1_all_functions",
    "test_json1_jsonb_roundtrip",
    "test_json_each_tree",
    "test_fts5_tokenize_100k",
    "test_fts5_highlight_snippet",
    "test_fts5_prefix_queries",
    "test_fts3_matchinfo",
    "test_rtree_2d_100k",
    "test_rtree_custom_geometry",
    "test_session_changeset",
    "test_icu_collation",
    "test_generate_series_perf",
    "test_cli_dot_commands",
    "test_cli_output_modes",
    "test_cli_tab_completion",
    "test_cli_syntax_highlighting",
    "test_conformance_100pct_parity",
    "test_conformance_slt_ingestion",
    "test_benchmark_single_writer_3x",
    "test_benchmark_multiwriter_scaling",
    "test_replication_10pct_loss",
    "test_snapshot_shipping_raptorq",
    "test_e2e_bd_3fve",
    "test_e2e_bd_3fve_compliance",
    "fsqlite-json",
    "fsqlite-fts5",
    "fsqlite-fts3",
    "fsqlite-rtree",
    "fsqlite-session",
    "fsqlite-icu",
    "fsqlite-misc",
    "fsqlite-cli",
    "fsqlite-harness",
    "fsqlite-replication",
    ".tables",
    ".schema",
    ".mode",
    ".headers",
    ".import",
    ".dump",
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
    missing_phase8_9_test_ids: Vec<&'static str>,
    missing_e2e_ids: Vec<&'static str>,
    missing_phase8_9_markers: Vec<&'static str>,
    missing_log_levels: Vec<&'static str>,
    missing_log_standard_ref: bool,
}

impl ComplianceEvaluation {
    fn is_compliant(&self) -> bool {
        self.missing_unit_ids.is_empty()
            && self.missing_phase8_9_test_ids.is_empty()
            && self.missing_e2e_ids.is_empty()
            && self.missing_phase8_9_markers.is_empty()
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

    let missing_phase8_9_test_ids = PHASE8_9_TEST_IDS
        .into_iter()
        .filter(|id| !contains_identifier(description, id))
        .collect::<Vec<_>>();

    let missing_e2e_ids = E2E_TEST_IDS
        .into_iter()
        .filter(|id| !contains_identifier(description, id))
        .collect::<Vec<_>>();

    let missing_phase8_9_markers = PHASE8_9_REQUIRED_MARKERS
        .into_iter()
        .filter(|marker| !description.contains(marker))
        .collect::<Vec<_>>();

    let missing_log_levels = LOG_LEVEL_MARKERS
        .into_iter()
        .filter(|level| !description.contains(level))
        .collect::<Vec<_>>();

    ComplianceEvaluation {
        missing_unit_ids,
        missing_phase8_9_test_ids,
        missing_e2e_ids,
        missing_phase8_9_markers,
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
    for id in PHASE8_9_TEST_IDS {
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

    text.push_str("\n## Deliverables\n");
    text.push_str(
        "- crates: fsqlite-json, fsqlite-fts5, fsqlite-fts3, fsqlite-rtree, fsqlite-session, fsqlite-icu, fsqlite-misc\n",
    );
    text.push_str("- crates: fsqlite-cli, fsqlite-harness, fsqlite-replication\n");
    text.push_str("- dot-commands: .tables .schema .mode .headers .import .dump\n");

    text.push_str("\n## Logging Requirements\n");
    text.push_str("- DEBUG: stage-level progress\n");
    text.push_str("- INFO: summary counters and completion status\n");
    text.push_str("- WARN: degraded mode and retry conditions\n");
    text.push_str("- ERROR: terminal diagnostics\n");
    text.push_str("- Reference: ");
    text.push_str(LOG_STANDARD_REF);
    text.push('\n');

    text
}

fn unique_runtime_dir(label: &str) -> Result<PathBuf, String> {
    let root = workspace_root()?.join("target").join("bd_3fve_runtime");
    fs::create_dir_all(&root).map_err(|error| {
        format!(
            "runtime_dir_create_failed path={} error={error}",
            root.as_path().display()
        )
    })?;

    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0_u128, |duration| duration.as_nanos());
    let path = root.join(format!("{label}_{}_{}", std::process::id(), stamp));
    fs::create_dir_all(&path).map_err(|error| {
        format!(
            "runtime_subdir_create_failed path={} error={error}",
            path.as_path().display()
        )
    })?;
    Ok(path)
}

#[test]
fn test_bd_3fve_unit_compliance_gate() -> Result<(), String> {
    let description = load_issue_description(BEAD_ID)?;
    let evaluation = evaluate_description(&description);

    if !evaluation.missing_unit_ids.is_empty() {
        return Err(format!(
            "bead_id={BEAD_ID} case=unit_ids_missing missing={:?}",
            evaluation.missing_unit_ids
        ));
    }
    if !evaluation.missing_phase8_9_test_ids.is_empty() {
        return Err(format!(
            "bead_id={BEAD_ID} case=phase8_9_test_ids_missing missing={:?}",
            evaluation.missing_phase8_9_test_ids
        ));
    }
    if !evaluation.missing_e2e_ids.is_empty() {
        return Err(format!(
            "bead_id={BEAD_ID} case=e2e_ids_missing missing={:?}",
            evaluation.missing_e2e_ids
        ));
    }
    if !evaluation.missing_phase8_9_markers.is_empty() {
        return Err(format!(
            "bead_id={BEAD_ID} case=phase8_9_markers_missing missing={:?}",
            evaluation.missing_phase8_9_markers
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
    fn prop_bd_3fve_structure_compliance(missing_index in 0usize..REQUIRED_TOKENS.len()) {
        let mut synthetic = synthetic_compliant_description();
        synthetic = synthetic.replacen(REQUIRED_TOKENS[missing_index], "", 1);

        let evaluation = evaluate_description(&synthetic);
        if evaluation.is_compliant() {
            return Err(TestCaseError::fail(format!(
                "bead_id={} case=structure_compliance expected_non_compliant missing_index={} missing_marker={}",
                BEAD_ID,
                missing_index,
                REQUIRED_TOKENS[missing_index],
            )));
        }
    }
}

#[test]
fn test_e2e_bd_3fve_compliance() -> Result<(), String> {
    let description = load_issue_description(BEAD_ID)?;
    let evaluation = evaluate_description(&description);

    let runtime_dir = unique_runtime_dir("e2e")?;
    let artifact_path = runtime_dir.join("bd_3fve_artifact.json");
    let artifact = json!({
        "bead_id": BEAD_ID,
        "missing_unit_ids": evaluation.missing_unit_ids,
        "missing_phase8_9_test_ids": evaluation.missing_phase8_9_test_ids,
        "missing_e2e_ids": evaluation.missing_e2e_ids,
        "missing_phase8_9_markers": evaluation.missing_phase8_9_markers,
        "missing_log_levels": evaluation.missing_log_levels,
        "missing_log_standard_ref": evaluation.missing_log_standard_ref,
    });
    let artifact_pretty = serde_json::to_string_pretty(&artifact)
        .map_err(|error| format!("artifact_serialize_failed error={error}"))?;
    fs::write(&artifact_path, artifact_pretty).map_err(|error| {
        format!(
            "artifact_write_failed path={} error={error}",
            artifact_path.as_path().display()
        )
    })?;

    eprintln!(
        "DEBUG bead_id={BEAD_ID} case=artifact_written path={} size_bytes={}",
        artifact_path.display(),
        fs::metadata(&artifact_path)
            .map_err(|error| format!("artifact_metadata_failed error={error}"))?
            .len()
    );
    eprintln!(
        "INFO bead_id={BEAD_ID} case=e2e_summary missing_unit_ids={} missing_phase8_9_test_ids={} missing_e2e_ids={} missing_phase8_9_markers={} missing_log_levels={} missing_log_standard_ref={}",
        evaluation.missing_unit_ids.len(),
        evaluation.missing_phase8_9_test_ids.len(),
        evaluation.missing_e2e_ids.len(),
        evaluation.missing_phase8_9_markers.len(),
        evaluation.missing_log_levels.len(),
        evaluation.missing_log_standard_ref
    );

    for id in &evaluation.missing_unit_ids {
        eprintln!("WARN bead_id={BEAD_ID} case=missing_unit_id id={id}");
    }
    for id in &evaluation.missing_phase8_9_test_ids {
        eprintln!("WARN bead_id={BEAD_ID} case=missing_phase8_9_test_id id={id}");
    }
    for id in &evaluation.missing_e2e_ids {
        eprintln!("WARN bead_id={BEAD_ID} case=missing_e2e_id id={id}");
    }
    for marker in &evaluation.missing_phase8_9_markers {
        eprintln!("WARN bead_id={BEAD_ID} case=missing_phase8_9_marker marker={marker}");
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
fn test_e2e_bd_3fve() -> Result<(), String> {
    test_e2e_bd_3fve_compliance()
}
