use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use proptest::prelude::proptest;
use proptest::test_runner::TestCaseError;
use serde::Deserialize;
use serde_json::Value;

const BEAD_ID: &str = "bd-3kp.1";
const ISSUES_JSONL: &str = ".beads/issues.jsonl";
const DECISION_REGISTER_PATH: &str = "docs/decision_register.jsonl";
const UNIT_TEST_IDS: [&str; 5] = [
    "test_bd_3kp_1_unit_compliance_gate",
    "prop_bd_3kp_1_structure_compliance",
    "test_decision_register_entry_schema",
    "test_decision_register_links_resolve",
    "test_decision_register_append_only",
];
const E2E_TEST_IDS: [&str; 2] = ["test_e2e_bd_3kp_1", "test_e2e_bd_3kp_1_compliance"];
const LOG_LEVEL_MARKERS: [&str; 4] = ["DEBUG", "INFO", "WARN", "ERROR"];
const LOG_STANDARD_REF: &str = "bd-1fpm";
const EXPECTED_QUESTIONS: [&str; 8] = ["Q1", "Q2", "Q3", "Q4", "Q5", "Q6", "Q7", "Q8"];
const REQUIRED_TOKENS: [&str; 12] = [
    "test_bd_3kp_1_unit_compliance_gate",
    "prop_bd_3kp_1_structure_compliance",
    "test_decision_register_entry_schema",
    "test_decision_register_links_resolve",
    "test_decision_register_append_only",
    "test_e2e_bd_3kp_1",
    "test_e2e_bd_3kp_1_compliance",
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

#[derive(Debug, Deserialize, Clone)]
#[allow(clippy::struct_field_names)]
struct DecisionEntry {
    decision_id: String,
    question_id: String,
    date: String,
    status: String,
    context: String,
    options_considered: Vec<String>,
    decision: String,
    rationale: String,
    owner_bead: String,
    blocks_implementation: bool,
    links: Vec<String>,
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

fn load_decision_entries() -> Result<Vec<DecisionEntry>, String> {
    let path = workspace_root()?.join(DECISION_REGISTER_PATH);
    let raw = fs::read_to_string(&path).map_err(|error| {
        format!(
            "decision_register_read_failed path={} error={error}",
            path.display()
        )
    })?;

    raw.lines()
        .enumerate()
        .filter_map(|(index, line)| {
            if line.trim().is_empty() {
                None
            } else {
                Some(
                    serde_json::from_str::<DecisionEntry>(line).map_err(|error| {
                        format!(
                            "decision_register_parse_failed path={} line={} error={error}",
                            path.display(),
                            index + 1
                        )
                    }),
                )
            }
        })
        .collect::<Result<Vec<_>, _>>()
}

fn load_bead_ids() -> Result<BTreeSet<String>, String> {
    let issues_path = workspace_root()?.join(ISSUES_JSONL);
    let raw = fs::read_to_string(&issues_path).map_err(|error| {
        format!(
            "issues_jsonl_read_failed path={} error={error}",
            issues_path.display()
        )
    })?;

    let mut ids = BTreeSet::new();
    for line in raw.lines().filter(|line| !line.trim().is_empty()) {
        let value: Value = serde_json::from_str(line)
            .map_err(|error| format!("issues_jsonl_parse_failed error={error} line={line}"))?;
        if let Some(id) = value.get("id").and_then(Value::as_str) {
            ids.insert(id.to_owned());
        }
    }

    Ok(ids)
}

fn load_workspace_rust_source() -> Result<String, String> {
    fn collect(path: &Path, aggregate: &mut String) -> Result<(), String> {
        let read_dir = fs::read_dir(path)
            .map_err(|error| format!("read_dir_failed path={} error={error}", path.display()))?;

        for entry_result in read_dir {
            let entry = entry_result.map_err(|error| {
                format!(
                    "read_dir_entry_failed path={} error={error}",
                    path.display()
                )
            })?;
            let entry_path = entry.path();

            if entry_path.is_dir() {
                collect(&entry_path, aggregate)?;
            } else if entry_path.extension().and_then(|ext| ext.to_str()) == Some("rs") {
                let content = fs::read_to_string(&entry_path).map_err(|error| {
                    format!(
                        "rust_source_read_failed path={} error={error}",
                        entry_path.display()
                    )
                })?;
                aggregate.push_str(&content);
                aggregate.push('\n');
            }
        }

        Ok(())
    }

    let root = workspace_root()?;
    let mut aggregate = String::new();
    collect(&root.join("crates"), &mut aggregate)?;
    collect(&root.join("tests"), &mut aggregate)?;

    Ok(aggregate)
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

fn parse_decision_seq(decision_id: &str) -> Result<u32, String> {
    let Some(raw) = decision_id.strip_prefix("dr-") else {
        return Err(format!("decision_id_format_invalid id={decision_id}"));
    };

    raw.parse::<u32>()
        .map_err(|error| format!("decision_id_parse_failed id={decision_id} error={error}"))
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
    text.push_str("- DEBUG: stage progress\n");
    text.push_str("- INFO: completion summary\n");
    text.push_str("- WARN: degraded behavior\n");
    text.push_str("- ERROR: reproducible failure details\n");
    text.push_str("- Reference: ");
    text.push_str(LOG_STANDARD_REF);
    text.push('\n');

    text.push_str("\n## Schema\n");
    text.push_str("- decision_id\n");

    text
}

#[test]
fn test_decision_register_entry_schema() -> Result<(), String> {
    let entries = load_decision_entries()?;
    if entries.is_empty() {
        return Err("decision_register_empty".to_owned());
    }

    for entry in &entries {
        if entry.decision_id.trim().is_empty()
            || entry.question_id.trim().is_empty()
            || entry.date.trim().is_empty()
            || entry.status.trim().is_empty()
            || entry.context.trim().is_empty()
            || entry.decision.trim().is_empty()
            || entry.rationale.trim().is_empty()
            || entry.owner_bead.trim().is_empty()
        {
            return Err(format!(
                "decision_register_required_field_missing decision_id={}",
                entry.decision_id
            ));
        }

        if entry.options_considered.is_empty() {
            return Err(format!(
                "decision_register_options_empty decision_id={}",
                entry.decision_id
            ));
        }

        if entry.links.is_empty() {
            return Err(format!(
                "decision_register_links_empty decision_id={}",
                entry.decision_id
            ));
        }

        if !matches!(entry.status.as_str(), "proposed" | "accepted" | "rejected") {
            return Err(format!(
                "decision_register_status_invalid decision_id={} status={}",
                entry.decision_id, entry.status
            ));
        }

        if entry.blocks_implementation && entry.owner_bead.trim().is_empty() {
            return Err(format!(
                "decision_register_owner_missing decision_id={}",
                entry.decision_id
            ));
        }
    }

    Ok(())
}

#[test]
fn test_decision_register_links_resolve() -> Result<(), String> {
    let entries = load_decision_entries()?;
    let bead_ids = load_bead_ids()?;
    let rust_source = load_workspace_rust_source()?;

    for entry in &entries {
        for link in &entry.links {
            if link.starts_with("bd-") {
                if !bead_ids.contains(link) {
                    return Err(format!(
                        "decision_register_bead_link_missing decision_id={} link={link}",
                        entry.decision_id
                    ));
                }
            } else if link.starts_with("test_") && !contains_identifier(&rust_source, link) {
                return Err(format!(
                    "decision_register_test_link_missing decision_id={} link={link}",
                    entry.decision_id
                ));
            }
        }

        if entry.status == "accepted"
            && !entry
                .links
                .iter()
                .any(|link| link.starts_with("test_") || link.starts_with("bd-"))
        {
            return Err(format!(
                "decision_register_accepted_without_guard decision_id={}",
                entry.decision_id
            ));
        }
    }

    Ok(())
}

#[test]
fn test_decision_register_append_only() -> Result<(), String> {
    let entries = load_decision_entries()?;
    let mut previous_seq = 0_u32;
    let mut seen_ids = BTreeSet::new();
    let mut seen_questions = BTreeSet::new();

    for entry in &entries {
        let seq = parse_decision_seq(&entry.decision_id)?;
        if seq <= previous_seq {
            return Err(format!(
                "decision_register_not_append_only decision_id={} previous_seq={} current_seq={seq}",
                entry.decision_id, previous_seq
            ));
        }

        if !seen_ids.insert(entry.decision_id.clone()) {
            return Err(format!(
                "decision_register_duplicate_decision_id decision_id={}",
                entry.decision_id
            ));
        }

        if !seen_questions.insert(entry.question_id.clone()) {
            return Err(format!(
                "decision_register_duplicate_question_id question_id={}",
                entry.question_id
            ));
        }

        previous_seq = seq;
    }

    let expected = EXPECTED_QUESTIONS
        .into_iter()
        .map(ToOwned::to_owned)
        .collect::<BTreeSet<_>>();
    if seen_questions != expected {
        return Err(format!(
            "decision_register_question_set_mismatch actual={seen_questions:?} expected={expected:?}"
        ));
    }

    Ok(())
}

#[test]
fn test_bd_3kp_1_unit_compliance_gate() -> Result<(), String> {
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
    fn prop_bd_3kp_1_structure_compliance(missing_index in 0usize..REQUIRED_TOKENS.len()) {
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

fn render_decision_report(entries: &[DecisionEntry]) -> Vec<String> {
    let mut sorted = entries.to_vec();
    sorted.sort_by(|left, right| left.decision_id.cmp(&right.decision_id));

    sorted
        .into_iter()
        .map(|entry| {
            format!(
                "{}|{}|{}|{}|{}",
                entry.decision_id,
                entry.question_id,
                entry.status,
                entry.owner_bead,
                entry.links.join(",")
            )
        })
        .collect::<Vec<_>>()
}

#[test]
fn test_e2e_bd_3kp_1_compliance() -> Result<(), String> {
    let entries = load_decision_entries()?;
    let bead_ids = load_bead_ids()?;

    let mut question_to_owner = BTreeSet::new();
    for entry in &entries {
        if entry.blocks_implementation {
            if entry.owner_bead.trim().is_empty() {
                return Err(format!(
                    "bead_id={BEAD_ID} case=owner_missing decision_id={}",
                    entry.decision_id
                ));
            }
            if !bead_ids.contains(&entry.owner_bead) {
                return Err(format!(
                    "bead_id={BEAD_ID} case=owner_bead_missing decision_id={} owner_bead={}",
                    entry.decision_id, entry.owner_bead
                ));
            }
        }

        question_to_owner.insert((entry.question_id.clone(), entry.status.clone()));
    }

    let expected_question_set = EXPECTED_QUESTIONS
        .into_iter()
        .map(ToOwned::to_owned)
        .collect::<BTreeSet<_>>();
    let observed_question_set = entries
        .iter()
        .map(|entry| entry.question_id.clone())
        .collect::<BTreeSet<_>>();

    let report_once = render_decision_report(&entries);
    let report_twice = render_decision_report(&entries);

    eprintln!(
        "DEBUG bead_id={BEAD_ID} case=e2e_start decision_entries={} expected_questions={} report_lines={}",
        entries.len(),
        expected_question_set.len(),
        report_once.len()
    );

    eprintln!(
        "INFO bead_id={BEAD_ID} case=e2e_summary observed_questions={} deterministic_report={} owner_pairs={}",
        observed_question_set.len(),
        report_once == report_twice,
        question_to_owner.len()
    );

    eprintln!(
        "WARN bead_id={BEAD_ID} case=diagnostic missing_questions={} extra_questions={}",
        expected_question_set
            .difference(&observed_question_set)
            .count(),
        observed_question_set
            .difference(&expected_question_set)
            .count()
    );

    eprintln!(
        "ERROR bead_id={BEAD_ID} case=diagnostic_guard report_mismatch={}",
        report_once != report_twice
    );

    if observed_question_set != expected_question_set {
        return Err(format!(
            "bead_id={BEAD_ID} case=question_set_mismatch observed={observed_question_set:?} expected={expected_question_set:?}"
        ));
    }

    if report_once != report_twice {
        return Err(format!(
            "bead_id={BEAD_ID} case=nondeterministic_report first={report_once:?} second={report_twice:?}"
        ));
    }

    Ok(())
}

#[test]
fn test_e2e_bd_3kp_1() -> Result<(), String> {
    test_e2e_bd_3kp_1_compliance()
}
