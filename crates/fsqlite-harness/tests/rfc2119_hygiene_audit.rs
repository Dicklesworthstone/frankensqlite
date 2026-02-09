//! RFC2119 hygiene audit gate tests (§0.2 normative language).
//!
//! Bead: bd-1wx.4

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

const BEAD_ID: &str = "bd-1wx.4";
const SPEC_REL_PATH: &str = "COMPREHENSIVE_SPEC_FOR_FRANKENSQLITE_V1.md";
const BEADS_REL_PATH: &str = ".beads/issues.jsonl";
const CHECKLIST_REL_PATH: &str = "docs/rfc2119_hygiene_checklist.md";
const REPORT_REL_PATH: &str = "target/rfc2119_hygiene_report.json";
const MAX_AMBIGUOUS_EXAMPLES: usize = 64;

#[derive(Debug, Deserialize)]
struct IssueComment {
    #[serde(default)]
    text: String,
}

#[derive(Debug, Deserialize)]
struct IssueRecord {
    id: String,
    #[serde(default)]
    status: String,
    #[serde(default)]
    issue_type: String,
    #[serde(default)]
    description: String,
    #[serde(default)]
    comments: Vec<IssueComment>,
}

impl IssueRecord {
    fn is_active_task(&self) -> bool {
        self.issue_type.eq_ignore_ascii_case("task")
            && (self.status.eq_ignore_ascii_case("open")
                || self.status.eq_ignore_ascii_case("in_progress"))
    }

    fn full_text(&self) -> String {
        let mut text = self.description.clone();
        for comment in &self.comments {
            if !comment.text.is_empty() {
                if !text.is_empty() {
                    text.push('\n');
                }
                text.push_str(&comment.text);
            }
        }
        text
    }
}

#[derive(Debug, Clone)]
struct MustStatement {
    section: u8,
    line: usize,
    text: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct MustMappingGap {
    section: u8,
    line: usize,
    statement: String,
    missing_mapping_hint: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct ShouldDeviationGap {
    issue_id: String,
    context: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct AmbiguousNormativeUsage {
    source: String,
    line: usize,
    keyword: String,
    snippet: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct HygieneSummary {
    must_statements_scanned: usize,
    must_without_mapping_count: usize,
    should_deviation_markers_scanned: usize,
    should_deviation_without_justification_count: usize,
    ambiguous_normative_usage_count: usize,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct Rfc2119HygieneReport {
    bead_id: String,
    spec_path: String,
    beads_path: String,
    checklist_path: String,
    summary: HygieneSummary,
    must_without_mapping: Vec<MustMappingGap>,
    should_deviation_without_justification: Vec<ShouldDeviationGap>,
    ambiguous_normative_usage: Vec<AmbiguousNormativeUsage>,
}

fn workspace_root() -> &'static Path {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    manifest_dir
        .parent()
        .and_then(Path::parent)
        .expect("workspace root should be two levels up from fsqlite-harness")
}

fn spec_path() -> PathBuf {
    workspace_root().join(SPEC_REL_PATH)
}

fn beads_path() -> PathBuf {
    workspace_root().join(BEADS_REL_PATH)
}

fn checklist_path() -> PathBuf {
    workspace_root().join(CHECKLIST_REL_PATH)
}

fn report_path() -> PathBuf {
    workspace_root().join(REPORT_REL_PATH)
}

fn read_file(path: &Path) -> String {
    let content = fs::read_to_string(path);
    assert!(
        content.is_ok(),
        "bead_id={BEAD_ID} case=missing_file path={}",
        path.display()
    );
    content.expect("file read should succeed after is_ok assertion")
}

fn parse_issues() -> Vec<IssueRecord> {
    let path = beads_path();
    let text = read_file(&path);
    let mut issues = Vec::new();

    for (index, raw_line) in text.lines().enumerate() {
        let line = raw_line.trim();
        if line.is_empty() {
            continue;
        }

        let parsed = serde_json::from_str::<IssueRecord>(line);
        assert!(
            parsed.is_ok(),
            "bead_id={BEAD_ID} case=invalid_issue_json line={} error={}",
            index + 1,
            parsed
                .err()
                .as_ref()
                .map_or_else(|| "unknown".to_string(), std::string::ToString::to_string)
        );
        issues.push(parsed.expect("JSON parse should succeed after is_ok assertion"));
    }

    assert!(
        !issues.is_empty(),
        "bead_id={BEAD_ID} case=empty_issue_set path={}",
        path.display()
    );

    issues
}

fn parse_top_level_section(line: &str) -> Option<u8> {
    let rest = line.strip_prefix("## ")?;
    let (num, _) = rest.split_once('.')?;
    num.trim().parse::<u8>().ok()
}

fn contains_ci(haystack: &str, needle: &str) -> bool {
    haystack
        .to_ascii_lowercase()
        .contains(&needle.to_ascii_lowercase())
}

fn has_word_exact_case(text: &str, needle: &str) -> bool {
    text.split(|ch: char| !ch.is_ascii_alphanumeric())
        .any(|token| token == needle)
}

fn collect_must_statements(spec_text: &str) -> Vec<MustStatement> {
    let mut out = Vec::new();
    let mut current_section = None;

    for (index, raw_line) in spec_text.lines().enumerate() {
        if let Some(section) = parse_top_level_section(raw_line) {
            current_section = Some(section);
        }

        if let Some(section) = current_section {
            if section <= 2 && has_word_exact_case(raw_line, "MUST") {
                out.push(MustStatement {
                    section,
                    line: index + 1,
                    text: raw_line.trim().to_string(),
                });
            }
        }
    }

    out
}

fn section_mapping_issue(section: u8) -> Option<&'static str> {
    match section {
        0 => Some("bd-1wx"),
        1 => Some("bd-22n"),
        2 => Some("bd-iwu"),
        _ => None,
    }
}

fn has_acceptance_criteria(text: &str) -> bool {
    contains_ci(text, "acceptance criteria")
}

fn has_test_plan_reference(text: &str) -> bool {
    contains_ci(text, "unit test")
        || contains_ci(text, "property test")
        || contains_ci(text, "e2e test")
        || contains_ci(text, "test_")
}

fn issue_has_mapping_evidence(issue: &IssueRecord) -> bool {
    let text = issue.full_text();
    has_acceptance_criteria(&text) && has_test_plan_reference(&text)
}

fn collect_must_mapping_gaps(
    must_statements: &[MustStatement],
    issue_index: &BTreeMap<String, &IssueRecord>,
) -> Vec<MustMappingGap> {
    let mut gaps = Vec::new();

    for statement in must_statements {
        let Some(issue_id) = section_mapping_issue(statement.section) else {
            gaps.push(MustMappingGap {
                section: statement.section,
                line: statement.line,
                statement: statement.text.clone(),
                missing_mapping_hint: "No section-level mapping bead configured".to_string(),
            });
            continue;
        };

        let Some(issue) = issue_index.get(issue_id) else {
            gaps.push(MustMappingGap {
                section: statement.section,
                line: statement.line,
                statement: statement.text.clone(),
                missing_mapping_hint: format!("Mapped bead {issue_id} not found in issues.jsonl"),
            });
            continue;
        };

        if !issue_has_mapping_evidence(issue) {
            gaps.push(MustMappingGap {
                section: statement.section,
                line: statement.line,
                statement: statement.text.clone(),
                missing_mapping_hint: format!(
                    "Mapped bead {issue_id} missing acceptance criteria and/or test references"
                ),
            });
        }
    }

    gaps.sort_by(|left, right| match left.section.cmp(&right.section) {
        Ordering::Equal => left.line.cmp(&right.line),
        other => other,
    });

    gaps
}

fn has_structured_should_justification(text: &str) -> bool {
    let lower = text.to_ascii_lowercase();
    let has_why = lower.contains("why deviation is safe")
        || lower.contains("why:")
        || lower.contains("safety:");
    let has_tradeoff = lower.contains("what we give up")
        || lower.contains("tradeoff")
        || lower.contains("trade-off");
    let has_regression = lower.contains("regression detection")
        || lower.contains("detect regressions")
        || lower.contains("how we'll detect regressions");

    has_why && has_tradeoff && has_regression
}

fn collect_should_deviation_gaps(issues: &[IssueRecord]) -> (usize, Vec<ShouldDeviationGap>) {
    let mut marker_count = 0usize;
    let mut gaps = Vec::new();

    for issue in issues {
        let text = issue.full_text();
        if contains_ci(&text, "should deviation") {
            marker_count += 1;
            if !has_structured_should_justification(&text) {
                gaps.push(ShouldDeviationGap {
                    issue_id: issue.id.clone(),
                    context: "Found SHOULD deviation marker without structured why/safety/tradeoff/regression block"
                        .to_string(),
                });
            }
        }
    }

    gaps.sort_by(|left, right| left.issue_id.cmp(&right.issue_id));
    (marker_count, gaps)
}

fn detect_ambiguous_keyword(line: &str) -> Option<&'static str> {
    if has_word_exact_case(line, "must") && !has_word_exact_case(line, "MUST") {
        return Some("must");
    }
    if has_word_exact_case(line, "should") && !has_word_exact_case(line, "SHOULD") {
        return Some("should");
    }
    if has_word_exact_case(line, "may") && !has_word_exact_case(line, "MAY") {
        return Some("may");
    }
    None
}

fn collect_ambiguous_from_text(
    source: &str,
    text: &str,
    total_count: &mut usize,
    samples: &mut Vec<AmbiguousNormativeUsage>,
) {
    for (line_index, raw_line) in text.lines().enumerate() {
        let Some(keyword) = detect_ambiguous_keyword(raw_line) else {
            continue;
        };

        *total_count += 1;
        if samples.len() < MAX_AMBIGUOUS_EXAMPLES {
            samples.push(AmbiguousNormativeUsage {
                source: source.to_string(),
                line: line_index + 1,
                keyword: keyword.to_string(),
                snippet: raw_line.trim().to_string(),
            });
        }
    }
}

fn build_hygiene_report() -> Rfc2119HygieneReport {
    let spec = read_file(&spec_path());
    let issues = parse_issues();

    let must_statements = collect_must_statements(&spec);
    let issue_index: BTreeMap<String, &IssueRecord> = issues
        .iter()
        .map(|issue| (issue.id.clone(), issue))
        .collect();
    let must_without_mapping = collect_must_mapping_gaps(&must_statements, &issue_index);
    let (should_markers_scanned, should_deviation_without_justification) =
        collect_should_deviation_gaps(&issues);

    let mut ambiguous_count = 0usize;
    let mut ambiguous_normative_usage = Vec::new();
    collect_ambiguous_from_text(
        SPEC_REL_PATH,
        &spec,
        &mut ambiguous_count,
        &mut ambiguous_normative_usage,
    );

    let mut sorted_issues: Vec<&IssueRecord> = issues.iter().collect();
    sorted_issues.sort_by(|left, right| left.id.cmp(&right.id));
    for issue in sorted_issues {
        let source = format!("bead:{}", issue.id);
        let text = issue.full_text();
        collect_ambiguous_from_text(
            &source,
            &text,
            &mut ambiguous_count,
            &mut ambiguous_normative_usage,
        );
    }

    ambiguous_normative_usage.sort_by(|left, right| match left.source.cmp(&right.source) {
        Ordering::Equal => left.line.cmp(&right.line),
        other => other,
    });

    Rfc2119HygieneReport {
        bead_id: BEAD_ID.to_string(),
        spec_path: SPEC_REL_PATH.to_string(),
        beads_path: BEADS_REL_PATH.to_string(),
        checklist_path: CHECKLIST_REL_PATH.to_string(),
        summary: HygieneSummary {
            must_statements_scanned: must_statements.len(),
            must_without_mapping_count: must_without_mapping.len(),
            should_deviation_markers_scanned: should_markers_scanned,
            should_deviation_without_justification_count: should_deviation_without_justification
                .len(),
            ambiguous_normative_usage_count: ambiguous_count,
        },
        must_without_mapping,
        should_deviation_without_justification,
        ambiguous_normative_usage,
    }
}

#[test]
fn test_normative_language_section_present() {
    let spec = read_file(&spec_path());
    assert!(
        spec.contains("### 0.2 Normative Language"),
        "bead_id={BEAD_ID} case=missing_normative_language_section"
    );
    assert!(
        spec.contains("**MUST** / **MUST NOT**"),
        "bead_id={BEAD_ID} case=missing_must_definition"
    );
    assert!(
        spec.contains("**SHOULD** / **SHOULD NOT**"),
        "bead_id={BEAD_ID} case=missing_should_definition"
    );
    assert!(
        spec.contains("**MAY**"),
        "bead_id={BEAD_ID} case=missing_may_definition"
    );

    let checklist = read_file(&checklist_path());
    assert!(
        contains_ci(&checklist, "MUST boundary rule"),
        "bead_id={BEAD_ID} case=missing_checklist_must_boundary"
    );
    assert!(
        contains_ci(&checklist, "SHOULD Deviation Rule"),
        "bead_id={BEAD_ID} case=missing_checklist_should_deviation"
    );
}

#[test]
fn test_beads_use_acceptance_criteria_sections() {
    let issues = parse_issues();
    let mut missing = Vec::new();

    for issue in issues.iter().filter(|issue| issue.is_active_task()) {
        if !has_acceptance_criteria(&issue.full_text()) {
            missing.push(issue.id.clone());
        }
    }

    missing.sort();
    assert!(
        missing.is_empty(),
        "bead_id={BEAD_ID} case=missing_acceptance_criteria open_task_ids={missing:?}"
    );
}

#[test]
fn test_should_deviation_requires_justification_marker() {
    let issues = parse_issues();
    let (_, gaps) = collect_should_deviation_gaps(&issues);
    assert!(
        gaps.is_empty(),
        "bead_id={BEAD_ID} case=should_deviation_missing_justification gaps={gaps:?}"
    );
}

#[test]
fn test_must_keywords_have_test_mapping() {
    let spec = read_file(&spec_path());
    let issues = parse_issues();
    let issue_index: BTreeMap<String, &IssueRecord> = issues
        .iter()
        .map(|issue| (issue.id.clone(), issue))
        .collect();
    let must_statements = collect_must_statements(&spec);

    let mut sampled: BTreeMap<u8, MustStatement> = BTreeMap::new();
    for statement in must_statements {
        sampled.entry(statement.section).or_insert(statement);
    }

    assert!(
        sampled.contains_key(&0),
        "bead_id={BEAD_ID} case=missing_section0_must_sample"
    );
    assert!(
        sampled.contains_key(&1),
        "bead_id={BEAD_ID} case=missing_section1_must_sample"
    );
    assert!(
        sampled.contains_key(&2),
        "bead_id={BEAD_ID} case=missing_section2_must_sample"
    );

    let samples: Vec<MustStatement> = sampled.into_values().collect();
    let gaps = collect_must_mapping_gaps(&samples, &issue_index);
    assert!(
        gaps.is_empty(),
        "bead_id={BEAD_ID} case=missing_must_mapping sampled_gaps={gaps:?}"
    );
}

#[test]
fn test_e2e_rfc2119_hygiene_report() {
    let report_a = build_hygiene_report();
    let report_b = build_hygiene_report();
    assert_eq!(
        report_a, report_b,
        "bead_id={BEAD_ID} case=non_deterministic_report"
    );

    eprintln!(
        "INFO bead_id={BEAD_ID} case=hygiene_report_summary must_scanned={} must_without_mapping={} should_markers={} should_without_justification={} ambiguous_count={}",
        report_a.summary.must_statements_scanned,
        report_a.summary.must_without_mapping_count,
        report_a.summary.should_deviation_markers_scanned,
        report_a
            .summary
            .should_deviation_without_justification_count,
        report_a.summary.ambiguous_normative_usage_count
    );
    for gap in &report_a.should_deviation_without_justification {
        eprintln!(
            "WARN bead_id={BEAD_ID} case=should_deviation_without_justification issue_id={} detail={}",
            gap.issue_id, gap.context
        );
    }
    for gap in &report_a.must_without_mapping {
        eprintln!(
            "ERROR bead_id={BEAD_ID} case=must_without_mapping section={} line={} hint={}",
            gap.section, gap.line, gap.missing_mapping_hint
        );
    }

    let output_path = report_path();
    if let Some(parent) = output_path.parent() {
        let created = fs::create_dir_all(parent);
        assert!(
            created.is_ok(),
            "bead_id={BEAD_ID} case=failed_create_report_dir path={} error={}",
            parent.display(),
            created
                .err()
                .as_ref()
                .map_or_else(|| "unknown".to_string(), std::string::ToString::to_string)
        );
    }

    let report_json = serde_json::to_string_pretty(&report_a);
    assert!(
        report_json.is_ok(),
        "bead_id={BEAD_ID} case=report_serialization_failed error={}",
        report_json
            .err()
            .as_ref()
            .map_or_else(|| "unknown".to_string(), std::string::ToString::to_string)
    );

    let written = fs::write(
        &output_path,
        report_json.expect("report serialization should succeed after is_ok assertion"),
    );
    assert!(
        written.is_ok(),
        "bead_id={BEAD_ID} case=report_write_failed path={} error={}",
        output_path.display(),
        written
            .err()
            .as_ref()
            .map_or_else(|| "unknown".to_string(), std::string::ToString::to_string)
    );

    assert!(
        report_a.must_without_mapping.is_empty(),
        "bead_id={BEAD_ID} case=must_mapping_failures count={} output={}",
        report_a.must_without_mapping.len(),
        output_path.display()
    );
    assert!(
        report_a.should_deviation_without_justification.is_empty(),
        "bead_id={BEAD_ID} case=should_justification_failures count={} output={}",
        report_a.should_deviation_without_justification.len(),
        output_path.display()
    );
}
