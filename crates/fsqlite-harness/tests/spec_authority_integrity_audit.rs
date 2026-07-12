//! Spec authority and integrity audit gate tests (§0 authority + supersession).
//!
//! Bead: bd-1wx.5

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use proptest::prelude::*;
use serde::Serialize;

const BEAD_ID: &str = "bd-1wx.5";
const SPEC_REL_PATH: &str = "docs/planning/COMPREHENSIVE_SPEC_FOR_FRANKENSQLITE_V1.md";
const REPORT_REL_PATH: &str = "target/spec_authority_integrity_report.json";
const EXPECTED_SECTIONS: std::ops::RangeInclusive<u8> = 0..=23;
const LEGACY_DOCS: &[&str] = &[
    "docs/planning/PROPOSED_ARCHITECTURE.md",
    "docs/planning/MVCC_SPECIFICATION.md",
    "docs/planning/PLAN_TO_PORT_SQLITE_TO_RUST.md",
    "docs/planning/EXISTING_SQLITE_STRUCTURE.md",
];

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct SectionHeader {
    number: u8,
    title: String,
    line: usize,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct TocEntry {
    number: u8,
    title: String,
    line: usize,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct LegacyDocStatus {
    path: String,
    exists: bool,
    has_historical_notice: bool,
    references_comprehensive_spec: bool,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct SpecIntegritySummary {
    toc_entries: usize,
    section_headers: usize,
    missing_docs: usize,
    docs_missing_notice: usize,
    toc_alignment_errors: usize,
    duplicate_sections: usize,
    missing_sections: usize,
    broken_internal_references: usize,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct SpecIntegrityReport {
    bead_id: String,
    spec_path: String,
    report_path: String,
    preamble_present: bool,
    supersession_list_present: bool,
    metadata_header_parseable: bool,
    legacy_docs: Vec<LegacyDocStatus>,
    missing_docs: Vec<String>,
    docs_missing_notice: Vec<String>,
    duplicate_sections: Vec<u8>,
    missing_sections: Vec<u8>,
    toc_alignment_errors: Vec<String>,
    broken_internal_references: Vec<String>,
    summary: SpecIntegritySummary,
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

fn report_path() -> PathBuf {
    workspace_root().join(REPORT_REL_PATH)
}

fn contains_case_insensitive(haystack: &str, needle: &str) -> bool {
    haystack
        .to_ascii_lowercase()
        .contains(&needle.to_ascii_lowercase())
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

fn parse_numbered_heading(raw: &str) -> Option<(u8, String)> {
    let heading = raw.trim();
    let body = heading.strip_prefix("## ")?;
    let (number_raw, title_raw) = body.split_once('.')?;
    let number = number_raw.trim().parse::<u8>().ok()?;
    let title = title_raw.trim();
    if title.is_empty() {
        return None;
    }
    Some((number, title.to_string()))
}

fn parse_toc_entry(raw: &str) -> Option<(u8, String)> {
    let entry = raw.trim().strip_prefix("- ")?;
    let (number_raw, title_raw) = entry.split_once('.')?;
    let number = number_raw.trim().parse::<u8>().ok()?;
    let title = title_raw.trim();
    if title.is_empty() {
        return None;
    }
    Some((number, title.to_string()))
}

fn parse_section_headers(spec_text: &str) -> Vec<SectionHeader> {
    spec_text
        .lines()
        .enumerate()
        .filter_map(|(idx, line)| {
            parse_numbered_heading(line).map(|(number, title)| SectionHeader {
                number,
                title,
                line: idx + 1,
            })
        })
        .collect()
}

fn parse_toc_entries(spec_text: &str) -> Vec<TocEntry> {
    let mut entries = Vec::new();
    let mut in_toc = false;

    for (idx, line) in spec_text.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed == "## Table of Contents" {
            in_toc = true;
            continue;
        }
        if !in_toc {
            continue;
        }
        if trimmed.starts_with("## ") {
            break;
        }
        if let Some((number, title)) = parse_toc_entry(trimmed) {
            entries.push(TocEntry {
                number,
                title,
                line: idx + 1,
            });
        }
    }

    entries
}

fn section_duplicate_and_missing(section_numbers: &[u8]) -> (BTreeSet<u8>, BTreeSet<u8>) {
    let mut counts: BTreeMap<u8, usize> = BTreeMap::new();
    for section in section_numbers {
        let counter = counts.entry(*section).or_insert(0);
        *counter += 1;
    }

    let mut duplicates = BTreeSet::new();
    let mut missing = BTreeSet::new();
    for section in EXPECTED_SECTIONS {
        let count = counts.get(&section).copied().unwrap_or(0);
        if count == 0 {
            missing.insert(section);
        } else if count > 1 {
            duplicates.insert(section);
        }
    }

    (duplicates, missing)
}

fn preamble_present(spec_text: &str) -> bool {
    spec_text.contains("# COMPREHENSIVE SPECIFICATION FOR FRANKENSQLITE")
        && contains_case_insensitive(spec_text, "clean-room rust reimplementation")
        && contains_case_insensitive(spec_text, "single authoritative specification")
}

fn supersession_list_present(spec_text: &str) -> bool {
    LEGACY_DOCS.iter().all(|doc| {
        let file_name = Path::new(doc)
            .file_name()
            .and_then(|name| name.to_str())
            .expect("legacy spec paths must end in valid UTF-8 file names");
        spec_text.contains(&format!("`{file_name}`"))
    })
}

fn metadata_header_parseable(spec_text: &str) -> bool {
    let mut saw_version = false;
    let mut saw_last_updated = false;

    for line in spec_text.lines().take(60) {
        let trimmed = line.trim();
        if let Some(value) = trimmed.strip_prefix("Version:") {
            saw_version = !value.trim().is_empty();
        }
        if let Some(value) = trimmed.strip_prefix("Last updated:") {
            saw_last_updated = !value.trim().is_empty();
        }
    }

    (!saw_version && !saw_last_updated) || (saw_version && saw_last_updated)
}

fn collect_legacy_doc_statuses() -> Vec<LegacyDocStatus> {
    let mut statuses = Vec::new();
    for doc in LEGACY_DOCS {
        let path = workspace_root().join(doc);
        let content = fs::read_to_string(&path).unwrap_or_default();
        let lower = content.to_ascii_lowercase();
        statuses.push(LegacyDocStatus {
            path: (*doc).to_string(),
            exists: path.exists(),
            has_historical_notice: lower.contains("historical reference"),
            references_comprehensive_spec: lower
                .contains("comprehensive_spec_for_frankensqlite_v1.md"),
        });
    }
    statuses
}

fn build_integrity_report() -> SpecIntegrityReport {
    let spec_text = read_file(&spec_path());
    let headers = parse_section_headers(&spec_text);
    let toc_entries = parse_toc_entries(&spec_text);

    let header_numbers: Vec<u8> = headers.iter().map(|header| header.number).collect();
    let (duplicate_headers, missing_headers) = section_duplicate_and_missing(&header_numbers);

    let mut header_by_number = BTreeMap::new();
    for header in &headers {
        header_by_number
            .entry(header.number)
            .or_insert(&header.title);
    }

    let mut toc_by_number = BTreeMap::new();
    let mut toc_alignment_errors = Vec::new();
    for entry in &toc_entries {
        if toc_by_number.insert(entry.number, &entry.title).is_some() {
            toc_alignment_errors.push(format!(
                "duplicate TOC entry for section {} at line {}",
                entry.number, entry.line
            ));
        }
    }

    for entry in &toc_entries {
        match header_by_number.get(&entry.number) {
            None => toc_alignment_errors.push(format!(
                "TOC references missing section {} ({})",
                entry.number, entry.title
            )),
            Some(actual_title) if *actual_title != &entry.title => {
                toc_alignment_errors.push(format!(
                    "TOC title mismatch for section {}: toc='{}' header='{}'",
                    entry.number, entry.title, actual_title
                ));
            }
            Some(_) => {}
        }
    }

    for header in &headers {
        if !toc_by_number.contains_key(&header.number) {
            toc_alignment_errors.push(format!(
                "section {} ('{}') missing from TOC",
                header.number, header.title
            ));
        }
    }

    let toc_numbers: Vec<u8> = toc_entries.iter().map(|entry| entry.number).collect();
    let expected_numbers: Vec<u8> = EXPECTED_SECTIONS.collect();
    if toc_numbers != expected_numbers {
        toc_alignment_errors.push(format!(
            "TOC order mismatch: expected {:?}, got {:?}",
            expected_numbers, toc_numbers
        ));
    }

    let legacy_docs = collect_legacy_doc_statuses();
    let missing_docs: Vec<String> = legacy_docs
        .iter()
        .filter(|status| !status.exists)
        .map(|status| status.path.clone())
        .collect();
    let docs_missing_notice: Vec<String> = legacy_docs
        .iter()
        .filter(|status| {
            status.exists
                && (!status.has_historical_notice || !status.references_comprehensive_spec)
        })
        .map(|status| status.path.clone())
        .collect();

    let duplicate_sections = duplicate_headers.into_iter().collect::<Vec<_>>();
    let missing_sections = missing_headers.into_iter().collect::<Vec<_>>();
    let broken_internal_references = toc_alignment_errors.clone();

    SpecIntegrityReport {
        bead_id: BEAD_ID.to_string(),
        spec_path: SPEC_REL_PATH.to_string(),
        report_path: REPORT_REL_PATH.to_string(),
        preamble_present: preamble_present(&spec_text),
        supersession_list_present: supersession_list_present(&spec_text),
        metadata_header_parseable: metadata_header_parseable(&spec_text),
        legacy_docs,
        missing_docs: missing_docs.clone(),
        docs_missing_notice: docs_missing_notice.clone(),
        duplicate_sections: duplicate_sections.clone(),
        missing_sections: missing_sections.clone(),
        toc_alignment_errors: toc_alignment_errors.clone(),
        broken_internal_references: broken_internal_references.clone(),
        summary: SpecIntegritySummary {
            toc_entries: toc_entries.len(),
            section_headers: headers.len(),
            missing_docs: missing_docs.len(),
            docs_missing_notice: docs_missing_notice.len(),
            toc_alignment_errors: toc_alignment_errors.len(),
            duplicate_sections: duplicate_sections.len(),
            missing_sections: missing_sections.len(),
            broken_internal_references: broken_internal_references.len(),
        },
    }
}

fn write_report(report: &SpecIntegrityReport) -> PathBuf {
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

    let report_json = serde_json::to_string_pretty(report);
    assert!(
        report_json.is_ok(),
        "bead_id={BEAD_ID} case=report_serialization_failed error={}",
        report_json
            .err()
            .as_ref()
            .map_or_else(|| "unknown".to_string(), std::string::ToString::to_string)
    );

    let write_result = fs::write(
        &output_path,
        report_json.expect("report serialization should succeed after is_ok assertion"),
    );
    assert!(
        write_result.is_ok(),
        "bead_id={BEAD_ID} case=report_write_failed path={} error={}",
        output_path.display(),
        write_result
            .err()
            .as_ref()
            .map_or_else(|| "unknown".to_string(), std::string::ToString::to_string)
    );
    output_path
}

#[test]
fn test_spec_preamble_present() {
    let report = build_integrity_report();
    assert!(
        report.preamble_present,
        "bead_id={BEAD_ID} case=missing_spec_preamble"
    );
}

#[test]
fn test_supersession_list_present() {
    let report = build_integrity_report();
    assert!(
        report.supersession_list_present,
        "bead_id={BEAD_ID} case=missing_supersession_list"
    );
    assert!(
        report.missing_docs.is_empty(),
        "bead_id={BEAD_ID} case=missing_superseded_docs docs={:?}",
        report.missing_docs
    );
}

#[test]
fn test_superseded_docs_marked_historical() {
    let report = build_integrity_report();
    assert!(
        report.docs_missing_notice.is_empty(),
        "bead_id={BEAD_ID} case=legacy_docs_missing_notice docs={:?}",
        report.docs_missing_notice
    );
}

#[test]
fn test_toc_matches_headers() {
    let report = build_integrity_report();
    assert!(
        report.toc_alignment_errors.is_empty(),
        "bead_id={BEAD_ID} case=toc_header_mismatch errors={:?}",
        report.toc_alignment_errors
    );
}

#[test]
fn test_all_sections_0_through_23_present_once() {
    let report = build_integrity_report();
    assert!(
        report.duplicate_sections.is_empty(),
        "bead_id={BEAD_ID} case=duplicate_sections sections={:?}",
        report.duplicate_sections
    );
    assert!(
        report.missing_sections.is_empty(),
        "bead_id={BEAD_ID} case=missing_sections sections={:?}",
        report.missing_sections
    );
}

#[test]
fn test_bd_1wx_5_unit_compliance_gate() {
    let report_a = build_integrity_report();
    let report_b = build_integrity_report();
    assert_eq!(
        report_a, report_b,
        "bead_id={BEAD_ID} case=non_deterministic_report"
    );
    assert!(
        report_a.metadata_header_parseable,
        "bead_id={BEAD_ID} case=metadata_header_unparseable"
    );
}

proptest! {
    #[test]
    fn prop_bd_1wx_5_structure_compliance(sections in proptest::collection::vec(0u8..=23u8, 0..80)) {
        let (duplicates, missing) = section_duplicate_and_missing(&sections);
        let mut counts = [0usize; 24];
        for section in &sections {
            counts[usize::from(*section)] += 1;
        }

        for section in EXPECTED_SECTIONS {
            let count = counts[usize::from(section)];
            prop_assert_eq!(
                duplicates.contains(&section),
                count > 1,
                "bead_id={} case=prop_duplicates section={} count={}",
                BEAD_ID,
                section,
                count
            );
            prop_assert_eq!(
                missing.contains(&section),
                count == 0,
                "bead_id={} case=prop_missing section={} count={}",
                BEAD_ID,
                section,
                count
            );
        }
    }
}

#[test]
fn test_e2e_bd_1wx_5_compliance() {
    let report = build_integrity_report();

    eprintln!(
        "INFO bead_id={BEAD_ID} case=audit_summary toc_entries={} section_headers={} missing_docs={} docs_missing_notice={} toc_errors={} duplicate_sections={} missing_sections={} broken_refs={}",
        report.summary.toc_entries,
        report.summary.section_headers,
        report.summary.missing_docs,
        report.summary.docs_missing_notice,
        report.summary.toc_alignment_errors,
        report.summary.duplicate_sections,
        report.summary.missing_sections,
        report.summary.broken_internal_references
    );
    for missing_doc in &report.missing_docs {
        eprintln!("WARN bead_id={BEAD_ID} case=legacy_doc_missing path={missing_doc}");
    }
    for missing_notice in &report.docs_missing_notice {
        eprintln!("WARN bead_id={BEAD_ID} case=missing_historical_notice path={missing_notice}");
    }
    for error in &report.toc_alignment_errors {
        eprintln!("ERROR bead_id={BEAD_ID} case=toc_alignment_failure detail={error}");
    }
    for missing_section in &report.missing_sections {
        eprintln!("ERROR bead_id={BEAD_ID} case=missing_section section={missing_section}");
    }
    for duplicate_section in &report.duplicate_sections {
        eprintln!("ERROR bead_id={BEAD_ID} case=duplicate_section section={duplicate_section}");
    }

    assert!(
        report.missing_docs.is_empty(),
        "bead_id={BEAD_ID} case=e2e_missing_docs docs={:?}",
        report.missing_docs
    );
    assert!(
        report.docs_missing_notice.is_empty(),
        "bead_id={BEAD_ID} case=e2e_missing_historical_notice docs={:?}",
        report.docs_missing_notice
    );
    assert!(
        report.duplicate_sections.is_empty(),
        "bead_id={BEAD_ID} case=e2e_duplicate_sections sections={:?}",
        report.duplicate_sections
    );
    assert!(
        report.missing_sections.is_empty(),
        "bead_id={BEAD_ID} case=e2e_missing_sections sections={:?}",
        report.missing_sections
    );
    assert!(
        report.toc_alignment_errors.is_empty(),
        "bead_id={BEAD_ID} case=e2e_toc_alignment_errors errors={:?}",
        report.toc_alignment_errors
    );
}

#[test]
fn test_e2e_spec_integrity_audit() {
    let report = build_integrity_report();
    let output_path = write_report(&report);

    let report_value: serde_json::Value =
        serde_json::from_slice(&fs::read(&output_path).expect("report JSON should be readable"))
            .expect("report JSON should parse");

    assert!(
        report_value.get("summary").is_some()
            && report_value.get("missing_docs").is_some()
            && report_value.get("toc_alignment_errors").is_some()
            && report_value.get("broken_internal_references").is_some(),
        "bead_id={BEAD_ID} case=e2e_report_schema report_keys={:?}",
        report_value
            .as_object()
            .map(|obj| obj.keys().cloned().collect::<Vec<_>>())
    );
}
