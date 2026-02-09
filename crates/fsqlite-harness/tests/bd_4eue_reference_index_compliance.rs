//! §20 key reference files audit and compliance gates (bd-4eue).
//!
//! This test module does two things:
//! 1. Validates bead-structure compliance markers in `.beads/issues.jsonl`.
//! 2. Audits the concrete reference index paths and API-shape guardrails.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use proptest::prelude::proptest;
use proptest::test_runner::TestCaseError;
use serde_json::Value;

const BEAD_ID: &str = "bd-4eue";
const ISSUES_JSONL: &str = ".beads/issues.jsonl";
const UNIT_TEST_IDS: [&str; 2] = [
    "test_bd_4eue_unit_compliance_gate",
    "prop_bd_4eue_structure_compliance",
];
const E2E_TEST_IDS: [&str; 2] = ["test_e2e_bd_4eue", "test_e2e_bd_4eue_compliance"];
const LOG_LEVEL_MARKERS: [&str; 4] = ["DEBUG", "INFO", "WARN", "ERROR"];
const LOG_STANDARD_REF: &str = "bd-1fpm";
const REQUIRED_TOKENS: [&str; 9] = [
    "test_bd_4eue_unit_compliance_gate",
    "prop_bd_4eue_structure_compliance",
    "test_e2e_bd_4eue",
    "test_e2e_bd_4eue_compliance",
    "DEBUG",
    "INFO",
    "WARN",
    "ERROR",
    "bd-1fpm",
];

const C_SQLITE_REFERENCE_FILES: [&str; 15] = [
    "legacy_sqlite_code/sqlite/src/sqliteInt.h",
    "legacy_sqlite_code/sqlite/src/btree.c",
    "legacy_sqlite_code/sqlite/src/pager.c",
    "legacy_sqlite_code/sqlite/src/wal.c",
    "legacy_sqlite_code/sqlite/src/vdbe.c",
    "legacy_sqlite_code/sqlite/src/select.c",
    "legacy_sqlite_code/sqlite/src/where.c",
    "legacy_sqlite_code/sqlite/src/wherecode.c",
    "legacy_sqlite_code/sqlite/src/whereexpr.c",
    "legacy_sqlite_code/sqlite/src/whereInt.h",
    "legacy_sqlite_code/sqlite/src/parse.y",
    "legacy_sqlite_code/sqlite/src/tokenize.c",
    "legacy_sqlite_code/sqlite/src/func.c",
    "legacy_sqlite_code/sqlite/src/expr.c",
    "legacy_sqlite_code/sqlite/src/build.c",
];

const PROJECT_REFERENCE_DOCS: [&str; 6] = [
    "COMPREHENSIVE_SPEC_FOR_FRANKENSQLITE_V1.md",
    "EXISTING_SQLITE_STRUCTURE.md",
    "docs/rfc6330.txt",
    "AGENTS.md",
    "MVCC_SPECIFICATION.md",
    "PROPOSED_ARCHITECTURE.md",
];

const ASUPERSYNC_REFERENCE_MODULES: [&str; 11] = [
    "/dp/asupersync/src/raptorq",
    "/dp/asupersync/src/sync",
    "/dp/asupersync/src/channel/mpsc.rs",
    "/dp/asupersync/src/channel/oneshot.rs",
    "/dp/asupersync/src/cx",
    "/dp/asupersync/src/lab/runtime.rs",
    "/dp/asupersync/src/lab/explorer.rs",
    "/dp/asupersync/src/obligation/eprocess.rs",
    "/dp/asupersync/src/lab/oracle/eprocess.rs",
    "/dp/asupersync/src/lab/conformal.rs",
    "/dp/asupersync/src/database/sqlite.rs",
];

const MAJOR_SUBSYSTEM_HINTS: [&str; 7] =
    ["btree", "pager", "wal", "vdbe", "parser", "planner", "func"];

const C_SQLITE_SOURCE_MARKERS: [&str; 16] = [
    "legacy_sqlite_code/sqlite/src/",
    "sqliteInt.h",
    "btree.c",
    "pager.c",
    "wal.c",
    "vdbe.c",
    "select.c",
    "where.c",
    "wherecode.c",
    "whereexpr.c",
    "whereInt.h",
    "parse.y",
    "tokenize.c",
    "func.c",
    "expr.c",
    "build.c",
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
    text.split(|ch: char| !(ch.is_ascii_alphanumeric() || ch == '_' || ch == '.'))
        .any(|x| x == expected_marker)
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

fn canonical_text(path: &Path) -> Result<String, String> {
    fs::read_to_string(path)
        .map_err(|error| format!("read_failed path={} error={error}", path.display()))
}

fn contains_source_marker_token(text: &str, needle: &str) -> bool {
    text.split(|ch: char| {
        !(ch.is_ascii_alphanumeric() || ch == '_' || ch == '.' || ch == '/' || ch == '-')
    })
    .any(|segment| segment == needle)
}

fn collect_rust_files(dir: &Path, out: &mut Vec<PathBuf>) -> Result<(), String> {
    let entries = fs::read_dir(dir)
        .map_err(|error| format!("read_dir_failed path={} error={error}", dir.display()))?;
    for entry in entries {
        let entry = entry.map_err(|error| {
            format!("read_dir_entry_failed path={} error={error}", dir.display())
        })?;
        let path = entry.path();
        if path.is_dir() {
            collect_rust_files(&path, out)?;
            continue;
        }
        if path.extension().and_then(|ext| ext.to_str()) == Some("rs") {
            out.push(path);
        }
    }
    Ok(())
}

fn missing_relative_paths(root: &Path, paths: &[&str]) -> Vec<String> {
    paths
        .iter()
        .filter_map(|relative| {
            let absolute = root.join(relative);
            (!absolute.exists()).then(|| (*relative).to_owned())
        })
        .collect::<Vec<_>>()
}

fn missing_absolute_paths(paths: &[&str]) -> Vec<String> {
    paths
        .iter()
        .filter(|absolute| !Path::new(absolute).exists())
        .map(|absolute| (*absolute).to_owned())
        .collect::<Vec<_>>()
}

#[test]
fn test_bd_4eue_unit_compliance_gate() -> Result<(), String> {
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
    fn prop_bd_4eue_structure_compliance(missing_index in 0usize..REQUIRED_TOKENS.len()) {
        let mut synthetic = format!(
            "## Unit Test Requirements\n- {}\n- {}\n\n\
             ## E2E Test\n- {}\n- {}\n\n\
             ## Logging Requirements\n- DEBUG: stage progress\n- INFO: summary\n\
             - WARN: degraded mode\n- ERROR: terminal failure\n- Reference: {}\n\n\
             C source list root: legacy_sqlite_code/sqlite/src\n",
            UNIT_TEST_IDS[0],
            UNIT_TEST_IDS[1],
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
fn test_reference_paths_exist_in_repo() -> Result<(), String> {
    let root = workspace_root()?;
    let mut missing = missing_relative_paths(&root, &PROJECT_REFERENCE_DOCS);
    missing.extend(missing_relative_paths(&root, &C_SQLITE_REFERENCE_FILES));
    missing.sort();

    if !missing.is_empty() {
        return Err(format!(
            "bead_id={BEAD_ID} case=reference_paths_missing missing={missing:?}"
        ));
    }

    Ok(())
}

#[test]
fn test_reference_table_has_no_duplicates() {
    let c_count = C_SQLITE_REFERENCE_FILES.len();
    let c_unique_count = C_SQLITE_REFERENCE_FILES
        .iter()
        .collect::<BTreeSet<_>>()
        .len();
    assert_eq!(
        c_count, c_unique_count,
        "bead_id={BEAD_ID} case=duplicate_c_reference_entries total={c_count} unique={c_unique_count}"
    );

    let doc_count = PROJECT_REFERENCE_DOCS.len();
    let doc_unique_count = PROJECT_REFERENCE_DOCS.iter().collect::<BTreeSet<_>>().len();
    assert_eq!(
        doc_count, doc_unique_count,
        "bead_id={BEAD_ID} case=duplicate_doc_reference_entries total={doc_count} unique={doc_unique_count}"
    );
}

#[test]
fn test_reference_index_covers_all_major_subsystems() -> Result<(), String> {
    let description = load_issue_description(BEAD_ID)?;
    let lowercase = description.to_ascii_lowercase();

    let missing = MAJOR_SUBSYSTEM_HINTS
        .iter()
        .filter(|subsystem| !lowercase.contains(**subsystem))
        .copied()
        .collect::<Vec<_>>();

    if !missing.is_empty() {
        return Err(format!(
            "bead_id={BEAD_ID} case=missing_major_subsystem_hints missing={missing:?}"
        ));
    }

    Ok(())
}

#[test]
fn test_asupersync_reference_paths_exist() {
    let missing = missing_absolute_paths(&ASUPERSYNC_REFERENCE_MODULES);
    assert!(
        missing.is_empty(),
        "bead_id={BEAD_ID} case=missing_asupersync_modules missing={missing:?}"
    );
}

#[test]
fn test_api_mirrors_asupersync_sqlite() -> Result<(), String> {
    let root = workspace_root()?;
    let asupersync_sqlite = canonical_text(Path::new("/dp/asupersync/src/database/sqlite.rs"))?;
    let connection_api = canonical_text(&root.join("crates/fsqlite-core/src/connection.rs"))?;
    let facade = canonical_text(&root.join("crates/fsqlite/src/lib.rs"))?;

    for marker in [
        "pub async fn open(",
        "pub async fn execute(",
        "pub async fn query(",
    ] {
        if !asupersync_sqlite.contains(marker) {
            return Err(format!(
                "bead_id={BEAD_ID} case=asupersync_api_marker_missing marker={marker}"
            ));
        }
    }

    for marker in [
        "pub fn open(",
        "pub fn prepare(",
        "pub fn execute(",
        "pub fn query(",
    ] {
        if !connection_api.contains(marker) {
            return Err(format!(
                "bead_id={BEAD_ID} case=fsqlite_connection_marker_missing marker={marker}"
            ));
        }
    }

    if !facade.contains("pub use fsqlite_core::connection::{Connection, PreparedStatement, Row};") {
        return Err(format!(
            "bead_id={BEAD_ID} case=fsqlite_facade_missing_connection_reexport"
        ));
    }

    Ok(())
}

#[test]
fn test_no_direct_csqlite_source_imports() -> Result<(), String> {
    let root = workspace_root()?;
    let crates_dir = root.join("crates");
    let mut rust_files = Vec::new();
    collect_rust_files(&crates_dir, &mut rust_files)?;

    let mut hits = Vec::new();
    for file in rust_files {
        if file
            .components()
            .any(|component| component.as_os_str() == "fsqlite-harness")
        {
            continue;
        }
        let text = canonical_text(&file)?;
        for marker in &C_SQLITE_SOURCE_MARKERS {
            if contains_source_marker_token(&text, marker) {
                hits.push(format!("{}::{marker}", file.display()));
            }
        }
    }

    if !hits.is_empty() {
        return Err(format!(
            "bead_id={BEAD_ID} case=direct_csqlite_source_references_found count={} hits={hits:?}",
            hits.len()
        ));
    }

    Ok(())
}

#[test]
fn test_e2e_bd_4eue_compliance() -> Result<(), String> {
    let description = load_issue_description(BEAD_ID)?;
    let evaluation = evaluate_description(&description);
    let root = workspace_root()?;

    let missing_docs = missing_relative_paths(&root, &PROJECT_REFERENCE_DOCS);
    let missing_c_sources = missing_relative_paths(&root, &C_SQLITE_REFERENCE_FILES);
    let missing_asupersync = missing_absolute_paths(&ASUPERSYNC_REFERENCE_MODULES);

    eprintln!(
        "DEBUG bead_id={BEAD_ID} case=e2e_reference_audit docs_total={} c_sources_total={} asupersync_total={}",
        PROJECT_REFERENCE_DOCS.len(),
        C_SQLITE_REFERENCE_FILES.len(),
        ASUPERSYNC_REFERENCE_MODULES.len()
    );
    eprintln!(
        "INFO bead_id={BEAD_ID} case=e2e_summary missing_unit_ids={} missing_e2e_ids={} missing_log_levels={} missing_log_standard_ref={} missing_docs={} missing_c_sources={} missing_asupersync={}",
        evaluation.missing_unit_ids.len(),
        evaluation.missing_e2e_ids.len(),
        evaluation.missing_log_levels.len(),
        evaluation.missing_log_standard_ref,
        missing_docs.len(),
        missing_c_sources.len(),
        missing_asupersync.len(),
    );

    for missing in &missing_docs {
        eprintln!("WARN bead_id={BEAD_ID} case=missing_project_doc path={missing}");
    }
    for missing in &missing_c_sources {
        eprintln!("WARN bead_id={BEAD_ID} case=missing_c_source_reference path={missing}");
    }
    for missing in &missing_asupersync {
        eprintln!("WARN bead_id={BEAD_ID} case=missing_asupersync_reference path={missing}");
    }
    if evaluation.missing_log_standard_ref {
        eprintln!(
            "ERROR bead_id={BEAD_ID} case=missing_log_standard_ref expected={LOG_STANDARD_REF}"
        );
    }

    if !evaluation.is_compliant()
        || !missing_docs.is_empty()
        || !missing_c_sources.is_empty()
        || !missing_asupersync.is_empty()
    {
        return Err(format!(
            "bead_id={BEAD_ID} case=e2e_compliance_failure evaluation={evaluation:?} missing_docs={missing_docs:?} missing_c_sources={missing_c_sources:?} missing_asupersync={missing_asupersync:?}"
        ));
    }

    Ok(())
}

#[test]
fn test_e2e_bd_4eue() -> Result<(), String> {
    test_e2e_bd_4eue_compliance()
}
