//! Contract, discovery, and E2E report tests for the Turso intake baseline.

use std::fs;
use std::path::{Path, PathBuf};

use fsqlite_harness::test_inventory::{
    AdoptionDecision, DEFAULT_TURSO_INVENTORY_CONTRACT_PATH, GitSnapshot, InventoryRunMetadata,
    SourceKind, TestClass, UpstreamTree, UpstreamTreeEntry, build_test_inventory_report,
    classify_test_entries, find_duplicate_groups, load_test_inventory_contract,
    render_test_inventory_csv, render_test_inventory_json, render_test_inventory_markdown,
    validate_test_inventory_contract, validate_upstream_tree, write_test_inventory_outputs,
};
use tempfile::TempDir;

const BEAD_ID: &str = "bd-turso-test-adaptation-zu081.1";

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn contract_path() -> PathBuf {
    workspace_root().join(DEFAULT_TURSO_INVENTORY_CONTRACT_PATH)
}

fn load_contract() -> fsqlite_harness::test_inventory::TestInventoryContract {
    load_test_inventory_contract(&contract_path()).expect("load canonical Turso inventory contract")
}

fn diagnostic_codes(
    diagnostics: &[fsqlite_harness::test_inventory::InventoryDiagnostic],
) -> Vec<&str> {
    diagnostics
        .iter()
        .map(|diagnostic| diagnostic.code.as_str())
        .collect()
}

fn synthetic_upstream_tree() -> UpstreamTree {
    let contract = load_contract();
    let mut tree = vec![
        UpstreamTreeEntry {
            path: contract.source.testing_path.clone(),
            entry_type: "tree".to_owned(),
            sha: contract.source.testing_tree_sha.clone(),
            size: None,
        },
        UpstreamTreeEntry {
            path: contract.source.license_path.clone(),
            entry_type: "blob".to_owned(),
            sha: contract.source.license_blob_sha.clone(),
            size: Some(1_070),
        },
    ];

    for entry in &contract.portfolio {
        match entry.source_kind {
            SourceKind::File => tree.push(UpstreamTreeEntry {
                path: entry.source_path.clone(),
                entry_type: "blob".to_owned(),
                sha: "1111111111111111111111111111111111111111".to_owned(),
                size: Some(1),
            }),
            SourceKind::Directory => {
                tree.push(UpstreamTreeEntry {
                    path: entry.source_path.clone(),
                    entry_type: "tree".to_owned(),
                    sha: "2222222222222222222222222222222222222222".to_owned(),
                    size: None,
                });
                for index in 1..entry.tree_count {
                    tree.push(UpstreamTreeEntry {
                        path: format!("{}/tree-{index}", entry.source_path),
                        entry_type: "tree".to_owned(),
                        sha: "3333333333333333333333333333333333333333".to_owned(),
                        size: None,
                    });
                }
                for index in 0..entry.blob_count {
                    tree.push(UpstreamTreeEntry {
                        path: format!("{}/blob-{index}", entry.source_path),
                        entry_type: "blob".to_owned(),
                        sha: "4444444444444444444444444444444444444444".to_owned(),
                        size: Some(index),
                    });
                }
            }
        }
    }

    UpstreamTree {
        sha: contract.source.commit,
        truncated: false,
        tree,
    }
}

#[test]
fn canonical_contract_is_self_contained_and_valid() {
    let root = workspace_root();
    let snapshot = GitSnapshot::capture(&root).expect("capture tracked HEAD");
    let contract = load_contract();
    let diagnostics = validate_test_inventory_contract(&contract, &snapshot);
    assert!(
        diagnostics.is_empty(),
        "canonical contract diagnostics: {diagnostics:#?}"
    );
    assert_eq!(contract.meta.bead_id, BEAD_ID);
    assert_eq!(contract.portfolio.len(), 17);
    assert_eq!(
        contract
            .portfolio
            .iter()
            .map(|entry| entry.entry_count)
            .sum::<usize>(),
        373
    );
    assert_eq!(
        contract
            .portfolio
            .iter()
            .filter(|entry| entry.decision == AdoptionDecision::Adopt)
            .count(),
        8
    );
    assert_eq!(contract.contract_authority.len(), 5);
    assert!(
        contract
            .contract_authority
            .iter()
            .all(|authority| authority.handoff_bead == "bd-turso-test-adaptation-zu081.18")
    );
}

#[test]
fn pinned_tree_metadata_matches_every_reviewed_portfolio_entry() {
    let contract = load_contract();
    let upstream = synthetic_upstream_tree();
    let diagnostics = validate_upstream_tree(&contract, &upstream);
    assert!(
        diagnostics.is_empty(),
        "synthetic pinned tree diagnostics: {diagnostics:#?}"
    );
}

#[test]
fn pinned_tree_validation_fails_closed_for_unknown_truncated_and_missing_license_inputs() {
    let contract = load_contract();

    let mut unknown = synthetic_upstream_tree();
    unknown.tree.push(UpstreamTreeEntry {
        path: "testing/new-unreviewed-family".to_owned(),
        entry_type: "tree".to_owned(),
        sha: "5555555555555555555555555555555555555555".to_owned(),
        size: None,
    });
    let diagnostics = validate_upstream_tree(&contract, &unknown);
    assert!(diagnostic_codes(&diagnostics).contains(&"unknown_upstream_subtree"));

    let mut truncated = synthetic_upstream_tree();
    truncated.truncated = true;
    let diagnostics = validate_upstream_tree(&contract, &truncated);
    assert!(diagnostic_codes(&diagnostics).contains(&"upstream_tree_truncated"));

    let mut missing_license = synthetic_upstream_tree();
    missing_license
        .tree
        .retain(|entry| entry.path != contract.source.license_path);
    let diagnostics = validate_upstream_tree(&contract, &missing_license);
    assert!(diagnostic_codes(&diagnostics).contains(&"license_blob_missing"));
}

#[test]
fn provenance_schema_rejects_missing_revision_license_and_classification() {
    let root = workspace_root();
    let snapshot = GitSnapshot::capture(&root).expect("capture tracked HEAD");
    let mut contract = load_contract();
    contract.source.commit.clear();
    contract.source.license_spdx.clear();
    contract.source.license_class.clear();
    let diagnostics = validate_test_inventory_contract(&contract, &snapshot);
    let codes = diagnostic_codes(&diagnostics);
    assert!(codes.contains(&"source_commit_invalid"));
    assert!(codes.contains(&"required_field_missing"));
    assert!(codes.contains(&"license_classification_invalid"));
}

#[test]
fn ownership_and_handoff_validation_reject_stale_or_incomplete_records() {
    let root = workspace_root();
    let snapshot = GitSnapshot::capture(&root).expect("capture tracked HEAD");

    let mut stale_owner = load_contract();
    stale_owner.portfolio[0]
        .owner_paths
        .push("crates/does-not-exist/src/lib.rs".to_owned());
    stale_owner.portfolio[0]
        .owner_beads
        .push("bd-does-not-exist".to_owned());
    stale_owner.portfolio[0]
        .surface_ids
        .push("SURF-DOES-NOT-EXIST".to_owned());
    let diagnostics = validate_test_inventory_contract(&stale_owner, &snapshot);
    let codes = diagnostic_codes(&diagnostics);
    assert!(codes.contains(&"stale_owner_path"));
    assert!(codes.contains(&"stale_owner_bead"));
    assert!(codes.contains(&"unknown_surface_id"));

    let mut incomplete_handoff = load_contract();
    incomplete_handoff.contract_authority.pop();
    incomplete_handoff.contract_authority[0].handoff_bead = "bd-missing".to_owned();
    let diagnostics = validate_test_inventory_contract(&incomplete_handoff, &snapshot);
    let codes = diagnostic_codes(&diagnostics);
    assert!(codes.contains(&"contract_authority_set_incomplete"));
    assert!(codes.contains(&"contract_handoff_incomplete"));
}

#[test]
fn controlled_tree_covers_all_classes_storage_flags_and_duplicates() {
    let temp = TempDir::new().expect("create controlled tree");
    let fixtures = [
        (
            "crates/demo/src/lib.rs",
            "#[cfg(test)]\nmod tests { #[test]\nfn unit() {} }",
        ),
        (
            "crates/demo/tests/integration.rs",
            "#[test]\nfn integration() { let _ = tempfile::TempDir::new(); }",
        ),
        (
            "crates/demo/tests/duplicate.rs",
            "#[test]\nfn integration() { let _ = tempfile::TempDir::new(); }",
        ),
        (
            "crates/fsqlite-e2e/tests/public.rs",
            "#[test]\nfn e2e() { let _ = rusqlite::Connection::open_in_memory(); }",
        ),
        (
            "crates/fsqlite-harness/tests/tracker.rs",
            "const PATH: &str = \".beads/issues.jsonl\";\n#[test]\nfn tracker() {}",
        ),
        (
            "conformance/slt/smoke/basic.slt",
            "statement ok\nSELECT 1;\n",
        ),
        ("fuzz/fuzz_targets/parser.rs", "#![no_main]\n"),
        ("fuzz/parser/corpus/seed", "SELECT 1;\n"),
    ];
    let mut entries = Vec::new();
    for (relative, content) in fixtures {
        let path = temp.path().join(relative);
        fs::create_dir_all(path.parent().expect("fixture parent")).expect("create fixture parent");
        fs::write(&path, content).expect("write controlled fixture");
        entries.push((
            relative.to_owned(),
            fs::read(path).expect("read controlled fixture"),
        ));
    }

    let records = classify_test_entries(entries).expect("classify controlled tree");
    for expected in [
        TestClass::Unit,
        TestClass::Integration,
        TestClass::Corpus,
        TestClass::Fuzz,
        TestClass::E2e,
        TestClass::TrackerMetadata,
    ] {
        assert!(
            records.iter().any(|record| record.class == expected),
            "missing class {expected:?}"
        );
    }
    assert!(records.iter().any(|record| record.uses_file_backend));
    assert!(records.iter().any(|record| record.uses_rusqlite));
    assert!(records.iter().any(|record| record.uses_literal_beads_path));
    let duplicates = find_duplicate_groups(&records);
    assert_eq!(duplicates.len(), 1);
    assert_eq!(duplicates[0].paths.len(), 2);
}

#[test]
fn controlled_tree_rejects_unknown_test_layout() {
    let diagnostics = classify_test_entries(vec![(
        "mystery/place/test.rs".to_owned(),
        b"#[test]\nfn unknown() {}\n".to_vec(),
    )])
    .expect_err("unknown test layout must fail closed");
    assert!(diagnostic_codes(&diagnostics).contains(&"unknown_test_class"));
}

#[test]
fn report_json_markdown_and_csv_reconcile_from_one_model() {
    let root = workspace_root();
    let report = build_test_inventory_report(
        &root,
        &contract_path(),
        None,
        InventoryRunMetadata {
            run_id: "inventory-test-run".to_owned(),
            trace_id: "inventory-test-trace".to_owned(),
            scenario_id: "TURSO-TEST-INVENTORY-V1".to_owned(),
            generated_unix_ms: 0,
            command: "scripts/test_inventory.sh full".to_owned(),
        },
    )
    .expect("build canonical report");
    let json = render_test_inventory_json(&report).expect("render JSON");
    let decoded: fsqlite_harness::test_inventory::TestInventoryReport =
        serde_json::from_str(&json).expect("decode rendered JSON");
    assert_eq!(decoded, report);

    let markdown = render_test_inventory_markdown(&report);
    assert!(markdown.contains("harness_tracker_shaped_files` | 68 | 68 | +0"));
    assert!(markdown.contains("harness_literal_beads_path_files` | 64 | 64 | +0"));
    assert!(markdown.contains("testing/differential-oracle"));
    assert!(markdown.contains("Decision totals: adopt=8, defer=7, reject=2."));
    assert!(markdown.contains("Contract Authority Handoff"));

    let csv = render_test_inventory_csv(&report);
    assert!(csv.starts_with("crate,file,test_count,realism_tier"));
    assert!(csv.contains("tracker-metadata"));
    assert!(csv.contains("conformance/slt/smoke/basic.slt"));

    let temp = TempDir::new().expect("create output directory");
    let json_path = temp.path().join("report.json");
    let markdown_path = temp.path().join("report.md");
    let csv_path = temp.path().join("report.csv");
    write_test_inventory_outputs(&report, &json_path, &markdown_path, &csv_path)
        .expect("write all report views");
    assert_eq!(fs::read_to_string(json_path).expect("read JSON"), json);
    assert_eq!(
        fs::read_to_string(markdown_path).expect("read Markdown"),
        markdown
    );
    assert_eq!(fs::read_to_string(csv_path).expect("read CSV"), csv);
}
