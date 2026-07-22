//! bd-2bjaf: Tests for the corruption fingerprint extractor and normalizer.
//!
//! Validates parsing of PRAGMA integrity_check output into structured
//! failures and normalization into stable signatures for grouping.

use std::fs::OpenOptions;
use std::io::{Read as _, Seek as _, SeekFrom, Write as _};

use fsqlite_e2e::corruption_fingerprint::{
    FailureKind, NormalizedSignature, RefCountClass, TreeIdClass, classify_sqlite_artifact,
    fingerprint, fingerprint_collection, inventory_report, normalize, parse_failure,
    parse_integrity_check_output,
};

const _BEAD_ID: &str = "bd-2bjaf";

#[test]
fn t1_parse_2nd_reference_basic() {
    let line = "Tree 20 page 20 cell 80: 2nd reference to page 5366";
    let f = parse_failure(line).unwrap();
    assert_eq!(f.tree_id, Some(20));
    assert_eq!(f.page, Some(20));
    assert_eq!(f.cell, Some(80));
    assert_eq!(f.kind, FailureKind::PageDoubleReference { ref_count: 2 });
    assert_eq!(f.referenced_page, Some(5366));
}

#[test]
fn t2_parse_2nd_reference_low_cell() {
    let line = "Tree 32 page 32 cell 0: 2nd reference to page 491";
    let f = parse_failure(line).unwrap();
    assert_eq!(f.tree_id, Some(32));
    assert_eq!(f.page, Some(32));
    assert_eq!(f.cell, Some(0));
    assert_eq!(f.referenced_page, Some(491));
}

#[test]
fn t3_parse_tree_page_free_space_corruption() {
    let line = "Tree 5 page 12: free space corruption";
    let f = parse_failure(line).unwrap();
    assert_eq!(f.tree_id, Some(5));
    assert_eq!(f.page, Some(12));
    assert_eq!(f.cell, None);
    assert_eq!(f.kind, FailureKind::FreeSpaceCorruption);
}

#[test]
fn t4_parse_rowid_out_of_order() {
    let line = "Tree 3 page 7 cell 5: Rowid 42 out of order";
    let f = parse_failure(line).unwrap();
    assert_eq!(f.tree_id, Some(3));
    assert_eq!(f.page, Some(7));
    assert_eq!(f.cell, Some(5));
    assert_eq!(f.kind, FailureKind::RowidOutOfOrder);
}

#[test]
fn t5_parse_extends_off_end() {
    let line = "Tree 1 page 3 cell 2: Extends off end of page";
    let f = parse_failure(line).unwrap();
    assert_eq!(f.kind, FailureKind::ExtendsOffEnd);
}

#[test]
fn t6_parse_offset_out_of_range() {
    let line = "Tree 1 page 3 cell 2: Offset 100 out of range 10..90";
    let f = parse_failure(line).unwrap();
    match f.kind {
        FailureKind::OffsetOutOfRange {
            offset,
            range_lo,
            range_hi,
        } => {
            assert_eq!(offset, 100);
            assert_eq!(range_lo, 10);
            assert_eq!(range_hi, 90);
        }
        _ => panic!("expected OffsetOutOfRange, got {:?}", f.kind),
    }
}

#[test]
fn t7_parse_child_page_depth() {
    let line = "Tree 10 page 50 cell 3: Child page depth differs";
    let f = parse_failure(line).unwrap();
    assert_eq!(f.kind, FailureKind::ChildPageDepthDiffers);
}

#[test]
fn t8_parse_page_never_used() {
    let line = "Page 42: never used";
    let f = parse_failure(line).unwrap();
    assert_eq!(f.tree_id, None);
    assert_eq!(f.page, Some(42));
    assert_eq!(f.kind, FailureKind::PageNeverUsed);
}

#[test]
fn t9_parse_page_pointer_map() {
    let line = "Page 7: pointer map referenced";
    let f = parse_failure(line).unwrap();
    assert_eq!(f.kind, FailureKind::PagePointerMapReferenced);
}

#[test]
fn t10_parse_multiple_uses() {
    let line = "Page 5: Multiple uses for byte 100 of page 5";
    let f = parse_failure(line).unwrap();
    assert_eq!(f.kind, FailureKind::MultipleUsesForByte);
}

#[test]
fn t11_parse_invalid_page_number() {
    let line = "invalid page number 99999";
    let f = parse_failure(line).unwrap();
    assert_eq!(f.kind, FailureKind::InvalidPageNumber);
    assert_eq!(f.page, Some(99999));
}

#[test]
fn t12_parse_freelist_leaf_count() {
    let line = "Tree 1 page 4: freelist leaf count too big on page 4";
    let f = parse_failure(line).unwrap();
    assert_eq!(f.kind, FailureKind::FreelistLeafCountTooBig);
}

#[test]
fn t13_parse_ok_returns_none() {
    assert!(parse_failure("ok").is_none());
}

#[test]
fn t14_parse_header_returns_none() {
    assert!(parse_failure("*** in database main ***").is_none());
}

#[test]
fn t15_parse_empty_returns_none() {
    assert!(parse_failure("").is_none());
    assert!(parse_failure("   ").is_none());
}

#[test]
fn t16_normalize_double_ref_small_tree() {
    let line = "Tree 5 page 5 cell 3: 2nd reference to page 100";
    let f = parse_failure(line).unwrap();
    let sig = normalize(&f);
    assert_eq!(sig.tree_id_class, Some(TreeIdClass::Small));
    assert_eq!(sig.ref_count_class, Some(RefCountClass::Single));
    assert!(sig.has_cell);
}

#[test]
fn t17_normalize_double_ref_medium_tree() {
    let line = "Tree 20 page 20 cell 80: 2nd reference to page 5366";
    let f = parse_failure(line).unwrap();
    let sig = normalize(&f);
    assert_eq!(sig.tree_id_class, Some(TreeIdClass::Medium));
    assert_eq!(sig.ref_count_class, Some(RefCountClass::Single));
}

#[test]
fn t18_normalize_double_ref_large_tree() {
    let line = "Tree 200 page 200 cell 1: 2nd reference to page 500";
    let f = parse_failure(line).unwrap();
    let sig = normalize(&f);
    assert_eq!(sig.tree_id_class, Some(TreeIdClass::Large));
}

#[test]
fn t19_normalize_strips_page_numbers() {
    let l1 = "Tree 20 page 20 cell 80: 2nd reference to page 5366";
    let l2 = "Tree 20 page 20 cell 1: 2nd reference to page 123";
    let s1 = normalize(&parse_failure(l1).unwrap());
    let s2 = normalize(&parse_failure(l2).unwrap());
    assert_eq!(s1, s2, "same tree class + kind = same signature");
}

#[test]
fn t20_normalize_different_tree_class_different_sig() {
    let l1 = "Tree 5 page 5 cell 1: 2nd reference to page 10";
    let l2 = "Tree 50 page 50 cell 1: 2nd reference to page 10";
    let s1 = normalize(&parse_failure(l1).unwrap());
    let s2 = normalize(&parse_failure(l2).unwrap());
    assert_ne!(s1, s2, "different tree class = different signature");
}

#[test]
fn t21_normalize_offset_strips_values() {
    let l1 = "Tree 1 page 3 cell 2: Offset 100 out of range 10..90";
    let l2 = "Tree 1 page 5 cell 7: Offset 200 out of range 20..180";
    let s1 = normalize(&parse_failure(l1).unwrap());
    let s2 = normalize(&parse_failure(l2).unwrap());
    assert_eq!(s1, s2, "offset values are stripped in normalization");
}

#[test]
fn t22_fingerprint_function() {
    let sig = fingerprint("Tree 20 page 20 cell 55: 2nd reference to page 4058").unwrap();
    assert_eq!(sig.tree_id_class, Some(TreeIdClass::Medium));
    assert!(sig.has_cell);
}

#[test]
fn t23_fingerprint_not_a_failure() {
    assert!(fingerprint("ok").is_err());
    assert!(fingerprint("").is_err());
}

#[test]
fn t24_fingerprint_collection_groups() {
    let lines = vec![
        "Tree 20 page 20 cell 80: 2nd reference to page 5366",
        "Tree 20 page 20 cell 79: 2nd reference to page 5262",
        "Tree 20 page 20 cell 1: 2nd reference to page 123",
        "Tree 32 page 32 cell 0: 2nd reference to page 491",
    ];
    let grouped = fingerprint_collection(&lines);
    assert_eq!(
        grouped.len(),
        1,
        "trees 20 and 32 are both Medium class — same signature"
    );
    let total: usize = grouped.values().map(|v| v.len()).sum();
    assert_eq!(total, 4);
}

#[test]
fn t25_fingerprint_collection_distinct_kinds() {
    let lines = vec![
        "Tree 20 page 20 cell 80: 2nd reference to page 5366",
        "Tree 20 page 20: free space corruption",
        "Page 42: never used",
    ];
    let grouped = fingerprint_collection(&lines);
    assert_eq!(grouped.len(), 3, "three distinct failure kinds");
}

#[test]
fn t26_parse_full_forensic_output() {
    let output = include_str!("../fixtures/beads_corruption_integrity_check.txt");
    let failures = parse_integrity_check_output(output);
    assert!(
        failures.len() >= 90,
        "forensic snapshot has ~100 failures, got {}",
        failures.len()
    );

    let lines: Vec<&str> = output
        .lines()
        .filter(|l| !l.trim().is_empty() && !l.starts_with("***"))
        .collect();
    let grouped = fingerprint_collection(&lines);
    assert!(
        grouped.len() <= 5,
        "forensic snapshot should normalize to few distinct signatures, got {}",
        grouped.len()
    );
}

#[test]
fn t27_inventory_report_produces_markdown() {
    let output = "*** in database main ***\nTree 20 page 20 cell 80: 2nd reference to page 5366\nTree 20 page 20 cell 79: 2nd reference to page 5262\nTree 5 page 5: free space corruption\n";
    let report = inventory_report(output);
    assert!(report.contains("# Corruption Fingerprint Inventory"));
    assert!(report.contains("Total failures: 3"));
    assert!(report.contains("Distinct signatures:"));
}

#[test]
fn t28_inventory_report_empty_output() {
    let report = inventory_report("ok\n");
    assert!(report.contains("No integrity check failures"));
}

#[test]
fn t29_signature_display_format() {
    let sig = NormalizedSignature {
        tree_id_class: Some(TreeIdClass::Medium),
        kind: FailureKind::PageDoubleReference { ref_count: 2 },
        ref_count_class: Some(RefCountClass::Single),
        has_cell: true,
    };
    let display = sig.to_string();
    assert!(display.contains("tree:medium"));
    assert!(display.contains("page_double_ref"));
    assert!(display.contains("cell"));
    assert!(display.contains("refs:single"));
}

#[test]
fn t30_parse_ptrmap_failed() {
    let line = "Tree 1 page 2 cell 0: Failed to read ptrmap key=5";
    let f = parse_failure(line).unwrap();
    assert_eq!(f.kind, FailureKind::PtrmapReadFailed);
}

#[test]
fn t31_parse_page_read_failed() {
    let line = "Tree 1 page 2 cell 0: failed to get page 99";
    let f = parse_failure(line).unwrap();
    assert_eq!(f.kind, FailureKind::PageReadFailed);
}

#[test]
fn t32_fingerprint_collection_count() {
    let output = include_str!("../fixtures/beads_corruption_integrity_check.txt");
    let lines: Vec<&str> = output
        .lines()
        .filter(|l| !l.trim().is_empty() && !l.starts_with("***"))
        .collect();
    let grouped = fingerprint_collection(&lines);
    let total_failures: usize = grouped.values().map(|v| v.len()).sum();
    assert_eq!(total_failures, lines.len(), "no failures lost in grouping");
}

#[test]
fn t33_all_forensic_failures_have_tree_and_page() {
    let output = include_str!("../fixtures/beads_corruption_integrity_check.txt");
    let failures = parse_integrity_check_output(output);
    for f in &failures {
        assert!(
            f.tree_id.is_some(),
            "all forensic entries have tree_id: {}",
            f.raw_line
        );
        assert!(
            f.page.is_some(),
            "all forensic entries have page: {}",
            f.raw_line
        );
        assert!(
            f.cell.is_some(),
            "all forensic entries have cell: {}",
            f.raw_line
        );
    }
}

#[test]
fn t34_forensic_snapshot_all_double_ref() {
    let output = include_str!("../fixtures/beads_corruption_integrity_check.txt");
    let failures = parse_integrity_check_output(output);
    for f in &failures {
        assert!(
            matches!(f.kind, FailureKind::PageDoubleReference { .. }),
            "all forensic entries should be PageDoubleReference, got {:?} for: {}",
            f.kind,
            f.raw_line
        );
    }
}

#[test]
fn t35_forensic_snapshot_trees_20_32_34() {
    let output = include_str!("../fixtures/beads_corruption_integrity_check.txt");
    let failures = parse_integrity_check_output(output);
    let trees: std::collections::BTreeSet<u32> =
        failures.iter().filter_map(|f| f.tree_id).collect();
    assert!(trees.contains(&20));
    assert!(trees.contains(&32));
    assert!(trees.contains(&34));
}

#[test]
fn t36_normalized_signature_is_stable_across_runs() {
    let line = "Tree 20 page 20 cell 80: 2nd reference to page 5366";
    let s1 = fingerprint(line).unwrap();
    let s2 = fingerprint(line).unwrap();
    assert_eq!(s1, s2);
    assert_eq!(format!("{s1}"), format!("{s2}"));
}

#[test]
fn t37_tree_id_class_boundaries() {
    assert_eq!(TreeIdClass::from_id(0), TreeIdClass::Small);
    assert_eq!(TreeIdClass::from_id(10), TreeIdClass::Small);
    assert_eq!(TreeIdClass::from_id(11), TreeIdClass::Medium);
    assert_eq!(TreeIdClass::from_id(100), TreeIdClass::Medium);
    assert_eq!(TreeIdClass::from_id(101), TreeIdClass::Large);
}

#[test]
fn t38_ref_count_class_boundaries() {
    assert_eq!(RefCountClass::from_count(1), RefCountClass::Single);
    assert_eq!(RefCountClass::from_count(2), RefCountClass::Few);
    assert_eq!(RefCountClass::from_count(5), RefCountClass::Few);
    assert_eq!(RefCountClass::from_count(6), RefCountClass::Many);
}

#[test]
fn t39_other_kind_preserved() {
    let line = "Tree 1 page 2 cell 3: something unexpected happened";
    let f = parse_failure(line).unwrap();
    match &f.kind {
        FailureKind::Other(s) => assert_eq!(s, "something unexpected happened"),
        _ => panic!("expected Other, got {:?}", f.kind),
    }
}

#[test]
fn t40_full_inventory_report_on_forensic_snapshot() {
    let output = include_str!("../fixtures/beads_corruption_integrity_check.txt");
    let report = inventory_report(output);
    assert!(report.contains("# Corruption Fingerprint Inventory"));
    assert!(report.contains("Total failures: 100"));
    assert!(report.contains("Distinct signatures:"));
    assert!(report.contains("page_double_ref"));
}

#[test]
fn t41_read_only_artifact_classifier_maps_index_freelist_and_sidecars() {
    let directory = tempfile::tempdir().expect("artifact tempdir");
    let database = directory.path().join("classifier.db");
    let connection = rusqlite::Connection::open(&database).expect("create artifact");
    connection
        .execute_batch(
            "PRAGMA page_size=512;
             PRAGMA journal_mode=WAL;
             CREATE TABLE conversations (
                 id INTEGER PRIMARY KEY,
                 source_id TEXT NOT NULL,
                 agent_id INTEGER NOT NULL,
                 external_id TEXT NOT NULL,
                 UNIQUE(source_id,agent_id,external_id)
             );
             INSERT INTO conversations(source_id,agent_id,external_id) VALUES
                 ('source-a',1,'external-a'),
                 ('source-b',2,'external-b'),
                 ('source-c',3,'external-c'),
                 ('source-d',4,'external-d');
             CREATE TABLE recyclable(id INTEGER PRIMARY KEY, payload BLOB);
             INSERT INTO recyclable(payload) VALUES(zeroblob(1800));
             DELETE FROM recyclable;
             PRAGMA wal_checkpoint(PASSIVE);",
        )
        .expect("build artifact with ownership and freelist state");

    let report = classify_sqlite_artifact(&database, Some("sqlite_autoindex_conversations_1"))
        .expect("classify healthy artifact");
    assert!(report.input_files_unchanged);
    assert_eq!(report.page_size, 512);
    assert_eq!(report.header_page_count, report.pragma_page_count);
    assert_eq!(report.header_freelist_count, report.pragma_freelist_count);
    assert_eq!(
        report.freelist_pages.len(),
        report.header_freelist_count as usize
    );
    assert!(report.orphan_pages.is_empty());
    assert!(report.multiply_owned_pages.is_empty());
    assert_eq!(report.integrity_check_lines, ["ok"]);
    assert!(report.integrity_check_error.is_none());

    let ownership = report
        .ownership
        .iter()
        .find(|owner| owner.name == "sqlite_autoindex_conversations_1")
        .expect("target autoindex ownership");
    assert!(ownership.root_page > 1);
    assert!(ownership.height >= 1);
    assert!(ownership.pages.contains(&ownership.root_page));

    let comparison = report.target_index.as_ref().expect("target comparison");
    assert!(comparison.comparable, "{:?}", comparison.reason);
    assert_eq!(comparison.table_entry_count, 4);
    assert_eq!(comparison.index_entry_count, 4);
    assert!(comparison.missing_from_index.is_empty());
    assert!(comparison.extra_in_index.is_empty());
    assert!(
        comparison
            .query_plan
            .as_deref()
            .is_some_and(|plan| plan.contains("sqlite_autoindex_conversations_1"))
    );

    let wal = report
        .sidecars
        .iter()
        .find(|sidecar| sidecar.kind == "wal")
        .expect("WAL sidecar report");
    assert!(
        wal.snapshot.exists,
        "open WAL connection must retain its sidecar"
    );
    let wal_metadata = wal.wal.as_ref().expect("parse WAL header");
    assert_eq!(wal_metadata.page_size, 512);
    assert!(wal_metadata.frame_count > 0);
    assert_eq!(wal_metadata.trailing_bytes, 0);
}

#[test]
fn t42_artifact_classifier_finds_recognizable_orphan_index_page() {
    let directory = tempfile::tempdir().expect("artifact tempdir");
    let database = directory.path().join("orphan.db");
    let index_root = {
        let connection = rusqlite::Connection::open(&database).expect("create artifact");
        connection
            .execute_batch(
                "PRAGMA page_size=512;
                 CREATE TABLE conversations (
                     id INTEGER PRIMARY KEY,
                     source_id TEXT NOT NULL,
                     agent_id INTEGER NOT NULL,
                     external_id TEXT NOT NULL,
                     UNIQUE(source_id,agent_id,external_id)
                 );
                 INSERT INTO conversations(source_id,agent_id,external_id) VALUES
                     ('source-a',1,'external-a'),
                     ('source-b',2,'external-b'),
                     ('source-c',3,'external-c'),
                     ('source-d',4,'external-d');",
            )
            .expect("build target index");
        connection
            .query_row(
                "SELECT rootpage FROM sqlite_schema \
                 WHERE name='sqlite_autoindex_conversations_1'",
                [],
                |row| row.get::<_, u32>(0),
            )
            .expect("target root page")
    };

    let clean = classify_sqlite_artifact(&database, Some("sqlite_autoindex_conversations_1"))
        .expect("classify clean source");
    assert!(clean.orphan_pages.is_empty());

    let orphan_page = clean.page_count + 1;
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&database)
        .expect("open disposable artifact for corruption fixture construction");
    let mut copied_index_page = vec![0_u8; clean.page_size as usize];
    file.seek(SeekFrom::Start(
        u64::from(index_root - 1) * u64::from(clean.page_size),
    ))
    .expect("seek target index root");
    file.read_exact(&mut copied_index_page)
        .expect("read target index root");
    file.seek(SeekFrom::End(0)).expect("seek artifact end");
    file.write_all(&copied_index_page)
        .expect("append orphan clone");
    file.seek(SeekFrom::Start(28))
        .expect("seek header page count");
    file.write_all(&orphan_page.to_be_bytes())
        .expect("publish orphan page in header count");
    file.sync_all().expect("sync corruption fixture");
    drop(file);

    let report = classify_sqlite_artifact(&database, Some("sqlite_autoindex_conversations_1"))
        .expect("classify orphan artifact");
    assert!(report.input_files_unchanged);
    assert_eq!(report.orphan_pages, [orphan_page]);
    assert!(
        report
            .integrity_check_lines
            .iter()
            .any(|line| line.contains(&format!("Page {orphan_page}: never used")))
    );
    let probe = report.orphan_probes.first().expect("orphan probe");
    assert_eq!(probe.page, orphan_page);
    assert!(
        probe
            .btree_page_type
            .as_deref()
            .is_some_and(|page_type| page_type.contains("index"))
    );
    assert!(
        !probe.matching_target_key_fragments.is_empty(),
        "copied index page should retain recognizable target-key fragments"
    );
    let comparison = report.target_index.as_ref().expect("target comparison");
    assert!(comparison.comparable);
    assert!(comparison.missing_from_index.is_empty());
    assert!(comparison.extra_in_index.is_empty());
}
