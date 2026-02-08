//! RaptorQ permeation-map audit gate tests (§0.4, §3.5.7).
//!
//! These tests enforce a living checklist that maps each durable/sync
//! subsystem to:
//! - bytes format
//! - ECS object type
//! - decode/repair mechanism
//! - replication path
//!
//! Bead: bd-1wx.2

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use fsqlite_types::ecs::{
    ManifestSegment, ObjectLocatorSegment, PageVersionIndexSegment, PatchKind, SymbolRecord,
    SymbolRecordFlags, VersionPointer as EcsVersionPointer,
};
use fsqlite_types::{
    CommitCapsule, CommitMarker, CommitProof, CommitSeq, DependencyEdge, ObjectId, Oti, PageNumber,
    RootManifest, SchemaEpoch, TxnId, WitnessKey,
};

const BEAD_ID: &str = "bd-1wx.2";
const CHECKLIST_REL_PATH: &str = "docs/raptorq_permeation_map_checklist.md";

#[derive(Debug, Clone, PartialEq, Eq)]
struct ChecklistEntry {
    subsystem_key: String,
    bytes_format: String,
    ecs_object_type: String,
    repair_mechanism: String,
    replication_path: String,
    status: String,
    evidence: String,
}

impl ChecklistEntry {
    fn is_exempt(&self) -> bool {
        self.ecs_object_type.eq_ignore_ascii_case("EXEMPT")
    }

    fn has_decode_repair(&self) -> bool {
        self.repair_mechanism
            .to_ascii_lowercase()
            .contains("decode")
    }
}

fn workspace_root() -> &'static Path {
    // CARGO_MANIFEST_DIR = .../crates/fsqlite-harness
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    manifest_dir
        .parent()
        .and_then(Path::parent)
        .expect("workspace root should be two levels up from fsqlite-harness")
}

fn checklist_path() -> PathBuf {
    workspace_root().join(CHECKLIST_REL_PATH)
}

fn checklist_text() -> String {
    let path = checklist_path();
    let content = fs::read_to_string(&path);
    assert!(
        content.is_ok(),
        "bead_id={BEAD_ID} case=missing_checklist path={}",
        path.display()
    );
    content.expect("checklist read should succeed after is_ok assertion")
}

fn parse_checklist_entries(text: &str) -> Vec<ChecklistEntry> {
    let mut out = Vec::new();

    for raw_line in text.lines() {
        let line = raw_line.trim();
        if !line.starts_with('|') || !line.ends_with('|') {
            continue;
        }

        let columns: Vec<&str> = line.split('|').map(str::trim).collect();
        if columns.len() < 9 {
            continue;
        }

        let cells = &columns[1..columns.len() - 1];
        if cells[0].eq_ignore_ascii_case("subsystem_key") {
            continue;
        }

        let is_separator = cells
            .iter()
            .all(|cell| cell.chars().all(|ch| ch == '-' || ch == ':'));
        if is_separator {
            continue;
        }

        out.push(ChecklistEntry {
            subsystem_key: cells[0].to_string(),
            bytes_format: cells[1].to_string(),
            ecs_object_type: cells[2].to_string(),
            repair_mechanism: cells[3].to_string(),
            replication_path: cells[4].to_string(),
            status: cells[5].to_string(),
            evidence: cells[6].to_string(),
        });
    }

    out
}

fn entries_by_key() -> BTreeMap<String, ChecklistEntry> {
    let text = checklist_text();
    let entries = parse_checklist_entries(&text);
    assert!(
        !entries.is_empty(),
        "bead_id={BEAD_ID} case=empty_checklist path={CHECKLIST_REL_PATH}"
    );

    let mut by_key = BTreeMap::new();
    for entry in entries {
        let duplicate = by_key.insert(entry.subsystem_key.clone(), entry);
        assert!(
            duplicate.is_none(),
            "bead_id={BEAD_ID} case=duplicate_subsystem_key key={}",
            duplicate
                .as_ref()
                .map_or("<unknown>", |old| old.subsystem_key.as_str())
        );
    }
    by_key
}

fn entry<'a>(entries: &'a BTreeMap<String, ChecklistEntry>, key: &str) -> &'a ChecklistEntry {
    assert!(
        entries.contains_key(key),
        "bead_id={BEAD_ID} case=missing_subsystem_entry key={key}"
    );
    entries
        .get(key)
        .expect("entry must exist after contains_key assertion")
}

fn required_subsystems() -> BTreeSet<&'static str> {
    BTreeSet::from([
        "commit_capsules",
        "commit_markers",
        "commit_proofs",
        "root_manifest",
        "checkpoints",
        "index_segments_page_versions",
        "index_segments_object_locator",
        "index_segments_manifest",
        "patch_chains_history",
        "replication_symbol_transport",
        "repair_decode_pipeline",
        "compat_mode_wal_exemption",
    ])
}

fn tracked_durable_types() -> BTreeSet<&'static str> {
    BTreeSet::from([
        "CommitCapsule",
        "CommitMarker",
        "CommitProof",
        "RootManifest",
        "SymbolRecord",
        "PageHistory",
        "VersionPointer",
        "PageVersionIndexSegment",
        "ObjectLocatorSegment",
        "ManifestSegment",
    ])
}

fn sample_oti(symbol_size: u32) -> Oti {
    Oti {
        f: u64::from(symbol_size),
        al: 1,
        t: symbol_size,
        z: 1,
        n: 1,
    }
}

#[test]
fn test_commit_capsules_are_ecs_objects() {
    let capsule = CommitCapsule {
        object_id: ObjectId::from_bytes([0x11; ObjectId::LEN]),
        snapshot_basis: CommitSeq::new(0),
        intent_log: Vec::new(),
        page_deltas: Vec::new(),
        read_set_digest: [0u8; 32],
        write_set_digest: [0u8; 32],
        read_witness_refs: Vec::new(),
        write_witness_refs: Vec::new(),
        dependency_edge_refs: Vec::new(),
        merge_witness_refs: Vec::new(),
    };
    assert_eq!(
        capsule.object_id.as_bytes().len(),
        ObjectId::LEN,
        "bead_id={BEAD_ID} case=commit_capsule_object_id_len"
    );

    let entries = entries_by_key();
    let row = entry(&entries, "commit_capsules");
    assert!(
        row.ecs_object_type.contains("CommitCapsule"),
        "bead_id={BEAD_ID} case=commit_capsule_row_missing_type row={row:?}"
    );
    assert!(
        row.has_decode_repair(),
        "bead_id={BEAD_ID} case=commit_capsule_row_missing_decode row={row:?}"
    );
}

#[test]
fn test_commit_markers_are_ecs_objects() {
    let marker = CommitMarker {
        commit_seq: CommitSeq::new(7),
        commit_time_unix_ns: 1_730_000_000_000_000_000,
        capsule_object_id: ObjectId::from_bytes([0x22; ObjectId::LEN]),
        proof_object_id: ObjectId::from_bytes([0x23; ObjectId::LEN]),
        prev_marker: Some(ObjectId::from_bytes([0x24; ObjectId::LEN])),
        integrity_hash: [0x42; 16],
    };
    assert_eq!(
        marker.commit_seq.get(),
        7,
        "bead_id={BEAD_ID} case=commit_marker_commit_seq"
    );

    let entries = entries_by_key();
    let row = entry(&entries, "commit_markers");
    assert!(
        row.ecs_object_type.contains("CommitMarker"),
        "bead_id={BEAD_ID} case=commit_marker_row_missing_type row={row:?}"
    );
    assert!(
        row.has_decode_repair(),
        "bead_id={BEAD_ID} case=commit_marker_row_missing_decode row={row:?}"
    );
}

#[test]
fn test_commit_proofs_are_ecs_objects() {
    let proof = CommitProof {
        commit_seq: CommitSeq::new(1),
        edges: vec![DependencyEdge {
            from: TxnId::new(1).expect("valid txn id"),
            to: TxnId::new(2).expect("valid txn id"),
            key_basis: WitnessKey::Page(PageNumber::ONE),
            observed_by: TxnId::new(3).expect("valid txn id"),
        }],
        evidence_refs: Vec::new(),
    };
    assert_eq!(
        proof.edges.len(),
        1,
        "bead_id={BEAD_ID} case=commit_proof_edges_len"
    );

    let entries = entries_by_key();
    let row = entry(&entries, "commit_proofs");
    assert!(
        row.ecs_object_type.contains("CommitProof"),
        "bead_id={BEAD_ID} case=commit_proof_row_missing_type row={row:?}"
    );
    assert!(
        row.has_decode_repair(),
        "bead_id={BEAD_ID} case=commit_proof_row_missing_decode row={row:?}"
    );
}

#[test]
fn test_root_manifest_is_ecs() {
    let root_page = PageNumber::new(1).expect("page 1 must be valid");
    let manifest = RootManifest {
        schema_epoch: SchemaEpoch::new(3),
        root_page,
    };
    assert_eq!(
        manifest.root_page.get(),
        1,
        "bead_id={BEAD_ID} case=root_manifest_root_page"
    );

    let entries = entries_by_key();
    let row = entry(&entries, "root_manifest");
    assert!(
        row.ecs_object_type.contains("RootManifest"),
        "bead_id={BEAD_ID} case=root_manifest_row_missing_type row={row:?}"
    );
}

#[test]
fn test_checkpoints_are_ecs_objects() {
    let entries = entries_by_key();
    let row = entry(&entries, "checkpoints");
    assert!(
        !row.is_exempt(),
        "bead_id={BEAD_ID} case=checkpoint_row_must_not_be_exempt row={row:?}"
    );
    assert!(
        row.has_decode_repair(),
        "bead_id={BEAD_ID} case=checkpoint_row_missing_decode row={row:?}"
    );
    assert!(
        row.replication_path.to_ascii_lowercase().contains("symbol"),
        "bead_id={BEAD_ID} case=checkpoint_row_missing_symbol_replication row={row:?}"
    );
}

#[test]
fn test_index_segments_are_ecs_objects() {
    let object_id = ObjectId::from_bytes([0x33; ObjectId::LEN]);
    let page = PageNumber::new(7).expect("page 7 must be valid");

    let version_pointer = EcsVersionPointer {
        commit_seq: 10,
        patch_object: object_id,
        patch_kind: PatchKind::FullImage,
        base_hint: None,
    };
    let page_segment = PageVersionIndexSegment::new(1, 10, vec![(page, version_pointer)]);
    assert_eq!(
        page_segment.lookup(page, 10).map(|vp| vp.commit_seq),
        Some(10),
        "bead_id={BEAD_ID} case=page_version_index_lookup"
    );

    let locator_segment = ObjectLocatorSegment::new(vec![(object_id, Vec::new())]);
    assert!(
        locator_segment.lookup(&object_id).is_some(),
        "bead_id={BEAD_ID} case=object_locator_lookup"
    );

    let manifest_segment = ManifestSegment::new(vec![(1, 20, object_id)]);
    assert_eq!(
        manifest_segment.lookup(5),
        Some(&object_id),
        "bead_id={BEAD_ID} case=manifest_segment_lookup"
    );

    let entries = entries_by_key();
    for key in [
        "index_segments_page_versions",
        "index_segments_object_locator",
        "index_segments_manifest",
    ] {
        let row = entry(&entries, key);
        assert!(
            !row.is_exempt(),
            "bead_id={BEAD_ID} case=index_segment_row_must_not_be_exempt key={key} row={row:?}"
        );
        assert!(
            row.has_decode_repair(),
            "bead_id={BEAD_ID} case=index_segment_row_missing_decode key={key} row={row:?}"
        );
    }
}

#[test]
fn test_patch_chains_are_ecs_objects() {
    let entries = entries_by_key();
    let row = entry(&entries, "patch_chains_history");
    assert!(
        row.ecs_object_type.contains("PageHistory")
            && row.ecs_object_type.contains("VersionPointer"),
        "bead_id={BEAD_ID} case=patch_chain_row_missing_types row={row:?}"
    );
    assert!(
        row.has_decode_repair(),
        "bead_id={BEAD_ID} case=patch_chain_row_missing_decode row={row:?}"
    );
}

#[test]
fn test_replication_uses_symbols_not_files() {
    let oti = sample_oti(64);
    let record = SymbolRecord::new(
        ObjectId::from_bytes([0x55; ObjectId::LEN]),
        oti,
        0,
        vec![0xAB; 64],
        SymbolRecordFlags::SYSTEMATIC_RUN_START,
    );
    assert_eq!(
        record.symbol_data.len(),
        64,
        "bead_id={BEAD_ID} case=symbol_record_payload_size"
    );

    let entries = entries_by_key();
    let row = entry(&entries, "replication_symbol_transport");
    let replication = row.replication_path.to_ascii_lowercase();
    assert!(
        replication.contains("symbol"),
        "bead_id={BEAD_ID} case=replication_row_missing_symbol_transport row={row:?}"
    );
    assert!(
        !replication.contains("raw file copy"),
        "bead_id={BEAD_ID} case=replication_row_forbids_raw_file_copy row={row:?}"
    );
}

#[test]
fn test_repair_uses_decode_not_panic() {
    let oti = sample_oti(32);
    let record = SymbolRecord::new(
        ObjectId::from_bytes([0x66; ObjectId::LEN]),
        oti,
        5,
        vec![0xCD; 32],
        SymbolRecordFlags::empty(),
    );
    let mut wire = record.to_bytes();
    wire[20] ^= 0x01;

    let decode_attempt = std::panic::catch_unwind(|| SymbolRecord::from_bytes(&wire));
    assert!(
        decode_attempt.is_ok(),
        "bead_id={BEAD_ID} case=repair_path_must_return_error_not_panic"
    );
    let decode_result = decode_attempt.expect("catch_unwind should succeed");
    assert!(
        decode_result.is_err(),
        "bead_id={BEAD_ID} case=tampered_symbol_record_must_fail_decode"
    );

    let entries = entries_by_key();
    for (key, row) in &entries {
        if row.is_exempt() {
            continue;
        }
        let repair = row.repair_mechanism.to_ascii_lowercase();
        assert!(
            repair.contains("decode"),
            "bead_id={BEAD_ID} case=row_missing_decode key={key} row={row:?}"
        );
        if repair.contains("panic") {
            assert!(
                repair.contains("never panic"),
                "bead_id={BEAD_ID} case=row_mentions_panic_without_negation key={key} row={row:?}"
            );
        }
    }
}

#[test]
fn test_permeation_map_completeness() {
    let entries = entries_by_key();
    let keys: BTreeSet<&str> = entries.keys().map(String::as_str).collect();

    for required in required_subsystems() {
        assert!(
            keys.contains(required),
            "bead_id={BEAD_ID} case=missing_required_subsystem key={required}"
        );
    }
}

#[test]
fn test_every_ecs_object_specifies_repair() {
    let entries = entries_by_key();
    for (key, row) in &entries {
        assert!(
            !row.bytes_format.is_empty(),
            "bead_id={BEAD_ID} case=empty_bytes_format key={key}"
        );
        assert!(
            !row.status.is_empty(),
            "bead_id={BEAD_ID} case=empty_status key={key}"
        );
        assert!(
            !row.evidence.is_empty(),
            "bead_id={BEAD_ID} case=empty_evidence key={key}"
        );

        if row.is_exempt() {
            continue;
        }

        assert!(
            !row.ecs_object_type.is_empty(),
            "bead_id={BEAD_ID} case=empty_ecs_object_type key={key}"
        );
        assert!(
            !row.repair_mechanism.is_empty(),
            "bead_id={BEAD_ID} case=empty_repair_mechanism key={key}"
        );
        assert!(
            !row.replication_path.is_empty(),
            "bead_id={BEAD_ID} case=empty_replication_path key={key}"
        );
    }
}

#[test]
fn test_new_durable_structure_requires_ecs() {
    let entries = entries_by_key();
    let mapped_types: String = entries
        .values()
        .map(|row| row.ecs_object_type.as_str())
        .collect::<Vec<&str>>()
        .join(" ");

    for durable_type in tracked_durable_types() {
        assert!(
            mapped_types.contains(durable_type),
            "bead_id={BEAD_ID} case=durable_type_not_mapped durable_type={durable_type}"
        );
    }
}

#[test]
fn test_e2e_raptorq_permeation_audit_report() {
    let entries = entries_by_key();
    let mut lines = Vec::new();
    for (key, row) in &entries {
        lines.push(format!(
            "{key}\t{}\t{}\t{}\t{}\t{}\t{}",
            row.bytes_format,
            row.ecs_object_type,
            row.repair_mechanism,
            row.replication_path,
            row.status,
            row.evidence
        ));
    }
    let report = lines.join("\n");

    for required in required_subsystems() {
        assert!(
            report.contains(required),
            "bead_id={BEAD_ID} case=e2e_report_missing_required_subsystem key={required}"
        );
    }

    for (key, row) in &entries {
        let raw_bytes = row.bytes_format.to_ascii_lowercase().contains("raw");
        if raw_bytes && key != "compat_mode_wal_exemption" {
            assert!(
                !row.is_exempt(),
                "bead_id={BEAD_ID} case=unexpected_raw_bytes_exemption key={key}"
            );
        }
    }
}
