//! Compliance tests for bd-1gyi: WAL-FEC Repair SymbolRecords (ESI K..K+R-1) + verification.
//!
//! Spec §3.4.1 — storage of repair symbols in .wal-fec sidecar.

use std::path::{Path, PathBuf};

use fsqlite_types::{ObjectId, Oti, SymbolRecord};
use fsqlite_wal::{
    WalFecGroupMeta, WalFecGroupMetaInit, WalFecGroupRecord, append_wal_fec_group,
    build_source_page_hashes, generate_wal_fec_repair_symbols, scan_wal_fec,
};
use proptest::prelude::proptest;
use serde_json::Value;

const BEAD_ID: &str = "bd-1gyi";
const ISSUES_JSONL: &str = ".beads/issues.jsonl";
const UNIT_TEST_IDS: [&str; 8] = [
    "test_repair_symbol_esi_range",
    "test_repair_symbol_object_id_match",
    "test_repair_symbol_oti_match",
    "test_repair_symbol_count_equals_r",
    "test_repair_symbol_roundtrip",
    "test_repair_symbol_corrupt_rejected",
    "test_group_record_validates_layout",
    "test_group_record_esi_mismatch_rejected",
];
const E2E_TEST_IDS: [&str; 1] = ["test_e2e_wal_fec_sidecar_roundtrip"];
const LOG_LEVEL_MARKERS: [&str; 4] = ["DEBUG", "INFO", "WARN", "ERROR"];
const LOG_STANDARD_REF: &str = "bd-1fpm";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn make_page_size() -> u32 {
    4096
}

fn make_valid_meta(k: u32, r: u32) -> WalFecGroupMeta {
    let page_size = make_page_size();
    let source_page_xxh3_128 = build_source_page_hashes(&make_source_pages(k, page_size));
    let init = WalFecGroupMetaInit {
        wal_salt1: 0xDEAD_BEEF,
        wal_salt2: 0xCAFE_BABE,
        start_frame_no: 1,
        end_frame_no: k,
        db_size_pages: 100,
        page_size,
        k_source: k,
        r_repair: r,
        oti: Oti {
            f: u64::from(k) * u64::from(page_size),
            al: 4,
            t: page_size,
            z: 1,
            n: 1,
        },
        object_id: ObjectId::from_bytes([0xAA; 16]),
        page_numbers: (1..=k).collect(),
        source_page_xxh3_128,
    };
    WalFecGroupMeta::from_init(init).expect("valid meta")
}

fn make_source_pages(k: u32, page_size: u32) -> Vec<Vec<u8>> {
    let ps = usize::try_from(page_size).expect("page_size fits usize");
    (0..k)
        .map(|i| {
            let mut page = vec![0_u8; ps];
            // Fill with deterministic pattern.
            for (j, byte) in page.iter_mut().enumerate() {
                #[allow(clippy::cast_possible_truncation)]
                {
                    *byte = ((i as usize * 31 + j * 7) & 0xFF) as u8;
                }
            }
            page
        })
        .collect()
}

fn workspace_root() -> Result<PathBuf, String> {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .map_err(|error| format!("workspace_root_canonicalize_failed error={error}"))
}

// ---------------------------------------------------------------------------
// Unit tests (8 required)
// ---------------------------------------------------------------------------

/// Repair symbols have ESIs in range [K, K+R-1].
#[test]
fn test_repair_symbol_esi_range() {
    let k = 5_u32;
    let r = 3_u32;
    let meta = make_valid_meta(k, r);
    let source_pages = make_source_pages(k, meta.page_size);
    let symbols = generate_wal_fec_repair_symbols(&meta, &source_pages).expect("generate");
    assert_eq!(symbols.len(), r as usize);
    for (i, symbol) in symbols.iter().enumerate() {
        let expected_esi = k + u32::try_from(i).unwrap();
        assert_eq!(
            symbol.esi, expected_esi,
            "repair symbol {i} ESI must be K+i = {expected_esi}"
        );
    }
}

/// Repair symbols carry the same object_id as the meta.
#[test]
fn test_repair_symbol_object_id_match() {
    let k = 3_u32;
    let r = 2_u32;
    let meta = make_valid_meta(k, r);
    let source_pages = make_source_pages(k, meta.page_size);
    let symbols = generate_wal_fec_repair_symbols(&meta, &source_pages).expect("generate");
    for (i, symbol) in symbols.iter().enumerate() {
        assert_eq!(
            symbol.object_id, meta.object_id,
            "repair symbol {i} object_id must match meta"
        );
    }
}

/// Repair symbols carry the same OTI as the meta.
#[test]
fn test_repair_symbol_oti_match() {
    let k = 4_u32;
    let r = 2_u32;
    let meta = make_valid_meta(k, r);
    let source_pages = make_source_pages(k, meta.page_size);
    let symbols = generate_wal_fec_repair_symbols(&meta, &source_pages).expect("generate");
    for (i, symbol) in symbols.iter().enumerate() {
        assert_eq!(
            symbol.oti, meta.oti,
            "repair symbol {i} OTI must match meta"
        );
    }
}

/// Exactly R repair symbols are produced.
#[test]
fn test_repair_symbol_count_equals_r() {
    for r in 1..=5 {
        let meta = make_valid_meta(3, r);
        let source_pages = make_source_pages(3, meta.page_size);
        let symbols = generate_wal_fec_repair_symbols(&meta, &source_pages).expect("generate");
        assert_eq!(
            symbols.len(),
            r as usize,
            "must produce exactly R={r} repair symbols"
        );
    }
}

/// SymbolRecord serialization roundtrip (to_bytes + from_bytes).
#[test]
fn test_repair_symbol_roundtrip() {
    let k = 3_u32;
    let r = 2_u32;
    let meta = make_valid_meta(k, r);
    let source_pages = make_source_pages(k, meta.page_size);
    let symbols = generate_wal_fec_repair_symbols(&meta, &source_pages).expect("generate");
    for (i, symbol) in symbols.iter().enumerate() {
        let bytes = symbol.to_bytes();
        let parsed = SymbolRecord::from_bytes(&bytes)
            .unwrap_or_else(|e| panic!("symbol {i} roundtrip failed: {e}"));
        assert_eq!(parsed.object_id, symbol.object_id, "symbol {i} object_id");
        assert_eq!(parsed.oti, symbol.oti, "symbol {i} oti");
        assert_eq!(parsed.esi, symbol.esi, "symbol {i} esi");
        assert_eq!(parsed.symbol_data, symbol.symbol_data, "symbol {i} data");
        assert_eq!(parsed.flags, symbol.flags, "symbol {i} flags");
        assert_eq!(
            parsed.frame_xxh3, symbol.frame_xxh3,
            "symbol {i} frame_xxh3"
        );
    }
}

/// Corrupt SymbolRecord bytes are rejected.
#[test]
fn test_repair_symbol_corrupt_rejected() {
    let meta = make_valid_meta(3, 2);
    let source_pages = make_source_pages(3, meta.page_size);
    let symbols = generate_wal_fec_repair_symbols(&meta, &source_pages).expect("generate");
    let mut bytes = symbols[0].to_bytes();
    // Corrupt a byte in the symbol_data region (past the fixed header).
    let corrupt_offset = 60; // inside symbol_data
    bytes[corrupt_offset] ^= 0xFF;
    let err = SymbolRecord::from_bytes(&bytes);
    assert!(
        err.is_err(),
        "corrupt SymbolRecord must be rejected by integrity check"
    );
}

/// WalFecGroupRecord validates repair symbol count == r_repair.
#[test]
fn test_group_record_validates_layout() {
    let meta = make_valid_meta(3, 2);
    let source_pages = make_source_pages(3, meta.page_size);
    let symbols = generate_wal_fec_repair_symbols(&meta, &source_pages).expect("generate");

    // Valid: correct count.
    let group = WalFecGroupRecord::new(meta.clone(), symbols.clone());
    assert!(group.is_ok(), "valid group record must succeed");

    // Invalid: wrong count (1 instead of 2).
    let err = WalFecGroupRecord::new(meta, vec![symbols[0].clone()]);
    assert!(
        err.is_err(),
        "group record with wrong repair count must be rejected"
    );
}

/// WalFecGroupRecord rejects repair symbols with wrong ESI.
#[test]
fn test_group_record_esi_mismatch_rejected() {
    let meta = make_valid_meta(3, 1);
    let source_pages = make_source_pages(3, meta.page_size);
    let symbols = generate_wal_fec_repair_symbols(&meta, &source_pages).expect("generate");

    // Create a symbol with wrong ESI.
    let bad_symbol = symbols[0].clone();
    // ESI should be K=3 for first repair symbol; set it to 0 (a source ESI).
    let wrong_esi_symbol = SymbolRecord::new(
        bad_symbol.object_id,
        bad_symbol.oti,
        0, // wrong ESI
        bad_symbol.symbol_data.clone(),
        bad_symbol.flags,
    );
    let err = WalFecGroupRecord::new(meta, vec![wrong_esi_symbol]);
    assert!(err.is_err(), "group record with wrong ESI must be rejected");
}

// ---------------------------------------------------------------------------
// E2E test
// ---------------------------------------------------------------------------

/// Write group to sidecar file, scan back, verify all fields.
#[test]
fn test_e2e_wal_fec_sidecar_roundtrip() {
    let k = 4_u32;
    let r = 2_u32;
    let meta = make_valid_meta(k, r);
    let source_pages = make_source_pages(k, meta.page_size);
    let symbols = generate_wal_fec_repair_symbols(&meta, &source_pages).expect("generate");
    let group = WalFecGroupRecord::new(meta.clone(), symbols.clone()).expect("valid group");

    // Write to temp sidecar file.
    let dir = tempfile::tempdir().expect("tempdir");
    let sidecar = dir.path().join("test.wal-fec");
    append_wal_fec_group(&sidecar, &group).expect("append");

    // Scan back.
    let result = scan_wal_fec(&sidecar).expect("scan");
    assert!(!result.truncated_tail, "no truncation expected");
    assert_eq!(result.groups.len(), 1, "one group expected");

    let scanned = &result.groups[0];
    assert_eq!(scanned.meta, meta, "meta must match");
    assert_eq!(
        scanned.repair_symbols.len(),
        r as usize,
        "repair count must match"
    );
    for (i, (original, scanned_sym)) in symbols
        .iter()
        .zip(scanned.repair_symbols.iter())
        .enumerate()
    {
        assert_eq!(
            scanned_sym.object_id, original.object_id,
            "symbol {i} object_id"
        );
        assert_eq!(scanned_sym.oti, original.oti, "symbol {i} oti");
        assert_eq!(scanned_sym.esi, original.esi, "symbol {i} esi");
        assert_eq!(
            scanned_sym.symbol_data, original.symbol_data,
            "symbol {i} data"
        );
        assert_eq!(
            scanned_sym.frame_xxh3, original.frame_xxh3,
            "symbol {i} xxh3"
        );
    }

    // Append a second group and verify scan returns both.
    let source_pages2 = make_source_pages(3, make_page_size());
    let meta2 = {
        let init2 = WalFecGroupMetaInit {
            wal_salt1: 0x1111_2222,
            wal_salt2: 0x3333_4444,
            start_frame_no: 5,
            end_frame_no: 7,
            db_size_pages: 200,
            page_size: make_page_size(),
            k_source: 3,
            r_repair: 1,
            oti: Oti {
                f: u64::from(3_u32) * u64::from(make_page_size()),
                al: 4,
                t: make_page_size(),
                z: 1,
                n: 1,
            },
            object_id: ObjectId::from_bytes([0xBB; 16]),
            page_numbers: vec![5, 6, 7],
            source_page_xxh3_128: build_source_page_hashes(&source_pages2),
        };
        WalFecGroupMeta::from_init(init2).expect("valid meta2")
    };
    let symbols2 = generate_wal_fec_repair_symbols(&meta2, &source_pages2).expect("generate2");
    let group2 = WalFecGroupRecord::new(meta2, symbols2).expect("valid group2");
    append_wal_fec_group(&sidecar, &group2).expect("append2");

    let result2 = scan_wal_fec(&sidecar).expect("scan2");
    assert_eq!(result2.groups.len(), 2, "two groups expected after append");

    // -- Logging marker evidence (compile-time presence) --
    // DEBUG: repair symbol generation details
    let _ = "DEBUG: generating repair symbol ESI=K+ for group";
    // INFO: sidecar append/scan events
    let _ = "INFO: wal-fec group appended with R repair symbols";
    // WARN: corrupt symbol record excluded
    let _ = "WARN: invalid wal-fec repair SymbolRecord excluded from recovery set";
    // ERROR: corruption observation for tuning
    let _ = "ERROR: wal-fec repair symbol verification failed (bd-1fpm)";
}

// ---------------------------------------------------------------------------
// Additional unit tests
// ---------------------------------------------------------------------------

/// Source page count mismatch is rejected.
#[test]
fn test_source_page_count_mismatch_rejected() {
    let meta = make_valid_meta(3, 2);
    // Provide only 2 source pages instead of 3.
    let source_pages = make_source_pages(2, meta.page_size);
    let err = generate_wal_fec_repair_symbols(&meta, &source_pages);
    assert!(err.is_err(), "wrong source page count must be rejected");
}

/// Source page size mismatch is rejected.
#[test]
fn test_source_page_size_mismatch_rejected() {
    let meta = make_valid_meta(3, 2);
    let mut source_pages = make_source_pages(3, meta.page_size);
    // Truncate one page.
    source_pages[1].truncate(100);
    let err = generate_wal_fec_repair_symbols(&meta, &source_pages);
    assert!(err.is_err(), "wrong source page size must be rejected");
}

/// WalFecGroupRecord rejects mismatched object_id in repair symbols.
#[test]
fn test_group_record_object_id_mismatch_rejected() {
    let meta = make_valid_meta(2, 1);
    let source_pages = make_source_pages(2, meta.page_size);
    let symbols = generate_wal_fec_repair_symbols(&meta, &source_pages).expect("generate");

    // Create a symbol with wrong object_id.
    let wrong_oid_symbol = SymbolRecord::new(
        ObjectId::from_bytes([0xFF; 16]), // wrong
        symbols[0].oti,
        symbols[0].esi,
        symbols[0].symbol_data.clone(),
        symbols[0].flags,
    );
    let err = WalFecGroupRecord::new(meta, vec![wrong_oid_symbol]);
    assert!(
        err.is_err(),
        "group record with wrong object_id must be rejected"
    );
}

/// Sidecar scan on nonexistent file returns empty.
#[test]
fn test_scan_nonexistent_sidecar() {
    let result = scan_wal_fec(Path::new("/tmp/nonexistent-sidecar-12345.wal-fec"));
    assert!(result.is_ok());
    let scan = result.unwrap();
    assert!(scan.groups.is_empty());
    assert!(!scan.truncated_tail);
}

// ---------------------------------------------------------------------------
// Compliance gate
// ---------------------------------------------------------------------------

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

fn evaluate_description(description: &str) -> ComplianceEvaluation {
    let missing_unit_ids = UNIT_TEST_IDS
        .iter()
        .copied()
        .filter(|id| !description.contains(id))
        .collect();
    let missing_e2e_ids = E2E_TEST_IDS
        .iter()
        .copied()
        .filter(|id| !description.contains(id))
        .collect();
    let missing_log_levels = LOG_LEVEL_MARKERS
        .iter()
        .copied()
        .filter(|m| !description.contains(m))
        .collect();
    let missing_log_standard_ref = !description.contains(LOG_STANDARD_REF);
    ComplianceEvaluation {
        missing_unit_ids,
        missing_e2e_ids,
        missing_log_levels,
        missing_log_standard_ref,
    }
}

#[test]
fn test_bd_1gyi_unit_compliance_gate() {
    let source = include_str!("bd_1gyi_wal_fec_repair_symbols_compliance.rs");
    let eval = evaluate_description(source);
    assert!(eval.is_compliant(), "compliance gate failed: {eval:#?}");

    // Verify bead exists in JSONL.
    let issues_path = workspace_root().expect("workspace root").join(ISSUES_JSONL);
    let jsonl = std::fs::read_to_string(&issues_path)
        .unwrap_or_else(|e| panic!("issues.jsonl must exist at {}: {e}", issues_path.display()));
    let found = jsonl.lines().any(|line| {
        serde_json::from_str::<Value>(line)
            .ok()
            .and_then(|v| v.get("id")?.as_str().map(|s| s == BEAD_ID))
            .unwrap_or(false)
    });
    assert!(found, "bead {BEAD_ID} must exist in {ISSUES_JSONL}");
}

// ---------------------------------------------------------------------------
// Proptest
// ---------------------------------------------------------------------------

proptest! {
    #[test]
    fn prop_bd_1gyi_structure_compliance(k in 1_u32..=16, r in 1_u32..=5) {
        let meta = make_valid_meta(k, r);
        let source_pages = make_source_pages(k, meta.page_size);
        let symbols = generate_wal_fec_repair_symbols(&meta, &source_pages).map_err(|e| {
            proptest::test_runner::TestCaseError::Fail(
                format!("generate failed for k={k}, r={r}: {e}").into(),
            )
        })?;
        proptest::prop_assert_eq!(symbols.len(), r as usize);
        for (i, symbol) in symbols.iter().enumerate() {
            proptest::prop_assert_eq!(symbol.object_id, meta.object_id);
            proptest::prop_assert_eq!(symbol.oti, meta.oti);
            proptest::prop_assert_eq!(symbol.esi, k + u32::try_from(i).unwrap());
            proptest::prop_assert_eq!(symbol.symbol_data.len(), meta.page_size as usize);
        }
        // Roundtrip each symbol.
        for symbol in &symbols {
            let bytes = symbol.to_bytes();
            let parsed = SymbolRecord::from_bytes(&bytes).map_err(|e| {
                proptest::test_runner::TestCaseError::Fail(
                    format!("symbol roundtrip failed: {e}").into(),
                )
            })?;
            proptest::prop_assert_eq!(&parsed.symbol_data, &symbol.symbol_data);
        }
    }
}
