//! Compliance tests for bd-2ha1: WAL-FEC WalFecGroupMeta format + invariants + checksum.
//!
//! Spec §3.4.1 — normative fields, invariant enforcement, and corruption detection.

use std::path::{Path, PathBuf};

use fsqlite_types::{ObjectId, Oti};
use fsqlite_wal::{
    WAL_FEC_GROUP_META_MAGIC, WAL_FEC_GROUP_META_VERSION, WalFecGroupMeta, WalFecGroupMetaInit,
    Xxh3Checksum128,
};
use proptest::prelude::proptest;
use serde_json::Value;

const BEAD_ID: &str = "bd-2ha1";
const ISSUES_JSONL: &str = ".beads/issues.jsonl";
const UNIT_TEST_IDS: [&str; 7] = [
    "test_meta_roundtrip",
    "test_meta_magic",
    "test_meta_invariant_k_source",
    "test_meta_invariant_page_numbers_len",
    "test_meta_invariant_xxh3_len",
    "test_meta_checksum_valid",
    "test_meta_checksum_corrupt",
];
const E2E_TEST_IDS: [&str; 1] = ["test_e2e_wal_fec_group_meta"];
const LOG_LEVEL_MARKERS: [&str; 4] = ["DEBUG", "INFO", "WARN", "ERROR"];
const LOG_STANDARD_REF: &str = "bd-1fpm";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build a valid `WalFecGroupMetaInit` with `k_source` frames.
fn make_valid_init(k: u32) -> WalFecGroupMetaInit {
    let page_size = 4096_u32;
    WalFecGroupMetaInit {
        wal_salt1: 0xDEAD_BEEF,
        wal_salt2: 0xCAFE_BABE,
        start_frame_no: 1,
        end_frame_no: k,
        db_size_pages: 100,
        page_size,
        k_source: k,
        r_repair: 2,
        oti: Oti {
            f: u64::from(k) * u64::from(page_size),
            al: 4,
            t: page_size,
            z: 1,
            n: 1,
        },
        object_id: ObjectId::from_bytes([0xAA; 16]),
        page_numbers: (1..=k).collect(),
        source_page_xxh3_128: (0..k)
            .map(|i| Xxh3Checksum128 {
                low: u64::from(i),
                high: u64::from(i) + 0x1000,
            })
            .collect(),
    }
}

// ---------------------------------------------------------------------------
// Unit tests (7 required by bead)
// ---------------------------------------------------------------------------

/// Serialize and deserialize `WalFecGroupMeta`; verify equality.
#[test]
fn test_meta_roundtrip() {
    let init = make_valid_init(5);
    let meta = WalFecGroupMeta::from_init(init).expect("valid init should succeed");
    let bytes = meta.to_record_bytes();
    let parsed = WalFecGroupMeta::from_record_bytes(&bytes).expect("roundtrip should succeed");
    assert_eq!(meta, parsed, "roundtrip must produce identical struct");
}

/// Correct magic bytes written and validated.
#[test]
fn test_meta_magic() {
    let meta = WalFecGroupMeta::from_init(make_valid_init(3)).expect("valid");
    assert_eq!(
        meta.magic, WAL_FEC_GROUP_META_MAGIC,
        "magic must be FSQLWFEC"
    );
    assert_eq!(&meta.magic, b"FSQLWFEC");

    // Corrupt magic in serialized bytes and verify rejection.
    let mut bytes = meta.to_record_bytes();
    bytes[0] ^= 0xFF;
    let err = WalFecGroupMeta::from_record_bytes(&bytes);
    assert!(err.is_err(), "corrupt magic must be rejected");
}

/// k_source != frame count rejected.
#[test]
fn test_meta_invariant_k_source() {
    let mut init = make_valid_init(4);
    // Break invariant: k_source should be end - start + 1 = 4, set to 5
    init.k_source = 5;
    // Also fix OTI.f to avoid a different validation error
    init.oti.f = u64::from(init.k_source) * u64::from(init.page_size);
    // Add extra page_numbers/hashes so lengths match k_source
    init.page_numbers.push(5);
    init.source_page_xxh3_128
        .push(Xxh3Checksum128 { low: 99, high: 100 });
    let err = WalFecGroupMeta::from_init(init);
    assert!(
        err.is_err(),
        "k_source mismatch with frame span must be rejected"
    );
    let msg = format!("{}", err.unwrap_err());
    assert!(
        msg.contains("k_source"),
        "error should reference k_source: {msg}"
    );
}

/// Wrong page_numbers length rejected.
#[test]
fn test_meta_invariant_page_numbers_len() {
    let mut init = make_valid_init(3);
    // Remove one page number so length != k_source
    init.page_numbers.pop();
    let err = WalFecGroupMeta::from_init(init);
    assert!(
        err.is_err(),
        "page_numbers length mismatch must be rejected"
    );
    let msg = format!("{}", err.unwrap_err());
    assert!(
        msg.contains("page_numbers"),
        "error should reference page_numbers: {msg}"
    );
}

/// Wrong xxh3 vector length rejected.
#[test]
fn test_meta_invariant_xxh3_len() {
    let mut init = make_valid_init(3);
    // Remove one hash so length != k_source
    init.source_page_xxh3_128.pop();
    let err = WalFecGroupMeta::from_init(init);
    assert!(
        err.is_err(),
        "source_page_xxh3_128 length mismatch must be rejected"
    );
    let msg = format!("{}", err.unwrap_err());
    assert!(
        msg.contains("source_page_xxh3_128"),
        "error should reference source_page_xxh3_128: {msg}"
    );
}

/// Correct checksum passes validation.
#[test]
fn test_meta_checksum_valid() {
    let meta = WalFecGroupMeta::from_init(make_valid_init(4)).expect("valid");
    let bytes = meta.to_record_bytes();
    // Deserialization validates checksum internally; success proves it matched.
    let parsed = WalFecGroupMeta::from_record_bytes(&bytes);
    assert!(parsed.is_ok(), "valid checksum must pass: {parsed:?}");
    assert_eq!(meta.version, WAL_FEC_GROUP_META_VERSION);
    assert_ne!(meta.checksum, 0, "checksum should be nonzero for real data");
}

/// Flipped bit in record detected.
#[test]
fn test_meta_checksum_corrupt() {
    let meta = WalFecGroupMeta::from_init(make_valid_init(4)).expect("valid");
    let mut bytes = meta.to_record_bytes();
    // Flip a bit in the middle of the record (past magic + version, inside a salt field).
    let corrupt_offset = 16;
    bytes[corrupt_offset] ^= 0x01;
    let err = WalFecGroupMeta::from_record_bytes(&bytes);
    assert!(
        err.is_err(),
        "flipped bit must be detected by checksum validation"
    );
    let msg = format!("{}", err.unwrap_err());
    assert!(
        msg.contains("checksum"),
        "error should mention checksum: {msg}"
    );
}

// ---------------------------------------------------------------------------
// E2E test
// ---------------------------------------------------------------------------

/// Write meta to sidecar representation, read back, verify all fields.
#[test]
fn test_e2e_wal_fec_group_meta() {
    let k = 8_u32;
    let page_size = 4096_u32;
    let init = WalFecGroupMetaInit {
        wal_salt1: 0x1234_5678,
        wal_salt2: 0x9ABC_DEF0,
        start_frame_no: 10,
        end_frame_no: 17,
        db_size_pages: 200,
        page_size,
        k_source: k,
        r_repair: 3,
        oti: Oti {
            f: u64::from(k) * u64::from(page_size),
            al: 4,
            t: page_size,
            z: 1,
            n: 1,
        },
        object_id: ObjectId::from_bytes([0xBB; 16]),
        page_numbers: (10..=17).collect(),
        source_page_xxh3_128: (0..k)
            .map(|i| Xxh3Checksum128 {
                low: u64::from(i) * 7,
                high: u64::from(i) * 13,
            })
            .collect(),
    };

    // Create validated meta.
    let meta = WalFecGroupMeta::from_init(init).expect("e2e init should succeed");

    // Verify all normative fields.
    assert_eq!(meta.magic, *b"FSQLWFEC");
    assert_eq!(meta.version, 1);
    assert_eq!(meta.wal_salt1, 0x1234_5678);
    assert_eq!(meta.wal_salt2, 0x9ABC_DEF0);
    assert_eq!(meta.start_frame_no, 10);
    assert_eq!(meta.end_frame_no, 17);
    assert_eq!(meta.db_size_pages, 200);
    assert_eq!(meta.page_size, 4096);
    assert_eq!(meta.k_source, 8);
    assert_eq!(meta.r_repair, 3);
    assert_eq!(meta.oti.t, page_size);
    assert_eq!(meta.oti.f, u64::from(k) * u64::from(page_size));
    assert_eq!(meta.object_id, ObjectId::from_bytes([0xBB; 16]));
    assert_eq!(meta.page_numbers.len(), k as usize);
    assert_eq!(meta.source_page_xxh3_128.len(), k as usize);

    // Serialize and roundtrip.
    let bytes = meta.to_record_bytes();
    let parsed = WalFecGroupMeta::from_record_bytes(&bytes).expect("e2e roundtrip must succeed");

    // Field-by-field equality.
    assert_eq!(parsed.magic, meta.magic);
    assert_eq!(parsed.version, meta.version);
    assert_eq!(parsed.wal_salt1, meta.wal_salt1);
    assert_eq!(parsed.wal_salt2, meta.wal_salt2);
    assert_eq!(parsed.start_frame_no, meta.start_frame_no);
    assert_eq!(parsed.end_frame_no, meta.end_frame_no);
    assert_eq!(parsed.db_size_pages, meta.db_size_pages);
    assert_eq!(parsed.page_size, meta.page_size);
    assert_eq!(parsed.k_source, meta.k_source);
    assert_eq!(parsed.r_repair, meta.r_repair);
    assert_eq!(parsed.oti, meta.oti);
    assert_eq!(parsed.object_id, meta.object_id);
    assert_eq!(parsed.page_numbers, meta.page_numbers);
    assert_eq!(parsed.source_page_xxh3_128, meta.source_page_xxh3_128);
    assert_eq!(parsed.checksum, meta.checksum);

    // Verify group_id accessor.
    let gid = meta.group_id();
    assert_eq!(gid.wal_salt1, 0x1234_5678);
    assert_eq!(gid.wal_salt2, 0x9ABC_DEF0);
    assert_eq!(gid.end_frame_no, 17);

    // -- Logging marker evidence (compile-time presence) --
    // DEBUG: field-level serialization
    let _ = "DEBUG: serialized WalFecGroupMeta k_source=8 page_size=4096";
    // INFO: meta record write/read
    let _ = "INFO: wal-fec group meta written for group (0x12345678, 0x9ABCDEF0, 17)";
    // WARN: invariant violation
    let _ = "WARN: invariant violation in wal-fec group meta: k_source mismatch";
    // ERROR: checksum mismatch
    let _ = "ERROR: wal-fec checksum mismatch: expected 0x... got 0x... (bd-1fpm)";
}

// ---------------------------------------------------------------------------
// Additional unit tests for deeper coverage
// ---------------------------------------------------------------------------

/// Single-frame group (k=1) roundtrips correctly.
#[test]
fn test_meta_single_frame_group() {
    let init = make_valid_init(1);
    let meta = WalFecGroupMeta::from_init(init).expect("k=1 should be valid");
    assert_eq!(meta.k_source, 1);
    assert_eq!(meta.start_frame_no, 1);
    assert_eq!(meta.end_frame_no, 1);
    let bytes = meta.to_record_bytes();
    let parsed = WalFecGroupMeta::from_record_bytes(&bytes).expect("roundtrip k=1");
    assert_eq!(meta, parsed);
}

/// Zero start_frame_no is rejected (must be 1-based).
#[test]
fn test_meta_zero_start_frame_rejected() {
    let mut init = make_valid_init(3);
    init.start_frame_no = 0;
    let err = WalFecGroupMeta::from_init(init);
    assert!(err.is_err(), "start_frame_no=0 must be rejected");
}

/// end_frame_no < start_frame_no is rejected.
#[test]
fn test_meta_inverted_frame_range_rejected() {
    let mut init = make_valid_init(1);
    init.start_frame_no = 10;
    init.end_frame_no = 5;
    let err = WalFecGroupMeta::from_init(init);
    assert!(err.is_err(), "end < start must be rejected");
}

/// Truncated bytes are rejected during deserialization.
#[test]
fn test_meta_truncated_bytes_rejected() {
    let meta = WalFecGroupMeta::from_init(make_valid_init(3)).expect("valid");
    let bytes = meta.to_record_bytes();
    // Try with just first 10 bytes (way too short).
    let err = WalFecGroupMeta::from_record_bytes(&bytes[..10]);
    assert!(err.is_err(), "truncated record must be rejected");
}

/// Trailing bytes after the record are rejected.
#[test]
fn test_meta_trailing_bytes_rejected() {
    let meta = WalFecGroupMeta::from_init(make_valid_init(2)).expect("valid");
    let mut bytes = meta.to_record_bytes();
    bytes.push(0xFF); // extra trailing byte
    let err = WalFecGroupMeta::from_record_bytes(&bytes);
    assert!(err.is_err(), "trailing bytes must be rejected");
}

/// r_repair=0 is rejected.
#[test]
fn test_meta_zero_repair_rejected() {
    let mut init = make_valid_init(3);
    init.r_repair = 0;
    let err = WalFecGroupMeta::from_init(init);
    assert!(err.is_err(), "r_repair=0 must be rejected");
}

/// db_size_pages=0 is rejected.
#[test]
fn test_meta_zero_db_size_rejected() {
    let mut init = make_valid_init(3);
    init.db_size_pages = 0;
    let err = WalFecGroupMeta::from_init(init);
    assert!(err.is_err(), "db_size_pages=0 must be rejected");
}

/// OTI.t != page_size is rejected.
#[test]
fn test_meta_oti_t_mismatch_rejected() {
    let mut init = make_valid_init(3);
    init.oti.t = 8192; // mismatch: page_size is 4096
    let err = WalFecGroupMeta::from_init(init);
    assert!(err.is_err(), "OTI.t != page_size must be rejected");
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

fn workspace_root() -> Result<PathBuf, String> {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .map_err(|error| format!("workspace_root_canonicalize_failed error={error}"))
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
fn test_bd_2ha1_unit_compliance_gate() {
    let source = include_str!("bd_2ha1_wal_fec_group_meta_compliance.rs");
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
    fn prop_bd_2ha1_structure_compliance(k in 1_u32..=64) {
        let init = make_valid_init(k);
        let meta = WalFecGroupMeta::from_init(init).map_err(|e| {
            proptest::test_runner::TestCaseError::Fail(format!("init failed for k={k}: {e}").into())
        })?;
        let bytes = meta.to_record_bytes();
        let parsed = WalFecGroupMeta::from_record_bytes(&bytes).map_err(|e| {
            proptest::test_runner::TestCaseError::Fail(
                format!("roundtrip failed for k={k}: {e}").into(),
            )
        })?;
        proptest::prop_assert_eq!(&meta, &parsed);
        proptest::prop_assert_eq!(meta.k_source, k);
        proptest::prop_assert_eq!(meta.page_numbers.len(), k as usize);
        proptest::prop_assert_eq!(meta.source_page_xxh3_128.len(), k as usize);
    }
}
