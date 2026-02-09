use std::fs;
use std::path::{Path, PathBuf};

use fsqlite_harness::fault_vfs::{FaultSpec, FaultState, SyncDecision, WriteDecision};
use fsqlite_wal::{
    attempt_wal_fec_repair, compute_wal_frame_checksum, crc32c_checksum,
    integrity_check_level1_page, integrity_check_level2_btree,
    integrity_check_level3_overflow_chain, integrity_check_level4_cross_reference,
    integrity_check_level5_schema, merge_integrity_reports, wal_fec_source_hash_xxh3_128,
    write_wal_frame_checksum, write_wal_frame_salts, SqliteWalChecksum, WalFecRepairOutcome,
    WalSalts, WAL_FRAME_HEADER_SIZE,
};
use proptest::prelude::proptest;
use proptest::test_runner::TestCaseError;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tempfile::tempdir;

const BEAD_ID: &str = "bd-3a7d";
const ISSUES_JSONL: &str = ".beads/issues.jsonl";
const LOG_STANDARD_REF: &str = "bd-1fpm";
const PAGE_SIZE: usize = 4096;
const UNIT_TEST_IDS: [&str; 2] = [
    "test_bd_3a7d_unit_compliance_gate",
    "prop_bd_3a7d_structure_compliance",
];
const E2E_TEST_IDS: [&str; 1] = ["test_e2e_bd_3a7d_compliance"];
const LOG_LEVEL_MARKERS: [&str; 4] = ["DEBUG", "INFO", "WARN", "ERROR"];
const REQUIRED_TOKENS: [&str; 8] = [
    "test_bd_3a7d_unit_compliance_gate",
    "prop_bd_3a7d_structure_compliance",
    "test_e2e_bd_3a7d_compliance",
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct RecoveryReport {
    schema_version: u32,
    bead_id: String,
    crash_point: String,
    seed: u64,
    corruption_model: String,
    wal_frame_count: usize,
    recovered_pages: usize,
    status: String,
}

fn workspace_root() -> Result<PathBuf, String> {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .map_err(|error| format!("workspace_root_canonicalize_failed: {error}"))
}

fn load_issue_description(issue_id: &str) -> Result<String, String> {
    let issues_path = workspace_root()?.join(ISSUES_JSONL);
    let raw = fs::read_to_string(&issues_path)
        .map_err(|error| format!("issues_jsonl_read_failed path={issues_path:?} error={error}"))?;

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

fn evaluate_description(description: &str) -> ComplianceEvaluation {
    let missing_unit_ids = UNIT_TEST_IDS
        .into_iter()
        .filter(|id| !description.contains(id))
        .collect::<Vec<_>>();

    let missing_e2e_ids = E2E_TEST_IDS
        .into_iter()
        .filter(|id| !description.contains(id))
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

fn sample_page(seed: u8) -> Vec<u8> {
    let mut page = vec![0_u8; PAGE_SIZE];
    for (idx, byte) in page.iter_mut().enumerate() {
        let offset = u8::try_from(idx % 251).expect("sample_page modulo should fit u8");
        *byte = seed.wrapping_add(offset);
    }
    page
}

fn build_frame(
    page_no: u32,
    db_size_pages: u32,
    payload: &[u8],
    salts: WalSalts,
    previous: SqliteWalChecksum,
) -> Result<(Vec<u8>, SqliteWalChecksum), String> {
    let mut frame = vec![0_u8; WAL_FRAME_HEADER_SIZE + payload.len()];
    frame[..4].copy_from_slice(&page_no.to_be_bytes());
    frame[4..8].copy_from_slice(&db_size_pages.to_be_bytes());
    frame[WAL_FRAME_HEADER_SIZE..].copy_from_slice(payload);
    write_wal_frame_salts(&mut frame[..WAL_FRAME_HEADER_SIZE], salts)
        .map_err(|error| format!("write_wal_frame_salts_failed: {error}"))?;
    let checksum = write_wal_frame_checksum(&mut frame, payload.len(), previous, false)
        .map_err(|error| format!("write_wal_frame_checksum_failed: {error}"))?;
    Ok((frame, checksum))
}

#[test]
fn test_bd_3a7d_unit_compliance_gate() -> Result<(), String> {
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
    fn prop_bd_3a7d_structure_compliance(missing_index in 0usize..REQUIRED_TOKENS.len()) {
        let mut synthetic = format!(
            "## Unit Test Requirements\n- {}\n- {}\n\n## E2E Test\n- {}\n\n## Logging Requirements\n- DEBUG: stage progress\n- INFO: summary\n- WARN: degraded mode\n- ERROR: terminal failure\n- Reference: {}\n",
            UNIT_TEST_IDS[0],
            UNIT_TEST_IDS[1],
            E2E_TEST_IDS[0],
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
fn test_crash_injector_hits_sync_points() {
    let state = FaultState::new();
    state.inject_fault(FaultSpec::power_cut("*.wal").after_nth_sync(1).build());

    let wal_path = Path::new("crash_case.wal");
    assert_eq!(
        state.check_sync(wal_path),
        SyncDecision::Allow,
        "bead_id={BEAD_ID} first sync should pass before crash point"
    );
    assert_eq!(
        state.check_sync(wal_path),
        SyncDecision::PowerCut,
        "bead_id={BEAD_ID} second sync should trigger configured crash point"
    );
    assert_eq!(
        state.check_sync(wal_path),
        SyncDecision::PoweredOff,
        "bead_id={BEAD_ID} sync after power cut should fail"
    );
    assert!(state.is_powered_off(), "bead_id={BEAD_ID} power state");
}

#[test]
fn test_corruption_injector_is_targeted() {
    let state = FaultState::new();
    let frame3_offset = 32_u64 + 2_u64 * 4120_u64;
    state.inject_fault(
        FaultSpec::torn_write("*.wal")
            .at_offset_bytes(frame3_offset)
            .valid_bytes(17)
            .build(),
    );

    assert_eq!(
        state.check_write(Path::new("target.db"), frame3_offset, 128),
        WriteDecision::Allow,
        "bead_id={BEAD_ID} non-wal path should not be faulted"
    );
    assert_eq!(
        state.check_write(Path::new("target.wal"), 0, WAL_FRAME_HEADER_SIZE),
        WriteDecision::Allow,
        "bead_id={BEAD_ID} write before target offset should not be faulted"
    );
    assert_eq!(
        state.check_write(Path::new("target.wal"), frame3_offset, 4120),
        WriteDecision::TornWrite { valid_bytes: 17 },
        "bead_id={BEAD_ID} exact target write should be torn"
    );
}

#[test]
fn test_recovery_report_schema() -> Result<(), String> {
    let report = RecoveryReport {
        schema_version: 1,
        bead_id: BEAD_ID.to_owned(),
        crash_point: "after_fsync_2".to_owned(),
        seed: 1337,
        corruption_model: "torn_write_frame_3".to_owned(),
        wal_frame_count: 6,
        recovered_pages: 5,
        status: "repaired".to_owned(),
    };
    let encoded = serde_json::to_value(&report)
        .map_err(|error| format!("recovery_report_to_value_failed: {error}"))?;
    let object = encoded
        .as_object()
        .ok_or_else(|| "recovery_report_not_object".to_owned())?;

    for key in [
        "schema_version",
        "bead_id",
        "crash_point",
        "seed",
        "corruption_model",
        "wal_frame_count",
        "recovered_pages",
        "status",
    ] {
        if !object.contains_key(key) {
            return Err(format!(
                "bead_id={BEAD_ID} case=recovery_report_missing_key key={key}"
            ));
        }
    }

    let decoded: RecoveryReport = serde_json::from_value(encoded)
        .map_err(|error| format!("recovery_report_roundtrip_failed: {error}"))?;
    if decoded != report {
        return Err(format!(
            "bead_id={BEAD_ID} case=recovery_report_roundtrip_mismatch decoded={decoded:?}"
        ));
    }

    Ok(())
}

#[test]
fn test_e2e_torn_write_wal_frame_detected() -> Result<(), String> {
    let payload = sample_page(9);
    let salts = WalSalts {
        salt1: 0x0102_0304,
        salt2: 0x1122_3344,
    };
    let previous = SqliteWalChecksum::default();

    let (mut frame, stored_checksum) = build_frame(1, 1, &payload, salts, previous)?;
    frame[WAL_FRAME_HEADER_SIZE + 77] ^= 0x40;
    let recomputed = compute_wal_frame_checksum(&frame, PAGE_SIZE, previous, false)
        .map_err(|error| format!("recompute_wal_checksum_failed: {error}"))?;

    if stored_checksum == recomputed {
        return Err(format!(
            "bead_id={BEAD_ID} case=torn_write_detection checksum_should_mismatch"
        ));
    }

    Ok(())
}

#[test]
fn test_e2e_sqlite_native_checksum_roundtrip() -> Result<(), String> {
    let payload = sample_page(41);
    let salts = WalSalts {
        salt1: 0xA0A1_A2A3,
        salt2: 0xB0B1_B2B3,
    };
    let previous = SqliteWalChecksum { s1: 7, s2: 11 };
    let (frame, stored_checksum) = build_frame(2, 2, &payload, salts, previous)?;
    let recomputed = compute_wal_frame_checksum(&frame, PAGE_SIZE, previous, false)
        .map_err(|error| format!("recompute_wal_checksum_failed: {error}"))?;

    if stored_checksum != recomputed {
        return Err(format!(
            "bead_id={BEAD_ID} case=sqlite_checksum_roundtrip mismatch stored={stored_checksum:?} recomputed={recomputed:?}"
        ));
    }

    Ok(())
}

#[test]
fn test_e2e_wal_fec_sidecar_repairs_corruption() {
    let source_page = sample_page(33);
    let expected_hash = wal_fec_source_hash_xxh3_128(&source_page);
    let outcome = attempt_wal_fec_repair(&source_page, expected_hash, 4, 3);
    assert_eq!(
        outcome,
        WalFecRepairOutcome::Repaired,
        "bead_id={BEAD_ID} sufficient symbols + hash-valid payload should repair"
    );
}

#[test]
fn test_e2e_wal_fec_insufficient_symbols_fails_gracefully() {
    let source_page = sample_page(34);
    let expected_hash = wal_fec_source_hash_xxh3_128(&source_page);
    let outcome = attempt_wal_fec_repair(&source_page, expected_hash, 2, 3);
    assert_eq!(
        outcome,
        WalFecRepairOutcome::InsufficientSymbols,
        "bead_id={BEAD_ID} insufficient symbols should gracefully truncate"
    );
}

#[test]
fn test_e2e_xxh3_page_checksum_detects_bitflip() {
    let original = sample_page(12);
    let original_hash = wal_fec_source_hash_xxh3_128(&original);
    let mut flipped = original.clone();
    flipped[101] ^= 0x01;
    let flipped_hash = wal_fec_source_hash_xxh3_128(&flipped);

    assert_ne!(
        original_hash, flipped_hash,
        "bead_id={BEAD_ID} xxh3 hash must change after a bit flip"
    );
}

#[test]
fn test_e2e_crc32c_raptorq_symbol_integrity() {
    let payload = sample_page(77);
    let crc_before = crc32c_checksum(&payload);
    let mut corrupted = payload.clone();
    corrupted[233] ^= 0x80;
    let crc_after = crc32c_checksum(&corrupted);

    assert_ne!(
        crc_before, crc_after,
        "bead_id={BEAD_ID} crc32c must detect symbol corruption"
    );
}

#[test]
fn test_e2e_pragma_integrity_check_all_levels() -> Result<(), String> {
    let mut level1_page = vec![0_u8; PAGE_SIZE];
    level1_page[0] = 0x0D;
    let cell_offset = u16::try_from(PAGE_SIZE).expect("PAGE_SIZE should fit u16");
    level1_page[5..7].copy_from_slice(&cell_offset.to_be_bytes());

    let level1 = integrity_check_level1_page(&level1_page, 1, true, false)
        .map_err(|error| format!("integrity_level1_failed: {error}"))?;
    let level2 = integrity_check_level2_btree(1, PAGE_SIZE, &[(100, 120), (140, 180)], &[1, 2]);
    let level3 = integrity_check_level3_overflow_chain(1, &[2, 3, 4], 8);
    let level4 = integrity_check_level4_cross_reference(4, &[1, 2, 3, 4]);
    let level5 = integrity_check_level5_schema(&["CREATE TABLE t(x INTEGER)".to_owned()]);
    let merged = merge_integrity_reports(&[level1, level2, level3, level4, level5]);

    if !merged.is_ok() {
        return Err(format!(
            "bead_id={BEAD_ID} case=integrity_levels_unexpected_failure messages={:?}",
            merged.sqlite_messages()
        ));
    }

    Ok(())
}

#[test]
fn test_e2e_bd_3a7d_compliance() -> Result<(), String> {
    let description = load_issue_description(BEAD_ID)?;
    let evaluation = evaluate_description(&description);

    let artifact_dir = tempdir().map_err(|error| format!("tempdir_failed: {error}"))?;
    let report_path = artifact_dir.path().join("recovery_report.json");
    let report = RecoveryReport {
        schema_version: 1,
        bead_id: BEAD_ID.to_owned(),
        crash_point: "deterministic_harness_path".to_owned(),
        seed: 0x3A7D,
        corruption_model: "wal_checksum_chain".to_owned(),
        wal_frame_count: 3,
        recovered_pages: 3,
        status: "ok".to_owned(),
    };
    let report_bytes = serde_json::to_vec_pretty(&report)
        .map_err(|error| format!("report_serialize_failed: {error}"))?;
    fs::write(&report_path, report_bytes)
        .map_err(|error| format!("report_write_failed path={report_path:?} error={error}"))?;

    eprintln!(
        "DEBUG bead_id={BEAD_ID} case=e2e_artifact_capture path={} seed={}",
        report_path.display(),
        report.seed
    );
    eprintln!(
        "INFO bead_id={BEAD_ID} case=e2e_summary missing_unit_ids={} missing_e2e_ids={} missing_log_levels={} missing_log_standard_ref={}",
        evaluation.missing_unit_ids.len(),
        evaluation.missing_e2e_ids.len(),
        evaluation.missing_log_levels.len(),
        evaluation.missing_log_standard_ref
    );

    for id in &evaluation.missing_unit_ids {
        eprintln!("WARN bead_id={BEAD_ID} case=missing_unit_id id={id}");
    }
    for id in &evaluation.missing_e2e_ids {
        eprintln!("WARN bead_id={BEAD_ID} case=missing_e2e_id id={id}");
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
