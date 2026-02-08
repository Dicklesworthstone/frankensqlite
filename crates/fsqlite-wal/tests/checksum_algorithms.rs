use std::fmt::Write as _;
use std::fs;
use std::path::Path;
use std::process::Command;
use std::time::Instant;

use fsqlite_wal::checksum::{
    HashTier, SQLITE_DB_HEADER_RESERVED_OFFSET, SQLITE_DB_HEADER_SIZE, WAL_FRAME_HEADER_SIZE,
    WAL_HEADER_SIZE, WalChainInvalidReason, WalSalts, Xxh3Checksum128,
    configure_page_checksum_reserved_bytes, content_address_hash_128, crc32c_checksum,
    integrity_check_level1_page, integrity_check_level2_btree,
    integrity_check_level3_overflow_chain, integrity_check_level4_cross_reference,
    integrity_check_level5_schema, integrity_check_sqlite_file_level1, merge_integrity_reports,
    page_checksum_reserved_bytes, read_page_checksum, sqlite_wal_checksum, tier_for_algorithm,
    validate_wal_chain, verify_page_checksum, verify_wal_fec_source_hash,
    wal_fec_source_hash_xxh3_128, write_page_checksum, write_wal_frame_checksum,
    write_wal_frame_salts, write_wal_header_checksum, write_wal_header_salts,
    zero_page_checksum_trailer,
};
use tempfile::tempdir;

const BEAD_30B5_ID: &str = "bd-30b5";
const BEAD_3I98_ID: &str = "bd-3i98";
const PAGE_SIZE: usize = 4096;
const FRAME_SIZE: usize = WAL_FRAME_HEADER_SIZE + PAGE_SIZE;
const PAGE_RESERVED_U8: u8 = 16;
const BIG_END_CKSUM: bool = false;

struct WalFixture {
    wal_header: [u8; WAL_HEADER_SIZE],
    frame_bytes: Vec<u8>,
    source_hashes: Vec<Xxh3Checksum128>,
}

fn sample_payload(seed: u8) -> [u8; PAGE_SIZE] {
    let mut payload = [0_u8; PAGE_SIZE];
    for (index, byte) in payload.iter_mut().enumerate() {
        let reduced_index = u8::try_from(index % 251).expect("modulo result must fit in u8");
        *byte = reduced_index ^ seed;
    }
    payload
}

fn sample_btree_leaf_page() -> [u8; PAGE_SIZE] {
    let mut page = [0_u8; PAGE_SIZE];
    page[0] = 0x0D; // leaf table page
    page[1..3].copy_from_slice(&0_u16.to_be_bytes());
    page[3..5].copy_from_slice(&0_u16.to_be_bytes());
    page[5..7].copy_from_slice(
        &u16::try_from(PAGE_SIZE)
            .expect("PAGE_SIZE should fit in u16 for test")
            .to_be_bytes(),
    );
    page[7] = 0;
    page
}

fn frame_offset(frame_index: usize) -> usize {
    frame_index * FRAME_SIZE
}

fn frame_payload_slice(frame_bytes: &[u8], frame_index: usize) -> &[u8] {
    let start = frame_offset(frame_index) + WAL_FRAME_HEADER_SIZE;
    let end = frame_offset(frame_index) + FRAME_SIZE;
    &frame_bytes[start..end]
}

fn wal_bytes(wal_header: &[u8; WAL_HEADER_SIZE], frame_bytes: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(WAL_HEADER_SIZE + frame_bytes.len());
    bytes.extend_from_slice(wal_header);
    bytes.extend_from_slice(frame_bytes);
    bytes
}

fn build_wal(db_sizes: &[u32], salts: WalSalts) -> WalFixture {
    let mut wal_header = [0_u8; WAL_HEADER_SIZE];
    wal_header[..4].copy_from_slice(&0x377F_0682_u32.to_be_bytes());
    wal_header[4..8].copy_from_slice(&3_007_000_u32.to_be_bytes());
    wal_header[8..12].copy_from_slice(
        &u32::try_from(PAGE_SIZE)
            .expect("PAGE_SIZE must fit in u32")
            .to_be_bytes(),
    );
    write_wal_header_salts(&mut wal_header, salts).expect("header salts should be writable");

    let mut rolling_checksum = write_wal_header_checksum(&mut wal_header, BIG_END_CKSUM)
        .expect("header checksum should write");

    let mut frame_bytes = Vec::with_capacity(db_sizes.len() * FRAME_SIZE);
    let mut source_hashes = Vec::with_capacity(db_sizes.len());

    for (frame_index, db_size) in db_sizes.iter().copied().enumerate() {
        let mut frame = vec![0_u8; FRAME_SIZE];
        let (frame_header, frame_page) = frame.split_at_mut(WAL_FRAME_HEADER_SIZE);

        let page_number = u32::try_from(frame_index + 1).expect("frame index must fit in u32");
        frame_header[..4].copy_from_slice(&page_number.to_be_bytes());
        frame_header[4..8].copy_from_slice(&db_size.to_be_bytes());
        write_wal_frame_salts(frame_header, salts).expect("frame salts should write");

        let seed = u8::try_from((frame_index % 250) + 1).expect("frame seed must fit in u8");
        let payload = sample_payload(seed);
        frame_page.copy_from_slice(&payload);
        source_hashes.push(wal_fec_source_hash_xxh3_128(&payload));

        rolling_checksum =
            write_wal_frame_checksum(&mut frame, PAGE_SIZE, rolling_checksum, BIG_END_CKSUM)
                .expect("frame checksum should write");

        frame_bytes.extend_from_slice(&frame);
    }

    WalFixture {
        wal_header,
        frame_bytes,
        source_hashes,
    }
}

fn sqlite3_exec(db_path: &Path, sql: &str) -> std::process::Output {
    Command::new("sqlite3")
        .arg(db_path)
        .arg(sql)
        .output()
        .expect("sqlite3 command should execute")
}

fn normalize_sqlite_integrity_output(output: &std::process::Output) -> String {
    let stdout = String::from_utf8_lossy(&output.stdout);
    if output.status.success() && stdout.trim() == "ok" {
        "ok".to_owned()
    } else {
        "corrupt".to_owned()
    }
}

fn normalize_local_integrity_output(messages: &[String]) -> String {
    if messages.len() == 1 && messages[0] == "ok" {
        "ok".to_owned()
    } else {
        "corrupt".to_owned()
    }
}

#[test]
fn test_integrity_check_output_matches_c() {
    let temp_dir = tempdir().expect("tempdir should be created");
    let db_path = temp_dir.path().join("integrity_output_matches_c.db");
    let setup = sqlite3_exec(
        &db_path,
        "PRAGMA page_size=4096; CREATE TABLE t(x INTEGER); INSERT INTO t VALUES (1);",
    );
    assert!(
        setup.status.success(),
        "sqlite3 setup failed: {}",
        String::from_utf8_lossy(&setup.stderr)
    );

    let sqlite_report = sqlite3_exec(&db_path, "PRAGMA integrity_check;");
    assert!(
        sqlite_report.status.success(),
        "sqlite3 integrity_check failed: {}",
        String::from_utf8_lossy(&sqlite_report.stderr)
    );

    let db_bytes = fs::read(&db_path).expect("database file should be readable");
    let report =
        integrity_check_sqlite_file_level1(&db_bytes).expect("local integrity_check should run");

    assert_eq!(
        normalize_local_integrity_output(&report.sqlite_messages()),
        normalize_sqlite_integrity_output(&sqlite_report)
    );
    assert_eq!(normalize_sqlite_integrity_output(&sqlite_report), "ok");
}

#[test]
fn test_integrity_check_corrupt_output_parity_bad_header_magic() {
    let temp_dir = tempdir().expect("tempdir should be created");
    let db_path = temp_dir.path().join("integrity_corrupt_header.db");
    let setup = sqlite3_exec(
        &db_path,
        "PRAGMA page_size=4096; CREATE TABLE t(x INTEGER); INSERT INTO t VALUES (1);",
    );
    assert!(setup.status.success());

    let mut db_bytes = fs::read(&db_path).expect("database file should be readable");
    db_bytes[0] ^= 0x20;
    fs::write(&db_path, &db_bytes).expect("database file should be writable");

    let sqlite_report = sqlite3_exec(&db_path, "PRAGMA integrity_check;");
    let local_report =
        integrity_check_sqlite_file_level1(&db_bytes).expect("local integrity_check should run");

    assert_eq!(
        normalize_local_integrity_output(&local_report.sqlite_messages()),
        normalize_sqlite_integrity_output(&sqlite_report)
    );
    assert_eq!(normalize_sqlite_integrity_output(&sqlite_report), "corrupt");
}

#[test]
fn test_integrity_check_corrupt_output_parity_bad_btree_page_type() {
    let temp_dir = tempdir().expect("tempdir should be created");
    let db_path = temp_dir.path().join("integrity_corrupt_btree_type.db");
    let setup = sqlite3_exec(
        &db_path,
        "PRAGMA page_size=4096; CREATE TABLE t(x INTEGER); INSERT INTO t VALUES (1);",
    );
    assert!(setup.status.success());

    let mut db_bytes = fs::read(&db_path).expect("database file should be readable");
    let page_type_offset = SQLITE_DB_HEADER_SIZE;
    db_bytes[page_type_offset] = 0xFF;
    fs::write(&db_path, &db_bytes).expect("database file should be writable");

    let sqlite_report = sqlite3_exec(&db_path, "PRAGMA integrity_check;");
    let local_report =
        integrity_check_sqlite_file_level1(&db_bytes).expect("local integrity_check should run");

    assert_eq!(
        normalize_local_integrity_output(&local_report.sqlite_messages()),
        normalize_sqlite_integrity_output(&sqlite_report)
    );
    assert_eq!(normalize_sqlite_integrity_output(&sqlite_report), "corrupt");
}

#[test]
fn test_integrity_check_corrupt_output_parity_truncated_file() {
    let temp_dir = tempdir().expect("tempdir should be created");
    let db_path = temp_dir.path().join("integrity_corrupt_truncated.db");
    let setup = sqlite3_exec(
        &db_path,
        "PRAGMA page_size=4096; CREATE TABLE t(x INTEGER); INSERT INTO t VALUES (1);",
    );
    assert!(setup.status.success());

    let db_bytes = fs::read(&db_path).expect("database file should be readable");
    let truncated_len = SQLITE_DB_HEADER_SIZE + 24;
    let truncated = db_bytes[..truncated_len].to_vec();
    fs::write(&db_path, &truncated).expect("database file should be writable");

    let sqlite_report = sqlite3_exec(&db_path, "PRAGMA integrity_check;");
    let local_report =
        integrity_check_sqlite_file_level1(&truncated).expect("local integrity_check should run");

    assert_eq!(
        normalize_local_integrity_output(&local_report.sqlite_messages()),
        normalize_sqlite_integrity_output(&sqlite_report)
    );
    assert_eq!(normalize_sqlite_integrity_output(&sqlite_report), "corrupt");
}

#[test]
fn test_sqlite_native_checksum_compat() {
    let data = [
        0x10_u8, 0x20, 0x30, 0x40, 0x50, 0x60, 0x70, 0x80, 0x91, 0xA2, 0xB3, 0xC4, 0xD5, 0xE6,
        0xF7, 0x08,
    ];

    let little_endian =
        sqlite_wal_checksum(&data, 0, 0, false).expect("LE checksum should compute");
    let big_endian = sqlite_wal_checksum(&data, 0, 0, true).expect("BE checksum should compute");

    let little_endian_repeat =
        sqlite_wal_checksum(&data, 0, 0, false).expect("LE checksum recomputation should work");
    let big_endian_repeat =
        sqlite_wal_checksum(&data, 0, 0, true).expect("BE checksum recomputation should work");

    assert_eq!(
        little_endian.s1, 0xC584_4301,
        "bead_id={BEAD_30B5_ID} case=sqlite_checksum_le_s1"
    );
    assert_eq!(
        little_endian.s2, 0x8F1C_AA36,
        "bead_id={BEAD_30B5_ID} case=sqlite_checksum_le_s2"
    );
    assert_eq!(
        big_endian.s1, 0x0243_84C4,
        "bead_id={BEAD_30B5_ID} case=sqlite_checksum_be_s1"
    );
    assert_eq!(
        big_endian.s2, 0x38AB_1C8C,
        "bead_id={BEAD_30B5_ID} case=sqlite_checksum_be_s2"
    );
    assert_eq!(
        little_endian, little_endian_repeat,
        "bead_id={BEAD_30B5_ID} case=sqlite_checksum_le_deterministic"
    );
    assert_eq!(
        big_endian, big_endian_repeat,
        "bead_id={BEAD_30B5_ID} case=sqlite_checksum_be_deterministic"
    );
    assert_ne!(
        (little_endian.s1, little_endian.s2),
        (big_endian.s1, big_endian.s2),
        "bead_id={BEAD_30B5_ID} case=sqlite_checksum_endian_variant"
    );
}

#[test]
fn test_xxh3_round_trip() {
    let payload = b"frankensqlite-page-payload";
    let digest = Xxh3Checksum128::compute(payload);
    assert!(
        digest.verify(payload),
        "bead_id={BEAD_30B5_ID} case=xxh3_verify_original"
    );

    let mut tampered_payload = payload.to_vec();
    tampered_payload[0] ^= 0x80;
    assert!(
        !digest.verify(&tampered_payload),
        "bead_id={BEAD_30B5_ID} case=xxh3_detects_single_bit_flip"
    );
}

#[test]
fn test_crc32c_rfc_vectors() {
    let vector = b"123456789";
    assert_eq!(
        crc32c_checksum(vector),
        0xE306_9283,
        "bead_id={BEAD_30B5_ID} case=crc32c_rfc_vector"
    );
}

#[test]
fn test_three_tier_separation() {
    assert_eq!(tier_for_algorithm("xxh3_128"), Some(HashTier::Integrity));
    assert_eq!(
        tier_for_algorithm("blake3_128"),
        Some(HashTier::ContentAddressing)
    );
    assert_eq!(tier_for_algorithm("crc32c"), Some(HashTier::Protocol));
    assert_eq!(tier_for_algorithm("unknown"), None);

    let input = b"tiered-hash-input";
    let integrity_digest = Xxh3Checksum128::compute(input);
    let content_digest = content_address_hash_128(input);
    let protocol_digest = crc32c_checksum(input);

    assert!(
        integrity_digest.low != 0 || integrity_digest.high != 0,
        "bead_id={BEAD_30B5_ID} case=tier_integrity_nonzero_digest"
    );
    assert!(
        content_digest != [0_u8; 16],
        "bead_id={BEAD_30B5_ID} case=tier_content_nonzero_digest"
    );
    assert_ne!(
        protocol_digest, 0,
        "bead_id={BEAD_30B5_ID} case=tier_protocol_nonzero_crc"
    );
}

#[test]
fn test_hash_performance() {
    let block = [0xAB_u8; PAGE_SIZE];
    let rounds = 4096_u64;
    let block_len = u64::try_from(block.len()).expect("block length must fit in u64");
    let bytes_total = rounds * block_len;

    let start = Instant::now();
    let mut sink = 0_u64;
    for index in 0..rounds {
        let digest = Xxh3Checksum128::compute(&block);
        sink = sink.wrapping_add((digest.low ^ digest.high).wrapping_add(index));
    }
    let seconds = start.elapsed().as_secs_f64();
    let gigabytes_per_second = if seconds > 0.0 {
        (bytes_total as f64 / seconds) / 1_000_000_000.0
    } else {
        0.0
    };

    eprintln!(
        "bead_id={BEAD_30B5_ID} case=hash_perf xxh3_gbps={gigabytes_per_second:.2} sink={sink:#x}"
    );
    assert_ne!(
        sink, 0,
        "bead_id={BEAD_30B5_ID} case=hash_perf_sink_nonzero"
    );
}

#[test]
fn test_e2e_integrity_check_with_checksum_modes() {
    let salts = WalSalts {
        salt1: 0xA1A2_A3A4,
        salt2: 0xB1B2_B3B4,
    };
    let fixture = build_wal(&[1, 2, 3], salts);
    let valid_wal_bytes = wal_bytes(&fixture.wal_header, &fixture.frame_bytes);

    let valid_chain =
        validate_wal_chain(&valid_wal_bytes, PAGE_SIZE, BIG_END_CKSUM).expect("valid chain");
    assert!(valid_chain.header_valid);
    assert_eq!(valid_chain.valid_frame_count, 3);

    let mut frame_corrupt = fixture.frame_bytes.clone();
    frame_corrupt[frame_offset(1) + WAL_FRAME_HEADER_SIZE + 128] ^= 0x01;
    let corrupted_wal_bytes = wal_bytes(&fixture.wal_header, &frame_corrupt);
    let chain_after_corrupt = validate_wal_chain(&corrupted_wal_bytes, PAGE_SIZE, BIG_END_CKSUM)
        .expect("corrupted chain");
    assert_eq!(chain_after_corrupt.first_invalid_frame, Some(1));
    assert_eq!(
        chain_after_corrupt.first_invalid_reason,
        Some(WalChainInvalidReason::FrameChecksumMismatch)
    );

    let mut page = sample_payload(11);
    write_page_checksum(&mut page).expect("page checksum should write");
    assert!(verify_page_checksum(&page).expect("page checksum should verify"));
    page[512] ^= 0x80;
    assert!(!verify_page_checksum(&page).expect("corrupted page checksum should fail"));
}

#[test]
fn test_page_checksum_xxh3_round_trip() {
    let mut page = sample_payload(17);
    let stored_checksum = write_page_checksum(&mut page).expect("page checksum should write");
    let read_back_checksum = read_page_checksum(&page).expect("page checksum should read");

    assert_eq!(stored_checksum, read_back_checksum);
    assert!(verify_page_checksum(&page).expect("page checksum should verify"));
}

#[test]
fn test_page_checksum_detect_corruption() {
    let mut page = sample_payload(21);
    write_page_checksum(&mut page).expect("page checksum should write");
    page[1234] ^= 0x01;
    assert!(!verify_page_checksum(&page).expect("corruption should be detected"));
}

#[test]
fn test_reserved_bytes_16() {
    let mut header = [0_u8; 100];
    configure_page_checksum_reserved_bytes(&mut header, true)
        .expect("header reserved-byte config should succeed");
    assert_eq!(header[SQLITE_DB_HEADER_RESERVED_OFFSET], PAGE_RESERVED_U8);
    assert_eq!(
        page_checksum_reserved_bytes(&header).expect("reserved byte read should succeed"),
        PAGE_RESERVED_U8
    );
}

#[test]
fn test_legacy_reads_reserved_ok() {
    let temp_dir = tempdir().expect("tempdir should be created");
    let db_path = temp_dir.path().join("legacy_read_reserved_ok.db");

    let create_output = sqlite3_exec(
        &db_path,
        "PRAGMA page_size=4096; CREATE TABLE t(x INTEGER); INSERT INTO t VALUES (1);",
    );
    assert!(
        create_output.status.success(),
        "sqlite3 setup failed: {}",
        String::from_utf8_lossy(&create_output.stderr)
    );

    let mut db_bytes = fs::read(&db_path).expect("database file should be readable");
    db_bytes[SQLITE_DB_HEADER_RESERVED_OFFSET] = PAGE_RESERVED_U8;
    fs::write(&db_path, db_bytes).expect("database file should be writable");

    let query_output = sqlite3_exec(&db_path, "SELECT COUNT(*) FROM t;");
    assert!(
        query_output.status.success(),
        "sqlite3 read failed: {}",
        String::from_utf8_lossy(&query_output.stderr)
    );
    assert_eq!(String::from_utf8_lossy(&query_output.stdout).trim(), "1");
}

#[test]
fn test_legacy_writes_invalidate_checksum() {
    let mut page = sample_payload(29);
    write_page_checksum(&mut page).expect("page checksum should write");
    assert!(verify_page_checksum(&page).expect("checksum should verify before legacy write"));

    zero_page_checksum_trailer(&mut page).expect("legacy zeroing should succeed");
    assert!(!verify_page_checksum(&page).expect("zeroed trailer should invalidate checksum"));
}

#[test]
fn test_wal_cumulative_chain_valid() {
    let salts = WalSalts {
        salt1: 0x1111_2222,
        salt2: 0x3333_4444,
    };
    let fixture = build_wal(&[1_u32; 100], salts);

    let wal_bytes = wal_bytes(&fixture.wal_header, &fixture.frame_bytes);
    let validation =
        validate_wal_chain(&wal_bytes, PAGE_SIZE, BIG_END_CKSUM).expect("chain should validate");
    assert!(validation.valid);
    assert_eq!(validation.valid_frames, 100);
    assert_eq!(validation.replayable_frames, 100);
    assert_eq!(validation.first_invalid_frame, None);
    assert_eq!(validation.reason, None);
}

#[test]
fn test_wal_cumulative_chain_torn() {
    let salts = WalSalts {
        salt1: 0x1212_3434,
        salt2: 0x5656_7878,
    };
    let fixture = build_wal(&[1_u32; 100], salts);

    let torn_frame_index = 50;
    let torn_cut = frame_offset(torn_frame_index) + WAL_FRAME_HEADER_SIZE + PAGE_SIZE / 2;
    let torn_frames = fixture.frame_bytes[..torn_cut].to_vec();

    let torn_wal_bytes = wal_bytes(&fixture.wal_header, &torn_frames);
    let validation = validate_wal_chain(&torn_wal_bytes, PAGE_SIZE, BIG_END_CKSUM)
        .expect("torn chain should parse");
    assert_eq!(validation.valid_frames, 50);
    assert_eq!(validation.replayable_frames, 50);
    assert_eq!(validation.first_invalid_frame, Some(50));
    assert_eq!(
        validation.reason,
        Some(WalChainInvalidReason::TruncatedFrame)
    );
}

#[test]
fn test_wal_cumulative_chain_modified() {
    let salts = WalSalts {
        salt1: 0x0A0A_1B1B,
        salt2: 0x2C2C_3D3D,
    };
    let mut fixture = build_wal(&[1_u32; 100], salts);

    let modified_frame_index = 10;
    let corrupt_offset = frame_offset(modified_frame_index) + WAL_FRAME_HEADER_SIZE + 37;
    fixture.frame_bytes[corrupt_offset] ^= 0xFF;

    let wal_bytes = wal_bytes(&fixture.wal_header, &fixture.frame_bytes);
    let validation = validate_wal_chain(&wal_bytes, PAGE_SIZE, BIG_END_CKSUM)
        .expect("modified chain should parse");
    assert_eq!(validation.valid_frames, 10);
    assert_eq!(validation.first_invalid_frame, Some(10));
    assert_eq!(
        validation.reason,
        Some(WalChainInvalidReason::FrameChecksumMismatch)
    );
}

#[test]
fn test_wal_recovery_truncation() {
    let salts = WalSalts {
        salt1: 0x0102_0304,
        salt2: 0x0506_0708,
    };
    let mut fixture = build_wal(&[1_u32; 40], salts);

    let first_invalid_frame = 30;
    let corrupt_offset = frame_offset(first_invalid_frame) + WAL_FRAME_HEADER_SIZE + 9;
    fixture.frame_bytes[corrupt_offset] ^= 0x10;

    let wal_bytes = wal_bytes(&fixture.wal_header, &fixture.frame_bytes);
    let validation = validate_wal_chain(&wal_bytes, PAGE_SIZE, BIG_END_CKSUM)
        .expect("recovery chain should parse");
    assert_eq!(validation.first_invalid_frame, Some(30));
    assert_eq!(validation.valid_frames, first_invalid_frame);
    assert_eq!(validation.replayable_frames, first_invalid_frame);
    assert_eq!(
        validation.replayable_prefix_len,
        WAL_HEADER_SIZE + first_invalid_frame * FRAME_SIZE
    );
}

#[test]
fn test_wal_salt_mismatch_rejects() {
    let salts = WalSalts {
        salt1: 0xABCD_1234,
        salt2: 0x9876_5432,
    };
    let mut fixture = build_wal(&[1_u32; 8], salts);

    let mismatch_index = 3;
    let start = frame_offset(mismatch_index);
    let end = start + WAL_FRAME_HEADER_SIZE;
    write_wal_frame_salts(
        &mut fixture.frame_bytes[start..end],
        WalSalts {
            salt1: 0xDEAD_BEEF,
            salt2: 0xFACE_FEED,
        },
    )
    .expect("salt rewrite should succeed");

    let wal_bytes = wal_bytes(&fixture.wal_header, &fixture.frame_bytes);
    let validation = validate_wal_chain(&wal_bytes, PAGE_SIZE, BIG_END_CKSUM)
        .expect("salt mismatch should parse");
    assert_eq!(validation.valid_frames, 3);
    assert_eq!(validation.first_invalid_frame, Some(3));
    assert_eq!(validation.reason, Some(WalChainInvalidReason::SaltMismatch));
}

#[test]
fn test_commit_frame_marker() {
    let salts = WalSalts {
        salt1: 0xABAB_CACA,
        salt2: 0xDADA_EFEF,
    };
    let fixture = build_wal(&[0_u32, 0, 8, 0, 0, 16, 0], salts);

    let wal_bytes = wal_bytes(&fixture.wal_header, &fixture.frame_bytes);
    let validation =
        validate_wal_chain(&wal_bytes, PAGE_SIZE, BIG_END_CKSUM).expect("commit chain");
    assert_eq!(validation.valid_frames, 7);
    assert_eq!(validation.last_commit_frame, Some(5));
    assert_eq!(validation.replayable_frames, 6);
}

#[test]
fn test_partial_txn_discarded() {
    let salts = WalSalts {
        salt1: 0x0101_0202,
        salt2: 0x0303_0404,
    };
    let fixture = build_wal(&[0_u32, 0, 7, 0, 0], salts);

    let wal_bytes = wal_bytes(&fixture.wal_header, &fixture.frame_bytes);
    let validation =
        validate_wal_chain(&wal_bytes, PAGE_SIZE, BIG_END_CKSUM).expect("partial chain");
    assert_eq!(validation.valid_frames, 5);
    assert_eq!(validation.last_commit_frame, Some(2));
    assert_eq!(validation.replayable_frames, 3);
}

#[test]
fn test_self_healing_random_access() {
    let salts = WalSalts {
        salt1: 0x1111_1111,
        salt2: 0x2222_2222,
    };
    let mut fixture = build_wal(&[1_u32, 1, 1, 1], salts);

    let damaged_frame_index = 1;
    let damaged_offset = frame_offset(damaged_frame_index) + WAL_FRAME_HEADER_SIZE + 77;
    fixture.frame_bytes[damaged_offset] ^= 0x80;

    let wal_bytes = wal_bytes(&fixture.wal_header, &fixture.frame_bytes);
    let chain_validation =
        validate_wal_chain(&wal_bytes, PAGE_SIZE, BIG_END_CKSUM).expect("damaged chain");
    assert_eq!(chain_validation.first_invalid_frame, Some(1));
    assert_eq!(
        chain_validation.reason,
        Some(WalChainInvalidReason::FrameChecksumMismatch)
    );

    let damaged_payload = frame_payload_slice(&fixture.frame_bytes, damaged_frame_index);
    let intact_payload = frame_payload_slice(&fixture.frame_bytes, 3);

    assert!(
        !verify_wal_fec_source_hash(damaged_payload, fixture.source_hashes[damaged_frame_index]),
        "bead_id={BEAD_3I98_ID} case=self_healing_detects_damaged_source"
    );
    assert!(
        verify_wal_fec_source_hash(intact_payload, fixture.source_hashes[3]),
        "bead_id={BEAD_3I98_ID} case=self_healing_validates_random_access_source"
    );
}

#[test]
fn test_e2e_bd_3i98() {
    let salts = WalSalts {
        salt1: 0x9090_A0A0,
        salt2: 0xB0B0_C0C0,
    };
    let fixture = build_wal(&[1_u32; 100], salts);

    for crash_frame in [0_usize, 1, 13, 37, 89] {
        let torn_cut = frame_offset(crash_frame) + WAL_FRAME_HEADER_SIZE + PAGE_SIZE / 3;
        let torn_frames = fixture.frame_bytes[..torn_cut].to_vec();
        let torn_wal_bytes = wal_bytes(&fixture.wal_header, &torn_frames);
        let torn_validation = validate_wal_chain(&torn_wal_bytes, PAGE_SIZE, BIG_END_CKSUM)
            .expect("torn WAL should parse");

        assert_eq!(torn_validation.valid_frames, crash_frame);
        assert_eq!(torn_validation.replayable_frames, crash_frame);
        assert_eq!(torn_validation.first_invalid_frame, Some(crash_frame));
    }

    let mut corrupted_frames = fixture.frame_bytes.clone();
    let corrupted_frame = 42;
    let corrupted_offset = frame_offset(corrupted_frame) + WAL_FRAME_HEADER_SIZE + 211;
    corrupted_frames[corrupted_offset] ^= 0x01;
    let corrupted_wal_bytes = wal_bytes(&fixture.wal_header, &corrupted_frames);
    let corrupted_validation =
        validate_wal_chain(&corrupted_wal_bytes, PAGE_SIZE, BIG_END_CKSUM).expect("corrupted WAL");

    assert_eq!(
        corrupted_validation.first_invalid_frame,
        Some(corrupted_frame)
    );
    assert_eq!(
        corrupted_validation.reason,
        Some(WalChainInvalidReason::FrameChecksumMismatch)
    );

    let mut checksummed_page = sample_payload(73);
    write_page_checksum(&mut checksummed_page).expect("page checksum write should succeed");
    assert!(verify_page_checksum(&checksummed_page).expect("checksum should verify"));
    checksummed_page[2048] ^= 0x04;
    assert!(
        !verify_page_checksum(&checksummed_page).expect("single-bit corruption should be detected")
    );
}

#[test]
fn test_e2e_bd_36hc() {
    let temp_dir = tempdir().expect("tempdir should be created");
    let db_path = temp_dir.path().join("e2e_bd_36hc.db");

    let mut setup_sql = String::from("PRAGMA page_size=4096;");
    for table_index in 0..50 {
        let _ = write!(
            setup_sql,
            "CREATE TABLE t{table_index}(id INTEGER PRIMARY KEY, v TEXT);"
        );
        let _ = write!(
            setup_sql,
            "CREATE INDEX i{table_index} ON t{table_index}(v);"
        );
        let _ = write!(
            setup_sql,
            "INSERT INTO t{table_index}(v) VALUES ('row-{table_index}');"
        );
    }

    let setup_output = sqlite3_exec(&db_path, &setup_sql);
    assert!(
        setup_output.status.success(),
        "sqlite3 setup failed: {}",
        String::from_utf8_lossy(&setup_output.stderr)
    );

    let pragma_output = sqlite3_exec(&db_path, "PRAGMA integrity_check;");
    assert!(
        pragma_output.status.success(),
        "sqlite3 integrity_check failed: {}",
        String::from_utf8_lossy(&pragma_output.stderr)
    );
    assert_eq!(String::from_utf8_lossy(&pragma_output.stdout).trim(), "ok");

    let level1 = integrity_check_level1_page(&sample_btree_leaf_page(), 1, true, false)
        .expect("level1 integrity check should run");
    let level2 = integrity_check_level2_btree(
        1,
        PAGE_SIZE,
        &[(128, 170), (220, 260), (300, 340)],
        &[10, 20, 30],
    );
    let level3 = integrity_check_level3_overflow_chain(1, &[4, 8, 12], 512);
    let level4 = integrity_check_level4_cross_reference(50, &(1..=50).collect::<Vec<u32>>());
    let level5 = integrity_check_level5_schema(&["CREATE TABLE x(id INTEGER)".to_owned()]);
    let report = merge_integrity_reports(&[level1, level2, level3, level4, level5]);
    assert!(report.is_ok());
    assert_eq!(report.sqlite_messages(), vec!["ok".to_owned()]);

    let salts = WalSalts {
        salt1: 0xABCD_1001,
        salt2: 0x1234_9001,
    };
    let fixture = build_wal(&[1_u32; 100], salts);
    for scenario in 0..100_usize {
        let crash_frame = (scenario * 19) % 100;
        let torn_cut = frame_offset(crash_frame) + WAL_FRAME_HEADER_SIZE + PAGE_SIZE / 3;
        let torn_frames = fixture.frame_bytes[..torn_cut].to_vec();
        let torn_wal_bytes = wal_bytes(&fixture.wal_header, &torn_frames);
        let validation = validate_wal_chain(&torn_wal_bytes, PAGE_SIZE, BIG_END_CKSUM)
            .expect("torn WAL should parse");
        assert_eq!(validation.valid_frames, crash_frame);
        assert_eq!(validation.replayable_frames, crash_frame);
        assert_eq!(validation.first_invalid_frame, Some(crash_frame));
    }
}
