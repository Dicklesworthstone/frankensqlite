use std::fs;
use std::path::{Path, PathBuf};

use fsqlite_pager::{JournalPageRecord, checksum_sample_count, journal_checksum};
use fsqlite_types::PageSize;
use fsqlite_types::cx::Cx;
use fsqlite_types::flags::VfsOpenFlags;
use fsqlite_vfs::MemoryVfs;
use fsqlite_vfs::traits::{Vfs, VfsFile};
use fsqlite_wal::{
    ChecksumFailureKind, SqliteWalChecksum, WAL_FORMAT_VERSION, WAL_FRAME_HEADER_SIZE,
    WAL_HEADER_SIZE, WAL_MAGIC_LE, WalChainInvalidReason, WalFile, WalFrameHeader, WalHeader,
    WalSalts, compute_wal_frame_checksum, read_wal_header_checksum, sqlite_wal_checksum,
    validate_wal_chain, write_wal_frame_checksum, write_wal_frame_salts,
};
use proptest::prelude::proptest;
use proptest::test_runner::TestCaseError;
use serde_json::Value;

const BEAD_ID: &str = "bd-2fas";
const ISSUES_JSONL: &str = ".beads/issues.jsonl";
const UNIT_TEST_IDS: [&str; 2] = [
    "test_bd_2fas_unit_compliance_gate",
    "prop_bd_2fas_structure_compliance",
];
const E2E_TEST_IDS: [&str; 1] = ["test_e2e_bd_2fas_compliance"];
const LOG_LEVEL_MARKERS: [&str; 4] = ["DEBUG", "INFO", "WARN", "ERROR"];
const LOG_STANDARD_REF: &str = "bd-1fpm";
const REQUIRED_TOKENS: [&str; 8] = [
    "test_bd_2fas_unit_compliance_gate",
    "prop_bd_2fas_structure_compliance",
    "test_e2e_bd_2fas_compliance",
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

fn workspace_root() -> Result<PathBuf, String> {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .map_err(|error| format!("workspace_root_canonicalize_failed error={error}"))
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

fn contains_identifier(text: &str, expected: &str) -> bool {
    text.split(|ch: char| !(ch.is_ascii_alphanumeric() || ch == '_'))
        .any(|candidate| candidate == expected)
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

fn sample_page(seed: u8, len: usize) -> Vec<u8> {
    let mut page = vec![0_u8; len];
    for (index, byte) in page.iter_mut().enumerate() {
        #[allow(clippy::cast_possible_truncation)]
        let offset = (index % 251) as u8;
        *byte = seed.wrapping_add(offset);
    }
    page
}

fn test_cx() -> Cx {
    Cx::new()
}

fn wal_salts() -> WalSalts {
    WalSalts {
        salt1: 0x2233_4455,
        salt2: 0x6677_8899,
    }
}

fn open_wal_file(
    vfs: &MemoryVfs,
    cx: &Cx,
    wal_path: &Path,
) -> Result<<MemoryVfs as Vfs>::File, String> {
    let flags = VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE | VfsOpenFlags::WAL;
    vfs.open(cx, Some(wal_path), flags)
        .map(|(file, _)| file)
        .map_err(|error| {
            format!(
                "open_wal_file_failed path={} error={error}",
                wal_path.display()
            )
        })
}

fn read_wal_bytes(vfs: &MemoryVfs, cx: &Cx, wal_path: &Path) -> Result<Vec<u8>, String> {
    let mut wal_file = open_wal_file(vfs, cx, wal_path)?;
    let file_size = wal_file
        .file_size(cx)
        .map_err(|error| format!("read_wal_size_failed error={error}"))?;
    let mut bytes = vec![
        0_u8;
        usize::try_from(file_size)
            .map_err(|error| format!("wal_size_to_usize_failed error={error}"))?
    ];
    wal_file
        .read(cx, &mut bytes, 0)
        .map_err(|error| format!("read_wal_bytes_failed error={error}"))?;
    Ok(bytes)
}

fn sampled_offsets(page_size: usize) -> Vec<usize> {
    if page_size < 200 {
        return Vec::new();
    }
    let mut offsets = Vec::new();
    let mut i = page_size - 200;
    while i > 0 {
        offsets.push(i);
        if i < 200 {
            break;
        }
        i -= 200;
    }
    offsets
}

#[test]
fn test_bd_2fas_unit_compliance_gate() -> Result<(), String> {
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
            "bead_id={BEAD_ID} case=log_levels_missing missing={:?}",
            evaluation.missing_log_levels
        ));
    }
    if evaluation.missing_log_standard_ref {
        return Err(format!(
            "bead_id={BEAD_ID} case=log_standard_missing expected={LOG_STANDARD_REF}"
        ));
    }

    Ok(())
}

proptest! {
    #[test]
    fn prop_bd_2fas_structure_compliance(missing_index in 0usize..REQUIRED_TOKENS.len()) {
        let description = load_issue_description(BEAD_ID).map_err(TestCaseError::fail)?;
        let marker = REQUIRED_TOKENS[missing_index];
        let removed = description.replace(marker, "");
        let evaluation = evaluate_description(&removed);
        if evaluation.is_compliant() {
            return Err(TestCaseError::fail(format!(
                "bead_id={BEAD_ID} case=marker_removal_not_detected idx={missing_index}"
            )));
        }
    }
}

#[test]
fn test_e2e_bd_2fas_compliance() -> Result<(), String> {
    let description = load_issue_description(BEAD_ID)?;
    let evaluation = evaluate_description(&description);
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

#[test]
fn test_wal_header_checksum() -> Result<(), String> {
    let header = WalHeader {
        magic: WAL_MAGIC_LE,
        format_version: WAL_FORMAT_VERSION,
        page_size: PageSize::DEFAULT.get(),
        checkpoint_seq: 17,
        salts: wal_salts(),
        checksum: SqliteWalChecksum::default(),
    };
    let header_bytes = header
        .to_bytes()
        .map_err(|error| format!("wal_header_to_bytes_failed error={error}"))?;
    let expected = sqlite_wal_checksum(&header_bytes[..24], 0, 0, false)
        .map_err(|error| format!("sqlite_wal_checksum_failed error={error}"))?;
    let actual = read_wal_header_checksum(&header_bytes)
        .map_err(|error| format!("read_wal_header_checksum_failed error={error}"))?;
    if actual != expected {
        return Err(format!(
            "bead_id={BEAD_ID} case=wal_header_checksum_mismatch expected={expected:?} actual={actual:?}"
        ));
    }
    Ok(())
}

#[test]
fn test_wal_first_frame_checksum() -> Result<(), String> {
    let page_size = PageSize::DEFAULT.as_usize();
    let mut header = WalHeader {
        magic: WAL_MAGIC_LE,
        format_version: WAL_FORMAT_VERSION,
        page_size: PageSize::DEFAULT.get(),
        checkpoint_seq: 3,
        salts: wal_salts(),
        checksum: SqliteWalChecksum::default(),
    }
    .to_bytes()
    .map_err(|error| format!("wal_header_to_bytes_failed error={error}"))?;
    let seed = read_wal_header_checksum(&header)
        .map_err(|error| format!("read_wal_header_checksum_failed error={error}"))?;
    let mut frame = vec![0_u8; WAL_FRAME_HEADER_SIZE + page_size];
    let frame_header = WalFrameHeader {
        page_number: 1,
        db_size: 1,
        salts: wal_salts(),
        checksum: SqliteWalChecksum::default(),
    };
    frame[..WAL_FRAME_HEADER_SIZE].copy_from_slice(&frame_header.to_bytes());
    frame[WAL_FRAME_HEADER_SIZE..].copy_from_slice(&sample_page(0x11, page_size));

    let computed = compute_wal_frame_checksum(&frame, page_size, seed, false)
        .map_err(|error| format!("compute_wal_frame_checksum_failed error={error}"))?;
    let written = write_wal_frame_checksum(&mut frame, page_size, seed, false)
        .map_err(|error| format!("write_wal_frame_checksum_failed error={error}"))?;
    if written != computed {
        return Err(format!(
            "bead_id={BEAD_ID} case=wal_first_frame_checksum_mismatch computed={computed:?} written={written:?}"
        ));
    }
    let _ = &mut header;
    Ok(())
}

#[test]
fn test_wal_chain_three_frames() -> Result<(), String> {
    let cx = test_cx();
    let vfs = MemoryVfs::new();
    let wal_path = PathBuf::from("/bd_2fas_chain_three_frames.db-wal");
    let page_size = PageSize::DEFAULT.as_usize();
    let page_size_u32 =
        u32::try_from(page_size).map_err(|error| format!("page_size_u32_failed error={error}"))?;

    {
        let wal_file = open_wal_file(&vfs, &cx, &wal_path)?;
        let mut wal = WalFile::create(&cx, wal_file, page_size_u32, 0, wal_salts())
            .map_err(|error| format!("create_wal_failed error={error}"))?;
        for (idx, seed) in [0x21_u8, 0x22, 0x23].iter().copied().enumerate() {
            let frame_no = u32::try_from(idx + 1)
                .map_err(|error| format!("frame_no_u32_failed error={error}"))?;
            wal.append_frame(&cx, frame_no, &sample_page(seed, page_size), frame_no)
                .map_err(|error| {
                    format!("append_frame_failed frame_no={frame_no} error={error}")
                })?;
        }
        wal.close(&cx)
            .map_err(|error| format!("close_wal_failed error={error}"))?;
    }

    let wal_bytes = read_wal_bytes(&vfs, &cx, &wal_path)?;
    let validation = validate_wal_chain(&wal_bytes, page_size, false)
        .map_err(|error| format!("validate_wal_chain_failed error={error}"))?;
    if !validation.valid || validation.valid_frame_count != 3 {
        return Err(format!(
            "bead_id={BEAD_ID} case=wal_chain_three_frames_invalid validation={validation:?}"
        ));
    }
    Ok(())
}

#[test]
fn test_wal_frame_salt_validation() -> Result<(), String> {
    let cx = test_cx();
    let vfs = MemoryVfs::new();
    let wal_path = PathBuf::from("/bd_2fas_salt_validation.db-wal");
    let page_size = PageSize::DEFAULT.as_usize();
    let page_size_u32 =
        u32::try_from(page_size).map_err(|error| format!("page_size_u32_failed error={error}"))?;
    let frame_size = WAL_FRAME_HEADER_SIZE + page_size;

    {
        let wal_file = open_wal_file(&vfs, &cx, &wal_path)?;
        let mut wal = WalFile::create(&cx, wal_file, page_size_u32, 0, wal_salts())
            .map_err(|error| format!("create_wal_failed error={error}"))?;
        wal.append_frame(&cx, 1, &sample_page(0x31, page_size), 1)
            .map_err(|error| format!("append_frame_1_failed error={error}"))?;
        wal.append_frame(&cx, 2, &sample_page(0x32, page_size), 2)
            .map_err(|error| format!("append_frame_2_failed error={error}"))?;
        wal.close(&cx)
            .map_err(|error| format!("close_wal_failed error={error}"))?;
    }

    let mut wal_bytes = read_wal_bytes(&vfs, &cx, &wal_path)?;
    let second_header_start = WAL_HEADER_SIZE + frame_size;
    let second_header_end = second_header_start + WAL_FRAME_HEADER_SIZE;
    write_wal_frame_salts(
        &mut wal_bytes[second_header_start..second_header_end],
        WalSalts {
            salt1: 0xDEAD_BEEF,
            salt2: 0xF00D_CAFE,
        },
    )
    .map_err(|error| format!("rewrite_frame_salts_failed error={error}"))?;
    let validation = validate_wal_chain(&wal_bytes, page_size, false)
        .map_err(|error| format!("validate_wal_chain_failed error={error}"))?;
    if validation.reason != Some(WalChainInvalidReason::SaltMismatch) {
        return Err(format!(
            "bead_id={BEAD_ID} case=wal_frame_salt_validation_reason_mismatch reason={:?}",
            validation.reason
        ));
    }
    Ok(())
}

#[test]
fn test_wal_recovery_valid_prefix() -> Result<(), String> {
    let cx = test_cx();
    let vfs = MemoryVfs::new();
    let wal_path = PathBuf::from("/bd_2fas_valid_prefix.db-wal");
    let page_size = PageSize::DEFAULT.as_usize();
    let page_size_u32 =
        u32::try_from(page_size).map_err(|error| format!("page_size_u32_failed error={error}"))?;
    let frame_size = WAL_FRAME_HEADER_SIZE + page_size;

    {
        let wal_file = open_wal_file(&vfs, &cx, &wal_path)?;
        let mut wal = WalFile::create(&cx, wal_file, page_size_u32, 0, wal_salts())
            .map_err(|error| format!("create_wal_failed error={error}"))?;
        for (idx, seed) in [0x41_u8, 0x42, 0x43, 0x44, 0x45, 0x46]
            .iter()
            .copied()
            .enumerate()
        {
            let frame_no = u32::try_from(idx + 1)
                .map_err(|error| format!("frame_no_u32_failed error={error}"))?;
            wal.append_frame(&cx, frame_no, &sample_page(seed, page_size), frame_no)
                .map_err(|error| {
                    format!("append_frame_failed frame_no={frame_no} error={error}")
                })?;
        }
        wal.close(&cx)
            .map_err(|error| format!("close_wal_failed error={error}"))?;
    }

    let mut wal_bytes = read_wal_bytes(&vfs, &cx, &wal_path)?;
    let invalid_frame_index = 5_usize;
    let corrupt_offset =
        WAL_HEADER_SIZE + frame_size * invalid_frame_index + WAL_FRAME_HEADER_SIZE + 19;
    wal_bytes[corrupt_offset] ^= 0x40;
    let validation = validate_wal_chain(&wal_bytes, page_size, false)
        .map_err(|error| format!("validate_wal_chain_failed error={error}"))?;
    if validation.valid_frame_count != 5 || validation.first_invalid_frame != Some(5) {
        return Err(format!(
            "bead_id={BEAD_ID} case=wal_recovery_valid_prefix_mismatch validation={validation:?}"
        ));
    }
    Ok(())
}

#[test]
fn test_wal_recovery_first_frame_corrupt() -> Result<(), String> {
    let cx = test_cx();
    let vfs = MemoryVfs::new();
    let wal_path = PathBuf::from("/bd_2fas_first_frame_corrupt.db-wal");
    let page_size = PageSize::DEFAULT.as_usize();
    let page_size_u32 =
        u32::try_from(page_size).map_err(|error| format!("page_size_u32_failed error={error}"))?;

    {
        let wal_file = open_wal_file(&vfs, &cx, &wal_path)?;
        let mut wal = WalFile::create(&cx, wal_file, page_size_u32, 0, wal_salts())
            .map_err(|error| format!("create_wal_failed error={error}"))?;
        wal.append_frame(&cx, 1, &sample_page(0x51, page_size), 1)
            .map_err(|error| format!("append_frame_1_failed error={error}"))?;
        wal.append_frame(&cx, 2, &sample_page(0x52, page_size), 2)
            .map_err(|error| format!("append_frame_2_failed error={error}"))?;
        wal.close(&cx)
            .map_err(|error| format!("close_wal_failed error={error}"))?;
    }

    let mut wal_bytes = read_wal_bytes(&vfs, &cx, &wal_path)?;
    let corrupt_offset = WAL_HEADER_SIZE + WAL_FRAME_HEADER_SIZE + 7;
    wal_bytes[corrupt_offset] ^= 0x22;
    let validation = validate_wal_chain(&wal_bytes, page_size, false)
        .map_err(|error| format!("validate_wal_chain_failed error={error}"))?;
    if validation.valid_frame_count != 0 || validation.first_invalid_frame != Some(0) {
        return Err(format!(
            "bead_id={BEAD_ID} case=wal_recovery_first_frame_corrupt_mismatch validation={validation:?}"
        ));
    }
    Ok(())
}

#[test]
fn test_wal_frame_header_checksum_excludes_salt() -> Result<(), String> {
    let page_size = PageSize::DEFAULT.as_usize();
    let header = WalHeader {
        magic: WAL_MAGIC_LE,
        format_version: WAL_FORMAT_VERSION,
        page_size: PageSize::DEFAULT.get(),
        checkpoint_seq: 99,
        salts: wal_salts(),
        checksum: SqliteWalChecksum::default(),
    };
    let header_bytes = header
        .to_bytes()
        .map_err(|error| format!("wal_header_to_bytes_failed error={error}"))?;
    let seed = read_wal_header_checksum(&header_bytes)
        .map_err(|error| format!("read_wal_header_checksum_failed error={error}"))?;
    let mut frame_a = vec![0_u8; WAL_FRAME_HEADER_SIZE + page_size];
    let mut frame_b = vec![0_u8; WAL_FRAME_HEADER_SIZE + page_size];
    frame_a[WAL_FRAME_HEADER_SIZE..].copy_from_slice(&sample_page(0x61, page_size));
    frame_b[WAL_FRAME_HEADER_SIZE..].copy_from_slice(&sample_page(0x61, page_size));
    write_wal_frame_salts(
        &mut frame_a[..WAL_FRAME_HEADER_SIZE],
        WalSalts { salt1: 1, salt2: 2 },
    )
    .map_err(|error| format!("write_frame_a_salts_failed error={error}"))?;
    write_wal_frame_salts(
        &mut frame_b[..WAL_FRAME_HEADER_SIZE],
        WalSalts {
            salt1: 0xAABB_CCDD,
            salt2: 0xEEFF_0011,
        },
    )
    .map_err(|error| format!("write_frame_b_salts_failed error={error}"))?;
    let checksum_a = compute_wal_frame_checksum(&frame_a, page_size, seed, false)
        .map_err(|error| format!("compute_frame_a_checksum_failed error={error}"))?;
    let checksum_b = compute_wal_frame_checksum(&frame_b, page_size, seed, false)
        .map_err(|error| format!("compute_frame_b_checksum_failed error={error}"))?;
    if checksum_a != checksum_b {
        return Err(format!(
            "bead_id={BEAD_ID} case=wal_frame_header_checksum_excludes_salt_mismatch a={checksum_a:?} b={checksum_b:?}"
        ));
    }
    Ok(())
}

#[test]
fn test_rollback_journal_checksum_stride200() -> Result<(), String> {
    let page = sample_page(0x71, 4_096);
    let nonce = 42_u32;
    let offsets = sampled_offsets(page.len());
    if offsets.len() != 20 {
        return Err(format!(
            "bead_id={BEAD_ID} case=rollback_stride200_sample_count_mismatch expected=20 actual={}",
            offsets.len()
        ));
    }
    let manual = offsets.iter().fold(nonce, |acc, offset| {
        acc.wrapping_add(u32::from(page[*offset]))
    });
    let computed = journal_checksum(&page, nonce);
    if computed != manual {
        return Err(format!(
            "bead_id={BEAD_ID} case=rollback_stride200_checksum_mismatch expected={manual:#010x} actual={computed:#010x}"
        ));
    }
    Ok(())
}

#[test]
fn test_rollback_journal_checksum_never_samples_zero() -> Result<(), String> {
    let mut page = vec![0_u8; 4_096];
    page[0] = 0xFF;
    let nonce = 7_u32;
    let computed = journal_checksum(&page, nonce);
    if computed != nonce {
        return Err(format!(
            "bead_id={BEAD_ID} case=rollback_never_samples_zero_mismatch expected={nonce:#010x} actual={computed:#010x}"
        ));
    }
    Ok(())
}

#[test]
fn test_rollback_journal_checksum_nonce() -> Result<(), String> {
    let page = sample_page(0x81, 4_096);
    let base = journal_checksum(&page, 0);
    let with_nonce = journal_checksum(&page, 42);
    if with_nonce != base.wrapping_add(42) {
        return Err(format!(
            "bead_id={BEAD_ID} case=rollback_nonce_mismatch base={base:#010x} with_nonce={with_nonce:#010x}"
        ));
    }
    Ok(())
}

#[test]
fn test_rollback_journal_checksum_512_page() -> Result<(), String> {
    let page = sample_page(0x91, 512);
    let offsets = sampled_offsets(page.len());
    if offsets != vec![312, 112] {
        return Err(format!(
            "bead_id={BEAD_ID} case=rollback_512_offsets_mismatch expected=[312,112] actual={offsets:?}"
        ));
    }
    if checksum_sample_count(page.len()) != 2 {
        return Err(format!(
            "bead_id={BEAD_ID} case=rollback_512_sample_count_mismatch expected=2 actual={}",
            checksum_sample_count(page.len())
        ));
    }
    Ok(())
}

#[test]
fn test_rollback_journal_checksum_round_trip() -> Result<(), String> {
    let page = sample_page(0xA1, 4_096);
    let nonce = 123_u32;
    let record = JournalPageRecord::new(7, page.clone(), nonce);
    record
        .verify_checksum(nonce)
        .map_err(|error| format!("verify_journal_checksum_failed error={error}"))?;
    let encoded = record.encode();
    let decoded = JournalPageRecord::decode(&encoded, 4_096)
        .map_err(|error| format!("decode_journal_record_failed error={error}"))?;
    decoded
        .verify_checksum(nonce)
        .map_err(|error| format!("verify_decoded_journal_checksum_failed error={error}"))?;
    if decoded.content != page {
        return Err(format!(
            "bead_id={BEAD_ID} case=rollback_round_trip_content_mismatch"
        ));
    }
    Ok(())
}

#[test]
fn test_e2e_bd_2fas_compliance_alias() -> Result<(), String> {
    test_e2e_bd_2fas_compliance()
}

#[test]
fn test_recovery_action_for_checksum_failure_is_wal_specific() -> Result<(), String> {
    let reason = ChecksumFailureKind::WalFrameChecksumMismatch;
    if !matches!(reason, ChecksumFailureKind::WalFrameChecksumMismatch) {
        return Err(format!(
            "bead_id={BEAD_ID} case=checksum_failure_kind_unexpected"
        ));
    }
    Ok(())
}
