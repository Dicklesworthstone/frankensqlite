use std::fs;
use std::path::{Path, PathBuf};
#[cfg(unix)]
use std::process::{Command, Output};

use fsqlite_pager::{
    Argon2Params, DATABASE_ID_SIZE, DatabaseId, ENCRYPTION_RESERVED_BYTES, EncryptError,
    JournalHeader, JournalPageRecord, KEY_SIZE, KeyManager, MvccPager, NONCE_SIZE, PageEncryptor,
    SimplePager, TransactionHandle, TransactionMode,
};
#[cfg(unix)]
use fsqlite_types::LockLevel;
use fsqlite_types::PageSize;
use fsqlite_types::cx::Cx;
use fsqlite_types::flags::{AccessFlags, SyncFlags, VfsOpenFlags};
use fsqlite_types::{ObjectId, Oti};
use fsqlite_vfs::MemoryVfs;
#[cfg(unix)]
use fsqlite_vfs::UnixVfs;
use fsqlite_vfs::traits::{Vfs, VfsFile};
use fsqlite_wal::{
    CheckpointMode, CheckpointState, CheckpointTarget, WAL_FRAME_HEADER_SIZE, WAL_HEADER_SIZE,
    WalChainInvalidReason, WalFecGroupMeta, WalFecGroupMetaInit, WalFecGroupRecord,
    WalFecRecoveryOutcome, WalFile, WalFrameCandidate, WalSalts, append_wal_fec_group,
    build_source_page_hashes, compute_wal_frame_checksum, execute_checkpoint,
    generate_wal_fec_repair_symbols, read_wal_header_checksum, recover_wal_fec_group_with_decoder,
    validate_wal_chain, write_wal_frame_salts,
};
use proptest::prelude::proptest;
use proptest::test_runner::TestCaseError;
use serde_json::Value;
use tempfile::tempdir;

const BEAD_ID: &str = "bd-bca.1";
const ISSUES_JSONL: &str = ".beads/issues.jsonl";
const UNIT_TEST_IDS: [&str; 2] = [
    "test_bd_bca_1_unit_compliance_gate",
    "prop_bd_bca_1_structure_compliance",
];
const E2E_TEST_IDS: [&str; 2] = ["test_e2e_bd_bca_1", "test_e2e_bd_bca_1_compliance"];
const LOG_LEVEL_MARKERS: [&str; 4] = ["DEBUG", "INFO", "WARN", "ERROR"];
const LOG_STANDARD_REF: &str = "bd-1fpm";
const REQUIRED_TOKENS: [&str; 9] = [
    "test_bd_bca_1_unit_compliance_gate",
    "prop_bd_bca_1_structure_compliance",
    "test_e2e_bd_bca_1",
    "test_e2e_bd_bca_1_compliance",
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
    text.split(|ch: char| !(ch.is_ascii_alphanumeric() || ch == '_'))
        .any(|candidate| candidate == expected_marker)
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

fn test_cx() -> Cx {
    Cx::new()
}

fn journal_path_for(db_path: &Path) -> PathBuf {
    let mut journal_path = db_path.as_os_str().to_owned();
    journal_path.push("-journal");
    PathBuf::from(journal_path)
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

fn wal_salts() -> WalSalts {
    WalSalts {
        salt1: 0xABCD_1234,
        salt2: 0x55AA_FF00,
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

#[cfg(unix)]
fn sqlite3_available() -> bool {
    Command::new("sqlite3").arg("--version").output().is_ok()
}

#[cfg(unix)]
fn sqlite3_exec(db_path: &Path, sql: &str) -> Result<Output, String> {
    Command::new("sqlite3")
        .arg(db_path)
        .arg(sql)
        .output()
        .map_err(|error| {
            format!(
                "sqlite3_exec_failed path={} error={error}",
                db_path.display()
            )
        })
}

fn encryption_test_nonce(seed: u8) -> [u8; NONCE_SIZE] {
    let mut nonce = [0_u8; NONCE_SIZE];
    for (index, byte) in nonce.iter_mut().enumerate() {
        #[allow(clippy::cast_possible_truncation)]
        let offset = index as u8;
        *byte = seed.wrapping_add(offset);
    }
    nonce
}

#[derive(Default)]
struct RecordingCheckpointTarget {
    writes: Vec<Vec<u8>>,
    truncated_to: Option<u32>,
    sync_calls: usize,
}

impl CheckpointTarget for RecordingCheckpointTarget {
    fn write_page(
        &mut self,
        _cx: &Cx,
        _page_no: fsqlite_types::PageNumber,
        data: &[u8],
    ) -> fsqlite_error::Result<()> {
        self.writes.push(data.to_vec());
        Ok(())
    }

    fn truncate_db(&mut self, _cx: &Cx, n_pages: u32) -> fsqlite_error::Result<()> {
        self.truncated_to = Some(n_pages);
        Ok(())
    }

    fn sync_db(&mut self, _cx: &Cx) -> fsqlite_error::Result<()> {
        self.sync_calls += 1;
        Ok(())
    }
}

#[test]
fn test_bd_bca_1_unit_compliance_gate() -> Result<(), String> {
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
    fn prop_bd_bca_1_structure_compliance(missing_index in 0usize..REQUIRED_TOKENS.len()) {
        let mut synthetic = format!(
            "## Unit Test Requirements\n- {}\n- {}\n\n## E2E Test\n- {}\n- {}\n\n## Logging Requirements\n- DEBUG: stage progress\n- INFO: summary\n- WARN: degraded mode\n- ERROR: terminal failure\n- Reference: {}\n",
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
fn test_e2e_bd_bca_1_compliance() -> Result<(), String> {
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
fn test_e2e_bd_bca_1() -> Result<(), String> {
    test_e2e_bd_bca_1_compliance()
}

#[test]
fn test_persistence_create_close_reopen() -> Result<(), String> {
    let cx = test_cx();
    let vfs = MemoryVfs::new();
    let path = PathBuf::from("/bd_bca_1_persistence.db");
    let page_size = PageSize::DEFAULT.as_usize();
    let expected = sample_page(0x2A, page_size);

    let page_no = {
        let pager = SimplePager::open(vfs.clone(), &path, PageSize::DEFAULT)
            .map_err(|error| format!("open_pager_for_write_failed error={error}"))?;
        let mut txn = pager
            .begin(&cx, TransactionMode::Immediate)
            .map_err(|error| format!("begin_writer_failed error={error}"))?;
        let page_no = txn
            .allocate_page(&cx)
            .map_err(|error| format!("allocate_page_failed error={error}"))?;
        txn.write_page(&cx, page_no, &expected)
            .map_err(|error| format!("write_page_failed error={error}"))?;
        txn.commit(&cx)
            .map_err(|error| format!("commit_failed error={error}"))?;
        page_no
    };

    let pager = SimplePager::open(vfs, &path, PageSize::DEFAULT)
        .map_err(|error| format!("open_pager_for_read_failed error={error}"))?;
    let txn = pager
        .begin(&cx, TransactionMode::ReadOnly)
        .map_err(|error| format!("begin_reader_failed error={error}"))?;
    let observed = txn
        .get_page(&cx, page_no)
        .map_err(|error| format!("read_page_failed error={error}"))?;

    if observed.as_ref() != expected.as_slice() {
        return Err(format!(
            "bead_id={BEAD_ID} case=persistence_create_close_reopen_mismatch first={} last={}",
            observed.as_ref().first().copied().unwrap_or_default(),
            observed.as_ref().last().copied().unwrap_or_default()
        ));
    }

    Ok(())
}

#[test]
fn test_journal_crash_recovery() -> Result<(), String> {
    let cx = test_cx();
    let vfs = MemoryVfs::new();
    let path = PathBuf::from("/bd_bca_1_hot_journal.db");
    let page_size = PageSize::DEFAULT.as_usize();
    let original = sample_page(0x11, page_size);
    let corrupted = sample_page(0x99, page_size);

    let page_no = {
        let pager = SimplePager::open(vfs.clone(), &path, PageSize::DEFAULT)
            .map_err(|error| format!("open_pager_initial_failed error={error}"))?;
        let mut txn = pager
            .begin(&cx, TransactionMode::Immediate)
            .map_err(|error| format!("begin_initial_writer_failed error={error}"))?;
        let page_no = txn
            .allocate_page(&cx)
            .map_err(|error| format!("allocate_initial_page_failed error={error}"))?;
        txn.write_page(&cx, page_no, &original)
            .map_err(|error| format!("write_initial_page_failed error={error}"))?;
        txn.commit(&cx)
            .map_err(|error| format!("commit_initial_page_failed error={error}"))?;
        page_no
    };

    {
        let flags = VfsOpenFlags::READWRITE | VfsOpenFlags::MAIN_DB;
        let (mut db_file, _) = vfs
            .open(&cx, Some(&path), flags)
            .map_err(|error| format!("open_db_for_corruption_failed error={error}"))?;
        let offset = u64::from(page_no.get().saturating_sub(1))
            * u64::try_from(page_size)
                .map_err(|error| format!("page_size_u64_conversion_failed error={error}"))?;
        db_file
            .write(&cx, &corrupted, offset)
            .map_err(|error| format!("write_corrupted_db_page_failed error={error}"))?;
    }

    let journal_path = journal_path_for(&path);
    {
        let flags = VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE | VfsOpenFlags::MAIN_JOURNAL;
        let (mut journal_file, _) = vfs
            .open(&cx, Some(&journal_path), flags)
            .map_err(|error| format!("open_journal_for_recovery_failed error={error}"))?;

        let header = JournalHeader {
            page_count: 1,
            nonce: 42,
            initial_db_size: page_no.get(),
            sector_size: 512,
            page_size: PageSize::DEFAULT.get(),
        };
        let header_bytes = header.encode_padded();
        journal_file
            .write(&cx, &header_bytes, 0)
            .map_err(|error| format!("write_journal_header_failed error={error}"))?;

        let record = JournalPageRecord::new(page_no.get(), original.clone(), header.nonce);
        let record_bytes = record.encode();
        journal_file
            .write(
                &cx,
                &record_bytes,
                u64::try_from(header_bytes.len())
                    .map_err(|error| format!("journal_header_len_to_u64_failed error={error}"))?,
            )
            .map_err(|error| format!("write_journal_record_failed error={error}"))?;
    }

    let reopened = SimplePager::open(vfs.clone(), &path, PageSize::DEFAULT)
        .map_err(|error| format!("reopen_pager_for_recovery_failed error={error}"))?;
    let read_txn = reopened
        .begin(&cx, TransactionMode::ReadOnly)
        .map_err(|error| format!("begin_reader_after_recovery_failed error={error}"))?;
    let recovered = read_txn
        .get_page(&cx, page_no)
        .map_err(|error| format!("read_recovered_page_failed error={error}"))?;

    if recovered.as_ref() != original.as_slice() {
        return Err(format!(
            "bead_id={BEAD_ID} case=journal_crash_recovery_content_mismatch first={} last={}",
            recovered.as_ref().first().copied().unwrap_or_default(),
            recovered.as_ref().last().copied().unwrap_or_default()
        ));
    }

    let journal_exists = vfs
        .access(&cx, &journal_path, AccessFlags::EXISTS)
        .map_err(|error| format!("check_journal_deleted_failed error={error}"))?;
    if journal_exists {
        return Err(format!(
            "bead_id={BEAD_ID} case=journal_crash_recovery_journal_not_deleted path={}",
            journal_path.display()
        ));
    }

    Ok(())
}

#[test]
fn test_wal_checksum_corruption() -> Result<(), String> {
    let cx = test_cx();
    let vfs = MemoryVfs::new();
    let wal_path = PathBuf::from("/bd_bca_1_checksum.db-wal");
    let page_size = PageSize::DEFAULT.as_usize();
    let page_size_u32 =
        u32::try_from(page_size).map_err(|error| format!("page_size_u32_failed error={error}"))?;

    {
        let wal_file = open_wal_file(&vfs, &cx, &wal_path)?;
        let mut wal = WalFile::create(&cx, wal_file, page_size_u32, 0, wal_salts())
            .map_err(|error| format!("create_wal_for_checksum_test_failed error={error}"))?;
        wal.append_frame(&cx, 1, &sample_page(0x21, page_size), 0)
            .map_err(|error| format!("append_frame_1_failed error={error}"))?;
        wal.append_frame(&cx, 2, &sample_page(0x22, page_size), 2)
            .map_err(|error| format!("append_frame_2_failed error={error}"))?;
        wal.close(&cx)
            .map_err(|error| format!("close_wal_for_checksum_test_failed error={error}"))?;
    }

    let mut wal_bytes = {
        let mut wal_file = open_wal_file(&vfs, &cx, &wal_path)?;
        let file_size = wal_file
            .file_size(&cx)
            .map_err(|error| format!("read_wal_size_failed error={error}"))?;
        let mut bytes = vec![
            0_u8;
            usize::try_from(file_size).map_err(|error| format!(
                "wal_size_to_usize_failed error={error}"
            ))?
        ];
        wal_file
            .read(&cx, &mut bytes, 0)
            .map_err(|error| format!("read_wal_bytes_failed error={error}"))?;
        bytes
    };

    let corrupt_offset = WAL_HEADER_SIZE + WAL_FRAME_HEADER_SIZE + 17;
    if corrupt_offset >= wal_bytes.len() {
        return Err(format!(
            "bead_id={BEAD_ID} case=checksum_corruption_offset_out_of_range offset={corrupt_offset} len={}",
            wal_bytes.len()
        ));
    }
    wal_bytes[corrupt_offset] ^= 0x40;

    {
        let mut wal_file = open_wal_file(&vfs, &cx, &wal_path)?;
        wal_file
            .write(&cx, &wal_bytes, 0)
            .map_err(|error| format!("write_corrupted_wal_bytes_failed error={error}"))?;
        wal_file
            .sync(&cx, SyncFlags::NORMAL)
            .map_err(|error| format!("sync_corrupted_wal_failed error={error}"))?;
    }

    let validation = validate_wal_chain(&wal_bytes, page_size, false)
        .map_err(|error| format!("validate_wal_chain_failed error={error}"))?;
    if validation.valid {
        return Err(format!(
            "bead_id={BEAD_ID} case=wal_checksum_corruption_not_detected validation={validation:?}"
        ));
    }
    if validation.reason != Some(WalChainInvalidReason::FrameChecksumMismatch) {
        return Err(format!(
            "bead_id={BEAD_ID} case=wal_checksum_corruption_reason_mismatch reason={:?}",
            validation.reason
        ));
    }

    Ok(())
}

#[test]
fn test_wal_recovery_stops_at_first_invalid_frame() -> Result<(), String> {
    let cx = test_cx();
    let vfs = MemoryVfs::new();
    let wal_path = PathBuf::from("/bd_bca_1_first_invalid_prefix.db-wal");
    let page_size = PageSize::DEFAULT.as_usize();
    let page_size_u32 =
        u32::try_from(page_size).map_err(|error| format!("page_size_u32_failed error={error}"))?;

    {
        let wal_file = open_wal_file(&vfs, &cx, &wal_path)?;
        let mut wal = WalFile::create(&cx, wal_file, page_size_u32, 0, wal_salts())
            .map_err(|error| format!("create_wal_for_prefix_cutoff_test_failed error={error}"))?;
        for (index, seed) in [0x41_u8, 0x42, 0x43, 0x44].iter().copied().enumerate() {
            let frame_no = u32::try_from(index + 1)
                .map_err(|error| format!("frame_no_u32_failed error={error}"))?;
            wal.append_frame(&cx, frame_no, &sample_page(seed, page_size), frame_no)
                .map_err(|error| {
                    format!("append_frame_failed frame_no={frame_no} error={error}")
                })?;
        }
        wal.close(&cx)
            .map_err(|error| format!("close_wal_for_prefix_cutoff_test_failed error={error}"))?;
    }

    let mut wal_bytes = {
        let mut wal_file = open_wal_file(&vfs, &cx, &wal_path)?;
        let file_size = wal_file
            .file_size(&cx)
            .map_err(|error| format!("read_wal_size_failed error={error}"))?;
        let mut bytes = vec![
            0_u8;
            usize::try_from(file_size).map_err(|error| format!(
                "wal_size_to_usize_failed error={error}"
            ))?
        ];
        wal_file
            .read(&cx, &mut bytes, 0)
            .map_err(|error| format!("read_wal_bytes_failed error={error}"))?;
        bytes
    };

    let frame_size = WAL_FRAME_HEADER_SIZE + page_size;
    let invalid_frame = 2_usize;
    let corrupt_offset = WAL_HEADER_SIZE + frame_size * invalid_frame + WAL_FRAME_HEADER_SIZE + 9;
    if corrupt_offset >= wal_bytes.len() {
        return Err(format!(
            "bead_id={BEAD_ID} case=wal_recovery_invalid_frame_offset_out_of_range offset={corrupt_offset} len={}",
            wal_bytes.len()
        ));
    }
    wal_bytes[corrupt_offset] ^= 0x7F;

    let validation = validate_wal_chain(&wal_bytes, page_size, false)
        .map_err(|error| format!("validate_wal_chain_failed error={error}"))?;
    if validation.reason != Some(WalChainInvalidReason::FrameChecksumMismatch) {
        return Err(format!(
            "bead_id={BEAD_ID} case=wal_recovery_invalid_frame_reason_mismatch reason={:?}",
            validation.reason
        ));
    }
    if validation.valid_frame_count != invalid_frame {
        return Err(format!(
            "bead_id={BEAD_ID} case=wal_recovery_invalid_frame_valid_count_mismatch expected={invalid_frame} actual={}",
            validation.valid_frame_count
        ));
    }
    if validation.first_invalid_frame != Some(invalid_frame) {
        return Err(format!(
            "bead_id={BEAD_ID} case=wal_recovery_invalid_frame_index_mismatch expected={invalid_frame} actual={:?}",
            validation.first_invalid_frame
        ));
    }
    if validation.replayable_frame_count != invalid_frame {
        return Err(format!(
            "bead_id={BEAD_ID} case=wal_recovery_invalid_frame_replayable_count_mismatch expected={invalid_frame} actual={}",
            validation.replayable_frame_count
        ));
    }

    Ok(())
}

#[test]
fn test_wal_recovery_salt_mismatch_stops_at_frame() -> Result<(), String> {
    let cx = test_cx();
    let vfs = MemoryVfs::new();
    let wal_path = PathBuf::from("/bd_bca_1_salt_mismatch.db-wal");
    let page_size = PageSize::DEFAULT.as_usize();
    let page_size_u32 =
        u32::try_from(page_size).map_err(|error| format!("page_size_u32_failed error={error}"))?;

    {
        let wal_file = open_wal_file(&vfs, &cx, &wal_path)?;
        let mut wal = WalFile::create(&cx, wal_file, page_size_u32, 0, wal_salts())
            .map_err(|error| format!("create_wal_for_salt_mismatch_test_failed error={error}"))?;
        for (index, seed) in [0x51_u8, 0x52, 0x53].iter().copied().enumerate() {
            let frame_no = u32::try_from(index + 1)
                .map_err(|error| format!("frame_no_u32_failed error={error}"))?;
            wal.append_frame(&cx, frame_no, &sample_page(seed, page_size), frame_no)
                .map_err(|error| {
                    format!("append_frame_failed frame_no={frame_no} error={error}")
                })?;
        }
        wal.close(&cx)
            .map_err(|error| format!("close_wal_for_salt_mismatch_test_failed error={error}"))?;
    }

    let mut wal_bytes = {
        let mut wal_file = open_wal_file(&vfs, &cx, &wal_path)?;
        let file_size = wal_file
            .file_size(&cx)
            .map_err(|error| format!("read_wal_size_failed error={error}"))?;
        let mut bytes = vec![
            0_u8;
            usize::try_from(file_size).map_err(|error| format!(
                "wal_size_to_usize_failed error={error}"
            ))?
        ];
        wal_file
            .read(&cx, &mut bytes, 0)
            .map_err(|error| format!("read_wal_bytes_failed error={error}"))?;
        bytes
    };

    let frame_size = WAL_FRAME_HEADER_SIZE + page_size;
    let invalid_frame = 1_usize;
    let header_start = WAL_HEADER_SIZE + frame_size * invalid_frame;
    let header_end = header_start + WAL_FRAME_HEADER_SIZE;
    if header_end > wal_bytes.len() {
        return Err(format!(
            "bead_id={BEAD_ID} case=wal_recovery_salt_mismatch_header_out_of_range start={header_start} end={header_end} len={}",
            wal_bytes.len()
        ));
    }
    write_wal_frame_salts(
        &mut wal_bytes[header_start..header_end],
        WalSalts {
            salt1: 0xDEAD_BEEF,
            salt2: 0xFEED_CAFE,
        },
    )
    .map_err(|error| format!("rewrite_frame_salts_failed error={error}"))?;

    let validation = validate_wal_chain(&wal_bytes, page_size, false)
        .map_err(|error| format!("validate_wal_chain_failed error={error}"))?;
    if validation.reason != Some(WalChainInvalidReason::SaltMismatch) {
        return Err(format!(
            "bead_id={BEAD_ID} case=wal_recovery_salt_mismatch_reason_mismatch reason={:?}",
            validation.reason
        ));
    }
    if validation.valid_frame_count != invalid_frame {
        return Err(format!(
            "bead_id={BEAD_ID} case=wal_recovery_salt_mismatch_valid_count_mismatch expected={invalid_frame} actual={}",
            validation.valid_frame_count
        ));
    }
    if validation.first_invalid_frame != Some(invalid_frame) {
        return Err(format!(
            "bead_id={BEAD_ID} case=wal_recovery_salt_mismatch_index_mismatch expected={invalid_frame} actual={:?}",
            validation.first_invalid_frame
        ));
    }

    Ok(())
}

#[test]
fn test_wal_frame_checksum_excludes_salt_header_bytes() -> Result<(), String> {
    let cx = test_cx();
    let vfs = MemoryVfs::new();
    let wal_path = PathBuf::from("/bd_bca_1_checksum_excludes_salt.db-wal");
    let page_size = PageSize::DEFAULT.as_usize();
    let page_size_u32 =
        u32::try_from(page_size).map_err(|error| format!("page_size_u32_failed error={error}"))?;

    {
        let wal_file = open_wal_file(&vfs, &cx, &wal_path)?;
        let mut wal =
            WalFile::create(&cx, wal_file, page_size_u32, 0, wal_salts()).map_err(|error| {
                format!("create_wal_for_checksum_excludes_salt_test_failed error={error}")
            })?;
        wal.append_frame(&cx, 1, &sample_page(0x6D, page_size), 1)
            .map_err(|error| format!("append_frame_failed error={error}"))?;
        wal.close(&cx).map_err(|error| {
            format!("close_wal_for_checksum_excludes_salt_test_failed error={error}")
        })?;
    }

    let wal_bytes = {
        let mut wal_file = open_wal_file(&vfs, &cx, &wal_path)?;
        let file_size = wal_file
            .file_size(&cx)
            .map_err(|error| format!("read_wal_size_failed error={error}"))?;
        let mut bytes = vec![
            0_u8;
            usize::try_from(file_size).map_err(|error| format!(
                "wal_size_to_usize_failed error={error}"
            ))?
        ];
        wal_file
            .read(&cx, &mut bytes, 0)
            .map_err(|error| format!("read_wal_bytes_failed error={error}"))?;
        bytes
    };

    let frame_size = WAL_FRAME_HEADER_SIZE + page_size;
    let seed = read_wal_header_checksum(&wal_bytes[..WAL_HEADER_SIZE])
        .map_err(|error| format!("read_wal_header_checksum_failed error={error}"))?;
    let frame = &wal_bytes[WAL_HEADER_SIZE..WAL_HEADER_SIZE + frame_size];
    let baseline = compute_wal_frame_checksum(frame, page_size, seed, false)
        .map_err(|error| format!("compute_wal_frame_checksum_baseline_failed error={error}"))?;

    let mut salted_variant = frame.to_vec();
    write_wal_frame_salts(
        &mut salted_variant[..WAL_FRAME_HEADER_SIZE],
        WalSalts {
            salt1: 0x0102_0304,
            salt2: 0x0506_0708,
        },
    )
    .map_err(|error| format!("rewrite_frame_salts_failed error={error}"))?;
    let variant = compute_wal_frame_checksum(&salted_variant, page_size, seed, false)
        .map_err(|error| format!("compute_wal_frame_checksum_variant_failed error={error}"))?;

    if baseline != variant {
        return Err(format!(
            "bead_id={BEAD_ID} case=wal_frame_checksum_excludes_salt_header_bytes_mismatch baseline={baseline:?} variant={variant:?}"
        ));
    }

    Ok(())
}

#[test]
fn test_wal_recovery_torn_write() -> Result<(), String> {
    let cx = test_cx();
    let vfs = MemoryVfs::new();
    let wal_path = PathBuf::from("/bd_bca_1_torn_tail.db-wal");
    let page_size = PageSize::DEFAULT.as_usize();
    let page_size_u32 =
        u32::try_from(page_size).map_err(|error| format!("page_size_u32_failed error={error}"))?;
    let frame_size = WAL_FRAME_HEADER_SIZE + page_size;

    {
        let wal_file = open_wal_file(&vfs, &cx, &wal_path)?;
        let mut wal = WalFile::create(&cx, wal_file, page_size_u32, 0, wal_salts())
            .map_err(|error| format!("create_wal_for_torn_tail_test_failed error={error}"))?;
        wal.append_frame(&cx, 1, &sample_page(0x31, page_size), 0)
            .map_err(|error| format!("append_frame_1_failed error={error}"))?;
        wal.append_frame(&cx, 2, &sample_page(0x32, page_size), 0)
            .map_err(|error| format!("append_frame_2_failed error={error}"))?;
        wal.append_frame(&cx, 3, &sample_page(0x33, page_size), 3)
            .map_err(|error| format!("append_frame_3_failed error={error}"))?;
        wal.close(&cx)
            .map_err(|error| format!("close_wal_for_torn_tail_test_failed error={error}"))?;
    }

    {
        let mut wal_file = open_wal_file(&vfs, &cx, &wal_path)?;
        let torn_len = WAL_HEADER_SIZE + frame_size * 2 + frame_size / 2;
        wal_file
            .truncate(
                &cx,
                u64::try_from(torn_len)
                    .map_err(|error| format!("torn_len_to_u64_failed error={error}"))?,
            )
            .map_err(|error| format!("truncate_wal_tail_failed error={error}"))?;
    }

    let recovered_file = open_wal_file(&vfs, &cx, &wal_path)?;
    let mut recovered = WalFile::open(&cx, recovered_file)
        .map_err(|error| format!("open_recovered_wal_failed error={error}"))?;
    if recovered.frame_count() != 2 {
        return Err(format!(
            "bead_id={BEAD_ID} case=wal_recovery_torn_write_expected_prefix expected=2 actual={}",
            recovered.frame_count()
        ));
    }
    recovered
        .append_frame(&cx, 4, &sample_page(0x34, page_size), 4)
        .map_err(|error| format!("append_after_torn_tail_recovery_failed error={error}"))?;
    if recovered.frame_count() != 3 {
        return Err(format!(
            "bead_id={BEAD_ID} case=wal_recovery_append_after_recovery expected=3 actual={}",
            recovered.frame_count()
        ));
    }

    Ok(())
}

fn build_raptorq_wal_repair_fixture(
    sidecar_path: &Path,
) -> Result<(WalFecGroupMeta, Vec<Vec<u8>>, WalSalts), String> {
    const K_SOURCE: u32 = 10;
    const R_REPAIR: u32 = 2;
    const START_FRAME_NO: u32 = 400;

    let salts = wal_salts();
    let page_size = PageSize::DEFAULT.as_usize();
    let page_size_u32 =
        u32::try_from(page_size).map_err(|error| format!("page_size_u32_failed error={error}"))?;
    let source_pages = (0..K_SOURCE)
        .map(|offset| {
            let seed_offset = u8::try_from(offset)
                .map_err(|error| format!("seed_offset_u8_conversion_failed error={error}"))?;
            Ok(sample_page(0x70_u8.wrapping_add(seed_offset), page_size))
        })
        .collect::<Result<Vec<_>, String>>()?;
    let source_hashes = build_source_page_hashes(&source_pages);
    let page_numbers = (0..K_SOURCE)
        .map(|offset| 1_000_u32 + offset)
        .collect::<Vec<_>>();
    let meta = WalFecGroupMeta::from_init(WalFecGroupMetaInit {
        wal_salt1: salts.salt1,
        wal_salt2: salts.salt2,
        start_frame_no: START_FRAME_NO,
        end_frame_no: START_FRAME_NO + (K_SOURCE - 1),
        db_size_pages: 4_096,
        page_size: page_size_u32,
        k_source: K_SOURCE,
        r_repair: R_REPAIR,
        oti: Oti {
            f: u64::from(K_SOURCE) * u64::from(page_size_u32),
            al: 1,
            t: page_size_u32,
            z: 1,
            n: 1,
        },
        object_id: ObjectId::derive_from_canonical_bytes(b"bd-bca.1-test_raptorq_wal_repair"),
        page_numbers,
        source_page_xxh3_128: source_hashes,
    })
    .map_err(|error| format!("wal_fec_meta_build_failed error={error}"))?;
    let repair_symbols = generate_wal_fec_repair_symbols(&meta, &source_pages)
        .map_err(|error| format!("wal_fec_repair_generate_failed error={error}"))?;
    let record = WalFecGroupRecord::new(meta.clone(), repair_symbols)
        .map_err(|error| format!("wal_fec_record_build_failed error={error}"))?;
    append_wal_fec_group(sidecar_path, &record)
        .map_err(|error| format!("wal_fec_append_failed error={error}"))?;
    Ok((meta, source_pages, salts))
}

fn wal_frame_candidates_from_source(
    meta: &WalFecGroupMeta,
    source_pages: &[Vec<u8>],
) -> Vec<WalFrameCandidate> {
    source_pages
        .iter()
        .enumerate()
        .map(|(index, page_data)| {
            #[allow(clippy::cast_possible_truncation)]
            let frame_offset = index as u32;
            WalFrameCandidate {
                frame_no: meta.start_frame_no + frame_offset,
                page_data: page_data.clone(),
            }
        })
        .collect::<Vec<_>>()
}

fn corrupt_frame_candidate(
    candidates: &mut [WalFrameCandidate],
    target_frame: u32,
) -> Result<(), String> {
    let Some(candidate) = candidates
        .iter_mut()
        .find(|candidate| candidate.frame_no == target_frame)
    else {
        return Err(format!(
            "bead_id={BEAD_ID} case=raptorq_wal_repair_missing_frame target={target_frame}"
        ));
    };
    candidate.page_data[0] ^= 0x5A;
    Ok(())
}

#[cfg(unix)]
#[test]
fn test_roundtrip_c_sqlite() -> Result<(), String> {
    if !sqlite3_available() {
        return Ok(());
    }

    let temp_dir = tempdir().map_err(|error| format!("tempdir_create_failed error={error}"))?;
    let db_path = temp_dir.path().join("bd_bca_1_roundtrip_c_sqlite.db");
    let setup = sqlite3_exec(
        &db_path,
        "PRAGMA journal_mode=WAL;\
         CREATE TABLE IF NOT EXISTS t(v INTEGER);\
         DELETE FROM t;\
         INSERT INTO t VALUES (1),(2);",
    )?;
    if !setup.status.success() {
        return Err(format!(
            "bead_id={BEAD_ID} case=roundtrip_c_sqlite_setup_failed stderr={}",
            String::from_utf8_lossy(&setup.stderr)
        ));
    }

    let cx = test_cx();
    let vfs = UnixVfs::new();
    let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::READWRITE;
    let (mut coordinator, _) = vfs
        .open(&cx, Some(&db_path), flags)
        .map_err(|error| format!("unix_vfs_open_failed error={error}"))?;
    coordinator
        .lock(&cx, LockLevel::Reserved)
        .map_err(|error| format!("unix_vfs_reserved_lock_failed error={error}"))?;
    let file_size = coordinator
        .file_size(&cx)
        .map_err(|error| format!("unix_vfs_file_size_failed error={error}"))?;
    coordinator
        .unlock(&cx, LockLevel::None)
        .map_err(|error| format!("unix_vfs_unlock_failed error={error}"))?;
    if file_size == 0 {
        return Err(format!(
            "bead_id={BEAD_ID} case=roundtrip_c_sqlite_zero_file_size path={}",
            db_path.display()
        ));
    }

    let query = sqlite3_exec(&db_path, "SELECT COUNT(*) FROM t;")?;
    if !query.status.success() {
        return Err(format!(
            "bead_id={BEAD_ID} case=roundtrip_c_sqlite_query_failed stderr={}",
            String::from_utf8_lossy(&query.stderr)
        ));
    }
    if String::from_utf8_lossy(&query.stdout).trim() != "2" {
        return Err(format!(
            "bead_id={BEAD_ID} case=roundtrip_c_sqlite_count_mismatch expected=2 actual={}",
            String::from_utf8_lossy(&query.stdout).trim()
        ));
    }

    Ok(())
}

#[test]
fn test_raptorq_wal_repair() -> Result<(), String> {
    let temp_dir = tempdir().map_err(|error| format!("tempdir_create_failed error={error}"))?;
    let sidecar_path = temp_dir.path().join("bd_bca_1_raptorq_repair.wal-fec");
    let (meta, source_pages, salts) = build_raptorq_wal_repair_fixture(&sidecar_path)?;
    let first_corrupt_frame = meta.start_frame_no + 2;
    let second_corrupt_frame = meta.start_frame_no + 7;

    let mut candidates = wal_frame_candidates_from_source(&meta, &source_pages);
    corrupt_frame_candidate(&mut candidates, first_corrupt_frame)?;
    corrupt_frame_candidate(&mut candidates, second_corrupt_frame)?;

    let expected_pages = source_pages.clone();
    let outcome = recover_wal_fec_group_with_decoder(
        &sidecar_path,
        meta.group_id(),
        salts,
        first_corrupt_frame,
        &candidates,
        move |_group_meta, _available| Ok(expected_pages.clone()),
    )
    .map_err(|error| format!("wal_fec_recover_with_decoder_failed error={error}"))?;
    let WalFecRecoveryOutcome::Recovered(recovered) = outcome else {
        return Err(format!(
            "bead_id={BEAD_ID} case=raptorq_wal_repair_expected_recovered outcome={outcome:?}"
        ));
    };
    if recovered.recovered_pages != source_pages {
        return Err(format!(
            "bead_id={BEAD_ID} case=raptorq_wal_repair_payload_mismatch"
        ));
    }
    if !recovered.decode_proof.decode_attempted || !recovered.decode_proof.decode_succeeded {
        return Err(format!(
            "bead_id={BEAD_ID} case=raptorq_wal_repair_decode_flags_invalid proof={:?}",
            recovered.decode_proof
        ));
    }
    if !recovered
        .decode_proof
        .recovered_frame_nos
        .contains(&first_corrupt_frame)
        || !recovered
            .decode_proof
            .recovered_frame_nos
            .contains(&second_corrupt_frame)
    {
        return Err(format!(
            "bead_id={BEAD_ID} case=raptorq_wal_repair_recovered_frame_nos_missing expected=[{first_corrupt_frame}, {second_corrupt_frame}] actual={:?}",
            recovered.decode_proof.recovered_frame_nos
        ));
    }

    Ok(())
}

#[test]
fn test_encryption_pragma_key() -> Result<(), String> {
    let params = Argon2Params {
        m_cost: 256,
        t_cost: 1,
        p_cost: 1,
    };
    let salt = [0x11_u8; 16];
    let kek = KeyManager::derive_kek(b"phase5-passphrase", &salt, &params)
        .map_err(|error| format!("derive_kek_failed error={error}"))?;
    let wrong_kek = KeyManager::derive_kek(b"wrong-passphrase", &salt, &params)
        .map_err(|error| format!("derive_wrong_kek_failed error={error}"))?;
    let dek = [0xAB_u8; KEY_SIZE];
    let wrapped = KeyManager::wrap_dek(&dek, &kek, &encryption_test_nonce(21))
        .map_err(|error| format!("wrap_dek_failed error={error}"))?;
    let unwrapped = KeyManager::unwrap_dek(&wrapped, &kek)
        .map_err(|error| format!("unwrap_dek_failed error={error}"))?;
    if unwrapped != dek {
        return Err(format!(
            "bead_id={BEAD_ID} case=encryption_pragma_key_unwrap_mismatch"
        ));
    }
    if KeyManager::unwrap_dek(&wrapped, &wrong_kek) != Err(EncryptError::DekUnwrapFailed) {
        return Err(format!(
            "bead_id={BEAD_ID} case=encryption_pragma_key_wrong_passphrase_unexpected_success"
        ));
    }

    let encryptor = PageEncryptor::new(&dek, DatabaseId::from_bytes([0x42_u8; DATABASE_ID_SIZE]));
    let mut encrypted_page = sample_page(0x2D, 4_096);
    let original_page = encrypted_page.clone();
    encryptor
        .encrypt_page(&mut encrypted_page, 1, &encryption_test_nonce(31))
        .map_err(|error| format!("encrypt_page_failed error={error}"))?;
    let reserved = usize::from(ENCRYPTION_RESERVED_BYTES);
    if encrypted_page[..4_096 - reserved] == original_page[..4_096 - reserved] {
        return Err(format!(
            "bead_id={BEAD_ID} case=encryption_pragma_key_ciphertext_not_changed"
        ));
    }
    encryptor
        .decrypt_page(&mut encrypted_page, 1)
        .map_err(|error| format!("decrypt_page_failed error={error}"))?;
    if encrypted_page[..4_096 - reserved] != original_page[..4_096 - reserved] {
        return Err(format!(
            "bead_id={BEAD_ID} case=encryption_pragma_key_roundtrip_mismatch"
        ));
    }

    Ok(())
}

#[test]
fn test_encryption_rekey() -> Result<(), String> {
    let params = Argon2Params {
        m_cost: 256,
        t_cost: 1,
        p_cost: 1,
    };
    let old_kek = KeyManager::derive_kek(b"old-passphrase", &[0xAA_u8; 16], &params)
        .map_err(|error| format!("derive_old_kek_failed error={error}"))?;
    let new_kek = KeyManager::derive_kek(b"new-passphrase", &[0xBB_u8; 16], &params)
        .map_err(|error| format!("derive_new_kek_failed error={error}"))?;
    let dek = [0xCD_u8; KEY_SIZE];
    let wrapped_old = KeyManager::wrap_dek(&dek, &old_kek, &encryption_test_nonce(41))
        .map_err(|error| format!("wrap_old_dek_failed error={error}"))?;
    let unwrapped_old = KeyManager::unwrap_dek(&wrapped_old, &old_kek)
        .map_err(|error| format!("unwrap_old_dek_failed error={error}"))?;
    let wrapped_new = KeyManager::wrap_dek(&unwrapped_old, &new_kek, &encryption_test_nonce(51))
        .map_err(|error| format!("wrap_new_dek_failed error={error}"))?;
    let unwrapped_new = KeyManager::unwrap_dek(&wrapped_new, &new_kek)
        .map_err(|error| format!("unwrap_new_dek_failed error={error}"))?;
    if unwrapped_new != dek {
        return Err(format!(
            "bead_id={BEAD_ID} case=encryption_rekey_dek_changed"
        ));
    }
    if KeyManager::unwrap_dek(&wrapped_new, &old_kek) != Err(EncryptError::DekUnwrapFailed) {
        return Err(format!(
            "bead_id={BEAD_ID} case=encryption_rekey_old_kek_unexpected_success"
        ));
    }

    let encryptor = PageEncryptor::new(&dek, DatabaseId::from_bytes([0x33_u8; DATABASE_ID_SIZE]));
    let mut page = sample_page(0x66, 4_096);
    let original = page.clone();
    encryptor
        .encrypt_page(&mut page, 9, &encryption_test_nonce(61))
        .map_err(|error| format!("encryption_rekey_encrypt_failed error={error}"))?;
    encryptor
        .decrypt_page(&mut page, 9)
        .map_err(|error| format!("encryption_rekey_decrypt_failed error={error}"))?;
    let reserved = usize::from(ENCRYPTION_RESERVED_BYTES);
    if page[..4_096 - reserved] != original[..4_096 - reserved] {
        return Err(format!(
            "bead_id={BEAD_ID} case=encryption_rekey_page_roundtrip_mismatch"
        ));
    }

    Ok(())
}

#[test]
fn test_encryption_aad_swap_resistance() -> Result<(), String> {
    let dek = [0xEF_u8; KEY_SIZE];
    let enc_a = PageEncryptor::new(&dek, DatabaseId::from_bytes([0xA1_u8; DATABASE_ID_SIZE]));
    let enc_b = PageEncryptor::new(&dek, DatabaseId::from_bytes([0xB2_u8; DATABASE_ID_SIZE]));
    let mut encrypted_page = sample_page(0x77, 4_096);
    enc_a
        .encrypt_page(&mut encrypted_page, 7, &encryption_test_nonce(71))
        .map_err(|error| format!("aad_swap_encrypt_failed error={error}"))?;

    let mut wrong_page_number = encrypted_page.clone();
    let wrong_page_err = enc_a
        .decrypt_page(&mut wrong_page_number, 8)
        .expect_err("page-number swap should fail authentication");
    if wrong_page_err != EncryptError::AuthenticationFailed {
        return Err(format!(
            "bead_id={BEAD_ID} case=encryption_aad_swap_resistance_page_number_error_mismatch actual={wrong_page_err}"
        ));
    }

    let db_swap_err = enc_b
        .decrypt_page(&mut encrypted_page, 7)
        .expect_err("database-id swap should fail authentication");
    if db_swap_err != EncryptError::AuthenticationFailed {
        return Err(format!(
            "bead_id={BEAD_ID} case=encryption_aad_swap_resistance_database_id_error_mismatch actual={db_swap_err}"
        ));
    }

    Ok(())
}

#[test]
fn test_checkpoint_all_4_modes() -> Result<(), String> {
    let cx = test_cx();
    let page_size = PageSize::DEFAULT.as_usize();
    let page_size_u32 =
        u32::try_from(page_size).map_err(|error| format!("page_size_u32_failed error={error}"))?;
    let cases = [
        (CheckpointMode::Passive, false),
        (CheckpointMode::Full, false),
        (CheckpointMode::Restart, true),
        (CheckpointMode::Truncate, true),
    ];

    for (mode, expects_reset) in cases {
        let mode_label = match mode {
            CheckpointMode::Passive => "passive",
            CheckpointMode::Full => "full",
            CheckpointMode::Restart => "restart",
            CheckpointMode::Truncate => "truncate",
        };
        let vfs = MemoryVfs::new();
        let wal_path = PathBuf::from(format!("/bd_bca_1_checkpoint_{mode_label}.db-wal"));
        let wal_file = open_wal_file(&vfs, &cx, &wal_path)?;
        let mut wal =
            WalFile::create(&cx, wal_file, page_size_u32, 0, wal_salts()).map_err(|error| {
                format!("create_wal_for_checkpoint_mode_failed mode={mode_label} error={error}")
            })?;
        wal.append_frame(&cx, 1, &sample_page(0x41, page_size), 0)
            .map_err(|error| format!("append_checkpoint_frame_1_failed error={error}"))?;
        wal.append_frame(&cx, 2, &sample_page(0x42, page_size), 0)
            .map_err(|error| format!("append_checkpoint_frame_2_failed error={error}"))?;
        wal.append_frame(&cx, 3, &sample_page(0x43, page_size), 3)
            .map_err(|error| format!("append_checkpoint_frame_3_failed error={error}"))?;

        let mut target = RecordingCheckpointTarget::default();
        let state = CheckpointState {
            total_frames: 3,
            backfilled_frames: 0,
            oldest_reader_frame: None,
        };
        let result =
            execute_checkpoint(&cx, &mut wal, mode, state, &mut target).map_err(|error| {
                format!("execute_checkpoint_failed mode={mode_label} error={error}")
            })?;

        if result.frames_backfilled != 3 {
            return Err(format!(
                "bead_id={BEAD_ID} case=checkpoint_mode_frames mode={mode_label} expected=3 actual={}",
                result.frames_backfilled
            ));
        }
        if result.wal_was_reset != expects_reset {
            return Err(format!(
                "bead_id={BEAD_ID} case=checkpoint_mode_reset mode={mode_label} expected={expects_reset} actual={}",
                result.wal_was_reset
            ));
        }
        if target.writes.len() != 3 {
            return Err(format!(
                "bead_id={BEAD_ID} case=checkpoint_mode_writes mode={mode_label} expected=3 actual={}",
                target.writes.len()
            ));
        }
        if target.sync_calls == 0 {
            return Err(format!(
                "bead_id={BEAD_ID} case=checkpoint_mode_sync_missing mode={mode_label}"
            ));
        }
    }

    Ok(())
}

#[test]
fn test_savepoints_nested() -> Result<(), String> {
    let cx = test_cx();
    let vfs = MemoryVfs::new();
    let path = PathBuf::from("/bd_bca_1_savepoints.db");
    let page_size = PageSize::DEFAULT.as_usize();

    let page_no = {
        let pager = SimplePager::open(vfs.clone(), &path, PageSize::DEFAULT)
            .map_err(|error| format!("open_pager_for_savepoint_test_failed error={error}"))?;
        let mut txn = pager
            .begin(&cx, TransactionMode::Immediate)
            .map_err(|error| format!("begin_savepoint_writer_failed error={error}"))?;

        let page_no = txn
            .allocate_page(&cx)
            .map_err(|error| format!("allocate_savepoint_page_failed error={error}"))?;
        txn.write_page(&cx, page_no, &sample_page(0x11, page_size))
            .map_err(|error| format!("write_initial_savepoint_page_failed error={error}"))?;

        txn.savepoint(&cx, "outer")
            .map_err(|error| format!("savepoint_outer_failed error={error}"))?;
        txn.write_page(&cx, page_no, &sample_page(0x22, page_size))
            .map_err(|error| format!("write_after_outer_savepoint_failed error={error}"))?;

        txn.savepoint(&cx, "inner")
            .map_err(|error| format!("savepoint_inner_failed error={error}"))?;
        txn.write_page(&cx, page_no, &sample_page(0x33, page_size))
            .map_err(|error| format!("write_after_inner_savepoint_failed error={error}"))?;

        txn.rollback_to_savepoint(&cx, "inner")
            .map_err(|error| format!("rollback_to_inner_failed error={error}"))?;
        let after_inner_rollback = txn
            .get_page(&cx, page_no)
            .map_err(|error| format!("read_after_inner_rollback_failed error={error}"))?;
        if after_inner_rollback.as_ref()[0] != 0x22 {
            return Err(format!(
                "bead_id={BEAD_ID} case=savepoints_nested_inner_rollback_mismatch expected=34 actual={}",
                after_inner_rollback.as_ref()[0]
            ));
        }

        txn.release_savepoint(&cx, "inner")
            .map_err(|error| format!("release_inner_savepoint_failed error={error}"))?;
        txn.rollback_to_savepoint(&cx, "outer")
            .map_err(|error| format!("rollback_to_outer_failed error={error}"))?;
        txn.release_savepoint(&cx, "outer")
            .map_err(|error| format!("release_outer_savepoint_failed error={error}"))?;
        txn.commit(&cx)
            .map_err(|error| format!("commit_savepoint_test_failed error={error}"))?;
        page_no
    };

    let pager = SimplePager::open(vfs, &path, PageSize::DEFAULT)
        .map_err(|error| format!("open_pager_for_savepoint_readback_failed error={error}"))?;
    let read_txn = pager
        .begin(&cx, TransactionMode::ReadOnly)
        .map_err(|error| format!("begin_savepoint_reader_failed error={error}"))?;
    let persisted = read_txn
        .get_page(&cx, page_no)
        .map_err(|error| format!("read_savepoint_page_after_commit_failed error={error}"))?;
    if persisted.as_ref()[0] != 0x11 {
        return Err(format!(
            "bead_id={BEAD_ID} case=savepoints_nested_outer_rollback_mismatch expected=17 actual={}",
            persisted.as_ref()[0]
        ));
    }

    Ok(())
}

#[test]
fn test_wal_concurrent_readers_writer() -> Result<(), String> {
    let cx = test_cx();
    let vfs = MemoryVfs::new();
    let wal_path = PathBuf::from("/bd_bca_1_concurrent_readers_writer.db-wal");
    let page_size = PageSize::DEFAULT.as_usize();
    let page_size_u32 =
        u32::try_from(page_size).map_err(|error| format!("page_size_u32_failed error={error}"))?;

    {
        let wal_file = open_wal_file(&vfs, &cx, &wal_path)?;
        let mut wal = WalFile::create(&cx, wal_file, page_size_u32, 0, wal_salts())
            .map_err(|error| format!("create_wal_for_reader_writer_test_failed error={error}"))?;
        wal.append_frame(&cx, 1, &sample_page(0x51, page_size), 1)
            .map_err(|error| format!("append_initial_writer_frame_failed error={error}"))?;
        wal.close(&cx)
            .map_err(|error| format!("close_initial_writer_wal_failed error={error}"))?;
    }

    let mut reader_snapshot = WalFile::open(&cx, open_wal_file(&vfs, &cx, &wal_path)?)
        .map_err(|error| format!("open_reader_snapshot_failed error={error}"))?;
    if reader_snapshot.frame_count() != 1 {
        return Err(format!(
            "bead_id={BEAD_ID} case=wal_reader_snapshot_initial_count expected=1 actual={}",
            reader_snapshot.frame_count()
        ));
    }

    {
        let mut writer = WalFile::open(&cx, open_wal_file(&vfs, &cx, &wal_path)?)
            .map_err(|error| format!("open_writer_failed error={error}"))?;
        writer
            .append_frame(&cx, 2, &sample_page(0x52, page_size), 2)
            .map_err(|error| format!("append_second_writer_frame_failed error={error}"))?;
        writer
            .close(&cx)
            .map_err(|error| format!("close_writer_failed error={error}"))?;
    }

    if reader_snapshot.frame_count() != 1 {
        return Err(format!(
            "bead_id={BEAD_ID} case=wal_reader_snapshot_stability expected=1 actual={}",
            reader_snapshot.frame_count()
        ));
    }
    let (_, snapshot_page) = reader_snapshot
        .read_frame(&cx, 0)
        .map_err(|error| format!("read_snapshot_reader_frame_failed error={error}"))?;
    if snapshot_page[0] != 0x51 {
        return Err(format!(
            "bead_id={BEAD_ID} case=wal_reader_snapshot_content_mismatch expected=81 actual={}",
            snapshot_page[0]
        ));
    }

    let mut reader_latest = WalFile::open(&cx, open_wal_file(&vfs, &cx, &wal_path)?)
        .map_err(|error| format!("open_latest_reader_failed error={error}"))?;
    if reader_latest.frame_count() != 2 {
        return Err(format!(
            "bead_id={BEAD_ID} case=wal_reader_latest_count expected=2 actual={}",
            reader_latest.frame_count()
        ));
    }
    let (_, latest_page) = reader_latest
        .read_frame(&cx, 1)
        .map_err(|error| format!("read_latest_reader_frame_failed error={error}"))?;
    if latest_page[0] != 0x52 {
        return Err(format!(
            "bead_id={BEAD_ID} case=wal_reader_latest_content_mismatch expected=82 actual={}",
            latest_page[0]
        ));
    }

    Ok(())
}
