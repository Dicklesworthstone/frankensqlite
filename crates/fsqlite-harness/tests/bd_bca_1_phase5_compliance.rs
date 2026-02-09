use std::fs;
use std::path::{Path, PathBuf};

use fsqlite_pager::{
    JournalHeader, JournalPageRecord, MvccPager, SimplePager, TransactionHandle, TransactionMode,
};
use fsqlite_types::PageSize;
use fsqlite_types::cx::Cx;
use fsqlite_types::flags::{AccessFlags, SyncFlags, VfsOpenFlags};
use fsqlite_vfs::MemoryVfs;
use fsqlite_vfs::traits::{Vfs, VfsFile};
use fsqlite_wal::{
    CheckpointMode, CheckpointState, CheckpointTarget, WAL_FRAME_HEADER_SIZE, WAL_HEADER_SIZE,
    WalChainInvalidReason, WalFile, WalSalts, execute_checkpoint, validate_wal_chain,
};
use proptest::prelude::proptest;
use proptest::test_runner::TestCaseError;
use serde_json::Value;

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
        .any(|token| token == expected_marker)
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

    let pager = SimplePager::open(vfs.clone(), &path, PageSize::DEFAULT)
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
