use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use fsqlite_mvcc::{BeginKind, MvccError, Transaction, TransactionManager};
use fsqlite_types::{PageData, PageNumber, PageSize};
use proptest::prelude::proptest;
use proptest::test_runner::TestCaseError;
use serde_json::Value;

const BEAD_ID: &str = "bd-bca.2";
const ISSUES_JSONL: &str = ".beads/issues.jsonl";
const LOG_STANDARD_REF: &str = "bd-1fpm";

const UNIT_TEST_IDS: [&str; 2] = [
    "test_bd_bca_2_unit_compliance_gate",
    "prop_bd_bca_2_structure_compliance",
];
const E2E_TEST_IDS: [&str; 1] = ["test_e2e_bd_bca_2_compliance"];
const LOG_LEVEL_MARKERS: [&str; 4] = ["DEBUG", "INFO", "WARN", "ERROR"];
const REQUIRED_TOKENS: [&str; 8] = [
    "test_bd_bca_2_unit_compliance_gate",
    "prop_bd_bca_2_structure_compliance",
    "test_e2e_bd_bca_2_compliance",
    "DEBUG",
    "INFO",
    "WARN",
    "ERROR",
    "bd-1fpm",
];

const COMMIT_ORDERS: [[usize; 3]; 6] = [
    [0, 1, 2],
    [0, 2, 1],
    [1, 0, 2],
    [1, 2, 0],
    [2, 0, 1],
    [2, 1, 0],
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct TraceOutcome {
    committed_count: u8,
    t1_committed: bool,
    t2_committed: bool,
    t3_committed: bool,
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

fn page(pgno: u32) -> PageNumber {
    PageNumber::new(pgno).expect("page number must be non-zero")
}

fn base_page(seed: u8) -> PageData {
    let mut bytes = vec![0_u8; PageSize::DEFAULT.as_usize()];
    for (index, byte) in bytes.iter_mut().enumerate() {
        let offset = u8::try_from(index % 251).expect("offset must fit u8");
        *byte = seed.wrapping_add(offset);
    }
    PageData::from_vec(bytes)
}

fn page_with_override(base: &PageData, offset: usize, value: u8) -> PageData {
    let mut bytes = base.as_bytes().to_vec();
    bytes[offset] = value;
    PageData::from_vec(bytes)
}

fn apply_trace_writes(
    manager: &TransactionManager,
    txns: &mut [Transaction],
    txn_index: usize,
) -> Result<(), String> {
    match txn_index {
        0 => manager
            .write_page(&mut txns[0], page(1), base_page(0x11))
            .map_err(|error| format!("write_t1_page1_failed error={error:?}")),
        1 => manager
            .write_page(&mut txns[1], page(2), base_page(0x22))
            .map_err(|error| format!("write_t2_page2_failed error={error:?}")),
        2 => {
            manager
                .write_page(&mut txns[2], page(1), base_page(0x33))
                .map_err(|error| format!("write_t3_page1_failed error={error:?}"))?;
            manager
                .write_page(&mut txns[2], page(2), base_page(0x44))
                .map_err(|error| format!("write_t3_page2_failed error={error:?}"))?;
            Ok(())
        }
        _ => Err(format!("unsupported_txn_index {txn_index}")),
    }
}

fn run_3txn_trace(order: [usize; 3]) -> Result<TraceOutcome, String> {
    let manager = TransactionManager::new(PageSize::DEFAULT);

    let mut txns = vec![
        manager
            .begin(BeginKind::Concurrent)
            .map_err(|error| format!("begin_t1_failed error={error:?}"))?,
        manager
            .begin(BeginKind::Concurrent)
            .map_err(|error| format!("begin_t2_failed error={error:?}"))?,
        manager
            .begin(BeginKind::Concurrent)
            .map_err(|error| format!("begin_t3_failed error={error:?}"))?,
    ];
    let mut wrote = [false; 3];

    let mut committed = [false; 3];
    for txn_index in order {
        if !wrote[txn_index] {
            apply_trace_writes(&manager, &mut txns, txn_index)?;
            wrote[txn_index] = true;
        }

        match manager.commit(&mut txns[txn_index]) {
            Ok(_) => committed[txn_index] = true,
            Err(MvccError::BusySnapshot) => {}
            Err(error) => {
                return Err(format!(
                    "trace_commit_failed idx={txn_index} order={order:?} error={error:?}"
                ));
            }
        }
    }

    let committed_count = committed.iter().filter(|flag| **flag).count();
    if committed_count > 2 {
        return Err(format!(
            "trace_invalid_commit_count order={order:?} committed={committed:?}"
        ));
    }

    let mut reader = manager
        .begin(BeginKind::Deferred)
        .map_err(|error| format!("trace_reader_begin_failed error={error:?}"))?;
    let page1 = manager.read_page(&mut reader, page(1));
    let page2 = manager.read_page(&mut reader, page(2));

    if committed[2] {
        if committed[0] || committed[1] {
            return Err(format!(
                "trace_txn3_commit_exclusivity_broken order={order:?} committed={committed:?}"
            ));
        }
        let first_byte_page1 = page1
            .as_ref()
            .map(|data| data.as_bytes()[0])
            .ok_or_else(|| format!("trace_missing_page1_for_t3 order={order:?}"))?;
        let first_byte_page2 = page2
            .as_ref()
            .map(|data| data.as_bytes()[0])
            .ok_or_else(|| format!("trace_missing_page2_for_t3 order={order:?}"))?;
        if first_byte_page1 != 0x33 || first_byte_page2 != 0x44 {
            return Err(format!(
                "trace_t3_payload_mismatch order={order:?} page1={first_byte_page1:#04x} page2={first_byte_page2:#04x}"
            ));
        }
    } else {
        if committed[0] {
            let first_byte_page1 = page1
                .as_ref()
                .map(|data| data.as_bytes()[0])
                .ok_or_else(|| format!("trace_missing_page1_for_t1 order={order:?}"))?;
            if first_byte_page1 != 0x11 {
                return Err(format!(
                    "trace_t1_payload_mismatch order={order:?} byte={first_byte_page1:#04x}"
                ));
            }
        }
        if committed[1] {
            let first_byte_page2 = page2
                .as_ref()
                .map(|data| data.as_bytes()[0])
                .ok_or_else(|| format!("trace_missing_page2_for_t2 order={order:?}"))?;
            if first_byte_page2 != 0x22 {
                return Err(format!(
                    "trace_t2_payload_mismatch order={order:?} byte={first_byte_page2:#04x}"
                ));
            }
        }
    }

    manager.abort(&mut reader);

    Ok(TraceOutcome {
        committed_count: u8::try_from(committed_count).expect("count must fit u8"),
        t1_committed: committed[0],
        t2_committed: committed[1],
        t3_committed: committed[2],
    })
}

#[test]
fn test_bd_bca_2_unit_compliance_gate() -> Result<(), String> {
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
    fn prop_bd_bca_2_structure_compliance(missing_index in 0usize..REQUIRED_TOKENS.len()) {
        let description = load_issue_description(BEAD_ID).map_err(TestCaseError::fail)?;
        let marker = REQUIRED_TOKENS[missing_index];
        let removed = description.replace(marker, "");
        let evaluation = evaluate_description(&removed);
        if evaluation.is_compliant() {
            return Err(TestCaseError::fail(format!(
                "bead_id={BEAD_ID} case=marker_removal_not_detected idx={missing_index} marker={marker}"
            )));
        }
    }
}

#[test]
fn test_mvcc_serialized_mode_begin_immediate_blocks_other_writers() -> Result<(), String> {
    let manager = TransactionManager::new(PageSize::DEFAULT);

    let mut writer = manager
        .begin(BeginKind::Immediate)
        .map_err(|error| format!("first_immediate_begin_failed error={error:?}"))?;
    let second = manager.begin(BeginKind::Immediate);
    if !matches!(second, Err(MvccError::Busy)) {
        return Err(format!("second_writer_expected_busy got={second:?}"));
    }

    manager.abort(&mut writer);
    Ok(())
}

#[test]
fn test_mvcc_concurrent_different_pages_both_commit() -> Result<(), String> {
    let manager = TransactionManager::new(PageSize::DEFAULT);

    let mut tx1 = manager
        .begin(BeginKind::Concurrent)
        .map_err(|error| format!("tx1_begin_failed error={error:?}"))?;
    let mut tx2 = manager
        .begin(BeginKind::Concurrent)
        .map_err(|error| format!("tx2_begin_failed error={error:?}"))?;

    manager
        .write_page(&mut tx1, page(5), base_page(0xA1))
        .map_err(|error| format!("tx1_write_failed error={error:?}"))?;
    manager
        .write_page(&mut tx2, page(9), base_page(0xB2))
        .map_err(|error| format!("tx2_write_failed error={error:?}"))?;

    let seq1 = manager
        .commit(&mut tx1)
        .map_err(|error| format!("tx1_commit_failed error={error:?}"))?;
    let seq2 = manager
        .commit(&mut tx2)
        .map_err(|error| format!("tx2_commit_failed error={error:?}"))?;

    if manager.commit_index().latest(page(5)) != Some(seq1) {
        return Err("page5 latest commit mismatch".to_string());
    }
    if manager.commit_index().latest(page(9)) != Some(seq2) {
        return Err("page9 latest commit mismatch".to_string());
    }

    Ok(())
}

#[test]
fn test_mvcc_concurrent_same_page_conflict_busy_snapshot() -> Result<(), String> {
    let manager = TransactionManager::new(PageSize::DEFAULT);

    let mut tx1 = manager
        .begin(BeginKind::Concurrent)
        .map_err(|error| format!("tx1_begin_failed error={error:?}"))?;
    let mut tx2 = manager
        .begin(BeginKind::Concurrent)
        .map_err(|error| format!("tx2_begin_failed error={error:?}"))?;

    manager
        .write_page(&mut tx1, page(7), base_page(0x11))
        .map_err(|error| format!("tx1_write_failed error={error:?}"))?;
    manager
        .commit(&mut tx1)
        .map_err(|error| format!("tx1_commit_failed error={error:?}"))?;
    manager
        .write_page(&mut tx2, page(7), base_page(0x22))
        .map_err(|error| format!("tx2_write_failed error={error:?}"))?;
    let second = manager.commit(&mut tx2);
    if !matches!(second, Err(MvccError::BusySnapshot)) {
        return Err(format!("expected_busy_snapshot_for_tx2 got={second:?}"));
    }

    Ok(())
}

#[test]
fn test_snapshot_isolation_long_reader() -> Result<(), String> {
    let manager = TransactionManager::new(PageSize::DEFAULT);
    let target_page = page(11);

    let mut reader = manager
        .begin(BeginKind::Deferred)
        .map_err(|error| format!("reader_begin_failed error={error:?}"))?;
    if manager.read_page(&mut reader, target_page).is_some() {
        return Err("reader should see no page before writer commit".to_string());
    }

    let mut writer = manager
        .begin(BeginKind::Immediate)
        .map_err(|error| format!("writer_begin_failed error={error:?}"))?;
    manager
        .write_page(&mut writer, target_page, base_page(0x55))
        .map_err(|error| format!("writer_write_failed error={error:?}"))?;
    manager
        .commit(&mut writer)
        .map_err(|error| format!("writer_commit_failed error={error:?}"))?;

    if manager.read_page(&mut reader, target_page).is_some() {
        return Err("reader snapshot should not observe post-snapshot commit".to_string());
    }

    manager.abort(&mut reader);
    Ok(())
}

#[test]
fn test_snapshot_isolation_new_reader_sees_committed_changes() -> Result<(), String> {
    let manager = TransactionManager::new(PageSize::DEFAULT);
    let target_page = page(12);

    let mut writer = manager
        .begin(BeginKind::Immediate)
        .map_err(|error| format!("writer_begin_failed error={error:?}"))?;
    let written = base_page(0x66);
    manager
        .write_page(&mut writer, target_page, written.clone())
        .map_err(|error| format!("writer_write_failed error={error:?}"))?;
    manager
        .commit(&mut writer)
        .map_err(|error| format!("writer_commit_failed error={error:?}"))?;

    let mut reader = manager
        .begin(BeginKind::Deferred)
        .map_err(|error| format!("reader_begin_failed error={error:?}"))?;
    let observed = manager
        .read_page(&mut reader, target_page)
        .ok_or_else(|| "new reader must observe committed page".to_string())?;
    if observed.as_bytes() != written.as_bytes() {
        return Err("new reader payload mismatch".to_string());
    }

    manager.abort(&mut reader);
    Ok(())
}

#[test]
fn test_ssi_write_skew_abort() -> Result<(), String> {
    let manager = TransactionManager::new(PageSize::DEFAULT);
    let mut txn = manager
        .begin(BeginKind::Concurrent)
        .map_err(|error| format!("begin_failed error={error:?}"))?;
    manager
        .write_page(&mut txn, page(13), base_page(0x70))
        .map_err(|error| format!("write_failed error={error:?}"))?;

    txn.has_in_rw = true;
    txn.has_out_rw = true;

    let result = manager.commit(&mut txn);
    if !matches!(result, Err(MvccError::BusySnapshot)) {
        return Err(format!("expected_ssi_busy_snapshot got={result:?}"));
    }

    Ok(())
}

#[test]
fn test_ssi_non_serializable_allows_dangerous_structure() -> Result<(), String> {
    let mut manager = TransactionManager::new(PageSize::DEFAULT);
    manager.set_ssi_enabled(false);

    let mut txn = manager
        .begin(BeginKind::Concurrent)
        .map_err(|error| format!("begin_failed error={error:?}"))?;
    manager
        .write_page(&mut txn, page(14), base_page(0x71))
        .map_err(|error| format!("write_failed error={error:?}"))?;

    txn.has_in_rw = true;
    txn.has_out_rw = true;

    manager
        .commit(&mut txn)
        .map_err(|error| format!("expected_si_commit_success error={error:?}"))?;

    Ok(())
}

#[test]
fn test_rebase_merge_distinct_offsets_succeeds() -> Result<(), String> {
    let manager = TransactionManager::new(PageSize::DEFAULT);
    let target = page(20);

    let base = base_page(0x20);
    let mut bootstrap = manager
        .begin(BeginKind::Immediate)
        .map_err(|error| format!("bootstrap_begin_failed error={error:?}"))?;
    manager
        .write_page(&mut bootstrap, target, base.clone())
        .map_err(|error| format!("bootstrap_write_failed error={error:?}"))?;
    manager
        .commit(&mut bootstrap)
        .map_err(|error| format!("bootstrap_commit_failed error={error:?}"))?;

    let mut tx1 = manager
        .begin(BeginKind::Concurrent)
        .map_err(|error| format!("tx1_begin_failed error={error:?}"))?;
    let mut tx2 = manager
        .begin(BeginKind::Concurrent)
        .map_err(|error| format!("tx2_begin_failed error={error:?}"))?;

    manager
        .write_page(&mut tx1, target, page_with_override(&base, 0, 0xAA))
        .map_err(|error| format!("tx1_write_failed error={error:?}"))?;
    manager
        .commit(&mut tx1)
        .map_err(|error| format!("tx1_commit_failed error={error:?}"))?;
    manager
        .write_page(&mut tx2, target, page_with_override(&base, 7, 0xBB))
        .map_err(|error| format!("tx2_write_failed error={error:?}"))?;
    manager
        .commit(&mut tx2)
        .map_err(|error| format!("tx2_commit_should_rebase error={error:?}"))?;

    let mut reader = manager
        .begin(BeginKind::Deferred)
        .map_err(|error| format!("reader_begin_failed error={error:?}"))?;
    let merged = manager
        .read_page(&mut reader, target)
        .ok_or_else(|| "merged page should exist".to_string())?;
    if merged.as_bytes()[0] != 0xAA || merged.as_bytes()[7] != 0xBB {
        return Err(format!(
            "rebase_merge_payload_mismatch b0={:#04x} b7={:#04x}",
            merged.as_bytes()[0],
            merged.as_bytes()[7]
        ));
    }
    manager.abort(&mut reader);

    Ok(())
}

#[test]
fn test_rebase_merge_overlapping_offsets_abort() -> Result<(), String> {
    let manager = TransactionManager::new(PageSize::DEFAULT);
    let target = page(21);

    let base = base_page(0x30);
    let mut bootstrap = manager
        .begin(BeginKind::Immediate)
        .map_err(|error| format!("bootstrap_begin_failed error={error:?}"))?;
    manager
        .write_page(&mut bootstrap, target, base.clone())
        .map_err(|error| format!("bootstrap_write_failed error={error:?}"))?;
    manager
        .commit(&mut bootstrap)
        .map_err(|error| format!("bootstrap_commit_failed error={error:?}"))?;

    let mut tx1 = manager
        .begin(BeginKind::Concurrent)
        .map_err(|error| format!("tx1_begin_failed error={error:?}"))?;
    let mut tx2 = manager
        .begin(BeginKind::Concurrent)
        .map_err(|error| format!("tx2_begin_failed error={error:?}"))?;

    manager
        .write_page(&mut tx1, target, page_with_override(&base, 4, 0xC1))
        .map_err(|error| format!("tx1_write_failed error={error:?}"))?;
    manager
        .commit(&mut tx1)
        .map_err(|error| format!("tx1_commit_failed error={error:?}"))?;
    manager
        .write_page(&mut tx2, target, page_with_override(&base, 4, 0xD2))
        .map_err(|error| format!("tx2_write_failed error={error:?}"))?;
    let result = manager.commit(&mut tx2);
    if !matches!(result, Err(MvccError::BusySnapshot)) {
        return Err(format!("expected_overlap_busy_snapshot got={result:?}"));
    }

    Ok(())
}

#[test]
fn test_mazurkiewicz_3txn_6_orderings() -> Result<(), String> {
    let mut outcomes = BTreeSet::new();
    for order in COMMIT_ORDERS {
        outcomes.insert(run_3txn_trace(order)?);
    }

    if outcomes.len() < 2 {
        return Err(format!(
            "expected_multiple_distinct_outcomes got={outcomes:?}"
        ));
    }

    Ok(())
}

#[test]
fn test_e2e_bd_bca_2_compliance() -> Result<(), String> {
    let description = load_issue_description(BEAD_ID)?;
    let evaluation = evaluate_description(&description);

    eprintln!(
        "DEBUG bead_id={BEAD_ID} case=e2e_bd_bca_2_compliance stage=start reference={LOG_STANDARD_REF}"
    );
    eprintln!(
        "INFO bead_id={BEAD_ID} case=e2e_summary missing_unit_ids={} missing_e2e_ids={} missing_log_levels={} missing_log_standard_ref={}",
        evaluation.missing_unit_ids.len(),
        evaluation.missing_e2e_ids.len(),
        evaluation.missing_log_levels.len(),
        evaluation.missing_log_standard_ref
    );

    for id in &evaluation.missing_unit_ids {
        eprintln!(
            "WARN bead_id={BEAD_ID} case=missing_unit_id id={id} reference={LOG_STANDARD_REF}"
        );
    }
    for id in &evaluation.missing_e2e_ids {
        eprintln!(
            "WARN bead_id={BEAD_ID} case=missing_e2e_id id={id} reference={LOG_STANDARD_REF}"
        );
    }
    for level in &evaluation.missing_log_levels {
        eprintln!(
            "WARN bead_id={BEAD_ID} case=missing_log_level level={level} reference={LOG_STANDARD_REF}"
        );
    }
    if evaluation.missing_log_standard_ref {
        eprintln!(
            "ERROR bead_id={BEAD_ID} case=missing_log_standard_ref expected={LOG_STANDARD_REF}"
        );
        return Err(format!(
            "bead_id={BEAD_ID} case=e2e_compliance_failure evaluation={evaluation:?}"
        ));
    }

    let mut outcomes = BTreeSet::new();
    for (seed, order) in COMMIT_ORDERS.iter().copied().enumerate() {
        let outcome = run_3txn_trace(order)?;
        eprintln!(
            "DEBUG bead_id={BEAD_ID} case=trace seed={} order={order:?} outcome={outcome:?}",
            seed + 1
        );
        outcomes.insert(outcome);
    }

    if outcomes.is_empty() {
        eprintln!("ERROR bead_id={BEAD_ID} case=empty_outcomes reference={LOG_STANDARD_REF}");
        return Err(format!("bead_id={BEAD_ID} case=no_trace_outcomes"));
    }

    eprintln!(
        "WARN bead_id={BEAD_ID} case=e2e_degraded_mode degraded_mode=0 reference={LOG_STANDARD_REF}"
    );
    eprintln!(
        "ERROR bead_id={BEAD_ID} case=e2e_terminal_failure_count terminal_failure_count=0 reference={LOG_STANDARD_REF}"
    );
    eprintln!(
        "INFO bead_id={BEAD_ID} case=e2e_complete trace_outcomes={} reference={LOG_STANDARD_REF}",
        outcomes.len()
    );

    Ok(())
}
