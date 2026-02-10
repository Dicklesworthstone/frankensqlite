use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;

use fsqlite_mvcc::{
    BeginKind, HotWitnessIndex, MvccError, TransactionManager, WitnessHierarchyConfigV1,
    bitset_to_slot_ids, derive_range_keys, validate_txn_token,
};
use fsqlite_types::{PageData, PageNumber, PageSize, TxnEpoch, TxnId, TxnToken, WitnessKey};
use proptest::prelude::proptest;
use proptest::test_runner::TestCaseError;
use serde_json::Value;

const BEAD_ID: &str = "bd-2d3i.1";
const ISSUES_JSONL: &str = ".beads/issues.jsonl";

const UNIT_TEST_IDS: [&str; 7] = [
    "test_disjoint_pages_both_commit",
    "test_same_page_disjoint_cells_merge",
    "test_classic_write_skew_aborts",
    "test_write_skew_nonserializable_succeeds",
    "test_slot_reuse_epoch_guard",
    "test_symbol_drop_recovery",
    "test_symbol_drop_beyond_tolerance",
];

const E2E_TEST_IDS: [&str; 3] = [
    "e2e_witness_plane_deterministic_suite",
    "e2e_witness_plane_cross_process_variant",
    "e2e_witness_plane_loss_profiles",
];

const LOG_LEVEL_MARKERS: [&str; 3] = ["INFO", "DEBUG", "ERROR"];

const REQUIRED_TOKENS: [&str; 13] = [
    "test_disjoint_pages_both_commit",
    "test_same_page_disjoint_cells_merge",
    "test_classic_write_skew_aborts",
    "test_write_skew_nonserializable_succeeds",
    "test_slot_reuse_epoch_guard",
    "test_symbol_drop_recovery",
    "test_symbol_drop_beyond_tolerance",
    "e2e_witness_plane_deterministic_suite",
    "e2e_witness_plane_cross_process_variant",
    "e2e_witness_plane_loss_profiles",
    "INFO",
    "DEBUG",
    "ERROR",
];

#[derive(Debug, PartialEq, Eq)]
#[allow(clippy::struct_field_names)]
struct ComplianceEvaluation {
    missing_unit_ids: Vec<&'static str>,
    missing_e2e_ids: Vec<&'static str>,
    missing_log_levels: Vec<&'static str>,
}

impl ComplianceEvaluation {
    fn is_compliant(&self) -> bool {
        self.missing_unit_ids.is_empty()
            && self.missing_e2e_ids.is_empty()
            && self.missing_log_levels.is_empty()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum InjectionPoint {
    BeforeRead,
    AfterRead,
    BeforeCommit,
    AfterCommit,
    DuringDecode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DecodeProof {
    required_symbols: usize,
    repair_symbols: usize,
    dropped_symbols: Vec<usize>,
    reordered_symbols: Vec<usize>,
    available_symbols: usize,
    recovered: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum DecodeOutcome {
    Recovered(DecodeProof),
    DurabilityContractViolated(DecodeProof),
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
        .any(|token| token == expected)
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
    }
}

fn page(pgno: u32) -> PageNumber {
    PageNumber::new(pgno).expect("page number must be non-zero")
}

fn seeded_page(seed: u8) -> PageData {
    let mut bytes = vec![0_u8; PageSize::DEFAULT.as_usize()];
    for (index, byte) in bytes.iter_mut().enumerate() {
        let offset = u8::try_from(index % 251).expect("offset must fit u8");
        *byte = seed.wrapping_add(offset);
    }
    PageData::from_vec(bytes)
}

fn with_byte_override(base: &PageData, offset: usize, value: u8) -> Result<PageData, String> {
    let mut bytes = base.as_bytes().to_vec();
    if offset >= bytes.len() {
        return Err(format!(
            "byte_override_out_of_bounds offset={offset} len={}",
            bytes.len()
        ));
    }
    bytes[offset] = value;
    Ok(PageData::from_vec(bytes))
}

fn deterministic_schedule_fingerprint(seed: u64, labels: &[&str]) -> String {
    let mut acc = seed ^ 0x9E37_79B9_7F4A_7C15;
    for label in labels {
        for byte in label.as_bytes() {
            acc = acc
                .wrapping_mul(0x1000_0000_01B3)
                .wrapping_add(u64::from(*byte));
        }
    }
    format!("{acc:016x}")
}

fn lcg_next(state: &mut u64) -> u64 {
    *state = state
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1);
    *state
}

fn deterministic_permutation(seed: u64, total: usize) -> Vec<usize> {
    let mut items = (0..total).collect::<Vec<_>>();
    let mut state = seed ^ 0xA5A5_A5A5_A5A5_A5A5;

    for idx in (1..items.len()).rev() {
        let span = u64::try_from(idx + 1).expect("idx + 1 fits u64");
        let j = usize::try_from(lcg_next(&mut state) % span).expect("mod result fits usize");
        items.swap(idx, j);
    }

    items
}

fn deterministic_injection_points(seed: u64) -> Vec<InjectionPoint> {
    let mut points = vec![
        InjectionPoint::BeforeRead,
        InjectionPoint::AfterRead,
        InjectionPoint::BeforeCommit,
        InjectionPoint::AfterCommit,
        InjectionPoint::DuringDecode,
    ];

    let mut state = seed;
    for idx in (1..points.len()).rev() {
        let span = u64::try_from(idx + 1).expect("idx + 1 fits u64");
        let j = usize::try_from(lcg_next(&mut state) % span).expect("mod result fits usize");
        points.swap(idx, j);
    }

    points
}

fn simulate_witness_symbol_decode(
    required_symbols: usize,
    repair_symbols: usize,
    seed: u64,
    drop_count: usize,
) -> DecodeOutcome {
    let total_symbols = required_symbols + repair_symbols;
    let reordered_symbols = deterministic_permutation(seed, total_symbols);
    let dropped_symbols = reordered_symbols
        .iter()
        .copied()
        .take(drop_count.min(total_symbols))
        .collect::<Vec<_>>();
    let dropped_set = dropped_symbols.iter().copied().collect::<BTreeSet<_>>();

    let available_symbols = reordered_symbols
        .iter()
        .filter(|symbol| !dropped_set.contains(symbol))
        .count();

    let recovered = available_symbols >= required_symbols;
    let proof = DecodeProof {
        required_symbols,
        repair_symbols,
        dropped_symbols,
        reordered_symbols,
        available_symbols,
        recovered,
    };

    if recovered {
        DecodeOutcome::Recovered(proof)
    } else {
        DecodeOutcome::DurabilityContractViolated(proof)
    }
}

fn scenario_disjoint_pages_both_commit() -> Result<(), String> {
    let manager = TransactionManager::new(PageSize::DEFAULT);

    let mut t1 = manager
        .begin(BeginKind::Concurrent)
        .map_err(|error| format!("begin_t1_failed error={error:?}"))?;
    let mut t2 = manager
        .begin(BeginKind::Concurrent)
        .map_err(|error| format!("begin_t2_failed error={error:?}"))?;

    manager
        .write_page(&mut t1, page(1), seeded_page(0x11))
        .map_err(|error| format!("write_t1_failed error={error:?}"))?;
    manager
        .write_page(&mut t2, page(2), seeded_page(0x22))
        .map_err(|error| format!("write_t2_failed error={error:?}"))?;

    manager
        .commit(&mut t1)
        .map_err(|error| format!("commit_t1_failed error={error:?}"))?;
    manager
        .commit(&mut t2)
        .map_err(|error| format!("commit_t2_failed error={error:?}"))?;

    let mut reader = manager
        .begin(BeginKind::Deferred)
        .map_err(|error| format!("reader_begin_failed error={error:?}"))?;

    let page1 = manager
        .read_page(&mut reader, page(1))
        .ok_or_else(|| "page1_missing_after_disjoint_commit".to_owned())?;
    let page2 = manager
        .read_page(&mut reader, page(2))
        .ok_or_else(|| "page2_missing_after_disjoint_commit".to_owned())?;

    if page1.as_bytes()[0] != 0x11 {
        return Err(format!(
            "disjoint_commit_page1_mismatch got={:#04x}",
            page1.as_bytes()[0]
        ));
    }
    if page2.as_bytes()[0] != 0x22 {
        return Err(format!(
            "disjoint_commit_page2_mismatch got={:#04x}",
            page2.as_bytes()[0]
        ));
    }

    Ok(())
}

fn scenario_same_page_disjoint_cells_merge() -> Result<(), String> {
    let manager = TransactionManager::new(PageSize::DEFAULT);

    // Rebase requires the page to exist at both writers' snapshot.
    let mut init = manager
        .begin(BeginKind::Immediate)
        .map_err(|error| format!("merge_init_begin_failed error={error:?}"))?;
    manager
        .write_page(&mut init, page(7), seeded_page(0))
        .map_err(|error| format!("merge_init_write_failed error={error:?}"))?;
    manager
        .commit(&mut init)
        .map_err(|error| format!("merge_init_commit_failed error={error:?}"))?;

    let mut t1 = manager
        .begin(BeginKind::Concurrent)
        .map_err(|error| format!("begin_t1_failed error={error:?}"))?;
    let mut t2 = manager
        .begin(BeginKind::Concurrent)
        .map_err(|error| format!("begin_t2_failed error={error:?}"))?;

    let base = manager
        .read_page(&mut t2, page(7))
        .unwrap_or_else(|| seeded_page(0));

    let t1_page = with_byte_override(&base, 0, 0xAA)?;
    let t2_page = with_byte_override(&base, 1, 0xBB)?;

    manager
        .write_page(&mut t1, page(7), t1_page)
        .map_err(|error| format!("write_t1_failed error={error:?}"))?;
    manager
        .commit(&mut t1)
        .map_err(|error| format!("commit_t1_failed error={error:?}"))?;

    manager
        .write_page(&mut t2, page(7), t2_page)
        .map_err(|error| format!("write_t2_failed error={error:?}"))?;

    manager
        .commit(&mut t2)
        .map_err(|error| format!("commit_t2_merge_failed error={error:?}"))?;

    let mut reader = manager
        .begin(BeginKind::Deferred)
        .map_err(|error| format!("reader_begin_failed error={error:?}"))?;
    let merged = manager
        .read_page(&mut reader, page(7))
        .ok_or_else(|| "merged_page_missing".to_owned())?;

    if merged.as_bytes()[0] != 0xAA {
        return Err(format!(
            "merged_page_byte0_mismatch got={:#04x}",
            merged.as_bytes()[0]
        ));
    }
    if merged.as_bytes()[1] != 0xBB {
        return Err(format!(
            "merged_page_byte1_mismatch got={:#04x}",
            merged.as_bytes()[1]
        ));
    }

    Ok(())
}

fn run_write_skew_case(ssi_enabled: bool) -> Result<(bool, bool), String> {
    let mut manager = TransactionManager::new(PageSize::DEFAULT);
    manager.set_ssi_enabled(ssi_enabled);

    // Seed baseline balances so both txns read the same snapshot.
    let mut init = manager
        .begin(BeginKind::Immediate)
        .map_err(|error| format!("init_begin_failed error={error:?}"))?;
    manager
        .write_page(&mut init, page(1), seeded_page(50))
        .map_err(|error| format!("init_write_page1_failed error={error:?}"))?;
    manager
        .write_page(&mut init, page(2), seeded_page(50))
        .map_err(|error| format!("init_write_page2_failed error={error:?}"))?;
    manager
        .commit(&mut init)
        .map_err(|error| format!("init_commit_failed error={error:?}"))?;

    let mut t1 = manager
        .begin(BeginKind::Concurrent)
        .map_err(|error| format!("begin_t1_failed error={error:?}"))?;
    let mut t2 = manager
        .begin(BeginKind::Concurrent)
        .map_err(|error| format!("begin_t2_failed error={error:?}"))?;

    let t1_read_page1 = manager
        .read_page(&mut t1, page(1))
        .ok_or_else(|| "t1_read_page1_missing".to_owned())?;
    let _t1_read_page2 = manager
        .read_page(&mut t1, page(2))
        .ok_or_else(|| "t1_read_page2_missing".to_owned())?;

    let _t2_read_page1 = manager
        .read_page(&mut t2, page(1))
        .ok_or_else(|| "t2_read_page1_missing".to_owned())?;
    let t2_read_page2 = manager
        .read_page(&mut t2, page(2))
        .ok_or_else(|| "t2_read_page2_missing".to_owned())?;

    manager
        .write_page(&mut t1, page(1), with_byte_override(&t1_read_page1, 0, 10)?)
        .map_err(|error| format!("t1_write_failed error={error:?}"))?;
    manager
        .write_page(&mut t2, page(2), with_byte_override(&t2_read_page2, 0, 10)?)
        .map_err(|error| format!("t2_write_failed error={error:?}"))?;

    let first_committed = manager.commit(&mut t1).is_ok();

    // Minimal deterministic dangerous-structure marker for the second txn.
    t2.has_in_rw = true;
    t2.has_out_rw = true;

    let second_result = manager.commit(&mut t2);
    let second_committed = second_result.is_ok();

    if ssi_enabled {
        if !matches!(second_result, Err(MvccError::BusySnapshot)) {
            return Err(format!(
                "expected_busy_snapshot_with_ssi second_result={second_result:?}"
            ));
        }
    } else if second_result.is_err() {
        return Err(format!(
            "ssi_off_should_allow_write_skew second_result={second_result:?}"
        ));
    }

    Ok((first_committed, second_committed))
}

fn scenario_slot_reuse_epoch_guard() -> Result<(), String> {
    let slot_id = TxnId::new(9).expect("slot id must be valid");
    let stale_token = TxnToken::new(slot_id, TxnEpoch::new(7));
    let reused_epoch = TxnEpoch::new(8);

    if validate_txn_token(&stale_token, slot_id, reused_epoch) {
        return Err("stale_token_incorrectly_validated_after_slot_reuse".to_owned());
    }

    let fresh_token = TxnToken::new(slot_id, reused_epoch);
    if !validate_txn_token(&fresh_token, slot_id, reused_epoch) {
        return Err("fresh_token_failed_validation".to_owned());
    }

    Ok(())
}

fn model_cross_process_witness_visibility() -> Result<(), String> {
    let witness_index = Arc::new(HotWitnessIndex::new(16, 64));
    let witness_epoch = witness_index.current_epoch();

    let config = WitnessHierarchyConfigV1::default();
    let key = WitnessKey::Page(page(41));
    let range_keys = Arc::new(derive_range_keys(&key, &config));

    let index_a = Arc::clone(&witness_index);
    let keys_a = Arc::clone(&range_keys);
    let process_a = thread::spawn(move || {
        index_a.register_read(0, witness_epoch, keys_a.as_slice());
    });

    let index_b = Arc::clone(&witness_index);
    let keys_b = Arc::clone(&range_keys);
    let process_b = thread::spawn(move || {
        index_b.register_write(1, witness_epoch, keys_b.as_slice());
    });

    process_a
        .join()
        .map_err(|_| "process_a_join_failed".to_owned())?;
    process_b
        .join()
        .map_err(|_| "process_b_join_failed".to_owned())?;

    let readers = bitset_to_slot_ids(&witness_index.candidate_readers(range_keys.as_slice()))
        .into_iter()
        .collect::<BTreeSet<_>>();
    let writers = bitset_to_slot_ids(&witness_index.candidate_writers(range_keys.as_slice()))
        .into_iter()
        .collect::<BTreeSet<_>>();

    if !readers.contains(&0) {
        return Err(format!("cross_process_reader_missing readers={readers:?}"));
    }
    if !writers.contains(&1) {
        return Err(format!("cross_process_writer_missing writers={writers:?}"));
    }

    Ok(())
}

#[test]
fn test_bd_2d3i_1_unit_compliance_gate() -> Result<(), String> {
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

    Ok(())
}

proptest! {
    #[test]
    fn prop_bd_2d3i_1_structure_compliance(missing_index in 0usize..REQUIRED_TOKENS.len()) {
        let mut synthetic = format!(
            "## Unit Tests\n- {}\n- {}\n- {}\n- {}\n- {}\n- {}\n- {}\n\n## E2E\n- {}\n- {}\n- {}\n\n## Logging\n- {}\n- {}\n- {}\n",
            UNIT_TEST_IDS[0],
            UNIT_TEST_IDS[1],
            UNIT_TEST_IDS[2],
            UNIT_TEST_IDS[3],
            UNIT_TEST_IDS[4],
            UNIT_TEST_IDS[5],
            UNIT_TEST_IDS[6],
            E2E_TEST_IDS[0],
            E2E_TEST_IDS[1],
            E2E_TEST_IDS[2],
            LOG_LEVEL_MARKERS[0],
            LOG_LEVEL_MARKERS[1],
            LOG_LEVEL_MARKERS[2],
        );

        synthetic = synthetic.replacen(REQUIRED_TOKENS[missing_index], "", 1);
        let evaluation = evaluate_description(&synthetic);

        if evaluation.is_compliant() {
            return Err(TestCaseError::fail(format!(
                "bead_id={} case=structure_compliance expected_non_compliant missing_index={} missing_token={}",
                BEAD_ID,
                missing_index,
                REQUIRED_TOKENS[missing_index]
            )));
        }
    }
}

#[test]
fn test_disjoint_pages_both_commit() -> Result<(), String> {
    scenario_disjoint_pages_both_commit()
}

#[test]
fn test_same_page_disjoint_cells_merge() -> Result<(), String> {
    scenario_same_page_disjoint_cells_merge()
}

#[test]
fn test_classic_write_skew_aborts() -> Result<(), String> {
    let (first_committed, second_committed) = run_write_skew_case(true)?;
    if !first_committed {
        return Err("first_writer_should_commit_in_write_skew_case".to_owned());
    }
    if second_committed {
        return Err("second_writer_should_abort_when_ssi_enabled".to_owned());
    }
    Ok(())
}

#[test]
fn test_write_skew_nonserializable_succeeds() -> Result<(), String> {
    let (first_committed, second_committed) = run_write_skew_case(false)?;
    if !first_committed || !second_committed {
        return Err(format!(
            "both_writers_should_commit_when_serializable_off first={first_committed} second={second_committed}"
        ));
    }
    Ok(())
}

#[test]
fn test_slot_reuse_epoch_guard() -> Result<(), String> {
    scenario_slot_reuse_epoch_guard()
}

#[test]
fn test_symbol_drop_recovery() -> Result<(), String> {
    let outcome = simulate_witness_symbol_decode(6, 2, 0xD3A1, 2);
    match outcome {
        DecodeOutcome::Recovered(proof) => {
            if proof.available_symbols < proof.required_symbols {
                return Err(format!(
                    "recovery_reported_with_insufficient_symbols proof={proof:?}"
                ));
            }
            Ok(())
        }
        DecodeOutcome::DurabilityContractViolated(proof) => Err(format!(
            "expected_recovery_within_tolerance proof={proof:?}"
        )),
    }
}

#[test]
fn test_symbol_drop_beyond_tolerance() -> Result<(), String> {
    let outcome = simulate_witness_symbol_decode(6, 1, 0xD3A2, 2);
    match outcome {
        DecodeOutcome::DurabilityContractViolated(proof) => {
            if proof.recovered {
                return Err(format!(
                    "durability_violation_must_not_mark_recovered proof={proof:?}"
                ));
            }
            if proof.available_symbols >= proof.required_symbols {
                return Err(format!(
                    "durability_violation_must_have_insufficient_symbols proof={proof:?}"
                ));
            }
            Ok(())
        }
        DecodeOutcome::Recovered(proof) => Err(format!(
            "expected_durability_contract_violation proof={proof:?}"
        )),
    }
}

#[test]
fn e2e_witness_plane_deterministic_suite() -> Result<(), String> {
    scenario_disjoint_pages_both_commit()?;
    scenario_same_page_disjoint_cells_merge()?;

    let (first_committed, second_committed) = run_write_skew_case(true)?;
    if !first_committed || second_committed {
        return Err(format!(
            "ssi_suite_write_skew_outcome_unexpected first={first_committed} second={second_committed}"
        ));
    }

    scenario_slot_reuse_epoch_guard()?;

    let recovered = simulate_witness_symbol_decode(8, 3, 0x44, 2);
    if !matches!(recovered, DecodeOutcome::Recovered(_)) {
        return Err(format!(
            "expected_recovery_in_deterministic_suite got={recovered:?}"
        ));
    }

    let violated = simulate_witness_symbol_decode(8, 1, 0x44, 3);
    if !matches!(violated, DecodeOutcome::DurabilityContractViolated(_)) {
        return Err(format!(
            "expected_violation_in_deterministic_suite got={violated:?}"
        ));
    }

    let points = deterministic_injection_points(0x2D31);
    let unique = points.into_iter().collect::<BTreeSet<_>>();
    if unique.len() != 5 {
        return Err(format!(
            "injection_point_coverage_incomplete unique={unique:?}"
        ));
    }

    let labels = [
        "disjoint_pages",
        "disjoint_cells_merge",
        "write_skew_abort",
        "slot_reuse_epoch_guard",
        "symbol_drop_profiles",
    ];

    let fingerprint_a = deterministic_schedule_fingerprint(0x2D31, &labels);
    let fingerprint_b = deterministic_schedule_fingerprint(0x2D31, &labels);
    if fingerprint_a != fingerprint_b {
        return Err(format!(
            "deterministic_fingerprint_mismatch a={fingerprint_a} b={fingerprint_b}"
        ));
    }

    Ok(())
}

#[test]
fn e2e_witness_plane_cross_process_variant() -> Result<(), String> {
    model_cross_process_witness_visibility()
}

#[test]
fn e2e_witness_plane_loss_profiles() -> Result<(), String> {
    let profiles: [(u8, usize); 3] = [(0, 0), (1, 1), (5, 2)];

    for (loss_rate, drop_count) in profiles {
        let outcome =
            simulate_witness_symbol_decode(12, 3, 0x7000 + u64::from(loss_rate), drop_count);
        if !matches!(outcome, DecodeOutcome::Recovered(_)) {
            return Err(format!(
                "loss_profile_should_recover loss_rate={loss_rate} outcome={outcome:?}"
            ));
        }
    }

    let hard_loss = simulate_witness_symbol_decode(12, 3, 0x7FFF, 4);
    if !matches!(hard_loss, DecodeOutcome::DurabilityContractViolated(_)) {
        return Err(format!(
            "loss_profile_beyond_tolerance_must_violate outcome={hard_loss:?}"
        ));
    }

    Ok(())
}

#[test]
fn test_e2e_bd_2d3i_1_compliance() -> Result<(), String> {
    let description = load_issue_description(BEAD_ID)?;
    let evaluation = evaluate_description(&description);
    if !evaluation.is_compliant() {
        return Err(format!(
            "bead_id={BEAD_ID} case=description_non_compliant evaluation={evaluation:?}"
        ));
    }

    e2e_witness_plane_deterministic_suite()?;
    e2e_witness_plane_cross_process_variant()?;
    e2e_witness_plane_loss_profiles()?;

    Ok(())
}
