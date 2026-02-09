use std::fs;
use std::path::{Path, PathBuf};

use fsqlite_mvcc::{
    CommitIndex, ConcurrentRegistry, FcwResult, InProcessPageLockTable, MAIN_DB_ID, MvccError,
    PrepareResult, RecoveryAction, TwoPhaseCoordinator, concurrent_commit, concurrent_write_page,
};
use fsqlite_types::{CommitSeq, PageData, PageNumber, PageSize, SchemaEpoch, Snapshot};
use proptest::prelude::proptest;
use proptest::test_runner::TestCaseError;
use serde_json::Value;

const BEAD_ID: &str = "bd-d2m7";
const ISSUES_JSONL: &str = ".beads/issues.jsonl";
const LOG_STANDARD_REF: &str = "bd-1fpm";
const UNIT_TEST_IDS: [&str; 2] = [
    "test_bd_d2m7_unit_compliance_gate",
    "prop_bd_d2m7_structure_compliance",
];
const SPEC_UNIT_TEST_IDS: [&str; 12] = [
    "test_begin_concurrent_multiple_writers",
    "test_begin_concurrent_page_conflict_busy_snapshot",
    "test_begin_concurrent_first_committer_wins",
    "test_cross_database_two_phase_commit",
    "test_savepoint_within_concurrent",
    "test_attach_detach_limit",
    "test_cross_db_2pc_both_committed",
    "test_cross_db_2pc_crash_after_prepare",
    "test_cross_db_2pc_one_db_fails_prepare",
    "test_cross_db_2pc_crash_during_phase2",
    "test_cross_db_2pc_max_attached",
    "test_cross_db_2pc_wal_mode_required",
];
const E2E_TEST_IDS: [&str; 2] = [
    "test_e2e_bd_d2m7_compliance",
    "test_e2e_cross_db_two_phase_commit_atomicity",
];
const LOG_LEVEL_MARKERS: [&str; 4] = ["DEBUG", "INFO", "WARN", "ERROR"];
const REQUIRED_TOKENS: [&str; 21] = [
    "test_bd_d2m7_unit_compliance_gate",
    "prop_bd_d2m7_structure_compliance",
    "test_begin_concurrent_multiple_writers",
    "test_begin_concurrent_page_conflict_busy_snapshot",
    "test_begin_concurrent_first_committer_wins",
    "test_cross_database_two_phase_commit",
    "test_savepoint_within_concurrent",
    "test_attach_detach_limit",
    "test_cross_db_2pc_both_committed",
    "test_cross_db_2pc_crash_after_prepare",
    "test_cross_db_2pc_one_db_fails_prepare",
    "test_cross_db_2pc_crash_during_phase2",
    "test_cross_db_2pc_max_attached",
    "test_cross_db_2pc_wal_mode_required",
    "test_e2e_bd_d2m7_compliance",
    "test_e2e_cross_db_two_phase_commit_atomicity",
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
    missing_spec_unit_ids: Vec<&'static str>,
    missing_e2e_ids: Vec<&'static str>,
    missing_log_levels: Vec<&'static str>,
    missing_log_standard_ref: bool,
}

impl ComplianceEvaluation {
    fn is_compliant(&self) -> bool {
        self.missing_unit_ids.is_empty()
            && self.missing_spec_unit_ids.is_empty()
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

fn contains_identifier(text: &str, expected: &str) -> bool {
    text.split(|ch: char| !(ch.is_ascii_alphanumeric() || ch == '_'))
        .any(|token| token == expected)
}

fn evaluate_description(description: &str) -> ComplianceEvaluation {
    let missing_unit_ids = UNIT_TEST_IDS
        .into_iter()
        .filter(|id| !contains_identifier(description, id))
        .collect::<Vec<_>>();
    let missing_spec_unit_ids = SPEC_UNIT_TEST_IDS
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
        missing_spec_unit_ids,
        missing_e2e_ids,
        missing_log_levels,
        missing_log_standard_ref: !description.contains(LOG_STANDARD_REF),
    }
}

fn snapshot_at(high: u64) -> Snapshot {
    Snapshot {
        high: CommitSeq::new(high),
        schema_epoch: SchemaEpoch::ZERO,
    }
}

fn page(number: u32) -> PageNumber {
    PageNumber::new(number).expect("page number must be non-zero")
}

fn page_data(tag: u8) -> PageData {
    let mut bytes = vec![0_u8; PageSize::DEFAULT.as_usize()];
    bytes[0] = tag;
    PageData::from_vec(bytes)
}

fn run_begin_concurrent_scenario() -> Result<(), String> {
    let lock_table = InProcessPageLockTable::new();
    let commit_index = CommitIndex::new();
    let mut registry = ConcurrentRegistry::new();

    let s1 = registry
        .begin_concurrent(snapshot_at(100))
        .map_err(|error| format!("begin_s1_failed error={error:?}"))?;
    let s2 = registry
        .begin_concurrent(snapshot_at(100))
        .map_err(|error| format!("begin_s2_failed error={error:?}"))?;

    {
        let handle = registry
            .get_mut(s1)
            .ok_or_else(|| "missing_handle_s1".to_string())?;
        concurrent_write_page(handle, &lock_table, s1, page(5), page_data(0xA1))
            .map_err(|error| format!("write_s1_failed error={error:?}"))?;
    }
    {
        let handle = registry
            .get_mut(s2)
            .ok_or_else(|| "missing_handle_s2".to_string())?;
        concurrent_write_page(handle, &lock_table, s2, page(9), page_data(0xB2))
            .map_err(|error| format!("write_s2_failed error={error:?}"))?;
    }

    let seq1 = {
        let handle = registry
            .get_mut(s1)
            .ok_or_else(|| "missing_handle_s1_commit".to_string())?;
        concurrent_commit(handle, &commit_index, &lock_table, s1, CommitSeq::new(101))
            .map_err(|(error, fcw)| format!("commit_s1_failed error={error:?} fcw={fcw:?}"))?
    };
    let seq2 = {
        let handle = registry
            .get_mut(s2)
            .ok_or_else(|| "missing_handle_s2_commit".to_string())?;
        concurrent_commit(handle, &commit_index, &lock_table, s2, CommitSeq::new(102))
            .map_err(|(error, fcw)| format!("commit_s2_failed error={error:?} fcw={fcw:?}"))?
    };

    if seq1 != CommitSeq::new(101) || seq2 != CommitSeq::new(102) {
        return Err(format!(
            "unexpected_commit_seq seq1={} seq2={}",
            seq1.get(),
            seq2.get()
        ));
    }

    let s3 = registry
        .begin_concurrent(snapshot_at(100))
        .map_err(|error| format!("begin_s3_failed error={error:?}"))?;
    {
        let handle = registry
            .get_mut(s3)
            .ok_or_else(|| "missing_handle_s3".to_string())?;
        concurrent_write_page(handle, &lock_table, s3, page(5), page_data(0xC3))
            .map_err(|error| format!("write_s3_failed error={error:?}"))?;
    }

    let conflict = {
        let handle = registry
            .get_mut(s3)
            .ok_or_else(|| "missing_handle_s3_commit".to_string())?;
        concurrent_commit(handle, &commit_index, &lock_table, s3, CommitSeq::new(103))
    };
    match conflict {
        Err((MvccError::BusySnapshot, FcwResult::Conflict { .. })) => Ok(()),
        other => Err(format!("expected_busy_snapshot_conflict got={other:?}")),
    }
}

fn run_cross_db_2pc_atomicity_scenario() -> Result<(), String> {
    let mut coordinator = TwoPhaseCoordinator::new(77);
    coordinator
        .add_participant(MAIN_DB_ID, "main".to_owned(), true)
        .map_err(|error| format!("add_main_failed error={error:?}"))?;
    coordinator
        .add_participant(2, "aux".to_owned(), true)
        .map_err(|error| format!("add_aux_failed error={error:?}"))?;

    coordinator
        .prepare_participant(
            MAIN_DB_ID,
            PrepareResult::Ok {
                wal_offset: 4096,
                frame_count: 2,
            },
        )
        .map_err(|error| format!("prepare_main_failed error={error:?}"))?;
    coordinator
        .prepare_participant(
            2,
            PrepareResult::Ok {
                wal_offset: 8192,
                frame_count: 2,
            },
        )
        .map_err(|error| format!("prepare_aux_failed error={error:?}"))?;
    coordinator
        .check_all_prepared()
        .map_err(|error| format!("check_all_prepared_failed error={error:?}"))?;
    coordinator
        .write_commit_marker(CommitSeq::new(200), 2_000_000)
        .map_err(|error| format!("write_marker_failed error={error:?}"))?;
    coordinator
        .commit_participant(MAIN_DB_ID)
        .map_err(|error| format!("commit_main_failed error={error:?}"))?;

    let recovery = TwoPhaseCoordinator::determine_recovery(true, false);
    if recovery != RecoveryAction::RollForward {
        return Err(format!("expected_roll_forward got={recovery:?}"));
    }

    coordinator
        .commit_participant(2)
        .map_err(|error| format!("commit_aux_failed error={error:?}"))?;
    coordinator
        .check_all_committed()
        .map_err(|error| format!("check_all_committed_failed error={error:?}"))?;
    if !coordinator.is_committed() {
        return Err("coordinator_not_committed_after_recovery".to_string());
    }

    Ok(())
}

fn run_cross_db_prepare_crash_atomicity_scenario() -> Result<(), String> {
    let mut coordinator = TwoPhaseCoordinator::new(78);
    coordinator
        .add_participant(MAIN_DB_ID, "main".to_owned(), true)
        .map_err(|error| format!("add_main_failed error={error:?}"))?;
    coordinator
        .add_participant(2, "aux".to_owned(), true)
        .map_err(|error| format!("add_aux_failed error={error:?}"))?;

    coordinator
        .prepare_participant(
            MAIN_DB_ID,
            PrepareResult::Ok {
                wal_offset: 12288,
                frame_count: 1,
            },
        )
        .map_err(|error| format!("prepare_main_failed error={error:?}"))?;
    coordinator
        .prepare_participant(
            2,
            PrepareResult::Ok {
                wal_offset: 16384,
                frame_count: 1,
            },
        )
        .map_err(|error| format!("prepare_aux_failed error={error:?}"))?;
    coordinator
        .check_all_prepared()
        .map_err(|error| format!("check_all_prepared_failed error={error:?}"))?;

    match TwoPhaseCoordinator::determine_recovery(false, false) {
        RecoveryAction::NoAction => Err("prepare_crash_cannot_resolve_to_no_action".to_string()),
        RecoveryAction::RollBack => {
            coordinator
                .abort()
                .map_err(|error| format!("abort_failed error={error:?}"))?;
            if coordinator.is_aborted() && !coordinator.is_committed() {
                Ok(())
            } else {
                Err("rollback_path_not_atomic".to_string())
            }
        }
        RecoveryAction::RollForward => {
            coordinator
                .write_commit_marker(CommitSeq::new(201), 2_100_000)
                .map_err(|error| format!("write_marker_failed error={error:?}"))?;
            coordinator
                .commit_participant(MAIN_DB_ID)
                .map_err(|error| format!("commit_main_failed error={error:?}"))?;
            coordinator
                .commit_participant(2)
                .map_err(|error| format!("commit_aux_failed error={error:?}"))?;
            coordinator
                .check_all_committed()
                .map_err(|error| format!("check_all_committed_failed error={error:?}"))?;
            if coordinator.is_committed() {
                Ok(())
            } else {
                Err("rollforward_path_not_atomic".to_string())
            }
        }
    }
}

#[test]
fn test_bd_d2m7_unit_compliance_gate() -> Result<(), String> {
    let description = load_issue_description(BEAD_ID)?;
    let evaluation = evaluate_description(&description);

    if !evaluation.missing_unit_ids.is_empty() {
        return Err(format!(
            "bead_id={BEAD_ID} case=unit_ids_missing missing={:?}",
            evaluation.missing_unit_ids
        ));
    }
    if !evaluation.missing_spec_unit_ids.is_empty() {
        return Err(format!(
            "bead_id={BEAD_ID} case=spec_unit_ids_missing missing={:?}",
            evaluation.missing_spec_unit_ids
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
    fn prop_bd_d2m7_structure_compliance(missing_index in 0usize..REQUIRED_TOKENS.len()) {
        let mut synthetic = String::from("## Unit Test Requirements\n");
        for id in UNIT_TEST_IDS {
            synthetic.push_str("- ");
            synthetic.push_str(id);
            synthetic.push('\n');
        }
        for id in SPEC_UNIT_TEST_IDS {
            synthetic.push_str("- ");
            synthetic.push_str(id);
            synthetic.push('\n');
        }

        synthetic.push_str("\n## E2E Test\n");
        for id in E2E_TEST_IDS {
            synthetic.push_str("- ");
            synthetic.push_str(id);
            synthetic.push('\n');
        }

        synthetic.push_str("\n## Logging Requirements\n");
        synthetic.push_str("- DEBUG: stage-level progress\n");
        synthetic.push_str("- INFO: summary counters\n");
        synthetic.push_str("- WARN: degraded/retry conditions\n");
        synthetic.push_str("- ERROR: terminal failure diagnostics\n");
        synthetic.push_str("- Reference: ");
        synthetic.push_str(LOG_STANDARD_REF);
        synthetic.push('\n');

        synthetic = synthetic.replacen(REQUIRED_TOKENS[missing_index], "", 1);
        let evaluation = evaluate_description(&synthetic);
        if evaluation.is_compliant() {
            return Err(TestCaseError::fail(format!(
                "bead_id={BEAD_ID} case=structure_compliance expected_non_compliant missing_index={missing_index} missing_marker={}",
                REQUIRED_TOKENS[missing_index]
            )));
        }
    }
}

#[test]
fn test_e2e_cross_db_two_phase_commit_atomicity() -> Result<(), String> {
    run_cross_db_2pc_atomicity_scenario()?;
    run_cross_db_prepare_crash_atomicity_scenario()?;
    Ok(())
}

#[test]
fn test_e2e_bd_d2m7_compliance() -> Result<(), String> {
    let description = load_issue_description(BEAD_ID)?;
    let evaluation = evaluate_description(&description);

    eprintln!(
        "DEBUG bead_id={BEAD_ID} case=e2e_bd_d2m7_compliance stage=start reference={LOG_STANDARD_REF}"
    );
    eprintln!(
        "INFO bead_id={BEAD_ID} case=e2e_summary missing_unit_ids={} missing_spec_unit_ids={} missing_e2e_ids={} missing_log_levels={} missing_log_standard_ref={}",
        evaluation.missing_unit_ids.len(),
        evaluation.missing_spec_unit_ids.len(),
        evaluation.missing_e2e_ids.len(),
        evaluation.missing_log_levels.len(),
        evaluation.missing_log_standard_ref
    );

    for id in &evaluation.missing_unit_ids {
        eprintln!(
            "WARN bead_id={BEAD_ID} case=missing_unit_id id={id} reference={LOG_STANDARD_REF}"
        );
    }
    for id in &evaluation.missing_spec_unit_ids {
        eprintln!(
            "WARN bead_id={BEAD_ID} case=missing_spec_unit_id id={id} reference={LOG_STANDARD_REF}"
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

    if !evaluation.is_compliant() {
        eprintln!(
            "ERROR bead_id={BEAD_ID} case=e2e_compliance_failure reference={LOG_STANDARD_REF}"
        );
        return Err(format!(
            "bead_id={BEAD_ID} case=e2e_compliance_failure evaluation={evaluation:?}"
        ));
    }

    run_begin_concurrent_scenario()?;
    run_cross_db_2pc_atomicity_scenario()?;
    run_cross_db_prepare_crash_atomicity_scenario()?;

    eprintln!(
        "WARN bead_id={BEAD_ID} case=e2e_degraded_mode degraded_mode=0 reference={LOG_STANDARD_REF}"
    );
    eprintln!(
        "ERROR bead_id={BEAD_ID} case=e2e_terminal_failure_count terminal_failure_count=0 reference={LOG_STANDARD_REF}"
    );
    eprintln!("INFO bead_id={BEAD_ID} case=e2e_complete status=pass reference={LOG_STANDARD_REF}");

    Ok(())
}
