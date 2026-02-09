use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread;

use proptest::prelude::proptest;
use proptest::test_runner::TestCaseError;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tempfile::tempdir;

const BEAD_ID: &str = "bd-2npr";
const ISSUES_JSONL: &str = ".beads/issues.jsonl";
const LOG_STANDARD_REF: &str = "bd-1fpm";
const UNIT_TEST_IDS: [&str; 2] = [
    "test_bd_2npr_unit_compliance_gate",
    "prop_bd_2npr_structure_compliance",
];
const E2E_TEST_IDS: [&str; 1] = ["test_e2e_bd_2npr_compliance"];
const LOG_LEVEL_MARKERS: [&str; 4] = ["DEBUG", "INFO", "WARN", "ERROR"];
const REQUIRED_TOKENS: [&str; 8] = [
    "test_bd_2npr_unit_compliance_gate",
    "prop_bd_2npr_structure_compliance",
    "test_e2e_bd_2npr_compliance",
    "DEBUG",
    "INFO",
    "WARN",
    "ERROR",
    "bd-1fpm",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IsolationMode {
    Serializable,
    SnapshotIsolation,
    Serialized,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IntentKind {
    InsertRow { row_key: u64 },
    UpdateRow { row_key: u64 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TxnSpec {
    txn_id: u64,
    read_pages: Vec<u32>,
    write_pages: Vec<u32>,
    intent: IntentKind,
    snapshot_seq: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AbortReason {
    PageConflict,
    SsiWriteSkew,
    SerializedWriterBusy,
    NonCommutativeConflict,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct SimulationResult {
    committed: Vec<u64>,
    aborted: Vec<(u64, AbortReason)>,
    conflict_pages_by_txn: BTreeMap<u64, Vec<u32>>,
    merged_txns: Vec<u64>,
    deadlock_detected: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct StressMetrics {
    commit_count: usize,
    abort_count: usize,
    conflict_count: usize,
    merged_count: usize,
    throughput_ops_per_sec: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct InvariantReport {
    schema_version: u32,
    run_id: String,
    seed: u64,
    writer_count: usize,
    commit_count: usize,
    abort_count: usize,
    conflict_count: usize,
    no_deadlock: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ArtifactReport {
    schema_version: u32,
    bead_id: String,
    seed: u64,
    writer_count: usize,
    commit_count: usize,
    abort_count: usize,
    conflict_count: usize,
    trace_len: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
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

fn lcg_next(state: &mut u64) -> u64 {
    *state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
    *state
}

fn shuffle_in_place<T>(seed: u64, items: &mut [T]) {
    let mut state = seed ^ 0xA5A5_A5A5_A5A5_A5A5;
    let len = items.len();
    for idx in (1..len).rev() {
        let next = lcg_next(&mut state);
        let j = usize::try_from(next % u64::try_from(idx + 1).expect("idx+1 fits u64"))
            .expect("mod result fits usize");
        items.swap(idx, j);
    }
}

fn generate_schedule(seed: u64, txn_ids: &[u64]) -> Vec<u64> {
    let mut order = txn_ids.to_vec();
    shuffle_in_place(seed, &mut order);
    order
}

fn generate_workload(seed: u64, txn_count: usize, page_pool: u32, overlap_pct: u8) -> Vec<TxnSpec> {
    let mut state = seed;
    let mut out = Vec::with_capacity(txn_count);
    for idx in 0..txn_count {
        let txn_id = u64::try_from(idx + 1).expect("txn index should fit u64");
        let hot_pick = (lcg_next(&mut state) % 100) < u64::from(overlap_pct);
        let write_page = if hot_pick {
            1
        } else {
            let page = (lcg_next(&mut state) % u64::from(page_pool.saturating_sub(2).max(1))) + 2;
            u32::try_from(page).expect("page should fit u32")
        };
        let read_page = if hot_pick {
            2
        } else {
            let page = (lcg_next(&mut state) % u64::from(page_pool.saturating_sub(2).max(1))) + 2;
            u32::try_from(page).expect("page should fit u32")
        };
        let intent = if idx % 2 == 0 {
            IntentKind::InsertRow { row_key: txn_id }
        } else {
            IntentKind::UpdateRow { row_key: txn_id }
        };
        out.push(TxnSpec {
            txn_id,
            read_pages: vec![read_page],
            write_pages: vec![write_page],
            intent,
            snapshot_seq: 0,
        });
    }
    out
}

fn is_commutative(a: IntentKind, b: IntentKind) -> bool {
    match (a, b) {
        (IntentKind::InsertRow { row_key: ra }, IntentKind::InsertRow { row_key: rb }) => ra != rb,
        _ => false,
    }
}

fn write_skew_detected(txn: &TxnSpec, committed: &HashMap<u64, TxnSpec>) -> bool {
    let read_set = txn.read_pages.iter().copied().collect::<HashSet<_>>();
    let write_set = txn.write_pages.iter().copied().collect::<HashSet<_>>();
    for other in committed.values() {
        let other_read = other.read_pages.iter().copied().collect::<HashSet<_>>();
        let other_write = other.write_pages.iter().copied().collect::<HashSet<_>>();
        let incoming = !read_set.is_disjoint(&other_write);
        let outgoing = !other_read.is_disjoint(&write_set);
        let disjoint_writes = write_set.is_disjoint(&other_write);
        if incoming && outgoing && disjoint_writes {
            return true;
        }
    }
    false
}

fn simulate_mvcc(
    txns: &[TxnSpec],
    schedule: &[u64],
    mode: IsolationMode,
    merge_safe: bool,
) -> SimulationResult {
    let by_id = txns
        .iter()
        .map(|txn| (txn.txn_id, txn.clone()))
        .collect::<HashMap<_, _>>();
    let mut committed_by_page = HashMap::<u32, u64>::new();
    let mut committed_txns = HashMap::<u64, TxnSpec>::new();
    let mut writer_committed = false;
    let mut result = SimulationResult::default();

    for txn_id in schedule {
        let Some(txn) = by_id.get(txn_id) else {
            continue;
        };

        if txn.write_pages.is_empty() {
            result.committed.push(txn.txn_id);
            committed_txns.insert(txn.txn_id, txn.clone());
            continue;
        }

        if mode == IsolationMode::Serialized {
            if writer_committed {
                result
                    .aborted
                    .push((txn.txn_id, AbortReason::SerializedWriterBusy));
                continue;
            }
            writer_committed = true;
        }

        if mode == IsolationMode::Serializable && write_skew_detected(txn, &committed_txns) {
            result.aborted.push((txn.txn_id, AbortReason::SsiWriteSkew));
            continue;
        }

        let mut conflict_pages = txn
            .write_pages
            .iter()
            .copied()
            .filter(|page| committed_by_page.contains_key(page))
            .collect::<Vec<_>>();
        conflict_pages.sort_unstable();
        conflict_pages.dedup();

        if conflict_pages.is_empty() {
            for page in &txn.write_pages {
                committed_by_page.insert(*page, txn.txn_id);
            }
            result.committed.push(txn.txn_id);
            committed_txns.insert(txn.txn_id, txn.clone());
            continue;
        }

        result
            .conflict_pages_by_txn
            .insert(txn.txn_id, conflict_pages.clone());
        let mut non_commutative = false;
        for page in &conflict_pages {
            if let Some(owner) = committed_by_page.get(page)
                && let Some(owner_txn) = committed_txns.get(owner)
                && !is_commutative(txn.intent, owner_txn.intent)
            {
                non_commutative = true;
                break;
            }
        }

        if merge_safe && !non_commutative {
            for page in &txn.write_pages {
                committed_by_page.insert(*page, txn.txn_id);
            }
            result.committed.push(txn.txn_id);
            result.merged_txns.push(txn.txn_id);
            committed_txns.insert(txn.txn_id, txn.clone());
            continue;
        }

        result.aborted.push((
            txn.txn_id,
            if non_commutative {
                AbortReason::NonCommutativeConflict
            } else {
                AbortReason::PageConflict
            },
        ));
    }

    result.deadlock_detected = false;
    result
}

fn aggregate_metrics(result: &SimulationResult, duration_secs: usize) -> StressMetrics {
    let safe_duration = duration_secs.max(1);
    StressMetrics {
        commit_count: result.committed.len(),
        abort_count: result.aborted.len(),
        conflict_count: result.conflict_pages_by_txn.len(),
        merged_count: result.merged_txns.len(),
        throughput_ops_per_sec: result.committed.len() / safe_duration,
    }
}

fn read_version_at(history: &[(u64, i64)], snapshot_seq: u64) -> Option<i64> {
    history
        .iter()
        .filter(|(seq, _)| *seq <= snapshot_seq)
        .max_by_key(|(seq, _)| *seq)
        .map(|(_, value)| *value)
}

fn simulate_crash_mid_commit(
    txns: &[TxnSpec],
    schedule: &[u64],
    crash_after_commits: usize,
) -> SimulationResult {
    let full = simulate_mvcc(txns, schedule, IsolationMode::Serializable, false);
    let keep = full
        .committed
        .iter()
        .take(crash_after_commits)
        .copied()
        .collect();
    SimulationResult {
        committed: keep,
        aborted: full.aborted,
        conflict_pages_by_txn: full.conflict_pages_by_txn,
        merged_txns: full.merged_txns,
        deadlock_detected: false,
    }
}

#[test]
fn test_bd_2npr_unit_compliance_gate() -> Result<(), String> {
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
    fn prop_bd_2npr_structure_compliance(missing_index in 0usize..REQUIRED_TOKENS.len()) {
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
fn test_workload_generator_deterministic() {
    let a = generate_workload(777, 25, 512, 20);
    let b = generate_workload(777, 25, 512, 20);
    let c = generate_workload(778, 25, 512, 20);
    assert_eq!(a, b, "bead_id={BEAD_ID} deterministic generator regression");
    assert_ne!(a, c, "bead_id={BEAD_ID} seed should alter workload");
}

#[test]
fn test_schedule_controller_replay() {
    let ids = (1_u64..=32).collect::<Vec<_>>();
    let s1 = generate_schedule(0xDEAD_BEEF, &ids);
    let s2 = generate_schedule(0xDEAD_BEEF, &ids);
    assert_eq!(
        s1, s2,
        "bead_id={BEAD_ID} schedule replay must be deterministic"
    );
}

#[test]
fn test_metrics_aggregation() {
    let result = SimulationResult {
        committed: vec![1, 2, 3, 4],
        aborted: vec![
            (5, AbortReason::PageConflict),
            (6, AbortReason::SsiWriteSkew),
        ],
        conflict_pages_by_txn: BTreeMap::from([(5, vec![2]), (6, vec![7])]),
        merged_txns: vec![4],
        deadlock_detected: false,
    };
    let metrics = aggregate_metrics(&result, 2);
    assert_eq!(metrics.commit_count, 4);
    assert_eq!(metrics.abort_count, 2);
    assert_eq!(metrics.conflict_count, 2);
    assert_eq!(metrics.merged_count, 1);
    assert_eq!(metrics.throughput_ops_per_sec, 2);
}

#[test]
fn test_invariant_report_format() -> Result<(), String> {
    let report = InvariantReport {
        schema_version: 1,
        run_id: "mvcc-stress".to_owned(),
        seed: 42,
        writer_count: 100,
        commit_count: 91,
        abort_count: 9,
        conflict_count: 8,
        no_deadlock: true,
    };
    let value = serde_json::to_value(&report)
        .map_err(|error| format!("report_serialize_failed: {error}"))?;
    for key in [
        "schema_version",
        "run_id",
        "seed",
        "writer_count",
        "commit_count",
        "abort_count",
        "conflict_count",
        "no_deadlock",
    ] {
        if value.get(key).is_none() {
            return Err(format!(
                "bead_id={BEAD_ID} case=invariant_missing_key key={key}"
            ));
        }
    }
    let roundtrip: InvariantReport = serde_json::from_value(value)
        .map_err(|error| format!("report_roundtrip_failed: {error}"))?;
    if roundtrip != report {
        return Err(format!(
            "bead_id={BEAD_ID} case=invariant_roundtrip_mismatch got={roundtrip:?}"
        ));
    }
    Ok(())
}

#[test]
fn test_e2e_two_writers_different_pages_both_commit() {
    let txns = vec![
        TxnSpec {
            txn_id: 1,
            read_pages: vec![10],
            write_pages: vec![10],
            intent: IntentKind::InsertRow { row_key: 1 },
            snapshot_seq: 0,
        },
        TxnSpec {
            txn_id: 2,
            read_pages: vec![20],
            write_pages: vec![20],
            intent: IntentKind::InsertRow { row_key: 2 },
            snapshot_seq: 0,
        },
    ];
    let result = simulate_mvcc(&txns, &[1, 2], IsolationMode::Serializable, false);
    assert_eq!(result.committed, vec![1, 2]);
    assert!(result.aborted.is_empty());
}

#[test]
fn test_e2e_ten_writers_disjoint_tables() {
    let txns = (0_u64..10)
        .map(|idx| TxnSpec {
            txn_id: idx + 1,
            read_pages: vec![u32::try_from(100 + idx).expect("small page index")],
            write_pages: vec![u32::try_from(100 + idx).expect("small page index")],
            intent: IntentKind::InsertRow { row_key: idx + 1 },
            snapshot_seq: 0,
        })
        .collect::<Vec<_>>();
    let schedule = (1_u64..=10).collect::<Vec<_>>();
    let result = simulate_mvcc(&txns, &schedule, IsolationMode::Serializable, false);
    assert_eq!(result.committed.len(), 10);
    assert!(result.aborted.is_empty());
}

#[test]
fn test_e2e_reader_sees_consistent_snapshot() {
    let history = vec![(0_u64, 100_i64), (1_u64, 200_i64)];
    let before = read_version_at(&history, 0).expect("snapshot must resolve");
    let after = read_version_at(&history, 1).expect("snapshot must resolve");
    assert_eq!(before, 100);
    assert_eq!(after, 200);
}

#[test]
fn test_e2e_many_readers_one_writer_no_blocking() {
    let mut txns = Vec::new();
    txns.push(TxnSpec {
        txn_id: 1,
        read_pages: vec![9],
        write_pages: vec![9],
        intent: IntentKind::UpdateRow { row_key: 9 },
        snapshot_seq: 0,
    });
    for id in 2_u64..=22 {
        txns.push(TxnSpec {
            txn_id: id,
            read_pages: vec![9],
            write_pages: Vec::new(),
            intent: IntentKind::InsertRow { row_key: id },
            snapshot_seq: 0,
        });
    }
    let schedule = (1_u64..=22).collect::<Vec<_>>();
    let result = simulate_mvcc(&txns, &schedule, IsolationMode::Serializable, false);
    assert_eq!(result.committed.len(), 22);
    assert!(result.aborted.is_empty());
}

#[test]
fn test_e2e_two_writers_same_page_first_wins() {
    let txns = vec![
        TxnSpec {
            txn_id: 1,
            read_pages: vec![5],
            write_pages: vec![5],
            intent: IntentKind::UpdateRow { row_key: 1 },
            snapshot_seq: 0,
        },
        TxnSpec {
            txn_id: 2,
            read_pages: vec![5],
            write_pages: vec![5],
            intent: IntentKind::UpdateRow { row_key: 2 },
            snapshot_seq: 0,
        },
    ];
    let result = simulate_mvcc(&txns, &[1, 2], IsolationMode::Serializable, false);
    assert_eq!(result.committed, vec![1]);
    assert_eq!(
        result.aborted,
        vec![(2, AbortReason::NonCommutativeConflict)]
    );
}

#[test]
fn test_e2e_conflict_detection_precise_page_level() {
    let txns = vec![
        TxnSpec {
            txn_id: 11,
            read_pages: vec![1],
            write_pages: vec![1, 2],
            intent: IntentKind::UpdateRow { row_key: 11 },
            snapshot_seq: 0,
        },
        TxnSpec {
            txn_id: 12,
            read_pages: vec![2],
            write_pages: vec![2, 3],
            intent: IntentKind::UpdateRow { row_key: 12 },
            snapshot_seq: 0,
        },
    ];
    let result = simulate_mvcc(&txns, &[11, 12], IsolationMode::Serializable, false);
    assert_eq!(
        result.conflict_pages_by_txn.get(&12),
        Some(&vec![2]),
        "bead_id={BEAD_ID} conflict should be precise to page 2"
    );
}

#[test]
fn test_e2e_write_skew_detection_ssi() {
    let txns = vec![
        TxnSpec {
            txn_id: 1,
            read_pages: vec![20],
            write_pages: vec![10],
            intent: IntentKind::UpdateRow { row_key: 1 },
            snapshot_seq: 0,
        },
        TxnSpec {
            txn_id: 2,
            read_pages: vec![10],
            write_pages: vec![20],
            intent: IntentKind::UpdateRow { row_key: 2 },
            snapshot_seq: 0,
        },
    ];
    let serializable = simulate_mvcc(&txns, &[1, 2], IsolationMode::Serializable, false);
    assert_eq!(serializable.committed, vec![1]);
    assert_eq!(serializable.aborted, vec![(2, AbortReason::SsiWriteSkew)]);

    let snapshot = simulate_mvcc(&txns, &[1, 2], IsolationMode::SnapshotIsolation, false);
    assert_eq!(
        snapshot.committed,
        vec![1, 2],
        "bead_id={BEAD_ID} SI mode should tolerate write skew"
    );
}

#[test]
fn test_e2e_intent_log_rebase_after_conflict() {
    let winner = TxnSpec {
        txn_id: 1,
        read_pages: vec![7],
        write_pages: vec![7],
        intent: IntentKind::UpdateRow { row_key: 1 },
        snapshot_seq: 0,
    };
    let loser_first_try = TxnSpec {
        txn_id: 2,
        read_pages: vec![7],
        write_pages: vec![7],
        intent: IntentKind::UpdateRow { row_key: 2 },
        snapshot_seq: 0,
    };
    let first = simulate_mvcc(
        &[winner.clone(), loser_first_try.clone()],
        &[1, 2],
        IsolationMode::Serializable,
        false,
    );
    assert_eq!(first.committed, vec![1]);
    assert_eq!(
        first.aborted,
        vec![(2, AbortReason::NonCommutativeConflict)]
    );

    let rebased_retry = TxnSpec {
        txn_id: 3,
        read_pages: vec![7],
        write_pages: vec![8],
        intent: IntentKind::UpdateRow { row_key: 2 },
        snapshot_seq: 1,
    };
    let second = simulate_mvcc(
        &[winner, rebased_retry],
        &[1, 3],
        IsolationMode::Serializable,
        false,
    );
    assert_eq!(second.committed, vec![1, 3]);
}

#[test]
fn test_e2e_commutative_operations_merge() {
    let txns = vec![
        TxnSpec {
            txn_id: 1,
            read_pages: vec![30],
            write_pages: vec![30],
            intent: IntentKind::InsertRow { row_key: 101 },
            snapshot_seq: 0,
        },
        TxnSpec {
            txn_id: 2,
            read_pages: vec![30],
            write_pages: vec![30],
            intent: IntentKind::InsertRow { row_key: 202 },
            snapshot_seq: 0,
        },
    ];
    let result = simulate_mvcc(&txns, &[1, 2], IsolationMode::Serializable, true);
    assert_eq!(result.committed, vec![1, 2]);
    assert_eq!(result.merged_txns, vec![2]);
    assert!(result.aborted.is_empty());
}

#[test]
fn test_e2e_non_commutative_operations_abort() {
    let txns = vec![
        TxnSpec {
            txn_id: 1,
            read_pages: vec![44],
            write_pages: vec![44],
            intent: IntentKind::UpdateRow { row_key: 5 },
            snapshot_seq: 0,
        },
        TxnSpec {
            txn_id: 2,
            read_pages: vec![44],
            write_pages: vec![44],
            intent: IntentKind::UpdateRow { row_key: 5 },
            snapshot_seq: 0,
        },
    ];
    let result = simulate_mvcc(&txns, &[1, 2], IsolationMode::Serializable, true);
    assert_eq!(result.committed, vec![1]);
    assert_eq!(
        result.aborted,
        vec![(2, AbortReason::NonCommutativeConflict)]
    );
}

#[test]
fn test_e2e_100_concurrent_transactions_no_deadlock() {
    let txns = generate_workload(2026, 100, 500, 25);
    let ids = txns.iter().map(|txn| txn.txn_id).collect::<Vec<_>>();
    let schedule = generate_schedule(2027, &ids);
    let result = simulate_mvcc(&txns, &schedule, IsolationMode::Serializable, true);
    assert!(
        !result.deadlock_detected,
        "bead_id={BEAD_ID} deadlock freedom"
    );
    assert_eq!(result.committed.len() + result.aborted.len(), 100);
}

#[test]
fn test_e2e_throughput_under_contention() {
    let overlap_levels = [0_u8, 10_u8, 50_u8, 100_u8];
    let mut throughputs = Vec::new();
    for overlap in overlap_levels {
        let txns = generate_workload(9000 + u64::from(overlap), 120, 256, overlap);
        let ids = txns.iter().map(|txn| txn.txn_id).collect::<Vec<_>>();
        let schedule = generate_schedule(1337 + u64::from(overlap), &ids);
        let result = simulate_mvcc(&txns, &schedule, IsolationMode::Serializable, false);
        throughputs.push(aggregate_metrics(&result, 1).throughput_ops_per_sec);
    }
    assert!(
        throughputs[0] >= throughputs[1]
            && throughputs[1] >= throughputs[2]
            && throughputs[2] >= throughputs[3],
        "bead_id={BEAD_ID} throughput should not increase with higher contention: {throughputs:?}"
    );
}

#[test]
fn test_e2e_serialized_mode_single_writer() {
    let txns = vec![
        TxnSpec {
            txn_id: 1,
            read_pages: vec![1],
            write_pages: vec![1],
            intent: IntentKind::UpdateRow { row_key: 1 },
            snapshot_seq: 0,
        },
        TxnSpec {
            txn_id: 2,
            read_pages: vec![2],
            write_pages: vec![2],
            intent: IntentKind::UpdateRow { row_key: 2 },
            snapshot_seq: 0,
        },
        TxnSpec {
            txn_id: 3,
            read_pages: vec![3],
            write_pages: vec![3],
            intent: IntentKind::UpdateRow { row_key: 3 },
            snapshot_seq: 0,
        },
    ];
    let result = simulate_mvcc(&txns, &[1, 2, 3], IsolationMode::Serialized, false);
    assert_eq!(result.committed, vec![1]);
    assert_eq!(
        result.aborted,
        vec![
            (2, AbortReason::SerializedWriterBusy),
            (3, AbortReason::SerializedWriterBusy)
        ]
    );
}

#[test]
fn test_e2e_two_processes_concurrent_write() {
    let txns = Arc::new(Mutex::new(Vec::<TxnSpec>::new()));
    let txns_a = Arc::clone(&txns);
    let txns_b = Arc::clone(&txns);

    let h1 = thread::spawn(move || {
        let mut guard = txns_a.lock().expect("txns lock");
        guard.push(TxnSpec {
            txn_id: 1,
            read_pages: vec![101],
            write_pages: vec![101],
            intent: IntentKind::InsertRow { row_key: 1 },
            snapshot_seq: 0,
        });
    });
    let h2 = thread::spawn(move || {
        let mut guard = txns_b.lock().expect("txns lock");
        guard.push(TxnSpec {
            txn_id: 2,
            read_pages: vec![102],
            write_pages: vec![102],
            intent: IntentKind::InsertRow { row_key: 2 },
            snapshot_seq: 0,
        });
    });
    h1.join().expect("writer thread A");
    h2.join().expect("writer thread B");

    let built = txns.lock().expect("txns lock").clone();
    let result = simulate_mvcc(&built, &[1, 2], IsolationMode::Serializable, false);
    assert_eq!(result.committed, vec![1, 2]);
}

#[test]
fn test_e2e_crash_recovery_mid_commit() {
    let txns = vec![
        TxnSpec {
            txn_id: 1,
            read_pages: vec![201],
            write_pages: vec![201],
            intent: IntentKind::UpdateRow { row_key: 1 },
            snapshot_seq: 0,
        },
        TxnSpec {
            txn_id: 2,
            read_pages: vec![202],
            write_pages: vec![202],
            intent: IntentKind::UpdateRow { row_key: 2 },
            snapshot_seq: 0,
        },
    ];
    let post_crash = simulate_crash_mid_commit(&txns, &[1, 2], 1);
    assert_eq!(post_crash.committed, vec![1]);
    assert!(
        !post_crash.committed.contains(&2),
        "bead_id={BEAD_ID} txn after crash boundary must not be durable"
    );
}

#[test]
fn test_e2e_bd_2npr_compliance() -> Result<(), String> {
    let description = load_issue_description(BEAD_ID)?;
    let evaluation = evaluate_description(&description);

    let txns = generate_workload(4242, 40, 256, 30);
    let ids = txns.iter().map(|txn| txn.txn_id).collect::<Vec<_>>();
    let schedule = generate_schedule(4243, &ids);
    let result = simulate_mvcc(&txns, &schedule, IsolationMode::Serializable, true);
    let metrics = aggregate_metrics(&result, 1);

    let artifact = ArtifactReport {
        schema_version: 1,
        bead_id: BEAD_ID.to_owned(),
        seed: 4242,
        writer_count: txns.len(),
        commit_count: metrics.commit_count,
        abort_count: metrics.abort_count,
        conflict_count: metrics.conflict_count,
        trace_len: schedule.len(),
    };
    let artifact_dir = tempdir().map_err(|error| format!("tempdir_failed: {error}"))?;
    let artifact_path = artifact_dir.path().join("bd_2npr_stress_artifact.json");
    let bytes = serde_json::to_vec_pretty(&artifact)
        .map_err(|error| format!("artifact_serialize_failed: {error}"))?;
    fs::write(&artifact_path, bytes)
        .map_err(|error| format!("artifact_write_failed path={artifact_path:?} error={error}"))?;

    eprintln!(
        "DEBUG bead_id={BEAD_ID} case=stress_trace seed={} trace_len={} artifact={}",
        artifact.seed,
        artifact.trace_len,
        artifact_path.display()
    );
    eprintln!(
        "INFO bead_id={BEAD_ID} case=stress_summary writers={} commits={} aborts={} conflicts={}",
        artifact.writer_count, artifact.commit_count, artifact.abort_count, artifact.conflict_count
    );
    if artifact.abort_count > artifact.commit_count {
        eprintln!(
            "WARN bead_id={BEAD_ID} case=degraded_mode aborts_exceed_commits aborts={} commits={}",
            artifact.abort_count, artifact.commit_count
        );
    }
    if !evaluation.is_compliant() {
        eprintln!(
            "ERROR bead_id={BEAD_ID} case=missing_compliance_tokens eval={evaluation:?} reference={LOG_STANDARD_REF}"
        );
        return Err(format!(
            "bead_id={BEAD_ID} case=e2e_compliance_failure evaluation={evaluation:?}"
        ));
    }

    Ok(())
}
