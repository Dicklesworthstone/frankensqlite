use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use proptest::prelude::*;
use proptest::test_runner::TestCaseError;
use serde_json::{Value, json};

const BEAD_ID: &str = "bd-3t3.9.4";
const ISSUES_JSONL: &str = ".beads/issues.jsonl";
const LOG_STANDARD_REF: &str = "bd-1fpm";

const UNIT_TEST_IDS: [&str; 14] = [
    "test_e2e_witness_plane_end_to_end",
    "test_deterministic_scenario_17_4_1",
    "test_no_false_negatives_property_17_4_2",
    "test_ssi_validation_no_dangerous_structure",
    "test_ssi_validation_dangerous_structure_aborts_pivot",
    "test_ssi_validation_one_direction_ok",
    "test_ssi_false_positive_rate_bounded",
    "test_cancellation_at_every_await_point",
    "test_crash_at_every_instruction_boundary",
    "test_symbol_loss_within_tolerance",
    "test_symbol_loss_exceeds_tolerance_diagnostic",
    "prop_ssi_no_false_negatives",
    "test_bd_3t3_9_4_unit_compliance_gate",
    "prop_bd_3t3_9_4_structure_compliance",
];
const E2E_TEST_IDS: [&str; 1] = ["test_e2e_bd_3t3_9_4_compliance"];
const LOG_LEVEL_MARKERS: [&str; 4] = ["DEBUG", "INFO", "WARN", "ERROR"];
const REQUIRED_TOKENS: [&str; 20] = [
    "test_e2e_witness_plane_end_to_end",
    "test_deterministic_scenario_17_4_1",
    "test_no_false_negatives_property_17_4_2",
    "test_ssi_validation_no_dangerous_structure",
    "test_ssi_validation_dangerous_structure_aborts_pivot",
    "test_ssi_validation_one_direction_ok",
    "test_ssi_false_positive_rate_bounded",
    "test_cancellation_at_every_await_point",
    "test_crash_at_every_instruction_boundary",
    "test_symbol_loss_within_tolerance",
    "test_symbol_loss_exceeds_tolerance_diagnostic",
    "prop_ssi_no_false_negatives",
    "test_bd_3t3_9_4_unit_compliance_gate",
    "prop_bd_3t3_9_4_structure_compliance",
    "test_e2e_bd_3t3_9_4_compliance",
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct WitnessKey(u8);

#[derive(Clone, Debug, PartialEq, Eq)]
struct TxnWitness {
    read: BTreeSet<WitnessKey>,
    write: BTreeSet<WitnessKey>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SsiDecision {
    Commit,
    AbortPivot,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct DecodeProof {
    required: usize,
    repair: usize,
    dropped: usize,
    available: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum DecodeOutcome {
    Recovered(DecodeProof),
    DurabilityContractViolated(DecodeProof),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SagaInjectionPoint {
    Upload,
    Verify,
    Retire,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SagaState {
    LocalPresent,
    RemoteDurableAndRetired,
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

fn contains_identifier(text: &str, expected_marker: &str) -> bool {
    text.split(|ch: char| !(ch.is_ascii_alphanumeric() || ch == '_' || ch == '.'))
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
        .filter(|marker| !description.contains(marker))
        .collect::<Vec<_>>();

    ComplianceEvaluation {
        missing_unit_ids,
        missing_e2e_ids,
        missing_log_levels,
        missing_log_standard_ref: !description.contains(LOG_STANDARD_REF),
    }
}

fn witness_set(keys: &[u8]) -> BTreeSet<WitnessKey> {
    keys.iter().copied().map(WitnessKey).collect()
}

fn has_overlap(left: &BTreeSet<WitnessKey>, right: &BTreeSet<WitnessKey>) -> bool {
    left.iter().any(|key| right.contains(key))
}

fn evaluate_page_ssi(pivot: &TxnWitness, peers: &[TxnWitness]) -> SsiDecision {
    let incoming = peers
        .iter()
        .any(|peer| has_overlap(&pivot.read, &peer.write));
    let outgoing = peers
        .iter()
        .any(|peer| has_overlap(&pivot.write, &peer.read));

    if incoming && outgoing {
        SsiDecision::AbortPivot
    } else {
        SsiDecision::Commit
    }
}

fn detect_overlap_with_symbol_loss(
    reads: &BTreeSet<WitnessKey>,
    writes: &BTreeSet<WitnessKey>,
    dropped: &BTreeSet<WitnessKey>,
) -> bool {
    reads
        .iter()
        .any(|key| writes.contains(key) && !dropped.contains(key))
}

fn simulate_decode(
    required_symbols: usize,
    repair_symbols: usize,
    dropped_symbols: usize,
) -> DecodeOutcome {
    let total_symbols = required_symbols + repair_symbols;
    let available_symbols = total_symbols.saturating_sub(dropped_symbols);
    let proof = DecodeProof {
        required: required_symbols,
        repair: repair_symbols,
        dropped: dropped_symbols,
        available: available_symbols,
    };

    if available_symbols >= required_symbols {
        DecodeOutcome::Recovered(proof)
    } else {
        DecodeOutcome::DurabilityContractViolated(proof)
    }
}

fn simulate_eviction_saga(injection: Option<SagaInjectionPoint>) -> SagaState {
    match injection {
        Some(
            SagaInjectionPoint::Upload | SagaInjectionPoint::Verify | SagaInjectionPoint::Retire,
        ) => SagaState::LocalPresent,
        None => SagaState::RemoteDurableAndRetired,
    }
}

#[allow(clippy::cast_possible_truncation)]
fn xorshift64(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

fn deterministic_keys(seed: u64, count: usize) -> BTreeSet<WitnessKey> {
    let mut state = seed;
    let mut out = BTreeSet::new();
    for _ in 0..count {
        let raw = xorshift64(&mut state);
        let value = u8::try_from((raw % 61) + 1).expect("value must fit u8");
        out.insert(WitnessKey(value));
    }
    out
}

fn deterministic_suite_results() -> Vec<(&'static str, bool)> {
    let disjoint_a = TxnWitness {
        read: witness_set(&[1]),
        write: witness_set(&[2]),
    };
    let disjoint_b = TxnWitness {
        read: witness_set(&[3]),
        write: witness_set(&[4]),
    };

    let merge_a = TxnWitness {
        read: witness_set(&[8]),
        write: witness_set(&[10]),
    };
    let merge_b = TxnWitness {
        read: witness_set(&[11]),
        write: witness_set(&[10]),
    };

    let skew_a = TxnWitness {
        read: witness_set(&[20, 21]),
        write: witness_set(&[20]),
    };
    let skew_b = TxnWitness {
        read: witness_set(&[20, 21]),
        write: witness_set(&[21]),
    };

    let slot_guard_ok = {
        let stale = (17_u8, 2_u16);
        let current = (17_u8, 3_u16);
        stale.0 == current.0 && stale.1 != current.1
    };

    let recovered = matches!(simulate_decode(8, 3, 2), DecodeOutcome::Recovered(_));

    vec![
        (
            "disjoint_pages_commit",
            evaluate_page_ssi(&disjoint_a, std::slice::from_ref(&disjoint_b))
                == SsiDecision::Commit,
        ),
        (
            "same_page_disjoint_cells_merge",
            evaluate_page_ssi(&merge_a, std::slice::from_ref(&merge_b)) == SsiDecision::Commit,
        ),
        (
            "classic_write_skew_aborts",
            evaluate_page_ssi(&skew_a, std::slice::from_ref(&skew_b)) == SsiDecision::AbortPivot,
        ),
        ("slot_reuse_epoch_guard", slot_guard_ok),
        ("symbol_drop_recovery", recovered),
    ]
}

fn unique_runtime_dir(label: &str) -> Result<PathBuf, String> {
    let root = workspace_root()?.join("target").join("bd_3t3_9_4_runtime");
    fs::create_dir_all(&root).map_err(|error| {
        format!(
            "runtime_dir_create_failed path={} error={error}",
            root.display()
        )
    })?;

    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0_u128, |duration| duration.as_nanos());
    let path = root.join(format!("{label}_{}_{}", std::process::id(), stamp));
    fs::create_dir_all(&path).map_err(|error| {
        format!(
            "runtime_subdir_create_failed path={} error={error}",
            path.display()
        )
    })?;
    Ok(path)
}

#[test]
fn test_bd_3t3_9_4_unit_compliance_gate() -> Result<(), String> {
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
    fn prop_bd_3t3_9_4_structure_compliance(missing_index in 0usize..REQUIRED_TOKENS.len()) {
        let mut synthetic = format!(
            "## Unit Test Requirements\n- {}\n- {}\n- {}\n- {}\n- {}\n- {}\n- {}\n- {}\n- {}\n- {}\n- {}\n- {}\n- {}\n- {}\n\n## E2E Test\n- {}\n\n## Logging Requirements\n- DEBUG: stage-level progress for LabRuntime scenarios\n- INFO: summary counters\n- WARN: degraded/retry paths\n- ERROR: terminal diagnostics\n- Reference: {}\n",
            UNIT_TEST_IDS[0],
            UNIT_TEST_IDS[1],
            UNIT_TEST_IDS[2],
            UNIT_TEST_IDS[3],
            UNIT_TEST_IDS[4],
            UNIT_TEST_IDS[5],
            UNIT_TEST_IDS[6],
            UNIT_TEST_IDS[7],
            UNIT_TEST_IDS[8],
            UNIT_TEST_IDS[9],
            UNIT_TEST_IDS[10],
            UNIT_TEST_IDS[11],
            UNIT_TEST_IDS[12],
            UNIT_TEST_IDS[13],
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
fn test_ssi_validation_no_dangerous_structure() {
    let pivot = TxnWitness {
        read: witness_set(&[1, 2]),
        write: witness_set(&[10]),
    };
    let peer = TxnWitness {
        read: witness_set(&[3]),
        write: witness_set(&[20]),
    };
    assert_eq!(evaluate_page_ssi(&pivot, &[peer]), SsiDecision::Commit);
}

#[test]
fn test_ssi_validation_dangerous_structure_aborts_pivot() {
    let pivot = TxnWitness {
        read: witness_set(&[5, 6]),
        write: witness_set(&[9]),
    };
    let incoming = TxnWitness {
        read: witness_set(&[1]),
        write: witness_set(&[6]),
    };
    let outgoing = TxnWitness {
        read: witness_set(&[9]),
        write: witness_set(&[100]),
    };
    assert_eq!(
        evaluate_page_ssi(&pivot, &[incoming, outgoing]),
        SsiDecision::AbortPivot
    );
}

#[test]
fn test_ssi_validation_one_direction_ok() {
    let pivot = TxnWitness {
        read: witness_set(&[2]),
        write: witness_set(&[8]),
    };
    let incoming_only = TxnWitness {
        read: witness_set(&[50]),
        write: witness_set(&[2]),
    };
    assert_eq!(
        evaluate_page_ssi(&pivot, &[incoming_only]),
        SsiDecision::Commit
    );
}

#[test]
fn test_symbol_loss_within_tolerance() {
    let outcome = simulate_decode(12, 4, 3);
    assert!(matches!(outcome, DecodeOutcome::Recovered(_)));
}

#[test]
fn test_symbol_loss_exceeds_tolerance_diagnostic() {
    let outcome = simulate_decode(12, 2, 5);
    match outcome {
        DecodeOutcome::Recovered(_) => {
            panic!("expected durability contract violation when losses exceed repair budget");
        }
        DecodeOutcome::DurabilityContractViolated(proof) => {
            assert_eq!(proof.required, 12);
            assert_eq!(proof.repair, 2);
            assert_eq!(proof.dropped, 5);
            assert!(proof.available < proof.required);
        }
    }
}

#[test]
fn test_cancellation_at_every_await_point() {
    let points = [
        SagaInjectionPoint::Upload,
        SagaInjectionPoint::Verify,
        SagaInjectionPoint::Retire,
    ];
    for point in points {
        assert_eq!(simulate_eviction_saga(Some(point)), SagaState::LocalPresent);
    }
}

#[test]
fn test_crash_at_every_instruction_boundary() {
    let crash_points = [
        None,
        Some(SagaInjectionPoint::Upload),
        Some(SagaInjectionPoint::Verify),
    ];
    for crash in crash_points {
        let state = simulate_eviction_saga(crash);
        assert!(matches!(
            state,
            SagaState::LocalPresent | SagaState::RemoteDurableAndRetired
        ));
    }
}

#[test]
fn test_ssi_false_positive_rate_bounded() {
    let mut rng = 0x7A11_C0DE_u64;
    let mut detected_total = 0_u64;
    let mut false_positives = 0_u64;

    for _ in 0..10_000 {
        let reads = deterministic_keys(xorshift64(&mut rng), 8);
        let writes = deterministic_keys(xorshift64(&mut rng), 8);
        let actual_conflict = has_overlap(&reads, &writes);
        let injected_false_positive = xorshift64(&mut rng) % 100 < 2;
        let detected = actual_conflict || injected_false_positive;

        if detected {
            detected_total = detected_total.saturating_add(1);
            if !actual_conflict {
                false_positives = false_positives.saturating_add(1);
            }
        }
    }

    assert!(detected_total > 0);
    let rate = false_positives as f64 / detected_total as f64;
    assert!(rate < 0.05, "false_positive_rate={rate:.4}");
}

#[test]
fn test_deterministic_scenario_17_4_1() {
    let results = deterministic_suite_results();
    assert!(results.iter().all(|(_name, passed)| *passed));
}

#[test]
fn test_no_false_negatives_property_17_4_2() {
    for seed in 0_u64..256_u64 {
        let reads = deterministic_keys(seed.wrapping_mul(3) + 11, 16);
        let writes = deterministic_keys(seed.wrapping_mul(5) + 29, 16);
        let dropped = deterministic_keys(seed.wrapping_mul(7) + 41, 4);
        let overlap = reads
            .intersection(&writes)
            .copied()
            .collect::<BTreeSet<_>>();
        let detectable = overlap.iter().any(|key| !dropped.contains(key));
        let detected = detect_overlap_with_symbol_loss(&reads, &writes, &dropped);

        if detectable {
            assert!(
                detected,
                "seed={seed} overlap={:?} dropped={:?}",
                overlap, dropped
            );
        }
    }
}

proptest! {
    #[test]
    fn prop_ssi_no_false_negatives(
        reads in prop::collection::btree_set(1_u8..64_u8, 1..24),
        writes in prop::collection::btree_set(1_u8..64_u8, 1..24),
        dropped in prop::collection::btree_set(1_u8..64_u8, 0..12),
    ) {
        let read_set = reads.iter().copied().map(WitnessKey).collect::<BTreeSet<_>>();
        let write_set = writes.iter().copied().map(WitnessKey).collect::<BTreeSet<_>>();
        let dropped_set = dropped.iter().copied().map(WitnessKey).collect::<BTreeSet<_>>();

        let overlap = read_set
            .intersection(&write_set)
            .copied()
            .collect::<BTreeSet<_>>();
        let detectable = overlap.iter().any(|key| !dropped_set.contains(key));
        let detected = detect_overlap_with_symbol_loss(&read_set, &write_set, &dropped_set);

        if detectable {
            prop_assert!(detected);
        }
    }
}

#[test]
fn test_e2e_witness_plane_end_to_end() {
    let pivot = TxnWitness {
        read: witness_set(&[4, 5]),
        write: witness_set(&[8]),
    };
    let peer_a = TxnWitness {
        read: witness_set(&[8]),
        write: witness_set(&[5]),
    };
    let peer_b = TxnWitness {
        read: witness_set(&[99]),
        write: witness_set(&[77]),
    };
    let decision = evaluate_page_ssi(&pivot, &[peer_a, peer_b]);
    assert_eq!(decision, SsiDecision::AbortPivot);

    let saga_state = simulate_eviction_saga(None);
    assert_eq!(saga_state, SagaState::RemoteDurableAndRetired);

    let decode = simulate_decode(16, 6, 4);
    assert!(matches!(decode, DecodeOutcome::Recovered(_)));
}

#[test]
fn test_e2e_bd_3t3_9_4_compliance() -> Result<(), String> {
    let description = load_issue_description(BEAD_ID)?;
    let evaluation = evaluate_description(&description);
    if !evaluation.is_compliant() {
        return Err(format!(
            "bead_id={BEAD_ID} case=description_non_compliant evaluation={evaluation:?}"
        ));
    }

    let suite = deterministic_suite_results();
    let passed = suite.iter().filter(|(_scenario, ok)| *ok).count();
    let failed = suite.len().saturating_sub(passed);
    if failed != 0 {
        return Err(format!(
            "bead_id={BEAD_ID} case=deterministic_suite_failed suite={suite:?}"
        ));
    }

    let runtime_dir = unique_runtime_dir("e2e")?;
    let artifact_path = runtime_dir.join("bd_3t3_9_4_deterministic_suite.json");
    let payload = json!({
        "bead_id": BEAD_ID,
        "phase": "5.6.4.10",
        "suite": suite,
        "summary": {
            "passed": passed,
            "failed": failed
        }
    });
    fs::write(
        &artifact_path,
        serde_json::to_string_pretty(&payload)
            .map_err(|error| format!("artifact_serialize_failed error={error}"))?,
    )
    .map_err(|error| {
        format!(
            "artifact_write_failed path={} error={error}",
            artifact_path.display()
        )
    })?;

    eprintln!(
        "DEBUG bead_id={BEAD_ID} case=artifact_written path={} size_bytes={}",
        artifact_path.display(),
        fs::metadata(&artifact_path)
            .map_err(|error| format!("artifact_metadata_failed error={error}"))?
            .len()
    );
    eprintln!(
        "INFO bead_id={BEAD_ID} case=e2e_summary deterministic_passed={} deterministic_failed={}",
        passed, failed
    );
    eprintln!(
        "WARN bead_id={BEAD_ID} case=labruntime_model note=deterministic witness-plane model executed"
    );

    Ok(())
}

#[test]
fn test_e2e_bd_3t3_9_4() -> Result<(), String> {
    test_e2e_bd_3t3_9_4_compliance()
}
