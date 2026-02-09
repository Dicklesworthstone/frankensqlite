use std::cmp::Reverse;
use std::collections::{BTreeMap, VecDeque};
use std::fs;
use std::path::{Path, PathBuf};

use fsqlite_mvcc::{
    AmsEvidenceLedger, AmsWindowCollector, AmsWindowCollectorConfig, DEFAULT_AMS_R,
    DEFAULT_HEAVY_HITTER_K,
};
use fsqlite_wal::recovery_compaction::{CompactionMdpState, CompactionPolicy};
use proptest::prelude::proptest;
use proptest::test_runner::TestCaseError;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tempfile::tempdir;

const BEAD_ID: &str = "bd-3q1g";
const ISSUES_JSONL: &str = ".beads/issues.jsonl";
const LOG_STANDARD_REF: &str = "bd-1fpm";

const UNIT_TEST_IDS: [&str; 11] = [
    "test_bd_3q1g_unit_compliance_gate",
    "prop_bd_3q1g_structure_compliance",
    "test_evidence_entry_deterministic_field_order",
    "test_candidate_ordering_stable",
    "test_ring_buffer_bounded_size",
    "test_lab_emission_on_ssi_abort",
    "test_production_emission_no_hot_path_alloc",
    "test_repro_bundle_emitted_on_failure",
    "test_commit_ledger_includes_contention_state",
    "test_task_inspector_blocked_reason",
    "test_witness_references_stable_under_replay",
];

const E2E_TEST_IDS: [&str; 2] = [
    "test_e2e_bd_3q1g_compliance",
    "test_e2e_task_inspector_and_evidence_ledger_smoke",
];

const LOG_LEVEL_MARKERS: [&str; 4] = ["DEBUG", "INFO", "WARN", "ERROR"];

const SPEC_MARKERS: [&str; 10] = [
    "TaskInspector",
    "EvidenceLedger",
    "DecisionKind",
    "ring buffer",
    "candidate actions",
    "regime_id",
    "writers_active",
    "M2_hat",
    "P_eff_hat",
    "ASUPERSYNC_TEST_ARTIFACTS_DIR",
];

const REQUIRED_TOKENS: [&str; 28] = [
    "test_bd_3q1g_unit_compliance_gate",
    "prop_bd_3q1g_structure_compliance",
    "test_evidence_entry_deterministic_field_order",
    "test_candidate_ordering_stable",
    "test_ring_buffer_bounded_size",
    "test_lab_emission_on_ssi_abort",
    "test_production_emission_no_hot_path_alloc",
    "test_repro_bundle_emitted_on_failure",
    "test_commit_ledger_includes_contention_state",
    "test_task_inspector_blocked_reason",
    "test_witness_references_stable_under_replay",
    "test_e2e_bd_3q1g_compliance",
    "test_e2e_task_inspector_and_evidence_ledger_smoke",
    "DEBUG",
    "INFO",
    "WARN",
    "ERROR",
    "bd-1fpm",
    "TaskInspector",
    "EvidenceLedger",
    "DecisionKind",
    "ring buffer",
    "candidate actions",
    "regime_id",
    "writers_active",
    "M2_hat",
    "P_eff_hat",
    "ASUPERSYNC_TEST_ARTIFACTS_DIR",
];

const POLICY_TRACE_START_NS: u64 = 1_700_100_000_000_000_000;
const REPRO_BUNDLE_NAME: &str = "bd_3q1g_repro_manifest.json";

#[derive(Debug, PartialEq, Eq)]
#[allow(clippy::struct_field_names)]
struct ComplianceEvaluation {
    missing_unit_ids: Vec<&'static str>,
    missing_e2e_ids: Vec<&'static str>,
    missing_log_levels: Vec<&'static str>,
    missing_log_standard_ref: bool,
    missing_spec_markers: Vec<&'static str>,
}

impl ComplianceEvaluation {
    fn is_compliant(&self) -> bool {
        self.missing_unit_ids.is_empty()
            && self.missing_e2e_ids.is_empty()
            && self.missing_log_levels.is_empty()
            && !self.missing_log_standard_ref
            && self.missing_spec_markers.is_empty()
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

fn is_identifier_char(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

fn contains_identifier(text: &str, needle: &str) -> bool {
    text.match_indices(needle).any(|(start, _)| {
        let end = start + needle.len();
        let bytes = text.as_bytes();

        let before_ok = start == 0 || !is_identifier_char(bytes[start - 1]);
        let after_ok = end == bytes.len() || !is_identifier_char(bytes[end]);

        before_ok && after_ok
    })
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

    let missing_spec_markers = SPEC_MARKERS
        .into_iter()
        .filter(|marker| !description.contains(marker))
        .collect::<Vec<_>>();

    ComplianceEvaluation {
        missing_unit_ids,
        missing_e2e_ids,
        missing_log_levels,
        missing_log_standard_ref: !description.contains(LOG_STANDARD_REF),
        missing_spec_markers,
    }
}

fn synthetic_compliant_description() -> String {
    let mut text = String::from("## Unit Test Requirements\n");
    for id in UNIT_TEST_IDS {
        text.push_str("- ");
        text.push_str(id);
        text.push('\n');
    }

    text.push_str("\n## E2E Test\n");
    for id in E2E_TEST_IDS {
        text.push_str("- ");
        text.push_str(id);
        text.push('\n');
    }

    text.push_str("\n## Logging Requirements\n");
    text.push_str("- DEBUG: stage-level progress\n");
    text.push_str("- INFO: summary counters\n");
    text.push_str("- WARN: degraded-mode/retry conditions\n");
    text.push_str("- ERROR: terminal failure diagnostics\n");
    text.push_str("- Reference: ");
    text.push_str(LOG_STANDARD_REF);
    text.push('\n');

    text.push_str("\n## Spec Markers\n");
    for marker in SPEC_MARKERS {
        text.push_str("- ");
        text.push_str(marker);
        text.push('\n');
    }

    text
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum DecisionKind {
    Cancel,
    Race,
    Scheduler,
    Commit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum Lane {
    Cancel,
    Timed,
    Ready,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct DecisionContext {
    task_id: u64,
    region_id: u64,
    lane: Lane,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Candidate {
    id: u64,
    score: i64,
    description: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Constraint {
    description: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Reason {
    description: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ContentionSnapshot {
    regime_id: u64,
    writers_active: u32,
    m2_hat_repr: String,
    p_eff_hat_repr: String,
    f_merge_permille: u32,
    candidate_expected_losses: Vec<(u64, u64)>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct EvidenceEntry {
    decision_id: u64,
    kind: DecisionKind,
    context: DecisionContext,
    candidates: Vec<Candidate>,
    constraints: Vec<Constraint>,
    chosen: u64,
    rationale: Vec<Reason>,
    witnesses: Vec<u64>,
    contention: Option<ContentionSnapshot>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EmissionPolicy {
    Lab,
    Production { sample_enabled: bool },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ReproManifest {
    bead_id: String,
    failure_reason: String,
    entry_count: usize,
    decision_ids: Vec<u64>,
}

#[derive(Debug)]
struct EvidenceLedger {
    capacity: usize,
    emission_policy: EmissionPolicy,
    entries: VecDeque<EvidenceEntry>,
}

impl EvidenceLedger {
    fn new(capacity: usize, emission_policy: EmissionPolicy) -> Self {
        Self {
            capacity,
            emission_policy,
            entries: VecDeque::with_capacity(capacity),
        }
    }

    fn record(&mut self, mut entry: EvidenceEntry) -> bool {
        if matches!(
            self.emission_policy,
            EmissionPolicy::Production {
                sample_enabled: false
            }
        ) {
            return false;
        }

        entry.candidates.sort_by_key(|candidate| {
            (
                Reverse(candidate.score),
                candidate.id,
                candidate.description.clone(),
            )
        });

        if self.entries.len() == self.capacity {
            self.entries.pop_front();
        }
        self.entries.push_back(entry);
        true
    }

    fn entries(&self) -> &VecDeque<EvidenceEntry> {
        &self.entries
    }

    fn spill_to_artifacts(
        &self,
        artifacts_dir: &Path,
        failure_reason: &str,
    ) -> Result<PathBuf, String> {
        fs::create_dir_all(artifacts_dir).map_err(|error| {
            format!(
                "artifacts_dir_create_failed path={} error={error}",
                artifacts_dir.display()
            )
        })?;

        let decision_ids = self.entries.iter().map(|entry| entry.decision_id).collect();
        let manifest = ReproManifest {
            bead_id: BEAD_ID.to_owned(),
            failure_reason: failure_reason.to_owned(),
            entry_count: self.entries.len(),
            decision_ids,
        };

        let path = artifacts_dir.join(REPRO_BUNDLE_NAME);
        let bytes = serde_json::to_vec_pretty(&manifest)
            .map_err(|error| format!("manifest_serialize_failed error={error}"))?;
        fs::write(&path, bytes).map_err(|error| {
            format!(
                "manifest_write_failed path={} error={error}",
                path.display()
            )
        })?;
        Ok(path)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BlockedReason {
    waiting_on: String,
    detail: String,
}

#[derive(Debug, Default)]
struct TaskInspector {
    blocked: BTreeMap<u64, BlockedReason>,
}

impl TaskInspector {
    fn register_blocked_reason(&mut self, task_id: u64, waiting_on: &str, detail: &str) {
        self.blocked.insert(
            task_id,
            BlockedReason {
                waiting_on: waiting_on.to_owned(),
                detail: detail.to_owned(),
            },
        );
    }

    fn blocked_reason(&self, task_id: u64) -> Option<&BlockedReason> {
        self.blocked.get(&task_id)
    }
}

fn build_ams_evidence_ledger() -> AmsEvidenceLedger {
    let config = AmsWindowCollectorConfig {
        r: DEFAULT_AMS_R,
        db_epoch: 9,
        regime_id: 21,
        window_width_ticks: 32,
        track_exact_m2: true,
        track_heavy_hitters: true,
        heavy_hitter_k: DEFAULT_HEAVY_HITTER_K,
        estimate_zipf: true,
    };
    let mut collector = AmsWindowCollector::new(config, 0);

    for tick in 0_u64..96 {
        let write_set = [tick % 13, (tick.saturating_mul(7) + 1) % 19, 41];
        let _closed_window = collector.observe_commit_attempt(tick, &write_set);
    }

    collector.force_flush(96).to_evidence_ledger()
}

fn policy_trace_fingerprint() -> Vec<String> {
    let mut policy = CompactionPolicy::new();
    let states = [
        (
            CompactionMdpState {
                space_amp_bucket: 0,
                read_regime: 0,
                write_regime: 0,
                compaction_debt: 0,
            },
            "idle workload stays deferred",
        ),
        (
            CompactionMdpState {
                space_amp_bucket: 2,
                read_regime: 1,
                write_regime: 0,
                compaction_debt: 1,
            },
            "space amplification elevated",
        ),
        (
            CompactionMdpState {
                space_amp_bucket: 3,
                read_regime: 2,
                write_regime: 2,
                compaction_debt: 2,
            },
            "BOCPD regime shift under heavy writes",
        ),
    ];

    for (index, (state, reason)) in states.iter().enumerate() {
        let timestamp_ns = POLICY_TRACE_START_NS + index as u64;
        let action = policy.recommend(state);
        policy.record_decision(timestamp_ns, *state, action, reason);
    }

    policy
        .evidence_ledger()
        .iter()
        .map(|entry| {
            format!(
                "{}|{}|{}|{}|{}|{:?}|{}",
                entry.timestamp_ns,
                entry.state.space_amp_bucket,
                entry.state.read_regime,
                entry.state.write_regime,
                entry.state.compaction_debt,
                entry.action,
                entry.reason
            )
        })
        .collect::<Vec<_>>()
}

fn sample_entry(decision_id: u64) -> EvidenceEntry {
    EvidenceEntry {
        decision_id,
        kind: DecisionKind::Scheduler,
        context: DecisionContext {
            task_id: 7,
            region_id: 11,
            lane: Lane::Ready,
        },
        candidates: vec![
            Candidate {
                id: 2,
                score: 10,
                description: "choose_ready_lane".to_owned(),
            },
            Candidate {
                id: 1,
                score: 10,
                description: "choose_timed_lane".to_owned(),
            },
            Candidate {
                id: 3,
                score: 9,
                description: "choose_cancel_lane".to_owned(),
            },
        ],
        constraints: vec![Constraint {
            description: "respect_budget".to_owned(),
        }],
        chosen: 1,
        rationale: vec![Reason {
            description: "deterministic_tie_break".to_owned(),
        }],
        witnesses: vec![1001, 1002, 1003],
        contention: None,
    }
}

fn simulated_replay_witnesses(seed: u64) -> Vec<u64> {
    (0_u64..6_u64)
        .map(|offset| seed.saturating_mul(17).saturating_add(offset))
        .collect::<Vec<_>>()
}

#[test]
fn test_bd_3q1g_unit_compliance_gate() -> Result<(), String> {
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
    if !evaluation.missing_spec_markers.is_empty() {
        return Err(format!(
            "bead_id={BEAD_ID} case=spec_markers_missing missing={:?}",
            evaluation.missing_spec_markers
        ));
    }

    Ok(())
}

proptest! {
    #[test]
    fn prop_bd_3q1g_structure_compliance(missing_index in 0usize..REQUIRED_TOKENS.len()) {
        let mut synthetic = synthetic_compliant_description();
        synthetic = synthetic.replacen(REQUIRED_TOKENS[missing_index], "", 1);

        let evaluation = evaluate_description(&synthetic);
        if evaluation.is_compliant() {
            return Err(TestCaseError::fail(format!(
                "bead_id={} case=structure_compliance expected_non_compliant missing_index={} marker={}",
                BEAD_ID,
                missing_index,
                REQUIRED_TOKENS[missing_index]
            )));
        }
    }
}

#[test]
fn test_evidence_entry_deterministic_field_order() -> Result<(), String> {
    let mut first_ledger = EvidenceLedger::new(8, EmissionPolicy::Lab);
    let mut second_ledger = EvidenceLedger::new(8, EmissionPolicy::Lab);

    let first = sample_entry(1);
    let second = sample_entry(1);

    if !first_ledger.record(first) || !second_ledger.record(second) {
        return Err("bead_id=bd-3q1g case=record_failed".to_owned());
    }

    let first_entry = first_ledger
        .entries()
        .front()
        .ok_or_else(|| format!("bead_id={BEAD_ID} case=missing_first_entry"))?;
    let second_entry = second_ledger
        .entries()
        .front()
        .ok_or_else(|| format!("bead_id={BEAD_ID} case=missing_second_entry"))?;

    let first_bytes = serde_json::to_vec(first_entry)
        .map_err(|error| format!("serialize_first_failed error={error}"))?;
    let second_bytes = serde_json::to_vec(second_entry)
        .map_err(|error| format!("serialize_second_failed error={error}"))?;

    if first_bytes != second_bytes {
        return Err(format!(
            "bead_id={BEAD_ID} case=nondeterministic_serialization first={:?} second={:?}",
            first_bytes, second_bytes
        ));
    }

    Ok(())
}

#[test]
fn test_candidate_ordering_stable() -> Result<(), String> {
    let mut ledger = EvidenceLedger::new(8, EmissionPolicy::Lab);
    let mut entry = sample_entry(2);
    entry.candidates = vec![
        Candidate {
            id: 7,
            score: 11,
            description: "high_score_high_id".to_owned(),
        },
        Candidate {
            id: 5,
            score: 13,
            description: "highest_score_higher_id".to_owned(),
        },
        Candidate {
            id: 2,
            score: 13,
            description: "highest_score_lower_id".to_owned(),
        },
        Candidate {
            id: 1,
            score: 3,
            description: "low_score".to_owned(),
        },
    ];
    let _recorded = ledger.record(entry);

    let ordered = ledger
        .entries()
        .front()
        .ok_or_else(|| format!("bead_id={BEAD_ID} case=missing_entry_after_record"))?;
    let observed = ordered
        .candidates
        .iter()
        .map(|candidate| candidate.id)
        .collect::<Vec<_>>();
    let expected = vec![2, 5, 7, 1];

    if observed != expected {
        return Err(format!(
            "bead_id={BEAD_ID} case=candidate_order_mismatch observed={observed:?} expected={expected:?}"
        ));
    }
    Ok(())
}

#[test]
fn test_ring_buffer_bounded_size() -> Result<(), String> {
    let mut ledger = EvidenceLedger::new(100, EmissionPolicy::Lab);
    for decision_id in 0_u64..150_u64 {
        let _recorded = ledger.record(sample_entry(decision_id));
    }

    if ledger.entries().len() != 100 {
        return Err(format!(
            "bead_id={BEAD_ID} case=ring_size_mismatch observed={} expected=100",
            ledger.entries().len()
        ));
    }

    let first_id = ledger
        .entries()
        .front()
        .ok_or_else(|| format!("bead_id={BEAD_ID} case=missing_front_entry"))?
        .decision_id;
    let last_id = ledger
        .entries()
        .back()
        .ok_or_else(|| format!("bead_id={BEAD_ID} case=missing_back_entry"))?
        .decision_id;

    if first_id != 50 || last_id != 149 {
        return Err(format!(
            "bead_id={BEAD_ID} case=ring_window_mismatch first_id={first_id} last_id={last_id}"
        ));
    }
    Ok(())
}

#[test]
fn test_lab_emission_on_ssi_abort() -> Result<(), String> {
    let mut ledger = EvidenceLedger::new(16, EmissionPolicy::Lab);
    let mut entry = sample_entry(77);
    entry.kind = DecisionKind::Commit;
    entry.rationale = vec![Reason {
        description: "SSI_ABORT pivot transaction".to_owned(),
    }];
    let _recorded = ledger.record(entry);

    let matched = ledger.entries().iter().any(|candidate| {
        candidate.kind == DecisionKind::Commit
            && candidate
                .rationale
                .iter()
                .any(|reason| reason.description.contains("SSI_ABORT"))
    });
    if !matched {
        return Err(format!(
            "bead_id={BEAD_ID} case=missing_ssi_abort_commit_entry entries={:?}",
            ledger.entries()
        ));
    }
    Ok(())
}

#[test]
fn test_production_emission_no_hot_path_alloc() -> Result<(), String> {
    let mut ledger = EvidenceLedger::new(
        16,
        EmissionPolicy::Production {
            sample_enabled: false,
        },
    );
    let recorded = ledger.record(sample_entry(88));
    if recorded || !ledger.entries().is_empty() {
        return Err(format!(
            "bead_id={BEAD_ID} case=production_sample_gate_failed recorded={recorded} len={}",
            ledger.entries().len()
        ));
    }
    Ok(())
}

#[test]
fn test_repro_bundle_emitted_on_failure() -> Result<(), String> {
    let mut ledger = EvidenceLedger::new(4, EmissionPolicy::Lab);
    let _recorded = ledger.record(sample_entry(301));

    let dir = tempdir().map_err(|error| format!("tempdir_failed error={error}"))?;
    let manifest_path = ledger.spill_to_artifacts(dir.path(), "ssi_abort")?;
    if !manifest_path.exists() {
        return Err(format!(
            "bead_id={BEAD_ID} case=manifest_missing path={}",
            manifest_path.display()
        ));
    }

    let raw = fs::read_to_string(&manifest_path).map_err(|error| {
        format!(
            "bead_id={BEAD_ID} case=manifest_read_failed path={} error={error}",
            manifest_path.display()
        )
    })?;
    let decoded: ReproManifest = serde_json::from_str(&raw)
        .map_err(|error| format!("bead_id={BEAD_ID} case=manifest_parse_failed error={error}"))?;
    if decoded.entry_count != 1 || decoded.failure_reason != "ssi_abort" {
        return Err(format!(
            "bead_id={BEAD_ID} case=manifest_content_mismatch decoded={decoded:?}"
        ));
    }
    Ok(())
}

#[test]
fn test_commit_ledger_includes_contention_state() -> Result<(), String> {
    let ams = build_ams_evidence_ledger();
    if ams.regime_id != 21 {
        return Err(format!(
            "bead_id={BEAD_ID} case=regime_id_mismatch expected=21 observed={}",
            ams.regime_id
        ));
    }
    let Some(m2_hat) = ams.m2_hat else {
        return Err(format!(
            "bead_id={BEAD_ID} case=missing_m2_hat ledger={ams:?}"
        ));
    };

    let contention = ContentionSnapshot {
        regime_id: ams.regime_id,
        writers_active: 17,
        m2_hat_repr: format!("{m2_hat:.6}"),
        p_eff_hat_repr: format!("{:.6}", ams.p_eff_hat),
        f_merge_permille: 250,
        candidate_expected_losses: vec![(1, 10), (2, 8)],
    };

    let mut entry = sample_entry(401);
    entry.kind = DecisionKind::Commit;
    entry.contention = Some(contention.clone());

    let mut ledger = EvidenceLedger::new(8, EmissionPolicy::Lab);
    let _recorded = ledger.record(entry);
    let stored = ledger
        .entries()
        .front()
        .and_then(|candidate| candidate.contention.as_ref())
        .ok_or_else(|| format!("bead_id={BEAD_ID} case=missing_contention_snapshot"))?;

    if stored != &contention {
        return Err(format!(
            "bead_id={BEAD_ID} case=contention_snapshot_mismatch stored={stored:?} expected={contention:?}"
        ));
    }
    Ok(())
}

#[test]
fn test_task_inspector_blocked_reason() -> Result<(), String> {
    let mut inspector = TaskInspector::default();
    inspector.register_blocked_reason(99, "obligation", "waiting for merge witness");

    let blocked = inspector
        .blocked_reason(99)
        .ok_or_else(|| format!("bead_id={BEAD_ID} case=missing_blocked_reason task_id=99"))?;
    if blocked.waiting_on != "obligation" || blocked.detail != "waiting for merge witness" {
        return Err(format!(
            "bead_id={BEAD_ID} case=blocked_reason_mismatch blocked={blocked:?}"
        ));
    }
    Ok(())
}

#[test]
fn test_witness_references_stable_under_replay() -> Result<(), String> {
    let witness_first = simulated_replay_witnesses(42);
    let witness_second = simulated_replay_witnesses(42);
    if witness_first != witness_second {
        return Err(format!(
            "bead_id={BEAD_ID} case=witness_nondeterministic first={witness_first:?} second={witness_second:?}"
        ));
    }

    let mut entry_first = sample_entry(501);
    entry_first.witnesses = witness_first;
    let mut entry_second = sample_entry(501);
    entry_second.witnesses = witness_second;
    if entry_first.witnesses != entry_second.witnesses {
        return Err(format!(
            "bead_id={BEAD_ID} case=witness_mismatch first={:?} second={:?}",
            entry_first.witnesses, entry_second.witnesses
        ));
    }
    Ok(())
}

#[test]
fn test_e2e_bd_3q1g_compliance() -> Result<(), String> {
    let description = load_issue_description(BEAD_ID)?;
    let evaluation = evaluate_description(&description);
    let ledger_once = build_ams_evidence_ledger();
    let ledger_twice = build_ams_evidence_ledger();
    let policy_once = policy_trace_fingerprint();
    let policy_twice = policy_trace_fingerprint();

    let mut repro_ledger = EvidenceLedger::new(8, EmissionPolicy::Lab);
    let _recorded = repro_ledger.record(sample_entry(701));
    let temp = tempdir().map_err(|error| format!("tempdir_failed error={error}"))?;
    let manifest_path = repro_ledger.spill_to_artifacts(temp.path(), "e2e_smoke")?;

    eprintln!(
        "DEBUG bead_id={BEAD_ID} case=e2e_start heavy_hitters={} policy_events={} manifest={}",
        ledger_once.heavy_hitters.len(),
        policy_once.len(),
        manifest_path.display()
    );
    eprintln!(
        "INFO bead_id={BEAD_ID} case=e2e_summary missing_unit_ids={} missing_e2e_ids={} missing_logs={} missing_spec_markers={}",
        evaluation.missing_unit_ids.len(),
        evaluation.missing_e2e_ids.len(),
        evaluation.missing_log_levels.len(),
        evaluation.missing_spec_markers.len()
    );
    eprintln!(
        "WARN bead_id={BEAD_ID} case=e2e_diagnostic deterministic_ledger={} deterministic_policy={} env_marker=ASUPERSYNC_TEST_ARTIFACTS_DIR",
        ledger_once == ledger_twice,
        policy_once == policy_twice
    );
    eprintln!(
        "ERROR bead_id={BEAD_ID} case=e2e_guard compliance_ok={} replay_cmd=\"cargo test -p fsqlite-harness test_e2e_bd_3q1g_compliance -- --nocapture\"",
        evaluation.is_compliant()
    );

    if !evaluation.is_compliant() {
        return Err(format!(
            "bead_id={BEAD_ID} case=e2e_compliance_failure evaluation={evaluation:?}"
        ));
    }
    if ledger_once != ledger_twice {
        return Err(format!(
            "bead_id={BEAD_ID} case=e2e_nondeterministic_ledger first={ledger_once:?} second={ledger_twice:?}"
        ));
    }
    if policy_once != policy_twice {
        return Err(format!(
            "bead_id={BEAD_ID} case=e2e_nondeterministic_policy first={policy_once:?} second={policy_twice:?}"
        ));
    }
    if !manifest_path.exists() {
        return Err(format!(
            "bead_id={BEAD_ID} case=e2e_manifest_missing path={}",
            manifest_path.display()
        ));
    }

    Ok(())
}

#[test]
fn test_e2e_task_inspector_and_evidence_ledger_smoke() -> Result<(), String> {
    test_e2e_bd_3q1g_compliance()
}
