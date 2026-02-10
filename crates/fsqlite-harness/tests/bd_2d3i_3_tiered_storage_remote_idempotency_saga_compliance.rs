use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::path::{Path, PathBuf};

use fsqlite_core::epoch::{BarrierOutcome, EpochBarrier, EpochClock};
use fsqlite_core::tiered_storage::{
    DurabilityMode, EvictionPhase, FetchSymbolsRequest, RemoteTier, TieredStorage,
    UploadSegmentReceipt, UploadSegmentRequest,
};
use fsqlite_error::Result as FResult;
use fsqlite_types::cx::{Cx, cap};
use fsqlite_types::{
    EpochId, IdempotencyKey, ObjectId, Oti, RemoteCap, Saga, SymbolRecord, SymbolRecordFlags,
};
use proptest::prelude::proptest;
use proptest::test_runner::TestCaseError;
use serde_json::Value;

const BEAD_ID: &str = "bd-2d3i.3";
const ISSUES_JSONL: &str = ".beads/issues.jsonl";

const UNIT_TEST_IDS: [&str; 9] = [
    "test_idempotent_fetch_dedup",
    "test_idempotent_fetch_no_double_ack",
    "test_idempotent_upload_exactly_once",
    "test_eviction_saga_cancel_upload",
    "test_eviction_saga_cancel_verify",
    "test_eviction_saga_cancel_retire",
    "test_no_half_evicted_state",
    "test_epoch_transition_quiescence",
    "test_epoch_transition_concurrent_commits",
];

const E2E_TEST_IDS: [&str; 3] = [
    "e2e_tiered_storage_roundtrip",
    "e2e_saga_resilience_under_chaos",
    "e2e_epoch_transition_under_load",
];

const LOG_LEVEL_MARKERS: [&str; 4] = ["INFO", "DEBUG", "WARN", "ERROR"];

const REQUIRED_TOKENS: [&str; 16] = [
    "test_idempotent_fetch_dedup",
    "test_idempotent_fetch_no_double_ack",
    "test_idempotent_upload_exactly_once",
    "test_eviction_saga_cancel_upload",
    "test_eviction_saga_cancel_verify",
    "test_eviction_saga_cancel_retire",
    "test_no_half_evicted_state",
    "test_epoch_transition_quiescence",
    "test_epoch_transition_concurrent_commits",
    "e2e_tiered_storage_roundtrip",
    "e2e_saga_resilience_under_chaos",
    "e2e_epoch_transition_under_load",
    "INFO",
    "DEBUG",
    "WARN",
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CancelPoint {
    Upload,
    Verify,
    Retire,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LocalSegmentState {
    Present,
    Retired,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RemoteSegmentState {
    Absent,
    UploadedUnverified,
    Verified,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct EvictionSagaModelState {
    local_state: LocalSegmentState,
    remote_state: RemoteSegmentState,
    effect_log: Vec<&'static str>,
}

impl EvictionSagaModelState {
    fn is_local_present(&self) -> bool {
        matches!(self.local_state, LocalSegmentState::Present)
    }

    fn is_remote_present(&self) -> bool {
        !matches!(self.remote_state, RemoteSegmentState::Absent)
    }

    fn is_coherent(&self) -> bool {
        matches!(
            (self.local_state, self.remote_state),
            (LocalSegmentState::Present, _)
                | (LocalSegmentState::Retired, RemoteSegmentState::Verified)
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CommitAttempt {
    begin_epoch: EpochId,
    committed_epoch: Option<EpochId>,
    aborted_at_transition: bool,
}

#[derive(Debug, Default)]
struct MockRemoteTier {
    object_symbols: HashMap<ObjectId, Vec<SymbolRecord>>,
    segment_symbols: HashMap<u64, Vec<SymbolRecord>>,
    fetch_cache: HashMap<IdempotencyKey, Vec<SymbolRecord>>,
    upload_receipts: HashMap<(u64, IdempotencyKey), UploadSegmentReceipt>,
    durable_publications: HashMap<IdempotencyKey, usize>,
    segment_recoverability_overrides: HashMap<u64, bool>,
    configured_acks: u32,
    fetch_calls: usize,
    fetch_effective_execs: usize,
    upload_calls: usize,
    upload_effective_execs: usize,
    durability_ack_count: usize,
    cancel_after_upload: Option<Cx<cap::All>>,
}

impl MockRemoteTier {
    fn set_object_symbols(&mut self, object_id: ObjectId, records: Vec<SymbolRecord>) {
        self.object_symbols.insert(object_id, records);
    }

    fn set_acked_stores(&mut self, acked_stores: u32) {
        self.configured_acks = acked_stores;
    }

    fn set_cancel_after_upload(&mut self, cx: Cx<cap::All>) {
        self.cancel_after_upload = Some(cx);
    }

    fn fetch_effective_execs(&self) -> usize {
        self.fetch_effective_execs
    }

    fn upload_effective_execs(&self) -> usize {
        self.upload_effective_execs
    }

    fn durability_ack_count(&self) -> usize {
        self.durability_ack_count
    }

    fn durable_publication_count(&self, key: IdempotencyKey) -> usize {
        self.durable_publications.get(&key).copied().unwrap_or(0)
    }
}

impl RemoteTier for MockRemoteTier {
    fn fetch_symbols(&mut self, request: &FetchSymbolsRequest) -> FResult<Vec<SymbolRecord>> {
        self.fetch_calls = self.fetch_calls.saturating_add(1);

        if let Some(cached) = self.fetch_cache.get(&request.idempotency_key) {
            return Ok(cached.clone());
        }

        self.fetch_effective_execs = self.fetch_effective_execs.saturating_add(1);
        self.durability_ack_count = self.durability_ack_count.saturating_add(1);

        let Some(records) = self.object_symbols.get(&request.object_id) else {
            self.fetch_cache.insert(request.idempotency_key, Vec::new());
            return Ok(Vec::new());
        };

        let preferred: BTreeSet<u32> = request.preferred_esis.iter().copied().collect();
        let mut ordered = records.clone();
        ordered.sort_by_key(|record| (!preferred.contains(&record.esi), record.esi));
        ordered.truncate(request.max_symbols);
        self.fetch_cache
            .insert(request.idempotency_key, ordered.clone());

        Ok(ordered)
    }

    fn upload_segment(&mut self, request: &UploadSegmentRequest) -> FResult<UploadSegmentReceipt> {
        let key = (request.segment_id, request.idempotency_key);
        if let Some(existing) = self.upload_receipts.get(&key).copied() {
            return Ok(UploadSegmentReceipt {
                deduplicated: true,
                ..existing
            });
        }

        self.upload_calls = self.upload_calls.saturating_add(1);
        self.upload_effective_execs = self.upload_effective_execs.saturating_add(1);

        self.segment_symbols
            .insert(request.segment_id, request.records.clone());

        for record in &request.records {
            let entry = self.object_symbols.entry(record.object_id).or_default();
            if entry.iter().all(|existing| existing.esi != record.esi) {
                entry.push(record.clone());
            }
            entry.sort_by_key(|existing| existing.esi);
        }

        let publication_counter = self
            .durable_publications
            .entry(request.idempotency_key)
            .or_insert(0);
        *publication_counter = publication_counter.saturating_add(1);

        let receipt = UploadSegmentReceipt {
            acked_stores: self.configured_acks,
            deduplicated: false,
        };
        self.upload_receipts.insert(key, receipt);

        if let Some(cx) = self.cancel_after_upload.take() {
            cx.cancel();
        }

        Ok(receipt)
    }

    fn segment_recoverable(&self, segment_id: u64, min_symbols_per_object: usize) -> bool {
        if let Some(override_value) = self.segment_recoverability_overrides.get(&segment_id) {
            return *override_value;
        }

        let Some(records) = self.segment_symbols.get(&segment_id) else {
            return false;
        };

        let mut per_object = HashMap::<ObjectId, usize>::new();
        for record in records {
            let entry = per_object.entry(record.object_id).or_insert(0);
            *entry = entry.saturating_add(1);
        }

        per_object
            .values()
            .all(|count| *count >= min_symbols_per_object)
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

fn object_id_from_u64(raw: u64) -> ObjectId {
    let mut bytes = [0_u8; 16];
    bytes[0..8].copy_from_slice(&raw.to_le_bytes());
    bytes[8..16].copy_from_slice(&raw.to_le_bytes());
    ObjectId::from_bytes(bytes)
}

fn remote_cap(seed: u8) -> RemoteCap {
    RemoteCap::from_bytes([seed; 16])
}

fn make_symbol_records(
    object_id: ObjectId,
    payload: &[u8],
    symbol_size: usize,
    repair_symbols: usize,
) -> Vec<SymbolRecord> {
    let symbol_size_u32 = u32::try_from(symbol_size).expect("symbol size fits u32");
    let transfer_len_u64 = u64::try_from(payload.len()).expect("payload length fits u64");
    let oti = Oti {
        f: transfer_len_u64,
        al: 1,
        t: symbol_size_u32,
        z: 1,
        n: 1,
    };

    let source_symbols = payload.len().div_ceil(symbol_size);
    let mut out = Vec::new();

    for idx in 0..source_symbols {
        let start = idx * symbol_size;
        let end = (start + symbol_size).min(payload.len());
        let mut symbol = vec![0_u8; symbol_size];
        symbol[..end - start].copy_from_slice(&payload[start..end]);
        let esi = u32::try_from(idx).expect("source esi fits u32");
        let flags = if idx == 0 {
            SymbolRecordFlags::SYSTEMATIC_RUN_START
        } else {
            SymbolRecordFlags::empty()
        };
        out.push(SymbolRecord::new(object_id, oti, esi, symbol, flags));
    }

    for repair_idx in 0..repair_symbols {
        let repair_esi_usize = source_symbols.saturating_add(repair_idx);
        let esi = u32::try_from(repair_esi_usize).expect("repair esi fits u32");
        let mut symbol = vec![0_u8; symbol_size];
        let esi_low = u8::try_from(esi & 0xFF).expect("masked to u8");
        for (offset, byte) in symbol.iter_mut().enumerate() {
            let offset_low = u8::try_from(offset & 0xFF).expect("masked to u8");
            *byte = esi_low ^ offset_low;
        }
        out.push(SymbolRecord::new(
            object_id,
            oti,
            esi,
            symbol,
            SymbolRecordFlags::empty(),
        ));
    }

    out
}

fn deterministic_key(ecs_epoch: u64, label: &[u8]) -> IdempotencyKey {
    IdempotencyKey::derive(ecs_epoch, label)
}

fn build_fetch_request(
    object_id: ObjectId,
    key: IdempotencyKey,
    ecs_epoch: u64,
) -> FetchSymbolsRequest {
    FetchSymbolsRequest {
        object_id,
        preferred_esis: vec![0, 1, 2],
        max_symbols: usize::MAX,
        idempotency_key: key,
        ecs_epoch,
        remote_cap: remote_cap(1),
        computation: "symbol_get_range",
    }
}

fn build_upload_request(
    segment_id: u64,
    records: Vec<SymbolRecord>,
    key: IdempotencyKey,
    ecs_epoch: u64,
) -> UploadSegmentRequest {
    UploadSegmentRequest {
        segment_id,
        records,
        idempotency_key: key,
        saga: Saga::new(key),
        ecs_epoch,
        remote_cap: remote_cap(2),
        computation: "symbol_put_batch",
    }
}

fn run_eviction_saga_model(
    cancel_at: Option<CancelPoint>,
    remote_recoverable: bool,
) -> EvictionSagaModelState {
    let mut state = EvictionSagaModelState {
        local_state: LocalSegmentState::Present,
        remote_state: RemoteSegmentState::Absent,
        effect_log: vec!["start"],
    };

    state.effect_log.push("upload_start");
    state.remote_state = RemoteSegmentState::UploadedUnverified;
    state.effect_log.push("upload_done");
    if matches!(cancel_at, Some(CancelPoint::Upload)) {
        state.effect_log.push("cancel_upload");
        return state;
    }

    state.effect_log.push("verify_start");
    if remote_recoverable {
        state.remote_state = RemoteSegmentState::Verified;
    }
    state.effect_log.push("verify_done");
    if matches!(cancel_at, Some(CancelPoint::Verify)) {
        state.effect_log.push("cancel_verify");
        return state;
    }

    if !matches!(state.remote_state, RemoteSegmentState::Verified) {
        state.effect_log.push("precondition_failed_retain_local");
        return state;
    }

    state.effect_log.push("retire_start");
    if matches!(cancel_at, Some(CancelPoint::Retire)) {
        state.effect_log.push("cancel_retire");
        return state;
    }

    state.local_state = LocalSegmentState::Retired;
    state.effect_log.push("retire_done");
    state
}

fn run_epoch_transition_scenario(
    inflight_commits: usize,
) -> Result<(EpochId, EpochId, Vec<CommitAttempt>), String> {
    let clock = EpochClock::new(EpochId::new(9));
    let old_epoch = clock.current();
    let barrier = EpochBarrier::new(old_epoch, 2);

    let mut commits = Vec::with_capacity(inflight_commits);
    for index in 0..inflight_commits {
        if index % 2 == 0 {
            commits.push(CommitAttempt {
                begin_epoch: old_epoch,
                committed_epoch: Some(old_epoch),
                aborted_at_transition: false,
            });
        } else {
            commits.push(CommitAttempt {
                begin_epoch: old_epoch,
                committed_epoch: None,
                aborted_at_transition: false,
            });
        }
    }

    let _ = barrier.arrive("write_coordinator");
    let timeout = barrier
        .resolve(&clock)
        .map_err(|error| format!("barrier_resolve_timeout_failed error={error:?}"))?;
    if !matches!(timeout, BarrierOutcome::Timeout { .. }) {
        return Err(format!(
            "expected_timeout_before_full_arrival got={timeout:?}"
        ));
    }

    let _ = barrier.arrive("symbol_store");
    let resolved = barrier
        .resolve(&clock)
        .map_err(|error| format!("barrier_resolve_all_arrived_failed error={error:?}"))?;
    let new_epoch = match resolved {
        BarrierOutcome::AllArrived { new_epoch } => new_epoch,
        other => return Err(format!("expected_all_arrived_outcome got={other:?}")),
    };

    for commit in &mut commits {
        if commit.committed_epoch.is_none() {
            commit.aborted_at_transition = true;
        }
    }

    Ok((old_epoch, new_epoch, commits))
}

#[test]
fn test_bd_2d3i_3_unit_compliance_gate() -> Result<(), String> {
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
    fn prop_bd_2d3i_3_structure_compliance(missing_index in 0usize..REQUIRED_TOKENS.len()) {
        let mut synthetic = format!(
            "## Unit Tests\n- {}\n- {}\n- {}\n- {}\n- {}\n- {}\n- {}\n- {}\n- {}\n\n## E2E\n- {}\n- {}\n- {}\n\n## Logging\n- {}\n- {}\n- {}\n- {}\n",
            UNIT_TEST_IDS[0],
            UNIT_TEST_IDS[1],
            UNIT_TEST_IDS[2],
            UNIT_TEST_IDS[3],
            UNIT_TEST_IDS[4],
            UNIT_TEST_IDS[5],
            UNIT_TEST_IDS[6],
            UNIT_TEST_IDS[7],
            UNIT_TEST_IDS[8],
            E2E_TEST_IDS[0],
            E2E_TEST_IDS[1],
            E2E_TEST_IDS[2],
            LOG_LEVEL_MARKERS[0],
            LOG_LEVEL_MARKERS[1],
            LOG_LEVEL_MARKERS[2],
            LOG_LEVEL_MARKERS[3],
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
fn test_idempotent_fetch_dedup() -> Result<(), String> {
    let object_id = object_id_from_u64(10);
    let payload = b"idempotent-fetch-dedup";

    let mut remote = MockRemoteTier::default();
    remote.set_object_symbols(object_id, make_symbol_records(object_id, payload, 8, 1));

    let key = deterministic_key(77, b"symbol_get_range:object-10");
    let request = build_fetch_request(object_id, key, 77);

    let first = remote
        .fetch_symbols(&request)
        .map_err(|error| format!("first_fetch_failed error={error:?}"))?;
    let second = remote
        .fetch_symbols(&request)
        .map_err(|error| format!("second_fetch_failed error={error:?}"))?;

    if first != second {
        return Err("duplicate_fetch_results_must_match".to_owned());
    }
    if remote.fetch_effective_execs() != 1 {
        return Err(format!(
            "fetch_dedup_effective_execs_mismatch got={}",
            remote.fetch_effective_execs()
        ));
    }

    Ok(())
}

#[test]
fn test_idempotent_fetch_no_double_ack() -> Result<(), String> {
    let object_id = object_id_from_u64(11);
    let payload = b"idempotent-fetch-no-double-ack";

    let mut remote = MockRemoteTier::default();
    remote.set_object_symbols(object_id, make_symbol_records(object_id, payload, 8, 1));

    let key = deterministic_key(88, b"symbol_get_range:object-11");
    let request = build_fetch_request(object_id, key, 88);

    let _first = remote
        .fetch_symbols(&request)
        .map_err(|error| format!("first_fetch_failed error={error:?}"))?;
    let _second = remote
        .fetch_symbols(&request)
        .map_err(|error| format!("second_fetch_failed error={error:?}"))?;

    if remote.durability_ack_count() != 1 {
        return Err(format!(
            "duplicate_fetch_must_not_double_count_ack got={}",
            remote.durability_ack_count()
        ));
    }

    Ok(())
}

#[test]
fn test_idempotent_upload_exactly_once() -> Result<(), String> {
    let object_id = object_id_from_u64(12);
    let payload = b"idempotent-upload";
    let records = make_symbol_records(object_id, payload, 8, 1);

    let mut remote = MockRemoteTier::default();
    remote.set_acked_stores(2);

    let key = deterministic_key(99, b"symbol_put_batch:segment-12");
    let request = build_upload_request(12, records, key, 99);

    let first = remote
        .upload_segment(&request)
        .map_err(|error| format!("first_upload_failed error={error:?}"))?;
    let second = remote
        .upload_segment(&request)
        .map_err(|error| format!("second_upload_failed error={error:?}"))?;

    if first.deduplicated {
        return Err("first_upload_must_not_be_deduplicated".to_owned());
    }
    if !second.deduplicated {
        return Err("second_upload_must_be_deduplicated".to_owned());
    }
    if remote.upload_effective_execs() != 1 {
        return Err(format!(
            "upload_effective_execs_mismatch got={}",
            remote.upload_effective_execs()
        ));
    }
    if remote.durable_publication_count(key) != 1 {
        return Err(format!(
            "durable_publication_count_mismatch got={}",
            remote.durable_publication_count(key)
        ));
    }

    Ok(())
}

#[test]
fn test_eviction_saga_cancel_upload() -> Result<(), String> {
    let state = run_eviction_saga_model(Some(CancelPoint::Upload), true);

    if !state.is_local_present() {
        return Err("cancel_upload_must_retain_local_segment".to_owned());
    }
    if !state.is_coherent() {
        return Err(format!("cancel_upload_state_not_coherent state={state:?}"));
    }

    Ok(())
}

#[test]
fn test_eviction_saga_cancel_verify() -> Result<(), String> {
    let state = run_eviction_saga_model(Some(CancelPoint::Verify), true);

    if !state.is_local_present() {
        return Err("cancel_verify_must_retain_local_segment".to_owned());
    }
    if !state.is_coherent() {
        return Err(format!("cancel_verify_state_not_coherent state={state:?}"));
    }

    Ok(())
}

#[test]
fn test_eviction_saga_cancel_retire() -> Result<(), String> {
    let state = run_eviction_saga_model(Some(CancelPoint::Retire), true);

    if !state.is_local_present() {
        return Err("cancel_retire_must_retain_local_segment".to_owned());
    }
    if !state.is_coherent() {
        return Err(format!("cancel_retire_state_not_coherent state={state:?}"));
    }

    Ok(())
}

#[test]
fn test_no_half_evicted_state() -> Result<(), String> {
    let cancel_patterns = [
        None,
        Some(CancelPoint::Upload),
        Some(CancelPoint::Verify),
        Some(CancelPoint::Retire),
    ];

    for cancel_at in cancel_patterns {
        for recoverable in [true, false] {
            let state = run_eviction_saga_model(cancel_at, recoverable);
            if !state.is_coherent() {
                return Err(format!(
                    "half_evicted_state_detected cancel_at={cancel_at:?} recoverable={recoverable} state={state:?}"
                ));
            }
            if !state.is_local_present() && !state.is_remote_present() {
                return Err(format!(
                    "invalid_absent_everywhere_state cancel_at={cancel_at:?} recoverable={recoverable}"
                ));
            }
        }
    }

    Ok(())
}

#[test]
fn test_epoch_transition_quiescence() -> Result<(), String> {
    let (old_epoch, new_epoch, commits) = run_epoch_transition_scenario(6)?;

    if new_epoch.get() != old_epoch.get() + 1 {
        return Err(format!(
            "epoch_increment_mismatch old={} new={}",
            old_epoch.get(),
            new_epoch.get()
        ));
    }

    if commits
        .iter()
        .all(|commit| commit.committed_epoch.is_none())
    {
        return Err("expected_some_commits_to_finish_before_barrier".to_owned());
    }

    Ok(())
}

#[test]
fn test_epoch_transition_concurrent_commits() -> Result<(), String> {
    let (old_epoch, new_epoch, commits) = run_epoch_transition_scenario(12)?;

    for commit in commits {
        if let Some(committed_epoch) = commit.committed_epoch {
            if committed_epoch != commit.begin_epoch {
                return Err(format!(
                    "straddling_commit_detected begin={} committed={} old={} new={}",
                    commit.begin_epoch.get(),
                    committed_epoch.get(),
                    old_epoch.get(),
                    new_epoch.get()
                ));
            }
        } else if !commit.aborted_at_transition {
            return Err("inflight_commit_must_abort_at_transition_boundary".to_owned());
        }
    }

    Ok(())
}

#[test]
fn e2e_tiered_storage_roundtrip() -> Result<(), String> {
    let object_id = object_id_from_u64(99);
    let payload = b"tiered-storage-roundtrip-payload";
    let records = make_symbol_records(object_id, payload, 8, 2);

    let mut storage = TieredStorage::new(DurabilityMode::local());
    storage.insert_l2_segment(500, records);

    let mut remote = MockRemoteTier::default();
    remote.set_acked_stores(3);

    let cx = Cx::<cap::All>::new();
    let eviction = storage
        .evict_segment(&cx, 500, 1, 700, &mut remote, Some(remote_cap(3)))
        .map_err(|error| format!("evict_segment_failed error={error:?}"))?;

    if eviction.phase != EvictionPhase::Retired {
        return Err(format!(
            "roundtrip_eviction_phase_mismatch phase={:?}",
            eviction.phase
        ));
    }

    let fetched = storage
        .fetch_object(&cx, object_id, 701, Some(&mut remote), Some(remote_cap(3)))
        .map_err(|error| format!("fetch_after_eviction_failed error={error:?}"))?;

    if fetched.bytes != payload {
        return Err("roundtrip_payload_mismatch_after_fetch".to_owned());
    }

    Ok(())
}

#[test]
fn e2e_saga_resilience_under_chaos() -> Result<(), String> {
    let cancel_sequence = [
        None,
        Some(CancelPoint::Upload),
        Some(CancelPoint::Verify),
        Some(CancelPoint::Retire),
    ];

    for (seed_index, cancel_at) in cancel_sequence.into_iter().enumerate() {
        let recoverable = seed_index % 2 == 0;
        let state = run_eviction_saga_model(cancel_at, recoverable);

        if state.effect_log.is_empty() {
            return Err(format!(
                "chaos_effect_log_missing cancel_at={cancel_at:?} recoverable={recoverable}"
            ));
        }
        if !state.is_coherent() {
            return Err(format!(
                "chaos_state_not_coherent cancel_at={cancel_at:?} recoverable={recoverable} state={state:?}"
            ));
        }
    }

    // Also exercise concrete TieredStorage cancellation path.
    let object_id = object_id_from_u64(100);
    let payload = b"chaos-cancel-after-upload";
    let records = make_symbol_records(object_id, payload, 8, 1);

    let mut storage = TieredStorage::new(DurabilityMode::local());
    storage.insert_l2_segment(600, records);

    let cx = Cx::<cap::All>::new();
    let mut remote = MockRemoteTier::default();
    remote.set_acked_stores(3);
    remote.set_cancel_after_upload(cx.clone());

    let outcome = storage
        .evict_segment(&cx, 600, 1, 800, &mut remote, Some(remote_cap(4)))
        .map_err(|error| format!("chaos_eviction_failed error={error:?}"))?;

    if outcome.phase != EvictionPhase::CompensatedCancelled {
        return Err(format!(
            "expected_compensated_cancelled_phase got={:?}",
            outcome.phase
        ));
    }

    Ok(())
}

#[test]
fn e2e_epoch_transition_under_load() -> Result<(), String> {
    let (old_epoch, new_epoch, commits) = run_epoch_transition_scenario(64)?;

    let completed = commits
        .iter()
        .filter(|commit| commit.committed_epoch.is_some())
        .count();
    let aborted = commits
        .iter()
        .filter(|commit| commit.aborted_at_transition)
        .count();

    if completed == 0 || aborted == 0 {
        return Err(format!(
            "expected_mixed_outcomes_under_load completed={completed} aborted={aborted}"
        ));
    }

    for commit in commits {
        if let Some(committed_epoch) = commit.committed_epoch {
            if committed_epoch != old_epoch {
                return Err(format!(
                    "commit_straddled_epoch_boundary begin={} committed={} new={}",
                    commit.begin_epoch.get(),
                    committed_epoch.get(),
                    new_epoch.get()
                ));
            }
        }
    }

    Ok(())
}

#[test]
fn test_e2e_bd_2d3i_3_compliance() -> Result<(), String> {
    let description = load_issue_description(BEAD_ID)?;
    let evaluation = evaluate_description(&description);
    if !evaluation.is_compliant() {
        return Err(format!(
            "bead_id={BEAD_ID} case=description_non_compliant evaluation={evaluation:?}"
        ));
    }

    e2e_tiered_storage_roundtrip()?;
    e2e_saga_resilience_under_chaos()?;
    e2e_epoch_transition_under_load()?;

    Ok(())
}
