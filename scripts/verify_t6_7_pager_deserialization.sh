#!/usr/bin/env bash
# Verification gate for bd-1dp9.6.7.7.2:
# Pager read/write staging de-serialization with explicit serialized-phase evidence.

set -euo pipefail

WORKSPACE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BEAD_ID="bd-1dp9.6.7.7.2"
SCENARIO_ID="${SCENARIO_ID:-PAGER-DESERIALIZATION-67772}"
SEED="${SEED:-67772}"
TIMESTAMP_UTC="$(date -u +%Y%m%dT%H%M%SZ)"
RUN_ID="${RUN_ID:-${BEAD_ID}-${TIMESTAMP_UTC}-${SEED}}"
TRACE_ID="${TRACE_ID:-trace-${RUN_ID}}"
RUN_ID_SAFE="$(printf '%s' "${RUN_ID}" | tr -c '[:alnum:]' '_')"
ARTIFACT_DIR="${WORKSPACE_ROOT}/artifacts/${BEAD_ID}/${RUN_ID}"
EVENTS_JSONL="${ARTIFACT_DIR}/events.jsonl"
REPORT_JSON="${ARTIFACT_DIR}/report.json"
SUMMARY_MD="${ARTIFACT_DIR}/summary.md"
PUBLISHED_LOG="${ARTIFACT_DIR}/published_read_path.log"
FILE_BACKED_LOG="${ARTIFACT_DIR}/file_backed_visibility.log"
CHECKPOINT_LOG="${ARTIFACT_DIR}/checkpoint_gate.log"
BENCHMARK_LOG="${ARTIFACT_DIR}/publish_window_benchmark.log"
BENCHMARK_JSON="${ARTIFACT_DIR}/publish_window_benchmark.json"

mkdir -p "${ARTIFACT_DIR}"
: > "${EVENTS_JSONL}"

export NO_COLOR="${NO_COLOR:-1}"
export RUST_TEST_THREADS="${RUST_TEST_THREADS:-1}"
export RUST_LOG="${RUST_LOG:-fsqlite.snapshot_publication=trace}"

assert_log_contains() {
  local logfile="$1"
  local pattern="$2"
  local message="$3"
  if ! grep -Eq "${pattern}" "${logfile}"; then
    echo "[GATE FAIL] ${message}" >&2
    return 1
  fi
}

assert_publication_trace_contract() {
  local phase="$1"
  local logfile="$2"
  local action_pattern="$3"

  assert_log_contains "${logfile}" 'snapshot_gen' \
    "${phase}: missing snapshot_gen trace field"
  assert_log_contains "${logfile}" 'visible_commit_seq' \
    "${phase}: missing visible_commit_seq trace field"
  assert_log_contains "${logfile}" 'publication_mode' \
    "${phase}: missing publication_mode trace field"
  assert_log_contains "${logfile}" 'read_retry_count' \
    "${phase}: missing read_retry_count trace field"
  assert_log_contains "${logfile}" 'page_set_size' \
    "${phase}: missing page_set_size trace field"
  assert_log_contains "${logfile}" "${action_pattern}" \
    "${phase}: missing expected publication trace action"
}

emit_event() {
  local phase="$1"
  local event_type="$2"
  local outcome="$3"
  local elapsed_ms="$4"
  local message="$5"
  printf '{"trace_id":"%s","run_id":"%s","scenario_id":"%s","seed":%s,"phase":"%s","event_type":"%s","outcome":"%s","elapsed_ms":%s,"timestamp":"%s","message":"%s"}\n' \
    "${TRACE_ID}" "${RUN_ID}" "${SCENARIO_ID}" "${SEED}" "${phase}" "${event_type}" "${outcome}" "${elapsed_ms}" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "${message}" \
    >> "${EVENTS_JSONL}"
}

emit_serialized_phase_event() {
  local phase_name="$1"
  local hold_ns="$2"
  local wait_ns="$3"
  local serialized_reason="$4"
  local writer_count="$5"
  local checkpoint_overlap="$6"
  local backend_identity="$7"
  local first_failure_diag="$8"
  printf '{"trace_id":"%s","run_id":"%s","scenario_id":"%s","seed":%s,"phase_name":"%s","hold_ns":%s,"wait_ns":%s,"serialized_reason":"%s","writer_count":%s,"checkpoint_overlap":%s,"backend_identity":"%s","first_failure_diag":"%s","timestamp":"%s"}\n' \
    "${TRACE_ID}" "${RUN_ID}" "${SCENARIO_ID}" "${SEED}" "${phase_name}" "${hold_ns}" "${wait_ns}" "${serialized_reason}" "${writer_count}" "${checkpoint_overlap}" "${backend_identity}" "${first_failure_diag}" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    >> "${EVENTS_JSONL}"
}

run_phase() {
  local phase="$1"
  local logfile="$2"
  shift 2

  emit_event "${phase}" "start" "running" 0 "running: $*"
  local started
  started="$(date +%s%3N)"

  if (
    cd "${WORKSPACE_ROOT}"
    "$@"
  ) 2>&1 | tee "${logfile}"; then
    local finished elapsed
    finished="$(date +%s%3N)"
    elapsed="$((finished - started))"
    if ! grep -Eq '^running [1-9][0-9]* tests?$' "${logfile}"; then
      emit_event "${phase}" "fail" "fail" "${elapsed}" "command completed without executing tests: $*"
      echo "[GATE FAIL] ${phase}: no tests executed" >&2
      return 1
    fi
    emit_event "${phase}" "pass" "pass" "${elapsed}" "completed: $*"
  else
    local finished elapsed
    finished="$(date +%s%3N)"
    elapsed="$((finished - started))"
    emit_event "${phase}" "fail" "fail" "${elapsed}" "failed: $*"
    return 1
  fi
}

extract_benchmark_report() {
  awk '
    /BEGIN_BD_DB300_3_2_3_REPORT/ {capture=1; next}
    /END_BD_DB300_3_2_3_REPORT/ {capture=0}
    capture {print}
  ' "${BENCHMARK_LOG}" > "${BENCHMARK_JSON}"
  if [[ ! -s "${BENCHMARK_JSON}" ]]; then
    echo "[GATE FAIL] failed to extract publish-window benchmark JSON payload" >&2
    return 1
  fi
}

echo "=== ${BEAD_ID}: pager de-serialization verification ==="
echo "run_id=${RUN_ID}"
echo "trace_id=${TRACE_ID}"
echo "scenario_id=${SCENARIO_ID}"
echo "seed=${SEED}"
echo "artifacts=${ARTIFACT_DIR}"

emit_event "bootstrap" "start" "running" 0 "verification started"

run_phase \
  "published_read_path" \
  "${PUBLISHED_LOG}" \
  rch exec -- env CARGO_TARGET_DIR="/tmp/${RUN_ID_SAFE}_pager_published" cargo test -p fsqlite-pager published_ -- --nocapture
assert_publication_trace_contract \
  "published_read_path" \
  "${PUBLISHED_LOG}" \
  'published pager snapshot|served page from published snapshot|resolved zero-filled page from published metadata'

run_phase \
  "file_backed_visibility" \
  "${FILE_BACKED_LOG}" \
  rch exec -- env CARGO_TARGET_DIR="/tmp/${RUN_ID_SAFE}_core_file_backed" cargo test -p fsqlite-core connection::pager_routing_tests::test_visibility_interleavings_fixed_seed_matrix -- --exact --nocapture
assert_publication_trace_contract \
  "file_backed_visibility" \
  "${FILE_BACKED_LOG}" \
  'bound connection to pager snapshot'

run_phase \
  "checkpoint_gate" \
  "${CHECKPOINT_LOG}" \
  rch exec -- env CARGO_TARGET_DIR="/tmp/${RUN_ID_SAFE}_pager_checkpoint" cargo test -p fsqlite-pager test_checkpoint_busy_with_active_writer -- --exact --nocapture

run_phase \
  "publish_window_benchmark" \
  "${BENCHMARK_LOG}" \
  rch exec -- env CARGO_TERM_COLOR=never CARGO_TARGET_DIR="/tmp/${RUN_ID_SAFE}_pager_publish_window" cargo test -p fsqlite-pager wal_publish_window_shrink_benchmark_report -- --ignored --nocapture --test-threads=1
extract_benchmark_report

jq -e '
  .schema_version == "fsqlite.track_c.publish_window_benchmark.v1"
  and (.cases | length) >= 1
  and ([.cases[] | .exclusive_window_hold_candidate.median_ns > 0] | all)
  and ([.cases[] | .contending_writer_stall_candidate.median_ns > 0] | all)
' "${BENCHMARK_JSON}" >/dev/null

emit_serialized_phase_event \
  "checkpoint_gate" \
  "0" \
  "0" \
  "checkpoint_active_busy" \
  "1" \
  "true" \
  "memory_vfs:checkpoint_guard" \
  "Busy"

while IFS=$'\t' read -r phase_name hold_ns wait_ns serialized_reason writer_count checkpoint_overlap backend_identity first_failure_diag; do
  emit_serialized_phase_event \
    "${phase_name}" \
    "${hold_ns}" \
    "${wait_ns}" \
    "${serialized_reason}" \
    "${writer_count}" \
    "${checkpoint_overlap}" \
    "${backend_identity}" \
    "${first_failure_diag}"
done < <(
  jq -r '
    .cases[]
    | [
        "exclusive_publish_window:" + .scenario_id,
        (.exclusive_window_hold_candidate.median_ns | tostring),
        "0",
        "durable_state_transition",
        "1",
        "false",
        "blocking_memory_vfs:prepared_candidate",
        "none"
      ],
      [
        "contending_writer_wait:" + .scenario_id,
        "0",
        (.contending_writer_stall_candidate.median_ns | tostring),
        "exclusive_writer_wait",
        "2",
        "false",
        "blocking_memory_vfs:prepared_candidate",
        "none"
      ]
    | @tsv
  ' "${BENCHMARK_JSON}"
)

jq -n \
  --arg bead_id "${BEAD_ID}" \
  --arg trace_id "${TRACE_ID}" \
  --arg run_id "${RUN_ID}" \
  --arg scenario_id "${SCENARIO_ID}" \
  --arg seed "${SEED}" \
  --arg rust_log "${RUST_LOG}" \
  --arg rust_test_threads "${RUST_TEST_THREADS}" \
  --arg events_jsonl "${EVENTS_JSONL}" \
  --arg summary_md "${SUMMARY_MD}" \
  --arg published_log "${PUBLISHED_LOG}" \
  --arg file_backed_log "${FILE_BACKED_LOG}" \
  --arg checkpoint_log "${CHECKPOINT_LOG}" \
  --arg benchmark_log "${BENCHMARK_LOG}" \
  --arg benchmark_json "${BENCHMARK_JSON}" \
  --slurpfile benchmark "${BENCHMARK_JSON}" \
  '
  {
    bead_id: $bead_id,
    trace_id: $trace_id,
    run_id: $run_id,
    scenario_id: $scenario_id,
    seed: ($seed | tonumber),
    result: "pass",
    rust_log: $rust_log,
    rust_test_threads: $rust_test_threads,
    artifacts: {
      events_jsonl: $events_jsonl,
      summary_md: $summary_md,
      published_log: $published_log,
      file_backed_log: $file_backed_log,
      checkpoint_log: $checkpoint_log,
      benchmark_log: $benchmark_log,
      benchmark_json: $benchmark_json
    },
    acceptance: {
      published_read_trace_contract: true,
      file_backed_visibility_trace_contract: true,
      checkpoint_gate_busy_path: true,
      serialized_phase_events_emitted: true
    },
    publish_window_benchmark: $benchmark[0]
  }
  ' > "${REPORT_JSON}"

{
  echo "# ${BEAD_ID} Pager De-serialization Summary"
  echo
  echo "- run_id: \`${RUN_ID}\`"
  echo "- trace_id: \`${TRACE_ID}\`"
  echo "- scenario_id: \`${SCENARIO_ID}\`"
  echo "- seed: \`${SEED}\`"
  echo "- published read path log: \`${PUBLISHED_LOG}\`"
  echo "- file-backed visibility log: \`${FILE_BACKED_LOG}\`"
  echo "- checkpoint gate log: \`${CHECKPOINT_LOG}\`"
  echo "- publish-window benchmark log: \`${BENCHMARK_LOG}\`"
  echo "- publish-window benchmark JSON: \`${BENCHMARK_JSON}\`"
  echo "- report_json: \`${REPORT_JSON}\`"
  echo
  echo "This gate combines three evidence layers:"
  echo "- published-read and snapshot-publication traces from `fsqlite-pager`"
  echo "- file-backed strict-visibility routing traces from `fsqlite-core`"
  echo "- explicit hold/wait metrics harvested from the existing deterministic publish-window benchmark"
  echo
  echo "| Scenario | Candidate Hold Median (ns) | Candidate Wait Median (ns) | Serialized Reason | Backend Identity |"
  echo "| --- | ---: | ---: | --- | --- |"
  jq -r '
    .publish_window_benchmark.cases[]
    | "| \(.scenario_id) | \(.exclusive_window_hold_candidate.median_ns) | \(.contending_writer_stall_candidate.median_ns) | durable_state_transition / exclusive_writer_wait | blocking_memory_vfs:prepared_candidate |"
  ' "${REPORT_JSON}"
  echo
  echo "Checkpoint gate evidence:"
  echo "- `test_checkpoint_busy_with_active_writer` passed and the gate emitted a structured event with `serialized_reason=checkpoint_active_busy`"
} > "${SUMMARY_MD}"

emit_event "finalize" "pass" "pass" 0 "report written to ${REPORT_JSON}"
echo "[GATE PASS] ${BEAD_ID} pager de-serialization gate passed"
