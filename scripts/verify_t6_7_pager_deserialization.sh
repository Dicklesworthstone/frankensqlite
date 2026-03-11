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
SOURCE_SNAPSHOT_JSON="${ARTIFACT_DIR}/source_snapshot.json"
WATCHED_SOURCE_PATHS=(
  "${WORKSPACE_ROOT}/crates/fsqlite-pager/src/pager.rs"
  "${WORKSPACE_ROOT}/crates/fsqlite-core/src/connection.rs"
  "${WORKSPACE_ROOT}/scripts/verify_t6_7_pager_deserialization.sh"
)

mkdir -p "${ARTIFACT_DIR}"
: > "${EVENTS_JSONL}"

export NO_COLOR="${NO_COLOR:-1}"
export RUST_TEST_THREADS="${RUST_TEST_THREADS:-1}"
export RUST_LOG="${RUST_LOG:-fsqlite.snapshot_publication=trace}"

capture_source_snapshot() {
  local output="$1"
  local entries=()
  local path rel_path sha256 mtime_epoch mtime_iso
  for path in "${WATCHED_SOURCE_PATHS[@]}"; do
    if [[ ! -f "${path}" ]]; then
      echo "[GATE FAIL] watched source path missing: ${path}" >&2
      return 1
    fi

    rel_path="${path#${WORKSPACE_ROOT}/}"
    sha256="$(sha256sum "${path}" | awk '{print $1}')"
    mtime_epoch="$(stat -c '%Y' "${path}")"
    mtime_iso="$(date -u -d "@${mtime_epoch}" +%Y-%m-%dT%H:%M:%SZ)"
    entries+=("$(
      jq -cn \
        --arg path "${rel_path}" \
        --arg sha256 "${sha256}" \
        --arg mtime_iso "${mtime_iso}" \
        --argjson mtime_epoch "${mtime_epoch}" \
        '{
          path: $path,
          sha256: $sha256,
          mtime_epoch: $mtime_epoch,
          mtime_iso: $mtime_iso
        }'
    )")
  done

  printf '%s\n' "${entries[@]}" | jq -s '.' > "${output}"
}

write_source_drift_report() {
  local before_snapshot="$1"
  local after_snapshot="$2"
  local phase="$3"
  local output="$4"

  jq -n \
    --arg phase "${phase}" \
    --slurpfile before "${before_snapshot}" \
    --slurpfile after "${after_snapshot}" \
    '
    def as_map($rows):
      reduce $rows[] as $row ({}; .[$row.path] = $row);

    ($before[0] // []) as $before_rows
    | ($after[0] // []) as $after_rows
    | (as_map($before_rows)) as $before_map
    | (as_map($after_rows)) as $after_map
    | {
        phase: $phase,
        changed_files: (
          ($before_rows + $after_rows)
          | map(.path)
          | unique
          | map(
              . as $path
              | {
                  path: $path,
                  before: ($before_map[$path] // null),
                  after: ($after_map[$path] // null)
                }
              | select(.before != .after)
            )
        )
      }
    ' > "${output}"
}

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

  local phase_snapshot_before="${ARTIFACT_DIR}/${phase}_source_snapshot_before.json"
  local phase_snapshot_after="${ARTIFACT_DIR}/${phase}_source_snapshot_after.json"
  local drift_report="${ARTIFACT_DIR}/${phase}_source_drift.json"
  capture_source_snapshot "${phase_snapshot_before}"
  if ! cmp -s "${SOURCE_SNAPSHOT_JSON}" "${phase_snapshot_before}"; then
    write_source_drift_report "${SOURCE_SNAPSHOT_JSON}" "${phase_snapshot_before}" "${phase}" "${drift_report}"
    emit_event "${phase}" "fail" "fail" 0 "watched source changed before phase start: ${drift_report}"
    echo "[GATE FAIL] ${phase}: watched source changed before phase start; see ${drift_report}" >&2
    return 1
  fi

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
    capture_source_snapshot "${phase_snapshot_after}"
    if ! cmp -s "${phase_snapshot_before}" "${phase_snapshot_after}"; then
      write_source_drift_report "${phase_snapshot_before}" "${phase_snapshot_after}" "${phase}" "${drift_report}"
      emit_event "${phase}" "fail" "fail" "${elapsed}" "watched source changed while phase was running: ${drift_report}"
      echo "[GATE FAIL] ${phase}: watched source changed while phase was running; see ${drift_report}" >&2
      return 1
    fi
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
    if capture_source_snapshot "${phase_snapshot_after}" && ! cmp -s "${phase_snapshot_before}" "${phase_snapshot_after}"; then
      write_source_drift_report "${phase_snapshot_before}" "${phase_snapshot_after}" "${phase}" "${drift_report}"
      emit_event "${phase}" "fail" "fail" "${elapsed}" "watched source changed while phase was running: ${drift_report}"
      echo "[GATE FAIL] ${phase}: watched source changed while phase was running; see ${drift_report}" >&2
      return 1
    fi
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
capture_source_snapshot "${SOURCE_SNAPSHOT_JSON}"
emit_event "bootstrap" "pass" "pass" 0 "captured watched source snapshot: ${SOURCE_SNAPSHOT_JSON}"

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
  rch exec -- env CARGO_TARGET_DIR="/tmp/${RUN_ID_SAFE}_pager_checkpoint" cargo test -p fsqlite-pager pager::tests::test_checkpoint_busy_with_active_writer -- --exact --nocapture

run_phase \
  "publish_window_benchmark" \
  "${BENCHMARK_LOG}" \
  rch exec -- env CARGO_TERM_COLOR=never CARGO_TARGET_DIR="/tmp/${RUN_ID_SAFE}_pager_publish_window" cargo test -p fsqlite-pager pager::tests::wal_publish_window_shrink_benchmark_report -- --ignored --exact --nocapture --test-threads=1
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
  --arg source_snapshot_json "${SOURCE_SNAPSHOT_JSON}" \
  --slurpfile benchmark "${BENCHMARK_JSON}" \
  --slurpfile source_snapshot "${SOURCE_SNAPSHOT_JSON}" \
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
      benchmark_json: $benchmark_json,
      source_snapshot_json: $source_snapshot_json
    },
    watched_source_snapshot: $source_snapshot[0],
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
  echo "- watched source snapshot JSON: \`${SOURCE_SNAPSHOT_JSON}\`"
  echo
  echo "This gate combines three evidence layers:"
  echo "- published-read and snapshot-publication traces from `fsqlite-pager`"
  echo "- file-backed strict-visibility routing traces from `fsqlite-core`"
  echo "- explicit hold/wait metrics harvested from the existing deterministic publish-window benchmark"
  echo "- a watched-source snapshot that invalidates the run if `pager.rs`, `connection.rs`, or this gate changes mid-flight"
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
  echo
  echo "Watched source snapshot:"
  jq -r '
    .watched_source_snapshot[]
    | "- `\(.path)` sha256=`\(.sha256)` mtime=`\(.mtime_iso)`"
  ' "${REPORT_JSON}"
} > "${SUMMARY_MD}"

emit_event "finalize" "pass" "pass" 0 "report written to ${REPORT_JSON}"
echo "[GATE PASS] ${BEAD_ID} pager de-serialization gate passed"
