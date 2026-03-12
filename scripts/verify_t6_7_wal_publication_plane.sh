#!/usr/bin/env bash
# Verification gate for bd-1dp9.6.7.8.4:
# commit-published WAL visibility snapshots and generation-stamped read binding.

set -euo pipefail

BEAD_ID="bd-1dp9.6.7.8.4"
SCENARIO_ID="WAL-PUBLICATION-784"
SEED=784
TIMESTAMP_UTC="$(date -u +%Y%m%dT%H%M%SZ)"
RUN_ID="${BEAD_ID}-${TIMESTAMP_UTC}-${SEED}"
RUN_ID_SAFE="${RUN_ID//[^A-Za-z0-9_.-]/_}"
TRACE_ID="trace-${RUN_ID}"
ARTIFACT_DIR="artifacts/${BEAD_ID}/${RUN_ID}"
EVENTS_JSONL="${ARTIFACT_DIR}/events.jsonl"
REPORT_JSON="${ARTIFACT_DIR}/report.json"
SUMMARY_MD="${ARTIFACT_DIR}/summary.md"
HASHES_TXT="${ARTIFACT_DIR}/artifact_hashes.txt"
CORE_TARGET_DIR="${CORE_TARGET_DIR:-/tmp/bd_1dp9_6_7_8_4_core_wal_publication}"
WAL_TARGET_DIR="${WAL_TARGET_DIR:-/tmp/bd_1dp9_6_7_8_4_wal_publication}"

mkdir -p "${ARTIFACT_DIR}"

export RUST_LOG="${RUST_LOG:-fsqlite.wal_publication=trace,fsqlite_core::wal_adapter=debug,fsqlite_wal=debug}"
export RUST_TEST_THREADS="${RUST_TEST_THREADS:-1}"
export NO_COLOR="${NO_COLOR:-1}"

json_escape() {
  local value="${1//\\/\\\\}"
  value="${value//\"/\\\"}"
  value="${value//$'\n'/ }"
  printf '%s' "${value}"
}

emit_event() {
  local phase="$1"
  local event_type="$2"
  local outcome="$3"
  local elapsed_ms="$4"
  local message="$5"
  printf '{"trace_id":"%s","run_id":"%s","scenario_id":"%s","seed":%d,"phase":"%s","event_type":"%s","outcome":"%s","elapsed_ms":%s,"timestamp":"%s","message":"%s"}\n' \
    "${TRACE_ID}" "${RUN_ID}" "${SCENARIO_ID}" "${SEED}" "${phase}" "${event_type}" "${outcome}" "${elapsed_ms}" \
    "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$(json_escape "${message}")" >> "${EVENTS_JSONL}"
}

run_phase() {
  local phase="$1"
  local logfile="$2"
  shift 2

  emit_event "${phase}" "start" "running" 0 "running: $*"
  local started finished elapsed
  started="$(date +%s%3N)"

  if "$@" 2>&1 | tee "${logfile}"; then
    finished="$(date +%s%3N)"
    elapsed="$((finished - started))"
    if ! grep -Eq '^running [1-9][0-9]* tests?$' "${logfile}"; then
      emit_event "${phase}" "fail" "fail" "${elapsed}" "command completed without executing tests"
      return 1
    fi
    emit_event "${phase}" "pass" "pass" "${elapsed}" "completed successfully"
  else
    finished="$(date +%s%3N)"
    elapsed="$((finished - started))"
    emit_event "${phase}" "fail" "fail" "${elapsed}" "command failed"
    return 1
  fi
}

trace_contract_status() {
  local logfile="$1"
  shift

  local pattern
  for pattern in "$@"; do
    if ! grep -Eq "${pattern}" "${logfile}"; then
      printf 'missing'
      return 0
    fi
  done

  printf 'present'
}

require_publication_trace_contract() {
  local phase="$1"
  local logfile="$2"
  local description="$3"
  require_trace_contract \
    "${phase}" \
    "${logfile}" \
    "${description}" \
    'wal_generation' \
    'publication_seq' \
    'frame_delta_count' \
    'latest_frame_entries' \
    'snapshot_age' \
    'lookup_mode' \
    'fallback_reason'
}

require_reader_trace_contract() {
  local phase="$1"
  local logfile="$2"
  local description="$3"
  require_trace_contract \
    "${phase}" \
    "${logfile}" \
    "${description}" \
    'wal_checkpoint_seq' \
    'publication_seq' \
    'snapshot_age' \
    'lookup_mode' \
    'fallback_reason'
}

require_trace_contract() {
  local phase="$1"
  local logfile="$2"
  local description="$3"
  shift 3
  local status
  status="$(trace_contract_status "${logfile}" "$@")"
  emit_event "${phase}" "trace_contract" "${status}" 0 "${description} trace contract ${status}"
  if [[ "${status}" != "present" ]]; then
    emit_event "${phase}" "fail" "fail" 0 "${description} trace contract missing"
    return 1
  fi
}

hash_artifacts() {
  : > "${HASHES_TXT}"
  (
    cd "${ARTIFACT_DIR}"
    sha256sum ./*.log ./events.jsonl 2>/dev/null
  ) > "${HASHES_TXT}"
}

echo "=== ${BEAD_ID}: WAL publication plane verification ==="
echo "run_id=${RUN_ID}"
echo "trace_id=${TRACE_ID}"
echo "scenario_id=${SCENARIO_ID}"
echo "seed=${SEED}"
echo "artifacts=${ARTIFACT_DIR}"

emit_event "bootstrap" "start" "running" 0 "verification started"

run_phase \
  "commit_publication" \
  "${ARTIFACT_DIR}/commit_publication.log" \
  rch exec -- env RUST_LOG="${RUST_LOG}" RUST_TEST_THREADS="${RUST_TEST_THREADS}" NO_COLOR="${NO_COLOR}" CARGO_TARGET_DIR="${CORE_TARGET_DIR}" \
    cargo test -p fsqlite-core test_commit_append_publishes_visibility_snapshot -- --nocapture
require_publication_trace_contract \
  "commit_publication" \
  "${ARTIFACT_DIR}/commit_publication.log" \
  "publication"
TRACE_COMMIT_PUBLICATION="present"

run_phase \
  "prepared_commit_publication" \
  "${ARTIFACT_DIR}/prepared_commit_publication.log" \
  rch exec -- env RUST_LOG="${RUST_LOG}" RUST_TEST_THREADS="${RUST_TEST_THREADS}" NO_COLOR="${NO_COLOR}" CARGO_TARGET_DIR="${CORE_TARGET_DIR}" \
    cargo test -p fsqlite-core test_prepared_append_publishes_visibility_snapshot -- --nocapture
require_publication_trace_contract \
  "prepared_commit_publication" \
  "${ARTIFACT_DIR}/prepared_commit_publication.log" \
  "prepared publication"
TRACE_PREPARED_COMMIT_PUBLICATION="present"

run_phase \
  "reader_snapshot_binding" \
  "${ARTIFACT_DIR}/reader_snapshot_binding.log" \
  rch exec -- env RUST_LOG="${RUST_LOG}" RUST_TEST_THREADS="${RUST_TEST_THREADS}" NO_COLOR="${NO_COLOR}" CARGO_TARGET_DIR="${CORE_TARGET_DIR}" \
    cargo test -p fsqlite-core test_adapter_pins_read_snapshot_until_next_begin -- --nocapture
require_reader_trace_contract \
  "reader_snapshot_binding" \
  "${ARTIFACT_DIR}/reader_snapshot_binding.log" \
  "reader binding"
TRACE_READER_BINDING="present"

run_phase \
  "partial_index_lookup_contract" \
  "${ARTIFACT_DIR}/partial_index_lookup_contract.log" \
  rch exec -- env RUST_LOG="${RUST_LOG}" RUST_TEST_THREADS="${RUST_TEST_THREADS}" NO_COLOR="${NO_COLOR}" CARGO_TARGET_DIR="${CORE_TARGET_DIR}" \
    cargo test -p fsqlite-core test_lookup_contract_distinguishes_authoritative_and_fallback_paths -- --nocapture
require_publication_trace_contract \
  "partial_index_lookup_contract" \
  "${ARTIFACT_DIR}/partial_index_lookup_contract.log" \
  "lookup contract"
TRACE_LOOKUP_CONTRACT="present"

run_phase \
  "same_salt_generation_rollover" \
  "${ARTIFACT_DIR}/same_salt_generation_rollover.log" \
  rch exec -- env RUST_LOG="${RUST_LOG}" RUST_TEST_THREADS="${RUST_TEST_THREADS}" NO_COLOR="${NO_COLOR}" CARGO_TARGET_DIR="${CORE_TARGET_DIR}" \
    cargo test -p fsqlite-core test_page_index_invalidated_on_same_salt_generation_change -- --nocapture
require_publication_trace_contract \
  "same_salt_generation_rollover" \
  "${ARTIFACT_DIR}/same_salt_generation_rollover.log" \
  "generation rollover"
TRACE_SAME_SALT_ROLLOVER="present"

run_phase \
  "wal_refresh_generation_identity" \
  "${ARTIFACT_DIR}/wal_refresh_generation_identity.log" \
  rch exec -- env RUST_LOG="${RUST_LOG}" RUST_TEST_THREADS="${RUST_TEST_THREADS}" NO_COLOR="${NO_COLOR}" CARGO_TARGET_DIR="${WAL_TARGET_DIR}" \
    cargo test -p fsqlite-wal test_refresh_after_reset_with_same_salts_detects_new_generation -- --nocapture
emit_event "wal_refresh_generation_identity" "trace_contract" "not_applicable" 0 \
  "wal.rs generation-identity unit test does not emit wal_adapter publication logs"

hash_artifacts

cat > "${SUMMARY_MD}" <<EOF
# ${BEAD_ID} Verification Summary

- run_id: \`${RUN_ID}\`
- trace_id: \`${TRACE_ID}\`
- scenario_id: \`${SCENARIO_ID}\`
- seed: \`${SEED}\`

## Replay Commands

\`\`\`bash
RUST_LOG='${RUST_LOG}' rch exec -- env CARGO_TARGET_DIR=${CORE_TARGET_DIR} cargo test -p fsqlite-core test_commit_append_publishes_visibility_snapshot -- --nocapture
RUST_LOG='${RUST_LOG}' rch exec -- env CARGO_TARGET_DIR=${CORE_TARGET_DIR} cargo test -p fsqlite-core test_prepared_append_publishes_visibility_snapshot -- --nocapture
RUST_LOG='${RUST_LOG}' rch exec -- env CARGO_TARGET_DIR=${CORE_TARGET_DIR} cargo test -p fsqlite-core test_adapter_pins_read_snapshot_until_next_begin -- --nocapture
RUST_LOG='${RUST_LOG}' rch exec -- env CARGO_TARGET_DIR=${CORE_TARGET_DIR} cargo test -p fsqlite-core test_lookup_contract_distinguishes_authoritative_and_fallback_paths -- --nocapture
RUST_LOG='${RUST_LOG}' rch exec -- env CARGO_TARGET_DIR=${CORE_TARGET_DIR} cargo test -p fsqlite-core test_page_index_invalidated_on_same_salt_generation_change -- --nocapture
RUST_LOG='${RUST_LOG}' rch exec -- env CARGO_TARGET_DIR=${WAL_TARGET_DIR} cargo test -p fsqlite-wal test_refresh_after_reset_with_same_salts_detects_new_generation -- --nocapture
\`\`\`

## Trace Contract Detection

- commit_publication: \`${TRACE_COMMIT_PUBLICATION}\`
- prepared_commit_publication: \`${TRACE_PREPARED_COMMIT_PUBLICATION}\`
- reader_snapshot_binding: \`${TRACE_READER_BINDING}\`
- partial_index_lookup_contract: \`${TRACE_LOOKUP_CONTRACT}\`
- same_salt_generation_rollover: \`${TRACE_SAME_SALT_ROLLOVER}\`
- wal_refresh_generation_identity: \`not_applicable\`

## Artifact Hashes

\`\`\`
$(cat "${HASHES_TXT}")
\`\`\`
EOF

cat > "${REPORT_JSON}" <<EOF
{
  "trace_id": "${TRACE_ID}",
  "run_id": "${RUN_ID}",
  "scenario_id": "${SCENARIO_ID}",
  "seed": ${SEED},
  "bead_id": "${BEAD_ID}",
  "phases": [
    {"name":"commit_publication","trace_contract":"${TRACE_COMMIT_PUBLICATION}"},
    {"name":"prepared_commit_publication","trace_contract":"${TRACE_PREPARED_COMMIT_PUBLICATION}"},
    {"name":"reader_snapshot_binding","trace_contract":"${TRACE_READER_BINDING}"},
    {"name":"partial_index_lookup_contract","trace_contract":"${TRACE_LOOKUP_CONTRACT}"},
    {"name":"same_salt_generation_rollover","trace_contract":"${TRACE_SAME_SALT_ROLLOVER}"},
    {"name":"wal_refresh_generation_identity","trace_contract":"not_applicable"}
  ]
}
EOF

emit_event "complete" "finish" "pass" 0 "verification completed"
echo "verification artifacts written to ${ARTIFACT_DIR}"
