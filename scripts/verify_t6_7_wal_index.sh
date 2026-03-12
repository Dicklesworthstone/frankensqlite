#!/usr/bin/env bash
# Verification prep gate for bd-1dp9.6.7.8.3:
# WAL truncate/torn-tail/corruption evidence pack for authoritative indexing work.

set -euo pipefail

BEAD_ID="bd-1dp9.6.7.8.3"
SCENARIO_ID="WAL-INDEX-7813"
SEED=7813
TIMESTAMP_UTC="$(date -u +%Y%m%dT%H%M%SZ)"
RUN_ID="${BEAD_ID}-${TIMESTAMP_UTC}-${SEED}"
RUN_ID_SAFE="${RUN_ID//[^A-Za-z0-9_.-]/_}"
TRACE_ID="trace-${RUN_ID}"
ARTIFACT_DIR="artifacts/${BEAD_ID}/${RUN_ID}"
EVENTS_JSONL="${ARTIFACT_DIR}/events.jsonl"
REPORT_JSON="${ARTIFACT_DIR}/report.json"
SUMMARY_MD="${ARTIFACT_DIR}/summary.md"
HASHES_TXT="${ARTIFACT_DIR}/artifact_hashes.txt"
CORE_TARGET_DIR="${CORE_TARGET_DIR:-/tmp/bd_1dp9_6_7_8_3_core_wal_index}"
WAL_TARGET_DIR="${WAL_TARGET_DIR:-/tmp/bd_1dp9_6_7_8_3_wal_index}"

mkdir -p "${ARTIFACT_DIR}"

export RUST_LOG="${RUST_LOG:-fsqlite_core::wal_adapter=debug,fsqlite_wal=trace}"
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
  local mode_pattern="$2"

  if grep -Eq 'wal_checkpoint_seq' "${logfile}" \
    && grep -Eq 'wal_salt1' "${logfile}" \
    && grep -Eq 'wal_salt2' "${logfile}" \
    && grep -Eq 'lookup_mode' "${logfile}" \
    && grep -Eq "${mode_pattern}" "${logfile}"; then
    printf 'present'
  else
    printf 'missing'
  fi
}

require_trace_contract() {
  local phase="$1"
  local logfile="$2"
  local mode_pattern="$3"
  local description="$4"
  local status
  status="$(trace_contract_status "${logfile}" "${mode_pattern}")"
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

echo "=== ${BEAD_ID}: WAL authoritative-index verification prep ==="
echo "run_id=${RUN_ID}"
echo "trace_id=${TRACE_ID}"
echo "scenario_id=${SCENARIO_ID}"
echo "seed=${SEED}"
echo "artifacts=${ARTIFACT_DIR}"

emit_event "bootstrap" "start" "running" 0 "verification started"

run_phase \
  "wal_reset_new_generation" \
  "${ARTIFACT_DIR}/wal_reset_new_generation.log" \
  rch exec -- env RUST_LOG="${RUST_LOG}" RUST_TEST_THREADS="${RUST_TEST_THREADS}" NO_COLOR="${NO_COLOR}" CARGO_TARGET_DIR="${CORE_TARGET_DIR}" \
    cargo test -p fsqlite-core test_page_index_invalidated_on_wal_reset -- --nocapture
require_trace_contract \
  "wal_reset_new_generation" \
  "${ARTIFACT_DIR}/wal_reset_new_generation.log" \
  'authoritative_index' \
  "wal reset new generation"
TRACE_WAL_RESET_NEW_GENERATION="present"

run_phase \
  "wal_reset_same_salts_generation_change" \
  "${ARTIFACT_DIR}/wal_reset_same_salts_generation_change.log" \
  rch exec -- env RUST_LOG="${RUST_LOG}" RUST_TEST_THREADS="${RUST_TEST_THREADS}" NO_COLOR="${NO_COLOR}" CARGO_TARGET_DIR="${CORE_TARGET_DIR}" \
    cargo test -p fsqlite-core test_page_index_invalidated_on_same_salt_generation_change -- --nocapture
require_trace_contract \
  "wal_reset_same_salts_generation_change" \
  "${ARTIFACT_DIR}/wal_reset_same_salts_generation_change.log" \
  'authoritative_index' \
  "wal reset same-salts generation change"
TRACE_WAL_RESET_SAME_SALTS="present"

run_phase \
  "wal_lookup_contract" \
  "${ARTIFACT_DIR}/wal_lookup_contract.log" \
  rch exec -- env RUST_LOG="${RUST_LOG}" RUST_TEST_THREADS="${RUST_TEST_THREADS}" NO_COLOR="${NO_COLOR}" CARGO_TARGET_DIR="${CORE_TARGET_DIR}" \
    cargo test -p fsqlite-core test_lookup_contract_distinguishes_authoritative_and_fallback_paths -- --nocapture
require_trace_contract \
  "wal_lookup_contract" \
  "${ARTIFACT_DIR}/wal_lookup_contract.log" \
  'partial_index_fallback|authoritative_index' \
  "wal lookup contract"
TRACE_WAL_LOOKUP_CONTRACT="present"

run_phase \
  "wal_partial_index_fallback" \
  "${ARTIFACT_DIR}/wal_partial_index_fallback.log" \
  rch exec -- env RUST_LOG="${RUST_LOG}" RUST_TEST_THREADS="${RUST_TEST_THREADS}" NO_COLOR="${NO_COLOR}" CARGO_TARGET_DIR="${CORE_TARGET_DIR}" \
    cargo test -p fsqlite-core test_partial_index_falls_back_to_linear_scan -- --nocapture
require_trace_contract \
  "wal_partial_index_fallback" \
  "${ARTIFACT_DIR}/wal_partial_index_fallback.log" \
  'partial_index_fallback' \
  "wal partial-index fallback"
TRACE_WAL_PARTIAL_INDEX="present"

run_phase \
  "wal_truncate_recovery_matrix" \
  "${ARTIFACT_DIR}/wal_truncate_recovery_matrix.log" \
  rch exec -- env RUST_LOG="${RUST_LOG}" RUST_TEST_THREADS="${RUST_TEST_THREADS}" NO_COLOR="${NO_COLOR}" CARGO_TARGET_DIR="${WAL_TARGET_DIR}" \
    cargo test -p fsqlite-wal test_crash_matrix_truncate_at_every_frame_boundary -- --nocapture
emit_event "wal_truncate_recovery_matrix" "trace_contract" "not_applicable" 0 \
  "runtime trace contract not asserted for wal.rs crash-matrix unit test"

run_phase \
  "wal_corruption_recovery_matrix" \
  "${ARTIFACT_DIR}/wal_corruption_recovery_matrix.log" \
  rch exec -- env RUST_LOG="${RUST_LOG}" RUST_TEST_THREADS="${RUST_TEST_THREADS}" NO_COLOR="${NO_COLOR}" CARGO_TARGET_DIR="${WAL_TARGET_DIR}" \
    cargo test -p fsqlite-wal test_crash_matrix_bit_flip_at_every_frame -- --nocapture
emit_event "wal_corruption_recovery_matrix" "trace_contract" "not_applicable" 0 \
  "runtime trace contract not asserted for wal.rs crash-matrix unit test"

run_phase \
  "wal_reset_then_crash_matrix" \
  "${ARTIFACT_DIR}/wal_reset_then_crash_matrix.log" \
  rch exec -- env RUST_LOG="${RUST_LOG}" RUST_TEST_THREADS="${RUST_TEST_THREADS}" NO_COLOR="${NO_COLOR}" CARGO_TARGET_DIR="${WAL_TARGET_DIR}" \
    cargo test -p fsqlite-wal test_crash_matrix_reset_then_crash -- --nocapture
emit_event "wal_reset_then_crash_matrix" "trace_contract" "not_applicable" 0 \
  "runtime trace contract not asserted for wal.rs crash-matrix unit test"

hash_artifacts

cat > "${SUMMARY_MD}" <<EOF
# ${BEAD_ID} Verification Prep Summary

- run_id: \`${RUN_ID}\`
- trace_id: \`${TRACE_ID}\`
- scenario_id: \`${SCENARIO_ID}\`
- seed: \`${SEED}\`

## Replay Commands

\`\`\`bash
RUST_LOG='${RUST_LOG}' rch exec -- env CARGO_TARGET_DIR=${CORE_TARGET_DIR} cargo test -p fsqlite-core test_page_index_invalidated_on_wal_reset -- --nocapture
RUST_LOG='${RUST_LOG}' rch exec -- env CARGO_TARGET_DIR=${CORE_TARGET_DIR} cargo test -p fsqlite-core test_page_index_invalidated_on_same_salt_generation_change -- --nocapture
RUST_LOG='${RUST_LOG}' rch exec -- env CARGO_TARGET_DIR=${CORE_TARGET_DIR} cargo test -p fsqlite-core test_lookup_contract_distinguishes_authoritative_and_fallback_paths -- --nocapture
RUST_LOG='${RUST_LOG}' rch exec -- env CARGO_TARGET_DIR=${CORE_TARGET_DIR} cargo test -p fsqlite-core test_partial_index_falls_back_to_linear_scan -- --nocapture
RUST_LOG='${RUST_LOG}' rch exec -- env CARGO_TARGET_DIR=${WAL_TARGET_DIR} cargo test -p fsqlite-wal test_crash_matrix_truncate_at_every_frame_boundary -- --nocapture
RUST_LOG='${RUST_LOG}' rch exec -- env CARGO_TARGET_DIR=${WAL_TARGET_DIR} cargo test -p fsqlite-wal test_crash_matrix_bit_flip_at_every_frame -- --nocapture
RUST_LOG='${RUST_LOG}' rch exec -- env CARGO_TARGET_DIR=${WAL_TARGET_DIR} cargo test -p fsqlite-wal test_crash_matrix_reset_then_crash -- --nocapture
\`\`\`

## Trace Contract Detection

- wal_reset_new_generation: \`${TRACE_WAL_RESET_NEW_GENERATION}\`
- wal_reset_same_salts_generation_change: \`${TRACE_WAL_RESET_SAME_SALTS}\`
- wal_lookup_contract: \`${TRACE_WAL_LOOKUP_CONTRACT}\`
- wal_partial_index_fallback: \`${TRACE_WAL_PARTIAL_INDEX}\`
- wal_truncate_recovery_matrix: \`not_applicable\`
- wal_corruption_recovery_matrix: \`not_applicable\`
- wal_reset_then_crash_matrix: \`not_applicable\`

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
  "result": "pass",
  "rust_log": "$(json_escape "${RUST_LOG}")",
  "rust_test_threads": "${RUST_TEST_THREADS}",
  "events_jsonl": "${EVENTS_JSONL}",
  "summary_md": "${SUMMARY_MD}",
  "artifact_hashes": "${HASHES_TXT}",
  "trace_contract": {
    "wal_reset_new_generation": "${TRACE_WAL_RESET_NEW_GENERATION}",
    "wal_reset_same_salts_generation_change": "${TRACE_WAL_RESET_SAME_SALTS}",
    "wal_lookup_contract": "${TRACE_WAL_LOOKUP_CONTRACT}",
    "wal_partial_index_fallback": "${TRACE_WAL_PARTIAL_INDEX}",
    "wal_truncate_recovery_matrix": "not_applicable",
    "wal_corruption_recovery_matrix": "not_applicable",
    "wal_reset_then_crash_matrix": "not_applicable"
  },
  "log_files": [
    "${ARTIFACT_DIR}/wal_reset_new_generation.log",
    "${ARTIFACT_DIR}/wal_reset_same_salts_generation_change.log",
    "${ARTIFACT_DIR}/wal_lookup_contract.log",
    "${ARTIFACT_DIR}/wal_partial_index_fallback.log",
    "${ARTIFACT_DIR}/wal_truncate_recovery_matrix.log",
    "${ARTIFACT_DIR}/wal_corruption_recovery_matrix.log",
    "${ARTIFACT_DIR}/wal_reset_then_crash_matrix.log"
  ]
}
EOF

emit_event "finalize" "pass" "pass" 0 "report written to ${REPORT_JSON}"
echo "[GATE PASS] ${BEAD_ID} WAL index verification prep passed"
