#!/usr/bin/env bash
# Verification gate for bd-1dp9.6.7.7.4:
# MVCC-native snapshot publication plane for pager metadata and page visibility.

set -euo pipefail

BEAD_ID="bd-1dp9.6.7.7.4"
SCENARIO_ID="SNAPSHOT-PUBLICATION-3520"
SEED=3520
TIMESTAMP_UTC="$(date -u +%Y%m%dT%H%M%SZ)"
RUN_ID="${BEAD_ID}-${TIMESTAMP_UTC}-${SEED}"
TRACE_ID="trace-${RUN_ID}"
ARTIFACT_DIR="artifacts/${BEAD_ID}/${RUN_ID}"
EVENTS_JSONL="${ARTIFACT_DIR}/events.jsonl"
REPORT_JSON="${ARTIFACT_DIR}/report.json"

mkdir -p "${ARTIFACT_DIR}"

export RUST_LOG="${RUST_LOG:-fsqlite.snapshot_publication=trace,fsqlite_core=trace,fsqlite_pager=trace}"
export RUST_TEST_THREADS="${RUST_TEST_THREADS:-1}"
export NO_COLOR="${NO_COLOR:-1}"

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
  printf '{"trace_id":"%s","run_id":"%s","scenario_id":"%s","seed":%d,"phase":"%s","event_type":"%s","outcome":"%s","elapsed_ms":%s,"timestamp":"%s","message":"%s"}\n' \
    "${TRACE_ID}" "${RUN_ID}" "${SCENARIO_ID}" "${SEED}" "${phase}" "${event_type}" "${outcome}" "${elapsed_ms}" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "${message}" \
    >> "${EVENTS_JSONL}"
}

run_phase() {
  local phase="$1"
  local logfile="$2"
  shift 2

  emit_event "${phase}" "start" "running" 0 "running: $*"
  local started
  started="$(date +%s%3N)"

  if "$@" 2>&1 | tee "${logfile}"; then
    local finished elapsed
    finished="$(date +%s%3N)"
    elapsed="$((finished - started))"
    if ! grep -Eq '^running [1-9][0-9]* tests?$' "${logfile}"; then
      emit_event "${phase}" "fail" "fail" "${elapsed}" "command completed without executing any tests: $*"
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

echo "=== ${BEAD_ID}: snapshot publication verification ==="
echo "run_id=${RUN_ID}"
echo "trace_id=${TRACE_ID}"
echo "scenario_id=${SCENARIO_ID}"
echo "seed=${SEED}"
echo "artifacts=${ARTIFACT_DIR}"

emit_event "bootstrap" "start" "running" 0 "verification started"

run_phase \
  "pager_publication_unit_matrix" \
  "${ARTIFACT_DIR}/pager_publication_unit_matrix.log" \
  rch exec -- cargo test -p fsqlite-pager published_ -- --nocapture
assert_publication_trace_contract \
  "pager_publication_unit_matrix" \
  "${ARTIFACT_DIR}/pager_publication_unit_matrix.log" \
  'published pager snapshot|served page from published snapshot|resolved zero-filled page from published metadata'

run_phase \
  "file_backed_strict_visibility_matrix" \
  "${ARTIFACT_DIR}/file_backed_strict_visibility_matrix.log" \
  rch exec -- cargo test -p fsqlite-core connection::pager_routing_tests::test_visibility_interleavings_fixed_seed_matrix -- --exact --nocapture
assert_publication_trace_contract \
  "file_backed_strict_visibility_matrix" \
  "${ARTIFACT_DIR}/file_backed_strict_visibility_matrix.log" \
  'bound connection to pager snapshot'

run_phase \
  "file_backed_stale_refresh" \
  "${ARTIFACT_DIR}/file_backed_stale_refresh.log" \
  rch exec -- cargo test -p fsqlite-core connection::pager_routing_tests::test_memdb_visible_commit_seq_drives_stale_detection -- --exact --nocapture
assert_publication_trace_contract \
  "file_backed_stale_refresh" \
  "${ARTIFACT_DIR}/file_backed_stale_refresh.log" \
  'bound connection to pager snapshot'

cat > "${REPORT_JSON}" <<EOF
{
  "trace_id": "${TRACE_ID}",
  "run_id": "${RUN_ID}",
  "scenario_id": "${SCENARIO_ID}",
  "seed": ${SEED},
  "bead_id": "${BEAD_ID}",
  "rust_log": "${RUST_LOG}",
  "rust_test_threads": "${RUST_TEST_THREADS}",
  "events_jsonl": "${EVENTS_JSONL}",
  "log_files": [
    "${ARTIFACT_DIR}/pager_publication_unit_matrix.log",
    "${ARTIFACT_DIR}/file_backed_strict_visibility_matrix.log",
    "${ARTIFACT_DIR}/file_backed_stale_refresh.log"
  ],
  "result": "pass"
}
EOF

emit_event "finalize" "pass" "pass" 0 "report written to ${REPORT_JSON}"
echo "[GATE PASS] ${BEAD_ID} snapshot publication gate passed"
