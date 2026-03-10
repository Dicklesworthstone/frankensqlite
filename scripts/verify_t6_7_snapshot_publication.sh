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

export RUST_LOG="${RUST_LOG:-fsqlite_core=trace,fsqlite_pager=trace}"
export RUST_TEST_THREADS="${RUST_TEST_THREADS:-1}"

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

run_phase \
  "file_backed_strict_visibility_matrix" \
  "${ARTIFACT_DIR}/file_backed_strict_visibility_matrix.log" \
  rch exec -- cargo test -p fsqlite-core test_visibility_interleavings_fixed_seed_matrix -- --nocapture

run_phase \
  "file_backed_stale_refresh" \
  "${ARTIFACT_DIR}/file_backed_stale_refresh.log" \
  rch exec -- cargo test -p fsqlite-core test_memdb_visible_commit_seq_drives_stale_detection -- --nocapture

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
