#!/usr/bin/env bash
# Verification gate for bd-xfn30.3:
# deterministic WAL fault-injection crash matrix with replayable artifacts.

set -euo pipefail

BEAD_ID="bd-xfn30.3"
SCENARIO_ID="WAL-FAULT-MATRIX-E2E"
SEED="${SEED:-40101}"
TIMESTAMP_UTC="$(date -u +%Y%m%dT%H%M%SZ)"
RUN_ID="${BEAD_ID}-${TIMESTAMP_UTC}-${SEED}"
TRACE_ID="trace-${RUN_ID}"
ARTIFACT_DIR="artifacts/${BEAD_ID}/${RUN_ID}"
EVENTS_JSONL="${ARTIFACT_DIR}/events.jsonl"
REPORT_JSON="${ARTIFACT_DIR}/report.json"
MANIFEST_JSON="${ARTIFACT_DIR}/manifest.json"
SCENARIO_SUMMARY_JSONL="${ARTIFACT_DIR}/scenario_summary.jsonl"
LOG_CONTRACT="${ARTIFACT_DIR}/contract-check.log"
LOG_MATRIX="${ARTIFACT_DIR}/cargo-test-bd_xfn30_3_fault_injection_matrix.log"
LOG_REGRESSION="${ARTIFACT_DIR}/cargo-test-wal-crash-matrix-regression.log"

mkdir -p "${ARTIFACT_DIR}"
start_ns="$(date +%s%N)"

emit_event() {
  local phase="$1"
  local event_type="$2"
  local outcome="$3"
  local message="$4"
  local now_ns elapsed_ms
  now_ns="$(date +%s%N)"
  elapsed_ms="$(( (now_ns - start_ns) / 1000000 ))"
  printf '{"trace_id":"%s","run_id":"%s","scenario_id":"%s","seed":%d,"phase":"%s","event_type":"%s","outcome":"%s","elapsed_ms":%d,"timestamp":"%s","message":"%s"}\n' \
    "${TRACE_ID}" "${RUN_ID}" "${SCENARIO_ID}" "${SEED}" "${phase}" "${event_type}" "${outcome}" "${elapsed_ms}" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "${message}" \
    >> "${EVENTS_JSONL}"
}

echo "=== ${BEAD_ID}: WAL fault-injection matrix verification ==="
echo "run_id=${RUN_ID}"
echo "trace_id=${TRACE_ID}"
echo "scenario_id=${SCENARIO_ID}"

emit_event "bootstrap" "start" "running" "verification started"

emit_event "contract_scan" "start" "running" "checking matrix scenario coverage and replay contract"
if {
  rg -n "scenario_mid_commit_kill_recovery" crates/fsqlite-wal/tests/bd_xfn30_3_fault_injection_matrix.rs
  rg -n "scenario_restart_loop_recovery" crates/fsqlite-wal/tests/bd_xfn30_3_fault_injection_matrix.rs
  rg -n "scenario_torn_write_recovery" crates/fsqlite-wal/tests/bd_xfn30_3_fault_injection_matrix.rs
  rg -n "scenario_checksum_corruption_recovery" crates/fsqlite-wal/tests/bd_xfn30_3_fault_injection_matrix.rs
  rg -n "scenario_partial_fsync_recovery" crates/fsqlite-wal/tests/bd_xfn30_3_fault_injection_matrix.rs
  rg -n "SCENARIO_OUTCOME:" crates/fsqlite-wal/tests/bd_xfn30_3_fault_injection_matrix.rs
} >"${LOG_CONTRACT}" 2>&1; then
  emit_event "contract_scan" "pass" "pass" "matrix contract markers found"
else
  emit_event "contract_scan" "fail" "fail" "matrix contract markers missing"
  echo "[GATE FAIL] contract scan failed; see ${LOG_CONTRACT}" >&2
  exit 1
fi

emit_event "matrix_tests" "start" "running" "running deterministic matrix integration test via rch"
if rch exec -- cargo test -p fsqlite-wal --test bd_xfn30_3_fault_injection_matrix -- --nocapture --test-threads=1 >"${LOG_MATRIX}" 2>&1; then
  emit_event "matrix_tests" "pass" "pass" "matrix integration tests passed"
  MATRIX_RESULT="pass"
else
  emit_event "matrix_tests" "fail" "fail" "matrix integration tests failed"
  MATRIX_RESULT="fail"
fi

emit_event "regression_tests" "start" "running" "running wal crash-matrix regression tests via rch"
if rch exec -- cargo test -p fsqlite-wal --lib test_crash_matrix_ -- --nocapture --test-threads=1 >"${LOG_REGRESSION}" 2>&1; then
  emit_event "regression_tests" "pass" "pass" "wal crash-matrix regression tests passed"
  REGRESSION_RESULT="pass"
else
  emit_event "regression_tests" "fail" "fail" "wal crash-matrix regression tests failed"
  REGRESSION_RESULT="fail"
fi

if grep -E 'SCENARIO_OUTCOME:' "${LOG_MATRIX}" > /dev/null; then
  sed -n 's/.*SCENARIO_OUTCOME://p' "${LOG_MATRIX}" > "${SCENARIO_SUMMARY_JSONL}"
else
  : > "${SCENARIO_SUMMARY_JSONL}"
fi

scenario_rows="$(wc -l < "${SCENARIO_SUMMARY_JSONL}" | tr -d ' ')"
if [[ "${scenario_rows}" -lt 5 ]]; then
  emit_event "scenario_summary" "fail" "fail" "expected >=5 scenario summary rows, got ${scenario_rows}"
  SUMMARY_RESULT="fail"
else
  emit_event "scenario_summary" "pass" "pass" "scenario summary rows=${scenario_rows}"
  SUMMARY_RESULT="pass"
fi

if [[ "${MATRIX_RESULT}" == "pass" && "${REGRESSION_RESULT}" == "pass" && "${SUMMARY_RESULT}" == "pass" ]]; then
  RESULT="pass"
else
  RESULT="fail"
fi

contract_sha="$(sha256sum "${LOG_CONTRACT}" | awk '{print $1}')"
matrix_sha="$(sha256sum "${LOG_MATRIX}" | awk '{print $1}')"
regression_sha="$(sha256sum "${LOG_REGRESSION}" | awk '{print $1}')"
summary_sha="$(sha256sum "${SCENARIO_SUMMARY_JSONL}" | awk '{print $1}')"

cat > "${MANIFEST_JSON}" <<EOF_MANIFEST
{
  "trace_id": "${TRACE_ID}",
  "run_id": "${RUN_ID}",
  "scenario_id": "${SCENARIO_ID}",
  "seed": ${SEED},
  "bead_id": "${BEAD_ID}",
  "files": [
    {"path":"${EVENTS_JSONL}"},
    {"path":"${LOG_CONTRACT}","sha256":"${contract_sha}"},
    {"path":"${LOG_MATRIX}","sha256":"${matrix_sha}"},
    {"path":"${LOG_REGRESSION}","sha256":"${regression_sha}"},
    {"path":"${SCENARIO_SUMMARY_JSONL}","sha256":"${summary_sha}"}
  ]
}
EOF_MANIFEST

manifest_sha="$(sha256sum "${MANIFEST_JSON}" | awk '{print $1}')"

cat > "${REPORT_JSON}" <<EOF_REPORT
{
  "trace_id": "${TRACE_ID}",
  "run_id": "${RUN_ID}",
  "scenario_id": "${SCENARIO_ID}",
  "seed": ${SEED},
  "bead_id": "${BEAD_ID}",
  "events_jsonl": "${EVENTS_JSONL}",
  "scenario_summary_jsonl": "${SCENARIO_SUMMARY_JSONL}",
  "scenario_summary_rows": ${scenario_rows},
  "contract_log": "${LOG_CONTRACT}",
  "contract_log_sha256": "${contract_sha}",
  "matrix_test_log": "${LOG_MATRIX}",
  "matrix_test_log_sha256": "${matrix_sha}",
  "regression_test_log": "${LOG_REGRESSION}",
  "regression_test_log_sha256": "${regression_sha}",
  "manifest_json": "${MANIFEST_JSON}",
  "manifest_sha256": "${manifest_sha}",
  "result": "${RESULT}"
}
EOF_REPORT

emit_event "finalize" "info" "${RESULT}" "report written to ${REPORT_JSON}"

if [[ "${RESULT}" != "pass" ]]; then
  echo "[GATE FAIL] ${BEAD_ID} verification failed"
  echo "See ${REPORT_JSON}"
  exit 1
fi

echo "[GATE PASS] ${BEAD_ID} fault-injection matrix gate passed"
echo "Artifacts: ${ARTIFACT_DIR}"
