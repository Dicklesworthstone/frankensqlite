#!/usr/bin/env bash
# Verification gate for bd-26631.1:
# deterministic WAL startup replay scan/recovery with reproducible artifacts.

set -euo pipefail

BEAD_ID="bd-26631.1"
SCENARIO_ID="WAL-REPLAY-STARTUP-DETERMINISTIC"
SEED="${SEED:-2663101}"
TIMESTAMP_UTC="$(date -u +%Y%m%dT%H%M%SZ)"
RUN_ID="${BEAD_ID}-${TIMESTAMP_UTC}-${SEED}"
TRACE_ID="trace-${RUN_ID}"
ARTIFACT_DIR="artifacts/${BEAD_ID}/${RUN_ID}"
EVENTS_JSONL="${ARTIFACT_DIR}/events.jsonl"
REPORT_JSON="${ARTIFACT_DIR}/report.json"
MANIFEST_JSON="${ARTIFACT_DIR}/manifest.json"
SCENARIO_SUMMARY_JSONL="${ARTIFACT_DIR}/scenario_summary.jsonl"
LOG_CONTRACT="${ARTIFACT_DIR}/contract-check.log"
LOG_TEST="${ARTIFACT_DIR}/cargo-test-bd_26631_1_deterministic_replay_startup.log"
LOG_REGRESSION="${ARTIFACT_DIR}/cargo-test-replay-regression.log"

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

echo "=== ${BEAD_ID}: deterministic WAL startup replay verification ==="
echo "run_id=${RUN_ID}"
echo "trace_id=${TRACE_ID}"
echo "scenario_id=${SCENARIO_ID}"

emit_event "bootstrap" "start" "running" "verification started"

emit_event "contract_scan" "start" "running" "checking replay decision fields and scenario coverage"
if {
  rg -n "replay_cursor" crates/fsqlite-wal/src/wal.rs
  rg -n "frame_no" crates/fsqlite-wal/src/wal.rs
  rg -n "commit_boundary" crates/fsqlite-wal/src/wal.rs
  rg -n "decision_reason" crates/fsqlite-wal/src/wal.rs
  rg -n "scenario_truncated_tail_recovers_last_valid_commit" crates/fsqlite-wal/tests/bd_26631_1_deterministic_replay_startup.rs
  rg -n "scenario_duplicate_frames_remain_deterministic" crates/fsqlite-wal/tests/bd_26631_1_deterministic_replay_startup.rs
  rg -n "scenario_commit_boundary_drops_uncommitted_tail" crates/fsqlite-wal/tests/bd_26631_1_deterministic_replay_startup.rs
  rg -n "scenario_restart_loop_with_corruption_is_deterministic" crates/fsqlite-wal/tests/bd_26631_1_deterministic_replay_startup.rs
  rg -n "SCENARIO_OUTCOME:" crates/fsqlite-wal/tests/bd_26631_1_deterministic_replay_startup.rs
} >"${LOG_CONTRACT}" 2>&1; then
  emit_event "contract_scan" "pass" "pass" "contract markers found"
else
  emit_event "contract_scan" "fail" "fail" "contract markers missing"
  echo "[GATE FAIL] contract scan failed; see ${LOG_CONTRACT}" >&2
  exit 1
fi

emit_event "integration_tests" "start" "running" "running deterministic startup replay integration tests via rch"
if rch exec -- cargo test -p fsqlite-wal --test bd_26631_1_deterministic_replay_startup -- --nocapture --test-threads=1 2>&1 | tee "${LOG_TEST}" >/dev/null; then
  emit_event "integration_tests" "pass" "pass" "integration tests passed"
  TEST_RESULT="pass"
else
  emit_event "integration_tests" "fail" "fail" "integration tests failed"
  TEST_RESULT="fail"
fi

emit_event "regression_tests" "start" "running" "running startup replay regression tests via rch"
if rch exec -- cargo test -p fsqlite-wal --lib test_uncommitted_tail_dropped_on_reopen -- --nocapture --test-threads=1 >"${LOG_REGRESSION}" 2>&1; then
  emit_event "regression_tests" "pass" "pass" "regression tests passed"
  REGRESSION_RESULT="pass"
else
  emit_event "regression_tests" "fail" "fail" "regression tests failed"
  REGRESSION_RESULT="fail"
fi

if grep -E 'SCENARIO_OUTCOME:' "${LOG_TEST}" > /dev/null; then
  sed -n 's/.*SCENARIO_OUTCOME://p' "${LOG_TEST}" > "${SCENARIO_SUMMARY_JSONL}"
else
  : > "${SCENARIO_SUMMARY_JSONL}"
fi

scenario_rows="$(wc -l < "${SCENARIO_SUMMARY_JSONL}" | tr -d ' ')"
if [[ "${scenario_rows}" -lt 4 ]]; then
  emit_event "scenario_summary" "fail" "fail" "expected >=4 scenario summary rows, got ${scenario_rows}"
  SUMMARY_RESULT="fail"
else
  emit_event "scenario_summary" "pass" "pass" "scenario summary rows=${scenario_rows}"
  SUMMARY_RESULT="pass"
fi

if [[ "${TEST_RESULT}" == "pass" && "${REGRESSION_RESULT}" == "pass" && "${SUMMARY_RESULT}" == "pass" ]]; then
  RESULT="pass"
else
  RESULT="fail"
fi

contract_sha="$(sha256sum "${LOG_CONTRACT}" | awk '{print $1}')"
test_sha="$(sha256sum "${LOG_TEST}" | awk '{print $1}')"
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
    {"path":"${LOG_TEST}","sha256":"${test_sha}"},
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
  "test_log": "${LOG_TEST}",
  "test_log_sha256": "${test_sha}",
  "regression_log": "${LOG_REGRESSION}",
  "regression_log_sha256": "${regression_sha}",
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

echo "[GATE PASS] ${BEAD_ID} deterministic WAL startup replay gate passed"
echo "Artifacts: ${ARTIFACT_DIR}"
