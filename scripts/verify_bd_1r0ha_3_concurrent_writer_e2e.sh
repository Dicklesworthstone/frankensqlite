#!/usr/bin/env bash
# Verification gate for bd-1r0ha.3:
# deterministic concurrent-writer e2e with fairness/latency evidence.

set -euo pipefail

BEAD_ID="bd-1r0ha.3"
SCENARIO_ID="MVCC-CONCURRENT-WRITER-E2E"
SEED=61301
TIMESTAMP_UTC="$(date -u +%Y%m%dT%H%M%SZ)"
RUN_ID="${BEAD_ID}-${TIMESTAMP_UTC}-${SEED}"
TRACE_ID="trace-${RUN_ID}"
ARTIFACT_DIR="artifacts/${BEAD_ID}/${RUN_ID}"
EVENTS_JSONL="${ARTIFACT_DIR}/events.jsonl"
REPORT_JSON="${ARTIFACT_DIR}/report.json"
MANIFEST_JSON="${ARTIFACT_DIR}/manifest.json"
LOG_TEST="${ARTIFACT_DIR}/bd_1r0ha_3_test.log"
SCENARIO_SUMMARY_JSONL="${ARTIFACT_DIR}/scenario_summary.jsonl"
REPLAY_COMMAND="cargo test -p fsqlite-e2e --test bd_1r0ha_3_concurrent_writer_e2e -- --nocapture --test-threads=1"
START_EPOCH="$(date +%s)"

mkdir -p "${ARTIFACT_DIR}"

elapsed_ms() {
  local now
  now="$(date +%s)"
  local delta="$((now - START_EPOCH))"
  echo "$((delta * 1000))"
}

emit_event() {
  local phase="$1"
  local event_type="$2"
  local outcome="$3"
  local message="$4"
  printf '{"trace_id":"%s","run_id":"%s","scenario_id":"%s","seed":%d,"phase":"%s","event_type":"%s","outcome":"%s","elapsed_ms":%s,"timestamp":"%s","message":"%s"}\n' \
    "${TRACE_ID}" "${RUN_ID}" "${SCENARIO_ID}" "${SEED}" "${phase}" "${event_type}" "${outcome}" "$(elapsed_ms)" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "${message}" \
    >> "${EVENTS_JSONL}"
}

echo "=== ${BEAD_ID}: deterministic concurrent-writer e2e verification ==="
echo "run_id=${RUN_ID}"
echo "trace_id=${TRACE_ID}"
echo "scenario_id=${SCENARIO_ID}"
echo "replay=${REPLAY_COMMAND}"

emit_event "bootstrap" "start" "running" "verification started"

emit_event "test_matrix" "start" "running" "running deterministic concurrent-writer e2e test matrix"
if rch exec -- cargo test -p fsqlite-e2e --test bd_1r0ha_3_concurrent_writer_e2e -- --nocapture --test-threads=1 2>&1 | tee "${LOG_TEST}"; then
  emit_event "test_matrix" "pass" "pass" "bd-1r0ha.3 test matrix passed"
  RESULT="pass"
else
  emit_event "test_matrix" "fail" "fail" "bd-1r0ha.3 test matrix failed"
  RESULT="fail"
fi

emit_event "extract_outcomes" "start" "running" "extracting SCENARIO_OUTCOME JSON lines"
if grep -E 'SCENARIO_OUTCOME:' "${LOG_TEST}" > /dev/null; then
  sed -n 's/.*SCENARIO_OUTCOME://p' "${LOG_TEST}" > "${SCENARIO_SUMMARY_JSONL}"
else
  : > "${SCENARIO_SUMMARY_JSONL}"
fi
SCENARIO_ROWS="$(wc -l < "${SCENARIO_SUMMARY_JSONL}" | tr -d ' ')"
emit_event "extract_outcomes" "info" "${RESULT}" "scenario_summary_rows=${SCENARIO_ROWS}"

if [[ "${RESULT}" == "pass" && "${SCENARIO_ROWS}" -lt 3 ]]; then
  emit_event "extract_outcomes" "fail" "fail" "expected >=3 scenario summaries"
  RESULT="fail"
fi

EVENTS_SHA="$(sha256sum "${EVENTS_JSONL}" | awk '{print $1}')"
TEST_SHA="$(sha256sum "${LOG_TEST}" | awk '{print $1}')"
SUMMARY_SHA="$(sha256sum "${SCENARIO_SUMMARY_JSONL}" | awk '{print $1}')"

cat > "${MANIFEST_JSON}" <<EOF
{
  "bead_id": "${BEAD_ID}",
  "scenario_id": "${SCENARIO_ID}",
  "run_id": "${RUN_ID}",
  "trace_id": "${TRACE_ID}",
  "seed": ${SEED},
  "replay_command": "${REPLAY_COMMAND}",
  "artifacts": [
    {"path":"${EVENTS_JSONL}","sha256":"${EVENTS_SHA}"},
    {"path":"${LOG_TEST}","sha256":"${TEST_SHA}"},
    {"path":"${SCENARIO_SUMMARY_JSONL}","sha256":"${SUMMARY_SHA}"}
  ]
}
EOF

emit_event "manifest" "info" "${RESULT}" "manifest written to ${MANIFEST_JSON}"

cat > "${REPORT_JSON}" <<EOF
{
  "bead_id": "${BEAD_ID}",
  "scenario_id": "${SCENARIO_ID}",
  "run_id": "${RUN_ID}",
  "trace_id": "${TRACE_ID}",
  "seed": ${SEED},
  "result": "${RESULT}",
  "events_jsonl": "${EVENTS_JSONL}",
  "scenario_summary_jsonl": "${SCENARIO_SUMMARY_JSONL}",
  "scenario_summary_rows": ${SCENARIO_ROWS},
  "test_log": "${LOG_TEST}",
  "manifest_json": "${MANIFEST_JSON}",
  "replay_command": "${REPLAY_COMMAND}"
}
EOF

emit_event "finalize" "info" "${RESULT}" "report written to ${REPORT_JSON}"

if [[ "${RESULT}" != "pass" ]]; then
  echo "[GATE FAIL] ${BEAD_ID} deterministic concurrent-writer e2e gate failed"
  exit 1
fi

echo "[GATE PASS] ${BEAD_ID} deterministic concurrent-writer e2e gate passed"
