#!/usr/bin/env bash
# Verification gate for bd-2yqp6.2.1:
# enforce true C-SQLite oracle wiring for parity differential runners.

set -euo pipefail

BEAD_ID="bd-2yqp6.2.1"
SCENARIO_ID="PARITY-ORACLE-B1"
SEED="${SEED:-3520}"
TIMESTAMP_UTC="$(date -u +%Y%m%dT%H%M%SZ)"
RUN_ID="${BEAD_ID}-${TIMESTAMP_UTC}-${SEED}"
TRACE_ID="trace-${RUN_ID}"
ARTIFACT_DIR="artifacts/${BEAD_ID}/${RUN_ID}"
EVENTS_JSONL="${ARTIFACT_DIR}/events.jsonl"
REPORT_JSON="${ARTIFACT_DIR}/report.json"
LOG_CONTRACT="${ARTIFACT_DIR}/contract-check.log"
LOG_BEAD_TEST="${ARTIFACT_DIR}/cargo-test-bd_2yqp6_2_1_oracle_wiring.log"
LOG_DIFF_V2_TEST="${ARTIFACT_DIR}/cargo-test-differential_v2_test.log"

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

echo "=== ${BEAD_ID}: oracle wiring verification ==="
echo "run_id=${RUN_ID}"
echo "trace_id=${TRACE_ID}"
echo "scenario_id=${SCENARIO_ID}"

emit_event "bootstrap" "start" "running" "verification started"

emit_event "contract_scan" "start" "running" "checking manifest runner oracle wiring contract"
if {
  rg -n "CsqliteExecutor::open_in_memory" crates/fsqlite-harness/src/bin/differential_manifest_runner.rs
  rg -n "reference executor must identify as CSqliteOracle|envelope.engines.csqlite must be non-empty" crates/fsqlite-harness/src/differential_v2.rs
} >"${LOG_CONTRACT}" 2>&1; then
  emit_event "contract_scan" "pass" "pass" "oracle wiring contract markers found"
else
  emit_event "contract_scan" "fail" "fail" "oracle wiring contract markers missing"
  echo "[GATE FAIL] contract scan failed; see ${LOG_CONTRACT}" >&2
  exit 1
fi

emit_event "bead_tests" "start" "running" "running bead-specific oracle wiring tests via rch"
if rch exec -- cargo test -p fsqlite-harness --test bd_2yqp6_2_1_oracle_wiring -- --nocapture >"${LOG_BEAD_TEST}" 2>&1; then
  emit_event "bead_tests" "pass" "pass" "bd_2yqp6_2_1_oracle_wiring passed"
  RESULT_BEAD="pass"
else
  emit_event "bead_tests" "fail" "fail" "bd_2yqp6_2_1_oracle_wiring failed"
  RESULT_BEAD="fail"
fi

emit_event "regression_tests" "start" "running" "running differential_v2_test regression suite via rch"
if rch exec -- cargo test -p fsqlite-harness --test differential_v2_test -- --nocapture >"${LOG_DIFF_V2_TEST}" 2>&1; then
  emit_event "regression_tests" "pass" "pass" "differential_v2_test passed"
  RESULT_REGRESSION="pass"
else
  emit_event "regression_tests" "fail" "fail" "differential_v2_test failed"
  RESULT_REGRESSION="fail"
fi

if [[ "${RESULT_BEAD}" == "pass" && "${RESULT_REGRESSION}" == "pass" ]]; then
  RESULT="pass"
else
  RESULT="fail"
fi

contract_sha="$(sha256sum "${LOG_CONTRACT}" | awk '{print $1}')"
bead_log_sha="$(sha256sum "${LOG_BEAD_TEST}" | awk '{print $1}')"
regression_log_sha="$(sha256sum "${LOG_DIFF_V2_TEST}" | awk '{print $1}')"

cat > "${REPORT_JSON}" <<EOF_JSON
{
  "trace_id": "${TRACE_ID}",
  "run_id": "${RUN_ID}",
  "scenario_id": "${SCENARIO_ID}",
  "seed": ${SEED},
  "bead_id": "${BEAD_ID}",
  "events_jsonl": "${EVENTS_JSONL}",
  "contract_log": "${LOG_CONTRACT}",
  "contract_log_sha256": "${contract_sha}",
  "bead_test_log": "${LOG_BEAD_TEST}",
  "bead_test_log_sha256": "${bead_log_sha}",
  "regression_test_log": "${LOG_DIFF_V2_TEST}",
  "regression_test_log_sha256": "${regression_log_sha}",
  "result": "${RESULT}"
}
EOF_JSON

emit_event "finalize" "info" "${RESULT}" "report written to ${REPORT_JSON}"

if [[ "${RESULT}" != "pass" ]]; then
  echo "[GATE FAIL] ${BEAD_ID} oracle-wiring gate failed"
  echo "See ${LOG_BEAD_TEST} and ${LOG_DIFF_V2_TEST}"
  exit 1
fi

echo "[GATE PASS] ${BEAD_ID} oracle-wiring contract is valid"
echo "Artifacts: ${ARTIFACT_DIR}"
