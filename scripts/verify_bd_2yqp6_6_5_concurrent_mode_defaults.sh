#!/usr/bin/env bash
# Verification gate for bd-2yqp6.6.5:
# concurrent-mode default anti-regression tests for e2e knobs.

set -euo pipefail

BEAD_ID="bd-2yqp6.6.5"
SCENARIO_ID="CONCURRENT-DEFAULTS-F5"
SEED=3520
TIMESTAMP_UTC="$(date -u +%Y%m%dT%H%M%SZ)"
RUN_ID="${BEAD_ID}-${TIMESTAMP_UTC}-${SEED}"
TRACE_ID="trace-${RUN_ID}"
ARTIFACT_DIR="artifacts/${BEAD_ID}/${RUN_ID}"
EVENTS_JSONL="${ARTIFACT_DIR}/events.jsonl"
REPORT_JSON="${ARTIFACT_DIR}/report.json"

mkdir -p "${ARTIFACT_DIR}"

emit_event() {
  local phase="$1"
  local event_type="$2"
  local outcome="$3"
  local message="$4"
  printf '{"trace_id":"%s","run_id":"%s","scenario_id":"%s","seed":%d,"phase":"%s","event_type":"%s","outcome":"%s","timestamp":"%s","message":"%s"}\n' \
    "${TRACE_ID}" "${RUN_ID}" "${SCENARIO_ID}" "${SEED}" "${phase}" "${event_type}" "${outcome}" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "${message}" \
    >> "${EVENTS_JSONL}"
}

echo "=== ${BEAD_ID}: concurrent-mode default anti-regression verification ==="
echo "run_id=${RUN_ID}"
echo "trace_id=${TRACE_ID}"
echo "scenario_id=${SCENARIO_ID}"

emit_event "bootstrap" "start" "running" "verification started"

emit_event "e2e_default_ratchet" "start" "running" "running rch cargo test gate"
if rch exec -- cargo test -p fsqlite-e2e --test bd_2yqp6_6_5_concurrent_mode_defaults -- --nocapture; then
  emit_event "e2e_default_ratchet" "pass" "pass" "concurrent-mode anti-regression tests passed"
  RESULT="pass"
else
  emit_event "e2e_default_ratchet" "fail" "fail" "concurrent-mode anti-regression tests failed"
  RESULT="fail"
fi

cat > "${REPORT_JSON}" <<EOF
{
  "trace_id": "${TRACE_ID}",
  "run_id": "${RUN_ID}",
  "scenario_id": "${SCENARIO_ID}",
  "seed": ${SEED},
  "bead_id": "${BEAD_ID}",
  "events_jsonl": "${EVENTS_JSONL}",
  "result": "${RESULT}"
}
EOF

emit_event "finalize" "info" "${RESULT}" "report written to ${REPORT_JSON}"

if [[ "${RESULT}" != "pass" ]]; then
  echo "[GATE FAIL] ${BEAD_ID} concurrent-mode anti-regression gate failed"
  exit 1
fi

echo "[GATE PASS] ${BEAD_ID} concurrent-mode anti-regression gate passed"
