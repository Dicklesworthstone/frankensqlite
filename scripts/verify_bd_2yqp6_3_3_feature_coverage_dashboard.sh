#!/usr/bin/env bash
# Verification gate for bd-2yqp6.3.3:
# feature-to-test coverage accounting and machine-readable dashboard artifact.
#
# Deterministic replay:
#   bash scripts/verify_bd_2yqp6_3_3_feature_coverage_dashboard.sh
#
# Structured logging contract:
#   emits JSONL events with trace_id/run_id/scenario_id/seed/timing/outcome
#   to artifacts/bd-2yqp6.3.3/<run_id>/events.jsonl
#
# Set FSQLITE_DISABLE_RCH=1 to force a local Cargo invocation (for example on
# GitHub-hosted runners, where rch is not installed).

set -euo pipefail

BEAD_ID="bd-2yqp6.3.3"
SCENARIO_ID="PARITY-COVERAGE-C3"
SEED="${SEED:-3520}"
RUN_ID="${RUN_ID:-${BEAD_ID}-${SEED}}"
TRACE_ID="${TRACE_ID:-trace-${RUN_ID}}"
ARTIFACT_DIR="artifacts/${BEAD_ID}/${RUN_ID}"
EVENTS_JSONL="${ARTIFACT_DIR}/events.jsonl"
REPORT_JSON="${ARTIFACT_DIR}/feature_coverage_dashboard.json"
RUN_LOG="${ARTIFACT_DIR}/feature_coverage_dashboard.log"
MANIFEST="docs/contracts/corpus_manifest.toml"

mkdir -p "${ARTIFACT_DIR}"
: > "${EVENTS_JSONL}"

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

echo "=== ${BEAD_ID}: feature coverage dashboard gate ==="
echo "run_id=${RUN_ID}"
echo "trace_id=${TRACE_ID}"
echo "scenario_id=${SCENARIO_ID}"

emit_event "bootstrap" "start" "running" "verification started"

if [[ ! -f "${MANIFEST}" ]]; then
  emit_event "manifest_presence" "fail" "fail" "missing manifest"
  echo "missing ${MANIFEST}" >&2
  exit 1
fi
emit_event "manifest_presence" "pass" "pass" "canonical manifest exists"

DASHBOARD_RUNNER=(cargo run -p fsqlite-harness --bin feature_coverage_dashboard --)
if command -v rch >/dev/null 2>&1 && [[ "${FSQLITE_DISABLE_RCH:-0}" != "1" ]]; then
  DASHBOARD_RUNNER=(rch exec -- cargo run -p fsqlite-harness --bin feature_coverage_dashboard --)
fi

emit_event "dashboard" "start" "running" "building feature coverage dashboard"
if "${DASHBOARD_RUNNER[@]}" \
  --manifest "${MANIFEST}" \
  --output-json "${REPORT_JSON}" \
  --run-id "${RUN_ID}" \
  --trace-id "${TRACE_ID}" \
  --scenario-id "${SCENARIO_ID}" \
  --seed "${SEED}" \
  --generated-unix-ms 0 \
  > "${RUN_LOG}" 2>&1; then
  emit_event "dashboard" "pass" "pass" "dashboard build passed"
else
  emit_event "dashboard" "fail" "fail" "dashboard build failed"
  echo "[GATE FAIL] ${BEAD_ID} dashboard build failed"
  echo "See ${RUN_LOG}"
  exit 1
fi

emit_event "dashboard_schema" "start" "running" "validating machine-readable dashboard"
jq -e '
  .schema_version == "1.0.0"
  and .bead_id == "bd-2yqp6.3.3"
  and .scenario_id == "PARITY-COVERAGE-C3"
  and .release_gate.outcome == "pass"
  and .release_gate.missing_feature_count == 0
  and .required_feature_count >= 1
  and (.families | length) >= 3
  and (.features | length) == .required_feature_count
  and (.coverage_signature_sha256 | test("^[0-9a-f]{64}$"))
' "${REPORT_JSON}" >/dev/null
emit_event "dashboard_schema" "pass" "pass" "dashboard schema validated"

emit_event "finalize" "info" "pass" "feature coverage dashboard gate passed"

echo "[GATE PASS] ${BEAD_ID} feature coverage dashboard is valid"
echo "Artifacts: ${ARTIFACT_DIR}"
