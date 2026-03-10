#!/usr/bin/env bash
# Verification gate for bd-db300.7.3:
# explicit exit criteria for leapfrog claims against SQLite.
#
# Deterministic replay:
#   bash scripts/verify_bd_db300_7_3_leapfrog_exit_criteria.sh

set -euo pipefail

BEAD_ID="bd-db300.7.3"
SCENARIO_ID="LEAPFROG-EXIT-CRITERIA-G3"
SEED=7300
TIMESTAMP_UTC="$(date -u +%Y%m%dT%H%M%SZ)"
RUN_ID="${BEAD_ID}-${TIMESTAMP_UTC}-${SEED}"
TRACE_ID="trace-${RUN_ID}"
ARTIFACT_DIR="artifacts/${BEAD_ID}/${RUN_ID}"
EVENTS_JSONL="${ARTIFACT_DIR}/events.jsonl"
REPORT_JSON="${ARTIFACT_DIR}/manifest.json"
SUMMARY_MD="${ARTIFACT_DIR}/summary.md"
TEST_LOG="${ARTIFACT_DIR}/cargo-test.log"
CONTRACT_FILE="leapfrog_exit_criteria.toml"
TEST_FILE="crates/fsqlite-harness/tests/bd_db300_7_3_leapfrog_exit_criteria.rs"

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

echo "=== ${BEAD_ID}: leapfrog exit criteria verification ==="
echo "run_id=${RUN_ID}"
echo "trace_id=${TRACE_ID}"
echo "scenario_id=${SCENARIO_ID}"

emit_event "bootstrap" "start" "running" "verification started"

if [[ ! -f "${CONTRACT_FILE}" ]]; then
  emit_event "contract_presence" "fail" "fail" "missing ${CONTRACT_FILE}"
  echo "missing ${CONTRACT_FILE}" >&2
  exit 1
fi
if [[ ! -f "${TEST_FILE}" ]]; then
  emit_event "test_presence" "fail" "fail" "missing ${TEST_FILE}"
  echo "missing ${TEST_FILE}" >&2
  exit 1
fi

emit_event "contract_schema" "start" "running" "validating contract schema"
python3 - <<'PY' > "${REPORT_JSON}"
import json
import tomllib
from pathlib import Path

contract = tomllib.loads(Path("leapfrog_exit_criteria.toml").read_text(encoding="utf-8"))

required_meta = {
    "schema_version": "fsqlite-harness.leapfrog_exit_criteria.v1",
    "bead_id": "bd-db300.7.3",
}
for key, expected in required_meta.items():
    actual = contract["meta"].get(key)
    if actual != expected:
        raise SystemExit(f"meta.{key} expected {expected!r}, found {actual!r}")

cell_gates = contract.get("cell_gates", [])
if [gate.get("cell") for gate in cell_gates] != ["c1", "c4", "c8"]:
    raise SystemExit("cell_gates must appear in c1, c4, c8 order")

required_artifacts = {
    "artifacts/{bead_id}/{run_id}/events.jsonl",
    "artifacts/{bead_id}/{run_id}/manifest.json",
    "artifacts/{bead_id}/{run_id}/summary.md",
    "artifacts/{bead_id}/{run_id}/cell_metrics.jsonl",
    "artifacts/{bead_id}/{run_id}/retry_report.json",
    "artifacts/{bead_id}/{run_id}/topology.json",
}
artifacts = set(contract["verification_plan"].get("logging_artifacts", []))
missing = sorted(required_artifacts - artifacts)
if missing:
    raise SystemExit(f"missing logging artifacts: {missing}")

summary = {
    "bead_id": contract["meta"]["bead_id"],
    "policy_id": contract["meta"]["policy_id"],
    "required_profiles": contract["campaign"]["required_placement_profiles"],
    "required_cells": [gate["cell"] for gate in cell_gates],
    "required_scenarios": contract["verification_plan"]["e2e_scenarios"],
    "claim_language": contract["scorecard"]["claim_language"],
}
print(json.dumps(summary, indent=2))
PY
emit_event "contract_schema" "pass" "pass" "contract schema validated"

cat > "${SUMMARY_MD}" <<EOF
# ${BEAD_ID} Verification Summary

- run_id: \`${RUN_ID}\`
- trace_id: \`${TRACE_ID}\`
- scenario_id: \`${SCENARIO_ID}\`
- contract: \`${CONTRACT_FILE}\`
- test: \`${TEST_FILE}\`
- report: \`${REPORT_JSON}\`
EOF

emit_event "cargo_test" "start" "running" "running harness test via rch"
if ! rch exec -- cargo test --package fsqlite-harness --test bd_db300_7_3_leapfrog_exit_criteria -- --nocapture 2>&1 | tee "${TEST_LOG}"; then
  emit_event "cargo_test" "fail" "fail" "harness test failed"
  exit 1
fi
emit_event "cargo_test" "pass" "pass" "harness test passed"

emit_event "complete" "finish" "pass" "verification completed"
echo "[GATE PASS] ${BEAD_ID} leapfrog exit criteria verification passed"
