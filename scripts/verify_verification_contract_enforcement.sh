#!/usr/bin/env bash
# verify_verification_contract_enforcement.sh — bd-1dp9.7.7 e2e verifier
#
# Validates verification-contract enforcement wiring across:
# 1. Unit-level classification/enforcement logic
# 2. CI/scorecard/release integration tests
# 3. Structured artifact report with deterministic hashes
#
# Usage:
#   ./scripts/verify_verification_contract_enforcement.sh [--json]

set -euo pipefail

RUN_ID="verification-contract-enforcement-$(date -u +%Y%m%dT%H%M%SZ)-$$"
SEED="20260213"
JSON_OUTPUT=false
if [[ "${1:-}" == "--json" ]]; then
    JSON_OUTPUT=true
fi

BEAD_ID="bd-1dp9.7.7"
REPORT_DIR="test-results/bd_1dp9_7_7"
REPORT_PATH="$REPORT_DIR/${RUN_ID}.json"
mkdir -p "$REPORT_DIR"

MODULE_HASH="$(sha256sum crates/fsqlite-harness/src/verification_contract_enforcement.rs | awk '{print $1}')"
CI_HASH="$(sha256sum crates/fsqlite-harness/src/ci_gate_matrix.rs | awk '{print $1}')"
SCORE_HASH="$(sha256sum crates/fsqlite-harness/src/score_engine.rs | awk '{print $1}')"
CONFIDENCE_HASH="$(sha256sum crates/fsqlite-harness/src/confidence_gates.rs | awk '{print $1}')"

run_phase() {
    local phase="$1"
    shift
    local output
    local result="pass"
    if output="$("$@" 2>&1)"; then
        result="pass"
    else
        result="fail"
    fi
    local result_line
    result_line="$(echo "$output" | grep "test result:" | tail -1 || true)"
    local passed failed
    passed="$(echo "$result_line" | grep -o '[0-9]* passed' | grep -o '[0-9]*' || echo 0)"
    failed="$(echo "$result_line" | grep -o '[0-9]* failed' | grep -o '[0-9]*' || echo 0)"
    echo "$result|$passed|$failed"
}

echo "INFO bead_id=${BEAD_ID} phase=setup run_id=${RUN_ID} seed=${SEED}"

UNIT_RESULT_RAW="$(run_phase unit cargo test -p fsqlite-harness --lib verification_contract_enforcement)"
IFS='|' read -r UNIT_RESULT UNIT_PASSED UNIT_FAILED <<<"$UNIT_RESULT_RAW"
echo "INFO bead_id=${BEAD_ID} phase=unit run_id=${RUN_ID} seed=${SEED} result=${UNIT_RESULT} passed=${UNIT_PASSED} failed=${UNIT_FAILED}"

E2E_RESULT_RAW="$(run_phase e2e cargo test -p fsqlite-harness --test bd_1dp9_7_7_verification_contract_enforcement)"
IFS='|' read -r E2E_RESULT E2E_PASSED E2E_FAILED <<<"$E2E_RESULT_RAW"
echo "INFO bead_id=${BEAD_ID} phase=e2e run_id=${RUN_ID} seed=${SEED} result=${E2E_RESULT} passed=${E2E_PASSED} failed=${E2E_FAILED}"

SCORE_RESULT_RAW="$(run_phase scorecard_gate cargo test -p fsqlite-harness --test score_engine_test release_gating_with_contract_blocks_when_evidence_missing)"
IFS='|' read -r SCORE_RESULT SCORE_PASSED SCORE_FAILED <<<"$SCORE_RESULT_RAW"
echo "INFO bead_id=${BEAD_ID} phase=scorecard run_id=${RUN_ID} seed=${SEED} result=${SCORE_RESULT} passed=${SCORE_PASSED} failed=${SCORE_FAILED}"

RELEASE_RESULT_RAW="$(run_phase release_gate cargo test -p fsqlite-harness --lib evaluate_gate_with_contract_attaches_enforcement_payload)"
IFS='|' read -r RELEASE_RESULT RELEASE_PASSED RELEASE_FAILED <<<"$RELEASE_RESULT_RAW"
echo "INFO bead_id=${BEAD_ID} phase=release run_id=${RUN_ID} seed=${SEED} result=${RELEASE_RESULT} passed=${RELEASE_PASSED} failed=${RELEASE_FAILED}"

ERRORS=0
[[ "$UNIT_RESULT" == "pass" ]] || ERRORS=$((ERRORS + 1))
[[ "$E2E_RESULT" == "pass" ]] || ERRORS=$((ERRORS + 1))
[[ "$SCORE_RESULT" == "pass" ]] || ERRORS=$((ERRORS + 1))
[[ "$RELEASE_RESULT" == "pass" ]] || ERRORS=$((ERRORS + 1))

FINAL_RESULT="$([ "$ERRORS" -eq 0 ] && echo "pass" || echo "fail")"

cat >"$REPORT_PATH" <<ENDJSON
{
  "run_id": "${RUN_ID}",
  "bead_id": "${BEAD_ID}",
  "seed": ${SEED},
  "phase_markers": ["setup", "unit", "e2e", "scorecard", "release", "done"],
  "unit_tests": {
    "result": "${UNIT_RESULT}",
    "passed": ${UNIT_PASSED},
    "failed": ${UNIT_FAILED}
  },
  "e2e_tests": {
    "result": "${E2E_RESULT}",
    "passed": ${E2E_PASSED},
    "failed": ${E2E_FAILED}
  },
  "scorecard_gate_tests": {
    "result": "${SCORE_RESULT}",
    "passed": ${SCORE_PASSED},
    "failed": ${SCORE_FAILED}
  },
  "release_gate_tests": {
    "result": "${RELEASE_RESULT}",
    "passed": ${RELEASE_PASSED},
    "failed": ${RELEASE_FAILED}
  },
  "artifact_hashes": {
    "verification_contract_module": "${MODULE_HASH}",
    "ci_gate_matrix_module": "${CI_HASH}",
    "score_engine_module": "${SCORE_HASH}",
    "confidence_gates_module": "${CONFIDENCE_HASH}"
  },
  "first_divergence": null,
  "validation_errors": ${ERRORS},
  "result": "${FINAL_RESULT}"
}
ENDJSON

if [[ "$JSON_OUTPUT" == "true" ]]; then
    cat "$REPORT_PATH"
else
    echo "=== Verification Contract Enforcement Validation ==="
    echo "Run ID:          $RUN_ID"
    echo "Bead ID:         $BEAD_ID"
    echo "Seed:            $SEED"
    echo "Report:          $REPORT_PATH"
    echo "Unit tests:      $UNIT_RESULT (passed=$UNIT_PASSED failed=$UNIT_FAILED)"
    echo "E2E tests:       $E2E_RESULT (passed=$E2E_PASSED failed=$E2E_FAILED)"
    echo "Scorecard gate:  $SCORE_RESULT (passed=$SCORE_PASSED failed=$SCORE_FAILED)"
    echo "Release gate:    $RELEASE_RESULT (passed=$RELEASE_PASSED failed=$RELEASE_FAILED)"
    echo "Final result:    $([ "$ERRORS" -eq 0 ] && echo 'PASS' || echo 'FAIL')"
fi

echo "INFO bead_id=${BEAD_ID} phase=done run_id=${RUN_ID} seed=${SEED} result=${FINAL_RESULT} report=${REPORT_PATH}"
exit "$ERRORS"
