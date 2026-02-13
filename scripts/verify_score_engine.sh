#!/usr/bin/env bash
# verify_score_engine.sh — E2E validation for Bayesian score engine (bd-1dp9.1.3)
#
# Validates:
# 1. Unit tests (numerical routines) compile and pass
# 2. Integration tests (scorecard, intervals, release gating) pass
# 3. Module is registered in lib.rs
# 4. Artifacts are present
#
# Usage: ./scripts/verify_score_engine.sh [--json]

set -euo pipefail

RUN_ID="score-engine-$(date -u +%Y%m%dT%H%M%SZ)-$$"
JSON_OUTPUT=false

if [[ "${1:-}" == "--json" ]]; then
    JSON_OUTPUT=true
fi

# ── Phase 1: Unit tests ──────────────────────────────────────────────────

UNIT_OUTPUT=""
UNIT_RESULT="unknown"
UNIT_PASSED=0
UNIT_FAILED=0

if UNIT_OUTPUT=$(cargo test -p fsqlite-harness --lib score_engine 2>&1); then
    UNIT_RESULT="pass"
else
    UNIT_RESULT="fail"
fi

if echo "$UNIT_OUTPUT" | grep -q "test result:"; then
    RESULT_LINE=$(echo "$UNIT_OUTPUT" | grep "test result:" | tail -1)
    UNIT_PASSED=$(echo "$RESULT_LINE" | grep -o '[0-9]* passed' | grep -o '[0-9]*' || echo 0)
    UNIT_FAILED=$(echo "$RESULT_LINE" | grep -o '[0-9]* failed' | grep -o '[0-9]*' || echo 0)
fi

# ── Phase 2: Integration tests ───────────────────────────────────────────

INTEG_OUTPUT=""
INTEG_RESULT="unknown"
INTEG_PASSED=0
INTEG_FAILED=0

if INTEG_OUTPUT=$(cargo test -p fsqlite-harness --test score_engine_test 2>&1); then
    INTEG_RESULT="pass"
else
    INTEG_RESULT="fail"
fi

if echo "$INTEG_OUTPUT" | grep -q "test result:"; then
    RESULT_LINE=$(echo "$INTEG_OUTPUT" | grep "test result:" | tail -1)
    INTEG_PASSED=$(echo "$RESULT_LINE" | grep -o '[0-9]* passed' | grep -o '[0-9]*' || echo 0)
    INTEG_FAILED=$(echo "$RESULT_LINE" | grep -o '[0-9]* failed' | grep -o '[0-9]*' || echo 0)
fi

# ── Phase 3: Verify artifacts exist ──────────────────────────────────────

ARTIFACTS_OK="true"
for f in \
    "crates/fsqlite-harness/src/score_engine.rs" \
    "crates/fsqlite-harness/tests/score_engine_test.rs" \
    "scripts/verify_score_engine.sh" \
; do
    if [[ ! -f "$f" ]]; then
        ARTIFACTS_OK="false"
        echo "ERROR: Missing artifact: $f" >&2
    fi
done

MODULE_HASH=$(sha256sum crates/fsqlite-harness/src/score_engine.rs | awk '{print $1}')
TEST_HASH=$(sha256sum crates/fsqlite-harness/tests/score_engine_test.rs | awk '{print $1}')

# ── Phase 4: Verify module is registered ─────────────────────────────────

MODULE_REGISTERED="false"
if grep -q "pub mod score_engine" crates/fsqlite-harness/src/lib.rs; then
    MODULE_REGISTERED="true"
fi

# ── Aggregate ────────────────────────────────────────────────────────────

TOTAL_TESTS=$((UNIT_PASSED + UNIT_FAILED + INTEG_PASSED + INTEG_FAILED))
TOTAL_PASSED=$((UNIT_PASSED + INTEG_PASSED))
TOTAL_FAILED=$((UNIT_FAILED + INTEG_FAILED))

ERRORS=0
[[ "$UNIT_RESULT" == "pass" ]] || ERRORS=$((ERRORS + 1))
[[ "$INTEG_RESULT" == "pass" ]] || ERRORS=$((ERRORS + 1))
[[ "$MODULE_REGISTERED" == "true" ]] || ERRORS=$((ERRORS + 1))
[[ "$ARTIFACTS_OK" == "true" ]] || ERRORS=$((ERRORS + 1))

# ── Output ───────────────────────────────────────────────────────────────

if $JSON_OUTPUT; then
    cat <<ENDJSON
{
  "run_id": "$RUN_ID",
  "phase": "score_engine_validation",
  "bead_id": "bd-1dp9.1.3",
  "unit_test_result": "$UNIT_RESULT",
  "unit_test_counts": {
    "passed": $UNIT_PASSED,
    "failed": $UNIT_FAILED
  },
  "integration_test_result": "$INTEG_RESULT",
  "integration_test_counts": {
    "passed": $INTEG_PASSED,
    "failed": $INTEG_FAILED
  },
  "total_tests": $TOTAL_TESTS,
  "total_passed": $TOTAL_PASSED,
  "total_failed": $TOTAL_FAILED,
  "module_registered": $MODULE_REGISTERED,
  "artifacts_present": $ARTIFACTS_OK,
  "artifact_hashes": {
    "score_engine_module": "$MODULE_HASH",
    "score_engine_test": "$TEST_HASH"
  },
  "validation_errors": $ERRORS,
  "result": "$([ $ERRORS -eq 0 ] && echo 'pass' || echo 'fail')"
}
ENDJSON
else
    echo "=== Score Engine Validation (bd-1dp9.1.3) ==="
    echo "Run ID:           $RUN_ID"
    echo ""
    echo "--- Unit Tests ---"
    echo "Result:           $UNIT_RESULT"
    echo "  Passed:         $UNIT_PASSED"
    echo "  Failed:         $UNIT_FAILED"
    echo ""
    echo "--- Integration Tests ---"
    echo "Result:           $INTEG_RESULT"
    echo "  Passed:         $INTEG_PASSED"
    echo "  Failed:         $INTEG_FAILED"
    echo ""
    echo "--- Totals ---"
    echo "Tests:            $TOTAL_TESTS"
    echo "Passed:           $TOTAL_PASSED"
    echo "Failed:           $TOTAL_FAILED"
    echo ""
    echo "--- Artifacts ---"
    echo "Present:          $ARTIFACTS_OK"
    echo "Module hash:      $MODULE_HASH"
    echo "Test hash:        $TEST_HASH"
    echo "Registered:       $MODULE_REGISTERED"
    echo ""
    echo "--- Validation ---"
    echo "Errors:           $ERRORS"
    echo "Result:           $([ $ERRORS -eq 0 ] && echo 'PASS' || echo 'FAIL')"
fi

exit $ERRORS
