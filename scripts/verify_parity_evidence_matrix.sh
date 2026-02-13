#!/usr/bin/env bash
# verify_parity_evidence_matrix.sh — parity evidence contract validation (bd-1dp9.7.5)
#
# Validates that parity closure beads have linked unit/e2e/log evidence.
# The gate can be run in strict mode (default) or intentional-failure mode
# (`--expect-fail`) for negative-path verification scenarios.
#
# Usage:
#   ./scripts/verify_parity_evidence_matrix.sh [--json] [--expect-fail] [--workspace-root <PATH>]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
WORKSPACE_ROOT="$REPO_ROOT"
RUN_ID="parity-evidence-matrix-$(date -u +%Y%m%dT%H%M%SZ)-$$"
JSON_OUTPUT=false
EXPECT_FAIL=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --json)
            JSON_OUTPUT=true
            shift
            ;;
        --expect-fail)
            EXPECT_FAIL=true
            shift
            ;;
        --workspace-root)
            shift
            if [[ $# -eq 0 ]]; then
                echo "ERROR: --workspace-root requires a value" >&2
                exit 2
            fi
            WORKSPACE_ROOT="$1"
            shift
            ;;
        *)
            echo "ERROR: unknown argument: $1" >&2
            exit 2
            ;;
    esac
done

MODULE_FILE="$REPO_ROOT/crates/fsqlite-harness/src/parity_evidence_matrix.rs"
if [[ ! -f "$MODULE_FILE" ]]; then
    echo "ERROR: $MODULE_FILE not found" >&2
    exit 1
fi

MODULE_HASH="$(sha256sum "$MODULE_FILE" | awk '{print $1}')"
REPORT_DIR="$REPO_ROOT/test-results/bd_1dp9_7_5"
REPORT_PATH="$REPORT_DIR/${RUN_ID}.json"
mkdir -p "$REPORT_DIR"

ERRORS=0
TEST_RESULT="unknown"
TEST_COUNT=0
if TEST_OUTPUT="$(cargo test -p fsqlite-harness --lib -- parity_evidence_matrix 2>&1)"; then
    TEST_RESULT="pass"
    TEST_COUNT="$(echo "$TEST_OUTPUT" | grep -oP '\d+ passed' | grep -oP '\d+' || echo 0)"
else
    TEST_RESULT="fail"
    ERRORS=$((ERRORS + 1))
fi

GATE_EXIT=0
if cargo run -p fsqlite-harness --bin parity_evidence_matrix_gate -- \
    --workspace-root "$WORKSPACE_ROOT" \
    --output "$REPORT_PATH" >/dev/null 2>&1; then
    GATE_RESULT="pass"
else
    GATE_EXIT=$?
    GATE_RESULT="fail"
fi

if [[ ! -f "$REPORT_PATH" ]]; then
    ERRORS=$((ERRORS + 1))
    REPORT_HASH=""
    VIOLATION_COUNT=0
else
    REPORT_HASH="$(sha256sum "$REPORT_PATH" | awk '{print $1}')"
    VIOLATION_COUNT="$(jq -r '.summary.violation_count // 0' "$REPORT_PATH" 2>/dev/null || echo 0)"
fi

if [[ "$EXPECT_FAIL" == "true" ]]; then
    if [[ $GATE_EXIT -eq 0 ]]; then
        ERRORS=$((ERRORS + 1))
    fi
else
    if [[ $GATE_EXIT -ne 0 ]]; then
        ERRORS=$((ERRORS + 1))
    fi
fi

FINAL_RESULT="$([ $ERRORS -eq 0 ] && echo 'pass' || echo 'fail')"

if [[ "$JSON_OUTPUT" == "true" ]]; then
    cat <<ENDJSON
{
  "run_id": "$RUN_ID",
  "phase": "parity_evidence_matrix_validation",
  "bead_id": "bd-1dp9.7.5",
  "module_hash": "$MODULE_HASH",
  "workspace_root": "$WORKSPACE_ROOT",
  "expect_fail": $EXPECT_FAIL,
  "unit_tests": {
    "result": "$TEST_RESULT",
    "count": $TEST_COUNT
  },
  "gate": {
    "result": "$GATE_RESULT",
    "exit_code": $GATE_EXIT,
    "report_path": "$REPORT_PATH",
    "report_hash": "$REPORT_HASH",
    "violation_count": $VIOLATION_COUNT
  },
  "validation_errors": $ERRORS,
  "result": "$FINAL_RESULT"
}
ENDJSON
else
    echo "=== Parity Evidence Matrix Validation ==="
    echo "Run ID:            $RUN_ID"
    echo "Module hash:       $MODULE_HASH"
    echo "Workspace root:    $WORKSPACE_ROOT"
    echo "Expect fail mode:  $EXPECT_FAIL"
    echo ""
    echo "--- Unit Tests ---"
    echo "Result:            $TEST_RESULT"
    echo "Tests:             $TEST_COUNT"
    echo ""
    echo "--- Gate ---"
    echo "Result:            $GATE_RESULT"
    echo "Exit code:         $GATE_EXIT"
    echo "Report path:       $REPORT_PATH"
    echo "Report hash:       $REPORT_HASH"
    echo "Violations:        $VIOLATION_COUNT"
    echo ""
    echo "--- Validation ---"
    echo "Errors:            $ERRORS"
    echo "Result:            $([ $ERRORS -eq 0 ] && echo 'PASS' || echo 'FAIL')"
fi

exit "$ERRORS"
