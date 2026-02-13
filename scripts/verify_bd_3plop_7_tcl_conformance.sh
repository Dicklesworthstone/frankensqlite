#!/usr/bin/env bash
# verify_bd_3plop_7_tcl_conformance.sh — bead bd-3plop.7 verification runner
#
# Usage:
#   ./scripts/verify_bd_3plop_7_tcl_conformance.sh [--json] [--execute] [--max-scenarios N] [--timeout-secs N]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
RUN_ID="bd-3plop-7-$(date -u +%Y%m%dT%H%M%SZ)-$$"
JSON_OUTPUT=false
EXECUTE=false
MAX_SCENARIOS="${BD_3PLOP7_MAX_SCENARIOS:-1}"
TIMEOUT_SECS="${BD_3PLOP7_TIMEOUT_SECS:-120}"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --json)
            JSON_OUTPUT=true
            shift
            ;;
        --execute)
            EXECUTE=true
            shift
            ;;
        --max-scenarios)
            shift
            [[ $# -gt 0 ]] || { echo "ERROR: --max-scenarios requires value" >&2; exit 2; }
            MAX_SCENARIOS="$1"
            shift
            ;;
        --timeout-secs)
            shift
            [[ $# -gt 0 ]] || { echo "ERROR: --timeout-secs requires value" >&2; exit 2; }
            TIMEOUT_SECS="$1"
            shift
            ;;
        *)
            echo "ERROR: unknown argument: $1" >&2
            exit 2
            ;;
    esac
done

mkdir -p "$REPO_ROOT/test-results/bd_3plop_7"
TEST_LOG="$REPO_ROOT/test-results/bd_3plop_7/${RUN_ID}.test.log"
SUITE_PATH="$REPO_ROOT/test-results/bd_3plop_7/${RUN_ID}.suite.json"
SUMMARY_PATH="$REPO_ROOT/test-results/bd_3plop_7/${RUN_ID}.summary.json"
RESULT="pass"

if ! cargo test -p fsqlite-harness --test bd_3plop_7_tcl_conformance -- --nocapture \
    >"$TEST_LOG" 2>&1; then
    RESULT="fail"
fi

if ! cargo run -p fsqlite-harness --bin tcl_conformance_manifest -- \
    --output "$SUITE_PATH" >>"$TEST_LOG" 2>&1; then
    RESULT="fail"
fi

MODE_FLAG="--dry-run"
if [[ "$EXECUTE" == "true" ]]; then
    MODE_FLAG="--execute"
fi

if ! cargo run -p fsqlite-harness --bin tcl_conformance_manifest -- \
    "$MODE_FLAG" \
    --max-scenarios "$MAX_SCENARIOS" \
    --timeout-secs "$TIMEOUT_SECS" \
    --output "$SUMMARY_PATH" >>"$TEST_LOG" 2>&1; then
    RESULT="fail"
fi

SCENARIO_TOTAL="$(jq -r '.total_scenarios // 0' "$SUMMARY_PATH" 2>/dev/null || echo 0)"
SCENARIO_SKIPPED="$(jq -r '.skipped_scenarios // 0' "$SUMMARY_PATH" 2>/dev/null || echo 0)"
SCENARIO_FAILED="$(jq -r '.failed_scenarios // 0' "$SUMMARY_PATH" 2>/dev/null || echo 0)"
SCENARIO_ERRORS="$(jq -r '.error_scenarios // 0' "$SUMMARY_PATH" 2>/dev/null || echo 0)"

if [[ -f "$SUITE_PATH" ]]; then
    SUITE_HASH="$(sha256sum "$SUITE_PATH" | awk '{print $1}')"
else
    SUITE_HASH=""
    RESULT="fail"
fi

if [[ -f "$SUMMARY_PATH" ]]; then
    SUMMARY_HASH="$(sha256sum "$SUMMARY_PATH" | awk '{print $1}')"
else
    SUMMARY_HASH=""
    RESULT="fail"
fi

if [[ "$JSON_OUTPUT" == "true" ]]; then
    cat <<ENDJSON
{
  "run_id": "$RUN_ID",
  "bead_id": "bd-3plop.7",
  "result": "$RESULT",
  "execute_mode": $EXECUTE,
  "max_scenarios": $MAX_SCENARIOS,
  "timeout_secs": $TIMEOUT_SECS,
  "suite_path": "$SUITE_PATH",
  "suite_hash": "$SUITE_HASH",
  "summary_path": "$SUMMARY_PATH",
  "summary_hash": "$SUMMARY_HASH",
  "test_log_path": "$TEST_LOG",
  "scenario_total": $SCENARIO_TOTAL,
  "scenario_skipped": $SCENARIO_SKIPPED,
  "scenario_failed": $SCENARIO_FAILED,
  "scenario_errors": $SCENARIO_ERRORS
}
ENDJSON
else
    echo "=== bd-3plop.7 Verification ==="
    echo "Run ID:           $RUN_ID"
    echo "Result:           $RESULT"
    echo "Execute mode:     $EXECUTE"
    echo "Max scenarios:    $MAX_SCENARIOS"
    echo "Timeout (secs):   $TIMEOUT_SECS"
    echo "Suite path:       $SUITE_PATH"
    echo "Suite hash:       $SUITE_HASH"
    echo "Summary path:     $SUMMARY_PATH"
    echo "Summary hash:     $SUMMARY_HASH"
    echo "Test log:         $TEST_LOG"
    echo "Scenarios total:  $SCENARIO_TOTAL"
    echo "Scenarios skipped:$SCENARIO_SKIPPED"
    echo "Scenarios failed: $SCENARIO_FAILED"
    echo "Scenarios errors: $SCENARIO_ERRORS"
fi

[[ "$RESULT" == "pass" ]]
