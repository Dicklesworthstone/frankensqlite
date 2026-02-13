#!/usr/bin/env bash
# verify_bd_3plop_1_skewed_key_fault_injection.sh — bead bd-3plop.1 verification runner
#
# Runs deterministic skewed-key workload tests and emits a compact verification report.
#
# Usage:
#   ./scripts/verify_bd_3plop_1_skewed_key_fault_injection.sh [--json] [--ops <N>]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
RUN_ID="bd-3plop-1-$(date -u +%Y%m%dT%H%M%SZ)-$$"
JSON_OUTPUT=false
OPS="${BD_3PLOP_OPS:-2000}"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --json)
            JSON_OUTPUT=true
            shift
            ;;
        --ops)
            shift
            if [[ $# -eq 0 ]]; then
                echo "ERROR: --ops requires a value" >&2
                exit 2
            fi
            OPS="$1"
            shift
            ;;
        *)
            echo "ERROR: unknown argument: $1" >&2
            exit 2
            ;;
    esac
done

TEST_LOG="$(mktemp)"
RESULT="pass"

if ! BD_3PLOP_OPS="$OPS" cargo test -p fsqlite-harness --test bd_3plop_1_skewed_key_fault_injection -- --nocapture \
    >"$TEST_LOG" 2>&1; then
    RESULT="fail"
fi

ARTIFACT_PATH="$(
    {
        rg -o 'path=[^ ]+' "$TEST_LOG" | tail -n1 | sed 's/^path=//'
    } || true
)"
if [[ -n "$ARTIFACT_PATH" && ! "$ARTIFACT_PATH" = /* ]]; then
    ARTIFACT_PATH="$REPO_ROOT/$ARTIFACT_PATH"
fi

if [[ -n "$ARTIFACT_PATH" && -f "$ARTIFACT_PATH" ]]; then
    ARTIFACT_HASH="$(sha256sum "$ARTIFACT_PATH" | awk '{print $1}')"
else
    ARTIFACT_HASH=""
fi

if [[ "$JSON_OUTPUT" == "true" ]]; then
    cat <<ENDJSON
{
  "run_id": "$RUN_ID",
  "bead_id": "bd-3plop.1",
  "ops_per_case": $OPS,
  "result": "$RESULT",
  "artifact_path": "$ARTIFACT_PATH",
  "artifact_hash": "$ARTIFACT_HASH",
  "test_log_path": "$TEST_LOG"
}
ENDJSON
else
    echo "=== bd-3plop.1 Verification ==="
    echo "Run ID:        $RUN_ID"
    echo "Result:        $RESULT"
    echo "Ops/case:      $OPS"
    echo "Artifact path: $ARTIFACT_PATH"
    echo "Artifact hash: $ARTIFACT_HASH"
    echo "Test log:      $TEST_LOG"
fi

[[ "$RESULT" == "pass" ]]
