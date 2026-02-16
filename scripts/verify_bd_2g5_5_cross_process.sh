#!/usr/bin/env bash
# verify_bd_2g5_5_cross_process.sh — deterministic verifier for bd-2g5.5.1
#
# Validates the cross-process crash harness integration test and emits
# replay + artifact metadata.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCRIPT_PATH="${BASH_SOURCE[0]}"
if [[ "$SCRIPT_PATH" != /* ]]; then
  SCRIPT_PATH="$REPO_ROOT/$SCRIPT_PATH"
fi

JSON_OUTPUT=false
NO_RCH=false
CYCLES="${BD_2G5_5_CYCLES:-100}"
SEED="${BD_2G5_5_SEED:-270550001}"
RUN_TAG="${BD_2G5_5_RUN_TAG:-verify}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --json)
      JSON_OUTPUT=true
      shift
      ;;
    --no-rch)
      NO_RCH=true
      shift
      ;;
    --cycles)
      shift
      [[ $# -gt 0 ]] || {
        echo "ERROR: --cycles requires a value" >&2
        exit 2
      }
      CYCLES="$1"
      shift
      ;;
    --seed)
      shift
      [[ $# -gt 0 ]] || {
        echo "ERROR: --seed requires a value" >&2
        exit 2
      }
      SEED="$1"
      shift
      ;;
    --run-tag)
      shift
      [[ $# -gt 0 ]] || {
        echo "ERROR: --run-tag requires a value" >&2
        exit 2
      }
      RUN_TAG="$1"
      shift
      ;;
    *)
      echo "ERROR: unknown argument '$1'" >&2
      exit 2
      ;;
  esac
done

if ! $NO_RCH && command -v rch >/dev/null 2>&1; then
  OFFLOAD=(rch exec -- bash "$SCRIPT_PATH" --no-rch --cycles "$CYCLES" --seed "$SEED" --run-tag "$RUN_TAG")
  if $JSON_OUTPUT; then
    OFFLOAD+=(--json)
  fi
  exec "${OFFLOAD[@]}"
fi

TEST_LOG="$(mktemp)"
RESULT="pass"

if ! BD_2G5_5_CYCLES="$CYCLES" \
     BD_2G5_5_SEED="$SEED" \
     BD_2G5_5_RUN_TAG="$RUN_TAG" \
     cargo test -p fsqlite-harness --test bd_2g5_5_cross_process -- --nocapture \
     >"$TEST_LOG" 2>&1; then
  RESULT="fail"
fi

REPORT_PATH="$({
  rg '^bead_id=bd-2g5\.5\.1 path=' "$TEST_LOG" \
    | tail -n1 \
    | sed 's/^bead_id=bd-2g5\.5\.1 path=//'
} || true)"
EVENTS_PATH="$({
  rg '^bead_id=bd-2g5\.5\.1 events_path=' "$TEST_LOG" \
    | tail -n1 \
    | sed 's/^bead_id=bd-2g5\.5\.1 events_path=//'
} || true)"
REPLAY_COMMAND="$({
  rg '^bead_id=bd-2g5\.5\.1 replay_command=' "$TEST_LOG" \
    | tail -n1 \
    | sed 's/^bead_id=bd-2g5\.5\.1 replay_command=//'
} || true)"

if [[ -n "$REPORT_PATH" && "$REPORT_PATH" != /* ]]; then
  REPORT_PATH="$REPO_ROOT/$REPORT_PATH"
fi
if [[ -n "$EVENTS_PATH" && "$EVENTS_PATH" != /* ]]; then
  EVENTS_PATH="$REPO_ROOT/$EVENTS_PATH"
fi

REPORT_HASH=""
EVENTS_HASH=""
MATRIX_COMPLETE="false"
TRACE_ID=""
SCHEMA_ERRORS="-1"

if [[ -n "$REPORT_PATH" && -f "$REPORT_PATH" ]]; then
  REPORT_HASH="$(sha256sum "$REPORT_PATH" | awk '{print $1}')"
  MATRIX_COMPLETE="$(jq -r '.scenario_matrix_complete' "$REPORT_PATH" 2>/dev/null || echo false)"
  TRACE_ID="$(jq -r '.trace_id' "$REPORT_PATH" 2>/dev/null || true)"
  SCHEMA_ERRORS="$(jq -r '.schema_conformance_errors | length' "$REPORT_PATH" 2>/dev/null || echo -1)"
  if [[ -z "$REPLAY_COMMAND" ]]; then
    REPLAY_COMMAND="$(jq -r '.replay_command' "$REPORT_PATH" 2>/dev/null || true)"
  fi
fi

if [[ -n "$EVENTS_PATH" && -f "$EVENTS_PATH" ]]; then
  EVENTS_HASH="$(sha256sum "$EVENTS_PATH" | awk '{print $1}')"
fi

if [[ "$JSON_OUTPUT" == "true" ]]; then
  cat <<ENDJSON
{
  "bead_id": "bd-2g5.5.1",
  "result": "$RESULT",
  "cycles": $CYCLES,
  "seed": $SEED,
  "run_tag": "$RUN_TAG",
  "matrix_complete": $MATRIX_COMPLETE,
  "schema_errors": $SCHEMA_ERRORS,
  "trace_id": "$TRACE_ID",
  "replay_command": "$REPLAY_COMMAND",
  "artifact_bundle": {
    "report_path": "$REPORT_PATH",
    "report_hash": "$REPORT_HASH",
    "events_path": "$EVENTS_PATH",
    "events_hash": "$EVENTS_HASH",
    "test_log_path": "$TEST_LOG"
  }
}
ENDJSON
else
  echo "=== bd-2g5.5.1 Cross-Process Verification ==="
  echo "Result:         $RESULT"
  echo "Cycles:         $CYCLES"
  echo "Seed:           $SEED"
  echo "Run tag:        $RUN_TAG"
  echo "Matrix complete:$MATRIX_COMPLETE"
  echo "Schema errors:  $SCHEMA_ERRORS"
  echo "Trace ID:       $TRACE_ID"
  echo "Replay command: $REPLAY_COMMAND"
  echo "Report path:    $REPORT_PATH"
  echo "Report hash:    $REPORT_HASH"
  echo "Events path:    $EVENTS_PATH"
  echo "Events hash:    $EVENTS_HASH"
  echo "Test log:       $TEST_LOG"
fi

[[ "$RESULT" == "pass" ]]
[[ "$MATRIX_COMPLETE" == "true" ]]
[[ "$SCHEMA_ERRORS" == "0" ]]
