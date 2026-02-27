#!/usr/bin/env bash
# bd_2y306_2_chain_length_controls_e2e.sh — deterministic verifier for bd-2y306.2
#
# Runs:
# 1) MVCC unit stress for chain-length boundedness
# 2) MVCC unit stress for horizon-pinned backpressure
# 3) Bead-specific e2e replay artifact emission test
#
# Writes a machine-readable artifact with run/trace/scenario IDs, per-test
# timing, replay command, and pass/fail summary.

set -euo pipefail

WORKSPACE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

JSON_OUTPUT=false
NO_RCH=false

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
    *)
      echo "ERROR: unknown argument '$1'" >&2
      exit 2
      ;;
  esac
done

BEAD_ID="bd-2y306.2"
SCENARIO_ID="${SCENARIO_ID:-CHAIN-LENGTH-CONTROLS-E2E}"
SEED="${SEED:-20260227}"
TRACE_ID="${TRACE_ID:-$SEED}"
RUN_ID="${RUN_ID:-${BEAD_ID}-seed-${SEED}}"

ARTIFACT_DIR="$WORKSPACE_ROOT/artifacts/bd-2y306.2"
ARTIFACT_PATH="${FSQLITE_CHAIN_LENGTH_CONTROL_ARTIFACT:-$ARTIFACT_DIR/chain_length_controls_artifact.json}"
CHAIN_E2E_ARTIFACT="${FSQLITE_CHAIN_LENGTH_E2E_ARTIFACT:-$ARTIFACT_DIR/chain_length_e2e_test_artifact.json}"
mkdir -p "$ARTIFACT_DIR"

TEST1_NAME="mvcc_chain_bound_unit"
TEST1_FILTER="test_chain_length_bounded_after_10000_updates_same_page"
TEST2_NAME="mvcc_chain_backpressure_unit"
TEST2_FILTER="test_chain_backpressure_reports_blocked_when_horizon_pinned"
TEST3_NAME="e2e_chain_length_replay_artifact"
TEST3_FILTER="bd_2y306_2_chain_length_e2e_replay_emits_artifact"

if ! $NO_RCH && command -v rch >/dev/null 2>&1; then
  RUNNER=(rch exec --)
else
  RUNNER=()
fi

run_unit_test() {
  local test_name="$1"
  local test_filter="$2"
  local log_path="$ARTIFACT_DIR/${test_name}.log"
  local start_ms end_ms elapsed_ms status

  start_ms="$(date +%s%3N)"
  if "${RUNNER[@]}" cargo test -p fsqlite-mvcc "$test_filter" -- --nocapture --test-threads=1 >"$log_path" 2>&1; then
    status="pass"
  else
    status="fail"
  fi
  if [[ "$status" == "pass" ]] && ! rg -q "test .*${test_filter} .* ok" "$log_path"; then
    status="fail"
  fi
  end_ms="$(date +%s%3N)"
  elapsed_ms="$((end_ms - start_ms))"

  printf '%s|%s|%s\n' "$status" "$elapsed_ms" "$log_path"
}

run_e2e_test() {
  local log_path="$ARTIFACT_DIR/${TEST3_NAME}.log"
  local start_ms end_ms elapsed_ms status

  start_ms="$(date +%s%3N)"
  if "${RUNNER[@]}" env \
    RUN_ID="$RUN_ID" \
    TRACE_ID="$TRACE_ID" \
    SCENARIO_ID="$SCENARIO_ID" \
    SEED="$SEED" \
    FSQLITE_CHAIN_LENGTH_E2E_ARTIFACT="$CHAIN_E2E_ARTIFACT" \
    cargo test -p fsqlite-e2e --test bd_2y306_2_chain_length_controls "$TEST3_FILTER" -- --exact --nocapture --test-threads=1 >"$log_path" 2>&1; then
    status="pass"
  else
    status="fail"
  fi
  if [[ "$status" == "pass" ]] && ! rg -q "test .*${TEST3_FILTER} .* ok" "$log_path"; then
    status="fail"
  fi
  if [[ "$status" == "pass" ]] && [[ ! -s "$CHAIN_E2E_ARTIFACT" ]]; then
    status="fail"
  fi
  end_ms="$(date +%s%3N)"
  elapsed_ms="$((end_ms - start_ms))"

  printf '%s|%s|%s\n' "$status" "$elapsed_ms" "$log_path"
}

test1_result="$(run_unit_test "$TEST1_NAME" "$TEST1_FILTER")"
test2_result="$(run_unit_test "$TEST2_NAME" "$TEST2_FILTER")"
test3_result="$(run_e2e_test)"

TEST1_STATUS="${test1_result%%|*}"
test1_tail="${test1_result#*|}"
TEST1_ELAPSED_MS="${test1_tail%%|*}"
TEST1_LOG_PATH="${test1_tail#*|}"

TEST2_STATUS="${test2_result%%|*}"
test2_tail="${test2_result#*|}"
TEST2_ELAPSED_MS="${test2_tail%%|*}"
TEST2_LOG_PATH="${test2_tail#*|}"

TEST3_STATUS="${test3_result%%|*}"
test3_tail="${test3_result#*|}"
TEST3_ELAPSED_MS="${test3_tail%%|*}"
TEST3_LOG_PATH="${test3_tail#*|}"

OVERALL_STATUS="pass"
if [[ "$TEST1_STATUS" != "pass" || "$TEST2_STATUS" != "pass" || "$TEST3_STATUS" != "pass" ]]; then
  OVERALL_STATUS="fail"
fi

REPLAY_CMD="scripts/bd_2y306_2_chain_length_controls_e2e.sh --json"

cat >"$ARTIFACT_PATH" <<EOF
{
  "bead_id": "$BEAD_ID",
  "run_id": "$RUN_ID",
  "trace_id": "$TRACE_ID",
  "scenario_id": "$SCENARIO_ID",
  "seed": $SEED,
  "overall_status": "$OVERALL_STATUS",
  "tests": [
    {
      "test_id": "$TEST1_NAME",
      "crate": "fsqlite-mvcc",
      "filter": "$TEST1_FILTER",
      "status": "$TEST1_STATUS",
      "elapsed_ms": $TEST1_ELAPSED_MS,
      "log_path": "${TEST1_LOG_PATH#$WORKSPACE_ROOT/}"
    },
    {
      "test_id": "$TEST2_NAME",
      "crate": "fsqlite-mvcc",
      "filter": "$TEST2_FILTER",
      "status": "$TEST2_STATUS",
      "elapsed_ms": $TEST2_ELAPSED_MS,
      "log_path": "${TEST2_LOG_PATH#$WORKSPACE_ROOT/}"
    },
    {
      "test_id": "$TEST3_NAME",
      "crate": "fsqlite-e2e",
      "filter": "$TEST3_FILTER",
      "status": "$TEST3_STATUS",
      "elapsed_ms": $TEST3_ELAPSED_MS,
      "log_path": "${TEST3_LOG_PATH#$WORKSPACE_ROOT/}",
      "artifact_path": "${CHAIN_E2E_ARTIFACT#$WORKSPACE_ROOT/}"
    }
  ],
  "replay_command": "$REPLAY_CMD"
}
EOF

if $JSON_OUTPUT; then
  cat "$ARTIFACT_PATH"
else
  echo "=== bd-2y306.2 Chain-Length Controls Verification ==="
  echo "Run ID:      $RUN_ID"
  echo "Trace ID:    $TRACE_ID"
  echo "Scenario ID: $SCENARIO_ID"
  echo "Seed:        $SEED"
  echo "Artifact:    ${ARTIFACT_PATH#$WORKSPACE_ROOT/}"
  echo "Unit 1:      $TEST1_STATUS (${TEST1_ELAPSED_MS}ms)"
  echo "Unit 2:      $TEST2_STATUS (${TEST2_ELAPSED_MS}ms)"
  echo "E2E:         $TEST3_STATUS (${TEST3_ELAPSED_MS}ms)"
  echo "Overall:     $OVERALL_STATUS"
  echo "Replay:"
  echo "  $REPLAY_CMD"
fi

if [[ "$OVERALL_STATUS" != "pass" ]]; then
  exit 1
fi
