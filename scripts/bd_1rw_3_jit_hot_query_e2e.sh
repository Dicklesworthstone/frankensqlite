#!/usr/bin/env bash
# bd_1rw_3_jit_hot_query_e2e.sh — deterministic verifier for bd-1rw.3
#
# Runs:
# 1) VDBE unit check for JIT compile + cache-hit metrics
# 2) VDBE unit check for JIT compile-failure fallback
# 3) Core PRAGMA surface check for JIT controls/stats/reset
# 4) E2E replay test with trace/run/scenario IDs + artifact emission
#
# Writes a machine-readable artifact bundle with deterministic replay metadata.

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

BEAD_ID="bd-1rw.3"
SCENARIO_ID="${SCENARIO_ID:-JIT-HOT-QUERY-E2E}"
SEED="${SEED:-1003202603}"
TRACE_ID="${TRACE_ID:-$SEED}"
RUN_ID="${RUN_ID:-${BEAD_ID}-seed-${SEED}}"

ARTIFACT_DIR="$WORKSPACE_ROOT/artifacts/bd-1rw.3"
ARTIFACT_PATH="${FSQLITE_JIT_HOT_QUERY_ARTIFACT:-$ARTIFACT_DIR/jit_hot_query_artifact.json}"
JIT_E2E_ARTIFACT="${FSQLITE_JIT_E2E_ARTIFACT:-$ARTIFACT_DIR/jit_e2e_test_artifact.json}"
mkdir -p "$ARTIFACT_DIR"

TEST1_NAME="vdbe_jit_metrics_unit"
TEST1_FILTER="test_jit_scaffold_metrics_compile_and_cache_hit"
TEST2_NAME="vdbe_jit_fallback_unit"
TEST2_FILTER="test_jit_scaffold_compile_failure_falls_back_to_interpreter"
TEST3_NAME="core_jit_pragma_unit"
TEST3_FILTER="test_pragma_jit_controls_stats_and_reset"
TEST4_NAME="jit_e2e_replay_artifact"
TEST4_FILTER="bd_1rw_3_jit_e2e_replay_emits_artifact"

if ! $NO_RCH && command -v rch >/dev/null 2>&1; then
  RUNNER=(rch exec --)
  USES_RCH=true
else
  RUNNER=()
  USES_RCH=false
fi

run_unit_test() {
  local test_name="$1"
  local crate="$2"
  local filter="$3"
  local log_path="$ARTIFACT_DIR/${test_name}.log"
  local start_ms end_ms elapsed_ms status

  start_ms="$(date +%s%3N)"
  if "${RUNNER[@]}" cargo test -p "$crate" "$filter" -- --nocapture --test-threads=1 >"$log_path" 2>&1; then
    status="pass"
  else
    status="fail"
  fi
  if [[ "$status" == "pass" ]] && ! rg -q "test .*${filter} .* ok" "$log_path"; then
    status="fail"
  fi
  end_ms="$(date +%s%3N)"
  elapsed_ms="$((end_ms - start_ms))"

  printf '%s|%s|%s\n' "$status" "$elapsed_ms" "$log_path"
}

run_e2e_test() {
  local log_path="$ARTIFACT_DIR/${TEST4_NAME}.log"
  local start_ms end_ms elapsed_ms status

  start_ms="$(date +%s%3N)"
  if "${RUNNER[@]}" env \
    RUN_ID="$RUN_ID" \
    TRACE_ID="$TRACE_ID" \
    SCENARIO_ID="$SCENARIO_ID" \
    SEED="$SEED" \
    FSQLITE_JIT_E2E_ARTIFACT="$JIT_E2E_ARTIFACT" \
    cargo test -p fsqlite-e2e --test bd_1rw_3_jit_hot_query_replay "$TEST4_FILTER" -- --exact --nocapture --test-threads=1 >"$log_path" 2>&1; then
    status="pass"
  else
    status="fail"
  fi
  if [[ "$status" == "pass" ]] && ! rg -q "test .*${TEST4_FILTER} .* ok" "$log_path"; then
    status="fail"
  fi
  if [[ "$status" == "pass" ]]; then
    if $USES_RCH; then
      if ! rg -q "DEBUG bead_id=${BEAD_ID} .*artifact_path=" "$log_path"; then
        status="fail"
      fi
    elif [[ ! -s "$JIT_E2E_ARTIFACT" ]]; then
      status="fail"
    fi
  fi
  end_ms="$(date +%s%3N)"
  elapsed_ms="$((end_ms - start_ms))"

  printf '%s|%s|%s\n' "$status" "$elapsed_ms" "$log_path"
}

test1_result="$(run_unit_test "$TEST1_NAME" "fsqlite-vdbe" "$TEST1_FILTER")"
test2_result="$(run_unit_test "$TEST2_NAME" "fsqlite-vdbe" "$TEST2_FILTER")"
test3_result="$(run_unit_test "$TEST3_NAME" "fsqlite-core" "$TEST3_FILTER")"
test4_result="$(run_e2e_test)"

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

TEST4_STATUS="${test4_result%%|*}"
test4_tail="${test4_result#*|}"
TEST4_ELAPSED_MS="${test4_tail%%|*}"
TEST4_LOG_PATH="${test4_tail#*|}"

OVERALL_STATUS="pass"
if [[ "$TEST1_STATUS" != "pass" || "$TEST2_STATUS" != "pass" || "$TEST3_STATUS" != "pass" || "$TEST4_STATUS" != "pass" ]]; then
  OVERALL_STATUS="fail"
fi

REPLAY_CMD="scripts/bd_1rw_3_jit_hot_query_e2e.sh --json"

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
      "crate": "fsqlite-vdbe",
      "filter": "$TEST1_FILTER",
      "status": "$TEST1_STATUS",
      "elapsed_ms": $TEST1_ELAPSED_MS,
      "log_path": "${TEST1_LOG_PATH#$WORKSPACE_ROOT/}"
    },
    {
      "test_id": "$TEST2_NAME",
      "crate": "fsqlite-vdbe",
      "filter": "$TEST2_FILTER",
      "status": "$TEST2_STATUS",
      "elapsed_ms": $TEST2_ELAPSED_MS,
      "log_path": "${TEST2_LOG_PATH#$WORKSPACE_ROOT/}"
    },
    {
      "test_id": "$TEST3_NAME",
      "crate": "fsqlite-core",
      "filter": "$TEST3_FILTER",
      "status": "$TEST3_STATUS",
      "elapsed_ms": $TEST3_ELAPSED_MS,
      "log_path": "${TEST3_LOG_PATH#$WORKSPACE_ROOT/}"
    },
    {
      "test_id": "$TEST4_NAME",
      "crate": "fsqlite-e2e",
      "filter": "$TEST4_FILTER",
      "status": "$TEST4_STATUS",
      "elapsed_ms": $TEST4_ELAPSED_MS,
      "log_path": "${TEST4_LOG_PATH#$WORKSPACE_ROOT/}",
      "artifact_path": "${JIT_E2E_ARTIFACT#$WORKSPACE_ROOT/}"
    }
  ],
  "replay_command": "$REPLAY_CMD"
}
EOF

if $JSON_OUTPUT; then
  cat "$ARTIFACT_PATH"
else
  echo "=== bd-1rw.3 JIT Hot Query Verification ==="
  echo "Run ID:      $RUN_ID"
  echo "Trace ID:    $TRACE_ID"
  echo "Scenario ID: $SCENARIO_ID"
  echo "Seed:        $SEED"
  echo "Artifact:    ${ARTIFACT_PATH#$WORKSPACE_ROOT/}"
  echo "Unit 1:      $TEST1_STATUS (${TEST1_ELAPSED_MS}ms)"
  echo "Unit 2:      $TEST2_STATUS (${TEST2_ELAPSED_MS}ms)"
  echo "Unit 3:      $TEST3_STATUS (${TEST3_ELAPSED_MS}ms)"
  echo "E2E:         $TEST4_STATUS (${TEST4_ELAPSED_MS}ms)"
  echo "Overall:     $OVERALL_STATUS"
  echo "Replay:"
  echo "  $REPLAY_CMD"
fi

if [[ "$OVERALL_STATUS" != "pass" ]]; then
  exit 1
fi
