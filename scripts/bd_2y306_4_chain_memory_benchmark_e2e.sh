#!/usr/bin/env bash
# bd_2y306_4_chain_memory_benchmark_e2e.sh — deterministic verifier for bd-2y306.4
#
# Runs:
# 1) MVCC correctness guard tests for chain-length controls
# 2) Chain-memory benchmark workloads (hot/uniform/zipf)
# 3) Long-reader impact and recovery benchmark
# 4) Replay artifact emission test
#
# Produces machine-readable JSON with run/trace/scenario IDs and per-test timing.

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

BEAD_ID="bd-2y306.4"
SCENARIO_ID="${SCENARIO_ID:-CHAIN-MEMORY-BENCH-E2E}"
SEED="${SEED:-20260227}"
TRACE_ID="${TRACE_ID:-$SEED}"
RUN_ID="${RUN_ID:-${BEAD_ID}-seed-${SEED}}"

ARTIFACT_DIR="$WORKSPACE_ROOT/artifacts/bd-2y306.4"
ARTIFACT_PATH="${FSQLITE_CHAIN_MEMORY_BENCH_CONTROL_ARTIFACT:-$ARTIFACT_DIR/chain_memory_benchmark_artifact.json}"
CHAIN_E2E_ARTIFACT="${FSQLITE_CHAIN_MEMORY_BENCH_ARTIFACT:-$ARTIFACT_DIR/chain_memory_benchmark_e2e_test_artifact.json}"
mkdir -p "$ARTIFACT_DIR"

GUARD1_NAME="mvcc_chain_bound_guard"
GUARD1_FILTER="lifecycle::tests::test_chain_length_bounded_after_10000_updates_same_page"
GUARD2_NAME="mvcc_backpressure_guard"
GUARD2_FILTER="lifecycle::tests::test_chain_backpressure_reports_blocked_when_horizon_pinned"
BENCH1_NAME="e2e_chain_memory_workloads"
BENCH1_FILTER="bd_2y306_4_workloads_plateau_vs_unbounded_control"
BENCH2_NAME="e2e_chain_memory_long_reader"
BENCH2_FILTER="bd_2y306_4_long_reader_impact_documented_and_recovery_bounded"
BENCH3_NAME="e2e_chain_memory_replay_artifact"
BENCH3_FILTER="bd_2y306_4_chain_memory_benchmark_e2e_replay_emits_artifact"

if ! $NO_RCH && command -v rch >/dev/null 2>&1; then
  RUNNER=(rch exec --)
  USES_RCH=true
else
  RUNNER=()
  USES_RCH=false
fi

run_test() {
  local test_name="$1"
  local test_filter="$2"
  local crate="$3"
  local log_path="$ARTIFACT_DIR/${test_name}.log"
  local start_ms end_ms elapsed_ms status

  start_ms="$(date +%s%3N)"
  if "${RUNNER[@]}" cargo test -p "$crate" "$test_filter" -- --exact --nocapture --test-threads=1 >"$log_path" 2>&1; then
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

run_artifact_test() {
  local log_path="$ARTIFACT_DIR/${BENCH3_NAME}.log"
  local start_ms end_ms elapsed_ms status

  start_ms="$(date +%s%3N)"
  if "${RUNNER[@]}" env \
    RUN_ID="$RUN_ID" \
    TRACE_ID="$TRACE_ID" \
    SCENARIO_ID="$SCENARIO_ID" \
    SEED="$SEED" \
    FSQLITE_CHAIN_MEMORY_BENCH_ARTIFACT="$CHAIN_E2E_ARTIFACT" \
    cargo test -p fsqlite-e2e --test bd_2y306_4_chain_memory_benchmarks "$BENCH3_FILTER" -- --exact --nocapture --test-threads=1 >"$log_path" 2>&1; then
    status="pass"
  else
    status="fail"
  fi
  if [[ "$status" == "pass" ]] && ! rg -q "test .*${BENCH3_FILTER} .* ok" "$log_path"; then
    status="fail"
  fi
  if [[ "$status" == "pass" ]]; then
    if $USES_RCH; then
      if ! rg -q "DEBUG bead_id=${BEAD_ID} .*artifact_path=" "$log_path"; then
        status="fail"
      fi
    elif [[ ! -s "$CHAIN_E2E_ARTIFACT" ]]; then
      status="fail"
    fi
  fi
  end_ms="$(date +%s%3N)"
  elapsed_ms="$((end_ms - start_ms))"

  printf '%s|%s|%s\n' "$status" "$elapsed_ms" "$log_path"
}

guard1_result="$(run_test "$GUARD1_NAME" "$GUARD1_FILTER" "fsqlite-mvcc")"
guard2_result="$(run_test "$GUARD2_NAME" "$GUARD2_FILTER" "fsqlite-mvcc")"
bench1_result="$(run_test "$BENCH1_NAME" "$BENCH1_FILTER" "fsqlite-e2e")"
bench2_result="$(run_test "$BENCH2_NAME" "$BENCH2_FILTER" "fsqlite-e2e")"
bench3_result="$(run_artifact_test)"

GUARD1_STATUS="${guard1_result%%|*}"
guard1_tail="${guard1_result#*|}"
GUARD1_ELAPSED_MS="${guard1_tail%%|*}"
GUARD1_LOG_PATH="${guard1_tail#*|}"

GUARD2_STATUS="${guard2_result%%|*}"
guard2_tail="${guard2_result#*|}"
GUARD2_ELAPSED_MS="${guard2_tail%%|*}"
GUARD2_LOG_PATH="${guard2_tail#*|}"

BENCH1_STATUS="${bench1_result%%|*}"
bench1_tail="${bench1_result#*|}"
BENCH1_ELAPSED_MS="${bench1_tail%%|*}"
BENCH1_LOG_PATH="${bench1_tail#*|}"

BENCH2_STATUS="${bench2_result%%|*}"
bench2_tail="${bench2_result#*|}"
BENCH2_ELAPSED_MS="${bench2_tail%%|*}"
BENCH2_LOG_PATH="${bench2_tail#*|}"

BENCH3_STATUS="${bench3_result%%|*}"
bench3_tail="${bench3_result#*|}"
BENCH3_ELAPSED_MS="${bench3_tail%%|*}"
BENCH3_LOG_PATH="${bench3_tail#*|}"

OVERALL_STATUS="pass"
if [[ "$GUARD1_STATUS" != "pass" || "$GUARD2_STATUS" != "pass" || "$BENCH1_STATUS" != "pass" || "$BENCH2_STATUS" != "pass" || "$BENCH3_STATUS" != "pass" ]]; then
  OVERALL_STATUS="fail"
fi

REPLAY_CMD="scripts/bd_2y306_4_chain_memory_benchmark_e2e.sh --json"

cat >"$ARTIFACT_PATH" <<EOF2
{
  "bead_id": "$BEAD_ID",
  "run_id": "$RUN_ID",
  "trace_id": "$TRACE_ID",
  "scenario_id": "$SCENARIO_ID",
  "seed": $SEED,
  "overall_status": "$OVERALL_STATUS",
  "tests": [
    {
      "test_id": "$GUARD1_NAME",
      "crate": "fsqlite-mvcc",
      "filter": "$GUARD1_FILTER",
      "status": "$GUARD1_STATUS",
      "elapsed_ms": $GUARD1_ELAPSED_MS,
      "log_path": "${GUARD1_LOG_PATH#$WORKSPACE_ROOT/}"
    },
    {
      "test_id": "$GUARD2_NAME",
      "crate": "fsqlite-mvcc",
      "filter": "$GUARD2_FILTER",
      "status": "$GUARD2_STATUS",
      "elapsed_ms": $GUARD2_ELAPSED_MS,
      "log_path": "${GUARD2_LOG_PATH#$WORKSPACE_ROOT/}"
    },
    {
      "test_id": "$BENCH1_NAME",
      "crate": "fsqlite-e2e",
      "filter": "$BENCH1_FILTER",
      "status": "$BENCH1_STATUS",
      "elapsed_ms": $BENCH1_ELAPSED_MS,
      "log_path": "${BENCH1_LOG_PATH#$WORKSPACE_ROOT/}"
    },
    {
      "test_id": "$BENCH2_NAME",
      "crate": "fsqlite-e2e",
      "filter": "$BENCH2_FILTER",
      "status": "$BENCH2_STATUS",
      "elapsed_ms": $BENCH2_ELAPSED_MS,
      "log_path": "${BENCH2_LOG_PATH#$WORKSPACE_ROOT/}"
    },
    {
      "test_id": "$BENCH3_NAME",
      "crate": "fsqlite-e2e",
      "filter": "$BENCH3_FILTER",
      "status": "$BENCH3_STATUS",
      "elapsed_ms": $BENCH3_ELAPSED_MS,
      "log_path": "${BENCH3_LOG_PATH#$WORKSPACE_ROOT/}",
      "artifact_path": "${CHAIN_E2E_ARTIFACT#$WORKSPACE_ROOT/}"
    }
  ],
  "replay_command": "$REPLAY_CMD"
}
EOF2

if $JSON_OUTPUT; then
  cat "$ARTIFACT_PATH"
else
  echo "=== bd-2y306.4 Chain-Memory Benchmark Verification ==="
  echo "Run ID:      $RUN_ID"
  echo "Trace ID:    $TRACE_ID"
  echo "Scenario ID: $SCENARIO_ID"
  echo "Seed:        $SEED"
  echo "Artifact:    ${ARTIFACT_PATH#$WORKSPACE_ROOT/}"
  echo "Guard 1:     $GUARD1_STATUS (${GUARD1_ELAPSED_MS}ms)"
  echo "Guard 2:     $GUARD2_STATUS (${GUARD2_ELAPSED_MS}ms)"
  echo "Benchmark 1: $BENCH1_STATUS (${BENCH1_ELAPSED_MS}ms)"
  echo "Benchmark 2: $BENCH2_STATUS (${BENCH2_ELAPSED_MS}ms)"
  echo "Benchmark 3: $BENCH3_STATUS (${BENCH3_ELAPSED_MS}ms)"
  echo "Overall:     $OVERALL_STATUS"
  echo "Replay:"
  echo "  $REPLAY_CMD"
fi

if [[ "$OVERALL_STATUS" != "pass" ]]; then
  exit 1
fi
