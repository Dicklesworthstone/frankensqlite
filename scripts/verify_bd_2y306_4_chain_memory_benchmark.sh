#!/usr/bin/env bash
# CI verification gate for bd-2y306.4: chain-memory benchmark workloads.
set -euo pipefail

WORKSPACE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "=== bd-2y306.4: Chain-Memory Benchmark Verification ==="

RESULT_JSON="$($WORKSPACE_ROOT/scripts/bd_2y306_4_chain_memory_benchmark_e2e.sh --json)"
if ! jq -e '.overall_status == "pass"' >/dev/null <<<"$RESULT_JSON"; then
  echo "ERROR: bd-2y306.4 verification reported failure" >&2
  echo "$RESULT_JSON" | jq '.'
  exit 1
fi

echo "[GATE PASS] bd-2y306.4 Chain-Memory Benchmark — all checks passed"
