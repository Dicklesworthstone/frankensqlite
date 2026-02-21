#!/usr/bin/env bash
# CI verification gate for bd-2y306.7: EBR Version-Chain GC rollout
# Validates: end-to-end GC lifecycle with EBR, budget enforcement, stale-reader
# warning budget, scheduler-driven tick cadence, concurrent writer interference,
# chain-pressure feedback loop, eager reclaim fallback, tail latency targets.
set -euo pipefail

echo "=== bd-2y306.7: EBR GC Rollout Verification ==="

# Run the harness integration tests
cargo test --package fsqlite-harness --test bd_2y306_7_ebr_gc_rollout -- --nocapture 2>&1

echo ""
echo "[GATE PASS] bd-2y306.7 EBR GC Rollout — all tests passed"
