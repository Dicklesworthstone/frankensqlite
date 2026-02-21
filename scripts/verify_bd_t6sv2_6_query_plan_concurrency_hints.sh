#!/usr/bin/env bash
# CI verification gate for bd-t6sv2.6: Query Plan Concurrency Hints
# Validates: probabilistic conflict estimation, AMS F2 sketch, NitroSketch,
# SpaceSaving heavy-hitter, contention bucket quantization, concurrent writer
# invariant catalog, parity assessment, deterministic seeding, monotonicity.
set -euo pipefail

echo "=== bd-t6sv2.6: Query Plan Concurrency Hints Verification ==="

# Run the harness integration tests
cargo test --package fsqlite-harness --test bd_t6sv2_6_query_plan_concurrency_hints -- --nocapture 2>&1

echo ""
echo "[GATE PASS] bd-t6sv2.6 Query Plan Concurrency Hints — all tests passed"
