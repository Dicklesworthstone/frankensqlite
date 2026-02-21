#!/usr/bin/env bash
# CI verification gate for bd-14vp7.6: Morsel-driven parallel dispatcher
# Validates: morsel partitioning, pipeline tasks, work-stealing execution,
# pipeline barriers, scaling, NUMA-awareness, exchange operators, metrics,
# and deterministic results.
set -euo pipefail

echo "=== bd-14vp7.6: Morsel Dispatcher Verification ==="

# Run the harness integration tests
cargo test --package fsqlite-harness --test bd_14vp7_6_morsel_dispatcher -- --nocapture 2>&1

echo ""
echo "[GATE PASS] bd-14vp7.6 Morsel Dispatcher — all tests passed"
