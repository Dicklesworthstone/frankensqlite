#!/usr/bin/env bash
# CI verification gate for bd-2y306.3: Epoch advance frequency tuning
# Validates: GC scheduler frequency computation, should_tick interval behavior,
# EBR guard lifecycle latency, GC tick latency distributions (p50/p95/p99),
# memory-vs-frequency tradeoff, and background collection patterns.
set -euo pipefail

echo "=== bd-2y306.3: Epoch Advance Tuning Verification ==="

# Run the harness integration tests
cargo test --package fsqlite-harness --test bd_2y306_3_epoch_advance_tuning -- --nocapture 2>&1

echo ""
echo "[GATE PASS] bd-2y306.3 Epoch Advance Tuning — all tests passed"
