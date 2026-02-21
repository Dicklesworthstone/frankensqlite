#!/usr/bin/env bash
# CI verification gate for bd-14vp7.7: Vectorized vs interpreter TPC-H benchmark
# Validates: Q1/Q6 correctness, throughput comparison, parallel scaling,
# sort/hash-join operators, metrics, and deterministic results.
set -euo pipefail

echo "=== bd-14vp7.7: Vectorized vs Interpreter Benchmark Verification ==="

# Run the harness integration tests
cargo test --package fsqlite-harness --test bd_14vp7_7_vectorized_vs_interpreter_benchmark -- --nocapture 2>&1

echo ""
echo "[GATE PASS] bd-14vp7.7 Vectorized vs Interpreter Benchmark — all tests passed"
