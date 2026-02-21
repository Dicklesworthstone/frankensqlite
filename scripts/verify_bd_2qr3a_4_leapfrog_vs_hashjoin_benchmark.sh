#!/usr/bin/env bash
# CI verification gate for bd-2qr3a.4: Leapfrog Triejoin vs pairwise hash-join benchmark
# Validates: correctness (2-6 way joins), throughput comparison, scaling,
# seek metrics, composite keys, zero intermediate blowup.
set -euo pipefail

echo "=== bd-2qr3a.4: Leapfrog vs Hash-Join Benchmark Verification ==="

# Run the harness integration tests
cargo test --package fsqlite-harness --test bd_2qr3a_4_leapfrog_vs_hashjoin_benchmark -- --nocapture 2>&1

echo ""
echo "[GATE PASS] bd-2qr3a.4 Leapfrog vs Hash-Join Benchmark — all tests passed"
