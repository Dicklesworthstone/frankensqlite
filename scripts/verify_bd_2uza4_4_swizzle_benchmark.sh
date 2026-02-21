#!/usr/bin/env bash
# CI verification gate for bd-2uza4.4: Swizzled vs unswizzled B-tree point lookup benchmark
# Validates: throughput comparison, depth scaling, page table overhead, SwizzlePtr decode cost.
set -euo pipefail

echo "=== bd-2uza4.4: Swizzle Benchmark Verification ==="

# Run the harness integration tests
cargo test --package fsqlite-harness --test bd_2uza4_4_swizzle_benchmark -- --nocapture 2>&1

echo ""
echo "[GATE PASS] bd-2uza4.4 Swizzle Benchmark — all tests passed"
