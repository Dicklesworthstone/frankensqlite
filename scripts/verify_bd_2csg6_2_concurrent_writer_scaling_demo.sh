#!/usr/bin/env bash
# CI verification gate for bd-2csg6.2: Concurrent writer scaling demo
# Validates: MVCC + SSI concurrent writers vs single-writer baseline,
# FCW conflict detection, SSI abort transparency, throughput scaling.
set -euo pipefail

echo "=== bd-2csg6.2: Concurrent Writer Scaling Demo Verification ==="

# Run the harness integration tests
cargo test --package fsqlite-harness --test bd_2csg6_2_concurrent_writer_scaling_demo -- --nocapture 2>&1

echo ""
echo "[GATE PASS] bd-2csg6.2 Concurrent Writer Scaling Demo — all tests passed"
