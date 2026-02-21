#!/usr/bin/env bash
# CI verification gate for bd-u43hx: Flat Combining for lock contention hot spots (§14.2)
# Validates contention-to-throughput, latency comparison, slot lifecycle, and metrics.
set -euo pipefail

echo "=== bd-u43hx: Flat Combining Contention Hot Spots Verification ==="

# Run the harness integration tests
cargo test --package fsqlite-harness --test bd_u43hx_flat_combining_contention -- --nocapture 2>&1

echo ""
echo "[GATE PASS] bd-u43hx Flat Combining contention hot spots — all tests passed"
