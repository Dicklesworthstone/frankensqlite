#!/usr/bin/env bash
# CI verification gate for bd-3bw.6: Local Reconstruction Codes for distributed repair (§1.4)
# Validates LRC encode, local/global repair, I/O reduction, and metrics.
set -euo pipefail

echo "=== bd-3bw.6: Local Reconstruction Codes Verification ==="

# Run the harness integration tests
cargo test --package fsqlite-harness --test bd_3bw_6_lrc_distributed_repair -- --nocapture 2>&1

echo ""
echo "[GATE PASS] bd-3bw.6 Local Reconstruction Codes — all tests passed"
