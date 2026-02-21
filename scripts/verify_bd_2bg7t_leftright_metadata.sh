#!/usr/bin/env bash
# CI verification gate for bd-2bg7t: Left-Right Sync for metadata hot paths (§14.3)
# Validates wait-free reads, pair/triple consistency, metrics, and side alternation.
set -euo pipefail

echo "=== bd-2bg7t: Left-Right Metadata Hot Paths Verification ==="

# Run the harness integration tests
cargo test --package fsqlite-harness --test bd_2bg7t_leftright_metadata -- --nocapture 2>&1

echo ""
echo "[GATE PASS] bd-2bg7t Left-Right metadata hot paths — all tests passed"
