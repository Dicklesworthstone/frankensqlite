#!/usr/bin/env bash
# CI verification gate for bd-nlcyx: Database Cracking / Adaptive Indexing (§8.8)
# Validates CrackedColumn range queries, progressive refinement, and metrics.
set -euo pipefail

echo "=== bd-nlcyx: Database Cracking / Adaptive Indexing Verification ==="

# Run the harness integration tests
cargo test --package fsqlite-harness --test bd_nlcyx_database_cracking -- --nocapture 2>&1

echo ""
echo "[GATE PASS] bd-nlcyx Database Cracking — all tests passed"
