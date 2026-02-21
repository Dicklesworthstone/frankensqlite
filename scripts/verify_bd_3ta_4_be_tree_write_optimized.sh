#!/usr/bin/env bash
# CI verification gate for bd-3ta.4: Bε-tree write-optimized index option (§15.2)
# Validates buffered inserts, flush cascades, point/range queries, delete, metrics.
set -euo pipefail

echo "=== bd-3ta.4: Bε-tree Write-Optimized Index Verification ==="

# Run the harness integration tests
cargo test --package fsqlite-harness --test bd_3ta_4_be_tree_write_optimized -- --nocapture 2>&1

echo ""
echo "[GATE PASS] bd-3ta.4 Bε-tree Write-Optimized Index — all tests passed"
