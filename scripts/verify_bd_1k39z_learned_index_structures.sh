#!/usr/bin/env bash
# CI verification gate for bd-1k39z: Learned Index Structures for static lookup (§8.4)
# Validates PiecewiseLinearModel training, bounded-error lookups, and metrics.
set -euo pipefail

echo "=== bd-1k39z: Learned Index Structures Verification ==="

# Run the harness integration tests
cargo test --package fsqlite-harness --test bd_1k39z_learned_index_structures -- --nocapture 2>&1

echo ""
echo "[GATE PASS] bd-1k39z Learned Index Structures — all tests passed"
