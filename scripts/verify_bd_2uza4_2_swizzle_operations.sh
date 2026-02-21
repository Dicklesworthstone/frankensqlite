#!/usr/bin/env bash
# CI verification gate for bd-2uza4.2: SwizzlePtr and B-tree swizzle/unswizzle operations
# Validates CAS operations, SwizzleRegistry lifecycle, temperature FSM, concurrent access, metrics.
set -euo pipefail

echo "=== bd-2uza4.2: SwizzlePtr Operations Verification ==="

# Run the harness integration tests
cargo test --package fsqlite-harness --test bd_2uza4_2_swizzle_operations -- --nocapture 2>&1

echo ""
echo "[GATE PASS] bd-2uza4.2 SwizzlePtr Operations — all tests passed"
