#!/usr/bin/env bash
# CI verification gate for bd-t6sv2.14: Extension Compatibility Matrix
# Validates: extension module catalog, feature flag truth table, surface kind
# classification, canonical matrix construction/validation, per-module entry
# counts, status distribution, extension coverage computation, determinism,
# omissions/future candidates, tag-based filtering, parity status scoring.
set -euo pipefail

echo "=== bd-t6sv2.14: Extension Compatibility Matrix Verification ==="

# Run the harness integration tests
cargo test --package fsqlite-harness --test bd_t6sv2_14_extension_compatibility_matrix -- --nocapture 2>&1

echo ""
echo "[GATE PASS] bd-t6sv2.14 Extension Compatibility Matrix — all tests passed"
