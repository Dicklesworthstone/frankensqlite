#!/usr/bin/env bash
# CI verification gate for bd-2uza4.5: Property tests for SwizzlePtr and cooling state machine
# Validates proptest-based roundtrip, tag-bit, CAS, FSM, overflow, and concurrent invariants.
set -euo pipefail

echo "=== bd-2uza4.5: SwizzlePtr Property Tests Verification ==="

# Run the harness integration tests
cargo test --package fsqlite-harness --test bd_2uza4_5_swizzle_property_tests -- --nocapture 2>&1

echo ""
echo "[GATE PASS] bd-2uza4.5 SwizzlePtr Property Tests — all tests passed"
