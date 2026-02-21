#!/usr/bin/env bash
# CI verification gate for bd-2uza4.3: Page cooling/heating state machine with eviction integration
# Validates: HOT/COOLING/COLD transitions, access-based re-heating, root pinning,
# eviction protocol, cooling scan, concurrent access, and metrics.
set -euo pipefail

echo "=== bd-2uza4.3: Cooling State Machine Verification ==="

# Run the harness integration tests
cargo test --package fsqlite-harness --test bd_2uza4_3_cooling_state_machine -- --nocapture 2>&1

echo ""
echo "[GATE PASS] bd-2uza4.3 Cooling State Machine — all tests passed"
