#!/usr/bin/env bash
# CI verification gate for bd-z4mtc: RCU/QSBR for hot metadata paths (§14.8)
# Validates QSBR registration, grace periods, RcuCell/Pair/Triple, and metrics.
set -euo pipefail

echo "=== bd-z4mtc: RCU/QSBR Metadata Hot Paths Verification ==="

# Run the harness integration tests
cargo test --package fsqlite-harness --test bd_z4mtc_rcu_qsbr_metadata -- --nocapture 2>&1

echo ""
echo "[GATE PASS] bd-z4mtc RCU/QSBR metadata hot paths — all tests passed"
