#!/usr/bin/env bash
# CI verification gate for bd-j2cfs: Differential Privacy for aggregate queries (§12.5)
# Validates: PRAGMA configuration model, per-session budget isolation, utility analysis
# (accuracy improves with epsilon), sequential composition budget tracking, multi-aggregate
# workflows (COUNT/SUM/AVG), uniform and proportional budget allocation, sensitivity bounds,
# Laplace/Gaussian noise scale formulas, error display, metrics serialization, invalid
# parameter rejection, budget exhaustion edge cases, conformance.
set -euo pipefail

echo "=== bd-j2cfs: Differential Privacy for Aggregate Queries Verification ==="

# Run the harness integration tests
cargo test --package fsqlite-harness --test bd_j2cfs_differential_privacy_aggregate_queries -- --nocapture 2>&1

echo ""
echo "[GATE PASS] bd-j2cfs Differential Privacy for Aggregate Queries — all tests passed"
