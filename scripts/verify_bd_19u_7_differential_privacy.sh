#!/usr/bin/env bash
# CI verification gate for bd-19u.7: Differential privacy for aggregate queries
# Validates Laplace/Gaussian mechanisms, budget tracking, and metrics.
set -euo pipefail

echo "=== bd-19u.7: Differential Privacy Verification ==="

# Run the harness integration tests
cargo test --package fsqlite-harness --test bd_19u_7_differential_privacy -- --nocapture 2>&1

echo ""
echo "[GATE PASS] bd-19u.7 Differential privacy — all tests passed"
