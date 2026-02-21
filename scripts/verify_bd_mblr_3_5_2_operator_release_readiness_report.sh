#!/usr/bin/env bash
# CI verification gate for bd-mblr.3.5.2: Operator-Facing Release Readiness
# Report with Failure Drill-Down
# Validates: readiness verdict, gate status summary, blocked criteria
# identification, invariant drift drill-down, scenario drift drill-down,
# artifact link resolution, report compactness, determinism, JSON round-trip,
# evidence-link integrity, conformance.
set -euo pipefail

echo "=== bd-mblr.3.5.2: Operator-Facing Release Readiness Report Verification ==="

# Run the harness integration tests
cargo test --package fsqlite-harness --test bd_mblr_3_5_2_operator_release_readiness_report -- --nocapture 2>&1

echo ""
echo "[GATE PASS] bd-mblr.3.5.2 Operator-Facing Release Readiness Report — all tests passed"
