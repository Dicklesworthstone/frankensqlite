#!/usr/bin/env bash
# CI verification gate for bd-mblr.7.3.3: Performance SLO CI Gate
# with Waiver Lifecycle
# Validates: CI gate pass/fail/warning paths, SLO threshold enforcement
# (p95, throughput), waiver grant/expiry/severity-tolerance, multi-scenario
# worst-verdict propagation, governance report persistence and round-trip,
# deterministic report generation, policy and registry validation, expired
# waiver tracking, edge cases, conformance.
set -euo pipefail

echo "=== bd-mblr.7.3.3: Performance SLO CI Gate with Waiver Lifecycle Verification ==="

# Run the harness integration tests
cargo test --package fsqlite-harness --test bd_mblr_7_3_3_perf_slo_ci_gate_waiver_lifecycle -- --nocapture 2>&1

echo ""
echo "[GATE PASS] bd-mblr.7.3.3 Performance SLO CI Gate — all tests passed"
