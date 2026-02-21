#!/usr/bin/env bash
# CI verification gate for bd-t6sv2.4: SQLite Conformance & Compatibility Dashboard
# Validates: feature taxonomy completeness, Bayesian scorecard generation,
# MVCC divergence cataloging, per-category parity breakdowns, feature matrix
# serialization, dashboard report determinism.
set -euo pipefail

echo "=== bd-t6sv2.4: SQLite Conformance & Compatibility Dashboard Verification ==="

# Run the harness integration tests
cargo test --package fsqlite-harness --test bd_t6sv2_4_sqlite_conformance_dashboard -- --nocapture 2>&1

echo ""
echo "[GATE PASS] bd-t6sv2.4 SQLite Conformance Dashboard — all tests passed"
