#!/usr/bin/env bash
# CI verification gate for bd-mblr.3.5.1.1: Manifest Ingestion Regression
# Validates: JSON round-trip preservation, replay-mode determinism, contract
# validation on ingested manifests, gate completeness, artifact archive
# round-trip, tamper detection, backward compatibility, evidence regression
# for unit/e2e/logging gates, conformance.
set -euo pipefail

echo "=== bd-mblr.3.5.1.1: Manifest Ingestion Regression Verification ==="

# Run the harness integration tests
cargo test --package fsqlite-harness --test bd_mblr_3_5_1_1_manifest_ingestion_regression -- --nocapture 2>&1

echo ""
echo "[GATE PASS] bd-mblr.3.5.1.1 Manifest Ingestion Regression — all tests passed"
