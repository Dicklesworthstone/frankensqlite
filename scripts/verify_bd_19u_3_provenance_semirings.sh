#!/usr/bin/env bash
# CI verification gate for bd-19u.3: Provenance semirings for query lineage
# Validates semiring algebra, why/how/why-not provenance, tracker pipeline, and metrics.
set -euo pipefail

echo "=== bd-19u.3: Provenance Semirings Verification ==="

# Run the harness integration tests
cargo test --package fsqlite-harness --test bd_19u_3_provenance_semirings -- --nocapture 2>&1

echo ""
echo "[GATE PASS] bd-19u.3 Provenance semirings — all tests passed"
