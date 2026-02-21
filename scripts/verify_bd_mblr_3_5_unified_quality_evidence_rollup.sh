#!/usr/bin/env bash
# CI verification gate for bd-mblr.3.5: Unified Quality Evidence Rollup
# and Release-Readiness Gate
# Validates: 5-gate rollup aggregation, outcome aggregation (Fail > Warn > Pass),
# contract validation, determinism, replay contract, JSON roundtrip, backward
# compatibility detection, human summary rendering, embedded reports, artifact
# URI indexing, conformance.
set -euo pipefail

echo "=== bd-mblr.3.5: Unified Quality Evidence Rollup Verification ==="

# Run the harness integration tests
cargo test --package fsqlite-harness --test bd_mblr_3_5_unified_quality_evidence_rollup -- --nocapture 2>&1

echo ""
echo "[GATE PASS] bd-mblr.3.5 Unified Quality Evidence Rollup — all tests passed"
