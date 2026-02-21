#!/usr/bin/env bash
# CI verification gate for bd-3mr81: NitroSketch for streaming query statistics (§15.5)
# Validates NitroSketch cardinality, CMS frequency, SpaceSaving heavy hitters, and metrics.
set -euo pipefail

echo "=== bd-3mr81: NitroSketch Streaming Query Stats Verification ==="

# Run the harness integration tests
cargo test --package fsqlite-harness --test bd_3mr81_nitrosketch_query_stats -- --nocapture 2>&1

echo ""
echo "[GATE PASS] bd-3mr81 NitroSketch streaming query stats — all tests passed"
