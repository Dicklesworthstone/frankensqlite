#!/usr/bin/env bash
# CI verification gate for bd-19u.4: NitroSketch probabilistic telemetry
# Validates Count-Min Sketch, streaming histograms, and sketch metrics.
set -euo pipefail

echo "=== bd-19u.4: NitroSketch Telemetry Verification ==="

# Run the harness integration tests
cargo test --package fsqlite-harness --test bd_19u_4_nitrosketch_telemetry -- --nocapture 2>&1

echo ""
echo "[GATE PASS] bd-19u.4 NitroSketch telemetry — all tests passed"
