#!/usr/bin/env bash
# CI verification gate for bd-xox.6: NitroSketch streaming stats for high-frequency telemetry
# Validates sliding window histograms, CMS, memory allocation tracker, and metrics.
set -euo pipefail

echo "=== bd-xox.6: NitroSketch Streaming Stats Verification ==="

# Run the harness integration tests
cargo test --package fsqlite-harness --test bd_xox_6_nitrosketch_streaming -- --nocapture 2>&1

echo ""
echo "[GATE PASS] bd-xox.6 NitroSketch streaming stats — all tests passed"
