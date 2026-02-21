#!/usr/bin/env bash
# CI verification gate for bd-t6sv2.13: Multi-Process Coordination Documentation & Testing
# Validates: SHM header layout, seqlock protocol, serialized writer lifecycle,
# wire frame codec, permit lifecycle, idempotency cache, canonical ordering,
# GC horizon coordination, cross-thread snapshot consistency.
set -euo pipefail

echo "=== bd-t6sv2.13: Multi-Process Coordination Verification ==="

# Run the harness integration tests
cargo test --package fsqlite-harness --test bd_t6sv2_13_multi_process_coordination -- --nocapture 2>&1

echo ""
echo "[GATE PASS] bd-t6sv2.13 Multi-Process Coordination — all tests passed"
