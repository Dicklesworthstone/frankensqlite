#!/usr/bin/env bash
# CI verification gate for bd-2csg6.1: Self-healing durability demo
# Validates: RaptorQ erasure coding, BLAKE3 corruption detection,
# single/multi-page repair with witness proofs, graceful degradation,
# repair determinism, evidence ledger serialization.
set -euo pipefail

echo "=== bd-2csg6.1: Self-Healing Durability Demo Verification ==="

# Run the harness integration tests
cargo test --package fsqlite-harness --test bd_2csg6_1_self_healing_durability_demo -- --nocapture 2>&1

echo ""
echo "[GATE PASS] bd-2csg6.1 Self-Healing Durability Demo — all tests passed"
