#!/usr/bin/env bash
# CI verification gate for bd-368z9: Proofs of Retrievability for durability audit (§11.11)
# Validates PoR challenge, proof, audit lifecycle, corruption detection, and metrics.
set -euo pipefail

echo "=== bd-368z9: Proofs of Retrievability Verification ==="

# Run the harness integration tests
cargo test --package fsqlite-harness --test bd_368z9_proofs_of_retrievability -- --nocapture 2>&1

echo ""
echo "[GATE PASS] bd-368z9 Proofs of Retrievability — all tests passed"
