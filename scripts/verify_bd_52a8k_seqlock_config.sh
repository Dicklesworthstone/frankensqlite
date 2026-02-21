#!/usr/bin/env bash
# CI verification gate for bd-52a8k: Seqlocks for rarely-changing config (§14.9)
# Validates seqlock config pattern, zero-overhead reads, bounded retry, and metrics.
set -euo pipefail

echo "=== bd-52a8k: Seqlocks for Config Verification ==="

# Run the harness integration tests
cargo test --package fsqlite-harness --test bd_52a8k_seqlock_config -- --nocapture 2>&1

echo ""
echo "[GATE PASS] bd-52a8k Seqlocks for config — all tests passed"
