#!/usr/bin/env bash
# CI verification gate for bd-ncivz.4: Parallel WAL vs Single WAL Throughput Benchmark
# Validates: group commit configuration/lifecycle, consolidation metrics, WAL metrics,
# fsync reduction ratio, benchmark corpus construction/validation/determinism,
# checkpoint mode coverage, scaling concurrency parameters.
set -euo pipefail

echo "=== bd-ncivz.4: Parallel WAL vs Single WAL Throughput Benchmark Verification ==="

# Run the harness integration tests
cargo test --package fsqlite-harness --test bd_ncivz_4_parallel_wal_throughput_benchmark -- --nocapture 2>&1

echo ""
echo "[GATE PASS] bd-ncivz.4 Parallel WAL vs Single WAL Throughput Benchmark — all tests passed"
