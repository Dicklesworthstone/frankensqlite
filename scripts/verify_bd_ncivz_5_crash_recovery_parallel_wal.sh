#!/usr/bin/env bash
# CI verification gate for bd-ncivz.5: Crash Recovery with Parallel WAL Multi-Buffer Replay
# Validates: crash scenario catalog, fault category mapping, checksum failure recovery,
# WAL chain invalid reasons, fault injection VFS, group commit epoch ordering,
# cross-process crash roles/points, durability matrix, compaction policy MDP,
# SSI evidence metrics, recovery decisions, multi-epoch consolidation, conformance.
set -euo pipefail

echo "=== bd-ncivz.5: Crash Recovery with Parallel WAL Multi-Buffer Replay Verification ==="

# Run the harness integration tests
cargo test --package fsqlite-harness --test bd_ncivz_5_crash_recovery_parallel_wal -- --nocapture 2>&1

echo ""
echo "[GATE PASS] bd-ncivz.5 Crash Recovery with Parallel WAL Multi-Buffer Replay — all tests passed"
