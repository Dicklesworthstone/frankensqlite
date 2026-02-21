#!/usr/bin/env bash
# CI verification gate for bd-ncivz.7: Alien Contract — Parallel WAL group-commit
# with deterministic fallback
# Validates: two-barrier durability contract, WriteCoordinator lifecycle, CommitIndex
# FCW detection, GroupCommitBatch lifecycle, marker chain linking/integrity, shutdown
# rejection, two-phase commit state machine, recovery action determination,
# WAL journal parity, concurrent writer parity, replay harness regime classification,
# lane selector safety domains, group commit metrics, commit time monotonicity,
# operating mode discrimination, conformance.
set -euo pipefail

echo "=== bd-ncivz.7: Alien Contract — Parallel WAL Group-Commit Verification ==="

# Run the harness integration tests
cargo test --package fsqlite-harness --test bd_ncivz_7_alien_contract_parallel_wal_group_commit -- --nocapture 2>&1

echo ""
echo "[GATE PASS] bd-ncivz.7 Alien Contract — Parallel WAL Group-Commit — all tests passed"
