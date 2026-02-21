#!/usr/bin/env bash
# CI verification gate for bd-t6sv2.12: Deterministic Replay Debugger
# Validates: drift detector lifecycle, replay session windowing, ReplaySummary
# JSON round-trip, rebase metrics, rebase eligibility, time travel errors,
# bisect replay manifest construction/validation/evaluation, FsLab config.
set -euo pipefail

echo "=== bd-t6sv2.12: Deterministic Replay Debugger Verification ==="

# Run the harness integration tests
cargo test --package fsqlite-harness --test bd_t6sv2_12_deterministic_replay_debugger -- --nocapture 2>&1

echo ""
echo "[GATE PASS] bd-t6sv2.12 Deterministic Replay Debugger — all tests passed"
