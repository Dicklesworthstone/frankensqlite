#!/usr/bin/env bash
# CI verification gate for bd-mblr.7.9.3: Selection Correctness Audits
# and Fallback Policy
# Validates: lane selection correctness for known paths, fallback policy for
# unknown code paths, safety floor enforcement, selection determinism,
# multi-scenario audit runner, disallowed-fallback failure, audit report
# persistence, impact graph validation, conformance.
set -euo pipefail

echo "=== bd-mblr.7.9.3: Selection Correctness Audits and Fallback Policy Verification ==="

# Run the harness integration tests
cargo test --package fsqlite-harness --test bd_mblr_7_9_3_selection_correctness_audits -- --nocapture 2>&1

echo ""
echo "[GATE PASS] bd-mblr.7.9.3 Selection Correctness Audits — all tests passed"
