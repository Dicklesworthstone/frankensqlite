#!/usr/bin/env bash
# CI verification gate for bd-mblr.3.2: CI E2E Matrix and Artifact Upload Gate
# Validates: E2E matrix lane tier classification (correctness/recovery/concurrency),
# artifact upload gate enforcement, failure bundle integration, multi-lane pipeline
# execution, retry and quarantine policy, bisect dispatch concurrency, artifact
# manifest completeness and serialization, run context correlation, conformance.
set -euo pipefail

echo "=== bd-mblr.3.2: CI E2E Matrix and Artifact Upload Gate Verification ==="

# Run the harness integration tests
cargo test --package fsqlite-harness --test bd_mblr_3_2_ci_e2e_matrix_artifact_upload_gate -- --nocapture 2>&1

echo ""
echo "[GATE PASS] bd-mblr.3.2 CI E2E Matrix and Artifact Upload Gate — all tests passed"
