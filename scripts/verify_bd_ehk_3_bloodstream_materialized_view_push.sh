#!/usr/bin/env bash
# CI verification gate for bd-ehk.3: Bloodstream — FrankenSQLite materialized view
# push to render tree
# Validates: DeltaKind variants, AlgebraicDelta construction, DeltaBatch lifecycle,
# delta coalescing algebra (INSERT+DELETE cancel, INSERT+UPDATE merge, UPDATE+DELETE,
# DELETE+INSERT), ViewBinding lifecycle (active/suspended/detached), PropagationEngine
# bind/unbind/suspend/resume/shutdown/max-bindings, delta routing to matching bindings,
# partial propagation, PropagationMetrics accumulation and percentile computation,
# contract violation detection, tracing/metrics contract constants, conformance.
set -euo pipefail

echo "=== bd-ehk.3: Bloodstream — Materialized View Push to Render Tree Verification ==="

# Run the harness integration tests
cargo test --package fsqlite-harness --test bd_ehk_3_bloodstream_materialized_view_push -- --nocapture 2>&1

echo ""
echo "[GATE PASS] bd-ehk.3 Bloodstream — Materialized View Push — all tests passed"
