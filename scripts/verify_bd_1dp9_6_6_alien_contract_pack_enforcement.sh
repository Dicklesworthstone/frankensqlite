#!/usr/bin/env bash
# CI verification gate for bd-1dp9.6.6: Alien Contract Pack — EV/risk/fallback enforcement
# Validates: verification contract enforcement types/classification, Bayesian score engine
# (BetaParams, PriorConfig), confidence gate system (GateDecision, GateConfig), ratchet
# policy (RatchetPolicy, RatchetState, waiver/quarantine lifecycle), impact graph
# (ImpactGraph, ValidationLane, ScenarioCategory, coverage computation), validation
# manifest contract types (GateOutcome, GateRecord, ReplayContract, gap types),
# enforcement disposition logic, conformance.
set -euo pipefail

echo "=== bd-1dp9.6.6: Alien Contract Pack — EV/Risk/Fallback Enforcement Verification ==="

# Run the harness integration tests
cargo test --package fsqlite-harness --test bd_1dp9_6_6_alien_contract_pack_enforcement -- --nocapture 2>&1

echo ""
echo "[GATE PASS] bd-1dp9.6.6 Alien Contract Pack — EV/Risk/Fallback Enforcement — all tests passed"
