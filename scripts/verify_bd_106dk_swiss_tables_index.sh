#!/usr/bin/env bash
# CI verification gate for bd-106dk: Swiss Tables for index lookups (§7.7)
# Validates SwissIndex CRUD, probes, load factor tracking, and metrics.
set -euo pipefail

echo "=== bd-106dk: Swiss Tables Index Lookups Verification ==="

# Run the harness integration tests
cargo test --package fsqlite-harness --test bd_106dk_swiss_tables_index -- --nocapture 2>&1

echo ""
echo "[GATE PASS] bd-106dk Swiss Tables index lookups — all tests passed"
