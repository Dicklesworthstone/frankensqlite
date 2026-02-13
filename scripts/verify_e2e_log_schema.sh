#!/usr/bin/env bash
# verify_e2e_log_schema.sh — E2E validation for unified log schema
# (bd-1dp9.7.2, bd-mblr.5.3, bd-mblr.5.3.1)
#
# Validates the E2E log schema and scenario coverage report:
# 1. Runs unit tests for the e2e_log_schema module
# 2. Verifies schema field specifications
# 3. Checks critical scenario coverage
# 4. Emits structured log output
#
# Usage: ./scripts/verify_e2e_log_schema.sh [--json]

set -euo pipefail

WORKSPACE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_ID="e2e-log-schema-$(date -u +%Y%m%dT%H%M%SZ)-$$"
JSON_OUTPUT=false
MODULE_FILE="$WORKSPACE_ROOT/crates/fsqlite-harness/src/e2e_log_schema.rs"
CONTRACT_DOC="$WORKSPACE_ROOT/docs/e2e_log_schema_contract.md"

if [[ "${1:-}" == "--json" ]]; then
    JSON_OUTPUT=true
fi

# Verify module file exists
if [[ ! -f "$MODULE_FILE" ]]; then
    echo "ERROR: $MODULE_FILE not found" >&2
    exit 1
fi
if [[ ! -f "$CONTRACT_DOC" ]]; then
    echo "ERROR: $CONTRACT_DOC not found" >&2
    exit 1
fi

# Compute artifact hash
MODULE_HASH=$(sha256sum "$MODULE_FILE" | awk '{print $1}')
DOC_HASH=$(sha256sum "$CONTRACT_DOC" | awk '{print $1}')

# Run unit tests
ERRORS=0
TEST_RESULT="unknown"
TEST_COUNT=0
if TEST_OUTPUT=$(cargo test -p fsqlite-harness --lib -- e2e_log_schema 2>&1); then
    TEST_RESULT="pass"
    TEST_COUNT=$(echo "$TEST_OUTPUT" | grep -oP '\d+ passed' | grep -oP '\d+' || echo 0)
else
    TEST_RESULT="fail"
    ERRORS=$((ERRORS + 1))
fi

# Count schema elements from source
FIELD_SPEC_COUNT=$(grep -Ec '^[[:space:]]*FieldSpec \{' "$MODULE_FILE" || echo 0)
CRITICAL_SCENARIO_COUNT=$(grep -c '"[A-Z]\+-[0-9]' "$MODULE_FILE" | head -1 || echo 0)
LOG_PHASE_VARIANTS=$(grep -c '^\s*[A-Z][a-z]*,' "$MODULE_FILE" | head -1 || echo 0)

# Validate contract document alignment
CONTRACT_STATUS="pass"
for expected in "Schema version: \`1.0.0\`" "\`run_id\`" "\`timestamp\`" "\`phase\`" "\`event_type\`"; do
    if ! grep -Fq "$expected" "$CONTRACT_DOC"; then
        CONTRACT_STATUS="fail"
        ERRORS=$((ERRORS + 1))
        break
    fi
done

# Output results
if $JSON_OUTPUT; then
    cat <<ENDJSON
{
  "run_id": "$RUN_ID",
  "phase": "e2e_log_schema_validation",
  "bead_id": "bd-1dp9.7.2",
  "related_bead_ids": ["bd-mblr.5.3", "bd-mblr.5.3.1"],
  "module_hash": "$MODULE_HASH",
  "contract_doc_hash": "$DOC_HASH",
  "unit_tests": {
    "result": "$TEST_RESULT",
    "count": $TEST_COUNT
  },
  "schema_stats": {
    "field_specs": $FIELD_SPEC_COUNT,
    "log_phase_variants": $LOG_PHASE_VARIANTS
  },
  "contract_doc": {
    "path": "docs/e2e_log_schema_contract.md",
    "status": "$CONTRACT_STATUS"
  },
  "validation_errors": $ERRORS,
  "result": "$([ $ERRORS -eq 0 ] && echo 'pass' || echo 'fail')"
}
ENDJSON
else
    echo "=== E2E Log Schema Validation ==="
    echo "Run ID:           $RUN_ID"
    echo "Module hash:      $MODULE_HASH"
    echo "Contract hash:    $DOC_HASH"
    echo ""
    echo "--- Unit Tests ---"
    echo "Result:           $TEST_RESULT"
    echo "Tests:            $TEST_COUNT"
    echo ""
    echo "--- Schema Stats ---"
    echo "Field specs:      $FIELD_SPEC_COUNT"
    echo "Phase variants:   $LOG_PHASE_VARIANTS"
    echo "Contract doc:     $CONTRACT_STATUS"
    echo ""
    echo "--- Validation ---"
    echo "Errors:           $ERRORS"
    echo "Result:           $([ $ERRORS -eq 0 ] && echo 'PASS' || echo 'FAIL')"
fi

exit $ERRORS
