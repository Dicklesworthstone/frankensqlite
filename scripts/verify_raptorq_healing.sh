#!/usr/bin/env bash
# verify_raptorq_healing.sh — verification for bd-t6sv2.3
#
# Usage:
#   bash scripts/verify_raptorq_healing.sh [--json]
set -euo pipefail

BEAD_ID="bd-t6sv2.3"
SCHEMA_VERSION="1"
RUN_ID="${BEAD_ID}-$(date -u +%Y%m%dT%H%M%SZ)-$$"
REPORT_DIR="test-results"
REPORT_FILE="${REPORT_DIR}/${BEAD_ID}-raptorq-healing-verify.json"
JSON_MODE=false

for arg in "$@"; do
  case "$arg" in
    --json) JSON_MODE=true ;;
  esac
done

mkdir -p "$REPORT_DIR"

parse_passed() {
  echo "$1" | grep 'test result:' | grep -oP '\d+ passed' | grep -oP '\d+' | tail -1 || echo "0"
}

parse_failed() {
  echo "$1" | grep 'test result:' | grep -oP '\d+ failed' | grep -oP '\d+' | tail -1 || echo "0"
}

declare -A CASE_STATUS
declare -A CASE_PASSED
declare -A CASE_FAILED

run_case() {
  local case_id="$1"
  shift
  echo "phase=${case_id} bead_id=${BEAD_ID} run_id=${RUN_ID}"
  local output
  output=$("$@" 2>&1) || true
  local exit_code=$?
  local passed
  local failed
  passed=$(parse_passed "$output")
  failed=$(parse_failed "$output")
  CASE_PASSED["$case_id"]="$passed"
  CASE_FAILED["$case_id"]="$failed"
  if [ "$exit_code" -eq 0 ] && [ "$failed" -eq 0 ]; then
    CASE_STATUS["$case_id"]="pass"
  else
    CASE_STATUS["$case_id"]="fail"
  fi
}

# Required corruption classes: 1, R/2, R, R+1
run_case \
  "single_symbol_loss" \
  cargo test -p fsqlite-wal --test wal_fec_recovery test_raptorq_bitflip_repair -- --nocapture
run_case \
  "half_budget_loss" \
  cargo test -p fsqlite-wal --test wal_fec_recovery test_raptorq_symbol_loss_half_R -- --nocapture
run_case \
  "full_budget_loss" \
  cargo test -p fsqlite-wal --test wal_fec_recovery test_raptorq_symbol_loss_within_R -- --nocapture
run_case \
  "over_budget_loss" \
  cargo test -p fsqlite-wal --test wal_fec_recovery test_raptorq_symbol_loss_beyond_R -- --nocapture

# Telemetry + PRAGMA coverage
run_case \
  "telemetry_unit_suite" \
  cargo test -p fsqlite-wal --lib test_raptorq_telemetry_ -- --nocapture
run_case \
  "pragma_surface_suite" \
  cargo test -p fsqlite-core --lib test_pragma_raptorq_stats_events_and_reset -- --nocapture

TOTAL_PASSED=0
TOTAL_FAILED=0
VERDICT="pass"
for case_id in "${!CASE_STATUS[@]}"; do
  TOTAL_PASSED=$((TOTAL_PASSED + CASE_PASSED["$case_id"]))
  TOTAL_FAILED=$((TOTAL_FAILED + CASE_FAILED["$case_id"]))
  if [ "${CASE_STATUS["$case_id"]}" != "pass" ]; then
    VERDICT="fail"
  fi
done

TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
REPORT_CONTENT=$(cat <<ENDJSON
{
  "schema_version": ${SCHEMA_VERSION},
  "bead_id": "${BEAD_ID}",
  "run_id": "${RUN_ID}",
  "timestamp": "${TIMESTAMP}",
  "verdict": "${VERDICT}",
  "cases": {
    "single_symbol_loss": { "status": "${CASE_STATUS["single_symbol_loss"]}", "passed": ${CASE_PASSED["single_symbol_loss"]}, "failed": ${CASE_FAILED["single_symbol_loss"]} },
    "half_budget_loss": { "status": "${CASE_STATUS["half_budget_loss"]}", "passed": ${CASE_PASSED["half_budget_loss"]}, "failed": ${CASE_FAILED["half_budget_loss"]} },
    "full_budget_loss": { "status": "${CASE_STATUS["full_budget_loss"]}", "passed": ${CASE_PASSED["full_budget_loss"]}, "failed": ${CASE_FAILED["full_budget_loss"]} },
    "over_budget_loss": { "status": "${CASE_STATUS["over_budget_loss"]}", "passed": ${CASE_PASSED["over_budget_loss"]}, "failed": ${CASE_FAILED["over_budget_loss"]} }
  },
  "telemetry_contract": {
    "telemetry_unit_suite": { "status": "${CASE_STATUS["telemetry_unit_suite"]}", "passed": ${CASE_PASSED["telemetry_unit_suite"]}, "failed": ${CASE_FAILED["telemetry_unit_suite"]} },
    "pragma_surface_suite": { "status": "${CASE_STATUS["pragma_surface_suite"]}", "passed": ${CASE_PASSED["pragma_surface_suite"]}, "failed": ${CASE_FAILED["pragma_surface_suite"]} }
  },
  "totals": {
    "passed": ${TOTAL_PASSED},
    "failed": ${TOTAL_FAILED}
  }
}
ENDJSON
)

echo "$REPORT_CONTENT" > "$REPORT_FILE"
REPORT_SHA=$(sha256sum "$REPORT_FILE" | cut -d' ' -f1)

if [ "$JSON_MODE" = true ]; then
  echo "$REPORT_CONTENT"
else
  echo "phase=complete bead_id=${BEAD_ID} run_id=${RUN_ID} verdict=${VERDICT}"
  echo "  single_symbol_loss: ${CASE_STATUS["single_symbol_loss"]}"
  echo "  half_budget_loss: ${CASE_STATUS["half_budget_loss"]}"
  echo "  full_budget_loss: ${CASE_STATUS["full_budget_loss"]}"
  echo "  over_budget_loss: ${CASE_STATUS["over_budget_loss"]}"
  echo "  telemetry_unit_suite: ${CASE_STATUS["telemetry_unit_suite"]}"
  echo "  pragma_surface_suite: ${CASE_STATUS["pragma_surface_suite"]}"
  echo "  report_path=${REPORT_FILE}"
  echo "  report_sha256=${REPORT_SHA}"
fi

if [ "$VERDICT" = "pass" ]; then
  exit 0
fi
exit 1
