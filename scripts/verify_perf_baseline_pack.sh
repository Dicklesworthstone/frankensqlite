#!/usr/bin/env bash
# verify_perf_baseline_pack.sh — deterministic perf baseline pack verification (bd-1dp9.6.1)
#
# Verifies:
# 1. Baseline benchmark pack artifacts (p50/p95/p99/throughput) are generated
# 2. Profiling artifact report and smoke report validate
# 3. Opportunity matrix promotes only score >= threshold candidates

set -euo pipefail

WORKSPACE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_ID="bd-1dp9.6.1-$(date -u +%Y%m%dT%H%M%SZ)-$$"
OUTPUT_DIR="$WORKSPACE_ROOT/artifacts/perf/bd-1dp9.6.1"
REPORT_JSON="$OUTPUT_DIR/report.json"
SUMMARY_MD="$OUTPUT_DIR/summary.md"
LOG_FILE="$OUTPUT_DIR/verify_perf_baseline_pack.log"

mkdir -p "$OUTPUT_DIR"

log_event() {
    local level="$1"
    local stage="$2"
    local message="$3"
    printf '{"run_id":"%s","bead_id":"bd-1dp9.6.1","level":"%s","stage":"%s","message":"%s"}\n' \
        "$RUN_ID" "$level" "$stage" "$message" >> "$LOG_FILE"
}

log_event "INFO" "start" "starting deterministic baseline pack verification"

set +e
cargo run -p fsqlite-harness --bin perf_baseline_pack_runner -- \
  --workspace-root "$WORKSPACE_ROOT" \
  --output-dir "$OUTPUT_DIR" \
  --output-json "$REPORT_JSON" \
  --output-human "$SUMMARY_MD" \
  > >(tee -a "$LOG_FILE") 2> >(tee -a "$LOG_FILE" >&2)
status=$?
set -e

if [[ $status -ne 0 ]]; then
    log_event "ERROR" "runner" "perf_baseline_pack_runner failed"
    exit $status
fi

log_event "INFO" "runner" "perf_baseline_pack_runner completed"

if ! jq -e '.bead_id == "bd-1dp9.6.1" and .overall_pass == true' "$REPORT_JSON" >/dev/null; then
    log_event "ERROR" "report" "report validation failed"
    echo "ERROR: invalid report payload at $REPORT_JSON" >&2
    exit 1
fi

OPPORTUNITY_JSON="$(jq -r '.opportunity_matrix_path' "$REPORT_JSON")"
if [[ ! -f "$OPPORTUNITY_JSON" ]]; then
    log_event "ERROR" "opportunity" "missing opportunity matrix artifact"
    echo "ERROR: missing opportunity matrix artifact: $OPPORTUNITY_JSON" >&2
    exit 1
fi

if ! jq -e '(.promoted | all(.selected == true and (.score >= .threshold)))' "$OPPORTUNITY_JSON" >/dev/null; then
    log_event "ERROR" "opportunity" "promoted set includes below-threshold or unselected rows"
    echo "ERROR: promoted opportunities must all satisfy selected=true and score>=threshold" >&2
    exit 1
fi

if ! jq -e '(.decisions | any(.selected == false))' "$OPPORTUNITY_JSON" >/dev/null; then
    log_event "ERROR" "opportunity" "no below-threshold candidates found; expected mixed matrix"
    echo "ERROR: expected at least one non-promoted candidate for gate realism" >&2
    exit 1
fi

PROMOTED_COUNT="$(jq -r '.promoted_count' "$REPORT_JSON")"
SCENARIO_COUNT="$(jq -r '.scenario_count' "$REPORT_JSON")"

log_event "INFO" "complete" "verification succeeded"
echo "RUN_ID:            $RUN_ID"
echo "Scenario count:    $SCENARIO_COUNT"
echo "Promoted count:    $PROMOTED_COUNT"
echo "Report:            $REPORT_JSON"
echo "Summary:           $SUMMARY_MD"
echo "Log:               $LOG_FILE"
