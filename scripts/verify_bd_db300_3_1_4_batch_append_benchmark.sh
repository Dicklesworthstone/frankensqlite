#!/usr/bin/env bash
# verify_bd_db300_3_1_4_batch_append_benchmark.sh
#
# Runs the Track C / C1.4 batch-append commit benchmark through rch and emits
# artifact-grade evidence under artifacts/perf/bd-db300.3.1.4/.

set -euo pipefail

WORKSPACE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BEAD_ID="bd-db300.3.1.4"
PARENT_BEAD_ID="bd-db300.3.1"
RUN_ID="${BEAD_ID}-$(date -u +%Y%m%dT%H%M%SZ)-$$"
GENERATED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
OUTPUT_DIR="${OUTPUT_DIR:-${WORKSPACE_ROOT}/artifacts/perf/${BEAD_ID}}"
LOG_FILE="${OUTPUT_DIR}/events.jsonl"
RAW_OUTPUT="${OUTPUT_DIR}/raw_test_output.txt"
BENCHMARK_JSON="${OUTPUT_DIR}/benchmark.json"
REPORT_JSON="${OUTPUT_DIR}/report.json"
SUMMARY_MD="${OUTPUT_DIR}/summary.md"
GIT_SHA="$(git -C "${WORKSPACE_ROOT}" rev-parse HEAD)"
TEST_COMMAND="rch exec -- env CARGO_TERM_COLOR=never cargo test -p fsqlite-pager wal_commit_batch_benchmark_report -- --ignored --nocapture --test-threads=1"
REPLAY_COMMAND="bash scripts/verify_bd_db300_3_1_4_batch_append_benchmark.sh"

mkdir -p "${OUTPUT_DIR}"
: > "${LOG_FILE}"

log_event() {
    local level="$1"
    local stage="$2"
    local message="$3"
    printf '{"run_id":"%s","bead_id":"%s","level":"%s","stage":"%s","message":"%s","ts":"%s"}\n' \
        "${RUN_ID}" "${BEAD_ID}" "${level}" "${stage}" "${message}" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
        >> "${LOG_FILE}"
}

log_event "INFO" "start" "starting Track C batch-append benchmark evidence pass"

if ! eval "${TEST_COMMAND}" 2>&1 | tee "${RAW_OUTPUT}"; then
    log_event "ERROR" "benchmark" "rch-offloaded pager benchmark test failed"
    echo "ERROR: benchmark command failed: ${TEST_COMMAND}" >&2
    exit 1
fi

awk '
    /BEGIN_BD_DB300_3_1_4_REPORT/ {capture=1; next}
    /END_BD_DB300_3_1_4_REPORT/ {capture=0}
    capture {print}
' "${RAW_OUTPUT}" > "${BENCHMARK_JSON}"

if [[ ! -s "${BENCHMARK_JSON}" ]]; then
    log_event "ERROR" "artifact" "missing extracted benchmark JSON payload"
    echo "ERROR: failed to extract benchmark JSON from ${RAW_OUTPUT}" >&2
    exit 1
fi

jq -e '
    .schema_version == "fsqlite.track_c.batch_commit_benchmark.v1"
    and .bead_id == "bd-db300.3.1.4"
    and (.cases | length) >= 1
' "${BENCHMARK_JSON}" >/dev/null

ALL_MEDIAN_FASTER="$(jq -r '[.cases[] | (.batch_append.median_ns < .single_frame.median_ns)] | all' "${BENCHMARK_JSON}")"
ALL_MEAN_FASTER="$(jq -r '[.cases[] | (.batch_append.mean_ns < .single_frame.mean_ns)] | all' "${BENCHMARK_JSON}")"

jq -n \
    --arg schema_version "fsqlite.perf.batch-append-report.v1" \
    --arg bead_id "${BEAD_ID}" \
    --arg parent_bead_id "${PARENT_BEAD_ID}" \
    --arg run_id "${RUN_ID}" \
    --arg generated_at "${GENERATED_AT}" \
    --arg git_sha "${GIT_SHA}" \
    --arg replay_command "${REPLAY_COMMAND}" \
    --arg benchmark_command "${TEST_COMMAND}" \
    --arg raw_output "${RAW_OUTPUT}" \
    --arg benchmark_json "${BENCHMARK_JSON}" \
    --arg summary_md "${SUMMARY_MD}" \
    --arg report_json "${REPORT_JSON}" \
    --slurpfile benchmark "${BENCHMARK_JSON}" \
    --argjson all_median_faster "${ALL_MEDIAN_FASTER}" \
    --argjson all_mean_faster "${ALL_MEAN_FASTER}" \
    '
    {
        schema_version: $schema_version,
        bead_id: $bead_id,
        parent_bead_id: $parent_bead_id,
        run_id: $run_id,
        generated_at: $generated_at,
        git_sha: $git_sha,
        replay_command: $replay_command,
        benchmark_command: $benchmark_command,
        acceptance: {
            batch_faster_in_all_cases_by_median: $all_median_faster,
            batch_faster_in_all_cases_by_mean: $all_mean_faster
        },
        artifacts: {
            raw_output: $raw_output,
            benchmark_json: $benchmark_json,
            summary_md: $summary_md,
            report_json: $report_json
        },
        benchmark: $benchmark[0]
    }
    ' > "${REPORT_JSON}"

{
    echo "# ${BEAD_ID} Batch Append Benchmark Summary"
    echo
    echo "- run_id: \`${RUN_ID}\`"
    echo "- git_sha: \`${GIT_SHA}\`"
    echo "- replay_command: \`${REPLAY_COMMAND}\`"
    echo "- benchmark_command: \`${TEST_COMMAND}\`"
    echo "- all_cases_batch_faster_by_median: \`${ALL_MEDIAN_FASTER}\`"
    echo "- all_cases_batch_faster_by_mean: \`${ALL_MEAN_FASTER}\`"
    echo
    echo "| Scenario | Dirty Pages | Single Median (ns) | Batch Median (ns) | Median Speedup | Mean Speedup | Faster By Median |"
    echo "| --- | ---: | ---: | ---: | ---: | ---: | --- |"
    jq -r '
        .benchmark.cases[]
        | "| \(.scenario_id) | \(.dirty_pages) | \(.single_frame.median_ns) | \(.batch_append.median_ns) | \(.speedup_vs_single_median | tostring) | \(.speedup_vs_single_mean | tostring) | \(.faster_variant_by_median) |"
    ' "${REPORT_JSON}"
    echo
    echo "Artifacts:"
    echo "- raw_output: \`${RAW_OUTPUT}\`"
    echo "- benchmark_json: \`${BENCHMARK_JSON}\`"
    echo "- report_json: \`${REPORT_JSON}\`"
} > "${SUMMARY_MD}"

log_event "INFO" "complete" "Track C batch-append benchmark evidence completed"
echo "Wrote Track C batch-append artifacts to ${OUTPUT_DIR}"
