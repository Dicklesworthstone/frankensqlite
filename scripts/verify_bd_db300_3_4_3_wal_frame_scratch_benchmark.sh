#!/usr/bin/env bash
# verify_bd_db300_3_4_3_wal_frame_scratch_benchmark.sh
#
# Runs the Track C / C4.3 WAL frame scratch benchmark through rch and emits
# artifact-grade evidence under artifacts/perf/bd-db300.3.4.3/.

set -euo pipefail

WORKSPACE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WAL_CRATE_DIR="${WORKSPACE_ROOT}/crates/fsqlite-wal"
BEAD_ID="bd-db300.3.4.3"
PARENT_BEAD_ID="bd-db300.3.4"
RUN_ID="${BEAD_ID}-$(date -u +%Y%m%dT%H%M%SZ)-$$"
GENERATED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
OUTPUT_DIR="${OUTPUT_DIR:-${WORKSPACE_ROOT}/artifacts/perf/${BEAD_ID}}"
LOG_FILE="${OUTPUT_DIR}/events.jsonl"
RAW_OUTPUT="${OUTPUT_DIR}/raw_test_output.txt"
BENCHMARK_JSON="${OUTPUT_DIR}/benchmark.json"
REPORT_JSON="${OUTPUT_DIR}/report.json"
SUMMARY_MD="${OUTPUT_DIR}/summary.md"
GIT_SHA="$(git -C "${WORKSPACE_ROOT}" rev-parse HEAD)"
TEST_COMMAND="cd \"${WAL_CRATE_DIR}\" && rch exec -- env CARGO_TERM_COLOR=never cargo test --lib wal_frame_scratch_benchmark_report -- --ignored --nocapture --test-threads=1"
REPLAY_COMMAND="bash scripts/verify_bd_db300_3_4_3_wal_frame_scratch_benchmark.sh"

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

log_event "INFO" "start" "starting Track C WAL frame scratch benchmark evidence pass"

if ! eval "${TEST_COMMAND}" 2>&1 | tee "${RAW_OUTPUT}"; then
    log_event "ERROR" "benchmark" "rch-offloaded WAL scratch benchmark test failed"
    echo "ERROR: benchmark command failed: ${TEST_COMMAND}" >&2
    exit 1
fi

awk '
    /BEGIN_BD_DB300_3_4_3_REPORT/ {capture=1; next}
    /END_BD_DB300_3_4_3_REPORT/ {capture=0}
    capture {print}
' "${RAW_OUTPUT}" > "${BENCHMARK_JSON}"

if [[ ! -s "${BENCHMARK_JSON}" ]]; then
    log_event "ERROR" "artifact" "missing extracted WAL scratch benchmark JSON payload"
    echo "ERROR: failed to extract benchmark JSON from ${RAW_OUTPUT}" >&2
    exit 1
fi

jq -e '
    .schema_version == "fsqlite.track_c.wal_scratch_benchmark.v1"
    and .bead_id == "bd-db300.3.4.3"
    and (.cases | length) >= 1
' "${BENCHMARK_JSON}" >/dev/null

ALL_CASES_REDUCE_ALLOCATIONS="$(jq -r '[.cases[] | (.fresh_buffer_allocations_avoided_per_sample > 0)] | all' "${BENCHMARK_JSON}")"
ALL_CASES_REDUCE_BUFFER_BYTES="$(jq -r '[.cases[] | (.buffer_bytes_saved_vs_fresh_requested_per_sample > 0)] | all' "${BENCHMARK_JSON}")"
ALL_CASES_FASTER_BY_MEDIAN="$(jq -r '[.cases[] | (.faster_variant_by_median == "scratch_reuse_candidate")] | all' "${BENCHMARK_JSON}")"
ANY_CASE_FASTER_BY_MEDIAN="$(jq -r '[.cases[] | (.faster_variant_by_median == "scratch_reuse_candidate")] | any' "${BENCHMARK_JSON}")"
ALL_CASES_LIMIT_GROWTH_TO_ONE="$(jq -r '[.cases[] | (.scratch_reuse_candidate.scratch_capacity_growth_events_per_sample <= 1)] | all' "${BENCHMARK_JSON}")"

if [[ "${ALL_CASES_REDUCE_ALLOCATIONS}" != "true" ]]; then
    log_event "ERROR" "acceptance" "scratch reuse did not reduce buffer allocation events in every case"
    echo "ERROR: scratch reuse did not reduce buffer allocation events in every case" >&2
    exit 1
fi

if [[ "${ALL_CASES_REDUCE_BUFFER_BYTES}" != "true" ]]; then
    log_event "ERROR" "acceptance" "scratch reuse did not reduce fresh buffer byte churn in every case"
    echo "ERROR: scratch reuse did not reduce fresh buffer byte churn in every case" >&2
    exit 1
fi

if [[ "${ALL_CASES_LIMIT_GROWTH_TO_ONE}" != "true" ]]; then
    log_event "ERROR" "acceptance" "scratch reuse grew capacity more than once in a benchmark case"
    echo "ERROR: scratch reuse grew capacity more than once in a benchmark case" >&2
    exit 1
fi

jq -n \
    --arg schema_version "fsqlite.perf.wal-frame-scratch-report.v1" \
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
    --argjson all_cases_reduce_allocations "${ALL_CASES_REDUCE_ALLOCATIONS}" \
    --argjson all_cases_reduce_buffer_bytes "${ALL_CASES_REDUCE_BUFFER_BYTES}" \
    --argjson all_cases_faster_by_median "${ALL_CASES_FASTER_BY_MEDIAN}" \
    --argjson any_case_faster_by_median "${ANY_CASE_FASTER_BY_MEDIAN}" \
    --argjson all_cases_limit_growth_to_one "${ALL_CASES_LIMIT_GROWTH_TO_ONE}" \
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
            all_cases_reduce_allocations: $all_cases_reduce_allocations,
            all_cases_reduce_buffer_bytes: $all_cases_reduce_buffer_bytes,
            all_cases_limit_growth_to_one: $all_cases_limit_growth_to_one,
            any_case_faster_by_median: $any_case_faster_by_median,
            all_cases_faster_by_median: $all_cases_faster_by_median
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
    echo "# ${BEAD_ID} WAL Frame Scratch Benchmark Summary"
    echo
    echo "- run_id: \`${RUN_ID}\`"
    echo "- git_sha: \`${GIT_SHA}\`"
    echo "- replay_command: \`${REPLAY_COMMAND}\`"
    echo "- benchmark_command: \`${TEST_COMMAND}\`"
    echo "- all_cases_reduce_allocations: \`${ALL_CASES_REDUCE_ALLOCATIONS}\`"
    echo "- all_cases_reduce_buffer_bytes: \`${ALL_CASES_REDUCE_BUFFER_BYTES}\`"
    echo "- all_cases_limit_growth_to_one: \`${ALL_CASES_LIMIT_GROWTH_TO_ONE}\`"
    echo "- any_case_faster_by_median: \`${ANY_CASE_FASTER_BY_MEDIAN}\`"
    echo "- all_cases_faster_by_median: \`${ALL_CASES_FASTER_BY_MEDIAN}\`"
    echo
    echo "| Scenario | Frames/Op | Ops/Sample | Baseline Median (ns) | Candidate Median (ns) | Baseline Fresh Allocs | Candidate Capacity Growths | Baseline Fresh Bytes | Candidate Peak Scratch Bytes | Median Speedup |"
    echo "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"
    jq -r '
        .benchmark.cases[]
        | "| \(.scenario_id) | \(.frames_per_operation) | \(.operations_per_sample) | \(.fresh_alloc_baseline.median_ns) | \(.scratch_reuse_candidate.median_ns) | \(.fresh_alloc_baseline.explicit_fresh_buffer_allocations_per_sample) | \(.scratch_reuse_candidate.scratch_capacity_growth_events_per_sample) | \(.fresh_alloc_baseline.frame_buffer_bytes_requested_per_sample) | \(.scratch_reuse_candidate.peak_scratch_capacity_bytes) | \(.speedup_vs_baseline_median | tostring) |"
    ' "${REPORT_JSON}"
    echo
    echo "Artifacts:"
    echo "- raw_output: \`${RAW_OUTPUT}\`"
    echo "- benchmark_json: \`${BENCHMARK_JSON}\`"
    echo "- report_json: \`${REPORT_JSON}\`"
} > "${SUMMARY_MD}"

log_event "INFO" "complete" "Track C WAL frame scratch benchmark evidence completed"
echo "Wrote Track C WAL frame scratch artifacts to ${OUTPUT_DIR}"
