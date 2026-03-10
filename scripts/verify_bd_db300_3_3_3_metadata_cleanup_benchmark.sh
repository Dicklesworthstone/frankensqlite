#!/usr/bin/env bash
# verify_bd_db300_3_3_3_metadata_cleanup_benchmark.sh
#
# Runs the Track C / C3.3 metadata-cleanup benchmark through rch and emits
# artifact-grade evidence under artifacts/perf/bd-db300.3.3.3/.

set -euo pipefail

WORKSPACE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BEAD_ID="bd-db300.3.3.3"
PARENT_BEAD_ID="bd-db300.3.3"
RUN_ID="${BEAD_ID}-$(date -u +%Y%m%dT%H%M%SZ)-$$"
GENERATED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
OUTPUT_DIR="${OUTPUT_DIR:-${WORKSPACE_ROOT}/artifacts/perf/${BEAD_ID}}"
LOG_FILE="${OUTPUT_DIR}/events.jsonl"
RAW_OUTPUT="${OUTPUT_DIR}/raw_test_output.txt"
BENCHMARK_JSON="${OUTPUT_DIR}/benchmark.json"
REPORT_JSON="${OUTPUT_DIR}/report.json"
SUMMARY_MD="${OUTPUT_DIR}/summary.md"
GIT_SHA="$(git -C "${WORKSPACE_ROOT}" rev-parse HEAD)"
TEST_COMMAND="rch exec -- env CARGO_TERM_COLOR=never cargo test -p fsqlite-pager wal_metadata_cleanup_benchmark_report -- --ignored --nocapture --test-threads=1"
REPLAY_COMMAND="bash scripts/verify_bd_db300_3_3_3_metadata_cleanup_benchmark.sh"

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

log_event "INFO" "start" "starting Track C metadata-cleanup benchmark evidence pass"

if ! eval "${TEST_COMMAND}" 2>&1 | tee "${RAW_OUTPUT}"; then
    log_event "ERROR" "benchmark" "rch-offloaded pager metadata benchmark test failed"
    echo "ERROR: benchmark command failed: ${TEST_COMMAND}" >&2
    exit 1
fi

awk '
    /BEGIN_BD_DB300_3_3_3_REPORT/ {capture=1; next}
    /END_BD_DB300_3_3_3_REPORT/ {capture=0}
    capture {print}
' "${RAW_OUTPUT}" > "${BENCHMARK_JSON}"

if [[ ! -s "${BENCHMARK_JSON}" ]]; then
    log_event "ERROR" "artifact" "missing extracted metadata benchmark JSON payload"
    echo "ERROR: failed to extract benchmark JSON from ${RAW_OUTPUT}" >&2
    exit 1
fi

jq -e '
    .schema_version == "fsqlite.track_c.metadata_cleanup_benchmark.v1"
    and .bead_id == "bd-db300.3.3.3"
    and (.cases | length) >= 1
' "${BENCHMARK_JSON}" >/dev/null

ALL_BASELINE_TOUCH_PAGE_ONE="$(jq -r '[.cases[] | (.baseline_page_one_frames_per_commit > 0)] | all' "${BENCHMARK_JSON}")"
ALL_CANDIDATE_AVOIDS_PAGE_ONE="$(jq -r '[.cases[] | (.candidate_page_one_frames_per_commit == 0)] | all' "${BENCHMARK_JSON}")"
ALL_FRAME_COUNT_REDUCED="$(jq -r '[.cases[] | (.candidate_total_frames_per_commit < .baseline_total_frames_per_commit)] | all' "${BENCHMARK_JSON}")"
ANY_MEDIAN_FASTER="$(jq -r '[.cases[] | (.semantic_cleanup_candidate.median_ns < .forced_page_one_baseline.median_ns)] | any' "${BENCHMARK_JSON}")"
ALL_MEDIAN_FASTER="$(jq -r '[.cases[] | (.semantic_cleanup_candidate.median_ns < .forced_page_one_baseline.median_ns)] | all' "${BENCHMARK_JSON}")"
ANY_MEAN_FASTER="$(jq -r '[.cases[] | (.semantic_cleanup_candidate.mean_ns < .forced_page_one_baseline.mean_ns)] | any' "${BENCHMARK_JSON}")"
ALL_MEAN_FASTER="$(jq -r '[.cases[] | (.semantic_cleanup_candidate.mean_ns < .forced_page_one_baseline.mean_ns)] | all' "${BENCHMARK_JSON}")"

if [[ "${ALL_BASELINE_TOUCH_PAGE_ONE}" != "true" ]]; then
    log_event "ERROR" "acceptance" "baseline did not force page-1 exposure in every case"
    echo "ERROR: baseline did not force page-1 exposure in every case" >&2
    exit 1
fi

if [[ "${ALL_CANDIDATE_AVOIDS_PAGE_ONE}" != "true" ]]; then
    log_event "ERROR" "acceptance" "candidate still emitted page-1 frames in an interior-only case"
    echo "ERROR: candidate still emitted page-1 frames in an interior-only case" >&2
    exit 1
fi

if [[ "${ALL_FRAME_COUNT_REDUCED}" != "true" ]]; then
    log_event "ERROR" "acceptance" "candidate did not reduce commit frame count in every case"
    echo "ERROR: candidate did not reduce commit frame count in every case" >&2
    exit 1
fi

jq -n \
    --arg schema_version "fsqlite.perf.metadata-cleanup-report.v1" \
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
    --argjson all_baseline_touch_page_one "${ALL_BASELINE_TOUCH_PAGE_ONE}" \
    --argjson all_candidate_avoids_page_one "${ALL_CANDIDATE_AVOIDS_PAGE_ONE}" \
    --argjson all_frame_count_reduced "${ALL_FRAME_COUNT_REDUCED}" \
    --argjson any_median_faster "${ANY_MEDIAN_FASTER}" \
    --argjson all_median_faster "${ALL_MEDIAN_FASTER}" \
    --argjson any_mean_faster "${ANY_MEAN_FASTER}" \
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
            all_baseline_touch_page_one: $all_baseline_touch_page_one,
            all_candidate_avoids_page_one: $all_candidate_avoids_page_one,
            all_frame_count_reduced: $all_frame_count_reduced,
            any_case_faster_by_median: $any_median_faster,
            all_cases_faster_by_median: $all_median_faster,
            any_case_faster_by_mean: $any_mean_faster,
            all_cases_faster_by_mean: $all_mean_faster
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
    echo "# ${BEAD_ID} Metadata Cleanup Benchmark Summary"
    echo
    echo "- run_id: \`${RUN_ID}\`"
    echo "- git_sha: \`${GIT_SHA}\`"
    echo "- replay_command: \`${REPLAY_COMMAND}\`"
    echo "- benchmark_command: \`${TEST_COMMAND}\`"
    echo "- all_baseline_touch_page_one: \`${ALL_BASELINE_TOUCH_PAGE_ONE}\`"
    echo "- all_candidate_avoids_page_one: \`${ALL_CANDIDATE_AVOIDS_PAGE_ONE}\`"
    echo "- all_frame_count_reduced: \`${ALL_FRAME_COUNT_REDUCED}\`"
    echo "- any_case_faster_by_median: \`${ANY_MEDIAN_FASTER}\`"
    echo "- any_case_faster_by_mean: \`${ANY_MEAN_FASTER}\`"
    echo
    echo "| Scenario | Interior Dirty Pages | Baseline Median (ns) | Candidate Median (ns) | Baseline Frames | Candidate Frames | Baseline Page-1 Frames | Candidate Page-1 Frames | Median Speedup |"
    echo "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"
    jq -r '
        .benchmark.cases[]
        | "| \(.scenario_id) | \(.interior_dirty_pages) | \(.forced_page_one_baseline.median_ns) | \(.semantic_cleanup_candidate.median_ns) | \(.baseline_total_frames_per_commit) | \(.candidate_total_frames_per_commit) | \(.baseline_page_one_frames_per_commit) | \(.candidate_page_one_frames_per_commit) | \(.speedup_vs_baseline_median | tostring) |"
    ' "${REPORT_JSON}"
    echo
    echo "Artifacts:"
    echo "- raw_output: \`${RAW_OUTPUT}\`"
    echo "- benchmark_json: \`${BENCHMARK_JSON}\`"
    echo "- report_json: \`${REPORT_JSON}\`"
} > "${SUMMARY_MD}"

log_event "INFO" "complete" "Track C metadata-cleanup benchmark evidence completed"
echo "Wrote Track C metadata-cleanup artifacts to ${OUTPUT_DIR}"
