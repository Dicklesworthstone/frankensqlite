#!/usr/bin/env bash
# verify_bd_db300_3_2_3_publish_window_benchmark.sh
#
# Runs the Track C / C2.3 synthetic WAL critical section benchmark through rch and emits
# artifact-grade evidence under artifacts/perf/bd-db300.3.2.3/.

set -euo pipefail

WORKSPACE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BEAD_ID="bd-db300.3.2.3"
PARENT_BEAD_ID="bd-db300.3.2"
RUN_ID="${BEAD_ID}-$(date -u +%Y%m%dT%H%M%SZ)-$$"
GENERATED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
OUTPUT_DIR="${OUTPUT_DIR:-${WORKSPACE_ROOT}/artifacts/perf/${BEAD_ID}}"
LOG_FILE="${OUTPUT_DIR}/events.jsonl"
RAW_OUTPUT="${OUTPUT_DIR}/raw_test_output.txt"
BENCHMARK_JSON="${OUTPUT_DIR}/benchmark.json"
REPORT_JSON="${OUTPUT_DIR}/report.json"
SUMMARY_MD="${OUTPUT_DIR}/summary.md"
GIT_SHA="$(git -C "${WORKSPACE_ROOT}" rev-parse HEAD)"
TEST_COMMAND="rch exec -- env CARGO_TERM_COLOR=never cargo test -p fsqlite-pager wal_publish_window_shrink_benchmark_report -- --ignored --nocapture --test-threads=1"
REPLAY_COMMAND="bash scripts/verify_bd_db300_3_2_3_publish_window_benchmark.sh"

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

log_event "INFO" "start" "starting Track C synthetic WAL critical section benchmark evidence pass"

if ! eval "${TEST_COMMAND}" 2>&1 | tee "${RAW_OUTPUT}"; then
    log_event "ERROR" "benchmark" "rch-offloaded pager synthetic WAL critical section benchmark test failed"
    echo "ERROR: benchmark command failed: ${TEST_COMMAND}" >&2
    exit 1
fi

awk '
    /BEGIN_BD_DB300_3_2_3_REPORT/ {capture=1; next}
    /END_BD_DB300_3_2_3_REPORT/ {capture=0}
    capture {print}
' "${RAW_OUTPUT}" > "${BENCHMARK_JSON}"

if [[ ! -s "${BENCHMARK_JSON}" ]]; then
    log_event "ERROR" "artifact" "missing extracted synthetic WAL critical section benchmark JSON payload"
    echo "ERROR: failed to extract benchmark JSON from ${RAW_OUTPUT}" >&2
    exit 1
fi

if ! jq -e '
    .schema_version == "fsqlite.track_c.publish_window_benchmark.v2"
    and .bead_id == "bd-db300.3.2.3"
    and .measured_operation == "synthetic_wal_append_critical_section"
    and (.cases | type == "array" and length >= 1)
    and all(.cases[];
        all([
            .synthetic_wal_lock_hold_baseline,
            .synthetic_wal_lock_hold_candidate,
            .synthetic_wal_lock_contended_wait_baseline,
            .synthetic_wal_lock_contended_wait_candidate
        ][]; all([.median_ns, .p95_ns][]; type == "number" and . >= 0))
    )
' "${BENCHMARK_JSON}" >/dev/null; then
    log_event "ERROR" "artifact" "invalid v2 synthetic WAL critical section benchmark schema or timing fields"
    echo "ERROR: invalid v2 synthetic WAL critical section benchmark schema or timing fields" >&2
    exit 1
fi

if ! jq -e '
    all(.cases[];
        all([
            .synthetic_wal_lock_contended_wait_baseline,
            .synthetic_wal_lock_contended_wait_candidate
        ][]; .median_ns > 0 and .p95_ns > 0)
    )
' "${BENCHMARK_JSON}" >/dev/null; then
    log_event "ERROR" "acceptance" "synthetic WAL critical section wait improvement is unmeasured: baseline or candidate contended wait is zero"
    echo "ERROR: synthetic WAL critical section wait improvement is unmeasured: baseline or candidate contended wait is zero; acceptance remains unmet" >&2
    exit 1
fi

ALL_HOLD_MEDIAN_SHRUNK="$(jq -r '[.cases[] | (.synthetic_wal_lock_hold_candidate.median_ns < .synthetic_wal_lock_hold_baseline.median_ns)] | all' "${BENCHMARK_JSON}")"
ALL_WAIT_MEDIAN_SHRUNK="$(jq -r '[.cases[] | (.synthetic_wal_lock_contended_wait_candidate.median_ns < .synthetic_wal_lock_contended_wait_baseline.median_ns)] | all' "${BENCHMARK_JSON}")"
ALL_HOLD_P95_SHRUNK="$(jq -r '[.cases[] | (.synthetic_wal_lock_hold_candidate.p95_ns <= .synthetic_wal_lock_hold_baseline.p95_ns)] | all' "${BENCHMARK_JSON}")"
ALL_WAIT_P95_SHRUNK="$(jq -r '[.cases[] | (.synthetic_wal_lock_contended_wait_candidate.p95_ns <= .synthetic_wal_lock_contended_wait_baseline.p95_ns)] | all' "${BENCHMARK_JSON}")"

if [[ "${ALL_HOLD_MEDIAN_SHRUNK}" != "true" ]]; then
    log_event "ERROR" "acceptance" "candidate did not shrink synthetic WAL critical section hold median in every case"
    echo "ERROR: candidate did not shrink synthetic WAL critical section hold median in every case" >&2
    exit 1
fi

if [[ "${ALL_WAIT_MEDIAN_SHRUNK}" != "true" ]]; then
    log_event "ERROR" "acceptance" "candidate did not shrink synthetic WAL critical section contended wait median in every case"
    echo "ERROR: candidate did not shrink synthetic WAL critical section contended wait median in every case" >&2
    exit 1
fi

jq -n \
    --arg schema_version "fsqlite.perf.publish-window-shrink-report.v2" \
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
    --argjson all_hold_median_shrunk "${ALL_HOLD_MEDIAN_SHRUNK}" \
    --argjson all_wait_median_shrunk "${ALL_WAIT_MEDIAN_SHRUNK}" \
    --argjson all_hold_p95_shrunk "${ALL_HOLD_P95_SHRUNK}" \
    --argjson all_wait_p95_shrunk "${ALL_WAIT_P95_SHRUNK}" \
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
        measured_operation: $benchmark[0].measured_operation,
        acceptance: {
            synthetic_wal_lock_contended_wait_measured_in_all_cases: true,
            candidate_shrinks_synthetic_wal_lock_hold_median_in_all_cases: $all_hold_median_shrunk,
            candidate_shrinks_synthetic_wal_lock_contended_wait_median_in_all_cases: $all_wait_median_shrunk,
            candidate_shrinks_or_matches_synthetic_wal_lock_hold_p95_in_all_cases: $all_hold_p95_shrunk,
            candidate_shrinks_or_matches_synthetic_wal_lock_contended_wait_p95_in_all_cases: $all_wait_p95_shrunk
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
    echo "# ${BEAD_ID} Synthetic WAL Critical Section Benchmark Summary"
    echo
    echo "- run_id: \`${RUN_ID}\`"
    echo "- git_sha: \`${GIT_SHA}\`"
    echo "- replay_command: \`${REPLAY_COMMAND}\`"
    echo "- benchmark_command: \`${TEST_COMMAND}\`"
    echo "- candidate_shrinks_synthetic_wal_lock_hold_median_in_all_cases: \`${ALL_HOLD_MEDIAN_SHRUNK}\`"
    echo "- candidate_shrinks_synthetic_wal_lock_contended_wait_median_in_all_cases: \`${ALL_WAIT_MEDIAN_SHRUNK}\`"
    echo "- candidate_shrinks_or_matches_synthetic_wal_lock_hold_p95_in_all_cases: \`${ALL_HOLD_P95_SHRUNK}\`"
    echo "- candidate_shrinks_or_matches_synthetic_wal_lock_contended_wait_p95_in_all_cases: \`${ALL_WAIT_P95_SHRUNK}\`"
    echo
    echo 'The benchmark uses `blocking_memory_vfs` in `crates/fsqlite-pager/src/pager.rs` to measure a synthetic WAL critical section in-process. Hold time covers the synthetic WAL lock; contended wait counts only time blocked by another owner, with uncontended acquisitions contributing zero. OS and main-file lock windows are unmeasured.'
    echo
    echo "| Scenario | Dirty Pages | Synthetic WAL Hold Baseline Median (ns) | Synthetic WAL Hold Candidate Median (ns) | Hold Reduction | Synthetic WAL Wait Baseline Median (ns) | Synthetic WAL Wait Candidate Median (ns) | Wait Reduction |"
    echo "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"
    jq -r '
        .benchmark.cases[]
        | "| \(.scenario_id) | \(.dirty_pages) | \(.synthetic_wal_lock_hold_baseline.median_ns) | \(.synthetic_wal_lock_hold_candidate.median_ns) | \(.hold_reduction_ratio_median | tostring) | \(.synthetic_wal_lock_contended_wait_baseline.median_ns) | \(.synthetic_wal_lock_contended_wait_candidate.median_ns) | \(.stall_reduction_ratio_median | tostring) |"
    ' "${REPORT_JSON}"
    echo
    echo "Artifacts:"
    echo "- raw_output: \`${RAW_OUTPUT}\`"
    echo "- benchmark_json: \`${BENCHMARK_JSON}\`"
    echo "- report_json: \`${REPORT_JSON}\`"
} > "${SUMMARY_MD}"

log_event "INFO" "complete" "Track C synthetic WAL critical section benchmark evidence completed"
echo "Wrote Track C synthetic WAL critical section artifacts to ${OUTPUT_DIR}"
