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
OUTPUT_BASE_DIR="${OUTPUT_DIR:-${WORKSPACE_ROOT}/artifacts/perf/${BEAD_ID}}"
# OUTPUT_DIR selects the parent; each invocation preserves its own artifacts.
# A failed run must never expose an older passing report as its current result.
OUTPUT_DIR="${OUTPUT_BASE_DIR}/${RUN_ID}"
LOG_FILE="${OUTPUT_DIR}/events.jsonl"
RAW_OUTPUT="${OUTPUT_DIR}/raw_test_output.txt"
STDERR_OUTPUT="${OUTPUT_DIR}/raw_test_stderr.txt"
REMOVED_DIAGNOSTICS="${OUTPUT_DIR}/excluded_cargo_launch_lines.tsv"
BENCHMARK_JSON="${OUTPUT_DIR}/benchmark.json"
REPORT_JSON="${OUTPUT_DIR}/report.json"
SUMMARY_MD="${OUTPUT_DIR}/summary.md"
GIT_SHA="$(git -C "${WORKSPACE_ROOT}" rev-parse HEAD)"
GIT_WORKTREE_DIRTY=false
if [[ -n "$(git -C "${WORKSPACE_ROOT}" status --porcelain=v1 --untracked-files=normal)" ]]; then
    GIT_WORKTREE_DIRTY=true
fi
TEST_COMMAND="rch exec -- env CARGO_TERM_COLOR=never cargo test -j 2 --locked -p fsqlite-pager --lib pager::tests::wal_publish_window_shrink_benchmark_report -- --exact --ignored --nocapture --test-threads=1"
REPLAY_COMMAND="bash scripts/verify_bd_db300_3_2_3_publish_window_benchmark.sh"

mkdir -p "${OUTPUT_BASE_DIR}"
mkdir "${OUTPUT_DIR}"
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
echo "Track C run artifacts: ${OUTPUT_DIR}"
log_event "INFO" "command" "${TEST_COMMAND}"
: > "${REMOVED_DIAGNOSTICS}"

# Preserve the streams as received from RCH. A remote transport can already
# have merged Cargo stderr into stdout; the extractor handles only known,
# complete Cargo launch lines and records every excluded line verbatim.
if ! eval "${TEST_COMMAND}" 2>"${STDERR_OUTPUT}" | tee "${RAW_OUTPUT}"; then
    cat "${STDERR_OUTPUT}" >&2
    log_event "ERROR" "benchmark" "rch-offloaded pager synthetic WAL critical section benchmark test failed"
    echo "ERROR: benchmark command failed: ${TEST_COMMAND}" >&2
    exit 1
fi
cat "${STDERR_OUTPUT}" >&2

# RCH can route the complete remote output to either local stream. Select the
# one carrying report markers, preserving both original streams. Never merge
# fragments across streams or choose between two competing report payloads.
CAPTURE_INPUT="${RAW_OUTPUT}"
CAPTURE_STREAM="rch_stdout_as_received"
has_report_marker() {
    awk '
        /^(test pager::tests::wal_publish_window_shrink_benchmark_report \.\.\. )?BEGIN_BD_DB300_3_2_3_REPORT$/ {found=1}
        /^END_BD_DB300_3_2_3_REPORT$/ {found=1}
        END {exit !found}
    ' "$1"
}
if has_report_marker "${STDERR_OUTPUT}"; then
    if has_report_marker "${RAW_OUTPUT}"; then
        log_event "ERROR" "artifact" "report markers appeared on both RCH streams; refusing ambiguous or split payloads"
        echo "ERROR: report markers appeared on both RCH streams" >&2
        exit 1
    fi
    CAPTURE_INPUT="${STDERR_OUTPUT}"
    CAPTURE_STREAM="rch_stderr_as_received"
fi

if ! awk -v removed="${REMOVED_DIAGNOSTICS}" '
    /^(test pager::tests::wal_publish_window_shrink_benchmark_report \.\.\. )?BEGIN_BD_DB300_3_2_3_REPORT$/ {
        if (++begins != 1 || capture) exit 2
        capture=1
        next
    }
    /^END_BD_DB300_3_2_3_REPORT$/ {
        if (!capture || ++ends != 1) exit 2
        capture=0
        next
    }
    capture && /^     Running unittests src\/lib\.rs \([[:alnum:]_.\/-]+\/fsqlite_pager-[[:xdigit:]]+\)$/ {
        print NR "\t" $0 >> removed
        next
    }
    capture && /^     Running tests\/self_alloc_extension_not_conflict\.rs \([[:alnum:]_.\/-]+\/self_alloc_extension_not_conflict-[[:xdigit:]]+\)$/ {
        print NR "\t" $0 >> removed
        next
    }
    capture {print}
    END {if (begins != 1 || ends != 1 || capture) exit 2}
' "${CAPTURE_INPUT}" > "${BENCHMARK_JSON}"; then
    log_event "ERROR" "artifact" "benchmark output must contain exactly one complete report marker pair"
    echo "ERROR: missing, repeated, or incomplete report markers in ${CAPTURE_INPUT}" >&2
    exit 1
fi
if [[ -s "${REMOVED_DIAGNOSTICS}" ]]; then
    log_event "WARN" "artifact" "excluded known interleaved Cargo launch diagnostics; exact raw line receipts are in excluded_cargo_launch_lines.tsv"
fi

if [[ ! -s "${BENCHMARK_JSON}" ]]; then
    log_event "ERROR" "artifact" "missing extracted synthetic WAL critical section benchmark JSON payload"
    echo "ERROR: failed to extract benchmark JSON from ${CAPTURE_INPUT}" >&2
    exit 1
fi

if ! jq -se '
    length == 1 and (.[0] |
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
    )
' "${BENCHMARK_JSON}" >/dev/null; then
    log_event "ERROR" "artifact" "invalid v2 synthetic WAL critical section benchmark schema or timing fields"
    echo "ERROR: invalid v2 synthetic WAL critical section benchmark schema or timing fields" >&2
    exit 1
fi

ALL_WAIT_MEASURED="$(jq -r '
    all(.cases[];
        all([
            .synthetic_wal_lock_contended_wait_baseline,
            .synthetic_wal_lock_contended_wait_candidate
        ][]; .median_ns > 0 and .p95_ns > 0)
    )
' "${BENCHMARK_JSON}")"

ALL_HOLD_MEDIAN_SHRUNK="$(jq -r '[.cases[] | (.synthetic_wal_lock_hold_candidate.median_ns < .synthetic_wal_lock_hold_baseline.median_ns)] | all' "${BENCHMARK_JSON}")"
ALL_WAIT_MEDIAN_SHRUNK="$(jq -r '[.cases[] | (.synthetic_wal_lock_contended_wait_candidate.median_ns < .synthetic_wal_lock_contended_wait_baseline.median_ns)] | all' "${BENCHMARK_JSON}")"
ALL_HOLD_P95_SHRUNK="$(jq -r '[.cases[] | (.synthetic_wal_lock_hold_candidate.p95_ns <= .synthetic_wal_lock_hold_baseline.p95_ns)] | all' "${BENCHMARK_JSON}")"
ALL_WAIT_P95_SHRUNK="$(jq -r '[.cases[] | (.synthetic_wal_lock_contended_wait_candidate.p95_ns <= .synthetic_wal_lock_contended_wait_baseline.p95_ns)] | all' "${BENCHMARK_JSON}")"

# Package valid measurements even when their acceptance gates fail. The final
# exit status still refuses unmeasured waits and non-improving medians.
jq -n \
    --arg schema_version "fsqlite.perf.publish-window-shrink-report.v2" \
    --arg bead_id "${BEAD_ID}" \
    --arg parent_bead_id "${PARENT_BEAD_ID}" \
    --arg run_id "${RUN_ID}" \
    --arg generated_at "${GENERATED_AT}" \
    --arg git_sha "${GIT_SHA}" \
    --argjson git_worktree_dirty "${GIT_WORKTREE_DIRTY}" \
    --arg replay_command "${REPLAY_COMMAND}" \
    --arg benchmark_command "${TEST_COMMAND}" \
    --arg raw_output "${RAW_OUTPUT}" \
    --arg raw_stderr "${STDERR_OUTPUT}" \
    --arg capture_input "${CAPTURE_INPUT}" \
    --arg capture_stream "${CAPTURE_STREAM}" \
    --arg excluded_cargo_launch_lines "${REMOVED_DIAGNOSTICS}" \
    --arg benchmark_json "${BENCHMARK_JSON}" \
    --arg summary_md "${SUMMARY_MD}" \
    --arg report_json "${REPORT_JSON}" \
    --slurpfile benchmark "${BENCHMARK_JSON}" \
    --argjson all_wait_measured "${ALL_WAIT_MEASURED}" \
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
        base_git_head: $git_sha,
        worktree_dirty: $git_worktree_dirty,
        replay_command: $replay_command,
        benchmark_command: $benchmark_command,
        measured_operation: $benchmark[0].measured_operation,
        acceptance: {
            passed: ($all_wait_measured and $all_hold_median_shrunk and $all_wait_median_shrunk),
            synthetic_wal_lock_contended_wait_measured_in_all_cases: $all_wait_measured,
            candidate_shrinks_synthetic_wal_lock_hold_median_in_all_cases: $all_hold_median_shrunk,
            candidate_shrinks_synthetic_wal_lock_contended_wait_median_in_all_cases: $all_wait_median_shrunk,
            candidate_shrinks_or_matches_synthetic_wal_lock_hold_p95_in_all_cases: $all_hold_p95_shrunk,
            candidate_shrinks_or_matches_synthetic_wal_lock_contended_wait_p95_in_all_cases: $all_wait_p95_shrunk
        },
        artifacts: {
            raw_output: $raw_output,
            report_source: $capture_input,
            report_source_stream: $capture_stream,
            raw_stderr: $raw_stderr,
            excluded_cargo_launch_lines: $excluded_cargo_launch_lines,
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
    echo "- base_git_head: \`${GIT_SHA}\`"
    echo "- worktree_dirty: \`${GIT_WORKTREE_DIRTY}\`"
    echo '- Git HEAD identifies the base revision; a dirty worktree needs a separate source-content receipt.'
    echo "- replay_command: \`${REPLAY_COMMAND}\`"
    echo "- benchmark_command: \`${TEST_COMMAND}\`"
    echo "- synthetic_wal_lock_contended_wait_measured_in_all_cases: \`${ALL_WAIT_MEASURED}\`"
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
        | (if .synthetic_wal_lock_contended_wait_baseline.median_ns == 0 or .synthetic_wal_lock_contended_wait_candidate.median_ns == 0 then "unmeasured" else (.stall_reduction_ratio_median | tostring) end) as $wait_reduction
        | "| \(.scenario_id) | \(.dirty_pages) | \(.synthetic_wal_lock_hold_baseline.median_ns) | \(.synthetic_wal_lock_hold_candidate.median_ns) | \(.hold_reduction_ratio_median | tostring) | \(.synthetic_wal_lock_contended_wait_baseline.median_ns) | \(.synthetic_wal_lock_contended_wait_candidate.median_ns) | \($wait_reduction) |"
    ' "${REPORT_JSON}"
    echo
    echo "Artifacts:"
    echo "- raw_output: \`${RAW_OUTPUT}\`"
    echo "- raw_stderr: \`${STDERR_OUTPUT}\`"
    echo "- report_source: \`${CAPTURE_INPUT}\` (\`${CAPTURE_STREAM}\`)"
    echo "- excluded_cargo_launch_lines (raw line number and exact text): \`${REMOVED_DIAGNOSTICS}\`"
    echo "- benchmark_json: \`${BENCHMARK_JSON}\`"
    echo "- report_json: \`${REPORT_JSON}\`"
} > "${SUMMARY_MD}"

echo "Wrote Track C synthetic WAL critical section artifacts to ${OUTPUT_DIR}"
if [[ "${ALL_WAIT_MEASURED}" != "true" ]]; then
    log_event "ERROR" "acceptance" "synthetic WAL critical section wait improvement is unmeasured: baseline or candidate contended wait is zero"
    echo "ERROR: synthetic WAL critical section wait improvement is unmeasured: baseline or candidate contended wait is zero; acceptance remains unmet" >&2
    exit 1
fi
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
log_event "INFO" "complete" "Track C synthetic WAL critical section benchmark evidence completed with acceptance satisfied"
