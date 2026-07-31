#!/usr/bin/env bash
# Verification gate for bd-db300.7.2.2:
# targeted batched-append and publish fault-hook contract.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

BEAD_ID="bd-db300.7.2.2"
SCENARIO_ID="COMMIT-PATH-FAULT-HOOKS"
SEED=20260323
TIMESTAMP_UTC="$(date -u +%Y%m%dT%H%M%SZ)"
RUN_ID="${BEAD_ID}-${TIMESTAMP_UTC}-${SEED}"
TRACE_ID="trace-${RUN_ID}"
ARTIFACT_DIR="${REPO_ROOT}/artifacts/${BEAD_ID}/${RUN_ID}"
EVENTS_JSONL="${ARTIFACT_DIR}/events.jsonl"
TEST_LOG="${ARTIFACT_DIR}/cargo-test.log"
REPORT_JSON="${ARTIFACT_DIR}/report.json"
RESULT="running"

mkdir -p "${ARTIFACT_DIR}"
: > "${EVENTS_JSONL}"
: > "${TEST_LOG}"

emit_event() {
    local phase="$1"
    local event_type="$2"
    local outcome="$3"
    local message="$4"
    printf '{"trace_id":"%s","run_id":"%s","scenario_id":"%s","bead_id":"%s","seed":%d,"phase":"%s","event_type":"%s","outcome":"%s","timestamp":"%s","message":"%s"}\n' \
        "${TRACE_ID}" "${RUN_ID}" "${SCENARIO_ID}" "${BEAD_ID}" "${SEED}" "${phase}" "${event_type}" "${outcome}" \
        "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "${message}" >> "${EVENTS_JSONL}"
}

finish() {
    local exit_code=$?
    if [[ ${exit_code} -eq 0 ]]; then
        RESULT="pass"
    else
        RESULT="fail"
    fi

    cat > "${REPORT_JSON}" <<EOF
{
  "bead_id": "${BEAD_ID}",
  "run_id": "${RUN_ID}",
  "trace_id": "${TRACE_ID}",
  "scenario_id": "${SCENARIO_ID}",
  "seed": ${SEED},
  "result": "${RESULT}",
  "events_jsonl": "${EVENTS_JSONL}",
  "test_log": "${TEST_LOG}",
  "hook_contract": {
    "wal_points": [
      "wal_after_append",
      "wal_sync_failure",
      "wal_append_busy_countdown"
    ],
    "pager_points": [
      "after_flush_before_publish"
    ],
    "required_context": [
      "run_id",
      "scenario_id",
      "invariant_family",
      "trigger_seq",
      "detail"
    ]
  }
}
EOF

    emit_event "finalize" "info" "${RESULT}" "report written to ${REPORT_JSON}"

    if [[ ${exit_code} -eq 0 ]]; then
        echo "[GATE PASS] ${BEAD_ID} fault-hook verification passed"
    else
        echo "[GATE FAIL] ${BEAD_ID} fault-hook verification failed"
    fi
}
trap finish EXIT

run_step() {
    local phase="$1"
    local description="$2"
    shift 2

    emit_event "${phase}" "start" "running" "${description}"
    if "$@" 2>&1 | tee -a "${TEST_LOG}"; then
        emit_event "${phase}" "pass" "pass" "${description}"
    else
        emit_event "${phase}" "fail" "fail" "${description}"
        return 1
    fi
}

run_exact_test() {
    local package="$1"
    local test_name="$2"
    local output
    local status=0

    output="$(
        rch exec -- cargo test \
            -p "${package}" \
            --lib \
            --features fault-injection \
            "${test_name}" \
            -- \
            --exact \
            --nocapture 2>&1
    )" || status=$?
    printf '%s\n' "${output}"
    if [[ ${status} -ne 0 ]]; then
        return "${status}"
    fi
    if ! grep -Fq "running 1 test" <<<"${output}"; then
        echo "exact selector did not run one test: ${test_name}" >&2
        return 1
    fi
    if ! grep -Eq 'test result: ok\. 1 passed; 0 failed;' <<<"${output}"; then
        echo "exact selector did not report one passing test: ${test_name}" >&2
        return 1
    fi
}

echo "=== ${BEAD_ID}: commit-path fault-hook verification ==="
echo "run_id=${RUN_ID}"
echo "trace_id=${TRACE_ID}"
echo "scenario_id=${SCENARIO_ID}"
echo "artifact_dir=${ARTIFACT_DIR}"

emit_event "bootstrap" "start" "running" "verification started"

export RUST_LOG="${RUST_LOG:-fsqlite_wal::fault_injection=info,fsqlite_pager::fault_injection=info}"

run_step \
    "wal_after_append" \
    "running WAL after-append hook contract test" \
    run_exact_test fsqlite-wal wal::tests::test_fault_hook_after_wal_append_returns_error_and_records_context

run_step \
    "wal_sync_failure" \
    "running WAL sync hook contract test" \
    run_exact_test fsqlite-wal wal::tests::test_fault_hook_sync_failure_returns_error_and_records_context

run_step \
    "wal_busy_countdown" \
    "running WAL append busy-countdown hook contract test" \
    run_exact_test fsqlite-wal wal::tests::test_fault_hook_append_busy_countdown_fires_once_and_preserves_retry_surface

run_step \
    "pager_publish_boundary" \
    "running pager after-flush-before-publish hook contract test" \
    run_exact_test fsqlite-pager pager::tests::test_group_commit_fault_hook_after_durability_completes_without_abort_and_records_context
