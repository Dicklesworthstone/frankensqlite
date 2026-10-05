#!/usr/bin/env bash
# Verification gate for bd-db300.7.2.2:
# targeted batched-append and publish fault-hook contract.

set -euo pipefail
umask 077

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
RCH_WORKERS_FILE="${ARTIFACT_DIR}/rch-workers.txt"
RESULT="running"
GIT_COMMIT="$(git -C "${REPO_ROOT}" rev-parse HEAD)"
GIT_DIRTY_PATH_COUNT="$(git -C "${REPO_ROOT}" status --porcelain=v1 | wc -l | tr -d ' ')"
RUSTC_VERSION="$(rustc --version)"
CARGO_VERSION="$(cargo --version)"
RCH_VERSION="$(rch --version | head -n 1)"

mkdir -p "${REPO_ROOT}/artifacts/${BEAD_ID}"
# A fresh run directory: never append to (or silently reuse) older evidence.
mkdir "${ARTIFACT_DIR}"
: > "${EVENTS_JSONL}"
: > "${TEST_LOG}"
: > "${RCH_WORKERS_FILE}"

emit_event() {
    local phase="$1"
    local event_type="$2"
    local outcome="$3"
    local message="$4"
    # jq escapes every field, so messages containing quotes or backslashes
    # still produce valid JSONL.
    jq -cn \
        --arg trace_id "${TRACE_ID}" \
        --arg run_id "${RUN_ID}" \
        --arg scenario_id "${SCENARIO_ID}" \
        --arg bead_id "${BEAD_ID}" \
        --argjson seed "${SEED}" \
        --arg phase "${phase}" \
        --arg event_type "${event_type}" \
        --arg outcome "${outcome}" \
        --arg timestamp "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
        --arg message "${message}" \
        '{
          trace_id:$trace_id,
          run_id:$run_id,
          scenario_id:$scenario_id,
          bead_id:$bead_id,
          seed:$seed,
          phase:$phase,
          event_type:$event_type,
          outcome:$outcome,
          timestamp:$timestamp,
          message:$message
        }' >> "${EVENTS_JSONL}"
}

finish() {
    local exit_code=$?
    local events_sha256
    local rch_workers_sha256
    local test_log_sha256

    if [[ ${exit_code} -eq 0 ]]; then
        RESULT="pass"
    else
        RESULT="fail"
    fi

    if [[ -s "${RCH_WORKERS_FILE}" ]]; then
        sort -u -o "${RCH_WORKERS_FILE}" "${RCH_WORKERS_FILE}"
    else
        printf 'unknown\n' > "${RCH_WORKERS_FILE}"
    fi
    rch_workers_sha256="$(sha256sum "${RCH_WORKERS_FILE}" | awk '{print $1}')"
    emit_event \
        "finalize" \
        "result" \
        "${RESULT}" \
        "verification complete; evidence hashes follow in ${REPORT_JSON}"
    events_sha256="$(sha256sum "${EVENTS_JSONL}" | awk '{print $1}')"
    test_log_sha256="$(sha256sum "${TEST_LOG}" | awk '{print $1}')"

    cat > "${REPORT_JSON}" <<EOF
{
  "schema_version": "fsqlite-e2e.commit-path-fault-hook-report.v2",
  "generated_at_utc": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "bead_id": "${BEAD_ID}",
  "run_id": "${RUN_ID}",
  "trace_id": "${TRACE_ID}",
  "scenario_id": "${SCENARIO_ID}",
  "seed": ${SEED},
  "result": "${RESULT}",
  "provenance": {
    "git_commit": "${GIT_COMMIT}",
    "git_dirty_path_count": ${GIT_DIRTY_PATH_COUNT},
    "rustc_version": "${RUSTC_VERSION}",
    "cargo_version": "${CARGO_VERSION}",
    "rch_version": "${RCH_VERSION}",
    "execution_transport": "rch exec (remote worker or local fallback; see execution_workers_file)",
    "execution_workers_file": "${RCH_WORKERS_FILE}",
    "execution_workers_sha256": "${rch_workers_sha256}",
    "script": "scripts/verify_bd_db300_7_2_2_fault_hooks.sh",
    "cargo_profile": "test",
    "features": ["fault-injection"],
    "lib_only": true,
    "exact_selectors": true
  },
  "events_jsonl": {
    "path": "${EVENTS_JSONL}",
    "sha256": "${events_sha256}"
  },
  "test_log": {
    "path": "${TEST_LOG}",
    "sha256": "${test_log_sha256}"
  },
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

    jq -e . "${REPORT_JSON}" >/dev/null

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

# Run exactly one fully qualified lib test remotely and fail closed unless
# that test actually ran and passed. A substring filter that matches nothing
# makes `cargo test` exit 0 with "running 0 tests", which would let a renamed
# or deleted hook test turn this gate silently green.
run_exact_test() {
    local package="$1"
    local test_name="$2"
    local output
    local status=0

    output="$(
        rch exec -- cargo test \
            -p "${package}" \
            --lib \
            --color never \
            --features fault-injection \
            "${test_name}" \
            -- \
            --exact \
            --nocapture 2>&1
    )" || status=$?
    printf '%s\n' "${output}"
    # Record where the test actually ran. rch prints "Selected worker" for
    # attempts that may still fail over, so only a local fallback or a
    # completed "[RCH] remote <worker>" counts as provenance.
    if grep -Eq '\[RCH\] local|falling back to local' <<<"${output}"; then
        printf 'local:%s\n' "$(hostname)" >> "${RCH_WORKERS_FILE}"
    else
        grep -Eo '\[RCH\] remote [^[:space:]]+' <<<"${output}" |
            awk '{print $3}' >> "${RCH_WORKERS_FILE}" || true
    fi
    if [[ ${status} -ne 0 ]]; then
        return "${status}"
    fi
    if ! grep -Fq "running 1 test" <<<"${output}"; then
        echo "exact selector did not run one test: ${test_name}" >&2
        return 1
    fi
    if ! grep -Fq "test ${test_name} ... ok" <<<"${output}"; then
        echo "exact selector did not pass the requested test: ${test_name}" >&2
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
