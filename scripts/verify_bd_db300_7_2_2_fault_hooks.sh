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
SOURCE_MANIFEST="${ARTIFACT_DIR}/source-manifest.txt"
RCH_WORKERS_FILE="${ARTIFACT_DIR}/rch-workers.txt"
RESULT="running"
GIT_COMMIT="$(git -C "${REPO_ROOT}" rev-parse HEAD)"
GIT_BRANCH="$(git -C "${REPO_ROOT}" rev-parse --abbrev-ref HEAD)"
GIT_DIRTY_PATH_COUNT="$(git -C "${REPO_ROOT}" status --porcelain=v1 | wc -l | tr -d ' ')"
GIT_STATUS_SHA256="$(
    git -C "${REPO_ROOT}" status --porcelain=v1 -z | sha256sum | awk '{print $1}'
)"
GIT_TRACKED_DIFF_SHA256="$(
    git -C "${REPO_ROOT}" diff --binary HEAD -- . | sha256sum | awk '{print $1}'
)"
readarray -d '' -t GIT_UNTRACKED_PATHS < <(
    git -C "${REPO_ROOT}" ls-files --others --exclude-standard -z
)

hash_untracked_paths() {
    local path
    for path in "$@"; do
        if [[ ! -e "${REPO_ROOT}/${path}" && ! -L "${REPO_ROOT}/${path}" ]]; then
            printf 'missing\0%s\n' "${path}"
            continue
        fi
        printf '%s\0%s\n' \
            "${path}" \
            "$(git -C "${REPO_ROOT}" hash-object --no-filters -- "${path}")"
    done | sha256sum | awk '{print $1}'
}

GIT_UNTRACKED_MANIFEST_SHA256="$(hash_untracked_paths "${GIT_UNTRACKED_PATHS[@]}")"
if (( ${#GIT_UNTRACKED_PATHS[@]} == 0 )); then
    GIT_UNTRACKED_PATHS_B64=""
else
    GIT_UNTRACKED_PATHS_B64="$(
        printf '%s\0' "${GIT_UNTRACKED_PATHS[@]}" | base64 --wrap=0
    )"
fi
RUSTC_VERSION="$(rustc --version)"
CARGO_VERSION="$(cargo --version)"
RCH_VERSION="$(rch --version | head -n 1)"
CONTROLLER_HOST_NAME="$(hostname)"
CONTROLLER_KERNEL_VERSION="$(uname -srmo)"

mkdir -p "${REPO_ROOT}/artifacts/${BEAD_ID}"
mkdir "${ARTIFACT_DIR}"
: > "${EVENTS_JSONL}"
: > "${TEST_LOG}"
: > "${RCH_WORKERS_FILE}"
{
    printf 'git_commit\t%s\n' "${GIT_COMMIT}"
    printf 'git_branch\t%s\n' "${GIT_BRANCH}"
    printf 'git_status_sha256\t%s\n' "${GIT_STATUS_SHA256}"
    printf 'git_tracked_diff_sha256\t%s\n' "${GIT_TRACKED_DIFF_SHA256}"
    printf 'git_untracked_manifest_sha256\t%s\n' "${GIT_UNTRACKED_MANIFEST_SHA256}"
    printf 'untracked_path_count\t%d\n' "${#GIT_UNTRACKED_PATHS[@]}"
    for path in "${GIT_UNTRACKED_PATHS[@]}"; do
        printf 'untracked_blob\t%s\t%s\n' \
            "$(git -C "${REPO_ROOT}" hash-object --no-filters -- "${path}")" \
            "${path}"
    done
} > "${SOURCE_MANIFEST}"
SOURCE_MANIFEST_SHA256="$(sha256sum "${SOURCE_MANIFEST}" | awk '{print $1}')"

emit_event() {
    local phase="$1"
    local event_type="$2"
    local outcome="$3"
    local message="$4"
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
        "verification complete; immutable evidence hashes follow in ${REPORT_JSON}"
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
    "git_branch": "${GIT_BRANCH}",
    "git_dirty_path_count": ${GIT_DIRTY_PATH_COUNT},
    "git_status_sha256": "${GIT_STATUS_SHA256}",
    "git_tracked_diff_sha256": "${GIT_TRACKED_DIFF_SHA256}",
    "git_untracked_path_count": ${#GIT_UNTRACKED_PATHS[@]},
    "git_untracked_manifest_sha256": "${GIT_UNTRACKED_MANIFEST_SHA256}",
    "source_manifest": "${SOURCE_MANIFEST}",
    "source_manifest_sha256": "${SOURCE_MANIFEST_SHA256}",
    "rustc_version": "${RUSTC_VERSION}",
    "cargo_version": "${CARGO_VERSION}",
    "rch_version": "${RCH_VERSION}",
    "controller_host": "${CONTROLLER_HOST_NAME}",
    "controller_kernel": "${CONTROLLER_KERNEL_VERSION}",
    "execution_transport": "rch remote worker",
    "strict_remote_required": true,
    "rch_self_healing_disabled": true,
    "execution_workers_file": "${RCH_WORKERS_FILE}",
    "execution_workers_sha256": "${rch_workers_sha256}",
    "remote_source_attestation": "HEAD plus tracked binary diff plus explicit untracked blobs verified before every cargo test",
    "rch_clean_overlay": false,
    "rch_clean_overlay_unavailable_reason": "repository contains a Git submodule",
    "script": "scripts/verify_bd_db300_7_2_2_fault_hooks.sh",
    "cargo_profile": "test",
    "cargo_locked": true,
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

run_exact_test() {
    local package="$1"
    local test_name="$2"
    local output
    local status=0

    # The single-quoted program is intentionally expanded only by the remote
    # `bash -lc`; expected values are passed through explicit environment
    # variables above it.
    # shellcheck disable=SC2016
    output="$(
        RCH_REQUIRE_REMOTE=1 \
        RCH_NO_SELF_HEALING=1 \
        rch --no-self-healing exec -- env \
            "FSQLITE_EXPECTED_HEAD=${GIT_COMMIT}" \
            "FSQLITE_EXPECTED_TRACKED_DIFF_SHA256=${GIT_TRACKED_DIFF_SHA256}" \
            "FSQLITE_EXPECTED_UNTRACKED_MANIFEST_SHA256=${GIT_UNTRACKED_MANIFEST_SHA256}" \
            "FSQLITE_UNTRACKED_PATHS_B64=${GIT_UNTRACKED_PATHS_B64}" \
            "CARGO_TERM_COLOR=never" \
            "RUST_LOG=${RUST_LOG}" \
            bash -lc '
                set -euo pipefail

                actual_head="$(git rev-parse HEAD)"
                if [[ "${actual_head}" != "${FSQLITE_EXPECTED_HEAD}" ]]; then
                    echo "remote HEAD mismatch: expected ${FSQLITE_EXPECTED_HEAD}, got ${actual_head}" >&2
                    exit 86
                fi

                actual_tracked_diff_sha256="$(
                    git diff --binary HEAD -- . | sha256sum | awk "{print \$1}"
                )"
                if [[ "${actual_tracked_diff_sha256}" != "${FSQLITE_EXPECTED_TRACKED_DIFF_SHA256}" ]]; then
                    echo "remote tracked diff mismatch" >&2
                    echo "expected=${FSQLITE_EXPECTED_TRACKED_DIFF_SHA256}" >&2
                    echo "actual=${actual_tracked_diff_sha256}" >&2
                    exit 87
                fi

                actual_untracked_paths_b64="$(
                    git ls-files --others --exclude-standard -z | base64 --wrap=0
                )"
                if [[ "${actual_untracked_paths_b64}" != "${FSQLITE_UNTRACKED_PATHS_B64}" ]]; then
                    echo "remote untracked path set mismatch" >&2
                    exit 88
                fi

                actual_untracked_manifest_sha256="$(
                    while IFS= read -r -d "" path; do
                        if [[ ! -e "${path}" && ! -L "${path}" ]]; then
                            printf "missing\0%s\n" "${path}"
                            continue
                        fi
                        printf "%s\0%s\n" \
                            "${path}" \
                            "$(git hash-object --no-filters -- "${path}")"
                    done < <(printf "%s" "${FSQLITE_UNTRACKED_PATHS_B64}" | base64 --decode) |
                        sha256sum |
                        awk "{print \$1}"
                )"
                if [[ "${actual_untracked_manifest_sha256}" != "${FSQLITE_EXPECTED_UNTRACKED_MANIFEST_SHA256}" ]]; then
                    echo "remote untracked manifest mismatch" >&2
                    echo "expected=${FSQLITE_EXPECTED_UNTRACKED_MANIFEST_SHA256}" >&2
                    echo "actual=${actual_untracked_manifest_sha256}" >&2
                    exit 89
                fi

                echo "remote source attestation passed"
                exec cargo test "$@"
            ' bash \
            -p "${package}" \
            --lib \
            --locked \
            --color never \
            --features fault-injection \
            "${test_name}" \
            -- \
            --exact \
            --nocapture 2>&1
    )" || status=$?
    printf '%s\n' "${output}"
    {
        grep -Eo 'Selected worker: [^[:space:]]+' <<<"${output}" |
            awk '{print $3}'
        grep -Eo '\[RCH\] remote [^[:space:]]+' <<<"${output}" |
            awk '{print $3}'
    } >> "${RCH_WORKERS_FILE}" || true
    if [[ ${status} -ne 0 ]]; then
        return "${status}"
    fi
    if ! grep -Fq "remote source attestation passed" <<<"${output}"; then
        echo "remote source attestation did not complete: ${test_name}" >&2
        return 1
    fi
    if ! grep -Eq '\[RCH\] remote [^[:space:]]+' <<<"${output}"; then
        echo "strict-remote execution receipt is missing: ${test_name}" >&2
        return 1
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
