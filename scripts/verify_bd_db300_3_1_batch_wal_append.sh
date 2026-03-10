#!/usr/bin/env bash
# Verification gate for bd-db300.3.1:
# transaction-wide batch WAL append with checksum-equivalence evidence.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

BEAD_ID="bd-db300.3.1"
SCENARIO_ID="WAL-BATCH-MATRIX"
SEED=20260310
TIMESTAMP_UTC="$(date -u +%Y%m%dT%H%M%SZ)"
RUN_ID="${BEAD_ID}-${TIMESTAMP_UTC}-${SEED}"
TRACE_ID="trace-${RUN_ID}"
ARTIFACT_DIR="${REPO_ROOT}/artifacts/${BEAD_ID}/${RUN_ID}"
EVENTS_JSONL="${ARTIFACT_DIR}/events.jsonl"
TEST_LOG="${ARTIFACT_DIR}/cargo-test.log"
TEST_REPORT_JSON="${ARTIFACT_DIR}/batch_wal_append_report.json"
GATE_REPORT_JSON="${ARTIFACT_DIR}/gate_report.json"
RESULT="running"

mkdir -p "${ARTIFACT_DIR}"
: > "${EVENTS_JSONL}"
: > "${TEST_LOG}"

emit_event() {
    local phase="$1"
    local event_type="$2"
    local outcome="$3"
    local message="$4"
    printf '{"trace_id":"%s","run_id":"%s","scenario_id":"%s","seed":%d,"phase":"%s","event_type":"%s","outcome":"%s","timestamp":"%s","message":"%s"}\n' \
        "${TRACE_ID}" "${RUN_ID}" "${SCENARIO_ID}" "${SEED}" "${phase}" "${event_type}" "${outcome}" \
        "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "${message}" >> "${EVENTS_JSONL}"
}

finish() {
    local exit_code=$?
    if [[ ${exit_code} -eq 0 ]]; then
        RESULT="pass"
    else
        RESULT="fail"
    fi

    cat > "${GATE_REPORT_JSON}" <<EOF
{
  "bead_id": "${BEAD_ID}",
  "run_id": "${RUN_ID}",
  "trace_id": "${TRACE_ID}",
  "scenario_id": "${SCENARIO_ID}",
  "seed": ${SEED},
  "events_jsonl": "${EVENTS_JSONL}",
  "test_log": "${TEST_LOG}",
  "test_report_json": "${TEST_REPORT_JSON}",
  "result": "${RESULT}"
}
EOF

    emit_event "finalize" "info" "${RESULT}" "gate report written to ${GATE_REPORT_JSON}"

    if [[ ${exit_code} -eq 0 ]]; then
        echo "[GATE PASS] ${BEAD_ID} batch WAL append verification passed"
    else
        echo "[GATE FAIL] ${BEAD_ID} batch WAL append verification failed"
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

echo "=== ${BEAD_ID}: transaction-wide batch WAL append verification ==="
echo "run_id=${RUN_ID}"
echo "trace_id=${TRACE_ID}"
echo "scenario_id=${SCENARIO_ID}"
echo "artifact_dir=${ARTIFACT_DIR}"

emit_event "bootstrap" "start" "running" "verification started"

run_step \
    "core_unit" \
    "running adapter checksum-equivalence unit test" \
    rch exec -- cargo test -p fsqlite-core --lib test_adapter_batch_append_checksum_chain_matches_single_append -- --nocapture

run_step \
    "wal_unit" \
    "running WAL checksum-equivalence unit test" \
    rch exec -- cargo test -p fsqlite-wal test_batch_append_checksum_chain_matches_single_append -- --nocapture

run_step \
    "pager_unit" \
    "running pager single-batch-append unit test" \
    rch exec -- cargo test -p fsqlite-pager test_wal_commit_uses_single_batch_append -- --nocapture

run_step \
    "harness_e2e" \
    "running harness integration matrix for batch WAL append" \
    rch exec -- env \
        BD_DB300_3_1_RUN_ID="${RUN_ID}" \
        BD_DB300_3_1_TRACE_ID="${TRACE_ID}" \
        BD_DB300_3_1_SCENARIO_ID="${SCENARIO_ID}" \
        BD_DB300_3_1_SEED="${SEED}" \
        BD_DB300_3_1_ARTIFACT_DIR="${ARTIFACT_DIR}" \
        cargo test -p fsqlite-harness --test bd_db300_3_1_batch_wal_append -- --nocapture

if [[ ! -f "${TEST_REPORT_JSON}" ]]; then
    awk '
        /BEGIN_BD_DB300_3_1_REPORT/ {capture=1; next}
        /END_BD_DB300_3_1_REPORT/ {capture=0}
        capture {print}
    ' "${TEST_LOG}" > "${TEST_REPORT_JSON}"

    if [[ -s "${TEST_REPORT_JSON}" ]]; then
        emit_event "artifact_check" "info" "running" "reconstructed missing test report from ${TEST_LOG}"
    else
        rm -f "${TEST_REPORT_JSON}"
    fi
fi

if [[ ! -f "${TEST_REPORT_JSON}" ]]; then
    emit_event "artifact_check" "fail" "fail" "missing expected test report ${TEST_REPORT_JSON}"
    echo "ERROR: missing expected test report ${TEST_REPORT_JSON}" >&2
    exit 1
fi

emit_event "artifact_check" "pass" "pass" "found test report ${TEST_REPORT_JSON}"
