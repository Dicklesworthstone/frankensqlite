#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

BEAD_ID="bd-3wop3.1.3"
SCENARIO_ID="parallel_wal_publication"
COMPATIBILITY_SELECTOR="wal_invariant,integrity_check,row_level"
EVIDENCE_OWNER="durability-combiner-owner"
VERIFICATION_CLASS="COR,PFA"
ORACLE="rusqlite,wal_invariant,integrity_check"
ALLOWED_DIFFERENCE_POLICY="none"
BASELINE_COMPARATOR="sqlite3_c1_baseline,conservative_publication"
POLICY_ID="parallel_wal_control.v1"
TIMESTAMP_UTC="$(date -u +%Y%m%dT%H%M%SZ)"
RUN_ID="${BEAD_ID}-${TIMESTAMP_UTC}"
TRACE_ID="trace-${RUN_ID}"
ARTIFACT_DIR="${REPO_ROOT}/artifacts/${BEAD_ID}/${RUN_ID}"
RUN_ROOT="${ARTIFACT_DIR}/runs"
TEST_LOG="${ARTIFACT_DIR}/cargo-test.log"
REPORT_JSON="${ARTIFACT_DIR}/parallel_wal_publication_report.json"
GATE_REPORT_JSON="${ARTIFACT_DIR}/gate_report.json"
FIRST_FAILURE_JSON="${ARTIFACT_DIR}/first_failure.json"
EVENTS_JSONL="${ARTIFACT_DIR}/events.jsonl"
FAILED_PHASE=""
FAILED_DESCRIPTION=""
FAILED_REPLAY=""
RESULT="running"

mkdir -p "${RUN_ROOT}"
: > "${TEST_LOG}"
: > "${EVENTS_JSONL}"

json_escape() {
    local value="$1"
    value="${value//\\/\\\\}"
    value="${value//\"/\\\"}"
    value="${value//$'\n'/\\n}"
    printf '%s' "${value}"
}

emit_event() {
    local phase="$1"
    local outcome="$2"
    local message="$3"
    printf '{"claim_id":"%s","evidence_id":"%s","evidence_owner":"%s","verification_class":"%s","trace_id":"%s","run_id":"%s","scenario_id":"%s","phase":"%s","outcome":"%s","compatibility_selector":"%s","oracle":"%s","allowed_difference_policy":"%s","baseline_comparator":"%s","policy_id":"%s","decision_id":"%s:%s","timestamp":"%s","message":"%s"}\n' \
        "${BEAD_ID}" "D1-GATE:${RUN_ID}" "${EVIDENCE_OWNER}" "${VERIFICATION_CLASS}" \
        "${TRACE_ID}" "${RUN_ID}" "${SCENARIO_ID}" "${phase}" "${outcome}" \
        "${COMPATIBILITY_SELECTOR}" "${ORACLE}" "${ALLOWED_DIFFERENCE_POLICY}" \
        "${BASELINE_COMPARATOR}" "${POLICY_ID}" "${TRACE_ID}" "${phase}" \
        "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
        "$(json_escape "${message}")" >> "${EVENTS_JSONL}"
}

write_first_failure() {
    if [[ -z "${FAILED_PHASE}" ]]; then
        return
    fi
    printf '{\n  "bead_id": "%s",\n  "run_id": "%s",\n  "trace_id": "%s",\n  "scenario_id": "%s",\n  "phase": "%s",\n  "description": "%s",\n  "replay_command": "%s",\n  "test_log": "%s",\n  "compatibility_selector": "%s"\n}\n' \
        "${BEAD_ID}" "${RUN_ID}" "${TRACE_ID}" "${SCENARIO_ID}" \
        "$(json_escape "${FAILED_PHASE}")" "$(json_escape "${FAILED_DESCRIPTION}")" \
        "$(json_escape "${FAILED_REPLAY}")" "$(json_escape "${TEST_LOG}")" \
        "${COMPATIBILITY_SELECTOR}" > "${FIRST_FAILURE_JSON}"
}

finish() {
    local exit_code=$?
    if [[ ${exit_code} -eq 0 ]]; then
        RESULT="pass"
    else
        RESULT="fail"
        write_first_failure
    fi

    printf '{\n  "bead_id": "%s",\n  "claim_id": "%s",\n  "evidence_id": "%s",\n  "evidence_owner": "%s",\n  "verification_class": "%s",\n  "run_id": "%s",\n  "trace_id": "%s",\n  "scenario_id": "%s",\n  "compatibility_selector": "%s",\n  "oracle": "%s",\n  "allowed_difference_policy": "%s",\n  "baseline_comparator": "%s",\n  "policy_id": "%s",\n  "shadow_lineage": "D1-COR-01:fsqlite_mvcc:baseline_unpinned:c2:conservative:42",\n  "fallback_lineage": "D1-COR-01:fsqlite_mvcc:baseline_unpinned:c2:conservative:42",\n  "artifact_dir": "%s",\n  "test_log": "%s",\n  "report_json": "%s",\n  "first_failure_json": "%s",\n  "result": "%s"\n}\n' \
        "${BEAD_ID}" "${BEAD_ID}" "D1-GATE:${RUN_ID}" "${EVIDENCE_OWNER}" \
        "${VERIFICATION_CLASS}" "${RUN_ID}" "${TRACE_ID}" "${SCENARIO_ID}" \
        "${COMPATIBILITY_SELECTOR}" "${ORACLE}" "${ALLOWED_DIFFERENCE_POLICY}" \
        "${BASELINE_COMPARATOR}" "${POLICY_ID}" "${ARTIFACT_DIR}" "${TEST_LOG}" \
        "${REPORT_JSON}" "${FIRST_FAILURE_JSON}" "${RESULT}" > "${GATE_REPORT_JSON}"

    emit_event "finalize" "${RESULT}" "gate report written to ${GATE_REPORT_JSON}"
    if [[ ${exit_code} -eq 0 ]]; then
        echo "[GATE PASS] ${BEAD_ID} durability-certificate publication verification passed"
    else
        echo "[GATE FAIL] ${BEAD_ID} durability-certificate publication verification failed"
        echo "artifact_dir=${ARTIFACT_DIR}"
        echo "first_failure=${FIRST_FAILURE_JSON}"
    fi
}
trap finish EXIT

remember_command() {
    local replay=""
    printf -v replay '%q ' "$@"
    printf '%s' "${replay% }"
}

run_compile_step() {
    local phase="$1"
    local description="$2"
    shift 2

    local replay
    replay="$(remember_command "$@")"
    emit_event "${phase}" "running" "${description}"
    if command -v rch >/dev/null 2>&1; then
        replay="RCH_REQUIRE_REMOTE=1 RCH_NO_SELF_HEALING=1 rch --no-self-healing exec -- ${replay}"
        if RCH_REQUIRE_REMOTE=1 RCH_NO_SELF_HEALING=1 \
            rch --no-self-healing exec -- "$@" 2>&1 | tee -a "${TEST_LOG}"; then
            emit_event "${phase}" "pass" "${description} via strict rch"
            return 0
        fi
    else
        replay="$(remember_command "$@")"
        if "$@" 2>&1 | tee -a "${TEST_LOG}"; then
            emit_event "${phase}" "pass" "${description} via local execution"
            return 0
        fi
    fi

    FAILED_PHASE="${phase}"
    FAILED_DESCRIPTION="${description}"
    FAILED_REPLAY="${replay}"
    emit_event "${phase}" "fail" "${description}"
    return 1
}

echo "=== ${BEAD_ID}: durability combiner and certificate-backed publication ==="
echo "run_id=${RUN_ID}"
echo "trace_id=${TRACE_ID}"
echo "scenario_id=${SCENARIO_ID}"
echo "compatibility_selector=${COMPATIBILITY_SELECTOR}"
echo "artifact_dir=${ARTIFACT_DIR}"

emit_event "bootstrap" "running" "verification started"

run_compile_step \
    "wal_unit" \
    "deterministic certificates, durable-before-visible ordering, safe fallbacks, and concurrency" \
    cargo test -p fsqlite-wal parallel_wal::tests --lib -- --nocapture

run_compile_step \
    "pager_unit" \
    "certificate-backed pager publication authorization and atomic full-group page-plane handoff" \
    cargo test -p fsqlite-pager parallel_wal_ --lib -- --nocapture

run_compile_step \
    "certificate_storage" \
    "durable sidecar reconstruction and commit-marker authorization boundary" \
    cargo test -p fsqlite-core durable_certificate_sidecar_precedes_and_reconstructs_wal_commit --lib -- --nocapture

run_compile_step \
    "cross_process_continuity" \
    "bounded authorized-tail lookup and two-instance certificate clock continuity" \
    cargo test -p fsqlite-core two_backend_instances_continue_authorized_certificate_clocks --lib -- --nocapture

run_compile_step \
    "checkpoint_compatibility" \
    "existing WAL checkpoint compatibility invariant" \
    cargo test -p fsqlite-core test_pragma_wal_checkpoint_after_writes --lib -- --nocapture

run_compile_step \
    "e2e" \
    "auto, conservative, and shadow-compare row/integrity/publication matrix" \
    env \
    RUST_LOG=trace \
    FSQLITE_BD_3WOP3_1_3_RUN_ROOT="${RUN_ROOT}" \
    FSQLITE_BD_3WOP3_1_3_ARTIFACT="${REPORT_JSON}" \
    cargo test -p fsqlite-e2e --test bd_3wop3_1_3_parallel_wal_publication -- --nocapture --test-threads=1

# Strict RCH executes the test on the worker, so its direct artifact path is
# worker-local. The test also emits the same compact JSON on stdout; materialize
# that proof bundle locally without weakening remote-only compilation.
if [[ ! -f "${REPORT_JSON}" ]]; then
    report_line="$(rg '^FSQLITE_D1_REPORT_JSON=' "${TEST_LOG}" | tail -n 1 || true)"
    if [[ -n "${report_line}" ]]; then
        printf '%s\n' "${report_line#FSQLITE_D1_REPORT_JSON=}" > "${REPORT_JSON}"
    fi
fi

if [[ ! -f "${REPORT_JSON}" ]]; then
    FAILED_PHASE="artifact_check"
    FAILED_DESCRIPTION="expected publication report was not written"
    FAILED_REPLAY="cargo test -p fsqlite-e2e --test bd_3wop3_1_3_parallel_wal_publication -- --nocapture --test-threads=1"
    emit_event "artifact_check" "fail" "missing expected report ${REPORT_JSON}"
    exit 1
fi

emit_event "artifact_check" "pass" "found report ${REPORT_JSON}"
