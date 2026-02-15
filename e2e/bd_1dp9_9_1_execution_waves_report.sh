#!/usr/bin/env bash
set -euo pipefail

BEAD_ID="bd-1dp9.9.1"
LOG_STANDARD_REF="bd-1fpm"
LOG_SCHEMA_VERSION="1.0.0"
SEED="1091901001"
WORKSPACE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_ID="bd-1dp9.9.1-$(date -u +%Y%m%dT%H%M%SZ)-$$"
TARGET_DIR="${CARGO_TARGET_DIR:-target_bd_1dp9_9_1}"
REPORT_DIR="${WORKSPACE_ROOT}/test-results/bd_1dp9_9_1"
LOG_DIR="${REPORT_DIR}/logs/${RUN_ID}"
REPORT_JSONL="${REPORT_DIR}/${RUN_ID}.jsonl"
WORKER="${HOSTNAME:-local}"
JSON_OUTPUT=false

if [[ "${1:-}" == "--json" ]]; then
    JSON_OUTPUT=true
fi

if ! command -v rch >/dev/null 2>&1; then
    printf 'bead_id=%s level=ERROR case=missing_rch reference=%s\n' "${BEAD_ID}" "${LOG_STANDARD_REF}"
    exit 1
fi

mkdir -p "${LOG_DIR}"

printf 'bead_id=%s level=DEBUG run_id=%s seed=%s scenario_id=EXEC-WAVES phase=start report=%s reference=%s\n' \
    "${BEAD_ID}" "${RUN_ID}" "${SEED}" "${REPORT_JSONL}" "${LOG_STANDARD_REF}"

emit_event() {
    local scenario_id="$1"
    local phase="$2"
    local event_type="$3"
    local status="$4"
    local exit_code="$5"
    local duration_ms="$6"
    local log_path="$7"
    local log_sha256="$8"
    local timestamp
    timestamp="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

    local error_code_json="null"
    if [[ "${exit_code}" -ne 0 ]]; then
        error_code_json="\"E_EXIT_${exit_code}\""
    fi

    printf '{"schema_version":"%s","bead_id":"%s","run_id":"%s","timestamp":"%s","scenario_id":"%s","phase":"%s","event_type":"%s","seed":"%s","worker":"%s","duration_ms":%s,"outcome":"%s","error_code":%s,"artifact_paths":["%s"],"context":{"artifact_paths":["%s"],"log_standard_ref":"%s"},"status":"%s","exit_code":%s,"log_path":"%s","log_sha256":"%s","log_standard_ref":"%s"}\n' \
        "${LOG_SCHEMA_VERSION}" "${BEAD_ID}" "${RUN_ID}" "${timestamp}" "${scenario_id}" \
        "${phase}" "${event_type}" "${SEED}" "${WORKER}" "${duration_ms}" "${status}" "${error_code_json}" \
        "${log_path}" "${log_path}" "${LOG_STANDARD_REF}" "${status}" "${exit_code}" "${log_path}" \
        "${log_sha256}" "${LOG_STANDARD_REF}" >>"${REPORT_JSONL}"
}

run_phase() {
    local scenario_id="$1"
    local phase="$2"
    shift 2

    local log_file="${LOG_DIR}/${phase}.log"
    local log_rel="${log_file#${WORKSPACE_ROOT}/}"
    local start_ns
    start_ns="$(date +%s%N)"

    printf 'bead_id=%s level=DEBUG run_id=%s seed=%s scenario_id=%s phase=%s marker=start reference=%s\n' \
        "${BEAD_ID}" "${RUN_ID}" "${SEED}" "${scenario_id}" "${phase}" "${LOG_STANDARD_REF}"

    local status="pass"
    local exit_code=0
    set +e
    CARGO_TARGET_DIR="${TARGET_DIR}" rch exec -- "$@" >"${log_file}" 2>&1
    exit_code=$?
    set -e
    if [[ "${exit_code}" -ne 0 ]]; then
        status="fail"
    fi

    local end_ns
    end_ns="$(date +%s%N)"
    local duration_ms=$(( (end_ns - start_ns) / 1000000 ))
    local log_sha256
    log_sha256="$(sha256sum "${log_file}" | awk '{print $1}')"

    if [[ "${status}" == "pass" ]]; then
        printf 'bead_id=%s level=INFO run_id=%s seed=%s scenario_id=%s phase=%s status=pass duration_ms=%s log=%s\n' \
            "${BEAD_ID}" "${RUN_ID}" "${SEED}" "${scenario_id}" "${phase}" "${duration_ms}" "${log_file}"
    else
        printf 'bead_id=%s level=ERROR run_id=%s seed=%s scenario_id=%s phase=%s status=fail exit_code=%s duration_ms=%s log=%s\n' \
            "${BEAD_ID}" "${RUN_ID}" "${SEED}" "${scenario_id}" "${phase}" "${exit_code}" "${duration_ms}" "${log_file}"
    fi

    emit_event "${scenario_id}" "${phase}" "phase_completed" "${status}" "${exit_code}" "${duration_ms}" "${log_rel}" "${log_sha256}"
    return "${exit_code}"
}

failures=0

run_phase \
    "EXEC-WAVES-UNIT" \
    "unit_determinism" \
    cargo test -p fsqlite-harness execution_waves::tests::plan_builds_deterministic_waves_and_milestones -- --nocapture \
    || failures=$((failures + 1))

run_phase \
    "EXEC-WAVES-ARTIFACT" \
    "artifact_emission" \
    cargo test -p fsqlite-harness execution_waves::tests::test_execution_wave_report_emits_structured_artifact -- --nocapture \
    || failures=$((failures + 1))

summary_sha256="$(sha256sum "${REPORT_JSONL}" | awk '{print $1}')"
overall_status="pass"
if [[ "${failures}" -gt 0 ]]; then
    overall_status="fail"
fi

summary_timestamp="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
summary_report_rel="${REPORT_JSONL#${WORKSPACE_ROOT}/}"
summary_error_code="null"
if [[ "${overall_status}" == "fail" ]]; then
    summary_error_code="\"E_FAILURES_${failures}\""
fi

printf '{"schema_version":"%s","bead_id":"%s","run_id":"%s","timestamp":"%s","scenario_id":"EXEC-WAVES","phase":"summary","event_type":"run_summary","seed":"%s","worker":"%s","duration_ms":0,"outcome":"%s","error_code":%s,"artifact_paths":["%s"],"context":{"artifact_paths":["%s"],"log_standard_ref":"%s"},"status":"%s","failures":%s,"report":"%s","report_sha256":"%s","log_standard_ref":"%s"}\n' \
    "${LOG_SCHEMA_VERSION}" "${BEAD_ID}" "${RUN_ID}" "${summary_timestamp}" "${SEED}" "${WORKER}" \
    "${overall_status}" "${summary_error_code}" "${summary_report_rel}" "${summary_report_rel}" \
    "${LOG_STANDARD_REF}" "${overall_status}" "${failures}" "${summary_report_rel}" "${summary_sha256}" \
    "${LOG_STANDARD_REF}" >>"${REPORT_JSONL}"

if [[ "${overall_status}" == "pass" ]]; then
    printf 'bead_id=%s level=WARN run_id=%s seed=%s scenario_id=EXEC-WAVES phase=summary degraded_mode_count=0 reference=%s\n' \
        "${BEAD_ID}" "${RUN_ID}" "${SEED}" "${LOG_STANDARD_REF}"
    printf 'bead_id=%s level=INFO run_id=%s seed=%s scenario_id=EXEC-WAVES phase=summary status=pass report=%s report_sha256=%s\n' \
        "${BEAD_ID}" "${RUN_ID}" "${SEED}" "${REPORT_JSONL}" "${summary_sha256}"
else
    printf 'bead_id=%s level=WARN run_id=%s seed=%s scenario_id=EXEC-WAVES phase=summary degraded_mode_count=%s reference=%s\n' \
        "${BEAD_ID}" "${RUN_ID}" "${SEED}" "${failures}" "${LOG_STANDARD_REF}"
    printf 'bead_id=%s level=ERROR run_id=%s seed=%s scenario_id=EXEC-WAVES phase=summary status=fail failures=%s report=%s report_sha256=%s\n' \
        "${BEAD_ID}" "${RUN_ID}" "${SEED}" "${failures}" "${REPORT_JSONL}" "${summary_sha256}"
fi

if ${JSON_OUTPUT}; then
    cat <<ENDJSON
{
  "bead_id": "${BEAD_ID}",
  "run_id": "${RUN_ID}",
  "seed": "${SEED}",
  "status": "${overall_status}",
  "failures": ${failures},
  "report_jsonl": "${REPORT_JSONL}",
  "report_sha256": "${summary_sha256}"
}
ENDJSON
fi

if [[ "${failures}" -gt 0 ]]; then
    exit 1
fi
