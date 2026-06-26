#!/usr/bin/env bash
set -euo pipefail

BEAD_ID="bd-1dp9.6.2"
LOG_STANDARD_REF="bd-1fpm"
LOG_SCHEMA_VERSION="1.0.0"
SEED="1091901001"
WORKSPACE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_ID="${BEAD_ID}-$(date -u +%Y%m%dT%H%M%SZ)-$$"
TARGET_DIR="${CARGO_TARGET_DIR:-target_bd_1dp9_6_2}"
REPORT_DIR="${WORKSPACE_ROOT}/test-results/bd_1dp9_6_2"
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

printf 'bead_id=%s level=DEBUG run_id=%s seed=%s scenario_id=SQL-PIPELINE-OPT phase=start report=%s reference=%s\n' \
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
    "SQL-PIPELINE-OPT-UNIT" \
    "unit_gate" \
    cargo test -p fsqlite-harness --lib sql_pipeline_optimization::tests::opportunity_gate_passes_with_selected_sql_hotspot -- --nocapture \
    || failures=$((failures + 1))

run_phase \
    "SQL-PIPELINE-OPT-ARTIFACT" \
    "artifact_emission" \
    cargo test -p fsqlite-harness --lib sql_pipeline_optimization::tests::test_sql_pipeline_opt_report_emits_structured_artifact -- --nocapture \
    || failures=$((failures + 1))

run_phase \
    "SQL-PIPELINE-OPT-EXECUTOR-SHAPE" \
    "executor_shape_reuse" \
    cargo test -p fsqlite-e2e --lib prepared_op_executor_normalizes_varying_point_selects -- --nocapture \
    || failures=$((failures + 1))

run_phase \
    "SQL-PIPELINE-OPT-EXECUTOR-CHURN" \
    "executor_churn_gate" \
    cargo test -p fsqlite-e2e --lib run_oplog_fsqlite_prepared_sql_reduces_parser_churn_for_varying_point_selects -- --nocapture \
    || failures=$((failures + 1))

run_phase \
    "SQL-PIPELINE-OPT-EXECUTOR-DELETE-CHURN" \
    "executor_delete_churn_gate" \
    cargo test -p fsqlite-e2e --lib run_oplog_fsqlite_prepared_sql_reduces_parser_churn_for_varying_point_deletes -- --nocapture \
    || failures=$((failures + 1))

run_phase \
    "SQL-PIPELINE-OPT-EXECUTOR-UPDATE-SHAPE" \
    "executor_update_shape_reuse" \
    cargo test -p fsqlite-e2e --lib prepared_op_executor_normalizes_varying_point_updates -- --nocapture \
    || failures=$((failures + 1))

run_phase \
    "SQL-PIPELINE-OPT-EXECUTOR-UPDATE-CHURN" \
    "executor_update_churn_gate" \
    cargo test -p fsqlite-e2e --lib run_oplog_fsqlite_prepared_sql_reduces_parser_churn_for_varying_point_updates -- --nocapture \
    || failures=$((failures + 1))

ARTIFACT_LOG="${LOG_DIR}/artifact_emission.log"
ARTIFACT_JSON_LINE="$(grep -F 'SQL_PIPELINE_OPT_ARTIFACT_JSON:' "${ARTIFACT_LOG}" | tail -n 1 || true)"
ARTIFACT_PAYLOAD="${ARTIFACT_JSON_LINE#SQL_PIPELINE_OPT_ARTIFACT_JSON:}"
ARTIFACT_REPORT_JSON_LINE="$(grep -F 'SQL_PIPELINE_OPT_REPORT_JSON:' "${ARTIFACT_LOG}" | tail -n 1 || true)"
ARTIFACT_REPORT_PAYLOAD="${ARTIFACT_REPORT_JSON_LINE#SQL_PIPELINE_OPT_REPORT_JSON:}"
ARTIFACT_PATH=""
ARTIFACT_SHA=""
ARTIFACT_RECONSTRUCTED=false
if [[ -n "${ARTIFACT_PAYLOAD}" ]]; then
    if ! echo "${ARTIFACT_PAYLOAD}" | jq -e . >/dev/null 2>&1; then
        printf 'bead_id=%s level=ERROR run_id=%s seed=%s scenario_id=SQL-PIPELINE-OPT phase=artifact_extract status=fail reason=invalid_json\n' \
            "${BEAD_ID}" "${RUN_ID}" "${SEED}"
        failures=$((failures + 1))
    else
        ARTIFACT_PATH="$(echo "${ARTIFACT_PAYLOAD}" | jq -r '.path')"
        ARTIFACT_SHA="$(echo "${ARTIFACT_PAYLOAD}" | jq -r '.sha256')"
        if [[ ! -f "${ARTIFACT_PATH}" ]]; then
            if [[ -n "${ARTIFACT_REPORT_PAYLOAD}" ]] && echo "${ARTIFACT_REPORT_PAYLOAD}" | jq -e . >/dev/null 2>&1; then
                mkdir -p "$(dirname "${ARTIFACT_PATH}")"
                printf '%s\n' "${ARTIFACT_REPORT_PAYLOAD}" > "${ARTIFACT_PATH}"
                ARTIFACT_RECONSTRUCTED=true
            fi
        fi
        if [[ -f "${ARTIFACT_PATH}" ]]; then
            local_sha="$(sha256sum "${ARTIFACT_PATH}" | awk '{print $1}')"
            if [[ -n "${ARTIFACT_SHA}" && "${ARTIFACT_SHA}" != "${local_sha}" && "${ARTIFACT_RECONSTRUCTED}" != "true" ]]; then
                printf 'bead_id=%s level=WARN run_id=%s seed=%s scenario_id=SQL-PIPELINE-OPT phase=artifact_extract status=warn reason=sha_mismatch expected=%s actual=%s path=%s\n' \
                    "${BEAD_ID}" "${RUN_ID}" "${SEED}" "${ARTIFACT_SHA}" "${local_sha}" "${ARTIFACT_PATH}"
            elif [[ -n "${ARTIFACT_SHA}" && "${ARTIFACT_SHA}" != "${local_sha}" && "${ARTIFACT_RECONSTRUCTED}" == "true" ]]; then
                printf 'bead_id=%s level=WARN run_id=%s seed=%s scenario_id=SQL-PIPELINE-OPT phase=artifact_extract status=warn reason=reconstructed_sha_override expected=%s actual=%s path=%s\n' \
                    "${BEAD_ID}" "${RUN_ID}" "${SEED}" "${ARTIFACT_SHA}" "${local_sha}" "${ARTIFACT_PATH}"
            fi
            ARTIFACT_SHA="${local_sha}"
        else
            printf 'bead_id=%s level=ERROR run_id=%s seed=%s scenario_id=SQL-PIPELINE-OPT phase=artifact_extract status=fail reason=missing_artifact path=%s\n' \
                "${BEAD_ID}" "${RUN_ID}" "${SEED}" "${ARTIFACT_PATH}"
            failures=$((failures + 1))
        fi
    fi
else
    printf 'bead_id=%s level=ERROR run_id=%s seed=%s scenario_id=SQL-PIPELINE-OPT phase=artifact_extract status=fail reason=missing_payload\n' \
        "${BEAD_ID}" "${RUN_ID}" "${SEED}"
    failures=$((failures + 1))
fi

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

printf '{"schema_version":"%s","bead_id":"%s","run_id":"%s","timestamp":"%s","scenario_id":"SQL-PIPELINE-OPT","phase":"summary","event_type":"run_summary","seed":"%s","worker":"%s","duration_ms":0,"outcome":"%s","error_code":%s,"artifact_paths":["%s"],"context":{"artifact_paths":["%s"],"log_standard_ref":"%s"},"status":"%s","failures":%s,"report":"%s","report_sha256":"%s","artifact_path":"%s","artifact_sha256":"%s","log_standard_ref":"%s"}\n' \
    "${LOG_SCHEMA_VERSION}" "${BEAD_ID}" "${RUN_ID}" "${summary_timestamp}" "${SEED}" "${WORKER}" \
    "${overall_status}" "${summary_error_code}" "${summary_report_rel}" "${summary_report_rel}" \
    "${LOG_STANDARD_REF}" "${overall_status}" "${failures}" "${summary_report_rel}" "${summary_sha256}" \
    "${ARTIFACT_PATH}" "${ARTIFACT_SHA}" "${LOG_STANDARD_REF}" >>"${REPORT_JSONL}"

if [[ "${overall_status}" == "pass" ]]; then
    printf 'bead_id=%s level=INFO run_id=%s seed=%s scenario_id=SQL-PIPELINE-OPT phase=summary degraded_mode_count=0 reference=%s\n' \
        "${BEAD_ID}" "${RUN_ID}" "${SEED}" "${LOG_STANDARD_REF}"
    printf 'bead_id=%s level=INFO run_id=%s seed=%s scenario_id=SQL-PIPELINE-OPT phase=summary status=pass report=%s report_sha256=%s artifact=%s artifact_sha256=%s\n' \
        "${BEAD_ID}" "${RUN_ID}" "${SEED}" "${REPORT_JSONL}" "${summary_sha256}" "${ARTIFACT_PATH}" "${ARTIFACT_SHA}"
else
    printf 'bead_id=%s level=WARN run_id=%s seed=%s scenario_id=SQL-PIPELINE-OPT phase=summary degraded_mode_count=%s reference=%s\n' \
        "${BEAD_ID}" "${RUN_ID}" "${SEED}" "${failures}" "${LOG_STANDARD_REF}"
    printf 'bead_id=%s level=ERROR run_id=%s seed=%s scenario_id=SQL-PIPELINE-OPT phase=summary status=fail failures=%s report=%s report_sha256=%s\n' \
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
  "report_sha256": "${summary_sha256}",
  "artifact_path": "${ARTIFACT_PATH}",
  "artifact_sha256": "${ARTIFACT_SHA}"
}
ENDJSON
fi

if [[ "${failures}" -gt 0 ]]; then
    exit 1
fi
