#!/usr/bin/env bash
set -euo pipefail

BEAD_ID="bd-1dp9.5.2"
LOG_STANDARD_REF="bd-1fpm"
LOG_SCHEMA_VERSION="1.0.0"
SEED="1095200001"
WORKSPACE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_ID="bd-1dp9.5.2-$(date -u +%Y%m%dT%H%M%SZ)-$$"
TARGET_DIR="${CARGO_TARGET_DIR:-/var/tmp/target_bd_1dp9_5_2}"
REPORT_DIR="${WORKSPACE_ROOT}/test-results/bd_1dp9_5_2"
LOG_DIR="${REPORT_DIR}/logs/${RUN_ID}"
REPORT_JSONL="${REPORT_DIR}/${RUN_ID}.jsonl"
WORKER="${HOSTNAME:-local}"
FIXTURE="json_fts_wave"
JSON_OUTPUT=false

if [[ "${1:-}" == "--json" ]]; then
    JSON_OUTPUT=true
fi

mkdir -p "${LOG_DIR}"

printf 'bead_id=%s level=DEBUG run_id=%s seed=%s scenario_id=EXT-ALL phase=start report=%s reference=%s\n' \
    "${BEAD_ID}" "${RUN_ID}" "${SEED}" "${REPORT_JSONL}" "${LOG_STANDARD_REF}"

emit_event() {
    local scenario_id="$1"
    local phase="$2"
    local marker="$3"
    local event_type="$4"
    local status="$5"
    local exit_code="$6"
    local duration_ms="$7"
    local log_path="$8"
    local log_sha256="$9"
    local first_divergence="${10}"
    local artifact_paths_json="${11}"
    local timestamp
    timestamp="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

    local error_code_json="null"
    if [[ "${exit_code}" -ne 0 ]]; then
        error_code_json="\"E_EXIT_${exit_code}\""
    fi

    printf '{"schema_version":"%s","bead_id":"%s","run_id":"%s","timestamp":"%s","scenario_id":"%s","test_id":"%s","phase":"%s","stage":"%s","operation":"%s","event_type":"%s","seed":"%s","fixture":"%s","worker":"%s","duration_ms":%s,"outcome":"%s","error_code":%s,"artifact_paths":%s,"context":{"artifact_paths":%s,"log_standard_ref":"%s"},"marker":"%s","status":"%s","exit_code":%s,"log_path":"%s","log_sha256":"%s","first_divergence":%s,"log_standard_ref":"%s"}\n' \
        "${LOG_SCHEMA_VERSION}" "${BEAD_ID}" "${RUN_ID}" "${timestamp}" "${scenario_id}" \
        "${phase}" "${phase}" "${marker}" "${phase}" "${event_type}" "${SEED}" \
        "${FIXTURE}" "${WORKER}" "${duration_ms}" "${status}" "${error_code_json}" \
        "${artifact_paths_json}" "${artifact_paths_json}" "${LOG_STANDARD_REF}" \
        "${marker}" "${status}" "${exit_code}" "${log_path}" "${log_sha256}" \
        "${first_divergence}" "${LOG_STANDARD_REF}" >>"${REPORT_JSONL}"
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
    emit_event \
        "${scenario_id}" \
        "${phase}" \
        "start" \
        "phase_started" \
        "running" \
        0 \
        0 \
        "${log_rel}" \
        "" \
        "false" \
        "[\"${log_rel}\"]"

    local status="pass"
    local exit_code=0
    set +e
    rch exec -- env CARGO_TARGET_DIR="${TARGET_DIR}" "$@" >"${log_file}" 2>&1
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
    local first_divergence="false"
    if grep -qi "first_divergence_index=[1-9]\|first_divergence_sql=\|mismatch\|status=fail" "${log_file}"; then
        first_divergence="true"
    fi

    if [[ "${status}" == "pass" ]]; then
        printf 'bead_id=%s level=INFO run_id=%s seed=%s scenario_id=%s phase=%s status=pass duration_ms=%s log=%s sha256=%s first_divergence=%s\n' \
            "${BEAD_ID}" "${RUN_ID}" "${SEED}" "${scenario_id}" "${phase}" "${duration_ms}" "${log_file}" "${log_sha256}" "${first_divergence}"
    else
        printf 'bead_id=%s level=ERROR run_id=%s seed=%s scenario_id=%s phase=%s status=fail exit_code=%s duration_ms=%s log=%s sha256=%s first_divergence=%s\n' \
            "${BEAD_ID}" "${RUN_ID}" "${SEED}" "${scenario_id}" "${phase}" "${exit_code}" "${duration_ms}" "${log_file}" "${log_sha256}" "${first_divergence}"
    fi

    emit_event \
        "${scenario_id}" \
        "${phase}" \
        "finish" \
        "phase_completed" \
        "${status}" \
        "${exit_code}" \
        "${duration_ms}" \
        "${log_rel}" \
        "${log_sha256}" \
        "${first_divergence}" \
        "[\"${log_rel}\"]"
    return "${exit_code}"
}

failures=0

run_phase \
    "EXT-1" \
    "differential_wave" \
    cargo test -p fsqlite-harness --test bd_1dp9_5_2_json_fts_wave -- --nocapture \
    || failures=$((failures + 1))

run_phase \
    "EXT-2" \
    "unit_json_extension" \
    cargo test -p fsqlite-ext-json --lib -- --nocapture \
    || failures=$((failures + 1))

run_phase \
    "EXT-2" \
    "unit_fts5_extension" \
    cargo test -p fsqlite-ext-fts5 --lib -- --nocapture \
    || failures=$((failures + 1))

run_phase \
    "EXT-4" \
    "e2e_json_storage" \
    cargo test -p fsqlite-harness --test ext_real_storage_test json_text_storage_round_trip -- --nocapture \
    || failures=$((failures + 1))

run_phase \
    "EXT-4" \
    "e2e_fts_tokenizer" \
    cargo test -p fsqlite-harness --test ext_real_storage_test fts5_tokenizer_on_stored_text -- --nocapture \
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

printf '{"schema_version":"%s","bead_id":"%s","run_id":"%s","timestamp":"%s","scenario_id":"EXT-ALL","test_id":"summary","phase":"summary","stage":"summary","operation":"summary","event_type":"run_summary","seed":"%s","fixture":"%s","worker":"%s","duration_ms":0,"outcome":"%s","error_code":%s,"artifact_paths":["%s"],"context":{"artifact_paths":["%s"],"log_standard_ref":"%s"},"status":"%s","failures":%s,"report":"%s","report_sha256":"%s","log_standard_ref":"%s"}\n' \
    "${LOG_SCHEMA_VERSION}" "${BEAD_ID}" "${RUN_ID}" "${summary_timestamp}" \
    "${SEED}" "${FIXTURE}" "${WORKER}" "${overall_status}" "${summary_error_code}" \
    "${summary_report_rel}" "${summary_report_rel}" "${LOG_STANDARD_REF}" \
    "${overall_status}" "${failures}" "${summary_report_rel}" "${summary_sha256}" "${LOG_STANDARD_REF}" \
    >>"${REPORT_JSONL}"

if [[ "${overall_status}" == "pass" ]]; then
    printf 'bead_id=%s level=WARN run_id=%s seed=%s scenario_id=EXT-ALL phase=summary degraded_mode_count=0 reference=%s\n' \
        "${BEAD_ID}" "${RUN_ID}" "${SEED}" "${LOG_STANDARD_REF}"
    printf 'bead_id=%s level=INFO run_id=%s seed=%s scenario_id=EXT-ALL phase=summary status=pass report=%s report_sha256=%s\n' \
        "${BEAD_ID}" "${RUN_ID}" "${SEED}" "${REPORT_JSONL}" "${summary_sha256}"
else
    printf 'bead_id=%s level=WARN run_id=%s seed=%s scenario_id=EXT-ALL phase=summary degraded_mode_count=%s reference=%s\n' \
        "${BEAD_ID}" "${RUN_ID}" "${SEED}" "${failures}" "${LOG_STANDARD_REF}"
    printf 'bead_id=%s level=ERROR run_id=%s seed=%s scenario_id=EXT-ALL phase=summary status=fail failures=%s report=%s report_sha256=%s\n' \
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
