#!/usr/bin/env bash
set -euo pipefail

BEAD_ID="bd-ncivz.1"
SCENARIO_ID="E2E-CNC-007"
SEED=2026021601
WORKSPACE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPORT_ROOT="${WORKSPACE_ROOT}/artifacts/ncivz_1_parallel_wal_buffer"
RUN_ID="${BEAD_ID}-$(date -u +%Y%m%dT%H%M%SZ)-$$"
TRACE_ID="${RUN_ID}"
REPORT_JSONL="${REPORT_ROOT}/${RUN_ID}.jsonl"
LOG_PATH="${REPORT_ROOT}/${RUN_ID}.log"
TEST_FILTER="bd_ncivz_1_"
REPLAY_COMMAND="RUST_TEST_THREADS=1 rch exec -- cargo test -p fsqlite-wal ${TEST_FILTER} -- --nocapture"
JSON_OUTPUT=false

if [[ "${1:-}" == "--json" ]]; then
    JSON_OUTPUT=true
fi

if ! command -v rch >/dev/null 2>&1; then
    printf 'bead_id=%s level=ERROR run_id=%s trace_id=%s scenario_id=%s phase=preflight error_code=E_RCH_MISSING\n' \
        "${BEAD_ID}" "${RUN_ID}" "${TRACE_ID}" "${SCENARIO_ID}" >&2
    exit 2
fi

mkdir -p "${REPORT_ROOT}"
: >"${REPORT_JSONL}"

emit_event() {
    local phase="$1"
    local event_type="$2"
    local outcome="$3"
    local duration_us="$4"
    local error_code="$5"
    local schema_conforms="$6"

    jq -nc \
        --arg trace_id "${TRACE_ID}" \
        --arg run_id "${RUN_ID}" \
        --arg scenario_id "${SCENARIO_ID}" \
        --arg bead_id "${BEAD_ID}" \
        --arg phase "${phase}" \
        --arg event_type "${event_type}" \
        --arg outcome "${outcome}" \
        --arg error_code "${error_code}" \
        --arg replay_command "${REPLAY_COMMAND}" \
        --arg log_path "${LOG_PATH}" \
        --arg report_jsonl "${REPORT_JSONL}" \
        --argjson duration_us "${duration_us}" \
        --argjson seed "${SEED}" \
        --argjson schema_conforms "${schema_conforms}" \
        '{
            trace_id: $trace_id,
            run_id: $run_id,
            scenario_id: $scenario_id,
            bead_id: $bead_id,
            phase: $phase,
            event_type: $event_type,
            core_id: null,
            epoch: null,
            lane_state_from: null,
            lane_state_to: null,
            outcome: $outcome,
            duration_us: $duration_us,
            error_code: (if $error_code == "null" then null else $error_code end),
            seed: $seed,
            schema_conforms: $schema_conforms,
            replay_command: $replay_command,
            artifact_paths: [$log_path, $report_jsonl]
        }' >>"${REPORT_JSONL}"
}

printf 'bead_id=%s level=INFO run_id=%s trace_id=%s scenario_id=%s phase=start seed=%s\n' \
    "${BEAD_ID}" "${RUN_ID}" "${TRACE_ID}" "${SCENARIO_ID}" "${SEED}"

start_ns="$(date +%s%N)"
set +e
(
    cd "${WORKSPACE_ROOT}"
    RUST_TEST_THREADS=1 rch exec -- cargo test -p fsqlite-wal "${TEST_FILTER}" -- --nocapture
) >"${LOG_PATH}" 2>&1
exit_code=$?
set -e
end_ns="$(date +%s%N)"

duration_us="$(( (end_ns - start_ns) / 1000 ))"
if [[ ${exit_code} -eq 0 ]]; then
    emit_event "execute" "pilot_run" "pass" "${duration_us}" "null" true
else
    emit_event "execute" "pilot_run" "fail" "${duration_us}" "E_TEST_${exit_code}" true
fi

schema_conforms=true
if ! jq -s -e '
    all(.[]; has("trace_id")
        and has("run_id")
        and has("scenario_id")
        and has("bead_id")
        and has("core_id")
        and has("epoch")
        and has("lane_state_from")
        and has("lane_state_to")
        and has("outcome")
        and has("duration_us")
        and has("error_code")
        and has("schema_conforms"))
' "${REPORT_JSONL}" >/dev/null; then
    schema_conforms=false
fi

summary_outcome="pass"
summary_error="null"
if [[ ${exit_code} -ne 0 || "${schema_conforms}" != "true" ]]; then
    summary_outcome="fail"
    if [[ ${exit_code} -ne 0 ]]; then
        summary_error="E_TEST_${exit_code}"
    else
        summary_error="E_SCHEMA"
    fi
fi

if [[ "${schema_conforms}" == "true" ]]; then
    emit_event "summary" "run_summary" "${summary_outcome}" "${duration_us}" "${summary_error}" true
else
    emit_event "summary" "run_summary" "${summary_outcome}" "${duration_us}" "${summary_error}" false
fi

printf 'bead_id=%s level=INFO run_id=%s trace_id=%s scenario_id=%s phase=summary outcome=%s duration_us=%s path=%s replay_command=%s schema_conforms=%s\n' \
    "${BEAD_ID}" "${RUN_ID}" "${TRACE_ID}" "${SCENARIO_ID}" "${summary_outcome}" "${duration_us}" "${REPORT_JSONL}" "${REPLAY_COMMAND}" "${schema_conforms}"

if ${JSON_OUTPUT}; then
    jq -nc \
        --arg bead_id "${BEAD_ID}" \
        --arg run_id "${RUN_ID}" \
        --arg trace_id "${TRACE_ID}" \
        --arg scenario_id "${SCENARIO_ID}" \
        --arg outcome "${summary_outcome}" \
        --arg report_jsonl "${REPORT_JSONL}" \
        --arg log_path "${LOG_PATH}" \
        --arg replay_command "${REPLAY_COMMAND}" \
        --argjson schema_conforms "${schema_conforms}" \
        --argjson seed "${SEED}" \
        '{
            bead_id: $bead_id,
            run_id: $run_id,
            trace_id: $trace_id,
            scenario_id: $scenario_id,
            outcome: $outcome,
            schema_conforms: $schema_conforms,
            seed: $seed,
            report_jsonl: $report_jsonl,
            log_path: $log_path,
            replay_command: $replay_command
        }'
fi

if [[ ${exit_code} -ne 0 ]]; then
    exit "${exit_code}"
fi
if [[ "${schema_conforms}" != "true" ]]; then
    exit 3
fi

exit 0
