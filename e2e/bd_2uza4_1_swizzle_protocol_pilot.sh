#!/usr/bin/env bash
set -euo pipefail

BEAD_ID="bd-2uza4.1"
SCENARIO_ID="E2E-CNC-006"
SEED=2026021501
WORKSPACE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPORT_ROOT="${WORKSPACE_ROOT}/artifacts/swizzle_protocol_pilot"
RUN_ID="${BEAD_ID}-$(date -u +%Y%m%dT%H%M%SZ)-$$"
TRACE_ID="${RUN_ID}"
REPORT_JSONL="${REPORT_ROOT}/${RUN_ID}.jsonl"
LOG_PATH="${REPORT_ROOT}/${RUN_ID}.log"
LOG_SCHEMA_REF="docs/e2e_log_schema_contract.md"
JSON_OUTPUT=false

if [[ "${1:-}" == "--json" ]]; then
    JSON_OUTPUT=true
fi

TEST_CMD=(cargo test -p fsqlite-btree swizzle -- --nocapture)
if command -v rch >/dev/null 2>&1; then
    TEST_CMD=(rch exec -- "${TEST_CMD[@]}")
fi

mkdir -p "${REPORT_ROOT}"
: >"${REPORT_JSONL}"

emit_event() {
    local phase="$1"
    local event_type="$2"
    local op="$3"
    local outcome="$4"
    local duration_ms="$5"
    local error_code="$6"
    local log_sha256="$7"
    jq -nc \
        --arg trace_id "${TRACE_ID}" \
        --arg run_id "${RUN_ID}" \
        --arg scenario_id "${SCENARIO_ID}" \
        --arg bead_id "${BEAD_ID}" \
        --arg phase "${phase}" \
        --arg event_type "${event_type}" \
        --arg op "${op}" \
        --arg outcome "${outcome}" \
        --argjson duration_ms "${duration_ms}" \
        --arg error_code "${error_code}" \
        --arg log_path "${LOG_PATH}" \
        --arg log_sha256 "${log_sha256}" \
        --argjson seed "${SEED}" \
        --arg schema_ref "${LOG_SCHEMA_REF}" \
        '{
            trace_id: $trace_id,
            run_id: $run_id,
            scenario_id: $scenario_id,
            bead_id: $bead_id,
            phase: $phase,
            event_type: $event_type,
            op: $op,
            page_id: null,
            frame_addr: null,
            from_state: null,
            to_state: null,
            outcome: $outcome,
            duration_ms: $duration_ms,
            duration_us: ($duration_ms * 1000),
            error_code: (if $error_code == "null" then null else $error_code end),
            seed: $seed,
            artifact_paths: [$log_path],
            log_sha256: $log_sha256,
            schema_ref: $schema_ref
        }' >>"${REPORT_JSONL}"
}

printf 'bead_id=%s level=INFO run_id=%s trace_id=%s scenario_id=%s phase=start seed=%s reference=%s\n' \
    "${BEAD_ID}" "${RUN_ID}" "${TRACE_ID}" "${SCENARIO_ID}" "${SEED}" "${LOG_SCHEMA_REF}"

start_ns="$(date +%s%N)"
set +e
(
    cd "${WORKSPACE_ROOT}"
    RUST_TEST_THREADS=1 "${TEST_CMD[@]}"
) >"${LOG_PATH}" 2>&1
exit_code=$?
set -e
end_ns="$(date +%s%N)"

duration_ms="$(( (end_ns - start_ns) / 1000000 ))"
log_sha256="$(sha256sum "${LOG_PATH}" | awk '{print $1}')"

if [[ ${exit_code} -eq 0 ]]; then
    emit_event "execute" "pilot_result" "swizzle" "pass" "${duration_ms}" "null" "${log_sha256}"
else
    emit_event "execute" "pilot_result" "swizzle" "fail" "${duration_ms}" "E_EXIT_${exit_code}" "${log_sha256}"
fi

if ! jq -e '
    has("trace_id")
    and has("run_id")
    and has("scenario_id")
    and has("bead_id")
    and has("op")
    and has("outcome")
    and has("duration_us")
    and has("seed")
    and has("error_code")
' "${REPORT_JSONL}" >/dev/null; then
    printf 'bead_id=%s level=ERROR run_id=%s trace_id=%s scenario_id=%s phase=validate case=log_schema_conformance_failed report=%s\n' \
        "${BEAD_ID}" "${RUN_ID}" "${TRACE_ID}" "${SCENARIO_ID}" "${REPORT_JSONL}"
    exit 1
fi

summary_outcome="pass"
summary_error="null"
if [[ ${exit_code} -ne 0 ]]; then
    summary_outcome="fail"
    summary_error="E_EXIT_${exit_code}"
fi
emit_event "summary" "run_summary" "swizzle" "${summary_outcome}" "${duration_ms}" "${summary_error}" "${log_sha256}"

printf 'bead_id=%s level=INFO run_id=%s trace_id=%s scenario_id=%s phase=summary outcome=%s duration_ms=%s log=%s report=%s log_sha256=%s\n' \
    "${BEAD_ID}" "${RUN_ID}" "${TRACE_ID}" "${SCENARIO_ID}" "${summary_outcome}" "${duration_ms}" "${LOG_PATH}" "${REPORT_JSONL}" "${log_sha256}"

if ${JSON_OUTPUT}; then
    jq -nc \
        --arg bead_id "${BEAD_ID}" \
        --arg run_id "${RUN_ID}" \
        --arg trace_id "${TRACE_ID}" \
        --arg scenario_id "${SCENARIO_ID}" \
        --arg outcome "${summary_outcome}" \
        --arg report_jsonl "${REPORT_JSONL}" \
        --arg log_path "${LOG_PATH}" \
        --arg log_sha256 "${log_sha256}" \
        --arg replay "RUST_TEST_THREADS=1 ${TEST_CMD[*]}" \
        '{
            bead_id: $bead_id,
            run_id: $run_id,
            trace_id: $trace_id,
            scenario_id: $scenario_id,
            outcome: $outcome,
            report_jsonl: $report_jsonl,
            log_path: $log_path,
            log_sha256: $log_sha256,
            replay: $replay
        }'
fi

exit "${exit_code}"
