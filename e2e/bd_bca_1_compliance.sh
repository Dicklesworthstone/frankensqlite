#!/usr/bin/env bash
set -euo pipefail

BEAD_ID="bd-bca.1"
SCENARIO_ID="${SCENARIO_ID:-CON-1}"
SEED="${SEED:-2026022004}"
RUN_ID="${BEAD_ID}-$(date -u +%Y%m%dT%H%M%SZ)-$$"
LOG_STANDARD_REF="${LOG_STANDARD_REF:-docs/e2e_shell_script_log_profile.json}"
WORKSPACE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ISSUES_PATH="${WORKSPACE_ROOT}/.beads/issues.jsonl"
TEST_TARGET="bd_bca_1_phase5_compliance"
REPORT_DIR="${WORKSPACE_ROOT}/test-results"
SCHEMA_LOG_PATH="${REPORT_DIR}/bd_bca_1_compliance_events.jsonl"

mkdir -p "${REPORT_DIR}"
: >"${SCHEMA_LOG_PATH}"

emit_schema_event() {
    local phase="$1"
    local event_type="$2"
    local outcome="$3"
    local timestamp
    timestamp="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

    printf '{"run_id":"%s","timestamp":"%s","phase":"%s","event_type":"%s","scenario_id":"%s","seed":"%s","context":{"bead_id":"%s","outcome":"%s","log_standard_ref":"%s","schema_log_path":"%s"}}\n' \
        "${RUN_ID}" "${timestamp}" "${phase}" "${event_type}" "${SCENARIO_ID}" "${SEED}" "${BEAD_ID}" "${outcome}" "${LOG_STANDARD_REF}" "${SCHEMA_LOG_PATH}" \
        >>"${SCHEMA_LOG_PATH}"
}

on_exit() {
    local exit_code=$?
    if [[ ${exit_code} -eq 0 ]]; then
        emit_schema_event "report" "pass" "pass"
    else
        emit_schema_event "report" "fail" "fail"
    fi
}
trap on_exit EXIT

printf 'bead_id=%s level=DEBUG case=start workspace=%s target=%s\n' \
    "${BEAD_ID}" "${WORKSPACE_ROOT}" "${TEST_TARGET}"
emit_schema_event "setup" "start" "running"

if [[ ! -f "${ISSUES_PATH}" ]]; then
    printf 'bead_id=%s level=ERROR case=missing_issues_jsonl path=%s\n' "${BEAD_ID}" "${ISSUES_PATH}"
    exit 1
fi

description="$(
    jq -r '
        select(.id == "bd-bca.1")
        | .description,
          (.comments[]?.text // empty)
    ' "${ISSUES_PATH}" | tr '\n' ' '
)"

if [[ -z "${description// }" ]]; then
    printf 'bead_id=%s level=ERROR case=missing_bead_description path=%s\n' "${BEAD_ID}" "${ISSUES_PATH}"
    exit 1
fi

required_tokens=(
    "test_bd_bca_1_unit_compliance_gate"
    "prop_bd_bca_1_structure_compliance"
    "test_e2e_bd_bca_1"
    "test_e2e_bd_bca_1_compliance"
    "DEBUG"
    "INFO"
    "WARN"
    "ERROR"
    "bd-1fpm"
)

declare -a missing_tokens=()
for token in "${required_tokens[@]}"; do
    if ! rg -Fq "${token}" <<<"${description}"; then
        missing_tokens+=("${token}")
    fi
done

printf \
    'bead_id=%s level=INFO case=description_scan required=%s missing=%s\n' \
    "${BEAD_ID}" "${#required_tokens[@]}" "${#missing_tokens[@]}"

if [[ "${#missing_tokens[@]}" -gt 0 ]]; then
    printf 'bead_id=%s level=WARN case=degraded_mode_count=%s reference=bd-1fpm\n' \
        "${BEAD_ID}" "${#missing_tokens[@]}"
    printf 'bead_id=%s level=ERROR case=missing_tokens items=%s\n' \
        "${BEAD_ID}" "${missing_tokens[*]}"
    exit 1
fi

if (cd "${WORKSPACE_ROOT}" && cargo test -p fsqlite-harness --test "${TEST_TARGET}" -- --nocapture); then
    printf 'bead_id=%s level=INFO case=degraded_mode_count=0 reference=bd-1fpm\n' "${BEAD_ID}"
    printf 'bead_id=%s level=INFO case=terminal_failure_count=0 reference=bd-1fpm\n' "${BEAD_ID}"
    printf 'bead_id=%s level=INFO case=pass\n' "${BEAD_ID}"
    exit 0
fi

printf 'bead_id=%s level=WARN case=degraded_mode_count=1 reference=bd-1fpm\n' "${BEAD_ID}"
printf 'bead_id=%s level=ERROR case=harness_test_failed target=%s\n' "${BEAD_ID}" "${TEST_TARGET}"
exit 1
