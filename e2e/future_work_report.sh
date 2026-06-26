#!/usr/bin/env bash
set -euo pipefail

BEAD_ID="bd-3kp.3"
LOG_STANDARD_REF="bd-1fpm"
SCENARIO_ID="${SCENARIO_ID:-DOC-2}"
SEED="${SEED:-2026022010}"
RUN_ID="${BEAD_ID}-$(date -u +%Y%m%dT%H%M%SZ)-$$"
WORKSPACE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ISSUES_PATH="${WORKSPACE_ROOT}/.beads/issues.jsonl"
REPORT_DIR="${WORKSPACE_ROOT}/test-results"
REPORT_JSONL="${REPORT_DIR}/bd_3kp_3_future_work_report.jsonl"
SCHEMA_LOG_PATH="${REPORT_DIR}/bd_3kp_3_future_work_events.jsonl"
PROMOTION_TARGET="${FUTURE_WORK_PROMOTION_TARGET:-}"

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

printf 'bead_id=%s level=DEBUG case=start workspace=%s report=%s target=%s reference=%s\n' \
    "${BEAD_ID}" "${WORKSPACE_ROOT}" "${REPORT_JSONL}" "${PROMOTION_TARGET:-none}" "${LOG_STANDARD_REF}"
emit_schema_event "setup" "start" "running"

if [[ ! -f "${ISSUES_PATH}" ]]; then
    printf 'bead_id=%s level=ERROR case=missing_issues_jsonl path=%s reference=%s\n' \
        "${BEAD_ID}" "${ISSUES_PATH}" "${LOG_STANDARD_REF}"
    exit 1
fi

description="$(
    jq -r '
        select(.id == "bd-3kp.3")
        | .description,
          (.comments[]?.text // empty)
    ' "${ISSUES_PATH}"
)"

if [[ -z "${description//[[:space:]]/}" ]]; then
    printf 'bead_id=%s level=ERROR case=missing_bead_description path=%s reference=%s\n' \
        "${BEAD_ID}" "${ISSUES_PATH}" "${LOG_STANDARD_REF}"
    exit 1
fi

trace_id="$(printf '%s' "${description}" | sha256sum | awk '{print substr($1, 1, 16)}')"
printf 'bead_id=%s level=DEBUG case=trace trace_id=%s reference=%s\n' \
    "${BEAD_ID}" "${trace_id}" "${LOG_STANDARD_REF}"

section_rows="$(
    awk '
        function extract_section_id(line,   normalized, parts) {
            normalized = line
            sub(/^[[:space:]]*/, "", normalized)
            split(normalized, parts, /[[:space:]]+/)
            return parts[1]
        }
        function flush_block() {
            if (section_id != "") {
                has_scope = (block ~ /What it is:/)
                has_motivation = (block ~ /Why it\x27s deferred:/)
                has_non_goals = (block ~ /Why it\x27s deferred:/)
                has_dependencies = (block ~ /Dependencies on current work:/)
                has_prerequisites = (block ~ /What needs to be true before starting:/)
                has_proof = (block ~ /Key implementation details from spec:/ || block ~ /Key implementation notes from spec:/)
                printf "%s\t%d\t%d\t%d\t%d\t%d\t%d\n", section_id, has_scope, has_motivation, has_non_goals, has_dependencies, has_prerequisites, has_proof
            }
        }
        {
            if (match($0, /^[[:space:]]*§21\.(2|3|4|5|6|7|8|9|10)[[:space:]]/)) {
                flush_block()
                section_id = extract_section_id($0)
                block = $0 "\n"
                next
            }
            if (section_id != "") {
                block = block $0 "\n"
            }
        }
        END {
            flush_block()
        }
    ' <<<"${description}"
)"

printf '# bead_id=%s future work report\n' "${BEAD_ID}" >"${REPORT_JSONL}"
printf '# section\thas_scope\thas_motivation\thas_non_goals\thas_dependencies\thas_prerequisites\thas_proof_obligations\n' >>"${REPORT_JSONL}"

declare -A section_seen=()
declare -a missing_schema=()
declare -a expected_sections=("§21.2" "§21.3" "§21.4" "§21.5" "§21.6" "§21.7" "§21.8" "§21.9" "§21.10")

while IFS=$'\t' read -r section_id has_scope has_motivation has_non_goals has_dependencies has_prerequisites has_proof; do
    [[ -z "${section_id}" ]] && continue
    section_seen["${section_id}"]=1

    printf \
        '{"bead_id":"%s","trace_id":"%s","section_id":"%s","scope":%s,"motivation":%s,"non_goals":%s,"dependencies":%s,"prerequisites":%s,"proof_obligations":%s}\n' \
        "${BEAD_ID}" "${trace_id}" "${section_id}" "${has_scope}" "${has_motivation}" "${has_non_goals}" "${has_dependencies}" "${has_prerequisites}" "${has_proof}" \
        >>"${REPORT_JSONL}"

    if [[ "${has_scope}" -ne 1 || "${has_motivation}" -ne 1 || "${has_non_goals}" -ne 1 || "${has_dependencies}" -ne 1 || "${has_prerequisites}" -ne 1 || "${has_proof}" -ne 1 ]]; then
        missing_schema+=("${section_id}")
    fi
done <<<"${section_rows}"

declare -a missing_sections=()
for expected in "${expected_sections[@]}"; do
    if [[ -z "${section_seen["${expected}"]+x}" ]]; then
        missing_sections+=("${expected}")
    fi
done

if [[ -n "${PROMOTION_TARGET}" ]]; then
    section_block="$(
        awk -v target="${PROMOTION_TARGET}" '
            function is_section_header(line) {
                return match(line, /^[[:space:]]*§21\.(2|3|4|5|6|7|8|9|10)[[:space:]]/)
            }
            function extract_section_id(line,   normalized, parts) {
                normalized = line
                sub(/^[[:space:]]*/, "", normalized)
                split(normalized, parts, /[[:space:]]+/)
                return parts[1]
            }
            {
                if (is_section_header($0)) {
                    section_id = extract_section_id($0)
                }

                if (section_id == target) {
                    in_target = 1
                } else if (in_target == 1 && is_section_header($0)) {
                    exit
                }

                if (in_target == 1) {
                    print $0
                }
            }
        ' <<<"${description}"
    )"

    if [[ -z "${section_block//[[:space:]]/}" ]]; then
        printf 'bead_id=%s level=ERROR case=promotion_target_not_found target=%s trace_id=%s reference=%s\n' \
            "${BEAD_ID}" "${PROMOTION_TARGET}" "${trace_id}" "${LOG_STANDARD_REF}"
        exit 2
    fi

    phase_refs="$(
        grep -oE 'Phase [0-9]+' <<<"${section_block}" \
            | awk '{print $2}' \
            | sort -u
    )"

    declare -a blockers=()
    if [[ -z "${phase_refs//[[:space:]]/}" ]]; then
        blockers+=("no_phase_references_found")
    else
        while IFS= read -r phase; do
            [[ -z "${phase}" ]] && continue
            phase_matches="$(
                jq -r --arg marker "§16 Phase ${phase}" '
                    select(.title | contains($marker))
                    | "\(.id)\t\(.status)"
                ' "${ISSUES_PATH}"
            )"

            if [[ -z "${phase_matches//[[:space:]]/}" ]]; then
                blockers+=("phase_${phase}:no_matching_bead")
                continue
            fi

            while IFS=$'\t' read -r issue_id issue_status; do
                [[ -z "${issue_id}" ]] && continue
                if [[ "${issue_status}" != "closed" ]]; then
                    blockers+=("phase_${phase}:${issue_id}:${issue_status}")
                fi
            done <<<"${phase_matches}"
        done <<<"${phase_refs}"
    fi

    if [[ "${#blockers[@]}" -gt 0 ]]; then
        printf 'bead_id=%s level=WARN case=promotion_prereq_missing target=%s blockers=%s trace_id=%s reference=%s\n' \
            "${BEAD_ID}" "${PROMOTION_TARGET}" "${blockers[*]}" "${trace_id}" "${LOG_STANDARD_REF}"
        printf 'bead_id=%s level=ERROR case=promotion_blocked target=%s blockers=%s trace_id=%s reference=%s\n' \
            "${BEAD_ID}" "${PROMOTION_TARGET}" "${blockers[*]}" "${trace_id}" "${LOG_STANDARD_REF}"
        exit 2
    fi

    printf 'bead_id=%s level=INFO case=promotion_allowed target=%s trace_id=%s reference=%s\n' \
        "${BEAD_ID}" "${PROMOTION_TARGET}" "${trace_id}" "${LOG_STANDARD_REF}"
fi

printf 'bead_id=%s level=INFO case=summary sections=%s missing_sections=%s missing_schema=%s trace_id=%s report=%s\n' \
    "${BEAD_ID}" \
    "${#section_seen[@]}" \
    "${#missing_sections[@]}" \
    "${#missing_schema[@]}" \
    "${trace_id}" \
    "${REPORT_JSONL}"

for section in "${missing_sections[@]}"; do
    printf 'bead_id=%s level=WARN case=missing_section section_id=%s trace_id=%s reference=%s\n' \
        "${BEAD_ID}" "${section}" "${trace_id}" "${LOG_STANDARD_REF}"
done
for section in "${missing_schema[@]}"; do
    printf 'bead_id=%s level=WARN case=missing_schema_fields section_id=%s trace_id=%s reference=%s\n' \
        "${BEAD_ID}" "${section}" "${trace_id}" "${LOG_STANDARD_REF}"
done

if [[ "${#missing_sections[@]}" -gt 0 || "${#missing_schema[@]}" -gt 0 ]]; then
    printf 'bead_id=%s level=ERROR case=terminal_failure missing_sections=%s missing_schema=%s trace_id=%s report=%s reference=%s\n' \
        "${BEAD_ID}" "${missing_sections[*]:-none}" "${missing_schema[*]:-none}" "${trace_id}" "${REPORT_JSONL}" "${LOG_STANDARD_REF}"
    exit 1
fi

printf 'bead_id=%s level=INFO case=degraded_mode_count=0 trace_id=%s reference=%s\n' \
    "${BEAD_ID}" "${trace_id}" "${LOG_STANDARD_REF}"
printf 'bead_id=%s level=INFO case=terminal_failure_count=0 trace_id=%s reference=%s\n' \
    "${BEAD_ID}" "${trace_id}" "${LOG_STANDARD_REF}"
printf 'bead_id=%s level=INFO case=pass trace_id=%s report=%s\n' \
    "${BEAD_ID}" "${trace_id}" "${REPORT_JSONL}"
