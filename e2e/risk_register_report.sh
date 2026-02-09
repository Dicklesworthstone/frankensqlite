#!/usr/bin/env bash
set -euo pipefail

BEAD_ID="bd-3kp.2"
LOG_STANDARD_REF="bd-1fpm"
WORKSPACE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ISSUES_PATH="${WORKSPACE_ROOT}/.beads/issues.jsonl"
REPORT_DIR="${WORKSPACE_ROOT}/test-results"
REPORT_JSONL="${REPORT_DIR}/bd_3kp_2_risk_register_report.jsonl"

printf 'bead_id=%s level=DEBUG case=start workspace=%s report=%s reference=%s\n' \
    "${BEAD_ID}" "${WORKSPACE_ROOT}" "${REPORT_JSONL}" "${LOG_STANDARD_REF}"

if [[ ! -f "${ISSUES_PATH}" ]]; then
    printf 'bead_id=%s level=ERROR case=missing_issues_jsonl path=%s reference=%s\n' \
        "${BEAD_ID}" "${ISSUES_PATH}" "${LOG_STANDARD_REF}"
    exit 1
fi

mkdir -p "${REPORT_DIR}"

description="$(
    jq -r '
        select(.id == "bd-3kp.2")
        | .description,
          (.comments[]?.text // empty)
    ' "${ISSUES_PATH}"
)"

if [[ -z "${description//[[:space:]]/}" ]]; then
    printf 'bead_id=%s level=ERROR case=missing_bead_description path=%s reference=%s\n' \
        "${BEAD_ID}" "${ISSUES_PATH}" "${LOG_STANDARD_REF}"
    exit 1
fi

risk_rows="$(
    awk '
        function flush_block() {
            if (risk_id != "") {
                has_mitigation = (block ~ /Mitigation strategies/)
                has_monitoring = (block ~ /Monitoring approach/)
                has_tests = (block ~ /Test requirements/ || block ~ /test_/)
                has_triggers = (block ~ /Trigger conditions/)
                printf "%s\t%d\t%d\t%d\t%d\n", risk_id, has_mitigation, has_monitoring, has_tests, has_triggers
            }
        }
        {
            if (match($0, /^[[:space:]]*R[0-9]+\./)) {
                flush_block()
                risk_id = $0
                sub(/^[[:space:]]*/, "", risk_id)
                sub(/\..*/, "", risk_id)
                block = $0 "\n"
                next
            }
            if (risk_id != "") {
                block = block $0 "\n"
            }
        }
        END {
            flush_block()
        }
    ' <<<"${description}"
)"

printf '# bead_id=%s risk register summary\n' "${BEAD_ID}" >"${REPORT_JSONL}"
printf '# risk_id\thas_mitigation\thas_monitoring\thas_tests\thas_triggers\n' >>"${REPORT_JSONL}"

declare -A risk_counts=()
declare -A high_priority=(
    [R1]=1
    [R2]=1
    [R5]=1
    [R7]=1
    [R8]=1
)
declare -a missing_mitigations=()
declare -a missing_signals=()

while IFS=$'\t' read -r risk_id has_mitigation has_monitoring has_tests has_triggers; do
    [[ -z "${risk_id}" ]] && continue
    risk_counts["${risk_id}"]=$(( ${risk_counts["${risk_id}"]:-0} + 1 ))

    printf \
        '{"bead_id":"%s","risk_id":"%s","has_mitigation":%s,"has_monitoring":%s,"has_tests":%s,"has_triggers":%s}\n' \
        "${BEAD_ID}" "${risk_id}" "${has_mitigation}" "${has_monitoring}" "${has_tests}" "${has_triggers}" \
        >>"${REPORT_JSONL}"

    if [[ "${has_mitigation}" -ne 1 ]]; then
        missing_mitigations+=("${risk_id}")
    fi

    if [[ -n "${high_priority["${risk_id}"]+x}" ]]; then
        if [[ "${has_monitoring}" -ne 1 || "${has_tests}" -ne 1 || "${has_triggers}" -ne 1 ]]; then
            missing_signals+=("${risk_id}")
        fi
    fi
done <<<"${risk_rows}"

declare -a missing_risk_ids=()
for expected in R1 R2 R3 R4 R5 R6 R7 R8; do
    if [[ -z "${risk_counts["${expected}"]+x}" ]]; then
        missing_risk_ids+=("${expected}")
    fi
done

declare -a duplicate_risk_ids=()
for risk_id in "${!risk_counts[@]}"; do
    if [[ "${risk_counts["${risk_id}"]}" -gt 1 ]]; then
        duplicate_risk_ids+=("${risk_id}")
    fi
done

printf 'bead_id=%s level=INFO case=summary total_risks=%s missing_ids=%s duplicate_ids=%s missing_mitigations=%s missing_signals=%s report=%s\n' \
    "${BEAD_ID}" \
    "${#risk_counts[@]}" \
    "${#missing_risk_ids[@]}" \
    "${#duplicate_risk_ids[@]}" \
    "${#missing_mitigations[@]}" \
    "${#missing_signals[@]}" \
    "${REPORT_JSONL}"

for risk_id in "${missing_risk_ids[@]}"; do
    printf 'bead_id=%s level=WARN case=missing_risk_id risk_id=%s reference=%s\n' \
        "${BEAD_ID}" "${risk_id}" "${LOG_STANDARD_REF}"
done
for risk_id in "${duplicate_risk_ids[@]}"; do
    printf 'bead_id=%s level=WARN case=duplicate_risk_id risk_id=%s count=%s reference=%s\n' \
        "${BEAD_ID}" "${risk_id}" "${risk_counts["${risk_id}"]}" "${LOG_STANDARD_REF}"
done
for risk_id in "${missing_mitigations[@]}"; do
    printf 'bead_id=%s level=WARN case=missing_mitigation_pointer risk_id=%s reference=%s\n' \
        "${BEAD_ID}" "${risk_id}" "${LOG_STANDARD_REF}"
done
for risk_id in "${missing_signals[@]}"; do
    printf 'bead_id=%s level=WARN case=missing_signal risk_id=%s reference=%s\n' \
        "${BEAD_ID}" "${risk_id}" "${LOG_STANDARD_REF}"
done

if [[ "${#missing_risk_ids[@]}" -gt 0 || "${#duplicate_risk_ids[@]}" -gt 0 || "${#missing_mitigations[@]}" -gt 0 || "${#missing_signals[@]}" -gt 0 ]]; then
    printf 'bead_id=%s level=ERROR case=terminal_failure missing_ids=%s duplicate_ids=%s missing_mitigations=%s missing_signals=%s report=%s reference=%s\n' \
        "${BEAD_ID}" \
        "${missing_risk_ids[*]:-none}" \
        "${duplicate_risk_ids[*]:-none}" \
        "${missing_mitigations[*]:-none}" \
        "${missing_signals[*]:-none}" \
        "${REPORT_JSONL}" \
        "${LOG_STANDARD_REF}"
    exit 1
fi

printf 'bead_id=%s level=WARN case=degraded_mode_count=0 reference=%s\n' "${BEAD_ID}" "${LOG_STANDARD_REF}"
printf 'bead_id=%s level=ERROR case=terminal_failure_count=0 reference=%s\n' "${BEAD_ID}" "${LOG_STANDARD_REF}"
printf 'bead_id=%s level=INFO case=pass report=%s\n' "${BEAD_ID}" "${REPORT_JSONL}"
