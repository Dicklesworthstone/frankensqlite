#!/usr/bin/env bash
set -euo pipefail

BEAD_ID="bd-sxm2"
LOG_STANDARD_REF="bd-1fpm"
LOG_SCHEMA_VERSION="1.0.0"
SCENARIO_ID="SPEC-8"
SEED=202602130202
BACKEND="fsqlite"
SPEC_PATH="docs/planning/COMPREHENSIVE_SPEC_FOR_FRANKENSQLITE_V1.md"
WORKSPACE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_ID="${BEAD_ID}-$(date -u +%Y%m%dT%H%M%SZ)-$$"
REPORT_ROOT="${WORKSPACE_ROOT}/test-results/bd_sxm2"
REPORT_JSONL="${REPORT_ROOT}/${RUN_ID}.jsonl"
REPORT_REL="${REPORT_JSONL#${WORKSPACE_ROOT}/}"
WORKER="${HOSTNAME:-local}"
JSON_OUTPUT=false

if [[ "${1:-}" == "--json" ]]; then
    JSON_OUTPUT=true
fi

emit_event() {
    local phase="$1"
    local event_type="$2"
    local outcome="$3"
    local message="$4"
    local error_code="$5"
    local context_json="$6"
    local timestamp
    timestamp="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

    jq -nc \
        --arg schema_version "${LOG_SCHEMA_VERSION}" \
        --arg bead_id "${BEAD_ID}" \
        --arg run_id "${RUN_ID}" \
        --arg timestamp "${timestamp}" \
        --arg phase "${phase}" \
        --arg event_type "${event_type}" \
        --arg scenario_id "${SCENARIO_ID}" \
        --argjson seed "${SEED}" \
        --arg backend "${BACKEND}" \
        --arg worker "${WORKER}" \
        --arg outcome "${outcome}" \
        --arg message "${message}" \
        --arg error_code "${error_code}" \
        --arg log_standard_ref "${LOG_STANDARD_REF}" \
        --arg report_rel "${REPORT_REL}" \
        --argjson context "${context_json}" \
        '{
            schema_version: $schema_version,
            bead_id: $bead_id,
            run_id: $run_id,
            timestamp: $timestamp,
            phase: $phase,
            event_type: $event_type,
            scenario_id: $scenario_id,
            seed: $seed,
            backend: $backend,
            outcome: $outcome,
            error_code: (if $error_code == "" then null else $error_code end),
            artifact_paths: [$report_rel],
            context: ($context + {
                message: $message,
                worker: $worker,
                log_standard_ref: $log_standard_ref,
                artifact_paths: $report_rel
            })
        }' >>"${REPORT_JSONL}"
}

sanitize_case_name() {
    printf '%s' "$1" \
        | tr '[:lower:]-' '[:upper:]_' \
        | tr -cd 'A-Z0-9_'
}

log_line() {
    local level="$1"
    local case_name="$2"
    shift 2
    local details="$*"
    local phase="validate"
    local event_type="info"
    local outcome="info"
    local error_code=""

    case "${case_name}" in
        start)
            phase="setup"
            event_type="start"
            outcome="running"
            ;;
        summary)
            phase="report"
            event_type="info"
            outcome="info"
            ;;
        violations)
            phase="report"
            event_type="fail"
            outcome="fail"
            ;;
        pass)
            phase="report"
            event_type="pass"
            outcome="pass"
            ;;
        degraded_mode_count)
            phase="report"
            event_type="warn"
            outcome="warn"
            ;;
        terminal_failure_count)
            phase="report"
            event_type="info"
            outcome="pass"
            ;;
        missing_spec | missing_sections)
            phase="setup"
            event_type="fail"
            outcome="fail"
            ;;
    esac

    case "${level}" in
        WARN)
            event_type="warn"
            outcome="warn"
            ;;
        ERROR)
            if [[ "${case_name}" == "terminal_failure_count" ]]; then
                event_type="info"
                outcome="pass"
            elif [[ "${event_type}" != "fail" ]]; then
                event_type="error"
                outcome="fail"
            fi
            ;;
    esac

    if [[ "${event_type}" == "fail" || "${event_type}" == "error" ]]; then
        error_code="E_$(sanitize_case_name "${case_name}")"
    fi

    local context_json
    context_json="$(
        jq -nc \
            --arg level "${level}" \
            --arg case_name "${case_name}" \
            --arg details "${details}" \
            '{level: $level, case: $case_name, details: $details}'
    )"

    emit_event \
        "${phase}" \
        "${event_type}" \
        "${outcome}" \
        "${case_name}" \
        "${error_code}" \
        "${context_json}"

    printf 'bead_id=%s level=%s run_id=%s scenario_id=%s phase=%s event_type=%s case=%s %s reference=%s\n' \
        "${BEAD_ID}" "${level}" "${RUN_ID}" "${SCENARIO_ID}" "${phase}" "${event_type}" "${case_name}" "${details}" "${LOG_STANDARD_REF}"
}

mkdir -p "${REPORT_ROOT}"
: >"${REPORT_JSONL}"

if [[ ! -f "${SPEC_PATH}" ]]; then
    log_line "ERROR" "missing_spec" "path=${SPEC_PATH}"
    if ${JSON_OUTPUT}; then
        cat <<ENDJSON
{
  "bead_id": "${BEAD_ID}",
  "run_id": "${RUN_ID}",
  "status": "fail",
  "report_jsonl": "${REPORT_JSONL}"
}
ENDJSON
    fi
    exit 1
fi

mapfile -t EXPECTED_CRATES <<'EOF'
fsqlite-ast
fsqlite-btree
fsqlite-cli
fsqlite-core
fsqlite-error
fsqlite-ext-fts3
fsqlite-ext-fts5
fsqlite-ext-icu
fsqlite-ext-json
fsqlite-ext-misc
fsqlite-ext-rtree
fsqlite-ext-session
fsqlite-func
fsqlite-harness
fsqlite-mvcc
fsqlite-pager
fsqlite-parser
fsqlite-planner
fsqlite-types
fsqlite-vdbe
fsqlite-vfs
fsqlite-wal
fsqlite
EOF

is_concise_allowed() {
    local crate_name="$1"
    [[ "${crate_name}" == fsqlite-ext-* ]] \
        || [[ "${crate_name}" == "fsqlite" ]] \
        || [[ "${crate_name}" == "fsqlite-cli" ]] \
        || [[ "${crate_name}" == "fsqlite-harness" ]] \
        || [[ "${crate_name}" == "fsqlite-error" ]]
}

extract_section_8_3() {
    awk '
        /^### 8\.3 Per-Crate Detailed Descriptions/ { in_section = 1; next }
        /^### 8\.4 Dependency Edges with Rationale/ { in_section = 0 }
        in_section { print }
    ' "${SPEC_PATH}"
}

extract_section_8_4() {
    awk '
        /^### 8\.4 Dependency Edges with Rationale/ { in_section = 1; next }
        /^### 8\.5 Feature Flags/ { in_section = 0 }
        in_section { print }
    ' "${SPEC_PATH}"
}

extract_crate_block() {
    local section="$1"
    local crate_name="$2"
    local marker="**\`${crate_name}\`**"
    printf '%s\n' "${section}" | awk -v marker="${marker}" '
        index($0, marker) > 0 { in_block = 1; next }
        in_block && $0 ~ /^\*\*`/ { exit }
        in_block { print }
    '
}

section_8_3="$(extract_section_8_3)"
section_8_4="$(extract_section_8_4)"

if [[ -z "${section_8_3}" || -z "${section_8_4}" ]]; then
    log_line "ERROR" "missing_sections" "spec=${SPEC_PATH}"
    if ${JSON_OUTPUT}; then
        cat <<ENDJSON
{
  "bead_id": "${BEAD_ID}",
  "run_id": "${RUN_ID}",
  "status": "fail",
  "report_jsonl": "${REPORT_JSONL}"
}
ENDJSON
    fi
    exit 1
fi

log_line "DEBUG" "start" \
    "expected_crates=${#EXPECTED_CRATES[@]} spec=${SPEC_PATH} report=${REPORT_JSONL}"

declare -a violations=()
described_count=0
module_listed_count=0

for crate_name in "${EXPECTED_CRATES[@]}"; do
    marker="**\`${crate_name}\`**"
    marker_count="$(printf '%s\n' "${section_8_3}" | rg -F "${marker}" -c || true)"

    if [[ "${marker_count}" -ne 1 ]]; then
        violations+=("description_marker_count:${crate_name}:${marker_count}")
        continue
    fi

    described_count=$((described_count + 1))
    block="$(extract_crate_block "${section_8_3}" "${crate_name}")"
    block_len="$(printf '%s' "${block}" | wc -c | awk '{print $1}')"

    if [[ "${block_len}" -lt 70 ]]; then
        violations+=("description_too_short:${crate_name}:${block_len}")
    fi

    first_nonempty_line="$(printf '%s\n' "${block}" | awk 'NF { print; exit }')"
    if [[ -z "${first_nonempty_line}" ]]; then
        violations+=("summary_line_missing:${crate_name}")
    fi

    has_modules=0
    if printf '%s\n' "${block}" | rg -qi 'modules:'; then
        has_modules=1
        module_listed_count=$((module_listed_count + 1))
        module_line_count="$(printf '%s\n' "${block}" | rg -c '^- `[^`]+\.rs`' || true)"
        if [[ "${module_line_count}" -lt 3 || "${module_line_count}" -gt 12 ]]; then
            violations+=("module_count_out_of_range:${crate_name}:${module_line_count}")
        fi
    fi

    has_dependency_signal=0
    if printf '%s\n' "${block}" | rg -q 'Dependency rationale:|depends on'; then
        has_dependency_signal=1
    elif printf '%s\n' "${section_8_4}" | rg -q "${crate_name}"; then
        has_dependency_signal=1
    elif is_concise_allowed "${crate_name}"; then
        has_dependency_signal=1
    fi

    if [[ "${has_dependency_signal}" -eq 0 ]]; then
        violations+=("dependency_direction_missing:${crate_name}")
    fi

    log_line "DEBUG" "crate_scan" \
        "crate=${crate_name} described=1 modules=${has_modules} block_len=${block_len}"
done

log_line "INFO" "summary" \
    "described_count=${described_count} module_listed_count=${module_listed_count} expected=${#EXPECTED_CRATES[@]}"

summary_sha256="$(sha256sum "${REPORT_JSONL}" | awk '{print $1}')"

if [[ "${#violations[@]}" -gt 0 ]]; then
    log_line "WARN" "degraded_mode_count" "count=${#violations[@]}"
    log_line "ERROR" "violations" "items=${violations[*]} report_sha256=${summary_sha256}"
    if ${JSON_OUTPUT}; then
        cat <<ENDJSON
{
  "bead_id": "${BEAD_ID}",
  "run_id": "${RUN_ID}",
  "status": "fail",
  "violations": [$(printf '"%s",' "${violations[@]}" | sed 's/,$//')],
  "report_jsonl": "${REPORT_JSONL}",
  "report_sha256": "${summary_sha256}"
}
ENDJSON
    fi
    exit 1
fi

log_line "INFO" "degraded_mode_count" "count=0"
log_line "INFO" "terminal_failure_count" "count=0"
log_line "INFO" "pass" "report=${REPORT_JSONL} report_sha256=${summary_sha256}"

if ${JSON_OUTPUT}; then
    cat <<ENDJSON
{
  "bead_id": "${BEAD_ID}",
  "run_id": "${RUN_ID}",
  "status": "pass",
  "violations": [],
  "report_jsonl": "${REPORT_JSONL}",
  "report_sha256": "${summary_sha256}"
}
ENDJSON
fi
