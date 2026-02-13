#!/usr/bin/env bash
set -euo pipefail

BEAD_ID="bd-2ddl"
LOG_STANDARD_REF="bd-1fpm"
LOG_SCHEMA_VERSION="1.0.0"
SCENARIO_ID="INFRA-6"
SEED=202602130201
BACKEND="fsqlite"
WORKSPACE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_ID="${BEAD_ID}-$(date -u +%Y%m%dT%H%M%SZ)-$$"
REPORT_ROOT="${WORKSPACE_ROOT}/test-results/bd_2ddl"
REPORT_JSONL="${REPORT_ROOT}/${RUN_ID}.jsonl"
REPORT_REL="${REPORT_JSONL#${WORKSPACE_ROOT}/}"
LOG_DIR="${REPORT_ROOT}/logs/${RUN_ID}"
WORKER="${HOSTNAME:-local}"
RUN_PER_CRATE_TESTS="${BD_2DDL_RUN_PER_CRATE_TESTS:-1}"
RUN_WORKSPACE_TEST="${BD_2DDL_RUN_WORKSPACE_TEST:-1}"
JSON_OUTPUT=false

if [[ "${1:-}" == "--json" ]]; then
    JSON_OUTPUT=true
fi

CRATES=(
    "fsqlite-types"
    "fsqlite-error"
    "fsqlite-vfs"
    "fsqlite-pager"
    "fsqlite-wal"
    "fsqlite-mvcc"
    "fsqlite-btree"
    "fsqlite-ast"
    "fsqlite-parser"
    "fsqlite-planner"
    "fsqlite-vdbe"
    "fsqlite-func"
    "fsqlite-ext-fts3"
    "fsqlite-ext-fts5"
    "fsqlite-ext-rtree"
    "fsqlite-ext-json"
    "fsqlite-ext-session"
    "fsqlite-ext-icu"
    "fsqlite-ext-misc"
    "fsqlite-core"
    "fsqlite"
    "fsqlite-cli"
    "fsqlite-harness"
)

emit_event() {
    local phase="$1"
    local event_type="$2"
    local outcome="$3"
    local message="$4"
    local error_code="$5"
    local artifact_paths_json="$6"
    local context_json="$7"
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
        --argjson artifact_paths "${artifact_paths_json}" \
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
            artifact_paths: $artifact_paths,
            context: ($context + {
                message: $message,
                worker: $worker,
                log_standard_ref: $log_standard_ref,
                artifact_paths: ($artifact_paths | join(","))
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
        pass)
            phase="report"
            event_type="pass"
            outcome="pass"
            ;;
        terminal_failure)
            phase="report"
            event_type="fail"
            outcome="fail"
            ;;
        terminal_failure_count)
            phase="report"
            event_type="info"
            outcome="pass"
            ;;
        workspace_summary)
            phase="report"
            event_type="info"
            outcome="info"
            ;;
        degraded_mode | degraded_mode_count | missing_test_category | public_api_coverage_gap)
            phase="validate"
            event_type="warn"
            outcome="warn"
            ;;
        crate_test_failed | workspace_test_failed | missing_workspace_manifest | missing_crate_dir)
            phase="validate"
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

    local log_path=""
    if [[ "${details}" =~ (^|[[:space:]])log=([^[:space:]]+) ]]; then
        log_path="${BASH_REMATCH[2]}"
    fi

    local artifact_paths_json
    if [[ -n "${log_path}" ]]; then
        local log_rel="${log_path#${WORKSPACE_ROOT}/}"
        artifact_paths_json="$(
            jq -nc \
                --arg report "${REPORT_REL}" \
                --arg log "${log_rel}" \
                '[ $report, $log ] | unique'
        )"
    else
        artifact_paths_json="$(jq -nc --arg report "${REPORT_REL}" '[ $report ]')"
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
        "${artifact_paths_json}" \
        "${context_json}"

    printf 'bead_id=%s level=%s run_id=%s scenario_id=%s phase=%s event_type=%s case=%s %s reference=%s\n' \
        "${BEAD_ID}" "${level}" "${RUN_ID}" "${SCENARIO_ID}" "${phase}" "${event_type}" "${case_name}" "${details}" "${LOG_STANDARD_REF}"
}

count_pattern() {
    local pattern="$1"
    shift

    local total=0
    local path
    local count
    for path in "$@"; do
        if [[ -d "${path}" ]]; then
            count="$(rg -n --glob '*.rs' -e "${pattern}" "${path}" 2>/dev/null | wc -l | tr -d '[:space:]')"
            total=$((total + count))
        fi
    done

    printf '%s' "${total}"
}

collect_public_fn_names() {
    local src_dir="$1"
    if [[ ! -d "${src_dir}" ]]; then
        return 0
    fi

    rg -n --glob '*.rs' -e '^[[:space:]]*pub(\([^)]*\))?[[:space:]]+fn[[:space:]]+[A-Za-z_][A-Za-z0-9_]*' "${src_dir}" 2>/dev/null \
        | sed -E 's/.*pub(\([^)]*\))?[[:space:]]+fn[[:space:]]+([A-Za-z_][A-Za-z0-9_]*).*/\2/' \
        | sort -u
}

collect_test_corpus() {
    local crate_dir="$1"

    if [[ -d "${crate_dir}/tests" ]]; then
        rg -N --glob '*.rs' -e '.' "${crate_dir}/tests" 2>/dev/null || true
    fi

    if [[ -d "${crate_dir}/src" ]]; then
        while IFS= read -r src_file; do
            awk '
                BEGIN { in_test = 0 }
                /#\[cfg\(test\)\]/ { in_test = 1 }
                { if (in_test == 1) print }
            ' "${src_file}"
        done < <(find "${crate_dir}/src" -type f -name '*.rs' | sort)
    fi
}

mkdir -p "${REPORT_ROOT}" "${LOG_DIR}"
: >"${REPORT_JSONL}"

log_line "DEBUG" "start" \
    "workspace=${WORKSPACE_ROOT} report=${REPORT_JSONL} per_crate_tests=${RUN_PER_CRATE_TESTS} workspace_test=${RUN_WORKSPACE_TEST}"

if [[ ! -f "${WORKSPACE_ROOT}/Cargo.toml" ]]; then
    log_line "ERROR" "missing_workspace_manifest" "path=${WORKSPACE_ROOT}/Cargo.toml"
    exit 1
fi

declare -a failing_crates=()
declare -a zero_test_crates=()
declare -a missing_public_api_test_crates=()
declare -A missing_categories_by_crate=()

for crate in "${CRATES[@]}"; do
    crate_dir="${WORKSPACE_ROOT}/crates/${crate}"
    if [[ ! -d "${crate_dir}" ]]; then
        log_line "ERROR" "missing_crate_dir" "crate=${crate} path=${crate_dir}"
        failing_crates+=("${crate}")
        continue
    fi

    unit_count="$(count_pattern '#\[test\]' "${crate_dir}/src" "${crate_dir}/tests")"
    prop_count="$(count_pattern 'proptest!' "${crate_dir}/src" "${crate_dir}/tests")"
    conformance_count="$(count_pattern 'conformance' "${crate_dir}/src" "${crate_dir}/tests")"

    fuzz_count=0
    if [[ -d "${crate_dir}/fuzz" ]]; then
        fuzz_count="$(find "${crate_dir}/fuzz" -type f -name '*.rs' | wc -l | tr -d '[:space:]')"
    fi

    mapfile -t public_fn_names < <(collect_public_fn_names "${crate_dir}/src")
    test_corpus="$(collect_test_corpus "${crate_dir}")"

    public_fn_total="${#public_fn_names[@]}"
    public_fn_covered=0
    missing_api_preview=()
    for fn_name in "${public_fn_names[@]}"; do
        if grep -Eq "\\b${fn_name}\\b" <<<"${test_corpus}"; then
            public_fn_covered=$((public_fn_covered + 1))
        else
            if [[ "${#missing_api_preview[@]}" -lt 5 ]]; then
                missing_api_preview+=("${fn_name}")
            fi
        fi
    done

    missing_category=()
    if [[ "${unit_count}" -eq 0 ]]; then
        missing_category+=("unit")
        zero_test_crates+=("${crate}")
    fi
    if [[ "${prop_count}" -eq 0 ]]; then
        missing_category+=("prop")
    fi
    if [[ "${conformance_count}" -eq 0 ]]; then
        missing_category+=("conformance")
    fi
    if [[ "${fuzz_count}" -eq 0 ]]; then
        missing_category+=("fuzz")
    fi
    if [[ "${public_fn_total}" -gt 0 && "${public_fn_covered}" -lt "${public_fn_total}" ]]; then
        missing_public_api_test_crates+=("${crate}")
    fi

    if [[ "${#missing_category[@]}" -gt 0 ]]; then
        missing_categories_by_crate["${crate}"]="$(IFS=,; echo "${missing_category[*]}")"
        log_line "WARN" "missing_test_category" \
            "crate=${crate} missing=${missing_categories_by_crate["${crate}"]}"
    fi
    if [[ "${public_fn_total}" -gt 0 && "${public_fn_covered}" -lt "${public_fn_total}" ]]; then
        log_line "WARN" "public_api_coverage_gap" \
            "crate=${crate} covered=${public_fn_covered}/${public_fn_total} sample_missing=${missing_api_preview[*]:-none}"
    fi

    cargo_test_exit=0
    crate_log="${LOG_DIR}/${crate}.log"
    if [[ "${RUN_PER_CRATE_TESTS}" == "1" ]]; then
        set +e
        (
            cd "${WORKSPACE_ROOT}" || exit 1
            cargo test -p "${crate}"
        ) >"${crate_log}" 2>&1
        cargo_test_exit=$?
        set -e

        if [[ "${cargo_test_exit}" -ne 0 ]]; then
            failing_crates+=("${crate}")
            log_line "ERROR" "crate_test_failed" \
                "crate=${crate} exit=${cargo_test_exit} log=${crate_log}"
        fi
    fi

    log_line "INFO" "crate_matrix_summary" \
        "crate=${crate} unit=${unit_count} prop=${prop_count} conformance=${conformance_count} fuzz=${fuzz_count} public_fn_covered=${public_fn_covered}/${public_fn_total} cargo_test_exit=${cargo_test_exit}"
done

workspace_test_exit=0
workspace_log="${LOG_DIR}/workspace.log"
if [[ "${RUN_WORKSPACE_TEST}" == "1" ]]; then
    set +e
    (
        cd "${WORKSPACE_ROOT}" || exit 1
        cargo test --workspace
    ) >"${workspace_log}" 2>&1
    workspace_test_exit=$?
    set -e

    if [[ "${workspace_test_exit}" -ne 0 ]]; then
        log_line "ERROR" "workspace_test_failed" \
            "exit=${workspace_test_exit} log=${workspace_log}"
    fi
fi

log_line "INFO" "workspace_summary" \
    "crates=${#CRATES[@]} failing_crates=${#failing_crates[@]} zero_test_crates=${#zero_test_crates[@]} missing_public_api_test_crates=${#missing_public_api_test_crates[@]} workspace_test_exit=${workspace_test_exit} report=${REPORT_JSONL}"

summary_sha256="$(sha256sum "${REPORT_JSONL}" | awk '{print $1}')"

if [[ "${#failing_crates[@]}" -gt 0 ]]; then
    log_line "WARN" "degraded_mode" "type=crate_failures crates=${failing_crates[*]}"
fi
if [[ "${#zero_test_crates[@]}" -gt 0 ]]; then
    log_line "WARN" "degraded_mode" "type=zero_test_crates crates=${zero_test_crates[*]}"
fi
if [[ "${#missing_public_api_test_crates[@]}" -gt 0 ]]; then
    log_line "WARN" "degraded_mode" "type=public_api_coverage_gap crates=${missing_public_api_test_crates[*]}"
fi

if [[ "${#failing_crates[@]}" -gt 0 || "${#zero_test_crates[@]}" -gt 0 || "${#missing_public_api_test_crates[@]}" -gt 0 || "${workspace_test_exit}" -ne 0 ]]; then
    log_line "ERROR" "terminal_failure" \
        "failing_crates=${failing_crates[*]:-none} zero_test_crates=${zero_test_crates[*]:-none} missing_public_api_test_crates=${missing_public_api_test_crates[*]:-none} workspace_test_exit=${workspace_test_exit} report=${REPORT_JSONL} report_sha256=${summary_sha256}"
    if ${JSON_OUTPUT}; then
        if (( ${#failing_crates[@]} > 0 )); then
            failing_crates_json="$(printf '%s\n' "${failing_crates[@]}" | jq -R . | jq -s .)"
        else
            failing_crates_json='[]'
        fi
        cat <<ENDJSON
{
  "bead_id": "${BEAD_ID}",
  "run_id": "${RUN_ID}",
  "status": "fail",
  "report_jsonl": "${REPORT_JSONL}",
  "report_sha256": "${summary_sha256}",
  "failing_crates": ${failing_crates_json}
}
ENDJSON
    fi
    exit 1
fi

log_line "WARN" "degraded_mode_count" "count=0"
log_line "ERROR" "terminal_failure_count" "count=0"
log_line "INFO" "pass" "report=${REPORT_JSONL} report_sha256=${summary_sha256}"

if ${JSON_OUTPUT}; then
    cat <<ENDJSON
{
  "bead_id": "${BEAD_ID}",
  "run_id": "${RUN_ID}",
  "status": "pass",
  "report_jsonl": "${REPORT_JSONL}",
  "report_sha256": "${summary_sha256}",
  "failing_crates": []
}
ENDJSON
fi
