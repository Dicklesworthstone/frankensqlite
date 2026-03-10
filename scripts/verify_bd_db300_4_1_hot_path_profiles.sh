#!/usr/bin/env bash
# verify_bd_db300_4_1_hot_path_profiles.sh — archived hot-path profile synthesis
#
# Rebuilds a structured Track D / D1 report from the existing Beads benchmark
# workspace under sample_sqlite_db_files/working/beads_bench_20260310.
#
# Outputs:
#   artifacts/perf/bd-db300.4.1/
#     events.jsonl
#     scenario_profiles.json
#     actionable_ranking.json
#     benchmark_context.json
#     report.json
#     summary.md
#     raw/*.perf.report.txt
#     raw/*.top_symbols.tsv
#     raw/*.scenario.json

set -euo pipefail

WORKSPACE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BEAD_ID="bd-db300.4.1"
RUN_ID="${BEAD_ID}-$(date -u +%Y%m%dT%H%M%SZ)-$$"
GENERATED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
BENCH_ROOT_DEFAULT="${WORKSPACE_ROOT}/sample_sqlite_db_files/working/beads_bench_20260310"
BENCH_ROOT="${BENCH_ROOT:-${BENCH_ROOT_DEFAULT}}"
OUTPUT_DIR="${OUTPUT_DIR:-${WORKSPACE_ROOT}/artifacts/perf/${BEAD_ID}}"
RAW_DIR="${OUTPUT_DIR}/raw"
LOG_FILE="${OUTPUT_DIR}/events.jsonl"
SCENARIO_PROFILES_JSON="${OUTPUT_DIR}/scenario_profiles.json"
ACTIONABLE_RANKING_JSON="${OUTPUT_DIR}/actionable_ranking.json"
BENCHMARK_CONTEXT_JSON="${OUTPUT_DIR}/benchmark_context.json"
REPORT_JSON="${OUTPUT_DIR}/report.json"
SUMMARY_MD="${OUTPUT_DIR}/summary.md"

mkdir -p "${RAW_DIR}"
: > "${LOG_FILE}"

log_event() {
    local level="$1"
    local stage="$2"
    local message="$3"
    printf '{"run_id":"%s","bead_id":"%s","level":"%s","stage":"%s","message":"%s","ts":"%s"}\n' \
        "${RUN_ID}" "${BEAD_ID}" "${level}" "${stage}" "${message}" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
        >> "${LOG_FILE}"
}

require_file() {
    local path="$1"
    if [[ ! -f "${path}" ]]; then
        log_event "ERROR" "inputs" "missing required input: ${path}"
        echo "ERROR: missing required input: ${path}" >&2
        exit 1
    fi
}

json_string() {
    jq -Rn --arg value "$1" '$value'
}

top_symbols_from_perf() {
    local perf_data="$1"
    local report_txt="$2"
    local tsv_out="$3"

    perf report --stdio --no-children -F overhead,symbol,dso -t '|' \
        -i "${perf_data}" --percent-limit 0.1 > "${report_txt}"

    awk -F'|' '
        /^[[:space:]]*[0-9]+\.[0-9]+%[[:space:]]*\|/ {
            pct = $1;
            sym = $2;
            dso = $3;
            gsub(/^[[:space:]]+|[[:space:]]+$/, "", pct);
            gsub(/^[[:space:]]+|[[:space:]]+$/, "", sym);
            gsub(/^[[:space:]]+|[[:space:]]+$/, "", dso);
            sub(/%$/, "", pct);
            print pct "\t" sym "\t" dso;
        }
    ' "${report_txt}" > "${tsv_out}"
}

build_benchmark_context() {
    jq -n \
        --arg bead_id "${BEAD_ID}" \
        --arg run_id "${RUN_ID}" \
        --arg generated_at "${GENERATED_AT}" \
        '
        {
            schema_version: "fsqlite.perf.hot-path-benchmark-context.v1",
            bead_id: $bead_id,
            run_id: $run_id,
            generated_at: $generated_at,
            scenarios: []
        }
        ' > "${BENCHMARK_CONTEXT_JSON}"
}

append_benchmark_context() {
    local scenario_id="$1"
    local mode="$2"
    local engine="$3"
    local workload="$4"
    local concurrency="$5"
    local bench_jsonl="$6"
    local bench_sha256="$7"

    local tmp_json
    tmp_json="$(mktemp)"
    jq -s \
        --arg scenario_id "${scenario_id}" \
        --arg mode "${mode}" \
        --arg engine "${engine}" \
        --arg workload "${workload}" \
        --arg concurrency "${concurrency}" \
        --arg bench_jsonl "${bench_jsonl}" \
        --arg bench_sha256 "${bench_sha256}" \
        '
        [
            .[]
            | select(.engine == $engine and .workload == $workload and (.concurrency | tostring) == $concurrency)
        ] as $rows
        | {
            scenario_id: $scenario_id,
            mode: $mode,
            engine: $engine,
            workload: $workload,
            concurrency: ($concurrency | tonumber),
            benchmark_jsonl_path: $bench_jsonl,
            benchmark_jsonl_sha256: $bench_sha256,
            fixture_count: ($rows | length),
            fixture_medians: (
                $rows
                | map({
                    fixture_id,
                    median_ms: .latency.median_ms,
                    p95_ms: .latency.p95_ms,
                    median_ops_per_sec: .throughput.median_ops_per_sec,
                    measurement_count
                })
            ),
            avg_median_ms: (
                if ($rows | length) > 0
                then (($rows | map(.latency.median_ms) | add) / ($rows | length))
                else null
                end
            ),
            avg_median_ops_per_sec: (
                if ($rows | length) > 0
                then (($rows | map(.throughput.median_ops_per_sec) | add) / ($rows | length))
                else null
                end
            )
        }
        ' "${bench_jsonl}" > "${tmp_json}"

    jq \
        --slurpfile scenario "${tmp_json}" \
        '.scenarios += $scenario' \
        "${BENCHMARK_CONTEXT_JSON}" > "${BENCHMARK_CONTEXT_JSON}.tmp"
    mv "${BENCHMARK_CONTEXT_JSON}.tmp" "${BENCHMARK_CONTEXT_JSON}"
    rm -f "${tmp_json}"
}

build_scenario_json() {
    local scenario_id="$1"
    local mode="$2"
    local label="$3"
    local analysis_scope="$4"
    local engine="$5"
    local workload="$6"
    local concurrency="$7"
    local perf_data="$8"
    local perf_sha256="$9"
    local bench_jsonl="${10}"
    local bench_sha256="${11}"
    local tsv_path="${12}"
    local scenario_json="${13}"

    jq -Rn \
        --arg scenario_id "${scenario_id}" \
        --arg mode "${mode}" \
        --arg label "${label}" \
        --arg analysis_scope "${analysis_scope}" \
        --arg engine "${engine}" \
        --arg workload "${workload}" \
        --arg concurrency "${concurrency}" \
        --arg perf_data "${perf_data}" \
        --arg perf_sha256 "${perf_sha256}" \
        --arg bench_jsonl "${bench_jsonl}" \
        --arg bench_sha256 "${bench_sha256}" \
        '
        def classify($marker; $symbol; $dso):
            if ($marker == "kernel") or ($dso == "[unknown]") then "kernel_unresolved"
            elif ($symbol | test("_int_malloc|__libc_malloc2|_int_free_chunk|_int_free_merge_chunk|malloc_consolidate|cfree@GLIBC|unlink_chunk")) then "allocator_pressure"
            elif ($symbol | test("__memmove|memcpy")) then "copy_movement"
            elif ($symbol | test("fsqlite_types::record::parse_record|fsqlite_types::record::decode_value|core::str::converts::from_utf8")) then "record_decode"
            elif ($symbol | test("HashMap<i32, fsqlite_vdbe::engine::MemTable|drop_in_place::<fsqlite_vdbe::engine::MemTable>")) then "row_materialization"
            elif ($symbol | test("fsqlite_parser::|parse_columns_from_create_sql|lexer::|Parser::|planner|codegen")) then "parser_ast_churn"
            elif ($symbol | test("PagerInner<|read_page_copy|IoUringFile")) then "pager_io"
            else "other_user_space"
            end;
        def opcode_responsibility($category):
            if $category == "allocator_pressure" then "cross-cutting allocator churn under Column/MakeRecord/materialization paths"
            elif $category == "copy_movement" then "MakeRecord and row-copy heavy movement around record/cell assembly"
            elif $category == "record_decode" then "Column and ResultRow decode path (parse_record, decode_value, UTF-8 conversion)"
            elif $category == "row_materialization" then "Insert/Update/Delete materialization and MemTable clone/drop churn"
            elif $category == "parser_ast_churn" then "prepare/DDL parse path, not the steady-state opcode hot loop"
            elif $category == "pager_io" then "OpenRead / page fetch assistance to Column"
            elif $category == "kernel_unresolved" then "kernel samples hidden by restricted kallsyms"
            else "uncategorized user-space work"
            end;
        [
            inputs
            | select(length > 0)
            | split("\t")
            | {
                overhead_pct: (.[0] | tonumber),
                raw_symbol: .[1],
                dso: .[2]
            }
            | .marker = (
                if (.raw_symbol | test("^\\[k\\]")) then "kernel"
                elif (.raw_symbol | test("^\\[\\.\\]")) then "user"
                else "unknown"
                end
            )
            | .symbol = (.raw_symbol | sub("^\\[[^]]+\\]\\s*"; ""))
            | .category = classify(.marker; .symbol; .dso)
            | .opcode_responsibility = opcode_responsibility(.category)
        ] as $symbols
        | {
            schema_version: "fsqlite.perf.hot-path-scenario.v1",
            scenario_id: $scenario_id,
            mode: $mode,
            label: $label,
            analysis_scope: $analysis_scope,
            engine: $engine,
            workload: $workload,
            concurrency: ($concurrency | tonumber),
            source_perf_data: $perf_data,
            source_perf_sha256: $perf_sha256,
            source_benchmark_jsonl: $bench_jsonl,
            source_benchmark_sha256: $bench_sha256,
            reported_entries: ($symbols | length),
            reported_user_space_overhead_pct: (
                $symbols
                | map(select(.category != "kernel_unresolved") | .overhead_pct)
                | add // 0
            ),
            top_symbols: ($symbols | .[:20]),
            category_totals: (
                $symbols
                | sort_by(.category)
                | group_by(.category)
                | map({
                    scenario_id: $scenario_id,
                    mode: $mode,
                    category: .[0].category,
                    opcode_responsibility: .[0].opcode_responsibility,
                    overhead_pct: (map(.overhead_pct) | add)
                })
                | sort_by(-.overhead_pct)
            )
        }
        ' < "${tsv_path}" > "${scenario_json}"
}

aggregate_actionable_ranking() {
    jq \
        --arg bead_id "${BEAD_ID}" \
        --arg run_id "${RUN_ID}" \
        --arg generated_at "${GENERATED_AT}" \
        '
        {
            schema_version: "fsqlite.perf.hot-path-ranking.v1",
            bead_id: $bead_id,
            run_id: $run_id,
            generated_at: $generated_at,
            mixed_hot_path_categories: (
                .scenarios
                | map(select(.analysis_scope == "mixed_hot_path"))
                | map(.category_totals[])
                | sort_by(.category)
                | group_by(.category)
                | map({
                    category: .[0].category,
                    opcode_responsibility: .[0].opcode_responsibility,
                    avg_overhead_pct: (map(.overhead_pct) | add / length),
                    max_overhead_pct: (map(.overhead_pct) | max),
                    scenario_breakdown: map({
                        scenario_id,
                        mode,
                        overhead_pct
                    }) | sort_by(-.overhead_pct),
                    implication: (
                        if .[0].category == "allocator_pressure" then
                            "Primary D3 target: allocator churn dominates both modes, so reuse/scratch-space work should land before parser-focused reuse."
                        elif .[0].category == "copy_movement" then
                            "Primary D4 target: memmove-heavy record/cell movement is the second-largest named user-space cost."
                        elif .[0].category == "record_decode" then
                            "D3/D4 target: Column/ResultRow decode and UTF-8 conversion are large enough to justify focused row decode work."
                        elif .[0].category == "row_materialization" then
                            "D2/D3 target: MemTable clone/drop churn is measurable and should be removed from ordinary runtime paths."
                        elif .[0].category == "parser_ast_churn" then
                            "D2 remains relevant, but parser/AST work is secondary on the mixed hot path compared with allocator/copy/decode pressure."
                        elif .[0].category == "kernel_unresolved" then
                            "Do not rank hidden kernel frames; kallsyms are restricted in these archived captures."
                        else
                            "Secondary follow-up bucket after the named D2-D4 targets."
                        end
                    ),
                    mapped_beads: (
                        if .[0].category == "allocator_pressure" then ["bd-db300.4.3", "bd-db300.4.2"]
                        elif .[0].category == "copy_movement" then ["bd-db300.4.4"]
                        elif .[0].category == "record_decode" then ["bd-db300.4.3", "bd-db300.4.4"]
                        elif .[0].category == "row_materialization" then ["bd-db300.4.2", "bd-db300.4.3"]
                        elif .[0].category == "parser_ast_churn" then ["bd-db300.4.2"]
                        else []
                        end
                    )
                })
                | sort_by(-.avg_overhead_pct)
                | to_entries
                | map(.value + { rank: (.key + 1) })
            )
        }
        | .actionable_named_categories = (
            .mixed_hot_path_categories
            | map(select(.category != "kernel_unresolved" and .category != "other_user_space"))
            | to_entries
            | map(.value + { actionable_rank: (.key + 1) })
        )
        ' "${SCENARIO_PROFILES_JSON}" > "${ACTIONABLE_RANKING_JSON}"
}

build_summary_md() {
    local top_categories
    top_categories="$(jq -r '
        .actionable_named_categories[:5]
        | map("- rank \(.actionable_rank): `\(.category)` avg=\(((.avg_overhead_pct * 100 | round) / 100) | tostring)% max=\(((.max_overhead_pct * 100 | round) / 100) | tostring)% -> \(.implication)")
        | .[]
    ' "${ACTIONABLE_RANKING_JSON}")"

    local bench_summary
    bench_summary="$(jq -r '
        .scenarios
        | map(
            "- `\(.scenario_id)`: fixtures=\(.fixture_count), avg_median_ms=\((((.avg_median_ms // 0) * 10 | round) / 10) | tostring), avg_median_ops_per_sec=\((((.avg_median_ops_per_sec // 0) * 10 | round) / 10) | tostring)"
          )
        | .[]
    ' "${BENCHMARK_CONTEXT_JSON}")"

    cat > "${SUMMARY_MD}" <<EOF
# ${BEAD_ID} Hot-Path Profile Summary

- run_id: \`${RUN_ID}\`
- generated_at: \`${GENERATED_AT}\`
- benchmark_root: \`${BENCH_ROOT}\`
- replay_command: \`bash scripts/verify_bd_db300_4_1_hot_path_profiles.sh\`
- limitation: archived perf captures have restricted kallsyms, so unresolved kernel frames are excluded from the actionable ranking

## Benchmark Context

${bench_summary}

## Actionable Ranking

${top_categories}

## Artifacts

- structured_log: \`${LOG_FILE}\`
- scenario_profiles: \`${SCENARIO_PROFILES_JSON}\`
- actionable_ranking: \`${ACTIONABLE_RANKING_JSON}\`
- benchmark_context: \`${BENCHMARK_CONTEXT_JSON}\`
- report: \`${REPORT_JSON}\`
EOF
}

log_event "INFO" "start" "starting archived D1 hot-path synthesis"

declare -a SCENARIO_ROWS=(
    "mvcc_c4_mixed|mvcc|mixed_hot_path|fsqlite_mvcc|mixed_read_write|4|${BENCH_ROOT}/profiles/fsqlite_mvcc_c4_mixed.perf.data|${BENCH_ROOT}/results/after_sort_opt_mvcc_c4_mixed_all3.jsonl|real Beads fixtures / MVCC / c4"
    "single_c4_mixed|single_writer|mixed_hot_path|fsqlite|mixed_read_write|4|${BENCH_ROOT}/profiles/fsqlite_single_c4_mixed.perf.data|${BENCH_ROOT}/results/retry_executor_c4_single.jsonl|real Beads fixtures / single-writer / c4"
    "mvcc_c1_disjoint|mvcc|contrast|fsqlite_mvcc|commutative_inserts_disjoint_keys|1|${BENCH_ROOT}/profiles/fsqlite_mvcc_c1_disjoint.perf.data|${BENCH_ROOT}/results/current_c1_disjoint_mvcc_vs_sqlite.jsonl|contrast baseline / MVCC / c1"
)

build_benchmark_context

scenario_json_paths=()

for row in "${SCENARIO_ROWS[@]}"; do
    IFS='|' read -r scenario_id mode analysis_scope engine workload concurrency perf_data bench_jsonl label <<< "${row}"
    require_file "${perf_data}"
    require_file "${bench_jsonl}"

    perf_sha256="$(sha256sum "${perf_data}" | awk '{print $1}')"
    bench_sha256="$(sha256sum "${bench_jsonl}" | awk '{print $1}')"
    report_txt="${RAW_DIR}/${scenario_id}.perf.report.txt"
    top_symbols_tsv="${RAW_DIR}/${scenario_id}.top_symbols.tsv"
    scenario_json="${RAW_DIR}/${scenario_id}.scenario.json"

    log_event "INFO" "perf-report" "rendering ${scenario_id} from ${perf_data}"
    top_symbols_from_perf "${perf_data}" "${report_txt}" "${top_symbols_tsv}"
    build_scenario_json \
        "${scenario_id}" "${mode}" "${label}" "${analysis_scope}" "${engine}" \
        "${workload}" "${concurrency}" "${perf_data}" "${perf_sha256}" \
        "${bench_jsonl}" "${bench_sha256}" "${top_symbols_tsv}" "${scenario_json}"
    append_benchmark_context \
        "${scenario_id}" "${mode}" "${engine}" "${workload}" "${concurrency}" \
        "${bench_jsonl}" "${bench_sha256}"
    scenario_json_paths+=("${scenario_json}")
done

jq -s \
    --arg bead_id "${BEAD_ID}" \
    --arg run_id "${RUN_ID}" \
    --arg generated_at "${GENERATED_AT}" \
    '
    {
        schema_version: "fsqlite.perf.hot-path-scenarios.v1",
        bead_id: $bead_id,
        run_id: $run_id,
        generated_at: $generated_at,
        scenarios: .
    }
    ' "${scenario_json_paths[@]}" > "${SCENARIO_PROFILES_JSON}"

aggregate_actionable_ranking
build_summary_md

jq -n \
    --arg schema_version "fsqlite.perf.hot-path-report.v1" \
    --arg bead_id "${BEAD_ID}" \
    --arg run_id "${RUN_ID}" \
    --arg generated_at "${GENERATED_AT}" \
    --arg benchmark_root "${BENCH_ROOT}" \
    --arg replay_command "bash scripts/verify_bd_db300_4_1_hot_path_profiles.sh" \
    --arg structured_log "${LOG_FILE}" \
    --arg scenario_profiles "${SCENARIO_PROFILES_JSON}" \
    --arg actionable_ranking "${ACTIONABLE_RANKING_JSON}" \
    --arg benchmark_context "${BENCHMARK_CONTEXT_JSON}" \
    --arg summary_md "${SUMMARY_MD}" \
    --arg report_json "${REPORT_JSON}" \
    '
    {
        schema_version: $schema_version,
        bead_id: $bead_id,
        run_id: $run_id,
        generated_at: $generated_at,
        benchmark_root: $benchmark_root,
        replay: {
            command: $replay_command
        },
        artifacts: {
            structured_log: $structured_log,
            scenario_profiles: $scenario_profiles,
            actionable_ranking: $actionable_ranking,
            benchmark_context: $benchmark_context,
            summary_md: $summary_md,
            report_json: $report_json
        },
        limitations: [
            "perf.data inputs are archived captures rather than newly recorded runs",
            "kernel frames are partially unresolved because kallsyms are restricted on the source machine"
        ]
    }
    ' > "${REPORT_JSON}"

jq -e '.scenarios | length >= 3' "${SCENARIO_PROFILES_JSON}" >/dev/null
jq -e '.mixed_hot_path_categories | length >= 4' "${ACTIONABLE_RANKING_JSON}" >/dev/null
jq -e '.scenarios | all(.fixture_count >= 1)' "${BENCHMARK_CONTEXT_JSON}" >/dev/null

log_event "INFO" "complete" "archived D1 hot-path synthesis completed"
echo "RUN_ID:              ${RUN_ID}"
echo "Benchmark root:      ${BENCH_ROOT}"
echo "Scenario profiles:   ${SCENARIO_PROFILES_JSON}"
echo "Actionable ranking:  ${ACTIONABLE_RANKING_JSON}"
echo "Benchmark context:   ${BENCHMARK_CONTEXT_JSON}"
echo "Summary:             ${SUMMARY_MD}"
echo "Report:              ${REPORT_JSON}"
