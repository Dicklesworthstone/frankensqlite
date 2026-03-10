#!/usr/bin/env bash
# verify_bd_db300_4_1_hot_path_profiles.sh — inline hot-path profile campaign
#
# Captures current Track D / D1 evidence from the inline `realdb-e2e hot-profile`
# path instead of scraping archived `perf.data` files. The script runs the
# mixed_read_write hot-path profile across the pinned Beads fixture copies in
# both MVCC and forced single-writer modes, then aggregates the per-run
# `profile.json` + `actionable_ranking.json` bundles into a bead-level ledger.
#
# Outputs:
#   artifacts/perf/bd-db300.4.1/inline/
#     events.jsonl
#     run_records.jsonl
#     scenario_profiles.json
#     actionable_ranking.json
#     benchmark_context.json
#     report.json
#     summary.md
#     runs/<scenario>__<fixture>/{profile.json,actionable_ranking.json,summary.md,manifest.json}

set -euo pipefail

WORKSPACE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BEAD_ID="bd-db300.4.1"
RUN_ID="${BEAD_ID}-$(date -u +%Y%m%dT%H%M%SZ)-$$"
GENERATED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
GOLDEN_DIR_DEFAULT="${WORKSPACE_ROOT}/sample_sqlite_db_files/working/beads_bench_20260310/golden"
GOLDEN_DIR="${GOLDEN_DIR:-${GOLDEN_DIR_DEFAULT}}"
OUTPUT_DIR="${OUTPUT_DIR:-${WORKSPACE_ROOT}/artifacts/perf/${BEAD_ID}/inline}"
RUNS_DIR="${OUTPUT_DIR}/runs"
LOG_FILE="${OUTPUT_DIR}/events.jsonl"
RUN_RECORDS_JSONL="${OUTPUT_DIR}/run_records.jsonl"
SCENARIO_PROFILES_JSON="${OUTPUT_DIR}/scenario_profiles.json"
ACTIONABLE_RANKING_JSON="${OUTPUT_DIR}/actionable_ranking.json"
BENCHMARK_CONTEXT_JSON="${OUTPUT_DIR}/benchmark_context.json"
REPORT_JSON="${OUTPUT_DIR}/report.json"
SUMMARY_MD="${OUTPUT_DIR}/summary.md"
WORKLOAD_ID="mixed_read_write"
CONCURRENCY="${CONCURRENCY:-4}"
SEED="${SEED:-42}"
SCALE="${SCALE:-50}"
CARGO_PROFILE="${CARGO_PROFILE:-release-perf}"
RCH_TARGET_DIR="${RCH_TARGET_DIR:-/tmp/rch_target_bd_db300_4_1}"

mkdir -p "${RUNS_DIR}"
: > "${LOG_FILE}"
: > "${RUN_RECORDS_JSONL}"

log_event() {
    local level="$1"
    local stage="$2"
    local message="$3"
    printf '{"run_id":"%s","bead_id":"%s","level":"%s","stage":"%s","message":"%s","ts":"%s"}\n' \
        "${RUN_ID}" "${BEAD_ID}" "${level}" "${stage}" "${message}" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
        >> "${LOG_FILE}"
}

fail() {
    local stage="$1"
    local message="$2"
    log_event "ERROR" "${stage}" "${message}"
    echo "ERROR: ${message}" >&2
    exit 1
}

require_dir() {
    local path="$1"
    [[ -d "${path}" ]] || fail "inputs" "missing required directory: ${path}"
}

require_file() {
    local path="$1"
    [[ -f "${path}" ]] || fail "inputs" "missing required file: ${path}"
}

mode_cli_flag() {
    local mode_id="$1"
    case "${mode_id}" in
        mvcc) printf '%s\n' "--mvcc" ;;
        single_writer) printf '%s\n' "--no-mvcc" ;;
        *) fail "inputs" "unsupported mode id: ${mode_id}" ;;
    esac
}

mode_engine_label() {
    local mode_id="$1"
    case "${mode_id}" in
        mvcc) printf '%s\n' "fsqlite_mvcc" ;;
        single_writer) printf '%s\n' "fsqlite_single_writer" ;;
        *) fail "inputs" "unsupported mode id: ${mode_id}" ;;
    esac
}

scenario_id_for_mode() {
    local mode_id="$1"
    printf '%s_c%s_%s\n' "${mode_id}" "${CONCURRENCY}" "${WORKLOAD_ID}"
}

discover_fixture_ids() {
    if [[ -n "${FIXTURE_IDS:-}" ]]; then
        tr ',' '\n' <<< "${FIXTURE_IDS}" | sed '/^$/d'
        return
    fi

    find "${GOLDEN_DIR}" -maxdepth 1 -type f -name '*.db' -printf '%f\n' \
        | sed 's/\.db$//' \
        | sort
}

discover_mode_ids() {
    if [[ -n "${MODE_IDS:-}" ]]; then
        tr ',' '\n' <<< "${MODE_IDS}" | sed '/^$/d'
        return
    fi

    printf '%s\n' mvcc single_writer
}

capture_run_record() {
    local scenario_id="$1"
    local fixture_id="$2"
    local mode_id="$3"
    local scenario_dir="$4"
    local stdout_log="$5"
    local stderr_log="$6"

    jq -n \
        --arg scenario_id "${scenario_id}" \
        --arg fixture_id "${fixture_id}" \
        --arg mode_id "${mode_id}" \
        --arg workload "${WORKLOAD_ID}" \
        --arg output_dir "${scenario_dir}" \
        --arg stdout_log "${stdout_log}" \
        --arg stderr_log "${stderr_log}" \
        --arg engine_label "$(mode_engine_label "${mode_id}")" \
        --arg golden_dir "${GOLDEN_DIR}" \
        --arg cargo_profile "${CARGO_PROFILE}" \
        --argjson concurrency "${CONCURRENCY}" \
        --argjson seed "${SEED}" \
        --argjson scale "${SCALE}" \
        --slurpfile profile "${scenario_dir}/profile.json" \
        --slurpfile actionable_ranking "${scenario_dir}/actionable_ranking.json" \
        --slurpfile manifest "${scenario_dir}/manifest.json" \
        '
        {
            scenario_id: $scenario_id,
            fixture_id: $fixture_id,
            mode_id: $mode_id,
            engine_label: $engine_label,
            workload: $workload,
            concurrency: $concurrency,
            seed: $seed,
            scale: $scale,
            cargo_profile: $cargo_profile,
            golden_dir: $golden_dir,
            output_dir: $output_dir,
            stdout_log: $stdout_log,
            stderr_log: $stderr_log,
            profile: $profile[0],
            actionable_ranking: $actionable_ranking[0],
            manifest: $manifest[0]
        }
        ' >> "${RUN_RECORDS_JSONL}"
}

run_hot_profile() {
    local fixture_id="$1"
    local mode_id="$2"
    local scenario_id
    scenario_id="$(scenario_id_for_mode "${mode_id}")"
    local scenario_dir="${RUNS_DIR}/${scenario_id}__${fixture_id}"
    local stdout_log="${scenario_dir}/stdout.log"
    local stderr_log="${scenario_dir}/stderr.log"
    local cli_flag
    cli_flag="$(mode_cli_flag "${mode_id}")"

    mkdir -p "${scenario_dir}"
    log_event "INFO" "run" "starting ${scenario_id} fixture=${fixture_id} mode=${mode_id}"

    if ! rch exec -- env CARGO_TARGET_DIR="${RCH_TARGET_DIR}" cargo run \
        -p fsqlite-e2e \
        --profile "${CARGO_PROFILE}" \
        --bin realdb-e2e \
        -- hot-profile \
        --golden-dir "${GOLDEN_DIR}" \
        --db "${fixture_id}" \
        --concurrency "${CONCURRENCY}" \
        --seed "${SEED}" \
        --scale "${SCALE}" \
        --output-dir "${scenario_dir}" \
        "${cli_flag}" \
        > "${stdout_log}" \
        2> "${stderr_log}"; then
        fail "run" "hot-profile failed for fixture=${fixture_id} mode=${mode_id}; see ${stderr_log}"
    fi

    require_file "${scenario_dir}/profile.json"
    require_file "${scenario_dir}/actionable_ranking.json"
    require_file "${scenario_dir}/summary.md"
    require_file "${scenario_dir}/manifest.json"
    capture_run_record "${scenario_id}" "${fixture_id}" "${mode_id}" "${scenario_dir}" "${stdout_log}" "${stderr_log}"
    log_event "INFO" "run" "completed ${scenario_id} fixture=${fixture_id} mode=${mode_id}"
}

build_scenario_profiles() {
    jq -s \
        --arg bead_id "${BEAD_ID}" \
        --arg run_id "${RUN_ID}" \
        --arg generated_at "${GENERATED_AT}" \
        --arg golden_dir "${GOLDEN_DIR}" \
        --arg workload "${WORKLOAD_ID}" \
        '
        {
            schema_version: "fsqlite-e2e.hot_path_campaign_scenarios.v1",
            bead_id: $bead_id,
            run_id: $run_id,
            generated_at: $generated_at,
            golden_dir: $golden_dir,
            workload: $workload,
            runs: .
        }
        ' "${RUN_RECORDS_JSONL}" > "${SCENARIO_PROFILES_JSON}"
}

build_benchmark_context() {
    jq -s \
        --arg bead_id "${BEAD_ID}" \
        --arg run_id "${RUN_ID}" \
        --arg generated_at "${GENERATED_AT}" \
        --arg golden_dir "${GOLDEN_DIR}" \
        --arg workload "${WORKLOAD_ID}" \
        '
        {
            schema_version: "fsqlite-e2e.hot_path_campaign_context.v1",
            bead_id: $bead_id,
            run_id: $run_id,
            generated_at: $generated_at,
            golden_dir: $golden_dir,
            workload: $workload,
            runs: [
                .[] | {
                    scenario_id,
                    fixture_id,
                    mode_id,
                    engine_label,
                    concurrency,
                    seed,
                    scale,
                    ops_per_sec: .profile.engine_report.ops_per_sec,
                    wall_time_ms: .profile.engine_report.wall_time_ms,
                    retries: .profile.engine_report.retries,
                    aborts: .profile.engine_report.aborts,
                    error: (.profile.engine_report.error // null),
                    output_dir
                }
            ]
        }
        ' "${RUN_RECORDS_JSONL}" > "${BENCHMARK_CONTEXT_JSON}"
}

build_actionable_ranking() {
    jq -s \
        --arg bead_id "${BEAD_ID}" \
        --arg run_id "${RUN_ID}" \
        --arg generated_at "${GENERATED_AT}" \
        '
        def named_entries:
            [ .[] as $run
              | $run.actionable_ranking.named_hotspots[]
              | . + {
                    fixture_id: $run.fixture_id,
                    mode_id: $run.mode_id,
                    scenario_id: $run.scenario_id
                }
            ];
        def allocator_entries:
            [ .[] as $run
              | $run.actionable_ranking.allocator_pressure[]
              | . + {
                    fixture_id: $run.fixture_id,
                    mode_id: $run.mode_id,
                    scenario_id: $run.scenario_id
                }
            ];
        def opcode_entries:
            [ .[] as $run
              | $run.profile.opcode_profile[]
              | . + {
                    fixture_id: $run.fixture_id,
                    mode_id: $run.mode_id,
                    scenario_id: $run.scenario_id
                }
            ];
        {
            schema_version: "fsqlite-e2e.hot_path_campaign_ranking.v1",
            bead_id: $bead_id,
            run_id: $run_id,
            generated_at: $generated_at,
            scenario_count: length,
            named_hotspots: (
                named_entries
                | sort_by(.subsystem)
                | group_by(.subsystem)
                | map({
                    subsystem: .[0].subsystem,
                    metric_kind: .[0].metric_kind,
                    avg_metric_value: ((map(.metric_value) | add) / length),
                    max_metric_value: (map(.metric_value) | max),
                    run_breakdown: (
                        map({
                            fixture_id,
                            mode_id,
                            scenario_id,
                            rank,
                            metric_value,
                            rationale,
                            implication,
                            mapped_beads
                        })
                        | sort_by(.metric_value)
                        | reverse
                    )
                })
                | sort_by(.avg_metric_value)
                | reverse
                | to_entries
                | map(.value + { rank: (.key + 1) })
            ),
            allocator_pressure: (
                allocator_entries
                | sort_by(.subsystem)
                | group_by(.subsystem)
                | map({
                    subsystem: .[0].subsystem,
                    metric_kind: .[0].metric_kind,
                    avg_metric_value: ((map(.metric_value) | add) / length),
                    max_metric_value: (map(.metric_value) | max),
                    run_breakdown: (
                        map({
                            fixture_id,
                            mode_id,
                            scenario_id,
                            rank,
                            metric_value,
                            rationale,
                            implication,
                            mapped_beads
                        })
                        | sort_by(.metric_value)
                        | reverse
                    )
                })
                | sort_by(.avg_metric_value)
                | reverse
                | to_entries
                | map(.value + { rank: (.key + 1) })
            ),
            top_opcodes: (
                opcode_entries
                | sort_by(.opcode)
                | group_by(.opcode)
                | map({
                    opcode: .[0].opcode,
                    total: (map(.total) | add),
                    run_breakdown: (
                        map({
                            fixture_id,
                            mode_id,
                            scenario_id,
                            total
                        })
                        | sort_by(.total)
                        | reverse
                    )
                })
                | sort_by(.total)
                | reverse
                | .[:12]
            )
        }
        ' "${RUN_RECORDS_JSONL}" > "${ACTIONABLE_RANKING_JSON}"
}

build_summary_md() {
    local run_summary
    run_summary="$(jq -r '
        .runs
        | map(
            "- `\(.scenario_id)` / `\(.fixture_id)` / `\(.mode_id)`: ops_per_sec=\(.ops_per_sec), wall_time_ms=\(.wall_time_ms), retries=\(.retries), aborts=\(.aborts)"
          )
        | .[]
    ' "${BENCHMARK_CONTEXT_JSON}")"

    local hotspot_summary
    hotspot_summary="$(jq -r '
        .named_hotspots[:5]
        | map(
            "- rank \(.rank): `\(.subsystem)` avg=\(.avg_metric_value) \(.metric_kind) max=\(.max_metric_value) -> \(.run_breakdown[0].implication)"
          )
        | .[]
    ' "${ACTIONABLE_RANKING_JSON}")"

    local allocator_summary
    allocator_summary="$(jq -r '
        .allocator_pressure[:3]
        | map(
            "- rank \(.rank): `\(.subsystem)` avg=\(.avg_metric_value) \(.metric_kind) max=\(.max_metric_value) -> \(.run_breakdown[0].implication)"
          )
        | .[]
    ' "${ACTIONABLE_RANKING_JSON}")"

    local opcode_summary
    opcode_summary="$(jq -r '
        .top_opcodes[:8]
        | map("- `\(.opcode)`: total=\(.total)")
        | .[]
    ' "${ACTIONABLE_RANKING_JSON}")"

    cat > "${SUMMARY_MD}" <<EOF
# ${BEAD_ID} Inline Hot-Path Campaign Summary

- run_id: \`${RUN_ID}\`
- generated_at: \`${GENERATED_AT}\`
- golden_dir: \`${GOLDEN_DIR}\`
- replay_command: \`bash scripts/verify_bd_db300_4_1_hot_path_profiles.sh\`
- cargo_profile: \`${CARGO_PROFILE}\`
- workload: \`${WORKLOAD_ID}\`
- concurrency: \`${CONCURRENCY}\`

## Run Context

${run_summary}

## Ranked Hotspots

${hotspot_summary}

## Allocator Pressure

${allocator_summary}

## Top Opcodes

${opcode_summary}

## Artifacts

- structured_log: \`${LOG_FILE}\`
- run_records: \`${RUN_RECORDS_JSONL}\`
- scenario_profiles: \`${SCENARIO_PROFILES_JSON}\`
- actionable_ranking: \`${ACTIONABLE_RANKING_JSON}\`
- benchmark_context: \`${BENCHMARK_CONTEXT_JSON}\`
- report: \`${REPORT_JSON}\`
EOF
}

build_report_json() {
    jq -n \
        --arg schema_version "fsqlite-e2e.hot_path_campaign_report.v1" \
        --arg bead_id "${BEAD_ID}" \
        --arg run_id "${RUN_ID}" \
        --arg generated_at "${GENERATED_AT}" \
        --arg golden_dir "${GOLDEN_DIR}" \
        --arg output_dir "${OUTPUT_DIR}" \
        --arg replay_command "bash scripts/verify_bd_db300_4_1_hot_path_profiles.sh" \
        --arg structured_log "${LOG_FILE}" \
        --arg run_records "${RUN_RECORDS_JSONL}" \
        --arg scenario_profiles "${SCENARIO_PROFILES_JSON}" \
        --arg actionable_ranking "${ACTIONABLE_RANKING_JSON}" \
        --arg benchmark_context "${BENCHMARK_CONTEXT_JSON}" \
        --arg summary_md "${SUMMARY_MD}" \
        --arg report_json "${REPORT_JSON}" \
        --arg workload "${WORKLOAD_ID}" \
        --arg cargo_profile "${CARGO_PROFILE}" \
        --argjson concurrency "${CONCURRENCY}" \
        --argjson seed "${SEED}" \
        --argjson scale "${SCALE}" \
        '
        {
            schema_version: $schema_version,
            bead_id: $bead_id,
            run_id: $run_id,
            generated_at: $generated_at,
            workload: $workload,
            golden_dir: $golden_dir,
            output_dir: $output_dir,
            cargo_profile: $cargo_profile,
            concurrency: $concurrency,
            seed: $seed,
            scale: $scale,
            replay: {
                command: $replay_command
            },
            artifacts: {
                structured_log: $structured_log,
                run_records: $run_records,
                scenario_profiles: $scenario_profiles,
                actionable_ranking: $actionable_ranking,
                benchmark_context: $benchmark_context,
                summary_md: $summary_md,
                report_json: $report_json
            },
            limitations: [
                "This workflow captures mixed_read_write inline hot-path evidence only.",
                "Results depend on the pinned Beads working-copy fixtures under the benchmark workspace golden directory."
            ]
        }
        ' > "${REPORT_JSON}"
}

log_event "INFO" "start" "starting inline D1 hot-path campaign"
require_dir "${GOLDEN_DIR}"

mapfile -t FIXTURE_IDS_ARRAY < <(discover_fixture_ids)
mapfile -t MODE_IDS_ARRAY < <(discover_mode_ids)

(( ${#FIXTURE_IDS_ARRAY[@]} > 0 )) || fail "inputs" "no fixture ids discovered under ${GOLDEN_DIR}"
(( ${#MODE_IDS_ARRAY[@]} > 0 )) || fail "inputs" "no mode ids configured"

expected_runs=$(( ${#FIXTURE_IDS_ARRAY[@]} * ${#MODE_IDS_ARRAY[@]} ))
log_event "INFO" "plan" "fixtures=${#FIXTURE_IDS_ARRAY[@]} modes=${#MODE_IDS_ARRAY[@]} expected_runs=${expected_runs}"

for mode_id in "${MODE_IDS_ARRAY[@]}"; do
    for fixture_id in "${FIXTURE_IDS_ARRAY[@]}"; do
        run_hot_profile "${fixture_id}" "${mode_id}"
    done
done

build_scenario_profiles
build_benchmark_context
build_actionable_ranking
build_summary_md
build_report_json

jq -e '.runs | length >= 1' "${SCENARIO_PROFILES_JSON}" >/dev/null
jq -e '.named_hotspots | length >= 1' "${ACTIONABLE_RANKING_JSON}" >/dev/null
jq -e '.allocator_pressure | length >= 1' "${ACTIONABLE_RANKING_JSON}" >/dev/null
jq -e '.runs | length == '"${expected_runs}" "${BENCHMARK_CONTEXT_JSON}" >/dev/null

log_event "INFO" "complete" "inline D1 hot-path campaign completed"
echo "RUN_ID:              ${RUN_ID}"
echo "Golden dir:          ${GOLDEN_DIR}"
echo "Run records:         ${RUN_RECORDS_JSONL}"
echo "Scenario profiles:   ${SCENARIO_PROFILES_JSON}"
echo "Actionable ranking:  ${ACTIONABLE_RANKING_JSON}"
echo "Benchmark context:   ${BENCHMARK_CONTEXT_JSON}"
echo "Summary:             ${SUMMARY_MD}"
echo "Report:              ${REPORT_JSON}"
