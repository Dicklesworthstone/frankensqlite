#!/usr/bin/env bash
# verify_bd_db300_4_1_hot_path_profiles.sh — inline hot-path profile campaign
#
# Captures current Track D / D1 evidence from the inline `realdb-e2e hot-profile`
# path instead of scraping archived `perf.data` files. The script runs the
# mixed_read_write hot-path profile across the pinned Beads fixture copies in
# both MVCC and forced single-writer modes, then aggregates the per-run
# `profile.json` + `actionable_ranking.json` bundles into a bead-level ledger.
# Default profile is `release` for worker survivability; override with
# `CARGO_PROFILE=release-perf` when the worker fleet has headroom for it.
#
# Outputs:
#   artifacts/perf/bd-db300.4.1/inline/
#     events.jsonl
#     run_records.jsonl
#     opcode_profile_packs.json
#     subsystem_profile_packs.json
#     scenario_profiles.json
#     actionable_ranking.json
#     benchmark_context.json
#     report.json
#     summary.md
#     runs/<scenario>__<fixture>/{profile.json,opcode_profile.json,subsystem_profile.json,actionable_ranking.json,summary.md,manifest.json}

set -euo pipefail

WORKSPACE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BEAD_ID="bd-db300.4.1"
RUN_ID="${BEAD_ID}-$(date -u +%Y%m%dT%H%M%SZ)-$$"
GENERATED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
SOURCE_GOLDEN_DIR_DEFAULT="${WORKSPACE_ROOT}/sample_sqlite_db_files/working/beads_bench_20260310/golden"
SOURCE_GOLDEN_DIR="${GOLDEN_DIR:-${SOURCE_GOLDEN_DIR_DEFAULT}}"
OUTPUT_DIR="${OUTPUT_DIR:-${WORKSPACE_ROOT}/artifacts/perf/${BEAD_ID}/inline}"
RUNS_DIR="${OUTPUT_DIR}/runs"
SYNC_GOLDEN_DIR="${SYNC_GOLDEN_DIR:-${OUTPUT_DIR}/golden}"
LOG_FILE="${OUTPUT_DIR}/events.jsonl"
RUN_RECORDS_JSONL="${OUTPUT_DIR}/run_records.jsonl"
OPCODE_PROFILE_PACKS_JSON="${OUTPUT_DIR}/opcode_profile_packs.json"
SUBSYSTEM_PROFILE_PACKS_JSON="${OUTPUT_DIR}/subsystem_profile_packs.json"
SCENARIO_PROFILES_JSON="${OUTPUT_DIR}/scenario_profiles.json"
ACTIONABLE_RANKING_JSON="${OUTPUT_DIR}/actionable_ranking.json"
BENCHMARK_CONTEXT_JSON="${OUTPUT_DIR}/benchmark_context.json"
REPORT_JSON="${OUTPUT_DIR}/report.json"
SUMMARY_MD="${OUTPUT_DIR}/summary.md"
WORKLOAD_ID="mixed_read_write"
CONCURRENCY="${CONCURRENCY:-4}"
SEED="${SEED:-42}"
SCALE="${SCALE:-50}"
CARGO_PROFILE="${CARGO_PROFILE:-release}"
RCH_TARGET_DIR="${RCH_TARGET_DIR:-/tmp/rch_target_bd_db300_4_1}"
HOT_PATH_PROFILE_SCHEMA="fsqlite-e2e.hot_path_profile.v1"
HOT_PATH_PROFILE_MANIFEST_SCHEMA="fsqlite-e2e.hot_path_profile_manifest.v1"
HOT_PATH_OPCODE_PROFILE_SCHEMA="fsqlite-e2e.hot_path_opcode_profile.v1"
HOT_PATH_SUBSYSTEM_PROFILE_SCHEMA="fsqlite-e2e.hot_path_subsystem_profile.v1"
HOT_PATH_ACTIONABLE_RANKING_SCHEMA="fsqlite-e2e.hot_path_actionable_ranking.v2"
HOT_PATH_INLINE_BUNDLE_SCHEMA="fsqlite-e2e.hot_path_inline_bundle.v1"
HOT_PATH_INLINE_BUNDLE_PREFIX="HOT_PATH_INLINE_BUNDLE_JSON="

mkdir -p "${RUNS_DIR}"
: > "${LOG_FILE}"
: > "${RUN_RECORDS_JSONL}"
: > "${OPCODE_PROFILE_PACKS_JSON}"
: > "${SUBSYSTEM_PROFILE_PACKS_JSON}"
: > "${SCENARIO_PROFILES_JSON}"
: > "${ACTIONABLE_RANKING_JSON}"
: > "${BENCHMARK_CONTEXT_JSON}"
: > "${REPORT_JSON}"
: > "${SUMMARY_MD}"

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

require_nonempty_file() {
    local path="$1"
    [[ -s "${path}" ]] || fail "inputs" "missing or empty required file: ${path}"
}

require_json_schema() {
    local path="$1"
    local schema="$2"

    require_nonempty_file "${path}"
    jq -e --arg schema "${schema}" '
        type == "object" and .schema_version == $schema
    ' "${path}" > /dev/null \
        || fail "inputs" "invalid schema in ${path}; expected ${schema}"
}

preseed_output_bundle() {
    local scenario_dir="$1"

    mkdir -p "${scenario_dir}"
    : > "${scenario_dir}/profile.json"
    : > "${scenario_dir}/opcode_profile.json"
    : > "${scenario_dir}/subsystem_profile.json"
    : > "${scenario_dir}/actionable_ranking.json"
    : > "${scenario_dir}/summary.md"
    : > "${scenario_dir}/manifest.json"
}

materialize_output_bundle_from_log() {
    local scenario_dir="$1"
    local stderr_log="$2"
    local bundle_line

    bundle_line="$(sed -n "s/^${HOT_PATH_INLINE_BUNDLE_PREFIX}//p" "${stderr_log}" | tail -n 1)"
    [[ -n "${bundle_line}" ]] || return 0

    printf '%s\n' "${bundle_line}" \
        | jq -e --arg schema "${HOT_PATH_INLINE_BUNDLE_SCHEMA}" '
            type == "object" and .schema_version == $schema
        ' > /dev/null \
        || fail "run" "invalid inline hot-path bundle in ${stderr_log}"

    printf '%s\n' "${bundle_line}" | jq '.profile' > "${scenario_dir}/profile.json"
    printf '%s\n' "${bundle_line}" | jq '.opcode_profile' > "${scenario_dir}/opcode_profile.json"
    printf '%s\n' "${bundle_line}" | jq '.subsystem_profile' > "${scenario_dir}/subsystem_profile.json"
    printf '%s\n' "${bundle_line}" | jq '.actionable_ranking' > "${scenario_dir}/actionable_ranking.json"
    printf '%s\n' "${bundle_line}" | jq -r '.summary_markdown' > "${scenario_dir}/summary.md"
    printf '%s\n' "${bundle_line}" | jq '.manifest' > "${scenario_dir}/manifest.json"
}

copy_fixture_seed() {
    local fixture_id="$1"
    local source_db="${SOURCE_GOLDEN_DIR}/${fixture_id}.db"
    local dest_db="${SYNC_GOLDEN_DIR}/${fixture_id}.db"

    require_file "${source_db}"
    mkdir -p "${SYNC_GOLDEN_DIR}"
    cp "${source_db}" "${dest_db}"
    for suffix in -wal -shm -journal; do
        if [[ -f "${source_db}${suffix}" ]]; then
            cp "${source_db}${suffix}" "${dest_db}${suffix}"
        fi
    done
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

    find "${SOURCE_GOLDEN_DIR}" -maxdepth 1 -type f -name '*.db' -printf '%f\n' \
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

join_csv() {
    local IFS=','
    printf '%s' "$*"
}

capture_run_record() {
    local scenario_id="$1"
    local fixture_id="$2"
    local mode_id="$3"
    local scenario_dir="$4"
    local stdout_log="$5"
    local stderr_log="$6"

    jq -c -n \
        --arg scenario_id "${scenario_id}" \
        --arg fixture_id "${fixture_id}" \
        --arg mode_id "${mode_id}" \
        --arg workload "${WORKLOAD_ID}" \
        --arg output_dir "${scenario_dir}" \
        --arg stdout_log "${stdout_log}" \
        --arg stderr_log "${stderr_log}" \
        --arg engine_label "$(mode_engine_label "${mode_id}")" \
        --arg source_golden_dir "${SOURCE_GOLDEN_DIR}" \
        --arg sync_golden_dir "${SYNC_GOLDEN_DIR}" \
        --arg cargo_profile "${CARGO_PROFILE}" \
        --argjson concurrency "${CONCURRENCY}" \
        --argjson seed "${SEED}" \
        --argjson scale "${SCALE}" \
        --slurpfile profile "${scenario_dir}/profile.json" \
        --slurpfile opcode_profile "${scenario_dir}/opcode_profile.json" \
        --slurpfile subsystem_profile "${scenario_dir}/subsystem_profile.json" \
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
            source_golden_dir: $source_golden_dir,
            sync_golden_dir: $sync_golden_dir,
            output_dir: $output_dir,
            stdout_log: $stdout_log,
            stderr_log: $stderr_log,
            profile: $profile[0],
            opcode_profile_pack: $opcode_profile[0],
            subsystem_profile_pack: $subsystem_profile[0],
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
    preseed_output_bundle "${scenario_dir}"
    log_event "INFO" "run" "starting ${scenario_id} fixture=${fixture_id} mode=${mode_id}"

    if ! rch exec -- env CARGO_TARGET_DIR="${RCH_TARGET_DIR}" cargo run \
        -p fsqlite-e2e \
        --profile "${CARGO_PROFILE}" \
        --bin realdb-e2e \
        -- hot-profile \
        --golden-dir "${SYNC_GOLDEN_DIR}" \
        --db "${fixture_id}" \
        --concurrency "${CONCURRENCY}" \
        --seed "${SEED}" \
        --scale "${SCALE}" \
        --output-dir "${scenario_dir}" \
        --emit-inline-bundle \
        "${cli_flag}" \
        > "${stdout_log}" \
        2> "${stderr_log}"; then
        fail "run" "hot-profile failed for fixture=${fixture_id} mode=${mode_id}; see ${stderr_log}"
    fi

    materialize_output_bundle_from_log "${scenario_dir}" "${stderr_log}"
    require_json_schema "${scenario_dir}/profile.json" "${HOT_PATH_PROFILE_SCHEMA}"
    require_json_schema "${scenario_dir}/opcode_profile.json" "${HOT_PATH_OPCODE_PROFILE_SCHEMA}"
    require_json_schema "${scenario_dir}/subsystem_profile.json" "${HOT_PATH_SUBSYSTEM_PROFILE_SCHEMA}"
    require_json_schema "${scenario_dir}/actionable_ranking.json" "${HOT_PATH_ACTIONABLE_RANKING_SCHEMA}"
    require_nonempty_file "${scenario_dir}/summary.md"
    require_json_schema "${scenario_dir}/manifest.json" "${HOT_PATH_PROFILE_MANIFEST_SCHEMA}"
    capture_run_record "${scenario_id}" "${fixture_id}" "${mode_id}" "${scenario_dir}" "${stdout_log}" "${stderr_log}"
    log_event "INFO" "run" "completed ${scenario_id} fixture=${fixture_id} mode=${mode_id}"
}

build_scenario_profiles() {
    jq -s \
        --arg bead_id "${BEAD_ID}" \
        --arg run_id "${RUN_ID}" \
        --arg generated_at "${GENERATED_AT}" \
        --arg source_golden_dir "${SOURCE_GOLDEN_DIR}" \
        --arg sync_golden_dir "${SYNC_GOLDEN_DIR}" \
        --arg workload "${WORKLOAD_ID}" \
        '
        {
            schema_version: "fsqlite-e2e.hot_path_campaign_scenarios.v1",
            bead_id: $bead_id,
            run_id: $run_id,
            generated_at: $generated_at,
            source_golden_dir: $source_golden_dir,
            sync_golden_dir: $sync_golden_dir,
            workload: $workload,
            runs: .
        }
        ' "${RUN_RECORDS_JSONL}" > "${SCENARIO_PROFILES_JSON}"
}

build_opcode_profile_packs() {
    jq -s \
        --arg bead_id "${BEAD_ID}" \
        --arg run_id "${RUN_ID}" \
        --arg generated_at "${GENERATED_AT}" \
        '
        {
            schema_version: "fsqlite-e2e.hot_path_campaign_opcode_packs.v1",
            bead_id: $bead_id,
            run_id: $run_id,
            generated_at: $generated_at,
            runs: [
                .[] | {
                    scenario_id,
                    fixture_id,
                    mode_id,
                    engine_label,
                    output_dir,
                    opcode_profile: .opcode_profile_pack
                }
            ]
        }
        ' "${RUN_RECORDS_JSONL}" > "${OPCODE_PROFILE_PACKS_JSON}"
}

build_subsystem_profile_packs() {
    jq -s \
        --arg bead_id "${BEAD_ID}" \
        --arg run_id "${RUN_ID}" \
        --arg generated_at "${GENERATED_AT}" \
        '
        {
            schema_version: "fsqlite-e2e.hot_path_campaign_subsystem_packs.v1",
            bead_id: $bead_id,
            run_id: $run_id,
            generated_at: $generated_at,
            runs: [
                .[] | {
                    scenario_id,
                    fixture_id,
                    mode_id,
                    engine_label,
                    output_dir,
                    subsystem_profile: .subsystem_profile_pack
                }
            ]
        }
        ' "${RUN_RECORDS_JSONL}" > "${SUBSYSTEM_PROFILE_PACKS_JSON}"
}

build_benchmark_context() {
    jq -s \
        --arg bead_id "${BEAD_ID}" \
        --arg run_id "${RUN_ID}" \
        --arg generated_at "${GENERATED_AT}" \
        --arg source_golden_dir "${SOURCE_GOLDEN_DIR}" \
        --arg sync_golden_dir "${SYNC_GOLDEN_DIR}" \
        --arg workload "${WORKLOAD_ID}" \
        --arg fixture_ids_csv "${FIXTURE_IDS_CSV}" \
        --arg mode_ids_csv "${MODE_IDS_CSV}" \
        --argjson expected_runs "${expected_runs}" \
        '
        {
            schema_version: "fsqlite-e2e.hot_path_campaign_context.v1",
            bead_id: $bead_id,
            run_id: $run_id,
            generated_at: $generated_at,
            source_golden_dir: $source_golden_dir,
            sync_golden_dir: $sync_golden_dir,
            workload: $workload,
            fixture_ids: ($fixture_ids_csv | split(",") | map(select(length > 0))),
            mode_ids: ($mode_ids_csv | split(",") | map(select(length > 0))),
            expected_runs: $expected_runs,
            completed_runs: length,
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
        def cost_component_entries:
            [ .[] as $run
              | $run.actionable_ranking.cost_components[]
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
            cost_components: (
                cost_component_entries
                | sort_by(.component)
                | group_by(.component)
                | map({
                    component: .[0].component,
                    avg_time_ns: ((map(.time_ns) | add) / length),
                    max_time_ns: (map(.time_ns) | max),
                    avg_time_share_basis_points: ((map(.time_share_basis_points) | add) / length),
                    avg_allocator_pressure_bytes: ((map(.allocator_pressure_bytes) | add) / length),
                    max_allocator_pressure_bytes: (map(.allocator_pressure_bytes) | max),
                    avg_allocator_share_basis_points: ((map(.allocator_share_basis_points) | add) / length),
                    max_allocator_share_basis_points: (map(.allocator_share_basis_points) | max),
                    max_activity_count: (map(.activity_count) | max),
                    run_breakdown: (
                        map({
                            fixture_id,
                            mode_id,
                            scenario_id,
                            rank,
                            time_ns,
                            time_share_basis_points,
                            allocator_pressure_bytes,
                            allocator_share_basis_points,
                            activity_count,
                            rationale,
                            implication,
                            mapped_beads
                        })
                        | sort_by([.time_ns, .allocator_pressure_bytes])
                        | reverse
                    )
                })
                | sort_by([.avg_time_ns, .avg_allocator_pressure_bytes])
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
    local coverage_summary
    coverage_summary="$(jq -r '
        [
            "- fixture_ids: `\(.fixture_ids | join(","))`",
            "- mode_ids: `\(.mode_ids | join(","))`",
            "- expected_runs: `\(.expected_runs)`",
            "- completed_runs: `\(.completed_runs)`"
        ]
        | .[]
    ' "${BENCHMARK_CONTEXT_JSON}")"

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

    local cost_component_summary
    cost_component_summary="$(jq -r '
        .cost_components[:3]
        | map(
            "- rank \(.rank): `\(.component)` avg_time_ns=\(.avg_time_ns) avg_time_share_bps=\(.avg_time_share_basis_points) avg_allocator_pressure_bytes=\(.avg_allocator_pressure_bytes) avg_allocator_share_bps=\(.avg_allocator_share_basis_points) max_activity_count=\(.max_activity_count) -> \(.run_breakdown[0].implication)"
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
- source_golden_dir: \`${SOURCE_GOLDEN_DIR}\`
- sync_golden_dir: \`${SYNC_GOLDEN_DIR}\`
- replay_command: \`${REPLAY_COMMAND}\`
- cargo_profile: \`${CARGO_PROFILE}\`
- workload: \`${WORKLOAD_ID}\`
- concurrency: \`${CONCURRENCY}\`
${coverage_summary}

## Run Context

${run_summary}

## Ranked Hotspots

${hotspot_summary}

## Quantified Cost Components

${cost_component_summary}

## Allocator Pressure

${allocator_summary}

## Top Opcodes

${opcode_summary}

## Artifacts

- structured_log: \`${LOG_FILE}\`
- run_records: \`${RUN_RECORDS_JSONL}\`
- opcode_profile_packs: \`${OPCODE_PROFILE_PACKS_JSON}\`
- subsystem_profile_packs: \`${SUBSYSTEM_PROFILE_PACKS_JSON}\`
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
        --arg source_golden_dir "${SOURCE_GOLDEN_DIR}" \
        --arg sync_golden_dir "${SYNC_GOLDEN_DIR}" \
        --arg output_dir "${OUTPUT_DIR}" \
        --arg replay_command "${REPLAY_COMMAND}" \
        --arg structured_log "${LOG_FILE}" \
        --arg run_records "${RUN_RECORDS_JSONL}" \
        --arg opcode_profile_packs "${OPCODE_PROFILE_PACKS_JSON}" \
        --arg subsystem_profile_packs "${SUBSYSTEM_PROFILE_PACKS_JSON}" \
        --arg scenario_profiles "${SCENARIO_PROFILES_JSON}" \
        --arg actionable_ranking "${ACTIONABLE_RANKING_JSON}" \
        --arg benchmark_context "${BENCHMARK_CONTEXT_JSON}" \
        --arg summary_md "${SUMMARY_MD}" \
        --arg report_json "${REPORT_JSON}" \
        --arg workload "${WORKLOAD_ID}" \
        --arg cargo_profile "${CARGO_PROFILE}" \
        --arg fixture_ids_csv "${FIXTURE_IDS_CSV}" \
        --arg mode_ids_csv "${MODE_IDS_CSV}" \
        --argjson concurrency "${CONCURRENCY}" \
        --argjson seed "${SEED}" \
        --argjson scale "${SCALE}" \
        --argjson expected_runs "${expected_runs}" \
        --slurpfile benchmark_context "${BENCHMARK_CONTEXT_JSON}" \
        '
        {
            schema_version: $schema_version,
            bead_id: $bead_id,
            run_id: $run_id,
            generated_at: $generated_at,
            workload: $workload,
            source_golden_dir: $source_golden_dir,
            sync_golden_dir: $sync_golden_dir,
            output_dir: $output_dir,
            cargo_profile: $cargo_profile,
            concurrency: $concurrency,
            seed: $seed,
            scale: $scale,
            fixture_ids: ($fixture_ids_csv | split(",") | map(select(length > 0))),
            mode_ids: ($mode_ids_csv | split(",") | map(select(length > 0))),
            expected_runs: $expected_runs,
            completed_runs: ($benchmark_context[0].completed_runs // null),
            replay: {
                command: $replay_command
            },
            artifacts: {
                structured_log: $structured_log,
                run_records: $run_records,
                opcode_profile_packs: $opcode_profile_packs,
                subsystem_profile_packs: $subsystem_profile_packs,
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
require_dir "${SOURCE_GOLDEN_DIR}"

mapfile -t FIXTURE_IDS_ARRAY < <(discover_fixture_ids)
mapfile -t MODE_IDS_ARRAY < <(discover_mode_ids)

(( ${#FIXTURE_IDS_ARRAY[@]} > 0 )) || fail "inputs" "no fixture ids discovered under ${SOURCE_GOLDEN_DIR}"
(( ${#MODE_IDS_ARRAY[@]} > 0 )) || fail "inputs" "no mode ids configured"

expected_runs=$(( ${#FIXTURE_IDS_ARRAY[@]} * ${#MODE_IDS_ARRAY[@]} ))
FIXTURE_IDS_CSV="$(join_csv "${FIXTURE_IDS_ARRAY[@]}")"
MODE_IDS_CSV="$(join_csv "${MODE_IDS_ARRAY[@]}")"
printf -v REPLAY_COMMAND \
    'cd %q && GOLDEN_DIR=%q SYNC_GOLDEN_DIR=%q OUTPUT_DIR=%q FIXTURE_IDS=%q MODE_IDS=%q CONCURRENCY=%q SEED=%q SCALE=%q CARGO_PROFILE=%q RCH_TARGET_DIR=%q bash scripts/verify_bd_db300_4_1_hot_path_profiles.sh' \
    "${WORKSPACE_ROOT}" "${SOURCE_GOLDEN_DIR}" "${SYNC_GOLDEN_DIR}" "${OUTPUT_DIR}" \
    "${FIXTURE_IDS_CSV}" "${MODE_IDS_CSV}" "${CONCURRENCY}" "${SEED}" "${SCALE}" \
    "${CARGO_PROFILE}" "${RCH_TARGET_DIR}"
log_event "INFO" "plan" "fixtures=${#FIXTURE_IDS_ARRAY[@]} modes=${#MODE_IDS_ARRAY[@]} expected_runs=${expected_runs}"

for fixture_id in "${FIXTURE_IDS_ARRAY[@]}"; do
    copy_fixture_seed "${fixture_id}"
done
log_event "INFO" "inputs" "prepared synced fixture seed directory at ${SYNC_GOLDEN_DIR}"

for mode_id in "${MODE_IDS_ARRAY[@]}"; do
    for fixture_id in "${FIXTURE_IDS_ARRAY[@]}"; do
        run_hot_profile "${fixture_id}" "${mode_id}"
    done
done

build_scenario_profiles
build_opcode_profile_packs
build_subsystem_profile_packs
build_benchmark_context
build_actionable_ranking
build_summary_md
build_report_json

jq -e '.runs | length >= 1' "${SCENARIO_PROFILES_JSON}" >/dev/null
jq -e '.named_hotspots | length >= 1' "${ACTIONABLE_RANKING_JSON}" >/dev/null
jq -e '.cost_components | length >= 1' "${ACTIONABLE_RANKING_JSON}" >/dev/null
jq -e '.allocator_pressure | length >= 1' "${ACTIONABLE_RANKING_JSON}" >/dev/null
jq -e '.runs | length == '"${expected_runs}" "${BENCHMARK_CONTEXT_JSON}" >/dev/null

log_event "INFO" "complete" "inline D1 hot-path campaign completed"
echo "RUN_ID:              ${RUN_ID}"
echo "Source golden dir:   ${SOURCE_GOLDEN_DIR}"
echo "Synced golden dir:   ${SYNC_GOLDEN_DIR}"
echo "Run records:         ${RUN_RECORDS_JSONL}"
echo "Scenario profiles:   ${SCENARIO_PROFILES_JSON}"
echo "Actionable ranking:  ${ACTIONABLE_RANKING_JSON}"
echo "Benchmark context:   ${BENCHMARK_CONTEXT_JSON}"
echo "Summary:             ${SUMMARY_MD}"
echo "Report:              ${REPORT_JSON}"
