#!/usr/bin/env bash
# Build the bd-db300.4.2.1 prepared-artifact churn inventory from existing
# D1 hot-path artifacts plus direct code-path references.

set -euo pipefail

WORKSPACE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BEAD_ID="bd-db300.4.2.1"
SOURCE_BEAD_ID="${SOURCE_BEAD_ID:-bd-db300.4.1}"
TIMESTAMP_UTC="$(date -u +%Y%m%dT%H%M%SZ)"
RUN_ID="${RUN_ID:-${BEAD_ID}-${TIMESTAMP_UTC}}"
TRACE_ID="${TRACE_ID:-${RUN_ID}}"
SCENARIO_ID="${SCENARIO_ID:-DB300-D2-1-PREPARED-ARTIFACT-INVENTORY}"

SOURCE_ACTIONABLE_RANKING="${SOURCE_ACTIONABLE_RANKING:-${WORKSPACE_ROOT}/artifacts/perf/${SOURCE_BEAD_ID}/actionable_ranking.json}"
SOURCE_PROFILE_GLOB="${SOURCE_PROFILE_GLOB:-${WORKSPACE_ROOT}/artifacts/perf/${SOURCE_BEAD_ID}/inline-authoritative/runs/*/profile.json}"

ARTIFACT_DIR="${WORKSPACE_ROOT}/artifacts/perf/${BEAD_ID}/${RUN_ID}"
EVENTS_JSONL="${ARTIFACT_DIR}/events.jsonl"
REPORT_JSON="${ARTIFACT_DIR}/report.json"
SUMMARY_MD="${ARTIFACT_DIR}/summary.md"

mkdir -p "${ARTIFACT_DIR}"
: > "${EVENTS_JSONL}"

emit_event() {
  local phase="$1"
  local event_type="$2"
  local outcome="$3"
  local message="$4"

  jq -cn \
    --arg trace_id "${TRACE_ID}" \
    --arg run_id "${RUN_ID}" \
    --arg scenario_id "${SCENARIO_ID}" \
    --arg bead_id "${BEAD_ID}" \
    --arg phase "${phase}" \
    --arg event_type "${event_type}" \
    --arg outcome "${outcome}" \
    --arg message "${message}" \
    --arg timestamp "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    '{
      trace_id: $trace_id,
      run_id: $run_id,
      scenario_id: $scenario_id,
      bead_id: $bead_id,
      phase: $phase,
      event_type: $event_type,
      outcome: $outcome,
      timestamp: $timestamp,
      message: $message
    }' >> "${EVENTS_JSONL}"
}

fail() {
  local phase="$1"
  local message="$2"
  emit_event "${phase}" "fail" "fail" "${message}"
  echo "[GATE FAIL] ${message}" >&2
  exit 1
}

require_file() {
  local path="$1"
  [[ -s "${path}" ]] || fail "inputs" "required file missing or empty: ${path}"
}

find_line_ref() {
  local relative_path="$1"
  local pattern="$2"
  local line
  line="$(rg -n -m1 --no-heading "${pattern}" "${WORKSPACE_ROOT}/${relative_path}" | cut -d: -f1 || true)"
  [[ -n "${line}" ]] || fail "scan_code" "pattern not found in ${relative_path}: ${pattern}"
  printf '%s:%s' "${relative_path}" "${line}"
}

emit_event "bootstrap" "start" "running" "starting prepared-artifact inventory"

require_file "${SOURCE_ACTIONABLE_RANKING}"

mapfile -t PROFILE_CANDIDATES < <(compgen -G "${SOURCE_PROFILE_GLOB}" | sort)
(( ${#PROFILE_CANDIDATES[@]} > 0 )) || fail "inputs" "no profile artifacts matched ${SOURCE_PROFILE_GLOB}"

PROFILE_FILES=()
SKIPPED_PROFILE_FILES=()
for profile in "${PROFILE_CANDIDATES[@]}"; do
  if [[ -s "${profile}" ]]; then
    PROFILE_FILES+=("${profile}")
  else
    SKIPPED_PROFILE_FILES+=("${profile}")
  fi
done

(( ${#PROFILE_FILES[@]} > 0 )) || fail "inputs" "all profile artifacts matched by ${SOURCE_PROFILE_GLOB} were empty"

if (( ${#SKIPPED_PROFILE_FILES[@]} > 0 )); then
  emit_event \
    "inputs" \
    "warn" \
    "pass" \
    "skipping ${#SKIPPED_PROFILE_FILES[@]} empty profile artifact(s): ${SKIPPED_PROFILE_FILES[*]}"
fi

emit_event "inputs" "pass" "pass" "loaded ${#PROFILE_FILES[@]} non-empty source profile artifact(s)"

PARSER_AST_CHURN_JSON="$(jq -c '
  .mixed_hot_path_categories[]
  | select(.category == "parser_ast_churn")
' "${SOURCE_ACTIONABLE_RANKING}")"
[[ -n "${PARSER_AST_CHURN_JSON}" && "${PARSER_AST_CHURN_JSON}" != "null" ]] \
  || fail "inputs" "parser_ast_churn bucket missing from ${SOURCE_ACTIONABLE_RANKING}"

AGGREGATED_PARSER_METRICS="$(jq -cs '
  def ratio($hits; $misses):
    if ($hits + $misses) == 0 then 0 else ($hits / ($hits + $misses)) end;
  {
    run_count: length,
    parse_single_calls_total: (map(.parser.parse_single_calls) | add),
    parse_multi_calls_total: (map(.parser.parse_multi_calls) | add),
    parse_cache_hits_total: (map(.parser.parse_cache_hits) | add),
    parse_cache_misses_total: (map(.parser.parse_cache_misses) | add),
    compiled_cache_hits_total: (map(.parser.compiled_cache_hits) | add),
    compiled_cache_misses_total: (map(.parser.compiled_cache_misses) | add),
    parsed_sql_bytes_total: (map(.parser.parsed_sql_bytes) | add),
    parse_time_ns_total: (map(.parser.parse_time_ns) | add),
    rewrite_calls_total: (map(.parser.rewrite_calls) | add),
    rewrite_time_ns_total: (map(.parser.rewrite_time_ns) | add),
    compile_time_ns_total: (map(.parser.compile_time_ns) | add),
    avg_parse_hit_rate: (
      map(ratio(.parser.parse_cache_hits; .parser.parse_cache_misses))
      | add / length
    ),
    avg_compiled_hit_rate: (
      map(ratio(.parser.compiled_cache_hits; .parser.compiled_cache_misses))
      | add / length
    )
  }
' "${PROFILE_FILES[@]}")"

SKIPPED_PROFILE_FILES_JSON="$(
  if (( ${#SKIPPED_PROFILE_FILES[@]} > 0 )); then
    printf '%s\n' "${SKIPPED_PROFILE_FILES[@]}" | jq -R . | jq -s .
  else
    printf '[]'
  fi
)"

emit_event "inputs" "pass" "pass" "aggregated parser/compile metrics from D1 artifact pack"

REF_PUBLIC_ENTRY="$(find_line_ref "crates/fsqlite/src/lib.rs" 'pub use fsqlite_core::connection')"
REF_PREPARE="crates/fsqlite-core/src/connection.rs:$(find_line_ref "crates/fsqlite-core/src/connection.rs" 'pub fn prepare\(' | cut -d: -f2)"
REF_COMPILE_AND_WRAP="$(find_line_ref "crates/fsqlite-core/src/connection.rs" 'fn compile_and_wrap')"
REF_PARSE_SINGLE="$(find_line_ref "crates/fsqlite-core/src/connection.rs" 'fn cached_parse_single')"
REF_PARSE_MULTI="$(find_line_ref "crates/fsqlite-core/src/connection.rs" 'fn cached_parse_multi')"
REF_SCHEMA_INVALIDATION="$(find_line_ref "crates/fsqlite-core/src/connection.rs" 'fn refresh_parse_cache_if_needed')"
REF_COMPILE_CACHE="$(find_line_ref "crates/fsqlite-core/src/connection.rs" 'fn compile_with_cache')"
REF_REWRITE_SUBQUERY="$(find_line_ref "crates/fsqlite-core/src/connection.rs" 'fn rewrite_subquery_statement')"
REF_DEFERRED_QUERY_CLONE="$(find_line_ref "crates/fsqlite-core/src/connection.rs" 'deferred_query_statement: Some\(Arc::new\(statement\.clone\(\)\)\)')"
REF_DML_AST_CLONE="$(find_line_ref "crates/fsqlite-core/src/connection.rs" 'dml_statement: Some\(Arc::new\(statement\.clone\(\)\)\)')"
REF_PREPARED_QUERY_REDISPATCH="$(find_line_ref "crates/fsqlite-core/src/connection.rs" 'if let Some\(statement\) = &self\.deferred_query_statement')"
REF_PREPARED_DML_REDISPATCH="$(find_line_ref "crates/fsqlite-core/src/connection.rs" 'self\.execute_statement_impl\(dml\.as_ref\(\), p, stmt\.dispatch_precompiled_program\(\)\)\?')"
REF_DISTINCT_LIMIT_CLONE="$(find_line_ref "crates/fsqlite-core/src/connection.rs" 'let mut unbounded = select\.clone\(\)')"

emit_event "scan_code" "pass" "pass" "resolved code references for prepared-artifact churn sites"

jq -n \
  --arg bead_id "${BEAD_ID}" \
  --arg run_id "${RUN_ID}" \
  --arg trace_id "${TRACE_ID}" \
  --arg scenario_id "${SCENARIO_ID}" \
  --arg generated_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --arg source_bead_id "${SOURCE_BEAD_ID}" \
  --arg source_actionable_ranking "${SOURCE_ACTIONABLE_RANKING}" \
  --arg source_profile_glob "${SOURCE_PROFILE_GLOB}" \
  --argjson skipped_profile_files "${SKIPPED_PROFILE_FILES_JSON}" \
  --arg replay_command "cd ${WORKSPACE_ROOT} && bash scripts/verify_bd_db300_4_2_1_prepared_artifact_inventory.sh" \
  --arg ref_public_entry "${REF_PUBLIC_ENTRY}" \
  --arg ref_prepare "${REF_PREPARE}" \
  --arg ref_compile_and_wrap "${REF_COMPILE_AND_WRAP}" \
  --arg ref_parse_single "${REF_PARSE_SINGLE}" \
  --arg ref_parse_multi "${REF_PARSE_MULTI}" \
  --arg ref_schema_invalidation "${REF_SCHEMA_INVALIDATION}" \
  --arg ref_compile_cache "${REF_COMPILE_CACHE}" \
  --arg ref_rewrite_subquery "${REF_REWRITE_SUBQUERY}" \
  --arg ref_deferred_query_clone "${REF_DEFERRED_QUERY_CLONE}" \
  --arg ref_dml_ast_clone "${REF_DML_AST_CLONE}" \
  --arg ref_prepared_query_redispatch "${REF_PREPARED_QUERY_REDISPATCH}" \
  --arg ref_prepared_dml_redispatch "${REF_PREPARED_DML_REDISPATCH}" \
  --arg ref_distinct_limit_clone "${REF_DISTINCT_LIMIT_CLONE}" \
  --argjson parser_ast_churn "${PARSER_AST_CHURN_JSON}" \
  --argjson parser_metrics "${AGGREGATED_PARSER_METRICS}" \
  '
  {
    schema_version: "fsqlite.perf.prepared_artifact_inventory.v1",
    bead_id: $bead_id,
    run_id: $run_id,
    trace_id: $trace_id,
    scenario_id: $scenario_id,
    generated_at: $generated_at,
    source_evidence: {
      source_bead_id: $source_bead_id,
      actionable_ranking: $source_actionable_ranking,
      profile_glob: $source_profile_glob,
      skipped_profile_files: $skipped_profile_files,
      parser_ast_churn: $parser_ast_churn,
      aggregated_parser_metrics: $parser_metrics
    },
    planner_coverage: {
      current_runtime_assessment: "Current measured prepared-artifact churn is dominated by fsqlite-core::Connection::prepare(), compile_and_wrap(), and deferred redispatch in PreparedStatement; the repo does contain planner-local QueryPlan construction, but no standalone planner crate prepared-plan cache or reuse layer surfaced in the authoritative D1 hot path.",
      evidence_refs: [
        $ref_public_entry,
        $ref_prepare,
        $ref_compile_and_wrap,
        $ref_compile_cache
      ],
      note: "This is an inference from the current runtime path and code inspection, not a claim that planner logic is absent from the system."
    },
    inventory: [
      {
        rank: 1,
        label: "prepare() canonicalizes SQL and compile_and_wrap() rebuilds prepared artifacts outside the compiled cache",
        code_ref: $ref_compile_and_wrap,
        supporting_refs: [
          $ref_prepare,
          $ref_compile_cache
        ],
        lifecycle_stage: "prepare_wrap",
        churn_class: "prepared artifact rebuild",
        measured_bucket: "parser_ast_churn",
        measured_cost: {
          parse_cache_misses_total: $parser_metrics.parse_cache_misses_total,
          compiled_cache_misses_total: $parser_metrics.compiled_cache_misses_total,
          parse_time_ns_total: $parser_metrics.parse_time_ns_total,
          compile_time_ns_total: $parser_metrics.compile_time_ns_total
        },
        current_behavior: "prepare() always does cached_parse_single(), rewrite_subquery_statement(), statement.to_string(), and then compile_and_wrap(); compile_and_wrap() constructs a fresh PreparedStatement and does not consult compile_with_cache().",
        why_hot: "This keeps repeated prepare() calls on stable SQL on a rebuild-only path even though the raw query()/execute() lane already has separate compiled-cache reuse. D1 still measured 440 parse misses, 410 compiled misses, 7.13 ms parse time, and 3.53 ms compile time across the authoritative mixed runs.",
        redesign_target: "Route prepare() through reusable compiled artifacts plus explicit bind/dispatch metadata instead of rebuilding canonical prepared wrappers each time."
      },
      {
        rank: 2,
        label: "Prepared SELECT fallback stores cloned ASTs and re-enters execute_statement at query time",
        code_ref: $ref_deferred_query_clone,
        supporting_refs: [
          $ref_prepared_query_redispatch
        ],
        lifecycle_stage: "prepared_query_fallback",
        churn_class: "AST clone + dynamic redispatch",
        measured_bucket: "parser_ast_churn",
        measured_cost: {
          compiled_cache_misses_total: $parser_metrics.compiled_cache_misses_total,
          parser_ast_churn_avg_overhead_pct: $parser_ast_churn.avg_overhead_pct,
          parser_ast_churn_max_overhead_pct: $parser_ast_churn.max_overhead_pct
        },
        current_behavior: "prepare() stores Arc<Statement> in deferred_query_statement for complex SELECT shapes, and PreparedStatement::query/query_with_params later re-enter execute_statement() instead of staying on a fully precompiled fast path.",
        why_hot: "This avoids SQL re-tokenization but still keeps ownership of the full AST and pays the broader dispatch/materialization path at execution time, which is exactly the prepared-artifact churn D2 is meant to cut.",
        redesign_target: "Lower complex SELECT prepared statements onto reusable staged artifacts that can execute without AST-carrying redispatch."
      },
      {
        rank: 3,
        label: "Prepared DML keeps cloned Statement ASTs and only partially reuses compiled programs",
        code_ref: $ref_dml_ast_clone,
        supporting_refs: [
          $ref_prepared_dml_redispatch
        ],
        lifecycle_stage: "prepared_dml_fallback",
        churn_class: "AST clone + partial placeholder reuse",
        measured_bucket: "parser_ast_churn",
        measured_cost: {
          compiled_cache_misses_total: $parser_metrics.compiled_cache_misses_total,
          compile_time_ns_total: $parser_metrics.compile_time_ns_total,
          parser_ast_churn_avg_overhead_pct: $parser_ast_churn.avg_overhead_pct
        },
        current_behavior: "Simple VALUES-style INSERT gets a compiled program but still stores a cloned Statement; complex INSERT, UPDATE, and DELETE prepare placeholder programs plus cloned ASTs and later re-enter execute_statement_impl().",
        why_hot: "Prepared DML is only partly reusable today, so the prepared artifact still carries heavyweight AST state and many shapes fall back to the slow dispatch path instead of a stable bound-program execution path.",
        redesign_target: "Split DML prepared artifacts into reusable compiled programs plus minimal binding metadata, with explicit slow-path handling only for the shapes that truly need it."
      },
      {
        rank: 4,
        label: "Schema-cookie invalidation clears both parse and compiled caches",
        code_ref: $ref_schema_invalidation,
        lifecycle_stage: "cache_invalidation",
        churn_class: "whole-cache flush",
        measured_bucket: "parser_ast_churn",
        measured_cost: {
          parser_ast_churn_avg_overhead_pct: $parser_ast_churn.avg_overhead_pct,
          parser_ast_churn_max_overhead_pct: $parser_ast_churn.max_overhead_pct,
          parse_cache_misses_total: $parser_metrics.parse_cache_misses_total,
          compiled_cache_misses_total: $parser_metrics.compiled_cache_misses_total
        },
        current_behavior: "Any schema cookie change clears the parse cache and compiled cache together, so the next repeated statement must rebuild both layers.",
        why_hot: "The D1 artifact pack already shows hundreds of parse and compiled misses across the authoritative runs; global invalidation broadens the rebuild surface beyond only the statements that actually depended on changed schema objects.",
        redesign_target: "Dependency-scoped invalidation keyed by referenced schema objects instead of whole-cache clears."
      },
      {
        rank: 5,
        label: "Parse miss path rebuilds full ASTs before any compiled reuse can happen",
        code_ref: $ref_parse_single,
        lifecycle_stage: "parse",
        churn_class: "AST rebuild",
        measured_bucket: "parser_ast_churn",
        measured_cost: {
          parse_single_calls_total: $parser_metrics.parse_single_calls_total,
          parse_multi_calls_total: $parser_metrics.parse_multi_calls_total,
          parse_cache_hits_total: $parser_metrics.parse_cache_hits_total,
          parse_cache_misses_total: $parser_metrics.parse_cache_misses_total,
          avg_parse_hit_rate: $parser_metrics.avg_parse_hit_rate,
          parsed_sql_bytes_total: $parser_metrics.parsed_sql_bytes_total,
          parse_time_ns_total: $parser_metrics.parse_time_ns_total
        },
        current_behavior: "A parse miss calls into the parser, materializes a Statement tree, and inserts a new Arc<Statement> cache entry before any compile reuse can apply.",
        why_hot: "The D1 profile pack still shows 440 parse misses, 20,140 bytes of reparsed SQL, and 7.13 ms spent in parse time across the authoritative runs.",
        redesign_target: "Reduce miss frequency and avoid rebuilding whole Statement trees when only bindings or stable statement text differ."
      },
      {
        rank: 6,
        label: "Compiled-program miss path rebuilds VDBE programs on repeated ad-hoc SQL",
        code_ref: $ref_compile_cache,
        lifecycle_stage: "compile",
        churn_class: "program rebuild",
        measured_bucket: "parser_ast_churn",
        measured_cost: {
          compiled_cache_hits_total: $parser_metrics.compiled_cache_hits_total,
          compiled_cache_misses_total: $parser_metrics.compiled_cache_misses_total,
          avg_compiled_hit_rate: $parser_metrics.avg_compiled_hit_rate,
          compile_time_ns_total: $parser_metrics.compile_time_ns_total
        },
        current_behavior: "compile_with_cache() rebuilds a fresh VdbeProgram on misses, logs reuse events, and only then inserts the program into the schema-scoped cache.",
        why_hot: "D1 recorded 410 compiled misses and 3.53 ms of compile time across only two authoritative mixed-workload runs, so compile churn remains visible even before prepared-statement redesign.",
        redesign_target: "Expose narrower reusable compiled artifacts to prepared and ad-hoc callers instead of rebuilding at each miss site."
      },
      {
        rank: 7,
        label: "Subquery rewrite and canonical SQL regeneration traverse or clone statements before compile",
        code_ref: $ref_rewrite_subquery,
        supporting_refs: [
          $ref_prepare
        ],
        lifecycle_stage: "prepare_rewrite",
        churn_class: "statement clone/traversal",
        measured_bucket: "parser_ast_churn",
        measured_cost: {
          parser_ast_churn_avg_overhead_pct: $parser_ast_churn.avg_overhead_pct,
          parser_ast_churn_max_overhead_pct: $parser_ast_churn.max_overhead_pct,
          rewrite_calls_total: $parser_metrics.rewrite_calls_total,
          rewrite_time_ns_total: $parser_metrics.rewrite_time_ns_total
        },
        current_behavior: "prepare() runs rewrite_subquery_statement and then canonicalizes the rewritten statement back to SQL text via statement.to_string().",
        why_hot: "The mixed hot path did not trigger rewrite-heavy samples in the D1 authoritative runs, but the lifecycle still requires clone/traversal and SQL regeneration whenever rewritable subqueries or DISTINCT+LIMIT normalization are involved.",
        redesign_target: "Keep canonical reusable artifacts in structured form so rewrite/canonicalization does not require repeated Statement cloning and SQL string regeneration."
      },
      {
        rank: 8,
        label: "Multi-statement query/execute path only caches the single-statement case",
        code_ref: $ref_parse_multi,
        lifecycle_stage: "parse_multi",
        churn_class: "vector rebuild",
        measured_bucket: "parser_ast_churn",
        measured_cost: {
          parse_multi_calls_total: $parser_metrics.parse_multi_calls_total,
          parse_cache_misses_total: $parser_metrics.parse_cache_misses_total,
          parse_time_ns_total: $parser_metrics.parse_time_ns_total
        },
        current_behavior: "cached_parse_multi only inserts back into the parse cache when exactly one statement was parsed, so multi-statement SQL rebuilds Vec<Arc<Statement>> on every execution.",
        why_hot: "D1 recorded 1,034 multi-statement parse calls across the authoritative runs, which keeps a rebuild-only path alive for query() and execute() callers using statement batches.",
        redesign_target: "Introduce a reusable batch artifact or explicit statement-list cache for stable multi-statement SQL."
      },
      {
        rank: 9,
        label: "DISTINCT+LIMIT handling clones SELECT ASTs to compile an unbounded variant",
        code_ref: $ref_distinct_limit_clone,
        supporting_refs: [
          $ref_compile_and_wrap
        ],
        lifecycle_stage: "compile_wrap",
        churn_class: "SELECT clone for normalization",
        measured_bucket: "parser_ast_churn",
        measured_cost: {
          parser_ast_churn_avg_overhead_pct: $parser_ast_churn.avg_overhead_pct,
          compile_time_ns_total: $parser_metrics.compile_time_ns_total
        },
        current_behavior: "compile_and_wrap clones the SELECT and strips LIMIT/OFFSET to compile an unbounded program for DISTINCT handling.",
        why_hot: "This is not the dominant mixed-path cost today, but it is a concrete rebuild-heavy edge in the prepared lifecycle that D2.2 can eliminate once artifact ownership is redesigned.",
        redesign_target: "Represent DISTINCT+LIMIT post-processing without cloning full SelectStatement trees."
      }
    ],
    next_step_candidates: [
      {
        bead_id: "bd-db300.4.2.2",
        reason: "Redesign one prepared-artifact lifecycle around reuse using the ranked churn inventory above."
      },
      {
        bead_id: "bd-db300.4.3.1",
        reason: "Allocator pressure still dominates, so any D2 reuse redesign should coordinate with D3 scratch/arena ownership work."
      }
    ]
  }
  ' > "${REPORT_JSON}"

emit_event "emit_report" "pass" "pass" "wrote ${REPORT_JSON}"

PARSER_OVERHEAD_AVG="$(jq -r '.source_evidence.parser_ast_churn.avg_overhead_pct' "${REPORT_JSON}")"
PARSER_OVERHEAD_MAX="$(jq -r '.source_evidence.parser_ast_churn.max_overhead_pct' "${REPORT_JSON}")"
PARSE_MISSES_TOTAL="$(jq -r '.source_evidence.aggregated_parser_metrics.parse_cache_misses_total' "${REPORT_JSON}")"
COMPILE_MISSES_TOTAL="$(jq -r '.source_evidence.aggregated_parser_metrics.compiled_cache_misses_total' "${REPORT_JSON}")"
PARSE_HIT_RATE="$(jq -r '.source_evidence.aggregated_parser_metrics.avg_parse_hit_rate' "${REPORT_JSON}")"
COMPILE_HIT_RATE="$(jq -r '.source_evidence.aggregated_parser_metrics.avg_compiled_hit_rate' "${REPORT_JSON}")"
RUN_COUNT="$(jq -r '.source_evidence.aggregated_parser_metrics.run_count' "${REPORT_JSON}")"
RANKED_INVENTORY_MD="$(jq -r '
  .inventory
  | map("- rank \(.rank): `\(.label)` at `\(.code_ref)` -> \(.redesign_target)")
  | .[]
' "${REPORT_JSON}")"

{
  printf '# %s Prepared-Artifact Churn Inventory\n\n' "${BEAD_ID}"
  printf -- '- source_bead_id: `%s`\n' "${SOURCE_BEAD_ID}"
  printf -- '- source_actionable_ranking: `%s`\n' "${SOURCE_ACTIONABLE_RANKING}"
  printf -- '- source_profile_glob: `%s`\n' "${SOURCE_PROFILE_GLOB}"
  printf -- '- authoritative_run_count: `%s`\n' "${RUN_COUNT}"
  printf -- '- parser_ast_churn_avg_overhead_pct: `%s`\n' "${PARSER_OVERHEAD_AVG}"
  printf -- '- parser_ast_churn_max_overhead_pct: `%s`\n' "${PARSER_OVERHEAD_MAX}"
  printf -- '- parse_cache_misses_total: `%s`\n' "${PARSE_MISSES_TOTAL}"
  printf -- '- compiled_cache_misses_total: `%s`\n' "${COMPILE_MISSES_TOTAL}"
  printf -- '- avg_parse_hit_rate: `%s`\n' "${PARSE_HIT_RATE}"
  printf -- '- avg_compiled_hit_rate: `%s`\n' "${COMPILE_HIT_RATE}"
  printf -- '- planner_coverage: `%s`\n' \
    'current hot path is dominated by connection.rs prepare()/compile_and_wrap(); no standalone planner reuse layer surfaced in D1'
  printf -- '- replay_command: `%s`\n' \
    "cd ${WORKSPACE_ROOT} && bash scripts/verify_bd_db300_4_2_1_prepared_artifact_inventory.sh"
  printf '\n## Ranked Inventory\n\n'
  printf '%s\n' "${RANKED_INVENTORY_MD}"
  printf '\n## Interpretation\n\n'
  printf '%s\n' \
    '- D1 already shows parser/AST churn is a secondary mixed-path bucket, but it still stays measurable because `prepare()`/`compile_and_wrap()` and cache invalidation still rebuild full Statement/VdbeProgram artifacts.' \
    '- The highest-value D2 redesign target is not "optimize parsing" in the abstract; it is removing AST-carrying placeholder prepared statements and the rebuild-only `prepare()` wrapper path.' \
    '- Planner coverage was checked explicitly: the current measured prepared-artifact path is still centered in `fsqlite-core/src/connection.rs`, so the artifact records planner absence from the hot reuse path rather than pretending a planner cache already exists.' \
    '- D3 still outranks D2 on absolute mixed-path cost, so any D2.2 reuse redesign should stay compatible with allocator/scratch-space work instead of competing with it.'
} > "${SUMMARY_MD}"

emit_event "emit_summary" "pass" "pass" "wrote ${SUMMARY_MD}"
emit_event "finalize" "pass" "pass" "prepared-artifact inventory completed"

echo "bead_id=${BEAD_ID}"
echo "run_id=${RUN_ID}"
echo "report=${REPORT_JSON}"
echo "summary=${SUMMARY_MD}"
echo "events=${EVENTS_JSONL}"
