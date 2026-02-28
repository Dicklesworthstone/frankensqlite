#!/usr/bin/env bash
# verify_differential_ci_lane.sh — deterministic differential CI lane (bd-mblr.7.1.3)
#
# Goals:
# 1. Run deterministic differential manifest generation twice per lane.
# 2. Validate reproducibility (byte-for-byte manifest + summary parity).
# 3. Emit machine-readable report for CI summaries and dashboards.
# 4. Keep lane in warning mode for semantic mismatches (exit 0/1 both accepted).
# 5. Run oracle preflight doctor before lane execution (red => hard-fail,
#    yellow => explicit non-certifying warning).
#
# Usage:
#   ./scripts/verify_differential_ci_lane.sh [--lane smoke|expanded] [--json]
#   ./scripts/verify_differential_ci_lane.sh --lane smoke --seed 424242 --generated-unix-ms 1700000000000
# Env overrides:
#   DIFF_LANE_USE_RCH=1
#   DIFF_LANE_FORCE_RCH=1     Force rch execution even when local runner binaries exist.
#   DIFF_LANE_RUNNER_BIN=target/debug/differential_manifest_runner
#   DIFF_DOCTOR_RUNNER_BIN=target/debug/oracle_preflight_doctor_runner

set -euo pipefail

WORKSPACE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BEAD_ID="bd-mblr.7.1.3"
JSON_OUTPUT=false
LANE_ID="smoke"
ROOT_SEED="${DIFF_LANE_SEED:-}"
GENERATED_UNIX_MS="${DIFF_LANE_GENERATED_UNIX_MS:-1700000000000}"
SCENARIO_ID="DIFF-CI-713"
MAX_ENTRIES=""
MAX_CASES_PER_ENTRY=""
FORCE_RCH="${DIFF_LANE_FORCE_RCH:-0}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --json)
      JSON_OUTPUT=true
      shift
      ;;
    --lane)
      if [[ $# -lt 2 ]]; then
        echo "ERROR: --lane requires a value" >&2
        exit 2
      fi
      LANE_ID="$2"
      shift 2
      ;;
    --seed)
      if [[ $# -lt 2 ]]; then
        echo "ERROR: --seed requires a value" >&2
        exit 2
      fi
      ROOT_SEED="$2"
      shift 2
      ;;
    --generated-unix-ms)
      if [[ $# -lt 2 ]]; then
        echo "ERROR: --generated-unix-ms requires a value" >&2
        exit 2
      fi
      GENERATED_UNIX_MS="$2"
      shift 2
      ;;
    *)
      echo "ERROR: unknown argument '$1'" >&2
      exit 2
      ;;
  esac
done

case "${LANE_ID}" in
  smoke)
    ROOT_SEED="${ROOT_SEED:-424242}"
    MAX_ENTRIES=32
    MAX_CASES_PER_ENTRY=4
    ;;
  expanded)
    ROOT_SEED="${ROOT_SEED:-525252}"
    MAX_ENTRIES=160
    MAX_CASES_PER_ENTRY=12
    ;;
  *)
    echo "ERROR: --lane must be one of: smoke, expanded" >&2
    exit 2
    ;;
esac

RUN_ROOT="${WORKSPACE_ROOT}/artifacts/differential-ci/${LANE_ID}"
RUN_A="${RUN_ROOT}/run-a"
RUN_B="${RUN_ROOT}/run-b"
MANIFEST_A="${RUN_A}/differential_manifest.json"
MANIFEST_B="${RUN_B}/differential_manifest.json"
SUMMARY_A="${RUN_A}/differential_manifest.md"
SUMMARY_B="${RUN_B}/differential_manifest.md"
RUN_A_LOG="${RUN_A}/differential_manifest_runner.log"
RUN_B_LOG="${RUN_B}/differential_manifest_runner.log"
DOCTOR_DIR="${RUN_ROOT}/doctor"
DOCTOR_JSON="${DOCTOR_DIR}/oracle_preflight_doctor.json"
DOCTOR_MD="${DOCTOR_DIR}/oracle_preflight_doctor.md"
DOCTOR_LOG="${DOCTOR_DIR}/oracle_preflight_doctor_runner.log"
FIXTURE_ROOT_MANIFEST="${WORKSPACE_ROOT}/corpus_manifest.toml"

mkdir -p "${RUN_A}" "${RUN_B}" "${DOCTOR_DIR}"

if [[ ! -f "${FIXTURE_ROOT_MANIFEST}" ]]; then
  echo "ERROR: canonical fixture-root manifest missing at ${FIXTURE_ROOT_MANIFEST}" >&2
  exit 1
fi
FIXTURE_ROOT_MANIFEST_SHA256="$(sha256sum "${FIXTURE_ROOT_MANIFEST}" | awk '{print $1}')"

RUN_ID="${BEAD_ID}-${LANE_ID}-seed-${ROOT_SEED}"
TRACE_ID="trace-$(printf '%s' "${RUN_ID}" | sha256sum | awk '{print $1}' | cut -c1-16)"
COMMIT_SHA="$(git -C "${WORKSPACE_ROOT}" rev-parse HEAD 2>/dev/null || echo unknown)"

resolve_runner_path() {
  local candidate="$1"
  if [[ -x "${candidate}" ]]; then
    printf '%s\n' "${candidate}"
    return 0
  fi
  if [[ "${candidate}" != /* ]] && [[ -x "${WORKSPACE_ROOT}/${candidate}" ]]; then
    printf '%s\n' "${WORKSPACE_ROOT}/${candidate}"
    return 0
  fi
  return 1
}

RUNNER_BIN="${DIFF_LANE_RUNNER_BIN:-}"
RUNNER=(cargo run -p fsqlite-harness --bin differential_manifest_runner --)
if [[ "${FORCE_RCH}" == "1" ]]; then
  if ! command -v rch >/dev/null 2>&1; then
    echo "ERROR: DIFF_LANE_FORCE_RCH=1 but rch is unavailable" >&2
    exit 2
  fi
  RUNNER=(rch exec -- cargo run -p fsqlite-harness --bin differential_manifest_runner --)
elif [[ -n "${RUNNER_BIN}" ]]; then
  if ! RUNNER_BIN="$(resolve_runner_path "${RUNNER_BIN}")"; then
    echo "ERROR: DIFF_LANE_RUNNER_BIN is not executable: ${RUNNER_BIN}" >&2
    exit 2
  fi
  RUNNER=("${RUNNER_BIN}")
elif [[ -n "${CARGO_TARGET_DIR:-}" ]] && RUNNER_BIN="$(resolve_runner_path "${CARGO_TARGET_DIR}/debug/differential_manifest_runner")"; then
  RUNNER=("${RUNNER_BIN}")
elif RUNNER_BIN="$(resolve_runner_path "/data/tmp/cargo-target/debug/differential_manifest_runner")"; then
  RUNNER=("${RUNNER_BIN}")
elif RUNNER_BIN="$(resolve_runner_path "target/debug/differential_manifest_runner")"; then
  RUNNER=("${RUNNER_BIN}")
elif [[ "${DIFF_LANE_USE_RCH:-0}" == "1" ]] && command -v rch >/dev/null 2>&1; then
  RUNNER=(rch exec -- cargo run -p fsqlite-harness --bin differential_manifest_runner --)
fi

DOCTOR_RUNNER_BIN="${DIFF_DOCTOR_RUNNER_BIN:-}"
DOCTOR_RUNNER=(cargo run -p fsqlite-harness --bin oracle_preflight_doctor_runner --)
if [[ "${FORCE_RCH}" == "1" ]]; then
  if ! command -v rch >/dev/null 2>&1; then
    echo "ERROR: DIFF_LANE_FORCE_RCH=1 but rch is unavailable" >&2
    exit 2
  fi
  DOCTOR_RUNNER=(rch exec -- cargo run -p fsqlite-harness --bin oracle_preflight_doctor_runner --)
elif [[ -n "${DOCTOR_RUNNER_BIN}" ]]; then
  if ! DOCTOR_RUNNER_BIN="$(resolve_runner_path "${DOCTOR_RUNNER_BIN}")"; then
    echo "ERROR: DIFF_DOCTOR_RUNNER_BIN is not executable: ${DOCTOR_RUNNER_BIN}" >&2
    exit 2
  fi
  DOCTOR_RUNNER=("${DOCTOR_RUNNER_BIN}")
elif [[ -n "${CARGO_TARGET_DIR:-}" ]] && DOCTOR_RUNNER_BIN="$(resolve_runner_path "${CARGO_TARGET_DIR}/debug/oracle_preflight_doctor_runner")"; then
  DOCTOR_RUNNER=("${DOCTOR_RUNNER_BIN}")
elif DOCTOR_RUNNER_BIN="$(resolve_runner_path "/data/tmp/cargo-target/debug/oracle_preflight_doctor_runner")"; then
  DOCTOR_RUNNER=("${DOCTOR_RUNNER_BIN}")
elif DOCTOR_RUNNER_BIN="$(resolve_runner_path "target/debug/oracle_preflight_doctor_runner")"; then
  DOCTOR_RUNNER=("${DOCTOR_RUNNER_BIN}")
elif [[ "${DIFF_LANE_USE_RCH:-0}" == "1" ]] && command -v rch >/dev/null 2>&1; then
  DOCTOR_RUNNER=(rch exec -- cargo run -p fsqlite-harness --bin oracle_preflight_doctor_runner --)
fi

COMMON_ARGS=(
  --workspace-root "${WORKSPACE_ROOT}"
  --fixture-root-manifest "${FIXTURE_ROOT_MANIFEST}"
  --run-id "${RUN_ID}"
  --trace-id "${TRACE_ID}"
  --scenario-id "${SCENARIO_ID}"
  --root-seed "${ROOT_SEED}"
  --max-cases-per-entry "${MAX_CASES_PER_ENTRY}"
  --max-entries "${MAX_ENTRIES}"
  --generated-unix-ms "${GENERATED_UNIX_MS}"
)

run_manifest() {
  local output_json="$1"
  local output_human="$2"
  local runner_log="$3"
  local run_status

  set +e
  "${RUNNER[@]}" \
    "${COMMON_ARGS[@]}" \
    --output-json "${output_json}" \
    --output-human "${output_human}" \
    >"${runner_log}" 2>&1
  run_status=$?
  set -e

  # 0 => no divergences; 1 => divergences present (warning-mode acceptable)
  if [[ ${run_status} -ne 0 && ${run_status} -ne 1 ]]; then
    echo "ERROR: differential_manifest_runner failed unexpectedly (exit=${run_status})" >&2
    echo "----- runner log: ${runner_log} -----" >&2
    cat "${runner_log}" >&2
    exit 1
  fi

  printf '%s' "${run_status}"
}

run_doctor() {
  local doctor_status

  set +e
  "${DOCTOR_RUNNER[@]}" \
    --workspace-root "${WORKSPACE_ROOT}" \
    --fixtures-dir "${WORKSPACE_ROOT}/crates/fsqlite-harness/conformance" \
    --fixture-manifest-path "${FIXTURE_ROOT_MANIFEST}" \
    --run-id "${RUN_ID}-doctor" \
    --trace-id "${TRACE_ID}-doctor" \
    --scenario-id "${SCENARIO_ID}-doctor" \
    --seed "${ROOT_SEED}" \
    --generated-unix-ms "${GENERATED_UNIX_MS}" \
    --output-json "${DOCTOR_JSON}" \
    --output-human "${DOCTOR_MD}" \
    >"${DOCTOR_LOG}" 2>&1
  doctor_status=$?
  set -e

  # 0 => green (certifying), 1 => yellow (non-certifying warning), 2 => red (blocking)
  if [[ ${doctor_status} -eq 2 ]]; then
    echo "ERROR: oracle preflight doctor reported red (blocking) condition (exit=${doctor_status})" >&2
    echo "----- doctor log: ${DOCTOR_LOG} -----" >&2
    cat "${DOCTOR_LOG}" >&2
    exit 1
  fi

  if [[ ${doctor_status} -ne 0 && ${doctor_status} -ne 1 ]]; then
    echo "ERROR: oracle preflight doctor failed unexpectedly (exit=${doctor_status})" >&2
    echo "----- doctor log: ${DOCTOR_LOG} -----" >&2
    cat "${DOCTOR_LOG}" >&2
    exit 1
  fi

  if [[ ! -f "${DOCTOR_JSON}" ]]; then
    echo "ERROR: oracle preflight doctor output missing at ${DOCTOR_JSON}" >&2
    exit 1
  fi

  printf '%s' "${doctor_status}"
}

doctor_status="$(run_doctor)"
DOCTOR_OUTCOME="$(jq -r '.outcome' "${DOCTOR_JSON}")"
DOCTOR_CERTIFYING="$(jq -r '.certifying' "${DOCTOR_JSON}")"

run_a_status="$(run_manifest "${MANIFEST_A}" "${SUMMARY_A}" "${RUN_A_LOG}")"
run_b_status="$(run_manifest "${MANIFEST_B}" "${SUMMARY_B}" "${RUN_B_LOG}")"

if [[ "${run_a_status}" != "${run_b_status}" ]]; then
  echo "ERROR: differential lane reruns produced different exit codes (run-a=${run_a_status}, run-b=${run_b_status})" >&2
  exit 1
fi

if [[ ! -f "${MANIFEST_A}" || ! -f "${MANIFEST_B}" ]]; then
  echo "ERROR: differential manifest output missing" >&2
  exit 1
fi

if [[ ! -f "${SUMMARY_A}" || ! -f "${SUMMARY_B}" ]]; then
  echo "ERROR: differential human summary output missing" >&2
  exit 1
fi

jq -e \
  --arg lane_id "${LANE_ID}" \
  --arg run_id "${RUN_ID}" \
  --arg trace_id "${TRACE_ID}" \
  --arg fixture_root_manifest_path "${FIXTURE_ROOT_MANIFEST}" \
  --arg fixture_root_manifest_sha256 "${FIXTURE_ROOT_MANIFEST_SHA256}" \
  --argjson seed "${ROOT_SEED}" \
  --arg scenario_id "${SCENARIO_ID}" \
  '
    .schema_version == 1 and
    .bead_id == "bd-mblr.7.1.2" and
    .run_id == $run_id and
    .trace_id == $trace_id and
    .fixture_root_manifest_path == $fixture_root_manifest_path and
    .fixture_root_manifest_sha256 == $fixture_root_manifest_sha256 and
    .scenario_id == $scenario_id and
    .root_seed == $seed and
    (.run_report.total_cases >= 0) and
    (.run_report.passed >= 0) and
    (.run_report.diverged >= 0) and
    (.run_report.deduplicated.total_before_dedup >= 0) and
    (.replay.command | type == "string" and length > 0 and (contains("\n") | not) and (contains("\r") | not)) and
    ((.run_report.diverged == 0) or (.first_failure != null and (.first_failure.replay_command | type == "string" and length > 0 and (contains("\n") | not) and (contains("\r") | not)))) and
    ((.run_report.passed == 0) or ((.sampled_passing_replays | length) > 0 and (.sampled_passing_replays | all(.replay_command | type == "string" and length > 0 and (contains("\n") | not) and (contains("\r") | not)))))
  ' "${MANIFEST_A}" >/dev/null

if ! diff -u \
  <(jq -S '.' "${MANIFEST_A}") \
  <(jq -S '.' "${MANIFEST_B}") \
  >/dev/null; then
  echo "ERROR: deterministic replay check failed; manifests differ" >&2
  diff -u \
    <(jq -S '.' "${MANIFEST_A}") \
    <(jq -S '.' "${MANIFEST_B}") \
    >&2 || true
  exit 1
fi

if ! diff -u \
  <(sed -E 's/run-[ab]/run-x/g' "${SUMMARY_A}") \
  <(sed -E 's/run-[ab]/run-x/g' "${SUMMARY_B}") \
  >/dev/null; then
  echo "ERROR: deterministic replay check failed; summaries differ" >&2
  diff -u \
    <(sed -E 's/run-[ab]/run-x/g' "${SUMMARY_A}") \
    <(sed -E 's/run-[ab]/run-x/g' "${SUMMARY_B}") \
    >&2 || true
  exit 1
fi

REPLAY_COMMAND="$(jq -r '.replay.command' "${MANIFEST_A}")"
FIRST_FAILURE_REPLAY_COMMAND="$(jq -r '.first_failure.replay_command // ""' "${MANIFEST_A}")"
SAMPLED_PASSING_REPLAY_COUNT="$(jq -r '.sampled_passing_replays | length' "${MANIFEST_A}")"
TOTAL_CASES="$(jq -r '.run_report.total_cases' "${MANIFEST_A}")"
PASSED_CASES="$(jq -r '.run_report.passed' "${MANIFEST_A}")"
DIVERGED_CASES="$(jq -r '.run_report.diverged' "${MANIFEST_A}")"
DEDUP_UNIQUE_COUNT="$(jq -r '.run_report.deduplicated.unique_failures | length' "${MANIFEST_A}")"
DEDUP_TOTAL_COUNT="$(jq -r '.run_report.deduplicated.total_before_dedup' "${MANIFEST_A}")"
MISMATCH_CLASS_COUNTS="$(
  jq -c '
    .run_report.divergent_cases
    | map((.classification // "unknown") | tostring)
    | reduce .[] as $class ({}; .[$class] = ((.[$class] // 0) + 1))
  ' "${MANIFEST_A}"
)"

# Warning mode: semantic mismatches and yellow preflight findings are tracked but non-blocking.
if [[ "${run_a_status}" == "0" && "${doctor_status}" == "0" ]]; then
  LANE_OUTCOME="pass"
else
  LANE_OUTCOME="warn"
fi

PROMOTION_CRITERIA=(
  "two consecutive weeks of deterministic_match=true on mainline"
  "zero harness-infrastructure failures across smoke + expanded lanes"
  "mismatch class trends triaged with owners and replay artifacts attached"
)

if ${JSON_OUTPUT}; then
  jq -n \
    --arg schema_version "fsqlite.differential-ci-lane.v1" \
    --arg bead_id "${BEAD_ID}" \
    --arg lane_id "${LANE_ID}" \
    --arg lane_mode "warning" \
    --arg lane_outcome "${LANE_OUTCOME}" \
    --arg run_id "${RUN_ID}" \
    --arg trace_id "${TRACE_ID}" \
    --arg scenario_id "${SCENARIO_ID}" \
    --arg commit_sha "${COMMIT_SHA}" \
    --argjson root_seed "${ROOT_SEED}" \
    --argjson generated_unix_ms "${GENERATED_UNIX_MS}" \
    --arg fixture_root_manifest_path "${FIXTURE_ROOT_MANIFEST}" \
    --arg fixture_root_manifest_sha256 "${FIXTURE_ROOT_MANIFEST_SHA256}" \
    --argjson run_a_exit_code "${run_a_status}" \
    --argjson run_b_exit_code "${run_b_status}" \
    --argjson doctor_exit_code "${doctor_status}" \
    --arg doctor_outcome "${DOCTOR_OUTCOME}" \
    --arg doctor_certifying "${DOCTOR_CERTIFYING}" \
    --argjson deterministic_match true \
    --argjson total_cases "${TOTAL_CASES}" \
    --argjson passed_cases "${PASSED_CASES}" \
    --argjson diverged_cases "${DIVERGED_CASES}" \
    --argjson dedup_unique_count "${DEDUP_UNIQUE_COUNT}" \
    --argjson dedup_total_count "${DEDUP_TOTAL_COUNT}" \
    --arg replay_command "${REPLAY_COMMAND}" \
    --arg first_failure_replay_command "${FIRST_FAILURE_REPLAY_COMMAND}" \
    --argjson sampled_passing_replay_count "${SAMPLED_PASSING_REPLAY_COUNT}" \
    --arg mismatch_class_counts "${MISMATCH_CLASS_COUNTS}" \
    --arg manifest_a "${MANIFEST_A#${WORKSPACE_ROOT}/}" \
    --arg manifest_b "${MANIFEST_B#${WORKSPACE_ROOT}/}" \
    --arg summary_a "${SUMMARY_A#${WORKSPACE_ROOT}/}" \
    --arg summary_b "${SUMMARY_B#${WORKSPACE_ROOT}/}" \
    --arg runner_log_a "${RUN_A_LOG#${WORKSPACE_ROOT}/}" \
    --arg runner_log_b "${RUN_B_LOG#${WORKSPACE_ROOT}/}" \
    --arg doctor_json "${DOCTOR_JSON#${WORKSPACE_ROOT}/}" \
    --arg doctor_human "${DOCTOR_MD#${WORKSPACE_ROOT}/}" \
    --arg doctor_log "${DOCTOR_LOG#${WORKSPACE_ROOT}/}" \
    --argjson promotion_criteria "$(printf '%s\n' "${PROMOTION_CRITERIA[@]}" | jq -R . | jq -s .)" \
    '
      {
        schema_version: $schema_version,
        bead_id: $bead_id,
        lane: {
          id: $lane_id,
          mode: $lane_mode,
          outcome: $lane_outcome,
          promotion_criteria: $promotion_criteria
        },
        run: {
          run_id: $run_id,
          trace_id: $trace_id,
          scenario_id: $scenario_id,
          commit_sha: $commit_sha,
          root_seed: $root_seed,
          generated_unix_ms: $generated_unix_ms,
          fixture_root_manifest_path: $fixture_root_manifest_path,
          fixture_root_manifest_sha256: $fixture_root_manifest_sha256,
          doctor_exit_code: $doctor_exit_code,
          doctor_outcome: $doctor_outcome,
          doctor_certifying: ($doctor_certifying == "true"),
          run_a_exit_code: $run_a_exit_code,
          run_b_exit_code: $run_b_exit_code,
          deterministic_match: $deterministic_match
        },
        results: {
          total_cases: $total_cases,
          passed_cases: $passed_cases,
          diverged_cases: $diverged_cases,
          mismatch_class_counts: ($mismatch_class_counts | fromjson),
          deduplicated_unique_failures: $dedup_unique_count,
          deduplicated_total_failures: $dedup_total_count,
          sampled_passing_replay_count: $sampled_passing_replay_count
        },
        artifacts: {
          manifest_a: $manifest_a,
          manifest_b: $manifest_b,
          summary_a: $summary_a,
          summary_b: $summary_b,
          runner_log_a: $runner_log_a,
          runner_log_b: $runner_log_b,
          doctor_json: $doctor_json,
          doctor_human: $doctor_human,
          doctor_log: $doctor_log
        },
        replay_command: $replay_command,
        first_failure_replay_command: $first_failure_replay_command
      }
    '
else
  echo "=== Differential CI Lane Verification (${BEAD_ID}) ==="
  echo "Lane:                 ${LANE_ID} (warning mode)"
  echo "Run ID:               ${RUN_ID}"
  echo "Trace ID:             ${TRACE_ID}"
  echo "Scenario ID:          ${SCENARIO_ID}"
  echo "Commit SHA:           ${COMMIT_SHA}"
  echo "Root seed:            ${ROOT_SEED}"
  echo "Generated unix ms:    ${GENERATED_UNIX_MS}"
  echo "Fixture manifest:     ${FIXTURE_ROOT_MANIFEST#${WORKSPACE_ROOT}/}"
  echo "Fixture sha256:       ${FIXTURE_ROOT_MANIFEST_SHA256}"
  echo "Doctor exit code:     ${doctor_status}"
  echo "Doctor outcome:       ${DOCTOR_OUTCOME}"
  echo "Doctor certifying:    ${DOCTOR_CERTIFYING}"
  echo "Run A exit code:      ${run_a_status}"
  echo "Run B exit code:      ${run_b_status}"
  echo "Deterministic match:  true"
  echo "Lane outcome:         ${LANE_OUTCOME}"
  echo "Total cases:          ${TOTAL_CASES}"
  echo "Passed cases:         ${PASSED_CASES}"
  echo "Diverged cases:       ${DIVERGED_CASES}"
  echo "Dedup unique:         ${DEDUP_UNIQUE_COUNT}"
  echo "Dedup total:          ${DEDUP_TOTAL_COUNT}"
  echo "Mismatch classes:     ${MISMATCH_CLASS_COUNTS}"
  echo "Sampled pass replays: ${SAMPLED_PASSING_REPLAY_COUNT}"
  echo "Manifest A:           ${MANIFEST_A#${WORKSPACE_ROOT}/}"
  echo "Manifest B:           ${MANIFEST_B#${WORKSPACE_ROOT}/}"
  echo "Runner log A:         ${RUN_A_LOG#${WORKSPACE_ROOT}/}"
  echo "Runner log B:         ${RUN_B_LOG#${WORKSPACE_ROOT}/}"
  echo "Doctor report:        ${DOCTOR_JSON#${WORKSPACE_ROOT}/}"
  echo "Doctor log:           ${DOCTOR_LOG#${WORKSPACE_ROOT}/}"
  echo "Promotion criteria:"
  for criterion in "${PROMOTION_CRITERIA[@]}"; do
    echo "  - ${criterion}"
  done
  echo "Replay command:"
  echo "  ${REPLAY_COMMAND}"
  if [[ -n "${FIRST_FAILURE_REPLAY_COMMAND}" ]]; then
    echo "First-failure replay command:"
    echo "  ${FIRST_FAILURE_REPLAY_COMMAND}"
  fi
fi
