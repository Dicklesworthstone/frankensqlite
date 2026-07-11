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
REPORT_JSON="${RUN_ROOT}/differential_ci_lane_report.json"
DOCTOR_DIR="${RUN_ROOT}/doctor"
DOCTOR_JSON="${DOCTOR_DIR}/oracle_preflight_doctor.json"
DOCTOR_MD="${DOCTOR_DIR}/oracle_preflight_doctor.md"
DOCTOR_LOG="${DOCTOR_DIR}/oracle_preflight_doctor_runner.log"
FIXTURE_ROOT_MANIFEST="${WORKSPACE_ROOT}/docs/contracts/corpus_manifest.toml"
RCH_JSON_ARTIFACT_BEGIN="__FSQLITE_RCH_JSON_ARTIFACT_BEGIN__"
RCH_JSON_ARTIFACT_END="__FSQLITE_RCH_JSON_ARTIFACT_END__"
RCH_HUMAN_ARTIFACT_BEGIN="__FSQLITE_RCH_HUMAN_ARTIFACT_BEGIN__"
RCH_HUMAN_ARTIFACT_END="__FSQLITE_RCH_HUMAN_ARTIFACT_END__"

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

RCH_PREFIX=(rch exec --)
if [[ -n "${TMPDIR:-}" ]]; then
  if [[ "${TMPDIR}" != /* ]]; then
    TMPDIR="${WORKSPACE_ROOT}/${TMPDIR}"
  fi
  mkdir -p "${TMPDIR}"
  RCH_PREFIX+=(env "TMPDIR=${TMPDIR}")
fi
if [[ -n "${CARGO_TARGET_DIR:-}" ]]; then
  if [[ "${CARGO_TARGET_DIR}" != /* ]]; then
    CARGO_TARGET_DIR="${WORKSPACE_ROOT}/${CARGO_TARGET_DIR}"
  fi
  mkdir -p "${CARGO_TARGET_DIR}"
  if [[ ${#RCH_PREFIX[@]} -eq 3 ]]; then
    RCH_PREFIX+=(env)
  fi
  RCH_PREFIX+=("CARGO_TARGET_DIR=${CARGO_TARGET_DIR}")
fi
if [[ -n "${CFLAGS:-}" ]]; then
  if [[ ${#RCH_PREFIX[@]} -eq 3 ]]; then
    RCH_PREFIX+=(env)
  fi
  RCH_PREFIX+=("CFLAGS=${CFLAGS}")
fi
REMOTE_ARTIFACT_STREAM=0
if [[ "${FORCE_RCH}" == "1" || "${DIFF_LANE_USE_RCH:-0}" == "1" ]]; then
  REMOTE_ARTIFACT_STREAM=1
  if [[ ${#RCH_PREFIX[@]} -eq 3 ]]; then
    RCH_PREFIX+=(env)
  fi
  RCH_PREFIX+=("FSQLITE_RCH_STREAM_ARTIFACTS=1")
fi

RUNNER_BIN="${DIFF_LANE_RUNNER_BIN:-}"
RUNNER=(cargo run -p fsqlite-harness --bin differential_manifest_runner --)
if [[ "${FORCE_RCH}" == "1" ]]; then
  if ! command -v rch >/dev/null 2>&1; then
    echo "ERROR: DIFF_LANE_FORCE_RCH=1 but rch is unavailable" >&2
    exit 2
  fi
  RUNNER=("${RCH_PREFIX[@]}" cargo run -p fsqlite-harness --bin differential_manifest_runner --)
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
  RUNNER=("${RCH_PREFIX[@]}" cargo run -p fsqlite-harness --bin differential_manifest_runner --)
fi

DOCTOR_RUNNER_BIN="${DIFF_DOCTOR_RUNNER_BIN:-}"
DOCTOR_RUNNER=(cargo run -p fsqlite-harness --bin oracle_preflight_doctor_runner --)
if [[ "${FORCE_RCH}" == "1" ]]; then
  if ! command -v rch >/dev/null 2>&1; then
    echo "ERROR: DIFF_LANE_FORCE_RCH=1 but rch is unavailable" >&2
    exit 2
  fi
  DOCTOR_RUNNER=("${RCH_PREFIX[@]}" cargo run -p fsqlite-harness --bin oracle_preflight_doctor_runner --)
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
  DOCTOR_RUNNER=("${RCH_PREFIX[@]}" cargo run -p fsqlite-harness --bin oracle_preflight_doctor_runner --)
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

  if [[ "${REMOTE_ARTIFACT_STREAM}" == "1" ]]; then
    hydrate_marked_artifact \
      "${runner_log}" \
      "${RCH_JSON_ARTIFACT_BEGIN}" \
      "${RCH_JSON_ARTIFACT_END}" \
      "${output_json}" || true
    hydrate_marked_artifact \
      "${runner_log}" \
      "${RCH_HUMAN_ARTIFACT_BEGIN}" \
      "${RCH_HUMAN_ARTIFACT_END}" \
      "${output_human}" || true
  fi

  # 0 => no divergences; 1 => divergences present (warning-mode acceptable)
  if [[ ${run_status} -ne 0 && ${run_status} -ne 1 ]]; then
    echo "ERROR: differential_manifest_runner failed unexpectedly (exit=${run_status})" >&2
    echo "----- runner log: ${runner_log} -----" >&2
    cat "${runner_log}" >&2
    exit 1
  fi

  printf '%s' "${run_status}"
}

hydrate_marked_artifact() {
  local log_path="$1"
  local begin_marker="$2"
  local end_marker="$3"
  local output_path="$4"

  awk \
    -v begin_marker="${begin_marker}" \
    -v end_marker="${end_marker}" \
    '
      $0 == begin_marker { capture = 1; found = 1; next }
      $0 == end_marker { capture = 0; next }
      # The manifest runner returns exit 1 for accepted warning-mode divergences.
      # stderr is merged into the rch log, so the final warning line can arrive
      # before buffered stdout finishes and would otherwise corrupt the streamed
      # JSON artifact.
      capture && $0 == "ERROR differential_manifest_runner overall_pass=false" { next }
      capture { print }
      END { exit found ? 0 : 1 }
    ' \
    "${log_path}" >"${output_path}"
}

sha256_file() {
  local path="$1"
  sha256sum "${path}" | awk '{print $1}'
}

validate_manifest_runner_log_schema() {
  local log_path="$1"
  local run_label="$2"
  local log_line

  log_line="$(grep -E '^INFO differential_manifest_written ' "${log_path}" | tail -n 1 || true)"
  if [[ -z "${log_line}" ]]; then
    echo "ERROR: ${run_label} runner log is missing differential_manifest_written structured line: ${log_path}" >&2
    exit 1
  fi

  for required_field in \
    "run_id=${RUN_ID}" \
    "trace_id=${TRACE_ID}" \
    "scenario_id=${SCENARIO_ID}" \
    "seed=${ROOT_SEED}" \
    "first_failure_pointer=" \
    "first_failure_artifact_entries="; do
    if [[ "${log_line}" != *"${required_field}"* ]]; then
      echo "ERROR: ${run_label} runner log missing required field ${required_field}: ${log_line}" >&2
      exit 1
    fi
  done

  if [[ ! "${log_line}" =~ elapsed_ms=[0-9]+ ]]; then
    echo "ERROR: ${run_label} runner log has invalid elapsed_ms field: ${log_line}" >&2
    exit 1
  fi
  if [[ ! "${log_line}" =~ outcome=(pass|warn) ]]; then
    echo "ERROR: ${run_label} runner log has invalid outcome field: ${log_line}" >&2
    exit 1
  fi
  if [[ ! "${log_line}" =~ first_failure_domain=(none|parser|planner|vdbe|storage|harness|fixture) ]]; then
    echo "ERROR: ${run_label} runner log has invalid first_failure_domain field: ${log_line}" >&2
    exit 1
  fi
  if [[ ! "${log_line}" =~ first_failure_replay_present=(true|false) ]]; then
    echo "ERROR: ${run_label} runner log has invalid first_failure_replay_present field: ${log_line}" >&2
    exit 1
  fi
}

validate_ci_report_schema() {
  local report_path="$1"

  jq -e \
    --arg schema_version "fsqlite.differential-ci-lane.v1" \
    --arg bead_id "${BEAD_ID}" \
    --arg lane_id "${LANE_ID}" \
    --arg run_id "${RUN_ID}" \
    --arg trace_id "${TRACE_ID}" \
    --arg scenario_id "${SCENARIO_ID}" \
    --argjson root_seed "${ROOT_SEED}" \
    --argjson generated_unix_ms "${GENERATED_UNIX_MS}" \
    '
      def one_line:
        type == "string" and length > 0 and (contains("\n") | not) and (contains("\r") | not);
      def sha256_hex:
        type == "string" and test("^[0-9a-f]{64}$");
      .schema_version == $schema_version and
      .bead_id == $bead_id and
      .lane.id == $lane_id and
      .lane.mode == "warning" and
      (.lane.outcome | IN("pass", "warn")) and
      .run.run_id == $run_id and
      .run.trace_id == $trace_id and
      .run.scenario_id == $scenario_id and
      .run.root_seed == $root_seed and
      .run.generated_unix_ms == $generated_unix_ms and
      (.run.doctor_certifying | type == "boolean") and
      (.run.deterministic_match | type == "boolean") and
      (.results.total_cases | type == "number" and . >= 0) and
      (.results.passed_cases | type == "number" and . >= 0) and
      (.results.diverged_cases | type == "number" and . >= 0) and
      (.results.sampled_passing_replay_count | type == "number" and . >= 0) and
      (.sampled_passing_replays | type == "array") and
      ((.sampled_passing_replays | length) == .results.sampled_passing_replay_count) and
      (
        (.results.passed_cases == 0 and .results.sampled_passing_replay_count == 0) or
        (.results.passed_cases > 0 and .results.sampled_passing_replay_count > 0)
      ) and
      (
        all(.sampled_passing_replays[];
          (.case_id | one_line) and
          (.transform_name | one_line) and
          (.seed | type == "number" and . >= 0) and
          (.replay_command | one_line) and
          (.diagnostic_json_pointer | one_line) and
          (.artifact_entries | type == "array" and length > 0) and
          (all(.artifact_entries[]; one_line))
        )
      ) and
      (.artifacts.manifest_a | one_line) and
      (.artifacts.manifest_b | one_line) and
      (.artifacts.summary_a | one_line) and
      (.artifacts.summary_b | one_line) and
      (.artifacts.runner_log_a | one_line) and
      (.artifacts.runner_log_b | one_line) and
      (.artifacts.doctor_json | one_line) and
      (.artifacts.doctor_human | one_line) and
      (.artifacts.doctor_log | one_line) and
      (.artifacts.report_artifact | one_line) and
      (.artifacts.hashes.manifest_a_sha256 | sha256_hex) and
      (.artifacts.hashes.manifest_b_sha256 | sha256_hex) and
      (.artifacts.hashes.summary_a_sha256 | sha256_hex) and
      (.artifacts.hashes.summary_b_sha256 | sha256_hex) and
      (.artifacts.hashes.runner_log_a_sha256 | sha256_hex) and
      (.artifacts.hashes.runner_log_b_sha256 | sha256_hex) and
      (.artifacts.hashes.doctor_json_sha256 | sha256_hex) and
      (.artifacts.hashes.doctor_human_sha256 | sha256_hex) and
      (.artifacts.hashes.doctor_log_sha256 | sha256_hex) and
      (.replay_command | one_line) and
      (
        (.results.diverged_cases == 0 and .first_failure == null) or
        (
          (.results.diverged_cases > 0) and
          (.first_failure | type == "object") and
          (.first_failure.replay_command | one_line) and
          (.first_failure.root_cause_domain | IN("parser", "planner", "vdbe", "storage", "harness", "fixture")) and
          (.first_failure.diagnostic_json_pointer | one_line) and
          (.first_failure.remediation_playbook.summary | one_line) and
          (.first_failure.remediation_playbook.evidence_json_pointer | one_line) and
          (.first_failure.remediation_playbook.owner_hint | one_line) and
          (.first_failure.remediation_playbook.next_commands | type == "array" and length > 0) and
          (all(.first_failure.remediation_playbook.next_commands[]; one_line)) and
          (
            (.first_failure.minimal_reproduction_json_pointer == null) or
            (.first_failure.minimal_reproduction_json_pointer | one_line)
          ) and
          (.first_failure.artifact_entries | type == "array" and length > 0) and
          (all(.first_failure.artifact_entries[]; one_line))
        )
      )
    ' "${report_path}" >/dev/null
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

  if [[ "${REMOTE_ARTIFACT_STREAM}" == "1" ]]; then
    hydrate_marked_artifact \
      "${DOCTOR_LOG}" \
      "${RCH_JSON_ARTIFACT_BEGIN}" \
      "${RCH_JSON_ARTIFACT_END}" \
      "${DOCTOR_JSON}" || true
    hydrate_marked_artifact \
      "${DOCTOR_LOG}" \
      "${RCH_HUMAN_ARTIFACT_BEGIN}" \
      "${RCH_HUMAN_ARTIFACT_END}" \
      "${DOCTOR_MD}" || true
  fi

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

  if [[ ! -s "${DOCTOR_JSON}" ]]; then
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

if [[ ! -s "${MANIFEST_A}" || ! -s "${MANIFEST_B}" ]]; then
  echo "ERROR: differential manifest output missing" >&2
  exit 1
fi

if [[ ! -s "${SUMMARY_A}" || ! -s "${SUMMARY_B}" ]]; then
  echo "ERROR: differential human summary output missing" >&2
  exit 1
fi

validate_manifest_runner_log_schema "${RUN_A_LOG}" "run-a"
validate_manifest_runner_log_schema "${RUN_B_LOG}" "run-b"

jq -e \
  --arg run_id "${RUN_ID}" \
  --arg trace_id "${TRACE_ID}" \
  --argjson seed "${ROOT_SEED}" \
  --arg scenario_id "${SCENARIO_ID}" \
  '
    .schema_version == 1 and
    .bead_id == "bd-mblr.7.1.2" and
    .run_id == $run_id and
    .trace_id == $trace_id and
    .scenario_id == $scenario_id and
    .root_seed == $seed and
    (.fixture_json_files_seen >= 0) and
    (.fixture_entries_ingested >= 0) and
    (.fixture_sql_statements_ingested >= 0) and
    (.run_report.total_cases >= 0) and
    (.run_report.passed >= 0) and
    (.run_report.diverged >= 0) and
    (.run_report.deduplicated.total_before_dedup >= 0) and
    (.replay.command | type == "string" and length > 0 and (contains("\n") | not) and (contains("\r") | not)) and
    (.sampled_passing_replays | type == "array") and
    (
      (.run_report.passed == 0 and (.sampled_passing_replays | length) == 0) or
      (.run_report.passed > 0 and (.sampled_passing_replays | length) > 0)
    ) and
    (
      all(.sampled_passing_replays[];
        (.case_id | type == "string" and length > 0) and
        (.transform_name | type == "string" and length > 0) and
        (.seed | type == "number" and . >= 0) and
        (.replay_command | type == "string" and length > 0 and (contains("\n") | not) and (contains("\r") | not)) and
        (.diagnostic_json_pointer | type == "string" and length > 0) and
        (.artifact_entries | type == "array" and length > 0) and
        (all(.artifact_entries[]; type == "string" and length > 0 and (contains("\n") | not) and (contains("\r") | not)))
      )
    ) and
    (
      (.run_report.diverged == 0) or
      (
        (.run_report.divergent_cases | length) > 0 and
        (.first_failure | type == "object") and
        (.first_failure.root_cause_domain | IN("parser", "planner", "vdbe", "storage", "harness", "fixture")) and
        (.first_failure.replay_command | type == "string" and length > 0 and (contains("\n") | not) and (contains("\r") | not)) and
        (.first_failure.diagnostic_json_pointer | type == "string" and length > 0) and
        (.first_failure.remediation_playbook.summary | type == "string" and length > 0) and
        (.first_failure.remediation_playbook.evidence_json_pointer | type == "string" and length > 0) and
        (.first_failure.remediation_playbook.owner_hint | type == "string" and length > 0) and
        (.first_failure.remediation_playbook.next_commands | type == "array" and length > 0) and
        (all(.first_failure.remediation_playbook.next_commands[]; type == "string" and length > 0 and (contains("\n") | not) and (contains("\r") | not))) and
        (.first_failure.artifact_entries | type == "array" and length > 0) and
        (
          (.run_report.divergent_cases[0].minimal_reproduction == null) or
          (.first_failure.minimal_reproduction_json_pointer == "/run_report/divergent_cases/0/minimal_reproduction")
        )
      )
    )
  ' "${MANIFEST_A}" >/dev/null

if ! diff -u \
  <(jq -S '.' "${MANIFEST_A}" | sed -E 's/run-[ab]/run-x/g') \
  <(jq -S '.' "${MANIFEST_B}" | sed -E 's/run-[ab]/run-x/g') \
  >/dev/null; then
  echo "ERROR: deterministic replay check failed; manifests differ" >&2
  diff -u \
    <(jq -S '.' "${MANIFEST_A}" | sed -E 's/run-[ab]/run-x/g') \
    <(jq -S '.' "${MANIFEST_B}" | sed -E 's/run-[ab]/run-x/g') \
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
FIRST_FAILURE_REPLAY_COMMAND="$(
  jq -r \
    '.first_failure.replay_command // .run_report.divergent_cases[0].minimal_reproduction.repro_command // ""' \
    "${MANIFEST_A}"
)"
FIRST_FAILURE_ROOT_CAUSE_DOMAIN="$(jq -r '.first_failure.root_cause_domain // ""' "${MANIFEST_A}")"
FIRST_FAILURE_DIAGNOSTIC_POINTER="$(jq -r '.first_failure.diagnostic_json_pointer // ""' "${MANIFEST_A}")"
FIRST_FAILURE_REMEDIATION_SUMMARY="$(jq -r '.first_failure.remediation_playbook.summary // ""' "${MANIFEST_A}")"
FIRST_FAILURE_REMEDIATION_EVIDENCE="$(jq -r '.first_failure.remediation_playbook.evidence_json_pointer // ""' "${MANIFEST_A}")"
FIRST_FAILURE_REMEDIATION_OWNER="$(jq -r '.first_failure.remediation_playbook.owner_hint // ""' "${MANIFEST_A}")"
FIRST_FAILURE_REMEDIATION_NEXT_COMMANDS="$(
  jq -c '.first_failure.remediation_playbook.next_commands // []' "${MANIFEST_A}"
)"
FIRST_FAILURE_MINIMAL_REPRO_POINTER="$(
  jq -r '.first_failure.minimal_reproduction_json_pointer // ""' "${MANIFEST_A}"
)"
FIRST_FAILURE_ARTIFACT_ENTRIES="$(jq -c '.first_failure.artifact_entries // []' "${MANIFEST_A}")"
SAMPLED_PASSING_REPLAY_COUNT="$(
  jq -r \
    'if has("sampled_passing_replays") then (.sampled_passing_replays | length) else 0 end' \
    "${MANIFEST_A}"
)"
SAMPLED_PASSING_REPLAYS="$(jq -c '.sampled_passing_replays // []' "${MANIFEST_A}")"
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
MANIFEST_A_SHA256="$(sha256_file "${MANIFEST_A}")"
MANIFEST_B_SHA256="$(sha256_file "${MANIFEST_B}")"
SUMMARY_A_SHA256="$(sha256_file "${SUMMARY_A}")"
SUMMARY_B_SHA256="$(sha256_file "${SUMMARY_B}")"
RUNNER_LOG_A_SHA256="$(sha256_file "${RUN_A_LOG}")"
RUNNER_LOG_B_SHA256="$(sha256_file "${RUN_B_LOG}")"
DOCTOR_JSON_SHA256="$(sha256_file "${DOCTOR_JSON}")"
DOCTOR_HUMAN_SHA256="$(sha256_file "${DOCTOR_MD}")"
DOCTOR_LOG_SHA256="$(sha256_file "${DOCTOR_LOG}")"

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

write_ci_report_json() {
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
    --arg first_failure_root_cause_domain "${FIRST_FAILURE_ROOT_CAUSE_DOMAIN}" \
    --arg first_failure_diagnostic_pointer "${FIRST_FAILURE_DIAGNOSTIC_POINTER}" \
    --arg first_failure_remediation_summary "${FIRST_FAILURE_REMEDIATION_SUMMARY}" \
    --arg first_failure_remediation_evidence "${FIRST_FAILURE_REMEDIATION_EVIDENCE}" \
    --arg first_failure_remediation_owner "${FIRST_FAILURE_REMEDIATION_OWNER}" \
    --argjson first_failure_remediation_next_commands "${FIRST_FAILURE_REMEDIATION_NEXT_COMMANDS}" \
    --arg first_failure_minimal_repro_pointer "${FIRST_FAILURE_MINIMAL_REPRO_POINTER}" \
    --argjson first_failure_artifact_entries "${FIRST_FAILURE_ARTIFACT_ENTRIES}" \
    --argjson sampled_passing_replay_count "${SAMPLED_PASSING_REPLAY_COUNT}" \
    --argjson sampled_passing_replays "${SAMPLED_PASSING_REPLAYS}" \
    --arg mismatch_class_counts "${MISMATCH_CLASS_COUNTS}" \
    --arg manifest_a "${MANIFEST_A#"${WORKSPACE_ROOT}"/}" \
    --arg manifest_b "${MANIFEST_B#"${WORKSPACE_ROOT}"/}" \
    --arg summary_a "${SUMMARY_A#"${WORKSPACE_ROOT}"/}" \
    --arg summary_b "${SUMMARY_B#"${WORKSPACE_ROOT}"/}" \
    --arg runner_log_a "${RUN_A_LOG#"${WORKSPACE_ROOT}"/}" \
    --arg runner_log_b "${RUN_B_LOG#"${WORKSPACE_ROOT}"/}" \
    --arg doctor_json "${DOCTOR_JSON#"${WORKSPACE_ROOT}"/}" \
    --arg doctor_human "${DOCTOR_MD#"${WORKSPACE_ROOT}"/}" \
    --arg doctor_log "${DOCTOR_LOG#"${WORKSPACE_ROOT}"/}" \
    --arg report_artifact "${REPORT_JSON#"${WORKSPACE_ROOT}"/}" \
    --arg manifest_a_sha256 "${MANIFEST_A_SHA256}" \
    --arg manifest_b_sha256 "${MANIFEST_B_SHA256}" \
    --arg summary_a_sha256 "${SUMMARY_A_SHA256}" \
    --arg summary_b_sha256 "${SUMMARY_B_SHA256}" \
    --arg runner_log_a_sha256 "${RUNNER_LOG_A_SHA256}" \
    --arg runner_log_b_sha256 "${RUNNER_LOG_B_SHA256}" \
    --arg doctor_json_sha256 "${DOCTOR_JSON_SHA256}" \
    --arg doctor_human_sha256 "${DOCTOR_HUMAN_SHA256}" \
    --arg doctor_log_sha256 "${DOCTOR_LOG_SHA256}" \
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
          doctor_log: $doctor_log,
          report_artifact: $report_artifact,
          hashes: {
            manifest_a_sha256: $manifest_a_sha256,
            manifest_b_sha256: $manifest_b_sha256,
            summary_a_sha256: $summary_a_sha256,
            summary_b_sha256: $summary_b_sha256,
            runner_log_a_sha256: $runner_log_a_sha256,
            runner_log_b_sha256: $runner_log_b_sha256,
            doctor_json_sha256: $doctor_json_sha256,
            doctor_human_sha256: $doctor_human_sha256,
            doctor_log_sha256: $doctor_log_sha256
          }
        },
        replay_command: $replay_command,
        sampled_passing_replays: $sampled_passing_replays,
        first_failure_replay_command: $first_failure_replay_command,
        first_failure: (if $diverged_cases == 0 then null else {
          replay_command: $first_failure_replay_command,
          root_cause_domain: $first_failure_root_cause_domain,
          diagnostic_json_pointer: $first_failure_diagnostic_pointer,
          remediation_playbook: {
            summary: $first_failure_remediation_summary,
            evidence_json_pointer: $first_failure_remediation_evidence,
            owner_hint: $first_failure_remediation_owner,
            next_commands: $first_failure_remediation_next_commands
          },
          minimal_reproduction_json_pointer: (
            if $first_failure_minimal_repro_pointer == "" then null else $first_failure_minimal_repro_pointer end
          ),
          artifact_entries: $first_failure_artifact_entries
        } end)
      }
    ' >"${REPORT_JSON}"
}

write_ci_report_json
validate_ci_report_schema "${REPORT_JSON}"
REPORT_JSON_SHA256="$(sha256_file "${REPORT_JSON}")"

if ${JSON_OUTPUT}; then
  cat "${REPORT_JSON}"
else
  echo "=== Differential CI Lane Verification (${BEAD_ID}) ==="
  echo "Lane:                 ${LANE_ID} (warning mode)"
  echo "Run ID:               ${RUN_ID}"
  echo "Trace ID:             ${TRACE_ID}"
  echo "Scenario ID:          ${SCENARIO_ID}"
  echo "Commit SHA:           ${COMMIT_SHA}"
  echo "Root seed:            ${ROOT_SEED}"
  echo "Generated unix ms:    ${GENERATED_UNIX_MS}"
  echo "Fixture manifest:     ${FIXTURE_ROOT_MANIFEST#"${WORKSPACE_ROOT}"/}"
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
  if [[ "${SAMPLED_PASSING_REPLAY_COUNT}" != "0" ]]; then
    echo "Sampled passing replay commands:"
    jq -r \
      '.[] | "  - " + .diagnostic_json_pointer + ": " + .replay_command' \
      <<<"${SAMPLED_PASSING_REPLAYS}"
    echo "Sampled passing artifact entries:"
    jq -r \
      '.[] | "  - " + .diagnostic_json_pointer + ": " + (.artifact_entries | join(", "))' \
      <<<"${SAMPLED_PASSING_REPLAYS}"
  fi
  echo "Manifest A:           ${MANIFEST_A#"${WORKSPACE_ROOT}"/}"
  echo "Manifest B:           ${MANIFEST_B#"${WORKSPACE_ROOT}"/}"
  echo "Runner log A:         ${RUN_A_LOG#"${WORKSPACE_ROOT}"/}"
  echo "Runner log B:         ${RUN_B_LOG#"${WORKSPACE_ROOT}"/}"
  echo "Doctor report:        ${DOCTOR_JSON#"${WORKSPACE_ROOT}"/}"
  echo "Doctor log:           ${DOCTOR_LOG#"${WORKSPACE_ROOT}"/}"
  echo "Manifest A sha256:    ${MANIFEST_A_SHA256}"
  echo "Manifest B sha256:    ${MANIFEST_B_SHA256}"
  echo "Summary A sha256:     ${SUMMARY_A_SHA256}"
  echo "Summary B sha256:     ${SUMMARY_B_SHA256}"
  echo "Runner log A sha256:  ${RUNNER_LOG_A_SHA256}"
  echo "Runner log B sha256:  ${RUNNER_LOG_B_SHA256}"
  echo "Doctor JSON sha256:   ${DOCTOR_JSON_SHA256}"
  echo "Doctor human sha256:  ${DOCTOR_HUMAN_SHA256}"
  echo "Doctor log sha256:    ${DOCTOR_LOG_SHA256}"
  echo "CI report:            ${REPORT_JSON#"${WORKSPACE_ROOT}"/}"
  echo "CI report sha256:     ${REPORT_JSON_SHA256}"
  echo "Promotion criteria:"
  for criterion in "${PROMOTION_CRITERIA[@]}"; do
    echo "  - ${criterion}"
  done
  echo "Replay command:"
  echo "  ${REPLAY_COMMAND}"
  if [[ -n "${FIRST_FAILURE_REPLAY_COMMAND}" ]]; then
    echo "First-failure root cause:"
    echo "  ${FIRST_FAILURE_ROOT_CAUSE_DOMAIN}"
    echo "First-failure diagnostic pointer:"
    echo "  ${FIRST_FAILURE_DIAGNOSTIC_POINTER}"
    echo "First-failure remediation:"
    echo "  Summary: ${FIRST_FAILURE_REMEDIATION_SUMMARY}"
    echo "  Evidence: ${FIRST_FAILURE_REMEDIATION_EVIDENCE}"
    echo "  Owner: ${FIRST_FAILURE_REMEDIATION_OWNER}"
    if [[ -n "${FIRST_FAILURE_MINIMAL_REPRO_POINTER}" ]]; then
      echo "  Minimal repro: ${FIRST_FAILURE_MINIMAL_REPRO_POINTER}"
    fi
    echo "First-failure remediation commands:"
    while IFS= read -r command; do
      echo "  - ${command}"
    done < <(jq -r '.[]' <<<"${FIRST_FAILURE_REMEDIATION_NEXT_COMMANDS}")
    echo "First-failure artifact entries:"
    while IFS= read -r artifact_entry; do
      echo "  - ${artifact_entry}"
    done < <(jq -r '.[]' <<<"${FIRST_FAILURE_ARTIFACT_ENTRIES}")
    echo "First-failure replay command:"
    echo "  ${FIRST_FAILURE_REPLAY_COMMAND}"
  fi
fi
