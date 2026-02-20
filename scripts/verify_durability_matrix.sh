#!/usr/bin/env bash
# verify_durability_matrix.sh — deterministic durability-matrix e2e verifier (bd-mblr.7.4)
#
# Usage:
#   ./scripts/verify_durability_matrix.sh [--json] [--root-seed <u64|0xhex>]
#
# Env overrides:
#   DURABILITY_MATRIX_ROOT_SEED=<u64|0xhex>
#   DURABILITY_MATRIX_USE_RCH=1
#   DURABILITY_MATRIX_RUNNER_BIN=target/debug/durability_matrix_manifest

set -euo pipefail

WORKSPACE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BEAD_ID="bd-mblr.7.4"
JSON_OUTPUT=false
ROOT_SEED="${DURABILITY_MATRIX_ROOT_SEED:-0xB740000000000001}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --json)
      JSON_OUTPUT=true
      shift
      ;;
    --root-seed)
      if [[ $# -lt 2 ]]; then
        echo "ERROR: --root-seed requires a value" >&2
        exit 2
      fi
      ROOT_SEED="$2"
      shift 2
      ;;
    *)
      echo "ERROR: unknown argument '$1'" >&2
      exit 2
      ;;
  esac
done

normalize_root_seed_u64() {
  local raw="$1"
  if [[ "${raw}" == 0x* || "${raw}" == 0X* ]]; then
    local hex="${raw#0x}"
    hex="${hex#0X}"
    printf '%u' "$((16#${hex}))"
  else
    printf '%u' "${raw}"
  fi
}

ROOT_SEED_DECIMAL="$(normalize_root_seed_u64 "${ROOT_SEED}")"

RUN_TS="$(date -u +%Y%m%dT%H%M%SZ)"
RUN_ID="${BEAD_ID}-${RUN_TS}"
TRACE_ID="trace-$(printf '%s' "${RUN_ID}" | sha256sum | awk '{print $1}' | cut -c1-16)"
COMMIT_SHA="$(git -C "${WORKSPACE_ROOT}" rev-parse HEAD 2>/dev/null || echo unknown)"

ART_ROOT="${WORKSPACE_ROOT}/artifacts/durability-matrix"
RUN_DIR="${ART_ROOT}/run"
mkdir -p "${RUN_DIR}"

MATRIX_A_JSON="${RUN_DIR}/durability_matrix_a.json"
MATRIX_B_JSON="${RUN_DIR}/durability_matrix_b.json"
WORKFLOW_A_TXT="${RUN_DIR}/durability_matrix_workflow_a.txt"
WORKFLOW_B_TXT="${RUN_DIR}/durability_matrix_workflow_b.txt"
DRY_RUN_JSON="${RUN_DIR}/durability_probe_dry_run.json"
LOG_MATRIX_A="${RUN_DIR}/matrix_a.log"
LOG_MATRIX_B="${RUN_DIR}/matrix_b.log"
LOG_WORKFLOW_A="${RUN_DIR}/workflow_a.log"
LOG_WORKFLOW_B="${RUN_DIR}/workflow_b.log"
LOG_DRY_RUN="${RUN_DIR}/dry_run.log"
REPORT_JSON="${ART_ROOT}/report.json"

RUNNER_BIN="${DURABILITY_MATRIX_RUNNER_BIN:-}"
RUNNER=(cargo run -p fsqlite-harness --bin durability_matrix_manifest --)
if [[ -n "${RUNNER_BIN}" ]]; then
  if [[ ! -x "${RUNNER_BIN}" ]]; then
    echo "ERROR: DURABILITY_MATRIX_RUNNER_BIN is not executable: ${RUNNER_BIN}" >&2
    exit 2
  fi
  RUNNER=("${RUNNER_BIN}")
elif [[ "${DURABILITY_MATRIX_USE_RCH:-0}" == "1" ]] && command -v rch >/dev/null 2>&1; then
  RUNNER=(rch exec -- cargo run -p fsqlite-harness --bin durability_matrix_manifest --)
fi

log_event() {
  local level="$1"
  local phase="$2"
  local message="$3"
  printf '%s bead_id=%s trace_id=%s run_id=%s phase=%s msg=%s\n' \
    "${level}" "${BEAD_ID}" "${TRACE_ID}" "${RUN_ID}" "${phase}" "${message}" >&2
}

run_runner() {
  local phase="$1"
  local log_file="$2"
  shift 2

  local status
  set +e
  "${RUNNER[@]}" --root-seed "${ROOT_SEED}" "$@" >"${log_file}" 2>&1
  status=$?
  set -e

  if [[ ${status} -ne 0 ]]; then
    log_event "ERROR" "${phase}" "runner_exit=${status}"
    echo "ERROR: durability_matrix_manifest failed for phase=${phase} (exit=${status})" >&2
    echo "----- runner log: ${log_file} -----" >&2
    cat "${log_file}" >&2
    return "${status}"
  fi

  log_event "INFO" "${phase}" "runner_exit=0"
  return 0
}

severity_rank() {
  local severity="$1"
  case "${severity}" in
    none) echo 0 ;;
    low) echo 1 ;;
    medium) echo 2 ;;
    high) echo 3 ;;
    critical) echo 4 ;;
    *) echo 4 ;;
  esac
}

VIOLATIONS_FILE="${RUN_DIR}/violations.tsv"
MATRIX_DIFF_FILE="${RUN_DIR}/matrix_diff.txt"
WORKFLOW_DIFF_FILE="${RUN_DIR}/workflow_diff.txt"
: >"${VIOLATIONS_FILE}"

FAILURE_COUNT=0
HIGHEST_SEVERITY="none"

MATRIX_DETERMINISTIC=true
WORKFLOW_DETERMINISTIC=true
MATRIX_CONTRACT_VALID=true
DRY_RUN_CONTRACT_VALID=true
MATRIX_DRYRUN_COUNTS_VALID=true

MATRIX_DETERMINISM_SEVERITY="none"
WORKFLOW_DETERMINISM_SEVERITY="none"
MATRIX_CONTRACT_SEVERITY="none"
DRY_RUN_CONTRACT_SEVERITY="none"
MATRIX_DRYRUN_COUNTS_SEVERITY="none"

record_violation() {
  local severity="$1"
  local phase="$2"
  local detail="$3"
  local clean_detail
  clean_detail="$(printf '%s' "${detail}" | tr '\n' ' ' | tr '\t' ' ')"
  printf '%s\t%s\t%s\n' "${severity}" "${phase}" "${clean_detail}" >>"${VIOLATIONS_FILE}"
  FAILURE_COUNT=$((FAILURE_COUNT + 1))
  if [[ "$(severity_rank "${severity}")" -gt "$(severity_rank "${HIGHEST_SEVERITY}")" ]]; then
    HIGHEST_SEVERITY="${severity}"
  fi
  log_event "ERROR" "${phase}" "${clean_detail}"
}

RUNNER_FAILURE=false
if ! run_runner "matrix_a" "${LOG_MATRIX_A}" --output "${MATRIX_A_JSON}"; then
  RUNNER_FAILURE=true
  record_violation "critical" "matrix_a" "matrix_a_generation_failed log=${LOG_MATRIX_A#${WORKSPACE_ROOT}/}"
fi
if ! run_runner "matrix_b" "${LOG_MATRIX_B}" --output "${MATRIX_B_JSON}"; then
  RUNNER_FAILURE=true
  record_violation "critical" "matrix_b" "matrix_b_generation_failed log=${LOG_MATRIX_B#${WORKSPACE_ROOT}/}"
fi
if ! run_runner "workflow_a" "${LOG_WORKFLOW_A}" --workflow --output "${WORKFLOW_A_TXT}"; then
  RUNNER_FAILURE=true
  record_violation "critical" "workflow_a" "workflow_a_generation_failed log=${LOG_WORKFLOW_A#${WORKSPACE_ROOT}/}"
fi
if ! run_runner "workflow_b" "${LOG_WORKFLOW_B}" --workflow --output "${WORKFLOW_B_TXT}"; then
  RUNNER_FAILURE=true
  record_violation "critical" "workflow_b" "workflow_b_generation_failed log=${LOG_WORKFLOW_B#${WORKSPACE_ROOT}/}"
fi
if ! run_runner "probe_dry_run" "${LOG_DRY_RUN}" --probe-dry-run --output "${DRY_RUN_JSON}"; then
  RUNNER_FAILURE=true
  record_violation "critical" "probe_dry_run" "dry_run_generation_failed log=${LOG_DRY_RUN#${WORKSPACE_ROOT}/}"
fi

if [[ -f "${MATRIX_A_JSON}" && -f "${MATRIX_B_JSON}" ]]; then
  if ! diff -u "${MATRIX_A_JSON}" "${MATRIX_B_JSON}" >"${MATRIX_DIFF_FILE}"; then
    MATRIX_DETERMINISTIC=false
    MATRIX_DETERMINISM_SEVERITY="critical"
    record_violation \
      "critical" \
      "matrix_determinism" \
      "deterministic matrix outputs diverged diff=${MATRIX_DIFF_FILE#${WORKSPACE_ROOT}/}"
  fi
else
  MATRIX_DETERMINISTIC=false
  MATRIX_DETERMINISM_SEVERITY="critical"
  record_violation "critical" "matrix_determinism" "missing matrix output artifacts"
fi

if [[ -f "${WORKFLOW_A_TXT}" && -f "${WORKFLOW_B_TXT}" ]]; then
  if ! diff -u "${WORKFLOW_A_TXT}" "${WORKFLOW_B_TXT}" >"${WORKFLOW_DIFF_FILE}"; then
    WORKFLOW_DETERMINISTIC=false
    WORKFLOW_DETERMINISM_SEVERITY="critical"
    record_violation \
      "critical" \
      "workflow_determinism" \
      "deterministic workflow outputs diverged diff=${WORKFLOW_DIFF_FILE#${WORKSPACE_ROOT}/}"
  fi
else
  WORKFLOW_DETERMINISTIC=false
  WORKFLOW_DETERMINISM_SEVERITY="critical"
  record_violation "critical" "workflow_determinism" "missing workflow output artifacts"
fi

if [[ -f "${MATRIX_A_JSON}" ]]; then
  if ! jq -e \
    --arg bead_id "${BEAD_ID}" \
    --argjson schema_version 1 \
    --argjson root_seed "${ROOT_SEED_DECIMAL}" \
    '
      .bead_id == $bead_id and
      .schema_version == $schema_version and
      .root_seed == $root_seed and
      (.environments | length) > 0 and
      (.scenarios | length) > 0 and
      (.probes | length) > 0 and
      ([.environments[].os] | index("Linux")) != null and
      ([.environments[].os] | index("MacOs")) != null and
      ([.environments[].os] | index("Windows")) != null
    ' "${MATRIX_A_JSON}" >/dev/null; then
    MATRIX_CONTRACT_VALID=false
    MATRIX_CONTRACT_SEVERITY="high"
    record_violation "high" "matrix_contract" "matrix contract validation failed"
  fi
else
  MATRIX_CONTRACT_VALID=false
  MATRIX_CONTRACT_SEVERITY="high"
  record_violation "high" "matrix_contract" "matrix contract file missing"
fi

if [[ -f "${DRY_RUN_JSON}" ]]; then
  if ! jq -e \
    --arg bead_id "${BEAD_ID}" \
    --argjson schema_version 1 \
    '
      .bead_id == $bead_id and
      .schema_version == $schema_version and
      .mode == "dry_run" and
      .total_probes >= 0 and
      .skipped_probes == .total_probes and
      .passed_probes == 0 and
      .failed_probes == 0 and
      .timeout_probes == 0 and
      .error_probes == 0 and
      (.results | length) == .total_probes
    ' "${DRY_RUN_JSON}" >/dev/null; then
    DRY_RUN_CONTRACT_VALID=false
    DRY_RUN_CONTRACT_SEVERITY="high"
    record_violation "high" "dry_run_contract" "dry-run contract validation failed"
  fi
else
  DRY_RUN_CONTRACT_VALID=false
  DRY_RUN_CONTRACT_SEVERITY="high"
  record_violation "high" "dry_run_contract" "dry-run contract file missing"
fi

ENV_COUNT=0
SCENARIO_COUNT=0
PROBE_COUNT=0
DRY_TOTAL=0
DRY_SKIPPED=0

if [[ -f "${MATRIX_A_JSON}" ]]; then
  ENV_COUNT="$(jq -r '.environments | length' "${MATRIX_A_JSON}")"
  SCENARIO_COUNT="$(jq -r '.scenarios | length' "${MATRIX_A_JSON}")"
  PROBE_COUNT="$(jq -r '.probes | length' "${MATRIX_A_JSON}")"
fi
if [[ -f "${DRY_RUN_JSON}" ]]; then
  DRY_TOTAL="$(jq -r '.total_probes' "${DRY_RUN_JSON}")"
  DRY_SKIPPED="$(jq -r '.skipped_probes' "${DRY_RUN_JSON}")"
fi

if [[ "${PROBE_COUNT}" != "${DRY_TOTAL}" || "${PROBE_COUNT}" != "${DRY_SKIPPED}" ]]; then
  MATRIX_DRYRUN_COUNTS_VALID=false
  MATRIX_DRYRUN_COUNTS_SEVERITY="high"
  record_violation \
    "high" \
    "matrix_dry_run_counts" \
    "probe counts mismatch matrix=${PROBE_COUNT} dry_total=${DRY_TOTAL} dry_skipped=${DRY_SKIPPED}"
fi

REPLAY_MATRIX_CMD="cargo run -p fsqlite-harness --bin durability_matrix_manifest -- --root-seed ${ROOT_SEED} --output ${MATRIX_A_JSON}"
REPLAY_DRY_RUN_CMD="cargo run -p fsqlite-harness --bin durability_matrix_manifest -- --root-seed ${ROOT_SEED} --probe-dry-run --output ${DRY_RUN_JSON}"

BLOCKING_SEVERITY_THRESHOLD="${DURABILITY_MATRIX_BLOCKING_SEVERITY:-high}"
BLOCK_RELEASE=false
if [[ "$(severity_rank "${HIGHEST_SEVERITY}")" -ge "$(severity_rank "${BLOCKING_SEVERITY_THRESHOLD}")" ]]; then
  BLOCK_RELEASE=true
fi

LANE_OUTCOME="pass"
if [[ "${FAILURE_COUNT}" -gt 0 ]]; then
  LANE_OUTCOME="warning"
  if ${BLOCK_RELEASE}; then
    LANE_OUTCOME="fail"
  fi
fi

VIOLATIONS_JSON="$(jq -Rn '
  [inputs
   | select(length > 0)
   | split("\t")
   | {severity: .[0], phase: .[1], detail: .[2], owner: "bd-mblr.7.4"}]
' <"${VIOLATIONS_FILE}")"

REPORT_PAYLOAD="$(jq -n \
  --arg schema_version "fsqlite.durability-matrix.v1" \
  --arg bead_id "${BEAD_ID}" \
  --arg run_id "${RUN_ID}" \
  --arg trace_id "${TRACE_ID}" \
  --arg commit_sha "${COMMIT_SHA}" \
  --argjson root_seed "${ROOT_SEED_DECIMAL}" \
  --arg lane_outcome "${LANE_OUTCOME}" \
  --arg highest_severity "${HIGHEST_SEVERITY}" \
  --arg blocking_threshold "${BLOCKING_SEVERITY_THRESHOLD}" \
  --argjson block_release "${BLOCK_RELEASE}" \
  --argjson runner_failure "${RUNNER_FAILURE}" \
  --argjson deterministic_matrix "${MATRIX_DETERMINISTIC}" \
  --arg matrix_determinism_severity "${MATRIX_DETERMINISM_SEVERITY}" \
  --argjson deterministic_workflow "${WORKFLOW_DETERMINISTIC}" \
  --arg workflow_determinism_severity "${WORKFLOW_DETERMINISM_SEVERITY}" \
  --argjson matrix_contract_valid "${MATRIX_CONTRACT_VALID}" \
  --arg matrix_contract_severity "${MATRIX_CONTRACT_SEVERITY}" \
  --argjson dry_run_contract_valid "${DRY_RUN_CONTRACT_VALID}" \
  --arg dry_run_contract_severity "${DRY_RUN_CONTRACT_SEVERITY}" \
  --argjson matrix_dry_run_counts_valid "${MATRIX_DRYRUN_COUNTS_VALID}" \
  --arg matrix_dry_run_counts_severity "${MATRIX_DRYRUN_COUNTS_SEVERITY}" \
  --argjson failure_count "${FAILURE_COUNT}" \
  --argjson environment_count "${ENV_COUNT}" \
  --argjson scenario_count "${SCENARIO_COUNT}" \
  --argjson probe_count "${PROBE_COUNT}" \
  --argjson dry_run_total "${DRY_TOTAL}" \
  --argjson dry_run_skipped "${DRY_SKIPPED}" \
  --arg matrix_a "${MATRIX_A_JSON#${WORKSPACE_ROOT}/}" \
  --arg matrix_b "${MATRIX_B_JSON#${WORKSPACE_ROOT}/}" \
  --arg workflow_a "${WORKFLOW_A_TXT#${WORKSPACE_ROOT}/}" \
  --arg workflow_b "${WORKFLOW_B_TXT#${WORKSPACE_ROOT}/}" \
  --arg dry_run "${DRY_RUN_JSON#${WORKSPACE_ROOT}/}" \
  --arg matrix_diff "${MATRIX_DIFF_FILE#${WORKSPACE_ROOT}/}" \
  --arg workflow_diff "${WORKFLOW_DIFF_FILE#${WORKSPACE_ROOT}/}" \
  --arg log_matrix_a "${LOG_MATRIX_A#${WORKSPACE_ROOT}/}" \
  --arg log_matrix_b "${LOG_MATRIX_B#${WORKSPACE_ROOT}/}" \
  --arg log_workflow_a "${LOG_WORKFLOW_A#${WORKSPACE_ROOT}/}" \
  --arg log_workflow_b "${LOG_WORKFLOW_B#${WORKSPACE_ROOT}/}" \
  --arg log_dry_run "${LOG_DRY_RUN#${WORKSPACE_ROOT}/}" \
  --arg replay_matrix "${REPLAY_MATRIX_CMD}" \
  --arg replay_dry_run "${REPLAY_DRY_RUN_CMD}" \
  --argjson violations "${VIOLATIONS_JSON}" \
  '
    {
      schema_version: $schema_version,
      bead_id: $bead_id,
      run: {
        run_id: $run_id,
        trace_id: $trace_id,
        commit_sha: $commit_sha,
        root_seed: $root_seed
      },
      lane: {
        outcome: $lane_outcome,
        highest_severity: $highest_severity,
        violation_count: $failure_count,
        runner_failure: $runner_failure
      },
      checks: {
        matrix_determinism: { pass: $deterministic_matrix, severity: $matrix_determinism_severity },
        workflow_determinism: { pass: $deterministic_workflow, severity: $workflow_determinism_severity },
        matrix_contract: { pass: $matrix_contract_valid, severity: $matrix_contract_severity },
        dry_run_contract: { pass: $dry_run_contract_valid, severity: $dry_run_contract_severity },
        matrix_dry_run_counts: { pass: $matrix_dry_run_counts_valid, severity: $matrix_dry_run_counts_severity }
      },
      matrix: {
        environment_count: $environment_count,
        scenario_count: $scenario_count,
        probe_count: $probe_count
      },
      dry_run: {
        total_probes: $dry_run_total,
        skipped_probes: $dry_run_skipped
      },
      drift: {
        owner: "bd-mblr.7.4",
        violations: $violations
      },
      release_gate: {
        owner: "bd-mblr.7.4",
        blocking_threshold: $blocking_threshold,
        block_release: $block_release
      },
      artifacts: {
        matrix_a: $matrix_a,
        matrix_b: $matrix_b,
        matrix_diff: $matrix_diff,
        workflow_a: $workflow_a,
        workflow_b: $workflow_b,
        workflow_diff: $workflow_diff,
        dry_run: $dry_run,
        log_matrix_a: $log_matrix_a,
        log_matrix_b: $log_matrix_b,
        log_workflow_a: $log_workflow_a,
        log_workflow_b: $log_workflow_b,
        log_dry_run: $log_dry_run
      },
      replay: {
        matrix_command: $replay_matrix,
        dry_run_command: $replay_dry_run
      }
    }
  ')"

printf '%s\n' "${REPORT_PAYLOAD}" >"${REPORT_JSON}"

if ${JSON_OUTPUT}; then
  printf '%s\n' "${REPORT_PAYLOAD}"
else
  cat <<TEXT
=== Durability Matrix Verification (${BEAD_ID}) ===
Run ID:             ${RUN_ID}
Trace ID:           ${TRACE_ID}
Commit SHA:         ${COMMIT_SHA}
Root seed:          ${ROOT_SEED}
Lane outcome:        ${LANE_OUTCOME}
Highest severity:    ${HIGHEST_SEVERITY}
Block release:       ${BLOCK_RELEASE}
Matrix deterministic:    ${MATRIX_DETERMINISTIC}
Workflow deterministic:  ${WORKFLOW_DETERMINISTIC}
Matrix contract valid:   ${MATRIX_CONTRACT_VALID}
Dry-run contract valid:  ${DRY_RUN_CONTRACT_VALID}
Counts contract valid:   ${MATRIX_DRYRUN_COUNTS_VALID}
Environment count:   ${ENV_COUNT}
Scenario count:      ${SCENARIO_COUNT}
Probe count:         ${PROBE_COUNT}
Dry-run total:       ${DRY_TOTAL}
Dry-run skipped:     ${DRY_SKIPPED}
Matrix A:            ${MATRIX_A_JSON#${WORKSPACE_ROOT}/}
Workflow A:          ${WORKFLOW_A_TXT#${WORKSPACE_ROOT}/}
Dry-run summary:     ${DRY_RUN_JSON#${WORKSPACE_ROOT}/}
Report JSON:         ${REPORT_JSON#${WORKSPACE_ROOT}/}
Replay matrix cmd:   ${REPLAY_MATRIX_CMD}
Replay dry-run cmd:  ${REPLAY_DRY_RUN_CMD}
TEXT
fi

if ${BLOCK_RELEASE}; then
  exit 1
fi
