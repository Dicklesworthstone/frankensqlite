#!/usr/bin/env bash
# verify_bd_zjisk_3_parity_durability_cert.sh
# Deterministic parity + durability certification gate for bd-zjisk.3.
#
# This wrapper enforces rch-offloaded execution for CPU-heavy commands.
#
# Usage:
#   ./scripts/verify_bd_zjisk_3_parity_durability_cert.sh [--json]
#   ./scripts/verify_bd_zjisk_3_parity_durability_cert.sh --lane smoke --diff-seed 424242
#
# Options:
#   --json                        Emit machine-readable summary JSON to stdout
#   --lane <smoke|expanded>       Differential lane (default: smoke)
#   --diff-seed <u64>             Differential lane deterministic seed
#   --generated-unix-ms <u128>    Differential lane deterministic timestamp
#   --durability-seed <u64|0xhex> Durability matrix deterministic seed

set -euo pipefail

WORKSPACE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BEAD_ID="bd-zjisk.3"

JSON_OUTPUT=false
DIFF_LANE="${ZJISK3_DIFF_LANE:-smoke}"
DIFF_SEED="${ZJISK3_DIFF_SEED:-424242}"
GENERATED_UNIX_MS="${ZJISK3_GENERATED_UNIX_MS:-1700000000000}"
DURABILITY_SEED="${ZJISK3_DURABILITY_SEED:-0xB740000000000001}"
REMOTE_TARGET_DIR="${ZJISK3_REMOTE_TARGET_DIR:-target-local/bd-zjisk_3_cert}"
REMOTE_TMP_DIR="${ZJISK3_REMOTE_TMP_DIR:-target-local/bd-zjisk_3_tmp}"
REMOTE_CFLAGS="${ZJISK3_REMOTE_CFLAGS:--pipe}"

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
      DIFF_LANE="$2"
      shift 2
      ;;
    --diff-seed)
      if [[ $# -lt 2 ]]; then
        echo "ERROR: --diff-seed requires a value" >&2
        exit 2
      fi
      DIFF_SEED="$2"
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
    --durability-seed)
      if [[ $# -lt 2 ]]; then
        echo "ERROR: --durability-seed requires a value" >&2
        exit 2
      fi
      DURABILITY_SEED="$2"
      shift 2
      ;;
    *)
      echo "ERROR: unknown argument '$1'" >&2
      exit 2
      ;;
  esac
done

if [[ "${REMOTE_TARGET_DIR}" != /* ]]; then
  REMOTE_TARGET_DIR="${WORKSPACE_ROOT}/${REMOTE_TARGET_DIR}"
fi
if [[ "${REMOTE_TMP_DIR}" != /* ]]; then
  REMOTE_TMP_DIR="${WORKSPACE_ROOT}/${REMOTE_TMP_DIR}"
fi

mkdir -p "${REMOTE_TARGET_DIR}" "${REMOTE_TMP_DIR}"

RUN_TS="$(date -u +%Y%m%dT%H%M%SZ)"
RUN_ID="${BEAD_ID}-${RUN_TS}"
TRACE_ID="trace-$(printf '%s' "${RUN_ID}" | sha256sum | awk '{print $1}' | cut -c1-16)"
COMMIT_SHA="$(git -C "${WORKSPACE_ROOT}" rev-parse HEAD 2>/dev/null || echo unknown)"

ARTIFACT_DIR="${WORKSPACE_ROOT}/artifacts/${BEAD_ID}/${RUN_ID}"
mkdir -p "${ARTIFACT_DIR}"

UNIT_REPLAY_LOG="${ARTIFACT_DIR}/unit_replay_command.log"
UNIT_SUMMARY_LOG="${ARTIFACT_DIR}/unit_human_summary.log"
UNIT_MANIFEST_LOG="${ARTIFACT_DIR}/unit_artifact_manifest.log"
DIFF_LOG="${ARTIFACT_DIR}/verify_differential_ci_lane.log"
DURABILITY_LOG="${ARTIFACT_DIR}/verify_durability_matrix.log"
DIFF_REPORT="${ARTIFACT_DIR}/differential_lane_report.json"
DURABILITY_REPORT="${ARTIFACT_DIR}/durability_matrix_report.json"
REPORT_JSON="${ARTIFACT_DIR}/report.json"
REPORT_MD="${ARTIFACT_DIR}/report.md"

run_checked() {
  local phase="$1"
  local log_path="$2"
  shift 2

  local status=0
  set +e
  "$@" >"${log_path}" 2>&1
  status=$?
  set -e

  if [[ ${status} -ne 0 ]]; then
    echo "ERROR: ${phase} failed (exit=${status})" >&2
    echo "----- ${phase} log: ${log_path} -----" >&2
    cat "${log_path}" >&2
    exit 1
  fi
}

run_checked \
  "unit_replay_command_generation" \
  "${UNIT_REPLAY_LOG}" \
  rch exec -- env TMPDIR="${REMOTE_TMP_DIR}" CARGO_TARGET_DIR="${REMOTE_TARGET_DIR}" CFLAGS="${REMOTE_CFLAGS}" cargo test -p fsqlite-harness --bin differential_manifest_runner replay_command_includes_deterministic_controls -- --nocapture

run_checked \
  "unit_human_summary_replay_embedding" \
  "${UNIT_SUMMARY_LOG}" \
  rch exec -- env TMPDIR="${REMOTE_TMP_DIR}" CARGO_TARGET_DIR="${REMOTE_TARGET_DIR}" CFLAGS="${REMOTE_CFLAGS}" cargo test -p fsqlite-harness --bin differential_manifest_runner human_summary_contains_replay_and_counts -- --nocapture

run_checked \
  "unit_artifact_manifest_assembly" \
  "${UNIT_MANIFEST_LOG}" \
  rch exec -- env TMPDIR="${REMOTE_TMP_DIR}" CARGO_TARGET_DIR="${REMOTE_TARGET_DIR}" CFLAGS="${REMOTE_CFLAGS}" cargo test -p fsqlite-harness --lib artifact_manifest_validation_pass -- --nocapture

set +e
(
  cd "${WORKSPACE_ROOT}"
  TMPDIR="${REMOTE_TMP_DIR}" \
  CARGO_TARGET_DIR="${REMOTE_TARGET_DIR}" \
  CFLAGS="${REMOTE_CFLAGS}" \
  DIFF_LANE_USE_RCH=1 \
  DIFF_LANE_FORCE_RCH=1 \
  ./scripts/verify_differential_ci_lane.sh \
    --lane "${DIFF_LANE}" \
    --seed "${DIFF_SEED}" \
    --generated-unix-ms "${GENERATED_UNIX_MS}" \
    --json >"${DIFF_REPORT}"
) >"${DIFF_LOG}" 2>&1
DIFF_STATUS=$?
set -e
if [[ ${DIFF_STATUS} -ne 0 ]]; then
  echo "ERROR: differential lane verification failed (exit=${DIFF_STATUS})" >&2
  echo "----- differential log: ${DIFF_LOG} -----" >&2
  cat "${DIFF_LOG}" >&2
  exit 1
fi

set +e
(
  cd "${WORKSPACE_ROOT}"
  TMPDIR="${REMOTE_TMP_DIR}" \
  CARGO_TARGET_DIR="${REMOTE_TARGET_DIR}" \
  CFLAGS="${REMOTE_CFLAGS}" \
  DURABILITY_MATRIX_USE_RCH=1 \
  DURABILITY_MATRIX_FORCE_RCH=1 \
  ./scripts/verify_durability_matrix.sh \
    --root-seed "${DURABILITY_SEED}" \
    --json >"${DURABILITY_REPORT}"
) >"${DURABILITY_LOG}" 2>&1
DURABILITY_STATUS=$?
set -e
if [[ ${DURABILITY_STATUS} -ne 0 ]]; then
  echo "ERROR: durability matrix verification failed (exit=${DURABILITY_STATUS})" >&2
  echo "----- durability log: ${DURABILITY_LOG} -----" >&2
  cat "${DURABILITY_LOG}" >&2
  exit 1
fi

jq -e '.' "${DIFF_REPORT}" >/dev/null
jq -e '.' "${DURABILITY_REPORT}" >/dev/null

jq -e '
  .run.deterministic_match == true and
  (.lane.outcome == "pass" or .lane.outcome == "warn") and
  (.replay_command | type == "string") and
  (.replay_command | length > 0)
' "${DIFF_REPORT}" >/dev/null

jq -e '
  .release_gate.block_release == false and
  .checks.matrix_determinism.pass == true and
  .checks.workflow_determinism.pass == true and
  .checks.matrix_contract.pass == true and
  .checks.dry_run_contract.pass == true and
  .checks.matrix_dry_run_counts.pass == true and
  (.replay.matrix_command | type == "string") and
  (.replay.matrix_command | length > 0) and
  (.replay.dry_run_command | type == "string") and
  (.replay.dry_run_command | length > 0)
' "${DURABILITY_REPORT}" >/dev/null

collect_artifact_paths() {
  local report_path="$1"
  jq -r '.artifacts | to_entries[] | .value' "${report_path}"
}

artifact_hashes_json='[]'
declare -A seen_paths=()

while IFS= read -r rel_path; do
  if [[ -z "${rel_path}" || "${rel_path}" == "null" ]]; then
    continue
  fi
  rel_path="${rel_path#./}"
  if [[ -n "${seen_paths[${rel_path}]:-}" ]]; then
    continue
  fi
  seen_paths["${rel_path}"]=1

  abs_path="${WORKSPACE_ROOT}/${rel_path}"
  if [[ -f "${abs_path}" ]]; then
    sha256="$(sha256sum "${abs_path}" | awk '{print $1}')"
    size_bytes="$(wc -c <"${abs_path}" | tr -d ' ')"
    artifact_hashes_json="$(
      jq -c \
        --arg path "${rel_path}" \
        --arg sha256 "${sha256}" \
        --argjson size_bytes "${size_bytes}" \
        '. + [{path: $path, sha256: $sha256, size_bytes: $size_bytes}]' \
        <<<"${artifact_hashes_json}"
    )"
  fi
done < <(
  {
    collect_artifact_paths "${DIFF_REPORT}"
    collect_artifact_paths "${DURABILITY_REPORT}"
    printf '%s\n' \
      "${DIFF_REPORT#${WORKSPACE_ROOT}/}" \
      "${DURABILITY_REPORT#${WORKSPACE_ROOT}/}" \
      "${UNIT_REPLAY_LOG#${WORKSPACE_ROOT}/}" \
      "${UNIT_SUMMARY_LOG#${WORKSPACE_ROOT}/}" \
      "${UNIT_MANIFEST_LOG#${WORKSPACE_ROOT}/}" \
      "${DIFF_LOG#${WORKSPACE_ROOT}/}" \
      "${DURABILITY_LOG#${WORKSPACE_ROOT}/}"
  }
)

DIFF_REPLAY_COMMAND="$(jq -r '.replay_command' "${DIFF_REPORT}")"
DURABILITY_REPLAY_MATRIX="$(jq -r '.replay.matrix_command' "${DURABILITY_REPORT}")"
DURABILITY_REPLAY_DRY_RUN="$(jq -r '.replay.dry_run_command' "${DURABILITY_REPORT}")"
DIFF_OUTCOME="$(jq -r '.lane.outcome' "${DIFF_REPORT}")"
DURABILITY_OUTCOME="$(jq -r '.lane.outcome' "${DURABILITY_REPORT}")"

REPORT_PAYLOAD="$(
  jq -n \
    --arg schema_version "fsqlite.zjisk3.cert.v1" \
    --arg bead_id "${BEAD_ID}" \
    --arg run_id "${RUN_ID}" \
    --arg trace_id "${TRACE_ID}" \
    --arg commit_sha "${COMMIT_SHA}" \
    --arg lane "${DIFF_LANE}" \
    --arg diff_seed "${DIFF_SEED}" \
    --arg generated_unix_ms "${GENERATED_UNIX_MS}" \
    --arg durability_seed "${DURABILITY_SEED}" \
    --arg remote_target_dir "${REMOTE_TARGET_DIR}" \
    --arg remote_tmp_dir "${REMOTE_TMP_DIR}" \
    --arg remote_cflags "${REMOTE_CFLAGS}" \
    --arg diff_report "${DIFF_REPORT#${WORKSPACE_ROOT}/}" \
    --arg durability_report "${DURABILITY_REPORT#${WORKSPACE_ROOT}/}" \
    --arg unit_replay_log "${UNIT_REPLAY_LOG#${WORKSPACE_ROOT}/}" \
    --arg unit_summary_log "${UNIT_SUMMARY_LOG#${WORKSPACE_ROOT}/}" \
    --arg unit_manifest_log "${UNIT_MANIFEST_LOG#${WORKSPACE_ROOT}/}" \
    --arg diff_log "${DIFF_LOG#${WORKSPACE_ROOT}/}" \
    --arg durability_log "${DURABILITY_LOG#${WORKSPACE_ROOT}/}" \
    --arg diff_outcome "${DIFF_OUTCOME}" \
    --arg durability_outcome "${DURABILITY_OUTCOME}" \
    --arg diff_replay_command "${DIFF_REPLAY_COMMAND}" \
    --arg durability_replay_matrix "${DURABILITY_REPLAY_MATRIX}" \
    --arg durability_replay_dry_run "${DURABILITY_REPLAY_DRY_RUN}" \
    --argjson artifact_hashes "${artifact_hashes_json}" \
    --argfile differential "${DIFF_REPORT}" \
    --argfile durability "${DURABILITY_REPORT}" \
    '
      {
        schema_version: $schema_version,
        bead_id: $bead_id,
        run: {
          run_id: $run_id,
          trace_id: $trace_id,
          commit_sha: $commit_sha
        },
        deterministic_inputs: {
          differential_lane: $lane,
          differential_seed: $diff_seed,
          generated_unix_ms: $generated_unix_ms,
          durability_seed: $durability_seed,
          remote_target_dir: $remote_target_dir,
          remote_tmp_dir: $remote_tmp_dir,
          remote_cflags: $remote_cflags
        },
        checks: {
          unit_replay_command_generation: "pass",
          unit_human_summary_replay_embedding: "pass",
          unit_artifact_manifest_assembly: "pass",
          differential_lane_outcome: $diff_outcome,
          durability_lane_outcome: $durability_outcome
        },
        replay: {
          differential: $diff_replay_command,
          durability_matrix: $durability_replay_matrix,
          durability_dry_run: $durability_replay_dry_run
        },
        artifacts: {
          differential_report: $diff_report,
          durability_report: $durability_report,
          unit_replay_log: $unit_replay_log,
          unit_summary_log: $unit_summary_log,
          unit_manifest_log: $unit_manifest_log,
          differential_log: $diff_log,
          durability_log: $durability_log
        },
        artifact_hashes: $artifact_hashes,
        differential_gate: $differential,
        durability_gate: $durability
      }
    '
)"

printf '%s\n' "${REPORT_PAYLOAD}" >"${REPORT_JSON}"

cat >"${REPORT_MD}" <<EOF
# ${BEAD_ID} Certification Report

- run_id: ${RUN_ID}
- trace_id: ${TRACE_ID}
- commit_sha: ${COMMIT_SHA}
- differential_lane: ${DIFF_LANE}
- differential_seed: ${DIFF_SEED}
- generated_unix_ms: ${GENERATED_UNIX_MS}
- durability_seed: ${DURABILITY_SEED}
- differential_lane_outcome: ${DIFF_OUTCOME}
- durability_lane_outcome: ${DURABILITY_OUTCOME}

## Replay Commands

\`${DIFF_REPLAY_COMMAND}\`

\`${DURABILITY_REPLAY_MATRIX}\`

\`${DURABILITY_REPLAY_DRY_RUN}\`

## Artifacts

- report_json: ${REPORT_JSON#${WORKSPACE_ROOT}/}
- differential_report: ${DIFF_REPORT#${WORKSPACE_ROOT}/}
- durability_report: ${DURABILITY_REPORT#${WORKSPACE_ROOT}/}
- unit_replay_log: ${UNIT_REPLAY_LOG#${WORKSPACE_ROOT}/}
- unit_summary_log: ${UNIT_SUMMARY_LOG#${WORKSPACE_ROOT}/}
- unit_manifest_log: ${UNIT_MANIFEST_LOG#${WORKSPACE_ROOT}/}
- differential_log: ${DIFF_LOG#${WORKSPACE_ROOT}/}
- durability_log: ${DURABILITY_LOG#${WORKSPACE_ROOT}/}
EOF

if ${JSON_OUTPUT}; then
  printf '%s\n' "${REPORT_PAYLOAD}"
else
  echo "=== ${BEAD_ID} deterministic parity/durability certification ==="
  echo "Run ID:                ${RUN_ID}"
  echo "Trace ID:              ${TRACE_ID}"
  echo "Commit SHA:            ${COMMIT_SHA}"
  echo "Differential lane:     ${DIFF_LANE}"
  echo "Differential outcome:  ${DIFF_OUTCOME}"
  echo "Durability outcome:    ${DURABILITY_OUTCOME}"
  echo "Report JSON:           ${REPORT_JSON#${WORKSPACE_ROOT}/}"
  echo "Report Markdown:       ${REPORT_MD#${WORKSPACE_ROOT}/}"
fi
