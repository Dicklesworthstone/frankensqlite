#!/usr/bin/env bash
# Deterministic fallback-denial replay gate for bd-2yqp6.7.9.3.

set -euo pipefail

BEAD_ID="bd-2yqp6.7.9.3"
SCENARIO_ID="G9-FALLBACK-DENIAL-REPLAY"
SEED=793
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_ID="${BEAD_ID}-$(date -u +%Y%m%dT%H%M%SZ)-${SEED}"
TRACE_ID="trace-${RUN_ID}"
ARTIFACT_ROOT="${FSQLITE_G9_REPLAY_ARTIFACT_ROOT:-${REPO_ROOT}/artifacts/${BEAD_ID}}"
ARTIFACT_DIR="${ARTIFACT_ROOT}/${RUN_ID}"
SUMMARY_JSON="${ARTIFACT_DIR}/fallback_denial_replay_summary.json"
TEST_LOG="${ARTIFACT_DIR}/fallback_denial_replay_test.log"
TARGET_DIR="${FSQLITE_G9_REPLAY_TARGET_DIR:-/data/tmp/frankensqlite-g9-fallback-denial-replay-target}"
JSON_OUTPUT=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --json)
      JSON_OUTPUT=true
      shift
      ;;
    *)
      echo "ERROR: unknown argument: $1" >&2
      exit 2
      ;;
  esac
done

mkdir -p "${ARTIFACT_DIR}"

TEST_COMMAND=(
  cargo test
  -p fsqlite-core
  --test agent_swarm_fallback_transparency_contract
  deterministic_fallback_denial_replay
  --
  --nocapture
)

REPLAY_COMMAND="rch exec -- env CARGO_TARGET_DIR=${TARGET_DIR} cargo test -p fsqlite-core --test agent_swarm_fallback_transparency_contract deterministic_fallback_denial_replay -- --nocapture"

run_replay() {
  if command -v rch >/dev/null 2>&1 && [[ "${FSQLITE_DISABLE_RCH:-0}" != "1" ]]; then
    rch exec -- env CARGO_TARGET_DIR="${TARGET_DIR}" "${TEST_COMMAND[@]}"
  else
    env CARGO_TARGET_DIR="${TARGET_DIR}" "${TEST_COMMAND[@]}"
  fi
}

STARTED_UNIX_MS="$(date -u +%s%3N)"
set +e
run_replay >"${TEST_LOG}" 2>&1
EXIT_CODE="$?"
set -e
FINISHED_UNIX_MS="$(date -u +%s%3N)"

if [[ "${EXIT_CODE}" -eq 0 ]]; then
  OUTCOME="pass"
  FIRST_FAILURE="null"
else
  OUTCOME="fail"
  FIRST_FAILURE="$(jq -n \
    --arg summary "G9 fallback-denial replay contract failed" \
    --arg log_path "${TEST_LOG}" \
    '{summary: $summary, log_path: $log_path}')"
fi

TEST_LOG_SHA256="$(sha256sum "${TEST_LOG}" | awk '{print $1}')"
jq -n \
  --arg bead_id "${BEAD_ID}" \
  --arg trace_id "${TRACE_ID}" \
  --arg run_id "${RUN_ID}" \
  --arg scenario_id "${SCENARIO_ID}" \
  --arg replay_command "${REPLAY_COMMAND}" \
  --arg outcome "${OUTCOME}" \
  --arg test_log "${TEST_LOG}" \
  --arg test_log_sha256 "${TEST_LOG_SHA256}" \
  --argjson seed "${SEED}" \
  --argjson started_unix_ms "${STARTED_UNIX_MS}" \
  --argjson finished_unix_ms "${FINISHED_UNIX_MS}" \
  --argjson exit_code "${EXIT_CODE}" \
  --argjson first_failure "${FIRST_FAILURE}" \
  '{
    schema_version: "fallback_denial_replay.v1",
    bead_id: $bead_id,
    trace_id: $trace_id,
    run_id: $run_id,
    scenario_id: $scenario_id,
    seed: $seed,
    started_unix_ms: $started_unix_ms,
    finished_unix_ms: $finished_unix_ms,
    replay_command: $replay_command,
    outcome: $outcome,
    exit_code: $exit_code,
    artifacts: [
      {
        artifact_id: "fallback_denial_replay_test_log",
        role: "test_log",
        path: $test_log,
        sha256: $test_log_sha256,
        required: true
      }
    ],
    first_failure: $first_failure
  }' >"${SUMMARY_JSON}"

if [[ "${JSON_OUTPUT}" == "true" ]]; then
  cat "${SUMMARY_JSON}"
else
  echo "G9 fallback-denial replay: ${OUTCOME}"
  echo "summary: ${SUMMARY_JSON}"
  echo "test_log: ${TEST_LOG}"
  echo "replay_command: ${REPLAY_COMMAND}"
fi

exit "${EXIT_CODE}"
