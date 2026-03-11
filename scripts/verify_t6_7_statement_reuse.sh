#!/usr/bin/env bash
# Verification gate for bd-1dp9.6.7.2.3:
# Prepared/ad-hoc reuse proof pack with invalidation property tests and replay logs.

set -euo pipefail

WORKSPACE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BEAD_ID="bd-1dp9.6.7.2.3"
SCENARIO_ID="${SCENARIO_ID:-STATEMENT-REUSE-67023}"
SEED="${SEED:-67023}"
TIMESTAMP_UTC="$(date -u +%Y%m%dT%H%M%SZ)"
RUN_ID="${RUN_ID:-${BEAD_ID}-${TIMESTAMP_UTC}-${SEED}}"
TRACE_ID="${TRACE_ID:-${SEED}}"
ARTIFACT_DIR="${WORKSPACE_ROOT}/artifacts/${BEAD_ID}/${RUN_ID}"
EVENTS_JSONL="${ARTIFACT_DIR}/events.jsonl"
REPORT_JSON="${ARTIFACT_DIR}/report.json"
SUMMARY_MD="${ARTIFACT_DIR}/summary.md"

mkdir -p "${ARTIFACT_DIR}"
: > "${EVENTS_JSONL}"

export RUN_ID
export TRACE_ID
export SCENARIO_ID
export SEED
export NO_COLOR="${NO_COLOR:-1}"
export RUST_TEST_THREADS="${RUST_TEST_THREADS:-1}"
export RUST_LOG="${RUST_LOG:-fsqlite.statement_reuse=info,fsqlite.statement=debug,fsqlite.execution=debug}"

emit_event() {
  local phase="$1"
  local event_type="$2"
  local outcome="$3"
  local elapsed_ms="$4"
  local message="$5"

  printf '{"trace_id":"%s","run_id":"%s","scenario_id":"%s","seed":%s,"phase":"%s","event_type":"%s","outcome":"%s","elapsed_ms":%s,"timestamp":"%s","message":"%s"}\n' \
    "${TRACE_ID}" "${RUN_ID}" "${SCENARIO_ID}" "${SEED}" "${phase}" "${event_type}" "${outcome}" "${elapsed_ms}" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "${message}" \
    >> "${EVENTS_JSONL}"
}

assert_log_contains() {
  local logfile="$1"
  local pattern="$2"
  local message="$3"
  if ! grep -Eq "${pattern}" "${logfile}"; then
    echo "[GATE FAIL] ${message}" >&2
    return 1
  fi
}

assert_statement_reuse_trace_contract() {
  local logfile="$1"
  local matched=0

  while IFS= read -r line; do
    matched=$((matched + 1))
    for field in \
      'run_id=' \
      'trace_id=' \
      'scenario_id=' \
      'cache_kind=' \
      'statement_fingerprint=' \
      'schema_identity=' \
      'cache_hit=' \
      'invalidate_reason=' \
      'compile_ns=' \
      'execute_ns=' \
      'backend_identity=' \
      'first_failure_diag='; do
      if [[ "${line}" != *"${field}"* ]]; then
        echo "[GATE FAIL] missing ${field} in trace line: ${line}" >&2
        return 1
      fi
    done
  done < <(grep 'statement_fingerprint=' "${logfile}" || true)

  if [[ "${matched}" -eq 0 ]]; then
    echo "[GATE FAIL] no statement reuse telemetry found in ${logfile}" >&2
    return 1
  fi

  assert_log_contains "${logfile}" 'cache_kind=.*parse' \
    "trace contract missing parse cache telemetry"
  assert_log_contains "${logfile}" 'cache_kind=.*compiled' \
    "trace contract missing compiled cache telemetry"
  assert_log_contains "${logfile}" 'cache_kind=.*execution' \
    "trace contract missing execution telemetry"
  assert_log_contains "${logfile}" 'cache_kind=.*prepared_schema_validation' \
    "trace contract missing prepared-schema invalidation telemetry"
  assert_log_contains "${logfile}" 'invalidate_reason=.*schema_cookie_changed' \
    "trace contract missing schema invalidation reason"
  assert_log_contains "${logfile}" 'backend_identity=.*parity_cert_strict' \
    "trace contract missing strict parity-cert backend identity"
  assert_log_contains "${logfile}" 'first_failure_diag=.*prepared_schema_identity=' \
    "trace contract missing first-failure schema diagnostics"
}

run_phase() {
  local phase="$1"
  local logfile="$2"
  shift 2

  emit_event "${phase}" "start" "running" 0 "running: $*"
  local started
  started="$(date +%s%3N)"

  if (
    cd "${WORKSPACE_ROOT}"
    "$@"
  ) 2>&1 | tee "${logfile}"; then
    local finished elapsed
    finished="$(date +%s%3N)"
    elapsed="$((finished - started))"
    if ! grep -Eq '^running [1-9][0-9]* tests?$' "${logfile}"; then
      emit_event "${phase}" "fail" "fail" "${elapsed}" "command completed without executing tests: $*"
      echo "[GATE FAIL] ${phase}: no tests executed" >&2
      return 1
    fi
    emit_event "${phase}" "pass" "pass" "${elapsed}" "completed: $*"
  else
    local finished elapsed
    finished="$(date +%s%3N)"
    elapsed="$((finished - started))"
    emit_event "${phase}" "fail" "fail" "${elapsed}" "failed: $*"
    return 1
  fi
}

COMMON_ENV=(
  env
  "RUN_ID=${RUN_ID}"
  "TRACE_ID=${TRACE_ID}"
  "SCENARIO_ID=${SCENARIO_ID}"
  "SEED=${SEED}"
  "NO_COLOR=${NO_COLOR}"
  "RUST_TEST_THREADS=${RUST_TEST_THREADS}"
  "RUST_LOG=${RUST_LOG}"
)

REGRESSION_CMD=(rch exec -- "${COMMON_ENV[@]}" cargo test -p fsqlite-core statement_reuse_regression_ -- --nocapture)
PROPERTY_CMD=(rch exec -- "${COMMON_ENV[@]}" cargo test -p fsqlite-core proptest_statement_reuse_ -- --nocapture)
TRACE_CMD=(rch exec -- "${COMMON_ENV[@]}" cargo test -p fsqlite-core statement_reuse_regression_file_backed_trace_contract -- --nocapture)

echo "=== ${BEAD_ID}: statement reuse verification ==="
echo "run_id=${RUN_ID}"
echo "trace_id=${TRACE_ID}"
echo "scenario_id=${SCENARIO_ID}"
echo "seed=${SEED}"
echo "artifacts=${ARTIFACT_DIR}"

emit_event "bootstrap" "start" "running" 0 "verification started"

run_phase \
  "regression_suite" \
  "${ARTIFACT_DIR}/regression_suite.log" \
  "${REGRESSION_CMD[@]}"

run_phase \
  "property_suite" \
  "${ARTIFACT_DIR}/property_suite.log" \
  "${PROPERTY_CMD[@]}"

run_phase \
  "file_backed_trace_contract" \
  "${ARTIFACT_DIR}/file_backed_trace_contract.log" \
  "${TRACE_CMD[@]}"

assert_statement_reuse_trace_contract "${ARTIFACT_DIR}/file_backed_trace_contract.log"

cat > "${SUMMARY_MD}" <<EOF
# ${BEAD_ID} Statement Reuse Proof Pack

- Scenario matrix reference: T6.7.1 repeated SQL hotspot matrix
- Artifact pack dependency: bd-1dp9.6.7.1.2
- Regression suite: \`statement_reuse_regression_\`
- Property suite: \`proptest_statement_reuse_\`
- Trace contract: validated against \`fsqlite.statement_reuse\` structured fields
- Replay (regressions): \`RUN_ID='${RUN_ID}' TRACE_ID='${TRACE_ID}' SCENARIO_ID='${SCENARIO_ID}' SEED='${SEED}' ${REGRESSION_CMD[*]}\`
- Replay (properties): \`RUN_ID='${RUN_ID}' TRACE_ID='${TRACE_ID}' SCENARIO_ID='${SCENARIO_ID}' SEED='${SEED}' ${PROPERTY_CMD[*]}\`
- Replay (trace contract): \`RUN_ID='${RUN_ID}' TRACE_ID='${TRACE_ID}' SCENARIO_ID='${SCENARIO_ID}' SEED='${SEED}' ${TRACE_CMD[*]}\`
EOF

cat > "${REPORT_JSON}" <<EOF
{
  "bead_id": "${BEAD_ID}",
  "trace_id": "${TRACE_ID}",
  "run_id": "${RUN_ID}",
  "scenario_id": "${SCENARIO_ID}",
  "seed": ${SEED},
  "result": "pass",
  "scenario_matrix_ref": "T6.7.1",
  "artifact_pack_ref": "bd-1dp9.6.7.1.2",
  "rust_log": "${RUST_LOG}",
  "rust_test_threads": "${RUST_TEST_THREADS}",
  "events_jsonl": "${EVENTS_JSONL}",
  "summary_md": "${SUMMARY_MD}",
  "log_files": [
    "${ARTIFACT_DIR}/regression_suite.log",
    "${ARTIFACT_DIR}/property_suite.log",
    "${ARTIFACT_DIR}/file_backed_trace_contract.log"
  ],
  "replay_commands": [
    "RUN_ID='${RUN_ID}' TRACE_ID='${TRACE_ID}' SCENARIO_ID='${SCENARIO_ID}' SEED='${SEED}' ${REGRESSION_CMD[*]}",
    "RUN_ID='${RUN_ID}' TRACE_ID='${TRACE_ID}' SCENARIO_ID='${SCENARIO_ID}' SEED='${SEED}' ${PROPERTY_CMD[*]}",
    "RUN_ID='${RUN_ID}' TRACE_ID='${TRACE_ID}' SCENARIO_ID='${SCENARIO_ID}' SEED='${SEED}' ${TRACE_CMD[*]}"
  ]
}
EOF

emit_event "finalize" "pass" "pass" 0 "report written to ${REPORT_JSON}"
echo "[GATE PASS] ${BEAD_ID} statement reuse gate passed"
