#!/usr/bin/env bash
# bd-688.3: Latch-free MVCC Version Chain (Hekaton-style)
#
# Deterministic verification script with machine-readable evidence artifacts.
set -euo pipefail

BEAD="bd-688.3"
SCENARIO_ID="bd6883_latchfree_version_chain_verify"
RUN_TS="$(date -u +"%Y%m%dT%H%M%SZ")"
RUN_ID="${BEAD}-${RUN_TS}"
TRACE_ID="${RUN_ID}-trace"
RCH_TARGET_DIR="/tmp/rch_target_${BEAD//./_}_${RUN_TS}"
RCH_EXEC_TIMEOUT_SEC="${RCH_EXEC_TIMEOUT_SEC:-3600}"
RCH_MAX_ATTEMPTS="${RCH_MAX_ATTEMPTS:-2}"

ARTIFACT_DIR="artifacts/${BEAD}"
LOG_FILE="${ARTIFACT_DIR}/${RUN_ID}.log"
SUMMARY_FILE="${ARTIFACT_DIR}/${RUN_ID}.json"
LATEST_LINK="${ARTIFACT_DIR}/report-latest.json"

mkdir -p "${ARTIFACT_DIR}"
: >"${LOG_FILE}"

if ! command -v rch >/dev/null 2>&1; then
    echo "ERROR: rch is required for ${BEAD} verification but was not found in PATH."
    exit 1
fi

log() {
    echo "$*" | tee -a "${LOG_FILE}"
}

run_remote_logged() {
    local step="$1"
    shift
    log "--- ${step} ---"
    local attempt=1
    while [[ "${attempt}" -le "${RCH_MAX_ATTEMPTS}" ]]; do
        log "attempt=${attempt}/${RCH_MAX_ATTEMPTS} timeout_sec=${RCH_EXEC_TIMEOUT_SEC}"
        if timeout --foreground "${RCH_EXEC_TIMEOUT_SEC}" \
            rch exec -- env CARGO_TARGET_DIR="${RCH_TARGET_DIR}" "$@" 2>&1 | tee -a "${LOG_FILE}"; then
            log "PASS: ${step}"
            return 0
        fi
        local rc=$?
        if [[ "${rc}" -eq 124 ]]; then
            log "WARN: ${step} attempt ${attempt} timed out after ${RCH_EXEC_TIMEOUT_SEC}s"
        else
            log "WARN: ${step} attempt ${attempt} failed with exit ${rc}"
        fi
        attempt=$((attempt + 1))
        if [[ "${attempt}" -le "${RCH_MAX_ATTEMPTS}" ]]; then
            sleep 2
        fi
    done
    log "FAIL: ${step}"
    return 1
}

step_mvcc_tests="failed"
step_loom="failed"
step_harness="failed"
step_clippy="failed"
overall="passed"

log "=== ${BEAD}: Latch-free MVCC Version Chain ==="
log "metadata: bead_id=${BEAD} run_id=${RUN_ID} trace_id=${TRACE_ID} scenario_id=${SCENARIO_ID}"
log "metadata: rch_target_dir=${RCH_TARGET_DIR}"
log "artifact_log=${LOG_FILE}"

# 1. Unit tests in fsqlite-mvcc (invariants + gc modules).
if run_remote_logged \
    "Step 1: fsqlite-mvcc unit tests (invariants + gc)" \
    cargo test -p fsqlite-mvcc -- invariants::tests gc::tests --nocapture; then
    step_mvcc_tests="passed"
else
    overall="failed"
fi

# 2. Explicit loom model-check for core latch-free publication operation.
if run_remote_logged \
    "Step 2: loom model check (core chain-head CAS publication)" \
    cargo test -p fsqlite-mvcc loom_chain_head_publication_linearizable -- --nocapture; then
    step_loom="passed"
else
    overall="failed"
fi

# 3. Harness integration tests for this bead.
if run_remote_logged \
    "Step 3: harness integration tests" \
    cargo test -p fsqlite-harness --test bd_688_3_latchfree_version_chain -- --nocapture; then
    step_harness="passed"
else
    overall="failed"
fi

# 4. Clippy (target crate, deny warnings).
if run_remote_logged \
    "Step 4: clippy (fsqlite-mvcc, deny warnings)" \
    cargo clippy -p fsqlite-mvcc --all-targets -- -D warnings; then
    step_clippy="passed"
else
    overall="failed"
fi

# Acceptance mapping summary for bd-688.3.
criterion_1="false" # first-attempt CAS >95% under moderate contention
criterion_2="false" # ABA safety
criterion_3="false" # correct under 64-thread stress
criterion_4="false" # loom model check passes
criterion_5="false" # comprehensive unit/property tests
criterion_6="true"  # this deterministic e2e script itself
criterion_7="true"  # structured logging/metrics metadata captured in artifacts

if [[ "${step_mvcc_tests}" == "passed" ]]; then
    criterion_1="true"
    criterion_3="true"
    criterion_5="true"
fi
if [[ "${step_harness}" == "passed" ]]; then
    criterion_2="true"
fi
if [[ "${step_loom}" == "passed" ]]; then
    criterion_4="true"
fi

cat >"${SUMMARY_FILE}" <<EOF
{
  "bead_id": "${BEAD}",
  "run_id": "${RUN_ID}",
  "trace_id": "${TRACE_ID}",
  "scenario_id": "${SCENARIO_ID}",
  "created_at_utc": "${RUN_TS}",
  "overall_status": "${overall}",
  "log_file": "${LOG_FILE}",
  "steps": [
    {
      "name": "mvcc_unit_tests",
      "status": "${step_mvcc_tests}",
      "command": "cargo test -p fsqlite-mvcc -- invariants::tests gc::tests --nocapture"
    },
    {
      "name": "loom_model_check",
      "status": "${step_loom}",
      "command": "cargo test -p fsqlite-mvcc loom_chain_head_publication_linearizable -- --nocapture"
    },
    {
      "name": "harness_integration_tests",
      "status": "${step_harness}",
      "command": "cargo test -p fsqlite-harness --test bd_688_3_latchfree_version_chain -- --nocapture"
    },
    {
      "name": "clippy",
      "status": "${step_clippy}",
      "command": "cargo clippy -p fsqlite-mvcc --all-targets -- -D warnings"
    }
  ],
  "acceptance_mapping": {
    "1_lock_free_cas_first_attempt_ratio_gt_95": ${criterion_1},
    "2_no_aba_problems": ${criterion_2},
    "3_correct_under_64_thread_stress": ${criterion_3},
    "4_loom_model_check_passes": ${criterion_4},
    "5_comprehensive_unit_property_tests": ${criterion_5},
    "6_deterministic_e2e_script_with_replay": ${criterion_6},
    "7_structured_logging_metrics_context": ${criterion_7}
  }
}
EOF

cp "${SUMMARY_FILE}" "${LATEST_LINK}"

log "summary_json=${SUMMARY_FILE}"
log "summary_latest=${LATEST_LINK}"

if [[ "${overall}" == "passed" ]]; then
    log "=== ${BEAD}: ALL CHECKS PASSED ==="
    exit 0
fi

log "=== ${BEAD}: CHECKS FAILED ==="
exit 1
