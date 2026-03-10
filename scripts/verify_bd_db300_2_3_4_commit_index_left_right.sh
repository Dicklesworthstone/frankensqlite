#!/usr/bin/env bash
# Verification artifact for bd-db300.2.3.4:
# attach model-check and reclamation proof obligations to the chosen primitive.
#
# Deterministic replay:
#   bash scripts/verify_bd_db300_2_3_4_commit_index_left_right.sh

set -euo pipefail

WORKSPACE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BEAD_ID="bd-db300.2.3.4"
SCENARIO_ID="COMMIT-INDEX-LEFT-RIGHT-PROOF-OBLIGATIONS-B3.4"
TIMESTAMP_UTC="$(date -u +%Y%m%dT%H%M%SZ)"
RUN_ID="${BEAD_ID}-${TIMESTAMP_UTC}-$$"
TRACE_ID="trace-${RUN_ID}"
GENERATED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
REPLAY_COMMAND="bash scripts/verify_bd_db300_2_3_4_commit_index_left_right.sh"
ARTIFACT_DIR="${OUTPUT_DIR:-${WORKSPACE_ROOT}/artifacts/perf/${BEAD_ID}/${RUN_ID}}"
EVENTS_JSONL="${ARTIFACT_DIR}/events.jsonl"
REPORT_JSON="${ARTIFACT_DIR}/report.json"
MANIFEST_JSON="${ARTIFACT_DIR}/manifest.json"
SUMMARY_MD="${ARTIFACT_DIR}/summary.md"
CHECK_LOG="${ARTIFACT_DIR}/cargo-check.log"
MONOTONE_LOG="${ARTIFACT_DIR}/monotone-test.log"
DRAIN_LOG="${ARTIFACT_DIR}/publish-drain-test.log"

COMMAND_CHECK="rch exec -- cargo check -p fsqlite-mvcc --lib"
COMMAND_MONOTONE="rch exec -- cargo test -p fsqlite-mvcc test_commit_index_latest_monotone_under_concurrent_updates -- --nocapture"
COMMAND_DRAIN="rch exec -- cargo test -p fsqlite-mvcc test_commit_index_left_right_publish_drains_old_readers_before_mirror -- --nocapture"

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
        --arg bead_id "${BEAD_ID}" \
        --arg scenario_id "${SCENARIO_ID}" \
        --arg phase "${phase}" \
        --arg event_type "${event_type}" \
        --arg outcome "${outcome}" \
        --arg message "${message}" \
        --arg timestamp "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
        '{
            trace_id: $trace_id,
            run_id: $run_id,
            bead_id: $bead_id,
            scenario_id: $scenario_id,
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
    echo "ERROR: ${message}" >&2
    exit 1
}

require_tool() {
    local tool_name="$1"
    command -v "${tool_name}" >/dev/null 2>&1 || fail "bootstrap" "missing required tool: ${tool_name}"
}

require_file() {
    local path="$1"
    [[ -f "${path}" ]] || fail "inputs" "missing required file: ${path}"
}

require_match() {
    local file="$1"
    local pattern="$2"
    local description="$3"
    if rg -n --fixed-strings -- "${pattern}" "${file}" >/dev/null; then
        emit_event "inputs" "symbol_present" "pass" "${description}"
        return
    fi
    fail "inputs" "missing pattern in ${file}: ${description} :: ${pattern}"
}

run_and_capture() {
    local phase="$1"
    local description="$2"
    local log_path="$3"
    shift 3
    emit_event "${phase}" "command_start" "running" "${description}"
    if "$@" 2>&1 | tee "${log_path}"; then
        emit_event "${phase}" "command_pass" "pass" "${description}"
        return
    fi
    emit_event "${phase}" "command_fail" "fail" "${description}"
    fail "${phase}" "${description}"
}

emit_event "bootstrap" "start" "running" "commit index left-right proof verification started"

require_tool jq
require_tool rg
require_tool rch

require_file "${WORKSPACE_ROOT}/.beads/issues.jsonl"
require_file "${WORKSPACE_ROOT}/crates/fsqlite-mvcc/src/core_types.rs"

require_match "${WORKSPACE_ROOT}/.beads/issues.jsonl" '"id":"bd-db300.2.3.3"' "B3.3 prototype bead is present"
require_match "${WORKSPACE_ROOT}/.beads/issues.jsonl" '"id":"bd-db300.2.3.4"' "B3.4 proof-obligation bead is present"
require_match "${WORKSPACE_ROOT}/.beads/issues.jsonl" '"id":"bd-db300.7.5"' "Track G verification-matrix bead is present"
require_match "${WORKSPACE_ROOT}/.beads/issues.jsonl" '"id":"bd-db300.7.8"' "Track G interference-microbench bead is present"
require_match "${WORKSPACE_ROOT}/crates/fsqlite-mvcc/src/core_types.rs" 'struct LeftRightCommitIndexShard {' "CommitIndex left-right shard still exists"
require_match "${WORKSPACE_ROOT}/crates/fsqlite-mvcc/src/core_types.rs" 'CI-LR-1 monotone-publication' "proof obligation CI-LR-1 is documented in code"
require_match "${WORKSPACE_ROOT}/crates/fsqlite-mvcc/src/core_types.rs" 'CI-LR-2 publish-before-drain' "proof obligation CI-LR-2 is documented in code"
require_match "${WORKSPACE_ROOT}/crates/fsqlite-mvcc/src/core_types.rs" 'CI-LR-3 post-update-convergence' "proof obligation CI-LR-3 is documented in code"
require_match "${WORKSPACE_ROOT}/crates/fsqlite-mvcc/src/core_types.rs" 'fn test_commit_index_latest_monotone_under_concurrent_updates()' "monotone publication stress test exists"
require_match "${WORKSPACE_ROOT}/crates/fsqlite-mvcc/src/core_types.rs" 'fn test_commit_index_left_right_publish_drains_old_readers_before_mirror()' "publish-before-drain deterministic test exists"

run_and_capture "verify" "${COMMAND_CHECK}" "${CHECK_LOG}" \
    rch exec -- cargo check -p fsqlite-mvcc --lib
run_and_capture "verify" "${COMMAND_MONOTONE}" "${MONOTONE_LOG}" \
    rch exec -- cargo test -p fsqlite-mvcc test_commit_index_latest_monotone_under_concurrent_updates -- --nocapture
run_and_capture "verify" "${COMMAND_DRAIN}" "${DRAIN_LOG}" \
    rch exec -- cargo test -p fsqlite-mvcc test_commit_index_left_right_publish_drains_old_readers_before_mirror -- --nocapture

emit_event "report" "start" "running" "writing proof-obligation report"

jq -n \
    --arg schema_version "frankensqlite.db300.commit_index_left_right_proof_obligations.v1" \
    --arg bead_id "${BEAD_ID}" \
    --arg run_id "${RUN_ID}" \
    --arg trace_id "${TRACE_ID}" \
    --arg generated_at "${GENERATED_AT}" \
    --arg scenario_id "${SCENARIO_ID}" \
    --arg replay_command "${REPLAY_COMMAND}" \
    --arg report_json "${REPORT_JSON}" \
    --arg summary_md "${SUMMARY_MD}" \
    --arg events_jsonl "${EVENTS_JSONL}" \
    --arg check_command "${COMMAND_CHECK}" \
    --arg monotone_command "${COMMAND_MONOTONE}" \
    --arg drain_command "${COMMAND_DRAIN}" \
    --arg check_log "${CHECK_LOG}" \
    --arg monotone_log "${MONOTONE_LOG}" \
    --arg drain_log "${DRAIN_LOG}" \
    '
    {
        schema_version: $schema_version,
        bead_id: $bead_id,
        run_id: $run_id,
        trace_id: $trace_id,
        generated_at: $generated_at,
        scenario_id: $scenario_id,
        replay_command: $replay_command,
        exact_path: {
            file: "crates/fsqlite-mvcc/src/core_types.rs",
            symbols: [
                "LeftRightCommitIndexShard",
                "CommitIndex::latest",
                "CommitIndex::update",
                "test_commit_index_latest_monotone_under_concurrent_updates",
                "test_commit_index_left_right_publish_drains_old_readers_before_mirror"
            ]
        },
        chosen_primitive: {
            class: "read-mostly page metadata publication",
            primitive: "duplicated left-right shard with shard-local writer mutex and side-specific reader counters",
            reclamation_scope: "no heap-node reclamation in current prototype; proof scope is grace-period-before-overwrite of the old side"
        },
        obligations: [
            {
                id: "CI-LR-1",
                title: "monotone-publication",
                invariant: "Once a page read observes commit sequence N, later reads of that page must not regress below N.",
                current_checks: [
                    {
                        type: "targeted_unit_stress",
                        owner: "fsqlite-mvcc",
                        entrypoint: $monotone_command,
                        log_path: $monotone_log
                    }
                ],
                reuse: {
                    g5: [
                        "record as required unit-stress coverage for bd-db300.7.5",
                        "map to coverage ownership in bd-db300.7.5.1"
                    ],
                    g8: [
                        "same-page hot-reader/hot-writer interference case for bd-db300.7.8.1",
                        "same-LLC and SMT placement latency/fairness capture for bd-db300.7.8"
                    ]
                }
            },
            {
                id: "CI-LR-2",
                title: "publish-before-drain",
                invariant: "After the active side flips, new readers observe the new value while old-side readers keep the previous committed copy until they drain.",
                current_checks: [
                    {
                        type: "deterministic_unit_orchestration",
                        owner: "fsqlite-mvcc",
                        entrypoint: $drain_command,
                        log_path: $drain_log
                    }
                ],
                reuse: {
                    g5: [
                        "record as mandatory concurrency-orchestration coverage for bd-db300.7.5.2",
                        "treat failure as blocking because it invalidates the publication contract"
                    ],
                    g8: [
                        "microbench read-side retry and side-flip cost under pinned-reader interference for bd-db300.7.8.1",
                        "cross-CCD fallback only after same-LLC results are established"
                    ]
                }
            },
            {
                id: "CI-LR-3",
                title: "post-update-convergence",
                invariant: "Once update() returns, both copies contain the same commit sequence for the updated page.",
                current_checks: [
                    {
                        type: "deterministic_unit_state_inspection",
                        owner: "fsqlite-mvcc",
                        entrypoint: $drain_command,
                        log_path: $drain_log
                    },
                    {
                        type: "targeted_compile_guard",
                        owner: "fsqlite-mvcc",
                        entrypoint: $check_command,
                        log_path: $check_log
                    }
                ],
                reuse: {
                    g5: [
                        "attach to rerun matrix as the minimal post-merge regression gate for the CommitIndex primitive",
                        "treat any future RCU/QSBR conversion as requiring a new reclamation bead rather than silently inheriting this weaker proof"
                    ],
                    g8: [
                        "use convergence requirement as the correctness oracle for candidate publication microbenches",
                        "reject any primitive tuning that improves throughput but violates post-update equality"
                    ]
                }
            }
        ],
        executed_commands: [
            {
                command: $check_command,
                status: "pass",
                log_path: $check_log
            },
            {
                command: $monotone_command,
                status: "pass",
                log_path: $monotone_log
            },
            {
                command: $drain_command,
                status: "pass",
                log_path: $drain_log
            }
        ],
        handoff: {
            primary_consumers: ["bd-db300.7.5", "bd-db300.7.8"],
            operator_entrypoint: $replay_command,
            note: "Current left-right duplication avoids pointer-lifetime reclamation. A future pointer-swapping variant must add epoch/hazard-style proofs before replacing this contract."
        },
        artifacts: {
            report_json: $report_json,
            summary_md: $summary_md,
            events_jsonl: $events_jsonl
        }
    }
    ' > "${REPORT_JSON}"

cat > "${SUMMARY_MD}" <<EOF
# ${BEAD_ID} verification summary

- generated_at: \`${GENERATED_AT}\`
- run_id: \`${RUN_ID}\`
- trace_id: \`${TRACE_ID}\`
- replay_command: \`${REPLAY_COMMAND}\`
- exact_path: \`crates/fsqlite-mvcc/src/core_types.rs\`

## Attached proof obligations

1. \`CI-LR-1 monotone-publication\`
   - Evidence: \`${COMMAND_MONOTONE}\`
   - Purpose: ensure \`CommitIndex::latest(page)\` never regresses under concurrent updates.
2. \`CI-LR-2 publish-before-drain\`
   - Evidence: \`${COMMAND_DRAIN}\`
   - Purpose: prove the writer flips publication before mirroring the old side and waits for old-side readers to drain.
3. \`CI-LR-3 post-update-convergence\`
   - Evidence: \`${COMMAND_DRAIN}\` plus \`${COMMAND_CHECK}\`
   - Purpose: prove both copies agree once \`update()\` returns and keep the path build-clean.

## Track G reuse

- \`bd-db300.7.5\`: treat the three \`CI-LR-*\` IDs as the verification-matrix entries for the current CommitIndex publication primitive.
- \`bd-db300.7.8\`: use \`CI-LR-1\` and \`CI-LR-2\` as the correctness oracle for same-page/same-LLC interference microbenches before comparing alternative primitives.
- Reclamation note: this prototype duplicates shard maps instead of reclaiming nodes, so the current proof is a grace-period-before-overwrite proof, not a pointer-lifetime proof.

## Artifact paths

- report_json: \`${REPORT_JSON}\`
- manifest_json: \`${MANIFEST_JSON}\`
- events_jsonl: \`${EVENTS_JSONL}\`
- cargo_check_log: \`${CHECK_LOG}\`
- monotone_test_log: \`${MONOTONE_LOG}\`
- publish_drain_test_log: \`${DRAIN_LOG}\`
EOF

jq -n \
    --arg bead_id "${BEAD_ID}" \
    --arg run_id "${RUN_ID}" \
    --arg generated_at "${GENERATED_AT}" \
    --arg replay_command "${REPLAY_COMMAND}" \
    --arg report_json "${REPORT_JSON}" \
    --arg summary_md "${SUMMARY_MD}" \
    --arg events_jsonl "${EVENTS_JSONL}" \
    --arg check_log "${CHECK_LOG}" \
    --arg monotone_log "${MONOTONE_LOG}" \
    --arg drain_log "${DRAIN_LOG}" \
    '
    {
        bead_id: $bead_id,
        run_id: $run_id,
        generated_at: $generated_at,
        replay_command: $replay_command,
        artifacts: {
            report_json: $report_json,
            summary_md: $summary_md,
            events_jsonl: $events_jsonl,
            cargo_check_log: $check_log,
            monotone_test_log: $monotone_log,
            publish_drain_test_log: $drain_log
        }
    }
    ' > "${MANIFEST_JSON}"

emit_event "report" "artifact_written" "pass" "proof-obligation report written"
emit_event "finalize" "complete" "pass" "commit index left-right proof verification finished"

echo "Wrote ${REPORT_JSON}"
echo "Wrote ${SUMMARY_MD}"
echo "Wrote ${MANIFEST_JSON}"
