#!/usr/bin/env bash
set -euo pipefail

BEAD_ID="bd-2v8x"
SCENARIO_ID="${SCENARIO_ID:-BUILD-2}"
SEED="${SEED:-2026022003}"
RUN_ID="${BEAD_ID}-$(date -u +%Y%m%dT%H%M%SZ)-$$"
LOG_STANDARD_REF="${LOG_STANDARD_REF:-docs/e2e_shell_script_log_profile.json}"
WORKSPACE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FSQLITE_MANIFEST="${WORKSPACE_ROOT}/crates/fsqlite/Cargo.toml"
WORKSPACE_MANIFEST="${WORKSPACE_ROOT}/Cargo.toml"
REPORT_DIR="${WORKSPACE_ROOT}/test-results"
SCHEMA_LOG_PATH="${REPORT_DIR}/bd_2v8x_compliance_events.jsonl"

mkdir -p "${REPORT_DIR}"
: >"${SCHEMA_LOG_PATH}"

emit_schema_event() {
    local phase="$1"
    local event_type="$2"
    local outcome="$3"
    local timestamp
    timestamp="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

    printf '{"run_id":"%s","timestamp":"%s","phase":"%s","event_type":"%s","scenario_id":"%s","seed":"%s","context":{"bead_id":"%s","outcome":"%s","log_standard_ref":"%s","schema_log_path":"%s"}}\n' \
        "${RUN_ID}" "${timestamp}" "${phase}" "${event_type}" "${SCENARIO_ID}" "${SEED}" "${BEAD_ID}" "${outcome}" "${LOG_STANDARD_REF}" "${SCHEMA_LOG_PATH}" \
        >>"${SCHEMA_LOG_PATH}"
}

printf 'bead_id=%s level=DEBUG case=start workspace=%s\n' "${BEAD_ID}" "${WORKSPACE_ROOT}"
emit_schema_event "setup" "start" "running"

if [[ ! -f "${FSQLITE_MANIFEST}" || ! -f "${WORKSPACE_MANIFEST}" ]]; then
    printf 'bead_id=%s level=ERROR case=missing_manifest fsqlite=%s workspace=%s\n' \
        "${BEAD_ID}" "${FSQLITE_MANIFEST}" "${WORKSPACE_MANIFEST}"
    exit 1
fi

required_feature_lines=(
    '[features]'
    'default = ["native", "linux-asupersync-uring", "json", "fts5", "rtree"]'
    'json = ["dep:fsqlite-ext-json"]'
    'fts5 = ["dep:fsqlite-ext-fts5"]'
    'fts3 = ["dep:fsqlite-ext-fts3"]'
    'rtree = ["dep:fsqlite-ext-rtree"]'
    'session = ["dep:fsqlite-ext-session"]'
    'icu = ["dep:fsqlite-ext-icu"]'
    'misc = ["dep:fsqlite-ext-misc"]'
    'raptorq = []'
    'mvcc = []'
)

for line in "${required_feature_lines[@]}"; do
    if ! rg -Fq "${line}" "${FSQLITE_MANIFEST}"; then
        printf 'bead_id=%s level=ERROR case=missing_feature_marker marker=%q\n' "${BEAD_ID}" "${line}"
        exit 1
    fi
done

required_profile_lines=(
    '[profile.release]'
    'opt-level = "z"'
    'lto = true'
    'codegen-units = 1'
    'panic = "abort"'
    'strip = true'
    '[profile.release-perf]'
    'inherits = "release"'
    'opt-level = 3'
)

for line in "${required_profile_lines[@]}"; do
    if ! rg -Fq "${line}" "${WORKSPACE_MANIFEST}"; then
        printf 'bead_id=%s level=ERROR case=missing_profile_marker marker=%q\n' "${BEAD_ID}" "${line}"
        exit 1
    fi
done

metadata_json="$(mktemp)"
cleanup() {
    local exit_code=$?
    if [[ ${exit_code} -eq 0 ]]; then
        emit_schema_event "report" "pass" "pass"
    else
        emit_schema_event "report" "fail" "fail"
    fi
    rm -f "${metadata_json}"
}
trap cleanup EXIT

(cd "${WORKSPACE_ROOT}" && cargo metadata --format-version=1 >"${metadata_json}")

member_count="$(jq '.workspace_members | length' "${metadata_json}")"
if [[ "${member_count}" -ne 27 ]]; then
    printf 'bead_id=%s level=ERROR case=workspace_member_count expected=27 actual=%s\n' \
        "${BEAD_ID}" "${member_count}"
    exit 1
fi

has_wal_pager_edge="$(
    jq -r '
        . as $m
        | ($m.workspace_members | map((split("/")[-1] | split("#")[0]))) as $members
        | .resolve.nodes[]
        | select((.id | split("/")[-1] | split("#")[0]) == "fsqlite-wal")
        | [.deps[]?
           | select((.dep_kinds | length) == 0 or any(.dep_kinds[]; .kind != "dev"))
           | .name
           | gsub("_"; "-")
           | select(. == "fsqlite-pager")]
        | length
    ' "${metadata_json}"
)"
if [[ "${has_wal_pager_edge}" -ne 0 ]]; then
    printf 'bead_id=%s level=ERROR case=forbidden_edge_present edge=fsqlite-wal->fsqlite-pager\n' \
        "${BEAD_ID}"
    exit 1
fi

if ! jq -r '
        . as $m
        | ($m.workspace_members | map((split("/")[-1] | split("#")[0]))) as $members
        | .resolve.nodes[]
        | (.id | split("/")[-1] | split("#")[0]) as $from
        | select($members | index($from))
        | .deps[]?
        | select((.dep_kinds | length) == 0 or any(.dep_kinds[]; .kind != "dev"))
        | .name as $dep_raw
        | ($dep_raw | gsub("_"; "-")) as $dep
        | select($members | index($dep))
        | "\($from) \($dep)"
    ' "${metadata_json}" | tsort >/dev/null; then
    printf 'bead_id=%s level=ERROR case=dependency_cycle_detected\n' "${BEAD_ID}"
    exit 1
fi

printf \
    'bead_id=%s level=INFO case=static_checks_passed member_count=%s forbidden_edge_absent=1 acyclic=1\n' \
    "${BEAD_ID}" "${member_count}"

if (cd "${WORKSPACE_ROOT}" && ./e2e/build_matrix.sh); then
    printf 'bead_id=%s level=INFO case=degraded_mode_count=0 reference=bd-1fpm\n' "${BEAD_ID}"
    printf 'bead_id=%s level=INFO case=terminal_failure_count=0 reference=bd-1fpm\n' "${BEAD_ID}"
    printf 'bead_id=%s level=INFO case=pass\n' "${BEAD_ID}"
    exit 0
fi

printf 'bead_id=%s level=WARN case=degraded_mode_count=1 reference=bd-1fpm\n' "${BEAD_ID}"
printf 'bead_id=%s level=ERROR case=build_matrix_failed reference=bd-1fpm\n' "${BEAD_ID}"
exit 1
