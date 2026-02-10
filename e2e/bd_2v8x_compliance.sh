#!/usr/bin/env bash
set -euo pipefail

BEAD_ID="bd-2v8x"
WORKSPACE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FSQLITE_MANIFEST="${WORKSPACE_ROOT}/crates/fsqlite/Cargo.toml"
WORKSPACE_MANIFEST="${WORKSPACE_ROOT}/Cargo.toml"

printf 'bead_id=%s level=DEBUG case=start workspace=%s\n' "${BEAD_ID}" "${WORKSPACE_ROOT}"

if [[ ! -f "${FSQLITE_MANIFEST}" || ! -f "${WORKSPACE_MANIFEST}" ]]; then
    printf 'bead_id=%s level=ERROR case=missing_manifest fsqlite=%s workspace=%s\n' \
        "${BEAD_ID}" "${FSQLITE_MANIFEST}" "${WORKSPACE_MANIFEST}"
    exit 1
fi

required_feature_lines=(
    '[features]'
    'default = ["json", "fts5", "rtree"]'
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
    rm -f "${metadata_json}"
}
trap cleanup EXIT

(cd "${WORKSPACE_ROOT}" && cargo metadata --format-version=1 >"${metadata_json}")

member_count="$(jq '.workspace_members | length' "${metadata_json}")"
if [[ "${member_count}" -ne 24 ]]; then
    printf 'bead_id=%s level=ERROR case=workspace_member_count expected=24 actual=%s\n' \
        "${BEAD_ID}" "${member_count}"
    exit 1
fi

has_wal_pager_edge="$(
    jq -r '
        . as $m
        | ($m.workspace_members | map((split("/")[-1] | split("#")[0]))) as $members
        | .resolve.nodes[]
        | select((.id | split("/")[-1] | split("#")[0]) == "fsqlite-wal")
        | [.deps[]?.name | gsub("_"; "-") | select(. == "fsqlite-pager")] | length
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
        | .deps[]?.name as $dep_raw
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
    printf 'bead_id=%s level=WARN case=degraded_mode_count=0 reference=bd-1fpm\n' "${BEAD_ID}"
    printf 'bead_id=%s level=ERROR case=terminal_failure_count=0 reference=bd-1fpm\n' "${BEAD_ID}"
    printf 'bead_id=%s level=INFO case=pass\n' "${BEAD_ID}"
    exit 0
fi

printf 'bead_id=%s level=WARN case=degraded_mode_count=1 reference=bd-1fpm\n' "${BEAD_ID}"
printf 'bead_id=%s level=ERROR case=build_matrix_failed reference=bd-1fpm\n' "${BEAD_ID}"
exit 1
