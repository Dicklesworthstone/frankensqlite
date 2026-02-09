#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RESULT_DIR="${ROOT_DIR}/test-results"
REPORT_JSONL="${RESULT_DIR}/reference_index_audit.jsonl"
REQUIRE_ASUPERSYNC="${REQUIRE_ASUPERSYNC:-0}"

mkdir -p "${RESULT_DIR}"
: >"${REPORT_JSONL}"

failures=0
warnings=0

emit() {
    local ref_kind="$1"
    local path="$2"
    local exists="$3"
    local notes="$4"
    printf '{"ref_kind":"%s","path":"%s","exists":%s,"notes":"%s"}\n' \
        "${ref_kind}" "${path}" "${exists}" "${notes}" >>"${REPORT_JSONL}"
}

check_repo_path() {
    local ref_kind="$1"
    local rel_path="$2"
    local notes="$3"
    local abs_path="${ROOT_DIR}/${rel_path}"
    if [[ -e "${abs_path}" ]]; then
        emit "${ref_kind}" "${rel_path}" "true" "${notes}"
    else
        emit "${ref_kind}" "${rel_path}" "false" "${notes}"
        failures=$((failures + 1))
    fi
}

check_external_path() {
    local rel_path="$1"
    local notes="$2"
    if [[ -e "${rel_path}" ]]; then
        emit "asupersync" "${rel_path}" "true" "${notes}"
        return
    fi

    emit "asupersync" "${rel_path}" "false" "${notes}"
    warnings=$((warnings + 1))
    if [[ "${REQUIRE_ASUPERSYNC}" == "1" ]]; then
        failures=$((failures + 1))
    fi
}

c_sqlite_refs=(
    "legacy_sqlite_code/sqlite/src/sqliteInt.h"
    "legacy_sqlite_code/sqlite/src/btree.c"
    "legacy_sqlite_code/sqlite/src/pager.c"
    "legacy_sqlite_code/sqlite/src/wal.c"
    "legacy_sqlite_code/sqlite/src/vdbe.c"
    "legacy_sqlite_code/sqlite/src/select.c"
    "legacy_sqlite_code/sqlite/src/where.c"
    "legacy_sqlite_code/sqlite/src/wherecode.c"
    "legacy_sqlite_code/sqlite/src/whereexpr.c"
    "legacy_sqlite_code/sqlite/src/whereInt.h"
    "legacy_sqlite_code/sqlite/src/parse.y"
    "legacy_sqlite_code/sqlite/src/tokenize.c"
    "legacy_sqlite_code/sqlite/src/func.c"
    "legacy_sqlite_code/sqlite/src/expr.c"
    "legacy_sqlite_code/sqlite/src/build.c"
)

project_docs=(
    "COMPREHENSIVE_SPEC_FOR_FRANKENSQLITE_V1.md"
    "EXISTING_SQLITE_STRUCTURE.md"
    "docs/rfc6330.txt"
    "AGENTS.md"
    "MVCC_SPECIFICATION.md"
    "PROPOSED_ARCHITECTURE.md"
)

asupersync_refs=(
    "/dp/asupersync/src/raptorq"
    "/dp/asupersync/src/sync"
    "/dp/asupersync/src/channel/mpsc.rs"
    "/dp/asupersync/src/channel/oneshot.rs"
    "/dp/asupersync/src/cx"
    "/dp/asupersync/src/lab/runtime.rs"
    "/dp/asupersync/src/lab/explorer.rs"
    "/dp/asupersync/src/obligation/eprocess.rs"
    "/dp/asupersync/src/lab/oracle/eprocess.rs"
    "/dp/asupersync/src/lab/conformal.rs"
    "/dp/asupersync/src/database/sqlite.rs"
)

for rel_path in "${c_sqlite_refs[@]}"; do
    check_repo_path "c_sqlite" "${rel_path}" "spec_extraction_reference"
done

for rel_path in "${project_docs[@]}"; do
    check_repo_path "doc" "${rel_path}" "project_reference_doc"
done

for rel_path in "${asupersync_refs[@]}"; do
    check_external_path "${rel_path}" "integration_reference_module"
done

printf 'INFO bead_id=bd-4eue case=reference_index_audit report_jsonl=%s failures=%d warnings=%d require_asupersync=%s\n' \
    "${REPORT_JSONL}" "${failures}" "${warnings}" "${REQUIRE_ASUPERSYNC}"

if (( failures > 0 )); then
    printf 'ERROR bead_id=bd-4eue case=reference_index_audit_failed failures=%d warnings=%d report_jsonl=%s\n' \
        "${failures}" "${warnings}" "${REPORT_JSONL}" >&2
    exit 1
fi

printf 'INFO bead_id=bd-4eue case=reference_index_audit_pass report_jsonl=%s\n' "${REPORT_JSONL}"
