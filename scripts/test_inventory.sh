#!/usr/bin/env bash
# Canonical FrankenSQLite test-realism and Turso adaptation inventory.
#
# The default command is offline and validates the reviewed contract against
# tracked HEAD. `audit` additionally fetches metadata for the pinned Turso Git
# tree and proves every upstream top-level testing entry is classified.

set -euo pipefail

WORKSPACE_ROOT="${WORKSPACE_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
OUTPUT_DIR="${OUTPUT_DIR:-${WORKSPACE_ROOT}/target/test-inventory}"
CONTRACT_PATH="${CONTRACT_PATH:-${WORKSPACE_ROOT}/docs/contracts/turso_test_adaptation_inventory.toml}"
UPSTREAM_TREE_JSON="${TURSO_TREE_JSON:-${OUTPUT_DIR}/turso-tree.json}"
REPORT_JSON="${OUTPUT_DIR}/test_inventory.json"
REPORT_MARKDOWN="${OUTPUT_DIR}/summary.md"
REPORT_CSV="${OUTPUT_DIR}/test_inventory.csv"

contract_commit() {
    awk '
        /^\[source\]$/ { in_source = 1; next }
        /^\[/ && in_source { exit }
        in_source && /^commit = / {
            value = $0
            sub(/^commit = "/, "", value)
            sub(/"$/, "", value)
            print value
            exit
        }
    ' "${CONTRACT_PATH}"
}

fetch_upstream_tree() {
    local commit
    commit="$(contract_commit)"
    if [[ ! "${commit}" =~ ^[0-9a-f]{40}$ ]]; then
        echo "ERROR invalid pinned Turso commit in ${CONTRACT_PATH}: ${commit}" >&2
        return 1
    fi

    mkdir -p "${OUTPUT_DIR}"
    local -a curl_args=(
        --fail
        --silent
        --show-error
        --location
        --retry 3
        --header "Accept: application/vnd.github+json"
        --header "X-GitHub-Api-Version: 2022-11-28"
    )
    if [[ -n "${GITHUB_TOKEN:-}" ]]; then
        curl_args+=(--header "Authorization: Bearer ${GITHUB_TOKEN}")
    fi
    curl "${curl_args[@]}" \
        "https://api.github.com/repos/tursodatabase/turso/git/trees/${commit}?recursive=1" \
        --output "${UPSTREAM_TREE_JSON}"
    echo "INFO upstream_tree_fetched commit=${commit} artifact=${UPSTREAM_TREE_JSON}"
}

run_inventory() {
    local require_upstream="$1"
    shift
    local -a args=(
        --workspace-root "${WORKSPACE_ROOT}"
        --contract "${CONTRACT_PATH}"
        --output-json "${REPORT_JSON}"
        --output-markdown "${REPORT_MARKDOWN}"
        --output-csv "${REPORT_CSV}"
        "$@"
    )
    if [[ "${require_upstream}" == "true" ]]; then
        args+=(--upstream-tree "${UPSTREAM_TREE_JSON}" --require-upstream-tree)
    fi

    mkdir -p "${OUTPUT_DIR}"
    if [[ -n "${FSQLITE_TEST_INVENTORY_BIN:-}" ]]; then
        "${FSQLITE_TEST_INVENTORY_BIN}" "${args[@]}"
    else
        cargo run --quiet -p fsqlite-harness --bin test_inventory -- "${args[@]}"
    fi
}

cmd_full() {
    run_inventory false "$@"
}

cmd_audit() {
    if [[ -z "${TURSO_TREE_JSON:-}" ]]; then
        fetch_upstream_tree
    elif [[ ! -f "${UPSTREAM_TREE_JSON}" ]]; then
        echo "ERROR TURSO_TREE_JSON does not exist: ${UPSTREAM_TREE_JSON}" >&2
        return 1
    fi
    run_inventory true "$@"
}

cmd_summary() {
    if [[ ! -f "${REPORT_MARKDOWN}" ]]; then
        echo "ERROR no report found; run '$0 full' or '$0 audit' first" >&2
        return 1
    fi
    printf '%s\n' "$(cat "${REPORT_MARKDOWN}")"
}

cmd_crate() {
    local crate_name="${1:-}"
    if [[ -z "${crate_name}" ]]; then
        echo "ERROR crate name required" >&2
        return 2
    fi
    if [[ ! -f "${REPORT_CSV}" ]]; then
        cmd_full
    fi
    awk -F, -v crate="${crate_name}" 'NR == 1 || $1 == crate' "${REPORT_CSV}"
}

print_help() {
    cat <<'EOF'
FrankenSQLite Test Realism and Turso Adaptation Inventory

Usage: scripts/test_inventory.sh <command> [runner options]

Commands:
  full        Validate tracked HEAD and generate JSON, Markdown, and CSV (default)
  audit       Fetch and validate the pinned Turso Git-tree metadata, then generate reports
  summary     Print the most recently generated human report
  crate NAME  Print CSV rows for one crate (generates the offline report if absent)
  help        Show this help

Environment:
  OUTPUT_DIR                  Output directory (default: target/test-inventory)
  CONTRACT_PATH               Canonical intake contract path
  TURSO_TREE_JSON             Use an existing pinned GitHub tree JSON instead of fetching
  GITHUB_TOKEN                Optional GitHub API token
  FSQLITE_TEST_INVENTORY_BIN  Prebuilt runner used instead of cargo run
EOF
}

main() {
    local command="${1:-full}"
    shift || true
    case "${command}" in
        full) cmd_full "$@" ;;
        audit) cmd_audit "$@" ;;
        summary) cmd_summary ;;
        crate) cmd_crate "$@" ;;
        help|--help|-h) print_help ;;
        *)
            echo "ERROR unknown command: ${command}" >&2
            print_help >&2
            return 2
            ;;
    esac
}

main "$@"
