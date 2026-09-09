#!/usr/bin/env bash
# bd-oemkn: keep isolated consumer builds out of workspace feature unification.
# Actions is disabled; this is an executable local/RCH gate, not a CI claim.
# Usage: bash scripts/verify_feature_configurations.sh [all|core-no-default|cli-default]
# Set RCH_WORKER to coordinate worker selection; builds are strict remote, -j2.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONFIGURATION="${1:-all}"
if [[ $# -gt 1 ]]; then
    echo "Usage: $0 [all|core-no-default|cli-default]" >&2
    exit 2
fi
case "$CONFIGURATION" in
    all|core-no-default|cli-default) ;;
    *) echo "Unknown feature configuration: $CONFIGURATION" >&2; exit 2 ;;
esac
cd "$REPO_ROOT"

run_check() {
    local name="$1"
    shift
    printf '[bd-oemkn] %s: cargo check --locked -j 2' "$name"
    printf ' %q' "$@"
    printf '\n'
    RCH_REQUIRE_REMOTE=1 rch exec --source-content-receipt -- \
        env CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 \
        cargo check --locked -j 2 "$@"
    printf '[bd-oemkn] %s: PASS\n' "$name"
}

if [[ "$CONFIGURATION" == all || "$CONFIGURATION" == core-no-default ]]; then
    # Native now implies diagnostic-pragmas, including through the CLI's
    # unconditional native dependency. Only a separate featureless core build
    # exercises helpers accidentally missing that cfg. Exclude dev edges too.
    core_features="$(cargo tree --locked -p fsqlite-core --no-default-features \
        -e normal,build --depth 0 --format '{f}')"
    if [[ -n "$core_features" ]]; then
        printf '[bd-oemkn] expected no core features, found: %s\n' "$core_features" >&2
        exit 1
    fi
    run_check core-no-default -p fsqlite-core --lib --no-default-features
fi

if [[ "$CONFIGURATION" == all || "$CONFIGURATION" == cli-default ]]; then
    # Match the shipped consumer's package selection. Never combine this with
    # the core selection: Cargo would unify features and mask the first check.
    run_check cli-default -p fsqlite-cli --bin fsqlite
fi
