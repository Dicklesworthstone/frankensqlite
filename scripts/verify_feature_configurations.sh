#!/usr/bin/env bash
# bd-oemkn: keep isolated consumer builds out of workspace feature unification.
# Actions is disabled; this is an executable local/RCH gate, not a CI claim.
# Usage: bash scripts/verify_feature_configurations.sh [all|core-no-default|cli-default|stable-x86_64|stable-windows]
# Set RCH_WORKER to coordinate worker selection; existing modes are strict remote.
# stable-x86_64 also works locally when rch is absent. Install stable and its
# x86_64-unknown-linux-gnu standard library first; the local host or chosen remote
# worker must be able to execute that target. This gate never installs toolchains.
# stable-windows cross-checks VFS libraries and test compilation. It defaults
# to x86_64-pc-windows-gnu; CARGO_BUILD_TARGET can select x86_64-pc-windows-msvc.
# Install the selected Rust target and its C toolchain before running this mode.
# Windows-specific tests still need execution on a Windows host.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONFIGURATION="${1:-all}"
if [[ $# -gt 1 ]]; then
    echo "Usage: $0 [all|core-no-default|cli-default|stable-x86_64|stable-windows]" >&2
    exit 2
fi
case "$CONFIGURATION" in
    all|core-no-default|cli-default|stable-x86_64|stable-windows) ;;
    *) echo "Unknown feature configuration: $CONFIGURATION" >&2; exit 2 ;;
esac
cd "$REPO_ROOT"

run_stable_build() {
    if command -v rch >/dev/null 2>&1; then
        # A failed remote build must fail this gate, never silently retry locally.
        RCH_REQUIRE_REMOTE=1 rch exec --source-content-receipt -- "$@"
    else
        "$@"
    fi
}

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

run_stable() {
    local name="$1"
    shift
    printf '[GH#522] %s:' "$name"
    printf ' %q' cargo +stable "$@"
    printf '\n'
    # The checkout pins nightly for development. Select stable explicitly and
    # disable bootstrap so inherited settings cannot conceal unstable APIs.
    run_stable_build \
        env RUSTC_BOOTSTRAP=-1 CARGO_INCREMENTAL=0 \
        CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 \
        cargo +stable "$@"
    printf '[GH#522] %s: PASS\n' "$name"
}

if [[ "$CONFIGURATION" == all || "$CONFIGURATION" == stable-x86_64 ]]; then
    # Print stable compiler metadata through the same command wrapper.
    run_stable_build env RUSTC_BOOTSTRAP=-1 rustc +stable -vV
    # Pin the reported architecture even on hosts whose default target differs.
    # Do not use --all-features: fsqlite-types/nightly-simd intentionally opts
    # into nightly. Independent invocations keep default features from leaking
    # into the featureless checks through workspace feature unification.
    stable_target=x86_64-unknown-linux-gnu
    for package in fsqlite-pager fsqlite-btree fsqlite; do
        run_stable "$package-default" check --locked -j 2 \
            --target "$stable_target" -p "$package" --lib
        run_stable "$package-no-default" check --locked -j 2 \
            --target "$stable_target" -p "$package" --lib --no-default-features
    done
    # Exercise raw cache hints, resident/absent/evicted pages, B-tree descent,
    # and sequential/random inserts with the same stable compiler as consumers.
    run_stable vfs-prefetch test --locked -j 2 \
        --target "$stable_target" -p fsqlite-vfs --lib prefetch
    run_stable pager-prefetch test --locked -j 2 \
        --target "$stable_target" -p fsqlite-pager --lib prefetch
    run_stable btree-prefetch test --locked -j 2 \
        --target "$stable_target" -p fsqlite-btree --lib prefetch
fi

if [[ "$CONFIGURATION" == stable-windows ]]; then
    # Keep this explicit: Linux-only hosts running `all` need not install a
    # Windows cross toolchain. Do not let a non-Windows target bypass the gate.
    stable_windows_target="${CARGO_BUILD_TARGET:-x86_64-pc-windows-gnu}"
    case "$stable_windows_target" in
        x86_64-pc-windows-gnu|x86_64-pc-windows-msvc) ;;
        *) echo "Expected an x86_64 Windows target, found: $stable_windows_target" >&2; exit 2 ;;
    esac
    run_stable_build env RUSTC_BOOTSTRAP=-1 rustc +stable -vV
    run_stable vfs-windows-default check --locked -j 2 \
        --target "$stable_windows_target" -p fsqlite-vfs --lib
    run_stable vfs-windows-no-default check --locked -j 2 \
        --target "$stable_windows_target" -p fsqlite-vfs --lib --no-default-features
    # Type-check the actual Windows hard-link, retained-handle, and cleanup
    # regressions without pretending a cross host can execute Windows binaries.
    run_stable vfs-windows-tests check --locked -j 2 \
        --target "$stable_windows_target" -p fsqlite-vfs --tests
fi

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
