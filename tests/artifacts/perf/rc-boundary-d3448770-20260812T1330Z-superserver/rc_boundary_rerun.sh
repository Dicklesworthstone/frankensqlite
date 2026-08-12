#!/usr/bin/env bash
# RC boundary rerun: control b612eb7b5 vs RC d34487705 (bd-dqdoe release gate).
# Sequential citation-grade builds, then ABBA invocation campaign, CPUs 40-47.
set -euo pipefail

export RCH_CARGO_WRAPPER_BYPASS=1
BASE=/data/tmp/claude-1000/-data-projects-frankensqlite/95241f1f-1d30-401b-b12a-7a16cff39274/scratchpad/rcgate
REPO=/data/projects/frankensqlite
CTRL_SHA=b612eb7b5513c03c7cf5a091208c9cfe7d20c755
RC_SHA=d34487705
TARGET_TRIPLE=x86_64-unknown-linux-gnu
CPUS=40-47

mkdir -p "$BASE"/runs/read "$BASE"/runs/conc
cd "$REPO"
RC_FULL=$(git rev-parse "$RC_SHA")

for side in ctrl rc; do
  sha_var=$([ "$side" = ctrl ] && echo "$CTRL_SHA" || echo "$RC_FULL")
  wt="$BASE/wt-$side"
  if [ ! -d "$wt" ]; then
    git worktree add --detach "$wt" "$sha_var"
  fi
  ( cd "$wt" && [ -z "$(git status --porcelain)" ] ) || { echo "FATAL: $side worktree dirty"; exit 2; }
done

build_side() {
  local side=$1 wt="$BASE/wt-$1"
  local bin="$BASE/$side.bin"
  if [ -s "$BASE/$side.binary.sha256" ] && [ -x "$bin" ]; then
    echo "=== BUILD $side already complete, reusing $(cat "$BASE/$side.binary.sha256")"
    return 0
  fi
  local nonce
  nonce=$(openssl rand -hex 32)
  echo "$nonce" > "$BASE/$side.nonce"
  echo "=== BUILD $side ($(cd "$wt" && git rev-parse --short HEAD)) nonce=$nonce $(date -u +%H:%M:%S) ==="
  # Canonical `release` profile environment per canonical_profile_environment():
  # every value explicitly forced so no Cargo config / wrapper can vary the build.
  ( cd "$wt" && \
    env CARGO_TARGET_DIR="$BASE/$side-target" \
        CARGO_ENCODED_RUSTFLAGS= \
        CARGO_BUILD_RUSTC_WORKSPACE_WRAPPER= \
        CARGO_BUILD_RUSTC_WRAPPER= \
        CARGO_BUILD_RUSTFLAGS= \
        RUSTC_WORKSPACE_WRAPPER= \
        RUSTC_WRAPPER= \
        CARGO_PROFILE_RELEASE_CODEGEN_UNITS=1 \
        CARGO_PROFILE_RELEASE_DEBUG=false \
        CARGO_PROFILE_RELEASE_DEBUG_ASSERTIONS=false \
        CARGO_PROFILE_RELEASE_INCREMENTAL=false \
        CARGO_PROFILE_RELEASE_LTO=true \
        CARGO_PROFILE_RELEASE_OPT_LEVEL=z \
        CARGO_PROFILE_RELEASE_OVERFLOW_CHECKS=false \
        CARGO_PROFILE_RELEASE_PANIC=abort \
        CARGO_PROFILE_RELEASE_RPATH=false \
        CARGO_PROFILE_RELEASE_SPLIT_DEBUGINFO=off \
        CARGO_PROFILE_RELEASE_STRIP=true \
        FSQLITE_BENCH_PROFILE_NAME=release \
        FSQLITE_BENCH_BUILD_NONCE="$nonce" \
        LIBSQLITE3_FLAGS="-DSQLITE_ENABLE_MATH_FUNCTIONS" \
        cargo build -vv --locked --offline --release --target "$TARGET_TRIPLE" \
        -p fsqlite-e2e --bin comprehensive-bench > "$BASE/$side-build-vv.log" 2>&1 )
  local built="$BASE/$side-target/$TARGET_TRIPLE/release/comprehensive-bench"
  [ -x "$built" ] || { echo "FATAL: $side binary missing"; exit 2; }
  # Rescue the binary out of the target tree immediately: scratch cargo
  # targets on this box get janitor-purged (ctrl-target vanished mid-campaign
  # on attempt 2), so runs use the rescued copy, never the target path.
  cp "$built" "$bin"
  # The provenance apparatus (build.rs nonce -> rustc-env -> -vv receipt) exists
  # only at the RC vintage; control b612eb7b5 predates it, so the nonce proof
  # is asserted for rc only and the control arm is receipted by clean-worktree
  # SHA + ELF sha256 + kept build log (vintage limitation, noted in verdict).
  if [ "$side" = rc ]; then
    grep -q "FSQLITE_BENCH_BUILD_NONCE=$nonce" "$BASE/$side-build-vv.log" \
      || { echo "FATAL: nonce not proven in rc -vv log"; exit 2; }
  fi
  sha256sum "$bin" > "$BASE/$side.binary.sha256"
  echo "=== BUILD $side done $(date -u +%H:%M:%S) $(cat "$BASE/$side.binary.sha256")"
}

build_side ctrl
build_side rc

run_one() {
  local side=$1 family=$2 tag=$3
  local wt="$BASE/wt-$side"
  local bin="$BASE/$side.bin"
  local out="$BASE/runs/$family/$tag.$side.json"
  local log="$BASE/runs/$family/$tag.$side.log"
  local filter=$([ "$family" = read ] && echo read || echo concurrent)
  local extra=""
  [ "$side" = rc ] && extra="--allow-unverified-provenance"
  echo "--- run $family $tag $side $(date -u +%H:%M:%S) load=$(cut -d' ' -f1 /proc/loadavg)"
  cd "$BASE/wt-$side"
  env FSQLITE_BENCH_BUILD_LOG_PATH="$BASE/$side-build-vv.log" \
      FSQLITE_BENCH_SOURCE_ROOT="$wt" \
      FSQLITE_BENCH_EXPECTED_CPU_AFFINITY="$CPUS" \
      taskset -c "$CPUS" "$bin" --quick --filter "$filter" \
      --json-out "$out" --no-html $extra \
      > "$log" 2>&1 || { echo "RUN FAILED: $family $tag $side rc=$?"; exit 3; }
}

for family in read conc; do
  for block in 1 2 3 4 5 6 7 8 9 10; do
    run_one ctrl "$family" "b$block.1"
    run_one rc   "$family" "b$block.2"
    run_one rc   "$family" "b$block.3"
    run_one ctrl "$family" "b$block.4"
  done
done
echo "=== CAMPAIGN COMPLETE $(date -u +%H:%M:%S) ==="
