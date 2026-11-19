#!/usr/bin/env bash
# bd-8oo13: a layout A/A of the bd-es2im judging lines. One release-perf build of the tree as synced (any commit),
# relinked with seeds <first-seed>..<first-seed + K - 1> (scripts/layout_shuffle_relink.sh), after the same
# stale-artifact guard as scripts/layout_shuffle_pair.sh: every workspace source touched, and their per-file
# sha256 list written to .rch/layout/<tag>/src-sha.txt for comparison with git before any measurement.
#   rch exec --job --result-dir .rch/layout/<tag> -- bash scripts/layout_aa.sh <tag> <K> <first-seed> -j 3
# Pairs are then formed from disjoint six-seed subsets of the one build (bd-8oo13 step 1: seeds 1..6 against
# 7..12 and 13..18 against 19..24, K=24) and judged as a lever against its parent: identical code, so every
# replicated verdict is false. On 5c7963d88 (2026-11-19) the count was 0 in 56 tests and the lines stood. A
# later question about the lines is answered by re-running this, not by argument.
set -euo pipefail
tag=${1:?tag}
k=${2:?count}
seed0=${3:?first seed}
if [ "${4:-}" = "-j" ]; then
  export LAYOUT_JOBS=${5:?-j needs a count}
fi
mkdir -p ".rch/layout/$tag"
find crates -name '*.rs' -exec touch {} +
find crates -name '*.rs' -print0 | sort -z | xargs -0 sha256sum >> ".rch/layout/$tag/src-sha.txt"
bash scripts/layout_shuffle_relink.sh "$tag" "$k" "$seed0"
