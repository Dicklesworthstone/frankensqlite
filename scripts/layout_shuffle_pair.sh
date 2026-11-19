#!/usr/bin/env bash
# bd-es2im: two arms of a layout-averaged A/B in one rch job. The parent arm is the tree as synced; the lever arm is
# that tree with <patch> applied (patch -p1). Each arm: one release-perf build with -C save-temps and K relinks
# (scripts/layout_shuffle_relink.sh). The second build is incremental over the first.
#   rch exec --job --result-dir .rch/layout/<tagP> --result-dir .rch/layout/<tagL> -- \
#     bash scripts/layout_shuffle_pair.sh <tagP> <tagL> <patch> <K> -j 3
# The trailing -j N (cargo jobs) also tells rch's admission how many cores the job takes.
#
# Stale-artifact guard: the sync gives a changed file its local mtime, which can be older than the artifact a
# previous job on the same worker built from different bytes, and cargo then keeps that artifact (2026-11-19: a
# parent arm was built from the previous pair's patched sources). Every workspace source is therefore touched before
# the parent build, and the parent's per-file sha256 list is written to .rch/layout/<tagP>/src-sha.txt, to be
# compared with git before the arm is measured.
set -euo pipefail
tp=${1:?parent tag}
tl=${2:?lever tag}
patchfile=${3:?patch}
k=${4:-6}
if [ "${5:-}" = "-j" ]; then
  export LAYOUT_JOBS=${6:?-j needs a count}
fi
[ -f "$patchfile" ] || { echo "no patch $patchfile"; exit 2; }
# LAYOUT_BENCH="<criterion filter> ..." also runs fsqlite-vdbe's pipeline_stages benches for each arm on this
# worker (a same-worker A/B), output in .rch/layout/<tag>/bench.txt.
bench() { # <tag>
  local f
  for f in ${LAYOUT_BENCH:-}; do
    cargo bench --locked -j "${LAYOUT_JOBS:-3}" -p fsqlite-vdbe --bench pipeline_stages -- "$f" \
      --warm-up-time 1 --measurement-time 4 >> ".rch/layout/$1/bench.txt" 2>&1
  done
}
mkdir -p ".rch/layout/$tp"
find crates -name '*.rs' -exec touch {} +
find crates -name '*.rs' -print0 | sort -z | xargs -0 sha256sum >> ".rch/layout/$tp/src-sha.txt"
bash scripts/layout_shuffle_relink.sh "$tp" "$k"
bench "$tp"
patch -p1 --forward --batch < "$patchfile" >> ".rch/layout/$tp/build.log" 2>&1
bash scripts/layout_shuffle_relink.sh "$tl" "$k"
bench "$tl"
