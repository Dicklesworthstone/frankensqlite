#!/usr/bin/env bash
# bd-es2im: one arm of a layout-averaged A/B. Builds the release-perf CLI (`fsqlite`) of the current tree once,
# with -C save-temps and its link command printed, then relinks the same objects K times with
# lld --shuffle-sections='.text*=<seed>': each seed is one random order of the functions in .text (Rust emits one
# section per function). Measurement only: no source change, no alignment flags.
#
#   scripts/layout_shuffle_relink.sh <tag> [K] [first-seed]
#
# Run it on an rch worker in one job, so the objects never leave the worker:
#   rch exec --job --result-dir .rch/layout/<tag> -- bash scripts/layout_shuffle_relink.sh <tag> 6
# Output in .rch/layout/<tag>/: fsqlite-cargo (cargo's own ELF, the unshuffled layout), fsqlite-s<seed> for each
# seed, manifest.tsv (file, seed, sha256, bytes, address of run_hot_opcodes_sync), link-command.txt, build.log.
# Before any seed, the captured command is replayed unchanged into .rch/layout-check/ (not synced back) and must
# reproduce cargo's ELF byte for byte, which proves the command is complete. Nothing is deleted.
#
# Another binary (bd-q4o7z probe 5: mt-mvcc-bench) is relinked the same way with LAYOUT_PACKAGE, LAYOUT_BIN and
# LAYOUT_TOUCH (the binary's own source, touched so cargo relinks and prints the command); its files are then
# <LAYOUT_BIN>-cargo and <LAYOUT_BIN>-s<seed>. The defaults are the CLI's.
set -euo pipefail
tag=${1:?tag}
k=${2:-6}
seed0=${3:-1}
pkg=${LAYOUT_PACKAGE:-fsqlite-cli}
bin=${LAYOUT_BIN:-fsqlite}
touch_src=${LAYOUT_TOUCH:-crates/fsqlite-cli/src/main.rs}
out=.rch/layout/$tag
mkdir -p "$out"
log=$out/build.log
export CARGO_PROFILE_RELEASE_PERF_STRIP=false CARGO_PROFILE_RELEASE_PERF_DEBUG=line-tables-only
{
  echo "host $(uname -n) $(date -u +%FT%TZ)"
  echo "head $(git rev-parse HEAD 2>/dev/null || echo unknown) diff $(git diff 2>/dev/null | sha256sum | cut -c1-16)"
  rustc -vV
} >> "$log" 2>&1
# Only the binary's crate takes the extra flags; with fat LTO its link is the whole program. A fresh crate
# prints no link command, so its source is touched first (only that crate and the LTO step rerun).
touch "$touch_src"
cargo rustc --locked -j "${LAYOUT_JOBS:-3}" --profile release-perf -p "$pkg" --bin "$bin" -- \
  -C save-temps --print link-args >> "$out/link-args.raw" 2>> "$log"
cmd=$(grep -E '"-o" ' "$out/link-args.raw" | tail -n 1)
[ -n "$cmd" ] || { echo "no link command captured" >> "$log"; exit 3; }
printf '%s\n' "$cmd" >> "$out/link-command.txt"
orig=$(printf '%s\n' "$cmd" | grep -oE '"-o" "[^"]+"' | head -n 1 | sed -E 's/^"-o" "(.*)"$/\1/')
[ -f "$orig" ] || { echo "linked ELF $orig not found" >> "$log"; exit 4; }
cp "$orig" "$out/$bin-cargo"
here=$(pwd)
# The captured command is a shell-quoted line (environment assignments, then cc and its arguments). It is split
# into words with Python's shlex, its -o target replaced and the extra argument appended, and run through env:
# no eval of the text.
relink() { # <dest> [extra link arg]
  local dest=$1
  shift
  local -a argv
  mapfile -d '' -t argv < <(python3 -c '
import shlex, sys
words = shlex.split(sys.argv[1])
words[words.index("-o") + 1] = sys.argv[2]
sys.stdout.write("".join(w + "\0" for w in words + sys.argv[3:]))
' "$cmd" "$here/$dest" "$@")
  env "${argv[@]}" >> "$log" 2>&1
}
hot_addr() { nm "$1" 2>/dev/null | grep -m 1 'run_hot_opcodes_sync' | cut -d' ' -f1 || true; }
printf 'file\tseed\tsha256\tbytes\trun_hot_opcodes_sync\n' >> "$out/manifest.tsv"
mkdir -p .rch/layout-check
check=.rch/layout-check/$tag
relink "$check"
printf '%s\t-\t%s\t%s\t%s\n' "$bin-cargo" "$(sha256sum "$out/$bin-cargo" | cut -d' ' -f1)" \
  "$(stat -c %s "$out/$bin-cargo")" "$(hot_addr "$out/$bin-cargo")" >> "$out/manifest.tsv"
printf '%s\t-\t%s\t%s\t%s\n' "replayed-command" "$(sha256sum "$check" | cut -d' ' -f1)" \
  "$(stat -c %s "$check")" "$(hot_addr "$check")" >> "$out/manifest.tsv"
if ! cmp -s "$out/$bin-cargo" "$check"; then
  echo "replayed link command does not reproduce cargo's ELF" >> "$log"
  exit 5
fi
for ((s = seed0; s < seed0 + k; s++)); do
  relink "$out/$bin-s$s" "-Wl,--shuffle-sections=.text*=$s"
  printf '%s\t%s\t%s\t%s\t%s\n' "$bin-s$s" "$s" "$(sha256sum "$out/$bin-s$s" | cut -d' ' -f1)" \
    "$(stat -c %s "$out/$bin-s$s")" "$(hot_addr "$out/$bin-s$s")" >> "$out/manifest.tsv"
done
echo "done $(date -u +%FT%TZ)" >> "$log"
