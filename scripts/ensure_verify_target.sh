#!/usr/bin/env bash
# ensure_verify_target.sh — bd-k0t3r / bd-kvrap
#
# Guarantee a build-cache (cargo target) directory carries the canonical
# CACHEDIR.TAG marker so the /data/tmp reclaimer finds it BY TAG (reclaim-by-tag
# + staleness), not by an `rch_target_*` name match. Agents name verify-target
# dirs arbitrarily (`verify-*`, `*bld`, `p4-verify`, `srw`, `cargo-target`, ...);
# without the tag those regenerable caches are never reclaimed and drive `/` to
# 98%+, at which point the host starts killing running builds. Cargo writes this
# tag itself once it builds, but a target dir created up front — or a build
# killed before cargo's first write — would otherwise stay untagged and invisible
# to the reclaimer. Tagging up front closes that window and retires the
# "name the dir without 'target' to dodge the sweeper" anti-pattern.
#
# Usage:
#   scripts/ensure_verify_target.sh <dir> [<dir> ...]
#   eval "$(scripts/ensure_verify_target.sh --export <dir>)"   # also exports CARGO_TARGET_DIR=<dir>
#
# Idempotent and strictly ADDITIVE: only ever creates the directory and the tag,
# and only writes the tag when the canonical signature line is absent (so it
# never clobbers cargo's own tag nor churns its mtime). It NEVER deletes.
set -uo pipefail

# The canonical cache-directory-tag signature (identical to cargo's), per
# https://bford.info/cachedir/ — the reclaimer keys off this exact first line.
readonly CACHEDIR_TAG_SIGNATURE='Signature: 8a477f597d28d172789f06886806bc55'

write_tag() {
  local dir="$1"
  mkdir -p "$dir" || return 1
  local tag="$dir/CACHEDIR.TAG"
  if [ ! -f "$tag" ] || ! head -n1 "$tag" 2>/dev/null | grep -qxF "$CACHEDIR_TAG_SIGNATURE"; then
    {
      printf '%s\n' "$CACHEDIR_TAG_SIGNATURE"
      printf '%s\n' '# Cache directory tag created by frankensqlite verify tooling (bd-k0t3r).'
      printf '%s\n' '# Marks this cargo target for reclaim-by-tag (cargo-sweep / SBH reclaimer),'
      printf '%s\n' '# so a regenerable build cache ages out by staleness regardless of dir name.'
      printf '%s\n' '# For information about cache directory tags see https://bford.info/cachedir/'
    } >"$tag"
  fi
}

export_mode=0
if [ "${1:-}" = "--export" ]; then
  export_mode=1
  shift
fi

if [ "$#" -eq 0 ]; then
  printf 'usage: %s [--export] <target-dir> [<target-dir> ...]\n' "$0" >&2
  exit 2
fi

for dir in "$@"; do
  write_tag "$dir" || {
    printf 'ensure_verify_target: failed to create/tag %s\n' "$dir" >&2
    exit 1
  }
done

# With --export, emit a line to `eval` that points CARGO_TARGET_DIR at the FIRST
# dir, so a verify script can both create+tag and select its target in one step.
if [ "$export_mode" -eq 1 ]; then
  printf 'export CARGO_TARGET_DIR=%q\n' "$1"
fi
