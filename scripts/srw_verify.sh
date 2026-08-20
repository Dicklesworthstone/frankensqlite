#!/usr/bin/env bash
# srw_verify.sh — bd-96vf9 (retention fix for the regrowing /data/tmp/srw)
#
# THE PROBLEM
#   Agents run private-target verifies/stress-churn builds by hand-picking a
#   CARGO_TARGET_DIR under /data/tmp (the "srw" convention: /data/tmp/srw,
#   /data/tmp/srw<bead>, ...). Two things then drive /data/tmp/srw to tens of GB
#   and keep it there:
#     1. the cargo BUILD CACHE grows (every distinct test binary ever linked
#        piles into deps/) and, being untagged, the reclaimer never ages it out;
#     2. the RUN SCRATCH (stress test DBs / WAL / -journal / logs a churn harness
#        generates every run) is written into a persistent dir and never pruned.
#   The user's manual `rm -rf /data/tmp/srw` reclaims it, but it regrows within
#   hours because the next run repeats the pattern.
#
# THE FIX (this wrapper — the documented way to run a private-target verify)
#   Mechanism 1 — TAG: the cargo target is created + CACHEDIR.TAG-tagged via
#     scripts/ensure_verify_target.sh, so the /data/tmp reclaimer (cargo-sweep /
#     SBH reclaim-by-tag, bd-k0t3r/bd-kvrap) can age the regenerable cache out by
#     staleness regardless of the dir's name.
#   Mechanism 2 — CLEANUP-ON-SUCCESS (keep only failing-run artifacts): the run's
#     scratch (TMPDIR + FSQLITE_* scratch env) is sandboxed into a run-scoped dir
#     this wrapper OWNS. On success it is removed; on failure it is kept (and its
#     path printed) for debugging.
#
#   Default (throwaway) mode: the whole target is a run-scoped, wrapper-owned dir
#   under /data/tmp/srw/run-<runid>; a PASSING run leaves nothing behind, so
#   /data/tmp/srw cannot regrow. A FAILING run keeps its full target for triage.
#
#   --persist-cache DIR: keep a warm build cache across runs (DIR is tagged and
#   NEVER auto-deleted); only the per-run scratch under DIR/_runs/<runid> is
#   cleaned on success. Use this for fast iteration / stress LOOPS where a cold
#   rebuild each run would be prohibitive — you accept a persistent (but now
#   tag-reclaimable) cache in exchange for warmth.
#
# RULE 1 SAFETY
#   This wrapper NEVER deletes a shared/warm target dir, the /data/tmp/srw ROOT,
#   /data/tmp/cargo-target, a git worktree, or any path it did not create this
#   run. Deletion of pre-existing big dirs stays a user-only decision. Cleanup is
#   confined to a single wrapper-constructed, this-run path validated below.
#
# Usage:
#   scripts/srw_verify.sh [options] -- <command> [args...]
#
# Options:
#   --persist-cache DIR   Warm, kept build cache at DIR (tagged, never deleted).
#                         Default: throwaway target at <root>/run-<runid>.
#   --root DIR            Parent for throwaway targets (default: /data/tmp/srw).
#   --label NAME          Human tag folded into the run id (default: srw).
#   --keep-on-success     Do not clean even on success (debug/inspect).
#   -h, --help            Show this help.
#
# Examples:
#   # Throwaway verify — passing run leaves nothing under /data/tmp/srw:
#   scripts/srw_verify.sh --label bd96vf9 -- \
#     cargo test -p fsqlite-harness --test bd_mblr_7_2_endurance -- --nocapture
#
#   # Warm cache for a stress loop; run scratch still cleaned on success:
#   scripts/srw_verify.sh --persist-cache /data/tmp/srw -- \
#     cargo test -p fsqlite-core --lib
set -uo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ENSURE_TARGET="${SCRIPT_DIR}/ensure_verify_target.sh"

PERSIST_CACHE=""
ROOT="/data/tmp/srw"
LABEL="srw"
KEEP_ON_SUCCESS=0

usage() { sed -n '2,60p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; }

# ---- parse options up to `--` ------------------------------------------------
while [ "$#" -gt 0 ]; do
  case "$1" in
    --persist-cache) PERSIST_CACHE="${2:?--persist-cache needs a DIR}"; shift 2 ;;
    --root)          ROOT="${2:?--root needs a DIR}"; shift 2 ;;
    --label)         LABEL="${2:?--label needs a NAME}"; shift 2 ;;
    --keep-on-success) KEEP_ON_SUCCESS=1; shift ;;
    -h|--help)       usage; exit 0 ;;
    --)              shift; break ;;
    *) printf 'srw_verify: unknown option %q (did you forget `--` before the command?)\n' "$1" >&2; exit 2 ;;
  esac
done

if [ "$#" -eq 0 ]; then
  printf 'srw_verify: no command given after `--`\n' >&2
  usage >&2
  exit 2
fi

if [ ! -x "$ENSURE_TARGET" ]; then
  printf 'srw_verify: missing %s (needed to tag the target)\n' "$ENSURE_TARGET" >&2
  exit 1
fi

# ---- run id (no Date.now-style nondeterminism concerns here; this is bash) ----
# Sanitize the label to the CACHEDIR-tag-safe charset the cleanup guard enforces.
SAFE_LABEL="$(printf '%s' "$LABEL" | tr -c 'A-Za-z0-9._-' '_')"
RUNID="${SAFE_LABEL}-$(date -u +%Y%m%dT%H%M%SZ)-$$"

# ---- hardened cleanup helper -------------------------------------------------
# Removes exactly ONE wrapper-constructed path, after proving it is safe. Refuses
# anything that is not a real directory strictly under /data/tmp, is a symlink,
# is a git worktree, is a known shared/root cache, or has an empty run id.
srw_safe_rm() {
  local victim="$1"
  case "$victim" in
    "") printf 'srw_verify: refuse to clean empty path\n' >&2; return 1 ;;
    /data/tmp/*) : ;;  # ok — must live under /data/tmp
    *) printf 'srw_verify: refuse to clean outside /data/tmp: %q\n' "$victim" >&2; return 1 ;;
  esac
  # Never the shared/root caches, never a bare srw root.
  case "$victim" in
    /data/tmp/srw|/data/tmp/srw/|/data/tmp/cargo-target|/data/tmp/cargo-target/)
      printf 'srw_verify: refuse to clean a shared/root cache: %q\n' "$victim" >&2; return 1 ;;
  esac
  if [ -z "$RUNID" ]; then
    printf 'srw_verify: refuse to clean with empty run id\n' >&2; return 1
  fi
  # The path must end in our exact run id — proves we constructed it this run.
  case "$victim" in
    *"$RUNID") : ;;
    *) printf 'srw_verify: refuse to clean path not owned by this run: %q\n' "$victim" >&2; return 1 ;;
  esac
  if [ -L "$victim" ]; then
    printf 'srw_verify: refuse to clean a symlink: %q\n' "$victim" >&2; return 1
  fi
  if [ ! -d "$victim" ]; then
    return 0  # nothing there — nothing to do
  fi
  if [ -e "$victim/.git" ]; then
    printf 'srw_verify: refuse to clean a git worktree: %q\n' "$victim" >&2; return 1
  fi
  rm -rf -- "$victim"
}

# ---- pick target + run-scratch dirs, tag the cache ---------------------------
if [ -n "$PERSIST_CACHE" ]; then
  TARGET="$PERSIST_CACHE"
  SCRATCH="${TARGET}/_runs/${RUNID}"      # only the per-run scratch is cleaned
  CLEAN_TARGET="$SCRATCH"
  MODE="persist-cache"
else
  TARGET="${ROOT}/run-${RUNID}"           # whole throwaway target is cleaned
  SCRATCH="${TARGET}/_scratch"
  CLEAN_TARGET="$TARGET"
  MODE="throwaway"
fi

# Mechanism 1: create + CACHEDIR.TAG the cargo target (and export CARGO_TARGET_DIR).
# shellcheck disable=SC1090
eval "$("$ENSURE_TARGET" --export "$TARGET")"
mkdir -p "$SCRATCH"

# Sandbox the run's scratch so the harness's DBs/WAL/logs land where we can prune.
export TMPDIR="$SCRATCH"
export FSQLITE_STRESS_SCRATCH="$SCRATCH"   # honored by stress/churn harnesses that read it
export FSQLITE_SCRATCH_DIR="$SCRATCH"

printf 'srw_verify: mode=%s runid=%s\n' "$MODE" "$RUNID" >&2
printf 'srw_verify: CARGO_TARGET_DIR=%s\n' "$TARGET" >&2
printf 'srw_verify: scratch(TMPDIR)=%s\n' "$SCRATCH" >&2

# ---- run the wrapped command -------------------------------------------------
"$@"
RC=$?

# ---- Mechanism 2: cleanup-on-success, keep only failing-run artifacts --------
if [ "$RC" -eq 0 ]; then
  if [ "$KEEP_ON_SUCCESS" -eq 1 ]; then
    printf 'srw_verify: PASS (rc=0) — --keep-on-success set, leaving %s\n' "$CLEAN_TARGET" >&2
  else
    if srw_safe_rm "$CLEAN_TARGET"; then
      printf 'srw_verify: PASS (rc=0) — cleaned %s\n' "$CLEAN_TARGET" >&2
    else
      printf 'srw_verify: PASS (rc=0) — cleanup declined; left %s for manual review\n' "$CLEAN_TARGET" >&2
    fi
  fi
else
  printf 'srw_verify: FAIL (rc=%d) — KEEPING artifacts for triage: %s\n' "$RC" "$CLEAN_TARGET" >&2
fi

exit "$RC"
